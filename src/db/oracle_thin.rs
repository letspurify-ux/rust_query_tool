use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use oracle_rs::{
    BindDirection as ThinBindDirection, BindParam, Config as ThinConfig,
    Connection as ThinConnectionInner, ImplicitResult as ThinImplicitResult,
    OracleType as ThinOracleType, QueryResult as ThinQueryResult, Row as ThinRow,
    Value as ThinValue,
};

use crate::db::connection::ConnectionInfo;
use crate::db::query::{
    ColumnInfo, CompilationError, ConstraintInfo, IndexInfo, PackageRoutine, ProcedureArgument,
    QueryExecutor, QueryResult, ResolvedBind, SequenceInfo, SynonymInfo, TableColumnDetail,
};
use crate::db::session::{BindDataType, BindValue, CompiledObject, CursorResult};
use crate::sql_text;

pub type ThinConnection = ThinConnectionInner;

#[derive(Debug, Default)]
pub struct ThinStatementExecution {
    pub rows_affected: u64,
    pub scalar_updates: Vec<(String, BindValue)>,
    pub ref_cursors: Vec<(String, CursorResult)>,
    pub implicit_results: Vec<QueryResult>,
}

static ORACLE_THIN_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn runtime() -> &'static tokio::runtime::Runtime {
    ORACLE_THIN_RUNTIME.get_or_init(|| {
        match tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("oracle-thin")
            .build()
        {
            Ok(runtime) => runtime,
            Err(multi_thread_err) => match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(current_thread_err) => {
                    eprintln!(
                        "failed to build Oracle thin runtime (multi-thread: {multi_thread_err}; current-thread: {current_thread_err})"
                    );
                    std::process::abort();
                }
            },
        }
    })
}

fn thin_config(info: &ConnectionInfo) -> ThinConfig {
    ThinConfig::new(
        &info.host,
        info.port,
        &info.service_name,
        &info.username,
        &info.password,
    )
}

fn normalize_generated_ddl(ddl: String) -> String {
    let normalized_newlines = ddl.replace("\r\n", "\n");
    let trimmed = normalized_newlines.trim_matches('\n');
    let lines: Vec<&str> = trimmed.lines().collect();
    if lines.is_empty() {
        return String::new();
    }

    let common_indent = lines
        .iter()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.chars().take_while(|c| *c == ' ').count())
        .min()
        .unwrap_or(0);

    let mut out = String::with_capacity(trimmed.len());
    for (idx, line) in lines.iter().enumerate() {
        if idx > 0 {
            out.push('\n');
        }
        if line.trim().is_empty() {
            continue;
        }
        let cut = common_indent.min(line.len());
        out.push_str(&line[cut..]);
    }

    out.trim_start_matches([' ', '\t']).to_string()
}

fn normalize_result_column_name(name: &str, normalize_internal_rowid_alias: bool) -> String {
    if normalize_internal_rowid_alias && name.eq_ignore_ascii_case("SQ_INTERNAL_ROWID") {
        "ROWID".to_string()
    } else {
        name.to_string()
    }
}

fn thin_value_to_text(value: &ThinValue) -> String {
    match value {
        ThinValue::Null => "NULL".to_string(),
        _ => value.to_string(),
    }
}

fn thin_lob_to_text(conn: &ThinConnection, lob: &oracle_rs::LobValue) -> Result<String, String> {
    match lob {
        oracle_rs::LobValue::Null => Ok("NULL".to_string()),
        oracle_rs::LobValue::Empty => Ok(String::new()),
        oracle_rs::LobValue::Inline(data) => Ok(String::from_utf8_lossy(data).to_string()),
        oracle_rs::LobValue::Locator(locator) => {
            if locator.is_clob() {
                runtime()
                    .block_on(conn.read_clob(locator))
                    .map_err(|err| err.to_string())
            } else if locator.is_blob() || locator.is_bfile() {
                let data = runtime()
                    .block_on(conn.read_blob(locator))
                    .map_err(|err| err.to_string())?;
                Ok(format!("<{} bytes>", data.len()))
            } else {
                Ok(format!("<LOB: {} bytes>", locator.size()))
            }
        }
    }
}

fn thin_value_to_display_text(conn: &ThinConnection, value: &ThinValue) -> String {
    match value {
        ThinValue::Null => "NULL".to_string(),
        ThinValue::Lob(lob) => {
            thin_lob_to_text(conn, lob).unwrap_or_else(|_| thin_value_to_text(value))
        }
        _ => thin_value_to_text(value),
    }
}

fn thin_value_to_optional_string(value: &ThinValue) -> Option<String> {
    if matches!(value, ThinValue::Null) {
        None
    } else {
        Some(thin_value_to_text(value))
    }
}

fn thin_value_to_i32(value: &ThinValue) -> Result<i32, String> {
    if let Some(number) = value.as_i64() {
        return i32::try_from(number).map_err(|_| format!("value {number} does not fit in i32"));
    }

    let text = thin_value_to_text(value);
    text.parse::<i32>()
        .map_err(|err| format!("failed to parse `{text}` as i32: {err}"))
}

fn thin_value_to_bool_flag(value: &ThinValue, expected: &str) -> bool {
    thin_value_to_text(value).eq_ignore_ascii_case(expected)
}

fn row_value(row: &ThinRow, index: usize) -> Result<&ThinValue, String> {
    row.get(index)
        .ok_or_else(|| format!("missing Oracle thin column at index {index}"))
}

fn row_optional_string(row: &ThinRow, index: usize) -> Result<Option<String>, String> {
    Ok(thin_value_to_optional_string(row_value(row, index)?))
}

fn row_string(row: &ThinRow, index: usize) -> Result<String, String> {
    Ok(thin_value_to_optional_string(row_value(row, index)?).unwrap_or_default())
}

fn row_optional_i32(row: &ThinRow, index: usize) -> Result<Option<i32>, String> {
    let value = row_value(row, index)?;
    if matches!(value, ThinValue::Null) {
        Ok(None)
    } else {
        Ok(Some(thin_value_to_i32(value)?))
    }
}

fn thin_oracle_type(data_type: &BindDataType) -> ThinOracleType {
    match data_type {
        BindDataType::Number => ThinOracleType::Number,
        BindDataType::Varchar2(_) => ThinOracleType::Varchar,
        BindDataType::Date => ThinOracleType::Date,
        BindDataType::Timestamp(_) => ThinOracleType::Timestamp,
        BindDataType::RefCursor => ThinOracleType::Cursor,
        BindDataType::Clob => ThinOracleType::Clob,
    }
}

fn thin_bind_buffer_size(data_type: &BindDataType) -> u32 {
    match data_type {
        BindDataType::Number => 22,
        BindDataType::Varchar2(size) => (*size).max(1),
        BindDataType::Date => 64,
        BindDataType::Timestamp(_) => 128,
        BindDataType::RefCursor => 0,
        BindDataType::Clob => 1_000_000,
    }
}

fn convert_bind_value(bind: &ResolvedBind) -> Result<ThinValue, String> {
    let Some(value) = bind.value.as_ref() else {
        return Ok(ThinValue::Null);
    };

    match bind.data_type {
        BindDataType::Number => {
            if let Ok(number) = value.parse::<i64>() {
                Ok(ThinValue::Integer(number))
            } else if let Ok(number) = value.parse::<f64>() {
                Ok(ThinValue::Float(number))
            } else {
                Err(format!("Invalid numeric bind value for :{}", bind.name))
            }
        }
        BindDataType::Varchar2(_)
        | BindDataType::Date
        | BindDataType::Timestamp(_)
        | BindDataType::Clob => Ok(ThinValue::String(value.clone())),
        BindDataType::RefCursor => Err(format!(
            "REFCURSOR binds must be handled through PL/SQL execution (:{}).",
            bind.name
        )),
    }
}

fn convert_query_params(binds: &[ResolvedBind]) -> Result<Vec<ThinValue>, String> {
    binds.iter().map(convert_bind_value).collect()
}

fn convert_statement_bind_params(
    binds: &[ResolvedBind],
    input_output_scalars: bool,
) -> Result<Vec<BindParam>, String> {
    binds
        .iter()
        .map(|bind| match bind.data_type {
            BindDataType::RefCursor => Ok(BindParam::output_cursor()),
            _ => {
                let oracle_type = thin_oracle_type(&bind.data_type);
                let buffer_size = thin_bind_buffer_size(&bind.data_type);
                match bind.value.as_ref() {
                    Some(_) => Ok(BindParam {
                        value: Some(convert_bind_value(bind)?),
                        direction: if input_output_scalars {
                            ThinBindDirection::InputOutput
                        } else {
                            ThinBindDirection::Input
                        },
                        oracle_type,
                        buffer_size,
                    }),
                    None => Ok(BindParam::output(oracle_type, buffer_size)),
                }
            }
        })
        .collect()
}

fn convert_columns(
    columns: &[oracle_rs::ColumnInfo],
    normalize_internal_rowid_alias: bool,
) -> Vec<ColumnInfo> {
    columns
        .iter()
        .map(|column| ColumnInfo {
            name: normalize_result_column_name(&column.name, normalize_internal_rowid_alias),
            data_type: format!("{:?}", column.oracle_type),
        })
        .collect()
}

fn row_to_strings(conn: &ThinConnection, row: &ThinRow, column_count: usize) -> Vec<String> {
    (0..column_count)
        .map(|index| {
            row.get(index)
                .map(|value| thin_value_to_display_text(conn, value))
                .unwrap_or_else(|| "NULL".to_string())
        })
        .collect()
}

fn queue_server_cursor_close(conn: &ThinConnection, cursor_id: u16) {
    if cursor_id == 0 {
        return;
    }
    runtime().block_on(conn.queue_cursor_for_close(cursor_id));
}

fn execute_plsql_with_cursor_cleanup(
    conn: &ThinConnection,
    sql: &str,
    params: &[BindParam],
) -> Result<oracle_rs::PlsqlResult, String> {
    let result = runtime()
        .block_on(conn.execute_plsql(sql, params))
        .map_err(|err| err.to_string())?;
    queue_server_cursor_close(conn, result.statement_cursor_id);
    Ok(result)
}

fn fetch_complete_result(
    conn: &ThinConnection,
    cursor_to_close: u16,
    mut result: ThinQueryResult,
) -> Result<ThinQueryResult, String> {
    let columns = result.columns.clone();
    while result.has_more_rows {
        let mut next = runtime()
            .block_on(conn.fetch_more(result.cursor_id, &columns, 100))
            .map_err(|err| err.to_string())?;
        result.rows.append(&mut next.rows);
        result.has_more_rows = next.has_more_rows;
        result.cursor_id = next.cursor_id;
        result.rows_affected = next.rows_affected;
    }
    queue_server_cursor_close(conn, cursor_to_close);
    Ok(result)
}

fn query_all(
    conn: &ThinConnection,
    sql: &str,
    params: &[ThinValue],
) -> Result<ThinQueryResult, String> {
    let result = runtime()
        .block_on(conn.query(sql, params))
        .map_err(|err| err.to_string())?;
    fetch_complete_result(conn, result.cursor_id, result)
}

fn query_object_list(conn: &ThinConnection, sql: &str) -> Result<Vec<String>, String> {
    const PAGE_SIZE: usize = 200;
    let mut items = Vec::new();
    let mut last_name = " ".to_string();

    loop {
        let page = query_all(conn, sql, &[ThinValue::String(last_name.clone())])?;

        if page.rows.is_empty() {
            break;
        }

        for row in &page.rows {
            let item_name = row_string(row, 0)?;
            last_name = item_name.clone();
            items.push(item_name);
        }

        if page.rows.len() < PAGE_SIZE {
            break;
        }
    }

    Ok(items)
}

fn query_single_string(
    conn: &ThinConnection,
    sql: &str,
    params: &[ThinValue],
) -> Result<String, String> {
    let result = query_all(conn, sql, params)?;
    let row = result
        .rows
        .first()
        .ok_or_else(|| "Oracle thin query returned no rows".to_string())?;
    row_string(row, 0)
}

fn fetch_cursor_result(
    conn: &ThinConnection,
    cursor: &oracle_rs::RefCursor,
) -> Result<CursorResult, String> {
    let result = runtime()
        .block_on(conn.fetch_cursor(cursor))
        .map_err(|err| err.to_string())?;
    let result = fetch_complete_result(conn, cursor.cursor_id(), result)?;
    let columns = result
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let rows = result
        .rows
        .iter()
        .map(|row| row_to_strings(conn, row, result.columns.len()))
        .collect::<Vec<_>>();
    Ok(CursorResult { columns, rows })
}

fn fetch_implicit_result(
    conn: &ThinConnection,
    result: &ThinImplicitResult,
    label: &str,
) -> Result<QueryResult, String> {
    let result = runtime()
        .block_on(conn.fetch_implicit_result(result))
        .map_err(|err| err.to_string())?;
    let result = fetch_complete_result(conn, result.cursor_id, result)?;
    let columns = convert_columns(&result.columns, false);
    let rows = result
        .rows
        .iter()
        .map(|row| row_to_strings(conn, row, result.columns.len()))
        .collect::<Vec<_>>();
    Ok(QueryResult::new_select(
        label,
        columns,
        rows,
        Duration::from_secs(0),
    ))
}

fn extract_plsql_updates(
    conn: &ThinConnection,
    result: oracle_rs::PlsqlResult,
    binds: &[ResolvedBind],
    sql: &str,
) -> Result<ThinStatementExecution, String> {
    let out_values_by_index = result
        .out_param_indices
        .iter()
        .copied()
        .zip(result.out_values.iter())
        .collect::<HashMap<usize, &ThinValue>>();

    let mut execution = ThinStatementExecution {
        rows_affected: result.rows_affected,
        ..ThinStatementExecution::default()
    };

    for (index, bind) in binds.iter().enumerate() {
        match bind.data_type {
            BindDataType::RefCursor => {
                if let Some(ThinValue::Cursor(cursor)) = out_values_by_index.get(&index) {
                    execution
                        .ref_cursors
                        .push((bind.name.clone(), fetch_cursor_result(conn, cursor)?));
                }
            }
            _ => {
                let next_value = out_values_by_index
                    .get(&index)
                    .map(|value| thin_value_to_optional_string(value))
                    .unwrap_or_else(|| bind.value.clone());
                execution
                    .scalar_updates
                    .push((bind.name.clone(), BindValue::Scalar(next_value)));
            }
        }
    }

    for (idx, implicit) in result.implicit_results.results.iter().enumerate() {
        execution.implicit_results.push(fetch_implicit_result(
            conn,
            implicit,
            &format!("{sql} [IMPLICIT RESULT {}]", idx + 1),
        )?);
    }

    Ok(execution)
}

fn ensure_plsql_terminator(sql: &str) -> String {
    let trimmed = sql.trim_end();
    if trimmed.ends_with(';') {
        trimmed.to_string()
    } else {
        format!("{trimmed};")
    }
}

fn wrap_statement_in_plsql(sql: &str) -> String {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    format!("BEGIN\n  {trimmed};\nEND;")
}

fn is_ddl_like_statement(sql: &str) -> bool {
    matches!(
        QueryExecutor::leading_keyword(&QueryExecutor::normalize_sql_for_execute(sql)).as_deref(),
        Some(
            "CREATE"
                | "ALTER"
                | "DROP"
                | "TRUNCATE"
                | "RENAME"
                | "GRANT"
                | "REVOKE"
                | "COMMENT"
                | "EXPLAIN"
        )
    )
}

fn should_ensure_compiled_ddl_terminator(sql: &str) -> bool {
    is_ddl_like_statement(sql) && QueryExecutor::parse_compiled_object(sql).is_some()
}

fn execute_direct_sql(
    conn: &ThinConnection,
    sql: &str,
    params: &[ThinValue],
) -> Result<(), String> {
    match runtime().block_on(conn.execute(sql, params)) {
        Ok(result) => {
            queue_server_cursor_close(conn, result.cursor_id);
            Ok(())
        }
        Err(err) => {
            if params.is_empty()
                && is_ddl_like_statement(sql)
                && err.to_string().to_ascii_uppercase().contains("ORA-24344")
            {
                Ok(())
            } else {
                Err(err.to_string())
            }
        }
    }
}

fn flush_pending_cursor_closes(conn: &ThinConnection) -> Result<(), String> {
    runtime()
        .block_on(conn.flush_pending_close_cursors())
        .map_err(|err| err.to_string())
}

fn should_flush_pending_cursor_closes(sql: &str, sql_upper: &str) -> bool {
    if is_ddl_like_statement(sql) {
        return true;
    }

    [
        "EXECUTE IMMEDIATE 'DROP ",
        "EXECUTE IMMEDIATE 'TRUNCATE ",
        "EXECUTE IMMEDIATE 'ALTER ",
        "EXECUTE IMMEDIATE 'CREATE ",
        "EXECUTE IMMEDIATE 'RENAME ",
        "EXECUTE IMMEDIATE 'GRANT ",
        "EXECUTE IMMEDIATE 'REVOKE ",
        "EXECUTE IMMEDIATE 'COMMENT ",
    ]
    .iter()
    .any(|needle| sql_upper.contains(needle))
}

fn uses_returning_into(sql_upper: &str) -> bool {
    sql_upper.contains(" RETURNING ") && sql_upper.contains(" INTO ")
}

fn should_execute_plain_plsql_directly(
    _sql: &str,
    _binds: &[ResolvedBind],
    _is_plsql_block: bool,
) -> bool {
    false
}

fn should_retry_without_rowid(err: &str) -> bool {
    QueryExecutor::is_retryable_rowid_injection_error(err)
}

fn normalize_object_name(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        sql_text::strip_identifier_quotes(trimmed)
    } else {
        trimmed.to_ascii_uppercase()
    }
}

fn split_qualified_name(value: &str) -> (Option<String>, String) {
    let trimmed = value.trim();
    let mut in_quotes = false;
    let mut split_at = None;

    for (idx, ch) in trimmed.char_indices() {
        if ch == '"' {
            in_quotes = !in_quotes;
        } else if ch == '.' && !in_quotes {
            split_at = Some(idx);
            break;
        }
    }

    if let Some(idx) = split_at {
        let (owner, name) = trimmed.split_at(idx);
        (
            Some(owner.trim().to_string()),
            name.trim_start_matches('.').trim().to_string(),
        )
    } else {
        (None, trimmed.to_string())
    }
}

fn rowid_target_is_view(conn: &ThinConnection, sql: &str) -> Result<bool, String> {
    let Some(source_name) = QueryExecutor::rowid_edit_target_source_name(sql) else {
        return Ok(false);
    };

    let (owner_raw, object_raw) = split_qualified_name(&source_name);
    let object_name = normalize_object_name(&object_raw);
    if object_name.is_empty() {
        return Ok(false);
    }

    let rows = if let Some(owner_raw) = owner_raw {
        query_all(
            conn,
            r#"
                SELECT 1
                FROM all_objects
                WHERE owner = :1
                  AND object_name = :2
                  AND object_type IN ('VIEW', 'MATERIALIZED VIEW')
                  AND ROWNUM = 1
            "#,
            &[
                ThinValue::String(normalize_object_name(&owner_raw)),
                ThinValue::String(object_name),
            ],
        )?
    } else {
        query_all(
            conn,
            r#"
                SELECT 1
                FROM user_objects
                WHERE object_name = :1
                  AND object_type IN ('VIEW', 'MATERIALIZED VIEW')
                  AND ROWNUM = 1
            "#,
            &[ThinValue::String(object_name)],
        )?
    };

    Ok(!rows.rows.is_empty())
}

fn resolve_select_execution_sql(
    conn: &ThinConnection,
    sql: &str,
) -> Result<(String, bool), String> {
    let sql_for_editing = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    let mut sql_for_execution = QueryExecutor::rowid_safe_execution_sql(sql, &sql_for_editing);
    let mut normalize_internal_rowid_alias = sql_for_execution != sql;

    if sql_for_execution != sql && rowid_target_is_view(conn, sql)? {
        sql_for_execution = sql.to_string();
        normalize_internal_rowid_alias = false;
    }

    Ok((sql_for_execution, normalize_internal_rowid_alias))
}

pub fn connect(info: &ConnectionInfo) -> Result<Arc<ThinConnection>, String> {
    let conn = runtime()
        .block_on(ThinConnection::connect_with_config(thin_config(info)))
        .map_err(|err| err.to_string())?;
    let conn = Arc::new(conn);
    apply_default_session_settings(conn.as_ref());
    Ok(conn)
}

pub fn test_connection(info: &ConnectionInfo) -> Result<(), String> {
    let conn = runtime()
        .block_on(ThinConnection::connect_with_config(thin_config(info)))
        .map_err(|err| err.to_string())?;
    runtime()
        .block_on(conn.close())
        .map_err(|err| err.to_string())
}

pub fn close(conn: &ThinConnection) -> Result<(), String> {
    runtime()
        .block_on(conn.close())
        .map_err(|err| err.to_string())
}

pub fn interrupt(conn: &ThinConnection) -> Result<(), String> {
    conn.interrupt().map_err(|err| err.to_string())
}

pub fn ping(conn: &ThinConnection) -> Result<(), String> {
    runtime()
        .block_on(conn.ping())
        .map_err(|err| err.to_string())
}

pub fn commit(conn: &ThinConnection) -> Result<(), String> {
    runtime()
        .block_on(conn.commit())
        .map_err(|err| err.to_string())
}

pub fn rollback(conn: &ThinConnection) -> Result<(), String> {
    runtime()
        .block_on(conn.rollback())
        .map_err(|err| err.to_string())
}

pub fn apply_default_session_settings(conn: &ThinConnection) {
    let statements = [
        "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'yyyy-mm-dd hh24:mi:ss.ff6'",
        "ALTER SESSION SET NLS_DATE_FORMAT = 'yyyy-mm-dd hh24:mi:ss'",
    ];

    for statement in statements {
        let escaped_statement = statement.replace('\'', "''");
        let plsql = format!("BEGIN EXECUTE IMMEDIATE '{escaped_statement}'; END;");
        if let Err(err) = execute_plsql_with_cursor_cleanup(conn, &plsql, &[]) {
            eprintln!("Warning: failed to apply Oracle thin session setting `{statement}`: {err}");
        }
    }
}

pub fn enable_dbms_output(conn: &ThinConnection, buffer_size: Option<u32>) -> Result<(), String> {
    let sql = match buffer_size {
        Some(size) => format!("BEGIN DBMS_OUTPUT.ENABLE({size}); END;"),
        None => "BEGIN DBMS_OUTPUT.ENABLE(NULL); END;".to_string(),
    };
    execute_plsql_with_cursor_cleanup(conn, &sql, &[])?;
    Ok(())
}

pub fn disable_dbms_output(conn: &ThinConnection) -> Result<(), String> {
    execute_plsql_with_cursor_cleanup(conn, "BEGIN DBMS_OUTPUT.DISABLE; END;", &[])?;
    Ok(())
}

pub fn get_dbms_output(conn: &ThinConnection, max_lines: u32) -> Result<Vec<String>, String> {
    let params = vec![
        BindParam {
            value: Some(ThinValue::Integer(max_lines.max(1) as i64)),
            direction: ThinBindDirection::Input,
            oracle_type: ThinOracleType::Number,
            buffer_size: thin_bind_buffer_size(&BindDataType::Number),
        },
        BindParam::output(ThinOracleType::Varchar, 32_767),
    ];
    let result = execute_plsql_with_cursor_cleanup(
        conn,
        r#"
            DECLARE
              l_line   VARCHAR2(32767);
              l_status NUMBER := 0;
              l_count  NUMBER := 0;
              l_text   VARCHAR2(32767);
            BEGIN
              LOOP
                DBMS_OUTPUT.GET_LINE(l_line, l_status);
                EXIT WHEN l_status <> 0 OR l_count >= :MAX_LINES;
                l_count := l_count + 1;
                IF l_text IS NULL THEN
                  l_text := l_line;
                ELSE
                  l_text := l_text || CHR(10) || l_line;
                END IF;
              END LOOP;
              :OUTPUT_TEXT := l_text;
            END;
            "#,
        &params,
    )?;

    let output_text = result
        .out_param_indices
        .iter()
        .copied()
        .zip(result.out_values.iter())
        .find_map(|(param_index, value)| {
            if param_index == 1 {
                thin_value_to_optional_string(value)
            } else {
                None
            }
        });

    let Some(output_text) = output_text else {
        return Ok(Vec::new());
    };

    Ok(output_text
        .split('\n')
        .map(|line| line.to_string())
        .collect())
}

pub fn execute_select_streaming_with_binds<F, G>(
    conn: &ThinConnection,
    sql: &str,
    binds: &[ResolvedBind],
    on_select_start: &mut F,
    on_row: &mut G,
) -> Result<(QueryResult, bool), String>
where
    F: FnMut(&[ColumnInfo]),
    G: FnMut(Vec<String>) -> bool,
{
    let params = convert_query_params(binds)?;
    let start = Instant::now();
    let (sql_for_execution, mut normalize_internal_rowid_alias) =
        resolve_select_execution_sql(conn, sql)?;

    let mut batch = match runtime().block_on(conn.query(&sql_for_execution, &params)) {
        Ok(batch) => batch,
        Err(err) => {
            let message = err.to_string();
            if sql_for_execution != sql && should_retry_without_rowid(&message) {
                normalize_internal_rowid_alias = false;
                runtime()
                    .block_on(conn.query(sql, &params))
                    .map_err(|retry_err| retry_err.to_string())?
            } else {
                return Err(message);
            }
        }
    };
    let cursor_to_close = batch.cursor_id;

    let oracle_columns = batch.columns.clone();
    let columns = convert_columns(&oracle_columns, normalize_internal_rowid_alias);
    let column_count = oracle_columns.len();
    let has_lob_columns = oracle_columns.iter().any(|column| column.is_lob());
    on_select_start(&columns);

    let mut row_count = 0usize;
    let mut cancelled = false;

    if has_lob_columns {
        let result = fetch_complete_result(conn, cursor_to_close, batch)?;
        for row in &result.rows {
            row_count += 1;
            if !on_row(row_to_strings(conn, row, column_count)) {
                cancelled = true;
                break;
            }
        }

        return Ok((
            QueryResult::new_select_streamed(sql, columns, row_count, start.elapsed()),
            cancelled,
        ));
    }

    loop {
        for row in &batch.rows {
            row_count += 1;
            if !on_row(row_to_strings(conn, row, column_count)) {
                cancelled = true;
                break;
            }
        }

        if cancelled || !batch.has_more_rows {
            break;
        }

        batch = runtime()
            .block_on(conn.fetch_more(batch.cursor_id, &oracle_columns, 100))
            .map_err(|err| err.to_string())?;
    }

    queue_server_cursor_close(conn, cursor_to_close);

    Ok((
        QueryResult::new_select_streamed(sql, columns, row_count, start.elapsed()),
        cancelled,
    ))
}

pub fn execute_select_all_with_binds(
    conn: &ThinConnection,
    sql: &str,
    binds: &[ResolvedBind],
) -> Result<QueryResult, String> {
    let params = convert_query_params(binds)?;
    let start = Instant::now();
    let (sql_for_execution, mut normalize_internal_rowid_alias) =
        resolve_select_execution_sql(conn, sql)?;

    let result = match query_all(conn, &sql_for_execution, &params) {
        Ok(result) => result,
        Err(err) => {
            if sql_for_execution != sql && should_retry_without_rowid(&err) {
                normalize_internal_rowid_alias = false;
                query_all(conn, sql, &params)?
            } else {
                return Err(err);
            }
        }
    };

    let columns = convert_columns(&result.columns, normalize_internal_rowid_alias);
    let rows = result
        .rows
        .iter()
        .map(|row| row_to_strings(conn, row, result.columns.len()))
        .collect::<Vec<_>>();
    Ok(QueryResult::new_select(sql, columns, rows, start.elapsed()))
}

pub fn execute_statement_with_binds(
    conn: &ThinConnection,
    sql: &str,
    binds: &[ResolvedBind],
) -> Result<ThinStatementExecution, String> {
    if QueryExecutor::is_plain_commit(sql) {
        commit(conn)?;
        return Ok(ThinStatementExecution::default());
    }

    if QueryExecutor::is_plain_rollback(sql) {
        rollback(conn)?;
        return Ok(ThinStatementExecution::default());
    }

    let normalized = QueryExecutor::normalize_sql_for_execute(sql);
    let upper = normalized.to_ascii_uppercase();
    let exec_call = QueryExecutor::normalize_exec_call(sql);
    let is_plsql_block = upper.starts_with("BEGIN") || upper.starts_with("DECLARE");
    let execute_plain_plsql_directly =
        should_execute_plain_plsql_directly(sql, binds, is_plsql_block);
    let is_dml = matches!(
        QueryExecutor::leading_keyword(&normalized).as_deref(),
        Some("INSERT") | Some("UPDATE") | Some("DELETE") | Some("MERGE")
    );
    let requires_plsql = exec_call.is_some()
        || (is_plsql_block && !execute_plain_plsql_directly)
        || uses_returning_into(&upper)
        || binds
            .iter()
            .any(|bind| matches!(bind.data_type, BindDataType::RefCursor) || bind.value.is_none());

    if should_flush_pending_cursor_closes(sql, &upper) {
        flush_pending_cursor_closes(conn)?;
    }

    if requires_plsql {
        let sql_to_execute = if let Some(exec_body) = exec_call {
            ensure_plsql_terminator(&exec_body)
        } else if is_plsql_block {
            ensure_plsql_terminator(sql)
        } else {
            wrap_statement_in_plsql(sql)
        };
        let params = convert_statement_bind_params(binds, true)?;
        let result = execute_plsql_with_cursor_cleanup(conn, &sql_to_execute, &params)?;
        let execution = extract_plsql_updates(conn, result, binds, sql)?;
        flush_pending_cursor_closes(conn)?;
        return Ok(execution);
    }

    if execute_plain_plsql_directly {
        let sql_to_execute = ensure_plsql_terminator(sql);
        execute_direct_sql(conn, &sql_to_execute, &[])?;
        flush_pending_cursor_closes(conn)?;
        return Ok(ThinStatementExecution::default());
    }

    if is_dml {
        let params = convert_query_params(binds)?;
        let result = runtime()
            .block_on(conn.execute(sql, &params))
            .map_err(|err| err.to_string())?;
        queue_server_cursor_close(conn, result.cursor_id);
        flush_pending_cursor_closes(conn)?;
        return Ok(ThinStatementExecution {
            rows_affected: result.rows_affected,
            ..ThinStatementExecution::default()
        });
    }

    let params = convert_query_params(binds)?;
    let sql_to_execute = if should_ensure_compiled_ddl_terminator(sql) {
        ensure_plsql_terminator(sql)
    } else {
        sql.to_string()
    };
    execute_direct_sql(conn, &sql_to_execute, &params)?;
    flush_pending_cursor_closes(conn)?;
    Ok(ThinStatementExecution::default())
}

pub fn explain_plan(conn: &ThinConnection, sql: &str) -> Result<Vec<String>, String> {
    let explain_sql = format!("EXPLAIN PLAN FOR {sql}");
    execute_direct_sql(conn, &explain_sql, &[])?;

    let result = query_all(
        conn,
        "SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY('PLAN_TABLE', NULL, 'ALL'))",
        &[],
    )?;

    result
        .rows
        .iter()
        .map(|row| row_string(row, 0))
        .collect::<Result<Vec<_>, _>>()
}

pub fn fetch_compilation_errors(
    conn: &ThinConnection,
    object: &CompiledObject,
) -> Result<Vec<Vec<String>>, String> {
    let owner = object
        .owner
        .as_ref()
        .map(|value| value.trim().to_ascii_uppercase());

    let query_all_errors = |owner: &str| -> Result<Vec<Vec<String>>, String> {
        let result = query_all(
            conn,
            "SELECT line, position, text FROM ALL_ERRORS WHERE owner = :1 AND name = :2 AND type = :3 ORDER BY sequence",
            &[
                ThinValue::String(owner.to_string()),
                ThinValue::String(object.name.clone()),
                ThinValue::String(object.object_type.clone()),
            ],
        )?;
        result
            .rows
            .iter()
            .map(|row| {
                Ok(vec![
                    row_string(row, 0)?,
                    row_string(row, 1)?,
                    row_string(row, 2)?,
                ])
            })
            .collect::<Result<Vec<_>, String>>()
    };

    let query_user_errors = || -> Result<Vec<Vec<String>>, String> {
        let result = query_all(
            conn,
            "SELECT line, position, text FROM USER_ERRORS WHERE name = :1 AND type = :2 ORDER BY sequence",
            &[
                ThinValue::String(object.name.clone()),
                ThinValue::String(object.object_type.clone()),
            ],
        )?;
        result
            .rows
            .iter()
            .map(|row| {
                Ok(vec![
                    row_string(row, 0)?,
                    row_string(row, 1)?,
                    row_string(row, 2)?,
                ])
            })
            .collect::<Result<Vec<_>, String>>()
    };

    match owner {
        Some(ref owner_name) => query_all_errors(owner_name).or_else(|err| {
            let message = err.to_ascii_uppercase();
            if message.contains("ORA-00942")
                || message.contains("ORA-01031")
                || message.contains("ORA-00904")
            {
                let current_user = show_user(conn)?.to_ascii_uppercase();
                if current_user == *owner_name {
                    query_user_errors()
                } else {
                    Err(err)
                }
            } else {
                Err(err)
            }
        }),
        None => query_user_errors(),
    }
}

pub fn show_user(conn: &ThinConnection) -> Result<String, String> {
    query_single_string(conn, "SELECT USER FROM DUAL", &[])
}

pub fn get_tables(conn: &ThinConnection) -> Result<Vec<String>, String> {
    query_object_list(
        conn,
        r#"
            SELECT table_name FROM (
                SELECT table_name
                FROM all_tables
                WHERE owner = USER
                  AND table_name > :1
                ORDER BY table_name
            )
            WHERE ROWNUM <= 200
        "#,
    )
}

pub fn get_views(conn: &ThinConnection) -> Result<Vec<String>, String> {
    query_object_list(
        conn,
        r#"
            SELECT view_name FROM (
                SELECT view_name
                FROM all_views
                WHERE owner = USER
                  AND view_name > :1
                ORDER BY view_name
            )
            WHERE ROWNUM <= 200
        "#,
    )
}

pub fn get_procedures(conn: &ThinConnection) -> Result<Vec<String>, String> {
    query_object_list(
        conn,
        r#"
            SELECT object_name FROM (
                SELECT object_name
                FROM user_procedures
                WHERE object_type = 'PROCEDURE'
                  AND object_name > :1
                ORDER BY object_name
            )
            WHERE ROWNUM <= 200
        "#,
    )
}

pub fn get_functions(conn: &ThinConnection) -> Result<Vec<String>, String> {
    query_object_list(
        conn,
        r#"
            SELECT object_name FROM (
                SELECT object_name
                FROM user_procedures
                WHERE object_type = 'FUNCTION'
                  AND object_name > :1
                ORDER BY object_name
            )
            WHERE ROWNUM <= 200
        "#,
    )
}

pub fn get_sequences(conn: &ThinConnection) -> Result<Vec<String>, String> {
    query_object_list(
        conn,
        r#"
            SELECT sequence_name FROM (
                SELECT sequence_name
                FROM user_sequences
                WHERE sequence_name > :1
                ORDER BY sequence_name
            )
            WHERE ROWNUM <= 200
        "#,
    )
}

pub fn get_triggers(conn: &ThinConnection) -> Result<Vec<String>, String> {
    query_object_list(
        conn,
        r#"
            SELECT trigger_name FROM (
                SELECT trigger_name
                FROM user_triggers
                WHERE trigger_name > :1
                ORDER BY trigger_name
            )
            WHERE ROWNUM <= 200
        "#,
    )
}

pub fn get_synonyms(conn: &ThinConnection) -> Result<Vec<String>, String> {
    query_object_list(
        conn,
        r#"
            SELECT synonym_name FROM (
                SELECT synonym_name
                FROM user_synonyms
                WHERE synonym_name > :1
                ORDER BY synonym_name
            )
            WHERE ROWNUM <= 200
        "#,
    )
}

pub fn get_packages(conn: &ThinConnection) -> Result<Vec<String>, String> {
    query_object_list(
        conn,
        r#"
            SELECT object_name FROM (
                SELECT object_name
                FROM user_objects
                WHERE object_type = 'PACKAGE'
                  AND object_name > :1
                ORDER BY object_name
            )
            WHERE ROWNUM <= 200
        "#,
    )
}

fn ascii_keyword_at(haystack: &[u8], start: usize, keyword: &[u8]) -> bool {
    haystack
        .get(start..start + keyword.len())
        .map(|slice| slice.eq_ignore_ascii_case(keyword))
        .unwrap_or(false)
}

fn parse_package_spec_routines(source: &str) -> Vec<PackageRoutine> {
    let mut routines = Vec::new();
    let mut seen = HashSet::new();
    let bytes = source.as_bytes();
    let len = bytes.len();
    let mut i = 0usize;

    while i < len {
        if i + 1 < len && bytes[i] == b'-' && bytes[i + 1] == b'-' {
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        if i + 1 < len && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = i.saturating_add(2);
            continue;
        }

        if bytes[i] == b'\'' {
            i += 1;
            while i < len {
                if bytes[i] == b'\'' {
                    i += 1;
                    if i < len && bytes[i] == b'\'' {
                        i += 1;
                    } else {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            continue;
        }

        let (keyword_len, routine_type) = if ascii_keyword_at(bytes, i, b"PROCEDURE") {
            (9usize, "PROCEDURE")
        } else if ascii_keyword_at(bytes, i, b"FUNCTION") {
            (8usize, "FUNCTION")
        } else {
            i += 1;
            continue;
        };

        if i > 0 && sql_text::is_identifier_byte(bytes[i - 1]) {
            i += keyword_len;
            continue;
        }

        let after = i + keyword_len;
        if after < len && sql_text::is_identifier_byte(bytes[after]) {
            i += keyword_len;
            continue;
        }

        let mut j = after;
        while j < len && bytes[j].is_ascii_whitespace() {
            j += 1;
        }

        let name_start = j;
        if j < len && bytes[j] == b'"' {
            j += 1;
            let quoted_start = j;
            while j < len && bytes[j] != b'"' {
                j += 1;
            }
            let name = source.get(quoted_start..j).unwrap_or("").to_uppercase();
            if !name.is_empty() && seen.insert(name.clone()) {
                routines.push(PackageRoutine {
                    name,
                    routine_type: routine_type.to_string(),
                });
            }
            i = j.saturating_add(1);
            continue;
        }

        while j < len
            && (bytes[j].is_ascii_alphanumeric()
                || bytes[j] == b'_'
                || bytes[j] == b'$'
                || bytes[j] == b'#')
        {
            j += 1;
        }

        if j > name_start {
            let name = source
                .get(name_start..j)
                .unwrap_or("")
                .trim()
                .to_uppercase();
            if !name.is_empty() && seen.insert(name.clone()) {
                routines.push(PackageRoutine {
                    name,
                    routine_type: routine_type.to_string(),
                });
            }
        }

        i = j;
    }

    routines.sort_by(|left, right| left.name.cmp(&right.name));
    routines
}

pub fn get_package_routines(
    conn: &ThinConnection,
    package_name: &str,
) -> Result<Vec<PackageRoutine>, String> {
    let package_name = package_name.to_ascii_uppercase();
    let source = query_all(
        conn,
        "SELECT text FROM user_source WHERE name = :1 AND type = 'PACKAGE' ORDER BY line",
        &[ThinValue::String(package_name.clone())],
    )?;

    let mut source_text = String::new();
    for row in &source.rows {
        source_text.push_str(&row_string(row, 0)?);
    }

    let parsed = parse_package_spec_routines(&source_text);
    if !parsed.is_empty() {
        return Ok(parsed);
    }

    let rows = query_all(
        conn,
        r#"
            SELECT DISTINCT
                p.procedure_name,
                CASE
                    WHEN EXISTS (
                        SELECT 1 FROM user_arguments a
                        WHERE a.package_name = p.object_name
                          AND a.object_name = p.procedure_name
                          AND a.position = 0
                          AND (a.overload = p.overload OR (a.overload IS NULL AND p.overload IS NULL))
                    ) THEN 'FUNCTION'
                    ELSE 'PROCEDURE'
                END AS routine_type
            FROM user_procedures p
            WHERE p.object_type = 'PACKAGE'
              AND p.object_name = :1
              AND p.procedure_name IS NOT NULL
            ORDER BY p.procedure_name
        "#,
        &[ThinValue::String(package_name)],
    )?;

    rows.rows
        .iter()
        .map(|row| {
            Ok(PackageRoutine {
                name: row_string(row, 0)?,
                routine_type: row_string(row, 1)?,
            })
        })
        .collect::<Result<Vec<_>, String>>()
}

pub fn get_procedure_arguments(
    conn: &ThinConnection,
    procedure_name: &str,
) -> Result<Vec<ProcedureArgument>, String> {
    get_procedure_arguments_inner(conn, None, procedure_name)
}

pub fn get_package_procedure_arguments(
    conn: &ThinConnection,
    package_name: &str,
    procedure_name: &str,
) -> Result<Vec<ProcedureArgument>, String> {
    get_procedure_arguments_inner(conn, Some(package_name), procedure_name)
}

fn get_procedure_arguments_inner(
    conn: &ThinConnection,
    package_name: Option<&str>,
    procedure_name: &str,
) -> Result<Vec<ProcedureArgument>, String> {
    let (sql, params) = if let Some(package_name) = package_name {
        (
            r#"
                SELECT
                    argument_name,
                    position,
                    sequence,
                    data_type,
                    in_out,
                    data_length,
                    data_precision,
                    data_scale,
                    type_owner,
                    type_name,
                    pls_type,
                    overload,
                    default_value
                FROM user_arguments
                WHERE package_name = :1
                  AND object_name = :2
                ORDER BY NVL(overload, 0), position, sequence
            "#,
            vec![
                ThinValue::String(package_name.to_ascii_uppercase()),
                ThinValue::String(procedure_name.to_ascii_uppercase()),
            ],
        )
    } else {
        (
            r#"
                SELECT
                    argument_name,
                    position,
                    sequence,
                    data_type,
                    in_out,
                    data_length,
                    data_precision,
                    data_scale,
                    type_owner,
                    type_name,
                    pls_type,
                    overload,
                    default_value
                FROM user_arguments
                WHERE package_name IS NULL
                  AND object_name = :1
                ORDER BY NVL(overload, 0), position, sequence
            "#,
            vec![ThinValue::String(procedure_name.to_ascii_uppercase())],
        )
    };

    let result = query_all(conn, sql, &params)?;
    result
        .rows
        .iter()
        .map(|row| {
            Ok(ProcedureArgument {
                name: row_optional_string(row, 0)?,
                position: thin_value_to_i32(row_value(row, 1)?)?,
                sequence: thin_value_to_i32(row_value(row, 2)?)?,
                data_type: row_optional_string(row, 3)?,
                in_out: row_optional_string(row, 4)?,
                data_length: row_optional_i32(row, 5)?,
                data_precision: row_optional_i32(row, 6)?,
                data_scale: row_optional_i32(row, 7)?,
                type_owner: row_optional_string(row, 8)?,
                type_name: row_optional_string(row, 9)?,
                pls_type: row_optional_string(row, 10)?,
                overload: row_optional_i32(row, 11)?,
                default_value: row_optional_string(row, 12)?,
            })
        })
        .collect::<Result<Vec<_>, String>>()
}

pub fn get_table_columns(
    conn: &ThinConnection,
    table_name: &str,
) -> Result<Vec<ColumnInfo>, String> {
    let result = query_all(
        conn,
        "SELECT column_name, data_type FROM user_tab_columns WHERE table_name = :1 ORDER BY column_id",
        &[ThinValue::String(table_name.to_ascii_uppercase())],
    )?;
    result
        .rows
        .iter()
        .map(|row| {
            Ok(ColumnInfo {
                name: row_string(row, 0)?,
                data_type: row_string(row, 1)?,
            })
        })
        .collect::<Result<Vec<_>, String>>()
}

pub fn get_object_types(conn: &ThinConnection, object_name: &str) -> Result<Vec<String>, String> {
    let result = query_all(
        conn,
        "SELECT DISTINCT object_type FROM user_objects WHERE object_name = :1",
        &[ThinValue::String(object_name.to_ascii_uppercase())],
    )?;
    result
        .rows
        .iter()
        .map(|row| row_string(row, 0))
        .collect::<Result<Vec<_>, _>>()
}

pub fn get_object_status(
    conn: &ThinConnection,
    object_name: &str,
    object_type: &str,
) -> Result<String, String> {
    query_single_string(
        conn,
        "SELECT status FROM user_objects WHERE object_name = :1 AND object_type = :2",
        &[
            ThinValue::String(object_name.to_ascii_uppercase()),
            ThinValue::String(object_type.to_ascii_uppercase()),
        ],
    )
}

pub fn get_compilation_errors(
    conn: &ThinConnection,
    object_name: &str,
    object_type: &str,
) -> Result<Vec<CompilationError>, String> {
    let result = query_all(
        conn,
        "SELECT line, position, text, attribute FROM user_errors WHERE name = :1 AND type = :2 ORDER BY sequence",
        &[
            ThinValue::String(object_name.to_ascii_uppercase()),
            ThinValue::String(object_type.to_ascii_uppercase()),
        ],
    )?;

    result
        .rows
        .iter()
        .map(|row| {
            Ok(CompilationError {
                line: row_optional_i32(row, 0)?.unwrap_or(0),
                position: row_optional_i32(row, 1)?.unwrap_or(0),
                text: row_optional_string(row, 2)?
                    .unwrap_or_default()
                    .trim()
                    .to_string(),
                attribute: row_optional_string(row, 3)?.unwrap_or_default(),
            })
        })
        .collect::<Result<Vec<_>, String>>()
}

pub fn get_table_structure(
    conn: &ThinConnection,
    table_name: &str,
) -> Result<Vec<TableColumnDetail>, String> {
    let table_name = table_name.to_ascii_uppercase();
    let params = [ThinValue::String(table_name)];

    let with_default_vc = query_all(
        conn,
        r#"
            SELECT
                c.column_name,
                c.data_type,
                c.data_length,
                c.data_precision,
                c.data_scale,
                c.nullable,
                c.data_default_vc,
                (SELECT 'PK' FROM user_cons_columns cc
                 JOIN user_constraints con ON cc.constraint_name = con.constraint_name
                 WHERE con.constraint_type = 'P'
                   AND cc.table_name = c.table_name
                   AND cc.column_name = c.column_name
                   AND ROWNUM = 1) as is_pk
            FROM user_tab_cols c
            WHERE c.table_name = :1
            ORDER BY c.column_id
        "#,
        &params,
    );

    let (result, has_default_column) = match with_default_vc {
        Ok(result) => (result, true),
        Err(err) => {
            let upper = err.to_ascii_uppercase();
            if !upper.contains("ORA-00904") && !upper.contains("ORA-00942") {
                return Err(err);
            }

            let fallback = query_all(
                conn,
                r#"
                    SELECT
                        c.column_name,
                        c.data_type,
                        c.data_length,
                        c.data_precision,
                        c.data_scale,
                        c.nullable,
                        (SELECT 'PK' FROM user_cons_columns cc
                         JOIN user_constraints con ON cc.constraint_name = con.constraint_name
                         WHERE con.constraint_type = 'P'
                           AND cc.table_name = c.table_name
                           AND cc.column_name = c.column_name
                           AND ROWNUM = 1) as is_pk
                    FROM user_tab_columns c
                    WHERE c.table_name = :1
                    ORDER BY c.column_id
                "#,
                &params,
            )?;
            (fallback, false)
        }
    };

    result
        .rows
        .iter()
        .map(|row| {
            let default_value = if has_default_column {
                row_optional_string(row, 6)?
            } else {
                None
            };
            let pk_index = if has_default_column { 7 } else { 6 };
            Ok(TableColumnDetail {
                name: row_string(row, 0)?,
                data_type: row_string(row, 1)?,
                data_length: row_optional_i32(row, 2)?.unwrap_or(0),
                data_precision: row_optional_i32(row, 3)?,
                data_scale: row_optional_i32(row, 4)?,
                nullable: thin_value_to_bool_flag(row_value(row, 5)?, "Y"),
                default_value,
                is_primary_key: row_optional_string(row, pk_index)?.is_some(),
            })
        })
        .collect::<Result<Vec<_>, String>>()
}

pub fn get_table_indexes(
    conn: &ThinConnection,
    table_name: &str,
) -> Result<Vec<IndexInfo>, String> {
    let result = query_all(
        conn,
        r#"
            SELECT
                i.index_name,
                i.uniqueness,
                LISTAGG(ic.column_name, ', ') WITHIN GROUP (ORDER BY ic.column_position) as columns
            FROM user_indexes i
            JOIN user_ind_columns ic ON i.index_name = ic.index_name
            WHERE i.table_name = :1
            GROUP BY i.index_name, i.uniqueness
            ORDER BY i.index_name
        "#,
        &[ThinValue::String(table_name.to_ascii_uppercase())],
    )?;

    result
        .rows
        .iter()
        .map(|row| {
            Ok(IndexInfo {
                name: row_string(row, 0)?,
                is_unique: thin_value_to_bool_flag(row_value(row, 1)?, "UNIQUE"),
                columns: row_string(row, 2)?,
            })
        })
        .collect::<Result<Vec<_>, String>>()
}

pub fn get_table_constraints(
    conn: &ThinConnection,
    table_name: &str,
) -> Result<Vec<ConstraintInfo>, String> {
    let result = query_all(
        conn,
        r#"
            SELECT
                c.constraint_name,
                c.constraint_type,
                LISTAGG(cc.column_name, ', ') WITHIN GROUP (ORDER BY cc.position) as columns,
                c.r_constraint_name,
                (SELECT table_name FROM user_constraints WHERE constraint_name = c.r_constraint_name) as ref_table
            FROM user_constraints c
            LEFT JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name
            WHERE c.table_name = :1
            GROUP BY c.constraint_name, c.constraint_type, c.r_constraint_name
            ORDER BY c.constraint_type, c.constraint_name
        "#,
        &[ThinValue::String(table_name.to_ascii_uppercase())],
    )?;

    result
        .rows
        .iter()
        .map(|row| {
            let constraint_type_raw = row_string(row, 1)?;
            Ok(ConstraintInfo {
                name: row_string(row, 0)?,
                constraint_type: match constraint_type_raw.as_str() {
                    "P" => "PRIMARY KEY".to_string(),
                    "R" => "FOREIGN KEY".to_string(),
                    "U" => "UNIQUE".to_string(),
                    "C" => "CHECK".to_string(),
                    _ => constraint_type_raw,
                },
                columns: row_optional_string(row, 2)?.unwrap_or_default(),
                ref_table: row_optional_string(row, 4)?,
            })
        })
        .collect::<Result<Vec<_>, String>>()
}

pub fn get_sequence_info(
    conn: &ThinConnection,
    sequence_name: &str,
) -> Result<SequenceInfo, String> {
    let result = query_all(
        conn,
        r#"
            SELECT
                sequence_name,
                TO_CHAR(min_value),
                TO_CHAR(max_value),
                TO_CHAR(increment_by),
                cycle_flag,
                order_flag,
                TO_CHAR(cache_size),
                TO_CHAR(last_number)
            FROM user_sequences
            WHERE sequence_name = :1
        "#,
        &[ThinValue::String(sequence_name.to_ascii_uppercase())],
    )?;

    let row = result
        .rows
        .first()
        .ok_or_else(|| format!("sequence not found: {}", sequence_name.to_ascii_uppercase()))?;

    Ok(SequenceInfo {
        name: row_string(row, 0)?,
        min_value: row_string(row, 1)?,
        max_value: row_string(row, 2)?,
        increment_by: row_string(row, 3)?,
        cycle_flag: row_string(row, 4)?,
        order_flag: row_string(row, 5)?,
        cache_size: row_string(row, 6)?,
        last_number: row_string(row, 7)?,
    })
}

pub fn get_synonym_info(conn: &ThinConnection, synonym_name: &str) -> Result<SynonymInfo, String> {
    let result = query_all(
        conn,
        r#"
            SELECT
                synonym_name,
                table_owner,
                table_name,
                db_link
            FROM user_synonyms
            WHERE synonym_name = :1
        "#,
        &[ThinValue::String(synonym_name.to_ascii_uppercase())],
    )?;

    let row = result
        .rows
        .first()
        .ok_or_else(|| format!("synonym not found: {}", synonym_name.to_ascii_uppercase()))?;

    Ok(SynonymInfo {
        name: row_string(row, 0)?,
        table_owner: row_optional_string(row, 1)?.unwrap_or_default(),
        table_name: row_optional_string(row, 2)?.unwrap_or_default(),
        db_link: row_optional_string(row, 3)?.unwrap_or_default(),
    })
}

fn get_metadata_ddl(
    conn: &ThinConnection,
    object_type: &str,
    object_name: &str,
) -> Result<String, String> {
    let chunks = query_all(
        conn,
        r#"
            SELECT DBMS_LOB.SUBSTR(ddl, 32767, 1 + ((LEVEL - 1) * 32767)) AS ddl_chunk
            FROM (
                SELECT DBMS_METADATA.GET_DDL(:1, :2) AS ddl
                FROM DUAL
            )
            CONNECT BY LEVEL <= CEIL(DBMS_LOB.GETLENGTH(ddl) / 32767)
        "#,
        &[
            ThinValue::String(object_type.to_string()),
            ThinValue::String(object_name.to_ascii_uppercase()),
        ],
    )?;

    let mut ddl = String::new();
    for row in &chunks.rows {
        ddl.push_str(&row_string(row, 0)?);
    }

    Ok(normalize_generated_ddl(ddl))
}

pub fn get_table_ddl(conn: &ThinConnection, table_name: &str) -> Result<String, String> {
    get_metadata_ddl(conn, "TABLE", table_name)
}

pub fn get_view_ddl(conn: &ThinConnection, view_name: &str) -> Result<String, String> {
    get_metadata_ddl(conn, "VIEW", view_name)
}

pub fn get_procedure_ddl(conn: &ThinConnection, procedure_name: &str) -> Result<String, String> {
    get_metadata_ddl(conn, "PROCEDURE", procedure_name)
}

pub fn get_function_ddl(conn: &ThinConnection, function_name: &str) -> Result<String, String> {
    get_metadata_ddl(conn, "FUNCTION", function_name)
}

pub fn get_sequence_ddl(conn: &ThinConnection, sequence_name: &str) -> Result<String, String> {
    get_metadata_ddl(conn, "SEQUENCE", sequence_name)
}

pub fn get_synonym_ddl(conn: &ThinConnection, synonym_name: &str) -> Result<String, String> {
    get_metadata_ddl(conn, "SYNONYM", synonym_name)
}

pub fn get_package_spec_ddl(conn: &ThinConnection, package_name: &str) -> Result<String, String> {
    get_metadata_ddl(conn, "PACKAGE", package_name)
}

pub fn get_object_ddl(
    conn: &ThinConnection,
    object_type: &str,
    object_name: &str,
) -> Result<String, String> {
    get_metadata_ddl(conn, object_type, object_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::connection::DatabaseType;
    use crate::db::{ScriptItem, ToolCommand};
    use std::env;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_connection_info() -> ConnectionInfo {
        ConnectionInfo {
            name: "oracle-thin-test".to_string(),
            username: env::var("ORACLE_TEST_USER").unwrap_or_else(|_| "system".to_string()),
            password: env::var("ORACLE_TEST_PASSWORD").unwrap_or_else(|_| "password".to_string()),
            host: env::var("ORACLE_TEST_HOST").unwrap_or_else(|_| "localhost".to_string()),
            port: env::var("ORACLE_TEST_PORT")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1521),
            service_name: env::var("ORACLE_TEST_SERVICE").unwrap_or_else(|_| "FREE".to_string()),
            db_type: DatabaseType::OracleThin,
        }
    }

    fn unique_object_name(prefix: &str) -> String {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must be after UNIX_EPOCH")
            .as_millis();
        format!("{prefix}_{:X}_{:X}", std::process::id(), millis).to_ascii_uppercase()
    }

    #[test]
    #[ignore = "requires local Oracle XE at localhost:1521/FREE"]
    fn oracle_thin_connects_and_executes_plsql_features() {
        let info = test_connection_info();
        let conn = connect(&info).expect("oracle thin connection must succeed");

        let user = show_user(conn.as_ref()).expect("SHOW USER must succeed");
        assert!(
            !user.trim().is_empty(),
            "connected user should not be empty"
        );

        let select_result =
            execute_select_all_with_binds(conn.as_ref(), "SELECT 1 AS VALUE FROM dual", &[])
                .expect("basic SELECT must succeed");
        assert_eq!(select_result.row_count, 1);
        let value_index = select_result
            .columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case("VALUE"))
            .expect("VALUE column should exist");
        assert_eq!(select_result.rows[0][value_index], "1");

        let scalar_binds = vec![ResolvedBind {
            name: "OUT_MSG".to_string(),
            data_type: BindDataType::Varchar2(128),
            value: None,
        }];
        let scalar_result = execute_statement_with_binds(
            conn.as_ref(),
            "BEGIN :OUT_MSG := 'hello thin'; END;",
            &scalar_binds,
        )
        .expect("PL/SQL OUT bind must succeed");
        assert_eq!(scalar_result.scalar_updates.len(), 1);
        match &scalar_result.scalar_updates[0].1 {
            BindValue::Scalar(Some(value)) => assert_eq!(value, "hello thin"),
            other => panic!("unexpected OUT bind result: {other:?}"),
        }

        let cursor_binds = vec![ResolvedBind {
            name: "OUT_CURSOR".to_string(),
            data_type: BindDataType::RefCursor,
            value: None,
        }];
        let cursor_result = execute_statement_with_binds(
            conn.as_ref(),
            "BEGIN OPEN :OUT_CURSOR FOR SELECT 7 AS ID, 'ALPHA' AS NAME FROM dual; END;",
            &cursor_binds,
        )
        .expect("REF CURSOR bind must succeed");
        assert_eq!(cursor_result.ref_cursors.len(), 1);
        assert_eq!(cursor_result.ref_cursors[0].1.columns, vec!["ID", "NAME"]);
        assert_eq!(cursor_result.ref_cursors[0].1.rows[0], vec!["7", "ALPHA"]);

        let implicit_result = execute_statement_with_binds(
            conn.as_ref(),
            r#"
            DECLARE
                c SYS_REFCURSOR;
            BEGIN
                OPEN c FOR SELECT 42 AS VALUE FROM dual;
                DBMS_SQL.RETURN_RESULT(c);
            END;
            "#,
            &[],
        )
        .expect("implicit result PL/SQL must succeed");
        assert_eq!(implicit_result.implicit_results.len(), 1);
        assert_eq!(implicit_result.implicit_results[0].rows[0][0], "42");

        enable_dbms_output(conn.as_ref(), Some(1_000_000))
            .expect("DBMS_OUTPUT enable must succeed");
        execute_statement_with_binds(
            conn.as_ref(),
            "BEGIN DBMS_OUTPUT.PUT_LINE('thin output ok'); END;",
            &[],
        )
        .expect("DBMS_OUTPUT PL/SQL must succeed");
        let output_lines =
            get_dbms_output(conn.as_ref(), 10).expect("DBMS_OUTPUT fetch must succeed");
        assert_eq!(output_lines, vec!["thin output ok"]);
        disable_dbms_output(conn.as_ref()).expect("DBMS_OUTPUT disable must succeed");

        close(conn.as_ref()).expect("connection close must succeed");
    }

    #[test]
    #[ignore = "requires local Oracle XE at localhost:1521/FREE"]
    fn oracle_thin_flushes_pending_select_cursors_before_cleanup_drop_blocks() {
        let info = test_connection_info();
        let conn = connect(&info).expect("oracle thin connection must succeed");

        execute_statement_with_binds(
            conn.as_ref(),
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE thin_drop_probe PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;",
            &[],
        )
        .expect("stale drop cleanup must succeed");
        execute_statement_with_binds(
            conn.as_ref(),
            "CREATE TABLE thin_drop_probe (id NUMBER PRIMARY KEY, note VARCHAR2(30))",
            &[],
        )
        .expect("probe table creation must succeed");
        execute_statement_with_binds(
            conn.as_ref(),
            "INSERT INTO thin_drop_probe (id, note) VALUES (1, 'probe')",
            &[],
        )
        .expect("probe row insert must succeed");

        let select_result = execute_select_all_with_binds(
            conn.as_ref(),
            "SELECT COUNT(*) AS cnt FROM thin_drop_probe",
            &[],
        )
        .expect("probe select must succeed");
        assert_eq!(select_result.rows[0][0], "1");

        execute_statement_with_binds(
            conn.as_ref(),
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE thin_drop_probe PURGE'; END;",
            &[],
        )
        .expect("drop block after select must succeed");

        close(conn.as_ref()).expect("connection close must succeed");
    }

    #[test]
    #[ignore = "requires local Oracle XE at localhost:1521/FREE"]
    fn oracle_thin_repeated_clob_select_and_drop_blocks_remain_usable() {
        let info = test_connection_info();
        let conn = connect(&info).expect("oracle thin connection must succeed");

        for iteration in 0..40 {
            execute_statement_with_binds(
                conn.as_ref(),
                "BEGIN EXECUTE IMMEDIATE 'DROP TABLE thin_clob_drop_probe PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;",
                &[],
            )
            .expect("stale CLOB probe drop cleanup must succeed");
            execute_statement_with_binds(
                conn.as_ref(),
                "CREATE TABLE thin_clob_drop_probe (id NUMBER PRIMARY KEY, detail_text CLOB)",
                &[],
            )
            .expect("CLOB probe table creation must succeed");
            execute_statement_with_binds(
                conn.as_ref(),
                &format!(
                    "INSERT INTO thin_clob_drop_probe (id, detail_text) VALUES (1, 'clob probe iteration {iteration}')"
                ),
                &[],
            )
            .expect("CLOB probe insert must succeed");

            let select_result = execute_select_all_with_binds(
                conn.as_ref(),
                "SELECT detail_text FROM thin_clob_drop_probe",
                &[],
            )
            .expect("CLOB probe select must succeed");
            assert_eq!(select_result.rows.len(), 1);
            assert!(
                select_result.rows[0]
                    .iter()
                    .any(|value| value.contains("clob probe iteration")),
                "CLOB content must be materialized as text: {:?}",
                select_result.rows
            );

            execute_statement_with_binds(
                conn.as_ref(),
                "BEGIN EXECUTE IMMEDIATE 'DROP TABLE thin_clob_drop_probe PURGE'; END;",
                &[],
            )
            .expect("CLOB probe drop block must succeed");
        }

        close(conn.as_ref()).expect("connection close must succeed");
    }

    #[test]
    #[ignore = "requires local Oracle XE at localhost:1521/FREE"]
    fn oracle_thin_reads_metadata_and_compilation_errors() {
        let info = test_connection_info();
        let conn = connect(&info).expect("oracle thin connection must succeed");

        let table_name = unique_object_name("THIN_META");
        let procedure_name = unique_object_name("THIN_PROC");

        execute_statement_with_binds(
            conn.as_ref(),
            &format!(
                "CREATE TABLE {table_name} (ID NUMBER PRIMARY KEY, NAME VARCHAR2(40), CREATED_AT DATE)"
            ),
            &[],
        )
        .expect("test table creation must succeed");

        let created_object = query_all(
            conn.as_ref(),
            "SELECT object_name FROM user_objects WHERE object_name = :1 AND object_type = 'TABLE'",
            &[ThinValue::String(table_name.clone())],
        )
        .expect("created table lookup must load");
        assert_eq!(
            created_object.rows.len(),
            1,
            "created table should be visible in USER_OBJECTS"
        );

        let columns =
            get_table_columns(conn.as_ref(), &table_name).expect("table columns must load");
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0].name, "ID");

        let structure =
            get_table_structure(conn.as_ref(), &table_name).expect("table structure must load");
        assert_eq!(structure.len(), 3);
        assert_eq!(structure[0].name, "ID");
        assert!(structure[0].is_primary_key);

        let ddl = get_table_ddl(conn.as_ref(), &table_name).expect("table DDL must load");
        assert!(
            ddl.to_ascii_uppercase().contains(&table_name),
            "DDL should mention the created table"
        );

        execute_statement_with_binds(
            conn.as_ref(),
            &format!(
                "CREATE OR REPLACE PROCEDURE {procedure_name} IS BEGIN missing_symbol := 1; END;"
            ),
            &[],
        )
        .expect("invalid procedure should still compile with stored errors");

        let status =
            get_object_status(conn.as_ref(), &procedure_name, "PROCEDURE").expect("status load");
        assert!(
            status.eq_ignore_ascii_case("INVALID") || status.eq_ignore_ascii_case("VALID"),
            "unexpected procedure status: {status}"
        );

        let errors = get_compilation_errors(conn.as_ref(), &procedure_name, "PROCEDURE")
            .expect("compilation errors must load");
        assert!(
            !errors.is_empty(),
            "invalid procedure should produce compilation errors"
        );

        let _ = execute_statement_with_binds(
            conn.as_ref(),
            &format!("DROP PROCEDURE {procedure_name}"),
            &[],
        );
        let _ = execute_statement_with_binds(
            conn.as_ref(),
            &format!("DROP TABLE {table_name} PURGE"),
            &[],
        );
        let _ = close(conn.as_ref());
    }

    #[test]
    #[ignore = "requires local Oracle XE at localhost:1521/FREE"]
    fn oracle_thin_interrupts_long_running_plsql() {
        let info = test_connection_info();
        let conn = connect(&info).expect("oracle thin connection must succeed");
        let worker_conn = Arc::clone(&conn);

        let handle = std::thread::spawn(move || {
            execute_statement_with_binds(
                worker_conn.as_ref(),
                "BEGIN DBMS_LOCK.SLEEP(30); END;",
                &[],
            )
        });

        std::thread::sleep(Duration::from_millis(750));
        interrupt(conn.as_ref()).expect("interrupt marker must be sent");

        let result = handle
            .join()
            .expect("worker thread should join without panicking");
        let message = result
            .err()
            .expect("long-running PL/SQL should be interrupted")
            .to_ascii_uppercase();
        assert!(
            message.contains("ORA-01013")
                || message.contains("CANCEL")
                || message.contains("INTERRUPT"),
            "unexpected interrupt error: {message}"
        );
        let value = query_single_string(conn.as_ref(), "SELECT 1 FROM dual", &[])
            .expect("connection must remain usable after interrupt");
        assert_eq!(value.trim(), "1");

        let _ = close(conn.as_ref());
    }

    #[test]
    #[ignore = "requires local Oracle XE at localhost:1521/FREE"]
    fn oracle_thin_creates_valid_simple_function() {
        let info = test_connection_info();
        let conn = connect(&info).expect("oracle thin connection must succeed");

        let _ = execute_statement_with_binds(
            conn.as_ref(),
            "BEGIN EXECUTE IMMEDIATE 'DROP FUNCTION oqt_f_add'; EXCEPTION WHEN OTHERS THEN NULL; END;",
            &[],
        );

        execute_statement_with_binds(
            conn.as_ref(),
            r#"
            CREATE OR REPLACE FUNCTION oqt_f_add(p_a NUMBER, p_b NUMBER)
            RETURN NUMBER
            IS
            BEGIN
              RETURN NVL(p_a,0) + NVL(p_b,0);
            END;
            "#,
            &[],
        )
        .expect("simple function creation must succeed");

        let status =
            get_object_status(conn.as_ref(), "OQT_F_ADD", "FUNCTION").expect("status load");
        let errors = get_compilation_errors(conn.as_ref(), "OQT_F_ADD", "FUNCTION")
            .expect("compilation errors load");
        assert!(
            status.eq_ignore_ascii_case("VALID"),
            "simple function must be VALID, got status={status}, errors={errors:?}"
        );
        assert!(
            errors.is_empty(),
            "simple function must not have compilation errors: {errors:?}"
        );

        let rows = execute_select_all_with_binds(
            conn.as_ref(),
            "SELECT oqt_f_add(10, 20) AS add_10_20 FROM dual",
            &[],
        )
        .expect("simple function call must succeed");
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0].len(), 2, "ROWID + function result expected");
        let direct_value = rows.rows[0][1].trim().to_string();

        let mut streaming_columns = Vec::new();
        let mut streaming_rows = Vec::new();
        let streaming = execute_select_streaming_with_binds(
            conn.as_ref(),
            "SELECT oqt_f_add(10, 20) AS add_10_20 FROM dual",
            &[],
            &mut |columns| streaming_columns = columns.to_vec(),
            &mut |row| {
                streaming_rows.push(row);
                true
            },
        );
        let streaming_result = streaming
            .map(|(_, cancelled)| cancelled)
            .map_err(|err| err.to_string());

        assert_eq!(
            direct_value, "30",
            "direct function result mismatch: direct_value={direct_value}, streaming_columns={streaming_columns:?}, streaming_rows={streaming_rows:?}, streaming_result={streaming_result:?}"
        );
        assert_eq!(
            streaming_result,
            Ok(false),
            "streaming function select must succeed without cancellation"
        );
        assert_eq!(streaming_rows.len(), 1);
        assert_eq!(streaming_rows[0].len(), 2, "ROWID + function result expected");
        assert_eq!(streaming_rows[0][1].trim(), "30");

        let _ = execute_statement_with_binds(conn.as_ref(), "DROP FUNCTION oqt_f_add", &[]);
        let _ = close(conn.as_ref());
    }

    #[test]
    #[ignore = "requires local Oracle XE at localhost:1521/FREE"]
    fn oracle_thin_executes_test3_prefix_then_function_select() {
        use crate::db::{ScriptItem, ToolCommand};

        let info = test_connection_info();
        let conn = connect(&info).expect("oracle thin connection must succeed");
        let items = QueryExecutor::split_script_items_for_db_type(
            include_str!("../../test/test3.txt"),
            Some(DatabaseType::OracleThin),
        );

        for item in items {
            match item {
                ScriptItem::Statement(statement) => {
                    let cleaned = QueryExecutor::strip_leading_comments(&statement);
                    if cleaned.is_empty() {
                        continue;
                    }

                    if QueryExecutor::is_select_statement(&cleaned)
                        && cleaned.contains("oqt_f_add(10, 20)")
                    {
                        let mut rows = Vec::new();
                        let streaming = execute_select_streaming_with_binds(
                            conn.as_ref(),
                            &cleaned,
                            &[],
                            &mut |_columns| {},
                            &mut |row| {
                                rows.push(row);
                                true
                            },
                        );
                        let status =
                            get_object_status(conn.as_ref(), "OQT_F_ADD", "FUNCTION")
                                .unwrap_or_else(|err| format!("STATUS ERROR: {err}"));
                        let errors = get_compilation_errors(conn.as_ref(), "OQT_F_ADD", "FUNCTION");
                        assert!(
                            streaming.is_ok(),
                            "test3 function select must succeed; status={status}, errors={errors:?}, result={streaming:?}"
                        );
                        assert_eq!(rows.len(), 1, "expected one row, got {rows:?}");
                        assert_eq!(rows[0][1].trim(), "30", "unexpected function row: {rows:?}");
                        let _ = close(conn.as_ref());
                        return;
                    }

                    if QueryExecutor::is_select_statement(&cleaned) {
                        execute_select_streaming_with_binds(
                            conn.as_ref(),
                            &cleaned,
                            &[],
                            &mut |_columns| {},
                            &mut |_row| true,
                        )
                        .unwrap_or_else(|err| {
                            panic!("pre-function SELECT in test3 must succeed:\n{cleaned}\n\n{err}")
                        });
                    } else {
                        execute_statement_with_binds(conn.as_ref(), &cleaned, &[])
                            .unwrap_or_else(|err| {
                                panic!(
                                    "pre-function statement in test3 must succeed:\n{cleaned}\n\n{err}"
                                )
                            });
                    }
                }
                ScriptItem::ToolCommand(ToolCommand::SetServerOutput {
                    enabled,
                    size,
                    unlimited,
                }) => {
                    if enabled {
                        let resolved_size = if unlimited {
                            None
                        } else {
                            size.or(Some(1_000_000))
                        };
                        enable_dbms_output(conn.as_ref(), resolved_size)
                            .expect("DBMS_OUTPUT enable must succeed");
                    } else {
                        disable_dbms_output(conn.as_ref())
                            .expect("DBMS_OUTPUT disable must succeed");
                    }
                }
                ScriptItem::ToolCommand(ToolCommand::ShowErrors { .. })
                | ScriptItem::ToolCommand(ToolCommand::MysqlShowErrors)
                | ScriptItem::ToolCommand(ToolCommand::Prompt { .. })
                | ScriptItem::ToolCommand(_) => continue,
            }
        }

        panic!("target SELECT oqt_f_add(10, 20) was not found in test3.txt");
    }

    #[test]
    #[ignore = "requires local Oracle XE at localhost:1521/FREE"]
    fn oracle_thin_executes_packaged_procedure_with_dml_and_dbms_output() {
        let info = test_connection_info();
        let conn = connect(&info).expect("oracle thin connection must succeed");

        let table_name = unique_object_name("THIN_LOG");
        let package_name = unique_object_name("THIN_PKG");

        execute_statement_with_binds(
            conn.as_ref(),
            &format!("CREATE TABLE {table_name} (RUN_ID NUMBER, MSG VARCHAR2(200))"),
            &[],
        )
        .expect("log table creation must succeed");

        execute_statement_with_binds(
            conn.as_ref(),
            &format!(
                "CREATE OR REPLACE PACKAGE {package_name} AS PROCEDURE proc_in_only(p_tag IN VARCHAR2); END {package_name};"
            ),
            &[],
        )
        .expect("package spec creation must succeed");

        execute_statement_with_binds(
            conn.as_ref(),
            &format!(
                r#"
                CREATE OR REPLACE PACKAGE BODY {package_name} AS
                  PROCEDURE log_msg(p_run_id NUMBER, p_msg VARCHAR2) IS
                  BEGIN
                    INSERT INTO {table_name}(RUN_ID, MSG) VALUES (p_run_id, p_msg);
                  END;

                  PROCEDURE proc_in_only(p_tag IN VARCHAR2) IS
                    v_run_id NUMBER := TRUNC(DBMS_RANDOM.VALUE(100000, 999999));
                  BEGIN
                    DBMS_OUTPUT.PUT_LINE('[proc_in_only] tag=' || p_tag || ', run_id=' || v_run_id);
                    log_msg(v_run_id, '[proc_in_only] tag=' || p_tag);
                    COMMIT;
                  END;
                END {package_name};
                "#
            ),
            &[],
        )
        .expect("package body creation must succeed");

        let errors = get_compilation_errors(conn.as_ref(), &package_name, "PACKAGE BODY")
            .expect("package body errors query must succeed");
        assert!(
            errors.is_empty(),
            "package body should compile without errors: {errors:?}"
        );

        enable_dbms_output(conn.as_ref(), Some(1_000_000))
            .expect("DBMS_OUTPUT enable must succeed");
        execute_statement_with_binds(
            conn.as_ref(),
            &format!("BEGIN {package_name}.proc_in_only('HELLO_TOAD'); END;"),
            &[],
        )
        .expect("packaged procedure call must succeed");

        let rows = execute_select_all_with_binds(
            conn.as_ref(),
            &format!("SELECT MSG FROM {table_name}"),
            &[],
        )
        .expect("log row query must succeed");
        assert_eq!(rows.rows.len(), 1);
        let msg_index = rows
            .columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case("MSG"))
            .expect("MSG column must exist");
        assert_eq!(rows.rows[0][msg_index], "[proc_in_only] tag=HELLO_TOAD");

        let output_lines =
            get_dbms_output(conn.as_ref(), 10).expect("DBMS_OUTPUT fetch must succeed");
        assert!(
            output_lines
                .iter()
                .any(|line| line.contains("[proc_in_only] tag=HELLO_TOAD")),
            "expected DBMS_OUTPUT line from packaged procedure, got {output_lines:?}"
        );

        let _ = execute_statement_with_binds(
            conn.as_ref(),
            &format!("DROP PACKAGE {package_name}"),
            &[],
        );
        let _ = execute_statement_with_binds(
            conn.as_ref(),
            &format!("DROP TABLE {table_name} PURGE"),
            &[],
        );
        let _ = close(conn.as_ref());
    }

    #[test]
    #[ignore = "requires local Oracle XE at localhost:1521/FREE"]
    fn oracle_thin_executes_complex_package_with_types_and_refcursors() {
        let info = test_connection_info();
        let conn = connect(&info).expect("oracle thin connection must succeed");

        let emp_table = unique_object_name("THIN_EMP");
        let run_log_table = unique_object_name("THIN_RUN_LOG");
        let tmp_result_table = unique_object_name("THIN_TMP_RESULT");
        let row_type = unique_object_name("THIN_ROW");
        let row_tab_type = unique_object_name("THIN_ROW_TAB");
        let package_name = unique_object_name("THIN_COMPLEX_PKG");

        execute_statement_with_binds(
            conn.as_ref(),
            &format!(
                "CREATE TABLE {emp_table} (EMP_ID NUMBER PRIMARY KEY, EMP_NAME VARCHAR2(50) NOT NULL, SAL NUMBER NOT NULL)"
            ),
            &[],
        )
        .expect("employee table creation must succeed");
        execute_statement_with_binds(
            conn.as_ref(),
            &format!("INSERT INTO {emp_table}(EMP_ID, EMP_NAME, SAL) VALUES (100, 'ALICE', 5000)"),
            &[],
        )
        .expect("seed row 100 must be inserted");
        execute_statement_with_binds(
            conn.as_ref(),
            &format!("INSERT INTO {emp_table}(EMP_ID, EMP_NAME, SAL) VALUES (101, 'BOB', 7000)"),
            &[],
        )
        .expect("seed row 101 must be inserted");
        execute_statement_with_binds(
            conn.as_ref(),
            &format!("INSERT INTO {emp_table}(EMP_ID, EMP_NAME, SAL) VALUES (102, 'CAROL', 9000)"),
            &[],
        )
        .expect("seed row 102 must be inserted");
        execute_statement_with_binds(
            conn.as_ref(),
            &format!("INSERT INTO {emp_table}(EMP_ID, EMP_NAME, SAL) VALUES (103, 'DAVE', 12000)"),
            &[],
        )
        .expect("seed row 103 must be inserted");
        execute_statement_with_binds(conn.as_ref(), "COMMIT", &[])
            .expect("seed data commit must succeed");

        execute_statement_with_binds(
            conn.as_ref(),
            &format!(
                "CREATE TABLE {run_log_table} (RUN_ID NUMBER, RUN_TS TIMESTAMP DEFAULT SYSTIMESTAMP, MSG VARCHAR2(4000))"
            ),
            &[],
        )
        .expect("run log table creation must succeed");
        execute_statement_with_binds(
            conn.as_ref(),
            &format!(
                "CREATE TABLE {tmp_result_table} (RUN_ID NUMBER, ROW_NO NUMBER, PAYLOAD VARCHAR2(4000))"
            ),
            &[],
        )
        .expect("tmp result table creation must succeed");

        execute_statement_with_binds(
            conn.as_ref(),
            &format!(
                "CREATE OR REPLACE TYPE {row_type} AS OBJECT (EMP_ID NUMBER, EMP_NAME VARCHAR2(50), SAL NUMBER)"
            ),
            &[],
        )
        .expect("object type creation must succeed");
        execute_statement_with_binds(
            conn.as_ref(),
            &format!("CREATE OR REPLACE TYPE {row_tab_type} AS TABLE OF {row_type}"),
            &[],
        )
        .expect("table type creation must succeed");

        execute_statement_with_binds(
            conn.as_ref(),
            &format!(
                r#"
                CREATE OR REPLACE PACKAGE {package_name} AS
                  PROCEDURE proc_in_only(p_tag IN VARCHAR2);
                  PROCEDURE proc_out_params(
                    p_emp_id   IN  NUMBER,
                    p_raise_by IN  NUMBER,
                    p_old_sal  OUT NUMBER,
                    p_new_sal  OUT NUMBER,
                    p_status   OUT VARCHAR2
                  );
                  PROCEDURE proc_inout_counter(p_counter IN OUT NUMBER);
                  PROCEDURE proc_refcursor_out(
                    p_min_sal IN  NUMBER,
                    p_rc      OUT SYS_REFCURSOR
                  );
                  FUNCTION func_get_sal(p_emp_id IN NUMBER) RETURN NUMBER;
                  FUNCTION func_refcursor(p_min_sal IN NUMBER) RETURN SYS_REFCURSOR;
                  FUNCTION func_pipe_rows(p_min_sal IN NUMBER)
                    RETURN {row_tab_type} PIPELINED;
                  PROCEDURE proc_fill_result_table(
                    p_run_id  IN NUMBER,
                    p_min_sal IN NUMBER
                  );
                END {package_name};
                "#
            ),
            &[],
        )
        .expect("complex package spec creation must succeed");

        execute_statement_with_binds(
            conn.as_ref(),
            &format!(
                r#"
                CREATE OR REPLACE PACKAGE BODY {package_name} AS
                  PROCEDURE log_msg(p_run_id NUMBER, p_msg VARCHAR2) IS
                  BEGIN
                    INSERT INTO {run_log_table}(RUN_ID, MSG) VALUES (p_run_id, p_msg);
                  END;

                  PROCEDURE proc_in_only(p_tag IN VARCHAR2) IS
                    v_run_id NUMBER := TRUNC(DBMS_RANDOM.VALUE(100000, 999999));
                  BEGIN
                    DBMS_OUTPUT.PUT_LINE('[proc_in_only] tag=' || p_tag || ', run_id=' || v_run_id);
                    log_msg(v_run_id, '[proc_in_only] tag=' || p_tag);
                    COMMIT;
                  END;

                  PROCEDURE proc_out_params(
                    p_emp_id   IN  NUMBER,
                    p_raise_by IN  NUMBER,
                    p_old_sal  OUT NUMBER,
                    p_new_sal  OUT NUMBER,
                    p_status   OUT VARCHAR2
                  ) IS
                  BEGIN
                    SELECT sal INTO p_old_sal FROM {emp_table} WHERE emp_id = p_emp_id FOR UPDATE;
                    p_new_sal := p_old_sal + p_raise_by;
                    UPDATE {emp_table}
                       SET sal = p_new_sal
                     WHERE emp_id = p_emp_id;
                    p_status := 'OK';
                    DBMS_OUTPUT.PUT_LINE('[proc_out_params] emp_id='||p_emp_id||', old='||p_old_sal||', new='||p_new_sal);
                    COMMIT;
                  EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                      p_old_sal := NULL;
                      p_new_sal := NULL;
                      p_status := 'NOT_FOUND';
                      DBMS_OUTPUT.PUT_LINE('[proc_out_params] emp_id='||p_emp_id||' not found');
                  END;

                  PROCEDURE proc_inout_counter(p_counter IN OUT NUMBER) IS
                  BEGIN
                    p_counter := NVL(p_counter, 0) + 1;
                    DBMS_OUTPUT.PUT_LINE('[proc_inout_counter] counter=' || p_counter);
                  END;

                  PROCEDURE proc_refcursor_out(
                    p_min_sal IN  NUMBER,
                    p_rc      OUT SYS_REFCURSOR
                  ) IS
                  BEGIN
                    OPEN p_rc FOR
                      SELECT emp_id, emp_name, sal
                        FROM {emp_table}
                       WHERE sal >= p_min_sal
                       ORDER BY sal;
                  END;

                  FUNCTION func_get_sal(p_emp_id IN NUMBER) RETURN NUMBER IS
                    v_sal NUMBER;
                  BEGIN
                    SELECT sal INTO v_sal FROM {emp_table} WHERE emp_id = p_emp_id;
                    RETURN v_sal;
                  EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                      RETURN NULL;
                  END;

                  FUNCTION func_refcursor(p_min_sal IN NUMBER) RETURN SYS_REFCURSOR IS
                    rc SYS_REFCURSOR;
                  BEGIN
                    OPEN rc FOR
                      SELECT emp_id, emp_name, sal
                        FROM {emp_table}
                       WHERE sal >= p_min_sal
                       ORDER BY emp_id;
                    RETURN rc;
                  END;

                  FUNCTION func_pipe_rows(p_min_sal IN NUMBER)
                    RETURN {row_tab_type} PIPELINED
                  IS
                  BEGIN
                    FOR r IN (SELECT emp_id, emp_name, sal
                                FROM {emp_table}
                               WHERE sal >= p_min_sal
                               ORDER BY sal)
                    LOOP
                      PIPE ROW({row_type}(r.emp_id, r.emp_name, r.sal));
                    END LOOP;
                    RETURN;
                  END;

                  PROCEDURE proc_fill_result_table(
                    p_run_id  IN NUMBER,
                    p_min_sal IN NUMBER
                  ) IS
                    v_row_no NUMBER := 0;
                  BEGIN
                    DELETE FROM {tmp_result_table} WHERE run_id = p_run_id;

                    FOR r IN (SELECT emp_id, emp_name, sal
                                FROM {emp_table}
                               WHERE sal >= p_min_sal
                               ORDER BY sal)
                    LOOP
                      v_row_no := v_row_no + 1;
                      INSERT INTO {tmp_result_table}(RUN_ID, ROW_NO, PAYLOAD)
                      VALUES (p_run_id, v_row_no,
                              'emp_id='||r.emp_id||', name='||r.emp_name||', sal='||r.sal);
                    END LOOP;

                    log_msg(p_run_id, '[proc_fill_result_table] min_sal='||p_min_sal||', rows='||v_row_no);
                    COMMIT;
                  END;
                END {package_name};
                "#
            ),
            &[],
        )
        .expect("complex package body creation must succeed");

        let errors = get_compilation_errors(conn.as_ref(), &package_name, "PACKAGE BODY")
            .expect("package body errors query must succeed");
        assert!(
            errors.is_empty(),
            "complex package body should compile without errors: {errors:?}"
        );

        enable_dbms_output(conn.as_ref(), Some(1_000_000))
            .expect("DBMS_OUTPUT enable must succeed");

        let call_result = execute_statement_with_binds(
            conn.as_ref(),
            &format!("BEGIN {package_name}.proc_in_only('HELLO_TOAD'); END;"),
            &[],
        );

        if let Err(err) = call_result {
            let reconnect = connect(&info).expect("reconnect must succeed after failed call");
            let status = get_object_status(reconnect.as_ref(), &package_name, "PACKAGE BODY")
                .unwrap_or_else(|status_err| format!("status lookup failed: {status_err}"));
            let reconnect_errors =
                get_compilation_errors(reconnect.as_ref(), &package_name, "PACKAGE BODY")
                    .unwrap_or_else(|query_err| {
                        vec![CompilationError {
                            line: 0,
                            position: 0,
                            text: format!("error query failed: {query_err}"),
                            attribute: String::new(),
                        }]
                    });
            panic!(
                "complex package proc_in_only call failed: {err}\npackage body status: {status}\npackage body errors: {reconnect_errors:?}"
            );
        }

        let rows = execute_select_all_with_binds(
            conn.as_ref(),
            &format!("SELECT MSG FROM {run_log_table} ORDER BY RUN_TS DESC"),
            &[],
        )
        .expect("run log query must succeed");
        assert_eq!(rows.rows.len(), 1);
        let msg_index = rows
            .columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case("MSG"))
            .expect("MSG column must exist");
        assert_eq!(rows.rows[0][msg_index], "[proc_in_only] tag=HELLO_TOAD");

        let output_lines =
            get_dbms_output(conn.as_ref(), 10).expect("DBMS_OUTPUT fetch must succeed");
        assert!(
            output_lines
                .iter()
                .any(|line| line.contains("[proc_in_only] tag=HELLO_TOAD")),
            "expected DBMS_OUTPUT line from complex packaged procedure, got {output_lines:?}"
        );

        let _ = execute_statement_with_binds(
            conn.as_ref(),
            &format!("DROP PACKAGE {package_name}"),
            &[],
        );
        let _ = execute_statement_with_binds(
            conn.as_ref(),
            &format!("DROP TYPE {row_tab_type} FORCE"),
            &[],
        );
        let _ = execute_statement_with_binds(
            conn.as_ref(),
            &format!("DROP TYPE {row_type} FORCE"),
            &[],
        );
        let _ = execute_statement_with_binds(
            conn.as_ref(),
            &format!("DROP TABLE {tmp_result_table} PURGE"),
            &[],
        );
        let _ = execute_statement_with_binds(
            conn.as_ref(),
            &format!("DROP TABLE {run_log_table} PURGE"),
            &[],
        );
        let _ = execute_statement_with_binds(
            conn.as_ref(),
            &format!("DROP TABLE {emp_table} PURGE"),
            &[],
        );
        let _ = close(conn.as_ref());
    }

    #[test]
    #[ignore = "requires local Oracle XE at localhost:1521/FREE"]
    fn oracle_thin_returns_multiple_scalar_out_binds_from_plsql_block() {
        let info = test_connection_info();
        let conn = connect(&info).expect("oracle thin connection must succeed");

        let binds = vec![
            ResolvedBind {
                name: "V_GRP".to_string(),
                data_type: BindDataType::Number,
                value: None,
            },
            ResolvedBind {
                name: "V_N".to_string(),
                data_type: BindDataType::Number,
                value: None,
            },
            ResolvedBind {
                name: "V_TXT".to_string(),
                data_type: BindDataType::Varchar2(400),
                value: None,
            },
        ];

        let result = execute_statement_with_binds(
            conn.as_ref(),
            r#"
            BEGIN
              :V_GRP := 2;
              :V_N := 5;
              :V_TXT := 'hello OQT_MEGA';
            END;
            "#,
            &binds,
        )
        .expect("PL/SQL block with multiple scalar OUT binds must succeed");

        let mut updates = HashMap::new();
        for (name, value) in result.scalar_updates {
            updates.insert(name, value);
        }

        match updates.get("V_GRP") {
            Some(BindValue::Scalar(Some(value))) => assert_eq!(value, "2"),
            other => panic!("unexpected V_GRP update: {other:?}"),
        }
        match updates.get("V_N") {
            Some(BindValue::Scalar(Some(value))) => assert_eq!(value, "5"),
            other => panic!("unexpected V_N update: {other:?}"),
        }
        match updates.get("V_TXT") {
            Some(BindValue::Scalar(Some(value))) => assert_eq!(value, "hello OQT_MEGA"),
            other => panic!("unexpected V_TXT update: {other:?}"),
        }

        let _ = close(conn.as_ref());
    }

    #[test]
    #[ignore = "requires local Oracle XE at localhost:1521/FREE"]
    fn oracle_thin_handles_caught_raise_after_exec_calls_with_dbms_output() {
        let info = test_connection_info();
        let conn = connect(&info).expect("oracle thin connection must succeed");

        let table_name = unique_object_name("THIN_EXEC_TEST");
        let package_name = unique_object_name("THIN_EXEC_DEMO_PKG");

        execute_statement_with_binds(
            conn.as_ref(),
            &format!(
                "CREATE TABLE {table_name} (id NUMBER PRIMARY KEY, tag VARCHAR2(30), msg VARCHAR2(4000))"
            ),
            &[],
        )
        .expect("test table creation must succeed");

        execute_statement_with_binds(
            conn.as_ref(),
            &format!(
                r#"
                CREATE OR REPLACE PACKAGE {package_name} AS
                  PROCEDURE p_mix(
                    p_id      IN     NUMBER,
                    p_in_txt  IN     VARCHAR2 DEFAULT 'DEF',
                    p_out_txt OUT    VARCHAR2,
                    p_inout_n IN OUT NUMBER
                  );
                  PROCEDURE p_over(p_id IN NUMBER);
                  PROCEDURE p_over(p_tag IN VARCHAR2, p_id IN NUMBER);
                  PROCEDURE p_raise(p_code IN NUMBER);
                END {package_name};
                "#
            ),
            &[],
        )
        .expect("package spec creation must succeed");

        execute_statement_with_binds(
            conn.as_ref(),
            &format!(
                r#"
                CREATE OR REPLACE PACKAGE BODY {package_name} AS
                  PROCEDURE p_mix(
                    p_id      IN     NUMBER,
                    p_in_txt  IN     VARCHAR2 DEFAULT 'DEF',
                    p_out_txt OUT    VARCHAR2,
                    p_inout_n IN OUT NUMBER
                  ) IS
                  BEGIN
                    p_out_txt := 'ID='||p_id||', IN='||p_in_txt||', INOUT='||NVL(p_inout_n,0);
                    p_inout_n := NVL(p_inout_n,0) + p_id;
                    INSERT INTO {table_name}(id, tag, msg) VALUES (p_id, 'MIX', p_out_txt);
                    DBMS_OUTPUT.PUT_LINE('[p_mix] out='||p_out_txt||', new_inout='||p_inout_n);
                  END;

                  PROCEDURE p_over(p_id IN NUMBER) IS
                  BEGIN
                    INSERT INTO {table_name}(id, tag, msg) VALUES (p_id, 'OVER1', 'only id');
                    DBMS_OUTPUT.PUT_LINE('[p_over#1] id='||p_id);
                  END;

                  PROCEDURE p_over(p_tag IN VARCHAR2, p_id IN NUMBER) IS
                  BEGIN
                    INSERT INTO {table_name}(id, tag, msg) VALUES (p_id, 'OVER2', p_tag);
                    DBMS_OUTPUT.PUT_LINE('[p_over#2] tag='||p_tag||', id='||p_id);
                  END;

                  PROCEDURE p_raise(p_code IN NUMBER) IS
                  BEGIN
                    IF p_code = 1 THEN
                      RAISE_APPLICATION_ERROR(-20001, 'demo error from p_raise');
                    END IF;
                    DBMS_OUTPUT.PUT_LINE('[p_raise] no error for code='||p_code);
                  END;
                END {package_name};
                "#
            ),
            &[],
        )
        .expect("package body creation must succeed");

        enable_dbms_output(conn.as_ref(), Some(1_000_000))
            .expect("DBMS_OUTPUT enable must succeed");

        let binds = |inout_value: Option<&str>| {
            vec![
                ResolvedBind {
                    name: "V_OUT".to_string(),
                    data_type: BindDataType::Varchar2(4000),
                    value: None,
                },
                ResolvedBind {
                    name: "V_INOUT".to_string(),
                    data_type: BindDataType::Number,
                    value: inout_value.map(|value| value.to_string()),
                },
            ]
        };

        execute_statement_with_binds(
            conn.as_ref(),
            &format!("EXEC {package_name}.p_mix(2, p_out_txt => :V_OUT, p_inout_n => :V_INOUT)"),
            &binds(Some("10")),
        )
        .expect("first p_mix EXEC must succeed");
        let mix_lines =
            get_dbms_output(conn.as_ref(), 10).expect("first DBMS_OUTPUT fetch must succeed");
        assert!(
            mix_lines.iter().any(|line| line.contains("[p_mix]")),
            "expected p_mix DBMS_OUTPUT after first EXEC, got {mix_lines:?}"
        );

        execute_statement_with_binds(
            conn.as_ref(),
            &format!("EXEC {package_name}.p_mix(3, p_out_txt => :V_OUT, p_inout_n => :V_INOUT)"),
            &binds(Some("12")),
        )
        .expect("second p_mix EXEC must succeed");
        let mix_default_lines =
            get_dbms_output(conn.as_ref(), 10).expect("second DBMS_OUTPUT fetch must succeed");
        assert!(
            mix_default_lines
                .iter()
                .any(|line| line.contains("[p_mix]")),
            "expected p_mix DBMS_OUTPUT after second EXEC, got {mix_default_lines:?}"
        );

        execute_statement_with_binds(
            conn.as_ref(),
            &format!("EXEC {package_name}.p_over(5)"),
            &[],
        )
        .expect("single-arg p_over EXEC must succeed");
        let over1_lines =
            get_dbms_output(conn.as_ref(), 10).expect("p_over#1 DBMS_OUTPUT fetch must succeed");
        assert!(
            over1_lines.iter().any(|line| line.contains("[p_over#1]")),
            "expected p_over#1 DBMS_OUTPUT, got {over1_lines:?}"
        );

        execute_statement_with_binds(
            conn.as_ref(),
            &format!("EXEC {package_name}.p_over('tagged-overload', 6)"),
            &[],
        )
        .expect("two-arg p_over EXEC must succeed");
        let over2_lines =
            get_dbms_output(conn.as_ref(), 10).expect("p_over#2 DBMS_OUTPUT fetch must succeed");
        assert!(
            over2_lines.iter().any(|line| line.contains("[p_over#2]")),
            "expected p_over#2 DBMS_OUTPUT, got {over2_lines:?}"
        );

        execute_statement_with_binds(
            conn.as_ref(),
            &format!(
                r#"
                BEGIN
                  {package_name}.p_raise(1);
                EXCEPTION WHEN OTHERS THEN
                  DBMS_OUTPUT.PUT_LINE('[p_raise expected] '||SQLERRM);
                END;
                "#
            ),
            &[],
        )
        .expect("handled p_raise block must succeed after prior EXEC calls");

        let raise_lines = get_dbms_output(conn.as_ref(), 10)
            .expect("handled p_raise DBMS_OUTPUT fetch must succeed");
        assert!(
            raise_lines
                .iter()
                .any(|line| line.contains("[p_raise expected]")),
            "expected handled p_raise DBMS_OUTPUT, got {raise_lines:?}"
        );

        let _ = execute_statement_with_binds(
            conn.as_ref(),
            &format!("DROP PACKAGE {package_name}"),
            &[],
        );
        let _ = execute_statement_with_binds(
            conn.as_ref(),
            &format!("DROP TABLE {table_name} PURGE"),
            &[],
        );
        let _ = close(conn.as_ref());
    }

    #[test]
    #[ignore = "requires local Oracle XE at localhost:1521/FREE"]
    fn oracle_thin_executes_test5_package_body_from_split_script() {
        let info = test_connection_info();
        let conn = connect(&info).expect("oracle thin connection must succeed");
        let items = QueryExecutor::split_script_items_for_db_type(
            include_str!("../../test/test5.txt"),
            Some(DatabaseType::OracleThin),
        );

        let mut package_body_seen = false;
        for item in items {
            match item {
                ScriptItem::Statement(statement) => {
                    let result = execute_statement_with_binds(conn.as_ref(), &statement, &[]);
                    if statement.starts_with("CREATE OR REPLACE PACKAGE BODY oqt_deep_pkg AS") {
                        package_body_seen = true;
                        result.expect("test5 package body must execute successfully");
                        break;
                    } else {
                        result.unwrap_or_else(|err| {
                            panic!(
                                "setup statement failed before package body:\n{statement}\n\n{err}"
                            )
                        });
                    }
                }
                ScriptItem::ToolCommand(ToolCommand::SetServerOutput {
                    enabled,
                    size,
                    unlimited,
                }) => {
                    if enabled {
                        let resolved_size = if unlimited {
                            None
                        } else {
                            size.or(Some(1_000_000))
                        };
                        enable_dbms_output(conn.as_ref(), resolved_size)
                            .expect("DBMS_OUTPUT enable must succeed");
                    } else {
                        disable_dbms_output(conn.as_ref())
                            .expect("DBMS_OUTPUT disable must succeed");
                    }
                }
                ScriptItem::ToolCommand(ToolCommand::ShowErrors { .. })
                | ScriptItem::ToolCommand(ToolCommand::MysqlShowErrors)
                | ScriptItem::ToolCommand(ToolCommand::Prompt { .. }) => {}
                ScriptItem::ToolCommand(other) => {
                    panic!("unexpected tool command before test5 package body: {other:?}");
                }
            }
        }

        assert!(
            package_body_seen,
            "test5 package body statement must be found"
        );
        let _ = close(conn.as_ref());
    }

    #[test]
    #[ignore = "requires local Oracle XE at localhost:1521/FREE"]
    fn oracle_thin_streaming_clob_select_and_drop_blocks_remain_usable() {
        let info = test_connection_info();
        let conn = connect(&info).expect("oracle thin connection must succeed");

        for iteration in 0..40 {
            execute_statement_with_binds(
                conn.as_ref(),
                "BEGIN EXECUTE IMMEDIATE 'DROP TABLE thin_clob_stream_probe PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;",
                &[],
            )
            .expect("stale streaming CLOB probe drop cleanup must succeed");
            execute_statement_with_binds(
                conn.as_ref(),
                "CREATE TABLE thin_clob_stream_probe (id NUMBER PRIMARY KEY, detail_text CLOB)",
                &[],
            )
            .expect("streaming CLOB probe table creation must succeed");
            execute_statement_with_binds(
                conn.as_ref(),
                &format!(
                    "INSERT INTO thin_clob_stream_probe (id, detail_text) VALUES (1, 'streaming clob probe iteration {iteration}')"
                ),
                &[],
            )
            .expect("streaming CLOB probe insert must succeed");

            let mut seen_rows = Vec::new();
            let (result, cancelled) = execute_select_streaming_with_binds(
                conn.as_ref(),
                "SELECT detail_text FROM thin_clob_stream_probe",
                &[],
                &mut |_columns| {},
                &mut |row| {
                    seen_rows.push(row);
                    true
                },
            )
            .expect("streaming CLOB probe select must succeed");

            assert!(
                !cancelled,
                "streaming CLOB probe select must not be cancelled"
            );
            assert_eq!(result.row_count, 1);
            assert_eq!(seen_rows.len(), 1);
            assert!(
                seen_rows[0]
                    .iter()
                    .any(|value| value.contains("streaming clob probe iteration")),
                "streaming CLOB content must be materialized as text: {:?}",
                seen_rows
            );

            execute_statement_with_binds(
                conn.as_ref(),
                "BEGIN EXECUTE IMMEDIATE 'DROP TABLE thin_clob_stream_probe PURGE'; END;",
                &[],
            )
            .expect("streaming CLOB probe drop must succeed after select");
        }
    }

    #[test]
    #[ignore = "diagnostic helper for repeated test11 block cursor growth"]
    fn oracle_thin_diagnostic_repeated_test11_block_reports_open_cursor_growth() {
        let info = test_connection_info();
        let exec_conn = connect(&info).expect("oracle thin execution connection must succeed");
        let probe_conn = connect(&info).expect("oracle thin probe connection must succeed");

        let exec_sid = query_single_string(
            exec_conn.as_ref(),
            "SELECT SYS_CONTEXT('USERENV', 'SID') FROM dual",
            &[],
        )
        .expect("execution session SID lookup must succeed")
        .parse::<i64>()
        .expect("execution SID must parse as i64");

        let items = QueryExecutor::split_script_items_for_db_type(
            include_str!("../../test/test11.txt"),
            Some(DatabaseType::OracleThin),
        );
        let block_statements = items
            .into_iter()
            .filter_map(|item| match item {
                ScriptItem::Statement(statement) => Some(statement),
                ScriptItem::ToolCommand(ToolCommand::ShowErrors { .. })
                | ScriptItem::ToolCommand(ToolCommand::MysqlShowErrors)
                | ScriptItem::ToolCommand(ToolCommand::Prompt { .. }) => None,
                ScriptItem::ToolCommand(other) => {
                    panic!("unexpected test11 tool command in diagnostic block: {other:?}")
                }
            })
            .take(50)
            .collect::<Vec<_>>();

        assert_eq!(
            block_statements.len(),
            50,
            "test11 diagnostic block must contain 50 statements"
        );

        enable_dbms_output(exec_conn.as_ref(), Some(1_000_000))
            .expect("diagnostic test11 block must enable DBMS_OUTPUT");

        for cycle in 0..100 {
            for (statement_offset, statement) in block_statements.iter().enumerate() {
                let cleaned = QueryExecutor::strip_leading_comments(statement);
                if QueryExecutor::is_select_statement(&cleaned) {
                    let (_result, cancelled) = execute_select_streaming_with_binds(
                        exec_conn.as_ref(),
                        &cleaned,
                        &[],
                        &mut |_columns| {},
                        &mut |_row| true,
                    )
                    .unwrap_or_else(|err| {
                        panic!(
                            "diagnostic test11 block select must succeed at cycle {} statement {}.\nSQL:\n{}\n\n{}",
                            cycle + 1,
                            statement_offset + 1,
                            cleaned,
                            err
                        )
                    });
                    assert!(
                        !cancelled,
                        "diagnostic test11 block select must not be cancelled at cycle {} statement {}",
                        cycle + 1,
                        statement_offset + 1
                    );
                } else {
                    execute_statement_with_binds(exec_conn.as_ref(), &cleaned, &[]).unwrap_or_else(
                        |err| {
                            panic!(
                                "diagnostic test11 block statement must succeed at cycle {} statement {}.\nSQL:\n{}\n\n{}",
                                cycle + 1,
                                statement_offset + 1,
                                cleaned,
                                err
                            )
                        },
                    );
                }

                let upper = QueryExecutor::normalize_sql_for_execute(&cleaned).to_ascii_uppercase();
                if (upper.starts_with("BEGIN")
                    || upper.starts_with("DECLARE")
                    || upper.starts_with("CALL")
                    || upper.starts_with("EXEC "))
                    && upper.contains("DBMS_OUTPUT")
                {
                    let _ = get_dbms_output(exec_conn.as_ref(), 10_000);
                }
            }

            let open_cursor_count = query_single_string(
                probe_conn.as_ref(),
                "SELECT COUNT(*) FROM sys.v_$open_cursor WHERE sid = :1",
                &[ThinValue::Integer(exec_sid)],
            )
            .expect("open cursor count query must succeed");
            let audit_cursor_count = query_single_string(
                probe_conn.as_ref(),
                "SELECT COUNT(*) FROM sys.v_$open_cursor WHERE sid = :1 AND UPPER(sql_text) LIKE '%QT_AUDIT_LOG%'",
                &[ThinValue::Integer(exec_sid)],
            )
            .expect("audit cursor count query must succeed");

            if cycle < 7 || (cycle + 1) % 10 == 0 {
                eprintln!(
                    "cycle {}: open_cursors={}, qt_audit_log_cursors={}",
                    cycle + 1,
                    open_cursor_count,
                    audit_cursor_count
                );
            }
        }

        let top_open_sql = query_all(
            probe_conn.as_ref(),
            r#"
            SELECT *
            FROM (
                SELECT SUBSTR(sql_text, 1, 160) AS sql_text,
                       COUNT(*) AS cursor_count
                FROM sys.v_$open_cursor
                WHERE sid = :1
                GROUP BY SUBSTR(sql_text, 1, 160)
                ORDER BY COUNT(*) DESC, SUBSTR(sql_text, 1, 160)
            )
            WHERE ROWNUM <= 20
            "#,
            &[ThinValue::Integer(exec_sid)],
        )
        .expect("top open cursor sql query must succeed");

        eprintln!("top open cursor SQL texts:");
        for row in top_open_sql.rows {
            eprintln!(
                "{} | {}",
                row_string(&row, 1).unwrap(),
                row_string(&row, 0).unwrap()
            );
        }
    }

    #[test]
    #[ignore = "diagnostic helper for repeated larger test11 block stability"]
    fn oracle_thin_diagnostic_repeated_test11_first_120_statements_remain_stable() {
        let info = test_connection_info();
        let exec_conn = connect(&info).expect("oracle thin execution connection must succeed");
        let probe_conn = connect(&info).expect("oracle thin probe connection must succeed");

        let exec_sid = query_single_string(
            exec_conn.as_ref(),
            "SELECT SYS_CONTEXT('USERENV', 'SID') FROM dual",
            &[],
        )
        .expect("execution session SID lookup must succeed")
        .parse::<i64>()
        .expect("execution SID must parse as i64");

        let items = QueryExecutor::split_script_items_for_db_type(
            include_str!("../../test/test11.txt"),
            Some(DatabaseType::OracleThin),
        );
        let block_statements = items
            .into_iter()
            .filter_map(|item| match item {
                ScriptItem::Statement(statement) => Some(statement),
                ScriptItem::ToolCommand(ToolCommand::ShowErrors { .. })
                | ScriptItem::ToolCommand(ToolCommand::MysqlShowErrors)
                | ScriptItem::ToolCommand(ToolCommand::Prompt { .. }) => None,
                ScriptItem::ToolCommand(other) => {
                    panic!("unexpected test11 tool command in diagnostic block: {other:?}")
                }
            })
            .take(120)
            .collect::<Vec<_>>();

        assert_eq!(
            block_statements.len(),
            120,
            "test11 diagnostic block must contain 120 statements"
        );

        enable_dbms_output(exec_conn.as_ref(), Some(1_000_000))
            .expect("diagnostic test11 block must enable DBMS_OUTPUT");

        for cycle in 0..60 {
            for (statement_offset, statement) in block_statements.iter().enumerate() {
                let cleaned = QueryExecutor::strip_leading_comments(statement);
                if QueryExecutor::is_select_statement(&cleaned) {
                    let (_result, cancelled) = execute_select_streaming_with_binds(
                        exec_conn.as_ref(),
                        &cleaned,
                        &[],
                        &mut |_columns| {},
                        &mut |_row| true,
                    )
                    .unwrap_or_else(|err| {
                        panic!(
                            "diagnostic test11 larger block select must succeed at cycle {} statement {}.\nSQL:\n{}\n\n{}",
                            cycle + 1,
                            statement_offset + 1,
                            cleaned,
                            err
                        )
                    });
                    assert!(
                        !cancelled,
                        "diagnostic test11 larger block select must not be cancelled at cycle {} statement {}",
                        cycle + 1,
                        statement_offset + 1
                    );
                } else {
                    execute_statement_with_binds(exec_conn.as_ref(), &cleaned, &[]).unwrap_or_else(
                        |err| {
                            panic!(
                                "diagnostic test11 larger block statement must succeed at cycle {} statement {}.\nSQL:\n{}\n\n{}",
                                cycle + 1,
                                statement_offset + 1,
                                cleaned,
                                err
                            )
                        },
                    );
                }

                let upper = QueryExecutor::normalize_sql_for_execute(&cleaned).to_ascii_uppercase();
                if (upper.starts_with("BEGIN")
                    || upper.starts_with("DECLARE")
                    || upper.starts_with("CALL")
                    || upper.starts_with("EXEC "))
                    && upper.contains("DBMS_OUTPUT")
                {
                    let _ = get_dbms_output(exec_conn.as_ref(), 10_000);
                }
            }

            let open_cursor_count = query_single_string(
                probe_conn.as_ref(),
                "SELECT COUNT(*) FROM sys.v_$open_cursor WHERE sid = :1",
                &[ThinValue::Integer(exec_sid)],
            )
            .expect("open cursor count query must succeed");

            if cycle < 5 || (cycle + 1) % 5 == 0 {
                eprintln!(
                    "larger block cycle {}: open_cursors={}",
                    cycle + 1,
                    open_cursor_count,
                );
            }
        }
    }

    #[test]
    #[ignore = "diagnostic helper for PL/SQL cursor ids"]
    fn oracle_thin_diagnostic_plsql_statement_cursor_ids() {
        let info = test_connection_info();
        let conn = connect(&info).expect("oracle thin connection must succeed");
        let probe_conn = connect(&info).expect("oracle thin probe connection must succeed");
        let exec_sid = query_single_string(
            conn.as_ref(),
            "SELECT SYS_CONTEXT('USERENV', 'SID') FROM dual",
            &[],
        )
        .expect("execution session SID lookup must succeed");

        let plsql_statements = [
            "BEGIN NULL; END;",
            "BEGIN /* thin unique no-op */ NULL; END;",
            "BEGIN DBMS_OUTPUT.ENABLE(NULL); END;",
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE thin_diag_cursor_probe PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;",
        ];

        for statement in plsql_statements {
            let before = query_single_string(
                probe_conn.as_ref(),
                r#"
                    SELECT s.value
                    FROM sys.v_$sesstat s
                    JOIN sys.v_$statname n
                      ON n.statistic# = s.statistic#
                    WHERE s.sid = :1
                      AND n.name = 'opened cursors current'
                "#,
                &[ThinValue::Integer(exec_sid.parse::<i64>().unwrap())],
            )
            .expect("opened cursors current before query must succeed");

            let result = runtime()
                .block_on(conn.execute_plsql(statement, &[]))
                .unwrap_or_else(|err| {
                    panic!("PL/SQL statement must execute successfully:\n{statement}\n\n{err}")
                });

            eprintln!(
                "plsql={statement}\nstatement_cursor_id={}\nresult_cursor_id={:?}\nrows_affected={}",
                result.statement_cursor_id, result.cursor_id, result.rows_affected
            );

            queue_server_cursor_close(conn.as_ref(), result.statement_cursor_id);
            flush_pending_cursor_closes(conn.as_ref()).expect("pending cursor closes must flush");

            let after = query_single_string(
                probe_conn.as_ref(),
                r#"
                    SELECT s.value
                    FROM sys.v_$sesstat s
                    JOIN sys.v_$statname n
                      ON n.statistic# = s.statistic#
                    WHERE s.sid = :1
                      AND n.name = 'opened cursors current'
                "#,
                &[ThinValue::Integer(exec_sid.parse::<i64>().unwrap())],
            )
            .expect("opened cursors current after query must succeed");

            eprintln!("opened cursors current: before={before}, after={after}");

            flush_pending_cursor_closes(conn.as_ref())
                .expect("second pending cursor close flush must succeed");
            let after_second_flush = query_single_string(
                probe_conn.as_ref(),
                r#"
                    SELECT s.value
                    FROM sys.v_$sesstat s
                    JOIN sys.v_$statname n
                      ON n.statistic# = s.statistic#
                    WHERE s.sid = :1
                      AND n.name = 'opened cursors current'
                "#,
                &[ThinValue::Integer(exec_sid.parse::<i64>().unwrap())],
            )
            .expect("opened cursors current after second flush must succeed");
            eprintln!("opened cursors current after second flush: {after_second_flush}");
        }

        let before = query_single_string(
            probe_conn.as_ref(),
            r#"
                SELECT s.value
                FROM sys.v_$sesstat s
                JOIN sys.v_$statname n
                  ON n.statistic# = s.statistic#
                WHERE s.sid = :1
                  AND n.name = 'opened cursors current'
            "#,
            &[ThinValue::Integer(exec_sid.parse::<i64>().unwrap())],
        )
        .expect("opened cursors current before direct DDL must succeed");

        let ddl_result = runtime()
            .block_on(conn.execute("CREATE TABLE thin_diag_cursor_probe (id NUMBER)", &[]))
            .expect("direct DDL must execute successfully");
        eprintln!(
            "direct ddl cursor_id={}\nrows_affected={}",
            ddl_result.cursor_id, ddl_result.rows_affected
        );
        queue_server_cursor_close(conn.as_ref(), ddl_result.cursor_id);
        flush_pending_cursor_closes(conn.as_ref()).expect("pending cursor closes must flush");

        let after = query_single_string(
            probe_conn.as_ref(),
            r#"
                SELECT s.value
                FROM sys.v_$sesstat s
                JOIN sys.v_$statname n
                  ON n.statistic# = s.statistic#
                WHERE s.sid = :1
                  AND n.name = 'opened cursors current'
            "#,
            &[ThinValue::Integer(exec_sid.parse::<i64>().unwrap())],
        )
        .expect("opened cursors current after direct DDL must succeed");
        eprintln!("direct ddl opened cursors current: before={before}, after={after}");
        flush_pending_cursor_closes(conn.as_ref())
            .expect("second flush after direct DDL must succeed");
        let after_second_flush = query_single_string(
            probe_conn.as_ref(),
            r#"
                SELECT s.value
                FROM sys.v_$sesstat s
                JOIN sys.v_$statname n
                  ON n.statistic# = s.statistic#
                WHERE s.sid = :1
                  AND n.name = 'opened cursors current'
            "#,
            &[ThinValue::Integer(exec_sid.parse::<i64>().unwrap())],
        )
        .expect("opened cursors current after second direct DDL flush must succeed");
        eprintln!("direct ddl opened cursors current after second flush: {after_second_flush}");

        let top_sql = query_all(
            probe_conn.as_ref(),
            r#"
                SELECT *
                FROM (
                    SELECT SUBSTR(sql_text, 1, 160) AS sql_text,
                           COUNT(*) AS cursor_count
                    FROM sys.v_$open_cursor
                    WHERE sid = :1
                    GROUP BY SUBSTR(sql_text, 1, 160)
                    ORDER BY COUNT(*) DESC, SUBSTR(sql_text, 1, 160)
                )
                WHERE ROWNUM <= 15
            "#,
            &[ThinValue::Integer(exec_sid.parse::<i64>().unwrap())],
        )
        .expect("top open cursor query must succeed");

        for row in top_sql.rows {
            eprintln!("open cursor top SQL: {:?}", row);
        }

        for idx in 0..10 {
            let before = query_single_string(
                probe_conn.as_ref(),
                r#"
                    SELECT s.value
                    FROM sys.v_$sesstat s
                    JOIN sys.v_$statname n
                      ON n.statistic# = s.statistic#
                    WHERE s.sid = :1
                      AND n.name = 'opened cursors current'
                "#,
                &[ThinValue::Integer(exec_sid.parse::<i64>().unwrap())],
            )
            .expect("opened cursors current before repeated drop probe must succeed");
            let sql = format!(
                "BEGIN EXECUTE IMMEDIATE 'DROP TABLE thin_diag_cursor_probe_{} PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;",
                idx
            );
            let result = runtime()
                .block_on(conn.execute_plsql(&sql, &[]))
                .unwrap_or_else(|err| panic!("repeated drop probe must succeed:\n{sql}\n\n{err}"));
            queue_server_cursor_close(conn.as_ref(), result.statement_cursor_id);
            flush_pending_cursor_closes(conn.as_ref())
                .expect("flush after repeated drop probe must succeed");
            let after = query_single_string(
                probe_conn.as_ref(),
                r#"
                    SELECT s.value
                    FROM sys.v_$sesstat s
                    JOIN sys.v_$statname n
                      ON n.statistic# = s.statistic#
                    WHERE s.sid = :1
                      AND n.name = 'opened cursors current'
                "#,
                &[ThinValue::Integer(exec_sid.parse::<i64>().unwrap())],
            )
            .expect("opened cursors current after repeated drop probe must succeed");
            eprintln!("repeated unique drop probe {idx}: before={before}, after={after}");
        }

        for idx in 0..10 {
            let before = query_single_string(
                probe_conn.as_ref(),
                r#"
                    SELECT s.value
                    FROM sys.v_$sesstat s
                    JOIN sys.v_$statname n
                      ON n.statistic# = s.statistic#
                    WHERE s.sid = :1
                      AND n.name = 'opened cursors current'
                "#,
                &[ThinValue::Integer(exec_sid.parse::<i64>().unwrap())],
            )
            .expect("opened cursors current before repeated create/drop probe must succeed");

            let create_sql = format!("CREATE TABLE thin_diag_cursor_ct_{} (id NUMBER)", idx);
            let create_result = runtime()
                .block_on(conn.execute(&create_sql, &[]))
                .unwrap_or_else(|err| panic!("create probe must succeed:\n{create_sql}\n\n{err}"));
            queue_server_cursor_close(conn.as_ref(), create_result.cursor_id);
            flush_pending_cursor_closes(conn.as_ref())
                .expect("flush after create probe must succeed");

            let drop_sql = format!("DROP TABLE thin_diag_cursor_ct_{} PURGE", idx);
            let drop_result = runtime()
                .block_on(conn.execute(&drop_sql, &[]))
                .unwrap_or_else(|err| panic!("drop probe must succeed:\n{drop_sql}\n\n{err}"));
            queue_server_cursor_close(conn.as_ref(), drop_result.cursor_id);
            flush_pending_cursor_closes(conn.as_ref())
                .expect("flush after direct drop probe must succeed");

            let after = query_single_string(
                probe_conn.as_ref(),
                r#"
                    SELECT s.value
                    FROM sys.v_$sesstat s
                    JOIN sys.v_$statname n
                      ON n.statistic# = s.statistic#
                    WHERE s.sid = :1
                      AND n.name = 'opened cursors current'
                "#,
                &[ThinValue::Integer(exec_sid.parse::<i64>().unwrap())],
            )
            .expect("opened cursors current after repeated create/drop probe must succeed");
            eprintln!("repeated unique create/drop probe {idx}: before={before}, after={after}");
        }
    }

    #[test]
    #[ignore = "diagnostic helper for low-level test11 one-pass cursor growth"]
    fn oracle_thin_diagnostic_test11_one_pass_cursor_growth() {
        let info = test_connection_info();
        let exec_conn = connect(&info).expect("oracle thin execution connection must succeed");
        let probe_conn = connect(&info).expect("oracle thin probe connection must succeed");
        let exec_sid = query_single_string(
            exec_conn.as_ref(),
            "SELECT SYS_CONTEXT('USERENV', 'SID') FROM dual",
            &[],
        )
        .expect("execution session SID lookup must succeed")
        .parse::<i64>()
        .expect("execution SID must parse as i64");

        let items = QueryExecutor::split_script_items_for_db_type(
            include_str!("../../test/test11.txt"),
            Some(DatabaseType::OracleThin),
        );

        enable_dbms_output(exec_conn.as_ref(), Some(1_000_000))
            .expect("diagnostic test11 one-pass must enable DBMS_OUTPUT");

        let mut statement_index = 0usize;
        for item in items {
            let statement = match item {
                ScriptItem::Statement(statement) => statement,
                ScriptItem::ToolCommand(ToolCommand::SetServerOutput {
                    enabled,
                    size,
                    unlimited,
                }) => {
                    if enabled {
                        let resolved_size = if unlimited {
                            None
                        } else {
                            size.or(Some(1_000_000))
                        };
                        enable_dbms_output(exec_conn.as_ref(), resolved_size)
                            .expect("DBMS_OUTPUT enable must succeed");
                    } else {
                        disable_dbms_output(exec_conn.as_ref())
                            .expect("DBMS_OUTPUT disable must succeed");
                    }
                    continue;
                }
                ScriptItem::ToolCommand(ToolCommand::ShowErrors { .. })
                | ScriptItem::ToolCommand(ToolCommand::MysqlShowErrors)
                | ScriptItem::ToolCommand(ToolCommand::Prompt { .. }) => {
                    continue;
                }
                ScriptItem::ToolCommand(other) => {
                    panic!("unexpected test11 tool command in one-pass diagnostic: {other:?}");
                }
            };

            statement_index += 1;
            let cleaned = QueryExecutor::strip_leading_comments(&statement);
            if QueryExecutor::is_select_statement(&cleaned) {
                let (_result, cancelled) = execute_select_streaming_with_binds(
                    exec_conn.as_ref(),
                    &cleaned,
                    &[],
                    &mut |_columns| {},
                    &mut |_row| true,
                )
                .unwrap_or_else(|err| {
                    panic!(
                        "test11 one-pass select must succeed at statement {}.\nSQL:\n{}\n\n{}",
                        statement_index, cleaned, err
                    )
                });
                assert!(
                    !cancelled,
                    "test11 one-pass select must not be cancelled at statement {}",
                    statement_index
                );
            } else {
                execute_statement_with_binds(exec_conn.as_ref(), &cleaned, &[]).unwrap_or_else(
                    |err| {
                        panic!(
                            "test11 one-pass statement must succeed at statement {}.\nSQL:\n{}\n\n{}",
                            statement_index, cleaned, err
                        )
                    },
                );
            }

            let upper = QueryExecutor::normalize_sql_for_execute(&cleaned).to_ascii_uppercase();
            if (upper.starts_with("BEGIN")
                || upper.starts_with("DECLARE")
                || upper.starts_with("CALL")
                || upper.starts_with("EXEC "))
                && upper.contains("DBMS_OUTPUT")
            {
                let _ = get_dbms_output(exec_conn.as_ref(), 10_000);
            }

            if statement_index <= 20 || statement_index % 250 == 0 {
                let opened = query_single_string(
                    probe_conn.as_ref(),
                    r#"
                        SELECT s.value
                        FROM sys.v_$sesstat s
                        JOIN sys.v_$statname n
                          ON n.statistic# = s.statistic#
                        WHERE s.sid = :1
                          AND n.name = 'opened cursors current'
                    "#,
                    &[ThinValue::Integer(exec_sid)],
                )
                .expect("opened cursors current query must succeed");
                eprintln!(
                    "test11 one-pass statement {}: opened_cursors_current={}",
                    statement_index, opened
                );
            }

            if statement_index >= 6000 {
                break;
            }
        }
    }

    #[test]
    #[ignore = "diagnostic helper for first leaking test11 statements"]
    fn oracle_thin_diagnostic_test11_first_leaking_statements() {
        let info = test_connection_info();
        let exec_conn = connect(&info).expect("oracle thin execution connection must succeed");
        let probe_conn = connect(&info).expect("oracle thin probe connection must succeed");
        let exec_sid = query_single_string(
            exec_conn.as_ref(),
            "SELECT SYS_CONTEXT('USERENV', 'SID') FROM dual",
            &[],
        )
        .expect("execution session SID lookup must succeed")
        .parse::<i64>()
        .expect("execution SID must parse as i64");

        let items = QueryExecutor::split_script_items_for_db_type(
            include_str!("../../test/test11.txt"),
            Some(DatabaseType::OracleThin),
        );

        enable_dbms_output(exec_conn.as_ref(), Some(1_000_000))
            .expect("diagnostic leak test must enable DBMS_OUTPUT");

        let mut statement_index = 0usize;
        let mut previous_opened = query_single_string(
            probe_conn.as_ref(),
            r#"
                SELECT s.value
                FROM sys.v_$sesstat s
                JOIN sys.v_$statname n
                  ON n.statistic# = s.statistic#
                WHERE s.sid = :1
                  AND n.name = 'opened cursors current'
            "#,
            &[ThinValue::Integer(exec_sid)],
        )
        .expect("initial opened cursors current query must succeed")
        .parse::<i64>()
        .expect("initial opened cursors current must parse as i64");

        for item in items {
            let statement = match item {
                ScriptItem::Statement(statement) => statement,
                ScriptItem::ToolCommand(ToolCommand::SetServerOutput {
                    enabled,
                    size,
                    unlimited,
                }) => {
                    if enabled {
                        let resolved_size = if unlimited {
                            None
                        } else {
                            size.or(Some(1_000_000))
                        };
                        enable_dbms_output(exec_conn.as_ref(), resolved_size)
                            .expect("DBMS_OUTPUT enable must succeed");
                    } else {
                        disable_dbms_output(exec_conn.as_ref())
                            .expect("DBMS_OUTPUT disable must succeed");
                    }
                    continue;
                }
                ScriptItem::ToolCommand(ToolCommand::ShowErrors { .. })
                | ScriptItem::ToolCommand(ToolCommand::MysqlShowErrors)
                | ScriptItem::ToolCommand(ToolCommand::Prompt { .. }) => continue,
                ScriptItem::ToolCommand(other) => {
                    panic!("unexpected test11 tool command in leak diagnostic: {other:?}");
                }
            };

            statement_index += 1;
            let cleaned = QueryExecutor::strip_leading_comments(&statement);
            if QueryExecutor::is_select_statement(&cleaned) {
                let (_result, cancelled) = execute_select_streaming_with_binds(
                    exec_conn.as_ref(),
                    &cleaned,
                    &[],
                    &mut |_columns| {},
                    &mut |_row| true,
                )
                .unwrap_or_else(|err| {
                    panic!(
                        "test11 leak diagnostic select must succeed at statement {}.\nSQL:\n{}\n\n{}",
                        statement_index, cleaned, err
                    )
                });
                assert!(
                    !cancelled,
                    "test11 leak diagnostic select must not be cancelled at statement {}",
                    statement_index
                );
            } else {
                execute_statement_with_binds(exec_conn.as_ref(), &cleaned, &[]).unwrap_or_else(
                    |err| {
                        panic!(
                            "test11 leak diagnostic statement must succeed at statement {}.\nSQL:\n{}\n\n{}",
                            statement_index, cleaned, err
                        )
                    },
                );
            }

            let upper = QueryExecutor::normalize_sql_for_execute(&cleaned).to_ascii_uppercase();
            if (upper.starts_with("BEGIN")
                || upper.starts_with("DECLARE")
                || upper.starts_with("CALL")
                || upper.starts_with("EXEC "))
                && upper.contains("DBMS_OUTPUT")
            {
                let _ = get_dbms_output(exec_conn.as_ref(), 10_000);
            }

            let opened = query_single_string(
                probe_conn.as_ref(),
                r#"
                    SELECT s.value
                    FROM sys.v_$sesstat s
                    JOIN sys.v_$statname n
                      ON n.statistic# = s.statistic#
                    WHERE s.sid = :1
                      AND n.name = 'opened cursors current'
                "#,
                &[ThinValue::Integer(exec_sid)],
            )
            .expect("opened cursors current query must succeed")
            .parse::<i64>()
            .expect("opened cursors current must parse as i64");

            if opened != previous_opened {
                eprintln!(
                    "statement {} changed opened cursors current: {} -> {}\n{}\n---",
                    statement_index, previous_opened, opened, cleaned
                );
            }
            previous_opened = opened;

            if statement_index >= 120 {
                break;
            }
        }
    }

    #[test]
    #[ignore = "diagnostic helper for object DDL cursor ids"]
    fn oracle_thin_diagnostic_object_ddl_cursor_ids() {
        let info = test_connection_info();
        let conn = connect(&info).expect("oracle thin connection must succeed");
        let probe_conn = connect(&info).expect("oracle thin probe connection must succeed");
        let exec_sid = query_single_string(
            conn.as_ref(),
            "SELECT SYS_CONTEXT('USERENV', 'SID') FROM dual",
            &[],
        )
        .expect("execution session SID lookup must succeed")
        .parse::<i64>()
        .expect("execution SID must parse as i64");

        let statements = [
            "BEGIN EXECUTE IMMEDIATE 'DROP TYPE thin_diag_obj_tab FORCE'; EXCEPTION WHEN OTHERS THEN NULL; END;",
            "BEGIN EXECUTE IMMEDIATE 'DROP TYPE thin_diag_obj FORCE'; EXCEPTION WHEN OTHERS THEN NULL; END;",
            "CREATE OR REPLACE TYPE thin_diag_obj AS OBJECT (id NUMBER, name VARCHAR2(30))",
            "CREATE OR REPLACE TYPE thin_diag_obj_tab AS TABLE OF thin_diag_obj",
        ];

        for statement in statements {
            let before = query_single_string(
                probe_conn.as_ref(),
                r#"
                    SELECT s.value
                    FROM sys.v_$sesstat s
                    JOIN sys.v_$statname n
                      ON n.statistic# = s.statistic#
                    WHERE s.sid = :1
                      AND n.name = 'opened cursors current'
                "#,
                &[ThinValue::Integer(exec_sid)],
            )
            .expect("opened cursors current before object DDL query must succeed");

            let result = runtime()
                .block_on(conn.execute(statement, &[]))
                .unwrap_or_else(|err| {
                    panic!("object DDL statement must execute successfully:\n{statement}\n\n{err}")
                });
            eprintln!(
                "object ddl sql={statement}\nquery_result.cursor_id={}\nrows_affected={}",
                result.cursor_id, result.rows_affected
            );
            queue_server_cursor_close(conn.as_ref(), result.cursor_id);
            flush_pending_cursor_closes(conn.as_ref())
                .expect("pending cursor closes after object DDL must flush");

            let after = query_single_string(
                probe_conn.as_ref(),
                r#"
                    SELECT s.value
                    FROM sys.v_$sesstat s
                    JOIN sys.v_$statname n
                      ON n.statistic# = s.statistic#
                    WHERE s.sid = :1
                      AND n.name = 'opened cursors current'
                "#,
                &[ThinValue::Integer(exec_sid)],
            )
            .expect("opened cursors current after object DDL query must succeed");

            eprintln!("object ddl opened cursors current: before={before}, after={after}");
        }
    }

    #[test]
    #[ignore = "requires local Oracle XE at localhost:1521/FREE"]
    fn oracle_thin_executes_test8_package_body_from_split_script() {
        let info = test_connection_info();
        let conn = connect(&info).expect("oracle thin connection must succeed");
        let items = QueryExecutor::split_script_items_for_db_type(
            include_str!("../../test/test8.txt"),
            Some(DatabaseType::OracleThin),
        );

        let mut package_body_seen = false;
        for item in items {
            match item {
                ScriptItem::Statement(statement) => {
                    let result = execute_statement_with_binds(conn.as_ref(), &statement, &[]);
                    if statement.starts_with("CREATE OR REPLACE PACKAGE BODY oqt_mega_pkg AS") {
                        package_body_seen = true;
                        result.expect("test8 package body must execute successfully");
                        break;
                    } else {
                        result.unwrap_or_else(|err| {
                            panic!(
                                "setup statement failed before test8 package body:\n{statement}\n\n{err}"
                            )
                        });
                    }
                }
                ScriptItem::ToolCommand(ToolCommand::SetServerOutput {
                    enabled,
                    size,
                    unlimited,
                }) => {
                    if enabled {
                        let resolved_size = if unlimited {
                            None
                        } else {
                            size.or(Some(1_000_000))
                        };
                        enable_dbms_output(conn.as_ref(), resolved_size)
                            .expect("DBMS_OUTPUT enable must succeed");
                    } else {
                        disable_dbms_output(conn.as_ref())
                            .expect("DBMS_OUTPUT disable must succeed");
                    }
                }
                ScriptItem::ToolCommand(ToolCommand::ShowErrors { .. })
                | ScriptItem::ToolCommand(ToolCommand::MysqlShowErrors)
                | ScriptItem::ToolCommand(ToolCommand::Prompt { .. })
                | ScriptItem::ToolCommand(ToolCommand::Var { .. })
                | ScriptItem::ToolCommand(ToolCommand::Define { .. })
                | ScriptItem::ToolCommand(ToolCommand::SetFeedback { .. })
                | ScriptItem::ToolCommand(_) => {}
            }
        }

        assert!(
            package_body_seen,
            "test8 package body statement must be found"
        );
        let _ = close(conn.as_ref());
    }

    #[test]
    #[ignore = "requires local Oracle XE at localhost:1521/FREE"]
    fn oracle_thin_dbms_output_fetch_after_silent_block_keeps_connection_usable() {
        let info = test_connection_info();
        let conn = connect(&info).expect("oracle thin connection must succeed");

        enable_dbms_output(conn.as_ref(), Some(1_000_000)).expect("DBMS_OUTPUT enable must work");
        execute_statement_with_binds(
            conn.as_ref(),
            r#"
            BEGIN
              NULL;
            END;
            "#,
            &[],
        )
        .expect("silent PL/SQL block must execute");

        let silent_lines =
            get_dbms_output(conn.as_ref(), 10).expect("silent DBMS_OUTPUT fetch must succeed");
        assert!(
            silent_lines.is_empty(),
            "silent block must not produce DBMS_OUTPUT lines: {silent_lines:?}"
        );

        execute_statement_with_binds(
            conn.as_ref(),
            "BEGIN DBMS_OUTPUT.PUT_LINE('thin-silent-ok'); END;",
            &[],
        )
        .expect("DBMS_OUTPUT producer block must execute");
        let output_lines = get_dbms_output(conn.as_ref(), 10)
            .expect("DBMS_OUTPUT fetch after silent block must succeed");
        assert!(
            output_lines
                .iter()
                .any(|line| line.contains("thin-silent-ok")),
            "expected DBMS_OUTPUT line after silent block, got {output_lines:?}"
        );

        let _ = close(conn.as_ref());
    }
}
