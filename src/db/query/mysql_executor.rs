use mysql::prelude::*;
use mysql::{Conn, Error as MysqlError, Row, Value as MysqlValue};
use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::db::connection::ConnectionInfo;
use crate::sql_text;

use super::executor::{ConstraintInfo, IndexInfo, QueryExecutor, TableColumnDetail};
use super::types::{ColumnInfo, ProcedureArgument, QueryResult};

pub struct MysqlExecutor;

pub struct MysqlObjectBrowser;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MysqlStatementKind {
    Select,
    Dml,
    Commit,
    Rollback,
    Use,
    Call,
    Ddl,
}

#[derive(Debug, Clone)]
struct MysqlResultSetSnapshot {
    columns: Vec<ColumnInfo>,
    rows: Vec<Vec<String>>,
    affected_rows: u64,
    info: String,
}

impl MysqlExecutor {
    fn timeout_millis(timeout: Option<Duration>) -> u128 {
        timeout.map(|value| value.as_millis()).unwrap_or(0)
    }

    fn mysql_timeout_statement(timeout: Option<Duration>) -> String {
        format!(
            "SET SESSION MAX_EXECUTION_TIME = {}",
            Self::timeout_millis(timeout)
        )
    }

    fn mariadb_timeout_statement(timeout: Option<Duration>) -> String {
        let timeout_seconds = timeout.map(|value| value.as_secs_f64()).unwrap_or(0.0);
        format!("SET SESSION max_statement_time = {:.3}", timeout_seconds)
    }

    fn is_unknown_system_variable_error(err: &MysqlError, variable_name: &str) -> bool {
        match err {
            MysqlError::MySqlError(server_err) => {
                server_err.code == 1193
                    || server_err
                        .message
                        .contains(&format!("Unknown system variable '{variable_name}'"))
            }
            _ => false,
        }
    }

    pub(crate) fn apply_session_timeout(
        conn: &mut Conn,
        timeout: Option<Duration>,
    ) -> Result<(), MysqlError> {
        let statement = Self::mysql_timeout_statement(timeout);
        match conn.query_drop(statement.as_str()) {
            Ok(()) => Ok(()),
            Err(err) if Self::is_unknown_system_variable_error(&err, "MAX_EXECUTION_TIME") => {
                let fallback = Self::mariadb_timeout_statement(timeout);
                conn.query_drop(fallback.as_str())
            }
            Err(err) => Err(err),
        }
    }

    fn classify_statement(sql: &str) -> MysqlStatementKind {
        let normalized = QueryExecutor::normalize_sql_for_execute(sql);
        if QueryExecutor::is_select_statement(&normalized) {
            return MysqlStatementKind::Select;
        }

        match QueryExecutor::leading_keyword(&normalized).as_deref() {
            Some("INSERT") | Some("UPDATE") | Some("DELETE") | Some("REPLACE") | Some("WITH") => {
                MysqlStatementKind::Dml
            }
            Some("COMMIT") => MysqlStatementKind::Commit,
            Some("ROLLBACK") => MysqlStatementKind::Rollback,
            Some("USE") => MysqlStatementKind::Use,
            Some("CALL") => MysqlStatementKind::Call,
            // SHOW, DESCRIBE/DESC, EXPLAIN, and several table-maintenance
            // statements return tabular result sets in MySQL/MariaDB; route
            // them through execute_select so the results are not silently
            // discarded by query_drop().
            Some("SHOW") | Some("DESCRIBE") | Some("DESC") | Some("EXPLAIN") | Some("ANALYZE")
            | Some("CHECK") | Some("CHECKSUM") | Some("OPTIMIZE") | Some("REPAIR") => {
                MysqlStatementKind::Select
            }
            _ => MysqlStatementKind::Ddl,
        }
    }

    fn row_to_strings(row: &Row, column_count: usize) -> Vec<String> {
        let mut row_values = Vec::with_capacity(column_count);
        for index in 0..column_count {
            let value = row
                .as_ref(index)
                .map(Self::value_to_string)
                .unwrap_or_else(|| "NULL".to_string());
            row_values.push(value);
        }
        row_values
    }

    fn value_to_string(value: &MysqlValue) -> String {
        match value {
            MysqlValue::NULL => "NULL".to_string(),
            MysqlValue::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            MysqlValue::Int(number) => number.to_string(),
            MysqlValue::UInt(number) => number.to_string(),
            MysqlValue::Float(number) => number.to_string(),
            MysqlValue::Double(number) => number.to_string(),
            MysqlValue::Date(year, month, day, hour, minute, second, micros) => {
                if *year == 0 && *month == 0 && *day == 0 {
                    return "0000-00-00".to_string();
                }
                if *hour == 0 && *minute == 0 && *second == 0 && *micros == 0 {
                    format!("{year:04}-{month:02}-{day:02}")
                } else if *micros == 0 {
                    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
                } else {
                    format!(
                        "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}"
                    )
                }
            }
            MysqlValue::Time(is_negative, days, hours, minutes, seconds, micros) => {
                let sign = if *is_negative { "-" } else { "" };
                let total_hours = days.saturating_mul(24).saturating_add(*hours as u32);
                if *micros == 0 {
                    format!("{sign}{total_hours:02}:{minutes:02}:{seconds:02}")
                } else {
                    format!("{sign}{total_hours:02}:{minutes:02}:{seconds:02}.{micros:06}")
                }
            }
        }
    }

    pub fn execute(conn: &mut Conn, sql: &str) -> Result<Vec<QueryResult>, MysqlError> {
        let trimmed = sql.trim();
        match Self::classify_statement(trimmed) {
            MysqlStatementKind::Select => Ok(vec![Self::execute_select(conn, sql)?]),
            MysqlStatementKind::Dml => Ok(vec![Self::execute_dml(conn, sql)?]),
            MysqlStatementKind::Commit => {
                let start = Instant::now();
                conn.query_drop("COMMIT")?;
                Ok(vec![QueryResult {
                    sql: sql.to_string(),
                    columns: vec![],
                    rows: vec![],
                    row_count: 0,
                    execution_time: start.elapsed(),
                    message: "Commit complete.".to_string(),
                    is_select: false,
                    success: true,
                }])
            }
            MysqlStatementKind::Rollback => {
                let start = Instant::now();
                conn.query_drop("ROLLBACK")?;
                Ok(vec![QueryResult {
                    sql: sql.to_string(),
                    columns: vec![],
                    rows: vec![],
                    row_count: 0,
                    execution_time: start.elapsed(),
                    message: "Rollback complete.".to_string(),
                    is_select: false,
                    success: true,
                }])
            }
            MysqlStatementKind::Use => {
                let start = Instant::now();
                conn.query_drop(sql)?;
                let db_name = Self::extract_use_database_name(trimmed);
                Ok(vec![QueryResult {
                    sql: sql.to_string(),
                    columns: vec![],
                    rows: vec![],
                    row_count: 0,
                    execution_time: start.elapsed(),
                    message: format!("Database changed to '{}'.", db_name),
                    is_select: false,
                    success: true,
                }])
            }
            MysqlStatementKind::Call => Self::execute_call(conn, sql),
            MysqlStatementKind::Ddl => Ok(vec![Self::execute_ddl(conn, sql)?]),
        }
    }

    fn execute_select(conn: &mut Conn, sql: &str) -> Result<QueryResult, MysqlError> {
        let start = Instant::now();
        let result = conn.query_iter(sql)?;

        let columns: Vec<ColumnInfo> = result
            .columns()
            .as_ref()
            .iter()
            .map(|col| ColumnInfo {
                name: col.name_str().to_string(),
                data_type: format!("{:?}", col.column_type()),
            })
            .collect();

        let mut rows: Vec<Vec<String>> = Vec::new();
        for row_result in result {
            let row: Row = row_result?;
            rows.push(Self::row_to_strings(&row, columns.len()));
        }

        Ok(QueryResult::new_select(sql, columns, rows, start.elapsed()))
    }

    pub fn execute_select_streaming<F, G>(
        conn: &mut Conn,
        sql: &str,
        on_select_start: &mut F,
        on_row: &mut G,
    ) -> Result<(QueryResult, bool), MysqlError>
    where
        F: FnMut(&[ColumnInfo]),
        G: FnMut(Vec<String>) -> bool,
    {
        let start = Instant::now();
        let result = conn.query_iter(sql)?;

        let columns: Vec<ColumnInfo> = result
            .columns()
            .as_ref()
            .iter()
            .map(|col| ColumnInfo {
                name: col.name_str().to_string(),
                data_type: format!("{:?}", col.column_type()),
            })
            .collect();

        on_select_start(&columns);

        let mut row_count: usize = 0;
        let mut cancelled = false;

        for row_result in result {
            let row: Row = row_result?;
            row_count += 1;
            if !on_row(Self::row_to_strings(&row, columns.len())) {
                cancelled = true;
                break;
            }
        }

        let result = QueryResult::new_select_streamed(sql, columns, row_count, start.elapsed());
        Ok((result, cancelled))
    }

    fn execute_dml(conn: &mut Conn, sql: &str) -> Result<QueryResult, MysqlError> {
        let start = Instant::now();
        conn.query_drop(sql)?;
        let affected = conn.affected_rows();
        let trimmed = sql.trim();
        let stmt_type = if QueryExecutor::leading_keyword(trimmed)
            .as_deref()
            .is_some_and(|keyword| keyword.eq_ignore_ascii_case("WITH"))
        {
            "DML".to_string()
        } else {
            trimmed
                .split_whitespace()
                .next()
                .unwrap_or("DML")
                .to_ascii_uppercase()
        };
        Ok(QueryResult::new_dml(
            sql,
            affected,
            start.elapsed(),
            &stmt_type,
        ))
    }

    fn execute_ddl(conn: &mut Conn, sql: &str) -> Result<QueryResult, MysqlError> {
        let start = Instant::now();
        conn.query_drop(sql)?;
        let trimmed = sql.trim();
        let stmt_type = trimmed
            .split_whitespace()
            .take(2)
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_uppercase();
        Ok(QueryResult {
            sql: sql.to_string(),
            columns: vec![],
            rows: vec![],
            row_count: 0,
            execution_time: start.elapsed(),
            message: format!("{} executed.", stmt_type),
            is_select: false,
            success: true,
        })
    }

    fn call_result_from_snapshot(
        sql: &str,
        snapshot: MysqlResultSetSnapshot,
        execution_time: Duration,
    ) -> Option<QueryResult> {
        if !snapshot.columns.is_empty() {
            return Some(QueryResult::new_select(
                sql,
                snapshot.columns,
                snapshot.rows,
                execution_time,
            ));
        }

        let info = snapshot.info.trim();
        if snapshot.affected_rows > 0 {
            let mut result =
                QueryResult::new_dml(sql, snapshot.affected_rows, execution_time, "CALL");
            if !info.is_empty() {
                result.message = format!("{} | {}", result.message, info);
            }
            return Some(result);
        }

        if !info.is_empty() {
            return Some(QueryResult {
                sql: sql.to_string(),
                columns: vec![],
                rows: vec![],
                row_count: 0,
                execution_time,
                message: format!("CALL executed. {}", info),
                is_select: false,
                success: true,
            });
        }

        None
    }

    fn default_call_result(sql: &str, execution_time: Duration) -> QueryResult {
        QueryResult {
            sql: sql.to_string(),
            columns: vec![],
            rows: vec![],
            row_count: 0,
            execution_time,
            message: "CALL executed.".to_string(),
            is_select: false,
            success: true,
        }
    }

    fn materialize_call_results(
        sql: &str,
        snapshots: Vec<MysqlResultSetSnapshot>,
        execution_time: Duration,
    ) -> Vec<QueryResult> {
        let mut results = snapshots
            .into_iter()
            .filter_map(|snapshot| Self::call_result_from_snapshot(sql, snapshot, execution_time))
            .collect::<Vec<_>>();
        if results.is_empty() {
            results.push(Self::default_call_result(sql, execution_time));
        }
        results
    }

    fn execute_call(conn: &mut Conn, sql: &str) -> Result<Vec<QueryResult>, MysqlError> {
        // CALL may return multiple select and non-select result sets.
        let start = Instant::now();
        let mut query_result = conn.query_iter(sql)?;
        let mut snapshots = Vec::new();

        while let Some(mut result_set) = query_result.iter() {
            let columns: Vec<ColumnInfo> = result_set
                .columns()
                .as_ref()
                .iter()
                .map(|col| ColumnInfo {
                    name: col.name_str().to_string(),
                    data_type: format!("{:?}", col.column_type()),
                })
                .collect();

            let mut rows = Vec::new();
            for row_result in result_set.by_ref() {
                let row: Row = row_result?;
                if !columns.is_empty() {
                    rows.push(Self::row_to_strings(&row, columns.len()));
                }
            }

            snapshots.push(MysqlResultSetSnapshot {
                columns,
                rows,
                affected_rows: result_set.affected_rows(),
                info: result_set.info_str().into_owned(),
            });
        }

        Ok(Self::materialize_call_results(
            sql,
            snapshots,
            start.elapsed(),
        ))
    }

    pub fn execute_batch(
        conn: &mut Conn,
        statements: &[String],
    ) -> Vec<Result<QueryResult, MysqlError>> {
        let mut results = Vec::new();
        for stmt in statements {
            let trimmed = stmt.trim();
            if trimmed.is_empty() {
                continue;
            }
            match Self::execute(conn, trimmed) {
                Ok(statement_results) => {
                    results.extend(statement_results.into_iter().map(Ok));
                }
                Err(err) => results.push(Err(err)),
            }
        }
        results
    }

    fn format_explain_lines(columns: &[ColumnInfo], rows: &[Vec<String>]) -> Vec<String> {
        if columns.is_empty() {
            return vec!["No EXPLAIN output.".to_string()];
        }

        let sanitize = |value: &str| value.replace(['\r', '\n'], " ");

        let mut widths: Vec<usize> = columns
            .iter()
            .map(|column| sanitize(&column.name).len())
            .collect();

        for row in rows {
            for (index, value) in row.iter().enumerate() {
                if let Some(width) = widths.get_mut(index) {
                    *width = (*width).max(sanitize(value).len());
                }
            }
        }

        let format_row = |values: Vec<String>| {
            values
                .into_iter()
                .enumerate()
                .map(|(index, value)| {
                    let width = widths.get(index).copied().unwrap_or(value.len());
                    format!("{value:<width$}")
                })
                .collect::<Vec<_>>()
                .join(" | ")
        };

        let mut lines = Vec::with_capacity(rows.len().saturating_add(2));
        lines.push(format_row(
            columns
                .iter()
                .map(|column| sanitize(&column.name))
                .collect::<Vec<_>>(),
        ));
        lines.push(
            widths
                .iter()
                .map(|width| "-".repeat(*width))
                .collect::<Vec<_>>()
                .join("-+-"),
        );

        for row in rows {
            lines.push(format_row(
                row.iter().map(|value| sanitize(value)).collect::<Vec<_>>(),
            ));
        }

        lines
    }

    pub fn get_explain_plan(conn: &mut Conn, sql: &str) -> Result<Vec<String>, MysqlError> {
        let explain_sql = format!("EXPLAIN {}", sql);
        let result = Self::execute_select(conn, &explain_sql)?;
        Ok(Self::format_explain_lines(
            result.columns.as_slice(),
            result.rows.as_slice(),
        ))
    }

    fn build_cancel_opts(info: &ConnectionInfo) -> mysql::OptsBuilder {
        let mut opts = mysql::OptsBuilder::new()
            .ip_or_hostname(Some(&info.host))
            .tcp_port(info.port)
            .user(Some(&info.username))
            .pass(Some(&info.password));

        let database = info.service_name.trim();
        if !database.is_empty() {
            opts = opts.db_name(Some(database));
        }

        opts
    }

    pub fn cancel_running_query(
        info: &ConnectionInfo,
        connection_id: u32,
    ) -> Result<(), MysqlError> {
        let opts = Self::build_cancel_opts(info);
        let mut cancel_conn = mysql::Conn::new(opts)?;
        let kill_sql = format!("KILL QUERY {connection_id}");
        cancel_conn.query_drop(kill_sql.as_str())
    }

    /// Extract the database name from a `USE <db>` statement for display purposes.
    /// Handles backtick-quoted identifiers, including names containing spaces.
    fn extract_use_database_name(trimmed_use_sql: &str) -> String {
        let bytes = trimmed_use_sql.as_bytes();
        let mut index = 0usize;

        loop {
            while bytes
                .get(index)
                .is_some_and(|byte| byte.is_ascii_whitespace())
            {
                index += 1;
            }

            if bytes.get(index) == Some(&b'/') && bytes.get(index + 1) == Some(&b'*') {
                index += 2;
                while index + 1 < bytes.len() {
                    if bytes[index] == b'*' && bytes[index + 1] == b'/' {
                        index += 2;
                        break;
                    }
                    index += 1;
                }
                continue;
            }

            break;
        }

        if bytes
            .get(index..index.saturating_add("USE".len()))
            .is_some_and(|slice| slice.eq_ignore_ascii_case(b"USE"))
        {
            index = index.saturating_add("USE".len());
        }

        loop {
            while bytes
                .get(index)
                .is_some_and(|byte| byte.is_ascii_whitespace())
            {
                index += 1;
            }

            if bytes.get(index) == Some(&b'/') && bytes.get(index + 1) == Some(&b'*') {
                index += 2;
                while index + 1 < bytes.len() {
                    if bytes[index] == b'*' && bytes[index + 1] == b'/' {
                        index += 2;
                        break;
                    }
                    index += 1;
                }
                continue;
            }

            break;
        }

        let after_use = trimmed_use_sql.get(index..).unwrap_or("").trim_start();
        if after_use.starts_with('`') {
            // Backtick-quoted identifier: scan for the closing backtick,
            // treating `` as an escaped backtick.
            let bytes = after_use.as_bytes();
            let mut idx = 1usize;
            while idx < bytes.len() {
                if bytes[idx] == b'`' {
                    if bytes.get(idx + 1) == Some(&b'`') {
                        idx += 2;
                    } else {
                        break;
                    }
                } else {
                    idx += 1;
                }
            }
            after_use
                .get(1..idx)
                .unwrap_or(after_use)
                .replace("``", "`")
        } else {
            // Unquoted: take the first whitespace/semicolon-delimited token.
            after_use
                .split(|c: char| c.is_ascii_whitespace() || c == ';')
                .next()
                .unwrap_or("")
                .to_string()
        }
    }

    /// Check if a MySQL error is a timeout/cancelled error.
    pub fn is_timeout_error(err: &MysqlError) -> bool {
        let lowered = err.to_string().to_ascii_lowercase();
        matches!(err, MysqlError::MySqlError(server_err) if server_err.code == 3024)
            || lowered.contains("max_execution_time")
            || lowered.contains("max statement time exceeded")
            || lowered.contains("maximum statement execution time exceeded")
            || lowered.contains("query timed out")
    }

    pub fn is_cancel_error(err: &MysqlError) -> bool {
        if Self::is_timeout_error(err) {
            return false;
        }

        let lowered = err.to_string().to_ascii_lowercase();
        matches!(err, MysqlError::MySqlError(server_err) if server_err.code == 1317)
            || lowered.contains("query execution was interrupted")
            || lowered.contains("query was killed")
    }
}

// ---------------------------------------------------------------------------
// MySQL Object Browser
// ---------------------------------------------------------------------------

pub struct MysqlTableColumnDetail {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub default_value: Option<String>,
    pub is_primary_key: bool,
    pub extra: String,
}

impl MysqlObjectBrowser {
    fn create_ddl_column_names(object_type: &str) -> &'static [&'static str] {
        match object_type {
            "TABLE" | "VIEW" => &["Create Table", "Create View"],
            "PROCEDURE" => &["Create Procedure"],
            "FUNCTION" => &["Create Function"],
            "TRIGGER" => &["SQL Original Statement", "Create Trigger"],
            "EVENT" => &["Create Event"],
            "SEQUENCE" => &["Create Sequence", "Create Table"],
            _ => &[],
        }
    }

    fn optional_schema_param(schema_name: Option<&str>) -> Option<String> {
        schema_name
            .map(str::trim)
            .filter(|schema| !schema.is_empty())
            .map(ToOwned::to_owned)
    }

    fn escape_identifier(identifier: &str) -> String {
        identifier.replace('`', "``")
    }

    fn quoted_identifier(identifier: &str) -> String {
        format!("`{}`", Self::escape_identifier(identifier))
    }

    fn skip_ascii_whitespace(source: &str, mut index: usize) -> usize {
        let bytes = source.as_bytes();
        while bytes
            .get(index)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            index += 1;
        }
        index
    }

    fn append_comment_free_segment(cleaned: &mut String, source: &str, start: usize, end: usize) {
        if start >= end {
            return;
        }

        if let Some(segment) = source.get(start..end) {
            cleaned.push_str(segment);
        }
    }

    fn ensure_comment_gap(cleaned: &mut String) {
        if cleaned.chars().last().is_some_and(|ch| !ch.is_whitespace()) {
            cleaned.push(' ');
        }
    }

    /// Strip MySQL/MariaDB inline comments from `source`, honouring single-
    /// quoted, double-quoted and backtick-quoted literals.
    ///
    /// Comments are replaced with at most one separating space so adjacent
    /// tokens remain parseable after removal.
    fn strip_mysql_inline_comments(source: &str) -> String {
        let bytes = source.as_bytes();
        let mut cleaned = String::with_capacity(source.len());
        let mut segment_start = 0usize;
        let mut index = 0usize;
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_backtick = false;

        while index < bytes.len() {
            let byte = bytes[index];
            let next = bytes.get(index + 1).copied();

            if in_single_quote {
                if byte == b'\\' && next.is_some() {
                    index += 2;
                    continue;
                }
                if byte == b'\'' {
                    if next == Some(b'\'') {
                        index += 2;
                        continue;
                    }
                    in_single_quote = false;
                }
                index += 1;
                continue;
            }

            if in_double_quote {
                if byte == b'\\' && next.is_some() {
                    index += 2;
                    continue;
                }
                if byte == b'"' {
                    if next == Some(b'"') {
                        index += 2;
                        continue;
                    }
                    in_double_quote = false;
                }
                index += 1;
                continue;
            }

            if in_backtick {
                if byte == b'`' {
                    if next == Some(b'`') {
                        index += 2;
                        continue;
                    }
                    in_backtick = false;
                }
                index += 1;
                continue;
            }

            match byte {
                b'\'' => {
                    in_single_quote = true;
                    index += 1;
                    continue;
                }
                b'"' => {
                    in_double_quote = true;
                    index += 1;
                    continue;
                }
                b'`' => {
                    in_backtick = true;
                    index += 1;
                    continue;
                }
                b'#' => {
                    Self::append_comment_free_segment(&mut cleaned, source, segment_start, index);
                    Self::ensure_comment_gap(&mut cleaned);
                    while index < bytes.len() && bytes[index] != b'\n' {
                        index += 1;
                    }
                    segment_start = Self::skip_ascii_whitespace(source, index);
                    continue;
                }
                b'-' if sql_text::is_mysql_dash_comment_start(bytes, index) => {
                    Self::append_comment_free_segment(&mut cleaned, source, segment_start, index);
                    Self::ensure_comment_gap(&mut cleaned);
                    while index < bytes.len() && bytes[index] != b'\n' {
                        index += 1;
                    }
                    segment_start = Self::skip_ascii_whitespace(source, index);
                    continue;
                }
                b'/' if next == Some(b'*') => {
                    Self::append_comment_free_segment(&mut cleaned, source, segment_start, index);
                    Self::ensure_comment_gap(&mut cleaned);
                    index += 2;
                    let mut closed = false;
                    while index + 1 < bytes.len() {
                        if bytes[index] == b'*' && bytes[index + 1] == b'/' {
                            index += 2;
                            closed = true;
                            break;
                        }
                        index += 1;
                    }
                    if !closed {
                        index = bytes.len();
                    }
                    segment_start = Self::skip_ascii_whitespace(source, index);
                    continue;
                }
                _ => {}
            }

            index += 1;
        }

        Self::append_comment_free_segment(&mut cleaned, source, segment_start, source.len());
        cleaned
    }

    fn unquote_identifier(identifier: &str) -> String {
        let trimmed = identifier.trim();
        if let Some(inner) = trimmed
            .strip_prefix('`')
            .and_then(|value| value.strip_suffix('`'))
        {
            return inner.replace("``", "`");
        }
        trimmed.to_string()
    }

    fn parse_identifier_segment_end(source: &str, start: usize) -> Option<usize> {
        let bytes = source.as_bytes();
        match bytes.get(start).copied() {
            Some(b'`') => {
                let mut index = start + 1;
                while index < bytes.len() {
                    if bytes[index] == b'`' {
                        if bytes.get(index + 1) == Some(&b'`') {
                            index += 2;
                            continue;
                        }
                        return Some(index + 1);
                    }
                    index += 1;
                }
                None
            }
            Some(_) => {
                let mut index = start;
                while let Some(byte) = bytes.get(index).copied() {
                    if byte.is_ascii_whitespace() || matches!(byte, b'.' | b'(' | b')' | b',') {
                        break;
                    }
                    index += 1;
                }
                if index > start {
                    Some(index)
                } else {
                    None
                }
            }
            None => None,
        }
    }

    fn parse_identifier_path_end(source: &str, start: usize) -> Option<usize> {
        let mut index = Self::skip_ascii_whitespace(source, start);
        let segment_end = Self::parse_identifier_segment_end(source, index)?;
        index = segment_end;

        loop {
            let bytes = source.as_bytes();
            if bytes.get(index) != Some(&b'.') {
                break;
            }
            let next_segment_start = Self::skip_ascii_whitespace(source, index + 1);
            let next_segment_end = Self::parse_identifier_segment_end(source, next_segment_start)?;
            index = next_segment_end;
        }

        Some(index)
    }

    fn keyword_matches_at(source: &str, index: usize, keyword: &str) -> bool {
        let keyword_len = keyword.len();
        let Some(slice) = source.get(index..index.saturating_add(keyword_len)) else {
            return false;
        };
        if !slice.eq_ignore_ascii_case(keyword) {
            return false;
        }

        let bytes = source.as_bytes();
        let prev = index.checked_sub(1).and_then(|idx| bytes.get(idx)).copied();
        let next = bytes.get(index.saturating_add(keyword_len)).copied();
        let is_ident = |byte: u8| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$');

        !prev.is_some_and(is_ident) && !next.is_some_and(is_ident)
    }

    fn find_keyword_at_top_level(source: &str, keyword: &str, start: usize) -> Option<usize> {
        let bytes = source.as_bytes();
        let mut index = start.min(bytes.len());
        let mut depth = 0usize;
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_backtick = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false;

        while index < bytes.len() {
            let byte = bytes[index];
            let next = bytes.get(index + 1).copied();

            if in_line_comment {
                if byte == b'\n' {
                    in_line_comment = false;
                }
                index += 1;
                continue;
            }

            if in_block_comment {
                if byte == b'*' && next == Some(b'/') {
                    in_block_comment = false;
                    index += 2;
                } else {
                    index += 1;
                }
                continue;
            }

            if in_single_quote {
                if byte == b'\\' && next.is_some() {
                    index += 2;
                    continue;
                }
                if byte == b'\'' {
                    if next == Some(b'\'') {
                        index += 2;
                        continue;
                    }
                    in_single_quote = false;
                }
                index += 1;
                continue;
            }

            if in_double_quote {
                if byte == b'\\' && next.is_some() {
                    index += 2;
                    continue;
                }
                if byte == b'"' {
                    if next == Some(b'"') {
                        index += 2;
                        continue;
                    }
                    in_double_quote = false;
                }
                index += 1;
                continue;
            }

            if in_backtick {
                if byte == b'`' {
                    if next == Some(b'`') {
                        index += 2;
                        continue;
                    }
                    in_backtick = false;
                }
                index += 1;
                continue;
            }

            if sql_text::is_mysql_dash_comment_start(bytes, index) {
                in_line_comment = true;
                index += 2;
                continue;
            }

            if byte == b'#' {
                in_line_comment = true;
                index += 1;
                continue;
            }

            if byte == b'/' && next == Some(b'*') {
                in_block_comment = true;
                index += 2;
                continue;
            }

            match byte {
                b'\'' => {
                    in_single_quote = true;
                    index += 1;
                    continue;
                }
                b'"' => {
                    in_double_quote = true;
                    index += 1;
                    continue;
                }
                b'`' => {
                    in_backtick = true;
                    index += 1;
                    continue;
                }
                b'(' => {
                    depth += 1;
                }
                b')' => {
                    depth = depth.saturating_sub(1);
                }
                _ => {}
            }

            if depth == 0 && Self::keyword_matches_at(source, index, keyword) {
                return Some(index);
            }

            index += 1;
        }

        None
    }

    fn find_matching_paren(source: &str, open_index: usize) -> Option<usize> {
        let bytes = source.as_bytes();
        if bytes.get(open_index) != Some(&b'(') {
            return None;
        }

        let mut index = open_index;
        let mut depth = 0usize;
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_backtick = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false;

        while index < bytes.len() {
            let byte = bytes[index];
            let next = bytes.get(index + 1).copied();

            if in_line_comment {
                if byte == b'\n' {
                    in_line_comment = false;
                }
                index += 1;
                continue;
            }

            if in_block_comment {
                if byte == b'*' && next == Some(b'/') {
                    in_block_comment = false;
                    index += 2;
                } else {
                    index += 1;
                }
                continue;
            }

            if in_single_quote {
                if byte == b'\\' && next.is_some() {
                    index += 2;
                    continue;
                }
                if byte == b'\'' {
                    if next == Some(b'\'') {
                        index += 2;
                        continue;
                    }
                    in_single_quote = false;
                }
                index += 1;
                continue;
            }

            if in_double_quote {
                if byte == b'\\' && next.is_some() {
                    index += 2;
                    continue;
                }
                if byte == b'"' {
                    if next == Some(b'"') {
                        index += 2;
                        continue;
                    }
                    in_double_quote = false;
                }
                index += 1;
                continue;
            }

            if in_backtick {
                if byte == b'`' {
                    if next == Some(b'`') {
                        index += 2;
                        continue;
                    }
                    in_backtick = false;
                }
                index += 1;
                continue;
            }

            if sql_text::is_mysql_dash_comment_start(bytes, index) {
                in_line_comment = true;
                index += 2;
                continue;
            }

            if byte == b'#' {
                in_line_comment = true;
                index += 1;
                continue;
            }

            if byte == b'/' && next == Some(b'*') {
                in_block_comment = true;
                index += 2;
                continue;
            }

            match byte {
                b'\'' => in_single_quote = true,
                b'"' => in_double_quote = true,
                b'`' => in_backtick = true,
                b'(' => depth += 1,
                b')' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return Some(index);
                    }
                }
                _ => {}
            }

            index += 1;
        }

        None
    }

    fn split_top_level_comma_list(source: &str) -> Vec<String> {
        let bytes = source.as_bytes();
        let mut items = Vec::new();
        let mut start = 0usize;
        let mut index = 0usize;
        let mut depth = 0usize;
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_backtick = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false;

        while index < bytes.len() {
            let byte = bytes[index];
            let next = bytes.get(index + 1).copied();

            if in_line_comment {
                if byte == b'\n' {
                    in_line_comment = false;
                }
                index += 1;
                continue;
            }

            if in_block_comment {
                if byte == b'*' && next == Some(b'/') {
                    in_block_comment = false;
                    index += 2;
                } else {
                    index += 1;
                }
                continue;
            }

            if in_single_quote {
                if byte == b'\\' && next.is_some() {
                    index += 2;
                    continue;
                }
                if byte == b'\'' {
                    if next == Some(b'\'') {
                        index += 2;
                        continue;
                    }
                    in_single_quote = false;
                }
                index += 1;
                continue;
            }

            if in_double_quote {
                if byte == b'\\' && next.is_some() {
                    index += 2;
                    continue;
                }
                if byte == b'"' {
                    if next == Some(b'"') {
                        index += 2;
                        continue;
                    }
                    in_double_quote = false;
                }
                index += 1;
                continue;
            }

            if in_backtick {
                if byte == b'`' {
                    if next == Some(b'`') {
                        index += 2;
                        continue;
                    }
                    in_backtick = false;
                }
                index += 1;
                continue;
            }

            if sql_text::is_mysql_dash_comment_start(bytes, index) {
                in_line_comment = true;
                index += 2;
                continue;
            }

            if byte == b'#' {
                in_line_comment = true;
                index += 1;
                continue;
            }

            if byte == b'/' && next == Some(b'*') {
                in_block_comment = true;
                index += 2;
                continue;
            }

            match byte {
                b'\'' => in_single_quote = true,
                b'"' => in_double_quote = true,
                b'`' => in_backtick = true,
                b'(' => depth += 1,
                b')' => depth = depth.saturating_sub(1),
                b',' if depth == 0 => {
                    if let Some(item) = source.get(start..index) {
                        let trimmed = item.trim();
                        if !trimmed.is_empty() {
                            items.push(trimmed.to_string());
                        }
                    }
                    start = index + 1;
                }
                _ => {}
            }

            index += 1;
        }

        if let Some(item) = source.get(start..) {
            let trimmed = item.trim();
            if !trimmed.is_empty() {
                items.push(trimmed.to_string());
            }
        }

        items
    }

    fn parse_mysql_parameter(parameter: &str, position: i32) -> Option<ProcedureArgument> {
        let parameter = Self::strip_mysql_inline_comments(parameter);
        let parameter = parameter.trim();
        let mut index = Self::skip_ascii_whitespace(parameter, 0);

        let mut direction = "IN".to_string();

        for candidate in ["INOUT", "OUT", "IN"] {
            if Self::keyword_matches_at(parameter, index, candidate) {
                direction = candidate.to_string();
                index = Self::skip_ascii_whitespace(parameter, index + candidate.len());
                break;
            }
        }

        let name_end = Self::parse_identifier_segment_end(parameter, index)?;
        let name_raw = parameter.get(index..name_end)?;
        let name = Self::unquote_identifier(name_raw);

        let remainder = parameter.get(name_end..)?.trim();
        if remainder.is_empty() {
            return None;
        }

        let (data_type, default_value) =
            if let Some(default_idx) = Self::find_keyword_at_top_level(remainder, "DEFAULT", 0) {
                let type_part = remainder
                    .get(..default_idx)
                    .map(str::trim)
                    .unwrap_or_default()
                    .to_string();
                let default_part = remainder
                    .get(default_idx + "DEFAULT".len()..)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned);
                (type_part, default_part)
            } else {
                (remainder.to_string(), None)
            };

        if data_type.trim().is_empty() {
            return None;
        }

        Some(ProcedureArgument {
            name: Some(name),
            position,
            sequence: position,
            data_type: Some(data_type.trim().to_string()),
            in_out: Some(direction),
            data_length: None,
            data_precision: None,
            data_scale: None,
            type_owner: None,
            type_name: None,
            pls_type: None,
            overload: None,
            default_value,
        })
    }

    fn parse_function_return_type(ddl: &str, close_paren_index: usize) -> Option<String> {
        let returns_index = Self::find_keyword_at_top_level(ddl, "RETURNS", close_paren_index)?;
        let type_start = Self::skip_ascii_whitespace(ddl, returns_index + "RETURNS".len());
        let type_section = ddl.get(type_start..)?.trim();
        if type_section.is_empty() {
            return None;
        }

        let mut type_end = type_section.len();
        for keyword in [
            "DETERMINISTIC",
            "NOT",
            "CONTAINS",
            "NO",
            "READS",
            "MODIFIES",
            "SQL",
            "COMMENT",
            "BEGIN",
            "RETURN",
        ] {
            if let Some(position) = Self::find_keyword_at_top_level(type_section, keyword, 0) {
                type_end = type_end.min(position);
            }
        }

        type_section
            .get(..type_end)
            .map(Self::strip_mysql_inline_comments)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    }

    fn parse_routine_arguments_from_create_ddl(
        ddl: &str,
        routine_type: &str,
    ) -> Option<Vec<ProcedureArgument>> {
        let routine_type = routine_type.trim();
        let keyword_index = Self::find_keyword_at_top_level(ddl, routine_type, 0)?;
        let name_start = Self::skip_ascii_whitespace(ddl, keyword_index + routine_type.len());
        let name_end = Self::parse_identifier_path_end(ddl, name_start)?;
        let open_paren_index = Self::skip_ascii_whitespace(ddl, name_end);
        if ddl.as_bytes().get(open_paren_index) != Some(&b'(') {
            return None;
        }

        let close_paren_index = Self::find_matching_paren(ddl, open_paren_index)?;
        let params_source = ddl.get(open_paren_index + 1..close_paren_index)?;
        let mut arguments = Vec::new();

        for (index, parameter) in Self::split_top_level_comma_list(params_source)
            .into_iter()
            .enumerate()
        {
            let position = i32::try_from(index + 1).ok().unwrap_or(i32::MAX);
            if let Some(argument) = Self::parse_mysql_parameter(&parameter, position) {
                arguments.push(argument);
            }
        }

        if routine_type.eq_ignore_ascii_case("FUNCTION") {
            if let Some(return_type) = Self::parse_function_return_type(ddl, close_paren_index) {
                arguments.insert(
                    0,
                    ProcedureArgument {
                        name: None,
                        position: 0,
                        sequence: 0,
                        data_type: Some(return_type),
                        in_out: Some("RETURN".to_string()),
                        data_length: None,
                        data_precision: None,
                        data_scale: None,
                        type_owner: None,
                        type_name: None,
                        pls_type: None,
                        overload: None,
                        default_value: None,
                    },
                );
            }
        }

        Some(arguments)
    }

    fn fallback_routine_arguments_from_ddl(
        conn: &mut Conn,
        schema_name: Option<&str>,
        routine_name: &str,
    ) -> Option<Vec<ProcedureArgument>> {
        let schema_name = Self::optional_schema_param(schema_name);
        let routine_type: Option<String> = conn
            .exec_first(
                "SELECT ROUTINE_TYPE \
                 FROM INFORMATION_SCHEMA.ROUTINES \
                 WHERE ROUTINE_SCHEMA = COALESCE(?, DATABASE()) AND ROUTINE_NAME = ? \
                 LIMIT 1",
                (schema_name.clone(), routine_name),
            )
            .ok()
            .flatten();
        let routine_type = routine_type?;
        let ddl = Self::get_create_object_in_schema(
            conn,
            schema_name.as_deref(),
            &routine_type,
            routine_name,
        )
        .ok()?;
        if ddl.trim().is_empty() {
            return None;
        }
        Self::parse_routine_arguments_from_create_ddl(&ddl, &routine_type)
    }

    pub fn get_tables(conn: &mut Conn) -> Result<Vec<String>, MysqlError> {
        let rows: Vec<String> = conn.query(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE' \
             ORDER BY TABLE_NAME",
        )?;
        Ok(rows)
    }

    pub fn get_views(conn: &mut Conn) -> Result<Vec<String>, MysqlError> {
        let rows: Vec<String> = conn.query(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS \
             WHERE TABLE_SCHEMA = DATABASE() \
             ORDER BY TABLE_NAME",
        )?;
        Ok(rows)
    }

    pub fn get_procedures(conn: &mut Conn) -> Result<Vec<String>, MysqlError> {
        let rows: Vec<String> = conn.query(
            "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES \
             WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_TYPE = 'PROCEDURE' \
             ORDER BY ROUTINE_NAME",
        )?;
        Ok(rows)
    }

    pub fn get_functions(conn: &mut Conn) -> Result<Vec<String>, MysqlError> {
        let rows: Vec<String> = conn.query(
            "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES \
             WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_TYPE = 'FUNCTION' \
             ORDER BY ROUTINE_NAME",
        )?;
        Ok(rows)
    }

    pub fn get_schemas(conn: &mut Conn) -> Result<Vec<String>, MysqlError> {
        let rows: Vec<String> = conn.query(
            "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA \
             WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') \
             ORDER BY SCHEMA_NAME",
        )?;
        Ok(rows)
    }

    pub fn get_schema_objects_by_schema(
        conn: &mut Conn,
    ) -> Result<HashMap<String, Vec<(String, String)>>, MysqlError> {
        let rows: Vec<(String, String, String)> = conn.query(
            "SELECT TABLE_SCHEMA, TABLE_NAME, \
                    CASE TABLE_TYPE \
                        WHEN 'BASE TABLE' THEN 'TABLE' \
                        ELSE TABLE_TYPE \
                    END AS OBJECT_TYPE \
             FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') \
               AND TABLE_TYPE IN ('BASE TABLE', 'VIEW', 'SEQUENCE') \
             UNION ALL \
             SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE \
             FROM INFORMATION_SCHEMA.ROUTINES \
             WHERE ROUTINE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') \
             ORDER BY 1, 2, 3",
        )?;

        let mut grouped = HashMap::new();
        for (schema, name, object_type) in rows {
            grouped
                .entry(schema)
                .or_insert_with(Vec::new)
                .push((name, object_type));
        }
        Ok(grouped)
    }

    pub fn get_schema_relation_members_by_schema(
        conn: &mut Conn,
    ) -> Result<HashMap<String, Vec<String>>, MysqlError> {
        let rows: Vec<(String, String)> = conn.query(
            "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') \
               AND TABLE_TYPE IN ('BASE TABLE', 'VIEW') \
             ORDER BY 1, 2",
        )?;

        let mut grouped = HashMap::new();
        for (schema, name) in rows {
            grouped.entry(schema).or_insert_with(Vec::new).push(name);
        }
        Ok(grouped)
    }

    pub fn get_triggers(conn: &mut Conn) -> Result<Vec<String>, MysqlError> {
        let rows: Vec<String> = conn.query(
            "SELECT TRIGGER_NAME FROM INFORMATION_SCHEMA.TRIGGERS \
             WHERE TRIGGER_SCHEMA = DATABASE() \
             ORDER BY TRIGGER_NAME",
        )?;
        Ok(rows)
    }

    pub fn get_sequences(conn: &mut Conn) -> Result<Vec<String>, MysqlError> {
        let rows: Vec<String> = conn.query(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'SEQUENCE' \
             ORDER BY TABLE_NAME",
        )?;
        Ok(rows)
    }

    pub fn get_events(conn: &mut Conn) -> Result<Vec<String>, MysqlError> {
        let rows: Vec<String> = conn.query(
            "SELECT EVENT_NAME FROM INFORMATION_SCHEMA.EVENTS \
             WHERE EVENT_SCHEMA = DATABASE() \
             ORDER BY EVENT_NAME",
        )?;
        Ok(rows)
    }

    pub fn get_indexes(conn: &mut Conn, table_name: &str) -> Result<Vec<String>, MysqlError> {
        let rows: Vec<String> = conn.exec(
            "SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? \
             ORDER BY INDEX_NAME",
            (table_name,),
        )?;
        Ok(rows)
    }

    pub fn get_index_details(
        conn: &mut Conn,
        table_name: &str,
    ) -> Result<Vec<IndexInfo>, MysqlError> {
        let rows: Vec<(String, u8, Option<String>)> = conn.exec(
            "SELECT INDEX_NAME, NON_UNIQUE, \
             GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ', ') \
             FROM INFORMATION_SCHEMA.STATISTICS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? \
             GROUP BY INDEX_NAME, NON_UNIQUE \
             ORDER BY INDEX_NAME",
            (table_name,),
        )?;

        Ok(rows
            .into_iter()
            .map(|(name, non_unique, columns)| IndexInfo {
                name,
                is_unique: non_unique == 0,
                columns: columns.unwrap_or_default(),
            })
            .collect())
    }

    pub fn describe_object(
        conn: &mut Conn,
        table_name: &str,
    ) -> Result<Vec<MysqlTableColumnDetail>, MysqlError> {
        let rows: Vec<(String, String, String, Option<String>, String, String)> = conn.exec(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY, EXTRA \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? \
             ORDER BY ORDINAL_POSITION",
            (table_name,),
        )?;

        Ok(rows
            .into_iter()
            .map(
                |(name, data_type, nullable, default_value, column_key, extra)| {
                    MysqlTableColumnDetail {
                        name,
                        data_type,
                        is_nullable: nullable == "YES",
                        default_value,
                        is_primary_key: column_key == "PRI",
                        extra,
                    }
                },
            )
            .collect())
    }

    pub fn get_table_structure(
        conn: &mut Conn,
        table_name: &str,
    ) -> Result<Vec<TableColumnDetail>, MysqlError> {
        Self::get_table_structure_in_schema(conn, None, table_name)
    }

    pub fn get_table_structure_in_schema(
        conn: &mut Conn,
        schema_name: Option<&str>,
        table_name: &str,
    ) -> Result<Vec<TableColumnDetail>, MysqlError> {
        let schema_name = Self::optional_schema_param(schema_name);
        let rows: Vec<(
            String,
            String,
            Option<u64>,
            Option<u64>,
            Option<u64>,
            String,
            Option<String>,
            String,
        )> = conn.exec(
            "SELECT COLUMN_NAME, COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, \
             NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = COALESCE(?, DATABASE()) AND TABLE_NAME = ? \
             ORDER BY ORDINAL_POSITION",
            (schema_name, table_name),
        )?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    name,
                    data_type,
                    data_length,
                    data_precision,
                    data_scale,
                    nullable,
                    default_value,
                    column_key,
                )| TableColumnDetail {
                    name,
                    data_type,
                    data_length: data_length
                        .and_then(|value| i32::try_from(value).ok())
                        .unwrap_or(0),
                    data_precision: data_precision.and_then(|value| i32::try_from(value).ok()),
                    data_scale: data_scale.and_then(|value| i32::try_from(value).ok()),
                    nullable: nullable == "YES",
                    default_value,
                    is_primary_key: column_key == "PRI",
                },
            )
            .collect())
    }

    pub fn get_table_columns(
        conn: &mut Conn,
        table_name: &str,
    ) -> Result<Vec<ColumnInfo>, MysqlError> {
        Self::get_table_columns_in_schema(conn, None, table_name)
    }

    pub fn get_table_columns_in_schema(
        conn: &mut Conn,
        schema_name: Option<&str>,
        table_name: &str,
    ) -> Result<Vec<ColumnInfo>, MysqlError> {
        let schema_name = Self::optional_schema_param(schema_name);
        let rows: Vec<(String, String)> = conn.exec(
            "SELECT COLUMN_NAME, COLUMN_TYPE \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = COALESCE(?, DATABASE()) AND TABLE_NAME = ? \
             ORDER BY ORDINAL_POSITION",
            (schema_name, table_name),
        )?;

        Ok(rows
            .into_iter()
            .map(|(name, data_type)| ColumnInfo { name, data_type })
            .collect())
    }

    pub fn get_databases(conn: &mut Conn) -> Result<Vec<String>, MysqlError> {
        let rows: Vec<String> =
            conn.query("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME")?;
        Ok(rows)
    }

    pub fn get_create_table(conn: &mut Conn, table_name: &str) -> Result<String, MysqlError> {
        let result: Option<(String, String)> = conn.exec_first(
            format!("SHOW CREATE TABLE {}", Self::quoted_identifier(table_name)),
            (),
        )?;
        match result {
            Some((_, ddl)) => Ok(ddl),
            None => Ok(String::new()),
        }
    }

    pub fn get_table_constraints(
        conn: &mut Conn,
        table_name: &str,
    ) -> Result<Vec<ConstraintInfo>, MysqlError> {
        let rows: Vec<(String, String, Option<String>, Option<String>)> = conn.exec(
            "SELECT tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE, \
             GROUP_CONCAT(kcu.COLUMN_NAME ORDER BY kcu.ORDINAL_POSITION SEPARATOR ', ') AS columns, \
             rc.REFERENCED_TABLE_NAME \
             FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc \
             LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu \
               ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA \
              AND tc.TABLE_NAME = kcu.TABLE_NAME \
              AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME \
             LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc \
               ON tc.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA \
              AND tc.TABLE_NAME = rc.TABLE_NAME \
              AND tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME \
             WHERE tc.TABLE_SCHEMA = DATABASE() AND tc.TABLE_NAME = ? \
             GROUP BY tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE, rc.REFERENCED_TABLE_NAME \
             ORDER BY tc.CONSTRAINT_TYPE, tc.CONSTRAINT_NAME",
            (table_name,),
        )?;

        Ok(rows
            .into_iter()
            .map(
                |(name, constraint_type, columns, ref_table)| ConstraintInfo {
                    name,
                    constraint_type,
                    columns: columns.unwrap_or_default(),
                    ref_table,
                },
            )
            .collect())
    }

    pub fn get_create_object(
        conn: &mut Conn,
        object_type: &str,
        object_name: &str,
    ) -> Result<String, MysqlError> {
        Self::get_create_object_in_schema(conn, None, object_type, object_name)
    }

    pub fn get_create_object_in_schema(
        conn: &mut Conn,
        schema_name: Option<&str>,
        object_type: &str,
        object_name: &str,
    ) -> Result<String, MysqlError> {
        let object_type_upper = object_type.to_ascii_uppercase();
        if Self::create_ddl_column_names(&object_type_upper).is_empty() {
            return Ok(String::new());
        }

        let qualified_name = if let Some(schema) = Self::optional_schema_param(schema_name) {
            format!(
                "{}.{}",
                Self::quoted_identifier(&schema),
                Self::quoted_identifier(object_name)
            )
        } else {
            Self::quoted_identifier(object_name)
        };

        let sql = format!("SHOW CREATE {} {}", object_type_upper, qualified_name);
        let mut result = conn.query_iter(sql)?;
        let ddl_column_index = result.columns().as_ref().iter().position(|column| {
            let column_name = column.name_str();
            Self::create_ddl_column_names(&object_type_upper)
                .iter()
                .any(|candidate| column_name.eq_ignore_ascii_case(candidate))
        });
        let Some(row_result) = result.next() else {
            return Ok(String::new());
        };
        let row = row_result?;
        let ddl = ddl_column_index
            .and_then(|index| row.as_ref(index))
            .map(MysqlExecutor::value_to_string)
            .unwrap_or_default();
        Ok(ddl)
    }

    pub fn get_object_types(conn: &mut Conn, object_name: &str) -> Result<Vec<String>, MysqlError> {
        Self::get_object_types_in_schema(conn, None, object_name)
    }

    pub fn get_object_types_in_schema(
        conn: &mut Conn,
        schema_name: Option<&str>,
        object_name: &str,
    ) -> Result<Vec<String>, MysqlError> {
        let schema_name = Self::optional_schema_param(schema_name);

        let mut object_types: Vec<String> = conn.exec(
            "SELECT CASE WHEN TABLE_TYPE = 'VIEW' THEN 'VIEW' ELSE 'TABLE' END \
             FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = COALESCE(?, DATABASE()) AND TABLE_NAME = ?",
            (schema_name.clone(), object_name),
        )?;

        let mut routine_types: Vec<String> = conn.exec(
            "SELECT ROUTINE_TYPE \
             FROM INFORMATION_SCHEMA.ROUTINES \
             WHERE ROUTINE_SCHEMA = COALESCE(?, DATABASE()) AND ROUTINE_NAME = ?",
            (schema_name.clone(), object_name),
        )?;
        object_types.append(&mut routine_types);

        let mut trigger_types: Vec<String> = conn.exec(
            "SELECT 'TRIGGER' \
             FROM INFORMATION_SCHEMA.TRIGGERS \
             WHERE TRIGGER_SCHEMA = COALESCE(?, DATABASE()) AND TRIGGER_NAME = ?",
            (schema_name.clone(), object_name),
        )?;
        object_types.append(&mut trigger_types);

        let mut sequence_types: Vec<String> = conn.exec(
            "SELECT 'SEQUENCE' \
             FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = COALESCE(?, DATABASE()) AND TABLE_NAME = ? \
               AND TABLE_TYPE = 'SEQUENCE'",
            (schema_name.clone(), object_name),
        )?;
        object_types.append(&mut sequence_types);

        let mut event_types: Vec<String> = conn.exec(
            "SELECT 'EVENT' \
             FROM INFORMATION_SCHEMA.EVENTS \
             WHERE EVENT_SCHEMA = COALESCE(?, DATABASE()) AND EVENT_NAME = ?",
            (schema_name, object_name),
        )?;
        object_types.append(&mut event_types);

        object_types.sort();
        object_types.dedup();
        Ok(object_types)
    }

    pub fn get_routine_arguments(
        conn: &mut Conn,
        routine_name: &str,
    ) -> Result<Vec<ProcedureArgument>, MysqlError> {
        Self::get_routine_arguments_in_schema(conn, None, routine_name)
    }

    pub fn get_routine_arguments_in_schema(
        conn: &mut Conn,
        schema_name: Option<&str>,
        routine_name: &str,
    ) -> Result<Vec<ProcedureArgument>, MysqlError> {
        let schema_name = Self::optional_schema_param(schema_name);
        let query_schema_name = schema_name.clone();
        let rows_result: Result<
            Vec<(
                Option<String>,
                u64,
                Option<String>,
                Option<String>,
                Option<u64>,
                Option<u64>,
                Option<u64>,
            )>,
            MysqlError,
        > = conn.exec(
            "SELECT PARAMETER_NAME, ORDINAL_POSITION, PARAMETER_MODE, DTD_IDENTIFIER, \
             CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE \
             FROM INFORMATION_SCHEMA.PARAMETERS \
             WHERE SPECIFIC_SCHEMA = COALESCE(?, DATABASE()) AND SPECIFIC_NAME = ? \
             ORDER BY ORDINAL_POSITION",
            (query_schema_name, routine_name),
        );

        let fallback_arguments = |conn: &mut Conn| {
            Self::fallback_routine_arguments_from_ddl(conn, schema_name.as_deref(), routine_name)
        };

        let rows = match rows_result {
            Ok(rows) if !rows.is_empty() => rows,
            Ok(_) => {
                return Ok(fallback_arguments(conn).unwrap_or_default());
            }
            Err(err) => {
                if let Some(arguments) = fallback_arguments(conn) {
                    return Ok(arguments);
                }
                return Err(err);
            }
        };

        Ok(rows
            .into_iter()
            .map(
                |(
                    name,
                    position,
                    parameter_mode,
                    data_type,
                    data_length,
                    data_precision,
                    data_scale,
                )| {
                    let in_out = if position == 0 && name.is_none() {
                        Some("RETURN".to_string())
                    } else {
                        parameter_mode
                    };

                    ProcedureArgument {
                        name,
                        position: i32::try_from(position).ok().unwrap_or(i32::MAX),
                        sequence: i32::try_from(position).ok().unwrap_or(i32::MAX),
                        data_type,
                        in_out,
                        data_length: data_length.and_then(|value| i32::try_from(value).ok()),
                        data_precision: data_precision.and_then(|value| i32::try_from(value).ok()),
                        data_scale: data_scale.and_then(|value| i32::try_from(value).ok()),
                        type_owner: None,
                        type_name: None,
                        pls_type: None,
                        overload: None,
                        default_value: None,
                    }
                },
            )
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::{MysqlExecutor, MysqlObjectBrowser, MysqlResultSetSnapshot};
    use crate::db::query::types::ColumnInfo;
    use mysql::{Error as MysqlError, MySqlError, Value as MysqlValue};
    use std::time::Duration;

    #[test]
    fn mysql_value_to_string_formats_common_non_text_types() {
        assert_eq!(MysqlExecutor::value_to_string(&MysqlValue::Int(-7)), "-7");
        assert_eq!(MysqlExecutor::value_to_string(&MysqlValue::UInt(42)), "42");
        assert_eq!(
            MysqlExecutor::value_to_string(&MysqlValue::Date(2026, 4, 5, 13, 14, 15, 123_456)),
            "2026-04-05 13:14:15.123456"
        );
        assert_eq!(
            MysqlExecutor::value_to_string(&MysqlValue::Time(true, 1, 2, 3, 4, 0)),
            "-26:03:04"
        );
    }

    #[test]
    fn mysql_escape_identifier_doubles_backticks() {
        assert_eq!(
            super::MysqlObjectBrowser::quoted_identifier("odd`name"),
            "`odd``name`"
        );
    }

    #[test]
    fn mysql_format_explain_lines_renders_table_output() {
        let columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: "BIGINT".to_string(),
            },
            ColumnInfo {
                name: "table".to_string(),
                data_type: "VARCHAR".to_string(),
            },
            ColumnInfo {
                name: "Extra".to_string(),
                data_type: "VARCHAR".to_string(),
            },
        ];
        let rows = vec![vec![
            "1".to_string(),
            "employees".to_string(),
            "Using where".to_string(),
        ]];

        let lines = MysqlExecutor::format_explain_lines(columns.as_slice(), rows.as_slice());

        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("id"));
        assert!(lines[0].contains("table"));
        assert!(lines[0].contains("Extra"));
        assert!(lines[1].contains("-+-"));
        assert!(lines[2].contains("employees"));
        assert!(lines[2].contains("Using where"));
    }

    #[test]
    fn mysql_classify_statement_treats_values_and_table_as_selects() {
        assert_eq!(
            MysqlExecutor::classify_statement("VALUES ROW(1, 'A')"),
            super::MysqlStatementKind::Select
        );
        assert_eq!(
            MysqlExecutor::classify_statement("TABLE employees"),
            super::MysqlStatementKind::Select
        );
    }

    #[test]
    fn mysql_classify_statement_treats_with_dml_as_non_select() {
        assert_eq!(
            MysqlExecutor::classify_statement(
                "WITH recent AS (SELECT 1 AS id) INSERT INTO audit_log(id) SELECT id FROM recent"
            ),
            super::MysqlStatementKind::Dml
        );
        assert_eq!(
            MysqlExecutor::classify_statement(
                "WITH recent AS (SELECT 1 AS id) UPDATE audit_log SET id = (SELECT id FROM recent)"
            ),
            super::MysqlStatementKind::Dml
        );
    }

    #[test]
    fn mysql_timeout_statement_uses_millisecond_session_setting() {
        assert_eq!(
            MysqlExecutor::mysql_timeout_statement(Some(Duration::from_secs(5))),
            "SET SESSION MAX_EXECUTION_TIME = 5000"
        );
        assert_eq!(
            MysqlExecutor::mysql_timeout_statement(None),
            "SET SESSION MAX_EXECUTION_TIME = 0"
        );
    }

    #[test]
    fn mysql_error_detection_distinguishes_timeout_from_cancel() {
        let timeout_err = MysqlError::MySqlError(MySqlError {
            state: "HY000".to_string(),
            code: 3024,
            message: "Query execution was interrupted, maximum statement execution time exceeded"
                .to_string(),
        });
        assert!(MysqlExecutor::is_timeout_error(&timeout_err));
        assert!(!MysqlExecutor::is_cancel_error(&timeout_err));

        let cancel_err = MysqlError::MySqlError(MySqlError {
            state: "70100".to_string(),
            code: 1317,
            message: "Query execution was interrupted".to_string(),
        });
        assert!(MysqlExecutor::is_cancel_error(&cancel_err));
        assert!(!MysqlExecutor::is_timeout_error(&cancel_err));
    }

    #[test]
    fn mysql_materialize_call_results_preserves_dml_and_select_sets() {
        let results = MysqlExecutor::materialize_call_results(
            "CALL sync_and_list_users()",
            vec![
                MysqlResultSetSnapshot {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    affected_rows: 2,
                    info: String::new(),
                },
                MysqlResultSetSnapshot {
                    columns: vec![ColumnInfo {
                        name: "user_name".to_string(),
                        data_type: "VARCHAR".to_string(),
                    }],
                    rows: vec![vec!["alice".to_string()], vec!["bob".to_string()]],
                    affected_rows: 0,
                    info: String::new(),
                },
                MysqlResultSetSnapshot {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    affected_rows: 0,
                    info: String::new(),
                },
            ],
            Duration::from_millis(5),
        );

        assert_eq!(results.len(), 2);
        assert!(!results[0].is_select);
        assert_eq!(results[0].message, "CALL 2 row(s) affected");
        assert!(results[1].is_select);
        assert_eq!(results[1].row_count, 2);
        assert_eq!(results[1].columns[0].name, "user_name");
    }

    #[test]
    fn mysql_materialize_call_results_falls_back_to_call_executed_for_empty_ok_packets() {
        let results = MysqlExecutor::materialize_call_results(
            "CALL noop()",
            vec![MysqlResultSetSnapshot {
                columns: Vec::new(),
                rows: Vec::new(),
                affected_rows: 0,
                info: String::new(),
            }],
            Duration::from_millis(1),
        );

        assert_eq!(results.len(), 1);
        assert!(!results[0].is_select);
        assert_eq!(results[0].message, "CALL executed.");
    }

    #[test]
    fn mysql_parse_routine_arguments_from_create_ddl_handles_procedure_signature() {
        let ddl = "CREATE DEFINER=`root`@`localhost` PROCEDURE `demo_proc`(IN p_id INT, INOUT `p_name` VARCHAR(50) DEFAULT 'guest', OUT p_total DECIMAL(10,2))\nBEGIN\n  SELECT 1;\nEND";

        let arguments =
            MysqlObjectBrowser::parse_routine_arguments_from_create_ddl(ddl, "PROCEDURE")
                .expect("procedure signature should parse");

        assert_eq!(arguments.len(), 3);
        assert_eq!(arguments[0].name.as_deref(), Some("p_id"));
        assert_eq!(arguments[0].in_out.as_deref(), Some("IN"));
        assert_eq!(arguments[0].data_type.as_deref(), Some("INT"));

        assert_eq!(arguments[1].name.as_deref(), Some("p_name"));
        assert_eq!(arguments[1].in_out.as_deref(), Some("INOUT"));
        assert_eq!(arguments[1].data_type.as_deref(), Some("VARCHAR(50)"));
        assert_eq!(arguments[1].default_value.as_deref(), Some("'guest'"));

        assert_eq!(arguments[2].name.as_deref(), Some("p_total"));
        assert_eq!(arguments[2].in_out.as_deref(), Some("OUT"));
        assert_eq!(arguments[2].data_type.as_deref(), Some("DECIMAL(10,2)"));
    }

    #[test]
    fn mysql_parse_routine_arguments_from_create_ddl_handles_function_return_type() {
        let ddl = "CREATE DEFINER=`root`@`localhost` FUNCTION `demo_func`(p_id INT, p_kind ENUM('A','B')) RETURNS VARCHAR(20) CHARACTER SET utf8mb4 DETERMINISTIC\nRETURN 'ok'";

        let arguments =
            MysqlObjectBrowser::parse_routine_arguments_from_create_ddl(ddl, "FUNCTION")
                .expect("function signature should parse");

        assert_eq!(arguments.len(), 3);
        assert_eq!(arguments[0].position, 0);
        assert_eq!(arguments[0].in_out.as_deref(), Some("RETURN"));
        assert_eq!(
            arguments[0].data_type.as_deref(),
            Some("VARCHAR(20) CHARACTER SET utf8mb4")
        );
        assert_eq!(arguments[1].name.as_deref(), Some("p_id"));
        assert_eq!(arguments[1].data_type.as_deref(), Some("INT"));
        assert_eq!(arguments[2].name.as_deref(), Some("p_kind"));
        assert_eq!(arguments[2].data_type.as_deref(), Some("ENUM('A','B')"));
    }

    // -----------------------------------------------------------------------
    // # comment handling in DDL parser helpers
    // -----------------------------------------------------------------------

    #[test]
    fn mysql_parse_routine_arguments_ignores_comma_inside_hash_comment() {
        // The hash comment on the first parameter contains a comma; it must not
        // be treated as a parameter separator.
        let ddl = "CREATE PROCEDURE `annotated_proc`(\
            p_id INT,    # first param, the user id\n\
            p_name VARCHAR(50)\
        )\nBEGIN SELECT 1; END";

        let arguments =
            MysqlObjectBrowser::parse_routine_arguments_from_create_ddl(ddl, "PROCEDURE")
                .expect("procedure with hash comments should parse");

        assert_eq!(
            arguments.len(),
            2,
            "comma inside # comment must not create a phantom parameter: {arguments:?}"
        );
        assert_eq!(arguments[0].name.as_deref(), Some("p_id"));
        assert_eq!(arguments[0].data_type.as_deref(), Some("INT"));
        assert_eq!(arguments[1].name.as_deref(), Some("p_name"));
        assert_eq!(arguments[1].data_type.as_deref(), Some("VARCHAR(50)"));
    }

    #[test]
    fn mysql_parse_routine_arguments_ignores_default_keyword_inside_hash_comment() {
        // DEFAULT inside a hash comment must not be mistaken for the parameter
        // default-value marker.
        let ddl = "CREATE PROCEDURE `commented_proc`(\
            p_status VARCHAR(20) # DEFAULT 'active' -- legacy default\n\
        )\nBEGIN SELECT 1; END";

        let arguments =
            MysqlObjectBrowser::parse_routine_arguments_from_create_ddl(ddl, "PROCEDURE")
                .expect("procedure with DEFAULT in hash comment should parse");

        assert_eq!(arguments.len(), 1);
        assert_eq!(arguments[0].name.as_deref(), Some("p_status"));
        assert_eq!(arguments[0].data_type.as_deref(), Some("VARCHAR(20)"));
        assert!(
            arguments[0].default_value.is_none(),
            "DEFAULT inside # comment must not be parsed as actual default value"
        );
    }

    #[test]
    fn mysql_parse_routine_arguments_hash_comment_with_paren_does_not_confuse_matching() {
        // A hash comment that contains parentheses must not disturb the paren-
        // matching logic used to locate the parameter list.
        let ddl = "CREATE FUNCTION `fn_hash_paren`(\
            p_val INT # range: (0, 100)\n\
        ) RETURNS INT DETERMINISTIC\nRETURN p_val * 2";

        let arguments =
            MysqlObjectBrowser::parse_routine_arguments_from_create_ddl(ddl, "FUNCTION")
                .expect("function with paren inside hash comment should parse");

        // First argument is the synthetic RETURN entry; second is p_val.
        assert!(
            arguments.iter().any(|a| a.name.as_deref() == Some("p_val")),
            "p_val parameter should be present: {arguments:?}"
        );
        assert!(
            arguments
                .iter()
                .any(|a| a.in_out.as_deref() == Some("RETURN")),
            "RETURN entry should be present: {arguments:?}"
        );
    }

    // -----------------------------------------------------------------------
    // USE statement db_name extraction
    // -----------------------------------------------------------------------

    #[test]
    fn mysql_extract_use_database_name_simple() {
        assert_eq!(MysqlExecutor::extract_use_database_name("USE mydb"), "mydb");
        assert_eq!(
            MysqlExecutor::extract_use_database_name("USE mydb;"),
            "mydb"
        );
    }

    #[test]
    fn mysql_extract_use_database_name_backtick_quoted() {
        assert_eq!(
            MysqlExecutor::extract_use_database_name("USE `mydb`"),
            "mydb"
        );
        assert_eq!(
            MysqlExecutor::extract_use_database_name("USE `mydb`;"),
            "mydb"
        );
    }

    #[test]
    fn mysql_extract_use_database_name_backtick_with_spaces() {
        assert_eq!(
            MysqlExecutor::extract_use_database_name("USE `my database`"),
            "my database",
            "backtick-quoted name with spaces should be fully extracted"
        );
    }

    #[test]
    fn mysql_extract_use_database_name_backtick_with_escaped_backtick() {
        assert_eq!(
            MysqlExecutor::extract_use_database_name("USE `odd``name`"),
            "odd`name",
            "escaped backtick inside quoted name should be unescaped"
        );
    }

    #[test]
    fn mysql_extract_use_database_name_skips_leading_block_comments() {
        assert_eq!(
            MysqlExecutor::extract_use_database_name("USE /* switch */ mydb"),
            "mydb",
            "block comment between USE and db name should be ignored"
        );
        assert_eq!(
            MysqlExecutor::extract_use_database_name("USE /* first */ /* second */ `my database`;"),
            "my database",
            "multiple block comments before a quoted db name should be ignored"
        );
    }

    #[test]
    fn mysql_extract_use_database_name_skips_block_comments_before_use_keyword() {
        assert_eq!(
            MysqlExecutor::extract_use_database_name("/* preface */ USE mydb"),
            "mydb",
            "leading block comment before USE should be ignored"
        );
        assert_eq!(
            MysqlExecutor::extract_use_database_name(
                "  /* first */ /* second */ USE `my database`;"
            ),
            "my database",
            "multiple leading block comments before USE should be ignored"
        );
    }

    // -----------------------------------------------------------------------
    // classify_statement additional coverage
    // -----------------------------------------------------------------------

    #[test]
    fn mysql_classify_statement_with_cte_select_is_select() {
        assert_eq!(
            MysqlExecutor::classify_statement(
                "WITH recent AS (SELECT 1 AS id) SELECT id FROM recent"
            ),
            super::MysqlStatementKind::Select,
            "WITH ... SELECT (CTE) should be classified as Select, not Dml"
        );
    }

    #[test]
    fn mysql_classify_statement_replace_into_is_dml() {
        assert_eq!(
            MysqlExecutor::classify_statement("REPLACE INTO t(id, v) VALUES (1, 'x')"),
            super::MysqlStatementKind::Dml
        );
    }

    // -----------------------------------------------------------------------
    // Bug fix: SHOW / DESCRIBE / EXPLAIN must be routed as Select so that
    // their tabular result sets are not silently discarded by query_drop().
    // -----------------------------------------------------------------------

    #[test]
    fn mysql_classify_statement_show_databases_is_select() {
        assert_eq!(
            MysqlExecutor::classify_statement("SHOW DATABASES"),
            super::MysqlStatementKind::Select,
            "SHOW DATABASES must be Select so its result set is not discarded"
        );
    }

    #[test]
    fn mysql_classify_statement_show_tables_is_select() {
        assert_eq!(
            MysqlExecutor::classify_statement("SHOW TABLES"),
            super::MysqlStatementKind::Select
        );
        // Variants with qualifiers must also be Select.
        assert_eq!(
            MysqlExecutor::classify_statement("SHOW TABLES FROM mydb"),
            super::MysqlStatementKind::Select
        );
        assert_eq!(
            MysqlExecutor::classify_statement("SHOW FULL TABLES"),
            super::MysqlStatementKind::Select
        );
    }

    #[test]
    fn mysql_classify_statement_show_variables_is_select() {
        assert_eq!(
            MysqlExecutor::classify_statement("SHOW VARIABLES"),
            super::MysqlStatementKind::Select
        );
        assert_eq!(
            MysqlExecutor::classify_statement("SHOW VARIABLES LIKE 'sql_mode'"),
            super::MysqlStatementKind::Select
        );
    }

    #[test]
    fn mysql_classify_statement_show_status_is_select() {
        assert_eq!(
            MysqlExecutor::classify_statement("SHOW STATUS"),
            super::MysqlStatementKind::Select
        );
    }

    #[test]
    fn mysql_classify_statement_show_processlist_is_select() {
        assert_eq!(
            MysqlExecutor::classify_statement("SHOW PROCESSLIST"),
            super::MysqlStatementKind::Select
        );
        assert_eq!(
            MysqlExecutor::classify_statement("SHOW FULL PROCESSLIST"),
            super::MysqlStatementKind::Select
        );
    }

    #[test]
    fn mysql_classify_statement_show_warnings_and_errors_is_select() {
        assert_eq!(
            MysqlExecutor::classify_statement("SHOW WARNINGS"),
            super::MysqlStatementKind::Select
        );
        assert_eq!(
            MysqlExecutor::classify_statement("SHOW ERRORS"),
            super::MysqlStatementKind::Select
        );
    }

    #[test]
    fn mysql_classify_statement_show_create_is_select() {
        assert_eq!(
            MysqlExecutor::classify_statement("SHOW CREATE TABLE orders"),
            super::MysqlStatementKind::Select
        );
        assert_eq!(
            MysqlExecutor::classify_statement("SHOW CREATE PROCEDURE p_sync"),
            super::MysqlStatementKind::Select
        );
        assert_eq!(
            MysqlExecutor::classify_statement("SHOW CREATE VIEW v_active"),
            super::MysqlStatementKind::Select
        );
    }

    #[test]
    fn mysql_classify_statement_describe_is_select() {
        assert_eq!(
            MysqlExecutor::classify_statement("DESCRIBE employees"),
            super::MysqlStatementKind::Select,
            "DESCRIBE must be Select so its result set is not discarded"
        );
        assert_eq!(
            MysqlExecutor::classify_statement("DESC employees"),
            super::MysqlStatementKind::Select,
            "DESC must be Select so its result set is not discarded"
        );
    }

    #[test]
    fn mysql_classify_statement_explain_is_select() {
        assert_eq!(
            MysqlExecutor::classify_statement("EXPLAIN SELECT * FROM employees"),
            super::MysqlStatementKind::Select,
            "EXPLAIN must be Select so its result set is not discarded"
        );
        assert_eq!(
            MysqlExecutor::classify_statement("EXPLAIN UPDATE employees SET salary = 0"),
            super::MysqlStatementKind::Select,
            "EXPLAIN UPDATE must also be routed as Select"
        );
    }

    #[test]
    fn mysql_classify_statement_table_maintenance_commands_are_selects() {
        for sql in [
            "ANALYZE TABLE employees",
            "CHECK TABLE employees",
            "CHECKSUM TABLE employees",
            "OPTIMIZE TABLE employees",
            "REPAIR TABLE employees",
        ] {
            assert_eq!(
                MysqlExecutor::classify_statement(sql),
                super::MysqlStatementKind::Select,
                "{sql} returns a result set in MySQL/MariaDB and must not be discarded"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Bug fix: parse_mysql_parameter must skip leading `--` style comments
    // just as it already skips leading `#` comments.
    // -----------------------------------------------------------------------

    #[test]
    fn mysql_parse_routine_arguments_ignores_comma_inside_dash_dash_comment() {
        // The `--` comment on the first parameter contains a comma; it must not
        // be treated as a parameter separator.
        let ddl = "CREATE PROCEDURE `dash_comment_proc`(\
            p_id INT,    -- first param, the user id\n\
            p_name VARCHAR(50)\
        )\nBEGIN SELECT 1; END";

        let arguments =
            MysqlObjectBrowser::parse_routine_arguments_from_create_ddl(ddl, "PROCEDURE")
                .expect("procedure with -- comments should parse");

        assert_eq!(
            arguments.len(),
            2,
            "comma inside -- comment must not create a phantom parameter: {arguments:?}"
        );
        assert_eq!(arguments[0].name.as_deref(), Some("p_id"));
        assert_eq!(arguments[0].data_type.as_deref(), Some("INT"));
        assert_eq!(arguments[1].name.as_deref(), Some("p_name"));
        assert_eq!(arguments[1].data_type.as_deref(), Some("VARCHAR(50)"));
    }

    #[test]
    fn mysql_parse_routine_arguments_skips_leading_dash_dash_comment_before_param() {
        // A `--` comment appears on its own line before the parameter name.
        // parse_mysql_parameter must skip it and still parse the name correctly.
        let ddl = "CREATE PROCEDURE `leading_dash_proc`(\
            -- user id\n\
            p_id INT,\
            -- user name\n\
            p_name VARCHAR(100)\
        )\nBEGIN SELECT 1; END";

        let arguments =
            MysqlObjectBrowser::parse_routine_arguments_from_create_ddl(ddl, "PROCEDURE")
                .expect("procedure with leading -- comments should parse");

        assert_eq!(
            arguments.len(),
            2,
            "leading -- comment must not break parameter parsing: {arguments:?}"
        );
        assert_eq!(arguments[0].name.as_deref(), Some("p_id"));
        assert_eq!(arguments[0].data_type.as_deref(), Some("INT"));
        assert_eq!(arguments[1].name.as_deref(), Some("p_name"));
        assert_eq!(arguments[1].data_type.as_deref(), Some("VARCHAR(100)"));
    }

    #[test]
    fn mysql_parse_routine_arguments_ignores_default_inside_dash_dash_comment() {
        // DEFAULT keyword appearing in a `--` comment must not be treated as
        // the parameter default-value marker.
        let ddl = "CREATE PROCEDURE `dash_default_proc`(\
            p_status VARCHAR(20) -- DEFAULT 'active'\n\
        )\nBEGIN SELECT 1; END";

        let arguments =
            MysqlObjectBrowser::parse_routine_arguments_from_create_ddl(ddl, "PROCEDURE")
                .expect("procedure with DEFAULT in -- comment should parse");

        assert_eq!(arguments.len(), 1);
        assert_eq!(arguments[0].name.as_deref(), Some("p_status"));
        assert_eq!(arguments[0].data_type.as_deref(), Some("VARCHAR(20)"));
        assert!(
            arguments[0].default_value.is_none(),
            "DEFAULT inside -- comment must not be parsed as actual default value"
        );
    }

    #[test]
    fn mysql_parse_routine_arguments_keeps_backslash_escaped_quote_inside_default_string() {
        let ddl = "CREATE PROCEDURE `escaped_default_proc`(\
            p_msg VARCHAR(50) DEFAULT 'it\\'s ok',\
            p_count INT\
        )\nBEGIN SELECT 1; END";

        let arguments =
            MysqlObjectBrowser::parse_routine_arguments_from_create_ddl(ddl, "PROCEDURE")
                .expect("procedure with backslash-escaped default string should parse");

        assert_eq!(
            arguments.len(),
            2,
            "escaped quote must not swallow the next parameter"
        );
        assert_eq!(arguments[0].name.as_deref(), Some("p_msg"));
        assert_eq!(arguments[0].default_value.as_deref(), Some("'it\\'s ok'"));
        assert_eq!(arguments[1].name.as_deref(), Some("p_count"));
    }

    #[test]
    fn mysql_parse_routine_arguments_keeps_double_dash_expression_when_no_comment_whitespace() {
        let ddl = "CREATE PROCEDURE `dash_expr_proc`(\
            p_score INT DEFAULT (5--2),\
            p_limit INT\
        )\nBEGIN SELECT 1; END";

        let arguments =
            MysqlObjectBrowser::parse_routine_arguments_from_create_ddl(ddl, "PROCEDURE")
                .expect("procedure with `--` arithmetic default should parse");

        assert_eq!(
            arguments.len(),
            2,
            "`--<non-space>` must stay part of the default expression"
        );
        assert_eq!(arguments[0].name.as_deref(), Some("p_score"));
        assert_eq!(arguments[0].default_value.as_deref(), Some("(5--2)"));
        assert_eq!(arguments[1].name.as_deref(), Some("p_limit"));
    }

    #[test]
    fn mysql_parse_routine_arguments_skips_leading_block_comment_before_param() {
        let ddl = "CREATE PROCEDURE `block_comment_proc`(\
            p_id INT,\
            /* second, user-facing parameter */ p_name VARCHAR(100)\
        )\nBEGIN SELECT 1; END";

        let arguments =
            MysqlObjectBrowser::parse_routine_arguments_from_create_ddl(ddl, "PROCEDURE")
                .expect("procedure with leading block comments should parse");

        assert_eq!(
            arguments.len(),
            2,
            "leading block comment must not hide the following parameter: {arguments:?}"
        );
        assert_eq!(arguments[0].name.as_deref(), Some("p_id"));
        assert_eq!(arguments[0].data_type.as_deref(), Some("INT"));
        assert_eq!(arguments[1].name.as_deref(), Some("p_name"));
        assert_eq!(arguments[1].data_type.as_deref(), Some("VARCHAR(100)"));
    }

    #[test]
    fn mysql_parse_routine_arguments_strips_inline_block_comment_from_type_section() {
        let ddl = "CREATE PROCEDURE `block_default_proc`(\
            p_status VARCHAR(20) /* keep legacy width */ DEFAULT 'active'\
        )\nBEGIN SELECT 1; END";

        let arguments =
            MysqlObjectBrowser::parse_routine_arguments_from_create_ddl(ddl, "PROCEDURE")
                .expect("procedure with inline block comment before DEFAULT should parse");

        assert_eq!(arguments.len(), 1);
        assert_eq!(arguments[0].name.as_deref(), Some("p_status"));
        assert_eq!(arguments[0].data_type.as_deref(), Some("VARCHAR(20)"));
        assert_eq!(arguments[0].default_value.as_deref(), Some("'active'"));
    }

    #[test]
    fn mysql_parse_routine_arguments_function_return_type_ignores_inline_block_comment() {
        let ddl = "CREATE FUNCTION `fn_block_comment_return`(p_id INT)\
            RETURNS VARCHAR(20) /* display label */ CHARACTER SET utf8mb4 DETERMINISTIC\n\
            RETURN 'ok'";

        let arguments =
            MysqlObjectBrowser::parse_routine_arguments_from_create_ddl(ddl, "FUNCTION")
                .expect("function return type with inline block comment should parse");

        assert_eq!(arguments[0].position, 0);
        assert_eq!(arguments[0].in_out.as_deref(), Some("RETURN"));
        assert_eq!(
            arguments[0].data_type.as_deref(),
            Some("VARCHAR(20) CHARACTER SET utf8mb4")
        );
    }
}
