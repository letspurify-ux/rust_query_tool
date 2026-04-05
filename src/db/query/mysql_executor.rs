use mysql::prelude::*;
use mysql::{Conn, Error as MysqlError, Row, Value as MysqlValue};
use std::time::{Duration, Instant};

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
                let db_name = trimmed
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or("")
                    .trim_end_matches(';')
                    .trim_matches('`');
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

    /// Check if a MySQL error is a timeout/cancelled error.
    pub fn is_timeout_error(err: &MysqlError) -> bool {
        let msg = err.to_string();
        msg.contains("Query execution was interrupted")
            || msg.contains("Lock wait timeout exceeded")
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

            if byte == b'-' && next == Some(b'-') {
                in_line_comment = true;
                index += 2;
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

            if byte == b'-' && next == Some(b'-') {
                in_line_comment = true;
                index += 2;
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

            if byte == b'-' && next == Some(b'-') {
                in_line_comment = true;
                index += 2;
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
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
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

    pub fn get_triggers(conn: &mut Conn) -> Result<Vec<String>, MysqlError> {
        let rows: Vec<String> = conn.query(
            "SELECT TRIGGER_NAME FROM INFORMATION_SCHEMA.TRIGGERS \
             WHERE TRIGGER_SCHEMA = DATABASE() \
             ORDER BY TRIGGER_NAME",
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
        let ddl_column_index = match object_type_upper.as_str() {
            "TABLE" | "VIEW" => 1usize,
            "PROCEDURE" | "FUNCTION" | "TRIGGER" => 2usize,
            _ => return Ok(String::new()),
        };

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
        let Some(row_result) = result.next() else {
            return Ok(String::new());
        };
        let row = row_result?;
        Ok(row
            .as_ref(ddl_column_index)
            .map(MysqlExecutor::value_to_string)
            .unwrap_or_default())
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
            (schema_name, object_name),
        )?;
        object_types.append(&mut trigger_types);

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
    use mysql::Value as MysqlValue;
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
}
