/// Oracle Thin executor — mirrors the OCI `QueryExecutor::execute_batch_streaming`
/// interface but uses oracle-rs under the hood.
///
/// Statement classification mirrors `QueryExecutor` so that the same tool-command
/// handling in `execution.rs` applies equally to OCI and thin connections.
use std::time::Instant;

use oracle_rs::Value as ThinValue;

use crate::db::{ColumnInfo, QueryResult};

use super::client::OracleThinClient;

const THIN_FETCH_SIZE: u32 = 1_000;

pub struct OracleThinExecutor;

// ─── Value → String ───────────────────────────────────────────────────────────

/// Convert an oracle-rs `Value` to the display string used throughout the query
/// tool result grid (matching OCI behaviour: NULL → "NULL", etc.).
fn value_to_string(v: &ThinValue) -> String {
    use oracle_rs::Value::*;
    match v {
        Null => "NULL".to_string(),
        // Use the Display impl which already handles Date/Timestamp formatting
        other => other.to_string(),
    }
}

/// Convert a row (slice of oracle-rs Values) to a Vec<String>.
fn row_to_strings(row: &oracle_rs::Row) -> Vec<String> {
    row.values().iter().map(value_to_string).collect()
}

/// Map oracle-rs ColumnInfo to our internal ColumnInfo.
fn map_columns(cols: &[oracle_rs::ColumnInfo]) -> Vec<ColumnInfo> {
    cols.iter()
        .map(|c| ColumnInfo {
            name: c.name.clone(),
            data_type: format!("{:?}", c.oracle_type),
        })
        .collect()
}

// ─── Statement classification ────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ThinStatementKind {
    Select,
    Dml,
    Ddl,
    Commit,
    Rollback,
    Plsql,
}

fn classify_statement(sql: &str) -> ThinStatementKind {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();
    let first_word = upper.split_whitespace().next().unwrap_or("");

    match first_word {
        "SELECT" | "WITH" => {
            // WITH ... SELECT is a query
            if first_word == "WITH" {
                // Heuristic: does the first non-CTE clause contain SELECT?
                // For simplicity treat all WITH as SELECT.
                ThinStatementKind::Select
            } else {
                ThinStatementKind::Select
            }
        }
        "INSERT" | "UPDATE" | "DELETE" | "MERGE" => ThinStatementKind::Dml,
        "COMMIT" => ThinStatementKind::Commit,
        "ROLLBACK" => ThinStatementKind::Rollback,
        "BEGIN" | "DECLARE" | "CALL" => ThinStatementKind::Plsql,
        _ => ThinStatementKind::Ddl,
    }
}

// ─── Streaming executor ───────────────────────────────────────────────────────

impl OracleThinExecutor {
    /// Execute a single SQL statement and stream rows via callbacks.
    ///
    /// * `on_select_start` – called once with column metadata when a SELECT starts
    /// * `on_row`          – called for each row; return `false` to cancel
    ///
    /// Returns `(QueryResult, was_cancelled)`.
    pub fn execute_streaming<F, G>(
        client: &OracleThinClient,
        sql: &str,
        on_select_start: &mut F,
        on_row: &mut G,
    ) -> Result<(QueryResult, bool), String>
    where
        F: FnMut(&[ColumnInfo]),
        G: FnMut(Vec<String>) -> bool,
    {
        let trimmed = sql.trim();
        let kind = classify_statement(trimmed);
        let start = Instant::now();

        match kind {
            ThinStatementKind::Select => {
                Self::execute_select_streaming(client, trimmed, start, on_select_start, on_row)
            }
            ThinStatementKind::Dml => {
                let result = Self::execute_dml(client, trimmed, start)?;
                Ok((result, false))
            }
            ThinStatementKind::Commit => {
                client.commit()?;
                Ok((
                    QueryResult {
                        sql: sql.to_string(),
                        columns: vec![],
                        rows: vec![],
                        row_count: 0,
                        execution_time: start.elapsed(),
                        message: "Commit complete.".to_string(),
                        is_select: false,
                        success: true,
                    },
                    false,
                ))
            }
            ThinStatementKind::Rollback => {
                client.rollback()?;
                Ok((
                    QueryResult {
                        sql: sql.to_string(),
                        columns: vec![],
                        rows: vec![],
                        row_count: 0,
                        execution_time: start.elapsed(),
                        message: "Rollback complete.".to_string(),
                        is_select: false,
                        success: true,
                    },
                    false,
                ))
            }
            ThinStatementKind::Plsql | ThinStatementKind::Ddl => {
                let result = Self::execute_ddl(client, trimmed, start)?;
                Ok((result, false))
            }
        }
    }

    // ── SELECT with paged streaming ─────────────────────────────────────────

    fn execute_select_streaming<F, G>(
        client: &OracleThinClient,
        sql: &str,
        start: Instant,
        on_select_start: &mut F,
        on_row: &mut G,
    ) -> Result<(QueryResult, bool), String>
    where
        F: FnMut(&[ColumnInfo]),
        G: FnMut(Vec<String>) -> bool,
    {
        // First page
        let page = client.query(sql, &[])?;
        let our_cols = map_columns(&page.columns);
        on_select_start(&our_cols);

        let mut row_count: usize = 0;
        let mut cancelled = false;

        // Emit first-page rows
        for row in &page.rows {
            if client.is_cancel_requested() {
                cancelled = true;
                break;
            }
            row_count += 1;
            if !on_row(row_to_strings(row)) {
                cancelled = true;
                break;
            }
        }

        // Fetch remaining pages (python-oracledb thin uses same paging approach)
        if !cancelled && page.has_more_rows {
            let cursor_id = page.cursor_id;
            let cols = page.columns.clone();

            loop {
                if client.is_cancel_requested() {
                    cancelled = true;
                    break;
                }

                let next = client.fetch_more(cursor_id, &cols, THIN_FETCH_SIZE)?;

                for row in &next.rows {
                    if client.is_cancel_requested() {
                        cancelled = true;
                        break;
                    }
                    row_count += 1;
                    if !on_row(row_to_strings(row)) {
                        cancelled = true;
                        break;
                    }
                }

                if cancelled || !next.has_more_rows {
                    break;
                }
            }
        }

        let result = QueryResult::new_select_streamed(sql, our_cols, row_count, start.elapsed());
        Ok((result, cancelled))
    }

    // ── DML ─────────────────────────────────────────────────────────────────

    fn execute_dml(client: &OracleThinClient, sql: &str, start: Instant) -> Result<QueryResult, String> {
        let qr = client.execute(sql, &[])?;
        let stmt_type = sql
            .split_whitespace()
            .next()
            .unwrap_or("DML")
            .to_ascii_uppercase();
        Ok(QueryResult::new_dml(sql, qr.rows_affected, start.elapsed(), &stmt_type))
    }

    // ── DDL / PL-SQL ────────────────────────────────────────────────────────

    fn execute_ddl(client: &OracleThinClient, sql: &str, start: Instant) -> Result<QueryResult, String> {
        client.execute(sql, &[]).map(|qr| {
            let stmt_keyword = sql
                .split_whitespace()
                .take(2)
                .collect::<Vec<_>>()
                .join(" ")
                .to_ascii_uppercase();
            let message = if qr.rows_affected > 0 {
                format!("{} - {} row(s) affected", stmt_keyword, qr.rows_affected)
            } else {
                format!("{} complete.", stmt_keyword)
            };
            QueryResult {
                sql: sql.to_string(),
                columns: vec![],
                rows: vec![],
                row_count: qr.rows_affected as usize,
                execution_time: start.elapsed(),
                message,
                is_select: false,
                success: true,
            }
        })
    }

    // ── Bind variable handling (positional) ─────────────────────────────────

    /// Build positional oracle-rs bind params from session bind values.
    ///
    /// OCI thin mode supports positional parameters (:1, :2, …). Named binds
    /// (:name) are also passed through — oracle-rs handles both.
    pub fn build_bind_params(
        bind_values: &[(String, Option<String>)],
    ) -> Vec<ThinValue> {
        bind_values
            .iter()
            .map(|(_, val)| match val {
                Some(s) => ThinValue::String(s.clone()),
                None => ThinValue::Null,
            })
            .collect()
    }

    // ── Non-streaming helper for single-statement execution ─────────────────

    /// Execute a single statement and return all rows buffered (no streaming).
    /// Used by intellisense and object browser lookups.
    pub fn execute(client: &OracleThinClient, sql: &str) -> Result<QueryResult, String> {
        let start = Instant::now();
        let kind = classify_statement(sql.trim());

        match kind {
            ThinStatementKind::Select => {
                let mut all_rows: Vec<Vec<String>> = Vec::new();
                let mut columns: Vec<ColumnInfo> = Vec::new();

                let mut first = true;
                let mut on_start = |cols: &[ColumnInfo]| {
                    if first {
                        columns = cols.to_vec();
                        first = false;
                    }
                };
                let mut on_row = |row: Vec<String>| {
                    all_rows.push(row);
                    true
                };

                let (result, _) = Self::execute_streaming(
                    client,
                    sql,
                    &mut on_start,
                    &mut on_row,
                )?;

                Ok(QueryResult {
                    rows: all_rows,
                    row_count: result.row_count,
                    ..result
                })
            }
            ThinStatementKind::Dml => Self::execute_dml(client, sql.trim(), start),
            ThinStatementKind::Commit => {
                client.commit()?;
                Ok(QueryResult {
                    sql: sql.to_string(),
                    columns: vec![],
                    rows: vec![],
                    row_count: 0,
                    execution_time: start.elapsed(),
                    message: "Commit complete.".to_string(),
                    is_select: false,
                    success: true,
                })
            }
            ThinStatementKind::Rollback => {
                client.rollback()?;
                Ok(QueryResult {
                    sql: sql.to_string(),
                    columns: vec![],
                    rows: vec![],
                    row_count: 0,
                    execution_time: start.elapsed(),
                    message: "Rollback complete.".to_string(),
                    is_select: false,
                    success: true,
                })
            }
            ThinStatementKind::Plsql | ThinStatementKind::Ddl => {
                Self::execute_ddl(client, sql.trim(), start)
            }
        }
    }
}
