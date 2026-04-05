use mysql::prelude::*;
use mysql::{Conn, Error as MysqlError, Row};
use std::time::Instant;

use crate::db::query::types::{ColumnInfo, QueryResult};

pub struct MysqlExecutor;

pub struct MysqlObjectBrowser;

impl MysqlExecutor {
    pub fn execute(conn: &mut Conn, sql: &str) -> Result<QueryResult, MysqlError> {
        let trimmed = sql.trim();
        let upper = trimmed.to_ascii_uppercase();
        let first_word = upper.split_whitespace().next().unwrap_or("");

        match first_word {
            "SELECT" | "WITH" | "SHOW" | "DESCRIBE" | "DESC" | "EXPLAIN" => {
                Self::execute_select(conn, sql)
            }
            "INSERT" | "UPDATE" | "DELETE" | "REPLACE" => Self::execute_dml(conn, sql),
            "COMMIT" => {
                let start = Instant::now();
                conn.query_drop("COMMIT")?;
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
            "ROLLBACK" => {
                let start = Instant::now();
                conn.query_drop("ROLLBACK")?;
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
            "USE" => {
                let start = Instant::now();
                conn.query_drop(sql)?;
                let db_name = trimmed
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or("")
                    .trim_end_matches(';')
                    .trim_matches('`');
                Ok(QueryResult {
                    sql: sql.to_string(),
                    columns: vec![],
                    rows: vec![],
                    row_count: 0,
                    execution_time: start.elapsed(),
                    message: format!("Database changed to '{}'.", db_name),
                    is_select: false,
                    success: true,
                })
            }
            "CALL" => Self::execute_call(conn, sql),
            _ => Self::execute_ddl(conn, sql),
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
            let mut row_values = Vec::with_capacity(columns.len());
            for i in 0..columns.len() {
                let val: Option<String> = row.get(i);
                row_values.push(val.unwrap_or_else(|| "NULL".to_string()));
            }
            rows.push(row_values);
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
            let mut row_values = Vec::with_capacity(columns.len());
            for i in 0..columns.len() {
                let val: Option<String> = row.get(i);
                row_values.push(val.unwrap_or_else(|| "NULL".to_string()));
            }
            row_count += 1;
            if !on_row(row_values) {
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
        let stmt_type = trimmed
            .split_whitespace()
            .next()
            .unwrap_or("DML")
            .to_ascii_uppercase();
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

    fn execute_call(conn: &mut Conn, sql: &str) -> Result<QueryResult, MysqlError> {
        // CALL may return result sets
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

        if columns.is_empty() {
            return Ok(QueryResult {
                sql: sql.to_string(),
                columns: vec![],
                rows: vec![],
                row_count: 0,
                execution_time: start.elapsed(),
                message: "CALL executed.".to_string(),
                is_select: false,
                success: true,
            });
        }

        let mut rows: Vec<Vec<String>> = Vec::new();
        for row_result in result {
            let row: Row = row_result?;
            let mut row_values = Vec::with_capacity(columns.len());
            for i in 0..columns.len() {
                let val: Option<String> = row.get(i);
                row_values.push(val.unwrap_or_else(|| "NULL".to_string()));
            }
            rows.push(row_values);
        }

        Ok(QueryResult::new_select(sql, columns, rows, start.elapsed()))
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
            results.push(Self::execute(conn, trimmed));
        }
        results
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

    pub fn get_table_columns(
        conn: &mut Conn,
        table_name: &str,
    ) -> Result<Vec<ColumnInfo>, MysqlError> {
        let rows: Vec<(String, String)> = conn.exec(
            "SELECT COLUMN_NAME, COLUMN_TYPE \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? \
             ORDER BY ORDINAL_POSITION",
            (table_name,),
        )?;

        Ok(rows
            .into_iter()
            .map(|(name, data_type)| ColumnInfo { name, data_type })
            .collect())
    }

    pub fn get_databases(conn: &mut Conn) -> Result<Vec<String>, MysqlError> {
        let rows: Vec<String> = conn.query("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME")?;
        Ok(rows)
    }

    pub fn get_create_table(
        conn: &mut Conn,
        table_name: &str,
    ) -> Result<String, MysqlError> {
        let result: Option<(String, String)> =
            conn.exec_first(format!("SHOW CREATE TABLE `{}`", table_name), ())?;
        match result {
            Some((_, ddl)) => Ok(ddl),
            None => Ok(String::new()),
        }
    }

    pub fn get_table_constraints(
        conn: &mut Conn,
        table_name: &str,
    ) -> Result<Vec<(String, String, String)>, MysqlError> {
        let rows: Vec<(String, String, String)> = conn.exec(
            "SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE, \
             GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION) as COLUMNS \
             FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc \
             JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu USING (CONSTRAINT_NAME, TABLE_SCHEMA, TABLE_NAME) \
             WHERE tc.TABLE_SCHEMA = DATABASE() AND tc.TABLE_NAME = ? \
             GROUP BY CONSTRAINT_NAME, CONSTRAINT_TYPE \
             ORDER BY CONSTRAINT_TYPE, CONSTRAINT_NAME",
            (table_name,),
        )?;
        Ok(rows)
    }
}
