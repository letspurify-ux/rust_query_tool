/// Synchronous wrapper around the async oracle-rs Connection.
///
/// oracle-rs uses Tokio async/await. This wrapper bridges it into the
/// existing synchronous execution model by running each async operation
/// on a dedicated Tokio runtime.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use oracle_rs::{Config, Connection};

/// Shared cancel handle — `Arc<AtomicBool>` pointing into the oracle-rs
/// connection's cancel flag. Stored in `SqlEditorWidget` the same way
/// `current_query_connection` is used for OCI.
#[derive(Clone)]
pub struct OracleThinCancelHandle {
    flag: Arc<AtomicBool>,
}

impl OracleThinCancelHandle {
    pub fn cancel(&self) {
        self.flag.store(true, Ordering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
    }
}

impl Drop for OracleThinCancelHandle {
    fn drop(&mut self) {
        // Best-effort reset so the flag is clean for re-use.
        self.flag.store(false, Ordering::Relaxed);
    }
}

/// A synchronous Oracle Thin client backed by oracle-rs.
///
/// Holds an Arc so the connection can be passed across threads while the
/// `OracleThinCancelHandle` lives in a separate, independently cloneable
/// `Arc<AtomicBool>`.
pub struct OracleThinClient {
    conn: Arc<Connection>,
    runtime: tokio::runtime::Runtime,
}

impl OracleThinClient {
    /// Connect synchronously, returning the client and its cancel handle.
    pub fn connect(
        host: &str,
        port: u16,
        service_name: &str,
        username: &str,
        password: &str,
    ) -> Result<(Self, OracleThinCancelHandle), String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("Failed to create Tokio runtime: {e}"))?;

        let config = Config::new(host, port, service_name, username, password);

        let conn = runtime
            .block_on(Connection::connect_with_config(config))
            .map_err(|e| e.to_string())?;

        let conn = Arc::new(conn);
        let cancel_flag = conn.cancel_handle();
        let handle = OracleThinCancelHandle { flag: cancel_flag };

        Ok((Self { conn, runtime }, handle))
    }

    /// Apply default session settings (NLS formats) matching the OCI path.
    pub fn apply_default_session_settings(&self) -> Result<(), String> {
        let statements = [
            "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'yyyy-mm-dd hh24:mi:ss.ff6'",
            "ALTER SESSION SET NLS_DATE_FORMAT = 'yyyy-mm-dd hh24:mi:ss'",
        ];
        for sql in statements {
            if let Err(e) = self.execute_ddl(sql) {
                eprintln!("Warning: failed to apply thin session setting `{sql}`: {e}");
            }
        }
        Ok(())
    }

    // ─── Accessor ────────────────────────────────────────────────────────────

    pub fn connection(&self) -> Arc<Connection> {
        Arc::clone(&self.conn)
    }

    // ─── Synchronous query interface ─────────────────────────────────────────

    /// Execute a SELECT and return the complete result set in one shot.
    pub fn query(
        &self,
        sql: &str,
        params: &[oracle_rs::Value],
    ) -> Result<oracle_rs::QueryResult, String> {
        self.runtime
            .block_on(self.conn.query(sql, params))
            .map_err(|e| e.to_string())
    }

    /// Fetch the next page of rows for a cursor that has `has_more_rows == true`.
    pub fn fetch_more(
        &self,
        cursor_id: u16,
        columns: &[oracle_rs::ColumnInfo],
        fetch_size: u32,
    ) -> Result<oracle_rs::QueryResult, String> {
        self.runtime
            .block_on(self.conn.fetch_more(cursor_id, columns, fetch_size))
            .map_err(|e| e.to_string())
    }

    /// Execute a DML/DDL statement and return rows affected.
    pub fn execute(
        &self,
        sql: &str,
        params: &[oracle_rs::Value],
    ) -> Result<oracle_rs::QueryResult, String> {
        self.runtime
            .block_on(self.conn.execute(sql, params))
            .map_err(|e| e.to_string())
    }

    /// Execute a DDL/utility statement that returns no rows.
    pub fn execute_ddl(&self, sql: &str) -> Result<(), String> {
        self.runtime
            .block_on(self.conn.execute(sql, &[]))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    pub fn commit(&self) -> Result<(), String> {
        self.runtime
            .block_on(self.conn.commit())
            .map_err(|e| e.to_string())
    }

    pub fn rollback(&self) -> Result<(), String> {
        self.runtime
            .block_on(self.conn.rollback())
            .map_err(|e| e.to_string())
    }

    /// Set a query execution timeout.
    ///
    /// oracle-rs does not expose a native call timeout; we implement it via
    /// Tokio's `timeout` combinator inside `executor.rs` instead, so this
    /// method is a no-op placeholder that keeps the OCI execution path
    /// structurally identical.
    pub fn set_call_timeout(&self, _timeout: Option<Duration>) -> Result<(), String> {
        // Implemented in executor streaming loop via tokio::time::timeout.
        Ok(())
    }

    /// Request cancellation of any in-flight query.
    ///
    /// python-oracledb thin mode sends a TNS MARKER/BREAK packet on the
    /// existing socket to interrupt the server. We do the same by delegating
    /// to oracle-rs's `request_cancel()` which sets the per-connection
    /// cancel flag. The fetch loop in `executor.rs` checks this flag
    /// between each page and returns early.
    pub fn request_cancel(&self) {
        self.conn.request_cancel();
    }

    pub fn reset_cancel(&self) {
        self.conn.reset_cancel();
    }

    pub fn is_cancel_requested(&self) -> bool {
        self.conn.is_cancel_requested()
    }
}
