use oracle::{Connection, Error as OracleError};
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use crate::db::session::SessionState;

pub const NOT_CONNECTED_MESSAGE: &str = "Not connected to database";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionInfo {
    pub name: String,
    pub username: String,
    #[serde(skip_serializing, default)]
    pub password: String,
    pub host: String,
    pub port: u16,
    pub service_name: String,
}

impl ConnectionInfo {
    pub fn new(
        name: &str,
        username: &str,
        password: &str,
        host: &str,
        port: u16,
        service_name: &str,
    ) -> Self {
        Self {
            name: name.to_string(),
            username: username.to_string(),
            password: password.to_string(),
            host: host.to_string(),
            port,
            service_name: service_name.to_string(),
        }
    }

    pub fn connection_string(&self) -> String {
        format!("//{}:{}/{}", self.host, self.port, self.service_name)
    }

    pub fn display_string(&self) -> String {
        format!(
            "{} ({}@{}:{}/{})",
            self.name, self.username, self.host, self.port, self.service_name
        )
    }

    /// Securely clear the password from memory by overwriting with zeros
    /// then releasing the allocation.
    pub fn clear_password(&mut self) {
        // Overwrite the existing bytes with zeros before dropping
        // SAFETY: we write zeros over the valid UTF-8 bytes (zeros are valid UTF-8)
        let bytes = unsafe { self.password.as_bytes_mut() };
        for b in bytes.iter_mut() {
            // Use write_volatile to prevent the compiler from optimizing away the zeroing
            unsafe { std::ptr::write_volatile(b, 0) };
        }
        self.password.clear();
        self.password.shrink_to_fit();
    }
}

impl Default for ConnectionInfo {
    fn default() -> Self {
        Self {
            name: String::new(),
            username: String::new(),
            password: String::new(),
            host: "localhost".to_string(),
            port: 1521,
            service_name: "ORCL".to_string(),
        }
    }
}

pub struct DatabaseConnection {
    connection: Option<Arc<Connection>>,
    info: ConnectionInfo,
    connected: bool,
    auto_commit: bool,
    session: Arc<Mutex<SessionState>>,
    last_disconnect_reason: Option<String>,
    connection_generation: u64,
}

impl DatabaseConnection {
    pub fn new() -> Self {
        Self {
            connection: None,
            info: ConnectionInfo::default(),
            connected: false,
            auto_commit: false,
            session: Arc::new(Mutex::new(SessionState::default())),
            last_disconnect_reason: None,
            connection_generation: 0,
        }
    }

    pub fn connect(&mut self, info: ConnectionInfo) -> Result<(), OracleError> {
        let conn_str = info.connection_string();
        let connection = Arc::new(
            match Connection::connect(&info.username, &info.password, &conn_str) {
                Ok(connection) => connection,
                Err(err) => {
                    eprintln!("Connection error: {err}");
                    return Err(err);
                }
            },
        );

        Self::apply_default_session_settings(connection.as_ref());

        self.connection = Some(connection);
        self.info = info;
        // Clear password from memory now that the connection is established
        self.info.clear_password();
        self.connected = true;
        self.last_disconnect_reason = None;
        self.connection_generation = self.connection_generation.wrapping_add(1);

        Ok(())
    }

    fn apply_default_session_settings(conn: &Connection) {
        let statements = [
            "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'yyyy-mm-dd hh24:mi:ss'",
            "ALTER SESSION SET NLS_DATE_FORMAT = 'yyyy-mm-dd hh24:mi:ss'",
        ];

        for statement in statements {
            if let Err(err) = conn.execute(statement, &[]) {
                eprintln!("Warning: failed to apply default session setting `{statement}`: {err}");
            }
        }
    }

    pub fn disconnect(&mut self) {
        let had_connection = self.connection.is_some() || self.connected;
        self.connection = None;
        self.connected = false;
        self.last_disconnect_reason = None;
        if had_connection {
            self.connection_generation = self.connection_generation.wrapping_add(1);
        }
    }

    fn mark_disconnected_with_reason(&mut self, reason: impl Into<String>) {
        let had_connection = self.connection.is_some() || self.connected;
        self.connection = None;
        self.connected = false;
        self.last_disconnect_reason = Some(reason.into());
        if had_connection {
            self.connection_generation = self.connection_generation.wrapping_add(1);
        }
    }

    fn disconnect_message(&self) -> String {
        self.last_disconnect_reason
            .clone()
            .unwrap_or_else(|| NOT_CONNECTED_MESSAGE.to_string())
    }

    /// Validate that the current connection is still alive.
    ///
    /// Some DB servers terminate idle sessions; in that case we clear the stale
    /// handle so callers can prompt for reconnect before running work.
    pub fn ensure_connection_alive(&mut self) -> bool {
        if !self.connected {
            return false;
        }

        let Some(conn) = self.connection.as_ref() else {
            self.connected = false;
            if self.last_disconnect_reason.is_none() {
                self.last_disconnect_reason = Some(NOT_CONNECTED_MESSAGE.to_string());
            }
            return false;
        };

        match conn.ping() {
            Ok(()) => true,
            Err(err) => {
                eprintln!("Detected stale DB connection during ping: {err}");
                self.mark_disconnected_with_reason(format!(
                    "Connection was lost unexpectedly: {err}"
                ));
                false
            }
        }
    }

    pub fn require_live_connection(&mut self) -> Result<Arc<Connection>, String> {
        if !self.ensure_connection_alive() {
            return Err(self.disconnect_message());
        }

        self.get_connection()
            .ok_or_else(|| self.disconnect_message())
    }

    pub fn is_connected(&self) -> bool {
        self.connected
    }

    pub fn get_connection(&self) -> Option<Arc<Connection>> {
        self.connection.clone()
    }

    pub fn get_info(&self) -> &ConnectionInfo {
        &self.info
    }

    pub fn connection_generation(&self) -> u64 {
        self.connection_generation
    }

    pub fn set_auto_commit(&mut self, enabled: bool) {
        self.auto_commit = enabled;
    }

    pub fn auto_commit(&self) -> bool {
        self.auto_commit
    }

    pub fn session_state(&self) -> Arc<Mutex<SessionState>> {
        Arc::clone(&self.session)
    }

    pub fn test_connection(info: &ConnectionInfo) -> Result<(), OracleError> {
        let conn_str = info.connection_string();
        match Connection::connect(&info.username, &info.password, &conn_str) {
            Ok(_connection) => {}
            Err(err) => {
                eprintln!("Connection error: {err}");
                return Err(err);
            }
        }
        Ok(())
    }
}

impl Default for DatabaseConnection {
    fn default() -> Self {
        Self::new()
    }
}

pub type SharedConnection = Arc<Mutex<DatabaseConnection>>;

static ACTIVE_DB_ACTIVITY: OnceLock<Mutex<Option<String>>> = OnceLock::new();

fn db_activity_slot() -> &'static Mutex<Option<String>> {
    ACTIVE_DB_ACTIVITY.get_or_init(|| Mutex::new(None))
}

fn set_current_db_activity(activity: Option<String>) {
    match db_activity_slot().lock() {
        Ok(mut guard) => {
            *guard = activity;
        }
        Err(poisoned) => {
            eprintln!("Warning: DB activity lock was poisoned; recovering.");
            *poisoned.into_inner() = activity;
        }
    }
}

pub fn current_db_activity() -> Option<String> {
    match db_activity_slot().lock() {
        Ok(guard) => guard.clone(),
        Err(poisoned) => {
            eprintln!("Warning: DB activity lock was poisoned; recovering.");
            poisoned.into_inner().clone()
        }
    }
}

pub fn format_connection_busy_message() -> String {
    match current_db_activity() {
        Some(activity) => format!("Connection is busy. Current DB activity: {}", activity),
        None => "Connection is busy. Try again after the current operation finishes.".to_string(),
    }
}

pub struct ConnectionLockGuard<'a> {
    guard: MutexGuard<'a, DatabaseConnection>,
    tracks_activity: bool,
}

impl<'a> ConnectionLockGuard<'a> {
    fn with_activity(mut self, activity: String) -> Self {
        set_current_db_activity(Some(activity));
        self.tracks_activity = true;
        self
    }
}

impl<'a> Deref for ConnectionLockGuard<'a> {
    type Target = DatabaseConnection;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<'a> DerefMut for ConnectionLockGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl<'a> Drop for ConnectionLockGuard<'a> {
    fn drop(&mut self) {
        if self.tracks_activity {
            set_current_db_activity(None);
        }
    }
}

pub fn create_shared_connection() -> SharedConnection {
    Arc::new(Mutex::new(DatabaseConnection::new()))
}

pub fn lock_connection(connection: &SharedConnection) -> ConnectionLockGuard<'_> {
    let guard = match connection.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            eprintln!("Warning: database connection lock was poisoned; recovering.");
            poisoned.into_inner()
        }
    };
    ConnectionLockGuard {
        guard,
        tracks_activity: false,
    }
}

pub fn lock_connection_with_activity(
    connection: &SharedConnection,
    activity: impl Into<String>,
) -> ConnectionLockGuard<'_> {
    lock_connection(connection).with_activity(activity.into())
}

/// Try to acquire the connection lock without blocking.
/// Returns None if the lock is already held (query is running).
pub fn try_lock_connection(connection: &SharedConnection) -> Option<ConnectionLockGuard<'_>> {
    match connection.try_lock() {
        Ok(guard) => Some(ConnectionLockGuard {
            guard,
            tracks_activity: false,
        }),
        Err(std::sync::TryLockError::WouldBlock) => None,
        Err(std::sync::TryLockError::Poisoned(poisoned)) => {
            eprintln!("Warning: database connection lock was poisoned; recovering.");
            Some(ConnectionLockGuard {
                guard: poisoned.into_inner(),
                tracks_activity: false,
            })
        }
    }
}

pub fn try_lock_connection_with_activity(
    connection: &SharedConnection,
    activity: impl Into<String>,
) -> Option<ConnectionLockGuard<'_>> {
    match connection.try_lock() {
        Ok(guard) => {
            set_current_db_activity(Some(activity.into()));
            Some(ConnectionLockGuard {
                guard,
                tracks_activity: true,
            })
        }
        Err(std::sync::TryLockError::WouldBlock) => None,
        Err(std::sync::TryLockError::Poisoned(poisoned)) => {
            eprintln!("Warning: database connection lock was poisoned; recovering.");
            set_current_db_activity(Some(activity.into()));
            Some(ConnectionLockGuard {
                guard: poisoned.into_inner(),
                tracks_activity: true,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn require_live_connection_returns_default_message_when_never_connected() {
        let mut conn = DatabaseConnection::new();
        let err = conn
            .require_live_connection()
            .expect_err("must be disconnected");
        assert_eq!(err, NOT_CONNECTED_MESSAGE);
    }

    #[test]
    fn require_live_connection_returns_unexpected_disconnect_reason() {
        let mut conn = DatabaseConnection::new();
        let reason = "Connection was lost unexpectedly: ORA-03113".to_string();
        conn.mark_disconnected_with_reason(reason.clone());
        let err = conn
            .require_live_connection()
            .expect_err("must be disconnected");
        assert_eq!(err, reason);
    }

    #[test]
    fn manual_disconnect_clears_unexpected_disconnect_reason() {
        let mut conn = DatabaseConnection::new();
        conn.mark_disconnected_with_reason("Connection was lost unexpectedly: ORA-00028");
        conn.disconnect();
        let err = conn
            .require_live_connection()
            .expect_err("must be disconnected");
        assert_eq!(err, NOT_CONNECTED_MESSAGE);
    }
}
