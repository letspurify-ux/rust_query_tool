use mysql::prelude::*;
use oracle::{Connection, Error as OracleError, ErrorKind as OracleErrorKind, InitParams};
use serde::{Deserialize, Serialize};
use std::env;
use std::fmt;
use std::fs;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use crate::db::session::SessionState;
use crate::utils::logging;

pub const NOT_CONNECTED_MESSAGE: &str = "Not connected to database";
const ORACLE_CLIENT_LOAD_HELP_URL: &str =
    "https://oracle.github.io/odpi/doc/installation.html#macos";
const ORACLE_CLIENT_LIB_ENV_VAR: &str = "ORACLE_CLIENT_LIB_DIR";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatabaseType {
    #[default]
    Oracle,
    MySQL,
}

impl fmt::Display for DatabaseType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatabaseType::Oracle => write!(f, "Oracle"),
            DatabaseType::MySQL => write!(f, "MySQL"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionInfo {
    pub name: String,
    pub username: String,
    #[serde(skip_serializing, default)]
    pub password: String,
    pub host: String,
    pub port: u16,
    pub service_name: String,
    #[serde(default)]
    pub db_type: DatabaseType,
}

impl ConnectionInfo {
    pub(crate) fn clear_secret(secret: &mut String) {
        // Overwrite the secret bytes with zeros before releasing the allocation.
        // SAFETY: 0x00 bytes are valid UTF-8 code points, so the String's UTF-8
        // invariant is preserved during zeroing. We immediately clear and shrink the
        // Vec to release the underlying allocation that held the secret.
        let vec = unsafe { secret.as_mut_vec() };
        for b in vec.iter_mut() {
            // write_volatile prevents the compiler from optimizing away the zeroing.
            unsafe { std::ptr::write_volatile(b as *mut u8, 0) };
        }
        vec.clear();
        vec.shrink_to_fit();
    }

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
            db_type: DatabaseType::Oracle,
        }
    }

    pub fn new_with_type(
        name: &str,
        username: &str,
        password: &str,
        host: &str,
        port: u16,
        service_name: &str,
        db_type: DatabaseType,
    ) -> Self {
        Self {
            name: name.to_string(),
            username: username.to_string(),
            password: password.to_string(),
            host: host.to_string(),
            port,
            service_name: service_name.to_string(),
            db_type,
        }
    }

    pub fn connection_string(&self) -> String {
        match self.db_type {
            DatabaseType::Oracle => format!("//{}:{}/{}", self.host, self.port, self.service_name),
            DatabaseType::MySQL => {
                let database = self.service_name.trim();
                if database.is_empty() {
                    format!("mysql://{}:{}", self.host, self.port)
                } else {
                    format!("mysql://{}:{}/{}", self.host, self.port, database)
                }
            }
        }
    }

    pub fn default_for(db_type: DatabaseType) -> Self {
        match db_type {
            DatabaseType::Oracle => Self::default(),
            DatabaseType::MySQL => Self {
                name: String::new(),
                username: String::new(),
                password: String::new(),
                host: "localhost".to_string(),
                port: 3306,
                service_name: String::new(),
                db_type: DatabaseType::MySQL,
            },
        }
    }

    /// The label used for the service_name field depending on database type.
    pub fn service_name_label(&self) -> &'static str {
        match self.db_type {
            DatabaseType::Oracle => "Service Name",
            DatabaseType::MySQL => "Database",
        }
    }

    /// Securely clear the password from memory by overwriting with zeros
    /// then releasing the allocation.
    pub fn clear_password(&mut self) {
        Self::clear_secret(&mut self.password);
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
            db_type: DatabaseType::Oracle,
        }
    }
}

pub enum DbConnection {
    Oracle(Arc<Connection>),
    MySQL(mysql::Conn),
}

pub struct DatabaseConnection {
    connection: Option<DbConnection>,
    info: ConnectionInfo,
    session_password: String,
    connected: bool,
    auto_commit: bool,
    session: Arc<Mutex<SessionState>>,
    last_disconnect_reason: Option<String>,
    connection_generation: u64,
}

impl DatabaseConnection {
    fn build_mysql_opts(info: &ConnectionInfo) -> mysql::OptsBuilder {
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

    pub fn new() -> Self {
        Self {
            connection: None,
            info: ConnectionInfo::default(),
            session_password: String::new(),
            connected: false,
            auto_commit: false,
            session: Arc::new(Mutex::new(SessionState::default())),
            last_disconnect_reason: None,
            connection_generation: 0,
        }
    }

    pub fn connect(&mut self, info: ConnectionInfo) -> Result<(), String> {
        let db_conn = match info.db_type {
            DatabaseType::Oracle => {
                ensure_oracle_client_initialized().map_err(|e| e.to_string())?;
                let conn_str = info.connection_string();
                let connection = Arc::new(
                    Connection::connect(&info.username, &info.password, &conn_str).map_err(
                        |err| {
                            eprintln!("Connection error: {err}");
                            err.to_string()
                        },
                    )?,
                );
                Self::apply_oracle_default_session_settings(connection.as_ref());
                DbConnection::Oracle(connection)
            }
            DatabaseType::MySQL => {
                let opts = Self::build_mysql_opts(&info);
                let mut conn = mysql::Conn::new(opts).map_err(|err| {
                    eprintln!("MySQL connection error: {err}");
                    err.to_string()
                })?;
                Self::apply_mysql_default_session_settings(&mut conn);
                Self::apply_mysql_autocommit_setting(&mut conn, self.auto_commit);
                DbConnection::MySQL(conn)
            }
        };

        // Swap in the new connection only after a successful handshake.
        // This preserves the active session when users mistype credentials
        // during reconnect attempts.
        self.connection = Some(db_conn);
        let db_type = info.db_type;
        let new_session_password = if db_type == DatabaseType::MySQL {
            info.password.clone()
        } else {
            String::new()
        };
        ConnectionInfo::clear_secret(&mut self.session_password);
        self.session_password = new_session_password;
        self.info = info;
        // Clear password from memory now that the connection is established
        self.info.clear_password();
        if db_type == DatabaseType::MySQL {
            if let Err(err) = self.sync_mysql_current_database_name() {
                eprintln!("Warning: failed to sync MySQL current database after connect: {err}");
            }
        }
        self.connected = true;
        self.last_disconnect_reason = None;
        self.connection_generation = self.connection_generation.wrapping_add(1);

        // Update session state with the database type
        match self.session.lock() {
            Ok(mut guard) => guard.db_type = db_type,
            Err(poisoned) => poisoned.into_inner().db_type = db_type,
        }

        Ok(())
    }

    fn apply_oracle_default_session_settings(conn: &Connection) {
        let statements = [
            "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'yyyy-mm-dd hh24:mi:ss.ff6'",
            "ALTER SESSION SET NLS_DATE_FORMAT = 'yyyy-mm-dd hh24:mi:ss'",
        ];

        for statement in statements {
            if let Err(err) = conn.execute(statement, &[]) {
                eprintln!("Warning: failed to apply default session setting `{statement}`: {err}");
            }
        }
    }

    fn apply_mysql_default_session_settings(conn: &mut mysql::Conn) {
        let statements = [
            "SET SESSION sql_mode = 'TRADITIONAL'",
            "SET SESSION time_zone = '+00:00'",
            "SET NAMES utf8mb4",
        ];

        for statement in statements {
            if let Err(err) = conn.query_drop(statement) {
                eprintln!("Warning: failed to apply MySQL session setting `{statement}`: {err}");
            }
        }
    }

    fn apply_mysql_autocommit_setting(conn: &mut mysql::Conn, enabled: bool) {
        let statement = if enabled {
            "SET autocommit = 1"
        } else {
            "SET autocommit = 0"
        };

        if let Err(err) = conn.query_drop(statement) {
            eprintln!("Warning: failed to apply MySQL autocommit setting `{statement}`: {err}");
        }
    }

    pub fn disconnect(&mut self) {
        self.clear_connection_state(None);
    }

    fn clear_connection_state(&mut self, disconnect_reason: Option<String>) {
        let had_connection = self.connection.is_some() || self.connected;
        self.connection = None;
        self.connected = false;
        self.last_disconnect_reason = disconnect_reason;
        self.info = ConnectionInfo::default();
        ConnectionInfo::clear_secret(&mut self.session_password);
        self.auto_commit = false;
        match self.session.lock() {
            Ok(mut guard) => guard.reset(),
            Err(poisoned) => poisoned.into_inner().reset(),
        }
        if had_connection {
            self.connection_generation = self.connection_generation.wrapping_add(1);
        }
    }

    fn disconnect_message(&self) -> String {
        self.last_disconnect_reason
            .clone()
            .unwrap_or_else(|| NOT_CONNECTED_MESSAGE.to_string())
    }

    /// Returns the Oracle connection if connected to Oracle.
    /// For backward compatibility with existing Oracle-specific code paths.
    pub fn require_live_connection(&mut self) -> Result<Arc<Connection>, String> {
        let db_conn = self.require_live_db_connection()?;
        match db_conn {
            DbConnection::Oracle(conn) => Ok(conn),
            DbConnection::MySQL(_) => Err("Expected Oracle connection but found MySQL".to_string()),
        }
    }

    /// Returns the underlying DbConnection enum for dispatch-based code.
    pub fn require_live_db_connection(&mut self) -> Result<DbConnection, String> {
        if !self.connected {
            if self.connection.is_some() {
                self.clear_connection_state(Some(NOT_CONNECTED_MESSAGE.to_string()));
            }
            return Err(self.disconnect_message());
        }

        if self.connection.is_none() {
            self.connected = false;
            if self.last_disconnect_reason.is_none() {
                self.last_disconnect_reason = Some(NOT_CONNECTED_MESSAGE.to_string());
            }
            return Err(self.disconnect_message());
        }

        self.get_db_connection()
            .ok_or_else(|| self.disconnect_message())
    }

    pub fn is_connected(&self) -> bool {
        self.connected
    }

    pub fn has_connection_handle(&self) -> bool {
        self.connection.is_some()
    }

    /// Returns the Oracle connection (backward compat).
    pub fn get_connection(&self) -> Option<Arc<Connection>> {
        match &self.connection {
            Some(DbConnection::Oracle(conn)) => Some(Arc::clone(conn)),
            _ => None,
        }
    }

    /// Returns the DbConnection enum clone.
    pub fn get_db_connection(&self) -> Option<DbConnection> {
        match &self.connection {
            Some(DbConnection::Oracle(conn)) => Some(DbConnection::Oracle(Arc::clone(conn))),
            Some(DbConnection::MySQL(_)) => {
                // MySQL connections are not Arc-wrapped; return None here.
                // Use get_mysql_connection_mut() via mutable access instead.
                None
            }
            None => None,
        }
    }

    /// Returns a mutable reference to the MySQL connection, if connected to MySQL.
    pub fn get_mysql_connection_mut(&mut self) -> Option<&mut mysql::Conn> {
        match &mut self.connection {
            Some(DbConnection::MySQL(conn)) => Some(conn),
            _ => None,
        }
    }

    pub fn db_type(&self) -> DatabaseType {
        self.info.db_type
    }

    pub fn get_info(&self) -> &ConnectionInfo {
        &self.info
    }

    pub fn mysql_runtime_connection_info(&self) -> Option<ConnectionInfo> {
        if self.info.db_type != DatabaseType::MySQL
            || !self.connected
            || !matches!(self.connection, Some(DbConnection::MySQL(_)))
        {
            return None;
        }

        let mut info = self.info.clone();
        info.password = self.session_password.clone();
        Some(info)
    }

    pub fn connection_generation(&self) -> u64 {
        self.connection_generation
    }

    pub fn set_auto_commit(&mut self, enabled: bool) {
        self.auto_commit = enabled;
        if let Some(DbConnection::MySQL(conn)) = self.connection.as_mut() {
            Self::apply_mysql_autocommit_setting(conn, enabled);
        }
    }

    pub fn auto_commit(&self) -> bool {
        self.auto_commit
    }

    pub fn sync_mysql_current_database_name(&mut self) -> Result<String, String> {
        let Some(conn) = self.get_mysql_connection_mut() else {
            return Err("Expected MySQL connection but none is active".to_string());
        };

        let current_database = conn
            .query_first::<Option<String>, _>("SELECT DATABASE()")
            .map_err(|err| err.to_string())?
            .flatten()
            .map(|database| database.trim().to_string())
            .unwrap_or_default();
        self.info.service_name = current_database.clone();
        Ok(current_database)
    }

    pub fn session_state(&self) -> Arc<Mutex<SessionState>> {
        Arc::clone(&self.session)
    }

    pub fn test_connection(info: &ConnectionInfo) -> Result<(), String> {
        match info.db_type {
            DatabaseType::Oracle => {
                ensure_oracle_client_initialized().map_err(|e| e.to_string())?;
                let conn_str = info.connection_string();
                Connection::connect(&info.username, &info.password, &conn_str).map_err(|err| {
                    eprintln!("Connection error: {err}");
                    err.to_string()
                })?;
                Ok(())
            }
            DatabaseType::MySQL => {
                let opts = Self::build_mysql_opts(info);
                mysql::Conn::new(opts).map_err(|err| {
                    eprintln!("MySQL connection error: {err}");
                    err.to_string()
                })?;
                Ok(())
            }
        }
    }
}

impl Default for DatabaseConnection {
    fn default() -> Self {
        Self::new()
    }
}

pub type SharedConnection = Arc<Mutex<DatabaseConnection>>;

static ACTIVE_DB_ACTIVITY: OnceLock<Mutex<Option<String>>> = OnceLock::new();
static ORACLE_CLIENT_INIT_SUCCESS: OnceLock<()> = OnceLock::new();
static ORACLE_CLIENT_INIT_ATTEMPT_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn ensure_oracle_client_initialized() -> Result<(), OracleError> {
    if ORACLE_CLIENT_INIT_SUCCESS.get().is_some() {
        return Ok(());
    }

    let attempt_lock = ORACLE_CLIENT_INIT_ATTEMPT_LOCK.get_or_init(|| Mutex::new(()));
    let _attempt_guard = match attempt_lock.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            logging::log_warning(
                "db::connection",
                "oracle init lock was poisoned; recovering",
            );
            poisoned.into_inner()
        }
    };

    if ORACLE_CLIENT_INIT_SUCCESS.get().is_some() {
        return Ok(());
    }

    match init_oracle_client() {
        Ok(_) => {
            ORACLE_CLIENT_INIT_SUCCESS.get_or_init(|| ());
            Ok(())
        }
        Err(err) => Err(OracleError::new(
            OracleErrorKind::InternalError,
            format_oracle_client_init_error(&err),
        )),
    }
}

fn init_oracle_client() -> Result<(), OracleError> {
    let candidate_dirs = oracle_client_lib_dir_candidates();
    let mut last_error: Option<OracleError> = None;

    for dir in candidate_dirs {
        if !dir.join("libclntsh.dylib").is_file() {
            continue;
        }

        let mut params = InitParams::new();
        params.load_error_url(ORACLE_CLIENT_LOAD_HELP_URL)?;
        params.oracle_client_lib_dir(&dir)?;

        match params.init() {
            Ok(_) => return Ok(()),
            Err(err) => last_error = Some(err),
        }
    }

    if let Some(err) = last_error {
        return Err(err);
    }

    let mut params = InitParams::new();
    params.load_error_url(ORACLE_CLIENT_LOAD_HELP_URL)?;
    params.init().map(|_| ())
}

fn oracle_client_lib_dir_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    if let Some(env_dir) = env::var_os(ORACLE_CLIENT_LIB_ENV_VAR) {
        push_oracle_client_dir_candidate(&mut candidates, PathBuf::from(env_dir));
    }

    for root in oracle_client_search_roots() {
        for dir in collect_instantclient_dirs(&root) {
            push_oracle_client_dir_candidate(&mut candidates, dir);
        }
    }

    candidates
}

fn oracle_client_search_roots() -> Vec<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        let mut roots = vec![PathBuf::from("/opt/oracle")];
        if let Some(home) = env::var_os("HOME") {
            roots.push(PathBuf::from(home).join("Downloads"));
        }
        roots
    }

    #[cfg(not(target_os = "macos"))]
    {
        Vec::new()
    }
}

fn collect_instantclient_dirs(root: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(root) else {
        return Vec::new();
    };

    let mut dirs = Vec::new();
    for entry_result in entries {
        let Ok(entry) = entry_result else {
            continue;
        };
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.starts_with("instantclient_") {
            dirs.push(path);
        }
    }

    dirs.sort_unstable_by(|left, right| right.as_os_str().cmp(left.as_os_str()));
    dirs
}

fn push_oracle_client_dir_candidate(candidates: &mut Vec<PathBuf>, dir: PathBuf) {
    if candidates.iter().any(|existing| existing == &dir) {
        return;
    }
    candidates.push(dir);
}

fn format_oracle_client_init_error(err: &OracleError) -> String {
    let err_text = err.to_string();
    let mut message = format!("Failed to initialize Oracle client library: {err_text}");

    if is_oracle_client_architecture_mismatch(&err_text) {
        message.push_str(
            " Detected an Oracle Client CPU architecture mismatch. Install an Oracle Instant Client that matches this app's architecture. On Apple Silicon, use an arm64 client and set ORACLE_CLIENT_LIB_DIR if you need to override auto-detection.",
        );
    } else if err_text.contains("DPI-1047") {
        message.push_str(
            " Set ORACLE_CLIENT_LIB_DIR to the directory containing libclntsh.dylib if the client is installed in a non-default location.",
        );
    }

    message
}

fn is_oracle_client_architecture_mismatch(err_text: &str) -> bool {
    err_text.contains("incompatible architecture")
        || (err_text.contains("have 'x86_64'") && err_text.contains("need 'arm64"))
}

fn db_activity_slot() -> &'static Mutex<Option<String>> {
    ACTIVE_DB_ACTIVITY.get_or_init(|| Mutex::new(None))
}

fn set_current_db_activity(activity: Option<String>) {
    match db_activity_slot().lock() {
        Ok(mut guard) => {
            *guard = activity;
        }
        Err(poisoned) => {
            logging::log_warning(
                "db::connection",
                "DB activity lock was poisoned; recovering",
            );
            *poisoned.into_inner() = activity;
        }
    }
}

pub fn current_db_activity() -> Option<String> {
    match db_activity_slot().lock() {
        Ok(guard) => guard.clone(),
        Err(poisoned) => {
            logging::log_warning(
                "db::connection",
                "DB activity lock was poisoned; recovering",
            );
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

pub fn clear_tracked_db_activity() {
    set_current_db_activity(None);
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

    pub fn refresh_tracked_connection(&self) {}
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
            logging::log_warning(
                "db::connection",
                "database connection lock was poisoned; recovering",
            );
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
            logging::log_warning(
                "db::connection",
                "database connection lock was poisoned; recovering",
            );
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
            logging::log_warning(
                "db::connection",
                "database connection lock was poisoned; recovering",
            );
            let guard = poisoned.into_inner();
            set_current_db_activity(Some(activity.into()));
            Some(ConnectionLockGuard {
                guard,
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
    fn disconnect_resets_connection_metadata_and_auto_commit() {
        let mut conn = DatabaseConnection::new();
        conn.info = ConnectionInfo::new("Prod", "scott", "pw", "db", 1521, "FREE");
        conn.connected = true;
        conn.auto_commit = true;
        conn.disconnect();

        assert!(!conn.connected);
        assert!(!conn.auto_commit);
        assert!(conn.info.name.is_empty());
        assert!(conn.info.username.is_empty());
        assert_eq!(conn.info.host, "localhost");
    }

    #[test]
    fn disconnect_resets_session_state() {
        let mut conn = DatabaseConnection::new();
        conn.connected = true;
        if let Ok(mut session) = conn.session.lock() {
            session.continue_on_error = true;
            session.colsep = ",".to_string();
        }

        conn.disconnect();

        let (continue_on_error, colsep) = match conn.session.lock() {
            Ok(guard) => (guard.continue_on_error, guard.colsep.clone()),
            Err(poisoned) => {
                let guard = poisoned.into_inner();
                (guard.continue_on_error, guard.colsep.clone())
            }
        };
        assert!(!continue_on_error);
        assert_eq!(colsep, " | ");
    }

    #[test]
    fn mysql_connection_string_omits_database_segment_when_empty() {
        let info = ConnectionInfo::new_with_type(
            "local",
            "root",
            "pw",
            "localhost",
            3306,
            "",
            DatabaseType::MySQL,
        );

        assert_eq!(info.connection_string(), "mysql://localhost:3306");
    }

    #[test]
    fn architecture_mismatch_detection_identifies_x86_client_on_arm_runtime() {
        let err = "DPI-1047: Cannot locate a 64-bit Oracle Client library: \"dlopen(libclntsh.dylib, 0x0001): tried: '/opt/homebrew/libclntsh.dylib' (mach-o file, but is an incompatible architecture (have 'x86_64', need 'arm64'))\"";
        assert!(is_oracle_client_architecture_mismatch(err));
    }

    #[test]
    fn formatted_init_error_adds_actionable_architecture_hint() {
        let err = OracleError::new(
            OracleErrorKind::InternalError,
            "DPI-1047: incompatible architecture (have 'x86_64', need 'arm64')".to_string(),
        );
        let message = format_oracle_client_init_error(&err);
        assert!(message.contains("CPU architecture mismatch"));
        assert!(message.contains("ORACLE_CLIENT_LIB_DIR"));
    }
}
