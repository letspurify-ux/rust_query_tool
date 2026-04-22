use mysql::prelude::*;
use oracle::{Connection, Error as OracleError, ErrorKind as OracleErrorKind, InitParams};
use serde::{Deserialize, Serialize};
use std::env;
use std::fmt;
use std::fs;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use crate::db::session::SessionState;
use crate::utils::config::{
    DEFAULT_CONNECTION_POOL_SIZE, MAX_CONNECTION_POOL_SIZE, MIN_CONNECTION_POOL_SIZE,
};
use crate::utils::logging;

pub const NOT_CONNECTED_MESSAGE: &str = "Not connected to database";
const ORACLE_CLIENT_LOAD_HELP_URL: &str =
    "https://oracle.github.io/odpi/doc/installation.html#macos";
const ORACLE_CLIENT_LIB_ENV_VAR: &str = "ORACLE_CLIENT_LIB_DIR";
const MYSQL_POOL_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatabaseType {
    #[default]
    Oracle,
    MySQL,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DbConnectionFormSpec {
    pub service_name_form_label: &'static str,
    pub service_name_value_label: &'static str,
    pub service_name_required: bool,
    pub default_host: &'static str,
    pub default_port: u16,
    pub default_service_name: &'static str,
    pub supports_tns_alias: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DbExecutionEngine {
    Oracle,
    MySql,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DbSqlDialect {
    Oracle,
    MySql,
}

impl DatabaseType {
    pub const ALL: [Self; 2] = [Self::Oracle, Self::MySQL];

    pub fn supported() -> &'static [Self] {
        &Self::ALL
    }

    pub fn choice_label(self) -> &'static str {
        backend_for(self).choice_label()
    }

    pub fn connection_form_spec(self) -> DbConnectionFormSpec {
        backend_for(self).connection_form_spec()
    }

    pub fn supports_tns_alias(self) -> bool {
        self.connection_form_spec().supports_tns_alias
    }

    pub fn execution_engine(self) -> DbExecutionEngine {
        backend_for(self).execution_engine()
    }

    pub fn sql_dialect(self) -> DbSqlDialect {
        backend_for(self).sql_dialect()
    }

    pub fn uses_mysql_sql_dialect(self) -> bool {
        self.sql_dialect() == DbSqlDialect::MySql
    }

    pub fn uses_oracle_sql_dialect(self) -> bool {
        self.sql_dialect() == DbSqlDialect::Oracle
    }

    pub fn cache_key(self) -> u8 {
        backend_for(self).cache_key()
    }

    pub fn from_cache_key(raw: u8) -> Self {
        Self::supported()
            .iter()
            .copied()
            .find(|db_type| db_type.cache_key() == raw)
            .unwrap_or_default()
    }
}

impl fmt::Display for DatabaseType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", backend_for(*self).display_name())
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
    pub fn uses_oracle_tns_alias(&self) -> bool {
        self.db_type.supports_tns_alias()
            && self.host.trim().is_empty()
            && !self.service_name.trim().is_empty()
    }

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
        backend_for(self.db_type).connection_string(self)
    }

    pub fn default_for(db_type: DatabaseType) -> Self {
        backend_for(db_type).default_connection_info()
    }

    /// The label used for the service_name field depending on database type.
    pub fn service_name_label(&self) -> &'static str {
        backend_for(self.db_type).service_name_label()
    }

    /// Securely clear the password from memory by overwriting with zeros
    /// then releasing the allocation.
    pub fn clear_password(&mut self) {
        Self::clear_secret(&mut self.password);
    }
}

impl Default for ConnectionInfo {
    fn default() -> Self {
        Self::default_for(DatabaseType::Oracle)
    }
}

pub enum DbConnection {
    Oracle(Arc<Connection>),
    MySQL(mysql::Conn),
}

#[derive(Clone)]
pub enum DbConnectionPool {
    Oracle(oracle::pool::Pool),
    MySQL(mysql::Pool),
}

pub enum DbPoolSession {
    Oracle(Connection),
    MySQL(mysql::PooledConn),
}

pub enum DbSessionLease {
    Oracle(Arc<Connection>),
    MySQL(mysql::PooledConn),
}

pub type SharedDbSessionLease = Arc<Mutex<Option<(u64, DbSessionLease)>>>;

#[derive(Clone)]
pub struct DbPoolSessionContext {
    pub connection_generation: u64,
    pub connection_info: ConnectionInfo,
    pub pool: DbConnectionPool,
    pub current_service_name: String,
}

impl DbConnectionPool {
    pub fn acquire_session(&self) -> Result<DbPoolSession, String> {
        let mut session = match self {
            DbConnectionPool::Oracle(pool) => DbPoolSession::Oracle(
                pool.get()
                    .map_err(|err| Self::format_oracle_pool_acquire_error(pool, &err))?,
            ),
            DbConnectionPool::MySQL(pool) => DbPoolSession::MySQL(
                pool.try_get_conn(MYSQL_POOL_ACQUIRE_TIMEOUT)
                    .map_err(|err| Self::format_mysql_pool_acquire_error(&err))?,
            ),
        };
        backend_for(session.db_type()).apply_pool_session_defaults(&mut session);
        Ok(session)
    }

    fn format_oracle_pool_acquire_error(pool: &oracle::pool::Pool, err: &OracleError) -> String {
        let message = err.to_string();
        let lower = message.to_ascii_lowercase();
        let looks_pool_exhausted = lower.contains("ora-24418")
            || lower.contains("ora-24496")
            || lower.contains("ocisessionget timed out")
            || lower.contains("waiting for pool")
            || lower.contains("connection pool");
        if !looks_pool_exhausted {
            return message;
        }

        let pool_counts = match (pool.busy_count(), pool.open_count()) {
            (Ok(busy), Ok(open)) => format!(" busy/open sessions: {busy}/{open}."),
            _ => String::new(),
        };

        format!(
            "{}. Oracle session pool appears exhausted.{} Finish or cancel lazy fetches in other result tabs, close unused query tabs, or increase Settings > Connection pool size.",
            message, pool_counts
        )
    }

    fn format_mysql_pool_acquire_error(err: &mysql::Error) -> String {
        let message = err.to_string();
        let lower = message.to_ascii_lowercase();
        let looks_pool_exhausted = lower.contains("operation timed out");
        if !looks_pool_exhausted {
            return message;
        }

        format!(
            "{}. MySQL connection pool appears exhausted. Finish or cancel lazy fetches in other result tabs, close unused query tabs, or increase Settings > Connection pool size.",
            message
        )
    }
}

impl DbPoolSession {
    pub fn db_type(&self) -> DatabaseType {
        match self {
            DbPoolSession::Oracle(_) => DatabaseType::Oracle,
            DbPoolSession::MySQL(_) => DatabaseType::MySQL,
        }
    }

    pub fn into_lease(self) -> DbSessionLease {
        match self {
            DbPoolSession::Oracle(conn) => DbSessionLease::Oracle(Arc::new(conn)),
            DbPoolSession::MySQL(conn) => DbSessionLease::MySQL(conn),
        }
    }
}

impl DbSessionLease {
    pub fn db_type(&self) -> DatabaseType {
        match self {
            DbSessionLease::Oracle(_) => DatabaseType::Oracle,
            DbSessionLease::MySQL(_) => DatabaseType::MySQL,
        }
    }

    pub fn oracle_connection(&self) -> Option<Arc<Connection>> {
        match self {
            DbSessionLease::Oracle(conn) => Some(Arc::clone(conn)),
            DbSessionLease::MySQL(_) => None,
        }
    }

    pub fn into_mysql_connection(self) -> Option<mysql::PooledConn> {
        match self {
            DbSessionLease::MySQL(conn) => Some(conn),
            DbSessionLease::Oracle(_) => None,
        }
    }
}

pub fn create_shared_db_session_lease() -> SharedDbSessionLease {
    Arc::new(Mutex::new(None))
}

pub fn clear_pooled_session_lease(pooled_db_session: &SharedDbSessionLease) -> bool {
    let lease_to_drop = {
        pooled_db_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    };
    lease_to_drop.is_some()
}

pub fn clear_pooled_session_lease_if_current(
    pooled_db_session: &SharedDbSessionLease,
    connection_generation: u64,
    db_type: DatabaseType,
) -> bool {
    let lease_to_drop = {
        let mut lease = pooled_db_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let should_clear = lease.as_ref().is_some_and(|(lease_generation, existing)| {
            *lease_generation == connection_generation && existing.db_type() == db_type
        });
        if should_clear {
            lease.take()
        } else {
            None
        }
    };
    lease_to_drop.is_some()
}

pub fn current_oracle_pooled_session_lease(
    pooled_db_session: &SharedDbSessionLease,
    connection_generation: u64,
) -> Option<Arc<Connection>> {
    let mut lease_to_drop = None;
    let conn = {
        let mut lease = pooled_db_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match lease.as_ref() {
            Some((lease_generation, existing)) if *lease_generation == connection_generation => {
                if let Some(conn) = existing.oracle_connection() {
                    Some(conn)
                } else {
                    lease_to_drop = lease.take();
                    None
                }
            }
            Some(_) => {
                lease_to_drop = lease.take();
                None
            }
            None => None,
        }
    };
    drop(lease_to_drop);
    conn
}

pub fn take_reusable_pooled_session_lease(
    pooled_db_session: &SharedDbSessionLease,
    connection_generation: u64,
    db_type: DatabaseType,
) -> Option<DbSessionLease> {
    let mut stale_lease_to_drop = None;
    let reusable_lease = {
        let mut lease = pooled_db_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let reusable = lease.as_ref().is_some_and(|(generation, existing)| {
            *generation == connection_generation && existing.db_type() == db_type
        });
        if reusable {
            lease.take().map(|(_, lease)| lease)
        } else {
            if lease.is_some() {
                stale_lease_to_drop = lease.take();
            }
            None
        }
    };
    drop(stale_lease_to_drop);
    reusable_lease
}

pub fn store_pooled_session_lease_if_empty(
    pooled_db_session: &SharedDbSessionLease,
    connection_generation: u64,
    lease_to_store: DbSessionLease,
) -> bool {
    let lease_db_type = lease_to_store.db_type();
    let mut lease_to_store = Some(lease_to_store);
    let old_lease_to_drop = {
        let mut lease = pooled_db_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let should_store = match lease.as_ref() {
            None => true,
            Some((existing_generation, existing)) => {
                *existing_generation != connection_generation || existing.db_type() != lease_db_type
            }
        };
        if should_store {
            let old_lease = lease.take();
            if let Some(lease_to_store) = lease_to_store.take() {
                *lease = Some((connection_generation, lease_to_store));
            }
            old_lease
        } else {
            None
        }
    };
    drop(old_lease_to_drop);
    lease_to_store.is_none()
}

pub(crate) trait DbBackend: Sync {
    fn display_name(&self) -> &'static str;
    fn choice_label(&self) -> &'static str {
        self.display_name()
    }
    fn connection_form_spec(&self) -> DbConnectionFormSpec;
    fn execution_engine(&self) -> DbExecutionEngine;
    fn sql_dialect(&self) -> DbSqlDialect;
    fn cache_key(&self) -> u8;
    fn default_connection_info(&self) -> ConnectionInfo;
    fn connection_string(&self, info: &ConnectionInfo) -> String;
    fn service_name_label(&self) -> &'static str;
    fn connect(
        &self,
        info: &ConnectionInfo,
        pool_size: u32,
        auto_commit: bool,
    ) -> Result<(DbConnection, DbConnectionPool), String>;
    fn test_connection(&self, info: &ConnectionInfo) -> Result<(), String>;
    fn after_connect(&self, _connection: &mut DatabaseConnection) {}
    fn apply_auto_commit(&self, _connection: &mut DbConnection, _enabled: bool) {}
    fn apply_pool_session_defaults(&self, _session: &mut DbPoolSession) {}
}

struct OracleBackend;
struct MysqlBackend;

static ORACLE_BACKEND: OracleBackend = OracleBackend;
static MYSQL_BACKEND: MysqlBackend = MysqlBackend;

pub(crate) fn backend_for(db_type: DatabaseType) -> &'static dyn DbBackend {
    match db_type {
        DatabaseType::Oracle => &ORACLE_BACKEND,
        DatabaseType::MySQL => &MYSQL_BACKEND,
    }
}

impl DbBackend for OracleBackend {
    fn display_name(&self) -> &'static str {
        "Oracle"
    }

    fn connection_form_spec(&self) -> DbConnectionFormSpec {
        DbConnectionFormSpec {
            service_name_form_label: "Service:",
            service_name_value_label: "Service name",
            service_name_required: true,
            default_host: "localhost",
            default_port: 1521,
            default_service_name: "ORCL",
            supports_tns_alias: true,
        }
    }

    fn execution_engine(&self) -> DbExecutionEngine {
        DbExecutionEngine::Oracle
    }

    fn sql_dialect(&self) -> DbSqlDialect {
        DbSqlDialect::Oracle
    }

    fn cache_key(&self) -> u8 {
        0
    }

    fn default_connection_info(&self) -> ConnectionInfo {
        let form = self.connection_form_spec();
        ConnectionInfo {
            name: String::new(),
            username: String::new(),
            password: String::new(),
            host: form.default_host.to_string(),
            port: form.default_port,
            service_name: form.default_service_name.to_string(),
            db_type: DatabaseType::Oracle,
        }
    }

    fn connection_string(&self, info: &ConnectionInfo) -> String {
        if info.uses_oracle_tns_alias() {
            info.service_name.trim().to_string()
        } else {
            format!("//{}:{}/{}", info.host, info.port, info.service_name)
        }
    }

    fn service_name_label(&self) -> &'static str {
        "Service Name"
    }

    fn connect(
        &self,
        info: &ConnectionInfo,
        pool_size: u32,
        _auto_commit: bool,
    ) -> Result<(DbConnection, DbConnectionPool), String> {
        ensure_oracle_client_initialized().map_err(|e| e.to_string())?;
        let conn_str = info.connection_string();
        let connection = Arc::new(
            Connection::connect(&info.username, &info.password, &conn_str).map_err(|err| {
                eprintln!("Connection error: {err}");
                err.to_string()
            })?,
        );
        DatabaseConnection::apply_oracle_default_session_settings(connection.as_ref());
        let pool = DatabaseConnection::build_oracle_pool(info, pool_size)?;
        Ok((
            DbConnection::Oracle(connection),
            DbConnectionPool::Oracle(pool),
        ))
    }

    fn test_connection(&self, info: &ConnectionInfo) -> Result<(), String> {
        ensure_oracle_client_initialized().map_err(|e| e.to_string())?;
        let conn_str = info.connection_string();
        Connection::connect(&info.username, &info.password, &conn_str).map_err(|err| {
            eprintln!("Connection error: {err}");
            err.to_string()
        })?;
        Ok(())
    }

    fn apply_pool_session_defaults(&self, session: &mut DbPoolSession) {
        if let DbPoolSession::Oracle(conn) = session {
            DatabaseConnection::apply_oracle_default_session_settings(conn);
        }
    }
}

impl DbBackend for MysqlBackend {
    fn display_name(&self) -> &'static str {
        "MySQL"
    }

    fn choice_label(&self) -> &'static str {
        "MySQL or MariaDB"
    }

    fn connection_form_spec(&self) -> DbConnectionFormSpec {
        DbConnectionFormSpec {
            service_name_form_label: "Database:",
            service_name_value_label: "Database name",
            service_name_required: false,
            default_host: "localhost",
            default_port: 3306,
            default_service_name: "",
            supports_tns_alias: false,
        }
    }

    fn execution_engine(&self) -> DbExecutionEngine {
        DbExecutionEngine::MySql
    }

    fn sql_dialect(&self) -> DbSqlDialect {
        DbSqlDialect::MySql
    }

    fn cache_key(&self) -> u8 {
        1
    }

    fn default_connection_info(&self) -> ConnectionInfo {
        let form = self.connection_form_spec();
        ConnectionInfo {
            name: String::new(),
            username: String::new(),
            password: String::new(),
            host: form.default_host.to_string(),
            port: form.default_port,
            service_name: form.default_service_name.to_string(),
            db_type: DatabaseType::MySQL,
        }
    }

    fn connection_string(&self, info: &ConnectionInfo) -> String {
        let database = info.service_name.trim();
        if database.is_empty() {
            format!("mysql://{}:{}", info.host, info.port)
        } else {
            format!("mysql://{}:{}/{}", info.host, info.port, database)
        }
    }

    fn service_name_label(&self) -> &'static str {
        "Database"
    }

    fn connect(
        &self,
        info: &ConnectionInfo,
        pool_size: u32,
        auto_commit: bool,
    ) -> Result<(DbConnection, DbConnectionPool), String> {
        let opts = DatabaseConnection::build_mysql_opts(info);
        let mut conn = mysql::Conn::new(opts).map_err(|err| {
            eprintln!("MySQL connection error: {err}");
            err.to_string()
        })?;
        DatabaseConnection::apply_mysql_default_session_settings(&mut conn);
        DatabaseConnection::apply_mysql_autocommit_setting(&mut conn, auto_commit);
        let pool = DatabaseConnection::build_mysql_pool(info, pool_size)?;
        Ok((DbConnection::MySQL(conn), DbConnectionPool::MySQL(pool)))
    }

    fn test_connection(&self, info: &ConnectionInfo) -> Result<(), String> {
        let opts = DatabaseConnection::build_mysql_opts(info);
        mysql::Conn::new(opts).map_err(|err| {
            eprintln!("MySQL connection error: {err}");
            err.to_string()
        })?;
        Ok(())
    }

    fn after_connect(&self, connection: &mut DatabaseConnection) {
        if let Err(err) = connection.sync_mysql_current_database_name() {
            eprintln!("Warning: failed to sync MySQL current database after connect: {err}");
        }
    }

    fn apply_auto_commit(&self, connection: &mut DbConnection, enabled: bool) {
        if let DbConnection::MySQL(conn) = connection {
            DatabaseConnection::apply_mysql_autocommit_setting(conn, enabled);
        }
    }

    fn apply_pool_session_defaults(&self, session: &mut DbPoolSession) {
        if let DbPoolSession::MySQL(conn) = session {
            DatabaseConnection::apply_mysql_default_session_settings(conn);
        }
    }
}

pub struct DatabaseConnection {
    connection: Option<DbConnection>,
    pool: Option<DbConnectionPool>,
    info: ConnectionInfo,
    session_password: String,
    connected: bool,
    auto_commit: bool,
    session: Arc<Mutex<SessionState>>,
    last_disconnect_reason: Option<String>,
    connection_generation: u64,
    connection_pool_size: u32,
}

impl DatabaseConnection {
    fn clamp_connection_pool_size(size: u32) -> u32 {
        size.clamp(MIN_CONNECTION_POOL_SIZE, MAX_CONNECTION_POOL_SIZE)
    }

    fn build_mysql_opts(info: &ConnectionInfo) -> mysql::OptsBuilder {
        Self::build_mysql_opts_with_pool_size(info, None)
    }

    fn build_mysql_opts_with_pool_size(
        info: &ConnectionInfo,
        pool_size: Option<u32>,
    ) -> mysql::OptsBuilder {
        let mut opts = mysql::OptsBuilder::new()
            .ip_or_hostname(Some(&info.host))
            .tcp_port(info.port)
            .user(Some(&info.username))
            .pass(Some(&info.password));

        let database = info.service_name.trim();
        if !database.is_empty() {
            opts = opts.db_name(Some(database));
        }

        if let Some(pool_size) = pool_size {
            let pool_size = Self::clamp_connection_pool_size(pool_size) as usize;
            if let Some(constraints) = mysql::PoolConstraints::new(0, pool_size) {
                opts = opts.pool_opts(Some(
                    mysql::PoolOpts::default().with_constraints(constraints),
                ));
            }
        }

        opts
    }

    fn build_oracle_pool(
        info: &ConnectionInfo,
        pool_size: u32,
    ) -> Result<oracle::pool::Pool, String> {
        let conn_str = info.connection_string();
        let pool_size = Self::clamp_connection_pool_size(pool_size);
        let mut builder =
            oracle::pool::PoolBuilder::new(info.username.clone(), info.password.clone(), conn_str);
        builder
            .min_connections(1)
            .max_connections(pool_size)
            .connection_increment(1);
        builder.build().map_err(|err| err.to_string())
    }

    fn build_mysql_pool(info: &ConnectionInfo, pool_size: u32) -> Result<mysql::Pool, String> {
        let opts = Self::build_mysql_opts_with_pool_size(info, Some(pool_size));
        mysql::Pool::new(opts).map_err(|err| err.to_string())
    }

    fn build_pool_for_info(
        info: &ConnectionInfo,
        pool_size: u32,
    ) -> Result<DbConnectionPool, String> {
        match info.db_type {
            DatabaseType::Oracle => {
                Self::build_oracle_pool(info, pool_size).map(DbConnectionPool::Oracle)
            }
            DatabaseType::MySQL => {
                Self::build_mysql_pool(info, pool_size).map(DbConnectionPool::MySQL)
            }
        }
    }

    pub fn new() -> Self {
        Self {
            connection: None,
            pool: None,
            info: ConnectionInfo::default(),
            session_password: String::new(),
            connected: false,
            auto_commit: false,
            session: Arc::new(Mutex::new(SessionState::default())),
            last_disconnect_reason: None,
            connection_generation: 0,
            connection_pool_size: DEFAULT_CONNECTION_POOL_SIZE,
        }
    }

    pub fn connect(&mut self, info: ConnectionInfo) -> Result<(), String> {
        let (db_conn, pool) = backend_for(info.db_type).connect(
            &info,
            self.connection_pool_size,
            self.auto_commit,
        )?;

        // Swap in the new connection only after a successful handshake.
        // This preserves the active session when users mistype credentials
        // during reconnect attempts.
        self.connection = Some(db_conn);
        self.pool = Some(pool);
        let db_type = info.db_type;
        let new_session_password = info.password.clone();
        ConnectionInfo::clear_secret(&mut self.session_password);
        self.session_password = new_session_password;
        self.info = info;
        backend_for(db_type).after_connect(self);
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

    pub(crate) fn apply_oracle_default_session_settings(conn: &Connection) {
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

    pub(crate) fn apply_mysql_default_session_settings<C: Queryable>(conn: &mut C) {
        let statements = [
            "SET SESSION sql_mode = 'TRADITIONAL'",
            "SET SESSION time_zone = '+00:00'",
        ];

        for statement in statements {
            if let Err(err) = conn.query_drop(statement) {
                eprintln!("Warning: failed to apply MySQL session setting `{statement}`: {err}");
            }
        }

        Self::apply_mysql_connection_encoding(conn);
    }

    pub(crate) fn apply_mysql_connection_encoding<C: Queryable>(conn: &mut C) {
        let database_collation = match conn.query_first::<String, _>("SELECT @@collation_database")
        {
            Ok(value) => value.map(|collation| collation.trim().to_string()),
            Err(err) => {
                eprintln!(
                    "Warning: failed to read MySQL database collation for session setup: {err}"
                );
                None
            }
        };
        let statement = Self::mysql_set_names_statement(database_collation.as_deref());

        if let Err(err) = conn.query_drop(statement.as_str()) {
            eprintln!("Warning: failed to apply MySQL session setting `{statement}`: {err}");
        }
    }

    fn mysql_set_names_statement(database_collation: Option<&str>) -> String {
        match database_collation.map(str::trim) {
            Some(collation)
                if !collation.is_empty()
                    && Self::mysql_collation_name_is_safe(collation)
                    && collation.starts_with("utf8mb4_") =>
            {
                format!("SET NAMES utf8mb4 COLLATE {collation}")
            }
            _ => "SET NAMES utf8mb4".to_string(),
        }
    }

    fn mysql_collation_name_is_safe(collation: &str) -> bool {
        collation
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    }

    fn apply_mysql_autocommit_setting<C: Queryable>(conn: &mut C, enabled: bool) {
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
        self.pool = None;
        self.connected = false;
        self.last_disconnect_reason = disconnect_reason;
        self.info.clear_password();
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

    pub fn runtime_connection_info_for(&self, db_type: DatabaseType) -> Option<ConnectionInfo> {
        if self.info.db_type != db_type || !self.connected || self.connection.is_none() {
            return None;
        }

        let mut info = self.info.clone();
        info.password = self.session_password.clone();
        Some(info)
    }

    pub fn mysql_runtime_connection_info(&self) -> Option<ConnectionInfo> {
        if !matches!(self.connection, Some(DbConnection::MySQL(_))) {
            return None;
        }
        self.runtime_connection_info_for(DatabaseType::MySQL)
    }

    pub fn pool_session_context_for(
        &self,
        db_type: DatabaseType,
    ) -> Result<DbPoolSessionContext, String> {
        if !self.can_reuse_pool_session(self.connection_generation, db_type) {
            return Err(NOT_CONNECTED_MESSAGE.to_string());
        }

        let pool = self
            .get_pool()
            .ok_or_else(|| format!("{} connection pool is not available", db_type))?;
        let mut connection_info = self.info.clone();
        connection_info.password = self.session_password.clone();

        Ok(DbPoolSessionContext {
            connection_generation: self.connection_generation,
            connection_info,
            pool,
            current_service_name: self.info.service_name.clone(),
        })
    }

    pub fn get_pool(&self) -> Option<DbConnectionPool> {
        self.pool.clone()
    }

    pub fn acquire_pool_session(&self) -> Result<Option<DbPoolSession>, String> {
        self.pool
            .as_ref()
            .map(DbConnectionPool::acquire_session)
            .transpose()
    }

    pub fn connection_pool_size(&self) -> u32 {
        self.connection_pool_size
    }

    pub fn set_connection_pool_size(&mut self, size: u32) {
        self.connection_pool_size = Self::clamp_connection_pool_size(size);
    }

    pub fn resize_current_connection_pool(&mut self, size: u32) -> Result<(), String> {
        let size = Self::clamp_connection_pool_size(size);
        if self.connection_pool_size == size {
            return Ok(());
        }

        if !self.connected || self.connection.is_none() {
            self.connection_pool_size = size;
            return Ok(());
        }

        let mut info = self.info.clone();
        info.password = self.session_password.clone();
        let pool = Self::build_pool_for_info(&info, size)?;
        self.pool = Some(pool);
        self.connection_pool_size = size;
        self.connection_generation = self.connection_generation.wrapping_add(1);
        Ok(())
    }

    pub fn connection_generation(&self) -> u64 {
        self.connection_generation
    }

    pub fn can_reuse_pool_session(
        &self,
        connection_generation: u64,
        db_type: DatabaseType,
    ) -> bool {
        self.info.db_type == db_type
            && self.connected
            && self.connection.is_some()
            && self.connection_generation == connection_generation
    }

    pub fn set_auto_commit(&mut self, enabled: bool) {
        self.auto_commit = enabled;
        let db_type = self.info.db_type;
        if let Some(connection) = self.connection.as_mut() {
            backend_for(db_type).apply_auto_commit(connection, enabled);
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
        Self::apply_mysql_connection_encoding(conn);
        self.info.service_name = current_database.clone();
        Ok(current_database)
    }

    pub fn sync_mysql_current_database_name_from_session<C: Queryable>(
        &mut self,
        conn: &mut C,
        refresh_encoding: bool,
    ) -> Result<String, String> {
        if self.info.db_type != DatabaseType::MySQL || !self.connected {
            return Err("Expected MySQL connection but none is active".to_string());
        }

        let current_database = conn
            .query_first::<Option<String>, _>("SELECT DATABASE()")
            .map_err(|err| err.to_string())?
            .flatten()
            .map(|database| database.trim().to_string())
            .unwrap_or_default();
        if refresh_encoding {
            Self::apply_mysql_connection_encoding(conn);
        }
        self.info.service_name = current_database.clone();
        Ok(current_database)
    }

    pub fn session_state(&self) -> Arc<Mutex<SessionState>> {
        Arc::clone(&self.session)
    }

    pub fn test_connection(info: &ConnectionInfo) -> Result<(), String> {
        backend_for(info.db_type).test_connection(info)
    }

    #[cfg(test)]
    fn simulate_connected_metadata_for_test(&mut self, info: ConnectionInfo) {
        self.connected = true;
        self.session_password = info.password.clone();
        self.info = info;
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
    fn connected_metadata_retains_password_until_disconnect() {
        let mut conn = DatabaseConnection::new();
        conn.simulate_connected_metadata_for_test(ConnectionInfo::new(
            "Prod", "scott", "pw", "db", 1521, "FREE",
        ));

        assert_eq!(conn.get_info().password, "pw");

        conn.disconnect();

        assert!(conn.get_info().password.is_empty());
        assert!(conn.session_password.is_empty());
    }

    #[test]
    fn connection_pool_size_defaults_and_clamps() {
        let mut conn = DatabaseConnection::new();

        assert_eq!(conn.connection_pool_size(), DEFAULT_CONNECTION_POOL_SIZE);

        conn.set_connection_pool_size(0);
        assert_eq!(conn.connection_pool_size(), MIN_CONNECTION_POOL_SIZE);

        conn.set_connection_pool_size(99);
        assert_eq!(conn.connection_pool_size(), MAX_CONNECTION_POOL_SIZE);
    }

    #[test]
    fn resize_disconnected_connection_pool_size_clamps_preference() {
        let mut conn = DatabaseConnection::new();

        conn.resize_current_connection_pool(0)
            .expect("disconnected resize should not require a live pool");
        assert_eq!(conn.connection_pool_size(), MIN_CONNECTION_POOL_SIZE);

        conn.resize_current_connection_pool(99)
            .expect("disconnected resize should not require a live pool");
        assert_eq!(conn.connection_pool_size(), MAX_CONNECTION_POOL_SIZE);
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
    fn oracle_connection_string_uses_tns_alias_when_host_is_empty() {
        let info = ConnectionInfo::new_with_type(
            "local",
            "system",
            "pw",
            "",
            0,
            "LOCAL_FREE",
            DatabaseType::Oracle,
        );

        assert_eq!(info.connection_string(), "LOCAL_FREE");
    }

    #[test]
    fn database_form_specs_keep_connection_defaults_in_backend_metadata() {
        let oracle = DatabaseType::Oracle.connection_form_spec();
        assert_eq!(oracle.default_port, 1521);
        assert!(oracle.service_name_required);
        assert!(oracle.supports_tns_alias);

        let mysql = DatabaseType::MySQL.connection_form_spec();
        assert_eq!(mysql.default_port, 3306);
        assert!(!mysql.service_name_required);
        assert!(!mysql.supports_tns_alias);
    }

    #[test]
    fn database_backend_metadata_covers_execution_dialect_and_cache_keys() {
        assert_eq!(
            DatabaseType::Oracle.execution_engine(),
            DbExecutionEngine::Oracle
        );
        assert_eq!(DatabaseType::Oracle.sql_dialect(), DbSqlDialect::Oracle);
        assert_eq!(
            DatabaseType::from_cache_key(DatabaseType::Oracle.cache_key()),
            DatabaseType::Oracle
        );

        assert_eq!(
            DatabaseType::MySQL.execution_engine(),
            DbExecutionEngine::MySql
        );
        assert_eq!(DatabaseType::MySQL.sql_dialect(), DbSqlDialect::MySql);
        assert_eq!(
            DatabaseType::from_cache_key(DatabaseType::MySQL.cache_key()),
            DatabaseType::MySQL
        );
    }

    #[test]
    fn mysql_set_names_statement_uses_utf8mb4_database_collation_when_available() {
        assert_eq!(
            DatabaseConnection::mysql_set_names_statement(Some("utf8mb4_unicode_ci")),
            "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"
        );
    }

    #[test]
    fn mysql_set_names_statement_falls_back_for_non_utf8mb4_database_collation() {
        assert_eq!(
            DatabaseConnection::mysql_set_names_statement(Some("latin1_swedish_ci")),
            "SET NAMES utf8mb4"
        );
    }

    #[test]
    fn mysql_set_names_statement_falls_back_for_unsafe_collation_name() {
        assert_eq!(
            DatabaseConnection::mysql_set_names_statement(Some("utf8mb4_unicode_ci;DROP")),
            "SET NAMES utf8mb4"
        );
    }

    #[test]
    #[ignore = "requires local Oracle XE plus TNS_ADMIN/ORACLE_TEST_* environment variables"]
    fn oracle_test_connection_supports_tns_alias_from_tns_admin() {
        let username =
            std::env::var("ORACLE_TEST_USERNAME").expect("ORACLE_TEST_USERNAME must be set");
        let password =
            std::env::var("ORACLE_TEST_PASSWORD").expect("ORACLE_TEST_PASSWORD must be set");
        let alias =
            std::env::var("ORACLE_TEST_TNS_ALIAS").expect("ORACLE_TEST_TNS_ALIAS must be set");

        let info = ConnectionInfo::new_with_type(
            "local",
            &username,
            &password,
            "",
            0,
            &alias,
            DatabaseType::Oracle,
        );

        DatabaseConnection::test_connection(&info)
            .expect("TNS alias connection should succeed against local Oracle XE");
    }

    #[test]
    #[ignore = "requires local Oracle XE plus ORACLE_TEST_* environment variables"]
    fn oracle_test_connection_supports_direct_local_xe() {
        let username =
            std::env::var("ORACLE_TEST_USERNAME").expect("ORACLE_TEST_USERNAME must be set");
        let password =
            std::env::var("ORACLE_TEST_PASSWORD").expect("ORACLE_TEST_PASSWORD must be set");
        let service_name = std::env::var("ORACLE_TEST_SERVICE_NAME")
            .expect("ORACLE_TEST_SERVICE_NAME must be set");
        let host = std::env::var("ORACLE_TEST_HOST").unwrap_or_else(|_| "localhost".to_string());
        let port = std::env::var("ORACLE_TEST_PORT")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(1521);

        let info = ConnectionInfo::new_with_type(
            "local",
            &username,
            &password,
            &host,
            port,
            &service_name,
            DatabaseType::Oracle,
        );

        DatabaseConnection::test_connection(&info)
            .expect("Direct localhost Oracle connection should succeed against local Oracle XE");
    }

    #[test]
    #[ignore = "requires local MariaDB test database via SPACE_QUERY_TEST_MYSQL_* env vars"]
    fn mysql_pool_session_applies_default_session_settings_from_local_mariadb() {
        let host = std::env::var("SPACE_QUERY_TEST_MYSQL_HOST")
            .expect("SPACE_QUERY_TEST_MYSQL_HOST must be set");
        let database = std::env::var("SPACE_QUERY_TEST_MYSQL_DATABASE")
            .expect("SPACE_QUERY_TEST_MYSQL_DATABASE must be set");
        let user = std::env::var("SPACE_QUERY_TEST_MYSQL_USER")
            .expect("SPACE_QUERY_TEST_MYSQL_USER must be set");
        let password = std::env::var("SPACE_QUERY_TEST_MYSQL_PASSWORD")
            .expect("SPACE_QUERY_TEST_MYSQL_PASSWORD must be set");
        let port = std::env::var("SPACE_QUERY_TEST_MYSQL_PORT")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(3306);

        let mut connection = DatabaseConnection::new();
        connection
            .connect(ConnectionInfo::new_with_type(
                "local",
                &user,
                &password,
                &host,
                port,
                &database,
                DatabaseType::MySQL,
            ))
            .expect("MariaDB connection should succeed");

        let Some(DbPoolSession::MySQL(mut conn)) = connection
            .acquire_pool_session()
            .expect("MySQL pool session should be acquired")
        else {
            panic!("expected MySQL pool session");
        };
        let sql_mode = conn
            .query_first::<String, _>("SELECT @@SESSION.sql_mode")
            .expect("read sql_mode")
            .unwrap_or_default();
        let time_zone = conn
            .query_first::<String, _>("SELECT @@SESSION.time_zone")
            .expect("read time_zone")
            .unwrap_or_default();
        let character_set_client = conn
            .query_first::<String, _>("SELECT @@SESSION.character_set_client")
            .expect("read character_set_client")
            .unwrap_or_default();

        assert!(sql_mode.contains("STRICT_TRANS_TABLES"));
        assert_eq!(time_zone, "+00:00");
        assert_eq!(character_set_client, "utf8mb4");
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
