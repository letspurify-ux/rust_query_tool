use mysql::prelude::*;
use oracle::{
    Connection, Connector, Error as OracleError, ErrorKind as OracleErrorKind, InitParams,
};
use serde::{Deserialize, Serialize};
use std::env;
use std::fmt;
use std::fs;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use crate::db::session::SessionState;
use crate::db::transaction::{
    TransactionAccessMode, TransactionIsolation, TransactionMode, TransactionSessionState,
};
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionSslMode {
    #[default]
    Disabled,
    Required,
    VerifyCa,
    VerifyIdentity,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum OracleNetworkProtocol {
    #[default]
    Tcp,
    Tcps,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ConnectionAdvancedSettings {
    #[serde(default)]
    pub ssl_mode: ConnectionSslMode,
    #[serde(default = "ConnectionAdvancedSettings::default_transaction_isolation")]
    pub default_transaction_isolation: TransactionIsolation,
    #[serde(default)]
    pub default_transaction_access_mode: TransactionAccessMode,
    #[serde(default)]
    pub session_time_zone: String,
    #[serde(default = "ConnectionAdvancedSettings::default_mysql_sql_mode")]
    pub mysql_sql_mode: String,
    #[serde(default = "ConnectionAdvancedSettings::default_mysql_charset")]
    pub mysql_charset: String,
    #[serde(default)]
    pub mysql_collation: String,
    #[serde(default)]
    pub mysql_ssl_ca_path: String,
    #[serde(default)]
    pub oracle_protocol: OracleNetworkProtocol,
    #[serde(default = "ConnectionAdvancedSettings::default_oracle_nls_date_format")]
    pub oracle_nls_date_format: String,
    #[serde(default = "ConnectionAdvancedSettings::default_oracle_nls_timestamp_format")]
    pub oracle_nls_timestamp_format: String,
}

impl ConnectionSslMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Disabled => "Disabled",
            Self::Required => "Required",
            Self::VerifyCa => "Verify CA",
            Self::VerifyIdentity => "Verify identity",
        }
    }
}

impl OracleNetworkProtocol {
    pub fn label(self) -> &'static str {
        match self {
            Self::Tcp => "TCP",
            Self::Tcps => "TCPS",
        }
    }
}

impl ConnectionAdvancedSettings {
    fn default_transaction_isolation() -> TransactionIsolation {
        TransactionIsolation::ReadCommitted
    }

    fn default_mysql_sql_mode() -> String {
        "TRADITIONAL".to_string()
    }

    fn default_mysql_charset() -> String {
        "utf8mb4".to_string()
    }

    fn default_oracle_nls_date_format() -> String {
        "yyyy-mm-dd hh24:mi:ss".to_string()
    }

    fn default_oracle_nls_timestamp_format() -> String {
        "yyyy-mm-dd hh24:mi:ss.ff6".to_string()
    }

    pub fn default_for(db_type: DatabaseType) -> Self {
        let mut settings = Self::default();
        if db_type == DatabaseType::MySQL {
            settings.session_time_zone = "+00:00".to_string();
        }
        settings
    }

    /// Produce a settings value appropriate for `new_db_type` while keeping
    /// cross-database fields the user has already customized (isolation,
    /// access mode, SSL mode, time zone). DB-specific fields fall back to
    /// the defaults for `new_db_type` because the `self` value holds fields
    /// for the other backend.
    pub fn migrate_for_db_type(
        &self,
        previous_db_type: DatabaseType,
        new_db_type: DatabaseType,
    ) -> Self {
        if previous_db_type == new_db_type {
            return self.clone();
        }

        let mut settings = Self::default_for(new_db_type);
        let previous_defaults = Self::default_for(previous_db_type);

        if self.default_transaction_isolation != previous_defaults.default_transaction_isolation
            && new_db_type
                .supported_transaction_isolations()
                .contains(&self.default_transaction_isolation)
        {
            settings.default_transaction_isolation = self.default_transaction_isolation;
        }
        if self.default_transaction_access_mode != previous_defaults.default_transaction_access_mode
        {
            settings.default_transaction_access_mode = self.default_transaction_access_mode;
        }
        if self.session_time_zone != previous_defaults.session_time_zone
            && validate_session_time_zone_for_db(new_db_type, self.session_time_zone.trim()).is_ok()
        {
            settings.session_time_zone = self.session_time_zone.clone();
        }

        // Oracle only supports Disabled or Required (TCPS); remap the stricter
        // MySQL modes onto Required so the user does not silently "downgrade"
        // to Disabled when switching databases.
        if self.ssl_mode != previous_defaults.ssl_mode {
            settings.ssl_mode = match (new_db_type, self.ssl_mode) {
                (DatabaseType::Oracle, ConnectionSslMode::VerifyCa)
                | (DatabaseType::Oracle, ConnectionSslMode::VerifyIdentity) => {
                    ConnectionSslMode::Required
                }
                (_, mode) => mode,
            }
        }

        settings
    }

    pub fn validate_for_db(
        &self,
        db_type: DatabaseType,
        using_tns_alias: bool,
    ) -> Result<(), String> {
        if !db_type
            .supported_transaction_isolations()
            .contains(&self.default_transaction_isolation)
        {
            return Err(format!(
                "{} does not support {} as a default transaction isolation",
                db_type,
                self.default_transaction_isolation.label()
            ));
        }

        if !self.session_time_zone.trim().is_empty() {
            validate_session_time_zone_for_db(db_type, self.session_time_zone.trim())?;
        }

        match db_type {
            DatabaseType::Oracle => self.validate_oracle(using_tns_alias),
            DatabaseType::MySQL => self.validate_mysql(),
        }
    }

    fn validate_oracle(&self, using_tns_alias: bool) -> Result<(), String> {
        if !using_tns_alias
            && matches!(
                self.ssl_mode,
                ConnectionSslMode::VerifyCa | ConnectionSslMode::VerifyIdentity
            )
        {
            return Err(
                "Oracle SSL certificate verification is not configured in this dialog; use Required/TCPS or configure verification through a TNS alias"
                    .to_string(),
            );
        }
        if self.default_transaction_access_mode == TransactionAccessMode::ReadOnly
            && self.default_transaction_isolation != TransactionIsolation::Default
        {
            return Err(
                "Oracle does not support combining READ ONLY with an explicit transaction isolation level"
                    .to_string(),
            );
        }
        validate_oracle_nls_format("Oracle NLS date format", self.oracle_nls_date_format.trim())?;
        validate_oracle_nls_format(
            "Oracle NLS timestamp format",
            self.oracle_nls_timestamp_format.trim(),
        )?;
        Ok(())
    }

    fn validate_mysql(&self) -> Result<(), String> {
        let charset = self.mysql_charset.trim();
        let collation = self.mysql_collation.trim();
        validate_mysql_sql_mode(self.mysql_sql_mode.trim())?;
        validate_mysql_identifier("MySQL character set", charset, false)?;
        validate_mysql_identifier("MySQL collation", collation, true)?;
        if !collation.is_empty() && !mysql_collation_matches_charset(collation, charset) {
            return Err(format!(
                "MySQL collation `{collation}` does not match character set `{charset}`"
            ));
        }
        Ok(())
    }

    fn oracle_effective_protocol(&self) -> OracleNetworkProtocol {
        if self.ssl_mode == ConnectionSslMode::Disabled {
            self.oracle_protocol
        } else {
            OracleNetworkProtocol::Tcps
        }
    }
}

impl Default for ConnectionAdvancedSettings {
    fn default() -> Self {
        Self {
            ssl_mode: ConnectionSslMode::Disabled,
            default_transaction_isolation: Self::default_transaction_isolation(),
            default_transaction_access_mode: TransactionAccessMode::ReadWrite,
            session_time_zone: String::new(),
            mysql_sql_mode: Self::default_mysql_sql_mode(),
            mysql_charset: Self::default_mysql_charset(),
            mysql_collation: String::new(),
            mysql_ssl_ca_path: String::new(),
            oracle_protocol: OracleNetworkProtocol::Tcp,
            oracle_nls_date_format: Self::default_oracle_nls_date_format(),
            oracle_nls_timestamp_format: Self::default_oracle_nls_timestamp_format(),
        }
    }
}

#[derive(Clone, Copy)]
struct SessionTimeZoneOffset {
    sign: u8,
    hour: u8,
    minute: u8,
}

fn parse_session_time_zone_offset(value: &str) -> Option<SessionTimeZoneOffset> {
    let bytes = value.as_bytes();
    if bytes.len() != 6 || !matches!(bytes[0], b'+' | b'-') || bytes[3] != b':' {
        return None;
    }
    let hour = value[1..3].parse::<u8>().ok()?;
    let minute = value[4..6].parse::<u8>().ok()?;
    if minute > 59 {
        return None;
    }
    Some(SessionTimeZoneOffset {
        sign: bytes[0],
        hour,
        minute,
    })
}

fn oracle_session_time_zone_in_range(offset: SessionTimeZoneOffset) -> bool {
    offset.hour <= 14
}

fn mysql_session_time_zone_in_range(offset: SessionTimeZoneOffset) -> bool {
    match offset.sign {
        b'+' => offset.hour < 14 || (offset.hour == 14 && offset.minute == 0),
        b'-' => offset.hour < 14,
        _ => false,
    }
}

fn mariadb_session_time_zone_in_range(offset: SessionTimeZoneOffset) -> bool {
    match offset.sign {
        b'+' => offset.hour < 13 || (offset.hour == 13 && offset.minute == 0),
        b'-' => offset.hour < 13,
        _ => false,
    }
}

fn validate_session_time_zone_for_db(db_type: DatabaseType, value: &str) -> Result<(), String> {
    let Some(offset) = parse_session_time_zone_offset(value) else {
        return Err(
            "Session time zone must be blank or an offset like +00:00 or -05:30".to_string(),
        );
    };

    let valid = match db_type {
        DatabaseType::Oracle => oracle_session_time_zone_in_range(offset),
        DatabaseType::MySQL => mysql_session_time_zone_in_range(offset),
    };

    if valid {
        return Ok(());
    }

    match db_type {
        DatabaseType::Oracle => Err(
            "Oracle session time zone must be blank or an offset from -14:59 through +14:59"
                .to_string(),
        ),
        DatabaseType::MySQL => Err(
            "MySQL/MariaDB session time zone must be blank or an offset from -13:59 through +14:00"
                .to_string(),
        ),
    }
}

fn validate_oracle_nls_format(label: &str, value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!("{label} is required"));
    }
    if !value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric()
            || matches!(byte, b' ' | b':' | b'.' | b'-' | b'_' | b'/' | b',' | b';')
    }) {
        return Err(format!("{label} contains invalid characters"));
    }
    Ok(())
}

fn validate_mysql_sql_mode(value: &str) -> Result<(), String> {
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b',' | b'_'))
    {
        return Err("MySQL sql_mode contains invalid characters".to_string());
    }
    Ok(())
}

fn validate_mysql_identifier(label: &str, value: &str, allow_empty: bool) -> Result<(), String> {
    if value.is_empty() {
        return if allow_empty {
            Ok(())
        } else {
            Err(format!("{label} is required"))
        };
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(format!("{label} contains invalid characters"));
    }
    Ok(())
}

fn mysql_collation_matches_charset(collation: &str, charset: &str) -> bool {
    let collation = collation.to_ascii_lowercase();
    let charset = charset.to_ascii_lowercase();
    if collation.starts_with(&format!("{charset}_")) {
        return true;
    }
    if charset == "binary" && collation == "binary" {
        return true;
    }

    matches!(charset.as_str(), "utf8" | "utf8mb3")
        && (collation.starts_with("utf8_") || collation.starts_with("utf8mb3_"))
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

    pub fn supported_transaction_isolations(self) -> &'static [TransactionIsolation] {
        backend_for(self).supported_transaction_isolations()
    }

    pub fn transaction_mode_requires_first_statement(self, mode: TransactionMode) -> bool {
        backend_for(self).transaction_mode_requires_first_statement(mode)
    }

    fn fallback_default_transaction_isolation(self) -> TransactionIsolation {
        match self {
            DatabaseType::Oracle | DatabaseType::MySQL => TransactionIsolation::ReadCommitted,
        }
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

#[derive(Clone, Debug, Serialize)]
pub struct ConnectionInfo {
    pub name: String,
    pub username: String,
    #[serde(skip_serializing)]
    pub password: String,
    pub host: String,
    pub port: u16,
    pub service_name: String,
    pub db_type: DatabaseType,
    pub advanced: ConnectionAdvancedSettings,
}

#[derive(Deserialize)]
struct ConnectionInfoSerde {
    name: String,
    username: String,
    #[serde(default)]
    password: String,
    host: String,
    port: u16,
    service_name: String,
    #[serde(default)]
    db_type: DatabaseType,
    advanced: Option<ConnectionAdvancedSettingsPatch>,
}

#[derive(Default, Deserialize)]
struct ConnectionAdvancedSettingsPatch {
    ssl_mode: Option<ConnectionSslMode>,
    default_transaction_isolation: Option<TransactionIsolation>,
    default_transaction_access_mode: Option<TransactionAccessMode>,
    session_time_zone: Option<String>,
    mysql_sql_mode: Option<String>,
    mysql_charset: Option<String>,
    mysql_collation: Option<String>,
    mysql_ssl_ca_path: Option<String>,
    oracle_protocol: Option<OracleNetworkProtocol>,
    oracle_nls_date_format: Option<String>,
    oracle_nls_timestamp_format: Option<String>,
}

impl ConnectionAdvancedSettings {
    fn default_for_with_patch(
        db_type: DatabaseType,
        patch: Option<ConnectionAdvancedSettingsPatch>,
    ) -> Self {
        let mut settings = Self::default_for(db_type);
        let Some(patch) = patch else {
            return settings;
        };

        if let Some(value) = patch.ssl_mode {
            settings.ssl_mode = value;
        }
        if let Some(value) = patch.default_transaction_isolation {
            settings.default_transaction_isolation = value;
        }
        if let Some(value) = patch.default_transaction_access_mode {
            settings.default_transaction_access_mode = value;
        }
        if let Some(value) = patch.session_time_zone {
            settings.session_time_zone = value;
        }
        if let Some(value) = patch.mysql_sql_mode {
            settings.mysql_sql_mode = value;
        }
        if let Some(value) = patch.mysql_charset {
            settings.mysql_charset = value;
        }
        if let Some(value) = patch.mysql_collation {
            settings.mysql_collation = value;
        }
        if let Some(value) = patch.mysql_ssl_ca_path {
            settings.mysql_ssl_ca_path = value;
        }
        if let Some(value) = patch.oracle_protocol {
            settings.oracle_protocol = value;
        }
        if let Some(value) = patch.oracle_nls_date_format {
            settings.oracle_nls_date_format = value;
        }
        if let Some(value) = patch.oracle_nls_timestamp_format {
            settings.oracle_nls_timestamp_format = value;
        }
        settings
    }
}

impl<'de> Deserialize<'de> for ConnectionInfo {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let fields = ConnectionInfoSerde::deserialize(deserializer)?;
        Ok(Self {
            name: fields.name,
            username: fields.username,
            password: fields.password,
            host: fields.host,
            port: fields.port,
            service_name: fields.service_name,
            db_type: fields.db_type,
            advanced: ConnectionAdvancedSettings::default_for_with_patch(
                fields.db_type,
                fields.advanced,
            ),
        })
    }
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
            advanced: ConnectionAdvancedSettings::default_for(DatabaseType::Oracle),
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
            advanced: ConnectionAdvancedSettings::default_for(db_type),
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
    Oracle {
        pool: oracle::pool::Pool,
        advanced: ConnectionAdvancedSettings,
    },
    MySQL {
        pool: mysql::Pool,
        advanced: ConnectionAdvancedSettings,
    },
}

pub enum DbPoolSession {
    Oracle(Connection),
    MySQL(mysql::PooledConn),
}

pub enum DbSessionLease {
    Oracle(Arc<Connection>),
    MySQL(mysql::PooledConn),
}

pub struct DbSessionLeaseEntry {
    connection_generation: u64,
    lease: DbSessionLease,
    may_have_uncommitted_work: bool,
    requires_transaction_decision: bool,
}

/// One editor tab's owned DB session slot.
///
/// Oracle and MySQL/MariaDB both use this same lifecycle: take the lease for
/// execution, retain it in the tab slot after cleanup, and clear it on close,
/// disconnect, cancel, or stale connection generation.
#[derive(Clone, Default)]
pub struct SharedDbSessionLease {
    inner: Arc<Mutex<Option<DbSessionLeaseEntry>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PooledSessionLeaseSnapshot {
    pub db_type: DatabaseType,
    pub may_have_uncommitted_work: bool,
    pub requires_transaction_decision: bool,
}

impl PooledSessionLeaseSnapshot {
    pub fn transaction_state(self) -> TransactionSessionState {
        TransactionSessionState::from_flags(
            self.may_have_uncommitted_work,
            self.requires_transaction_decision,
        )
    }
}

#[derive(Clone)]
pub struct DbPoolSessionContext {
    pub connection_generation: u64,
    pub connection_info: ConnectionInfo,
    pub pool: DbConnectionPool,
    pub current_service_name: String,
    pub transaction_mode: TransactionMode,
}

impl DbConnectionPool {
    pub fn acquire_session(&self) -> Result<DbPoolSession, String> {
        let mut session = match self {
            DbConnectionPool::Oracle { pool, .. } => DbPoolSession::Oracle(
                pool.get()
                    .map_err(|err| Self::format_oracle_pool_acquire_error(pool, &err))?,
            ),
            DbConnectionPool::MySQL { pool, .. } => DbPoolSession::MySQL(
                pool.try_get_conn(MYSQL_POOL_ACQUIRE_TIMEOUT)
                    .map_err(|err| Self::format_mysql_pool_acquire_error(&err))?,
            ),
        };
        match (self, &mut session) {
            (DbConnectionPool::Oracle { advanced, .. }, DbPoolSession::Oracle(conn)) => {
                DatabaseConnection::apply_oracle_session_settings(conn, advanced)?;
            }
            (DbConnectionPool::MySQL { advanced, .. }, DbPoolSession::MySQL(conn)) => {
                DatabaseConnection::apply_mysql_session_settings(conn, advanced)?;
            }
            _ => {}
        }
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
        let looks_pool_exhausted =
            matches!(err, mysql::Error::DriverError(mysql::DriverError::Timeout));
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

impl DbSessionLeaseEntry {
    fn new(
        connection_generation: u64,
        lease: DbSessionLease,
        may_have_uncommitted_work: bool,
        requires_transaction_decision: bool,
    ) -> Self {
        Self {
            connection_generation,
            lease,
            may_have_uncommitted_work,
            requires_transaction_decision,
        }
    }

    fn matches(&self, connection_generation: u64, db_type: DatabaseType) -> bool {
        self.connection_generation == connection_generation && self.lease.db_type() == db_type
    }
}

impl SharedDbSessionLease {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(None)),
        }
    }

    pub fn clear(&self) -> bool {
        let lease_to_drop = {
            self.inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
        };
        lease_to_drop.is_some()
    }

    pub fn snapshot(&self) -> Option<PooledSessionLeaseSnapshot> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(|entry| PooledSessionLeaseSnapshot {
                db_type: entry.lease.db_type(),
                may_have_uncommitted_work: entry.may_have_uncommitted_work,
                requires_transaction_decision: entry.requires_transaction_decision,
            })
    }

    pub fn take_reusable_with_decision_state(
        &self,
        connection_generation: u64,
        db_type: DatabaseType,
    ) -> Option<(DbSessionLease, bool, bool)> {
        let mut stale_lease_to_drop = None;
        let reusable_lease = {
            let mut lease = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let reusable = lease
                .as_ref()
                .is_some_and(|existing| existing.matches(connection_generation, db_type));
            if reusable {
                lease.take().map(|entry| {
                    (
                        entry.lease,
                        entry.may_have_uncommitted_work,
                        entry.requires_transaction_decision,
                    )
                })
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

    pub fn clear_oracle_if_current_connection(
        &self,
        connection_generation: u64,
        expected_conn: &Arc<Connection>,
    ) -> bool {
        let lease_to_drop = {
            let mut lease = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let should_clear = lease.as_ref().is_some_and(|existing| {
                existing.connection_generation == connection_generation
                    && matches!(
                        &existing.lease,
                        DbSessionLease::Oracle(conn) if Arc::ptr_eq(conn, expected_conn)
                    )
            });
            if should_clear {
                lease.take()
            } else {
                None
            }
        };
        lease_to_drop.is_some()
    }

    pub fn take_reusable_with_state(
        &self,
        connection_generation: u64,
        db_type: DatabaseType,
    ) -> Option<(DbSessionLease, bool)> {
        self.take_reusable_with_decision_state(connection_generation, db_type)
            .map(|(lease, may_have_uncommitted_work, _)| (lease, may_have_uncommitted_work))
    }

    pub fn store_if_empty(
        &self,
        connection_generation: u64,
        lease_to_store: DbSessionLease,
        may_have_uncommitted_work: bool,
    ) -> bool {
        self.store_if_empty_with_transaction_decision(
            connection_generation,
            lease_to_store,
            may_have_uncommitted_work,
            false,
        )
    }

    pub fn store_if_empty_with_transaction_decision(
        &self,
        connection_generation: u64,
        lease_to_store: DbSessionLease,
        may_have_uncommitted_work: bool,
        requires_transaction_decision: bool,
    ) -> bool {
        let lease_db_type = lease_to_store.db_type();
        let mut lease_to_store = Some(lease_to_store);
        let old_lease_to_drop = {
            let mut lease = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let should_store = match lease.as_ref() {
                None => true,
                Some(existing) => {
                    existing.connection_generation != connection_generation
                        || existing.lease.db_type() != lease_db_type
                }
            };
            if should_store {
                let old_lease = lease.take();
                if let Some(lease_to_store) = lease_to_store.take() {
                    *lease = Some(DbSessionLeaseEntry::new(
                        connection_generation,
                        lease_to_store,
                        may_have_uncommitted_work,
                        requires_transaction_decision,
                    ));
                }
                old_lease
            } else {
                None
            }
        };
        drop(old_lease_to_drop);
        lease_to_store.is_none()
    }
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
    fn apply_auto_commit(
        &self,
        _connection: &mut DbConnection,
        _enabled: bool,
    ) -> Result<(), String> {
        Ok(())
    }
    fn supported_transaction_isolations(&self) -> &'static [TransactionIsolation] {
        &DEFAULT_TRANSACTION_ISOLATIONS
    }
    fn transaction_mode_requires_first_statement(&self, _mode: TransactionMode) -> bool {
        false
    }
    fn transaction_mode_statements(&self, mode: TransactionMode) -> Result<Vec<String>, String> {
        if self
            .supported_transaction_isolations()
            .contains(&mode.isolation)
        {
            Ok(Vec::new())
        } else {
            Err(format!(
                "{} does not support {} transaction isolation",
                self.display_name(),
                mode.isolation.label()
            ))
        }
    }
}

struct OracleBackend;
struct MysqlBackend;

const ORACLE_TRANSACTION_ISOLATIONS: [TransactionIsolation; 3] = [
    TransactionIsolation::Default,
    TransactionIsolation::ReadCommitted,
    TransactionIsolation::Serializable,
];
const DEFAULT_TRANSACTION_ISOLATIONS: [TransactionIsolation; 1] = [TransactionIsolation::Default];
const MYSQL_TRANSACTION_ISOLATIONS: [TransactionIsolation; 5] = [
    TransactionIsolation::Default,
    TransactionIsolation::ReadUncommitted,
    TransactionIsolation::ReadCommitted,
    TransactionIsolation::RepeatableRead,
    TransactionIsolation::Serializable,
];

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
            advanced: ConnectionAdvancedSettings::default_for(DatabaseType::Oracle),
        }
    }

    fn connection_string(&self, info: &ConnectionInfo) -> String {
        if info.uses_oracle_tns_alias() {
            info.service_name.trim().to_string()
        } else if info.advanced.oracle_effective_protocol() == OracleNetworkProtocol::Tcps {
            format!(
                "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST={})(PORT={}))(CONNECT_DATA=(SERVICE_NAME={})))",
                info.host, info.port, info.service_name
            )
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
            Connector::new(&info.username, &info.password, &conn_str)
                .connect()
                .map_err(|err| {
                    eprintln!("Connection error: {err}");
                    err.to_string()
                })?,
        );
        DatabaseConnection::apply_oracle_session_settings(connection.as_ref(), &info.advanced)?;
        let pool = DatabaseConnection::build_oracle_pool(info, pool_size)?;
        Ok((
            DbConnection::Oracle(connection),
            DbConnectionPool::Oracle {
                pool,
                advanced: info.advanced.clone(),
            },
        ))
    }

    fn test_connection(&self, info: &ConnectionInfo) -> Result<(), String> {
        ensure_oracle_client_initialized().map_err(|e| e.to_string())?;
        let conn_str = info.connection_string();
        let connection = Connector::new(&info.username, &info.password, &conn_str)
            .connect()
            .map_err(|err| {
                eprintln!("Connection error: {err}");
                err.to_string()
            })?;
        DatabaseConnection::apply_oracle_session_settings(&connection, &info.advanced)?;
        Ok(())
    }

    fn supported_transaction_isolations(&self) -> &'static [TransactionIsolation] {
        &ORACLE_TRANSACTION_ISOLATIONS
    }

    fn transaction_mode_requires_first_statement(&self, mode: TransactionMode) -> bool {
        !mode.is_default()
    }

    fn transaction_mode_statements(&self, mode: TransactionMode) -> Result<Vec<String>, String> {
        if !self
            .supported_transaction_isolations()
            .contains(&mode.isolation)
        {
            return Err(format!(
                "Oracle does not support {} transaction isolation",
                mode.isolation.label()
            ));
        }

        if mode.access_mode == TransactionAccessMode::ReadOnly {
            if mode.isolation != TransactionIsolation::Default {
                return Err(
                    "Oracle does not support combining READ ONLY with an explicit transaction isolation level"
                        .to_string(),
                );
            }
            return Ok(vec![format!(
                "SET TRANSACTION {}",
                mode.access_mode.sql_clause()
            )]);
        }

        if let Some(level) = mode.isolation.sql_level() {
            return Ok(vec![format!("SET TRANSACTION ISOLATION LEVEL {level}")]);
        }

        Ok(Vec::new())
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
            advanced: ConnectionAdvancedSettings::default_for(DatabaseType::MySQL),
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
        DatabaseConnection::apply_mysql_session_settings(&mut conn, &info.advanced)?;
        DatabaseConnection::apply_mysql_autocommit_setting(&mut conn, auto_commit)?;
        let pool = DatabaseConnection::build_mysql_pool(info, pool_size)?;
        Ok((
            DbConnection::MySQL(conn),
            DbConnectionPool::MySQL {
                pool,
                advanced: info.advanced.clone(),
            },
        ))
    }

    fn test_connection(&self, info: &ConnectionInfo) -> Result<(), String> {
        let opts = DatabaseConnection::build_mysql_opts(info);
        let mut conn = mysql::Conn::new(opts).map_err(|err| {
            eprintln!("MySQL connection error: {err}");
            err.to_string()
        })?;
        DatabaseConnection::apply_mysql_session_settings(&mut conn, &info.advanced)?;
        Ok(())
    }

    fn after_connect(&self, connection: &mut DatabaseConnection) {
        if let Err(err) = connection.sync_mysql_current_database_name() {
            eprintln!("Warning: failed to sync MySQL current database after connect: {err}");
        }
    }

    fn apply_auto_commit(
        &self,
        connection: &mut DbConnection,
        enabled: bool,
    ) -> Result<(), String> {
        if let DbConnection::MySQL(conn) = connection {
            DatabaseConnection::apply_mysql_autocommit_setting(conn, enabled)?;
        }
        Ok(())
    }

    fn supported_transaction_isolations(&self) -> &'static [TransactionIsolation] {
        &MYSQL_TRANSACTION_ISOLATIONS
    }

    fn transaction_mode_statements(&self, mode: TransactionMode) -> Result<Vec<String>, String> {
        if !self
            .supported_transaction_isolations()
            .contains(&mode.isolation)
        {
            return Err(format!(
                "MySQL/MariaDB does not support {} transaction isolation",
                mode.isolation.label()
            ));
        }

        let mut characteristics = Vec::new();
        if let Some(level) = mode.isolation.sql_level() {
            characteristics.push(format!("ISOLATION LEVEL {level}"));
        }
        characteristics.push(mode.access_mode.sql_clause().to_string());

        Ok(vec![format!(
            "SET SESSION TRANSACTION {}",
            characteristics.join(", ")
        )])
    }
}

pub struct DatabaseConnection {
    connection: Option<DbConnection>,
    pool: Option<DbConnectionPool>,
    info: ConnectionInfo,
    session_password: String,
    oracle_current_schema: Option<String>,
    connected: bool,
    auto_commit: bool,
    transaction_mode: TransactionMode,
    default_transaction_isolation: TransactionIsolation,
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

    pub(crate) fn build_mysql_opts_without_database(info: &ConnectionInfo) -> mysql::OptsBuilder {
        Self::build_mysql_opts_with_pool_size_and_database(info, None, false)
    }

    fn build_mysql_opts_with_pool_size(
        info: &ConnectionInfo,
        pool_size: Option<u32>,
    ) -> mysql::OptsBuilder {
        Self::build_mysql_opts_with_pool_size_and_database(info, pool_size, true)
    }

    fn build_mysql_pool_opts(info: &ConnectionInfo, pool_size: u32) -> mysql::OptsBuilder {
        Self::build_mysql_opts_with_pool_size_and_database(info, Some(pool_size), false)
    }

    fn build_mysql_opts_with_pool_size_and_database(
        info: &ConnectionInfo,
        pool_size: Option<u32>,
        include_database: bool,
    ) -> mysql::OptsBuilder {
        let mut opts = mysql::OptsBuilder::new()
            .ip_or_hostname(Some(&info.host))
            .tcp_port(info.port)
            .user(Some(&info.username))
            .pass(Some(&info.password))
            .prefer_socket(false);

        let database = info.service_name.trim();
        if include_database && !database.is_empty() {
            opts = opts.db_name(Some(database));
        }

        opts = Self::apply_mysql_driver_options(opts, &info.advanced);

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

    fn apply_mysql_driver_options(
        mut opts: mysql::OptsBuilder,
        advanced: &ConnectionAdvancedSettings,
    ) -> mysql::OptsBuilder {
        if advanced.ssl_mode != ConnectionSslMode::Disabled {
            let mut ssl_opts = mysql::SslOpts::default();
            let ca_path = advanced.mysql_ssl_ca_path.trim();
            if !ca_path.is_empty() {
                ssl_opts = ssl_opts.with_root_cert_path(Some(std::path::PathBuf::from(ca_path)));
            }
            ssl_opts = match advanced.ssl_mode {
                ConnectionSslMode::Disabled => ssl_opts,
                ConnectionSslMode::Required => ssl_opts
                    .with_danger_skip_domain_validation(true)
                    .with_danger_accept_invalid_certs(true),
                ConnectionSslMode::VerifyCa => ssl_opts.with_danger_skip_domain_validation(true),
                ConnectionSslMode::VerifyIdentity => ssl_opts,
            };
            opts = opts.ssl_opts(ssl_opts);
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
        let opts = Self::build_mysql_pool_opts(info, pool_size);
        mysql::Pool::new(opts).map_err(|err| err.to_string())
    }

    fn build_pool_for_info(
        info: &ConnectionInfo,
        pool_size: u32,
    ) -> Result<DbConnectionPool, String> {
        match info.db_type {
            DatabaseType::Oracle => {
                Self::build_oracle_pool(info, pool_size).map(|pool| DbConnectionPool::Oracle {
                    pool,
                    advanced: info.advanced.clone(),
                })
            }
            DatabaseType::MySQL => {
                Self::build_mysql_pool(info, pool_size).map(|pool| DbConnectionPool::MySQL {
                    pool,
                    advanced: info.advanced.clone(),
                })
            }
        }
    }

    pub fn new() -> Self {
        Self {
            connection: None,
            pool: None,
            info: ConnectionInfo::default(),
            session_password: String::new(),
            oracle_current_schema: None,
            connected: false,
            auto_commit: false,
            transaction_mode: TransactionMode::default(),
            default_transaction_isolation: TransactionIsolation::Default,
            session: Arc::new(Mutex::new(SessionState::default())),
            last_disconnect_reason: None,
            connection_generation: 0,
            connection_pool_size: DEFAULT_CONNECTION_POOL_SIZE,
        }
    }

    pub fn connect(&mut self, info: ConnectionInfo) -> Result<(), String> {
        info.advanced
            .validate_for_db(info.db_type, info.uses_oracle_tns_alias())?;
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
        self.oracle_current_schema = None;
        self.sync_default_transaction_isolation(db_type);
        self.transaction_mode = TransactionMode::new(
            TransactionIsolation::Default,
            self.info.advanced.default_transaction_access_mode,
        );
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

    pub(crate) fn apply_oracle_session_settings(
        conn: &Connection,
        advanced: &ConnectionAdvancedSettings,
    ) -> Result<(), String> {
        let statements = Self::oracle_session_setting_statements(advanced);

        for statement in statements {
            if let Err(err) = conn.execute(statement.as_str(), &[]) {
                return Err(format!(
                    "Failed to apply Oracle session setting `{statement}`: {err}"
                ));
            }
        }
        Ok(())
    }

    fn oracle_session_setting_statements(advanced: &ConnectionAdvancedSettings) -> Vec<String> {
        let mut statements = vec![
            format!(
                "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = '{}'",
                advanced.oracle_nls_timestamp_format.trim()
            ),
            format!(
                "ALTER SESSION SET NLS_DATE_FORMAT = '{}'",
                advanced.oracle_nls_date_format.trim()
            ),
        ];

        if let Some(level) = advanced.default_transaction_isolation.sql_level() {
            statements.push(format!("ALTER SESSION SET ISOLATION_LEVEL = {level}"));
        }
        let time_zone = advanced.session_time_zone.trim();
        if !time_zone.is_empty() {
            statements.push(format!("ALTER SESSION SET TIME_ZONE = '{time_zone}'"));
        }
        statements
    }

    fn normalize_oracle_current_schema_name(schema: &str) -> Option<String> {
        let trimmed = schema.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    }

    fn set_tracked_oracle_current_schema(&mut self, schema: Option<String>) {
        self.oracle_current_schema = schema
            .as_deref()
            .and_then(Self::normalize_oracle_current_schema_name);
    }

    pub(crate) fn apply_mysql_session_settings<C: Queryable>(
        conn: &mut C,
        advanced: &ConnectionAdvancedSettings,
    ) -> Result<(), String> {
        Self::validate_mysql_session_time_zone_for_server(conn, advanced.session_time_zone.trim())?;
        let statements = Self::mysql_session_setting_statements(advanced);

        for statement in statements {
            if let Err(err) = conn.query_drop(statement.as_str()) {
                return Err(format!(
                    "Failed to apply MySQL session setting `{statement}`: {err}"
                ));
            }
        }

        Self::apply_mysql_connection_encoding_with_settings(conn, advanced)
    }

    fn validate_mysql_session_time_zone_for_server<C: Queryable>(
        conn: &mut C,
        time_zone: &str,
    ) -> Result<(), String> {
        let Some(offset) = parse_session_time_zone_offset(time_zone) else {
            return Ok(());
        };
        if mariadb_session_time_zone_in_range(offset) {
            return Ok(());
        }

        if let Ok(Some(version)) = conn.query_first::<String, _>("SELECT VERSION()") {
            Self::validate_mysql_session_time_zone_for_server_version(time_zone, &version)?;
        }
        Ok(())
    }

    fn validate_mysql_session_time_zone_for_server_version(
        time_zone: &str,
        server_version: &str,
    ) -> Result<(), String> {
        let Some(offset) = parse_session_time_zone_offset(time_zone) else {
            return Ok(());
        };
        if mariadb_session_time_zone_in_range(offset)
            || !server_version.to_ascii_lowercase().contains("mariadb")
        {
            return Ok(());
        }

        Err(format!(
            "MariaDB session time zone `{time_zone}` is outside MariaDB's supported offset range (-12:59 through +13:00)"
        ))
    }

    fn mysql_session_setting_statements(advanced: &ConnectionAdvancedSettings) -> Vec<String> {
        let mut statements = Vec::new();
        statements.push(format!(
            "SET SESSION sql_mode = '{}'",
            advanced.mysql_sql_mode.trim()
        ));
        let time_zone = advanced.session_time_zone.trim();
        if !time_zone.is_empty() {
            statements.push(format!("SET SESSION time_zone = '{time_zone}'"));
        }
        if let Some(level) = advanced.default_transaction_isolation.sql_level() {
            statements.push(format!("SET SESSION TRANSACTION ISOLATION LEVEL {level}"));
        }
        statements
    }

    pub(crate) fn apply_mysql_connection_encoding_with_settings<C: Queryable>(
        conn: &mut C,
        advanced: &ConnectionAdvancedSettings,
    ) -> Result<(), String> {
        let database_collation = Self::mysql_current_database_collation(conn);
        let statement =
            Self::mysql_set_names_statement_with_settings(database_collation.as_deref(), advanced);

        if let Err(err) = conn.query_drop(statement.as_str()) {
            return Err(format!(
                "Failed to apply MySQL session setting `{statement}`: {err}"
            ));
        }
        Ok(())
    }

    fn mysql_current_database_collation<C: Queryable>(conn: &mut C) -> Option<String> {
        match conn.query_first::<String, _>(
            "SELECT DEFAULT_COLLATION_NAME \
             FROM INFORMATION_SCHEMA.SCHEMATA \
             WHERE SCHEMA_NAME = DATABASE()",
        ) {
            Ok(Some(collation)) => return Some(collation.trim().to_string()),
            Ok(None) => {}
            Err(err) => {
                eprintln!(
                    "Warning: failed to read MySQL current database collation for session setup: {err}"
                );
            }
        }

        match conn.query_first::<String, _>("SELECT @@collation_database") {
            Ok(value) => value.map(|collation| collation.trim().to_string()),
            Err(err) => {
                eprintln!(
                    "Warning: failed to read MySQL database collation for session setup: {err}"
                );
                None
            }
        }
    }

    #[cfg(test)]
    fn mysql_set_names_statement(database_collation: Option<&str>) -> String {
        Self::mysql_set_names_statement_with_settings(
            database_collation,
            &ConnectionAdvancedSettings::default_for(DatabaseType::MySQL),
        )
    }

    fn mysql_set_names_statement_with_settings(
        database_collation: Option<&str>,
        advanced: &ConnectionAdvancedSettings,
    ) -> String {
        let charset = advanced.mysql_charset.trim();
        let configured_collation = advanced.mysql_collation.trim();
        if !configured_collation.is_empty()
            && Self::mysql_collation_name_is_safe(configured_collation)
        {
            return format!("SET NAMES {charset} COLLATE {configured_collation}");
        }

        match database_collation.map(str::trim) {
            Some(collation)
                if !collation.is_empty()
                    && Self::mysql_collation_name_is_safe(collation)
                    && mysql_collation_matches_charset(collation, charset) =>
            {
                format!("SET NAMES {charset} COLLATE {collation}")
            }
            _ => format!("SET NAMES {charset}"),
        }
    }

    fn mysql_collation_name_is_safe(collation: &str) -> bool {
        collation
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    }

    fn oracle_identifier_needs_quotes(identifier: &str) -> bool {
        let mut chars = identifier.chars();
        let Some(first) = chars.next() else {
            return true;
        };
        if !(first.is_ascii_alphabetic() || matches!(first, '_' | '$' | '#')) {
            return true;
        }
        !chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '$' | '#'))
    }

    fn quote_oracle_identifier(identifier: &str) -> String {
        let trimmed = identifier.trim();
        if trimmed.is_empty() {
            return "\"\"".to_string();
        }
        if trimmed.starts_with('"') && trimmed.ends_with('"') {
            return trimmed.to_string();
        }
        if Self::oracle_identifier_needs_quotes(trimmed) {
            format!("\"{}\"", trimmed.replace('"', "\"\""))
        } else {
            trimmed.to_string()
        }
    }

    fn oracle_set_current_schema_statement(schema: &str) -> String {
        format!(
            "ALTER SESSION SET CURRENT_SCHEMA = {}",
            Self::quote_oracle_identifier(schema)
        )
    }

    fn read_oracle_current_schema(conn: &Connection) -> Result<String, String> {
        let sql = "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM dual";
        let mut stmt = conn.statement(sql).build().map_err(|err| err.to_string())?;
        let row = stmt.query_row(&[]).map_err(|err| err.to_string())?;
        row.get::<_, Option<String>>(0)
            .map_err(|err| err.to_string())
            .map(|value| value.unwrap_or_default().trim().to_string())
    }

    fn read_oracle_default_transaction_isolation(
        conn: &Connection,
    ) -> Result<Option<TransactionIsolation>, String> {
        let sql = "\
            SELECT value \
            FROM v$ses_optimizer_env \
            WHERE sid = SYS_CONTEXT('USERENV', 'SID') \
              AND name = 'transaction_isolation_level'";
        let mut stmt = conn.statement(sql).build().map_err(|err| err.to_string())?;
        let row = stmt.query_row(&[]).map_err(|err| err.to_string())?;
        let raw = row
            .get::<_, Option<String>>(0)
            .map_err(|err| err.to_string())?
            .unwrap_or_default();
        Ok(TransactionIsolation::from_sql_level(&raw))
    }

    fn apply_oracle_current_schema(conn: &Connection, schema: Option<&str>) -> Result<(), String> {
        let Some(schema) = schema.and_then(Self::normalize_oracle_current_schema_name) else {
            return Ok(());
        };

        let statement = Self::oracle_set_current_schema_statement(&schema);
        conn.execute(&statement, &[])
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    fn apply_mysql_autocommit_setting<C: Queryable>(
        conn: &mut C,
        enabled: bool,
    ) -> Result<(), String> {
        let statement = if enabled {
            "SET autocommit = 1"
        } else {
            "SET autocommit = 0"
        };

        conn.query_drop(statement)
            .map_err(|err| format!("Failed to apply MySQL autocommit setting `{statement}`: {err}"))
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
        self.oracle_current_schema = None;
        self.auto_commit = false;
        self.transaction_mode = TransactionMode::default();
        self.default_transaction_isolation = TransactionIsolation::Default;
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
            transaction_mode: self.transaction_mode,
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

    pub fn set_auto_commit(&mut self, enabled: bool) -> Result<(), String> {
        if self.auto_commit == enabled {
            return Ok(());
        }

        let db_type = self.info.db_type;
        if let Some(connection) = self.connection.as_mut() {
            backend_for(db_type).apply_auto_commit(connection, enabled)?;
        }
        self.auto_commit = enabled;
        Ok(())
    }

    pub fn auto_commit(&self) -> bool {
        self.auto_commit
    }

    fn sync_default_transaction_isolation(&mut self, db_type: DatabaseType) {
        let configured = self.info.advanced.default_transaction_isolation;
        if configured != TransactionIsolation::Default
            && db_type
                .supported_transaction_isolations()
                .contains(&configured)
        {
            self.default_transaction_isolation = configured;
            return;
        }

        self.default_transaction_isolation = self
            .read_current_default_transaction_isolation(db_type)
            .ok()
            .flatten()
            .unwrap_or_else(|| db_type.fallback_default_transaction_isolation());
    }

    fn read_current_default_transaction_isolation(
        &mut self,
        db_type: DatabaseType,
    ) -> Result<Option<TransactionIsolation>, String> {
        match (db_type, &mut self.connection) {
            (DatabaseType::Oracle, Some(DbConnection::Oracle(conn))) => {
                Self::read_oracle_default_transaction_isolation(conn.as_ref())
            }
            (DatabaseType::MySQL, Some(DbConnection::MySQL(conn))) => {
                Self::read_mysql_default_transaction_isolation(conn)
            }
            _ => Ok(None),
        }
    }

    pub fn set_transaction_mode(&mut self, mode: TransactionMode) -> Result<(), String> {
        let db_type = self.info.db_type;
        backend_for(db_type).transaction_mode_statements(mode)?;
        if let (DatabaseType::MySQL, Some(DbConnection::MySQL(conn))) =
            (db_type, self.connection.as_mut())
        {
            Self::apply_mysql_transaction_mode(conn, mode)?;
        }
        self.transaction_mode = mode;
        Ok(())
    }

    pub fn transaction_mode(&self) -> TransactionMode {
        self.transaction_mode
    }

    pub fn default_transaction_isolation(&self) -> TransactionIsolation {
        self.default_transaction_isolation
    }

    pub fn transaction_mode_statements_for(
        db_type: DatabaseType,
        mode: TransactionMode,
    ) -> Result<Vec<String>, String> {
        backend_for(db_type).transaction_mode_statements(mode)
    }

    pub fn apply_oracle_transaction_mode(
        conn: &Connection,
        mode: TransactionMode,
    ) -> Result<(), String> {
        for statement in Self::transaction_mode_statements_for(DatabaseType::Oracle, mode)? {
            conn.execute(&statement, &[])
                .map_err(|err| format!("Failed to apply transaction mode: {err}"))?;
        }
        Ok(())
    }

    pub fn apply_mysql_transaction_mode<C: Queryable>(
        conn: &mut C,
        mode: TransactionMode,
    ) -> Result<(), String> {
        for statement in Self::transaction_mode_statements_for(DatabaseType::MySQL, mode)? {
            conn.query_drop(statement.as_str())
                .map_err(|err| format!("Failed to apply transaction mode: {err}"))?;
        }
        Ok(())
    }

    fn read_mysql_default_transaction_isolation<C: Queryable>(
        conn: &mut C,
    ) -> Result<Option<TransactionIsolation>, String> {
        let raw = match conn.query_first::<String, _>("SELECT @@transaction_isolation") {
            Ok(value) => value,
            Err(_) => conn
                .query_first::<String, _>("SELECT @@tx_isolation")
                .map_err(|err| err.to_string())?,
        };

        Ok(raw
            .as_deref()
            .and_then(TransactionIsolation::from_sql_level))
    }

    pub fn apply_tracked_oracle_current_schema(&self, conn: &Connection) -> Result<(), String> {
        Self::apply_oracle_current_schema(conn, self.oracle_current_schema.as_deref())
    }

    pub fn sync_mysql_current_database_name(&mut self) -> Result<String, String> {
        let advanced = self.info.advanced.clone();
        let Some(conn) = self.get_mysql_connection_mut() else {
            return Err("Expected MySQL connection but none is active".to_string());
        };

        let current_database = conn
            .query_first::<Option<String>, _>("SELECT DATABASE()")
            .map_err(|err| err.to_string())?
            .flatten()
            .map(|database| database.trim().to_string())
            .unwrap_or_default();
        Self::apply_mysql_connection_encoding_with_settings(conn, &advanced)?;
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
            Self::apply_mysql_connection_encoding_with_settings(conn, &self.info.advanced)?;
        }
        self.info.service_name = current_database.clone();
        Ok(current_database)
    }

    pub fn switch_mysql_database(&mut self, database: &str) -> Result<(), String> {
        if self.info.db_type != DatabaseType::MySQL || !self.connected {
            return Err("Expected MySQL connection but none is active".to_string());
        }

        let target_database = database.trim();
        let advanced = self.info.advanced.clone();
        let Some(conn) = self.get_mysql_connection_mut() else {
            return Err("Expected MySQL connection but none is active".to_string());
        };

        conn.select_db(target_database)
            .map_err(|err| err.to_string())?;
        Self::apply_mysql_connection_encoding_with_settings(conn, &advanced)?;
        self.info.service_name = target_database.to_string();
        Ok(())
    }

    pub fn sync_oracle_current_schema_from_session(
        &mut self,
        conn: &Connection,
    ) -> Result<String, String> {
        if self.info.db_type != DatabaseType::Oracle || !self.connected {
            return Err("Expected Oracle connection but none is active".to_string());
        }

        let current_schema = Self::read_oracle_current_schema(conn)?;
        self.set_tracked_oracle_current_schema(Some(current_schema.clone()));

        if let Ok(primary_conn) = self.require_live_connection() {
            if !std::ptr::eq(primary_conn.as_ref(), conn) {
                if let Err(err) =
                    Self::apply_oracle_current_schema(primary_conn.as_ref(), Some(&current_schema))
                {
                    eprintln!(
                        "Warning: failed to mirror Oracle current schema to primary connection: {err}"
                    );
                }
            }
        }

        Ok(current_schema)
    }

    pub fn switch_oracle_current_schema(&mut self, schema: &str) -> Result<(), String> {
        if self.info.db_type != DatabaseType::Oracle || !self.connected {
            return Err("Expected Oracle connection but none is active".to_string());
        }

        let target_schema = schema.trim();
        if target_schema.is_empty() {
            return Err("Schema name cannot be empty".to_string());
        }

        let conn = self.require_live_connection()?;
        let statement = Self::oracle_set_current_schema_statement(target_schema);
        conn.execute(&statement, &[])
            .map(|_| ())
            .map_err(|err| err.to_string())?;
        self.set_tracked_oracle_current_schema(Some(target_schema.to_string()));
        Ok(())
    }

    pub fn session_state(&self) -> Arc<Mutex<SessionState>> {
        Arc::clone(&self.session)
    }

    pub fn test_connection(info: &ConnectionInfo) -> Result<(), String> {
        info.advanced
            .validate_for_db(info.db_type, info.uses_oracle_tns_alias())?;
        backend_for(info.db_type).test_connection(info)
    }

    #[cfg(test)]
    fn simulate_connected_metadata_for_test(&mut self, info: ConnectionInfo) {
        self.connected = true;
        self.session_password = info.password.clone();
        self.oracle_current_schema = None;
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

    fn oracle_test_connection_info_from_env() -> ConnectionInfo {
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

        ConnectionInfo::new_with_type(
            "local",
            &username,
            &password,
            &host,
            port,
            &service_name,
            DatabaseType::Oracle,
        )
    }

    fn mysql_test_connection_info_from_env() -> ConnectionInfo {
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

        ConnectionInfo::new_with_type(
            "local",
            &user,
            &password,
            &host,
            port,
            &database,
            DatabaseType::MySQL,
        )
    }

    fn read_oracle_session_parameter(conn: &Connection, parameter: &str) -> String {
        let mut stmt = conn
            .statement("SELECT value FROM nls_session_parameters WHERE parameter = :1")
            .build()
            .expect("build Oracle session parameter query");
        let row = stmt
            .query_row(&[&parameter])
            .expect("read Oracle session parameter");
        row.get::<_, String>(0)
            .expect("Oracle session parameter value")
    }

    fn read_oracle_session_time_zone(conn: &Connection) -> String {
        let mut stmt = conn
            .statement("SELECT SESSIONTIMEZONE FROM dual")
            .build()
            .expect("build Oracle session time zone query");
        let row = stmt.query_row(&[]).expect("read Oracle session time zone");
        row.get::<_, String>(0).expect("Oracle session time zone")
    }

    #[test]
    fn require_live_connection_returns_default_message_when_never_connected() {
        let mut conn = DatabaseConnection::new();
        let err = conn
            .require_live_connection()
            .expect_err("must be disconnected");
        assert_eq!(err, NOT_CONNECTED_MESSAGE);
    }

    #[test]
    fn disconnect_resets_connection_metadata_auto_commit_and_transaction_mode() {
        let mut conn = DatabaseConnection::new();
        conn.info = ConnectionInfo::new("Prod", "scott", "pw", "db", 1521, "FREE");
        conn.connected = true;
        conn.auto_commit = true;
        conn.transaction_mode = TransactionMode::new(
            TransactionIsolation::Serializable,
            TransactionAccessMode::ReadOnly,
        );
        conn.disconnect();

        assert!(!conn.connected);
        assert!(!conn.auto_commit);
        assert_eq!(conn.transaction_mode(), TransactionMode::default());
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
    fn mysql_interactive_connection_opts_keep_requested_database() {
        let info = ConnectionInfo::new_with_type(
            "local",
            "root",
            "pw",
            "localhost",
            3306,
            "initial_db",
            DatabaseType::MySQL,
        );
        let opts = mysql::Opts::from(DatabaseConnection::build_mysql_opts(&info));

        assert_eq!(opts.get_db_name(), Some("initial_db"));
    }

    #[test]
    fn mysql_pool_opts_do_not_pin_initial_database() {
        let info = ConnectionInfo::new_with_type(
            "local",
            "root",
            "pw",
            "localhost",
            3306,
            "initial_db",
            DatabaseType::MySQL,
        );
        let opts = mysql::Opts::from(DatabaseConnection::build_mysql_pool_opts(&info, 4));

        assert_eq!(opts.get_db_name(), None);
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
    fn oracle_transaction_mode_generates_first_statement_sql() {
        let mode = TransactionMode::new(
            TransactionIsolation::Serializable,
            TransactionAccessMode::ReadWrite,
        );

        assert_eq!(
            DatabaseConnection::transaction_mode_statements_for(DatabaseType::Oracle, mode)
                .expect("Oracle mode should be supported"),
            vec!["SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"]
        );
        assert!(DatabaseType::Oracle.transaction_mode_requires_first_statement(mode));
    }

    #[test]
    fn oracle_transaction_mode_generates_read_only_sql() {
        let mode = TransactionMode::new(
            TransactionIsolation::Default,
            TransactionAccessMode::ReadOnly,
        );

        assert_eq!(
            DatabaseConnection::transaction_mode_statements_for(DatabaseType::Oracle, mode)
                .expect("Oracle read-only mode should be supported"),
            vec!["SET TRANSACTION READ ONLY"]
        );
        assert!(DatabaseType::Oracle.transaction_mode_requires_first_statement(mode));
    }

    #[test]
    fn oracle_transaction_mode_rejects_read_only_with_explicit_isolation() {
        let mode = TransactionMode::new(
            TransactionIsolation::Serializable,
            TransactionAccessMode::ReadOnly,
        );

        let err = DatabaseConnection::transaction_mode_statements_for(DatabaseType::Oracle, mode)
            .expect_err("Oracle cannot combine read-only and explicit isolation");
        assert!(err.contains("READ ONLY"));
        assert!(err.contains("isolation"));
    }

    #[test]
    fn oracle_transaction_mode_rejects_unsupported_isolation() {
        let mode = TransactionMode::new(
            TransactionIsolation::RepeatableRead,
            TransactionAccessMode::ReadWrite,
        );

        assert!(
            DatabaseConnection::transaction_mode_statements_for(DatabaseType::Oracle, mode)
                .is_err()
        );
    }

    #[test]
    fn mysql_transaction_mode_generates_session_sql() {
        let mode = TransactionMode::new(
            TransactionIsolation::ReadCommitted,
            TransactionAccessMode::ReadOnly,
        );

        assert_eq!(
            DatabaseConnection::transaction_mode_statements_for(DatabaseType::MySQL, mode)
                .expect("MySQL/MariaDB mode should be supported"),
            vec!["SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY"]
        );
    }

    #[test]
    fn mysql_default_transaction_mode_resets_access_mode_to_read_write() {
        assert_eq!(
            DatabaseConnection::transaction_mode_statements_for(
                DatabaseType::MySQL,
                TransactionMode::default()
            )
            .expect("MySQL/MariaDB default mode should be supported"),
            vec!["SET SESSION TRANSACTION READ WRITE"]
        );
    }

    #[test]
    fn transaction_isolation_parses_database_reported_values() {
        assert_eq!(
            TransactionIsolation::from_sql_level("READ-COMMITTED"),
            Some(TransactionIsolation::ReadCommitted)
        );
        assert_eq!(
            TransactionIsolation::from_sql_level("read_commited"),
            Some(TransactionIsolation::ReadCommitted)
        );
        assert_eq!(
            TransactionIsolation::from_sql_level("REPEATABLE-READ"),
            Some(TransactionIsolation::RepeatableRead)
        );
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
    fn advanced_defaults_preserve_existing_db_specific_session_settings() {
        let oracle = ConnectionAdvancedSettings::default_for(DatabaseType::Oracle);
        assert_eq!(
            oracle.default_transaction_isolation,
            TransactionIsolation::ReadCommitted
        );
        assert_eq!(
            oracle.default_transaction_access_mode,
            TransactionAccessMode::ReadWrite
        );
        assert!(oracle.session_time_zone.is_empty());
        assert_eq!(
            oracle.oracle_nls_timestamp_format,
            "yyyy-mm-dd hh24:mi:ss.ff6"
        );
        assert_eq!(oracle.oracle_nls_date_format, "yyyy-mm-dd hh24:mi:ss");

        let mysql = ConnectionAdvancedSettings::default_for(DatabaseType::MySQL);
        assert_eq!(
            mysql.default_transaction_isolation,
            TransactionIsolation::ReadCommitted
        );
        assert_eq!(
            mysql.default_transaction_access_mode,
            TransactionAccessMode::ReadWrite
        );
        assert_eq!(mysql.session_time_zone, "+00:00");
        assert_eq!(mysql.mysql_sql_mode, "TRADITIONAL");
        assert_eq!(mysql.mysql_charset, "utf8mb4");
    }

    #[test]
    fn sync_default_transaction_isolation_trusts_applied_advanced_setting() {
        let mut connection = DatabaseConnection::new();
        connection.info = ConnectionInfo::new_with_type(
            "local",
            "system",
            "pw",
            "localhost",
            1521,
            "FREE",
            DatabaseType::Oracle,
        );
        connection.info.advanced.default_transaction_isolation = TransactionIsolation::Serializable;

        connection.sync_default_transaction_isolation(DatabaseType::Oracle);

        assert_eq!(
            connection.default_transaction_isolation(),
            TransactionIsolation::Serializable
        );
    }

    #[test]
    fn oracle_advanced_session_statements_use_configured_values() {
        let mut advanced = ConnectionAdvancedSettings::default_for(DatabaseType::Oracle);
        advanced.default_transaction_isolation = TransactionIsolation::Serializable;
        advanced.session_time_zone = "+09:00".to_string();
        advanced.oracle_nls_date_format = "YYYY/MM/DD HH24:MI:SS".to_string();
        advanced.oracle_nls_timestamp_format = "YYYY/MM/DD HH24:MI:SS.FF3".to_string();

        assert_eq!(
            DatabaseConnection::oracle_session_setting_statements(&advanced),
            vec![
                "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY/MM/DD HH24:MI:SS.FF3'",
                "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY/MM/DD HH24:MI:SS'",
                "ALTER SESSION SET ISOLATION_LEVEL = SERIALIZABLE",
                "ALTER SESSION SET TIME_ZONE = '+09:00'",
            ]
        );
    }

    #[test]
    fn mysql_advanced_session_statements_use_configured_values() {
        let mut advanced = ConnectionAdvancedSettings::default_for(DatabaseType::MySQL);
        advanced.default_transaction_isolation = TransactionIsolation::RepeatableRead;
        advanced.session_time_zone = "+09:00".to_string();
        advanced.mysql_sql_mode = "ANSI_QUOTES,STRICT_TRANS_TABLES".to_string();

        assert_eq!(
            DatabaseConnection::mysql_session_setting_statements(&advanced),
            vec![
                "SET SESSION sql_mode = 'ANSI_QUOTES,STRICT_TRANS_TABLES'",
                "SET SESSION time_zone = '+09:00'",
                "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            ]
        );
    }

    #[test]
    fn oracle_direct_connection_string_uses_tcps_for_ssl_or_protocol() {
        let mut info = ConnectionInfo::new_with_type(
            "local",
            "system",
            "pw",
            "localhost",
            2484,
            "FREE",
            DatabaseType::Oracle,
        );
        info.advanced.ssl_mode = ConnectionSslMode::Required;

        assert_eq!(
            info.connection_string(),
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=localhost)(PORT=2484))(CONNECT_DATA=(SERVICE_NAME=FREE)))"
        );

        info.advanced.ssl_mode = ConnectionSslMode::Disabled;
        info.advanced.oracle_protocol = OracleNetworkProtocol::Tcps;
        assert_eq!(
            info.connection_string(),
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=localhost)(PORT=2484))(CONNECT_DATA=(SERVICE_NAME=FREE)))"
        );
    }

    #[test]
    fn mysql_driver_ssl_options_follow_advanced_mode() {
        let mut info = ConnectionInfo::new_with_type(
            "local",
            "root",
            "pw",
            "localhost",
            3306,
            "initial_db",
            DatabaseType::MySQL,
        );
        let opts = mysql::Opts::from(DatabaseConnection::build_mysql_opts(&info));
        assert!(opts.get_ssl_opts().is_none());

        info.advanced.ssl_mode = ConnectionSslMode::Required;
        let opts = mysql::Opts::from(DatabaseConnection::build_mysql_opts(&info));
        let ssl = opts.get_ssl_opts().expect("required SSL should be enabled");
        assert!(ssl.skip_domain_validation());
        assert!(ssl.accept_invalid_certs());

        info.advanced.ssl_mode = ConnectionSslMode::VerifyCa;
        info.advanced.mysql_ssl_ca_path = "/tmp/mysql-ca.pem".to_string();
        let opts = mysql::Opts::from(DatabaseConnection::build_mysql_opts(&info));
        let ssl = opts.get_ssl_opts().expect("Verify CA should enable SSL");
        assert!(ssl.skip_domain_validation());
        assert!(!ssl.accept_invalid_certs());
        assert_eq!(
            ssl.root_cert_path(),
            Some(std::path::Path::new("/tmp/mysql-ca.pem"))
        );

        info.advanced.ssl_mode = ConnectionSslMode::VerifyIdentity;
        let opts = mysql::Opts::from(DatabaseConnection::build_mysql_opts(&info));
        let ssl = opts
            .get_ssl_opts()
            .expect("Verify identity should enable SSL");
        assert!(!ssl.skip_domain_validation());
        assert!(!ssl.accept_invalid_certs());
    }

    #[test]
    fn advanced_validation_rejects_unsafe_values() {
        let mut mysql = ConnectionAdvancedSettings::default_for(DatabaseType::MySQL);
        mysql.session_time_zone = "UTC".to_string();
        assert!(mysql.validate_for_db(DatabaseType::MySQL, false).is_err());

        mysql.session_time_zone = "+00:00".to_string();
        mysql.mysql_sql_mode = "TRADITIONAL;DROP".to_string();
        assert!(mysql.validate_for_db(DatabaseType::MySQL, false).is_err());

        let mut oracle = ConnectionAdvancedSettings::default_for(DatabaseType::Oracle);
        oracle.ssl_mode = ConnectionSslMode::Required;
        oracle.oracle_protocol = OracleNetworkProtocol::Tcps;
        assert!(oracle.validate_for_db(DatabaseType::Oracle, true).is_ok());

        oracle.ssl_mode = ConnectionSslMode::VerifyCa;
        assert!(oracle.validate_for_db(DatabaseType::Oracle, false).is_err());
    }

    #[test]
    fn oracle_advanced_validation_rejects_read_only_with_explicit_isolation() {
        let mut oracle = ConnectionAdvancedSettings::default_for(DatabaseType::Oracle);
        oracle.default_transaction_access_mode = TransactionAccessMode::ReadOnly;
        oracle.default_transaction_isolation = TransactionIsolation::ReadCommitted;

        let err = oracle
            .validate_for_db(DatabaseType::Oracle, false)
            .expect_err("Oracle READ ONLY must not be combined with explicit isolation");

        assert!(err.contains("combining READ ONLY with an explicit transaction isolation level"));

        oracle.default_transaction_isolation = TransactionIsolation::Default;
        assert!(oracle.validate_for_db(DatabaseType::Oracle, false).is_ok());
    }

    #[test]
    fn mysql_advanced_validation_allows_read_only_with_explicit_isolation() {
        let mut mysql = ConnectionAdvancedSettings::default_for(DatabaseType::MySQL);
        mysql.default_transaction_access_mode = TransactionAccessMode::ReadOnly;
        mysql.default_transaction_isolation = TransactionIsolation::ReadCommitted;

        assert!(mysql.validate_for_db(DatabaseType::MySQL, false).is_ok());
    }

    #[test]
    fn session_time_zone_validation_matches_database_ranges() {
        let mut mysql = ConnectionAdvancedSettings::default_for(DatabaseType::MySQL);
        mysql.session_time_zone = "+14:00".to_string();
        assert!(mysql.validate_for_db(DatabaseType::MySQL, false).is_ok());
        mysql.session_time_zone = "-13:59".to_string();
        assert!(mysql.validate_for_db(DatabaseType::MySQL, false).is_ok());
        mysql.session_time_zone = "+14:01".to_string();
        assert!(mysql.validate_for_db(DatabaseType::MySQL, false).is_err());
        mysql.session_time_zone = "-14:00".to_string();
        assert!(mysql.validate_for_db(DatabaseType::MySQL, false).is_err());

        let mut oracle = ConnectionAdvancedSettings::default_for(DatabaseType::Oracle);
        oracle.session_time_zone = "+14:59".to_string();
        assert!(oracle.validate_for_db(DatabaseType::Oracle, false).is_ok());
        oracle.session_time_zone = "-14:59".to_string();
        assert!(oracle.validate_for_db(DatabaseType::Oracle, false).is_ok());
        oracle.session_time_zone = "+15:00".to_string();
        assert!(oracle.validate_for_db(DatabaseType::Oracle, false).is_err());
    }

    #[test]
    fn migrate_for_db_type_drops_session_time_zone_unsupported_by_target_db() {
        let mut oracle = ConnectionAdvancedSettings::default_for(DatabaseType::Oracle);
        oracle.session_time_zone = "+14:59".to_string();

        let migrated = oracle.migrate_for_db_type(DatabaseType::Oracle, DatabaseType::MySQL);

        assert_eq!(
            migrated.session_time_zone,
            ConnectionAdvancedSettings::default_for(DatabaseType::MySQL).session_time_zone
        );
    }

    #[test]
    fn mariadb_time_zone_range_is_narrower_than_mysql() {
        let mysql_only_positive = parse_session_time_zone_offset("+13:01").unwrap();
        assert!(mysql_session_time_zone_in_range(mysql_only_positive));
        assert!(!mariadb_session_time_zone_in_range(mysql_only_positive));

        let mysql_only_negative = parse_session_time_zone_offset("-13:00").unwrap();
        assert!(mysql_session_time_zone_in_range(mysql_only_negative));
        assert!(!mariadb_session_time_zone_in_range(mysql_only_negative));

        assert!(mariadb_session_time_zone_in_range(
            parse_session_time_zone_offset("+13:00").unwrap()
        ));
        assert!(mariadb_session_time_zone_in_range(
            parse_session_time_zone_offset("-12:59").unwrap()
        ));
    }

    #[test]
    fn mysql_server_version_time_zone_validation_handles_mariadb_only_limits() {
        assert!(
            DatabaseConnection::validate_mysql_session_time_zone_for_server_version(
                "+13:01", "8.0.46"
            )
            .is_ok()
        );
        assert!(
            DatabaseConnection::validate_mysql_session_time_zone_for_server_version(
                "-13:00", "8.0.46"
            )
            .is_ok()
        );

        let positive_err = DatabaseConnection::validate_mysql_session_time_zone_for_server_version(
            "+13:01",
            "12.2.2-MariaDB",
        )
        .expect_err("MariaDB should reject offsets above +13:00");
        assert!(positive_err.contains("outside MariaDB's supported offset range"));

        let negative_err = DatabaseConnection::validate_mysql_session_time_zone_for_server_version(
            "-13:00",
            "12.2.2-MariaDB",
        )
        .expect_err("MariaDB should reject offsets below -12:59");
        assert!(negative_err.contains("outside MariaDB's supported offset range"));
    }

    #[test]
    fn mysql_advanced_validation_rejects_charset_collation_mismatch() {
        let mut mysql = ConnectionAdvancedSettings::default_for(DatabaseType::MySQL);
        mysql.mysql_charset = "utf8mb4".to_string();
        mysql.mysql_collation = "latin1_swedish_ci".to_string();

        let err = mysql
            .validate_for_db(DatabaseType::MySQL, false)
            .expect_err("collation must belong to the selected character set");

        assert!(err.contains("does not match character set"));
    }

    #[test]
    fn mysql_advanced_validation_accepts_utf8_utf8mb3_alias_collations() {
        let mut mysql = ConnectionAdvancedSettings::default_for(DatabaseType::MySQL);

        mysql.mysql_charset = "utf8".to_string();
        mysql.mysql_collation = "utf8mb3_general_ci".to_string();
        assert!(mysql.validate_for_db(DatabaseType::MySQL, false).is_ok());

        mysql.mysql_charset = "utf8mb3".to_string();
        mysql.mysql_collation = "utf8_general_ci".to_string();
        assert!(mysql.validate_for_db(DatabaseType::MySQL, false).is_ok());
    }

    #[test]
    fn mysql_advanced_validation_accepts_binary_charset_collation() {
        let mut mysql = ConnectionAdvancedSettings::default_for(DatabaseType::MySQL);
        mysql.mysql_charset = "binary".to_string();
        mysql.mysql_collation = "binary".to_string();

        assert!(mysql.validate_for_db(DatabaseType::MySQL, false).is_ok());
    }

    #[test]
    fn mysql_set_names_statement_uses_configured_charset_and_collation() {
        let mut advanced = ConnectionAdvancedSettings::default_for(DatabaseType::MySQL);
        advanced.mysql_charset = "utf8mb4".to_string();
        advanced.mysql_collation = "utf8mb4_0900_ai_ci".to_string();

        assert_eq!(
            DatabaseConnection::mysql_set_names_statement_with_settings(
                Some("utf8mb4_unicode_ci"),
                &advanced,
            ),
            "SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci"
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
    fn mysql_set_names_statement_matches_database_collation_case_insensitively() {
        let mut advanced = ConnectionAdvancedSettings::default_for(DatabaseType::MySQL);
        advanced.mysql_charset = "UTF8MB4".to_string();

        assert_eq!(
            DatabaseConnection::mysql_set_names_statement_with_settings(
                Some("utf8mb4_unicode_ci"),
                &advanced,
            ),
            "SET NAMES UTF8MB4 COLLATE utf8mb4_unicode_ci"
        );
    }

    #[test]
    fn mysql_set_names_statement_accepts_utf8_utf8mb3_alias_collations() {
        let mut advanced = ConnectionAdvancedSettings::default_for(DatabaseType::MySQL);
        advanced.mysql_charset = "utf8".to_string();

        assert_eq!(
            DatabaseConnection::mysql_set_names_statement_with_settings(
                Some("utf8mb3_general_ci"),
                &advanced,
            ),
            "SET NAMES utf8 COLLATE utf8mb3_general_ci"
        );

        advanced.mysql_charset = "utf8mb3".to_string();

        assert_eq!(
            DatabaseConnection::mysql_set_names_statement_with_settings(
                Some("utf8_general_ci"),
                &advanced,
            ),
            "SET NAMES utf8mb3 COLLATE utf8_general_ci"
        );
    }

    #[test]
    fn mysql_set_names_statement_accepts_binary_database_collation() {
        let mut advanced = ConnectionAdvancedSettings::default_for(DatabaseType::MySQL);
        advanced.mysql_charset = "binary".to_string();

        assert_eq!(
            DatabaseConnection::mysql_set_names_statement_with_settings(Some("binary"), &advanced,),
            "SET NAMES binary COLLATE binary"
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
    fn oracle_set_current_schema_statement_keeps_simple_identifier_unquoted() {
        assert_eq!(
            DatabaseConnection::oracle_set_current_schema_statement("SCOTT"),
            "ALTER SESSION SET CURRENT_SCHEMA = SCOTT"
        );
    }

    #[test]
    fn oracle_set_current_schema_statement_quotes_schema_when_needed() {
        assert_eq!(
            DatabaseConnection::oracle_set_current_schema_statement("Sales Ops"),
            r#"ALTER SESSION SET CURRENT_SCHEMA = "Sales Ops""#
        );
    }

    #[test]
    fn normalize_oracle_current_schema_name_trims_blank_values() {
        assert_eq!(
            DatabaseConnection::normalize_oracle_current_schema_name("   "),
            None
        );
        assert_eq!(
            DatabaseConnection::normalize_oracle_current_schema_name(" sys "),
            Some("sys".to_string())
        );
    }

    #[test]
    fn disconnect_clears_tracked_oracle_current_schema() {
        let mut conn = DatabaseConnection::new();
        conn.info = ConnectionInfo::new("Prod", "scott", "pw", "db", 1521, "FREE");
        conn.connected = true;
        conn.oracle_current_schema = Some("SYS".to_string());

        conn.disconnect();

        assert!(conn.oracle_current_schema.is_none());
    }

    #[test]
    fn mysql_pool_timeout_error_gets_actionable_exhaustion_message() {
        let message = DbConnectionPool::format_mysql_pool_acquire_error(
            &mysql::Error::DriverError(mysql::DriverError::Timeout),
        );

        assert!(message.contains("MySQL connection pool appears exhausted"));
    }

    #[test]
    fn mysql_network_timeout_error_is_not_reported_as_pool_exhaustion() {
        let err = mysql::Error::IoError(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "Operation timed out",
        ));
        let message = DbConnectionPool::format_mysql_pool_acquire_error(&err);

        assert!(!message.contains("MySQL connection pool appears exhausted"));
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
    #[ignore = "requires local Oracle XE plus ORACLE_TEST_* environment variables"]
    fn oracle_connect_sets_read_committed_as_default_transaction_isolation() {
        let mut connection = DatabaseConnection::new();
        connection
            .connect(oracle_test_connection_info_from_env())
            .expect("Direct localhost Oracle connection should succeed");

        assert_eq!(
            connection.default_transaction_isolation(),
            TransactionIsolation::ReadCommitted
        );
        assert_eq!(connection.transaction_mode(), TransactionMode::default());
    }

    #[test]
    #[ignore = "requires local Oracle XE plus ORACLE_TEST_* environment variables"]
    fn oracle_connect_applies_advanced_session_settings_from_local_xe() {
        let mut info = oracle_test_connection_info_from_env();
        info.advanced.default_transaction_isolation = TransactionIsolation::Serializable;
        info.advanced.session_time_zone = "+09:00".to_string();
        info.advanced.oracle_nls_date_format = "YYYY-MM-DD HH24:MI:SS".to_string();
        info.advanced.oracle_nls_timestamp_format = "YYYY-MM-DD HH24:MI:SS.FF3".to_string();

        let mut connection = DatabaseConnection::new();
        connection
            .connect(info)
            .expect("Direct localhost Oracle connection should succeed");
        let conn = connection
            .require_live_connection()
            .expect("Oracle connection should be live");

        assert_eq!(
            DatabaseConnection::read_oracle_default_transaction_isolation(conn.as_ref())
                .expect("read Oracle current transaction isolation"),
            Some(TransactionIsolation::Serializable)
        );
        assert_eq!(
            read_oracle_session_parameter(conn.as_ref(), "NLS_DATE_FORMAT"),
            "YYYY-MM-DD HH24:MI:SS"
        );
        assert_eq!(
            read_oracle_session_parameter(conn.as_ref(), "NLS_TIMESTAMP_FORMAT"),
            "YYYY-MM-DD HH24:MI:SS.FF3"
        );
        assert_eq!(read_oracle_session_time_zone(conn.as_ref()), "+09:00");
    }

    #[test]
    #[ignore = "requires local Oracle XE plus ORACLE_TEST_* environment variables"]
    fn oracle_pool_session_applies_advanced_session_settings_from_local_xe() {
        let mut info = oracle_test_connection_info_from_env();
        info.advanced.default_transaction_isolation = TransactionIsolation::Serializable;
        info.advanced.session_time_zone = "+09:00".to_string();
        info.advanced.oracle_nls_date_format = "YYYY-MM-DD HH24:MI:SS".to_string();
        info.advanced.oracle_nls_timestamp_format = "YYYY-MM-DD HH24:MI:SS.FF3".to_string();

        let mut connection = DatabaseConnection::new();
        connection
            .connect(info)
            .expect("Direct localhost Oracle connection should succeed");

        let Some(DbPoolSession::Oracle(conn)) = connection
            .acquire_pool_session()
            .expect("Oracle pool session should be acquired")
        else {
            panic!("expected Oracle pool session");
        };

        assert_eq!(
            DatabaseConnection::read_oracle_default_transaction_isolation(&conn)
                .expect("read Oracle current transaction isolation"),
            Some(TransactionIsolation::Serializable)
        );
        assert_eq!(
            read_oracle_session_parameter(&conn, "NLS_DATE_FORMAT"),
            "YYYY-MM-DD HH24:MI:SS"
        );
        assert_eq!(
            read_oracle_session_parameter(&conn, "NLS_TIMESTAMP_FORMAT"),
            "YYYY-MM-DD HH24:MI:SS.FF3"
        );
        assert_eq!(read_oracle_session_time_zone(&conn), "+09:00");
    }

    #[test]
    #[ignore = "requires local Oracle TCPS listener plus ORACLE_TEST_* environment variables"]
    fn oracle_tcps_connection_uses_advanced_ssl_protocol() {
        let mut info = oracle_test_connection_info_from_env();
        info.port = std::env::var("ORACLE_TEST_TCPS_PORT")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(2484);
        info.advanced.ssl_mode = ConnectionSslMode::Required;

        DatabaseConnection::test_connection(&info)
            .expect("Oracle TCPS connection should succeed against configured listener");
    }

    #[test]
    #[ignore = "requires local Oracle XE plus ORACLE_TEST_* environment variables"]
    fn oracle_transaction_mode_applies_every_supported_isolation_from_local_xe() {
        let mut connection = DatabaseConnection::new();
        connection
            .connect(oracle_test_connection_info_from_env())
            .expect("Direct localhost Oracle connection should succeed");
        let conn = connection
            .require_live_connection()
            .expect("Oracle connection should be live");

        for isolation in [
            TransactionIsolation::ReadCommitted,
            TransactionIsolation::Serializable,
        ] {
            DatabaseConnection::apply_oracle_transaction_mode(
                conn.as_ref(),
                TransactionMode::new(isolation, TransactionAccessMode::ReadWrite),
            )
            .unwrap_or_else(|err| panic!("Oracle should apply {}: {err}", isolation.label()));

            let observed =
                DatabaseConnection::read_oracle_default_transaction_isolation(conn.as_ref())
                    .expect("read Oracle current transaction isolation")
                    .expect("Oracle should report a transaction isolation");
            assert_eq!(observed, isolation);
            let _ = conn.rollback();
        }
    }

    #[test]
    #[ignore = "requires local Oracle XE plus ORACLE_TEST_* environment variables"]
    fn oracle_transaction_mode_serializable_applies_from_local_xe() {
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

        let mut connection = DatabaseConnection::new();
        connection
            .connect(ConnectionInfo::new_with_type(
                "local",
                &username,
                &password,
                &host,
                port,
                &service_name,
                DatabaseType::Oracle,
            ))
            .expect("Direct localhost Oracle connection should succeed");
        let conn = connection
            .require_live_connection()
            .expect("Oracle connection should be live");

        DatabaseConnection::apply_oracle_transaction_mode(
            conn.as_ref(),
            TransactionMode::new(
                TransactionIsolation::Serializable,
                TransactionAccessMode::ReadWrite,
            ),
        )
        .expect("Oracle serializable transaction mode should apply");

        let mut stmt = conn
            .statement("SELECT 1 FROM dual")
            .build()
            .expect("build serializable probe statement");
        let value = stmt
            .query_row_as::<i64>(&[])
            .expect("serializable transaction should allow SELECT");
        assert_eq!(value, 1);
        let _ = conn.rollback();
    }

    #[test]
    #[ignore = "requires local Oracle XE plus ORACLE_TEST_* environment variables"]
    fn oracle_transaction_mode_read_only_blocks_dml_from_local_xe() {
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

        {
            let mut setup = DatabaseConnection::new();
            setup
                .connect(info.clone())
                .expect("Direct localhost Oracle connection should succeed");
            let conn = setup
                .require_live_connection()
                .expect("Oracle setup connection should be live");
            let _ = conn.execute("DROP TABLE qt_tx_mode_probe PURGE", &[]);
            conn.execute("CREATE TABLE qt_tx_mode_probe (id NUMBER)", &[])
                .expect("create transaction mode probe table");
            conn.commit().expect("commit probe table DDL");
        }

        {
            let mut connection = DatabaseConnection::new();
            connection
                .connect(info.clone())
                .expect("Direct localhost Oracle connection should succeed");
            let conn = connection
                .require_live_connection()
                .expect("Oracle connection should be live");

            DatabaseConnection::apply_oracle_transaction_mode(
                conn.as_ref(),
                TransactionMode::new(
                    TransactionIsolation::Default,
                    TransactionAccessMode::ReadOnly,
                ),
            )
            .expect("Oracle transaction mode should apply");

            let mut stmt = conn
                .statement("SELECT 1 FROM dual")
                .build()
                .expect("build read probe statement");
            let value = stmt
                .query_row_as::<i64>(&[])
                .expect("read-only transaction should allow SELECT");
            assert_eq!(value, 1);
            drop(stmt);

            let insert_err = conn
                .execute("INSERT INTO qt_tx_mode_probe (id) VALUES (1)", &[])
                .expect_err("read-only transaction should reject DML");
            let insert_message = insert_err.to_string();
            assert!(
                insert_message.contains("ORA-01456")
                    || insert_message.to_ascii_lowercase().contains("read only"),
                "unexpected Oracle read-only DML error: {insert_message}"
            );
            let _ = conn.rollback();
        }

        {
            let mut cleanup = DatabaseConnection::new();
            cleanup
                .connect(info)
                .expect("Direct localhost Oracle connection should succeed for cleanup");
            if let Ok(conn) = cleanup.require_live_connection() {
                let _ = conn.execute("DROP TABLE qt_tx_mode_probe PURGE", &[]);
            }
        }
    }

    #[test]
    #[ignore = "requires local Oracle XE plus ORACLE_TEST_* environment variables"]
    fn oracle_read_only_transaction_can_be_reapplied_after_rollback_from_local_xe() {
        let mut connection = DatabaseConnection::new();
        connection
            .connect(oracle_test_connection_info_from_env())
            .expect("Direct localhost Oracle connection should succeed");
        let conn = connection
            .require_live_connection()
            .expect("Oracle connection should be live");
        let read_only_mode = TransactionMode::new(
            TransactionIsolation::Default,
            TransactionAccessMode::ReadOnly,
        );

        for attempt in 1..=2 {
            DatabaseConnection::apply_oracle_transaction_mode(conn.as_ref(), read_only_mode)
                .unwrap_or_else(|err| {
                    panic!("Oracle read-only mode should apply on attempt {attempt}: {err}")
                });

            let mut stmt = conn
                .statement("SELECT 1 FROM dual")
                .build()
                .unwrap_or_else(|err| panic!("build read-only probe on attempt {attempt}: {err}"));
            let value = stmt
                .query_row_as::<i64>(&[])
                .unwrap_or_else(|err| panic!("run read-only probe on attempt {attempt}: {err}"));
            assert_eq!(value, 1);
            drop(stmt);

            conn.rollback().unwrap_or_else(|err| {
                panic!("close read-only transaction on attempt {attempt}: {err}")
            });
        }
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
        let isolation = DatabaseConnection::read_mysql_default_transaction_isolation(&mut conn)
            .expect("read transaction isolation")
            .expect("transaction isolation should be available");

        assert!(sql_mode.contains("STRICT_TRANS_TABLES"));
        assert_eq!(time_zone, "+00:00");
        assert_eq!(character_set_client, "utf8mb4");
        assert_eq!(isolation, TransactionIsolation::ReadCommitted);
    }

    #[test]
    #[ignore = "requires local MySQL or MariaDB test database via SPACE_QUERY_TEST_MYSQL_* env vars"]
    fn mysql_pool_session_applies_advanced_session_settings() {
        let mut info = mysql_test_connection_info_from_env();
        info.advanced.default_transaction_isolation = TransactionIsolation::RepeatableRead;
        info.advanced.session_time_zone = "+09:00".to_string();
        info.advanced.mysql_sql_mode = "ANSI_QUOTES,STRICT_TRANS_TABLES".to_string();
        info.advanced.mysql_charset = "utf8mb4".to_string();
        info.advanced.mysql_collation = "utf8mb4_unicode_ci".to_string();

        let mut connection = DatabaseConnection::new();
        connection
            .connect(info)
            .expect("MySQL/MariaDB connection should succeed");

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
        let collation_connection = conn
            .query_first::<String, _>("SELECT @@SESSION.collation_connection")
            .expect("read collation_connection")
            .unwrap_or_default();
        let isolation = DatabaseConnection::read_mysql_default_transaction_isolation(&mut conn)
            .expect("read transaction isolation")
            .expect("transaction isolation should be available");

        assert!(sql_mode.contains("ANSI_QUOTES"));
        assert!(sql_mode.contains("STRICT_TRANS_TABLES"));
        assert_eq!(time_zone, "+09:00");
        assert_eq!(character_set_client, "utf8mb4");
        assert_eq!(collation_connection, "utf8mb4_unicode_ci");
        assert_eq!(isolation, TransactionIsolation::RepeatableRead);
    }

    #[test]
    #[ignore = "requires local MySQL or MariaDB test database via SPACE_QUERY_TEST_MYSQL_* env vars"]
    fn mysql_connect_applies_advanced_session_settings() {
        let mut info = mysql_test_connection_info_from_env();
        info.advanced.default_transaction_isolation = TransactionIsolation::RepeatableRead;
        info.advanced.default_transaction_access_mode = TransactionAccessMode::ReadOnly;
        info.advanced.session_time_zone = "+09:00".to_string();
        info.advanced.mysql_sql_mode = "ANSI_QUOTES,STRICT_TRANS_TABLES".to_string();
        info.advanced.mysql_charset = "utf8mb4".to_string();
        info.advanced.mysql_collation = "utf8mb4_unicode_ci".to_string();

        let mut connection = DatabaseConnection::new();
        connection
            .connect(info)
            .expect("MySQL/MariaDB connection should succeed");
        assert_eq!(
            connection.default_transaction_isolation(),
            TransactionIsolation::RepeatableRead
        );
        assert_eq!(
            connection.transaction_mode(),
            TransactionMode::new(
                TransactionIsolation::Default,
                TransactionAccessMode::ReadOnly
            )
        );

        let conn = connection
            .get_mysql_connection_mut()
            .expect("MySQL connection should be live");
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
        let collation_connection = conn
            .query_first::<String, _>("SELECT @@SESSION.collation_connection")
            .expect("read collation_connection")
            .unwrap_or_default();
        let isolation = DatabaseConnection::read_mysql_default_transaction_isolation(conn)
            .expect("read transaction isolation")
            .expect("transaction isolation should be available");

        assert!(sql_mode.contains("ANSI_QUOTES"));
        assert!(sql_mode.contains("STRICT_TRANS_TABLES"));
        assert_eq!(time_zone, "+09:00");
        assert_eq!(character_set_client, "utf8mb4");
        assert_eq!(collation_connection, "utf8mb4_unicode_ci");
        assert_eq!(isolation, TransactionIsolation::RepeatableRead);
    }

    #[test]
    #[ignore = "requires local MySQL or MariaDB test database via SPACE_QUERY_TEST_MYSQL_* env vars"]
    fn mysql_connect_reports_invalid_advanced_session_setting() {
        let mut info = mysql_test_connection_info_from_env();
        info.advanced.mysql_collation = "utf8mb4_not_a_real_ci".to_string();

        let mut connection = DatabaseConnection::new();
        let err = connection
            .connect(info)
            .expect_err("invalid collation should fail connection setup");

        assert!(err.contains("Failed to apply MySQL session setting"));
        assert!(err.contains("SET NAMES"));
    }

    #[test]
    #[ignore = "requires MySQL or MariaDB TLS config via SPACE_QUERY_TEST_MYSQL_* env vars"]
    fn mysql_ssl_required_connects_when_server_tls_is_configured() {
        let mut info = mysql_test_connection_info_from_env();
        info.advanced.ssl_mode = ConnectionSslMode::Required;
        if let Ok(ca_path) = std::env::var("SPACE_QUERY_TEST_MYSQL_SSL_CA") {
            info.advanced.ssl_mode = ConnectionSslMode::VerifyCa;
            info.advanced.mysql_ssl_ca_path = ca_path;
        }

        let mut connection = DatabaseConnection::new();
        connection
            .connect(info)
            .expect("MySQL/MariaDB TLS connection should succeed");
        let conn = connection
            .get_mysql_connection_mut()
            .expect("MySQL connection should be live");
        let ssl_cipher = conn
            .query_first::<(String, String), _>("SHOW STATUS LIKE 'Ssl_cipher'")
            .expect("read SSL cipher")
            .map(|(_, value)| value)
            .unwrap_or_default();

        assert!(!ssl_cipher.is_empty());
    }

    #[test]
    #[ignore = "requires local MySQL or MariaDB test database via SPACE_QUERY_TEST_MYSQL_* env vars"]
    fn mysql_connect_sets_read_committed_as_default_transaction_isolation() {
        let mut connection = DatabaseConnection::new();
        connection
            .connect(mysql_test_connection_info_from_env())
            .expect("MySQL/MariaDB connection should succeed");

        assert_eq!(
            connection.default_transaction_isolation(),
            TransactionIsolation::ReadCommitted
        );
        assert_eq!(connection.transaction_mode(), TransactionMode::default());
    }

    #[test]
    #[ignore = "requires local MySQL or MariaDB test database via SPACE_QUERY_TEST_MYSQL_* env vars"]
    fn mysql_transaction_mode_applies_every_supported_isolation() {
        let mut connection = DatabaseConnection::new();
        connection
            .connect(mysql_test_connection_info_from_env())
            .expect("MySQL/MariaDB connection should succeed");
        let conn = connection
            .get_mysql_connection_mut()
            .expect("MySQL connection should be live");

        for isolation in [
            TransactionIsolation::ReadUncommitted,
            TransactionIsolation::ReadCommitted,
            TransactionIsolation::RepeatableRead,
            TransactionIsolation::Serializable,
        ] {
            DatabaseConnection::apply_mysql_transaction_mode(
                conn,
                TransactionMode::new(isolation, TransactionAccessMode::ReadWrite),
            )
            .unwrap_or_else(|err| {
                panic!("MySQL/MariaDB should apply {}: {err}", isolation.label())
            });

            let observed = DatabaseConnection::read_mysql_default_transaction_isolation(conn)
                .expect("read MySQL/MariaDB transaction isolation")
                .expect("MySQL/MariaDB should report a transaction isolation");
            assert_eq!(observed, isolation);
        }
    }

    #[test]
    #[ignore = "requires local MySQL or MariaDB test database via SPACE_QUERY_TEST_MYSQL_* env vars"]
    fn mysql_read_only_transaction_mode_blocks_dml() {
        let mut connection = DatabaseConnection::new();
        connection
            .set_auto_commit(true)
            .expect("set initial MySQL/MariaDB auto-commit");
        connection
            .connect(mysql_test_connection_info_from_env())
            .expect("MySQL/MariaDB connection should succeed");
        let conn = connection
            .get_mysql_connection_mut()
            .expect("MySQL connection should be live");

        let _ = conn.query_drop("DROP TABLE IF EXISTS qt_tx_mode_probe_mysql");
        conn.query_drop("CREATE TABLE qt_tx_mode_probe_mysql (id INT)")
            .expect("create transaction mode probe table");

        DatabaseConnection::apply_mysql_transaction_mode(
            conn,
            TransactionMode::new(
                TransactionIsolation::ReadCommitted,
                TransactionAccessMode::ReadOnly,
            ),
        )
        .expect("MySQL/MariaDB read-only mode should apply");

        let insert_err = conn
            .query_drop("INSERT INTO qt_tx_mode_probe_mysql (id) VALUES (1)")
            .expect_err("read-only transaction should reject DML");
        let insert_message = insert_err.to_string();
        assert!(
            insert_message.to_ascii_lowercase().contains("read only")
                || insert_message.contains("1792"),
            "unexpected MySQL/MariaDB read-only DML error: {insert_message}"
        );

        let _ = conn.query_drop("ROLLBACK");
        let _ = conn.query_drop("SET SESSION TRANSACTION READ WRITE");
        let _ = conn.query_drop("DROP TABLE IF EXISTS qt_tx_mode_probe_mysql");
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
