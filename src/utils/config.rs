use serde::{Deserialize, Serialize};
use std::fs;
use std::io::BufWriter;
use std::path::PathBuf;
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use crate::db::ConnectionInfo;
use crate::utils::credential_store;

const APP_DIR_NAME: &str = "space_query";
const LEGACY_APP_DIR_NAME: &str = "oracle_query_tool";
const MAX_RECENT_CONNECTIONS: usize = 50;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct AppConfig {
    pub recent_connections: Vec<ConnectionInfo>,
    pub last_connection: Option<String>,
    pub editor_font: String,
    pub ui_font_size: u32,
    pub editor_font_size: u32,
    pub result_font: String,
    pub result_font_size: u32,
    pub result_cell_max_chars: u32,
    pub max_rows: u32,
    pub auto_commit: bool,
}

impl AppConfig {
    fn app_file_path(base: Option<PathBuf>, app_dir: &str, file_name: &str) -> Option<PathBuf> {
        base.map(|mut path| {
            path.push(app_dir);
            path.push(file_name);
            path
        })
    }

    fn load_from_path(path: &PathBuf) -> Option<Self> {
        if !path.exists() {
            return None;
        }
        let content = fs::read_to_string(path).ok()?;
        serde_json::from_str(&content).ok()
    }

    pub fn new() -> Self {
        Self {
            recent_connections: Vec::new(),
            last_connection: None,
            editor_font: "Courier".to_string(),
            ui_font_size: 14,
            editor_font_size: 14,
            result_font: "Helvetica".to_string(),
            result_font_size: 14,
            result_cell_max_chars: crate::ui::constants::RESULT_CELL_MAX_DISPLAY_CHARS_DEFAULT,
            max_rows: 1000,
            auto_commit: false,
        }
    }

    pub fn config_path() -> Option<PathBuf> {
        Self::app_file_path(dirs::config_dir(), APP_DIR_NAME, "config.json")
    }

    fn legacy_config_path() -> Option<PathBuf> {
        Self::app_file_path(dirs::config_dir(), LEGACY_APP_DIR_NAME, "config.json")
    }

    pub fn load() -> Self {
        let mut loaded_from_legacy = false;
        let mut config = if let Some(path) = Self::config_path() {
            if let Some(loaded) = Self::load_from_path(&path) {
                loaded
            } else if let Some(legacy_path) = Self::legacy_config_path() {
                if let Some(loaded) = Self::load_from_path(&legacy_path) {
                    loaded_from_legacy = true;
                    loaded
                } else {
                    Self::new()
                }
            } else {
                Self::new()
            }
        } else {
            Self::new()
        };

        // Migrate plain-text passwords from old config to keyring.
        // Passwords are NOT loaded eagerly; use get_password_for_connection() on demand.
        let mut needs_resave = false;
        for conn in &mut config.recent_connections {
            if !conn.password.is_empty() {
                if let Err(e) = credential_store::store_password(&conn.name, &conn.password) {
                    eprintln!("Keyring migration warning: {}", e);
                } else {
                    conn.clear_password();
                    needs_resave = true;
                }
            }
        }

        // Re-save to strip plain-text passwords from config.json
        if needs_resave {
            if let Err(e) = config.save() {
                eprintln!("Failed to re-save config after keyring migration: {}", e);
            }
        } else if loaded_from_legacy {
            // Migrate config location from legacy app folder to new app folder.
            if let Err(e) = config.save() {
                eprintln!("Failed to migrate config path: {}", e);
            }
        }

        config
    }

    pub fn save(&self) -> Result<(), Box<dyn std::error::Error>> {
        let path = Self::config_path().ok_or_else(|| {
            let err = std::io::Error::other("Config directory is unavailable");
            crate::utils::logging::log_error("config", &format!("Config persistence error: {err}"));
            eprintln!("Config persistence error: {err}");
            err
        })?;

        if let Some(parent) = path.parent() {
            match fs::create_dir_all(parent) {
                Ok(()) => {}
                Err(err) => {
                    crate::utils::logging::log_error(
                        "config",
                        &format!("Config persistence error: {err}"),
                    );
                    eprintln!("Config persistence error: {err}");
                    return Err(Box::new(err));
                }
            }
        }
        let content = match serde_json::to_string_pretty(self) {
            Ok(content) => content,
            Err(err) => {
                crate::utils::logging::log_error(
                    "config",
                    &format!("Config persistence error: {err}"),
                );
                eprintln!("Config persistence error: {err}");
                return Err(Box::new(err));
            }
        };
        match fs::write(&path, content) {
            Ok(()) => {}
            Err(err) => {
                crate::utils::logging::log_error(
                    "config",
                    &format!("Config persistence error: {err}"),
                );
                eprintln!("Config persistence error: {err}");
                return Err(Box::new(err));
            }
        }

        // Restrict file permissions to owner-only (0600) on Unix
        #[cfg(unix)]
        {
            let permissions = fs::Permissions::from_mode(0o600);
            if let Err(e) = fs::set_permissions(&path, permissions) {
                eprintln!("Warning: could not set config file permissions: {}", e);
            }
        }
        Ok(())
    }

    pub fn add_recent_connection(&mut self, mut info: ConnectionInfo) -> Result<(), String> {
        // Store password in OS keyring, then clear from memory
        if !info.password.is_empty() {
            credential_store::store_password(&info.name, &info.password)
                .map_err(|e| format!("Failed to store password in keyring: {e}"))?;
        }
        info.clear_password();

        // Remove existing connection with same name
        self.recent_connections.retain(|c| c.name != info.name);

        // Add to front
        self.recent_connections.insert(0, info);

        // Keep only last 10 connections
        self.recent_connections.truncate(MAX_RECENT_CONNECTIONS);
        Ok(())
    }

    pub fn get_connection_by_name(&self, name: &str) -> Option<&ConnectionInfo> {
        self.recent_connections.iter().find(|c| c.name == name)
    }

    /// Retrieve the password for a saved connection from the OS keyring on demand.
    /// Returns None if no password is stored or the connection name is not found.
    pub fn get_password_for_connection(name: &str) -> Result<Option<String>, String> {
        match credential_store::get_password(name) {
            Ok(Some(password)) => Ok(Some(password)),
            Ok(None) => Ok(None),
            Err(e) => Err(format!("Failed to load password from keyring: {e}")),
        }
    }

    pub fn remove_connection(&mut self, name: &str) -> Result<(), String> {
        self.recent_connections.retain(|c| c.name != name);
        // Remove password from OS keyring after config list cleanup so users can
        // still remove stale/broken entries even if keyring backends fail.
        credential_store::delete_password(name)
            .map_err(|e| format!("Connection removed, but failed to remove password from keyring: {e}"))
    }

    pub fn get_all_connections(&self) -> &Vec<ConnectionInfo> {
        &self.recent_connections
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryHistory {
    pub queries: Vec<QueryHistoryEntry>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryHistoryEntry {
    pub sql: String,
    pub timestamp: String,
    pub execution_time_ms: u64,
    pub row_count: usize,
    pub connection_name: String,
    #[serde(default = "default_query_success")]
    pub success: bool,
    #[serde(default)]
    pub error_message: Option<String>,
    #[serde(default)]
    pub error_line: Option<usize>,
}

fn default_query_success() -> bool {
    true
}

impl QueryHistory {
    fn history_path_for(app_dir: &str) -> Option<PathBuf> {
        AppConfig::app_file_path(dirs::data_dir(), app_dir, "history.json")
    }

    pub fn new() -> Self {
        Self {
            queries: Vec::new(),
        }
    }

    pub fn history_path() -> Option<PathBuf> {
        Self::history_path_for(APP_DIR_NAME)
    }

    fn legacy_history_path() -> Option<PathBuf> {
        Self::history_path_for(LEGACY_APP_DIR_NAME)
    }

    fn preserve_corrupt_history_file(path: &PathBuf) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or_default();
        let backup_path = path.with_extension(format!("corrupt.{}.json", timestamp));
        if let Err(err) = fs::rename(path, &backup_path) {
            crate::utils::logging::log_warning(
                "config",
                &format!(
                    "Failed to preserve corrupt history file {}: {}",
                    path.display(),
                    err
                ),
            );
            eprintln!(
                "Failed to preserve corrupt history file {}: {}",
                path.display(),
                err
            );
        } else {
            crate::utils::logging::log_warning(
                "config",
                &format!(
                    "Corrupt history file was moved to {}",
                    backup_path.display()
                ),
            );
            eprintln!(
                "Corrupt history file was moved to {}",
                backup_path.display()
            );
        }
    }

    fn load_from_path(path: &PathBuf, source_label: &str) -> Option<Self> {
        if !path.exists() {
            return None;
        }

        let content = match fs::read_to_string(path) {
            Ok(content) => content,
            Err(err) => {
                crate::utils::logging::log_warning(
                    "config",
                    &format!(
                        "Failed to read {} history file {}: {}",
                        source_label,
                        path.display(),
                        err
                    ),
                );
                eprintln!(
                    "Failed to read {} history file {}: {}",
                    source_label,
                    path.display(),
                    err
                );
                return None;
            }
        };

        match serde_json::from_str::<Self>(&content) {
            Ok(history) => Some(history),
            Err(err) => {
                crate::utils::logging::log_warning(
                    "config",
                    &format!(
                        "Failed to parse {} history file {}: {}",
                        source_label,
                        path.display(),
                        err
                    ),
                );
                eprintln!(
                    "Failed to parse {} history file {}: {}",
                    source_label,
                    path.display(),
                    err
                );
                Self::preserve_corrupt_history_file(path);
                None
            }
        }
    }

    pub fn load() -> Self {
        let mut loaded_from_legacy = false;

        let loaded = if let Some(path) = Self::history_path() {
            if path.exists() {
                Self::load_from_path(&path, "primary")
            } else if let Some(legacy_path) = Self::legacy_history_path() {
                if legacy_path.exists() {
                    loaded_from_legacy = true;
                    Self::load_from_path(&legacy_path, "legacy")
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        if let Some(history) = loaded {
            if loaded_from_legacy {
                if let Err(err) = history.save() {
                    crate::utils::logging::log_warning(
                        "config",
                        &format!("Failed to migrate legacy history path: {err}"),
                    );
                    eprintln!("Failed to migrate legacy history path: {err}");
                }
            }
            return history;
        }

        Self::new()
    }

    pub fn save(&self) -> Result<(), Box<dyn std::error::Error>> {
        let path = Self::history_path().ok_or_else(|| {
            let err = std::io::Error::other("History directory is unavailable");
            crate::utils::logging::log_error(
                "config",
                &format!("History persistence error: {err}"),
            );
            eprintln!("History persistence error: {err}");
            err
        })?;

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                crate::utils::logging::log_error(
                    "config",
                    &format!("History persistence error: {err}"),
                );
                eprintln!("History persistence error: {err}");
                err
            })?;
        }
        // Atomic write: write to temp file first, then rename to avoid
        // data loss if the process crashes mid-write.
        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or_default();
        let tmp_path = path.with_extension(format!("json.tmp.{}.{}", process::id(), now_millis));
        let file = fs::File::create(&tmp_path).map_err(|err| {
            crate::utils::logging::log_error(
                "config",
                &format!("History persistence error: {err}"),
            );
            eprintln!("History persistence error: {err}");
            err
        })?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, self).map_err(|err| {
            crate::utils::logging::log_error(
                "config",
                &format!("History persistence error: {err}"),
            );
            eprintln!("History persistence error: {err}");
            err
        })?;
        // Explicit flush so buffered bytes reach disk before rename.
        use std::io::Write;
        writer.flush().map_err(|err| {
            crate::utils::logging::log_error(
                "config",
                &format!("History persistence error: {err}"),
            );
            eprintln!("History persistence error: {err}");
            err
        })?;
        fs::rename(&tmp_path, &path).map_err(|err| {
            crate::utils::logging::log_error(
                "config",
                &format!("History persistence error: {err}"),
            );
            eprintln!("History persistence error: {err}");
            err
        })?;

        // Restrict file permissions to owner-only (0600) on Unix
        #[cfg(unix)]
        {
            let permissions = fs::Permissions::from_mode(0o600);
            if let Err(err) = fs::set_permissions(&path, permissions) {
                eprintln!(
                    "Warning: could not set query history file permissions: {}",
                    err
                );
            }
        }
        Ok(())
    }

    pub fn add_entry(&mut self, entry: QueryHistoryEntry) {
        self.queries.insert(0, entry);
        // Keep only last 1000 queries
        self.queries.truncate(1000);
    }
}

impl Default for QueryHistory {
    fn default() -> Self {
        Self::new()
    }
}
