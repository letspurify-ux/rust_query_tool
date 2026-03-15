use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fs;
use std::path::PathBuf;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use crate::db::ConnectionInfo;
use crate::utils::credential_store;

const APP_DIR_NAME: &str = "space_query";
const LEGACY_APP_DIR_NAME: &str = "oracle_query_tool";
const MAX_RECENT_CONNECTIONS: usize = 50;
const MAX_QUERY_HISTORY_ENTRIES: usize = 100;
const DEFAULT_RESULT_CELL_MAX_CHARS: u32 = 50;

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
            editor_font: "맑은 고딕".to_string(),
            ui_font_size: 16,
            editor_font_size: 16,
            result_font: "맑은 고딕".to_string(),
            result_font_size: 16,
            result_cell_max_chars: DEFAULT_RESULT_CELL_MAX_CHARS,
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
        let config = if let Some(path) = Self::config_path() {
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

        if loaded_from_legacy {
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
        self.remove_connection_with(name, credential_store::delete_password)
    }

    pub fn get_all_connections(&self) -> &Vec<ConnectionInfo> {
        &self.recent_connections
    }
}

impl AppConfig {
    fn remove_connection_with<F>(&mut self, name: &str, delete_password: F) -> Result<(), String>
    where
        F: FnOnce(&str) -> Result<(), String>,
    {
        let removed = self.recent_connections.iter().any(|c| c.name == name);
        self.recent_connections.retain(|c| c.name != name);

        if self.last_connection.as_deref() == Some(name) {
            self.last_connection = None;
        }

        if !removed {
            return Ok(());
        }

        // Remove password from OS keyring after config list cleanup.
        // Keyring failures are logged but do not block removal from config.
        if let Err(err) = delete_password(name) {
            crate::utils::logging::log_warning(
                "config",
                &format!(
                    "Connection removed from config, but failed to remove password from keyring: {err}"
                ),
            );
            eprintln!(
                "Connection removed from config, but failed to remove password from keyring: {}",
                err
            );
        }

        Ok(())
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryHistory {
    pub queries: VecDeque<QueryHistoryEntry>,
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
    pub fn new() -> Self {
        Self {
            queries: VecDeque::new(),
        }
    }

    pub fn load() -> Self {
        Self::new()
    }

    pub fn add_entry(&mut self, entry: QueryHistoryEntry) {
        self.queries.push_front(entry);
        // Keep only last 100 queries
        self.queries.truncate(MAX_QUERY_HISTORY_ENTRIES);
    }
}

impl Default for QueryHistory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::AppConfig;
    use crate::db::ConnectionInfo;

    fn sample_connection(name: &str) -> ConnectionInfo {
        ConnectionInfo {
            name: name.to_string(),
            host: "localhost".to_string(),
            port: 1521,
            service_name: "orcl".to_string(),
            username: "scott".to_string(),
            password: String::new(),
        }
    }

    #[test]
    fn remove_connection_clears_last_selected_connection() {
        let mut config = AppConfig::new();
        config.recent_connections.push(sample_connection("primary"));
        config.last_connection = Some("primary".to_string());

        let result = config.remove_connection_with("primary", |_| Ok(()));

        assert!(result.is_ok());
        assert!(config.recent_connections.is_empty());
        assert!(config.last_connection.is_none());
    }

    #[test]
    fn remove_connection_ignores_keyring_error_after_list_cleanup() {
        let mut config = AppConfig::new();
        config.recent_connections.push(sample_connection("primary"));

        let result =
            config.remove_connection_with("primary", |_| Err("keyring backend unavailable".into()));

        assert!(result.is_ok());
        assert!(config.recent_connections.is_empty());
    }

    #[test]
    fn remove_connection_skips_keyring_delete_when_entry_does_not_exist() {
        let mut config = AppConfig::new();
        config.recent_connections.push(sample_connection("primary"));
        let mut delete_called = false;

        let result = config.remove_connection_with("missing", |_| {
            delete_called = true;
            Ok(())
        });

        assert!(result.is_ok());
        assert!(!delete_called);
        assert_eq!(config.recent_connections.len(), 1);
    }
}
