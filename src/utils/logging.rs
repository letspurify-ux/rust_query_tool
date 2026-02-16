use chrono::Local;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::{mpsc, Mutex, OnceLock};
use std::thread;

const APP_DIR_NAME: &str = "space_query";
const LOG_FILE_NAME: &str = "app.log.json";
const CRASH_LOG_FILE_NAME: &str = "crash.log";
const MAX_LOG_ENTRIES: usize = 5000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogLevel {
    Debug,
    Info,
    Warning,
    Error,
}

impl LogLevel {
    pub fn label(&self) -> &'static str {
        match self {
            LogLevel::Debug => "DEBUG",
            LogLevel::Info => "INFO",
            LogLevel::Warning => "WARN",
            LogLevel::Error => "ERROR",
        }
    }
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: String,
    pub level: LogLevel,
    pub source: String,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AppLog {
    pub entries: Vec<LogEntry>,
}

impl AppLog {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    fn log_path() -> Option<PathBuf> {
        dirs::data_dir().map(|mut p| {
            p.push(APP_DIR_NAME);
            p.push(LOG_FILE_NAME);
            p
        })
    }

    pub fn load() -> Self {
        if let Some(path) = Self::log_path() {
            if path.exists() {
                if let Ok(content) = fs::read_to_string(&path) {
                    if let Ok(log) = serde_json::from_str::<Self>(&content) {
                        return log;
                    }
                }
            }
        }
        Self::new()
    }

    pub fn save(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(path) = Self::log_path() {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let file = fs::File::create(&path)?;
            let writer = BufWriter::new(file);
            serde_json::to_writer(writer, self)?;
        }
        Ok(())
    }

    pub fn add_entry(&mut self, entry: LogEntry) {
        self.entries.insert(0, entry);
        self.entries.truncate(MAX_LOG_ENTRIES);
    }
}

impl Default for AppLog {
    fn default() -> Self {
        Self::new()
    }
}

enum LogCommand {
    Write(LogEntry),
    Clear,
}

fn log_writer_sender() -> &'static mpsc::Sender<LogCommand> {
    static LOG_WRITER: OnceLock<mpsc::Sender<LogCommand>> = OnceLock::new();
    LOG_WRITER.get_or_init(|| {
        let (sender, receiver) = mpsc::channel::<LogCommand>();
        thread::spawn(move || {
            let mut log = AppLog::load();
            while let Ok(cmd) = receiver.recv() {
                match cmd {
                    LogCommand::Write(entry) => log.add_entry(entry),
                    LogCommand::Clear => log.entries.clear(),
                }
                while let Ok(next) = receiver.try_recv() {
                    match next {
                        LogCommand::Write(entry) => log.add_entry(entry),
                        LogCommand::Clear => log.entries.clear(),
                    }
                }
                if let Err(err) = log.save() {
                    eprintln!("Log save error: {err}");
                }
            }
        });
        sender
    })
}

/// In-memory ring buffer so the UI can show recent entries without
/// re-reading the file every time the dialog is opened.
fn in_memory_log() -> &'static Mutex<Vec<LogEntry>> {
    static BUFFER: OnceLock<Mutex<Vec<LogEntry>>> = OnceLock::new();
    BUFFER.get_or_init(|| {
        let log = AppLog::load();
        Mutex::new(log.entries)
    })
}

pub fn log(level: LogLevel, source: &str, message: &str) {
    let entry = LogEntry {
        timestamp: Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
        level,
        source: source.to_string(),
        message: message.to_string(),
    };

    // Update in-memory buffer
    if let Ok(mut buf) = in_memory_log().lock() {
        buf.insert(0, entry.clone());
        buf.truncate(MAX_LOG_ENTRIES);
    }

    // Persist via background writer
    let _ = log_writer_sender().send(LogCommand::Write(entry));
}

pub fn log_info(source: &str, message: &str) {
    log(LogLevel::Info, source, message);
}

pub fn log_warning(source: &str, message: &str) {
    log(LogLevel::Warning, source, message);
}

pub fn log_error(source: &str, message: &str) {
    log(LogLevel::Error, source, message);
}

#[allow(dead_code)]
pub fn log_debug(source: &str, message: &str) {
    log(LogLevel::Debug, source, message);
}

/// Return a snapshot of the in-memory log entries.
pub fn get_log_entries() -> Vec<LogEntry> {
    in_memory_log()
        .lock()
        .map(|buf| buf.clone())
        .unwrap_or_default()
}

/// Clear all log entries (in-memory + persisted).
pub fn clear_log() {
    if let Ok(mut buf) = in_memory_log().lock() {
        buf.clear();
    }
    let _ = log_writer_sender().send(LogCommand::Clear);
}

// ── Crash log helpers ──

pub fn crash_log_path() -> Option<PathBuf> {
    dirs::data_dir().map(|mut p| {
        p.push(APP_DIR_NAME);
        p.push(CRASH_LOG_FILE_NAME);
        p
    })
}

/// Write a crash report synchronously (called from panic hook).
pub fn write_crash_log(info: &str) {
    if let Some(path) = crash_log_path() {
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        let report = format!(
            "=== SPACE Query Crash Report ===\nTimestamp: {}\n\n{}\n\n",
            timestamp, info
        );
        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&path) {
            let _ = std::io::Write::write_all(&mut file, report.as_bytes());
        }
    }
}

/// Read and remove the crash log if it exists. Returns the content.
pub fn take_crash_log() -> Option<String> {
    let path = crash_log_path()?;
    if !path.exists() {
        return None;
    }
    let content = fs::read_to_string(&path).ok()?;
    let _ = fs::remove_file(&path);
    Some(content)
}

#[cfg(test)]
mod logging_tests {
    use super::*;

    #[test]
    fn log_level_label_returns_expected_strings() {
        assert_eq!(LogLevel::Debug.label(), "DEBUG");
        assert_eq!(LogLevel::Info.label(), "INFO");
        assert_eq!(LogLevel::Warning.label(), "WARN");
        assert_eq!(LogLevel::Error.label(), "ERROR");
    }

    #[test]
    fn app_log_add_entry_inserts_at_front_and_truncates() {
        let mut log = AppLog::new();
        for i in 0..10 {
            log.add_entry(LogEntry {
                timestamp: format!("t{i}"),
                level: LogLevel::Info,
                source: "test".to_string(),
                message: format!("msg{i}"),
            });
        }
        assert_eq!(log.entries.len(), 10);
        assert_eq!(log.entries[0].message, "msg9");
        assert_eq!(log.entries[9].message, "msg0");
    }

    #[test]
    fn log_level_display_matches_label() {
        assert_eq!(format!("{}", LogLevel::Warning), "WARN");
    }
}
