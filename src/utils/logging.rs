use chrono::Local;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::{mpsc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

const APP_DIR_NAME: &str = "space_query";
const LOG_FILE_NAME: &str = "app.log.json";
const CRASH_LOG_FILE_NAME: &str = "crash.log";
const MAX_LOG_ENTRIES: usize = 5000;
const LOG_WRITER_RESPONSE_TIMEOUT: Duration = Duration::from_millis(1500);

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

#[derive(Debug, Serialize, Deserialize, Clone)]
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

    fn preserve_corrupt_log_file(path: &PathBuf) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or_default();
        let backup_path = path.with_extension(format!("corrupt.{}.json", timestamp));
        match fs::rename(path, &backup_path) {
            Ok(()) => {
                eprintln!("Corrupt app log was moved to {}", backup_path.display());
            }
            Err(err) => {
                eprintln!(
                    "Failed to preserve corrupt app log file {}: {}",
                    path.display(),
                    err
                );
            }
        }
    }

    pub fn load() -> Self {
        if let Some(path) = Self::log_path() {
            if path.exists() {
                match fs::read_to_string(&path) {
                    Ok(content) => match serde_json::from_str::<Self>(&content) {
                        Ok(log) => return log,
                        Err(err) => {
                            eprintln!(
                                "Failed to parse app log file {}: {}",
                                path.display(),
                                err
                            );
                            Self::preserve_corrupt_log_file(&path);
                        }
                    },
                    Err(err) => {
                        eprintln!("Failed to read app log file {}: {}", path.display(), err);
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
            let tmp_path = path.with_extension("json.tmp");
            let file = fs::File::create(&tmp_path)?;
            let mut writer = BufWriter::new(file);
            serde_json::to_writer(&mut writer, self)?;
            use std::io::Write;
            writer.flush()?;
            fs::rename(&tmp_path, &path)?;
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
    Flush(mpsc::Sender<Result<(), String>>),
}

fn log_writer_sender() -> &'static mpsc::Sender<LogCommand> {
    static LOG_WRITER: OnceLock<mpsc::Sender<LogCommand>> = OnceLock::new();
    LOG_WRITER.get_or_init(|| {
        let (sender, receiver) = mpsc::channel::<LogCommand>();
        thread::spawn(move || {
            let mut log = AppLog::load();
            let mut last_persist_error: Option<String> = None;
            let apply_command =
                |log: &mut AppLog,
                 command: LogCommand,
                 needs_save: &mut bool,
                 flush_replies: &mut Vec<mpsc::Sender<Result<(), String>>>| {
                    match command {
                        LogCommand::Write(entry) => {
                            log.add_entry(entry);
                            *needs_save = true;
                        }
                        LogCommand::Clear => {
                            log.entries.clear();
                            *needs_save = true;
                        }
                        LogCommand::Flush(reply) => {
                            flush_replies.push(reply);
                        }
                    }
                };
            while let Ok(cmd) = receiver.recv() {
                let previous_state = log.clone();
                let mut needs_save = false;
                let mut flush_replies: Vec<mpsc::Sender<Result<(), String>>> = Vec::new();
                apply_command(&mut log, cmd, &mut needs_save, &mut flush_replies);
                while let Ok(next) = receiver.try_recv() {
                    apply_command(&mut log, next, &mut needs_save, &mut flush_replies);
                }
                if needs_save {
                    match log.save() {
                        Ok(()) => {
                            last_persist_error = None;
                        }
                        Err(err) => {
                            let msg = format!("Log save error: {err}");
                            eprintln!("{msg}");
                            log = previous_state;
                            last_persist_error = Some(msg);
                        }
                    }
                }

                let save_result: Result<(), String> = match &last_persist_error {
                    Some(err) => Err(err.clone()),
                    None => Ok(()),
                };

                for reply in flush_replies {
                    let _ = reply.send(save_result.clone());
                }
            }
        });
        sender
    })
}

pub fn flush_log_writer() -> Result<(), String> {
    let (tx, rx) = mpsc::channel::<Result<(), String>>();
    if log_writer_sender().send(LogCommand::Flush(tx)).is_err() {
        return Err("Log writer is not available".to_string());
    }

    match rx.recv_timeout(LOG_WRITER_RESPONSE_TIMEOUT) {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            Err("Timed out while waiting for log persistence".to_string())
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            Err("Log writer disconnected while flushing".to_string())
        }
    }
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
pub fn clear_log() -> Result<(), String> {
    match log_writer_sender().send(LogCommand::Clear) {
        Ok(()) => {
            flush_log_writer()?;
            if let Ok(mut buf) = in_memory_log().lock() {
                buf.clear();
            }
            Ok(())
        }
        Err(send_err) => {
            if let LogCommand::Clear = send_err.0 {
                let mut log = AppLog::load();
                log.entries.clear();
                log.save()
                    .map_err(|err| format!("Failed to clear persisted log: {err}"))?;
                if let Ok(mut buf) = in_memory_log().lock() {
                    buf.clear();
                }
                Ok(())
            } else {
                Err("Failed to clear application log".to_string())
            }
        }
    }
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
