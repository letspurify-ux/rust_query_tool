use chrono::Local;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::{mpsc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

const APP_DIR_NAME: &str = "space_query";
const LOG_FILE_NAME: &str = "app.log.json";
const CRASH_LOG_FILE_NAME: &str = "crash.log";
const MAX_LOG_ENTRIES: usize = 5000;
const LOG_WRITER_RESPONSE_TIMEOUT_DEFAULT_SECS: u64 = 15;
const LOG_WRITER_SAVE_DEBOUNCE_DEFAULT_MS: u64 = 200;

fn app_data_base_dir() -> Option<PathBuf> {
    if let Some(path) = dirs::data_dir() {
        return Some(path);
    }
    if let Some(home) = dirs::home_dir() {
        return Some(home.join(".local").join("share"));
    }
    None
}

fn log_writer_response_timeout() -> Duration {
    std::env::var("SPACE_QUERY_LOG_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(LOG_WRITER_RESPONSE_TIMEOUT_DEFAULT_SECS))
}

fn log_writer_save_debounce() -> Duration {
    std::env::var("SPACE_QUERY_LOG_SAVE_DEBOUNCE_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_millis(LOG_WRITER_SAVE_DEBOUNCE_DEFAULT_MS))
}

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
        app_data_base_dir().map(|mut p| {
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
                            eprintln!("Failed to parse app log file {}: {}", path.display(), err);
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
        let path = Self::log_path()
            .ok_or_else(|| std::io::Error::other("Log directory is unavailable"))?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or_default();
        let tmp_path =
            path.with_extension(format!("json.tmp.{}.{}", std::process::id(), now_millis));
        let file = fs::File::create(&tmp_path)?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, self)?;
        use std::io::Write;
        writer.flush()?;
        rename_overwrite(&tmp_path, &path)?;
        Ok(())
    }

    pub fn add_entry(&mut self, entry: LogEntry) {
        self.entries.insert(0, entry);
        self.entries.truncate(MAX_LOG_ENTRIES);
    }
}

fn rename_overwrite(from: &PathBuf, to: &PathBuf) -> Result<(), std::io::Error> {
    match fs::rename(from, to) {
        Ok(()) => Ok(()),
        Err(rename_err) => {
            if !to.exists() {
                return Err(rename_err);
            }

            if let Err(remove_err) = fs::remove_file(to) {
                return Err(std::io::Error::new(
                    remove_err.kind(),
                    format!(
                        "Failed to replace destination file {} while finalizing {}: {remove_err}",
                        to.display(),
                        from.display()
                    ),
                ));
            }

            fs::rename(from, to).map_err(|retry_err| {
                std::io::Error::new(
                    retry_err.kind(),
                    format!(
                        "Failed to finalize log file move from {} to {} after removing destination: {retry_err}",
                        from.display(),
                        to.display()
                    ),
                )
            })
        }
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

fn spawn_log_writer() -> mpsc::Sender<LogCommand> {
    let (sender, receiver) = mpsc::channel::<LogCommand>();
    thread::spawn(move || {
        let mut log = AppLog::load();
        let save_debounce = log_writer_save_debounce();
        let mut save_pending = false;
        let apply_command =
            |log: &mut AppLog,
             command: LogCommand,
             flush_replies: &mut Vec<mpsc::Sender<Result<(), String>>>|
             -> bool {
                match command {
                    LogCommand::Write(entry) => {
                        log.add_entry(entry);
                        true
                    }
                    LogCommand::Clear => {
                        log.entries.clear();
                        true
                    }
                    LogCommand::Flush(reply) => {
                        flush_replies.push(reply);
                        false
                    }
                }
            };
        loop {
            let cmd = match receiver.recv() {
                Ok(cmd) => cmd,
                Err(_) => break,
            };

            let mut channel_connected = true;
            let mut mutated_in_batch = false;
            let mut flush_replies: Vec<mpsc::Sender<Result<(), String>>> = Vec::new();
            let mut persist_result: Result<(), String> = Ok(());

            if apply_command(&mut log, cmd, &mut flush_replies) {
                save_pending = true;
                mutated_in_batch = true;
            }

            while let Ok(next) = receiver.try_recv() {
                if apply_command(&mut log, next, &mut flush_replies) {
                    save_pending = true;
                    mutated_in_batch = true;
                }
            }

            // Small debounce window to coalesce bursts of log writes into
            // one disk write while keeping Flush responsive.
            if save_pending && mutated_in_batch && flush_replies.is_empty() {
                let mut save_deadline = Instant::now() + save_debounce;
                loop {
                    if !flush_replies.is_empty() {
                        break;
                    }
                    let now = Instant::now();
                    if now >= save_deadline {
                        break;
                    }

                    let wait_for = save_deadline.saturating_duration_since(now);
                    match receiver.recv_timeout(wait_for) {
                        Ok(next) => {
                            if apply_command(&mut log, next, &mut flush_replies) {
                                save_pending = true;
                                save_deadline = Instant::now() + save_debounce;
                            }
                            while let Ok(pending_next) = receiver.try_recv() {
                                if apply_command(&mut log, pending_next, &mut flush_replies) {
                                    save_pending = true;
                                    save_deadline = Instant::now() + save_debounce;
                                }
                            }
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => break,
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            channel_connected = false;
                            break;
                        }
                    }
                }
            }

            if save_pending {
                match log.save() {
                    Ok(()) => {
                        persist_result = Ok(());
                        save_pending = false;
                    }
                    Err(err) => {
                        let msg = format!("Log save error: {err}");
                        eprintln!("{msg}");
                        persist_result = Err(msg);
                        // Keep entries in memory; they will be retried on the next save cycle.
                    }
                }
            }

            for reply in flush_replies {
                let _ = reply.send(persist_result.clone());
            }

            if !channel_connected {
                break;
            }
        }
    });
    sender
}

fn log_writer_handle() -> &'static Mutex<mpsc::Sender<LogCommand>> {
    static LOG_WRITER: OnceLock<Mutex<mpsc::Sender<LogCommand>>> = OnceLock::new();
    LOG_WRITER.get_or_init(|| Mutex::new(spawn_log_writer()))
}

fn log_writer_sender() -> mpsc::Sender<LogCommand> {
    log_writer_handle()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

fn send_log_command(command: LogCommand) -> Result<(), mpsc::SendError<LogCommand>> {
    let sender = log_writer_sender();
    let command = match sender.send(command) {
        Ok(()) => return Ok(()),
        Err(err) => err.0,
    };
    let mut guard = log_writer_handle()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *guard = spawn_log_writer();
    guard.send(command)
}

pub fn flush_log_writer() -> Result<(), String> {
    let (tx, rx) = mpsc::channel::<Result<(), String>>();
    if send_log_command(LogCommand::Flush(tx)).is_err() {
        return Err("Log writer is not available".to_string());
    }

    let timeout = log_writer_response_timeout();
    match rx.recv_timeout(timeout) {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            let (retry_tx, retry_rx) = mpsc::channel::<Result<(), String>>();
            if send_log_command(LogCommand::Flush(retry_tx)).is_err() {
                return Err("Log writer is not available".to_string());
            }
            match retry_rx.recv_timeout(timeout) {
                Ok(result) => result,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    Err("Timed out while waiting for log persistence".to_string())
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    Err("Log writer disconnected while flushing".to_string())
                }
            }
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
    if let Err(send_err) = send_log_command(LogCommand::Write(entry.clone())) {
        if let LogCommand::Write(failed_entry) = send_err.0 {
            let mut log = AppLog::load();
            log.add_entry(failed_entry);
            if let Err(err) = log.save() {
                eprintln!("Failed to persist log entry through fallback path: {err}");
            }
        }
    }
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
    match send_log_command(LogCommand::Clear) {
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
    app_data_base_dir().map(|mut p| {
        p.push(APP_DIR_NAME);
        p.push(CRASH_LOG_FILE_NAME);
        p
    })
}

/// Write a crash report synchronously (called from panic hook).
pub fn write_crash_log(info: &str) {
    if let Some(path) = crash_log_path() {
        if let Some(parent) = path.parent() {
            if let Err(err) = fs::create_dir_all(parent) {
                eprintln!(
                    "Failed to create crash log directory {}: {}",
                    parent.display(),
                    err
                );
                return;
            }
        }
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        let report = format!(
            "=== SPACE Query Crash Report ===\nTimestamp: {}\n\n{}\n\n",
            timestamp, info
        );
        match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(mut file) => {
                if let Err(err) = std::io::Write::write_all(&mut file, report.as_bytes()) {
                    eprintln!("Failed to write crash log {}: {}", path.display(), err);
                }
            }
            Err(err) => {
                eprintln!("Failed to open crash log {}: {}", path.display(), err);
            }
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
    if let Err(err) = fs::remove_file(&path) {
        eprintln!(
            "Failed to remove crash log after reading {}: {}",
            path.display(),
            err
        );
    }
    Some(content)
}

#[cfg(test)]
mod logging_tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

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

    #[test]
    fn rename_overwrite_replaces_existing_destination_file() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let base = std::env::temp_dir().join(format!("space_query_logging_test_{}", unique));
        fs::create_dir_all(&base).expect("failed to create test directory");

        let from = base.join("from.tmp");
        let to = base.join("to.log");

        fs::write(&from, "new").expect("failed to write source file");
        fs::write(&to, "old").expect("failed to write destination file");

        rename_overwrite(&from, &to).expect("rename_overwrite should replace destination");

        let contents = fs::read_to_string(&to).expect("failed to read destination file");
        assert_eq!(contents, "new");
        assert!(!from.exists());

        let _ = fs::remove_file(&to);
        let _ = fs::remove_dir_all(&base);
    }
}
