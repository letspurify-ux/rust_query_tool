use std::path::PathBuf;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Initialize the application-wide tracing/logging subsystem.
///
/// Logs are written to daily-rotated files under the application data directory
/// (`~/.local/share/space_query/logs/` on Linux, platform equivalent elsewhere).
///
/// The default log level is `warn`.  Override at runtime with the
/// `SPACE_QUERY_LOG` environment variable (e.g. `SPACE_QUERY_LOG=debug`).
///
/// Returns a [`WorkerGuard`] that **must** be held alive for the entire
/// lifetime of the program – dropping it flushes and closes the log file.
pub fn init() -> WorkerGuard {
    let log_dir = log_directory();

    let file_appender = rolling::daily(&log_dir, "space_query.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_env("SPACE_QUERY_LOG")
                .unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .with(
            fmt::layer()
                .with_writer(non_blocking)
                .with_ansi(false)
                .with_target(true)
                .with_thread_ids(true)
                .with_line_number(true)
                .with_file(true),
        )
        .init();

    guard
}

/// Return the directory where log files are stored, creating it if necessary.
fn log_directory() -> PathBuf {
    let mut path = dirs::data_dir().unwrap_or_else(|| PathBuf::from("."));
    path.push("space_query");
    path.push("logs");
    let _ = std::fs::create_dir_all(&path);
    path
}
