use std::fmt;
use std::path::PathBuf;
use std::sync::{mpsc, Mutex};

use once_cell::sync::OnceCell;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling;
use tracing_subscriber::fmt as subscriber_fmt;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// A single log entry destined for UI display.
#[derive(Clone, Debug)]
pub struct LogEntry {
    pub level: Level,
    pub message: String,
    pub target: String,
    pub timestamp: String,
}

/// A tracing Layer that sends formatted log entries to the UI via an mpsc channel.
struct UiLayer {
    sender: mpsc::Sender<LogEntry>,
}

impl<S: Subscriber> tracing_subscriber::Layer<S> for UiLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();

        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);

        let entry = LogEntry {
            level: *metadata.level(),
            message: visitor.0,
            target: metadata.target().to_string(),
            timestamp: chrono::Local::now().format("%H:%M:%S").to_string(),
        };

        // Non-blocking: drop if receiver is gone
        let _ = self.sender.send(entry);
    }
}

/// Visitor that extracts the `message` field from a tracing event.
struct MessageVisitor(String);

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{:?}", value);
        } else if self.0.is_empty() {
            self.0 = format!("{} = {:?}", field.name(), value);
        } else {
            self.0.push_str(&format!(", {} = {:?}", field.name(), value));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.0 = value.to_string();
        } else if self.0.is_empty() {
            self.0 = format!("{} = {}", field.name(), value);
        } else {
            self.0.push_str(&format!(", {} = {}", field.name(), value));
        }
    }
}

/// Global storage for the UI log receiver.
/// Set once during `init()`, consumed once by the UI via `take_ui_receiver()`.
static UI_LOG_RECEIVER: OnceCell<Mutex<Option<mpsc::Receiver<LogEntry>>>> = OnceCell::new();

/// Take the UI log receiver.  Returns `Some` exactly once; subsequent calls return `None`.
pub fn take_ui_receiver() -> Option<mpsc::Receiver<LogEntry>> {
    UI_LOG_RECEIVER
        .get()
        .and_then(|m| m.lock().ok())
        .and_then(|mut opt| opt.take())
}

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

    let (ui_sender, ui_receiver) = mpsc::channel::<LogEntry>();
    let _ = UI_LOG_RECEIVER.set(Mutex::new(Some(ui_receiver)));

    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_env("SPACE_QUERY_LOG")
                .unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .with(
            subscriber_fmt::layer()
                .with_writer(non_blocking)
                .with_ansi(false)
                .with_target(true)
                .with_thread_ids(true)
                .with_line_number(true)
                .with_file(true),
        )
        .with(UiLayer { sender: ui_sender })
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
