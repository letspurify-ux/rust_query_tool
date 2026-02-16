#![cfg_attr(
    all(not(debug_assertions), target_os = "windows"),
    windows_subsystem = "windows"
)]

mod app;
mod db;
mod ui;
mod utils;

use app::App;
use utils::logging;

fn main() {
    // Install custom panic hook for crash handling
    std::panic::set_hook(Box::new(|panic_info| {
        let location = panic_info
            .location()
            .map(|loc| format!("{}:{}:{}", loc.file(), loc.line(), loc.column()))
            .unwrap_or_else(|| "unknown location".to_string());

        let payload = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            (*s).to_string()
        } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "unknown panic payload".to_string()
        };

        let crash_message = format!(
            "Panic at {}\nMessage: {}\n\nFull info: {}",
            location, payload, panic_info
        );

        // Log the crash
        logging::log_error("panic", &crash_message);

        // Write crash log file synchronously
        logging::write_crash_log(&crash_message);

        eprintln!("SPACE Query crashed: {}", crash_message);
    }));

    logging::log_info("app", "SPACE Query starting");

    let app = App::new();
    app.run();
}
