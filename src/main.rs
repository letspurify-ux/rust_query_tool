#![cfg_attr(
    all(not(debug_assertions), target_os = "windows"),
    windows_subsystem = "windows"
)]

mod app;
mod db;
mod ui;
mod utils;

use app::App;

fn main() {
    // Initialize file-based logging (must hold guard until app exit)
    let _log_guard = utils::logging::init();

    // Install crash handler for panic reports
    utils::crash_handler::install();

    tracing::info!("SPACE Query v{} starting", env!("CARGO_PKG_VERSION"));

    let app = App::new();
    app.run();

    tracing::info!("SPACE Query shutting down");
}
