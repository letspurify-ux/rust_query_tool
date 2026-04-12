#![allow(
    clippy::arc_with_non_send_sync,
    clippy::too_many_arguments,
    clippy::type_complexity
)]

pub(crate) mod auto_format_engine;
pub mod app;
pub mod app_icon;
pub mod db;
#[cfg(not(feature = "no-splash"))]
pub mod splash;
pub mod sql_parser_engine;
pub mod sql_text;
pub mod ui;
pub mod utils;
