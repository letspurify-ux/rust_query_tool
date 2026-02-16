pub mod connection_dialog;
pub mod constants;
pub mod find_replace;
pub mod font_settings;
pub mod intellisense;
pub mod intellisense_context;
pub mod log_viewer;
pub mod main_window;
pub mod menu;
pub mod object_browser;
pub mod query_history;
pub mod query_tabs;
pub mod result_table;
pub mod result_tabs;
pub mod settings_dialog;
pub mod sql_editor;
pub mod syntax_highlight;
pub mod theme;

use fltk::{app, prelude::WidgetExt, prelude::WindowExt, window::Window};

pub use connection_dialog::*;
pub use find_replace::*;
pub use font_settings::*;
pub use intellisense::*;
pub use main_window::*;
pub use menu::*;
pub use object_browser::*;
pub use query_history::*;
pub use query_tabs::*;
pub use result_table::*;
pub use result_tabs::*;
pub use settings_dialog::*;
pub use sql_editor::*;
pub use syntax_highlight::*;

pub fn center_on_main(window: &mut Window) {
    if let Some(main) = app::widget_from_id::<Window>("main_window") {
        if main.as_widget_ptr() != window.as_widget_ptr() {
            window.clone().center_of(&main);
            return;
        }
    }

    if let Some(main) = app::first_window() {
        window.clone().center_of(&main);
    } else {
        window.clone().center_screen();
    }
}
