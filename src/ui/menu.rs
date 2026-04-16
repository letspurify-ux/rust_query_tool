use fltk::{
    app,
    button::Button,
    enums::{FrameType, Shortcut},
    menu::{MenuBar, MenuFlag},
    prelude::*,
    text::{TextBuffer, TextDisplay, WrapMode},
    window::Window,
};

use crate::ui::center_on_main;
use crate::ui::constants::*;
use crate::ui::theme;
use crate::ui::{configured_editor_profile, configured_ui_font_size};
use crate::utils::arithmetic::safe_div;

pub struct MenuBarBuilder;

fn forward_menu_callback(menu: &mut MenuBar) {
    menu.do_callback();
}

fn show_info_dialog(title: &str, content: &str, width: i32, height: i32) {
    let current_group = fltk::group::Group::try_current();

    fltk::group::Group::set_current(None::<&fltk::group::Group>);

    let mut dialog = Window::default().with_size(width, height).with_label(title);
    center_on_main(&mut dialog);
    dialog.set_color(theme::panel_raised());
    dialog.make_modal(true);
    dialog.begin();

    let mut display = TextDisplay::default()
        .with_pos(10, 10)
        .with_size(width - 20, height - 60);
    display.set_color(theme::editor_bg());
    display.set_text_color(theme::text_primary());
    display.set_text_font(configured_editor_profile().normal);
    display.set_text_size(configured_ui_font_size());
    display.wrap_mode(WrapMode::AtBounds, 0);

    let mut buffer = TextBuffer::default();
    buffer.set_text(content);
    display.set_buffer(buffer);

    let button_x = safe_div(width - BUTTON_WIDTH, 2);
    let button_y = height - BUTTON_HEIGHT - DIALOG_MARGIN;
    let mut close_btn = Button::default()
        .with_pos(button_x, button_y)
        .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
        .with_label("Close");
    close_btn.set_color(theme::button_secondary());
    close_btn.set_label_color(theme::text_primary());
    close_btn.set_frame(FrameType::RFlatBox);

    let mut dialog_handle = dialog.clone();
    close_btn.set_callback(move |_| {
        dialog_handle.hide();
        app::awake();
    });

    dialog.end();
    dialog.show();
    fltk::group::Group::set_current(current_group.as_ref());

    while dialog.shown() {
        app::wait();
    }

    // Explicitly destroy top-level dialog widgets to release native resources.
    Window::delete(dialog);
}

fn build_about_dialog_content() -> String {
    let version = crate::version::display_version();
    let build_profile = if cfg!(debug_assertions) {
        "Debug"
    } else {
        "Release"
    };
    let splash_status = if cfg!(feature = "no-splash") {
        "Disabled"
    } else {
        "Enabled"
    };
    let platform = format!("{} {}", std::env::consts::OS, std::env::consts::ARCH);

    format!(
        "SPACE Query\n\
Version {version}\n\
\n\
Desktop SQL client for Oracle and MySQL/MariaDB built with Rust and FLTK.\n\
\n\
Highlights\n\
- Multi-tab SQL editor with execution history and result/message tabs\n\
- Oracle and MySQL/MariaDB object browser, syntax highlighting, and IntelliSense\n\
- Automatic SQL formatting for Oracle SQL, PL/SQL, and MySQL scripts\n\
- Explain Plan / EXPLAIN, SQL*Plus-style script execution, and transaction controls\n\
- Saved connections, OS keyring password storage, and application log viewer\n\
\n\
Runtime\n\
- Build: {build_profile}\n\
- Platform: {platform}\n\
- Splash screen: {splash_status}\n\
\n\
Notes\n\
- Oracle connections require Oracle Instant Client.\n\
- MySQL/MariaDB connections do not require Oracle Instant Client.\n\
- See Help > Keyboard Shortcuts for editor and execution shortcuts."
    )
}

impl MenuBarBuilder {
    pub fn build() -> MenuBar {
        let mut menu = MenuBar::default();
        menu.set_color(theme::panel_raised());
        menu.set_text_color(theme::text_primary());
        menu.set_id("main_menu");

        // File menu
        menu.add(
            "&File/&Connect",
            Shortcut::Ctrl | Shortcut::Command | 'n',
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&File/&Disconnect",
            Shortcut::Ctrl | Shortcut::Command | 'd',
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&File/",
            Shortcut::None,
            MenuFlag::MenuDivider,
            forward_menu_callback,
        );
        menu.add(
            "&File/&Open SQL File",
            Shortcut::Ctrl | Shortcut::Command | 'o',
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&File/&New SQL File",
            Shortcut::Command | 't',
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&File/&Save SQL File",
            Shortcut::Ctrl | Shortcut::Command | 's',
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&File/Save SQL File &As",
            Shortcut::Ctrl | Shortcut::Command | Shortcut::Shift | 's',
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&File/&Close SQL File",
            Shortcut::Command | 'w',
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&File/",
            Shortcut::None,
            MenuFlag::MenuDivider,
            forward_menu_callback,
        );
        menu.add(
            "&File/E&xit",
            Shortcut::Ctrl | Shortcut::Command | 'q',
            MenuFlag::Normal,
            forward_menu_callback,
        );

        // Edit menu
        menu.add(
            "&Edit/&Undo",
            Shortcut::Ctrl | Shortcut::Command | 'z',
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Edit/&Redo",
            Shortcut::Ctrl | Shortcut::Command | 'y',
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Edit/",
            Shortcut::None,
            MenuFlag::MenuDivider,
            forward_menu_callback,
        );
        menu.add(
            "&Edit/Cu&t",
            Shortcut::Ctrl | Shortcut::Command | 'x',
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Edit/&Copy",
            Shortcut::Ctrl | Shortcut::Command | 'c',
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Edit/Copy with &Headers",
            Shortcut::Ctrl | Shortcut::Command | Shortcut::Shift | 'c',
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Edit/&Paste",
            Shortcut::Ctrl | Shortcut::Command | 'v',
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Edit/",
            Shortcut::None,
            MenuFlag::MenuDivider,
            forward_menu_callback,
        );
        menu.add(
            "&Edit/Select &All",
            Shortcut::Ctrl | Shortcut::Command | 'a',
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Edit/",
            Shortcut::None,
            MenuFlag::MenuDivider,
            forward_menu_callback,
        );
        menu.add(
            "&Edit/&Find",
            Shortcut::Ctrl | Shortcut::Command | 'f',
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Edit/Find &Next",
            Shortcut::from_key(fltk::enums::Key::F3),
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Edit/&Replace",
            Shortcut::Ctrl | Shortcut::Command | 'h',
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Edit/",
            Shortcut::None,
            MenuFlag::MenuDivider,
            forward_menu_callback,
        );
        menu.add(
            "&Edit/&Format SQL",
            Shortcut::Ctrl | Shortcut::Command | Shortcut::Shift | 'f',
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Edit/Toggle &Comment",
            Shortcut::Ctrl | Shortcut::Command | '/',
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Edit/Upper&case Selection",
            Shortcut::Ctrl | Shortcut::Command | 'u',
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Edit/Lower&case Selection",
            Shortcut::Ctrl | Shortcut::Command | 'l',
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Edit/&Intellisense",
            Shortcut::Ctrl | Shortcut::Command | ' ',
            MenuFlag::Normal,
            forward_menu_callback,
        );

        // Query menu
        menu.add(
            "&Query/&Execute",
            Shortcut::from_key(fltk::enums::Key::F5),
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Query/Execute &Statement",
            Shortcut::Ctrl | fltk::enums::Key::Enter,
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Query/Execute Statement (&F9)",
            Shortcut::from_key(fltk::enums::Key::F9),
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Query/Execute &Selected",
            Shortcut::None,
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Query/&Quick Describe",
            Shortcut::from_key(fltk::enums::Key::F4),
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Query/",
            Shortcut::None,
            MenuFlag::MenuDivider,
            forward_menu_callback,
        );
        menu.add(
            "&Query/E&xplain Plan",
            Shortcut::from_key(fltk::enums::Key::F6),
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Query/",
            Shortcut::None,
            MenuFlag::MenuDivider,
            forward_menu_callback,
        );
        menu.add(
            "&Query/&Commit",
            Shortcut::from_key(fltk::enums::Key::F7),
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Query/&Rollback",
            Shortcut::from_key(fltk::enums::Key::F8),
            MenuFlag::Normal,
            forward_menu_callback,
        );

        // Tools menu
        menu.add(
            "&Tools/&Refresh Objects",
            Shortcut::None,
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Tools/",
            Shortcut::None,
            MenuFlag::MenuDivider,
            forward_menu_callback,
        );
        menu.add(
            "&Tools/&Export Results",
            Shortcut::Ctrl | Shortcut::Command | 'e',
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Tools/&Query History",
            Shortcut::None,
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Tools/Application &Log",
            Shortcut::None,
            MenuFlag::Normal,
            forward_menu_callback,
        );
        menu.add(
            "&Tools/",
            Shortcut::None,
            MenuFlag::MenuDivider,
            forward_menu_callback,
        );
        menu.add(
            "&Tools/&Auto-Commit",
            Shortcut::None,
            MenuFlag::Toggle,
            forward_menu_callback,
        );

        // Settings menu
        menu.add(
            "&Settings/&Preferences",
            Shortcut::None,
            MenuFlag::Normal,
            forward_menu_callback,
        );

        // Help menu
        menu.add("&Help/&About", Shortcut::None, MenuFlag::Normal, |_| {
            let content = build_about_dialog_content();
            show_info_dialog("About", &content, 640, 420);
        });
        menu.add(
            "&Help/&Keyboard Shortcuts",
            Shortcut::None,
            MenuFlag::Normal,
            |_| {
                show_info_dialog(
                    "Keyboard Shortcuts",
                    "Keyboard Shortcuts:\n\n\
                    macOS note: use Cmd where Ctrl is shown.\n\n\
                    File:\n\
                    Ctrl+N - Connect\n\
                    Ctrl+D - Disconnect\n\
                    Ctrl+T - New SQL File\n\
                    Ctrl+O - Open SQL File\n\
                    Ctrl+S - Save SQL File\n\
                    Ctrl+Shift+S - Save SQL File As\n\
                    Ctrl+W - Close SQL File\n\
                    Ctrl+Q - Exit\n\n\
                    Edit (SQL Editor):\n\
                    Ctrl+Z - Undo\n\
                    Ctrl+Y - Redo\n\
                    Ctrl+Shift+Z - Redo (Alt)\n\
                    Ctrl+X - Cut\n\
                    Ctrl+C - Copy\n\
                    Ctrl+Shift+C - Copy with Headers\n\
                    Ctrl+V - Paste\n\
                    Ctrl+A - Select All\n\
                    Ctrl+F - Find\n\
                    F3 - Find Next\n\
                    Ctrl+H - Replace\n\
                    Ctrl+Shift+F - Format SQL\n\
                    Ctrl+/ - Toggle Comment\n\
                    Ctrl+U - Uppercase Selection\n\
                    Ctrl+L - Lowercase Selection\n\
                    Ctrl+Space - Intellisense\n\
                    Ctrl+Shift+Up/Down - Select SQL Block\n\
                    Alt+Up/Down - Query History Prev/Next\n\
                    Ctrl+Click - Quick Describe at Cursor\n\n\
                    Query:\n\
                    Ctrl+Enter - Execute Statement\n\
                    F5 - Execute Script\n\
                    F9 - Execute Statement\n\
                    F6 - Explain Plan\n\
                    F7 - Commit\n\
                    F8 - Rollback\n\
                    F4 - Quick Describe (Editor)\n\n\
                    Tools:\n\
                    Ctrl+E - Export Results\n\
                    Query History - no shortcut\n\n\
                    Results Table:\n\
                    Ctrl+C - Copy Selected Cells\n\
                    Ctrl+Shift+C - Copy with Headers\n\
                    Ctrl+A - Select All\n\n\
                    Object Browser:\n\
                    Enter - Generate SELECT (tables/views)",
                    640,
                    640,
                );
            },
        );

        menu
    }
}
