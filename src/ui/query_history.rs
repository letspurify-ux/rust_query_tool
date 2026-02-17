use fltk::{
    app,
    browser::HoldBrowser,
    button::Button,
    enums::FrameType,
    group::Flex,
    prelude::*,
    text::{StyleTableEntry, TextBuffer, TextDisplay},
    window::Window,
};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{mpsc, OnceLock};
use std::thread;
use std::time::Duration;

use crate::ui::center_on_main;
use crate::ui::constants::*;
use crate::ui::theme;
use crate::ui::{configured_editor_profile, configured_ui_font_size};
use crate::utils::config::{QueryHistory, QueryHistoryEntry};

enum HistoryCommand {
    Add(QueryHistoryEntry),
    Clear,
    Snapshot(mpsc::Sender<Vec<QueryHistoryEntry>>),
    Flush(mpsc::Sender<Result<(), String>>),
}

const HISTORY_WRITER_RESPONSE_TIMEOUT: Duration = Duration::from_millis(1500);

fn history_writer_sender() -> &'static mpsc::Sender<HistoryCommand> {
    static HISTORY_WRITER: OnceLock<mpsc::Sender<HistoryCommand>> = OnceLock::new();
    HISTORY_WRITER.get_or_init(|| {
        let (sender, receiver) = mpsc::channel::<HistoryCommand>();
        thread::spawn(move || {
            let mut history = QueryHistory::load();
            let apply_command = |history: &mut QueryHistory,
                                 command: HistoryCommand,
                                 needs_save: &mut bool,
                                 snapshot_replies: &mut Vec<mpsc::Sender<Vec<QueryHistoryEntry>>>,
                                 flush_replies: &mut Vec<mpsc::Sender<Result<(), String>>>| {
                match command {
                    HistoryCommand::Add(entry) => {
                        history.add_entry(entry);
                        *needs_save = true;
                    }
                    HistoryCommand::Clear => {
                        history.queries.clear();
                        *needs_save = true;
                    }
                    HistoryCommand::Snapshot(reply) => {
                        snapshot_replies.push(reply);
                    }
                    HistoryCommand::Flush(reply) => {
                        flush_replies.push(reply);
                    }
                }
            };
            while let Ok(cmd) = receiver.recv() {
                let previous_state = history.clone();
                let mut needs_save = false;
                let mut snapshot_replies: Vec<mpsc::Sender<Vec<QueryHistoryEntry>>> = Vec::new();
                let mut flush_replies: Vec<mpsc::Sender<Result<(), String>>> = Vec::new();
                apply_command(
                    &mut history,
                    cmd,
                    &mut needs_save,
                    &mut snapshot_replies,
                    &mut flush_replies,
                );
                let mut persist_result: Result<(), String> = Ok(());
                // Drain any pending commands before saving
                while let Ok(next) = receiver.try_recv() {
                    apply_command(
                        &mut history,
                        next,
                        &mut needs_save,
                        &mut snapshot_replies,
                        &mut flush_replies,
                    );
                }
                if needs_save {
                    match history.save() {
                        Ok(()) => {
                            persist_result = Ok(());
                        }
                        Err(err) => {
                            let msg = format!("Query history save error: {err}");
                            crate::utils::logging::log_error("history", &msg);
                            eprintln!("{msg}");
                            history = previous_state;
                            persist_result = Err(msg);
                        }
                    }
                }

                for reply in snapshot_replies {
                    let _ = reply.send(history.queries.clone());
                }

                for reply in flush_replies {
                    let _ = reply.send(persist_result.clone());
                }
            }
        });
        sender
    })
}

pub fn flush_history_writer_with_timeout(timeout: Duration) -> Result<(), String> {
    let (tx, rx) = mpsc::channel::<Result<(), String>>();
    if history_writer_sender()
        .send(HistoryCommand::Flush(tx))
        .is_err()
    {
        return Err("Query history writer is not available".to_string());
    }

    match rx.recv_timeout(timeout) {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            Err("Timed out while waiting for query history persistence".to_string())
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            Err("Query history writer disconnected while flushing".to_string())
        }
    }
}

pub fn flush_history_writer() -> Result<(), String> {
    flush_history_writer_with_timeout(HISTORY_WRITER_RESPONSE_TIMEOUT)
}

fn parse_error_line(message: &str) -> Option<usize> {
    let lowercase = message.to_ascii_lowercase();
    for needle in ["line ", "line:"] {
        if let Some(idx) = lowercase.find(needle) {
            let start = idx + needle.len();
            let mut digits = String::new();
            for ch in lowercase[start..].chars() {
                if ch.is_ascii_digit() {
                    digits.push(ch);
                } else if !digits.is_empty() {
                    break;
                }
            }
            if !digits.is_empty() {
                if let Ok(value) = digits.parse::<usize>() {
                    return Some(value);
                }
            }
        }
    }
    None
}

fn build_preview_styles(sql: &str, error_line: Option<usize>) -> String {
    if sql.is_empty() {
        return String::new();
    }
    let mut styles = String::with_capacity(sql.len());
    let mut line_number = 1usize;
    for line in sql.split_inclusive('\n') {
        let style_char = if error_line == Some(line_number) {
            'B'
        } else {
            'A'
        };
        styles.extend(std::iter::repeat(style_char).take(line.len()));
        line_number = line_number.saturating_add(1);
    }
    styles
}

fn preview_style_table() -> Vec<StyleTableEntry> {
    let profile = configured_editor_profile();
    let size = configured_ui_font_size() as i32;
    vec![
        StyleTableEntry {
            color: theme::text_primary(),
            font: profile.normal,
            size,
        },
        StyleTableEntry {
            color: theme::button_danger(),
            font: profile.normal,
            size,
        },
    ]
}

/// Retrieve a snapshot of the current history from the background writer thread,
/// avoiding a redundant disk read + parse.  Falls back to disk if the writer
/// thread is unreachable.
fn load_snapshot() -> Vec<QueryHistoryEntry> {
    let (tx, rx) = mpsc::channel();
    if history_writer_sender()
        .send(HistoryCommand::Snapshot(tx))
        .is_ok()
    {
        match rx.recv_timeout(HISTORY_WRITER_RESPONSE_TIMEOUT) {
            Ok(snapshot) => snapshot,
            Err(_) => QueryHistory::load().queries,
        }
    } else {
        // Writer thread dead – fall back to disk
        QueryHistory::load().queries
    }
}

pub fn clear_history() -> Result<(), String> {
    match history_writer_sender().send(HistoryCommand::Clear) {
        Ok(()) => flush_history_writer(),
        Err(send_err) => {
            if let HistoryCommand::Clear = send_err.0 {
                let mut history = QueryHistory::load();
                history.queries.clear();
                history
                    .save()
                    .map_err(|err| format!("Failed to clear query history: {err}"))?;
                Ok(())
            } else {
                Err("Failed to clear query history".to_string())
            }
        }
    }
}

/// Query history dialog for viewing and re-executing past queries
pub struct QueryHistoryDialog;

impl QueryHistoryDialog {
    pub fn show_with_registry(popups: Rc<RefCell<Vec<Window>>>) -> Option<String> {
        enum DialogMessage {
            UpdatePreview(usize),
            UseSelected,
            ClearHistory,
            Close,
        }

        let snapshot = load_snapshot();

        let current_group = fltk::group::Group::try_current();
        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let mut dialog = Window::default()
            .with_size(800, 500)
            .with_label("Query History");
        center_on_main(&mut dialog);
        dialog.set_color(theme::panel_raised());
        dialog.make_modal(true);

        let mut main_flex = Flex::default().with_pos(10, 10).with_size(780, 480);
        main_flex.set_type(fltk::group::FlexType::Column);
        main_flex.set_spacing(DIALOG_SPACING);

        // Top section with list and preview
        let mut content_flex = Flex::default();
        content_flex.set_type(fltk::group::FlexType::Row);
        content_flex.set_spacing(DIALOG_SPACING);

        // Left - History list
        let mut list_flex = Flex::default();
        list_flex.set_type(fltk::group::FlexType::Column);
        list_flex.set_spacing(DIALOG_SPACING);

        let mut list_label =
            fltk::frame::Frame::default().with_label("Query History (Most Recent First):");
        list_label.set_label_color(theme::text_primary());
        list_flex.fixed(&list_label, LABEL_ROW_HEIGHT);

        let mut browser = HoldBrowser::default();
        browser.set_color(theme::input_bg());
        browser.set_selection_color(theme::selection_strong());

        // Populate browser with history entries
        for entry in snapshot.iter() {
            let color_prefix = if entry.success { "@C255 " } else { "@C1 " };
            let display = format!(
                "{color_prefix}{} | {} | {}ms | {} rows",
                entry.timestamp,
                truncate_sql(&entry.sql, 50),
                entry.execution_time_ms,
                entry.row_count
            );
            browser.add(&display);
        }

        list_flex.end();
        content_flex.fixed(&list_flex, 350);

        // Right - SQL preview
        let mut preview_flex = Flex::default();
        preview_flex.set_type(fltk::group::FlexType::Column);
        preview_flex.set_spacing(DIALOG_SPACING);

        let mut preview_label = fltk::frame::Frame::default().with_label("SQL Preview:");
        preview_label.set_label_color(theme::text_primary());
        preview_flex.fixed(&preview_label, LABEL_ROW_HEIGHT);

        let preview_buffer = TextBuffer::default();
        let preview_style_buffer = TextBuffer::default();
        let mut preview_display = TextDisplay::default();
        preview_display.set_buffer(preview_buffer.clone());
        preview_display.set_color(theme::editor_bg());
        preview_display.set_text_color(theme::text_primary());
        preview_display.set_text_font(configured_editor_profile().normal);
        preview_display.set_text_size(configured_ui_font_size());
        preview_display.set_linenumber_width(48);
        preview_display.set_linenumber_fgcolor(theme::text_muted());
        preview_display.set_linenumber_bgcolor(theme::panel_bg());
        preview_display.set_linenumber_font(configured_editor_profile().normal);
        preview_display.set_linenumber_size((configured_ui_font_size().saturating_sub(2)) as i32);
        preview_display.set_highlight_data(preview_style_buffer.clone(), preview_style_table());

        let mut error_label = fltk::frame::Frame::default().with_label("Error details:");
        error_label.set_label_color(theme::text_primary());
        preview_flex.fixed(&error_label, LABEL_ROW_HEIGHT);

        let error_buffer = TextBuffer::default();
        let mut error_display = TextDisplay::default();
        error_display.set_buffer(error_buffer.clone());
        error_display.set_color(theme::panel_alt());
        error_display.set_text_color(theme::text_primary());
        error_display.set_text_font(configured_editor_profile().normal);
        error_display.set_text_size(configured_ui_font_size());
        error_display.hide();
        error_label.hide();
        preview_flex.fixed(&error_display, 90);

        preview_flex.end();

        content_flex.end();

        // Bottom buttons
        let mut button_flex = Flex::default();
        button_flex.set_type(fltk::group::FlexType::Row);
        button_flex.set_spacing(DIALOG_SPACING);

        let _spacer = fltk::frame::Frame::default();

        let mut use_btn = Button::default()
            .with_size(BUTTON_WIDTH_LARGE, BUTTON_HEIGHT)
            .with_label("Use Query");
        use_btn.set_color(theme::button_primary());
        use_btn.set_label_color(theme::text_primary());
        use_btn.set_frame(FrameType::RFlatBox);

        let mut clear_btn = Button::default()
            .with_size(BUTTON_WIDTH_LARGE, BUTTON_HEIGHT)
            .with_label("Clear History");
        clear_btn.set_color(theme::button_danger());
        clear_btn.set_label_color(theme::text_primary());
        clear_btn.set_frame(FrameType::RFlatBox);

        let mut close_btn = Button::default()
            .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
            .with_label("Close");
        close_btn.set_color(theme::button_subtle());
        close_btn.set_label_color(theme::text_primary());
        close_btn.set_frame(FrameType::RFlatBox);

        button_flex.fixed(&use_btn, BUTTON_WIDTH_LARGE);
        button_flex.fixed(&clear_btn, BUTTON_WIDTH_LARGE);
        button_flex.fixed(&close_btn, BUTTON_WIDTH);
        button_flex.end();
        main_flex.fixed(&button_flex, BUTTON_ROW_HEIGHT);

        main_flex.end();
        dialog.end();
        fltk::group::Group::set_current(current_group.as_ref());

        popups.borrow_mut().push(dialog.clone());
        // State for selected query
        let selected_sql: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));
        let queries: Rc<RefCell<Vec<QueryHistoryEntry>>> = Rc::new(RefCell::new(snapshot));

        let (sender, receiver) = mpsc::channel::<DialogMessage>();

        // Browser selection callback - update preview
        let sender_for_preview = sender.clone();
        browser.set_callback(move |b| {
            let selected = b.value();
            if selected > 0 {
                if let Some(idx) = (selected - 1).try_into().ok() {
                    let _ = sender_for_preview.send(DialogMessage::UpdatePreview(idx));
                    app::awake();
                }
            }
        });

        // Use Query button
        let sender_for_use = sender.clone();
        use_btn.set_callback(move |_| {
            let _ = sender_for_use.send(DialogMessage::UseSelected);
            app::awake();
        });

        // Clear History button
        let sender_for_clear = sender.clone();
        clear_btn.set_callback(move |_| {
            let _ = sender_for_clear.send(DialogMessage::ClearHistory);
            app::awake();
        });

        // Close button
        let sender_for_close = sender.clone();
        close_btn.set_callback(move |_| {
            let _ = sender_for_close.send(DialogMessage::Close);
            app::awake();
        });

        dialog.show();

        let mut preview_buffer = preview_buffer.clone();
        let mut preview_style_buffer = preview_style_buffer.clone();
        let mut error_buffer = error_buffer.clone();
        let mut error_display = error_display.clone();
        let mut error_label = error_label.clone();
        let preview_flex_for_error = preview_flex.clone();
        let mut browser = browser.clone();
        while dialog.shown() {
            fltk::app::wait();
            while let Ok(message) = receiver.try_recv() {
                match message {
                    DialogMessage::UpdatePreview(index) => {
                        let queries = queries.borrow();
                        if let Some(entry) = queries.get(index) {
                            preview_buffer.set_text(&entry.sql);
                            let styles = build_preview_styles(&entry.sql, entry.error_line);
                            preview_style_buffer.set_text(&styles);
                            if entry.success {
                                error_buffer.set_text("");
                                error_display.hide();
                                error_label.hide();
                            } else if let Some(message) = &entry.error_message {
                                error_buffer.set_text(message);
                                error_display.show();
                                error_label.show();
                            } else {
                                error_buffer.set_text("Unknown error");
                                error_display.show();
                                error_label.show();
                            }
                            preview_flex_for_error.layout();
                        }
                    }
                    DialogMessage::UseSelected => {
                        let selected = browser.value();
                        if selected > 0 {
                            if let Ok(idx) = usize::try_from(selected - 1) {
                                let queries = queries.borrow();
                                if let Some(entry) = queries.get(idx) {
                                    *selected_sql.borrow_mut() = Some(entry.sql.clone());
                                    dialog.hide();
                                }
                            }
                        } else {
                            fltk::dialog::alert_default("Please select a query from the list");
                        }
                    }
                    DialogMessage::ClearHistory => {
                        let choice = fltk::dialog::choice2_default(
                            "Are you sure you want to clear all query history?",
                            "Cancel",
                            "Clear All",
                            "",
                        );
                        if choice == Some(1) {
                            match clear_history() {
                                Ok(()) => {
                                    app::awake();
                                    queries.borrow_mut().clear();
                                    browser.clear();
                                    preview_buffer.set_text("");
                                    preview_style_buffer.set_text("");
                                    error_buffer.set_text("");
                                    error_display.hide();
                                    error_label.hide();
                                    preview_flex_for_error.layout();
                                }
                                Err(err) => {
                                    fltk::dialog::alert_default(&format!(
                                        "Failed to clear query history: {}",
                                        err
                                    ));
                                }
                            }
                        }
                    }
                    DialogMessage::Close => {
                        dialog.hide();
                    }
                }
            }
        }

        // Remove dialog from popups to prevent memory leak
        popups
            .borrow_mut()
            .retain(|w| w.as_widget_ptr() != dialog.as_widget_ptr());

        let result = selected_sql.borrow().clone();
        result
    }

    /// Add a query to history
    pub fn add_to_history(
        sql: &str,
        execution_time_ms: u64,
        row_count: usize,
        connection_name: &str,
        success: bool,
        message: &str,
    ) {
        if sql.trim().is_empty() {
            return;
        }

        let error_message = if success {
            None
        } else {
            Some(message.to_string())
        };
        let error_line = error_message.as_deref().and_then(parse_error_line);
        let entry = QueryHistoryEntry {
            sql: sql.to_string(),
            timestamp: chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
            execution_time_ms,
            row_count,
            connection_name: connection_name.to_string(),
            success,
            error_message,
            error_line,
        };

        if let Err(err) = history_writer_sender().send(HistoryCommand::Add(entry)) {
            app::awake();
            // Fallback: if channel is disconnected, save directly
            if let HistoryCommand::Add(entry) = err.0 {
                let mut history = QueryHistory::load();
                history.add_entry(entry);
                let _ = history.save();
            }
        }
    }
}

/// Truncate SQL for display in list
fn truncate_sql(sql: &str, max_len: usize) -> String {
    let mut normalized = String::with_capacity(sql.len());
    let mut last_was_whitespace = false;
    for ch in sql.chars() {
        if ch.is_whitespace() {
            if !last_was_whitespace {
                normalized.push(' ');
                last_was_whitespace = true;
            }
        } else {
            normalized.push(ch);
            last_was_whitespace = false;
        }
    }
    let trimmed = normalized.trim();

    if trimmed.is_empty() {
        return String::new();
    }

    if max_len == 0 {
        return String::new();
    }

    if trimmed.chars().count() > max_len {
        let end = trimmed
            .char_indices()
            .nth(max_len)
            .map(|(idx, _)| idx)
            .unwrap_or(trimmed.len());
        format!("{}...", &trimmed[..end])
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod query_history_tests {
    use super::truncate_sql;

    #[test]
    fn truncate_sql_preserves_multibyte_text_while_normalizing_whitespace() {
        let sql = "  SELECT\t'프로시저 테스트'\nFROM dual  ";
        assert_eq!(truncate_sql(sql, 100), "SELECT '프로시저 테스트' FROM dual");
    }

    #[test]
    fn truncate_sql_truncates_on_char_boundary_for_multibyte_text() {
        let sql = "가나다라마바사";
        assert_eq!(truncate_sql(sql, 5), "가나다라마...");
    }

    #[test]
    fn truncate_sql_collapses_whitespace_runs() {
        let sql = "SELECT\n\n\t\t*\t\tFROM\t\tdual";
        assert_eq!(truncate_sql(sql, 100), "SELECT * FROM dual");
    }

    #[test]
    fn truncate_sql_returns_empty_for_zero_limit() {
        assert_eq!(truncate_sql("SELECT 1", 0), "");
    }
}
