use fltk::{
    app,
    browser::HoldBrowser,
    button::{Button, CheckButton},
    enums::FrameType,
    group::Flex,
    input::Input,
    prelude::*,
    text::{StyleTableEntry, TextBuffer, TextDisplay},
    window::Window,
};
use std::sync::{mpsc, OnceLock};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::db::{QueryExecutor, ToolCommand};
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

const HISTORY_WRITER_RESPONSE_TIMEOUT_DEFAULT_SECS: u64 = 15;
const REDACTED_SECRET: &str = "<redacted>";

fn fold_for_case_insensitive(value: &str) -> String {
    value.chars().flat_map(|ch| ch.to_lowercase()).collect()
}

fn history_writer_response_timeout() -> Duration {
    std::env::var("SPACE_QUERY_HISTORY_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(HISTORY_WRITER_RESPONSE_TIMEOUT_DEFAULT_SECS))
}

fn spawn_history_writer() -> mpsc::Sender<HistoryCommand> {
    let (sender, receiver) = mpsc::channel::<HistoryCommand>();
    thread::spawn(move || {
        let mut history = QueryHistory::load();
        let apply_command =
            |history: &mut QueryHistory,
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
}

fn history_writer_handle() -> &'static Mutex<mpsc::Sender<HistoryCommand>> {
    static HISTORY_WRITER: OnceLock<Mutex<mpsc::Sender<HistoryCommand>>> = OnceLock::new();
    HISTORY_WRITER.get_or_init(|| Mutex::new(spawn_history_writer()))
}

fn history_writer_sender() -> mpsc::Sender<HistoryCommand> {
    history_writer_handle()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

fn send_history_command(command: HistoryCommand) -> Result<(), mpsc::SendError<HistoryCommand>> {
    let initial_sender = history_writer_sender();
    let command = match initial_sender.send(command) {
        Ok(()) => return Ok(()),
        Err(err) => err.0,
    };

    let mut guard = history_writer_handle()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *guard = spawn_history_writer();
    guard.send(command)
}

pub fn flush_history_writer_with_timeout(timeout: Duration) -> Result<(), String> {
    let (tx, rx) = mpsc::channel::<Result<(), String>>();
    if send_history_command(HistoryCommand::Flush(tx)).is_err() {
        return Err("Query history writer is not available".to_string());
    }

    match rx.recv_timeout(timeout) {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            let (retry_tx, retry_rx) = mpsc::channel::<Result<(), String>>();
            if send_history_command(HistoryCommand::Flush(retry_tx)).is_err() {
                return Err("Query history writer is not available".to_string());
            }
            match retry_rx.recv_timeout(timeout) {
                Ok(result) => result,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    Err("Timed out while waiting for query history persistence".to_string())
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    Err("Query history writer disconnected while flushing".to_string())
                }
            }
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            Err("Query history writer disconnected while flushing".to_string())
        }
    }
}

pub fn flush_history_writer() -> Result<(), String> {
    flush_history_writer_with_timeout(history_writer_response_timeout())
}

fn parse_error_line(message: &str) -> Option<usize> {
    let lowercase = fold_for_case_insensitive(message);
    let patterns = [
        "error at line",
        "line:",
        " line ",
        // Keep ORA-06512 lower priority so we prefer primary parser errors.
        "ora-06512: at line",
    ];

    let mut best_line: Option<(usize, usize)> = None;

    for (priority, needle) in patterns.iter().enumerate() {
        let mut search_start = 0usize;
        while search_start < lowercase.len() {
            let Some(relative_idx) = lowercase[search_start..].find(needle) else {
                break;
            };
            let idx = search_start + relative_idx;
            let mut cursor = idx + needle.len();
            cursor = next_char_boundary(&lowercase, cursor);

            let mut digits = String::new();
            while cursor < lowercase.len() {
                let Some((ch, next)) = next_char_with_clamped_boundary(&lowercase, cursor) else {
                    break;
                };
                if ch.is_whitespace() && digits.is_empty() {
                    cursor = next;
                    continue;
                }
                if ch.is_ascii_digit() {
                    digits.push(ch);
                    cursor = next;
                    continue;
                }
                break;
            }

            if let Ok(value) = digits.parse::<usize>() {
                match best_line {
                    None => best_line = Some((priority, value)),
                    Some((best_priority, _)) if priority < best_priority => {
                        best_line = Some((priority, value));
                    }
                    _ => {}
                }
            }
            search_start = idx.saturating_add(needle.len());
        }
    }

    best_line.map(|(_, line)| line).filter(|line| *line > 0)
}

fn clamp_error_line_to_sql(error_line: Option<usize>, sql: &str) -> Option<usize> {
    let line_count = sql.lines().count();
    match (error_line, line_count) {
        (Some(line), count) if count > 0 => Some(line.min(count).max(1)),
        _ => None,
    }
}

fn sanitize_history_sql(sql: &str) -> String {
    sanitize_sensitive_text(sql)
}

fn sanitize_history_message(message: &str) -> String {
    sanitize_sensitive_text(message)
}

fn sanitize_sensitive_text(text: &str) -> String {
    let redacted_connect = redact_connect_commands(text);
    let redacted_identified = redact_identified_by_clause(&redacted_connect);
    redact_uri_credentials(&redacted_identified)
}

fn redact_connect_commands(text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }

    let mut output = String::with_capacity(text.len());
    for segment in text.split_inclusive('\n') {
        let has_newline = segment.ends_with('\n');
        let line = if has_newline {
            &segment[..segment.len().saturating_sub(1)]
        } else {
            segment
        };
        let redacted = redact_connect_command_line(line);
        output.push_str(&redacted);
        if has_newline {
            output.push('\n');
        }
    }

    if !text.ends_with('\n') && output.ends_with('\n') {
        output.pop();
    }

    output
}

fn redact_connect_command_line(line: &str) -> String {
    let trimmed = line.trim_start();
    let indent_len = line.len().saturating_sub(trimmed.len());
    let indent = &line[..indent_len];

    if let Some(ToolCommand::Connect {
        username,
        host,
        port,
        service_name,
        ..
    }) = QueryExecutor::parse_tool_command(trimmed)
    {
        return format!(
            "{}CONNECT {}/{}@{}:{}/{}",
            indent, username, REDACTED_SECRET, host, port, service_name
        );
    }

    let upper = trimmed.to_ascii_uppercase();
    if (upper.starts_with("CONNECT ") && !upper.starts_with("CONNECT BY"))
        || upper.starts_with("CONN ")
    {
        if let Some(masked) = redact_connect_credentials_fallback(trimmed) {
            return format!("{}{}", indent, masked);
        }
    }

    line.to_string()
}

fn redact_connect_credentials_fallback(trimmed: &str) -> Option<String> {
    let mut parts = trimmed.splitn(2, char::is_whitespace);
    let command = parts.next().unwrap_or_default();
    let rest = parts.next().unwrap_or_default().trim_start();
    if command.is_empty() || rest.is_empty() {
        return None;
    }

    let slash_idx = rest.find('/')?;
    let at_idx = rest
        .char_indices()
        .filter_map(|(idx, ch)| if ch == '@' { Some(idx) } else { None })
        .last()?;
    if slash_idx >= at_idx {
        return None;
    }

    Some(format!(
        "{} {}/{}{}",
        command,
        &rest[..slash_idx],
        REDACTED_SECRET,
        &rest[at_idx..]
    ))
}

fn redact_identified_by_clause(text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }

    #[derive(Clone, Copy)]
    enum SqlState {
        Code,
        SingleQuoted,
        DoubleQuoted,
        LineComment,
        BlockComment,
    }

    const IDENTIFIED_BY: &str = "IDENTIFIED BY";

    let mut output = String::with_capacity(text.len());
    let mut state = SqlState::Code;
    let mut idx = 0usize;

    while idx < text.len() {
        let Some((ch, next)) = next_char_with_clamped_boundary(text, idx) else {
            break;
        };

        match state {
            SqlState::Code => {
                if ch == '\'' {
                    output.push(ch);
                    state = SqlState::SingleQuoted;
                    idx = next;
                    continue;
                }
                if ch == '"' {
                    output.push(ch);
                    state = SqlState::DoubleQuoted;
                    idx = next;
                    continue;
                }
                if ch == '-' {
                    if let Some((next_ch, next_boundary)) =
                        next_char_with_clamped_boundary(text, next)
                    {
                        if next_ch == '-' {
                            output.push(ch);
                            output.push(next_ch);
                            state = SqlState::LineComment;
                            idx = next_boundary;
                            continue;
                        }
                    }
                }
                if ch == '/' {
                    if let Some((next_ch, next_boundary)) =
                        next_char_with_clamped_boundary(text, next)
                    {
                        if next_ch == '*' {
                            output.push(ch);
                            output.push(next_ch);
                            state = SqlState::BlockComment;
                            idx = next_boundary;
                            continue;
                        }
                    }
                }

                if let Some(found) = find_ascii_case_insensitive(text, IDENTIFIED_BY, idx) {
                    if found == idx {
                        let pattern_end = found + IDENTIFIED_BY.len();
                        output.push_str(&text[idx..pattern_end]);

                        let mut value_start = pattern_end;
                        while value_start < text.len() {
                            let Some((ws, next_ws)) =
                                next_char_with_clamped_boundary(text, value_start)
                            else {
                                break;
                            };
                            if ws.is_whitespace() {
                                output.push(ws);
                                value_start = next_ws;
                            } else {
                                break;
                            }
                        }

                        if value_start >= text.len() {
                            idx = value_start;
                            continue;
                        }

                        let Some((first, _)) = next_char_with_clamped_boundary(text, value_start)
                        else {
                            idx = text.len();
                            continue;
                        };

                        if first == '\'' || first == '"' {
                            let quote = first;
                            output.push(quote);
                            let mut value_end = value_start + quote.len_utf8();
                            let mut closed_quote = false;
                            while value_end < text.len() {
                                let Some((vch, vnext)) =
                                    next_char_with_clamped_boundary(text, value_end)
                                else {
                                    value_end = text.len();
                                    break;
                                };
                                value_end = vnext;
                                if vch == quote {
                                    if value_end < text.len() {
                                        let Some((escaped, escaped_next)) =
                                            next_char_with_clamped_boundary(text, value_end)
                                        else {
                                            value_end = text.len();
                                            break;
                                        };
                                        if escaped == quote {
                                            value_end = escaped_next;
                                            continue;
                                        }
                                    }
                                    closed_quote = true;
                                    break;
                                }
                            }
                            output.push_str(REDACTED_SECRET);
                            if closed_quote {
                                output.push(quote);
                            }
                            idx = value_end;
                            continue;
                        }

                        let mut value_end = value_start;
                        while value_end < text.len() {
                            let Some((vch, vnext)) =
                                next_char_with_clamped_boundary(text, value_end)
                            else {
                                value_end = text.len();
                                break;
                            };
                            if vch.is_whitespace() || matches!(vch, ';' | ')' | ',' | '\n' | '\r') {
                                break;
                            }
                            value_end = vnext;
                        }
                        output.push_str(REDACTED_SECRET);
                        idx = value_end;
                        continue;
                    }
                }

                output.push(ch);
                idx = next;
            }
            SqlState::SingleQuoted => {
                output.push(ch);
                idx = next;
                if ch == '\'' {
                    if let Some((escaped, escaped_next)) =
                        next_char_with_clamped_boundary(text, idx)
                    {
                        if escaped == '\'' {
                            output.push(escaped);
                            idx = escaped_next;
                            continue;
                        }
                    }
                    state = SqlState::Code;
                }
            }
            SqlState::DoubleQuoted => {
                output.push(ch);
                idx = next;
                if ch == '"' {
                    if let Some((escaped, escaped_next)) =
                        next_char_with_clamped_boundary(text, idx)
                    {
                        if escaped == '"' {
                            output.push(escaped);
                            idx = escaped_next;
                            continue;
                        }
                    }
                    state = SqlState::Code;
                }
            }
            SqlState::LineComment => {
                output.push(ch);
                idx = next;
                if ch == '\n' {
                    state = SqlState::Code;
                }
            }
            SqlState::BlockComment => {
                output.push(ch);
                idx = next;
                if ch == '*' {
                    if let Some((slash, slash_next)) = next_char_with_clamped_boundary(text, idx) {
                        if slash == '/' {
                            output.push(slash);
                            idx = slash_next;
                            state = SqlState::Code;
                        }
                    }
                }
            }
        }
    }

    output
}

fn next_char_boundary(text: &str, idx: usize) -> usize {
    let mut boundary = idx.min(text.len());
    while boundary > 0 && !text.is_char_boundary(boundary) {
        boundary -= 1;
    }
    boundary
}

fn next_char_with_clamped_boundary(text: &str, idx: usize) -> Option<(char, usize)> {
    let start = next_char_boundary(text, idx);
    if start >= text.len() {
        return None;
    }

    let ch = text[start..].chars().next()?;
    Some((ch, start + ch.len_utf8()))
}

fn redact_uri_credentials(text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }

    let mut output = String::with_capacity(text.len());
    let mut cursor = 0usize;

    while let Some(rel) = text[cursor..].find("://") {
        let scheme_sep = cursor + rel;
        let auth_start = scheme_sep + 3;
        output.push_str(&text[cursor..auth_start]);

        let authority_end = text[auth_start..]
            .char_indices()
            .find_map(|(offset, ch)| {
                if matches!(
                    ch,
                    '/' | '?' | '#' | ' ' | '\t' | '\n' | '\r' | '\'' | '"' | ';' | ')' | '('
                ) {
                    Some(auth_start + offset)
                } else {
                    None
                }
            })
            .unwrap_or(text.len());

        let authority = &text[auth_start..authority_end];
        if let Some(at_pos) = authority.rfind('@') {
            let userinfo = &authority[..at_pos];
            let host = &authority[at_pos..];
            if let Some(colon_pos) = userinfo.find(':') {
                output.push_str(&userinfo[..colon_pos + 1]);
                output.push_str(REDACTED_SECRET);
                output.push_str(host);
            } else if !userinfo.is_empty() {
                // Preserve source format for user-only URI auth segments.
                output.push_str(userinfo);
                output.push_str(host);
            } else {
                output.push_str(authority);
            }
        } else {
            output.push_str(authority);
        }

        cursor = authority_end;
    }

    output.push_str(&text[cursor..]);
    output
}

fn find_ascii_case_insensitive(text: &str, needle: &str, start: usize) -> Option<usize> {
    let haystack = text.as_bytes();
    let pattern = needle.as_bytes();
    if pattern.is_empty() || start >= haystack.len() || pattern.len() > haystack.len() {
        return None;
    }

    for idx in start..=haystack.len().saturating_sub(pattern.len()) {
        if !text.is_char_boundary(idx) {
            continue;
        }
        if haystack[idx..idx + pattern.len()]
            .iter()
            .zip(pattern.iter())
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
        {
            return Some(idx);
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
fn load_snapshot() -> (Vec<QueryHistoryEntry>, bool) {
    let (tx, rx) = mpsc::channel();
    if send_history_command(HistoryCommand::Snapshot(tx)).is_ok() {
        match rx.recv_timeout(history_writer_response_timeout()) {
            Ok(snapshot) => (snapshot, false),
            Err(_) => {
                for _ in 0..2 {
                    if flush_history_writer().is_err() {
                        continue;
                    }
                    let (retry_tx, retry_rx) = mpsc::channel();
                    if send_history_command(HistoryCommand::Snapshot(retry_tx)).is_ok() {
                        if let Ok(retry_snapshot) =
                            retry_rx.recv_timeout(history_writer_response_timeout())
                        {
                            return (retry_snapshot, false);
                        }
                    }
                }
                (QueryHistory::load().queries, true)
            }
        }
    } else {
        // Writer thread dead – fall back to disk
        (QueryHistory::load().queries, true)
    }
}

pub fn clear_history() -> Result<(), String> {
    match send_history_command(HistoryCommand::Clear) {
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
    pub fn show_with_registry(popups: Arc<Mutex<Vec<Window>>>) -> Option<String> {
        enum DialogMessage {
            UpdatePreview(usize),
            FilterChanged,
            UseSelected,
            ClearHistory,
            Close,
        }

        let (snapshot, snapshot_is_fallback) = load_snapshot();

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
        if snapshot_is_fallback {
            list_label.set_label("Query History (disk fallback; recent updates may be delayed):");
        }
        list_label.set_label_color(theme::text_primary());
        list_flex.fixed(&list_label, LABEL_ROW_HEIGHT);

        let mut filter_row = Flex::default();
        filter_row.set_type(fltk::group::FlexType::Row);
        filter_row.set_spacing(DIALOG_SPACING);

        let mut search_input = Input::default();
        search_input.set_color(theme::input_bg());
        search_input.set_text_color(theme::text_primary());
        search_input.set_tooltip("Filter by SQL text, connection, timestamp, or error");
        filter_row.fixed(&search_input, 224);

        let mut failed_only_check = CheckButton::default().with_label("Failed only");
        failed_only_check.set_label_color(theme::text_primary());
        filter_row.fixed(&failed_only_check, 114);

        filter_row.end();
        list_flex.fixed(&filter_row, INPUT_ROW_HEIGHT);

        let mut browser = HoldBrowser::default();
        browser.set_color(theme::input_bg());
        browser.set_selection_color(theme::selection_strong());

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

        popups
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(dialog.clone());
        // State for selected query
        let selected_sql: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let queries: Arc<Mutex<Vec<QueryHistoryEntry>>> = Arc::new(Mutex::new(snapshot));
        let filtered_indices: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));

        {
            let query_snapshot = queries
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            populate_history_browser(
                &query_snapshot,
                &mut browser,
                &filtered_indices,
                &search_input.value(),
                failed_only_check.value(),
            );
        }

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

        let sender_for_filter = sender.clone();
        search_input.set_callback(move |_| {
            let _ = sender_for_filter.send(DialogMessage::FilterChanged);
            app::awake();
        });

        let sender_for_failed_only = sender.clone();
        failed_only_check.set_callback(move |_| {
            let _ = sender_for_failed_only.send(DialogMessage::FilterChanged);
            app::awake();
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
                        let entry_index = {
                            let filtered = filtered_indices
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                            filtered.get(index).copied()
                        };
                        let queries = queries
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        if let Some(entry) = entry_index.and_then(|idx| queries.get(idx)) {
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
                    DialogMessage::FilterChanged => {
                        let query_snapshot = queries
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        populate_history_browser(
                            &query_snapshot,
                            &mut browser,
                            &filtered_indices,
                            &search_input.value(),
                            failed_only_check.value(),
                        );
                        preview_buffer.set_text("");
                        preview_style_buffer.set_text("");
                        error_buffer.set_text("");
                        error_display.hide();
                        error_label.hide();
                        preview_flex_for_error.layout();
                    }
                    DialogMessage::UseSelected => {
                        let selected = browser.value();
                        if selected > 0 {
                            if let Ok(idx) = usize::try_from(selected - 1) {
                                let entry_index = {
                                    let filtered = filtered_indices
                                        .lock()
                                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                                    filtered.get(idx).copied()
                                };
                                let queries = queries
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                                if let Some(entry) =
                                    entry_index.and_then(|actual| queries.get(actual))
                                {
                                    *selected_sql
                                        .lock()
                                        .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                                        Some(entry.sql.clone());
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
                                    queries
                                        .lock()
                                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                                        .clear();
                                    let query_snapshot = queries
                                        .lock()
                                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                                    populate_history_browser(
                                        &query_snapshot,
                                        &mut browser,
                                        &filtered_indices,
                                        &search_input.value(),
                                        failed_only_check.value(),
                                    );
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
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|w| w.as_widget_ptr() != dialog.as_widget_ptr());

        // Explicitly destroy top-level dialog widgets to release native resources.
        Window::delete(dialog);

        let result = selected_sql
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
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
    ) -> Result<(), String> {
        if sql.trim().is_empty() {
            return Ok(());
        }

        let sanitized_sql = sanitize_history_sql(sql);
        let sanitized_message = sanitize_history_message(message);
        let error_message = if success {
            None
        } else {
            Some(sanitized_message.clone())
        };
        let error_line = clamp_error_line_to_sql(
            error_message.as_deref().and_then(parse_error_line),
            &sanitized_sql,
        );
        let entry = QueryHistoryEntry {
            sql: sanitized_sql,
            timestamp: chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
            execution_time_ms,
            row_count,
            connection_name: connection_name.to_string(),
            success,
            error_message,
            error_line,
        };

        if let Err(err) = send_history_command(HistoryCommand::Add(entry)) {
            app::awake();
            if let HistoryCommand::Add(entry) = err.0 {
                let mut history = QueryHistory::load();
                history.add_entry(entry);
                if let Err(save_err) = history.save() {
                    let error_message = format!("Query history fallback save error: {save_err}");
                    crate::utils::logging::log_error("history", &error_message);
                    eprintln!("{error_message}");
                    return Err(error_message);
                }
            }
        }
        Ok(())
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
        if max_len <= 3 {
            return "...".chars().take(max_len).collect();
        }
        let visible_len = max_len - 3;
        let end = trimmed
            .char_indices()
            .nth(visible_len)
            .map(|(idx, _)| idx)
            .unwrap_or(trimmed.len());
        format!("{}...", &trimmed[..end])
    } else {
        trimmed.to_string()
    }
}

fn history_entry_matches_filter(
    entry: &QueryHistoryEntry,
    search_lower: &str,
    failed_only: bool,
) -> bool {
    if failed_only && entry.success {
        return false;
    }

    if search_lower.is_empty() {
        return true;
    }

    if fold_for_case_insensitive(&entry.sql).contains(search_lower) {
        return true;
    }

    if fold_for_case_insensitive(&entry.connection_name).contains(search_lower) {
        return true;
    }

    if fold_for_case_insensitive(&entry.timestamp).contains(search_lower) {
        return true;
    }

    match entry.error_message.as_deref() {
        Some(message) => fold_for_case_insensitive(message).contains(search_lower),
        None => false,
    }
}

fn history_entry_display(entry: &QueryHistoryEntry) -> String {
    let color_prefix = if entry.success { "@C255 " } else { "@C1 " };
    format!(
        "{color_prefix}{} | {} | {}ms | {} rows",
        entry.timestamp,
        truncate_sql(&entry.sql, 50),
        entry.execution_time_ms,
        entry.row_count
    )
}

fn populate_history_browser(
    entries: &[QueryHistoryEntry],
    browser: &mut HoldBrowser,
    filtered_indices: &Arc<Mutex<Vec<usize>>>,
    search_text: &str,
    failed_only: bool,
) {
    let search_lower = fold_for_case_insensitive(search_text.trim());
    browser.clear();

    let mut indices = filtered_indices
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    indices.clear();

    for (index, entry) in entries.iter().enumerate() {
        if history_entry_matches_filter(entry, &search_lower, failed_only) {
            browser.add(&history_entry_display(entry));
            indices.push(index);
        }
    }
}

#[cfg(test)]
mod query_history_tests {
    use super::{
        history_entry_matches_filter, parse_error_line, sanitize_history_message,
        sanitize_history_sql, truncate_sql, QueryHistoryEntry, REDACTED_SECRET,
    };

    #[test]
    fn truncate_sql_preserves_multibyte_text_while_normalizing_whitespace() {
        let sql = "  SELECT\t'프로시저 테스트'\nFROM dual  ";
        assert_eq!(truncate_sql(sql, 100), "SELECT '프로시저 테스트' FROM dual");
    }

    #[test]
    fn truncate_sql_truncates_on_char_boundary_for_multibyte_text() {
        let sql = "가나다라마바사";
        assert_eq!(truncate_sql(sql, 5), "가나...");
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

    #[test]
    fn sanitize_history_sql_redacts_connect_password() {
        let sql = "CONNECT scott/tiger@localhost:1521/ORCL";
        let sanitized = sanitize_history_sql(sql);
        assert!(sanitized.contains(&format!("scott/{}@", REDACTED_SECRET)));
        assert!(!sanitized.contains("tiger"));
    }

    #[test]
    fn sanitize_history_sql_keeps_connect_by_clause() {
        let sql = "SELECT * FROM emp CONNECT BY PRIOR empno = mgr";
        let sanitized = sanitize_history_sql(sql);
        assert_eq!(sanitized, sql);
    }

    #[test]
    fn sanitize_history_sql_redacts_identified_by_secret() {
        let sql = "CREATE USER app IDENTIFIED BY \"MySecret!\"";
        let sanitized = sanitize_history_sql(sql);
        assert!(sanitized.contains(&format!("IDENTIFIED BY \"{}\"", REDACTED_SECRET)));
        assert!(!sanitized.contains("MySecret!"));
    }

    #[test]
    fn sanitize_history_sql_redacts_unterminated_identified_by_quote_without_appending_quote() {
        let sql = "CREATE USER app IDENTIFIED BY 'MySecret!";
        let sanitized = sanitize_history_sql(sql);
        assert_eq!(
            sanitized,
            format!("CREATE USER app IDENTIFIED BY '{}", REDACTED_SECRET)
        );
        assert!(!sanitized.contains("MySecret!"));
    }

    #[test]
    fn sanitize_history_message_redacts_uri_password() {
        let message = "failed to connect via https://alice:pa55@example.com/path";
        let sanitized = sanitize_history_message(message);
        assert!(sanitized.contains(&format!("alice:{}@", REDACTED_SECRET)));
        assert!(!sanitized.contains("pa55"));
    }

    #[test]
    fn parse_error_line_prefers_primary_error_line_reference() {
        let message = "failed near line 1
ORA-06512: at line 27";
        assert_eq!(parse_error_line(message), Some(1));
    }

    #[test]
    fn sanitize_history_message_preserves_user_only_uri_format() {
        let sql = "failed to reach https://scott@db-host/service";
        let sanitized = sanitize_history_message(sql);
        assert!(sanitized.contains("https://scott@db-host/service"));
    }

    #[test]
    fn sanitize_history_sql_ignores_identified_by_in_comments_and_strings() {
        let sql = "/* IDENTIFIED BY should_not_change */
SELECT 'IDENTIFIED BY keep_me' FROM dual;
CREATE USER app IDENTIFIED BY real_secret;";
        let sanitized = sanitize_history_sql(sql);
        assert!(sanitized.contains("IDENTIFIED BY should_not_change"));
        assert!(sanitized.contains("IDENTIFIED BY keep_me"));
        assert!(sanitized.contains(&format!("IDENTIFIED BY {}", REDACTED_SECRET)));
        assert!(!sanitized.contains("real_secret"));
    }

    #[test]
    fn history_filter_matches_sql_and_connection() {
        let entry = QueryHistoryEntry {
            sql: "SELECT * FROM employees".to_string(),
            timestamp: "2026-02-21 10:00:00".to_string(),
            execution_time_ms: 10,
            row_count: 1,
            connection_name: "HRDEV".to_string(),
            success: true,
            error_message: None,
            error_line: None,
        };
        assert!(history_entry_matches_filter(&entry, "employees", false));
        assert!(history_entry_matches_filter(&entry, "hrdev", false));
        assert!(!history_entry_matches_filter(&entry, "orders", false));
    }

    #[test]
    fn history_filter_failed_only_excludes_success_rows() {
        let success_entry = QueryHistoryEntry {
            sql: "SELECT 1".to_string(),
            timestamp: "2026-02-21 10:00:00".to_string(),
            execution_time_ms: 5,
            row_count: 1,
            connection_name: "DEV".to_string(),
            success: true,
            error_message: None,
            error_line: None,
        };
        let failed_entry = QueryHistoryEntry {
            sql: "BROKEN".to_string(),
            timestamp: "2026-02-21 11:00:00".to_string(),
            execution_time_ms: 5,
            row_count: 0,
            connection_name: "DEV".to_string(),
            success: false,
            error_message: Some("ORA-00900 invalid SQL statement".to_string()),
            error_line: Some(1),
        };
        assert!(!history_entry_matches_filter(&success_entry, "", true));
        assert!(history_entry_matches_filter(&failed_entry, "", true));
    }
}
