use fltk::{
    app,
    button::Button,
    draw::set_cursor,
    enums::{Align, CallbackTrigger, Cursor, FrameType},
    frame::Frame,
    group::{Flex, FlexType},
    input::Input,
    prelude::*,
};
use oracle::{Connection, Error as OracleError};
use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::db::{
    lock_connection_with_activity, BindValue, BindVar, ColumnInfo, CursorResult, FormatItem,
    QueryExecutor, QueryResult, ScriptItem, SessionState, ToolCommand,
};
use crate::sql_text::{
    self, FORMAT_BLOCK_END_QUALIFIER_KEYWORDS, FORMAT_BLOCK_START_KEYWORDS, FORMAT_CLAUSE_KEYWORDS,
    FORMAT_CONDITION_KEYWORDS, FORMAT_CREATE_SUFFIX_BREAK_KEYWORDS, FORMAT_JOIN_MODIFIER_KEYWORDS,
};
use crate::ui::sql_depth::{
    is_depth, is_top_level_depth, paren_depths, split_top_level_keyword_groups,
    split_top_level_symbol_groups,
};

use super::*;

#[derive(Default)]
struct SelectTransformState {
    break_index: Option<usize>,
    previous_break_value: Option<String>,
    compute_of_index: Option<usize>,
    compute_on_index: Option<usize>,
    compute_group_value: Option<String>,
    compute_count: usize,
    compute_sum: f64,
    compute_sum_seen: bool,
    compute_sums: Vec<f64>,
    compute_seen_numeric: Vec<bool>,
}

// Flush streamed rows in bounded batches so very large fetches still surface
// progressive UI updates without waiting for oversized buffers.
// Send buffered rows when either:
// - first batch reaches 100 rows
// - 200ms passes
// - an additional batch reaches 10,000 rows
const PROGRESS_ROWS_INITIAL_BATCH: usize = 100;
const PROGRESS_ROWS_FLUSH_INTERVAL: Duration = Duration::from_millis(200);
const PROGRESS_ROWS_MAX_BATCH: usize = 10_000;
const MAX_SCRIPT_INCLUDE_DEPTH: usize = 64;
// For huge buffers, avoid an additional full/prefix reformat pass when remapping cursor position.
const CURSOR_MAPPING_FULL_REFORMAT_THRESHOLD_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone)]
struct ScriptExecutionFrame {
    items: Vec<ScriptItem>,
    index: usize,
    base_dir: PathBuf,
    source_path: Option<PathBuf>,
}

struct ResolvedScriptInclude {
    source_path: PathBuf,
    script_dir: PathBuf,
    items: Vec<ScriptItem>,
}

#[derive(Debug, Clone)]
enum PrintNamedData {
    Scalar(Option<String>),
    Cursor(CursorResult),
    CursorEmpty,
    Missing,
}

#[derive(Clone, Copy)]
struct ExecutionStartupPolicy {
    has_connect_command: bool,
    requires_connected_session: bool,
}

struct QueryExecutionCleanupGuard {
    sender: mpsc::Sender<QueryProgress>,
    current_query_connection: Arc<Mutex<Option<Arc<Connection>>>>,
    cancel_flag: Arc<Mutex<bool>>,
    query_running: Arc<Mutex<bool>>,
    timeout_connection: Option<Arc<Connection>>,
    previous_timeout: Option<Duration>,
}

impl QueryExecutionCleanupGuard {
    fn new(
        sender: mpsc::Sender<QueryProgress>,
        current_query_connection: Arc<Mutex<Option<Arc<Connection>>>>,
        cancel_flag: Arc<Mutex<bool>>,
        query_running: Arc<Mutex<bool>>,
    ) -> Self {
        Self {
            sender,
            current_query_connection,
            cancel_flag,
            query_running,
            timeout_connection: None,
            previous_timeout: None,
        }
    }

    fn track_timeout(&mut self, connection: Arc<Connection>, previous_timeout: Option<Duration>) {
        self.timeout_connection = Some(connection);
        self.previous_timeout = previous_timeout;
    }

    fn clear_timeout_tracking(&mut self) {
        self.timeout_connection = None;
        self.previous_timeout = None;
    }
}

impl Drop for QueryExecutionCleanupGuard {
    fn drop(&mut self) {
        SqlEditorWidget::set_current_query_connection(&self.current_query_connection, None);
        store_mutex_bool(&self.cancel_flag, false);
        // Keep execution state fail-safe even if the UI progress poller has
        // stopped (e.g. tab closed while worker thread is still unwinding).
        store_mutex_bool(&self.query_running, false);
        let _ = self.sender.send(QueryProgress::BatchFinished);
        app::awake();

        // Restoring call timeout is best-effort cleanup. Run it after shared
        // execution state is reset so cancel/timeout unwind paths never appear
        // to hang in "running" state if the driver stalls while resetting.
        if let Some(conn) = self.timeout_connection.as_ref() {
            let _ = conn.set_call_timeout(self.previous_timeout);
        }
    }
}

struct QueryRunningReservation {
    query_running: Arc<Mutex<bool>>,
    armed: bool,
}

impl QueryRunningReservation {
    fn acquire(query_running: Arc<Mutex<bool>>) -> Option<Self> {
        if try_mark_query_running(&query_running) {
            Some(Self {
                query_running,
                armed: true,
            })
        } else {
            None
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for QueryRunningReservation {
    fn drop(&mut self) {
        if self.armed {
            store_mutex_bool(&self.query_running, false);
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum OpenCursorFormatState {
    None,
    AwaitingFor,
    InSelect { anchor_indent: usize },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SelectListBreakState {
    None,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SelectListLayoutState {
    Inactive,
    Pending { anchor: usize, indent: usize },
    Multiline { indent: usize },
}

impl SelectListLayoutState {
    fn activate(&mut self, anchor: usize, indent: usize) {
        *self = Self::Pending { anchor, indent };
    }

    fn clear(&mut self) {
        *self = Self::Inactive;
    }

    fn is_multiline(self) -> bool {
        matches!(self, Self::Multiline { .. })
    }

    fn force_newline_if_possible(self, out: &mut String) -> Self {
        match self {
            Self::Pending { anchor, indent } => {
                if anchor < out.len() && out.as_bytes().get(anchor) == Some(&b' ') {
                    let indentation = " ".repeat(indent * 4);
                    out.replace_range(anchor..anchor + 1, &format!("\n{indentation}"));
                    Self::Multiline { indent }
                } else {
                    self
                }
            }
            _ => self,
        }
    }
}

impl SelectListBreakState {
    fn clear(&mut self) {
        *self = Self::None;
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExitConditionState {
    None,
    AwaitingWhen,
}

impl ExitConditionState {
    fn on_keyword(&mut self, keyword: &str) {
        match (keyword, *self) {
            ("EXIT" | "CONTINUE", _) => {
                *self = Self::AwaitingWhen;
            }
            ("WHEN", Self::AwaitingWhen) => {
                *self = Self::None;
            }
            _ => {}
        }
    }

    fn is_exit_when(self, keyword: &str) -> bool {
        keyword == "WHEN" && matches!(self, Self::AwaitingWhen)
    }

    fn clear(&mut self) {
        *self = Self::None;
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum WithCteFormatState {
    None,
    InDefinitions { paren_depth: usize },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TriggerHeaderState {
    None,
    InHeader,
}

impl TriggerHeaderState {
    fn is_active(self) -> bool {
        matches!(self, Self::InHeader)
    }

    fn start(&mut self) {
        *self = Self::InHeader;
    }

    fn clear(&mut self) {
        *self = Self::None;
    }
}

impl WithCteFormatState {
    fn on_clause_keyword(&mut self, keyword: &str) {
        match keyword {
            "WITH" => *self = Self::InDefinitions { paren_depth: 0 },
            "SELECT" if self.can_close_on_select() => *self = Self::None,
            _ => {}
        }
    }

    fn on_open_paren(&mut self) {
        if let Self::InDefinitions { paren_depth } = self {
            *paren_depth = paren_depth.saturating_add(1);
        }
    }

    fn on_close_paren(&mut self) {
        if let Self::InDefinitions { paren_depth } = self {
            *paren_depth = paren_depth.saturating_sub(1);
        }
    }

    fn can_close_on_select(self) -> bool {
        matches!(self, Self::InDefinitions { paren_depth: 0 })
    }
}

impl OpenCursorFormatState {
    fn base_indent(self, indent_level: usize) -> usize {
        match self {
            Self::InSelect { anchor_indent } => {
                anchor_indent + indent_level.saturating_sub(anchor_indent.saturating_sub(1))
            }
            _ => indent_level,
        }
    }

    fn in_select(self) -> bool {
        matches!(self, Self::InSelect { .. })
    }
}

impl SqlEditorWidget {
    fn starts_with_end_suffix_terminator(trimmed_upper: &str) -> bool {
        if !crate::sql_text::starts_with_keyword_token(trimmed_upper, "END") {
            return false;
        }

        let Some(rest) = trimmed_upper.strip_prefix("END") else {
            return false;
        };
        let rest = rest.trim_start();

        FORMAT_BLOCK_END_QUALIFIER_KEYWORDS
            .iter()
            .any(|keyword| crate::sql_text::starts_with_keyword_token(rest, keyword))
    }

    fn starts_with_plain_end(trimmed_upper: &str) -> bool {
        crate::sql_text::starts_with_keyword_token(trimmed_upper, "END")
            && !Self::starts_with_end_suffix_terminator(trimmed_upper)
    }

    fn starts_with_bare_end(trimmed_upper: &str) -> bool {
        if !Self::starts_with_plain_end(trimmed_upper) {
            return false;
        }

        let Some(rest) = trimmed_upper.strip_prefix("END") else {
            return false;
        };
        let rest = rest.trim_start();

        rest.is_empty() || rest.starts_with(';')
    }

    fn connection_info_for_ui(info: &ConnectionInfo) -> ConnectionInfo {
        let mut sanitized = info.clone();
        sanitized.clear_password();
        sanitized
    }

    fn db_activity_label_for_sql(sql: &str, script_mode: bool) -> String {
        let compact = sql.split_whitespace().collect::<Vec<_>>().join(" ");
        let preview = if compact.is_empty() {
            "<empty>".to_string()
        } else {
            compact.chars().take(72).collect::<String>()
        };
        let preview = if compact.chars().count() > 72 {
            format!("{}...", preview)
        } else {
            preview
        };
        if script_mode {
            format!("Executing script: {}", preview)
        } else {
            format!("Executing SQL: {}", preview)
        }
    }

    pub fn execute_sql_text(&self, sql: &str) {
        self.execute_sql(sql, false);
    }

    pub fn focus(&mut self) {
        self.group.show();
        let _ = self.editor.take_focus();
    }

    pub fn execute_current(&self) {
        let buffer = self.buffer.clone();
        let sql = buffer.text();

        if let Some((start, end)) = buffer.selection_position() {
            let (start, end) = if start <= end {
                (start, end)
            } else {
                (end, start)
            };

            if start != end {
                let selected_text = buffer.selection_text();
                if !selected_text.is_empty() {
                    // F5 runs script execution semantics even when only a range is selected.
                    self.execute_sql(&selected_text, true);
                    return;
                }
            }
        }

        self.execute_sql(&sql, true);
    }

    pub fn execute_statement_at_cursor(&self) {
        // Check if there's a selection
        let selected_text = self.buffer.selection_text();
        if !selected_text.is_empty() {
            // Execute selected text
            self.execute_sql(&selected_text, false);
        } else {
            // Execute statement at cursor position
            if let Some(statement) = self.statement_at_cursor_text() {
                let normalized = Self::normalize_statement_for_single_execution(&statement);
                self.execute_sql(&normalized, false);
            } else {
                SqlEditorWidget::show_alert_dialog("No SQL at cursor");
            }
        }
    }

    pub fn execute_selected(&self) {
        let mut buffer = self.buffer.clone();
        if !buffer.selected() {
            SqlEditorWidget::show_alert_dialog("No SQL selected");
            return;
        }

        let selection = buffer.selection_position();
        let insert_pos = self.editor.insert_position();
        let sql = buffer.selection_text();
        self.execute_sql(&sql, false);
        if let Some((start, end)) = selection {
            buffer.select(start, end);
            let mut editor = self.editor.clone();
            editor.set_insert_position(insert_pos);
            editor.show_insert_position();
        }
    }

    pub fn format_selected_sql(&self) {
        let mut buffer = self.buffer.clone();
        let full_text = buffer.text();
        let selection = buffer.selection_position();
        let (start, end, source, select_formatted) = match selection {
            Some((start, end)) if start != end => {
                let (start, end) = if start <= end {
                    (start, end)
                } else {
                    (end, start)
                };
                (
                    Self::normalize_index(&full_text, start),
                    Self::normalize_index(&full_text, end),
                    buffer.selection_text(),
                    true,
                )
            }
            _ => {
                let text = buffer.text();
                let end = Self::normalize_index(&full_text, buffer.length());
                (0, end, text, false)
            }
        };

        let mut formatted = Self::format_sql_basic(&source);
        if select_formatted {
            formatted = Self::preserve_selected_text_terminator(&source, formatted);
        }
        if formatted == source {
            return;
        }

        let mut editor = self.editor.clone();
        let original_pos = Self::normalize_index(&full_text, editor.insert_position());
        buffer.replace(start as i32, end as i32, &formatted);

        if select_formatted {
            let original_within_selection =
                (original_pos as isize - start as isize).clamp(0, source.len() as isize) as i32;
            let mapped_within_selection =
                Self::map_cursor_after_format(&source, &formatted, original_within_selection, true);
            let selection_end = start + Self::clamp_to_char_boundary(&formatted, formatted.len());
            let mapped_cursor =
                start + Self::clamp_to_char_boundary(&formatted, mapped_within_selection as usize);
            buffer.select(start as i32, selection_end as i32);
            editor.set_insert_position(mapped_cursor as i32);
        } else {
            let new_pos =
                Self::map_cursor_after_format(&source, &formatted, original_pos as i32, false);
            editor.set_insert_position(new_pos);
        }
        editor.show_insert_position();
        self.refresh_highlighting();
    }

    fn normalize_index(text: &str, index: i32) -> usize {
        if index <= 0 {
            0
        } else {
            Self::clamp_to_char_boundary(text, index as usize)
        }
    }

    fn clamp_to_char_boundary(text: &str, index: usize) -> usize {
        let mut idx = index.min(text.len());
        if text.is_char_boundary(idx) {
            return idx;
        }

        // Clamp invalid UTF-8 byte offsets to the previous valid boundary.
        while idx > 0 && !text.is_char_boundary(idx) {
            idx -= 1;
        }
        idx
    }

    fn map_cursor_after_format(
        source: &str,
        formatted: &str,
        original_pos: i32,
        preserve_selection_terminator: bool,
    ) -> i32 {
        if original_pos <= 0 {
            return 0;
        }

        let source_pos = Self::clamp_to_char_boundary(source, original_pos as usize);
        if source.len() >= CURSOR_MAPPING_FULL_REFORMAT_THRESHOLD_BYTES {
            if source.is_empty() || formatted.is_empty() {
                return 0;
            }
            let scaled_pos =
                (source_pos as u128).saturating_mul(formatted.len() as u128) / source.len() as u128;
            return Self::clamp_to_char_boundary(formatted, scaled_pos as usize) as i32;
        }

        let source_prefix = &source[..source_pos];
        let mut formatted_prefix = Self::format_sql_basic(source_prefix);
        if preserve_selection_terminator {
            formatted_prefix =
                Self::preserve_selected_text_terminator(source_prefix, formatted_prefix);
        }
        let formatted_pos = formatted_prefix.len().min(formatted.len());
        Self::clamp_to_char_boundary(formatted, formatted_pos) as i32
    }

    fn preserve_selected_text_terminator(source: &str, formatted: String) -> String {
        if Self::statement_ends_with_semicolon(source) {
            return formatted;
        }

        if let Some(without_semicolon) = Self::remove_trailing_statement_semicolon(&formatted) {
            return without_semicolon;
        }

        formatted
    }

    fn removable_trailing_semicolon_span(formatted: &str) -> Option<(usize, usize)> {
        let trimmed_len = formatted.trim_end().len();
        if trimmed_len == 0 {
            return None;
        }
        let trimmed = &formatted[..trimmed_len];
        let spans = super::query_text::tokenize_sql_spanned(trimmed);

        let mut semicolon_span: Option<(usize, usize)> = None;
        let mut trailing_line_comment_span: Option<(usize, usize)> = None;
        let mut has_non_comment_token_before_trailing_comment = false;

        for span in spans.iter().rev() {
            match &span.token {
                SqlToken::Comment(comment_text) if comment_text.starts_with("--") => {
                    if trailing_line_comment_span.is_none() {
                        trailing_line_comment_span = Some((span.start, span.end));
                    }
                    continue;
                }
                SqlToken::Comment(_) => continue,
                SqlToken::Symbol(sym) if sym == ";" => {
                    semicolon_span = Some((span.start, span.end));
                    break;
                }
                _ => {
                    if trailing_line_comment_span.is_some() {
                        has_non_comment_token_before_trailing_comment = true;
                    }
                    break;
                }
            }
        }

        if semicolon_span.is_some() {
            return semicolon_span;
        }

        let (comment_start, comment_end) = trailing_line_comment_span?;
        if !has_non_comment_token_before_trailing_comment || comment_end == comment_start {
            return None;
        }

        let comment_text = trimmed.get(comment_start..comment_end)?;
        if !comment_text.ends_with(';') {
            return None;
        }

        Some((comment_end - 1, comment_end))
    }

    fn remove_trailing_statement_semicolon(formatted: &str) -> Option<String> {
        let (semicolon_start, semicolon_end) = Self::removable_trailing_semicolon_span(formatted)?;

        let mut out = String::with_capacity(
            formatted
                .len()
                .saturating_sub(semicolon_end.saturating_sub(semicolon_start)),
        );
        out.push_str(&formatted[..semicolon_start]);
        out.push_str(&formatted[semicolon_end..]);
        Some(out)
    }

    pub fn toggle_comment(&self) {
        let mut buffer = self.buffer.clone();
        let mut editor = self.editor.clone();
        let selection = buffer.selection_position();
        let had_selection = matches!(selection, Some((start, end)) if start != end);
        let original_pos = editor.insert_position();

        let (start, end) = if let Some((start, end)) = selection {
            if start <= end {
                (start, end)
            } else {
                (end, start)
            }
        } else {
            let line_start = buffer.line_start(original_pos);
            let line_end = buffer.line_end(original_pos);
            (line_start, line_end)
        };

        let line_start = buffer.line_start(start);
        let line_end = buffer.line_end(end);
        let text = buffer.text_range(line_start, line_end).unwrap_or_default();
        let ends_with_newline = text.ends_with('\n');
        let lines: Vec<&str> = text.lines().collect();

        let all_commented = lines
            .iter()
            .filter(|line| !line.trim().is_empty())
            .all(|line| line.trim_start().starts_with("--"));

        let mut new_lines: Vec<String> = Vec::with_capacity(lines.len());
        for line in lines {
            if line.trim().is_empty() {
                new_lines.push(line.to_string());
                continue;
            }

            let prefix_len = line.len() - line.trim_start().len();
            let prefix = &line[..prefix_len];
            let trimmed = &line[prefix_len..];

            if all_commented {
                let uncommented = trimmed.strip_prefix("--").unwrap_or(trimmed);
                let uncommented = uncommented.strip_prefix(' ').unwrap_or(uncommented);
                new_lines.push(format!("{}{}", prefix, uncommented));
            } else if trimmed.starts_with("--") {
                new_lines.push(line.to_string());
            } else {
                new_lines.push(format!("{}-- {}", prefix, trimmed));
            }
        }

        let mut new_text = new_lines.join("\n");
        if ends_with_newline {
            new_text.push('\n');
        }

        buffer.replace(line_start, line_end, &new_text);
        let new_end = line_start + new_text.len() as i32;
        if had_selection {
            buffer.select(line_start, new_end);
            editor.set_insert_position(new_end);
        } else {
            let delta = new_text.len() as i32 - (line_end - line_start);
            let new_pos = if original_pos >= line_start {
                original_pos + delta
            } else {
                original_pos
            };
            editor.set_insert_position(new_pos);
        }
        editor.show_insert_position();
        self.refresh_highlighting();
    }

    pub fn convert_selection_case(&self, to_upper: bool) {
        let mut buffer = self.buffer.clone();
        let selection = buffer.selection_position();
        let (start, end) = match selection {
            Some((start, end)) if start != end => {
                if start <= end {
                    (start, end)
                } else {
                    (end, start)
                }
            }
            _ => {
                SqlEditorWidget::show_alert_dialog("No SQL selected");
                return;
            }
        };

        let selected = buffer.selection_text();
        let converted = if to_upper {
            selected.to_uppercase()
        } else {
            selected.to_lowercase()
        };

        if converted == selected {
            return;
        }

        buffer.replace(start, end, &converted);
        buffer.select(start, start + converted.len() as i32);

        let mut editor = self.editor.clone();
        editor.set_insert_position(start + converted.len() as i32);
        editor.show_insert_position();
        self.refresh_highlighting();
    }

    pub(crate) fn format_sql_basic(sql: &str) -> String {
        let mut formatted = String::with_capacity(sql.len().saturating_add(64));
        let items = super::query_text::split_format_items(sql);
        if items.is_empty() {
            return String::new();
        }

        let mut select_list_break_state = SelectListBreakState::None;
        for (idx, item) in items.iter().enumerate() {
            let next_item = items.get(idx + 1);

            match item {
                FormatItem::Statement(statement) => {
                    let statement_tokens = Self::tokenize_sql(statement);
                    let formatted_statement = Self::format_statement(
                        statement,
                        &statement_tokens,
                        select_list_break_state,
                    );
                    let has_code = Self::statement_has_code(statement, &statement_tokens);
                    formatted.push_str(&formatted_statement);
                    if has_code && !Self::statement_ends_with_semicolon_tokens(&statement_tokens) {
                        formatted.push(';');
                    }
                }
                FormatItem::ToolCommand(command) => {
                    formatted.push_str(&Self::format_tool_command(command));
                    select_list_break_state.clear();
                }
                FormatItem::Verbatim(text) => {
                    if let Some(command) = QueryExecutor::parse_tool_command(text)
                        .filter(|cmd| matches!(cmd, ToolCommand::Prompt { .. }))
                    {
                        formatted.push_str(&Self::format_tool_command(&command));
                    } else {
                        formatted.push_str(text);
                    }
                    select_list_break_state.clear();
                }
                FormatItem::Slash => {
                    formatted.push('/');
                    select_list_break_state.clear();
                }
            }

            if let Some(next_item) = next_item {
                formatted.push_str(Self::item_separator(item, next_item));
            }
        }

        formatted
    }

    fn item_separator(current: &FormatItem, next: &FormatItem) -> &'static str {
        if matches!(next, FormatItem::Slash) || Self::keeps_tight_spacing(current, next) {
            "\n"
        } else {
            "\n\n"
        }
    }

    fn keeps_tight_spacing(current: &FormatItem, next: &FormatItem) -> bool {
        match (current, next) {
            (FormatItem::Statement(left), FormatItem::Statement(right)) => {
                (Self::is_sqlplus_comment_line(left) && Self::is_sqlplus_comment_line(right))
                    || (Self::is_create_trigger_statement(left)
                        && Self::is_alter_trigger_statement(right))
            }
            _ if Self::is_prompt_format_item(current) && Self::is_prompt_format_item(next) => true,
            (
                FormatItem::ToolCommand(ToolCommand::ClearBreaks),
                FormatItem::ToolCommand(ToolCommand::ClearComputes),
            )
            | (
                FormatItem::ToolCommand(ToolCommand::ClearComputes),
                FormatItem::ToolCommand(ToolCommand::ClearBreaks),
            ) => true,
            _ => false,
        }
    }

    fn is_prompt_format_item(item: &FormatItem) -> bool {
        match item {
            FormatItem::ToolCommand(ToolCommand::Prompt { .. }) => true,
            FormatItem::Verbatim(text) => QueryExecutor::parse_tool_command(text)
                .is_some_and(|cmd| matches!(cmd, ToolCommand::Prompt { .. })),
            _ => false,
        }
    }

    fn is_sqlplus_comment_line(statement: &str) -> bool {
        crate::sql_text::is_sqlplus_comment_line(statement)
    }

    fn is_create_trigger_statement(statement: &str) -> bool {
        let mut word_idx = 0usize;
        let mut has_trigger_in_prefix = false;

        for token in Self::tokenize_sql(statement) {
            let SqlToken::Word(word) = token else {
                continue;
            };

            if word_idx == 0 && !word.eq_ignore_ascii_case("CREATE") {
                return false;
            }

            if word_idx < 8 && word.eq_ignore_ascii_case("TRIGGER") {
                has_trigger_in_prefix = true;
            }

            word_idx += 1;
        }

        word_idx > 0 && has_trigger_in_prefix
    }

    fn is_alter_trigger_statement(statement: &str) -> bool {
        let mut words = Self::tokenize_sql(statement)
            .into_iter()
            .filter_map(|token| match token {
                SqlToken::Word(word) => Some(word),
                _ => None,
            });

        matches!(
            (words.next(), words.next()),
            (Some(first), Some(second))
                if first.eq_ignore_ascii_case("ALTER")
                    && second.eq_ignore_ascii_case("TRIGGER")
        )
    }

    fn statement_has_code(statement: &str, tokens: &[SqlToken]) -> bool {
        let trimmed = statement.trim_start();
        if let Some(first_word) = trimmed.split_whitespace().next() {
            if first_word.eq_ignore_ascii_case("REM") || first_word.eq_ignore_ascii_case("REMARK") {
                return false;
            }
        }

        tokens
            .iter()
            .any(|token| !matches!(token, SqlToken::Comment(_)))
    }

    fn is_sqlplus_remark_comment_statement(statement: &str) -> bool {
        statement
            .trim_start()
            .split_whitespace()
            .next()
            .is_some_and(|first_word| {
                first_word.eq_ignore_ascii_case("REM") || first_word.eq_ignore_ascii_case("REMARK")
            })
    }

    fn statement_ends_with_semicolon(statement: &str) -> bool {
        let tokens = Self::tokenize_sql(statement);
        Self::statement_ends_with_semicolon_tokens(&tokens)
    }

    fn statement_ends_with_semicolon_tokens(tokens: &[SqlToken]) -> bool {
        for token in tokens.iter().rev() {
            match token {
                SqlToken::Comment(_) => continue,
                SqlToken::Symbol(sym) if sym == ";" => return true,
                _ => return false,
            }
        }
        false
    }

    fn format_tool_command(command: &ToolCommand) -> String {
        match command {
            ToolCommand::Var { name, data_type } => {
                format!("VAR {} {}", name, data_type.display())
            }
            ToolCommand::Print { name } => match name {
                Some(name) => format!("PRINT {}", name),
                None => "PRINT".to_string(),
            },
            ToolCommand::SetServerOutput {
                enabled,
                size,
                unlimited,
            } => {
                if !*enabled {
                    "SET SERVEROUTPUT OFF".to_string()
                } else if *unlimited {
                    "SET SERVEROUTPUT ON SIZE UNLIMITED".to_string()
                } else if let Some(size) = size {
                    format!("SET SERVEROUTPUT ON SIZE {}", size)
                } else {
                    "SET SERVEROUTPUT ON".to_string()
                }
            }
            ToolCommand::ShowErrors {
                object_type,
                object_name,
            } => {
                if let (Some(obj_type), Some(obj_name)) = (object_type, object_name) {
                    format!("SHOW ERRORS {} {}", obj_type, obj_name)
                } else {
                    "SHOW ERRORS".to_string()
                }
            }
            ToolCommand::ShowUser => "SHOW USER".to_string(),
            ToolCommand::ShowAll => "SHOW ALL".to_string(),
            ToolCommand::Describe { name } => format!("DESCRIBE {}", name),
            ToolCommand::Prompt { text } => {
                if text.trim().is_empty() {
                    "PROMPT".to_string()
                } else {
                    format!("PROMPT {}", text)
                }
            }
            ToolCommand::Pause { message } => match message {
                Some(text) if !text.trim().is_empty() => format!("PAUSE {}", text),
                _ => "PAUSE".to_string(),
            },
            ToolCommand::Accept { name, prompt } => match prompt {
                Some(text) => {
                    format!(
                        "ACCEPT {} PROMPT '{}'",
                        name,
                        Self::escape_sql_literal(text)
                    )
                }
                None => format!("ACCEPT {}", name),
            },
            ToolCommand::Define { name, value } => format!("DEFINE {} = {}", name, value),
            ToolCommand::Undefine { name } => format!("UNDEFINE {}", name),
            ToolCommand::ColumnNewValue {
                column_name,
                variable_name,
            } => format!("COLUMN {} NEW_VALUE {}", column_name, variable_name),
            ToolCommand::BreakOn { column_name } => format!("BREAK ON {}", column_name),
            ToolCommand::BreakOff => "BREAK OFF".to_string(),
            ToolCommand::ClearBreaks => "CLEAR BREAKS".to_string(),
            ToolCommand::ClearComputes => "CLEAR COMPUTES".to_string(),
            ToolCommand::ClearBreaksComputes => "CLEAR BREAKS\nCLEAR COMPUTES".to_string(),
            ToolCommand::Compute {
                mode,
                of_column,
                on_column,
            } => {
                let mode_text = match mode {
                    crate::db::ComputeMode::Sum => "SUM",
                    crate::db::ComputeMode::Count => "COUNT",
                };
                match (of_column.as_deref(), on_column.as_deref()) {
                    (Some(of_col), Some(on_col)) => {
                        format!("COMPUTE {} OF {} ON {}", mode_text, of_col, on_col)
                    }
                    _ => format!("COMPUTE {}", mode_text),
                }
            }
            ToolCommand::ComputeOff => "COMPUTE OFF".to_string(),
            ToolCommand::SetErrorContinue { enabled } => {
                if *enabled {
                    "SET ERRORCONTINUE ON".to_string()
                } else {
                    "SET ERRORCONTINUE OFF".to_string()
                }
            }
            ToolCommand::SetAutoCommit { enabled } => {
                if *enabled {
                    "SET AUTOCOMMIT ON".to_string()
                } else {
                    "SET AUTOCOMMIT OFF".to_string()
                }
            }
            ToolCommand::SetDefine {
                enabled,
                define_char,
            } => {
                if let Some(ch) = define_char {
                    format!("SET DEFINE '{}'", ch)
                } else if *enabled {
                    "SET DEFINE ON".to_string()
                } else {
                    "SET DEFINE OFF".to_string()
                }
            }
            ToolCommand::SetScan { enabled } => {
                if *enabled {
                    "SET SCAN ON".to_string()
                } else {
                    "SET SCAN OFF".to_string()
                }
            }
            ToolCommand::SetVerify { enabled } => {
                if *enabled {
                    "SET VERIFY ON".to_string()
                } else {
                    "SET VERIFY OFF".to_string()
                }
            }
            ToolCommand::SetEcho { enabled } => {
                if *enabled {
                    "SET ECHO ON".to_string()
                } else {
                    "SET ECHO OFF".to_string()
                }
            }
            ToolCommand::SetTiming { enabled } => {
                if *enabled {
                    "SET TIMING ON".to_string()
                } else {
                    "SET TIMING OFF".to_string()
                }
            }
            ToolCommand::SetFeedback { enabled } => {
                if *enabled {
                    "SET FEEDBACK ON".to_string()
                } else {
                    "SET FEEDBACK OFF".to_string()
                }
            }
            ToolCommand::SetHeading { enabled } => {
                if *enabled {
                    "SET HEADING ON".to_string()
                } else {
                    "SET HEADING OFF".to_string()
                }
            }
            ToolCommand::SetPageSize { size } => format!("SET PAGESIZE {}", size),
            ToolCommand::SetLineSize { size } => format!("SET LINESIZE {}", size),
            ToolCommand::SetTrimSpool { enabled } => {
                if *enabled {
                    "SET TRIMSPOOL ON".to_string()
                } else {
                    "SET TRIMSPOOL OFF".to_string()
                }
            }
            ToolCommand::SetTrimOut { enabled } => {
                if *enabled {
                    "SET TRIMOUT ON".to_string()
                } else {
                    "SET TRIMOUT OFF".to_string()
                }
            }
            ToolCommand::SetSqlBlankLines { enabled } => {
                if *enabled {
                    "SET SQLBLANKLINES ON".to_string()
                } else {
                    "SET SQLBLANKLINES OFF".to_string()
                }
            }
            ToolCommand::SetTab { enabled } => {
                if *enabled {
                    "SET TAB ON".to_string()
                } else {
                    "SET TAB OFF".to_string()
                }
            }
            ToolCommand::SetColSep { separator } => format!("SET COLSEP {}", separator),
            ToolCommand::SetNull { null_text } => format!("SET NULL {}", null_text),
            ToolCommand::Spool { path, append } => match path {
                Some(path) if *append => format!("SPOOL {} APPEND", path),
                Some(path) => format!("SPOOL {}", path),
                None if *append => "SPOOL APPEND".to_string(),
                None => "SPOOL OFF".to_string(),
            },
            ToolCommand::WheneverSqlError { exit, action } => {
                let mode = if *exit { "EXIT" } else { "CONTINUE" };
                match action.as_deref() {
                    Some(extra) if !extra.trim().is_empty() => {
                        format!("WHENEVER SQLERROR {} {}", mode, extra.trim())
                    }
                    _ => format!("WHENEVER SQLERROR {}", mode),
                }
            }
            ToolCommand::WheneverOsError { exit } => {
                if *exit {
                    "WHENEVER OSERROR EXIT".to_string()
                } else {
                    "WHENEVER OSERROR CONTINUE".to_string()
                }
            }
            ToolCommand::Exit => "EXIT".to_string(),
            ToolCommand::Quit => "QUIT".to_string(),
            ToolCommand::RunScript {
                path,
                relative_to_caller,
            } => {
                if *relative_to_caller {
                    format!("@@{}", path)
                } else {
                    format!("@{}", path)
                }
            }
            ToolCommand::Connect {
                username,
                password,
                host,
                port,
                service_name,
            } => {
                // 자동 포맷팅 결과를 AI(Codex/Claude)가 재마스킹하지 않도록 실제 비밀번호를 그대로 유지한다.
                format!(
                    "CONNECT {}/{}@{}:{}/{}",
                    username, password, host, port, service_name
                )
            }
            ToolCommand::Disconnect => "DISCONNECT".to_string(),
            ToolCommand::Unsupported { raw, .. } => raw.clone(),
        }
    }

    fn format_statement(
        statement: &str,
        tokens: &[SqlToken],
        select_list_break_state_on_start: SelectListBreakState,
    ) -> String {
        if Self::is_sqlplus_remark_comment_statement(statement) {
            return statement.trim().to_string();
        }

        if let Some(formatted) = Self::format_create_table(statement) {
            return formatted;
        }

        let clause_keywords = FORMAT_CLAUSE_KEYWORDS;
        let join_modifiers = FORMAT_JOIN_MODIFIER_KEYWORDS;
        let join_keyword = "JOIN";
        let outer_keyword = "OUTER";
        let condition_keywords = FORMAT_CONDITION_KEYWORDS; // ELSE handled separately for IF blocks
                                                            // BEGIN is handled separately to support DECLARE ... BEGIN ... END blocks
                                                            // CASE is handled separately for SELECT vs PL/SQL context
                                                            // LOOP is handled separately for FOR ... LOOP on same line
        let block_start_keywords = FORMAT_BLOCK_START_KEYWORDS;
        let block_end_qualifiers = FORMAT_BLOCK_END_QUALIFIER_KEYWORDS; // END LOOP, END IF, END CASE, END REPEAT

        let mut out = String::new();
        let mut indent_level = 0usize;
        let mut suppress_comma_break_depth = 0usize;
        let mut paren_stack: Vec<bool> = Vec::new();
        let mut paren_clause_restore_stack: Vec<Option<String>> = Vec::new();
        let mut block_stack: Vec<String> = Vec::new(); // Track which block keywords started blocks
        let mut at_line_start = true;
        let mut needs_space = false;
        let mut line_indent = 0usize;
        let mut join_modifier_active = false;
        let mut after_for_while = false; // Track FOR/WHILE for LOOP on same line
        let mut in_plsql_block = false; // Track if we're in PL/SQL block (for CASE handling)
        let mut prev_word_upper: Option<String> = None;
        let mut create_pending = false;
        let mut create_object: Option<String> = None;
        let mut routine_decl_pending = false;
        let mut create_table_paren_expected = false;
        let mut column_list_stack: Vec<bool> = Vec::new();
        let mut current_clause: Option<String> = None;
        let mut pending_package_member_separator = false;
        let mut open_cursor_state = OpenCursorFormatState::None;
        let mut case_branch_started: Vec<bool> = Vec::new();
        let mut between_pending = false;
        let mut select_list_layout_state = SelectListLayoutState::Inactive;
        let mut select_list_break_state = select_list_break_state_on_start;
        let mut exit_condition_state = ExitConditionState::None;
        let mut with_cte_state = WithCteFormatState::None;
        let mut statement_has_with_clause = false;
        let mut paren_indent_increase_stack: Vec<usize> = Vec::new();
        let mut trigger_header_state = TriggerHeaderState::None;
        let is_package_body_statement = {
            let upper = statement.to_ascii_uppercase();
            upper.contains("CREATE OR REPLACE PACKAGE BODY")
                || upper.contains("CREATE PACKAGE BODY")
        };

        let newline_with = |out: &mut String,
                            indent_level: usize,
                            extra: usize,
                            at_line_start: &mut bool,
                            needs_space: &mut bool,
                            line_indent: &mut usize| {
            if !out.is_empty() && !out.ends_with('\n') {
                out.push('\n');
            }
            *line_indent = indent_level + extra;
            *at_line_start = true;
            *needs_space = false;
        };

        let base_indent = |indent_level: usize, open_cursor_state: OpenCursorFormatState| {
            open_cursor_state.base_indent(indent_level)
        };

        let ensure_indent = |out: &mut String, at_line_start: &mut bool, line_indent: usize| {
            if *at_line_start {
                out.push_str(&" ".repeat(line_indent * 4));
                *at_line_start = false;
            }
        };

        let trim_trailing_space = |out: &mut String| {
            while out.ends_with(' ') {
                out.pop();
            }
        };

        let force_select_list_newline =
            |out: &mut String, select_list_layout_state: &mut SelectListLayoutState| {
                *select_list_layout_state = select_list_layout_state.force_newline_if_possible(out);
            };

        let mut idx = 0;
        while idx < tokens.len() {
            let next_word = tokens[idx + 1..].iter().find_map(|t| match t {
                SqlToken::Word(w) => Some(w.as_str()),
                _ => None,
            });
            let next_word_is =
                |expected: &str| next_word.is_some_and(|word| word.eq_ignore_ascii_case(expected));

            match &tokens[idx] {
                SqlToken::Word(word) => {
                    let upper = word.to_uppercase();
                    let in_sql_case_clause = matches!(
                        current_clause.as_deref(),
                        Some(
                            "SELECT"
                                | "WHERE"
                                | "ORDER"
                                | "GROUP"
                                | "HAVING"
                                | "VALUES"
                                | "SET"
                                | "INTO"
                        )
                    );
                    let is_keyword = sql_text::is_oracle_sql_keyword(upper.as_str());
                    let is_or_in_create = upper == "OR"
                        && matches!(prev_word_upper.as_deref(), Some("CREATE"))
                        && next_word_is("REPLACE");
                    let is_insert_into =
                        upper == "INTO" && matches!(prev_word_upper.as_deref(), Some("INSERT"));
                    let is_merge_into =
                        upper == "INTO" && matches!(prev_word_upper.as_deref(), Some("MERGE"));
                    let is_start_with =
                        upper == "WITH" && matches!(prev_word_upper.as_deref(), Some("START"));
                    let is_within_group =
                        upper == "GROUP" && matches!(prev_word_upper.as_deref(), Some("WITHIN"));
                    let mut newline_after_keyword = false;
                    let is_between_and = upper == "AND" && between_pending;
                    let is_exit_when = exit_condition_state.is_exit_when(upper.as_str());
                    let is_trigger_event_keyword = trigger_header_state.is_active()
                        && matches!(upper.as_str(), "INSERT" | "UPDATE" | "DELETE");
                    let is_trigger_or_on_keyword =
                        trigger_header_state.is_active() && matches!(upper.as_str(), "OR" | "ON");
                    let suppress_order_clause_break =
                        suppress_comma_break_depth > 0 && upper == "ORDER";
                    let at_package_body_member_depth = is_package_body_statement && indent_level == 1;
                    if upper == "END" {
                        let end_qualifier = {
                            let mut qualifier = None;
                            for t in &tokens[idx + 1..] {
                                match t {
                                    SqlToken::Comment(comment) => {
                                        if comment.contains('\n') {
                                            break;
                                        }
                                    }
                                    SqlToken::Word(w) => {
                                        qualifier = Some(w.to_uppercase());
                                        break;
                                    }
                                    SqlToken::Symbol(sym) if sym == ";" => break,
                                    _ => break,
                                }
                            }
                            qualifier
                        };
                        // Check if this is END LOOP, END IF, END CASE, etc.
                        let mut end_tail: Vec<String> = Vec::new();
                        if let Some(qualifier) = end_qualifier.as_deref() {
                            match qualifier {
                                "LOOP" | "IF" | "CASE" | "REPEAT" | "FOR" | "WHILE" => {
                                    end_tail.push(qualifier.to_string());
                                }
                                "BEFORE" | "AFTER" => {
                                    end_tail.push(qualifier.to_string());
                                    let mut lookahead = idx + 1;
                                    while lookahead < tokens.len() {
                                        match &tokens[lookahead] {
                                            SqlToken::Comment(comment) => {
                                                if !comment.contains('\n') {
                                                    lookahead += 1;
                                                    continue;
                                                }
                                                break;
                                            }
                                            SqlToken::Word(word) => {
                                                let qualifier_part = word.to_uppercase();
                                                if end_tail
                                                    .last()
                                                    .is_some_and(|value| value == "EACH")
                                                {
                                                    if qualifier_part == "ROW" {
                                                        end_tail.push(qualifier_part);
                                                    }
                                                    break;
                                                }
                                                if matches!(
                                                    qualifier_part.as_str(),
                                                    "EACH" | "STATEMENT"
                                                ) {
                                                    end_tail.push(qualifier_part);
                                                    lookahead += 1;
                                                    continue;
                                                }
                                                break;
                                            }
                                            SqlToken::Symbol(sym) if sym == ";" => break,
                                            _ => break,
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                        let is_qualified_end = matches!(
                            end_tail.first().map(String::as_str),
                            Some("LOOP" | "IF" | "CASE" | "REPEAT" | "FOR" | "WHILE")
                        );
                        let paren_extra = if suppress_comma_break_depth > 0 { 1 } else { 0 };

                        let case_expression_end =
                            !is_qualified_end && block_stack.last().is_some_and(|s| s == "CASE");

                        if is_qualified_end {
                            // END LOOP, END IF, END CASE - pop matching block
                            if let Some(top) = block_stack.last() {
                                if block_end_qualifiers.contains(&top.as_str()) {
                                    block_stack.pop();
                                }
                            }
                            if end_tail.first().is_some_and(|q| q == "CASE")
                                && !case_branch_started.is_empty()
                            {
                                case_branch_started.pop();
                            }
                        } else if case_expression_end {
                            block_stack.pop();
                            if !case_branch_started.is_empty() {
                                case_branch_started.pop();
                            }
                        } else {
                            // Plain END - closes BEGIN or DECLARE/PACKAGE_BODY block
                            // Pop until we find BEGIN or DECLARE/PACKAGE_BODY
                            let mut closed_block = None;
                            while let Some(top) = block_stack.pop() {
                                if top == "BEGIN" || top == "DECLARE" || top == "PACKAGE_BODY" {
                                    closed_block = Some(top);
                                    break;
                                }
                            }
                            if matches!(closed_block.as_deref(), Some("BEGIN" | "DECLARE"))
                                && (block_stack.last().is_some_and(|s| s == "PACKAGE_BODY")
                                    || at_package_body_member_depth)
                            {
                                pending_package_member_separator = true;
                            }
                        }

                        indent_level = indent_level.saturating_sub(1);
                        if is_package_body_statement
                            && !is_qualified_end
                            && !case_expression_end
                            && indent_level == 1
                        {
                            pending_package_member_separator = true;
                        }
                        let end_extra =
                            if case_expression_end && (in_sql_case_clause || !in_plsql_block) {
                                1
                            } else {
                                0
                            };
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            end_extra + paren_extra,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );

                        // Output "END"
                        ensure_indent(&mut out, &mut at_line_start, line_indent);
                        out.push_str("END");

                        // If qualified (END LOOP/IF/CASE/REPEAT/BEFORE/AFTER), output tail and skip it.
                        let skip_count = end_tail.len();
                        for qualifier in end_tail.iter() {
                            out.push(' ');
                            out.push_str(qualifier);
                        }
                        needs_space = true;
                        if skip_count == 0 {
                            idx += 1;
                        } else {
                            let mut lookahead = idx + 1;
                            let mut words_skipped = 0usize;
                            while lookahead < tokens.len() && words_skipped < skip_count {
                                match &tokens[lookahead] {
                                    SqlToken::Word(_) => {
                                        words_skipped += 1;
                                    }
                                    SqlToken::Comment(comment) => {
                                        if comment.contains('\n') {
                                            break;
                                        }
                                    }
                                    _ => {}
                                }
                                lookahead += 1;
                            }
                            idx = lookahead;
                        }
                        continue;
                    } else if trigger_header_state.is_active()
                        && matches!(upper.as_str(), "BEFORE" | "AFTER" | "INSTEAD")
                    {
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            1,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    } else if is_trigger_event_keyword
                        && matches!(prev_word_upper.as_deref(), Some("BEFORE" | "AFTER" | "OF"))
                    {
                        // Keep trigger event verbs on the same line as BEFORE/AFTER/INSTEAD OF.
                    } else if clause_keywords.contains(&upper.as_str())
                        && !is_insert_into
                        && !is_merge_into
                        && !is_start_with
                        && !suppress_order_clause_break
                        && !is_trigger_event_keyword
                    {
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            if is_within_group { 1 } else { 0 },
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                        if !is_within_group {
                            current_clause = Some(upper.clone());
                            if upper != "SELECT" {
                                select_list_layout_state.clear();
                            }
                            if upper == "SELECT" && open_cursor_state.in_select() {
                                // Keep OPEN ... FOR SELECT inside the cursor SQL context.
                            }
                            if upper == "WITH" {
                                with_cte_state.on_clause_keyword("WITH");
                                statement_has_with_clause = true;
                            } else if upper == "SELECT" {
                                // Main query SELECT after CTE definitions.
                                with_cte_state.on_clause_keyword("SELECT");
                            }
                        }
                    } else if condition_keywords.contains(&upper.as_str())
                        && !is_or_in_create
                        && !is_between_and
                        && !is_exit_when
                        && !is_trigger_or_on_keyword
                    {
                        let paren_extra = if suppress_comma_break_depth > 0 { 1 } else { 0 };
                        if upper == "WHEN"
                            && block_stack.last().is_some_and(|s| s == "CASE")
                            && case_branch_started.last().is_some()
                        {
                            if let Some(last) = case_branch_started.last_mut() {
                                *last = true;
                            }
                        }
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            1 + paren_extra,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    } else if upper == "CREATE" {
                        create_pending = true;
                        create_object = None;
                    } else if create_pending && (upper == "OR" || upper == "REPLACE") {
                        // part of CREATE OR REPLACE
                    } else if create_pending && upper == "PACKAGE" {
                        if next_word_is("BODY") {
                            create_object = Some("PACKAGE_BODY".to_string());
                        } else {
                            create_object = Some("PACKAGE".to_string());
                        }
                        create_pending = false;
                    } else if create_pending && upper == "TABLE" {
                        create_table_paren_expected = true;
                        create_pending = false;
                    } else if create_pending
                        && matches!(
                            upper.as_str(),
                            "PROCEDURE" | "FUNCTION" | "TYPE" | "TRIGGER"
                        )
                    {
                        create_object = Some(upper.clone());
                        if upper == "TRIGGER" {
                            trigger_header_state.start();
                        }
                        create_pending = false;
                    } else if matches!(upper.as_str(), "PROCEDURE" | "FUNCTION")
                        && (block_stack.iter().any(|s| s == "PACKAGE_BODY")
                            || at_package_body_member_depth)
                    {
                        if !at_line_start {
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                0,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        }
                        routine_decl_pending = true;
                    } else if upper == "ELSE" || upper == "ELSIF" {
                        // ELSE/ELSIF in IF block: same level as IF
                        let in_if_block = block_stack.last().is_some_and(|s| s == "IF");
                        let in_case_block = block_stack.last().is_some_and(|s| s == "CASE");
                        let paren_extra = if suppress_comma_break_depth > 0 { 1 } else { 0 };
                        if upper == "ELSE"
                            && in_case_block
                            && case_branch_started.last().is_some()
                            && !in_if_block
                        {
                            if let Some(last) = case_branch_started.last_mut() {
                                *last = true;
                            }
                        }
                        if in_if_block {
                            newline_with(
                                &mut out,
                                base_indent(indent_level.saturating_sub(1), open_cursor_state),
                                0,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        } else {
                            // ELSE in CASE or other context
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                1 + paren_extra,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        }
                        if upper == "ELSE"
                            && in_plsql_block
                            && !matches!(current_clause.as_deref(), Some("SELECT"))
                        {
                            newline_after_keyword = true;
                        } else if upper == "ELSE" && in_sql_case_clause && next_word_is("CASE") {
                            // Keep ELSE CASE from collapsing into one long SQL expression line.
                            newline_after_keyword = true;
                        }
                    } else if upper == "THEN" {
                        if in_plsql_block && !matches!(current_clause.as_deref(), Some("SELECT")) {
                            newline_after_keyword = true;
                        } else if in_sql_case_clause && next_word_is("CASE") {
                            // Nested CASE in SQL expressions should start on its own line.
                            newline_after_keyword = true;
                        }
                    } else if upper == join_keyword {
                        if !join_modifier_active {
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                0,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        }
                        join_modifier_active = false;
                    } else if join_modifiers.contains(&upper.as_str()) {
                        if next_word_is("JOIN") || next_word_is("OUTER") {
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                0,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                            join_modifier_active = true;
                        }
                    } else if upper == outer_keyword {
                        if next_word_is("JOIN") && !join_modifier_active {
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                0,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                            join_modifier_active = true;
                        }
                    } else if upper == "OPEN" {
                        open_cursor_state = OpenCursorFormatState::AwaitingFor;
                    } else if upper == "FOR" || upper == "WHILE" {
                        if upper == "FOR" && trigger_header_state.is_active() {
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                1,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                            after_for_while = false;
                        } else if upper == "FOR"
                            && open_cursor_state == OpenCursorFormatState::AwaitingFor
                        {
                            open_cursor_state = OpenCursorFormatState::InSelect {
                                anchor_indent: indent_level.saturating_add(1),
                            };
                            newline_after_keyword = true;
                        } else {
                            // FOR/WHILE starts a line, LOOP will follow on same line
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                0,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                            after_for_while = true;
                        }
                    } else if upper == "LOOP" {
                        // LOOP after FOR/WHILE stays on same line
                        if !after_for_while {
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                0,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        }
                        after_for_while = false;
                        // LOOP always starts a block body on the next line.
                        newline_after_keyword = true;
                    } else if upper == "REPEAT" {
                        // REPEAT starts a block body on the next line.
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            0,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    } else if upper == "CASE" {
                        // CASE in PL/SQL block vs SELECT context
                        if in_sql_case_clause {
                            let paren_extra = if suppress_comma_break_depth > 0 { 1 } else { 0 };
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                1 + paren_extra,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        } else if in_plsql_block {
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                0,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        }
                        // In SELECT context, CASE stays inline
                    } else if block_start_keywords.contains(&upper.as_str()) {
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            0,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    } else if upper == "BEGIN" {
                        // BEGIN handling: check if we're inside a DECLARE block
                        let inside_declare = block_stack
                            .last()
                            .is_some_and(|s| s == "DECLARE" || s == "PACKAGE_BODY");
                        if inside_declare {
                            // DECLARE ... BEGIN - BEGIN is at same level as DECLARE
                            // Don't increase indent, just newline at current level
                            newline_with(
                                &mut out,
                                base_indent(indent_level.saturating_sub(1), open_cursor_state),
                                0,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        } else {
                            // Standalone BEGIN block
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                0,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        }
                    }

                    ensure_indent(&mut out, &mut at_line_start, line_indent);
                    if needs_space {
                        out.push(' ');
                    }
                    if is_keyword {
                        out.push_str(&upper);
                    } else {
                        out.push_str(word);
                    }
                    needs_space = true;
                    if upper == "SELECT" {
                        select_list_layout_state
                            .activate(out.len(), base_indent(indent_level, open_cursor_state) + 1);
                    }

                    if create_table_paren_expected
                        && upper == "AS"
                        && (next_word_is("SELECT") || next_word_is("WITH") || next_word_is("VALUES"))
                    {
                        create_table_paren_expected = false;
                    }

                    let starts_create_block = matches!(upper.as_str(), "AS" | "IS")
                        && (create_object.is_some() || routine_decl_pending);

                    // Handle block start - push to stack and increase indent
                    if block_start_keywords.contains(&upper.as_str()) {
                        block_stack.push(upper.clone());
                        indent_level += 1;
                        if upper == "DECLARE" || upper == "IF" {
                            in_plsql_block = true;
                        }
                    } else if upper == "BEGIN" {
                        let inside_declare = block_stack
                            .last()
                            .is_some_and(|s| s == "DECLARE" || s == "PACKAGE_BODY");
                        if inside_declare {
                            // DECLARE ... BEGIN - same block depth.
                            // PACKAGE BODY initialization BEGIN is also same depth as PACKAGE_BODY.
                            if block_stack.last().is_some_and(|s| s == "DECLARE") {
                                block_stack.pop();
                                block_stack.push("BEGIN".to_string());
                            }
                            // indent_level stays the same for both cases
                        } else {
                            // Standalone BEGIN block
                            block_stack.push("BEGIN".to_string());
                            indent_level += 1;
                        }
                        in_plsql_block = true;
                    } else if upper == "LOOP" {
                        block_stack.push("LOOP".to_string());
                        indent_level += 1;
                    } else if upper == "REPEAT" {
                        block_stack.push("REPEAT".to_string());
                        indent_level += 1;
                        in_plsql_block = true;
                    } else if upper == "CASE" {
                        block_stack.push("CASE".to_string());
                        if in_plsql_block && current_clause.is_none() {
                            case_branch_started.push(false);
                        }
                        indent_level += 1;
                    } else if starts_create_block {
                        // Treat AS/IS in CREATE PACKAGE/PROC/FUNC/TYPE/TRIGGER and package-body routines as declaration section start
                        let is_package_body =
                            matches!(create_object.as_deref(), Some("PACKAGE_BODY"));
                        if is_package_body {
                            block_stack.push("PACKAGE_BODY".to_string());
                        } else {
                            block_stack.push("DECLARE".to_string());
                        }
                        indent_level += 1;
                        in_plsql_block = true;
                        create_object = None;
                        routine_decl_pending = false;
                        newline_with(
                            &mut out,
                            indent_level,
                            0,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    }

                    if upper == "DECLARE" || upper == "BEGIN" {
                        if upper == "BEGIN" {
                            trigger_header_state.clear();
                        }
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            0,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    }

                    if newline_after_keyword {
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            0,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    }

                    if upper == "BETWEEN" {
                        between_pending = true;
                    } else if upper == "AND" && between_pending {
                        between_pending = false;
                    }
                    exit_condition_state.on_keyword(upper.as_str());

                    prev_word_upper = Some(upper);
                }
                SqlToken::String(literal) => {
                    let started_line = at_line_start;
                    ensure_indent(&mut out, &mut at_line_start, line_indent);
                    if needs_space {
                        out.push(' ');
                    }
                    out.push_str(literal);
                    needs_space = true;
                    if literal.contains('\n') {
                        at_line_start = true;
                    }
                    if started_line {}
                }
                SqlToken::Comment(comment) => {
                    let has_leading_newline = comment.starts_with('\n');
                    let comment_body = if has_leading_newline {
                        &comment[1..]
                    } else {
                        comment.as_str()
                    };
                    let trimmed_comment = comment_body.trim_end_matches('\n');
                    let is_block_comment =
                        trimmed_comment.starts_with("/*") && trimmed_comment.ends_with("*/");
                    let next_is_word_like = matches!(
                        tokens.get(idx + 1),
                        Some(SqlToken::Word(_) | SqlToken::String(_))
                    );
                    let in_select_list = matches!(current_clause.as_deref(), Some("SELECT"));
                    let top_level_select_list =
                        in_select_list && suppress_comma_break_depth == 0 && paren_stack.is_empty();
                    if top_level_select_list && !has_leading_newline {
                        force_select_list_newline(&mut out, &mut select_list_layout_state);
                    }

                    if has_leading_newline {
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            0,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    } else if !at_line_start {
                        out.push(' ');
                    }

                    let comment_starts_line = at_line_start;
                    if comment_starts_line {
                        let base = base_indent(indent_level, open_cursor_state);
                        let current_select_indent = base + 1;
                        if has_leading_newline {
                            line_indent =
                                if in_select_list && select_list_layout_state.is_multiline() {
                                    current_select_indent
                                } else {
                                    base
                                };
                        } else if top_level_select_list {
                            line_indent = if select_list_layout_state.is_multiline() {
                                current_select_indent
                            } else {
                                base
                            };
                        } else if in_select_list
                            || column_list_stack.last().copied().unwrap_or(false)
                        {
                            line_indent = current_select_indent;
                        } else if line_indent == 0 {
                            line_indent = base;
                        }
                        ensure_indent(&mut out, &mut at_line_start, line_indent);
                    }

                    let output_comment = if comment_body.trim_start().starts_with("--") {
                        comment_body.to_string()
                    } else if Self::is_sqlplus_comment_line(comment_body) {
                        comment_body.to_uppercase()
                    } else {
                        comment_body.to_string()
                    };
                    out.push_str(&output_comment);

                    needs_space = true;
                    if comment_body.ends_with('\n') || comment_body.contains('\n') {
                        at_line_start = true;
                        needs_space = false;
                        if in_select_list || column_list_stack.last().copied().unwrap_or(false) {
                            line_indent = base_indent(indent_level, open_cursor_state) + 1;
                        }
                    } else if is_block_comment && next_is_word_like {
                        let list_extra = if in_select_list
                            || column_list_stack.last().copied().unwrap_or(false)
                        {
                            1
                        } else {
                            0
                        };
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            list_extra,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    } else if comment_starts_line {
                    }
                }
                SqlToken::Symbol(sym) => {
                    let started_line = at_line_start;
                    match sym.as_str() {
                        "," => {
                            if statement_has_with_clause
                                && matches!(current_clause.as_deref(), Some("SELECT"))
                                && !open_cursor_state.in_select()
                                && suppress_comma_break_depth == 0
                            {
                                force_select_list_newline(&mut out, &mut select_list_layout_state);
                            }
                            trim_trailing_space(&mut out);
                            out.push(',');
                            between_pending = false;
                            let is_with_cte_separator = with_cte_state.can_close_on_select();
                            if column_list_stack.last().copied().unwrap_or(false) {
                                newline_with(
                                    &mut out,
                                    base_indent(indent_level, open_cursor_state),
                                    1,
                                    &mut at_line_start,
                                    &mut needs_space,
                                    &mut line_indent,
                                );
                            } else if is_with_cte_separator {
                                newline_with(
                                    &mut out,
                                    base_indent(indent_level, open_cursor_state),
                                    0,
                                    &mut at_line_start,
                                    &mut needs_space,
                                    &mut line_indent,
                                );
                            } else if suppress_comma_break_depth == 0 {
                                newline_with(
                                    &mut out,
                                    base_indent(indent_level, open_cursor_state),
                                    1,
                                    &mut at_line_start,
                                    &mut needs_space,
                                    &mut line_indent,
                                );
                                if matches!(current_clause.as_deref(), Some("SELECT")) {
                                    // The select list is already multiline after the first comma.
                                    // Avoid retroactively forcing a newline right after SELECT.
                                    let select_list_indent =
                                        base_indent(indent_level, open_cursor_state) + 1;
                                    select_list_layout_state = SelectListLayoutState::Multiline {
                                        indent: select_list_indent,
                                    };
                                }
                            } else {
                                out.push(' ');
                                needs_space = false;
                            }
                        }
                        ";" => {
                            trim_trailing_space(&mut out);
                            out.push(';');
                            current_clause = None;
                            select_list_layout_state.clear();
                            open_cursor_state = OpenCursorFormatState::None;
                            between_pending = false;
                            trigger_header_state.clear();
                            exit_condition_state.clear();
                            if pending_package_member_separator
                                && (next_word_is("PROCEDURE") || next_word_is("FUNCTION"))
                            {
                                out.push_str("\n\n");
                            }
                            pending_package_member_separator = false;
                            routine_decl_pending = false;
                            let should_reset_paren_tracking =
                                indent_level == 0 || block_stack.is_empty();
                            if should_reset_paren_tracking {
                                select_list_break_state.clear();
                            }
                            newline_with(
                                &mut out,
                                indent_level,
                                0,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                            if indent_level == 0 {
                                out.push('\n');
                                at_line_start = true;
                                needs_space = false;
                            }
                        }
                        "(" => {
                            with_cte_state.on_open_paren();
                            if matches!(current_clause.as_deref(), Some("SELECT"))
                                && matches!(prev_word_upper.as_deref(), Some("SELECT"))
                            {
                                newline_with(
                                    &mut out,
                                    base_indent(indent_level, open_cursor_state),
                                    1,
                                    &mut at_line_start,
                                    &mut needs_space,
                                    &mut line_indent,
                                );
                            }

                            ensure_indent(&mut out, &mut at_line_start, line_indent);
                            let is_subquery = next_word_is("SELECT")
                                || next_word_is("WITH")
                                || next_word_is("INSERT")
                                || next_word_is("UPDATE")
                                || next_word_is("DELETE")
                                || next_word_is("MERGE")
                                || next_word_is("VALUES");
                            if needs_space {
                                out.push(' ');
                            }
                            out.push('(');
                            let is_column_list = create_table_paren_expected;
                            create_table_paren_expected = false;
                            paren_stack.push(is_subquery);
                            paren_clause_restore_stack.push(if is_subquery {
                                current_clause.clone()
                            } else {
                                None
                            });
                            column_list_stack.push(is_column_list);
                            let indent_increase = if is_subquery || is_column_list {
                                let in_cte_as_subquery =
                                    matches!(
                                        with_cte_state,
                                        WithCteFormatState::InDefinitions { .. }
                                    ) && matches!(prev_word_upper.as_deref(), Some("AS"));
                                let deep_subquery_indent =
                                    matches!(current_clause.as_deref(), Some("SELECT" | "FROM"))
                                        && !in_cte_as_subquery
                                        || (matches!(current_clause.as_deref(), Some("WHERE"))
                                            && matches!(
                                                prev_word_upper.as_deref(),
                                                Some("EXISTS" | "IN")
                                            ));
                                if is_subquery && deep_subquery_indent {
                                    2
                                } else {
                                    1
                                }
                            } else {
                                0
                            };
                            paren_indent_increase_stack.push(indent_increase);
                            if indent_increase > 0 {
                                indent_level += indent_increase;
                                newline_with(
                                    &mut out,
                                    base_indent(indent_level, open_cursor_state),
                                    0,
                                    &mut at_line_start,
                                    &mut needs_space,
                                    &mut line_indent,
                                );
                            } else {
                                suppress_comma_break_depth += 1;
                            }
                            needs_space = false;
                        }
                        ")" => {
                            with_cte_state.on_close_paren();
                            trim_trailing_space(&mut out);
                            let was_subquery = paren_stack.pop().unwrap_or(false);
                            let restore_clause = paren_clause_restore_stack.pop().unwrap_or(None);
                            let was_column_list = column_list_stack.pop().unwrap_or(false);
                            let indent_increase = paren_indent_increase_stack.pop().unwrap_or(0);
                            let close_case_paren_on_newline = !was_subquery
                                && !was_column_list
                                && suppress_comma_break_depth > 0
                                && out.trim_end().ends_with("END");
                            if was_subquery || was_column_list {
                                if indent_level > 0 && indent_increase > 0 {
                                    indent_level = indent_level.saturating_sub(indent_increase);
                                }
                                newline_with(
                                    &mut out,
                                    base_indent(indent_level, open_cursor_state),
                                    indent_increase.saturating_sub(1),
                                    &mut at_line_start,
                                    &mut needs_space,
                                    &mut line_indent,
                                );
                                ensure_indent(&mut out, &mut at_line_start, line_indent);
                            } else {
                                suppress_comma_break_depth =
                                    suppress_comma_break_depth.saturating_sub(1);
                            }
                            if close_case_paren_on_newline {
                                let close_case_extra_indent =
                                    usize::from(!open_cursor_state.in_select());
                                let close_case_indent_level =
                                    if next_word_is("ELSE") || next_word_is("WHEN") {
                                        indent_level.saturating_sub(1)
                                    } else {
                                        indent_level
                                    };
                                newline_with(
                                    &mut out,
                                    close_case_indent_level,
                                    close_case_extra_indent,
                                    &mut at_line_start,
                                    &mut needs_space,
                                    &mut line_indent,
                                );
                                ensure_indent(&mut out, &mut at_line_start, line_indent);
                            }
                            if was_subquery {
                                current_clause = restore_clause;
                            }
                            out.push(')');
                            needs_space = true;
                        }
                        "." => {
                            trim_trailing_space(&mut out);
                            out.push('.');
                            needs_space = false;
                        }
                        _ => {
                            ensure_indent(&mut out, &mut at_line_start, line_indent);
                            let is_plsql_attribute_prefix =
                                Self::is_plsql_attribute_prefix(sym, tokens.get(idx + 1));
                            // Don't add space between consecutive ampersands (&&var substitution)
                            if needs_space
                                && !(sym == "&" && out.ends_with('&'))
                                && !is_plsql_attribute_prefix
                            {
                                out.push(' ');
                            }
                            out.push_str(sym);
                            // For bind variables (:name) and assignment (:=), don't add space after colon
                            // Check if this is ":" and next token is a Word (bind variable)
                            let is_bind_var_colon = sym == ":"
                                && tokens
                                    .get(idx + 1)
                                    .is_some_and(|t| matches!(t, SqlToken::Word(_)));
                            // For substitution variables (&var, &&var), don't add space after &
                            let is_ampersand_prefix = sym == "&"
                                && tokens.get(idx + 1).is_some_and(|t| {
                                    matches!(t, SqlToken::Word(_))
                                        || matches!(t, SqlToken::Symbol(s) if s == "&")
                                });
                            needs_space = !is_bind_var_colon
                                && !is_ampersand_prefix
                                && !is_plsql_attribute_prefix;
                        }
                    }
                    if started_line {}
                }
            }

            idx += 1;
        }

        let is_plsql_like = Self::is_plsql_like_tokens(statement, tokens);
        Self::apply_parser_depth_indentation(out.trim_end(), is_plsql_like)
    }

    fn apply_parser_depth_indentation(formatted: &str, is_plsql_like: bool) -> String {
        if formatted.is_empty() || !is_plsql_like {
            return formatted.to_string();
        }

        let depths = QueryExecutor::line_block_depths(formatted);
        let line_count = formatted.lines().count();
        if depths.len() != line_count {
            return formatted.to_string();
        }

        let multiline_string_continuation_lines =
            Self::multiline_string_continuation_lines(formatted, line_count);

        let mut out = String::new();
        let mut into_list_active = false;
        let mut in_dml_statement = false;
        let mut in_block_comment = false;
        let mut paren_case_expression_depth = 0usize;
        let mut pending_paren_case_closer_indent = false;
        let mut last_code_line_trimmed: Option<String> = None;
        let lines: Vec<&str> = formatted.lines().collect();
        for (idx, line) in lines.iter().enumerate() {
            let line = *line;
            let depth = depths.get(idx).copied().unwrap_or(0);
            if idx > 0 {
                out.push('\n');
            }

            if multiline_string_continuation_lines
                .get(idx)
                .copied()
                .unwrap_or(false)
            {
                out.push_str(line);
                continue;
            }

            let trimmed = line.trim_start();
            if trimmed.is_empty() {
                out.push_str(trimmed);
                continue;
            }

            if in_block_comment {
                out.push_str(line);
                if trimmed.contains("*/") {
                    in_block_comment = false;
                }
                continue;
            }

            let is_comment = Self::is_sqlplus_comment_line(trimmed)
                || trimmed.starts_with("/*")
                || trimmed == "*/";
            if is_comment {
                if trimmed.starts_with("/*") {
                    out.push_str(line);
                    if !trimmed.contains("*/") {
                        in_block_comment = true;
                    }
                    continue;
                }

                let leading_spaces = line.len().saturating_sub(trimmed.len());
                let existing_indent = leading_spaces / 4;
                let extra_indent = if into_list_active { 1 } else { 0 };
                let parser_depth = depth + extra_indent;
                let effective_depth = if in_dml_statement {
                    existing_indent.clamp(parser_depth, parser_depth.saturating_add(1))
                } else {
                    parser_depth
                };
                out.push_str(&" ".repeat(effective_depth * 4));
                out.push_str(trimmed);
                continue;
            }

            let trimmed_upper = trimmed.to_ascii_uppercase();
            let previous_line_ends_with_open_paren = last_code_line_trimmed
                .as_deref()
                .is_some_and(Self::line_ends_with_open_paren_before_inline_comment);
            let starts_paren_case_expression = crate::sql_text::starts_with_keyword_token(&trimmed_upper, "CASE")
                && previous_line_ends_with_open_paren;
            if starts_paren_case_expression {
                paren_case_expression_depth += 1;
            }
            let in_paren_case_expression = paren_case_expression_depth > 0;
            let starts_dml = crate::sql_text::starts_with_keyword_token(&trimmed_upper, "SELECT")
                || crate::sql_text::starts_with_keyword_token(&trimmed_upper, "INSERT")
                || crate::sql_text::starts_with_keyword_token(&trimmed_upper, "UPDATE")
                || crate::sql_text::starts_with_keyword_token(&trimmed_upper, "DELETE")
                || crate::sql_text::starts_with_keyword_token(&trimmed_upper, "MERGE");
            if starts_dml {
                in_dml_statement = true;
                into_list_active = false;
            }
            let starts_into = crate::sql_text::starts_with_keyword_token(&trimmed_upper, "INTO");
            let starts_into_ender = Self::ends_into_list_context(&trimmed_upper);
            let extra_indent = if into_list_active && !starts_into_ender {
                1
            } else {
                0
            };
            let paren_case_extra_indent = if !in_dml_statement
                && in_paren_case_expression
                && (crate::sql_text::starts_with_keyword_token(&trimmed_upper, "CASE")
                    || trimmed_upper.starts_with("WHEN ")
                    || trimmed_upper.starts_with("ELSE")
                    || crate::sql_text::starts_with_keyword_token(&trimmed_upper, "END"))
            {
                1
            } else {
                0
            };
            let previous_line_is_plain_end =
                last_code_line_trimmed.as_deref().is_some_and(|prev| {
                    let prev_upper = prev.to_ascii_uppercase();
                    Self::starts_with_plain_end(&prev_upper)
                });
            let mut next_significant_line: Option<(usize, &str)> = None;
            let mut in_peek_block_comment = false;
            for (next_idx, next) in lines.iter().enumerate().skip(idx + 1) {
                let next_trimmed = next.trim_start();
                if next_trimmed.is_empty() || Self::is_sqlplus_comment_line(next_trimmed) {
                    continue;
                }

                if in_peek_block_comment {
                    if next_trimmed.contains("*/") {
                        in_peek_block_comment = false;
                    }
                    continue;
                }

                if next_trimmed.starts_with("/*") {
                    if !next_trimmed.contains("*/") {
                        in_peek_block_comment = true;
                    }
                    continue;
                }

                if next_trimmed == "*/" {
                    continue;
                }

                next_significant_line = Some((next_idx, next_trimmed));
                break;
            }
            let next_significant_line_trimmed = next_significant_line.map(|(_, next)| next);
            let next_line_is_named_plain_end = next_significant_line_trimmed.is_some_and(|next| {
                let next_upper = next.to_ascii_uppercase();
                Self::starts_with_plain_end(&next_upper) && !Self::starts_with_bare_end(&next_upper)
            });
            let next_line_is_case_branch = next_significant_line_trimmed.is_some_and(|next| {
                let next_upper = next.to_ascii_uppercase();
                next_upper.starts_with("WHEN ") || next_upper.starts_with("ELSE")
            });
            let next_line_existing_indent = next_significant_line.map(|(next_idx, next_trimmed)| {
                let next_line = lines[next_idx];
                next_line.len().saturating_sub(next_trimmed.len()) / 4
            });
            let force_end_suffix_depth = Self::starts_with_end_suffix_terminator(&trimmed_upper)
                && !previous_line_is_plain_end
                && !next_line_is_named_plain_end;
            let force_block_depth = !in_dml_statement
                && (trimmed_upper.starts_with("EXCEPTION")
                    || trimmed_upper.starts_with("WHEN ")
                    || trimmed_upper.starts_with("ELSE")
                    || trimmed_upper.starts_with("ELSIF")
                    || trimmed_upper.starts_with("ELSEIF")
                    || trimmed_upper.starts_with("CASE")
                    || Self::starts_with_bare_end(&trimmed_upper)
                    || force_end_suffix_depth);

            let leading_spaces = line.len().saturating_sub(trimmed.len());
            let existing_indent = leading_spaces / 4;
            let parser_depth = depth + extra_indent + paren_case_extra_indent;
            let starts_with_close_paren = trimmed.starts_with(')');
            let is_paren_case_closer =
                pending_paren_case_closer_indent && starts_with_close_paren;
            let paren_case_closer_extra_indent = usize::from(is_paren_case_closer);
            let parser_depth = parser_depth + paren_case_closer_extra_indent;
            let effective_depth = if force_block_depth {
                parser_depth
            } else if in_dml_statement && starts_with_close_paren {
                if next_line_is_case_branch {
                    next_line_existing_indent.unwrap_or(parser_depth)
                } else if is_paren_case_closer {
                    parser_depth
                } else if previous_line_is_plain_end {
                    parser_depth.saturating_add(2)
                } else {
                    existing_indent.clamp(parser_depth, parser_depth.saturating_add(1))
                }
            } else if in_dml_statement {
                let is_dml_clause_line = Self::is_dml_clause_starter(&trimmed_upper)
                    || crate::sql_text::starts_with_keyword_token(&trimmed_upper, "INTO");
                let max_extra = if is_dml_clause_line { 1 } else { 2 };
                existing_indent.clamp(parser_depth, parser_depth.saturating_add(max_extra))
            } else if existing_indent > parser_depth.saturating_add(3) {
                parser_depth
            } else {
                parser_depth.max(existing_indent)
            };
            out.push_str(&" ".repeat(effective_depth * 4));
            out.push_str(trimmed);

            if in_paren_case_expression
                && crate::sql_text::starts_with_keyword_token(&trimmed_upper, "END")
            {
                paren_case_expression_depth = paren_case_expression_depth.saturating_sub(1);
                if paren_case_expression_depth == 0 && in_dml_statement {
                    pending_paren_case_closer_indent = true;
                }
            }

            if pending_paren_case_closer_indent && starts_with_close_paren {
                pending_paren_case_closer_indent = false;
            }

            if starts_into_ender {
                into_list_active = false;
            }
            if starts_into {
                into_list_active = true;
            }
            if trimmed.ends_with(';') {
                into_list_active = false;
            }
            if trimmed.ends_with(';') {
                in_dml_statement = false;
                into_list_active = false;
            }
            last_code_line_trimmed = Some(trimmed.to_string());
        }

        Self::align_case_close_parens_with_branch_depth(&out)
    }

    fn align_case_close_parens_with_branch_depth(formatted: &str) -> String {
        let lines: Vec<&str> = formatted.lines().collect();
        let mut out = String::new();

        for (idx, line) in lines.iter().enumerate() {
            if idx > 0 {
                out.push('\n');
            }

            let trimmed = line.trim_start();
            if !trimmed.starts_with(')') {
                out.push_str(line);
                continue;
            }

            let previous_code_line = lines[..idx].iter().rev().find(|candidate| {
                let candidate_trimmed = candidate.trim_start();
                !candidate_trimmed.is_empty()
                    && !Self::is_sqlplus_comment_line(candidate_trimmed)
                    && !candidate_trimmed.starts_with("/*")
                    && candidate_trimmed != "*/"
            });
            let next_code_line = lines[idx + 1..].iter().find(|candidate| {
                let candidate_trimmed = candidate.trim_start();
                !candidate_trimmed.is_empty()
                    && !Self::is_sqlplus_comment_line(candidate_trimmed)
                    && !candidate_trimmed.starts_with("/*")
                    && candidate_trimmed != "*/"
            });

            let previous_is_plain_end = previous_code_line.is_some_and(|candidate| {
                let upper = candidate.trim_start().to_ascii_uppercase();
                Self::starts_with_plain_end(&upper)
            });
            let next_is_case_branch = next_code_line.is_some_and(|candidate| {
                let upper = candidate.trim_start().to_ascii_uppercase();
                upper.starts_with("WHEN ") || upper.starts_with("ELSE")
            });

            if previous_is_plain_end && next_is_case_branch {
                let branch_indent = next_code_line
                    .map(|candidate| candidate.len().saturating_sub(candidate.trim_start().len()) / 4)
                    .unwrap_or(0);
                out.push_str(&" ".repeat(branch_indent * 4));
                out.push_str(trimmed);
            } else {
                out.push_str(line);
            }
        }

        out
    }

    fn is_dml_clause_starter(trimmed_upper: &str) -> bool {
        crate::sql_text::starts_with_keyword_token(trimmed_upper, "SELECT")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "WITH")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "FROM")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "WHERE")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "GROUP")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "HAVING")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "ORDER")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "VALUES")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "SET")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "CONNECT")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "START")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "UNION")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "INTERSECT")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "MINUS")
    }

    fn ends_into_list_context(trimmed_upper: &str) -> bool {
        Self::is_dml_clause_starter(trimmed_upper)
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "END")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "EXCEPTION")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "ELSIF")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "ELSEIF")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "ELSE")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "WHEN")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "BEGIN")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "LOOP")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "CASE")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "INSERT")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "UPDATE")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "DELETE")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "MERGE")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "FETCH")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "OPEN")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "CLOSE")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "RETURN")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "EXIT")
    }

    fn multiline_string_continuation_lines(formatted: &str, line_count: usize) -> Vec<bool> {
        let mut continuation_lines = vec![false; line_count];
        if line_count == 0 {
            return continuation_lines;
        }

        let chars: Vec<char> = formatted.chars().collect();
        let mut i = 0usize;
        let mut line = 0usize;

        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false;
        let mut in_q_quote = false;
        let mut q_quote_end: Option<char> = None;

        while i < chars.len() {
            let c = chars[i];
            let next = chars.get(i + 1).copied();

            if in_line_comment {
                if c == '\n' {
                    in_line_comment = false;
                    line += 1;
                }
                i += 1;
                continue;
            }

            if in_block_comment {
                if c == '*' && next == Some('/') {
                    in_block_comment = false;
                    i += 2;
                    continue;
                }
                if c == '\n' {
                    line += 1;
                }
                i += 1;
                continue;
            }

            if in_q_quote {
                if Some(c) == q_quote_end && next == Some('\'') {
                    in_q_quote = false;
                    q_quote_end = None;
                    i += 2;
                    continue;
                }
                if c == '\n' {
                    if line + 1 < line_count {
                        continuation_lines[line + 1] = true;
                    }
                    line += 1;
                }
                i += 1;
                continue;
            }

            if in_single_quote {
                if c == '\'' {
                    if next == Some('\'') {
                        i += 2;
                        continue;
                    }
                    in_single_quote = false;
                    i += 1;
                    continue;
                }
                if c == '\n' {
                    if line + 1 < line_count {
                        continuation_lines[line + 1] = true;
                    }
                    line += 1;
                }
                i += 1;
                continue;
            }

            if in_double_quote {
                if c == '"' {
                    if next == Some('"') {
                        i += 2;
                        continue;
                    }
                    in_double_quote = false;
                    i += 1;
                    continue;
                }
                if c == '\n' {
                    if line + 1 < line_count {
                        continuation_lines[line + 1] = true;
                    }
                    line += 1;
                }
                i += 1;
                continue;
            }

            if c == '\n' {
                line += 1;
                i += 1;
                continue;
            }

            if c == '-' && next == Some('-') {
                in_line_comment = true;
                i += 2;
                continue;
            }

            if c == '/' && next == Some('*') {
                in_block_comment = true;
                i += 2;
                continue;
            }

            if (c == 'n' || c == 'N')
                && matches!(next, Some('q') | Some('Q'))
                && chars.get(i + 2) == Some(&'\'')
                && chars.get(i + 3).is_some()
            {
                let delimiter = chars[i + 3];
                in_q_quote = true;
                q_quote_end = Some(sql_text::q_quote_closing(delimiter));
                i += 4;
                continue;
            }

            if (c == 'q' || c == 'Q') && next == Some('\'') && chars.get(i + 2).is_some() {
                let delimiter = chars[i + 2];
                in_q_quote = true;
                q_quote_end = Some(sql_text::q_quote_closing(delimiter));
                i += 3;
                continue;
            }

            if c == '\'' {
                in_single_quote = true;
                i += 1;
                continue;
            }

            if c == '"' {
                in_double_quote = true;
                i += 1;
                continue;
            }

            i += 1;
        }

        continuation_lines
    }

    fn line_ends_with_open_paren_before_inline_comment(line: &str) -> bool {
        let tokens = super::query_text::tokenize_sql(line);
        for token in tokens.iter().rev() {
            match token {
                SqlToken::Comment(_) => continue,
                SqlToken::Symbol(sym) => {
                    let trailing_symbol = sym.trim_end();
                    return trailing_symbol.ends_with('(');
                }
                _ => return false,
            }
        }

        false
    }

    fn is_plsql_like_tokens(statement: &str, tokens: &[SqlToken]) -> bool {
        let words: Vec<&str> = tokens
            .iter()
            .filter_map(|token| match token {
                SqlToken::Word(word) => Some(word.as_str()),
                _ => None,
            })
            .collect();

        if let Some(first) = words.first().copied() {
            if first.eq_ignore_ascii_case("SELECT")
                || first.eq_ignore_ascii_case("INSERT")
                || first.eq_ignore_ascii_case("UPDATE")
                || first.eq_ignore_ascii_case("DELETE")
                || first.eq_ignore_ascii_case("MERGE")
            {
                return false;
            }
            if first.eq_ignore_ascii_case("WITH") {
                let mut next_index = 1usize;
                if words
                    .get(next_index)
                    .is_some_and(|word| word.eq_ignore_ascii_case("RECURSIVE"))
                {
                    next_index += 1;
                }
                if words.get(next_index).is_some_and(|word| {
                    word.eq_ignore_ascii_case("FUNCTION") || word.eq_ignore_ascii_case("PROCEDURE")
                }) {
                    return true;
                }
                return false;
            }
        }

        for word in words {
            if word.eq_ignore_ascii_case("BEGIN") || word.eq_ignore_ascii_case("DECLARE") {
                return true;
            }
            if word.eq_ignore_ascii_case("CREATE") {
                let object_type = Self::parse_ddl_object_type(statement);
                return matches!(
                    object_type,
                    "Procedure"
                        | "Function"
                        | "Package"
                        | "Package Body"
                        | "Type"
                        | "Type Body"
                        | "Trigger"
                );
            }
        }

        false
    }

    #[cfg(test)]
    fn is_plsql_like_statement(statement: &str) -> bool {
        let tokens = Self::tokenize_sql(statement);
        Self::is_plsql_like_tokens(statement, &tokens)
    }

    fn parse_ddl_object_type(statement: &str) -> &'static str {
        let upper = statement.to_uppercase();
        QueryExecutor::parse_ddl_object_type(&upper)
    }

    fn format_create_table(statement: &str) -> Option<String> {
        let trimmed = statement.trim();
        if trimmed.is_empty() {
            return None;
        }

        let tokens = Self::tokenize_sql(trimmed);
        if tokens.is_empty() {
            return None;
        }

        // Guard: only apply CREATE TABLE formatting when TABLE is the actual
        // object keyword in the CREATE header. This avoids false matches like
        // CREATE PACKAGE BODY ... TYPE ... IS TABLE OF ...
        let mut word_positions: Vec<(usize, String)> = Vec::new();
        for (idx, token) in tokens.iter().enumerate() {
            if let SqlToken::Word(word) = token {
                word_positions.push((idx, word.to_uppercase()));
            }
        }

        let create_word_idx = word_positions
            .iter()
            .position(|(_, word)| word == "CREATE")?;

        let mut header_idx = create_word_idx + 1;
        while let Some((_, word)) = word_positions.get(header_idx) {
            if matches!(
                word.as_str(),
                "OR" | "REPLACE" | "EDITIONABLE" | "NONEDITIONABLE"
            ) {
                header_idx += 1;
                continue;
            }
            break;
        }

        if (word_positions
            .get(header_idx)
            .is_some_and(|(_, word)| word == "GLOBAL")
            || word_positions
                .get(header_idx)
                .is_some_and(|(_, word)| word == "PRIVATE"))
            && word_positions
                .get(header_idx + 1)
                .is_some_and(|(_, word)| word == "TEMPORARY")
        {
            header_idx += 2;
        }

        let (_, create_object) = word_positions.get(header_idx)?;
        if create_object != "TABLE" {
            return None;
        }

        let mut seen_table = false;
        let mut ctas = false;
        let mut open_idx: Option<usize> = None;
        let mut close_idx: Option<usize> = None;
        let token_depths = paren_depths(&tokens);
        let mut idx = 0usize;

        while idx < tokens.len() {
            let token = &tokens[idx];
            match token {
                SqlToken::Word(word) => {
                    let upper = word.to_uppercase();
                    if !seen_table && upper == "TABLE" {
                        seen_table = true;
                    } else if seen_table
                        && upper == "AS"
                        && tokens[idx + 1..]
                            .iter()
                            .find_map(|t| match t {
                                SqlToken::Word(w) => Some(w.to_uppercase()),
                                _ => None,
                            })
                            .is_some_and(|w| w == "SELECT" || w == "WITH")
                    {
                        ctas = true;
                    }
                }
                SqlToken::Symbol(sym) if sym == "(" => {
                    if is_top_level_depth(&token_depths, idx)
                        && seen_table
                        && !ctas
                        && open_idx.is_none()
                    {
                        open_idx = Some(idx);
                    }
                }
                SqlToken::Symbol(sym) if sym == ")" => {
                    if is_depth(&token_depths, idx, 1) && open_idx.is_some() && close_idx.is_none()
                    {
                        close_idx = Some(idx);
                        break;
                    }
                }
                _ => {}
            }
            idx += 1;
        }

        let (open_idx, close_idx) = match (open_idx, close_idx) {
            (Some(open_idx), Some(close_idx)) => (open_idx, close_idx),
            _ => return None,
        };

        let prefix_tokens = &tokens[..open_idx];
        let column_tokens = &tokens[open_idx + 1..close_idx];
        let suffix_tokens = &tokens[close_idx + 1..];

        let mut columns: Vec<Vec<SqlToken>> = Vec::new();
        for group in split_top_level_symbol_groups(column_tokens, ",") {
            columns.push(group.into_iter().cloned().collect());
        }

        if columns.is_empty() {
            return None;
        }

        let mut formatted_cols: Vec<(bool, String, String, String)> = Vec::new();
        let mut max_name = 0usize;
        let mut max_type = 0usize;

        for column in &columns {
            let mut iter = column.iter().filter(|t| !matches!(t, SqlToken::Comment(_)));
            let first = iter.next();
            let is_constraint = match first {
                Some(SqlToken::Word(word)) => {
                    matches!(
                        word.to_uppercase().as_str(),
                        "CONSTRAINT" | "PRIMARY" | "UNIQUE" | "FOREIGN" | "CHECK"
                    )
                }
                _ => false,
            };

            if is_constraint {
                let text = Self::join_tokens_spaced(column, 0);
                formatted_cols.push((true, text, String::new(), String::new()));
                continue;
            }

            let mut tokens_iter = column.iter().peekable();
            let name_token = tokens_iter.next();
            let name = name_token.map(Self::token_text).unwrap_or_default();

            let mut type_tokens: Vec<SqlToken> = Vec::new();
            let mut rest_tokens: Vec<SqlToken> = Vec::new();
            let mut in_type = true;

            for token in tokens_iter {
                let is_constraint_token = match token {
                    SqlToken::Word(word) => {
                        sql_text::is_format_column_constraint_keyword(word.as_str())
                    }
                    _ => false,
                };
                if in_type && is_constraint_token {
                    in_type = false;
                }
                if in_type {
                    type_tokens.push(token.clone());
                } else {
                    rest_tokens.push(token.clone());
                }
            }

            let type_str = Self::join_tokens_compact(&type_tokens);
            let rest_str = Self::join_tokens_spaced(&rest_tokens, 0);

            max_name = max_name.max(name.len());
            max_type = max_type.max(type_str.len());
            formatted_cols.push((false, name, type_str, rest_str));
        }

        let mut out = String::new();
        let prefix = Self::join_tokens_spaced(prefix_tokens, 0);
        out.push_str(prefix.trim_end());
        out.push_str(" (\n");

        let indent = " ".repeat(4);
        for (idx, (is_constraint, name, type_str, rest_str)) in
            formatted_cols.into_iter().enumerate()
        {
            out.push_str(&indent);
            if is_constraint {
                out.push_str(&name);
            } else {
                let name_pad = max_name.saturating_sub(name.len());
                let type_pad = max_type.saturating_sub(type_str.len());
                out.push_str(&name);
                if !type_str.is_empty() {
                    out.push_str(&" ".repeat(name_pad + 1));
                    out.push_str(&type_str);
                    if !rest_str.is_empty() {
                        out.push_str(&" ".repeat(type_pad + 1));
                        out.push_str(&rest_str);
                    }
                }
            }
            if idx + 1 < columns.len() {
                out.push(',');
            }
            out.push('\n');
        }
        out.push(')');

        let suffix = Self::format_create_suffix(suffix_tokens);
        if !suffix.is_empty() {
            out.push('\n');
            out.push_str(&suffix);
        }

        Some(out.trim_end().to_string())
    }

    fn token_text(token: &SqlToken) -> String {
        match token {
            SqlToken::Word(word) => {
                let upper = word.to_uppercase();
                if sql_text::is_oracle_sql_keyword(upper.as_str()) {
                    upper
                } else {
                    word.clone()
                }
            }
            SqlToken::String(literal) => literal.clone(),
            SqlToken::Comment(comment) => comment.clone(),
            SqlToken::Symbol(sym) => sym.clone(),
        }
    }

    fn token_is_word_like(token: &SqlToken) -> bool {
        matches!(token, SqlToken::Word(_))
    }

    fn is_plsql_attribute_prefix(sym: &str, next_token: Option<&SqlToken>) -> bool {
        sym == "%"
            && next_token
                .map(Self::token_is_word_like)
                .unwrap_or(false)
    }

    fn join_tokens_compact(tokens: &[SqlToken]) -> String {
        let mut out = String::new();
        let mut needs_space = false;
        for (idx, token) in tokens.iter().enumerate() {
            let text = Self::token_text(token);
            match token {
                SqlToken::Symbol(sym) if sym == "(" => {
                    out.push_str(&text);
                    needs_space = false;
                }
                SqlToken::Symbol(sym) if sym == ")" => {
                    out.push_str(&text);
                    needs_space = true;
                }
                SqlToken::Symbol(sym) if sym == "," => {
                    out.push_str(&text);
                    out.push(' ');
                    needs_space = false;
                }
                SqlToken::Symbol(sym)
                    if Self::is_plsql_attribute_prefix(sym, tokens.get(idx + 1)) =>
                {
                    out.push_str(&text);
                    needs_space = false;
                }
                _ => {
                    if needs_space {
                        out.push(' ');
                    }
                    out.push_str(&text);
                    needs_space = true;
                }
            }
        }
        out.trim().to_string()
    }

    fn join_tokens_spaced(tokens: &[SqlToken], indent_level: usize) -> String {
        let mut out = String::new();
        let mut needs_space = false;
        let indent = " ".repeat(indent_level * 4);
        let mut at_line_start = true;

        for (idx, token) in tokens.iter().enumerate() {
            let text = Self::token_text(token);
            match token {
                SqlToken::Comment(comment) => {
                    if !at_line_start {
                        out.push(' ');
                    } else if !indent.is_empty() {
                        out.push_str(&indent);
                    }
                    out.push_str(comment);
                    if comment.ends_with('\n') {
                        at_line_start = true;
                        needs_space = false;
                    } else {
                        at_line_start = false;
                        needs_space = true;
                    }
                }
                SqlToken::Symbol(sym) if sym == "." => {
                    out.push('.');
                    needs_space = false;
                    at_line_start = false;
                }
                SqlToken::Symbol(sym) if sym == "(" => {
                    out.push('(');
                    needs_space = false;
                    at_line_start = false;
                }
                SqlToken::Symbol(sym) if sym == ")" => {
                    out.push(')');
                    needs_space = true;
                    at_line_start = false;
                }
                SqlToken::Symbol(sym) if sym == "," => {
                    out.push(',');
                    out.push(' ');
                    needs_space = false;
                    at_line_start = false;
                }
                SqlToken::Symbol(sym)
                    if Self::is_plsql_attribute_prefix(sym, tokens.get(idx + 1)) =>
                {
                    out.push('%');
                    needs_space = false;
                    at_line_start = false;
                }
                SqlToken::Symbol(sym) => {
                    if needs_space {
                        out.push(' ');
                    }
                    out.push_str(sym);
                    needs_space = true;
                    at_line_start = false;
                }
                _ => {
                    if at_line_start && !indent.is_empty() {
                        out.push_str(&indent);
                    }
                    if needs_space {
                        out.push(' ');
                    }
                    out.push_str(&text);
                    needs_space = true;
                    at_line_start = false;
                }
            }
        }

        out.trim().to_string()
    }

    fn format_create_suffix(tokens: &[SqlToken]) -> String {
        if tokens.is_empty() {
            return String::new();
        }

        let break_keywords = FORMAT_CREATE_SUFFIX_BREAK_KEYWORDS;

        let mut parts: Vec<Vec<SqlToken>> = Vec::new();

        for part in split_top_level_keyword_groups(tokens, break_keywords) {
            parts.push(part.into_iter().cloned().collect());
        }

        let mut out = String::new();
        for (idx, part) in parts.iter().enumerate() {
            if idx > 0 {
                out.push('\n');
            }
            out.push_str(&Self::join_tokens_spaced(part, 0));
        }
        out.trim().to_string()
    }

    /// 토크나이저는 공통 로직(`query_text`)로 위임합니다.
    pub(crate) fn tokenize_sql(sql: &str) -> Vec<SqlToken> {
        super::query_text::tokenize_sql(sql)
    }

    fn normalize_script_include_path(path: &Path) -> PathBuf {
        fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
    }

    fn validate_script_include_target(
        frames: &[ScriptExecutionFrame],
        target_path: &Path,
    ) -> Result<(), String> {
        let nested_depth = frames
            .iter()
            .filter(|frame| frame.source_path.is_some())
            .count();
        if nested_depth >= MAX_SCRIPT_INCLUDE_DEPTH {
            return Err(format!(
                "Maximum nested script depth ({MAX_SCRIPT_INCLUDE_DEPTH}) exceeded."
            ));
        }

        if frames.iter().any(|frame| {
            frame
                .source_path
                .as_ref()
                .is_some_and(|path| path.as_path() == target_path)
        }) {
            return Err(format!(
                "Recursive script include detected: {}",
                target_path.display()
            ));
        }

        Ok(())
    }

    fn resolve_script_include_path(
        path: &str,
        relative_to_caller: bool,
        caller_base_dir: &Path,
        working_dir: &Path,
    ) -> (PathBuf, PathBuf) {
        let base_dir = if relative_to_caller {
            caller_base_dir
        } else {
            working_dir
        };
        let target_path = if Path::new(path).is_absolute() {
            PathBuf::from(path)
        } else {
            base_dir.join(path)
        };
        let normalized_target_path = Self::normalize_script_include_path(&target_path);

        (target_path, normalized_target_path)
    }

    fn load_script_include(
        target_path: &Path,
        normalized_target_path: &Path,
        base_dir: &Path,
    ) -> Result<ResolvedScriptInclude, String> {
        let contents = fs::read_to_string(target_path)
            .map_err(|err| format!("Failed to read script {}: {}", target_path.display(), err))?;

        let script_dir = normalized_target_path
            .parent()
            .unwrap_or(base_dir)
            .to_path_buf();

        Ok(ResolvedScriptInclude {
            source_path: normalized_target_path.to_path_buf(),
            script_dir,
            items: super::query_text::split_script_items(&contents),
        })
    }

    fn requires_connected_session_for_precheck(
        has_connection_bootstrap_command: bool,
        can_run_while_disconnected: bool,
    ) -> bool {
        !has_connection_bootstrap_command && !can_run_while_disconnected
    }

    fn execution_startup_policy(sql: &str) -> ExecutionStartupPolicy {
        let has_connect_command = super::query_text::has_connection_bootstrap_command(sql);
        let can_run_while_disconnected = super::query_text::can_execute_while_disconnected(sql);
        let requires_connected_session = Self::requires_connected_session_for_precheck(
            has_connect_command,
            can_run_while_disconnected,
        );

        ExecutionStartupPolicy {
            has_connect_command,
            requires_connected_session,
        }
    }

    fn acquire_execution_connection(
        conn_guard: &mut crate::db::ConnectionLockGuard<'_>,
        sender: &mpsc::Sender<QueryProgress>,
        has_connect_command: bool,
    ) -> Result<Option<Arc<Connection>>, String> {
        if has_connect_command {
            if conn_guard.is_connected() {
                match conn_guard.require_live_connection() {
                    Ok(conn) => Ok(Some(conn)),
                    Err(_) => {
                        let _ = sender.send(QueryProgress::ConnectionChanged { info: None });
                        app::awake();
                        Ok(None)
                    }
                }
            } else {
                Ok(None)
            }
        } else {
            match conn_guard.require_live_connection() {
                Ok(conn) => Ok(Some(conn)),
                Err(message) => {
                    if !conn_guard.is_connected() || conn_guard.get_connection().is_none() {
                        let _ = sender.send(QueryProgress::ConnectionChanged { info: None });
                        app::awake();
                    }
                    Err(message)
                }
            }
        }
    }

    fn emit_execution_startup_error(
        sender: &mpsc::Sender<QueryProgress>,
        script_mode: bool,
        sql_text: &str,
        conn_name: &str,
        message: &str,
        session: Option<&Arc<Mutex<SessionState>>>,
    ) {
        if script_mode {
            let result = QueryResult::new_error(sql_text, message);
            SqlEditorWidget::emit_script_result(sender, conn_name, 0, result, false);
            return;
        }

        if let Some(active_session) = session {
            SqlEditorWidget::append_spool_output(active_session, &[message.to_string()]);
        }

        let _ = sender.send(QueryProgress::StatementFinished {
            index: 0,
            result: QueryResult::new_error(sql_text, message),
            connection_name: conn_name.to_string(),
            timed_out: false,
        });
        app::awake();
    }

    fn execute_sql(&self, sql: &str, script_mode: bool) {
        if sql.trim().is_empty() {
            return;
        }

        let mut query_run_reservation =
            match QueryRunningReservation::acquire(self.query_running.clone()) {
                Some(reservation) => reservation,
                None => {
                    let _ = self
                        .ui_action_sender
                        .send(UiActionResult::QueryAlreadyRunning);
                    app::awake();
                    return;
                }
            };

        // Build an execution policy once and reuse it for both UI pre-check and worker startup.
        let startup_policy = Self::execution_startup_policy(sql);

        // Pre-check connection status without holding lock for long
        {
            let Some(conn_guard) = crate::db::try_lock_connection(&self.connection) else {
                let _ = self.ui_action_sender.send(UiActionResult::ConnectionBusy);
                app::awake();
                return;
            };

            // Keep UI responsive: avoid network round-trip checks (ping) on the UI thread.
            // The execution worker performs full liveness validation.
            // Regression guard: scripts that contain CONNECT/@START must pass this gate
            // even when disconnected, so CONNECT can establish a session for later SQL.
            if startup_policy.requires_connected_session
                && (!conn_guard.is_connected() || conn_guard.get_connection().is_none())
            {
                SqlEditorWidget::show_alert_dialog("Not connected to database");
                return;
            }
        } // Release lock early for the pre-check

        // Worker thread cleanup now owns execution state reset.
        query_run_reservation.disarm();

        let shared_connection = self.connection.clone();
        let query_timeout = Self::parse_timeout(&self.timeout_input.value());
        let sql_text = sql.to_string();
        let db_activity = Self::db_activity_label_for_sql(&sql_text, script_mode);
        let sender = self.progress_sender.clone();
        let query_running = self.query_running.clone();
        let current_query_connection = self.current_query_connection.clone();
        let cancel_flag = self.cancel_flag.clone();

        // Reset cancel flag before starting new execution
        store_mutex_bool(&cancel_flag, false);

        set_cursor(Cursor::Wait);
        app::flush();

        thread::spawn(move || {
            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                let mut cleanup = QueryExecutionCleanupGuard::new(
                    sender.clone(),
                    current_query_connection.clone(),
                    cancel_flag.clone(),
                    query_running.clone(),
                );

                // Acquire connection lock inside thread and hold it during execution
                let mut conn_guard =
                    lock_connection_with_activity(&shared_connection, db_activity.clone());

                let mut conn_name = if conn_guard.is_connected() {
                    conn_guard.get_info().name.clone()
                } else {
                    String::new()
                };

                let mut conn_opt = match Self::acquire_execution_connection(
                    &mut conn_guard,
                    &sender,
                    startup_policy.has_connect_command,
                ) {
                    Ok(conn) => conn,
                    Err(message) => {
                        SqlEditorWidget::emit_execution_startup_error(
                            &sender,
                            script_mode,
                            &sql_text,
                            &conn_name,
                            &message,
                            None,
                        );
                        return;
                    }
                };

                if conn_guard.is_connected() {
                    conn_name = conn_guard.get_info().name.clone();
                } else {
                    conn_name.clear();
                }

                if startup_policy.requires_connected_session && conn_opt.is_none() {
                    let message = crate::db::NOT_CONNECTED_MESSAGE.to_string();
                    let _ = sender.send(QueryProgress::ConnectionChanged { info: None });
                    app::awake();
                    SqlEditorWidget::emit_execution_startup_error(
                        &sender,
                        script_mode,
                        &sql_text,
                        &conn_name,
                        &message,
                        None,
                    );
                    return;
                }
                let auto_commit = conn_guard.auto_commit();
                let session = conn_guard.session_state();

                // Store connection for cancel operation (separate from mutex)
                if let Some(ref conn) = conn_opt {
                    SqlEditorWidget::set_current_query_connection(
                        &current_query_connection,
                        Some(Arc::clone(conn)),
                    );
                    if load_mutex_bool(&cancel_flag) {
                        let _ = conn.break_execution();
                    }
                }
                // Release the shared connection mutex before running statements so
                // UI/auxiliary workers are not blocked for the full execution window.
                drop(conn_guard);

                let items = super::query_text::split_script_items(&sql_text);
                if items.is_empty() {
                    return;
                }

                let _ = sender.send(QueryProgress::BatchStart);
                app::awake();

                // Set timeout only if we have a connection
                let previous_timeout = conn_opt
                    .as_ref()
                    .and_then(|c| c.call_timeout().ok())
                    .flatten();

                if let Some(conn) = conn_opt.as_ref() {
                    cleanup.track_timeout(Arc::clone(conn), previous_timeout);
                }

                let requires_transaction_first_statement =
                    SqlEditorWidget::requires_transaction_first_statement(&items);

                if let Some(conn) = conn_opt.as_ref() {
                    if let Err(err) = conn.set_call_timeout(query_timeout) {
                        let timeout_error = err.to_string();
                        SqlEditorWidget::emit_execution_startup_error(
                            &sender,
                            script_mode,
                            &sql_text,
                            &conn_name,
                            &timeout_error,
                            Some(&session),
                        );
                        return;
                    }
                    if !requires_transaction_first_statement {
                        if let Err(err) =
                            SqlEditorWidget::sync_serveroutput_with_session(conn.as_ref(), &session)
                        {
                            eprintln!(
                                "Failed to apply SERVEROUTPUT setting on session start: {err}"
                            );
                        }
                    }
                }

                let mut result_index = 0usize;
                let mut auto_commit = auto_commit;
                let mut continue_on_error = match session.lock() {
                    Ok(guard) => guard.continue_on_error,
                    Err(poisoned) => {
                        eprintln!("Warning: session state lock was poisoned; recovering.");
                        poisoned.into_inner().continue_on_error
                    }
                };
                let mut stop_execution = false;
                let working_dir = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
                let mut frames = vec![ScriptExecutionFrame {
                    items,
                    index: 0,
                    base_dir: working_dir.clone(),
                    source_path: None,
                }];

                while !frames.is_empty() {
                    if stop_execution || load_mutex_bool(&cancel_flag) {
                        break;
                    }

                    let Some((item, current_frame_base_dir)) = ({
                        let frame = match frames.last_mut() {
                            Some(frame) => frame,
                            None => break,
                        };

                        if frame.index >= frame.items.len() {
                            None
                        } else {
                            let item = frame.items[frame.index].clone();
                            frame.index += 1;
                            Some((item, frame.base_dir.clone()))
                        }
                    }) else {
                        frames.pop();
                        continue;
                    };

                    let echo_enabled = match session.lock() {
                        Ok(guard) => guard.echo_enabled,
                        Err(poisoned) => {
                            eprintln!("Warning: session state lock was poisoned; recovering.");
                            poisoned.into_inner().echo_enabled
                        }
                    };
                    if echo_enabled {
                        let echo_line = match &item {
                            ScriptItem::Statement(statement) => statement.trim().to_string(),
                            ScriptItem::ToolCommand(command) => {
                                SqlEditorWidget::format_tool_command(command)
                            }
                        };
                        if !echo_line.trim().is_empty() {
                            SqlEditorWidget::emit_script_output(&sender, &session, vec![echo_line]);
                        }
                    }

                    match item {
                        ScriptItem::ToolCommand(command) => {
                            let mut command_error = false;
                            match command {
                                ToolCommand::Var { name, data_type } => {
                                    let normalized = SessionState::normalize_name(&name);
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.binds.insert(
                                            normalized.clone(),
                                            BindVar::new(data_type.clone()),
                                        );
                                    }
                                    let message = format!(
                                        "Variable :{} declared as {}",
                                        normalized,
                                        data_type.display()
                                    );
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        &format!("VAR {} {}", normalized, data_type.display()),
                                        &message,
                                    );
                                }
                                ToolCommand::Print { name } => {
                                    let (heading_enabled, feedback_enabled) =
                                        SqlEditorWidget::current_output_settings(&session);
                                    let (_colsep, null_text, _trimspool_enabled) =
                                        SqlEditorWidget::current_text_output_settings(&session);

                                    if let Some(name) = name {
                                        let key = SessionState::normalize_name(&name);
                                        let named_data = {
                                            let guard = match session.lock() {
                                                Ok(guard) => guard,
                                                Err(poisoned) => {
                                                    eprintln!("Warning: session state lock was poisoned; recovering.");
                                                    poisoned.into_inner()
                                                }
                                            };
                                            SqlEditorWidget::clone_print_named_data(&guard, &key)
                                        };

                                        match named_data {
                                            PrintNamedData::Scalar(value) => {
                                                let columns =
                                                    vec!["NAME".to_string(), "VALUE".to_string()];
                                                let rows = vec![vec![
                                                    key.clone(),
                                                    value.unwrap_or_else(|| null_text.clone()),
                                                ]];
                                                let headers =
                                                    SqlEditorWidget::apply_heading_setting(
                                                        columns,
                                                        heading_enabled,
                                                    );
                                                SqlEditorWidget::emit_select_result(
                                                    &sender,
                                                    &session,
                                                    &conn_name,
                                                    result_index,
                                                    &format!("PRINT {}", key),
                                                    headers,
                                                    rows,
                                                    true,
                                                    feedback_enabled,
                                                );
                                                result_index += 1;
                                            }
                                            PrintNamedData::Cursor(cursor) => {
                                                let headers =
                                                    SqlEditorWidget::apply_heading_setting(
                                                        cursor.columns,
                                                        heading_enabled,
                                                    );
                                                SqlEditorWidget::emit_select_result(
                                                    &sender,
                                                    &session,
                                                    &conn_name,
                                                    result_index,
                                                    &format!("PRINT {}", key),
                                                    headers,
                                                    cursor.rows,
                                                    true,
                                                    feedback_enabled,
                                                );
                                                result_index += 1;
                                            }
                                            PrintNamedData::CursorEmpty => {
                                                SqlEditorWidget::emit_script_message(
                                                    &sender,
                                                    &session,
                                                    &format!("PRINT {}", key),
                                                    &format!(
                                                        "Error: Cursor :{} has no data to print.",
                                                        key
                                                    ),
                                                );
                                                command_error = true;
                                            }
                                            PrintNamedData::Missing => {
                                                SqlEditorWidget::emit_script_message(
                                                    &sender,
                                                    &session,
                                                    &format!("PRINT {}", key),
                                                    &format!(
                                                        "Error: Bind variable :{} is not defined.",
                                                        key
                                                    ),
                                                );
                                                command_error = true;
                                            }
                                        }
                                    } else {
                                        let (summary_rows, cursor_results) = {
                                            let guard = match session.lock() {
                                                Ok(guard) => guard,
                                                Err(poisoned) => {
                                                    eprintln!("Warning: session state lock was poisoned; recovering.");
                                                    poisoned.into_inner()
                                                }
                                            };

                                            if guard.binds.is_empty() {
                                                (Vec::new(), Vec::new())
                                            } else {
                                                SqlEditorWidget::collect_print_all_data(
                                                    &guard, &null_text,
                                                )
                                            }
                                        };

                                        if summary_rows.is_empty() {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                "PRINT",
                                                "No bind variables declared.",
                                            );
                                        } else {
                                            let headers = SqlEditorWidget::apply_heading_setting(
                                                vec![
                                                    "NAME".to_string(),
                                                    "TYPE".to_string(),
                                                    "VALUE".to_string(),
                                                ],
                                                heading_enabled,
                                            );
                                            SqlEditorWidget::emit_select_result(
                                                &sender,
                                                &session,
                                                &conn_name,
                                                result_index,
                                                "PRINT",
                                                headers,
                                                summary_rows,
                                                true,
                                                feedback_enabled,
                                            );
                                            result_index += 1;

                                            for (cursor_name, cursor) in cursor_results {
                                                let headers =
                                                    SqlEditorWidget::apply_heading_setting(
                                                        cursor.columns,
                                                        heading_enabled,
                                                    );
                                                SqlEditorWidget::emit_select_result(
                                                    &sender,
                                                    &session,
                                                    &conn_name,
                                                    result_index,
                                                    &format!("PRINT {}", cursor_name),
                                                    headers,
                                                    cursor.rows,
                                                    true,
                                                    feedback_enabled,
                                                );
                                                result_index += 1;
                                            }
                                        }
                                    }
                                }
                                ToolCommand::SetServerOutput {
                                    enabled,
                                    size,
                                    unlimited,
                                } => {
                                    // This command needs a connection
                                    let conn = match conn_opt.as_ref() {
                                        Some(c) => c,
                                        None => {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                "SET SERVEROUTPUT",
                                                "Error: Not connected to database",
                                            );
                                            continue;
                                        }
                                    };

                                    let default_size = 1_000_000u32;
                                    let current_size = match session.lock() {
                                        Ok(guard) => guard.server_output.size,
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            poisoned.into_inner().server_output.size
                                        }
                                    };
                                    let mut message = String::new();
                                    let mut success = true;

                                    if enabled {
                                        if unlimited {
                                            // SIZE UNLIMITED: pass None to enable unlimited buffer
                                            let enable_result = QueryExecutor::enable_dbms_output(
                                                conn.as_ref(),
                                                None,
                                            );

                                            match enable_result {
                                                Ok(()) => {
                                                    let mut guard = match session.lock() {
                                                        Ok(guard) => guard,
                                                        Err(poisoned) => {
                                                            eprintln!("Warning: session state lock was poisoned; recovering.");
                                                            poisoned.into_inner()
                                                        }
                                                    };
                                                    guard.server_output.enabled = true;
                                                    guard.server_output.size = 0; // 0 indicates unlimited
                                                    message =
                                                        "SERVEROUTPUT enabled (size UNLIMITED)"
                                                            .to_string();
                                                }
                                                Err(err) => {
                                                    success = false;
                                                    message = format!(
                                                        "SERVEROUTPUT enable failed: {}",
                                                        err
                                                    );
                                                }
                                            }
                                        } else {
                                            let desired_size = size.unwrap_or(current_size);
                                            let mut applied_size = desired_size;
                                            let mut enable_result =
                                                QueryExecutor::enable_dbms_output(
                                                    conn.as_ref(),
                                                    Some(desired_size),
                                                );

                                            if enable_result.is_err()
                                                && size.is_some()
                                                && desired_size != default_size
                                                && QueryExecutor::enable_dbms_output(
                                                    conn.as_ref(),
                                                    Some(default_size),
                                                )
                                                .is_ok()
                                            {
                                                applied_size = default_size;
                                                message = format!(
                                                        "SERVEROUTPUT enabled with size {} (requested {} not supported)",
                                                        applied_size, desired_size
                                                    );
                                                enable_result = Ok(());
                                            }

                                            match enable_result {
                                                Ok(()) => {
                                                    let mut guard = match session.lock() {
                                                        Ok(guard) => guard,
                                                        Err(poisoned) => {
                                                            eprintln!("Warning: session state lock was poisoned; recovering.");
                                                            poisoned.into_inner()
                                                        }
                                                    };
                                                    guard.server_output.enabled = true;
                                                    guard.server_output.size = applied_size;
                                                    if message.is_empty() {
                                                        message = format!(
                                                            "SERVEROUTPUT enabled (size {})",
                                                            applied_size
                                                        );
                                                    }
                                                }
                                                Err(err) => {
                                                    success = false;
                                                    message = format!(
                                                        "SERVEROUTPUT enable failed: {}",
                                                        err
                                                    );
                                                }
                                            }
                                        }
                                    } else {
                                        match QueryExecutor::disable_dbms_output(conn.as_ref()) {
                                            Ok(()) => {
                                                let mut guard = match session.lock() {
                                                    Ok(guard) => guard,
                                                    Err(poisoned) => {
                                                        eprintln!("Warning: session state lock was poisoned; recovering.");
                                                        poisoned.into_inner()
                                                    }
                                                };
                                                guard.server_output.enabled = false;
                                                message = "SERVEROUTPUT disabled".to_string();
                                            }
                                            Err(err) => {
                                                success = false;
                                                message =
                                                    format!("SERVEROUTPUT disable failed: {}", err);
                                            }
                                        }
                                    }

                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET SERVEROUTPUT",
                                        &message,
                                    );
                                    if !success {
                                        command_error = true;
                                    }
                                }
                                ToolCommand::ShowErrors {
                                    object_type,
                                    object_name,
                                } => {
                                    // This command needs a connection
                                    let conn = match conn_opt.as_ref() {
                                        Some(c) => c,
                                        None => {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                "SHOW ERRORS",
                                                "Error: Not connected to database",
                                            );
                                            continue;
                                        }
                                    };

                                    let mut target = None;
                                    if object_type.is_none() {
                                        target = match session.lock() {
                                            Ok(guard) => guard.last_compiled.clone(),
                                            Err(poisoned) => {
                                                eprintln!("Warning: session state lock was poisoned; recovering.");
                                                poisoned.into_inner().last_compiled.clone()
                                            }
                                        };
                                    } else if let (Some(obj_type), Some(obj_name)) =
                                        (object_type.clone(), object_name.clone())
                                    {
                                        let (owner, name) = if let Some(dot) = obj_name.find('.') {
                                            let (owner_raw, name_raw) = obj_name.split_at(dot);
                                            (
                                                Some(SqlEditorWidget::normalize_object_name(
                                                    owner_raw,
                                                )),
                                                SqlEditorWidget::normalize_object_name(
                                                    name_raw.trim_start_matches('.'),
                                                ),
                                            )
                                        } else {
                                            (
                                                None,
                                                SqlEditorWidget::normalize_object_name(&obj_name),
                                            )
                                        };

                                        target = Some(crate::db::CompiledObject {
                                            owner,
                                            object_type: obj_type.to_uppercase(),
                                            name,
                                        });
                                    }

                                    if let Some(object) = target {
                                        match QueryExecutor::fetch_compilation_errors(
                                            conn.as_ref(),
                                            &object,
                                        ) {
                                            Ok(rows) => {
                                                if rows.is_empty() {
                                                    SqlEditorWidget::emit_script_message(
                                                        &sender,
                                                        &session,
                                                        "SHOW ERRORS",
                                                        "No errors found.",
                                                    );
                                                } else {
                                                    let (heading_enabled, feedback_enabled) =
                                                        SqlEditorWidget::current_output_settings(
                                                            &session,
                                                        );
                                                    let headers =
                                                        SqlEditorWidget::apply_heading_setting(
                                                            vec![
                                                                "LINE".to_string(),
                                                                "POSITION".to_string(),
                                                                "TEXT".to_string(),
                                                            ],
                                                            heading_enabled,
                                                        );
                                                    SqlEditorWidget::emit_select_result(
                                                        &sender,
                                                        &session,
                                                        &conn_name,
                                                        result_index,
                                                        "SHOW ERRORS",
                                                        headers,
                                                        rows,
                                                        true,
                                                        feedback_enabled,
                                                    );
                                                    result_index += 1;
                                                }
                                            }
                                            Err(err) => {
                                                SqlEditorWidget::emit_script_message(
                                                    &sender,
                                                    &session,
                                                    "SHOW ERRORS",
                                                    &format!("Error: {}", err),
                                                );
                                                command_error = true;
                                            }
                                        }
                                    } else {
                                        SqlEditorWidget::emit_script_message(
                                            &sender,
                                            &session,
                                            "SHOW ERRORS",
                                            "Error: No compiled object found to show errors.",
                                        );
                                        command_error = true;
                                    }
                                }
                                ToolCommand::ShowUser => {
                                    // This command needs a connection
                                    let conn = match conn_opt.as_ref() {
                                        Some(c) => c,
                                        None => {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                "SHOW USER",
                                                "Error: Not connected to database",
                                            );
                                            continue;
                                        }
                                    };

                                    let sql = "SELECT USER FROM DUAL";
                                    let user_result: Result<String, OracleError> = (|| {
                                        let mut stmt = conn.statement(sql).build()?;
                                        let row = stmt.query_row(&[])?;
                                        let user: String = row.get(0)?;
                                        Ok(user)
                                    })(
                                    );
                                    match user_result {
                                        Ok(user) => {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                "SHOW USER",
                                                &format!("USER: {}", user),
                                            );
                                        }
                                        Err(err) => {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                "SHOW USER",
                                                &format!("Error: {}", err),
                                            );
                                            command_error = true;
                                        }
                                    }
                                }
                                ToolCommand::ShowAll => {
                                    let (
                                        server_output,
                                        define_enabled,
                                        define_char,
                                        scan_enabled,
                                        verify_enabled,
                                        echo_enabled,
                                        timing_enabled,
                                        feedback_enabled,
                                        heading_enabled,
                                        pagesize,
                                        linesize,
                                        trimspool_enabled,
                                        trimout_enabled,
                                        sqlblanklines_enabled,
                                        tab_enabled,
                                        colsep,
                                        null_text,
                                        break_column,
                                        compute_config,
                                        continue_on_error,
                                        spool_path,
                                    ) = match session.lock() {
                                        Ok(guard) => (
                                            guard.server_output.clone(),
                                            guard.define_enabled,
                                            guard.define_char,
                                            guard.scan_enabled,
                                            guard.verify_enabled,
                                            guard.echo_enabled,
                                            guard.timing_enabled,
                                            guard.feedback_enabled,
                                            guard.heading_enabled,
                                            guard.pagesize,
                                            guard.linesize,
                                            guard.trimspool_enabled,
                                            guard.trimout_enabled,
                                            guard.sqlblanklines_enabled,
                                            guard.tab_enabled,
                                            guard.colsep.clone(),
                                            guard.null_text.clone(),
                                            guard.break_column.clone(),
                                            guard.compute.clone(),
                                            guard.continue_on_error,
                                            guard.spool_path.clone(),
                                        ),
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let guard = poisoned.into_inner();
                                            (
                                                guard.server_output.clone(),
                                                guard.define_enabled,
                                                guard.define_char,
                                                guard.scan_enabled,
                                                guard.verify_enabled,
                                                guard.echo_enabled,
                                                guard.timing_enabled,
                                                guard.feedback_enabled,
                                                guard.heading_enabled,
                                                guard.pagesize,
                                                guard.linesize,
                                                guard.trimspool_enabled,
                                                guard.trimout_enabled,
                                                guard.sqlblanklines_enabled,
                                                guard.tab_enabled,
                                                guard.colsep.clone(),
                                                guard.null_text.clone(),
                                                guard.break_column.clone(),
                                                guard.compute.clone(),
                                                guard.continue_on_error,
                                                guard.spool_path.clone(),
                                            )
                                        }
                                    };

                                    let autocommit_enabled = auto_commit;

                                    let serveroutput_line = if server_output.enabled {
                                        if server_output.size == 0 {
                                            "SERVEROUTPUT ON SIZE UNLIMITED".to_string()
                                        } else {
                                            format!("SERVEROUTPUT ON SIZE {}", server_output.size)
                                        }
                                    } else {
                                        "SERVEROUTPUT OFF".to_string()
                                    };

                                    let spool_line = match spool_path {
                                        Some(path) => format!("SPOOL {}", path.display()),
                                        None => "SPOOL OFF".to_string(),
                                    };

                                    let lines = vec![
                                        format!(
                                            "AUTOCOMMIT {}",
                                            if autocommit_enabled { "ON" } else { "OFF" }
                                        ),
                                        serveroutput_line,
                                        if define_enabled {
                                            format!("DEFINE '{}'", define_char)
                                        } else {
                                            "DEFINE OFF".to_string()
                                        },
                                        format!("SCAN {}", if scan_enabled { "ON" } else { "OFF" }),
                                        format!(
                                            "VERIFY {}",
                                            if verify_enabled { "ON" } else { "OFF" }
                                        ),
                                        format!("ECHO {}", if echo_enabled { "ON" } else { "OFF" }),
                                        format!(
                                            "TIMING {}",
                                            if timing_enabled { "ON" } else { "OFF" }
                                        ),
                                        format!(
                                            "FEEDBACK {}",
                                            if feedback_enabled { "ON" } else { "OFF" }
                                        ),
                                        format!(
                                            "HEADING {}",
                                            if heading_enabled { "ON" } else { "OFF" }
                                        ),
                                        format!("PAGESIZE {}", pagesize),
                                        format!("LINESIZE {}", linesize),
                                        format!(
                                            "TRIMSPOOL {}",
                                            if trimspool_enabled { "ON" } else { "OFF" }
                                        ),
                                        format!(
                                            "TRIMOUT {}",
                                            if trimout_enabled { "ON" } else { "OFF" }
                                        ),
                                        format!(
                                            "SQLBLANKLINES {}",
                                            if sqlblanklines_enabled { "ON" } else { "OFF" }
                                        ),
                                        format!("TAB {}", if tab_enabled { "ON" } else { "OFF" }),
                                        format!("COLSEP {}", colsep),
                                        format!("NULL {}", null_text),
                                        match break_column {
                                            Some(column) => format!("BREAK ON {}", column),
                                            None => "BREAK OFF".to_string(),
                                        },
                                        match compute_config {
                                            Some(config) => {
                                                let mode_text = match config.mode {
                                                    crate::db::ComputeMode::Sum => "SUM",
                                                    crate::db::ComputeMode::Count => "COUNT",
                                                };
                                                match (
                                                    config.of_column.as_deref(),
                                                    config.on_column.as_deref(),
                                                ) {
                                                    (Some(of_col), Some(on_col)) => format!(
                                                        "COMPUTE {} OF {} ON {}",
                                                        mode_text, of_col, on_col
                                                    ),
                                                    _ => format!("COMPUTE {}", mode_text),
                                                }
                                            }
                                            None => "COMPUTE OFF".to_string(),
                                        },
                                        format!(
                                            "ERRORCONTINUE {}",
                                            if continue_on_error { "ON" } else { "OFF" }
                                        ),
                                        spool_line,
                                    ];

                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SHOW ALL",
                                        &lines.join("\n"),
                                    );
                                }
                                ToolCommand::Describe { name } => {
                                    let conn = match conn_opt.as_ref() {
                                        Some(c) => c,
                                        None => {
                                            if script_mode {
                                                SqlEditorWidget::emit_script_message(
                                                    &sender,
                                                    &session,
                                                    "DESCRIBE",
                                                    "Error: Not connected to database",
                                                );
                                            } else {
                                                let emitted =
                                                    SqlEditorWidget::emit_non_select_result(
                                                        &sender,
                                                        &session,
                                                        &conn_name,
                                                        result_index,
                                                        &format!("DESCRIBE {}", name),
                                                        "Error: Not connected to database"
                                                            .to_string(),
                                                        false,
                                                        false,
                                                        false,
                                                    );
                                                if emitted {
                                                    result_index += 1;
                                                }
                                            }
                                            continue;
                                        }
                                    };
                                    let title = format!("DESCRIBE {}", name);
                                    match QueryExecutor::describe_object(conn.as_ref(), &name) {
                                        Ok(columns) => {
                                            if columns.is_empty() {
                                                if script_mode {
                                                    SqlEditorWidget::emit_script_message(
                                                        &sender,
                                                        &session,
                                                        &title,
                                                        "Error: Object not found.",
                                                    );
                                                } else {
                                                    let emitted =
                                                        SqlEditorWidget::emit_non_select_result(
                                                            &sender,
                                                            &session,
                                                            &conn_name,
                                                            result_index,
                                                            &title,
                                                            "Error: Object not found.".to_string(),
                                                            false,
                                                            false,
                                                            false,
                                                        );
                                                    if emitted {
                                                        result_index += 1;
                                                    }
                                                }
                                                command_error = true;
                                            } else {
                                                let rows = columns
                                                    .into_iter()
                                                    .map(|col| {
                                                        let type_display = col.get_type_display();
                                                        let TableColumnDetail {
                                                            name,
                                                            nullable,
                                                            is_primary_key,
                                                            ..
                                                        } = col;
                                                        vec![
                                                            name,
                                                            type_display,
                                                            if nullable {
                                                                "YES".to_string()
                                                            } else {
                                                                "NO".to_string()
                                                            },
                                                            if is_primary_key {
                                                                "PK".to_string()
                                                            } else {
                                                                String::new()
                                                            },
                                                        ]
                                                    })
                                                    .collect::<Vec<Vec<String>>>();
                                                let (heading_enabled, feedback_enabled) =
                                                    SqlEditorWidget::current_output_settings(
                                                        &session,
                                                    );
                                                let headers =
                                                    SqlEditorWidget::apply_heading_setting(
                                                        vec![
                                                            "COLUMN".to_string(),
                                                            "TYPE".to_string(),
                                                            "NULLABLE".to_string(),
                                                            "PK".to_string(),
                                                        ],
                                                        heading_enabled,
                                                    );
                                                SqlEditorWidget::emit_select_result(
                                                    &sender,
                                                    &session,
                                                    &conn_name,
                                                    result_index,
                                                    &title,
                                                    headers,
                                                    rows,
                                                    true,
                                                    feedback_enabled,
                                                );
                                                result_index += 1;
                                            }
                                        }
                                        Err(err) => {
                                            if script_mode {
                                                SqlEditorWidget::emit_script_message(
                                                    &sender,
                                                    &session,
                                                    &title,
                                                    &format!("Error: {}", err),
                                                );
                                            } else {
                                                let emitted =
                                                    SqlEditorWidget::emit_non_select_result(
                                                        &sender,
                                                        &session,
                                                        &conn_name,
                                                        result_index,
                                                        &title,
                                                        format!("Error: {}", err),
                                                        false,
                                                        false,
                                                        false,
                                                    );
                                                if emitted {
                                                    result_index += 1;
                                                }
                                            }
                                            command_error = true;
                                        }
                                    }
                                }
                                ToolCommand::Prompt { text } => {
                                    let mut output_text = text;
                                    let (define_enabled, scan_enabled) = match session.lock() {
                                        Ok(guard) => (guard.define_enabled, guard.scan_enabled),
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let guard = poisoned.into_inner();
                                            (guard.define_enabled, guard.scan_enabled)
                                        }
                                    };
                                    if define_enabled && scan_enabled && !output_text.is_empty() {
                                        match SqlEditorWidget::apply_define_substitution(
                                            &output_text,
                                            &session,
                                            &sender,
                                        ) {
                                            Ok(updated) => {
                                                output_text = updated;
                                            }
                                            Err(message) => {
                                                SqlEditorWidget::emit_script_message(
                                                    &sender,
                                                    &session,
                                                    "PROMPT",
                                                    &format!("Error: {}", message),
                                                );
                                                command_error = true;
                                            }
                                        }
                                    }
                                    if !command_error {
                                        SqlEditorWidget::emit_script_output(
                                            &sender,
                                            &session,
                                            vec![output_text],
                                        );
                                    }
                                }
                                ToolCommand::Pause { message } => {
                                    let prompt_text = message
                                        .filter(|text| !text.trim().is_empty())
                                        .unwrap_or_else(|| "Press ENTER to continue.".to_string());
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "PAUSE",
                                        &prompt_text,
                                    );
                                    match SqlEditorWidget::prompt_for_input_with_sender(
                                        &sender,
                                        &prompt_text,
                                    ) {
                                        Ok(_) => {}
                                        Err(_) => {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                "PAUSE",
                                                "Error: PAUSE cancelled.",
                                            );
                                            command_error = true;
                                        }
                                    }
                                }
                                ToolCommand::Accept { name, prompt } => {
                                    let prompt_text = prompt
                                        .unwrap_or_else(|| format!("Enter value for {}:", name));
                                    match SqlEditorWidget::prompt_for_input_with_sender(
                                        &sender,
                                        &prompt_text,
                                    ) {
                                        Ok(value) => {
                                            let key = SessionState::normalize_name(&name);
                                            match session.lock() {
                                                Ok(mut guard) => {
                                                    guard
                                                        .define_vars
                                                        .insert(key.clone(), value.clone());
                                                }
                                                Err(poisoned) => {
                                                    eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                    let mut guard = poisoned.into_inner();
                                                    guard
                                                        .define_vars
                                                        .insert(key.clone(), value.clone());
                                                }
                                            }
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                &format!("ACCEPT {}", key),
                                                &format!("Value assigned to {}", key),
                                            );
                                        }
                                        Err(message) => {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                &format!("ACCEPT {}", name),
                                                &format!("Error: {}", message),
                                            );
                                            command_error = true;
                                        }
                                    }
                                }
                                ToolCommand::Define { name, value } => {
                                    let (define_enabled, scan_enabled) = match session.lock() {
                                        Ok(guard) => (guard.define_enabled, guard.scan_enabled),
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let guard = poisoned.into_inner();
                                            (guard.define_enabled, guard.scan_enabled)
                                        }
                                    };
                                    let mut resolved_value = value;
                                    if define_enabled && scan_enabled {
                                        match SqlEditorWidget::apply_define_substitution(
                                            &resolved_value,
                                            &session,
                                            &sender,
                                        ) {
                                            Ok(updated) => {
                                                resolved_value = updated;
                                            }
                                            Err(message) => {
                                                SqlEditorWidget::emit_script_message(
                                                    &sender,
                                                    &session,
                                                    &format!("DEFINE {}", name),
                                                    &format!("Error: {}", message),
                                                );
                                                command_error = true;
                                            }
                                        }
                                    }
                                    let key = SessionState::normalize_name(&name);
                                    if !command_error {
                                        match session.lock() {
                                            Ok(mut guard) => {
                                                guard
                                                    .define_vars
                                                    .insert(key.clone(), resolved_value.clone());
                                            }
                                            Err(poisoned) => {
                                                eprintln!(
                                                "Warning: session state lock was poisoned; recovering."
                                            );
                                                let mut guard = poisoned.into_inner();
                                                guard
                                                    .define_vars
                                                    .insert(key.clone(), resolved_value.clone());
                                            }
                                        }
                                        SqlEditorWidget::emit_script_message(
                                            &sender,
                                            &session,
                                            &format!("DEFINE {}", key),
                                            &format!("Defined {} = {}", key, resolved_value),
                                        );
                                    }
                                }
                                ToolCommand::Undefine { name } => {
                                    let key = SessionState::normalize_name(&name);
                                    match session.lock() {
                                        Ok(mut guard) => {
                                            guard.define_vars.remove(&key);
                                        }
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let mut guard = poisoned.into_inner();
                                            guard.define_vars.remove(&key);
                                        }
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        &format!("UNDEFINE {}", key),
                                        &format!("Undefined {}", key),
                                    );
                                }
                                ToolCommand::ColumnNewValue {
                                    column_name,
                                    variable_name,
                                } => {
                                    let column_key = SessionState::normalize_name(&column_name);
                                    let variable_key = SessionState::normalize_name(&variable_name);
                                    match session.lock() {
                                        Ok(mut guard) => {
                                            guard
                                                .column_new_values
                                                .insert(column_key.clone(), variable_key.clone());
                                        }
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let mut guard = poisoned.into_inner();
                                            guard
                                                .column_new_values
                                                .insert(column_key.clone(), variable_key.clone());
                                        }
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        &format!(
                                            "COLUMN {} NEW_VALUE {}",
                                            column_key, variable_key
                                        ),
                                        &format!(
                                            "Registered NEW_VALUE mapping: {} -> {}",
                                            column_key, variable_key
                                        ),
                                    );
                                }
                                ToolCommand::BreakOn { column_name } => {
                                    let key = SessionState::normalize_name(&column_name);
                                    match session.lock() {
                                        Ok(mut guard) => {
                                            guard.break_column = Some(key.clone());
                                        }
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let mut guard = poisoned.into_inner();
                                            guard.break_column = Some(key.clone());
                                        }
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "BREAK",
                                        &format!("BREAK ON {}", key),
                                    );
                                }
                                ToolCommand::BreakOff => {
                                    match session.lock() {
                                        Ok(mut guard) => {
                                            guard.break_column = None;
                                        }
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let mut guard = poisoned.into_inner();
                                            guard.break_column = None;
                                        }
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "BREAK",
                                        "BREAK OFF",
                                    );
                                }
                                ToolCommand::ClearBreaks => {
                                    match session.lock() {
                                        Ok(mut guard) => {
                                            guard.break_column = None;
                                        }
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let mut guard = poisoned.into_inner();
                                            guard.break_column = None;
                                        }
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "CLEAR",
                                        "BREAKS cleared",
                                    );
                                }
                                ToolCommand::ClearComputes => {
                                    match session.lock() {
                                        Ok(mut guard) => {
                                            guard.compute = None;
                                        }
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let mut guard = poisoned.into_inner();
                                            guard.compute = None;
                                        }
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "CLEAR",
                                        "COMPUTES cleared",
                                    );
                                }
                                ToolCommand::ClearBreaksComputes => {
                                    match session.lock() {
                                        Ok(mut guard) => {
                                            guard.break_column = None;
                                            guard.compute = None;
                                        }
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let mut guard = poisoned.into_inner();
                                            guard.break_column = None;
                                            guard.compute = None;
                                        }
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "CLEAR",
                                        "BREAKS and COMPUTES cleared",
                                    );
                                }
                                ToolCommand::Compute {
                                    mode,
                                    of_column,
                                    on_column,
                                } => {
                                    match session.lock() {
                                        Ok(mut guard) => {
                                            guard.compute = Some(crate::db::ComputeConfig {
                                                mode,
                                                of_column: of_column.clone(),
                                                on_column: on_column.clone(),
                                            });
                                        }
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let mut guard = poisoned.into_inner();
                                            guard.compute = Some(crate::db::ComputeConfig {
                                                mode,
                                                of_column: of_column.clone(),
                                                on_column: on_column.clone(),
                                            });
                                        }
                                    }
                                    let mode_text = match mode {
                                        crate::db::ComputeMode::Sum => "COMPUTE SUM",
                                        crate::db::ComputeMode::Count => "COMPUTE COUNT",
                                    };
                                    let label = match (of_column.as_deref(), on_column.as_deref()) {
                                        (Some(of_col), Some(on_col)) => {
                                            format!("{} OF {} ON {}", mode_text, of_col, on_col)
                                        }
                                        _ => mode_text.to_string(),
                                    };
                                    SqlEditorWidget::emit_script_message(
                                        &sender, &session, "COMPUTE", &label,
                                    );
                                }
                                ToolCommand::ComputeOff => {
                                    match session.lock() {
                                        Ok(mut guard) => {
                                            guard.compute = None;
                                        }
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let mut guard = poisoned.into_inner();
                                            guard.compute = None;
                                        }
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "COMPUTE",
                                        "COMPUTE OFF",
                                    );
                                }
                                ToolCommand::SetErrorContinue { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.continue_on_error = enabled;
                                    }
                                    continue_on_error = enabled;

                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET ERRORCONTINUE",
                                        &format!(
                                            "ERRORCONTINUE {}",
                                            if enabled { "ON" } else { "OFF" }
                                        ),
                                    );
                                }
                                ToolCommand::SetAutoCommit { enabled } => {
                                    {
                                        let mut conn_guard = lock_connection_with_activity(
                                            &shared_connection,
                                            db_activity.clone(),
                                        );
                                        conn_guard.set_auto_commit(enabled);
                                    }
                                    auto_commit = enabled;
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET AUTOCOMMIT",
                                        if enabled {
                                            "Auto-commit enabled"
                                        } else {
                                            "Auto-commit disabled"
                                        },
                                    );
                                    let _ =
                                        sender.send(QueryProgress::AutoCommitChanged { enabled });
                                    app::awake();
                                }
                                ToolCommand::SetDefine {
                                    enabled,
                                    define_char,
                                } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.define_enabled = enabled;
                                        if let Some(ch) = define_char {
                                            guard.define_char = ch;
                                        }
                                    }
                                    let msg = if let Some(ch) = define_char {
                                        format!("DEFINE '{}'", ch)
                                    } else {
                                        format!("DEFINE {}", if enabled { "ON" } else { "OFF" })
                                    };
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET DEFINE",
                                        &msg,
                                    );
                                }
                                ToolCommand::SetScan { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.scan_enabled = enabled;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET SCAN",
                                        &format!("SCAN {}", if enabled { "ON" } else { "OFF" }),
                                    );
                                }
                                ToolCommand::SetVerify { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.verify_enabled = enabled;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET VERIFY",
                                        &format!("VERIFY {}", if enabled { "ON" } else { "OFF" }),
                                    );
                                }
                                ToolCommand::SetEcho { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.echo_enabled = enabled;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET ECHO",
                                        &format!("ECHO {}", if enabled { "ON" } else { "OFF" }),
                                    );
                                }
                                ToolCommand::SetTiming { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.timing_enabled = enabled;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET TIMING",
                                        &format!("TIMING {}", if enabled { "ON" } else { "OFF" }),
                                    );
                                }
                                ToolCommand::SetFeedback { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.feedback_enabled = enabled;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET FEEDBACK",
                                        &format!("FEEDBACK {}", if enabled { "ON" } else { "OFF" }),
                                    );
                                }
                                ToolCommand::SetHeading { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.heading_enabled = enabled;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET HEADING",
                                        &format!("HEADING {}", if enabled { "ON" } else { "OFF" }),
                                    );
                                }
                                ToolCommand::SetPageSize { size } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.pagesize = size;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET PAGESIZE",
                                        &format!("PAGESIZE {}", size),
                                    );
                                }
                                ToolCommand::SetLineSize { size } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.linesize = size;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET LINESIZE",
                                        &format!("LINESIZE {}", size),
                                    );
                                }
                                ToolCommand::SetTrimSpool { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.trimspool_enabled = enabled;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET TRIMSPOOL",
                                        &format!(
                                            "TRIMSPOOL {}",
                                            if enabled { "ON" } else { "OFF" }
                                        ),
                                    );
                                }
                                ToolCommand::SetTrimOut { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.trimout_enabled = enabled;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET TRIMOUT",
                                        &format!("TRIMOUT {}", if enabled { "ON" } else { "OFF" }),
                                    );
                                }
                                ToolCommand::SetSqlBlankLines { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.sqlblanklines_enabled = enabled;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET SQLBLANKLINES",
                                        &format!(
                                            "SQLBLANKLINES {}",
                                            if enabled { "ON" } else { "OFF" }
                                        ),
                                    );
                                }
                                ToolCommand::SetTab { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.tab_enabled = enabled;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET TAB",
                                        &format!("TAB {}", if enabled { "ON" } else { "OFF" }),
                                    );
                                }
                                ToolCommand::SetColSep { separator } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.colsep = separator.clone();
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET COLSEP",
                                        &format!("COLSEP {}", separator),
                                    );
                                }
                                ToolCommand::SetNull { null_text } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.null_text = null_text.clone();
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET NULL",
                                        &format!("NULL {}", null_text),
                                    );
                                }
                                ToolCommand::Spool { path, append } => match path {
                                    Some(path) => {
                                        let target_path = if Path::new(&path).is_absolute() {
                                            PathBuf::from(&path)
                                        } else {
                                            current_frame_base_dir.join(&path)
                                        };
                                        match session.lock() {
                                            Ok(mut guard) => {
                                                guard.spool_path = Some(target_path.clone());
                                                guard.spool_truncate = !append;
                                            }
                                            Err(poisoned) => {
                                                eprintln!(
                                                "Warning: session state lock was poisoned; recovering."
                                            );
                                                let mut guard = poisoned.into_inner();
                                                guard.spool_path = Some(target_path.clone());
                                                guard.spool_truncate = !append;
                                            }
                                        }
                                        SqlEditorWidget::emit_script_message(
                                            &sender,
                                            &session,
                                            "SPOOL",
                                            &format!(
                                                "Spooling output to {} ({})",
                                                target_path.display(),
                                                if append { "append" } else { "replace" }
                                            ),
                                        );
                                    }
                                    None if append => {
                                        let has_spool_target = match session.lock() {
                                            Ok(mut guard) => {
                                                let has_target = guard.spool_path.is_some();
                                                guard.spool_truncate = false;
                                                has_target
                                            }
                                            Err(poisoned) => {
                                                eprintln!(
                                                "Warning: session state lock was poisoned; recovering."
                                            );
                                                let mut guard = poisoned.into_inner();
                                                let has_target = guard.spool_path.is_some();
                                                guard.spool_truncate = false;
                                                has_target
                                            }
                                        };
                                        if has_spool_target {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                "SPOOL",
                                                "Spooling in append mode",
                                            );
                                        } else {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                "SPOOL APPEND",
                                                "Error: No active spool file. Use SPOOL <file> APPEND.",
                                            );
                                            command_error = true;
                                        }
                                    }
                                    None => {
                                        match session.lock() {
                                            Ok(mut guard) => {
                                                guard.spool_path = None;
                                                guard.spool_truncate = false;
                                            }
                                            Err(poisoned) => {
                                                eprintln!(
                                                "Warning: session state lock was poisoned; recovering."
                                            );
                                                let mut guard = poisoned.into_inner();
                                                guard.spool_path = None;
                                                guard.spool_truncate = false;
                                            }
                                        }
                                        SqlEditorWidget::emit_script_message(
                                            &sender,
                                            &session,
                                            "SPOOL",
                                            "Spooling disabled",
                                        );
                                    }
                                },
                                ToolCommand::WheneverSqlError { exit, action } => {
                                    if exit
                                        && action
                                            .as_deref()
                                            .map(|v| v.trim().eq_ignore_ascii_case("SQL.SQLCODE"))
                                            .unwrap_or(false)
                                        && !script_mode
                                    {
                                        SqlEditorWidget::emit_script_message(
                                            &sender,
                                            &session,
                                            "WHENEVER SQLERROR",
                                            "Error: EXIT SQL.SQLCODE is supported only in batch(script) execution.",
                                        );
                                        command_error = true;
                                    } else {
                                        {
                                            let mut guard = match session.lock() {
                                                Ok(guard) => guard,
                                                Err(poisoned) => {
                                                    eprintln!(
                                                        "Warning: session state lock was poisoned; recovering."
                                                    );
                                                    poisoned.into_inner()
                                                }
                                            };
                                            guard.continue_on_error = !exit;
                                        }
                                        continue_on_error = !exit;
                                        SqlEditorWidget::emit_script_message(
                                            &sender,
                                            &session,
                                            "WHENEVER SQLERROR",
                                            if exit { "Mode EXIT" } else { "Mode CONTINUE" },
                                        );
                                    }
                                }
                                ToolCommand::WheneverOsError { exit } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.continue_on_error = !exit;
                                    }
                                    continue_on_error = !exit;
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "WHENEVER OSERROR",
                                        if exit { "Mode EXIT" } else { "Mode CONTINUE" },
                                    );
                                }
                                ToolCommand::Exit => {
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "EXIT",
                                        "Execution stopped.",
                                    );
                                    stop_execution = true;
                                }
                                ToolCommand::Quit => {
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "QUIT",
                                        "Execution stopped.",
                                    );
                                    stop_execution = true;
                                }
                                ToolCommand::Connect {
                                    username,
                                    password,
                                    host,
                                    port,
                                    service_name,
                                } => {
                                    let conn_info = ConnectionInfo {
                                        name: format!("{}@{}", username, host),
                                        username,
                                        password,
                                        host,
                                        port,
                                        service_name,
                                    };

                                    let connect_result = {
                                        let mut conn_guard = lock_connection_with_activity(
                                            &shared_connection,
                                            db_activity.clone(),
                                        );
                                        match conn_guard.connect(conn_info.clone()) {
                                            Ok(_) => {
                                                conn_guard.refresh_tracked_connection();
                                                let conn_opt_local = conn_guard.get_connection();
                                                let sanitized =
                                                    SqlEditorWidget::connection_info_for_ui(
                                                        conn_guard.get_info(),
                                                    );
                                                let conn_name_local = if conn_guard.is_connected() {
                                                    conn_guard.get_info().name.clone()
                                                } else {
                                                    String::new()
                                                };
                                                Ok((conn_opt_local, sanitized, conn_name_local))
                                            }
                                            Err(err) => Err(err),
                                        }
                                    };

                                    match connect_result {
                                        Ok((
                                            next_conn_opt,
                                            sanitized_conn_info,
                                            next_conn_name,
                                        )) => {
                                            conn_opt = next_conn_opt;
                                            conn_name = next_conn_name;
                                            // Update cancel connection so break_execution() uses the new connection
                                            if let Some(ref conn) = conn_opt {
                                                SqlEditorWidget::set_current_query_connection(
                                                    &current_query_connection,
                                                    Some(Arc::clone(conn)),
                                                );
                                            }
                                            match session.lock() {
                                                Ok(mut guard) => guard.reset(),
                                                Err(poisoned) => {
                                                    eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                    poisoned.into_inner().reset();
                                                }
                                            }
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                "CONNECT",
                                                &format!("Connected to {}", conn_info.name),
                                            );
                                            if let Some(conn) = conn_opt.as_ref() {
                                                let previous_timeout =
                                                    conn.call_timeout().ok().flatten();
                                                cleanup.track_timeout(
                                                    Arc::clone(conn),
                                                    previous_timeout,
                                                );
                                                if let Err(err) =
                                                    conn.set_call_timeout(query_timeout)
                                                {
                                                    SqlEditorWidget::emit_script_message(
                                                        &sender,
                                                        &session,
                                                        "CONNECT",
                                                        &format!(
                                                            "Error: Failed to apply query timeout after CONNECT: {}",
                                                            err
                                                        ),
                                                    );
                                                    command_error = true;
                                                }
                                                if let Err(err) =
                                                    SqlEditorWidget::sync_serveroutput_with_session(
                                                        conn.as_ref(),
                                                        &session,
                                                    )
                                                {
                                                    eprintln!(
                                                        "Failed to apply SERVEROUTPUT after CONNECT: {err}"
                                                    );
                                                }
                                            }
                                            let _ = sender.send(QueryProgress::ConnectionChanged {
                                                info: Some(sanitized_conn_info),
                                            });
                                            app::awake();
                                        }
                                        Err(err) => {
                                            conn_opt = None;
                                            conn_name.clear();
                                            SqlEditorWidget::set_current_query_connection(
                                                &current_query_connection,
                                                None,
                                            );
                                            let error_msg = format!("Connection failed: {}", err);
                                            SqlEditorWidget::emit_script_message(
                                                &sender, &session, "CONNECT", &error_msg,
                                            );
                                            let _ = sender.send(QueryProgress::ConnectionChanged {
                                                info: None,
                                            });
                                            app::awake();
                                            command_error = true;
                                        }
                                    }
                                }
                                ToolCommand::Disconnect => {
                                    // Treat stale handles (connection exists but connected flag is false)
                                    // as a disconnectable state so UI/session state is fully reset.
                                    let (had_connection, next_conn_opt, next_conn_name) = {
                                        let mut conn_guard = lock_connection_with_activity(
                                            &shared_connection,
                                            db_activity.clone(),
                                        );
                                        let had_connection = conn_guard.is_connected()
                                            || conn_guard.get_connection().is_some();
                                        conn_guard.disconnect();
                                        conn_guard.refresh_tracked_connection();
                                        let next_conn_opt = conn_guard.get_connection();
                                        let next_conn_name = if conn_guard.is_connected() {
                                            conn_guard.get_info().name.clone()
                                        } else {
                                            String::new()
                                        };
                                        (had_connection, next_conn_opt, next_conn_name)
                                    };

                                    // Clear cancel connection before disconnect
                                    SqlEditorWidget::set_current_query_connection(
                                        &current_query_connection,
                                        None,
                                    );
                                    conn_opt = next_conn_opt;
                                    conn_name = next_conn_name;
                                    match session.lock() {
                                        Ok(mut guard) => guard.reset(),
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            poisoned.into_inner().reset();
                                        }
                                    }
                                    let disconnect_message = if had_connection {
                                        "Disconnected from database"
                                    } else {
                                        "Not connected to any database"
                                    };
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "DISCONNECT",
                                        disconnect_message,
                                    );
                                    cleanup.clear_timeout_tracking();
                                    let _ = sender
                                        .send(QueryProgress::ConnectionChanged { info: None });
                                    app::awake();
                                }
                                ToolCommand::RunScript {
                                    path,
                                    relative_to_caller,
                                } => {
                                    let include_base_dir = if relative_to_caller {
                                        current_frame_base_dir.as_path()
                                    } else {
                                        working_dir.as_path()
                                    };
                                    let (target_path, normalized_target_path) =
                                        SqlEditorWidget::resolve_script_include_path(
                                            &path,
                                            relative_to_caller,
                                            current_frame_base_dir.as_path(),
                                            working_dir.as_path(),
                                        );
                                    if let Err(message) =
                                        SqlEditorWidget::validate_script_include_target(
                                            &frames,
                                            normalized_target_path.as_path(),
                                        )
                                    {
                                        SqlEditorWidget::emit_script_message(
                                            &sender,
                                            &session,
                                            if relative_to_caller { "@@" } else { "@" },
                                            &format!("Error: {}", message),
                                        );
                                        if !continue_on_error {
                                            stop_execution = true;
                                        }
                                        continue;
                                    }
                                    match SqlEditorWidget::load_script_include(
                                        target_path.as_path(),
                                        normalized_target_path.as_path(),
                                        include_base_dir,
                                    ) {
                                        Ok(resolved_include) => {
                                            frames.push(ScriptExecutionFrame {
                                                items: resolved_include.items,
                                                index: 0,
                                                base_dir: resolved_include.script_dir,
                                                source_path: Some(resolved_include.source_path),
                                            });
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                if relative_to_caller { "@@" } else { "@" },
                                                &format!(
                                                    "Running script {}",
                                                    target_path.display()
                                                ),
                                            );
                                        }
                                        Err(message) => {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                if relative_to_caller { "@@" } else { "@" },
                                                &format!("Error: {}", message),
                                            );
                                            command_error = true;
                                        }
                                    }
                                }
                                ToolCommand::Unsupported {
                                    raw,
                                    message,
                                    is_error,
                                } => {
                                    if is_error {
                                        SqlEditorWidget::emit_script_message(
                                            &sender,
                                            &session,
                                            &raw,
                                            &format!("Error: {}", message),
                                        );
                                        command_error = true;
                                    } else {
                                        SqlEditorWidget::emit_script_message(
                                            &sender,
                                            &session,
                                            &raw,
                                            &format!("Warning: {}", message),
                                        );
                                    }
                                }
                            }

                            if command_error && !continue_on_error {
                                stop_execution = true;
                            }
                        }
                        ScriptItem::Statement(statement) => {
                            // For statements, we need a connection
                            let conn = match conn_opt.as_ref() {
                                Some(c) => c,
                                None => {
                                    // This shouldn't happen as we checked earlier
                                    eprintln!(
                                        "Error: No connection available for statement execution"
                                    );
                                    let emitted = SqlEditorWidget::emit_non_select_result(
                                        &sender,
                                        &session,
                                        &conn_name,
                                        result_index,
                                        &statement,
                                        "Error: Not connected to database".to_string(),
                                        false,
                                        false,
                                        script_mode,
                                    );
                                    if emitted {
                                        result_index += 1;
                                    }
                                    stop_execution = true;
                                    continue;
                                }
                            };

                            let trimmed = statement.trim_start_matches(';').trim();
                            if trimmed.is_empty() {
                                continue;
                            }

                            let mut sql_text = trimmed.to_string();
                            let (define_enabled, scan_enabled, verify_enabled) =
                                match session.lock() {
                                    Ok(guard) => (
                                        guard.define_enabled,
                                        guard.scan_enabled,
                                        guard.verify_enabled,
                                    ),
                                    Err(poisoned) => {
                                        eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                        let guard = poisoned.into_inner();
                                        (
                                            guard.define_enabled,
                                            guard.scan_enabled,
                                            guard.verify_enabled,
                                        )
                                    }
                                };
                            if define_enabled && scan_enabled {
                                let sql_before = sql_text.clone();
                                match SqlEditorWidget::apply_define_substitution(
                                    &sql_text, &session, &sender,
                                ) {
                                    Ok(updated) => {
                                        if verify_enabled && updated != sql_before {
                                            SqlEditorWidget::emit_script_output(
                                                &sender,
                                                &session,
                                                vec![
                                                    format!("old: {}", sql_before),
                                                    format!("new: {}", updated),
                                                ],
                                            );
                                        }
                                        sql_text = updated;
                                    }
                                    Err(message) => {
                                        let emitted = SqlEditorWidget::emit_non_select_result(
                                            &sender,
                                            &session,
                                            &conn_name,
                                            result_index,
                                            trimmed,
                                            format!("Error: {}", message),
                                            false,
                                            false,
                                            script_mode,
                                        );
                                        if emitted {
                                            result_index += 1;
                                        }
                                        if !continue_on_error {
                                            stop_execution = true;
                                        }
                                        continue;
                                    }
                                }
                            }

                            let cleaned = SqlEditorWidget::strip_leading_comments(&sql_text);
                            let upper = cleaned.to_uppercase();

                            if QueryExecutor::is_plain_commit(&sql_text) {
                                let mut timed_out = false;
                                let statement_start = Instant::now();
                                let mut result = match conn.commit() {
                                    Ok(()) => QueryResult {
                                        sql: sql_text.to_string(),
                                        columns: vec![],
                                        rows: vec![],
                                        row_count: 0,
                                        execution_time: Duration::from_secs(0),
                                        message: "Commit complete".to_string(),
                                        is_select: false,
                                        success: true,
                                    },
                                    Err(err) => {
                                        timed_out = SqlEditorWidget::is_timeout_error(&err);
                                        QueryResult::new_error(&sql_text, &err.to_string())
                                    }
                                };
                                let timing_duration = statement_start.elapsed();
                                result.execution_time = timing_duration;
                                let result_success = result.success;
                                if script_mode {
                                    if result_success {
                                        SqlEditorWidget::emit_script_lines(
                                            &sender,
                                            &session,
                                            &result.message,
                                        );
                                    }
                                    SqlEditorWidget::emit_script_result(
                                        &sender,
                                        &conn_name,
                                        result_index,
                                        result,
                                        timed_out,
                                    );
                                } else {
                                    let index = result_index;
                                    let _ = sender.send(QueryProgress::StatementStart { index });
                                    app::awake();
                                    if !result.message.trim().is_empty() {
                                        SqlEditorWidget::append_spool_output(
                                            &session,
                                            std::slice::from_ref(&result.message),
                                        );
                                    }
                                    let _ = sender.send(QueryProgress::StatementFinished {
                                        index,
                                        result,
                                        connection_name: conn_name.clone(),
                                        timed_out,
                                    });
                                    app::awake();
                                    result_index += 1;
                                }
                                SqlEditorWidget::emit_timing_if_enabled(
                                    &sender,
                                    &session,
                                    timing_duration,
                                );
                                if load_mutex_bool(&cancel_flag)
                                    || timed_out
                                    || (!result_success && !continue_on_error)
                                {
                                    stop_execution = true;
                                }
                                continue;
                            }

                            if QueryExecutor::is_plain_rollback(&sql_text) {
                                let mut timed_out = false;
                                let statement_start = Instant::now();
                                let mut result = match conn.rollback() {
                                    Ok(()) => QueryResult {
                                        sql: sql_text.to_string(),
                                        columns: vec![],
                                        rows: vec![],
                                        row_count: 0,
                                        execution_time: Duration::from_secs(0),
                                        message: "Rollback complete".to_string(),
                                        is_select: false,
                                        success: true,
                                    },
                                    Err(err) => {
                                        timed_out = SqlEditorWidget::is_timeout_error(&err);
                                        QueryResult::new_error(&sql_text, &err.to_string())
                                    }
                                };
                                let timing_duration = statement_start.elapsed();
                                result.execution_time = timing_duration;
                                let result_success = result.success;
                                if script_mode {
                                    if result_success {
                                        SqlEditorWidget::emit_script_lines(
                                            &sender,
                                            &session,
                                            &result.message,
                                        );
                                    }
                                    SqlEditorWidget::emit_script_result(
                                        &sender,
                                        &conn_name,
                                        result_index,
                                        result,
                                        timed_out,
                                    );
                                } else {
                                    let index = result_index;
                                    let _ = sender.send(QueryProgress::StatementStart { index });
                                    app::awake();
                                    if !result.message.trim().is_empty() {
                                        SqlEditorWidget::append_spool_output(
                                            &session,
                                            std::slice::from_ref(&result.message),
                                        );
                                    }
                                    let _ = sender.send(QueryProgress::StatementFinished {
                                        index,
                                        result,
                                        connection_name: conn_name.clone(),
                                        timed_out,
                                    });
                                    app::awake();
                                    result_index += 1;
                                }
                                SqlEditorWidget::emit_timing_if_enabled(
                                    &sender,
                                    &session,
                                    timing_duration,
                                );
                                if load_mutex_bool(&cancel_flag)
                                    || timed_out
                                    || (!result_success && !continue_on_error)
                                {
                                    stop_execution = true;
                                }
                                continue;
                            }

                            let compiled_object = QueryExecutor::parse_compiled_object(&sql_text);
                            let is_compiled_plsql = compiled_object.is_some();
                            if let Some(object) = compiled_object.clone() {
                                let mut guard = match session.lock() {
                                    Ok(guard) => guard,
                                    Err(poisoned) => {
                                        eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                        poisoned.into_inner()
                                    }
                                };
                                guard.last_compiled = Some(object);
                            }

                            let exec_call = QueryExecutor::normalize_exec_call(&sql_text);
                            if exec_call.is_some() {
                                if let Err(message) =
                                    QueryExecutor::check_named_positional_mix(&sql_text)
                                {
                                    let emitted = SqlEditorWidget::emit_non_select_result(
                                        &sender,
                                        &session,
                                        &conn_name,
                                        result_index,
                                        &sql_text,
                                        format!("Error: {}", message),
                                        false,
                                        false,
                                        script_mode,
                                    );
                                    if emitted {
                                        result_index += 1;
                                    }
                                    if !continue_on_error {
                                        stop_execution = true;
                                    }
                                    continue;
                                }
                            }

                            let is_plsql_block =
                                upper.starts_with("BEGIN") || upper.starts_with("DECLARE");
                            let is_select = QueryExecutor::is_select_statement(&sql_text);

                            if exec_call.is_some() || is_plsql_block {
                                let mut sql_to_execute =
                                    exec_call.unwrap_or_else(|| sql_text.to_string());
                                if is_plsql_block {
                                    sql_to_execute =
                                        SqlEditorWidget::ensure_plsql_terminator(&sql_to_execute);
                                }
                                let binds = match session.lock() {
                                    Ok(guard) => {
                                        QueryExecutor::resolve_binds(&sql_to_execute, &guard)
                                    }
                                    Err(poisoned) => {
                                        eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                        QueryExecutor::resolve_binds(
                                            &sql_to_execute,
                                            &poisoned.into_inner(),
                                        )
                                    }
                                };

                                let binds = match binds {
                                    Ok(binds) => binds,
                                    Err(message) => {
                                        let emitted = SqlEditorWidget::emit_non_select_result(
                                            &sender,
                                            &session,
                                            &conn_name,
                                            result_index,
                                            &sql_text,
                                            format!("Error: {}", message),
                                            false,
                                            false,
                                            script_mode,
                                        );
                                        if emitted {
                                            result_index += 1;
                                        }
                                        if !continue_on_error {
                                            stop_execution = true;
                                        }
                                        continue;
                                    }
                                };

                                let statement_start = Instant::now();
                                let mut timed_out = false;
                                let stmt = match QueryExecutor::execute_with_binds(
                                    conn.as_ref(),
                                    &sql_to_execute,
                                    &binds,
                                ) {
                                    Ok(stmt) => stmt,
                                    Err(err) => {
                                        let cancelled = SqlEditorWidget::is_cancel_error(&err);
                                        timed_out = SqlEditorWidget::is_timeout_error(&err);
                                        let message =
                                            SqlEditorWidget::choose_execution_error_message(
                                                cancelled,
                                                timed_out,
                                                query_timeout,
                                                err.to_string(),
                                            );
                                        if script_mode {
                                            let result =
                                                QueryResult::new_error(&sql_text, &message);
                                            SqlEditorWidget::emit_script_result(
                                                &sender,
                                                &conn_name,
                                                result_index,
                                                result,
                                                timed_out,
                                            );
                                        } else {
                                            let index = result_index;
                                            let _ = sender
                                                .send(QueryProgress::StatementStart { index });
                                            app::awake();
                                            SqlEditorWidget::append_spool_output(
                                                &session,
                                                std::slice::from_ref(&message),
                                            );
                                            let result =
                                                QueryResult::new_error(&sql_text, &message);
                                            let _ = sender.send(QueryProgress::StatementFinished {
                                                index,
                                                result,
                                                connection_name: conn_name.clone(),
                                                timed_out,
                                            });
                                            app::awake();
                                            result_index += 1;
                                        }
                                        SqlEditorWidget::emit_timing_if_enabled(
                                            &sender,
                                            &session,
                                            statement_start.elapsed(),
                                        );
                                        if timed_out || cancelled || !continue_on_error {
                                            stop_execution = true;
                                        }
                                        continue;
                                    }
                                };

                                let timing_duration = statement_start.elapsed();
                                let base_message = if is_plsql_block {
                                    "PL/SQL block executed successfully".to_string()
                                } else {
                                    "Call executed successfully".to_string()
                                };
                                let mut result = QueryResult {
                                    sql: sql_text.to_string(),
                                    columns: vec![],
                                    rows: vec![],
                                    row_count: 0,
                                    execution_time: timing_duration,
                                    message: base_message,
                                    is_select: false,
                                    success: true,
                                };

                                if load_mutex_bool(&cancel_flag) {
                                    stop_execution = true;
                                    continue;
                                }

                                let mut out_messages: Vec<String> = Vec::new();
                                let (_colsep, null_text, _trimspool_enabled) =
                                    SqlEditorWidget::current_text_output_settings(&session);
                                if let Ok(updates) =
                                    QueryExecutor::fetch_scalar_bind_updates(&stmt, &binds)
                                {
                                    let mut guard = match session.lock() {
                                        Ok(guard) => guard,
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            poisoned.into_inner()
                                        }
                                    };
                                    for (name, value) in updates {
                                        if let Some(bind) = guard.binds.get_mut(&name) {
                                            bind.value = value.clone();
                                        }
                                        if let BindValue::Scalar(val) = value {
                                            out_messages.push(format!(
                                                ":{} = {}",
                                                name,
                                                val.unwrap_or_else(|| null_text.clone())
                                            ));
                                        }
                                    }
                                }

                                if !out_messages.is_empty() {
                                    result.message = format!(
                                        "{} | OUT: {}",
                                        result.message,
                                        out_messages.join(", ")
                                    );
                                }

                                if load_mutex_bool(&cancel_flag) {
                                    stop_execution = true;
                                    continue;
                                }

                                if auto_commit {
                                    if let Err(err) = conn.commit() {
                                        result = QueryResult::new_error(
                                            &sql_text,
                                            &format!("Auto-commit failed: {}", err),
                                        );
                                    } else {
                                        result.message =
                                            format!("{} | Auto-commit applied", result.message);
                                    }
                                }

                                if load_mutex_bool(&cancel_flag) {
                                    stop_execution = true;
                                    continue;
                                }

                                let ref_cursors =
                                    match QueryExecutor::extract_ref_cursors(&stmt, &binds) {
                                        Ok(cursors) => cursors,
                                        Err(err) => {
                                            let cancelled = SqlEditorWidget::is_cancel_error(&err);
                                            timed_out = SqlEditorWidget::is_timeout_error(&err);
                                            let message =
                                                SqlEditorWidget::choose_execution_error_message(
                                                    cancelled,
                                                    timed_out,
                                                    query_timeout,
                                                    format!(
                                                        "Failed to fetch REF CURSOR results: {err}"
                                                    ),
                                                );
                                            if script_mode {
                                                let error_result =
                                                    QueryResult::new_error(&sql_text, &message);
                                                SqlEditorWidget::emit_script_result(
                                                    &sender,
                                                    &conn_name,
                                                    result_index,
                                                    error_result,
                                                    timed_out,
                                                );
                                            } else {
                                                let index = result_index;
                                                let _ = sender
                                                    .send(QueryProgress::StatementStart { index });
                                                app::awake();
                                                SqlEditorWidget::append_spool_output(
                                                    &session,
                                                    std::slice::from_ref(&message),
                                                );
                                                let error_result =
                                                    QueryResult::new_error(&sql_text, &message);
                                                let _ =
                                                    sender.send(QueryProgress::StatementFinished {
                                                        index,
                                                        result: error_result,
                                                        connection_name: conn_name.clone(),
                                                        timed_out,
                                                    });
                                                app::awake();
                                                result_index += 1;
                                            }
                                            SqlEditorWidget::emit_timing_if_enabled(
                                                &sender,
                                                &session,
                                                timing_duration,
                                            );
                                            if timed_out || cancelled || !continue_on_error {
                                                stop_execution = true;
                                            }
                                            continue;
                                        }
                                    };
                                if load_mutex_bool(&cancel_flag) {
                                    stop_execution = true;
                                    continue;
                                }
                                let implicit_results = match QueryExecutor::extract_implicit_results(
                                    &stmt,
                                ) {
                                    Ok(cursors) => cursors,
                                    Err(err) => {
                                        let cancelled = SqlEditorWidget::is_cancel_error(&err);
                                        timed_out = SqlEditorWidget::is_timeout_error(&err);
                                        let message =
                                                SqlEditorWidget::choose_execution_error_message(
                                                    cancelled,
                                                    timed_out,
                                                    query_timeout,
                                                    format!(
                                                        "Failed to fetch implicit result cursors: {err}"
                                                    ),
                                                );
                                        if script_mode {
                                            let error_result =
                                                QueryResult::new_error(&sql_text, &message);
                                            SqlEditorWidget::emit_script_result(
                                                &sender,
                                                &conn_name,
                                                result_index,
                                                error_result,
                                                timed_out,
                                            );
                                        } else {
                                            let index = result_index;
                                            let _ = sender
                                                .send(QueryProgress::StatementStart { index });
                                            app::awake();
                                            SqlEditorWidget::append_spool_output(
                                                &session,
                                                std::slice::from_ref(&message),
                                            );
                                            let error_result =
                                                QueryResult::new_error(&sql_text, &message);
                                            let _ = sender.send(QueryProgress::StatementFinished {
                                                index,
                                                result: error_result,
                                                connection_name: conn_name.clone(),
                                                timed_out,
                                            });
                                            app::awake();
                                            result_index += 1;
                                        }
                                        SqlEditorWidget::emit_timing_if_enabled(
                                            &sender,
                                            &session,
                                            timing_duration,
                                        );
                                        if timed_out || cancelled || !continue_on_error {
                                            stop_execution = true;
                                        }
                                        continue;
                                    }
                                };

                                if load_mutex_bool(&cancel_flag) {
                                    stop_execution = true;
                                    continue;
                                }

                                // Capture success before moving result into the channel
                                // to avoid cloning the entire QueryResult.
                                let result_success = result.success;
                                if script_mode {
                                    if result_success {
                                        SqlEditorWidget::emit_script_lines(
                                            &sender,
                                            &session,
                                            &result.message,
                                        );
                                    }
                                    SqlEditorWidget::emit_script_result(
                                        &sender,
                                        &conn_name,
                                        result_index,
                                        result,
                                        timed_out,
                                    );
                                } else {
                                    let index = result_index;
                                    let _ = sender.send(QueryProgress::StatementStart { index });
                                    app::awake();
                                    if !result.message.trim().is_empty() {
                                        SqlEditorWidget::append_spool_output(
                                            &session,
                                            std::slice::from_ref(&result.message),
                                        );
                                    }
                                    let _ = sender.send(QueryProgress::StatementFinished {
                                        index,
                                        result,
                                        connection_name: conn_name.clone(),
                                        timed_out,
                                    });
                                    app::awake();
                                    result_index += 1;
                                }

                                for (cursor_name, mut cursor) in ref_cursors {
                                    if stop_execution || load_mutex_bool(&cancel_flag) {
                                        break;
                                    }
                                    let index = result_index;
                                    let _ = sender.send(QueryProgress::StatementStart { index });
                                    app::awake();

                                    let mut buffered_rows: Vec<Vec<String>> = Vec::new();
                                    let mut cursor_rows: Vec<Vec<String>> = Vec::new();
                                    let mut last_flush = Instant::now();
                                    let mut has_flushed_rows = false;
                                    let cursor_start = Instant::now();
                                    let mut cursor_timed_out = false;
                                    let (heading_enabled, feedback_enabled) =
                                        SqlEditorWidget::current_output_settings(&session);
                                    let (colsep, null_text, _trimspool_enabled) =
                                        SqlEditorWidget::current_text_output_settings(&session);

                                    let cursor_label = format!("REFCURSOR :{}", cursor_name);
                                    let cursor_result = QueryExecutor::execute_ref_cursor_streaming(
                                        &mut cursor,
                                        &cursor_label,
                                        &mut |columns| {
                                            let names = columns
                                                .iter()
                                                .map(|col| col.name.clone())
                                                .collect::<Vec<String>>();
                                            let display_columns =
                                                SqlEditorWidget::apply_heading_setting(
                                                    names,
                                                    heading_enabled,
                                                );
                                            let _ = sender.send(QueryProgress::SelectStart {
                                                index,
                                                columns: display_columns.clone(),
                                                null_text: null_text.clone(),
                                            });
                                            app::awake();
                                            if !display_columns.is_empty() {
                                                SqlEditorWidget::append_spool_output(
                                                    &session,
                                                    &[display_columns.join(&colsep)],
                                                );
                                            }
                                        },
                                        &mut |row| {
                                            if load_mutex_bool(&cancel_flag) {
                                                return false;
                                            }
                                            if let Some(timeout_duration) = query_timeout {
                                                if cursor_start.elapsed() >= timeout_duration {
                                                    cursor_timed_out = true;
                                                    let _ = conn.break_execution();
                                                    return false;
                                                }
                                            }
                                            cursor_rows.push(row.clone());
                                            let mut display_row = row;
                                            SqlEditorWidget::apply_null_text_to_row(
                                                &mut display_row,
                                                &null_text,
                                            );
                                            buffered_rows.push(display_row);
                                            if SqlEditorWidget::should_flush_progress_rows(
                                                last_flush,
                                                buffered_rows.len(),
                                                has_flushed_rows,
                                            ) {
                                                let rows = std::mem::take(&mut buffered_rows);
                                                SqlEditorWidget::append_spool_rows(&session, &rows);
                                                let _ = sender
                                                    .send(QueryProgress::Rows { index, rows });
                                                app::awake();
                                                last_flush = Instant::now();
                                                has_flushed_rows = true;
                                            }
                                            true
                                        },
                                    );

                                    match cursor_result {
                                        Ok((mut query_result, was_cancelled)) => {
                                            let cursor_column_names =
                                                SqlEditorWidget::cursor_result_column_names(
                                                    &query_result.columns,
                                                );
                                            if cursor_timed_out {
                                                query_result.message =
                                                    SqlEditorWidget::timeout_message(query_timeout);
                                                query_result.success = false;
                                                cursor_timed_out = true;
                                            } else if was_cancelled {
                                                query_result.message =
                                                    SqlEditorWidget::cancel_message();
                                                query_result.success = false;
                                            }
                                            let interrupted = cursor_timed_out || was_cancelled;
                                            SqlEditorWidget::flush_buffered_rows(
                                                &sender,
                                                &session,
                                                index,
                                                &mut buffered_rows,
                                                interrupted,
                                            );
                                            SqlEditorWidget::apply_heading_to_result(
                                                &mut query_result,
                                                heading_enabled,
                                            );
                                            if !feedback_enabled {
                                                query_result.message.clear();
                                            }

                                            // Spool output before sending to avoid
                                            // cloning the message string a second time.
                                            if !query_result.message.trim().is_empty() {
                                                SqlEditorWidget::append_spool_output(
                                                    &session,
                                                    std::slice::from_ref(&query_result.message),
                                                );
                                            }
                                            let cursor_success = query_result.success;
                                            let _ = sender.send(QueryProgress::StatementFinished {
                                                index,
                                                result: query_result,
                                                connection_name: conn_name.clone(),
                                                timed_out: cursor_timed_out,
                                            });
                                            app::awake();
                                            result_index += 1;

                                            let mut guard = match session.lock() {
                                                Ok(guard) => guard,
                                                Err(poisoned) => {
                                                    eprintln!("Warning: session state lock was poisoned; recovering.");
                                                    poisoned.into_inner()
                                                }
                                            };
                                            if let Some(bind) = guard.binds.get_mut(&cursor_name) {
                                                bind.value =
                                                    BindValue::Cursor(Some(CursorResult {
                                                        columns: cursor_column_names,
                                                        rows: cursor_rows,
                                                    }));
                                            }

                                            if cursor_timed_out {
                                                stop_execution = true;
                                                break;
                                            }
                                            if !cursor_success && !continue_on_error {
                                                stop_execution = true;
                                                break;
                                            }
                                        }
                                        Err(err) => {
                                            let cancelled = SqlEditorWidget::is_cancel_error(&err);
                                            cursor_timed_out =
                                                SqlEditorWidget::is_timeout_error(&err);
                                            let message =
                                                SqlEditorWidget::choose_execution_error_message(
                                                    cancelled,
                                                    cursor_timed_out,
                                                    query_timeout,
                                                    err.to_string(),
                                                );
                                            SqlEditorWidget::append_spool_output(
                                                &session,
                                                std::slice::from_ref(&message),
                                            );
                                            let _ = sender.send(QueryProgress::StatementFinished {
                                                index,
                                                result: QueryResult::new_error(
                                                    &cursor_label,
                                                    &message,
                                                ),
                                                connection_name: conn_name.clone(),
                                                timed_out: cursor_timed_out,
                                            });
                                            app::awake();
                                            result_index += 1;

                                            if cursor_timed_out || cancelled || !continue_on_error {
                                                stop_execution = true;
                                                break;
                                            }
                                        }
                                    }
                                }

                                for (idx, mut cursor) in implicit_results.into_iter().enumerate() {
                                    if stop_execution || load_mutex_bool(&cancel_flag) {
                                        break;
                                    }
                                    let index = result_index;
                                    let _ = sender.send(QueryProgress::StatementStart { index });
                                    app::awake();

                                    let mut buffered_rows: Vec<Vec<String>> = Vec::new();
                                    let mut last_flush = Instant::now();
                                    let mut has_flushed_rows = false;
                                    let cursor_start = Instant::now();
                                    let mut cursor_timed_out = false;
                                    let (heading_enabled, feedback_enabled) =
                                        SqlEditorWidget::current_output_settings(&session);
                                    let (colsep, null_text, _trimspool_enabled) =
                                        SqlEditorWidget::current_text_output_settings(&session);
                                    let cursor_label = format!("IMPLICIT RESULT {}", idx + 1);

                                    let cursor_result = QueryExecutor::execute_ref_cursor_streaming(
                                        &mut cursor,
                                        &cursor_label,
                                        &mut |columns| {
                                            let names = columns
                                                .iter()
                                                .map(|col| col.name.clone())
                                                .collect::<Vec<String>>();
                                            let display_columns =
                                                SqlEditorWidget::apply_heading_setting(
                                                    names,
                                                    heading_enabled,
                                                );
                                            let _ = sender.send(QueryProgress::SelectStart {
                                                index,
                                                columns: display_columns.clone(),
                                                null_text: null_text.clone(),
                                            });
                                            app::awake();
                                            if !display_columns.is_empty() {
                                                SqlEditorWidget::append_spool_output(
                                                    &session,
                                                    &[display_columns.join(&colsep)],
                                                );
                                            }
                                        },
                                        &mut |row| {
                                            if load_mutex_bool(&cancel_flag) {
                                                return false;
                                            }
                                            if let Some(timeout_duration) = query_timeout {
                                                if cursor_start.elapsed() >= timeout_duration {
                                                    cursor_timed_out = true;
                                                    let _ = conn.break_execution();
                                                    return false;
                                                }
                                            }
                                            let mut display_row = row;
                                            SqlEditorWidget::apply_null_text_to_row(
                                                &mut display_row,
                                                &null_text,
                                            );
                                            buffered_rows.push(display_row);
                                            if SqlEditorWidget::should_flush_progress_rows(
                                                last_flush,
                                                buffered_rows.len(),
                                                has_flushed_rows,
                                            ) {
                                                let rows = std::mem::take(&mut buffered_rows);
                                                SqlEditorWidget::append_spool_rows(&session, &rows);
                                                let _ = sender
                                                    .send(QueryProgress::Rows { index, rows });
                                                app::awake();
                                                last_flush = Instant::now();
                                                has_flushed_rows = true;
                                            }
                                            true
                                        },
                                    );

                                    match cursor_result {
                                        Ok((mut query_result, was_cancelled)) => {
                                            if cursor_timed_out {
                                                query_result.message =
                                                    SqlEditorWidget::timeout_message(query_timeout);
                                                query_result.success = false;
                                                cursor_timed_out = true;
                                            } else if was_cancelled {
                                                query_result.message =
                                                    SqlEditorWidget::cancel_message();
                                                query_result.success = false;
                                            }
                                            let interrupted = cursor_timed_out || was_cancelled;
                                            SqlEditorWidget::flush_buffered_rows(
                                                &sender,
                                                &session,
                                                index,
                                                &mut buffered_rows,
                                                interrupted,
                                            );
                                            SqlEditorWidget::apply_heading_to_result(
                                                &mut query_result,
                                                heading_enabled,
                                            );
                                            if !feedback_enabled {
                                                query_result.message.clear();
                                            }

                                            // Spool output before sending to avoid
                                            // cloning the message string a second time.
                                            if !query_result.message.trim().is_empty() {
                                                SqlEditorWidget::append_spool_output(
                                                    &session,
                                                    std::slice::from_ref(&query_result.message),
                                                );
                                            }
                                            let cursor_success = query_result.success;
                                            let _ = sender.send(QueryProgress::StatementFinished {
                                                index,
                                                result: query_result,
                                                connection_name: conn_name.clone(),
                                                timed_out: cursor_timed_out,
                                            });
                                            app::awake();
                                            result_index += 1;

                                            if cursor_timed_out {
                                                stop_execution = true;
                                                break;
                                            }
                                            if !cursor_success && !continue_on_error {
                                                stop_execution = true;
                                                break;
                                            }
                                        }
                                        Err(err) => {
                                            let cancelled = SqlEditorWidget::is_cancel_error(&err);
                                            cursor_timed_out =
                                                SqlEditorWidget::is_timeout_error(&err);
                                            let message =
                                                SqlEditorWidget::choose_execution_error_message(
                                                    cancelled,
                                                    cursor_timed_out,
                                                    query_timeout,
                                                    err.to_string(),
                                                );
                                            SqlEditorWidget::append_spool_output(
                                                &session,
                                                std::slice::from_ref(&message),
                                            );
                                            let _ = sender.send(QueryProgress::StatementFinished {
                                                index,
                                                result: QueryResult::new_error(
                                                    &cursor_label,
                                                    &message,
                                                ),
                                                connection_name: conn_name.clone(),
                                                timed_out: cursor_timed_out,
                                            });
                                            app::awake();
                                            result_index += 1;

                                            if cursor_timed_out || cancelled || !continue_on_error {
                                                stop_execution = true;
                                                break;
                                            }
                                        }
                                    }
                                }

                                let cancel_requested = load_mutex_bool(&cancel_flag);
                                let should_stop_after_statement = stop_execution
                                    || cancel_requested
                                    || timed_out
                                    || (!result_success && !continue_on_error);
                                if SqlEditorWidget::should_capture_post_execution_output(
                                    cancel_requested,
                                    timed_out,
                                    should_stop_after_statement,
                                ) {
                                    let _ = SqlEditorWidget::emit_dbms_output(
                                        &sender,
                                        &conn_name,
                                        conn.as_ref(),
                                        &session,
                                        &mut result_index,
                                    );
                                }
                                SqlEditorWidget::emit_timing_if_enabled(
                                    &sender,
                                    &session,
                                    timing_duration,
                                );

                                if should_stop_after_statement {
                                    stop_execution = true;
                                }
                            } else if is_select {
                                let sql_to_execute =
                                    sql_text.trim_end_matches(';').trim().to_string();
                                let binds = match session.lock() {
                                    Ok(guard) => {
                                        QueryExecutor::resolve_binds(&sql_to_execute, &guard)
                                    }
                                    Err(poisoned) => {
                                        eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                        QueryExecutor::resolve_binds(
                                            &sql_to_execute,
                                            &poisoned.into_inner(),
                                        )
                                    }
                                };

                                let binds = match binds {
                                    Ok(binds) => binds,
                                    Err(message) => {
                                        let emitted = SqlEditorWidget::emit_non_select_result(
                                            &sender,
                                            &session,
                                            &conn_name,
                                            result_index,
                                            &sql_text,
                                            format!("Error: {}", message),
                                            false,
                                            false,
                                            script_mode,
                                        );
                                        if emitted {
                                            result_index += 1;
                                        }
                                        if !continue_on_error {
                                            stop_execution = true;
                                        }
                                        continue;
                                    }
                                };

                                let index = result_index;
                                let _ = sender.send(QueryProgress::StatementStart { index });
                                app::awake();

                                let (heading_enabled, feedback_enabled) =
                                    SqlEditorWidget::current_output_settings(&session);
                                let mut buffered_rows: Vec<Vec<String>> = Vec::new();
                                let mut select_column_names: Vec<String> = Vec::new();
                                let select_column_count = std::cell::Cell::new(0usize);
                                let mut last_select_row: Option<Vec<String>> = None;
                                let mut last_flush = Instant::now();
                                let mut has_flushed_rows = false;
                                let statement_start = Instant::now();
                                let mut timed_out = false;
                                let mut statement_interrupted = false;
                                let (colsep, null_text, _trimspool_enabled) =
                                    SqlEditorWidget::current_text_output_settings(&session);
                                let (break_column, compute_config) = match session.lock() {
                                    Ok(guard) => {
                                        (guard.break_column.clone(), guard.compute.clone())
                                    }
                                    Err(poisoned) => {
                                        eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                        let guard = poisoned.into_inner();
                                        (guard.break_column.clone(), guard.compute.clone())
                                    }
                                };
                                let transform_state =
                                    std::sync::Mutex::new(SelectTransformState::default());

                                let result =
                                    match QueryExecutor::execute_select_streaming_with_binds(
                                        conn.as_ref(),
                                        &sql_to_execute,
                                        &binds,
                                        &mut |columns| {
                                            let names = columns
                                                .iter()
                                                .map(|col| col.name.clone())
                                                .collect::<Vec<String>>();
                                            select_column_names = names.clone();
                                            select_column_count.set(names.len());
                                            {
                                                let mut state =
                                                    transform_state.lock().unwrap_or_else(
                                                        |poisoned| poisoned.into_inner(),
                                                    );
                                                state.break_index =
                                                    break_column.as_ref().and_then(|target| {
                                                        let target_key =
                                                            SessionState::normalize_name(target);
                                                        names.iter().position(|column_name| {
                                                            SessionState::normalize_name(
                                                                column_name,
                                                            ) == target_key
                                                        })
                                                    });
                                                state.compute_of_index =
                                                    compute_config
                                                        .as_ref()
                                                        .and_then(|config| {
                                                            config.of_column.as_ref().and_then(
                                                                |target| {
                                                                    let target_key =
                                                                        SessionState::normalize_name(
                                                                            target,
                                                                        );
                                                                    names.iter().position(
                                                                        |column_name| {
                                                                            SessionState::normalize_name(column_name)
                                                                                == target_key
                                                                        },
                                                                    )
                                                                },
                                                            )
                                                        });
                                                state.compute_on_index =
                                                    compute_config
                                                        .as_ref()
                                                        .and_then(|config| {
                                                            config.on_column.as_ref().and_then(
                                                                |target| {
                                                                    let target_key =
                                                                        SessionState::normalize_name(
                                                                            target,
                                                                        );
                                                                    names.iter().position(
                                                                        |column_name| {
                                                                            SessionState::normalize_name(column_name)
                                                                                == target_key
                                                                        },
                                                                    )
                                                                },
                                                            )
                                                        });
                                                if compute_config
                                                    .as_ref()
                                                    .map(|config| {
                                                        config.mode == crate::db::ComputeMode::Sum
                                                            && config.of_column.is_none()
                                                    })
                                                    .unwrap_or(false)
                                                {
                                                    state.compute_sums = vec![0.0; names.len()];
                                                    state.compute_seen_numeric =
                                                        vec![false; names.len()];
                                                }
                                            }
                                            let display_columns =
                                                SqlEditorWidget::apply_heading_setting(
                                                    names,
                                                    heading_enabled,
                                                );
                                            let _ = sender.send(QueryProgress::SelectStart {
                                                index,
                                                columns: display_columns.clone(),
                                                null_text: null_text.clone(),
                                            });
                                            app::awake();
                                            if !display_columns.is_empty() {
                                                SqlEditorWidget::append_spool_output(
                                                    &session,
                                                    &[display_columns.join(&colsep)],
                                                );
                                            }
                                        },
                                        &mut |row| {
                                            if load_mutex_bool(&cancel_flag) {
                                                return false;
                                            }
                                            if let Some(timeout_duration) = query_timeout {
                                                if statement_start.elapsed() >= timeout_duration {
                                                    timed_out = true;
                                                    let _ = conn.break_execution();
                                                    return false;
                                                }
                                            }

                                            let mut row = row;
                                            last_select_row = Some(row.clone());
                                            {
                                                let mut state =
                                                    transform_state.lock().unwrap_or_else(
                                                        |poisoned| poisoned.into_inner(),
                                                    );
                                                if let Some(config) = compute_config.as_ref() {
                                                    let grouped_compute =
                                                        config.of_column.is_some()
                                                            && config.on_column.is_some()
                                                            && state.compute_of_index.is_some()
                                                            && state.compute_on_index.is_some();
                                                    if grouped_compute {
                                                        if let Some(on_idx) = state.compute_on_index
                                                        {
                                                            if let Some(current_group_value) =
                                                                row.get(on_idx).cloned()
                                                            {
                                                                if let Some(previous_group_value) =
                                                                    state
                                                                        .compute_group_value
                                                                        .clone()
                                                                {
                                                                    if previous_group_value
                                                                        != current_group_value
                                                                    {
                                                                        if let Some(summary_row) =
                                                                            SqlEditorWidget::build_compute_summary_row(
                                                                                select_column_count.get(),
                                                                                Some(config),
                                                                                &state,
                                                                            )
                                                                        {
                                                                            let mut display_summary =
                                                                                summary_row;
                                                                            SqlEditorWidget::apply_null_text_to_row(
                                                                                &mut display_summary,
                                                                                &null_text,
                                                                            );
                                                                            buffered_rows
                                                                                .push(display_summary);
                                                                        }
                                                                        state.compute_count = 0;
                                                                        state.compute_sum = 0.0;
                                                                        state.compute_sum_seen =
                                                                            false;
                                                                    }
                                                                }
                                                                state.compute_group_value =
                                                                    Some(current_group_value);
                                                            }
                                                        }
                                                    }
                                                    SqlEditorWidget::accumulate_compute(
                                                        config, &row, &mut state,
                                                    );
                                                }
                                                if let Some(idx) = state.break_index {
                                                    if let Some(current_break_value) =
                                                        row.get(idx).cloned()
                                                    {
                                                        if state
                                                            .previous_break_value
                                                            .as_ref()
                                                            .map(|prev| {
                                                                prev == &current_break_value
                                                            })
                                                            .unwrap_or(false)
                                                        {
                                                            if let Some(cell) = row.get_mut(idx) {
                                                                *cell = String::new();
                                                            }
                                                        } else {
                                                            state.previous_break_value =
                                                                Some(current_break_value);
                                                        }
                                                    }
                                                }
                                            }
                                            SqlEditorWidget::apply_null_text_to_row(
                                                &mut row, &null_text,
                                            );
                                            buffered_rows.push(row);
                                            if SqlEditorWidget::should_flush_progress_rows(
                                                last_flush,
                                                buffered_rows.len(),
                                                has_flushed_rows,
                                            ) {
                                                let rows = std::mem::take(&mut buffered_rows);
                                                SqlEditorWidget::append_spool_rows(&session, &rows);
                                                let _ = sender
                                                    .send(QueryProgress::Rows { index, rows });
                                                app::awake();
                                                last_flush = Instant::now();
                                                has_flushed_rows = true;
                                            }
                                            true
                                        },
                                    ) {
                                        Ok((mut query_result, was_cancelled)) => {
                                            SqlEditorWidget::apply_heading_to_result(
                                                &mut query_result,
                                                heading_enabled,
                                            );
                                            if timed_out {
                                                query_result.message =
                                                    SqlEditorWidget::timeout_message(query_timeout);
                                                query_result.success = false;
                                                timed_out = true;
                                                statement_interrupted = true;
                                            } else if was_cancelled {
                                                query_result.message =
                                                    SqlEditorWidget::cancel_message();
                                                query_result.success = false;
                                                statement_interrupted = true;
                                            }
                                            if !feedback_enabled {
                                                query_result.message.clear();
                                            }
                                            if !query_result.message.trim().is_empty() {
                                                SqlEditorWidget::append_spool_output(
                                                    &session,
                                                    std::slice::from_ref(&query_result.message),
                                                );
                                            }
                                            query_result
                                        }
                                        Err(err) => {
                                            let cancelled = SqlEditorWidget::is_cancel_error(&err);
                                            timed_out = SqlEditorWidget::is_timeout_error(&err);
                                            statement_interrupted = timed_out || cancelled;
                                            let message =
                                                SqlEditorWidget::choose_execution_error_message(
                                                    cancelled,
                                                    timed_out,
                                                    query_timeout,
                                                    err.to_string(),
                                                );
                                            let mut error_result =
                                                QueryResult::new_error(&sql_text, &message);
                                            // Preserve is_select flag so existing streamed data is kept
                                            error_result.is_select = true;
                                            error_result
                                        }
                                    };
                                let transform_state = match transform_state.into_inner() {
                                    Ok(state) => state,
                                    Err(poisoned) => {
                                        eprintln!(
                                            "Warning: transform state lock was poisoned; recovering."
                                        );
                                        poisoned.into_inner()
                                    }
                                };

                                SqlEditorWidget::flush_buffered_rows(
                                    &sender,
                                    &session,
                                    index,
                                    &mut buffered_rows,
                                    statement_interrupted,
                                );

                                if !result.message.trim().is_empty() {
                                    SqlEditorWidget::append_spool_output(
                                        &session,
                                        std::slice::from_ref(&result.message),
                                    );
                                }
                                if result.success {
                                    let grouped_compute = compute_config
                                        .as_ref()
                                        .map(|config| {
                                            config.of_column.is_some()
                                                && config.on_column.is_some()
                                                && transform_state.compute_of_index.is_some()
                                                && transform_state.compute_on_index.is_some()
                                        })
                                        .unwrap_or(false);
                                    if grouped_compute {
                                        if transform_state.compute_group_value.is_some() {
                                            if let Some(summary_row) =
                                                SqlEditorWidget::build_compute_summary_row(
                                                    select_column_names.len(),
                                                    compute_config.as_ref(),
                                                    &transform_state,
                                                )
                                            {
                                                let rows = vec![summary_row];
                                                SqlEditorWidget::append_spool_rows(&session, &rows);
                                                let _ = sender
                                                    .send(QueryProgress::Rows { index, rows });
                                                app::awake();
                                            }
                                        }
                                    } else if let Some(summary_row) =
                                        SqlEditorWidget::build_compute_summary_row(
                                            select_column_names.len(),
                                            compute_config.as_ref(),
                                            &transform_state,
                                        )
                                    {
                                        let rows = vec![summary_row];
                                        SqlEditorWidget::append_spool_rows(&session, &rows);
                                        let _ = sender.send(QueryProgress::Rows { index, rows });
                                        app::awake();
                                    }
                                    SqlEditorWidget::apply_column_new_value_from_row(
                                        &session,
                                        &select_column_names,
                                        last_select_row.as_deref(),
                                    );
                                }
                                let timing_duration = if result.execution_time.is_zero() {
                                    statement_start.elapsed()
                                } else {
                                    result.execution_time
                                };
                                let result_success = result.success;
                                let _ = sender.send(QueryProgress::StatementFinished {
                                    index,
                                    result,
                                    connection_name: conn_name.clone(),
                                    timed_out,
                                });
                                app::awake();
                                result_index += 1;

                                let cancel_requested = load_mutex_bool(&cancel_flag);
                                let should_stop_after_statement = cancel_requested
                                    || timed_out
                                    || (!result_success && !continue_on_error);
                                let skip_post_execution_output =
                                    should_stop_after_statement || statement_interrupted;
                                if SqlEditorWidget::should_capture_post_execution_output(
                                    cancel_requested,
                                    timed_out,
                                    skip_post_execution_output,
                                ) {
                                    let _ = SqlEditorWidget::emit_dbms_output(
                                        &sender,
                                        &conn_name,
                                        conn.as_ref(),
                                        &session,
                                        &mut result_index,
                                    );
                                }
                                SqlEditorWidget::emit_timing_if_enabled(
                                    &sender,
                                    &session,
                                    timing_duration,
                                );

                                if should_stop_after_statement {
                                    stop_execution = true;
                                }
                            } else {
                                let sql_to_execute = if is_compiled_plsql {
                                    SqlEditorWidget::ensure_plsql_terminator(&sql_text)
                                } else {
                                    sql_text.trim_end_matches(';').trim().to_string()
                                };
                                let binds = match session.lock() {
                                    Ok(guard) => {
                                        QueryExecutor::resolve_binds(&sql_to_execute, &guard)
                                    }
                                    Err(poisoned) => {
                                        eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                        QueryExecutor::resolve_binds(
                                            &sql_to_execute,
                                            &poisoned.into_inner(),
                                        )
                                    }
                                };

                                let binds = match binds {
                                    Ok(binds) => binds,
                                    Err(message) => {
                                        let emitted = SqlEditorWidget::emit_non_select_result(
                                            &sender,
                                            &session,
                                            &conn_name,
                                            result_index,
                                            &sql_text,
                                            format!("Error: {}", message),
                                            false,
                                            false,
                                            script_mode,
                                        );
                                        if emitted {
                                            result_index += 1;
                                        }
                                        if !continue_on_error {
                                            stop_execution = true;
                                        }
                                        continue;
                                    }
                                };

                                let statement_start = Instant::now();
                                let mut timed_out = false;
                                let stmt = match QueryExecutor::execute_with_binds(
                                    conn.as_ref(),
                                    &sql_to_execute,
                                    &binds,
                                ) {
                                    Ok(stmt) => stmt,
                                    Err(err) => {
                                        let cancelled = SqlEditorWidget::is_cancel_error(&err);
                                        timed_out = SqlEditorWidget::is_timeout_error(&err);
                                        let message =
                                            SqlEditorWidget::choose_execution_error_message(
                                                cancelled,
                                                timed_out,
                                                query_timeout,
                                                err.to_string(),
                                            );
                                        if script_mode {
                                            let result =
                                                QueryResult::new_error(&sql_text, &message);
                                            SqlEditorWidget::emit_script_result(
                                                &sender,
                                                &conn_name,
                                                result_index,
                                                result,
                                                timed_out,
                                            );
                                        } else {
                                            let index = result_index;
                                            let _ = sender
                                                .send(QueryProgress::StatementStart { index });
                                            app::awake();
                                            let result =
                                                QueryResult::new_error(&sql_text, &message);
                                            let _ = sender.send(QueryProgress::StatementFinished {
                                                index,
                                                result,
                                                connection_name: conn_name.clone(),
                                                timed_out,
                                            });
                                            app::awake();
                                            result_index += 1;
                                        }
                                        SqlEditorWidget::emit_timing_if_enabled(
                                            &sender,
                                            &session,
                                            statement_start.elapsed(),
                                        );
                                        if timed_out || cancelled || !continue_on_error {
                                            stop_execution = true;
                                        }
                                        continue;
                                    }
                                };

                                let execution_time = statement_start.elapsed();
                                let timing_duration = execution_time;
                                let dml_type = if upper.starts_with("INSERT") {
                                    Some("INSERT")
                                } else if upper.starts_with("UPDATE") {
                                    Some("UPDATE")
                                } else if upper.starts_with("DELETE") {
                                    Some("DELETE")
                                } else if upper.starts_with("MERGE") {
                                    Some("MERGE")
                                } else {
                                    None
                                };

                                let mut result = if let Some(statement_type) = dml_type {
                                    let affected_rows = stmt.row_count().unwrap_or(0);
                                    QueryResult::new_dml(
                                        &sql_text,
                                        affected_rows,
                                        execution_time,
                                        statement_type,
                                    )
                                } else {
                                    QueryResult {
                                        sql: sql_text.to_string(),
                                        columns: vec![],
                                        rows: vec![],
                                        row_count: 0,
                                        execution_time,
                                        message: if upper.starts_with("CREATE")
                                            || upper.starts_with("ALTER")
                                            || upper.starts_with("DROP")
                                            || upper.starts_with("TRUNCATE")
                                            || upper.starts_with("RENAME")
                                            || upper.starts_with("GRANT")
                                            || upper.starts_with("REVOKE")
                                            || upper.starts_with("COMMENT")
                                        {
                                            SqlEditorWidget::ddl_message(&upper)
                                        } else {
                                            "Statement executed successfully".to_string()
                                        },
                                        is_select: false,
                                        success: true,
                                    }
                                };

                                if load_mutex_bool(&cancel_flag) {
                                    stop_execution = true;
                                    continue;
                                }

                                let mut out_messages: Vec<String> = Vec::new();
                                let (_colsep, null_text, _trimspool_enabled) =
                                    SqlEditorWidget::current_text_output_settings(&session);
                                if let Ok(updates) =
                                    QueryExecutor::fetch_scalar_bind_updates(&stmt, &binds)
                                {
                                    let mut guard = match session.lock() {
                                        Ok(guard) => guard,
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            poisoned.into_inner()
                                        }
                                    };
                                    for (name, value) in updates {
                                        if let Some(bind) = guard.binds.get_mut(&name) {
                                            bind.value = value.clone();
                                        }
                                        if let BindValue::Scalar(val) = value {
                                            out_messages.push(format!(
                                                ":{} = {}",
                                                name,
                                                val.unwrap_or_else(|| null_text.clone())
                                            ));
                                        }
                                    }
                                }

                                if !out_messages.is_empty() {
                                    result.message = format!(
                                        "{} | OUT: {}",
                                        result.message,
                                        out_messages.join(", ")
                                    );
                                }

                                if load_mutex_bool(&cancel_flag) {
                                    stop_execution = true;
                                    continue;
                                }

                                let mut compile_errors: Option<Vec<Vec<String>>> = None;
                                if let Some(object) = compiled_object.clone() {
                                    match QueryExecutor::fetch_compilation_errors(
                                        conn.as_ref(),
                                        &object,
                                    ) {
                                        Ok(rows) => {
                                            if !rows.is_empty() {
                                                result.message = format!(
                                                    "{} | Compiled with errors",
                                                    result.message
                                                );
                                                result.success = false;
                                                compile_errors = Some(rows);
                                            }
                                        }
                                        Err(err) => {
                                            result.message = format!(
                                                "{} | Failed to fetch compilation errors: {}",
                                                result.message, err
                                            );
                                            result.success = false;
                                        }
                                    }
                                }

                                if dml_type.is_some() && !auto_commit && result.success {
                                    result.message =
                                        format!("{} | Commit required", result.message);
                                }

                                if load_mutex_bool(&cancel_flag) {
                                    stop_execution = true;
                                    continue;
                                }

                                if auto_commit && result.success {
                                    if let Err(err) = conn.commit() {
                                        result = QueryResult::new_error(
                                            &sql_text,
                                            &format!("Auto-commit failed: {}", err),
                                        );
                                    } else {
                                        result.message =
                                            format!("{} | Auto-commit applied", result.message);
                                    }
                                }

                                // Capture success before moving result into the channel
                                // to avoid cloning the entire QueryResult.
                                let result_success = result.success;
                                if script_mode {
                                    if result_success {
                                        SqlEditorWidget::emit_script_lines(
                                            &sender,
                                            &session,
                                            &result.message,
                                        );
                                    }
                                    SqlEditorWidget::emit_script_result(
                                        &sender,
                                        &conn_name,
                                        result_index,
                                        result,
                                        timed_out,
                                    );
                                } else {
                                    let index = result_index;
                                    let _ = sender.send(QueryProgress::StatementStart { index });
                                    app::awake();
                                    if !result.message.trim().is_empty() {
                                        SqlEditorWidget::append_spool_output(
                                            &session,
                                            std::slice::from_ref(&result.message),
                                        );
                                    }
                                    let _ = sender.send(QueryProgress::StatementFinished {
                                        index,
                                        result,
                                        connection_name: conn_name.clone(),
                                        timed_out,
                                    });
                                    app::awake();
                                    result_index += 1;
                                }

                                if let Some(rows) = compile_errors {
                                    let (heading_enabled, feedback_enabled) =
                                        SqlEditorWidget::current_output_settings(&session);
                                    SqlEditorWidget::emit_select_result(
                                        &sender,
                                        &session,
                                        &conn_name,
                                        result_index,
                                        "COMPILE ERRORS",
                                        SqlEditorWidget::apply_heading_setting(
                                            vec![
                                                "LINE".to_string(),
                                                "POSITION".to_string(),
                                                "TEXT".to_string(),
                                            ],
                                            heading_enabled,
                                        ),
                                        rows,
                                        false,
                                        feedback_enabled,
                                    );
                                    result_index += 1;
                                }

                                let cancel_requested = load_mutex_bool(&cancel_flag);
                                let should_stop_after_statement = cancel_requested
                                    || timed_out
                                    || (!result_success && !continue_on_error);
                                if SqlEditorWidget::should_capture_post_execution_output(
                                    cancel_requested,
                                    timed_out,
                                    should_stop_after_statement,
                                ) {
                                    let _ = SqlEditorWidget::emit_dbms_output(
                                        &sender,
                                        &conn_name,
                                        conn.as_ref(),
                                        &session,
                                        &mut result_index,
                                    );
                                }
                                SqlEditorWidget::emit_timing_if_enabled(
                                    &sender,
                                    &session,
                                    timing_duration,
                                );

                                if should_stop_after_statement {
                                    stop_execution = true;
                                }
                            }
                        }
                    }
                }
            })); // end catch_unwind

            if let Err(e) = result {
                let panic_payload = SqlEditorWidget::panic_payload_to_string(e.as_ref());
                crate::utils::logging::log_error(
                    "sql_editor::execution",
                    &format!("Query worker thread panicked: {panic_payload}"),
                );
                let _ = sender.send(QueryProgress::WorkerPanicked {
                    message: format!("Query execution thread panicked: {panic_payload}"),
                });
                app::awake();
                eprintln!("Query thread panicked: {panic_payload}");
            }
        });
    }

    fn emit_non_select_result(
        sender: &mpsc::Sender<QueryProgress>,
        session: &Arc<Mutex<SessionState>>,
        conn_name: &str,
        index: usize,
        sql: &str,
        message: String,
        success: bool,
        timed_out: bool,
        script_mode: bool,
    ) -> bool {
        if script_mode {
            if success {
                SqlEditorWidget::emit_script_lines(sender, session, &message);
            }
            let result = QueryResult {
                sql: sql.to_string(),
                columns: vec![],
                rows: vec![],
                row_count: 0,
                execution_time: Duration::from_secs(0),
                message,
                is_select: false,
                success,
            };
            SqlEditorWidget::emit_script_result(sender, conn_name, index, result, timed_out);
            return false;
        }

        let _ = sender.send(QueryProgress::StatementStart { index });
        app::awake();
        if !message.trim().is_empty() {
            SqlEditorWidget::append_spool_output(session, std::slice::from_ref(&message));
        }
        let result = QueryResult {
            sql: sql.to_string(),
            columns: vec![],
            rows: vec![],
            row_count: 0,
            execution_time: Duration::from_secs(0),
            message,
            is_select: false,
            success,
        };
        let _ = sender.send(QueryProgress::StatementFinished {
            index,
            result,
            connection_name: conn_name.to_string(),
            timed_out,
        });
        app::awake();
        true
    }

    fn emit_script_result(
        sender: &mpsc::Sender<QueryProgress>,
        conn_name: &str,
        index: usize,
        result: QueryResult,
        timed_out: bool,
    ) {
        let _ = sender.send(QueryProgress::StatementFinished {
            index,
            result,
            connection_name: conn_name.to_string(),
            timed_out,
        });
        app::awake();
    }

    fn apply_column_new_value_from_row(
        session: &Arc<Mutex<SessionState>>,
        column_names: &[String],
        row: Option<&[String]>,
    ) {
        let Some(row_values) = row else {
            return;
        };

        match session.lock() {
            Ok(mut guard) => {
                if guard.column_new_values.is_empty() {
                    return;
                }
                for (idx, column_name) in column_names.iter().enumerate() {
                    let column_key = SessionState::normalize_name(column_name);
                    let Some(variable_key) = guard.column_new_values.get(&column_key).cloned()
                    else {
                        continue;
                    };
                    let Some(value) = row_values.get(idx).cloned() else {
                        continue;
                    };
                    guard.define_vars.insert(variable_key, value);
                }
            }
            Err(poisoned) => {
                eprintln!("Warning: session state lock was poisoned; recovering.");
                let mut guard = poisoned.into_inner();
                if guard.column_new_values.is_empty() {
                    return;
                }
                for (idx, column_name) in column_names.iter().enumerate() {
                    let column_key = SessionState::normalize_name(column_name);
                    let Some(variable_key) = guard.column_new_values.get(&column_key).cloned()
                    else {
                        continue;
                    };
                    let Some(value) = row_values.get(idx).cloned() else {
                        continue;
                    };
                    guard.define_vars.insert(variable_key, value);
                }
            }
        }
    }

    fn current_output_settings(session: &Arc<Mutex<SessionState>>) -> (bool, bool) {
        match session.lock() {
            Ok(guard) => (guard.heading_enabled, guard.feedback_enabled),
            Err(poisoned) => {
                eprintln!("Warning: session state lock was poisoned; recovering.");
                let guard = poisoned.into_inner();
                (guard.heading_enabled, guard.feedback_enabled)
            }
        }
    }

    fn current_text_output_settings(session: &Arc<Mutex<SessionState>>) -> (String, String, bool) {
        match session.lock() {
            Ok(guard) => (
                guard.colsep.clone(),
                guard.null_text.clone(),
                guard.trimspool_enabled,
            ),
            Err(poisoned) => {
                eprintln!("Warning: session state lock was poisoned; recovering.");
                let guard = poisoned.into_inner();
                (
                    guard.colsep.clone(),
                    guard.null_text.clone(),
                    guard.trimspool_enabled,
                )
            }
        }
    }

    fn current_script_output_settings(session: &Arc<Mutex<SessionState>>) -> (bool, bool) {
        match session.lock() {
            Ok(guard) => (guard.trimout_enabled, guard.tab_enabled),
            Err(poisoned) => {
                eprintln!("Warning: session state lock was poisoned; recovering.");
                let guard = poisoned.into_inner();
                (guard.trimout_enabled, guard.tab_enabled)
            }
        }
    }

    fn clone_print_named_data(session: &SessionState, key: &str) -> PrintNamedData {
        match session.binds.get(key) {
            Some(bind) => match &bind.value {
                BindValue::Scalar(value) => PrintNamedData::Scalar(value.clone()),
                BindValue::Cursor(Some(cursor_result)) => {
                    PrintNamedData::Cursor(cursor_result.clone())
                }
                BindValue::Cursor(None) => PrintNamedData::CursorEmpty,
            },
            None => PrintNamedData::Missing,
        }
    }

    fn collect_print_all_data(
        session: &SessionState,
        null_text: &str,
    ) -> (Vec<Vec<String>>, Vec<(String, CursorResult)>) {
        let mut summary_rows = Vec::new();
        let mut cursor_results = Vec::new();

        for (bind_name, bind) in &session.binds {
            let value_display = match &bind.value {
                BindValue::Scalar(value) => value.clone().unwrap_or_else(|| null_text.to_string()),
                BindValue::Cursor(Some(cursor_result)) => {
                    let row_count = cursor_result.rows.len();
                    cursor_results.push((bind_name.clone(), cursor_result.clone()));
                    format!("REFCURSOR ({} rows)", row_count)
                }
                BindValue::Cursor(None) => "REFCURSOR (empty)".to_string(),
            };

            summary_rows.push(vec![
                bind_name.clone(),
                bind.data_type.display(),
                value_display,
            ]);
        }

        (summary_rows, cursor_results)
    }

    fn cursor_result_column_names(columns: &[ColumnInfo]) -> Vec<String> {
        columns.iter().map(|column| column.name.clone()).collect()
    }

    fn has_spool_target(session: &Arc<Mutex<SessionState>>) -> bool {
        match session.lock() {
            Ok(guard) => guard.spool_path.is_some(),
            Err(poisoned) => {
                eprintln!("Warning: session state lock was poisoned; recovering.");
                poisoned.into_inner().spool_path.is_some()
            }
        }
    }

    fn expand_tabs(text: &str) -> String {
        const TAB_STOP: usize = 8;
        let mut out = String::with_capacity(text.len());
        let mut col = 0usize;

        for ch in text.chars() {
            if ch == '\t' {
                let spaces = TAB_STOP - (col % TAB_STOP);
                for _ in 0..spaces {
                    out.push(' ');
                }
                col += spaces;
            } else {
                out.push(ch);
                col += 1;
            }
        }

        out
    }

    fn format_script_output_line(line: &str, trimout_enabled: bool, tab_enabled: bool) -> String {
        let mut rendered = if tab_enabled {
            SqlEditorWidget::expand_tabs(line)
        } else {
            line.to_string()
        };

        if trimout_enabled {
            rendered = rendered.trim_end().to_string();
        }

        rendered
    }

    fn display_cell_value(value: &str, null_text: &str) -> String {
        if value == "NULL" {
            null_text.to_string()
        } else {
            value.to_string()
        }
    }

    fn display_row_values(row: &[String], null_text: &str) -> Vec<String> {
        row.iter()
            .map(|value| SqlEditorWidget::display_cell_value(value, null_text))
            .collect()
    }

    fn apply_null_text_to_row(row: &mut [String], null_text: &str) {
        if null_text == "NULL" {
            return;
        }
        for value in row.iter_mut() {
            if value == "NULL" {
                value.clear();
                value.push_str(null_text);
            }
        }
    }

    fn parse_numeric_value(value: &str) -> Option<f64> {
        let trimmed = value.trim();
        if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("NULL") {
            return None;
        }
        let normalized = trimmed.replace(',', "");
        normalized.parse::<f64>().ok()
    }

    fn format_number(value: f64) -> String {
        let mut text = format!("{}", value);
        if text.contains('.') {
            while text.ends_with('0') {
                text.pop();
            }
            if text.ends_with('.') {
                text.pop();
            }
        }
        text
    }

    fn accumulate_compute(
        config: &crate::db::ComputeConfig,
        row: &[String],
        state: &mut SelectTransformState,
    ) {
        match config.mode {
            crate::db::ComputeMode::Count => {
                if let Some(of_idx) = state.compute_of_index {
                    let is_not_null = row
                        .get(of_idx)
                        .map(|value| !value.eq_ignore_ascii_case("NULL"))
                        .unwrap_or(false);
                    if is_not_null {
                        state.compute_count += 1;
                    }
                } else {
                    state.compute_count += 1;
                }
            }
            crate::db::ComputeMode::Sum => {
                if let Some(of_idx) = state.compute_of_index {
                    if let Some(number) = row
                        .get(of_idx)
                        .and_then(|value| SqlEditorWidget::parse_numeric_value(value))
                    {
                        state.compute_sum += number;
                        state.compute_sum_seen = true;
                    }
                } else {
                    for (idx, value) in row.iter().enumerate() {
                        if let Some(number) = SqlEditorWidget::parse_numeric_value(value) {
                            if let Some(sum_slot) = state.compute_sums.get_mut(idx) {
                                *sum_slot += number;
                            }
                            if let Some(seen_slot) = state.compute_seen_numeric.get_mut(idx) {
                                *seen_slot = true;
                            }
                        }
                    }
                }
            }
        }
    }

    fn build_compute_summary_row(
        column_count: usize,
        compute_config: Option<&crate::db::ComputeConfig>,
        state: &SelectTransformState,
    ) -> Option<Vec<String>> {
        let config = compute_config?;
        let mode = config.mode;
        if column_count == 0 {
            return None;
        }

        let mut row = vec![String::new(); column_count];
        if let (Some(of_idx), Some(on_idx)) = (state.compute_of_index, state.compute_on_index) {
            let label = match mode {
                crate::db::ComputeMode::Count => "COUNT",
                crate::db::ComputeMode::Sum => "SUM",
            };
            if on_idx < column_count {
                row[on_idx] = label.to_string();
            }
            if of_idx < column_count {
                row[of_idx] = match mode {
                    crate::db::ComputeMode::Count => state.compute_count.to_string(),
                    crate::db::ComputeMode::Sum => {
                        if state.compute_sum_seen {
                            SqlEditorWidget::format_number(state.compute_sum)
                        } else {
                            "0".to_string()
                        }
                    }
                };
            }
            return Some(row);
        }

        match mode {
            crate::db::ComputeMode::Count => {
                if column_count == 1 {
                    row[0] = state.compute_count.to_string();
                } else {
                    row[0] = "COUNT".to_string();
                    row[column_count - 1] = state.compute_count.to_string();
                }
            }
            crate::db::ComputeMode::Sum => {
                if let Some(of_idx) = state.compute_of_index {
                    if column_count == 1 {
                        row[0] = if state.compute_sum_seen {
                            SqlEditorWidget::format_number(state.compute_sum)
                        } else {
                            "0".to_string()
                        };
                    } else {
                        row[0] = "SUM".to_string();
                        if of_idx < column_count {
                            row[of_idx] = if state.compute_sum_seen {
                                SqlEditorWidget::format_number(state.compute_sum)
                            } else {
                                "0".to_string()
                            };
                        }
                    }
                } else if column_count == 1 {
                    let total = state.compute_sums.first().copied().unwrap_or(0.0);
                    row[0] = SqlEditorWidget::format_number(total);
                } else {
                    row[0] = "SUM".to_string();
                    let mut has_any_numeric = false;
                    for (idx, cell) in row.iter_mut().enumerate().take(column_count).skip(1) {
                        if state
                            .compute_seen_numeric
                            .get(idx)
                            .copied()
                            .unwrap_or(false)
                        {
                            let total = state.compute_sums.get(idx).copied().unwrap_or(0.0);
                            *cell = SqlEditorWidget::format_number(total);
                            has_any_numeric = true;
                        }
                    }
                    if !has_any_numeric {
                        row[column_count - 1] = "0".to_string();
                    }
                }
            }
        }

        Some(row)
    }

    fn format_row_line(row: &[String], colsep: &str, null_text: &str) -> String {
        let display_row = SqlEditorWidget::display_row_values(row, null_text);
        display_row.join(colsep)
    }

    fn apply_heading_setting(column_names: Vec<String>, heading_enabled: bool) -> Vec<String> {
        if heading_enabled {
            column_names
        } else {
            column_names.into_iter().map(|_| String::new()).collect()
        }
    }

    fn apply_heading_to_result(result: &mut QueryResult, heading_enabled: bool) {
        if heading_enabled {
            return;
        }
        for column in &mut result.columns {
            column.name.clear();
        }
    }

    fn should_flush_progress_rows(
        last_flush: Instant,
        buffered_len: usize,
        has_flushed_rows: bool,
    ) -> bool {
        let row_threshold = if has_flushed_rows {
            PROGRESS_ROWS_MAX_BATCH
        } else {
            PROGRESS_ROWS_INITIAL_BATCH
        };

        buffered_len >= row_threshold || last_flush.elapsed() >= PROGRESS_ROWS_FLUSH_INTERVAL
    }

    fn should_capture_post_execution_output(
        cancel_requested: bool,
        timed_out: bool,
        stop_execution: bool,
    ) -> bool {
        !cancel_requested && !timed_out && !stop_execution
    }

    fn flush_buffered_rows(
        sender: &mpsc::Sender<QueryProgress>,
        session: &Arc<Mutex<SessionState>>,
        index: usize,
        buffered_rows: &mut Vec<Vec<String>>,
        interrupted: bool,
    ) {
        if buffered_rows.is_empty() {
            return;
        }

        if interrupted {
            // Prefer immediate cancel/timeout completion over flushing a large
            // buffered tail that has not been rendered yet.
            buffered_rows.clear();
            return;
        }

        let rows = std::mem::take(buffered_rows);
        SqlEditorWidget::append_spool_rows(session, &rows);
        let _ = sender.send(QueryProgress::Rows { index, rows });
        app::awake();
    }

    fn flush_buffered_result_rows(
        sender: &mpsc::Sender<QueryProgress>,
        session: &Arc<Mutex<SessionState>>,
        index: usize,
        buffered_display_rows: &mut Vec<Vec<String>>,
        buffered_raw_rows: &mut Vec<Vec<String>>,
    ) {
        if buffered_display_rows.is_empty() {
            buffered_raw_rows.clear();
            return;
        }

        let rows = std::mem::take(buffered_display_rows);
        let raw_rows = std::mem::take(buffered_raw_rows);
        SqlEditorWidget::append_spool_rows(session, &raw_rows);
        let _ = sender.send(QueryProgress::Rows { index, rows });
        app::awake();
    }

    fn emit_select_result(
        sender: &mpsc::Sender<QueryProgress>,
        session: &Arc<Mutex<SessionState>>,
        conn_name: &str,
        index: usize,
        sql: &str,
        column_names: Vec<String>,
        rows: Vec<Vec<String>>,
        success: bool,
        feedback_enabled: bool,
    ) {
        let _ = sender.send(QueryProgress::StatementStart { index });
        app::awake();
        let (colsep, null_text, _trimspool_enabled) =
            SqlEditorWidget::current_text_output_settings(session);
        let _ = sender.send(QueryProgress::SelectStart {
            index,
            columns: column_names.clone(),
            null_text: null_text.clone(),
        });
        app::awake();
        if !column_names.is_empty() {
            SqlEditorWidget::append_spool_output(session, &[column_names.join(&colsep)]);
        }

        let mut row_count = 0usize;
        let mut buffered_display_rows: Vec<Vec<String>> = Vec::new();
        let mut buffered_raw_rows: Vec<Vec<String>> = Vec::new();
        let mut last_flush = Instant::now();
        let mut has_flushed_rows = false;
        if !rows.is_empty() {
            for row in rows {
                row_count += 1;
                buffered_display_rows.push(SqlEditorWidget::display_row_values(&row, &null_text));
                buffered_raw_rows.push(row);
                if SqlEditorWidget::should_flush_progress_rows(
                    last_flush,
                    buffered_display_rows.len(),
                    has_flushed_rows,
                ) {
                    SqlEditorWidget::flush_buffered_result_rows(
                        sender,
                        session,
                        index,
                        &mut buffered_display_rows,
                        &mut buffered_raw_rows,
                    );
                    last_flush = Instant::now();
                    has_flushed_rows = true;
                }
            }
            SqlEditorWidget::flush_buffered_result_rows(
                sender,
                session,
                index,
                &mut buffered_display_rows,
                &mut buffered_raw_rows,
            );
        }
        let column_info: Vec<ColumnInfo> = column_names
            .iter()
            .map(|name| ColumnInfo {
                name: name.clone(),
                data_type: "VARCHAR2".to_string(),
            })
            .collect();
        let mut result =
            QueryResult::new_select_streamed(sql, column_info, row_count, Duration::from_secs(0));
        result.success = success;
        if !feedback_enabled {
            result.message.clear();
        }
        if !result.message.trim().is_empty() {
            SqlEditorWidget::append_spool_output(session, std::slice::from_ref(&result.message));
        }
        let _ = sender.send(QueryProgress::StatementFinished {
            index,
            result,
            connection_name: conn_name.to_string(),
            timed_out: false,
        });
        app::awake();
    }

    fn emit_script_output(
        sender: &mpsc::Sender<QueryProgress>,
        session: &Arc<Mutex<SessionState>>,
        lines: Vec<String>,
    ) {
        if lines.is_empty() {
            return;
        }
        SqlEditorWidget::append_spool_output(session, &lines);
        let (trimout_enabled, tab_enabled) =
            SqlEditorWidget::current_script_output_settings(session);
        let display_lines: Vec<String> = lines
            .into_iter()
            .map(|line| {
                SqlEditorWidget::format_script_output_line(&line, trimout_enabled, tab_enabled)
            })
            .collect();
        let _ = sender.send(QueryProgress::ScriptOutput {
            lines: display_lines,
        });
        app::awake();
    }

    fn emit_timing_if_enabled(
        sender: &mpsc::Sender<QueryProgress>,
        session: &Arc<Mutex<SessionState>>,
        duration: Duration,
    ) {
        let enabled = match session.lock() {
            Ok(guard) => guard.timing_enabled,
            Err(poisoned) => {
                eprintln!("Warning: session state lock was poisoned; recovering.");
                poisoned.into_inner().timing_enabled
            }
        };
        if !enabled {
            return;
        }
        let line = format!("Elapsed: {:.3}s", duration.as_secs_f64());
        SqlEditorWidget::emit_script_output(sender, session, vec![line]);
    }

    fn emit_script_lines(
        sender: &mpsc::Sender<QueryProgress>,
        session: &Arc<Mutex<SessionState>>,
        message: &str,
    ) {
        let lines: Vec<String> = message.lines().map(|line| line.to_string()).collect();
        if lines.is_empty() {
            return;
        }
        SqlEditorWidget::emit_script_output(sender, session, lines);
    }

    fn emit_script_message(
        sender: &mpsc::Sender<QueryProgress>,
        session: &Arc<Mutex<SessionState>>,
        title: &str,
        message: &str,
    ) {
        let mut lines = Vec::new();
        lines.push(format!("[{}]", title));
        for line in message.lines() {
            lines.push(line.to_string());
        }
        SqlEditorWidget::emit_script_output(sender, session, lines);
    }

    fn append_spool_output(session: &Arc<Mutex<SessionState>>, lines: &[String]) {
        if lines.is_empty() {
            return;
        }

        let (path, truncate, trimspool_enabled) = match session.lock() {
            Ok(mut guard) => {
                let path = guard.spool_path.clone();
                let truncate = guard.spool_truncate;
                if truncate {
                    guard.spool_truncate = false;
                }
                (path, truncate, guard.trimspool_enabled)
            }
            Err(poisoned) => {
                eprintln!("Warning: session state lock was poisoned; recovering.");
                let mut guard = poisoned.into_inner();
                let path = guard.spool_path.clone();
                let truncate = guard.spool_truncate;
                if truncate {
                    guard.spool_truncate = false;
                }
                (path, truncate, guard.trimspool_enabled)
            }
        };

        let Some(path) = path else {
            return;
        };

        let mut options = OpenOptions::new();
        options.create(true).write(true);
        if truncate {
            options.truncate(true);
        } else {
            options.append(true);
        }

        let mut file = match options.open(&path) {
            Ok(file) => file,
            Err(err) => {
                eprintln!("Failed to open spool file {}: {}", path.display(), err);
                return;
            }
        };

        for line in lines {
            let line_to_write = if trimspool_enabled {
                line.trim_end()
            } else {
                line.as_str()
            };
            if let Err(err) = writeln!(file, "{}", line_to_write) {
                eprintln!("Failed to write to spool file {}: {}", path.display(), err);
                break;
            }
        }
    }

    fn append_spool_rows(session: &Arc<Mutex<SessionState>>, rows: &[Vec<String>]) {
        if rows.is_empty() {
            return;
        }
        if !SqlEditorWidget::has_spool_target(session) {
            return;
        }
        let (colsep, null_text, _trimspool_enabled) =
            SqlEditorWidget::current_text_output_settings(session);
        let lines: Vec<String> = rows
            .iter()
            .map(|row| SqlEditorWidget::format_row_line(row, &colsep, &null_text))
            .collect();
        SqlEditorWidget::append_spool_output(session, &lines);
    }

    fn apply_define_substitution(
        sql: &str,
        session: &Arc<Mutex<SessionState>>,
        sender: &mpsc::Sender<QueryProgress>,
    ) -> Result<String, String> {
        let define_char = match session.lock() {
            Ok(guard) => guard.define_char,
            Err(poisoned) => {
                eprintln!("Warning: session state lock was poisoned; recovering.");
                poisoned.into_inner().define_char
            }
        };

        let mut result = String::with_capacity(sql.len());
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false;
        let mut in_q_quote = false;
        let mut q_quote_end: Option<char> = None;

        let chars: Vec<char> = sql.chars().collect();
        let len = chars.len();
        let mut i = 0usize;

        while i < len {
            let c = chars[i];
            let next = if i + 1 < len {
                Some(chars[i + 1])
            } else {
                None
            };
            let next2 = if i + 2 < len {
                Some(chars[i + 2])
            } else {
                None
            };

            if in_line_comment {
                result.push(c);
                if c == '\n' {
                    in_line_comment = false;
                }
                i += 1;
                continue;
            }

            if in_block_comment {
                result.push(c);
                if c == '*' && next == Some('/') {
                    result.push('/');
                    in_block_comment = false;
                    i += 2;
                    continue;
                }
                i += 1;
                continue;
            }

            if c == '-' && next == Some('-') {
                result.push('-');
                result.push('-');
                in_line_comment = true;
                i += 2;
                continue;
            }

            if c == '/' && next == Some('*') {
                result.push('/');
                result.push('*');
                in_block_comment = true;
                i += 2;
                continue;
            }

            if c == define_char {
                let is_double = next == Some(define_char);
                let start = if is_double { i + 2 } else { i + 1 };
                let mut j = start;
                while j < len {
                    let ch = chars[j];
                    if sql_text::is_identifier_char(ch) {
                        j += 1;
                    } else {
                        break;
                    }
                }

                if j == start {
                    result.push(c);
                    if is_double {
                        result.push(define_char);
                        i += 2;
                    } else {
                        i += 1;
                    }
                    continue;
                }

                let name: String = chars[start..j].iter().collect();
                let key = SessionState::normalize_name(&name);
                let (define_value, bind_value) = match session.lock() {
                    Ok(guard) => (
                        guard.define_vars.get(&key).cloned(),
                        guard.binds.get(&key).cloned(),
                    ),
                    Err(poisoned) => {
                        eprintln!("Warning: session state lock was poisoned; recovering.");
                        let guard = poisoned.into_inner();
                        (
                            guard.define_vars.get(&key).cloned(),
                            guard.binds.get(&key).cloned(),
                        )
                    }
                };

                let mut replacement = if let Some(value) = define_value {
                    value
                } else if let Some(bind) = bind_value {
                    SqlEditorWidget::format_define_value(&key, &bind)?
                } else {
                    let prompt = format!("Enter value for {}:", name);
                    let input = SqlEditorWidget::prompt_for_input_with_sender(sender, &prompt)?;
                    if is_double {
                        match session.lock() {
                            Ok(mut guard) => {
                                guard.define_vars.insert(key.clone(), input.clone());
                            }
                            Err(poisoned) => {
                                eprintln!("Warning: session state lock was poisoned; recovering.");
                                let mut guard = poisoned.into_inner();
                                guard.define_vars.insert(key.clone(), input.clone());
                            }
                        }
                    }
                    input
                }
                .to_string();

                if in_single_quote || in_q_quote {
                    if let Some(stripped) =
                        SqlEditorWidget::strip_wrapping_single_quotes(&replacement)
                    {
                        replacement = stripped;
                    }
                }

                result.push_str(&replacement);
                i = j;
                continue;
            }

            if in_q_quote {
                result.push(c);
                if Some(c) == q_quote_end && next == Some('\'') {
                    result.push('\'');
                    in_q_quote = false;
                    q_quote_end = None;
                    i += 2;
                    continue;
                }
                i += 1;
                continue;
            }

            if in_single_quote {
                result.push(c);
                if c == '\'' {
                    if next == Some('\'') {
                        result.push('\'');
                        i += 2;
                        continue;
                    }
                    in_single_quote = false;
                }
                i += 1;
                continue;
            }

            if in_double_quote {
                result.push(c);
                if c == '"' {
                    in_double_quote = false;
                }
                i += 1;
                continue;
            }

            // Handle nq'[...]' (National Character q-quoted strings)
            if (c == 'n' || c == 'N')
                && (next == Some('q') || next == Some('Q'))
                && i + 2 < len
                && chars[i + 2] == '\''
                && i + 3 < len
            {
                let delimiter = chars[i + 3];
                result.push(c);
                result.push(chars[i + 1]);
                result.push('\'');
                result.push(delimiter);
                in_q_quote = true;
                q_quote_end = Some(sql_text::q_quote_closing(delimiter));
                i += 4;
                continue;
            }

            // Handle q'[...]' (q-quoted strings)
            if (c == 'q' || c == 'Q') && next == Some('\'') && next2.is_some() {
                let delimiter = chars[i + 2];
                result.push(c);
                result.push('\'');
                result.push(delimiter);
                in_q_quote = true;
                q_quote_end = Some(sql_text::q_quote_closing(delimiter));
                i += 3;
                continue;
            }

            if c == '\'' {
                result.push(c);
                in_single_quote = true;
                i += 1;
                continue;
            }

            if c == '"' {
                result.push(c);
                in_double_quote = true;
                i += 1;
                continue;
            }

            result.push(c);
            i += 1;
        }

        Ok(result)
    }

    fn prompt_for_input_with_sender(
        sender: &mpsc::Sender<QueryProgress>,
        prompt: &str,
    ) -> Result<String, String> {
        let (response_tx, response_rx) = mpsc::channel();
        if sender
            .send(QueryProgress::PromptInput {
                prompt: prompt.to_string(),
                response: response_tx,
            })
            .is_err()
        {
            return Err("Substitution prompt failed: UI disconnected.".to_string());
        }

        match response_rx.recv_timeout(Duration::from_secs(300)) {
            Ok(Some(value)) => Ok(value),
            Ok(None) => Err("Substitution prompt cancelled.".to_string()),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                Err("Substitution prompt timed out.".to_string())
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err("Substitution prompt failed: UI disconnected.".to_string())
            }
        }
    }

    pub fn prompt_input_dialog(prompt: &str) -> Option<String> {
        let current_group = fltk::group::Group::try_current();
        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let mut dialog = fltk::window::Window::default()
            .with_size(420, 150)
            .with_label("Input");
        crate::ui::center_on_main(&mut dialog);
        dialog.set_color(theme::panel_raised());
        dialog.make_modal(true);

        let mut main_flex = Flex::default().with_pos(10, 10).with_size(400, 130);
        main_flex.set_type(FlexType::Column);
        main_flex.set_spacing(8);

        let mut prompt_frame = Frame::default().with_label(prompt);
        prompt_frame.set_label_color(theme::text_primary());
        prompt_frame.set_align(Align::Left | Align::Inside | Align::Wrap);
        main_flex.fixed(&prompt_frame, 48);

        let mut input = Input::default();
        input.set_color(theme::input_bg());
        input.set_text_color(theme::text_primary());
        input.set_trigger(CallbackTrigger::EnterKeyAlways);
        main_flex.fixed(&input, 30);

        let mut button_flex = Flex::default();
        button_flex.set_type(FlexType::Row);
        button_flex.set_spacing(8);

        let _spacer = Frame::default();

        let mut ok_btn = Button::default()
            .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
            .with_label("OK");
        ok_btn.set_color(theme::button_primary());
        ok_btn.set_label_color(theme::text_primary());
        ok_btn.set_frame(FrameType::RFlatBox);

        let mut cancel_btn = Button::default()
            .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
            .with_label("Cancel");
        cancel_btn.set_color(theme::button_subtle());
        cancel_btn.set_label_color(theme::text_primary());
        cancel_btn.set_frame(FrameType::RFlatBox);

        button_flex.fixed(&ok_btn, BUTTON_WIDTH);
        button_flex.fixed(&cancel_btn, BUTTON_WIDTH);
        button_flex.end();
        main_flex.fixed(&button_flex, BUTTON_ROW_HEIGHT);
        main_flex.end();
        dialog.end();
        fltk::group::Group::set_current(current_group.as_ref());

        let result: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let cancelled = Arc::new(Mutex::new(false));

        {
            let result = result.clone();
            let mut dialog = dialog.clone();
            let input = input.clone();
            ok_btn.set_callback(move |_| {
                *result
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(input.value());
                dialog.hide();
            });
        }

        {
            let cancelled = cancelled.clone();
            let mut dialog = dialog.clone();
            cancel_btn.set_callback(move |_| {
                *cancelled
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
                dialog.hide();
            });
        }

        {
            let result = result.clone();
            let mut input_cb = input.clone();
            let input_value = input.clone();
            let mut dialog_cb = dialog.clone();
            input_cb.set_callback(move |_| {
                *result
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(input_value.value());
                dialog_cb.hide();
            });
        }

        {
            let cancelled = cancelled.clone();
            let mut dialog_cb = dialog.clone();
            let mut dialog_handle = dialog.clone();
            dialog_cb.set_callback(move |_| {
                *cancelled
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
                dialog_handle.hide();
            });
        }

        dialog.show();
        input.take_focus().ok();

        while dialog.shown() {
            app::wait();
        }

        // Explicitly destroy top-level dialog widgets to release native resources.
        fltk::window::Window::delete(dialog);

        if *cancelled
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
        {
            None
        } else {
            result
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    fn format_define_value(name: &str, bind: &BindVar) -> Result<String, String> {
        let BindValue::Scalar(value) = &bind.value else {
            return Err(format!(
                "Substitution variable &{} must be a scalar value.",
                name
            ));
        };

        let value = value
            .as_ref()
            .ok_or_else(|| format!("Substitution variable &{} has no value.", name))?;

        if value.eq_ignore_ascii_case("NULL") {
            return Ok("NULL".to_string());
        }

        match bind.data_type {
            crate::db::session::BindDataType::Number => Ok(value.clone()),
            crate::db::session::BindDataType::Date
            | crate::db::session::BindDataType::Timestamp(_)
            | crate::db::session::BindDataType::Varchar2(_)
            | crate::db::session::BindDataType::Clob => {
                Ok(format!("'{}'", SqlEditorWidget::escape_sql_literal(value)))
            }
            crate::db::session::BindDataType::RefCursor => Err(format!(
                "Substitution variable &{} cannot be a REFCURSOR.",
                name
            )),
        }
    }

    fn escape_sql_literal(value: &str) -> String {
        value.replace('\'', "''")
    }

    fn strip_wrapping_single_quotes(value: &str) -> Option<String> {
        let trimmed = value.trim();
        if trimmed.len() < 2 {
            return None;
        }
        if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
            Some(trimmed[1..trimmed.len() - 1].to_string())
        } else {
            None
        }
    }

    fn requires_transaction_first_statement(items: &[ScriptItem]) -> bool {
        let first_statement = items.iter().find_map(|item| match item {
            ScriptItem::Statement(statement) => Some(statement.as_str()),
            ScriptItem::ToolCommand(_) => None,
        });
        first_statement
            .map(Self::is_transaction_first_statement)
            .unwrap_or(false)
    }

    fn is_transaction_first_statement(statement: &str) -> bool {
        let trimmed = statement.trim().trim_end_matches(';').trim();
        if trimmed.is_empty() {
            return false;
        }
        let upper = trimmed.to_ascii_uppercase();
        crate::sql_text::starts_with_keyword_token(&upper, "SET TRANSACTION")
            || crate::sql_text::starts_with_keyword_token(
                &upper,
                "ALTER SESSION SET ISOLATION_LEVEL",
            )
    }

    fn sync_serveroutput_with_session(
        conn: &Connection,
        session: &Arc<Mutex<SessionState>>,
    ) -> Result<(), OracleError> {
        let (enabled, size) = match session.lock() {
            Ok(guard) => (guard.server_output.enabled, guard.server_output.size),
            Err(poisoned) => {
                eprintln!("Warning: session state lock was poisoned; recovering.");
                let guard = poisoned.into_inner();
                (guard.server_output.enabled, guard.server_output.size)
            }
        };

        if enabled {
            let buffer_size = if size == 0 { None } else { Some(size) };
            QueryExecutor::enable_dbms_output(conn, buffer_size)
        } else {
            QueryExecutor::disable_dbms_output(conn)
        }
    }

    fn emit_dbms_output(
        sender: &mpsc::Sender<QueryProgress>,
        _conn_name: &str,
        conn: &Connection,
        session: &Arc<Mutex<SessionState>>,
        _result_index: &mut usize,
    ) -> Result<(), OracleError> {
        let (enabled, size) = match session.lock() {
            Ok(guard) => (guard.server_output.enabled, guard.server_output.size),
            Err(poisoned) => {
                eprintln!("Warning: session state lock was poisoned; recovering.");
                let guard = poisoned.into_inner();
                (guard.server_output.enabled, guard.server_output.size)
            }
        };

        if !enabled {
            return Ok(());
        }

        let max_lines = if size == 0 {
            10_000
        } else {
            (size / 80).clamp(1, 10_000)
        };
        let lines = QueryExecutor::get_dbms_output(conn, max_lines)?;
        if lines.is_empty() {
            return Ok(());
        }

        let mut output_lines = Vec::with_capacity(lines.len() + 1);
        output_lines.push("DBMS_OUTPUT".to_string());
        output_lines.extend(lines);
        SqlEditorWidget::emit_script_output(sender, session, output_lines);
        Ok(())
    }

    fn ensure_plsql_terminator(sql: &str) -> String {
        let trimmed = sql.trim_end();
        if trimmed.ends_with(';') {
            trimmed.to_string()
        } else {
            format!("{};", trimmed)
        }
    }

    fn strip_leading_comments(sql: &str) -> String {
        let mut remaining = sql;

        loop {
            let trimmed = remaining.trim_start();

            if trimmed.starts_with("--") {
                if let Some(line_end) = trimmed.find('\n') {
                    remaining = &trimmed[line_end + 1..];
                    continue;
                }
                return String::new();
            }

            if trimmed.starts_with("/*") {
                if let Some(block_end) = trimmed.find("*/") {
                    remaining = &trimmed[block_end + 2..];
                    continue;
                }
                return String::new();
            }

            if matches!(
                trimmed.split_whitespace().next(),
                Some(first) if first.eq_ignore_ascii_case("REM")
                    || first.eq_ignore_ascii_case("REMARK")
            ) {
                if let Some(line_end) = trimmed.find('\n') {
                    remaining = &trimmed[line_end + 1..];
                    continue;
                }
                return String::new();
            }

            return trimmed.to_string();
        }
    }

    fn normalize_object_name(value: &str) -> String {
        let trimmed = value.trim();
        if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
            trimmed.trim_matches('"').to_string()
        } else {
            trimmed.to_uppercase()
        }
    }

    fn ddl_message(sql_upper: &str) -> String {
        QueryExecutor::ddl_message(sql_upper)
    }

    fn timeout_error_message_contains_timeout_signal(message: &str) -> bool {
        let lowered = message.trim().to_ascii_lowercase();
        // Keep timeout detection strict so non-call timeout errors
        // (e.g. lock wait timeout expired) are not misclassified.
        lowered.contains("dpi-1067")
            || lowered.contains("call timeout")
            || lowered.contains("query timed out")
            || lowered.contains("timed out after")
    }

    fn is_timeout_error(err: &OracleError) -> bool {
        Self::timeout_error_message_contains_timeout_signal(&err.to_string())
    }

    fn is_cancel_error(err: &OracleError) -> bool {
        let message = err.to_string();
        message.contains("ORA-01013")
    }

    fn timeout_message(timeout: Option<Duration>) -> String {
        match timeout {
            Some(duration) => format!("Query timed out after {} seconds", duration.as_secs()),
            None => "Query timed out".to_string(),
        }
    }

    fn cancel_message() -> String {
        "Query cancelled".to_string()
    }

    fn choose_execution_error_message(
        cancelled: bool,
        timed_out: bool,
        timeout: Option<Duration>,
        fallback: String,
    ) -> String {
        if timed_out {
            Self::timeout_message(timeout)
        } else if cancelled {
            Self::cancel_message()
        } else {
            fallback
        }
    }

    fn parse_timeout(value: &str) -> Option<Duration> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return None;
        }

        let secs = match trimmed.parse::<u64>() {
            Ok(secs) => secs,
            Err(err) => {
                eprintln!("Invalid timeout value '{trimmed}': {err}");
                return None;
            }
        };
        if secs == 0 {
            None
        } else {
            Some(Duration::from_secs(secs))
        }
    }

    pub fn set_progress_callback<F>(&mut self, callback: F)
    where
        F: FnMut(QueryProgress) + 'static,
    {
        *self
            .progress_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(callback));
    }
}

#[cfg(test)]
mod formatter_regression_tests {
    use super::{QueryProgress, ScriptItem, SqlEditorWidget, PROGRESS_ROWS_INITIAL_BATCH};
    use crate::db::SessionState;
    use std::sync::{mpsc, Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn does_not_force_select_line_break_after_malformed_statement() {
        let sql = "select fn(a, b;\nselect x, y from dual;";
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(formatted.contains("SELECT x, y\nFROM DUAL;"));
        assert!(!formatted.contains("SELECT\n    x,\n    y\nFROM DUAL;"));
    }

    #[test]
    fn comments_do_not_change_paren_tracking_state() {
        let sql = "select a, /* comment with (, ), and , */ b from dual;";
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(formatted.contains("/* comment with (, ), and , */"));
        assert!(
            formatted
                .contains("SELECT\n    a,\n    /* comment with (, ), and , */\n    b\nFROM DUAL;")
                || formatted
                    .contains("SELECT a,\n    /* comment with (, ), and , */\n    b\nFROM DUAL;"),
            "Comment-preserving select formatting should remain stable, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_preserves_prompt_banner_lines_verbatim() {
        let sql = "PROMPT =======================================================================\n\
PROMPT [END] If you saw outputs + cursor print + summary selects, parsing/execution is OK\n\
PROMPT =======================================================================";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert_eq!(formatted, sql);
    }

    #[test]
    fn keeps_multiline_string_continuation_lines_without_depth_reindent() {
        let sql = "BEGIN
DBMS_OUTPUT.PUT_LINE('first line
second line
third line');
END;";
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(formatted.contains("DBMS_OUTPUT.PUT_LINE ('first line\nsecond line\nthird line');"));
    }

    #[test]
    fn keeps_ampersand_substitution_variables_together() {
        let sql = "SELECT &&pp FROM dual";
        let formatted = SqlEditorWidget::format_sql_basic(sql);
        assert!(
            formatted.contains("&&pp"),
            "&&pp should stay together, got: {}",
            formatted
        );

        let sql2 = "SELECT &var1 FROM dual";
        let formatted2 = SqlEditorWidget::format_sql_basic(sql2);
        assert!(
            formatted2.contains("&var1"),
            "&var1 should stay together, got: {}",
            formatted2
        );
    }

    #[test]
    fn keeps_merge_into_together() {
        let sql = "MERGE INTO target_table t USING source_table s ON (t.id = s.id) WHEN MATCHED THEN UPDATE SET t.name = s.name";
        let formatted = SqlEditorWidget::format_sql_basic(sql);
        assert!(
            formatted.contains("MERGE INTO target_table"),
            "MERGE INTO should stay on the same line, got: {}",
            formatted
        );
    }

    #[test]
    fn keeps_start_with_together() {
        let sql = "SELECT employee_id, manager_id FROM employees START WITH manager_id IS NULL CONNECT BY PRIOR employee_id = manager_id";
        let formatted = SqlEditorWidget::format_sql_basic(sql);
        assert!(
            formatted.contains("START WITH"),
            "START WITH should stay on the same line, got: {}",
            formatted
        );
    }

    #[test]
    fn formats_where_in_subquery_with_deep_indent_and_alias() {
        let source = "select a.topic, a.TOPIC from help a where a.SEQ in (select seq from help) b";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);

        assert_eq!(
            preserved.trim_end(),
            "SELECT a.topic,\n    a.TOPIC\nFROM help a\nWHERE a.SEQ IN (\n        SELECT seq\n        FROM help\n    ) b"
        );
    }

    #[test]
    fn deeply_nested_subqueries_keep_progressive_depth_indentation() {
        let source = r#"BEGIN
  SELECT col1
  INTO v_col
  FROM t1
  WHERE EXISTS (
    SELECT 1
    FROM t2
    WHERE t2.id IN (
      SELECT t3.id
      FROM t3
      WHERE t3.flag = 'Y'
    )
  );
END;"#;

        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            formatted.contains("WHERE EXISTS (\n            SELECT 1"),
            "first nested subquery should stay indented under EXISTS, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("WHERE t2.id IN (\n                SELECT t3.id"),
            "second nested subquery should stay indented under IN, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("WHERE t3.flag = 'Y'\n            )"),
            "closing parenthesis should dedent one level from deepest query body, got:\n{}",
            formatted
        );
    }

    #[test]
    fn keeps_repeat_block_as_single_indented_block() {
        let sql = r#"BEGIN
  REPEAT
    DBMS_OUTPUT.PUT_LINE('start');
    i := i + 1;
  UNTIL i >= 3
  END REPEAT;
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("END REPEAT;"),
            "REPEAT block terminator should stay on a single line, got: {}",
            formatted
        );

        let repeat_end_line = formatted
            .lines()
            .find(|line| line.trim().starts_with("END REPEAT;"))
            .expect("formatted output should contain END REPEAT line");
        let end_line = formatted.lines().find(|line| line.trim() == "END");

        assert!(
            end_line.unwrap_or("    ").starts_with("    "),
            "END should be indented"
        );
        assert!(
            formatted.contains("DBMS_OUTPUT.PUT_LINE"),
            "REPEAT body should remain present, got: {}",
            formatted
        );
        assert!(
            repeat_end_line.starts_with("    "),
            "END REPEAT should match block indent"
        );

        let formatted_again = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            formatted, formatted_again,
            "Formatting should be idempotent for REPEAT blocks"
        );
    }

    #[test]
    fn tab_off_keeps_tab_character_in_script_output() {
        let line = "A\tB";
        let rendered = SqlEditorWidget::format_script_output_line(line, false, false);
        assert_eq!(rendered, "A\tB");
    }

    #[test]
    fn tab_on_expands_tab_character_in_script_output() {
        let line = "A\tB";
        let rendered = SqlEditorWidget::format_script_output_line(line, false, true);
        assert_eq!(rendered, "A       B");
    }

    #[test]
    fn nested_case_expression_in_plsql_aligns_else_correctly() {
        let sql = r#"CREATE OR REPLACE PACKAGE BODY oqt_mega_pkg AS
FUNCTION f_deep(p_grp IN NUMBER, p_n IN NUMBER, p_txt IN VARCHAR2) RETURN NUMBER IS
  v NUMBER := 0;
BEGIN
  v :=
    CASE
      WHEN p_grp < 0 THEN -1000
      WHEN p_grp = 0 THEN
        CASE
          WHEN p_n > 10 THEN 100
          ELSE 10
        END
      ELSE
        CASE
          WHEN INSTR(NVL(p_txt,'x'), 'END;') > 0 THEN 777
          ELSE LENGTH(NVL(p_txt,'')) + p_n
        END
    END;
  RETURN v;
END;
END oqt_mega_pkg;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        // ELSE after inner CASE END should align with outer WHEN
        assert!(
            formatted.contains("END\n            ELSE\n                CASE"),
            "ELSE should align with outer WHEN after inner CASE END, got:\n{}",
            formatted
        );

        // Outer END should close the outer CASE properly
        assert!(
            formatted.contains("END\n        END;"),
            "Outer CASE END should be properly indented, got:\n{}",
            formatted
        );

        // Idempotent
        let formatted_again = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            formatted, formatted_again,
            "Formatting should be idempotent for nested CASE expressions"
        );
    }

    #[test]
    fn package_body_named_end_with_if_prefix_is_not_treated_as_end_if_suffix() {
        let sql = r#"CREATE OR REPLACE PACKAGE BODY if_owner AS
FUNCTION run_check RETURN NUMBER IS
BEGIN
  IF 1 = 1 THEN
    RETURN 1;
  END IF;
END run_check;
BEGIN
  NULL;
END if_owner;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        let end_if_owner_line = formatted
            .lines()
            .find(|line| line.trim().eq_ignore_ascii_case("END if_owner;"));
        assert!(
            end_if_owner_line.is_some_and(|line| !line.starts_with(' ')),
            "Package named END label should close package body at top-level depth, got:
{}",
            formatted
        );

        let end_if_line = formatted
            .lines()
            .find(|line| line.trim().eq_ignore_ascii_case("END IF;"));
        assert!(
            end_if_line.is_some_and(|line| line.starts_with("        ")),
            "Nested END IF should remain more indented than END package label, got:
{}",
            formatted
        );
    }

    #[test]
    fn package_body_final_end_label_aligns_to_top_level() {
        let sql = r#"CREATE OR REPLACE PACKAGE BODY fmt_pkg_extreme AS
PROCEDURE run_extreme IS
BEGIN
  NULL;
END run_extreme;

BEGIN
  NULL;
END fmt_pkg_extreme;"#;

        let formatted = SqlEditorWidget::format_sql_basic(sql);
        let end_pkg_line = formatted
            .lines()
            .find(|line| line.trim().eq_ignore_ascii_case("END fmt_pkg_extreme;"));

        assert!(
            end_pkg_line.is_some_and(|line| !line.starts_with(' ')),
            "Final package END label should align with CREATE at top-level, got:\n{}",
            formatted
        );
    }

    #[test]
    fn plsql_else_if_clause_uses_block_depth_without_extra_into_indent() {
        let sql = r#"BEGIN
  IF 1 = 1 THEN
    SELECT col1,
           col2
      INTO v_col1,
           v_col2
      FROM dual;
  ELSEIF v_col1 = 1 THEN
      NULL;
  END IF;
  NULL;
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("    ELSEIF v_col1 = 1 THEN"),
            "ELSEIF should align to block depth, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("        ELSEIF v_col1 = 1 THEN"),
            "ELSEIF should not keep stale INTO-list extra indent, got:\n{}",
            formatted
        );
    }

    #[test]
    fn plsql_elseif_clause_uses_block_depth_without_extra_into_indent() {
        let sql = r#"BEGIN
  IF 1 = 1 THEN
    SELECT col1,
           col2
      INTO v_col1,
           v_col2
      FROM dual;
  ELSIF v_col1 = 1 THEN
      NULL;
  END IF;
  NULL;
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("    ELSIF v_col1 = 1 THEN"),
            "ELSIF should align to block depth, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("        ELSIF v_col1 = 1 THEN"),
            "ELSIF should not keep stale INTO-list extra indent, got:\n{}",
            formatted
        );
    }

    #[test]
    fn plsql_statement_after_returning_into_does_not_keep_extra_into_indent() {
        let sql = r#"BEGIN
  UPDATE emp
  SET sal = sal + 1
  RETURNING empno
  INTO v_empno;
  NULL;
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("    NULL;"),
            "line after RETURNING INTO should align to block depth, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("        NULL;"),
            "line after RETURNING INTO should not keep stale INTO-list extra indent, got:\n{}",
            formatted
        );
    }

    #[test]
    fn plsql_statement_after_fetch_into_does_not_keep_extra_into_indent() {
        let sql = r#"BEGIN
  FETCH c1
  INTO v_empno;
  v_total := v_total + 1;
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("    v_total := v_total + 1;"),
            "line after FETCH INTO should align to block depth, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("        v_total := v_total + 1;"),
            "line after FETCH INTO should not keep stale INTO-list extra indent, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_statement_preserves_end_for_and_end_while_suffixes() {
        let for_sql = "BEGIN\n  FOR i IN 1..3 LOOP\n    NULL;\n  END FOR;\nEND;";
        let for_formatted = SqlEditorWidget::format_sql_basic(for_sql);
        assert!(
            for_formatted.contains("END FOR;"),
            "END FOR suffix should be preserved as a single terminator line, got:\n{}",
            for_formatted
        );

        let while_sql = "BEGIN\n  WHILE i < 3 LOOP\n    i := i + 1;\n  END WHILE;\nEND;";
        let while_formatted = SqlEditorWidget::format_sql_basic(while_sql);
        assert!(
            while_formatted.contains("END WHILE;"),
            "END WHILE suffix should be preserved as a single terminator line, got:\n{}",
            while_formatted
        );
    }

    #[test]
    fn paren_case_expression_tracks_searched_case_headers() {
        let sql = r#"BEGIN
  v_val := (
    CASE v_mode
      WHEN 1 THEN 10
      ELSE 0
    END
  );
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("(\n        CASE v_mode\n            WHEN 1 THEN"),
            "CASE <expr> after '(' should get parenthesized CASE depth indent, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("ELSE\n            0\n        END"),
            "CASE <expr> END should keep CASE-block depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn paren_case_expression_tracks_end_case_terminator() {
        let sql = r#"BEGIN
  v_val := (
    CASE
      WHEN v_mode = 1 THEN 10
      ELSE 0
    END CASE
  );
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("CASE\n            WHEN v_mode = 1 THEN"),
            "Parenthesized CASE should still format branch depth, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("ELSE\n            0\n        END CASE);"),
            "END CASE terminator should stay aligned with CASE header in parenthesized expression, got:\n{}",
            formatted
        );
    }

    #[test]
    fn starts_with_end_suffix_terminator_requires_keyword_boundary() {
        assert!(SqlEditorWidget::starts_with_end_suffix_terminator(
            "END IF;"
        ));
        assert!(SqlEditorWidget::starts_with_end_suffix_terminator(
            "END LOOP"
        ));
        assert!(SqlEditorWidget::starts_with_end_suffix_terminator(
            "END CASE"
        ));
        assert!(SqlEditorWidget::starts_with_end_suffix_terminator(
            "END REPEAT"
        ));
        assert!(!SqlEditorWidget::starts_with_end_suffix_terminator("END"));
        assert!(SqlEditorWidget::starts_with_end_suffix_terminator(
            "END FOR"
        ));
        assert!(SqlEditorWidget::starts_with_end_suffix_terminator(
            "END WHILE"
        ));
        assert!(!SqlEditorWidget::starts_with_end_suffix_terminator(
            "END IF_OWNER;"
        ));
        assert!(!SqlEditorWidget::starts_with_end_suffix_terminator(
            "END FORWARD;"
        ));
    }

    #[test]
    fn starts_with_plain_end_excludes_qualified_end_suffixes() {
        assert!(SqlEditorWidget::starts_with_plain_end("END"));
        assert!(SqlEditorWidget::starts_with_plain_end("END pkg;"));
        assert!(!SqlEditorWidget::starts_with_plain_end("END FOR"));
        assert!(!SqlEditorWidget::starts_with_plain_end("END WHILE"));
        assert!(!SqlEditorWidget::starts_with_plain_end("END IF;"));
        assert!(!SqlEditorWidget::starts_with_plain_end("END LOOP"));
        assert!(!SqlEditorWidget::starts_with_plain_end("END CASE"));
        assert!(!SqlEditorWidget::starts_with_plain_end("END REPEAT"));
    }

    #[test]
    fn starts_with_bare_end_matches_only_unqualified_end() {
        assert!(SqlEditorWidget::starts_with_bare_end("END"));
        assert!(SqlEditorWidget::starts_with_bare_end("END;"));
        assert!(!SqlEditorWidget::starts_with_bare_end("END pkg;"));
        assert!(!SqlEditorWidget::starts_with_bare_end("END IF;"));
    }

    #[test]
    fn keyword_token_match_handles_exact_keyword_lines() {
        assert!(crate::sql_text::starts_with_keyword_token(
            "SELECT", "SELECT"
        ));
        assert!(crate::sql_text::starts_with_keyword_token("INTO", "INTO"));
        assert!(crate::sql_text::starts_with_keyword_token(
            "SELECT x", "SELECT"
        ));
        assert!(!crate::sql_text::starts_with_keyword_token(
            "SELECTED", "SELECT"
        ));
    }

    #[test]
    fn detects_set_transaction_as_first_statement() {
        let items = vec![ScriptItem::Statement(
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;".to_string(),
        )];
        assert!(SqlEditorWidget::requires_transaction_first_statement(
            &items
        ));
    }

    #[test]
    fn detects_alter_session_isolation_level_as_first_statement() {
        let items = vec![ScriptItem::Statement(
            "ALTER SESSION SET ISOLATION_LEVEL = SERIALIZABLE;".to_string(),
        )];
        assert!(SqlEditorWidget::requires_transaction_first_statement(
            &items
        ));
    }

    #[test]
    fn cursor_mapping_tracks_prefix_after_full_reformat() {
        let source = "SELECT a, b FROM dual";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let source_pos = source
            .find("b FROM")
            .expect("source cursor anchor should exist") as i32;
        let mapped =
            SqlEditorWidget::map_cursor_after_format(source, &formatted, source_pos, false);
        let mapped_slice = &formatted[mapped as usize..];
        assert!(
            mapped_slice.trim_start().starts_with("b\nFROM DUAL;"),
            "Mapped cursor should stay near the same token after reformat, got: {}",
            mapped_slice
        );
    }

    #[test]
    fn cursor_mapping_large_source_uses_fast_path_and_keeps_utf8_boundary() {
        let source = "x".repeat(super::CURSOR_MAPPING_FULL_REFORMAT_THRESHOLD_BYTES + 128);
        let formatted = "SELECT\n    1\nFROM DUAL;";
        let source_pos = (source.len() / 2) as i32;

        let mapped = SqlEditorWidget::map_cursor_after_format(&source, formatted, source_pos, false)
            as usize;

        assert!(
            mapped <= formatted.len(),
            "mapped cursor should stay in bounds"
        );
        assert!(
            formatted.is_char_boundary(mapped),
            "mapped cursor should stay on UTF-8 boundary"
        );
    }

    #[test]
    fn cursor_mapping_selection_uses_selection_relative_offset() {
        let source = "SELECT a, b FROM dual";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let source_pos_within_selection = source
            .find("b FROM")
            .expect("source cursor anchor should exist")
            as i32;
        let mapped_within_selection = SqlEditorWidget::map_cursor_after_format(
            source,
            &formatted,
            source_pos_within_selection,
            true,
        );
        let selection_start = 25i32;
        let final_cursor_pos = selection_start + mapped_within_selection;
        let formatted_slice = &formatted[mapped_within_selection as usize..];

        assert!(
            formatted_slice.trim_start().starts_with("b\nFROM DUAL;"),
            "Mapped cursor inside selection should stay near the same token after reformat, got: {}",
            formatted_slice
        );
        assert_eq!(
            final_cursor_pos,
            selection_start + mapped_within_selection,
            "Selection-relative mapping should compose with selection offset"
        );
    }

    #[test]
    fn cursor_mapping_selection_without_semicolon_keeps_token_anchor() {
        let source = "SELECT a, b FROM dual";
        let formatted = SqlEditorWidget::preserve_selected_text_terminator(
            source,
            SqlEditorWidget::format_sql_basic(source),
        );
        let source_pos_within_selection = source
            .find("b FROM")
            .expect("source cursor anchor should exist")
            as i32;

        let mapped_within_selection = SqlEditorWidget::map_cursor_after_format(
            source,
            &formatted,
            source_pos_within_selection,
            true,
        );
        let formatted_slice = &formatted[mapped_within_selection as usize..];

        assert!(
            formatted_slice.trim_start().starts_with("b\nFROM DUAL"),
            "Mapped cursor should stay near same token for semicolon-free selection, got: {}",
            formatted_slice
        );
        assert!(
            !formatted.trim_end().ends_with(';'),
            "Selection-preserved formatted SQL should not end with semicolon"
        );
    }

    #[test]
    fn cursor_mapping_uses_utf8_byte_offsets() {
        let source = "SELECT 한글, b FROM dual";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let byte_offset = source
            .find("b FROM")
            .expect("source cursor anchor should exist") as i32;

        let mapped =
            SqlEditorWidget::map_cursor_after_format(source, &formatted, byte_offset, false);
        let mapped_slice = &formatted[mapped as usize..];
        assert!(
            mapped_slice.trim_start().starts_with("b\nFROM DUAL;"),
            "Mapped cursor should stay near token with byte-offset mapping, got: {}",
            mapped_slice
        );
    }

    #[test]
    fn normalize_index_treats_input_as_byte_offset() {
        let source = "SELECT éa, b FROM dual";
        let byte_offset = source
            .find('b')
            .expect("expected cursor anchor should exist") as i32;

        let normalized = SqlEditorWidget::normalize_index(source, byte_offset);
        assert_eq!(
            normalized, byte_offset as usize,
            "normalize_index should preserve byte offsets as-is"
        );
    }

    #[test]
    fn normalize_index_clamps_non_boundary_utf8_byte_offset() {
        let source = "SELECT 한글, b FROM dual";
        let utf8_start = source.find('한').expect("expected utf-8 anchor");
        let mid_char_offset = utf8_start + 1;
        let normalized = SqlEditorWidget::normalize_index(source, mid_char_offset as i32);
        assert_eq!(
            normalized, utf8_start,
            "normalize_index should clamp invalid UTF-8 byte offsets"
        );
    }

    #[test]
    fn normalize_index_clamps_invalid_utf8_boundary_without_panic() {
        let source = "SELECT 한글, b FROM dual";
        let mid_char_index = source.find("한").expect("expected unicode anchor") + 1;

        let normalized = SqlEditorWidget::normalize_index(source, mid_char_index as i32);
        assert!(source.is_char_boundary(normalized));
        assert!(normalized <= source.len());
    }

    #[test]
    fn choose_execution_error_message_prioritizes_timeout_over_cancel() {
        let message = SqlEditorWidget::choose_execution_error_message(
            true,
            true,
            Some(Duration::from_secs(9)),
            "ORA-01013".to_string(),
        );
        assert_eq!(message, "Query timed out after 9 seconds");
    }

    #[test]
    fn choose_execution_error_message_uses_cancel_when_not_timed_out() {
        let message = SqlEditorWidget::choose_execution_error_message(
            true,
            false,
            Some(Duration::from_secs(9)),
            "ORA-01013".to_string(),
        );
        assert_eq!(message, "Query cancelled");
    }

    #[test]
    fn choose_execution_error_message_falls_back_to_original_error() {
        let message = SqlEditorWidget::choose_execution_error_message(
            false,
            false,
            Some(Duration::from_secs(9)),
            "ORA-00001: unique constraint".to_string(),
        );
        assert_eq!(message, "ORA-00001: unique constraint");
    }

    #[test]
    fn timeout_error_message_signal_detects_dpi_call_timeout() {
        assert!(
            SqlEditorWidget::timeout_error_message_contains_timeout_signal(
                "DPI-1067: call timeout of 5000 ms reached",
            )
        );
    }

    #[test]
    fn timeout_error_message_signal_detects_timeout_keyword_with_ora_01013() {
        assert!(
            SqlEditorWidget::timeout_error_message_contains_timeout_signal(
                "ORA-01013: user requested cancel of current operation (call timeout reached)",
            )
        );
    }

    #[test]
    fn timeout_error_message_signal_does_not_treat_plain_cancel_as_timeout() {
        assert!(
            !SqlEditorWidget::timeout_error_message_contains_timeout_signal(
                "ORA-01013: user requested cancel of current operation",
            )
        );
    }

    #[test]
    fn timeout_error_message_signal_does_not_treat_lock_wait_timeout_expired_as_call_timeout() {
        assert!(
            !SqlEditorWidget::timeout_error_message_contains_timeout_signal(
                "ORA-00054: resource busy and acquire with NOWAIT specified or timeout expired",
            )
        );
    }

    #[test]
    fn post_execution_output_is_skipped_when_cancel_requested() {
        assert!(!SqlEditorWidget::should_capture_post_execution_output(
            true, false, false
        ));
    }

    #[test]
    fn post_execution_output_is_skipped_when_timed_out() {
        assert!(!SqlEditorWidget::should_capture_post_execution_output(
            false, true, false
        ));
    }

    #[test]
    fn post_execution_output_is_skipped_when_execution_should_stop() {
        assert!(!SqlEditorWidget::should_capture_post_execution_output(
            false, false, true
        ));
    }

    #[test]
    fn post_execution_output_is_allowed_for_normal_completion() {
        assert!(SqlEditorWidget::should_capture_post_execution_output(
            false, false, false
        ));
    }

    #[test]
    fn flush_buffered_rows_drops_pending_rows_when_interrupted() {
        let (sender, receiver) = mpsc::channel();
        let session = Arc::new(Mutex::new(SessionState::default()));
        let mut buffered_rows = vec![vec!["1".to_string()], vec!["2".to_string()]];

        SqlEditorWidget::flush_buffered_rows(&sender, &session, 7, &mut buffered_rows, true);

        assert!(buffered_rows.is_empty());
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn flush_buffered_rows_emits_rows_when_not_interrupted() {
        let (sender, receiver) = mpsc::channel();
        let session = Arc::new(Mutex::new(SessionState::default()));
        let mut buffered_rows = vec![vec!["1".to_string()], vec!["2".to_string()]];

        SqlEditorWidget::flush_buffered_rows(&sender, &session, 3, &mut buffered_rows, false);

        assert!(buffered_rows.is_empty());
        let message = receiver
            .try_recv()
            .unwrap_or_else(|err| panic!("expected buffered rows progress message: {err}"));
        match message {
            QueryProgress::Rows { index, rows } => {
                assert_eq!(index, 3);
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("expected QueryProgress::Rows"),
        }
    }

    #[test]
    fn flush_buffered_result_rows_emits_display_rows() {
        let (sender, receiver) = mpsc::channel();
        let session = Arc::new(Mutex::new(SessionState::default()));
        let mut buffered_display_rows = vec![vec!["(null)".to_string()], vec!["2".to_string()]];
        let mut buffered_raw_rows = vec![vec!["".to_string()], vec!["2".to_string()]];

        SqlEditorWidget::flush_buffered_result_rows(
            &sender,
            &session,
            9,
            &mut buffered_display_rows,
            &mut buffered_raw_rows,
        );

        assert!(buffered_display_rows.is_empty());
        assert!(buffered_raw_rows.is_empty());
        let message = receiver
            .try_recv()
            .unwrap_or_else(|err| panic!("expected result rows progress message: {err}"));
        match message {
            QueryProgress::Rows { index, rows } => {
                assert_eq!(index, 9);
                assert_eq!(
                    rows,
                    vec![vec!["(null)".to_string()], vec!["2".to_string()]]
                );
            }
            _ => panic!("expected QueryProgress::Rows"),
        }
    }

    #[test]
    fn emit_select_result_uses_streaming_sized_initial_batch() {
        let (sender, receiver) = mpsc::channel();
        let session = Arc::new(Mutex::new(SessionState::default()));
        let rows = (0..101)
            .map(|index| vec![format!("value_{index}")])
            .collect::<Vec<Vec<String>>>();

        SqlEditorWidget::emit_select_result(
            &sender,
            &session,
            "LOCAL",
            4,
            "select * from dual",
            vec!["COL1".to_string()],
            rows,
            true,
            true,
        );

        let messages = receiver.try_iter().collect::<Vec<QueryProgress>>();
        assert_eq!(messages.len(), 5);
        match &messages[0] {
            QueryProgress::StatementStart { index } => assert_eq!(*index, 4),
            _ => panic!("expected QueryProgress::StatementStart"),
        }
        match &messages[1] {
            QueryProgress::SelectStart {
                index,
                columns,
                null_text: _,
            } => {
                assert_eq!(*index, 4);
                assert_eq!(columns, &vec!["COL1".to_string()]);
            }
            _ => panic!("expected QueryProgress::SelectStart"),
        }
        match &messages[2] {
            QueryProgress::Rows { index, rows } => {
                assert_eq!(*index, 4);
                assert_eq!(rows.len(), PROGRESS_ROWS_INITIAL_BATCH);
            }
            _ => panic!("expected initial QueryProgress::Rows"),
        }
        match &messages[3] {
            QueryProgress::Rows { index, rows } => {
                assert_eq!(*index, 4);
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("expected trailing QueryProgress::Rows"),
        }
        match &messages[4] {
            QueryProgress::StatementFinished {
                index,
                result,
                connection_name,
                timed_out,
            } => {
                assert_eq!(*index, 4);
                assert_eq!(connection_name, "LOCAL");
                assert!(!timed_out);
                assert_eq!(result.row_count, 101);
            }
            _ => panic!("expected QueryProgress::StatementFinished"),
        }
    }

    #[test]
    fn plsql_like_detection_ignores_begin_inside_strings_or_comments() {
        assert!(!SqlEditorWidget::is_plsql_like_statement(
            "SELECT 'BEGIN' AS txt FROM dual;"
        ));
        assert!(!SqlEditorWidget::is_plsql_like_statement(
            "/* DECLARE */ SELECT 1 FROM dual;"
        ));
        assert!(SqlEditorWidget::is_plsql_like_statement(
            "CREATE OR REPLACE PROCEDURE p IS BEGIN NULL; END;"
        ));
    }

    #[test]
    fn plsql_like_detection_ignores_explain_and_open_for() {
        assert!(!SqlEditorWidget::is_plsql_like_statement(
            "EXPLAIN PLAN FOR SELECT 1 FROM dual;"
        ));
        assert!(!SqlEditorWidget::is_plsql_like_statement(
            "OPEN p_rc FOR SELECT empno FROM oqt_t_emp;"
        ));
    }

    #[test]
    fn plsql_like_detection_supports_with_function_factoring() {
        assert!(SqlEditorWidget::is_plsql_like_statement(
            "WITH FUNCTION format_name(p_name IN VARCHAR2) RETURN VARCHAR2 IS\nBEGIN\n  RETURN INITCAP(p_name);\nEND;\nSELECT * FROM dual;"
        ));
    }

    #[test]
    fn plsql_like_detection_supports_or_replace_force_procedure() {
        assert!(SqlEditorWidget::is_plsql_like_statement(
            "CREATE OR REPLACE FORCE PROCEDURE test_proc AS\nBEGIN\n  NULL;\nEND;"
        ));
    }

    #[test]
    fn plsql_like_detection_supports_or_replace_editionable_function() {
        assert!(SqlEditorWidget::is_plsql_like_statement(
            "CREATE OR REPLACE EDITIONABLE FUNCTION test_fn RETURN NUMBER IS\nBEGIN\n  RETURN 1;\nEND;"
        ));
    }

    #[test]
    fn plsql_like_detection_supports_package_body() {
        assert!(SqlEditorWidget::is_plsql_like_statement(
            "CREATE PACKAGE BODY test_pkg AS\n  PROCEDURE proc IS\n  BEGIN\n    NULL;\n  END;\nEND;"
        ));
    }

    #[test]
    fn plsql_like_detection_supports_no_force_function() {
        assert!(SqlEditorWidget::is_plsql_like_statement(
            "CREATE NO FORCE FUNCTION test_fn RETURN NUMBER IS\nBEGIN\n  RETURN 1;\nEND;"
        ));
    }

    #[test]
    fn plsql_like_detection_rejects_create_materialized_view() {
        assert!(!SqlEditorWidget::is_plsql_like_statement(
            "CREATE MATERIALIZED VIEW test_mv AS SELECT 1 FROM dual"
        ));
    }

    #[test]
    fn plsql_like_detection_rejects_create_materialized_view_log() {
        assert!(!SqlEditorWidget::is_plsql_like_statement(
            "CREATE MATERIALIZED VIEW LOG ON test_table"
        ));
    }

    #[test]
    fn plsql_like_detection_rejects_create_view() {
        assert!(!SqlEditorWidget::is_plsql_like_statement(
            "CREATE OR REPLACE VIEW test_view AS SELECT 1 FROM dual"
        ));
    }

    #[test]
    fn trigger_audit_block_keeps_expected_header_and_values_alignment() {
        let sql = r#"create or replace noneditionable trigger "SYSTEM"."OQT_TRG_MEG_CUD" after insert or update or delete on oqt_meg_master for each row begin if inserting then insert into oqt_meg_audit(event_type, table_name, pk_text, detail_text) values ('INSERT', 'OQT_MEG_MASTER', 'master_id='||:NEW.master_id, 'key='||:NEW.master_key||', status='||:NEW.status||', amount='||TO_CHAR(:NEW.amount)); elsif updating then insert into oqt_meg_audit(event_type, table_name, pk_text, detail_text) values ('UPDATE', 'OQT_MEG_MASTER', 'master_id='||:NEW.master_id, 'status:'||:OLD.status||'->'||:NEW.status||', amount:'||TO_CHAR(:OLD.amount)||'->'||TO_CHAR(:NEW.amount)); elsif deleting then insert into oqt_meg_audit(event_type, table_name, pk_text, detail_text) values ('DELETE', 'OQT_MEG_MASTER', 'master_id='||:OLD.master_id, 'key='||:OLD.master_key||', status='||:OLD.status||', amount='||TO_CHAR(:OLD.amount)); end if; end; alter trigger "SYSTEM"."OQT_TRG_MEG_CUD" enable"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);
        assert!(
            formatted.contains("\n    AFTER INSERT OR UPDATE OR DELETE ON oqt_meg_master"),
            "Trigger timing/event header should stay on one indented line, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("\n    FOR EACH ROW\nBEGIN"),
            "FOR EACH ROW should align with trigger header indentation, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("IF INSERTING THEN")
                && formatted.contains("ELSIF UPDATING THEN")
                && formatted.contains("ELSIF DELETING THEN"),
            "Conditional trigger predicates should be uppercased in IF/ELSIF blocks, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("END;\nALTER TRIGGER \"SYSTEM\".\"OQT_TRG_MEG_CUD\" ENABLE;"),
            "CREATE/ALTER trigger pair should not be separated by a blank line, got:\n{}",
            formatted
        );
    }

    #[test]
    fn preserve_selected_text_terminator_does_not_add_semicolon_when_selection_had_none() {
        let source = "SELECT 1 FROM dual";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);
        assert_eq!(
            preserved.trim_end(),
            "SELECT 1
FROM DUAL"
        );
        assert!(!preserved.trim_end().ends_with(';'));
    }

    #[test]
    fn preserve_selected_text_terminator_removes_inserted_semicolon_before_trailing_comment() {
        let source = "SELECT 1 FROM dual -- trailing note";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);
        assert!(
            !SqlEditorWidget::statement_ends_with_semicolon(&preserved),
            "Semicolon should be removed when original selection had no terminator, got:
{}",
            preserved
        );
        assert!(
            preserved.trim_end().ends_with("-- trailing note"),
            "Trailing comment should be preserved, got:
{}",
            preserved
        );
    }

    #[test]
    fn preserve_selected_text_terminator_removes_inserted_semicolon_when_string_has_comment_markers(
    ) {
        let source = "SELECT '-- keep literal' AS txt FROM dual";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);

        assert!(
            !SqlEditorWidget::statement_ends_with_semicolon(&preserved),
            "Semicolon should be removed when original selection had no terminator, got:\n{}",
            preserved
        );
        assert!(
            preserved.contains("'-- keep literal'"),
            "String literal containing comment markers should be preserved, got:\n{}",
            preserved
        );
    }

    #[test]
    fn preserve_selected_text_terminator_handles_multibyte_text_before_comment() {
        let formatted = "SELECT '한글' FROM dual;".to_string();
        let without_semicolon = SqlEditorWidget::remove_trailing_statement_semicolon(&formatted)
            .expect("trailing semicolon should be removable");
        assert_eq!(without_semicolon, "SELECT '한글' FROM dual");
    }

    #[test]
    fn preserve_selected_text_terminator_does_not_remove_semicolon_inside_string_literal() {
        let source = "SELECT 'a;b' AS txt FROM dual";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);

        assert!(
            preserved.contains("'a;b'"),
            "Semicolon inside string literal must remain unchanged, got:\n{}",
            preserved
        );
        assert!(
            !SqlEditorWidget::statement_ends_with_semicolon(&preserved),
            "Formatter should not append semicolon when original selection had none, got:\n{}",
            preserved
        );
    }

    #[test]
    fn preserve_selected_text_terminator_keeps_semicolon_when_selection_had_one() {
        let source = "SELECT 1 FROM dual;";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);
        assert!(preserved.trim_end().ends_with(';'));
    }

    #[test]
    fn preserve_selected_text_terminator_respects_trailing_comment_after_semicolon() {
        let source = "SELECT 1 FROM dual; -- keep terminator";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);
        assert!(
            preserved.trim_end().ends_with("-- keep terminator"),
            "Trailing comment should be preserved, got:
{}",
            preserved
        );
        assert!(
            SqlEditorWidget::statement_ends_with_semicolon(&preserved),
            "Semicolon should remain when selection already ended with semicolon before comment, got:
{}",
            preserved
        );
    }

    #[test]
    fn preserve_selected_text_terminator_ignores_semicolon_inside_trailing_comment() {
        let source = "SELECT 1 FROM dual -- existing; comment semicolon";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);
        assert!(
            preserved
                .trim_end()
                .ends_with("-- existing; comment semicolon"),
            "Trailing comment text should remain unchanged, got:\n{}",
            preserved
        );
        assert_eq!(
            preserved.matches(';').count(),
            1,
            "No extra semicolon should be appended when source had only comment semicolon, got:\n{}",
            preserved
        );
        assert!(
            !SqlEditorWidget::statement_ends_with_semicolon(&preserved),
            "Statement terminator should stay absent, got:\n{}",
            preserved
        );
    }

    #[test]
    fn preserve_selected_text_terminator_keeps_semicolon_in_comment_only_selection() {
        let source = "-- existing; comment semicolon";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);
        assert_eq!(
            preserved, "-- existing; comment semicolon",
            "Semicolon inside comment-only selections should not be removed"
        );
    }

    #[test]
    fn preserve_selected_text_terminator_keeps_semicolon_in_sqlplus_remark_comment() {
        let source = "REM existing; comment semicolon";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);
        assert_eq!(
            preserved, source,
            "Semicolon inside SQL*Plus remark comment should stay untouched, got:\n{}",
            preserved
        );
    }

    #[test]
    fn format_sql_basic_preserves_sqlplus_remark_comment_text_case() {
        let sql = "REMARK Keep MixedCase ; punctuation";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert_eq!(formatted, sql);
    }

    #[test]
    fn format_sql_basic_preserves_sqlplus_rem_comments_between_statements() {
        let sql = "REM keep this exact comment\nSELECT 1 FROM dual;\nREMARK Keep;This;Too";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("REM keep this exact comment"),
            "{}",
            formatted
        );
        assert!(formatted.contains("REMARK Keep;This;Too"), "{}", formatted);
        assert!(formatted.contains("SELECT 1\nFROM DUAL;"), "{}", formatted);
    }

    #[test]
    fn format_tool_command_accept_escapes_single_quote_prompt() {
        let rendered = SqlEditorWidget::format_tool_command(&crate::db::ToolCommand::Accept {
            name: "v_name".to_string(),
            prompt: Some("Owner's value?".to_string()),
        });

        assert_eq!(rendered, "ACCEPT v_name PROMPT 'Owner''s value?'");
    }

    #[test]
    fn statement_ends_with_semicolon_ignores_sqlplus_remark_comment_text() {
        assert!(!SqlEditorWidget::statement_ends_with_semicolon(
            "REM only a comment"
        ));
        assert!(!SqlEditorWidget::statement_ends_with_semicolon(
            "REMARK this is a comment with ; semicolon"
        ));
    }

    #[test]
    fn format_statement_preserves_compound_trigger_timing_end_qualifier() {
        let sql = r#"CREATE OR REPLACE TRIGGER test_compound_trg
  FOR INSERT ON test_table
  COMPOUND TRIGGER
    BEFORE EACH ROW IS
    BEGIN
      :NEW.status := 'new';
    END BEFORE EACH ROW;
    AFTER STATEMENT IS
    BEGIN
      NULL;
    END AFTER STATEMENT;
  END test_compound_trg;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);
        assert!(
            formatted.contains("END BEFORE EACH ROW;"),
            "Compound trigger BEFORE timing qualifier should be preserved, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("END AFTER STATEMENT;"),
            "Compound trigger AFTER timing qualifier should be preserved, got:\n{}",
            formatted
        );
    }

    #[test]
    fn formats_values_subquery_with_nested_depth() {
        let sql = "SELECT 1 FROM dual WHERE EXISTS (VALUES (1));";
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        let expected = [
            "SELECT 1",
            "FROM DUAL",
            "WHERE EXISTS (",
            "        VALUES (1)",
            "    );",
        ]
        .join("\n");

        assert_eq!(formatted, expected);
    }

    #[test]
    fn formats_deeply_nested_subqueries_with_consistent_depth() {
        let sql = "SELECT outer_col FROM outer_t o WHERE EXISTS (SELECT 1 FROM (SELECT inner_col FROM inner_t i WHERE i.id IN (SELECT id FROM leaf_t WHERE status = 'Y')) nested_q WHERE nested_q.inner_col = o.outer_col);";
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        let expected = [
            "SELECT outer_col",
            "FROM outer_t o",
            "WHERE EXISTS (",
            "        SELECT 1",
            "        FROM (",
            "                SELECT inner_col",
            "                FROM inner_t i",
            "                WHERE i.id IN (",
            "                        SELECT id",
            "                        FROM leaf_t",
            "                        WHERE status = 'Y'",
            "                    )",
            "            ) nested_q",
            "        WHERE nested_q.inner_col = o.outer_col",
            "    );",
        ]
        .join("\n");

        assert_eq!(formatted, expected);
    }

    #[test]
    fn formats_nested_union_subquery_with_consistent_depth() {
        let sql = "SELECT o.id FROM outer_t o WHERE EXISTS (SELECT 1 FROM (SELECT i.id FROM inner_a i WHERE i.flag = 'Y' UNION ALL SELECT j.id FROM inner_b j WHERE j.flag = 'N') merged WHERE merged.id = o.id);";
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        let expected = [
            "SELECT o.id",
            "FROM outer_t o",
            "WHERE EXISTS (",
            "        SELECT 1",
            "        FROM (",
            "                SELECT i.id",
            "                FROM inner_a i",
            "                WHERE i.flag = 'Y'",
            "                UNION ALL",
            "                SELECT j.id",
            "                FROM inner_b j",
            "                WHERE j.flag = 'N'",
            "            ) merged",
            "        WHERE merged.id = o.id",
            "    );",
        ]
        .join("\n");

        assert_eq!(formatted, expected);
    }

    #[test]
    fn plsql_nested_with_subquery_keeps_cte_body_depth() {
        let sql = r#"BEGIN
  SELECT o.id
  INTO v_id
  FROM outer_t o
  WHERE EXISTS (
    WITH filt AS (
      SELECT id
      FROM inner_t
      WHERE flag = 'Y'
    )
    SELECT id
    FROM filt
    WHERE filt.id = o.id
  );
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("WITH filt AS (
                SELECT id
                FROM inner_t
                WHERE flag = 'Y'
            )"),
            "CTE body inside nested subquery should indent one level deeper than WITH header, got:
{}",
            formatted
        );
    }



    #[test]
    fn plsql_nested_union_subquery_keeps_consistent_depth() {
        let sql = r#"BEGIN
  SELECT o.id
  INTO v_id
  FROM outer_t o
  WHERE EXISTS (
    SELECT 1
    FROM (
      SELECT i.id
      FROM inner_a i
      WHERE i.flag = 'Y'
      UNION ALL
      SELECT j.id
      FROM inner_b j
      WHERE j.flag = 'N'
    ) merged
    WHERE merged.id = o.id
  );
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("FROM (
                SELECT i.id"),
            "Nested subquery under FROM should increase depth, got:
{}",
            formatted
        );
        assert!(
            formatted.contains("UNION ALL
                SELECT j.id"),
            "Set operator branch inside nested subquery should keep same nested depth, got:
{}",
            formatted
        );
    }


    #[test]
    fn plsql_nested_with_multiple_ctes_keeps_cte_depth_aligned() {
        let sql = r#"BEGIN
  SELECT o.id
  INTO v_id
  FROM outer_t o
  WHERE EXISTS (
    WITH a AS (
      SELECT id
      FROM inner_t
    ), b AS (
      SELECT id
      FROM a
    )
    SELECT id
    FROM b
    WHERE b.id = o.id
  );
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        let expected = [
            "BEGIN",
            "    SELECT o.id",
            "    INTO v_id",
            "    FROM outer_t o",
            "    WHERE EXISTS (",
            "            WITH a AS (",
            "                SELECT id",
            "                FROM inner_t",
            "            ),",
            "            b AS (",
            "                SELECT id",
            "                FROM a",
            "            )",
            "            SELECT id",
            "            FROM b",
            "            WHERE b.id = o.id",
            "        );",
            "END;",
        ]
        .join("\n");

        assert_eq!(formatted, expected);
    }


    #[test]
    fn plsql_nested_with_clause_resets_excess_manual_indent() {
        let sql = r#"BEGIN
  SELECT o.id
  INTO v_id
  FROM outer_t o
  WHERE EXISTS (
                    WITH filt AS (
      SELECT id
      FROM inner_t
      WHERE flag = 'Y'
    )
    SELECT id
    FROM filt
    WHERE filt.id = o.id
  );
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("WHERE EXISTS (
            WITH filt AS ("),
            "WITH clause should align to nested query depth instead of preserving excess manual indent, got:
{}",
            formatted
        );
    }


    #[test]
    fn plsql_nested_with_clause_does_not_keep_two_level_extra_indent() {
        let sql = r#"BEGIN
    SELECT o.id
    INTO v_id
    FROM outer_t o
    WHERE EXISTS (
                WITH filt AS (
                    SELECT id
                    FROM inner_t
                    WHERE flag = 'Y'
                )
            SELECT id
            FROM filt
            WHERE filt.id = o.id
        );
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("WHERE EXISTS (
            WITH filt AS ("),
            "WITH clause should not keep two-level extra indent in nested DML depth, got:
{}",
            formatted
        );
    }
    #[test]
    fn formats_nested_with_subquery_with_consistent_depth() {
        let sql = "SELECT o.id FROM outer_t o WHERE EXISTS (WITH filt AS (SELECT id FROM inner_t WHERE flag = 'Y') SELECT id FROM filt WHERE filt.id = o.id);";
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        let expected = [
            "SELECT o.id",
            "FROM outer_t o",
            "WHERE EXISTS (",
            "        WITH filt AS (",
            "            SELECT id",
            "            FROM inner_t",
            "            WHERE flag = 'Y'",
            "        )",
            "        SELECT id",
            "        FROM filt",
            "        WHERE filt.id = o.id",
            "    );",
        ]
        .join("\n");

        assert_eq!(formatted, expected);
    }
}

#[cfg(test)]
mod query_execution_cleanup_tests {
    use super::{QueryExecutionCleanupGuard, QueryProgress};
    use oracle::Connection;
    use std::panic::{self, AssertUnwindSafe};
    use std::sync::{mpsc, Arc, Mutex};

    #[test]
    fn cleanup_guard_resets_cancel_and_emits_batch_finished_on_drop() {
        let (sender, receiver) = mpsc::channel();
        let cancel_flag = Arc::new(Mutex::new(true));
        let query_running = Arc::new(Mutex::new(true));
        let current_query_connection: Arc<Mutex<Option<Arc<Connection>>>> =
            Arc::new(Mutex::new(None));

        {
            let _guard = QueryExecutionCleanupGuard::new(
                sender,
                current_query_connection.clone(),
                cancel_flag.clone(),
                query_running.clone(),
            );
        }

        assert!(!cancel_flag
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .to_owned());
        assert!(!query_running
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .to_owned());
        let msg = receiver
            .try_recv()
            .expect("BatchFinished should be emitted");
        assert!(matches!(msg, QueryProgress::BatchFinished));
        assert!(current_query_connection
            .lock()
            .expect("connection mutex should not be poisoned")
            .is_none());
    }

    #[test]
    fn cleanup_guard_runs_during_panic_unwind() {
        let (sender, receiver) = mpsc::channel();
        let cancel_flag = Arc::new(Mutex::new(true));
        let query_running = Arc::new(Mutex::new(true));
        let current_query_connection: Arc<Mutex<Option<Arc<Connection>>>> =
            Arc::new(Mutex::new(None));

        let unwind_result = panic::catch_unwind(AssertUnwindSafe({
            let cancel_flag = cancel_flag.clone();
            let current_query_connection = current_query_connection;
            let query_running = query_running.clone();
            move || {
                let _guard = QueryExecutionCleanupGuard::new(
                    sender,
                    current_query_connection,
                    cancel_flag,
                    query_running,
                );
                panic!("simulate execution panic");
            }
        }));

        assert!(unwind_result.is_err());
        assert!(!cancel_flag
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .to_owned());
        assert!(!query_running
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .to_owned());
        let msg = receiver
            .try_recv()
            .expect("BatchFinished should be emitted");
        assert!(matches!(msg, QueryProgress::BatchFinished));
    }

    #[test]
    fn cleanup_guard_drop_tolerates_closed_progress_channel() {
        let (sender, receiver) = mpsc::channel();
        drop(receiver);

        let cancel_flag = Arc::new(Mutex::new(true));
        let query_running = Arc::new(Mutex::new(true));
        let current_query_connection: Arc<Mutex<Option<Arc<Connection>>>> =
            Arc::new(Mutex::new(None));

        let drop_result = panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = QueryExecutionCleanupGuard::new(
                sender,
                current_query_connection,
                cancel_flag.clone(),
                query_running.clone(),
            );
        }));

        assert!(drop_result.is_ok(), "Drop must ignore send failures");
        assert!(!cancel_flag
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .to_owned());
        assert!(!query_running
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .to_owned());
    }

    #[test]
    fn cleanup_guard_recovers_from_poisoned_connection_mutex() {
        let (sender, receiver) = mpsc::channel();
        let cancel_flag = Arc::new(Mutex::new(true));
        let query_running = Arc::new(Mutex::new(true));
        let current_query_connection: Arc<Mutex<Option<Arc<Connection>>>> =
            Arc::new(Mutex::new(None));

        let poison_target = current_query_connection.clone();
        let _ = panic::catch_unwind(AssertUnwindSafe(move || {
            let _lock = poison_target
                .lock()
                .expect("mutex lock should succeed before poisoning");
            panic!("poison current_query_connection mutex");
        }));

        {
            let _guard = QueryExecutionCleanupGuard::new(
                sender,
                current_query_connection,
                cancel_flag.clone(),
                query_running.clone(),
            );
        }

        assert!(!cancel_flag
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .to_owned());
        assert!(!query_running
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .to_owned());
        let msg = receiver
            .try_recv()
            .expect("BatchFinished should be emitted");
        assert!(matches!(msg, QueryProgress::BatchFinished));
    }
}

#[cfg(test)]
mod script_include_guard_tests {
    use super::{ScriptExecutionFrame, SqlEditorWidget, MAX_SCRIPT_INCLUDE_DEPTH};
    use std::path::PathBuf;

    fn frame_with_source(path: &str) -> ScriptExecutionFrame {
        ScriptExecutionFrame {
            items: Vec::new(),
            index: 0,
            base_dir: PathBuf::from("."),
            source_path: Some(PathBuf::from(path)),
        }
    }

    #[test]
    fn validate_script_include_target_rejects_recursive_include() {
        let frames = vec![frame_with_source("nested.sql")];
        let candidate = PathBuf::from("nested.sql");

        let err = SqlEditorWidget::validate_script_include_target(&frames, candidate.as_path())
            .expect_err("recursive include should be rejected");
        assert!(
            err.contains("Recursive script include detected"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn validate_script_include_target_rejects_depth_over_limit() {
        let mut frames = Vec::new();
        for i in 0..MAX_SCRIPT_INCLUDE_DEPTH {
            frames.push(frame_with_source(&format!("script_{i}.sql")));
        }

        let candidate = PathBuf::from("script_overflow.sql");
        let err = SqlEditorWidget::validate_script_include_target(&frames, candidate.as_path())
            .expect_err("depth overflow should be rejected");
        assert!(
            err.contains("Maximum nested script depth"),
            "unexpected error message: {err}"
        );
    }
}

#[cfg(test)]
mod execution_startup_error_tests {
    use super::SqlEditorWidget;
    use crate::ui::sql_editor::QueryProgress;
    use std::sync::mpsc;

    #[test]
    fn emit_execution_startup_error_reports_statement_finished_for_single_sql() {
        let (tx, rx) = mpsc::channel();

        SqlEditorWidget::emit_execution_startup_error(
            &tx,
            false,
            "select 1 from dual",
            "DEV",
            "startup failed",
            None,
        );

        let progress = rx
            .recv()
            .unwrap_or_else(|err| panic!("expected statement error progress event: {err}"));

        match progress {
            QueryProgress::StatementFinished {
                index,
                result,
                connection_name,
                timed_out,
            } => {
                assert_eq!(index, 0);
                assert_eq!(connection_name, "DEV");
                assert!(!timed_out);
                assert_eq!(result.sql, "select 1 from dual");
                assert_eq!(result.message, "Error: startup failed");
                assert!(!result.success);
            }
            _ => panic!("expected StatementFinished progress event"),
        }
    }

    #[test]
    fn emit_execution_startup_error_reports_script_result_in_script_mode() {
        let (tx, rx) = mpsc::channel();

        SqlEditorWidget::emit_execution_startup_error(
            &tx,
            true,
            "begin null; end;",
            "DEV",
            "script startup failed",
            None,
        );

        let progress = rx
            .recv()
            .unwrap_or_else(|err| panic!("expected script error progress event: {err}"));

        match progress {
            QueryProgress::StatementFinished {
                index,
                result,
                connection_name,
                timed_out,
            } => {
                assert_eq!(index, 0);
                assert_eq!(connection_name, "DEV");
                assert!(!timed_out);
                assert_eq!(result.sql, "begin null; end;");
                assert_eq!(result.message, "Error: script startup failed");
                assert!(!result.success);
            }
            _ => panic!("expected StatementFinished progress event"),
        }
    }
}

#[cfg(test)]
mod disconnected_precheck_gate_tests {
    use super::SqlEditorWidget;

    #[test]
    fn precheck_requires_connection_for_non_bootstrap_db_work() {
        assert!(SqlEditorWidget::requires_connected_session_for_precheck(
            false, false
        ));
    }

    #[test]
    fn precheck_allows_connect_bootstrap_while_disconnected() {
        assert!(!SqlEditorWidget::requires_connected_session_for_precheck(
            true, false
        ));
    }

    #[test]
    fn precheck_allows_local_only_commands_while_disconnected() {
        assert!(!SqlEditorWidget::requires_connected_session_for_precheck(
            false, true
        ));
    }

    #[test]
    fn execution_startup_policy_marks_bootstrap_queries_as_disconnected_safe() {
        let policy = SqlEditorWidget::execution_startup_policy("connect user/pass@db");

        assert!(policy.has_connect_command);
        assert!(!policy.requires_connected_session);
    }

    #[test]
    fn execution_startup_policy_requires_connection_for_regular_sql() {
        let policy = SqlEditorWidget::execution_startup_policy("select * from dual");

        assert!(!policy.has_connect_command);
        assert!(policy.requires_connected_session);
    }
}

#[cfg(test)]
mod print_bind_state_tests {
    use super::{PrintNamedData, SqlEditorWidget};
    use crate::db::{BindDataType, BindValue, BindVar, ColumnInfo, CursorResult, SessionState};

    #[test]
    fn clone_print_named_data_preserves_refcursor_in_session() {
        let mut session = SessionState::default();
        session.binds.insert(
            "V_RC".to_string(),
            BindVar {
                data_type: BindDataType::RefCursor,
                value: BindValue::Cursor(Some(CursorResult {
                    columns: vec!["EMPNO".to_string()],
                    rows: vec![vec!["7369".to_string()]],
                })),
            },
        );

        let named = SqlEditorWidget::clone_print_named_data(&session, "V_RC");

        match named {
            PrintNamedData::Cursor(cursor) => {
                assert_eq!(cursor.columns, vec!["EMPNO".to_string()]);
                assert_eq!(cursor.rows, vec![vec!["7369".to_string()]]);
            }
            _ => panic!("expected cursor print data"),
        }

        match session.binds.get("V_RC").map(|bind| &bind.value) {
            Some(BindValue::Cursor(Some(cursor))) => {
                assert_eq!(cursor.columns, vec!["EMPNO".to_string()]);
                assert_eq!(cursor.rows, vec![vec!["7369".to_string()]]);
            }
            _ => panic!("expected refcursor to remain in session after PRINT clone"),
        }
    }

    #[test]
    fn collect_print_all_data_preserves_cursor_results() {
        let mut session = SessionState::default();
        session.binds.insert(
            "V_RC".to_string(),
            BindVar {
                data_type: BindDataType::RefCursor,
                value: BindValue::Cursor(Some(CursorResult {
                    columns: vec!["ENAME".to_string()],
                    rows: vec![vec!["SMITH".to_string()]],
                })),
            },
        );

        let (summary_rows, cursor_results) = SqlEditorWidget::collect_print_all_data(&session, "");

        assert_eq!(summary_rows.len(), 1);
        assert_eq!(
            summary_rows[0],
            vec![
                "V_RC".to_string(),
                BindDataType::RefCursor.display(),
                "REFCURSOR (1 rows)".to_string(),
            ]
        );
        assert_eq!(cursor_results.len(), 1);
        assert_eq!(cursor_results[0].0, "V_RC".to_string());
        assert_eq!(cursor_results[0].1.columns, vec!["ENAME".to_string()]);
        assert_eq!(cursor_results[0].1.rows, vec![vec!["SMITH".to_string()]]);

        match session.binds.get("V_RC").map(|bind| &bind.value) {
            Some(BindValue::Cursor(Some(cursor))) => {
                assert_eq!(cursor.columns, vec!["ENAME".to_string()]);
                assert_eq!(cursor.rows, vec![vec!["SMITH".to_string()]]);
            }
            _ => panic!("expected refcursor to remain in session after PRINT ALL snapshot"),
        }
    }

    #[test]
    fn cursor_result_column_names_preserve_raw_headers_for_later_print() {
        let columns = vec![
            ColumnInfo {
                name: "EMPNO".to_string(),
                data_type: "Number".to_string(),
            },
            ColumnInfo {
                name: "ENAME".to_string(),
                data_type: "Varchar2".to_string(),
            },
        ];

        assert_eq!(
            SqlEditorWidget::cursor_result_column_names(&columns),
            vec!["EMPNO".to_string(), "ENAME".to_string()]
        );
    }
}

#[cfg(test)]
mod query_running_reservation_tests {
    use super::QueryRunningReservation;
    use std::sync::{Arc, Mutex};

    #[test]
    fn reservation_drop_releases_query_running_flag() {
        let query_running = Arc::new(Mutex::new(false));

        {
            let _reservation = QueryRunningReservation::acquire(query_running.clone());
            assert!(_reservation.is_some(), "query flag should be reservable");
            let running = query_running
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            assert!(*running);
        }

        let running = query_running
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(!*running);
    }

    #[test]
    fn reservation_disarm_keeps_query_running_flag_set() {
        let query_running = Arc::new(Mutex::new(false));

        {
            let mut reservation = QueryRunningReservation::acquire(query_running.clone());
            assert!(reservation.is_some(), "query flag should be reservable");
            if let Some(active) = reservation.as_mut() {
                active.disarm();
            }
        }

        let running = query_running
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(*running);
    }
}
