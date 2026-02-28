use fltk::{
    app,
    draw::set_cursor,
    enums::{Cursor, FrameType},
    frame::Frame,
    group::{Flex, FlexType},
    input::IntInput,
    prelude::*,
    text::{TextBuffer, TextEditor, WrapMode},
    window::Window,
};
use std::any::Any;
use std::collections::VecDeque;
use std::panic::{self, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex, OnceLock};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use crate::db::{ConnectionInfo, QueryExecutor, QueryResult, SharedConnection, TableColumnDetail};
use crate::ui::constants::*;
use crate::ui::font_settings::{configured_editor_profile, configured_ui_font_size, FontProfile};
use crate::ui::intellisense::{IntellisenseData, IntellisensePopup};
use crate::ui::query_history::{flush_history_writer_with_timeout, QueryHistoryDialog};
#[cfg(test)]
use crate::ui::syntax_highlight::STYLE_DEFAULT;
use crate::ui::syntax_highlight::{
    create_style_table_with, HighlightData, SqlHighlighter, STYLE_COMMENT, STYLE_STRING,
};
use crate::ui::theme;
use crate::utils::{AppConfig, QueryHistory, QueryHistoryEntry};
use oracle::Connection;

mod dba_tools;
mod execution;
mod intellisense;
// 공통 파싱/토큰 유틸(실행, 인텔리센스, 포맷팅 공통 경로)
pub(crate) mod query_text;
mod session_monitor;

#[derive(Clone, Debug)]
pub(crate) enum SqlToken {
    Word(String),
    String(String),
    Comment(String),
    Symbol(String),
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct SqlTokenSpan {
    pub token: SqlToken,
    pub start: usize,
    pub end: usize,
}

const INTELLISENSE_WORD_WINDOW: i32 = 256;
const INTELLISENSE_CONTEXT_WINDOW: i32 = 120_000;
const INTELLISENSE_QUALIFIER_WINDOW: i32 = 256;
const INTELLISENSE_STATEMENT_WINDOW: i32 = 120_000;
const MAX_PROGRESS_MESSAGES_PER_POLL: usize = 8000;
const PROGRESS_POLL_ACTIVE_INTERVAL_SECONDS: f64 = 0.001;
const PROGRESS_POLL_INTERVAL_SECONDS: f64 = 0.05;
const MAX_WORD_UNDO_HISTORY: usize = 500;
const MAX_WORD_UNDO_HISTORY_BYTES: usize = 64 * 1024 * 1024;
const HIGHLIGHT_RANGE_EXPANSION_WINDOW: usize = 4096;
/// Maximum scan radius for stateful delimiter changes (e.g. `'`, `/*`).
/// Replaces the old full-buffer scan with a bounded 128 KB window.
const STATEFUL_DELIMITER_SCAN_RADIUS: usize = 65_536;
const VIEWPORT_HIGHLIGHT_POLL_INTERVAL_SECONDS: f64 = 0.08;
const EDITOR_TOP_PADDING: i32 = 4;
const HISTORY_NAVIGATION_FLUSH_TIMEOUT: Duration = Duration::from_millis(200);
const ALERT_RETRY_INTERVAL_SECONDS: f64 = 0.25;

fn is_window_shown_and_visible(shown: bool, visible: bool) -> bool {
    shown && visible
}

fn update_alert_pump_state_after_display(queue_is_empty: bool, pump_scheduled: &mut bool) -> bool {
    if queue_is_empty {
        *pump_scheduled = false;
        false
    } else {
        *pump_scheduled = true;
        true
    }
}

fn load_mutex_bool(flag: &Arc<Mutex<bool>>) -> bool {
    match flag.lock() {
        Ok(guard) => *guard,
        Err(poisoned) => *poisoned.into_inner(),
    }
}

fn store_mutex_bool(flag: &Arc<Mutex<bool>>, value: bool) {
    match flag.lock() {
        Ok(mut guard) => *guard = value,
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            *guard = value;
        }
    }
}

#[derive(Default)]
struct PendingAlertState {
    queue: VecDeque<String>,
    pump_scheduled: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum EditGranularity {
    Word,
    Other,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum EditOperation {
    Insert,
    Delete,
    Replace,
    Other,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct EditGroup {
    granularity: EditGranularity,
    operation: EditOperation,
}

#[derive(Clone, Debug)]
struct BufferEdit {
    start: usize,
    deleted_len: usize,
    inserted_text: String,
    deleted_text: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct UndoSnapshot {
    text: String,
    cursor_pos: usize,
}

impl UndoSnapshot {
    fn new(text: String, cursor_pos: usize) -> Self {
        Self { text, cursor_pos }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct UndoDelta {
    start: usize,
    deleted_text: String,
    inserted_text: String,
    before_cursor: usize,
    after_cursor: usize,
    group_id: u64,
}

#[derive(Clone)]
struct WordUndoRedoState {
    anchor: UndoSnapshot,
    current: UndoSnapshot,
    deltas: Vec<UndoDelta>,
    history_total_bytes: usize,
    index: usize,
    active_group: Option<(EditGroup, u64)>,
    next_group_id: u64,
    applying_history: bool,
}

impl WordUndoRedoState {
    fn new(initial_text: String) -> Self {
        let initial_cursor = initial_text.len();
        let initial_snapshot = UndoSnapshot::new(initial_text, initial_cursor);
        Self {
            anchor: initial_snapshot.clone(),
            current: initial_snapshot,
            deltas: Vec::new(),
            history_total_bytes: 0,
            index: 0,
            active_group: None,
            next_group_id: 1,
            applying_history: false,
        }
    }

    fn normalize_index(&mut self) {
        if self.index > self.deltas.len() {
            self.index = self.deltas.len();
            self.active_group = None;
        }
        self.current.cursor_pos =
            Self::clamp_to_char_boundary(&self.current.text, self.current.cursor_pos);
    }

    #[cfg(test)]
    fn current_snapshot_matches(&self, current_text: &str) -> bool {
        self.current.text == current_text
    }

    fn clamp_to_char_boundary(text: &str, idx: usize) -> usize {
        let mut idx = idx.min(text.len());
        while idx > 0 && !text.is_char_boundary(idx) {
            idx -= 1;
        }
        idx
    }

    fn normalized_replace_range(text: &str, edit: &BufferEdit) -> (usize, usize) {
        let replace_start = Self::clamp_to_char_boundary(text, edit.start);
        let delete_end = replace_start
            .saturating_add(edit.deleted_len)
            .min(text.len());
        let replace_end = Self::clamp_to_char_boundary(text, delete_end).max(replace_start);
        (replace_start, replace_end)
    }

    fn apply_edit_to_snapshot(snapshot: &mut UndoSnapshot, edit: &BufferEdit) {
        let (replace_start, replace_end) = Self::normalized_replace_range(&snapshot.text, edit);
        snapshot
            .text
            .replace_range(replace_start..replace_end, &edit.inserted_text);
        let cursor = replace_start
            .saturating_add(edit.inserted_text.len())
            .min(snapshot.text.len());
        snapshot.cursor_pos = Self::clamp_to_char_boundary(&snapshot.text, cursor);
    }

    fn apply_delta_to_snapshot(snapshot: &mut UndoSnapshot, delta: &UndoDelta, reverse: bool) {
        let delete_len = if reverse {
            delta.inserted_text.len()
        } else {
            delta.deleted_text.len()
        };
        let edit = BufferEdit {
            start: delta.start,
            deleted_len: delete_len,
            inserted_text: if reverse {
                delta.deleted_text.clone()
            } else {
                delta.inserted_text.clone()
            },
            deleted_text: if reverse {
                delta.inserted_text.clone()
            } else {
                delta.deleted_text.clone()
            },
        };
        Self::apply_edit_to_snapshot(snapshot, &edit);
        let cursor = if reverse {
            delta.before_cursor
        } else {
            delta.after_cursor
        };
        snapshot.cursor_pos = Self::clamp_to_char_boundary(&snapshot.text, cursor);
    }

    fn should_merge_into_active_group(&self, edit_group: EditGroup, edit: &BufferEdit) -> bool {
        let Some((active_group, _)) = self.active_group else {
            return false;
        };

        // Group contiguous "word" edits together regardless of low-level operation
        // (insert/delete/replace). This keeps IME composition updates as one word step.
        if active_group.granularity != EditGranularity::Word
            || edit_group.granularity != EditGranularity::Word
            || active_group.operation == EditOperation::Other
            || edit_group.operation == EditOperation::Other
        {
            return false;
        }

        if edit.inserted_text.contains('\n') {
            return false;
        }

        let current_cursor = self.current.cursor_pos;
        let current_text = self.current.text.as_str();
        let (edit_start, edit_end) = Self::normalized_replace_range(current_text, edit);

        let near_current_cursor = edit_start <= current_cursor.saturating_add(12)
            && current_cursor <= edit_end.saturating_add(12);
        let deleted_size = edit.deleted_len.max(edit.deleted_text.len());
        let small_edit = deleted_size <= 24 && edit.inserted_text.len() <= 48;
        if !near_current_cursor || !small_edit {
            return false;
        }

        if !Self::is_same_line(current_text, current_cursor, edit_start)
            || !Self::is_same_line(current_text, current_cursor, edit_end)
        {
            return false;
        }

        let Some((word_start, word_end)) =
            Self::word_span_touching_offset(current_text, current_cursor)
        else {
            // IME composition can briefly remove the in-progress syllable,
            // leaving no identifier under the cursor for one callback.
            return edit_start == current_cursor;
        };
        if !Self::edit_touches_word_span(edit_start, edit_end, word_start, word_end) {
            return false;
        }
        true
    }

    fn is_same_line(text: &str, left: usize, right: usize) -> bool {
        if text.is_empty() {
            return true;
        }

        let left = Self::clamp_to_char_boundary(text, left.min(text.len()));
        let right = Self::clamp_to_char_boundary(text, right.min(text.len()));
        let (start, end) = if left <= right {
            (left, right)
        } else {
            (right, left)
        };
        !text.as_bytes()[start..end].contains(&b'\n')
    }

    fn truncate_redo_history(&mut self) {
        if self.index >= self.deltas.len() {
            return;
        }

        let removed_bytes: usize = self.deltas[self.index..]
            .iter()
            .map(|delta| {
                delta
                    .deleted_text
                    .len()
                    .saturating_add(delta.inserted_text.len())
            })
            .sum();
        self.deltas.truncate(self.index);
        self.history_total_bytes = self.history_total_bytes.saturating_sub(removed_bytes);
        self.active_group = None;
    }

    fn effective_history_byte_limit(&self) -> usize {
        MAX_WORD_UNDO_HISTORY_BYTES.max(self.current.text.len().saturating_mul(2))
    }

    fn trim_history_if_needed(&mut self) {
        let byte_limit = self.effective_history_byte_limit();
        while self.deltas.len() > 1
            && (self.deltas.len() > MAX_WORD_UNDO_HISTORY || self.history_total_bytes > byte_limit)
        {
            let removed = self.deltas.remove(0);
            let removed_len = removed
                .deleted_text
                .len()
                .saturating_add(removed.inserted_text.len());
            self.history_total_bytes = self.history_total_bytes.saturating_sub(removed_len);
            if self.index > 0 {
                Self::apply_delta_to_snapshot(&mut self.anchor, &removed, false);
                self.index = self.index.saturating_sub(1);
            }
        }

        if self.index > self.deltas.len() {
            self.index = self.deltas.len();
        }
        if self.index == 0 {
            self.active_group = None;
        }
    }

    fn word_span_touching_offset(text: &str, pos: usize) -> Option<(usize, usize)> {
        if text.is_empty() {
            return None;
        }

        let pos = Self::clamp_to_char_boundary(text, pos.min(text.len()));

        let anchor = if pos < text.len() {
            let ch = text.get(pos..)?.chars().next()?;
            if is_word_edit_char(ch) {
                Some(pos)
            } else {
                None
            }
        } else {
            None
        }
        .or_else(|| {
            if pos == 0 {
                return None;
            }
            text.get(..pos)
                .and_then(|prefix| prefix.char_indices().next_back())
                .and_then(|(start, ch)| is_word_edit_char(ch).then_some(start))
        })?;

        let mut start = anchor;
        while start > 0 {
            let Some((prev_start, ch)) = text
                .get(..start)
                .and_then(|prefix| prefix.char_indices().next_back())
            else {
                break;
            };
            if is_word_edit_char(ch) {
                start = prev_start;
            } else {
                break;
            }
        }

        let mut end = anchor;
        while end < text.len() {
            let Some(ch) = text.get(end..).and_then(|suffix| suffix.chars().next()) else {
                break;
            };
            if is_word_edit_char(ch) {
                end += ch.len_utf8();
            } else {
                break;
            }
        }

        Some((start, end))
    }

    fn edit_touches_word_span(
        edit_start: usize,
        edit_end: usize,
        word_start: usize,
        word_end: usize,
    ) -> bool {
        if edit_start == edit_end {
            return edit_start >= word_start && edit_start <= word_end;
        }
        edit_start < word_end && edit_end > word_start
    }

    fn next_group_id(&mut self) -> u64 {
        let group_id = self.next_group_id;
        self.next_group_id = self.next_group_id.saturating_add(1);
        group_id
    }

    fn record_edit(&mut self, edit: &BufferEdit, edit_group: EditGroup) {
        self.normalize_index();
        self.truncate_redo_history();

        let before_cursor = self.current.cursor_pos;
        let (replace_start, replace_end) = Self::normalized_replace_range(&self.current.text, edit);
        let deleted_text = self
            .current
            .text
            .get(replace_start..replace_end)
            .map(|text| text.to_string())
            .unwrap_or_else(String::new);
        let normalized_edit = BufferEdit {
            start: replace_start,
            deleted_len: replace_end.saturating_sub(replace_start),
            inserted_text: edit.inserted_text.clone(),
            deleted_text,
        };

        let merge_group = self.should_merge_into_active_group(edit_group, &normalized_edit);
        let group_id = if merge_group {
            self.active_group
                .map(|(_, id)| id)
                .unwrap_or_else(|| self.next_group_id())
        } else {
            self.next_group_id()
        };

        Self::apply_edit_to_snapshot(&mut self.current, &normalized_edit);
        let after_cursor = self.current.cursor_pos;

        let delta = UndoDelta {
            start: replace_start,
            deleted_text: normalized_edit.deleted_text.clone(),
            inserted_text: normalized_edit.inserted_text,
            before_cursor,
            after_cursor,
            group_id,
        };
        self.history_total_bytes = self.history_total_bytes.saturating_add(
            delta
                .deleted_text
                .len()
                .saturating_add(delta.inserted_text.len()),
        );
        self.deltas.push(delta);
        self.index = self.deltas.len();
        self.active_group = Some((edit_group, group_id));
        self.trim_history_if_needed();
    }

    #[cfg(test)]
    fn record_snapshot(&mut self, current_text: String, edit_group: EditGroup) {
        self.normalize_index();
        if self.current_snapshot_matches(&current_text) {
            return;
        }
        let deleted_len = self.current.text.len();
        let deleted_text = self.current.text.clone();
        let edit = BufferEdit {
            start: 0,
            deleted_len,
            inserted_text: current_text,
            deleted_text,
        };
        if self.active_group.map(|(group, _)| group) != Some(edit_group) {
            self.active_group = None;
        }
        self.record_edit(&edit, edit_group);
    }

    #[cfg(test)]
    fn history_snapshots(&self) -> Vec<UndoSnapshot> {
        let mut snapshots = Vec::with_capacity(self.deltas.len().saturating_add(1));
        let mut snapshot = self.anchor.clone();
        snapshots.push(snapshot.clone());
        for (idx, delta) in self.deltas.iter().enumerate() {
            Self::apply_delta_to_snapshot(&mut snapshot, delta, false);
            let next_group = self.deltas.get(idx.saturating_add(1)).map(|d| d.group_id);
            if next_group != Some(delta.group_id) {
                snapshots.push(snapshot.clone());
            }
        }
        snapshots
    }

    #[cfg(test)]
    fn history_texts(&self) -> Vec<String> {
        self.history_snapshots()
            .iter()
            .map(|snapshot| snapshot.text.clone())
            .collect()
    }

    fn take_undo_group(&mut self) -> Vec<UndoDelta> {
        self.normalize_index();
        if self.index == 0 {
            return Vec::new();
        }

        let Some(target_group_id) = self
            .deltas
            .get(self.index.saturating_sub(1))
            .map(|delta| delta.group_id)
        else {
            return Vec::new();
        };

        let mut group = Vec::new();
        while self.index > 0 {
            let Some(delta) = self.deltas.get(self.index.saturating_sub(1)).cloned() else {
                self.index = self.deltas.len();
                self.active_group = None;
                break;
            };
            if delta.group_id != target_group_id {
                break;
            }
            self.index = self.index.saturating_sub(1);
            Self::apply_delta_to_snapshot(&mut self.current, &delta, true);
            group.push(delta);
        }
        if !group.is_empty() {
            self.active_group = None;
            self.applying_history = true;
        }
        group
    }

    fn take_redo_group(&mut self) -> Vec<UndoDelta> {
        self.normalize_index();
        if self.index >= self.deltas.len() {
            return Vec::new();
        }
        let Some(target_group_id) = self.deltas.get(self.index).map(|delta| delta.group_id) else {
            return Vec::new();
        };

        let mut group = Vec::new();
        while self.index < self.deltas.len() {
            let Some(delta) = self.deltas.get(self.index).cloned() else {
                break;
            };
            if delta.group_id != target_group_id {
                break;
            }
            Self::apply_delta_to_snapshot(&mut self.current, &delta, false);
            self.index = self.index.saturating_add(1);
            group.push(delta);
        }
        if !group.is_empty() {
            self.active_group = None;
            self.applying_history = true;
        }
        group
    }
}

#[derive(Clone)]
pub enum QueryProgress {
    BatchStart,
    StatementStart {
        index: usize,
    },
    SelectStart {
        index: usize,
        columns: Vec<String>,
        null_text: String,
    },
    Rows {
        index: usize,
        rows: Vec<Vec<String>>,
    },
    ScriptOutput {
        lines: Vec<String>,
    },
    PromptInput {
        prompt: String,
        response: mpsc::Sender<Option<String>>,
    },
    AutoCommitChanged {
        enabled: bool,
    },
    ConnectionChanged {
        info: Option<ConnectionInfo>,
    },
    StatementFinished {
        index: usize,
        result: QueryResult,
        connection_name: String,
        timed_out: bool,
    },
    BatchFinished,
}

#[derive(Clone)]
pub(crate) struct ColumnLoadUpdate {
    table: String,
    columns: Vec<String>,
    cache_columns: bool,
}

#[derive(Clone)]
pub(crate) struct PendingIntellisense {
    cursor_pos: i32,
}

#[derive(Clone)]
pub(crate) struct IntellisenseParseCacheEntry {
    statement_text: String,
    cursor_in_statement: usize,
    context: crate::ui::intellisense_context::CursorContext,
}

#[derive(Clone)]
pub(crate) enum QuickDescribeData {
    TableColumns(Vec<TableColumnDetail>),
    Text { title: String, content: String },
}

#[derive(Clone)]
enum UiActionResult {
    ExplainPlan(Result<Vec<String>, String>),
    QuickDescribe {
        object_name: String,
        result: Result<QuickDescribeData, String>,
    },
    Commit(Result<(), String>),
    Rollback(Result<(), String>),
    Cancel(Result<(), String>),
    CancelPending,
    QueryAlreadyRunning,
    ConnectionBusy,
}

#[derive(Clone)]
struct HighlightRequest {
    revision: u64,
    generation: u64,
    text: String,
}

#[derive(Clone)]
struct HighlightResult {
    revision: u64,
    generation: u64,
    style_text: String,
}

#[derive(Default)]
struct HighlightQueueState {
    pending_request: Option<HighlightRequest>,
    shutdown: bool,
}

#[derive(Clone)]
pub struct SqlEditorWidget {
    group: Flex,
    editor: TextEditor,
    buffer: TextBuffer,
    style_buffer: TextBuffer,
    connection: SharedConnection,
    execute_callback: Arc<Mutex<Option<Box<dyn FnMut(&QueryResult)>>>>,
    progress_callback: Arc<Mutex<Option<Box<dyn FnMut(QueryProgress)>>>>,
    progress_sender: mpsc::Sender<QueryProgress>,
    column_sender: mpsc::Sender<ColumnLoadUpdate>,
    ui_action_sender: mpsc::Sender<UiActionResult>,
    query_running: Arc<Mutex<bool>>,
    current_query_connection: Arc<Mutex<Option<Arc<Connection>>>>,
    cancel_flag: Arc<Mutex<bool>>,
    intellisense_data: Arc<Mutex<IntellisenseData>>,
    intellisense_popup: Arc<Mutex<IntellisensePopup>>,
    highlighter: Arc<Mutex<SqlHighlighter>>,
    timeout_input: IntInput,
    status_callback: Arc<Mutex<Option<Box<dyn FnMut(&str)>>>>,
    find_callback: Arc<Mutex<Option<Box<dyn FnMut()>>>>,
    replace_callback: Arc<Mutex<Option<Box<dyn FnMut()>>>>,
    file_drop_callback: Arc<Mutex<Option<Box<dyn FnMut(PathBuf)>>>>,
    completion_range: Arc<Mutex<Option<(usize, usize)>>>,
    pending_intellisense: Arc<Mutex<Option<PendingIntellisense>>>,
    intellisense_parse_cache: Arc<Mutex<Option<IntellisenseParseCacheEntry>>>,
    history_cursor: Arc<Mutex<Option<usize>>>,
    history_original: Arc<Mutex<Option<String>>>,
    history_navigation_entries: Arc<Mutex<Option<Vec<QueryHistoryEntry>>>>,
    applying_history_navigation: Arc<Mutex<bool>>,
    undo_redo_state: Arc<Mutex<WordUndoRedoState>>,
    keyup_debounce_generation: Arc<Mutex<u64>>,
    keyup_debounce_handle: Arc<Mutex<Option<app::TimeoutHandle>>>,
    last_explain_plan: Arc<Mutex<Option<Vec<String>>>>,
    highlight_request_state: Arc<(Mutex<HighlightQueueState>, Condvar)>,
    highlight_revision: Arc<AtomicU64>,
    highlight_generation: Arc<AtomicU64>,
    highlight_worker_stopped: Arc<AtomicBool>,
    highlight_worker_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl SqlEditorWidget {
    fn is_main_window_visible() -> bool {
        app::widget_from_id::<Window>("main_window")
            .map(|window| is_window_shown_and_visible(window.shown(), window.visible()))
            .unwrap_or(false)
    }

    fn pending_alert_state() -> &'static Arc<Mutex<PendingAlertState>> {
        static STATE: OnceLock<Arc<Mutex<PendingAlertState>>> = OnceLock::new();
        STATE.get_or_init(|| Arc::new(Mutex::new(PendingAlertState::default())))
    }

    fn schedule_alert_pump(delay_seconds: f64) {
        app::add_timeout3(delay_seconds, move |_| {
            SqlEditorWidget::drain_pending_alerts();
        });
    }

    fn drain_pending_alerts() {
        if !Self::is_main_window_visible() {
            Self::schedule_alert_pump(ALERT_RETRY_INTERVAL_SECONDS);
            return;
        }

        let (maybe_message, should_continue) = {
            let state = Self::pending_alert_state();
            let mut guard = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let message = guard.queue.pop_front();
            let continue_pump = if message.is_some() {
                !guard.queue.is_empty()
            } else {
                guard.pump_scheduled = false;
                false
            };
            (message, continue_pump)
        };

        let Some(message) = maybe_message else {
            return;
        };

        fltk::dialog::alert_default(&message);

        if should_continue {
            Self::schedule_alert_pump(0.0);
        } else {
            let should_schedule = {
                let state = Self::pending_alert_state();
                let mut guard = state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                update_alert_pump_state_after_display(
                    guard.queue.is_empty(),
                    &mut guard.pump_scheduled,
                )
            };
            if should_schedule {
                Self::schedule_alert_pump(0.0);
            }
        }
    }

    pub(crate) fn show_alert_dialog(message: &str) {
        let should_schedule = {
            let state = Self::pending_alert_state();
            let mut guard = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.queue.push_back(message.to_string());
            if guard.pump_scheduled {
                false
            } else {
                guard.pump_scheduled = true;
                true
            }
        };

        if should_schedule {
            Self::schedule_alert_pump(0.0);
        }
    }

    fn statement_at_cursor_text(&self) -> Option<String> {
        let sql = self.buffer.text();
        let cursor_pos = self.editor.insert_position() as usize;
        // 실행/인텔리센스/포맷 공통 규칙으로 문장 경계를 계산합니다.
        query_text::statement_at_cursor(&sql, cursor_pos)
    }

    fn normalize_statement_for_single_execution(statement: &str) -> String {
        query_text::normalize_single_statement(statement)
    }

    fn panic_payload_to_string(payload: &(dyn Any + Send)) -> String {
        if let Some(msg) = payload.downcast_ref::<&str>() {
            (*msg).to_string()
        } else if let Some(msg) = payload.downcast_ref::<String>() {
            msg.clone()
        } else {
            "unknown panic payload".to_string()
        }
    }

    fn log_callback_panic(context: &str, payload: &(dyn Any + Send)) {
        let panic_payload = Self::panic_payload_to_string(payload);
        crate::utils::logging::log_error(
            "sql_editor::callback",
            &format!("{context} panicked: {panic_payload}"),
        );
        eprintln!("{context} panicked: {panic_payload}");
    }

    fn invoke_query_result_callback(
        callback_slot: &Arc<Mutex<Option<Box<dyn FnMut(&QueryResult)>>>>,
        result: &QueryResult,
    ) {
        let callback = {
            let mut slot = callback_slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            slot.take()
        };

        if let Some(mut cb) = callback {
            let call_result = panic::catch_unwind(AssertUnwindSafe(|| cb(result)));
            let mut slot = callback_slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if slot.is_none() {
                *slot = Some(cb);
            }
            if let Err(payload) = call_result {
                Self::log_callback_panic("query result callback", payload.as_ref());
            }
        }
    }

    fn invoke_progress_callback(
        callback_slot: &Arc<Mutex<Option<Box<dyn FnMut(QueryProgress)>>>>,
        message: QueryProgress,
    ) {
        let callback = {
            let mut slot = callback_slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            slot.take()
        };

        if let Some(mut cb) = callback {
            let call_result = panic::catch_unwind(AssertUnwindSafe(|| cb(message)));
            let mut slot = callback_slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if slot.is_none() {
                *slot = Some(cb);
            }
            if let Err(payload) = call_result {
                Self::log_callback_panic("progress callback", payload.as_ref());
            }
        }
    }

    fn invoke_status_callback(
        callback_slot: &Arc<Mutex<Option<Box<dyn FnMut(&str)>>>>,
        message: &str,
    ) {
        let callback = {
            let mut slot = callback_slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            slot.take()
        };

        if let Some(mut cb) = callback {
            let call_result = panic::catch_unwind(AssertUnwindSafe(|| cb(message)));
            let mut slot = callback_slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if slot.is_none() {
                *slot = Some(cb);
            }
            if let Err(payload) = call_result {
                Self::log_callback_panic("status callback", payload.as_ref());
            }
        }
    }

    pub fn new(connection: SharedConnection, timeout_input: IntInput) -> Self {
        let mut group = Flex::default();
        group.set_type(FlexType::Column);
        group.set_margin(0);
        group.set_spacing(0);
        group.set_frame(FrameType::FlatBox);
        group.set_color(theme::panel_bg()); // Windows 11-inspired panel background

        let mut top_padding = Frame::default().with_size(0, EDITOR_TOP_PADDING);
        top_padding.set_frame(FrameType::NoBox);
        group.fixed(&top_padding, EDITOR_TOP_PADDING);

        // SQL Editor with modern styling
        let buffer = TextBuffer::default();
        let style_buffer = TextBuffer::default();
        let mut editor = TextEditor::default();
        editor.set_buffer(buffer.clone());
        editor.set_color(theme::editor_bg());
        editor.set_text_color(theme::text_primary());
        let editor_config = AppConfig::load();
        let editor_profile = configured_editor_profile();
        let editor_size = editor_config.editor_font_size;
        editor.set_text_font(editor_profile.normal);
        editor.set_text_size(editor_size as i32);
        editor.set_cursor_color(theme::text_primary());
        editor.wrap_mode(WrapMode::None, 0);
        editor.super_handle_first(false);
        editor.set_linenumber_width(48);
        editor.set_linenumber_fgcolor(theme::text_muted());
        editor.set_linenumber_bgcolor(theme::panel_bg());
        editor.set_linenumber_font(editor_profile.normal);
        editor.set_linenumber_size((editor_size.saturating_sub(2)) as i32);

        // Windows 11 selection color
        editor.set_selection_color(theme::selection_soft());

        // Setup syntax highlighting
        let style_table = create_style_table_with(editor_profile, editor_size);
        editor.set_highlight_data(style_buffer.clone(), style_table);

        // Add editor to flex and make it resizable (takes remaining space)
        group.resizable(&editor);
        group.end();

        let execute_callback: Arc<Mutex<Option<Box<dyn FnMut(&QueryResult)>>>> =
            Arc::new(Mutex::new(None));
        let progress_callback: Arc<Mutex<Option<Box<dyn FnMut(QueryProgress)>>>> =
            Arc::new(Mutex::new(None));
        let (progress_sender, progress_receiver) = mpsc::channel::<QueryProgress>();
        let (column_sender, column_receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let (ui_action_sender, ui_action_receiver) = mpsc::channel::<UiActionResult>();
        let (highlight_result_sender, highlight_result_receiver) =
            mpsc::channel::<HighlightResult>();
        let query_running = Arc::new(Mutex::new(false));
        let current_query_connection = Arc::new(Mutex::new(None));
        let cancel_flag = Arc::new(Mutex::new(false));

        let intellisense_data = Arc::new(Mutex::new(IntellisenseData::new()));
        let intellisense_popup = Arc::new(Mutex::new(IntellisensePopup::new()));
        let highlighter = Arc::new(Mutex::new(SqlHighlighter::new()));
        let status_callback: Arc<Mutex<Option<Box<dyn FnMut(&str)>>>> = Arc::new(Mutex::new(None));
        let find_callback: Arc<Mutex<Option<Box<dyn FnMut()>>>> = Arc::new(Mutex::new(None));
        let replace_callback: Arc<Mutex<Option<Box<dyn FnMut()>>>> = Arc::new(Mutex::new(None));
        let file_drop_callback: Arc<Mutex<Option<Box<dyn FnMut(PathBuf)>>>> =
            Arc::new(Mutex::new(None));
        let completion_range = Arc::new(Mutex::new(None::<(usize, usize)>));
        let pending_intellisense = Arc::new(Mutex::new(None::<PendingIntellisense>));
        let intellisense_parse_cache = Arc::new(Mutex::new(None::<IntellisenseParseCacheEntry>));
        let history_cursor = Arc::new(Mutex::new(None::<usize>));
        let history_original = Arc::new(Mutex::new(None::<String>));
        let history_navigation_entries = Arc::new(Mutex::new(None::<Vec<QueryHistoryEntry>>));
        let applying_history_navigation = Arc::new(Mutex::new(false));
        let undo_redo_state = Arc::new(Mutex::new(WordUndoRedoState::new(String::new())));
        let keyup_debounce_generation = Arc::new(Mutex::new(0_u64));
        let keyup_debounce_handle = Arc::new(Mutex::new(None::<app::TimeoutHandle>));
        let last_explain_plan = Arc::new(Mutex::new(None::<Vec<String>>));
        let highlight_request_state =
            Arc::new((Mutex::new(HighlightQueueState::default()), Condvar::new()));
        let highlight_revision = Arc::new(AtomicU64::new(0));
        let highlight_generation = Arc::new(AtomicU64::new(0));
        let highlight_worker_stopped = Arc::new(AtomicBool::new(false));
        let highlight_worker_handle = Arc::new(Mutex::new(None::<JoinHandle<()>>));

        let mut widget = Self {
            group,
            editor,
            buffer,
            style_buffer,
            connection,
            execute_callback,
            progress_callback: progress_callback.clone(),
            progress_sender,
            column_sender,
            ui_action_sender,
            query_running: query_running.clone(),
            current_query_connection: current_query_connection.clone(),
            cancel_flag,
            intellisense_data,
            intellisense_popup,
            highlighter,
            timeout_input,
            status_callback,
            find_callback,
            replace_callback,
            file_drop_callback,
            completion_range,
            pending_intellisense,
            intellisense_parse_cache,
            history_cursor,
            history_original,
            history_navigation_entries,
            applying_history_navigation,
            undo_redo_state,
            keyup_debounce_generation,
            keyup_debounce_handle,
            last_explain_plan,
            highlight_request_state: highlight_request_state.clone(),
            highlight_revision: highlight_revision.clone(),
            highlight_generation: highlight_generation.clone(),
            highlight_worker_stopped: highlight_worker_stopped.clone(),
            highlight_worker_handle: highlight_worker_handle.clone(),
        };

        widget.setup_intellisense();
        widget.setup_word_undo_redo();
        widget.setup_syntax_highlighting();
        widget.setup_viewport_highlight_poll();
        widget.setup_progress_handler(progress_receiver, progress_callback, query_running);
        widget.setup_column_loader(column_receiver);
        widget.setup_ui_action_handler(ui_action_receiver);
        widget.setup_highlight_worker(highlight_result_receiver);
        SqlEditorWidget::spawn_highlight_worker(
            highlight_request_state,
            highlight_result_sender,
            widget.highlighter.clone(),
            highlight_worker_stopped,
            highlight_worker_handle,
        );

        widget
    }

    fn setup_word_undo_redo(&self) {
        let undo_state = self.undo_redo_state.clone();
        let applying_history_navigation = self.applying_history_navigation.clone();
        let mut buffer = self.buffer.clone();
        buffer.add_modify_callback2(move |buf, pos, ins, del, _restyled, deleted_text| {
            if ins <= 0 && del <= 0 {
                return;
            }
            let inserted = inserted_text(buf, pos, ins);
            let mut state = undo_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            if state.applying_history
                || *applying_history_navigation
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
            {
                return;
            }

            let edit_group = classify_edit_group(ins, del, &inserted, deleted_text);
            let edit = BufferEdit {
                start: pos.max(0) as usize,
                deleted_len: del.max(0) as usize,
                inserted_text: inserted,
                deleted_text: deleted_text.to_string(),
            };
            state.record_edit(&edit, edit_group);
        });
    }

    fn setup_progress_handler(
        &self,
        progress_receiver: mpsc::Receiver<QueryProgress>,
        progress_callback: Arc<Mutex<Option<Box<dyn FnMut(QueryProgress)>>>>,
        query_running: Arc<Mutex<bool>>,
    ) {
        let execute_callback = self.execute_callback.clone();
        let cancel_flag = self.cancel_flag.clone();
        let lifecycle_group = self.group.clone();

        // Wrap receiver in Arc<Mutex> to share across timeout callbacks
        let receiver: Arc<Mutex<mpsc::Receiver<QueryProgress>>> =
            Arc::new(Mutex::new(progress_receiver));

        fn schedule_poll(
            receiver: Arc<Mutex<mpsc::Receiver<QueryProgress>>>,
            progress_callback: Arc<Mutex<Option<Box<dyn FnMut(QueryProgress)>>>>,
            query_running: Arc<Mutex<bool>>,
            execute_callback: Arc<Mutex<Option<Box<dyn FnMut(&QueryResult)>>>>,
            cancel_flag: Arc<Mutex<bool>>,
            lifecycle_group: Flex,
        ) {
            if lifecycle_group.was_deleted() {
                return;
            }

            let mut disconnected = false;
            let mut processed = 0usize;
            let mut hit_budget = false;
            let mut pending_rows: Vec<(usize, Vec<Vec<String>>)> = Vec::new();

            let flush_rows = |pending_rows: &mut Vec<(usize, Vec<Vec<String>>)>| {
                if pending_rows.is_empty() {
                    return;
                }
                // IMPORTANT: Do not drop buffered rows when cancel is requested.
                // Users expect rows fetched before cancel to remain visible, and
                // cancel only stops additional fetches from the worker side.
                for (index, rows) in pending_rows.drain(..) {
                    SqlEditorWidget::invoke_progress_callback(
                        &progress_callback,
                        QueryProgress::Rows { index, rows },
                    );
                }
            };
            // Process any pending messages
            loop {
                if processed >= MAX_PROGRESS_MESSAGES_PER_POLL {
                    hit_budget = true;
                    break;
                }

                let message = {
                    let r = receiver
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    r.try_recv()
                };

                match message {
                    Ok(message) => {
                        processed += 1;
                        match &message {
                            QueryProgress::Rows { .. } => {
                                // Keep aggregating Rows even after cancel_flag is set.
                                // The cancel path must preserve already-received data.
                                if let QueryProgress::Rows { index, rows } = message {
                                    if let Some((_, buffered)) =
                                        pending_rows.iter_mut().find(|(i, _)| *i == index)
                                    {
                                        buffered.extend(rows);
                                    } else {
                                        pending_rows.push((index, rows));
                                    }
                                }
                                continue;
                            }
                            QueryProgress::PromptInput { prompt, response } => {
                                flush_rows(&mut pending_rows);
                                let value = SqlEditorWidget::prompt_input_dialog(&prompt);
                                let _ = response.send(value);
                                app::awake();
                            }
                            QueryProgress::StatementFinished {
                                result,
                                connection_name,
                                timed_out,
                                ..
                            } => {
                                flush_rows(&mut pending_rows);
                                if *timed_out {
                                    SqlEditorWidget::show_alert_dialog(&format!(
                                        "Query timed out!\n\n{}",
                                        result.message
                                    ));
                                }
                                if let Err(history_err) = QueryHistoryDialog::add_to_history(
                                    &result.sql,
                                    result.execution_time.as_millis() as u64,
                                    result.row_count,
                                    connection_name,
                                    result.success,
                                    &result.message,
                                ) {
                                    crate::utils::logging::log_error("history", &history_err);
                                    SqlEditorWidget::show_alert_dialog(&format!(
                                        "Failed to save query history: {}",
                                        history_err
                                    ));
                                }
                                SqlEditorWidget::invoke_query_result_callback(
                                    &execute_callback,
                                    result,
                                );
                            }
                            QueryProgress::BatchFinished => {
                                flush_rows(&mut pending_rows);
                                SqlEditorWidget::finalize_execution_state(
                                    &query_running,
                                    &cancel_flag,
                                );
                                set_cursor(Cursor::Default);
                                app::flush();
                            }
                            _ => {}
                        }

                        SqlEditorWidget::invoke_progress_callback(&progress_callback, message);
                    }
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        disconnected = true;
                        break;
                    }
                }
            }

            flush_rows(&mut pending_rows);

            if disconnected {
                // Fail-safe cleanup: if the worker thread exits unexpectedly and the
                // channel closes before BatchFinished arrives, make sure execution
                // state/cursor do not stay stuck as "running" and downstream
                // handlers can run orphaned result-grid state recovery.
                SqlEditorWidget::handle_progress_channel_disconnected(
                    &progress_callback,
                    &query_running,
                    &cancel_flag,
                );
                return;
            }

            // Reschedule for next poll: if we processed messages, poll again immediately
            // to keep the UI responsive for streaming rows.
            let delay = if hit_budget || processed > 0 {
                PROGRESS_POLL_ACTIVE_INTERVAL_SECONDS
            } else {
                PROGRESS_POLL_INTERVAL_SECONDS
            };
            app::add_timeout3(delay, move |_| {
                schedule_poll(
                    receiver.clone(),
                    progress_callback.clone(),
                    query_running.clone(),
                    execute_callback.clone(),
                    cancel_flag.clone(),
                    lifecycle_group.clone(),
                );
            });
        }

        // Start polling
        schedule_poll(
            receiver,
            progress_callback,
            query_running,
            execute_callback,
            cancel_flag,
            lifecycle_group,
        );
    }

    fn handle_progress_channel_disconnected(
        progress_callback: &Arc<Mutex<Option<Box<dyn FnMut(QueryProgress)>>>>,
        query_running: &Arc<Mutex<bool>>,
        cancel_flag: &Arc<Mutex<bool>>,
    ) {
        SqlEditorWidget::finalize_execution_state(query_running, cancel_flag);
        // Guard UI-thread-only calls so this function is safe to call from
        // non-UI contexts such as unit tests.
        if app::is_ui_thread() {
            set_cursor(Cursor::Default);
            app::flush();
        }
        SqlEditorWidget::invoke_progress_callback(progress_callback, QueryProgress::BatchFinished);
    }

    fn finalize_execution_state(query_running: &Arc<Mutex<bool>>, cancel_flag: &Arc<Mutex<bool>>) {
        store_mutex_bool(query_running, false);
        store_mutex_bool(cancel_flag, false);
    }

    fn setup_column_loader(&self, column_receiver: mpsc::Receiver<ColumnLoadUpdate>) {
        let intellisense_data = self.intellisense_data.clone();
        let editor = self.editor.clone();
        let buffer = self.buffer.clone();
        let highlighter = self.highlighter.clone();
        let highlight_generation = self.highlight_generation.clone();
        let widget = self.clone();
        let intellisense_popup = self.intellisense_popup.clone();
        let completion_range = self.completion_range.clone();
        let column_sender = self.column_sender.clone();
        let connection = self.connection.clone();
        let pending_intellisense = self.pending_intellisense.clone();
        let intellisense_parse_cache = self.intellisense_parse_cache.clone();

        // Wrap receiver in Arc<Mutex> to share across timeout callbacks
        let receiver: Arc<Mutex<mpsc::Receiver<ColumnLoadUpdate>>> =
            Arc::new(Mutex::new(column_receiver));

        const COLUMN_POLL_ACTIVE_INTERVAL_SECONDS: f64 = 0.05;
        const COLUMN_POLL_IDLE_INTERVAL_SECONDS: f64 = 0.5;
        const COLUMN_LOADING_STALE_TIMEOUT: Duration = Duration::from_secs(8);

        fn schedule_poll(
            receiver: Arc<Mutex<mpsc::Receiver<ColumnLoadUpdate>>>,
            intellisense_data: Arc<Mutex<IntellisenseData>>,
            editor: TextEditor,
            buffer: TextBuffer,
            highlighter: Arc<Mutex<SqlHighlighter>>,
            highlight_generation: Arc<AtomicU64>,
            widget: SqlEditorWidget,
            intellisense_popup: Arc<Mutex<IntellisensePopup>>,
            completion_range: Arc<Mutex<Option<(usize, usize)>>>,
            column_sender: mpsc::Sender<ColumnLoadUpdate>,
            connection: SharedConnection,
            pending_intellisense: Arc<Mutex<Option<PendingIntellisense>>>,
            intellisense_parse_cache: Arc<Mutex<Option<IntellisenseParseCacheEntry>>>,
        ) {
            if editor.was_deleted() {
                return;
            }

            let mut disconnected = false;
            let mut processed = 0usize;
            let mut should_refresh_pending = false;
            let mut should_clear_pending = false;
            let mut highlight_columns: Option<Vec<String>> = None;
            // Process any pending messages
            {
                let r = receiver
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                loop {
                    match r.try_recv() {
                        Ok(update) => {
                            processed += 1;
                            let (refresh_pending, clear_pending, new_highlight_columns) = {
                                let mut data = intellisense_data
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                                if update.cache_columns {
                                    data.set_columns_for_table(&update.table, update.columns);
                                    (
                                        true,
                                        false,
                                        Some(collect_highlight_columns_from_intellisense(&data)),
                                    )
                                } else {
                                    data.clear_columns_loading(&update.table);
                                    // If every pending table load has completed without cached
                                    // columns, clear pending intellisense to avoid retry loops.
                                    (false, data.columns_loading.is_empty(), None)
                                }
                            };
                            should_refresh_pending |= refresh_pending;
                            should_clear_pending |= clear_pending;
                            if new_highlight_columns.is_some() {
                                highlight_columns = new_highlight_columns;
                            }
                        }
                        Err(mpsc::TryRecvError::Empty) => break,
                        Err(mpsc::TryRecvError::Disconnected) => {
                            disconnected = true;
                            break;
                        }
                    }
                }
            }

            if disconnected {
                widget.request_highlight_worker_shutdown();
                return;
            }

            if should_clear_pending {
                *pending_intellisense
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
            }

            if let Some(highlight_columns) = highlight_columns {
                let should_refresh_highlighting = {
                    let mut highlighter = highlighter
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let mut highlight_data = highlighter.get_highlight_data();
                    if highlight_data.columns == highlight_columns {
                        false
                    } else {
                        highlight_data.columns = highlight_columns;
                        highlighter.set_highlight_data(highlight_data);
                        true
                    }
                };

                if should_refresh_highlighting {
                    highlight_generation.fetch_add(1, Ordering::Relaxed);
                    widget.refresh_highlighting();
                }
            }

            if should_refresh_pending {
                let pending = pending_intellisense
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                if let Some(pending) = pending {
                    let cursor_pos = editor.insert_position().max(0);
                    if cursor_pos == pending.cursor_pos {
                        SqlEditorWidget::trigger_intellisense(
                            &editor,
                            &buffer,
                            &intellisense_data,
                            &intellisense_popup,
                            &completion_range,
                            &column_sender,
                            &connection,
                            &pending_intellisense,
                            &intellisense_parse_cache,
                        );
                    } else {
                        // Cursor moved since async load was requested.
                        // Drop stale pending state so poll loop can idle.
                        *pending_intellisense
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
                    }
                }
            }

            let stale_cleared = {
                let mut data = intellisense_data
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                data.clear_stale_columns_loading(COLUMN_LOADING_STALE_TIMEOUT)
            };
            if stale_cleared > 0 {
                processed += stale_cleared;
                let no_columns_loading = {
                    let data = intellisense_data
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    data.columns_loading.is_empty()
                };
                if no_columns_loading {
                    *pending_intellisense
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
                }
            }

            let has_pending_column_work = {
                let data = intellisense_data
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                !data.columns_loading.is_empty()
            } || pending_intellisense
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_some();

            // Reschedule with adaptive backoff to reduce idle CPU usage.
            let delay = if processed > 0 {
                0.0
            } else if has_pending_column_work {
                COLUMN_POLL_ACTIVE_INTERVAL_SECONDS
            } else {
                COLUMN_POLL_IDLE_INTERVAL_SECONDS
            };

            app::add_timeout3(delay, move |_| {
                schedule_poll(
                    receiver.clone(),
                    intellisense_data.clone(),
                    editor.clone(),
                    buffer.clone(),
                    highlighter.clone(),
                    highlight_generation.clone(),
                    widget.clone(),
                    intellisense_popup.clone(),
                    completion_range.clone(),
                    column_sender.clone(),
                    connection.clone(),
                    pending_intellisense.clone(),
                    intellisense_parse_cache.clone(),
                );
            });
        }

        // Start polling
        schedule_poll(
            receiver,
            intellisense_data,
            editor,
            buffer,
            highlighter,
            highlight_generation,
            widget,
            intellisense_popup,
            completion_range,
            column_sender,
            connection,
            pending_intellisense,
            intellisense_parse_cache,
        );
    }

    fn setup_ui_action_handler(&self, ui_action_receiver: mpsc::Receiver<UiActionResult>) {
        let widget = self.clone();

        let receiver: Arc<Mutex<mpsc::Receiver<UiActionResult>>> =
            Arc::new(Mutex::new(ui_action_receiver));

        fn schedule_poll(
            receiver: Arc<Mutex<mpsc::Receiver<UiActionResult>>>,
            widget: SqlEditorWidget,
        ) {
            if widget.group.was_deleted() {
                widget.request_highlight_worker_shutdown();
                return;
            }

            let mut disconnected = false;
            loop {
                let message = {
                    let r = receiver
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    r.try_recv()
                };

                match message {
                    Ok(action) => {
                        let should_reset_cursor = !matches!(&action, UiActionResult::Cancel(_));
                        match action {
                            UiActionResult::ExplainPlan(result) => match result {
                                Ok(plan_lines) => {
                                    let previous_plan = {
                                        let mut plan_slot = widget
                                            .last_explain_plan
                                            .lock()
                                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                                        let previous = plan_slot.clone();
                                        *plan_slot = Some(plan_lines.clone());
                                        previous
                                    };

                                    let plan_text =
                                        SqlEditorWidget::render_explain_plan(&plan_lines);
                                    let comparison = previous_plan.as_ref().map(|previous| {
                                        SqlEditorWidget::render_explain_plan_diff(
                                            previous.as_slice(),
                                            plan_lines.as_slice(),
                                        )
                                    });
                                    SqlEditorWidget::show_plan_dialog(
                                        &plan_text,
                                        comparison.as_deref(),
                                    );
                                }
                                Err(err) => {
                                    let _ =
                                        widget.progress_sender.send(QueryProgress::ScriptOutput {
                                            lines: vec![format!("Explain plan failed: {}", err)],
                                        });
                                    app::awake();
                                    widget.emit_status("Explain plan failed");
                                }
                            },
                            UiActionResult::QuickDescribe {
                                object_name,
                                result,
                            } => match result {
                                Ok(QuickDescribeData::TableColumns(columns)) => {
                                    if columns.is_empty() {
                                        fltk::dialog::message_default(&format!(
                                            "No table or view found with name: {}",
                                            object_name.to_uppercase()
                                        ));
                                    } else {
                                        SqlEditorWidget::show_quick_describe_dialog(
                                            &object_name,
                                            &columns,
                                        );
                                    }
                                }
                                Ok(QuickDescribeData::Text { title, content }) => {
                                    SqlEditorWidget::show_quick_describe_text_dialog(
                                        &title, &content,
                                    );
                                }
                                Err(err) => {
                                    if err.contains("Not connected") {
                                        SqlEditorWidget::show_alert_dialog(
                                            "Not connected to database",
                                        );
                                    } else {
                                        fltk::dialog::message_default(&format!(
                                            "Object not found or not accessible: {} ({})",
                                            object_name.to_uppercase(),
                                            err
                                        ));
                                    }
                                }
                            },
                            UiActionResult::Commit(result) => match result {
                                Ok(()) => {
                                    widget.emit_status("Committed");
                                }
                                Err(err) => {
                                    let _ =
                                        widget.progress_sender.send(QueryProgress::ScriptOutput {
                                            lines: vec![format!("Commit failed: {}", err)],
                                        });
                                    app::awake();
                                    widget.emit_status("Commit failed");
                                }
                            },
                            UiActionResult::Rollback(result) => match result {
                                Ok(()) => {
                                    widget.emit_status("Rolled back");
                                }
                                Err(err) => {
                                    let _ =
                                        widget.progress_sender.send(QueryProgress::ScriptOutput {
                                            lines: vec![format!("Rollback failed: {}", err)],
                                        });
                                    app::awake();
                                    widget.emit_status("Rollback failed");
                                }
                            },
                            UiActionResult::Cancel(result) => {
                                if let Err(err) = result {
                                    let _ =
                                        widget.progress_sender.send(QueryProgress::ScriptOutput {
                                            lines: vec![format!("Cancel failed: {}", err)],
                                        });
                                    app::awake();
                                    widget.emit_status("Cancel failed");
                                }
                            }
                            UiActionResult::CancelPending => {
                                widget.emit_status(
                                    "Cancel requested; waiting for query initialization",
                                );
                            }
                            UiActionResult::QueryAlreadyRunning => {
                                let busy_message = crate::db::format_connection_busy_message();
                                widget.emit_status(&busy_message);
                                SqlEditorWidget::show_alert_dialog(&busy_message);
                            }
                            UiActionResult::ConnectionBusy => {
                                let busy_message = crate::db::format_connection_busy_message();
                                widget.emit_status(&busy_message);
                                SqlEditorWidget::show_alert_dialog(&busy_message);
                            }
                        }
                        if should_reset_cursor {
                            set_cursor(Cursor::Default);
                            app::flush();
                        }
                    }
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        disconnected = true;
                        break;
                    }
                }
            }

            if disconnected {
                widget.request_highlight_worker_shutdown();
                return;
            }

            app::add_timeout3(0.05, move |_| {
                schedule_poll(receiver.clone(), widget.clone());
            });
        }

        schedule_poll(receiver, widget);
    }

    fn setup_syntax_highlighting(&self) {
        let mut buffer = self.buffer.clone();
        let widget = self.clone();
        let intellisense_parse_cache = self.intellisense_parse_cache.clone();
        buffer.add_modify_callback2(move |buf, _pos, _ins, _del, _restyled, _deleted_text| {
            intellisense_parse_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take();
            widget.enqueue_highlight_request(buf.text());
        });
        self.refresh_highlighting();
    }

    fn setup_highlight_worker(&self, highlight_result_receiver: mpsc::Receiver<HighlightResult>) {
        let receiver: Arc<Mutex<mpsc::Receiver<HighlightResult>>> =
            Arc::new(Mutex::new(highlight_result_receiver));
        let widget = self.clone();

        fn schedule_poll(
            receiver: Arc<Mutex<mpsc::Receiver<HighlightResult>>>,
            widget: SqlEditorWidget,
        ) {
            if widget.group.was_deleted() {
                widget.request_highlight_worker_shutdown();
                return;
            }

            let mut disconnected = false;
            let mut latest_result: Option<HighlightResult> = None;
            {
                let r = receiver
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                loop {
                    match r.try_recv() {
                        Ok(result) => latest_result = Some(result),
                        Err(mpsc::TryRecvError::Empty) => break,
                        Err(mpsc::TryRecvError::Disconnected) => {
                            disconnected = true;
                            break;
                        }
                    }
                }
            }

            if disconnected {
                widget.request_highlight_worker_shutdown();
                return;
            }

            if let Some(result) = latest_result {
                let current_revision = widget.highlight_revision.load(Ordering::Relaxed);
                let current_generation = widget.highlight_generation.load(Ordering::Relaxed);
                if result.revision == current_revision && result.generation == current_generation {
                    let mut style_buffer = widget.style_buffer.clone();
                    style_buffer.set_text(&result.style_text);
                    let mut editor = widget.editor.clone();
                    editor.redraw();
                    app::redraw();
                }
            }

            app::add_timeout3(PROGRESS_POLL_INTERVAL_SECONDS, move |_| {
                schedule_poll(receiver.clone(), widget.clone());
            });
        }

        schedule_poll(receiver, widget);
    }

    fn spawn_highlight_worker(
        highlight_request_state: Arc<(Mutex<HighlightQueueState>, Condvar)>,
        highlight_result_sender: mpsc::Sender<HighlightResult>,
        highlighter: Arc<Mutex<SqlHighlighter>>,
        highlight_worker_stopped: Arc<AtomicBool>,
        highlight_worker_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    ) {
        let worker_stopped_for_thread = highlight_worker_stopped.clone();
        let spawn_result = thread::Builder::new()
            .name("sql-highlighter-worker".to_string())
            .spawn(move || loop {
                let request = {
                    let (queue_lock, queue_signal) = &*highlight_request_state;
                    let mut queue_state = queue_lock
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    while !queue_state.shutdown && queue_state.pending_request.is_none() {
                        queue_state = queue_signal
                            .wait(queue_state)
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                    }
                    if queue_state.shutdown {
                        worker_stopped_for_thread.store(true, Ordering::Relaxed);
                        break;
                    }
                    let Some(request) = queue_state.pending_request.take() else {
                        continue;
                    };
                    request
                };

                let style_text = {
                    let guard = highlighter
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    guard.generate_styles_for_text(&request.text)
                };

                if highlight_result_sender
                    .send(HighlightResult {
                        revision: request.revision,
                        generation: request.generation,
                        style_text,
                    })
                    .is_err()
                {
                    worker_stopped_for_thread.store(true, Ordering::Relaxed);
                    break;
                }

                app::awake();
            });

        match spawn_result {
            Ok(handle) => {
                let mut guard = highlight_worker_handle
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *guard = Some(handle);
            }
            Err(err) => {
                highlight_worker_stopped.store(true, Ordering::Relaxed);
                crate::utils::logging::log_error(
                    "sql_editor::highlight_worker",
                    &format!("failed to spawn highlight worker: {}", err),
                );
            }
        }
    }

    fn setup_viewport_highlight_poll(&self) {
        let widget = self.clone();
        let editor = self.editor.clone();
        let buffer = self.buffer.clone();
        let last_viewport_state = Arc::new(Mutex::new(None::<(bool, i32, i32, i32, i32, i32)>));

        fn schedule_poll(
            widget: SqlEditorWidget,
            editor: TextEditor,
            buffer: TextBuffer,
            last_viewport_state: Arc<Mutex<Option<(bool, i32, i32, i32, i32, i32)>>>,
        ) {
            if widget.group.was_deleted() || editor.was_deleted() {
                widget.request_highlight_worker_shutdown();
                return;
            }

            let visible = editor.visible_r();
            let top_row = editor.scroll_row();
            let left_col = editor.scroll_col();
            let w = editor.w();
            let h = editor.h();
            let text_len = buffer.length();
            let current_state = (visible, top_row, left_col, w, h, text_len);
            let should_refresh = {
                let mut previous = last_viewport_state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let changed = previous.map_or(true, |state| state != current_state);
                if changed {
                    *previous = Some(current_state);
                }
                changed
            };

            if should_refresh {
                widget.refresh_highlighting();
            }

            app::add_timeout3(VIEWPORT_HIGHLIGHT_POLL_INTERVAL_SECONDS, move |_| {
                schedule_poll(
                    widget.clone(),
                    editor.clone(),
                    buffer.clone(),
                    last_viewport_state.clone(),
                );
            });
        }

        schedule_poll(widget, editor, buffer, last_viewport_state);
    }

    pub fn explain_current(&self) {
        let Some(sql) = self.statement_at_cursor_text() else {
            SqlEditorWidget::show_alert_dialog("No SQL at cursor");
            return;
        };

        let connection = self.connection.clone();
        let sender = self.ui_action_sender.clone();
        set_cursor(Cursor::Wait);
        app::flush();
        thread::spawn(move || {
            // Try to acquire connection lock without blocking
            let Some(mut conn_guard) = crate::db::try_lock_connection_with_activity(
                &connection,
                "Generating explain plan",
            ) else {
                // Query is already running, notify user
                let _ = sender.send(UiActionResult::QueryAlreadyRunning);
                app::awake();
                return;
            };

            let result = match conn_guard.require_live_connection() {
                Ok(db_conn) => QueryExecutor::get_explain_plan(db_conn.as_ref(), &sql)
                    .map_err(|err| err.to_string()),
                Err(message) => Err(message.to_string()),
            };

            let _ = sender.send(UiActionResult::ExplainPlan(result));
            app::awake();
        });
    }

    fn render_explain_plan(plan_lines: &[String]) -> String {
        if plan_lines.is_empty() {
            return "No plan output.".to_string();
        }

        let mut out = String::new();
        for (idx, line) in plan_lines.iter().enumerate() {
            out.push_str(&format!("{:>3}: {}\n", idx + 1, line));
        }

        out.trim_end_matches('\n').to_string()
    }

    fn render_explain_plan_diff(previous: &[String], current: &[String]) -> String {
        let mut previous_used = vec![false; previous.len()];
        let mut added: Vec<String> = Vec::new();

        for current_line in current {
            let mut matched_index = None;
            for (idx, previous_line) in previous.iter().enumerate() {
                if !previous_used[idx] && previous_line == current_line {
                    matched_index = Some(idx);
                    break;
                }
            }

            if let Some(idx) = matched_index {
                previous_used[idx] = true;
            } else {
                added.push(current_line.clone());
            }
        }

        let mut removed: Vec<String> = Vec::new();
        for (idx, previous_line) in previous.iter().enumerate() {
            if !previous_used[idx] {
                removed.push(previous_line.clone());
            }
        }

        const DIFF_PREVIEW_LIMIT: usize = 20;

        let mut out = String::new();
        out.push_str("=== Comparison Against Previous Explain Plan ===\n");
        out.push_str(&format!(
            "Previous lines: {}, Current lines: {}\n",
            previous.len(),
            current.len()
        ));
        out.push_str(&format!(
            "Added lines: {}, Removed lines: {}\n",
            added.len(),
            removed.len()
        ));

        if added.is_empty() && removed.is_empty() {
            out.push_str("No line-level differences detected.\n");
            return out;
        }

        if !added.is_empty() {
            out.push('\n');
            out.push_str("Added:\n");
            for line in added.iter().take(DIFF_PREVIEW_LIMIT) {
                out.push_str("+ ");
                out.push_str(line);
                out.push('\n');
            }
            if added.len() > DIFF_PREVIEW_LIMIT {
                out.push_str(&format!(
                    "... {} more added lines\n",
                    added.len() - DIFF_PREVIEW_LIMIT
                ));
            }
        }

        if !removed.is_empty() {
            out.push('\n');
            out.push_str("Removed:\n");
            for line in removed.iter().take(DIFF_PREVIEW_LIMIT) {
                out.push_str("- ");
                out.push_str(line);
                out.push('\n');
            }
            if removed.len() > DIFF_PREVIEW_LIMIT {
                out.push_str(&format!(
                    "... {} more removed lines\n",
                    removed.len() - DIFF_PREVIEW_LIMIT
                ));
            }
        }

        out
    }

    fn show_plan_dialog(plan_text: &str, comparison_text: Option<&str>) {
        use fltk::{prelude::*, text::TextDisplay, window::Window};

        let current_group = fltk::group::Group::try_current();

        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let mut dialog = Window::default()
            .with_size(800, 500)
            .with_label("Explain Plan");
        crate::ui::center_on_main(&mut dialog);
        dialog.set_color(theme::panel_raised());
        dialog.make_modal(true);
        dialog.begin();

        let mut display = TextDisplay::default().with_pos(10, 10).with_size(780, 440);
        display.set_color(theme::editor_bg());
        display.set_text_color(theme::text_primary());
        display.set_text_font(configured_editor_profile().normal);
        display.set_text_size(configured_ui_font_size());

        let mut content = plan_text.to_string();
        if let Some(comparison) = comparison_text {
            content.push_str("\n\n");
            content.push_str(comparison);
        }

        let mut buffer = fltk::text::TextBuffer::default();
        buffer.set_text(&content);
        display.set_buffer(buffer);

        let mut close_btn = fltk::button::Button::default()
            .with_pos(690, 455)
            .with_size(BUTTON_WIDTH_LARGE, BUTTON_HEIGHT)
            .with_label("Close");
        close_btn.set_color(theme::button_secondary());
        close_btn.set_label_color(theme::text_primary());

        let (sender, receiver) = mpsc::channel::<()>();
        close_btn.set_callback(move |_| {
            let _ = sender.send(());
            app::awake();
        });

        dialog.end();
        dialog.show();
        fltk::group::Group::set_current(current_group.as_ref());
        let _ = dialog.take_focus();
        let _ = close_btn.take_focus();

        while dialog.shown() {
            app::wait();
            if receiver.try_recv().is_ok() {
                dialog.hide();
            }
        }

        // Explicitly destroy top-level dialog widgets to release native resources.
        Window::delete(dialog);
    }

    fn emit_status(&self, message: &str) {
        Self::invoke_status_callback(&self.status_callback, message);
    }

    pub fn clear(&self) {
        let mut buffer = self.buffer.clone();
        let len = buffer.length();
        if len > 0 {
            // Use edit-style deletion so Ctrl+Z/Cmd+Z can restore cleared text.
            buffer.remove(0, len);
        }
        let mut editor = self.editor.clone();
        editor.set_insert_position(0);
        editor.show_insert_position();
    }

    pub fn commit(&self) {
        let connection = self.connection.clone();
        let sender = self.ui_action_sender.clone();
        set_cursor(Cursor::Wait);
        app::flush();
        thread::spawn(move || {
            // Try to acquire connection lock without blocking
            let Some(mut conn_guard) =
                crate::db::try_lock_connection_with_activity(&connection, "Commit transaction")
            else {
                // Query is already running, notify user
                let _ = sender.send(UiActionResult::QueryAlreadyRunning);
                app::awake();
                return;
            };

            let result = match conn_guard.require_live_connection() {
                Ok(db_conn) => db_conn.commit().map_err(|err| err.to_string()),
                Err(message) => Err(message.to_string()),
            };

            let _ = sender.send(UiActionResult::Commit(result));
            app::awake();
        });
    }

    pub fn rollback(&self) {
        let connection = self.connection.clone();
        let sender = self.ui_action_sender.clone();
        set_cursor(Cursor::Wait);
        app::flush();
        thread::spawn(move || {
            // Try to acquire connection lock without blocking
            let Some(mut conn_guard) =
                crate::db::try_lock_connection_with_activity(&connection, "Rollback transaction")
            else {
                // Query is already running, notify user
                let _ = sender.send(UiActionResult::QueryAlreadyRunning);
                app::awake();
                return;
            };

            let result = match conn_guard.require_live_connection() {
                Ok(db_conn) => db_conn.rollback().map_err(|err| err.to_string()),
                Err(message) => Err(message.to_string()),
            };

            let _ = sender.send(UiActionResult::Rollback(result));
            app::awake();
        });
    }

    pub fn cancel_current(&self) {
        // Set cancel flag immediately so the execution thread can check it
        store_mutex_bool(&self.cancel_flag, true);

        let current_query_connection = self.current_query_connection.clone();
        let cancel_flag = self.cancel_flag.clone();
        let query_running = self.query_running.clone();
        let sender = self.ui_action_sender.clone();
        thread::spawn(move || {
            let mut conn =
                SqlEditorWidget::clone_current_query_connection(&current_query_connection);

            if !SqlEditorWidget::is_query_running_flag(&query_running) && conn.is_none() {
                // Execution can still be transitioning into "running" and may not
                // have published current_query_connection yet. Wait briefly so a
                // cancel click that races with query start can still interrupt.
                for _ in 0..40 {
                    if !load_mutex_bool(&cancel_flag) {
                        break;
                    }
                    if SqlEditorWidget::is_query_running_flag(&query_running) {
                        break;
                    }
                    thread::sleep(Duration::from_millis(25));
                    conn =
                        SqlEditorWidget::clone_current_query_connection(&current_query_connection);
                    if conn.is_some() {
                        break;
                    }
                }
            }

            if !SqlEditorWidget::is_query_running_flag(&query_running) && conn.is_none() {
                // This editor is idle. Do not attempt to cancel through the
                // global DB connection because that can interrupt a query that
                // is currently running in a different editor tab.
                store_mutex_bool(&cancel_flag, false);
                let _ = sender.send(UiActionResult::Cancel(Ok(())));
                app::awake();
                return;
            }

            if conn.is_none() {
                // Execution may still be initializing the DB connection.
                // Wait briefly so a single cancel click can still interrupt reliably.
                for _ in 0..40 {
                    if !load_mutex_bool(&cancel_flag) {
                        break;
                    }
                    thread::sleep(Duration::from_millis(25));
                    conn =
                        SqlEditorWidget::clone_current_query_connection(&current_query_connection);
                    if conn.is_some() {
                        break;
                    }
                }
            }

            // Re-check the cancel flag before breaking the connection. If it is
            // already false the previous query has already finished and reset it;
            // breaking the connection now would interrupt a newly-started query.
            if !load_mutex_bool(&cancel_flag) {
                let _ = sender.send(UiActionResult::Cancel(Ok(())));
                app::awake();
                return;
            }

            if conn.is_none() {
                // The worker has not published a break-able connection yet.
                // Keep cancel requested so execution stops at the first safe
                // cancellation point, and surface a status update instead of
                // pretending the DB-level break already happened.
                let _ = sender.send(UiActionResult::CancelPending);
                app::awake();
                return;
            }

            let result = SqlEditorWidget::break_current_query_connection(conn);

            let _ = sender.send(UiActionResult::Cancel(result));
            app::awake();
        });
    }

    fn is_query_running_flag(query_running: &Arc<Mutex<bool>>) -> bool {
        load_mutex_bool(query_running)
    }

    fn clone_current_query_connection(
        current_query_connection: &Arc<Mutex<Option<Arc<Connection>>>>,
    ) -> Option<Arc<Connection>> {
        match current_query_connection.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => {
                eprintln!("Warning: current query connection lock was poisoned; recovering.");
                poisoned.into_inner().clone()
            }
        }
    }

    fn break_current_query_connection(connection: Option<Arc<Connection>>) -> Result<(), String> {
        if let Some(db_conn) = connection {
            db_conn.break_execution().map_err(|err| err.to_string())
        } else {
            // No published connection yet. Keep cancel_flag set and let execution
            // stop at the first safe cancellation point.
            Ok(())
        }
    }

    fn set_current_query_connection(
        current_query_connection: &Arc<Mutex<Option<Arc<Connection>>>>,
        value: Option<Arc<Connection>>,
    ) {
        match current_query_connection.lock() {
            Ok(mut guard) => {
                *guard = value;
            }
            Err(poisoned) => {
                eprintln!("Warning: current query connection lock was poisoned; recovering.");
                *poisoned.into_inner() = value;
            }
        }
    }

    pub fn set_execute_callback<F>(&mut self, callback: F)
    where
        F: FnMut(&QueryResult) + 'static,
    {
        *self
            .execute_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(callback));
    }

    pub fn set_status_callback<F>(&mut self, callback: F)
    where
        F: FnMut(&str) + 'static,
    {
        *self
            .status_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(callback));
    }

    pub fn set_find_callback<F>(&mut self, callback: F)
    where
        F: FnMut() + 'static,
    {
        *self
            .find_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(callback));
    }

    pub fn set_replace_callback<F>(&mut self, callback: F)
    where
        F: FnMut() + 'static,
    {
        *self
            .replace_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(callback));
    }

    pub fn set_file_drop_callback<F>(&mut self, callback: F)
    where
        F: FnMut(PathBuf) + 'static,
    {
        *self
            .file_drop_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(callback));
    }

    /// Releases callback/data references so a closing tab can be dropped promptly.
    pub fn cleanup_for_close(&mut self) {
        Self::finalize_execution_state(&self.query_running, &self.cancel_flag);
        Self::set_current_query_connection(&self.current_query_connection, None);

        *self
            .execute_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        *self
            .progress_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        *self
            .status_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        *self
            .find_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        *self
            .replace_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        *self
            .file_drop_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;

        Self::invalidate_keyup_debounce(
            &self.keyup_debounce_generation,
            &self.keyup_debounce_handle,
        );

        self.intellisense_popup
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .delete_for_close();
        *self
            .intellisense_data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = IntellisenseData::new();
        self.highlighter
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .set_highlight_data(HighlightData::new());
        self.highlight_generation.fetch_add(1, Ordering::Relaxed);

        self.request_highlight_worker_shutdown();

        self.buffer.set_text("");
        self.style_buffer.set_text("");
        self.completion_range
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        self.pending_intellisense
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        self.intellisense_parse_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        self.history_cursor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        self.history_original
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        self.history_navigation_entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        *self
            .applying_history_navigation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = false;
        Self::reset_word_undo_state(&self.undo_redo_state);
    }

    fn reset_word_undo_state(undo_redo_state: &Arc<Mutex<WordUndoRedoState>>) {
        let mut state = undo_redo_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fresh_snapshot = UndoSnapshot::new(String::new(), 0);
        state.anchor = fresh_snapshot.clone();
        state.current = fresh_snapshot;
        state.deltas.clear();
        state.history_total_bytes = 0;
        state.index = 0;
        state.active_group = None;
        state.next_group_id = 1;
        state.applying_history = false;
    }

    fn apply_delta_to_buffer(buffer: &mut TextBuffer, delta: &UndoDelta, reverse: bool) {
        let buffer_len = buffer.length().max(0) as usize;
        let start = delta.start.min(buffer_len);
        let delete_len = if reverse {
            delta.inserted_text.len()
        } else {
            delta.deleted_text.len()
        };
        let end = start.saturating_add(delete_len).min(buffer_len);
        let start_i32 = start.min(i32::MAX as usize) as i32;
        let end_i32 = end.min(i32::MAX as usize) as i32;
        let replacement = if reverse {
            delta.deleted_text.as_str()
        } else {
            delta.inserted_text.as_str()
        };
        buffer.replace(start_i32, end_i32, replacement);
    }

    #[allow(dead_code)]
    pub fn update_highlight_data(&mut self, data: HighlightData) {
        self.highlighter
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .set_highlight_data(data);
        self.highlight_generation.fetch_add(1, Ordering::Relaxed);
        self.refresh_highlighting();
    }

    pub fn get_highlighter(&self) -> Arc<Mutex<SqlHighlighter>> {
        self.highlighter.clone()
    }

    #[allow(dead_code)]
    pub fn get_text(&self) -> String {
        self.buffer.text()
    }

    #[allow(dead_code)]
    pub fn set_text(&mut self, text: &str) {
        self.buffer.set_text(text);
    }

    #[allow(dead_code)]
    pub fn get_group(&self) -> &Flex {
        &self.group
    }

    pub fn get_buffer(&self) -> TextBuffer {
        self.buffer.clone()
    }

    pub fn apply_font_settings(&mut self, profile: FontProfile, size: u32, ui_size: i32) {
        let size_i32 = size as i32;
        self.editor.set_text_font(profile.normal);
        self.editor.set_text_size(size_i32);
        self.editor.set_linenumber_font(profile.normal);
        self.editor
            .set_linenumber_size((size.saturating_sub(2)) as i32);
        self.timeout_input.set_text_size(ui_size);
        let style_table = create_style_table_with(profile, size);
        self.editor
            .set_highlight_data(self.style_buffer.clone(), style_table);
        self.refresh_highlighting();
        // Force FLTK to recalculate internal display metrics (line heights,
        // character widths, scroll positions) by triggering a no-op resize.
        // Without this, the TextEditor may render with stale cached metrics
        // until an external event (e.g. window resize) forces recalculation.
        let (x, y, w, h) = (
            self.editor.x(),
            self.editor.y(),
            self.editor.w(),
            self.editor.h(),
        );
        self.editor.resize(x, y, w, h);
        self.timeout_input.redraw();
        self.editor.redraw();
    }

    fn enqueue_highlight_request(&self, text: String) {
        if self.highlight_worker_stopped.load(Ordering::Relaxed) {
            return;
        }

        let revision = self.highlight_revision.fetch_add(1, Ordering::Relaxed) + 1;
        let generation = self.highlight_generation.load(Ordering::Relaxed);
        let request = HighlightRequest {
            revision,
            generation,
            text,
        };
        let (queue_lock, queue_signal) = &*self.highlight_request_state;
        {
            let mut queue_state = queue_lock
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if queue_state.shutdown {
                self.highlight_worker_stopped.store(true, Ordering::Relaxed);
                return;
            }
            queue_state.pending_request = Some(request);
        }
        queue_signal.notify_one();
    }

    fn request_highlight_worker_shutdown(&self) {
        if self.highlight_worker_stopped.swap(true, Ordering::Relaxed) {
            return;
        }

        let (queue_lock, queue_signal) = &*self.highlight_request_state;
        {
            let mut queue_state = queue_lock
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            queue_state.shutdown = true;
            queue_state.pending_request = None;
        }
        queue_signal.notify_all();

        let handle = {
            let mut guard = self
                .highlight_worker_handle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.take()
        };
        if let Some(handle) = handle {
            if let Err(err) = handle.join() {
                crate::utils::logging::log_error(
                    "sql_editor::highlight_worker",
                    &format!("highlight worker join failed: {:?}", err),
                );
            }
        }
    }

    #[allow(dead_code)]
    pub fn refresh_highlighting(&self) {
        self.enqueue_highlight_request(self.buffer.text());
    }

    #[allow(dead_code)]
    pub fn append_text(&mut self, text: &str) {
        let current = self.buffer.text();
        if current.is_empty() {
            self.buffer.set_text(text);
        } else {
            self.buffer.set_text(&format!("{}\n{}", current, text));
        }
    }

    pub fn get_editor(&self) -> TextEditor {
        self.editor.clone()
    }

    pub fn reset_undo_redo_history(&self) {
        let current_text = self.buffer.text();
        let buffer_len = self.buffer.length().max(0);
        let cursor_pos = self.editor.insert_position().clamp(0, buffer_len) as usize;
        let clamped_cursor = WordUndoRedoState::clamp_to_char_boundary(
            &current_text,
            cursor_pos.min(current_text.len()),
        );
        let snapshot = UndoSnapshot::new(current_text, clamped_cursor);
        {
            let mut state = self
                .undo_redo_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.anchor = snapshot.clone();
            state.current = snapshot;
            state.deltas.clear();
            state.history_total_bytes = 0;
            state.index = 0;
            state.active_group = None;
            state.next_group_id = 1;
            state.applying_history = false;
        }
        *self
            .history_cursor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        *self
            .history_original
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        self.history_navigation_entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        *self
            .applying_history_navigation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = false;
    }

    pub fn undo(&self) {
        let (deltas, cursor_pos) = {
            let mut state = self
                .undo_redo_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let deltas = state.take_undo_group();
            if deltas.is_empty() {
                return;
            }
            let cursor_pos = state.current.cursor_pos.min(i32::MAX as usize) as i32;
            (deltas, cursor_pos)
        };

        let mut buffer = self.buffer.clone();
        for delta in &deltas {
            Self::apply_delta_to_buffer(&mut buffer, delta, true);
        }
        let mut editor = self.editor.clone();
        editor.set_insert_position(cursor_pos);
        editor.show_insert_position();

        self.undo_redo_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .applying_history = false;
    }

    pub fn redo(&self) {
        let (deltas, cursor_pos) = {
            let mut state = self
                .undo_redo_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let deltas = state.take_redo_group();
            if deltas.is_empty() {
                return;
            }
            let cursor_pos = state.current.cursor_pos.min(i32::MAX as usize) as i32;
            (deltas, cursor_pos)
        };

        let mut buffer = self.buffer.clone();
        for delta in &deltas {
            Self::apply_delta_to_buffer(&mut buffer, delta, false);
        }
        let mut editor = self.editor.clone();
        editor.set_insert_position(cursor_pos);
        editor.show_insert_position();

        self.undo_redo_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .applying_history = false;
    }

    pub fn is_query_running(&self) -> bool {
        load_mutex_bool(&self.query_running)
    }

    fn apply_history_navigation_text(&mut self, text: &str) {
        {
            let mut applying_navigation = self
                .applying_history_navigation
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *applying_navigation = true;
        }

        self.buffer.set_text(text);

        {
            let mut applying_navigation = self
                .applying_history_navigation
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *applying_navigation = false;
        }

        self.refresh_highlighting();
        let cursor_pos = text.len().min(i32::MAX as usize) as i32;
        self.editor.set_insert_position(cursor_pos);
        self.editor.show_insert_position();
    }

    pub fn navigate_history(&mut self, direction: i32) {
        enum NavigationUpdate {
            NoOp,
            RestoreOriginal(String),
            ShowSql(String),
        }

        let mut cursor = self
            .history_cursor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut original = self
            .history_original
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut history_entries = self
            .history_navigation_entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if cursor.is_none() {
            // Keep navigation aligned with persisted history while avoiding long UI stalls
            // on each key press: flush once when navigation starts, then reuse a snapshot.
            let _ = flush_history_writer_with_timeout(HISTORY_NAVIGATION_FLUSH_TIMEOUT);
            let loaded = QueryHistory::load();
            if loaded.queries.is_empty() {
                return;
            }
            *history_entries = Some(loaded.queries);
            *original = Some(self.buffer.text());
        }

        let Some(entries) = history_entries.as_ref() else {
            return;
        };

        let update = match *cursor {
            None => {
                if direction > 0 {
                    if let Some(first) = entries.first() {
                        *cursor = Some(0);
                        NavigationUpdate::ShowSql(first.sql.clone())
                    } else {
                        NavigationUpdate::NoOp
                    }
                } else {
                    return;
                }
            }
            Some(index) => {
                if direction > 0 {
                    let next_index = index.saturating_add(1);
                    if next_index >= entries.len() {
                        NavigationUpdate::NoOp
                    } else {
                        *cursor = Some(next_index);
                        NavigationUpdate::ShowSql(entries[next_index].sql.clone())
                    }
                } else if index == 0 {
                    *cursor = None;
                    history_entries.take();
                    if let Some(saved) = original.take() {
                        NavigationUpdate::RestoreOriginal(saved)
                    } else {
                        NavigationUpdate::NoOp
                    }
                } else {
                    let next_index = index.saturating_sub(1);
                    *cursor = Some(next_index);
                    NavigationUpdate::ShowSql(entries[next_index].sql.clone())
                }
            }
        };

        drop(history_entries);
        drop(original);
        drop(cursor);

        match update {
            NavigationUpdate::NoOp => {}
            NavigationUpdate::RestoreOriginal(saved) => {
                self.apply_history_navigation_text(&saved);
            }
            NavigationUpdate::ShowSql(sql) => {
                self.apply_history_navigation_text(&sql);
            }
        }
    }

    pub fn select_block_in_direction(&mut self, direction: i32) {
        let selection = self.buffer.selection_position();
        let cursor_pos = self.editor.insert_position().max(0);

        if selection.is_none() || selection == Some((cursor_pos, cursor_pos)) {
            let (start, end) = Self::block_bounds(&self.buffer, cursor_pos);
            self.buffer.select(start, end);
            self.editor.set_insert_position(end);
            self.editor.show_insert_position();
            return;
        }

        let (sel_start, sel_end) = selection.unwrap_or((cursor_pos, cursor_pos));
        if direction < 0 {
            if sel_start <= 0 {
                return;
            }
            let prev_pos = sel_start.saturating_sub(1);
            let (block_start, _) = Self::block_bounds(&self.buffer, prev_pos);
            self.buffer.select(block_start, sel_end);
            self.editor.set_insert_position(block_start);
        } else {
            let buffer_len = self.buffer.length();
            if sel_end >= buffer_len {
                return;
            }
            let next_pos = (sel_end + 1).min(buffer_len.saturating_sub(1));
            let (_, block_end) = Self::block_bounds(&self.buffer, next_pos);
            self.buffer.select(sel_start, block_end);
            self.editor.set_insert_position(block_end);
        }
        self.editor.show_insert_position();
    }

    fn block_bounds(buffer: &TextBuffer, pos: i32) -> (i32, i32) {
        let mut start = buffer.line_start(pos).max(0);
        let mut end = buffer.line_end(pos).max(start);
        let buffer_len = buffer.length();

        let is_blank = |start: i32, end: i32| {
            let text = buffer.text_range(start, end).unwrap_or_default();
            text.trim().is_empty()
        };

        let blank = is_blank(start, end);

        let mut scan = start;
        while scan > 0 {
            let prev_pos = scan.saturating_sub(1);
            let prev_start = buffer.line_start(prev_pos).max(0);
            let prev_end = buffer.line_end(prev_pos).max(prev_start);
            if is_blank(prev_start, prev_end) != blank {
                break;
            }
            start = prev_start;
            scan = prev_start;
        }

        let mut scan = end;
        while scan < buffer_len {
            let next_pos = (scan + 1).min(buffer_len.saturating_sub(1));
            let next_start = buffer.line_start(next_pos).max(0);
            let next_end = buffer.line_end(next_pos).max(next_start);
            if is_blank(next_start, next_end) != blank {
                break;
            }
            end = next_end;
            scan = next_end;
        }

        (start, end)
    }
}

fn inserted_text(buf: &TextBuffer, pos: i32, ins: i32) -> String {
    if ins <= 0 || pos < 0 {
        return String::new();
    }

    let insert_end = pos.saturating_add(ins).min(buf.length());
    buf.text_range(pos, insert_end).unwrap_or_default()
}

fn classify_edit_granularity(ins: i32, del: i32, inserted: &str, deleted: &str) -> EditGranularity {
    if ins <= 0 && del <= 0 {
        return EditGranularity::Other;
    }

    if (ins > 0 && inserted.chars().all(is_word_edit_char))
        || (del > 0 && deleted.chars().all(is_word_edit_char))
    {
        return EditGranularity::Word;
    }

    EditGranularity::Other
}

fn classify_edit_group(ins: i32, del: i32, inserted: &str, deleted: &str) -> EditGroup {
    let operation = match (ins > 0, del > 0) {
        (true, false) => EditOperation::Insert,
        (false, true) => EditOperation::Delete,
        (true, true) => EditOperation::Replace,
        _ => EditOperation::Other,
    };
    EditGroup {
        granularity: classify_edit_granularity(ins, del, inserted, deleted),
        operation,
    }
}

fn is_word_edit_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_'
}

fn collect_highlight_columns_from_intellisense(data: &IntellisenseData) -> Vec<String> {
    data.get_all_columns_for_highlighting()
}

fn expand_connected_word_range(buf: &TextBuffer, start: usize, end: usize) -> (usize, usize) {
    let text_len = buf.length().max(0) as usize;
    if text_len == 0 {
        return (0, 0);
    }

    let bounded_start = start.min(text_len);
    let bounded_end = end.min(text_len).max(bounded_start);
    let window_start = bounded_start.saturating_sub(HIGHLIGHT_RANGE_EXPANSION_WINDOW);
    let window_end = bounded_end
        .saturating_add(HIGHLIGHT_RANGE_EXPANSION_WINDOW)
        .min(text_len);

    let Some(window_text) = buf.text_range(window_start as i32, window_end as i32) else {
        return (bounded_start, bounded_end);
    };

    let bytes = window_text.as_bytes();
    let mut rel_start = bounded_start.saturating_sub(window_start).min(bytes.len());
    let mut rel_end = bounded_end.saturating_sub(window_start).min(bytes.len());

    while rel_start > 0 && crate::sql_text::is_identifier_byte(bytes[rel_start - 1]) {
        rel_start -= 1;
    }

    while rel_end < bytes.len() && crate::sql_text::is_identifier_byte(bytes[rel_end]) {
        rel_end += 1;
    }

    (
        window_start.saturating_add(rel_start).min(text_len),
        window_start.saturating_add(rel_end).min(text_len),
    )
}

fn editor_viewport_range(editor: &TextEditor, buffer: &TextBuffer) -> Option<(usize, usize)> {
    let text_len = buffer.length().max(0) as usize;
    if text_len == 0 {
        return Some((0, 0));
    }

    if !editor.visible_r() {
        return None;
    }

    let mut editor = editor.clone();
    let h = editor.h();
    let text_size = editor.text_size().max(1);
    if h <= 0 {
        return None;
    }

    // FLTK scroll_row is 1-based in practice; normalize to 0-based line count.
    let top_row = editor.scroll_row().max(1).saturating_sub(1);
    let start_pos = editor
        .skip_lines(0, top_row, true)
        .clamp(0, buffer.length());
    let visible_rows = (h / text_size).max(1).saturating_add(2);
    let end_pos = editor
        .skip_lines(start_pos, visible_rows, true)
        .clamp(start_pos, buffer.length());

    let line_start = buffer.line_start(start_pos).max(0) as usize;
    let line_end = buffer.line_end(end_pos).max(0) as usize;
    let start = line_start.min(text_len);
    let end = line_end.min(text_len).max(start);
    Some((start, end))
}

fn infer_cursor_after_edit(pos: i32, ins: i32, text_len: usize) -> usize {
    let base = pos.max(0) as usize;
    let inserted = ins.max(0) as usize;
    base.saturating_add(inserted).min(text_len)
}

fn compute_edited_range(pos: i32, ins: i32, del: i32, text_len: usize) -> Option<(usize, usize)> {
    if pos < 0 {
        return None;
    }

    let start = (pos as usize).min(text_len);
    let inserted = ins.max(0) as usize;
    let deleted = del.max(0) as usize;
    let changed_len = inserted.max(deleted);
    let end = start.saturating_add(changed_len).min(text_len);

    Some((start, end))
}

fn needs_full_rehighlight(buf: &TextBuffer, pos: i32, ins: i32, deleted_text: &str) -> bool {
    let mut changed_text = String::new();

    if !deleted_text.is_empty() {
        changed_text.push_str(deleted_text);
    }

    if ins > 0 && pos >= 0 {
        let insert_end = pos.saturating_add(ins).min(buf.length());
        if let Some(inserted_text) = buf.text_range(pos, insert_end) {
            changed_text.push_str(&inserted_text);
        }
    }

    if changed_text.is_empty() {
        return false;
    }

    if has_stateful_sql_delimiter(&changed_text) {
        return true;
    }

    if pos < 0 {
        return false;
    }

    let sample_start = pos.saturating_sub(2);
    let sample_end = pos
        .saturating_add(ins.max(0))
        .saturating_add(2)
        .min(buf.length());
    let nearby = buf.text_range(sample_start, sample_end).unwrap_or_default();

    has_stateful_sql_delimiter(&nearby)
}

fn has_stateful_sql_delimiter(text: &str) -> bool {
    text.contains("/*")
        || text.contains("*/")
        || text.contains("--")
        || text.contains("'")
        || text.contains("q'")
        || text.contains("Q'")
        || text.contains("nq'")
        || text.contains("NQ'")
        || text.contains("Nq'")
        || text.contains("nQ'")
}

#[allow(dead_code)]
fn style_before(style_buffer: &TextBuffer, pos: i32) -> Option<char> {
    if pos <= 0 {
        return None;
    }

    let end = pos.min(style_buffer.length());
    let start = end.saturating_sub(1);
    style_buffer
        .text_range(start, end)
        .and_then(|text| text.chars().next())
}

#[allow(dead_code)]
fn is_string_or_comment_style(style: char) -> bool {
    style == STYLE_COMMENT || style == STYLE_STRING
}

#[cfg(test)]
mod execution_state_tests {
    use super::{
        classify_edit_group, load_mutex_bool, BufferEdit, EditGranularity, EditOperation,
        QueryProgress, SqlEditorWidget, UndoDelta, UndoSnapshot, WordUndoRedoState,
    };
    use fltk::app;
    use std::ptr::NonNull;
    use std::sync::Arc;
    use std::sync::Mutex;

    fn build_edit(start: usize, deleted_text: &str, inserted_text: &str) -> BufferEdit {
        BufferEdit {
            start,
            deleted_len: deleted_text.len(),
            inserted_text: inserted_text.to_string(),
            deleted_text: deleted_text.to_string(),
        }
    }

    #[test]
    fn finalize_execution_state_clears_running_and_cancel_flags() {
        let query_running = Arc::new(Mutex::new(true));
        let cancel_flag = Arc::new(Mutex::new(true));

        SqlEditorWidget::finalize_execution_state(&query_running, &cancel_flag);

        assert!(!load_mutex_bool(&query_running));
        assert!(!load_mutex_bool(&cancel_flag));
    }

    #[test]
    fn reset_word_undo_state_reinitializes_history_safely() {
        let undo_state = Arc::new(Mutex::new(WordUndoRedoState {
            anchor: UndoSnapshot::new("SELECT 1".to_string(), 8),
            current: UndoSnapshot::new("SELECT 2".to_string(), 8),
            deltas: vec![UndoDelta {
                start: 7,
                deleted_text: "1".to_string(),
                inserted_text: "2".to_string(),
                before_cursor: 8,
                after_cursor: 8,
                group_id: 1,
            }],
            history_total_bytes: "12".len(),
            index: 1,
            active_group: Some((classify_edit_group(1, 1, "2", "1"), 1)),
            next_group_id: 2,
            applying_history: true,
        }));

        SqlEditorWidget::reset_word_undo_state(&undo_state);

        let state = undo_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(state.anchor, UndoSnapshot::new(String::new(), 0));
        assert_eq!(state.current, UndoSnapshot::new(String::new(), 0));
        assert!(state.deltas.is_empty());
        assert_eq!(state.history_total_bytes, 0);
        assert_eq!(state.index, 0);
        assert!(state.active_group.is_none());
        assert_eq!(state.next_group_id, 1);
        assert!(!state.applying_history);
    }

    #[test]
    fn take_keyup_debounce_timeout_handle_clears_slot() {
        let fake_handle: app::TimeoutHandle = NonNull::<()>::dangling().as_ptr();
        let handle_slot = Arc::new(Mutex::new(Some(fake_handle)));

        let taken = SqlEditorWidget::take_keyup_debounce_timeout_handle(&handle_slot);

        assert_eq!(taken, Some(fake_handle));
        assert!(handle_slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_none());
    }

    #[test]
    fn invalidate_keyup_debounce_increments_generation_when_slot_is_empty() {
        let generation = Arc::new(Mutex::new(0_u64));
        let handle_slot = Arc::new(Mutex::new(None::<app::TimeoutHandle>));

        let next = SqlEditorWidget::invalidate_keyup_debounce(&generation, &handle_slot);

        assert_eq!(next, 1);
        assert_eq!(
            *generation
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            1
        );
        assert!(handle_slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_none());
    }

    #[test]
    fn finalize_execution_state_is_idempotent_when_already_reset() {
        let query_running = Arc::new(Mutex::new(false));
        let cancel_flag = Arc::new(Mutex::new(false));

        SqlEditorWidget::finalize_execution_state(&query_running, &cancel_flag);

        assert!(!load_mutex_bool(&query_running));
        assert!(!load_mutex_bool(&cancel_flag));
    }

    #[test]
    fn handle_progress_channel_disconnected_finalizes_and_emits_batch_finished() {
        let query_running = Arc::new(Mutex::new(true));
        let cancel_flag = Arc::new(Mutex::new(true));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_for_callback = observed.clone();
        let progress_callback: Arc<Mutex<Option<Box<dyn FnMut(QueryProgress)>>>> =
            Arc::new(Mutex::new(Some(Box::new(move |progress| {
                observed_for_callback
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push(progress);
            }))));

        SqlEditorWidget::handle_progress_channel_disconnected(
            &progress_callback,
            &query_running,
            &cancel_flag,
        );

        assert!(!load_mutex_bool(&query_running));
        assert!(!load_mutex_bool(&cancel_flag));
        let callbacks = observed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(callbacks.len(), 1);
        assert!(matches!(callbacks[0], QueryProgress::BatchFinished));
    }

    #[test]
    fn break_current_query_connection_without_connection_is_noop() {
        assert!(SqlEditorWidget::break_current_query_connection(None).is_ok());
    }

    #[test]
    fn classify_edit_group_distinguishes_insert_and_delete_for_word_edits() {
        let insert_group = classify_edit_group(1, 0, "a", "");
        let delete_group = classify_edit_group(0, 1, "", "a");

        assert_eq!(insert_group.granularity, EditGranularity::Word);
        assert_eq!(delete_group.granularity, EditGranularity::Word);
        assert_eq!(insert_group.operation, EditOperation::Insert);
        assert_eq!(delete_group.operation, EditOperation::Delete);
        assert_ne!(insert_group, delete_group);
    }

    #[test]
    fn undo_history_keeps_pre_delete_snapshot_after_word_typing() {
        let mut state = WordUndoRedoState::new(String::new());

        state.record_snapshot("abc".to_string(), classify_edit_group(1, 0, "abc", ""));
        state.record_snapshot("ab".to_string(), classify_edit_group(0, 1, "", "c"));

        assert_eq!(
            state.history_texts(),
            vec!["".to_string(), "abc".to_string(), "ab".to_string()]
        );
        let snapshots = state.history_snapshots();
        assert_eq!(snapshots[2].cursor_pos, 2);
        assert_eq!(state.index, 2);
    }

    #[test]
    fn record_edit_sets_cursor_to_end_of_inserted_text() {
        let mut state = WordUndoRedoState::new(String::new());
        let edit = build_edit(0, "", "한글");

        state.record_edit(&edit, classify_edit_group(2, 0, "한글", ""));

        assert_eq!(
            state.history_texts(),
            vec!["".to_string(), "한글".to_string()]
        );
        let snapshots = state.history_snapshots();
        assert_eq!(snapshots[1].cursor_pos, "한글".len());
    }

    #[test]
    fn record_edit_sets_cursor_to_delete_start_for_deletion() {
        let mut state = WordUndoRedoState::new("abcd".to_string());
        let edit = build_edit(1, "bc", "");

        state.record_edit(&edit, classify_edit_group(0, 2, "", "bc"));

        assert_eq!(
            state.history_texts(),
            vec!["abcd".to_string(), "ad".to_string()]
        );
        let snapshots = state.history_snapshots();
        assert_eq!(snapshots[1].cursor_pos, 1);
    }

    #[test]
    fn record_edit_merges_korean_ime_replace_sequence_into_single_undo_step() {
        let mut state = WordUndoRedoState::new(String::new());

        state.record_edit(
            &build_edit(0, "", "ㅎ"),
            classify_edit_group("ㅎ".len() as i32, 0, "ㅎ", ""),
        );
        state.record_edit(
            &build_edit(0, "ㅎ", "하"),
            classify_edit_group("하".len() as i32, "ㅎ".len() as i32, "하", "ㅎ"),
        );
        state.record_edit(
            &build_edit(0, "하", "한"),
            classify_edit_group("한".len() as i32, "하".len() as i32, "한", "하"),
        );

        assert_eq!(
            state.history_texts(),
            vec!["".to_string(), "한".to_string()]
        );
        let snapshots = state.history_snapshots();
        assert_eq!(snapshots[1].cursor_pos, "한".len());
        assert_eq!(snapshots.len().saturating_sub(1), 1);
    }

    #[test]
    fn record_edit_merges_korean_ime_delete_insert_sequence_into_single_undo_step() {
        let mut state = WordUndoRedoState::new(String::new());

        state.record_edit(
            &build_edit(0, "", "ㅎ"),
            classify_edit_group("ㅎ".len() as i32, 0, "ㅎ", ""),
        );
        state.record_edit(
            &build_edit(0, "ㅎ", ""),
            classify_edit_group(0, "ㅎ".len() as i32, "", "ㅎ"),
        );
        state.record_edit(
            &build_edit(0, "", "하"),
            classify_edit_group("하".len() as i32, 0, "하", ""),
        );
        state.record_edit(
            &build_edit(0, "하", ""),
            classify_edit_group(0, "하".len() as i32, "", "하"),
        );
        state.record_edit(
            &build_edit(0, "", "한"),
            classify_edit_group("한".len() as i32, 0, "한", ""),
        );

        assert_eq!(
            state.history_texts(),
            vec!["".to_string(), "한".to_string()]
        );
        let snapshots = state.history_snapshots();
        assert_eq!(snapshots[1].cursor_pos, "한".len());
        assert_eq!(snapshots.len().saturating_sub(1), 1);
    }

    #[test]
    fn take_undo_group_reverts_grouped_korean_ime_sequence() {
        let mut state = WordUndoRedoState::new(String::new());
        state.record_edit(
            &build_edit(0, "", "ㅎ"),
            classify_edit_group("ㅎ".len() as i32, 0, "ㅎ", ""),
        );
        state.record_edit(
            &build_edit(0, "ㅎ", "하"),
            classify_edit_group("하".len() as i32, "ㅎ".len() as i32, "하", "ㅎ"),
        );
        state.record_edit(
            &build_edit(0, "하", "한"),
            classify_edit_group("한".len() as i32, "하".len() as i32, "한", "하"),
        );

        let undo_group = state.take_undo_group();

        assert_eq!(undo_group.len(), 3);
        assert_eq!(state.current.text, "");
        assert_eq!(state.index, 0);
    }

    #[test]
    fn take_redo_group_reapplies_grouped_korean_ime_sequence() {
        let mut state = WordUndoRedoState::new(String::new());
        state.record_edit(
            &build_edit(0, "", "ㅎ"),
            classify_edit_group("ㅎ".len() as i32, 0, "ㅎ", ""),
        );
        state.record_edit(
            &build_edit(0, "ㅎ", "하"),
            classify_edit_group("하".len() as i32, "ㅎ".len() as i32, "하", "ㅎ"),
        );
        state.record_edit(
            &build_edit(0, "하", "한"),
            classify_edit_group("한".len() as i32, "하".len() as i32, "한", "하"),
        );
        let _ = state.take_undo_group();

        let redo_group = state.take_redo_group();

        assert_eq!(redo_group.len(), 3);
        assert_eq!(state.current.text, "한");
        assert_eq!(state.index, 3);
    }

    #[test]
    fn record_edit_does_not_merge_word_edits_across_lines() {
        let mut state = WordUndoRedoState::new("abc\ndef".to_string());

        state.record_edit(&build_edit(3, "", "x"), classify_edit_group(1, 0, "x", ""));
        state.record_edit(&build_edit(8, "", "y"), classify_edit_group(1, 0, "y", ""));

        assert_eq!(
            state.history_texts(),
            vec![
                "abc\ndef".to_string(),
                "abcx\ndef".to_string(),
                "abcx\ndefy".to_string()
            ]
        );
        assert_eq!(state.index, 2);
    }

    #[test]
    fn record_edit_does_not_merge_word_edits_for_different_words_same_line() {
        let mut state = WordUndoRedoState::new("alpha beta".to_string());

        state.record_edit(&build_edit(5, "", "x"), classify_edit_group(1, 0, "x", ""));
        state.record_edit(&build_edit(11, "", "y"), classify_edit_group(1, 0, "y", ""));

        assert_eq!(
            state.history_texts(),
            vec![
                "alpha beta".to_string(),
                "alphax beta".to_string(),
                "alphax betay".to_string()
            ]
        );
        assert_eq!(state.index, 2);
    }
}

#[cfg(test)]
mod explain_plan_tests {
    use super::SqlEditorWidget;

    #[test]
    fn render_explain_plan_includes_line_numbers() {
        let plan = vec![
            "Plan hash value: 1".to_string(),
            "TABLE ACCESS FULL".to_string(),
        ];
        let rendered = SqlEditorWidget::render_explain_plan(&plan);
        assert!(rendered.contains("  1: Plan hash value: 1"));
        assert!(rendered.contains("  2: TABLE ACCESS FULL"));
    }

    #[test]
    fn render_explain_plan_diff_reports_added_and_removed() {
        let previous = vec![
            "SELECT STATEMENT".to_string(),
            "TABLE ACCESS FULL T1".to_string(),
        ];
        let current = vec![
            "SELECT STATEMENT".to_string(),
            "INDEX RANGE SCAN IDX_T1".to_string(),
        ];

        let diff = SqlEditorWidget::render_explain_plan_diff(&previous, &current);
        assert!(diff.contains("Added lines: 1"));
        assert!(diff.contains("Removed lines: 1"));
        assert!(diff.contains("+ INDEX RANGE SCAN IDX_T1"));
        assert!(diff.contains("- TABLE ACCESS FULL T1"));
    }
}

#[cfg(test)]
mod sql_editor_tests;
