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
use mysql::prelude::Queryable;
use std::any::Any;
use std::collections::VecDeque;
use std::panic::{self, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use crate::db::{
    ColumnInfo, ConnectionInfo, QueryExecutor, QueryResult, SharedConnection, TableColumnDetail,
};
use crate::ui::constants::*;
use crate::ui::font_settings::{configured_editor_profile, FontProfile};
use crate::ui::intellisense::{IntellisenseData, IntellisensePopup};
use crate::ui::query_history::{history_snapshot, QueryHistoryDialog};
use crate::ui::syntax_highlight::STYLE_DEFAULT;
use crate::ui::syntax_highlight::{
    create_style_table_with, HighlightData, SqlHighlighter, STYLE_STRING,
};
use crate::ui::text_buffer_access;
use crate::ui::theme;
use crate::ui::ResultTabRequest;
use crate::utils::{AppConfig, QueryHistoryEntry};
use oracle::Connection;

mod execution;
mod formatter;
mod intellisense;
mod intellisense_host;
mod intellisense_state;
// 공통 파싱/토큰 유틸(실행, 인텔리센스, 포맷팅 공통 경로)
pub(crate) mod query_text;

use self::intellisense_state::{
    IntellisenseCompletionRange, IntellisensePopupTransitionState, IntellisenseRuntimeState,
};

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
const EDITOR_TOP_PADDING: i32 = 4;
const ALERT_RETRY_INTERVAL_SECONDS: f64 = 0.25;

fn is_window_shown_and_visible(shown: bool, visible: bool) -> bool {
    shown && visible
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnPollPendingAction {
    None,
    Refresh,
    Clear,
    RefreshThenClear,
}

impl ColumnPollPendingAction {
    fn request_refresh(&mut self) {
        *self = match *self {
            Self::None => Self::Refresh,
            Self::Clear => Self::RefreshThenClear,
            current => current,
        };
    }

    fn request_clear(&mut self) {
        *self = match *self {
            Self::None => Self::Clear,
            Self::Refresh => Self::RefreshThenClear,
            current => current,
        };
    }

    fn should_refresh(self) -> bool {
        matches!(self, Self::Refresh | Self::RefreshThenClear)
    }

    fn should_clear(self, has_columns_loading: bool) -> bool {
        matches!(self, Self::Clear | Self::RefreshThenClear) && !has_columns_loading
    }
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

struct BufferCallbackSuppressionGuard {
    flag: Arc<Mutex<bool>>,
}

impl Drop for BufferCallbackSuppressionGuard {
    fn drop(&mut self) {
        store_mutex_bool(&self.flag, false);
    }
}

fn load_mutex_i32_option(slot: &Arc<Mutex<Option<i32>>>) -> Option<i32> {
    match slot.lock() {
        Ok(guard) => *guard,
        Err(poisoned) => *poisoned.into_inner(),
    }
}

fn store_mutex_i32_option(slot: &Arc<Mutex<Option<i32>>>, value: Option<i32>) {
    match slot.lock() {
        Ok(mut guard) => *guard = value,
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            *guard = value;
        }
    }
}

fn try_mark_query_running(query_running: &Arc<Mutex<bool>>) -> bool {
    match query_running.lock() {
        Ok(mut guard) => {
            if *guard {
                false
            } else {
                *guard = true;
                true
            }
        }
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            if *guard {
                false
            } else {
                *guard = true;
                true
            }
        }
    }
}

#[derive(Default)]
struct PendingAlertState {
    queue: VecDeque<String>,
    pump_scheduled: bool,
}

include!("undo_history.rs");

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
    WorkerPanicked {
        message: String,
    },
    StatementFinished {
        index: usize,
        result: QueryResult,
        connection_name: String,
        timed_out: bool,
    },
    BatchFinished,
    MetadataRefreshNeeded,
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
pub(crate) enum LocalScopeKind {
    Statement,
    PackageBody,
    Routine,
    DeclareBlock,
    Block,
    Loop,
}

#[derive(Clone)]
pub(crate) struct LocalScope {
    parent: Option<usize>,
    start: usize,
    end: usize,
    depth: usize,
    kind: LocalScopeKind,
}

#[derive(Clone)]
pub(crate) struct LocalSymbolEntry {
    scope_id: usize,
    name: String,
    upper: String,
    declared_at: usize,
}

#[derive(Clone)]
pub(crate) struct IntellisenseAnalysis {
    statement_start: usize,
    statement_end: usize,
    context: Arc<crate::ui::intellisense_context::CursorContext>,
    local_scopes: Arc<[LocalScope]>,
    local_symbols: Arc<[LocalSymbolEntry]>,
    text_bind_names: Arc<[String]>,
}

#[derive(Clone)]
pub(crate) struct RoutineSymbolCacheEntry {
    buffer_revision: u64,
    statement_start: usize,
    statement_end: usize,
    statement_tokens: Arc<[SqlToken]>,
    token_ends: Arc<[usize]>,
    local_scopes: Arc<[LocalScope]>,
    local_symbols: Arc<[LocalSymbolEntry]>,
    text_bind_names: Arc<[String]>,
}

#[derive(Clone)]
pub(crate) struct IntellisenseParseCacheEntry {
    buffer_revision: u64,
    cursor_pos: i32,
    analysis: Arc<IntellisenseAnalysis>,
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
struct MySqlQueryCancelContext {
    connection_info: ConnectionInfo,
    connection_id: u32,
}

impl Drop for MySqlQueryCancelContext {
    fn drop(&mut self) {
        self.connection_info.clear_password();
    }
}

include!("highlighting.rs");

#[derive(Clone)]
pub struct SqlEditorWidget {
    group: Flex,
    editor: TextEditor,
    buffer: TextBuffer,
    style_buffer: TextBuffer,
    connection: SharedConnection,
    execute_callback: Arc<Mutex<Option<Box<dyn FnMut(&QueryResult)>>>>,
    result_tab_callback: Arc<Mutex<Option<Box<dyn FnMut(ResultTabRequest)>>>>,
    progress_callback: Arc<Mutex<Option<Box<dyn FnMut(QueryProgress)>>>>,
    progress_sender: mpsc::Sender<QueryProgress>,
    column_sender: mpsc::Sender<ColumnLoadUpdate>,
    ui_action_sender: mpsc::Sender<UiActionResult>,
    query_running: Arc<Mutex<bool>>,
    current_query_connection: Arc<Mutex<Option<Arc<Connection>>>>,
    current_mysql_cancel_context: Arc<Mutex<Option<MySqlQueryCancelContext>>>,
    cancel_flag: Arc<Mutex<bool>>,
    intellisense_data: Arc<Mutex<IntellisenseData>>,
    intellisense_popup: Arc<Mutex<IntellisensePopup>>,
    highlighter: Arc<Mutex<SqlHighlighter>>,
    highlight_shadow: Arc<Mutex<HighlightShadowState>>,
    timeout_input: IntInput,
    status_callback: Arc<Mutex<Option<Box<dyn FnMut(&str)>>>>,
    find_callback: Arc<Mutex<Option<Box<dyn FnMut()>>>>,
    replace_callback: Arc<Mutex<Option<Box<dyn FnMut()>>>>,
    file_drop_callback: Arc<Mutex<Option<Box<dyn FnMut(PathBuf)>>>>,
    intellisense_runtime: Arc<IntellisenseRuntimeState>,
    history_cursor: Arc<Mutex<Option<usize>>>,
    history_original: Arc<Mutex<Option<String>>>,
    history_navigation_entries: Arc<Mutex<Option<Vec<QueryHistoryEntry>>>>,
    applying_history_navigation: Arc<Mutex<bool>>,
    suppress_buffer_callbacks: Arc<Mutex<bool>>,
    undo_redo_state: Arc<Mutex<WordUndoRedoState>>,
    preferred_insert_position: Arc<Mutex<Option<i32>>>,
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

    fn suppress_buffer_callbacks(&self) -> BufferCallbackSuppressionGuard {
        store_mutex_bool(&self.suppress_buffer_callbacks, true);
        BufferCallbackSuppressionGuard {
            flag: self.suppress_buffer_callbacks.clone(),
        }
    }

    fn invalidate_intellisense_after_buffer_edit(&self) {
        self.intellisense_runtime.next_buffer_revision();
        self.intellisense_runtime.next_parse_generation();
        self.intellisense_runtime.clear_parse_cache();
        self.intellisense_runtime.clear_routine_symbol_cache();
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
        let (_, cursor_pos) = Self::editor_cursor_position(&self.editor, &self.buffer);
        // 실행/인텔리센스/포맷 공통 규칙으로 문장 경계를 계산합니다.
        query_text::statement_at_cursor_for_db_type_with_mysql_delimiter(
            &sql,
            cursor_pos,
            Some(self.current_db_type()),
            self.current_mysql_delimiter().as_deref(),
        )
    }

    fn remember_preferred_insert_position(
        slot: &Arc<Mutex<Option<i32>>>,
        buffer: &TextBuffer,
        pos: i32,
    ) {
        let (pos, _) = Self::cursor_position(buffer, pos);
        store_mutex_i32_option(slot, Some(pos));
    }

    fn sync_preferred_insert_position_from_editor(
        slot: &Arc<Mutex<Option<i32>>>,
        editor: &TextEditor,
        buffer: &TextBuffer,
    ) {
        let (pos, _) = Self::editor_cursor_position(editor, buffer);
        Self::remember_preferred_insert_position(slot, buffer, pos);
    }

    fn refresh_editor_display_metrics(editor: &mut TextEditor) {
        // Force FLTK to recalculate internal display metrics before the next
        // pointer hit-test. Without this, a freshly created/activated editor can
        // still hold stale zero-width column metrics until an external redraw.
        let (x, y, w, h) = (editor.x(), editor.y(), editor.w(), editor.h());
        editor.resize(x, y, w, h);
        editor.redraw();
    }

    fn preferred_insert_position_for_external_insert(&self) -> i32 {
        let fallback = self.editor.insert_position();
        let candidate = load_mutex_i32_option(&self.preferred_insert_position).unwrap_or(fallback);
        let (pos, _) = Self::cursor_position(&self.buffer, candidate);
        pos
    }

    fn normalize_statement_for_single_execution(&self, statement: &str) -> String {
        query_text::normalize_single_statement(
            statement,
            Some(self.current_db_type()),
            self.current_mysql_delimiter().as_deref(),
        )
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

    fn invoke_result_tab_callback(
        callback_slot: &Arc<Mutex<Option<Box<dyn FnMut(ResultTabRequest)>>>>,
        request: ResultTabRequest,
    ) {
        let callback = {
            let mut slot = callback_slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            slot.take()
        };

        if let Some(mut cb) = callback {
            let call_result = panic::catch_unwind(AssertUnwindSafe(|| cb(request)));
            let mut slot = callback_slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if slot.is_none() {
                *slot = Some(cb);
            }
            if let Err(payload) = call_result {
                Self::log_callback_panic("result tab callback", payload.as_ref());
            }
            return;
        }

        crate::utils::logging::log_error("sql_editor::callback", "result tab callback is not set");
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
        let result_tab_callback: Arc<Mutex<Option<Box<dyn FnMut(ResultTabRequest)>>>> =
            Arc::new(Mutex::new(None));
        let progress_callback: Arc<Mutex<Option<Box<dyn FnMut(QueryProgress)>>>> =
            Arc::new(Mutex::new(None));
        let (progress_sender, progress_receiver) = mpsc::channel::<QueryProgress>();
        let (column_sender, column_receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let (ui_action_sender, ui_action_receiver) = mpsc::channel::<UiActionResult>();
        let query_running = Arc::new(Mutex::new(false));
        let current_query_connection = Arc::new(Mutex::new(None));
        let current_mysql_cancel_context = Arc::new(Mutex::new(None));
        let cancel_flag = Arc::new(Mutex::new(false));

        let intellisense_data = Arc::new(Mutex::new(IntellisenseData::new()));
        let intellisense_popup = Arc::new(Mutex::new(IntellisensePopup::new()));
        let highlighter = Arc::new(Mutex::new(SqlHighlighter::new()));
        let highlight_shadow = Arc::new(Mutex::new(HighlightShadowState::default()));
        let status_callback: Arc<Mutex<Option<Box<dyn FnMut(&str)>>>> = Arc::new(Mutex::new(None));
        let find_callback: Arc<Mutex<Option<Box<dyn FnMut()>>>> = Arc::new(Mutex::new(None));
        let replace_callback: Arc<Mutex<Option<Box<dyn FnMut()>>>> = Arc::new(Mutex::new(None));
        let file_drop_callback: Arc<Mutex<Option<Box<dyn FnMut(PathBuf)>>>> =
            Arc::new(Mutex::new(None));
        let intellisense_runtime = Arc::new(IntellisenseRuntimeState::new());
        let history_cursor = Arc::new(Mutex::new(None::<usize>));
        let history_original = Arc::new(Mutex::new(None::<String>));
        let history_navigation_entries = Arc::new(Mutex::new(None::<Vec<QueryHistoryEntry>>));
        let applying_history_navigation = Arc::new(Mutex::new(false));
        let suppress_buffer_callbacks = Arc::new(Mutex::new(false));
        let undo_redo_state = Arc::new(Mutex::new(WordUndoRedoState::new(String::new())));
        let preferred_insert_position = Arc::new(Mutex::new(None::<i32>));

        let mut widget = Self {
            group,
            editor,
            buffer,
            style_buffer,
            connection,
            execute_callback,
            result_tab_callback,
            progress_callback: progress_callback.clone(),
            progress_sender,
            column_sender,
            ui_action_sender,
            query_running: query_running.clone(),
            current_query_connection,
            current_mysql_cancel_context,
            cancel_flag,
            intellisense_data,
            intellisense_popup,
            highlighter,
            highlight_shadow,
            timeout_input,
            status_callback,
            find_callback,
            replace_callback,
            file_drop_callback,
            intellisense_runtime,
            history_cursor,
            history_original,
            history_navigation_entries,
            applying_history_navigation,
            suppress_buffer_callbacks,
            undo_redo_state,
            preferred_insert_position,
        };

        widget.setup_intellisense();
        widget.setup_word_undo_redo();
        widget.setup_syntax_highlighting();
        widget.sync_db_type_from_connection();
        widget.setup_progress_handler(progress_receiver, progress_callback, query_running);
        widget.setup_column_loader(column_receiver);
        widget.setup_ui_action_handler(ui_action_receiver);

        widget
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
                                SqlEditorWidget::invoke_progress_callback(
                                    &progress_callback,
                                    message,
                                );
                                continue;
                            }
                            QueryProgress::PromptInput { prompt, response } => {
                                let value = SqlEditorWidget::prompt_input_dialog(prompt);
                                let _ = response.send(value);
                                app::awake();
                            }
                            QueryProgress::StatementFinished {
                                result,
                                connection_name,
                                timed_out,
                                ..
                            } => {
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

    fn setup_ui_action_handler(&self, ui_action_receiver: mpsc::Receiver<UiActionResult>) {
        let widget = self.clone();

        let receiver: Arc<Mutex<mpsc::Receiver<UiActionResult>>> =
            Arc::new(Mutex::new(ui_action_receiver));

        fn schedule_poll(
            receiver: Arc<Mutex<mpsc::Receiver<UiActionResult>>>,
            widget: SqlEditorWidget,
        ) {
            if widget.group.was_deleted() {
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
                                    let plan_text =
                                        SqlEditorWidget::render_explain_plan(&plan_lines);
                                    let request =
                                        SqlEditorWidget::build_explain_plan_result_request(
                                            &plan_text,
                                        );
                                    SqlEditorWidget::invoke_result_tab_callback(
                                        &widget.result_tab_callback,
                                        request,
                                    );
                                    widget.emit_status("Explain plan loaded");
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
                                        let request =
                                            SqlEditorWidget::build_quick_describe_result_request(
                                                &object_name,
                                                &columns,
                                            );
                                        SqlEditorWidget::invoke_result_tab_callback(
                                            &widget.result_tab_callback,
                                            request,
                                        );
                                        widget.emit_status(&format!(
                                            "Describe loaded for {}",
                                            object_name.to_uppercase()
                                        ));
                                    }
                                }
                                Ok(QuickDescribeData::Text { title, content }) => {
                                    let request = SqlEditorWidget::build_text_result_request(
                                        &title,
                                        &content,
                                        "Describe loaded",
                                    );
                                    SqlEditorWidget::invoke_result_tab_callback(
                                        &widget.result_tab_callback,
                                        request,
                                    );
                                    widget.emit_status("Describe loaded");
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
                return;
            }

            app::add_timeout3(0.05, move |_| {
                schedule_poll(receiver.clone(), widget.clone());
            });
        }

        schedule_poll(receiver, widget);
    }

    pub fn explain_current(&self) {
        let Some(sql) = self.statement_at_cursor_text() else {
            SqlEditorWidget::show_alert_dialog("No SQL at cursor");
            return;
        };

        if !try_mark_query_running(&self.query_running) {
            let _ = self
                .ui_action_sender
                .send(UiActionResult::QueryAlreadyRunning);
            app::awake();
            return;
        }

        store_mutex_bool(&self.cancel_flag, false);

        let query_timeout = Self::parse_timeout(&self.timeout_input.value());
        let connection = self.connection.clone();
        let sender = self.ui_action_sender.clone();
        let query_running = self.query_running.clone();
        let current_query_connection = self.current_query_connection.clone();
        let current_mysql_cancel_context = self.current_mysql_cancel_context.clone();
        let cancel_flag = self.cancel_flag.clone();

        set_cursor(Cursor::Wait);
        app::flush();

        thread::spawn(move || {
            let sender_fallback = sender.clone();
            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                let Some(mut conn_guard) = crate::db::try_lock_connection_with_activity(
                    &connection,
                    "Generating explain plan",
                ) else {
                    let _ = sender.send(UiActionResult::QueryAlreadyRunning);
                    app::awake();
                    return;
                };

                let result = match conn_guard.db_type() {
                    crate::db::DatabaseType::Oracle => match conn_guard.require_live_connection() {
                        Ok(db_conn) => {
                            SqlEditorWidget::set_current_query_connection(
                                &current_query_connection,
                                Some(Arc::clone(&db_conn)),
                            );
                            if load_mutex_bool(&cancel_flag) {
                                let _ = db_conn.break_execution();
                            }
                            QueryExecutor::get_explain_plan(db_conn.as_ref(), &sql)
                                .map_err(|err| err.to_string())
                        }
                        Err(message) => Err(message),
                    },
                    crate::db::DatabaseType::MySQL => {
                        SqlEditorWidget::run_mysql_action_with_timeout(
                            &mut conn_guard,
                            &current_mysql_cancel_context,
                            &cancel_flag,
                            query_timeout,
                            "Generating explain plan",
                            |mysql_conn| {
                                crate::db::query::mysql_executor::MysqlExecutor::get_explain_plan(
                                    mysql_conn, &sql,
                                )
                            },
                        )
                    }
                };

                let _ = sender.send(UiActionResult::ExplainPlan(result));
                app::awake();
            }));

            SqlEditorWidget::set_current_query_connection(&current_query_connection, None);
            SqlEditorWidget::set_current_mysql_cancel_context(&current_mysql_cancel_context, None);
            SqlEditorWidget::finalize_execution_state(&query_running, &cancel_flag);

            if let Err(payload) = result {
                let panic_msg = SqlEditorWidget::panic_payload_to_string(payload.as_ref());
                crate::utils::logging::log_error(
                    "sql_editor::explain",
                    &format!("sql_editor::explain thread panicked: {panic_msg}"),
                );
                let _ = sender_fallback.send(UiActionResult::ExplainPlan(Err(format!(
                    "Internal error: {}",
                    panic_msg
                ))));
                app::awake();
            }
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

    fn build_text_result_request(label: &str, content: &str, message: &str) -> ResultTabRequest {
        let rows = if content.is_empty() {
            Vec::new()
        } else {
            content
                .lines()
                .enumerate()
                .map(|(idx, line)| vec![(idx + 1).to_string(), line.to_string()])
                .collect()
        };
        let result = QueryResult {
            sql: String::new(),
            columns: vec![
                ColumnInfo {
                    name: "Line".to_string(),
                    data_type: "NUMBER".to_string(),
                },
                ColumnInfo {
                    name: "Text".to_string(),
                    data_type: "VARCHAR2".to_string(),
                },
            ],
            row_count: rows.len(),
            rows,
            execution_time: Duration::from_secs(0),
            message: message.to_string(),
            is_select: true,
            success: true,
        };
        ResultTabRequest {
            label: label.to_string(),
            result,
        }
    }

    fn build_explain_plan_result_request(plan_text: &str) -> ResultTabRequest {
        Self::build_text_result_request("Explain Plan", plan_text, "Explain plan loaded")
    }

    fn build_quick_describe_result_request(
        object_name: &str,
        columns: &[TableColumnDetail],
    ) -> ResultTabRequest {
        let rows = columns
            .iter()
            .map(|col| {
                vec![
                    col.name.clone(),
                    col.get_type_display(),
                    if col.nullable {
                        "YES".to_string()
                    } else {
                        "NO".to_string()
                    },
                    if col.is_primary_key {
                        "PK".to_string()
                    } else {
                        String::new()
                    },
                ]
            })
            .collect::<Vec<_>>();
        let result = QueryResult {
            sql: String::new(),
            columns: vec![
                ColumnInfo {
                    name: "Column Name".to_string(),
                    data_type: "VARCHAR2".to_string(),
                },
                ColumnInfo {
                    name: "Data Type".to_string(),
                    data_type: "VARCHAR2".to_string(),
                },
                ColumnInfo {
                    name: "Nullable".to_string(),
                    data_type: "VARCHAR2".to_string(),
                },
                ColumnInfo {
                    name: "PK".to_string(),
                    data_type: "VARCHAR2".to_string(),
                },
            ],
            row_count: rows.len(),
            rows,
            execution_time: Duration::from_secs(0),
            message: format!("Describe loaded for {}", object_name.to_uppercase()),
            is_select: true,
            success: true,
        };
        ResultTabRequest {
            label: format!("Describe: {}", object_name.to_uppercase()),
            result,
        }
    }

    fn emit_status(&self, message: &str) {
        Self::invoke_status_callback(&self.status_callback, message);
    }

    fn spawn_tracked_transaction_action<F>(
        &self,
        activity_label: &'static str,
        panic_context: &'static str,
        make_ui_result: fn(Result<(), String>) -> UiActionResult,
        oracle_action: F,
        mysql_sql: &'static str,
        query_timeout: Option<Duration>,
    ) where
        F: FnOnce(Arc<Connection>) -> Result<(), String> + Send + 'static,
    {
        if !try_mark_query_running(&self.query_running) {
            let _ = self
                .ui_action_sender
                .send(UiActionResult::QueryAlreadyRunning);
            app::awake();
            return;
        }

        store_mutex_bool(&self.cancel_flag, false);

        let connection = self.connection.clone();
        let sender = self.ui_action_sender.clone();
        let query_running = self.query_running.clone();
        let current_query_connection = self.current_query_connection.clone();
        let current_mysql_cancel_context = self.current_mysql_cancel_context.clone();
        let cancel_flag = self.cancel_flag.clone();

        set_cursor(Cursor::Wait);
        app::flush();

        thread::spawn(move || {
            let sender_fallback = sender.clone();
            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                let Some(mut conn_guard) =
                    crate::db::try_lock_connection_with_activity(&connection, activity_label)
                else {
                    let _ = sender.send(UiActionResult::QueryAlreadyRunning);
                    app::awake();
                    return;
                };

                let result = match conn_guard.db_type() {
                    crate::db::DatabaseType::Oracle => match conn_guard.require_live_connection() {
                        Ok(db_conn) => {
                            SqlEditorWidget::set_current_query_connection(
                                &current_query_connection,
                                Some(Arc::clone(&db_conn)),
                            );
                            if load_mutex_bool(&cancel_flag) {
                                let _ = db_conn.break_execution();
                            }
                            oracle_action(db_conn)
                        }
                        Err(message) => Err(message),
                    },
                    crate::db::DatabaseType::MySQL => {
                        SqlEditorWidget::run_mysql_action_with_timeout(
                            &mut conn_guard,
                            &current_mysql_cancel_context,
                            &cancel_flag,
                            query_timeout,
                            activity_label,
                            |mysql_conn| mysql_conn.query_drop(mysql_sql),
                        )
                    }
                };

                let _ = sender.send(make_ui_result(result));
                app::awake();
            }));

            SqlEditorWidget::set_current_query_connection(&current_query_connection, None);
            SqlEditorWidget::set_current_mysql_cancel_context(&current_mysql_cancel_context, None);
            SqlEditorWidget::finalize_execution_state(&query_running, &cancel_flag);

            if let Err(payload) = result {
                let panic_msg = SqlEditorWidget::panic_payload_to_string(payload.as_ref());
                crate::utils::logging::log_error(
                    panic_context,
                    &format!("{panic_context} thread panicked: {panic_msg}"),
                );
                let _ = sender_fallback.send(make_ui_result(Err(format!(
                    "Internal error: {}",
                    panic_msg
                ))));
                app::awake();
            }
        });
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
        let query_timeout = Self::parse_timeout(&self.timeout_input.value());
        self.spawn_tracked_transaction_action(
            "Commit transaction",
            "sql_editor::commit",
            UiActionResult::Commit,
            |db_conn| db_conn.commit().map_err(|err| err.to_string()),
            "COMMIT",
            query_timeout,
        );
    }

    pub fn rollback(&self) {
        let query_timeout = Self::parse_timeout(&self.timeout_input.value());
        self.spawn_tracked_transaction_action(
            "Rollback transaction",
            "sql_editor::rollback",
            UiActionResult::Rollback,
            |db_conn| db_conn.rollback().map_err(|err| err.to_string()),
            "ROLLBACK",
            query_timeout,
        );
    }

    pub fn cancel_current(&self) {
        // Set cancel flag immediately so the execution thread can check it
        store_mutex_bool(&self.cancel_flag, true);

        let current_query_connection = self.current_query_connection.clone();
        let current_mysql_cancel_context = self.current_mysql_cancel_context.clone();
        let cancel_flag = self.cancel_flag.clone();
        let query_running = self.query_running.clone();
        let sender = self.ui_action_sender.clone();
        thread::spawn(move || {
            let mut conn =
                SqlEditorWidget::clone_current_query_connection(&current_query_connection);
            let mut mysql_cancel_context =
                SqlEditorWidget::clone_current_mysql_cancel_context(&current_mysql_cancel_context);

            if !SqlEditorWidget::is_query_running_flag(&query_running)
                && conn.is_none()
                && mysql_cancel_context.is_none()
            {
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
                    mysql_cancel_context = SqlEditorWidget::clone_current_mysql_cancel_context(
                        &current_mysql_cancel_context,
                    );
                    if conn.is_some() || mysql_cancel_context.is_some() {
                        break;
                    }
                }
            }

            if !SqlEditorWidget::is_query_running_flag(&query_running)
                && conn.is_none()
                && mysql_cancel_context.is_none()
            {
                // This editor is idle. Do not attempt to cancel through the
                // global DB connection because that can interrupt a query that
                // is currently running in a different editor tab.
                store_mutex_bool(&cancel_flag, false);
                let _ = sender.send(UiActionResult::Cancel(Ok(())));
                app::awake();
                return;
            }

            if conn.is_none() && mysql_cancel_context.is_none() {
                // Execution may still be initializing the DB connection.
                // Wait briefly so a single cancel click can still interrupt reliably.
                for _ in 0..40 {
                    if !load_mutex_bool(&cancel_flag) {
                        break;
                    }
                    thread::sleep(Duration::from_millis(25));
                    conn =
                        SqlEditorWidget::clone_current_query_connection(&current_query_connection);
                    mysql_cancel_context = SqlEditorWidget::clone_current_mysql_cancel_context(
                        &current_mysql_cancel_context,
                    );
                    if conn.is_some() || mysql_cancel_context.is_some() {
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

            if conn.is_none() && mysql_cancel_context.is_none() {
                // The worker has not published a break-able connection yet.
                // Keep cancel requested so execution stops at the first safe
                // cancellation point, and surface a status update instead of
                // pretending the DB-level break already happened.
                let _ = sender.send(UiActionResult::CancelPending);
                app::awake();
                return;
            }

            let result = if conn.is_some() {
                SqlEditorWidget::break_current_query_connection(conn)
            } else {
                SqlEditorWidget::break_current_mysql_query(mysql_cancel_context, &cancel_flag)
            };

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

    fn clone_current_mysql_cancel_context(
        current_mysql_cancel_context: &Arc<Mutex<Option<MySqlQueryCancelContext>>>,
    ) -> Option<MySqlQueryCancelContext> {
        match current_mysql_cancel_context.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => {
                eprintln!("Warning: MySQL cancel context lock was poisoned; recovering.");
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

    fn break_current_mysql_query(
        context: Option<MySqlQueryCancelContext>,
        cancel_flag: &Arc<Mutex<bool>>,
    ) -> Result<(), String> {
        if !load_mutex_bool(cancel_flag) {
            if let Some(mut context) = context {
                context.connection_info.clear_password();
            }
            return Ok(());
        }

        if let Some(mut context) = context {
            let result = crate::db::query::mysql_executor::MysqlExecutor::cancel_running_query(
                &context.connection_info,
                context.connection_id,
            )
            .map_err(|err| err.to_string());
            context.connection_info.clear_password();
            result
        } else {
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

    fn set_current_mysql_cancel_context(
        current_mysql_cancel_context: &Arc<Mutex<Option<MySqlQueryCancelContext>>>,
        value: Option<MySqlQueryCancelContext>,
    ) {
        match current_mysql_cancel_context.lock() {
            Ok(mut guard) => {
                if let Some(current) = guard.as_mut() {
                    current.connection_info.clear_password();
                }
                *guard = value;
            }
            Err(poisoned) => {
                eprintln!("Warning: MySQL cancel context lock was poisoned; recovering.");
                let mut guard = poisoned.into_inner();
                if let Some(current) = guard.as_mut() {
                    current.connection_info.clear_password();
                }
                *guard = value;
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

    pub fn set_result_tab_callback<F>(&mut self, callback: F)
    where
        F: FnMut(ResultTabRequest) + 'static,
    {
        *self
            .result_tab_callback
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

    fn current_db_type(&self) -> crate::db::connection::DatabaseType {
        match self.connection.lock() {
            Ok(conn_guard) => conn_guard.db_type(),
            Err(poisoned) => poisoned.into_inner().db_type(),
        }
    }

    fn current_mysql_delimiter(&self) -> Option<String> {
        let session = match self.connection.lock() {
            Ok(conn_guard) => {
                if conn_guard.db_type() != crate::db::connection::DatabaseType::MySQL {
                    return None;
                }
                conn_guard.session_state()
            }
            Err(poisoned) => {
                let conn_guard = poisoned.into_inner();
                if conn_guard.db_type() != crate::db::connection::DatabaseType::MySQL {
                    return None;
                }
                conn_guard.session_state()
            }
        };

        let delimiter = match session.lock() {
            Ok(guard) => guard.mysql_delimiter.clone(),
            Err(poisoned) => poisoned.into_inner().mysql_delimiter.clone(),
        };
        delimiter
    }

    fn mysql_delimiter_before_offset(&self, offset: usize) -> Option<String> {
        query_text::active_mysql_delimiter_before_offset(
            &self.buffer.text(),
            offset,
            Some(self.current_db_type()),
            self.current_mysql_delimiter().as_deref(),
        )
    }

    pub(crate) fn sync_db_type_from_connection(&self) {
        self.set_db_type(self.current_db_type());
    }

    pub fn stabilize_display_metrics(&mut self) {
        Self::refresh_editor_display_metrics(&mut self.editor);
        app::redraw();
        app::flush();
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
        Self::refresh_editor_display_metrics(&mut self.editor);
        self.timeout_input.redraw();
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

    pub fn insert_text_at_preferred_position(&mut self, text: &str) {
        let insert_pos = self.preferred_insert_position_for_external_insert();
        let (_, insert_pos_usize) = Self::cursor_position(&self.buffer, insert_pos);
        self.buffer.insert(insert_pos, text);
        let new_pos = insert_pos_usize.saturating_add(text.len());
        self.editor.set_insert_position(new_pos as i32);
        self.editor.show_insert_position();
        Self::remember_preferred_insert_position(
            &self.preferred_insert_position,
            &self.buffer,
            new_pos as i32,
        );
    }

    pub fn select_block_in_direction(&mut self, direction: i32) {
        let selection = self.buffer.selection_position();
        let cursor_pos = self.editor.insert_position().max(0);

        if selection.is_none() || selection == Some((cursor_pos, cursor_pos)) {
            let (start, end) = Self::block_bounds(&self.buffer, &self.highlight_shadow, cursor_pos);
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
            let (block_start, _) =
                Self::block_bounds(&self.buffer, &self.highlight_shadow, prev_pos);
            self.buffer.select(block_start, sel_end);
            self.editor.set_insert_position(block_start);
        } else {
            let buffer_len = self.buffer.length();
            if sel_end >= buffer_len {
                return;
            }
            let next_pos = (sel_end + 1).min(buffer_len.saturating_sub(1));
            let (_, block_end) = Self::block_bounds(&self.buffer, &self.highlight_shadow, next_pos);
            self.buffer.select(sel_start, block_end);
            self.editor.set_insert_position(block_end);
        }
        self.editor.show_insert_position();
    }

    fn block_bounds(
        buffer: &TextBuffer,
        text_shadow: &Arc<Mutex<HighlightShadowState>>,
        pos: i32,
    ) -> (i32, i32) {
        let mut start = text_buffer_access::line_start(buffer, Some(text_shadow), pos).max(0);
        let mut end = text_buffer_access::line_end(buffer, Some(text_shadow), pos).max(start);
        let buffer_len = buffer.length();

        let is_blank = |start: i32, end: i32| {
            let text = text_buffer_access::text_range(buffer, Some(text_shadow), start, end);
            text.trim().is_empty()
        };

        let blank = is_blank(start, end);

        let mut scan = start;
        while scan > 0 {
            let prev_pos = scan.saturating_sub(1);
            let prev_start =
                text_buffer_access::line_start(buffer, Some(text_shadow), prev_pos).max(0);
            let prev_end =
                text_buffer_access::line_end(buffer, Some(text_shadow), prev_pos).max(prev_start);
            if is_blank(prev_start, prev_end) != blank {
                break;
            }
            start = prev_start;
            scan = prev_start;
        }

        let mut scan = end;
        while scan < buffer_len {
            let next_pos = (scan + 1).min(buffer_len.saturating_sub(1));
            let next_start =
                text_buffer_access::line_start(buffer, Some(text_shadow), next_pos).max(0);
            let next_end =
                text_buffer_access::line_end(buffer, Some(text_shadow), next_pos).max(next_start);
            if is_blank(next_start, next_end) != blank {
                break;
            }
            end = next_end;
            scan = next_end;
        }

        (start, end)
    }
}

#[cfg(test)]
mod execution_state_tests {
    use super::{
        classify_edit_group, load_mutex_bool, try_mark_query_running, BufferEdit, EditGranularity,
        EditOperation, IntellisenseRuntimeState, QueryProgress, SqlEditorWidget, UndoDelta,
        UndoSnapshot, WordUndoRedoState,
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
        let runtime = Arc::new(IntellisenseRuntimeState::new());

        let next = SqlEditorWidget::invalidate_keyup_debounce(&runtime);

        assert_eq!(next, 1);
        assert_eq!(runtime.current_keyup_generation(), 1);
        assert!(runtime.take_keyup_timeout_handle().is_none());
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
    fn try_mark_query_running_sets_running_flag_once() {
        let query_running = Arc::new(Mutex::new(false));

        assert!(try_mark_query_running(&query_running));
        assert!(!try_mark_query_running(&query_running));
        assert!(load_mutex_bool(&query_running));
    }

    #[test]
    fn try_mark_query_running_recovers_when_mutex_is_poisoned() {
        let query_running = Arc::new(Mutex::new(false));
        let poison_target = query_running.clone();
        let _ = std::thread::spawn(move || {
            let _guard = poison_target
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            panic!("poison query_running mutex");
        })
        .join();

        assert!(try_mark_query_running(&query_running));
        assert!(load_mutex_bool(&query_running));
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
    fn break_current_mysql_query_without_context_is_noop() {
        let cancel_flag = Arc::new(Mutex::new(true));
        assert!(SqlEditorWidget::break_current_mysql_query(None, &cancel_flag).is_ok());
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
    fn undo_cursor_after_group_moves_to_end_of_restored_text_for_deletion() {
        let mut state = WordUndoRedoState::new("abcdef".to_string());
        // Simulate an out-of-cursor edit (e.g. programmatic replace) where
        // the current cursor is far from the edited span.
        state.current.cursor_pos = 6;
        state.record_edit(
            &build_edit(1, "bc", ""),
            classify_edit_group(0, 2, "", "bc"),
        );

        let undo_group = state.take_undo_group();
        assert_eq!(state.current.text, "abcdef");
        assert_eq!(state.current.cursor_pos, 6);

        let undo_cursor = state.undo_cursor_after_group(&undo_group);
        assert_eq!(undo_cursor, 3);
    }

    #[test]
    fn undo_cursor_after_group_restores_previous_cursor_for_insertion() {
        let mut state = WordUndoRedoState::new("abc".to_string());
        state.record_edit(&build_edit(3, "", "x"), classify_edit_group(1, 0, "x", ""));

        let undo_group = state.take_undo_group();
        assert_eq!(state.current.text, "abc");

        let undo_cursor = state.undo_cursor_after_group(&undo_group);
        assert_eq!(undo_cursor, 3);
    }

    #[test]
    fn undo_cursor_after_group_uses_group_start_for_grouped_insertion_with_trailing_text() {
        let mut state = WordUndoRedoState::new("xyz".to_string());
        state.current.cursor_pos = 0;
        state.record_edit(&build_edit(0, "", "a"), classify_edit_group(1, 0, "a", ""));
        state.record_edit(&build_edit(1, "", "s"), classify_edit_group(1, 0, "s", ""));
        state.record_edit(&build_edit(2, "", "d"), classify_edit_group(1, 0, "d", ""));
        state.record_edit(&build_edit(3, "", "f"), classify_edit_group(1, 0, "f", ""));

        let undo_group = state.take_undo_group();
        assert_eq!(undo_group.len(), 4);
        assert_eq!(state.current.text, "xyz");

        let undo_cursor = state.undo_cursor_after_group(&undo_group);
        assert_eq!(undo_cursor, 0);
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

    #[test]
    fn record_programmatic_edit_preserves_explicit_cursor_mapping() {
        let mut state = WordUndoRedoState::new("select  1".to_string());

        state.record_programmatic_edit(&build_edit(0, "select  1", "SELECT 1"), 8, 7);

        assert_eq!(
            state.history_texts(),
            vec!["select  1".to_string(), "SELECT 1".to_string()]
        );
        let snapshots = state.history_snapshots();
        assert_eq!(snapshots[0].cursor_pos, 9);
        assert_eq!(snapshots[1].cursor_pos, 7);

        let undo_group = state.take_undo_group();
        assert_eq!(undo_group.len(), 1);
        assert_eq!(undo_group[0].before_cursor, 8);
        assert_eq!(undo_group[0].after_cursor, 7);
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
}

#[cfg(test)]
mod sql_editor_tests;
