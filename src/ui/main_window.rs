use fltk::{
    app,
    button::{Button, CheckButton},
    dialog::{FileDialog, FileDialogType},
    draw::set_cursor,
    enums::{Cursor, FrameType},
    frame::Frame,
    group::{Flex, FlexType, Group, Tile},
    input::IntInput,
    menu::MenuBar,
    prelude::*,
    text::TextBuffer,
    widget::Widget,
    window::Window,
};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

use crate::app_icon;
use crate::db::{
    create_shared_connection, format_connection_busy_message, lock_connection_with_activity,
    try_lock_connection_with_activity, ObjectBrowser, QueryResult, SharedConnection,
};
use crate::ui::constants::*;
use crate::ui::result_table::ResultGridSqlExecuteCallback;
use crate::ui::theme;
use crate::ui::{
    font_settings, show_settings_dialog, ConnectionDialog, FindReplaceDialog, HighlightData,
    IntellisenseData, MenuBarBuilder, ObjectBrowserWidget, QueryHistoryDialog, QueryProgress,
    QueryTabId, QueryTabsWidget, ResultTabsWidget, SqlAction, SqlEditorWidget,
};
use crate::utils::{malloc_trim_process, AppConfig, QueryHistory};

fn try_set_mutex_flag(flag: &Arc<Mutex<bool>>) -> bool {
    match flag.lock() {
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

fn clear_mutex_flag(flag: &Arc<Mutex<bool>>) {
    match flag.lock() {
        Ok(mut guard) => *guard = false,
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            *guard = false;
        }
    }
}

#[derive(Clone)]
struct SchemaUpdate {
    data: IntellisenseData,
    highlight_data: HighlightData,
    connection_generation: u64,
}

#[derive(Clone)]
struct QueryEditorTab {
    tab_id: QueryTabId,
    base_label: String,
    sql_editor: SqlEditorWidget,
    sql_buffer: TextBuffer,
    current_file: Option<PathBuf>,
    pristine_text: String,
    current_text_len: usize,
    is_dirty: bool,
    schema_generation: u64,
}

#[derive(Clone)]
struct QueryProgressContext {
    result_tab_offset: usize,
    execution_target: Option<usize>,
    fetch_row_counts: HashMap<usize, usize>,
    last_fetch_status_update: Instant,
}

impl QueryProgressContext {
    fn new(result_tab_offset: usize, execution_target: Option<usize>) -> Self {
        Self {
            result_tab_offset,
            execution_target,
            fetch_row_counts: HashMap::new(),
            last_fetch_status_update: Instant::now(),
        }
    }
}

pub struct AppState {
    pub connection: SharedConnection,
    query_tabs: QueryTabsWidget,
    query_top_group: Group,
    pub query_split_bar: Frame,
    editor_tabs: Vec<QueryEditorTab>,
    active_editor_tab_id: QueryTabId,
    next_editor_tab_number: usize,
    pub sql_editor: SqlEditorWidget,
    pub sql_buffer: TextBuffer,
    schema_intellisense_data: IntellisenseData,
    schema_highlight_data: HighlightData,
    query_timeout_input: IntInput,
    pub result_tabs: ResultTabsWidget,
    result_toolbar: Flex,
    result_edit_check: CheckButton,
    result_insert_btn: Button,
    result_delete_btn: Button,
    result_save_btn: Button,
    result_cancel_btn: Button,
    execute_btn: Button,
    query_cancel_btn: Button,
    explain_btn: Button,
    commit_btn: Button,
    rollback_btn: Button,
    pub result_tab_offset: usize,
    result_grid_execution_target: Option<usize>,
    progress_contexts: HashMap<QueryTabId, QueryProgressContext>,
    pub object_browser: ObjectBrowserWidget,
    pub status_bar: Frame,
    pub current_file: Arc<Mutex<Option<PathBuf>>>,
    pub popups: Arc<Mutex<Vec<Window>>>,
    pub window: Window,
    pub right_tile: Tile,
    /// Saved query/result split ratio (0.0–1.0).  `None` means the user has
    /// not adjusted the split bar yet (use default 40%).
    pub query_split_ratio: Arc<Mutex<Option<f64>>>,
    pub connection_info: Arc<Mutex<Option<crate::db::ConnectionInfo>>>,
    has_live_connection: bool,
    pending_connection_metadata_refresh: bool,
    pub config: Arc<Mutex<AppConfig>>,
    status_animation_running: bool,
    status_animation_message: String,
    status_animation_frame: usize,
    schema_sender: Option<std::sync::mpsc::Sender<SchemaUpdate>>,
    file_sender: Option<std::sync::mpsc::Sender<FileActionResult>>,
    schema_refresh_in_progress: Arc<Mutex<bool>>,
    schema_apply_generation: Arc<AtomicU64>,
}

fn set_result_action_button_visibility(toolbar: &mut Flex, button: &mut Button, visible: bool) {
    if visible {
        toolbar.fixed(button, BUTTON_WIDTH_SMALL);
        if !button.visible() {
            button.show();
        }
        button.activate();
    } else {
        button.deactivate();
        if button.visible() {
            button.hide();
        }
        toolbar.fixed(button, 0);
    }
}

impl AppState {
    const STATUS_SPINNER_FRAMES: [&'static str; 10] =
        ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

    fn tab_display_label(tab: &QueryEditorTab) -> String {
        let mut label = match &tab.current_file {
            Some(path) => path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
            None => tab.base_label.clone(),
        };
        if tab.is_dirty {
            label.push('*');
        }
        label
    }

    fn refresh_window_title(&mut self) {
        if let Some(index) = self.find_tab_index(self.active_editor_tab_id) {
            let label = Self::tab_display_label(&self.editor_tabs[index]);
            self.window.set_label(&format!("SPACE Query - {}", label));
            return;
        }
        self.window.set_label("SPACE Query");
    }

    fn find_tab_index(&self, tab_id: QueryTabId) -> Option<usize> {
        self.editor_tabs.iter().position(|tab| tab.tab_id == tab_id)
    }

    fn current_schema_generation(&self) -> u64 {
        self.schema_apply_generation.load(Ordering::Relaxed)
    }

    fn apply_schema_to_tab_if_needed(&mut self, tab_index: usize) {
        let target_generation = self.current_schema_generation();
        let needs_schema_apply = self
            .editor_tabs
            .get(tab_index)
            .map(|tab| tab.schema_generation != target_generation)
            .unwrap_or(false);
        if !needs_schema_apply {
            return;
        }

        let schema_data = self.schema_intellisense_data.clone();
        let highlight_data = self.schema_highlight_data.clone();
        let Some(tab) = self.editor_tabs.get_mut(tab_index) else {
            return;
        };

        *tab.sql_editor
            .get_intellisense_data()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = schema_data;
        tab.sql_editor.update_highlight_data(highlight_data);
        tab.schema_generation = target_generation;
    }

    fn apply_schema_to_active_tab_if_needed(&mut self) {
        if let Some(index) = self.find_tab_index(self.active_editor_tab_id) {
            self.apply_schema_to_tab_if_needed(index);
        }
    }

    fn set_active_editor_tab(&mut self, tab_id: QueryTabId) -> bool {
        let Some(index) = self.find_tab_index(tab_id) else {
            return false;
        };
        let tab = self.editor_tabs[index].clone();
        self.active_editor_tab_id = tab_id;
        self.sql_editor = tab.sql_editor;
        self.sql_buffer = tab.sql_buffer;
        *self
            .current_file
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = tab.current_file;
        self.apply_schema_to_tab_if_needed(index);
        self.refresh_window_title();
        true
    }

    fn is_any_query_running(&self) -> bool {
        self.editor_tabs
            .iter()
            .any(|tab| tab.sql_editor.is_query_running())
    }

    fn tab_sql_text(&self, tab_id: QueryTabId) -> Option<String> {
        self.find_tab_index(tab_id)
            .map(|index| self.editor_tabs[index].sql_buffer.text())
    }

    fn tab_file_path(&self, tab_id: QueryTabId) -> Option<PathBuf> {
        self.find_tab_index(tab_id)
            .and_then(|index| self.editor_tabs[index].current_file.clone())
    }

    fn tab_display_name(&self, tab_id: QueryTabId) -> Option<String> {
        self.find_tab_index(tab_id)
            .map(|index| Self::tab_display_label(&self.editor_tabs[index]))
    }

    fn is_tab_dirty(&self, tab_id: QueryTabId) -> bool {
        self.find_tab_index(tab_id)
            .map(|index| self.editor_tabs[index].is_dirty)
            .unwrap_or(false)
    }

    fn set_tab_dirty(&mut self, tab_id: QueryTabId, is_dirty: bool) {
        let Some(index) = self.find_tab_index(tab_id) else {
            return;
        };
        if self.editor_tabs[index].is_dirty == is_dirty {
            return;
        }
        self.editor_tabs[index].is_dirty = is_dirty;
        let label = Self::tab_display_label(&self.editor_tabs[index]);
        self.query_tabs.set_tab_label(tab_id, &label);
        if self.active_editor_tab_id == tab_id {
            self.refresh_window_title();
        }
    }

    fn set_tab_pristine_text(&mut self, tab_id: QueryTabId, text: String) {
        let Some(index) = self.find_tab_index(tab_id) else {
            return;
        };
        self.editor_tabs[index].current_text_len = text.len();
        self.editor_tabs[index].pristine_text = text;
        self.set_tab_dirty(tab_id, false);
    }

    fn refresh_tab_dirty_from_text(&mut self, tab_id: QueryTabId, current_text: &str) {
        let Some(index) = self.find_tab_index(tab_id) else {
            return;
        };
        let is_dirty = self.editor_tabs[index].pristine_text != current_text;
        self.set_tab_dirty(tab_id, is_dirty);
    }

    fn on_tab_buffer_modified(&mut self, tab_id: QueryTabId, ins: i32, del: i32, buf: &TextBuffer) {
        let Some(index) = self.find_tab_index(tab_id) else {
            return;
        };

        let inserted = ins.max(0) as usize;
        let deleted = del.max(0) as usize;
        let tab = &mut self.editor_tabs[index];
        tab.current_text_len = tab
            .current_text_len
            .saturating_add(inserted)
            .saturating_sub(deleted);

        if tab.current_text_len != tab.pristine_text.len() {
            self.set_tab_dirty(tab_id, true);
            return;
        }

        let current_text = buf.text();
        self.refresh_tab_dirty_from_text(tab_id, &current_text);
    }

    fn set_tab_file_path(&mut self, tab_id: QueryTabId, path: Option<PathBuf>) {
        let Some(index) = self.find_tab_index(tab_id) else {
            return;
        };
        self.editor_tabs[index].current_file = path.clone();
        let label = Self::tab_display_label(&self.editor_tabs[index]);
        self.query_tabs.set_tab_label(tab_id, &label);
        if self.active_editor_tab_id == tab_id {
            *self
                .current_file
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = path;
            self.refresh_window_title();
        }
    }

    fn find_tab_id_by_file_name(&self, file_name: &str) -> Option<QueryTabId> {
        let target = file_name.trim();
        if target.is_empty() {
            return None;
        }
        self.editor_tabs.iter().find_map(|tab| {
            let current_name = tab
                .current_file
                .as_ref()
                .and_then(|path| path.file_name())
                .map(|name| name.to_string_lossy().to_string())?;
            if current_name.eq_ignore_ascii_case(target) {
                Some(tab.tab_id)
            } else {
                None
            }
        })
    }

    fn activate_editor_tab(&mut self, tab_id: QueryTabId) -> bool {
        self.query_tabs.select(tab_id);
        if self.set_active_editor_tab(tab_id) {
            self.sql_editor.focus();
            true
        } else {
            false
        }
    }

    fn set_status_message(&mut self, message: &str) {
        self.status_animation_running = false;
        self.status_animation_message.clear();
        self.status_animation_frame = 0;
        let conn_info = self
            .connection_info
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        self.status_bar
            .set_label(&format_status(message, &conn_info));
    }

    fn start_status_animation(&mut self, message: &str) {
        self.status_animation_running = true;
        self.status_animation_message.clear();
        self.status_animation_message.push_str(message);
        self.status_animation_frame = 0;
        self.render_status_animation_frame();
    }

    fn update_status_animation(&mut self, message: &str) {
        if !self.status_animation_running {
            self.start_status_animation(message);
            return;
        }
        self.status_animation_message.clear();
        self.status_animation_message.push_str(message);
        self.render_status_animation_frame();
    }

    fn tick_status_animation(&mut self) {
        if !self.status_animation_running {
            return;
        }
        self.status_animation_frame =
            (self.status_animation_frame + 1) % Self::STATUS_SPINNER_FRAMES.len();
        self.render_status_animation_frame();
    }

    fn render_status_animation_frame(&mut self) {
        if !self.status_animation_running {
            return;
        }
        let frame = Self::STATUS_SPINNER_FRAMES[self.status_animation_frame];
        let conn_info = self
            .connection_info
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        self.status_bar.set_label(&format_status(
            &format!("{} {}", frame, self.status_animation_message),
            &conn_info,
        ));
    }

    fn refresh_result_edit_controls(&mut self) {
        let can_edit = self.result_tabs.can_current_begin_edit_mode();
        let edit_active = self.result_tabs.is_current_edit_mode_enabled();
        let save_pending = self.result_tabs.is_current_save_pending();
        let query_running = self.is_any_query_running();
        let show_edit_check = can_edit;
        if show_edit_check {
            self.result_toolbar
                .fixed(&self.result_edit_check, BUTTON_WIDTH_SMALL);
            if !self.result_edit_check.visible() {
                self.result_edit_check.show();
            }
            if query_running || save_pending {
                self.result_edit_check.deactivate();
            } else {
                self.result_edit_check.activate();
            }
        } else {
            self.result_edit_check.deactivate();
            if self.result_edit_check.visible() {
                self.result_edit_check.hide();
            }
            self.result_toolbar.fixed(&self.result_edit_check, 0);
        }
        let desired_checked = edit_active && can_edit;
        if self.result_edit_check.value() != desired_checked {
            self.result_edit_check.set(desired_checked);
        }

        let show_action_buttons = edit_active && can_edit;
        let actions_enabled = show_action_buttons && !save_pending && !query_running;
        set_result_action_button_visibility(
            &mut self.result_toolbar,
            &mut self.result_insert_btn,
            show_action_buttons,
        );
        set_result_action_button_visibility(
            &mut self.result_toolbar,
            &mut self.result_delete_btn,
            show_action_buttons,
        );
        set_result_action_button_visibility(
            &mut self.result_toolbar,
            &mut self.result_save_btn,
            show_action_buttons,
        );
        set_result_action_button_visibility(
            &mut self.result_toolbar,
            &mut self.result_cancel_btn,
            show_action_buttons,
        );
        if show_action_buttons {
            if actions_enabled {
                self.result_insert_btn.activate();
                self.result_delete_btn.activate();
                self.result_save_btn.activate();
                self.result_cancel_btn.activate();
                self.result_edit_check.activate();
            } else {
                self.result_insert_btn.deactivate();
                self.result_delete_btn.deactivate();
                self.result_save_btn.deactivate();
                self.result_cancel_btn.deactivate();
                self.result_edit_check.deactivate();
            }
        }
        self.result_toolbar.layout();
        self.result_toolbar.redraw();
    }

    /// Enable or disable connection-dependent toolbar buttons and menu items.
    /// Execute remains enabled even when disconnected so scripts can CONNECT.
    /// Call this whenever the connection state changes
    /// (connect, disconnect, or connection lost).
    fn refresh_connection_dependent_controls(&mut self) {
        // If the connection lock is held (query is running) treat the state as
        // connected so we never disable buttons mid-execution.
        let is_connected = self
            .connection
            .try_lock()
            .map(|g| g.is_connected())
            .unwrap_or(true);

        // Regression guard: keep Execute enabled even when disconnected.
        // Script execution may begin with CONNECT (or @script that contains CONNECT),
        // so re-coupling this button to `is_connected` would break reconnect workflows.
        self.execute_btn.activate();

        if is_connected {
            self.query_cancel_btn.activate();
            self.explain_btn.activate();
            self.commit_btn.activate();
            self.rollback_btn.activate();
        } else {
            self.query_cancel_btn.deactivate();
            self.explain_btn.deactivate();
            self.commit_btn.deactivate();
            self.rollback_btn.deactivate();
        }

        // Sync the Disconnect menu item so it is only active when connected.
        if let Some(menu) = app::widget_from_id::<MenuBar>("main_menu") {
            if let Some(mut item) = menu.find_item("&File/&Disconnect") {
                if is_connected {
                    item.activate();
                } else {
                    item.deactivate();
                }
            }
        }
    }
}

const FETCH_STATUS_UPDATE_INTERVAL: Duration = Duration::from_millis(250);
const STATUS_ANIMATION_INTERVAL: f64 = 0.08;

/// 접속 정보를 상태 표시줄 메시지 끝에 붙는 헬퍼
fn format_status(msg: &str, conn_info: &Option<crate::db::ConnectionInfo>) -> String {
    match conn_info {
        Some(info) => format!("{} | {}", msg, info.name),
        None => msg.to_string(),
    }
}

pub struct MainWindow {
    state: Arc<Mutex<AppState>>,
}

#[derive(Clone)]
enum ConnectionResult {
    Success(crate::db::ConnectionInfo),
    Failure(String),
}

enum FileActionResult {
    OpenInNewTab {
        path: PathBuf,
        result: Result<String, String>,
    },
    Export {
        path: PathBuf,
        row_count: usize,
        result: Result<(), String>,
    },
}

enum SaveTabOutcome {
    Saved,
    Cancelled,
    Failed(String),
}

fn should_ignore_query_progress_when_disconnected(
    has_live_connection: bool,
    has_running_queries: bool,
) -> bool {
    !has_live_connection && !has_running_queries
}

fn should_run_global_batch_cleanup(has_running_queries: bool) -> bool {
    !has_running_queries
}

fn should_cancel_fallback_editor(fallback_editor_running: bool) -> bool {
    fallback_editor_running
}

fn validate_result_edit_action_allowed(has_running_queries: bool) -> Result<(), String> {
    if has_running_queries {
        Err("A query is running. Wait for completion before editing result rows.".to_string())
    } else {
        Ok(())
    }
}

fn acquire_sql_editor_if_idle(state: &Arc<Mutex<AppState>>) -> Option<SqlEditorWidget> {
    let editor = {
        let guard = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if guard.is_any_query_running() {
            None
        } else {
            Some(guard.sql_editor.clone())
        }
    };

    if editor.is_none() {
        SqlEditorWidget::show_alert_dialog(&crate::db::format_connection_busy_message());
    }

    editor
}

fn resolve_result_tab_offset(tab_count: usize, target: Option<usize>) -> usize {
    target.filter(|idx| *idx < tab_count).unwrap_or(tab_count)
}

fn resolve_progress_tab_index(
    tab_count: usize,
    result_tab_offset: usize,
    target: Option<usize>,
    statement_index: usize,
) -> usize {
    let base_offset = target
        .filter(|idx| *idx < tab_count)
        .unwrap_or_else(|| result_tab_offset.min(tab_count));
    base_offset.saturating_add(statement_index)
}

fn resolve_active_progress_tab_index(
    state: &AppState,
    tab_id: QueryTabId,
    statement_index: usize,
) -> Option<usize> {
    let has_running_queries = state.sql_editor.is_query_running()
        || state
            .editor_tabs
            .iter()
            .any(|tab| tab.sql_editor.is_query_running());
    if should_ignore_query_progress_when_disconnected(
        state.has_live_connection,
        has_running_queries,
    ) {
        return None;
    }

    let context = state.progress_contexts.get(&tab_id)?;

    Some(resolve_progress_tab_index(
        state.result_tabs.tab_count(),
        context.result_tab_offset,
        context.execution_target,
        statement_index,
    ))
}

impl MainWindow {
    fn clone_result_tabs_for_edit_action(
        state: &Arc<Mutex<AppState>>,
    ) -> Result<ResultTabsWidget, String> {
        let mut guard = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Err(err) = validate_result_edit_action_allowed(guard.is_any_query_running()) {
            guard.set_status_message(&err);
            guard.refresh_result_edit_controls();
            return Err(err);
        }
        Ok(guard.result_tabs.clone())
    }

    fn start_status_animation_timer(state: &Arc<Mutex<AppState>>) {
        let weak_state = Arc::downgrade(state);
        app::add_timeout3(STATUS_ANIMATION_INTERVAL, move |_| {
            let Some(state_for_tick) = weak_state.upgrade() else {
                return;
            };
            let should_reschedule = match state_for_tick.try_lock() {
                Ok(mut s) => {
                    s.tick_status_animation();
                    s.status_animation_running
                }
                Err(_) => true,
            };
            if should_reschedule {
                MainWindow::start_status_animation_timer(&state_for_tick);
            }
        });
    }

    fn transition_to_disconnected_state(state: &mut AppState, error_message: Option<&str>) {
        *state
            .connection_info
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        state.has_live_connection = false;
        state.pending_connection_metadata_refresh = false;

        // Disconnection can happen mid-stream (network drop,
        // explicit disconnect while a worker is still unwinding). Ensure every
        // result grid exits streaming mode immediately so edit controls are not
        // left disabled waiting for a BatchFinished event that may never arrive.
        state.result_tabs.finish_all_streaming();
        state.progress_contexts.clear();

        let recovered_save_states = state.result_tabs.clear_orphaned_save_requests();
        let recovered_edit_states = state.result_tabs.clear_orphaned_query_edit_backups();
        if recovered_save_states > 0 {
            state.set_status_message("Disconnected (save interrupted; staged edits preserved)");
        } else if recovered_edit_states > 0 {
            state.set_status_message("Disconnected (staged result-grid edits restored)");
        } else {
            state.set_status_message("Disconnected");
        }
        let reset_data = IntellisenseData::new();
        let reset_highlight = HighlightData::new();
        Self::update_schema_snapshot(state, reset_data, reset_highlight);
        state.apply_schema_to_active_tab_if_needed();

        // Clear object browser cache and tree so stale metadata from the previous
        // connection is not visible when connecting to a different database.
        state.object_browser.clear_on_disconnect();

        // DO NOT clear result_tabs on disconnect.
        //
        // Users frequently disconnect and reconnect (e.g. session timeout, switching
        // environments) and still need to read the query results that were already
        // fetched. Clearing tabs here would destroy that data silently.
        //
        // Staged edit data (pending INSERT/UPDATE/DELETE rows) must also survive
        // across a disconnect so the user can reconnect and retry the save without
        // losing their edits.
        //
        // If you are tempted to add result_tabs.clear() here — don't.
        // Let the user close individual tabs manually when they are done with them.

        // Reset session state (bind variables, settings, etc.) so they do not
        // leak into a subsequent connection, e.g. when disconnected by the health
        // disconnect path rather than via an explicit "Disconnect" menu action.
        if let Ok(conn_guard) = state.connection.try_lock() {
            let session = conn_guard.session_state();
            // Drop the connection guard before locking the session to preserve
            // the single-lock-at-a-time invariant.
            drop(conn_guard);
            let lock_result = session.lock();
            match lock_result {
                Ok(mut guard) => guard.reset(),
                Err(poisoned) => {
                    poisoned.into_inner().reset();
                }
            }
        }

        if let Some(message) = error_message {
            crate::utils::logging::log_error("connection", message);
            state
                .result_tabs
                .append_script_output_lines(&[message.to_string()]);
            state.result_tabs.select_script_output();
        }

        state.refresh_connection_dependent_controls();
        // Refresh the result-grid edit toolbar after orphan recovery may have
        // changed pending_save_request, ensuring buttons reflect the final state
        // rather than any intermediate snapshot from before orphan cleanup.
        state.refresh_result_edit_controls();
    }

    fn cancel_all_running_queries(state: &Arc<Mutex<AppState>>) {
        let (running_editors, fallback_editor) = {
            let s = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let running_editors = s
                .editor_tabs
                .iter()
                .filter(|tab| tab.sql_editor.is_query_running())
                .map(|tab| tab.sql_editor.clone())
                .collect::<Vec<_>>();
            (running_editors, s.sql_editor.clone())
        };

        let fallback_editor_running = fallback_editor.is_query_running();

        if should_cancel_fallback_editor(fallback_editor_running) {
            fallback_editor.cancel_current();
        }

        if running_editors.is_empty() {
            return;
        }

        for editor in &running_editors {
            editor.cancel_current();
        }

        let mut s = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        s.set_status_message("Cancelling running queries...");
    }

    fn focus_existing_tab_with_same_file_name(state: &mut AppState, path: &Path) -> bool {
        let Some(file_name) = path
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
        else {
            return false;
        };
        let Some(tab_id) = state.find_tab_id_by_file_name(&file_name) else {
            return false;
        };
        if !state.activate_editor_tab(tab_id) {
            return false;
        }
        state.set_status_message(&format!(
            "{} is already open. Switched to existing tab",
            file_name
        ));
        true
    }

    fn save_tab(
        state: &Arc<Mutex<AppState>>,
        tab_id: QueryTabId,
        force_save_as: bool,
    ) -> SaveTabOutcome {
        let (current_file, sql_text) = {
            let s = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(sql_text) = s.tab_sql_text(tab_id) else {
                return SaveTabOutcome::Cancelled;
            };
            (s.tab_file_path(tab_id), sql_text)
        };

        let target_path = if force_save_as { None } else { current_file }.or_else(|| {
            let mut dialog = FileDialog::new(FileDialogType::BrowseSaveFile);
            dialog.set_filter("SQL Files\t*.sql\nAll Files\t*.*");
            dialog.show();
            let filename = dialog.filename();
            if filename.as_os_str().is_empty() {
                None
            } else {
                Some(filename)
            }
        });

        let Some(path) = target_path else {
            return SaveTabOutcome::Cancelled;
        };

        if let Err(err) = fs::write(&path, &sql_text) {
            return SaveTabOutcome::Failed(err.to_string());
        }

        let mut s = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        s.set_tab_file_path(tab_id, Some(path.clone()));
        s.set_tab_pristine_text(tab_id, sql_text);
        let file_label = path.file_name().unwrap_or_default().to_string_lossy();
        s.set_status_message(&format!("Saved {}", file_label));
        SaveTabOutcome::Saved
    }

    fn confirm_save_if_dirty(
        state: &Arc<Mutex<AppState>>,
        tab_id: QueryTabId,
        action_verb: &str,
    ) -> bool {
        let (is_dirty, tab_label) = {
            let s = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            (s.is_tab_dirty(tab_id), s.tab_display_name(tab_id))
        };
        if !is_dirty {
            return true;
        }

        let tab_label = tab_label.unwrap_or_else(|| "Query".to_string());
        let choice = fltk::dialog::choice2_default(
            &format!(
                "Tab '{}' has unsaved changes.\nDo you want to save before {}?",
                tab_label, action_verb
            ),
            "Cancel",
            "Save",
            "Don't Save",
        );

        match choice {
            Some(1) => match Self::save_tab(state, tab_id, false) {
                SaveTabOutcome::Saved => true,
                SaveTabOutcome::Cancelled => false,
                SaveTabOutcome::Failed(err) => {
                    fltk::dialog::alert_default(&format!("Failed to save SQL file: {}", err));
                    false
                }
            },
            Some(2) => true,
            _ => false,
        }
    }

    fn confirm_save_for_all_dirty_tabs(state: &Arc<Mutex<AppState>>) -> bool {
        let tab_ids = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .query_tabs
            .tab_ids();
        for tab_id in tab_ids {
            if !Self::confirm_save_if_dirty(state, tab_id, "exiting") {
                return false;
            }
        }
        true
    }

    pub fn new() -> Self {
        Self::new_with_config(AppConfig::load())
    }

    pub fn new_with_config(config: AppConfig) -> Self {
        let connection = create_shared_connection();

        let current_group = fltk::group::Group::try_current();

        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let mut window = Window::default()
            .with_size(1200, 800)
            .with_label("SPACE Query")
            .center_screen();
        window.set_id("main_window");
        window.set_color(theme::window_bg());
        app_icon::apply_window_icon(&mut window);

        let mut main_flex = Flex::default_fill();
        main_flex.set_type(FlexType::Column);

        let menu_bar = MenuBarBuilder::build();
        main_flex.fixed(&menu_bar, MENU_BAR_HEIGHT);

        let mut query_toolbar = Flex::default();
        query_toolbar.set_type(FlexType::Row);
        query_toolbar.set_margin(TOOLBAR_SPACING);
        query_toolbar.set_spacing(TOOLBAR_SPACING);

        let mut execute_btn = Button::default()
            .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
            .with_label("@> Execute");
        execute_btn.set_color(theme::button_primary());
        execute_btn.set_label_color(theme::text_primary());
        execute_btn.set_frame(FrameType::RFlatBox);
        query_toolbar.fixed(&execute_btn, BUTTON_WIDTH);

        let mut cancel_btn = Button::default()
            .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
            .with_label("Cancel");
        cancel_btn.set_color(theme::button_cancel());
        cancel_btn.set_label_color(theme::text_primary());
        cancel_btn.set_frame(FrameType::RFlatBox);
        query_toolbar.fixed(&cancel_btn, BUTTON_WIDTH);

        let mut explain_btn = Button::default()
            .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
            .with_label("Explain");
        explain_btn.set_color(theme::button_secondary());
        explain_btn.set_label_color(theme::text_primary());
        explain_btn.set_frame(FrameType::RFlatBox);
        query_toolbar.fixed(&explain_btn, BUTTON_WIDTH);

        let mut clear_btn = Button::default()
            .with_size(BUTTON_WIDTH_SMALL, BUTTON_HEIGHT)
            .with_label("Clear");
        clear_btn.set_color(theme::button_subtle());
        clear_btn.set_label_color(theme::text_secondary());
        clear_btn.set_frame(FrameType::RFlatBox);
        query_toolbar.fixed(&clear_btn, BUTTON_WIDTH_SMALL);

        let mut commit_btn = Button::default()
            .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
            .with_label("Commit");
        commit_btn.set_color(theme::button_success());
        commit_btn.set_label_color(theme::text_primary());
        commit_btn.set_frame(FrameType::RFlatBox);
        query_toolbar.fixed(&commit_btn, BUTTON_WIDTH);

        let mut rollback_btn = Button::default()
            .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
            .with_label("Rollback");
        rollback_btn.set_color(theme::button_danger());
        rollback_btn.set_label_color(theme::text_primary());
        rollback_btn.set_frame(FrameType::RFlatBox);
        query_toolbar.fixed(&rollback_btn, BUTTON_WIDTH);

        let toolbar_spacer = Frame::default();
        query_toolbar.resizable(&toolbar_spacer);

        let mut timeout_label = Frame::default().with_size(85, BUTTON_HEIGHT);
        timeout_label.set_label("Timeout(s)");
        timeout_label.set_label_color(theme::text_muted());
        query_toolbar.fixed(&timeout_label, 85);

        let mut timeout_input = IntInput::default().with_size(NUMERIC_INPUT_WIDTH, BUTTON_HEIGHT);
        timeout_input.set_color(theme::input_bg());
        timeout_input.set_text_color(theme::text_primary());
        timeout_input.set_tooltip("Call timeout in seconds (empty = no timeout)");
        timeout_input.set_value("60");
        query_toolbar.fixed(&timeout_input, NUMERIC_INPUT_WIDTH);

        query_toolbar.end();
        main_flex.fixed(&query_toolbar, RESULT_TOOLBAR_HEIGHT);

        let mut content_flex = Flex::default();
        content_flex.set_type(FlexType::Row);
        content_flex.set_spacing(0);

        let object_browser = ObjectBrowserWidget::new(0, 0, 250, 600, connection.clone());
        let obj_browser_widget = object_browser.get_widget();
        content_flex.fixed(&obj_browser_widget, 250);

        let splitter_width = MAIN_SPLITTER_WIDTH;
        let mut split_bar = Frame::default().with_size(splitter_width, 0);
        split_bar.set_frame(FrameType::FlatBox);
        split_bar.set_color(theme::border());
        split_bar.set_tooltip("Drag to resize panels");

        let drag_state = Arc::new(Mutex::new(None::<(i32, i32)>));
        let mut content_flex_for_split = content_flex.clone();
        let obj_browser_for_split = obj_browser_widget.clone();
        let drag_state_for_split = drag_state;
        split_bar.handle(move |_bar, ev| match ev {
            fltk::enums::Event::Enter | fltk::enums::Event::Move => {
                set_cursor(Cursor::WE);
                true
            }
            fltk::enums::Event::Push => {
                *drag_state_for_split
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                    Some((app::event_x(), obj_browser_for_split.w()));
                true
            }
            fltk::enums::Event::Drag => {
                if let Some((start_x, start_w)) = *drag_state_for_split
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                {
                    let delta = app::event_x() - start_x;
                    let min_left = 180;
                    let min_right = 320;
                    let max_left =
                        (content_flex_for_split.w() - splitter_width - min_right).max(min_left);
                    let mut new_width = start_w + delta;
                    if new_width < min_left {
                        new_width = min_left;
                    } else if new_width > max_left {
                        new_width = max_left;
                    }
                    content_flex_for_split.fixed(&obj_browser_for_split, new_width);
                    content_flex_for_split.layout();
                    app::redraw();
                }
                true
            }
            fltk::enums::Event::Released => {
                *drag_state_for_split
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
                set_cursor(Cursor::WE);
                true
            }
            fltk::enums::Event::Leave => {
                set_cursor(Cursor::Default);
                true
            }
            _ => false,
        });
        content_flex.fixed(&split_bar, splitter_width);

        let mut right_flex = Flex::default();
        right_flex.set_type(FlexType::Column);

        let query_split_ratio: Arc<Mutex<Option<f64>>> = Arc::new(Mutex::new(None));
        let mut right_tile = Tile::new(0, 0, 900, 600, None);
        right_tile.set_frame(FrameType::FlatBox);
        right_tile.set_color(theme::panel_bg());
        let tile_x = right_tile.x();
        let tile_y = right_tile.y();
        let tile_w = right_tile.w().max(1);
        let tile_h = right_tile.h().max(1);
        let max_initial_query_height =
            (tile_h - MIN_RESULTS_HEIGHT - QUERY_SPLIT_BAR_HEIGHT).max(MIN_QUERY_HEIGHT);
        let initial_query_height = 250.clamp(MIN_QUERY_HEIGHT, max_initial_query_height);

        right_tile.begin();
        let mut query_top_group = Group::new(tile_x, tile_y, tile_w, initial_query_height, None);
        query_top_group.set_frame(FrameType::FlatBox);
        query_top_group.set_color(theme::panel_bg());
        query_top_group.begin();
        let mut query_top_flex = Flex::new(tile_x, tile_y, tile_w, initial_query_height, None);
        query_top_flex.set_type(FlexType::Column);

        let mut query_tabs = QueryTabsWidget::new(0, 0, 900, 400);
        let query_tabs_widget = query_tabs.get_widget();
        query_top_flex.add(&query_tabs_widget);
        query_top_flex.resizable(&query_tabs_widget);

        let mut query_tab_toolbar = Flex::default();
        query_tab_toolbar.set_type(FlexType::Row);
        query_tab_toolbar.set_margin(TOOLBAR_SPACING);
        query_tab_toolbar.set_spacing(TOOLBAR_SPACING);

        let mut query_close_tab_btn = Button::default()
            .with_size(BUTTON_WIDTH_LARGE, BUTTON_HEIGHT)
            .with_label("Close Query");
        query_close_tab_btn.set_color(theme::button_subtle());
        query_close_tab_btn.set_label_color(theme::text_secondary());
        query_close_tab_btn.set_frame(FrameType::RFlatBox);
        query_close_tab_btn.set_tooltip("Close the current query tab (Cmd/Ctrl+W)");
        query_tab_toolbar.fixed(&query_close_tab_btn, BUTTON_WIDTH_LARGE);

        let query_tab_toolbar_spacer = Frame::default();
        query_tab_toolbar.resizable(&query_tab_toolbar_spacer);
        query_tab_toolbar.end();
        query_top_flex.fixed(&query_tab_toolbar, RESULT_TOOLBAR_HEIGHT);
        query_top_flex.end();
        query_top_group.resizable(&query_top_flex);
        query_top_group.end();

        let result_y = tile_y + initial_query_height + QUERY_SPLIT_BAR_HEIGHT;
        let result_h = (tile_h - initial_query_height - QUERY_SPLIT_BAR_HEIGHT).max(1);
        let mut result_bottom_group = Group::new(tile_x, result_y, tile_w, result_h, None);
        result_bottom_group.set_frame(FrameType::FlatBox);
        result_bottom_group.set_color(theme::panel_bg());
        result_bottom_group.begin();

        let mut result_bottom_flex = Flex::new(tile_x, result_y, tile_w, result_h, None);
        result_bottom_flex.set_type(FlexType::Column);

        let result_tabs = ResultTabsWidget::new(0, 0, 900, 400);
        let result_widget = result_tabs.get_widget();
        result_bottom_flex.add(&result_widget);
        result_bottom_flex.resizable(&result_widget);

        let mut result_toolbar = Flex::default();
        result_toolbar.set_type(FlexType::Row);
        result_toolbar.set_margin(TOOLBAR_SPACING);
        result_toolbar.set_spacing(TOOLBAR_SPACING);

        let mut close_tab_btn = Button::default()
            .with_size(BUTTON_WIDTH_LARGE, BUTTON_HEIGHT)
            .with_label("Close Result");
        close_tab_btn.set_color(theme::button_subtle());
        close_tab_btn.set_label_color(theme::text_secondary());
        close_tab_btn.set_frame(FrameType::RFlatBox);
        close_tab_btn.set_tooltip("Close the current result tab");
        result_toolbar.fixed(&close_tab_btn, BUTTON_WIDTH_LARGE);

        let mut clear_tabs_btn = Button::default()
            .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
            .with_label("Close All");
        clear_tabs_btn.set_color(theme::button_subtle());
        clear_tabs_btn.set_label_color(theme::text_secondary());
        clear_tabs_btn.set_frame(FrameType::RFlatBox);
        clear_tabs_btn.set_tooltip("Remove all result tabs");
        result_toolbar.fixed(&clear_tabs_btn, BUTTON_WIDTH);

        let mut query_history_btn = Button::default()
            .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
            .with_label("History");
        query_history_btn.set_color(theme::button_subtle());
        query_history_btn.set_label_color(theme::text_secondary());
        query_history_btn.set_frame(FrameType::RFlatBox);
        query_history_btn.set_tooltip("Open query history");
        result_toolbar.fixed(&query_history_btn, BUTTON_WIDTH);

        let spacer = Frame::default();
        result_toolbar.resizable(&spacer);

        let mut edit_mode_check = CheckButton::default()
            .with_size(BUTTON_WIDTH_SMALL, BUTTON_HEIGHT)
            .with_label("Edit");
        edit_mode_check.set_tooltip("Enable staged edit mode for the current result tab");
        edit_mode_check.hide();
        result_toolbar.fixed(&edit_mode_check, 0);

        let mut edit_insert_btn = Button::default()
            .with_size(BUTTON_WIDTH_SMALL, BUTTON_HEIGHT)
            .with_label("Insert");
        edit_insert_btn.set_color(theme::button_secondary());
        edit_insert_btn.set_label_color(theme::text_primary());
        edit_insert_btn.set_frame(FrameType::RFlatBox);
        edit_insert_btn.set_tooltip("Add a staged row (DB is not changed until Save)");
        result_toolbar.fixed(&edit_insert_btn, BUTTON_WIDTH_SMALL);

        let mut edit_delete_btn = Button::default()
            .with_size(BUTTON_WIDTH_SMALL, BUTTON_HEIGHT)
            .with_label("Delete");
        edit_delete_btn.set_color(theme::button_danger());
        edit_delete_btn.set_label_color(theme::text_primary());
        edit_delete_btn.set_frame(FrameType::RFlatBox);
        edit_delete_btn.set_tooltip("Delete selected row(s) in staged edit mode");
        result_toolbar.fixed(&edit_delete_btn, BUTTON_WIDTH_SMALL);

        let mut edit_save_btn = Button::default()
            .with_size(BUTTON_WIDTH_SMALL, BUTTON_HEIGHT)
            .with_label("Save");
        edit_save_btn.set_color(theme::button_success());
        edit_save_btn.set_label_color(theme::text_primary());
        edit_save_btn.set_frame(FrameType::RFlatBox);
        edit_save_btn.set_tooltip("Apply staged edits to DB");
        result_toolbar.fixed(&edit_save_btn, BUTTON_WIDTH_SMALL);

        let mut edit_cancel_btn = Button::default()
            .with_size(BUTTON_WIDTH_SMALL, BUTTON_HEIGHT)
            .with_label("Cancel");
        edit_cancel_btn.set_color(theme::button_cancel());
        edit_cancel_btn.set_label_color(theme::text_primary());
        edit_cancel_btn.set_frame(FrameType::RFlatBox);
        edit_cancel_btn.set_tooltip("Discard staged edits and restore rows");
        edit_insert_btn.hide();
        edit_delete_btn.hide();
        edit_save_btn.hide();
        edit_cancel_btn.hide();
        result_toolbar.fixed(&edit_insert_btn, 0);
        result_toolbar.fixed(&edit_delete_btn, 0);
        result_toolbar.fixed(&edit_save_btn, 0);
        result_toolbar.fixed(&edit_cancel_btn, 0);
        result_toolbar.end();
        result_bottom_flex.fixed(&result_toolbar, RESULT_TOOLBAR_HEIGHT);
        result_bottom_flex.end();
        result_bottom_group.resizable(&result_bottom_flex);

        result_bottom_group.end();

        let mut query_split_bar = Frame::default().with_size(tile_w, QUERY_SPLIT_BAR_HEIGHT);
        query_split_bar.set_frame(FrameType::FlatBox);
        query_split_bar.set_color(theme::border());
        query_split_bar.set_tooltip("Drag to resize query and result panes");
        query_split_bar.resize(
            tile_x,
            tile_y + initial_query_height,
            tile_w,
            QUERY_SPLIT_BAR_HEIGHT,
        );

        right_tile.end();

        let query_split_ratio_for_tile = query_split_ratio.clone();
        let mut query_top_group_for_tile = query_top_group.clone();
        let mut query_split_bar_for_tile = query_split_bar.clone();
        let split_drag_active = Arc::new(Mutex::new(false));
        let split_drag_active_for_tile = split_drag_active;
        right_tile.handle(move |tile, ev| {
            const SPLIT_GRAB_MARGIN: i32 = 6;
            match ev {
                fltk::enums::Event::Push => {
                    // Avoid event_mouse_button() because FLTK can emit non-standard button
                    // values on some devices, which panics when cast to MouseButton.
                    if app::event_button() == fltk::app::MouseButton::Left as i32 {
                        let split_top = query_split_bar_for_tile.y();
                        let split_bottom = split_top + query_split_bar_for_tile.h();
                        let near_split = (app::event_y() >= split_top - SPLIT_GRAB_MARGIN)
                            && (app::event_y() <= split_bottom + SPLIT_GRAB_MARGIN);
                        if near_split {
                            *split_drag_active_for_tile
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
                            return true;
                        }
                    }
                    false
                }
                fltk::enums::Event::Drag => {
                    if *split_drag_active_for_tile
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                    {
                        let right_height = tile.h();
                        if right_height > 0 {
                            let max_query_height =
                                (right_height - MIN_RESULTS_HEIGHT - QUERY_SPLIT_BAR_HEIGHT)
                                    .max(MIN_QUERY_HEIGHT);
                            let split_pos = app::event_y() - tile.y();
                            let desired_query_height =
                                split_pos.clamp(MIN_QUERY_HEIGHT, max_query_height);
                            query_top_group_for_tile.resize(
                                tile.x(),
                                tile.y(),
                                tile.w(),
                                desired_query_height,
                            );
                            MainWindow::clamp_query_split_with(
                                tile,
                                &mut query_top_group_for_tile,
                                &mut query_split_bar_for_tile,
                            );
                            // Store the ratio for proportional resize.
                            *query_split_ratio_for_tile
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                                Some(desired_query_height as f64 / right_height as f64);
                        }
                        return true;
                    }
                    false
                }
                fltk::enums::Event::Released => {
                    if std::mem::replace(
                        &mut *split_drag_active_for_tile
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()),
                        false,
                    ) {
                        MainWindow::clamp_query_split_with(
                            tile,
                            &mut query_top_group_for_tile,
                            &mut query_split_bar_for_tile,
                        );
                        // Store final ratio after release.
                        let right_height = tile.h();
                        if right_height > 0 {
                            let query_height = query_top_group_for_tile.h();
                            *query_split_ratio_for_tile
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                                Some(query_height as f64 / right_height as f64);
                        }
                        return true;
                    }
                    false
                }
                _ => false,
            }
        });

        let mut first_tab_id = query_tabs.add_tab("Query 1");
        let mut first_tab_group = query_tabs.tab_group(first_tab_id);
        if first_tab_group.is_none() {
            eprintln!(
                "Warning: initial query tab group was missing; attempting recovery by creating a new tab."
            );
            let recovered_tab_id = query_tabs.add_tab("Query 1");
            first_tab_group = query_tabs.tab_group(recovered_tab_id);
            if first_tab_group.is_some() {
                first_tab_id = recovered_tab_id;
            }
        }
        let first_tab_group = first_tab_group.unwrap_or_else(|| query_top_group.clone());
        first_tab_group.begin();
        let first_editor = SqlEditorWidget::new(connection.clone(), timeout_input.clone());
        let mut first_editor_group = first_editor.get_group().clone();
        first_editor_group.resize(
            first_tab_group.x(),
            first_tab_group.y(),
            first_tab_group.w(),
            first_tab_group.h(),
        );
        first_editor_group.layout();
        first_tab_group.resizable(&first_editor_group);
        first_tab_group.end();
        query_tabs.select(first_tab_id);
        let sql_editor = first_editor.clone();
        let sql_buffer = first_editor.get_buffer();
        let editor_tabs = vec![QueryEditorTab {
            tab_id: first_tab_id,
            base_label: "Query 1".to_string(),
            sql_editor: first_editor,
            sql_buffer: sql_buffer.clone(),
            current_file: None,
            pristine_text: String::new(),
            current_text_len: 0,
            is_dirty: false,
            schema_generation: 0,
        }];

        right_flex.resizable(&right_tile);
        right_flex.end();

        content_flex.resizable(&right_flex);
        content_flex.end();
        main_flex.resizable(&content_flex);

        let mut status_bar = Frame::default().with_label("Not connected");
        status_bar.set_frame(FrameType::FlatBox);
        status_bar.set_color(theme::accent());
        status_bar.set_label_color(theme::text_primary());
        main_flex.fixed(&status_bar, STATUS_BAR_HEIGHT);
        main_flex.end();
        window.end();
        window.make_resizable(true);

        let state = Arc::new(Mutex::new(AppState {
            connection,
            query_tabs: query_tabs.clone(),
            query_top_group: query_top_group.clone(),
            query_split_bar: query_split_bar.clone(),
            editor_tabs,
            active_editor_tab_id: first_tab_id,
            next_editor_tab_number: 2,
            sql_editor,
            sql_buffer,
            schema_intellisense_data: IntellisenseData::new(),
            schema_highlight_data: HighlightData::new(),
            query_timeout_input: timeout_input.clone(),
            result_tabs,
            result_toolbar: result_toolbar.clone(),
            result_edit_check: edit_mode_check.clone(),
            result_insert_btn: edit_insert_btn.clone(),
            result_delete_btn: edit_delete_btn.clone(),
            result_save_btn: edit_save_btn.clone(),
            result_cancel_btn: edit_cancel_btn.clone(),
            execute_btn: execute_btn.clone(),
            query_cancel_btn: cancel_btn.clone(),
            explain_btn: explain_btn.clone(),
            commit_btn: commit_btn.clone(),
            rollback_btn: rollback_btn.clone(),
            result_tab_offset: 0,
            result_grid_execution_target: None,
            progress_contexts: HashMap::new(),
            object_browser,
            status_bar,
            current_file: Arc::new(Mutex::new(None)),
            popups: Arc::new(Mutex::new(Vec::new())),
            window,
            right_tile: right_tile.clone(),
            query_split_ratio,
            connection_info: Arc::new(Mutex::new(None)),
            has_live_connection: false,
            pending_connection_metadata_refresh: false,
            config: Arc::new(Mutex::new(config)),
            status_animation_running: false,
            status_animation_message: String::new(),
            status_animation_frame: 0,
            schema_sender: None,
            file_sender: None,
            schema_refresh_in_progress: Arc::new(Mutex::new(false)),
            schema_apply_generation: Arc::new(AtomicU64::new(0)),
        }));

        {
            let mut s = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let weak_state_for_result_tabs_change = Arc::downgrade(&state);
            s.result_tabs.set_on_change(move || {
                if let Some(state_for_result_tabs_change) =
                    weak_state_for_result_tabs_change.upgrade()
                {
                    if let Ok(mut s) = state_for_result_tabs_change.try_lock() {
                        s.refresh_result_edit_controls();
                    }
                }
            });
            s.refresh_result_edit_controls();
            // Set initial button / menu state: not connected at startup.
            s.refresh_connection_dependent_controls();
        }

        let weak_state_for_grid_edit = Arc::downgrade(&state);
        let grid_edit_callback: ResultGridSqlExecuteCallback =
            Arc::new(Mutex::new(Box::new(move |sql: String| {
                let Some(state_for_grid_edit) = weak_state_for_grid_edit.upgrade() else {
                    return Err("Main window is no longer available.".to_string());
                };
                let mut guard = state_for_grid_edit
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if guard.is_any_query_running() {
                    return Err("Another query is already running.".to_string());
                }
                let target_tab = guard
                    .result_tabs
                    .active_result_index()
                    .ok_or_else(|| "Open a result tab first.".to_string())?;
                guard.result_grid_execution_target = Some(target_tab);
                guard.sql_editor.execute_sql_text(&sql);
                if !guard.sql_editor.is_query_running() {
                    guard.result_grid_execution_target = None;
                    return Err("Failed to start query execution for result-grid edit.".to_string());
                }
                Ok(())
            })));
        {
            state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .result_tabs
                .set_execute_sql_callback(grid_edit_callback);
        }

        let weak_state_for_execute = Arc::downgrade(&state);
        execute_btn.set_callback(move |_| {
            if let Some(state_for_execute) = weak_state_for_execute.upgrade() {
                if let Some(editor) = acquire_sql_editor_if_idle(&state_for_execute) {
                    editor.execute_current();
                }
            }
        });

        let weak_state_for_cancel = Arc::downgrade(&state);
        cancel_btn.set_callback(move |_| {
            if let Some(state_for_cancel) = weak_state_for_cancel.upgrade() {
                MainWindow::cancel_all_running_queries(&state_for_cancel);
            }
        });

        let weak_state_for_explain = Arc::downgrade(&state);
        explain_btn.set_callback(move |_| {
            if let Some(state_for_explain) = weak_state_for_explain.upgrade() {
                if let Some(editor) = acquire_sql_editor_if_idle(&state_for_explain) {
                    editor.explain_current();
                }
            }
        });

        let weak_state_for_clear_btn = Arc::downgrade(&state);
        clear_btn.set_callback(move |_| {
            if let Some(state_for_clear_btn) = weak_state_for_clear_btn.upgrade() {
                state_for_clear_btn
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .sql_editor
                    .clear();
            }
        });

        let weak_state_for_commit = Arc::downgrade(&state);
        commit_btn.set_callback(move |_| {
            if let Some(state_for_commit) = weak_state_for_commit.upgrade() {
                state_for_commit
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .sql_editor
                    .commit();
            }
        });

        let weak_state_for_rollback = Arc::downgrade(&state);
        rollback_btn.set_callback(move |_| {
            if let Some(state_for_rollback) = weak_state_for_rollback.upgrade() {
                state_for_rollback
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .sql_editor
                    .rollback();
            }
        });

        let weak_state_for_result_close = Arc::downgrade(&state);
        close_tab_btn.set_callback(move |_| {
            let Some(state_for_result_close) = weak_state_for_result_close.upgrade() else {
                return;
            };
            if state_for_result_close
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_any_query_running()
            {
                fltk::dialog::alert_default("A query is running. Stop it before closing tabs.");
                return;
            }
            let mut s = state_for_result_close
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if s.result_tabs.close_current_tab() {
                // A result tab drop can release large row buffers.
                // Ask allocator to return free pages promptly.
                malloc_trim_process();
            }
            s.refresh_result_edit_controls();
            app::redraw();
        });

        let weak_state_for_result_clear = Arc::downgrade(&state);
        clear_tabs_btn.set_callback(move |_| {
            let Some(state_for_result_clear) = weak_state_for_result_clear.upgrade() else {
                return;
            };
            if state_for_result_clear
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_any_query_running()
            {
                fltk::dialog::alert_default("A query is running. Stop it before clearing tabs.");
                return;
            }
            let mut s = state_for_result_clear
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let had_tabs = s.result_tabs.tab_count() > 0;
            s.result_tabs.clear();
            if had_tabs {
                malloc_trim_process();
            }
            s.refresh_result_edit_controls();
            app::redraw();
        });

        let weak_state_for_query_close = Arc::downgrade(&state);
        query_close_tab_btn.set_callback(move |_| {
            let Some(state_for_query_close) = weak_state_for_query_close.upgrade() else {
                return;
            };
            let tab_id = state_for_query_close
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .active_editor_tab_id;
            MainWindow::close_query_editor_tab(&state_for_query_close, tab_id);
            app::redraw();
        });

        let weak_state_for_tab_select = Arc::downgrade(&state);
        query_tabs.set_on_select(move |tab_id| {
            if let Some(state_for_tab_select) = weak_state_for_tab_select.upgrade() {
                if let Ok(mut s) = state_for_tab_select.try_lock() {
                    if s.set_active_editor_tab(tab_id) {
                        s.sql_editor.focus();
                    }
                }
            }
        });

        {
            let mut state_borrow = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            Self::adjust_query_layout(&mut state_borrow);
            Self::apply_font_settings(&mut state_borrow);
        }

        let weak_state_for_history_btn = Arc::downgrade(&state);
        query_history_btn.set_callback(move |_| {
            if let Some(state_for_history) = weak_state_for_history_btn.upgrade() {
                MainWindow::open_query_history_dialog(&state_for_history);
            }
        });

        let weak_state_for_edit_check = Arc::downgrade(&state);
        edit_mode_check.set_callback(move |check| {
            let Some(state_for_edit_check) = weak_state_for_edit_check.upgrade() else {
                return;
            };
            let enabled = check.value();
            let mut result_tabs =
                match MainWindow::clone_result_tabs_for_edit_action(&state_for_edit_check) {
                    Ok(tabs) => tabs,
                    Err(err) => {
                        fltk::dialog::alert_default(&err);
                        app::redraw();
                        return;
                    }
                };
            let action_result = if enabled {
                result_tabs.begin_current_edit_mode()
            } else if result_tabs.is_current_edit_mode_enabled() {
                result_tabs.cancel_current_edit_mode()
            } else {
                Ok(String::new())
            };

            let mut error_message = None;
            if let Ok(mut s) = state_for_edit_check.try_lock() {
                match action_result {
                    Ok(msg) => {
                        if !msg.is_empty() {
                            s.set_status_message(&msg);
                        }
                    }
                    Err(err) => {
                        error_message = Some(err);
                    }
                }
                s.refresh_result_edit_controls();
            }
            if let Some(err) = error_message {
                fltk::dialog::alert_default(&err);
            }
            app::redraw();
        });

        let weak_state_for_edit_insert = Arc::downgrade(&state);
        edit_insert_btn.set_callback(move |_| {
            let Some(state_for_edit_insert) = weak_state_for_edit_insert.upgrade() else {
                return;
            };
            let mut result_tabs =
                match MainWindow::clone_result_tabs_for_edit_action(&state_for_edit_insert) {
                    Ok(tabs) => tabs,
                    Err(err) => {
                        fltk::dialog::alert_default(&err);
                        app::redraw();
                        return;
                    }
                };
            let action_result = result_tabs.insert_row_in_current_edit_mode();
            let mut error_message = None;
            {
                let mut s = state_for_edit_insert
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                match action_result {
                    Ok(msg) => s.set_status_message(&msg),
                    Err(err) => {
                        error_message = Some(err);
                    }
                }
                s.refresh_result_edit_controls();
            }
            if let Some(err) = error_message {
                fltk::dialog::alert_default(&err);
            }
            app::redraw();
        });

        let weak_state_for_edit_delete = Arc::downgrade(&state);
        edit_delete_btn.set_callback(move |_| {
            let Some(state_for_edit_delete) = weak_state_for_edit_delete.upgrade() else {
                return;
            };
            let mut result_tabs =
                match MainWindow::clone_result_tabs_for_edit_action(&state_for_edit_delete) {
                    Ok(tabs) => tabs,
                    Err(err) => {
                        fltk::dialog::alert_default(&err);
                        app::redraw();
                        return;
                    }
                };
            let action_result = result_tabs.delete_selected_rows_in_current_edit_mode();
            let mut error_message = None;
            {
                let mut s = state_for_edit_delete
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                match action_result {
                    Ok(msg) => s.set_status_message(&msg),
                    Err(err) => {
                        error_message = Some(err);
                    }
                }
                s.refresh_result_edit_controls();
            }
            if let Some(err) = error_message {
                fltk::dialog::alert_default(&err);
            }
            app::redraw();
        });

        let weak_state_for_edit_save = Arc::downgrade(&state);
        edit_save_btn.set_callback(move |_| {
            let Some(state_for_edit_save) = weak_state_for_edit_save.upgrade() else {
                return;
            };
            let mut result_tabs =
                match MainWindow::clone_result_tabs_for_edit_action(&state_for_edit_save) {
                    Ok(tabs) => tabs,
                    Err(err) => {
                        fltk::dialog::alert_default(&err);
                        app::redraw();
                        return;
                    }
                };
            let save_result = result_tabs.save_current_edit_mode();
            let mut error_message = None;
            {
                let mut s = state_for_edit_save
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                match save_result {
                    Ok(msg) => s.set_status_message(&msg),
                    Err(err) => {
                        error_message = Some(err);
                    }
                }
                s.refresh_result_edit_controls();
            }
            if let Some(err) = error_message {
                fltk::dialog::alert_default(&err);
            }
            app::redraw();
        });

        let weak_state_for_edit_cancel = Arc::downgrade(&state);
        edit_cancel_btn.set_callback(move |_| {
            let Some(state_for_edit_cancel) = weak_state_for_edit_cancel.upgrade() else {
                return;
            };
            let mut result_tabs =
                match MainWindow::clone_result_tabs_for_edit_action(&state_for_edit_cancel) {
                    Ok(tabs) => tabs,
                    Err(err) => {
                        fltk::dialog::alert_default(&err);
                        app::redraw();
                        return;
                    }
                };
            let action_result = result_tabs.cancel_current_edit_mode();
            let mut error_message = None;
            {
                let mut s = state_for_edit_cancel
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                match action_result {
                    Ok(msg) => s.set_status_message(&msg),
                    Err(err) => {
                        error_message = Some(err);
                    }
                }
                s.refresh_result_edit_controls();
            }
            if let Some(err) = error_message {
                fltk::dialog::alert_default(&err);
            }
            app::redraw();
        });

        // Restore current group
        if let Some(ref group) = current_group {
            fltk::group::Group::set_current(Some(group));
        }

        Self { state }
    }

    fn open_query_history_dialog(state: &Arc<Mutex<AppState>>) {
        let popups = {
            let s = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            s.popups.clone()
        };
        if let Some(sql) = QueryHistoryDialog::show_with_registry(popups) {
            let (created_tab_id, schema_sender, file_sender, created_editor, created_right_tile) = {
                let mut s = state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let mut created_tab_id = None;
                let mut created_editor: Option<SqlEditorWidget> = None;
                let mut created_right_tile: Option<Tile> = None;
                if let Some(tab_id) = MainWindow::create_query_editor_tab(&mut s) {
                    s.sql_buffer.set_text(&sql);
                    s.sql_editor.reset_undo_redo_history();
                    s.set_tab_file_path(tab_id, None);
                    s.set_tab_pristine_text(tab_id, sql);
                    created_editor = Some(s.sql_editor.clone());
                    created_right_tile = Some(s.right_tile.clone());
                    created_tab_id = Some(tab_id);
                }
                (
                    created_tab_id,
                    s.schema_sender.clone(),
                    s.file_sender.clone(),
                    created_editor,
                    created_right_tile,
                )
            };

            if let Some(tab_id) = created_tab_id {
                if let Some(schema_sender) = schema_sender {
                    MainWindow::attach_editor_callbacks(state, tab_id, schema_sender);
                }
                if let Some(file_sender) = file_sender {
                    MainWindow::attach_file_drop_callback(state, tab_id, file_sender);
                }
                if let Some(mut editor) = created_editor {
                    editor.focus();
                }
                if let Some(mut right_tile) = created_right_tile {
                    right_tile.redraw();
                }
                app::redraw();
            } else {
                state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .set_status_message("Failed to create a new query tab");
            }
        }
    }

    fn adjust_query_layout(state: &mut AppState) {
        let mut right_tile = state.right_tile.clone();
        let mut query_top_group = state.query_top_group.clone();
        let mut query_split_bar = state.query_split_bar.clone();
        let ratio = *state
            .query_split_ratio
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(r) = ratio {
            Self::apply_query_split_ratio(
                &mut right_tile,
                &mut query_top_group,
                &mut query_split_bar,
                r,
            );
        } else {
            Self::adjust_query_layout_with(
                &mut right_tile,
                &mut query_top_group,
                &mut query_split_bar,
            );
        }
    }

    fn apply_font_settings(state: &mut AppState) {
        let (unified_profile, ui_size, editor_size, result_size, result_cell_max_chars) = {
            let config = state
                .config
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            (
                font_settings::profile_by_name(&config.editor_font),
                config.ui_font_size.clamp(8, 24) as i32,
                config.editor_font_size,
                config.result_font_size,
                config.result_cell_max_chars.clamp(
                    RESULT_CELL_MAX_DISPLAY_CHARS_MIN,
                    RESULT_CELL_MAX_DISPLAY_CHARS_MAX,
                ),
            )
        };
        app::set_font(unified_profile.normal);
        app::set_font_size(ui_size);
        fltk::misc::Tooltip::set_font(unified_profile.normal);
        fltk::misc::Tooltip::set_font_size(ui_size);
        fltk::dialog::message_set_font(unified_profile.normal, ui_size);
        for tab in &mut state.editor_tabs {
            tab.sql_editor
                .apply_font_settings(unified_profile, editor_size, ui_size);
        }
        state
            .result_tabs
            .apply_font_settings(unified_profile, result_size);
        state
            .result_tabs
            .set_max_cell_display_chars(result_cell_max_chars as usize);
        state
            .object_browser
            .apply_font_settings(unified_profile, ui_size);
        Self::apply_runtime_ui_font(state, unified_profile.normal, ui_size);
        state.right_tile.redraw();
        state.window.redraw();
        app::redraw();
        // Force FLTK to process the pending redraw immediately, so font
        // changes are visible right after the settings dialog closes
        // instead of requiring multiple save cycles.
        app::flush();
        app::awake();
    }

    fn apply_runtime_ui_font(state: &mut AppState, font: fltk::enums::Font, ui_size: i32) {
        fn apply_widget_font_recursive(widget: &mut Widget, font: fltk::enums::Font, size: i32) {
            widget.set_label_font(font);
            widget.set_label_size(size);
            if let Some(group) = widget.as_group() {
                for mut child in group.into_iter() {
                    apply_widget_font_recursive(&mut child, font, size);
                }
            }
        }

        let mut window = state.window.clone();
        window.set_label_font(font);
        window.set_label_size(ui_size);
        for mut child in window.clone().into_iter() {
            apply_widget_font_recursive(&mut child, font, ui_size);
        }

        if let Some(mut menu) = app::widget_from_id::<MenuBar>("main_menu") {
            menu.set_text_font(font);
            menu.set_text_size(ui_size);
        }

        for popup in state
            .popups
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter_mut()
        {
            popup.set_label_font(font);
            popup.set_label_size(ui_size);
            for mut child in popup.clone().into_iter() {
                apply_widget_font_recursive(&mut child, font, ui_size);
            }
        }
    }

    fn clamp_query_split_with(
        right_tile: &mut Tile,
        query_top_group: &mut Group,
        query_split_bar: &mut Frame,
    ) {
        let right_height = right_tile.h();
        if right_height <= 0 {
            return;
        }

        let max_query_height =
            (right_height - MIN_RESULTS_HEIGHT - QUERY_SPLIT_BAR_HEIGHT).max(MIN_QUERY_HEIGHT);
        let desired_query_height = query_top_group
            .h()
            .clamp(MIN_QUERY_HEIGHT, max_query_height);
        Self::apply_query_split_layout(
            right_tile,
            query_top_group,
            query_split_bar,
            desired_query_height,
        );
    }

    /// Apply the saved split ratio to compute the query pane height.
    fn apply_query_split_ratio(
        right_tile: &mut Tile,
        query_top_group: &mut Group,
        query_split_bar: &mut Frame,
        ratio: f64,
    ) {
        let right_height = right_tile.h();
        if right_height <= 0 {
            return;
        }
        let max_height =
            (right_height - MIN_RESULTS_HEIGHT - QUERY_SPLIT_BAR_HEIGHT).max(MIN_QUERY_HEIGHT);
        let desired_height = ((right_height as f64) * ratio).round() as i32;
        let desired_height = desired_height.clamp(MIN_QUERY_HEIGHT, max_height);
        Self::apply_query_split_layout(right_tile, query_top_group, query_split_bar, desired_height);
    }

    fn adjust_query_layout_with(
        right_tile: &mut Tile,
        query_top_group: &mut Group,
        query_split_bar: &mut Frame,
    ) {
        let right_height = right_tile.h();
        if right_height <= 0 {
            return;
        }
        let max_height =
            (right_height - MIN_RESULTS_HEIGHT - QUERY_SPLIT_BAR_HEIGHT).max(MIN_QUERY_HEIGHT);
        let mut desired_height = ((right_height as f32) * 0.4).round() as i32;
        if desired_height < MIN_QUERY_HEIGHT {
            desired_height = MIN_QUERY_HEIGHT;
        } else if desired_height > max_height {
            desired_height = max_height;
        }
        Self::apply_query_split_layout(
            right_tile,
            query_top_group,
            query_split_bar,
            desired_height,
        );
    }

    fn apply_query_split_layout(
        right_tile: &mut Tile,
        query_top_group: &mut Group,
        query_split_bar: &mut Frame,
        desired_query_height: i32,
    ) {
        let right_height = right_tile.h().max(1);
        let right_width = right_tile.w();
        let tile_x = right_tile.x();
        let tile_y = right_tile.y();

        let max_query_height =
            (right_height - MIN_RESULTS_HEIGHT - QUERY_SPLIT_BAR_HEIGHT).max(MIN_QUERY_HEIGHT);
        let mut query_height = desired_query_height.clamp(MIN_QUERY_HEIGHT, max_query_height);
        if query_height >= right_height {
            query_height = right_height.saturating_sub(1).max(1);
        }
        let split_bar_height = QUERY_SPLIT_BAR_HEIGHT.min(right_height.max(0));
        let result_y = tile_y + query_height + split_bar_height;
        let result_height = (right_height - query_height - split_bar_height).max(1);
        let top_ptr = query_top_group.as_widget_ptr();

        query_top_group.resize(tile_x, tile_y, right_width, query_height);
        for child in right_tile.clone().into_iter() {
            let Some(mut child_group) = child.as_group() else {
                continue;
            };
            if child_group.as_widget_ptr() == top_ptr {
                continue;
            }
            child_group.resize(tile_x, result_y, right_width, result_height);
        }
        query_split_bar.resize(tile_x, tile_y + query_height, right_width, split_bar_height);
        right_tile.redraw();
    }

    fn adjust_query_layout_on_resize(state: &AppState) {
        let mut right_tile = state.right_tile.clone();
        let mut query_top_group = state.query_top_group.clone();
        let mut query_split_bar = state.query_split_bar.clone();
        let ratio = *state
            .query_split_ratio
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(r) = ratio {
            Self::apply_query_split_ratio(
                &mut right_tile,
                &mut query_top_group,
                &mut query_split_bar,
                r,
            );
        } else {
            Self::adjust_query_layout_with(
                &mut right_tile,
                &mut query_top_group,
                &mut query_split_bar,
            );
        }
    }

    fn create_query_editor_tab(state: &mut AppState) -> Option<QueryTabId> {
        let label = format!("Query {}", state.next_editor_tab_number);
        state.next_editor_tab_number = state.next_editor_tab_number.saturating_add(1);
        let tab_id = state.query_tabs.add_tab(&label);
        let group = state.query_tabs.tab_group(tab_id)?;
        group.begin();
        let editor =
            SqlEditorWidget::new(state.connection.clone(), state.query_timeout_input.clone());
        let mut editor_group = editor.get_group().clone();
        editor_group.resize(group.x(), group.y(), group.w(), group.h());
        editor_group.layout();
        group.resizable(&editor_group);
        group.end();
        let inherited_intellisense = state.schema_intellisense_data.clone();
        *editor
            .get_intellisense_data()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = inherited_intellisense;
        let inherited_highlight = state.schema_highlight_data.clone();
        editor
            .get_highlighter()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .set_highlight_data(inherited_highlight);
        let buffer = editor.get_buffer();
        state.editor_tabs.push(QueryEditorTab {
            tab_id,
            base_label: label,
            sql_editor: editor,
            sql_buffer: buffer,
            current_file: None,
            pristine_text: String::new(),
            current_text_len: 0,
            is_dirty: false,
            schema_generation: state.current_schema_generation(),
        });
        state.query_tabs.select(tab_id);
        let _ = state.set_active_editor_tab(tab_id);
        Some(tab_id)
    }

    fn close_query_editor_tab(state: &Arc<Mutex<AppState>>, tab_id: QueryTabId) -> bool {
        {
            let s = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(index) = s.find_tab_index(tab_id) else {
                return false;
            };
            if s.editor_tabs[index].sql_editor.is_query_running() {
                fltk::dialog::alert_default(
                    "A query is running in this tab. Stop it before closing.",
                );
                return false;
            }
        }

        if !Self::confirm_save_if_dirty(state, tab_id, "closing this tab") {
            return false;
        }

        let (created_tab_id, schema_sender, file_sender) = {
            let mut s = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(index) = s.find_tab_index(tab_id) else {
                return false;
            };

            let was_active = s.active_editor_tab_id == tab_id;
            s.editor_tabs[index].sql_editor.cleanup_for_close();
            if !s.query_tabs.close_tab(tab_id) {
                return false;
            }
            s.editor_tabs.remove(index);
            s.progress_contexts.remove(&tab_id);

            let mut created_tab_id = None;
            if s.editor_tabs.is_empty() {
                let Some(new_tab_id) = MainWindow::create_query_editor_tab(&mut s) else {
                    return false;
                };
                created_tab_id = Some(new_tab_id);
            }

            let next_tab_id = s
                .query_tabs
                .selected_id()
                .or_else(|| s.query_tabs.tab_ids().first().copied())
                .or_else(|| s.editor_tabs.first().map(|tab| tab.tab_id));
            let switched_to_next = next_tab_id
                .map(|next_tab_id| s.set_active_editor_tab(next_tab_id))
                .unwrap_or(false);

            if switched_to_next {
                if was_active {
                    s.sql_editor.focus();
                }
            } else if let Some(fallback_tab) = s.editor_tabs.first().cloned() {
                // Defensive fallback: if tab/widget selection loses sync, still point
                // app state to a live editor tab so closed-tab resources are not held
                // by stale SqlEditorWidget/TextBuffer handles.
                s.active_editor_tab_id = fallback_tab.tab_id;
                s.sql_editor = fallback_tab.sql_editor;
                s.sql_buffer = fallback_tab.sql_buffer;
                *s.current_file
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = fallback_tab.current_file;
                s.query_tabs.select(fallback_tab.tab_id);
                s.refresh_window_title();
                if was_active {
                    s.sql_editor.focus();
                }
            } else if was_active {
                // Defensive fallback: if tab selection cannot be resolved,
                // clear active editor references so closed-tab resources are
                // not kept alive by stale handles in application state.
                let detached_editor =
                    SqlEditorWidget::new(s.connection.clone(), s.query_timeout_input.clone());
                s.active_editor_tab_id = 0;
                s.sql_buffer = detached_editor.get_buffer();
                s.sql_editor = detached_editor;
                *s.current_file
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
                s.refresh_window_title();
            }

            s.right_tile.redraw();
            app::redraw();

            // Large SQL buffers are dropped above. Ask allocator to release
            // free pages proactively so RSS reflects the close action sooner.
            malloc_trim_process();
            (
                created_tab_id,
                s.schema_sender.clone(),
                s.file_sender.clone(),
            )
        };

        if let Some(tab_id) = created_tab_id {
            if let Some(schema_sender) = schema_sender {
                Self::attach_editor_callbacks(state, tab_id, schema_sender);
            }
            if let Some(file_sender) = file_sender {
                Self::attach_file_drop_callback(state, tab_id, file_sender);
            }
            let mut s = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            s.sql_editor.focus();
        }

        true
    }

    fn update_schema_snapshot(
        state: &mut AppState,
        data: IntellisenseData,
        highlight_data: HighlightData,
    ) {
        let mut combined_highlight = highlight_data.clone();
        let columns_from_intellisense = Self::collect_highlight_columns(&data);
        if !columns_from_intellisense.is_empty() {
            let mut seen: HashSet<String> = combined_highlight
                .columns
                .iter()
                .map(|name| name.to_uppercase())
                .collect();
            for name in columns_from_intellisense {
                let upper = name.to_uppercase();
                if seen.insert(upper) {
                    combined_highlight.columns.push(name);
                }
            }
        }

        state.schema_intellisense_data = data;
        state.schema_highlight_data = combined_highlight;
        let _ = state
            .schema_apply_generation
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
    }

    fn collect_highlight_columns(data: &IntellisenseData) -> Vec<String> {
        data.get_all_columns_for_highlighting()
    }

    fn load_schema_update_for_current_connection(
        connection: &SharedConnection,
    ) -> Option<SchemaUpdate> {
        let mut conn_guard = lock_connection_with_activity(connection, "Loading schema metadata");
        let connection_generation = conn_guard.connection_generation();
        let Ok(conn) = conn_guard.require_live_connection() else {
            return None;
        };

        let tables = match ObjectBrowser::get_tables(conn.as_ref()) {
            Ok(tables) => tables,
            Err(err) => {
                crate::utils::logging::log_error(
                    "schema",
                    &format!("failed to load tables for intellisense schema update: {err}"),
                );
                return None;
            }
        };

        let views = match ObjectBrowser::get_views(conn.as_ref()) {
            Ok(views) => views,
            Err(err) => {
                crate::utils::logging::log_error(
                    "schema",
                    &format!("failed to load views for intellisense schema update: {err}"),
                );
                Vec::new()
            }
        };

        let mut data = IntellisenseData::new();
        let mut highlight_data = HighlightData::new();
        highlight_data.tables = tables.clone();
        data.tables = tables;
        highlight_data.views = views.clone();
        data.views = views;
        data.rebuild_indices();
        highlight_data.columns = MainWindow::collect_highlight_columns(&data);

        Some(SchemaUpdate {
            data,
            highlight_data,
            connection_generation,
        })
    }

    fn start_connection_metadata_refresh(
        state: &mut AppState,
        schema_sender: &std::sync::mpsc::Sender<SchemaUpdate>,
    ) {
        if !try_set_mutex_flag(&state.schema_refresh_in_progress) {
            return;
        }

        state.object_browser.refresh();
        let schema_sender = schema_sender.clone();
        let connection = state.connection.clone();
        let schema_refresh_guard = state.schema_refresh_in_progress.clone();
        thread::spawn(move || {
            if let Some(update) = MainWindow::load_schema_update_for_current_connection(&connection)
            {
                let _ = schema_sender.send(update);
                app::awake();
            }
            clear_mutex_flag(&schema_refresh_guard);
        });
    }

    fn attach_editor_callbacks(
        state: &Arc<Mutex<AppState>>,
        tab_id: QueryTabId,
        schema_sender: std::sync::mpsc::Sender<SchemaUpdate>,
    ) {
        let Some(mut editor) = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .editor_tabs
            .iter()
            .find(|tab| tab.tab_id == tab_id)
            .map(|tab| tab.sql_editor.clone())
        else {
            return;
        };

        let weak_state_for_execute = Arc::downgrade(state);
        editor.set_execute_callback(move |query_result| {
            let Some(state_for_execute) = weak_state_for_execute.upgrade() else {
                return;
            };
            let mut s = state_for_execute
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let base_msg = if query_result.success {
                format!(
                    "{} | Time: {:.3}s",
                    query_result.message,
                    query_result.execution_time.as_secs_f64()
                )
            } else {
                format!(
                    "Error | Time: {:.3}s",
                    query_result.execution_time.as_secs_f64()
                )
            };
            s.set_status_message(&base_msg);
        });

        let weak_state_for_status = Arc::downgrade(state);
        editor.set_status_callback(move |message| {
            let Some(state_for_status) = weak_state_for_status.upgrade() else {
                return;
            };
            if let Ok(mut s) = state_for_status.try_lock() {
                s.set_status_message(message);
            };
        });

        let weak_state_for_find = Arc::downgrade(state);
        editor.set_find_callback(move || {
            let Some(state_for_find) = weak_state_for_find.upgrade() else {
                return;
            };
            let (mut editor, mut buffer, popups) = {
                let s = state_for_find
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                (
                    s.sql_editor.get_editor(),
                    s.sql_buffer.clone(),
                    s.popups.clone(),
                )
            };
            FindReplaceDialog::show_find_with_registry(&mut editor, &mut buffer, popups);
        });

        let weak_state_for_replace = Arc::downgrade(state);
        editor.set_replace_callback(move || {
            let Some(state_for_replace) = weak_state_for_replace.upgrade() else {
                return;
            };
            let (mut editor, mut buffer, popups) = {
                let s = state_for_replace
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                (
                    s.sql_editor.get_editor(),
                    s.sql_buffer.clone(),
                    s.popups.clone(),
                )
            };
            FindReplaceDialog::show_replace_with_registry(&mut editor, &mut buffer, popups);
        });

        let weak_state_for_progress = Arc::downgrade(state);
        let schema_sender_for_progress = schema_sender;
        editor.set_progress_callback(move |progress| {
            let Some(state_for_progress) = weak_state_for_progress.upgrade() else {
                return;
            };
            let mut s = state_for_progress
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match progress {
                QueryProgress::BatchStart => {
                    let has_live_connection = s.has_live_connection;
                    let has_running_queries = s.sql_editor.is_query_running()
                        || s.editor_tabs
                            .iter()
                            .any(|tab| tab.sql_editor.is_query_running());
                    if should_ignore_query_progress_when_disconnected(
                        has_live_connection,
                        has_running_queries,
                    ) {
                        return;
                    }
                    let tab_count = s.result_tabs.tab_count();
                    let context = QueryProgressContext::new(
                        resolve_result_tab_offset(tab_count, s.result_grid_execution_target),
                        s.result_grid_execution_target,
                    );
                    s.progress_contexts.insert(tab_id, context);
                }
                QueryProgress::StatementStart { index } => {
                    let has_live_connection = s.has_live_connection;
                    let has_running_queries = s.sql_editor.is_query_running()
                        || s.editor_tabs
                            .iter()
                            .any(|tab| tab.sql_editor.is_query_running());
                    if should_ignore_query_progress_when_disconnected(
                        has_live_connection,
                        has_running_queries,
                    ) {
                        return;
                    }
                    let tab_count = s.result_tabs.tab_count();
                    let mut result_tabs = s.result_tabs.clone();
                    let tab_index = {
                        let Some(context) = s.progress_contexts.get_mut(&tab_id) else {
                            return;
                        };
                        context.fetch_row_counts.remove(&index);
                        resolve_progress_tab_index(
                            tab_count,
                            context.result_tab_offset,
                            context.execution_target,
                            index,
                        )
                    };
                    let was_running = s.status_animation_running;
                    s.start_status_animation("Executing query...");
                    if !was_running {
                        MainWindow::start_status_animation_timer(&state_for_progress);
                    }
                    s.refresh_result_edit_controls();
                    drop(s);
                    result_tabs.start_statement(tab_index, &format!("Result {}", tab_index + 1));
                }
                QueryProgress::SelectStart {
                    index,
                    columns,
                    null_text,
                } => {
                    let has_live_connection = s.has_live_connection;
                    let has_running_queries = s.sql_editor.is_query_running()
                        || s.editor_tabs
                            .iter()
                            .any(|tab| tab.sql_editor.is_query_running());
                    if should_ignore_query_progress_when_disconnected(
                        has_live_connection,
                        has_running_queries,
                    ) {
                        return;
                    }
                    let tab_count = s.result_tabs.tab_count();
                    let mut result_tabs = s.result_tabs.clone();
                    let tab_index = {
                        let Some(context) = s.progress_contexts.get_mut(&tab_id) else {
                            return;
                        };
                        context.fetch_row_counts.insert(index, 0);
                        context.last_fetch_status_update = Instant::now();
                        resolve_progress_tab_index(
                            tab_count,
                            context.result_tab_offset,
                            context.execution_target,
                            index,
                        )
                    };
                    let was_running = s.status_animation_running;
                    s.start_status_animation("Fetching rows: 0");
                    if !was_running {
                        MainWindow::start_status_animation_timer(&state_for_progress);
                    }
                    s.refresh_result_edit_controls();
                    drop(s);
                    result_tabs.start_streaming(tab_index, &columns, &null_text);
                }
                QueryProgress::Rows { index, rows } => {
                    let Some(tab_index) = resolve_active_progress_tab_index(&s, tab_id, index)
                    else {
                        return;
                    };
                    let rows_len = rows.len();
                    let mut result_tabs = s.result_tabs.clone();
                    let Some(context) = s.progress_contexts.get_mut(&tab_id) else {
                        return;
                    };
                    let new_count = {
                        let count = context.fetch_row_counts.entry(index).or_insert(0);
                        *count += rows_len;
                        *count
                    };
                    // Throttle status bar updates to avoid formatting a new string
                    // and touching the label widget on every row batch.
                    let needs_status_update =
                        context.last_fetch_status_update.elapsed() >= FETCH_STATUS_UPDATE_INTERVAL;
                    if needs_status_update {
                        context.last_fetch_status_update = Instant::now();
                        s.update_status_animation(&format!("Fetching rows: {}", new_count));
                    }
                    drop(s);
                    result_tabs.append_rows(tab_index, rows);
                }
                QueryProgress::ScriptOutput { lines } => {
                    let mut result_tabs = s.result_tabs.clone();
                    drop(s);
                    result_tabs.append_script_output_lines(&lines);
                }
                QueryProgress::PromptInput { .. } => {}
                QueryProgress::AutoCommitChanged { enabled } => {
                    if let Some(menu) = app::widget_from_id::<MenuBar>("main_menu") {
                        if let Some(mut item) = menu.find_item("&Tools/&Auto-Commit") {
                            if enabled {
                                item.set();
                            } else {
                                item.clear();
                            }
                        }
                    }
                    let status = if enabled {
                        "Auto-commit enabled"
                    } else {
                        "Auto-commit disabled"
                    };
                    s.set_status_message(status);
                }
                QueryProgress::ConnectionChanged { info } => {
                    if let Some(info) = info {
                        let has_running_queries = s.sql_editor.is_query_running()
                            || s.editor_tabs
                                .iter()
                                .any(|tab| tab.sql_editor.is_query_running());
                        *s.connection_info
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(info.clone());
                        s.has_live_connection = true;
                        s.set_status_message(&format!("Connected | {}", info.name));
                        s.sql_editor.focus();
                        s.refresh_connection_dependent_controls();
                        if has_running_queries {
                            // CONNECT can appear mid-script. Deferring metadata fetch prevents
                            // object-browser/schema workers from competing with the active batch.
                            s.pending_connection_metadata_refresh = true;
                        } else {
                            MainWindow::start_connection_metadata_refresh(
                                &mut s,
                                &schema_sender_for_progress,
                            );
                            s.pending_connection_metadata_refresh = false;
                        }
                    } else {
                        Self::transition_to_disconnected_state(&mut s, None);
                    }
                }
                QueryProgress::StatementFinished { index, result, .. } => {
                    let has_live_connection = s.has_live_connection;
                    let has_running_queries = s.sql_editor.is_query_running()
                        || s.editor_tabs
                            .iter()
                            .any(|tab| tab.sql_editor.is_query_running());
                    if should_ignore_query_progress_when_disconnected(
                        has_live_connection,
                        has_running_queries,
                    ) {
                        return;
                    }
                    let tab_count = s.result_tabs.tab_count();
                    let mut result_tabs = s.result_tabs.clone();
                    let tab_index = {
                        let Some(context) = s.progress_contexts.get_mut(&tab_id) else {
                            return;
                        };
                        resolve_progress_tab_index(
                            tab_count,
                            context.result_tab_offset,
                            context.execution_target,
                            index,
                        )
                    };
                    let mut show_script_output = false;
                    let mut script_lines: Vec<String> = Vec::new();
                    if !result.success && !result.message.trim().is_empty() {
                        script_lines = result.message.lines().map(|l| l.to_string()).collect();
                        show_script_output = true;
                    }
                    if let Some(context) = s.progress_contexts.get_mut(&tab_id) {
                        context.fetch_row_counts.remove(&index);
                    }

                    s.refresh_result_edit_controls();
                    drop(s);

                    if show_script_output {
                        result_tabs.append_script_output_lines(&script_lines);
                        result_tabs.select_script_output();
                    }
                    if result.is_select {
                        result_tabs.finish_streaming(tab_index);
                        result_tabs.display_result(tab_index, &result);
                    } else {
                        result_tabs
                            .start_statement(tab_index, &format!("Result {}", tab_index + 1));
                        result_tabs.display_result(tab_index, &result);
                    }
                }
                QueryProgress::WorkerPanicked { message } => {
                    s.set_status_message(&message);
                    s.refresh_result_edit_controls();
                }
                QueryProgress::BatchFinished => {
                    s.progress_contexts.remove(&tab_id);
                    let has_running_queries = s.sql_editor.is_query_running()
                        || s.editor_tabs
                            .iter()
                            .any(|tab| tab.sql_editor.is_query_running());

                    if should_run_global_batch_cleanup(has_running_queries) {
                        let mut result_tabs = s.result_tabs.clone();
                        drop(s);

                        result_tabs.finish_all_streaming();
                        result_tabs.align_tab_strip_left();
                        let recovered_save_states = result_tabs.clear_orphaned_save_requests();
                        let recovered_edit_states = result_tabs.clear_orphaned_query_edit_backups();

                        let mut s = state_for_progress
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        s.result_grid_execution_target = None;
                        s.result_tab_offset = s.result_tabs.tab_count();
                        if s.pending_connection_metadata_refresh && s.has_live_connection {
                            MainWindow::start_connection_metadata_refresh(
                                &mut s,
                                &schema_sender_for_progress,
                            );
                            s.pending_connection_metadata_refresh = false;
                        }
                        // Query execution completed and large temporary buffers may
                        // have been released during result materialization.
                        malloc_trim_process();
                        let current_status = s.status_bar.label().to_ascii_lowercase();
                        let needs_reset = current_status.contains("executing query")
                            || current_status.contains("fetching rows")
                            || current_status.contains("connection is busy")
                            || current_status.contains("query is already running");
                        if recovered_save_states > 0 {
                            s.set_status_message(
                                "Save was interrupted. Staged edits are still available.",
                            );
                        } else if recovered_edit_states > 0 {
                            s.set_status_message(
                                "Query ended before completion. Restored staged result-grid edits.",
                            );
                        } else if needs_reset {
                            s.set_status_message("Ready");
                        }
                        s.refresh_result_edit_controls();
                    } else {
                        s.refresh_result_edit_controls();
                    }
                }
            }
        });

        let weak_state_for_dirty = Arc::downgrade(state);
        let mut buffer_for_dirty = editor.get_buffer();
        buffer_for_dirty.add_modify_callback2(move |buf, _pos, ins, del, _restyled, _deleted| {
            let Some(state_for_dirty) = weak_state_for_dirty.upgrade() else {
                return;
            };
            if let Ok(mut s) = state_for_dirty.try_lock() {
                s.on_tab_buffer_modified(tab_id, ins, del, buf)
            };
        });
    }

    fn attach_file_drop_callback(
        state: &Arc<Mutex<AppState>>,
        tab_id: QueryTabId,
        file_sender: std::sync::mpsc::Sender<FileActionResult>,
    ) {
        let Some(mut editor) = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .editor_tabs
            .iter()
            .find(|tab| tab.tab_id == tab_id)
            .map(|tab| tab.sql_editor.clone())
        else {
            return;
        };
        let weak_state_for_file_drop = Arc::downgrade(state);
        let file_sender_for_drop = file_sender;
        editor.set_file_drop_callback(move |path| {
            if let Some(state_for_drop) = weak_state_for_file_drop.upgrade() {
                let mut s = state_for_drop
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if MainWindow::focus_existing_tab_with_same_file_name(&mut s, &path) {
                    return;
                }
                let conn_info = s
                    .connection_info
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                let file_label = path.file_name().unwrap_or_default().to_string_lossy();
                s.status_bar.set_label(&format_status(
                    &format!("Opening {} in new tab", file_label),
                    &conn_info,
                ));
            }

            let sender = file_sender_for_drop.clone();
            thread::spawn(move || {
                let result = fs::read_to_string(&path).map_err(|err| err.to_string());
                let _ = sender.send(FileActionResult::OpenInNewTab { path, result });
                app::awake();
            });
        });
    }

    fn execute_menu_action(
        state: &Arc<Mutex<AppState>>,
        schema_sender: &std::sync::mpsc::Sender<SchemaUpdate>,
        conn_sender: &std::sync::mpsc::Sender<ConnectionResult>,
        file_sender: &std::sync::mpsc::Sender<FileActionResult>,
        choice: &str,
    ) -> bool {
        match choice {
            "File/Connect..." => {
                let (popups, connection) = {
                    let s = state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    (s.popups.clone(), s.connection.clone())
                };
                if let Some(info) = ConnectionDialog::show_with_registry(popups) {
                    let conn_sender = conn_sender.clone();
                    {
                        let mut s = state
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        s.status_bar
                            .set_label(&format!("Connecting to {}...", info.name));
                    }
                    thread::spawn(move || {
                        let Some(mut db_conn) = try_lock_connection_with_activity(
                            &connection,
                            format!("Connecting to {}", info.name),
                        ) else {
                            let _ = conn_sender
                                .send(ConnectionResult::Failure(format_connection_busy_message()));
                            app::awake();
                            return;
                        };
                        match db_conn.connect(info.clone()) {
                            Ok(_) => {
                                db_conn.refresh_tracked_connection();
                                let session = db_conn.session_state();
                                drop(db_conn);
                                match session.lock() {
                                    Ok(mut guard) => guard.reset(),
                                    Err(poisoned) => {
                                        eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                        poisoned.into_inner().reset();
                                    }
                                }
                                let mut info = info;
                                info.clear_password();
                                let _ = conn_sender.send(ConnectionResult::Success(info));
                                app::awake();
                            }
                            Err(e) => {
                                let _ = conn_sender.send(ConnectionResult::Failure(e.to_string()));
                                app::awake();
                            }
                        }
                    });
                }
                true
            }
            "File/Disconnect" => {
                let connection = state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .connection
                    .clone();
                let Some(mut db_conn) =
                    try_lock_connection_with_activity(&connection, "Disconnecting session")
                else {
                    let busy_message = format_connection_busy_message();
                    fltk::dialog::alert_default(&busy_message);
                    let mut s = state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let conn_info = s
                        .connection_info
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .clone();
                    s.status_bar
                        .set_label(&format_status(&busy_message, &conn_info));
                    return true;
                };
                crate::utils::logging::log_info("connection", "Disconnected from database");
                db_conn.disconnect();
                db_conn.refresh_tracked_connection();
                crate::db::clear_tracked_db_activity();
                // Release the connection lock before locking AppState.
                // Session reset is handled inside transition_to_disconnected_state.
                drop(db_conn);

                let mut s = state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                MainWindow::transition_to_disconnected_state(&mut s, None);
                true
            }
            "File/Open SQL File..." => {
                let mut dialog = FileDialog::new(FileDialogType::BrowseFile);
                dialog.set_filter("SQL Files\t*.sql\nAll Files\t*.*");
                dialog.show();
                let filename = dialog.filename();
                if !filename.as_os_str().is_empty() {
                    {
                        let mut s = state
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        if MainWindow::focus_existing_tab_with_same_file_name(&mut s, &filename) {
                            return true;
                        }
                    }
                    let sender = file_sender.clone();
                    thread::spawn(move || {
                        let result = fs::read_to_string(&filename).map_err(|err| err.to_string());
                        let _ = sender.send(FileActionResult::OpenInNewTab {
                            path: filename,
                            result,
                        });
                        app::awake();
                    });
                }
                true
            }
            "File/Save SQL File..." => {
                let tab_id = state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .active_editor_tab_id;
                if let SaveTabOutcome::Failed(err) = MainWindow::save_tab(state, tab_id, false) {
                    fltk::dialog::alert_default(&format!("Failed to save SQL file: {}", err));
                }
                true
            }
            "File/Save SQL File As..." => {
                let tab_id = state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .active_editor_tab_id;
                if let SaveTabOutcome::Failed(err) = MainWindow::save_tab(state, tab_id, true) {
                    fltk::dialog::alert_default(&format!("Failed to save SQL file: {}", err));
                }
                true
            }
            "File/Exit" => {
                let mut window = state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .window
                    .clone();
                window.do_callback();
                true
            }
            "Edit/Undo" => {
                state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .sql_editor
                    .undo();
                true
            }
            "Edit/Redo" => {
                state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .sql_editor
                    .redo();
                true
            }
            "Edit/Cut" => {
                state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .sql_editor
                    .get_editor()
                    .cut();
                true
            }
            "Edit/Copy" => {
                let mut s = state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let result_tabs_widget = s.result_tabs.get_widget();
                let focus_in_results = if let Some(focus) = app::focus() {
                    focus.as_widget_ptr() == result_tabs_widget.as_widget_ptr()
                        || focus.inside(&result_tabs_widget)
                } else {
                    false
                };

                if focus_in_results {
                    let cell_count = s.result_tabs.copy();
                    let conn_info = s
                        .connection_info
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .clone();
                    if cell_count > 0 {
                        s.status_bar.set_label(&format_status(
                            &format!("Copied {} cells to clipboard", cell_count),
                            &conn_info,
                        ));
                    } else {
                        s.status_bar
                            .set_label(&format_status("No cells selected to copy", &conn_info));
                    }
                } else {
                    s.sql_editor.get_editor().copy();
                }
                true
            }
            "Edit/Copy with Headers" => {
                let mut s = state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let result_tabs_widget = s.result_tabs.get_widget();
                let focus_in_results = if let Some(focus) = app::focus() {
                    focus.as_widget_ptr() == result_tabs_widget.as_widget_ptr()
                        || focus.inside(&result_tabs_widget)
                } else {
                    false
                };

                if focus_in_results {
                    s.result_tabs.copy_with_headers();
                    let conn_info = s
                        .connection_info
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .clone();
                    s.status_bar
                        .set_label(&format_status("Copied selection with headers", &conn_info));
                } else {
                    s.sql_editor.get_editor().copy();
                }
                true
            }
            "Edit/Paste" => {
                let s = state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let result_tabs_widget = s.result_tabs.get_widget();
                let focus_in_results = if let Some(focus) = app::focus() {
                    focus.as_widget_ptr() == result_tabs_widget.as_widget_ptr()
                        || focus.inside(&result_tabs_widget)
                } else {
                    false
                };

                if focus_in_results {
                    let _ = s.result_tabs.paste_from_clipboard();
                } else {
                    s.sql_editor.get_editor().paste();
                }
                true
            }
            "Edit/Select All" => {
                let mut s = state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let result_tabs_widget = s.result_tabs.get_widget();
                let focus_in_results = if let Some(focus) = app::focus() {
                    focus.as_widget_ptr() == result_tabs_widget.as_widget_ptr()
                        || focus.inside(&result_tabs_widget)
                } else {
                    false
                };

                if focus_in_results {
                    s.result_tabs.select_all();
                } else {
                    let len = s.sql_buffer.length();
                    s.sql_buffer.select(0, len);
                }
                true
            }
            "Query/Execute" => {
                if let Some(editor) = acquire_sql_editor_if_idle(state) {
                    editor.execute_current();
                }
                true
            }
            "Query/New Tab" => {
                let created_tab_id = {
                    let mut s = state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let created = MainWindow::create_query_editor_tab(&mut s);
                    s.right_tile.redraw();
                    created
                };
                if let Some(tab_id) = created_tab_id {
                    MainWindow::attach_editor_callbacks(state, tab_id, schema_sender.clone());
                    MainWindow::attach_file_drop_callback(state, tab_id, file_sender.clone());
                    state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .sql_editor
                        .focus();
                    app::redraw();
                }
                true
            }
            "Query/Close Tab" => {
                let tab_id = state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .active_editor_tab_id;
                MainWindow::close_query_editor_tab(state, tab_id);
                true
            }
            "Query/Execute Statement" => {
                if let Some(editor) = acquire_sql_editor_if_idle(state) {
                    editor.execute_statement_at_cursor();
                }
                true
            }
            "Query/Execute Statement (F9)" => {
                if let Some(editor) = acquire_sql_editor_if_idle(state) {
                    editor.execute_statement_at_cursor();
                }
                true
            }
            "Query/Execute Selected" => {
                if let Some(editor) = acquire_sql_editor_if_idle(state) {
                    editor.execute_selected();
                }
                true
            }
            "Query/Quick Describe" => {
                state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .sql_editor
                    .quick_describe_at_cursor();
                true
            }
            "Query/Explain Plan" => {
                if let Some(editor) = acquire_sql_editor_if_idle(state) {
                    editor.explain_current();
                }
                true
            }
            "Query/Commit" => {
                state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .sql_editor
                    .commit();
                true
            }
            "Query/Rollback" => {
                state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .sql_editor
                    .rollback();
                true
            }
            "Tools/Refresh Objects" => {
                let mut s = state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                MainWindow::start_connection_metadata_refresh(&mut s, schema_sender);
                true
            }
            "Tools/Session Lock Monitor..."
            | "Tools/Cursor Plan Analyzer..."
            | "Tools/SQL Monitor Dashboard..."
            | "Tools/Storage Dashboard..."
            | "Tools/Scheduler Manager..."
            | "Tools/Security Manager..."
            | "Tools/RMAN Dashboard..."
            | "Tools/AWR/ASH Dashboard..."
            | "Tools/Data Guard Dashboard..." => true,
            "Tools/Export Results..." => {
                let has_data = state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .result_tabs
                    .has_data();
                if !has_data {
                    fltk::dialog::alert_default("No results to export");
                    return true;
                }

                let mut dialog = FileDialog::new(FileDialogType::BrowseSaveFile);
                dialog.set_filter("CSV Files\t*.csv");
                dialog.show();
                let filename = dialog.filename();
                if filename.as_os_str().is_empty() {
                    return true;
                }

                let csv = state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .result_tabs
                    .export_to_csv();
                let row_count = state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .result_tabs
                    .row_count();
                let sender = file_sender.clone();
                thread::spawn(move || {
                    let result = fs::write(&filename, csv).map_err(|err| err.to_string());
                    let _ = sender.send(FileActionResult::Export {
                        path: filename,
                        row_count,
                        result,
                    });
                    app::awake();
                });
                true
            }
            "Edit/Find..." => {
                let (mut editor, mut buffer, popups) = {
                    let s = state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    (
                        s.sql_editor.get_editor(),
                        s.sql_buffer.clone(),
                        s.popups.clone(),
                    )
                };
                FindReplaceDialog::show_find_with_registry(&mut editor, &mut buffer, popups);
                true
            }
            "Edit/Find Next" => {
                let (mut editor, mut buffer, popups) = {
                    let s = state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    (
                        s.sql_editor.get_editor(),
                        s.sql_buffer.clone(),
                        s.popups.clone(),
                    )
                };
                if !FindReplaceDialog::find_next_from_session(&mut editor, &mut buffer)
                    && !FindReplaceDialog::has_search_text()
                {
                    FindReplaceDialog::show_find_with_registry(&mut editor, &mut buffer, popups);
                }
                true
            }
            "Edit/Replace..." => {
                let (mut editor, mut buffer, popups) = {
                    let s = state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    (
                        s.sql_editor.get_editor(),
                        s.sql_buffer.clone(),
                        s.popups.clone(),
                    )
                };
                FindReplaceDialog::show_replace_with_registry(&mut editor, &mut buffer, popups);
                true
            }
            "Edit/Format SQL" => {
                state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .sql_editor
                    .format_selected_sql();
                true
            }
            "Edit/Toggle Comment" => {
                state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .sql_editor
                    .toggle_comment();
                true
            }
            "Edit/Uppercase Selection" => {
                state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .sql_editor
                    .convert_selection_case(true);
                true
            }
            "Edit/Lowercase Selection" => {
                state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .sql_editor
                    .convert_selection_case(false);
                true
            }
            "Edit/Intellisense" => {
                state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .sql_editor
                    .show_intellisense();
                true
            }
            "Tools/Query History..." => {
                MainWindow::open_query_history_dialog(state);
                true
            }
            "Tools/Application Log..." => {
                let popups = state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .popups
                    .clone();
                crate::ui::log_viewer::LogViewerDialog::show(popups);
                true
            }
            "Tools/Auto-Commit" => {
                let mut item = app::widget_from_id::<MenuBar>("main_menu")
                    .and_then(|menu| menu.find_item("&Tools/&Auto-Commit"));
                let enabled = item.as_ref().map(|item| item.value()).unwrap_or(false);
                let status = if enabled {
                    "Auto-commit enabled"
                } else {
                    "Auto-commit disabled"
                };
                let connection = {
                    let s = state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    s.connection.clone()
                };
                if let Some(mut connection) =
                    try_lock_connection_with_activity(&connection, "Updating auto-commit setting")
                {
                    connection.set_auto_commit(enabled);
                } else {
                    let busy_message = format_connection_busy_message();
                    fltk::dialog::alert_default(&busy_message);
                    let mut s = state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let conn_info = s
                        .connection_info
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .clone();
                    s.status_bar
                        .set_label(&format_status(&busy_message, &conn_info));
                    if let Some(mut item) = item.take() {
                        if enabled {
                            item.clear();
                        } else {
                            item.set();
                        }
                    }
                    return true;
                }
                let mut s = state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let conn_info = s
                    .connection_info
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                s.status_bar.set_label(&format_status(status, &conn_info));
                true
            }
            "Settings/Preferences..." => {
                let config_snapshot = {
                    let s = state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let config_snapshot = s
                        .config
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .clone();
                    config_snapshot
                };
                if let Some(settings) = show_settings_dialog(&config_snapshot) {
                    let mut s = state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let save_result = {
                        let mut config = s
                            .config
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        config.editor_font = settings.font.clone();
                        config.ui_font_size = settings.ui_size;
                        config.editor_font_size = settings.editor_size;
                        config.result_font = settings.font;
                        config.result_font_size = settings.result_size;
                        config.result_cell_max_chars = settings.result_cell_max_chars;
                        config.save()
                    };
                    if let Err(err) = save_result {
                        fltk::dialog::alert_default(&format!("Failed to save settings: {}", err));
                    }
                    MainWindow::apply_font_settings(&mut s);
                }
                true
            }
            _ => false,
        }
    }

    fn strip_menu_label_shortcut(path: &str) -> String {
        let raw = path.split('\t').next().unwrap_or(path).trim();
        let label = if let Some(open_paren) = raw.rfind(" (") {
            if raw.ends_with(')') && raw[open_paren..].starts_with(" (") {
                raw[..open_paren].trim_end()
            } else {
                raw
            }
        } else {
            raw
        };
        label.replace('&', "")
    }

    fn menu_shortcut_for_key(
        key: fltk::enums::Key,
        modifiers: fltk::enums::Shortcut,
    ) -> Option<&'static str> {
        let ctrl_or_cmd = modifiers.contains(fltk::enums::Shortcut::Ctrl)
            || modifiers.contains(fltk::enums::Shortcut::Command);
        let shift = modifiers.contains(fltk::enums::Shortcut::Shift);

        match key {
            k if ctrl_or_cmd
                && (k == fltk::enums::Key::from_char('n')
                    || k == fltk::enums::Key::from_char('N')) =>
            {
                Some("File/Connect...")
            }
            k if ctrl_or_cmd
                && (k == fltk::enums::Key::from_char('d')
                    || k == fltk::enums::Key::from_char('D')) =>
            {
                Some("File/Disconnect")
            }
            k if ctrl_or_cmd
                && (k == fltk::enums::Key::from_char('o')
                    || k == fltk::enums::Key::from_char('O')) =>
            {
                Some("File/Open SQL File...")
            }
            k if ctrl_or_cmd
                && !shift
                && (k == fltk::enums::Key::from_char('s')
                    || k == fltk::enums::Key::from_char('S')) =>
            {
                Some("File/Save SQL File...")
            }
            k if ctrl_or_cmd
                && shift
                && (k == fltk::enums::Key::from_char('s')
                    || k == fltk::enums::Key::from_char('S')) =>
            {
                Some("File/Save SQL File As...")
            }
            k if ctrl_or_cmd
                && (k == fltk::enums::Key::from_char('q')
                    || k == fltk::enums::Key::from_char('Q')) =>
            {
                Some("File/Exit")
            }
            k if ctrl_or_cmd
                && shift
                && (k == fltk::enums::Key::from_char('z')
                    || k == fltk::enums::Key::from_char('Z')) =>
            {
                Some("Edit/Redo")
            }
            k if ctrl_or_cmd
                && (k == fltk::enums::Key::from_char('z')
                    || k == fltk::enums::Key::from_char('Z')) =>
            {
                Some("Edit/Undo")
            }
            k if ctrl_or_cmd
                && (k == fltk::enums::Key::from_char('y')
                    || k == fltk::enums::Key::from_char('Y')) =>
            {
                Some("Edit/Redo")
            }
            k if ctrl_or_cmd
                && (k == fltk::enums::Key::from_char('x')
                    || k == fltk::enums::Key::from_char('X')) =>
            {
                Some("Edit/Cut")
            }
            k if ctrl_or_cmd
                && shift
                && (k == fltk::enums::Key::from_char('c')
                    || k == fltk::enums::Key::from_char('C')) =>
            {
                Some("Edit/Copy with Headers")
            }
            k if ctrl_or_cmd
                && (k == fltk::enums::Key::from_char('c')
                    || k == fltk::enums::Key::from_char('C')) =>
            {
                Some("Edit/Copy")
            }
            k if ctrl_or_cmd
                && (k == fltk::enums::Key::from_char('v')
                    || k == fltk::enums::Key::from_char('V')) =>
            {
                Some("Edit/Paste")
            }
            k if ctrl_or_cmd
                && (k == fltk::enums::Key::from_char('a')
                    || k == fltk::enums::Key::from_char('A')) =>
            {
                Some("Edit/Select All")
            }
            fltk::enums::Key::F3 => Some("Edit/Find Next"),
            k if ctrl_or_cmd
                && (k == fltk::enums::Key::from_char('h')
                    || k == fltk::enums::Key::from_char('H')) =>
            {
                Some("Edit/Replace...")
            }
            k if ctrl_or_cmd
                && shift
                && (k == fltk::enums::Key::from_char('f')
                    || k == fltk::enums::Key::from_char('F')) =>
            {
                Some("Edit/Format SQL")
            }
            k if ctrl_or_cmd
                && (k == fltk::enums::Key::from_char('f')
                    || k == fltk::enums::Key::from_char('F')) =>
            {
                Some("Edit/Find...")
            }
            k if ctrl_or_cmd
                && (k == fltk::enums::Key::from_char('/')
                    || k == fltk::enums::Key::from_char('?')) =>
            {
                Some("Edit/Toggle Comment")
            }
            k if ctrl_or_cmd
                && (k == fltk::enums::Key::from_char('u')
                    || k == fltk::enums::Key::from_char('U')) =>
            {
                Some("Edit/Uppercase Selection")
            }
            k if ctrl_or_cmd
                && (k == fltk::enums::Key::from_char('l')
                    || k == fltk::enums::Key::from_char('L')) =>
            {
                Some("Edit/Lowercase Selection")
            }
            k if ctrl_or_cmd
                && (k == fltk::enums::Key::from_char(' ')
                    || k == fltk::enums::Key::from_char('\u{0020}')) =>
            {
                Some("Edit/Intellisense")
            }
            k if ctrl_or_cmd
                && (k == fltk::enums::Key::from_char('t')
                    || k == fltk::enums::Key::from_char('T')) =>
            {
                Some("Query/New Tab")
            }
            k if ctrl_or_cmd
                && (k == fltk::enums::Key::from_char('w')
                    || k == fltk::enums::Key::from_char('W')) =>
            {
                Some("Query/Close Tab")
            }
            fltk::enums::Key::F5 => Some("Query/Execute"),
            k if ctrl_or_cmd
                && (k == fltk::enums::Key::Enter || k == fltk::enums::Key::KPEnter) =>
            {
                Some("Query/Execute Statement")
            }
            fltk::enums::Key::F9 => Some("Query/Execute Statement (F9)"),
            fltk::enums::Key::F4 => Some("Query/Quick Describe"),
            fltk::enums::Key::F6 => Some("Query/Explain Plan"),
            fltk::enums::Key::F7 => Some("Query/Commit"),
            fltk::enums::Key::F8 => Some("Query/Rollback"),
            k if ctrl_or_cmd
                && (k == fltk::enums::Key::from_char('e')
                    || k == fltk::enums::Key::from_char('E')) =>
            {
                Some("Tools/Export Results...")
            }
            _ => None,
        }
    }

    fn resolve_window_shortcut_action(
        event_key: fltk::enums::Key,
        event_original_key: fltk::enums::Key,
        event_state: fltk::enums::Shortcut,
    ) -> Option<&'static str> {
        Self::menu_shortcut_for_key(event_key, event_state)
            .or_else(|| Self::menu_shortcut_for_key(event_original_key, event_state))
    }

    fn handle_window_shortcut(
        state: &Arc<Mutex<AppState>>,
        schema_sender: &std::sync::mpsc::Sender<SchemaUpdate>,
        conn_sender: &std::sync::mpsc::Sender<ConnectionResult>,
        file_sender: &std::sync::mpsc::Sender<FileActionResult>,
    ) -> bool {
        let event_key = app::event_key();
        let event_original_key = app::event_original_key();
        let event_state = app::event_state();
        let Some(action) =
            Self::resolve_window_shortcut_action(event_key, event_original_key, event_state)
        else {
            return false;
        };
        Self::execute_menu_action(state, schema_sender, conn_sender, file_sender, action)
    }

    pub fn setup_callbacks(&mut self) {
        let state = self.state.clone();
        let (schema_sender, schema_receiver) = std::sync::mpsc::channel::<SchemaUpdate>();
        let (conn_sender, conn_receiver) = std::sync::mpsc::channel::<ConnectionResult>();
        let (file_sender, file_receiver) = std::sync::mpsc::channel::<FileActionResult>();

        let tab_ids: Vec<QueryTabId> = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .editor_tabs
            .iter()
            .map(|tab| tab.tab_id)
            .collect();
        for tab_id in tab_ids {
            Self::attach_editor_callbacks(&state, tab_id, schema_sender.clone());
        }

        let (mut object_browser, mut window) = {
            let s = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            (s.object_browser.clone(), s.window.clone())
        };

        // Setup object browser callback
        let weak_state_for_browser_status = Arc::downgrade(&state);
        object_browser.set_status_callback(move |message| {
            let Some(state_for_status) = weak_state_for_browser_status.upgrade() else {
                return;
            };

            if let Ok(mut s) = state_for_status.try_lock() {
                let conn_info = s
                    .connection_info
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                s.status_bar.set_label(&format_status(message, &conn_info));
            };
        });

        let weak_state_for_browser = Arc::downgrade(&state);
        let schema_sender_for_browser = schema_sender.clone();
        let file_sender_for_browser = file_sender.clone();
        object_browser.set_sql_callback(move |action| {
            let Some(state_for_browser) = weak_state_for_browser.upgrade() else {
                return;
            };
            let mut created_tab_for_generated_sql: Option<QueryTabId> = None;
            let mut sql_to_execute: Option<String> = None;
            {
                let mut s = state_for_browser
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                match action {
                    SqlAction::Insert(text) => {
                        let mut editor = s.sql_editor.get_editor();
                        let insert_pos = editor.insert_position();
                        s.sql_buffer.insert(insert_pos, &text);
                        editor.set_insert_position(insert_pos + text.len() as i32);
                    }
                    SqlAction::OpenInNewTab(sql) => {
                        if let Some(tab_id) = MainWindow::create_query_editor_tab(&mut s) {
                            s.sql_buffer.set_text(&sql);
                            s.sql_editor.reset_undo_redo_history();
                            s.set_tab_file_path(tab_id, None);
                            s.set_tab_pristine_text(tab_id, sql);
                            s.sql_editor.focus();
                            s.right_tile.redraw();
                            created_tab_for_generated_sql = Some(tab_id);
                        }
                    }
                    SqlAction::Execute(sql) => {
                        sql_to_execute = Some(sql);
                    }
                }
            }

            if let Some(sql) = sql_to_execute {
                if let Some(editor) = acquire_sql_editor_if_idle(&state_for_browser) {
                    editor.execute_sql_text(&sql);
                }
            }

            if let Some(tab_id) = created_tab_for_generated_sql {
                MainWindow::attach_editor_callbacks(
                    &state_for_browser,
                    tab_id,
                    schema_sender_for_browser.clone(),
                );
                MainWindow::attach_file_drop_callback(
                    &state_for_browser,
                    tab_id,
                    file_sender_for_browser.clone(),
                );
                state_for_browser
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .sql_editor
                    .focus();
                app::redraw();
            }
        });

        let weak_state_for_window = Arc::downgrade(&state);
        let schema_sender_for_window = schema_sender.clone();
        let conn_sender_for_window = conn_sender.clone();
        let file_sender_for_window = file_sender.clone();
        window.handle(move |_w, ev| {
            let Some(state_for_window) = weak_state_for_window.upgrade() else {
                return false;
            };
            match ev {
                fltk::enums::Event::KeyDown => {
                    if app::event_key() == fltk::enums::Key::Escape {
                        return true;
                    }
                    if MainWindow::handle_window_shortcut(
                        &state_for_window,
                        &schema_sender_for_window,
                        &conn_sender_for_window,
                        &file_sender_for_window,
                    ) {
                        return true;
                    }
                    false
                }
                fltk::enums::Event::Shortcut => {
                    if MainWindow::handle_window_shortcut(
                        &state_for_window,
                        &schema_sender_for_window,
                        &conn_sender_for_window,
                        &file_sender_for_window,
                    ) {
                        return true;
                    }
                    false
                }
                fltk::enums::Event::Push => {
                    let sql_editor = {
                        let s = state_for_window
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        s.sql_editor.clone()
                    };
                    sql_editor
                        .hide_intellisense_if_outside(app::event_x_root(), app::event_y_root());
                    false
                }
                fltk::enums::Event::Resize => {
                    if let Ok(s) = state_for_window.try_lock() {
                        MainWindow::adjust_query_layout_on_resize(&s);
                    }
                    false
                }
                _ => false,
            }
        });

        self.setup_menu_callbacks(
            schema_sender,
            schema_receiver,
            conn_sender,
            conn_receiver,
            file_sender,
            file_receiver,
        );
    }

    fn setup_menu_callbacks(
        &mut self,
        schema_sender: std::sync::mpsc::Sender<SchemaUpdate>,
        schema_receiver: std::sync::mpsc::Receiver<SchemaUpdate>,
        conn_sender: std::sync::mpsc::Sender<ConnectionResult>,
        conn_receiver: std::sync::mpsc::Receiver<ConnectionResult>,
        file_sender: std::sync::mpsc::Sender<FileActionResult>,
        file_receiver: std::sync::mpsc::Receiver<FileActionResult>,
    ) {
        let state = self.state.clone();

        // Wrap receivers in Arc<Mutex> to share across timeout callbacks
        let schema_receiver: Arc<Mutex<std::sync::mpsc::Receiver<SchemaUpdate>>> =
            Arc::new(Mutex::new(schema_receiver));
        let conn_receiver: Arc<Mutex<std::sync::mpsc::Receiver<ConnectionResult>>> =
            Arc::new(Mutex::new(conn_receiver));
        let file_receiver: Arc<Mutex<std::sync::mpsc::Receiver<FileActionResult>>> =
            Arc::new(Mutex::new(file_receiver));
        let idle_poll_cycles = Arc::new(AtomicUsize::new(0));

        const CHANNEL_POLL_ACTIVE_INTERVAL_SECONDS: f64 = 0.05;
        const CHANNEL_POLL_IDLE_INTERVAL_SECONDS: f64 = 0.25;
        const MEMORY_TRIM_IDLE_CYCLE_THRESHOLD: usize =
            (60.0 / CHANNEL_POLL_IDLE_INTERVAL_SECONDS) as usize;

        fn schedule_poll(
            schema_receiver: Arc<Mutex<std::sync::mpsc::Receiver<SchemaUpdate>>>,
            conn_receiver: Arc<Mutex<std::sync::mpsc::Receiver<ConnectionResult>>>,
            file_receiver: Arc<Mutex<std::sync::mpsc::Receiver<FileActionResult>>>,
            state_weak: std::sync::Weak<Mutex<AppState>>,
            schema_sender: std::sync::mpsc::Sender<SchemaUpdate>,
            file_sender: std::sync::mpsc::Sender<FileActionResult>,
            idle_poll_cycles: Arc<AtomicUsize>,
        ) {
            let Some(state) = state_weak.upgrade() else {
                return;
            };
            let mut schema_disconnected = false;
            let mut conn_disconnected = false;
            let mut file_disconnected = false;
            let mut deferred_by_borrow_conflict = false;
            let mut processed_message = false;

            // Check for schema updates
            {
                let r = schema_receiver
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let current_generation = match state.try_lock() {
                    Ok(s) => {
                        let guard = try_lock_connection_with_activity(
                            &s.connection,
                            "Checking schema update generation",
                        );
                        match guard {
                            Some(connection_guard) => connection_guard.connection_generation(),
                            None => {
                                deferred_by_borrow_conflict = true;
                                0
                            }
                        }
                    }
                    Err(_) => {
                        deferred_by_borrow_conflict = true;
                        0
                    }
                };

                if !deferred_by_borrow_conflict {
                    let mut latest_update: Option<SchemaUpdate> = None;
                    loop {
                        match r.try_recv() {
                            Ok(update) => {
                                if update.connection_generation != current_generation {
                                    continue;
                                }
                                latest_update = Some(update);
                                processed_message = true;
                            }
                            Err(std::sync::mpsc::TryRecvError::Empty) => break,
                            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                                schema_disconnected = true;
                                break;
                            }
                        }
                    }

                    if let Some(update) = latest_update {
                        match state.try_lock() {
                            Ok(mut s) => {
                                MainWindow::update_schema_snapshot(
                                    &mut s,
                                    update.data,
                                    update.highlight_data,
                                );
                                s.apply_schema_to_active_tab_if_needed();
                            }
                            Err(_) => {
                                deferred_by_borrow_conflict = true;
                            }
                        }
                    }
                }
            }

            // Check for connection results
            {
                let r = conn_receiver
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                loop {
                    let Ok(mut s) = state.try_lock() else {
                        deferred_by_borrow_conflict = true;
                        break;
                    };
                    match r.try_recv() {
                        Ok(result) => {
                            processed_message = true;
                            match result {
                                ConnectionResult::Success(info) => {
                                    crate::utils::logging::log_info(
                                        "connection",
                                        &format!("Connected to {}", info.name),
                                    );
                                    *s.connection_info
                                        .lock()
                                        .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                                        Some(info.clone());
                                    s.has_live_connection = true;
                                    s.pending_connection_metadata_refresh = false;
                                    s.status_bar
                                        .set_label(&format!("Connected | {}", info.name));
                                    MainWindow::start_connection_metadata_refresh(
                                        &mut s,
                                        &schema_sender,
                                    );
                                    s.sql_editor.focus();
                                    s.refresh_connection_dependent_controls();
                                }
                                ConnectionResult::Failure(err) => {
                                    let current_connection = s
                                        .connection_info
                                        .lock()
                                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                                        .clone();
                                    let current_connection_label =
                                        current_connection.as_ref().map(|info| info.name.clone());

                                    if let Some(current_label) = current_connection_label {
                                        crate::utils::logging::log_error(
                                            "connection",
                                            &format!(
                                                "Connection failed: {} (keeping current connection: {})",
                                                err, current_label
                                            ),
                                        );
                                        s.status_bar.set_label(&format_status(
                                            "Connection failed; keeping current connection",
                                            &current_connection,
                                        ));
                                        let lines = vec![
                                            format!("Connection failed: {}", err),
                                            format!(
                                                "Keeping current connection: {}",
                                                current_label
                                            ),
                                        ];
                                        s.result_tabs.append_script_output_lines(&lines);
                                    } else {
                                        crate::utils::logging::log_error(
                                            "connection",
                                            &format!("Connection failed: {}", err),
                                        );
                                        s.status_bar.set_label("Connection failed");
                                        s.result_tabs.append_script_output_lines(&[format!(
                                            "Connection failed: {}",
                                            err
                                        )]);
                                    }
                                    s.result_tabs.select_script_output();
                                }
                            }
                        }
                        Err(std::sync::mpsc::TryRecvError::Empty) => break,
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            conn_disconnected = true;
                            break;
                        }
                    }
                }
            }

            // Check for file operations
            {
                let r = file_receiver
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                loop {
                    let Ok(mut s) = state.try_lock() else {
                        deferred_by_borrow_conflict = true;
                        break;
                    };
                    match r.try_recv() {
                        Ok(result) => {
                            processed_message = true;
                            let mut created_tab_for_open: Option<QueryTabId> = None;
                            let mut created_editor_for_open: Option<SqlEditorWidget> = None;
                            let mut created_right_tile_for_open: Option<Tile> = None;
                            match result {
                                FileActionResult::OpenInNewTab { path, result } => match result {
                                    Ok(content) => {
                                        if MainWindow::focus_existing_tab_with_same_file_name(
                                            &mut s, &path,
                                        ) {
                                            continue;
                                        }
                                        let normalized_content =
                                            MainWindow::normalize_line_endings_for_editor(content);
                                        if let Some(tab_id) =
                                            MainWindow::create_query_editor_tab(&mut s)
                                        {
                                            s.sql_buffer.set_text(&normalized_content);
                                            s.sql_editor.reset_undo_redo_history();
                                            s.set_tab_file_path(tab_id, Some(path.clone()));
                                            s.set_tab_pristine_text(tab_id, normalized_content);
                                            created_editor_for_open = Some(s.sql_editor.clone());
                                            created_right_tile_for_open =
                                                Some(s.right_tile.clone());
                                            created_tab_for_open = Some(tab_id);
                                        }
                                    }
                                    Err(err) => {
                                        fltk::dialog::alert_default(&format!(
                                            "Failed to open SQL file: {}",
                                            err
                                        ));
                                    }
                                },
                                FileActionResult::Export {
                                    path,
                                    row_count,
                                    result,
                                } => match result {
                                    Ok(()) => {
                                        let file_label =
                                            path.file_name().unwrap_or_default().to_string_lossy();
                                        let conn_info = s
                                            .connection_info
                                            .lock()
                                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                                            .clone();
                                        s.status_bar.set_label(&format_status(
                                            &format!(
                                                "Exported {} rows to {}",
                                                row_count, file_label
                                            ),
                                            &conn_info,
                                        ));
                                    }
                                    Err(err) => {
                                        fltk::dialog::alert_default(&format!(
                                            "Failed to export CSV: {}",
                                            err
                                        ));
                                    }
                                },
                            }

                            drop(s);

                            if let Some(tab_id) = created_tab_for_open {
                                MainWindow::attach_editor_callbacks(
                                    &state,
                                    tab_id,
                                    schema_sender.clone(),
                                );
                                MainWindow::attach_file_drop_callback(
                                    &state,
                                    tab_id,
                                    file_sender.clone(),
                                );
                                if let Some(mut editor) = created_editor_for_open {
                                    editor.focus();
                                }
                                if let Some(mut right_tile) = created_right_tile_for_open {
                                    right_tile.redraw();
                                }
                                app::redraw();
                            }
                        }
                        Err(std::sync::mpsc::TryRecvError::Empty) => break,
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            file_disconnected = true;
                            break;
                        }
                    }
                }
            }

            if deferred_by_borrow_conflict {
                app::add_timeout3(CHANNEL_POLL_ACTIVE_INTERVAL_SECONDS, move |_| {
                    schedule_poll(
                        schema_receiver.clone(),
                        conn_receiver.clone(),
                        file_receiver.clone(),
                        state_weak.clone(),
                        schema_sender.clone(),
                        file_sender.clone(),
                        idle_poll_cycles.clone(),
                    );
                });
                return;
            }

            // Stop polling if all channels are disconnected
            if schema_disconnected && conn_disconnected && file_disconnected {
                return;
            }

            let delay = if processed_message {
                idle_poll_cycles.store(0, Ordering::Relaxed);
                CHANNEL_POLL_ACTIVE_INTERVAL_SECONDS
            } else {
                let idle_cycles = idle_poll_cycles
                    .fetch_add(1, Ordering::Relaxed)
                    .saturating_add(1);
                if idle_cycles >= MEMORY_TRIM_IDLE_CYCLE_THRESHOLD {
                    idle_poll_cycles.store(0, Ordering::Relaxed);
                    malloc_trim_process();
                }
                CHANNEL_POLL_IDLE_INTERVAL_SECONDS
            };

            // Reschedule for next poll
            app::add_timeout3(delay, move |_| {
                schedule_poll(
                    schema_receiver.clone(),
                    conn_receiver.clone(),
                    file_receiver.clone(),
                    state_weak.clone(),
                    schema_sender.clone(),
                    file_sender.clone(),
                    idle_poll_cycles.clone(),
                );
            });
        }

        // Start polling
        let weak_state_for_poll = Arc::downgrade(&state);
        let schema_sender_for_poll = schema_sender.clone();
        {
            let mut s = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            s.schema_sender = Some(schema_sender.clone());
            s.file_sender = Some(file_sender.clone());
        }
        schedule_poll(
            schema_receiver,
            conn_receiver,
            file_receiver,
            weak_state_for_poll,
            schema_sender_for_poll,
            file_sender.clone(),
            idle_poll_cycles,
        );

        let tab_ids_for_drop: Vec<QueryTabId> = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .editor_tabs
            .iter()
            .map(|tab| tab.tab_id)
            .collect();
        for tab_id in tab_ids_for_drop {
            Self::attach_file_drop_callback(&state, tab_id, file_sender.clone());
        }

        if let Some(mut menu) = app::widget_from_id::<MenuBar>("main_menu") {
            let weak_state_for_menu = Arc::downgrade(&state);
            let schema_sender_for_menu = schema_sender;
            let conn_sender_for_menu = conn_sender;
            let file_sender_for_menu = file_sender;
            menu.set_callback(move |m| {
                let Some(state_for_menu) = weak_state_for_menu.upgrade() else {
                    return;
                };
                let menu_path = m.item_pathname(None).ok().or_else(|| m.choice());
                if let Some(path) = menu_path {
                    let choice = MainWindow::strip_menu_label_shortcut(&path);
                    if MainWindow::execute_menu_action(
                        &state_for_menu,
                        &schema_sender_for_menu,
                        &conn_sender_for_menu,
                        &file_sender_for_menu,
                        &choice,
                    ) {
                        // FLTK keeps the last activated menu item selected. When the selection
                        // doesn't change, repeated keyboard shortcuts for the same item may not
                        // trigger again. Clear the current value so Ctrl+N/Ctrl+S can fire
                        // repeatedly without requiring a different shortcut in between.
                        m.set_value(-1);
                    }
                }
            });
        }
    }

    pub fn show(&mut self) {
        let state = self.state.clone();
        let mut window = {
            let s = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            s.window.clone()
        };
        let weak_state_for_close = Arc::downgrade(&state);
        window.set_callback(move |w| {
            if let Some(state) = weak_state_for_close.upgrade() {
                if !MainWindow::confirm_save_for_all_dirty_tabs(&state) {
                    return;
                }
                crate::db::clear_tracked_db_activity();
                let (popups, editor_tabs, mut result_tabs) = {
                    let s = state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    (
                        s.popups.clone(),
                        s.editor_tabs.clone(),
                        s.result_tabs.clone(),
                    )
                };
                let mut popups = popups
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                for mut popup in popups.drain(..) {
                    if popup.was_deleted() {
                        continue;
                    }
                    popup.hide();
                    Window::delete(popup);
                }
                for mut tab in editor_tabs {
                    tab.sql_editor.cleanup_for_close();
                }
                // Clean up result tabs to release FLTK widget callbacks and data buffers
                result_tabs.clear();
            }
            crate::ui::sql_editor::SqlEditorWidget::shutdown_column_load_workers();
            if let Err(err) = crate::utils::logging::flush_log_writer() {
                eprintln!("Application log flush on exit failed: {err}");
            }
            w.hide();
            app::quit();
        });
        window.show();
        app::flush();
        let _ = app::wait();
        crate::db::clear_tracked_db_activity();
        {
            let mut s = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            MainWindow::adjust_query_layout(&mut s);
            s.window.redraw();
            s.sql_editor.focus();
        }
    }

    pub fn show_previous_crash_report(crash_report: &str) {
        crate::utils::logging::log_warning(
            "app",
            "Previous session ended with a crash. Crash report was shown to user.",
        );
        let crash_message = format!(
            "The previous session ended unexpectedly.

{}

The crash has been recorded in the application log.",
            crash_report
        );
        SqlEditorWidget::show_quick_describe_text_dialog(
            "Previous Session Crash Report",
            &crash_message,
        );
    }

    pub fn run() {
        let app = app::App::default()
            .with_scheme(app::Scheme::Gtk)
            .load_system_fonts();
        let config = AppConfig::load();
        crate::app::configure_fltk_globals(&config);

        let current_group = fltk::group::Group::try_current();

        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let mut main_window = MainWindow::new_with_config(config);
        main_window.setup_callbacks();
        main_window.show();

        // Check for crash log from a previous session
        if let Some(crash_report) = crate::utils::logging::take_crash_log() {
            Self::show_previous_crash_report(&crash_report);
        }

        match app.run() {
            Ok(()) => {}
            Err(err) => {
                crate::utils::logging::log_error("app", &format!("App run error: {err}"));
                eprintln!("Failed to run app: {err}");
            }
        }
        // Restore current group
        if let Some(ref group) = current_group {
            fltk::group::Group::set_current(Some(group));
        }
    }

    #[allow(dead_code)]
    fn export_results_csv(
        path: &PathBuf,
        result: &QueryResult,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut output = String::new();

        let headers: Vec<String> = result.columns.iter().map(|c| c.name.clone()).collect();
        output.push_str(&Self::csv_row(&headers));
        output.push('\n');

        for row in &result.rows {
            output.push_str(&Self::csv_row(row));
            output.push('\n');
        }

        match fs::write(path, output) {
            Ok(()) => {}
            Err(err) => {
                eprintln!("CSV export error: {err}");
                return Err(Box::new(err));
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn csv_row(values: &[String]) -> String {
        values
            .iter()
            .map(|value| Self::csv_escape(value))
            .collect::<Vec<String>>()
            .join(",")
    }

    #[allow(dead_code)]
    fn csv_escape(value: &str) -> String {
        if value.contains(',') || value.contains('"') || value.contains('\n') {
            format!("\"{}\"", value.replace('"', "\"\""))
        } else {
            value.to_string()
        }
    }

    #[allow(dead_code)]
    fn format_query_history(history: &QueryHistory) -> String {
        if history.queries.is_empty() {
            return "No query history yet.".to_string();
        }

        let mut lines = vec!["Recent Queries (latest first):".to_string()];
        for entry in history.queries.iter().take(20) {
            lines.push(format!(
                "[{}] {} | {} ms | {} rows",
                entry.timestamp, entry.connection_name, entry.execution_time_ms, entry.row_count
            ));
            lines.push(entry.sql.trim().to_string());
            lines.push(String::new());
        }

        lines.join("\n")
    }

    fn normalize_line_endings_for_editor(mut text: String) -> String {
        if !text.contains('\r') {
            return text;
        }

        text = text.replace("\r\n", "\n");
        text.replace('\r', "\n")
    }
}

impl Default for MainWindow {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fltk::enums::{Key, Shortcut};

    #[test]
    fn resolve_window_shortcut_prefers_current_key_match() {
        let action = MainWindow::resolve_window_shortcut_action(
            Key::from_char('f'),
            Key::from_char('x'),
            Shortcut::Ctrl,
        );

        assert_eq!(action, Some("Edit/Find..."));
    }

    #[test]
    fn resolve_window_shortcut_uses_original_key_for_non_ascii_layout() {
        let action = MainWindow::resolve_window_shortcut_action(
            Key::from_char('ㄹ'),
            Key::from_char('f'),
            Shortcut::Ctrl,
        );

        assert_eq!(action, Some("Edit/Find..."));
    }

    #[test]
    fn normalize_line_endings_for_editor_converts_crlf_and_cr_to_lf() {
        let text = String::from("select 1;\r\nselect 2;\rselect 3;");
        let normalized = MainWindow::normalize_line_endings_for_editor(text);

        assert_eq!(normalized, "select 1;\nselect 2;\nselect 3;");
    }

    #[test]
    fn normalize_line_endings_for_editor_keeps_lf_only_content() {
        let text = String::from("select 1;\nselect 2;");
        let normalized = MainWindow::normalize_line_endings_for_editor(text.clone());

        assert_eq!(normalized, text);
    }

    #[test]
    fn resolve_result_tab_offset_uses_target_when_it_is_valid() {
        assert_eq!(resolve_result_tab_offset(5, Some(2)), 2);
    }

    #[test]
    fn resolve_result_tab_offset_falls_back_to_tab_count_when_target_is_invalid() {
        assert_eq!(resolve_result_tab_offset(5, Some(5)), 5);
        assert_eq!(resolve_result_tab_offset(5, Some(9)), 5);
    }

    #[test]
    fn resolve_result_tab_offset_falls_back_to_tab_count_when_target_is_missing() {
        assert_eq!(resolve_result_tab_offset(5, None), 5);
    }

    #[test]
    fn validate_result_edit_action_allows_when_no_query_is_running() {
        assert!(validate_result_edit_action_allowed(false).is_ok());
    }

    #[test]
    fn validate_result_edit_action_blocks_when_query_is_running() {
        assert_eq!(
            validate_result_edit_action_allowed(true),
            Err("A query is running. Wait for completion before editing result rows.".to_string())
        );
    }

    #[test]
    fn resolve_progress_tab_index_uses_valid_target_for_grid_execution() {
        assert_eq!(resolve_progress_tab_index(5, 9, Some(2), 0), 2);
        assert_eq!(resolve_progress_tab_index(5, 9, Some(2), 1), 3);
    }

    #[test]
    fn resolve_progress_tab_index_clamps_stale_offset_when_target_is_missing() {
        assert_eq!(resolve_progress_tab_index(4, 9, None, 0), 4);
        assert_eq!(resolve_progress_tab_index(4, 9, None, 2), 6);
    }

    #[test]
    fn resolve_progress_tab_index_keeps_batch_offset_when_tabs_grow() {
        assert_eq!(resolve_progress_tab_index(6, 3, None, 0), 3);
        assert_eq!(resolve_progress_tab_index(6, 3, None, 2), 5);
    }
}
