use fltk::{
    app,
    draw::set_cursor,
    enums::{Cursor, FrameType},
    frame::Frame,
    group::{Flex, FlexType},
    input::IntInput,
    prelude::*,
    text::{TextBuffer, TextEditor, WrapMode},
};
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashSet;
use std::panic::{self, AssertUnwindSafe};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::db::{
    ConnectionInfo, QueryExecutor, QueryResult, SharedConnection, TableColumnDetail,
};
use crate::ui::constants::*;
use crate::ui::font_settings::{configured_editor_profile, configured_ui_font_size, FontProfile};
use crate::ui::intellisense::{IntellisenseData, IntellisensePopup};
use crate::ui::query_history::{flush_history_writer_with_timeout, QueryHistoryDialog};
use crate::ui::syntax_highlight::{
    create_style_table_with, HighlightData, SqlHighlighter, STYLE_COMMENT, STYLE_DEFAULT,
    STYLE_STRING,
};
use crate::ui::theme;
use crate::utils::{AppConfig, QueryHistory, QueryHistoryEntry};
use oracle::Connection;

mod execution;
mod intellisense;
// 공통 파싱/토큰 유틸(실행, 인텔리센스, 포맷팅 공통 경로)
mod query_text;

#[derive(Clone, Debug)]
pub(crate) enum SqlToken {
    Word(String),
    String(String),
    Comment(String),
    Symbol(String),
}

const INTELLISENSE_WORD_WINDOW: i32 = 256;
const INTELLISENSE_CONTEXT_WINDOW: i32 = 120_000;
const INTELLISENSE_QUALIFIER_WINDOW: i32 = 256;
const INTELLISENSE_STATEMENT_WINDOW: i32 = 120_000;
const MAX_PROGRESS_MESSAGES_PER_POLL: usize = 200;
const PROGRESS_POLL_INTERVAL_SECONDS: f64 = 0.05;
const MAX_WORD_UNDO_HISTORY: usize = 500;
const EDITOR_TOP_PADDING: i32 = 4;
const HISTORY_NAVIGATION_FLUSH_TIMEOUT: Duration = Duration::from_millis(200);

#[derive(Clone, Copy, PartialEq, Eq)]
enum EditGranularity {
    Word,
    Other,
}

#[derive(Clone)]
struct WordUndoRedoState {
    history: Vec<String>,
    index: usize,
    active_group: Option<EditGranularity>,
    applying_history: bool,
}

impl WordUndoRedoState {
    fn new(initial_text: String) -> Self {
        Self {
            history: vec![initial_text],
            index: 0,
            active_group: None,
            applying_history: false,
        }
    }

    fn normalize(&mut self, current_text: &str) {
        if self.history.is_empty() {
            self.history.push(current_text.to_string());
            self.index = 0;
            self.active_group = None;
            return;
        }

        if self.index >= self.history.len() {
            self.index = self.history.len().saturating_sub(1);
            self.active_group = None;
        }
    }

    fn current_snapshot_matches(&self, current_text: &str) -> bool {
        self.history
            .get(self.index)
            .map(|text| text == current_text)
            .unwrap_or(false)
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
    QueryAlreadyRunning,
}

#[derive(Clone)]
pub struct SqlEditorWidget {
    group: Flex,
    editor: TextEditor,
    buffer: TextBuffer,
    style_buffer: TextBuffer,
    connection: SharedConnection,
    execute_callback: Rc<RefCell<Option<Box<dyn FnMut(&QueryResult)>>>>,
    progress_callback: Rc<RefCell<Option<Box<dyn FnMut(QueryProgress)>>>>,
    progress_sender: mpsc::Sender<QueryProgress>,
    column_sender: mpsc::Sender<ColumnLoadUpdate>,
    ui_action_sender: mpsc::Sender<UiActionResult>,
    query_running: Rc<RefCell<bool>>,
    current_query_connection: Arc<Mutex<Option<Arc<Connection>>>>,
    cancel_flag: Arc<AtomicBool>,
    intellisense_data: Rc<RefCell<IntellisenseData>>,
    intellisense_popup: Rc<RefCell<IntellisensePopup>>,
    highlighter: Rc<RefCell<SqlHighlighter>>,
    timeout_input: IntInput,
    status_callback: Rc<RefCell<Option<Box<dyn FnMut(&str)>>>>,
    find_callback: Rc<RefCell<Option<Box<dyn FnMut()>>>>,
    replace_callback: Rc<RefCell<Option<Box<dyn FnMut()>>>>,
    file_drop_callback: Rc<RefCell<Option<Box<dyn FnMut(PathBuf)>>>>,
    completion_range: Rc<RefCell<Option<(usize, usize)>>>,
    pending_intellisense: Rc<RefCell<Option<PendingIntellisense>>>,
    history_cursor: Rc<RefCell<Option<usize>>>,
    history_original: Rc<RefCell<Option<String>>>,
    history_navigation_entries: Rc<RefCell<Option<Vec<QueryHistoryEntry>>>>,
    applying_history_navigation: Rc<RefCell<bool>>,
    undo_redo_state: Rc<RefCell<WordUndoRedoState>>,
}

impl SqlEditorWidget {
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
        callback_slot: &Rc<RefCell<Option<Box<dyn FnMut(&QueryResult)>>>>,
        result: &QueryResult,
    ) {
        let callback = {
            let mut slot = callback_slot.borrow_mut();
            slot.take()
        };

        if let Some(mut cb) = callback {
            let call_result = panic::catch_unwind(AssertUnwindSafe(|| cb(result)));
            let mut slot = callback_slot.borrow_mut();
            if slot.is_none() {
                *slot = Some(cb);
            }
            if let Err(payload) = call_result {
                Self::log_callback_panic("query result callback", payload.as_ref());
            }
        }
    }

    fn invoke_progress_callback(
        callback_slot: &Rc<RefCell<Option<Box<dyn FnMut(QueryProgress)>>>>,
        message: QueryProgress,
    ) {
        let callback = {
            let mut slot = callback_slot.borrow_mut();
            slot.take()
        };

        if let Some(mut cb) = callback {
            let call_result = panic::catch_unwind(AssertUnwindSafe(|| cb(message)));
            let mut slot = callback_slot.borrow_mut();
            if slot.is_none() {
                *slot = Some(cb);
            }
            if let Err(payload) = call_result {
                Self::log_callback_panic("progress callback", payload.as_ref());
            }
        }
    }

    fn invoke_status_callback(
        callback_slot: &Rc<RefCell<Option<Box<dyn FnMut(&str)>>>>,
        message: &str,
    ) {
        let callback = {
            let mut slot = callback_slot.borrow_mut();
            slot.take()
        };

        if let Some(mut cb) = callback {
            let call_result = panic::catch_unwind(AssertUnwindSafe(|| cb(message)));
            let mut slot = callback_slot.borrow_mut();
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

        let execute_callback: Rc<RefCell<Option<Box<dyn FnMut(&QueryResult)>>>> =
            Rc::new(RefCell::new(None));
        let progress_callback: Rc<RefCell<Option<Box<dyn FnMut(QueryProgress)>>>> =
            Rc::new(RefCell::new(None));
        let (progress_sender, progress_receiver) = mpsc::channel::<QueryProgress>();
        let (column_sender, column_receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let (ui_action_sender, ui_action_receiver) = mpsc::channel::<UiActionResult>();
        let query_running = Rc::new(RefCell::new(false));
        let current_query_connection = Arc::new(Mutex::new(None));
        let cancel_flag = Arc::new(AtomicBool::new(false));

        let intellisense_data = Rc::new(RefCell::new(IntellisenseData::new()));
        let intellisense_popup = Rc::new(RefCell::new(IntellisensePopup::new()));
        let highlighter = Rc::new(RefCell::new(SqlHighlighter::new()));
        let status_callback: Rc<RefCell<Option<Box<dyn FnMut(&str)>>>> =
            Rc::new(RefCell::new(None));
        let find_callback: Rc<RefCell<Option<Box<dyn FnMut()>>>> = Rc::new(RefCell::new(None));
        let replace_callback: Rc<RefCell<Option<Box<dyn FnMut()>>>> = Rc::new(RefCell::new(None));
        let file_drop_callback: Rc<RefCell<Option<Box<dyn FnMut(PathBuf)>>>> =
            Rc::new(RefCell::new(None));
        let completion_range = Rc::new(RefCell::new(None::<(usize, usize)>));
        let pending_intellisense = Rc::new(RefCell::new(None::<PendingIntellisense>));
        let history_cursor = Rc::new(RefCell::new(None::<usize>));
        let history_original = Rc::new(RefCell::new(None::<String>));
        let history_navigation_entries = Rc::new(RefCell::new(None::<Vec<QueryHistoryEntry>>));
        let applying_history_navigation = Rc::new(RefCell::new(false));
        let undo_redo_state = Rc::new(RefCell::new(WordUndoRedoState::new(String::new())));

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
            history_cursor,
            history_original,
            history_navigation_entries,
            applying_history_navigation,
            undo_redo_state,
        };

        widget.setup_intellisense();
        widget.setup_word_undo_redo();
        widget.setup_syntax_highlighting();
        widget.setup_progress_handler(progress_receiver, progress_callback, query_running);
        widget.setup_column_loader(column_receiver);
        widget.setup_ui_action_handler(ui_action_receiver);

        widget
    }

    fn setup_word_undo_redo(&self) {
        let undo_state = self.undo_redo_state.clone();
        let applying_history_navigation = self.applying_history_navigation.clone();
        let mut buffer = self.buffer.clone();
        buffer.add_modify_callback2(move |buf, pos, ins, del, _restyled, deleted_text| {
            let inserted = inserted_text(buf, pos, ins);
            let current_text = buf.text();
            let mut state = undo_state.borrow_mut();

            if state.applying_history || *applying_history_navigation.borrow() {
                return;
            }

            state.normalize(&current_text);
            if state.current_snapshot_matches(&current_text) {
                return;
            }

            let granularity = classify_edit_granularity(ins, del, &inserted, deleted_text);

            let current_index = state.index;
            if current_index + 1 < state.history.len() {
                state.history.truncate(current_index + 1);
            }

            if state.active_group == Some(granularity) {
                if let Some(snapshot) = state.history.get_mut(current_index) {
                    *snapshot = current_text;
                } else {
                    state.history.push(current_text);
                    state.index = state.history.len().saturating_sub(1);
                    state.active_group = Some(granularity);
                }
            } else {
                state.history.push(current_text);
                if state.history.len() > MAX_WORD_UNDO_HISTORY {
                    state.history.remove(0);
                    state.index = state.history.len().saturating_sub(1);
                } else {
                    state.index = state.history.len().saturating_sub(1);
                }
                state.active_group = Some(granularity);
            }
        });
    }

    fn setup_progress_handler(
        &self,
        progress_receiver: mpsc::Receiver<QueryProgress>,
        progress_callback: Rc<RefCell<Option<Box<dyn FnMut(QueryProgress)>>>>,
        query_running: Rc<RefCell<bool>>,
    ) {
        let execute_callback = self.execute_callback.clone();
        let cancel_flag = self.cancel_flag.clone();
        let lifecycle_group = self.group.clone();

        // Wrap receiver in Rc<RefCell> to share across timeout callbacks
        let receiver: Rc<RefCell<mpsc::Receiver<QueryProgress>>> =
            Rc::new(RefCell::new(progress_receiver));

        fn schedule_poll(
            receiver: Rc<RefCell<mpsc::Receiver<QueryProgress>>>,
            progress_callback: Rc<RefCell<Option<Box<dyn FnMut(QueryProgress)>>>>,
            query_running: Rc<RefCell<bool>>,
            execute_callback: Rc<RefCell<Option<Box<dyn FnMut(&QueryResult)>>>>,
            cancel_flag: Arc<AtomicBool>,
            lifecycle_group: Flex,
        ) {
            if lifecycle_group.was_deleted() {
                return;
            }

            let mut disconnected = false;
            let mut processed = 0usize;
            let mut hit_budget = false;
            let mut pending_rows: Vec<(usize, Vec<Vec<String>>)> = Vec::new();

            let flush_rows = |pending_rows: &mut Vec<(usize, Vec<Vec<String>>)>,
                              cancelled: bool| {
                if pending_rows.is_empty() {
                    return;
                }
                if cancelled {
                    // Cancel 상태이면 버퍼링된 Row 메시지를 모두 버림
                    pending_rows.clear();
                    return;
                }
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
                    let r = receiver.borrow();
                    r.try_recv()
                };

                match message {
                    Ok(message) => {
                        processed += 1;
                        let cancelled = cancel_flag.load(Ordering::Relaxed);

                        match &message {
                            QueryProgress::Rows { .. } => {
                                if cancelled {
                                    // Cancel 상태이면 Row 메시지를 즉시 버림
                                    continue;
                                }
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
                                flush_rows(&mut pending_rows, cancelled);
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
                                flush_rows(&mut pending_rows, cancelled);
                                if *timed_out {
                                    fltk::dialog::alert_default(&format!(
                                        "Query timed out!\n\n{}",
                                        result.message
                                    ));
                                }
                                QueryHistoryDialog::add_to_history(
                                    &result.sql,
                                    result.execution_time.as_millis() as u64,
                                    result.row_count,
                                    connection_name,
                                    result.success,
                                    &result.message,
                                );
                                SqlEditorWidget::invoke_query_result_callback(
                                    &execute_callback,
                                    result,
                                );
                            }
                            QueryProgress::BatchFinished => {
                                flush_rows(&mut pending_rows, cancelled);
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

            flush_rows(&mut pending_rows, cancel_flag.load(Ordering::Relaxed));

            if disconnected {
                // Fail-safe cleanup: if the worker thread exits unexpectedly and the
                // channel closes before BatchFinished arrives, make sure execution
                // state/cursor do not stay stuck as "running".
                SqlEditorWidget::finalize_execution_state(&query_running, &cancel_flag);
                set_cursor(Cursor::Default);
                app::flush();
                return;
            }

            // Reschedule for next poll: if we processed messages, poll again immediately
            // to keep the UI responsive for streaming rows.
            let delay = if hit_budget || processed > 0 {
                0.0
            } else {
                PROGRESS_POLL_INTERVAL_SECONDS
            };
            app::add_timeout3(delay, move |_| {
                schedule_poll(
                    Rc::clone(&receiver),
                    Rc::clone(&progress_callback),
                    Rc::clone(&query_running),
                    Rc::clone(&execute_callback),
                    Arc::clone(&cancel_flag),
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

    fn finalize_execution_state(query_running: &Rc<RefCell<bool>>, cancel_flag: &Arc<AtomicBool>) {
        *query_running.borrow_mut() = false;
        cancel_flag.store(false, Ordering::SeqCst);
    }

    fn setup_column_loader(&self, column_receiver: mpsc::Receiver<ColumnLoadUpdate>) {
        let intellisense_data = self.intellisense_data.clone();
        let editor = self.editor.clone();
        let buffer = self.buffer.clone();
        let style_buffer = self.style_buffer.clone();
        let highlighter = self.highlighter.clone();
        let intellisense_popup = self.intellisense_popup.clone();
        let completion_range = self.completion_range.clone();
        let column_sender = self.column_sender.clone();
        let connection = self.connection.clone();
        let pending_intellisense = self.pending_intellisense.clone();

        // Wrap receiver in Rc<RefCell> to share across timeout callbacks
        let receiver: Rc<RefCell<mpsc::Receiver<ColumnLoadUpdate>>> =
            Rc::new(RefCell::new(column_receiver));

        const COLUMN_POLL_ACTIVE_INTERVAL_SECONDS: f64 = 0.05;
        const COLUMN_POLL_IDLE_INTERVAL_SECONDS: f64 = 0.5;
        const COLUMN_LOADING_STALE_TIMEOUT: Duration = Duration::from_secs(8);

        fn schedule_poll(
            receiver: Rc<RefCell<mpsc::Receiver<ColumnLoadUpdate>>>,
            intellisense_data: Rc<RefCell<IntellisenseData>>,
            editor: TextEditor,
            buffer: TextBuffer,
            style_buffer: TextBuffer,
            highlighter: Rc<RefCell<SqlHighlighter>>,
            intellisense_popup: Rc<RefCell<IntellisensePopup>>,
            completion_range: Rc<RefCell<Option<(usize, usize)>>>,
            column_sender: mpsc::Sender<ColumnLoadUpdate>,
            connection: SharedConnection,
            pending_intellisense: Rc<RefCell<Option<PendingIntellisense>>>,
        ) {
            if editor.was_deleted() {
                return;
            }

            let mut disconnected = false;
            let mut processed = 0usize;
            // Process any pending messages
            {
                let r = receiver.borrow();
                loop {
                    match r.try_recv() {
                        Ok(update) => {
                            processed += 1;
                            let (should_refresh_pending, should_clear_pending, highlight_columns) = {
                                let mut data = intellisense_data.borrow_mut();
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

                            if let Some(highlight_columns) = highlight_columns {
                                let should_refresh_highlighting = {
                                    let mut highlighter = highlighter.borrow_mut();
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
                                    let cursor_pos = editor.insert_position().max(0) as usize;
                                    highlighter.borrow().highlight_buffer_window(
                                        &buffer,
                                        &mut style_buffer.clone(),
                                        cursor_pos,
                                        None,
                                    );
                                }
                            }

                            if should_refresh_pending {
                                let pending = pending_intellisense.borrow().clone();
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
                                        );
                                    } else {
                                        // Cursor moved since async load was requested.
                                        // Drop stale pending state so poll loop can idle.
                                        *pending_intellisense.borrow_mut() = None;
                                    }
                                }
                            }

                            if should_clear_pending {
                                *pending_intellisense.borrow_mut() = None;
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
                return;
            }

            let stale_cleared = {
                let mut data = intellisense_data.borrow_mut();
                data.clear_stale_columns_loading(COLUMN_LOADING_STALE_TIMEOUT)
            };
            if stale_cleared > 0 {
                processed += stale_cleared;
                let no_columns_loading = {
                    let data = intellisense_data.borrow();
                    data.columns_loading.is_empty()
                };
                if no_columns_loading {
                    *pending_intellisense.borrow_mut() = None;
                }
            }

            let has_pending_column_work = {
                let data = intellisense_data.borrow();
                !data.columns_loading.is_empty()
            } || pending_intellisense.borrow().is_some();

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
                    Rc::clone(&receiver),
                    Rc::clone(&intellisense_data),
                    editor.clone(),
                    buffer.clone(),
                    style_buffer.clone(),
                    Rc::clone(&highlighter),
                    Rc::clone(&intellisense_popup),
                    Rc::clone(&completion_range),
                    column_sender.clone(),
                    connection.clone(),
                    Rc::clone(&pending_intellisense),
                );
            });
        }

        // Start polling
        schedule_poll(
            receiver,
            intellisense_data,
            editor,
            buffer,
            style_buffer,
            highlighter,
            intellisense_popup,
            completion_range,
            column_sender,
            connection,
            pending_intellisense,
        );
    }

    fn setup_ui_action_handler(&self, ui_action_receiver: mpsc::Receiver<UiActionResult>) {
        let widget = self.clone();

        let receiver: Rc<RefCell<mpsc::Receiver<UiActionResult>>> =
            Rc::new(RefCell::new(ui_action_receiver));

        fn schedule_poll(
            receiver: Rc<RefCell<mpsc::Receiver<UiActionResult>>>,
            widget: SqlEditorWidget,
        ) {
            if widget.group.was_deleted() {
                return;
            }

            let mut disconnected = false;
            loop {
                let message = {
                    let r = receiver.borrow();
                    r.try_recv()
                };

                match message {
                    Ok(action) => {
                        let should_reset_cursor = !matches!(&action, UiActionResult::Cancel(_));
                        match action {
                            UiActionResult::ExplainPlan(result) => match result {
                                Ok(plan_lines) => {
                                    let plan_text = if plan_lines.is_empty() {
                                        "No plan output.".to_string()
                                    } else {
                                        plan_lines.join("\n")
                                    };
                                    SqlEditorWidget::show_plan_dialog(&plan_text);
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
                                        fltk::dialog::alert_default("Not connected to database");
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
                            UiActionResult::QueryAlreadyRunning => {
                                let busy_message = crate::db::format_connection_busy_message();
                                widget.emit_status(&busy_message);
                                fltk::dialog::message_default(&busy_message);
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
                schedule_poll(Rc::clone(&receiver), widget.clone());
            });
        }

        schedule_poll(receiver, widget);
    }

    fn setup_syntax_highlighting(&self) {
        let highlighter = self.highlighter.clone();
        let mut style_buffer = self.style_buffer.clone();
        let mut buffer = self.buffer.clone();
        let editor = self.editor.clone();
        buffer.add_modify_callback2(move |buf, pos, ins, del, _restyled, deleted_text| {
            // Synchronize style_buffer length with text buffer
            // highlight_buffer_window will reset if lengths differ, but we do incremental
            // updates here to maintain consistency for small edits
            let text_len = buf.length();
            let style_len = style_buffer.length();

            if del > 0 && ins == 0 {
                // Pure deletion
                if pos >= 0 && pos < style_len {
                    let del_end = (pos + del).min(style_len);
                    if pos < del_end {
                        style_buffer.remove(pos, del_end);
                    }
                }
            } else if ins > 0 && del == 0 {
                // Pure insertion
                if pos >= 0 {
                    let insert_pos = pos.min(style_buffer.length());
                    let insert_styles: String = std::iter::repeat(STYLE_DEFAULT)
                        .take(ins as usize)
                        .collect();
                    style_buffer.insert(insert_pos, &insert_styles);
                }
            } else if ins > 0 && del > 0 {
                // Replacement: remove then insert
                if pos >= 0 && pos < style_len {
                    let del_end = (pos + del).min(style_len);
                    if pos < del_end {
                        style_buffer.remove(pos, del_end);
                    }
                }
                if pos >= 0 {
                    let insert_pos = pos.min(style_buffer.length());
                    let insert_styles: String = std::iter::repeat(STYLE_DEFAULT)
                        .take(ins as usize)
                        .collect();
                    style_buffer.insert(insert_pos, &insert_styles);
                }
            }

            // Final length check - if still mismatched, let highlight_buffer_window handle it
            // This provides a safety net for edge cases
            let final_style_len = style_buffer.length();
            if final_style_len != text_len {
                // Length mismatch detected - highlight_buffer_window will reset
                // This can happen with complex multi-byte character operations
            }

            let cursor_pos = editor.insert_position().max(0) as usize;
            let text_len = buf.length().max(0) as usize;
            let mut edited_range = compute_edited_range(pos, ins, del, text_len);

            if needs_full_rehighlight(buf, pos, ins, deleted_text) {
                edited_range = Some((0, text_len));
            } else if let Some((start, end)) = edited_range {
                let inserted_text = inserted_text(buf, pos, ins);
                if !has_stateful_sql_delimiter(&inserted_text) {
                    edited_range = Some(expand_connected_word_range(buf, start, end));
                }
            }

            highlighter.borrow().highlight_buffer_window(
                buf,
                &mut style_buffer,
                cursor_pos,
                edited_range,
            );
        });
        self.refresh_highlighting();
    }

    pub fn explain_current(&self) {
        let Some(sql) = self.statement_at_cursor_text() else {
            fltk::dialog::alert_default("No SQL at cursor");
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

    fn show_plan_dialog(plan_text: &str) {
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

        let mut buffer = fltk::text::TextBuffer::default();
        buffer.set_text(plan_text);
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
        if !*self.query_running.borrow() {
            fltk::dialog::alert_default("No query is running");
            return;
        }

        // Set cancel flag immediately so the execution thread can check it
        self.cancel_flag.store(true, Ordering::SeqCst);

        let current_query_connection = self.current_query_connection.clone();
        let sender = self.ui_action_sender.clone();
        thread::spawn(move || {
            // Use separate connection path for cancel (no blocking on main mutex)
            let conn = SqlEditorWidget::clone_current_query_connection(&current_query_connection);

            let result = if let Some(db_conn) = conn {
                db_conn.break_execution().map_err(|err| err.to_string())
            } else {
                Err("No active query connection".to_string())
            };

            let _ = sender.send(UiActionResult::Cancel(result));
            app::awake();
        });
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
        *self.execute_callback.borrow_mut() = Some(Box::new(callback));
    }

    pub fn set_status_callback<F>(&mut self, callback: F)
    where
        F: FnMut(&str) + 'static,
    {
        *self.status_callback.borrow_mut() = Some(Box::new(callback));
    }

    pub fn set_find_callback<F>(&mut self, callback: F)
    where
        F: FnMut() + 'static,
    {
        *self.find_callback.borrow_mut() = Some(Box::new(callback));
    }

    pub fn set_replace_callback<F>(&mut self, callback: F)
    where
        F: FnMut() + 'static,
    {
        *self.replace_callback.borrow_mut() = Some(Box::new(callback));
    }

    pub fn set_file_drop_callback<F>(&mut self, callback: F)
    where
        F: FnMut(PathBuf) + 'static,
    {
        *self.file_drop_callback.borrow_mut() = Some(Box::new(callback));
    }

    /// Releases callback/data references so a closing tab can be dropped promptly.
    pub fn cleanup_for_close(&mut self) {
        Self::finalize_execution_state(&self.query_running, &self.cancel_flag);
        Self::set_current_query_connection(&self.current_query_connection, None);

        *self.execute_callback.borrow_mut() = None;
        *self.progress_callback.borrow_mut() = None;
        *self.status_callback.borrow_mut() = None;
        *self.find_callback.borrow_mut() = None;
        *self.replace_callback.borrow_mut() = None;
        *self.file_drop_callback.borrow_mut() = None;

        self.intellisense_popup.borrow_mut().clear_for_close();
        *self.intellisense_data.borrow_mut() = IntellisenseData::new();
        self.highlighter
            .borrow_mut()
            .set_highlight_data(HighlightData::new());

        self.buffer.set_text("");
        self.style_buffer.set_text("");
        self.completion_range.borrow_mut().take();
        self.pending_intellisense.borrow_mut().take();
        self.history_cursor.borrow_mut().take();
        self.history_original.borrow_mut().take();
        self.history_navigation_entries.borrow_mut().take();
        *self.applying_history_navigation.borrow_mut() = false;
        Self::reset_word_undo_state(&self.undo_redo_state);
    }

    fn reset_word_undo_state(undo_redo_state: &Rc<RefCell<WordUndoRedoState>>) {
        let mut state = undo_redo_state.borrow_mut();
        let mut fresh_history = Vec::with_capacity(1);
        fresh_history.push(String::new());
        state.history = fresh_history;
        state.index = 0;
        state.active_group = None;
        state.applying_history = false;
    }

    #[allow(dead_code)]
    pub fn update_highlight_data(&mut self, data: HighlightData) {
        self.highlighter.borrow_mut().set_highlight_data(data);
        // Re-highlight current text
        let mut style_buffer = self.style_buffer.clone();
        self.highlighter.borrow().highlight_buffer_window(
            &self.buffer,
            &mut style_buffer,
            self.editor.insert_position().max(0) as usize,
            None,
        );
    }

    pub fn get_highlighter(&self) -> Rc<RefCell<SqlHighlighter>> {
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

    #[allow(dead_code)]
    pub fn refresh_highlighting(&self) {
        self.highlighter.borrow().highlight_buffer_window(
            &self.buffer,
            &mut self.style_buffer.clone(),
            self.editor.insert_position().max(0) as usize,
            None,
        );
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
        {
            let mut state = self.undo_redo_state.borrow_mut();
            state.history.clear();
            state.history.push(current_text);
            state.index = 0;
            state.active_group = None;
            state.applying_history = false;
        }
        *self.history_cursor.borrow_mut() = None;
        *self.history_original.borrow_mut() = None;
        self.history_navigation_entries.borrow_mut().take();
        *self.applying_history_navigation.borrow_mut() = false;
    }

    pub fn undo(&self) {
        let next_text = {
            let mut state = self.undo_redo_state.borrow_mut();
            state.normalize(&self.buffer.text());
            if state.index == 0 {
                return;
            }

            let next_index = state.index.saturating_sub(1);
            let Some(snapshot) = state.history.get(next_index).cloned() else {
                state.index = state.history.len().saturating_sub(1);
                state.active_group = None;
                return;
            };

            state.index = next_index;
            state.active_group = None;
            state.applying_history = true;
            snapshot
        };

        let mut buffer = self.buffer.clone();
        buffer.set_text(&next_text);
        self.refresh_highlighting();
        let mut editor = self.editor.clone();
        editor.set_insert_position(next_text.len() as i32);
        editor.show_insert_position();

        self.undo_redo_state.borrow_mut().applying_history = false;
    }

    pub fn redo(&self) {
        let next_text = {
            let mut state = self.undo_redo_state.borrow_mut();
            state.normalize(&self.buffer.text());
            let next_index = state.index.saturating_add(1);
            if next_index >= state.history.len() {
                return;
            }

            let Some(snapshot) = state.history.get(next_index).cloned() else {
                state.index = state.history.len().saturating_sub(1);
                state.active_group = None;
                return;
            };

            state.index = next_index;
            state.active_group = None;
            state.applying_history = true;
            snapshot
        };

        let mut buffer = self.buffer.clone();
        buffer.set_text(&next_text);
        self.refresh_highlighting();
        let mut editor = self.editor.clone();
        editor.set_insert_position(next_text.len() as i32);
        editor.show_insert_position();

        self.undo_redo_state.borrow_mut().applying_history = false;
    }

    pub fn is_query_running(&self) -> bool {
        *self.query_running.borrow()
    }

    pub fn navigate_history(&mut self, direction: i32) {
        let mut cursor = self.history_cursor.borrow_mut();
        let mut original = self.history_original.borrow_mut();
        let mut history_entries = self.history_navigation_entries.borrow_mut();
        let mut applying_navigation = self.applying_history_navigation.borrow_mut();

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

        let next_index = match *cursor {
            None => {
                if direction > 0 {
                    Some(0)
                } else {
                    return;
                }
            }
            Some(index) => {
                if direction > 0 {
                    Some(index.saturating_add(1))
                } else if index == 0 {
                    if let Some(saved) = original.take() {
                        *applying_navigation = true;
                        self.buffer.set_text(&saved);
                        *applying_navigation = false;
                        self.refresh_highlighting();
                        self.editor.set_insert_position(saved.len() as i32);
                        self.editor.show_insert_position();
                    }
                    *cursor = None;
                    history_entries.take();
                    return;
                } else {
                    Some(index.saturating_sub(1))
                }
            }
        };

        let Some(next_index) = next_index else {
            return;
        };

        if next_index >= entries.len() {
            return;
        }

        *cursor = Some(next_index);
        let sql = &entries[next_index].sql;
        *applying_navigation = true;
        self.buffer.set_text(sql);
        *applying_navigation = false;
        self.refresh_highlighting();
        self.editor.set_insert_position(sql.len() as i32);
        self.editor.show_insert_position();
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

fn is_word_edit_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_'
}


fn collect_highlight_columns_from_intellisense(data: &IntellisenseData) -> Vec<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut columns = Vec::new();
    for names in data.columns.values() {
        for name in names {
            let upper = name.to_uppercase();
            if seen.insert(upper) {
                columns.push(name.clone());
            }
        }
    }
    columns
}

fn expand_connected_word_range(buf: &TextBuffer, start: usize, end: usize) -> (usize, usize) {
    let text = buf.text();
    let bytes = text.as_bytes();
    let mut expanded_start = start.min(bytes.len());
    let mut expanded_end = end.min(bytes.len());

    while expanded_start > 0 && crate::sql_text::is_identifier_byte(bytes[expanded_start - 1]) {
        expanded_start -= 1;
    }

    while expanded_end < bytes.len() && crate::sql_text::is_identifier_byte(bytes[expanded_end])
    {
        expanded_end += 1;
    }

    (expanded_start, expanded_end)
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
    use super::{SqlEditorWidget, WordUndoRedoState};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn finalize_execution_state_clears_running_and_cancel_flags() {
        let query_running = Rc::new(RefCell::new(true));
        let cancel_flag = Arc::new(AtomicBool::new(true));

        SqlEditorWidget::finalize_execution_state(&query_running, &cancel_flag);

        assert!(!*query_running.borrow());
        assert!(!cancel_flag.load(Ordering::SeqCst));
    }

    #[test]
    fn reset_word_undo_state_reinitializes_history_safely() {
        let undo_state = Rc::new(RefCell::new(WordUndoRedoState {
            history: vec!["SELECT 1".to_string(), "SELECT 2".to_string()],
            index: 1,
            active_group: None,
            applying_history: true,
        }));

        SqlEditorWidget::reset_word_undo_state(&undo_state);

        let state = undo_state.borrow();
        assert_eq!(state.history, vec![String::new()]);
        assert_eq!(state.index, 0);
        assert!(state.active_group.is_none());
        assert!(!state.applying_history);
    }
    #[test]
    fn finalize_execution_state_is_idempotent_when_already_reset() {
        let query_running = Rc::new(RefCell::new(false));
        let cancel_flag = Arc::new(AtomicBool::new(false));

        SqlEditorWidget::finalize_execution_state(&query_running, &cancel_flag);

        assert!(!*query_running.borrow());
        assert!(!cancel_flag.load(Ordering::SeqCst));
    }
}

#[cfg(test)]
mod sql_editor_tests;
