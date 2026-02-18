use fltk::{
    app,
    draw::set_cursor,
    enums::{Cursor, Event, Key},
    prelude::*,
    text::{PositionType, TextBuffer, TextEditor},
};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::panic::{self, AssertUnwindSafe};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, OnceLock};
use std::thread;
use std::time::Duration;

use oracle::Connection;

use crate::db::{
    ObjectBrowser, ProcedureArgument, SequenceInfo, SharedConnection, TableColumnDetail,
};
use crate::sql_text;
use crate::ui::intellisense::{
    detect_sql_context, get_word_at_cursor, IntellisenseData, IntellisensePopup, SqlContext,
};
use crate::ui::intellisense_context;
use crate::ui::FindReplaceDialog;

use super::*;

const MAX_MERGED_SUGGESTIONS: usize = 50;
const KEYUP_INTELLISENSE_DEBOUNCE_MS: u64 = 120;
const COLUMN_LOAD_WORKER_COUNT: usize = 4;

struct ColumnLoadTask {
    table_key: String,
    connection: SharedConnection,
    sender: mpsc::Sender<ColumnLoadUpdate>,
}

struct ColumnLoadWorkerPool {
    worker_senders: Vec<mpsc::Sender<ColumnLoadTask>>,
    next_worker: AtomicUsize,
}

impl ColumnLoadWorkerPool {
    fn enqueue(&self, task: ColumnLoadTask) -> Result<(), ColumnLoadTask> {
        if self.worker_senders.is_empty() {
            return Err(task);
        }
        let index = self.next_worker.fetch_add(1, Ordering::Relaxed) % self.worker_senders.len();
        self.worker_senders[index].send(task).map_err(|err| err.0)
    }
}

static COLUMN_LOAD_WORKER_POOL: OnceLock<ColumnLoadWorkerPool> = OnceLock::new();

impl SqlEditorWidget {
    const COLUMN_LOAD_LOCK_RETRY_ATTEMPTS: usize = 5;
    const COLUMN_LOAD_LOCK_RETRY_DELAY_MS: u64 = 60;
    const INTELLISENSE_POPUP_WIDTH: i32 = 320;
    const INTELLISENSE_POPUP_Y_OFFSET: i32 = 20;

    fn column_load_worker_pool() -> &'static ColumnLoadWorkerPool {
        COLUMN_LOAD_WORKER_POOL.get_or_init(Self::build_column_load_worker_pool)
    }

    fn build_column_load_worker_pool() -> ColumnLoadWorkerPool {
        let mut worker_senders = Vec::new();

        for idx in 0..COLUMN_LOAD_WORKER_COUNT {
            let (sender, receiver) = mpsc::channel::<ColumnLoadTask>();
            let spawn_result = thread::Builder::new()
                .name(format!("intellisense-column-worker-{idx}"))
                .spawn(move || {
                    while let Ok(task) = receiver.recv() {
                        Self::process_column_load_task(task);
                    }
                });

            if spawn_result.is_ok() {
                worker_senders.push(sender);
            } else if let Err(err) = spawn_result {
                crate::utils::logging::log_error(
                    "sql_editor::intellisense::column_loader",
                    &format!("failed to spawn column worker {idx}: {err}"),
                );
            }
        }

        ColumnLoadWorkerPool {
            worker_senders,
            next_worker: AtomicUsize::new(0),
        }
    }

    fn enqueue_column_load_task(task: ColumnLoadTask) -> Result<(), ColumnLoadTask> {
        Self::column_load_worker_pool().enqueue(task)
    }

    fn process_column_load_task(task: ColumnLoadTask) {
        let ColumnLoadTask {
            table_key,
            connection,
            sender,
        } = task;

        // Try-lock with bounded retries to avoid deadlock while still giving
        // background column loading a chance when the connection is briefly busy.
        let mut conn_guard = None;
        for attempt in 0..Self::COLUMN_LOAD_LOCK_RETRY_ATTEMPTS {
            if let Some(guard) = crate::db::try_lock_connection_with_activity(
                &connection,
                format!("Loading columns for {}", table_key),
            ) {
                conn_guard = Some(guard);
                break;
            }
            if attempt + 1 < Self::COLUMN_LOAD_LOCK_RETRY_ATTEMPTS {
                thread::sleep(Duration::from_millis(Self::COLUMN_LOAD_LOCK_RETRY_DELAY_MS));
            }
        }

        let Some(mut conn_guard) = conn_guard else {
            let _ = sender.send(ColumnLoadUpdate {
                table: table_key,
                columns: Vec::new(),
                cache_columns: false,
            });
            app::awake();
            return;
        };

        let (columns, cache_columns) = match conn_guard.require_live_connection() {
            Ok(conn) => {
                match crate::db::ObjectBrowser::get_table_columns(conn.as_ref(), &table_key) {
                    Ok(cols) => (cols.into_iter().map(|col| col.name).collect(), true),
                    Err(_) => (Vec::new(), false),
                }
            }
            Err(_) => (Vec::new(), false),
        };

        let _ = sender.send(ColumnLoadUpdate {
            table: table_key,
            columns,
            cache_columns,
        });
        app::awake();
    }

    fn invoke_void_callback(callback_slot: &Rc<RefCell<Option<Box<dyn FnMut()>>>>) -> bool {
        let callback = {
            let mut slot = callback_slot.borrow_mut();
            slot.take()
        };

        if let Some(mut cb) = callback {
            let result = panic::catch_unwind(AssertUnwindSafe(|| cb()));
            let mut slot = callback_slot.borrow_mut();
            if slot.is_none() {
                *slot = Some(cb);
            }
            if let Err(payload) = result {
                Self::log_callback_panic("find/replace callback", payload.as_ref());
            }
            true
        } else {
            false
        }
    }

    fn invoke_file_drop_callback(
        callback_slot: &Rc<RefCell<Option<Box<dyn FnMut(PathBuf)>>>>,
        path: PathBuf,
    ) -> bool {
        let callback = {
            let mut slot = callback_slot.borrow_mut();
            slot.take()
        };

        if let Some(mut cb) = callback {
            let result = panic::catch_unwind(AssertUnwindSafe(|| cb(path)));
            let mut slot = callback_slot.borrow_mut();
            if slot.is_none() {
                *slot = Some(cb);
            }
            if let Err(payload) = result {
                Self::log_callback_panic("file drop callback", payload.as_ref());
            }
            true
        } else {
            false
        }
    }

    fn should_consume_popup_confirm_key(key: Key, has_selected: bool) -> bool {
        has_selected && matches!(key, Key::Tab | Key::Enter | Key::KPEnter)
    }

    fn cancel_keyup_debounce_timeout(
        keyup_debounce_handle: &Rc<RefCell<Option<app::TimeoutHandle>>>,
    ) {
        if let Some(handle) = keyup_debounce_handle.borrow_mut().take() {
            if app::has_timeout3(handle) {
                app::remove_timeout3(handle);
            }
        }
    }

    fn invalidate_keyup_debounce(
        keyup_debounce_generation: &Rc<Cell<u64>>,
        keyup_debounce_handle: &Rc<RefCell<Option<app::TimeoutHandle>>>,
    ) -> u64 {
        Self::cancel_keyup_debounce_timeout(keyup_debounce_handle);
        let generation = keyup_debounce_generation.get().wrapping_add(1);
        keyup_debounce_generation.set(generation);
        generation
    }

    #[allow(clippy::too_many_arguments)]
    fn schedule_keyup_intellisense_debounce(
        keyup_debounce_generation: &Rc<Cell<u64>>,
        keyup_debounce_handle: &Rc<RefCell<Option<app::TimeoutHandle>>>,
        scheduled_cursor_raw: i32,
        buffer_len: i32,
        editor: &TextEditor,
        buffer: &TextBuffer,
        intellisense_data: &Rc<RefCell<IntellisenseData>>,
        intellisense_popup: &Rc<RefCell<IntellisensePopup>>,
        completion_range: &Rc<RefCell<Option<(usize, usize)>>>,
        column_sender: &mpsc::Sender<ColumnLoadUpdate>,
        connection: &SharedConnection,
        pending_intellisense: &Rc<RefCell<Option<PendingIntellisense>>>,
        intellisense_parse_cache: &Rc<RefCell<Option<IntellisenseParseCacheEntry>>>,
    ) {
        let generation =
            Self::invalidate_keyup_debounce(keyup_debounce_generation, keyup_debounce_handle);
        let keyup_debounce_generation_for_timeout = keyup_debounce_generation.clone();
        let keyup_debounce_handle_for_timeout = keyup_debounce_handle.clone();
        let editor_for_timeout = editor.clone();
        let buffer_for_timeout = buffer.clone();
        let intellisense_data_for_timeout = intellisense_data.clone();
        let intellisense_popup_for_timeout = intellisense_popup.clone();
        let completion_range_for_timeout = completion_range.clone();
        let column_sender_for_timeout = column_sender.clone();
        let connection_for_timeout = connection.clone();
        let pending_intellisense_for_timeout = pending_intellisense.clone();
        let intellisense_parse_cache_for_timeout = intellisense_parse_cache.clone();
        let handle = app::add_timeout3(
            Duration::from_millis(KEYUP_INTELLISENSE_DEBOUNCE_MS).as_secs_f64(),
            move |timeout_handle| {
                {
                    let mut slot = keyup_debounce_handle_for_timeout.borrow_mut();
                    if slot.as_ref().copied() == Some(timeout_handle) {
                        *slot = None;
                    }
                }

                if keyup_debounce_generation_for_timeout.get() != generation {
                    return;
                }

                if editor_for_timeout.was_deleted() {
                    return;
                }

                // Hot-path check: for debounce invalidation we only care whether the
                // cursor offset changed, not UTF-8 boundary normalization.
                if !Self::is_same_raw_cursor_offset(
                    editor_for_timeout.insert_position(),
                    scheduled_cursor_raw,
                ) {
                    return;
                }

                if buffer_for_timeout.length() != buffer_len {
                    return;
                }

                Self::trigger_intellisense(
                    &editor_for_timeout,
                    &buffer_for_timeout,
                    &intellisense_data_for_timeout,
                    &intellisense_popup_for_timeout,
                    &completion_range_for_timeout,
                    &column_sender_for_timeout,
                    &connection_for_timeout,
                    &pending_intellisense_for_timeout,
                    &intellisense_parse_cache_for_timeout,
                );
            },
        );
        *keyup_debounce_handle.borrow_mut() = Some(handle);
    }

    fn is_same_raw_cursor_offset(current_raw: i32, scheduled_raw: i32) -> bool {
        current_raw == scheduled_raw
    }

    pub fn setup_intellisense(&mut self) {
        let buffer = self.buffer.clone();
        let mut editor = self.editor.clone();
        let intellisense_data = self.intellisense_data.clone();
        let intellisense_popup = self.intellisense_popup.clone();
        let connection = self.connection.clone();
        let column_sender = self.column_sender.clone();
        let highlighter = self.highlighter.clone();
        let style_buffer = self.style_buffer.clone();
        let suppress_enter = Rc::new(RefCell::new(false));
        let suppress_nav = Rc::new(RefCell::new(false));
        let nav_anchor = Rc::new(RefCell::new(None::<i32>));
        let completion_range = self.completion_range.clone();
        let ctrl_enter_handled = Rc::new(RefCell::new(false));
        let pending_intellisense = self.pending_intellisense.clone();
        let intellisense_parse_cache = self.intellisense_parse_cache.clone();
        let keyup_debounce_generation = Rc::new(Cell::new(0_u64));
        let keyup_debounce_handle = Rc::new(RefCell::new(None::<app::TimeoutHandle>));

        // Setup callback for inserting selected text
        let mut buffer_for_insert = buffer.clone();
        let mut editor_for_insert = editor.clone();
        let completion_range_for_insert = completion_range.clone();
        let intellisense_data_for_insert = intellisense_data.clone();
        let column_sender_for_insert = column_sender.clone();
        let connection_for_insert = connection.clone();
        {
            let mut popup = intellisense_popup.borrow_mut();
            popup.set_selected_callback(move |selected| {
                let cursor_pos = Self::raw_cursor_position(
                    &buffer_for_insert,
                    editor_for_insert.insert_position(),
                );
                let cursor_pos_usize = cursor_pos as usize;
                let context_text = Self::normalize_intellisense_context_text(
                    &Self::context_before_cursor(&buffer_for_insert, cursor_pos),
                );
                let context = detect_sql_context(&context_text, context_text.len());
                if matches!(context, SqlContext::TableName) {
                    let should_prefetch = {
                        let data = intellisense_data_for_insert.borrow();
                        data.is_known_relation(&selected)
                    };
                    if should_prefetch {
                        Self::request_table_columns(
                            &selected,
                            &intellisense_data_for_insert,
                            &column_sender_for_insert,
                            &connection_for_insert,
                        );
                    }
                }
                let range = *completion_range_for_insert.borrow();
                let (start, end) = if let Some((range_start, range_end)) = range {
                    (range_start, range_end)
                } else {
                    let (word, start, _end) = Self::word_at_cursor(&buffer_for_insert, cursor_pos);
                    if word.is_empty() {
                        (cursor_pos_usize, cursor_pos_usize)
                    } else {
                        (start, cursor_pos_usize)
                    }
                };

                if start != end {
                    buffer_for_insert.replace(start as i32, end as i32, &selected);
                    editor_for_insert.set_insert_position((start + selected.len()) as i32);
                } else {
                    buffer_for_insert.insert(cursor_pos, &selected);
                    editor_for_insert
                        .set_insert_position((cursor_pos_usize + selected.len()) as i32);
                }
                *completion_range_for_insert.borrow_mut() = None;
            });
        }

        // Handle keyboard events for triggering intellisense and syntax highlighting
        let mut buffer_for_handle = buffer.clone();
        let intellisense_data_for_handle = intellisense_data.clone();
        let intellisense_popup_for_handle = intellisense_popup.clone();
        let column_sender_for_handle = column_sender.clone();
        let connection_for_handle = connection.clone();
        let highlighter_for_handle = highlighter.clone();
        let mut style_buffer_for_handle = style_buffer.clone();
        let suppress_enter_for_handle = suppress_enter.clone();
        let suppress_nav_for_handle = suppress_nav.clone();
        let nav_anchor_for_handle = nav_anchor.clone();
        let completion_range_for_handle = completion_range.clone();
        let mut widget_for_shortcuts = self.clone();
        let find_callback_for_handle = self.find_callback.clone();
        let replace_callback_for_handle = self.replace_callback.clone();
        let file_drop_callback_for_handle = self.file_drop_callback.clone();
        let ctrl_enter_handled_for_handle = ctrl_enter_handled.clone();
        let pending_intellisense_for_handle = pending_intellisense.clone();
        let intellisense_parse_cache_for_handle = intellisense_parse_cache.clone();
        let keyup_debounce_generation_for_handle = keyup_debounce_generation.clone();
        let keyup_debounce_handle_for_handle = keyup_debounce_handle.clone();
        let dnd_file_drop_pending_for_handle = Rc::new(RefCell::new(false));

        editor.handle(move |ed, ev| {
            match ev {
                Event::DndEnter | Event::DndDrag => {
                    *dnd_file_drop_pending_for_handle.borrow_mut() = true;
                    true
                }
                Event::DndLeave => {
                    *dnd_file_drop_pending_for_handle.borrow_mut() = false;
                    true
                }
                Event::DndRelease => {
                    *dnd_file_drop_pending_for_handle.borrow_mut() = true;
                    true
                }
                Event::Push => {
                    let state = fltk::app::event_state();
                    let ctrl_or_cmd = state.contains(fltk::enums::Shortcut::Ctrl)
                        || state.contains(fltk::enums::Shortcut::Command);
                    if ctrl_or_cmd && fltk::app::event_button() == 1 {
                        let pos = ed.xy_to_position(
                            fltk::app::event_x(),
                            fltk::app::event_y(),
                            PositionType::Cursor,
                        );
                        if pos >= 0 {
                            let pos = Self::raw_cursor_position(&buffer_for_handle, pos);
                            if let Some((_, start, end)) =
                                Self::identifier_at_position(&buffer_for_handle, pos)
                            {
                                buffer_for_handle.select(start, end);
                                ed.set_insert_position(end);
                            } else {
                                buffer_for_handle.unselect();
                                ed.set_insert_position(pos);
                            }
                            ed.show_insert_position();
                            widget_for_shortcuts.quick_describe_at_cursor();
                            return true;
                        }
                    }
                    false
                }
                Event::KeyDown => {
                    let key = fltk::app::event_key();
                    let popup_visible = intellisense_popup_for_handle.borrow().is_visible();
                    let state = fltk::app::event_state();
                    let ctrl_or_cmd = state.contains(fltk::enums::Shortcut::Ctrl)
                        || state.contains(fltk::enums::Shortcut::Command);
                    let shift = state.contains(fltk::enums::Shortcut::Shift);
                    let alt = state.contains(fltk::enums::Shortcut::Alt);

                    if ctrl_or_cmd && shift && matches!(key, Key::Up | Key::Down) {
                        if popup_visible {
                            intellisense_popup_for_handle.borrow_mut().hide();
                            *completion_range_for_handle.borrow_mut() = None;
                            *pending_intellisense_for_handle.borrow_mut() = None;
                            Self::invalidate_keyup_debounce(
                                &keyup_debounce_generation_for_handle,
                                &keyup_debounce_handle_for_handle,
                            );
                        }
                        let direction = if key == Key::Up { -1 } else { 1 };
                        widget_for_shortcuts.select_block_in_direction(direction);
                        return true;
                    }

                    if alt && matches!(key, Key::Up | Key::Down) {
                        if popup_visible {
                            intellisense_popup_for_handle.borrow_mut().hide();
                            *completion_range_for_handle.borrow_mut() = None;
                            *pending_intellisense_for_handle.borrow_mut() = None;
                            Self::invalidate_keyup_debounce(
                                &keyup_debounce_generation_for_handle,
                                &keyup_debounce_handle_for_handle,
                            );
                        }
                        let direction = if key == Key::Up { 1 } else { -1 };
                        widget_for_shortcuts.navigate_history(direction);
                        return true;
                    }

                    if popup_visible {
                        match key {
                            Key::Escape => {
                                // Close popup, consume event
                                intellisense_popup_for_handle.borrow_mut().hide();
                                *completion_range_for_handle.borrow_mut() = None;
                                *pending_intellisense_for_handle.borrow_mut() = None;
                                Self::invalidate_keyup_debounce(
                                    &keyup_debounce_generation_for_handle,
                                    &keyup_debounce_handle_for_handle,
                                );
                                return true;
                            }
                            Key::Up => {
                                // Navigate popup up, consume event
                                let pos = ed.insert_position();
                                *nav_anchor_for_handle.borrow_mut() = Some(pos);
                                intellisense_popup_for_handle.borrow_mut().select_prev();
                                ed.set_insert_position(pos);
                                ed.show_insert_position();
                                *suppress_nav_for_handle.borrow_mut() = true;
                                return true;
                            }
                            Key::Down => {
                                // Navigate popup down, consume event
                                let pos = ed.insert_position();
                                *nav_anchor_for_handle.borrow_mut() = Some(pos);
                                intellisense_popup_for_handle.borrow_mut().select_next();
                                ed.set_insert_position(pos);
                                ed.show_insert_position();
                                *suppress_nav_for_handle.borrow_mut() = true;
                                return true;
                            }
                            Key::Enter | Key::KPEnter | Key::Tab => {
                                // Insert selected suggestion, consume event
                                let selected =
                                    intellisense_popup_for_handle.borrow().get_selected();
                                let has_selected = selected.is_some();
                                if let Some(selected) = selected {
                                    let cursor_pos = Self::raw_cursor_position(
                                        &buffer_for_handle,
                                        ed.insert_position(),
                                    );
                                    let cursor_pos_usize = cursor_pos as usize;
                                    let range = *completion_range_for_handle.borrow();
                                    let (start, end) = if let Some((range_start, range_end)) = range
                                    {
                                        (range_start, range_end)
                                    } else {
                                        let (word, start, _end) =
                                            Self::word_at_cursor(&buffer_for_handle, cursor_pos);
                                        if word.is_empty() {
                                            (cursor_pos_usize, cursor_pos_usize)
                                        } else {
                                            (start, cursor_pos_usize)
                                        }
                                    };

                                    if start != end {
                                        buffer_for_handle.replace(
                                            start as i32,
                                            end as i32,
                                            &selected,
                                        );
                                        ed.set_insert_position((start + selected.len()) as i32);
                                    } else {
                                        buffer_for_handle.insert(cursor_pos, &selected);
                                        ed.set_insert_position(
                                            (cursor_pos_usize + selected.len()) as i32,
                                        );
                                    }
                                    *completion_range_for_handle.borrow_mut() = None;
                                    *pending_intellisense_for_handle.borrow_mut() = None;

                                    // Update syntax highlighting after insertion
                                    let cursor_pos = Self::raw_cursor_position(
                                        &buffer_for_handle,
                                        ed.insert_position(),
                                    ) as usize;
                                    highlighter_for_handle.borrow().highlight_buffer_window(
                                        &buffer_for_handle,
                                        &mut style_buffer_for_handle,
                                        cursor_pos,
                                        None,
                                    );
                                }
                                if matches!(key, Key::Enter | Key::KPEnter) {
                                    *suppress_enter_for_handle.borrow_mut() = true;
                                }
                                intellisense_popup_for_handle.borrow_mut().hide();
                                *pending_intellisense_for_handle.borrow_mut() = None;
                                Self::invalidate_keyup_debounce(
                                    &keyup_debounce_generation_for_handle,
                                    &keyup_debounce_handle_for_handle,
                                );
                                return Self::should_consume_popup_confirm_key(key, has_selected);
                            }
                            _ => {
                                // Let other keys pass through to editor
                            }
                        }
                    }

                    if !ed.active() || (!ed.has_focus() && !popup_visible) {
                        return false;
                    }
                    // KeyDown fires BEFORE the character is inserted into the buffer.
                    // Handle navigation and selection keys here to consume them
                    // before they affect the editor.

                    // Handle basic editing shortcuts
                    let ctrl_or_cmd = state.contains(fltk::enums::Shortcut::Ctrl)
                        || state.contains(fltk::enums::Shortcut::Command);
                    let shift = state.contains(fltk::enums::Shortcut::Shift);

                    if ctrl_or_cmd {
                        if shift && (key == Key::from_char('f') || key == Key::from_char('F')) {
                            widget_for_shortcuts.format_selected_sql();
                            return true;
                        }

                        if shift && (key == Key::from_char('z') || key == Key::from_char('Z')) {
                            widget_for_shortcuts.redo();
                            return true;
                        }

                        match key {
                            k if k == Key::from_char('z') || k == Key::from_char('Z') => {
                                widget_for_shortcuts.undo();
                                return true;
                            }
                            k if k == Key::from_char('y') || k == Key::from_char('Y') => {
                                widget_for_shortcuts.redo();
                                return true;
                            }
                            k if k == Key::from_char(' ') => {
                                // Ctrl+Space - Trigger intellisense
                                Self::trigger_intellisense(
                                    ed,
                                    &buffer_for_handle,
                                    &intellisense_data_for_handle,
                                    &intellisense_popup_for_handle,
                                    &completion_range_for_handle,
                                    &column_sender_for_handle,
                                    &connection_for_handle,
                                    &pending_intellisense_for_handle,
                                    &intellisense_parse_cache_for_handle,
                                );
                                return true;
                            }
                            Key::Enter | Key::KPEnter => {
                                if *ctrl_enter_handled_for_handle.borrow() {
                                    return true;
                                }
                                *ctrl_enter_handled_for_handle.borrow_mut() = true;
                                widget_for_shortcuts.execute_statement_at_cursor();
                                return true;
                            }
                            k if k == Key::from_char('f') || k == Key::from_char('F') => {
                                Self::invoke_void_callback(&find_callback_for_handle);
                                return true;
                            }
                            k if k == Key::from_char('/') || k == Key::from_char('?') => {
                                widget_for_shortcuts.toggle_comment();
                                return true;
                            }
                            k if k == Key::from_char('u') || k == Key::from_char('U') => {
                                widget_for_shortcuts.convert_selection_case(true);
                                return true;
                            }
                            k if k == Key::from_char('l') || k == Key::from_char('L') => {
                                widget_for_shortcuts.convert_selection_case(false);
                                return true;
                            }
                            k if k == Key::from_char('h') || k == Key::from_char('H') => {
                                Self::invoke_void_callback(&replace_callback_for_handle);
                                return true;
                            }
                            _ => {}
                        }
                    }

                    // F4 - Quick Describe (handle on KeyDown for immediate response)
                    if key == Key::F4 {
                        widget_for_shortcuts.quick_describe_at_cursor();
                        return true;
                    }

                    if key == Key::F3 {
                        let mut editor_for_find = ed.clone();
                        if !FindReplaceDialog::find_next_from_session(
                            &mut editor_for_find,
                            &mut buffer_for_handle,
                        ) && !FindReplaceDialog::has_search_text()
                        {
                            Self::invoke_void_callback(&find_callback_for_handle);
                        }
                        return true;
                    }

                    if key == Key::F5 {
                        widget_for_shortcuts.execute_current();
                        return true;
                    }

                    if key == Key::F9 {
                        widget_for_shortcuts.execute_statement_at_cursor();
                        return true;
                    }

                    if key == Key::F6 {
                        widget_for_shortcuts.explain_current();
                        return true;
                    }

                    if key == Key::F7 {
                        widget_for_shortcuts.commit();
                        return true;
                    }

                    if key == Key::F8 {
                        widget_for_shortcuts.rollback();
                        return true;
                    }

                    false
                }
                Event::KeyUp => {
                    let popup_visible = intellisense_popup_for_handle.borrow().is_visible();
                    if !ed.active() || (!ed.has_focus() && !popup_visible) {
                        return false;
                    }
                    // KeyUp fires AFTER the character is inserted into the buffer.
                    // Filter/show intellisense here.
                    let key = fltk::app::event_key();
                    let event_text = fltk::app::event_text();
                    let state = fltk::app::event_state();
                    let ctrl_or_cmd = state.contains(fltk::enums::Shortcut::Ctrl)
                        || state.contains(fltk::enums::Shortcut::Command);
                    let alt = state.contains(fltk::enums::Shortcut::Alt);
                    let shift = state.contains(fltk::enums::Shortcut::Shift);
                    // Keep KeyUp lightweight by using raw offsets (no full-buffer clones).
                    let cursor_pos = ed.insert_position();
                    let char_before_cursor =
                        Self::char_before_cursor(&buffer_for_handle, cursor_pos);
                    let typed_char = Self::typed_char_from_key_event(
                        &event_text,
                        key,
                        shift,
                        char_before_cursor,
                    );
                    if Self::is_modifier_key(key) {
                        return false;
                    }

                    if event_text.is_empty()
                        && typed_char.is_none()
                        && !ctrl_or_cmd
                        && !alt
                        && !matches!(
                            key,
                            Key::BackSpace
                                | Key::Delete
                                | Key::Left
                                | Key::Right
                                | Key::Up
                                | Key::Down
                                | Key::Home
                                | Key::End
                                | Key::PageUp
                                | Key::PageDown
                                | Key::Enter
                                | Key::KPEnter
                                | Key::Tab
                                | Key::Escape
                        )
                    {
                        if popup_visible {
                            intellisense_popup_for_handle.borrow_mut().hide();
                            *completion_range_for_handle.borrow_mut() = None;
                            *pending_intellisense_for_handle.borrow_mut() = None;
                            Self::invalidate_keyup_debounce(
                                &keyup_debounce_generation_for_handle,
                                &keyup_debounce_handle_for_handle,
                            );
                        }
                        return false;
                    }

                    if matches!(key, Key::Up | Key::Down) && *suppress_nav_for_handle.borrow() {
                        if let Some(pos) = *nav_anchor_for_handle.borrow() {
                            ed.set_insert_position(pos);
                            ed.show_insert_position();
                        }
                        *nav_anchor_for_handle.borrow_mut() = None;
                        *suppress_nav_for_handle.borrow_mut() = false;
                        return true;
                    }

                    if matches!(key, Key::Enter | Key::KPEnter)
                        && *suppress_enter_for_handle.borrow()
                    {
                        *suppress_enter_for_handle.borrow_mut() = false;
                        return true;
                    }
                    if matches!(key, Key::Enter | Key::KPEnter)
                        && *ctrl_enter_handled_for_handle.borrow()
                    {
                        *ctrl_enter_handled_for_handle.borrow_mut() = false;
                        return true;
                    }

                    // Navigation keys - hide popup and let editor handle cursor movement
                    if matches!(
                        key,
                        Key::Left | Key::Right | Key::Home | Key::End | Key::PageUp | Key::PageDown
                    ) {
                        if popup_visible {
                            intellisense_popup_for_handle.borrow_mut().hide();
                            *completion_range_for_handle.borrow_mut() = None;
                            *pending_intellisense_for_handle.borrow_mut() = None;
                        }
                        Self::invalidate_keyup_debounce(
                            &keyup_debounce_generation_for_handle,
                            &keyup_debounce_handle_for_handle,
                        );
                        return false;
                    }

                    // Skip if these keys (already handled in KeyDown)
                    if popup_visible
                        && matches!(
                            key,
                            Key::Up
                                | Key::Down
                                | Key::Escape
                                | Key::Enter
                                | Key::KPEnter
                                | Key::Tab
                        )
                    {
                        return true;
                    }

                    // Handle typing - update intellisense filter
                    let (word, word_start, _) =
                        Self::word_at_cursor(&buffer_for_handle, cursor_pos);
                    let buffer_len = buffer_for_handle.length();

                    let fast_path_applied = if popup_visible {
                        Self::try_fast_path_intellisense_filter(
                            ed,
                            &buffer_for_handle,
                            &intellisense_popup_for_handle,
                            &completion_range_for_handle,
                            cursor_pos,
                            key,
                            typed_char,
                        )
                    } else {
                        false
                    };

                    if fast_path_applied {
                        *pending_intellisense_for_handle.borrow_mut() = None;
                        Self::invalidate_keyup_debounce(
                            &keyup_debounce_generation_for_handle,
                            &keyup_debounce_handle_for_handle,
                        );
                    } else if key == Key::BackSpace || key == Key::Delete {
                        // After backspace/delete, re-evaluate (debounced)
                        if Self::has_min_intellisense_prefix(&word) {
                            Self::schedule_keyup_intellisense_debounce(
                                &keyup_debounce_generation_for_handle,
                                &keyup_debounce_handle_for_handle,
                                cursor_pos,
                                buffer_len,
                                ed,
                                &buffer_for_handle,
                                &intellisense_data_for_handle,
                                &intellisense_popup_for_handle,
                                &completion_range_for_handle,
                                &column_sender_for_handle,
                                &connection_for_handle,
                                &pending_intellisense_for_handle,
                                &intellisense_parse_cache_for_handle,
                            );
                        } else {
                            intellisense_popup_for_handle.borrow_mut().hide();
                            *completion_range_for_handle.borrow_mut() = None;
                            *pending_intellisense_for_handle.borrow_mut() = None;
                            Self::invalidate_keyup_debounce(
                                &keyup_debounce_generation_for_handle,
                                &keyup_debounce_handle_for_handle,
                            );
                        }
                    } else if let Some(ch) = typed_char {
                        if Self::should_force_full_analysis(ch) {
                            let qualifier =
                                Self::qualifier_before_word(&buffer_for_handle, word_start);
                            if Self::should_auto_trigger_intellisense_for_forced_char(
                                &word,
                                qualifier.as_deref(),
                            ) {
                                Self::schedule_keyup_intellisense_debounce(
                                    &keyup_debounce_generation_for_handle,
                                    &keyup_debounce_handle_for_handle,
                                    cursor_pos,
                                    buffer_len,
                                    ed,
                                    &buffer_for_handle,
                                    &intellisense_data_for_handle,
                                    &intellisense_popup_for_handle,
                                    &completion_range_for_handle,
                                    &column_sender_for_handle,
                                    &connection_for_handle,
                                    &pending_intellisense_for_handle,
                                    &intellisense_parse_cache_for_handle,
                                );
                            } else {
                                intellisense_popup_for_handle.borrow_mut().hide();
                                *completion_range_for_handle.borrow_mut() = None;
                                *pending_intellisense_for_handle.borrow_mut() = None;
                                Self::invalidate_keyup_debounce(
                                    &keyup_debounce_generation_for_handle,
                                    &keyup_debounce_handle_for_handle,
                                );
                            }
                        } else if sql_text::is_identifier_char(ch) {
                            // Alphanumeric typed - show/update popup if word is long enough
                            if Self::has_min_intellisense_prefix(&word) {
                                Self::schedule_keyup_intellisense_debounce(
                                    &keyup_debounce_generation_for_handle,
                                    &keyup_debounce_handle_for_handle,
                                    cursor_pos,
                                    buffer_len,
                                    ed,
                                    &buffer_for_handle,
                                    &intellisense_data_for_handle,
                                    &intellisense_popup_for_handle,
                                    &completion_range_for_handle,
                                    &column_sender_for_handle,
                                    &connection_for_handle,
                                    &pending_intellisense_for_handle,
                                    &intellisense_parse_cache_for_handle,
                                );
                            } else {
                                intellisense_popup_for_handle.borrow_mut().hide();
                                *completion_range_for_handle.borrow_mut() = None;
                                *pending_intellisense_for_handle.borrow_mut() = None;
                                Self::invalidate_keyup_debounce(
                                    &keyup_debounce_generation_for_handle,
                                    &keyup_debounce_handle_for_handle,
                                );
                            }
                        } else {
                            // Non-identifier character (space, punctuation, etc.)
                            // Close popup - user is done with this word
                            intellisense_popup_for_handle.borrow_mut().hide();
                            *completion_range_for_handle.borrow_mut() = None;
                            *pending_intellisense_for_handle.borrow_mut() = None;
                            Self::invalidate_keyup_debounce(
                                &keyup_debounce_generation_for_handle,
                                &keyup_debounce_handle_for_handle,
                            );
                        }
                    }

                    if Self::has_min_intellisense_prefix(&word) {
                        Self::maybe_prefetch_columns_for_word(
                            &word,
                            &intellisense_data_for_handle,
                            &column_sender_for_handle,
                            &connection_for_handle,
                        );
                    }
                    false
                }
                Event::Unfocus => {
                    Self::invalidate_keyup_debounce(
                        &keyup_debounce_generation_for_handle,
                        &keyup_debounce_handle_for_handle,
                    );
                    false
                }
                Event::Shortcut => {
                    let key = fltk::app::event_key();
                    let popup_visible = intellisense_popup_for_handle.borrow().is_visible();
                    let state = fltk::app::event_state();
                    let ctrl_or_cmd = state.contains(fltk::enums::Shortcut::Ctrl)
                        || state.contains(fltk::enums::Shortcut::Command);

                    // If intellisense is visible, consume Enter/Tab to prevent them from reaching other handlers
                    if popup_visible
                        && matches!(
                            key,
                            Key::Up | Key::Down | Key::Enter | Key::KPEnter | Key::Tab
                        )
                    {
                        return true;
                    }

                    if ctrl_or_cmd && matches!(key, Key::Enter | Key::KPEnter) {
                        if *ctrl_enter_handled_for_handle.borrow() {
                            return true;
                        }
                        *ctrl_enter_handled_for_handle.borrow_mut() = true;
                        widget_for_shortcuts.execute_statement_at_cursor();
                        return true;
                    }

                    false
                }
                Event::Paste => {
                    let from_drop = {
                        let mut pending = dnd_file_drop_pending_for_handle.borrow_mut();
                        let was_pending = *pending;
                        *pending = false;
                        was_pending
                    };
                    if !from_drop {
                        return false;
                    }

                    let event_text = app::event_text();
                    if let Some(path) = Self::extract_dropped_file_path(&event_text) {
                        if Self::invoke_file_drop_callback(&file_drop_callback_for_handle, path) {
                            return true;
                        }
                    }
                    false
                }
                _ => false,
            }
        });
    }

    fn extract_dropped_file_path(raw: &str) -> Option<PathBuf> {
        for token in raw.lines().map(str::trim).filter(|line| !line.is_empty()) {
            if token.starts_with('#') {
                continue;
            }
            let Some(path) = Self::parse_dropped_file_token(token) else {
                continue;
            };
            if path.is_file() {
                return Some(path);
            }
        }
        None
    }

    fn parse_dropped_file_token(token: &str) -> Option<PathBuf> {
        let cleaned = token.trim_matches('\0').trim();
        let cleaned = cleaned
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .or_else(|| {
                cleaned
                    .strip_prefix('\'')
                    .and_then(|value| value.strip_suffix('\''))
            })
            .unwrap_or(cleaned)
            .trim();
        if cleaned.is_empty() {
            return None;
        }

        let path_str = if let Some(rest) = Self::strip_prefix_ignore_ascii_case(cleaned, "file://")
        {
            let mut uri_path = rest.trim();
            if let Some(after_localhost) =
                Self::strip_prefix_ignore_ascii_case(uri_path, "localhost")
            {
                uri_path = after_localhost;
            }
            #[cfg(windows)]
            {
                let bytes = uri_path.as_bytes();
                if bytes.len() >= 3
                    && bytes[0] == b'/'
                    && bytes[1].is_ascii_alphabetic()
                    && bytes[2] == b':'
                {
                    uri_path = &uri_path[1..];
                }
            }
            Self::decode_uri_percent(uri_path)
        } else {
            cleaned.to_string()
        };

        if path_str.is_empty() {
            return None;
        }
        Some(PathBuf::from(path_str))
    }

    fn strip_prefix_ignore_ascii_case<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
        let value_bytes = value.as_bytes();
        let prefix_bytes = prefix.as_bytes();
        if value_bytes.len() < prefix_bytes.len() {
            return None;
        }
        if value_bytes[..prefix_bytes.len()].eq_ignore_ascii_case(prefix_bytes) {
            return value.get(prefix_bytes.len()..);
        }
        None
    }

    fn decode_uri_percent(value: &str) -> String {
        let bytes = value.as_bytes();
        let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
        let mut i = 0usize;
        while i < bytes.len() {
            if bytes[i] == b'%' && i + 2 < bytes.len() {
                let hex_value = |b: u8| -> Option<u8> {
                    match b {
                        b'0'..=b'9' => Some(b - b'0'),
                        b'a'..=b'f' => Some(b - b'a' + 10),
                        b'A'..=b'F' => Some(b - b'A' + 10),
                        _ => None,
                    }
                };
                if let (Some(high), Some(low)) = (hex_value(bytes[i + 1]), hex_value(bytes[i + 2]))
                {
                    out.push((high << 4) | low);
                    i += 3;
                    continue;
                }
            }
            out.push(bytes[i]);
            i += 1;
        }
        String::from_utf8(out)
            .unwrap_or_else(|err| String::from_utf8_lossy(&err.into_bytes()).into_owned())
    }

    pub fn trigger_intellisense(
        editor: &TextEditor,
        buffer: &TextBuffer,
        intellisense_data: &Rc<RefCell<IntellisenseData>>,
        intellisense_popup: &Rc<RefCell<IntellisensePopup>>,
        completion_range: &Rc<RefCell<Option<(usize, usize)>>>,
        column_sender: &mpsc::Sender<ColumnLoadUpdate>,
        connection: &SharedConnection,
        pending_intellisense: &Rc<RefCell<Option<PendingIntellisense>>>,
        intellisense_parse_cache: &Rc<RefCell<Option<IntellisenseParseCacheEntry>>>,
    ) {
        let cursor_pos = Self::raw_cursor_position(buffer, editor.insert_position());
        let cursor_pos_usize = cursor_pos as usize;
        let (word, start, _) = Self::word_at_cursor(buffer, cursor_pos);
        let qualifier = Self::qualifier_before_word(buffer, start);
        let prefix = word;
        let should_hide_after_statement_terminator = prefix.is_empty()
            && qualifier.is_none()
            && Self::non_whitespace_char_before_cursor(buffer, cursor_pos) == Some(';');

        if should_hide_after_statement_terminator {
            intellisense_popup.borrow_mut().hide();
            *pending_intellisense.borrow_mut() = None;
            *completion_range.borrow_mut() = None;
            return;
        }

        // Extract statement text once and cache parse results for repeated triggers
        // at the same cursor position (e.g. async column-load refreshes).
        let (statement_context_text, cursor_in_statement) =
            Self::statement_context_with_cursor(buffer, cursor_pos);
        let cursor_in_statement =
            Self::clamp_to_char_boundary_local(&statement_context_text, cursor_in_statement);

        let cached_context = {
            let cache = intellisense_parse_cache.borrow();
            cache
                .as_ref()
                .filter(|entry| {
                    entry.cursor_in_statement == cursor_in_statement
                        && entry.statement_text.as_str() == statement_context_text.as_str()
                })
                .map(|entry| entry.context.clone())
        };

        let deep_ctx = if let Some(context) = cached_context {
            context
        } else {
            let statement_text = Self::normalize_intellisense_context_text(&statement_context_text);
            let full_tokens = Self::tokenize_sql(&statement_text);
            let before_tokens =
                Self::tokens_before_cursor_byte(&statement_text, cursor_in_statement);
            let parsed = intellisense_context::analyze_cursor_context(&before_tokens, &full_tokens);
            *intellisense_parse_cache.borrow_mut() = Some(IntellisenseParseCacheEntry {
                statement_text: statement_context_text.clone(),
                cursor_in_statement,
                context: parsed.clone(),
            });
            parsed
        };

        let context = if deep_ctx.phase.is_table_context() {
            SqlContext::TableName
        } else if deep_ctx.phase.is_column_context() {
            if matches!(deep_ctx.phase, intellisense_context::SqlPhase::SelectList) {
                SqlContext::ColumnOrAll
            } else {
                SqlContext::ColumnName
            }
        } else {
            SqlContext::General
        };

        // Resolve column tables using deep context
        let column_tables = if let Some(ref q) = qualifier {
            intellisense_context::resolve_qualifier_tables(q, &deep_ctx.tables_in_scope)
        } else {
            intellisense_context::resolve_all_scope_tables(&deep_ctx.tables_in_scope)
        };

        let include_columns = qualifier.is_some()
            || matches!(context, SqlContext::ColumnName | SqlContext::ColumnOrAll);

        let allow_empty_prefix =
            qualifier.is_some() || include_columns || matches!(context, SqlContext::TableName);
        if prefix.is_empty() && !allow_empty_prefix {
            *pending_intellisense.borrow_mut() = None;
            *completion_range.borrow_mut() = None;
            return;
        }

        // Register CTE/subquery alias columns (text-based, with wildcard expansion
        // from base table metadata when possible).
        let mut virtual_wildcard_dependencies: HashMap<String, Vec<String>> = HashMap::new();
        let mut virtual_table_columns: HashMap<String, Vec<String>> = HashMap::new();

        // Register CTE columns
        for cte in &deep_ctx.ctes {
            let mut columns = if !cte.explicit_columns.is_empty() {
                cte.explicit_columns.clone()
            } else if !cte.body_tokens.is_empty() {
                intellisense_context::extract_select_list_columns(&cte.body_tokens)
            } else {
                Vec::new()
            };
            if cte.explicit_columns.is_empty() && !cte.body_tokens.is_empty() {
                let body_tables_in_scope =
                    intellisense_context::collect_tables_in_statement(&cte.body_tokens);
                let (wildcard_columns, wildcard_tables) = Self::expand_virtual_table_wildcards(
                    &cte.body_tokens,
                    &body_tables_in_scope,
                    intellisense_data,
                    column_sender,
                    connection,
                );
                if !wildcard_tables.is_empty() {
                    virtual_wildcard_dependencies.insert(cte.name.to_uppercase(), wildcard_tables);
                }
                columns.extend(wildcard_columns);
            }
            Self::dedup_column_names_case_insensitive(&mut columns);
            if !columns.is_empty() {
                virtual_table_columns.insert(cte.name.clone(), columns);
            }
        }

        // Register subquery alias columns
        for subq in &deep_ctx.subqueries {
            let mut columns = intellisense_context::extract_select_list_columns(&subq.body_tokens);
            let body_tables_in_scope =
                intellisense_context::collect_tables_in_statement(&subq.body_tokens);
            let (wildcard_columns, wildcard_tables) = Self::expand_virtual_table_wildcards(
                &subq.body_tokens,
                &body_tables_in_scope,
                intellisense_data,
                column_sender,
                connection,
            );
            if !wildcard_tables.is_empty() {
                virtual_wildcard_dependencies.insert(subq.alias.to_uppercase(), wildcard_tables);
            }
            columns.extend(wildcard_columns);
            Self::dedup_column_names_case_insensitive(&mut columns);
            if !columns.is_empty() {
                virtual_table_columns.insert(subq.alias.clone(), columns);
            }
        }
        intellisense_data
            .borrow_mut()
            .replace_virtual_table_columns(virtual_table_columns);

        // Load columns from DB for real tables (skip virtual tables)
        if include_columns {
            for table in &column_tables {
                let is_virtual = deep_ctx
                    .ctes
                    .iter()
                    .any(|c| c.name.eq_ignore_ascii_case(table))
                    || deep_ctx
                        .subqueries
                        .iter()
                        .any(|s| s.alias.eq_ignore_ascii_case(table));
                if !is_virtual {
                    Self::request_table_columns(
                        table,
                        intellisense_data,
                        column_sender,
                        connection,
                    );
                }
            }
        }

        let columns_loading = {
            let data = intellisense_data.borrow();
            Self::has_column_loading_for_scope(
                include_columns,
                &column_tables,
                &virtual_wildcard_dependencies,
                &data,
            )
        };

        let suggestions = {
            let mut data = intellisense_data.borrow_mut();
            let column_scope = if !column_tables.is_empty() {
                Some(column_tables.as_slice())
            } else {
                None
            };
            if qualifier.is_some() {
                data.get_column_suggestions(&prefix, column_scope)
            } else {
                data.get_suggestions(
                    &prefix,
                    include_columns,
                    column_scope,
                    matches!(context, SqlContext::TableName),
                    matches!(context, SqlContext::ColumnName | SqlContext::ColumnOrAll),
                )
            }
        };
        let context_alias_suggestions = Self::collect_context_alias_suggestions(&prefix, &deep_ctx);
        let suggestions = Self::maybe_merge_suggestions_with_context_aliases(
            suggestions,
            context_alias_suggestions,
            matches!(context, SqlContext::TableName),
            qualifier.is_some(),
        );

        let should_refresh_when_columns_ready = include_columns && columns_loading;
        if should_refresh_when_columns_ready {
            *pending_intellisense.borrow_mut() = Some(PendingIntellisense { cursor_pos });
        } else {
            *pending_intellisense.borrow_mut() = None;
        }

        if suggestions.is_empty() {
            intellisense_popup.borrow_mut().hide();
            *completion_range.borrow_mut() = None;
            return;
        }

        let popup_width = Self::INTELLISENSE_POPUP_WIDTH;
        let popup_height = (suggestions.len().min(10) * 20 + 10) as i32;
        let (popup_x, popup_y) =
            Self::popup_screen_position(editor, cursor_pos, popup_width, popup_height);

        intellisense_popup
            .borrow_mut()
            .show_suggestions(suggestions, popup_x, popup_y);
        let completion_start = if prefix.is_empty() {
            cursor_pos_usize
        } else {
            start
        };
        *completion_range.borrow_mut() = Some((completion_start, cursor_pos_usize));
        let mut editor = editor.clone();
        let _ = editor.take_focus();
    }

    fn expand_virtual_table_wildcards(
        body_tokens: &[SqlToken],
        body_tables_in_scope: &[intellisense_context::ScopedTableRef],
        intellisense_data: &Rc<RefCell<IntellisenseData>>,
        column_sender: &mpsc::Sender<ColumnLoadUpdate>,
        connection: &SharedConnection,
    ) -> (Vec<String>, Vec<String>) {
        let wildcard_tables = intellisense_context::extract_select_list_wildcard_tables(
            body_tokens,
            body_tables_in_scope,
        );
        if wildcard_tables.is_empty() {
            return (Vec::new(), Vec::new());
        }

        let mut wildcard_columns = Vec::new();
        for table in &wildcard_tables {
            Self::request_table_columns(table, intellisense_data, column_sender, connection);
            let columns = {
                let data = intellisense_data.borrow();
                data.get_columns_for_table(table)
            };
            wildcard_columns.extend(columns);
        }
        Self::dedup_column_names_case_insensitive(&mut wildcard_columns);
        (wildcard_columns, wildcard_tables)
    }

    fn dedup_column_names_case_insensitive(columns: &mut Vec<String>) {
        let mut seen = HashSet::new();
        columns.retain(|column| seen.insert(column.to_uppercase()));
    }

    fn has_column_loading_for_scope(
        include_columns: bool,
        column_tables: &[String],
        virtual_wildcard_dependencies: &HashMap<String, Vec<String>>,
        data: &IntellisenseData,
    ) -> bool {
        if !include_columns {
            return false;
        }

        fn table_is_loading(data: &IntellisenseData, table: &str) -> bool {
            SqlEditorWidget::table_lookup_key_candidates(table)
                .iter()
                .map(|key| key.to_uppercase())
                .any(|key| data.columns_loading.contains(&key))
        }

        column_tables.iter().any(|table| {
            if table_is_loading(data, table) {
                return true;
            }
            let key = table.to_uppercase();
            virtual_wildcard_dependencies
                .get(&key)
                .is_some_and(|deps| deps.iter().any(|dep| table_is_loading(data, dep)))
        })
    }

    fn collect_context_alias_suggestions(
        prefix: &str,
        deep_ctx: &intellisense_context::CursorContext,
    ) -> Vec<String> {
        let prefix_upper = prefix.to_uppercase();
        let mut suggestions = Vec::new();
        let mut seen = HashSet::new();

        let mut push_candidate = |candidate: &str| {
            if candidate.is_empty() {
                return;
            }
            if !prefix_upper.is_empty() {
                let candidate_upper = candidate.to_uppercase();
                if !candidate_upper.starts_with(&prefix_upper) || candidate_upper == prefix_upper {
                    return;
                }
            }
            if seen.insert(candidate.to_uppercase()) {
                suggestions.push(candidate.to_string());
            }
        };

        for table_ref in &deep_ctx.tables_in_scope {
            if let Some(alias) = table_ref.alias.as_deref() {
                push_candidate(alias);
            }
        }

        for cte in &deep_ctx.ctes {
            push_candidate(&cte.name);
        }

        for subq in &deep_ctx.subqueries {
            push_candidate(&subq.alias);
        }

        suggestions
    }

    fn merge_suggestions_with_context_aliases(
        mut base: Vec<String>,
        aliases: Vec<String>,
        prefer_aliases: bool,
    ) -> Vec<String> {
        if aliases.is_empty() {
            base.truncate(MAX_MERGED_SUGGESTIONS);
            return base;
        }

        let mut seen: HashSet<String> = base.iter().map(|item| item.to_uppercase()).collect();
        let mut filtered_aliases = Vec::new();
        for alias in aliases {
            if seen.insert(alias.to_uppercase()) {
                filtered_aliases.push(alias);
            }
        }

        if filtered_aliases.is_empty() {
            base.truncate(MAX_MERGED_SUGGESTIONS);
            return base;
        }

        let mut merged = if prefer_aliases {
            filtered_aliases.extend(base);
            filtered_aliases
        } else {
            base.extend(filtered_aliases);
            base
        };
        merged.truncate(MAX_MERGED_SUGGESTIONS);
        merged
    }

    fn maybe_merge_suggestions_with_context_aliases(
        mut base: Vec<String>,
        aliases: Vec<String>,
        prefer_aliases: bool,
        has_qualifier: bool,
    ) -> Vec<String> {
        if has_qualifier {
            base.truncate(MAX_MERGED_SUGGESTIONS);
            return base;
        }
        Self::merge_suggestions_with_context_aliases(base, aliases, prefer_aliases)
    }

    fn maybe_prefetch_columns_for_word(
        word: &str,
        intellisense_data: &Rc<RefCell<IntellisenseData>>,
        column_sender: &mpsc::Sender<ColumnLoadUpdate>,
        connection: &SharedConnection,
    ) {
        if word.is_empty() {
            return;
        }

        let should_prefetch = {
            let data = intellisense_data.borrow();
            data.is_known_relation(word)
        };

        if should_prefetch {
            Self::request_table_columns(word, intellisense_data, column_sender, connection);
        }
    }

    fn request_table_columns(
        table_name: &str,
        intellisense_data: &Rc<RefCell<IntellisenseData>>,
        column_sender: &mpsc::Sender<ColumnLoadUpdate>,
        connection: &SharedConnection,
    ) {
        let table_key_candidates = Self::table_lookup_key_candidates(table_name);
        if table_key_candidates.is_empty() {
            return;
        }

        let table_key = {
            let mut data = intellisense_data.borrow_mut();
            let selected = table_key_candidates
                .iter()
                .find(|candidate| data.is_known_relation(candidate))
                .cloned();
            let Some(selected) = selected else {
                return;
            };
            if !data.mark_columns_loading(&selected) {
                return;
            }
            selected
        };

        let task = ColumnLoadTask {
            table_key: table_key.clone(),
            connection: connection.clone(),
            sender: column_sender.clone(),
        };

        if let Err(task) = Self::enqueue_column_load_task(task) {
            crate::utils::logging::log_error(
                "sql_editor::intellisense::column_loader",
                &format!(
                    "failed to enqueue column loader task for {}",
                    task.table_key
                ),
            );
            let _ = task.sender.send(ColumnLoadUpdate {
                table: task.table_key,
                columns: Vec::new(),
                cache_columns: false,
            });
            app::awake();
        }
    }

    fn table_lookup_key_candidates(table_name: &str) -> Vec<String> {
        let segments = Self::relation_name_segments(table_name);
        let normalized = segments.join(".");
        if normalized.is_empty() {
            return Vec::new();
        }

        let mut candidates = vec![normalized.clone()];
        if Self::has_unquoted_dot(table_name) {
            if let Some(last) = segments.last() {
                if !last.eq_ignore_ascii_case(&normalized) && !last.trim().is_empty() {
                    candidates.push(last.trim().to_string());
                }
            }
        }

        candidates
    }

    fn relation_name_segments(value: &str) -> Vec<String> {
        let mut parts = Vec::new();
        let mut current = String::new();
        let mut chars = value.trim().chars().peekable();
        let mut in_quotes = false;

        while let Some(ch) = chars.next() {
            match ch {
                '"' => {
                    current.push(ch);
                    if in_quotes {
                        if chars.peek().copied() == Some('"') {
                            current.push('"');
                            chars.next();
                        } else {
                            in_quotes = false;
                        }
                    } else {
                        in_quotes = true;
                    }
                }
                '.' if !in_quotes => {
                    let segment = Self::strip_identifier_quotes(current.trim());
                    if !segment.is_empty() {
                        parts.push(segment);
                    }
                    current.clear();
                }
                _ => current.push(ch),
            }
        }

        let segment = Self::strip_identifier_quotes(current.trim());
        if !segment.is_empty() {
            parts.push(segment);
        }

        parts
    }

    fn has_unquoted_dot(value: &str) -> bool {
        let mut chars = value.trim().chars().peekable();
        let mut in_quotes = false;
        while let Some(ch) = chars.next() {
            match ch {
                '"' => {
                    if in_quotes {
                        if chars.peek().copied() == Some('"') {
                            chars.next();
                        } else {
                            in_quotes = false;
                        }
                    } else {
                        in_quotes = true;
                    }
                }
                '.' if !in_quotes => return true,
                _ => {}
            }
        }
        false
    }

    fn word_at_cursor(buffer: &TextBuffer, cursor_pos: i32) -> (String, usize, usize) {
        let buffer_len = buffer.length().max(0);
        if buffer_len == 0 {
            return (String::new(), 0, 0);
        }
        let cursor_pos = cursor_pos.clamp(0, buffer_len);
        let start = (cursor_pos - INTELLISENSE_WORD_WINDOW).max(0);
        let end = (cursor_pos + INTELLISENSE_WORD_WINDOW).min(buffer_len);
        let start = buffer.line_start(start).max(0);
        let end = buffer.line_end(end).max(start);
        let text = buffer.text_range(start, end).unwrap_or_default();
        let rel_cursor =
            Self::clamp_to_char_boundary_local(&text, (cursor_pos - start).max(0) as usize);
        let (word, rel_start, rel_end) = get_word_at_cursor(&text, rel_cursor);
        let abs_start = start as usize + rel_start;
        let abs_end = start as usize + rel_end;
        (word, abs_start, abs_end)
    }

    fn quoted_identifier_bounds_at(text: &str, rel_pos: usize) -> Option<(usize, usize)> {
        if text.is_empty() {
            return None;
        }

        let rel_pos = Self::clamp_to_char_boundary_local(text, rel_pos.min(text.len()));
        let mut idx = 0usize;

        while idx < text.len() {
            let ch = text.get(idx..)?.chars().next()?;
            if ch != '"' {
                idx += ch.len_utf8();
                continue;
            }

            let start = idx;
            idx += 1;

            while idx < text.len() {
                let cur = text.get(idx..)?.chars().next()?;
                if cur == '"' {
                    let next_idx = idx + cur.len_utf8();
                    if next_idx < text.len() && text.get(next_idx..)?.starts_with('"') {
                        idx = next_idx + 1;
                        continue;
                    }
                    let end = next_idx;
                    if rel_pos >= start && rel_pos <= end {
                        return Some((start, end));
                    }
                    idx = end;
                    break;
                }
                idx += cur.len_utf8();
            }

            if idx >= text.len() && rel_pos >= start && rel_pos <= text.len() {
                return Some((start, text.len()));
            }
        }

        None
    }

    fn identifier_at_position_in_text(
        text: &str,
        rel_pos: usize,
    ) -> Option<(String, usize, usize)> {
        if text.is_empty() {
            return None;
        }

        let rel_pos = Self::clamp_to_char_boundary_local(text, rel_pos.min(text.len()));

        if let Some((start, end)) = Self::quoted_identifier_bounds_at(text, rel_pos) {
            let raw = text.get(start..end)?;
            let word = Self::strip_identifier_quotes(raw);
            if !word.is_empty() {
                return Some((word, start, end));
            }
        }

        let anchor = if rel_pos < text.len() {
            let ch = text.get(rel_pos..)?.chars().next()?;
            if sql_text::is_identifier_char(ch) {
                Some(rel_pos)
            } else {
                None
            }
        } else {
            None
        }
        .or_else(|| {
            if rel_pos == 0 {
                None
            } else {
                text.get(..rel_pos)
                    .and_then(|prefix| prefix.char_indices().next_back())
                    .and_then(|(prev_start, ch)| {
                        if sql_text::is_identifier_char(ch) {
                            Some(prev_start)
                        } else {
                            None
                        }
                    })
            }
        })?;

        let mut start = anchor;
        while start > 0 {
            let Some((prev_start, ch)) = text
                .get(..start)
                .and_then(|prefix| prefix.char_indices().next_back())
            else {
                break;
            };
            if sql_text::is_identifier_char(ch) {
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
            if sql_text::is_identifier_char(ch) {
                end += ch.len_utf8();
            } else {
                break;
            }
        }

        let word = text.get(start..end)?.to_string();
        if word.is_empty() {
            None
        } else {
            Some((word, start, end))
        }
    }

    fn identifier_at_position(buffer: &TextBuffer, pos: i32) -> Option<(String, i32, i32)> {
        let buffer_len = buffer.length().max(0);
        if buffer_len == 0 {
            return None;
        }
        let pos = pos.clamp(0, buffer_len);
        let line_start = buffer.line_start(pos).max(0);
        let line_end = buffer.line_end(pos).max(line_start);
        let text = buffer.text_range(line_start, line_end).unwrap_or_default();
        if text.is_empty() {
            return None;
        }

        let rel_pos = (pos - line_start).max(0) as usize;
        let (word, start, end) = Self::identifier_at_position_in_text(&text, rel_pos)?;
        Some((word, line_start + start as i32, line_start + end as i32))
    }

    fn quick_describe_type_priority(object_type: &str) -> i32 {
        match object_type.to_uppercase().as_str() {
            "TABLE" => 0,
            "VIEW" => 1,
            "FUNCTION" => 2,
            "PROCEDURE" => 3,
            "SEQUENCE" => 4,
            "PACKAGE" => 5,
            "PACKAGE BODY" => 6,
            _ => 50,
        }
    }

    fn format_argument_type_for_quick_describe(arg: &ProcedureArgument) -> String {
        if let Some(pls_type) = arg.pls_type.as_deref() {
            let trimmed = pls_type.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }

        if let Some(data_type) = arg.data_type.as_deref() {
            let upper = data_type.trim().to_uppercase();
            if upper == "NUMBER" {
                if let (Some(p), Some(s)) = (arg.data_precision, arg.data_scale) {
                    return format!("NUMBER({},{})", p, s);
                }
                if let Some(p) = arg.data_precision {
                    return format!("NUMBER({})", p);
                }
                return "NUMBER".to_string();
            }

            if matches!(
                upper.as_str(),
                "VARCHAR2" | "NVARCHAR2" | "VARCHAR" | "CHAR" | "NCHAR" | "RAW"
            ) {
                if let Some(len) = arg.data_length {
                    return format!("{}({})", upper, len.max(1));
                }
                return upper;
            }

            return upper;
        }

        if let Some(type_name) = arg.type_name.as_deref() {
            if let Some(owner) = arg.type_owner.as_deref() {
                return format!("{}.{}", owner, type_name);
            }
            return type_name.to_string();
        }

        "UNKNOWN".to_string()
    }

    fn format_routine_details(
        qualified_name: &str,
        routine_type: &str,
        arguments: &[ProcedureArgument],
    ) -> String {
        let mut details = format!(
            "=== {} {} ===\n\n",
            routine_type.to_uppercase(),
            qualified_name.to_uppercase()
        );

        if arguments.is_empty() {
            details.push_str("No argument metadata found.\n");
            return details;
        }

        let selected_overload = arguments.first().and_then(|arg| arg.overload);
        let selected: Vec<&ProcedureArgument> = arguments
            .iter()
            .filter(|arg| arg.overload == selected_overload)
            .collect();

        if let Some(overload) = selected_overload {
            details.push_str(&format!("Overload: {}\n\n", overload));
        }

        details.push_str(&format!(
            "{:<24} {:<12} {}\n",
            "Argument", "Direction", "Type"
        ));
        details.push_str(&format!("{}\n", "-".repeat(72)));

        let mut return_type: Option<String> = None;
        for arg in selected {
            let is_return = arg.position == 0 && arg.name.is_none();
            let type_display = Self::format_argument_type_for_quick_describe(arg);
            if is_return {
                return_type = Some(type_display);
                continue;
            }
            let arg_name = arg
                .name
                .clone()
                .unwrap_or_else(|| format!("ARG{}", arg.position.max(1)));
            let direction = arg.in_out.clone().unwrap_or_else(|| "IN".to_string());
            details.push_str(&format!(
                "{:<24} {:<12} {}\n",
                arg_name, direction, type_display
            ));
        }

        if let Some(return_type) = return_type {
            details.push_str(&format!("\nReturn Type: {}\n", return_type));
        }

        details
    }

    fn format_sequence_details(info: &SequenceInfo) -> String {
        let mut details = format!("=== Sequence Info: {} ===\n\n", info.name.to_uppercase());
        details.push_str(&format!("{:<18} {}\n", "Min Value", info.min_value));
        details.push_str(&format!("{:<18} {}\n", "Max Value", info.max_value));
        details.push_str(&format!("{:<18} {}\n", "Increment By", info.increment_by));
        details.push_str(&format!("{:<18} {}\n", "Cycle", info.cycle_flag));
        details.push_str(&format!("{:<18} {}\n", "Order", info.order_flag));
        details.push_str(&format!("{:<18} {}\n", "Cache Size", info.cache_size));
        details.push_str(&format!("{:<18} {}\n", "Last Number", info.last_number));
        details.push_str("\nNote: LAST_NUMBER is the next value to be generated.\n");
        details
    }

    fn describe_object(
        conn: &Connection,
        object_name: &str,
        qualifier: Option<&str>,
    ) -> Result<QuickDescribeData, String> {
        let object_name_upper = object_name.to_uppercase();

        if let Some(package_name) = qualifier {
            let package_name_upper = package_name.to_uppercase();
            if let Ok(routines) = ObjectBrowser::get_package_routines(conn, &package_name_upper) {
                if let Some(routine) = routines
                    .iter()
                    .find(|routine| routine.name.eq_ignore_ascii_case(&object_name_upper))
                {
                    let args = ObjectBrowser::get_package_procedure_arguments(
                        conn,
                        &package_name_upper,
                        &object_name_upper,
                    )
                    .map_err(|err| err.to_string())?;
                    let qualified_name = format!("{}.{}", package_name_upper, object_name_upper);
                    let content =
                        Self::format_routine_details(&qualified_name, &routine.routine_type, &args);
                    return Ok(QuickDescribeData::Text {
                        title: format!(
                            "Describe: {} ({})",
                            qualified_name,
                            routine.routine_type.to_uppercase()
                        ),
                        content,
                    });
                }
            }
        }

        if let Ok(columns) = ObjectBrowser::get_table_structure(conn, &object_name_upper) {
            if !columns.is_empty() {
                return Ok(QuickDescribeData::TableColumns(columns));
            }
        }

        let mut object_types = ObjectBrowser::get_object_types(conn, &object_name_upper)
            .map_err(|err| err.to_string())?;
        if object_types.is_empty() {
            return Err(format!(
                "Object not found or not accessible: {}",
                object_name_upper
            ));
        }

        object_types.sort_by_key(|object_type| Self::quick_describe_type_priority(object_type));

        for object_type in object_types {
            let object_type_upper = object_type.to_uppercase();
            match object_type_upper.as_str() {
                "TABLE" | "VIEW" => {
                    if let Ok(columns) =
                        ObjectBrowser::get_table_structure(conn, &object_name_upper)
                    {
                        if !columns.is_empty() {
                            return Ok(QuickDescribeData::TableColumns(columns));
                        }
                    }
                }
                "FUNCTION" | "PROCEDURE" => {
                    let args = ObjectBrowser::get_procedure_arguments(conn, &object_name_upper)
                        .unwrap_or_default();
                    let content =
                        Self::format_routine_details(&object_name_upper, &object_type_upper, &args);
                    return Ok(QuickDescribeData::Text {
                        title: format!("Describe: {} ({})", object_name_upper, object_type_upper),
                        content,
                    });
                }
                "SEQUENCE" => {
                    if let Ok(info) = ObjectBrowser::get_sequence_info(conn, &object_name_upper) {
                        return Ok(QuickDescribeData::Text {
                            title: format!("Describe: {} (SEQUENCE)", object_name_upper),
                            content: Self::format_sequence_details(&info),
                        });
                    }
                }
                "PACKAGE" => {
                    if let Ok(ddl) = ObjectBrowser::get_package_spec_ddl(conn, &object_name_upper) {
                        return Ok(QuickDescribeData::Text {
                            title: format!("Describe: {} (PACKAGE)", object_name_upper),
                            content: ddl,
                        });
                    }
                }
                _ => {
                    if let Ok(ddl) =
                        ObjectBrowser::get_object_ddl(conn, &object_type_upper, &object_name_upper)
                    {
                        return Ok(QuickDescribeData::Text {
                            title: format!(
                                "Describe: {} ({})",
                                object_name_upper, object_type_upper
                            ),
                            content: ddl,
                        });
                    }
                }
            }
        }

        Err(format!(
            "Object not found or not accessible: {}",
            object_name_upper
        ))
    }

    fn context_before_cursor(buffer: &TextBuffer, cursor_pos: i32) -> String {
        let buffer_len = buffer.length().max(0);
        let cursor_pos = cursor_pos.clamp(0, buffer_len);
        let start = (cursor_pos - INTELLISENSE_CONTEXT_WINDOW).max(0);
        let full_text = buffer.text();
        let cursor_pos = Self::clamp_to_char_boundary_local(&full_text, cursor_pos as usize) as i32;
        let start = Self::clamp_to_char_boundary_local(&full_text, start as usize) as i32;
        let text = buffer.text_range(start, cursor_pos).unwrap_or_default();
        let (stmt_start, _) = Self::statement_bounds_in_text(&text, text.len());
        text.get(stmt_start..).unwrap_or("").to_string()
    }

    fn clamp_to_char_boundary_local(text: &str, idx: usize) -> usize {
        let mut idx = idx.min(text.len());
        if text.is_char_boundary(idx) {
            return idx;
        }

        // Clamp invalid UTF-8 byte offsets to the previous valid boundary.
        while idx > 0 && !text.is_char_boundary(idx) {
            idx -= 1;
        }
        idx
    }

    fn raw_cursor_position(buffer: &TextBuffer, pos: i32) -> i32 {
        let buffer_len = buffer.length().max(0);
        pos.clamp(0, buffer_len)
    }

    fn tokens_before_cursor_byte(sql: &str, cursor_byte: usize) -> Vec<SqlToken> {
        let cursor_byte = Self::clamp_to_char_boundary_local(sql, cursor_byte.min(sql.len()));
        let before = sql.get(..cursor_byte).unwrap_or("");
        Self::tokenize_sql(before)
    }

    fn statement_context_with_cursor(buffer: &TextBuffer, cursor_pos: i32) -> (String, usize) {
        let buffer_len = buffer.length().max(0);
        if buffer_len == 0 {
            return (String::new(), 0);
        }
        let cursor_pos = cursor_pos.clamp(0, buffer_len);
        let start = (cursor_pos - INTELLISENSE_STATEMENT_WINDOW).max(0);
        let end = (cursor_pos + INTELLISENSE_STATEMENT_WINDOW).min(buffer_len);
        let full_text = buffer.text();
        let cursor_pos = Self::clamp_to_char_boundary_local(&full_text, cursor_pos as usize) as i32;
        let start = Self::clamp_to_char_boundary_local(&full_text, start as usize) as i32;
        let end = Self::clamp_to_char_boundary_local(&full_text, end as usize) as i32;
        let Some(text) = buffer.text_range(start, end) else {
            return (String::new(), 0);
        };
        let mut rel_cursor = (cursor_pos - start).max(0) as usize;
        if rel_cursor > text.len() {
            rel_cursor = text.len();
        }
        rel_cursor = Self::clamp_to_char_boundary_local(&text, rel_cursor);
        let (stmt_start, stmt_end) = Self::statement_bounds_in_text(&text, rel_cursor);
        let statement = text.get(stmt_start..stmt_end).unwrap_or("").to_string();
        let cursor_in_statement = rel_cursor.saturating_sub(stmt_start).min(statement.len());
        let cursor_in_statement =
            Self::clamp_to_char_boundary_local(&statement, cursor_in_statement);
        (statement, cursor_in_statement)
    }

    #[cfg(test)]
    fn statement_context_in_text(text: &str, cursor_pos: usize) -> String {
        if text.is_empty() {
            return String::new();
        }
        let cursor_pos = Self::clamp_to_char_boundary_local(text, cursor_pos.min(text.len()));
        let start = cursor_pos.saturating_sub(INTELLISENSE_STATEMENT_WINDOW as usize);
        let end = cursor_pos
            .saturating_add(INTELLISENSE_STATEMENT_WINDOW as usize)
            .min(text.len());
        let start = Self::clamp_to_char_boundary_local(text, start);
        let end = Self::clamp_to_char_boundary_local(text, end);
        let window = text.get(start..end).unwrap_or("");
        let rel_cursor = cursor_pos.saturating_sub(start).min(window.len());
        let (stmt_start, stmt_end) = Self::statement_bounds_in_text(window, rel_cursor);
        window.get(stmt_start..stmt_end).unwrap_or("").to_string()
    }

    #[cfg(test)]
    fn context_before_cursor_in_text(text: &str, cursor_pos: usize) -> String {
        let cursor_pos = Self::clamp_to_char_boundary_local(text, cursor_pos.min(text.len()));
        let start = cursor_pos.saturating_sub(INTELLISENSE_CONTEXT_WINDOW as usize);
        let start = Self::clamp_to_char_boundary_local(text, start);
        let window = text.get(start..cursor_pos).unwrap_or("");
        let (stmt_start, _) = Self::statement_bounds_in_text(window, window.len());
        window.get(stmt_start..).unwrap_or("").to_string()
    }

    fn normalize_intellisense_context_text(text: &str) -> String {
        let text = Self::strip_sqlplus_prompt_prefixes(text);
        let mut offset = 0usize;
        while offset < text.len() {
            let rest = &text[offset..];
            let line_len = rest
                .find('\n')
                .map(|idx| idx + 1)
                .unwrap_or_else(|| rest.len());
            let line = &rest[..line_len];
            let trimmed = line.trim();

            if trimmed.is_empty() || trimmed.starts_with("--") {
                offset += line_len;
                continue;
            }

            if Self::is_sqlplus_command_line(trimmed) {
                offset += line_len;
                continue;
            }

            break;
        }
        text.get(offset..).unwrap_or("").to_string()
    }

    fn strip_sqlplus_prompt_prefixes(text: &str) -> String {
        let mut normalized = String::with_capacity(text.len());
        let mut saw_sql_prompt = false;

        for segment in text.split_inclusive('\n') {
            let (line, line_end) = if let Some(stripped) = segment.strip_suffix('\n') {
                (stripped, "\n")
            } else {
                (segment, "")
            };

            let stripped_line = if let Some(stripped) = Self::strip_sqlplus_sql_prompt_prefix(line)
            {
                saw_sql_prompt = true;
                stripped
            } else if saw_sql_prompt {
                Self::strip_sqlplus_numbered_prompt_prefix(line).unwrap_or(line)
            } else {
                line
            };
            normalized.push_str(stripped_line);
            normalized.push_str(line_end);
        }

        normalized
    }

    fn strip_sqlplus_sql_prompt_prefix(line: &str) -> Option<&str> {
        let bytes = line.as_bytes();
        let mut idx = 0usize;

        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }

        if bytes.get(idx..idx + 4).is_some_and(|slice| {
            slice[0].eq_ignore_ascii_case(&b'S')
                && slice[1].eq_ignore_ascii_case(&b'Q')
                && slice[2].eq_ignore_ascii_case(&b'L')
                && slice[3] == b'>'
        }) {
            idx += 4;
            while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                idx += 1;
            }
            return Some(&line[idx..]);
        }

        None
    }

    fn strip_sqlplus_numbered_prompt_prefix(line: &str) -> Option<&str> {
        let bytes = line.as_bytes();
        let mut idx = 0usize;

        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }

        let number_start = idx;
        let had_leading_whitespace = number_start > 0;
        while idx < bytes.len() && bytes[idx].is_ascii_digit() {
            idx += 1;
        }
        if had_leading_whitespace && idx > number_start {
            let mut sep = idx;
            while sep < bytes.len() && bytes[sep].is_ascii_whitespace() {
                sep += 1;
            }
            let whitespace_count = sep.saturating_sub(idx);
            if whitespace_count >= 2 {
                return Some(&line[sep..]);
            }
        }

        None
    }

    fn is_sqlplus_command_line(trimmed_line: &str) -> bool {
        crate::ui::sql_editor::query_text::is_sqlplus_command_line(trimmed_line)
    }

    // 문장 경계 계산은 실행/포맷 공통 규칙을 공유하기 위해 `query_text` 유틸을 사용합니다.
    fn statement_bounds_in_text(text: &str, cursor_pos: usize) -> (usize, usize) {
        crate::ui::sql_editor::query_text::statement_bounds_in_text(text, cursor_pos)
    }

    fn strip_identifier_quotes(value: &str) -> String {
        let trimmed = value.trim();
        if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
            trimmed[1..trimmed.len() - 1].replace("\"\"", "\"")
        } else {
            trimmed.to_string()
        }
    }

    fn qualifier_before_word(buffer: &TextBuffer, word_start: usize) -> Option<String> {
        if word_start == 0 {
            return None;
        }
        let buffer_len = buffer.length().max(0) as usize;
        if word_start > buffer_len {
            return None;
        }
        let start = word_start
            .saturating_sub(INTELLISENSE_QUALIFIER_WINDOW as usize)
            .min(word_start);
        let start = buffer.line_start(start as i32).max(0) as usize;
        let text = buffer
            .text_range(start as i32, word_start as i32)
            .unwrap_or_default();
        let mut rel_word_start = word_start - start;
        if rel_word_start > text.len() {
            rel_word_start = text.len();
        }
        Self::qualifier_before_word_in_text(&text, rel_word_start)
    }

    fn qualifier_before_word_in_text(text: &str, rel_word_start: usize) -> Option<String> {
        if rel_word_start == 0 {
            return None;
        }
        let bytes = text.as_bytes();

        // IntelliSense qualifier must be strict `qualifier.<cursor>` form.
        // Do not allow whitespace around `.` so cases like `e .|` / `e. |`
        // are treated as non-qualified context.
        if bytes.get(rel_word_start.saturating_sub(1)) != Some(&b'.') {
            return None;
        }
        let idx = rel_word_start - 1;

        if idx > 0 && bytes.get(idx - 1) == Some(&b'"') {
            let mut pos = idx as isize - 2;
            loop {
                if pos < 0 {
                    break;
                }
                let pos_usize = pos as usize;
                if bytes[pos_usize] == b'"' {
                    if pos_usize > 0 && bytes[pos_usize - 1] == b'"' {
                        // `""` escape sequence inside quoted identifier: skip the pair.
                        pos -= 2;
                        continue;
                    }
                    let quoted = text.get(pos_usize..idx)?;
                    let qualifier = Self::strip_identifier_quotes(quoted);
                    if qualifier.is_empty() {
                        return None;
                    }
                    return Some(qualifier);
                }
                pos -= 1;
            }
            return None;
        }

        let qualifier_candidate = text.get(..idx)?;
        let mut start_byte = qualifier_candidate.len();
        for (pos, ch) in qualifier_candidate.char_indices().rev() {
            if sql_text::is_identifier_char(ch) {
                start_byte = pos;
                continue;
            }
            break;
        }
        if start_byte == qualifier_candidate.len() {
            return None;
        }
        let qualifier = qualifier_candidate.get(start_byte..)?;
        let qualifier = Self::strip_identifier_quotes(qualifier);
        let starts_with_valid_ident_char = qualifier
            .chars()
            .next()
            .is_some_and(sql_text::is_identifier_start_char);
        if qualifier.is_empty() || !starts_with_valid_ident_char {
            None
        } else {
            Some(qualifier)
        }
    }

    fn try_fast_path_intellisense_filter(
        editor: &TextEditor,
        buffer: &TextBuffer,
        intellisense_popup: &Rc<RefCell<IntellisensePopup>>,
        completion_range: &Rc<RefCell<Option<(usize, usize)>>>,
        cursor_pos: i32,
        key: Key,
        typed_char: Option<char>,
    ) -> bool {
        if !intellisense_popup.borrow().is_visible() {
            return false;
        }

        let Some((start, end)) = *completion_range.borrow() else {
            return false;
        };

        let cursor = cursor_pos.max(0) as usize;
        if !Self::is_cursor_within_completion_range(cursor, start, end, key, typed_char) {
            return false;
        }

        if !Self::is_fast_filter_key(key, typed_char) {
            return false;
        }

        // Fast path: keep existing suggestions and just filter by the current in-range prefix.
        // This avoids re-tokenizing/re-analyzing SQL on each extra identifier keystroke.
        let prefix = Self::prefix_in_completion_range(buffer, start, cursor_pos);
        {
            let mut popup = intellisense_popup.borrow_mut();
            popup.filter_visible_suggestions_by_prefix(&prefix);
            if !popup.is_visible() {
                *completion_range.borrow_mut() = None;
            } else {
                let (popup_width, popup_height) = popup.popup_dimensions();
                let (popup_x, popup_y) =
                    Self::popup_screen_position(editor, cursor_pos, popup_width, popup_height);
                popup.set_position(popup_x, popup_y);
                *completion_range.borrow_mut() = Some((start, cursor.max(start)));
            }
        }
        true
    }

    fn popup_screen_position(
        editor: &TextEditor,
        cursor_pos: i32,
        popup_width: i32,
        popup_height: i32,
    ) -> (i32, i32) {
        let (cursor_x, cursor_y) = editor.position_to_xy(cursor_pos);
        let (win_x, win_y) = editor
            .window()
            .map(|win| (win.x_root(), win.y_root()))
            .unwrap_or((0, 0));

        let mut popup_x = win_x + cursor_x;
        let mut popup_y = win_y + cursor_y + Self::INTELLISENSE_POPUP_Y_OFFSET;

        if let Some(win) = editor.window() {
            let win_w = win.w();
            let win_h = win.h();
            let max_x = (win_x + win_w - popup_width).max(win_x);
            let max_y = (win_y + win_h - popup_height).max(win_y);
            popup_x = popup_x.clamp(win_x, max_x);
            popup_y = popup_y.clamp(win_y, max_y);
        }

        (popup_x, popup_y)
    }

    fn is_cursor_within_completion_range(
        cursor: usize,
        start: usize,
        end: usize,
        key: Key,
        typed_char: Option<char>,
    ) -> bool {
        if cursor >= start && cursor <= end {
            return true;
        }

        // Allow forward typing past the previous end only for identifier-extension input.
        cursor > end
            && typed_char.is_some_and(sql_text::is_identifier_char)
            && !matches!(key, Key::BackSpace | Key::Delete)
    }

    fn is_fast_filter_key(key: Key, typed_char: Option<char>) -> bool {
        if matches!(key, Key::BackSpace | Key::Delete) {
            return true;
        }
        typed_char.is_some_and(sql_text::is_identifier_char)
    }

    fn should_force_full_analysis(ch: char) -> bool {
        ch == '.'
            || ch.is_whitespace()
            || matches!(
                ch,
                ',' | '(' | ')' | '+' | '-' | '*' | '/' | '%' | '=' | '!' | '<' | '>' | ';' | ':'
            )
    }

    fn has_min_intellisense_prefix(word: &str) -> bool {
        word.chars().count() >= 2
    }

    fn should_auto_trigger_intellisense_for_forced_char(
        word: &str,
        qualifier: Option<&str>,
    ) -> bool {
        qualifier.is_some() || Self::has_min_intellisense_prefix(word)
    }

    fn prefix_in_completion_range(buffer: &TextBuffer, start: usize, cursor_pos: i32) -> String {
        let cursor = cursor_pos.max(0) as usize;
        let end = cursor.max(start);
        buffer
            .text_range(start as i32, end as i32)
            .unwrap_or_default()
            .chars()
            .filter(|ch| sql_text::is_identifier_char(*ch))
            .collect()
    }

    fn char_before_cursor(buffer: &TextBuffer, cursor_pos: i32) -> Option<char> {
        if cursor_pos <= 0 {
            return None;
        }
        let start = (cursor_pos - 4).max(0);
        let text = buffer.text_range(start, cursor_pos).unwrap_or_default();
        text.chars().next_back()
    }

    fn non_whitespace_char_before_cursor(buffer: &TextBuffer, cursor_pos: i32) -> Option<char> {
        if cursor_pos <= 0 {
            return None;
        }
        let start = (cursor_pos - INTELLISENSE_CONTEXT_WINDOW).max(0);
        let text = buffer.text_range(start, cursor_pos).unwrap_or_default();
        text.chars().rev().find(|ch| !ch.is_whitespace())
    }

    #[cfg(test)]
    fn non_whitespace_char_before_cursor_in_text(text: &str, cursor_pos: usize) -> Option<char> {
        if text.is_empty() || cursor_pos == 0 {
            return None;
        }
        let cursor_pos = cursor_pos.min(text.len());
        let text = text.get(..cursor_pos).unwrap_or("");
        text.chars().rev().find(|ch| !ch.is_whitespace())
    }

    fn typed_char_from_key_event(
        event_text: &str,
        key: Key,
        shift: bool,
        char_before_cursor: Option<char>,
    ) -> Option<char> {
        if let Some(ch) = event_text.chars().next() {
            return Some(ch);
        }

        if key == Key::from_char('-') {
            // FLTK can report '_' as key '-' with empty event_text when Shift state is
            // already released in KeyUp. Infer from the actual inserted buffer character.
            if let Some(prev) = char_before_cursor {
                if prev == '_' || prev == '-' {
                    return Some(prev);
                }
            }
            if shift {
                return Some('_');
            }
            return Some('-');
        }

        None
    }

    fn is_modifier_key(key: Key) -> bool {
        matches!(
            key,
            Key::ShiftL
                | Key::ShiftR
                | Key::ControlL
                | Key::ControlR
                | Key::AltL
                | Key::AltR
                | Key::MetaL
                | Key::MetaR
                | Key::CapsLock
        )
    }

    /// Show quick describe dialog for a table/view structure.
    pub fn show_quick_describe_dialog(object_name: &str, columns: &[TableColumnDetail]) {
        use fltk::{prelude::*, text::TextDisplay, window::Window};

        let mut info = format!("=== {} ===\n\n", object_name.to_uppercase());
        info.push_str(&format!(
            "{:<30} {:<20} {:<10} {:<10}\n",
            "Column Name", "Data Type", "Nullable", "PK"
        ));
        info.push_str(&format!("{}\n", "-".repeat(70)));

        for col in columns {
            info.push_str(&format!(
                "{:<30} {:<20} {:<10} {:<10}\n",
                col.name,
                col.get_type_display(),
                if col.nullable { "YES" } else { "NO" },
                if col.is_primary_key { "PK" } else { "" }
            ));
        }

        let current_group = fltk::group::Group::try_current();

        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let mut dialog = Window::default()
            .with_size(600, 400)
            .with_label(&format!("Describe: {}", object_name.to_uppercase()));
        crate::ui::center_on_main(&mut dialog);
        dialog.set_color(theme::panel_raised());
        dialog.make_modal(true);
        dialog.begin();

        let mut display = TextDisplay::default().with_pos(10, 10).with_size(580, 340);
        display.set_color(theme::editor_bg());
        display.set_text_color(theme::text_primary());
        display.set_text_font(crate::ui::configured_editor_profile().normal);
        display.set_text_size(crate::ui::configured_ui_font_size());

        let mut buffer = fltk::text::TextBuffer::default();
        buffer.set_text(&info);
        display.set_buffer(buffer);

        let close_btn_x = (600 - BUTTON_WIDTH) / 2;
        let mut close_btn = fltk::button::Button::default()
            .with_pos(close_btn_x, 360)
            .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
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

        while dialog.shown() {
            fltk::app::wait();
            if receiver.try_recv().is_ok() {
                dialog.hide();
            }
        }
    }

    pub fn show_quick_describe_text_dialog(title: &str, content: &str) {
        use fltk::{prelude::*, text::TextDisplay, window::Window};

        let current_group = fltk::group::Group::try_current();

        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let mut dialog = Window::default().with_size(760, 500).with_label(title);
        crate::ui::center_on_main(&mut dialog);
        dialog.set_color(theme::panel_raised());
        dialog.make_modal(true);
        dialog.begin();

        let mut display = TextDisplay::default().with_pos(10, 10).with_size(740, 440);
        display.set_color(theme::editor_bg());
        display.set_text_color(theme::text_primary());
        display.set_text_font(crate::ui::configured_editor_profile().normal);
        display.set_text_size(crate::ui::configured_ui_font_size());

        let mut buffer = fltk::text::TextBuffer::default();
        buffer.set_text(content);
        display.set_buffer(buffer);

        let close_btn_x = (760 - BUTTON_WIDTH) / 2;
        let mut close_btn = fltk::button::Button::default()
            .with_pos(close_btn_x, 460)
            .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
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

        while dialog.shown() {
            fltk::app::wait();
            if receiver.try_recv().is_ok() {
                dialog.hide();
            }
        }
    }
    pub fn hide_intellisense_if_outside(&self, x: i32, y: i32) {
        let mut popup = self.intellisense_popup.borrow_mut();
        if !popup.is_visible() {
            return;
        }
        if popup.contains_point(x, y) {
            return;
        }
        popup.hide();
        *self.completion_range.borrow_mut() = None;
        *self.pending_intellisense.borrow_mut() = None;
    }

    pub fn hide_intellisense(&self) {
        let mut popup = self.intellisense_popup.borrow_mut();
        if popup.is_visible() {
            popup.hide();
        }
        *self.completion_range.borrow_mut() = None;
        *self.pending_intellisense.borrow_mut() = None;
    }

    #[allow(dead_code)]
    pub fn update_intellisense_data(&mut self, data: IntellisenseData) {
        let mut data = data;
        data.rebuild_indices();
        *self.intellisense_data.borrow_mut() = data;
    }

    pub fn get_intellisense_data(&self) -> Rc<RefCell<IntellisenseData>> {
        self.intellisense_data.clone()
    }
    pub fn show_intellisense(&self) {
        Self::trigger_intellisense(
            &self.editor,
            &self.buffer,
            &self.intellisense_data,
            &self.intellisense_popup,
            &self.completion_range,
            &self.column_sender,
            &self.connection,
            &self.pending_intellisense,
            &self.intellisense_parse_cache,
        );
    }

    pub fn quick_describe_at_cursor(&self) {
        let cursor_pos = Self::raw_cursor_position(&self.buffer, self.editor.insert_position());
        let Some((word, start, _)) = Self::identifier_at_position(&self.buffer, cursor_pos) else {
            return;
        };
        let qualifier = Self::qualifier_before_word(&self.buffer, start as usize);
        let object_name = if let Some(ref qualifier) = qualifier {
            format!("{}.{}", qualifier.to_uppercase(), word.to_uppercase())
        } else {
            word.to_uppercase()
        };

        let connection = self.connection.clone();
        let sender = self.ui_action_sender.clone();
        let sender_for_thread = sender.clone();
        set_cursor(Cursor::Wait);
        app::flush();
        let object_name_for_thread = object_name.clone();
        let spawn_result = thread::Builder::new()
            .name("quick-describe".to_string())
            .spawn(move || {
                // Try to acquire connection lock without blocking
                let Some(mut conn_guard) = crate::db::try_lock_connection_with_activity(
                    &connection,
                    format!("Quick describe {}", object_name_for_thread),
                ) else {
                    // Query is already running, notify user
                    let _ = sender_for_thread.send(UiActionResult::QueryAlreadyRunning);
                    app::awake();
                    return;
                };

                let result = match conn_guard.require_live_connection() {
                    Ok(db_conn) => {
                        Self::describe_object(db_conn.as_ref(), &word, qualifier.as_deref())
                    }
                    Err(message) => Err(message.to_string()),
                };

                let _ = sender_for_thread.send(UiActionResult::QuickDescribe {
                    object_name: object_name_for_thread,
                    result,
                });
                app::awake();
            });

        if let Err(err) = spawn_result {
            let message = format!("Failed to start quick describe task: {err}");
            crate::utils::logging::log_error("sql_editor::intellisense::quick_describe", &message);
            let _ = sender.send(UiActionResult::QuickDescribe {
                object_name,
                result: Err(message),
            });
            app::awake();
        }
    }
}

#[cfg(test)]
mod intellisense_regression_tests {
    use super::*;
    use crate::db::create_shared_connection;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::time::Duration;

    #[test]
    fn statement_bounds_ignore_semicolon_in_string_literal() {
        let sql = "SELECT 'a;b' AS txt FROM dual; SELECT 2 FROM dual";
        let cursor = sql.find("FROM dual").unwrap_or(0);
        let (start, end) = SqlEditorWidget::statement_bounds_in_text(sql, cursor);
        assert_eq!(
            sql.get(start..end).unwrap_or(""),
            "SELECT 'a;b' AS txt FROM dual"
        );
    }

    #[test]
    fn statement_bounds_ignore_inner_plsql_semicolons() {
        let sql = "BEGIN\n  v := 1;\n  v := v + 1;\nEND;\nSELECT * FROM dual;";
        let cursor = sql.find("v + 1").unwrap_or(0);
        let (start, end) = SqlEditorWidget::statement_bounds_in_text(sql, cursor);
        assert_eq!(
            sql.get(start..end).unwrap_or(""),
            "BEGIN\n  v := 1;\n  v := v + 1;\nEND"
        );
    }

    #[test]
    fn statement_bounds_slash_terminates_create_plsql_block() {
        // After 'CREATE FUNCTION ... IS BEGIN ... END;\n/\n', a subsequent
        // SELECT should be recognised as a separate statement.
        let sql = "\
CREATE OR REPLACE FUNCTION oqt_f_add(p_a NUMBER, p_b NUMBER)\nRETURN NUMBER\nIS\nBEGIN\n  RETURN NVL(p_a,0) + NVL(p_b,0);\nEND;\n/\nSELECT empno FROM oqt_emp;";
        let cursor = sql.find("empno FROM").unwrap();
        let (start, end) = SqlEditorWidget::statement_bounds_in_text(sql, cursor);
        let stmt = sql.get(start..end).unwrap_or("");
        assert!(
            stmt.contains("SELECT empno FROM oqt_emp"),
            "expected SELECT statement, got: {:?}",
            stmt
        );
        assert!(
            !stmt.contains("CREATE"),
            "CREATE should not leak into the SELECT statement: {:?}",
            stmt
        );
    }

    #[test]
    fn statement_bounds_multiple_create_blocks_with_slash() {
        // Multiple CREATE blocks terminated by '/' followed by a SELECT
        let sql = "\
CREATE OR REPLACE FUNCTION f1 RETURN NUMBER IS\nBEGIN\n  RETURN 1;\nEND;\n/\n\
CREATE OR REPLACE PROCEDURE p1 IS\nBEGIN\n  NULL;\nEND;\n/\n\
SELECT sa FROM oqt_emp ORDER BY empno;";
        let cursor = sql.find("sa FROM").unwrap();
        let (start, end) = SqlEditorWidget::statement_bounds_in_text(sql, cursor);
        let stmt = sql.get(start..end).unwrap_or("");
        assert!(
            stmt.starts_with("SELECT") || stmt.trim_start().starts_with("SELECT"),
            "expected SELECT statement, got: {:?}",
            stmt
        );
        assert!(
            stmt.contains("oqt_emp"),
            "expected oqt_emp in statement: {:?}",
            stmt
        );
    }

    #[test]
    fn statement_bounds_script_with_plsql_blocks_then_select() {
        // Simulates a realistic script: anonymous PL/SQL blocks, CREATE blocks,
        // followed by a SELECT at the end. The cursor is inside the final SELECT.
        let sql = "\
BEGIN\n  EXECUTE IMMEDIATE 'DROP TABLE oqt_emp PURGE';\nEXCEPTION WHEN OTHERS THEN NULL;\nEND;\n/\n\
CREATE TABLE oqt_emp (\n  empno NUMBER PRIMARY KEY,\n  ename VARCHAR2(50),\n  salary NUMBER\n);\n\
INSERT INTO oqt_emp(empno, ename, salary) VALUES (100, 'ALICE', 3000);\nCOMMIT;\n\
CREATE OR REPLACE FUNCTION oqt_f_add(p_a NUMBER, p_b NUMBER)\nRETURN NUMBER\nIS\nBEGIN\n  RETURN NVL(p_a,0) + NVL(p_b,0);\nEND;\n/\n\
PROMPT === final ===\n\
SELECT empno, ename, sa FROM oqt_emp ORDER BY empno;";

        let cursor = sql.find("sa FROM oqt_emp").unwrap();
        let (stmt_start, stmt_end) = SqlEditorWidget::statement_bounds_in_text(sql, cursor);
        let stmt = sql.get(stmt_start..stmt_end).unwrap_or("");
        assert!(
            stmt.contains("oqt_emp"),
            "statement should contain oqt_emp: {:?}",
            stmt
        );
        assert!(
            stmt.contains("SELECT"),
            "statement should contain SELECT: {:?}",
            stmt
        );

        // Now test context analysis for intellisense
        let context_text = SqlEditorWidget::normalize_intellisense_context_text(
            sql.get(stmt_start..cursor).unwrap_or(""),
        );
        let statement_text = SqlEditorWidget::normalize_intellisense_context_text(
            sql.get(stmt_start..stmt_end).unwrap_or(""),
        );

        let before_tokens = SqlEditorWidget::tokenize_sql(&context_text);
        let full_tokens = SqlEditorWidget::tokenize_sql(&statement_text);
        let deep_ctx = intellisense_context::analyze_cursor_context(&before_tokens, &full_tokens);

        assert_eq!(
            deep_ctx.phase,
            intellisense_context::SqlPhase::SelectList,
            "cursor should be in SelectList phase"
        );

        let table_names: Vec<String> = deep_ctx
            .tables_in_scope
            .iter()
            .map(|t| t.name.to_uppercase())
            .collect();
        assert!(
            table_names.contains(&"OQT_EMP".to_string()),
            "oqt_emp should be in scope: {:?}",
            table_names
        );
    }

    #[test]
    fn qualifier_before_word_supports_quoted_identifier() {
        let sql_with_cursor = r#"SELECT "e".| FROM "Emp Table" "e""#;
        let cursor = sql_with_cursor.find('|').unwrap_or(0);
        let sql = sql_with_cursor.replace('|', "");
        let qualifier = SqlEditorWidget::qualifier_before_word_in_text(&sql, cursor);
        assert_eq!(qualifier.as_deref(), Some("e"));
    }

    #[test]
    fn qualifier_before_word_rejects_whitespace_between_dot_and_cursor() {
        let sql_with_cursor = "SELECT e.   | FROM emp e";
        let cursor = sql_with_cursor.find('|').unwrap_or(0);
        let sql = sql_with_cursor.replace('|', "");
        let qualifier = SqlEditorWidget::qualifier_before_word_in_text(&sql, cursor);
        assert_eq!(qualifier, None);
    }

    #[test]
    fn qualifier_before_word_rejects_whitespace_before_dot() {
        let sql_with_cursor = "SELECT e   .| FROM emp e";
        let cursor = sql_with_cursor.find('|').unwrap_or(0);
        let sql = sql_with_cursor.replace('|', "");
        let qualifier = SqlEditorWidget::qualifier_before_word_in_text(&sql, cursor);
        assert_eq!(qualifier, None);
    }

    #[test]
    fn qualifier_before_word_rejects_whitespace_before_dot_with_quoted_identifier() {
        let sql_with_cursor = r#"SELECT "e"   .| FROM "Emp Table" "e""#;
        let cursor = sql_with_cursor.find('|').unwrap_or(0);
        let sql = sql_with_cursor.replace('|', "");
        let qualifier = SqlEditorWidget::qualifier_before_word_in_text(&sql, cursor);
        assert_eq!(qualifier, None);
    }

    #[test]
    fn qualifier_before_word_supports_unicode_identifier() {
        let sql_with_cursor = "SELECT 사용자.| FROM emp 사용자";
        let cursor = sql_with_cursor.find('|').unwrap_or(0);
        let sql = sql_with_cursor.replace('|', "");
        let qualifier = SqlEditorWidget::qualifier_before_word_in_text(&sql, cursor);
        assert_eq!(qualifier.as_deref(), Some("사용자"));
    }

    #[test]
    fn identifier_at_position_supports_unicode_identifier() {
        let sql = "SELECT 사용자 FROM dual";
        let cursor = sql.find("사용자").unwrap_or(0) + "사용자".len();

        let (word, start, end) = SqlEditorWidget::identifier_at_position_in_text(sql, cursor)
            .expect("unicode identifier should be resolved at cursor");
        assert_eq!(word, "사용자");
        assert_eq!(sql.get(start..end), Some("사용자"));
    }

    #[test]
    fn identifier_at_position_supports_quoted_unicode_identifier() {
        let sql = r#"SELECT "사용자"."이름" FROM dual"#;
        let cursor = sql.find(r#""이름""#).unwrap_or(0) + r#""이름""#.len();

        let (word, start, _end) = SqlEditorWidget::identifier_at_position_in_text(sql, cursor)
            .expect("quoted unicode identifier should be resolved at cursor");
        assert_eq!(word, "이름");
        let qualifier = SqlEditorWidget::qualifier_before_word_in_text(sql, start);
        assert_eq!(qualifier.as_deref(), Some("사용자"));
    }

    #[test]
    fn qualifier_before_word_rejects_numeric_identifier_start() {
        let sql_with_cursor = "SELECT 1emp.| FROM emp";
        let cursor = sql_with_cursor.find('|').unwrap_or(0);
        let sql = sql_with_cursor.replace('|', "");
        let qualifier = SqlEditorWidget::qualifier_before_word_in_text(&sql, cursor);
        assert_eq!(qualifier, None);
    }

    #[test]
    fn qualifier_before_word_allows_special_identifier_start_chars() {
        let sql_with_cursor = "SELECT _emp.| FROM emp _emp";
        let cursor = sql_with_cursor.find('|').unwrap_or(0);
        let sql = sql_with_cursor.replace('|', "");
        let qualifier = SqlEditorWidget::qualifier_before_word_in_text(&sql, cursor);
        assert_eq!(qualifier.as_deref(), Some("_emp"));
    }

    #[test]
    fn normalize_intellisense_context_text_skips_leading_prompt_lines() {
        let input = "PROMPT [3] WITH basic + note\n-- separator\nWITH cte AS (SELECT 1 FROM dual)\nSELECT * FROM cte";
        let normalized = SqlEditorWidget::normalize_intellisense_context_text(input);

        assert!(normalized.starts_with("WITH cte AS"));
        assert!(!normalized.starts_with("PROMPT"));
    }

    #[test]
    fn normalize_intellisense_context_text_strips_sqlplus_line_prefixes() {
        let input = "SQL> WITH cte AS (SELECT 1 FROM dual)
  2  SELECT * FROM cte
";
        let normalized = SqlEditorWidget::normalize_intellisense_context_text(input);

        assert_eq!(
            normalized,
            "WITH cte AS (SELECT 1 FROM dual)
SELECT * FROM cte
"
        );
    }

    #[test]
    fn normalize_intellisense_context_text_keeps_numeric_literal_line_prefixes() {
        let input = "SELECT\n1 + 2 AS total\nFROM dual";
        let normalized = SqlEditorWidget::normalize_intellisense_context_text(input);

        assert_eq!(normalized, input);
    }

    #[test]
    fn normalize_intellisense_context_text_keeps_unindented_numeric_lines_with_wide_spacing() {
        let input = "SELECT\n1  + 2 AS total\nFROM dual";
        let normalized = SqlEditorWidget::normalize_intellisense_context_text(input);

        assert_eq!(normalized, input);
    }

    #[test]
    fn normalize_intellisense_context_text_keeps_indented_numeric_lines_without_sql_prompt() {
        let input = "SELECT\n  1  + 2 AS total\nFROM dual";
        let normalized = SqlEditorWidget::normalize_intellisense_context_text(input);

        assert_eq!(normalized, input);
    }

    #[test]
    fn prompt_line_before_with_does_not_break_cte_qualified_column_resolution() {
        let sql_with_cursor = r#"
PROMPT [3] WITH basic + multiple CTE + join + scalar subquery + nested expressions
WITH
  d AS (
    SELECT deptno, dname, loc
    FROM oqt_t_dept
  )
SELECT d.|, d.loc
FROM d
"#;

        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let context_text =
            SqlEditorWidget::normalize_intellisense_context_text(sql.get(..cursor).unwrap_or(""));
        let (stmt_start, stmt_end) = SqlEditorWidget::statement_bounds_in_text(&sql, cursor);
        let statement_text = SqlEditorWidget::normalize_intellisense_context_text(
            sql.get(stmt_start..stmt_end).unwrap_or(""),
        );

        let before_tokens = SqlEditorWidget::tokenize_sql(&context_text);
        let full_tokens = SqlEditorWidget::tokenize_sql(&statement_text);
        let deep_ctx = intellisense_context::analyze_cursor_context(&before_tokens, &full_tokens);

        assert!(
            deep_ctx
                .ctes
                .iter()
                .any(|cte| cte.name.eq_ignore_ascii_case("d")),
            "expected CTE d in parsed context: {:?}",
            deep_ctx
                .ctes
                .iter()
                .map(|cte| cte.name.clone())
                .collect::<Vec<_>>()
        );

        let column_tables =
            intellisense_context::resolve_qualifier_tables("d", &deep_ctx.tables_in_scope);
        assert_eq!(column_tables, vec!["d".to_string()]);

        let mut data = IntellisenseData::new();
        for cte in &deep_ctx.ctes {
            let mut columns = if !cte.explicit_columns.is_empty() {
                cte.explicit_columns.clone()
            } else if !cte.body_tokens.is_empty() {
                intellisense_context::extract_select_list_columns(&cte.body_tokens)
            } else {
                Vec::new()
            };
            SqlEditorWidget::dedup_column_names_case_insensitive(&mut columns);
            if !columns.is_empty() {
                data.set_virtual_table_columns(&cte.name, columns);
            }
        }

        let suggestions = data.get_column_suggestions("", Some(&column_tables));
        assert!(
            suggestions
                .iter()
                .any(|col| col.eq_ignore_ascii_case("DNAME")),
            "expected DNAME suggestion for d.* scope, got: {:?}",
            suggestions
        );
    }

    #[test]
    fn statement_context_uses_window_slice_for_large_multiline_statement() {
        let mut sql = String::from("SELECT\n");
        for _ in 0..3_000 {
            sql.push_str("col_a, col_b, col_c, col_d, col_e, col_f, col_g,\n");
        }
        sql.push_str("dummy_table.col_h,\n");
        sql.push_str("dummy_table.col_i\n");
        sql.push_str("FROM dummy_schema.dummy_table\n");

        let cursor = sql.len();
        let context = SqlEditorWidget::statement_context_in_text(&sql, cursor);
        assert!(
            context.contains("dummy_table.col_h"),
            "statement_context should include the latest select list columns, got {:?}",
            context.get(0..120).unwrap_or("")
        );
    }

    #[test]
    fn context_before_cursor_uses_window_slice_for_large_multiline_statement() {
        let mut sql = String::from("SELECT\n");
        for _ in 0..3_000 {
            sql.push_str("col_a, col_b, col_c, col_d, col_e, col_f, col_g,\n");
        }
        sql.push_str("dummy_table.col_h,\n");
        sql.push_str("dummy_table.col_i\n");
        sql.push_str("FROM dummy_schema.dummy_table\n");

        let cursor = sql.len();
        let context = SqlEditorWidget::context_before_cursor_in_text(&sql, cursor);
        assert!(
            context.contains("dummy_table.col_i"),
            "context_before_cursor should include the latest select list columns, got {:?}",
            context.get(0..120).unwrap_or("")
        );
    }

    #[test]
    fn statement_context_window_clamps_utf8_start_boundary() {
        let mut sql = String::from("가");
        sql.push_str(&"a".repeat(INTELLISENSE_STATEMENT_WINDOW as usize - 1));
        let cursor = sql.len();

        let context = SqlEditorWidget::statement_context_in_text(&sql, cursor);
        assert!(
            !context.is_empty(),
            "statement_context should not become empty when window starts in UTF-8 middle byte"
        );
        assert!(context.contains('가'));
    }

    #[test]
    fn context_before_cursor_window_clamps_utf8_start_boundary() {
        let mut sql = String::from("가");
        sql.push_str(&"a".repeat(INTELLISENSE_CONTEXT_WINDOW as usize - 1));
        let cursor = sql.len();

        let context = SqlEditorWidget::context_before_cursor_in_text(&sql, cursor);
        assert!(
            !context.is_empty(),
            "context_before_cursor should not become empty when window starts in UTF-8 middle byte"
        );
        assert!(context.contains('가'));
    }

    #[test]
    fn qualifier_before_word_in_text_supports_quoted_identifier_at_text_start() {
        let sql_with_cursor = r#""e".| FROM "Employees" e"#;
        let cursor = sql_with_cursor.find('|').unwrap_or(0);
        let sql = sql_with_cursor.replace('|', "");
        let qualifier = SqlEditorWidget::qualifier_before_word_in_text(&sql, cursor);
        assert_eq!(qualifier.as_deref(), Some("e"));
    }

    #[test]
    fn parse_dropped_file_token_decodes_utf8_percent_sequences() {
        let token = "file:///tmp/%ED%95%9C%EA%B8%80.sql";
        let parsed = SqlEditorWidget::parse_dropped_file_token(token);
        assert_eq!(parsed, Some(PathBuf::from("/tmp/한글.sql")));
    }

    #[test]
    fn parse_dropped_file_token_handles_case_insensitive_prefixes() {
        let token = "FiLe://LOCALHOST/tmp/My%20File.sql";
        let parsed = SqlEditorWidget::parse_dropped_file_token(token);
        assert_eq!(parsed, Some(PathBuf::from("/tmp/My File.sql")));
    }

    #[test]
    fn parse_dropped_file_token_strips_wrapping_quotes() {
        let token = "\"file:///tmp/Quoted%20Name.sql\"";
        let parsed = SqlEditorWidget::parse_dropped_file_token(token);
        assert_eq!(parsed, Some(PathBuf::from("/tmp/Quoted Name.sql")));

        let single_quoted = "'file:///tmp/Single%20Quoted.sql'";
        let parsed = SqlEditorWidget::parse_dropped_file_token(single_quoted);
        assert_eq!(parsed, Some(PathBuf::from("/tmp/Single Quoted.sql")));
    }

    #[test]
    fn typed_char_from_key_event_falls_back_for_shifted_underscore() {
        let ch = SqlEditorWidget::typed_char_from_key_event("", Key::from_char('-'), true, None);
        assert_eq!(ch, Some('_'));
    }

    #[test]
    fn typed_char_from_key_event_infers_underscore_from_buffer_even_without_shift_state() {
        let ch =
            SqlEditorWidget::typed_char_from_key_event("", Key::from_char('-'), false, Some('_'));
        assert_eq!(ch, Some('_'));
    }

    #[test]
    fn typed_char_from_key_event_keeps_minus_when_minus_was_inserted() {
        let ch =
            SqlEditorWidget::typed_char_from_key_event("", Key::from_char('-'), false, Some('-'));
        assert_eq!(ch, Some('-'));
    }

    #[test]
    fn debounce_cursor_comparison_uses_raw_offsets() {
        assert!(SqlEditorWidget::is_same_raw_cursor_offset(10, 10));
        assert!(!SqlEditorWidget::is_same_raw_cursor_offset(10, 11));
    }

    #[test]
    fn min_intellisense_prefix_uses_character_count() {
        assert!(!SqlEditorWidget::has_min_intellisense_prefix(""));
        assert!(!SqlEditorWidget::has_min_intellisense_prefix("a"));
        assert!(SqlEditorWidget::has_min_intellisense_prefix("ab"));
        assert!(!SqlEditorWidget::has_min_intellisense_prefix("한"));
        assert!(SqlEditorWidget::has_min_intellisense_prefix("한글"));
    }

    #[test]
    fn auto_trigger_forced_char_requires_qualifier_or_two_chars() {
        assert!(!SqlEditorWidget::should_auto_trigger_intellisense_for_forced_char("", None));
        assert!(!SqlEditorWidget::should_auto_trigger_intellisense_for_forced_char(
            "a", None
        ));
        assert!(!SqlEditorWidget::should_auto_trigger_intellisense_for_forced_char(
            "한", None
        ));
        assert!(SqlEditorWidget::should_auto_trigger_intellisense_for_forced_char(
            "ab", None
        ));
        assert!(SqlEditorWidget::should_auto_trigger_intellisense_for_forced_char(
            "한글", None
        ));
        assert!(SqlEditorWidget::should_auto_trigger_intellisense_for_forced_char(
            "", Some("t")
        ));
    }

    #[test]
    fn tokens_before_cursor_byte_handles_utf8_boundaries() {
        let sql = "SELECT 한글 FROM dual";
        let cursor = "SELECT 한".len();
        let tokens = SqlEditorWidget::tokens_before_cursor_byte(sql, cursor);
        assert_eq!(tokens.len(), 2);
        assert!(matches!(tokens.first(), Some(SqlToken::Word(word)) if word == "SELECT"));
        assert!(matches!(tokens.get(1), Some(SqlToken::Word(word)) if word == "한"));
    }

    #[test]
    fn modifier_key_is_detected_for_shift_release() {
        assert!(SqlEditorWidget::is_modifier_key(Key::ShiftL));
        assert!(SqlEditorWidget::is_modifier_key(Key::ShiftR));
        assert!(!SqlEditorWidget::is_modifier_key(Key::from_char('a')));
    }

    #[test]
    fn request_table_columns_releases_loading_when_connection_busy() {
        let data = Rc::new(RefCell::new(IntellisenseData::new()));
        {
            let mut guard = data.borrow_mut();
            guard.tables = vec!["EMP".to_string()];
            guard.rebuild_indices();
        }

        let (sender, receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let connection = create_shared_connection();
        let _conn_guard = connection.lock().ok();

        SqlEditorWidget::request_table_columns("EMP", &data, &sender, &connection);

        let update = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("column loader should emit a completion update even when lock is busy");
        assert_eq!(update.table, "EMP");
        assert!(update.columns.is_empty());
        assert!(!update.cache_columns);
    }

    #[test]
    fn request_table_columns_handles_quoted_schema_and_table_names() {
        let data = Rc::new(RefCell::new(IntellisenseData::new()));
        {
            let mut guard = data.borrow_mut();
            guard.tables = vec!["SCHEMA.TABLE.NAME".to_string()];
            guard.rebuild_indices();
        }

        let (sender, receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let connection = create_shared_connection();
        let _conn_guard = connection.lock().ok();

        SqlEditorWidget::request_table_columns(
            "\"SCHEMA\".\"TABLE.NAME\"",
            &data,
            &sender,
            &connection,
        );

        let update = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("quoted schema/table names should normalize before relation lookup");
        assert_eq!(update.table, "SCHEMA.TABLE.NAME");
        assert!(!update.cache_columns);
    }
    #[test]
    fn request_table_columns_keeps_exact_dotted_relation_name() {
        let data = Rc::new(RefCell::new(IntellisenseData::new()));
        {
            let mut guard = data.borrow_mut();
            guard.tables = vec!["A.B".to_string()];
            guard.rebuild_indices();
        }

        let (sender, receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let connection = create_shared_connection();
        let _conn_guard = connection.lock().ok();

        SqlEditorWidget::request_table_columns("A.B", &data, &sender, &connection);

        let update = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("known dotted relation name should still be used for column loading");
        assert_eq!(update.table, "A.B");
        assert!(!update.cache_columns);
    }

    #[test]
    fn request_table_columns_falls_back_to_unqualified_name() {
        let data = Rc::new(RefCell::new(IntellisenseData::new()));
        {
            let mut guard = data.borrow_mut();
            guard.tables = vec!["EMP".to_string()];
            guard.rebuild_indices();
        }

        let (sender, receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let connection = create_shared_connection();
        let _conn_guard = connection.lock().ok();

        SqlEditorWidget::request_table_columns("HR.EMP", &data, &sender, &connection);

        let update = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("schema-qualified names should fall back to relation key when needed");
        assert_eq!(update.table, "EMP");
        assert!(!update.cache_columns);
    }

    #[test]
    fn column_loading_scope_detects_unqualified_pending_refresh() {
        let mut data = IntellisenseData::new();
        data.columns_loading.insert("EMP".to_string());
        let column_tables = vec!["emp".to_string()];
        let deps = HashMap::new();
        assert!(SqlEditorWidget::has_column_loading_for_scope(
            true,
            &column_tables,
            &deps,
            &data
        ));
    }

    #[test]
    fn column_loading_scope_detects_schema_qualified_pending_refresh() {
        let mut data = IntellisenseData::new();
        data.columns_loading.insert("EMP".to_string());
        let column_tables = vec!["hr.emp".to_string()];
        let deps = HashMap::new();
        assert!(SqlEditorWidget::has_column_loading_for_scope(
            true,
            &column_tables,
            &deps,
            &data
        ));
    }

    #[test]
    fn request_table_columns_does_not_fallback_when_dot_is_inside_quoted_identifier() {
        let data = Rc::new(RefCell::new(IntellisenseData::new()));
        {
            let mut guard = data.borrow_mut();
            guard.tables = vec!["B".to_string()];
            guard.rebuild_indices();
        }

        let (sender, receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let connection = create_shared_connection();
        let _conn_guard = connection.lock().ok();

        SqlEditorWidget::request_table_columns("\"A.B\"", &data, &sender, &connection);

        let update = receiver.recv_timeout(Duration::from_millis(200));
        assert!(
            update.is_err(),
            "quoted identifier with embedded dot should not fall back to unqualified key"
        );
    }

    #[test]
    fn intellisense_data_clears_stale_column_loading_entries() {
        let mut data = IntellisenseData::new();
        assert!(data.mark_columns_loading("EMP"));
        std::thread::sleep(Duration::from_millis(20));

        let cleared = data.clear_stale_columns_loading(Duration::from_millis(1));
        assert_eq!(cleared, 1);
        assert!(!data.columns_loading.contains("EMP"));
    }

    #[test]
    fn expand_virtual_table_wildcards_uses_loaded_base_table_columns() {
        let data = Rc::new(RefCell::new(IntellisenseData::new()));
        {
            let mut guard = data.borrow_mut();
            guard.tables = vec!["HELP".to_string()];
            guard.rebuild_indices();
            guard.set_columns_for_table("HELP", vec!["TOPIC".to_string(), "TEXT".to_string()]);
        }

        let (sender, _receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let connection = create_shared_connection();
        let tokens = SqlEditorWidget::tokenize_sql("SELECT * FROM help");
        let tables_in_scope = intellisense_context::collect_tables_in_statement(&tokens);

        let (columns, tables) = SqlEditorWidget::expand_virtual_table_wildcards(
            &tokens,
            &tables_in_scope,
            &data,
            &sender,
            &connection,
        );

        let upper_tables: Vec<String> = tables.into_iter().map(|t| t.to_uppercase()).collect();
        assert_eq!(upper_tables, vec!["HELP"]);
        assert_eq!(columns, vec!["TOPIC", "TEXT"]);
    }

    #[test]
    fn collect_context_alias_suggestions_includes_table_aliases_and_ctes() {
        let before = SqlEditorWidget::tokenize_sql(
            "WITH recent_emp AS (SELECT empno FROM emp) SELECT  FROM emp e",
        );
        let full = SqlEditorWidget::tokenize_sql(
            "WITH recent_emp AS (SELECT empno FROM emp) SELECT  FROM emp e",
        );
        let ctx = intellisense_context::analyze_cursor_context(&before, &full);

        let suggestions = SqlEditorWidget::collect_context_alias_suggestions("", &ctx);
        let upper: Vec<String> = suggestions.into_iter().map(|s| s.to_uppercase()).collect();

        assert!(upper.contains(&"E".to_string()));
        assert!(upper.contains(&"RECENT_EMP".to_string()));
    }

    #[test]
    fn merge_suggestions_with_context_aliases_prioritizes_aliases_in_table_context() {
        let merged = SqlEditorWidget::merge_suggestions_with_context_aliases(
            vec!["EMP".to_string(), "SELECT".to_string()],
            vec!["e".to_string(), "recent_emp".to_string(), "EMP".to_string()],
            true,
        );

        assert_eq!(merged[0], "e");
        assert_eq!(merged[1], "recent_emp");
        assert!(merged.contains(&"EMP".to_string()));
        assert!(merged.contains(&"SELECT".to_string()));
    }

    #[test]
    fn merge_suggestions_with_context_aliases_limits_to_max_suggestions() {
        let base: Vec<String> = (0..MAX_MERGED_SUGGESTIONS)
            .map(|i| format!("BASE_{:02}", i))
            .collect();
        let aliases = vec!["e".to_string(), "x".to_string()];

        let merged =
            SqlEditorWidget::merge_suggestions_with_context_aliases(base.clone(), aliases, true);

        assert_eq!(merged.len(), MAX_MERGED_SUGGESTIONS);
        assert_eq!(merged[0], "e");
        assert_eq!(merged[1], "x");
        assert!(!merged.contains(&format!("BASE_{:02}", MAX_MERGED_SUGGESTIONS - 1)));
    }

    #[test]
    fn merge_suggestions_with_context_aliases_respects_max_without_aliases() {
        let base: Vec<String> = (0..(MAX_MERGED_SUGGESTIONS + 5))
            .map(|i| format!("BASE_{:02}", i))
            .collect();

        let merged = SqlEditorWidget::merge_suggestions_with_context_aliases(base, vec![], false);

        assert_eq!(merged.len(), MAX_MERGED_SUGGESTIONS);
    }

    #[test]
    fn maybe_merge_suggestions_with_context_aliases_skips_aliases_when_qualified() {
        let base = vec!["EMPNO".to_string(), "ENAME".to_string()];
        let aliases = vec!["e".to_string(), "emp".to_string()];

        let merged = SqlEditorWidget::maybe_merge_suggestions_with_context_aliases(
            base.clone(),
            aliases,
            false,
            true,
        );

        assert_eq!(merged, base);
    }

    #[test]
    fn cte_chain_qualified_column_suggestions_include_wildcard_expansion() {
        let sql_with_cursor = r#"
WITH
  base AS (
    SELECT e.empno, e.ename, e.job, e.deptno, e.sal,
           REGEXP_REPLACE(e.ename, '[AEIOU]', '*') AS masked_name
    FROM oqt_t_emp e
  ),
  enriched AS (
    SELECT
      b.*,
      (SELECT d.dname FROM oqt_t_dept d WHERE d.deptno = b.deptno) AS dname,
      NTILE(3) OVER (PARTITION BY b.deptno ORDER BY b.sal DESC) AS sal_band
    FROM base b
  ),
  filtered AS (
    SELECT *
    FROM enriched
    WHERE (sal > (SELECT AVG(sal) FROM oqt_t_emp WHERE deptno = enriched.deptno))
       OR (job IN ('MANAGER','ANALYST') AND sal >= 2500)
  )
SELECT
  f.|,
  f.dname,
  f.empno,
  f.ename,
  f.masked_name,
  f.job,
  f.sal,
  f.sal_band,
  -- window frame with last_value (needs careful frame)
  LAST_VALUE(f.sal) OVER (
    PARTITION BY f.deptno
    ORDER BY f.sal
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS max_sal_via_last_value
FROM filtered f
ORDER BY f.deptno, f.sal DESC, f.empno;
"#;

        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");
        let before = &sql[..cursor];

        let before_tokens = SqlEditorWidget::tokenize_sql(before);
        let (stmt_start, stmt_end) = SqlEditorWidget::statement_bounds_in_text(&sql, cursor);
        let statement_text = sql.get(stmt_start..stmt_end).unwrap_or("");
        let full_tokens = SqlEditorWidget::tokenize_sql(statement_text);
        let deep_ctx = intellisense_context::analyze_cursor_context(&before_tokens, &full_tokens);

        let column_tables =
            intellisense_context::resolve_qualifier_tables("f", &deep_ctx.tables_in_scope);
        assert_eq!(
            column_tables,
            vec!["filtered".to_string()],
            "qualifier should resolve to filtered CTE alias"
        );

        let data = Rc::new(RefCell::new(IntellisenseData::new()));
        let (sender, _receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let connection = create_shared_connection();

        for cte in &deep_ctx.ctes {
            let mut columns = if !cte.explicit_columns.is_empty() {
                cte.explicit_columns.clone()
            } else if !cte.body_tokens.is_empty() {
                intellisense_context::extract_select_list_columns(&cte.body_tokens)
            } else {
                Vec::new()
            };
            if cte.explicit_columns.is_empty() && !cte.body_tokens.is_empty() {
                let body_tables_in_scope =
                    intellisense_context::collect_tables_in_statement(&cte.body_tokens);
                let (wildcard_columns, _wildcard_tables) =
                    SqlEditorWidget::expand_virtual_table_wildcards(
                        &cte.body_tokens,
                        &body_tables_in_scope,
                        &data,
                        &sender,
                        &connection,
                    );
                columns.extend(wildcard_columns);
            }
            SqlEditorWidget::dedup_column_names_case_insensitive(&mut columns);
            if !columns.is_empty() {
                data.borrow_mut()
                    .set_virtual_table_columns(&cte.name, columns);
            }
        }

        let mut guard = data.borrow_mut();
        let suggestions = guard.get_column_suggestions("", Some(&column_tables));

        assert!(
            suggestions.iter().any(|c| c.eq_ignore_ascii_case("EMPNO")),
            "expected EMPNO in suggestions: {:?}",
            suggestions
        );
        assert!(
            suggestions.iter().any(|c| c.eq_ignore_ascii_case("DNAME")),
            "expected DNAME in suggestions: {:?}",
            suggestions
        );
        assert!(
            suggestions
                .iter()
                .any(|c| c.eq_ignore_ascii_case("SAL_BAND")),
            "expected SAL_BAND in suggestions: {:?}",
            suggestions
        );
    }

    #[test]
    fn popup_confirm_key_without_selection_does_not_consume_editor_keys() {
        assert!(!SqlEditorWidget::should_consume_popup_confirm_key(
            Key::Tab,
            false,
        ));
        assert!(!SqlEditorWidget::should_consume_popup_confirm_key(
            Key::Enter,
            false,
        ));
        assert!(!SqlEditorWidget::should_consume_popup_confirm_key(
            Key::KPEnter,
            false,
        ));
    }

    #[test]
    fn popup_confirm_key_with_selection_consumes_enter_and_tab() {
        assert!(SqlEditorWidget::should_consume_popup_confirm_key(
            Key::Tab,
            true,
        ));
        assert!(SqlEditorWidget::should_consume_popup_confirm_key(
            Key::Enter,
            true,
        ));
        assert!(SqlEditorWidget::should_consume_popup_confirm_key(
            Key::KPEnter,
            true,
        ));
    }

    #[test]
    fn non_whitespace_char_before_cursor_in_text_detects_semicolon_before_cursor_marker() {
        let sql_with_cursor = "select * from help;|";
        let cursor = sql_with_cursor.find('|').expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let ch = SqlEditorWidget::non_whitespace_char_before_cursor_in_text(&sql, cursor);
        assert_eq!(ch, Some(';'));
    }

    #[test]
    fn non_whitespace_char_before_cursor_in_text_skips_whitespace_after_semicolon() {
        let sql_with_cursor = "select * from help;   |";
        let cursor = sql_with_cursor.find('|').expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let ch = SqlEditorWidget::non_whitespace_char_before_cursor_in_text(&sql, cursor);
        assert_eq!(ch, Some(';'));
    }

    #[test]
    fn invoke_void_callback_restores_slot_even_when_callback_panics() {
        let calls = Rc::new(RefCell::new(0usize));
        let calls_for_cb = calls.clone();
        let callback_slot: Rc<RefCell<Option<Box<dyn FnMut()>>>> =
            Rc::new(RefCell::new(Some(Box::new(move || {
                *calls_for_cb.borrow_mut() += 1;
                panic!("expected callback panic");
            }))));

        let invoked = SqlEditorWidget::invoke_void_callback(&callback_slot);

        assert!(invoked);
        assert!(callback_slot.borrow().is_some());
        assert_eq!(*calls.borrow(), 1);
    }

    #[test]
    fn invoke_void_callback_can_run_again_after_panic() {
        let calls = Rc::new(RefCell::new(0usize));
        let calls_for_cb = calls.clone();
        let callback_slot: Rc<RefCell<Option<Box<dyn FnMut()>>>> =
            Rc::new(RefCell::new(Some(Box::new(move || {
                let mut count = calls_for_cb.borrow_mut();
                *count += 1;
                if *count == 1 {
                    panic!("expected first callback panic");
                }
            }))));

        let first_call = SqlEditorWidget::invoke_void_callback(&callback_slot);
        assert!(first_call);
        assert!(callback_slot.borrow().is_some());

        let second_call = SqlEditorWidget::invoke_void_callback(&callback_slot);
        assert!(second_call);
        assert_eq!(*calls.borrow(), 2);
        assert!(callback_slot.borrow().is_some());
    }

    #[test]
    fn invoke_void_callback_returns_false_when_slot_is_empty() {
        let callback_slot: Rc<RefCell<Option<Box<dyn FnMut()>>>> = Rc::new(RefCell::new(None));

        let invoked = SqlEditorWidget::invoke_void_callback(&callback_slot);

        assert!(!invoked);
        assert!(callback_slot.borrow().is_none());
    }

    #[test]
    fn invoke_void_callback_keeps_replaced_callback_when_original_panics() {
        let callback_slot: Rc<RefCell<Option<Box<dyn FnMut()>>>> = Rc::new(RefCell::new(None));
        let replacement_ran = Rc::new(RefCell::new(false));
        let replacement_ran_for_cb = replacement_ran.clone();
        let callback_slot_for_cb = callback_slot.clone();

        *callback_slot.borrow_mut() = Some(Box::new(move || {
            let replacement_ran_for_replacement = replacement_ran_for_cb.clone();
            *callback_slot_for_cb.borrow_mut() = Some(Box::new(move || {
                *replacement_ran_for_replacement.borrow_mut() = true;
            }));
            panic!("expected panic after replacement");
        }));

        let first_call = SqlEditorWidget::invoke_void_callback(&callback_slot);
        assert!(first_call);
        assert!(callback_slot.borrow().is_some());

        let second_call = SqlEditorWidget::invoke_void_callback(&callback_slot);
        assert!(second_call);
        assert!(*replacement_ran.borrow());
    }

    #[test]
    fn invoke_file_drop_callback_restores_slot_even_when_callback_panics() {
        let calls = Rc::new(RefCell::new(Vec::<PathBuf>::new()));
        let calls_for_cb = calls.clone();
        let callback_slot: Rc<RefCell<Option<Box<dyn FnMut(PathBuf)>>>> =
            Rc::new(RefCell::new(Some(Box::new(move |path: PathBuf| {
                calls_for_cb.borrow_mut().push(path);
                panic!("expected callback panic");
            }))));

        let expected_path = PathBuf::from("/tmp/panic.sql");
        let invoked =
            SqlEditorWidget::invoke_file_drop_callback(&callback_slot, expected_path.clone());

        assert!(invoked);
        assert!(callback_slot.borrow().is_some());
        assert_eq!(calls.borrow().as_slice(), &[expected_path]);
    }

    #[test]
    fn invoke_file_drop_callback_can_run_again_after_panic() {
        let calls = Rc::new(RefCell::new(Vec::<PathBuf>::new()));
        let calls_for_cb = calls.clone();
        let callback_slot: Rc<RefCell<Option<Box<dyn FnMut(PathBuf)>>>> =
            Rc::new(RefCell::new(Some(Box::new(move |path: PathBuf| {
                let mut events = calls_for_cb.borrow_mut();
                let should_panic = events.is_empty();
                events.push(path);
                if should_panic {
                    panic!("expected first callback panic");
                }
            }))));

        let first_path = PathBuf::from("/tmp/first.sql");
        let second_path = PathBuf::from("/tmp/second.sql");

        let first_call =
            SqlEditorWidget::invoke_file_drop_callback(&callback_slot, first_path.clone());
        assert!(first_call);
        assert!(callback_slot.borrow().is_some());

        let second_call =
            SqlEditorWidget::invoke_file_drop_callback(&callback_slot, second_path.clone());
        assert!(second_call);
        assert!(callback_slot.borrow().is_some());
        assert_eq!(calls.borrow().as_slice(), &[first_path, second_path]);
    }

    #[test]
    fn invoke_file_drop_callback_returns_false_when_slot_is_empty() {
        let callback_slot: Rc<RefCell<Option<Box<dyn FnMut(PathBuf)>>>> =
            Rc::new(RefCell::new(None));
        let path = PathBuf::from("/tmp/ignored.sql");

        let invoked = SqlEditorWidget::invoke_file_drop_callback(&callback_slot, path);

        assert!(!invoked);
        assert!(callback_slot.borrow().is_none());
    }

    #[test]
    fn invoke_file_drop_callback_keeps_replaced_callback_when_original_panics() {
        let callback_slot: Rc<RefCell<Option<Box<dyn FnMut(PathBuf)>>>> =
            Rc::new(RefCell::new(None));
        let captured_paths = Rc::new(RefCell::new(Vec::<PathBuf>::new()));
        let captured_paths_for_cb = captured_paths.clone();
        let callback_slot_for_cb = callback_slot.clone();

        *callback_slot.borrow_mut() = Some(Box::new(move |_path: PathBuf| {
            let captured_paths_for_replacement = captured_paths_for_cb.clone();
            *callback_slot_for_cb.borrow_mut() = Some(Box::new(move |path: PathBuf| {
                captured_paths_for_replacement.borrow_mut().push(path);
            }));
            panic!("expected panic after replacement");
        }));

        let first_path = PathBuf::from("/tmp/first-replace.sql");
        let second_path = PathBuf::from("/tmp/second-replace.sql");

        let first_call = SqlEditorWidget::invoke_file_drop_callback(&callback_slot, first_path);
        assert!(first_call);
        assert!(callback_slot.borrow().is_some());

        let second_call =
            SqlEditorWidget::invoke_file_drop_callback(&callback_slot, second_path.clone());
        assert!(second_call);
        assert_eq!(captured_paths.borrow().as_slice(), &[second_path]);
    }
}
