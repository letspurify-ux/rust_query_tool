use fltk::{
    app,
    button::Button,
    draw,
    enums::{Align, CallbackTrigger, Event, FrameType, Key, Shortcut},
    group::Group,
    input::Input,
    menu::MenuButton,
    prelude::*,
    table::{Table, TableContext},
    text::{TextBuffer, TextDisplay},
    window::Window,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::db::{current_active_db_connection, QueryExecutor, QueryResult};
use crate::ui::constants::*;
use crate::ui::font_settings::{configured_editor_profile, FontProfile};
use crate::ui::intellisense_context::{self, ScopedTableRef};
use crate::ui::sql_editor::{SqlEditorWidget, SqlToken};
use crate::ui::theme;

fn byte_index_after_n_chars(s: &str, n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    s.char_indices()
        .nth(n)
        .map(|(idx, _)| idx)
        .unwrap_or_else(|| s.len())
}

fn truncated_content_end(text: &str, max_chars: usize) -> Option<usize> {
    if max_chars == 0 {
        return if text.is_empty() { None } else { Some(0) };
    }

    if max_chars == 1 {
        let mut chars = text.chars();
        return match chars.next() {
            None => None,
            Some(_) => {
                if chars.next().is_some() {
                    Some(0)
                } else {
                    None
                }
            }
        };
    }

    let keep_chars = max_chars.saturating_sub(1);
    let keep_end = byte_index_after_n_chars(text, keep_chars);
    if keep_end >= text.len() {
        None
    } else {
        Some(keep_end)
    }
}

/// Minimum interval between UI updates during streaming
const UI_UPDATE_INTERVAL: Duration = Duration::from_millis(0);
/// Maximum rows to buffer before forcing a UI update
const MAX_BUFFERED_ROWS: usize = 500000;
/// Stop computing column widths after this many rows (widths stabilize quickly)
const WIDTH_SAMPLE_ROWS: usize = 5000;

pub type ResultGridSqlExecuteCallback = Arc<Mutex<Box<dyn FnMut(String)>>>;

#[derive(Clone)]
pub struct ResultTableWidget {
    table: Table,
    headers: Arc<Mutex<Vec<String>>>,
    /// Buffer for pending rows during streaming
    pending_rows: Arc<Mutex<Vec<Vec<String>>>>,
    /// Pending column width updates
    pending_widths: Arc<Mutex<Vec<i32>>>,
    /// Last UI update time
    last_flush: Arc<Mutex<Instant>>,
    /// The sole data store: full original data (non-truncated).
    /// draw_cell reads from here on demand — no data duplication.
    full_data: Arc<Mutex<Vec<Vec<String>>>>,
    /// Maximum displayed characters per cell; full text remains in full_data for copy/export.
    max_cell_display_chars: Arc<Mutex<usize>>,
    /// How many rows have been sampled for column width calculation
    width_sampled_rows: Arc<Mutex<usize>>,
    font_profile: Arc<Mutex<FontProfile>>,
    font_size: Arc<Mutex<u32>>,
    source_sql: Arc<Mutex<String>>,
    execute_sql_callback: Arc<Mutex<Option<ResultGridSqlExecuteCallback>>>,
    edit_session: Arc<Mutex<Option<TableEditSession>>>,
    hidden_auto_rowid_col: Arc<Mutex<Option<usize>>>,
}

#[derive(Default)]
struct DragState {
    is_dragging: bool,
    start_row: i32,
    start_col: i32,
}

#[derive(Clone)]
enum EditRowState {
    Existing { rowid: String },
    Inserted,
}

#[derive(Clone)]
struct TableEditSession {
    rowid_col: usize,
    table_name: String,
    editable_columns: Vec<(usize, String)>,
    original_rows_by_rowid: HashMap<String, Vec<String>>,
    original_row_order: Vec<String>,
    deleted_rowids: Vec<String>,
    row_states: Vec<EditRowState>,
}

impl ResultTableWidget {
    /// Returns the character count of the longest line in `text`,
    /// capped at `max_cell_display_chars`. Used for column width estimation
    /// so that multi-line cells are sized by their widest line, not total length.
    fn longest_line_char_count(text: &str, max_cell_display_chars: usize) -> usize {
        if max_cell_display_chars == 0 {
            return 0;
        }
        text.lines()
            .map(|line| line.chars().count().min(max_cell_display_chars))
            .max()
            .unwrap_or(0)
    }

    fn show_cell_text_dialog(value: &str, font_profile: FontProfile, font_size: u32) {
        let current_group = Group::try_current();
        Group::set_current(None::<&Group>);

        let mut dialog = Window::default()
            .with_size(760, 520)
            .with_label("Cell Value");
        dialog.set_color(theme::panel_raised());
        dialog.make_modal(true);

        let mut display = TextDisplay::new(10, 10, 740, 460, None);
        display.set_color(theme::editor_bg());
        display.set_text_color(theme::text_primary());
        display.set_text_font(font_profile.normal);
        display.set_text_size(font_size as i32);
        display.wrap_mode(fltk::text::WrapMode::AtBounds, 0);

        let mut buf = TextBuffer::default();
        buf.set_text(value);
        display.set_buffer(buf);

        let mut close_btn = Button::new(335, 480, BUTTON_WIDTH, BUTTON_HEIGHT, "Close");
        close_btn.set_color(theme::button_secondary());
        close_btn.set_label_color(theme::text_primary());
        close_btn.set_frame(FrameType::RFlatBox);

        let mut dialog_for_close = dialog.clone();
        close_btn.set_callback(move |_| {
            dialog_for_close.hide();
            app::awake();
        });

        dialog.end();
        dialog.show();
        Group::set_current(current_group.as_ref());

        while dialog.shown() {
            app::wait();
        }

        // Explicitly destroy top-level dialog widgets to release native resources.
        Window::delete(dialog);
    }

    fn try_clone_cell_value(
        full_data: &Arc<Mutex<Vec<Vec<String>>>>,
        row: i32,
        col: i32,
    ) -> Option<String> {
        let data = full_data.try_lock().ok()?;
        data.get(row as usize)
            .and_then(|r| r.get(col as usize))
            .cloned()
    }

    fn should_consume_boundary_arrow(table: &Table, key: Key) -> bool {
        let rows = table.rows();
        let cols = table.cols();
        if rows <= 0 || cols <= 0 {
            return true;
        }

        let (row_top, col_left, row_bot, col_right) = table.get_selection();
        let row = if row_top >= 0 && row_bot >= 0 {
            row_top.min(row_bot)
        } else {
            return false;
        };
        let col = if col_left >= 0 && col_right >= 0 {
            col_left.min(col_right)
        } else {
            return false;
        };

        match key {
            Key::Left => col <= 0,
            Key::Right => col >= cols - 1,
            Key::Up => row <= 0,
            Key::Down => row >= rows - 1,
            _ => return false,
        }
    }

    fn apply_table_metrics_for_current_font(&mut self) {
        let font_size = *self
            .font_size
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.table
            .set_row_height_all(Self::row_height_for_font(font_size));
        self.table
            .set_col_header_height(Self::header_height_for_font(font_size));
    }

    fn row_height_for_font(size: u32) -> i32 {
        (size as i32 + TABLE_CELL_PADDING * 2 + 4).max(TABLE_ROW_HEIGHT)
    }

    fn header_height_for_font(size: u32) -> i32 {
        (size as i32 + TABLE_CELL_PADDING * 2 + 6).max(TABLE_COL_HEADER_HEIGHT)
    }

    fn min_col_width_for_font(size: u32) -> i32 {
        (size as i32 * 6).max(80)
    }

    fn max_col_width_for_font(size: u32) -> i32 {
        (size as i32 * 28).max(300)
    }

    fn estimate_text_width(text: &str, font_size: u32) -> i32 {
        let char_count = text.chars().count() as i32;
        let avg_char_px = ((font_size as i32 * 62) + 99) / 100;
        let raw = char_count.saturating_mul(avg_char_px) + TABLE_CELL_PADDING * 2 + 2;
        raw.clamp(
            Self::min_col_width_for_font(font_size),
            Self::max_col_width_for_font(font_size),
        )
    }

    fn estimate_display_width(text: &str, font_size: u32, max_cell_display_chars: usize) -> i32 {
        let display_chars = Self::longest_line_char_count(text, max_cell_display_chars);
        let avg_char_px = ((font_size as i32 * 62) + 99) / 100;
        let raw = display_chars as i32 * avg_char_px + TABLE_CELL_PADDING * 2 + 2;
        // Cap scales with the cell preview setting rather than a fixed font-based limit,
        // so users who raise the preview length get proportionally wider columns.
        // A hard ceiling of 2000 px prevents absurdly wide columns at large preview values.
        let setting_max =
            (max_cell_display_chars as i32 * avg_char_px + TABLE_CELL_PADDING * 2 + 2).min(2000);
        raw.clamp(Self::min_col_width_for_font(font_size), setting_max)
    }

    fn update_widths_with_row(
        widths: &mut Vec<i32>,
        row: &[String],
        font_size: u32,
        max_cell_display_chars: usize,
    ) {
        let min_width = Self::min_col_width_for_font(font_size);
        if row.len() > widths.len() {
            widths.resize(row.len(), min_width);
        }
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(Self::estimate_display_width(
                cell,
                font_size,
                max_cell_display_chars,
            ));
        }
    }

    fn compute_column_widths(
        headers: &[String],
        rows: &[Vec<String>],
        font_size: u32,
        max_cell_display_chars: usize,
    ) -> Vec<i32> {
        let mut widths: Vec<i32> = headers
            .iter()
            .map(|h| Self::estimate_text_width(h, font_size))
            .collect();

        let sample_count = rows.len().min(WIDTH_SAMPLE_ROWS);
        for row in rows.iter().take(sample_count) {
            Self::update_widths_with_row(&mut widths, row, font_size, max_cell_display_chars);
        }

        widths
    }

    fn apply_widths_to_table(&mut self, widths: &[i32]) {
        if widths.is_empty() {
            return;
        }
        if self.table.cols() < widths.len() as i32 {
            self.table.set_cols(widths.len() as i32);
        }
        for (i, width) in widths.iter().enumerate() {
            self.table.set_col_width(i as i32, *width);
        }
        self.apply_hidden_rowid_column_width();
    }

    fn recalculate_widths_for_current_font(&mut self) {
        let headers = self
            .headers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if headers.is_empty() {
            return;
        }

        let font_size = *self
            .font_size
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let max_cell_display_chars = *self
            .max_cell_display_chars
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut widths: Vec<i32> = headers
            .iter()
            .map(|h| Self::estimate_text_width(h, font_size))
            .collect();

        let mut sampled = 0usize;
        {
            let full_data = self
                .full_data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for row in full_data.iter().take(WIDTH_SAMPLE_ROWS) {
                Self::update_widths_with_row(&mut widths, row, font_size, max_cell_display_chars);
                sampled += 1;
            }
        }

        if sampled < WIDTH_SAMPLE_ROWS {
            let pending = self
                .pending_rows
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let remaining = WIDTH_SAMPLE_ROWS - sampled;
            for row in pending.iter().take(remaining) {
                Self::update_widths_with_row(&mut widths, row, font_size, max_cell_display_chars);
            }
        }

        *self
            .pending_widths
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = widths.clone();
        self.apply_widths_to_table(&widths);
    }

    pub fn new() -> Self {
        Self::with_size(0, 0, 100, 100)
    }

    pub fn with_size(x: i32, y: i32, w: i32, h: i32) -> Self {
        let headers: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let full_data: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let font_profile = Arc::new(Mutex::new(configured_editor_profile()));
        let font_size = Arc::new(Mutex::new(DEFAULT_FONT_SIZE as u32));
        let max_cell_display_chars =
            Arc::new(Mutex::new(RESULT_CELL_MAX_DISPLAY_CHARS_DEFAULT as usize));
        let source_sql = Arc::new(Mutex::new(String::new()));
        let execute_sql_callback: Arc<Mutex<Option<ResultGridSqlExecuteCallback>>> =
            Arc::new(Mutex::new(None));
        let edit_session: Arc<Mutex<Option<TableEditSession>>> = Arc::new(Mutex::new(None));
        let hidden_auto_rowid_col: Arc<Mutex<Option<usize>>> = Arc::new(Mutex::new(None));

        let mut table = Table::new(x, y, w, h, None);

        // Apply dark theme colors
        table.set_color(theme::panel_bg());
        table.set_row_header(true);
        table.set_row_header_width(TABLE_ROW_HEADER_WIDTH);
        table.set_col_header(true);
        table.set_col_header_height(Self::header_height_for_font(DEFAULT_FONT_SIZE as u32));
        table.set_row_height_all(Self::row_height_for_font(DEFAULT_FONT_SIZE as u32));
        table.set_rows(0);
        table.set_cols(0);
        table.end();

        // Capture theme colors once for draw_cell (avoids per-cell function calls)
        let cell_bg = theme::table_cell_bg();
        let cell_fg = theme::text_primary();
        let sel_bg = theme::selection_soft();
        let header_bg = theme::table_header_bg();
        let header_fg = theme::text_primary();
        let border_color = theme::table_border();

        // Virtual rendering: draw_cell reads directly from full_data on demand.
        // Only visible cells are rendered — no per-cell data stored in the Table widget.
        let headers_for_draw = headers.clone();
        let full_data_for_draw = full_data.clone();
        let table_for_draw = table.clone();
        let font_profile_for_draw = font_profile.clone();
        let font_size_for_draw = font_size.clone();
        let max_cell_display_chars_for_draw = max_cell_display_chars.clone();

        table.draw_cell(move |_t, ctx, row, col, x, y, w, h| {
            let font_profile = *font_profile_for_draw
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let font_size = *font_size_for_draw
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                as i32;
            match ctx {
                TableContext::StartPage => {
                    draw::set_font(font_profile.normal, font_size);
                }
                TableContext::ColHeader => {
                    draw::push_clip(x, y, w, h);
                    draw::draw_box(FrameType::FlatBox, x, y, w, h, header_bg);
                    draw::set_draw_color(header_fg);
                    draw::set_font(font_profile.bold, font_size);
                    if let Ok(hdrs) = headers_for_draw.try_lock() {
                        if let Some(text) = hdrs.get(col as usize) {
                            draw::draw_text2(
                                text,
                                x + TABLE_CELL_PADDING,
                                y,
                                w - TABLE_CELL_PADDING * 2,
                                h,
                                Align::Left,
                            );
                        }
                    }
                    draw::set_draw_color(border_color);
                    draw::draw_line(x, y + h - 1, x + w, y + h - 1);
                    draw::pop_clip();
                }
                TableContext::RowHeader => {
                    draw::push_clip(x, y, w, h);
                    draw::draw_box(FrameType::FlatBox, x, y, w, h, header_bg);
                    draw::set_draw_color(header_fg);
                    draw::set_font(font_profile.normal, font_size);
                    let text = (row + 1).to_string();
                    draw::draw_text2(&text, x, y, w - TABLE_CELL_PADDING, h, Align::Right);
                    draw::set_draw_color(border_color);
                    draw::draw_line(x + w - 1, y, x + w - 1, y + h);
                    draw::pop_clip();
                }
                TableContext::Cell => {
                    draw::push_clip(x, y, w, h);
                    let selected = table_for_draw.is_selected(row, col);
                    let bg = if selected { sel_bg } else { cell_bg };
                    draw::draw_box(FrameType::FlatBox, x, y, w, h, bg);
                    draw::set_draw_color(cell_fg);
                    draw::set_font(font_profile.normal, font_size);

                    if let Ok(data) = full_data_for_draw.try_lock() {
                        if let Some(row_data) = data.get(row as usize) {
                            if let Some(cell_val) = row_data.get(col as usize) {
                                let max_chars = *max_cell_display_chars_for_draw
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                                if let Some(truncated_end) =
                                    truncated_content_end(cell_val, max_chars)
                                {
                                    if truncated_end > 0 {
                                        let visible = &cell_val[..truncated_end];
                                        draw::draw_text2(
                                            visible,
                                            x + TABLE_CELL_PADDING,
                                            y,
                                            w - TABLE_CELL_PADDING * 2,
                                            h,
                                            Align::Left,
                                        );
                                    }
                                    draw::draw_text2(
                                        "…",
                                        x + TABLE_CELL_PADDING,
                                        y,
                                        w - TABLE_CELL_PADDING * 2,
                                        h,
                                        Align::Right,
                                    );
                                } else {
                                    draw::draw_text2(
                                        cell_val,
                                        x + TABLE_CELL_PADDING,
                                        y,
                                        w - TABLE_CELL_PADDING * 2,
                                        h,
                                        Align::Left,
                                    );
                                }
                            }
                        }
                    }

                    draw::set_draw_color(border_color);
                    draw::draw_line(x, y + h - 1, x + w, y + h - 1);
                    draw::draw_line(x + w - 1, y, x + w - 1, y + h);
                    draw::pop_clip();
                }
                _ => {}
            }
        });

        // Setup event handler for mouse selection and keyboard shortcuts
        let headers_for_handle = headers.clone();
        let drag_state_for_handle = Arc::new(Mutex::new(DragState::default()));

        let mut table_for_handle = table.clone();
        let full_data_for_handle = full_data.clone();
        let font_profile_for_handle = font_profile.clone();
        let font_size_for_handle = font_size.clone();
        let source_sql_for_handle = source_sql.clone();
        let execute_sql_callback_for_handle = execute_sql_callback.clone();
        let edit_session_for_handle = edit_session.clone();
        let hidden_auto_rowid_col_for_handle = hidden_auto_rowid_col.clone();
        table.handle(move |_, ev| {
            if !table_for_handle.active() {
                return false;
            }
            match ev {
                Event::Push => {
                    let button = app::event_button();
                    if button == app::MouseButton::Right as i32 {
                        Self::show_context_menu(
                            &table_for_handle,
                            &headers_for_handle,
                            &full_data_for_handle,
                            &hidden_auto_rowid_col_for_handle,
                            &source_sql_for_handle,
                            &execute_sql_callback_for_handle,
                            &edit_session_for_handle,
                        );
                        return true;
                    }
                    // Left click - start drag selection
                    if button == app::MouseButton::Left as i32 {
                        let _ = table_for_handle.take_focus();
                        if let Some((row, col)) = Self::get_cell_at_mouse(&table_for_handle) {
                            if app::event_clicks() {
                                // Clone the cell value before entering the modal dialog
                                // event loop so the full_data lock is released first.
                                // Use try_lock() so a streaming flush that is currently
                                // mutating the backing data never blocks the UI thread.
                                let current_font_profile = {
                                    *font_profile_for_handle
                                        .lock()
                                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                                };
                                let current_font_size = {
                                    *font_size_for_handle
                                        .lock()
                                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                                };
                                if Self::try_edit_cell_in_edit_mode(
                                    &table_for_handle,
                                    &headers_for_handle,
                                    &full_data_for_handle,
                                    &edit_session_for_handle,
                                    row,
                                    col,
                                    current_font_profile,
                                    current_font_size,
                                ) {
                                    return true;
                                }

                                let cell_val_owned =
                                    Self::try_clone_cell_value(&full_data_for_handle, row, col);
                                if let Some(cell_val) = cell_val_owned {
                                    let current_font_profile = {
                                        *font_profile_for_handle
                                            .lock()
                                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                                    };
                                    let current_font_size = {
                                        *font_size_for_handle
                                            .lock()
                                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                                    };
                                    Self::show_cell_text_dialog(
                                        &cell_val,
                                        current_font_profile,
                                        current_font_size,
                                    );
                                    return true;
                                }
                            }
                            let mut state = drag_state_for_handle
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                            state.is_dragging = true;
                            state.start_row = row;
                            state.start_col = col;
                            table_for_handle.set_selection(row, col, row, col);
                            table_for_handle.redraw();
                            return true;
                        }
                    }
                    false
                }
                Event::Drag => {
                    let is_dragging = drag_state_for_handle
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .is_dragging;
                    if is_dragging {
                        if let Some((row, col)) =
                            Self::get_cell_at_mouse_for_drag(&table_for_handle)
                        {
                            let state = drag_state_for_handle
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                            let r1 = state.start_row.min(row);
                            let r2 = state.start_row.max(row);
                            let c1 = state.start_col.min(col);
                            let c2 = state.start_col.max(col);
                            drop(state);
                            table_for_handle.set_selection(r1, c1, r2, c2);
                            table_for_handle.redraw();
                        }
                        return true;
                    }
                    false
                }
                Event::Released => {
                    let mut state = drag_state_for_handle
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if state.is_dragging {
                        state.is_dragging = false;
                        return true;
                    }
                    false
                }
                Event::KeyDown => {
                    let key = app::event_key();
                    let original_key = app::event_original_key();
                    let state = app::event_state();
                    let ctrl_or_cmd =
                        state.contains(Shortcut::Ctrl) || state.contains(Shortcut::Command);
                    let shift = state.contains(Shortcut::Shift);

                    if matches!(key, Key::Left | Key::Right | Key::Up | Key::Down) {
                        return Self::should_consume_boundary_arrow(&table_for_handle, key);
                    }

                    if ctrl_or_cmd {
                        if shift && Self::matches_shortcut_key(key, original_key, 'c') {
                            Self::copy_selected_with_headers(
                                &table_for_handle,
                                &headers_for_handle,
                                &full_data_for_handle,
                                *hidden_auto_rowid_col_for_handle
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
                            );
                            return true;
                        }
                        if Self::matches_shortcut_key(key, original_key, 'a') {
                            let rows = table_for_handle.rows();
                            let cols = table_for_handle.cols();
                            if rows > 0 && cols > 0 {
                                table_for_handle.set_selection(0, 0, rows - 1, cols - 1);
                                table_for_handle.redraw();
                            }
                            return true;
                        }
                        if Self::matches_shortcut_key(key, original_key, 'c') {
                            Self::copy_selected_to_clipboard(
                                &table_for_handle,
                                &full_data_for_handle,
                                *hidden_auto_rowid_col_for_handle
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
                            );
                            return true;
                        }
                        if Self::matches_shortcut_key(key, original_key, 'v') {
                            app::paste_text(&table_for_handle);
                            return true;
                        }
                    }

                    if matches!(key, Key::Enter | Key::KPEnter | Key::F2) {
                        let can_edit = Self::resolve_update_target_cell(
                            table_for_handle.get_selection(),
                            table_for_handle.rows().max(0) as usize,
                            table_for_handle.cols().max(0) as usize,
                            None,
                        )
                        .is_some()
                            && edit_session_for_handle
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .is_some();

                        if can_edit {
                            let current_font_profile = {
                                *font_profile_for_handle
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                            };
                            let current_font_size = {
                                *font_size_for_handle
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                            };
                            if let Some((row, col)) = Self::resolve_update_target_cell(
                                table_for_handle.get_selection(),
                                table_for_handle.rows().max(0) as usize,
                                table_for_handle.cols().max(0) as usize,
                                None,
                            ) {
                                if Self::try_edit_cell_in_edit_mode(
                                    &table_for_handle,
                                    &headers_for_handle,
                                    &full_data_for_handle,
                                    &edit_session_for_handle,
                                    row as i32,
                                    col as i32,
                                    current_font_profile,
                                    current_font_size,
                                ) {
                                    return true;
                                }
                            }
                            return false;
                        }
                    }

                    false
                }
                Event::Shortcut => {
                    let key = app::event_key();
                    let original_key = app::event_original_key();
                    let state = app::event_state();
                    let ctrl_or_cmd =
                        state.contains(Shortcut::Ctrl) || state.contains(Shortcut::Command);
                    let shift = state.contains(Shortcut::Shift);

                    if ctrl_or_cmd && shift && Self::matches_shortcut_key(key, original_key, 'c') {
                        Self::copy_selected_with_headers(
                            &table_for_handle,
                            &headers_for_handle,
                            &full_data_for_handle,
                            *hidden_auto_rowid_col_for_handle
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner()),
                        );
                        return true;
                    }
                    if ctrl_or_cmd && Self::matches_shortcut_key(key, original_key, 'c') {
                        Self::copy_selected_to_clipboard(
                            &table_for_handle,
                            &full_data_for_handle,
                            *hidden_auto_rowid_col_for_handle
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner()),
                        );
                        return true;
                    }
                    if ctrl_or_cmd && Self::matches_shortcut_key(key, original_key, 'a') {
                        let rows = table_for_handle.rows();
                        let cols = table_for_handle.cols();
                        if rows > 0 && cols > 0 {
                            table_for_handle.set_selection(0, 0, rows - 1, cols - 1);
                            table_for_handle.redraw();
                        }
                        return true;
                    }
                    if ctrl_or_cmd && Self::matches_shortcut_key(key, original_key, 'v') {
                        app::paste_text(&table_for_handle);
                        return true;
                    }
                    false
                }
                Event::Paste => {
                    let pasted_text = app::event_text();
                    match Self::paste_clipboard_text_into_edit_mode(
                        &table_for_handle,
                        &full_data_for_handle,
                        &edit_session_for_handle,
                        &pasted_text,
                    ) {
                        Ok(_) => true,
                        Err(err) => {
                            // Clipboard text can be delivered to this widget via non-edit paths
                            // (e.g. drag/drop). Only surface actionable errors to users.
                            if !err.is_empty() {
                                fltk::dialog::alert_default(&err);
                            }
                            true
                        }
                    }
                }
                _ => false,
            }
        });

        Self {
            table,
            headers,
            pending_rows: Arc::new(Mutex::new(Vec::new())),
            pending_widths: Arc::new(Mutex::new(Vec::new())),
            last_flush: Arc::new(Mutex::new(Instant::now())),
            full_data,
            max_cell_display_chars,
            width_sampled_rows: Arc::new(Mutex::new(0)),
            font_profile,
            font_size,
            source_sql,
            execute_sql_callback,
            edit_session,
            hidden_auto_rowid_col,
        }
    }

    fn show_inline_cell_editor(
        table: &Table,
        row: i32,
        col: i32,
        current_value: &str,
        font_profile: FontProfile,
        font_size: u32,
    ) -> Option<String> {
        let Some((x, y, w, h)) = table.find_cell(TableContext::Cell, row, col) else {
            return fltk::dialog::input_default(
                "Value (blank/NULL -> NULL, '=expr' -> SQL)",
                current_value,
            );
        };

        let current_group = Group::try_current();
        let parent_group = table.parent();
        if let Some(ref parent) = parent_group {
            Group::set_current(Some(parent));
        } else {
            Group::set_current(None::<&Group>);
        }

        let input_x = x + 1;
        let input_y = y + 1;
        let input_w = (w - 2).max(24);
        let input_h = (h - 2).max(24);
        let mut input = Input::new(input_x, input_y, input_w, input_h, None);
        Group::set_current(current_group.as_ref());

        input.set_color(theme::input_bg());
        input.set_text_color(theme::text_primary());
        input.set_text_font(font_profile.normal);
        input.set_text_size(font_size as i32);
        input.set_value(current_value);
        input.set_trigger(CallbackTrigger::EnterKeyAlways);

        let result: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let finished = Arc::new(Mutex::new(false));
        let cancelled = Arc::new(Mutex::new(false));

        {
            let result = result.clone();
            let finished = finished.clone();
            let input_value = input.clone();
            input.set_callback(move |_| {
                *result
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(input_value.value());
                *finished
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
            });
        }

        {
            let result = result.clone();
            let finished = finished.clone();
            let cancelled = cancelled.clone();
            let mut input_handle = input.clone();
            input_handle.handle(move |widget, ev| match ev {
                Event::KeyDown if app::event_key() == Key::Escape => {
                    *cancelled
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
                    *finished
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
                    widget.hide();
                    true
                }
                Event::Unfocus => {
                    let is_finished = *finished
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if !is_finished {
                        *result
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                            Some(widget.value());
                        *finished
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
                    }
                    false
                }
                _ => false,
            });
        }

        let _ = input.take_focus();
        while !*finished
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
        {
            if input.was_deleted() {
                *cancelled
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
                *finished
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
                break;
            }
            app::wait();
        }

        let was_cancelled = *cancelled
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let value = result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if !input.was_deleted() {
            Input::delete(input);
        }
        let mut table = table.clone();
        if !table.was_deleted() {
            let _ = table.take_focus();
            table.redraw();
        }
        if was_cancelled {
            None
        } else {
            Some(value.unwrap_or_default())
        }
    }

    fn try_edit_cell_in_edit_mode(
        table: &Table,
        headers: &Arc<Mutex<Vec<String>>>,
        full_data: &Arc<Mutex<Vec<Vec<String>>>>,
        edit_session: &Arc<Mutex<Option<TableEditSession>>>,
        row: i32,
        col: i32,
        font_profile: FontProfile,
        font_size: u32,
    ) -> bool {
        let (row_idx, col_idx) = match (usize::try_from(row), usize::try_from(col)) {
            (Ok(r), Ok(c)) => (r, c),
            _ => return true,
        };

        let (rowid_col, is_editable_column) = {
            let guard = edit_session
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(session) = guard.as_ref() else {
                return false;
            };
            (
                session.rowid_col,
                session
                    .editable_columns
                    .iter()
                    .any(|(editable_col, _)| *editable_col == col_idx),
            )
        };

        if col_idx == rowid_col {
            fltk::dialog::alert_default("ROWID column cannot be edited.");
            return true;
        }
        if !is_editable_column {
            fltk::dialog::alert_default("Selected column is not editable.");
            return true;
        }

        let column_exists = headers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(col_idx)
            .is_some();
        if !column_exists {
            fltk::dialog::alert_default("Selected column is out of range.");
            return true;
        }

        let current_value = {
            let data = full_data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            data.get(row_idx)
                .and_then(|row_data| row_data.get(col_idx))
                .cloned()
                .unwrap_or_default()
        };

        let Some(new_value) =
            Self::show_inline_cell_editor(table, row, col, &current_value, font_profile, font_size)
        else {
            return true;
        };
        if new_value == current_value {
            return true;
        }

        {
            let mut data = full_data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(row_data) = data.get_mut(row_idx) else {
                fltk::dialog::alert_default("Selected row is out of range.");
                return true;
            };
            if col_idx >= row_data.len() {
                row_data.resize(col_idx + 1, String::new());
            }
            row_data[col_idx] = new_value;
        }
        let mut table = table.clone();
        table.redraw();
        true
    }

    fn matches_shortcut_key(key: Key, original_key: Key, ascii: char) -> bool {
        let lower = Key::from_char(ascii.to_ascii_lowercase());
        let upper = Key::from_char(ascii.to_ascii_uppercase());
        key == lower || key == upper || original_key == lower || original_key == upper
    }

    fn parse_clipboard_rows(clipboard_text: &str) -> Vec<Vec<String>> {
        if clipboard_text.is_empty() {
            return Vec::new();
        }
        let normalized = clipboard_text.replace("\r\n", "\n").replace('\r', "\n");
        let mut rows: Vec<Vec<String>> = normalized
            .split('\n')
            .map(|line| line.split('\t').map(|cell| cell.to_string()).collect())
            .collect();
        while rows.len() > 1
            && rows
                .last()
                .and_then(|row| row.first().map(|first| row.len() == 1 && first.is_empty()))
                .unwrap_or(false)
        {
            rows.pop();
        }
        rows
    }

    fn resolve_paste_anchor_column(
        anchor_col: usize,
        selection: Option<(usize, usize, usize, usize)>,
        rowid_col: usize,
        editable_cols: &HashSet<usize>,
        max_cols: usize,
    ) -> Option<usize> {
        if max_cols == 0 {
            return None;
        }

        let is_editable_target = |col: usize| col != rowid_col && editable_cols.contains(&col);
        if anchor_col < max_cols && is_editable_target(anchor_col) {
            return Some(anchor_col);
        }

        if let Some((_, col_start, _, col_end)) = selection {
            let start = col_start.min(max_cols.saturating_sub(1));
            let end = col_end.min(max_cols.saturating_sub(1));
            for col in start..=end {
                if is_editable_target(col) {
                    return Some(col);
                }
            }
        }

        let start_right = anchor_col.saturating_add(1);
        if start_right < max_cols {
            for col in start_right..max_cols {
                if is_editable_target(col) {
                    return Some(col);
                }
            }
        }

        let left_end = anchor_col.min(max_cols.saturating_sub(1));
        for col in 0..=left_end {
            if is_editable_target(col) {
                return Some(col);
            }
        }

        None
    }

    fn apply_paste_values_to_data(
        full_data: &mut Vec<Vec<String>>,
        rowid_col: usize,
        editable_cols: &HashSet<usize>,
        anchor: (usize, usize),
        selection: Option<(usize, usize, usize, usize)>,
        pasted_rows: &[Vec<String>],
    ) -> usize {
        if pasted_rows.is_empty() {
            return 0;
        }

        let mut changed_cells = 0usize;
        let mut apply_value = |target_row: usize, target_col: usize, value: &str| {
            if target_col == rowid_col || !editable_cols.contains(&target_col) {
                return;
            }
            let Some(row) = full_data.get_mut(target_row) else {
                return;
            };
            if target_col >= row.len() {
                row.resize(target_col + 1, String::new());
            }
            if row
                .get(target_col)
                .map(|existing| existing != value)
                .unwrap_or(true)
            {
                row[target_col] = value.to_string();
                changed_cells = changed_cells.saturating_add(1);
            }
        };

        if pasted_rows.len() == 1 && pasted_rows.first().map(|r| r.len()).unwrap_or(0) == 1 {
            let fill_value = pasted_rows
                .first()
                .and_then(|row| row.first())
                .map(|s| s.as_str())
                .unwrap_or_default();
            if let Some((row_start, col_start, row_end, col_end)) = selection {
                for row_idx in row_start..=row_end {
                    for col_idx in col_start..=col_end {
                        apply_value(row_idx, col_idx, fill_value);
                    }
                }
                return changed_cells;
            }
            apply_value(anchor.0, anchor.1, fill_value);
            return changed_cells;
        }

        for (row_offset, source_row) in pasted_rows.iter().enumerate() {
            for (col_offset, source_cell) in source_row.iter().enumerate() {
                let Some(target_row) = anchor.0.checked_add(row_offset) else {
                    continue;
                };
                let Some(target_col) = anchor.1.checked_add(col_offset) else {
                    continue;
                };
                apply_value(target_row, target_col, source_cell);
            }
        }

        changed_cells
    }

    fn paste_clipboard_text_into_edit_mode(
        table: &Table,
        full_data: &Arc<Mutex<Vec<Vec<String>>>>,
        edit_session: &Arc<Mutex<Option<TableEditSession>>>,
        clipboard_text: &str,
    ) -> Result<usize, String> {
        let session = edit_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .cloned()
            .ok_or_else(String::new)?;
        let anchor = Self::selected_anchor_cell(table)
            .ok_or_else(|| "Select a target cell in the result table first.".to_string())?;
        let pasted_rows = Self::parse_clipboard_rows(clipboard_text);
        if pasted_rows.is_empty() {
            return Err("Clipboard text is empty.".to_string());
        }

        let selection = Self::normalized_selection_bounds_with_limits(
            table.get_selection(),
            table.rows().max(0) as usize,
            table.cols().max(0) as usize,
        );
        let editable_cols: HashSet<usize> = session
            .editable_columns
            .iter()
            .map(|(col_idx, _)| *col_idx)
            .collect();
        let anchor_col = Self::resolve_paste_anchor_column(
            anchor.1,
            selection,
            session.rowid_col,
            &editable_cols,
            table.cols().max(0) as usize,
        )
        .ok_or_else(|| "No editable target column is selected for paste.".to_string())?;
        let anchor = (anchor.0, anchor_col);
        let changed_cells = {
            let mut rows = full_data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if rows.len() != session.row_states.len() {
                return Err("Edit session and table rows are out of sync.".to_string());
            }
            Self::apply_paste_values_to_data(
                &mut rows,
                session.rowid_col,
                &editable_cols,
                anchor,
                selection,
                &pasted_rows,
            )
        };

        if changed_cells == 0 {
            return Err("No editable cells were updated from pasted values.".to_string());
        }

        let mut table = table.clone();
        table.redraw();
        Ok(changed_cells)
    }

    /// Get cell at mouse position (returns None if outside cells)
    fn get_cell_at_mouse(table: &Table) -> Option<(i32, i32)> {
        let rows = table.rows();
        let cols = table.cols();
        if rows <= 0 || cols <= 0 {
            return None;
        }

        let mouse_x = app::event_x();
        let mouse_y = app::event_y();

        let table_x = table.x();
        let table_y = table.y();
        let table_w = table.w();
        let table_h = table.h();
        let data_left = table_x + table.row_header_width();
        let data_top = table_y + table.col_header_height();
        let data_right = table_x + table_w;
        let data_bottom = table_y + table_h;

        if mouse_x < data_left
            || mouse_y < data_top
            || mouse_x >= data_right
            || mouse_y >= data_bottom
        {
            return None;
        }

        let last_row = rows.saturating_sub(1);
        let last_col = cols.saturating_sub(1);
        let start_row = table.row_position().max(0).min(last_row);
        let start_col = table.col_position().max(0).min(last_col);

        let mut row_hit = None;
        let mut row = start_row;
        while row < rows {
            if let Some((_, cy, _, ch)) = table.find_cell(TableContext::Cell, row, start_col) {
                if mouse_y >= cy && mouse_y < cy + ch {
                    row_hit = Some(row);
                    break;
                }
                if cy > mouse_y || cy >= data_bottom {
                    break;
                }
            } else {
                break;
            }
            row += 1;
        }

        let row_hit = match row_hit {
            Some(row_hit) => row_hit,
            None => return None,
        };

        let mut col = start_col;
        while col < cols {
            if let Some((cx, _, cw, _)) = table.find_cell(TableContext::Cell, row_hit, col) {
                if mouse_x >= cx && mouse_x < cx + cw {
                    return Some((row_hit, col));
                }
                if cx > mouse_x || cx >= data_right {
                    break;
                }
            } else {
                break;
            }
            col += 1;
        }

        None
    }

    /// Get row index when mouse is over row header area.
    fn get_row_header_at_mouse(table: &Table) -> Option<i32> {
        let rows = table.rows();
        if rows <= 0 {
            return None;
        }

        let mouse_x = app::event_x();
        let mouse_y = app::event_y();

        let table_x = table.x();
        let table_y = table.y();
        let table_h = table.h();
        let row_header_right = table_x + table.row_header_width();
        let data_top = table_y + table.col_header_height();
        let data_bottom = table_y + table_h;

        if mouse_x < table_x
            || mouse_x >= row_header_right
            || mouse_y < data_top
            || mouse_y >= data_bottom
        {
            return None;
        }

        let last_row = rows.saturating_sub(1);
        let start_row = table.row_position().max(0).min(last_row);
        let mut row = start_row;
        while row < rows {
            if let Some((_, cy, _, ch)) = table.find_cell(TableContext::RowHeader, row, 0) {
                if mouse_y >= cy && mouse_y < cy + ch {
                    return Some(row);
                }
                if cy > mouse_y || cy >= data_bottom {
                    break;
                }
            } else {
                break;
            }
            row += 1;
        }

        None
    }

    /// Get cell at mouse position for drag (clamps to boundaries)
    fn get_cell_at_mouse_for_drag(table: &Table) -> Option<(i32, i32)> {
        let rows = table.rows();
        let cols = table.cols();

        if rows <= 0 || cols <= 0 {
            return None;
        }

        let mouse_x = app::event_x();
        let mouse_y = app::event_y();

        // Try direct lookup first
        if let Some((row, col)) = Self::get_cell_at_mouse(table) {
            return Some((row, col));
        }

        // Calculate boundaries for clamping
        let table_x = table.x();
        let table_y = table.y();
        let table_w = table.w();
        let table_h = table.h();
        let row_header_w = table.row_header_width();
        let col_header_h = table.col_header_height();

        let data_left = table_x + row_header_w;
        let data_top = table_y + col_header_h;
        let data_right = table_x + table_w;
        let data_bottom = table_y + table_h;

        // Clamp row
        let last_row = rows.saturating_sub(1);
        let last_col = cols.saturating_sub(1);

        let row = if mouse_y < data_top {
            0
        } else if mouse_y >= data_bottom {
            last_row
        } else {
            // Find row by iterating
            (0..rows)
                .find(|&r| {
                    if let Some((_, cy, _, ch)) = table.find_cell(TableContext::Cell, r, 0) {
                        mouse_y >= cy && mouse_y < cy + ch
                    } else {
                        false
                    }
                })
                .unwrap_or(last_row)
        };

        // Clamp col
        let col = if mouse_x < data_left {
            0
        } else if mouse_x >= data_right {
            last_col
        } else {
            (0..cols)
                .find(|&c| {
                    if let Some((cx, _, cw, _)) = table.find_cell(TableContext::Cell, 0, c) {
                        mouse_x >= cx && mouse_x < cx + cw
                    } else {
                        false
                    }
                })
                .unwrap_or(last_col)
        };

        Some((row, col))
    }

    fn is_unquoted_identifier(text: &str) -> bool {
        let mut chars = text.chars();
        let Some(first) = chars.next() else {
            return false;
        };
        if !(first.is_ascii_alphabetic() || matches!(first, '_' | '$' | '#')) {
            return false;
        }
        chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '$' | '#'))
    }

    fn quote_identifier_segment(text: &str) -> String {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return "\"\"".to_string();
        }
        if trimmed.starts_with('"') && trimmed.ends_with('"') {
            return trimmed.to_string();
        }
        // Keep unquoted identifiers unquoted regardless of case so SQL semantics
        // (case-insensitive resolution to upper identifiers) are preserved.
        if Self::is_unquoted_identifier(trimmed) {
            return trimmed.to_string();
        }
        format!("\"{}\"", trimmed.replace('"', "\"\""))
    }

    fn split_qualified_identifier(name: &str) -> Vec<&str> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Vec::new();
        }

        let mut segments = Vec::new();
        let mut in_quotes = false;
        let mut segment_start = 0usize;
        let mut chars = trimmed.char_indices().peekable();
        while let Some((idx, ch)) = chars.next() {
            if ch == '"' {
                if in_quotes {
                    if let Some((_, next_ch)) = chars.peek() {
                        if *next_ch == '"' {
                            chars.next();
                            continue;
                        }
                    }
                    in_quotes = false;
                } else {
                    in_quotes = true;
                }
                continue;
            }
            if ch == '.' && !in_quotes {
                if trimmed.is_char_boundary(segment_start) && trimmed.is_char_boundary(idx) {
                    let segment = trimmed[segment_start..idx].trim();
                    if !segment.is_empty() {
                        segments.push(segment);
                    }
                }
                segment_start = idx + ch.len_utf8();
            }
        }

        if trimmed.is_char_boundary(segment_start) {
            let segment = trimmed[segment_start..].trim();
            if !segment.is_empty() {
                segments.push(segment);
            }
        }
        segments
    }

    fn quote_qualified_identifier(name: &str) -> String {
        let segments = Self::split_qualified_identifier(name);
        if segments.is_empty() {
            return Self::quote_identifier_segment(name);
        }
        segments
            .into_iter()
            .map(Self::quote_identifier_segment)
            .collect::<Vec<_>>()
            .join(".")
    }

    fn last_identifier_segment(name: &str) -> &str {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return "";
        }

        let mut in_quotes = false;
        let mut last_dot_idx = None;
        let mut chars = trimmed.char_indices().peekable();
        while let Some((idx, ch)) = chars.next() {
            if ch == '"' {
                if in_quotes {
                    if let Some((_, next_ch)) = chars.peek() {
                        if *next_ch == '"' {
                            chars.next();
                            continue;
                        }
                    }
                    in_quotes = false;
                } else {
                    in_quotes = true;
                }
                continue;
            }
            if ch == '.' && !in_quotes {
                last_dot_idx = Some(idx);
            }
        }

        if let Some(dot_idx) = last_dot_idx {
            let start = dot_idx + 1;
            if trimmed.is_char_boundary(start) {
                return trimmed[start..].trim();
            }
        }
        trimmed
    }

    fn editable_column_identifier(column_header: &str) -> Option<String> {
        let column = Self::last_identifier_segment(column_header);
        if column.is_empty() {
            return None;
        }
        Some(Self::quote_identifier_segment(column))
    }

    fn sql_string_literal(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    fn sql_literal_from_input(input: &str) -> String {
        let trimmed = input.trim();
        if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("NULL") {
            return "NULL".to_string();
        }
        if let Some(expr) = trimmed.strip_prefix('=') {
            let expr_trimmed = expr.trim();
            if !expr_trimmed.is_empty() {
                return expr_trimmed.to_string();
            }
        }
        if trimmed.parse::<i64>().is_ok() || trimmed.parse::<f64>().is_ok() {
            return trimmed.to_string();
        }
        // Preserve user-entered leading/trailing whitespace for string literals.
        Self::sql_string_literal(input)
    }

    #[allow(dead_code)]
    fn compose_edit_script(dml_sql: &str, source_sql: &str) -> String {
        let dml = dml_sql.trim().trim_end_matches(';').trim();
        let select_sql = source_sql.trim().trim_end_matches(';').trim();
        if dml.is_empty() {
            return String::new();
        }
        if select_sql.is_empty() {
            return dml.to_string();
        }
        format!("{dml};\n{select_sql}")
    }

    fn normalize_header_for_lookup(header: &str) -> String {
        header.replace('"', "").trim().to_ascii_uppercase()
    }

    fn find_rowid_column_index(headers: &[String]) -> Option<usize> {
        headers.iter().position(|name| {
            let normalized = Self::normalize_header_for_lookup(name);
            normalized == "ROWID" || normalized.ends_with(".ROWID")
        })
    }

    fn detect_auto_hidden_rowid_col(
        headers: &[String],
        source_sql: &str,
        edit_mode_enabled: bool,
    ) -> Option<usize> {
        if edit_mode_enabled {
            return None;
        }
        let source_sql = source_sql.trim();
        if source_sql.is_empty() {
            return None;
        }
        let rowid_col = Self::find_rowid_column_index(headers)?;
        if rowid_col != 0 {
            return None;
        }
        let rewritten_sql = QueryExecutor::maybe_inject_rowid_for_editing(source_sql);
        if rewritten_sql == source_sql {
            return None;
        }
        Some(rowid_col)
    }

    fn hidden_auto_rowid_col_value(&self) -> Option<usize> {
        *self
            .hidden_auto_rowid_col
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn apply_hidden_rowid_column_width(&mut self) {
        let Some(hidden_col) = self.hidden_auto_rowid_col_value() else {
            return;
        };
        if hidden_col >= self.table.cols().max(0) as usize {
            return;
        }
        self.table.set_col_width(hidden_col as i32, 0);
    }

    fn refresh_table_layout_geometry(&mut self) {
        // Force FLTK to recompute scroll range and visible viewport
        // after runtime column width changes.
        let (x, y, w, h) = (
            self.table.x(),
            self.table.y(),
            self.table.w(),
            self.table.h(),
        );
        self.table.resize(x, y, w, h);
    }

    fn refresh_auto_rowid_visibility(&mut self) {
        let headers_snapshot = self
            .headers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let source_sql = self
            .source_sql
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let edit_mode_enabled = self.is_edit_mode_enabled();
        let next_hidden_col =
            Self::detect_auto_hidden_rowid_col(&headers_snapshot, &source_sql, edit_mode_enabled);
        let previous_hidden_col = self.hidden_auto_rowid_col_value();
        if previous_hidden_col == next_hidden_col {
            self.apply_hidden_rowid_column_width();
            self.refresh_table_layout_geometry();
            self.table.redraw();
            return;
        }

        *self
            .hidden_auto_rowid_col
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = next_hidden_col;

        if previous_hidden_col.is_some() && next_hidden_col.is_none() {
            self.recalculate_widths_for_current_font();
        }
        self.apply_hidden_rowid_column_width();
        self.refresh_table_layout_geometry();
        self.table.redraw();
    }

    fn visible_column_indices_in_range(
        col_left: usize,
        col_right: usize,
        hidden_col: Option<usize>,
    ) -> Vec<usize> {
        (col_left..=col_right)
            .filter(|col| Some(*col) != hidden_col)
            .collect()
    }

    fn visible_headers(headers: &[String], hidden_col: Option<usize>) -> Vec<String> {
        headers
            .iter()
            .enumerate()
            .filter(|(idx, _)| Some(*idx) != hidden_col)
            .map(|(_, header)| header.clone())
            .collect()
    }

    fn visible_row_values_internal(row: &[String], hidden_col: Option<usize>) -> Vec<String> {
        row.iter()
            .enumerate()
            .filter(|(idx, _)| Some(*idx) != hidden_col)
            .map(|(_, value)| value.clone())
            .collect()
    }

    fn strip_identifier_quotes(text: &str) -> String {
        let trimmed = text.trim();
        if let Some(inner) = trimmed.strip_prefix('"').and_then(|v| v.strip_suffix('"')) {
            return inner.replace("\"\"", "\"");
        }
        trimmed.to_string()
    }

    fn resolve_target_table_candidates(tables: &[ScopedTableRef]) -> Vec<String> {
        let mut result = Vec::new();
        let mut seen = HashSet::new();
        for table_ref in tables {
            if table_ref.is_cte {
                continue;
            }
            let key = table_ref.name.to_ascii_uppercase();
            if seen.insert(key) {
                result.push(table_ref.name.clone());
            }
        }
        result
    }

    fn find_rowid_qualifier(tokens: &[SqlToken]) -> Option<String> {
        let mut depth = 0usize;
        let mut in_select = false;
        let mut idx = 0usize;

        while idx < tokens.len() {
            match tokens.get(idx) {
                Some(SqlToken::Symbol(sym)) if sym == "(" => {
                    depth = depth.saturating_add(1);
                }
                Some(SqlToken::Symbol(sym)) if sym == ")" => {
                    depth = depth.saturating_sub(1);
                }
                Some(SqlToken::Word(word)) => {
                    if depth == 0 && word.eq_ignore_ascii_case("SELECT") {
                        in_select = true;
                    } else if in_select && depth == 0 && word.eq_ignore_ascii_case("FROM") {
                        break;
                    }
                }
                _ => {}
            }

            if in_select && depth == 0 {
                let qualifier = match (tokens.get(idx), tokens.get(idx + 1), tokens.get(idx + 2)) {
                    (
                        Some(SqlToken::Word(lhs)),
                        Some(SqlToken::Symbol(dot)),
                        Some(SqlToken::Word(rhs)),
                    ) if dot == "."
                        && Self::strip_identifier_quotes(rhs).eq_ignore_ascii_case("ROWID") =>
                    {
                        Some(Self::strip_identifier_quotes(lhs))
                    }
                    _ => None,
                };
                if qualifier.is_some() {
                    return qualifier;
                }
            }

            idx += 1;
        }

        None
    }

    fn resolve_target_table(source_sql: &str) -> Result<String, String> {
        let sql = source_sql.trim();
        if sql.is_empty() {
            return Err(
                "Cannot edit rows: source SQL is not available for this result.".to_string(),
            );
        }

        let tokens = SqlEditorWidget::tokenize_sql(sql);
        let tables_in_scope = intellisense_context::collect_tables_in_statement(&tokens);
        let candidates = Self::resolve_target_table_candidates(&tables_in_scope);
        if candidates.is_empty() {
            return Err(
                "Cannot edit rows: no base table was resolved from this query.".to_string(),
            );
        }

        if let Some(qualifier) = Self::find_rowid_qualifier(&tokens) {
            let resolved =
                intellisense_context::resolve_qualifier_tables(&qualifier, &tables_in_scope);
            let mut resolved_deduped = Vec::new();
            let mut seen = HashSet::new();
            for table in resolved {
                let key = table.to_ascii_uppercase();
                if seen.insert(key) {
                    resolved_deduped.push(table);
                }
            }
            if resolved_deduped.len() == 1 {
                return Ok(resolved_deduped.remove(0));
            }
        }

        if candidates.len() == 1 {
            return Ok(candidates[0].clone());
        }

        Err(format!(
            "Cannot resolve a single edit target table (candidates: {}). Query one table or qualify ROWID with an alias.",
            candidates.join(", ")
        ))
    }

    fn selected_anchor_cell(table: &Table) -> Option<(usize, usize)> {
        let (row_start, col_start, _, _) = Self::normalized_selection_bounds_with_limits(
            table.get_selection(),
            table.rows().max(0) as usize,
            table.cols().max(0) as usize,
        )?;
        Some((row_start, col_start))
    }

    #[allow(dead_code)]
    fn selected_row(table: &Table) -> Option<usize> {
        Self::selected_anchor_cell(table).map(|(row, _)| row)
    }

    fn selected_row_range(table: &Table) -> Option<(usize, usize)> {
        let (row_start, _, row_end, _) = Self::normalized_selection_bounds_with_limits(
            table.get_selection(),
            table.rows().max(0) as usize,
            table.cols().max(0) as usize,
        )?;
        Some((row_start, row_end))
    }

    fn normalized_selection_bounds(
        selection: (i32, i32, i32, i32),
    ) -> Option<(usize, usize, usize, usize)> {
        let (row_top, col_left, row_bot, col_right) = selection;
        if row_top < 0 || col_left < 0 || row_bot < 0 || col_right < 0 {
            return None;
        }

        let row_start = row_top.min(row_bot) as usize;
        let row_end = row_top.max(row_bot) as usize;
        let col_start = col_left.min(col_right) as usize;
        let col_end = col_left.max(col_right) as usize;
        Some((row_start, col_start, row_end, col_end))
    }

    fn normalized_selection_bounds_with_limits(
        selection: (i32, i32, i32, i32),
        max_rows: usize,
        max_cols: usize,
    ) -> Option<(usize, usize, usize, usize)> {
        if max_rows == 0 || max_cols == 0 {
            return None;
        }

        let (row_start, col_start, row_end, col_end) =
            Self::normalized_selection_bounds(selection)?;
        if row_start >= max_rows || col_start >= max_cols {
            return None;
        }

        let row_max = max_rows.saturating_sub(1);
        let col_max = max_cols.saturating_sub(1);
        let row_start = row_start.min(row_max);
        let row_end = row_end.min(row_max);
        let col_start = col_start.min(col_max);
        let col_end = col_end.min(col_max);

        if row_start > row_end || col_start > col_end {
            None
        } else {
            Some((row_start, col_start, row_end, col_end))
        }
    }

    fn selection_contains_cell(selection: (i32, i32, i32, i32), row: i32, col: i32) -> bool {
        if row < 0 || col < 0 {
            return false;
        }
        let Some((row_start, col_start, row_end, col_end)) =
            Self::normalized_selection_bounds(selection)
        else {
            return false;
        };

        let row = row as usize;
        let col = col as usize;
        row >= row_start && row <= row_end && col >= col_start && col <= col_end
    }

    fn resolve_update_target_cell(
        selection: (i32, i32, i32, i32),
        max_rows: usize,
        max_cols: usize,
        context_cell: Option<(usize, usize)>,
    ) -> Option<(usize, usize)> {
        if let Some((row, col)) = context_cell {
            if row >= max_rows || col >= max_cols {
                return None;
            }
            return Some((row, col));
        }

        let (row_start, col_start, row_end, col_end) =
            Self::normalized_selection_bounds_with_limits(selection, max_rows, max_cols)?;
        if row_start != row_end || col_start != col_end {
            return None;
        }

        Some((row_start, col_start))
    }

    fn try_execute_sql(
        execute_sql_callback: &Arc<Mutex<Option<ResultGridSqlExecuteCallback>>>,
        sql: String,
    ) -> Result<(), String> {
        let callback = execute_sql_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let Some(callback) = callback else {
            return Err("Edit callback is not connected.".to_string());
        };
        let mut cb = callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (*cb)(sql);
        Ok(())
    }

    #[allow(dead_code)]
    fn rowid_for_row(
        row_index: usize,
        headers: &[String],
        full_data: &[Vec<String>],
    ) -> Result<(usize, String), String> {
        let rowid_col = Self::find_rowid_column_index(headers)
            .ok_or_else(|| "Editing requires a ROWID column in the result set.".to_string())?;
        let row = full_data
            .get(row_index)
            .ok_or_else(|| "Selected row is out of range.".to_string())?;
        let rowid = row
            .get(rowid_col)
            .ok_or_else(|| "ROWID value is missing for the selected row.".to_string())?
            .trim()
            .to_string();
        if rowid.is_empty() {
            return Err("Selected row has an empty ROWID value.".to_string());
        }
        Ok((rowid_col, rowid))
    }

    #[allow(dead_code)]
    fn push_unique_rowid(rowids: &mut Vec<String>, seen: &mut HashSet<String>, rowid_raw: &str) {
        let rowid = rowid_raw.trim();
        if rowid.is_empty() || seen.contains(rowid) {
            return;
        }
        let rowid_owned = rowid.to_string();
        seen.insert(rowid_owned.clone());
        rowids.push(rowid_owned);
    }

    #[allow(dead_code)]
    fn selected_rowids(
        table: &Table,
        headers: &[String],
        full_data: &[Vec<String>],
    ) -> Result<Vec<String>, String> {
        let (row_start, row_end) = Self::selected_row_range(table)
            .ok_or_else(|| "Select at least one row.".to_string())?;
        let rowid_col = Self::find_rowid_column_index(headers)
            .ok_or_else(|| "Editing requires a ROWID column in the result set.".to_string())?;

        Self::collect_rowids_in_range(row_start, row_end, rowid_col, full_data)
    }

    #[allow(dead_code)]
    fn collect_rowids_in_range(
        row_start: usize,
        row_end: usize,
        rowid_col: usize,
        full_data: &[Vec<String>],
    ) -> Result<Vec<String>, String> {
        let mut rowids = Vec::new();
        let mut seen = HashSet::new();
        for row_index in row_start..=row_end {
            let row = full_data
                .get(row_index)
                .ok_or_else(|| format!("Selected row {} is out of range.", row_index + 1))?;
            let rowid_raw = row.get(rowid_col).ok_or_else(|| {
                format!("ROWID value is missing for selected row {}.", row_index + 1)
            })?;
            if rowid_raw.trim().is_empty() {
                return Err(format!(
                    "Selected row {} has an empty ROWID value.",
                    row_index + 1
                ));
            }
            Self::push_unique_rowid(&mut rowids, &mut seen, rowid_raw);
        }

        if rowids.is_empty() {
            return Err("Selected rows do not contain valid ROWID values.".to_string());
        }
        Ok(rowids)
    }

    fn can_show_insert_row_action(source_sql: &str) -> bool {
        if source_sql.trim().is_empty() {
            return false;
        }
        if !QueryExecutor::is_rowid_edit_eligible_query(source_sql) {
            return false;
        }
        Self::resolve_target_table(source_sql).is_ok()
    }

    fn can_show_rowid_edit_actions(headers: &[String], source_sql: &str) -> bool {
        if !Self::can_show_insert_row_action(source_sql) {
            return false;
        }
        Self::find_rowid_column_index(headers).is_some()
    }

    fn has_insert_privilege_for_edit_target(table_name: &str, editable_column: &str) -> bool {
        let Some(conn) = current_active_db_connection() else {
            return false;
        };
        let sql = format!(
            "INSERT INTO {table_name} ({editable_column}) SELECT {editable_column} FROM {table_name} WHERE 1 = 0"
        );
        conn.execute(&sql, &[]).is_ok()
    }

    pub fn is_edit_mode_enabled(&self) -> bool {
        self.edit_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some()
    }

    pub fn can_begin_edit_mode(&self) -> bool {
        if self.is_edit_mode_enabled() {
            return true;
        }

        let headers_snapshot = self
            .headers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let source_sql_text = self
            .source_sql
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if !Self::can_show_rowid_edit_actions(&headers_snapshot, &source_sql_text) {
            return false;
        }
        let Some(rowid_col) = Self::find_rowid_column_index(&headers_snapshot) else {
            return false;
        };
        let source_sql_text = self
            .source_sql
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if Self::resolve_target_table(&source_sql_text).is_err() {
            return false;
        }
        let editable_columns: Vec<(usize, String)> = headers_snapshot
            .iter()
            .enumerate()
            .filter(|(idx, _)| *idx != rowid_col)
            .filter_map(|(idx, name)| Self::editable_column_identifier(name).map(|id| (idx, id)))
            .collect();
        if editable_columns.is_empty() {
            return false;
        }

        let rows = self
            .full_data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut seen = HashSet::new();
        for row in rows.iter() {
            let Some(rowid) = row
                .get(rowid_col)
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())
            else {
                return false;
            };
            if !seen.insert(rowid.to_string()) {
                return false;
            }
        }
        true
    }

    pub fn begin_edit_mode(&mut self) -> Result<String, String> {
        if self.is_edit_mode_enabled() {
            return Ok("Edit mode is already enabled.".to_string());
        }

        let headers_snapshot = self
            .headers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if headers_snapshot.is_empty() {
            return Err("No result columns available for editing.".to_string());
        }

        let source_sql_text = self
            .source_sql
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let table_name = Self::resolve_target_table(&source_sql_text)?;
        let rowid_col = Self::find_rowid_column_index(&headers_snapshot)
            .ok_or_else(|| "Editing requires a ROWID column in the result set.".to_string())?;

        let editable_columns: Vec<(usize, String)> = headers_snapshot
            .iter()
            .enumerate()
            .filter(|(idx, _)| *idx != rowid_col)
            .filter_map(|(idx, name)| Self::editable_column_identifier(name).map(|id| (idx, id)))
            .collect();
        if editable_columns.is_empty() {
            return Err("No editable columns were detected in this result set.".to_string());
        }

        let full_data_snapshot = self
            .full_data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let mut original_rows_by_rowid = HashMap::new();
        let mut original_row_order = Vec::with_capacity(full_data_snapshot.len());
        let mut row_states = Vec::with_capacity(full_data_snapshot.len());
        for (row_idx, row) in full_data_snapshot.iter().enumerate() {
            let rowid = row
                .get(rowid_col)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    format!(
                        "Row {} cannot be edited because ROWID is missing or empty.",
                        row_idx + 1
                    )
                })?;
            if original_rows_by_rowid.contains_key(&rowid) {
                return Err(format!(
                    "Edit mode requires unique ROWID values (duplicate: {}).",
                    rowid
                ));
            }
            original_rows_by_rowid.insert(rowid.clone(), row.clone());
            original_row_order.push(rowid.clone());
            row_states.push(EditRowState::Existing { rowid });
        }

        *self
            .edit_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(TableEditSession {
            rowid_col,
            table_name,
            editable_columns,
            original_rows_by_rowid,
            original_row_order,
            deleted_rowids: Vec::new(),
            row_states,
        });

        self.refresh_auto_rowid_visibility();
        Ok("Edit mode enabled.".to_string())
    }

    pub fn insert_row_in_edit_mode(&mut self) -> Result<String, String> {
        let headers_len = self
            .headers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len();
        if headers_len == 0 {
            return Err("No result columns available for INSERT.".to_string());
        }

        let (rowid_col, first_edit_col, table_name, first_editable_column) = {
            let guard = self
                .edit_session
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(session) = guard.as_ref() else {
                return Err("Enable edit mode first.".to_string());
            };
            (
                session.rowid_col,
                session.editable_columns.first().map(|(idx, _)| *idx),
                session.table_name.clone(),
                session
                    .editable_columns
                    .first()
                    .map(|(_, column)| column.clone())
                    .unwrap_or_default(),
            )
        };

        if first_editable_column.is_empty()
            || !Self::has_insert_privilege_for_edit_target(&table_name, &first_editable_column)
        {
            return Err(
                "INSERT requires table INSERT privilege and at least one writable target column."
                    .to_string(),
            );
        }

        let new_row_index = {
            let mut full_data = self
                .full_data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let new_row_index = full_data.len();
            let mut row = vec![String::new(); headers_len];
            if rowid_col < row.len() {
                row[rowid_col].clear();
            }
            full_data.push(row);
            new_row_index
        };
        {
            let mut guard = self
                .edit_session
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(session) = guard.as_mut() else {
                let mut full_data = self
                    .full_data
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if new_row_index < full_data.len() {
                    full_data.remove(new_row_index);
                }
                return Err("Edit mode is no longer active.".to_string());
            };
            session.row_states.push(EditRowState::Inserted);
        }

        self.table.set_rows((new_row_index + 1) as i32);
        self.apply_table_metrics_for_current_font();

        if let Some(first_col) = first_edit_col {
            self.table.set_selection(
                new_row_index as i32,
                first_col as i32,
                new_row_index as i32,
                first_col as i32,
            );
            let profile = *self
                .font_profile
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let size = *self
                .font_size
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(value) = Self::show_inline_cell_editor(
                &self.table,
                new_row_index as i32,
                first_col as i32,
                "",
                profile,
                size,
            ) {
                let mut full_data = self
                    .full_data
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if let Some(row) = full_data.get_mut(new_row_index) {
                    if first_col >= row.len() {
                        row.resize(first_col + 1, String::new());
                    }
                    row[first_col] = value;
                }
            } else {
                {
                    let mut full_data = self
                        .full_data
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if new_row_index < full_data.len() {
                        full_data.remove(new_row_index);
                    }
                }
                {
                    let mut guard = self
                        .edit_session
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if let Some(session) = guard.as_mut() {
                        if session.row_states.last().is_some() {
                            session.row_states.pop();
                        }
                    }
                }

                let new_len = self
                    .full_data
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .len();
                self.table.set_rows(new_len as i32);
                if new_len > 0 {
                    let row = (new_row_index).min(new_len.saturating_sub(1)) as i32;
                    let col = self.table.get_selection().1.max(0);
                    self.table.set_selection(row, col, row, col);
                }
                self.apply_table_metrics_for_current_font();
                self.table.redraw();
                return Ok("Cancelled row insertion and removed staged row.".to_string());
            }
        }

        self.table.redraw();
        Ok("Inserted a new staged row.".to_string())
    }

    pub fn delete_selected_rows_in_edit_mode(&mut self) -> Result<String, String> {
        let (row_start, row_end) = Self::selected_row_range(&self.table)
            .ok_or_else(|| "Select row(s) to delete.".to_string())?;

        let mut removed = 0usize;
        {
            let mut full_data = self
                .full_data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut guard = self
                .edit_session
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(session) = guard.as_mut() else {
                return Err("Enable edit mode first.".to_string());
            };

            if full_data.len() != session.row_states.len() {
                return Err("Edit session and table rows are out of sync.".to_string());
            }

            let mut deleted_set: HashSet<String> = session.deleted_rowids.iter().cloned().collect();
            let end = row_end.min(full_data.len().saturating_sub(1));
            for idx in (row_start..=end).rev() {
                if idx >= full_data.len() || idx >= session.row_states.len() {
                    continue;
                }
                if let EditRowState::Existing { rowid } = &session.row_states[idx] {
                    if deleted_set.insert(rowid.clone()) {
                        session.deleted_rowids.push(rowid.clone());
                    }
                }
                full_data.remove(idx);
                session.row_states.remove(idx);
                removed += 1;
            }
        }

        let new_len = self
            .full_data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len();
        self.table.set_rows(new_len as i32);
        self.apply_table_metrics_for_current_font();
        if new_len > 0 {
            let row = row_start.min(new_len.saturating_sub(1)) as i32;
            let col = self.table.get_selection().1.max(0);
            self.table.set_selection(row, col, row, col);
        }
        self.table.redraw();
        Ok(format!("Staged delete for {} row(s).", removed))
    }

    pub fn save_edit_mode(&mut self) -> Result<String, String> {
        let session = self
            .edit_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .cloned()
            .ok_or_else(|| "Enable edit mode first.".to_string())?;
        let source_sql_text = self
            .source_sql
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let rows = self
            .full_data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();

        if rows.len() != session.row_states.len() {
            return Err("Edit session and table rows are out of sync.".to_string());
        }

        let mut statements = Vec::new();

        if !session.deleted_rowids.is_empty() {
            let delete_where = if session.deleted_rowids.len() == 1 {
                format!(
                    "ROWID = {}",
                    Self::sql_string_literal(&session.deleted_rowids[0])
                )
            } else {
                let rowid_literals = session
                    .deleted_rowids
                    .iter()
                    .map(|rowid| Self::sql_string_literal(rowid))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("ROWID IN ({rowid_literals})")
            };
            statements.push(format!(
                "DELETE FROM {} WHERE {}",
                Self::quote_qualified_identifier(&session.table_name),
                delete_where
            ));
        }

        for (idx, row_state) in session.row_states.iter().enumerate() {
            let Some(row) = rows.get(idx) else {
                continue;
            };
            match row_state {
                EditRowState::Existing { rowid } => {
                    let Some(original_row) = session.original_rows_by_rowid.get(rowid) else {
                        continue;
                    };
                    let mut assignments = Vec::new();
                    for (col_idx, column_id) in session.editable_columns.iter() {
                        let new_value = row.get(*col_idx).cloned().unwrap_or_default();
                        let old_value = original_row.get(*col_idx).cloned().unwrap_or_default();
                        if new_value != old_value {
                            assignments.push(format!(
                                "{} = {}",
                                column_id,
                                Self::sql_literal_from_input(&new_value)
                            ));
                        }
                    }
                    if !assignments.is_empty() {
                        statements.push(format!(
                            "UPDATE {} SET {} WHERE ROWID = {}",
                            Self::quote_qualified_identifier(&session.table_name),
                            assignments.join(", "),
                            Self::sql_string_literal(rowid)
                        ));
                    }
                }
                EditRowState::Inserted => {
                    let mut column_names = Vec::new();
                    let mut values = Vec::new();
                    for (col_idx, column_id) in session.editable_columns.iter() {
                        column_names.push(column_id.clone());
                        let value = row.get(*col_idx).cloned().unwrap_or_default();
                        values.push(Self::sql_literal_from_input(&value));
                    }
                    if !column_names.is_empty() {
                        statements.push(format!(
                            "INSERT INTO {} ({}) VALUES ({})",
                            Self::quote_qualified_identifier(&session.table_name),
                            column_names.join(", "),
                            values.join(", ")
                        ));
                    }
                }
            }
        }

        if statements.is_empty() {
            *self
                .edit_session
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
            self.refresh_auto_rowid_visibility();
            return Ok("No staged changes to save.".to_string());
        }

        let mut script = statements.join(";\n");
        script.push(';');
        let select_sql = source_sql_text.trim().trim_end_matches(';').trim();
        if !select_sql.is_empty() {
            script.push('\n');
            script.push_str(select_sql);
        }
        Self::try_execute_sql(&self.execute_sql_callback, script)?;
        *self
            .edit_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        self.refresh_auto_rowid_visibility();
        Ok(format!("Applied {} staged statement(s).", statements.len()))
    }

    pub fn cancel_edit_mode(&mut self) -> Result<String, String> {
        let session = self
            .edit_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .ok_or_else(|| "Edit mode is not active.".to_string())?;

        let mut restored_rows = Vec::with_capacity(session.original_row_order.len());
        for rowid in session.original_row_order.iter() {
            if let Some(row) = session.original_rows_by_rowid.get(rowid) {
                restored_rows.push(row.clone());
            }
        }
        *self
            .full_data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = restored_rows;
        let new_len = self
            .full_data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len();
        self.table.set_rows(new_len as i32);
        self.apply_table_metrics_for_current_font();
        self.recalculate_widths_for_current_font();
        self.refresh_auto_rowid_visibility();
        self.table.redraw();
        Ok("Cancelled staged edits and restored original rows.".to_string())
    }

    #[allow(dead_code)]
    fn show_update_cell_dialog(
        table: &Table,
        headers: &Arc<Mutex<Vec<String>>>,
        full_data: &Arc<Mutex<Vec<Vec<String>>>>,
        source_sql: &Arc<Mutex<String>>,
        execute_sql_callback: &Arc<Mutex<Option<ResultGridSqlExecuteCallback>>>,
        context_cell: Option<(usize, usize)>,
    ) {
        let Some((row_index, col_index)) = Self::resolve_update_target_cell(
            table.get_selection(),
            table.rows().max(0) as usize,
            table.cols().max(0) as usize,
            context_cell,
        ) else {
            fltk::dialog::alert_default("Select a single cell to update.");
            return;
        };

        let headers_snapshot = headers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let source_sql_text = source_sql
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();

        let (rowid_col, rowid_value, current_value) = {
            let data_guard = full_data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let (rowid_col, rowid_value) =
                match Self::rowid_for_row(row_index, &headers_snapshot, &data_guard) {
                    Ok(v) => v,
                    Err(err) => {
                        fltk::dialog::alert_default(&err);
                        return;
                    }
                };
            let current_value = data_guard
                .get(row_index)
                .and_then(|row| row.get(col_index))
                .cloned()
                .unwrap_or_default();
            (rowid_col, rowid_value, current_value)
        };
        if col_index == rowid_col {
            fltk::dialog::alert_default("ROWID cell cannot be updated.");
            return;
        }

        let Some(column_name) = headers_snapshot.get(col_index).cloned() else {
            fltk::dialog::alert_default("Selected column is out of range.");
            return;
        };
        let Some(column_identifier) = Self::editable_column_identifier(&column_name) else {
            fltk::dialog::alert_default(
                "Selected column cannot be mapped to an editable column name.",
            );
            return;
        };

        let prompt = format!(
            "New value for {} (blank/NULL -> NULL, '=expr' -> SQL expression)",
            column_name
        );
        let Some(input) = fltk::dialog::input_default(&prompt, &current_value) else {
            return;
        };

        let table_name = match Self::resolve_target_table(&source_sql_text) {
            Ok(name) => name,
            Err(err) => {
                fltk::dialog::alert_default(&err);
                return;
            }
        };

        let sql = format!(
            "UPDATE {} SET {} = {} WHERE ROWID = {}",
            Self::quote_qualified_identifier(&table_name),
            column_identifier,
            Self::sql_literal_from_input(&input),
            Self::sql_string_literal(&rowid_value)
        );
        let script = Self::compose_edit_script(&sql, &source_sql_text);
        if let Err(err) = Self::try_execute_sql(execute_sql_callback, script) {
            fltk::dialog::alert_default(&err);
        }
    }

    #[allow(dead_code)]
    fn show_delete_row_dialog(
        table: &Table,
        headers: &Arc<Mutex<Vec<String>>>,
        full_data: &Arc<Mutex<Vec<Vec<String>>>>,
        source_sql: &Arc<Mutex<String>>,
        execute_sql_callback: &Arc<Mutex<Option<ResultGridSqlExecuteCallback>>>,
    ) {
        let headers_snapshot = headers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let source_sql_text = source_sql
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();

        let rowids = {
            let data_guard = full_data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match Self::selected_rowids(table, &headers_snapshot, &data_guard) {
                Ok(v) => v,
                Err(err) => {
                    fltk::dialog::alert_default(&err);
                    return;
                }
            }
        };

        let delete_count = rowids.len();
        let confirm = fltk::dialog::choice2_default(
            &format!("Delete {} selected row(s)?", delete_count),
            "Cancel",
            "Delete",
            "",
        );
        if confirm != Some(1) {
            return;
        }

        let table_name = match Self::resolve_target_table(&source_sql_text) {
            Ok(name) => name,
            Err(err) => {
                fltk::dialog::alert_default(&err);
                return;
            }
        };

        let where_clause = if rowids.len() == 1 {
            format!("ROWID = {}", Self::sql_string_literal(&rowids[0]))
        } else {
            let literals = rowids
                .iter()
                .map(|rowid| Self::sql_string_literal(rowid))
                .collect::<Vec<_>>()
                .join(", ");
            format!("ROWID IN ({literals})")
        };
        let sql = format!(
            "DELETE FROM {} WHERE {}",
            Self::quote_qualified_identifier(&table_name),
            where_clause
        );
        let script = Self::compose_edit_script(&sql, &source_sql_text);
        if let Err(err) = Self::try_execute_sql(execute_sql_callback, script) {
            fltk::dialog::alert_default(&err);
        }
    }

    #[allow(dead_code)]
    fn show_insert_row_dialog(
        table: &Table,
        headers: &Arc<Mutex<Vec<String>>>,
        full_data: &Arc<Mutex<Vec<Vec<String>>>>,
        source_sql: &Arc<Mutex<String>>,
        execute_sql_callback: &Arc<Mutex<Option<ResultGridSqlExecuteCallback>>>,
    ) {
        let headers_snapshot = headers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let source_sql_text = source_sql
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();

        let table_name = match Self::resolve_target_table(&source_sql_text) {
            Ok(name) => name,
            Err(err) => {
                fltk::dialog::alert_default(&err);
                return;
            }
        };

        let rowid_col = Self::find_rowid_column_index(&headers_snapshot);
        let editable_columns: Vec<(usize, String)> = headers_snapshot
            .iter()
            .enumerate()
            .filter(|(idx, _)| Some(*idx) != rowid_col)
            .filter_map(|(idx, name)| {
                Self::editable_column_identifier(name)
                    .map(|column_identifier| (idx, column_identifier))
            })
            .collect();
        if editable_columns.is_empty() {
            fltk::dialog::alert_default("No editable columns are available for INSERT.");
            return;
        }

        let selected_row = Self::selected_row(table);
        let selected_row_values = selected_row.and_then(|row_index| {
            full_data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(row_index)
                .cloned()
        });
        let mut value_literals: Vec<String> = Vec::with_capacity(editable_columns.len());
        let mut column_names: Vec<String> = Vec::with_capacity(editable_columns.len());
        for (col_idx, column_identifier) in editable_columns {
            let Some(column_name) = headers_snapshot.get(col_idx).cloned() else {
                continue;
            };
            let default_value = selected_row_values
                .as_ref()
                .and_then(|row| row.get(col_idx))
                .cloned()
                .unwrap_or_default();
            let prompt = format!(
                "Value for {} (blank/NULL -> NULL, '=expr' -> SQL expression)",
                column_name
            );
            let Some(input) = fltk::dialog::input_default(&prompt, &default_value) else {
                return;
            };
            column_names.push(column_identifier);
            value_literals.push(Self::sql_literal_from_input(&input));
        }

        if column_names.is_empty() || value_literals.is_empty() {
            fltk::dialog::alert_default("No values were provided for INSERT.");
            return;
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            Self::quote_qualified_identifier(&table_name),
            column_names.join(", "),
            value_literals.join(", ")
        );
        let script = Self::compose_edit_script(&sql, &source_sql_text);
        if let Err(err) = Self::try_execute_sql(execute_sql_callback, script) {
            fltk::dialog::alert_default(&err);
        }
    }

    fn show_context_menu(
        table: &Table,
        headers: &Arc<Mutex<Vec<String>>>,
        full_data: &Arc<Mutex<Vec<Vec<String>>>>,
        hidden_auto_rowid_col: &Arc<Mutex<Option<usize>>>,
        _source_sql: &Arc<Mutex<String>>,
        _execute_sql_callback: &Arc<Mutex<Option<ResultGridSqlExecuteCallback>>>,
        _edit_session: &Arc<Mutex<Option<TableEditSession>>>,
    ) {
        let mouse_x = app::event_x();
        let mouse_y = app::event_y();

        let mut table = table.clone();
        let clicked_cell = Self::get_cell_at_mouse(&table);
        let clicked_row_header = if clicked_cell.is_none() {
            Self::get_row_header_at_mouse(&table)
        } else {
            None
        };

        if clicked_cell.is_none() && clicked_row_header.is_none() {
            return;
        }

        // Give focus and potentially select cell under mouse for better UX
        let _ = table.take_focus();
        if let Some((row, col)) = clicked_cell {
            // If the cell under mouse is not already in the selection, select it.
            if !Self::selection_contains_cell(table.get_selection(), row, col) {
                table.set_selection(row, col, row, col);
                table.redraw();
            }
        } else if let Some(row) = clicked_row_header {
            let cols = table.cols();
            if cols <= 0 {
                return;
            }
            // Row-header context actions (delete/insert defaults) should target the clicked row.
            table.set_selection(row, 0, row, cols.saturating_sub(1));
            table.redraw();
        }

        // Prevent menu from being added to parent container
        let current_group = fltk::group::Group::try_current();
        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let mut menu = MenuButton::new(mouse_x, mouse_y, 0, 0, None);
        menu.set_color(theme::panel_raised());
        menu.set_text_color(theme::text_primary());
        let menu_items = vec!["Copy", "Copy with Headers", "Copy All"];
        menu.add_choice(&menu_items.join("|"));

        if let Some(ref group) = current_group {
            fltk::group::Group::set_current(Some(group));
        }

        if let Some(choice) = menu.popup() {
            let hidden_col = *hidden_auto_rowid_col
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let choice_label = choice.label().unwrap_or_default();
            match choice_label.as_str() {
                "Copy" => {
                    Self::copy_selected_to_clipboard(&table, full_data, hidden_col);
                }
                "Copy with Headers" => {
                    Self::copy_selected_with_headers(&table, headers, full_data, hidden_col);
                }
                "Copy All" => Self::copy_all_to_clipboard(headers, full_data, hidden_col),
                _ => {}
            }
        }

        MenuButton::delete(menu);
    }

    fn copy_selected_to_clipboard(
        table: &Table,
        full_data: &Arc<Mutex<Vec<Vec<String>>>>,
        hidden_col: Option<usize>,
    ) -> usize {
        let Some((row_top, col_left, row_bot, col_right)) =
            Self::normalized_selection_bounds_with_limits(
                table.get_selection(),
                table.rows().max(0) as usize,
                table.cols().max(0) as usize,
            )
        else {
            return 0;
        };

        let rows = (row_bot - row_top + 1) as usize;
        let visible_cols = Self::visible_column_indices_in_range(
            col_left as usize,
            col_right as usize,
            hidden_col,
        );
        if visible_cols.is_empty() {
            return 0;
        }
        let cell_count = rows * visible_cols.len();

        let full_data = full_data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut result = String::with_capacity(rows * visible_cols.len() * 16);
        for row in row_top..=row_bot {
            if row > row_top {
                result.push('\n');
            }
            for (visible_idx, col) in visible_cols.iter().enumerate() {
                if visible_idx > 0 {
                    result.push('\t');
                }
                if let Some(val) = full_data.get(row as usize).and_then(|r| r.get(*col)) {
                    result.push_str(val);
                }
            }
        }

        if !result.is_empty() {
            app::copy(&result);
            cell_count
        } else {
            0
        }
    }

    fn copy_selected_with_headers(
        table: &Table,
        headers: &Arc<Mutex<Vec<String>>>,
        full_data: &Arc<Mutex<Vec<Vec<String>>>>,
        hidden_col: Option<usize>,
    ) -> usize {
        let Some((row_top, col_left, row_bot, col_right)) =
            Self::normalized_selection_bounds_with_limits(
                table.get_selection(),
                table.rows().max(0) as usize,
                table.cols().max(0) as usize,
            )
        else {
            return 0;
        };

        let rows = (row_bot - row_top + 1) as usize;
        let visible_cols = Self::visible_column_indices_in_range(
            col_left as usize,
            col_right as usize,
            hidden_col,
        );
        if visible_cols.is_empty() {
            return 0;
        }
        let cell_count = rows * visible_cols.len();
        let mut result = String::with_capacity((rows + 1) * visible_cols.len() * 16);

        {
            let headers = headers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for (visible_idx, col) in visible_cols.iter().enumerate() {
                if visible_idx > 0 {
                    result.push('\t');
                }
                if let Some(h) = headers.get(*col) {
                    result.push_str(h);
                }
            }
        }
        result.push('\n');

        {
            let full_data = full_data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for row in row_top..=row_bot {
                for (visible_idx, col) in visible_cols.iter().enumerate() {
                    if visible_idx > 0 {
                        result.push('\t');
                    }
                    if let Some(val) = full_data.get(row as usize).and_then(|r| r.get(*col)) {
                        result.push_str(val);
                    }
                }
                result.push('\n');
            }
        }

        if !result.is_empty() {
            app::copy(&result);
            cell_count
        } else {
            0
        }
    }

    fn copy_all_to_clipboard(
        headers: &Arc<Mutex<Vec<String>>>,
        full_data: &Arc<Mutex<Vec<Vec<String>>>>,
        hidden_col: Option<usize>,
    ) {
        let header_values = {
            let headers = headers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            Self::visible_headers(&headers, hidden_col)
        };
        let header_line = header_values.join("\t");

        let row_count = full_data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len();
        let mut result = String::with_capacity(row_count * 16 + header_line.len() + 1);

        result.push_str(&header_line);
        result.push('\n');

        let full_data = full_data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for row in full_data.iter() {
            let visible_row = Self::visible_row_values_internal(row, hidden_col);
            for (i, cell) in visible_row.iter().enumerate() {
                if i > 0 {
                    result.push('\t');
                }
                result.push_str(cell);
            }
            result.push('\n');
        }

        if !result.is_empty() {
            app::copy(&result);
        }
    }

    pub fn display_result(&mut self, result: &QueryResult) {
        *self
            .edit_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        *self
            .source_sql
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = if result.is_select {
            result.sql.clone()
        } else {
            String::new()
        };
        if !result.is_select {
            let font_size = *self
                .font_size
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let max_cell_display_chars = *self
                .max_cell_display_chars
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.table.set_rows(1);
            self.table.set_cols(1);
            self.apply_table_metrics_for_current_font();
            let message_width =
                Self::estimate_display_width(&result.message, font_size, max_cell_display_chars)
                    .max(200)
                    .min(1200);
            self.table.set_col_width(0, message_width);
            *self
                .headers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = vec!["Result".to_string()];
            *self
                .full_data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                vec![vec![result.message.clone()]];
            *self
                .hidden_auto_rowid_col
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
            self.table.redraw();
            return;
        }

        if result.rows.is_empty() && result.row_count > 0 && self.table.rows() > 0 {
            let col_names: Vec<String> = result.columns.iter().map(|c| c.name.clone()).collect();
            let col_count = col_names.len() as i32;
            if self.table.cols() < col_count {
                self.table.set_cols(col_count);
            }
            self.apply_table_metrics_for_current_font();
            *self
                .headers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = col_names;
            self.refresh_auto_rowid_visibility();
            self.table.redraw();
            return;
        }

        let col_names: Vec<String> = result.columns.iter().map(|c| c.name.clone()).collect();
        let row_count = result.rows.len() as i32;
        let col_count = col_names.len() as i32;

        // Update table dimensions — no internal CellMatrix to rebuild
        self.table.set_rows(row_count);
        self.table.set_cols(col_count);
        self.apply_table_metrics_for_current_font();

        let font_size = *self
            .font_size
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let max_cell_display_chars = *self
            .max_cell_display_chars
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let widths = Self::compute_column_widths(
            &col_names,
            &result.rows,
            font_size,
            max_cell_display_chars,
        );
        self.apply_widths_to_table(&widths);
        *self
            .pending_widths
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = widths;

        // Store data directly — draw_cell reads from full_data on demand.
        // No per-cell set_cell_value calls needed!
        *self
            .full_data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = result.rows.clone();
        *self
            .headers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = col_names;
        self.refresh_auto_rowid_visibility();
        self.table.redraw();
    }

    pub fn start_streaming(&mut self, headers: &[String]) {
        *self
            .edit_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        let col_count = headers.len() as i32;

        // Clear any pending data from previous queries
        self.pending_rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        self.pending_widths
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        self.full_data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        *self
            .last_flush
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Instant::now();
        *self
            .width_sampled_rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = 0;
        self.source_sql
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        *self
            .hidden_auto_rowid_col
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;

        // Initialize pending widths based on headers
        let font_size = *self
            .font_size
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let initial_widths: Vec<i32> = headers
            .iter()
            .map(|h| Self::estimate_text_width(h, font_size))
            .collect();
        *self
            .pending_widths
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = initial_widths.clone();

        self.table.set_rows(0);
        self.table.set_cols(col_count);
        self.apply_table_metrics_for_current_font();

        for (i, _name) in headers.iter().enumerate() {
            self.table.set_col_width(i as i32, initial_widths[i]);
        }
        self.apply_hidden_rowid_column_width();

        *self
            .headers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = headers.to_vec();
        self.table.redraw();
    }

    /// Append rows to the buffer. UI is updated periodically for performance.
    pub fn append_rows(&mut self, rows: Vec<Vec<String>>) {
        // Only compute column widths for the first WIDTH_SAMPLE_ROWS rows
        let sampled = *self
            .width_sampled_rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if sampled < WIDTH_SAMPLE_ROWS {
            let max_cols = rows.iter().map(|row| row.len()).max().unwrap_or(0);
            let mut widths = self
                .pending_widths
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let min_width = Self::min_col_width_for_font(
                *self
                    .font_size
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
            );
            let max_cell_display_chars = *self
                .max_cell_display_chars
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if widths.len() < max_cols {
                widths.resize(max_cols, min_width);
            }
            let remaining = WIDTH_SAMPLE_ROWS - sampled;
            let sample_count = rows.len().min(remaining);
            for row in rows[..sample_count].iter() {
                Self::update_widths_with_row(
                    &mut widths,
                    row,
                    *self
                        .font_size
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()),
                    max_cell_display_chars,
                );
            }
            drop(widths);
            *self
                .width_sampled_rows
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = sampled + sample_count;
        }

        // Add rows to pending buffer
        self.pending_rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend(rows);

        // Check if we should flush to UI
        let should_flush = {
            let elapsed = self
                .last_flush
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .elapsed();
            let buffered_count = self
                .pending_rows
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len();
            elapsed >= UI_UPDATE_INTERVAL || buffered_count >= MAX_BUFFERED_ROWS
        };

        if should_flush {
            self.flush_pending();
        }
    }

    /// Flush all pending rows to the UI.
    /// Data is moved (not cloned) from pending_rows into full_data.
    /// Only the table row count is updated — draw_cell handles rendering on demand.
    pub fn flush_pending(&mut self) {
        let rows_to_add: Vec<Vec<String>> = self
            .pending_rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .drain(..)
            .collect();
        if rows_to_add.is_empty() {
            return;
        }

        let new_rows_count = rows_to_add.len() as i32;
        let current_rows = self.table.rows();
        let new_total = current_rows + new_rows_count;

        // Update column widths
        {
            let widths = self
                .pending_widths
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let max_cols = widths.len().max(self.table.cols() as usize);
            if max_cols as i32 > self.table.cols() {
                self.table.set_cols(max_cols as i32);
            }
            for (col_idx, &width) in widths.iter().enumerate() {
                if col_idx < max_cols {
                    let current_width = self.table.col_width(col_idx as i32);
                    if width > current_width {
                        self.table.set_col_width(col_idx as i32, width);
                    }
                }
            }
        }
        self.apply_hidden_rowid_column_width();

        // Move data into full_data — zero-copy, no clone!
        self.full_data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend(rows_to_add);

        // Just update row count — draw_cell reads from full_data on demand
        self.table.set_rows(new_total);
        self.apply_table_metrics_for_current_font();

        *self
            .last_flush
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Instant::now();
        self.table.redraw();
    }

    /// Call this when streaming is complete to flush any remaining buffered rows
    pub fn finish_streaming(&mut self) {
        self.flush_pending();
        self.table.redraw();
    }

    #[allow(dead_code)]
    pub fn clear(&mut self) {
        *self
            .edit_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        self.table.set_rows(0);
        self.table.set_cols(0);
        {
            let mut headers = self
                .headers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            headers.clear();
            headers.shrink_to_fit();
        }
        {
            let mut pending_rows = self
                .pending_rows
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            pending_rows.clear();
            pending_rows.shrink_to_fit();
        }
        {
            let mut pending_widths = self
                .pending_widths
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            pending_widths.clear();
            pending_widths.shrink_to_fit();
        }
        {
            let mut full_data = self
                .full_data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            full_data.clear();
            full_data.shrink_to_fit();
        }
        *self
            .width_sampled_rows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = 0;
        *self
            .last_flush
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Instant::now();
        *self
            .hidden_auto_rowid_col
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        self.table.redraw();
    }

    pub fn copy(&self) -> usize {
        let Some((row_top, col_left, row_bot, col_right)) =
            Self::normalized_selection_bounds_with_limits(
                self.table.get_selection(),
                self.table.rows().max(0) as usize,
                self.table.cols().max(0) as usize,
            )
        else {
            return 0;
        };
        let hidden_col = self.hidden_auto_rowid_col_value();
        let count = Self::copy_selected_to_clipboard(&self.table, &self.full_data, hidden_col);
        if count > 0 {
            let rows = (row_bot - row_top + 1) as usize;
            let cols = Self::visible_column_indices_in_range(
                col_left as usize,
                col_right as usize,
                hidden_col,
            )
            .len();
            println!("Copied {} cells ({} rows x {} cols)", count, rows, cols);
        }
        count
    }

    pub fn copy_with_headers(&self) {
        Self::copy_selected_with_headers(
            &self.table,
            &self.headers,
            &self.full_data,
            self.hidden_auto_rowid_col_value(),
        );
    }

    pub fn select_all(&mut self) {
        let rows = self.table.rows();
        let cols = self.table.cols();
        if rows > 0 && cols > 0 {
            self.table.set_selection(0, 0, rows - 1, cols - 1);
            self.table.redraw();
        }
    }

    pub fn paste_from_clipboard(&mut self) {
        let _ = self.table.take_focus();
        app::paste_text(&self.table);
    }

    #[allow(dead_code)]
    pub fn get_selected_data(&self) -> Option<String> {
        let Some((row_top, col_left, row_bot, col_right)) =
            Self::normalized_selection_bounds_with_limits(
                self.table.get_selection(),
                self.table.rows().max(0) as usize,
                self.table.cols().max(0) as usize,
            )
        else {
            return None;
        };

        let full_data = self
            .full_data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let rows = (row_bot - row_top + 1) as usize;
        let hidden_col = self.hidden_auto_rowid_col_value();
        let visible_cols = Self::visible_column_indices_in_range(
            col_left as usize,
            col_right as usize,
            hidden_col,
        );
        if visible_cols.is_empty() {
            return None;
        }
        let mut result = String::with_capacity(rows * visible_cols.len() * 16);
        for row in row_top..=row_bot {
            if row > row_top {
                result.push('\n');
            }
            for (visible_idx, col) in visible_cols.iter().enumerate() {
                if visible_idx > 0 {
                    result.push('\t');
                }
                if let Some(val) = full_data.get(row as usize).and_then(|r| r.get(*col)) {
                    result.push_str(val);
                }
            }
        }

        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    /// Export all data to CSV format
    pub fn export_to_csv(&self) -> String {
        let line_ending = Self::csv_line_ending();
        let hidden_col = self.hidden_auto_rowid_col_value();
        let header_line = {
            let headers = self
                .headers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let escaped: Vec<String> = Self::visible_headers(&headers, hidden_col)
                .iter()
                .map(|h| Self::escape_csv_field(h))
                .collect();
            escaped.join(",")
        };

        let row_count = self
            .full_data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len();
        let mut csv = String::with_capacity(row_count * 20 + header_line.len() + 1);

        csv.push_str(&header_line);
        csv.push_str(line_ending);

        let full_data = self
            .full_data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for row in full_data.iter() {
            let visible_row = Self::visible_row_values_internal(row, hidden_col);
            for (i, cell) in visible_row.iter().enumerate() {
                if i > 0 {
                    csv.push(',');
                }
                csv.push_str(&Self::escape_csv_field(cell));
            }
            csv.push_str(line_ending);
        }

        csv
    }

    fn csv_line_ending() -> &'static str {
        if cfg!(windows) {
            "\r\n"
        } else {
            "\n"
        }
    }

    fn escape_csv_field(field: &str) -> String {
        if field.contains(',')
            || field.contains('"')
            || field.contains('\n')
            || field.contains('\r')
        {
            format!("\"{}\"", field.replace('"', "\"\""))
        } else {
            field.to_string()
        }
    }

    pub fn row_count(&self) -> usize {
        self.table.rows() as usize
    }

    pub fn has_data(&self) -> bool {
        self.table.rows() > 0
    }

    pub fn columns(&self) -> Vec<String> {
        let headers = self
            .headers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        Self::visible_headers(&headers, self.hidden_auto_rowid_col_value())
    }

    pub fn row_values(&self, row: usize) -> Option<Vec<String>> {
        self.full_data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(row)
            .map(|row_values| {
                Self::visible_row_values_internal(row_values, self.hidden_auto_rowid_col_value())
            })
    }

    pub fn get_widget(&self) -> Table {
        self.table.clone()
    }

    pub fn set_source_sql(&mut self, sql: &str) {
        *self
            .source_sql
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = sql.to_string();
        self.refresh_auto_rowid_visibility();
    }

    pub fn set_execute_sql_callback(&mut self, callback: Option<ResultGridSqlExecuteCallback>) {
        *self
            .execute_sql_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = callback;
    }

    pub fn apply_font_settings(&mut self, profile: FontProfile, size: u32) {
        *self
            .font_profile
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = profile;
        *self
            .font_size
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = size;
        self.apply_table_metrics_for_current_font();
        self.recalculate_widths_for_current_font();
        // Force FLTK to recalculate the table's internal layout after
        // row height / column width changes from the new font metrics.
        let (x, y, w, h) = (
            self.table.x(),
            self.table.y(),
            self.table.w(),
            self.table.h(),
        );
        self.table.resize(x, y, w, h);
        self.table.redraw();
    }

    pub fn set_max_cell_display_chars(&mut self, max_chars: usize) {
        *self
            .max_cell_display_chars
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = max_chars.max(1);
        self.recalculate_widths_for_current_font();
        self.table.redraw();
    }

    /// Cleanup method to release resources before the widget is deleted.
    pub fn cleanup(&mut self) {
        *self
            .edit_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        // Clear the event handler callback to release captured Arc<Mutex<T>> references.
        self.table.handle(|_, _| false);

        // Set an empty draw_cell to release captured Arc<Mutex<...>> references
        // from the virtual rendering callback.
        self.table.draw_cell(|_, _, _, _, _, _, _, _| {});

        // Reset table dimensions
        self.table.set_rows(0);
        self.table.set_cols(0);

        // Clear all data buffers to release memory
        {
            let mut headers = self
                .headers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            headers.clear();
            headers.shrink_to_fit();
        }
        {
            let mut pending_rows = self
                .pending_rows
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            pending_rows.clear();
            pending_rows.shrink_to_fit();
        }
        {
            let mut pending_widths = self
                .pending_widths
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            pending_widths.clear();
            pending_widths.shrink_to_fit();
        }
        {
            let mut full_data = self
                .full_data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            full_data.clear();
            full_data.shrink_to_fit();
        }
        self.source_sql
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        *self
            .hidden_auto_rowid_col
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        *self
            .execute_sql_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }
}

#[cfg(test)]
mod row_edit_sql_tests {
    use super::*;

    #[test]
    fn sql_literal_from_input_handles_null_numbers_and_expr() {
        assert_eq!(ResultTableWidget::sql_literal_from_input(""), "NULL");
        assert_eq!(ResultTableWidget::sql_literal_from_input("NULL"), "NULL");
        assert_eq!(ResultTableWidget::sql_literal_from_input("42"), "42");
        assert_eq!(ResultTableWidget::sql_literal_from_input("3.14"), "3.14");
        assert_eq!(
            ResultTableWidget::sql_literal_from_input("=sysdate"),
            "sysdate"
        );
        assert_eq!(
            ResultTableWidget::sql_literal_from_input("O'Reilly"),
            "'O''Reilly'"
        );
    }

    #[test]
    fn sql_literal_from_input_preserves_significant_string_whitespace() {
        assert_eq!(
            ResultTableWidget::sql_literal_from_input("  padded  "),
            "'  padded  '"
        );
        assert_eq!(
            ResultTableWidget::sql_literal_from_input(" = sysdate "),
            "sysdate"
        );
    }

    #[test]
    fn find_rowid_column_index_accepts_qualified_header() {
        let headers = vec!["E.ROWID".to_string(), "ENAME".to_string()];
        assert_eq!(
            ResultTableWidget::find_rowid_column_index(&headers),
            Some(0)
        );
    }

    #[test]
    fn find_rowid_column_index_rejects_internal_rowid_alias_without_normalization() {
        let headers = vec!["SQ_INTERNAL_ROWID".to_string(), "ENAME".to_string()];
        assert_eq!(ResultTableWidget::find_rowid_column_index(&headers), None);
    }

    #[test]
    fn resolve_target_table_uses_rowid_alias_resolution() {
        let sql = "SELECT e.ROWID, e.ENAME, d.DNAME FROM EMP e JOIN DEPT d ON d.DEPTNO = e.DEPTNO";
        assert_eq!(
            ResultTableWidget::resolve_target_table(sql),
            Ok("EMP".to_string())
        );
    }

    #[test]
    fn resolve_target_table_uses_quoted_rowid_alias_resolution() {
        let sql = r#"SELECT "e"."ROWID", "e"."ENAME", "d"."DNAME" FROM EMP "e" JOIN DEPT "d" ON "d"."DEPTNO" = "e"."DEPTNO""#;
        assert_eq!(
            ResultTableWidget::resolve_target_table(sql),
            Ok("EMP".to_string())
        );
    }

    #[test]
    fn resolve_target_table_rejects_ambiguous_multi_table_without_rowid_alias() {
        let sql = "SELECT ROWID, e.ENAME, d.DNAME FROM EMP e JOIN DEPT d ON d.DEPTNO = e.DEPTNO";
        let result = ResultTableWidget::resolve_target_table(sql);
        assert!(result.is_err());
    }

    #[test]
    fn resolve_target_table_resolves_join_with_qualified_rowid() {
        // This is the SQL after ROWID injection for a JOIN query
        let sql = "SELECT e.ROWID, e.ENAME, d.DNAME FROM EMP e JOIN DEPT d ON d.DEPTNO = e.DEPTNO";
        assert_eq!(
            ResultTableWidget::resolve_target_table(sql),
            Ok("EMP".to_string())
        );
    }

    #[test]
    fn resolve_target_table_resolves_comma_join_with_qualified_rowid() {
        let sql = "SELECT e.ROWID, ENAME FROM EMP e, DEPT d WHERE e.DEPTNO = d.DEPTNO";
        assert_eq!(
            ResultTableWidget::resolve_target_table(sql),
            Ok("EMP".to_string())
        );
    }

    #[test]
    fn resolve_target_table_resolves_with_clause_query() {
        let sql = "WITH dept_avg AS (SELECT DEPTNO, AVG(SAL) avg_sal FROM EMP GROUP BY DEPTNO) SELECT e.ROWID, ENAME FROM EMP e JOIN dept_avg d ON e.DEPTNO = d.DEPTNO";
        assert_eq!(
            ResultTableWidget::resolve_target_table(sql),
            Ok("EMP".to_string())
        );
    }

    #[test]
    fn resolve_target_table_resolves_left_join_with_qualified_rowid() {
        let sql =
            "SELECT e.ROWID, e.ENAME, d.DNAME FROM EMP e LEFT JOIN DEPT d ON e.DEPTNO = d.DEPTNO";
        assert_eq!(
            ResultTableWidget::resolve_target_table(sql),
            Ok("EMP".to_string())
        );
    }

    #[test]
    fn resolve_target_table_resolves_schema_qualified_table_with_alias() {
        let sql =
            "SELECT e.ROWID, e.ENAME FROM SCOTT.EMP e JOIN SCOTT.DEPT d ON e.DEPTNO = d.DEPTNO";
        assert_eq!(
            ResultTableWidget::resolve_target_table(sql),
            Ok("SCOTT.EMP".to_string())
        );
    }

    #[test]
    fn resolve_target_table_resolves_single_table_no_alias() {
        let sql = "SELECT EMP.ROWID, ENAME FROM EMP";
        assert_eq!(
            ResultTableWidget::resolve_target_table(sql),
            Ok("EMP".to_string())
        );
    }

    #[test]
    fn compose_edit_script_appends_source_select() {
        let script = ResultTableWidget::compose_edit_script(
            "UPDATE EMP SET ENAME = 'A' WHERE ROWID = 'AAA';",
            "SELECT ROWID, ENAME FROM EMP;",
        );
        assert_eq!(
            script,
            "UPDATE EMP SET ENAME = 'A' WHERE ROWID = 'AAA';\nSELECT ROWID, ENAME FROM EMP"
        );
    }

    #[test]
    fn last_identifier_segment_handles_qualified_and_quoted_identifiers() {
        assert_eq!(
            ResultTableWidget::last_identifier_segment("E.ENAME"),
            "ENAME"
        );
        assert_eq!(
            ResultTableWidget::last_identifier_segment("\"E\".\"EMP.NAME\""),
            "\"EMP.NAME\""
        );
        assert_eq!(
            ResultTableWidget::last_identifier_segment("  ENAME  "),
            "ENAME"
        );
    }

    #[test]
    fn editable_column_identifier_uses_base_column_segment() {
        assert_eq!(
            ResultTableWidget::editable_column_identifier("E.ENAME"),
            Some("ENAME".to_string())
        );
        assert_eq!(
            ResultTableWidget::editable_column_identifier("\"E\".\"User Name\""),
            Some("\"User Name\"".to_string())
        );
        assert_eq!(
            ResultTableWidget::editable_column_identifier("SCOTT.\"A.B\""),
            Some("\"A.B\"".to_string())
        );
        assert_eq!(ResultTableWidget::editable_column_identifier(""), None);
        assert_eq!(ResultTableWidget::editable_column_identifier("E."), None);
    }

    #[test]
    fn quote_qualified_identifier_preserves_dots_inside_quoted_segments() {
        assert_eq!(
            ResultTableWidget::quote_qualified_identifier(r#""SCHEMA.WITH.DOT"."TABLE.WITH.DOT""#),
            r#""SCHEMA.WITH.DOT"."TABLE.WITH.DOT""#
        );
        assert_eq!(
            ResultTableWidget::quote_qualified_identifier(r#""TABLE.WITH.DOT""#),
            r#""TABLE.WITH.DOT""#
        );
    }

    #[test]
    fn quote_qualified_identifier_keeps_unquoted_case_insensitive_identifiers_unquoted() {
        assert_eq!(
            ResultTableWidget::quote_qualified_identifier("scott.emp"),
            "scott.emp"
        );
        assert_eq!(
            ResultTableWidget::quote_qualified_identifier("MySchema.MyTable"),
            "MySchema.MyTable"
        );
    }

    #[test]
    fn push_unique_rowid_preserves_case_sensitive_values() {
        let mut rowids = Vec::new();
        let mut seen = HashSet::new();
        ResultTableWidget::push_unique_rowid(&mut rowids, &mut seen, "AAABbb");
        ResultTableWidget::push_unique_rowid(&mut rowids, &mut seen, "aaabbb");
        ResultTableWidget::push_unique_rowid(&mut rowids, &mut seen, " AAABbb ");
        assert_eq!(rowids, vec!["AAABbb".to_string(), "aaabbb".to_string()]);
    }

    #[test]
    fn resolve_update_target_cell_prefers_context_and_requires_single_selection_without_it() {
        assert_eq!(
            ResultTableWidget::resolve_update_target_cell((2, 3, 4, 5), 10, 10, Some((4, 5))),
            Some((4, 5))
        );
        assert_eq!(
            ResultTableWidget::resolve_update_target_cell((2, 3, 2, 3), 10, 10, None),
            Some((2, 3))
        );
        assert_eq!(
            ResultTableWidget::resolve_update_target_cell((2, 3, 4, 3), 10, 10, None),
            None
        );
    }

    #[test]
    fn resolved_selection_bounds_with_limits_clamps_to_current_table_size() {
        let bounds = ResultTableWidget::normalized_selection_bounds_with_limits((2, 3, 8, 9), 3, 4);
        assert_eq!(bounds, Some((2, 3, 2, 3)));
    }

    #[test]
    fn normalized_selection_bounds_with_limits_rejects_no_overlap_selection() {
        let bounds =
            ResultTableWidget::normalized_selection_bounds_with_limits((10, 0, 11, 1), 5, 5);
        assert_eq!(bounds, None);
    }

    #[test]
    fn resolve_update_target_cell_rejects_out_of_range_context_cell() {
        assert_eq!(
            ResultTableWidget::resolve_update_target_cell((2, 3, 2, 3), 1, 1, Some((3, 0))),
            None
        );
    }

    #[test]
    fn selection_contains_cell_normalizes_reversed_bounds() {
        assert!(ResultTableWidget::selection_contains_cell(
            (5, 6, 2, 3),
            4,
            5
        ));
        assert!(ResultTableWidget::selection_contains_cell(
            (5, 6, 2, 3),
            2,
            3
        ));
        assert!(!ResultTableWidget::selection_contains_cell(
            (5, 6, 2, 3),
            1,
            3
        ));
    }

    #[test]
    fn selection_contains_cell_rejects_negative_or_empty_selection() {
        assert!(!ResultTableWidget::selection_contains_cell(
            (-1, -1, -1, -1),
            0,
            0
        ));
        assert!(!ResultTableWidget::selection_contains_cell(
            (0, 0, 1, 1),
            -1,
            0
        ));
    }

    #[test]
    fn normalized_selection_bounds_reorders_reversed_selection() {
        assert_eq!(
            ResultTableWidget::normalized_selection_bounds((5, 6, 2, 3)),
            Some((2, 3, 5, 6))
        );
    }

    #[test]
    fn normalized_selection_bounds_rejects_negative_selection() {
        assert_eq!(
            ResultTableWidget::normalized_selection_bounds((-1, 6, 2, 3)),
            None
        );
        assert_eq!(
            ResultTableWidget::normalized_selection_bounds((2, 3, -1, 6)),
            None
        );
    }

    #[test]
    fn parse_clipboard_rows_normalizes_line_endings_and_trailing_newline() {
        let rows = ResultTableWidget::parse_clipboard_rows("A\tB\r\n1\t2\r\n");
        assert_eq!(
            rows,
            vec![
                vec!["A".to_string(), "B".to_string()],
                vec!["1".to_string(), "2".to_string()]
            ]
        );
    }

    #[test]
    fn apply_paste_values_to_data_fills_selection_for_single_value() {
        let mut data = vec![
            vec!["RID1".to_string(), "A".to_string(), "B".to_string()],
            vec!["RID2".to_string(), "C".to_string(), "D".to_string()],
        ];
        let editable_cols: HashSet<usize> = [1usize, 2usize].into_iter().collect();
        let changed = ResultTableWidget::apply_paste_values_to_data(
            &mut data,
            0,
            &editable_cols,
            (0, 1),
            Some((0, 1, 1, 2)),
            &[vec!["X".to_string()]],
        );
        assert_eq!(changed, 4);
        assert_eq!(
            data,
            vec![
                vec!["RID1".to_string(), "X".to_string(), "X".to_string()],
                vec!["RID2".to_string(), "X".to_string(), "X".to_string()],
            ]
        );
    }

    #[test]
    fn apply_paste_values_to_data_skips_rowid_and_non_editable_columns() {
        let mut data = vec![vec![
            "RID1".to_string(),
            "A".to_string(),
            "B".to_string(),
            "C".to_string(),
        ]];
        let editable_cols: HashSet<usize> = [1usize, 3usize].into_iter().collect();
        let changed = ResultTableWidget::apply_paste_values_to_data(
            &mut data,
            0,
            &editable_cols,
            (0, 0),
            None,
            &[vec![
                "R".to_string(),
                "X".to_string(),
                "Y".to_string(),
                "Z".to_string(),
            ]],
        );
        assert_eq!(changed, 2);
        assert_eq!(
            data,
            vec![vec![
                "RID1".to_string(),
                "X".to_string(),
                "B".to_string(),
                "Z".to_string(),
            ]]
        );
    }

    #[test]
    fn resolve_paste_anchor_column_prefers_editable_col_when_anchor_is_rowid() {
        let editable_cols: HashSet<usize> = [1usize, 2usize].into_iter().collect();
        let resolved = ResultTableWidget::resolve_paste_anchor_column(
            0,
            Some((0, 0, 0, 2)),
            0,
            &editable_cols,
            3,
        );
        assert_eq!(resolved, Some(1));
    }

    #[test]
    fn resolve_paste_anchor_column_keeps_anchor_when_already_editable() {
        let editable_cols: HashSet<usize> = [1usize, 3usize].into_iter().collect();
        let resolved = ResultTableWidget::resolve_paste_anchor_column(
            3,
            Some((0, 0, 0, 3)),
            0,
            &editable_cols,
            4,
        );
        assert_eq!(resolved, Some(3));
    }

    #[test]
    fn collect_rowids_in_range_errors_when_selected_row_lacks_rowid_cell() {
        let full_data = vec![vec!["AAABBB".to_string()], Vec::new()];
        let result = ResultTableWidget::collect_rowids_in_range(0, 1, 0, &full_data);
        assert!(result.is_err());
    }

    #[test]
    fn collect_rowids_in_range_errors_when_selected_row_has_empty_rowid() {
        let full_data = vec![vec!["AAABBB".to_string()], vec!["   ".to_string()]];
        let result = ResultTableWidget::collect_rowids_in_range(0, 1, 0, &full_data);
        assert!(result.is_err());
    }

    #[test]
    fn can_show_insert_row_action_requires_resolved_target() {
        assert!(ResultTableWidget::can_show_insert_row_action(
            "SELECT ENAME FROM EMP"
        ));
        assert!(!ResultTableWidget::can_show_insert_row_action("   "));
        // Unqualified ROWID in multi-table is ambiguous
        assert!(!ResultTableWidget::can_show_insert_row_action(
            "SELECT ROWID, e.ENAME, d.DNAME FROM EMP e JOIN DEPT d ON d.DEPTNO = e.DEPTNO"
        ));
        // JOIN result sets are not editable even with qualified ROWID.
        assert!(!ResultTableWidget::can_show_insert_row_action(
            "SELECT e.ROWID, e.ENAME, d.DNAME FROM EMP e JOIN DEPT d ON d.DEPTNO = e.DEPTNO"
        ));
    }

    #[test]
    fn can_show_rowid_edit_actions_requires_rowid_and_resolved_target() {
        let valid_headers = vec!["ROWID".to_string(), "ENAME".to_string()];
        assert!(ResultTableWidget::can_show_rowid_edit_actions(
            &valid_headers,
            "SELECT ROWID, ENAME FROM EMP"
        ));

        let missing_rowid_headers = vec!["ENAME".to_string()];
        assert!(!ResultTableWidget::can_show_rowid_edit_actions(
            &missing_rowid_headers,
            "SELECT ENAME FROM EMP"
        ));
        assert!(!ResultTableWidget::can_show_rowid_edit_actions(
            &valid_headers,
            "   "
        ));
        // Unqualified ROWID in multi-table is ambiguous
        assert!(!ResultTableWidget::can_show_rowid_edit_actions(
            &valid_headers,
            "SELECT ROWID, e.ENAME, d.DNAME FROM EMP e JOIN DEPT d ON d.DEPTNO = e.DEPTNO"
        ));
        // JOIN result sets are not editable even with qualified ROWID.
        let qualified_headers = vec!["E.ROWID".to_string(), "ENAME".to_string()];
        assert!(!ResultTableWidget::can_show_rowid_edit_actions(
            &qualified_headers,
            "SELECT e.ROWID, e.ENAME, d.DNAME FROM EMP e JOIN DEPT d ON d.DEPTNO = e.DEPTNO"
        ));
        let internal_alias_headers = vec!["SQ_INTERNAL_ROWID".to_string(), "ENAME".to_string()];
        assert!(!ResultTableWidget::can_show_rowid_edit_actions(
            &internal_alias_headers,
            "SELECT ENAME AS SQ_INTERNAL_ROWID, ENAME FROM EMP"
        ));
    }

    #[test]
    fn can_show_rowid_edit_actions_accepts_help_query_with_semicolon() {
        let headers = vec!["HELP.ROWID".to_string(), "TOPIC".to_string()];
        assert!(ResultTableWidget::can_show_rowid_edit_actions(
            &headers,
            "SELECT help.ROWID, help.* FROM help;"
        ));
    }

    #[test]
    fn detect_auto_hidden_rowid_col_hides_only_auto_injected_rowid() {
        let headers = vec!["EMP.ROWID".to_string(), "ENAME".to_string()];
        assert_eq!(
            ResultTableWidget::detect_auto_hidden_rowid_col(
                &headers,
                "SELECT ENAME FROM EMP",
                false,
            ),
            Some(0)
        );

        assert_eq!(
            ResultTableWidget::detect_auto_hidden_rowid_col(
                &headers,
                "SELECT ROWID, ENAME FROM EMP",
                false,
            ),
            None
        );
    }

    #[test]
    fn detect_auto_hidden_rowid_col_does_not_hide_while_edit_mode_enabled() {
        let headers = vec!["EMP.ROWID".to_string(), "ENAME".to_string()];
        assert_eq!(
            ResultTableWidget::detect_auto_hidden_rowid_col(
                &headers,
                "SELECT ENAME FROM EMP",
                true
            ),
            None
        );
    }

    #[test]
    fn try_execute_sql_returns_error_when_callback_is_missing() {
        let execute_sql_callback: Arc<Mutex<Option<ResultGridSqlExecuteCallback>>> =
            Arc::new(Mutex::new(None));
        let result = ResultTableWidget::try_execute_sql(
            &execute_sql_callback,
            "UPDATE EMP SET ENAME = 'A'".to_string(),
        );
        assert_eq!(result, Err("Edit callback is not connected.".to_string()));
    }

    #[test]
    fn try_execute_sql_invokes_registered_callback() {
        let captured_sql = Arc::new(Mutex::new(Vec::<String>::new()));
        let captured_sql_for_cb = captured_sql.clone();
        let callback: ResultGridSqlExecuteCallback = Arc::new(Mutex::new(Box::new(move |sql| {
            captured_sql_for_cb
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(sql);
        })));
        let execute_sql_callback: Arc<Mutex<Option<ResultGridSqlExecuteCallback>>> =
            Arc::new(Mutex::new(Some(callback)));
        let sql = "DELETE FROM EMP WHERE ROWID = 'AAABBB'".to_string();

        let result = ResultTableWidget::try_execute_sql(&execute_sql_callback, sql.clone());
        assert!(result.is_ok());
        assert_eq!(
            captured_sql
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_slice(),
            &[sql]
        );
    }
}

impl Default for ResultTableWidget {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_shortcut_key_accepts_current_ascii_key() {
        assert!(ResultTableWidget::matches_shortcut_key(
            Key::from_char('c'),
            Key::from_char('x'),
            'c',
        ));
    }

    #[test]
    fn matches_shortcut_key_accepts_original_ascii_key() {
        assert!(ResultTableWidget::matches_shortcut_key(
            Key::from_char('ㅊ'),
            Key::from_char('c'),
            'c',
        ));
    }

    #[test]
    fn escape_csv_field_quotes_carriage_return_values() {
        assert_eq!(
            ResultTableWidget::escape_csv_field("line1\rline2"),
            "\"line1\rline2\""
        );
    }

    #[test]
    fn csv_line_ending_matches_target_platform() {
        let expected = if cfg!(windows) { "\r\n" } else { "\n" };
        assert_eq!(ResultTableWidget::csv_line_ending(), expected);
    }
}
