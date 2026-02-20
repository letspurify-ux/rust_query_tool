use fltk::{
    app,
    button::Button,
    draw,
    enums::{Align, Event, FrameType, Key, Shortcut},
    group::Group,
    menu::MenuButton,
    prelude::*,
    table::{Table, TableContext},
    text::{TextBuffer, TextDisplay},
    window::Window,
};
use std::cell::Cell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::db::QueryResult;
use crate::ui::constants::*;
use crate::ui::font_settings::{configured_editor_profile, FontProfile};
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
    max_cell_display_chars: Rc<Cell<usize>>,
    /// How many rows have been sampled for column width calculation
    width_sampled_rows: Arc<Mutex<usize>>,
    font_profile: Rc<Cell<FontProfile>>,
    font_size: Rc<Cell<u32>>,
}

#[derive(Default)]
struct DragState {
    is_dragging: bool,
    start_row: i32,
    start_col: i32,
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
        let font_size = self.font_size.get();
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
    }

    fn recalculate_widths_for_current_font(&mut self) {
        let headers = self.headers.lock().unwrap().clone();
        if headers.is_empty() {
            return;
        }

        let font_size = self.font_size.get();
        let max_cell_display_chars = self.max_cell_display_chars.get();
        let mut widths: Vec<i32> = headers
            .iter()
            .map(|h| Self::estimate_text_width(h, font_size))
            .collect();

        let mut sampled = 0usize;
        {
            let full_data = self.full_data.lock().unwrap();
            for row in full_data.iter().take(WIDTH_SAMPLE_ROWS) {
                Self::update_widths_with_row(&mut widths, row, font_size, max_cell_display_chars);
                sampled += 1;
            }
        }

        if sampled < WIDTH_SAMPLE_ROWS {
            let pending = self.pending_rows.lock().unwrap();
            let remaining = WIDTH_SAMPLE_ROWS - sampled;
            for row in pending.iter().take(remaining) {
                Self::update_widths_with_row(&mut widths, row, font_size, max_cell_display_chars);
            }
        }

        *self.pending_widths.lock().unwrap() = widths.clone();
        self.apply_widths_to_table(&widths);
    }

    pub fn new() -> Self {
        Self::with_size(0, 0, 100, 100)
    }

    pub fn with_size(x: i32, y: i32, w: i32, h: i32) -> Self {
        let headers: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let full_data: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let font_profile = Rc::new(Cell::new(configured_editor_profile()));
        let font_size = Rc::new(Cell::new(DEFAULT_FONT_SIZE as u32));
        let max_cell_display_chars =
            Rc::new(Cell::new(RESULT_CELL_MAX_DISPLAY_CHARS_DEFAULT as usize));

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
            let font_profile = font_profile_for_draw.get();
            let font_size = font_size_for_draw.get() as i32;
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
                                let max_chars = max_cell_display_chars_for_draw.get();
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
        table.handle(move |_, ev| {
            if !table_for_handle.active() {
                return false;
            }
            match ev {
                Event::Push => {
                    if app::event_mouse_button() == app::MouseButton::Right {
                        Self::show_context_menu(
                            &table_for_handle,
                            &headers_for_handle,
                            &full_data_for_handle,
                        );
                        return true;
                    }
                    // Left click - start drag selection
                    if app::event_mouse_button() == app::MouseButton::Left {
                        let _ = table_for_handle.take_focus();
                        if let Some((row, col)) = Self::get_cell_at_mouse(&table_for_handle) {
                            if app::event_clicks() {
                                // Clone the cell value so the Ref is dropped before
                                // entering the modal dialog's event loop. Holding the
                                // borrow across app::wait() would panic if flush_pending
                                // tries to borrow_mut full_data during the nested loop.
                                let cell_val_owned =
                                    full_data_for_handle.try_lock().ok().and_then(|data| {
                                        data.get(row as usize)
                                            .and_then(|r| r.get(col as usize))
                                            .cloned()
                                    });
                                if let Some(cell_val) = cell_val_owned {
                                    Self::show_cell_text_dialog(
                                        &cell_val,
                                        font_profile_for_handle.get(),
                                        font_size_for_handle.get(),
                                    );
                                    return true;
                                }
                            }
                            let mut state = drag_state_for_handle.lock().unwrap();
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
                    let is_dragging = drag_state_for_handle.lock().unwrap().is_dragging;
                    if is_dragging {
                        if let Some((row, col)) =
                            Self::get_cell_at_mouse_for_drag(&table_for_handle)
                        {
                            let state = drag_state_for_handle.lock().unwrap();
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
                    let mut state = drag_state_for_handle.lock().unwrap();
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
                                &headers_for_handle,
                                &full_data_for_handle,
                            );
                            return true;
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
                        );
                        return true;
                    }
                    if ctrl_or_cmd && Self::matches_shortcut_key(key, original_key, 'c') {
                        Self::copy_selected_to_clipboard(
                            &table_for_handle,
                            &headers_for_handle,
                            &full_data_for_handle,
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
                    false
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
        }
    }

    fn matches_shortcut_key(key: Key, original_key: Key, ascii: char) -> bool {
        let lower = Key::from_char(ascii.to_ascii_lowercase());
        let upper = Key::from_char(ascii.to_ascii_uppercase());
        key == lower || key == upper || original_key == lower || original_key == upper
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

    fn show_context_menu(
        table: &Table,
        headers: &Arc<Mutex<Vec<String>>>,
        full_data: &Arc<Mutex<Vec<Vec<String>>>>,
    ) {
        let mouse_x = app::event_x();
        let mouse_y = app::event_y();

        let mut table = table.clone();
        // Give focus and potentially select cell under mouse for better UX
        let _ = table.take_focus();
        if let Some((row, col)) = Self::get_cell_at_mouse(&table) {
            let (row_top, col_left, row_bot, col_right) = table.get_selection();
            // If the cell under mouse is not already in the selection, select it
            if row < row_top || row > row_bot || col < col_left || col > col_right {
                table.set_selection(row, col, row, col);
                table.redraw();
            }
        }

        // Prevent menu from being added to parent container
        let current_group = fltk::group::Group::try_current();
        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let mut menu = MenuButton::new(mouse_x, mouse_y, 0, 0, None);
        menu.set_color(theme::panel_raised());
        menu.set_text_color(theme::text_primary());
        menu.add_choice("Copy|Copy with Headers|Copy All");

        if let Some(ref group) = current_group {
            fltk::group::Group::set_current(Some(group));
        }

        if let Some(choice) = menu.popup() {
            let choice_label = choice.label().unwrap_or_default();
            match choice_label.as_str() {
                "Copy" => {
                    Self::copy_selected_to_clipboard(&table, headers, full_data);
                }
                "Copy with Headers" => {
                    Self::copy_selected_with_headers(&table, headers, full_data);
                }
                "Copy All" => Self::copy_all_to_clipboard(headers, full_data),
                _ => {}
            }
        }

        MenuButton::delete(menu);
    }

    fn copy_selected_to_clipboard(
        table: &Table,
        _headers: &Arc<Mutex<Vec<String>>>,
        full_data: &Arc<Mutex<Vec<Vec<String>>>>,
    ) -> usize {
        let (row_top, col_left, row_bot, col_right) = table.get_selection();
        if row_top < 0 || col_left < 0 {
            return 0;
        }

        let rows = (row_bot - row_top + 1) as usize;
        let cols = (col_right - col_left + 1) as usize;
        let cell_count = rows * cols;

        let full_data = full_data.lock().unwrap();
        let mut result = String::with_capacity(rows * cols * 16);
        for row in row_top..=row_bot {
            if row > row_top {
                result.push('\n');
            }
            for col in col_left..=col_right {
                if col > col_left {
                    result.push('\t');
                }
                if let Some(val) = full_data
                    .get(row as usize)
                    .and_then(|r| r.get(col as usize))
                {
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
    ) -> usize {
        let (row_top, col_left, row_bot, col_right) = table.get_selection();
        if row_top < 0 || col_left < 0 {
            return 0;
        }

        let rows = (row_bot - row_top + 1) as usize;
        let cols = (col_right - col_left + 1) as usize;
        let cell_count = rows * cols;

        let headers = headers.lock().unwrap();
        let full_data = full_data.lock().unwrap();
        let mut result = String::with_capacity((rows + 1) * cols * 16);

        // Add headers
        for col in col_left..=col_right {
            if col > col_left {
                result.push('\t');
            }
            if let Some(h) = headers.get(col as usize) {
                result.push_str(h);
            }
        }
        result.push('\n');

        // Add data
        for row in row_top..=row_bot {
            for col in col_left..=col_right {
                if col > col_left {
                    result.push('\t');
                }
                if let Some(val) = full_data
                    .get(row as usize)
                    .and_then(|r| r.get(col as usize))
                {
                    result.push_str(val);
                }
            }
            result.push('\n');
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
    ) {
        let headers = headers.lock().unwrap();
        let full_data = full_data.lock().unwrap();
        let row_count = full_data.len();
        let col_count = headers.len();
        let mut result = String::with_capacity((row_count + 1) * col_count * 16);

        // Add headers
        result.push_str(&headers.join("\t"));
        result.push('\n');

        // Add all data
        for row in full_data.iter() {
            for (i, cell) in row.iter().enumerate() {
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
        if !result.is_select {
            let font_size = self.font_size.get();
            let max_cell_display_chars = self.max_cell_display_chars.get();
            self.table.set_rows(1);
            self.table.set_cols(1);
            self.apply_table_metrics_for_current_font();
            let message_width =
                Self::estimate_display_width(&result.message, font_size, max_cell_display_chars)
                    .max(200)
                    .min(1200);
            self.table.set_col_width(0, message_width);
            *self.headers.lock().unwrap() = vec!["Result".to_string()];
            *self.full_data.lock().unwrap() = vec![vec![result.message.clone()]];
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
            *self.headers.lock().unwrap() = col_names;
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

        let font_size = self.font_size.get();
        let max_cell_display_chars = self.max_cell_display_chars.get();
        let widths = Self::compute_column_widths(
            &col_names,
            &result.rows,
            font_size,
            max_cell_display_chars,
        );
        self.apply_widths_to_table(&widths);
        *self.pending_widths.lock().unwrap() = widths;

        // Store data directly — draw_cell reads from full_data on demand.
        // No per-cell set_cell_value calls needed!
        *self.full_data.lock().unwrap() = result.rows.clone();
        *self.headers.lock().unwrap() = col_names;
        self.table.redraw();
    }

    pub fn start_streaming(&mut self, headers: &[String]) {
        let col_count = headers.len() as i32;

        // Clear any pending data from previous queries
        self.pending_rows.lock().unwrap().clear();
        self.pending_widths.lock().unwrap().clear();
        self.full_data.lock().unwrap().clear();
        *self.last_flush.lock().unwrap() = Instant::now();
        *self.width_sampled_rows.lock().unwrap() = 0;

        // Initialize pending widths based on headers
        let font_size = self.font_size.get();
        let initial_widths: Vec<i32> = headers
            .iter()
            .map(|h| Self::estimate_text_width(h, font_size))
            .collect();
        *self.pending_widths.lock().unwrap() = initial_widths.clone();

        self.table.set_rows(0);
        self.table.set_cols(col_count);
        self.apply_table_metrics_for_current_font();

        for (i, _name) in headers.iter().enumerate() {
            self.table.set_col_width(i as i32, initial_widths[i]);
        }

        *self.headers.lock().unwrap() = headers.to_vec();
        self.table.redraw();
    }

    /// Append rows to the buffer. UI is updated periodically for performance.
    pub fn append_rows(&mut self, rows: Vec<Vec<String>>) {
        // Only compute column widths for the first WIDTH_SAMPLE_ROWS rows
        let sampled = *self.width_sampled_rows.lock().unwrap();
        if sampled < WIDTH_SAMPLE_ROWS {
            let max_cols = rows.iter().map(|row| row.len()).max().unwrap_or(0);
            let mut widths = self.pending_widths.lock().unwrap();
            let min_width = Self::min_col_width_for_font(self.font_size.get());
            let max_cell_display_chars = self.max_cell_display_chars.get();
            if widths.len() < max_cols {
                widths.resize(max_cols, min_width);
            }
            let remaining = WIDTH_SAMPLE_ROWS - sampled;
            let sample_count = rows.len().min(remaining);
            for row in rows[..sample_count].iter() {
                Self::update_widths_with_row(
                    &mut widths,
                    row,
                    self.font_size.get(),
                    max_cell_display_chars,
                );
            }
            drop(widths);
            *self.width_sampled_rows.lock().unwrap() = sampled + sample_count;
        }

        // Add rows to pending buffer
        self.pending_rows.lock().unwrap().extend(rows);

        // Check if we should flush to UI
        let should_flush = {
            let elapsed = self.last_flush.lock().unwrap().elapsed();
            let buffered_count = self.pending_rows.lock().unwrap().len();
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
        let rows_to_add: Vec<Vec<String>> = self.pending_rows.lock().unwrap().drain(..).collect();
        if rows_to_add.is_empty() {
            return;
        }

        let new_rows_count = rows_to_add.len() as i32;
        let current_rows = self.table.rows();
        let new_total = current_rows + new_rows_count;

        // Update column widths
        {
            let widths = self.pending_widths.lock().unwrap();
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

        // Move data into full_data — zero-copy, no clone!
        self.full_data.lock().unwrap().extend(rows_to_add);

        // Just update row count — draw_cell reads from full_data on demand
        self.table.set_rows(new_total);
        self.apply_table_metrics_for_current_font();

        *self.last_flush.lock().unwrap() = Instant::now();
        self.table.redraw();
    }

    /// Call this when streaming is complete to flush any remaining buffered rows
    pub fn finish_streaming(&mut self) {
        self.flush_pending();
        self.table.redraw();
    }

    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.table.set_rows(0);
        self.table.set_cols(0);
        {
            let mut headers = self.headers.lock().unwrap();
            headers.clear();
            headers.shrink_to_fit();
        }
        {
            let mut pending_rows = self.pending_rows.lock().unwrap();
            pending_rows.clear();
            pending_rows.shrink_to_fit();
        }
        {
            let mut pending_widths = self.pending_widths.lock().unwrap();
            pending_widths.clear();
            pending_widths.shrink_to_fit();
        }
        {
            let mut full_data = self.full_data.lock().unwrap();
            full_data.clear();
            full_data.shrink_to_fit();
        }
        *self.width_sampled_rows.lock().unwrap() = 0;
        *self.last_flush.lock().unwrap() = Instant::now();
        self.table.redraw();
    }

    pub fn copy(&self) -> usize {
        let count = Self::copy_selected_to_clipboard(&self.table, &self.headers, &self.full_data);
        if count > 0 {
            let rows = (self.table.get_selection().2 - self.table.get_selection().0 + 1) as usize;
            let cols = (self.table.get_selection().3 - self.table.get_selection().1 + 1) as usize;
            println!("Copied {} cells ({} rows x {} cols)", count, rows, cols);
        }
        count
    }

    pub fn copy_with_headers(&self) {
        Self::copy_selected_with_headers(&self.table, &self.headers, &self.full_data);
    }

    pub fn select_all(&mut self) {
        let rows = self.table.rows();
        let cols = self.table.cols();
        if rows > 0 && cols > 0 {
            self.table.set_selection(0, 0, rows - 1, cols - 1);
            self.table.redraw();
        }
    }

    #[allow(dead_code)]
    pub fn get_selected_data(&self) -> Option<String> {
        let (row_top, col_left, row_bot, col_right) = self.table.get_selection();

        if row_top < 0 || col_left < 0 {
            return None;
        }

        let full_data = self.full_data.lock().unwrap();
        let rows = (row_bot - row_top + 1) as usize;
        let cols = (col_right - col_left + 1) as usize;
        let mut result = String::with_capacity(rows * cols * 16);
        for row in row_top..=row_bot {
            if row > row_top {
                result.push('\n');
            }
            for col in col_left..=col_right {
                if col > col_left {
                    result.push('\t');
                }
                if let Some(val) = full_data
                    .get(row as usize)
                    .and_then(|r| r.get(col as usize))
                {
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
        let headers = self.headers.lock().unwrap();
        let full_data = self.full_data.lock().unwrap();
        let row_count = full_data.len();
        let col_count = headers.len();
        let mut csv = String::with_capacity((row_count + 1) * col_count * 20);

        // Header row
        let header_line: Vec<String> = headers.iter().map(|h| Self::escape_csv_field(h)).collect();
        csv.push_str(&header_line.join(","));
        csv.push('\n');

        // Data rows
        for row in full_data.iter() {
            for (i, cell) in row.iter().enumerate() {
                if i > 0 {
                    csv.push(',');
                }
                csv.push_str(&Self::escape_csv_field(cell));
            }
            csv.push('\n');
        }

        csv
    }

    fn escape_csv_field(field: &str) -> String {
        if field.contains(',') || field.contains('"') || field.contains('\n') {
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

    pub fn get_widget(&self) -> Table {
        self.table.clone()
    }

    pub fn apply_font_settings(&mut self, profile: FontProfile, size: u32) {
        self.font_profile.set(profile);
        self.font_size.set(size);
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
        self.max_cell_display_chars.set(max_chars.max(1));
        self.recalculate_widths_for_current_font();
        self.table.redraw();
    }

    /// Cleanup method to release resources before the widget is deleted.
    pub fn cleanup(&mut self) {
        // Clear the event handler callback to release captured Arc<Mutex<T>> references.
        self.table.handle(|_, _| false);

        // Set an empty draw_cell to release captured Rc references
        // from the virtual rendering callback.
        self.table.draw_cell(|_, _, _, _, _, _, _, _| {});

        // Reset table dimensions
        self.table.set_rows(0);
        self.table.set_cols(0);

        // Clear all data buffers to release memory
        {
            let mut headers = self.headers.lock().unwrap();
            headers.clear();
            headers.shrink_to_fit();
        }
        {
            let mut pending_rows = self.pending_rows.lock().unwrap();
            pending_rows.clear();
            pending_rows.shrink_to_fit();
        }
        {
            let mut pending_widths = self.pending_widths.lock().unwrap();
            pending_widths.clear();
            pending_widths.shrink_to_fit();
        }
        {
            let mut full_data = self.full_data.lock().unwrap();
            full_data.clear();
            full_data.shrink_to_fit();
        }
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
}
