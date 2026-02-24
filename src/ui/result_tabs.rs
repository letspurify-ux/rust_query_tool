use fltk::{
    app,
    enums::{Align, Event, Key},
    group::{Group, Tabs, TabsOverflow},
    prelude::*,
    text::{TextBuffer, TextDisplay},
};
use std::sync::{Arc, Mutex};

use crate::ui::constants;
use crate::ui::font_settings::{configured_editor_profile, FontProfile};
use crate::ui::result_table::ResultGridSqlExecuteCallback;
use crate::ui::theme;
use crate::ui::ResultTableWidget;

type ResultTabsChangeCallback = Box<dyn FnMut()>;

#[derive(Clone)]
pub struct ResultTabsWidget {
    tabs: Tabs,
    data: Arc<Mutex<Vec<ResultTab>>>,
    active_index: Arc<Mutex<Option<usize>>>,
    script_output: Arc<Mutex<ScriptOutputTab>>,
    font_profile: Arc<Mutex<FontProfile>>,
    font_size: Arc<Mutex<u32>>,
    max_cell_display_chars: Arc<Mutex<usize>>,
    execute_sql_callback: Arc<Mutex<Option<ResultGridSqlExecuteCallback>>>,
    on_change_callback: Arc<Mutex<Option<ResultTabsChangeCallback>>>,
}

#[derive(Clone)]
struct ResultTab {
    group: Group,
    table: ResultTableWidget,
}

#[derive(Clone)]
struct ScriptOutputTab {
    group: Group,
    display: TextDisplay,
    buffer: TextBuffer,
}

impl ResultTabsWidget {
    fn fire_on_change_callback(&self) {
        let mut callback = self
            .on_change_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(callback_fn) = callback.as_mut() {
            callback_fn();
        }
        *self
            .on_change_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = callback;
    }

    fn fire_on_change_with(callback_ref: &Arc<Mutex<Option<ResultTabsChangeCallback>>>) {
        let mut callback = callback_ref
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(callback_fn) = callback.as_mut() {
            callback_fn();
        }
        *callback_ref
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = callback;
    }

    fn content_bounds(tabs: &Tabs) -> (i32, i32, i32, i32) {
        // Keep a stable tab-header height regardless of surrounding splitter drags.
        // This avoids top/bottom header bar height jitter while panes are resized.
        let x = tabs.x();
        let y = tabs.y() + constants::TAB_HEADER_HEIGHT;
        let w = tabs.w();
        let h = tabs.h() - constants::TAB_HEADER_HEIGHT;
        (x, y, w.max(1), h.max(1))
    }

    fn layout_children(tabs: &Tabs) {
        let (x, y, w, h) = Self::content_bounds(tabs);
        for child in tabs.clone().into_iter() {
            if let Some(mut group) = child.as_group() {
                group.resize(x, y, w, h);
            }
        }
    }

    fn reset_tab_strip_left_anchor(&mut self) {
        // Re-applying overflow mode resets FLTK's internal tab offset,
        // keeping the visible strip anchored from the left.
        self.tabs.handle_overflow(TabsOverflow::Pulldown);
    }

    fn maybe_shrink_tab_storage(data: &mut Vec<ResultTab>) {
        // Avoid frequent shrinking; only compact when capacity is materially over-provisioned.
        let len = data.len();
        let capacity = data.capacity();
        if len == 0 || (capacity > 0 && len.saturating_mul(2) < capacity) {
            data.shrink_to_fit();
        }
    }

    fn buffer_ends_with_newline(buffer: &TextBuffer) -> bool {
        let len = buffer.length();
        if len <= 0 {
            return false;
        }
        buffer
            .text_range(len - 1, len)
            .map(|s| s == "\n")
            .unwrap_or(false)
    }

    fn trim_script_output_buffer(buffer: &mut TextBuffer) {
        let max_chars = constants::SCRIPT_OUTPUT_MAX_CHARS;
        let target_chars = constants::SCRIPT_OUTPUT_TRIM_TARGET_CHARS.min(max_chars);
        let len = buffer.length().max(0) as usize;
        if len <= max_chars {
            return;
        }

        let remove_upto = len.saturating_sub(target_chars);
        if remove_upto == 0 {
            return;
        }

        let prefix = buffer.text_range(0, remove_upto as i32).unwrap_or_default();
        let cut = prefix.rfind('\n').map(|idx| idx + 1).unwrap_or(remove_upto);
        if cut > 0 {
            buffer.remove(0, cut as i32);
        }
    }

    pub fn new(x: i32, y: i32, w: i32, h: i32) -> Self {
        // Use explicit dimensions to avoid "center of requires the size of the
        // widget to be known" panic that occurs with default_fill()
        let mut tabs = Tabs::new(x, y, w, h, None);
        tabs.set_color(theme::panel_bg());
        tabs.set_selection_color(theme::selection_strong());
        tabs.set_label_color(theme::text_secondary());
        tabs.set_label_size((constants::TAB_HEADER_HEIGHT - 8).max(8));
        // Center labels in tab headers.
        tabs.set_tab_align(Align::Center);
        // Keep tab header widths stable while surrounding panes are resized.
        // `Compress` dynamically shrinks/expands tab buttons as width changes,
        // which causes distracting header size jumps during splitter drags.
        tabs.handle_overflow(TabsOverflow::Pulldown);

        let data = Arc::new(Mutex::new(Vec::<ResultTab>::new()));
        let active_index = Arc::new(Mutex::new(None));
        let font_profile = Arc::new(Mutex::new(configured_editor_profile()));
        let font_size = Arc::new(Mutex::new(constants::DEFAULT_FONT_SIZE as u32));
        let max_cell_display_chars = Arc::new(Mutex::new(
            constants::RESULT_CELL_MAX_DISPLAY_CHARS_DEFAULT as usize,
        ));
        let execute_sql_callback: Arc<Mutex<Option<ResultGridSqlExecuteCallback>>> =
            Arc::new(Mutex::new(None));
        let on_change_callback: Arc<Mutex<Option<ResultTabsChangeCallback>>> =
            Arc::new(Mutex::new(None));

        tabs.begin();
        let (x, y, w, h) = Self::content_bounds(&tabs);
        let mut script_group = Group::new(x, y, w, h, None).with_label("Script Output");
        script_group.set_color(theme::panel_bg());
        script_group.set_label_color(theme::text_secondary());
        script_group.set_align(Align::Center | Align::Inside);
        script_group.begin();
        let padding = constants::SCRIPT_OUTPUT_PADDING;
        let display_x = x + padding;
        let display_y = y + padding;
        let display_w = (w - padding * 2).max(10);
        let display_h = (h - padding * 2).max(10);
        let mut script_display = TextDisplay::new(display_x, display_y, display_w, display_h, None);
        script_display.set_color(theme::panel_bg());
        script_display.set_text_color(theme::text_primary());
        let script_profile = *font_profile
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        script_display.set_text_font(script_profile.normal);
        script_display.set_text_size(
            *font_size
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) as i32,
        );
        let mut script_buffer = TextBuffer::default();
        script_buffer.set_text("");
        script_display.set_buffer(script_buffer.clone());
        script_group.resizable(&script_display);
        script_group.end();
        tabs.end();

        let script_output = Arc::new(Mutex::new(ScriptOutputTab {
            group: script_group,
            display: script_display,
            buffer: script_buffer,
        }));

        let data_for_cb = data.clone();
        let active_for_cb = active_index.clone();
        let script_for_cb = script_output.clone();
        let on_change_for_cb = on_change_callback.clone();
        tabs.set_callback(move |t| {
            if let Some(widget) = t.value() {
                let ptr = widget.as_widget_ptr();
                let script_ptr = script_for_cb
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .group
                    .as_widget_ptr();
                if ptr == script_ptr {
                    *active_for_cb
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
                    Self::fire_on_change_with(&on_change_for_cb);
                    return;
                }
                let index = data_for_cb
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .iter()
                    .position(|tab| tab.group.as_widget_ptr() == ptr);
                *active_for_cb
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = index;
                Self::fire_on_change_with(&on_change_for_cb);
            }
        });

        let tabs_for_key = tabs.clone();
        tabs.handle(move |tabs, ev| {
            if matches!(ev, Event::MouseWheel) {
                // Prevent FLTK Tabs from applying wheel-based strip offset changes.
                // Wheel events can bubble down from nearby panes and cause the
                // result-tab header to snap right unexpectedly.
                tabs.handle_overflow(TabsOverflow::Pulldown);
                return true;
            }

            if !matches!(ev, Event::KeyDown) {
                return false;
            }

            let key = app::event_key();
            if !matches!(key, Key::Left | Key::Right | Key::Up | Key::Down) {
                return false;
            }

            let children: Vec<Group> = tabs_for_key
                .clone()
                .into_iter()
                .filter_map(|w| w.as_group())
                .collect();
            if children.is_empty() {
                return true;
            }

            let current_ptr = tabs_for_key.value().map(|w| w.as_widget_ptr());
            let index = current_ptr
                .and_then(|ptr| children.iter().position(|g| g.as_widget_ptr() == ptr))
                .unwrap_or(0);

            match key {
                Key::Left | Key::Up => index == 0,
                Key::Right | Key::Down => index + 1 >= children.len(),
                _ => false,
            }
        });

        tabs.resize_callback(move |t, _, _, _, _| {
            Self::layout_children(t);
        });

        Self {
            tabs,
            data,
            active_index,
            script_output,
            font_profile,
            font_size,
            max_cell_display_chars,
            execute_sql_callback,
            on_change_callback,
        }
    }

    pub fn set_on_change<F>(&mut self, callback: F)
    where
        F: FnMut() + 'static,
    {
        *self
            .on_change_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(callback));
    }

    pub fn get_widget(&self) -> Tabs {
        self.tabs.clone()
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
        {
            let mut script_output = self
                .script_output
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            script_output.display.set_text_font(profile.normal);
            script_output.display.set_text_size(size as i32);
            script_output.display.redraw();
        }
    }

    pub fn set_max_cell_display_chars(&mut self, max_chars: usize) {
        *self
            .max_cell_display_chars
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = max_chars;
    }

    pub fn clear(&mut self) {
        let tabs_to_delete: Vec<_> = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .drain(..)
            .collect();
        for tab in tabs_to_delete {
            self.delete_tab(tab);
        }
        {
            let mut data = self
                .data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            Self::maybe_shrink_tab_storage(&mut data);
        }
        self.clear_script_output();
        *self
            .active_index
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        let script_group = {
            let script_output = self
                .script_output
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            script_output.group.clone()
        };
        let _ = self.tabs.set_value(&script_group);
        self.reset_tab_strip_left_anchor();
        self.tabs.redraw();
        let script_output = self
            .script_output
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut script_group = script_output.group.clone();
        let mut script_display = script_output.display.clone();
        script_group.redraw();
        script_display.redraw();
        self.fire_on_change_callback();
    }

    pub fn tab_count(&self) -> usize {
        self.data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    pub fn append_script_output_lines(&mut self, lines: &[String]) {
        let mut script_output = self
            .script_output
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut buffer = script_output.buffer.clone();
        if lines.is_empty() {
            return;
        }
        if buffer.length() > 0 && !Self::buffer_ends_with_newline(&buffer) {
            buffer.append("\n");
        }
        for (idx, line) in lines.iter().enumerate() {
            buffer.append(line);
            if idx + 1 < lines.len() {
                buffer.append("\n");
            }
        }
        buffer.append("\n");
        Self::trim_script_output_buffer(&mut buffer);
        let line_count = script_output.display.count_lines(0, buffer.length(), true);
        script_output.display.scroll(line_count, 0);
    }

    pub fn start_statement(&mut self, index: usize, label: &str) {
        let existing_group = {
            self.data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(index)
                .map(|tab| tab.group.clone())
        };
        if let Some(group) = existing_group {
            // Extract the group before calling set_value to avoid re-entrant borrow
            // when the tabs callback fires
            let _ = self.tabs.set_value(&group);
            *self
                .active_index
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(index);
            return;
        }

        self.tabs.begin();
        // Use explicit tab content bounds to avoid relying on hard-coded header height.
        let (x, y, w, h) = Self::content_bounds(&self.tabs);
        let mut group = Group::new(x, y, w, h, None).with_label(label);
        group.set_color(theme::panel_bg());
        group.set_label_color(theme::text_secondary());
        group.set_align(Align::Center | Align::Inside);

        group.begin();
        let mut table = ResultTableWidget::with_size(x, y, w, h);
        table.apply_font_settings(
            *self
                .font_profile
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            *self
                .font_size
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        table.set_max_cell_display_chars(
            *self
                .max_cell_display_chars
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        let execute_sql_callback = self
            .execute_sql_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        table.set_execute_sql_callback(execute_sql_callback);
        let widget = table.get_widget();
        group.resizable(&widget);
        group.end();
        self.tabs.end();

        let (new_index, new_group) = {
            let mut data = self
                .data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            data.push(ResultTab { group, table });
            let idx = data.len().saturating_sub(1);
            let group = data.get(idx).map(|tab| tab.group.clone());
            (idx, group)
        };
        // Extract the group before calling set_value to avoid re-entrant borrow
        // when the tabs callback fires
        if let Some(group) = new_group {
            let _ = self.tabs.set_value(&group);
        }
        self.reset_tab_strip_left_anchor();
        *self
            .active_index
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(new_index);
        self.fire_on_change_callback();
    }

    pub fn start_streaming(&mut self, index: usize, columns: &[String]) {
        if let Some(tab) = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(index)
        {
            let mut table = tab.table.clone();
            table.start_streaming(columns);
        }
        self.fire_on_change_callback();
    }

    pub fn append_rows(&mut self, index: usize, rows: Vec<Vec<String>>) {
        if let Some(tab) = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(index)
        {
            let mut table = tab.table.clone();
            table.append_rows(rows);
        }
    }

    pub fn finish_streaming(&mut self, index: usize) {
        if let Some(tab) = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(index)
        {
            let mut table = tab.table.clone();
            table.finish_streaming();
        }
        self.fire_on_change_callback();
    }

    pub fn finish_all_streaming(&mut self) {
        let tables = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for tab in tables.iter() {
            let mut table = tab.table.clone();
            table.finish_streaming();
        }
    }

    pub fn align_tab_strip_left(&mut self) {
        self.reset_tab_strip_left_anchor();
        self.tabs.redraw();
    }

    pub fn display_result(&mut self, index: usize, result: &crate::db::QueryResult) {
        if let Some(tab) = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(index)
        {
            let mut table = tab.table.clone();
            table.display_result(result);
        }
        self.fire_on_change_callback();
    }

    pub fn set_source_sql(&mut self, index: usize, sql: &str) {
        if let Some(tab) = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(index)
        {
            let mut table = tab.table.clone();
            table.set_source_sql(sql);
        }
        self.fire_on_change_callback();
    }

    pub fn set_execute_sql_callback(&mut self, callback: ResultGridSqlExecuteCallback) {
        *self
            .execute_sql_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(callback.clone());
        let tabs = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for tab in tabs.iter() {
            let mut table = tab.table.clone();
            table.set_execute_sql_callback(Some(callback.clone()));
        }
    }

    pub fn export_to_csv(&self) -> String {
        self.current_table()
            .map(|table| table.export_to_csv())
            .unwrap_or_default()
    }

    pub fn row_count(&self) -> usize {
        self.current_table()
            .map(|table| table.row_count())
            .unwrap_or(0)
    }

    pub fn has_data(&self) -> bool {
        self.current_table()
            .map(|table| table.has_data())
            .unwrap_or(false)
    }

    pub fn can_current_begin_edit_mode(&self) -> bool {
        self.current_table()
            .map(|table| table.can_begin_edit_mode())
            .unwrap_or(false)
    }

    pub fn is_current_edit_mode_enabled(&self) -> bool {
        self.current_table()
            .map(|table| table.is_edit_mode_enabled())
            .unwrap_or(false)
    }

    pub fn begin_current_edit_mode(&mut self) -> Result<String, String> {
        let Some(mut table) = self.current_table() else {
            return Err("Open a result tab first.".to_string());
        };
        let result = table.begin_edit_mode();
        self.fire_on_change_callback();
        result
    }

    pub fn insert_row_in_current_edit_mode(&mut self) -> Result<String, String> {
        let Some(mut table) = self.current_table() else {
            return Err("Open a result tab first.".to_string());
        };
        let result = table.insert_row_in_edit_mode();
        self.fire_on_change_callback();
        result
    }

    pub fn delete_selected_rows_in_current_edit_mode(&mut self) -> Result<String, String> {
        let Some(mut table) = self.current_table() else {
            return Err("Open a result tab first.".to_string());
        };
        let result = table.delete_selected_rows_in_edit_mode();
        self.fire_on_change_callback();
        result
    }

    pub fn save_current_edit_mode(&mut self) -> Result<String, String> {
        let Some(mut table) = self.current_table() else {
            return Err("Open a result tab first.".to_string());
        };
        let result = table.save_edit_mode();
        self.fire_on_change_callback();
        result
    }

    pub fn cancel_current_edit_mode(&mut self) -> Result<String, String> {
        let Some(mut table) = self.current_table() else {
            return Err("Open a result tab first.".to_string());
        };
        let result = table.cancel_edit_mode();
        self.fire_on_change_callback();
        result
    }

    fn current_table(&self) -> Option<ResultTableWidget> {
        let index = *self
            .active_index
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        index
            .and_then(|idx| {
                self.data
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .get(idx)
                    .cloned()
            })
            .map(|tab| tab.table)
    }

    pub fn copy(&self) -> usize {
        if let Some(table) = self.current_table() {
            table.copy()
        } else {
            0
        }
    }

    pub fn copy_with_headers(&self) {
        if let Some(table) = self.current_table() {
            table.copy_with_headers();
        }
    }

    pub fn select_all(&self) {
        if let Some(mut table) = self.current_table() {
            table.select_all();
        }
    }

    pub fn paste_from_clipboard(&self) -> bool {
        if let Some(mut table) = self.current_table() {
            table.paste_from_clipboard();
            true
        } else {
            false
        }
    }

    fn delete_tab(&mut self, mut tab: ResultTab) {
        // FLTK memory management: proper cleanup order is critical
        // 1. Clear callbacks on child widgets to release captured Arc<Mutex<T>> references
        // 2. Remove child widgets from parent before deletion
        // 3. Delete child widgets
        // 4. Delete parent container

        // Step 1: Cleanup the table widget (clears callbacks and data buffers)
        tab.table.cleanup();

        // Step 2 & 3: Explicitly remove/delete the table widget first to ensure
        // callback closures are dropped immediately, then clear/delete any
        // additional child widgets that may be added to result tabs in the future.
        let mut group = tab.group;
        let table_widget = tab.table.get_widget();
        if group.find(&table_widget) >= 0 {
            group.remove(&table_widget);
        }
        fltk::table::Table::delete(table_widget);
        group.clear();

        // Step 4: Remove group from tabs and delete
        if self.tabs.find(&group) >= 0 {
            self.tabs.remove(&group);
        }
        fltk::group::Group::delete(group);
    }

    /// Close the currently active result tab, freeing its data and FLTK resources.
    /// Returns true if a tab was closed.
    pub fn close_current_tab(&mut self) -> bool {
        let index = match *self
            .active_index
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
        {
            Some(idx) => idx,
            None => return false, // Script Output tab cannot be closed
        };

        let tab = {
            let mut data = self
                .data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if index >= data.len() {
                return false;
            }
            data.remove(index)
        };

        self.delete_tab(tab);

        {
            let mut data = self
                .data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            Self::maybe_shrink_tab_storage(&mut data);
        }

        // Update active index to nearest remaining tab
        let remaining = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len();
        if remaining == 0 {
            *self
                .active_index
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
            let script_group = self
                .script_output
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .group
                .clone();
            let _ = self.tabs.set_value(&script_group);
        } else {
            let new_index = if index >= remaining {
                remaining - 1
            } else {
                index
            };
            *self
                .active_index
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(new_index);
            let group = {
                self.data
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .get(new_index)
                    .map(|tab| tab.group.clone())
            };
            if let Some(group) = group {
                let _ = self.tabs.set_value(&group);
            }
        }

        self.tabs.redraw();
        self.fire_on_change_callback();
        true
    }

    pub fn select_script_output(&mut self) {
        let script_group = self
            .script_output
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .group
            .clone();
        let _ = self.tabs.set_value(&script_group);
        *self
            .active_index
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        self.fire_on_change_callback();
    }

    fn clear_script_output(&self) {
        let mut script_output = self
            .script_output
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Recreate the buffer to drop retained capacity after very large script outputs.
        let mut new_buffer = TextBuffer::default();
        new_buffer.set_text("");
        script_output.display.set_buffer(new_buffer.clone());
        script_output.buffer = new_buffer;
        script_output.display.scroll(0, 0);
    }
}

impl Default for ResultTabsWidget {
    fn default() -> Self {
        Self::new(0, 0, 100, 100)
    }
}
