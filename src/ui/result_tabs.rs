use fltk::{
    app,
    enums::{Align, Event, FrameType, Key},
    group::{Group, Tabs, TabsOverflow},
    prelude::*,
    text::{TextBuffer, TextDisplay},
};
use std::any::Any;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

use crate::ui::constants;
use crate::ui::font_settings::{configured_editor_profile, FontProfile};
use crate::ui::result_table::{
    LazyFetchCallback, ResultGridSqlExecuteCallback, ResultTableContextActionCallback,
};
use crate::ui::text_buffer_access;
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
    lazy_fetch_callback: LazyFetchCallback,
    context_action_callback: ResultTableContextActionCallback,
    on_change_callback: Arc<Mutex<Option<ResultTabsChangeCallback>>>,
    suppress_pointer_event_depth: Arc<Mutex<u32>>,
}

#[derive(Clone)]
struct ResultTab {
    group: Group,
    table: ResultTableWidget,
    status: ResultTabStatus,
    row_count: usize,
}

#[derive(Clone)]
struct ScriptOutputTab {
    group: Group,
    display: TextDisplay,
    buffer: TextBuffer,
    attached: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResultTabStatus {
    Running,
    Fetching,
    Waiting,
    Canceling,
    Done,
    Error,
    Cancelled,
}

impl ResultTabStatus {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Running => "Running",
            Self::Fetching => "Fetching",
            Self::Waiting => "Waiting",
            Self::Canceling => "Canceling",
            Self::Done => "Done",
            Self::Error => "Error",
            Self::Cancelled => "Cancelled",
        }
    }

    pub(crate) fn status_bar_message(self) -> &'static str {
        match self {
            Self::Running => "Running query...",
            Self::Fetching => "Fetching rows",
            Self::Waiting => "Waiting for lazy fetch",
            Self::Canceling => "Canceling",
            Self::Done => "Done",
            Self::Error => "Error",
            Self::Cancelled => "Cancelled",
        }
    }

    pub(crate) fn status_bar_message_with_rows(self, row_count: usize) -> String {
        if self == Self::Fetching {
            format!("{}: {}", self.status_bar_message(), row_count)
        } else {
            self.status_bar_message().to_string()
        }
    }

    fn for_stream_update(current: Self) -> Self {
        match current {
            Self::Canceling | Self::Cancelled | Self::Done | Self::Error => current,
            Self::Running | Self::Fetching | Self::Waiting => Self::Fetching,
        }
    }

    fn is_cancelled_message(message: &str) -> bool {
        let trimmed = message.trim();
        let normalized = if trimmed
            .get(.."Error:".len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("Error:"))
        {
            &trimmed["Error:".len()..]
        } else {
            trimmed
        }
        .trim();
        normalized.eq_ignore_ascii_case("Query cancelled")
            || normalized.eq_ignore_ascii_case("Query canceled")
    }

    pub(crate) fn from_query_result(result: &crate::db::QueryResult) -> Self {
        if result.success {
            Self::Done
        } else if Self::is_cancelled_message(&result.message) {
            Self::Cancelled
        } else {
            Self::Error
        }
    }
}

struct PointerEventSuppressGuard {
    counter: Arc<Mutex<u32>>,
}

impl PointerEventSuppressGuard {
    fn new(counter: Arc<Mutex<u32>>) -> Self {
        {
            let mut guard = counter
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *guard = guard.saturating_add(1);
        }
        Self { counter }
    }
}

impl Drop for PointerEventSuppressGuard {
    fn drop(&mut self) {
        let mut guard = self
            .counter
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = guard.saturating_sub(1);
    }
}

impl ResultTabsWidget {
    fn panic_payload_to_string(payload: &(dyn Any + Send)) -> String {
        if let Some(message) = payload.downcast_ref::<&str>() {
            (*message).to_string()
        } else if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else {
            "unknown panic payload".to_string()
        }
    }

    fn invoke_change_callback(callback: &mut ResultTabsChangeCallback) {
        let callback_result = panic::catch_unwind(AssertUnwindSafe(callback));
        if let Err(payload) = callback_result {
            let panic_payload = Self::panic_payload_to_string(payload.as_ref());
            crate::utils::logging::log_error(
                "result_tabs::callback",
                &format!("result tabs change callback panicked: {panic_payload}"),
            );
            eprintln!("result tabs change callback panicked: {panic_payload}");
        }
    }

    fn fire_on_change_callback(&self) {
        let mut callback = self
            .on_change_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(callback_fn) = callback.as_mut() {
            Self::invoke_change_callback(callback_fn);
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
            Self::invoke_change_callback(callback_fn);
        }
        *callback_ref
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = callback;
    }

    fn invoke_lazy_fetch_callback(
        callback_ref: &LazyFetchCallback,
        session_id: u64,
        request: crate::ui::sql_editor::LazyFetchRequest,
    ) {
        let mut callback = callback_ref
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(callback_fn) = callback.as_mut() {
            callback_fn(session_id, request);
        }
        let mut callback_guard = callback_ref
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if callback_guard.is_none() {
            *callback_guard = callback;
        }
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

    fn layout_script_output_tab(tabs: &Tabs, script_output: &mut ScriptOutputTab) {
        let (x, y, w, h) = Self::content_bounds(tabs);
        script_output.group.resize(x, y, w, h);
        let padding = constants::SCRIPT_OUTPUT_PADDING;
        script_output.display.resize(
            x + padding,
            y + padding,
            (w - padding * 2).max(10),
            (h - padding * 2).max(10),
        );
    }

    fn should_reset_tab_strip_left_anchor(child_count: i32, width: i32, height: i32) -> bool {
        child_count > 1 && width > 0 && height > 0
    }

    fn should_reapply_tab_overflow_mode_on_wheel(
        child_count: i32,
        width: i32,
        height: i32,
    ) -> bool {
        child_count > 0 && width > 0 && height > 0
    }

    fn should_suppress_pointer_event(depth: &Arc<Mutex<u32>>, ev: Event) -> bool {
        matches!(
            ev,
            Event::Enter
                | Event::Move
                | Event::Push
                | Event::Drag
                | Event::Released
                | Event::Leave
                | Event::MouseWheel
        ) && *depth
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            > 0
    }

    fn should_consume_empty_tab_pointer_event(child_count: i32, ev: Event) -> bool {
        child_count == 0
            && matches!(
                ev,
                Event::Enter
                    | Event::Move
                    | Event::Push
                    | Event::Drag
                    | Event::Released
                    | Event::Leave
                    | Event::MouseWheel
            )
    }

    fn reset_tab_strip_left_anchor(&mut self) {
        // Re-applying overflow mode resets FLTK's internal tab offset,
        // keeping the visible strip anchored from the left. Skip transient
        // empty/single-tab states while tabs are being recreated because
        // overflow math is irrelevant there.
        if Self::should_reset_tab_strip_left_anchor(
            self.tabs.children(),
            self.tabs.w(),
            self.tabs.h(),
        ) {
            self.tabs.handle_overflow(TabsOverflow::Pulldown);
        } else {
            self.tabs.handle_overflow(TabsOverflow::Compress);
        }
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
        text_buffer_access::text_range(buffer, None, len - 1, len) == "\n"
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

        let prefix = text_buffer_access::text_range(buffer, None, 0, remove_upto as i32);
        let cut = prefix.rfind('\n').map(|idx| idx + 1).unwrap_or(remove_upto);
        if cut > 0 {
            buffer.remove(0, cut as i32);
        }
    }

    fn result_tab_label(status: ResultTabStatus, row_count: usize) -> String {
        format!("{} ({})", status.label(), row_count)
    }

    fn tabs_contains_group(tabs: &Tabs, group: &Group) -> bool {
        !tabs.was_deleted() && !group.was_deleted() && tabs.find(group) < tabs.children()
    }

    fn active_result_group(&self) -> Option<Group> {
        let index = (*self
            .active_index
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()))?;
        self.data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(index)
            .map(|tab| tab.group.clone())
    }

    fn script_output_tab_is_visible(&self) -> bool {
        if self.tabs.was_deleted() {
            return false;
        }
        let (script_group, attached) = {
            let script_output = self
                .script_output
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            (script_output.group.clone(), script_output.attached)
        };
        attached && Self::tabs_contains_group(&self.tabs, &script_group)
    }

    fn script_output_tab_is_current(&self) -> bool {
        if !self.script_output_tab_is_visible() {
            return false;
        }
        let script_group = self
            .script_output
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .group
            .clone();
        self.tabs
            .value()
            .is_some_and(|current| current.as_widget_ptr() == script_group.as_widget_ptr())
    }

    fn ensure_script_output_tab_visible(&mut self) {
        if self.tabs.was_deleted() {
            return;
        }

        let active_group = self.active_result_group();
        let script_output_ref = self.script_output.clone();
        let script_group = {
            let mut script_output = script_output_ref
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if script_output.group.was_deleted() {
                return;
            }
            Self::layout_script_output_tab(&self.tabs, &mut script_output);
            if !Self::tabs_contains_group(&self.tabs, &script_output.group) {
                self.tabs.add(&script_output.group);
            }
            script_output.attached = true;
            script_output.group.clone()
        };

        if let Some(group) = active_group {
            if !group.was_deleted() {
                let _ = self.tabs.set_value(&group);
            }
        } else {
            let _ = self.tabs.set_value(&script_group);
            *self
                .active_index
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        }
        self.reset_tab_strip_left_anchor();
        self.tabs.redraw();
    }

    fn hide_script_output_tab(&mut self) {
        if self.tabs.was_deleted() {
            return;
        }

        let script_output_ref = self.script_output.clone();
        let removed = {
            let mut script_output = script_output_ref
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if script_output.group.was_deleted()
                || !script_output.attached
                || !Self::tabs_contains_group(&self.tabs, &script_output.group)
            {
                false
            } else {
                self.tabs.remove(&script_output.group);
                script_output.attached = false;
                true
            }
        };

        if removed {
            self.reset_tab_strip_left_anchor();
            self.tabs.redraw();
        }
    }

    fn update_tab_group_label(
        &mut self,
        mut group: Group,
        status: ResultTabStatus,
        row_count: usize,
    ) {
        if self.tabs.was_deleted() || group.was_deleted() {
            return;
        }
        group.set_label(&Self::result_tab_label(status, row_count));
        group.redraw();
        self.tabs.redraw();
    }

    fn set_result_tab_state(
        &mut self,
        index: usize,
        status: ResultTabStatus,
        row_count: usize,
    ) -> Option<(Group, ResultTableWidget)> {
        let tab_parts = {
            let mut data = self
                .data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            data.get_mut(index).map(|tab| {
                tab.status = status;
                tab.row_count = row_count;
                (tab.group.clone(), tab.table.clone())
            })
        };
        if let Some((group, _)) = tab_parts.as_ref() {
            self.update_tab_group_label(group.clone(), status, row_count);
        }
        tab_parts
    }

    pub fn new(x: i32, y: i32, w: i32, h: i32) -> Self {
        // Use explicit dimensions to avoid "center of requires the size of the
        // widget to be known" panic that occurs with default_fill()
        let mut tabs = Tabs::new(x, y, w, h, None);
        tabs.set_color(theme::panel_bg());
        tabs.set_selection_color(theme::selection_strong());
        tabs.set_frame(FrameType::RFlatBox);
        tabs.set_label_color(theme::text_secondary());
        tabs.set_label_size((constants::TAB_HEADER_HEIGHT - 8).max(8));
        // Center labels in tab headers.
        tabs.set_tab_align(Align::Center);
        // Keep tab header widths stable while surrounding panes are resized.
        // `Compress` dynamically shrinks/expands tab buttons as width changes,
        // which causes distracting header size jumps during splitter drags.
        tabs.handle_overflow(TabsOverflow::Compress);

        let data = Arc::new(Mutex::new(Vec::<ResultTab>::new()));
        let active_index = Arc::new(Mutex::new(None));
        let font_profile = Arc::new(Mutex::new(configured_editor_profile()));
        let font_size = Arc::new(Mutex::new(constants::DEFAULT_FONT_SIZE as u32));
        let max_cell_display_chars = Arc::new(Mutex::new(
            constants::RESULT_CELL_MAX_DISPLAY_CHARS_DEFAULT as usize,
        ));
        let execute_sql_callback: Arc<Mutex<Option<ResultGridSqlExecuteCallback>>> =
            Arc::new(Mutex::new(None));
        let lazy_fetch_callback: LazyFetchCallback = Arc::new(Mutex::new(None));
        let context_action_callback: ResultTableContextActionCallback = Arc::new(Mutex::new(None));
        let on_change_callback: Arc<Mutex<Option<ResultTabsChangeCallback>>> =
            Arc::new(Mutex::new(None));
        let suppress_pointer_event_depth = Arc::new(Mutex::new(0u32));

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
        theme::style_text_display_scrollbars(&script_display);
        script_group.resizable(&script_display);
        script_group.end();
        tabs.end();
        tabs.remove(&script_group);

        let script_output = Arc::new(Mutex::new(ScriptOutputTab {
            group: script_group,
            display: script_display,
            buffer: script_buffer,
            attached: false,
        }));

        let data_for_cb = data.clone();
        let active_for_cb = active_index.clone();
        let script_for_cb = script_output.clone();
        let on_change_for_cb = on_change_callback.clone();
        let suppress_pointer_for_cb = suppress_pointer_event_depth.clone();
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
            if Self::should_suppress_pointer_event(&suppress_pointer_for_cb, ev) {
                return true;
            }
            if Self::should_consume_empty_tab_pointer_event(tabs.children(), ev) {
                return true;
            }
            if matches!(ev, Event::MouseWheel)
                && Self::should_reapply_tab_overflow_mode_on_wheel(
                    tabs.children(),
                    tabs.w(),
                    tabs.h(),
                )
            {
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
            lazy_fetch_callback,
            context_action_callback,
            on_change_callback,
            suppress_pointer_event_depth,
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
        let _pointer_suppress_guard =
            PointerEventSuppressGuard::new(self.suppress_pointer_event_depth.clone());
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

    pub fn lazy_fetch_sessions(&self) -> Vec<u64> {
        self.data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter_map(|tab| tab.table.active_lazy_fetch_session())
            .collect()
    }

    pub fn lazy_fetch_session_at(&self, index: usize) -> Option<u64> {
        self.data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(index)
            .and_then(|tab| tab.table.active_lazy_fetch_session())
    }

    pub fn active_result_index(&self) -> Option<usize> {
        *self
            .active_index
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn append_script_output_lines(&mut self, lines: &[String]) {
        if lines.is_empty() {
            return;
        }
        self.ensure_script_output_tab_visible();
        let mut script_output = self
            .script_output
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut buffer = script_output.buffer.clone();
        let has_prefix_newline = buffer.length() > 0 && !Self::buffer_ends_with_newline(&buffer);
        let mut append_capacity = lines.iter().map(|line| line.len() + 1).sum::<usize>();
        if has_prefix_newline {
            append_capacity = append_capacity.saturating_add(1);
        }
        let mut appended = String::with_capacity(append_capacity);
        if has_prefix_newline {
            appended.push('\n');
        }
        for line in lines {
            appended.push_str(line);
            appended.push('\n');
        }
        buffer.append(&appended);
        Self::trim_script_output_buffer(&mut buffer);
        let end_pos = buffer.length();
        script_output.display.set_insert_position(end_pos);
        script_output.display.show_insert_position();
    }

    pub fn start_statement(&mut self, index: usize, _label: &str) {
        let _pointer_suppress_guard =
            PointerEventSuppressGuard::new(self.suppress_pointer_event_depth.clone());
        let existing_group = self
            .set_result_tab_state(index, ResultTabStatus::Running, 0)
            .map(|(group, _)| group);
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
        let mut group = Group::new(x, y, w, h, None)
            .with_label(&Self::result_tab_label(ResultTabStatus::Running, 0));
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
        table.set_lazy_fetch_callback(self.lazy_fetch_callback.clone());
        table.set_context_action_callback(self.context_action_callback.clone());
        let widget = table.get_widget();
        group.resizable(&widget);
        group.end();
        self.tabs.end();

        let (new_index, new_group) = {
            let mut data = self
                .data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            data.push(ResultTab {
                group,
                table,
                status: ResultTabStatus::Running,
                row_count: 0,
            });
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

    pub fn start_streaming(&mut self, index: usize, columns: &[String], null_text: &str) {
        let status = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(index)
            .map(|tab| ResultTabStatus::for_stream_update(tab.status));
        let table = status
            .and_then(|status| self.set_result_tab_state(index, status, 0))
            .map(|(_, table)| table);
        if let Some(mut table) = table {
            table.set_null_text(null_text);
            table.start_streaming(columns);
        }
        self.fire_on_change_callback();
    }

    pub fn append_rows(&mut self, index: usize, rows: Vec<Vec<String>>) {
        let rows_len = rows.len();
        let table = {
            let data = self
                .data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            data.get(index).map(|tab| {
                (
                    tab.row_count.saturating_add(rows_len),
                    ResultTabStatus::for_stream_update(tab.status),
                    tab.table.clone(),
                )
            })
        }
        .and_then(|(row_count, status, table)| {
            self.set_result_tab_state(index, status, row_count)
                .map(|_| table)
        });
        if let Some(mut table) = table {
            table.append_rows(rows);
        }
    }

    pub fn finish_streaming(&mut self, index: usize) {
        let table = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(index)
            .map(|tab| tab.table.clone());
        if let Some(mut table) = table {
            table.finish_streaming();
        }
        self.fire_on_change_callback();
    }

    pub fn set_lazy_fetch_session(&mut self, index: usize, session_id: u64) {
        let table = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(index)
            .map(|tab| tab.table.clone());
        if let Some(mut table) = table {
            table.set_lazy_fetch_session(session_id);
        }
        self.fire_on_change_callback();
    }

    pub fn mark_lazy_fetch_waiting(&mut self, index: usize) {
        let row_count = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(index)
            .map(|tab| tab.row_count);
        if let Some(row_count) = row_count {
            self.set_result_tab_state(index, ResultTabStatus::Waiting, row_count);
        }
        self.fire_on_change_callback();
    }

    pub fn mark_statement_canceling(&mut self, index: usize) {
        self.mark_statement_status(index, ResultTabStatus::Canceling);
    }

    pub fn mark_statement_cancelled(&mut self, index: usize) {
        self.mark_statement_status(index, ResultTabStatus::Cancelled);
    }

    fn mark_statement_status(&mut self, index: usize, status: ResultTabStatus) {
        let row_count = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(index)
            .map(|tab| tab.row_count);
        if let Some(row_count) = row_count {
            self.set_result_tab_state(index, status, row_count);
        }
        self.fire_on_change_callback();
    }

    pub fn mark_lazy_fetch_canceling(&mut self, session_id: u64) -> bool {
        let tab_updates: Vec<(usize, usize)> = {
            let data = self
                .data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            data.iter()
                .enumerate()
                .filter_map(|(index, tab)| {
                    if tab.table.active_lazy_fetch_session() == Some(session_id) {
                        Some((index, tab.row_count))
                    } else {
                        None
                    }
                })
                .collect()
        };
        if tab_updates.is_empty() {
            return false;
        }
        for (index, row_count) in tab_updates {
            self.set_result_tab_state(index, ResultTabStatus::Canceling, row_count);
        }
        self.fire_on_change_callback();
        true
    }

    pub fn clear_lazy_fetch_session(&mut self, index: usize, session_id: u64, run_pending: bool) {
        let tab_parts = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(index)
            .map(|tab| (tab.row_count, tab.table.clone()));
        let table = if let Some((row_count, table)) = tab_parts {
            self.set_result_tab_state(index, ResultTabStatus::Done, row_count);
            Some(table)
        } else {
            None
        };
        if let Some(mut table) = table {
            table.clear_lazy_fetch_session(session_id, run_pending);
        }
        self.fire_on_change_callback();
    }

    pub fn abort_lazy_fetch_session(&mut self, session_id: u64) -> bool {
        let tab_updates: Vec<(usize, ResultTabStatus, usize, ResultTableWidget)> = {
            let data = self
                .data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            data.iter()
                .enumerate()
                .filter_map(|(index, tab)| {
                    if tab.table.active_lazy_fetch_session() == Some(session_id) {
                        let status = if tab.status == ResultTabStatus::Error {
                            ResultTabStatus::Error
                        } else {
                            ResultTabStatus::Cancelled
                        };
                        Some((index, status, tab.row_count, tab.table.clone()))
                    } else {
                        None
                    }
                })
                .collect()
        };
        if tab_updates.is_empty() {
            return false;
        }
        let mut tables = Vec::with_capacity(tab_updates.len());
        for (index, status, row_count, table) in tab_updates {
            self.set_result_tab_state(index, status, row_count);
            tables.push(table);
        }
        for mut table in tables {
            table.clear_lazy_fetch_session(session_id, false);
            table.finish_streaming();
        }
        self.fire_on_change_callback();
        true
    }

    pub fn finish_all_streaming(&mut self) {
        let tables: Vec<ResultTableWidget> = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|tab| tab.table.clone())
            .collect();
        for mut table in tables {
            table.finish_streaming();
        }
    }

    pub fn finish_non_lazy_streaming(&mut self) {
        let tables: Vec<ResultTableWidget> = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter_map(|tab| {
                if tab.table.active_lazy_fetch_session().is_none() {
                    Some(tab.table.clone())
                } else {
                    None
                }
            })
            .collect();
        for mut table in tables {
            table.finish_streaming();
        }
    }

    pub fn clear_all_lazy_fetch_state_for_abort(&mut self) {
        let tables: Vec<ResultTableWidget> = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|tab| tab.table.clone())
            .collect();
        for mut table in tables {
            table.clear_lazy_fetch_state_for_abort();
        }
        self.fire_on_change_callback();
    }

    pub fn clear_orphaned_save_requests(&mut self) -> usize {
        let tables: Vec<ResultTableWidget> = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|tab| tab.table.clone())
            .collect();
        let mut cleared = 0usize;
        for mut table in tables {
            if table.clear_orphaned_save_request() {
                cleared = cleared.saturating_add(1);
            }
        }
        if cleared > 0 {
            self.fire_on_change_callback();
        }
        cleared
    }

    pub fn clear_orphaned_query_edit_backups(&mut self) -> usize {
        let tables: Vec<ResultTableWidget> = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|tab| tab.table.clone())
            .collect();
        let mut restored = 0usize;
        for mut table in tables {
            if table.clear_orphaned_query_edit_backup() {
                restored = restored.saturating_add(1);
            }
        }
        if restored > 0 {
            self.fire_on_change_callback();
        }
        restored
    }

    pub fn align_tab_strip_left(&mut self) {
        self.reset_tab_strip_left_anchor();
        self.tabs.redraw();
    }

    pub fn display_result(&mut self, index: usize, result: &crate::db::QueryResult) {
        let status = ResultTabStatus::from_query_result(result);
        let table = self
            .set_result_tab_state(index, status, result.row_count)
            .map(|(_, table)| table);
        if let Some(table) = table {
            let mut table = table;
            table.display_result(result);
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

    pub fn set_lazy_fetch_callback(&mut self, callback: LazyFetchCallback) {
        *self
            .lazy_fetch_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some(Box::new(move |id, request| {
                ResultTabsWidget::invoke_lazy_fetch_callback(&callback, id, request);
            }));
        let tabs = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for tab in tabs.iter() {
            let mut table = tab.table.clone();
            table.set_lazy_fetch_callback(self.lazy_fetch_callback.clone());
        }
    }

    pub fn set_context_action_callback(&mut self, callback: ResultTableContextActionCallback) {
        let mut guard = self
            .context_action_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = Some(Box::new(move |action| {
            let mut callback_fn = callback
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take();
            if let Some(callback_fn) = callback_fn.as_mut() {
                callback_fn(action);
            }
            let mut callback_guard = callback
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if callback_guard.is_none() {
                *callback_guard = callback_fn;
            }
        }));
        let tabs = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for tab in tabs.iter() {
            let mut table = tab.table.clone();
            table.set_context_action_callback(self.context_action_callback.clone());
        }
    }

    pub fn export_to_csv(&self) -> String {
        self.current_table()
            .map(|table| table.export_to_csv())
            .unwrap_or_default()
    }

    pub fn export_to_csv_after_fetch_all(
        &self,
        callback: Box<dyn FnMut(String, usize)>,
    ) -> Option<(String, usize)> {
        self.current_table()
            .and_then(|table| table.export_to_csv_after_fetch_all(callback))
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

    pub fn is_current_save_pending(&self) -> bool {
        self.current_table()
            .map(|table| table.is_save_pending())
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
        if !group.was_deleted() && !table_widget.was_deleted() && group.find(&table_widget) >= 0 {
            group.remove(&table_widget);
        }
        if !table_widget.was_deleted() {
            fltk::table::Table::delete(table_widget);
        }
        if !group.was_deleted() {
            group.clear();
        }

        // Step 4: Remove group from tabs and delete
        if !self.tabs.was_deleted() && !group.was_deleted() && self.tabs.find(&group) >= 0 {
            self.tabs.remove(&group);
        }
        if !group.was_deleted() {
            fltk::group::Group::delete(group);
        }
    }

    /// Close the currently active result tab, freeing its data and FLTK resources.
    /// Returns true if a tab was closed.
    pub fn close_current_tab(&mut self) -> bool {
        self.close_current_tab_and_take_lazy_fetch().is_some()
    }

    pub fn close_current_script_output_tab(&mut self) -> bool {
        if !self.script_output_tab_is_current() {
            return false;
        }

        let next_group = self
            .data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .first()
            .map(|tab| tab.group.clone());
        self.clear_script_output();
        if let Some(group) = next_group {
            if !self.tabs.was_deleted() && !group.was_deleted() {
                let _ = self.tabs.set_value(&group);
                *self
                    .active_index
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(0);
            }
        } else {
            *self
                .active_index
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        }
        self.fire_on_change_callback();
        true
    }

    pub fn close_current_tab_and_take_lazy_fetch(&mut self) -> Option<(usize, Option<u64>)> {
        let index = (*self
            .active_index
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()))?;

        let _pointer_suppress_guard =
            PointerEventSuppressGuard::new(self.suppress_pointer_event_depth.clone());
        let (tab, replacement_group) = {
            let mut data = self
                .data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if index >= data.len() {
                return None;
            }
            let replacement_group = if data.len() > 1 {
                let replacement_index = if index + 1 < data.len() {
                    index + 1
                } else {
                    index.saturating_sub(1)
                };
                data.get(replacement_index).map(|tab| tab.group.clone())
            } else {
                None
            };
            (data.remove(index), replacement_group)
        };
        let lazy_fetch_session = tab.table.active_lazy_fetch_session();

        if let Some(group) = replacement_group.as_ref() {
            if !self.tabs.was_deleted() && !group.was_deleted() {
                let _ = self.tabs.set_value(group);
            }
        }

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
            if self.script_output_tab_is_visible() {
                let script_group = self
                    .script_output
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .group
                    .clone();
                if !self.tabs.was_deleted() && !script_group.was_deleted() {
                    let _ = self.tabs.set_value(&script_group);
                }
            }
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
                if !self.tabs.was_deleted() && !group.was_deleted() {
                    let _ = self.tabs.set_value(&group);
                }
            }
        }

        if !self.tabs.was_deleted() {
            self.reset_tab_strip_left_anchor();
            self.tabs.redraw();
        }
        self.fire_on_change_callback();
        Some((index, lazy_fetch_session))
    }

    pub fn select_script_output(&mut self) {
        let _pointer_suppress_guard =
            PointerEventSuppressGuard::new(self.suppress_pointer_event_depth.clone());
        let script_group = self
            .script_output
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .group
            .clone();
        if !self.tabs.was_deleted()
            && !script_group.was_deleted()
            && Self::tabs_contains_group(&self.tabs, &script_group)
        {
            let _ = self.tabs.set_value(&script_group);
        } else {
            return;
        }
        *self
            .active_index
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        self.fire_on_change_callback();
    }

    fn clear_script_output(&mut self) {
        {
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
        self.hide_script_output_tab();
    }
}

impl Default for ResultTabsWidget {
    fn default() -> Self {
        Self::new(0, 0, 100, 100)
    }
}

#[cfg(test)]
mod tests {
    use crate::db::QueryResult;
    use crate::ui::result_table::LazyFetchCallback;
    use crate::ui::sql_editor::LazyFetchRequest;
    use fltk::enums::Event;

    use super::{ResultTabStatus, ResultTabsWidget};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn tab_strip_left_anchor_reset_requires_multi_tab_layout() {
        assert!(!ResultTabsWidget::should_reset_tab_strip_left_anchor(
            0, 320, 240
        ));
        assert!(!ResultTabsWidget::should_reset_tab_strip_left_anchor(
            1, 320, 240
        ));
        assert!(ResultTabsWidget::should_reset_tab_strip_left_anchor(
            2, 320, 240
        ));
    }

    #[test]
    fn mouse_wheel_overflow_reapply_allows_single_tab() {
        assert!(!ResultTabsWidget::should_reapply_tab_overflow_mode_on_wheel(0, 320, 240));
        assert!(ResultTabsWidget::should_reapply_tab_overflow_mode_on_wheel(
            1, 320, 240
        ));
        assert!(!ResultTabsWidget::should_reapply_tab_overflow_mode_on_wheel(1, 0, 240));
        assert!(!ResultTabsWidget::should_reapply_tab_overflow_mode_on_wheel(1, 320, 0));
    }

    #[test]
    fn empty_result_tabs_consume_pointer_events() {
        assert!(ResultTabsWidget::should_consume_empty_tab_pointer_event(
            0,
            Event::Push
        ));
        assert!(ResultTabsWidget::should_consume_empty_tab_pointer_event(
            0,
            Event::Released
        ));
        assert!(ResultTabsWidget::should_consume_empty_tab_pointer_event(
            0,
            Event::MouseWheel
        ));
        assert!(!ResultTabsWidget::should_consume_empty_tab_pointer_event(
            1,
            Event::Push
        ));
        assert!(!ResultTabsWidget::should_consume_empty_tab_pointer_event(
            0,
            Event::KeyDown
        ));
    }

    #[test]
    fn result_tab_label_uses_status_and_row_count() {
        assert_eq!(
            ResultTabsWidget::result_tab_label(ResultTabStatus::Running, 0),
            "Running (0)"
        );
        assert_eq!(
            ResultTabsWidget::result_tab_label(ResultTabStatus::Fetching, 42),
            "Fetching (42)"
        );
        assert_eq!(
            ResultTabsWidget::result_tab_label(ResultTabStatus::Waiting, 42),
            "Waiting (42)"
        );
        assert_eq!(
            ResultTabsWidget::result_tab_label(ResultTabStatus::Canceling, 42),
            "Canceling (42)"
        );
        assert_eq!(
            ResultTabsWidget::result_tab_label(ResultTabStatus::Done, 128),
            "Done (128)"
        );
        assert_eq!(
            ResultTabsWidget::result_tab_label(ResultTabStatus::Error, 0),
            "Error (0)"
        );
        assert_eq!(
            ResultTabsWidget::result_tab_label(ResultTabStatus::Cancelled, 0),
            "Cancelled (0)"
        );
    }

    #[test]
    fn result_status_uses_shared_terminal_state_mapping() {
        let done = QueryResult::new_select("select 1", Vec::new(), Vec::new(), Duration::ZERO);
        let mut cancelled = QueryResult::new_error("select sleep", "Query cancelled");
        cancelled.message = "Query cancelled".to_string();
        let prefixed_cancelled = QueryResult::new_error("select sleep", "Query cancelled");
        let mut american_canceled = QueryResult::new_error("select sleep", "Query canceled");
        american_canceled.message = "ERROR: Query canceled".to_string();
        let error = QueryResult::new_error("select missing", "table not found");

        assert_eq!(
            ResultTabStatus::from_query_result(&done),
            ResultTabStatus::Done
        );
        assert_eq!(
            ResultTabStatus::from_query_result(&cancelled),
            ResultTabStatus::Cancelled
        );
        assert_eq!(
            ResultTabStatus::from_query_result(&prefixed_cancelled),
            ResultTabStatus::Cancelled
        );
        assert_eq!(
            ResultTabStatus::from_query_result(&american_canceled),
            ResultTabStatus::Cancelled
        );
        assert_eq!(
            ResultTabStatus::from_query_result(&error),
            ResultTabStatus::Error
        );
    }

    #[test]
    fn status_bar_message_uses_same_state_labels() {
        assert_eq!(
            ResultTabStatus::Fetching.status_bar_message_with_rows(42),
            "Fetching rows: 42"
        );
        assert_eq!(
            ResultTabStatus::Canceling.status_bar_message(),
            ResultTabStatus::Canceling.label()
        );
    }

    #[test]
    fn stream_updates_do_not_overwrite_terminal_or_canceling_status() {
        assert_eq!(
            ResultTabStatus::for_stream_update(ResultTabStatus::Running),
            ResultTabStatus::Fetching
        );
        assert_eq!(
            ResultTabStatus::for_stream_update(ResultTabStatus::Waiting),
            ResultTabStatus::Fetching
        );
        assert_eq!(
            ResultTabStatus::for_stream_update(ResultTabStatus::Canceling),
            ResultTabStatus::Canceling
        );
        assert_eq!(
            ResultTabStatus::for_stream_update(ResultTabStatus::Error),
            ResultTabStatus::Error
        );
        assert_eq!(
            ResultTabStatus::for_stream_update(ResultTabStatus::Cancelled),
            ResultTabStatus::Cancelled
        );
    }

    #[test]
    fn lazy_fetch_callback_is_invoked_without_holding_callback_lock() {
        let callback: LazyFetchCallback = Arc::new(Mutex::new(None));
        let callback_for_assert = callback.clone();
        *callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some(Box::new(move |session_id, request| {
                assert_eq!(session_id, 11);
                assert_eq!(request, LazyFetchRequest::Cancel);
                assert!(callback_for_assert.try_lock().is_ok());
            }));

        ResultTabsWidget::invoke_lazy_fetch_callback(&callback, 11, LazyFetchRequest::Cancel);
    }
}
