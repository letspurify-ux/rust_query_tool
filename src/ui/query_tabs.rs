use fltk::{
    enums::Align,
    enums::Event,
    group::{Group, Tabs, TabsOverflow},
    prelude::*,
};
use std::any::Any;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

use crate::ui::constants::TAB_HEADER_HEIGHT;
use crate::ui::theme;

pub type QueryTabId = u64;
type TabSelectCallback = Box<dyn FnMut(QueryTabId)>;

#[derive(Clone)]
pub struct QueryTabsWidget {
    tabs: Tabs,
    entries: Arc<Mutex<Vec<TabEntry>>>,
    next_id: Arc<Mutex<QueryTabId>>,
    on_select: Arc<Mutex<Option<TabSelectCallback>>>,
    suppress_select_callback_depth: Arc<Mutex<u32>>,
    suppress_pointer_event_depth: Arc<Mutex<u32>>,
}

#[derive(Clone)]
struct TabEntry {
    id: QueryTabId,
    group: Group,
}

struct CallbackSuppressGuard {
    counter: Arc<Mutex<u32>>,
}

impl CallbackSuppressGuard {
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

impl Drop for CallbackSuppressGuard {
    fn drop(&mut self) {
        let mut guard = self
            .counter
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = guard.saturating_sub(1);
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

impl QueryTabsWidget {
    fn panic_payload_to_string(payload: &(dyn Any + Send)) -> String {
        if let Some(message) = payload.downcast_ref::<&str>() {
            (*message).to_string()
        } else if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else {
            "unknown panic payload".to_string()
        }
    }

    fn invoke_on_select_callback(
        callback_slot: &Arc<Mutex<Option<TabSelectCallback>>>,
        tab_id: QueryTabId,
    ) {
        let callback = {
            let mut slot = callback_slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            slot.take()
        };

        if let Some(mut cb) = callback {
            let callback_result = panic::catch_unwind(AssertUnwindSafe(|| cb(tab_id)));
            let mut slot = callback_slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if slot.is_none() {
                *slot = Some(cb);
            }
            if let Err(payload) = callback_result {
                let panic_payload = Self::panic_payload_to_string(payload.as_ref());
                crate::utils::logging::log_error(
                    "query_tabs::callback",
                    &format!("tab select callback panicked: {panic_payload}"),
                );
                eprintln!("tab select callback panicked: {panic_payload}");
            }
        }
    }

    fn content_bounds(tabs: &Tabs) -> (i32, i32, i32, i32) {
        // Keep a stable tab-header height regardless of surrounding splitter drags.
        // This avoids top/bottom header bar height jitter while panes are resized.
        let x = tabs.x();
        let y = tabs.y() + TAB_HEADER_HEIGHT;
        let w = tabs.w();
        let h = tabs.h() - TAB_HEADER_HEIGHT;
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

    fn should_reset_tab_strip_left_anchor(child_count: i32, width: i32, height: i32) -> bool {
        child_count > 1 && width > 0 && height > 0
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

    fn reset_tab_strip_left_anchor(&mut self) {
        // Re-applying overflow mode resets FLTK's internal tab offset,
        // keeping the visible strip anchored from the left. Skip transient
        // empty/single-tab states while tabs are being closed/recreated
        // because FLTK does not need overflow math there.
        if Self::should_reset_tab_strip_left_anchor(
            self.tabs.children(),
            self.tabs.w(),
            self.tabs.h(),
        ) {
            self.tabs.handle_overflow(TabsOverflow::Pulldown);
        }
    }

    fn maybe_shrink_entry_storage(entries: &mut Vec<TabEntry>) {
        // Closing many tabs can leave tab metadata capacity heavily over-allocated.
        // Shrink only when substantially over-provisioned to avoid churn.
        let len = entries.len();
        let capacity = entries.capacity();
        if len == 0 || (capacity > 0 && len.saturating_mul(2) < capacity) {
            entries.shrink_to_fit();
        }
    }

    pub fn new(x: i32, y: i32, w: i32, h: i32) -> Self {
        let mut tabs = Tabs::new(x, y, w, h, None);
        tabs.end();
        tabs.set_color(theme::panel_bg());
        tabs.set_selection_color(theme::selection_strong());
        tabs.set_label_color(theme::text_secondary());
        tabs.set_label_size((TAB_HEADER_HEIGHT - 8).max(8));
        // Center labels in tab headers.
        tabs.set_tab_align(Align::Center);
        // Keep tab header widths stable while surrounding panes are resized.
        // `Compress` dynamically shrinks/expands tab buttons as width changes,
        // which causes distracting header size jumps during splitter drags.
        tabs.handle_overflow(TabsOverflow::Pulldown);

        let entries = Arc::new(Mutex::new(Vec::<TabEntry>::new()));
        let next_id = Arc::new(Mutex::new(1u64));
        let on_select = Arc::new(Mutex::new(None::<TabSelectCallback>));
        let suppress_select_callback_depth = Arc::new(Mutex::new(0u32));
        let suppress_pointer_event_depth = Arc::new(Mutex::new(0u32));

        let entries_for_cb = entries.clone();
        let on_select_for_cb = on_select.clone();
        let suppress_for_cb = suppress_select_callback_depth.clone();
        let suppress_pointer_for_cb = suppress_pointer_event_depth.clone();
        tabs.set_callback(move |tabs| {
            if *suppress_for_cb
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                > 0
            {
                return;
            }
            let Some(selected) = tabs.value() else {
                return;
            };
            let selected_ptr = selected.as_widget_ptr();
            let selected_id = entries_for_cb
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .iter()
                .find(|entry| entry.group.as_widget_ptr() == selected_ptr)
                .map(|entry| entry.id);
            if let Some(tab_id) = selected_id {
                Self::invoke_on_select_callback(&on_select_for_cb, tab_id);
            }
        });
        tabs.resize_callback(move |t, _, _, _, _| {
            Self::layout_children(t);
        });
        tabs.handle(move |tabs, ev| {
            if Self::should_suppress_pointer_event(&suppress_pointer_for_cb, ev) {
                return true;
            }
            if matches!(ev, Event::MouseWheel)
                && Self::should_reset_tab_strip_left_anchor(tabs.children(), tabs.w(), tabs.h())
            {
                tabs.handle_overflow(TabsOverflow::Pulldown);
                return true;
            }
            false
        });

        Self {
            tabs,
            entries,
            next_id,
            on_select,
            suppress_select_callback_depth,
            suppress_pointer_event_depth,
        }
    }

    pub fn set_on_select<F>(&mut self, callback: F)
    where
        F: FnMut(QueryTabId) + 'static,
    {
        *self
            .on_select
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(callback));
    }

    pub fn get_widget(&self) -> Tabs {
        self.tabs.clone()
    }

    pub fn add_tab(&mut self, label: &str) -> QueryTabId {
        let tab_id = {
            let mut next = self
                .next_id
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let id = *next;
            *next = next.saturating_add(1);
            id
        };
        self.tabs.begin();
        let (x, y, w, h) = Self::content_bounds(&self.tabs);
        let mut group = Group::new(x, y, w, h, None).with_label(&Self::display_label(label));
        group.set_color(theme::panel_bg());
        group.set_label_color(theme::text_secondary());
        group.set_align(Align::Center | Align::Inside);
        group.end();
        self.tabs.end();

        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(TabEntry {
                id: tab_id,
                group: group.clone(),
            });
        let _pointer_suppress_guard =
            PointerEventSuppressGuard::new(self.suppress_pointer_event_depth.clone());
        let _suppress_guard =
            CallbackSuppressGuard::new(self.suppress_select_callback_depth.clone());
        let _ = self.tabs.set_value(&group);
        self.reset_tab_strip_left_anchor();
        Self::layout_children(&self.tabs);
        self.tabs.redraw();
        tab_id
    }

    pub fn select(&mut self, tab_id: QueryTabId) {
        if let Some(group) = self.tab_group(tab_id) {
            let _pointer_suppress_guard =
                PointerEventSuppressGuard::new(self.suppress_pointer_event_depth.clone());
            let _suppress_guard =
                CallbackSuppressGuard::new(self.suppress_select_callback_depth.clone());
            let _ = self.tabs.set_value(&group);
            self.reset_tab_strip_left_anchor();
            self.tabs.redraw();
        }
    }

    pub fn selected_id(&self) -> Option<QueryTabId> {
        let selected = self.tabs.value()?;
        let selected_ptr = selected.as_widget_ptr();
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .find(|entry| entry.group.as_widget_ptr() == selected_ptr)
            .map(|entry| entry.id)
    }

    pub fn set_tab_label(&mut self, tab_id: QueryTabId, label: &str) {
        if let Some(group) = self.tab_group(tab_id) {
            let _pointer_suppress_guard =
                PointerEventSuppressGuard::new(self.suppress_pointer_event_depth.clone());
            let mut group = group;
            group.set_label(&Self::display_label(label));
            group.set_align(Align::Center | Align::Inside);
            self.tabs.redraw();
        }
    }

    pub fn close_tab(&mut self, tab_id: QueryTabId) -> bool {
        let (group, replacement_group) = {
            let mut entries = self
                .entries
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(index) = entries.iter().position(|entry| entry.id == tab_id) else {
                return false;
            };
            let group = entries.remove(index).group;
            let replacement = entries
                .get(index)
                .or_else(|| index.checked_sub(1).and_then(|prev| entries.get(prev)))
                .map(|entry| entry.group.clone());
            Self::maybe_shrink_entry_storage(&mut entries);
            (group, replacement)
        };

        let _pointer_suppress_guard =
            PointerEventSuppressGuard::new(self.suppress_pointer_event_depth.clone());
        let _suppress_guard =
            CallbackSuppressGuard::new(self.suppress_select_callback_depth.clone());
        if let Some(replacement_group) = replacement_group {
            if !replacement_group.was_deleted() && self.tabs.find(&replacement_group) >= 0 {
                let _ = self.tabs.set_value(&replacement_group);
            }
        }
        if !self.tabs.was_deleted() && self.tabs.find(&group) >= 0 {
            self.tabs.remove(&group);
        }
        if !group.was_deleted() {
            fltk::group::Group::delete(group);
        }
        self.reset_tab_strip_left_anchor();
        Self::layout_children(&self.tabs);
        self.tabs.redraw();
        true
    }

    pub fn tab_group(&self, tab_id: QueryTabId) -> Option<Group> {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .find(|entry| entry.id == tab_id)
            .map(|entry| entry.group.clone())
    }

    pub fn tab_ids(&self) -> Vec<QueryTabId> {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|entry| entry.id)
            .collect()
    }

    fn display_label(label: &str) -> String {
        label.to_string()
    }
}

impl Default for QueryTabsWidget {
    fn default() -> Self {
        Self::new(0, 0, 100, 100)
    }
}

#[cfg(test)]
mod tests {
    use super::QueryTabsWidget;
    use fltk::enums::Event;
    use std::sync::{Arc, Mutex};

    #[test]
    fn tab_strip_left_anchor_reset_requires_real_overflow_state() {
        assert!(!QueryTabsWidget::should_reset_tab_strip_left_anchor(
            0, 320, 240
        ));
        assert!(!QueryTabsWidget::should_reset_tab_strip_left_anchor(
            1, 320, 240
        ));
        assert!(!QueryTabsWidget::should_reset_tab_strip_left_anchor(
            2, 0, 240
        ));
        assert!(!QueryTabsWidget::should_reset_tab_strip_left_anchor(
            2, 320, 0
        ));
        assert!(QueryTabsWidget::should_reset_tab_strip_left_anchor(
            2, 320, 240
        ));
    }

    #[test]
    fn pointer_event_suppression_only_applies_to_mouse_driven_tab_events() {
        let depth = Arc::new(Mutex::new(1u32));

        assert!(QueryTabsWidget::should_suppress_pointer_event(
            &depth,
            Event::Move
        ));
        assert!(QueryTabsWidget::should_suppress_pointer_event(
            &depth,
            Event::Push
        ));
        assert!(QueryTabsWidget::should_suppress_pointer_event(
            &depth,
            Event::Released
        ));
        assert!(!QueryTabsWidget::should_suppress_pointer_event(
            &depth,
            Event::KeyDown
        ));
    }
}
