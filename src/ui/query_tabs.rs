use fltk::{
    enums::Align,
    enums::Event,
    group::{Group, Tabs, TabsOverflow},
    prelude::*,
};
use std::any::Any;
use std::cell::{Cell, RefCell};
use std::panic::{self, AssertUnwindSafe};
use std::rc::Rc;

use crate::ui::constants::TAB_HEADER_HEIGHT;
use crate::ui::theme;

pub type QueryTabId = u64;
type TabSelectCallback = Box<dyn FnMut(QueryTabId)>;

#[derive(Clone)]
pub struct QueryTabsWidget {
    tabs: Tabs,
    entries: Rc<RefCell<Vec<TabEntry>>>,
    next_id: Rc<RefCell<QueryTabId>>,
    on_select: Rc<RefCell<Option<TabSelectCallback>>>,
    suppress_select_callback_depth: Rc<Cell<u32>>,
}

#[derive(Clone)]
struct TabEntry {
    id: QueryTabId,
    group: Group,
}

struct CallbackSuppressGuard {
    counter: Rc<Cell<u32>>,
}

impl CallbackSuppressGuard {
    fn new(counter: Rc<Cell<u32>>) -> Self {
        counter.set(counter.get().saturating_add(1));
        Self { counter }
    }
}

impl Drop for CallbackSuppressGuard {
    fn drop(&mut self) {
        self.counter.set(self.counter.get().saturating_sub(1));
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
        callback_slot: &Rc<RefCell<Option<TabSelectCallback>>>,
        tab_id: QueryTabId,
    ) {
        let callback = {
            let mut slot = callback_slot.borrow_mut();
            slot.take()
        };

        if let Some(mut cb) = callback {
            let callback_result = panic::catch_unwind(AssertUnwindSafe(|| cb(tab_id)));
            let mut slot = callback_slot.borrow_mut();
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

    fn reset_tab_strip_left_anchor(&mut self) {
        // Re-applying overflow mode resets FLTK's internal tab offset,
        // keeping the visible strip anchored from the left.
        self.tabs.handle_overflow(TabsOverflow::Pulldown);
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
        tabs.handle(move |tabs, ev| {
            if matches!(ev, Event::MouseWheel) {
                tabs.handle_overflow(TabsOverflow::Pulldown);
                return true;
            }
            false
        });

        let entries = Rc::new(RefCell::new(Vec::<TabEntry>::new()));
        let next_id = Rc::new(RefCell::new(1u64));
        let on_select = Rc::new(RefCell::new(None::<TabSelectCallback>));
        let suppress_select_callback_depth = Rc::new(Cell::new(0u32));

        let entries_for_cb = entries.clone();
        let on_select_for_cb = on_select.clone();
        let suppress_for_cb = suppress_select_callback_depth.clone();
        tabs.set_callback(move |tabs| {
            if suppress_for_cb.get() > 0 {
                return;
            }
            let Some(selected) = tabs.value() else {
                return;
            };
            let selected_ptr = selected.as_widget_ptr();
            let selected_id = entries_for_cb
                .borrow()
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

        Self {
            tabs,
            entries,
            next_id,
            on_select,
            suppress_select_callback_depth,
        }
    }

    pub fn set_on_select<F>(&mut self, callback: F)
    where
        F: FnMut(QueryTabId) + 'static,
    {
        *self.on_select.borrow_mut() = Some(Box::new(callback));
    }

    pub fn get_widget(&self) -> Tabs {
        self.tabs.clone()
    }

    pub fn add_tab(&mut self, label: &str) -> QueryTabId {
        let tab_id = {
            let mut next = self.next_id.borrow_mut();
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

        self.entries.borrow_mut().push(TabEntry {
            id: tab_id,
            group: group.clone(),
        });
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
            .borrow()
            .iter()
            .find(|entry| entry.group.as_widget_ptr() == selected_ptr)
            .map(|entry| entry.id)
    }

    pub fn set_tab_label(&mut self, tab_id: QueryTabId, label: &str) {
        if let Some(group) = self.tab_group(tab_id) {
            let mut group = group;
            group.set_label(&Self::display_label(label));
            group.set_align(Align::Center | Align::Inside);
            self.tabs.redraw();
        }
    }

    pub fn close_tab(&mut self, tab_id: QueryTabId) -> bool {
        let group = {
            let mut entries = self.entries.borrow_mut();
            let Some(index) = entries.iter().position(|entry| entry.id == tab_id) else {
                return false;
            };
            let group = entries.remove(index).group;
            Self::maybe_shrink_entry_storage(&mut entries);
            group
        };

        let _suppress_guard =
            CallbackSuppressGuard::new(self.suppress_select_callback_depth.clone());
        if self.tabs.find(&group) >= 0 {
            self.tabs.remove(&group);
        }
        fltk::group::Group::delete(group);
        self.reset_tab_strip_left_anchor();
        Self::layout_children(&self.tabs);
        self.tabs.redraw();
        true
    }

    pub fn tab_group(&self, tab_id: QueryTabId) -> Option<Group> {
        self.entries
            .borrow()
            .iter()
            .find(|entry| entry.id == tab_id)
            .map(|entry| entry.group.clone())
    }

    pub fn tab_ids(&self) -> Vec<QueryTabId> {
        self.entries.borrow().iter().map(|entry| entry.id).collect()
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
