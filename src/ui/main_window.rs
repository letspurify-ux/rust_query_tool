use fltk::{
    app,
    button::Button,
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
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::{Rc, Weak};
use std::thread;
use std::time::{Duration, Instant};

use crate::db::{
    create_shared_connection, format_connection_busy_message, lock_connection_with_activity,
    try_lock_connection_with_activity, ObjectBrowser, QueryResult, SharedConnection,
};
use crate::ui::constants::*;
use crate::ui::theme;
use crate::ui::{
    font_settings, show_settings_dialog, ConnectionDialog, FindReplaceDialog, HighlightData,
    IntellisenseData, MenuBarBuilder, ObjectBrowserWidget, QueryHistoryDialog, QueryProgress,
    QueryTabId, QueryTabsWidget, ResultTabsWidget, SqlAction, SqlEditorWidget,
};
use crate::utils::{AppConfig, QueryHistory};

#[derive(Clone)]
struct SchemaUpdate {
    data: IntellisenseData,
    highlight_data: HighlightData,
}

#[derive(Clone)]
struct QueryEditorTab {
    tab_id: QueryTabId,
    base_label: String,
    sql_editor: SqlEditorWidget,
    sql_buffer: TextBuffer,
    current_file: Option<PathBuf>,
    is_dirty: bool,
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
    query_timeout_input: IntInput,
    pub result_tabs: ResultTabsWidget,
    pub result_tab_offset: usize,
    pub object_browser: ObjectBrowserWidget,
    pub status_bar: Frame,
    pub fetch_row_counts: HashMap<usize, usize>,
    pub current_file: Rc<RefCell<Option<PathBuf>>>,
    pub popups: Rc<RefCell<Vec<Window>>>,
    pub window: Window,
    pub right_tile: Tile,
    pub query_split_adjusted: Rc<Cell<bool>>,
    pub connection_info: Rc<RefCell<Option<crate::db::ConnectionInfo>>>,
    pub config: Rc<RefCell<AppConfig>>,
    pub last_fetch_status_update: Instant,
    schema_sender: Option<std::sync::mpsc::Sender<SchemaUpdate>>,
    file_sender: Option<std::sync::mpsc::Sender<FileActionResult>>,
}

impl AppState {
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

    fn set_active_editor_tab(&mut self, tab_id: QueryTabId) -> bool {
        let Some(index) = self.find_tab_index(tab_id) else {
            return false;
        };
        let tab = self.editor_tabs[index].clone();
        self.active_editor_tab_id = tab_id;
        self.sql_editor = tab.sql_editor;
        self.sql_buffer = tab.sql_buffer;
        *self.current_file.borrow_mut() = tab.current_file;
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

    fn set_tab_file_path(&mut self, tab_id: QueryTabId, path: Option<PathBuf>) {
        let Some(index) = self.find_tab_index(tab_id) else {
            return;
        };
        self.editor_tabs[index].current_file = path.clone();
        let label = Self::tab_display_label(&self.editor_tabs[index]);
        self.query_tabs.set_tab_label(tab_id, &label);
        if self.active_editor_tab_id == tab_id {
            *self.current_file.borrow_mut() = path;
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
}

const FETCH_STATUS_UPDATE_INTERVAL: Duration = Duration::from_millis(250);
const CONNECTION_HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// 접속 정보를 상태 표시줄 메시지 끝에 붙는 헬퍼
fn format_status(msg: &str, conn_info: &Option<crate::db::ConnectionInfo>) -> String {
    match conn_info {
        Some(info) => format!("{} | {}", msg, info.display_string()),
        None => msg.to_string(),
    }
}

pub struct MainWindow {
    state: Rc<RefCell<AppState>>,
}

#[derive(Clone)]
enum ConnectionResult {
    Success(crate::db::ConnectionInfo),
    Failure(String),
}

enum ConnectionHealthResult {
    Checked(Option<String>),
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

impl MainWindow {
    fn transition_to_disconnected_state(state: &mut AppState, error_message: Option<&str>) {
        *state.connection_info.borrow_mut() = None;
        state.status_bar.set_label("Disconnected");
        let reset_data = IntellisenseData::new();
        let reset_highlight = HighlightData::new();
        Self::apply_schema_to_all_editors(state, &reset_data, &reset_highlight);

        if let Some(message) = error_message {
            crate::utils::logging::log_error("connection", message);
            state
                .result_tabs
                .append_script_output_lines(&[message.to_string()]);
            state.result_tabs.select_script_output();
        }
    }

    fn cancel_all_running_queries(state: &Rc<RefCell<AppState>>) {
        let (running_editors, fallback_editor) = {
            let s = state.borrow();
            let running_editors = s
                .editor_tabs
                .iter()
                .filter(|tab| tab.sql_editor.is_query_running())
                .map(|tab| tab.sql_editor.clone())
                .collect::<Vec<_>>();
            (running_editors, s.sql_editor.clone())
        };

        if running_editors.is_empty() {
            fallback_editor.cancel_current();
            return;
        }

        for editor in &running_editors {
            editor.cancel_current();
        }

        if let Ok(mut s) = state.try_borrow_mut() {
            let conn_info = s.connection_info.borrow().clone();
            s.status_bar
                .set_label(&format_status("Cancelling running queries...", &conn_info));
        }
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
        let conn_info = state.connection_info.borrow().clone();
        state.status_bar.set_label(&format_status(
            &format!("{} is already open. Switched to existing tab", file_name),
            &conn_info,
        ));
        true
    }

    fn save_tab(
        state: &Rc<RefCell<AppState>>,
        tab_id: QueryTabId,
        force_save_as: bool,
    ) -> SaveTabOutcome {
        let (current_file, sql_text) = {
            let s = state.borrow();
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

        if let Err(err) = fs::write(&path, sql_text) {
            return SaveTabOutcome::Failed(err.to_string());
        }

        let mut s = state.borrow_mut();
        s.set_tab_file_path(tab_id, Some(path.clone()));
        s.set_tab_dirty(tab_id, false);
        let file_label = path.file_name().unwrap_or_default().to_string_lossy();
        let conn_info = s.connection_info.borrow().clone();
        s.status_bar
            .set_label(&format_status(&format!("Saved {}", file_label), &conn_info));
        SaveTabOutcome::Saved
    }

    fn confirm_save_if_dirty(
        state: &Rc<RefCell<AppState>>,
        tab_id: QueryTabId,
        action_verb: &str,
    ) -> bool {
        let (is_dirty, tab_label) = {
            let s = state.borrow();
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

    fn confirm_save_for_all_dirty_tabs(state: &Rc<RefCell<AppState>>) -> bool {
        let tab_ids = state.borrow().query_tabs.tab_ids();
        for tab_id in tab_ids {
            if !Self::confirm_save_if_dirty(state, tab_id, "exiting") {
                return false;
            }
        }
        true
    }

    pub fn new() -> Self {
        let config = AppConfig::load();
        let connection = create_shared_connection();

        let current_group = fltk::group::Group::try_current();

        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let mut window = Window::default()
            .with_size(1200, 800)
            .with_label("SPACE Query")
            .center_screen();
        window.set_id("main_window");
        window.set_color(theme::window_bg());

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
        cancel_btn.set_color(theme::button_warning());
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

        let drag_state = Rc::new(RefCell::new(None::<(i32, i32)>));
        let mut content_flex_for_split = content_flex.clone();
        let obj_browser_for_split = obj_browser_widget.clone();
        let drag_state_for_split = drag_state.clone();
        split_bar.handle(move |_bar, ev| match ev {
            fltk::enums::Event::Enter | fltk::enums::Event::Move => {
                set_cursor(Cursor::WE);
                true
            }
            fltk::enums::Event::Push => {
                *drag_state_for_split.borrow_mut() =
                    Some((app::event_x(), obj_browser_for_split.w()));
                true
            }
            fltk::enums::Event::Drag => {
                if let Some((start_x, start_w)) = *drag_state_for_split.borrow() {
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
                *drag_state_for_split.borrow_mut() = None;
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

        let query_split_adjusted = Rc::new(Cell::new(false));
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
            .with_label("Clear Tabs");
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

        let query_split_adjusted_for_tile = query_split_adjusted.clone();
        let mut query_top_group_for_tile = query_top_group.clone();
        let mut query_split_bar_for_tile = query_split_bar.clone();
        let split_drag_active = Rc::new(Cell::new(false));
        let split_drag_active_for_tile = split_drag_active.clone();
        right_tile.handle(move |tile, ev| {
            const SPLIT_GRAB_MARGIN: i32 = 6;
            match ev {
                fltk::enums::Event::Push => {
                    if app::event_mouse_button() == fltk::app::MouseButton::Left {
                        let split_top = query_split_bar_for_tile.y();
                        let split_bottom = split_top + query_split_bar_for_tile.h();
                        let near_split = (app::event_y() >= split_top - SPLIT_GRAB_MARGIN)
                            && (app::event_y() <= split_bottom + SPLIT_GRAB_MARGIN);
                        if near_split {
                            split_drag_active_for_tile.set(true);
                            query_split_adjusted_for_tile.set(true);
                            return true;
                        }
                    }
                    false
                }
                fltk::enums::Event::Drag => {
                    if split_drag_active_for_tile.get() {
                        query_split_adjusted_for_tile.set(true);
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
                        }
                        return true;
                    }
                    false
                }
                fltk::enums::Event::Released => {
                    if split_drag_active_for_tile.replace(false) {
                        MainWindow::clamp_query_split_with(
                            tile,
                            &mut query_top_group_for_tile,
                            &mut query_split_bar_for_tile,
                        );
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
            is_dirty: false,
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

        let state = Rc::new(RefCell::new(AppState {
            connection,
            query_tabs: query_tabs.clone(),
            query_top_group: query_top_group.clone(),
            query_split_bar: query_split_bar.clone(),
            editor_tabs,
            active_editor_tab_id: first_tab_id,
            next_editor_tab_number: 2,
            sql_editor: sql_editor.clone(),
            sql_buffer,
            query_timeout_input: timeout_input.clone(),
            result_tabs,
            result_tab_offset: 0,
            object_browser,
            status_bar,
            fetch_row_counts: HashMap::new(),
            current_file: Rc::new(RefCell::new(None)),
            popups: Rc::new(RefCell::new(Vec::new())),
            window,
            right_tile: right_tile.clone(),
            query_split_adjusted: query_split_adjusted.clone(),
            connection_info: Rc::new(RefCell::new(None)),
            config: Rc::new(RefCell::new(config)),
            last_fetch_status_update: Instant::now(),
            schema_sender: None,
            file_sender: None,
        }));

        let weak_state_for_execute = Rc::downgrade(&state);
        execute_btn.set_callback(move |_| {
            if let Some(state_for_execute) = weak_state_for_execute.upgrade() {
                state_for_execute.borrow().sql_editor.execute_current();
            }
        });

        let weak_state_for_cancel = Rc::downgrade(&state);
        cancel_btn.set_callback(move |_| {
            if let Some(state_for_cancel) = weak_state_for_cancel.upgrade() {
                MainWindow::cancel_all_running_queries(&state_for_cancel);
            }
        });

        let weak_state_for_explain = Rc::downgrade(&state);
        explain_btn.set_callback(move |_| {
            if let Some(state_for_explain) = weak_state_for_explain.upgrade() {
                state_for_explain.borrow().sql_editor.explain_current();
            }
        });

        let weak_state_for_clear_btn = Rc::downgrade(&state);
        clear_btn.set_callback(move |_| {
            if let Some(state_for_clear_btn) = weak_state_for_clear_btn.upgrade() {
                state_for_clear_btn.borrow().sql_editor.clear();
            }
        });

        let weak_state_for_commit = Rc::downgrade(&state);
        commit_btn.set_callback(move |_| {
            if let Some(state_for_commit) = weak_state_for_commit.upgrade() {
                state_for_commit.borrow().sql_editor.commit();
            }
        });

        let weak_state_for_rollback = Rc::downgrade(&state);
        rollback_btn.set_callback(move |_| {
            if let Some(state_for_rollback) = weak_state_for_rollback.upgrade() {
                state_for_rollback.borrow().sql_editor.rollback();
            }
        });

        let weak_state_for_result_close = Rc::downgrade(&state);
        close_tab_btn.set_callback(move |_| {
            let Some(state_for_result_close) = weak_state_for_result_close.upgrade() else {
                return;
            };
            if state_for_result_close.borrow().is_any_query_running() {
                fltk::dialog::alert_default("A query is running. Stop it before closing tabs.");
                return;
            }
            let mut s = state_for_result_close.borrow_mut();
            s.result_tabs.close_current_tab();
            app::redraw();
        });

        let weak_state_for_result_clear = Rc::downgrade(&state);
        clear_tabs_btn.set_callback(move |_| {
            let Some(state_for_result_clear) = weak_state_for_result_clear.upgrade() else {
                return;
            };
            if state_for_result_clear.borrow().is_any_query_running() {
                fltk::dialog::alert_default("A query is running. Stop it before clearing tabs.");
                return;
            }
            let mut s = state_for_result_clear.borrow_mut();
            s.result_tabs.clear();
            app::redraw();
        });

        let weak_state_for_query_close = Rc::downgrade(&state);
        query_close_tab_btn.set_callback(move |_| {
            let Some(state_for_query_close) = weak_state_for_query_close.upgrade() else {
                return;
            };
            let tab_id = state_for_query_close.borrow().active_editor_tab_id;
            MainWindow::close_query_editor_tab(&state_for_query_close, tab_id);
            app::redraw();
        });

        let weak_state_for_tab_select = Rc::downgrade(&state);
        query_tabs.set_on_select(move |tab_id| {
            if let Some(state_for_tab_select) = weak_state_for_tab_select.upgrade() {
                if let Ok(mut s) = state_for_tab_select.try_borrow_mut() {
                    if s.set_active_editor_tab(tab_id) {
                        s.sql_editor.focus();
                    }
                }
            }
        });

        {
            let mut state_borrow = state.borrow_mut();
            Self::adjust_query_layout(&mut state_borrow);
            Self::apply_font_settings(&mut state_borrow);
        }

        let weak_state_for_history_btn = Rc::downgrade(&state);
        query_history_btn.set_callback(move |_| {
            if let Some(state_for_history) = weak_state_for_history_btn.upgrade() {
                MainWindow::open_query_history_dialog(&state_for_history);
            }
        });

        // Restore current group
        if let Some(ref group) = current_group {
            fltk::group::Group::set_current(Some(group));
        }

        Self { state }
    }

    fn open_query_history_dialog(state: &Rc<RefCell<AppState>>) {
        let (mut buffer, mut editor, popups) = {
            let s = state.borrow_mut();
            (
                s.sql_buffer.clone(),
                s.sql_editor.get_editor(),
                s.popups.clone(),
            )
        };
        if let Some(sql) = QueryHistoryDialog::show_with_registry(popups) {
            let buffer_length = buffer.length();
            let text_to_insert = if buffer_length > 0 {
                format!("\n{}", sql)
            } else {
                sql
            };
            buffer.insert(buffer_length, &text_to_insert);

            let new_length = buffer.length();
            editor.set_insert_position(new_length);
            editor.show_insert_position();

            state.borrow().sql_editor.refresh_highlighting();
        }
    }

    fn adjust_query_layout(state: &mut AppState) {
        let mut right_tile = state.right_tile.clone();
        let mut query_top_group = state.query_top_group.clone();
        let mut query_split_bar = state.query_split_bar.clone();
        if state.query_split_adjusted.get() {
            Self::clamp_query_split_with(
                &mut right_tile,
                &mut query_top_group,
                &mut query_split_bar,
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
            let config = state.config.borrow();
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

        for popup in state.popups.borrow_mut().iter_mut() {
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
        if state.query_split_adjusted.get() {
            Self::clamp_query_split_with(
                &mut right_tile,
                &mut query_top_group,
                &mut query_split_bar,
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
        let inherited_intellisense = state.sql_editor.get_intellisense_data().borrow().clone();
        *editor.get_intellisense_data().borrow_mut() = inherited_intellisense;
        let inherited_highlight = state
            .sql_editor
            .get_highlighter()
            .borrow()
            .get_highlight_data();
        editor
            .get_highlighter()
            .borrow_mut()
            .set_highlight_data(inherited_highlight);
        let buffer = editor.get_buffer();
        state.editor_tabs.push(QueryEditorTab {
            tab_id,
            base_label: label,
            sql_editor: editor.clone(),
            sql_buffer: buffer.clone(),
            current_file: None,
            is_dirty: false,
        });
        state.query_tabs.select(tab_id);
        let _ = state.set_active_editor_tab(tab_id);
        Some(tab_id)
    }

    fn close_query_editor_tab(state: &Rc<RefCell<AppState>>, tab_id: QueryTabId) -> bool {
        {
            let s = state.borrow();
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
            let mut s = state.borrow_mut();
            let Some(index) = s.find_tab_index(tab_id) else {
                return false;
            };

            let was_active = s.active_editor_tab_id == tab_id;
            s.editor_tabs[index].sql_editor.cleanup_for_close();
            if !s.query_tabs.close_tab(tab_id) {
                return false;
            }
            s.editor_tabs.remove(index);

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
                *s.current_file.borrow_mut() = fallback_tab.current_file;
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
                *s.current_file.borrow_mut() = None;
                s.refresh_window_title();
            }

            s.right_tile.redraw();
            app::redraw();
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
            if let Ok(mut s) = state.try_borrow_mut() {
                s.sql_editor.focus();
            }
        }

        true
    }

    fn apply_schema_to_all_editors(
        state: &mut AppState,
        data: &IntellisenseData,
        highlight_data: &HighlightData,
    ) {
        let mut combined_highlight = highlight_data.clone();
        let columns_from_intellisense = Self::collect_highlight_columns(data);
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

        for tab in &mut state.editor_tabs {
            *tab.sql_editor.get_intellisense_data().borrow_mut() = data.clone();
            tab.sql_editor
                .get_highlighter()
                .borrow_mut()
                .set_highlight_data(combined_highlight.clone());
            tab.sql_editor.refresh_highlighting();
        }
    }

    fn collect_highlight_columns(data: &IntellisenseData) -> Vec<String> {
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

    fn attach_editor_callbacks(
        state: &Rc<RefCell<AppState>>,
        tab_id: QueryTabId,
        schema_sender: std::sync::mpsc::Sender<SchemaUpdate>,
    ) {
        let Some(mut editor) = state
            .borrow()
            .editor_tabs
            .iter()
            .find(|tab| tab.tab_id == tab_id)
            .map(|tab| tab.sql_editor.clone())
        else {
            return;
        };

        let weak_state_for_execute = Rc::downgrade(state);
        editor.set_execute_callback(move |query_result| {
            let Some(state_for_execute) = weak_state_for_execute.upgrade() else {
                return;
            };
            let mut s = state_for_execute.borrow_mut();
            let conn_info = s.connection_info.borrow().clone();
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
            s.status_bar
                .set_label(&format_status(&base_msg, &conn_info));
        });

        let weak_state_for_status = Rc::downgrade(state);
        editor.set_status_callback(move |message| {
            let Some(state_for_status) = weak_state_for_status.upgrade() else {
                return;
            };
            match state_for_status.try_borrow_mut() {
                Ok(mut s) => {
                    let conn_info = s.connection_info.borrow().clone();
                    s.status_bar.set_label(&format_status(message, &conn_info));
                }
                Err(_) => {}
            };
        });

        let weak_state_for_find = Rc::downgrade(state);
        editor.set_find_callback(move || {
            let Some(state_for_find) = weak_state_for_find.upgrade() else {
                return;
            };
            let (mut editor, mut buffer, popups) = {
                let s = state_for_find.borrow();
                (
                    s.sql_editor.get_editor(),
                    s.sql_buffer.clone(),
                    s.popups.clone(),
                )
            };
            FindReplaceDialog::show_find_with_registry(&mut editor, &mut buffer, popups);
        });

        let weak_state_for_replace = Rc::downgrade(state);
        editor.set_replace_callback(move || {
            let Some(state_for_replace) = weak_state_for_replace.upgrade() else {
                return;
            };
            let (mut editor, mut buffer, popups) = {
                let s = state_for_replace.borrow();
                (
                    s.sql_editor.get_editor(),
                    s.sql_buffer.clone(),
                    s.popups.clone(),
                )
            };
            FindReplaceDialog::show_replace_with_registry(&mut editor, &mut buffer, popups);
        });

        let weak_state_for_progress = Rc::downgrade(state);
        let schema_sender_for_progress = schema_sender.clone();
        editor.set_progress_callback(move |progress| {
            let Some(state_for_progress) = weak_state_for_progress.upgrade() else {
                return;
            };
            let mut s = state_for_progress.borrow_mut();
            match progress {
                QueryProgress::BatchStart => {
                    s.result_tab_offset = s.result_tabs.tab_count();
                    s.fetch_row_counts.clear();
                }
                QueryProgress::StatementStart { index } => {
                    let tab_index = s.result_tab_offset + index;
                    s.result_tabs
                        .start_statement(tab_index, &format!("Result {}", tab_index + 1));
                    s.fetch_row_counts.remove(&index);
                    let conn_info = s.connection_info.borrow().clone();
                    s.status_bar
                        .set_label(&format_status("Executing query...", &conn_info));
                }
                QueryProgress::SelectStart { index, columns } => {
                    let tab_index = s.result_tab_offset + index;
                    s.result_tabs.start_streaming(tab_index, &columns);
                    s.fetch_row_counts.insert(index, 0);
                    s.last_fetch_status_update = Instant::now();
                    let conn_info = s.connection_info.borrow().clone();
                    s.status_bar
                        .set_label(&format_status("Fetching rows: 0", &conn_info));
                }
                QueryProgress::Rows { index, rows } => {
                    let tab_index = s.result_tab_offset + index;
                    let rows_len = rows.len();
                    s.result_tabs.append_rows(tab_index, rows);
                    let new_count = {
                        let count = s.fetch_row_counts.entry(index).or_insert(0);
                        *count += rows_len;
                        *count
                    };
                    if s.last_fetch_status_update.elapsed() >= FETCH_STATUS_UPDATE_INTERVAL {
                        let conn_info = s.connection_info.borrow().clone();
                        s.status_bar.set_label(&format_status(
                            &format!("Fetching rows: {}", new_count),
                            &conn_info,
                        ));
                        s.last_fetch_status_update = Instant::now();
                    }
                }
                QueryProgress::ScriptOutput { lines } => {
                    s.result_tabs.append_script_output_lines(&lines);
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
                    let conn_info = s.connection_info.borrow().clone();
                    s.status_bar.set_label(&format_status(status, &conn_info));
                }
                QueryProgress::ConnectionChanged { info } => {
                    if let Some(info) = info {
                        *s.connection_info.borrow_mut() = Some(info.clone());
                        s.status_bar
                            .set_label(&format!("Connected | {}", info.display_string()));
                        s.object_browser.refresh();
                        s.sql_editor.focus();

                        let schema_sender = schema_sender_for_progress.clone();
                        let connection = s.connection.clone();
                        thread::spawn(move || {
                            let conn = {
                                let mut conn_guard = lock_connection_with_activity(
                                    &connection,
                                    "Loading schema metadata",
                                );
                                conn_guard.require_live_connection().ok()
                            };
                            if let Some(conn) = conn {
                                let mut data = IntellisenseData::new();
                                let mut highlight_data = HighlightData::new();
                                if let Ok(tables) = ObjectBrowser::get_tables(conn.as_ref()) {
                                    highlight_data.tables = tables.clone();
                                    data.tables = tables;
                                }
                                if let Ok(views) = ObjectBrowser::get_views(conn.as_ref()) {
                                    highlight_data.views = views.clone();
                                    data.views = views;
                                }
                                data.rebuild_indices();
                                highlight_data.columns = MainWindow::collect_highlight_columns(&data);
                                let _ = schema_sender.send(SchemaUpdate {
                                    data,
                                    highlight_data,
                                });
                                app::awake();
                            }
                        });
                    } else {
                        Self::transition_to_disconnected_state(&mut s, None);
                    }
                }
                QueryProgress::StatementFinished { index, result, .. } => {
                    let tab_index = s.result_tab_offset + index;
                    if !result.success && !result.message.trim().is_empty() {
                        let lines: Vec<String> =
                            result.message.lines().map(|l| l.to_string()).collect();
                        s.result_tabs.append_script_output_lines(&lines);
                        s.result_tabs.select_script_output();
                    }
                    if result.is_select {
                        s.result_tabs.finish_streaming(tab_index);
                    } else {
                        s.result_tabs.display_result(tab_index, &result);
                    }
                    s.fetch_row_counts.remove(&index);
                }
                QueryProgress::BatchFinished => {
                    s.result_tabs.finish_all_streaming();
                    s.result_tabs.align_tab_strip_left();
                    s.fetch_row_counts.clear();
                    let current_status = s.status_bar.label().to_ascii_lowercase();
                    let needs_reset = current_status.contains("executing query")
                        || current_status.contains("fetching rows")
                        || current_status.contains("connection is busy")
                        || current_status.contains("query is already running");
                    if needs_reset {
                        let conn_info = s.connection_info.borrow().clone();
                        s.status_bar.set_label(&format_status("Ready", &conn_info));
                    }
                }
            }
        });

        let weak_state_for_dirty = Rc::downgrade(state);
        let mut buffer_for_dirty = editor.get_buffer();
        buffer_for_dirty.add_modify_callback2(
            move |_buf, _pos, _ins, _del, _restyled, _deleted| {
                let Some(state_for_dirty) = weak_state_for_dirty.upgrade() else {
                    return;
                };
                match state_for_dirty.try_borrow_mut() {
                    Ok(mut s) => s.set_tab_dirty(tab_id, true),
                    Err(_) => {}
                };
            },
        );
    }

    fn attach_file_drop_callback(
        state: &Rc<RefCell<AppState>>,
        tab_id: QueryTabId,
        file_sender: std::sync::mpsc::Sender<FileActionResult>,
    ) {
        let Some(mut editor) = state
            .borrow()
            .editor_tabs
            .iter()
            .find(|tab| tab.tab_id == tab_id)
            .map(|tab| tab.sql_editor.clone())
        else {
            return;
        };
        let weak_state_for_file_drop = Rc::downgrade(state);
        let file_sender_for_drop = file_sender.clone();
        editor.set_file_drop_callback(move |path| {
            if let Some(state_for_drop) = weak_state_for_file_drop.upgrade() {
                let mut s = state_for_drop.borrow_mut();
                if MainWindow::focus_existing_tab_with_same_file_name(&mut s, &path) {
                    return;
                }
                let conn_info = s.connection_info.borrow().clone();
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
        state: &Rc<RefCell<AppState>>,
        schema_sender: &std::sync::mpsc::Sender<SchemaUpdate>,
        conn_sender: &std::sync::mpsc::Sender<ConnectionResult>,
        file_sender: &std::sync::mpsc::Sender<FileActionResult>,
        choice: &str,
    ) -> bool {
        match choice {
            "File/Connect..." => {
                let (popups, connection) = {
                    let s = state.borrow();
                    (s.popups.clone(), s.connection.clone())
                };
                if let Some(info) = ConnectionDialog::show_with_registry(popups) {
                    let conn_sender = conn_sender.clone();
                    {
                        let mut s = state.borrow_mut();
                        s.status_bar
                            .set_label(&format!("Connecting to {}...", info.display_string()));
                    }
                    thread::spawn(move || {
                        let mut db_conn = lock_connection_with_activity(
                            &connection,
                            format!("Connecting to {}", info.name),
                        );
                        match db_conn.connect(info.clone()) {
                            Ok(_) => {
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
                let connection = state.borrow().connection.clone();
                let Some(mut db_conn) =
                    try_lock_connection_with_activity(&connection, "Disconnecting session")
                else {
                    let busy_message = format_connection_busy_message();
                    fltk::dialog::alert_default(&busy_message);
                    let mut s = state.borrow_mut();
                    let conn_info = s.connection_info.borrow().clone();
                    s.status_bar
                        .set_label(&format_status(&busy_message, &conn_info));
                    return true;
                };
                crate::utils::logging::log_info("connection", "Disconnected from database");
                db_conn.disconnect();
                let session = db_conn.session_state();
                drop(db_conn);
                match session.lock() {
                    Ok(mut guard) => guard.reset(),
                    Err(poisoned) => {
                        eprintln!("Warning: session state lock was poisoned; recovering.");
                        poisoned.into_inner().reset();
                    }
                }

                let mut s = state.borrow_mut();
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
                        let mut s = state.borrow_mut();
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
                let tab_id = state.borrow().active_editor_tab_id;
                if let SaveTabOutcome::Failed(err) = MainWindow::save_tab(state, tab_id, false) {
                    fltk::dialog::alert_default(&format!("Failed to save SQL file: {}", err));
                }
                true
            }
            "File/Save SQL File As..." => {
                let tab_id = state.borrow().active_editor_tab_id;
                if let SaveTabOutcome::Failed(err) = MainWindow::save_tab(state, tab_id, true) {
                    fltk::dialog::alert_default(&format!("Failed to save SQL file: {}", err));
                }
                true
            }
            "File/Exit" => {
                let mut window = state.borrow().window.clone();
                window.do_callback();
                true
            }
            "Edit/Undo" => {
                state.borrow().sql_editor.undo();
                true
            }
            "Edit/Redo" => {
                state.borrow().sql_editor.redo();
                true
            }
            "Edit/Cut" => {
                state.borrow_mut().sql_editor.get_editor().cut();
                true
            }
            "Edit/Copy" => {
                let mut s = state.borrow_mut();
                let result_tabs_widget = s.result_tabs.get_widget();
                let focus_in_results = if let Some(focus) = app::focus() {
                    focus.as_widget_ptr() == result_tabs_widget.as_widget_ptr()
                        || focus.inside(&result_tabs_widget)
                } else {
                    false
                };

                if focus_in_results {
                    let cell_count = s.result_tabs.copy();
                    let conn_info = s.connection_info.borrow().clone();
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
                let mut s = state.borrow_mut();
                let result_tabs_widget = s.result_tabs.get_widget();
                let focus_in_results = if let Some(focus) = app::focus() {
                    focus.as_widget_ptr() == result_tabs_widget.as_widget_ptr()
                        || focus.inside(&result_tabs_widget)
                } else {
                    false
                };

                if focus_in_results {
                    s.result_tabs.copy_with_headers();
                    let conn_info = s.connection_info.borrow().clone();
                    s.status_bar
                        .set_label(&format_status("Copied selection with headers", &conn_info));
                } else {
                    s.sql_editor.get_editor().copy();
                }
                true
            }
            "Edit/Paste" => {
                state.borrow_mut().sql_editor.get_editor().paste();
                true
            }
            "Edit/Select All" => {
                let mut s = state.borrow_mut();
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
                state.borrow_mut().sql_editor.execute_current();
                true
            }
            "Query/New Tab" => {
                let created_tab_id = {
                    let mut s = state.borrow_mut();
                    let created = MainWindow::create_query_editor_tab(&mut s);
                    s.right_tile.redraw();
                    created
                };
                if let Some(tab_id) = created_tab_id {
                    MainWindow::attach_editor_callbacks(state, tab_id, schema_sender.clone());
                    MainWindow::attach_file_drop_callback(state, tab_id, file_sender.clone());
                    state.borrow_mut().sql_editor.focus();
                    app::redraw();
                }
                true
            }
            "Query/Close Tab" => {
                let tab_id = state.borrow().active_editor_tab_id;
                MainWindow::close_query_editor_tab(state, tab_id);
                true
            }
            "Query/Execute Statement" => {
                state.borrow_mut().sql_editor.execute_statement_at_cursor();
                true
            }
            "Query/Execute Statement (F9)" => {
                state.borrow_mut().sql_editor.execute_statement_at_cursor();
                true
            }
            "Query/Execute Selected" => {
                state.borrow_mut().sql_editor.execute_selected();
                true
            }
            "Query/Quick Describe" => {
                state.borrow_mut().sql_editor.quick_describe_at_cursor();
                true
            }
            "Query/Explain Plan" => {
                state.borrow_mut().sql_editor.explain_current();
                true
            }
            "Query/Commit" => {
                state.borrow_mut().sql_editor.commit();
                true
            }
            "Query/Rollback" => {
                state.borrow_mut().sql_editor.rollback();
                true
            }
            "Tools/Refresh Objects" => {
                state.borrow_mut().object_browser.refresh();
                true
            }
            "Tools/Export Results..." => {
                let has_data = state.borrow().result_tabs.has_data();
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

                let csv = state.borrow().result_tabs.export_to_csv();
                let row_count = state.borrow().result_tabs.row_count();
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
                    let s = state.borrow_mut();
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
                    let s = state.borrow_mut();
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
                    let s = state.borrow_mut();
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
                state.borrow_mut().sql_editor.format_selected_sql();
                true
            }
            "Edit/Toggle Comment" => {
                state.borrow_mut().sql_editor.toggle_comment();
                true
            }
            "Edit/Uppercase Selection" => {
                state.borrow_mut().sql_editor.convert_selection_case(true);
                true
            }
            "Edit/Lowercase Selection" => {
                state.borrow_mut().sql_editor.convert_selection_case(false);
                true
            }
            "Edit/Intellisense" => {
                state.borrow().sql_editor.show_intellisense();
                true
            }
            "Tools/Query History..." => {
                MainWindow::open_query_history_dialog(state);
                true
            }
            "Tools/Application Log..." => {
                let popups = state.borrow().popups.clone();
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
                    let s = state.borrow();
                    s.connection.clone()
                };
                if let Some(mut connection) =
                    try_lock_connection_with_activity(&connection, "Updating auto-commit setting")
                {
                    connection.set_auto_commit(enabled);
                } else {
                    let busy_message = format_connection_busy_message();
                    fltk::dialog::alert_default(&busy_message);
                    let mut s = state.borrow_mut();
                    let conn_info = s.connection_info.borrow().clone();
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
                let mut s = state.borrow_mut();
                let conn_info = s.connection_info.borrow().clone();
                s.status_bar.set_label(&format_status(status, &conn_info));
                true
            }
            "Settings/Preferences..." => {
                let config_snapshot = {
                    let s = state.borrow();
                    let config_snapshot = s.config.borrow().clone();
                    config_snapshot
                };
                if let Some(settings) = show_settings_dialog(&config_snapshot) {
                    let mut s = state.borrow_mut();
                    let save_result = {
                        let mut config = s.config.borrow_mut();
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

    fn handle_window_shortcut(
        state: &Rc<RefCell<AppState>>,
        schema_sender: &std::sync::mpsc::Sender<SchemaUpdate>,
        conn_sender: &std::sync::mpsc::Sender<ConnectionResult>,
        file_sender: &std::sync::mpsc::Sender<FileActionResult>,
    ) -> bool {
        let event_key = app::event_key();
        let event_state = app::event_state();
        let Some(action) = Self::menu_shortcut_for_key(event_key, event_state) else {
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
            .borrow()
            .editor_tabs
            .iter()
            .map(|tab| tab.tab_id)
            .collect();
        for tab_id in tab_ids {
            Self::attach_editor_callbacks(&state, tab_id, schema_sender.clone());
        }

        let mut state_borrow = state.borrow_mut();

        // Setup object browser callback
        let weak_state_for_browser_status = Rc::downgrade(&state);
        state_borrow
            .object_browser
            .set_status_callback(move |message| {
                let Some(state_for_status) = weak_state_for_browser_status.upgrade() else {
                    return;
                };
                match state_for_status.try_borrow_mut() {
                    Ok(mut s) => {
                        let conn_info = s.connection_info.borrow().clone();
                        s.status_bar.set_label(&format_status(message, &conn_info));
                    }
                    Err(_) => {}
                };
            });

        let weak_state_for_browser = Rc::downgrade(&state);
        let schema_sender_for_browser = schema_sender.clone();
        let file_sender_for_browser = file_sender.clone();
        state_borrow.object_browser.set_sql_callback(move |action| {
            let Some(state_for_browser) = weak_state_for_browser.upgrade() else {
                return;
            };
            let mut created_tab_for_generated_sql: Option<QueryTabId> = None;
            {
                let mut s = state_for_browser.borrow_mut();
                match action {
                    SqlAction::Set(sql) => {
                        s.sql_buffer.set_text(&sql);
                        s.sql_editor.refresh_highlighting();
                    }
                    SqlAction::Insert(text) => {
                        let mut editor = s.sql_editor.get_editor();
                        let insert_pos = editor.insert_position();
                        s.sql_buffer.insert(insert_pos, &text);
                        editor.set_insert_position(insert_pos + text.len() as i32);
                        s.sql_editor.refresh_highlighting();
                    }
                    SqlAction::Append(text) => {
                        let mut editor = s.sql_editor.get_editor();
                        let buffer_length = s.sql_buffer.length();

                        // Add newline prefix if buffer is not empty
                        let text_to_insert = if buffer_length > 0 {
                            format!("\n{}", text)
                        } else {
                            text
                        };

                        // Insert at the end of the buffer
                        s.sql_buffer.insert(buffer_length, &text_to_insert);

                        // Move cursor to the end
                        let new_length = s.sql_buffer.length();
                        editor.set_insert_position(new_length);

                        // Scroll to the bottom to show the inserted text
                        editor.show_insert_position();

                        s.sql_editor.refresh_highlighting();
                    }
                    SqlAction::OpenInNewTab(sql) => {
                        if let Some(tab_id) = MainWindow::create_query_editor_tab(&mut s) {
                            s.sql_buffer.set_text(&sql);
                            s.sql_editor.reset_undo_redo_history();
                            s.set_tab_file_path(tab_id, None);
                            s.set_tab_dirty(tab_id, false);
                            s.sql_editor.refresh_highlighting();
                            s.sql_editor.focus();
                            s.right_tile.redraw();
                            created_tab_for_generated_sql = Some(tab_id);
                        }
                    }
                    SqlAction::Execute(sql) => {
                        s.sql_editor.execute_sql_text(&sql);
                    }
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
                state_for_browser.borrow_mut().sql_editor.focus();
                app::redraw();
            }
        });

        let weak_state_for_window = Rc::downgrade(&state);
        let schema_sender_for_window = schema_sender.clone();
        let conn_sender_for_window = conn_sender.clone();
        let file_sender_for_window = file_sender.clone();
        state_borrow.window.handle(move |_w, ev| {
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
                        let s = state_for_window.borrow();
                        s.sql_editor.clone()
                    };
                    sql_editor
                        .hide_intellisense_if_outside(app::event_x_root(), app::event_y_root());
                    false
                }
                fltk::enums::Event::Resize => {
                    if let Ok(s) = state_for_window.try_borrow() {
                        MainWindow::adjust_query_layout_on_resize(&s);
                    }
                    false
                }
                _ => false,
            }
        });

        drop(state_borrow);
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

        // Wrap receivers in Rc<RefCell> to share across timeout callbacks
        let schema_receiver: Rc<RefCell<std::sync::mpsc::Receiver<SchemaUpdate>>> =
            Rc::new(RefCell::new(schema_receiver));
        let conn_receiver: Rc<RefCell<std::sync::mpsc::Receiver<ConnectionResult>>> =
            Rc::new(RefCell::new(conn_receiver));
        let file_receiver: Rc<RefCell<std::sync::mpsc::Receiver<FileActionResult>>> =
            Rc::new(RefCell::new(file_receiver));
        let (health_sender, health_receiver) = std::sync::mpsc::channel::<ConnectionHealthResult>();
        let health_receiver: Rc<RefCell<std::sync::mpsc::Receiver<ConnectionHealthResult>>> =
            Rc::new(RefCell::new(health_receiver));
        let health_check_in_flight = Rc::new(Cell::new(false));
        let last_health_check = Rc::new(Cell::new(
            Instant::now()
                .checked_sub(CONNECTION_HEALTH_CHECK_INTERVAL)
                .unwrap_or_else(Instant::now),
        ));

        fn schedule_poll(
            schema_receiver: Rc<RefCell<std::sync::mpsc::Receiver<SchemaUpdate>>>,
            conn_receiver: Rc<RefCell<std::sync::mpsc::Receiver<ConnectionResult>>>,
            file_receiver: Rc<RefCell<std::sync::mpsc::Receiver<FileActionResult>>>,
            health_receiver: Rc<RefCell<std::sync::mpsc::Receiver<ConnectionHealthResult>>>,
            health_sender: std::sync::mpsc::Sender<ConnectionHealthResult>,
            health_check_in_flight: Rc<Cell<bool>>,
            last_health_check: Rc<Cell<Instant>>,
            state_weak: Weak<RefCell<AppState>>,
            schema_sender: std::sync::mpsc::Sender<SchemaUpdate>,
            file_sender: std::sync::mpsc::Sender<FileActionResult>,
        ) {
            let Some(state) = state_weak.upgrade() else {
                return;
            };
            let mut schema_disconnected = false;
            let mut conn_disconnected = false;
            let mut file_disconnected = false;

            // Check for schema updates
            {
                let r = schema_receiver.borrow();
                loop {
                    match r.try_recv() {
                        Ok(update) => {
                            let mut s = state.borrow_mut();
                            let mut data = update.data;
                            data.rebuild_indices();
                            MainWindow::apply_schema_to_all_editors(
                                &mut s,
                                &data,
                                &update.highlight_data,
                            );
                        }
                        Err(std::sync::mpsc::TryRecvError::Empty) => break,
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            schema_disconnected = true;
                            break;
                        }
                    }
                }
            }

            // Check for connection results
            {
                let r = conn_receiver.borrow();
                loop {
                    match r.try_recv() {
                        Ok(result) => {
                            let mut s = state.borrow_mut();
                            match result {
                                ConnectionResult::Success(info) => {
                                    crate::utils::logging::log_info(
                                        "connection",
                                        &format!("Connected to {}", info.display_string()),
                                    );
                                    *s.connection_info.borrow_mut() = Some(info.clone());
                                    s.status_bar.set_label(&format!(
                                        "Connected | {}",
                                        info.display_string()
                                    ));
                                    s.object_browser.refresh();
                                    s.sql_editor.focus();

                                    // Start schema update after successful connection
                                    let schema_sender = schema_sender.clone();
                                    let connection = s.connection.clone();
                                    thread::spawn(move || {
                                        let conn = {
                                            let mut conn_guard = lock_connection_with_activity(
                                                &connection,
                                                "Loading schema metadata",
                                            );
                                            conn_guard.require_live_connection().ok()
                                        };
                                        if let Some(conn) = conn {
                                            let mut data = IntellisenseData::new();
                                            let mut highlight_data = HighlightData::new();
                                            if let Ok(tables) =
                                                ObjectBrowser::get_tables(conn.as_ref())
                                            {
                                                highlight_data.tables = tables.clone();
                                                data.tables = tables;
                                            }
                                            if let Ok(views) =
                                                ObjectBrowser::get_views(conn.as_ref())
                                            {
                                                highlight_data.views = views.clone();
                                                data.views = views;
                                            }
                                            data.rebuild_indices();
                                            highlight_data.columns =
                                                MainWindow::collect_highlight_columns(&data);
                                            let _ = schema_sender.send(SchemaUpdate {
                                                data,
                                                highlight_data,
                                            });
                                            app::awake();
                                        }
                                    });
                                }
                                ConnectionResult::Failure(err) => {
                                    crate::utils::logging::log_error(
                                        "connection",
                                        &format!("Connection failed: {}", err),
                                    );
                                    s.status_bar.set_label("Connection failed");
                                    s.result_tabs.append_script_output_lines(&[format!(
                                        "Connection failed: {}",
                                        err
                                    )]);
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
                let r = file_receiver.borrow();
                loop {
                    match r.try_recv() {
                        Ok(result) => {
                            let mut created_tab_for_open: Option<QueryTabId> = None;
                            {
                                let mut s = state.borrow_mut();
                                match result {
                                    FileActionResult::OpenInNewTab { path, result } => match result
                                    {
                                        Ok(content) => {
                                            if MainWindow::focus_existing_tab_with_same_file_name(
                                                &mut s, &path,
                                            ) {
                                                continue;
                                            }
                                            if let Some(tab_id) =
                                                MainWindow::create_query_editor_tab(&mut s)
                                            {
                                                s.sql_buffer.set_text(&content);
                                                s.sql_editor.reset_undo_redo_history();
                                                s.set_tab_file_path(tab_id, Some(path.clone()));
                                                s.set_tab_dirty(tab_id, false);
                                                s.sql_editor.refresh_highlighting();
                                                s.sql_editor.focus();
                                                s.right_tile.redraw();
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
                                            let file_label = path
                                                .file_name()
                                                .unwrap_or_default()
                                                .to_string_lossy();
                                            let conn_info = s.connection_info.borrow().clone();
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
                            }

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
                                state.borrow_mut().sql_editor.focus();
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

            {
                let r = health_receiver.borrow();
                loop {
                    match r.try_recv() {
                        Ok(ConnectionHealthResult::Checked(disconnect_message)) => {
                            health_check_in_flight.set(false);
                            if let Some(message) = disconnect_message {
                                let mut s = state.borrow_mut();
                                if s.connection_info.borrow().is_some() {
                                    MainWindow::transition_to_disconnected_state(
                                        &mut s,
                                        Some(&message),
                                    );
                                }
                            }
                        }
                        Err(std::sync::mpsc::TryRecvError::Empty) => break,
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            health_check_in_flight.set(false);
                            break;
                        }
                    }
                }
            }

            let should_run_health_check = {
                let s = state.borrow();
                s.connection_info.borrow().is_some()
                    && !health_check_in_flight.get()
                    && last_health_check.get().elapsed() >= CONNECTION_HEALTH_CHECK_INTERVAL
            };
            if should_run_health_check {
                health_check_in_flight.set(true);
                last_health_check.set(Instant::now());
                let connection = state.borrow().connection.clone();
                let health_sender = health_sender.clone();
                thread::spawn(move || {
                    let disconnect_message =
                        if let Some(mut conn_guard) = crate::db::try_lock_connection(&connection) {
                            if !conn_guard.is_connected() || conn_guard.get_connection().is_none() {
                                Some(crate::db::NOT_CONNECTED_MESSAGE.to_string())
                            } else {
                                conn_guard.require_live_connection().err()
                            }
                        } else {
                            None
                        };
                    let _ = health_sender.send(ConnectionHealthResult::Checked(disconnect_message));
                    app::awake();
                });
            }

            // Stop polling if all channels are disconnected
            if schema_disconnected && conn_disconnected && file_disconnected {
                return;
            }

            // Reschedule for next poll
            app::add_timeout3(0.05, move |_| {
                schedule_poll(
                    Rc::clone(&schema_receiver),
                    Rc::clone(&conn_receiver),
                    Rc::clone(&file_receiver),
                    Rc::clone(&health_receiver),
                    health_sender.clone(),
                    Rc::clone(&health_check_in_flight),
                    Rc::clone(&last_health_check),
                    state_weak.clone(),
                    schema_sender.clone(),
                    file_sender.clone(),
                );
            });
        }

        // Start polling
        let weak_state_for_poll = Rc::downgrade(&state);
        let schema_sender_for_poll = schema_sender.clone();
        let health_sender_for_poll = health_sender.clone();
        {
            let mut s = state.borrow_mut();
            s.schema_sender = Some(schema_sender.clone());
            s.file_sender = Some(file_sender.clone());
        }
        schedule_poll(
            schema_receiver,
            conn_receiver,
            file_receiver,
            health_receiver,
            health_sender_for_poll,
            health_check_in_flight,
            last_health_check,
            weak_state_for_poll,
            schema_sender_for_poll,
            file_sender.clone(),
        );

        let tab_ids_for_drop: Vec<QueryTabId> = state
            .borrow()
            .editor_tabs
            .iter()
            .map(|tab| tab.tab_id)
            .collect();
        for tab_id in tab_ids_for_drop {
            Self::attach_file_drop_callback(&state, tab_id, file_sender.clone());
        }

        if let Some(mut menu) = app::widget_from_id::<MenuBar>("main_menu") {
            let weak_state_for_menu = Rc::downgrade(&state);
            let schema_sender_for_menu = schema_sender.clone();
            let conn_sender_for_menu = conn_sender.clone();
            let file_sender_for_menu = file_sender.clone();
            menu.set_callback(move |m| {
                let Some(state_for_menu) = weak_state_for_menu.upgrade() else {
                    return;
                };
                let menu_path = m
                    .item_pathname(None)
                    .ok()
                    .or_else(|| m.choice().map(|p| p.to_string()));
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
            let s = state.borrow();
            s.window.clone()
        };
        let weak_state_for_close = Rc::downgrade(&state);
        window.set_callback(move |w| {
            if let Some(state) = weak_state_for_close.upgrade() {
                if !MainWindow::confirm_save_for_all_dirty_tabs(&state) {
                    return;
                }
                let (popups, editor_tabs, mut result_tabs) = {
                    let s = state.borrow();
                    (
                        s.popups.clone(),
                        s.editor_tabs.clone(),
                        s.result_tabs.clone(),
                    )
                };
                let mut popups = popups.borrow_mut();
                for mut popup in popups.drain(..) {
                    popup.hide();
                }
                for tab in editor_tabs {
                    tab.sql_editor.hide_intellisense();
                }
                // Clean up result tabs to release FLTK widget callbacks and data buffers
                result_tabs.clear();
            }
            if let Err(err) = crate::ui::query_history::flush_history_writer() {
                eprintln!("Query history flush on exit failed: {err}");
            }
            if let Err(err) = crate::utils::logging::flush_log_writer() {
                eprintln!("Application log flush on exit failed: {err}");
            }
            w.hide();
            app::quit();
        });
        window.show();
        app::flush();
        let _ = app::wait();
        {
            let mut s = state.borrow_mut();
            MainWindow::adjust_query_layout(&mut s);
            s.window.redraw();
            s.sql_editor.focus();
        }
    }

    pub fn run() {
        let app = app::App::default()
            .with_scheme(app::Scheme::Gtk)
            .load_system_fonts();
        let config = AppConfig::load();
        let ui_size = config.ui_font_size.clamp(8, 24) as i32;
        app::set_font_size(ui_size);
        fltk::misc::Tooltip::set_font_size(ui_size);

        // Set default colors for Windows 11-inspired theme
        let (bg_r, bg_g, bg_b) = theme::app_background().to_rgb();
        app::background(bg_r, bg_g, bg_b);
        let (fg_r, fg_g, fg_b) = theme::app_foreground().to_rgb();
        app::foreground(fg_r, fg_g, fg_b);

        let current_group = fltk::group::Group::try_current();

        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let mut main_window = MainWindow::new();
        main_window.setup_callbacks();
        main_window.show();

        // Check for crash log from a previous session
        if let Some(crash_report) = crate::utils::logging::take_crash_log() {
            crate::utils::logging::log_warning(
                "app",
                "Previous session ended with a crash. Crash report was shown to user.",
            );
            let msg = format!(
                "The previous session ended unexpectedly.\n\n{}\n\nThe crash has been recorded in the application log.",
                crash_report
            );
            fltk::dialog::alert_default(&msg);
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
}

impl Default for MainWindow {
    fn default() -> Self {
        Self::new()
    }
}
