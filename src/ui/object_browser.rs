use fltk::{
    app,
    enums::{Event, Key},
    group::{Flex, FlexType},
    input::Input,
    prelude::*,
    tree::{Tree, TreeItem, TreeSelect},
};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::panic::{self, AssertUnwindSafe};
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Mutex;
use std::thread;

use crate::db::{
    format_connection_busy_message, lock_connection_with_activity,
    try_lock_connection_with_activity, CompilationError, ConstraintInfo, IndexInfo, ObjectBrowser,
    PackageRoutine, ProcedureArgument, SequenceInfo, SharedConnection, SynonymInfo,
    TableColumnDetail,
};
use crate::ui::constants::*;
use crate::ui::font_settings::FontProfile;
use crate::ui::theme;
use crate::ui::{configured_editor_profile, configured_ui_font_size};

#[derive(Clone)]
pub enum SqlAction {
    Insert(String),
    OpenInNewTab(String),
    Execute(String),
}

/// Callback type for executing SQL from object browser
pub type SqlExecuteCallback = Rc<Mutex<Option<Box<dyn FnMut(SqlAction)>>>>;
type StatusCallback = Rc<Mutex<Option<Box<dyn FnMut(&str)>>>>;

#[derive(Clone)]
enum ObjectItem {
    Simple {
        object_type: String,
        object_name: String,
    },
    PackageRoutine {
        package_name: String,
        routine_name: String,
        routine_type: String,
    },
}

/// Stores original object lists for filtering
#[derive(Clone, Default)]
struct ObjectCache {
    tables: Vec<String>,
    views: Vec<String>,
    procedures: Vec<String>,
    functions: Vec<String>,
    sequences: Vec<String>,
    triggers: Vec<String>,
    synonyms: Vec<String>,
    packages: Vec<String>,
    package_routines: HashMap<String, Vec<PackageRoutine>>,
}

#[derive(Clone)]
enum RefreshEvent {
    Cache(ObjectCache),
    Completed,
}

enum ObjectActionResult {
    TableStructure {
        table_name: String,
        result: Result<Vec<TableColumnDetail>, String>,
    },
    TableIndexes {
        table_name: String,
        result: Result<Vec<IndexInfo>, String>,
    },
    TableConstraints {
        table_name: String,
        result: Result<Vec<ConstraintInfo>, String>,
    },
    SequenceInfo(Result<SequenceInfo, String>),
    SynonymInfo(Result<SynonymInfo, String>),
    Ddl(Result<String, String>),
    RoutineScript {
        qualified_name: String,
        routine_type: String,
        result: Result<String, String>,
    },
    PackageRoutines {
        package_name: String,
        result: Result<Vec<PackageRoutine>, String>,
    },
    CompilationErrors {
        object_name: String,
        object_type: String,
        status: String,
        result: Result<Vec<CompilationError>, String>,
    },
    QueryAlreadyRunning,
}

#[derive(Clone)]
pub struct ObjectBrowserWidget {
    flex: Flex,
    tree: Tree,
    connection: SharedConnection,
    sql_callback: SqlExecuteCallback,
    status_callback: StatusCallback,
    filter_input: Input,
    object_cache: Rc<Mutex<ObjectCache>>,
    poll_lifecycle: Rc<()>,
    refresh_sender: std::sync::mpsc::Sender<RefreshEvent>,
    action_sender: std::sync::mpsc::Sender<ObjectActionResult>,
}

impl ObjectBrowserWidget {
    pub fn new(x: i32, y: i32, w: i32, h: i32, connection: SharedConnection) -> Self {
        // Create a flex container for the filter input and tree
        let mut flex = Flex::default().with_pos(x, y).with_size(w, h);
        flex.set_type(FlexType::Column);
        flex.set_spacing(DIALOG_SPACING);

        // Filter input with modern styling
        let mut filter_input = Input::default();
        filter_input.set_color(theme::input_bg());
        filter_input.set_text_color(theme::text_primary());
        filter_input.set_tooltip("Type to filter objects...");
        flex.fixed(&filter_input, FILTER_INPUT_HEIGHT);

        // Tree view with modern styling
        let mut tree = Tree::default();

        tree.set_color(theme::panel_bg());
        tree.set_selection_color(theme::selection_soft());
        tree.set_item_label_fgcolor(theme::text_secondary());
        tree.set_connector_color(theme::tree_connector());
        tree.set_select_mode(TreeSelect::Single);

        // Initialize tree structure
        tree.set_show_root(false);
        tree.add("Tables");
        tree.add("Views");
        tree.add("Procedures");
        tree.add("Functions");
        tree.add("Sequences");
        tree.add("Triggers");
        tree.add("Synonyms");
        tree.add("Packages");

        // Make tree resizable (takes remaining space after filter input)
        flex.resizable(&tree);
        flex.end();

        // Close all items by default
        if let Some(mut item) = tree.find_item("Tables") {
            item.close();
        }
        if let Some(mut item) = tree.find_item("Views") {
            item.close();
        }
        if let Some(mut item) = tree.find_item("Procedures") {
            item.close();
        }
        if let Some(mut item) = tree.find_item("Functions") {
            item.close();
        }
        if let Some(mut item) = tree.find_item("Sequences") {
            item.close();
        }
        if let Some(mut item) = tree.find_item("Triggers") {
            item.close();
        }
        if let Some(mut item) = tree.find_item("Synonyms") {
            item.close();
        }
        if let Some(mut item) = tree.find_item("Packages") {
            item.close();
        }

        let sql_callback: SqlExecuteCallback = Rc::new(Mutex::new(None));
        let status_callback: StatusCallback = Rc::new(Mutex::new(None));
        let object_cache = Rc::new(Mutex::new(ObjectCache::default()));
        let poll_lifecycle = Rc::new(());

        let (refresh_sender, refresh_receiver) = std::sync::mpsc::channel::<RefreshEvent>();
        let (action_sender, action_receiver) = std::sync::mpsc::channel::<ObjectActionResult>();

        let mut widget = Self {
            flex,
            tree,
            connection,
            filter_input,
            object_cache,
            poll_lifecycle,
            sql_callback,
            status_callback,
            refresh_sender,
            action_sender,
        };
        widget.setup_callbacks();
        widget.setup_filter_callback();
        widget.setup_refresh_handler(refresh_receiver);
        widget.setup_action_handler(action_receiver);
        widget
    }

    pub fn get_widget(&self) -> Flex {
        self.flex.clone()
    }

    pub fn apply_font_settings(&mut self, profile: FontProfile, ui_size: i32) {
        self.filter_input.set_text_font(profile.normal);
        self.filter_input.set_text_size(ui_size);
        self.tree.set_item_label_font(profile.normal);
        self.tree.set_item_label_size(ui_size);
        let filter_text = self.filter_input.value().to_lowercase();
        let cache_snapshot = self.object_cache.lock().unwrap().clone();
        Self::rebuild_root_categories(&mut self.tree);
        Self::populate_tree(&mut self.tree, &cache_snapshot, &filter_text);
        // Force layout recalculation so new font metrics take effect immediately.
        let (x, y, w, h) = (self.tree.x(), self.tree.y(), self.tree.w(), self.tree.h());
        self.tree.resize(x, y, w, h);
        self.flex.layout();
        self.filter_input.redraw();
        self.tree.redraw();
    }

    fn setup_filter_callback(&mut self) {
        let mut tree = self.tree.clone();
        let object_cache = self.object_cache.clone();

        self.filter_input.set_callback(move |input| {
            let filter_text = input.value().to_lowercase();
            let cache = object_cache.lock().unwrap();
            ObjectBrowserWidget::populate_tree(&mut tree, &cache, &filter_text);
            tree.redraw();
        });
    }

    fn setup_refresh_handler(&mut self, refresh_receiver: std::sync::mpsc::Receiver<RefreshEvent>) {
        let tree = self.tree.clone();
        let object_cache = self.object_cache.clone();
        let filter_input = self.filter_input.clone();

        let lifecycle = Rc::downgrade(&self.poll_lifecycle);

        // Wrap receiver in Rc<RefCell> to share across timeout callbacks
        let receiver: Rc<Mutex<std::sync::mpsc::Receiver<RefreshEvent>>> =
            Rc::new(Mutex::new(refresh_receiver));

        fn schedule_poll(
            receiver: Rc<Mutex<std::sync::mpsc::Receiver<RefreshEvent>>>,
            mut tree: Tree,
            object_cache: Rc<Mutex<ObjectCache>>,
            filter_input: Input,
            status_callback: StatusCallback,
            lifecycle: Weak<()>,
        ) {
            if lifecycle.upgrade().is_none() {
                return;
            }

            if tree.was_deleted() || filter_input.was_deleted() {
                return;
            }

            let mut disconnected = false;
            // Process any pending messages
            {
                let r = receiver.lock().unwrap();
                loop {
                    match r.try_recv() {
                        Ok(RefreshEvent::Cache(cache)) => {
                            {
                                *object_cache.lock().unwrap() = cache;
                            }
                            let filter_text = filter_input.value().to_lowercase();
                            let cache = object_cache.lock().unwrap();
                            ObjectBrowserWidget::populate_tree(&mut tree, &cache, &filter_text);
                            tree.redraw();
                        }
                        Ok(RefreshEvent::Completed) => {
                            ObjectBrowserWidget::emit_status_callback(
                                &status_callback,
                                "Object browser metadata refresh completed",
                            );
                        }
                        Err(std::sync::mpsc::TryRecvError::Empty) => break,
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            disconnected = true;
                            break;
                        }
                    }
                }
            }

            if disconnected {
                return;
            }

            // Reschedule for next poll
            app::add_timeout3(0.05, move |_| {
                schedule_poll(
                    Rc::clone(&receiver),
                    tree.clone(),
                    Rc::clone(&object_cache),
                    filter_input.clone(),
                    status_callback.clone(),
                    lifecycle.clone(),
                );
            });
        }

        // Start polling
        schedule_poll(
            receiver,
            tree,
            object_cache,
            filter_input,
            self.status_callback.clone(),
            lifecycle,
        );
    }

    fn setup_action_handler(
        &mut self,
        action_receiver: std::sync::mpsc::Receiver<ObjectActionResult>,
    ) {
        let sql_callback = self.sql_callback.clone();
        let status_callback = self.status_callback.clone();
        let tree = self.tree.clone();
        let object_cache = self.object_cache.clone();
        let filter_input = self.filter_input.clone();
        let lifecycle = Rc::downgrade(&self.poll_lifecycle);

        let receiver: Rc<Mutex<std::sync::mpsc::Receiver<ObjectActionResult>>> =
            Rc::new(Mutex::new(action_receiver));

        fn schedule_poll(
            receiver: Rc<Mutex<std::sync::mpsc::Receiver<ObjectActionResult>>>,
            sql_callback: SqlExecuteCallback,
            status_callback: StatusCallback,
            mut tree: Tree,
            object_cache: Rc<Mutex<ObjectCache>>,
            filter_input: Input,
            lifecycle: Weak<()>,
        ) {
            if lifecycle.upgrade().is_none() {
                return;
            }

            if tree.was_deleted() || filter_input.was_deleted() {
                return;
            }

            let mut disconnected = false;
            loop {
                let message = {
                    let r = receiver.lock().unwrap();
                    r.try_recv()
                };

                match message {
                    Ok(action) => match action {
                        ObjectActionResult::TableStructure { table_name, result } => match result {
                            Ok(columns) => {
                                let mut info =
                                    format!("=== Table Structure: {} ===\n\n", table_name);
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

                                ObjectBrowserWidget::show_info_dialog("Table Structure", &info);
                            }
                            Err(err) => {
                                fltk::dialog::alert_default(&format!(
                                    "Failed to get table structure: {}",
                                    err
                                ));
                            }
                        },
                        ObjectActionResult::TableIndexes { table_name, result } => match result {
                            Ok(indexes) => {
                                let mut info = format!("=== Indexes: {} ===\n\n", table_name);
                                info.push_str(&format!(
                                    "{:<30} {:<10} {:<40}\n",
                                    "Index Name", "Unique", "Columns"
                                ));
                                info.push_str(&format!("{}\n", "-".repeat(80)));

                                for idx in indexes {
                                    info.push_str(&format!(
                                        "{:<30} {:<10} {:<40}\n",
                                        idx.name,
                                        if idx.is_unique { "YES" } else { "NO" },
                                        idx.columns
                                    ));
                                }

                                ObjectBrowserWidget::show_info_dialog("Table Indexes", &info);
                            }
                            Err(err) => {
                                fltk::dialog::alert_default(&format!(
                                    "Failed to get indexes: {}",
                                    err
                                ));
                            }
                        },
                        ObjectActionResult::TableConstraints { table_name, result } => match result
                        {
                            Ok(constraints) => {
                                let mut info = format!("=== Constraints: {} ===\n\n", table_name);
                                info.push_str(&format!(
                                    "{:<30} {:<15} {:<25} {:<20}\n",
                                    "Constraint Name", "Type", "Columns", "Ref Table"
                                ));
                                info.push_str(&format!("{}\n", "-".repeat(90)));

                                for con in constraints {
                                    info.push_str(&format!(
                                        "{:<30} {:<15} {:<25} {:<20}\n",
                                        con.name,
                                        con.constraint_type,
                                        con.columns,
                                        con.ref_table.unwrap_or_default()
                                    ));
                                }

                                ObjectBrowserWidget::show_info_dialog("Table Constraints", &info);
                            }
                            Err(err) => {
                                fltk::dialog::alert_default(&format!(
                                    "Failed to get constraints: {}",
                                    err
                                ));
                            }
                        },
                        ObjectActionResult::SequenceInfo(result) => match result {
                            Ok(info) => {
                                let mut details =
                                    format!("=== Sequence Info: {} ===\n\n", info.name);
                                details
                                    .push_str(&format!("{:<18} {}\n", "Min Value", info.min_value));
                                details
                                    .push_str(&format!("{:<18} {}\n", "Max Value", info.max_value));
                                details.push_str(&format!(
                                    "{:<18} {}\n",
                                    "Increment By", info.increment_by
                                ));
                                details.push_str(&format!("{:<18} {}\n", "Cycle", info.cycle_flag));
                                details.push_str(&format!("{:<18} {}\n", "Order", info.order_flag));
                                details.push_str(&format!(
                                    "{:<18} {}\n",
                                    "Cache Size", info.cache_size
                                ));
                                details.push_str(&format!(
                                    "{:<18} {}\n",
                                    "Last Number", info.last_number
                                ));
                                details.push_str(
                                    "\nNote: LAST_NUMBER is the next value to be generated.\n",
                                );

                                ObjectBrowserWidget::show_info_dialog("Sequence Info", &details);
                            }
                            Err(err) => {
                                fltk::dialog::alert_default(&format!(
                                    "Failed to get sequence info: {}",
                                    err
                                ));
                            }
                        },
                        ObjectActionResult::SynonymInfo(result) => match result {
                            Ok(info) => {
                                let mut details =
                                    format!("=== Synonym Info: {} ===\n\n", info.name);
                                details.push_str(&format!(
                                    "{:<18} {}\n",
                                    "Table Owner", info.table_owner
                                ));
                                details.push_str(&format!(
                                    "{:<18} {}\n",
                                    "Table Name", info.table_name
                                ));
                                if !info.db_link.is_empty() {
                                    details
                                        .push_str(&format!("{:<18} {}\n", "DB Link", info.db_link));
                                }

                                ObjectBrowserWidget::show_info_dialog("Synonym Info", &details);
                            }
                            Err(err) => {
                                fltk::dialog::alert_default(&format!(
                                    "Failed to get synonym info: {}",
                                    err
                                ));
                            }
                        },
                        ObjectActionResult::Ddl(result) => match result {
                            Ok(ddl) => {
                                ObjectBrowserWidget::emit_sql_callback(
                                    &sql_callback,
                                    SqlAction::OpenInNewTab(ddl),
                                );
                            }
                            Err(err) => {
                                fltk::dialog::alert_default(&format!(
                                    "Failed to generate DDL: {}",
                                    err
                                ));
                            }
                        },
                        ObjectActionResult::RoutineScript {
                            qualified_name,
                            routine_type,
                            result,
                        } => {
                            let sql = match result {
                                Ok(sql) => sql,
                                Err(err) => {
                                    fltk::dialog::alert_default(&format!(
                                        "Failed to load routine arguments: {}",
                                        err
                                    ));
                                    if routine_type.eq_ignore_ascii_case("FUNCTION") {
                                        ObjectBrowserWidget::build_simple_function_script(
                                            &qualified_name,
                                        )
                                    } else {
                                        ObjectBrowserWidget::build_simple_procedure_script(
                                            &qualified_name,
                                        )
                                    }
                                }
                            };
                            ObjectBrowserWidget::emit_sql_callback(
                                &sql_callback,
                                SqlAction::OpenInNewTab(sql),
                            );
                        }
                        ObjectActionResult::PackageRoutines {
                            package_name,
                            result,
                        } => match result {
                            Ok(routines) => {
                                let mut cache = object_cache.lock().unwrap();
                                cache.package_routines.insert(package_name, routines);
                                let filter_text = filter_input.value().to_lowercase();
                                ObjectBrowserWidget::populate_tree(&mut tree, &cache, &filter_text);
                                tree.redraw();
                            }
                            Err(err) => {
                                fltk::dialog::alert_default(&format!(
                                    "Failed to load package routines: {}",
                                    err
                                ));
                            }
                        },
                        ObjectActionResult::CompilationErrors {
                            object_name,
                            object_type,
                            status,
                            result,
                        } => match result {
                            Ok(errors) => {
                                let mut info = format!(
                                    "=== Compilation Status: {} ({}) ===\n\n",
                                    object_name, object_type
                                );
                                info.push_str(&format!("Status: {}\n\n", status));

                                if errors.is_empty() {
                                    info.push_str(
                                        "No compilation errors found. Object is valid.\n",
                                    );
                                } else {
                                    info.push_str(&format!(
                                        "Found {} error(s)/warning(s):\n\n",
                                        errors.len()
                                    ));
                                    info.push_str(&format!(
                                        "{:<10} {:<10} {:<10} {}\n",
                                        "Line", "Position", "Type", "Message"
                                    ));
                                    info.push_str(&format!("{}\n", "-".repeat(80)));

                                    for err in &errors {
                                        info.push_str(&format!(
                                            "{:<10} {:<10} {:<10} {}\n",
                                            err.line, err.position, err.attribute, err.text
                                        ));
                                    }
                                }

                                ObjectBrowserWidget::show_info_dialog("Compilation Status", &info);
                            }
                            Err(err) => {
                                fltk::dialog::alert_default(&format!(
                                    "Failed to check compilation status: {}",
                                    err
                                ));
                            }
                        },
                        ObjectActionResult::QueryAlreadyRunning => {
                            let busy_message = format_connection_busy_message();
                            ObjectBrowserWidget::emit_status_callback(
                                &status_callback,
                                &busy_message,
                            );
                            fltk::dialog::message_default(&busy_message);
                        }
                    },
                    Err(std::sync::mpsc::TryRecvError::Empty) => break,
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        disconnected = true;
                        break;
                    }
                }
            }

            if disconnected {
                return;
            }

            app::add_timeout3(0.05, move |_| {
                schedule_poll(
                    Rc::clone(&receiver),
                    sql_callback.clone(),
                    status_callback.clone(),
                    tree.clone(),
                    Rc::clone(&object_cache),
                    filter_input.clone(),
                    lifecycle.clone(),
                );
            });
        }

        schedule_poll(
            receiver,
            sql_callback,
            status_callback,
            tree,
            object_cache,
            filter_input,
            lifecycle,
        );
    }

    fn setup_callbacks(&mut self) {
        let connection = self.connection.clone();
        let sql_callback = self.sql_callback.clone();
        let status_callback = self.status_callback.clone();
        let action_sender = self.action_sender.clone();
        let object_cache = self.object_cache.clone();

        self.tree.handle(move |t, ev| {
            if !t.active() {
                return false;
            }
            match ev {
                Event::Push => {
                    let mouse_button = fltk::app::event_mouse_button();
                    if mouse_button == fltk::app::MouseButton::Right {
                        let clicked_item = t
                            .find_clicked(false)
                            .or_else(|| t.find_clicked(true))
                            .or_else(|| Self::item_at_mouse(t));

                        if let Some(item) = clicked_item {
                            let _ = t.select_only(&item, false);
                            t.set_item_focus(&item);
                            Self::show_context_menu(
                                &connection,
                                &item,
                                &sql_callback,
                                &status_callback,
                                &action_sender,
                            );
                        } else if let Some(item) = t.first_selected_item() {
                            Self::show_context_menu(
                                &connection,
                                &item,
                                &sql_callback,
                                &status_callback,
                                &action_sender,
                            );
                        }
                        return true;
                    }

                    if mouse_button == fltk::app::MouseButton::Left {
                        if fltk::app::event_clicks() {
                            let clicked_item = t
                                .find_clicked(false)
                                .or_else(|| t.find_clicked(true))
                                .or_else(|| Self::item_at_mouse(t));

                            if let (Some(item), Some(selected_item)) =
                                (clicked_item, t.first_selected_item())
                            {
                                if item != selected_item {
                                    return false;
                                }

                                // Double-click on a package node: load sub-items
                                if let Some(ObjectItem::Simple {
                                    object_type,
                                    object_name,
                                }) = Self::get_item_info(&item)
                                {
                                    if object_type == "PACKAGES" {
                                        let package_name = object_name.clone();
                                        let should_fetch = {
                                            let cache = object_cache.lock().unwrap();
                                            !cache.package_routines.contains_key(&package_name)
                                        };
                                        if should_fetch {
                                            let connection = connection.clone();
                                            let sender = action_sender.clone();
                                            Self::emit_status_callback(
                                                &status_callback,
                                                &format!(
                                                    "Loading package members for {}",
                                                    package_name
                                                ),
                                            );
                                            thread::spawn(move || {
                                                // Try to acquire connection lock without blocking
                                                let Some(mut conn_guard) =
                                                    try_lock_connection_with_activity(
                                                        &connection,
                                                        format!(
                                                            "Loading package members for {}",
                                                            package_name
                                                        ),
                                                    )
                                                else {
                                                    // Query is already running, notify user
                                                    let _ = sender.send(
                                                        ObjectActionResult::QueryAlreadyRunning,
                                                    );
                                                    app::awake();
                                                    return;
                                                };

                                                let result =
                                                    match conn_guard.require_live_connection() {
                                                        Ok(db_conn) => {
                                                            ObjectBrowser::get_package_routines(
                                                                db_conn.as_ref(),
                                                                &package_name,
                                                            )
                                                            .map_err(|err| err.to_string())
                                                        }
                                                        Err(message) => Err(message.to_string()),
                                                    };

                                                let _ = sender.send(
                                                    ObjectActionResult::PackageRoutines {
                                                        package_name,
                                                        result,
                                                    },
                                                );
                                                app::awake();
                                                // conn_guard drops here, releasing the lock
                                            });
                                        }
                                        return true;
                                    }
                                }

                                // Double-click on other items: insert text into SQL editor
                                if let Some(insert_text) = Self::get_insert_text(&item) {
                                    ObjectBrowserWidget::emit_sql_callback(
                                        &sql_callback,
                                        SqlAction::Insert(insert_text),
                                    );
                                    return true;
                                }
                            }
                        }
                    }

                    false
                }
                Event::KeyUp => {
                    // Enter/KPEnter key to generate SELECT - only if tree has focus
                    if matches!(fltk::app::event_key(), Key::Enter | Key::KPEnter) && t.has_focus()
                    {
                        if let Some(item) = t.first_selected_item() {
                            if let Some(ObjectItem::Simple {
                                object_type,
                                object_name,
                            }) = Self::get_item_info(&item)
                            {
                                if object_type == "TABLES" || object_type == "VIEWS" {
                                    let sql = format!(
                                        "SELECT * FROM {} WHERE ROWNUM <= 100",
                                        object_name
                                    );
                                    ObjectBrowserWidget::emit_sql_callback(
                                        &sql_callback,
                                        SqlAction::OpenInNewTab(sql),
                                    );
                                }
                            }
                        }
                        return true;
                    }
                    false
                }

                _ => false,
            }
        });
    }

    fn item_at_mouse(tree: &Tree) -> Option<TreeItem> {
        let mouse_y = fltk::app::event_y();
        let mut current = tree.first_visible_item();
        while let Some(item) = current {
            let item_y = item.y();
            let item_h = item.h();
            if mouse_y >= item_y && mouse_y < item_y + item_h {
                return Some(item);
            }
            current = tree.next_visible_item(&item, Key::Down);
        }
        None
    }

    fn get_item_info(item: &TreeItem) -> Option<ObjectItem> {
        let object_name = match item.label() {
            Some(label) => label.trim().to_string(),
            None => return None,
        };
        let parent = match item.parent() {
            Some(parent) => parent,
            None => return None,
        };
        let parent_label = match parent.label() {
            Some(label) => label.trim().to_string(),
            None => return None,
        };
        let parent_type_upper = parent_label.to_uppercase();

        // Package member item: Packages/<pkg>/(Procedures|Functions)/<name>
        if parent_type_upper == "PROCEDURES" || parent_type_upper == "FUNCTIONS" {
            if let Some(grandparent) = parent.parent() {
                if let Some(package_label) = grandparent.label() {
                    if let Some(root) = grandparent.parent() {
                        if let Some(root_label) = root.label() {
                            if root_label.trim().eq_ignore_ascii_case("Packages") {
                                let routine_type = if parent_type_upper == "FUNCTIONS" {
                                    "FUNCTION"
                                } else {
                                    "PROCEDURE"
                                };
                                return Some(ObjectItem::PackageRoutine {
                                    package_name: package_label.trim().to_string(),
                                    routine_name: object_name,
                                    routine_type: routine_type.to_string(),
                                });
                            }
                        }
                    }
                }
            }
        }

        match parent_type_upper.as_str() {
            "TABLES" | "VIEWS" | "PROCEDURES" | "FUNCTIONS" | "SEQUENCES" | "TRIGGERS"
            | "SYNONYMS" | "PACKAGES" => Some(ObjectItem::Simple {
                object_type: parent_type_upper,
                object_name,
            }),
            _ => None,
        }
    }

    fn get_insert_text(item: &TreeItem) -> Option<String> {
        match Self::get_item_info(item) {
            Some(ObjectItem::Simple { object_name, .. }) => Some(object_name),
            Some(ObjectItem::PackageRoutine {
                package_name,
                routine_name,
                ..
            }) => Some(format!("{}.{}", package_name, routine_name)),
            None => None,
        }
    }

    fn build_simple_procedure_script(qualified_name: &str) -> String {
        format!("BEGIN\n  {};\nEND;\n/\n", qualified_name)
    }

    fn build_simple_function_script(qualified_name: &str) -> String {
        format!(
            "SELECT {} AS result\nFROM dual;\n",
            if qualified_name.contains('(') {
                qualified_name.to_string()
            } else {
                format!("{}()", qualified_name)
            }
        )
    }

    fn build_procedure_script(qualified_name: &str, arguments: &[ProcedureArgument]) -> String {
        if arguments.is_empty() {
            return Self::build_simple_procedure_script(qualified_name);
        }

        let selected_args = Self::select_overload_arguments(arguments);
        if selected_args.is_empty() {
            return Self::build_simple_procedure_script(qualified_name);
        }

        let mut used_names: HashSet<String> = HashSet::new();
        let mut local_decls: Vec<String> = Vec::new();
        let mut call_args: Vec<String> = Vec::new();
        let mut bind_decls: Vec<(String, String)> = Vec::new();
        let mut print_binds: Vec<String> = Vec::new();
        // Function return value (position=0, name=NULL) must be assigned
        // via ':=' rather than passed as a call argument.
        let mut return_var: Option<String> = None;

        for arg in &selected_args {
            let arg_label = arg.name.clone();
            let direction = arg
                .in_out
                .clone()
                .unwrap_or_else(|| "IN".to_string())
                .replace('/', " ")
                .to_uppercase();
            let is_out = direction.contains("OUT");
            let is_in = direction.contains("IN");

            // Detect function return value: position=0 with no argument name
            // and direction is OUT (not IN OUT).
            let is_return_value = arg.position == 0 && arg.name.is_none() && is_out && !is_in;

            let var_base =
                arg_label
                    .as_deref()
                    .unwrap_or(if is_return_value { "RESULT" } else { "ARG" });
            let var_name = Self::unique_var_name(var_base, arg.position, &mut used_names);

            if is_return_value {
                let type_str = Self::format_argument_type(arg);
                if let Some(bind_type) = Self::bind_type_for_return(&type_str) {
                    // Use bind variable for return value so PRINT can show it in results.
                    bind_decls.push((var_name.clone(), bind_type));
                    print_binds.push(var_name.clone());
                    return_var = Some(format!(":{}", var_name));
                } else {
                    // Fallback for unsupported return types: keep local variable assignment.
                    local_decls.push(format!("  {} {};", var_name, type_str));
                    return_var = Some(var_name);
                }
            } else if is_out && Self::is_ref_cursor(arg) {
                bind_decls.push((var_name.clone(), "REFCURSOR".to_string()));
                print_binds.push(var_name.clone());
                let target = format!(":{}", var_name);
                let call_expr = match &arg_label {
                    Some(label) => format!("{} => {}", label, target),
                    None => target,
                };
                call_args.push(call_expr);
            } else {
                let type_str = Self::format_argument_type(arg);
                if is_in {
                    let default_expr = Self::default_value_for_argument(arg, &type_str);
                    local_decls.push(format!("  {} {} := {};", var_name, type_str, default_expr));
                } else {
                    local_decls.push(format!("  {} {};", var_name, type_str));
                }
                let call_expr = match &arg_label {
                    Some(label) => format!("{} => {}", label, var_name),
                    None => var_name,
                };
                call_args.push(call_expr);
            }
        }

        let mut script = String::new();
        for (name, bind_type) in &bind_decls {
            script.push_str(&format!("VAR {} {}\n", name, bind_type));
        }

        if !local_decls.is_empty() {
            script.push_str("DECLARE\n");
            for decl in &local_decls {
                script.push_str(decl);
                script.push('\n');
            }
        }

        script.push_str("BEGIN\n");

        // Build the call expression (with or without arguments)
        let call_str = if call_args.is_empty() {
            format!("{}", qualified_name)
        } else {
            let mut s = format!("{}(\n", qualified_name);
            for (idx, arg) in call_args.iter().enumerate() {
                let suffix = if idx + 1 == call_args.len() { "" } else { "," };
                s.push_str(&format!("    {}{}\n", arg, suffix));
            }
            s.push_str("  )");
            s
        };

        if let Some(ref ret_var) = return_var {
            // Function: assign return value via ':='
            script.push_str(&format!("  {} := {};\n", ret_var, call_str));
        } else {
            // Procedure: plain call
            script.push_str(&format!("  {};\n", call_str));
        }

        script.push_str("END;\n/\n");

        for bind_name in print_binds {
            script.push_str(&format!("PRINT {}\n", bind_name));
        }

        script
    }

    fn bind_type_for_return(type_str: &str) -> Option<String> {
        let upper = type_str.trim().to_uppercase();
        if upper.is_empty() {
            return None;
        }
        let base = Self::normalize_type_base(&upper);
        if base.contains('.') {
            return None;
        }

        match base.as_str() {
            "NUMBER" | "NUMERIC" | "DECIMAL" | "INTEGER" | "INT" | "PLS_INTEGER"
            | "BINARY_INTEGER" | "NATURAL" | "NATURALN" | "POSITIVE" | "POSITIVEN"
            | "SIMPLE_INTEGER" | "FLOAT" | "BINARY_FLOAT" | "BINARY_DOUBLE" => {
                Some("NUMBER".to_string())
            }
            "DATE" => Some("DATE".to_string()),
            "TIMESTAMP" => {
                let precision = Self::extract_parenthesized_u32(&upper)
                    .unwrap_or(6)
                    .clamp(0, 9);
                Some(format!("TIMESTAMP({})", precision))
            }
            "CLOB" | "NCLOB" => Some("CLOB".to_string()),
            "VARCHAR2" | "NVARCHAR2" | "VARCHAR" | "CHAR" | "NCHAR" | "RAW" => {
                let size = Self::extract_parenthesized_u32(&upper)
                    .unwrap_or(4000)
                    .clamp(1, 4000);
                Some(format!("VARCHAR2({})", size))
            }
            _ => None,
        }
    }

    fn extract_parenthesized_u32(value: &str) -> Option<u32> {
        let start = value.find('(')?;
        let end = value[start + 1..].find(')')? + start + 1;
        let inner = value[start + 1..end].trim();
        let head = inner.split(',').next().unwrap_or(inner).trim();
        head.parse::<u32>().ok()
    }

    fn select_overload_arguments(arguments: &[ProcedureArgument]) -> Vec<ProcedureArgument> {
        let mut selected: Vec<ProcedureArgument> = Vec::new();
        let mut selected_overload: Option<i32> = None;
        for arg in arguments {
            if selected_overload.is_none() {
                selected_overload = arg.overload;
            }
            if arg.overload == selected_overload {
                selected.push(arg.clone());
            } else {
                break;
            }
        }
        selected
    }

    fn is_ref_cursor(arg: &ProcedureArgument) -> bool {
        let data_type = arg.data_type.as_deref().unwrap_or("").to_uppercase();
        if data_type.contains("REF CURSOR") || data_type.contains("REFCURSOR") {
            return true;
        }
        if data_type == "SYS_REFCURSOR" {
            return true;
        }
        if let Some(pls_type) = arg.pls_type.as_deref() {
            let upper = pls_type.to_uppercase();
            if upper.contains("REF CURSOR") || upper.contains("REFCURSOR") {
                return true;
            }
        }
        if let Some(type_name) = arg.type_name.as_deref() {
            if type_name.eq_ignore_ascii_case("REFCURSOR") {
                return true;
            }
        }
        false
    }

    fn format_argument_type(arg: &ProcedureArgument) -> String {
        if let Some(pls_type) = arg.pls_type.as_deref() {
            let trimmed = pls_type.trim();
            if !trimmed.is_empty() {
                if trimmed.contains('%') {
                    return trimmed.to_string();
                }
                let upper = trimmed.to_uppercase();
                if Self::is_string_type_without_length(&upper) {
                    let len = Self::clamp_string_length(arg.data_length);
                    return format!("{}({})", upper, len);
                }
                return trimmed.to_string();
            }
        }
        if let Some(data_type) = arg.data_type.as_deref() {
            let upper = data_type.to_uppercase();
            if upper.contains("REF CURSOR") || upper.contains("REFCURSOR") {
                return "SYS_REFCURSOR".to_string();
            }
            if upper.starts_with("NUMBER") {
                if let Some(precision) = arg.data_precision {
                    if let Some(scale) = arg.data_scale {
                        return format!("NUMBER({}, {})", precision, scale);
                    }
                    return format!("NUMBER({})", precision);
                }
                return "NUMBER".to_string();
            }
            if upper.starts_with("VARCHAR2")
                || upper.starts_with("NVARCHAR2")
                || upper.starts_with("CHAR")
                || upper.starts_with("NCHAR")
                || upper.starts_with("RAW")
            {
                let len = Self::clamp_string_length(arg.data_length);
                return format!("{}({})", upper, len);
            }
            return upper;
        }

        if let Some(type_name) = arg.type_name.as_deref() {
            if let Some(owner) = arg.type_owner.as_deref() {
                return format!("{}.{}", owner, type_name);
            }
            return type_name.to_string();
        }

        "VARCHAR2(4000)".to_string()
    }

    fn is_string_type_without_length(upper: &str) -> bool {
        if upper.contains('(') {
            return false;
        }
        matches!(
            upper,
            "VARCHAR2" | "NVARCHAR2" | "VARCHAR" | "CHAR" | "NCHAR" | "RAW"
        )
    }

    fn clamp_string_length(length: Option<i32>) -> i32 {
        let fallback = 32767;
        let len = length.unwrap_or(fallback);
        let len = if len <= 0 { fallback } else { len };
        len.clamp(1, 32767)
    }

    fn default_value_for_argument(arg: &ProcedureArgument, type_str: &str) -> String {
        if let Some(default_value) = arg.default_value.as_deref() {
            let trimmed = default_value.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
        if Self::is_ref_cursor(arg) {
            return "NULL".to_string();
        }

        let base = Self::normalize_type_base(type_str);
        if base.contains('.') {
            return "NULL".to_string();
        }

        match base.as_str() {
            "NUMBER" | "NUMERIC" | "DECIMAL" | "INTEGER" | "INT" | "PLS_INTEGER"
            | "BINARY_INTEGER" | "NATURAL" | "NATURALN" | "POSITIVE" | "POSITIVEN"
            | "SIMPLE_INTEGER" => "0".to_string(),
            "FLOAT" | "BINARY_FLOAT" | "BINARY_DOUBLE" => "0".to_string(),
            "VARCHAR2" | "NVARCHAR2" | "VARCHAR" | "CHAR" | "NCHAR" => "''".to_string(),
            "CLOB" | "NCLOB" => "EMPTY_CLOB()".to_string(),
            "BLOB" => "EMPTY_BLOB()".to_string(),
            "RAW" => "HEXTORAW('')".to_string(),
            "DATE" => "SYSDATE".to_string(),
            "TIMESTAMP" => "SYSTIMESTAMP".to_string(),
            "BOOLEAN" => "FALSE".to_string(),
            _ => "NULL".to_string(),
        }
    }

    fn normalize_type_base(type_str: &str) -> String {
        let mut upper = type_str.trim().to_uppercase();
        if let Some(idx) = upper.find('(') {
            upper.truncate(idx);
        }
        if let Some(idx) = upper.find(' ') {
            upper.truncate(idx);
        }
        upper
    }

    fn unique_var_name(base_name: &str, position: i32, used: &mut HashSet<String>) -> String {
        let mut cleaned = base_name
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() {
                    ch.to_ascii_lowercase()
                } else {
                    '_'
                }
            })
            .collect::<String>();
        if cleaned.is_empty() {
            cleaned = format!("arg{}", position.max(1));
        }
        if cleaned
            .chars()
            .next()
            .map(|ch| ch.is_ascii_digit())
            .unwrap_or(false)
        {
            cleaned.insert(0, '_');
        }
        let candidate = format!("v_{}", cleaned);
        if used.insert(candidate.clone()) {
            return candidate;
        }

        let mut suffix = 2;
        loop {
            let next = format!("{}_{}", candidate, suffix);
            if used.insert(next.clone()) {
                return next;
            }
            suffix += 1;
        }
    }

    fn show_context_menu(
        connection: &SharedConnection,
        item: &TreeItem,
        sql_callback: &SqlExecuteCallback,
        status_callback: &StatusCallback,
        action_sender: &std::sync::mpsc::Sender<ObjectActionResult>,
    ) {
        if let Some(item_info) = Self::get_item_info(item) {
            let menu_choices = match &item_info {
                ObjectItem::Simple { object_type, .. } if object_type == "TABLES" => {
                    "Select Data (Top 100)|View Structure|View Indexes|View Constraints|Generate DDL"
                }
                ObjectItem::Simple { object_type, .. } if object_type == "VIEWS" => {
                    "Select Data (Top 100)|Generate DDL"
                }
                ObjectItem::Simple { object_type, .. }
                    if object_type == "PROCEDURES" || object_type == "FUNCTIONS" =>
                {
                    if object_type == "PROCEDURES" {
                        "Execute Procedure|Check Compilation|Generate DDL"
                    } else {
                        "Execute Function|Check Compilation|Generate DDL"
                    }
                }
                ObjectItem::Simple { object_type, .. } if object_type == "SEQUENCES" => {
                    "View Info|Generate DDL"
                }
                ObjectItem::Simple { object_type, .. } if object_type == "TRIGGERS" => {
                    "Check Compilation|Generate DDL"
                }
                ObjectItem::Simple { object_type, .. } if object_type == "SYNONYMS" => {
                    "View Info|Generate DDL"
                }
                ObjectItem::PackageRoutine { routine_type, .. } => {
                    if routine_type == "FUNCTION" {
                        "Execute Function"
                    } else {
                        "Execute Procedure"
                    }
                }
                ObjectItem::Simple { object_type, .. } if object_type == "PACKAGES" => {
                    "Check Compilation|Generate DDL"
                }
                _ => return,
            };

            // Get mouse position for proper popup placement
            let mouse_x = fltk::app::event_x();
            let mouse_y = fltk::app::event_y();

            // Prevent menu from being added to parent container
            let current_group = fltk::group::Group::try_current();
            fltk::group::Group::set_current(None::<&fltk::group::Group>);

            let mut menu = fltk::menu::MenuButton::new(mouse_x, mouse_y, 0, 0, None);
            menu.set_color(theme::panel_raised());
            menu.set_text_color(theme::text_primary());
            menu.add_choice(menu_choices);

            if let Some(ref group) = current_group {
                fltk::group::Group::set_current(Some(group));
            }

            if let Some(choice_item) = menu.popup() {
                let choice_label = choice_item.label().unwrap_or_default();

                let handle_choice = || {
                    match (choice_label.as_str(), &item_info) {
                        ("Select Data (Top 100)", ObjectItem::Simple { object_name, .. }) => {
                            Self::emit_status_callback(
                                status_callback,
                                &format!("Preparing SELECT TOP 100 for {}", object_name),
                            );
                            let Some(conn_guard) = try_lock_connection_with_activity(
                                connection,
                                format!("Preparing SELECT TOP 100 for {}", object_name),
                            ) else {
                                let _ = action_sender.send(ObjectActionResult::QueryAlreadyRunning);
                                app::awake();
                                return;
                            };
                            if !conn_guard.is_connected() || conn_guard.get_connection().is_none() {
                                drop(conn_guard);
                                fltk::dialog::alert_default("Not connected to database");
                                return;
                            }
                            drop(conn_guard);
                            let sql = format!("SELECT * FROM {} WHERE ROWNUM <= 100", object_name);
                            ObjectBrowserWidget::emit_sql_callback(
                                &sql_callback,
                                SqlAction::Execute(sql),
                            );
                        }
                        (
                            label @ ("Execute Procedure" | "Execute Function"),
                            ObjectItem::Simple {
                                object_name,
                                object_type,
                            },
                        ) if (label == "Execute Procedure" && object_type == "PROCEDURES")
                            || (label == "Execute Function" && object_type == "FUNCTIONS") =>
                        {
                            let connection = connection.clone();
                            let sender = action_sender.clone();
                            let object_name = object_name.clone();
                            let routine_type = if label == "Execute Function" {
                                "FUNCTION".to_string()
                            } else {
                                "PROCEDURE".to_string()
                            };
                            Self::emit_status_callback(
                                status_callback,
                                &format!("Loading {} arguments for {}", routine_type, object_name),
                            );
                            thread::spawn(move || {
                                // Try to acquire connection lock without blocking
                                let Some(mut conn_guard) = try_lock_connection_with_activity(
                                    &connection,
                                    format!(
                                        "Loading {} arguments for {}",
                                        routine_type, object_name
                                    ),
                                ) else {
                                    // Query is already running, notify user
                                    let _ = sender.send(ObjectActionResult::QueryAlreadyRunning);
                                    app::awake();
                                    return;
                                };

                                let result = match conn_guard.require_live_connection() {
                                    Ok(db_conn) => ObjectBrowser::get_procedure_arguments(
                                        db_conn.as_ref(),
                                        &object_name,
                                    )
                                    .map(|arguments| {
                                        ObjectBrowserWidget::build_procedure_script(
                                            &object_name,
                                            &arguments,
                                        )
                                    })
                                    .map_err(|err| err.to_string()),
                                    Err(message) => Err(message.to_string()),
                                };

                                let _ = sender.send(ObjectActionResult::RoutineScript {
                                    qualified_name: object_name,
                                    routine_type,
                                    result,
                                });
                                app::awake();
                                // conn_guard drops here, releasing the lock
                            });
                        }
                        (
                            label @ ("Execute Procedure" | "Execute Function"),
                            ObjectItem::PackageRoutine {
                                package_name,
                                routine_name,
                                routine_type,
                            },
                        ) if (label == "Execute Procedure" && routine_type == "PROCEDURE")
                            || (label == "Execute Function" && routine_type == "FUNCTION") =>
                        {
                            let connection = connection.clone();
                            let sender = action_sender.clone();
                            let qualified_name = format!("{}.{}", package_name, routine_name);
                            let package_name = package_name.clone();
                            let routine_name = routine_name.clone();
                            let routine_type = routine_type.clone();
                            Self::emit_status_callback(
                                status_callback,
                                &format!(
                                    "Loading {} arguments for {}",
                                    routine_type, qualified_name
                                ),
                            );
                            thread::spawn(move || {
                                // Try to acquire connection lock without blocking
                                let Some(mut conn_guard) = try_lock_connection_with_activity(
                                    &connection,
                                    format!(
                                        "Loading {} arguments for {}",
                                        routine_type, qualified_name
                                    ),
                                ) else {
                                    // Query is already running, notify user
                                    let _ = sender.send(ObjectActionResult::QueryAlreadyRunning);
                                    app::awake();
                                    return;
                                };

                                let result = match conn_guard.require_live_connection() {
                                    Ok(db_conn) => ObjectBrowser::get_package_procedure_arguments(
                                        db_conn.as_ref(),
                                        &package_name,
                                        &routine_name,
                                    )
                                    .map(|arguments| {
                                        ObjectBrowserWidget::build_procedure_script(
                                            &qualified_name,
                                            &arguments,
                                        )
                                    })
                                    .map_err(|err| err.to_string()),
                                    Err(message) => Err(message.to_string()),
                                };

                                let _ = sender.send(ObjectActionResult::RoutineScript {
                                    qualified_name,
                                    routine_type,
                                    result,
                                });
                                app::awake();
                                // conn_guard drops here, releasing the lock
                            });
                        }
                        (
                            "Check Compilation",
                            ObjectItem::Simple {
                                object_type,
                                object_name,
                            },
                        ) => {
                            let db_object_type = match object_type.as_str() {
                                "PROCEDURES" => "PROCEDURE",
                                "FUNCTIONS" => "FUNCTION",
                                "PACKAGES" => "PACKAGE",
                                "TRIGGERS" => "TRIGGER",
                                _ => return,
                            };
                            let connection = connection.clone();
                            let sender = action_sender.clone();
                            let object_name = object_name.clone();
                            let object_type = db_object_type.to_string();
                            Self::emit_status_callback(
                                status_callback,
                                &format!("Checking compilation status for {}", object_name),
                            );
                            thread::spawn(move || {
                                // Try to acquire connection lock without blocking
                                let Some(mut conn_guard) = try_lock_connection_with_activity(
                                    &connection,
                                    format!("Checking compilation status for {}", object_name),
                                ) else {
                                    // Query is already running, notify user
                                    let _ = sender.send(ObjectActionResult::QueryAlreadyRunning);
                                    app::awake();
                                    return;
                                };

                                if let Ok(db_conn) = conn_guard.require_live_connection() {
                                    let status = ObjectBrowser::get_object_status(
                                        db_conn.as_ref(),
                                        &object_name,
                                        &object_type,
                                    )
                                    .unwrap_or_else(|_| "UNKNOWN".to_string());

                                    // Also check PACKAGE BODY status for packages
                                    let body_status = if object_type == "PACKAGE" {
                                        ObjectBrowser::get_object_status(
                                            db_conn.as_ref(),
                                            &object_name,
                                            "PACKAGE BODY",
                                        )
                                        .ok()
                                    } else {
                                        None
                                    };

                                    let mut errors = ObjectBrowser::get_compilation_errors(
                                        db_conn.as_ref(),
                                        &object_name,
                                        &object_type,
                                    )
                                    .unwrap_or_default();

                                    // For packages, also get PACKAGE BODY errors
                                    if object_type == "PACKAGE" {
                                        if let Ok(body_errors) =
                                            ObjectBrowser::get_compilation_errors(
                                                db_conn.as_ref(),
                                                &object_name,
                                                "PACKAGE BODY",
                                            )
                                        {
                                            errors.extend(body_errors);
                                        }
                                    }

                                    let combined_status = if let Some(bs) = body_status {
                                        format!("Spec: {} / Body: {}", status, bs)
                                    } else {
                                        status
                                    };

                                    let _ = sender.send(ObjectActionResult::CompilationErrors {
                                        object_name,
                                        object_type,
                                        status: combined_status,
                                        result: Ok(errors),
                                    });
                                    app::awake();
                                } else {
                                    let _ = sender.send(ObjectActionResult::CompilationErrors {
                                        object_name,
                                        object_type,
                                        status: String::new(),
                                        result: Err(crate::db::NOT_CONNECTED_MESSAGE.to_string()),
                                    });
                                    app::awake();
                                }

                                // conn_guard drops here, releasing the lock
                            });
                        }
                        ("View Structure", ObjectItem::Simple { object_name, .. }) => {
                            let connection = connection.clone();
                            let sender = action_sender.clone();
                            let table_name = object_name.clone();
                            Self::emit_status_callback(
                                status_callback,
                                &format!("Loading table structure for {}", table_name),
                            );
                            thread::spawn(move || {
                                // Try to acquire connection lock without blocking
                                let Some(mut conn_guard) = try_lock_connection_with_activity(
                                    &connection,
                                    format!("Loading table structure for {}", table_name),
                                ) else {
                                    // Query is already running, notify user
                                    let _ = sender.send(ObjectActionResult::QueryAlreadyRunning);
                                    app::awake();
                                    return;
                                };

                                let result = match conn_guard.require_live_connection() {
                                    Ok(db_conn) => ObjectBrowser::get_table_structure(
                                        db_conn.as_ref(),
                                        &table_name,
                                    )
                                    .map_err(|err| err.to_string()),
                                    Err(message) => Err(message.to_string()),
                                };
                                let _ = sender.send(ObjectActionResult::TableStructure {
                                    table_name,
                                    result,
                                });
                                app::awake();
                                // conn_guard drops here, releasing the lock
                            });
                        }
                        ("View Indexes", ObjectItem::Simple { object_name, .. }) => {
                            let connection = connection.clone();
                            let sender = action_sender.clone();
                            let table_name = object_name.clone();
                            Self::emit_status_callback(
                                status_callback,
                                &format!("Loading indexes for {}", table_name),
                            );
                            thread::spawn(move || {
                                // Try to acquire connection lock without blocking
                                let Some(mut conn_guard) = try_lock_connection_with_activity(
                                    &connection,
                                    format!("Loading indexes for {}", table_name),
                                ) else {
                                    // Query is already running, notify user
                                    let _ = sender.send(ObjectActionResult::QueryAlreadyRunning);
                                    app::awake();
                                    return;
                                };

                                let result = match conn_guard.require_live_connection() {
                                    Ok(db_conn) => ObjectBrowser::get_table_indexes(
                                        db_conn.as_ref(),
                                        &table_name,
                                    )
                                    .map_err(|err| err.to_string()),
                                    Err(message) => Err(message.to_string()),
                                };
                                let _ = sender
                                    .send(ObjectActionResult::TableIndexes { table_name, result });
                                app::awake();
                                // conn_guard drops here, releasing the lock
                            });
                        }
                        ("View Constraints", ObjectItem::Simple { object_name, .. }) => {
                            let connection = connection.clone();
                            let sender = action_sender.clone();
                            let table_name = object_name.clone();
                            Self::emit_status_callback(
                                status_callback,
                                &format!("Loading constraints for {}", table_name),
                            );
                            thread::spawn(move || {
                                // Try to acquire connection lock without blocking
                                let Some(mut conn_guard) = try_lock_connection_with_activity(
                                    &connection,
                                    format!("Loading constraints for {}", table_name),
                                ) else {
                                    // Query is already running, notify user
                                    let _ = sender.send(ObjectActionResult::QueryAlreadyRunning);
                                    app::awake();
                                    return;
                                };

                                let result = match conn_guard.require_live_connection() {
                                    Ok(db_conn) => ObjectBrowser::get_table_constraints(
                                        db_conn.as_ref(),
                                        &table_name,
                                    )
                                    .map_err(|err| err.to_string()),
                                    Err(message) => Err(message.to_string()),
                                };
                                let _ = sender.send(ObjectActionResult::TableConstraints {
                                    table_name,
                                    result,
                                });
                                app::awake();
                                // conn_guard drops here, releasing the lock
                            });
                        }
                        (
                            "View Info",
                            ObjectItem::Simple {
                                object_type,
                                object_name,
                            },
                        ) => {
                            let connection = connection.clone();
                            let sender = action_sender.clone();
                            let name = object_name.clone();
                            let obj_type = object_type.clone();
                            Self::emit_status_callback(
                                status_callback,
                                &format!("Loading {} info for {}", obj_type, name),
                            );
                            thread::spawn(move || {
                                // Try to acquire connection lock without blocking
                                let Some(mut conn_guard) = try_lock_connection_with_activity(
                                    &connection,
                                    format!("Loading {} info for {}", obj_type, name),
                                ) else {
                                    // Query is already running, notify user
                                    let _ = sender.send(ObjectActionResult::QueryAlreadyRunning);
                                    app::awake();
                                    return;
                                };

                                let send_err =
                                    |sender: &std::sync::mpsc::Sender<ObjectActionResult>,
                                     obj_type: &str,
                                     msg: &str| {
                                        match obj_type {
                                            "SYNONYMS" => {
                                                let _ =
                                                    sender.send(ObjectActionResult::SynonymInfo(
                                                        Err(msg.to_string()),
                                                    ));
                                            }
                                            "SEQUENCES" => {
                                                let _ =
                                                    sender.send(ObjectActionResult::SequenceInfo(
                                                        Err(msg.to_string()),
                                                    ));
                                            }
                                            other => {
                                                eprintln!(
                                                    "Unexpected object type for View Info: {other}"
                                                );
                                            }
                                        }
                                    };

                                if let Ok(db_conn) = conn_guard.require_live_connection() {
                                    match obj_type.as_str() {
                                        "SYNONYMS" => {
                                            let result = ObjectBrowser::get_synonym_info(
                                                db_conn.as_ref(),
                                                &name,
                                            )
                                            .map_err(|err| err.to_string());
                                            let _ = sender
                                                .send(ObjectActionResult::SynonymInfo(result));
                                        }
                                        _ => {
                                            let result = ObjectBrowser::get_sequence_info(
                                                db_conn.as_ref(),
                                                &name,
                                            )
                                            .map_err(|err| err.to_string());
                                            let _ = sender
                                                .send(ObjectActionResult::SequenceInfo(result));
                                        }
                                    }
                                } else {
                                    send_err(&sender, &obj_type, crate::db::NOT_CONNECTED_MESSAGE);
                                }
                                app::awake();
                                // conn_guard drops here, releasing the lock
                            });
                        }
                        (
                            "Generate DDL",
                            ObjectItem::Simple {
                                object_type,
                                object_name,
                            },
                        ) => {
                            let obj_type = match object_type.as_str() {
                                "TABLES" => Some("TABLE"),
                                "VIEWS" => Some("VIEW"),
                                "PROCEDURES" => Some("PROCEDURE"),
                                "FUNCTIONS" => Some("FUNCTION"),
                                "SEQUENCES" => Some("SEQUENCE"),
                                "TRIGGERS" => Some("TRIGGER"),
                                "SYNONYMS" => Some("SYNONYM"),
                                "PACKAGES" => Some("PACKAGE"),
                                _ => None,
                            };
                            if let Some(obj_type) = obj_type {
                                let connection = connection.clone();
                                let sender = action_sender.clone();
                                let object_type = obj_type.to_string();
                                let object_name = object_name.clone();
                                Self::emit_status_callback(
                                    status_callback,
                                    &format!("Generating {} DDL for {}", object_type, object_name),
                                );
                                thread::spawn(move || {
                                    // Try to acquire connection lock without blocking
                                    let Some(mut conn_guard) = try_lock_connection_with_activity(
                                        &connection,
                                        format!(
                                            "Generating {} DDL for {}",
                                            object_type, object_name
                                        ),
                                    ) else {
                                        // Query is already running, notify user
                                        let _ =
                                            sender.send(ObjectActionResult::QueryAlreadyRunning);
                                        app::awake();
                                        return;
                                    };

                                    let result = match conn_guard.require_live_connection() {
                                        Ok(db_conn) => match object_type.as_str() {
                                            "TABLE" => ObjectBrowser::get_table_ddl(
                                                db_conn.as_ref(),
                                                &object_name,
                                            ),
                                            "VIEW" => ObjectBrowser::get_view_ddl(
                                                db_conn.as_ref(),
                                                &object_name,
                                            ),
                                            "PROCEDURE" => ObjectBrowser::get_procedure_ddl(
                                                db_conn.as_ref(),
                                                &object_name,
                                            ),
                                            "FUNCTION" => ObjectBrowser::get_function_ddl(
                                                db_conn.as_ref(),
                                                &object_name,
                                            ),
                                            "SEQUENCE" => ObjectBrowser::get_sequence_ddl(
                                                db_conn.as_ref(),
                                                &object_name,
                                            ),
                                            "TRIGGER" => ObjectBrowser::get_object_ddl(
                                                db_conn.as_ref(),
                                                "TRIGGER",
                                                &object_name,
                                            ),
                                            "SYNONYM" => ObjectBrowser::get_synonym_ddl(
                                                db_conn.as_ref(),
                                                &object_name,
                                            ),
                                            "PACKAGE" => ObjectBrowser::get_package_spec_ddl(
                                                db_conn.as_ref(),
                                                &object_name,
                                            ),
                                            _ => return,
                                        }
                                        .map_err(|err| err.to_string()),
                                        Err(message) => Err(message.to_string()),
                                    };
                                    let _ = sender.send(ObjectActionResult::Ddl(result));
                                    app::awake();
                                    // conn_guard drops here, releasing the lock
                                });
                            }
                        }
                        _ => {}
                    }
                };
                handle_choice();
            }

            // FLTK memory management: widgets created without a parent must be deleted.
            fltk::menu::MenuButton::delete(menu);
        }
    }

    fn show_info_dialog(title: &str, content: &str) {
        use fltk::{prelude::*, text::TextDisplay, window::Window};

        let current_group = fltk::group::Group::try_current();
        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let mut dialog = Window::default().with_size(700, 500).with_label(title);
        crate::ui::center_on_main(&mut dialog);
        dialog.set_color(theme::panel_raised());
        dialog.make_modal(true);

        let mut display = TextDisplay::default().with_pos(10, 10).with_size(680, 440);
        display.set_color(theme::editor_bg());
        display.set_text_color(theme::text_primary());
        display.set_text_font(configured_editor_profile().normal);
        display.set_text_size(configured_ui_font_size());

        let mut buffer = fltk::text::TextBuffer::default();
        buffer.set_text(content);
        display.set_buffer(buffer);

        let mut close_btn = fltk::button::Button::default()
            .with_pos(300, 460)
            .with_size(BUTTON_WIDTH_LARGE, BUTTON_HEIGHT)
            .with_label("Close");
        close_btn.set_color(theme::button_secondary());
        close_btn.set_label_color(theme::text_primary());

        let (sender, receiver) = std::sync::mpsc::channel::<()>();
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

        // Top-level dialogs created without a parent should be explicitly deleted
        // to avoid retaining native FLTK resources across repeated opens.
        Window::delete(dialog);
    }

    pub fn set_sql_callback<F>(&mut self, callback: F)
    where
        F: FnMut(SqlAction) + 'static,
    {
        *self.sql_callback.lock().unwrap() = Some(Box::new(callback));
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
            "object_browser::callback",
            &format!("{context} panicked: {panic_payload}"),
        );
        eprintln!("{context} panicked: {panic_payload}");
    }

    fn emit_sql_callback(callback_slot: &SqlExecuteCallback, action: SqlAction) {
        let callback = {
            let mut slot = callback_slot.lock().unwrap();
            slot.take()
        };

        if let Some(mut cb) = callback {
            let call_result = panic::catch_unwind(AssertUnwindSafe(|| cb(action)));
            let mut slot = callback_slot.lock().unwrap();
            if slot.is_none() {
                *slot = Some(cb);
            }
            if let Err(payload) = call_result {
                Self::log_callback_panic("SQL callback", payload.as_ref());
            }
        }
    }

    fn emit_status_callback(callback_slot: &StatusCallback, message: &str) {
        let callback = {
            let mut slot = callback_slot.lock().unwrap();
            slot.take()
        };

        if let Some(mut cb) = callback {
            let call_result = panic::catch_unwind(AssertUnwindSafe(|| cb(message)));
            let mut slot = callback_slot.lock().unwrap();
            if slot.is_none() {
                *slot = Some(cb);
            }
            if let Err(payload) = call_result {
                Self::log_callback_panic("status callback", payload.as_ref());
            }
        }
    }

    fn emit_status(&self, message: &str) {
        Self::emit_status_callback(&self.status_callback, message);
    }

    pub fn set_status_callback<F>(&mut self, callback: F)
    where
        F: FnMut(&str) + 'static,
    {
        *self.status_callback.lock().unwrap() = Some(Box::new(callback));
    }

    pub fn refresh(&mut self) {
        // First clear items and filter
        self.clear_items();
        self.filter_input.set_value("");
        *self.object_cache.lock().unwrap() = ObjectCache::default();
        self.emit_status("Refreshing object browser metadata");

        let sender = self.refresh_sender.clone();
        let connection = self.connection.clone();

        thread::spawn(move || {
            // Acquire connection lock and hold it during all queries
            let mut conn_guard =
                lock_connection_with_activity(&connection, "Refreshing object browser metadata");
            let Ok(db_conn) = conn_guard.require_live_connection() else {
                return;
            };
            // Keep conn_guard alive (don't drop it) so the lock is held during execution

            let mut cache = ObjectCache::default();
            let send_update = |sender: &std::sync::mpsc::Sender<RefreshEvent>,
                               cache: &ObjectCache| {
                let _ = sender.send(RefreshEvent::Cache(cache.clone()));
                app::awake();
            };

            if let Ok(tables) = ObjectBrowser::get_tables(db_conn.as_ref()) {
                cache.tables = tables;
                send_update(&sender, &cache);
            }

            if let Ok(views) = ObjectBrowser::get_views(db_conn.as_ref()) {
                cache.views = views;
                send_update(&sender, &cache);
            }

            if let Ok(procedures) = ObjectBrowser::get_procedures(db_conn.as_ref()) {
                cache.procedures = procedures;
                send_update(&sender, &cache);
            }

            if let Ok(functions) = ObjectBrowser::get_functions(db_conn.as_ref()) {
                cache.functions = functions;
                send_update(&sender, &cache);
            }

            if let Ok(sequences) = ObjectBrowser::get_sequences(db_conn.as_ref()) {
                cache.sequences = sequences;
                send_update(&sender, &cache);
            }

            if let Ok(triggers) = ObjectBrowser::get_triggers(db_conn.as_ref()) {
                cache.triggers = triggers;
                send_update(&sender, &cache);
            }

            if let Ok(synonyms) = ObjectBrowser::get_synonyms(db_conn.as_ref()) {
                cache.synonyms = synonyms;
                send_update(&sender, &cache);
            }

            if let Ok(packages) = ObjectBrowser::get_packages(db_conn.as_ref()) {
                cache.packages = packages;
                send_update(&sender, &cache);
            }

            let _ = sender.send(RefreshEvent::Completed);
            app::awake();

            // conn_guard drops here, releasing the lock
        });
    }

    fn clear_items(&mut self) {
        Self::clear_tree_items(&mut self.tree);
    }

    fn clear_tree_items(tree: &mut Tree) {
        let categories = [
            "Tables",
            "Views",
            "Procedures",
            "Functions",
            "Sequences",
            "Triggers",
            "Synonyms",
            "Packages",
        ];

        for category in categories {
            if let Some(item) = tree.find_item(category) {
                while item.has_children() {
                    if let Some(child) = item.child(0) {
                        let _ = tree.remove(&child);
                    } else {
                        break;
                    }
                }
            }
        }
    }

    fn rebuild_root_categories(tree: &mut Tree) {
        let categories = [
            "Tables",
            "Views",
            "Procedures",
            "Functions",
            "Sequences",
            "Triggers",
            "Synonyms",
            "Packages",
        ];

        for category in categories {
            if let Some(item) = tree.find_item(category) {
                let _ = tree.remove(&item);
            }
            tree.add(category);
            if let Some(mut item) = tree.find_item(category) {
                item.close();
            }
        }
    }

    fn populate_tree(tree: &mut Tree, cache: &ObjectCache, filter_text: &str) {
        Self::clear_tree_items(tree);

        for table in &cache.tables {
            if filter_text.is_empty() || table.to_lowercase().contains(filter_text) {
                tree.add(&format!("Tables/{}", table));
            }
        }
        for view in &cache.views {
            if filter_text.is_empty() || view.to_lowercase().contains(filter_text) {
                tree.add(&format!("Views/{}", view));
            }
        }
        for proc in &cache.procedures {
            if filter_text.is_empty() || proc.to_lowercase().contains(filter_text) {
                tree.add(&format!("Procedures/{}", proc));
            }
        }
        for func in &cache.functions {
            if filter_text.is_empty() || func.to_lowercase().contains(filter_text) {
                tree.add(&format!("Functions/{}", func));
            }
        }
        for seq in &cache.sequences {
            if filter_text.is_empty() || seq.to_lowercase().contains(filter_text) {
                tree.add(&format!("Sequences/{}", seq));
            }
        }
        for trig in &cache.triggers {
            if filter_text.is_empty() || trig.to_lowercase().contains(filter_text) {
                tree.add(&format!("Triggers/{}", trig));
            }
        }
        for syn in &cache.synonyms {
            if filter_text.is_empty() || syn.to_lowercase().contains(filter_text) {
                tree.add(&format!("Synonyms/{}", syn));
            }
        }

        for package in &cache.packages {
            let routines = cache
                .package_routines
                .get(package)
                .cloned()
                .unwrap_or_default();
            let package_matches =
                filter_text.is_empty() || package.to_lowercase().contains(filter_text);
            let matching_routines: Vec<PackageRoutine> = routines
                .into_iter()
                .filter(|routine| {
                    filter_text.is_empty()
                        || routine.name.to_lowercase().contains(filter_text)
                        || package_matches
                })
                .collect();

            if package_matches || !matching_routines.is_empty() {
                tree.add(&format!("Packages/{}", package));
                for routine in matching_routines {
                    if routine.routine_type == "FUNCTION" {
                        tree.add(&format!("Packages/{}/Functions/{}", package, routine.name));
                    } else {
                        tree.add(&format!("Packages/{}/Procedures/{}", package, routine.name));
                    }
                }
            }
        }
    }

    #[allow(dead_code)]
    pub fn get_selected_item(&self) -> Option<String> {
        self.tree
            .first_selected_item()
            .and_then(|item| item.label())
    }
}

impl Drop for ObjectBrowserWidget {
    fn drop(&mut self) {
        // Release callback closures early so captured state does not outlive
        // the widget tree unnecessarily.
        self.filter_input.set_callback(|_| {});
        self.tree.handle(|_, _| false);
        *self.sql_callback.lock().unwrap() = None;
        *self.status_callback.lock().unwrap() = None;
    }
}
