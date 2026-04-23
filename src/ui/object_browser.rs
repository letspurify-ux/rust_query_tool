use fltk::{
    app,
    enums::{Event, Key},
    group::{Flex, FlexType},
    input::Input,
    menu::Choice,
    prelude::*,
    tree::{Tree, TreeItem, TreeSelect},
};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::panic::{self, AssertUnwindSafe};
use std::sync::mpsc::{Receiver, RecvError, Sender, TryRecvError};
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::Duration;

use crate::db::{
    format_connection_busy_message, lock_connection_with_activity,
    try_lock_connection_with_activity, ColumnInfo, CompilationError, ConstraintInfo, IndexInfo,
    ObjectBrowser, PackageRoutine, ProcedureArgument, QueryResult, SequenceInfo, SharedConnection,
    SynonymInfo, TableColumnDetail,
};
use crate::ui::constants::*;
use crate::ui::font_settings::FontProfile;
use crate::ui::theme;
use crate::ui::ResultTabRequest;

#[derive(Clone)]
pub enum SqlAction {
    Insert(String),
    OpenInNewTab(String),
    Execute(String),
    DisplayResult(ResultTabRequest),
}

/// Callback type for executing SQL from object browser
pub type SqlExecuteCallback = Arc<Mutex<Option<Box<dyn FnMut(SqlAction)>>>>;
type StatusCallback = Arc<Mutex<Option<Box<dyn FnMut(&str)>>>>;

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
    events: Vec<String>,
    synonyms: Vec<String>,
    packages: Vec<String>,
    package_routines: HashMap<String, Vec<PackageRoutine>>,
}

#[derive(Clone)]
enum RefreshEvent {
    Finished {
        cache: ObjectCache,
        db_type: crate::db::DatabaseType,
        owner: Option<String>,
        owners: Vec<String>,
    },
}

#[derive(Clone)]
enum RefreshRequest {
    Metadata { owner: Option<String> },
}

const REFRESH_TREE_BATCH_SIZE: usize = 300;

struct PendingTreeRefresh {
    paths: Vec<String>,
    next_index: usize,
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
        db_type: crate::db::DatabaseType,
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
    owner_choice: Choice,
    owner_list: Arc<Mutex<Vec<String>>>,
    selected_owner: Arc<Mutex<Option<String>>>,
    object_cache: Arc<Mutex<ObjectCache>>,
    current_db_type: Arc<Mutex<crate::db::DatabaseType>>,
    pending_tree_refresh: Arc<Mutex<Option<PendingTreeRefresh>>>,
    poll_lifecycle: Arc<()>,
    refresh_request_sender: Sender<RefreshRequest>,
    action_sender: std::sync::mpsc::Sender<ObjectActionResult>,
    owner_change_callback: Arc<Mutex<Option<Box<dyn FnMut(Option<String>)>>>>,
}

impl ObjectBrowserWidget {
    pub fn new(x: i32, y: i32, w: i32, h: i32, connection: SharedConnection) -> Self {
        let initial_db_type = crate::db::try_lock_connection(&connection)
            .map(|guard| guard.db_type())
            .unwrap_or(crate::db::DatabaseType::Oracle);

        // Create a flex container for the filter input and tree
        let mut flex = Flex::default().with_pos(x, y).with_size(w, h);
        flex.set_type(FlexType::Column);
        flex.set_spacing(DIALOG_SPACING);

        let mut filter_row = Flex::default();
        filter_row.set_type(FlexType::Row);
        filter_row.set_spacing(DIALOG_SPACING);

        let mut owner_choice = Choice::default();
        owner_choice.set_color(theme::input_bg());
        owner_choice.set_text_color(theme::text_primary());
        owner_choice.hide();
        filter_row.fixed(&owner_choice, 180);

        // Filter input with modern styling
        let mut filter_input = Input::default();
        filter_input.set_color(theme::input_bg());
        filter_input.set_text_color(theme::text_primary());
        filter_input.set_tooltip("Type to filter objects...");
        filter_row.resizable(&filter_input);
        filter_row.end();
        flex.fixed(&filter_row, FILTER_INPUT_HEIGHT);

        // Tree view with modern styling
        let mut tree = Tree::default();

        tree.set_color(theme::panel_bg());
        tree.set_selection_color(theme::selection_soft());
        tree.set_item_label_fgcolor(theme::text_secondary());
        tree.set_connector_color(theme::tree_connector());
        tree.set_select_mode(TreeSelect::Single);

        // Initialize tree structure
        tree.set_show_root(false);
        Self::rebuild_root_categories_for_db_type(
            &mut tree,
            initial_db_type,
            &ObjectCache::default(),
        );

        // Make tree resizable (takes remaining space after filter input)
        flex.resizable(&tree);
        flex.end();

        let sql_callback: SqlExecuteCallback = Arc::new(Mutex::new(None));
        let status_callback: StatusCallback = Arc::new(Mutex::new(None));
        let object_cache = Arc::new(Mutex::new(ObjectCache::default()));
        let owner_list = Arc::new(Mutex::new(Vec::new()));
        let selected_owner = Arc::new(Mutex::new(None));
        let current_db_type = Arc::new(Mutex::new(initial_db_type));
        let pending_tree_refresh = Arc::new(Mutex::new(None));
        let poll_lifecycle = Arc::new(());

        let (refresh_sender, refresh_receiver) = std::sync::mpsc::channel::<RefreshEvent>();
        let (refresh_request_sender, refresh_request_receiver) =
            std::sync::mpsc::channel::<RefreshRequest>();
        let (action_sender, action_receiver) = std::sync::mpsc::channel::<ObjectActionResult>();

        Self::spawn_refresh_worker(refresh_request_receiver, refresh_sender, connection.clone());

        let mut widget = Self {
            flex,
            tree,
            connection,
            filter_input,
            owner_choice,
            owner_list,
            selected_owner,
            object_cache,
            current_db_type,
            pending_tree_refresh,
            poll_lifecycle,
            sql_callback,
            status_callback,
            refresh_request_sender,
            action_sender,
            owner_change_callback: Arc::new(Mutex::new(None)),
        };
        widget.setup_callbacks();
        widget.setup_filter_callback();
        widget.setup_owner_callback();
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
        let canceled_pending_refresh = self.clear_pending_tree_refresh();
        let filter_text = self.filter_input.value().to_lowercase();
        let cache_snapshot = self
            .object_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let db_type = self
            .current_db_type
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .to_owned();
        Self::rebuild_root_categories_for_db_type(&mut self.tree, db_type, &cache_snapshot);
        Self::populate_tree(&mut self.tree, &cache_snapshot, &filter_text);
        // Force layout recalculation so new font metrics take effect immediately.
        let (x, y, w, h) = (self.tree.x(), self.tree.y(), self.tree.w(), self.tree.h());
        self.tree.resize(x, y, w, h);
        self.flex.layout();
        self.filter_input.redraw();
        self.tree.redraw();
        if canceled_pending_refresh {
            self.emit_status("Object browser metadata refresh completed");
        }
    }

    fn setup_filter_callback(&mut self) {
        let mut tree = self.tree.clone();
        let object_cache = self.object_cache.clone();
        let pending_tree_refresh = self.pending_tree_refresh.clone();
        let current_db_type = self.current_db_type.clone();
        let status_callback = self.status_callback.clone();

        self.filter_input.set_callback(move |input| {
            let canceled_pending_refresh = {
                let mut pending = pending_tree_refresh
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let had_pending = pending.is_some();
                *pending = None;
                had_pending
            };
            let filter_text = input.value().to_lowercase();
            let cache = object_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let db_type = *current_db_type
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            ObjectBrowserWidget::rebuild_root_categories_for_db_type(&mut tree, db_type, &cache);
            ObjectBrowserWidget::populate_tree(&mut tree, &cache, &filter_text);
            tree.redraw();
            if canceled_pending_refresh {
                ObjectBrowserWidget::emit_status_callback(
                    &status_callback,
                    "Object browser metadata refresh completed",
                );
            }
        });
    }

    fn setup_owner_callback(&mut self) {
        let owner_choice = self.owner_choice.clone();
        let owner_list = self.owner_list.clone();
        let selected_owner = self.selected_owner.clone();
        let refresh_sender = self.refresh_request_sender.clone();
        let owner_change_callback = self.owner_change_callback.clone();

        self.owner_choice.set_callback(move |_| {
            let owners = owner_list
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            let idx = owner_choice.value();
            if idx < 0 {
                return;
            }
            let Some(owner) = owners.get(idx as usize).cloned() else {
                return;
            };
            *selected_owner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(owner.clone());
            let _ = refresh_sender.send(RefreshRequest::Metadata {
                owner: Some(owner.clone()),
            });
            if let Ok(mut callback_guard) = owner_change_callback.lock() {
                if let Some(callback) = callback_guard.as_mut() {
                    callback(Some(owner));
                }
            }
        });
    }

    fn setup_refresh_handler(&mut self, refresh_receiver: std::sync::mpsc::Receiver<RefreshEvent>) {
        let tree = self.tree.clone();
        let object_cache = self.object_cache.clone();
        let current_db_type = self.current_db_type.clone();
        let filter_input = self.filter_input.clone();
        let pending_tree_refresh = self.pending_tree_refresh.clone();
        let owner_choice = self.owner_choice.clone();
        let owner_list = self.owner_list.clone();
        let selected_owner = self.selected_owner.clone();

        let lifecycle = Arc::downgrade(&self.poll_lifecycle);

        // Wrap receiver in Arc<Mutex> to share across timeout callbacks
        let receiver: Arc<Mutex<std::sync::mpsc::Receiver<RefreshEvent>>> =
            Arc::new(Mutex::new(refresh_receiver));

        fn schedule_poll(
            receiver: Arc<Mutex<Receiver<RefreshEvent>>>,
            mut tree: Tree,
            object_cache: Arc<Mutex<ObjectCache>>,
            current_db_type: Arc<Mutex<crate::db::DatabaseType>>,
            filter_input: Input,
            pending_tree_refresh: Arc<Mutex<Option<PendingTreeRefresh>>>,
            mut owner_choice: Choice,
            owner_list: Arc<Mutex<Vec<String>>>,
            selected_owner: Arc<Mutex<Option<String>>>,
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
            // Keep receiver lock scope minimal: drain messages first, then perform UI work.
            // This prevents long lock hold while rebuilding tree widgets.
            let mut latest_cache: Option<(
                crate::db::DatabaseType,
                ObjectCache,
                Option<String>,
                Vec<String>,
            )> = None;

            {
                let r = receiver
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                loop {
                    match r.try_recv() {
                        Ok(RefreshEvent::Finished {
                            cache,
                            db_type,
                            owner,
                            owners,
                        }) => {
                            latest_cache = Some((db_type, cache, owner, owners));
                            match current_db_type.lock() {
                                Ok(mut guard) => *guard = db_type,
                                Err(poisoned) => *poisoned.into_inner() = db_type,
                            }
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => {
                            disconnected = true;
                            break;
                        }
                    }
                }
            }

            if let Some((db_type, cache, owner, owners)) = latest_cache {
                let filter_text = filter_input.value().to_lowercase();
                let paths = ObjectBrowserWidget::collect_tree_paths(&cache, &filter_text);
                let cache_snapshot = cache.clone();

                {
                    let mut cache_guard = object_cache
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    *cache_guard = cache;
                }
                {
                    *owner_list
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = owners.clone();
                    *selected_owner
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = owner.clone();
                }
                if db_type.uses_oracle_sql_dialect() && !owners.is_empty() {
                    owner_choice.clear();
                    owner_choice.add_choice(&owners.join("|"));
                    if let Some(selected_owner_name) = owner {
                        let selected_upper = selected_owner_name.to_uppercase();
                        let selected_idx = owners
                            .iter()
                            .position(|candidate| candidate.to_uppercase() == selected_upper)
                            .unwrap_or(0);
                        owner_choice.set_value(selected_idx as i32);
                    } else {
                        owner_choice.set_value(0);
                    }
                    owner_choice.show();
                    owner_choice.activate();
                } else {
                    owner_choice.clear();
                    owner_choice.hide();
                }

                ObjectBrowserWidget::rebuild_root_categories_for_db_type(
                    &mut tree,
                    db_type,
                    &cache_snapshot,
                );
                ObjectBrowserWidget::clear_tree_items(&mut tree);
                {
                    let mut pending = pending_tree_refresh
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    *pending = Some(PendingTreeRefresh {
                        paths,
                        next_index: 0,
                    });
                }
            }

            let mut next_paths = Vec::new();
            let mut finished_refresh = false;
            {
                let mut pending = pending_tree_refresh
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if let Some(task) = pending.as_mut() {
                    let end = task
                        .next_index
                        .saturating_add(REFRESH_TREE_BATCH_SIZE)
                        .min(task.paths.len());
                    if task.next_index < end {
                        next_paths.extend(task.paths[task.next_index..end].iter().cloned());
                        task.next_index = end;
                    }
                    if task.next_index >= task.paths.len() {
                        *pending = None;
                        finished_refresh = true;
                    }
                }
            }

            if !next_paths.is_empty() {
                for path in next_paths {
                    tree.add(&path);
                }
                tree.redraw();
            }

            if finished_refresh {
                tree.redraw();
                ObjectBrowserWidget::emit_status_callback(
                    &status_callback,
                    "Object browser metadata refresh completed",
                );
            }

            if disconnected {
                return;
            }

            // Reschedule for next poll
            app::add_timeout3(0.05, move |_| {
                schedule_poll(
                    receiver.clone(),
                    tree.clone(),
                    object_cache.clone(),
                    current_db_type.clone(),
                    filter_input.clone(),
                    pending_tree_refresh.clone(),
                    owner_choice.clone(),
                    owner_list.clone(),
                    selected_owner.clone(),
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
            current_db_type,
            filter_input,
            pending_tree_refresh,
            owner_choice,
            owner_list,
            selected_owner,
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
        let lifecycle = Arc::downgrade(&self.poll_lifecycle);

        let receiver: Arc<Mutex<std::sync::mpsc::Receiver<ObjectActionResult>>> =
            Arc::new(Mutex::new(action_receiver));

        fn schedule_poll(
            receiver: Arc<Mutex<std::sync::mpsc::Receiver<ObjectActionResult>>>,
            sql_callback: SqlExecuteCallback,
            status_callback: StatusCallback,
            mut tree: Tree,
            object_cache: Arc<Mutex<ObjectCache>>,
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
                    let r = receiver
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    r.try_recv()
                };

                match message {
                    Ok(action) => match action {
                        ObjectActionResult::TableStructure { table_name, result } => match result {
                            Ok(columns) => {
                                ObjectBrowserWidget::emit_sql_callback(
                                    &sql_callback,
                                    SqlAction::DisplayResult(
                                        ObjectBrowserWidget::build_table_structure_result_request(
                                            &table_name,
                                            &columns,
                                        ),
                                    ),
                                );
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
                                ObjectBrowserWidget::emit_sql_callback(
                                    &sql_callback,
                                    SqlAction::DisplayResult(
                                        ObjectBrowserWidget::build_table_indexes_result_request(
                                            &table_name,
                                            &indexes,
                                        ),
                                    ),
                                );
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
                                ObjectBrowserWidget::emit_sql_callback(
                                    &sql_callback,
                                    SqlAction::DisplayResult(
                                        ObjectBrowserWidget::build_table_constraints_result_request(
                                            &table_name,
                                            &constraints,
                                        ),
                                    ),
                                );
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
                                ObjectBrowserWidget::emit_sql_callback(
                                    &sql_callback,
                                    SqlAction::DisplayResult(
                                        ObjectBrowserWidget::build_sequence_info_result_request(
                                            &info,
                                        ),
                                    ),
                                );
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
                                ObjectBrowserWidget::emit_sql_callback(
                                    &sql_callback,
                                    SqlAction::DisplayResult(
                                        ObjectBrowserWidget::build_synonym_info_result_request(
                                            &info,
                                        ),
                                    ),
                                );
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
                            db_type,
                            result,
                        } => {
                            let sql = match result {
                                Ok(sql) => sql,
                                Err(err) => {
                                    fltk::dialog::alert_default(&format!(
                                        "Failed to load routine arguments: {}",
                                        err
                                    ));
                                    ObjectBrowserWidget::build_simple_routine_script_for_db(
                                        db_type,
                                        &qualified_name,
                                        &routine_type,
                                    )
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
                                let mut cache = object_cache
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner());
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
                                ObjectBrowserWidget::emit_sql_callback(
                                    &sql_callback,
                                    SqlAction::DisplayResult(
                                        ObjectBrowserWidget::build_compilation_result_request(
                                            &object_name,
                                            &object_type,
                                            &status,
                                            &errors,
                                        ),
                                    ),
                                );
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
                    receiver.clone(),
                    sql_callback.clone(),
                    status_callback.clone(),
                    tree.clone(),
                    object_cache.clone(),
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
        let current_db_type = self.current_db_type.clone();

        self.tree.handle(move |t, ev| {
            if !t.active() {
                return false;
            }
            match ev {
                Event::Push => {
                    let mouse_button = fltk::app::event_button();
                    if mouse_button == fltk::app::MouseButton::Right as i32 {
                        let clicked_item = t
                            .find_clicked(false)
                            .or_else(|| t.find_clicked(true))
                            .or_else(|| Self::item_at_mouse(t));

                        if let Some(item) = clicked_item {
                            let _ = t.select_only(&item, false);
                            t.set_item_focus(&item);
                            Self::show_context_menu(
                                &connection,
                                &current_db_type,
                                &item,
                                &sql_callback,
                                &status_callback,
                                &action_sender,
                            );
                        } else if let Some(item) = t.first_selected_item() {
                            Self::show_context_menu(
                                &connection,
                                &current_db_type,
                                &item,
                                &sql_callback,
                                &status_callback,
                                &action_sender,
                            );
                        }
                        return true;
                    }

                    if mouse_button == fltk::app::MouseButton::Left as i32
                        && fltk::app::event_clicks()
                    {
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
                                    let package_name = object_name;
                                    let should_fetch = {
                                        let cache = object_cache
                                            .lock()
                                            .unwrap_or_else(|poisoned| poisoned.into_inner());
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
                                                let _ = sender
                                                    .send(ObjectActionResult::QueryAlreadyRunning);
                                                app::awake();
                                                return;
                                            };

                                            let result = match conn_guard.require_live_connection()
                                            {
                                                Ok(db_conn) => ObjectBrowser::get_package_routines(
                                                    db_conn.as_ref(),
                                                    &package_name,
                                                )
                                                .map_err(|err| err.to_string()),
                                                Err(message) => Err(message),
                                            };

                                            let _ =
                                                sender.send(ObjectActionResult::PackageRoutines {
                                                    package_name,
                                                    result,
                                                });
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
                                    let db_type = if let Some(conn_guard) =
                                        try_lock_connection_with_activity(
                                            &connection,
                                            format!("Preparing preview SQL for {}", object_name),
                                        ) {
                                        if !conn_guard.is_connected()
                                            || !conn_guard.has_connection_handle()
                                        {
                                            fltk::dialog::alert_default(
                                                "Not connected to database",
                                            );
                                            return true;
                                        }
                                        conn_guard.db_type()
                                    } else {
                                        fltk::dialog::alert_default(
                                            &format_connection_busy_message(),
                                        );
                                        return true;
                                    };
                                    let sql = ObjectBrowserWidget::preview_select_sql(
                                        db_type,
                                        &object_name,
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
        let parent = item.parent()?;
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
            | "EVENTS" | "SYNONYMS" | "PACKAGES" => Some(ObjectItem::Simple {
                object_type: parent_type_upper,
                object_name,
            }),
            _ => None,
        }
    }

    fn get_insert_text(item: &TreeItem) -> Option<String> {
        Self::get_item_info(item)
            .as_ref()
            .map(copy_text_for_object_item)
    }

    fn copy_text_for_selected_item(item: &TreeItem) -> Option<String> {
        Self::get_item_info(item)
            .as_ref()
            .map(copy_text_for_object_item)
            .or_else(|| {
                item.label().and_then(|label| {
                    let trimmed = label.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.to_string())
                    }
                })
            })
    }

    fn quote_mysql_identifier_path(identifier: &str) -> String {
        identifier
            .split('.')
            .filter_map(|segment| {
                let trimmed = segment.trim().trim_matches('`');
                if trimmed.is_empty() {
                    None
                } else {
                    Some(format!("`{}`", trimmed.replace('`', "``")))
                }
            })
            .collect::<Vec<_>>()
            .join(".")
    }

    fn preview_select_sql(db_type: crate::db::DatabaseType, object_name: &str) -> String {
        match db_type.sql_dialect() {
            crate::db::DbSqlDialect::Oracle => {
                format!("SELECT * FROM {} WHERE ROWNUM <= 100", object_name)
            }
            crate::db::DbSqlDialect::MySql => format!(
                "SELECT * FROM {} LIMIT 100",
                Self::quote_mysql_identifier_path(object_name)
            ),
        }
    }

    fn quote_mysql_alias(alias: &str) -> String {
        format!("`{}`", alias.trim().trim_matches('`').replace('`', "``"))
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

    fn build_simple_procedure_script_for_db(
        db_type: crate::db::DatabaseType,
        qualified_name: &str,
    ) -> String {
        match db_type.sql_dialect() {
            crate::db::DbSqlDialect::Oracle => Self::build_simple_procedure_script(qualified_name),
            crate::db::DbSqlDialect::MySql => {
                format!(
                    "CALL {}();\n",
                    Self::quote_mysql_identifier_path(qualified_name)
                )
            }
        }
    }

    fn build_simple_function_script_for_db(
        db_type: crate::db::DatabaseType,
        qualified_name: &str,
    ) -> String {
        match db_type.sql_dialect() {
            crate::db::DbSqlDialect::Oracle => Self::build_simple_function_script(qualified_name),
            crate::db::DbSqlDialect::MySql => format!(
                "SELECT {} AS result;\n",
                if qualified_name.contains('(') {
                    qualified_name.to_string()
                } else {
                    format!("{}()", Self::quote_mysql_identifier_path(qualified_name))
                }
            ),
        }
    }

    fn build_simple_routine_script_for_db(
        db_type: crate::db::DatabaseType,
        qualified_name: &str,
        routine_type: &str,
    ) -> String {
        if routine_type.eq_ignore_ascii_case("FUNCTION") {
            Self::build_simple_function_script_for_db(db_type, qualified_name)
        } else {
            Self::build_simple_procedure_script_for_db(db_type, qualified_name)
        }
    }

    fn default_value_for_mysql_argument(arg: &ProcedureArgument, type_str: &str) -> String {
        if let Some(default_value) = arg.default_value.as_deref() {
            let trimmed = default_value.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }

        let base = Self::normalize_type_base(type_str);
        match base.as_str() {
            "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" | "BIGINT" | "DECIMAL"
            | "NUMERIC" | "FLOAT" | "DOUBLE" | "REAL" | "BIT" => "0".to_string(),
            "DATE" => "CURRENT_DATE".to_string(),
            "DATETIME" | "TIMESTAMP" => "CURRENT_TIMESTAMP".to_string(),
            "TIME" => "CURRENT_TIME".to_string(),
            "BOOLEAN" | "BOOL" => "FALSE".to_string(),
            "CHAR" | "VARCHAR" | "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" | "ENUM"
            | "SET" | "JSON" => "''".to_string(),
            _ => "NULL".to_string(),
        }
    }

    fn build_mysql_routine_script(
        qualified_name: &str,
        routine_type: &str,
        arguments: &[ProcedureArgument],
    ) -> String {
        let selected_args = Self::select_overload_arguments(arguments);
        if selected_args.is_empty() {
            return Self::build_simple_routine_script_for_db(
                crate::db::DatabaseType::MySQL,
                qualified_name,
                routine_type,
            );
        }

        let target = Self::quote_mysql_identifier_path(qualified_name);
        let mut used_names: HashSet<String> = HashSet::new();
        let mut prelude_lines: Vec<String> = Vec::new();
        let mut call_args: Vec<String> = Vec::new();
        let mut post_lines: Vec<String> = Vec::new();

        for arg in &selected_args {
            if arg.position == 0 && arg.name.is_none() {
                continue;
            }

            let arg_label = arg
                .name
                .clone()
                .unwrap_or_else(|| format!("arg{}", arg.position.max(1)));
            let direction = arg
                .in_out
                .clone()
                .unwrap_or_else(|| "IN".to_string())
                .replace('/', " ")
                .to_uppercase();
            let type_str = Self::format_argument_type(arg);

            if direction.contains("OUT") && !direction.contains("IN") {
                let session_var = format!(
                    "@{}",
                    Self::unique_var_name(&arg_label, arg.position, &mut used_names)
                );
                call_args.push(session_var.clone());
                post_lines.push(format!(
                    "SELECT {} AS {};",
                    session_var,
                    Self::quote_mysql_alias(&arg_label)
                ));
                continue;
            }

            if direction.contains("IN") && direction.contains("OUT") {
                let session_var = format!(
                    "@{}",
                    Self::unique_var_name(&arg_label, arg.position, &mut used_names)
                );
                prelude_lines.push(format!(
                    "SET {} = {};",
                    session_var,
                    Self::default_value_for_mysql_argument(arg, &type_str)
                ));
                call_args.push(session_var.clone());
                post_lines.push(format!(
                    "SELECT {} AS {};",
                    session_var,
                    Self::quote_mysql_alias(&arg_label)
                ));
                continue;
            }

            call_args.push(Self::default_value_for_mysql_argument(arg, &type_str));
        }

        let multiline_args = if call_args.is_empty() {
            String::new()
        } else {
            let mut args_sql = String::from("(\n");
            for (index, arg) in call_args.iter().enumerate() {
                let suffix = if index + 1 == call_args.len() {
                    ""
                } else {
                    ","
                };
                args_sql.push_str(&format!("    {}{}\n", arg, suffix));
            }
            args_sql.push(')');
            args_sql
        };

        let mut script = String::new();
        for line in prelude_lines {
            script.push_str(&line);
            script.push('\n');
        }

        if routine_type.eq_ignore_ascii_case("FUNCTION") {
            if multiline_args.is_empty() {
                script.push_str(&format!("SELECT {}() AS result;\n", target));
            } else {
                script.push_str(&format!("SELECT {}{} AS result;\n", target, multiline_args));
            }
            return script;
        }

        if multiline_args.is_empty() {
            script.push_str(&format!("CALL {}();\n", target));
        } else {
            script.push_str(&format!("CALL {}{};\n", target, multiline_args));
        }

        for line in post_lines {
            script.push_str(&line);
            script.push('\n');
        }

        script
    }

    fn build_routine_script_for_db(
        db_type: crate::db::DatabaseType,
        qualified_name: &str,
        routine_type: &str,
        arguments: &[ProcedureArgument],
    ) -> String {
        match db_type.sql_dialect() {
            crate::db::DbSqlDialect::Oracle => {
                Self::build_procedure_script(qualified_name, arguments)
            }
            crate::db::DbSqlDialect::MySql => {
                Self::build_mysql_routine_script(qualified_name, routine_type, arguments)
            }
        }
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
            qualified_name.to_string()
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
        current_db_type: &Arc<Mutex<crate::db::DatabaseType>>,
        item: &TreeItem,
        sql_callback: &SqlExecuteCallback,
        status_callback: &StatusCallback,
        action_sender: &std::sync::mpsc::Sender<ObjectActionResult>,
    ) {
        if let Some(item_info) = Self::get_item_info(item) {
            let db_type = match current_db_type.lock() {
                Ok(guard) => *guard,
                Err(poisoned) => *poisoned.into_inner(),
            };
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
                    if db_type.uses_mysql_sql_dialect() {
                        if object_type == "PROCEDURES" {
                            "Execute Procedure|Generate DDL"
                        } else {
                            "Execute Function|Generate DDL"
                        }
                    } else if object_type == "PROCEDURES" {
                        "Execute Procedure|Check Compilation|Generate DDL"
                    } else {
                        "Execute Function|Check Compilation|Generate DDL"
                    }
                }
                ObjectItem::Simple { object_type, .. } if object_type == "SEQUENCES" => {
                    "View Info|Generate DDL"
                }
                ObjectItem::Simple { object_type, .. } if object_type == "TRIGGERS" => {
                    if db_type.uses_mysql_sql_dialect() {
                        "Generate DDL"
                    } else {
                    "Check Compilation|Generate DDL"
                    }
                }
                ObjectItem::Simple { object_type, .. } if object_type == "EVENTS" => {
                    "Generate DDL"
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
                            if !conn_guard.is_connected() || !conn_guard.has_connection_handle() {
                                drop(conn_guard);
                                fltk::dialog::alert_default("Not connected to database");
                                return;
                            }
                            let db_type = conn_guard.db_type();
                            drop(conn_guard);
                            let sql = ObjectBrowserWidget::preview_select_sql(db_type, object_name);
                            ObjectBrowserWidget::emit_sql_callback(
                                sql_callback,
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

                                let db_type = conn_guard.db_type();
                                let result = match db_type.sql_dialect() {
                                    crate::db::DbSqlDialect::Oracle => {
                                        match conn_guard.require_live_connection() {
                                            Ok(db_conn) => ObjectBrowser::get_procedure_arguments(
                                                db_conn.as_ref(),
                                                &object_name,
                                            )
                                            .map(|arguments| {
                                                ObjectBrowserWidget::build_routine_script_for_db(
                                                    db_type,
                                                    &object_name,
                                                    &routine_type,
                                                    &arguments,
                                                )
                                            })
                                            .map_err(|err| err.to_string()),
                                            Err(message) => Err(message),
                                        }
                                    }
                                    crate::db::DbSqlDialect::MySql => conn_guard
                                        .get_mysql_connection_mut()
                                        .ok_or_else(|| crate::db::NOT_CONNECTED_MESSAGE.to_string())
                                        .and_then(|mysql_conn| {
                                            crate::db::query::mysql_executor::MysqlObjectBrowser::get_routine_arguments(
                                                mysql_conn,
                                                &object_name,
                                            )
                                            .map(|arguments| {
                                                ObjectBrowserWidget::build_routine_script_for_db(
                                                    db_type,
                                                    &object_name,
                                                    &routine_type,
                                                    &arguments,
                                                )
                                            })
                                            .map_err(|err| err.to_string())
                                        }),
                                };

                                let _ = sender.send(ObjectActionResult::RoutineScript {
                                    qualified_name: object_name,
                                    routine_type,
                                    db_type,
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
                                        ObjectBrowserWidget::build_routine_script_for_db(
                                            crate::db::DatabaseType::Oracle,
                                            &qualified_name,
                                            &routine_type,
                                            &arguments,
                                        )
                                    })
                                    .map_err(|err| err.to_string()),
                                    Err(message) => Err(message),
                                };

                                let _ = sender.send(ObjectActionResult::RoutineScript {
                                    qualified_name,
                                    routine_type,
                                    db_type: crate::db::DatabaseType::Oracle,
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

                                if conn_guard.db_type().uses_mysql_sql_dialect() {
                                    let _ = sender.send(ObjectActionResult::CompilationErrors {
                                        object_name,
                                        object_type,
                                        status: String::new(),
                                        result: Err("Compilation status is only supported for Oracle objects.".to_string()),
                                    });
                                    app::awake();
                                } else if let Ok(db_conn) = conn_guard.require_live_connection() {
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

                                let result = match conn_guard.db_type().sql_dialect() {
                                    crate::db::DbSqlDialect::Oracle => {
                                        match conn_guard.require_live_connection() {
                                            Ok(db_conn) => ObjectBrowser::get_table_structure(
                                                db_conn.as_ref(),
                                                &table_name,
                                            )
                                            .map_err(|err| err.to_string()),
                                            Err(message) => Err(message),
                                        }
                                    }
                                    crate::db::DbSqlDialect::MySql => conn_guard
                                        .get_mysql_connection_mut()
                                        .ok_or_else(|| crate::db::NOT_CONNECTED_MESSAGE.to_string())
                                        .and_then(|mysql_conn| {
                                            crate::db::query::mysql_executor::MysqlObjectBrowser::get_table_structure(
                                                mysql_conn,
                                                &table_name,
                                            )
                                            .map_err(|err| err.to_string())
                                        }),
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

                                let result = match conn_guard.db_type().sql_dialect() {
                                    crate::db::DbSqlDialect::Oracle => {
                                        match conn_guard.require_live_connection() {
                                            Ok(db_conn) => ObjectBrowser::get_table_indexes(
                                                db_conn.as_ref(),
                                                &table_name,
                                            )
                                            .map_err(|err| err.to_string()),
                                            Err(message) => Err(message),
                                        }
                                    }
                                    crate::db::DbSqlDialect::MySql => conn_guard
                                        .get_mysql_connection_mut()
                                        .ok_or_else(|| crate::db::NOT_CONNECTED_MESSAGE.to_string())
                                        .and_then(|mysql_conn| {
                                            crate::db::query::mysql_executor::MysqlObjectBrowser::get_index_details(
                                                mysql_conn,
                                                &table_name,
                                            )
                                            .map_err(|err| err.to_string())
                                        }),
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

                                let result = match conn_guard.db_type().sql_dialect() {
                                    crate::db::DbSqlDialect::Oracle => {
                                        match conn_guard.require_live_connection() {
                                            Ok(db_conn) => ObjectBrowser::get_table_constraints(
                                                db_conn.as_ref(),
                                                &table_name,
                                            )
                                            .map_err(|err| err.to_string()),
                                            Err(message) => Err(message),
                                        }
                                    }
                                    crate::db::DbSqlDialect::MySql => conn_guard
                                        .get_mysql_connection_mut()
                                        .ok_or_else(|| crate::db::NOT_CONNECTED_MESSAGE.to_string())
                                        .and_then(|mysql_conn| {
                                            crate::db::query::mysql_executor::MysqlObjectBrowser::get_table_constraints(
                                                mysql_conn,
                                                &table_name,
                                            )
                                            .map_err(|err| err.to_string())
                                        }),
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
                                "EVENTS" => Some("EVENT"),
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

                                    let result = match conn_guard.db_type().sql_dialect() {
                                        crate::db::DbSqlDialect::Oracle => {
                                            match conn_guard.require_live_connection() {
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
                                                Err(message) => Err(message),
                                            }
                                        }
                                        crate::db::DbSqlDialect::MySql => match object_type.as_str()
                                        {
                                            "SEQUENCE" | "SYNONYM" | "PACKAGE" => Err(format!(
                                                "{} DDL is not supported for MySQL/MariaDB connections",
                                                object_type
                                            )),
                                            _ => conn_guard
                                                .get_mysql_connection_mut()
                                                .ok_or_else(|| {
                                                    crate::db::NOT_CONNECTED_MESSAGE.to_string()
                                                })
                                                .and_then(|mysql_conn| {
                                                    crate::db::query::mysql_executor::MysqlObjectBrowser::get_create_object(
                                                        mysql_conn,
                                                        object_type.as_str(),
                                                        &object_name,
                                                    )
                                                    .map_err(|err| err.to_string())
                                                }),
                                        },
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

    fn result_column(name: &str, data_type: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: data_type.to_string(),
        }
    }

    fn build_result_tab_request(
        label: String,
        columns: Vec<ColumnInfo>,
        rows: Vec<Vec<String>>,
        message: String,
    ) -> ResultTabRequest {
        ResultTabRequest {
            label,
            result: QueryResult {
                sql: String::new(),
                columns,
                row_count: rows.len(),
                rows,
                execution_time: Duration::from_secs(0),
                message,
                is_select: true,
                success: true,
            },
        }
    }

    fn build_table_structure_result_request(
        table_name: &str,
        columns: &[TableColumnDetail],
    ) -> ResultTabRequest {
        let rows = columns
            .iter()
            .map(|column| {
                vec![
                    column.name.clone(),
                    column.get_type_display(),
                    if column.nullable {
                        "YES".to_string()
                    } else {
                        "NO".to_string()
                    },
                    if column.is_primary_key {
                        "PK".to_string()
                    } else {
                        String::new()
                    },
                ]
            })
            .collect();
        Self::build_result_tab_request(
            format!("Structure: {table_name}"),
            vec![
                Self::result_column("Column Name", "VARCHAR2"),
                Self::result_column("Data Type", "VARCHAR2"),
                Self::result_column("Nullable", "VARCHAR2"),
                Self::result_column("PK", "VARCHAR2"),
            ],
            rows,
            format!("Loaded table structure for {table_name}"),
        )
    }

    fn build_table_indexes_result_request(
        table_name: &str,
        indexes: &[IndexInfo],
    ) -> ResultTabRequest {
        let rows = indexes
            .iter()
            .map(|index| {
                vec![
                    index.name.clone(),
                    if index.is_unique {
                        "YES".to_string()
                    } else {
                        "NO".to_string()
                    },
                    index.columns.clone(),
                ]
            })
            .collect();
        Self::build_result_tab_request(
            format!("Indexes: {table_name}"),
            vec![
                Self::result_column("Index Name", "VARCHAR2"),
                Self::result_column("Unique", "VARCHAR2"),
                Self::result_column("Columns", "VARCHAR2"),
            ],
            rows,
            format!("Loaded table indexes for {table_name}"),
        )
    }

    fn build_table_constraints_result_request(
        table_name: &str,
        constraints: &[ConstraintInfo],
    ) -> ResultTabRequest {
        let rows = constraints
            .iter()
            .map(|constraint| {
                vec![
                    constraint.name.clone(),
                    constraint.constraint_type.clone(),
                    constraint.columns.clone(),
                    constraint.ref_table.clone().unwrap_or_default(),
                ]
            })
            .collect();
        Self::build_result_tab_request(
            format!("Constraints: {table_name}"),
            vec![
                Self::result_column("Constraint Name", "VARCHAR2"),
                Self::result_column("Type", "VARCHAR2"),
                Self::result_column("Columns", "VARCHAR2"),
                Self::result_column("Ref Table", "VARCHAR2"),
            ],
            rows,
            format!("Loaded table constraints for {table_name}"),
        )
    }

    fn build_sequence_info_result_request(info: &SequenceInfo) -> ResultTabRequest {
        let rows = vec![
            vec!["Name".to_string(), info.name.clone()],
            vec!["Min Value".to_string(), info.min_value.clone()],
            vec!["Max Value".to_string(), info.max_value.clone()],
            vec!["Increment By".to_string(), info.increment_by.clone()],
            vec!["Cycle".to_string(), info.cycle_flag.clone()],
            vec!["Order".to_string(), info.order_flag.clone()],
            vec!["Cache Size".to_string(), info.cache_size.clone()],
            vec!["Last Number".to_string(), info.last_number.clone()],
            vec![
                "Note".to_string(),
                "LAST_NUMBER is the next value to be generated.".to_string(),
            ],
        ];
        Self::build_result_tab_request(
            format!("Sequence: {}", info.name),
            vec![
                Self::result_column("Property", "VARCHAR2"),
                Self::result_column("Value", "VARCHAR2"),
            ],
            rows,
            format!("Loaded sequence info for {}", info.name),
        )
    }

    fn build_synonym_info_result_request(info: &SynonymInfo) -> ResultTabRequest {
        let mut rows = vec![
            vec!["Name".to_string(), info.name.clone()],
            vec!["Table Owner".to_string(), info.table_owner.clone()],
            vec!["Table Name".to_string(), info.table_name.clone()],
        ];
        if !info.db_link.is_empty() {
            rows.push(vec!["DB Link".to_string(), info.db_link.clone()]);
        }
        Self::build_result_tab_request(
            format!("Synonym: {}", info.name),
            vec![
                Self::result_column("Property", "VARCHAR2"),
                Self::result_column("Value", "VARCHAR2"),
            ],
            rows,
            format!("Loaded synonym info for {}", info.name),
        )
    }

    fn build_compilation_result_request(
        object_name: &str,
        object_type: &str,
        status: &str,
        errors: &[CompilationError],
    ) -> ResultTabRequest {
        if errors.is_empty() {
            return Self::build_result_tab_request(
                format!("Compile: {object_name}"),
                vec![
                    Self::result_column("Status", "VARCHAR2"),
                    Self::result_column("Message", "VARCHAR2"),
                ],
                vec![vec![
                    status.to_string(),
                    format!("No compilation errors found for {object_type}."),
                ]],
                format!("Loaded compilation status for {object_name}"),
            );
        }

        let rows = errors
            .iter()
            .map(|error| {
                vec![
                    error.line.to_string(),
                    error.position.to_string(),
                    error.attribute.clone(),
                    error.text.clone(),
                ]
            })
            .collect();
        Self::build_result_tab_request(
            format!("Compile: {object_name}"),
            vec![
                Self::result_column("Line", "NUMBER"),
                Self::result_column("Position", "NUMBER"),
                Self::result_column("Type", "VARCHAR2"),
                Self::result_column("Message", "VARCHAR2"),
            ],
            rows,
            format!("Loaded compilation status for {object_name} ({status})"),
        )
    }

    pub fn set_sql_callback<F>(&mut self, callback: F)
    where
        F: FnMut(SqlAction) + 'static,
    {
        *self
            .sql_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(callback));
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
            let mut slot = callback_slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            slot.take()
        };

        if let Some(mut cb) = callback {
            let call_result = panic::catch_unwind(AssertUnwindSafe(|| cb(action)));
            let mut slot = callback_slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
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
            let mut slot = callback_slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            slot.take()
        };

        if let Some(mut cb) = callback {
            let call_result = panic::catch_unwind(AssertUnwindSafe(|| cb(message)));
            let mut slot = callback_slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
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
        *self
            .status_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(callback));
    }

    /// Clear the object browser tree and cache without triggering a network refetch.
    /// Called when the database connection is closed or lost.
    pub fn clear_on_disconnect(&mut self) {
        self.clear_pending_tree_refresh();
        self.clear_items();
        self.filter_input.set_value("");
        self.owner_choice.clear();
        self.owner_choice.hide();
        *self
            .owner_list
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Vec::new();
        *self
            .selected_owner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        *self
            .object_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = ObjectCache::default();
        self.tree.redraw();
    }

    pub fn refresh(&mut self) {
        self.clear_pending_tree_refresh();
        // First clear items and filter
        self.clear_items();
        self.filter_input.set_value("");
        *self
            .object_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = ObjectCache::default();
        self.emit_status("Refreshing object browser metadata");
        let owner = self.selected_owner();
        let _ = self
            .refresh_request_sender
            .send(RefreshRequest::Metadata { owner });
    }

    pub fn selected_owner(&self) -> Option<String> {
        self.selected_owner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn set_owner_change_callback<F>(&mut self, callback: F)
    where
        F: FnMut(Option<String>) + 'static,
    {
        *self
            .owner_change_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(callback));
    }

    fn spawn_refresh_worker(
        refresh_request_receiver: Receiver<RefreshRequest>,
        refresh_sender: Sender<RefreshEvent>,
        connection: SharedConnection,
    ) {
        thread::spawn(move || {
            while let Ok(request) = Self::recv_latest_refresh_request(&refresh_request_receiver) {
                match request {
                    RefreshRequest::Metadata { owner } => {
                        if let Some((db_type, cache, selected_owner, owners)) =
                            Self::load_metadata_cache(&connection, owner.as_deref())
                        {
                            let _ = refresh_sender.send(RefreshEvent::Finished {
                                cache,
                                db_type,
                                owner: selected_owner,
                                owners,
                            });
                            app::awake();
                        }
                    }
                }
            }
        });
    }

    fn recv_latest_refresh_request(
        refresh_request_receiver: &Receiver<RefreshRequest>,
    ) -> Result<RefreshRequest, RecvError> {
        let mut latest_request = refresh_request_receiver.recv()?;
        loop {
            match refresh_request_receiver.try_recv() {
                Ok(next_request) => {
                    latest_request = next_request;
                }
                Err(TryRecvError::Empty) => return Ok(latest_request),
                Err(TryRecvError::Disconnected) => return Ok(latest_request),
            }
        }
    }

    fn load_metadata_cache(
        connection: &SharedConnection,
        requested_owner: Option<&str>,
    ) -> Option<(
        crate::db::DatabaseType,
        ObjectCache,
        Option<String>,
        Vec<String>,
    )> {
        use crate::db::query::mysql_executor::MysqlObjectBrowser;

        // Acquire connection lock and hold it during all queries.
        let mut conn_guard =
            lock_connection_with_activity(connection, "Refreshing object browser metadata");

        let db_type = conn_guard.db_type();

        match db_type.sql_dialect() {
            crate::db::DbSqlDialect::Oracle => {
                let Ok(db_conn) = conn_guard.require_live_connection() else {
                    return None;
                };

                let owners = ObjectBrowser::get_users(db_conn.as_ref()).unwrap_or_default();
                let default_owner = ObjectBrowser::get_current_owner(db_conn.as_ref())
                    .unwrap_or_else(|_| conn_guard.get_info().username.to_uppercase());
                let selected_owner = requested_owner
                    .map(|owner| owner.trim().to_uppercase())
                    .filter(|owner| !owner.is_empty())
                    .unwrap_or(default_owner);
                let owner_prefix = format!("{}.", selected_owner);
                let qualify_if_needed = |names: Vec<String>| -> Vec<String> {
                    names
                        .into_iter()
                        .map(|name| {
                            if name.contains('.') {
                                name
                            } else {
                                format!("{owner_prefix}{name}")
                            }
                        })
                        .collect()
                };

                let mut cache = ObjectCache::default();

                if let Ok(tables) = ObjectBrowser::get_tables_by_owner(db_conn.as_ref(), &selected_owner)
                {
                    cache.tables = qualify_if_needed(tables);
                }
                if let Ok(views) = ObjectBrowser::get_views_by_owner(db_conn.as_ref(), &selected_owner)
                {
                    cache.views = qualify_if_needed(views);
                }
                if let Ok(procedures) =
                    ObjectBrowser::get_procedures_by_owner(db_conn.as_ref(), &selected_owner)
                {
                    cache.procedures = qualify_if_needed(procedures);
                }
                if let Ok(functions) =
                    ObjectBrowser::get_functions_by_owner(db_conn.as_ref(), &selected_owner)
                {
                    cache.functions = qualify_if_needed(functions);
                }
                if let Ok(sequences) =
                    ObjectBrowser::get_sequences_by_owner(db_conn.as_ref(), &selected_owner)
                {
                    cache.sequences = qualify_if_needed(sequences);
                }
                if let Ok(triggers) =
                    ObjectBrowser::get_triggers_by_owner(db_conn.as_ref(), &selected_owner)
                {
                    cache.triggers = qualify_if_needed(triggers);
                }
                if let Ok(synonyms) =
                    ObjectBrowser::get_synonyms_by_owner(db_conn.as_ref(), &selected_owner)
                {
                    cache.synonyms = qualify_if_needed(synonyms);
                }
                if let Ok(packages) =
                    ObjectBrowser::get_packages_by_owner(db_conn.as_ref(), &selected_owner)
                {
                    cache.packages = qualify_if_needed(packages);
                }

                Some((db_type, cache, Some(selected_owner), owners))
            }
            crate::db::DbSqlDialect::MySql => {
                let mysql_conn = conn_guard.get_mysql_connection_mut()?;

                let mut cache = ObjectCache::default();

                if let Ok(tables) = MysqlObjectBrowser::get_tables(mysql_conn) {
                    cache.tables = tables;
                }
                if let Ok(views) = MysqlObjectBrowser::get_views(mysql_conn) {
                    cache.views = views;
                }
                if let Ok(procedures) = MysqlObjectBrowser::get_procedures(mysql_conn) {
                    cache.procedures = procedures;
                }
                if let Ok(functions) = MysqlObjectBrowser::get_functions(mysql_conn) {
                    cache.functions = functions;
                }
                if let Ok(sequences) = MysqlObjectBrowser::get_sequences(mysql_conn) {
                    cache.sequences = sequences;
                }
                if let Ok(triggers) = MysqlObjectBrowser::get_triggers(mysql_conn) {
                    cache.triggers = triggers;
                }
                if let Ok(events) = MysqlObjectBrowser::get_events(mysql_conn) {
                    cache.events = events;
                }
                // MySQL/MariaDB connections do not expose Oracle-only synonyms or packages.

                Some((db_type, cache, None, Vec::new()))
            }
        }
    }

    fn clear_items(&mut self) {
        Self::clear_tree_items(&mut self.tree);
    }

    fn clear_pending_tree_refresh(&self) -> bool {
        let mut pending = self
            .pending_tree_refresh
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let had_pending = pending.is_some();
        *pending = None;
        had_pending
    }

    fn clear_tree_items(tree: &mut Tree) {
        for category in Self::all_root_categories() {
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

    fn all_root_categories() -> &'static [&'static str] {
        &[
            "Tables",
            "Views",
            "Procedures",
            "Functions",
            "Sequences",
            "Triggers",
            "Events",
            "Synonyms",
            "Packages",
        ]
    }

    fn root_categories_for_db_type(
        db_type: crate::db::DatabaseType,
        cache: &ObjectCache,
    ) -> Vec<&'static str> {
        match db_type.sql_dialect() {
            crate::db::DbSqlDialect::Oracle => vec![
                "Tables",
                "Views",
                "Procedures",
                "Functions",
                "Sequences",
                "Triggers",
                "Synonyms",
                "Packages",
            ],
            crate::db::DbSqlDialect::MySql => {
                let mut categories = vec![
                    "Tables",
                    "Views",
                    "Procedures",
                    "Functions",
                    "Triggers",
                    "Events",
                ];
                if !cache.sequences.is_empty() {
                    categories.insert(4, "Sequences");
                }
                categories
            }
        }
    }

    fn rebuild_root_categories_for_db_type(
        tree: &mut Tree,
        db_type: crate::db::DatabaseType,
        cache: &ObjectCache,
    ) {
        for category in Self::all_root_categories() {
            if let Some(item) = tree.find_item(category) {
                let _ = tree.remove(&item);
            }
        }

        for category in Self::root_categories_for_db_type(db_type, cache) {
            tree.add(category);
            if let Some(mut item) = tree.find_item(category) {
                item.close();
            }
        }
    }

    fn populate_tree(tree: &mut Tree, cache: &ObjectCache, filter_text: &str) {
        Self::clear_tree_items(tree);
        for path in Self::collect_tree_paths(cache, filter_text) {
            tree.add(&path);
        }
    }

    fn collect_tree_paths(cache: &ObjectCache, filter_text: &str) -> Vec<String> {
        let mut paths: Vec<String> = Vec::new();
        for table in &cache.tables {
            if filter_text.is_empty() || table.to_lowercase().contains(filter_text) {
                paths.push(format!("Tables/{}", table));
            }
        }
        for view in &cache.views {
            if filter_text.is_empty() || view.to_lowercase().contains(filter_text) {
                paths.push(format!("Views/{}", view));
            }
        }
        for procedure in &cache.procedures {
            if filter_text.is_empty() || procedure.to_lowercase().contains(filter_text) {
                paths.push(format!("Procedures/{}", procedure));
            }
        }
        for func in &cache.functions {
            if filter_text.is_empty() || func.to_lowercase().contains(filter_text) {
                paths.push(format!("Functions/{}", func));
            }
        }
        for seq in &cache.sequences {
            if filter_text.is_empty() || seq.to_lowercase().contains(filter_text) {
                paths.push(format!("Sequences/{}", seq));
            }
        }
        for trig in &cache.triggers {
            if filter_text.is_empty() || trig.to_lowercase().contains(filter_text) {
                paths.push(format!("Triggers/{}", trig));
            }
        }
        for event in &cache.events {
            if filter_text.is_empty() || event.to_lowercase().contains(filter_text) {
                paths.push(format!("Events/{}", event));
            }
        }
        for syn in &cache.synonyms {
            if filter_text.is_empty() || syn.to_lowercase().contains(filter_text) {
                paths.push(format!("Synonyms/{}", syn));
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
                paths.push(format!("Packages/{}", package));
                for routine in matching_routines {
                    if routine.routine_type == "FUNCTION" {
                        paths.push(format!("Packages/{}/Functions/{}", package, routine.name));
                    } else {
                        paths.push(format!("Packages/{}/Procedures/{}", package, routine.name));
                    }
                }
            }
        }

        paths
    }

    #[allow(dead_code)]
    pub fn get_selected_item(&self) -> Option<String> {
        self.tree
            .first_selected_item()
            .and_then(|item| Self::copy_text_for_selected_item(&item))
    }

    pub fn has_focus(&self) -> bool {
        widget_has_focus(&self.flex)
    }

    pub fn copy_focused_selection_to_clipboard(&self) -> bool {
        if widget_has_focus(&self.filter_input) {
            let mut filter_input = self.filter_input.clone();
            return filter_input.copy().is_ok();
        }

        if !widget_has_focus(&self.tree) {
            return false;
        }

        let Some(item) = self.tree.first_selected_item() else {
            return false;
        };
        let Some(text) = Self::copy_text_for_selected_item(&item) else {
            return false;
        };

        app::copy(&text);
        Self::emit_status_callback(
            &self.status_callback,
            &format!("Copied '{}' to clipboard", text),
        );
        true
    }
}

impl Drop for ObjectBrowserWidget {
    fn drop(&mut self) {
        // Clones share the same underlying FLTK widgets and callback slots.
        // Only the last owner may detach handlers, otherwise dropping a
        // temporary clone can disable interactions in the live widget.
        if Arc::strong_count(&self.poll_lifecycle) != 1 {
            return;
        }

        // Release callback closures early so captured state does not outlive
        // the widget tree unnecessarily.
        self.filter_input.set_callback(|_| {});
        self.tree.handle(|_, _| false);
        *self
            .sql_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        *self
            .status_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }
}

fn widget_has_focus<W: WidgetExt>(widget: &W) -> bool {
    if let Some(focus) = app::focus() {
        return focus.as_widget_ptr() == widget.as_widget_ptr() || focus.inside(widget);
    }

    false
}

fn copy_text_for_object_item(item_info: &ObjectItem) -> String {
    match item_info {
        ObjectItem::Simple { object_name, .. } => object_name.clone(),
        ObjectItem::PackageRoutine {
            package_name,
            routine_name,
            ..
        } => format!("{}.{}", package_name, routine_name),
    }
}

#[cfg(test)]
mod tests {
    use super::{copy_text_for_object_item, ObjectBrowserWidget, ObjectItem};
    use crate::db::DatabaseType;
    use crate::db::ProcedureArgument;

    #[test]
    fn copy_text_for_package_routine_uses_qualified_name() {
        let item = ObjectItem::PackageRoutine {
            package_name: "DEMO_PKG".to_string(),
            routine_name: "RUN_JOB".to_string(),
            routine_type: "PROCEDURE".to_string(),
        };

        assert_eq!(copy_text_for_object_item(&item), "DEMO_PKG.RUN_JOB");
    }

    #[test]
    fn preview_select_sql_uses_mysql_limit_and_identifier_quotes() {
        let sql =
            ObjectBrowserWidget::preview_select_sql(crate::db::DatabaseType::MySQL, "order.items");

        assert_eq!(sql, "SELECT * FROM `order`.`items` LIMIT 100");
    }

    #[test]
    fn build_mysql_routine_script_uses_call_and_session_variables() {
        let arguments = vec![
            ProcedureArgument {
                name: Some("p_id".to_string()),
                position: 1,
                sequence: 1,
                data_type: Some("INT".to_string()),
                in_out: Some("IN".to_string()),
                data_length: None,
                data_precision: Some(10),
                data_scale: Some(0),
                type_owner: None,
                type_name: None,
                pls_type: None,
                overload: None,
                default_value: None,
            },
            ProcedureArgument {
                name: Some("p_status".to_string()),
                position: 2,
                sequence: 2,
                data_type: Some("VARCHAR(32)".to_string()),
                in_out: Some("OUT".to_string()),
                data_length: Some(32),
                data_precision: None,
                data_scale: None,
                type_owner: None,
                type_name: None,
                pls_type: None,
                overload: None,
                default_value: None,
            },
        ];

        let sql =
            ObjectBrowserWidget::build_mysql_routine_script("demo_proc", "PROCEDURE", &arguments);

        assert!(sql.contains("CALL `demo_proc`("));
        assert!(sql.contains("0,"));
        assert!(sql.contains("@v_p_status"));
        assert!(sql.contains("SELECT @v_p_status AS `p_status`;"));
        assert!(!sql.contains("FROM dual"));
        assert!(!sql.contains("BEGIN\n"));
    }

    #[test]
    fn mysql_root_categories_hide_oracle_only_groups_and_keep_events() {
        let categories = ObjectBrowserWidget::root_categories_for_db_type(
            DatabaseType::MySQL,
            &Default::default(),
        );

        assert!(categories.contains(&"Tables"));
        assert!(categories.contains(&"Views"));
        assert!(categories.contains(&"Procedures"));
        assert!(categories.contains(&"Functions"));
        assert!(categories.contains(&"Triggers"));
        assert!(categories.contains(&"Events"));
        assert!(!categories.contains(&"Synonyms"));
        assert!(!categories.contains(&"Packages"));
    }
}
