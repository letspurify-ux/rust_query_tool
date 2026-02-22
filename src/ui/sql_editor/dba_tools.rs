use fltk::{
    app,
    button::{Button, CheckButton},
    draw::set_cursor,
    enums::{Align, CallbackTrigger, Cursor, FrameType},
    frame::Frame,
    group::{Flex, FlexType},
    input::{Input, IntInput, SecretInput},
    menu::Choice,
    prelude::*,
    window::Window,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::db::{
    format_connection_busy_message, try_lock_connection_with_activity, QueryExecutor, QueryResult,
};
use crate::ui::constants::*;
use crate::ui::theme;
use crate::ui::{center_on_main, configured_ui_font_size, ResultTableWidget};

use super::SqlEditorWidget;

#[derive(Clone, Copy)]
enum StorageViewMode {
    Tablespace,
    Temp,
    Undo,
    Archive,
    Datafiles,
}

#[derive(Clone, Copy)]
enum SecurityViewMode {
    Users,
    Summary,
    RoleGrants,
    SystemGrants,
    ObjectGrants,
    Profiles,
}

impl SecurityViewMode {
    fn label(self) -> &'static str {
        match self {
            Self::Users => "Users overview",
            Self::Summary => "User detail",
            Self::RoleGrants => "Role grants",
            Self::SystemGrants => "System privileges",
            Self::ObjectGrants => "Object privileges",
            Self::Profiles => "Profile limits",
        }
    }
}

#[derive(Clone, Copy)]
enum RmanViewMode {
    Jobs,
    BackupSets,
    Coverage,
}

impl RmanViewMode {
    fn label(self) -> &'static str {
        match self {
            Self::Jobs => "RMAN jobs",
            Self::BackupSets => "RMAN backup sets",
            Self::Coverage => "Backup coverage",
        }
    }
}

#[derive(Clone, Copy)]
enum PerfViewMode {
    AshSamples,
    AshTopSql,
    AwrTopSql,
}

impl PerfViewMode {
    fn label(self) -> &'static str {
        match self {
            Self::AshSamples => "ASH samples",
            Self::AshTopSql => "ASH top SQL",
            Self::AwrTopSql => "AWR top SQL",
        }
    }
}

#[derive(Clone, Copy)]
enum DataGuardViewMode {
    Overview,
    Destinations,
    Apply,
    ArchiveGap,
}

impl DataGuardViewMode {
    fn label(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Destinations => "Destinations",
            Self::Apply => "Apply processes",
            Self::ArchiveGap => "Archive gap",
        }
    }
}

const CURSOR_PLAN_DEFAULT_FORMAT: &str =
    "ALLSTATS LAST +COST +BYTES +PREDICATE +PEEKED_BINDS +OUTLINE";
const SQL_MONITOR_AUTO_REFRESH_INTERVAL_MS: u64 = 3_000;
const SQL_MONITOR_AUTO_REFRESH_POLL_MS: u64 = 200;
const RMAN_LOOKBACK_MAX_HOURS: u32 = 24 * 30;
const ASH_LOOKBACK_MAX_MINUTES: u32 = 24 * 60;
const AWR_LOOKBACK_MAX_HOURS: u32 = 24 * 30;
const PERFORMANCE_TOP_N_MAX: u32 = 200;
static RMAN_JOB_NAME_SEQUENCE: AtomicU64 = AtomicU64::new(0);

impl SqlEditorWidget {
    pub fn show_cursor_plan_analyzer(&self) {
        enum CursorPlanMessage {
            LoadRequested {
                sql_id_text: String,
                child_text: String,
                format_text: String,
            },
            RecentRequested,
            SqlTextRequested {
                sql_id_text: String,
            },
            Loaded {
                request_id: u64,
                result: Result<QueryResult, String>,
            },
            RecentLoaded {
                request_id: u64,
                result: Result<QueryResult, String>,
            },
            SqlTextLoaded {
                request_id: u64,
                sql_id: String,
                result: Result<QueryResult, String>,
            },
            CloseRequested,
        }

        let (sender, receiver) = mpsc::channel::<CursorPlanMessage>();

        let current_group = fltk::group::Group::try_current();
        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let dialog_w = 1200;
        let dialog_h = 720;
        let mut dialog = Window::default()
            .with_size(dialog_w, dialog_h)
            .with_label("Cursor Plan Analyzer (DBMS_XPLAN.DISPLAY_CURSOR)");
        center_on_main(&mut dialog);
        dialog.set_color(theme::panel_raised());
        dialog.make_modal(true);

        let mut root = Flex::default().with_pos(0, 0).with_size(dialog_w, dialog_h);
        root.set_type(FlexType::Column);
        root.set_margin(DIALOG_MARGIN);
        root.set_spacing(DIALOG_SPACING);

        let mut help = Frame::default().with_label(
            "Loads actual cursor plans with A-Rows/Buffer Gets/Peeked Binds. Leave SQL_ID empty to inspect the latest cursor in this session.",
        );
        help.set_align(Align::Left | Align::Inside);
        help.set_label_color(theme::text_secondary());
        help.set_label_size((configured_ui_font_size().saturating_sub(1)).max(10));
        root.fixed(&help, LABEL_ROW_HEIGHT);

        let mut input_row = Flex::default();
        input_row.set_type(FlexType::Row);
        input_row.set_spacing(DIALOG_SPACING);

        let mut sql_id_label = Frame::default().with_label("SQL_ID:");
        sql_id_label.set_label_color(theme::text_primary());
        sql_id_label.set_align(Align::Inside | Align::Left);
        input_row.fixed(&sql_id_label, 50);

        let mut sql_id_input = Input::default();
        sql_id_input.set_color(theme::input_bg());
        sql_id_input.set_text_color(theme::text_primary());
        sql_id_input.set_tooltip("Optional. 13-char SQL_ID");
        input_row.fixed(&sql_id_input, 150);

        let mut child_label = Frame::default().with_label("Child#:");
        child_label.set_label_color(theme::text_primary());
        child_label.set_align(Align::Inside | Align::Left);
        input_row.fixed(&child_label, 56);

        let mut child_input = IntInput::default();
        child_input.set_value("0");
        child_input.set_color(theme::input_bg());
        child_input.set_text_color(theme::text_primary());
        child_input.set_tooltip("Optional non-negative child cursor number");
        input_row.fixed(&child_input, 80);

        let mut format_label = Frame::default().with_label("Format:");
        format_label.set_label_color(theme::text_primary());
        format_label.set_align(Align::Inside | Align::Left);
        input_row.fixed(&format_label, 56);

        let mut format_input = Input::default();
        format_input.set_value(CURSOR_PLAN_DEFAULT_FORMAT);
        format_input.set_color(theme::input_bg());
        format_input.set_text_color(theme::text_primary());
        format_input.set_tooltip("DBMS_XPLAN format options");

        let input_filler = Frame::default();
        input_row.resizable(&input_filler);
        input_row.end();
        root.fixed(&input_row, INPUT_ROW_HEIGHT);

        let mut button_row = Flex::default();
        button_row.set_type(FlexType::Row);
        button_row.set_spacing(DIALOG_SPACING);

        let mut load_btn = Button::default().with_label("Load Plan");
        load_btn.set_color(theme::button_secondary());
        load_btn.set_label_color(theme::text_primary());
        load_btn.set_frame(FrameType::RFlatBox);
        button_row.fixed(&load_btn, BUTTON_WIDTH_LARGE + 20);

        let mut recent_btn = Button::default().with_label("Recent SQL");
        recent_btn.set_color(theme::button_secondary());
        recent_btn.set_label_color(theme::text_primary());
        recent_btn.set_frame(FrameType::RFlatBox);
        button_row.fixed(&recent_btn, BUTTON_WIDTH_LARGE + 18);

        let mut sql_text_btn = Button::default().with_label("SQL Text");
        sql_text_btn.set_color(theme::button_secondary());
        sql_text_btn.set_label_color(theme::text_primary());
        sql_text_btn.set_frame(FrameType::RFlatBox);
        button_row.fixed(&sql_text_btn, BUTTON_WIDTH_LARGE + 6);

        let button_filler = Frame::default();
        button_row.resizable(&button_filler);

        let mut close_btn = Button::default().with_label("Close");
        close_btn.set_color(theme::button_subtle());
        close_btn.set_label_color(theme::text_primary());
        close_btn.set_frame(FrameType::RFlatBox);
        button_row.fixed(&close_btn, BUTTON_WIDTH);

        button_row.end();
        root.fixed(&button_row, BUTTON_ROW_HEIGHT + 4);

        let mut result_table =
            ResultTableWidget::with_size(0, 0, dialog_w - DIALOG_MARGIN * 2, dialog_h - 180);
        result_table.set_max_cell_display_chars(4000);
        let table_widget = result_table.get_widget();
        root.resizable(&table_widget);
        result_table.display_result(&dba_info_result("Enter SQL_ID and press Load Plan."));

        let mut status = Frame::default().with_label("Ready");
        status.set_label_color(theme::text_secondary());
        status.set_align(Align::Left | Align::Inside);
        root.fixed(&status, LABEL_ROW_HEIGHT);

        root.end();
        dialog.end();
        fltk::group::Group::set_current(current_group.as_ref());

        let sender_load = sender.clone();
        let sql_id_input_for_load = sql_id_input.clone();
        let child_input_for_load = child_input.clone();
        let format_input_for_load = format_input.clone();
        load_btn.set_callback(move |_| {
            let _ = sender_load.send(CursorPlanMessage::LoadRequested {
                sql_id_text: sql_id_input_for_load.value(),
                child_text: child_input_for_load.value(),
                format_text: format_input_for_load.value(),
            });
            app::awake();
        });

        let sender_recent = sender.clone();
        recent_btn.set_callback(move |_| {
            let _ = sender_recent.send(CursorPlanMessage::RecentRequested);
            app::awake();
        });

        let sender_sql_text = sender.clone();
        let sql_id_input_for_sql_text = sql_id_input.clone();
        sql_text_btn.set_callback(move |_| {
            let _ = sender_sql_text.send(CursorPlanMessage::SqlTextRequested {
                sql_id_text: sql_id_input_for_sql_text.value(),
            });
            app::awake();
        });

        let sender_close = sender.clone();
        close_btn.set_callback(move |_| {
            let _ = sender_close.send(CursorPlanMessage::CloseRequested);
            app::awake();
        });

        dialog.show();
        let _ = dialog.take_focus();
        let _ = load_btn.take_focus();

        let mut latest_request_id = 0u64;
        let mut latest_recent_request_id = 0u64;
        let mut latest_sql_text_request_id = 0u64;
        let mut last_table_selection = (i32::MIN, i32::MIN, i32::MIN, i32::MIN);
        let mut last_snapshot_columns: Vec<String> = Vec::new();

        while dialog.shown() {
            app::wait();

            while let Ok(message) = receiver.try_recv() {
                match message {
                    CursorPlanMessage::LoadRequested {
                        sql_id_text,
                        child_text,
                        format_text,
                    } => {
                        let sql_id = match normalize_optional_sql_id(&sql_id_text) {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        let child_number =
                            match parse_optional_non_negative_i32(&child_text, "Child#") {
                                Ok(value) => value,
                                Err(err) => {
                                    fltk::dialog::alert_default(&err);
                                    continue;
                                }
                            };

                        let effective_child = if sql_id.is_some() { child_number } else { None };
                        let format_option = if format_text.trim().is_empty() {
                            CURSOR_PLAN_DEFAULT_FORMAT.to_string()
                        } else {
                            format_text.trim().to_string()
                        };

                        latest_request_id = latest_request_id.saturating_add(1);
                        let request_id = latest_request_id;

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label("Loading cursor plan...");
                        result_table.display_result(&dba_info_result("Loading cursor plan..."));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                "Loading cursor execution plan",
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::get_cursor_plan_snapshot(
                                        db_conn.as_ref(),
                                        sql_id.as_deref(),
                                        effective_child,
                                        &format_option,
                                    )
                                    .map_err(|err| format!("Failed to load cursor plan. {err}")),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result
                                .send(CursorPlanMessage::Loaded { request_id, result });
                            app::awake();
                        });
                    }
                    CursorPlanMessage::RecentRequested => {
                        latest_recent_request_id = latest_recent_request_id.saturating_add(1);
                        let request_id = latest_recent_request_id;

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label("Loading recent SQL candidates...");
                        result_table
                            .display_result(&dba_info_result("Loading recent SQL candidates..."));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                "Loading recent SQL candidates",
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::get_recent_sql_cursor_candidates(
                                        db_conn.as_ref(),
                                        200,
                                    )
                                    .map_err(|err| {
                                        format!("Failed to load recent SQL candidates: {err}")
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result
                                .send(CursorPlanMessage::RecentLoaded { request_id, result });
                            app::awake();
                        });
                    }
                    CursorPlanMessage::SqlTextRequested { sql_id_text } => {
                        let sql_id = match normalize_optional_sql_id(&sql_id_text) {
                            Ok(Some(value)) => value,
                            Ok(None) => {
                                fltk::dialog::alert_default(
                                    "SQL_ID is required for SQL Text lookup.",
                                );
                                continue;
                            }
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        latest_sql_text_request_id = latest_sql_text_request_id.saturating_add(1);
                        let request_id = latest_sql_text_request_id;

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Loading SQL text for {}...", sql_id));
                        result_table.display_result(&dba_info_result("Loading SQL text..."));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Loading SQL text for {}", sql_id),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::get_sql_text_by_sql_id(
                                        db_conn.as_ref(),
                                        &sql_id,
                                    )
                                    .map_err(|err| {
                                        format!("Failed to load SQL text for {}: {err}", sql_id)
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(CursorPlanMessage::SqlTextLoaded {
                                request_id,
                                sql_id,
                                result,
                            });
                            app::awake();
                        });
                    }
                    CursorPlanMessage::Loaded { request_id, result } => {
                        if request_id != latest_request_id {
                            continue;
                        }
                        set_cursor(Cursor::Default);
                        app::flush();

                        match result {
                            Ok(snapshot) => {
                                last_snapshot_columns = snapshot
                                    .columns
                                    .iter()
                                    .map(|column| column.name.clone())
                                    .collect();
                                result_table.display_result(&snapshot);
                                status.set_label(&format!(
                                    "Loaded {} lines in {} ms",
                                    snapshot.row_count,
                                    snapshot.execution_time.as_millis()
                                ));
                            }
                            Err(err) => {
                                result_table.display_result(&dba_info_result(&format!(
                                    "Cursor plan load failed. {}\nTip: Run the SQL first, then query DISPLAY_CURSOR. DBA privileges may be required.",
                                    err
                                )));
                                status.set_label("Cursor plan load failed");
                            }
                        }
                    }
                    CursorPlanMessage::RecentLoaded { request_id, result } => {
                        if request_id != latest_recent_request_id {
                            continue;
                        }
                        set_cursor(Cursor::Default);
                        app::flush();

                        match result {
                            Ok(snapshot) => {
                                last_snapshot_columns = snapshot
                                    .columns
                                    .iter()
                                    .map(|column| column.name.clone())
                                    .collect();
                                result_table.display_result(&snapshot);
                                last_table_selection = (i32::MIN, i32::MIN, i32::MIN, i32::MIN);
                                status.set_label(&format!(
                                    "Loaded {} recent cursor rows in {} ms",
                                    snapshot.row_count,
                                    snapshot.execution_time.as_millis()
                                ));
                            }
                            Err(err) => {
                                result_table.display_result(&dba_info_result(&format!(
                                    "Recent SQL load failed. {}\nTip: V$SQL access may require additional privileges.",
                                    err
                                )));
                                status.set_label("Recent SQL load failed");
                            }
                        }
                    }
                    CursorPlanMessage::SqlTextLoaded {
                        request_id,
                        sql_id,
                        result,
                    } => {
                        if request_id != latest_sql_text_request_id {
                            continue;
                        }

                        set_cursor(Cursor::Default);
                        app::flush();

                        match result {
                            Ok(snapshot) => {
                                last_snapshot_columns = snapshot
                                    .columns
                                    .iter()
                                    .map(|column| column.name.clone())
                                    .collect();
                                result_table.display_result(&snapshot);
                                status.set_label(&format!(
                                    "Loaded SQL text for {} ({} row(s), {} ms)",
                                    sql_id,
                                    snapshot.row_count,
                                    snapshot.execution_time.as_millis()
                                ));
                            }
                            Err(err) => {
                                result_table.display_result(&dba_info_result(&format!(
                                    "SQL text load failed. {}",
                                    err
                                )));
                                status.set_label("SQL text load failed");
                            }
                        }
                    }
                    CursorPlanMessage::CloseRequested => {
                        dialog.hide();
                    }
                }
            }

            let selection = table_widget.get_selection();
            if selection != last_table_selection {
                last_table_selection = selection;
                let selected_row = selection.0.min(selection.2);
                if selected_row >= 0 {
                    let selected_index = selected_row as usize;
                    if let Some(row) = result_table.row_values(selected_index) {
                        if let Some((sql_id, child)) =
                            parse_sql_id_child_row(&row, &last_snapshot_columns)
                        {
                            sql_id_input.set_value(&sql_id);
                            child_input.set_value(&child.to_string());
                        }
                    }
                }
            }
        }

        set_cursor(Cursor::Default);
        app::flush();
        Window::delete(dialog);
    }

    pub fn show_sql_monitor_dashboard(&self) {
        enum SqlMonitorMessage {
            RefreshRequested {
                min_elapsed_text: String,
                active_only: bool,
                sql_id_text: String,
                username_text: String,
                from_auto: bool,
            },
            KillSessionRequested,
            AutoTick,
            SnapshotLoaded {
                request_id: u64,
                min_elapsed_seconds: u32,
                active_only: bool,
                sql_id_filter: Option<String>,
                username_filter: Option<String>,
                result: Result<QueryResult, String>,
            },
            ActionFinished(Result<String, String>),
            CloseRequested,
        }

        let (sender, receiver) = mpsc::channel::<SqlMonitorMessage>();

        let current_group = fltk::group::Group::try_current();
        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let dialog_w = 1280;
        let dialog_h = 760;
        let mut dialog = Window::default()
            .with_size(dialog_w, dialog_h)
            .with_label("SQL Monitor Dashboard (GV$SQL_MONITOR)");
        center_on_main(&mut dialog);
        dialog.set_color(theme::panel_raised());
        dialog.make_modal(true);

        let mut root = Flex::default().with_pos(0, 0).with_size(dialog_w, dialog_h);
        root.set_type(FlexType::Column);
        root.set_margin(DIALOG_MARGIN);
        root.set_spacing(DIALOG_SPACING);

        let mut help = Frame::default().with_label(
            "Tracks long-running SQL in near real-time (RAC aware via GV$SQL_MONITOR). Auto-refresh interval is 3 seconds.",
        );
        help.set_align(Align::Left | Align::Inside);
        help.set_label_color(theme::text_secondary());
        help.set_label_size((configured_ui_font_size().saturating_sub(1)).max(10));
        root.fixed(&help, LABEL_ROW_HEIGHT);

        let mut control_row = Flex::default();
        control_row.set_type(FlexType::Row);
        control_row.set_spacing(DIALOG_SPACING);

        let mut min_label = Frame::default().with_label("Min Elapsed(s):");
        min_label.set_label_color(theme::text_primary());
        min_label.set_align(Align::Inside | Align::Left);
        control_row.fixed(&min_label, 110);

        let mut min_elapsed_input = IntInput::default();
        min_elapsed_input.set_value("5");
        min_elapsed_input.set_color(theme::input_bg());
        min_elapsed_input.set_text_color(theme::text_primary());
        control_row.fixed(&min_elapsed_input, 72);

        let mut sql_id_label = Frame::default().with_label("SQL_ID:");
        sql_id_label.set_label_color(theme::text_primary());
        sql_id_label.set_align(Align::Inside | Align::Left);
        control_row.fixed(&sql_id_label, 52);

        let mut sql_id_input = Input::default();
        sql_id_input.set_color(theme::input_bg());
        sql_id_input.set_text_color(theme::text_primary());
        sql_id_input.set_tooltip("Optional SQL_ID filter");
        control_row.fixed(&sql_id_input, 120);

        let mut user_label = Frame::default().with_label("User:");
        user_label.set_label_color(theme::text_primary());
        user_label.set_align(Align::Inside | Align::Left);
        control_row.fixed(&user_label, 42);

        let mut user_input = Input::default();
        user_input.set_color(theme::input_bg());
        user_input.set_text_color(theme::text_primary());
        user_input.set_tooltip("Optional parsing username filter");
        control_row.fixed(&user_input, 96);

        let mut active_only_check = CheckButton::default().with_label("Active only");
        active_only_check.set_value(true);
        active_only_check.set_label_color(theme::text_primary());
        control_row.fixed(&active_only_check, 110);

        let mut auto_refresh_check = CheckButton::default().with_label("Auto refresh (3s)");
        auto_refresh_check.set_label_color(theme::text_primary());
        control_row.fixed(&auto_refresh_check, 150);

        let mut refresh_btn = Button::default().with_label("Refresh");
        refresh_btn.set_color(theme::button_secondary());
        refresh_btn.set_label_color(theme::text_primary());
        refresh_btn.set_frame(FrameType::RFlatBox);
        control_row.fixed(&refresh_btn, BUTTON_WIDTH_LARGE);

        let mut kill_session_btn = Button::default().with_label("Kill Session");
        kill_session_btn.set_color(theme::button_warning());
        kill_session_btn.set_label_color(theme::text_primary());
        kill_session_btn.set_frame(FrameType::RFlatBox);
        kill_session_btn.set_tooltip("Kill selected INST_ID/SID/SERIAL# immediately");
        control_row.fixed(&kill_session_btn, BUTTON_WIDTH_LARGE + 20);

        let control_filler = Frame::default();
        control_row.resizable(&control_filler);

        let mut close_btn = Button::default().with_label("Close");
        close_btn.set_color(theme::button_subtle());
        close_btn.set_label_color(theme::text_primary());
        close_btn.set_frame(FrameType::RFlatBox);
        control_row.fixed(&close_btn, BUTTON_WIDTH);

        control_row.end();
        root.fixed(&control_row, BUTTON_ROW_HEIGHT + 4);

        let mut result_table =
            ResultTableWidget::with_size(0, 0, dialog_w - DIALOG_MARGIN * 2, dialog_h - 190);
        result_table.set_max_cell_display_chars(420);
        let table_widget = result_table.get_widget();
        root.resizable(&table_widget);
        result_table.display_result(&dba_info_result("Press Refresh to load SQL monitor rows."));

        let mut status = Frame::default().with_label("Ready");
        status.set_label_color(theme::text_secondary());
        status.set_align(Align::Left | Align::Inside);
        root.fixed(&status, LABEL_ROW_HEIGHT);

        root.end();
        dialog.end();
        fltk::group::Group::set_current(current_group.as_ref());

        let sender_refresh = sender.clone();
        let min_elapsed_input_for_refresh = min_elapsed_input.clone();
        let active_only_check_for_refresh = active_only_check.clone();
        let sql_id_input_for_refresh = sql_id_input.clone();
        let user_input_for_refresh = user_input.clone();
        refresh_btn.set_callback(move |_| {
            let _ = sender_refresh.send(SqlMonitorMessage::RefreshRequested {
                min_elapsed_text: min_elapsed_input_for_refresh.value(),
                active_only: active_only_check_for_refresh.value(),
                sql_id_text: sql_id_input_for_refresh.value(),
                username_text: user_input_for_refresh.value(),
                from_auto: false,
            });
            app::awake();
        });

        let sender_kill = sender.clone();
        kill_session_btn.set_callback(move |_| {
            let _ = sender_kill.send(SqlMonitorMessage::KillSessionRequested);
            app::awake();
        });

        let auto_refresh_enabled = Arc::new(AtomicBool::new(false));
        let auto_refresh_enabled_for_toggle = Arc::clone(&auto_refresh_enabled);
        auto_refresh_check.set_callback(move |check| {
            auto_refresh_enabled_for_toggle.store(check.value(), Ordering::SeqCst);
        });

        let stop_auto_signal = Arc::new(AtomicBool::new(false));
        let stop_auto_signal_for_thread = Arc::clone(&stop_auto_signal);
        let auto_refresh_enabled_for_thread = Arc::clone(&auto_refresh_enabled);
        let sender_tick = sender.clone();
        let auto_thread = thread::spawn(move || {
            let polls_per_refresh =
                ((SQL_MONITOR_AUTO_REFRESH_INTERVAL_MS + SQL_MONITOR_AUTO_REFRESH_POLL_MS - 1)
                    / SQL_MONITOR_AUTO_REFRESH_POLL_MS)
                    .max(1);
            let mut poll_count = 0u64;
            while !stop_auto_signal_for_thread.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(SQL_MONITOR_AUTO_REFRESH_POLL_MS));
                if stop_auto_signal_for_thread.load(Ordering::SeqCst) {
                    break;
                }
                if auto_refresh_enabled_for_thread.load(Ordering::SeqCst) {
                    poll_count = poll_count.saturating_add(1);
                    if poll_count >= polls_per_refresh {
                        poll_count = 0;
                        let _ = sender_tick.send(SqlMonitorMessage::AutoTick);
                        app::awake();
                    }
                } else {
                    poll_count = 0;
                }
            }
        });

        let sender_close = sender.clone();
        close_btn.set_callback(move |_| {
            let _ = sender_close.send(SqlMonitorMessage::CloseRequested);
            app::awake();
        });

        dialog.show();
        let _ = dialog.take_focus();
        let _ = refresh_btn.take_focus();

        let _ = sender.send(SqlMonitorMessage::RefreshRequested {
            min_elapsed_text: min_elapsed_input.value(),
            active_only: active_only_check.value(),
            sql_id_text: sql_id_input.value(),
            username_text: user_input.value(),
            from_auto: false,
        });
        app::awake();

        let mut latest_request_id = 0u64;
        let mut refresh_in_flight = false;
        let mut pending_refresh_request: Option<(String, bool, String, String)> = None;
        let mut last_table_selection = (i32::MIN, i32::MIN, i32::MIN, i32::MIN);
        let mut last_snapshot_columns: Vec<String> = Vec::new();

        while dialog.shown() {
            app::wait();

            while let Ok(message) = receiver.try_recv() {
                match message {
                    SqlMonitorMessage::RefreshRequested {
                        min_elapsed_text,
                        active_only,
                        sql_id_text,
                        username_text,
                        from_auto,
                    } => {
                        if refresh_in_flight {
                            if from_auto {
                                continue;
                            }
                            pending_refresh_request =
                                Some((min_elapsed_text, active_only, sql_id_text, username_text));
                            status.set_label(
                                "SQL monitor refresh queued (will run after current load)",
                            );
                            continue;
                        }

                        let min_elapsed_seconds =
                            match parse_positive_u32(&min_elapsed_text, "Min elapsed seconds") {
                                Ok(value) => value,
                                Err(err) => {
                                    if from_auto {
                                        status.set_label(&format!("Auto refresh skipped: {}", err));
                                    } else {
                                        fltk::dialog::alert_default(&err);
                                    }
                                    continue;
                                }
                            };
                        let sql_id_filter = match normalize_optional_sql_id(&sql_id_text) {
                            Ok(value) => value,
                            Err(err) => {
                                if from_auto {
                                    status.set_label(&format!("Auto refresh skipped: {}", err));
                                } else {
                                    fltk::dialog::alert_default(&err);
                                }
                                continue;
                            }
                        };
                        let username_filter =
                            match normalize_optional_identifier(&username_text, "User") {
                                Ok(value) => value,
                                Err(err) => {
                                    if from_auto {
                                        status.set_label(&format!("Auto refresh skipped: {}", err));
                                    } else {
                                        fltk::dialog::alert_default(&err);
                                    }
                                    continue;
                                }
                            };

                        latest_request_id = latest_request_id.saturating_add(1);
                        let request_id = latest_request_id;
                        refresh_in_flight = true;

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label("Loading SQL monitor snapshot...");
                        result_table
                            .display_result(&dba_info_result("Loading SQL monitor snapshot..."));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                "Loading SQL monitor dashboard",
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::get_sql_monitor_snapshot(
                                        db_conn.as_ref(),
                                        min_elapsed_seconds,
                                        active_only,
                                        sql_id_filter.as_deref(),
                                        username_filter.as_deref(),
                                    )
                                    .map_err(|err| {
                                        format!("Failed to load SQL monitor snapshot: {err}")
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(SqlMonitorMessage::SnapshotLoaded {
                                request_id,
                                min_elapsed_seconds,
                                active_only,
                                sql_id_filter,
                                username_filter,
                                result,
                            });
                            app::awake();
                        });
                    }
                    SqlMonitorMessage::AutoTick => {
                        let _ = sender.send(SqlMonitorMessage::RefreshRequested {
                            min_elapsed_text: min_elapsed_input.value(),
                            active_only: active_only_check.value(),
                            sql_id_text: sql_id_input.value(),
                            username_text: user_input.value(),
                            from_auto: true,
                        });
                        app::awake();
                    }
                    SqlMonitorMessage::KillSessionRequested => {
                        let selected_row = current_selected_row_index(table_widget.get_selection());
                        let Some(row_index) = selected_row else {
                            fltk::dialog::alert_default(
                                "Select a SQL Monitor row first (SID/SERIAL# required).",
                            );
                            continue;
                        };

                        let Some(row) = result_table.row_values(row_index) else {
                            fltk::dialog::alert_default("Failed to read selected SQL Monitor row.");
                            continue;
                        };

                        let Some((instance_id, sid, serial)) =
                            parse_sql_monitor_session_target(&row, &last_snapshot_columns)
                        else {
                            fltk::dialog::alert_default(
                                "Selected row does not contain valid INST_ID/SID/SERIAL# values.",
                            );
                            continue;
                        };

                        let target_label =
                            sql_monitor_session_target_label(instance_id, sid, serial);

                        let confirm = fltk::dialog::choice2_default(
                            &format!(
                                "Kill session '{}' immediately? (ALTER SYSTEM KILL SESSION)",
                                target_label
                            ),
                            "Cancel",
                            "Kill",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Killing session '{}'...", target_label));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Killing session {}", target_label),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::kill_session_on_instance(
                                        db_conn.as_ref(),
                                        sid,
                                        serial,
                                        instance_id,
                                        true,
                                    )
                                    .map(|_| format!("Killed session '{}'", target_label))
                                    .map_err(|err| {
                                        format!("Failed to kill session '{}': {err}", target_label)
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(SqlMonitorMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    SqlMonitorMessage::SnapshotLoaded {
                        request_id,
                        min_elapsed_seconds,
                        active_only,
                        sql_id_filter,
                        username_filter,
                        result,
                    } => {
                        if request_id != latest_request_id {
                            continue;
                        }
                        refresh_in_flight = false;

                        set_cursor(Cursor::Default);
                        app::flush();

                        match result {
                            Ok(snapshot) => {
                                last_snapshot_columns = snapshot
                                    .columns
                                    .iter()
                                    .map(|column| column.name.to_uppercase())
                                    .collect();
                                result_table.display_result(&snapshot);
                                status.set_label(&format!(
                                    "Loaded {} rows in {} ms (min={}s, active_only={}, sql_id={}, user={})",
                                    snapshot.row_count,
                                    snapshot.execution_time.as_millis(),
                                    min_elapsed_seconds,
                                    active_only,
                                    sql_id_filter.as_deref().unwrap_or("-"),
                                    username_filter.as_deref().unwrap_or("-")
                                ));
                            }
                            Err(err) => {
                                last_snapshot_columns.clear();
                                result_table.display_result(&dba_info_result(&format!(
                                    "SQL monitor snapshot failed. {}\nTip: GV$SQL_MONITOR access may require DBA/Tuning Pack privileges.",
                                    err
                                )));
                                status.set_label("SQL monitor load failed");
                            }
                        }

                        if let Some((
                            queued_min_elapsed_text,
                            queued_active_only,
                            queued_sql_id_text,
                            queued_username_text,
                        )) = pending_refresh_request.take()
                        {
                            if dialog.shown() {
                                let _ = sender.send(SqlMonitorMessage::RefreshRequested {
                                    min_elapsed_text: queued_min_elapsed_text,
                                    active_only: queued_active_only,
                                    sql_id_text: queued_sql_id_text,
                                    username_text: queued_username_text,
                                    from_auto: false,
                                });
                                app::awake();
                            }
                        }
                    }
                    SqlMonitorMessage::ActionFinished(result) => {
                        set_cursor(Cursor::Default);
                        app::flush();

                        match result {
                            Ok(message) => {
                                status.set_label(&message);
                                let _ = sender.send(SqlMonitorMessage::RefreshRequested {
                                    min_elapsed_text: min_elapsed_input.value(),
                                    active_only: active_only_check.value(),
                                    sql_id_text: sql_id_input.value(),
                                    username_text: user_input.value(),
                                    from_auto: false,
                                });
                                app::awake();
                            }
                            Err(err) => {
                                status.set_label("SQL monitor action failed");
                                fltk::dialog::alert_default(&err);
                            }
                        }
                    }
                    SqlMonitorMessage::CloseRequested => {
                        pending_refresh_request = None;
                        dialog.hide();
                    }
                }
            }

            let selection = table_widget.get_selection();
            if selection != last_table_selection {
                last_table_selection = selection;
                let selected_row = selection.0.min(selection.2);
                if selected_row >= 0 {
                    let selected_index = selected_row as usize;
                    if let Some(row) = result_table.row_values(selected_index) {
                        if let Some(sql_id) =
                            column_value_by_name(&row, &last_snapshot_columns, "SQL_ID")
                        {
                            let normalized = sql_id.trim().to_uppercase();
                            if normalize_optional_sql_id(&normalized)
                                .ok()
                                .flatten()
                                .is_some()
                            {
                                sql_id_input.set_value(&normalized);
                            }
                        }
                        if let Some(user_name) =
                            column_value_by_name(&row, &last_snapshot_columns, "USERNAME")
                        {
                            let normalized = user_name.trim().to_uppercase();
                            if normalize_optional_identifier(&normalized, "User")
                                .ok()
                                .flatten()
                                .is_some()
                            {
                                user_input.set_value(&normalized);
                            }
                        }
                    }
                }
            }
        }

        stop_auto_signal.store(true, Ordering::SeqCst);
        let _ = auto_thread.join();

        set_cursor(Cursor::Default);
        app::flush();
        Window::delete(dialog);
    }

    pub fn show_storage_dashboard(&self) {
        enum StorageMessage {
            LoadRequested {
                mode: StorageViewMode,
                warn_text: String,
                critical_text: String,
                alerts_only: bool,
            },
            SnapshotLoaded {
                request_id: u64,
                mode: StorageViewMode,
                alerts_only: bool,
                result: Result<QueryResult, String>,
            },
            CloseRequested,
        }

        let (sender, receiver) = mpsc::channel::<StorageMessage>();

        let current_group = fltk::group::Group::try_current();
        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let dialog_w = 1180;
        let dialog_h = 720;
        let mut dialog = Window::default()
            .with_size(dialog_w, dialog_h)
            .with_label("Storage Dashboard (Tablespace/UNDO/TEMP/Archive/Datafiles)");
        center_on_main(&mut dialog);
        dialog.set_color(theme::panel_raised());
        dialog.make_modal(true);

        let mut root = Flex::default().with_pos(0, 0).with_size(dialog_w, dialog_h);
        root.set_type(FlexType::Column);
        root.set_margin(DIALOG_MARGIN);
        root.set_spacing(DIALOG_SPACING);

        let mut help = Frame::default().with_label(
            "Shows storage usage and alert levels. Warning/Critical thresholds are percentage-based.",
        );
        help.set_align(Align::Left | Align::Inside);
        help.set_label_color(theme::text_secondary());
        help.set_label_size((configured_ui_font_size().saturating_sub(1)).max(10));
        root.fixed(&help, LABEL_ROW_HEIGHT);

        let mut controls = Flex::default();
        controls.set_type(FlexType::Row);
        controls.set_spacing(DIALOG_SPACING);

        let mut warn_label = Frame::default().with_label("Warn%:");
        warn_label.set_label_color(theme::text_primary());
        warn_label.set_align(Align::Inside | Align::Left);
        controls.fixed(&warn_label, 58);

        let mut warn_input = IntInput::default();
        warn_input.set_value("80");
        warn_input.set_color(theme::input_bg());
        warn_input.set_text_color(theme::text_primary());
        controls.fixed(&warn_input, 62);

        let mut critical_label = Frame::default().with_label("Critical%:");
        critical_label.set_label_color(theme::text_primary());
        critical_label.set_align(Align::Inside | Align::Left);
        controls.fixed(&critical_label, 72);

        let mut critical_input = IntInput::default();
        critical_input.set_value("90");
        critical_input.set_color(theme::input_bg());
        critical_input.set_text_color(theme::text_primary());
        controls.fixed(&critical_input, 62);

        let mut alerts_only_check = CheckButton::default().with_label("Alerts only");
        alerts_only_check.set_label_color(theme::text_primary());
        alerts_only_check.set_tooltip("Show only WARN/CRITICAL rows");
        controls.fixed(&alerts_only_check, 108);

        let mut tablespace_btn = Button::default().with_label("Tablespaces");
        tablespace_btn.set_color(theme::button_secondary());
        tablespace_btn.set_label_color(theme::text_primary());
        tablespace_btn.set_frame(FrameType::RFlatBox);
        controls.fixed(&tablespace_btn, BUTTON_WIDTH_LARGE + 20);

        let mut temp_btn = Button::default().with_label("TEMP");
        temp_btn.set_color(theme::button_secondary());
        temp_btn.set_label_color(theme::text_primary());
        temp_btn.set_frame(FrameType::RFlatBox);
        controls.fixed(&temp_btn, BUTTON_WIDTH_LARGE);

        let mut undo_btn = Button::default().with_label("UNDO");
        undo_btn.set_color(theme::button_secondary());
        undo_btn.set_label_color(theme::text_primary());
        undo_btn.set_frame(FrameType::RFlatBox);
        controls.fixed(&undo_btn, BUTTON_WIDTH_LARGE);

        let mut archive_btn = Button::default().with_label("Archive/FRA");
        archive_btn.set_color(theme::button_secondary());
        archive_btn.set_label_color(theme::text_primary());
        archive_btn.set_frame(FrameType::RFlatBox);
        controls.fixed(&archive_btn, BUTTON_WIDTH_LARGE + 12);

        let mut datafiles_btn = Button::default().with_label("Datafiles");
        datafiles_btn.set_color(theme::button_secondary());
        datafiles_btn.set_label_color(theme::text_primary());
        datafiles_btn.set_frame(FrameType::RFlatBox);
        controls.fixed(&datafiles_btn, BUTTON_WIDTH_LARGE + 8);

        let controls_filler = Frame::default();
        controls.resizable(&controls_filler);

        let mut close_btn = Button::default().with_label("Close");
        close_btn.set_color(theme::button_subtle());
        close_btn.set_label_color(theme::text_primary());
        close_btn.set_frame(FrameType::RFlatBox);
        controls.fixed(&close_btn, BUTTON_WIDTH);

        controls.end();
        root.fixed(&controls, BUTTON_ROW_HEIGHT + 4);

        let mut result_table =
            ResultTableWidget::with_size(0, 0, dialog_w - DIALOG_MARGIN * 2, dialog_h - 180);
        result_table.set_max_cell_display_chars(320);
        let table_widget = result_table.get_widget();
        root.resizable(&table_widget);
        result_table.display_result(&dba_info_result("Select a storage view to load metrics."));

        let mut status = Frame::default().with_label("Ready");
        status.set_label_color(theme::text_secondary());
        status.set_align(Align::Left | Align::Inside);
        root.fixed(&status, LABEL_ROW_HEIGHT);

        root.end();
        dialog.end();
        fltk::group::Group::set_current(current_group.as_ref());

        let sender_tablespace = sender.clone();
        let warn_input_for_ts = warn_input.clone();
        let critical_input_for_ts = critical_input.clone();
        let alerts_only_for_ts = alerts_only_check.clone();
        tablespace_btn.set_callback(move |_| {
            let _ = sender_tablespace.send(StorageMessage::LoadRequested {
                mode: StorageViewMode::Tablespace,
                warn_text: warn_input_for_ts.value(),
                critical_text: critical_input_for_ts.value(),
                alerts_only: alerts_only_for_ts.value(),
            });
            app::awake();
        });

        let sender_temp = sender.clone();
        let warn_input_for_temp = warn_input.clone();
        let critical_input_for_temp = critical_input.clone();
        let alerts_only_for_temp = alerts_only_check.clone();
        temp_btn.set_callback(move |_| {
            let _ = sender_temp.send(StorageMessage::LoadRequested {
                mode: StorageViewMode::Temp,
                warn_text: warn_input_for_temp.value(),
                critical_text: critical_input_for_temp.value(),
                alerts_only: alerts_only_for_temp.value(),
            });
            app::awake();
        });

        let sender_undo = sender.clone();
        let warn_input_for_undo = warn_input.clone();
        let critical_input_for_undo = critical_input.clone();
        let alerts_only_for_undo = alerts_only_check.clone();
        undo_btn.set_callback(move |_| {
            let _ = sender_undo.send(StorageMessage::LoadRequested {
                mode: StorageViewMode::Undo,
                warn_text: warn_input_for_undo.value(),
                critical_text: critical_input_for_undo.value(),
                alerts_only: alerts_only_for_undo.value(),
            });
            app::awake();
        });

        let sender_archive = sender.clone();
        let warn_input_for_archive = warn_input.clone();
        let critical_input_for_archive = critical_input.clone();
        let alerts_only_for_archive = alerts_only_check.clone();
        archive_btn.set_callback(move |_| {
            let _ = sender_archive.send(StorageMessage::LoadRequested {
                mode: StorageViewMode::Archive,
                warn_text: warn_input_for_archive.value(),
                critical_text: critical_input_for_archive.value(),
                alerts_only: alerts_only_for_archive.value(),
            });
            app::awake();
        });

        let sender_datafiles = sender.clone();
        let warn_input_for_datafiles = warn_input.clone();
        let critical_input_for_datafiles = critical_input.clone();
        let alerts_only_for_datafiles = alerts_only_check.clone();
        datafiles_btn.set_callback(move |_| {
            let _ = sender_datafiles.send(StorageMessage::LoadRequested {
                mode: StorageViewMode::Datafiles,
                warn_text: warn_input_for_datafiles.value(),
                critical_text: critical_input_for_datafiles.value(),
                alerts_only: alerts_only_for_datafiles.value(),
            });
            app::awake();
        });

        let sender_close = sender.clone();
        close_btn.set_callback(move |_| {
            let _ = sender_close.send(StorageMessage::CloseRequested);
            app::awake();
        });

        dialog.show();
        let _ = dialog.take_focus();
        let _ = tablespace_btn.take_focus();

        let _ = sender.send(StorageMessage::LoadRequested {
            mode: StorageViewMode::Tablespace,
            warn_text: warn_input.value(),
            critical_text: critical_input.value(),
            alerts_only: alerts_only_check.value(),
        });
        app::awake();

        let mut latest_request_id = 0u64;

        while dialog.shown() {
            app::wait();

            while let Ok(message) = receiver.try_recv() {
                match message {
                    StorageMessage::LoadRequested {
                        mode,
                        warn_text,
                        critical_text,
                        alerts_only,
                    } => {
                        let (warn_pct, critical_pct) =
                            match parse_percentage_thresholds(&warn_text, &critical_text) {
                                Ok(value) => value,
                                Err(err) => {
                                    fltk::dialog::alert_default(&err);
                                    continue;
                                }
                            };

                        latest_request_id = latest_request_id.saturating_add(1);
                        let request_id = latest_request_id;

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status
                            .set_label(&format!("Loading {} metrics...", storage_mode_label(mode)));
                        result_table.display_result(&dba_info_result("Loading storage metrics..."));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Loading {} usage snapshot", storage_mode_label(mode)),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => {
                                        let query_result = match mode {
                                            StorageViewMode::Tablespace => {
                                                QueryExecutor::get_tablespace_usage_snapshot(
                                                    db_conn.as_ref(),
                                                    warn_pct,
                                                    critical_pct,
                                                )
                                            }
                                            StorageViewMode::Temp => {
                                                QueryExecutor::get_temp_usage_snapshot(
                                                    db_conn.as_ref(),
                                                    warn_pct,
                                                    critical_pct,
                                                )
                                            }
                                            StorageViewMode::Undo => {
                                                QueryExecutor::get_undo_usage_snapshot(
                                                    db_conn.as_ref(),
                                                    warn_pct,
                                                    critical_pct,
                                                )
                                            }
                                            StorageViewMode::Archive => {
                                                QueryExecutor::get_archive_usage_snapshot(
                                                    db_conn.as_ref(),
                                                    warn_pct,
                                                    critical_pct,
                                                )
                                            }
                                            StorageViewMode::Datafiles => {
                                                QueryExecutor::get_datafile_usage_snapshot(
                                                    db_conn.as_ref(),
                                                    warn_pct,
                                                    critical_pct,
                                                )
                                            }
                                        };
                                        query_result.map_err(|err| {
                                            format!(
                                                "Failed to load {} metrics: {err}",
                                                storage_mode_label(mode)
                                            )
                                        })
                                    }
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(StorageMessage::SnapshotLoaded {
                                request_id,
                                mode,
                                alerts_only,
                                result,
                            });
                            app::awake();
                        });
                    }
                    StorageMessage::SnapshotLoaded {
                        request_id,
                        mode,
                        alerts_only,
                        result,
                    } => {
                        if request_id != latest_request_id {
                            continue;
                        }

                        set_cursor(Cursor::Default);
                        app::flush();

                        match result {
                            Ok(snapshot) => {
                                let display_snapshot = if alerts_only {
                                    filter_alert_rows(&snapshot)
                                } else {
                                    snapshot.clone()
                                };
                                result_table.display_result(&display_snapshot);
                                status.set_label(&format!(
                                    "{} loaded: {} rows in {} ms (alerts_only={})",
                                    storage_mode_label(mode),
                                    display_snapshot.row_count,
                                    snapshot.execution_time.as_millis(),
                                    alerts_only
                                ));
                            }
                            Err(err) => {
                                result_table.display_result(&dba_info_result(&format!(
                                    "{} load failed. {}\nTip: views like DBA_TABLESPACE_USAGE_METRICS / V$ views may require DBA privileges.",
                                    storage_mode_label(mode),
                                    err
                                )));
                                status.set_label("Storage load failed");
                            }
                        }
                    }
                    StorageMessage::CloseRequested => {
                        dialog.hide();
                    }
                }
            }
        }

        set_cursor(Cursor::Default);
        app::flush();
        Window::delete(dialog);
    }

    pub fn show_scheduler_manager(&self) {
        enum SchedulerMessage {
            RefreshRequested {
                owner_text: String,
                failed_only: bool,
            },
            CreateRequested {
                owner_text: String,
                job_text: String,
                job_type_text: String,
                job_action_text: String,
                repeat_interval_text: String,
                comments_text: String,
                enabled: bool,
            },
            AlterRequested {
                owner_text: String,
                job_text: String,
                job_action_text: String,
                repeat_interval_text: String,
                comments_text: String,
                enabled_state: Option<bool>,
            },
            HistoryRequested {
                owner_text: String,
                job_text: String,
            },
            RunRequested {
                owner_text: String,
                job_text: String,
            },
            StopRequested {
                owner_text: String,
                job_text: String,
            },
            EnableRequested {
                owner_text: String,
                job_text: String,
            },
            DisableRequested {
                owner_text: String,
                job_text: String,
            },
            DataPumpJobsRequested {
                owner_text: String,
            },
            DataPumpExportRequested {
                job_text: String,
                directory_text: String,
                dump_file_text: String,
                log_file_text: String,
                schema_text: String,
            },
            DataPumpImportRequested {
                job_text: String,
                directory_text: String,
                dump_file_text: String,
                log_file_text: String,
                schema_text: String,
            },
            DataPumpStopRequested {
                owner_text: String,
                job_text: String,
            },
            JobsLoaded {
                request_id: u64,
                failed_only: bool,
                result: Result<QueryResult, String>,
            },
            HistoryLoaded {
                request_id: u64,
                result: Result<QueryResult, String>,
            },
            DataPumpJobsLoaded {
                request_id: u64,
                result: Result<QueryResult, String>,
            },
            ActionFinished(Result<String, String>),
            DataPumpActionFinished(Result<String, String>),
            CloseRequested,
        }

        let (sender, receiver) = mpsc::channel::<SchedulerMessage>();

        let current_group = fltk::group::Group::try_current();
        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let dialog_w = 1280;
        let dialog_h = 760;
        let mut dialog = Window::default()
            .with_size(dialog_w, dialog_h)
            .with_label("Scheduler Manager (DBMS_SCHEDULER)");
        center_on_main(&mut dialog);
        dialog.set_color(theme::panel_raised());
        dialog.make_modal(true);

        let mut root = Flex::default().with_pos(0, 0).with_size(dialog_w, dialog_h);
        root.set_type(FlexType::Column);
        root.set_margin(DIALOG_MARGIN);
        root.set_spacing(DIALOG_SPACING);

        let mut help = Frame::default().with_label(
            "Refresh jobs, inspect run history, and run/stop/enable/disable scheduler jobs. Owner and failed-only filters are optional.",
        );
        help.set_align(Align::Left | Align::Inside);
        help.set_label_color(theme::text_secondary());
        help.set_label_size((configured_ui_font_size().saturating_sub(1)).max(10));
        root.fixed(&help, LABEL_ROW_HEIGHT);

        let mut input_row = Flex::default();
        input_row.set_type(FlexType::Row);
        input_row.set_spacing(DIALOG_SPACING);

        let mut owner_label = Frame::default().with_label("Owner:");
        owner_label.set_label_color(theme::text_primary());
        owner_label.set_align(Align::Inside | Align::Left);
        input_row.fixed(&owner_label, 52);

        let mut owner_input = Input::default();
        owner_input.set_color(theme::input_bg());
        owner_input.set_text_color(theme::text_primary());
        owner_input.set_tooltip("Optional owner filter (e.g. SYS, HR)");
        input_row.fixed(&owner_input, 140);

        let mut failed_only_check = CheckButton::default().with_label("Failed/Attention only");
        failed_only_check.set_label_color(theme::text_primary());
        input_row.fixed(&failed_only_check, 170);

        let mut job_label = Frame::default().with_label("Job:");
        job_label.set_label_color(theme::text_primary());
        job_label.set_align(Align::Inside | Align::Left);
        input_row.fixed(&job_label, 40);

        let mut job_input = Input::default();
        job_input.set_color(theme::input_bg());
        job_input.set_text_color(theme::text_primary());
        job_input.set_tooltip("Scheduler job name");

        let input_filler = Frame::default();
        input_row.resizable(&input_filler);
        input_row.end();
        root.fixed(&input_row, INPUT_ROW_HEIGHT);

        let mut button_row = Flex::default();
        button_row.set_type(FlexType::Row);
        button_row.set_spacing(DIALOG_SPACING);

        let mut refresh_btn = Button::default().with_label("Refresh Jobs");
        refresh_btn.set_color(theme::button_secondary());
        refresh_btn.set_label_color(theme::text_primary());
        refresh_btn.set_frame(FrameType::RFlatBox);
        button_row.fixed(&refresh_btn, BUTTON_WIDTH_LARGE + 26);

        let mut history_btn = Button::default().with_label("Run History");
        history_btn.set_color(theme::button_secondary());
        history_btn.set_label_color(theme::text_primary());
        history_btn.set_frame(FrameType::RFlatBox);
        button_row.fixed(&history_btn, BUTTON_WIDTH_LARGE + 20);

        let mut run_btn = Button::default().with_label("Run Job");
        run_btn.set_color(theme::button_success());
        run_btn.set_label_color(theme::text_primary());
        run_btn.set_frame(FrameType::RFlatBox);
        button_row.fixed(&run_btn, BUTTON_WIDTH_LARGE);

        let mut stop_btn = Button::default().with_label("Stop Job");
        stop_btn.set_color(theme::button_warning());
        stop_btn.set_label_color(theme::text_primary());
        stop_btn.set_frame(FrameType::RFlatBox);
        button_row.fixed(&stop_btn, BUTTON_WIDTH_LARGE);

        let mut enable_btn = Button::default().with_label("Enable Job");
        enable_btn.set_color(theme::button_secondary());
        enable_btn.set_label_color(theme::text_primary());
        enable_btn.set_frame(FrameType::RFlatBox);
        button_row.fixed(&enable_btn, BUTTON_WIDTH_LARGE + 10);

        let mut disable_btn = Button::default().with_label("Disable Job");
        disable_btn.set_color(theme::button_secondary());
        disable_btn.set_label_color(theme::text_primary());
        disable_btn.set_frame(FrameType::RFlatBox);
        button_row.fixed(&disable_btn, BUTTON_WIDTH_LARGE + 14);

        let button_filler = Frame::default();
        button_row.resizable(&button_filler);

        let mut close_btn = Button::default().with_label("Close");
        close_btn.set_color(theme::button_subtle());
        close_btn.set_label_color(theme::text_primary());
        close_btn.set_frame(FrameType::RFlatBox);
        button_row.fixed(&close_btn, BUTTON_WIDTH);

        button_row.end();
        root.fixed(&button_row, BUTTON_ROW_HEIGHT + 4);

        let mut admin_row = Flex::default();
        admin_row.set_type(FlexType::Row);
        admin_row.set_spacing(DIALOG_SPACING);

        let mut create_job_btn = Button::default().with_label("Create Job");
        create_job_btn.set_color(theme::button_success());
        create_job_btn.set_label_color(theme::text_primary());
        create_job_btn.set_frame(FrameType::RFlatBox);
        admin_row.fixed(&create_job_btn, BUTTON_WIDTH_LARGE + 12);

        let mut alter_job_btn = Button::default().with_label("Alter Job");
        alter_job_btn.set_color(theme::button_secondary());
        alter_job_btn.set_label_color(theme::text_primary());
        alter_job_btn.set_frame(FrameType::RFlatBox);
        admin_row.fixed(&alter_job_btn, BUTTON_WIDTH_LARGE + 8);

        let mut dp_jobs_btn = Button::default().with_label("Data Pump Jobs");
        dp_jobs_btn.set_color(theme::button_secondary());
        dp_jobs_btn.set_label_color(theme::text_primary());
        dp_jobs_btn.set_frame(FrameType::RFlatBox);
        admin_row.fixed(&dp_jobs_btn, BUTTON_WIDTH_LARGE + 34);

        let mut dp_export_btn = Button::default().with_label("DP Export");
        dp_export_btn.set_color(theme::button_secondary());
        dp_export_btn.set_label_color(theme::text_primary());
        dp_export_btn.set_frame(FrameType::RFlatBox);
        admin_row.fixed(&dp_export_btn, BUTTON_WIDTH_LARGE + 6);

        let mut dp_import_btn = Button::default().with_label("DP Import");
        dp_import_btn.set_color(theme::button_secondary());
        dp_import_btn.set_label_color(theme::text_primary());
        dp_import_btn.set_frame(FrameType::RFlatBox);
        admin_row.fixed(&dp_import_btn, BUTTON_WIDTH_LARGE + 6);

        let mut dp_stop_btn = Button::default().with_label("DP Stop");
        dp_stop_btn.set_color(theme::button_warning());
        dp_stop_btn.set_label_color(theme::text_primary());
        dp_stop_btn.set_frame(FrameType::RFlatBox);
        admin_row.fixed(&dp_stop_btn, BUTTON_WIDTH_LARGE);

        let admin_filler = Frame::default();
        admin_row.resizable(&admin_filler);
        admin_row.end();
        root.fixed(&admin_row, BUTTON_ROW_HEIGHT + 4);

        let mut result_table =
            ResultTableWidget::with_size(0, 0, dialog_w - DIALOG_MARGIN * 2, dialog_h - 210);
        result_table.set_max_cell_display_chars(320);
        let table_widget = result_table.get_widget();
        root.resizable(&table_widget);
        result_table.display_result(&dba_info_result(
            "Press Refresh Jobs to load scheduler metadata.",
        ));

        let mut status = Frame::default().with_label("Ready");
        status.set_label_color(theme::text_secondary());
        status.set_align(Align::Left | Align::Inside);
        root.fixed(&status, LABEL_ROW_HEIGHT);

        root.end();
        dialog.end();
        fltk::group::Group::set_current(current_group.as_ref());

        let sender_refresh = sender.clone();
        let owner_input_for_refresh = owner_input.clone();
        let failed_only_check_for_refresh = failed_only_check.clone();
        refresh_btn.set_callback(move |_| {
            let _ = sender_refresh.send(SchedulerMessage::RefreshRequested {
                owner_text: owner_input_for_refresh.value(),
                failed_only: failed_only_check_for_refresh.value(),
            });
            app::awake();
        });

        let sender_history = sender.clone();
        let owner_input_for_history = owner_input.clone();
        let job_input_for_history = job_input.clone();
        history_btn.set_callback(move |_| {
            let _ = sender_history.send(SchedulerMessage::HistoryRequested {
                owner_text: owner_input_for_history.value(),
                job_text: job_input_for_history.value(),
            });
            app::awake();
        });

        let sender_create = sender.clone();
        let owner_input_for_create = owner_input.clone();
        let job_input_for_create = job_input.clone();
        create_job_btn.set_callback(move |_| {
            let job_type = match prompt_optional_text("Scheduler job type", "PLSQL_BLOCK") {
                Some(value) if !value.trim().is_empty() => value,
                Some(_) => "PLSQL_BLOCK".to_string(),
                None => return,
            };
            let Some(job_action) = prompt_optional_text("Scheduler job action", "BEGIN NULL; END;")
            else {
                return;
            };
            let repeat_interval =
                prompt_optional_text("Repeat interval (optional)", "").unwrap_or_default();
            let comments = prompt_optional_text("Comments (optional)", "").unwrap_or_default();
            let enabled_choice = fltk::dialog::choice2_default(
                "Enable job immediately?",
                "Cancel",
                "Enable",
                "Create disabled",
            );
            let enabled = match enabled_choice {
                Some(1) => true,
                Some(2) => false,
                _ => return,
            };

            let _ = sender_create.send(SchedulerMessage::CreateRequested {
                owner_text: owner_input_for_create.value(),
                job_text: job_input_for_create.value(),
                job_type_text: job_type,
                job_action_text: job_action,
                repeat_interval_text: repeat_interval,
                comments_text: comments,
                enabled,
            });
            app::awake();
        });

        let sender_alter = sender.clone();
        let owner_input_for_alter = owner_input.clone();
        let job_input_for_alter = job_input.clone();
        alter_job_btn.set_callback(move |_| {
            let Some(job_action) = prompt_optional_text("New job action (blank = no change)", "")
            else {
                return;
            };
            let Some(repeat_interval) =
                prompt_optional_text("New repeat interval (blank = no change)", "")
            else {
                return;
            };
            let Some(comments) = prompt_optional_text("New comments (blank = no change)", "")
            else {
                return;
            };
            let enabled_choice =
                fltk::dialog::choice2_default("Enabled state change", "Skip", "Enable", "Disable");
            let enabled_state = match enabled_choice {
                Some(1) => Some(true),
                Some(2) => Some(false),
                _ => None,
            };

            let _ = sender_alter.send(SchedulerMessage::AlterRequested {
                owner_text: owner_input_for_alter.value(),
                job_text: job_input_for_alter.value(),
                job_action_text: job_action,
                repeat_interval_text: repeat_interval,
                comments_text: comments,
                enabled_state,
            });
            app::awake();
        });

        let sender_dp_jobs = sender.clone();
        let owner_input_for_dp_jobs = owner_input.clone();
        dp_jobs_btn.set_callback(move |_| {
            let _ = sender_dp_jobs.send(SchedulerMessage::DataPumpJobsRequested {
                owner_text: owner_input_for_dp_jobs.value(),
            });
            app::awake();
        });

        let sender_dp_export = sender.clone();
        let job_input_for_dp_export = job_input.clone();
        let owner_input_for_dp_export = owner_input.clone();
        dp_export_btn.set_callback(move |_| {
            let Some(directory) = prompt_optional_text("Data Pump directory", "DATA_PUMP_DIR")
            else {
                return;
            };
            let current_job = job_input_for_dp_export.value();
            let base_job_name = if current_job.trim().is_empty() {
                "DP_EXPORT_JOB".to_string()
            } else {
                current_job.trim().to_string()
            };
            let dump_default = format!("{}.dmp", base_job_name.to_lowercase());
            let log_default = format!("{}.log", base_job_name.to_lowercase());
            let Some(dump_file) = prompt_optional_text("Dump file", &dump_default) else {
                return;
            };
            let Some(log_file) = prompt_optional_text("Log file", &log_default) else {
                return;
            };
            let owner_hint = owner_input_for_dp_export.value();
            let schema_default = if owner_hint.trim().is_empty() {
                String::new()
            } else {
                owner_hint.trim().to_string()
            };
            let Some(schema_name) = prompt_optional_text("Schema name", &schema_default) else {
                return;
            };

            let _ = sender_dp_export.send(SchedulerMessage::DataPumpExportRequested {
                job_text: base_job_name,
                directory_text: directory,
                dump_file_text: dump_file,
                log_file_text: log_file,
                schema_text: schema_name,
            });
            app::awake();
        });

        let sender_dp_import = sender.clone();
        let job_input_for_dp_import = job_input.clone();
        let owner_input_for_dp_import = owner_input.clone();
        dp_import_btn.set_callback(move |_| {
            let Some(directory) = prompt_optional_text("Data Pump directory", "DATA_PUMP_DIR")
            else {
                return;
            };
            let current_job = job_input_for_dp_import.value();
            let base_job_name = if current_job.trim().is_empty() {
                "DP_IMPORT_JOB".to_string()
            } else {
                current_job.trim().to_string()
            };
            let dump_default = format!("{}.dmp", base_job_name.to_lowercase());
            let log_default = format!("{}.log", base_job_name.to_lowercase());
            let Some(dump_file) = prompt_optional_text("Dump file", &dump_default) else {
                return;
            };
            let Some(log_file) = prompt_optional_text("Log file", &log_default) else {
                return;
            };
            let owner_hint = owner_input_for_dp_import.value();
            let schema_default = if owner_hint.trim().is_empty() {
                String::new()
            } else {
                owner_hint.trim().to_string()
            };
            let Some(schema_name) =
                prompt_optional_text("Schema name (optional for full import)", &schema_default)
            else {
                return;
            };

            let _ = sender_dp_import.send(SchedulerMessage::DataPumpImportRequested {
                job_text: base_job_name,
                directory_text: directory,
                dump_file_text: dump_file,
                log_file_text: log_file,
                schema_text: schema_name,
            });
            app::awake();
        });

        let sender_dp_stop = sender.clone();
        let owner_input_for_dp_stop = owner_input.clone();
        let job_input_for_dp_stop = job_input.clone();
        dp_stop_btn.set_callback(move |_| {
            let _ = sender_dp_stop.send(SchedulerMessage::DataPumpStopRequested {
                owner_text: owner_input_for_dp_stop.value(),
                job_text: job_input_for_dp_stop.value(),
            });
            app::awake();
        });

        let sender_run = sender.clone();
        let owner_input_for_run = owner_input.clone();
        let job_input_for_run = job_input.clone();
        run_btn.set_callback(move |_| {
            let _ = sender_run.send(SchedulerMessage::RunRequested {
                owner_text: owner_input_for_run.value(),
                job_text: job_input_for_run.value(),
            });
            app::awake();
        });

        let sender_stop = sender.clone();
        let owner_input_for_stop = owner_input.clone();
        let job_input_for_stop = job_input.clone();
        stop_btn.set_callback(move |_| {
            let _ = sender_stop.send(SchedulerMessage::StopRequested {
                owner_text: owner_input_for_stop.value(),
                job_text: job_input_for_stop.value(),
            });
            app::awake();
        });

        let sender_enable = sender.clone();
        let owner_input_for_enable = owner_input.clone();
        let job_input_for_enable = job_input.clone();
        enable_btn.set_callback(move |_| {
            let _ = sender_enable.send(SchedulerMessage::EnableRequested {
                owner_text: owner_input_for_enable.value(),
                job_text: job_input_for_enable.value(),
            });
            app::awake();
        });

        let sender_disable = sender.clone();
        let owner_input_for_disable = owner_input.clone();
        let job_input_for_disable = job_input.clone();
        disable_btn.set_callback(move |_| {
            let _ = sender_disable.send(SchedulerMessage::DisableRequested {
                owner_text: owner_input_for_disable.value(),
                job_text: job_input_for_disable.value(),
            });
            app::awake();
        });

        let sender_close = sender.clone();
        close_btn.set_callback(move |_| {
            let _ = sender_close.send(SchedulerMessage::CloseRequested);
            app::awake();
        });

        dialog.show();
        let _ = dialog.take_focus();
        let _ = refresh_btn.take_focus();

        let _ = sender.send(SchedulerMessage::RefreshRequested {
            owner_text: owner_input.value(),
            failed_only: failed_only_check.value(),
        });
        app::awake();

        let mut latest_jobs_request_id = 0u64;
        let mut latest_history_request_id = 0u64;
        let mut latest_datapump_request_id = 0u64;
        let mut last_table_selection = (i32::MIN, i32::MIN, i32::MIN, i32::MIN);

        while dialog.shown() {
            app::wait();

            while let Ok(message) = receiver.try_recv() {
                match message {
                    SchedulerMessage::RefreshRequested {
                        owner_text,
                        failed_only,
                    } => {
                        let owner_filter = match normalize_optional_identifier(&owner_text, "Owner")
                        {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        latest_jobs_request_id = latest_jobs_request_id.saturating_add(1);
                        let request_id = latest_jobs_request_id;

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label("Loading scheduler jobs...");
                        result_table.display_result(&dba_info_result("Loading scheduler jobs..."));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                "Loading scheduler jobs",
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::get_scheduler_jobs_snapshot(
                                        db_conn.as_ref(),
                                        owner_filter.as_deref(),
                                        failed_only,
                                    )
                                    .map_err(|err| format!("Failed to load scheduler jobs: {err}")),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(SchedulerMessage::JobsLoaded {
                                request_id,
                                failed_only,
                                result,
                            });
                            app::awake();
                        });
                    }
                    SchedulerMessage::CreateRequested {
                        owner_text,
                        job_text,
                        job_type_text,
                        job_action_text,
                        repeat_interval_text,
                        comments_text,
                        enabled,
                    } => {
                        let owner = match normalize_optional_identifier(&owner_text, "Owner") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        let job_name = match normalize_required_identifier(&job_text, "Job") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        let qualified = qualified_owner_object(owner.as_deref(), &job_name);
                        let confirm = fltk::dialog::choice2_default(
                            &format!("Create scheduler job {}?", qualified),
                            "Cancel",
                            "Create",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Creating {}...", qualified));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Creating scheduler job {}", qualified),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::create_scheduler_job(
                                        db_conn.as_ref(),
                                        owner.as_deref(),
                                        &job_name,
                                        &job_type_text,
                                        &job_action_text,
                                        normalize_optional_text_param(&repeat_interval_text)
                                            .as_deref(),
                                        normalize_optional_text_param(&comments_text).as_deref(),
                                        enabled,
                                    )
                                    .map(|_| format!("Scheduler job {} created", qualified))
                                    .map_err(|err| {
                                        format!(
                                            "Failed to create scheduler job {}: {err}",
                                            qualified
                                        )
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };
                            let _ = sender_result.send(SchedulerMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    SchedulerMessage::AlterRequested {
                        owner_text,
                        job_text,
                        job_action_text,
                        repeat_interval_text,
                        comments_text,
                        enabled_state,
                    } => {
                        let owner = match normalize_optional_identifier(&owner_text, "Owner") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        let job_name = match normalize_required_identifier(&job_text, "Job") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        let job_action = normalize_optional_text_param(&job_action_text);
                        let repeat_interval = normalize_optional_text_param(&repeat_interval_text);
                        let comments = normalize_optional_text_param(&comments_text);
                        if job_action.is_none()
                            && repeat_interval.is_none()
                            && comments.is_none()
                            && enabled_state.is_none()
                        {
                            fltk::dialog::alert_default(
                                "At least one attribute change is required for ALTER.",
                            );
                            continue;
                        }

                        let qualified = qualified_owner_object(owner.as_deref(), &job_name);
                        let confirm = fltk::dialog::choice2_default(
                            &format!("Alter scheduler job {}?", qualified),
                            "Cancel",
                            "Alter",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Altering {}...", qualified));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Altering scheduler job {}", qualified),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::alter_scheduler_job(
                                        db_conn.as_ref(),
                                        owner.as_deref(),
                                        &job_name,
                                        job_action.as_deref(),
                                        repeat_interval.as_deref(),
                                        comments.as_deref(),
                                        enabled_state,
                                    )
                                    .map(|_| format!("Scheduler job {} altered", qualified))
                                    .map_err(|err| {
                                        format!(
                                            "Failed to alter scheduler job {}: {err}",
                                            qualified
                                        )
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };
                            let _ = sender_result.send(SchedulerMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    SchedulerMessage::HistoryRequested {
                        owner_text,
                        job_text,
                    } => {
                        let owner = match normalize_optional_identifier(&owner_text, "Owner") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        let job_name = match normalize_required_identifier(&job_text, "Job") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        latest_history_request_id = latest_history_request_id.saturating_add(1);
                        let request_id = latest_history_request_id;

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label("Loading scheduler job history...");
                        result_table
                            .display_result(&dba_info_result("Loading scheduler job history..."));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                "Loading scheduler job history",
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => {
                                        QueryExecutor::get_scheduler_job_history_snapshot(
                                            db_conn.as_ref(),
                                            owner.as_deref(),
                                            &job_name,
                                        )
                                        .map_err(|err| {
                                            format!(
                                                "Failed to load scheduler history for {}: {err}",
                                                job_name
                                            )
                                        })
                                    }
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result
                                .send(SchedulerMessage::HistoryLoaded { request_id, result });
                            app::awake();
                        });
                    }
                    SchedulerMessage::RunRequested {
                        owner_text,
                        job_text,
                    } => {
                        let owner = match normalize_optional_identifier(&owner_text, "Owner") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        let job_name = match normalize_required_identifier(&job_text, "Job") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        let qualified = qualified_owner_object(owner.as_deref(), &job_name);
                        let confirm = fltk::dialog::choice2_default(
                            &format!("Run scheduler job {} now?", qualified),
                            "Cancel",
                            "Run",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Running {}...", qualified));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Running scheduler job {}", qualified),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::run_scheduler_job(
                                        db_conn.as_ref(),
                                        owner.as_deref(),
                                        &job_name,
                                    )
                                    .map(|_| format!("Scheduler job {} started", qualified))
                                    .map_err(|err| {
                                        format!("Failed to run scheduler job {}: {err}", qualified)
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(SchedulerMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    SchedulerMessage::StopRequested {
                        owner_text,
                        job_text,
                    } => {
                        let owner = match normalize_optional_identifier(&owner_text, "Owner") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        let job_name = match normalize_required_identifier(&job_text, "Job") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        let qualified = qualified_owner_object(owner.as_deref(), &job_name);
                        let confirm = fltk::dialog::choice2_default(
                            &format!("Force stop scheduler job {}?", qualified),
                            "Cancel",
                            "Stop",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Stopping {}...", qualified));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Stopping scheduler job {}", qualified),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::stop_scheduler_job(
                                        db_conn.as_ref(),
                                        owner.as_deref(),
                                        &job_name,
                                        true,
                                    )
                                    .map(|_| format!("Scheduler job {} stop requested", qualified))
                                    .map_err(|err| {
                                        format!("Failed to stop scheduler job {}: {err}", qualified)
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(SchedulerMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    SchedulerMessage::EnableRequested {
                        owner_text,
                        job_text,
                    } => {
                        let owner = match normalize_optional_identifier(&owner_text, "Owner") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        let job_name = match normalize_required_identifier(&job_text, "Job") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        let qualified = qualified_owner_object(owner.as_deref(), &job_name);
                        let confirm = fltk::dialog::choice2_default(
                            &format!("Enable scheduler job {}?", qualified),
                            "Cancel",
                            "Enable",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Enabling {}...", qualified));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Enabling scheduler job {}", qualified),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::enable_scheduler_job(
                                        db_conn.as_ref(),
                                        owner.as_deref(),
                                        &job_name,
                                    )
                                    .map(|_| format!("Scheduler job {} enabled", qualified))
                                    .map_err(|err| {
                                        format!(
                                            "Failed to enable scheduler job {}: {err}",
                                            qualified
                                        )
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(SchedulerMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    SchedulerMessage::DisableRequested {
                        owner_text,
                        job_text,
                    } => {
                        let owner = match normalize_optional_identifier(&owner_text, "Owner") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        let job_name = match normalize_required_identifier(&job_text, "Job") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        let qualified = qualified_owner_object(owner.as_deref(), &job_name);
                        let confirm = fltk::dialog::choice2_default(
                            &format!("Disable scheduler job {}?", qualified),
                            "Cancel",
                            "Disable",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Disabling {}...", qualified));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Disabling scheduler job {}", qualified),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::disable_scheduler_job(
                                        db_conn.as_ref(),
                                        owner.as_deref(),
                                        &job_name,
                                        true,
                                    )
                                    .map(|_| format!("Scheduler job {} disabled", qualified))
                                    .map_err(|err| {
                                        format!(
                                            "Failed to disable scheduler job {}: {err}",
                                            qualified
                                        )
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(SchedulerMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    SchedulerMessage::DataPumpJobsRequested { owner_text } => {
                        let owner_filter = match normalize_optional_identifier(&owner_text, "Owner")
                        {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        latest_datapump_request_id = latest_datapump_request_id.saturating_add(1);
                        let request_id = latest_datapump_request_id;

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label("Loading Data Pump jobs...");
                        result_table.display_result(&dba_info_result("Loading Data Pump jobs..."));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                "Loading Data Pump jobs",
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::get_datapump_jobs_snapshot(
                                        db_conn.as_ref(),
                                        owner_filter.as_deref(),
                                    )
                                    .map_err(|err| format!("Failed to load Data Pump jobs: {err}")),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };
                            let _ = sender_result
                                .send(SchedulerMessage::DataPumpJobsLoaded { request_id, result });
                            app::awake();
                        });
                    }
                    SchedulerMessage::DataPumpExportRequested {
                        job_text,
                        directory_text,
                        dump_file_text,
                        log_file_text,
                        schema_text,
                    } => {
                        let job_name =
                            match normalize_required_identifier(&job_text, "Data Pump job") {
                                Ok(value) => value,
                                Err(err) => {
                                    fltk::dialog::alert_default(&err);
                                    continue;
                                }
                            };
                        let schema_name =
                            match normalize_required_identifier(&schema_text, "Schema") {
                                Ok(value) => value,
                                Err(err) => {
                                    fltk::dialog::alert_default(&err);
                                    continue;
                                }
                            };
                        let confirm = fltk::dialog::choice2_default(
                            &format!("Start Data Pump export job {}?", job_name),
                            "Cancel",
                            "Start",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Starting Data Pump export {}...", job_name));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Starting Data Pump export {}", job_name),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::start_datapump_export_job(
                                        db_conn.as_ref(),
                                        &job_name,
                                        &directory_text,
                                        &dump_file_text,
                                        &log_file_text,
                                        &schema_name,
                                    )
                                    .map(|_| format!("Data Pump export job {} started", job_name))
                                    .map_err(|err| {
                                        format!(
                                            "Failed to start Data Pump export job {}: {err}",
                                            job_name
                                        )
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };
                            let _ = sender_result
                                .send(SchedulerMessage::DataPumpActionFinished(result));
                            app::awake();
                        });
                    }
                    SchedulerMessage::DataPumpImportRequested {
                        job_text,
                        directory_text,
                        dump_file_text,
                        log_file_text,
                        schema_text,
                    } => {
                        let job_name =
                            match normalize_required_identifier(&job_text, "Data Pump job") {
                                Ok(value) => value,
                                Err(err) => {
                                    fltk::dialog::alert_default(&err);
                                    continue;
                                }
                            };
                        let schema_name = normalize_optional_text_param(&schema_text);
                        let confirm = fltk::dialog::choice2_default(
                            &format!("Start Data Pump import job {}?", job_name),
                            "Cancel",
                            "Start",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Starting Data Pump import {}...", job_name));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Starting Data Pump import {}", job_name),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::start_datapump_import_job(
                                        db_conn.as_ref(),
                                        &job_name,
                                        &directory_text,
                                        &dump_file_text,
                                        &log_file_text,
                                        schema_name.as_deref(),
                                    )
                                    .map(|_| format!("Data Pump import job {} started", job_name))
                                    .map_err(|err| {
                                        format!(
                                            "Failed to start Data Pump import job {}: {err}",
                                            job_name
                                        )
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };
                            let _ = sender_result
                                .send(SchedulerMessage::DataPumpActionFinished(result));
                            app::awake();
                        });
                    }
                    SchedulerMessage::DataPumpStopRequested {
                        owner_text,
                        job_text,
                    } => {
                        let owner = match normalize_optional_identifier(&owner_text, "Owner") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        let job_name =
                            match normalize_required_identifier(&job_text, "Data Pump job") {
                                Ok(value) => value,
                                Err(err) => {
                                    fltk::dialog::alert_default(&err);
                                    continue;
                                }
                            };

                        let qualified = qualified_owner_object(owner.as_deref(), &job_name);
                        let confirm = fltk::dialog::choice2_default(
                            &format!("Stop Data Pump job {}?", qualified),
                            "Cancel",
                            "Stop",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Stopping Data Pump {}...", qualified));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Stopping Data Pump job {}", qualified),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::stop_datapump_job(
                                        db_conn.as_ref(),
                                        owner.as_deref(),
                                        &job_name,
                                        true,
                                    )
                                    .map(|_| format!("Data Pump job {} stop requested", qualified))
                                    .map_err(|err| {
                                        format!("Failed to stop Data Pump job {}: {err}", qualified)
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };
                            let _ = sender_result
                                .send(SchedulerMessage::DataPumpActionFinished(result));
                            app::awake();
                        });
                    }
                    SchedulerMessage::JobsLoaded {
                        request_id,
                        failed_only,
                        result,
                    } => {
                        if request_id != latest_jobs_request_id {
                            continue;
                        }

                        set_cursor(Cursor::Default);
                        app::flush();

                        match result {
                            Ok(snapshot) => {
                                result_table.display_result(&snapshot);
                                last_table_selection = (i32::MIN, i32::MIN, i32::MIN, i32::MIN);
                                status.set_label(&format!(
                                    "Loaded {} jobs in {} ms (failed_only={})",
                                    snapshot.row_count,
                                    snapshot.execution_time.as_millis(),
                                    failed_only
                                ));
                            }
                            Err(err) => {
                                result_table.display_result(&dba_info_result(&format!(
                                    "Scheduler jobs load failed. {}\nTip: DBA_SCHEDULER_JOBS access can require elevated privileges.",
                                    err
                                )));
                                last_table_selection = (i32::MIN, i32::MIN, i32::MIN, i32::MIN);
                                status.set_label("Scheduler jobs load failed");
                            }
                        }
                    }
                    SchedulerMessage::HistoryLoaded { request_id, result } => {
                        if request_id != latest_history_request_id {
                            continue;
                        }

                        set_cursor(Cursor::Default);
                        app::flush();

                        match result {
                            Ok(snapshot) => {
                                result_table.display_result(&snapshot);
                                last_table_selection = (i32::MIN, i32::MIN, i32::MIN, i32::MIN);
                                status.set_label(&format!(
                                    "Loaded {} history rows in {} ms",
                                    snapshot.row_count,
                                    snapshot.execution_time.as_millis()
                                ));
                            }
                            Err(err) => {
                                result_table.display_result(&dba_info_result(&format!(
                                    "Scheduler history load failed. {}",
                                    err
                                )));
                                last_table_selection = (i32::MIN, i32::MIN, i32::MIN, i32::MIN);
                                status.set_label("Scheduler history load failed");
                            }
                        }
                    }
                    SchedulerMessage::DataPumpJobsLoaded { request_id, result } => {
                        if request_id != latest_datapump_request_id {
                            continue;
                        }

                        set_cursor(Cursor::Default);
                        app::flush();

                        match result {
                            Ok(snapshot) => {
                                result_table.display_result(&snapshot);
                                last_table_selection = (i32::MIN, i32::MIN, i32::MIN, i32::MIN);
                                status.set_label(&format!(
                                    "Loaded {} Data Pump jobs in {} ms",
                                    snapshot.row_count,
                                    snapshot.execution_time.as_millis()
                                ));
                            }
                            Err(err) => {
                                result_table.display_result(&dba_info_result(&format!(
                                    "Data Pump job load failed. {}\nTip: DBA_DATAPUMP_JOBS privilege may be required.",
                                    err
                                )));
                                last_table_selection = (i32::MIN, i32::MIN, i32::MIN, i32::MIN);
                                status.set_label("Data Pump job load failed");
                            }
                        }
                    }
                    SchedulerMessage::ActionFinished(result) => {
                        set_cursor(Cursor::Default);
                        app::flush();

                        match result {
                            Ok(message) => {
                                status.set_label(&message);
                                let _ = sender.send(SchedulerMessage::RefreshRequested {
                                    owner_text: owner_input.value(),
                                    failed_only: failed_only_check.value(),
                                });
                                app::awake();
                            }
                            Err(err) => {
                                status.set_label("Scheduler action failed");
                                fltk::dialog::alert_default(&err);
                            }
                        }
                    }
                    SchedulerMessage::DataPumpActionFinished(result) => {
                        set_cursor(Cursor::Default);
                        app::flush();

                        match result {
                            Ok(message) => {
                                status.set_label(&message);
                                let _ = sender.send(SchedulerMessage::DataPumpJobsRequested {
                                    owner_text: owner_input.value(),
                                });
                                app::awake();
                            }
                            Err(err) => {
                                status.set_label("Data Pump action failed");
                                fltk::dialog::alert_default(&err);
                            }
                        }
                    }
                    SchedulerMessage::CloseRequested => {
                        dialog.hide();
                    }
                }
            }

            let selection = table_widget.get_selection();
            if selection != last_table_selection {
                last_table_selection = selection;
                let selected_row = selection.0.min(selection.2);
                if selected_row >= 0 {
                    let selected_index = selected_row as usize;
                    if let Some(row) = result_table.row_values(selected_index) {
                        if let Some((owner, job)) = parse_owner_job_row(&row) {
                            owner_input.set_value(&owner);
                            job_input.set_value(&job);
                        }
                    }
                }
            }
        }

        set_cursor(Cursor::Default);
        app::flush();
        Window::delete(dialog);
    }

    pub fn show_security_manager(&self) {
        enum SecurityMessage {
            LoadRequested {
                mode: SecurityViewMode,
                user_text: String,
                profile_text: String,
                attention_only: bool,
            },
            GrantRoleRequested {
                user_text: String,
                role_text: String,
            },
            RevokeRoleRequested {
                user_text: String,
                role_text: String,
            },
            GrantSystemPrivRequested {
                user_text: String,
                priv_text: String,
            },
            RevokeSystemPrivRequested {
                user_text: String,
                priv_text: String,
            },
            SetProfileRequested {
                user_text: String,
                profile_text: String,
            },
            CreateUserRequested {
                user_text: String,
                password_text: String,
                default_tablespace_text: String,
                temporary_tablespace_text: String,
                profile_text: String,
            },
            DropUserRequested {
                user_text: String,
                cascade: bool,
            },
            CreateRoleRequested {
                role_text: String,
            },
            DropRoleRequested {
                role_text: String,
            },
            ExpirePasswordRequested {
                user_text: String,
            },
            LockUserRequested {
                user_text: String,
            },
            UnlockUserRequested {
                user_text: String,
            },
            SnapshotLoaded {
                request_id: u64,
                mode: SecurityViewMode,
                result: Result<QueryResult, String>,
            },
            ActionFinished(Result<String, String>),
            CloseRequested,
        }

        let (sender, receiver) = mpsc::channel::<SecurityMessage>();

        let current_group = fltk::group::Group::try_current();
        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let dialog_w = 1280;
        let dialog_h = 780;
        let mut dialog = Window::default()
            .with_size(dialog_w, dialog_h)
            .with_label("Security Manager (Users/Roles/Grants/Profiles)");
        center_on_main(&mut dialog);
        dialog.set_color(theme::panel_raised());
        dialog.make_modal(true);

        let mut root = Flex::default().with_pos(0, 0).with_size(dialog_w, dialog_h);
        root.set_type(FlexType::Column);
        root.set_margin(DIALOG_MARGIN);
        root.set_spacing(DIALOG_SPACING);

        let mut help = Frame::default().with_label(
            "Inspect users and security metadata, then apply role/system privilege/profile/account lock/password actions. Inputs accept Oracle identifiers only.",
        );
        help.set_align(Align::Left | Align::Inside);
        help.set_label_color(theme::text_secondary());
        help.set_label_size((configured_ui_font_size().saturating_sub(1)).max(10));
        root.fixed(&help, LABEL_ROW_HEIGHT);

        let mut input_row = Flex::default();
        input_row.set_type(FlexType::Row);
        input_row.set_spacing(DIALOG_SPACING);

        let mut user_label = Frame::default().with_label("User:");
        user_label.set_label_color(theme::text_primary());
        user_label.set_align(Align::Inside | Align::Left);
        input_row.fixed(&user_label, 44);

        let mut user_input = Input::default();
        user_input.set_color(theme::input_bg());
        user_input.set_text_color(theme::text_primary());
        user_input.set_tooltip("Target username");
        input_row.fixed(&user_input, 170);

        let mut role_label = Frame::default().with_label("Role/Priv:");
        role_label.set_label_color(theme::text_primary());
        role_label.set_align(Align::Inside | Align::Left);
        input_row.fixed(&role_label, 44);

        let mut role_input = Input::default();
        role_input.set_color(theme::input_bg());
        role_input.set_text_color(theme::text_primary());
        role_input.set_tooltip("Role or system privilege");
        input_row.fixed(&role_input, 170);

        let mut profile_label = Frame::default().with_label("Profile:");
        profile_label.set_label_color(theme::text_primary());
        profile_label.set_align(Align::Inside | Align::Left);
        input_row.fixed(&profile_label, 54);

        let mut profile_input = Input::default();
        profile_input.set_color(theme::input_bg());
        profile_input.set_text_color(theme::text_primary());
        profile_input.set_tooltip("Profile filter or target profile");

        let mut attention_only_check = CheckButton::default().with_label("Attention only");
        attention_only_check.set_label_color(theme::text_primary());
        attention_only_check.set_tooltip("Users view: show LOCKED/EXPIRED accounts only");
        input_row.fixed(&attention_only_check, 126);

        let input_filler = Frame::default();
        input_row.resizable(&input_filler);
        input_row.end();
        root.fixed(&input_row, INPUT_ROW_HEIGHT);

        let mut quick_row = Flex::default();
        quick_row.set_type(FlexType::Row);
        quick_row.set_spacing(DIALOG_SPACING);

        let mut quick_label = Frame::default().with_label("Quick:");
        quick_label.set_label_color(theme::text_primary());
        quick_label.set_align(Align::Inside | Align::Left);
        quick_row.fixed(&quick_label, 46);

        let mut quick_action_choice = Choice::default();
        quick_action_choice.set_color(theme::input_bg());
        quick_action_choice.set_text_color(theme::text_primary());
        quick_action_choice.add_choice(
            "Grant Role|Revoke Role|Grant Sys Priv|Revoke Sys Priv|Set Profile|Lock User|Unlock User|Expire Password|Create User|Drop User|Create Role|Drop Role",
        );
        quick_action_choice.set_value(0);
        quick_row.fixed(&quick_action_choice, 220);

        let mut quick_run_btn = Button::default().with_label("Run Quick Action");
        quick_run_btn.set_color(theme::button_primary());
        quick_run_btn.set_label_color(theme::text_primary());
        quick_run_btn.set_frame(FrameType::RFlatBox);
        quick_run_btn.set_tooltip("Run selected quick action");
        quick_row.fixed(&quick_run_btn, BUTTON_WIDTH_LARGE + 42);

        let quick_filler = Frame::default();
        quick_row.resizable(&quick_filler);
        quick_row.end();
        root.fixed(&quick_row, BUTTON_ROW_HEIGHT + 4);

        let mut quick_hint = Frame::default()
            .with_label("Hint: select user row, choose action, then run quick action.");
        quick_hint.set_label_color(theme::text_secondary());
        quick_hint.set_align(Align::Left | Align::Inside);
        root.fixed(&quick_hint, LABEL_ROW_HEIGHT);

        let mut view_row = Flex::default();
        view_row.set_type(FlexType::Row);
        view_row.set_spacing(DIALOG_SPACING);

        let mut users_btn = Button::default().with_label("Users");
        users_btn.set_color(theme::button_secondary());
        users_btn.set_label_color(theme::text_primary());
        users_btn.set_frame(FrameType::RFlatBox);
        view_row.fixed(&users_btn, BUTTON_WIDTH_LARGE);

        let mut summary_btn = Button::default().with_label("Summary");
        summary_btn.set_color(theme::button_secondary());
        summary_btn.set_label_color(theme::text_primary());
        summary_btn.set_frame(FrameType::RFlatBox);
        view_row.fixed(&summary_btn, BUTTON_WIDTH_LARGE);

        let mut roles_btn = Button::default().with_label("Role Grants");
        roles_btn.set_color(theme::button_secondary());
        roles_btn.set_label_color(theme::text_primary());
        roles_btn.set_frame(FrameType::RFlatBox);
        view_row.fixed(&roles_btn, BUTTON_WIDTH_LARGE + 20);

        let mut sys_btn = Button::default().with_label("Sys Privs");
        sys_btn.set_color(theme::button_secondary());
        sys_btn.set_label_color(theme::text_primary());
        sys_btn.set_frame(FrameType::RFlatBox);
        view_row.fixed(&sys_btn, BUTTON_WIDTH_LARGE + 10);

        let mut obj_btn = Button::default().with_label("Obj Privs");
        obj_btn.set_color(theme::button_secondary());
        obj_btn.set_label_color(theme::text_primary());
        obj_btn.set_frame(FrameType::RFlatBox);
        view_row.fixed(&obj_btn, BUTTON_WIDTH_LARGE + 10);

        let mut profile_btn = Button::default().with_label("Profiles");
        profile_btn.set_color(theme::button_secondary());
        profile_btn.set_label_color(theme::text_primary());
        profile_btn.set_frame(FrameType::RFlatBox);
        view_row.fixed(&profile_btn, BUTTON_WIDTH_LARGE + 4);

        let view_filler = Frame::default();
        view_row.resizable(&view_filler);

        view_row.end();
        root.fixed(&view_row, BUTTON_ROW_HEIGHT + 4);

        let mut action_row_primary = Flex::default();
        action_row_primary.set_type(FlexType::Row);
        action_row_primary.set_spacing(DIALOG_SPACING);

        let mut grant_btn = Button::default().with_label("Grant Role");
        grant_btn.set_color(theme::button_success());
        grant_btn.set_label_color(theme::text_primary());
        grant_btn.set_frame(FrameType::RFlatBox);
        action_row_primary.fixed(&grant_btn, BUTTON_WIDTH_LARGE + 10);

        let mut revoke_btn = Button::default().with_label("Revoke Role");
        revoke_btn.set_color(theme::button_warning());
        revoke_btn.set_label_color(theme::text_primary());
        revoke_btn.set_frame(FrameType::RFlatBox);
        action_row_primary.fixed(&revoke_btn, BUTTON_WIDTH_LARGE + 16);

        let mut grant_sys_btn = Button::default().with_label("Grant Sys Priv");
        grant_sys_btn.set_color(theme::button_success());
        grant_sys_btn.set_label_color(theme::text_primary());
        grant_sys_btn.set_frame(FrameType::RFlatBox);
        action_row_primary.fixed(&grant_sys_btn, BUTTON_WIDTH_LARGE + 26);

        let mut revoke_sys_btn = Button::default().with_label("Revoke Sys Priv");
        revoke_sys_btn.set_color(theme::button_warning());
        revoke_sys_btn.set_label_color(theme::text_primary());
        revoke_sys_btn.set_frame(FrameType::RFlatBox);
        action_row_primary.fixed(&revoke_sys_btn, BUTTON_WIDTH_LARGE + 32);

        let mut set_profile_btn = Button::default().with_label("Set Profile");
        set_profile_btn.set_color(theme::button_secondary());
        set_profile_btn.set_label_color(theme::text_primary());
        set_profile_btn.set_frame(FrameType::RFlatBox);
        action_row_primary.fixed(&set_profile_btn, BUTTON_WIDTH_LARGE + 14);

        let mut expire_password_btn = Button::default().with_label("Expire Password");
        expire_password_btn.set_color(theme::button_warning());
        expire_password_btn.set_label_color(theme::text_primary());
        expire_password_btn.set_frame(FrameType::RFlatBox);
        action_row_primary.fixed(&expire_password_btn, BUTTON_WIDTH_LARGE + 28);

        let action_primary_filler = Frame::default();
        action_row_primary.resizable(&action_primary_filler);
        action_row_primary.end();
        root.fixed(&action_row_primary, BUTTON_ROW_HEIGHT + 4);

        let mut action_row_secondary = Flex::default();
        action_row_secondary.set_type(FlexType::Row);
        action_row_secondary.set_spacing(DIALOG_SPACING);

        let mut create_user_btn = Button::default().with_label("Create User");
        create_user_btn.set_color(theme::button_success());
        create_user_btn.set_label_color(theme::text_primary());
        create_user_btn.set_frame(FrameType::RFlatBox);
        action_row_secondary.fixed(&create_user_btn, BUTTON_WIDTH_LARGE + 18);

        let mut drop_user_btn = Button::default().with_label("Drop User");
        drop_user_btn.set_color(theme::button_danger());
        drop_user_btn.set_label_color(theme::text_primary());
        drop_user_btn.set_frame(FrameType::RFlatBox);
        action_row_secondary.fixed(&drop_user_btn, BUTTON_WIDTH_LARGE + 12);

        let mut create_role_btn = Button::default().with_label("Create Role");
        create_role_btn.set_color(theme::button_success());
        create_role_btn.set_label_color(theme::text_primary());
        create_role_btn.set_frame(FrameType::RFlatBox);
        action_row_secondary.fixed(&create_role_btn, BUTTON_WIDTH_LARGE + 16);

        let mut drop_role_btn = Button::default().with_label("Drop Role");
        drop_role_btn.set_color(theme::button_danger());
        drop_role_btn.set_label_color(theme::text_primary());
        drop_role_btn.set_frame(FrameType::RFlatBox);
        action_row_secondary.fixed(&drop_role_btn, BUTTON_WIDTH_LARGE + 10);

        let mut lock_user_btn = Button::default().with_label("Lock User");
        lock_user_btn.set_color(theme::button_warning());
        lock_user_btn.set_label_color(theme::text_primary());
        lock_user_btn.set_frame(FrameType::RFlatBox);
        action_row_secondary.fixed(&lock_user_btn, BUTTON_WIDTH_LARGE + 4);

        let mut unlock_user_btn = Button::default().with_label("Unlock User");
        unlock_user_btn.set_color(theme::button_secondary());
        unlock_user_btn.set_label_color(theme::text_primary());
        unlock_user_btn.set_frame(FrameType::RFlatBox);
        action_row_secondary.fixed(&unlock_user_btn, BUTTON_WIDTH_LARGE + 12);

        let action_secondary_filler = Frame::default();
        action_row_secondary.resizable(&action_secondary_filler);

        let mut close_btn = Button::default().with_label("Close");
        close_btn.set_color(theme::button_subtle());
        close_btn.set_label_color(theme::text_primary());
        close_btn.set_frame(FrameType::RFlatBox);
        action_row_secondary.fixed(&close_btn, BUTTON_WIDTH);

        action_row_secondary.end();
        root.fixed(&action_row_secondary, BUTTON_ROW_HEIGHT + 4);

        let mut result_table =
            ResultTableWidget::with_size(0, 0, dialog_w - DIALOG_MARGIN * 2, dialog_h - 336);
        result_table.set_max_cell_display_chars(320);
        let table_widget = result_table.get_widget();
        root.resizable(&table_widget);
        result_table.display_result(&dba_info_result(
            "Select a view button to load security metadata.",
        ));

        let mut status = Frame::default().with_label("Ready");
        status.set_label_color(theme::text_secondary());
        status.set_align(Align::Left | Align::Inside);
        root.fixed(&status, LABEL_ROW_HEIGHT);

        root.end();
        dialog.end();
        fltk::group::Group::set_current(current_group.as_ref());

        let sender_users = sender.clone();
        let user_input_for_users = user_input.clone();
        let profile_input_for_users = profile_input.clone();
        let attention_only_check_for_users = attention_only_check.clone();
        users_btn.set_callback(move |_| {
            let _ = sender_users.send(SecurityMessage::LoadRequested {
                mode: SecurityViewMode::Users,
                user_text: user_input_for_users.value(),
                profile_text: profile_input_for_users.value(),
                attention_only: attention_only_check_for_users.value(),
            });
            app::awake();
        });

        let sender_summary = sender.clone();
        let user_input_for_summary = user_input.clone();
        let profile_input_for_summary = profile_input.clone();
        let attention_only_check_for_summary = attention_only_check.clone();
        summary_btn.set_callback(move |_| {
            let _ = sender_summary.send(SecurityMessage::LoadRequested {
                mode: SecurityViewMode::Summary,
                user_text: user_input_for_summary.value(),
                profile_text: profile_input_for_summary.value(),
                attention_only: attention_only_check_for_summary.value(),
            });
            app::awake();
        });

        let sender_roles = sender.clone();
        let user_input_for_roles = user_input.clone();
        let profile_input_for_roles = profile_input.clone();
        let attention_only_check_for_roles = attention_only_check.clone();
        roles_btn.set_callback(move |_| {
            let _ = sender_roles.send(SecurityMessage::LoadRequested {
                mode: SecurityViewMode::RoleGrants,
                user_text: user_input_for_roles.value(),
                profile_text: profile_input_for_roles.value(),
                attention_only: attention_only_check_for_roles.value(),
            });
            app::awake();
        });

        let sender_sys = sender.clone();
        let user_input_for_sys = user_input.clone();
        let profile_input_for_sys = profile_input.clone();
        let attention_only_check_for_sys = attention_only_check.clone();
        sys_btn.set_callback(move |_| {
            let _ = sender_sys.send(SecurityMessage::LoadRequested {
                mode: SecurityViewMode::SystemGrants,
                user_text: user_input_for_sys.value(),
                profile_text: profile_input_for_sys.value(),
                attention_only: attention_only_check_for_sys.value(),
            });
            app::awake();
        });

        let sender_obj = sender.clone();
        let user_input_for_obj = user_input.clone();
        let profile_input_for_obj = profile_input.clone();
        let attention_only_check_for_obj = attention_only_check.clone();
        obj_btn.set_callback(move |_| {
            let _ = sender_obj.send(SecurityMessage::LoadRequested {
                mode: SecurityViewMode::ObjectGrants,
                user_text: user_input_for_obj.value(),
                profile_text: profile_input_for_obj.value(),
                attention_only: attention_only_check_for_obj.value(),
            });
            app::awake();
        });

        let sender_profiles = sender.clone();
        let user_input_for_profiles = user_input.clone();
        let profile_input_for_profiles = profile_input.clone();
        let attention_only_check_for_profiles = attention_only_check.clone();
        profile_btn.set_callback(move |_| {
            let _ = sender_profiles.send(SecurityMessage::LoadRequested {
                mode: SecurityViewMode::Profiles,
                user_text: user_input_for_profiles.value(),
                profile_text: profile_input_for_profiles.value(),
                attention_only: attention_only_check_for_profiles.value(),
            });
            app::awake();
        });

        let mut quick_hint_for_choice = quick_hint.clone();
        quick_action_choice.set_callback(move |choice| {
            quick_hint_for_choice.set_label(security_quick_action_hint(choice.value()));
            app::awake();
        });
        quick_hint.set_label(security_quick_action_hint(quick_action_choice.value()));

        let sender_quick_run = sender.clone();
        let quick_choice_for_run = quick_action_choice.clone();
        let user_input_for_quick = user_input.clone();
        let role_input_for_quick = role_input.clone();
        let profile_input_for_quick = profile_input.clone();
        quick_run_btn.set_callback(move |_| {
            let action = quick_choice_for_run.value();
            let dispatched = match action {
                0 => sender_quick_run
                    .send(SecurityMessage::GrantRoleRequested {
                        user_text: user_input_for_quick.value(),
                        role_text: role_input_for_quick.value(),
                    })
                    .is_ok(),
                1 => sender_quick_run
                    .send(SecurityMessage::RevokeRoleRequested {
                        user_text: user_input_for_quick.value(),
                        role_text: role_input_for_quick.value(),
                    })
                    .is_ok(),
                2 => sender_quick_run
                    .send(SecurityMessage::GrantSystemPrivRequested {
                        user_text: user_input_for_quick.value(),
                        priv_text: role_input_for_quick.value(),
                    })
                    .is_ok(),
                3 => sender_quick_run
                    .send(SecurityMessage::RevokeSystemPrivRequested {
                        user_text: user_input_for_quick.value(),
                        priv_text: role_input_for_quick.value(),
                    })
                    .is_ok(),
                4 => sender_quick_run
                    .send(SecurityMessage::SetProfileRequested {
                        user_text: user_input_for_quick.value(),
                        profile_text: profile_input_for_quick.value(),
                    })
                    .is_ok(),
                5 => sender_quick_run
                    .send(SecurityMessage::LockUserRequested {
                        user_text: user_input_for_quick.value(),
                    })
                    .is_ok(),
                6 => sender_quick_run
                    .send(SecurityMessage::UnlockUserRequested {
                        user_text: user_input_for_quick.value(),
                    })
                    .is_ok(),
                7 => sender_quick_run
                    .send(SecurityMessage::ExpirePasswordRequested {
                        user_text: user_input_for_quick.value(),
                    })
                    .is_ok(),
                8 => {
                    let Some(password) = prompt_secret_text("User password") else {
                        return;
                    };
                    let Some(default_tablespace) =
                        prompt_optional_text("Default tablespace (optional)", "")
                    else {
                        return;
                    };
                    let Some(temporary_tablespace) =
                        prompt_optional_text("Temporary tablespace (optional)", "")
                    else {
                        return;
                    };
                    sender_quick_run
                        .send(SecurityMessage::CreateUserRequested {
                            user_text: user_input_for_quick.value(),
                            password_text: password,
                            default_tablespace_text: default_tablespace,
                            temporary_tablespace_text: temporary_tablespace,
                            profile_text: profile_input_for_quick.value(),
                        })
                        .is_ok()
                }
                9 => {
                    let choice = fltk::dialog::choice2_default(
                        "Drop user mode",
                        "Cancel",
                        "Drop",
                        "Drop CASCADE",
                    );
                    let cascade = match choice {
                        Some(1) => false,
                        Some(2) => true,
                        _ => return,
                    };
                    sender_quick_run
                        .send(SecurityMessage::DropUserRequested {
                            user_text: user_input_for_quick.value(),
                            cascade,
                        })
                        .is_ok()
                }
                10 => sender_quick_run
                    .send(SecurityMessage::CreateRoleRequested {
                        role_text: role_input_for_quick.value(),
                    })
                    .is_ok(),
                11 => sender_quick_run
                    .send(SecurityMessage::DropRoleRequested {
                        role_text: role_input_for_quick.value(),
                    })
                    .is_ok(),
                _ => {
                    fltk::dialog::alert_default("Select a quick action first.");
                    false
                }
            };
            if dispatched {
                app::awake();
            }
        });

        let sender_grant = sender.clone();
        let user_input_for_grant = user_input.clone();
        let role_input_for_grant = role_input.clone();
        grant_btn.set_callback(move |_| {
            let _ = sender_grant.send(SecurityMessage::GrantRoleRequested {
                user_text: user_input_for_grant.value(),
                role_text: role_input_for_grant.value(),
            });
            app::awake();
        });

        let sender_revoke = sender.clone();
        let user_input_for_revoke = user_input.clone();
        let role_input_for_revoke = role_input.clone();
        revoke_btn.set_callback(move |_| {
            let _ = sender_revoke.send(SecurityMessage::RevokeRoleRequested {
                user_text: user_input_for_revoke.value(),
                role_text: role_input_for_revoke.value(),
            });
            app::awake();
        });

        let sender_grant_sys = sender.clone();
        let user_input_for_grant_sys = user_input.clone();
        let role_input_for_grant_sys = role_input.clone();
        grant_sys_btn.set_callback(move |_| {
            let _ = sender_grant_sys.send(SecurityMessage::GrantSystemPrivRequested {
                user_text: user_input_for_grant_sys.value(),
                priv_text: role_input_for_grant_sys.value(),
            });
            app::awake();
        });

        let sender_revoke_sys = sender.clone();
        let user_input_for_revoke_sys = user_input.clone();
        let role_input_for_revoke_sys = role_input.clone();
        revoke_sys_btn.set_callback(move |_| {
            let _ = sender_revoke_sys.send(SecurityMessage::RevokeSystemPrivRequested {
                user_text: user_input_for_revoke_sys.value(),
                priv_text: role_input_for_revoke_sys.value(),
            });
            app::awake();
        });

        let sender_set_profile = sender.clone();
        let user_input_for_set_profile = user_input.clone();
        let profile_input_for_set_profile = profile_input.clone();
        set_profile_btn.set_callback(move |_| {
            let _ = sender_set_profile.send(SecurityMessage::SetProfileRequested {
                user_text: user_input_for_set_profile.value(),
                profile_text: profile_input_for_set_profile.value(),
            });
            app::awake();
        });

        let sender_create_user = sender.clone();
        let user_input_for_create_user = user_input.clone();
        let profile_input_for_create_user = profile_input.clone();
        create_user_btn.set_callback(move |_| {
            let Some(password) = prompt_secret_text("User password") else {
                return;
            };
            let Some(default_tablespace) =
                prompt_optional_text("Default tablespace (optional)", "")
            else {
                return;
            };
            let Some(temporary_tablespace) =
                prompt_optional_text("Temporary tablespace (optional)", "")
            else {
                return;
            };
            let _ = sender_create_user.send(SecurityMessage::CreateUserRequested {
                user_text: user_input_for_create_user.value(),
                password_text: password,
                default_tablespace_text: default_tablespace,
                temporary_tablespace_text: temporary_tablespace,
                profile_text: profile_input_for_create_user.value(),
            });
            app::awake();
        });

        let sender_drop_user = sender.clone();
        let user_input_for_drop_user = user_input.clone();
        drop_user_btn.set_callback(move |_| {
            let choice =
                fltk::dialog::choice2_default("Drop user mode", "Cancel", "Drop", "Drop CASCADE");
            let cascade = match choice {
                Some(1) => false,
                Some(2) => true,
                _ => return,
            };
            let _ = sender_drop_user.send(SecurityMessage::DropUserRequested {
                user_text: user_input_for_drop_user.value(),
                cascade,
            });
            app::awake();
        });

        let sender_create_role = sender.clone();
        let role_input_for_create_role = role_input.clone();
        create_role_btn.set_callback(move |_| {
            let _ = sender_create_role.send(SecurityMessage::CreateRoleRequested {
                role_text: role_input_for_create_role.value(),
            });
            app::awake();
        });

        let sender_drop_role = sender.clone();
        let role_input_for_drop_role = role_input.clone();
        drop_role_btn.set_callback(move |_| {
            let _ = sender_drop_role.send(SecurityMessage::DropRoleRequested {
                role_text: role_input_for_drop_role.value(),
            });
            app::awake();
        });

        let sender_lock_user = sender.clone();
        let user_input_for_lock_user = user_input.clone();
        lock_user_btn.set_callback(move |_| {
            let _ = sender_lock_user.send(SecurityMessage::LockUserRequested {
                user_text: user_input_for_lock_user.value(),
            });
            app::awake();
        });

        let sender_unlock_user = sender.clone();
        let user_input_for_unlock_user = user_input.clone();
        unlock_user_btn.set_callback(move |_| {
            let _ = sender_unlock_user.send(SecurityMessage::UnlockUserRequested {
                user_text: user_input_for_unlock_user.value(),
            });
            app::awake();
        });

        let sender_expire_password = sender.clone();
        let user_input_for_expire_password = user_input.clone();
        expire_password_btn.set_callback(move |_| {
            let _ = sender_expire_password.send(SecurityMessage::ExpirePasswordRequested {
                user_text: user_input_for_expire_password.value(),
            });
            app::awake();
        });

        let sender_close = sender.clone();
        close_btn.set_callback(move |_| {
            let _ = sender_close.send(SecurityMessage::CloseRequested);
            app::awake();
        });

        dialog.show();
        let _ = dialog.take_focus();
        let _ = users_btn.take_focus();

        let mut latest_request_id = 0u64;
        let mut current_view_mode = SecurityViewMode::Users;
        let mut last_table_selection = (i32::MIN, i32::MIN, i32::MIN, i32::MIN);
        refresh_security_action_controls(
            current_view_mode,
            &mut quick_run_btn,
            &mut grant_btn,
            &mut revoke_btn,
            &mut grant_sys_btn,
            &mut revoke_sys_btn,
            &mut set_profile_btn,
            &mut expire_password_btn,
            &mut create_user_btn,
            &mut drop_user_btn,
            &mut create_role_btn,
            &mut drop_role_btn,
            &mut lock_user_btn,
            &mut unlock_user_btn,
        );

        let _ = sender.send(SecurityMessage::LoadRequested {
            mode: SecurityViewMode::Users,
            user_text: user_input.value(),
            profile_text: profile_input.value(),
            attention_only: attention_only_check.value(),
        });
        app::awake();

        while dialog.shown() {
            app::wait();

            while let Ok(message) = receiver.try_recv() {
                match message {
                    SecurityMessage::LoadRequested {
                        mode,
                        user_text,
                        profile_text,
                        attention_only,
                    } => {
                        let user = match mode {
                            SecurityViewMode::Profiles => None,
                            SecurityViewMode::Users => {
                                match normalize_optional_identifier(&user_text, "User") {
                                    Ok(value) => value,
                                    Err(err) => {
                                        fltk::dialog::alert_default(&err);
                                        continue;
                                    }
                                }
                            }
                            _ => match normalize_required_identifier(&user_text, "User") {
                                Ok(value) => Some(value),
                                Err(err) => {
                                    fltk::dialog::alert_default(&err);
                                    continue;
                                }
                            },
                        };

                        let profile_filter =
                            match normalize_optional_identifier(&profile_text, "Profile") {
                                Ok(value) => value,
                                Err(err) => {
                                    fltk::dialog::alert_default(&err);
                                    continue;
                                }
                            };
                        current_view_mode = mode;
                        refresh_security_action_controls(
                            current_view_mode,
                            &mut quick_run_btn,
                            &mut grant_btn,
                            &mut revoke_btn,
                            &mut grant_sys_btn,
                            &mut revoke_sys_btn,
                            &mut set_profile_btn,
                            &mut expire_password_btn,
                            &mut create_user_btn,
                            &mut drop_user_btn,
                            &mut create_role_btn,
                            &mut drop_role_btn,
                            &mut lock_user_btn,
                            &mut unlock_user_btn,
                        );

                        latest_request_id = latest_request_id.saturating_add(1);
                        let request_id = latest_request_id;

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Loading {}...", mode.label()));
                        result_table
                            .display_result(&dba_info_result("Loading security metadata..."));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Loading security view: {}", mode.label()),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => {
                                        let snapshot = match mode {
                                            SecurityViewMode::Users => {
                                                QueryExecutor::get_users_overview_snapshot(
                                                    db_conn.as_ref(),
                                                    user.as_deref(),
                                                    profile_filter.as_deref(),
                                                    attention_only,
                                                )
                                            }
                                            SecurityViewMode::Summary => {
                                                QueryExecutor::get_user_summary_snapshot(
                                                    db_conn.as_ref(),
                                                    user.as_deref().unwrap_or_default(),
                                                )
                                            }
                                            SecurityViewMode::RoleGrants => {
                                                QueryExecutor::get_user_role_grants_snapshot(
                                                    db_conn.as_ref(),
                                                    user.as_deref().unwrap_or_default(),
                                                )
                                            }
                                            SecurityViewMode::SystemGrants => {
                                                QueryExecutor::get_user_system_grants_snapshot(
                                                    db_conn.as_ref(),
                                                    user.as_deref().unwrap_or_default(),
                                                )
                                            }
                                            SecurityViewMode::ObjectGrants => {
                                                QueryExecutor::get_user_object_grants_snapshot(
                                                    db_conn.as_ref(),
                                                    user.as_deref().unwrap_or_default(),
                                                )
                                            }
                                            SecurityViewMode::Profiles => {
                                                QueryExecutor::get_profile_limits_snapshot(
                                                    db_conn.as_ref(),
                                                    profile_filter.as_deref(),
                                                )
                                            }
                                        };
                                        snapshot.map_err(|err| {
                                            format!("Failed to load {}: {err}", mode.label())
                                        })
                                    }
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(SecurityMessage::SnapshotLoaded {
                                request_id,
                                mode,
                                result,
                            });
                            app::awake();
                        });
                    }
                    SecurityMessage::GrantRoleRequested {
                        user_text,
                        role_text,
                    } => {
                        let user = match normalize_required_identifier(&user_text, "User") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        let role = match normalize_required_identifier(&role_text, "Role") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        let confirm = fltk::dialog::choice2_default(
                            &format!("Grant role {} to {}?", role, user),
                            "Cancel",
                            "Grant",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Granting {} to {}...", role, user));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Granting role {} to {}", role, user),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::grant_role_to_user(
                                        db_conn.as_ref(),
                                        &role,
                                        &user,
                                    )
                                    .map(|_| format!("Granted {} to {}", role, user))
                                    .map_err(|err| {
                                        format!("Failed to grant {} to {}: {err}", role, user)
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(SecurityMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    SecurityMessage::RevokeRoleRequested {
                        user_text,
                        role_text,
                    } => {
                        let user = match normalize_required_identifier(&user_text, "User") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        let role = match normalize_required_identifier(&role_text, "Role") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        let confirm = fltk::dialog::choice2_default(
                            &format!("Revoke role {} from {}?", role, user),
                            "Cancel",
                            "Revoke",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Revoking {} from {}...", role, user));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Revoking role {} from {}", role, user),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::revoke_role_from_user(
                                        db_conn.as_ref(),
                                        &role,
                                        &user,
                                    )
                                    .map(|_| format!("Revoked {} from {}", role, user))
                                    .map_err(|err| {
                                        format!("Failed to revoke {} from {}: {err}", role, user)
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(SecurityMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    SecurityMessage::GrantSystemPrivRequested {
                        user_text,
                        priv_text,
                    } => {
                        let user = match normalize_required_identifier(&user_text, "User") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        let privilege = match normalize_required_system_privilege(&priv_text) {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        let confirm = fltk::dialog::choice2_default(
                            &format!("Grant system privilege {} to {}?", privilege, user),
                            "Cancel",
                            "Grant",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Granting {} to {}...", privilege, user));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Granting system privilege {} to {}", privilege, user),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::grant_system_priv_to_user(
                                        db_conn.as_ref(),
                                        &privilege,
                                        &user,
                                    )
                                    .map(|_| format!("Granted {} to {}", privilege, user))
                                    .map_err(|err| {
                                        format!(
                                            "Failed to grant system privilege {} to {}: {err}",
                                            privilege, user
                                        )
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(SecurityMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    SecurityMessage::RevokeSystemPrivRequested {
                        user_text,
                        priv_text,
                    } => {
                        let user = match normalize_required_identifier(&user_text, "User") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        let privilege = match normalize_required_system_privilege(&priv_text) {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        let confirm = fltk::dialog::choice2_default(
                            &format!("Revoke system privilege {} from {}?", privilege, user),
                            "Cancel",
                            "Revoke",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Revoking {} from {}...", privilege, user));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Revoking system privilege {} from {}", privilege, user),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::revoke_system_priv_from_user(
                                        db_conn.as_ref(),
                                        &privilege,
                                        &user,
                                    )
                                    .map(|_| format!("Revoked {} from {}", privilege, user))
                                    .map_err(|err| {
                                        format!(
                                            "Failed to revoke system privilege {} from {}: {err}",
                                            privilege, user
                                        )
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(SecurityMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    SecurityMessage::SetProfileRequested {
                        user_text,
                        profile_text,
                    } => {
                        let user = match normalize_required_identifier(&user_text, "User") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        let profile = match normalize_required_identifier(&profile_text, "Profile")
                        {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        let confirm = fltk::dialog::choice2_default(
                            &format!("Set profile {} for user {}?", profile, user),
                            "Cancel",
                            "Apply",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Applying profile {} to {}...", profile, user));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Setting profile {} for {}", profile, user),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::set_user_profile(
                                        db_conn.as_ref(),
                                        &user,
                                        &profile,
                                    )
                                    .map(|_| format!("Profile {} applied to {}", profile, user))
                                    .map_err(|err| {
                                        format!(
                                            "Failed to set profile {} for {}: {err}",
                                            profile, user
                                        )
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(SecurityMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    SecurityMessage::CreateUserRequested {
                        user_text,
                        password_text,
                        default_tablespace_text,
                        temporary_tablespace_text,
                        profile_text,
                    } => {
                        let user = match normalize_required_identifier(&user_text, "User") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        let profile = match normalize_optional_identifier(&profile_text, "Profile")
                        {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        let default_tablespace = match normalize_optional_identifier(
                            &default_tablespace_text,
                            "Default tablespace",
                        ) {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        let temporary_tablespace = match normalize_optional_identifier(
                            &temporary_tablespace_text,
                            "Temporary tablespace",
                        ) {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        if password_text.trim().is_empty() {
                            fltk::dialog::alert_default("Password is required for CREATE USER.");
                            continue;
                        }

                        let confirm = fltk::dialog::choice2_default(
                            &format!("Create user {}?", user),
                            "Cancel",
                            "Create",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Creating user {}...", user));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Creating user {}", user),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::create_user(
                                        db_conn.as_ref(),
                                        &user,
                                        &password_text,
                                        default_tablespace.as_deref(),
                                        temporary_tablespace.as_deref(),
                                        profile.as_deref(),
                                    )
                                    .map(|_| format!("User {} created", user))
                                    .map_err(|err| {
                                        format!("Failed to create user {}: {err}", user)
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(SecurityMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    SecurityMessage::DropUserRequested { user_text, cascade } => {
                        let user = match normalize_required_identifier(&user_text, "User") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        let confirm = fltk::dialog::choice2_default(
                            &format!(
                                "Drop user {}{}?",
                                user,
                                if cascade { " CASCADE" } else { "" }
                            ),
                            "Cancel",
                            "Drop",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Dropping user {}...", user));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Dropping user {}", user),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => {
                                        QueryExecutor::drop_user(db_conn.as_ref(), &user, cascade)
                                            .map(|_| {
                                                if cascade {
                                                    format!("User {} dropped (CASCADE)", user)
                                                } else {
                                                    format!("User {} dropped", user)
                                                }
                                            })
                                            .map_err(|err| {
                                                format!("Failed to drop user {}: {err}", user)
                                            })
                                    }
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(SecurityMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    SecurityMessage::CreateRoleRequested { role_text } => {
                        let role = match normalize_required_identifier(&role_text, "Role") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        let confirm = fltk::dialog::choice2_default(
                            &format!("Create role {}?", role),
                            "Cancel",
                            "Create",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Creating role {}...", role));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Creating role {}", role),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => {
                                        QueryExecutor::create_role(db_conn.as_ref(), &role)
                                            .map(|_| format!("Role {} created", role))
                                            .map_err(|err| {
                                                format!("Failed to create role {}: {err}", role)
                                            })
                                    }
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(SecurityMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    SecurityMessage::DropRoleRequested { role_text } => {
                        let role = match normalize_required_identifier(&role_text, "Role") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        let confirm = fltk::dialog::choice2_default(
                            &format!("Drop role {}?", role),
                            "Cancel",
                            "Drop",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Dropping role {}...", role));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Dropping role {}", role),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => {
                                        QueryExecutor::drop_role(db_conn.as_ref(), &role)
                                            .map(|_| format!("Role {} dropped", role))
                                            .map_err(|err| {
                                                format!("Failed to drop role {}: {err}", role)
                                            })
                                    }
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(SecurityMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    SecurityMessage::ExpirePasswordRequested { user_text } => {
                        let user = match normalize_required_identifier(&user_text, "User") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        let confirm = fltk::dialog::choice2_default(
                            &format!("Expire password for {} now?", user),
                            "Cancel",
                            "Expire",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Expiring password for {}...", user));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result =
                                match try_lock_connection_with_activity(
                                    &connection,
                                    format!("Expiring password for {}", user),
                                ) {
                                    Some(mut guard) => match guard.require_live_connection() {
                                        Ok(db_conn) => QueryExecutor::expire_user_password(
                                            db_conn.as_ref(),
                                            &user,
                                        )
                                        .map(|_| format!("Password expired for {}", user))
                                        .map_err(|err| {
                                            format!("Failed to expire password for {}: {err}", user)
                                        }),
                                        Err(message) => Err(message),
                                    },
                                    None => Err(format_connection_busy_message()),
                                };

                            let _ = sender_result.send(SecurityMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    SecurityMessage::LockUserRequested { user_text } => {
                        let user = match normalize_required_identifier(&user_text, "User") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        let confirm = fltk::dialog::choice2_default(
                            &format!("Lock account {}?", user),
                            "Cancel",
                            "Lock",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Locking {}...", user));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Locking user account {}", user),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => {
                                        QueryExecutor::lock_user_account(db_conn.as_ref(), &user)
                                            .map(|_| format!("User {} locked", user))
                                            .map_err(|err| {
                                                format!("Failed to lock {}: {err}", user)
                                            })
                                    }
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(SecurityMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    SecurityMessage::UnlockUserRequested { user_text } => {
                        let user = match normalize_required_identifier(&user_text, "User") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        let confirm = fltk::dialog::choice2_default(
                            &format!("Unlock account {}?", user),
                            "Cancel",
                            "Unlock",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Unlocking {}...", user));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Unlocking user account {}", user),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => {
                                        QueryExecutor::unlock_user_account(db_conn.as_ref(), &user)
                                            .map(|_| format!("User {} unlocked", user))
                                            .map_err(|err| {
                                                format!("Failed to unlock {}: {err}", user)
                                            })
                                    }
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(SecurityMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    SecurityMessage::SnapshotLoaded {
                        request_id,
                        mode,
                        result,
                    } => {
                        if request_id != latest_request_id {
                            continue;
                        }

                        set_cursor(Cursor::Default);
                        app::flush();

                        match result {
                            Ok(snapshot) => {
                                result_table.display_result(&snapshot);
                                last_table_selection = (i32::MIN, i32::MIN, i32::MIN, i32::MIN);
                                status.set_label(&format!(
                                    "{} loaded: {} rows in {} ms",
                                    mode.label(),
                                    snapshot.row_count,
                                    snapshot.execution_time.as_millis()
                                ));
                            }
                            Err(err) => {
                                result_table.display_result(&dba_info_result(&format!(
                                    "{} load failed. {}\nTip: DBA_* privilege views may be restricted.",
                                    mode.label(),
                                    err
                                )));
                                last_table_selection = (i32::MIN, i32::MIN, i32::MIN, i32::MIN);
                                status.set_label("Security metadata load failed");
                            }
                        }
                    }
                    SecurityMessage::ActionFinished(result) => {
                        set_cursor(Cursor::Default);
                        app::flush();

                        match result {
                            Ok(message) => {
                                status.set_label(&message);
                                let _ = sender.send(SecurityMessage::LoadRequested {
                                    mode: current_view_mode,
                                    user_text: user_input.value(),
                                    profile_text: profile_input.value(),
                                    attention_only: attention_only_check.value(),
                                });
                                app::awake();
                            }
                            Err(err) => {
                                status.set_label("Security action failed");
                                fltk::dialog::alert_default(&err);
                            }
                        }
                    }
                    SecurityMessage::CloseRequested => {
                        dialog.hide();
                    }
                }
            }

            let selection = table_widget.get_selection();
            if selection != last_table_selection {
                last_table_selection = selection;
                let selected_row = selection.0.min(selection.2);
                if selected_row >= 0 {
                    let selected_index = selected_row as usize;
                    if let Some(row) = result_table.row_values(selected_index) {
                        if !matches!(current_view_mode, SecurityViewMode::Profiles) {
                            if let Some(first_value) = row.first() {
                                let normalized = first_value.trim().to_uppercase();
                                if !normalized.is_empty() && is_ascii_identifier(&normalized) {
                                    user_input.set_value(&normalized);
                                }
                            }
                        }
                        match current_view_mode {
                            SecurityViewMode::RoleGrants => {
                                if let Some(value) = row.get(1) {
                                    let normalized = value.trim().to_uppercase();
                                    if !normalized.is_empty() && is_ascii_identifier(&normalized) {
                                        role_input.set_value(&normalized);
                                    }
                                }
                            }
                            SecurityViewMode::SystemGrants => {
                                if let Some(value) = row.get(1) {
                                    if let Ok(normalized) =
                                        normalize_required_system_privilege(value)
                                    {
                                        role_input.set_value(&normalized);
                                    }
                                }
                            }
                            SecurityViewMode::Summary => {
                                if let Some(value) = row.get(2) {
                                    let normalized = value.trim().to_uppercase();
                                    if !normalized.is_empty() && is_ascii_identifier(&normalized) {
                                        profile_input.set_value(&normalized);
                                    }
                                }
                            }
                            SecurityViewMode::Users => {
                                if let Some(value) = row.get(2) {
                                    let normalized = value.trim().to_uppercase();
                                    if !normalized.is_empty() && is_ascii_identifier(&normalized) {
                                        profile_input.set_value(&normalized);
                                    }
                                }
                            }
                            SecurityViewMode::Profiles => {
                                if let Some(value) = row.first() {
                                    let normalized = value.trim().to_uppercase();
                                    if !normalized.is_empty() && is_ascii_identifier(&normalized) {
                                        profile_input.set_value(&normalized);
                                    }
                                }
                            }
                            SecurityViewMode::ObjectGrants => {}
                        }
                    }
                }
            }
        }

        set_cursor(Cursor::Default);
        app::flush();
        Window::delete(dialog);
    }

    pub fn show_rman_dashboard(&self) {
        enum RmanMessage {
            LoadRequested {
                mode: RmanViewMode,
                lookback_text: String,
                attention_only: bool,
            },
            RunBackupRequested {
                owner_text: String,
                job_text: String,
                script_text: String,
            },
            RunRestoreRequested {
                owner_text: String,
                job_text: String,
                script_text: String,
            },
            SnapshotLoaded {
                request_id: u64,
                mode: RmanViewMode,
                attention_only: bool,
                result: Result<QueryResult, String>,
            },
            ActionFinished(Result<String, String>),
            CloseRequested,
        }

        let (sender, receiver) = mpsc::channel::<RmanMessage>();

        let current_group = fltk::group::Group::try_current();
        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let dialog_w = 1220;
        let dialog_h = 720;
        let mut dialog = Window::default()
            .with_size(dialog_w, dialog_h)
            .with_label("RMAN Dashboard (Backup Jobs/Sets/Coverage)");
        center_on_main(&mut dialog);
        dialog.set_color(theme::panel_raised());
        dialog.make_modal(true);

        let mut root = Flex::default().with_pos(0, 0).with_size(dialog_w, dialog_h);
        root.set_type(FlexType::Column);
        root.set_margin(DIALOG_MARGIN);
        root.set_spacing(DIALOG_SPACING);

        let mut help = Frame::default().with_label(
            "Monitors RMAN backup execution, backup sets, and coverage indicators using V$ backup views.",
        );
        help.set_align(Align::Left | Align::Inside);
        help.set_label_color(theme::text_secondary());
        help.set_label_size((configured_ui_font_size().saturating_sub(1)).max(10));
        root.fixed(&help, LABEL_ROW_HEIGHT);

        let mut controls = Flex::default();
        controls.set_type(FlexType::Row);
        controls.set_spacing(DIALOG_SPACING);

        let mut lookback_label = Frame::default().with_label("Lookback(h):");
        lookback_label.set_label_color(theme::text_primary());
        lookback_label.set_align(Align::Inside | Align::Left);
        controls.fixed(&lookback_label, 86);

        let mut lookback_input = IntInput::default();
        lookback_input.set_value("24");
        lookback_input.set_color(theme::input_bg());
        lookback_input.set_text_color(theme::text_primary());
        lookback_input.set_tooltip("Used by Jobs/Backup Sets view (1-720)");
        controls.fixed(&lookback_input, 68);

        let mut attention_only_check = CheckButton::default().with_label("Attention only");
        attention_only_check.set_label_color(theme::text_primary());
        attention_only_check.set_tooltip("Show only non-completed/non-available rows");
        controls.fixed(&attention_only_check, 128);

        let mut jobs_btn = Button::default().with_label("Backup Jobs");
        jobs_btn.set_color(theme::button_secondary());
        jobs_btn.set_label_color(theme::text_primary());
        jobs_btn.set_frame(FrameType::RFlatBox);
        controls.fixed(&jobs_btn, BUTTON_WIDTH_LARGE + 14);

        let mut sets_btn = Button::default().with_label("Backup Sets");
        sets_btn.set_color(theme::button_secondary());
        sets_btn.set_label_color(theme::text_primary());
        sets_btn.set_frame(FrameType::RFlatBox);
        controls.fixed(&sets_btn, BUTTON_WIDTH_LARGE + 16);

        let mut coverage_btn = Button::default().with_label("Coverage");
        coverage_btn.set_color(theme::button_secondary());
        coverage_btn.set_label_color(theme::text_primary());
        coverage_btn.set_frame(FrameType::RFlatBox);
        controls.fixed(&coverage_btn, BUTTON_WIDTH_LARGE);

        let controls_filler = Frame::default();
        controls.resizable(&controls_filler);

        let mut close_btn = Button::default().with_label("Close");
        close_btn.set_color(theme::button_subtle());
        close_btn.set_label_color(theme::text_primary());
        close_btn.set_frame(FrameType::RFlatBox);
        controls.fixed(&close_btn, BUTTON_WIDTH);

        controls.end();
        root.fixed(&controls, BUTTON_ROW_HEIGHT + 4);

        let mut action_row = Flex::default();
        action_row.set_type(FlexType::Row);
        action_row.set_spacing(DIALOG_SPACING);

        let mut owner_label = Frame::default().with_label("Owner:");
        owner_label.set_label_color(theme::text_primary());
        owner_label.set_align(Align::Inside | Align::Left);
        action_row.fixed(&owner_label, 48);

        let mut owner_input = Input::default();
        owner_input.set_color(theme::input_bg());
        owner_input.set_text_color(theme::text_primary());
        owner_input.set_tooltip("Optional scheduler owner for RMAN job");
        action_row.fixed(&owner_input, 130);

        let mut job_label = Frame::default().with_label("Job:");
        job_label.set_label_color(theme::text_primary());
        job_label.set_align(Align::Inside | Align::Left);
        action_row.fixed(&job_label, 36);

        let mut job_input = Input::default();
        job_input.set_value(&default_rman_job_name("RMAN_BACKUP_JOB"));
        job_input.set_color(theme::input_bg());
        job_input.set_text_color(theme::text_primary());
        job_input.set_tooltip("Auto-generated unique scheduler job name");
        job_input.set_readonly(true);
        action_row.fixed(&job_input, 170);

        let mut run_backup_btn = Button::default().with_label("Run Backup");
        run_backup_btn.set_color(theme::button_success());
        run_backup_btn.set_label_color(theme::text_primary());
        run_backup_btn.set_frame(FrameType::RFlatBox);
        action_row.fixed(&run_backup_btn, BUTTON_WIDTH_LARGE + 20);

        let mut run_restore_btn = Button::default().with_label("Run Restore");
        run_restore_btn.set_color(theme::button_warning());
        run_restore_btn.set_label_color(theme::text_primary());
        run_restore_btn.set_frame(FrameType::RFlatBox);
        action_row.fixed(&run_restore_btn, BUTTON_WIDTH_LARGE + 20);

        let action_filler = Frame::default();
        action_row.resizable(&action_filler);
        action_row.end();
        root.fixed(&action_row, BUTTON_ROW_HEIGHT + 4);

        let mut result_table =
            ResultTableWidget::with_size(0, 0, dialog_w - DIALOG_MARGIN * 2, dialog_h - 232);
        result_table.set_max_cell_display_chars(360);
        let table_widget = result_table.get_widget();
        root.resizable(&table_widget);
        result_table.display_result(&dba_info_result("Press a button to load RMAN metrics."));

        let mut status = Frame::default().with_label("Ready");
        status.set_label_color(theme::text_secondary());
        status.set_align(Align::Left | Align::Inside);
        root.fixed(&status, LABEL_ROW_HEIGHT);

        root.end();
        dialog.end();
        fltk::group::Group::set_current(current_group.as_ref());

        let sender_jobs = sender.clone();
        let lookback_input_for_jobs = lookback_input.clone();
        let attention_for_jobs = attention_only_check.clone();
        jobs_btn.set_callback(move |_| {
            let _ = sender_jobs.send(RmanMessage::LoadRequested {
                mode: RmanViewMode::Jobs,
                lookback_text: lookback_input_for_jobs.value(),
                attention_only: attention_for_jobs.value(),
            });
            app::awake();
        });

        let sender_sets = sender.clone();
        let lookback_input_for_sets = lookback_input.clone();
        let attention_for_sets = attention_only_check.clone();
        sets_btn.set_callback(move |_| {
            let _ = sender_sets.send(RmanMessage::LoadRequested {
                mode: RmanViewMode::BackupSets,
                lookback_text: lookback_input_for_sets.value(),
                attention_only: attention_for_sets.value(),
            });
            app::awake();
        });

        let sender_coverage = sender.clone();
        let lookback_input_for_coverage = lookback_input.clone();
        let attention_for_coverage = attention_only_check.clone();
        coverage_btn.set_callback(move |_| {
            let _ = sender_coverage.send(RmanMessage::LoadRequested {
                mode: RmanViewMode::Coverage,
                lookback_text: lookback_input_for_coverage.value(),
                attention_only: attention_for_coverage.value(),
            });
            app::awake();
        });

        let sender_run_backup = sender.clone();
        let owner_input_for_backup = owner_input.clone();
        let mut job_input_for_backup = job_input.clone();
        run_backup_btn.set_callback(move |_| {
            let job_text = default_rman_job_name("RMAN_BACKUP_JOB");
            job_input_for_backup.set_value(&job_text);
            let Some(script_text) = prompt_optional_text(
                "RMAN backup script (without EXIT)",
                "BACKUP DATABASE PLUS ARCHIVELOG;",
            ) else {
                return;
            };
            let _ = sender_run_backup.send(RmanMessage::RunBackupRequested {
                owner_text: owner_input_for_backup.value(),
                job_text,
                script_text,
            });
            app::awake();
        });

        let sender_run_restore = sender.clone();
        let owner_input_for_restore = owner_input.clone();
        let mut job_input_for_restore = job_input.clone();
        run_restore_btn.set_callback(move |_| {
            let job_text = default_rman_job_name("RMAN_RESTORE_JOB");
            job_input_for_restore.set_value(&job_text);
            let Some(script_text) = prompt_optional_text(
                "RMAN restore script (without EXIT)",
                "RESTORE DATABASE;\nRECOVER DATABASE;",
            ) else {
                return;
            };
            let _ = sender_run_restore.send(RmanMessage::RunRestoreRequested {
                owner_text: owner_input_for_restore.value(),
                job_text,
                script_text,
            });
            app::awake();
        });

        let sender_close = sender.clone();
        close_btn.set_callback(move |_| {
            let _ = sender_close.send(RmanMessage::CloseRequested);
            app::awake();
        });

        dialog.show();
        let _ = dialog.take_focus();
        let _ = jobs_btn.take_focus();

        let _ = sender.send(RmanMessage::LoadRequested {
            mode: RmanViewMode::Jobs,
            lookback_text: lookback_input.value(),
            attention_only: attention_only_check.value(),
        });
        app::awake();

        let mut latest_request_id = 0u64;
        let mut current_mode = RmanViewMode::Jobs;
        let mut current_lookback_text = lookback_input.value();
        let mut current_attention_only = attention_only_check.value();
        let mut loading_snapshot = false;
        let mut action_running = false;
        let mut pending_request: Option<(RmanViewMode, String, bool)> = None;
        while dialog.shown() {
            app::wait();

            while let Ok(message) = receiver.try_recv() {
                match message {
                    RmanMessage::LoadRequested {
                        mode,
                        lookback_text,
                        attention_only,
                    } => {
                        if loading_snapshot || action_running {
                            pending_request = Some((mode, lookback_text, attention_only));
                            status.set_label(&format!(
                                "{} request queued (will run after current load)",
                                mode.label()
                            ));
                            continue;
                        }

                        let lookback_hours = if matches!(mode, RmanViewMode::Coverage) {
                            1
                        } else {
                            match parse_bounded_positive_u32(
                                &lookback_text,
                                "Lookback hours",
                                RMAN_LOOKBACK_MAX_HOURS,
                            ) {
                                Ok(value) => value,
                                Err(err) => {
                                    fltk::dialog::alert_default(&err);
                                    continue;
                                }
                            }
                        };

                        current_mode = mode;
                        current_lookback_text = lookback_text;
                        current_attention_only = attention_only;
                        latest_request_id = latest_request_id.saturating_add(1);
                        let request_id = latest_request_id;

                        set_cursor(Cursor::Wait);
                        app::flush();
                        loading_snapshot = true;
                        jobs_btn.deactivate();
                        sets_btn.deactivate();
                        coverage_btn.deactivate();
                        lookback_input.deactivate();
                        attention_only_check.deactivate();
                        owner_input.deactivate();
                        job_input.deactivate();
                        run_backup_btn.deactivate();
                        run_restore_btn.deactivate();
                        status.set_label(&format!("Loading {}...", mode.label()));
                        result_table.display_result(&dba_info_result(&format!(
                            "Loading {}...",
                            mode.label()
                        )));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Loading {}", mode.label()),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => match mode {
                                        RmanViewMode::Jobs => QueryExecutor::get_rman_job_snapshot(
                                            db_conn.as_ref(),
                                            lookback_hours,
                                            attention_only,
                                        )
                                        .map_err(|err| {
                                            format!("Failed to load RMAN jobs snapshot: {err}")
                                        }),
                                        RmanViewMode::BackupSets => {
                                            QueryExecutor::get_rman_backup_set_snapshot(
                                                db_conn.as_ref(),
                                                lookback_hours,
                                                attention_only,
                                            )
                                            .map_err(|err| {
                                                format!(
                                                    "Failed to load RMAN backup set snapshot: {err}"
                                                )
                                            })
                                        }
                                        RmanViewMode::Coverage => {
                                            QueryExecutor::get_rman_backup_coverage_snapshot(
                                                db_conn.as_ref(),
                                            )
                                            .map_err(|err| {
                                                format!("Failed to load RMAN coverage snapshot: {err}")
                                            })
                                        }
                                    },
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(RmanMessage::SnapshotLoaded {
                                request_id,
                                mode,
                                attention_only,
                                result,
                            });
                            app::awake();
                        });
                    }
                    RmanMessage::RunBackupRequested {
                        owner_text,
                        job_text,
                        script_text,
                    } => {
                        if loading_snapshot || action_running {
                            status.set_label("RMAN request already in progress");
                            continue;
                        }

                        let owner = match normalize_optional_identifier(&owner_text, "Owner") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        let job_name = match normalize_required_identifier(&job_text, "Job") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        if script_text.trim().is_empty() {
                            fltk::dialog::alert_default("Backup script is required.");
                            continue;
                        }

                        let qualified = qualified_owner_object(owner.as_deref(), &job_name);
                        let confirm = fltk::dialog::choice2_default(
                            &format!("Submit RMAN backup job {}?", qualified),
                            "Cancel",
                            "Submit",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        action_running = true;
                        jobs_btn.deactivate();
                        sets_btn.deactivate();
                        coverage_btn.deactivate();
                        lookback_input.deactivate();
                        attention_only_check.deactivate();
                        owner_input.deactivate();
                        job_input.deactivate();
                        run_backup_btn.deactivate();
                        run_restore_btn.deactivate();
                        status.set_label(&format!("Submitting RMAN backup job {}...", qualified));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Submitting RMAN backup job {}", qualified),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::run_rman_backup_job(
                                        db_conn.as_ref(),
                                        owner.as_deref(),
                                        &job_name,
                                        &script_text,
                                    )
                                    .map(|_| format!("RMAN backup job {} submitted", qualified))
                                    .map_err(|err| {
                                        format!(
                                            "Failed to submit RMAN backup job {}: {err}",
                                            qualified
                                        )
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(RmanMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    RmanMessage::RunRestoreRequested {
                        owner_text,
                        job_text,
                        script_text,
                    } => {
                        if loading_snapshot || action_running {
                            status.set_label("RMAN request already in progress");
                            continue;
                        }

                        let owner = match normalize_optional_identifier(&owner_text, "Owner") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        let job_name = match normalize_required_identifier(&job_text, "Job") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        if script_text.trim().is_empty() {
                            fltk::dialog::alert_default("Restore script is required.");
                            continue;
                        }

                        let qualified = qualified_owner_object(owner.as_deref(), &job_name);
                        let confirm = fltk::dialog::choice2_default(
                            &format!("Submit RMAN restore job {}?", qualified),
                            "Cancel",
                            "Submit",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        action_running = true;
                        jobs_btn.deactivate();
                        sets_btn.deactivate();
                        coverage_btn.deactivate();
                        lookback_input.deactivate();
                        attention_only_check.deactivate();
                        owner_input.deactivate();
                        job_input.deactivate();
                        run_backup_btn.deactivate();
                        run_restore_btn.deactivate();
                        status.set_label(&format!("Submitting RMAN restore job {}...", qualified));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Submitting RMAN restore job {}", qualified),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::run_rman_restore_job(
                                        db_conn.as_ref(),
                                        owner.as_deref(),
                                        &job_name,
                                        &script_text,
                                    )
                                    .map(|_| format!("RMAN restore job {} submitted", qualified))
                                    .map_err(|err| {
                                        format!(
                                            "Failed to submit RMAN restore job {}: {err}",
                                            qualified
                                        )
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(RmanMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    RmanMessage::SnapshotLoaded {
                        request_id,
                        mode,
                        attention_only,
                        result,
                    } => {
                        if request_id != latest_request_id {
                            continue;
                        }

                        set_cursor(Cursor::Default);
                        app::flush();
                        loading_snapshot = false;
                        jobs_btn.activate();
                        sets_btn.activate();
                        coverage_btn.activate();
                        lookback_input.activate();
                        attention_only_check.activate();
                        owner_input.activate();
                        job_input.activate();
                        run_backup_btn.activate();
                        run_restore_btn.activate();

                        match result {
                            Ok(snapshot) => {
                                let displayed =
                                    if attention_only && matches!(mode, RmanViewMode::Coverage) {
                                        filter_alert_rows(&snapshot)
                                    } else {
                                        snapshot
                                    };
                                result_table.display_result(&displayed);
                                status.set_label(&format!(
                                    "{} loaded: {} rows in {} ms",
                                    mode.label(),
                                    displayed.row_count,
                                    displayed.execution_time.as_millis()
                                ));
                            }
                            Err(err) => {
                                result_table.display_result(&dba_info_result(&format!(
                                    "{} load failed. {}\nTip: V$RMAN_BACKUP_JOB_DETAILS / V$BACKUP_SET_DETAILS privileges may be required.",
                                    mode.label(),
                                    err
                                )));
                                status.set_label("RMAN snapshot load failed");
                            }
                        }

                        if let Some((queued_mode, queued_lookback_text, queued_attention_only)) =
                            pending_request.take()
                        {
                            if dialog.shown() {
                                let _ = sender.send(RmanMessage::LoadRequested {
                                    mode: queued_mode,
                                    lookback_text: queued_lookback_text,
                                    attention_only: queued_attention_only,
                                });
                                app::awake();
                            }
                        }
                    }
                    RmanMessage::ActionFinished(result) => {
                        set_cursor(Cursor::Default);
                        app::flush();
                        action_running = false;
                        jobs_btn.activate();
                        sets_btn.activate();
                        coverage_btn.activate();
                        lookback_input.activate();
                        attention_only_check.activate();
                        owner_input.activate();
                        job_input.activate();
                        run_backup_btn.activate();
                        run_restore_btn.activate();

                        match result {
                            Ok(message) => {
                                status.set_label(&message);
                                let (reload_mode, reload_lookback_text, reload_attention_only) =
                                    if let Some((
                                        queued_mode,
                                        queued_lookback_text,
                                        queued_attention_only,
                                    )) = pending_request.take()
                                    {
                                        (queued_mode, queued_lookback_text, queued_attention_only)
                                    } else {
                                        (
                                            current_mode,
                                            current_lookback_text.clone(),
                                            current_attention_only,
                                        )
                                    };
                                let _ = sender.send(RmanMessage::LoadRequested {
                                    mode: reload_mode,
                                    lookback_text: reload_lookback_text,
                                    attention_only: reload_attention_only,
                                });
                                app::awake();
                            }
                            Err(err) => {
                                status.set_label("RMAN action failed");
                                fltk::dialog::alert_default(&err);
                                if let Some((
                                    queued_mode,
                                    queued_lookback_text,
                                    queued_attention_only,
                                )) = pending_request.take()
                                {
                                    if dialog.shown() {
                                        let _ = sender.send(RmanMessage::LoadRequested {
                                            mode: queued_mode,
                                            lookback_text: queued_lookback_text,
                                            attention_only: queued_attention_only,
                                        });
                                        app::awake();
                                    }
                                }
                            }
                        }
                    }
                    RmanMessage::CloseRequested => {
                        pending_request = None;
                        dialog.hide();
                    }
                }
            }
        }

        set_cursor(Cursor::Default);
        app::flush();
        Window::delete(dialog);
    }

    pub fn show_awr_ash_dashboard(&self) {
        enum PerformanceMessage {
            LoadRequested {
                mode: PerfViewMode,
                ash_minutes_text: String,
                awr_hours_text: String,
                top_n_text: String,
                wait_only: bool,
                sql_id_text: String,
            },
            SnapshotLoaded {
                request_id: u64,
                mode: PerfViewMode,
                result: Result<QueryResult, String>,
            },
            CloseRequested,
        }

        let (sender, receiver) = mpsc::channel::<PerformanceMessage>();

        let current_group = fltk::group::Group::try_current();
        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let dialog_w = 1260;
        let dialog_h = 740;
        let mut dialog = Window::default()
            .with_size(dialog_w, dialog_h)
            .with_label("AWR / ASH Dashboard");
        center_on_main(&mut dialog);
        dialog.set_color(theme::panel_raised());
        dialog.make_modal(true);

        let mut root = Flex::default().with_pos(0, 0).with_size(dialog_w, dialog_h);
        root.set_type(FlexType::Column);
        root.set_margin(DIALOG_MARGIN);
        root.set_spacing(DIALOG_SPACING);

        let mut help = Frame::default().with_label(
            "ASH shows near real-time active session samples. AWR shows historical top SQL deltas by elapsed time.",
        );
        help.set_align(Align::Left | Align::Inside);
        help.set_label_color(theme::text_secondary());
        help.set_label_size((configured_ui_font_size().saturating_sub(1)).max(10));
        root.fixed(&help, LABEL_ROW_HEIGHT);

        let mut controls = Flex::default();
        controls.set_type(FlexType::Row);
        controls.set_spacing(DIALOG_SPACING);

        let mut ash_label = Frame::default().with_label("ASH Min:");
        ash_label.set_label_color(theme::text_primary());
        ash_label.set_align(Align::Inside | Align::Left);
        controls.fixed(&ash_label, 62);

        let mut ash_input = IntInput::default();
        ash_input.set_value("30");
        ash_input.set_color(theme::input_bg());
        ash_input.set_text_color(theme::text_primary());
        ash_input.set_tooltip("ASH lookback minutes (1-1440)");
        controls.fixed(&ash_input, 58);

        let mut awr_label = Frame::default().with_label("AWR Hour:");
        awr_label.set_label_color(theme::text_primary());
        awr_label.set_align(Align::Inside | Align::Left);
        controls.fixed(&awr_label, 72);

        let mut awr_input = IntInput::default();
        awr_input.set_value("24");
        awr_input.set_color(theme::input_bg());
        awr_input.set_text_color(theme::text_primary());
        awr_input.set_tooltip("AWR lookback hours (1-720)");
        controls.fixed(&awr_input, 62);

        let mut top_n_label = Frame::default().with_label("TopN:");
        top_n_label.set_label_color(theme::text_primary());
        top_n_label.set_align(Align::Inside | Align::Left);
        controls.fixed(&top_n_label, 44);

        let mut top_n_input = IntInput::default();
        top_n_input.set_value("20");
        top_n_input.set_color(theme::input_bg());
        top_n_input.set_text_color(theme::text_primary());
        top_n_input.set_tooltip("TopN limit (1-200)");
        controls.fixed(&top_n_input, 58);

        let mut sql_id_label = Frame::default().with_label("SQL_ID:");
        sql_id_label.set_label_color(theme::text_primary());
        sql_id_label.set_align(Align::Inside | Align::Left);
        controls.fixed(&sql_id_label, 52);

        let mut sql_id_input = Input::default();
        sql_id_input.set_color(theme::input_bg());
        sql_id_input.set_text_color(theme::text_primary());
        sql_id_input.set_tooltip("Optional SQL_ID filter");
        controls.fixed(&sql_id_input, 120);

        let mut wait_only_check = CheckButton::default().with_label("Wait only");
        wait_only_check.set_label_color(theme::text_primary());
        wait_only_check.set_tooltip("ASH views only");
        controls.fixed(&wait_only_check, 100);

        let mut ash_samples_btn = Button::default().with_label("ASH Samples");
        ash_samples_btn.set_color(theme::button_secondary());
        ash_samples_btn.set_label_color(theme::text_primary());
        ash_samples_btn.set_frame(FrameType::RFlatBox);
        controls.fixed(&ash_samples_btn, BUTTON_WIDTH_LARGE + 14);

        let mut ash_top_btn = Button::default().with_label("ASH Top SQL");
        ash_top_btn.set_color(theme::button_secondary());
        ash_top_btn.set_label_color(theme::text_primary());
        ash_top_btn.set_frame(FrameType::RFlatBox);
        controls.fixed(&ash_top_btn, BUTTON_WIDTH_LARGE + 16);

        let mut awr_top_btn = Button::default().with_label("AWR Top SQL");
        awr_top_btn.set_color(theme::button_secondary());
        awr_top_btn.set_label_color(theme::text_primary());
        awr_top_btn.set_frame(FrameType::RFlatBox);
        controls.fixed(&awr_top_btn, BUTTON_WIDTH_LARGE + 16);

        let controls_filler = Frame::default();
        controls.resizable(&controls_filler);

        let mut close_btn = Button::default().with_label("Close");
        close_btn.set_color(theme::button_subtle());
        close_btn.set_label_color(theme::text_primary());
        close_btn.set_frame(FrameType::RFlatBox);
        controls.fixed(&close_btn, BUTTON_WIDTH);

        controls.end();
        root.fixed(&controls, BUTTON_ROW_HEIGHT + 4);

        let mut result_table =
            ResultTableWidget::with_size(0, 0, dialog_w - DIALOG_MARGIN * 2, dialog_h - 190);
        result_table.set_max_cell_display_chars(420);
        let table_widget = result_table.get_widget();
        root.resizable(&table_widget);
        result_table.display_result(&dba_info_result("Press a button to load AWR/ASH metrics."));

        let mut status = Frame::default().with_label("Ready");
        status.set_label_color(theme::text_secondary());
        status.set_align(Align::Left | Align::Inside);
        root.fixed(&status, LABEL_ROW_HEIGHT);

        root.end();
        dialog.end();
        fltk::group::Group::set_current(current_group.as_ref());

        let sender_ash_samples = sender.clone();
        let ash_input_for_samples = ash_input.clone();
        let awr_input_for_samples = awr_input.clone();
        let top_n_for_samples = top_n_input.clone();
        let wait_for_samples = wait_only_check.clone();
        let sql_id_for_samples = sql_id_input.clone();
        ash_samples_btn.set_callback(move |_| {
            let _ = sender_ash_samples.send(PerformanceMessage::LoadRequested {
                mode: PerfViewMode::AshSamples,
                ash_minutes_text: ash_input_for_samples.value(),
                awr_hours_text: awr_input_for_samples.value(),
                top_n_text: top_n_for_samples.value(),
                wait_only: wait_for_samples.value(),
                sql_id_text: sql_id_for_samples.value(),
            });
            app::awake();
        });

        let sender_ash_top = sender.clone();
        let ash_input_for_top = ash_input.clone();
        let awr_input_for_top = awr_input.clone();
        let top_n_for_top = top_n_input.clone();
        let wait_for_top = wait_only_check.clone();
        let sql_id_for_top = sql_id_input.clone();
        ash_top_btn.set_callback(move |_| {
            let _ = sender_ash_top.send(PerformanceMessage::LoadRequested {
                mode: PerfViewMode::AshTopSql,
                ash_minutes_text: ash_input_for_top.value(),
                awr_hours_text: awr_input_for_top.value(),
                top_n_text: top_n_for_top.value(),
                wait_only: wait_for_top.value(),
                sql_id_text: sql_id_for_top.value(),
            });
            app::awake();
        });

        let sender_awr_top = sender.clone();
        let ash_input_for_awr = ash_input.clone();
        let awr_input_for_awr = awr_input.clone();
        let top_n_for_awr = top_n_input.clone();
        let wait_for_awr = wait_only_check.clone();
        let sql_id_for_awr = sql_id_input.clone();
        awr_top_btn.set_callback(move |_| {
            let _ = sender_awr_top.send(PerformanceMessage::LoadRequested {
                mode: PerfViewMode::AwrTopSql,
                ash_minutes_text: ash_input_for_awr.value(),
                awr_hours_text: awr_input_for_awr.value(),
                top_n_text: top_n_for_awr.value(),
                wait_only: wait_for_awr.value(),
                sql_id_text: sql_id_for_awr.value(),
            });
            app::awake();
        });

        let sender_close = sender.clone();
        close_btn.set_callback(move |_| {
            let _ = sender_close.send(PerformanceMessage::CloseRequested);
            app::awake();
        });

        dialog.show();
        let _ = dialog.take_focus();
        let _ = ash_samples_btn.take_focus();

        let _ = sender.send(PerformanceMessage::LoadRequested {
            mode: PerfViewMode::AshSamples,
            ash_minutes_text: ash_input.value(),
            awr_hours_text: awr_input.value(),
            top_n_text: top_n_input.value(),
            wait_only: wait_only_check.value(),
            sql_id_text: sql_id_input.value(),
        });
        app::awake();

        let mut latest_request_id = 0u64;
        let mut loading_snapshot = false;
        let mut pending_request: Option<(PerfViewMode, String, String, String, bool, String)> =
            None;
        let mut last_table_selection = (i32::MIN, i32::MIN, i32::MIN, i32::MIN);
        let mut last_snapshot_columns: Vec<String> = Vec::new();
        while dialog.shown() {
            app::wait();

            while let Ok(message) = receiver.try_recv() {
                match message {
                    PerformanceMessage::LoadRequested {
                        mode,
                        ash_minutes_text,
                        awr_hours_text,
                        top_n_text,
                        wait_only,
                        sql_id_text,
                    } => {
                        if loading_snapshot {
                            pending_request = Some((
                                mode,
                                ash_minutes_text,
                                awr_hours_text,
                                top_n_text,
                                wait_only,
                                sql_id_text,
                            ));
                            status.set_label(&format!(
                                "{} request queued (will run after current load)",
                                mode.label()
                            ));
                            continue;
                        }

                        let ash_minutes =
                            if matches!(mode, PerfViewMode::AshSamples | PerfViewMode::AshTopSql) {
                                match parse_bounded_positive_u32(
                                    &ash_minutes_text,
                                    "ASH minutes",
                                    ASH_LOOKBACK_MAX_MINUTES,
                                ) {
                                    Ok(value) => value,
                                    Err(err) => {
                                        fltk::dialog::alert_default(&err);
                                        continue;
                                    }
                                }
                            } else {
                                1
                            };
                        let awr_hours = if matches!(mode, PerfViewMode::AwrTopSql) {
                            match parse_bounded_positive_u32(
                                &awr_hours_text,
                                "AWR hours",
                                AWR_LOOKBACK_MAX_HOURS,
                            ) {
                                Ok(value) => value,
                                Err(err) => {
                                    fltk::dialog::alert_default(&err);
                                    continue;
                                }
                            }
                        } else {
                            1
                        };
                        let top_n =
                            if matches!(mode, PerfViewMode::AshTopSql | PerfViewMode::AwrTopSql) {
                                match parse_bounded_positive_u32(
                                    &top_n_text,
                                    "TopN",
                                    PERFORMANCE_TOP_N_MAX,
                                ) {
                                    Ok(value) => value,
                                    Err(err) => {
                                        fltk::dialog::alert_default(&err);
                                        continue;
                                    }
                                }
                            } else {
                                50
                            };
                        let sql_id_filter = match normalize_optional_sql_id(&sql_id_text) {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        latest_request_id = latest_request_id.saturating_add(1);
                        let request_id = latest_request_id;

                        set_cursor(Cursor::Wait);
                        app::flush();
                        loading_snapshot = true;
                        ash_samples_btn.deactivate();
                        ash_top_btn.deactivate();
                        awr_top_btn.deactivate();
                        ash_input.deactivate();
                        awr_input.deactivate();
                        top_n_input.deactivate();
                        sql_id_input.deactivate();
                        wait_only_check.deactivate();
                        status.set_label(&format!("Loading {}...", mode.label()));
                        result_table.display_result(&dba_info_result(&format!(
                            "Loading {}...",
                            mode.label()
                        )));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Loading {}", mode.label()),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => match mode {
                                        PerfViewMode::AshSamples => {
                                            QueryExecutor::get_ash_session_activity_snapshot(
                                                db_conn.as_ref(),
                                                ash_minutes,
                                                wait_only,
                                                sql_id_filter.as_deref(),
                                            )
                                            .map_err(|err| {
                                                format!(
                                                    "Failed to load ASH session activity snapshot: {err}"
                                                )
                                            })
                                        }
                                        PerfViewMode::AshTopSql => {
                                            QueryExecutor::get_ash_top_sql_snapshot(
                                                db_conn.as_ref(),
                                                ash_minutes,
                                                top_n,
                                                wait_only,
                                                sql_id_filter.as_deref(),
                                            )
                                            .map_err(|err| {
                                                format!("Failed to load ASH top SQL snapshot: {err}")
                                            })
                                        }
                                        PerfViewMode::AwrTopSql => {
                                            QueryExecutor::get_awr_top_sql_snapshot(
                                                db_conn.as_ref(),
                                                awr_hours,
                                                top_n,
                                                sql_id_filter.as_deref(),
                                            )
                                            .map_err(|err| {
                                                format!("Failed to load AWR top SQL snapshot: {err}")
                                            })
                                        }
                                    },
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(PerformanceMessage::SnapshotLoaded {
                                request_id,
                                mode,
                                result,
                            });
                            app::awake();
                        });
                    }
                    PerformanceMessage::SnapshotLoaded {
                        request_id,
                        mode,
                        result,
                    } => {
                        if request_id != latest_request_id {
                            continue;
                        }

                        set_cursor(Cursor::Default);
                        app::flush();
                        loading_snapshot = false;
                        ash_samples_btn.activate();
                        ash_top_btn.activate();
                        awr_top_btn.activate();
                        ash_input.activate();
                        awr_input.activate();
                        top_n_input.activate();
                        sql_id_input.activate();
                        wait_only_check.activate();

                        match result {
                            Ok(snapshot) => {
                                last_snapshot_columns = snapshot
                                    .columns
                                    .iter()
                                    .map(|column| column.name.to_uppercase())
                                    .collect();
                                result_table.display_result(&snapshot);
                                status.set_label(&format!(
                                    "{} loaded: {} rows in {} ms",
                                    mode.label(),
                                    snapshot.row_count,
                                    snapshot.execution_time.as_millis()
                                ));
                            }
                            Err(err) => {
                                last_snapshot_columns.clear();
                                result_table.display_result(&dba_info_result(&format!(
                                    "{} load failed. {}\nTip: AWR/ASH queries can require Diagnostic Pack privileges.",
                                    mode.label(),
                                    err
                                )));
                                status.set_label("AWR/ASH snapshot load failed");
                            }
                        }

                        if let Some((
                            queued_mode,
                            queued_ash_minutes_text,
                            queued_awr_hours_text,
                            queued_top_n_text,
                            queued_wait_only,
                            queued_sql_id_text,
                        )) = pending_request.take()
                        {
                            if dialog.shown() {
                                let _ = sender.send(PerformanceMessage::LoadRequested {
                                    mode: queued_mode,
                                    ash_minutes_text: queued_ash_minutes_text,
                                    awr_hours_text: queued_awr_hours_text,
                                    top_n_text: queued_top_n_text,
                                    wait_only: queued_wait_only,
                                    sql_id_text: queued_sql_id_text,
                                });
                                app::awake();
                            }
                        }
                    }
                    PerformanceMessage::CloseRequested => {
                        pending_request = None;
                        dialog.hide();
                    }
                }
            }

            let selection = table_widget.get_selection();
            if selection != last_table_selection {
                last_table_selection = selection;
                if let Some(selected_index) = current_selected_row_index(selection) {
                    if let Some(row) = result_table.row_values(selected_index) {
                        if let Some(sql_id) =
                            column_value_by_name(&row, &last_snapshot_columns, "SQL_ID")
                        {
                            let normalized = sql_id.trim().to_uppercase();
                            if normalize_optional_sql_id(&normalized)
                                .ok()
                                .flatten()
                                .is_some()
                            {
                                sql_id_input.set_value(&normalized);
                            }
                        }
                    }
                }
            }
        }

        set_cursor(Cursor::Default);
        app::flush();
        Window::delete(dialog);
    }

    pub fn show_data_guard_dashboard(&self) {
        enum DataGuardMessage {
            LoadRequested {
                mode: DataGuardViewMode,
                attention_only: bool,
            },
            StartApplyRequested,
            StopApplyRequested,
            SwitchoverRequested {
                target_text: String,
            },
            FailoverRequested {
                target_text: String,
            },
            ForceLogSwitchRequested,
            SnapshotLoaded {
                request_id: u64,
                mode: DataGuardViewMode,
                result: Result<QueryResult, String>,
            },
            ActionFinished(Result<String, String>),
            CloseRequested,
        }

        let (sender, receiver) = mpsc::channel::<DataGuardMessage>();

        let current_group = fltk::group::Group::try_current();
        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let dialog_w = 1200;
        let dialog_h = 700;
        let mut dialog = Window::default()
            .with_size(dialog_w, dialog_h)
            .with_label("Data Guard Dashboard");
        center_on_main(&mut dialog);
        dialog.set_color(theme::panel_raised());
        dialog.make_modal(true);

        let mut root = Flex::default().with_pos(0, 0).with_size(dialog_w, dialog_h);
        root.set_type(FlexType::Column);
        root.set_margin(DIALOG_MARGIN);
        root.set_spacing(DIALOG_SPACING);

        let mut help = Frame::default().with_label(
            "Tracks role/lag/transport/apply status across Data Guard views and can force a log switch on primary.",
        );
        help.set_align(Align::Left | Align::Inside);
        help.set_label_color(theme::text_secondary());
        help.set_label_size((configured_ui_font_size().saturating_sub(1)).max(10));
        root.fixed(&help, LABEL_ROW_HEIGHT);

        let mut controls = Flex::default();
        controls.set_type(FlexType::Row);
        controls.set_spacing(DIALOG_SPACING);

        let mut attention_only_check = CheckButton::default().with_label("Attention only");
        attention_only_check.set_label_color(theme::text_primary());
        attention_only_check.set_tooltip("Applies to Destinations view");
        controls.fixed(&attention_only_check, 128);

        let mut overview_btn = Button::default().with_label("Overview");
        overview_btn.set_color(theme::button_secondary());
        overview_btn.set_label_color(theme::text_primary());
        overview_btn.set_frame(FrameType::RFlatBox);
        controls.fixed(&overview_btn, BUTTON_WIDTH_LARGE);

        let mut dest_btn = Button::default().with_label("Destinations");
        dest_btn.set_color(theme::button_secondary());
        dest_btn.set_label_color(theme::text_primary());
        dest_btn.set_frame(FrameType::RFlatBox);
        controls.fixed(&dest_btn, BUTTON_WIDTH_LARGE + 12);

        let mut apply_btn = Button::default().with_label("Apply");
        apply_btn.set_color(theme::button_secondary());
        apply_btn.set_label_color(theme::text_primary());
        apply_btn.set_frame(FrameType::RFlatBox);
        controls.fixed(&apply_btn, BUTTON_WIDTH_LARGE);

        let mut gap_btn = Button::default().with_label("Archive Gap");
        gap_btn.set_color(theme::button_secondary());
        gap_btn.set_label_color(theme::text_primary());
        gap_btn.set_frame(FrameType::RFlatBox);
        controls.fixed(&gap_btn, BUTTON_WIDTH_LARGE + 16);

        let mut force_switch_btn = Button::default().with_label("Force Log Switch");
        force_switch_btn.set_color(theme::button_warning());
        force_switch_btn.set_label_color(theme::text_primary());
        force_switch_btn.set_frame(FrameType::RFlatBox);
        force_switch_btn.set_tooltip("Load Overview. Enabled only for PRIMARY role.");
        force_switch_btn.deactivate();
        controls.fixed(&force_switch_btn, BUTTON_WIDTH_LARGE + 30);

        let controls_filler = Frame::default();
        controls.resizable(&controls_filler);

        let mut close_btn = Button::default().with_label("Close");
        close_btn.set_color(theme::button_subtle());
        close_btn.set_label_color(theme::text_primary());
        close_btn.set_frame(FrameType::RFlatBox);
        controls.fixed(&close_btn, BUTTON_WIDTH);

        controls.end();
        root.fixed(&controls, BUTTON_ROW_HEIGHT + 4);

        let mut action_row = Flex::default();
        action_row.set_type(FlexType::Row);
        action_row.set_spacing(DIALOG_SPACING);

        let mut target_label = Frame::default().with_label("Target:");
        target_label.set_label_color(theme::text_primary());
        target_label.set_align(Align::Inside | Align::Left);
        action_row.fixed(&target_label, 48);

        let mut target_input = Input::default();
        target_input.set_color(theme::input_bg());
        target_input.set_text_color(theme::text_primary());
        target_input.set_tooltip("DB_UNIQUE_NAME for switchover/failover");
        action_row.fixed(&target_input, 180);

        let mut start_apply_btn = Button::default().with_label("Start Apply");
        start_apply_btn.set_color(theme::button_success());
        start_apply_btn.set_label_color(theme::text_primary());
        start_apply_btn.set_frame(FrameType::RFlatBox);
        action_row.fixed(&start_apply_btn, BUTTON_WIDTH_LARGE + 16);

        let mut stop_apply_btn = Button::default().with_label("Stop Apply");
        stop_apply_btn.set_color(theme::button_warning());
        stop_apply_btn.set_label_color(theme::text_primary());
        stop_apply_btn.set_frame(FrameType::RFlatBox);
        action_row.fixed(&stop_apply_btn, BUTTON_WIDTH_LARGE + 12);

        let mut switchover_btn = Button::default().with_label("Switchover");
        switchover_btn.set_color(theme::button_warning());
        switchover_btn.set_label_color(theme::text_primary());
        switchover_btn.set_frame(FrameType::RFlatBox);
        action_row.fixed(&switchover_btn, BUTTON_WIDTH_LARGE + 12);

        let mut failover_btn = Button::default().with_label("Failover");
        failover_btn.set_color(theme::button_danger());
        failover_btn.set_label_color(theme::text_primary());
        failover_btn.set_frame(FrameType::RFlatBox);
        action_row.fixed(&failover_btn, BUTTON_WIDTH_LARGE + 8);

        let action_filler = Frame::default();
        action_row.resizable(&action_filler);
        action_row.end();
        root.fixed(&action_row, BUTTON_ROW_HEIGHT + 4);

        let mut result_table =
            ResultTableWidget::with_size(0, 0, dialog_w - DIALOG_MARGIN * 2, dialog_h - 232);
        result_table.set_max_cell_display_chars(360);
        let table_widget = result_table.get_widget();
        root.resizable(&table_widget);
        result_table.display_result(&dba_info_result(
            "Press a button to load Data Guard metrics.",
        ));

        let mut status = Frame::default().with_label("Ready");
        status.set_label_color(theme::text_secondary());
        status.set_align(Align::Left | Align::Inside);
        root.fixed(&status, LABEL_ROW_HEIGHT);

        root.end();
        dialog.end();
        fltk::group::Group::set_current(current_group.as_ref());

        let sender_overview = sender.clone();
        let attention_for_overview = attention_only_check.clone();
        overview_btn.set_callback(move |_| {
            let _ = sender_overview.send(DataGuardMessage::LoadRequested {
                mode: DataGuardViewMode::Overview,
                attention_only: attention_for_overview.value(),
            });
            app::awake();
        });

        let sender_dest = sender.clone();
        let attention_for_dest = attention_only_check.clone();
        dest_btn.set_callback(move |_| {
            let _ = sender_dest.send(DataGuardMessage::LoadRequested {
                mode: DataGuardViewMode::Destinations,
                attention_only: attention_for_dest.value(),
            });
            app::awake();
        });

        let sender_apply = sender.clone();
        let attention_for_apply = attention_only_check.clone();
        apply_btn.set_callback(move |_| {
            let _ = sender_apply.send(DataGuardMessage::LoadRequested {
                mode: DataGuardViewMode::Apply,
                attention_only: attention_for_apply.value(),
            });
            app::awake();
        });

        let sender_gap = sender.clone();
        let attention_for_gap = attention_only_check.clone();
        gap_btn.set_callback(move |_| {
            let _ = sender_gap.send(DataGuardMessage::LoadRequested {
                mode: DataGuardViewMode::ArchiveGap,
                attention_only: attention_for_gap.value(),
            });
            app::awake();
        });

        let sender_start_apply = sender.clone();
        start_apply_btn.set_callback(move |_| {
            let _ = sender_start_apply.send(DataGuardMessage::StartApplyRequested);
            app::awake();
        });

        let sender_stop_apply = sender.clone();
        stop_apply_btn.set_callback(move |_| {
            let _ = sender_stop_apply.send(DataGuardMessage::StopApplyRequested);
            app::awake();
        });

        let sender_switchover = sender.clone();
        let target_input_for_switchover = target_input.clone();
        switchover_btn.set_callback(move |_| {
            let _ = sender_switchover.send(DataGuardMessage::SwitchoverRequested {
                target_text: target_input_for_switchover.value(),
            });
            app::awake();
        });

        let sender_failover = sender.clone();
        let target_input_for_failover = target_input.clone();
        failover_btn.set_callback(move |_| {
            let _ = sender_failover.send(DataGuardMessage::FailoverRequested {
                target_text: target_input_for_failover.value(),
            });
            app::awake();
        });

        let sender_force = sender.clone();
        force_switch_btn.set_callback(move |_| {
            let _ = sender_force.send(DataGuardMessage::ForceLogSwitchRequested);
            app::awake();
        });

        let sender_close = sender.clone();
        close_btn.set_callback(move |_| {
            let _ = sender_close.send(DataGuardMessage::CloseRequested);
            app::awake();
        });

        dialog.show();
        let _ = dialog.take_focus();
        let _ = overview_btn.take_focus();

        let _ = sender.send(DataGuardMessage::LoadRequested {
            mode: DataGuardViewMode::Overview,
            attention_only: attention_only_check.value(),
        });
        app::awake();

        let mut latest_request_id = 0u64;
        let mut current_mode = DataGuardViewMode::Overview;
        let mut current_database_role: Option<String> = None;
        let mut current_db_unique_name: Option<String> = None;
        let mut overview_loaded = false;
        let mut loading_snapshot = false;
        let mut action_running = false;
        let mut pending_load_request: Option<(DataGuardViewMode, bool)> = None;
        refresh_dataguard_force_switch_button(
            &mut force_switch_btn,
            current_database_role.as_deref(),
            overview_loaded,
            loading_snapshot,
            action_running,
        );
        while dialog.shown() {
            app::wait();

            while let Ok(message) = receiver.try_recv() {
                match message {
                    DataGuardMessage::LoadRequested {
                        mode,
                        attention_only,
                    } => {
                        if loading_snapshot || action_running {
                            pending_load_request = Some((mode, attention_only));
                            status
                                .set_label(&format!("Data Guard {} request queued", mode.label()));
                            continue;
                        }

                        current_mode = mode;
                        latest_request_id = latest_request_id.saturating_add(1);
                        let request_id = latest_request_id;

                        set_cursor(Cursor::Wait);
                        app::flush();
                        loading_snapshot = true;
                        attention_only_check.deactivate();
                        overview_btn.deactivate();
                        dest_btn.deactivate();
                        apply_btn.deactivate();
                        gap_btn.deactivate();
                        target_input.deactivate();
                        start_apply_btn.deactivate();
                        stop_apply_btn.deactivate();
                        switchover_btn.deactivate();
                        failover_btn.deactivate();
                        refresh_dataguard_force_switch_button(
                            &mut force_switch_btn,
                            current_database_role.as_deref(),
                            overview_loaded,
                            loading_snapshot,
                            action_running,
                        );
                        status.set_label(&format!("Loading Data Guard {}...", mode.label()));
                        result_table.display_result(&dba_info_result(&format!(
                            "Loading Data Guard {}...",
                            mode.label()
                        )));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Loading Data Guard {}", mode.label()),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => match mode {
                                        DataGuardViewMode::Overview => {
                                            QueryExecutor::get_dataguard_overview_snapshot(
                                                db_conn.as_ref(),
                                            )
                                            .map_err(|err| {
                                                format!(
                                                    "Failed to load Data Guard overview snapshot: {err}"
                                                )
                                            })
                                        }
                                        DataGuardViewMode::Destinations => {
                                            QueryExecutor::get_dataguard_destination_snapshot(
                                                db_conn.as_ref(),
                                                attention_only,
                                            )
                                            .map_err(|err| {
                                                format!(
                                                    "Failed to load Data Guard destination snapshot: {err}"
                                                )
                                            })
                                        }
                                        DataGuardViewMode::Apply => {
                                            QueryExecutor::get_dataguard_apply_process_snapshot(
                                                db_conn.as_ref(),
                                            )
                                            .map_err(|err| {
                                                format!(
                                                    "Failed to load Data Guard apply process snapshot: {err}"
                                                )
                                            })
                                        }
                                        DataGuardViewMode::ArchiveGap => {
                                            QueryExecutor::get_dataguard_archive_gap_snapshot(
                                                db_conn.as_ref(),
                                            )
                                            .map_err(|err| {
                                                format!(
                                                    "Failed to load Data Guard archive gap snapshot: {err}"
                                                )
                                            })
                                        }
                                    },
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(DataGuardMessage::SnapshotLoaded {
                                request_id,
                                mode,
                                result,
                            });
                            app::awake();
                        });
                    }
                    DataGuardMessage::StartApplyRequested => {
                        if loading_snapshot || action_running {
                            status.set_label("Data Guard request already in progress");
                            continue;
                        }
                        if !overview_loaded {
                            fltk::dialog::alert_default(
                                "Load Data Guard Overview first to verify role before apply control.",
                            );
                            continue;
                        }
                        if !dataguard_role_allows_apply_control(current_database_role.as_deref()) {
                            let role = current_database_role.as_deref().unwrap_or("UNKNOWN");
                            fltk::dialog::alert_default(&format!(
                                "Start apply is supported for PHYSICAL STANDBY role (current: {}).",
                                role
                            ));
                            continue;
                        }

                        let confirm = fltk::dialog::choice2_default(
                            "Start managed standby apply now?",
                            "Cancel",
                            "Start",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        action_running = true;
                        attention_only_check.deactivate();
                        overview_btn.deactivate();
                        dest_btn.deactivate();
                        apply_btn.deactivate();
                        gap_btn.deactivate();
                        target_input.deactivate();
                        start_apply_btn.deactivate();
                        stop_apply_btn.deactivate();
                        switchover_btn.deactivate();
                        failover_btn.deactivate();
                        refresh_dataguard_force_switch_button(
                            &mut force_switch_btn,
                            current_database_role.as_deref(),
                            overview_loaded,
                            loading_snapshot,
                            action_running,
                        );
                        status.set_label("Starting Data Guard apply...");

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                "Starting Data Guard apply",
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => {
                                        QueryExecutor::start_dataguard_apply(db_conn.as_ref())
                                            .map(|_| "Data Guard apply started".to_string())
                                            .map_err(|err| {
                                                format!("Failed to start Data Guard apply: {err}")
                                            })
                                    }
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(DataGuardMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    DataGuardMessage::StopApplyRequested => {
                        if loading_snapshot || action_running {
                            status.set_label("Data Guard request already in progress");
                            continue;
                        }
                        if !overview_loaded {
                            fltk::dialog::alert_default(
                                "Load Data Guard Overview first to verify role before apply control.",
                            );
                            continue;
                        }
                        if !dataguard_role_allows_apply_control(current_database_role.as_deref()) {
                            let role = current_database_role.as_deref().unwrap_or("UNKNOWN");
                            fltk::dialog::alert_default(&format!(
                                "Stop apply is supported for PHYSICAL STANDBY role (current: {}).",
                                role
                            ));
                            continue;
                        }

                        let confirm = fltk::dialog::choice2_default(
                            "Stop managed standby apply now?",
                            "Cancel",
                            "Stop",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        action_running = true;
                        attention_only_check.deactivate();
                        overview_btn.deactivate();
                        dest_btn.deactivate();
                        apply_btn.deactivate();
                        gap_btn.deactivate();
                        target_input.deactivate();
                        start_apply_btn.deactivate();
                        stop_apply_btn.deactivate();
                        switchover_btn.deactivate();
                        failover_btn.deactivate();
                        refresh_dataguard_force_switch_button(
                            &mut force_switch_btn,
                            current_database_role.as_deref(),
                            overview_loaded,
                            loading_snapshot,
                            action_running,
                        );
                        status.set_label("Stopping Data Guard apply...");

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                "Stopping Data Guard apply",
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => {
                                        QueryExecutor::stop_dataguard_apply(db_conn.as_ref())
                                            .map(|_| "Data Guard apply stopped".to_string())
                                            .map_err(|err| {
                                                format!("Failed to stop Data Guard apply: {err}")
                                            })
                                    }
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(DataGuardMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    DataGuardMessage::SwitchoverRequested { target_text } => {
                        if loading_snapshot || action_running {
                            status.set_label("Data Guard request already in progress");
                            continue;
                        }
                        if !overview_loaded {
                            fltk::dialog::alert_default(
                                "Load Data Guard Overview first to validate target information.",
                            );
                            continue;
                        }

                        let role = current_database_role.as_deref().unwrap_or("UNKNOWN");
                        if role != "PRIMARY" && role != "PHYSICAL STANDBY" {
                            fltk::dialog::alert_default(&format!(
                                "Switchover is supported only on PRIMARY or PHYSICAL STANDBY role (current: {}).",
                                role
                            ));
                            continue;
                        }

                        let target =
                            match normalize_required_identifier(&target_text, "Target DB name") {
                                Ok(value) => value,
                                Err(err) => {
                                    fltk::dialog::alert_default(&err);
                                    continue;
                                }
                            };
                        if current_db_unique_name.as_deref() == Some(target.as_str()) {
                            fltk::dialog::alert_default(
                                "Target DB_UNIQUE_NAME must be different from current database.",
                            );
                            continue;
                        }

                        let confirm = fltk::dialog::choice2_default(
                            &format!("Execute switchover to {}?", target),
                            "Cancel",
                            "Execute",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        action_running = true;
                        attention_only_check.deactivate();
                        overview_btn.deactivate();
                        dest_btn.deactivate();
                        apply_btn.deactivate();
                        gap_btn.deactivate();
                        target_input.deactivate();
                        start_apply_btn.deactivate();
                        stop_apply_btn.deactivate();
                        switchover_btn.deactivate();
                        failover_btn.deactivate();
                        refresh_dataguard_force_switch_button(
                            &mut force_switch_btn,
                            current_database_role.as_deref(),
                            overview_loaded,
                            loading_snapshot,
                            action_running,
                        );
                        status.set_label(&format!("Executing switchover to {}...", target));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                format!("Executing Data Guard switchover to {}", target),
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::switchover_dataguard(
                                        db_conn.as_ref(),
                                        &target,
                                    )
                                    .map(|_| {
                                        format!("Data Guard switchover executed to {}", target)
                                    })
                                    .map_err(|err| {
                                        format!(
                                            "Failed to execute Data Guard switchover to {}: {err}",
                                            target
                                        )
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(DataGuardMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    DataGuardMessage::FailoverRequested { target_text } => {
                        if loading_snapshot || action_running {
                            status.set_label("Data Guard request already in progress");
                            continue;
                        }
                        if !overview_loaded {
                            fltk::dialog::alert_default(
                                "Load Data Guard Overview first to validate target information.",
                            );
                            continue;
                        }

                        let role = current_database_role.as_deref().unwrap_or("UNKNOWN");
                        if role != "PHYSICAL STANDBY" {
                            fltk::dialog::alert_default(&format!(
                                "Failover is supported only on PHYSICAL STANDBY role (current: {}).",
                                role
                            ));
                            continue;
                        }

                        let target =
                            match normalize_required_identifier(&target_text, "Target DB name") {
                                Ok(value) => value,
                                Err(err) => {
                                    fltk::dialog::alert_default(&err);
                                    continue;
                                }
                            };
                        if current_db_unique_name.as_deref() == Some(target.as_str()) {
                            fltk::dialog::alert_default(
                                "Target DB_UNIQUE_NAME must be different from current database.",
                            );
                            continue;
                        }

                        let confirm = fltk::dialog::choice2_default(
                            &format!("Execute failover to {}?", target),
                            "Cancel",
                            "Execute",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        action_running = true;
                        attention_only_check.deactivate();
                        overview_btn.deactivate();
                        dest_btn.deactivate();
                        apply_btn.deactivate();
                        gap_btn.deactivate();
                        target_input.deactivate();
                        start_apply_btn.deactivate();
                        stop_apply_btn.deactivate();
                        switchover_btn.deactivate();
                        failover_btn.deactivate();
                        refresh_dataguard_force_switch_button(
                            &mut force_switch_btn,
                            current_database_role.as_deref(),
                            overview_loaded,
                            loading_snapshot,
                            action_running,
                        );
                        status.set_label(&format!("Executing failover to {}...", target));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result =
                                match try_lock_connection_with_activity(
                                    &connection,
                                    format!("Executing Data Guard failover to {}", target),
                                ) {
                                    Some(mut guard) => match guard.require_live_connection() {
                                        Ok(db_conn) => QueryExecutor::failover_dataguard(
                                            db_conn.as_ref(),
                                            &target,
                                        )
                                        .map(|_| {
                                            format!("Data Guard failover executed to {}", target)
                                        })
                                        .map_err(|err| {
                                            format!(
                                            "Failed to execute Data Guard failover to {}: {err}",
                                            target
                                        )
                                        }),
                                        Err(message) => Err(message),
                                    },
                                    None => Err(format_connection_busy_message()),
                                };

                            let _ = sender_result.send(DataGuardMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    DataGuardMessage::ForceLogSwitchRequested => {
                        if loading_snapshot || action_running {
                            status.set_label("Data Guard request already in progress");
                            continue;
                        }

                        if current_database_role.as_deref() != Some("PRIMARY") {
                            let role = current_database_role.as_deref().unwrap_or("UNKNOWN");
                            fltk::dialog::alert_default(&format!(
                                "Force log switch is available only on PRIMARY role (current: {}).",
                                role
                            ));
                            continue;
                        }

                        let confirm = fltk::dialog::choice2_default(
                            "Execute 'ALTER SYSTEM ARCHIVE LOG CURRENT' now?",
                            "Cancel",
                            "Execute",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        action_running = true;
                        attention_only_check.deactivate();
                        overview_btn.deactivate();
                        dest_btn.deactivate();
                        apply_btn.deactivate();
                        gap_btn.deactivate();
                        target_input.deactivate();
                        start_apply_btn.deactivate();
                        stop_apply_btn.deactivate();
                        switchover_btn.deactivate();
                        failover_btn.deactivate();
                        refresh_dataguard_force_switch_button(
                            &mut force_switch_btn,
                            current_database_role.as_deref(),
                            overview_loaded,
                            loading_snapshot,
                            action_running,
                        );
                        status.set_label("Forcing archive log switch...");

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                "Forcing archive log switch",
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => {
                                        QueryExecutor::force_archive_log_switch(db_conn.as_ref())
                                            .map(|_| "Archive log switch completed".to_string())
                                            .map_err(|err| {
                                                format!("Failed to force archive log switch: {err}")
                                            })
                                    }
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(DataGuardMessage::ActionFinished(result));
                            app::awake();
                        });
                    }
                    DataGuardMessage::SnapshotLoaded {
                        request_id,
                        mode,
                        result,
                    } => {
                        if request_id != latest_request_id {
                            continue;
                        }

                        set_cursor(Cursor::Default);
                        app::flush();
                        loading_snapshot = false;
                        attention_only_check.activate();
                        overview_btn.activate();
                        dest_btn.activate();
                        apply_btn.activate();
                        gap_btn.activate();
                        target_input.activate();
                        start_apply_btn.activate();
                        stop_apply_btn.activate();
                        switchover_btn.activate();
                        failover_btn.activate();

                        match result {
                            Ok(snapshot) => {
                                if matches!(mode, DataGuardViewMode::Overview) {
                                    overview_loaded = true;
                                    current_database_role = dataguard_role_from_snapshot(&snapshot);
                                    current_db_unique_name =
                                        dataguard_db_unique_name_from_snapshot(&snapshot);
                                }
                                result_table.display_result(&snapshot);
                                let role_suffix = if matches!(mode, DataGuardViewMode::Overview) {
                                    let role = current_database_role
                                        .as_deref()
                                        .map(|value| format!(", role={value}"))
                                        .unwrap_or_default();
                                    let db_unique_name = current_db_unique_name
                                        .as_deref()
                                        .map(|value| format!(", db_unique_name={value}"))
                                        .unwrap_or_default();
                                    format!("{role}{db_unique_name}")
                                } else {
                                    String::new()
                                };
                                status.set_label(&format!(
                                    "Data Guard {} loaded: {} rows in {} ms{}",
                                    mode.label(),
                                    snapshot.row_count,
                                    snapshot.execution_time.as_millis(),
                                    role_suffix
                                ));
                            }
                            Err(err) => {
                                if matches!(mode, DataGuardViewMode::Overview) {
                                    overview_loaded = false;
                                    current_database_role = None;
                                    current_db_unique_name = None;
                                }
                                result_table.display_result(&dba_info_result(&format!(
                                    "Data Guard {} load failed. {}\nTip: V$DATAGUARD_STATS / V$ARCHIVE_DEST_STATUS views can require elevated privileges.",
                                    mode.label(),
                                    err
                                )));
                                status.set_label("Data Guard snapshot load failed");
                            }
                        }

                        refresh_dataguard_force_switch_button(
                            &mut force_switch_btn,
                            current_database_role.as_deref(),
                            overview_loaded,
                            loading_snapshot,
                            action_running,
                        );

                        if let Some((queued_mode, queued_attention_only)) =
                            pending_load_request.take()
                        {
                            if dialog.shown() {
                                let _ = sender.send(DataGuardMessage::LoadRequested {
                                    mode: queued_mode,
                                    attention_only: queued_attention_only,
                                });
                                app::awake();
                            }
                        }
                    }
                    DataGuardMessage::ActionFinished(result) => {
                        set_cursor(Cursor::Default);
                        app::flush();
                        action_running = false;
                        attention_only_check.activate();
                        overview_btn.activate();
                        dest_btn.activate();
                        apply_btn.activate();
                        gap_btn.activate();
                        target_input.activate();
                        start_apply_btn.activate();
                        stop_apply_btn.activate();
                        switchover_btn.activate();
                        failover_btn.activate();
                        refresh_dataguard_force_switch_button(
                            &mut force_switch_btn,
                            current_database_role.as_deref(),
                            overview_loaded,
                            loading_snapshot,
                            action_running,
                        );

                        match result {
                            Ok(message) => {
                                status.set_label(&message);
                                let (reload_mode, reload_attention_only) =
                                    if let Some((queued_mode, queued_attention_only)) =
                                        pending_load_request.take()
                                    {
                                        (queued_mode, queued_attention_only)
                                    } else {
                                        (current_mode, attention_only_check.value())
                                    };
                                let _ = sender.send(DataGuardMessage::LoadRequested {
                                    mode: reload_mode,
                                    attention_only: reload_attention_only,
                                });
                                app::awake();
                            }
                            Err(err) => {
                                status.set_label("Data Guard action failed");
                                fltk::dialog::alert_default(&err);
                                if let Some((queued_mode, queued_attention_only)) =
                                    pending_load_request.take()
                                {
                                    if dialog.shown() {
                                        let _ = sender.send(DataGuardMessage::LoadRequested {
                                            mode: queued_mode,
                                            attention_only: queued_attention_only,
                                        });
                                        app::awake();
                                    }
                                }
                            }
                        }
                    }
                    DataGuardMessage::CloseRequested => {
                        pending_load_request = None;
                        dialog.hide();
                    }
                }
            }
        }

        set_cursor(Cursor::Default);
        app::flush();
        Window::delete(dialog);
    }
}

fn current_selected_row_index(selection: (i32, i32, i32, i32)) -> Option<usize> {
    let selected_row = selection.0.min(selection.2);
    if selected_row < 0 {
        return None;
    }

    usize::try_from(selected_row).ok()
}

fn column_index_by_name(columns: &[String], target_name: &str) -> Option<usize> {
    columns
        .iter()
        .position(|name| name.trim().eq_ignore_ascii_case(target_name))
}

fn column_value_by_name<'a>(
    row_values: &'a [String],
    columns: &[String],
    target_name: &str,
) -> Option<&'a str> {
    let index = column_index_by_name(columns, target_name)?;
    row_values.get(index).map(|value| value.as_str())
}

fn parse_positive_i64(value: &str) -> Option<i64> {
    let parsed = value.trim().parse::<i64>().ok()?;
    if parsed <= 0 {
        return None;
    }
    Some(parsed)
}

fn parse_sql_monitor_session_target(
    row_values: &[String],
    columns: &[String],
) -> Option<(Option<i64>, i64, i64)> {
    let sid_text = column_value_by_name(row_values, columns, "SID")
        .or_else(|| row_values.first().map(|value| value.as_str()))?;
    let serial_text = column_value_by_name(row_values, columns, "SERIAL#")
        .or_else(|| row_values.get(1).map(|value| value.as_str()))?;
    let sid = parse_positive_i64(sid_text)?;
    let serial = parse_positive_i64(serial_text)?;
    let instance_id =
        column_value_by_name(row_values, columns, "INST_ID").and_then(parse_positive_i64);

    Some((instance_id, sid, serial))
}

fn sql_monitor_session_target_label(instance_id: Option<i64>, sid: i64, serial: i64) -> String {
    match instance_id {
        Some(inst) => format!("{sid}.{serial}@{inst}"),
        None => format!("{sid}.{serial}"),
    }
}

fn filter_alert_rows(snapshot: &QueryResult) -> QueryResult {
    let Some(alert_status_index) = snapshot
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case("ALERT_STATUS"))
    else {
        return snapshot.clone();
    };

    let mut filtered = snapshot.clone();
    filtered.rows = snapshot
        .rows
        .iter()
        .filter(|row| {
            let Some(value) = row.get(alert_status_index) else {
                return false;
            };
            let upper = value.trim().to_uppercase();
            upper == "WARN" || upper == "CRITICAL"
        })
        .cloned()
        .collect();
    filtered.row_count = filtered.rows.len();
    filtered.message = format!("{} rows fetched (alerts only)", filtered.row_count);
    filtered
}

#[cfg(test)]
fn parse_sid_serial_row(row_values: &[String]) -> Option<(i64, i64)> {
    let (_, sid, serial) = parse_sql_monitor_session_target(row_values, &[])?;
    if sid < 0 || serial < 0 {
        return None;
    }

    Some((sid, serial))
}

fn storage_mode_label(mode: StorageViewMode) -> &'static str {
    match mode {
        StorageViewMode::Tablespace => "Tablespace",
        StorageViewMode::Temp => "TEMP",
        StorageViewMode::Undo => "UNDO",
        StorageViewMode::Archive => "Archive/FRA",
        StorageViewMode::Datafiles => "Datafiles",
    }
}

fn parse_positive_u32(value: &str, name: &str) -> Result<u32, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("{} is required", name));
    }

    let parsed = trimmed
        .parse::<u32>()
        .map_err(|_| format!("{} must be a positive integer", name))?;
    if parsed == 0 {
        return Err(format!("{} must be a positive integer", name));
    }
    Ok(parsed)
}

fn parse_bounded_positive_u32(value: &str, name: &str, max: u32) -> Result<u32, String> {
    let parsed = parse_positive_u32(value, name)?;
    if parsed > max {
        return Err(format!("{} must be {} or less", name, max));
    }
    Ok(parsed)
}

fn parse_optional_non_negative_i32(value: &str, name: &str) -> Result<Option<i32>, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let parsed = trimmed
        .parse::<i32>()
        .map_err(|_| format!("{} must be a non-negative integer", name))?;
    if parsed < 0 {
        return Err(format!("{} must be a non-negative integer", name));
    }

    Ok(Some(parsed))
}

fn parse_percentage_thresholds(warn_text: &str, critical_text: &str) -> Result<(u32, u32), String> {
    let warn = parse_positive_u32(warn_text, "Warn%")?;
    let critical = parse_positive_u32(critical_text, "Critical%")?;

    if warn > 100 || critical > 100 {
        return Err("Warn% and Critical% must be 100 or less".to_string());
    }
    if critical < warn {
        return Err("Critical% must be greater than or equal to Warn%".to_string());
    }

    Ok((warn, critical))
}

fn security_quick_action_hint(action_index: i32) -> &'static str {
    match action_index {
        0 => "Quick: grant role. Fill User + Role/Priv.",
        1 => "Quick: revoke role. Fill User + Role/Priv.",
        2 => "Quick: grant system privilege. Fill User + Role/Priv.",
        3 => "Quick: revoke system privilege. Fill User + Role/Priv.",
        4 => "Quick: set profile. Fill User + Profile.",
        5 => "Quick: lock user. Fill User.",
        6 => "Quick: unlock user. Fill User.",
        7 => "Quick: expire password. Fill User.",
        8 => "Quick: create user. Fill User (+ optional Profile).",
        9 => "Quick: drop user. Fill User.",
        10 => "Quick: create role. Fill Role/Priv.",
        11 => "Quick: drop role. Fill Role/Priv.",
        _ => "Quick: select action, then run.",
    }
}

fn prompt_optional_text(prompt: &str, default_value: &str) -> Option<String> {
    fltk::dialog::input_default(prompt, default_value)
}

fn prompt_secret_text(prompt: &str) -> Option<String> {
    let current_group = fltk::group::Group::try_current();
    fltk::group::Group::set_current(None::<&fltk::group::Group>);

    let mut dialog = Window::default().with_size(440, 160).with_label("Input");
    center_on_main(&mut dialog);
    dialog.set_color(theme::panel_raised());
    dialog.make_modal(true);

    let mut root = Flex::default().with_pos(10, 10).with_size(420, 140);
    root.set_type(FlexType::Column);
    root.set_spacing(DIALOG_SPACING);

    let mut prompt_label = Frame::default().with_label(prompt);
    prompt_label.set_align(Align::Left | Align::Inside | Align::Wrap);
    prompt_label.set_label_color(theme::text_primary());
    root.fixed(&prompt_label, 46);

    let mut password_input = SecretInput::default();
    password_input.set_color(theme::input_bg());
    password_input.set_text_color(theme::text_primary());
    password_input.set_trigger(CallbackTrigger::EnterKeyAlways);
    root.fixed(&password_input, INPUT_ROW_HEIGHT);

    let mut button_row = Flex::default();
    button_row.set_type(FlexType::Row);
    button_row.set_spacing(DIALOG_SPACING);

    let button_filler = Frame::default();
    button_row.resizable(&button_filler);

    let mut ok_btn = Button::default().with_label("OK");
    ok_btn.set_color(theme::button_primary());
    ok_btn.set_label_color(theme::text_primary());
    ok_btn.set_frame(FrameType::RFlatBox);
    button_row.fixed(&ok_btn, BUTTON_WIDTH);

    let mut cancel_btn = Button::default().with_label("Cancel");
    cancel_btn.set_color(theme::button_subtle());
    cancel_btn.set_label_color(theme::text_primary());
    cancel_btn.set_frame(FrameType::RFlatBox);
    button_row.fixed(&cancel_btn, BUTTON_WIDTH);

    button_row.end();
    root.fixed(&button_row, BUTTON_ROW_HEIGHT);
    root.end();
    dialog.end();
    fltk::group::Group::set_current(current_group.as_ref());

    let (response_tx, response_rx) = mpsc::channel::<Option<String>>();

    {
        let response_tx = response_tx.clone();
        let mut dialog = dialog.clone();
        let password_input = password_input.clone();
        ok_btn.set_callback(move |_| {
            let _ = response_tx.send(Some(password_input.value()));
            dialog.hide();
            app::awake();
        });
    }

    {
        let response_tx = response_tx.clone();
        let mut dialog = dialog.clone();
        cancel_btn.set_callback(move |_| {
            let _ = response_tx.send(None);
            dialog.hide();
            app::awake();
        });
    }

    {
        let response_tx = response_tx.clone();
        let mut dialog = dialog.clone();
        let password_input_for_enter = password_input.clone();
        let mut password_input_callback = password_input.clone();
        password_input_callback.set_callback(move |_| {
            let _ = response_tx.send(Some(password_input_for_enter.value()));
            dialog.hide();
            app::awake();
        });
    }

    dialog.show();
    let _ = dialog.take_focus();
    let _ = password_input.take_focus();

    while dialog.shown() {
        app::wait();
    }

    let result = match response_rx.try_recv() {
        Ok(value) => value,
        Err(_) => None,
    };
    Window::delete(dialog);
    result
}

fn normalize_optional_text_param(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    Some(trimmed.to_string())
}

fn normalize_optional_sql_id(value: &str) -> Result<Option<String>, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let upper = trimmed.to_uppercase();
    if !upper.chars().all(|ch| ch.is_ascii_alphanumeric()) {
        return Err("SQL_ID must contain only ASCII letters and digits".to_string());
    }
    if upper.len() != 13 {
        return Err("SQL_ID must be exactly 13 characters".to_string());
    }

    Ok(Some(upper))
}

fn normalize_required_identifier(value: &str, name: &str) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("{} is required", name));
    }

    let upper = trimmed.to_uppercase();
    if !is_ascii_identifier(&upper) {
        return Err(format!("{} must use only letters, digits, _, $, #", name));
    }

    Ok(upper)
}

fn normalize_required_system_privilege(value: &str) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("System privilege is required".to_string());
    }

    let mut normalized_tokens: Vec<String> = Vec::new();
    for token in trimmed.split_whitespace() {
        let upper = token.to_uppercase();
        if !is_ascii_identifier(&upper) {
            return Err(
                "System privilege must use words composed of letters, digits, _, $, #".to_string(),
            );
        }
        normalized_tokens.push(upper);
    }

    if normalized_tokens.is_empty() {
        return Err("System privilege is required".to_string());
    }

    Ok(normalized_tokens.join(" "))
}

fn normalize_optional_identifier(value: &str, name: &str) -> Result<Option<String>, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    normalize_required_identifier(trimmed, name).map(Some)
}

fn is_ascii_identifier(value: &str) -> bool {
    value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '$' || ch == '#')
}

fn qualified_owner_object(owner: Option<&str>, name: &str) -> String {
    match owner.map(str::trim).filter(|value| !value.is_empty()) {
        Some(owner_name) => format!("{owner_name}.{}", name.trim()),
        None => name.trim().to_string(),
    }
}

fn parse_owner_job_row(row_values: &[String]) -> Option<(String, String)> {
    let owner = row_values.first()?.trim().to_string();
    let job = row_values.get(1)?.trim().to_string();
    if owner.is_empty() || job.is_empty() {
        return None;
    }

    let owner_upper = owner.to_uppercase();
    let job_upper = job.to_uppercase();
    if !is_ascii_identifier(&owner_upper) || !is_ascii_identifier(&job_upper) {
        return None;
    }

    Some((owner_upper, job_upper))
}

fn parse_sql_id_child_row(row_values: &[String], columns: &[String]) -> Option<(String, i32)> {
    let sql_id_text = column_value_by_name(row_values, columns, "SQL_ID")
        .or_else(|| row_values.first().map(|value| value.as_str()))?;
    let child_text = column_value_by_name(row_values, columns, "CHILD_NUMBER")
        .or_else(|| column_value_by_name(row_values, columns, "CHILD#"))
        .or_else(|| row_values.get(1).map(|value| value.as_str()))?
        .trim();
    let sql_id = sql_id_text.trim().to_uppercase();
    if normalize_optional_sql_id(&sql_id).ok().flatten().is_none() {
        return None;
    }
    let child = parse_optional_non_negative_i32(child_text, "Child#")
        .ok()
        .flatten()?;
    Some((sql_id, child))
}

fn dba_info_result(message: &str) -> QueryResult {
    QueryResult {
        sql: String::new(),
        columns: Vec::new(),
        rows: Vec::new(),
        row_count: 0,
        execution_time: Duration::from_secs(0),
        message: message.to_string(),
        is_select: false,
        success: true,
    }
}

fn dataguard_role_from_snapshot(snapshot: &QueryResult) -> Option<String> {
    let role_index = snapshot
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case("DATABASE_ROLE"))?;
    let first_row = snapshot.rows.first()?;
    let role = first_row.get(role_index)?.trim().to_uppercase();
    if role.is_empty() || role == "-" {
        return None;
    }
    Some(role)
}

fn dataguard_db_unique_name_from_snapshot(snapshot: &QueryResult) -> Option<String> {
    let db_unique_name_index = snapshot
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case("DB_UNIQUE_NAME"))?;
    let first_row = snapshot.rows.first()?;
    let db_unique_name = first_row.get(db_unique_name_index)?.trim().to_uppercase();
    if db_unique_name.is_empty() || db_unique_name == "-" {
        return None;
    }
    Some(db_unique_name)
}

fn dataguard_role_allows_apply_control(database_role: Option<&str>) -> bool {
    database_role == Some("PHYSICAL STANDBY")
}

fn default_rman_job_name(prefix: &str) -> String {
    let timestamp_millis = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis(),
        Err(_) => 0,
    };
    let sequence = RMAN_JOB_NAME_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!(
        "{}_{}_{}",
        prefix.trim().to_uppercase(),
        timestamp_millis,
        sequence
    )
}

fn refresh_security_action_controls(
    mode: SecurityViewMode,
    quick_run_btn: &mut Button,
    grant_btn: &mut Button,
    revoke_btn: &mut Button,
    grant_sys_btn: &mut Button,
    revoke_sys_btn: &mut Button,
    set_profile_btn: &mut Button,
    expire_password_btn: &mut Button,
    create_user_btn: &mut Button,
    drop_user_btn: &mut Button,
    create_role_btn: &mut Button,
    drop_role_btn: &mut Button,
    lock_user_btn: &mut Button,
    unlock_user_btn: &mut Button,
) {
    let profiles_mode = matches!(mode, SecurityViewMode::Profiles);
    let set_enabled = |button: &mut Button| {
        if profiles_mode {
            button.deactivate();
        } else {
            button.activate();
        }
    };

    set_enabled(quick_run_btn);
    set_enabled(grant_btn);
    set_enabled(revoke_btn);
    set_enabled(grant_sys_btn);
    set_enabled(revoke_sys_btn);
    set_enabled(set_profile_btn);
    set_enabled(expire_password_btn);
    set_enabled(create_user_btn);
    set_enabled(drop_user_btn);
    set_enabled(create_role_btn);
    set_enabled(drop_role_btn);
    set_enabled(lock_user_btn);
    set_enabled(unlock_user_btn);

    if profiles_mode {
        quick_run_btn.set_tooltip(
            "Disabled in Profiles view. Switch to Users/Summary/Grants to run actions.",
        );
    } else {
        quick_run_btn.set_tooltip("Run selected quick action");
    }
}

fn refresh_dataguard_force_switch_button(
    force_switch_btn: &mut Button,
    database_role: Option<&str>,
    overview_loaded: bool,
    loading_snapshot: bool,
    action_running: bool,
) {
    if loading_snapshot {
        force_switch_btn.deactivate();
        force_switch_btn.set_tooltip("Loading Data Guard overview...");
        return;
    }

    if action_running {
        force_switch_btn.deactivate();
        force_switch_btn.set_tooltip("Archive log switch is in progress");
        return;
    }

    if !overview_loaded {
        force_switch_btn.deactivate();
        force_switch_btn.set_tooltip("Load Overview. Enabled only for PRIMARY role.");
        return;
    }

    if database_role == Some("PRIMARY") {
        force_switch_btn.activate();
        force_switch_btn.set_tooltip("ALTER SYSTEM ARCHIVE LOG CURRENT");
        return;
    }

    force_switch_btn.deactivate();
    let role = database_role.unwrap_or("UNKNOWN");
    force_switch_btn.set_tooltip(&format!("Disabled: database role is {}", role));
}

#[cfg(test)]
mod tests {
    use crate::db::ColumnInfo;
    use std::time::Duration;

    use super::{
        column_value_by_name, current_selected_row_index, dataguard_db_unique_name_from_snapshot,
        dataguard_role_allows_apply_control, dataguard_role_from_snapshot, default_rman_job_name,
        filter_alert_rows, is_ascii_identifier, normalize_optional_sql_id,
        normalize_required_identifier, normalize_required_system_privilege,
        parse_bounded_positive_u32, parse_optional_non_negative_i32, parse_percentage_thresholds,
        parse_positive_u32, parse_sid_serial_row, parse_sql_id_child_row,
        parse_sql_monitor_session_target, qualified_owner_object, security_quick_action_hint,
        sql_monitor_session_target_label, QueryResult,
    };

    #[test]
    fn parse_positive_u32_accepts_positive_values() {
        assert_eq!(parse_positive_u32("15", "Min"), Ok(15));
    }

    #[test]
    fn parse_bounded_positive_u32_accepts_values_in_range() {
        assert_eq!(parse_bounded_positive_u32("15", "Min", 30), Ok(15));
    }

    #[test]
    fn parse_bounded_positive_u32_rejects_values_above_max() {
        assert!(parse_bounded_positive_u32("31", "Min", 30).is_err());
    }

    #[test]
    fn parse_optional_non_negative_i32_handles_empty_and_values() {
        assert_eq!(parse_optional_non_negative_i32("", "Child"), Ok(None));
        assert_eq!(parse_optional_non_negative_i32("0", "Child"), Ok(Some(0)));
        assert_eq!(parse_optional_non_negative_i32("7", "Child"), Ok(Some(7)));
    }

    #[test]
    fn parse_percentage_thresholds_rejects_invalid_order() {
        assert!(parse_percentage_thresholds("90", "80").is_err());
    }

    #[test]
    fn normalize_optional_sql_id_rejects_non_alnum() {
        assert!(normalize_optional_sql_id("abc-123").is_err());
    }

    #[test]
    fn normalize_optional_sql_id_requires_exactly_13_chars() {
        assert!(normalize_optional_sql_id("abc123").is_err());
        assert!(normalize_optional_sql_id("12345678901234").is_err());
        assert_eq!(
            normalize_optional_sql_id("7v9h9ttw0g3cn"),
            Ok(Some("7V9H9TTW0G3CN".to_string()))
        );
    }

    #[test]
    fn normalize_required_identifier_uppercases_valid_input() {
        assert_eq!(
            normalize_required_identifier("hr_user", "User"),
            Ok("HR_USER".to_string())
        );
    }

    #[test]
    fn is_ascii_identifier_accepts_oracle_identifier_chars() {
        assert!(is_ascii_identifier("SYS$ROLE_1"));
        assert!(!is_ascii_identifier("bad-role"));
    }

    #[test]
    fn qualified_owner_object_formats_owner_and_name() {
        assert_eq!(
            qualified_owner_object(Some("HR"), "JOB1"),
            "HR.JOB1".to_string()
        );
        assert_eq!(qualified_owner_object(None, "JOB1"), "JOB1".to_string());
    }

    #[test]
    fn parse_sql_id_child_row_parses_valid_row() {
        let row = vec![
            "7v9h9ttw0g3cn".to_string(),
            "2".to_string(),
            "2026-02-20 11:00:00".to_string(),
        ];
        assert_eq!(
            parse_sql_id_child_row(&row, &[]),
            Some(("7V9H9TTW0G3CN".to_string(), 2))
        );
    }

    #[test]
    fn parse_sql_id_child_row_rejects_invalid_row() {
        let row = vec!["(message)".to_string(), "-".to_string()];
        assert_eq!(parse_sql_id_child_row(&row, &[]), None);
    }

    #[test]
    fn parse_sql_id_child_row_uses_named_columns() {
        let columns = vec![
            "INST_ID".to_string(),
            "SQL_ID".to_string(),
            "CHILD_NUMBER".to_string(),
        ];
        let row = vec!["1".to_string(), "7v9h9ttw0g3cn".to_string(), "4".to_string()];
        assert_eq!(
            parse_sql_id_child_row(&row, &columns),
            Some(("7V9H9TTW0G3CN".to_string(), 4))
        );
    }

    #[test]
    fn normalize_required_system_privilege_accepts_multi_word_privileges() {
        assert_eq!(
            normalize_required_system_privilege("create   session"),
            Ok("CREATE SESSION".to_string())
        );
    }

    #[test]
    fn normalize_required_system_privilege_rejects_invalid_symbols() {
        assert!(normalize_required_system_privilege("CREATE-SESSION").is_err());
    }

    #[test]
    fn current_selected_row_index_returns_none_for_empty_selection() {
        assert_eq!(current_selected_row_index((-1, -1, -1, -1)), None);
    }

    #[test]
    fn current_selected_row_index_returns_row_for_valid_selection() {
        assert_eq!(current_selected_row_index((3, 0, 3, 12)), Some(3));
    }

    #[test]
    fn parse_sid_serial_row_accepts_valid_numbers() {
        let row = vec![
            "123".to_string(),
            "456".to_string(),
            "EXECUTING".to_string(),
        ];
        assert_eq!(parse_sid_serial_row(&row), Some((123, 456)));
    }

    #[test]
    fn parse_sid_serial_row_rejects_invalid_values() {
        let row = vec!["-".to_string(), "ABC".to_string()];
        assert_eq!(parse_sid_serial_row(&row), None);
    }

    #[test]
    fn parse_sql_monitor_session_target_reads_columns_by_name() {
        let columns = vec![
            "INST_ID".to_string(),
            "SID".to_string(),
            "SERIAL#".to_string(),
            "STATUS".to_string(),
        ];
        let row = vec![
            "2".to_string(),
            "123".to_string(),
            "456".to_string(),
            "EXECUTING".to_string(),
        ];
        assert_eq!(
            parse_sql_monitor_session_target(&row, &columns),
            Some((Some(2), 123, 456))
        );
    }

    #[test]
    fn sql_monitor_session_target_label_formats_instance() {
        assert_eq!(
            sql_monitor_session_target_label(Some(3), 101, 202),
            "101.202@3".to_string()
        );
        assert_eq!(
            sql_monitor_session_target_label(None, 101, 202),
            "101.202".to_string()
        );
    }

    #[test]
    fn column_value_by_name_returns_expected_value() {
        let columns = vec!["SID".to_string(), "SERIAL#".to_string()];
        let row = vec!["111".to_string(), "222".to_string()];
        assert_eq!(column_value_by_name(&row, &columns, "serial#"), Some("222"));
    }

    #[test]
    fn filter_alert_rows_keeps_warn_and_critical_only() {
        let snapshot = QueryResult::new_select(
            "SELECT",
            vec![
                ColumnInfo {
                    name: "TABLESPACE_NAME".to_string(),
                    data_type: "VARCHAR2".to_string(),
                },
                ColumnInfo {
                    name: "ALERT_STATUS".to_string(),
                    data_type: "VARCHAR2".to_string(),
                },
            ],
            vec![
                vec!["USERS".to_string(), "OK".to_string()],
                vec!["TEMP".to_string(), "WARN".to_string()],
                vec!["UNDO".to_string(), "CRITICAL".to_string()],
            ],
            Duration::from_millis(10),
        );

        let filtered = filter_alert_rows(&snapshot);
        assert_eq!(filtered.row_count, 2);
        assert_eq!(filtered.rows.len(), 2);
        assert_eq!(filtered.rows[0][0], "TEMP");
        assert_eq!(filtered.rows[1][0], "UNDO");
    }

    #[test]
    fn dataguard_role_from_snapshot_reads_database_role_column() {
        let snapshot = QueryResult::new_select(
            "SELECT",
            vec![
                ColumnInfo {
                    name: "DB_UNIQUE_NAME".to_string(),
                    data_type: "VARCHAR2".to_string(),
                },
                ColumnInfo {
                    name: "DATABASE_ROLE".to_string(),
                    data_type: "VARCHAR2".to_string(),
                },
            ],
            vec![vec!["PROD".to_string(), "PRIMARY".to_string()]],
            Duration::from_millis(5),
        );

        assert_eq!(
            dataguard_role_from_snapshot(&snapshot),
            Some("PRIMARY".to_string())
        );
    }

    #[test]
    fn dataguard_role_from_snapshot_returns_none_for_missing_or_empty_role() {
        let missing_column = QueryResult::new_select(
            "SELECT",
            vec![ColumnInfo {
                name: "DB_UNIQUE_NAME".to_string(),
                data_type: "VARCHAR2".to_string(),
            }],
            vec![vec!["PROD".to_string()]],
            Duration::from_millis(5),
        );
        assert_eq!(dataguard_role_from_snapshot(&missing_column), None);

        let empty_role = QueryResult::new_select(
            "SELECT",
            vec![ColumnInfo {
                name: "DATABASE_ROLE".to_string(),
                data_type: "VARCHAR2".to_string(),
            }],
            vec![vec![" - ".to_string()]],
            Duration::from_millis(5),
        );
        assert_eq!(dataguard_role_from_snapshot(&empty_role), None);
    }

    #[test]
    fn dataguard_db_unique_name_from_snapshot_reads_value() {
        let snapshot = QueryResult::new_select(
            "SELECT",
            vec![ColumnInfo {
                name: "DB_UNIQUE_NAME".to_string(),
                data_type: "VARCHAR2".to_string(),
            }],
            vec![vec!["standby01".to_string()]],
            Duration::from_millis(5),
        );

        assert_eq!(
            dataguard_db_unique_name_from_snapshot(&snapshot),
            Some("STANDBY01".to_string())
        );
    }

    #[test]
    fn dataguard_role_allows_apply_control_only_for_physical_standby() {
        assert!(dataguard_role_allows_apply_control(Some(
            "PHYSICAL STANDBY"
        )));
        assert!(!dataguard_role_allows_apply_control(Some("PRIMARY")));
        assert!(!dataguard_role_allows_apply_control(None));
    }

    #[test]
    fn default_rman_job_name_includes_prefix() {
        let generated = default_rman_job_name("rman_backup_job");
        assert!(generated.starts_with("RMAN_BACKUP_JOB_"));
    }

    #[test]
    fn default_rman_job_name_is_unique_between_calls() {
        let first = default_rman_job_name("rman_backup_job");
        let second = default_rman_job_name("rman_backup_job");
        assert_ne!(first, second);
    }

    #[test]
    fn security_quick_action_hint_maps_known_actions() {
        assert_eq!(
            security_quick_action_hint(0),
            "Quick: grant role. Fill User + Role/Priv."
        );
        assert_eq!(
            security_quick_action_hint(11),
            "Quick: drop role. Fill Role/Priv."
        );
    }
}
