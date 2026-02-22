use fltk::{
    app,
    button::Button,
    draw::set_cursor,
    enums::{Align, Cursor, FrameType},
    frame::Frame,
    group::{Flex, FlexType},
    input::IntInput,
    prelude::*,
    window::Window,
};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use crate::db::{
    format_connection_busy_message, try_lock_connection_with_activity, QueryExecutor, QueryResult,
};
use crate::ui::constants::*;
use crate::ui::theme;
use crate::ui::{center_on_main, configured_ui_font_size, ResultTableWidget};

use super::SqlEditorWidget;

enum SessionMonitorMessage {
    RefreshRequested,
    HeavyLoadRequested {
        min_elapsed_text: String,
    },
    KillRequested {
        instance_text: String,
        sid_text: String,
        serial_text: String,
    },
    CloseRequested,
    SnapshotLoaded {
        request_id: u64,
        result: Result<QueryResult, String>,
    },
    HeavyLoadLoaded {
        request_id: u64,
        min_elapsed_seconds: u32,
        result: Result<QueryResult, String>,
    },
    KillFinished(Result<String, String>),
}

impl SqlEditorWidget {
    pub fn show_session_lock_monitor(&self) {
        let (sender, receiver) = mpsc::channel::<SessionMonitorMessage>();

        let current_group = fltk::group::Group::try_current();
        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let dialog_w = 1040;
        let dialog_h = 620;
        let mut dialog = Window::default()
            .with_size(dialog_w, dialog_h)
            .with_label("Session / Lock Monitor");
        center_on_main(&mut dialog);
        dialog.set_color(theme::panel_raised());
        dialog.make_modal(true);

        let mut root = Flex::default().with_pos(0, 0).with_size(dialog_w, dialog_h);
        root.set_type(FlexType::Column);
        root.set_margin(DIALOG_MARGIN);
        root.set_spacing(DIALOG_SPACING);

        let mut help = Frame::default().with_label(
            "Shows active user sessions and lock wait state. Use SID/SERIAL# to terminate a session.",
        );
        help.set_label_size((configured_ui_font_size().saturating_sub(1)).max(10));
        help.set_label_color(theme::text_secondary());
        help.set_align(Align::Left | Align::Inside);
        root.fixed(&help, LABEL_ROW_HEIGHT);

        let mut control_row = Flex::default();
        control_row.set_type(FlexType::Row);
        control_row.set_spacing(DIALOG_SPACING);

        let mut refresh_btn = Button::default().with_label("Refresh");
        refresh_btn.set_color(theme::button_secondary());
        refresh_btn.set_label_color(theme::text_primary());
        refresh_btn.set_frame(FrameType::RFlatBox);
        control_row.fixed(&refresh_btn, BUTTON_WIDTH_LARGE);

        let mut min_elapsed_label = Frame::default().with_label("Min Sec:");
        min_elapsed_label.set_label_color(theme::text_primary());
        min_elapsed_label.set_align(Align::Inside | Align::Left);
        control_row.fixed(&min_elapsed_label, 56);

        let mut min_elapsed_input = IntInput::default();
        min_elapsed_input.set_value("15");
        min_elapsed_input.set_color(theme::input_bg());
        min_elapsed_input.set_text_color(theme::text_primary());
        control_row.fixed(&min_elapsed_input, 64);

        let mut heavy_load_btn = Button::default().with_label("Heavy SQL/PLSQL");
        heavy_load_btn.set_color(theme::button_secondary());
        heavy_load_btn.set_label_color(theme::text_primary());
        heavy_load_btn.set_frame(FrameType::RFlatBox);
        control_row.fixed(&heavy_load_btn, BUTTON_WIDTH_LARGE + 54);

        let mut instance_label = Frame::default().with_label("INST:");
        instance_label.set_label_color(theme::text_primary());
        instance_label.set_align(Align::Inside | Align::Left);
        control_row.fixed(&instance_label, 42);

        let mut instance_input = IntInput::default();
        instance_input.set_color(theme::input_bg());
        instance_input.set_text_color(theme::text_primary());
        instance_input.set_tooltip("Optional RAC instance id");
        control_row.fixed(&instance_input, 62);

        let mut sid_label = Frame::default().with_label("SID:");
        sid_label.set_label_color(theme::text_primary());
        sid_label.set_align(Align::Inside | Align::Left);
        control_row.fixed(&sid_label, 34);

        let mut sid_input = IntInput::default();
        sid_input.set_color(theme::input_bg());
        sid_input.set_text_color(theme::text_primary());
        control_row.fixed(&sid_input, 90);

        let mut serial_label = Frame::default().with_label("SERIAL#:");
        serial_label.set_label_color(theme::text_primary());
        serial_label.set_align(Align::Inside | Align::Left);
        control_row.fixed(&serial_label, 64);

        let mut serial_input = IntInput::default();
        serial_input.set_color(theme::input_bg());
        serial_input.set_text_color(theme::text_primary());
        control_row.fixed(&serial_input, 110);

        let mut kill_btn = Button::default().with_label("Kill Session");
        kill_btn.set_color(theme::button_danger());
        kill_btn.set_label_color(theme::text_primary());
        kill_btn.set_frame(FrameType::RFlatBox);
        control_row.fixed(&kill_btn, BUTTON_WIDTH_LARGE + 20);

        let close_spacer = Frame::default();
        control_row.resizable(&close_spacer);

        let mut close_btn = Button::default().with_label("Close");
        close_btn.set_color(theme::button_subtle());
        close_btn.set_label_color(theme::text_primary());
        close_btn.set_frame(FrameType::RFlatBox);
        control_row.fixed(&close_btn, BUTTON_WIDTH);

        control_row.end();
        root.fixed(&control_row, BUTTON_ROW_HEIGHT + 4);

        let mut result_table =
            ResultTableWidget::with_size(0, 0, dialog_w - DIALOG_MARGIN * 2, 320);
        result_table.set_max_cell_display_chars(240);
        let table_widget = result_table.get_widget();
        root.resizable(&table_widget);
        result_table.display_result(&monitor_info_result("Loading session/lock snapshot..."));

        let mut status = Frame::default().with_label("Ready");
        status.set_label_color(theme::text_secondary());
        status.set_align(Align::Left | Align::Inside);
        root.fixed(&status, LABEL_ROW_HEIGHT);

        root.end();
        dialog.end();
        fltk::group::Group::set_current(current_group.as_ref());

        let sender_refresh = sender.clone();
        refresh_btn.set_callback(move |_| {
            let _ = sender_refresh.send(SessionMonitorMessage::RefreshRequested);
            app::awake();
        });

        let sender_kill = sender.clone();
        let instance_input_for_kill = instance_input.clone();
        let sid_input_for_kill = sid_input.clone();
        let serial_input_for_kill = serial_input.clone();
        kill_btn.set_callback(move |_| {
            let _ = sender_kill.send(SessionMonitorMessage::KillRequested {
                instance_text: instance_input_for_kill.value(),
                sid_text: sid_input_for_kill.value(),
                serial_text: serial_input_for_kill.value(),
            });
            app::awake();
        });

        let sender_heavy = sender.clone();
        let min_elapsed_input_for_heavy = min_elapsed_input.clone();
        heavy_load_btn.set_callback(move |_| {
            let _ = sender_heavy.send(SessionMonitorMessage::HeavyLoadRequested {
                min_elapsed_text: min_elapsed_input_for_heavy.value(),
            });
            app::awake();
        });

        let sender_close = sender.clone();
        close_btn.set_callback(move |_| {
            let _ = sender_close.send(SessionMonitorMessage::CloseRequested);
            app::awake();
        });

        dialog.show();
        let _ = dialog.take_focus();
        let _ = refresh_btn.take_focus();

        let _ = sender.send(SessionMonitorMessage::RefreshRequested);
        app::awake();

        let mut latest_refresh_request_id: u64 = 0;
        let mut latest_snapshot_columns: Vec<String> = Vec::new();
        let mut last_table_selection = (i32::MIN, i32::MIN, i32::MIN, i32::MIN);
        while dialog.shown() {
            app::wait();

            while let Ok(message) = receiver.try_recv() {
                match message {
                    SessionMonitorMessage::RefreshRequested => {
                        latest_refresh_request_id = latest_refresh_request_id.saturating_add(1);
                        let request_id = latest_refresh_request_id;
                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label("Loading session/lock snapshot...");
                        result_table.display_result(&monitor_info_result(
                            "Loading session/lock snapshot...",
                        ));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                "Loading session and lock monitor",
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => {
                                        QueryExecutor::get_session_lock_snapshot(db_conn.as_ref())
                                            .map_err(|err| {
                                                format!("Failed to load session snapshot: {err}")
                                            })
                                    }
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result
                                .send(SessionMonitorMessage::SnapshotLoaded { request_id, result });
                            app::awake();
                        });
                    }
                    SessionMonitorMessage::HeavyLoadRequested { min_elapsed_text } => {
                        let min_elapsed_seconds =
                            match parse_positive_u32(&min_elapsed_text, "Min seconds") {
                                Ok(value) => value,
                                Err(err) => {
                                    fltk::dialog::alert_default(&err);
                                    continue;
                                }
                            };

                        latest_refresh_request_id = latest_refresh_request_id.saturating_add(1);
                        let request_id = latest_refresh_request_id;
                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!(
                            "Loading heavy SQL/PLSQL sessions (>= {} sec)...",
                            min_elapsed_seconds
                        ));
                        result_table.display_result(&monitor_info_result(&format!(
                            "Loading heavy SQL/PLSQL sessions (>= {} sec)...",
                            min_elapsed_seconds
                        )));

                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                "Loading heavy SQL monitor",
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::get_heavy_execution_snapshot(
                                        db_conn.as_ref(),
                                        min_elapsed_seconds,
                                    )
                                    .map_err(|err| {
                                        format!("Failed to load heavy SQL snapshot: {err}")
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(SessionMonitorMessage::HeavyLoadLoaded {
                                request_id,
                                min_elapsed_seconds,
                                result,
                            });
                            app::awake();
                        });
                    }
                    SessionMonitorMessage::KillRequested {
                        instance_text,
                        sid_text,
                        serial_text,
                    } => {
                        let sid = match parse_positive_i64(&sid_text, "SID") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };
                        let serial = match parse_positive_i64(&serial_text, "SERIAL#") {
                            Ok(value) => value,
                            Err(err) => {
                                fltk::dialog::alert_default(&err);
                                continue;
                            }
                        };

                        let parsed_instance_id =
                            match parse_optional_positive_i64(&instance_text, "INST_ID") {
                                Ok(value) => value,
                                Err(err) => {
                                    fltk::dialog::alert_default(&err);
                                    continue;
                                }
                            };
                        let target_label = match parsed_instance_id {
                            Some(inst) => format!("{sid},{serial},@{inst}"),
                            None => format!("{sid},{serial}"),
                        };

                        let confirm = fltk::dialog::choice2_default(
                            &format!("Kill session {target_label} immediately?"),
                            "Cancel",
                            "Kill",
                            "",
                        );
                        if confirm != Some(1) {
                            continue;
                        }

                        set_cursor(Cursor::Wait);
                        app::flush();
                        status.set_label(&format!("Killing session {target_label}..."));

                        let instance_id_for_kill = parsed_instance_id;
                        let sender_result = sender.clone();
                        let connection = self.connection.clone();
                        thread::spawn(move || {
                            let result = match try_lock_connection_with_activity(
                                &connection,
                                "Killing database session",
                            ) {
                                Some(mut guard) => match guard.require_live_connection() {
                                    Ok(db_conn) => QueryExecutor::kill_session_on_instance(
                                        db_conn.as_ref(),
                                        sid,
                                        serial,
                                        instance_id_for_kill,
                                        true,
                                    )
                                    .map(|_| format!("Session {target_label} was killed"))
                                    .map_err(|err| {
                                        format!("Failed to kill session {target_label}: {err}")
                                    }),
                                    Err(message) => Err(message),
                                },
                                None => Err(format_connection_busy_message()),
                            };

                            let _ = sender_result.send(SessionMonitorMessage::KillFinished(result));
                            app::awake();
                        });
                    }
                    SessionMonitorMessage::SnapshotLoaded { request_id, result } => {
                        if request_id != latest_refresh_request_id {
                            continue;
                        }

                        set_cursor(Cursor::Default);
                        app::flush();

                        match result {
                            Ok(snapshot) => {
                                latest_snapshot_columns = snapshot
                                    .columns
                                    .iter()
                                    .map(|column| column.name.clone())
                                    .collect();
                                result_table.display_result(&snapshot);
                                last_table_selection = (i32::MIN, i32::MIN, i32::MIN, i32::MIN);
                                status.set_label(&format!(
                                    "Loaded {} rows in {} ms",
                                    snapshot.row_count,
                                    snapshot.execution_time.as_millis()
                                ));
                            }
                            Err(err) => {
                                result_table.display_result(&monitor_info_result(&format!(
                                    "Failed to load session/lock snapshot. {}\nTip: DBA privileges may be required for V$SESSION and V$LOCK.",
                                    err
                                )));
                                last_table_selection = (i32::MIN, i32::MIN, i32::MIN, i32::MIN);
                                status.set_label("Snapshot load failed");
                            }
                        }
                    }
                    SessionMonitorMessage::HeavyLoadLoaded {
                        request_id,
                        min_elapsed_seconds,
                        result,
                    } => {
                        if request_id != latest_refresh_request_id {
                            continue;
                        }

                        set_cursor(Cursor::Default);
                        app::flush();

                        match result {
                            Ok(snapshot) => {
                                latest_snapshot_columns = snapshot
                                    .columns
                                    .iter()
                                    .map(|column| column.name.clone())
                                    .collect();
                                result_table.display_result(&snapshot);
                                last_table_selection = (i32::MIN, i32::MIN, i32::MIN, i32::MIN);
                                status.set_label(&format!(
                                    "Loaded {} heavy session rows in {} ms (threshold: {}s)",
                                    snapshot.row_count,
                                    snapshot.execution_time.as_millis(),
                                    min_elapsed_seconds
                                ));
                            }
                            Err(err) => {
                                result_table.display_result(&monitor_info_result(&format!(
                                    "Failed to load heavy SQL/PLSQL snapshot. {}\nTip: privileges for V$SESSION and V$SQL may be required.",
                                    err
                                )));
                                last_table_selection = (i32::MIN, i32::MIN, i32::MIN, i32::MIN);
                                status.set_label("Heavy SQL snapshot load failed");
                            }
                        }
                    }
                    SessionMonitorMessage::KillFinished(result) => {
                        set_cursor(Cursor::Default);
                        app::flush();

                        match result {
                            Ok(message) => {
                                status.set_label(&message);
                                let _ = sender.send(SessionMonitorMessage::RefreshRequested);
                                app::awake();
                            }
                            Err(err) => {
                                status.set_label("Session kill failed");
                                fltk::dialog::alert_default(&err);
                            }
                        }
                    }
                    SessionMonitorMessage::CloseRequested => {
                        dialog.hide();
                    }
                }
            }

            let selection = table_widget.get_selection();
            if selection != last_table_selection {
                last_table_selection = selection;

                let selected_row = selection.0.min(selection.2);
                if selected_row >= 0 {
                    let selected_row_index = selected_row as usize;
                    if let Some(row_values) = result_table.row_values(selected_row_index) {
                        if let Some((instance_id, sid, serial)) =
                            parse_selected_session_identity(&row_values, &latest_snapshot_columns)
                        {
                            instance_input.set_value(
                                &instance_id
                                    .map(|value| value.to_string())
                                    .unwrap_or_default(),
                            );
                            sid_input.set_value(&sid.to_string());
                            serial_input.set_value(&serial.to_string());
                        }
                    }
                }
            }
        }

        set_cursor(Cursor::Default);
        app::flush();
        Window::delete(dialog);
    }
}

fn parse_positive_i64(value: &str, name: &str) -> Result<i64, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("{name} is required"));
    }

    let parsed = trimmed
        .parse::<i64>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed <= 0 {
        return Err(format!("{name} must be a positive integer"));
    }

    Ok(parsed)
}

fn parse_optional_positive_i64(value: &str, name: &str) -> Result<Option<i64>, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    parse_positive_i64(trimmed, name).map(Some)
}

fn parse_positive_u32(value: &str, name: &str) -> Result<u32, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("{name} is required"));
    }

    let parsed = trimmed
        .parse::<u32>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{name} must be a positive integer"));
    }

    Ok(parsed)
}

fn parse_selected_session_identity(
    row_values: &[String],
    columns: &[String],
) -> Option<(Option<i64>, i64, i64)> {
    let sid_index = columns
        .iter()
        .position(|name| name.eq_ignore_ascii_case("SID"))?;
    let serial_index = columns
        .iter()
        .position(|name| name.eq_ignore_ascii_case("SERIAL#"))
        .or_else(|| {
            columns
                .iter()
                .position(|name| name.eq_ignore_ascii_case("SERIAL"))
        })?;
    let instance_id = columns
        .iter()
        .position(|name| name.eq_ignore_ascii_case("INST_ID"))
        .and_then(|index| row_values.get(index))
        .and_then(|value| parse_positive_i64(value, "INST_ID").ok());
    if columns
        .iter()
        .any(|name| name.eq_ignore_ascii_case("INST_ID"))
        && instance_id.is_none()
    {
        return None;
    }

    let sid_text = row_values.get(sid_index)?;
    let serial_text = row_values.get(serial_index)?;
    let sid = parse_positive_i64(sid_text, "SID").ok()?;
    let serial = parse_positive_i64(serial_text, "SERIAL#").ok()?;
    if sid <= 0 || serial <= 0 {
        return None;
    }

    Some((instance_id, sid, serial))
}

fn monitor_info_result(message: &str) -> QueryResult {
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

#[cfg(test)]
mod tests {
    use super::{
        parse_optional_positive_i64, parse_positive_i64, parse_positive_u32,
        parse_selected_session_identity,
    };

    #[test]
    fn parse_positive_i64_accepts_positive_numbers() {
        assert_eq!(parse_positive_i64("123", "SID"), Ok(123));
    }

    #[test]
    fn parse_positive_i64_rejects_zero_or_negative() {
        assert!(parse_positive_i64("0", "SID").is_err());
        assert!(parse_positive_i64("-1", "SID").is_err());
    }

    #[test]
    fn parse_optional_positive_i64_handles_empty_and_value() {
        assert_eq!(parse_optional_positive_i64("", "INST_ID"), Ok(None));
        assert_eq!(parse_optional_positive_i64("3", "INST_ID"), Ok(Some(3)));
    }

    #[test]
    fn parse_positive_u32_accepts_positive_numbers() {
        assert_eq!(parse_positive_u32("15", "Min seconds"), Ok(15));
    }

    #[test]
    fn parse_positive_u32_rejects_zero_or_invalid() {
        assert!(parse_positive_u32("0", "Min seconds").is_err());
        assert!(parse_positive_u32("-1", "Min seconds").is_err());
        assert!(parse_positive_u32("abc", "Min seconds").is_err());
    }

    #[test]
    fn parse_selected_session_identity_reads_sid_and_serial() {
        let row = vec!["123".to_string(), "456".to_string(), "SCOTT".to_string()];
        let columns = vec!["SID".to_string(), "SERIAL#".to_string()];
        assert_eq!(
            parse_selected_session_identity(&row, &columns),
            Some((None, 123, 456))
        );
    }

    #[test]
    fn parse_selected_session_identity_reads_instance_sid_serial() {
        let row = vec![
            "3".to_string(),
            "123".to_string(),
            "456".to_string(),
            "SCOTT".to_string(),
            "ACTIVE".to_string(),
            "-".to_string(),
            "0".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
            "TM".to_string(),
        ];
        assert_eq!(
            parse_selected_session_identity(
                &row,
                &[
                    "INST_ID".to_string(),
                    "SID".to_string(),
                    "SERIAL#".to_string(),
                ],
            ),
            Some((Some(3), 123, 456))
        );
    }

    #[test]
    fn parse_selected_session_identity_rejects_non_numeric_row() {
        let row = vec!["(message)".to_string()];
        assert_eq!(parse_selected_session_identity(&row, &[]), None);
    }

    #[test]
    fn parse_selected_session_identity_rejects_invalid_inst_id_when_column_exists() {
        let row = vec!["-".to_string(), "101".to_string(), "202".to_string()];
        let columns = vec![
            "INST_ID".to_string(),
            "SID".to_string(),
            "SERIAL#".to_string(),
        ];
        assert_eq!(parse_selected_session_identity(&row, &columns), None);
    }
}
