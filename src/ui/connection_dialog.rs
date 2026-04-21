use fltk::{
    app,
    browser::HoldBrowser,
    button::Button,
    enums::{Event, FrameType, Key},
    frame::Frame,
    group::Flex,
    input::{Input, SecretInput},
    menu::Choice,
    prelude::*,
    window::Window,
};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::db::{ConnectionInfo, DatabaseConnection, DatabaseType};
use crate::ui::center_on_main;
use crate::ui::constants::*;
use crate::ui::theme;
use crate::utils::AppConfig;

pub struct ConnectionDialog;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum OracleConnectMode {
    #[default]
    Direct,
    TnsAlias,
}

#[derive(Clone, Debug)]
struct OracleModeFieldMemory {
    direct_host: String,
    direct_port: String,
    direct_service: String,
    tns_alias: String,
}

fn oracle_form_values_for_mode(
    oracle_mode: OracleConnectMode,
    memory: &mut OracleModeFieldMemory,
) -> (&'static str, String, String, String) {
    let form = DatabaseType::Oracle.connection_form_spec();
    match oracle_mode {
        OracleConnectMode::Direct => {
            if memory.direct_host.trim().is_empty() {
                memory.direct_host = form.default_host.to_string();
            }
            if memory.direct_port.trim().is_empty() {
                memory.direct_port = form.default_port.to_string();
            }
            if memory.direct_service.trim().is_empty() {
                memory.direct_service = form.default_service_name.to_string();
            }
            (
                form.service_name_form_label,
                memory.direct_host.clone(),
                memory.direct_port.clone(),
                memory.direct_service.clone(),
            )
        }
        OracleConnectMode::TnsAlias => (
            "TNS Alias:",
            String::new(),
            String::new(),
            memory.tns_alias.clone(),
        ),
    }
}

fn db_type_from_choice_index(idx: i32) -> DatabaseType {
    let supported = DatabaseType::supported();
    if idx < 0 {
        return supported.first().copied().unwrap_or_default();
    }
    supported
        .get(idx as usize)
        .copied()
        .or_else(|| supported.last().copied())
        .unwrap_or_default()
}

fn choice_index_from_db_type(db_type: DatabaseType) -> i32 {
    DatabaseType::supported()
        .iter()
        .position(|candidate| *candidate == db_type)
        .unwrap_or_default() as i32
}

fn oracle_connect_mode_from_choice_index(idx: i32) -> OracleConnectMode {
    if idx == 1 {
        OracleConnectMode::TnsAlias
    } else {
        OracleConnectMode::Direct
    }
}

fn choice_index_from_oracle_connect_mode(mode: OracleConnectMode) -> i32 {
    match mode {
        OracleConnectMode::Direct => 0,
        OracleConnectMode::TnsAlias => 1,
    }
}

fn oracle_connect_mode_for_info(info: &ConnectionInfo) -> OracleConnectMode {
    if info.uses_oracle_tns_alias() {
        OracleConnectMode::TnsAlias
    } else {
        OracleConnectMode::Direct
    }
}

fn sync_oracle_mode_memory_from_form(
    memory: &Arc<Mutex<OracleModeFieldMemory>>,
    mode: OracleConnectMode,
    host_input: &Input,
    port_input: &Input,
    service_input: &Input,
) {
    let mut memory = memory
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    match mode {
        OracleConnectMode::Direct => {
            memory.direct_host = host_input.value();
            memory.direct_port = port_input.value();
            memory.direct_service = service_input.value();
        }
        OracleConnectMode::TnsAlias => {
            memory.tns_alias = service_input.value();
        }
    }
}

fn sync_oracle_mode_memory_from_info(
    memory: &Arc<Mutex<OracleModeFieldMemory>>,
    info: &ConnectionInfo,
) {
    let mut memory = memory
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    match oracle_connect_mode_for_info(info) {
        OracleConnectMode::Direct => {
            memory.direct_host = info.host.clone();
            memory.direct_port = info.port.to_string();
            memory.direct_service = info.service_name.clone();
        }
        OracleConnectMode::TnsAlias => {
            memory.tns_alias = info.service_name.clone();
        }
    }
}

fn set_form_row_visible(form_col: &mut Flex, row: &mut Flex, visible: bool) {
    if visible {
        row.show();
        form_col.fixed(row, INPUT_ROW_HEIGHT);
    } else {
        row.hide();
        form_col.fixed(row, 0);
    }
    form_col.redraw();
}

fn replace_default_form_values_for_db_switch(
    previous_db_type: DatabaseType,
    next_db_type: DatabaseType,
    host_input: &mut Input,
    port_input: &mut Input,
    service_input: &mut Input,
) {
    let previous = previous_db_type.connection_form_spec();
    let next = next_db_type.connection_form_spec();

    if host_input.value().trim().is_empty() || host_input.value() == previous.default_host {
        host_input.set_value(next.default_host);
    }
    if port_input.value().trim().is_empty()
        || port_input.value() == previous.default_port.to_string()
    {
        port_input.set_value(&next.default_port.to_string());
    }
    if service_input.value() == previous.default_service_name {
        service_input.set_value(next.default_service_name);
    }
}

fn apply_connection_form_mode(
    form_col: &mut Flex,
    db_type: DatabaseType,
    oracle_mode: OracleConnectMode,
    mode_choice: &mut Choice,
    oracle_mode_row: &mut Flex,
    host_row: &mut Flex,
    port_row: &mut Flex,
    svc_label: &mut Frame,
    host_input: &mut Input,
    port_input: &mut Input,
    service_input: &mut Input,
    memory: &Arc<Mutex<OracleModeFieldMemory>>,
) {
    if db_type.supports_tns_alias() {
        set_form_row_visible(form_col, oracle_mode_row, true);
        mode_choice.activate();
        let mut memory = memory
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (svc_label_text, host_value, port_value, service_value) =
            oracle_form_values_for_mode(oracle_mode, &mut memory);
        svc_label.set_label(svc_label_text);
        host_input.set_value(&host_value);
        port_input.set_value(&port_value);
        service_input.set_value(&service_value);
        match oracle_mode {
            OracleConnectMode::Direct => {
                set_form_row_visible(form_col, host_row, true);
                set_form_row_visible(form_col, port_row, true);
                host_input.activate();
                port_input.activate();
            }
            OracleConnectMode::TnsAlias => {
                set_form_row_visible(form_col, host_row, false);
                set_form_row_visible(form_col, port_row, false);
                host_input.deactivate();
                port_input.deactivate();
            }
        }
    } else {
        let form = db_type.connection_form_spec();
        set_form_row_visible(form_col, oracle_mode_row, false);
        mode_choice.set_value(choice_index_from_oracle_connect_mode(
            OracleConnectMode::Direct,
        ));
        mode_choice.deactivate();
        set_form_row_visible(form_col, host_row, true);
        set_form_row_visible(form_col, port_row, true);
        svc_label.set_label(form.service_name_form_label);
        host_input.activate();
        port_input.activate();
        if host_input.value().trim().is_empty() {
            host_input.set_value(form.default_host);
        }
        if port_input.value().trim().is_empty() {
            port_input.set_value(&form.default_port.to_string());
        }
    }
}

fn build_connection_info(
    name: &str,
    username: &str,
    password: &str,
    host: &str,
    port_text: &str,
    service_name: &str,
    db_type: DatabaseType,
    oracle_mode: OracleConnectMode,
) -> Result<ConnectionInfo, String> {
    fn is_valid_host(host: &str) -> bool {
        if host.is_empty() {
            return false;
        }
        let is_ipv6_bracketed = host.starts_with('[')
            && host.ends_with(']')
            && host[1..host.len().saturating_sub(1)]
                .chars()
                .all(|ch| ch.is_ascii_hexdigit() || ch == ':');
        if is_ipv6_bracketed {
            return true;
        }
        host.chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-'))
    }

    fn is_valid_service_name(service_name: &str) -> bool {
        if service_name.is_empty() {
            return false;
        }
        service_name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '$' | '#' | '/'))
    }

    let name = name.trim();
    let username = username.trim();
    let host = host.trim();
    let service_name = service_name.trim();
    let port_text = port_text.trim();

    if name.is_empty() {
        return Err("Connection name is required".to_string());
    }
    if username.is_empty() {
        return Err("Username is required".to_string());
    }
    if password.is_empty() {
        return Err("Password is required".to_string());
    }
    let using_tns_alias =
        db_type.supports_tns_alias() && oracle_mode == OracleConnectMode::TnsAlias;
    let form = db_type.connection_form_spec();
    let svc_label = if using_tns_alias {
        "TNS alias"
    } else {
        form.service_name_value_label
    };
    let requires_service_name = using_tns_alias || form.service_name_required;
    if requires_service_name && service_name.is_empty() {
        return Err(format!("{} is required", svc_label));
    }
    if !service_name.is_empty() && !is_valid_service_name(service_name) {
        return Err(format!("{} contains invalid characters", svc_label));
    }

    let (host, port) = if using_tns_alias {
        (String::new(), 0)
    } else {
        if host.is_empty() {
            return Err("Host is required".to_string());
        }
        if !is_valid_host(host) {
            return Err("Host contains invalid characters".to_string());
        }

        let port = port_text
            .parse::<u16>()
            .map_err(|_| "Port must be a valid number between 0 and 65535".to_string())?;

        if port == 0 {
            return Err("Port must be between 1 and 65535".to_string());
        }

        (host.to_string(), port)
    };

    Ok(ConnectionInfo::new_with_type(
        name,
        username,
        password,
        &host,
        port,
        service_name,
        db_type,
    ))
}

fn resolved_password_for_saved_connection(
    current_connection_name: &str,
    selected_connection_name: &str,
    current_input: &str,
    loaded_password: Option<String>,
) -> String {
    match loaded_password {
        Some(password) => password,
        None => {
            if current_connection_name.trim() == selected_connection_name.trim() {
                current_input.to_string()
            } else {
                String::new()
            }
        }
    }
}

impl ConnectionDialog {
    pub fn show_with_registry(popups: Arc<Mutex<Vec<Window>>>) -> Option<ConnectionInfo> {
        enum DialogMessage {
            DeleteSelected,
            Test(ConnectionInfo),
            TestResult(Result<(), String>),
            Save(ConnectionInfo),
            Connect(ConnectionInfo, bool),
            SetTestInProgress(bool),
            Cancel,
        }

        let (sender, receiver) = mpsc::channel::<DialogMessage>();

        let result: Arc<Mutex<Option<ConnectionInfo>>> = Arc::new(Mutex::new(None));
        let config = Arc::new(Mutex::new(AppConfig::load()));
        let test_in_progress = Arc::new(Mutex::new(false));

        let current_group = fltk::group::Group::try_current();
        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let dialog_w = 620;
        // Oracle connection mode adds one more form row, so keep enough vertical
        // space for the bottom action buttons to remain fully visible.
        let dialog_h = 412 + INPUT_ROW_HEIGHT + DIALOG_SPACING;
        let mut dialog = Window::default()
            .with_size(dialog_w, dialog_h)
            .with_label("Connect to Database");
        center_on_main(&mut dialog);
        dialog.set_color(theme::panel_raised());
        dialog.make_modal(true);

        // Root layout: horizontal split — left panel (saved list) | right panel (form)
        let mut root = Flex::default().with_pos(0, 0).with_size(dialog_w, dialog_h);
        root.set_type(fltk::group::FlexType::Row);
        root.set_margin(DIALOG_MARGIN);
        root.set_spacing(DIALOG_SPACING + 4);

        // ── Left panel: Saved Connections ──
        let left_w = 200;
        let mut left_col = Flex::default();
        left_col.set_type(fltk::group::FlexType::Column);
        left_col.set_spacing(DIALOG_SPACING);

        let mut saved_header = Frame::default().with_label("Saved Connections");
        saved_header.set_label_color(theme::text_secondary());
        left_col.fixed(&saved_header, LABEL_ROW_HEIGHT);

        let mut saved_browser = HoldBrowser::default();
        saved_browser.set_color(theme::input_bg());
        saved_browser.set_selection_color(theme::selection_strong());

        // Load saved connections
        {
            let cfg = config
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for conn in cfg.get_all_connections() {
                saved_browser.add(&conn.name);
            }
        }

        let mut delete_btn = Button::default().with_label("Delete");
        delete_btn.set_color(theme::button_danger());
        delete_btn.set_label_color(theme::text_primary());
        delete_btn.set_frame(FrameType::RFlatBox);
        left_col.fixed(&delete_btn, BUTTON_HEIGHT);

        left_col.end();
        root.fixed(&left_col, left_w);

        // ── Right panel: Connection form ──
        let mut right_col = Flex::default();
        right_col.set_type(fltk::group::FlexType::Column);
        right_col.set_spacing(DIALOG_SPACING);

        let mut details_header = Frame::default().with_label("Connection Details");
        details_header.set_label_color(theme::text_secondary());
        right_col.fixed(&details_header, LABEL_ROW_HEIGHT);

        // Database Type selector
        let mut dbtype_flex = Flex::default();
        dbtype_flex.set_type(fltk::group::FlexType::Row);
        let mut dbtype_label = Frame::default().with_label("DB Type:");
        dbtype_label.set_label_color(theme::text_primary());
        dbtype_flex.fixed(&dbtype_label, FORM_LABEL_WIDTH);
        let mut dbtype_choice = Choice::default();
        let db_choices = DatabaseType::supported()
            .iter()
            .map(|db_type| db_type.choice_label())
            .collect::<Vec<_>>()
            .join("|");
        dbtype_choice.add_choice(&db_choices);
        dbtype_choice.set_value(0); // Oracle by default
        dbtype_choice.set_color(theme::input_bg());
        dbtype_choice.set_text_color(theme::text_primary());
        dbtype_flex.end();
        right_col.fixed(&dbtype_flex, INPUT_ROW_HEIGHT);

        let mut oracle_mode_flex = Flex::default();
        oracle_mode_flex.set_type(fltk::group::FlexType::Row);
        let mut oracle_mode_label = Frame::default().with_label("Oracle Mode:");
        oracle_mode_label.set_label_color(theme::text_primary());
        oracle_mode_flex.fixed(&oracle_mode_label, FORM_LABEL_WIDTH);
        let mut oracle_mode_choice = Choice::default();
        oracle_mode_choice.add_choice("Host + Port + Service|TNS Alias");
        oracle_mode_choice.set_value(0);
        oracle_mode_choice.set_color(theme::input_bg());
        oracle_mode_choice.set_text_color(theme::text_primary());
        oracle_mode_flex.end();
        right_col.fixed(&oracle_mode_flex, INPUT_ROW_HEIGHT);

        // Connection Name
        let mut name_flex = Flex::default();
        name_flex.set_type(fltk::group::FlexType::Row);
        let mut name_label = Frame::default().with_label("Name:");
        name_label.set_label_color(theme::text_primary());
        name_flex.fixed(&name_label, FORM_LABEL_WIDTH);
        let mut name_input = Input::default();
        name_input.set_value("My Connection");
        name_input.set_color(theme::input_bg());
        name_input.set_text_color(theme::text_primary());
        name_flex.end();
        right_col.fixed(&name_flex, INPUT_ROW_HEIGHT);

        // Username
        let mut user_flex = Flex::default();
        user_flex.set_type(fltk::group::FlexType::Row);
        let mut user_label = Frame::default().with_label("Username:");
        user_label.set_label_color(theme::text_primary());
        user_flex.fixed(&user_label, FORM_LABEL_WIDTH);
        let mut user_input = Input::default();
        user_input.set_color(theme::input_bg());
        user_input.set_text_color(theme::text_primary());
        user_flex.end();
        right_col.fixed(&user_flex, INPUT_ROW_HEIGHT);

        // Password
        let mut pass_flex = Flex::default();
        pass_flex.set_type(fltk::group::FlexType::Row);
        let mut pass_label = Frame::default().with_label("Password:");
        pass_label.set_label_color(theme::text_primary());
        pass_flex.fixed(&pass_label, FORM_LABEL_WIDTH);
        let mut pass_input = SecretInput::default();
        pass_input.set_color(theme::input_bg());
        pass_input.set_text_color(theme::text_primary());
        pass_flex.end();
        right_col.fixed(&pass_flex, INPUT_ROW_HEIGHT);

        let initial_form = DatabaseType::Oracle.connection_form_spec();

        // Host
        let mut host_flex = Flex::default();
        host_flex.set_type(fltk::group::FlexType::Row);
        let mut host_label = Frame::default().with_label("Host:");
        host_label.set_label_color(theme::text_primary());
        host_flex.fixed(&host_label, FORM_LABEL_WIDTH);
        let mut host_input = Input::default();
        host_input.set_value(initial_form.default_host);
        host_input.set_color(theme::input_bg());
        host_input.set_text_color(theme::text_primary());
        host_flex.end();
        right_col.fixed(&host_flex, INPUT_ROW_HEIGHT);

        // Port
        let mut port_flex = Flex::default();
        port_flex.set_type(fltk::group::FlexType::Row);
        let mut port_label = Frame::default().with_label("Port:");
        port_label.set_label_color(theme::text_primary());
        port_flex.fixed(&port_label, FORM_LABEL_WIDTH);
        let mut port_input = Input::default();
        port_input.set_value(&initial_form.default_port.to_string());
        port_input.set_color(theme::input_bg());
        port_input.set_text_color(theme::text_primary());
        port_flex.end();
        right_col.fixed(&port_flex, INPUT_ROW_HEIGHT);

        // Service
        let mut service_flex = Flex::default();
        service_flex.set_type(fltk::group::FlexType::Row);
        let mut svc_label = Frame::default().with_label("Service:");
        svc_label.set_label_color(theme::text_primary());
        service_flex.fixed(&svc_label, FORM_LABEL_WIDTH);
        let mut service_input = Input::default();
        service_input.set_value(initial_form.default_service_name);
        service_input.set_color(theme::input_bg());
        service_input.set_text_color(theme::text_primary());
        service_flex.end();
        right_col.fixed(&service_flex, INPUT_ROW_HEIGHT);

        let oracle_mode_memory = Arc::new(Mutex::new(OracleModeFieldMemory {
            direct_host: host_input.value(),
            direct_port: port_input.value(),
            direct_service: service_input.value(),
            tns_alias: String::new(),
        }));
        let current_oracle_mode = Arc::new(Mutex::new(OracleConnectMode::Direct));
        let current_db_type = Arc::new(Mutex::new(DatabaseType::Oracle));

        // Save connection button
        let mut save_flex = Flex::default();
        save_flex.set_type(fltk::group::FlexType::Row);
        let _spacer = Frame::default();
        save_flex.fixed(&_spacer, FORM_LABEL_WIDTH);
        let mut save_btn = Button::default().with_label("Save this connection");
        save_btn.set_color(theme::button_success());
        save_btn.set_label_color(theme::text_primary());
        save_btn.set_frame(FrameType::RFlatBox);
        save_flex.end();
        right_col.fixed(&save_flex, CHECKBOX_ROW_HEIGHT);

        // Flexible spacer to push buttons to bottom
        let spacer_frame = Frame::default();
        right_col.resizable(&spacer_frame);

        // Buttons row
        let mut button_flex = Flex::default();
        button_flex.set_type(fltk::group::FlexType::Row);
        button_flex.set_spacing(DIALOG_SPACING);

        let _btn_spacer = Frame::default();

        let mut test_btn = Button::default()
            .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
            .with_label("Test");
        test_btn.set_color(theme::button_secondary());
        test_btn.set_label_color(theme::text_primary());
        test_btn.set_frame(FrameType::RFlatBox);

        let mut connect_btn = Button::default()
            .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
            .with_label("Connect");
        connect_btn.set_color(theme::button_primary());
        connect_btn.set_label_color(theme::text_primary());
        connect_btn.set_frame(FrameType::RFlatBox);

        let mut cancel_btn = Button::default()
            .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
            .with_label("Cancel");
        cancel_btn.set_color(theme::button_cancel());
        cancel_btn.set_label_color(theme::text_primary());
        cancel_btn.set_frame(FrameType::RFlatBox);

        button_flex.fixed(&test_btn, BUTTON_WIDTH);
        button_flex.fixed(&connect_btn, BUTTON_WIDTH);
        button_flex.fixed(&cancel_btn, BUTTON_WIDTH);
        button_flex.end();
        right_col.fixed(&button_flex, BUTTON_ROW_HEIGHT);

        right_col.end();

        root.end();
        dialog.end();
        fltk::group::Group::set_current(current_group.as_ref());

        // DB Type change callback: update port and service_name label/defaults
        {
            let oracle_mode_memory_dt = Arc::clone(&oracle_mode_memory);
            let current_oracle_mode_dt = Arc::clone(&current_oracle_mode);
            let current_db_type_dt = Arc::clone(&current_db_type);
            let mut right_col_dt = right_col.clone();
            let mut oracle_mode_choice_dt = oracle_mode_choice.clone();
            let mut oracle_mode_flex_dt = oracle_mode_flex.clone();
            let mut host_flex_dt = host_flex.clone();
            let mut port_flex_dt = port_flex.clone();
            let mut port_input_dt = port_input.clone();
            let mut host_input_dt = host_input.clone();
            let mut service_input_dt = service_input.clone();
            let mut svc_label_dt = svc_label.clone();
            dbtype_choice.set_callback(move |choice| {
                let db_type = db_type_from_choice_index(choice.value());
                let previous_db_type = *current_db_type_dt
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let previous_oracle_mode = *current_oracle_mode_dt
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if previous_db_type.supports_tns_alias() {
                    sync_oracle_mode_memory_from_form(
                        &oracle_mode_memory_dt,
                        previous_oracle_mode,
                        &host_input_dt,
                        &port_input_dt,
                        &service_input_dt,
                    );
                }
                if db_type.supports_tns_alias() {
                    oracle_mode_choice_dt
                        .set_value(choice_index_from_oracle_connect_mode(previous_oracle_mode));
                } else {
                    replace_default_form_values_for_db_switch(
                        previous_db_type,
                        db_type,
                        &mut host_input_dt,
                        &mut port_input_dt,
                        &mut service_input_dt,
                    );
                }
                apply_connection_form_mode(
                    &mut right_col_dt,
                    db_type,
                    oracle_connect_mode_from_choice_index(oracle_mode_choice_dt.value()),
                    &mut oracle_mode_choice_dt,
                    &mut oracle_mode_flex_dt,
                    &mut host_flex_dt,
                    &mut port_flex_dt,
                    &mut svc_label_dt,
                    &mut host_input_dt,
                    &mut port_input_dt,
                    &mut service_input_dt,
                    &oracle_mode_memory_dt,
                );
                *current_db_type_dt
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = db_type;
            });
        }

        {
            let oracle_mode_memory_cb = Arc::clone(&oracle_mode_memory);
            let current_oracle_mode_cb = Arc::clone(&current_oracle_mode);
            let mut right_col_cb = right_col.clone();
            let mut oracle_mode_choice_cb = oracle_mode_choice.clone();
            let mut oracle_mode_flex_cb = oracle_mode_flex.clone();
            let mut host_flex_cb = host_flex.clone();
            let mut port_flex_cb = port_flex.clone();
            let dbtype_choice_cb = dbtype_choice.clone();
            let mut host_input_cb = host_input.clone();
            let mut port_input_cb = port_input.clone();
            let mut service_input_cb = service_input.clone();
            let mut svc_label_cb = svc_label.clone();
            oracle_mode_choice.set_callback(move |_| {
                let previous_mode = *current_oracle_mode_cb
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let next_mode =
                    oracle_connect_mode_from_choice_index(oracle_mode_choice_cb.value());
                sync_oracle_mode_memory_from_form(
                    &oracle_mode_memory_cb,
                    previous_mode,
                    &host_input_cb,
                    &port_input_cb,
                    &service_input_cb,
                );
                apply_connection_form_mode(
                    &mut right_col_cb,
                    db_type_from_choice_index(dbtype_choice_cb.value()),
                    next_mode,
                    &mut oracle_mode_choice_cb,
                    &mut oracle_mode_flex_cb,
                    &mut host_flex_cb,
                    &mut port_flex_cb,
                    &mut svc_label_cb,
                    &mut host_input_cb,
                    &mut port_input_cb,
                    &mut service_input_cb,
                    &oracle_mode_memory_cb,
                );
                *current_oracle_mode_cb
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = next_mode;
            });
        }

        apply_connection_form_mode(
            &mut right_col,
            DatabaseType::Oracle,
            OracleConnectMode::Direct,
            &mut oracle_mode_choice,
            &mut oracle_mode_flex,
            &mut host_flex,
            &mut port_flex,
            &mut svc_label,
            &mut host_input,
            &mut port_input,
            &mut service_input,
            &oracle_mode_memory,
        );

        let mut connect_btn_for_enter = connect_btn.clone();
        dialog.handle(move |_, ev| match ev {
            Event::KeyDown => {
                if matches!(app::event_key(), Key::Enter | Key::KPEnter) {
                    connect_btn_for_enter.do_callback();
                    true
                } else {
                    false
                }
            }
            _ => false,
        });

        popups
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(dialog.clone());

        // Saved connection selection callback
        let config_cb = config.clone();
        let mut name_input_cb = name_input.clone();
        let mut user_input_cb = user_input.clone();
        let mut pass_input_cb = pass_input.clone();
        let mut host_input_cb = host_input.clone();
        let mut port_input_cb = port_input.clone();
        let mut service_input_cb = service_input.clone();
        let mut dbtype_choice_cb = dbtype_choice.clone();
        let mut oracle_mode_choice_cb = oracle_mode_choice.clone();
        let mut right_col_saved = right_col.clone();
        let mut oracle_mode_flex_saved = oracle_mode_flex.clone();
        let mut host_flex_saved = host_flex.clone();
        let mut port_flex_saved = port_flex.clone();
        let mut svc_label_cb = svc_label.clone();
        let oracle_mode_memory_saved = Arc::clone(&oracle_mode_memory);
        let current_oracle_mode_saved = Arc::clone(&current_oracle_mode);
        let current_db_type_saved = Arc::clone(&current_db_type);
        let sender_for_click = sender.clone();

        saved_browser.set_callback(move |browser| {
            if let Some(selected) = browser.selected_text() {
                let cfg = config_cb
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if let Some(conn) = cfg.get_connection_by_name(&selected) {
                    let previous_connection_name = name_input_cb.value();
                    name_input_cb.set_value(&conn.name);
                    user_input_cb.set_value(&conn.username);
                    // Load password from OS keyring on demand.
                    let mut keyring_load_failed = false;
                    let password = match AppConfig::get_password_for_connection(&conn.name) {
                        Ok(password_opt) => {
                            resolved_password_for_saved_connection(
                                &previous_connection_name,
                                &conn.name,
                                &pass_input_cb.value(),
                                password_opt,
                            )
                        }
                        Err(err) => {
                            keyring_load_failed = true;
                            fltk::dialog::alert_default(&err);
                            pass_input_cb.value()
                        }
                    };
                    if !keyring_load_failed {
                        pass_input_cb.set_value(&password);
                    }
                    sync_oracle_mode_memory_from_info(&oracle_mode_memory_saved, conn);
                    let oracle_mode = oracle_connect_mode_for_info(conn);
                    *current_oracle_mode_saved
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = oracle_mode;
                    *current_db_type_saved
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = conn.db_type;
                    oracle_mode_choice_cb
                        .set_value(choice_index_from_oracle_connect_mode(oracle_mode));
                    dbtype_choice_cb.set_value(choice_index_from_db_type(conn.db_type));
                    if !conn.db_type.supports_tns_alias() {
                        service_input_cb.set_value(&conn.service_name);
                        host_input_cb.set_value(&conn.host);
                        port_input_cb.set_value(&conn.port.to_string());
                    }
                    apply_connection_form_mode(
                        &mut right_col_saved,
                        conn.db_type,
                        oracle_mode,
                        &mut oracle_mode_choice_cb,
                        &mut oracle_mode_flex_saved,
                        &mut host_flex_saved,
                        &mut port_flex_saved,
                        &mut svc_label_cb,
                        &mut host_input_cb,
                        &mut port_input_cb,
                        &mut service_input_cb,
                        &oracle_mode_memory_saved,
                    );

                    // Double click to connect immediately
                    if app::event_clicks() && !keyring_load_failed {
                        if password.is_empty() {
                            fltk::dialog::alert_default(
                                "No password is saved for this connection. Enter a password before connecting.",
                            );
                            return;
                        }
                        let info = ConnectionInfo::new_with_type(
                            &conn.name,
                            &conn.username,
                            &password,
                            &conn.host,
                            conn.port,
                            &conn.service_name,
                            conn.db_type,
                        );
                        let _ = sender_for_click.send(DialogMessage::Connect(info, false));
                        app::awake();
                    }
                }
            }
        });

        // Delete button callback
        let sender_for_delete = sender.clone();
        delete_btn.set_callback(move |_| {
            let _ = sender_for_delete.send(DialogMessage::DeleteSelected);
            app::awake();
        });

        // Save button callback
        let sender_for_save = sender.clone();
        let name_input_save = name_input.clone();
        let user_input_save = user_input.clone();
        let pass_input_save = pass_input.clone();
        let host_input_save = host_input.clone();
        let port_input_save = port_input.clone();
        let service_input_save = service_input.clone();
        let dbtype_choice_save = dbtype_choice.clone();
        let oracle_mode_choice_save = oracle_mode_choice.clone();

        save_btn.set_callback(move |_| {
            let info = match build_connection_info(
                &name_input_save.value(),
                &user_input_save.value(),
                &pass_input_save.value(),
                &host_input_save.value(),
                &port_input_save.value(),
                &service_input_save.value(),
                db_type_from_choice_index(dbtype_choice_save.value()),
                oracle_connect_mode_from_choice_index(oracle_mode_choice_save.value()),
            ) {
                Ok(info) => info,
                Err(message) => {
                    fltk::dialog::alert_default(&message);
                    return;
                }
            };

            let _ = sender_for_save.send(DialogMessage::Save(info));
            app::awake();
        });

        // Test button callback
        let sender_for_test = sender.clone();
        let mut test_btn_for_toggle = test_btn.clone();
        let test_in_progress_for_test = test_in_progress.clone();
        let name_input_test = name_input.clone();
        let user_input_test = user_input.clone();
        let pass_input_test = pass_input.clone();
        let host_input_test = host_input.clone();
        let port_input_test = port_input.clone();
        let service_input_test = service_input.clone();
        let dbtype_choice_test = dbtype_choice.clone();
        let oracle_mode_choice_test = oracle_mode_choice.clone();

        test_btn.set_callback(move |_| {
            {
                let mut guard = test_in_progress_for_test
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if *guard {
                    return;
                }
                *guard = true;
            }

            let info = match build_connection_info(
                &name_input_test.value(),
                &user_input_test.value(),
                &pass_input_test.value(),
                &host_input_test.value(),
                &port_input_test.value(),
                &service_input_test.value(),
                db_type_from_choice_index(dbtype_choice_test.value()),
                oracle_connect_mode_from_choice_index(oracle_mode_choice_test.value()),
            ) {
                Ok(info) => info,
                Err(message) => {
                    fltk::dialog::alert_default(&message);
                    *test_in_progress_for_test
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = false;
                    return;
                }
            };

            test_btn_for_toggle.deactivate();
            let _ = sender_for_test.send(DialogMessage::SetTestInProgress(true));
            let _ = sender_for_test.send(DialogMessage::Test(info));
            app::awake();
        });

        // Connect button callback
        let sender_for_connect = sender.clone();
        let name_input_conn = name_input.clone();
        let user_input_conn = user_input.clone();
        let pass_input_conn = pass_input.clone();
        let host_input_conn = host_input.clone();
        let port_input_conn = port_input.clone();
        let service_input_conn = service_input.clone();
        let dbtype_choice_conn = dbtype_choice.clone();
        let oracle_mode_choice_conn = oracle_mode_choice.clone();

        connect_btn.set_callback(move |_| {
            let info = match build_connection_info(
                &name_input_conn.value(),
                &user_input_conn.value(),
                &pass_input_conn.value(),
                &host_input_conn.value(),
                &port_input_conn.value(),
                &service_input_conn.value(),
                db_type_from_choice_index(dbtype_choice_conn.value()),
                oracle_connect_mode_from_choice_index(oracle_mode_choice_conn.value()),
            ) {
                Ok(info) => info,
                Err(message) => {
                    fltk::dialog::alert_default(&message);
                    return;
                }
            };

            let _ = sender_for_connect.send(DialogMessage::Connect(info, false));
            app::awake();
        });

        // Cancel button callback
        let sender_for_cancel = sender.clone();
        cancel_btn.set_callback(move |_| {
            let _ = sender_for_cancel.send(DialogMessage::Cancel);
            app::awake();
        });

        dialog.show();
        let _ = dialog.take_focus();
        let _ = connect_btn.take_focus();

        let mut saved_browser = saved_browser.clone();
        while dialog.shown() {
            app::wait();
            while let Ok(message) = receiver.try_recv() {
                match message {
                    DialogMessage::DeleteSelected => {
                        if let Some(selected) = saved_browser.selected_text() {
                            let choice = fltk::dialog::choice2_default(
                                &format!("Delete connection '{}'?", selected),
                                "Cancel",
                                "Delete",
                                "",
                            );
                            if choice == Some(1) {
                                let mut cfg = config
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                                let previous_config = cfg.clone();
                                let removal_error = cfg.remove_connection(&selected).err();
                                if let Err(e) = cfg.save() {
                                    *cfg = previous_config;
                                    fltk::dialog::alert_default(&format!(
                                        "Failed to save config: {}",
                                        e
                                    ));
                                } else {
                                    saved_browser.clear();
                                    for conn in cfg.get_all_connections() {
                                        saved_browser.add(&conn.name);
                                    }
                                    if let Some(error_message) = removal_error {
                                        fltk::dialog::alert_default(&error_message);
                                    }
                                }
                            }
                        } else {
                            fltk::dialog::alert_default("Please select a connection to delete");
                        }
                    }
                    DialogMessage::Test(info) => {
                        let sender = sender.clone();
                        thread::spawn(move || {
                            let result = DatabaseConnection::test_connection(&info);
                            let _ = sender.send(DialogMessage::TestResult(result));
                            let _ = sender.send(DialogMessage::SetTestInProgress(false));
                            app::awake();
                        });
                    }
                    DialogMessage::SetTestInProgress(in_progress) => {
                        *test_in_progress
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) = in_progress;
                        if in_progress {
                            test_btn.deactivate();
                        } else {
                            test_btn.activate();
                        }
                    }
                    DialogMessage::TestResult(result) => match result {
                        Ok(_) => {
                            fltk::dialog::message_default("Connection successful!");
                        }
                        Err(e) => {
                            fltk::dialog::alert_default(&format!("Connection failed: {}", e));
                        }
                    },
                    DialogMessage::Save(info) => {
                        let mut cfg = config
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        if let Err(e) = cfg.add_recent_connection(info.clone()) {
                            fltk::dialog::alert_default(&e);
                        } else if let Err(e) = cfg.save() {
                            let cleanup_error =
                                crate::utils::credential_store::delete_password(&info.name).err();
                            cfg.recent_connections.retain(|c| c.name != info.name);
                            let mut message = format!("Failed to save connection: {}", e);
                            if let Some(cleanup_error) = cleanup_error {
                                message.push_str(&format!(
                                    "\nAdditionally failed to roll back keyring entry: {}",
                                    cleanup_error
                                ));
                            }
                            fltk::dialog::alert_default(&message);
                        } else {
                            saved_browser.clear();
                            for conn in cfg.get_all_connections() {
                                saved_browser.add(&conn.name);
                            }
                        }
                    }
                    DialogMessage::Connect(info, save_connection) => {
                        if save_connection {
                            let mut cfg = config
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                            if let Err(e) = cfg.add_recent_connection(info.clone()) {
                                fltk::dialog::alert_default(&e);
                                continue;
                            }
                            if let Err(e) = cfg.save() {
                                let cleanup_error =
                                    crate::utils::credential_store::delete_password(&info.name)
                                        .err();
                                cfg.recent_connections.retain(|c| c.name != info.name);
                                let mut message = format!("Failed to save connection: {}", e);
                                if let Some(cleanup_error) = cleanup_error {
                                    message.push_str(&format!(
                                        "\nAdditionally failed to roll back keyring entry: {}",
                                        cleanup_error
                                    ));
                                }
                                fltk::dialog::alert_default(&message);
                                continue;
                            }
                        }

                        *result
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(info);
                        dialog.hide();
                    }
                    DialogMessage::Cancel => {
                        dialog.hide();
                    }
                }
            }
        }

        // Clear password input field to minimize password lifetime in memory
        pass_input.set_value("");

        // Remove dialog from popups to prevent memory leak
        popups
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|w| w.as_widget_ptr() != dialog.as_widget_ptr());

        // Explicitly destroy top-level dialog widgets to release native resources.
        Window::delete(dialog);

        // IMPORTANT: Do not clear password here.
        // The returned ConnectionInfo is consumed immediately by the caller to perform
        // DB login, and clearing it at this point makes connection impossible.
        // Password memory cleanup is handled after successful connect in the connection flow.
        let final_result = result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        final_result
    }
}

#[cfg(test)]
mod tests {
    use super::{build_connection_info, OracleConnectMode};
    use crate::db::DatabaseType;

    #[test]
    fn build_connection_info_rejects_empty_required_fields() {
        let result = build_connection_info(
            " ",
            "scott",
            "tiger",
            "localhost",
            "1521",
            "ORCL",
            DatabaseType::Oracle,
            OracleConnectMode::Direct,
        );
        assert!(result.is_err());

        let result = build_connection_info(
            "local",
            "",
            "tiger",
            "localhost",
            "1521",
            "ORCL",
            DatabaseType::Oracle,
            OracleConnectMode::Direct,
        );
        assert!(result.is_err());

        let result = build_connection_info(
            "local",
            "scott",
            "tiger",
            "",
            "1521",
            "ORCL",
            DatabaseType::Oracle,
            OracleConnectMode::Direct,
        );
        assert!(result.is_err());

        let result = build_connection_info(
            "local",
            "scott",
            "tiger",
            "localhost",
            "1521",
            "",
            DatabaseType::Oracle,
            OracleConnectMode::Direct,
        );
        assert!(result.is_err());
    }

    #[test]
    fn build_connection_info_rejects_invalid_port() {
        let result = build_connection_info(
            "local",
            "scott",
            "tiger",
            "localhost",
            "abc",
            "ORCL",
            DatabaseType::Oracle,
            OracleConnectMode::Direct,
        );
        assert!(result.is_err());

        let result = build_connection_info(
            "local",
            "scott",
            "tiger",
            "localhost",
            "0",
            "ORCL",
            DatabaseType::Oracle,
            OracleConnectMode::Direct,
        );
        assert!(result.is_err());
    }

    #[test]
    fn build_connection_info_rejects_invalid_host_and_service_characters() {
        let invalid_host = build_connection_info(
            "local",
            "scott",
            "tiger",
            "local host",
            "1521",
            "ORCL",
            DatabaseType::Oracle,
            OracleConnectMode::Direct,
        );
        assert!(invalid_host.is_err());

        let invalid_service = build_connection_info(
            "local",
            "scott",
            "tiger",
            "localhost",
            "1521",
            "ORCL!",
            DatabaseType::Oracle,
            OracleConnectMode::Direct,
        );
        assert!(invalid_service.is_err());
    }

    #[test]
    fn build_connection_info_trims_values_and_builds_info() {
        let info = build_connection_info(
            " local ",
            " scott ",
            "tiger",
            " localhost ",
            " 1521 ",
            " ORCL ",
            DatabaseType::Oracle,
            OracleConnectMode::Direct,
        )
        .expect("should build valid connection info");

        assert_eq!(info.name, "local");
        assert_eq!(info.username, "scott");
        assert_eq!(info.host, "localhost");
        assert_eq!(info.port, 1521);
        assert_eq!(info.service_name, "ORCL");
        assert_eq!(info.db_type, DatabaseType::Oracle);
    }

    #[test]
    fn build_connection_info_allows_empty_mysql_database_name() {
        let info = build_connection_info(
            " local ",
            " root ",
            "secret",
            " localhost ",
            " 3306 ",
            "   ",
            DatabaseType::MySQL,
            OracleConnectMode::Direct,
        )
        .expect("should allow MySQL connection without default database");

        assert_eq!(info.name, "local");
        assert_eq!(info.username, "root");
        assert_eq!(info.host, "localhost");
        assert_eq!(info.port, 3306);
        assert!(info.service_name.is_empty());
        assert_eq!(info.db_type, DatabaseType::MySQL);
    }

    #[test]
    fn build_connection_info_allows_oracle_tns_alias_mode_without_host_or_port() {
        let info = build_connection_info(
            " local ",
            " system ",
            "password",
            " ignored-host ",
            " 1521 ",
            " FREE_LOCAL ",
            DatabaseType::Oracle,
            OracleConnectMode::TnsAlias,
        )
        .expect("should build TNS alias connection info");

        assert_eq!(info.name, "local");
        assert_eq!(info.username, "system");
        assert_eq!(info.host, "");
        assert_eq!(info.port, 0);
        assert_eq!(info.service_name, "FREE_LOCAL");
        assert_eq!(info.db_type, DatabaseType::Oracle);
    }

    #[test]
    fn oracle_mode_switch_restores_direct_values_after_tns_toggle() {
        let mut memory = super::OracleModeFieldMemory {
            direct_host: "db.example.com".to_string(),
            direct_port: "1522".to_string(),
            direct_service: "FREEPDB1".to_string(),
            tns_alias: String::new(),
        };

        let (_, host_value, port_value, service_value) =
            super::oracle_form_values_for_mode(OracleConnectMode::Direct, &mut memory);
        assert_eq!(host_value, "db.example.com");
        assert_eq!(port_value, "1522");
        assert_eq!(service_value, "FREEPDB1");

        memory.tns_alias = "LOCAL_FREE".to_string();
        let (_, host_value, port_value, service_value) =
            super::oracle_form_values_for_mode(OracleConnectMode::TnsAlias, &mut memory);
        assert_eq!(host_value, "");
        assert_eq!(port_value, "");
        assert_eq!(service_value, "LOCAL_FREE");

        let (_, host_value, port_value, service_value) =
            super::oracle_form_values_for_mode(OracleConnectMode::Direct, &mut memory);
        assert_eq!(host_value, "db.example.com");
        assert_eq!(port_value, "1522");
        assert_eq!(service_value, "FREEPDB1");
    }

    #[test]
    fn db_type_choice_indexes_follow_supported_database_order() {
        let supported = DatabaseType::supported();

        assert_eq!(super::db_type_from_choice_index(-1), supported[0]);
        for (idx, db_type) in supported.iter().enumerate() {
            assert_eq!(super::db_type_from_choice_index(idx as i32), *db_type);
        }
        assert_eq!(
            super::db_type_from_choice_index(supported.len() as i32),
            *supported.last().expect("at least one database type")
        );
    }

    #[test]
    fn resolved_password_for_saved_connection_prefers_loaded_password() {
        let resolved = super::resolved_password_for_saved_connection(
            "LOCAL",
            "LOCAL",
            "existing-input",
            Some("from-keyring".to_string()),
        );

        assert_eq!(resolved, "from-keyring");
    }

    #[test]
    fn resolved_password_for_saved_connection_keeps_current_input_when_missing_for_same_connection()
    {
        let resolved =
            super::resolved_password_for_saved_connection("LOCAL", "LOCAL", "typed-password", None);

        assert_eq!(resolved, "typed-password");
    }

    #[test]
    fn resolved_password_for_saved_connection_clears_input_for_other_connection_when_missing() {
        let resolved =
            super::resolved_password_for_saved_connection("LOCAL", "DEV", "typed-password", None);

        assert_eq!(resolved, "");
    }
}
