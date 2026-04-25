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

use crate::db::{
    ConnectionAdvancedSettings, ConnectionInfo, ConnectionSslMode, DatabaseConnection,
    DatabaseType, OracleNetworkProtocol, TransactionAccessMode, TransactionIsolation,
};
use crate::ui::center_on_main;
use crate::ui::constants::*;
use crate::ui::theme;
use crate::utils::AppConfig;

const SAVED_CONNECTIONS_COLUMN_WIDTH: i32 = 200;
const DB_SELECTION_COLUMN_WIDTH: i32 = 250;
const CONNECTION_INFO_COLUMN_WIDTH: i32 = 300;
const ADVANCED_SETTINGS_COLUMN_WIDTH: i32 = 390;
const CONNECTION_DIALOG_COLUMN_SPACING: i32 = DIALOG_SPACING + 4;

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

fn ssl_mode_from_choice_index(db_type: DatabaseType, idx: i32) -> ConnectionSslMode {
    if db_type == DatabaseType::Oracle {
        return if idx == 1 {
            ConnectionSslMode::Required
        } else {
            ConnectionSslMode::Disabled
        };
    }

    match idx {
        1 => ConnectionSslMode::Required,
        2 => ConnectionSslMode::VerifyCa,
        3 => ConnectionSslMode::VerifyIdentity,
        _ => ConnectionSslMode::Disabled,
    }
}

fn choice_index_from_ssl_mode(db_type: DatabaseType, mode: ConnectionSslMode) -> i32 {
    if db_type == DatabaseType::Oracle {
        return if mode == ConnectionSslMode::Disabled {
            0
        } else {
            1
        };
    }

    match mode {
        ConnectionSslMode::Disabled => 0,
        ConnectionSslMode::Required => 1,
        ConnectionSslMode::VerifyCa => 2,
        ConnectionSslMode::VerifyIdentity => 3,
    }
}

fn repopulate_ssl_choice(choice: &mut Choice, db_type: DatabaseType) {
    choice.clear();
    if db_type == DatabaseType::Oracle {
        choice.add_choice("Disabled|Required (TCPS)");
    } else {
        choice.add_choice("Disabled|Required|Verify CA|Verify identity");
    }
}

fn oracle_protocol_from_choice_index(idx: i32) -> OracleNetworkProtocol {
    if idx == 1 {
        OracleNetworkProtocol::Tcps
    } else {
        OracleNetworkProtocol::Tcp
    }
}

fn choice_index_from_oracle_protocol(protocol: OracleNetworkProtocol) -> i32 {
    match protocol {
        OracleNetworkProtocol::Tcp => 0,
        OracleNetworkProtocol::Tcps => 1,
    }
}

fn transaction_isolation_from_choice_index(
    db_type: DatabaseType,
    index: i32,
) -> TransactionIsolation {
    db_type
        .supported_transaction_isolations()
        .get(index.max(0) as usize)
        .copied()
        .unwrap_or(TransactionIsolation::ReadCommitted)
}

fn choice_index_from_transaction_isolation(
    db_type: DatabaseType,
    isolation: TransactionIsolation,
) -> i32 {
    db_type
        .supported_transaction_isolations()
        .iter()
        .position(|candidate| *candidate == isolation)
        .unwrap_or_else(|| {
            db_type
                .supported_transaction_isolations()
                .iter()
                .position(|candidate| *candidate == TransactionIsolation::ReadCommitted)
                .unwrap_or_default()
        }) as i32
}

fn transaction_access_from_choice_index(idx: i32) -> TransactionAccessMode {
    if idx == 1 {
        TransactionAccessMode::ReadOnly
    } else {
        TransactionAccessMode::ReadWrite
    }
}

fn choice_index_from_transaction_access(access: TransactionAccessMode) -> i32 {
    match access {
        TransactionAccessMode::ReadWrite => 0,
        TransactionAccessMode::ReadOnly => 1,
    }
}

fn repopulate_isolation_choice(choice: &mut Choice, db_type: DatabaseType) {
    choice.clear();
    let labels = db_type
        .supported_transaction_isolations()
        .iter()
        .map(|isolation| isolation.label())
        .collect::<Vec<_>>()
        .join("|");
    choice.add_choice(&labels);
}

fn connection_dialog_width() -> i32 {
    DIALOG_MARGIN * 2
        + SAVED_CONNECTIONS_COLUMN_WIDTH
        + CONNECTION_DIALOG_COLUMN_SPACING
        + DB_SELECTION_COLUMN_WIDTH
        + CONNECTION_INFO_COLUMN_WIDTH
        + ADVANCED_SETTINGS_COLUMN_WIDTH
        + CONNECTION_DIALOG_COLUMN_SPACING * 2
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

fn set_advanced_form_values(
    advanced: &ConnectionAdvancedSettings,
    db_type: DatabaseType,
    ssl_choice: &mut Choice,
    isolation_choice: &mut Choice,
    access_choice: &mut Choice,
    timezone_input: &mut Input,
    mysql_sql_mode_input: &mut Input,
    mysql_charset_input: &mut Input,
    mysql_collation_input: &mut Input,
    mysql_ssl_ca_input: &mut Input,
    oracle_protocol_choice: &mut Choice,
    oracle_nls_date_input: &mut Input,
    oracle_nls_timestamp_input: &mut Input,
) {
    repopulate_ssl_choice(ssl_choice, db_type);
    repopulate_isolation_choice(isolation_choice, db_type);
    ssl_choice.set_value(choice_index_from_ssl_mode(db_type, advanced.ssl_mode));
    isolation_choice.set_value(choice_index_from_transaction_isolation(
        db_type,
        advanced.default_transaction_isolation,
    ));
    access_choice.set_value(choice_index_from_transaction_access(
        advanced.default_transaction_access_mode,
    ));
    timezone_input.set_value(&advanced.session_time_zone);
    mysql_sql_mode_input.set_value(&advanced.mysql_sql_mode);
    mysql_charset_input.set_value(&advanced.mysql_charset);
    mysql_collation_input.set_value(&advanced.mysql_collation);
    mysql_ssl_ca_input.set_value(&advanced.mysql_ssl_ca_path);
    oracle_protocol_choice.set_value(choice_index_from_oracle_protocol(advanced.oracle_protocol));
    oracle_nls_date_input.set_value(&advanced.oracle_nls_date_format);
    oracle_nls_timestamp_input.set_value(&advanced.oracle_nls_timestamp_format);
}

fn advanced_settings_from_form(
    db_type: DatabaseType,
    ssl_choice: &Choice,
    isolation_choice: &Choice,
    access_choice: &Choice,
    timezone_input: &Input,
    mysql_sql_mode_input: &Input,
    mysql_charset_input: &Input,
    mysql_collation_input: &Input,
    mysql_ssl_ca_input: &Input,
    oracle_protocol_choice: &Choice,
    oracle_nls_date_input: &Input,
    oracle_nls_timestamp_input: &Input,
) -> ConnectionAdvancedSettings {
    let mut advanced = ConnectionAdvancedSettings::default_for(db_type);
    advanced.ssl_mode = ssl_mode_from_choice_index(db_type, ssl_choice.value());
    advanced.default_transaction_isolation =
        transaction_isolation_from_choice_index(db_type, isolation_choice.value());
    advanced.default_transaction_access_mode =
        transaction_access_from_choice_index(access_choice.value());
    advanced.session_time_zone = timezone_input.value().trim().to_string();
    advanced.mysql_sql_mode = mysql_sql_mode_input.value().trim().to_string();
    advanced.mysql_charset = mysql_charset_input.value().trim().to_string();
    advanced.mysql_collation = mysql_collation_input.value().trim().to_string();
    advanced.mysql_ssl_ca_path = mysql_ssl_ca_input.value().trim().to_string();
    advanced.oracle_protocol = oracle_protocol_from_choice_index(oracle_protocol_choice.value());
    advanced.oracle_nls_date_format = oracle_nls_date_input.value().trim().to_string();
    advanced.oracle_nls_timestamp_format = oracle_nls_timestamp_input.value().trim().to_string();
    advanced
}

fn apply_advanced_form_mode(
    advanced_col: &mut Flex,
    db_type: DatabaseType,
    using_oracle_tns_alias: bool,
    oracle_protocol_row: &mut Flex,
    oracle_nls_date_row: &mut Flex,
    oracle_nls_timestamp_row: &mut Flex,
    mysql_sql_mode_row: &mut Flex,
    mysql_charset_row: &mut Flex,
    mysql_collation_row: &mut Flex,
    mysql_ssl_ca_row: &mut Flex,
    oracle_protocol_choice: &mut Choice,
    ssl_choice: &mut Choice,
) {
    let is_oracle = db_type == DatabaseType::Oracle;
    set_form_row_visible(advanced_col, oracle_protocol_row, is_oracle);
    set_form_row_visible(advanced_col, oracle_nls_date_row, is_oracle);
    set_form_row_visible(advanced_col, oracle_nls_timestamp_row, is_oracle);
    set_form_row_visible(advanced_col, mysql_sql_mode_row, !is_oracle);
    set_form_row_visible(advanced_col, mysql_charset_row, !is_oracle);
    set_form_row_visible(advanced_col, mysql_collation_row, !is_oracle);
    set_form_row_visible(advanced_col, mysql_ssl_ca_row, !is_oracle);

    if is_oracle && using_oracle_tns_alias {
        // TNS alias mode relies on Oracle Net for SSL/protocol; keep the
        // user's previous selections in the form (just disabled) so toggling
        // back to Direct mode restores them. build_connection_info neutralises
        // these fields before validation.
        oracle_protocol_choice.deactivate();
        ssl_choice.deactivate();
    } else {
        oracle_protocol_choice.activate();
        ssl_choice.activate();
    }
}

fn apply_connection_form_mode(
    form_col: &mut Flex,
    oracle_mode_col: &mut Flex,
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
        set_form_row_visible(oracle_mode_col, oracle_mode_row, true);
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
        set_form_row_visible(oracle_mode_col, oracle_mode_row, false);
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
    advanced: ConnectionAdvancedSettings,
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

    let mut advanced = advanced;
    if using_tns_alias {
        // TNS alias uses Oracle Net for SSL/protocol; neutralise the form
        // values that no longer apply so validation succeeds and we do not
        // persist stale SSL state alongside the alias.
        advanced.ssl_mode = ConnectionSslMode::Disabled;
        advanced.oracle_protocol = OracleNetworkProtocol::Tcp;
    }

    advanced.validate_for_db(db_type, using_tns_alias)?;

    let mut info =
        ConnectionInfo::new_with_type(name, username, password, &host, port, service_name, db_type);
    info.advanced = advanced;
    Ok(info)
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

        let dialog_w = connection_dialog_width();
        let dialog_h = 520;
        let mut dialog = Window::default()
            .with_size(dialog_w, dialog_h)
            .with_label("Connect to Database");
        center_on_main(&mut dialog);
        dialog.set_color(theme::panel_raised());
        dialog.make_modal(true);

        // Root layout: saved list | DB selection | connection info | advanced settings
        let mut root = Flex::default().with_pos(0, 0).with_size(dialog_w, dialog_h);
        root.set_type(fltk::group::FlexType::Row);
        root.set_margin(DIALOG_MARGIN);
        root.set_spacing(CONNECTION_DIALOG_COLUMN_SPACING);

        // ── Left panel: Saved Connections ──
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
        root.fixed(&left_col, SAVED_CONNECTIONS_COLUMN_WIDTH);

        let mut details_row = Flex::default();
        details_row.set_type(fltk::group::FlexType::Row);
        details_row.set_spacing(CONNECTION_DIALOG_COLUMN_SPACING);

        // ── DB selection column ──
        let mut db_col = Flex::default();
        db_col.set_type(fltk::group::FlexType::Column);
        db_col.set_spacing(DIALOG_SPACING);

        let mut db_header = Frame::default().with_label("DB Selection");
        db_header.set_label_color(theme::text_secondary());
        db_col.fixed(&db_header, LABEL_ROW_HEIGHT);

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
        db_col.fixed(&dbtype_flex, INPUT_ROW_HEIGHT);

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
        db_col.fixed(&oracle_mode_flex, INPUT_ROW_HEIGHT);

        let db_spacer = Frame::default();
        db_col.resizable(&db_spacer);
        db_col.end();
        details_row.fixed(&db_col, DB_SELECTION_COLUMN_WIDTH);

        // ── Connection form column ──
        let mut right_col = Flex::default();
        right_col.set_type(fltk::group::FlexType::Column);
        right_col.set_spacing(DIALOG_SPACING);

        let mut details_header = Frame::default().with_label("Connection Info");
        details_header.set_label_color(theme::text_secondary());
        right_col.fixed(&details_header, LABEL_ROW_HEIGHT);

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

        let connection_spacer = Frame::default();
        right_col.resizable(&connection_spacer);
        right_col.end();
        details_row.fixed(&right_col, CONNECTION_INFO_COLUMN_WIDTH);

        // ── Advanced settings column ──
        let mut advanced_col = Flex::default();
        advanced_col.set_type(fltk::group::FlexType::Column);
        advanced_col.set_spacing(DIALOG_SPACING);

        let mut advanced_header = Frame::default().with_label("Advanced Settings");
        advanced_header.set_label_color(theme::text_secondary());
        advanced_col.fixed(&advanced_header, LABEL_ROW_HEIGHT);

        let initial_advanced = ConnectionAdvancedSettings::default_for(DatabaseType::Oracle);

        let mut ssl_flex = Flex::default();
        ssl_flex.set_type(fltk::group::FlexType::Row);
        let mut ssl_label = Frame::default().with_label("SSL:");
        ssl_label.set_label_color(theme::text_primary());
        ssl_flex.fixed(&ssl_label, FORM_LABEL_WIDTH);
        let mut ssl_choice = Choice::default();
        repopulate_ssl_choice(&mut ssl_choice, DatabaseType::Oracle);
        ssl_choice.set_value(choice_index_from_ssl_mode(
            DatabaseType::Oracle,
            initial_advanced.ssl_mode,
        ));
        ssl_choice.set_color(theme::input_bg());
        ssl_choice.set_text_color(theme::text_primary());
        ssl_flex.end();
        advanced_col.fixed(&ssl_flex, INPUT_ROW_HEIGHT);

        let mut isolation_flex = Flex::default();
        isolation_flex.set_type(fltk::group::FlexType::Row);
        let mut isolation_label = Frame::default().with_label("Isolation:");
        isolation_label.set_label_color(theme::text_primary());
        isolation_flex.fixed(&isolation_label, FORM_LABEL_WIDTH);
        let mut isolation_choice = Choice::default();
        repopulate_isolation_choice(&mut isolation_choice, DatabaseType::Oracle);
        isolation_choice.set_value(choice_index_from_transaction_isolation(
            DatabaseType::Oracle,
            initial_advanced.default_transaction_isolation,
        ));
        isolation_choice.set_color(theme::input_bg());
        isolation_choice.set_text_color(theme::text_primary());
        isolation_flex.end();
        advanced_col.fixed(&isolation_flex, INPUT_ROW_HEIGHT);

        let mut access_flex = Flex::default();
        access_flex.set_type(fltk::group::FlexType::Row);
        let mut access_label = Frame::default().with_label("Access:");
        access_label.set_label_color(theme::text_primary());
        access_flex.fixed(&access_label, FORM_LABEL_WIDTH);
        let mut access_choice = Choice::default();
        access_choice.add_choice("Read write|Read only");
        access_choice.set_value(choice_index_from_transaction_access(
            initial_advanced.default_transaction_access_mode,
        ));
        access_choice.set_color(theme::input_bg());
        access_choice.set_text_color(theme::text_primary());
        access_flex.end();
        advanced_col.fixed(&access_flex, INPUT_ROW_HEIGHT);

        let mut timezone_flex = Flex::default();
        timezone_flex.set_type(fltk::group::FlexType::Row);
        let mut timezone_label = Frame::default().with_label("Time zone:");
        timezone_label.set_label_color(theme::text_primary());
        timezone_flex.fixed(&timezone_label, FORM_LABEL_WIDTH);
        let mut timezone_input = Input::default();
        timezone_input.set_value(&initial_advanced.session_time_zone);
        timezone_input.set_color(theme::input_bg());
        timezone_input.set_text_color(theme::text_primary());
        timezone_flex.end();
        advanced_col.fixed(&timezone_flex, INPUT_ROW_HEIGHT);

        let mut oracle_protocol_flex = Flex::default();
        oracle_protocol_flex.set_type(fltk::group::FlexType::Row);
        let mut oracle_protocol_label = Frame::default().with_label("Protocol:");
        oracle_protocol_label.set_label_color(theme::text_primary());
        oracle_protocol_flex.fixed(&oracle_protocol_label, FORM_LABEL_WIDTH);
        let mut oracle_protocol_choice = Choice::default();
        oracle_protocol_choice.add_choice("TCP|TCPS");
        oracle_protocol_choice.set_value(choice_index_from_oracle_protocol(
            initial_advanced.oracle_protocol,
        ));
        oracle_protocol_choice.set_color(theme::input_bg());
        oracle_protocol_choice.set_text_color(theme::text_primary());
        oracle_protocol_flex.end();
        advanced_col.fixed(&oracle_protocol_flex, INPUT_ROW_HEIGHT);

        let mut oracle_nls_date_flex = Flex::default();
        oracle_nls_date_flex.set_type(fltk::group::FlexType::Row);
        let mut oracle_nls_date_label = Frame::default().with_label("NLS Date:");
        oracle_nls_date_label.set_label_color(theme::text_primary());
        oracle_nls_date_flex.fixed(&oracle_nls_date_label, FORM_LABEL_WIDTH);
        let mut oracle_nls_date_input = Input::default();
        oracle_nls_date_input.set_value(&initial_advanced.oracle_nls_date_format);
        oracle_nls_date_input.set_color(theme::input_bg());
        oracle_nls_date_input.set_text_color(theme::text_primary());
        oracle_nls_date_flex.end();
        advanced_col.fixed(&oracle_nls_date_flex, INPUT_ROW_HEIGHT);

        let mut oracle_nls_timestamp_flex = Flex::default();
        oracle_nls_timestamp_flex.set_type(fltk::group::FlexType::Row);
        let mut oracle_nls_timestamp_label = Frame::default().with_label("NLS TS:");
        oracle_nls_timestamp_label.set_label_color(theme::text_primary());
        oracle_nls_timestamp_flex.fixed(&oracle_nls_timestamp_label, FORM_LABEL_WIDTH);
        let mut oracle_nls_timestamp_input = Input::default();
        oracle_nls_timestamp_input.set_value(&initial_advanced.oracle_nls_timestamp_format);
        oracle_nls_timestamp_input.set_color(theme::input_bg());
        oracle_nls_timestamp_input.set_text_color(theme::text_primary());
        oracle_nls_timestamp_flex.end();
        advanced_col.fixed(&oracle_nls_timestamp_flex, INPUT_ROW_HEIGHT);

        let mut mysql_sql_mode_flex = Flex::default();
        mysql_sql_mode_flex.set_type(fltk::group::FlexType::Row);
        let mut mysql_sql_mode_label = Frame::default().with_label("SQL mode:");
        mysql_sql_mode_label.set_label_color(theme::text_primary());
        mysql_sql_mode_flex.fixed(&mysql_sql_mode_label, FORM_LABEL_WIDTH);
        let mut mysql_sql_mode_input = Input::default();
        mysql_sql_mode_input.set_value(&initial_advanced.mysql_sql_mode);
        mysql_sql_mode_input.set_color(theme::input_bg());
        mysql_sql_mode_input.set_text_color(theme::text_primary());
        mysql_sql_mode_flex.end();
        advanced_col.fixed(&mysql_sql_mode_flex, INPUT_ROW_HEIGHT);

        let mut mysql_charset_flex = Flex::default();
        mysql_charset_flex.set_type(fltk::group::FlexType::Row);
        let mut mysql_charset_label = Frame::default().with_label("Charset:");
        mysql_charset_label.set_label_color(theme::text_primary());
        mysql_charset_flex.fixed(&mysql_charset_label, FORM_LABEL_WIDTH);
        let mut mysql_charset_input = Input::default();
        mysql_charset_input.set_value(&initial_advanced.mysql_charset);
        mysql_charset_input.set_color(theme::input_bg());
        mysql_charset_input.set_text_color(theme::text_primary());
        mysql_charset_flex.end();
        advanced_col.fixed(&mysql_charset_flex, INPUT_ROW_HEIGHT);

        let mut mysql_collation_flex = Flex::default();
        mysql_collation_flex.set_type(fltk::group::FlexType::Row);
        let mut mysql_collation_label = Frame::default().with_label("Collation:");
        mysql_collation_label.set_label_color(theme::text_primary());
        mysql_collation_flex.fixed(&mysql_collation_label, FORM_LABEL_WIDTH);
        let mut mysql_collation_input = Input::default();
        mysql_collation_input.set_value(&initial_advanced.mysql_collation);
        mysql_collation_input.set_color(theme::input_bg());
        mysql_collation_input.set_text_color(theme::text_primary());
        mysql_collation_flex.end();
        advanced_col.fixed(&mysql_collation_flex, INPUT_ROW_HEIGHT);

        let mut mysql_ssl_ca_flex = Flex::default();
        mysql_ssl_ca_flex.set_type(fltk::group::FlexType::Row);
        let mut mysql_ssl_ca_label = Frame::default().with_label("SSL CA:");
        mysql_ssl_ca_label.set_label_color(theme::text_primary());
        mysql_ssl_ca_flex.fixed(&mysql_ssl_ca_label, FORM_LABEL_WIDTH);
        let mut mysql_ssl_ca_input = Input::default();
        mysql_ssl_ca_input.set_value(&initial_advanced.mysql_ssl_ca_path);
        mysql_ssl_ca_input.set_color(theme::input_bg());
        mysql_ssl_ca_input.set_text_color(theme::text_primary());
        mysql_ssl_ca_flex.end();
        advanced_col.fixed(&mysql_ssl_ca_flex, INPUT_ROW_HEIGHT);

        // Flexible spacer to push buttons to bottom
        let spacer_frame = Frame::default();
        advanced_col.resizable(&spacer_frame);

        // Buttons row
        let mut button_flex = Flex::default();
        button_flex.set_type(fltk::group::FlexType::Row);
        button_flex.set_spacing(DIALOG_SPACING);

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
        advanced_col.fixed(&button_flex, BUTTON_ROW_HEIGHT);

        advanced_col.end();
        details_row.fixed(&advanced_col, ADVANCED_SETTINGS_COLUMN_WIDTH);
        details_row.end();

        let oracle_mode_memory = Arc::new(Mutex::new(OracleModeFieldMemory {
            direct_host: host_input.value(),
            direct_port: port_input.value(),
            direct_service: service_input.value(),
            tns_alias: String::new(),
        }));
        let current_oracle_mode = Arc::new(Mutex::new(OracleConnectMode::Direct));
        let current_db_type = Arc::new(Mutex::new(DatabaseType::Oracle));

        root.end();
        dialog.end();
        fltk::group::Group::set_current(current_group.as_ref());

        // DB Type change callback: update port and service_name label/defaults
        {
            let oracle_mode_memory_dt = Arc::clone(&oracle_mode_memory);
            let current_oracle_mode_dt = Arc::clone(&current_oracle_mode);
            let current_db_type_dt = Arc::clone(&current_db_type);
            let mut right_col_dt = right_col.clone();
            let mut db_col_dt = db_col.clone();
            let mut oracle_mode_choice_dt = oracle_mode_choice.clone();
            let mut oracle_mode_flex_dt = oracle_mode_flex.clone();
            let mut host_flex_dt = host_flex.clone();
            let mut port_flex_dt = port_flex.clone();
            let mut port_input_dt = port_input.clone();
            let mut host_input_dt = host_input.clone();
            let mut service_input_dt = service_input.clone();
            let mut svc_label_dt = svc_label.clone();
            let mut advanced_col_dt = advanced_col.clone();
            let mut ssl_choice_dt = ssl_choice.clone();
            let mut isolation_choice_dt = isolation_choice.clone();
            let mut access_choice_dt = access_choice.clone();
            let mut timezone_input_dt = timezone_input.clone();
            let mut mysql_sql_mode_input_dt = mysql_sql_mode_input.clone();
            let mut mysql_charset_input_dt = mysql_charset_input.clone();
            let mut mysql_collation_input_dt = mysql_collation_input.clone();
            let mut mysql_ssl_ca_input_dt = mysql_ssl_ca_input.clone();
            let mut oracle_protocol_choice_dt = oracle_protocol_choice.clone();
            let mut oracle_nls_date_input_dt = oracle_nls_date_input.clone();
            let mut oracle_nls_timestamp_input_dt = oracle_nls_timestamp_input.clone();
            let mut oracle_protocol_flex_dt = oracle_protocol_flex.clone();
            let mut oracle_nls_date_flex_dt = oracle_nls_date_flex.clone();
            let mut oracle_nls_timestamp_flex_dt = oracle_nls_timestamp_flex.clone();
            let mut mysql_sql_mode_flex_dt = mysql_sql_mode_flex.clone();
            let mut mysql_charset_flex_dt = mysql_charset_flex.clone();
            let mut mysql_collation_flex_dt = mysql_collation_flex.clone();
            let mut mysql_ssl_ca_flex_dt = mysql_ssl_ca_flex.clone();
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
                    &mut db_col_dt,
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
                let previous_advanced = advanced_settings_from_form(
                    previous_db_type,
                    &ssl_choice_dt,
                    &isolation_choice_dt,
                    &access_choice_dt,
                    &timezone_input_dt,
                    &mysql_sql_mode_input_dt,
                    &mysql_charset_input_dt,
                    &mysql_collation_input_dt,
                    &mysql_ssl_ca_input_dt,
                    &oracle_protocol_choice_dt,
                    &oracle_nls_date_input_dt,
                    &oracle_nls_timestamp_input_dt,
                );
                let advanced = previous_advanced.migrate_for_db_type(previous_db_type, db_type);
                set_advanced_form_values(
                    &advanced,
                    db_type,
                    &mut ssl_choice_dt,
                    &mut isolation_choice_dt,
                    &mut access_choice_dt,
                    &mut timezone_input_dt,
                    &mut mysql_sql_mode_input_dt,
                    &mut mysql_charset_input_dt,
                    &mut mysql_collation_input_dt,
                    &mut mysql_ssl_ca_input_dt,
                    &mut oracle_protocol_choice_dt,
                    &mut oracle_nls_date_input_dt,
                    &mut oracle_nls_timestamp_input_dt,
                );
                apply_advanced_form_mode(
                    &mut advanced_col_dt,
                    db_type,
                    db_type.supports_tns_alias()
                        && oracle_connect_mode_from_choice_index(oracle_mode_choice_dt.value())
                            == OracleConnectMode::TnsAlias,
                    &mut oracle_protocol_flex_dt,
                    &mut oracle_nls_date_flex_dt,
                    &mut oracle_nls_timestamp_flex_dt,
                    &mut mysql_sql_mode_flex_dt,
                    &mut mysql_charset_flex_dt,
                    &mut mysql_collation_flex_dt,
                    &mut mysql_ssl_ca_flex_dt,
                    &mut oracle_protocol_choice_dt,
                    &mut ssl_choice_dt,
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
            let mut db_col_cb = db_col.clone();
            let mut oracle_mode_choice_cb = oracle_mode_choice.clone();
            let mut oracle_mode_flex_cb = oracle_mode_flex.clone();
            let mut host_flex_cb = host_flex.clone();
            let mut port_flex_cb = port_flex.clone();
            let dbtype_choice_cb = dbtype_choice.clone();
            let mut host_input_cb = host_input.clone();
            let mut port_input_cb = port_input.clone();
            let mut service_input_cb = service_input.clone();
            let mut svc_label_cb = svc_label.clone();
            let mut advanced_col_cb = advanced_col.clone();
            let mut oracle_protocol_flex_cb = oracle_protocol_flex.clone();
            let mut oracle_nls_date_flex_cb = oracle_nls_date_flex.clone();
            let mut oracle_nls_timestamp_flex_cb = oracle_nls_timestamp_flex.clone();
            let mut mysql_sql_mode_flex_cb = mysql_sql_mode_flex.clone();
            let mut mysql_charset_flex_cb = mysql_charset_flex.clone();
            let mut mysql_collation_flex_cb = mysql_collation_flex.clone();
            let mut mysql_ssl_ca_flex_cb = mysql_ssl_ca_flex.clone();
            let mut oracle_protocol_choice_cb = oracle_protocol_choice.clone();
            let mut ssl_choice_cb = ssl_choice.clone();
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
                    &mut db_col_cb,
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
                let db_type = db_type_from_choice_index(dbtype_choice_cb.value());
                apply_advanced_form_mode(
                    &mut advanced_col_cb,
                    db_type,
                    db_type.supports_tns_alias() && next_mode == OracleConnectMode::TnsAlias,
                    &mut oracle_protocol_flex_cb,
                    &mut oracle_nls_date_flex_cb,
                    &mut oracle_nls_timestamp_flex_cb,
                    &mut mysql_sql_mode_flex_cb,
                    &mut mysql_charset_flex_cb,
                    &mut mysql_collation_flex_cb,
                    &mut mysql_ssl_ca_flex_cb,
                    &mut oracle_protocol_choice_cb,
                    &mut ssl_choice_cb,
                );
                *current_oracle_mode_cb
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = next_mode;
            });
        }

        apply_connection_form_mode(
            &mut right_col,
            &mut db_col,
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
        apply_advanced_form_mode(
            &mut advanced_col,
            DatabaseType::Oracle,
            false,
            &mut oracle_protocol_flex,
            &mut oracle_nls_date_flex,
            &mut oracle_nls_timestamp_flex,
            &mut mysql_sql_mode_flex,
            &mut mysql_charset_flex,
            &mut mysql_collation_flex,
            &mut mysql_ssl_ca_flex,
            &mut oracle_protocol_choice,
            &mut ssl_choice,
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
        let mut db_col_saved = db_col.clone();
        let mut oracle_mode_flex_saved = oracle_mode_flex.clone();
        let mut host_flex_saved = host_flex.clone();
        let mut port_flex_saved = port_flex.clone();
        let mut svc_label_cb = svc_label.clone();
        let mut advanced_col_saved = advanced_col.clone();
        let mut ssl_choice_saved = ssl_choice.clone();
        let mut isolation_choice_saved = isolation_choice.clone();
        let mut access_choice_saved = access_choice.clone();
        let mut timezone_input_saved = timezone_input.clone();
        let mut mysql_sql_mode_input_saved = mysql_sql_mode_input.clone();
        let mut mysql_charset_input_saved = mysql_charset_input.clone();
        let mut mysql_collation_input_saved = mysql_collation_input.clone();
        let mut mysql_ssl_ca_input_saved = mysql_ssl_ca_input.clone();
        let mut oracle_protocol_choice_saved = oracle_protocol_choice.clone();
        let mut oracle_nls_date_input_saved = oracle_nls_date_input.clone();
        let mut oracle_nls_timestamp_input_saved = oracle_nls_timestamp_input.clone();
        let mut oracle_protocol_flex_saved = oracle_protocol_flex.clone();
        let mut oracle_nls_date_flex_saved = oracle_nls_date_flex.clone();
        let mut oracle_nls_timestamp_flex_saved = oracle_nls_timestamp_flex.clone();
        let mut mysql_sql_mode_flex_saved = mysql_sql_mode_flex.clone();
        let mut mysql_charset_flex_saved = mysql_charset_flex.clone();
        let mut mysql_collation_flex_saved = mysql_collation_flex.clone();
        let mut mysql_ssl_ca_flex_saved = mysql_ssl_ca_flex.clone();
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
                    // Only sync Oracle-specific state when the saved connection is
                    // an Oracle connection. Loading a MySQL connection must not
                    // overwrite the Oracle Direct-mode memory (host/port/service) or
                    // reset the oracle mode choice, as doing so would show wrong
                    // values if the user later switches DB type back to Oracle.
                    let oracle_mode = if conn.db_type.supports_tns_alias() {
                        sync_oracle_mode_memory_from_info(&oracle_mode_memory_saved, conn);
                        let mode = oracle_connect_mode_for_info(conn);
                        *current_oracle_mode_saved
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) = mode;
                        oracle_mode_choice_cb
                            .set_value(choice_index_from_oracle_connect_mode(mode));
                        mode
                    } else {
                        *current_oracle_mode_saved
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                    };
                    *current_db_type_saved
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = conn.db_type;
                    dbtype_choice_cb.set_value(choice_index_from_db_type(conn.db_type));
                    if !conn.db_type.supports_tns_alias() {
                        service_input_cb.set_value(&conn.service_name);
                        host_input_cb.set_value(&conn.host);
                        port_input_cb.set_value(&conn.port.to_string());
                    }
                    apply_connection_form_mode(
                        &mut right_col_saved,
                        &mut db_col_saved,
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
                    set_advanced_form_values(
                        &conn.advanced,
                        conn.db_type,
                        &mut ssl_choice_saved,
                        &mut isolation_choice_saved,
                        &mut access_choice_saved,
                        &mut timezone_input_saved,
                        &mut mysql_sql_mode_input_saved,
                        &mut mysql_charset_input_saved,
                        &mut mysql_collation_input_saved,
                        &mut mysql_ssl_ca_input_saved,
                        &mut oracle_protocol_choice_saved,
                        &mut oracle_nls_date_input_saved,
                        &mut oracle_nls_timestamp_input_saved,
                    );
                    apply_advanced_form_mode(
                        &mut advanced_col_saved,
                        conn.db_type,
                        conn.db_type.supports_tns_alias()
                            && oracle_mode == OracleConnectMode::TnsAlias,
                        &mut oracle_protocol_flex_saved,
                        &mut oracle_nls_date_flex_saved,
                        &mut oracle_nls_timestamp_flex_saved,
                        &mut mysql_sql_mode_flex_saved,
                        &mut mysql_charset_flex_saved,
                        &mut mysql_collation_flex_saved,
                        &mut mysql_ssl_ca_flex_saved,
                        &mut oracle_protocol_choice_saved,
                        &mut ssl_choice_saved,
                    );

                    // Double click to connect immediately
                    if app::event_clicks() && !keyring_load_failed {
                        if password.is_empty() {
                            fltk::dialog::alert_default(
                                "No password is saved for this connection. Enter a password before connecting.",
                            );
                            return;
                        }
                        let mut info = conn.clone();
                        info.password = password;
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
        let ssl_choice_save = ssl_choice.clone();
        let isolation_choice_save = isolation_choice.clone();
        let access_choice_save = access_choice.clone();
        let timezone_input_save = timezone_input.clone();
        let mysql_sql_mode_input_save = mysql_sql_mode_input.clone();
        let mysql_charset_input_save = mysql_charset_input.clone();
        let mysql_collation_input_save = mysql_collation_input.clone();
        let mysql_ssl_ca_input_save = mysql_ssl_ca_input.clone();
        let oracle_protocol_choice_save = oracle_protocol_choice.clone();
        let oracle_nls_date_input_save = oracle_nls_date_input.clone();
        let oracle_nls_timestamp_input_save = oracle_nls_timestamp_input.clone();

        save_btn.set_callback(move |_| {
            let db_type = db_type_from_choice_index(dbtype_choice_save.value());
            let advanced = advanced_settings_from_form(
                db_type,
                &ssl_choice_save,
                &isolation_choice_save,
                &access_choice_save,
                &timezone_input_save,
                &mysql_sql_mode_input_save,
                &mysql_charset_input_save,
                &mysql_collation_input_save,
                &mysql_ssl_ca_input_save,
                &oracle_protocol_choice_save,
                &oracle_nls_date_input_save,
                &oracle_nls_timestamp_input_save,
            );
            let info = match build_connection_info(
                &name_input_save.value(),
                &user_input_save.value(),
                &pass_input_save.value(),
                &host_input_save.value(),
                &port_input_save.value(),
                &service_input_save.value(),
                db_type,
                oracle_connect_mode_from_choice_index(oracle_mode_choice_save.value()),
                advanced,
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
        let ssl_choice_test = ssl_choice.clone();
        let isolation_choice_test = isolation_choice.clone();
        let access_choice_test = access_choice.clone();
        let timezone_input_test = timezone_input.clone();
        let mysql_sql_mode_input_test = mysql_sql_mode_input.clone();
        let mysql_charset_input_test = mysql_charset_input.clone();
        let mysql_collation_input_test = mysql_collation_input.clone();
        let mysql_ssl_ca_input_test = mysql_ssl_ca_input.clone();
        let oracle_protocol_choice_test = oracle_protocol_choice.clone();
        let oracle_nls_date_input_test = oracle_nls_date_input.clone();
        let oracle_nls_timestamp_input_test = oracle_nls_timestamp_input.clone();

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

            let db_type = db_type_from_choice_index(dbtype_choice_test.value());
            let advanced = advanced_settings_from_form(
                db_type,
                &ssl_choice_test,
                &isolation_choice_test,
                &access_choice_test,
                &timezone_input_test,
                &mysql_sql_mode_input_test,
                &mysql_charset_input_test,
                &mysql_collation_input_test,
                &mysql_ssl_ca_input_test,
                &oracle_protocol_choice_test,
                &oracle_nls_date_input_test,
                &oracle_nls_timestamp_input_test,
            );
            let info = match build_connection_info(
                &name_input_test.value(),
                &user_input_test.value(),
                &pass_input_test.value(),
                &host_input_test.value(),
                &port_input_test.value(),
                &service_input_test.value(),
                db_type,
                oracle_connect_mode_from_choice_index(oracle_mode_choice_test.value()),
                advanced,
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
        let ssl_choice_conn = ssl_choice.clone();
        let isolation_choice_conn = isolation_choice.clone();
        let access_choice_conn = access_choice.clone();
        let timezone_input_conn = timezone_input.clone();
        let mysql_sql_mode_input_conn = mysql_sql_mode_input.clone();
        let mysql_charset_input_conn = mysql_charset_input.clone();
        let mysql_collation_input_conn = mysql_collation_input.clone();
        let mysql_ssl_ca_input_conn = mysql_ssl_ca_input.clone();
        let oracle_protocol_choice_conn = oracle_protocol_choice.clone();
        let oracle_nls_date_input_conn = oracle_nls_date_input.clone();
        let oracle_nls_timestamp_input_conn = oracle_nls_timestamp_input.clone();

        connect_btn.set_callback(move |_| {
            let db_type = db_type_from_choice_index(dbtype_choice_conn.value());
            let advanced = advanced_settings_from_form(
                db_type,
                &ssl_choice_conn,
                &isolation_choice_conn,
                &access_choice_conn,
                &timezone_input_conn,
                &mysql_sql_mode_input_conn,
                &mysql_charset_input_conn,
                &mysql_collation_input_conn,
                &mysql_ssl_ca_input_conn,
                &oracle_protocol_choice_conn,
                &oracle_nls_date_input_conn,
                &oracle_nls_timestamp_input_conn,
            );
            let info = match build_connection_info(
                &name_input_conn.value(),
                &user_input_conn.value(),
                &pass_input_conn.value(),
                &host_input_conn.value(),
                &port_input_conn.value(),
                &service_input_conn.value(),
                db_type,
                oracle_connect_mode_from_choice_index(oracle_mode_choice_conn.value()),
                advanced,
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
    use super::{build_connection_info, connection_dialog_width, OracleConnectMode};
    use crate::db::{
        ConnectionAdvancedSettings, ConnectionSslMode, DatabaseType, TransactionIsolation,
    };

    fn default_advanced(db_type: DatabaseType) -> ConnectionAdvancedSettings {
        ConnectionAdvancedSettings::default_for(db_type)
    }

    #[test]
    fn connection_dialog_width_covers_fixed_columns_and_spacing() {
        let required_width = crate::ui::constants::DIALOG_MARGIN * 2
            + super::SAVED_CONNECTIONS_COLUMN_WIDTH
            + super::DB_SELECTION_COLUMN_WIDTH
            + super::CONNECTION_INFO_COLUMN_WIDTH
            + super::ADVANCED_SETTINGS_COLUMN_WIDTH
            + super::CONNECTION_DIALOG_COLUMN_SPACING * 3;

        assert_eq!(connection_dialog_width(), required_width);
    }

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
            default_advanced(DatabaseType::Oracle),
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
            default_advanced(DatabaseType::Oracle),
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
            default_advanced(DatabaseType::Oracle),
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
            default_advanced(DatabaseType::Oracle),
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
            default_advanced(DatabaseType::Oracle),
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
            default_advanced(DatabaseType::Oracle),
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
            default_advanced(DatabaseType::Oracle),
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
            default_advanced(DatabaseType::Oracle),
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
            default_advanced(DatabaseType::Oracle),
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
            default_advanced(DatabaseType::MySQL),
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
    fn build_connection_info_preserves_advanced_settings() {
        let mut advanced = default_advanced(DatabaseType::MySQL);
        advanced.ssl_mode = ConnectionSslMode::VerifyCa;
        advanced.default_transaction_isolation = TransactionIsolation::RepeatableRead;
        advanced.session_time_zone = "+09:00".to_string();
        advanced.mysql_ssl_ca_path = "/tmp/mysql-ca.pem".to_string();

        let info = build_connection_info(
            " local ",
            " root ",
            "secret",
            " localhost ",
            " 3306 ",
            " query_tool_test ",
            DatabaseType::MySQL,
            OracleConnectMode::Direct,
            advanced.clone(),
        )
        .expect("should build valid MySQL connection info");

        assert_eq!(info.advanced, advanced);
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
            default_advanced(DatabaseType::Oracle),
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

    #[test]
    fn build_connection_info_neutralises_ssl_and_protocol_for_tns_alias_mode() {
        // User configured TCPS/Required in Direct mode, then toggled to TNS
        // alias. The form retains the previous selections, but the info we
        // hand back must sanitise them so the connection uses the alias'
        // own network settings.
        let mut advanced = default_advanced(DatabaseType::Oracle);
        advanced.ssl_mode = ConnectionSslMode::Required;
        advanced.oracle_protocol = crate::db::OracleNetworkProtocol::Tcps;

        let info = build_connection_info(
            "local",
            "system",
            "password",
            "ignored-host",
            "1521",
            "FREE_LOCAL",
            DatabaseType::Oracle,
            OracleConnectMode::TnsAlias,
            advanced,
        )
        .expect("TNS alias should silently drop direct-mode SSL/protocol");

        assert_eq!(info.advanced.ssl_mode, ConnectionSslMode::Disabled);
        assert_eq!(
            info.advanced.oracle_protocol,
            crate::db::OracleNetworkProtocol::Tcp
        );
    }

    #[test]
    fn migrate_for_db_type_preserves_shared_settings_across_db_switch() {
        // Simulates the user customising the Advanced Settings for MySQL and
        // then toggling the DB type to Oracle. Shared fields must stay; the
        // MySQL-specific fields fall back to Oracle defaults.
        let mut mysql_advanced = ConnectionAdvancedSettings::default_for(DatabaseType::MySQL);
        mysql_advanced.default_transaction_isolation = TransactionIsolation::Serializable;
        mysql_advanced.default_transaction_access_mode = crate::db::TransactionAccessMode::ReadOnly;
        mysql_advanced.session_time_zone = "+09:00".to_string();
        mysql_advanced.ssl_mode = ConnectionSslMode::VerifyIdentity;
        mysql_advanced.mysql_charset = "latin1".to_string();

        let migrated =
            mysql_advanced.migrate_for_db_type(DatabaseType::MySQL, DatabaseType::Oracle);

        assert_eq!(
            migrated.default_transaction_isolation,
            TransactionIsolation::Serializable
        );
        assert_eq!(
            migrated.default_transaction_access_mode,
            crate::db::TransactionAccessMode::ReadOnly
        );
        assert_eq!(migrated.session_time_zone, "+09:00");
        // Oracle cannot express VerifyIdentity, so we keep the SSL intent by
        // remapping to Required rather than silently downgrading to Disabled.
        assert_eq!(migrated.ssl_mode, ConnectionSslMode::Required);
        // Oracle-specific fields take the Oracle defaults.
        let oracle_default = ConnectionAdvancedSettings::default_for(DatabaseType::Oracle);
        assert_eq!(
            migrated.oracle_nls_date_format,
            oracle_default.oracle_nls_date_format
        );
        assert_eq!(
            migrated.oracle_nls_timestamp_format,
            oracle_default.oracle_nls_timestamp_format
        );
    }

    #[test]
    fn migrate_for_db_type_uses_target_defaults_for_unchanged_shared_settings() {
        // The dialog starts as Oracle. Switching to MySQL without editing the
        // shared fields should use MySQL's own defaults, including +00:00 for
        // session time zone.
        let oracle_advanced = ConnectionAdvancedSettings::default_for(DatabaseType::Oracle);
        let migrated =
            oracle_advanced.migrate_for_db_type(DatabaseType::Oracle, DatabaseType::MySQL);
        let mysql_default = ConnectionAdvancedSettings::default_for(DatabaseType::MySQL);

        assert_eq!(migrated.session_time_zone, mysql_default.session_time_zone);
        assert_eq!(
            migrated.default_transaction_isolation,
            mysql_default.default_transaction_isolation
        );
        assert_eq!(
            migrated.default_transaction_access_mode,
            mysql_default.default_transaction_access_mode
        );
        assert_eq!(migrated.ssl_mode, mysql_default.ssl_mode);

        let migrated_back =
            mysql_default.migrate_for_db_type(DatabaseType::MySQL, DatabaseType::Oracle);
        assert_eq!(
            migrated_back.session_time_zone,
            ConnectionAdvancedSettings::default_for(DatabaseType::Oracle).session_time_zone
        );
    }

    #[test]
    fn migrate_for_db_type_falls_back_when_isolation_unsupported() {
        // Oracle does not support ReadUncommitted. When migrating to Oracle,
        // the form must not end up with an invalid isolation selection.
        let mut mysql_advanced = ConnectionAdvancedSettings::default_for(DatabaseType::MySQL);
        mysql_advanced.default_transaction_isolation = TransactionIsolation::ReadUncommitted;

        let migrated =
            mysql_advanced.migrate_for_db_type(DatabaseType::MySQL, DatabaseType::Oracle);

        assert!(DatabaseType::Oracle
            .supported_transaction_isolations()
            .contains(&migrated.default_transaction_isolation));
    }

    #[test]
    fn sync_oracle_mode_memory_from_info_updates_only_active_mode_fields() {
        use crate::db::{ConnectionAdvancedSettings, ConnectionInfo, DatabaseType};
        use std::sync::{Arc, Mutex};

        let memory = Arc::new(Mutex::new(super::OracleModeFieldMemory {
            direct_host: "prod-host".to_string(),
            direct_port: "1521".to_string(),
            direct_service: "PROD".to_string(),
            tns_alias: "PROD_ALIAS".to_string(),
        }));

        // Loading a Direct Oracle connection updates only the direct_* fields.
        let direct_conn = ConnectionInfo {
            name: "direct".to_string(),
            host: "dev-host".to_string(),
            port: 1522,
            service_name: "DEV".to_string(),
            username: "dev".to_string(),
            password: String::new(),
            db_type: DatabaseType::Oracle,
            advanced: ConnectionAdvancedSettings::default_for(DatabaseType::Oracle),
        };
        super::sync_oracle_mode_memory_from_info(&memory, &direct_conn);
        {
            let guard = memory.lock().unwrap();
            assert_eq!(guard.direct_host, "dev-host");
            assert_eq!(guard.direct_port, "1522");
            assert_eq!(guard.direct_service, "DEV");
            assert_eq!(guard.tns_alias, "PROD_ALIAS"); // tns_alias must not change
        }

        // Loading a TNS alias Oracle connection updates only tns_alias.
        let tns_conn = ConnectionInfo {
            name: "tns".to_string(),
            host: String::new(), // empty host signals TNS alias
            port: 0,
            service_name: "DEV_ALIAS".to_string(),
            username: "dev".to_string(),
            password: String::new(),
            db_type: DatabaseType::Oracle,
            advanced: ConnectionAdvancedSettings::default_for(DatabaseType::Oracle),
        };
        super::sync_oracle_mode_memory_from_info(&memory, &tns_conn);
        {
            let guard = memory.lock().unwrap();
            assert_eq!(guard.tns_alias, "DEV_ALIAS");
            assert_eq!(guard.direct_host, "dev-host"); // direct_* must not change
            assert_eq!(guard.direct_port, "1522");
            assert_eq!(guard.direct_service, "DEV");
        }
    }
}
