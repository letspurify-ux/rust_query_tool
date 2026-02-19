use fltk::{
    app,
    browser::HoldBrowser,
    button::Button,
    enums::FrameType,
    frame::Frame,
    group::Flex,
    input::{Input, SecretInput},
    prelude::*,
    window::Window,
};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::mpsc;
use std::thread;

use crate::db::{ConnectionInfo, DatabaseConnection};
use crate::ui::center_on_main;
use crate::ui::constants::*;
use crate::ui::theme;
use crate::utils::AppConfig;

pub struct ConnectionDialog;

fn build_connection_info(
    name: &str,
    username: &str,
    password: &str,
    host: &str,
    port_text: &str,
    service_name: &str,
) -> Result<ConnectionInfo, String> {
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
    if host.is_empty() {
        return Err("Host is required".to_string());
    }
    if service_name.is_empty() {
        return Err("Service name is required".to_string());
    }

    let port = port_text
        .parse::<u16>()
        .map_err(|_| "Port must be a valid number between 0 and 65535".to_string())?;

    if port == 0 {
        return Err("Port must be between 1 and 65535".to_string());
    }

    Ok(ConnectionInfo::new(
        name,
        username,
        password,
        host,
        port,
        service_name,
    ))
}

impl ConnectionDialog {
    pub fn show_with_registry(popups: Rc<RefCell<Vec<Window>>>) -> Option<ConnectionInfo> {
        enum DialogMessage {
            DeleteSelected,
            Test(ConnectionInfo),
            TestResult(Result<(), String>),
            Save(ConnectionInfo),
            Connect(ConnectionInfo, bool),
            Cancel,
        }

        let (sender, receiver) = mpsc::channel::<DialogMessage>();

        let result: Rc<RefCell<Option<ConnectionInfo>>> = Rc::new(RefCell::new(None));
        let config = Rc::new(RefCell::new(AppConfig::load()));

        let current_group = fltk::group::Group::try_current();
        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let dialog_w = 620;
        let dialog_h = 400;
        let mut dialog = Window::default()
            .with_size(dialog_w, dialog_h)
            .with_label("Connect to Oracle Database");
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
            let cfg = config.borrow();
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

        // Separator: Server section header
        let mut server_header = Frame::default().with_label("Server");
        server_header.set_label_color(theme::text_secondary());
        right_col.fixed(&server_header, LABEL_ROW_HEIGHT);

        // Host
        let mut host_flex = Flex::default();
        host_flex.set_type(fltk::group::FlexType::Row);
        let mut host_label = Frame::default().with_label("Host:");
        host_label.set_label_color(theme::text_primary());
        host_flex.fixed(&host_label, FORM_LABEL_WIDTH);
        let mut host_input = Input::default();
        host_input.set_value("localhost");
        host_input.set_color(theme::input_bg());
        host_input.set_text_color(theme::text_primary());
        host_flex.end();
        right_col.fixed(&host_flex, INPUT_ROW_HEIGHT);

        // Port + Service on same row
        let mut port_svc_flex = Flex::default();
        port_svc_flex.set_type(fltk::group::FlexType::Row);
        port_svc_flex.set_spacing(DIALOG_SPACING);
        let mut port_label = Frame::default().with_label("Port:");
        port_label.set_label_color(theme::text_primary());
        port_svc_flex.fixed(&port_label, 40);
        let mut port_input = Input::default();
        port_input.set_value("1521");
        port_input.set_color(theme::input_bg());
        port_input.set_text_color(theme::text_primary());
        port_svc_flex.fixed(&port_input, 60);
        let mut svc_label = Frame::default().with_label("Service:");
        svc_label.set_label_color(theme::text_primary());
        port_svc_flex.fixed(&svc_label, 60);
        let mut service_input = Input::default();
        service_input.set_value("ORCL");
        service_input.set_color(theme::input_bg());
        service_input.set_text_color(theme::text_primary());
        port_svc_flex.end();
        right_col.fixed(&port_svc_flex, INPUT_ROW_HEIGHT);

        // Save connection button
        let mut save_flex = Flex::default();
        save_flex.set_type(fltk::group::FlexType::Row);
        let _spacer = Frame::default();
        save_flex.fixed(&_spacer, FORM_LABEL_WIDTH);
        let mut save_btn = Button::default().with_label("Save this connection");
        save_btn.set_color(theme::button_secondary());
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
        cancel_btn.set_color(theme::button_subtle());
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

        popups.borrow_mut().push(dialog.clone());

        // Saved connection selection callback
        let config_cb = config.clone();
        let mut name_input_cb = name_input.clone();
        let mut user_input_cb = user_input.clone();
        let mut pass_input_cb = pass_input.clone();
        let mut host_input_cb = host_input.clone();
        let mut port_input_cb = port_input.clone();
        let mut service_input_cb = service_input.clone();
        let sender_for_click = sender.clone();

        saved_browser.set_callback(move |browser| {
            if let Some(selected) = browser.selected_text() {
                let cfg = config_cb.borrow();
                if let Some(conn) = cfg.get_connection_by_name(&selected) {
                    name_input_cb.set_value(&conn.name);
                    user_input_cb.set_value(&conn.username);
                    // Load password from OS keyring on demand.
                    let password =
                        AppConfig::get_password_for_connection(&conn.name).unwrap_or_default();
                    pass_input_cb.set_value(&password);
                    host_input_cb.set_value(&conn.host);
                    port_input_cb.set_value(&conn.port.to_string());
                    service_input_cb.set_value(&conn.service_name);

                    // Double click to connect immediately
                    if app::event_clicks() {
                        let info = ConnectionInfo::new(
                            &conn.name,
                            &conn.username,
                            &password,
                            &conn.host,
                            conn.port,
                            &conn.service_name,
                        );
                        let _ = sender_for_click.send(DialogMessage::Connect(info, true));
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

        save_btn.set_callback(move |_| {
            let info = match build_connection_info(
                &name_input_save.value(),
                &user_input_save.value(),
                &pass_input_save.value(),
                &host_input_save.value(),
                &port_input_save.value(),
                &service_input_save.value(),
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
        let name_input_test = name_input.clone();
        let user_input_test = user_input.clone();
        let pass_input_test = pass_input.clone();
        let host_input_test = host_input.clone();
        let port_input_test = port_input.clone();
        let service_input_test = service_input.clone();

        test_btn.set_callback(move |_| {
            let info = match build_connection_info(
                &name_input_test.value(),
                &user_input_test.value(),
                &pass_input_test.value(),
                &host_input_test.value(),
                &port_input_test.value(),
                &service_input_test.value(),
            ) {
                Ok(info) => info,
                Err(message) => {
                    fltk::dialog::alert_default(&message);
                    return;
                }
            };

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

        connect_btn.set_callback(move |_| {
            let info = match build_connection_info(
                &name_input_conn.value(),
                &user_input_conn.value(),
                &pass_input_conn.value(),
                &host_input_conn.value(),
                &port_input_conn.value(),
                &service_input_conn.value(),
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
                                let mut cfg = config.borrow_mut();
                                cfg.remove_connection(&selected);
                                if let Err(e) = cfg.save() {
                                    fltk::dialog::alert_default(&format!(
                                        "Failed to save config: {}",
                                        e
                                    ));
                                }
                                saved_browser.clear();
                                for conn in cfg.get_all_connections() {
                                    saved_browser.add(&conn.name);
                                }
                            }
                        } else {
                            fltk::dialog::alert_default("Please select a connection to delete");
                        }
                    }
                    DialogMessage::Test(info) => {
                        let sender = sender.clone();
                        thread::spawn(move || {
                            let result = DatabaseConnection::test_connection(&info)
                                .map_err(|e| e.to_string());
                            let _ = sender.send(DialogMessage::TestResult(result));
                            app::awake();
                        });
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
                        let mut cfg = config.borrow_mut();
                        cfg.add_recent_connection(info.clone());
                        if let Err(e) = cfg.save() {
                            fltk::dialog::alert_default(&format!(
                                "Failed to save connection: {}",
                                e
                            ));
                        } else {
                            saved_browser.clear();
                            for conn in cfg.get_all_connections() {
                                saved_browser.add(&conn.name);
                            }
                        }
                    }
                    DialogMessage::Connect(info, save_connection) => {
                        if save_connection {
                            let mut cfg = config.borrow_mut();
                            cfg.add_recent_connection(info.clone());
                            if let Err(e) = cfg.save() {
                                fltk::dialog::alert_default(&format!(
                                    "Failed to save connection: {}",
                                    e
                                ));
                            }
                        }

                        *result.borrow_mut() = Some(info);
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
            .borrow_mut()
            .retain(|w| w.as_widget_ptr() != dialog.as_widget_ptr());

        // Explicitly destroy top-level dialog widgets to release native resources.
        Window::delete(dialog);

        // Clear password from the returned ConnectionInfo clone held in config
        // (it was already saved to keyring if needed)
        let final_result = result.borrow().clone();
        final_result
    }
}

#[cfg(test)]
mod tests {
    use super::build_connection_info;

    #[test]
    fn build_connection_info_rejects_empty_required_fields() {
        let result = build_connection_info(" ", "scott", "tiger", "localhost", "1521", "ORCL");
        assert!(result.is_err());

        let result = build_connection_info("local", "", "tiger", "localhost", "1521", "ORCL");
        assert!(result.is_err());

        let result = build_connection_info("local", "scott", "tiger", "", "1521", "ORCL");
        assert!(result.is_err());

        let result = build_connection_info("local", "scott", "tiger", "localhost", "1521", "");
        assert!(result.is_err());
    }

    #[test]
    fn build_connection_info_rejects_invalid_port() {
        let result = build_connection_info("local", "scott", "tiger", "localhost", "abc", "ORCL");
        assert!(result.is_err());

        let result = build_connection_info("local", "scott", "tiger", "localhost", "0", "ORCL");
        assert!(result.is_err());
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
        )
        .expect("should build valid connection info");

        assert_eq!(info.name, "local");
        assert_eq!(info.username, "scott");
        assert_eq!(info.host, "localhost");
        assert_eq!(info.port, 1521);
        assert_eq!(info.service_name, "ORCL");
    }
}
