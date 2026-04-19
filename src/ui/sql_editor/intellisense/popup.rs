impl SqlEditorWidget {
    pub fn show_quick_describe_text_dialog(title: &str, content: &str) {
        use fltk::{prelude::*, text::TextDisplay, window::Window};

        let current_group = fltk::group::Group::try_current();

        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let mut dialog = Window::default().with_size(760, 500).with_label(title);
        crate::ui::center_on_main(&mut dialog);
        dialog.set_color(theme::panel_raised());
        dialog.make_modal(true);
        dialog.begin();

        let mut display = TextDisplay::default().with_pos(10, 10).with_size(740, 440);
        display.set_color(theme::editor_bg());
        display.set_text_color(theme::text_primary());
        display.set_text_font(crate::ui::configured_editor_profile().normal);
        display.set_text_size(crate::ui::configured_ui_font_size());

        let mut buffer = fltk::text::TextBuffer::default();
        buffer.set_text(content);
        display.set_buffer(buffer);

        let close_btn_x = crate::utils::arithmetic::safe_div(760 - BUTTON_WIDTH, 2);
        let mut close_btn = fltk::button::Button::default()
            .with_pos(close_btn_x, 460)
            .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
            .with_label("Close");
        close_btn.set_color(theme::button_secondary());
        close_btn.set_label_color(theme::text_primary());

        let (sender, receiver) = mpsc::channel::<()>();
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

        // Explicitly destroy top-level dialog widgets to release native resources.
        Window::delete(dialog);
    }
    pub fn hide_intellisense_if_outside(&self, x: i32, y: i32) {
        if matches!(
            self.intellisense_runtime.popup_transition_state(),
            IntellisensePopupTransitionState::Showing
        ) {
            Self::schedule_deferred_outside_click_popup_hide(
                self.intellisense_popup.clone(),
                self.intellisense_runtime.clone(),
                x,
                y,
                INTELLISENSE_DEFERRED_HIDE_RETRIES,
            );
            return;
        }
        let mut popup = self
            .intellisense_popup
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let popup_visible = popup.is_visible();
        if !popup_visible {
            return;
        }
        let click_inside_popup = popup_visible && popup.contains_point(x, y);
        if Self::should_ignore_external_hide_click(popup_visible, click_inside_popup) {
            return;
        }
        popup.hide();
        drop(popup);
        Self::clear_intellisense_state_for_external_hide(&self.intellisense_runtime);
    }

    #[allow(dead_code)]
    pub fn update_intellisense_data(&mut self, data: IntellisenseData) {
        let mut data = data;
        data.rebuild_indices();
        *self
            .intellisense_data
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = data;
    }

    pub fn get_intellisense_data(&self) -> Arc<Mutex<IntellisenseData>> {
        self.intellisense_data.clone()
    }
    pub fn show_intellisense(&self) {
        Self::trigger_intellisense(
            &self.editor,
            &self.buffer,
            &self.highlight_shadow,
            &self.intellisense_data,
            &self.intellisense_popup,
            &self.column_sender,
            &self.connection,
            &self.intellisense_runtime,
        );
    }

    pub fn quick_describe_at_cursor(&self) {
        let (cursor_pos, _) = Self::editor_cursor_position(&self.editor, &self.buffer);
        let Some((word, start, _)) =
            Self::identifier_at_position(&self.buffer, &self.highlight_shadow, cursor_pos)
        else {
            return;
        };
        let qualifier =
            Self::qualifier_before_word(&self.buffer, &self.highlight_shadow, start as usize);
        let object_name = if let Some(ref qualifier) = qualifier {
            format!("{}.{}", qualifier.to_uppercase(), word.to_uppercase())
        } else {
            word.to_uppercase()
        };

        let connection = self.connection.clone();
        let sender = self.ui_action_sender.clone();
        let sender_for_thread = sender.clone();
        set_cursor(Cursor::Wait);
        app::flush();
        let object_name_for_thread = object_name.clone();
        let spawn_result = thread::Builder::new()
            .name("quick-describe".to_string())
            .spawn(move || {
                let sender_fallback = sender_for_thread.clone();
                let object_name_fallback = object_name_for_thread.clone();
                let result = panic::catch_unwind(AssertUnwindSafe(|| {
                    // Try to acquire connection lock without blocking
                    let Some(mut conn_guard) = crate::db::try_lock_connection_with_activity(
                        &connection,
                        format!("Quick describe {}", object_name_for_thread),
                    ) else {
                        // Query is already running, notify user
                        let _ = sender_for_thread.send(UiActionResult::QueryAlreadyRunning);
                        app::awake();
                        return;
                    };

                    let result = Self::describe_object_for_current_db(
                        &mut conn_guard,
                        &word,
                        qualifier.as_deref(),
                    );

                    let _ = sender_for_thread.send(UiActionResult::QuickDescribe {
                        object_name: object_name_for_thread,
                        result,
                    });
                    app::awake();
                }));
                if let Err(payload) = result {
                    let panic_msg = Self::panic_payload_to_string(payload.as_ref());
                    crate::utils::logging::log_error(
                        "sql_editor::intellisense::quick_describe",
                        &format!("quick describe thread panicked: {}", panic_msg),
                    );
                    let _ = sender_fallback.send(UiActionResult::QuickDescribe {
                        object_name: object_name_fallback,
                        result: Err(format!("Internal error: {}", panic_msg)),
                    });
                    app::awake();
                }
            });

        if let Err(err) = spawn_result {
            let message = format!("Failed to start quick describe task: {err}");
            crate::utils::logging::log_error("sql_editor::intellisense::quick_describe", &message);
            let _ = sender.send(UiActionResult::QuickDescribe {
                object_name,
                result: Err(message),
            });
            app::awake();
        }
    }

    fn describe_object_for_current_db(
        conn_guard: &mut crate::db::ConnectionLockGuard<'_>,
        object_name: &str,
        qualifier: Option<&str>,
    ) -> Result<QuickDescribeData, String> {
        match conn_guard.db_type() {
            crate::db::DatabaseType::Oracle => match conn_guard.require_live_connection() {
                Ok(db_conn) => Self::describe_object(db_conn.as_ref(), object_name, qualifier),
                Err(message) => Err(message),
            },
            crate::db::DatabaseType::MySQL => conn_guard
                .get_mysql_connection_mut()
                .ok_or_else(|| crate::db::NOT_CONNECTED_MESSAGE.to_string())
                .and_then(|mysql_conn| {
                    Self::describe_mysql_object(mysql_conn, object_name, qualifier)
                }),
            crate::db::DatabaseType::OracleThin => {
                Err("Quick describe not supported in Oracle thin mode".to_string())
            }
        }
    }

    fn describe_mysql_object(
        conn: &mut mysql::Conn,
        object_name: &str,
        qualifier: Option<&str>,
    ) -> Result<QuickDescribeData, String> {
        use crate::db::query::mysql_executor::MysqlObjectBrowser;

        let qualified_name = qualifier
            .map(|schema| format!("{schema}.{object_name}"))
            .unwrap_or_else(|| object_name.to_string());

        if let Ok(columns) =
            MysqlObjectBrowser::get_table_structure_in_schema(conn, qualifier, object_name)
        {
            if !columns.is_empty() {
                return Ok(QuickDescribeData::TableColumns(columns));
            }
        }

        let mut object_types =
            MysqlObjectBrowser::get_object_types_in_schema(conn, qualifier, object_name)
                .map_err(|err| err.to_string())?;
        if object_types.is_empty() {
            return Err(format!(
                "Object not found or not accessible: {}",
                qualified_name.to_uppercase()
            ));
        }

        object_types.sort_by_key(|object_type| Self::quick_describe_type_priority(object_type));

        for object_type in object_types {
            let object_type_upper = object_type.to_uppercase();
            match object_type_upper.as_str() {
                "TABLE" | "VIEW" => {
                    if let Ok(columns) = MysqlObjectBrowser::get_table_structure_in_schema(
                        conn,
                        qualifier,
                        object_name,
                    ) {
                        if !columns.is_empty() {
                            return Ok(QuickDescribeData::TableColumns(columns));
                        }
                    }
                }
                "FUNCTION" | "PROCEDURE" => {
                    let args = MysqlObjectBrowser::get_routine_arguments_in_schema(
                        conn,
                        qualifier,
                        object_name,
                    )
                    .map_err(|err| err.to_string())?;
                    let content =
                        Self::format_routine_details(&qualified_name, &object_type_upper, &args);
                    return Ok(QuickDescribeData::Text {
                        title: format!(
                            "Describe: {} ({})",
                            qualified_name.to_uppercase(),
                            object_type_upper
                        ),
                        content,
                    });
                }
                _ => {
                    let ddl = MysqlObjectBrowser::get_create_object_in_schema(
                        conn,
                        qualifier,
                        &object_type_upper,
                        object_name,
                    )
                    .map_err(|err| err.to_string())?;
                    if !ddl.trim().is_empty() {
                        return Ok(QuickDescribeData::Text {
                            title: format!(
                                "Describe: {} ({})",
                                qualified_name.to_uppercase(),
                                object_type_upper
                            ),
                            content: ddl,
                        });
                    }
                }
            }
        }

        Err(format!(
            "Object not found or not accessible: {}",
            qualified_name.to_uppercase()
        ))
    }
}
