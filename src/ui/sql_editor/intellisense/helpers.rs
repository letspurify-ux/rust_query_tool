impl SqlEditorWidget {
    fn is_cursor_inside_cte_explicit_column_list(
        deep_ctx: &intellisense_context::CursorContext,
        cte: &intellisense_context::CteDefinition,
    ) -> bool {
        let cursor_token_idx = deep_ctx
            .cursor_token_len
            .min(deep_ctx.statement_tokens.len());
        cte.explicit_column_range
            .is_some_and(|range| cursor_token_idx >= range.start && cursor_token_idx <= range.end)
    }

    fn collect_cte_virtual_columns_for_completion(
        deep_ctx: &intellisense_context::CursorContext,
        cte: &intellisense_context::CteDefinition,
        virtual_table_columns: &HashMap<String, Vec<String>>,
        intellisense_data: &Arc<Mutex<IntellisenseData>>,
        column_sender: &mpsc::Sender<ColumnLoadUpdate>,
        connection: &SharedConnection,
    ) -> (Vec<String>, Vec<String>) {
        let body_tokens = intellisense_context::token_range_slice(
            deep_ctx.statement_tokens.as_ref(),
            cte.body_range,
        );
        let recursive_generated_columns = intellisense_context::extract_recursive_cte_generated_columns(
            deep_ctx.statement_tokens.as_ref(),
            cte.body_range.end,
        );
        let cursor_in_explicit_list =
            Self::is_cursor_inside_cte_explicit_column_list(deep_ctx, cte);
        let prefer_body_projection = cursor_in_explicit_list && !cte.body_range.is_empty();
        let should_infer_from_body =
            !cte.body_range.is_empty() && (cte.explicit_columns.is_empty() || prefer_body_projection);

        if should_infer_from_body {
            let body_tables_in_scope = intellisense_context::collect_tables_in_statement(body_tokens);
            let (mut columns, wildcard_tables) = Self::collect_virtual_query_projection_columns(
                body_tokens,
                &body_tables_in_scope,
                &[],
                virtual_table_columns,
                intellisense_data,
                column_sender,
                connection,
            );
            columns.extend(recursive_generated_columns);
            Self::dedup_column_names_case_insensitive(&mut columns);
            return (columns, wildcard_tables);
        }

        if !cte.explicit_columns.is_empty() {
            let mut columns = cte.explicit_columns.clone();
            columns.extend(recursive_generated_columns);
            Self::dedup_column_names_case_insensitive(&mut columns);
            return (columns, Vec::new());
        }

        (recursive_generated_columns, Vec::new())
    }

    fn classify_intellisense_context(
        deep_ctx: &intellisense_context::CursorContext,
        _tokens: &[SqlToken],
    ) -> SqlContext {
        if deep_ctx.phase.is_variable_context() {
            SqlContext::VariableName
        } else if deep_ctx.phase.is_bind_context() {
            SqlContext::BindValue
        } else if deep_ctx.phase.is_table_context() {
            SqlContext::TableName
        } else if deep_ctx.phase.is_column_context()
            || matches!(deep_ctx.phase, intellisense_context::SqlPhase::PivotClause)
        {
            if matches!(deep_ctx.phase, intellisense_context::SqlPhase::SelectList) {
                SqlContext::ColumnOrAll
            } else {
                SqlContext::ColumnName
            }
        } else {
            SqlContext::General
        }
    }

    fn column_load_worker_pool() -> &'static ColumnLoadWorkerPool {
        COLUMN_LOAD_WORKER_POOL.get_or_init(Self::build_column_load_worker_pool)
    }

    fn build_column_load_worker_pool() -> ColumnLoadWorkerPool {
        let mut worker_senders = Vec::new();
        let mut worker_handles = Vec::new();

        for idx in 0..COLUMN_LOAD_WORKER_COUNT {
            let (sender, receiver) = mpsc::channel::<ColumnLoadWorkerMessage>();
            let spawn_result = thread::Builder::new()
                .name(format!("intellisense-column-worker-{idx}"))
                .spawn(move || {
                    while let Ok(message) = receiver.recv() {
                        match message {
                            ColumnLoadWorkerMessage::Task(task) => {
                                let task_sender = task.sender.clone();
                                let task_table_key = task.table_key.clone();
                                let result = panic::catch_unwind(AssertUnwindSafe(|| {
                                    Self::process_column_load_task(task);
                                }));
                                if let Err(payload) = result {
                                    let panic_msg = Self::panic_payload_to_string(payload.as_ref());
                                    crate::utils::logging::log_error(
                                        "sql_editor::intellisense::column_loader",
                                        &format!(
                                            "column worker panicked processing {}: {}",
                                            task_table_key, panic_msg
                                        ),
                                    );
                                    // Send empty result to unblock columns_loading tracking
                                    let _ = task_sender.send(ColumnLoadUpdate {
                                        table: task_table_key,
                                        columns: Vec::new(),
                                        cache_columns: false,
                                    });
                                    app::awake();
                                }
                            }
                            ColumnLoadWorkerMessage::Shutdown => break,
                        }
                    }
                });

            match spawn_result {
                Ok(handle) => {
                    worker_senders.push(sender);
                    worker_handles.push(handle);
                }
                Err(err) => {
                    crate::utils::logging::log_error(
                        "sql_editor::intellisense::column_loader",
                        &format!("failed to spawn column worker {idx}: {err}"),
                    );
                }
            }
        }

        ColumnLoadWorkerPool {
            worker_senders,
            worker_handles: Mutex::new(worker_handles),
            next_worker: AtomicUsize::new(0),
        }
    }

    fn enqueue_column_load_task(task: ColumnLoadTask) -> Result<(), ColumnLoadTask> {
        Self::column_load_worker_pool().enqueue(task)
    }

    pub(crate) fn shutdown_column_load_workers() {
        if let Some(pool) = COLUMN_LOAD_WORKER_POOL.get() {
            pool.shutdown();
        }
    }

    fn process_column_load_task(task: ColumnLoadTask) {
        let ColumnLoadTask {
            table_key,
            connection,
            sender,
        } = task;

        // Try-lock with bounded retries to avoid deadlock while still giving
        // background column loading a chance when the connection is briefly busy.
        let mut conn_guard = None;
        for attempt in 0..Self::COLUMN_LOAD_LOCK_RETRY_ATTEMPTS {
            if let Some(guard) = crate::db::try_lock_connection_with_activity(
                &connection,
                format!("Loading columns for {}", table_key),
            ) {
                conn_guard = Some(guard);
                break;
            }
            if attempt + 1 < Self::COLUMN_LOAD_LOCK_RETRY_ATTEMPTS {
                thread::sleep(Duration::from_millis(Self::COLUMN_LOAD_LOCK_RETRY_DELAY_MS));
            }
        }

        let Some(mut conn_guard) = conn_guard else {
            let _ = sender.send(ColumnLoadUpdate {
                table: table_key,
                columns: Vec::new(),
                cache_columns: false,
            });
            app::awake();
            return;
        };

        let (columns, cache_columns) = match conn_guard.require_live_connection() {
            Ok(conn) => {
                match crate::db::ObjectBrowser::get_table_columns(conn.as_ref(), &table_key) {
                    Ok(cols) => (cols.into_iter().map(|col| col.name).collect(), true),
                    Err(_) => (Vec::new(), false),
                }
            }
            Err(_) => (Vec::new(), false),
        };

        let _ = sender.send(ColumnLoadUpdate {
            table: table_key,
            columns,
            cache_columns,
        });
        app::awake();
    }

    fn invoke_void_callback(callback_slot: &Arc<Mutex<Option<Box<dyn FnMut()>>>>) -> bool {
        Self::invoke_callback(callback_slot, "find/replace callback", |cb| cb())
    }

    fn invoke_file_drop_callback(
        callback_slot: &Arc<Mutex<Option<Box<dyn FnMut(PathBuf)>>>>,
        path: PathBuf,
    ) -> bool {
        Self::invoke_callback(callback_slot, "file drop callback", move |cb| cb(path))
    }

    fn invoke_callback<TCallback, TInvoker>(
        callback_slot: &Arc<Mutex<Option<TCallback>>>,
        callback_name: &str,
        invoker: TInvoker,
    ) -> bool
    where
        TInvoker: FnOnce(&mut TCallback),
    {
        let callback = {
            let mut slot = Self::lock_callback_slot(callback_slot);
            slot.take()
        };

        if let Some(mut cb) = callback {
            let result = panic::catch_unwind(AssertUnwindSafe(|| invoker(&mut cb)));
            let mut slot = Self::lock_callback_slot(callback_slot);
            if slot.is_none() {
                *slot = Some(cb);
            }
            if let Err(payload) = result {
                Self::log_callback_panic(callback_name, payload.as_ref());
            }
            true
        } else {
            false
        }
    }

    fn lock_callback_slot<TCallback>(
        callback_slot: &Arc<Mutex<Option<TCallback>>>,
    ) -> std::sync::MutexGuard<'_, Option<TCallback>> {
        match callback_slot.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                eprintln!("Warning: callback slot lock was poisoned; recovering.");
                poisoned.into_inner()
            }
        }
    }

    fn should_consume_popup_confirm_key(key: Key, has_selected: bool) -> bool {
        has_selected && matches!(key, Key::Tab | Key::Enter | Key::KPEnter)
    }

    fn handle_enter_auto_indent(
        editor: &mut TextEditor,
        buffer: &mut TextBuffer,
        text_shadow: &Arc<Mutex<HighlightShadowState>>,
    ) -> bool {
        let selection = buffer.selection_position().map(|(start, end)| {
            let (start_pos, _) = Self::cursor_position(buffer, start);
            let (end_pos, _) = Self::cursor_position(buffer, end);
            if start_pos <= end_pos {
                (start_pos, end_pos)
            } else {
                (end_pos, start_pos)
            }
        });
        let (insert_pos, _) = Self::editor_cursor_position(editor, buffer);
        let anchor = selection
            .map(|(start, _)| start)
            .unwrap_or(insert_pos)
            .max(0);
        let line_start = text_buffer_access::line_start(buffer, Some(text_shadow), anchor).max(0);
        let line_text = text_buffer_access::text_range(buffer, Some(text_shadow), line_start, anchor);
        let indent = Self::leading_indent_prefix(&line_text);
        let inserted = format!("\n{indent}");

        if let Some((start, end)) = selection {
            if start != end {
                buffer.replace(start, end, &inserted);
                editor.set_insert_position(start + inserted.len() as i32);
                editor.show_insert_position();
                return true;
            }
        }

        buffer.insert(insert_pos, &inserted);
        editor.set_insert_position(insert_pos + inserted.len() as i32);
        editor.show_insert_position();
        true
    }

    fn leading_indent_prefix(line_text: &str) -> &str {
        let indent_len = line_text
            .as_bytes()
            .iter()
            .take_while(|byte| matches!(**byte, b' ' | b'\t'))
            .count();
        line_text.get(..indent_len).unwrap_or("")
    }

    #[cfg(test)]
    pub(super) fn take_keyup_debounce_timeout_handle(
        keyup_debounce_handle: &Arc<Mutex<Option<app::TimeoutHandle>>>,
    ) -> Option<app::TimeoutHandle> {
        keyup_debounce_handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    }

    pub(super) fn invalidate_keyup_debounce(
        runtime: &Arc<IntellisenseRuntimeState>,
    ) -> u64 {
        runtime.invalidate_keyup_debounce(false)
    }

    pub(super) fn invalidate_keyup_debounce_with_parse_generation(
        runtime: &Arc<IntellisenseRuntimeState>,
        invalidate_parse_generation: bool,
    ) -> u64 {
        runtime.invalidate_keyup_debounce(invalidate_parse_generation)
    }

    fn invalidate_manual_trigger_debounce_state(runtime: &Arc<IntellisenseRuntimeState>) {
        Self::invalidate_keyup_debounce_with_parse_generation(runtime, true);
    }

    fn finalize_completion_after_selection(
        runtime: &Arc<IntellisenseRuntimeState>,
    ) {
        runtime.clear_ui_tracking();
        Self::invalidate_keyup_debounce_with_parse_generation(runtime, true);
    }

    fn schedule_keyup_intellisense_debounce(
        runtime: &Arc<IntellisenseRuntimeState>,
        scheduled_cursor_raw: i32,
        buffer_len: i32,
        editor: &TextEditor,
        buffer: &TextBuffer,
        text_shadow: &Arc<Mutex<HighlightShadowState>>,
        intellisense_data: &Arc<Mutex<IntellisenseData>>,
        intellisense_popup: &Arc<Mutex<IntellisensePopup>>,
        column_sender: &mpsc::Sender<ColumnLoadUpdate>,
        connection: &SharedConnection,
    ) {
        let generation = Self::invalidate_keyup_debounce_with_parse_generation(runtime, true);
        let runtime_for_timeout = runtime.clone();
        let editor_for_timeout = editor.clone();
        let buffer_for_timeout = buffer.clone();
        let text_shadow_for_timeout = text_shadow.clone();
        let intellisense_data_for_timeout = intellisense_data.clone();
        let intellisense_popup_for_timeout = intellisense_popup.clone();
        let column_sender_for_timeout = column_sender.clone();
        let connection_for_timeout = connection.clone();
        let handle = app::add_timeout3(
            Duration::from_millis(KEYUP_INTELLISENSE_DEBOUNCE_MS).as_secs_f64(),
            move |timeout_handle| {
                {
                    let current_handle = runtime_for_timeout.take_keyup_timeout_handle();
                    if current_handle != Some(timeout_handle) {
                        runtime_for_timeout.set_keyup_timeout_handle(current_handle);
                    }
                }

                if runtime_for_timeout.current_keyup_generation() != generation {
                    return;
                }

                if editor_for_timeout.was_deleted() {
                    return;
                }

                // Hot-path check: for debounce invalidation we only care whether the
                // cursor offset changed, not UTF-8 boundary normalization.
                if !Self::is_same_raw_cursor_offset(
                    editor_for_timeout.insert_position(),
                    scheduled_cursor_raw,
                ) {
                    return;
                }

                if buffer_for_timeout.length() != buffer_len {
                    return;
                }

                Self::trigger_intellisense(
                    &editor_for_timeout,
                    &buffer_for_timeout,
                    &text_shadow_for_timeout,
                    &intellisense_data_for_timeout,
                    &intellisense_popup_for_timeout,
                    &column_sender_for_timeout,
                    &connection_for_timeout,
                    &runtime_for_timeout,
                );
            },
        );
        runtime.set_keyup_timeout_handle(Some(handle));
    }

    fn is_same_raw_cursor_offset(current_raw: i32, scheduled_raw: i32) -> bool {
        current_raw == scheduled_raw
    }


}
