impl SqlEditorWidget {
    fn is_insert_column_list_context(tokens: &[SqlToken], cursor_token_len: usize) -> bool {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        enum InsertParseState {
            Idle,
            AfterInsert,
            AfterInto,
            AfterTarget,
            InColumnList { start_depth: usize },
            AfterColumnList,
            InValuesOrSelectBody,
        }

        let starts_insert_body = |word: &str| {
            word.eq_ignore_ascii_case("VALUES")
                || word.eq_ignore_ascii_case("SELECT")
                || word.eq_ignore_ascii_case("WITH")
        };

        let cursor_token_len = cursor_token_len.min(tokens.len());
        let mut state = InsertParseState::Idle;
        let mut depth = 0usize;
        let mut multitable_insert = false;

        for token in &tokens[..cursor_token_len] {
            match token {
                SqlToken::Comment(_) => {}
                SqlToken::Word(word) => {
                    if word.eq_ignore_ascii_case("INSERT") {
                        state = InsertParseState::AfterInsert;
                        depth = 0;
                        multitable_insert = false;
                        continue;
                    }

                    if depth == 0
                        && matches!(state, InsertParseState::AfterInsert)
                        && (word.eq_ignore_ascii_case("ALL")
                            || word.eq_ignore_ascii_case("FIRST"))
                    {
                        multitable_insert = true;
                        continue;
                    }

                    state = match state {
                        InsertParseState::AfterInsert if word.eq_ignore_ascii_case("INTO") => {
                            InsertParseState::AfterInto
                        }
                        InsertParseState::AfterInto => InsertParseState::AfterTarget,
                        InsertParseState::InValuesOrSelectBody
                            if depth == 0
                                && multitable_insert
                                && word.eq_ignore_ascii_case("INTO") =>
                        {
                            InsertParseState::AfterInto
                        }
                        InsertParseState::AfterTarget | InsertParseState::AfterColumnList
                            if starts_insert_body(word) =>
                        {
                            InsertParseState::InValuesOrSelectBody
                        }
                        current => current,
                    };
                }
                SqlToken::Symbol(sym) if sym == "(" => {
                    if matches!(state, InsertParseState::AfterTarget) {
                        state = InsertParseState::InColumnList {
                            start_depth: depth + 1,
                        };
                    }
                    depth = depth.saturating_add(1);
                }
                SqlToken::Symbol(sym) if sym == ")" => {
                    if depth > 0 {
                        if matches!(
                            state,
                            InsertParseState::InColumnList { start_depth } if start_depth == depth
                        ) {
                            state = InsertParseState::AfterColumnList;
                        }
                        depth -= 1;
                    }
                }
                _ => {}
            }
        }

        matches!(state, InsertParseState::InColumnList { .. })
    }

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

    fn is_with_cte_column_list_context(deep_ctx: &intellisense_context::CursorContext) -> bool {
        deep_ctx
            .ctes
            .iter()
            .any(|cte| Self::is_cursor_inside_cte_explicit_column_list(deep_ctx, cte))
    }

    fn collect_cte_virtual_columns_for_completion(
        deep_ctx: &intellisense_context::CursorContext,
        cte: &intellisense_context::CteDefinition,
        intellisense_data: &Arc<Mutex<IntellisenseData>>,
        column_sender: &mpsc::Sender<ColumnLoadUpdate>,
        connection: &SharedConnection,
    ) -> (Vec<String>, Vec<String>) {
        let body_tokens = intellisense_context::token_range_slice(
            deep_ctx.statement_tokens.as_ref(),
            cte.body_range,
        );
        let cursor_in_explicit_list =
            Self::is_cursor_inside_cte_explicit_column_list(deep_ctx, cte);
        let prefer_body_projection = cursor_in_explicit_list && !cte.body_range.is_empty();

        // While editing WITH cte(col1, ...), prefer body projection columns as completion
        // candidates even when an explicit list is already partially typed.
        let mut columns = Self::collect_cte_base_columns(cte, body_tokens, prefer_body_projection);

        let mut wildcard_tables = Vec::new();
        if Self::should_expand_cte_wildcards(cte, prefer_body_projection) {
            let body_tables_in_scope =
                intellisense_context::collect_tables_in_statement(body_tokens);
            let (wildcard_columns, deps) = Self::expand_virtual_table_wildcards(
                body_tokens,
                &body_tables_in_scope,
                intellisense_data,
                column_sender,
                connection,
            );
            if !deps.is_empty() {
                wildcard_tables = deps;
            }
            columns.extend(wildcard_columns);
        }

        columns.extend(
            intellisense_context::extract_oracle_pivot_unpivot_projection_columns(body_tokens),
        );
        Self::dedup_column_names_case_insensitive(&mut columns);
        (columns, wildcard_tables)
    }

    fn collect_cte_base_columns(
        cte: &intellisense_context::CteDefinition,
        body_tokens: &[SqlToken],
        prefer_body_projection: bool,
    ) -> Vec<String> {
        if prefer_body_projection {
            return intellisense_context::extract_select_list_columns(body_tokens);
        }

        if !cte.explicit_columns.is_empty() {
            return cte.explicit_columns.clone();
        }

        if cte.body_range.is_empty() {
            Vec::new()
        } else {
            intellisense_context::extract_select_list_columns(body_tokens)
        }
    }

    fn should_expand_cte_wildcards(
        cte: &intellisense_context::CteDefinition,
        prefer_body_projection: bool,
    ) -> bool {
        !cte.body_range.is_empty() && (cte.explicit_columns.is_empty() || prefer_body_projection)
    }

    fn classify_intellisense_context(
        deep_ctx: &intellisense_context::CursorContext,
        tokens: &[SqlToken],
    ) -> SqlContext {
        let insert_column_list_context =
            matches!(deep_ctx.phase, intellisense_context::SqlPhase::IntoClause)
                && Self::is_insert_column_list_context(tokens, deep_ctx.cursor_token_len);
        let with_cte_column_list_context = Self::is_with_cte_column_list_context(deep_ctx);

        if deep_ctx.phase.is_table_context() && !insert_column_list_context {
            SqlContext::TableName
        } else if deep_ctx.phase.is_column_context()
            || matches!(deep_ctx.phase, intellisense_context::SqlPhase::PivotClause)
            || insert_column_list_context
            || with_cte_column_list_context
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
