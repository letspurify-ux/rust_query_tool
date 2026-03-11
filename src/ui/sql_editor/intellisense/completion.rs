impl SqlEditorWidget {
    pub(super) fn trigger_intellisense(
        editor: &TextEditor,
        buffer: &TextBuffer,
        text_shadow: &Arc<Mutex<HighlightShadowState>>,
        intellisense_data: &Arc<Mutex<IntellisenseData>>,
        intellisense_popup: &Arc<Mutex<IntellisensePopup>>,
        column_sender: &mpsc::Sender<ColumnLoadUpdate>,
        connection: &SharedConnection,
        runtime: &Arc<IntellisenseRuntimeState>,
    ) {
        let request_generation = runtime.next_parse_generation();
        let buffer_revision = runtime.current_buffer_revision();
        let (cursor_pos, cursor_pos_usize) = Self::editor_cursor_position(editor, buffer);
        let (prefix, word_start, _) = Self::word_at_cursor(buffer, text_shadow, cursor_pos);
        let qualifier = Self::qualifier_before_word(buffer, text_shadow, word_start);
        let should_hide_after_statement_terminator = prefix.is_empty()
            && qualifier.is_none()
            && Self::non_whitespace_char_before_cursor(buffer, text_shadow, cursor_pos) == Some(';');

        if should_hide_after_statement_terminator {
            intellisense_popup
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .hide();
            runtime.clear_ui_tracking();
            return;
        }

        let (statement_window_text, cursor_in_window) =
            Self::statement_window_with_cursor(buffer, text_shadow, cursor_pos);
        let snapshot = Arc::new(IntellisenseTriggerSnapshot {
            request_generation,
            buffer_revision,
            cursor_pos,
            cursor_pos_usize,
            prefix,
            word_start,
            qualifier,
            statement_window_text,
            cursor_in_window,
        });

        let cached_context = runtime.parse_cache().and_then(|entry| {
            (entry.buffer_revision == snapshot.buffer_revision
                && entry.cursor_pos == snapshot.cursor_pos)
                .then_some(entry.context.clone())
        });

        if let Some(context) = cached_context {
            Self::apply_intellisense_with_context(
                editor,
                intellisense_data,
                intellisense_popup,
                column_sender,
                connection,
                runtime,
                snapshot.as_ref(),
                context.as_ref(),
            );
            return;
        }

        // Cache miss means full parse is pending on a worker.
        // Hide stale popup/completion state to avoid applying outdated candidates.
        Self::clear_intellisense_ui_state(
            intellisense_popup,
            runtime,
        );

        Self::queue_async_intellisense_parse(
            editor,
            buffer,
            text_shadow,
            intellisense_data,
            intellisense_popup,
            column_sender,
            connection,
            runtime,
            snapshot.clone(),
        );
    }

    fn analyze_statement_context(
        statement_text: &str,
        cursor_in_statement: usize,
    ) -> intellisense_context::CursorContext {
        let full_token_spans = super::query_text::tokenize_sql_spanned(statement_text);
        let split_idx = full_token_spans.partition_point(|span| span.end <= cursor_in_statement);
        let full_tokens: Vec<SqlToken> = full_token_spans
            .into_iter()
            .map(|span| span.token)
            .collect();
        intellisense_context::analyze_cursor_context(&full_tokens, split_idx)
    }

    fn analyze_statement_window_context(
        statement_window_text: &str,
        cursor_in_window: usize,
    ) -> intellisense_context::CursorContext {
        if statement_window_text.is_empty() {
            return Self::analyze_statement_context("", 0);
        }

        let cursor_in_window =
            Self::clamp_to_char_boundary_local(statement_window_text, cursor_in_window);
        let (stmt_start, stmt_end) =
            Self::statement_bounds_in_text(statement_window_text, cursor_in_window);
        let statement = statement_window_text.get(stmt_start..stmt_end).unwrap_or("");
        let cursor_in_statement_raw = cursor_in_window
            .saturating_sub(stmt_start)
            .min(statement.len());
        let (statement_text, cursor_in_statement) =
            Self::normalize_intellisense_context_with_cursor(
                statement,
                cursor_in_statement_raw,
            );
        Self::analyze_statement_context(&statement_text, cursor_in_statement)
    }

    fn is_intellisense_snapshot_current(
        editor: &TextEditor,
        runtime: &Arc<IntellisenseRuntimeState>,
        snapshot: &IntellisenseTriggerSnapshot,
    ) -> bool {
        if editor.was_deleted() {
            return false;
        }

        if editor.insert_position() != snapshot.cursor_pos {
            return false;
        }

        runtime.current_buffer_revision() == snapshot.buffer_revision
    }

    fn is_intellisense_parse_generation_current(
        runtime: &Arc<IntellisenseRuntimeState>,
        snapshot: &IntellisenseTriggerSnapshot,
    ) -> bool {
        runtime.current_parse_generation() == snapshot.request_generation
    }

    fn clear_intellisense_ui_state(
        intellisense_popup: &Arc<Mutex<IntellisensePopup>>,
        runtime: &Arc<IntellisenseRuntimeState>,
    ) {
        intellisense_popup
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .hide();
        runtime.clear_ui_tracking();
    }

    fn clear_intellisense_state_for_external_hide(
        runtime: &Arc<IntellisenseRuntimeState>,
    ) {
        Self::invalidate_and_clear_pending_intellisense_state(runtime);
    }

    fn should_ignore_external_hide_click(popup_visible: bool, click_inside_popup: bool) -> bool {
        popup_visible && click_inside_popup
    }

    fn should_hide_popup_on_unfocus(popup_visible: bool, pointer_inside_popup: bool) -> bool {
        popup_visible && !pointer_inside_popup
    }

    fn schedule_deferred_unfocus_popup_hide(
        editor: TextEditor,
        intellisense_popup: Arc<Mutex<IntellisensePopup>>,
        runtime: Arc<IntellisenseRuntimeState>,
        pointer_x: i32,
        pointer_y: i32,
        retries_left: u8,
    ) {
        app::add_timeout3(0.0, move |_| {
            if editor.was_deleted() {
                return;
            }

            if matches!(
                runtime.popup_transition_state(),
                IntellisensePopupTransitionState::Showing
            ) {
                if retries_left > 0 {
                    Self::schedule_deferred_unfocus_popup_hide(
                        editor.clone(),
                        intellisense_popup.clone(),
                        runtime.clone(),
                        pointer_x,
                        pointer_y,
                        retries_left - 1,
                    );
                }
                return;
            }

            if editor.has_focus() {
                return;
            }
            let mut popup = intellisense_popup
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let popup_visible = popup.is_visible();
            let pointer_inside_popup = popup_visible && popup.contains_point(pointer_x, pointer_y);
            if !Self::should_hide_popup_on_unfocus(popup_visible, pointer_inside_popup) {
                return;
            }
            popup.hide();
            drop(popup);
            Self::clear_intellisense_state_for_external_hide(&runtime);
        });
    }

    fn schedule_deferred_outside_click_popup_hide(
        intellisense_popup: Arc<Mutex<IntellisensePopup>>,
        runtime: Arc<IntellisenseRuntimeState>,
        click_x: i32,
        click_y: i32,
        retries_left: u8,
    ) {
        app::add_timeout3(0.0, move |_| {
            if matches!(
                runtime.popup_transition_state(),
                IntellisensePopupTransitionState::Showing
            ) {
                if retries_left > 0 {
                    Self::schedule_deferred_outside_click_popup_hide(
                        intellisense_popup.clone(),
                        runtime.clone(),
                        click_x,
                        click_y,
                        retries_left - 1,
                    );
                }
                return;
            }
            let mut popup = intellisense_popup
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let popup_visible = popup.is_visible();
            if !popup_visible {
                return;
            }
            let click_inside_popup = popup.contains_point(click_x, click_y);
            if Self::should_ignore_external_hide_click(popup_visible, click_inside_popup) {
                return;
            }
            popup.hide();
            drop(popup);
            Self::clear_intellisense_state_for_external_hide(&runtime);
        });
    }

    fn invalidate_and_clear_pending_intellisense_state(
        runtime: &Arc<IntellisenseRuntimeState>,
    ) {
        runtime.clear_ui_tracking();
        Self::invalidate_keyup_debounce_with_parse_generation(runtime, true);
    }

    fn cancel_intellisense_on_escape_keydown(
        popup_visible: bool,
        runtime: &Arc<IntellisenseRuntimeState>,
    ) -> bool {
        Self::invalidate_and_clear_pending_intellisense_state(runtime);
        popup_visible
    }

    fn queue_async_intellisense_parse(
        editor: &TextEditor,
        buffer: &TextBuffer,
        text_shadow: &Arc<Mutex<HighlightShadowState>>,
        intellisense_data: &Arc<Mutex<IntellisenseData>>,
        intellisense_popup: &Arc<Mutex<IntellisensePopup>>,
        column_sender: &mpsc::Sender<ColumnLoadUpdate>,
        connection: &SharedConnection,
        runtime: &Arc<IntellisenseRuntimeState>,
        snapshot: Arc<IntellisenseTriggerSnapshot>,
    ) {
        let (parse_sender, parse_receiver) =
            mpsc::channel::<Result<intellisense_context::CursorContext, String>>();
        let parse_receiver = Arc::new(Mutex::new(parse_receiver));
        let snapshot_for_thread = snapshot.clone();
        let spawn_result = thread::Builder::new()
            .name("intellisense-parse-worker".to_string())
            .spawn(move || {
                let result = panic::catch_unwind(AssertUnwindSafe(|| {
                    Self::analyze_statement_window_context(
                        &snapshot_for_thread.statement_window_text,
                        snapshot_for_thread.cursor_in_window,
                    )
                }));

                match result {
                    Ok(parsed) => {
                        let _ = parse_sender.send(Ok(parsed));
                    }
                    Err(payload) => {
                        let panic_msg = Self::panic_payload_to_string(payload.as_ref());
                        crate::utils::logging::log_error(
                            "sql_editor::intellisense::parse_worker",
                            &format!("parse worker panicked: {panic_msg}"),
                        );
                        let _ = parse_sender.send(Err(format!("Internal error: {panic_msg}")));
                    }
                }
                app::awake();
            });

        if let Err(err) = spawn_result {
            crate::utils::logging::log_error(
                "sql_editor::intellisense::parse_worker",
                &format!("failed to spawn parse worker: {err}"),
            );
            if Self::is_intellisense_parse_generation_current(runtime, snapshot.as_ref())
                && Self::is_intellisense_snapshot_current(editor, runtime, snapshot.as_ref())
            {
                Self::clear_intellisense_ui_state(intellisense_popup, runtime);
            }
            return;
        }

        let editor_for_poll = editor.clone();
        let buffer_for_poll = buffer.clone();
        let text_shadow_for_poll = text_shadow.clone();
        let intellisense_data_for_poll = intellisense_data.clone();
        let intellisense_popup_for_poll = intellisense_popup.clone();
        let column_sender_for_poll = column_sender.clone();
        let connection_for_poll = connection.clone();
        let runtime_for_poll = runtime.clone();
        app::add_timeout3(0.0, move |_| {
            Self::poll_async_intellisense_parse(
                editor_for_poll.clone(),
                buffer_for_poll.clone(),
                text_shadow_for_poll.clone(),
                intellisense_data_for_poll.clone(),
                intellisense_popup_for_poll.clone(),
                column_sender_for_poll.clone(),
                connection_for_poll.clone(),
                runtime_for_poll.clone(),
                snapshot.clone(),
                parse_receiver.clone(),
            );
        });
    }

    fn poll_async_intellisense_parse(
        editor: TextEditor,
        buffer: TextBuffer,
        text_shadow: Arc<Mutex<HighlightShadowState>>,
        intellisense_data: Arc<Mutex<IntellisenseData>>,
        intellisense_popup: Arc<Mutex<IntellisensePopup>>,
        column_sender: mpsc::Sender<ColumnLoadUpdate>,
        connection: SharedConnection,
        runtime: Arc<IntellisenseRuntimeState>,
        snapshot: Arc<IntellisenseTriggerSnapshot>,
        parse_receiver: Arc<
            Mutex<mpsc::Receiver<Result<intellisense_context::CursorContext, String>>>,
        >,
    ) {
        if editor.was_deleted() {
            return;
        }
        if !Self::is_intellisense_parse_generation_current(&runtime, snapshot.as_ref()) {
            return;
        }

        let recv_result = {
            let receiver = parse_receiver
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            receiver.try_recv()
        };

        match recv_result {
            Ok(Ok(parsed)) => {
                if !Self::is_intellisense_parse_generation_current(&runtime, snapshot.as_ref())
                    || !Self::is_intellisense_snapshot_current(
                        &editor,
                        &runtime,
                        snapshot.as_ref(),
                    )
                {
                    return;
                }
                let parsed = Arc::new(parsed);
                runtime.set_parse_cache(Some(IntellisenseParseCacheEntry {
                    buffer_revision: snapshot.buffer_revision,
                    cursor_pos: snapshot.cursor_pos,
                    context: parsed.clone(),
                }));

                Self::apply_intellisense_with_context(
                    &editor,
                    &intellisense_data,
                    &intellisense_popup,
                    &column_sender,
                    &connection,
                    &runtime,
                    snapshot.as_ref(),
                    parsed.as_ref(),
                );
            }
            Ok(Err(message)) => {
                crate::utils::logging::log_error(
                    "sql_editor::intellisense::parse_worker",
                    &format!("failed to parse intellisense context: {message}"),
                );
                if Self::is_intellisense_parse_generation_current(&runtime, snapshot.as_ref())
                    && Self::is_intellisense_snapshot_current(
                        &editor,
                        &runtime,
                        snapshot.as_ref(),
                    )
                {
                    Self::clear_intellisense_ui_state(&intellisense_popup, &runtime);
                }
            }
            Err(mpsc::TryRecvError::Empty) => {
                app::add_timeout3(INTELLISENSE_PARSE_POLL_INTERVAL_SECONDS, move |_| {
                    Self::poll_async_intellisense_parse(
                        editor.clone(),
                        buffer.clone(),
                        text_shadow.clone(),
                        intellisense_data.clone(),
                        intellisense_popup.clone(),
                        column_sender.clone(),
                        connection.clone(),
                        runtime.clone(),
                        snapshot.clone(),
                        parse_receiver.clone(),
                    );
                });
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                if Self::is_intellisense_parse_generation_current(&runtime, snapshot.as_ref())
                    && Self::is_intellisense_snapshot_current(
                        &editor,
                        &runtime,
                        snapshot.as_ref(),
                    )
                {
                    Self::clear_intellisense_ui_state(&intellisense_popup, &runtime);
                }
            }
        }
    }

    fn apply_intellisense_with_context(
        editor: &TextEditor,
        intellisense_data: &Arc<Mutex<IntellisenseData>>,
        intellisense_popup: &Arc<Mutex<IntellisensePopup>>,
        column_sender: &mpsc::Sender<ColumnLoadUpdate>,
        connection: &SharedConnection,
        runtime: &Arc<IntellisenseRuntimeState>,
        snapshot: &IntellisenseTriggerSnapshot,
        deep_ctx: &intellisense_context::CursorContext,
    ) {
        let qualifier = snapshot.qualifier.as_deref();
        let context =
            Self::classify_intellisense_context(deep_ctx, deep_ctx.statement_tokens.as_ref());

        let column_tables = Self::resolve_column_tables_for_context(qualifier, deep_ctx);
        let include_columns = qualifier.is_some()
            || matches!(context, SqlContext::ColumnName | SqlContext::ColumnOrAll);

        let allow_empty_prefix =
            qualifier.is_some() || include_columns || matches!(context, SqlContext::TableName);
        if snapshot.prefix.is_empty() && !allow_empty_prefix {
            // Context no longer allows completion for empty prefix, so hide
            // stale popup state immediately.
            Self::clear_intellisense_ui_state(intellisense_popup, runtime);
            return;
        }

        let mut virtual_wildcard_dependencies: HashMap<String, Vec<String>> = HashMap::new();
        if include_columns {
            let mut virtual_table_columns: HashMap<String, Vec<String>> = HashMap::new();
            for cte in &deep_ctx.ctes {
                let (columns, wildcard_tables) = Self::collect_cte_virtual_columns_for_completion(
                    deep_ctx,
                    cte,
                    intellisense_data,
                    column_sender,
                    connection,
                );
                if !wildcard_tables.is_empty() {
                    virtual_wildcard_dependencies.insert(cte.name.to_uppercase(), wildcard_tables);
                }
                if !columns.is_empty() {
                    virtual_table_columns.insert(cte.name.clone(), columns);
                }
            }

            for subq in &deep_ctx.subqueries {
                let body_tokens = intellisense_context::token_range_slice(
                    deep_ctx.statement_tokens.as_ref(),
                    subq.body_range,
                );
                let mut columns = intellisense_context::extract_select_list_columns(body_tokens);
                let body_tables_in_scope =
                    intellisense_context::collect_tables_in_statement(body_tokens);
                if columns.is_empty() {
                    columns = intellisense_context::extract_table_function_columns(body_tokens);
                }
                if columns.is_empty() {
                    columns = Self::infer_columns_from_partial_select_qualifiers(
                        body_tokens,
                        &body_tables_in_scope,
                        &deep_ctx.tables_in_scope,
                        &virtual_table_columns,
                        intellisense_data,
                        column_sender,
                        connection,
                    );
                }
                let (wildcard_columns, wildcard_tables) = Self::expand_virtual_table_wildcards(
                    body_tokens,
                    &body_tables_in_scope,
                    intellisense_data,
                    column_sender,
                    connection,
                );
                if !wildcard_tables.is_empty() {
                    virtual_wildcard_dependencies
                        .insert(subq.alias.to_uppercase(), wildcard_tables);
                }
                columns.extend(wildcard_columns);
                columns.extend(
                    intellisense_context::extract_oracle_pivot_unpivot_projection_columns(
                        body_tokens,
                    ),
                );
                Self::dedup_column_names_case_insensitive(&mut columns);
                if !columns.is_empty() {
                    virtual_table_columns.insert(subq.alias.clone(), columns);
                }
            }
            intellisense_data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .replace_virtual_table_columns(virtual_table_columns);

            for table in &column_tables {
                let is_virtual = deep_ctx
                    .ctes
                    .iter()
                    .any(|c| c.name.eq_ignore_ascii_case(table))
                    || deep_ctx
                        .subqueries
                        .iter()
                        .any(|s| s.alias.eq_ignore_ascii_case(table));
                if !is_virtual {
                    Self::request_table_columns(
                        table,
                        intellisense_data,
                        column_sender,
                        connection,
                    );
                }
            }
        }

        let columns_loading = if include_columns {
            let data = intellisense_data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            Self::has_column_loading_for_scope(
                include_columns,
                &column_tables,
                &virtual_wildcard_dependencies,
                &data,
            )
        } else {
            false
        };

        let mut suggestions = {
            let mut data = intellisense_data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let column_scope = if !column_tables.is_empty() {
                Some(column_tables.as_slice())
            } else {
                None
            };
            if qualifier.is_some() {
                data.get_column_suggestions(&snapshot.prefix, column_scope)
            } else {
                data.get_suggestions(
                    &snapshot.prefix,
                    include_columns,
                    column_scope,
                    matches!(context, SqlContext::TableName),
                    matches!(context, SqlContext::ColumnName | SqlContext::ColumnOrAll),
                )
            }
        };
        if include_columns && qualifier.is_none() {
            let derived_columns = Self::collect_derived_columns_for_context(deep_ctx);
            suggestions = Self::merge_suggestions_with_derived_columns(
                suggestions,
                &snapshot.prefix,
                derived_columns,
            );
        }
        let context_alias_suggestions =
            Self::collect_context_alias_suggestions(&snapshot.prefix, deep_ctx);
        let suggestions = Self::maybe_merge_suggestions_with_context_aliases(
            suggestions,
            context_alias_suggestions,
            matches!(context, SqlContext::TableName),
            qualifier.is_some(),
        );

        let should_refresh_when_columns_ready = include_columns && columns_loading;
        if should_refresh_when_columns_ready {
            runtime.set_pending_intellisense(Some(PendingIntellisense {
                cursor_pos: snapshot.cursor_pos,
            }));
        } else {
            runtime.clear_pending_intellisense();
        }

        if suggestions.is_empty() {
            intellisense_popup
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .hide();
            runtime.clear_completion_range();
            return;
        }

        let popup_width = Self::INTELLISENSE_POPUP_WIDTH;
        let popup_height = (suggestions.len().min(10) * 20 + 10) as i32;
        let (popup_x, popup_y) =
            Self::popup_screen_position(editor, snapshot.cursor_pos, popup_width, popup_height);
        struct PopupShowInProgressReset {
            runtime: Arc<IntellisenseRuntimeState>,
        }
        impl Drop for PopupShowInProgressReset {
            fn drop(&mut self) {
                self.runtime
                    .set_popup_transition_state(IntellisensePopupTransitionState::Idle);
            }
        }
        runtime.set_popup_transition_state(IntellisensePopupTransitionState::Showing);
        let _popup_show_reset = PopupShowInProgressReset {
            runtime: runtime.clone(),
        };
        intellisense_popup
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .show_suggestions(suggestions, popup_x, popup_y);
        let completion_start = if snapshot.prefix.is_empty() {
            snapshot.cursor_pos_usize
        } else {
            snapshot.word_start
        };
        runtime.set_completion_range(Some(IntellisenseCompletionRange::new(
            completion_start,
            snapshot.cursor_pos_usize,
        )));
        let mut editor = editor.clone();
        let _ = editor.take_focus();
    }

    fn expand_virtual_table_wildcards(
        body_tokens: &[SqlToken],
        body_tables_in_scope: &[intellisense_context::ScopedTableRef],
        intellisense_data: &Arc<Mutex<IntellisenseData>>,
        column_sender: &mpsc::Sender<ColumnLoadUpdate>,
        connection: &SharedConnection,
    ) -> (Vec<String>, Vec<String>) {
        let wildcard_tables = intellisense_context::extract_select_list_wildcard_tables(
            body_tokens,
            body_tables_in_scope,
        );
        if wildcard_tables.is_empty() {
            return (Vec::new(), Vec::new());
        }

        let mut wildcard_columns = Vec::new();
        for table in &wildcard_tables {
            Self::request_table_columns(table, intellisense_data, column_sender, connection);
            let columns = {
                let data = intellisense_data
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                data.get_columns_for_table(table)
            };
            wildcard_columns.extend(columns);
        }
        Self::dedup_column_names_case_insensitive(&mut wildcard_columns);
        (wildcard_columns, wildcard_tables)
    }

    fn dedup_column_names_case_insensitive(columns: &mut Vec<String>) {
        let mut seen = HashSet::new();
        columns.retain(|column| seen.insert(column.to_ascii_uppercase()));
    }

    fn has_column_loading_for_scope(
        include_columns: bool,
        column_tables: &[String],
        virtual_wildcard_dependencies: &HashMap<String, Vec<String>>,
        data: &IntellisenseData,
    ) -> bool {
        if !include_columns {
            return false;
        }

        fn table_is_loading(data: &IntellisenseData, table: &str) -> bool {
            // Fast path: check uppercased name directly before allocating candidates.
            let upper = table.to_uppercase();
            if data.columns_loading.contains(&upper) {
                return true;
            }
            // Only build full candidate list when the name has a qualified dot.
            if !SqlEditorWidget::has_unquoted_dot(table) {
                return false;
            }
            SqlEditorWidget::table_lookup_key_candidates(table)
                .iter()
                .any(|key| {
                    let key_upper = key.to_uppercase();
                    key_upper != upper && data.columns_loading.contains(&key_upper)
                })
        }

        column_tables.iter().any(|table| {
            if table_is_loading(data, table) {
                return true;
            }
            let key = table.to_uppercase();
            virtual_wildcard_dependencies
                .get(&key)
                .is_some_and(|deps| deps.iter().any(|dep| table_is_loading(data, dep)))
        })
    }

    fn collect_context_alias_suggestions(
        prefix: &str,
        deep_ctx: &intellisense_context::CursorContext,
    ) -> Vec<String> {
        let prefix_upper = prefix.to_ascii_uppercase();
        let mut suggestions = Vec::new();
        let mut seen = HashSet::new();

        let mut push_candidate = |candidate: &str| {
            if candidate.is_empty() {
                return;
            }
            let candidate_upper = candidate.to_ascii_uppercase();
            if !prefix_upper.is_empty()
                && (!candidate_upper.starts_with(&prefix_upper) || candidate_upper == prefix_upper)
            {
                return;
            }
            if seen.insert(candidate_upper) {
                suggestions.push(candidate.to_string());
            }
        };

        for table_ref in &deep_ctx.tables_in_scope {
            if let Some(alias) = table_ref.alias.as_deref() {
                push_candidate(alias);
            }
        }

        for cte in &deep_ctx.ctes {
            push_candidate(&cte.name);
        }

        for subq in &deep_ctx.subqueries {
            push_candidate(&subq.alias);
        }

        suggestions
    }

    fn merge_suggestions_with_context_aliases(
        mut base: Vec<String>,
        aliases: Vec<String>,
        prefer_aliases: bool,
    ) -> Vec<String> {
        if aliases.is_empty() {
            base.truncate(MAX_MERGED_SUGGESTIONS);
            return base;
        }

        let mut seen: HashSet<String> = base.iter().map(|item| item.to_ascii_uppercase()).collect();
        let mut filtered_aliases = Vec::new();
        for alias in aliases {
            if seen.insert(alias.to_ascii_uppercase()) {
                filtered_aliases.push(alias);
            }
        }

        if filtered_aliases.is_empty() {
            base.truncate(MAX_MERGED_SUGGESTIONS);
            return base;
        }

        let mut merged = if prefer_aliases {
            filtered_aliases.extend(base);
            filtered_aliases
        } else {
            base.extend(filtered_aliases);
            base
        };
        merged.truncate(MAX_MERGED_SUGGESTIONS);
        merged
    }

    fn maybe_merge_suggestions_with_context_aliases(
        mut base: Vec<String>,
        aliases: Vec<String>,
        prefer_aliases: bool,
        has_qualifier: bool,
    ) -> Vec<String> {
        if has_qualifier {
            base.truncate(MAX_MERGED_SUGGESTIONS);
            return base;
        }
        Self::merge_suggestions_with_context_aliases(base, aliases, prefer_aliases)
    }

    fn infer_columns_from_partial_select_qualifiers(
        body_tokens: &[SqlToken],
        body_tables_in_scope: &[intellisense_context::ScopedTableRef],
        outer_tables_in_scope: &[intellisense_context::ScopedTableRef],
        virtual_table_columns: &HashMap<String, Vec<String>>,
        intellisense_data: &Arc<Mutex<IntellisenseData>>,
        column_sender: &mpsc::Sender<ColumnLoadUpdate>,
        connection: &SharedConnection,
    ) -> Vec<String> {
        let qualifiers = intellisense_context::extract_select_list_leading_qualifiers(body_tokens);
        if qualifiers.is_empty() {
            return Vec::new();
        }

        let mut columns = Vec::new();
        for qualifier in qualifiers {
            let mut tables =
                intellisense_context::resolve_qualifier_tables(&qualifier, body_tables_in_scope);
            let unresolved_direct =
                tables.len() == 1 && tables[0].eq_ignore_ascii_case(qualifier.as_str());
            if unresolved_direct {
                let outer_tables = intellisense_context::resolve_qualifier_tables(
                    &qualifier,
                    outer_tables_in_scope,
                );
                let outer_unresolved_direct = outer_tables.len() == 1
                    && outer_tables[0].eq_ignore_ascii_case(qualifier.as_str());
                if !outer_unresolved_direct {
                    tables = outer_tables;
                }
            }

            for table in tables {
                if let Some(virtual_cols) =
                    Self::find_virtual_columns_case_insensitive(virtual_table_columns, &table)
                {
                    columns.extend(virtual_cols.iter().cloned());
                    continue;
                }

                let mut table_columns = {
                    let data = intellisense_data
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    data.get_columns_for_table(&table)
                };
                if table_columns.is_empty() {
                    Self::request_table_columns(
                        &table,
                        intellisense_data,
                        column_sender,
                        connection,
                    );
                    table_columns = {
                        let data = intellisense_data
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        data.get_columns_for_table(&table)
                    };
                }
                columns.extend(table_columns);
            }
        }

        Self::dedup_column_names_case_insensitive(&mut columns);
        columns
    }

    fn find_virtual_columns_case_insensitive<'a>(
        virtual_table_columns: &'a HashMap<String, Vec<String>>,
        table: &str,
    ) -> Option<&'a [String]> {
        virtual_table_columns
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(table))
            .map(|(_, cols)| cols.as_slice())
    }

    fn resolve_column_tables_for_context(
        qualifier: Option<&str>,
        deep_ctx: &intellisense_context::CursorContext,
    ) -> Vec<String> {
        let Some(qualifier) = qualifier else {
            return intellisense_context::resolve_all_scope_tables(&deep_ctx.tables_in_scope);
        };

        let resolved =
            intellisense_context::resolve_qualifier_tables(qualifier, &deep_ctx.tables_in_scope);
        let unresolved_direct = resolved.len() == 1 && resolved[0].eq_ignore_ascii_case(qualifier);
        if !unresolved_direct {
            return resolved;
        }

        let pattern_vars = intellisense_context::extract_match_recognize_pattern_variables(
            deep_ctx.statement_tokens.as_ref(),
        );
        if pattern_vars
            .iter()
            .any(|var| var.eq_ignore_ascii_case(qualifier))
        {
            return intellisense_context::resolve_all_scope_tables(&deep_ctx.tables_in_scope);
        }

        resolved
    }

    fn merge_suggestions_with_derived_columns(
        mut base: Vec<String>,
        prefix: &str,
        derived_columns: Vec<String>,
    ) -> Vec<String> {
        if derived_columns.is_empty() {
            base.truncate(MAX_MERGED_SUGGESTIONS);
            return base;
        }

        let prefix_upper = prefix.to_ascii_uppercase();
        let mut seen: HashSet<String> = base.iter().map(|item| item.to_ascii_uppercase()).collect();

        for column in derived_columns {
            let upper = column.to_ascii_uppercase();
            if !prefix_upper.is_empty()
                && (!upper.starts_with(prefix_upper.as_str()) || upper == prefix_upper)
            {
                continue;
            }
            if seen.insert(upper) {
                base.push(column);
            }
        }

        base.truncate(MAX_MERGED_SUGGESTIONS);
        base
    }

    fn collect_derived_columns_for_context(
        deep_ctx: &intellisense_context::CursorContext,
    ) -> Vec<String> {
        let mut derived_columns = intellisense_context::extract_oracle_unpivot_generated_columns(
            deep_ctx.statement_tokens.as_ref(),
        );
        derived_columns.extend(
            intellisense_context::extract_oracle_model_generated_columns(
                deep_ctx.statement_tokens.as_ref(),
            ),
        );
        derived_columns.extend(
            intellisense_context::extract_match_recognize_generated_columns(
                deep_ctx.statement_tokens.as_ref(),
            ),
        );

        if matches!(
            deep_ctx.phase,
            intellisense_context::SqlPhase::OrderByClause
        ) {
            derived_columns.extend(intellisense_context::extract_select_list_columns(
                deep_ctx.statement_tokens.as_ref(),
            ));
        }

        Self::dedup_column_names_case_insensitive(&mut derived_columns);
        derived_columns
    }

    fn maybe_prefetch_columns_for_word(
        word: &str,
        intellisense_data: &Arc<Mutex<IntellisenseData>>,
        column_sender: &mpsc::Sender<ColumnLoadUpdate>,
        connection: &SharedConnection,
    ) {
        if word.is_empty() {
            return;
        }

        let should_prefetch = {
            let data = intellisense_data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            data.is_known_relation(word)
        };

        if should_prefetch {
            Self::request_table_columns(word, intellisense_data, column_sender, connection);
        }
    }

    fn request_table_columns(
        table_name: &str,
        intellisense_data: &Arc<Mutex<IntellisenseData>>,
        column_sender: &mpsc::Sender<ColumnLoadUpdate>,
        connection: &SharedConnection,
    ) {
        let table_key_candidates = Self::table_lookup_key_candidates(table_name);
        if table_key_candidates.is_empty() {
            return;
        }

        let table_key = {
            let mut data = intellisense_data
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let selected = table_key_candidates
                .iter()
                .find(|candidate| data.is_known_relation(candidate))
                .cloned();
            let Some(selected) = selected else {
                return;
            };
            if !data.mark_columns_loading(&selected) {
                return;
            }
            selected
        };

        let task = ColumnLoadTask {
            table_key,
            connection: connection.clone(),
            sender: column_sender.clone(),
        };

        if let Err(task) = Self::enqueue_column_load_task(task) {
            crate::utils::logging::log_error(
                "sql_editor::intellisense::column_loader",
                &format!(
                    "failed to enqueue column loader task for {}",
                    task.table_key
                ),
            );
            let _ = task.sender.send(ColumnLoadUpdate {
                table: task.table_key,
                columns: Vec::new(),
                cache_columns: false,
            });
            app::awake();
        }
    }

    fn table_lookup_key_candidates(table_name: &str) -> Vec<String> {
        let segments = Self::relation_name_segments(table_name);
        let normalized = segments.join(".");
        if normalized.is_empty() {
            return Vec::new();
        }

        let mut candidates = vec![normalized.clone()];
        if Self::has_unquoted_dot(table_name) {
            if let Some(last) = segments.last() {
                if !last.eq_ignore_ascii_case(&normalized) && !last.trim().is_empty() {
                    candidates.push(last.trim().to_string());
                }
            }
        }

        candidates
    }

    fn relation_name_segments(value: &str) -> Vec<String> {
        let mut parts = Vec::new();
        let mut current = String::new();
        let mut chars = value.trim().chars().peekable();
        let mut in_quotes = false;

        while let Some(ch) = chars.next() {
            match ch {
                '"' => {
                    current.push(ch);
                    if in_quotes {
                        if chars.peek().copied() == Some('"') {
                            current.push('"');
                            chars.next();
                        } else {
                            in_quotes = false;
                        }
                    } else {
                        in_quotes = true;
                    }
                }
                '.' if !in_quotes => {
                    let segment = Self::strip_identifier_quotes(current.trim());
                    if !segment.is_empty() {
                        parts.push(segment);
                    }
                    current.clear();
                }
                _ => current.push(ch),
            }
        }

        let segment = Self::strip_identifier_quotes(current.trim());
        if !segment.is_empty() {
            parts.push(segment);
        }

        parts
    }

    fn has_unquoted_dot(value: &str) -> bool {
        let mut chars = value.trim().chars().peekable();
        let mut in_quotes = false;
        while let Some(ch) = chars.next() {
            match ch {
                '"' => {
                    if in_quotes {
                        if chars.peek().copied() == Some('"') {
                            chars.next();
                        } else {
                            in_quotes = false;
                        }
                    } else {
                        in_quotes = true;
                    }
                }
                '.' if !in_quotes => return true,
                _ => {}
            }
        }
        false
    }


}
