use fltk::{
    app,
    button::Button,
    draw::set_cursor,
    enums::{Align, CallbackTrigger, Cursor, FrameType},
    frame::Frame,
    group::{Flex, FlexType},
    input::Input,
    prelude::*,
};
use mysql::prelude::Queryable;
use mysql::Error as MysqlError;
use oracle::{Connection, Error as OracleError, ErrorKind as OracleErrorKind};
use std::collections::VecDeque;
use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::db::{
    lock_connection_with_activity, BindValue, BindVar, ColumnInfo, CursorResult, DbPoolSession,
    DbSessionLease, QueryExecutor, QueryResult, ScriptItem, SessionState, SharedDbSessionLease,
    ToolCommand,
};
use crate::sql_text;
use crate::utils::arithmetic::{safe_div, safe_rem};

use super::*;

#[derive(Default)]
struct SelectTransformState {
    break_index: Option<usize>,
    previous_break_value: Option<String>,
    compute_of_index: Option<usize>,
    compute_on_index: Option<usize>,
    compute_group_value: Option<String>,
    compute_count: usize,
    compute_sum: f64,
    compute_sum_seen: bool,
    compute_sums: Vec<f64>,
    compute_seen_numeric: Vec<bool>,
}

// Flush streamed rows in bounded batches so very large fetches still surface
// progressive UI updates without waiting for oversized buffers.
// Send buffered rows when either:
// - first batch reaches 10,000 rows
// - 200ms passes
// - an additional batch reaches 10,000 rows
pub(super) const PROGRESS_ROWS_INITIAL_BATCH: usize = 10_000;
const PROGRESS_ROWS_FLUSH_INTERVAL: Duration = Duration::from_millis(200);
const PROGRESS_ROWS_MAX_BATCH: usize = 10_000;
const MAX_SCRIPT_INCLUDE_DEPTH: usize = 64;
const SESSION_POOL_CANCEL_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const SESSION_POOL_CANCEL_RESPONSE_TIMEOUT: Duration = Duration::from_millis(750);
const SESSION_POOL_CANCEL_RETRY_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone)]
struct ScriptExecutionFrame {
    items: Vec<ScriptItem>,
    index: usize,
    base_dir: PathBuf,
    source_path: Option<PathBuf>,
}

struct ResolvedScriptInclude {
    source_path: PathBuf,
    script_dir: PathBuf,
    items: Vec<ScriptItem>,
}

#[derive(Debug, Clone)]
enum PrintNamedData {
    Scalar(Option<String>),
    Cursor(CursorResult),
    CursorEmpty,
    Missing,
}

#[derive(Clone, Copy)]
struct ExecutionStartupPolicy {
    has_connect_command: bool,
    requires_connected_session: bool,
}

#[derive(Clone, Copy, Default)]
pub(super) struct MySqlSessionStateHint {
    clears_session_state: bool,
    may_leave_session_bound_state: bool,
}

struct QueryExecutionCleanupGuard {
    sender: mpsc::Sender<QueryProgress>,
    current_query_connection: Arc<Mutex<Option<Arc<Connection>>>>,
    current_mysql_cancel_context: Arc<Mutex<Option<MySqlQueryCancelContext>>>,
    cancel_flag: Arc<Mutex<bool>>,
    query_running: Arc<Mutex<bool>>,
    timeout_connection: Option<Arc<Connection>>,
    previous_timeout: Option<Duration>,
    oracle_pooled_session: Option<(SharedDbSessionLease, u64, Arc<Connection>)>,
    oracle_pooled_session_invalidated: bool,
    oracle_read_only_transaction: Option<Arc<Connection>>,
}

impl QueryExecutionCleanupGuard {
    fn new(
        sender: mpsc::Sender<QueryProgress>,
        current_query_connection: Arc<Mutex<Option<Arc<Connection>>>>,
        current_mysql_cancel_context: Arc<Mutex<Option<MySqlQueryCancelContext>>>,
        cancel_flag: Arc<Mutex<bool>>,
        query_running: Arc<Mutex<bool>>,
    ) -> Self {
        Self {
            sender,
            current_query_connection,
            current_mysql_cancel_context,
            cancel_flag,
            query_running,
            timeout_connection: None,
            previous_timeout: None,
            oracle_pooled_session: None,
            oracle_pooled_session_invalidated: false,
            oracle_read_only_transaction: None,
        }
    }

    fn track_timeout(&mut self, connection: Arc<Connection>, previous_timeout: Option<Duration>) {
        self.timeout_connection = Some(connection);
        self.previous_timeout = previous_timeout;
    }

    fn track_oracle_pooled_session(
        &mut self,
        pooled_db_session: SharedDbSessionLease,
        connection_generation: u64,
        connection: Arc<Connection>,
    ) {
        self.oracle_pooled_session = Some((pooled_db_session, connection_generation, connection));
    }

    fn invalidate_oracle_pooled_session(&mut self) {
        self.oracle_pooled_session_invalidated = true;
    }

    fn clear_oracle_pooled_session_tracking(&mut self) {
        self.oracle_pooled_session = None;
        self.oracle_pooled_session_invalidated = false;
    }

    fn clear_timeout_tracking(&mut self) {
        self.timeout_connection = None;
        self.previous_timeout = None;
    }

    fn track_oracle_read_only_transaction(&mut self, connection: Arc<Connection>) {
        self.oracle_read_only_transaction = Some(connection);
    }

    fn clear_oracle_read_only_transaction_tracking(&mut self) {
        self.oracle_read_only_transaction = None;
    }
}

impl Drop for QueryExecutionCleanupGuard {
    fn drop(&mut self) {
        let mut oracle_read_only_close_failed = false;
        if let Some(conn) = self.oracle_read_only_transaction.as_ref() {
            if let Err(err) = conn.rollback() {
                oracle_read_only_close_failed = true;
                crate::utils::logging::log_error(
                    "sql_editor::cleanup",
                    &format!("Failed to close Oracle read-only transaction: {err}"),
                );
            }
        }

        // Restore per-session driver state before the editor is marked idle.
        // Otherwise a fast follow-up execution can reuse the same pooled Oracle
        // session while this guard is still resetting its call timeout.
        let mut oracle_timeout_reset_failed = false;
        if let Some(conn) = self.timeout_connection.as_ref() {
            if let Err(err) = conn.set_call_timeout(self.previous_timeout) {
                oracle_timeout_reset_failed = true;
                crate::utils::logging::log_error(
                    "sql_editor::cleanup",
                    &format!("Failed to reset Oracle call timeout: {err}"),
                );
            }
        }

        let should_invalidate_oracle_session = self.oracle_pooled_session_invalidated
            || load_mutex_bool(&self.cancel_flag)
            || oracle_timeout_reset_failed
            || oracle_read_only_close_failed
            || std::thread::panicking();
        if let Some((pooled_db_session, connection_generation, conn)) =
            self.oracle_pooled_session.as_ref()
        {
            if should_invalidate_oracle_session {
                crate::db::clear_oracle_pooled_session_lease_if_current_connection(
                    pooled_db_session,
                    *connection_generation,
                    conn,
                );
            } else {
                let may_have_uncommitted_work =
                    SqlEditorWidget::oracle_session_may_have_uncommitted_work(
                        conn.as_ref(),
                        "sql_editor::cleanup",
                    );
                crate::db::store_pooled_session_lease_if_empty(
                    pooled_db_session,
                    *connection_generation,
                    DbSessionLease::Oracle(Arc::clone(conn)),
                    may_have_uncommitted_work,
                );
            }
        }

        SqlEditorWidget::set_current_query_connection(&self.current_query_connection, None);
        SqlEditorWidget::set_current_mysql_cancel_context(&self.current_mysql_cancel_context, None);
        store_mutex_bool(&self.cancel_flag, false);
        // Keep execution state fail-safe even if the UI progress poller has
        // stopped (e.g. tab closed while worker thread is still unwinding).
        store_mutex_bool(&self.query_running, false);
        let _ = self.sender.send(QueryProgress::BatchFinished);
        app::awake();
    }
}

struct LazyFetchAllTimeout {
    timeout: Option<Duration>,
    started_at: Option<Instant>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LazyFetchWorkerOutcome {
    Completed,
    Cancelled,
}

struct MySqlLazyFetchTimeoutCancelGuard {
    stop_sender: Option<mpsc::Sender<()>>,
    fired: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
}

impl MySqlLazyFetchTimeoutCancelGuard {
    fn fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }

    fn finish(&self) {
        self.finished.store(true, Ordering::SeqCst);
    }
}

impl Drop for MySqlLazyFetchTimeoutCancelGuard {
    fn drop(&mut self) {
        self.finish();
        if let Some(stop_sender) = self.stop_sender.take() {
            let _ = stop_sender.send(());
        }
    }
}

impl LazyFetchAllTimeout {
    fn new(timeout: Option<Duration>) -> Self {
        Self {
            timeout,
            started_at: None,
        }
    }

    fn note_row_received(&mut self) {
        if self.timeout.is_some() && self.started_at.is_none() {
            self.started_at = Some(Instant::now());
        }
    }

    fn timed_out(&self) -> bool {
        match (self.timeout, self.started_at) {
            (Some(timeout), Some(started_at)) => started_at.elapsed() >= timeout,
            _ => false,
        }
    }

    fn remaining_after_start(&self) -> Option<Duration> {
        let timeout = self.timeout?;
        let started_at = self.started_at?;
        Some(
            timeout
                .checked_sub(started_at.elapsed())
                .unwrap_or(Duration::ZERO),
        )
    }
}

struct QueryRunningReservation {
    query_running: Arc<Mutex<bool>>,
    armed: bool,
}

impl QueryRunningReservation {
    fn acquire(query_running: Arc<Mutex<bool>>) -> Option<Self> {
        if try_mark_query_running(&query_running) {
            Some(Self {
                query_running,
                armed: true,
            })
        } else {
            None
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for QueryRunningReservation {
    fn drop(&mut self) {
        if self.armed {
            store_mutex_bool(&self.query_running, false);
        }
    }
}

impl SqlEditorWidget {
    fn connection_info_for_ui(info: &ConnectionInfo) -> ConnectionInfo {
        let mut sanitized = info.clone();
        sanitized.clear_password();
        sanitized
    }

    fn sync_mysql_connection_info_for_ui(
        shared_connection: &crate::db::SharedConnection,
        db_activity: &str,
    ) -> Option<ConnectionInfo> {
        let mut conn_guard =
            lock_connection_with_activity(shared_connection, db_activity.to_string());
        if conn_guard.db_type().execution_engine() != crate::db::DbExecutionEngine::MySql {
            return None;
        }

        match conn_guard.sync_mysql_current_database_name() {
            Ok(_) => Some(Self::connection_info_for_ui(conn_guard.get_info())),
            Err(err) => {
                eprintln!("Warning: failed to sync MySQL current database metadata: {err}");
                None
            }
        }
    }

    fn db_activity_label_for_sql(sql: &str, script_mode: bool) -> String {
        let compact = sql.split_whitespace().collect::<Vec<_>>().join(" ");
        let preview = if compact.is_empty() {
            "<empty>".to_string()
        } else {
            compact.chars().take(72).collect::<String>()
        };
        let preview = if compact.chars().count() > 72 {
            format!("{}...", preview)
        } else {
            preview
        };
        if script_mode {
            format!("Executing script: {}", preview)
        } else {
            format!("Executing SQL: {}", preview)
        }
    }

    fn should_use_lazy_fetch_for_single_statement(items: &[ScriptItem]) -> bool {
        items.len() == 1 && matches!(items[0], ScriptItem::Statement(_))
    }

    pub fn execute_sql_text(&self, sql: &str) {
        self.execute_sql(sql, false);
    }

    pub fn focus(&mut self) {
        self.group.show();
        let _ = self.editor.take_focus();
    }

    pub fn execute_current(&self) {
        let buffer = self.buffer.clone();
        let sql = buffer.text();

        if let Some((start, end)) = buffer.selection_position() {
            let (start, end) = if start <= end {
                (start, end)
            } else {
                (end, start)
            };

            if start != end {
                let selected_text = buffer.selection_text();
                if !selected_text.is_empty() {
                    // F5 runs script execution semantics even when only a range is selected.
                    self.execute_sql_with_mysql_delimiter(
                        &selected_text,
                        true,
                        self.mysql_delimiter_before_offset(start as usize),
                    );
                    return;
                }
            }
        }

        self.execute_sql(&sql, true);
    }

    pub fn execute_statement_at_cursor(&self) {
        // Check if there's a selection
        let selected_text = self.buffer.selection_text();
        if !selected_text.is_empty() {
            // Execute selected text
            let selection_start = self
                .buffer
                .selection_position()
                .map(|(start, end)| start.min(end) as usize)
                .unwrap_or(0);
            self.execute_sql_with_mysql_delimiter(
                &selected_text,
                false,
                self.mysql_delimiter_before_offset(selection_start),
            );
        } else {
            // Execute statement at cursor position
            if let Some(statement) = self.statement_at_cursor_text() {
                let normalized = self.normalize_statement_for_single_execution(&statement);
                self.execute_sql(&normalized, false);
            } else {
                SqlEditorWidget::show_alert_dialog("No SQL at cursor");
            }
        }
    }

    pub fn execute_selected(&self) {
        let mut buffer = self.buffer.clone();
        if !buffer.selected() {
            SqlEditorWidget::show_alert_dialog("No SQL selected");
            return;
        }

        let selection = buffer.selection_position();
        let insert_pos = self.editor.insert_position();
        let sql = buffer.selection_text();
        let selection_start = selection
            .map(|(start, end)| start.min(end) as usize)
            .unwrap_or(0);
        self.execute_sql_with_mysql_delimiter(
            &sql,
            false,
            self.mysql_delimiter_before_offset(selection_start),
        );
        if let Some((start, end)) = selection {
            buffer.select(start, end);
            let mut editor = self.editor.clone();
            editor.set_insert_position(insert_pos);
            editor.show_insert_position();
        }
    }

    pub fn format_selected_sql(&self) {
        let mut buffer = self.buffer.clone();
        let selection = buffer.selection_position();
        let preferred_db_type = Some(self.current_db_type());
        if let Some((start, end)) = selection {
            if start != end {
                let buffer_len = buffer.length().max(0);
                let (start, end) = if start <= end {
                    (start, end)
                } else {
                    (end, start)
                };
                let start = start.clamp(0, buffer_len);
                let end = end.clamp(start, buffer_len);
                let source = text_buffer_access::text_range(
                    &buffer,
                    Some(&self.highlight_shadow),
                    start,
                    end,
                );
                let formatted =
                    Self::format_for_auto_formatting_with_db_type(&source, true, preferred_db_type);
                if formatted == source {
                    return;
                }

                let start_usize = start as usize;
                let deleted_len = end.saturating_sub(start) as usize;
                let mut editor = self.editor.clone();
                let original_pos = editor.insert_position().clamp(0, buffer_len) as usize;
                let selection_end =
                    start_usize + Self::clamp_to_char_boundary(&formatted, formatted.len());
                let mapped_cursor = selection_end;

                let _suppress_callbacks = self.suppress_buffer_callbacks();
                buffer.replace(start, end, &formatted);
                self.invalidate_intellisense_after_buffer_edit();
                self.handle_buffer_highlight_update_with_known_inserted_text(
                    &buffer,
                    start,
                    formatted.len().min(i32::MAX as usize) as i32,
                    deleted_len.min(i32::MAX as usize) as i32,
                    &formatted,
                    &source,
                );
                self.record_programmatic_buffer_edit(
                    start_usize,
                    &source,
                    &formatted,
                    original_pos,
                    mapped_cursor,
                );

                buffer.select(start, selection_end.min(i32::MAX as usize) as i32);
                editor.set_insert_position(mapped_cursor.min(i32::MAX as usize) as i32);
                editor.show_insert_position();
                return;
            }
        }

        let full_text = buffer.text();
        let formatted =
            Self::format_for_auto_formatting_with_db_type(&full_text, false, preferred_db_type);
        if formatted == full_text {
            return;
        }

        let mut editor = self.editor.clone();
        let original_pos = Self::normalize_index(&full_text, editor.insert_position());
        let mapped_cursor = Self::clamp_to_char_boundary(&formatted, formatted.len());

        let _suppress_callbacks = self.suppress_buffer_callbacks();
        buffer.set_text(&formatted);
        self.invalidate_intellisense_after_buffer_edit();
        self.handle_buffer_highlight_update_with_known_inserted_text(
            &buffer,
            0,
            formatted.len().min(i32::MAX as usize) as i32,
            full_text.len().min(i32::MAX as usize) as i32,
            &formatted,
            &full_text,
        );
        self.record_full_buffer_programmatic_replace(
            full_text,
            formatted,
            original_pos,
            mapped_cursor,
        );
        editor.set_insert_position(mapped_cursor.min(i32::MAX as usize) as i32);
        editor.show_insert_position();
    }

    pub fn toggle_comment(&self) {
        let mut buffer = self.buffer.clone();
        let mut editor = self.editor.clone();
        let selection = buffer.selection_position();
        let had_selection = matches!(selection, Some((start, end)) if start != end);
        let original_pos = editor.insert_position();

        let (start, end) = if let Some((start, end)) = selection {
            if start <= end {
                (start, end)
            } else {
                (end, start)
            }
        } else {
            let line_start =
                text_buffer_access::line_start(&buffer, Some(&self.highlight_shadow), original_pos);
            let line_end =
                text_buffer_access::line_end(&buffer, Some(&self.highlight_shadow), original_pos);
            (line_start, line_end)
        };

        let line_start =
            text_buffer_access::line_start(&buffer, Some(&self.highlight_shadow), start);
        let line_end = text_buffer_access::line_end(&buffer, Some(&self.highlight_shadow), end);
        let text = text_buffer_access::text_range(
            &buffer,
            Some(&self.highlight_shadow),
            line_start,
            line_end,
        );
        let ends_with_newline = text.ends_with('\n');
        let lines: Vec<&str> = text.lines().collect();

        let all_commented = lines
            .iter()
            .filter(|line| !line.trim().is_empty())
            .all(|line| line.trim_start().starts_with("--"));

        let mut new_lines: Vec<String> = Vec::with_capacity(lines.len());
        for line in lines {
            if line.trim().is_empty() {
                new_lines.push(line.to_string());
                continue;
            }

            let prefix_len = line.len() - line.trim_start().len();
            let prefix = &line[..prefix_len];
            let trimmed = &line[prefix_len..];

            if all_commented {
                let uncommented = trimmed.strip_prefix("--").unwrap_or(trimmed);
                let uncommented = uncommented.strip_prefix(' ').unwrap_or(uncommented);
                new_lines.push(format!("{}{}", prefix, uncommented));
            } else if trimmed.starts_with("--") {
                new_lines.push(line.to_string());
            } else {
                new_lines.push(format!("{}-- {}", prefix, trimmed));
            }
        }

        let mut new_text = new_lines.join("\n");
        if ends_with_newline {
            new_text.push('\n');
        }

        buffer.replace(line_start, line_end, &new_text);
        let new_end = line_start + new_text.len() as i32;
        if had_selection {
            buffer.select(line_start, new_end);
            editor.set_insert_position(new_end);
        } else {
            let delta = new_text.len() as i32 - (line_end - line_start);
            let new_pos = if original_pos >= line_start {
                original_pos + delta
            } else {
                original_pos
            };
            editor.set_insert_position(new_pos);
        }
        editor.show_insert_position();
    }

    pub fn convert_selection_case(&self, to_upper: bool) {
        let mut buffer = self.buffer.clone();
        let selection = buffer.selection_position();
        let (start, end) = match selection {
            Some((start, end)) if start != end => {
                if start <= end {
                    (start, end)
                } else {
                    (end, start)
                }
            }
            _ => {
                SqlEditorWidget::show_alert_dialog("No SQL selected");
                return;
            }
        };

        let selected = buffer.selection_text();
        let converted = if to_upper {
            selected.to_uppercase()
        } else {
            selected.to_lowercase()
        };

        if converted == selected {
            return;
        }

        buffer.replace(start, end, &converted);
        buffer.select(start, start + converted.len() as i32);

        let mut editor = self.editor.clone();
        editor.set_insert_position(start + converted.len() as i32);
        editor.show_insert_position();
    }

    fn normalize_script_include_path(path: &Path) -> PathBuf {
        fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
    }

    fn validate_script_include_target(
        frames: &[ScriptExecutionFrame],
        target_path: &Path,
    ) -> Result<(), String> {
        let nested_depth = frames
            .iter()
            .filter(|frame| frame.source_path.is_some())
            .count();
        if nested_depth >= MAX_SCRIPT_INCLUDE_DEPTH {
            return Err(format!(
                "Maximum nested script depth ({MAX_SCRIPT_INCLUDE_DEPTH}) exceeded."
            ));
        }

        if frames.iter().any(|frame| {
            frame
                .source_path
                .as_ref()
                .is_some_and(|path| path.as_path() == target_path)
        }) {
            return Err(format!(
                "Recursive script include detected: {}",
                target_path.display()
            ));
        }

        Ok(())
    }

    fn resolve_script_include_path(
        path: &str,
        relative_to_caller: bool,
        caller_base_dir: &Path,
        working_dir: &Path,
    ) -> (PathBuf, PathBuf) {
        let base_dir = if relative_to_caller {
            caller_base_dir
        } else {
            working_dir
        };
        let target_path = if Path::new(path).is_absolute() {
            PathBuf::from(path)
        } else {
            base_dir.join(path)
        };
        let normalized_target_path = Self::normalize_script_include_path(&target_path);

        (target_path, normalized_target_path)
    }

    fn load_script_include(
        target_path: &Path,
        normalized_target_path: &Path,
        base_dir: &Path,
        preferred_db_type: Option<crate::db::connection::DatabaseType>,
        initial_mysql_delimiter: Option<&str>,
    ) -> Result<ResolvedScriptInclude, String> {
        let contents = fs::read_to_string(target_path)
            .map_err(|err| format!("Failed to read script {}: {}", target_path.display(), err))?;

        let script_dir = normalized_target_path
            .parent()
            .unwrap_or(base_dir)
            .to_path_buf();

        Ok(ResolvedScriptInclude {
            source_path: normalized_target_path.to_path_buf(),
            script_dir,
            items: super::query_text::split_script_items_for_db_type_with_mysql_delimiter(
                &contents,
                preferred_db_type,
                initial_mysql_delimiter,
            ),
        })
    }

    fn requires_connected_session_for_precheck(
        has_connection_bootstrap_command: bool,
        can_run_while_disconnected: bool,
    ) -> bool {
        !has_connection_bootstrap_command && !can_run_while_disconnected
    }

    fn execution_startup_policy(sql: &str) -> ExecutionStartupPolicy {
        let has_connect_command = super::query_text::has_connection_bootstrap_command(sql);
        let can_run_while_disconnected = super::query_text::can_execute_while_disconnected(sql);
        let requires_connected_session = Self::requires_connected_session_for_precheck(
            has_connect_command,
            can_run_while_disconnected,
        );

        ExecutionStartupPolicy {
            has_connect_command,
            requires_connected_session,
        }
    }

    fn acquire_execution_connection(
        conn_guard: &mut crate::db::ConnectionLockGuard<'_>,
        sender: &mpsc::Sender<QueryProgress>,
        has_connect_command: bool,
    ) -> Result<Option<Arc<Connection>>, String> {
        if has_connect_command {
            if conn_guard.is_connected() {
                match conn_guard.require_live_connection() {
                    Ok(conn) => Ok(Some(conn)),
                    Err(_) => {
                        let _ = sender.send(QueryProgress::ConnectionChanged { info: None });
                        app::awake();
                        Ok(None)
                    }
                }
            } else {
                Ok(None)
            }
        } else {
            match conn_guard.require_live_connection() {
                Ok(conn) => Ok(Some(conn)),
                Err(message) => {
                    if !conn_guard.is_connected() || !conn_guard.has_connection_handle() {
                        let _ = sender.send(QueryProgress::ConnectionChanged { info: None });
                        app::awake();
                    }
                    Err(message)
                }
            }
        }
    }

    fn session_pool_error_is_exhausted(message: &str) -> bool {
        let lower = message.to_ascii_lowercase();
        lower.contains("ora-24418")
            || lower.contains("ora-24496")
            || lower.contains("ocisessionget() timed out")
            || lower.contains("ocisessionget timed out")
            || lower.contains("session pool appears exhausted")
            || lower.contains("connection pool appears exhausted")
            || (lower.contains("drivererror") && lower.contains("operation timed out"))
            || lower.contains("waiting for a free connection")
    }

    fn request_cancel_oldest_lazy_fetch_for_session_pool(
        sender: &mpsc::Sender<QueryProgress>,
    ) -> bool {
        let (response_sender, response_receiver) = mpsc::channel();
        if sender
            .send(QueryProgress::RequestCancelOldestLazyFetchForSessionPool {
                response: response_sender,
            })
            .is_err()
        {
            return false;
        }
        app::awake();
        match response_receiver.recv_timeout(SESSION_POOL_CANCEL_RESPONSE_TIMEOUT) {
            Ok(cancel_requested) => cancel_requested,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                crate::utils::logging::log_warning(
                    "session pool",
                    "Timed out waiting for lazy fetch cancel response; retrying pool acquire while UI handles the request",
                );
                true
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => false,
        }
    }

    // Fire-and-forget cancel notification; used on the Oracle acquire path
    // where waiting synchronously would extend the connection-mutex holding
    // window.
    fn notify_cancel_oldest_lazy_fetch_for_session_pool(
        sender: &mpsc::Sender<QueryProgress>,
    ) -> bool {
        let sent = sender
            .send(QueryProgress::NotifyCancelOldestLazyFetchForSessionPool)
            .is_ok();
        if sent {
            app::awake();
        }
        sent
    }

    // Retry pool acquisition against a cloned pool handle. Taking the pool by
    // reference (not the connection guard) means the retry loop does not depend
    // on holding the connection mutex — callers can release the mutex across
    // the cooperative sleep between attempts so UI threads, metadata
    // refreshes, and lazy-fetch finalizers stay unblocked while we are only
    // waiting for a pool slot to free up.
    fn retry_oracle_pool_session_after_lazy_cancel(
        pool: &crate::db::DbConnectionPool,
    ) -> Result<DbPoolSession, String> {
        Self::retry_pool_session_after_lazy_cancel(pool)
    }

    fn retry_mysql_pool_session_after_lazy_cancel(
        pool: &crate::db::DbConnectionPool,
    ) -> Result<DbPoolSession, String> {
        Self::retry_pool_session_after_lazy_cancel(pool)
    }

    fn retry_pool_session_after_lazy_cancel(
        pool: &crate::db::DbConnectionPool,
    ) -> Result<DbPoolSession, String> {
        let started_at = Instant::now();
        loop {
            match pool.acquire_session() {
                Ok(session) => return Ok(session),
                Err(message)
                    if Self::session_pool_error_is_exhausted(&message)
                        && started_at.elapsed() < SESSION_POOL_CANCEL_WAIT_TIMEOUT =>
                {
                    thread::sleep(SESSION_POOL_CANCEL_RETRY_INTERVAL);
                }
                Err(message) => return Err(message),
            }
        }
    }

    fn acquire_fresh_oracle_pool_session_once(
        pool: &crate::db::DbConnectionPool,
        sender: &mpsc::Sender<QueryProgress>,
    ) -> Result<DbPoolSession, String> {
        match pool.acquire_session() {
            Ok(session @ DbPoolSession::Oracle(_)) => Ok(session),
            Ok(other) => Err(format!(
                "Expected Oracle pool session but acquired {}",
                other.db_type()
            )),
            Err(message)
                if Self::session_pool_error_is_exhausted(&message)
                    && Self::notify_cancel_oldest_lazy_fetch_for_session_pool(sender) =>
            {
                match Self::retry_oracle_pool_session_after_lazy_cancel(pool)? {
                    session @ DbPoolSession::Oracle(_) => Ok(session),
                    other => Err(format!(
                        "Expected Oracle pool session but acquired {}",
                        other.db_type()
                    )),
                }
            }
            Err(message) => Err(message),
        }
    }

    fn acquire_fresh_oracle_pool_session(
        pool: &crate::db::DbConnectionPool,
        sender: &mpsc::Sender<QueryProgress>,
    ) -> Result<DbPoolSession, String> {
        match Self::acquire_fresh_oracle_pool_session_once(pool, sender) {
            Ok(session) => Ok(session),
            Err(message) if Self::oracle_pool_acquire_error_should_retry_fresh(&message) => {
                crate::utils::logging::log_warning(
                    "oracle pool session",
                    &format!(
                        "Oracle pool session acquire failed with a stale-session error; retrying once: {message}"
                    ),
                );
                Self::acquire_fresh_oracle_pool_session_once(pool, sender)
            }
            Err(message) => Err(message),
        }
    }

    fn oracle_pool_acquire_error_should_retry_fresh(message: &str) -> bool {
        !Self::session_pool_error_is_exhausted(message)
            && !Self::oracle_error_message_allows_session_reuse(message)
    }

    fn mysql_pool_acquire_error_should_retry_fresh(message: &str) -> bool {
        !Self::session_pool_error_is_exhausted(message)
            && !Self::mysql_error_allows_session_reuse(message)
    }

    fn acquire_fresh_mysql_pool_session_once(
        context: &crate::db::DbPoolSessionContext,
        session_pool_sender: Option<&mpsc::Sender<QueryProgress>>,
    ) -> Result<mysql::PooledConn, String> {
        let pool_session = match context.pool.acquire_session() {
            Ok(session) => session,
            Err(message)
                if Self::session_pool_error_is_exhausted(&message)
                    && session_pool_sender
                        .is_some_and(Self::request_cancel_oldest_lazy_fetch_for_session_pool) =>
            {
                Self::retry_mysql_pool_session_after_lazy_cancel(&context.pool)?
            }
            Err(message) => return Err(message),
        };
        match pool_session {
            DbPoolSession::MySQL(conn) => Ok(conn),
            other => Err(format!(
                "Expected MySQL pool session but acquired {}",
                other.db_type()
            )),
        }
    }

    fn acquire_fresh_mysql_pool_session(
        context: &crate::db::DbPoolSessionContext,
        session_pool_sender: Option<&mpsc::Sender<QueryProgress>>,
    ) -> Result<mysql::PooledConn, String> {
        match Self::acquire_fresh_mysql_pool_session_once(context, session_pool_sender) {
            Ok(conn) => Ok(conn),
            Err(message) if Self::mysql_pool_acquire_error_should_retry_fresh(&message) => {
                crate::utils::logging::log_warning(
                    "mysql pool session",
                    &format!(
                        "Discarding stale MySQL pooled session after acquire failure and retrying once: {message}"
                    ),
                );
                Self::acquire_fresh_mysql_pool_session_once(context, session_pool_sender)
            }
            Err(message) => Err(message),
        }
    }

    fn mysql_missing_current_database_error(err: &MysqlError) -> bool {
        matches!(err, MysqlError::MySqlError(server_err) if server_err.code == 1049)
            || err
                .to_string()
                .to_ascii_lowercase()
                .contains("unknown database")
    }

    fn reset_mysql_pooled_session_to_no_database(
        conn: &mut mysql::PooledConn,
        advanced: &crate::db::ConnectionAdvancedSettings,
    ) -> Result<(), String> {
        conn.as_mut()
            .reset()
            .map_err(|err| SqlEditorWidget::mysql_error_message(&err, None))?;
        crate::db::DatabaseConnection::apply_mysql_session_settings(conn, advanced)
    }

    fn prepare_mysql_pooled_session_database(
        conn: &mut mysql::PooledConn,
        current_service_name: &str,
        advanced: &crate::db::ConnectionAdvancedSettings,
    ) -> Result<(), String> {
        let database = current_service_name.trim();
        if database.is_empty() {
            return Ok(());
        }

        // Re-select even when SELECT DATABASE() would report the same name.
        // A database can be dropped while an idle pooled session still carries
        // that name as its default schema; COM_INIT_DB validates it before use.
        match conn.as_mut().select_db(database) {
            Ok(()) => {
                crate::db::DatabaseConnection::apply_mysql_connection_encoding_with_settings(
                    conn, advanced,
                )?;
                Ok(())
            }
            Err(err) if Self::mysql_missing_current_database_error(&err) => {
                crate::utils::logging::log_error(
                    "mysql pool session",
                    &format!(
                        "Current database `{database}` is not available; continuing without a default database"
                    ),
                );
                Self::reset_mysql_pooled_session_to_no_database(conn, advanced)
            }
            Err(err) => Err(SqlEditorWidget::mysql_error_message(&err, None)),
        }
    }

    // Acquires the Oracle connection for the upcoming execution. The caller
    // passes the connection guard by value so this function can release the
    // connection mutex during the cooperative pool-retry sleep when the pool
    // is exhausted. The guard (re-acquired before returning) is threaded back
    // to the caller so follow-up operations can continue to use it.
    fn acquire_oracle_pooled_execution_connection<'a>(
        mut conn_guard: crate::db::ConnectionLockGuard<'a>,
        shared_connection: &'a crate::db::SharedConnection,
        db_activity: &str,
        sender: &mpsc::Sender<QueryProgress>,
        has_connect_command: bool,
        pooled_db_session: &SharedDbSessionLease,
    ) -> (
        crate::db::ConnectionLockGuard<'a>,
        Result<Option<(Arc<Connection>, bool)>, String>,
    ) {
        if has_connect_command {
            crate::db::clear_pooled_session_lease(pooled_db_session);
            let result = Self::acquire_execution_connection(&mut conn_guard, sender, true)
                .map(|conn| conn.map(|conn| (conn, false)));
            return (conn_guard, result);
        }

        if !conn_guard.is_connected() || !conn_guard.has_connection_handle() {
            crate::db::clear_pooled_session_lease(pooled_db_session);
            let result = Self::acquire_execution_connection(&mut conn_guard, sender, false)
                .map(|conn| conn.map(|conn| (conn, false)));
            return (conn_guard, result);
        }

        let connection_generation = conn_guard.connection_generation();
        if let Some((lease, prior_may_have_uncommitted_work)) =
            crate::db::take_reusable_pooled_session_lease_with_state(
                pooled_db_session,
                connection_generation,
                crate::db::DatabaseType::Oracle,
            )
        {
            let Some(conn) = lease.oracle_connection() else {
                return (conn_guard, Err("Expected Oracle pool session".to_string()));
            };
            let mut reuse_error = None;
            if conn.ping().is_ok() {
                let setup_result = if prior_may_have_uncommitted_work {
                    Ok(())
                } else {
                    crate::db::DatabaseConnection::apply_oracle_session_settings(
                        conn.as_ref(),
                        &conn_guard.get_info().advanced,
                    )
                    .and_then(|_| conn_guard.apply_tracked_oracle_current_schema(conn.as_ref()))
                };
                match setup_result {
                    Ok(()) => {
                        return (
                            conn_guard,
                            Ok(Some((conn, prior_may_have_uncommitted_work))),
                        )
                    }
                    Err(message) => reuse_error = Some(message),
                }
            }
            drop(lease);
            if let Some(message) = reuse_error {
                if Self::oracle_pool_acquire_error_should_retry_fresh(&message) {
                    crate::utils::logging::log_warning(
                        "oracle pool session",
                        &format!(
                            "Discarding stale Oracle pooled session after setup failure and retrying with a fresh session: {message}"
                        ),
                    );
                } else {
                    return (conn_guard, Err(message));
                }
            }
        }

        let Some(pool) = conn_guard.get_pool() else {
            return (
                conn_guard,
                Err(crate::db::NOT_CONNECTED_MESSAGE.to_string()),
            );
        };
        drop(conn_guard);
        let pool_session_result = Self::acquire_fresh_oracle_pool_session(&pool, sender);
        conn_guard = lock_connection_with_activity(shared_connection, db_activity.to_string());
        let pool_session = match pool_session_result {
            Ok(session) => Some(session),
            Err(message) => return (conn_guard, Err(message)),
        };

        match pool_session {
            Some(session @ DbPoolSession::Oracle(_)) => {
                let mut session = session;
                let mut retried_current_schema = false;
                loop {
                    // While the mutex was released the user could have triggered
                    // a reconnect or disconnect; verify the generation is still
                    // current before keeping this session.
                    if !conn_guard.can_reuse_pool_session(
                        connection_generation,
                        crate::db::DatabaseType::Oracle,
                    ) {
                        drop(session);
                        return (
                            conn_guard,
                            Err(crate::db::NOT_CONNECTED_MESSAGE.to_string()),
                        );
                    }
                    let lease = session.into_lease();
                    let conn = match lease.oracle_connection() {
                        Some(conn) => conn,
                        None => {
                            return (conn_guard, Err("Expected Oracle pool session".to_string()))
                        }
                    };
                    match conn_guard.apply_tracked_oracle_current_schema(conn.as_ref()) {
                        Ok(()) => {
                            return (conn_guard, Ok(Some((conn, false))));
                        }
                        Err(message)
                            if !retried_current_schema
                                && Self::oracle_pool_acquire_error_should_retry_fresh(&message) =>
                        {
                            let Some(pool) = conn_guard.get_pool() else {
                                return (
                                    conn_guard,
                                    Err(crate::db::NOT_CONNECTED_MESSAGE.to_string()),
                                );
                            };
                            crate::utils::logging::log_warning(
                                "oracle pool session",
                                &format!(
                                    "Oracle pooled session setup failed with a stale-session error; retrying once: {message}"
                                ),
                            );
                            drop(conn);
                            drop(lease);
                            drop(conn_guard);
                            let retry_result =
                                Self::acquire_fresh_oracle_pool_session(&pool, sender);
                            conn_guard = lock_connection_with_activity(
                                shared_connection,
                                db_activity.to_string(),
                            );
                            match retry_result {
                                Ok(retry_session @ DbPoolSession::Oracle(_)) => {
                                    session = retry_session;
                                    retried_current_schema = true;
                                    continue;
                                }
                                Ok(other) => {
                                    return (
                                        conn_guard,
                                        Err(format!(
                                            "Expected Oracle pool session but acquired {}",
                                            other.db_type()
                                        )),
                                    );
                                }
                                Err(message) => return (conn_guard, Err(message)),
                            }
                        }
                        Err(message) => return (conn_guard, Err(message)),
                    }
                }
            }
            _ => {
                let result = Self::acquire_execution_connection(&mut conn_guard, sender, false)
                    .map(|conn| conn.map(|conn| (conn, false)));
                (conn_guard, result)
            }
        }
    }

    fn register_lazy_fetch_handle(
        active_lazy_fetch: &Arc<Mutex<Option<LazyFetchHandle>>>,
        session_id: u64,
        sender: mpsc::Sender<LazyFetchCommand>,
        cancel_handle: Option<LazyFetchCancelHandle>,
    ) {
        *active_lazy_fetch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(LazyFetchHandle {
            session_id,
            sender,
            cancel_handle,
            cancel_requested: Arc::new(AtomicBool::new(false)),
        });
    }

    fn clear_lazy_fetch_handle(
        active_lazy_fetch: &Arc<Mutex<Option<LazyFetchHandle>>>,
        session_id: u64,
    ) {
        let mut guard = active_lazy_fetch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if guard
            .as_ref()
            .is_some_and(|handle| handle.session_id == session_id)
        {
            *guard = None;
        }
    }

    fn lazy_fetch_handle_matches(
        active_lazy_fetch: &Arc<Mutex<Option<LazyFetchHandle>>>,
        session_id: u64,
    ) -> bool {
        active_lazy_fetch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .is_some_and(|handle| handle.session_id == session_id)
    }

    fn lazy_fetch_cancel_requested(
        active_lazy_fetch: &Arc<Mutex<Option<LazyFetchHandle>>>,
        session_id: u64,
    ) -> bool {
        active_lazy_fetch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .is_some_and(|handle| {
                handle.session_id == session_id && handle.cancel_requested.load(Ordering::Relaxed)
            })
    }

    fn lazy_fetch_can_keep_session(
        active_lazy_fetch: &Arc<Mutex<Option<LazyFetchHandle>>>,
        session_id: u64,
    ) -> bool {
        active_lazy_fetch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .is_some_and(|handle| {
                handle.session_id == session_id && !handle.cancel_requested.load(Ordering::Relaxed)
            })
    }

    fn fetch_lazy_oracle_rows(
        result_set: &mut oracle::ResultSet<'static, oracle::Row>,
        column_count: usize,
        limit: usize,
        null_text: &str,
        fetched_rows: &mut usize,
        last_select_row: &mut Option<Vec<String>>,
    ) -> Result<(Vec<Vec<String>>, bool), OracleError> {
        let (rows, eof, _) = Self::fetch_lazy_oracle_rows_with_timeout(
            result_set,
            column_count,
            limit,
            null_text,
            fetched_rows,
            last_select_row,
            None,
            None,
        )?;
        Ok((rows, eof))
    }

    fn fetch_lazy_oracle_rows_with_timeout(
        result_set: &mut oracle::ResultSet<'static, oracle::Row>,
        column_count: usize,
        limit: usize,
        null_text: &str,
        fetched_rows: &mut usize,
        last_select_row: &mut Option<Vec<String>>,
        mut fetch_all_timeout: Option<&mut LazyFetchAllTimeout>,
        timeout_conn: Option<&Connection>,
    ) -> Result<(Vec<Vec<String>>, bool, bool), OracleError> {
        let mut rows = Vec::new();
        for _ in 0..limit {
            if let Some(timeout) = fetch_all_timeout.as_deref() {
                if timeout.timed_out() {
                    return Ok((rows, false, true));
                }
            }
            if let (Some(timeout), Some(conn)) = (fetch_all_timeout.as_deref(), timeout_conn) {
                if let Some(remaining) = timeout.remaining_after_start() {
                    if remaining.is_zero() {
                        return Ok((rows, false, true));
                    }
                    conn.set_call_timeout(Some(remaining))?;
                }
            }
            let Some(row_result) = result_set.next() else {
                return Ok((rows, true, false));
            };
            let row = row_result?;
            let mut row_data = Vec::with_capacity(column_count);
            for i in 0..column_count {
                row_data.push(QueryExecutor::row_value_to_text(&row, i)?);
            }
            *last_select_row = Some(row_data.clone());
            SqlEditorWidget::apply_null_text_to_row(&mut row_data, null_text);
            rows.push(row_data);
            *fetched_rows = fetched_rows.saturating_add(1);
            if let Some(timeout) = fetch_all_timeout.as_deref_mut() {
                timeout.note_row_received();
            }
        }
        let timed_out = fetch_all_timeout
            .as_deref()
            .map(|timeout| timeout.timed_out())
            .unwrap_or(false);
        Ok((rows, false, timed_out))
    }

    fn emit_lazy_rows(sender: &mpsc::Sender<QueryProgress>, index: usize, rows: Vec<Vec<String>>) {
        if !rows.is_empty() {
            let _ = sender.send(QueryProgress::Rows { index, rows });
            app::awake();
        }
    }

    fn drain_lazy_cancel_request(
        receiver: &mpsc::Receiver<LazyFetchCommand>,
        pending_commands: &mut VecDeque<LazyFetchCommand>,
    ) -> bool {
        loop {
            match receiver.try_recv() {
                Ok(LazyFetchCommand::Cancel) => return true,
                Ok(command) => pending_commands.push_back(command),
                Err(mpsc::TryRecvError::Empty) => return false,
                Err(mpsc::TryRecvError::Disconnected) => return true,
            }
        }
    }

    fn next_lazy_fetch_command(
        receiver: &mpsc::Receiver<LazyFetchCommand>,
        pending_commands: &mut VecDeque<LazyFetchCommand>,
    ) -> Result<LazyFetchCommand, mpsc::RecvError> {
        if let Some(command) = pending_commands.pop_front() {
            Ok(command)
        } else {
            receiver.recv()
        }
    }

    fn emit_lazy_closed_result(
        sender: &mpsc::Sender<QueryProgress>,
        index: usize,
        session_id: u64,
        cancelled: bool,
    ) {
        let _ = sender.send(QueryProgress::LazyFetchClosed {
            index,
            session_id,
            cancelled,
        });
        app::awake();
    }

    fn emit_lazy_fetch_timeout_statement_result(
        sender: &mpsc::Sender<QueryProgress>,
        index: usize,
        sql: &str,
        column_info: &[ColumnInfo],
        fetched_rows: usize,
        execution_time: Duration,
        heading_enabled: bool,
        session: &Arc<Mutex<SessionState>>,
        raw_column_names: &[String],
        last_select_row: Option<&[String]>,
        conn_name: &str,
        query_timeout: Option<Duration>,
    ) {
        let mut query_result = QueryResult::new_select_streamed(
            sql,
            column_info.to_vec(),
            fetched_rows,
            execution_time,
        );
        SqlEditorWidget::apply_heading_to_result(&mut query_result, heading_enabled);
        query_result.success = false;
        query_result.message = SqlEditorWidget::timeout_message(query_timeout);
        SqlEditorWidget::append_spool_output(session, std::slice::from_ref(&query_result.message));
        SqlEditorWidget::apply_column_new_value_from_row(
            session,
            raw_column_names,
            last_select_row,
        );
        let _ = sender.send(QueryProgress::StatementFinished {
            index,
            result: query_result,
            connection_name: conn_name.to_string(),
            timed_out: true,
        });
        app::awake();
    }

    fn emit_lazy_waiting(sender: &mpsc::Sender<QueryProgress>, index: usize, session_id: u64) {
        let _ = sender.send(QueryProgress::LazyFetchWaiting { index, session_id });
        app::awake();
    }

    fn emit_lazy_worker_panicked(
        sender: &mpsc::Sender<QueryProgress>,
        index: usize,
        session_id: u64,
        panic_payload: &(dyn std::any::Any + Send),
    ) {
        let message = SqlEditorWidget::panic_payload_to_string(panic_payload);
        crate::utils::logging::log_error(
            "sql_editor::lazy_fetch",
            &format!("Lazy fetch worker thread panicked: {message}"),
        );
        let _ = sender.send(QueryProgress::LazyFetchClosed {
            index,
            session_id,
            cancelled: true,
        });
        let _ = sender.send(QueryProgress::WorkerPanicked {
            message: format!("Lazy fetch thread panicked: {message}"),
        });
        app::awake();
    }

    fn clear_lazy_fetch_after_worker_panic(
        sender: &mpsc::Sender<QueryProgress>,
        active_lazy_fetch: &Arc<Mutex<Option<LazyFetchHandle>>>,
        index: usize,
        session_id: u64,
        panic_payload: &(dyn std::any::Any + Send),
    ) {
        Self::clear_lazy_fetch_handle(active_lazy_fetch, session_id);
        Self::emit_lazy_worker_panicked(sender, index, session_id, panic_payload);
    }

    fn start_oracle_lazy_select(
        conn: Arc<Connection>,
        pooled_db_session: SharedDbSessionLease,
        connection_generation: u64,
        sender: mpsc::Sender<QueryProgress>,
        session: Arc<Mutex<SessionState>>,
        conn_name: String,
        index: usize,
        sql_to_execute: String,
        binds: Vec<crate::db::ResolvedBind>,
        heading_enabled: bool,
        feedback_enabled: bool,
        colsep: String,
        null_text: String,
        active_lazy_fetch: Arc<Mutex<Option<LazyFetchHandle>>>,
        session_id: u64,
        query_timeout: Option<Duration>,
        previous_timeout: Option<Duration>,
        close_read_only_transaction: bool,
    ) -> Result<(), OracleError> {
        let (command_sender, command_receiver) = mpsc::channel::<LazyFetchCommand>();
        Self::register_lazy_fetch_handle(
            &active_lazy_fetch,
            session_id,
            command_sender,
            Some(LazyFetchCancelHandle::Oracle(Arc::clone(&conn))),
        );
        let _ = sender.send(QueryProgress::LazyFetchSession { index, session_id });
        app::awake();
        let cleanup_sender = sender.clone();
        let cleanup_active_lazy_fetch = active_lazy_fetch.clone();
        let cleanup_pooled_db_session = pooled_db_session.clone();
        let cleanup_conn = Arc::clone(&conn);
        let spawn_result = thread::Builder::new()
            .name("oracle-lazy-fetch".to_string())
            .spawn(move || {
                let worker_result = panic::catch_unwind(AssertUnwindSafe(|| {
                    let statement_start = Instant::now();
                    let mut fetched_rows = 0usize;
                    let mut last_select_row: Option<Vec<String>> = None;
                    let mut raw_column_names: Vec<String> = Vec::new();
                    let mut keep_session = false;
                    let mut pending_commands = VecDeque::new();
                    let lazy_fetch_timeout = Self::lazy_fetch_query_timeout(query_timeout);
                    let result = (|| -> Result<LazyFetchWorkerOutcome, OracleError> {
                        conn.set_call_timeout(lazy_fetch_timeout)?;
                        if Self::drain_lazy_cancel_request(&command_receiver, &mut pending_commands)
                        {
                            return Ok(LazyFetchWorkerOutcome::Cancelled);
                        }
                        let (mut result_set, column_info) =
                            QueryExecutor::open_select_lazy_cursor_with_binds(
                                conn.as_ref(),
                                &sql_to_execute,
                                &binds,
                            )?;
                        raw_column_names = column_info
                            .iter()
                            .map(|column| column.name.clone())
                            .collect();
                        let display_columns = SqlEditorWidget::apply_heading_setting(
                            raw_column_names.clone(),
                            heading_enabled,
                        );
                        let _ = sender.send(QueryProgress::SelectStart {
                            index,
                            columns: display_columns.clone(),
                            null_text: null_text.clone(),
                        });
                        app::awake();
                        if !display_columns.is_empty() {
                            SqlEditorWidget::append_spool_output(
                                &session,
                                &[display_columns.join(&colsep)],
                            );
                        }
                        if Self::drain_lazy_cancel_request(&command_receiver, &mut pending_commands)
                        {
                            return Ok(LazyFetchWorkerOutcome::Cancelled);
                        }
                        let column_count = column_info.len();
                        let (rows, eof) = Self::fetch_lazy_oracle_rows(
                            &mut result_set,
                            column_count,
                            PROGRESS_ROWS_INITIAL_BATCH,
                            &null_text,
                            &mut fetched_rows,
                            &mut last_select_row,
                        )?;
                        SqlEditorWidget::append_spool_rows(&session, &rows);
                        Self::emit_lazy_rows(&sender, index, rows);
                        if Self::drain_lazy_cancel_request(&command_receiver, &mut pending_commands)
                        {
                            return Ok(LazyFetchWorkerOutcome::Cancelled);
                        }
                        if eof {
                            let mut query_result = QueryResult::new_select_streamed(
                                &sql_to_execute,
                                column_info.clone(),
                                fetched_rows,
                                statement_start.elapsed(),
                            );
                            SqlEditorWidget::apply_heading_to_result(
                                &mut query_result,
                                heading_enabled,
                            );
                            if !feedback_enabled {
                                query_result.message.clear();
                            }
                            if !query_result.message.trim().is_empty() {
                                SqlEditorWidget::append_spool_output(
                                    &session,
                                    std::slice::from_ref(&query_result.message),
                                );
                            }
                            SqlEditorWidget::apply_column_new_value_from_row(
                                &session,
                                &raw_column_names,
                                last_select_row.as_deref(),
                            );
                            keep_session = true;
                            let _ = sender.send(QueryProgress::StatementFinished {
                                index,
                                result: query_result,
                                connection_name: conn_name.clone(),
                                timed_out: false,
                            });
                            app::awake();
                            return Ok(LazyFetchWorkerOutcome::Completed);
                        }
                        Self::emit_lazy_waiting(&sender, index, session_id);
                        loop {
                            match Self::next_lazy_fetch_command(
                                &command_receiver,
                                &mut pending_commands,
                            ) {
                                Ok(LazyFetchCommand::FetchMore(limit)) => {
                                    let (rows, eof) = Self::fetch_lazy_oracle_rows(
                                        &mut result_set,
                                        column_count,
                                        limit,
                                        &null_text,
                                        &mut fetched_rows,
                                        &mut last_select_row,
                                    )?;
                                    SqlEditorWidget::append_spool_rows(&session, &rows);
                                    Self::emit_lazy_rows(&sender, index, rows);
                                    if Self::drain_lazy_cancel_request(
                                        &command_receiver,
                                        &mut pending_commands,
                                    ) {
                                        return Ok(LazyFetchWorkerOutcome::Cancelled);
                                    }
                                    if eof {
                                        let mut query_result = QueryResult::new_select_streamed(
                                            &sql_to_execute,
                                            column_info.clone(),
                                            fetched_rows,
                                            statement_start.elapsed(),
                                        );
                                        SqlEditorWidget::apply_heading_to_result(
                                            &mut query_result,
                                            heading_enabled,
                                        );
                                        if !feedback_enabled {
                                            query_result.message.clear();
                                        }
                                        if !query_result.message.trim().is_empty() {
                                            SqlEditorWidget::append_spool_output(
                                                &session,
                                                std::slice::from_ref(&query_result.message),
                                            );
                                        }
                                        SqlEditorWidget::apply_column_new_value_from_row(
                                            &session,
                                            &raw_column_names,
                                            last_select_row.as_deref(),
                                        );
                                        keep_session = true;
                                        let _ = sender.send(QueryProgress::StatementFinished {
                                            index,
                                            result: query_result,
                                            connection_name: conn_name.clone(),
                                            timed_out: false,
                                        });
                                        app::awake();
                                        return Ok(LazyFetchWorkerOutcome::Completed);
                                    }
                                    Self::emit_lazy_waiting(&sender, index, session_id);
                                }
                                Ok(LazyFetchCommand::FetchAll) => {
                                    let mut fetch_all_timeout =
                                        LazyFetchAllTimeout::new(query_timeout);
                                    loop {
                                        let (rows, eof, timed_out) =
                                            Self::fetch_lazy_oracle_rows_with_timeout(
                                                &mut result_set,
                                                column_count,
                                                PROGRESS_ROWS_MAX_BATCH,
                                                &null_text,
                                                &mut fetched_rows,
                                                &mut last_select_row,
                                                Some(&mut fetch_all_timeout),
                                                Some(conn.as_ref()),
                                            )?;
                                        SqlEditorWidget::append_spool_rows(&session, &rows);
                                        Self::emit_lazy_rows(&sender, index, rows);
                                        if Self::drain_lazy_cancel_request(
                                            &command_receiver,
                                            &mut pending_commands,
                                        ) {
                                            return Ok(LazyFetchWorkerOutcome::Cancelled);
                                        }
                                        if timed_out {
                                            let _ = conn.break_execution();
                                            Self::emit_lazy_fetch_timeout_statement_result(
                                                &sender,
                                                index,
                                                &sql_to_execute,
                                                &column_info,
                                                fetched_rows,
                                                statement_start.elapsed(),
                                                heading_enabled,
                                                &session,
                                                &raw_column_names,
                                                last_select_row.as_deref(),
                                                &conn_name,
                                                query_timeout,
                                            );
                                            return Ok(LazyFetchWorkerOutcome::Cancelled);
                                        }
                                        if eof {
                                            let mut query_result = QueryResult::new_select_streamed(
                                                &sql_to_execute,
                                                column_info.clone(),
                                                fetched_rows,
                                                statement_start.elapsed(),
                                            );
                                            SqlEditorWidget::apply_heading_to_result(
                                                &mut query_result,
                                                heading_enabled,
                                            );
                                            if !feedback_enabled {
                                                query_result.message.clear();
                                            }
                                            if !query_result.message.trim().is_empty() {
                                                SqlEditorWidget::append_spool_output(
                                                    &session,
                                                    std::slice::from_ref(&query_result.message),
                                                );
                                            }
                                            SqlEditorWidget::apply_column_new_value_from_row(
                                                &session,
                                                &raw_column_names,
                                                last_select_row.as_deref(),
                                            );
                                            keep_session = true;
                                            let _ = sender.send(QueryProgress::StatementFinished {
                                                index,
                                                result: query_result,
                                                connection_name: conn_name.clone(),
                                                timed_out: false,
                                            });
                                            app::awake();
                                            return Ok(LazyFetchWorkerOutcome::Completed);
                                        }
                                    }
                                }
                                Ok(LazyFetchCommand::Cancel) | Err(_) => {
                                    return Ok(LazyFetchWorkerOutcome::Cancelled);
                                }
                            }
                        }
                    })();
                    // Defer the close event until the fetch closure returns so ResultSet/Statement
                    // drops and cleanup runs before the UI treats the lazy fetch as closed.
                    let mut close_cancelled = false;
                    let mut error_result = None;
                    match result {
                        Ok(LazyFetchWorkerOutcome::Completed) => {}
                        Ok(LazyFetchWorkerOutcome::Cancelled) => {
                            close_cancelled = true;
                        }
                        Err(err) => {
                            close_cancelled = true;
                            if Self::oracle_error_allows_session_reuse(&err) {
                                keep_session = true;
                            }
                            let suppress_error_result =
                                !Self::lazy_fetch_handle_matches(&active_lazy_fetch, session_id)
                                    || Self::lazy_fetch_cancel_requested(
                                        &active_lazy_fetch,
                                        session_id,
                                    );
                            if !suppress_error_result {
                                let mut query_result =
                                    QueryResult::new_error(&sql_to_execute, &err.to_string());
                                query_result.is_select = true;
                                error_result = Some(QueryProgress::StatementFinished {
                                    index,
                                    result: query_result,
                                    connection_name: conn_name.clone(),
                                    timed_out: false,
                                });
                            }
                        }
                    }
                    let timeout_reset_ok = match conn.set_call_timeout(previous_timeout) {
                        Ok(_) => true,
                        Err(err) => {
                            crate::utils::logging::log_error(
                                "oracle lazy fetch cleanup",
                                &format!("Failed to reset Oracle lazy fetch call timeout: {err}"),
                            );
                            false
                        }
                    };
                    let read_only_close_ok = if close_read_only_transaction {
                        match conn.rollback() {
                            Ok(()) => true,
                            Err(err) => {
                                crate::utils::logging::log_error(
                                    "oracle lazy fetch cleanup",
                                    &format!("Failed to close Oracle read-only transaction: {err}"),
                                );
                                false
                            }
                        }
                    } else {
                        true
                    };
                    let should_keep_session = keep_session
                        && timeout_reset_ok
                        && read_only_close_ok
                        && Self::lazy_fetch_can_keep_session(&active_lazy_fetch, session_id);
                    if should_keep_session {
                        let may_have_uncommitted_work =
                            Self::oracle_session_may_have_uncommitted_work(
                                conn.as_ref(),
                                "oracle lazy fetch cleanup",
                            );
                        crate::db::store_pooled_session_lease_if_empty(
                            &pooled_db_session,
                            connection_generation,
                            DbSessionLease::Oracle(Arc::clone(&conn)),
                            may_have_uncommitted_work,
                        );
                    } else {
                        crate::db::clear_oracle_pooled_session_lease_if_current_connection(
                            &pooled_db_session,
                            connection_generation,
                            &conn,
                        );
                    }
                    Self::clear_lazy_fetch_handle(&active_lazy_fetch, session_id);
                    if let Some(error_result) = error_result {
                        let _ = sender.send(error_result);
                        app::awake();
                    }
                    Self::emit_lazy_closed_result(&sender, index, session_id, close_cancelled);
                }));
                if let Err(payload) = worker_result {
                    let _ = conn.set_call_timeout(previous_timeout);
                    crate::db::clear_oracle_pooled_session_lease_if_current_connection(
                        &pooled_db_session,
                        connection_generation,
                        &conn,
                    );
                    Self::clear_lazy_fetch_after_worker_panic(
                        &sender,
                        &active_lazy_fetch,
                        index,
                        session_id,
                        payload.as_ref(),
                    );
                }
            });
        if let Err(err) = spawn_result {
            let message = format!("Failed to start Oracle lazy fetch worker: {err}");
            crate::utils::logging::log_error("oracle lazy fetch", &message);
            crate::db::clear_oracle_pooled_session_lease_if_current_connection(
                &cleanup_pooled_db_session,
                connection_generation,
                &cleanup_conn,
            );
            Self::clear_lazy_fetch_handle(&cleanup_active_lazy_fetch, session_id);
            Self::emit_lazy_closed_result(&cleanup_sender, index, session_id, true);
            return Err(OracleError::new(OracleErrorKind::InternalError, message));
        }
        Ok(())
    }

    fn start_mysql_lazy_select(
        connection_generation: u64,
        shared_connection: crate::db::SharedConnection,
        conn: mysql::PooledConn,
        connection_info: ConnectionInfo,
        pooled_db_session: SharedDbSessionLease,
        sender: mpsc::Sender<QueryProgress>,
        session: Arc<Mutex<SessionState>>,
        conn_name: String,
        index: usize,
        sql_to_execute: String,
        heading_enabled: bool,
        feedback_enabled: bool,
        colsep: String,
        null_text: String,
        auto_commit: bool,
        prior_may_have_uncommitted_work: bool,
        state_hint: MySqlSessionStateHint,
        query_timeout: Option<Duration>,
        active_lazy_fetch: Arc<Mutex<Option<LazyFetchHandle>>>,
        session_id: u64,
    ) {
        let (command_sender, command_receiver) = mpsc::channel::<LazyFetchCommand>();
        let lazy_cancel_context: Arc<Mutex<Option<MySqlQueryCancelContext>>> =
            Arc::new(Mutex::new(None));
        Self::register_lazy_fetch_handle(
            &active_lazy_fetch,
            session_id,
            command_sender,
            Some(LazyFetchCancelHandle::MySql(lazy_cancel_context.clone())),
        );
        let _ = sender.send(QueryProgress::LazyFetchSession { index, session_id });
        app::awake();
        let cleanup_sender = sender.clone();
        let cleanup_active_lazy_fetch = active_lazy_fetch.clone();
        let cleanup_sql_to_execute = sql_to_execute.clone();
        let cleanup_conn_name = conn_name.clone();
        let spawn_result = thread::Builder::new()
            .name("mysql-lazy-fetch".to_string())
            .spawn(move || {
                let mut conn = Some(conn);
                let mut should_release_session = false;
                let mut may_have_uncommitted_work = false;
                let mut close_cancelled = false;
                let mut error_result = None;
                let worker_result = panic::catch_unwind(AssertUnwindSafe(|| {
                    let Some(conn) = conn.as_mut() else {
                        return;
                    };
                    let statement_start = Instant::now();
                    let mut fetched_rows = 0usize;
                    let mut last_select_row: Option<Vec<String>> = None;
                    let mut keep_session = false;
                    let mut pending_commands = VecDeque::new();
                    let lazy_fetch_timeout = Self::lazy_fetch_query_timeout(query_timeout);
                    *lazy_cancel_context
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                        Some(MySqlQueryCancelContext {
                            connection_info: connection_info.clone(),
                            connection_id: conn.connection_id(),
                        });
                    let result = (|| -> Result<LazyFetchWorkerOutcome, String> {
                        if !prior_may_have_uncommitted_work {
                            conn.query_drop(if auto_commit {
                                "SET autocommit=1"
                            } else {
                                "SET autocommit=0"
                            })
                            .map_err(|err| {
                                SqlEditorWidget::mysql_error_message(&err, lazy_fetch_timeout)
                            })?;
                        }
                        crate::db::query::mysql_executor::MysqlExecutor::apply_session_timeout(
                            conn,
                            lazy_fetch_timeout,
                        )
                        .map_err(|err| {
                            SqlEditorWidget::mysql_error_message(&err, lazy_fetch_timeout)
                        })?;
                        if Self::drain_lazy_cancel_request(&command_receiver, &mut pending_commands)
                        {
                            Self::cancel_mysql_lazy_fetch_query(
                                &lazy_cancel_context,
                                "mysql lazy fetch cancel",
                            );
                            return Ok(LazyFetchWorkerOutcome::Cancelled);
                        }
                        let mut result =
                            conn.query_iter(sql_to_execute.as_str()).map_err(|err| {
                                SqlEditorWidget::mysql_error_message(&err, lazy_fetch_timeout)
                            })?;
                        let column_info: Vec<ColumnInfo> = result
                            .columns()
                            .as_ref()
                            .iter()
                            .map(|col| ColumnInfo {
                                name: col.name_str().to_string(),
                                data_type: format!("{:?}", col.column_type()),
                            })
                            .collect();
                        let raw_column_names = column_info
                            .iter()
                            .map(|column| column.name.clone())
                            .collect::<Vec<String>>();
                        let display_columns = SqlEditorWidget::apply_heading_setting(
                            raw_column_names.clone(),
                            heading_enabled,
                        );
                        let _ = sender.send(QueryProgress::SelectStart {
                            index,
                            columns: display_columns.clone(),
                            null_text: null_text.clone(),
                        });
                        app::awake();
                        if !display_columns.is_empty() {
                            SqlEditorWidget::append_spool_output(
                                &session,
                                &[display_columns.join(&colsep)],
                            );
                        }
                        if Self::drain_lazy_cancel_request(&command_receiver, &mut pending_commands)
                        {
                            Self::cancel_mysql_lazy_fetch_query(
                                &lazy_cancel_context,
                                "mysql lazy fetch cancel",
                            );
                            return Ok(LazyFetchWorkerOutcome::Cancelled);
                        }
                        let column_count = column_info.len();
                        macro_rules! fetch_rows {
                        ($limit:expr) => {{
                            let mut rows = Vec::new();
                            let mut eof = false;
                            for _ in 0..$limit {
                                let Some(row_result) = result.next() else {
                                    eof = true;
                                    break;
                                };
                                let row: mysql::Row = row_result.map_err(|err| {
                                    SqlEditorWidget::mysql_error_message(&err, lazy_fetch_timeout)
                                })?;
                                let mut row_data =
                                    crate::db::query::mysql_executor::MysqlExecutor::row_to_strings(
                                        &row,
                                        column_count,
                                    );
                                last_select_row = Some(row_data.clone());
                                SqlEditorWidget::apply_null_text_to_row(&mut row_data, &null_text);
                                rows.push(row_data);
                                fetched_rows = fetched_rows.saturating_add(1);
                            }
                            (rows, eof)
                        }};
                    }
                        macro_rules! fetch_rows_with_timeout {
                        ($limit:expr, $fetch_all_timeout:expr, $timeout_cancel:expr) => {{
                            let mut rows = Vec::new();
                            let mut eof = false;
                            let mut timed_out = false;
                            for _ in 0..$limit {
                                if $fetch_all_timeout.timed_out()
                                    || $timeout_cancel
                                        .as_ref()
                                        .is_some_and(|guard| guard.fired())
                                {
                                    timed_out = true;
                                    break;
                                }
                                let Some(row_result) = result.next() else {
                                    eof = true;
                                    break;
                                };
                                let row: mysql::Row = row_result.map_err(|err| {
                                    if $timeout_cancel
                                        .as_ref()
                                        .is_some_and(|guard| guard.fired())
                                    {
                                        SqlEditorWidget::timeout_message(query_timeout)
                                    } else {
                                        SqlEditorWidget::mysql_error_message(
                                            &err,
                                            lazy_fetch_timeout,
                                        )
                                    }
                                })?;
                                let mut row_data =
                                    crate::db::query::mysql_executor::MysqlExecutor::row_to_strings(
                                        &row,
                                        column_count,
                                    );
                                last_select_row = Some(row_data.clone());
                                SqlEditorWidget::apply_null_text_to_row(&mut row_data, &null_text);
                                rows.push(row_data);
                                fetched_rows = fetched_rows.saturating_add(1);
                                $fetch_all_timeout.note_row_received();
                            }
                            if $fetch_all_timeout.timed_out()
                                || $timeout_cancel
                                    .as_ref()
                                    .is_some_and(|guard| guard.fired())
                            {
                                timed_out = true;
                            }
                            (rows, eof, timed_out)
                        }};
                    }
                        let (rows, eof) = fetch_rows!(PROGRESS_ROWS_INITIAL_BATCH);
                        SqlEditorWidget::append_spool_rows(&session, &rows);
                        Self::emit_lazy_rows(&sender, index, rows);
                        if Self::drain_lazy_cancel_request(&command_receiver, &mut pending_commands)
                        {
                            Self::cancel_mysql_lazy_fetch_query(
                                &lazy_cancel_context,
                                "mysql lazy fetch cancel",
                            );
                            return Ok(LazyFetchWorkerOutcome::Cancelled);
                        }
                        if eof {
                            let mut query_result = QueryResult::new_select_streamed(
                                &sql_to_execute,
                                column_info.clone(),
                                fetched_rows,
                                statement_start.elapsed(),
                            );
                            SqlEditorWidget::apply_heading_to_result(
                                &mut query_result,
                                heading_enabled,
                            );
                            if !feedback_enabled {
                                query_result.message.clear();
                            }
                            SqlEditorWidget::apply_column_new_value_from_row(
                                &session,
                                &raw_column_names,
                                last_select_row.as_deref(),
                            );
                            let _ = sender.send(QueryProgress::StatementFinished {
                                index,
                                result: query_result,
                                connection_name: conn_name.clone(),
                                timed_out: false,
                            });
                            keep_session = true;
                            app::awake();
                            return Ok(LazyFetchWorkerOutcome::Completed);
                        }
                        Self::emit_lazy_waiting(&sender, index, session_id);
                        loop {
                            match Self::next_lazy_fetch_command(
                                &command_receiver,
                                &mut pending_commands,
                            ) {
                                Ok(LazyFetchCommand::FetchMore(limit)) => {
                                    let (rows, eof) = fetch_rows!(limit);
                                    SqlEditorWidget::append_spool_rows(&session, &rows);
                                    Self::emit_lazy_rows(&sender, index, rows);
                                    if Self::drain_lazy_cancel_request(
                                        &command_receiver,
                                        &mut pending_commands,
                                    ) {
                                        Self::cancel_mysql_lazy_fetch_query(
                                            &lazy_cancel_context,
                                            "mysql lazy fetch cancel",
                                        );
                                        return Ok(LazyFetchWorkerOutcome::Cancelled);
                                    }
                                    if eof {
                                        let mut query_result = QueryResult::new_select_streamed(
                                            &sql_to_execute,
                                            column_info.clone(),
                                            fetched_rows,
                                            statement_start.elapsed(),
                                        );
                                        SqlEditorWidget::apply_heading_to_result(
                                            &mut query_result,
                                            heading_enabled,
                                        );
                                        if !feedback_enabled {
                                            query_result.message.clear();
                                        }
                                        SqlEditorWidget::apply_column_new_value_from_row(
                                            &session,
                                            &raw_column_names,
                                            last_select_row.as_deref(),
                                        );
                                        let _ = sender.send(QueryProgress::StatementFinished {
                                            index,
                                            result: query_result,
                                            connection_name: conn_name.clone(),
                                            timed_out: false,
                                        });
                                        keep_session = true;
                                        app::awake();
                                        return Ok(LazyFetchWorkerOutcome::Completed);
                                    }
                                    Self::emit_lazy_waiting(&sender, index, session_id);
                                }
                                Ok(LazyFetchCommand::FetchAll) => {
                                    let timeout_cancel =
                                        Self::start_mysql_lazy_fetch_timeout_cancel(
                                            query_timeout,
                                            lazy_cancel_context.clone(),
                                            "mysql lazy fetch timeout",
                                        );
                                    let mut fetch_all_timeout =
                                        LazyFetchAllTimeout::new(query_timeout);
                                    if fetched_rows > 0 {
                                        fetch_all_timeout.note_row_received();
                                    }
                                    loop {
                                        let (rows, eof, timed_out) = fetch_rows_with_timeout!(
                                            PROGRESS_ROWS_MAX_BATCH,
                                            fetch_all_timeout,
                                            timeout_cancel
                                        );
                                        SqlEditorWidget::append_spool_rows(&session, &rows);
                                        Self::emit_lazy_rows(&sender, index, rows);
                                        if Self::drain_lazy_cancel_request(
                                            &command_receiver,
                                            &mut pending_commands,
                                        ) {
                                            Self::cancel_mysql_lazy_fetch_query(
                                                &lazy_cancel_context,
                                                "mysql lazy fetch cancel",
                                            );
                                            return Ok(LazyFetchWorkerOutcome::Cancelled);
                                        }
                                        if timed_out {
                                            if !timeout_cancel
                                                .as_ref()
                                                .is_some_and(|guard| guard.fired())
                                            {
                                                Self::cancel_mysql_lazy_fetch_query(
                                                    &lazy_cancel_context,
                                                    "mysql lazy fetch timeout",
                                                );
                                            }
                                            Self::emit_lazy_fetch_timeout_statement_result(
                                                &sender,
                                                index,
                                                &sql_to_execute,
                                                &column_info,
                                                fetched_rows,
                                                statement_start.elapsed(),
                                                heading_enabled,
                                                &session,
                                                &raw_column_names,
                                                last_select_row.as_deref(),
                                                &conn_name,
                                                query_timeout,
                                            );
                                            return Ok(LazyFetchWorkerOutcome::Cancelled);
                                        }
                                        if eof {
                                            if let Some(timeout_cancel) = timeout_cancel.as_ref() {
                                                timeout_cancel.finish();
                                            }
                                            let mut query_result = QueryResult::new_select_streamed(
                                                &sql_to_execute,
                                                column_info.clone(),
                                                fetched_rows,
                                                statement_start.elapsed(),
                                            );
                                            SqlEditorWidget::apply_heading_to_result(
                                                &mut query_result,
                                                heading_enabled,
                                            );
                                            if !feedback_enabled {
                                                query_result.message.clear();
                                            }
                                            SqlEditorWidget::apply_column_new_value_from_row(
                                                &session,
                                                &raw_column_names,
                                                last_select_row.as_deref(),
                                            );
                                            let _ = sender.send(QueryProgress::StatementFinished {
                                                index,
                                                result: query_result,
                                                connection_name: conn_name.clone(),
                                                timed_out: false,
                                            });
                                            keep_session = true;
                                            app::awake();
                                            return Ok(LazyFetchWorkerOutcome::Completed);
                                        }
                                    }
                                }
                                Ok(LazyFetchCommand::Cancel) | Err(_) => {
                                    Self::cancel_mysql_lazy_fetch_query(
                                        &lazy_cancel_context,
                                        "mysql lazy fetch cancel",
                                    );
                                    return Ok(LazyFetchWorkerOutcome::Cancelled);
                                }
                            }
                        }
                    })();
                    // Defer the close event until the fetch closure returns so the MySQL result
                    // object drops and cleanup runs before the UI treats the lazy fetch as closed.
                    match result {
                        Ok(LazyFetchWorkerOutcome::Completed) => {}
                        Ok(LazyFetchWorkerOutcome::Cancelled) => {
                            close_cancelled = true;
                        }
                        Err(err) => {
                            close_cancelled = true;
                            let timed_out =
                                Self::timeout_error_message_contains_timeout_signal(&err);
                            if Self::mysql_error_allows_session_reuse(&err) {
                                keep_session = true;
                            }
                            let suppress_error_result =
                                !Self::lazy_fetch_handle_matches(&active_lazy_fetch, session_id)
                                    || Self::lazy_fetch_cancel_requested(
                                        &active_lazy_fetch,
                                        session_id,
                                    );
                            if !suppress_error_result {
                                let mut query_result =
                                    QueryResult::new_error(&sql_to_execute, &err);
                                query_result.is_select = true;
                                error_result = Some(QueryProgress::StatementFinished {
                                    index,
                                    result: query_result,
                                    connection_name: conn_name,
                                    timed_out,
                                });
                            }
                        }
                    }
                    should_release_session = keep_session
                        && Self::lazy_fetch_can_keep_session(&active_lazy_fetch, session_id);
                    if should_release_session {
                        if let Err(err) =
                            crate::db::query::mysql_executor::MysqlExecutor::apply_session_timeout(
                                conn, None,
                            )
                        {
                            crate::utils::logging::log_error(
                                "mysql lazy fetch cleanup",
                                &format!("Failed to reset MySQL lazy fetch session timeout: {err}"),
                            );
                            should_release_session = false;
                        }
                    }
                    if should_release_session {
                        should_release_session = Self::sync_mysql_pooled_session_info(
                            &shared_connection,
                            conn,
                            "mysql lazy fetch cleanup",
                            connection_generation,
                            false,
                        );
                    }
                    if should_release_session {
                        let fallback_on_error = if state_hint.clears_session_state {
                            state_hint.may_leave_session_bound_state
                        } else {
                            prior_may_have_uncommitted_work
                                || state_hint.may_leave_session_bound_state
                                || !auto_commit
                        };
                        may_have_uncommitted_work =
                            Self::mysql_pooled_session_may_need_preservation(
                                conn,
                                "mysql lazy fetch cleanup",
                                prior_may_have_uncommitted_work,
                                state_hint,
                                fallback_on_error,
                            );
                    }
                    *lazy_cancel_context
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
                }));
                match worker_result {
                    Ok(()) => {
                        if let Some(conn) = conn.take() {
                            if should_release_session
                                && Self::lazy_fetch_can_keep_session(&active_lazy_fetch, session_id)
                            {
                                Self::release_mysql_pooled_session_if_current(
                                    &shared_connection,
                                    &pooled_db_session,
                                    connection_generation,
                                    conn,
                                    may_have_uncommitted_work,
                                    "mysql lazy fetch cleanup",
                                );
                            } else {
                                drop(conn);
                            }
                        }
                        Self::clear_lazy_fetch_handle(&active_lazy_fetch, session_id);
                        if let Some(error_result) = error_result {
                            let _ = sender.send(error_result);
                            app::awake();
                        }
                        Self::emit_lazy_closed_result(&sender, index, session_id, close_cancelled);
                    }
                    Err(payload) => {
                        if let Some(mut conn) = conn.take() {
                            let _ =
                            crate::db::query::mysql_executor::MysqlExecutor::apply_session_timeout(
                                &mut conn, None,
                            );
                            drop(conn);
                        }
                        *lazy_cancel_context
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
                        Self::clear_lazy_fetch_after_worker_panic(
                            &sender,
                            &active_lazy_fetch,
                            index,
                            session_id,
                            payload.as_ref(),
                        );
                    }
                }
            });
        if let Err(err) = spawn_result {
            let message = format!("Failed to start MySQL lazy fetch worker: {err}");
            crate::utils::logging::log_error("mysql lazy fetch", &message);
            Self::clear_lazy_fetch_handle(&cleanup_active_lazy_fetch, session_id);
            let mut result = QueryResult::new_error(&cleanup_sql_to_execute, &message);
            result.is_select = true;
            let _ = cleanup_sender.send(QueryProgress::StatementFinished {
                index,
                result,
                connection_name: cleanup_conn_name,
                timed_out: false,
            });
            Self::emit_lazy_closed_result(&cleanup_sender, index, session_id, true);
        }
    }

    fn cancel_mysql_lazy_fetch_query(
        cancel_context: &Arc<Mutex<Option<MySqlQueryCancelContext>>>,
        log_context: &str,
    ) {
        let context = cancel_context
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(context) = context {
            if let Err(err) = crate::db::query::mysql_executor::MysqlExecutor::cancel_running_query(
                &context.connection_info,
                context.connection_id,
            ) {
                crate::utils::logging::log_error(
                    log_context,
                    &format!("Failed to cancel MySQL lazy fetch query: {err}"),
                );
            }
        }
    }

    fn start_mysql_lazy_fetch_timeout_cancel(
        timeout: Option<Duration>,
        cancel_context: Arc<Mutex<Option<MySqlQueryCancelContext>>>,
        log_context: &'static str,
    ) -> Option<MySqlLazyFetchTimeoutCancelGuard> {
        let timeout = timeout?;
        let (stop_sender, stop_receiver) = mpsc::channel();
        let fired = Arc::new(AtomicBool::new(false));
        let fired_for_thread = fired.clone();
        let finished = Arc::new(AtomicBool::new(false));
        let finished_for_thread = finished.clone();
        match thread::Builder::new()
            .name("mysql-lazy-fetch-timeout".to_string())
            .spawn(move || {
                if matches!(
                    stop_receiver.recv_timeout(timeout),
                    Err(mpsc::RecvTimeoutError::Timeout)
                ) {
                    if finished_for_thread.load(Ordering::SeqCst) {
                        return;
                    }
                    fired_for_thread.store(true, Ordering::SeqCst);
                    if !finished_for_thread.load(Ordering::SeqCst) {
                        SqlEditorWidget::cancel_mysql_lazy_fetch_query(
                            &cancel_context,
                            log_context,
                        );
                    }
                }
            }) {
            Ok(_) => Some(MySqlLazyFetchTimeoutCancelGuard {
                stop_sender: Some(stop_sender),
                fired,
                finished,
            }),
            Err(err) => {
                crate::utils::logging::log_error(
                    log_context,
                    &format!("Failed to start MySQL lazy fetch timeout watcher: {err}"),
                );
                None
            }
        }
    }

    fn emit_execution_startup_error(
        sender: &mpsc::Sender<QueryProgress>,
        script_mode: bool,
        sql_text: &str,
        conn_name: &str,
        message: &str,
        session: Option<&Arc<Mutex<SessionState>>>,
    ) {
        if script_mode {
            let result = QueryResult::new_error(sql_text, message);
            SqlEditorWidget::emit_script_result(sender, conn_name, 0, result, false);
            return;
        }

        if let Some(active_session) = session {
            SqlEditorWidget::append_spool_output(active_session, &[message.to_string()]);
        }

        let _ = sender.send(QueryProgress::StatementFinished {
            index: 0,
            result: QueryResult::new_error(sql_text, message),
            connection_name: conn_name.to_string(),
            timed_out: false,
        });
        app::awake();
    }

    fn mysql_result_requires_transaction_feedback(sql: &str, result: &QueryResult) -> bool {
        if result.is_select || !result.success || result.row_count == 0 {
            return false;
        }

        matches!(
            QueryExecutor::leading_keyword(sql.trim()).as_deref(),
            Some("INSERT")
                | Some("UPDATE")
                | Some("DELETE")
                | Some("REPLACE")
                | Some("WITH")
                | Some("CALL")
        )
    }

    fn apply_mysql_transaction_feedback(result: &mut QueryResult, sql: &str, auto_commit: bool) {
        if !Self::mysql_result_requires_transaction_feedback(sql, result) {
            return;
        }

        if auto_commit {
            result.message = format!("{} | Auto-commit applied", result.message);
        } else {
            result.message = format!("{} | Commit required", result.message);
        }
    }

    fn current_mysql_delimiter_from_session(session: &Arc<Mutex<SessionState>>) -> Option<String> {
        match session.lock() {
            Ok(guard) => guard.mysql_delimiter.clone(),
            Err(poisoned) => poisoned.into_inner().mysql_delimiter.clone(),
        }
    }

    fn build_mysql_batch_items(
        sql_text: &str,
        initial_mysql_delimiter: Option<&str>,
    ) -> Vec<ScriptItem> {
        super::query_text::split_script_items_for_db_type_with_mysql_delimiter(
            sql_text,
            Some(crate::db::connection::DatabaseType::MySQL),
            initial_mysql_delimiter,
        )
    }

    fn execute_mysql_batch(
        shared_connection: &crate::db::SharedConnection,
        sender: &mpsc::Sender<QueryProgress>,
        sql_text: &str,
        conn_name: &str,
        session: &Arc<Mutex<SessionState>>,
        pooled_db_session: &SharedDbSessionLease,
        active_lazy_fetch: &Arc<Mutex<Option<LazyFetchHandle>>>,
        next_lazy_fetch_session_id: &Arc<AtomicU64>,
        current_mysql_cancel_context: &Arc<Mutex<Option<MySqlQueryCancelContext>>>,
        cancel_flag: &Arc<Mutex<bool>>,
        script_mode: bool,
        initial_mysql_delimiter_override: Option<String>,
        query_timeout: Option<Duration>,
        initial_auto_commit: bool,
        db_activity: &str,
    ) {
        let items =
            Self::build_mysql_batch_items(sql_text, initial_mysql_delimiter_override.as_deref());
        if items.is_empty() {
            return;
        }
        let lazy_fetch_single_statement = Self::should_use_lazy_fetch_for_single_statement(&items);

        let _ = sender.send(QueryProgress::BatchStart {
            activity: db_activity.to_string(),
        });
        app::awake();

        let mut conn_name = conn_name.to_string();
        let mut result_index = 0usize;
        let mut auto_commit = initial_auto_commit;
        let mut continue_on_error = match session.lock() {
            Ok(guard) => guard.continue_on_error,
            Err(poisoned) => {
                eprintln!("Warning: session state lock was poisoned; recovering.");
                poisoned.into_inner().continue_on_error
            }
        };
        let mut stop_execution = false;
        let working_dir = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let mut frames = vec![ScriptExecutionFrame {
            items,
            index: 0,
            base_dir: working_dir.clone(),
            source_path: None,
        }];

        let execute_mysql_sql =
            |sql: &str, auto_commit: bool| -> Result<Vec<QueryResult>, String> {
                let refresh_encoding_after =
                    crate::db::query::mysql_executor::MysqlExecutor::is_use_statement(sql);
                let state_hint = SqlEditorWidget::mysql_session_state_hint_for_sql(sql);
                SqlEditorWidget::run_mysql_pooled_action_with_timeout(
                    shared_connection,
                    pooled_db_session,
                    Some(sender),
                    current_mysql_cancel_context,
                    cancel_flag,
                    query_timeout,
                    db_activity,
                    auto_commit,
                    refresh_encoding_after,
                    state_hint,
                    |mysql_conn| {
                        crate::db::query::mysql_executor::MysqlExecutor::execute(mysql_conn, sql)
                    },
                )
            };
        let mysql_interruption_flags = |message: &str| {
            (
                message == SqlEditorWidget::cancel_message(),
                message == SqlEditorWidget::timeout_message(query_timeout),
            )
        };

        while !frames.is_empty() {
            if stop_execution || load_mutex_bool(cancel_flag) {
                break;
            }

            let Some((item, current_frame_base_dir)) = ({
                let frame = match frames.last_mut() {
                    Some(frame) => frame,
                    None => break,
                };

                if frame.index >= frame.items.len() {
                    None
                } else {
                    let item = frame.items[frame.index].clone();
                    frame.index += 1;
                    Some((item, frame.base_dir.clone()))
                }
            }) else {
                frames.pop();
                continue;
            };

            let echo_enabled = match session.lock() {
                Ok(guard) => guard.echo_enabled,
                Err(poisoned) => {
                    eprintln!("Warning: session state lock was poisoned; recovering.");
                    poisoned.into_inner().echo_enabled
                }
            };
            if echo_enabled {
                let echo_line = match &item {
                    ScriptItem::Statement(statement) => statement.trim().to_string(),
                    ScriptItem::ToolCommand(command) => {
                        SqlEditorWidget::format_tool_command(command)
                    }
                };
                if !echo_line.trim().is_empty() {
                    SqlEditorWidget::emit_script_output(sender, session, vec![echo_line]);
                }
            }

            match item {
                ScriptItem::ToolCommand(command) => {
                    let mut command_error = false;
                    match command {
                        ToolCommand::Prompt { text } => {
                            SqlEditorWidget::emit_script_output(sender, session, vec![text]);
                        }
                        ToolCommand::Pause { message } => {
                            let prompt_text = message
                                .filter(|text| !text.trim().is_empty())
                                .unwrap_or_else(|| "Press ENTER to continue.".to_string());
                            SqlEditorWidget::emit_script_message(
                                sender,
                                session,
                                "PAUSE",
                                &prompt_text,
                            );
                            if SqlEditorWidget::prompt_for_input_with_sender(sender, &prompt_text)
                                .is_err()
                            {
                                SqlEditorWidget::emit_script_message(
                                    sender,
                                    session,
                                    "PAUSE",
                                    "Error: PAUSE cancelled.",
                                );
                                command_error = true;
                            }
                        }
                        ToolCommand::Accept { name, prompt } => {
                            let prompt_text =
                                prompt.unwrap_or_else(|| format!("Enter value for {}:", name));
                            match SqlEditorWidget::prompt_for_input_with_sender(
                                sender,
                                &prompt_text,
                            ) {
                                Ok(value) => {
                                    let key = SessionState::normalize_name(&name);
                                    match session.lock() {
                                        Ok(mut guard) => {
                                            guard.define_vars.insert(key.clone(), value);
                                        }
                                        Err(poisoned) => {
                                            eprintln!(
                                                "Warning: session state lock was poisoned; recovering."
                                            );
                                            poisoned
                                                .into_inner()
                                                .define_vars
                                                .insert(key.clone(), value);
                                        }
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        sender,
                                        session,
                                        &format!("ACCEPT {}", key),
                                        &format!("Value assigned to {}", key),
                                    );
                                }
                                Err(message) => {
                                    SqlEditorWidget::emit_script_message(
                                        sender,
                                        session,
                                        &format!("ACCEPT {}", name),
                                        &format!("Error: {}", message),
                                    );
                                    command_error = true;
                                }
                            }
                        }
                        ToolCommand::Define { name, value } => {
                            let key = SessionState::normalize_name(&name);
                            match session.lock() {
                                Ok(mut guard) => {
                                    guard.define_vars.insert(key.clone(), value.clone());
                                }
                                Err(poisoned) => {
                                    eprintln!(
                                        "Warning: session state lock was poisoned; recovering."
                                    );
                                    poisoned
                                        .into_inner()
                                        .define_vars
                                        .insert(key.clone(), value.clone());
                                }
                            }
                            SqlEditorWidget::emit_script_message(
                                sender,
                                session,
                                &format!("DEFINE {}", key),
                                &format!("Defined {} = {}", key, value),
                            );
                        }
                        ToolCommand::Undefine { name } => {
                            let key = SessionState::normalize_name(&name);
                            match session.lock() {
                                Ok(mut guard) => {
                                    guard.define_vars.remove(&key);
                                }
                                Err(poisoned) => {
                                    eprintln!(
                                        "Warning: session state lock was poisoned; recovering."
                                    );
                                    poisoned.into_inner().define_vars.remove(&key);
                                }
                            }
                            SqlEditorWidget::emit_script_message(
                                sender,
                                session,
                                &format!("UNDEFINE {}", key),
                                &format!("Undefined {}", key),
                            );
                        }
                        ToolCommand::ColumnNewValue {
                            column_name,
                            variable_name,
                        } => {
                            let column_key = SessionState::normalize_name(&column_name);
                            let variable_key = SessionState::normalize_name(&variable_name);
                            match session.lock() {
                                Ok(mut guard) => {
                                    guard
                                        .column_new_values
                                        .insert(column_key.clone(), variable_key.clone());
                                }
                                Err(poisoned) => {
                                    eprintln!(
                                        "Warning: session state lock was poisoned; recovering."
                                    );
                                    poisoned
                                        .into_inner()
                                        .column_new_values
                                        .insert(column_key.clone(), variable_key.clone());
                                }
                            }
                            SqlEditorWidget::emit_script_message(
                                sender,
                                session,
                                &format!("COLUMN {} NEW_VALUE {}", column_key, variable_key),
                                &format!(
                                    "Registered NEW_VALUE mapping: {} -> {}",
                                    column_key, variable_key
                                ),
                            );
                        }
                        ToolCommand::BreakOn { column_name } => {
                            let key = SessionState::normalize_name(&column_name);
                            match session.lock() {
                                Ok(mut guard) => guard.break_column = Some(key.clone()),
                                Err(poisoned) => {
                                    eprintln!(
                                        "Warning: session state lock was poisoned; recovering."
                                    );
                                    poisoned.into_inner().break_column = Some(key.clone());
                                }
                            }
                            SqlEditorWidget::emit_script_message(
                                sender,
                                session,
                                "BREAK",
                                &format!("BREAK ON {}", key),
                            );
                        }
                        ToolCommand::BreakOff | ToolCommand::ClearBreaks => {
                            match session.lock() {
                                Ok(mut guard) => guard.break_column = None,
                                Err(poisoned) => {
                                    eprintln!(
                                        "Warning: session state lock was poisoned; recovering."
                                    );
                                    poisoned.into_inner().break_column = None;
                                }
                            }
                            SqlEditorWidget::emit_script_message(
                                sender,
                                session,
                                "BREAK",
                                "BREAK OFF",
                            );
                        }
                        ToolCommand::Compute {
                            mode,
                            of_column,
                            on_column,
                        } => {
                            match session.lock() {
                                Ok(mut guard) => {
                                    guard.compute = Some(crate::db::ComputeConfig {
                                        mode,
                                        of_column: of_column.clone(),
                                        on_column: on_column.clone(),
                                    });
                                }
                                Err(poisoned) => {
                                    eprintln!(
                                        "Warning: session state lock was poisoned; recovering."
                                    );
                                    poisoned.into_inner().compute =
                                        Some(crate::db::ComputeConfig {
                                            mode,
                                            of_column: of_column.clone(),
                                            on_column: on_column.clone(),
                                        });
                                }
                            }
                            SqlEditorWidget::emit_script_message(
                                sender,
                                session,
                                "COMPUTE",
                                "COMPUTE configured",
                            );
                        }
                        ToolCommand::ComputeOff | ToolCommand::ClearComputes => {
                            match session.lock() {
                                Ok(mut guard) => guard.compute = None,
                                Err(poisoned) => {
                                    eprintln!(
                                        "Warning: session state lock was poisoned; recovering."
                                    );
                                    poisoned.into_inner().compute = None;
                                }
                            }
                            SqlEditorWidget::emit_script_message(
                                sender,
                                session,
                                "COMPUTE",
                                "COMPUTE OFF",
                            );
                        }
                        ToolCommand::ClearBreaksComputes => {
                            match session.lock() {
                                Ok(mut guard) => {
                                    guard.break_column = None;
                                    guard.compute = None;
                                }
                                Err(poisoned) => {
                                    eprintln!(
                                        "Warning: session state lock was poisoned; recovering."
                                    );
                                    let mut guard = poisoned.into_inner();
                                    guard.break_column = None;
                                    guard.compute = None;
                                }
                            }
                            SqlEditorWidget::emit_script_message(
                                sender,
                                session,
                                "CLEAR",
                                "BREAKS and COMPUTES cleared",
                            );
                        }
                        ToolCommand::SetErrorContinue { enabled } => {
                            match session.lock() {
                                Ok(mut guard) => guard.continue_on_error = enabled,
                                Err(poisoned) => {
                                    eprintln!(
                                        "Warning: session state lock was poisoned; recovering."
                                    );
                                    poisoned.into_inner().continue_on_error = enabled;
                                }
                            }
                            continue_on_error = enabled;
                            SqlEditorWidget::emit_script_message(
                                sender,
                                session,
                                "SET ERRORCONTINUE",
                                &format!("ERRORCONTINUE {}", if enabled { "ON" } else { "OFF" }),
                            );
                        }
                        ToolCommand::SetAutoCommit { enabled } => {
                            let connection_generation = {
                                let mut conn_guard = lock_connection_with_activity(
                                    shared_connection,
                                    db_activity.to_string(),
                                );
                                conn_guard.set_auto_commit(enabled);
                                conn_guard.connection_generation()
                            };
                            auto_commit = enabled;
                            match SqlEditorWidget::apply_mysql_autocommit_to_reusable_pooled_session(
                                shared_connection,
                                pooled_db_session,
                                connection_generation,
                                enabled,
                                db_activity,
                            ) {
                                Ok(()) => {
                                    SqlEditorWidget::emit_script_message(
                                        sender,
                                        session,
                                        "SET AUTOCOMMIT",
                                        if enabled {
                                            "Auto-commit enabled"
                                        } else {
                                            "Auto-commit disabled"
                                        },
                                    );
                                }
                                Err(message) => {
                                    SqlEditorWidget::emit_script_message(
                                        sender,
                                        session,
                                        "SET AUTOCOMMIT",
                                        &format!("Error: {}", message),
                                    );
                                    command_error = true;
                                }
                            }
                            let _ = sender.send(QueryProgress::AutoCommitChanged { enabled });
                            app::awake();
                        }
                        ToolCommand::SetDefine {
                            enabled,
                            define_char,
                        } => match session.lock() {
                            Ok(mut guard) => {
                                guard.define_enabled = enabled;
                                if let Some(ch) = define_char {
                                    guard.define_char = ch;
                                }
                            }
                            Err(poisoned) => {
                                eprintln!("Warning: session state lock was poisoned; recovering.");
                                let mut guard = poisoned.into_inner();
                                guard.define_enabled = enabled;
                                if let Some(ch) = define_char {
                                    guard.define_char = ch;
                                }
                            }
                        },
                        ToolCommand::SetScan { enabled } => match session.lock() {
                            Ok(mut guard) => guard.scan_enabled = enabled,
                            Err(poisoned) => {
                                eprintln!("Warning: session state lock was poisoned; recovering.");
                                poisoned.into_inner().scan_enabled = enabled;
                            }
                        },
                        ToolCommand::SetVerify { enabled } => match session.lock() {
                            Ok(mut guard) => guard.verify_enabled = enabled,
                            Err(poisoned) => {
                                eprintln!("Warning: session state lock was poisoned; recovering.");
                                poisoned.into_inner().verify_enabled = enabled;
                            }
                        },
                        ToolCommand::SetEcho { enabled } => match session.lock() {
                            Ok(mut guard) => guard.echo_enabled = enabled,
                            Err(poisoned) => {
                                eprintln!("Warning: session state lock was poisoned; recovering.");
                                poisoned.into_inner().echo_enabled = enabled;
                            }
                        },
                        ToolCommand::SetTiming { enabled } => match session.lock() {
                            Ok(mut guard) => guard.timing_enabled = enabled,
                            Err(poisoned) => {
                                eprintln!("Warning: session state lock was poisoned; recovering.");
                                poisoned.into_inner().timing_enabled = enabled;
                            }
                        },
                        ToolCommand::SetFeedback { enabled } => match session.lock() {
                            Ok(mut guard) => guard.feedback_enabled = enabled,
                            Err(poisoned) => {
                                eprintln!("Warning: session state lock was poisoned; recovering.");
                                poisoned.into_inner().feedback_enabled = enabled;
                            }
                        },
                        ToolCommand::SetHeading { enabled } => match session.lock() {
                            Ok(mut guard) => guard.heading_enabled = enabled,
                            Err(poisoned) => {
                                eprintln!("Warning: session state lock was poisoned; recovering.");
                                poisoned.into_inner().heading_enabled = enabled;
                            }
                        },
                        ToolCommand::SetPageSize { size } => match session.lock() {
                            Ok(mut guard) => guard.pagesize = size,
                            Err(poisoned) => {
                                eprintln!("Warning: session state lock was poisoned; recovering.");
                                poisoned.into_inner().pagesize = size;
                            }
                        },
                        ToolCommand::SetLineSize { size } => match session.lock() {
                            Ok(mut guard) => guard.linesize = size,
                            Err(poisoned) => {
                                eprintln!("Warning: session state lock was poisoned; recovering.");
                                poisoned.into_inner().linesize = size;
                            }
                        },
                        ToolCommand::SetTrimSpool { enabled } => match session.lock() {
                            Ok(mut guard) => guard.trimspool_enabled = enabled,
                            Err(poisoned) => {
                                eprintln!("Warning: session state lock was poisoned; recovering.");
                                poisoned.into_inner().trimspool_enabled = enabled;
                            }
                        },
                        ToolCommand::SetTrimOut { enabled } => match session.lock() {
                            Ok(mut guard) => guard.trimout_enabled = enabled,
                            Err(poisoned) => {
                                eprintln!("Warning: session state lock was poisoned; recovering.");
                                poisoned.into_inner().trimout_enabled = enabled;
                            }
                        },
                        ToolCommand::SetSqlBlankLines { enabled } => match session.lock() {
                            Ok(mut guard) => guard.sqlblanklines_enabled = enabled,
                            Err(poisoned) => {
                                eprintln!("Warning: session state lock was poisoned; recovering.");
                                poisoned.into_inner().sqlblanklines_enabled = enabled;
                            }
                        },
                        ToolCommand::SetTab { enabled } => match session.lock() {
                            Ok(mut guard) => guard.tab_enabled = enabled,
                            Err(poisoned) => {
                                eprintln!("Warning: session state lock was poisoned; recovering.");
                                poisoned.into_inner().tab_enabled = enabled;
                            }
                        },
                        ToolCommand::SetColSep { separator } => match session.lock() {
                            Ok(mut guard) => guard.colsep = separator,
                            Err(poisoned) => {
                                eprintln!("Warning: session state lock was poisoned; recovering.");
                                poisoned.into_inner().colsep = separator;
                            }
                        },
                        ToolCommand::SetNull { null_text } => match session.lock() {
                            Ok(mut guard) => guard.null_text = null_text,
                            Err(poisoned) => {
                                eprintln!("Warning: session state lock was poisoned; recovering.");
                                poisoned.into_inner().null_text = null_text;
                            }
                        },
                        ToolCommand::Spool { path, append } => match path {
                            Some(path) => {
                                let target_path = if Path::new(&path).is_absolute() {
                                    PathBuf::from(&path)
                                } else {
                                    current_frame_base_dir.join(&path)
                                };
                                match session.lock() {
                                    Ok(mut guard) => {
                                        guard.spool_path = Some(target_path.clone());
                                        guard.spool_truncate = !append;
                                    }
                                    Err(poisoned) => {
                                        eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                        let mut guard = poisoned.into_inner();
                                        guard.spool_path = Some(target_path.clone());
                                        guard.spool_truncate = !append;
                                    }
                                }
                                SqlEditorWidget::emit_script_message(
                                    sender,
                                    session,
                                    "SPOOL",
                                    &format!(
                                        "Spooling output to {} ({})",
                                        target_path.display(),
                                        if append { "append" } else { "replace" }
                                    ),
                                );
                            }
                            None if append => match SqlEditorWidget::has_spool_target(session) {
                                true => SqlEditorWidget::emit_script_message(
                                    sender,
                                    session,
                                    "SPOOL",
                                    "Spool switched to append mode",
                                ),
                                false => {
                                    SqlEditorWidget::emit_script_message(
                                        sender,
                                        session,
                                        "SPOOL",
                                        "Error: SPOOL APPEND requires an active spool target.",
                                    );
                                    command_error = true;
                                }
                            },
                            None => match session.lock() {
                                Ok(mut guard) => guard.spool_path = None,
                                Err(poisoned) => {
                                    eprintln!(
                                        "Warning: session state lock was poisoned; recovering."
                                    );
                                    poisoned.into_inner().spool_path = None;
                                }
                            },
                        },
                        ToolCommand::WheneverSqlError { exit, .. } => {
                            match session.lock() {
                                Ok(mut guard) => guard.continue_on_error = !exit,
                                Err(poisoned) => {
                                    eprintln!(
                                        "Warning: session state lock was poisoned; recovering."
                                    );
                                    poisoned.into_inner().continue_on_error = !exit;
                                }
                            }
                            continue_on_error = !exit;
                        }
                        ToolCommand::WheneverOsError { exit } => {
                            match session.lock() {
                                Ok(mut guard) => guard.continue_on_error = !exit,
                                Err(poisoned) => {
                                    eprintln!(
                                        "Warning: session state lock was poisoned; recovering."
                                    );
                                    poisoned.into_inner().continue_on_error = !exit;
                                }
                            }
                            continue_on_error = !exit;
                        }
                        ToolCommand::Exit | ToolCommand::Quit => {
                            stop_execution = true;
                        }
                        ToolCommand::Disconnect => {
                            let had_connection = {
                                let mut conn_guard = lock_connection_with_activity(
                                    shared_connection,
                                    db_activity.to_string(),
                                );
                                let had_connection =
                                    conn_guard.is_connected() || conn_guard.has_connection_handle();
                                conn_guard.disconnect();
                                had_connection
                            };
                            crate::db::clear_pooled_session_lease(pooled_db_session);
                            conn_name.clear();
                            let _ = sender.send(QueryProgress::ConnectionChanged { info: None });
                            app::awake();
                            SqlEditorWidget::emit_script_message(
                                sender,
                                session,
                                "DISCONNECT",
                                if had_connection {
                                    "Disconnected from database"
                                } else {
                                    "Not connected to any database"
                                },
                            );
                        }
                        ToolCommand::RunScript {
                            path,
                            relative_to_caller,
                        } => {
                            let include_base_dir = if relative_to_caller {
                                current_frame_base_dir.as_path()
                            } else {
                                working_dir.as_path()
                            };
                            let (target_path, normalized_target_path) =
                                SqlEditorWidget::resolve_script_include_path(
                                    &path,
                                    relative_to_caller,
                                    current_frame_base_dir.as_path(),
                                    working_dir.as_path(),
                                );
                            match SqlEditorWidget::validate_script_include_target(
                                &frames,
                                normalized_target_path.as_path(),
                            ) {
                                Ok(()) => {
                                    let current_mysql_delimiter =
                                        SqlEditorWidget::current_mysql_delimiter_from_session(
                                            session,
                                        );
                                    match SqlEditorWidget::load_script_include(
                                        target_path.as_path(),
                                        normalized_target_path.as_path(),
                                        include_base_dir,
                                        Some(crate::db::connection::DatabaseType::MySQL),
                                        current_mysql_delimiter.as_deref(),
                                    ) {
                                        Ok(resolved_include) => {
                                            frames.push(ScriptExecutionFrame {
                                                items: resolved_include.items,
                                                index: 0,
                                                base_dir: resolved_include.script_dir,
                                                source_path: Some(resolved_include.source_path),
                                            });
                                        }
                                        Err(message) => {
                                            SqlEditorWidget::emit_script_message(
                                                sender,
                                                session,
                                                if relative_to_caller { "@@" } else { "@" },
                                                &format!("Error: {}", message),
                                            );
                                            command_error = true;
                                        }
                                    }
                                }
                                Err(message) => {
                                    SqlEditorWidget::emit_script_message(
                                        sender,
                                        session,
                                        if relative_to_caller { "@@" } else { "@" },
                                        &format!("Error: {}", message),
                                    );
                                    command_error = true;
                                }
                            }
                        }
                        ToolCommand::MysqlSource { path } => {
                            frames.push(ScriptExecutionFrame {
                                items: vec![ScriptItem::ToolCommand(ToolCommand::RunScript {
                                    path,
                                    relative_to_caller: false,
                                })],
                                index: 0,
                                base_dir: current_frame_base_dir,
                                source_path: None,
                            });
                        }
                        ToolCommand::Use { database } => {
                            let use_sql = SqlEditorWidget::format_tool_command(&ToolCommand::Use {
                                database: database.clone(),
                            });
                            match execute_mysql_sql(use_sql.as_str(), auto_commit) {
                                Ok(_) => {
                                    let info = {
                                        let conn_guard = lock_connection_with_activity(
                                            shared_connection,
                                            db_activity.to_string(),
                                        );
                                        if conn_guard.db_type().execution_engine()
                                            == crate::db::DbExecutionEngine::MySql
                                        {
                                            Some(Self::connection_info_for_ui(
                                                conn_guard.get_info(),
                                            ))
                                        } else {
                                            None
                                        }
                                    };
                                    SqlEditorWidget::emit_script_output(
                                        sender,
                                        session,
                                        vec![format!("Database changed to {}", database)],
                                    );
                                    if let Some(info) = info {
                                        let _ = sender.send(QueryProgress::ConnectionChanged {
                                            info: Some(info),
                                        });
                                    } else {
                                        let _ = sender.send(QueryProgress::MetadataRefreshNeeded);
                                    }
                                    app::awake();
                                }
                                Err(message) => {
                                    let (cancelled, timed_out) = mysql_interruption_flags(&message);
                                    SqlEditorWidget::emit_script_message(
                                        sender,
                                        session,
                                        "USE",
                                        &format!("Error: {}", message),
                                    );
                                    command_error = true;
                                    if cancelled || timed_out {
                                        stop_execution = true;
                                    }
                                }
                            }
                        }
                        ToolCommand::MysqlDelimiter { delimiter } => {
                            match session.lock() {
                                Ok(mut guard) => {
                                    guard.mysql_delimiter = if delimiter == ";" {
                                        None
                                    } else {
                                        Some(delimiter.clone())
                                    };
                                }
                                Err(poisoned) => {
                                    eprintln!(
                                        "Warning: session state lock was poisoned; recovering."
                                    );
                                    let mut guard = poisoned.into_inner();
                                    guard.mysql_delimiter = if delimiter == ";" {
                                        None
                                    } else {
                                        Some(delimiter.clone())
                                    };
                                }
                            }
                            SqlEditorWidget::emit_script_output(
                                sender,
                                session,
                                vec![format!("Delimiter set to '{}'", delimiter)],
                            );
                        }
                        ref mysql_command @ (ToolCommand::Describe { .. }
                        | ToolCommand::ShowDatabases
                        | ToolCommand::ShowTables
                        | ToolCommand::ShowColumns { .. }
                        | ToolCommand::ShowCreateTable { .. }
                        | ToolCommand::ShowProcessList
                        | ToolCommand::ShowVariables { .. }
                        | ToolCommand::ShowStatus { .. }
                        | ToolCommand::ShowWarnings
                        | ToolCommand::MysqlShowErrors) => {
                            let sql = SqlEditorWidget::format_tool_command(mysql_command);
                            match execute_mysql_sql(sql.as_str(), auto_commit) {
                                Ok(results) => {
                                    let Some(result) = results.into_iter().next() else {
                                        let _ = SqlEditorWidget::emit_non_select_result(
                                            sender,
                                            session,
                                            &conn_name,
                                            result_index,
                                            sql.as_str(),
                                            "Command produced no result.".to_string(),
                                            false,
                                            false,
                                            script_mode,
                                        );
                                        result_index += 1;
                                        continue;
                                    };
                                    let (heading_enabled, feedback_enabled) =
                                        SqlEditorWidget::current_output_settings(session);
                                    let column_names = SqlEditorWidget::apply_heading_setting(
                                        result
                                            .columns
                                            .iter()
                                            .map(|column| column.name.clone())
                                            .collect(),
                                        heading_enabled,
                                    );
                                    SqlEditorWidget::emit_select_result(
                                        sender,
                                        session,
                                        &conn_name,
                                        result_index,
                                        sql.as_str(),
                                        column_names,
                                        result.rows.clone(),
                                        result.success,
                                        feedback_enabled,
                                    );
                                    SqlEditorWidget::apply_column_new_value_from_row(
                                        session,
                                        &result
                                            .columns
                                            .iter()
                                            .map(|column| column.name.clone())
                                            .collect::<Vec<String>>(),
                                        result.rows.last().map(Vec::as_slice),
                                    );
                                    result_index += 1;
                                }
                                Err(message) => {
                                    let (cancelled, timed_out) = mysql_interruption_flags(&message);
                                    let emitted = SqlEditorWidget::emit_non_select_result(
                                        sender,
                                        session,
                                        &conn_name,
                                        result_index,
                                        sql.as_str(),
                                        format!("Error: {}", message),
                                        false,
                                        timed_out,
                                        script_mode,
                                    );
                                    if emitted || script_mode {
                                        result_index += 1;
                                    }
                                    command_error = true;
                                    if cancelled || timed_out {
                                        stop_execution = true;
                                    }
                                }
                            }
                        }
                        ToolCommand::Unsupported {
                            raw,
                            message,
                            is_error,
                        } => {
                            SqlEditorWidget::emit_script_message(
                                sender,
                                session,
                                &raw,
                                &format!(
                                    "{}: {}",
                                    if is_error { "Error" } else { "Warning" },
                                    message
                                ),
                            );
                            command_error = is_error;
                        }
                        ToolCommand::Var { .. }
                        | ToolCommand::Print { .. }
                        | ToolCommand::SetServerOutput { .. }
                        | ToolCommand::ShowErrors { .. }
                        | ToolCommand::ShowUser
                        | ToolCommand::ShowAll
                        | ToolCommand::Connect { .. } => {
                            SqlEditorWidget::emit_script_message(
                                sender,
                                session,
                                &SqlEditorWidget::format_tool_command(&command),
                                "Error: This command is only supported for Oracle connections.",
                            );
                            command_error = true;
                        }
                    }

                    if command_error && !continue_on_error {
                        stop_execution = true;
                    }
                }
                ScriptItem::Statement(statement) => {
                    let trimmed = statement.trim_start_matches(';').trim();
                    if trimmed.is_empty() {
                        continue;
                    }

                    let mut sql_text = trimmed.to_string();
                    let (define_enabled, scan_enabled, verify_enabled) = match session.lock() {
                        Ok(guard) => (
                            guard.define_enabled,
                            guard.scan_enabled,
                            guard.verify_enabled,
                        ),
                        Err(poisoned) => {
                            eprintln!("Warning: session state lock was poisoned; recovering.");
                            let guard = poisoned.into_inner();
                            (
                                guard.define_enabled,
                                guard.scan_enabled,
                                guard.verify_enabled,
                            )
                        }
                    };
                    if define_enabled && scan_enabled {
                        let sql_before = sql_text.clone();
                        match SqlEditorWidget::apply_define_substitution(&sql_text, session, sender)
                        {
                            Ok(updated) => {
                                if verify_enabled && updated != sql_before {
                                    SqlEditorWidget::emit_script_output(
                                        sender,
                                        session,
                                        vec![
                                            format!("old: {}", sql_before),
                                            format!("new: {}", updated),
                                        ],
                                    );
                                }
                                sql_text = updated;
                            }
                            Err(message) => {
                                let emitted = SqlEditorWidget::emit_non_select_result(
                                    sender,
                                    session,
                                    &conn_name,
                                    result_index,
                                    trimmed,
                                    format!("Error: {}", message),
                                    false,
                                    false,
                                    script_mode,
                                );
                                if emitted || script_mode {
                                    result_index += 1;
                                }
                                if !continue_on_error {
                                    stop_execution = true;
                                }
                                continue;
                            }
                        }
                    }

                    let statement_start = Instant::now();
                    if lazy_fetch_single_statement
                        && crate::db::query::mysql_executor::MysqlExecutor::is_select_statement(
                            &sql_text,
                        )
                    {
                        match Self::acquire_mysql_pooled_session(
                            shared_connection,
                            pooled_db_session,
                            db_activity,
                            auto_commit,
                            Some(sender),
                        ) {
                            Ok((
                                connection_generation,
                                connection_info,
                                conn,
                                prior_may_have_uncommitted_work,
                            )) => {
                                let (heading_enabled, feedback_enabled) =
                                    SqlEditorWidget::current_output_settings(session);
                                let (colsep, null_text, _trimspool_enabled) =
                                    SqlEditorWidget::current_text_output_settings(session);
                                let state_hint =
                                    SqlEditorWidget::mysql_session_state_hint_for_sql(&sql_text);
                                let session_id = next_lazy_fetch_session_id
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                let _ = sender.send(QueryProgress::StatementStart {
                                    index: result_index,
                                });
                                app::awake();
                                SqlEditorWidget::start_mysql_lazy_select(
                                    connection_generation,
                                    Arc::clone(shared_connection),
                                    conn,
                                    connection_info,
                                    Arc::clone(pooled_db_session),
                                    sender.clone(),
                                    session.clone(),
                                    conn_name.clone(),
                                    result_index,
                                    sql_text.clone(),
                                    heading_enabled,
                                    feedback_enabled,
                                    colsep,
                                    null_text,
                                    auto_commit,
                                    prior_may_have_uncommitted_work,
                                    state_hint,
                                    query_timeout,
                                    active_lazy_fetch.clone(),
                                    session_id,
                                );
                                result_index += 1;
                                continue;
                            }
                            Err(message) => {
                                let (cancelled, timed_out) = mysql_interruption_flags(&message);
                                if script_mode {
                                    let emitted = SqlEditorWidget::emit_non_select_result(
                                        sender,
                                        session,
                                        &conn_name,
                                        result_index,
                                        &sql_text,
                                        format!("Error: {}", message),
                                        false,
                                        timed_out,
                                        script_mode,
                                    );
                                    if emitted || script_mode {
                                        result_index += 1;
                                    }
                                } else {
                                    let index = result_index;
                                    let _ = sender.send(QueryProgress::StatementStart { index });
                                    app::awake();
                                    let mut result = QueryResult::new_error(&sql_text, &message);
                                    result.is_select = true;
                                    if !result.message.trim().is_empty() {
                                        SqlEditorWidget::append_spool_output(
                                            session,
                                            std::slice::from_ref(&result.message),
                                        );
                                    }
                                    let _ = sender.send(QueryProgress::StatementFinished {
                                        index,
                                        result,
                                        connection_name: conn_name.clone(),
                                        timed_out,
                                    });
                                    app::awake();
                                    result_index += 1;
                                }
                                SqlEditorWidget::emit_timing_if_enabled(
                                    sender,
                                    session,
                                    statement_start.elapsed(),
                                );
                                if cancelled || timed_out || !continue_on_error {
                                    stop_execution = true;
                                }
                                continue;
                            }
                        }
                    }
                    match execute_mysql_sql(sql_text.as_str(), auto_commit) {
                        Ok(results) => {
                            if load_mutex_bool(cancel_flag) {
                                stop_execution = true;
                            }

                            for mut result in results {
                                SqlEditorWidget::apply_mysql_transaction_feedback(
                                    &mut result,
                                    &sql_text,
                                    auto_commit,
                                );

                                if result.is_select {
                                    let (heading_enabled, feedback_enabled) =
                                        SqlEditorWidget::current_output_settings(session);
                                    let raw_column_names = result
                                        .columns
                                        .iter()
                                        .map(|column| column.name.clone())
                                        .collect::<Vec<String>>();
                                    let column_names = SqlEditorWidget::apply_heading_setting(
                                        raw_column_names.clone(),
                                        heading_enabled,
                                    );
                                    SqlEditorWidget::emit_select_result(
                                        sender,
                                        session,
                                        &conn_name,
                                        result_index,
                                        &sql_text,
                                        column_names,
                                        result.rows.clone(),
                                        result.success,
                                        feedback_enabled,
                                    );
                                    SqlEditorWidget::apply_column_new_value_from_row(
                                        session,
                                        &raw_column_names,
                                        result.rows.last().map(Vec::as_slice),
                                    );
                                } else {
                                    let _ = SqlEditorWidget::emit_non_select_result(
                                        sender,
                                        session,
                                        &conn_name,
                                        result_index,
                                        &sql_text,
                                        result.message.clone(),
                                        result.success,
                                        false,
                                        script_mode,
                                    );
                                }

                                result_index += 1;
                                if !result.success && !continue_on_error {
                                    stop_execution = true;
                                    break;
                                }
                            }
                            SqlEditorWidget::emit_timing_if_enabled(
                                sender,
                                session,
                                statement_start.elapsed(),
                            );
                        }
                        Err(message) => {
                            let (cancelled, timed_out) = mysql_interruption_flags(&message);
                            let emitted = SqlEditorWidget::emit_non_select_result(
                                sender,
                                session,
                                &conn_name,
                                result_index,
                                &sql_text,
                                message,
                                false,
                                timed_out,
                                script_mode,
                            );
                            if emitted || script_mode {
                                result_index += 1;
                            }
                            SqlEditorWidget::emit_timing_if_enabled(
                                sender,
                                session,
                                statement_start.elapsed(),
                            );
                            if cancelled || timed_out || !continue_on_error {
                                stop_execution = true;
                            }
                        }
                    }
                }
            }
        }
    }

    fn execute_sql(&self, sql: &str, script_mode: bool) {
        self.execute_sql_with_mysql_delimiter(sql, script_mode, None);
    }

    fn execute_sql_with_mysql_delimiter(
        &self,
        sql: &str,
        script_mode: bool,
        initial_mysql_delimiter: Option<String>,
    ) {
        if sql.trim().is_empty() {
            return;
        }

        if let Some(session_id) = self.active_lazy_fetch_session() {
            if self.cancel_lazy_fetch_session(session_id) {
                self.emit_status("Canceling previous lazy fetch...");
                let _ = self
                    .progress_sender
                    .send(QueryProgress::LazyFetchCanceling { session_id });
                app::awake();
                let widget = self.clone();
                let sql = sql.to_string();
                let initial_mysql_delimiter_for_retry = initial_mysql_delimiter.clone();
                app::add_timeout3(0.2, move |_| {
                    widget.execute_sql_with_mysql_delimiter(
                        &sql,
                        script_mode,
                        initial_mysql_delimiter_for_retry.clone(),
                    );
                });
                return;
            }
        }

        let mut query_run_reservation =
            match QueryRunningReservation::acquire(self.query_running.clone()) {
                Some(reservation) => reservation,
                None => {
                    let _ = self
                        .ui_action_sender
                        .send(UiActionResult::QueryAlreadyRunning);
                    app::awake();
                    return;
                }
            };

        // Build an execution policy once and reuse it for both UI pre-check and worker startup.
        let startup_policy = Self::execution_startup_policy(sql);

        // Pre-check connection status without holding lock for long
        {
            let Some(conn_guard) = crate::db::try_lock_connection(&self.connection) else {
                let _ = self.ui_action_sender.send(UiActionResult::ConnectionBusy);
                app::awake();
                return;
            };

            // Keep UI responsive: avoid network round-trip checks (ping) on the UI thread.
            // The execution worker performs full liveness validation.
            // Regression guard: scripts that contain CONNECT/@START must pass this gate
            // even when disconnected, so CONNECT can establish a session for later SQL.
            if startup_policy.requires_connected_session
                && (!conn_guard.is_connected() || !conn_guard.has_connection_handle())
            {
                SqlEditorWidget::show_alert_dialog("Not connected to database");
                return;
            }
        } // Release lock early for the pre-check

        // Worker thread cleanup now owns execution state reset.
        query_run_reservation.disarm();

        let shared_connection = self.connection.clone();
        let query_timeout = Self::parse_timeout(&self.timeout_input.value());
        let sql_text = sql.to_string();
        let db_activity = Self::db_activity_label_for_sql(&sql_text, script_mode);
        let sender = self.progress_sender.clone();
        let query_running = self.query_running.clone();
        let current_query_connection = self.current_query_connection.clone();
        let pooled_db_session = self.pooled_db_session.clone();
        let active_lazy_fetch = self.active_lazy_fetch.clone();
        let next_lazy_fetch_session_id = self.next_lazy_fetch_session_id.clone();
        let current_mysql_cancel_context = self.current_mysql_cancel_context.clone();
        let cancel_flag = self.cancel_flag.clone();
        let initial_mysql_delimiter_for_worker = initial_mysql_delimiter;

        // Reset cancel flag before starting new execution
        store_mutex_bool(&cancel_flag, false);

        set_cursor(Cursor::Wait);
        app::flush();

        let spawn_error_sender = sender.clone();
        let spawn_error_query_running = query_running.clone();
        let spawn_error_cancel_flag = cancel_flag.clone();
        let spawn_error_current_query_connection = current_query_connection.clone();
        let spawn_error_current_mysql_cancel_context = current_mysql_cancel_context.clone();
        let spawn_result = thread::Builder::new()
            .name("query-execution".to_string())
            .spawn(move || {
            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                let mut cleanup = QueryExecutionCleanupGuard::new(
                    sender.clone(),
                    current_query_connection.clone(),
                    current_mysql_cancel_context.clone(),
                    cancel_flag.clone(),
                    query_running.clone(),
                );

                // Acquire connection lock inside thread and hold it during execution
                let conn_guard =
                    lock_connection_with_activity(&shared_connection, db_activity.clone());

                let mut conn_name = if conn_guard.is_connected() {
                    conn_guard.get_info().name.clone()
                } else {
                    String::new()
                };

                let db_type = conn_guard.db_type();
                let auto_commit = conn_guard.auto_commit();
                let selected_transaction_mode = conn_guard.transaction_mode();
                let session = conn_guard.session_state();

                match db_type.execution_engine() {
                    crate::db::DbExecutionEngine::MySql => {
                        if startup_policy.requires_connected_session
                            && (!conn_guard.is_connected() || !conn_guard.has_connection_handle())
                        {
                            let message = crate::db::NOT_CONNECTED_MESSAGE.to_string();
                            let _ = sender.send(QueryProgress::ConnectionChanged { info: None });
                            app::awake();
                            SqlEditorWidget::emit_execution_startup_error(
                                &sender,
                                script_mode,
                                &sql_text,
                                &conn_name,
                                &message,
                                None,
                            );
                            return;
                        }

                        if conn_guard.is_connected() {
                            conn_name = conn_guard.get_info().name.clone();
                        } else {
                            conn_name.clear();
                        }

                        drop(conn_guard);
                        SqlEditorWidget::execute_mysql_batch(
                            &shared_connection,
                            &sender,
                            &sql_text,
                            &conn_name,
                            &session,
                            &pooled_db_session,
                            &active_lazy_fetch,
                            &next_lazy_fetch_session_id,
                            &current_mysql_cancel_context,
                            &cancel_flag,
                            script_mode,
                            initial_mysql_delimiter_for_worker.clone(),
                            query_timeout,
                            auto_commit,
                            &db_activity,
                        );
                        return;
                    }
                    crate::db::DbExecutionEngine::Oracle => {}
                }

                let (guard_after_acquire, acquire_result) =
                    Self::acquire_oracle_pooled_execution_connection(
                        conn_guard,
                        &shared_connection,
                        &db_activity,
                        &sender,
                        startup_policy.has_connect_command,
                        &pooled_db_session,
                    );
                let conn_guard = guard_after_acquire;
                let (mut conn_opt, oracle_prior_may_have_uncommitted_work) = match acquire_result {
                    Ok(Some((conn, prior_may_have_uncommitted_work))) => {
                        (Some(conn), prior_may_have_uncommitted_work)
                    }
                    Ok(None) => (None, false),
                    Err(message) => {
                        SqlEditorWidget::emit_execution_startup_error(
                            &sender,
                            script_mode,
                            &sql_text,
                            &conn_name,
                            &message,
                            None,
                        );
                        return;
                    }
                };

                let connection_generation = conn_guard.connection_generation();

                if conn_guard.is_connected() {
                    conn_name = conn_guard.get_info().name.clone();
                } else {
                    conn_name.clear();
                }

                if startup_policy.requires_connected_session && conn_opt.is_none() {
                    let message = crate::db::NOT_CONNECTED_MESSAGE.to_string();
                    let _ = sender.send(QueryProgress::ConnectionChanged { info: None });
                    app::awake();
                    SqlEditorWidget::emit_execution_startup_error(
                        &sender,
                        script_mode,
                        &sql_text,
                        &conn_name,
                        &message,
                        None,
                    );
                    return;
                }
                // Store connection for cancel operation (separate from mutex)
                if let Some(ref conn) = conn_opt {
                    SqlEditorWidget::set_current_query_connection(
                        &current_query_connection,
                        Some(Arc::clone(conn)),
                    );
                    if load_mutex_bool(&cancel_flag) {
                        let _ = conn.break_execution();
                    }
                }
                // Release the shared connection mutex before running statements so
                // UI/auxiliary workers are not blocked for the full execution window.
                drop(conn_guard);

                let items = super::query_text::split_script_items(&sql_text);
                if items.is_empty() {
                    return;
                }
                let lazy_fetch_single_statement =
                    Self::should_use_lazy_fetch_for_single_statement(&items);

                let _ = sender.send(QueryProgress::BatchStart {
                    activity: db_activity.clone(),
                });
                app::awake();

                // Set timeout only if we have a connection
                let previous_timeout = conn_opt
                    .as_ref()
                    .and_then(|c| c.call_timeout().ok())
                    .flatten();

                if let Some(conn) = conn_opt.as_ref() {
                    cleanup.track_timeout(Arc::clone(conn), previous_timeout);
                    if !startup_policy.has_connect_command {
                        cleanup.track_oracle_pooled_session(
                            Arc::clone(&pooled_db_session),
                            connection_generation,
                            Arc::clone(conn),
                        );
                    }
                }

                let explicit_transaction_first_statement =
                    SqlEditorWidget::requires_transaction_first_statement(&items);
                let transaction_mode = if explicit_transaction_first_statement {
                    crate::db::TransactionMode::default()
                } else {
                    selected_transaction_mode
                };
                let requires_transaction_first_statement = explicit_transaction_first_statement
                    || db_type.transaction_mode_requires_first_statement(transaction_mode);
                let should_apply_oracle_transaction_mode =
                    !oracle_prior_may_have_uncommitted_work;

                if let Some(conn) = conn_opt.as_ref() {
                    if let Err(err) = conn.set_call_timeout(query_timeout) {
                        let timeout_error = err.to_string();
                        SqlEditorWidget::emit_execution_startup_error(
                            &sender,
                            script_mode,
                            &sql_text,
                            &conn_name,
                            &timeout_error,
                            Some(&session),
                        );
                        return;
                    }
                    if should_apply_oracle_transaction_mode {
                        if let Err(err) =
                            crate::db::DatabaseConnection::apply_oracle_transaction_mode(
                                conn.as_ref(),
                                transaction_mode,
                            )
                        {
                            SqlEditorWidget::emit_execution_startup_error(
                                &sender,
                                script_mode,
                                &sql_text,
                                &conn_name,
                                &err,
                                Some(&session),
                            );
                            return;
                        }
                    }
                    if should_apply_oracle_transaction_mode
                        && transaction_mode.access_mode == crate::db::TransactionAccessMode::ReadOnly
                    {
                        cleanup.track_oracle_read_only_transaction(Arc::clone(conn));
                    }
                    if !requires_transaction_first_statement {
                        if let Err(err) =
                            SqlEditorWidget::sync_serveroutput_with_session(conn.as_ref(), &session)
                        {
                            eprintln!(
                                "Failed to apply SERVEROUTPUT setting on session start: {err}"
                            );
                        }
                    }
                }

                let mut result_index = 0usize;
                let mut auto_commit = auto_commit;
                let mut continue_on_error = match session.lock() {
                    Ok(guard) => guard.continue_on_error,
                    Err(poisoned) => {
                        eprintln!("Warning: session state lock was poisoned; recovering.");
                        poisoned.into_inner().continue_on_error
                    }
                };
                let mut stop_execution = false;
                let working_dir = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
                let mut frames = vec![ScriptExecutionFrame {
                    items,
                    index: 0,
                    base_dir: working_dir.clone(),
                    source_path: None,
                }];

                while !frames.is_empty() {
                    if stop_execution || load_mutex_bool(&cancel_flag) {
                        break;
                    }

                    let Some((item, current_frame_base_dir)) = ({
                        let frame = match frames.last_mut() {
                            Some(frame) => frame,
                            None => break,
                        };

                        if frame.index >= frame.items.len() {
                            None
                        } else {
                            let item = frame.items[frame.index].clone();
                            frame.index += 1;
                            Some((item, frame.base_dir.clone()))
                        }
                    }) else {
                        frames.pop();
                        continue;
                    };

                    let echo_enabled = match session.lock() {
                        Ok(guard) => guard.echo_enabled,
                        Err(poisoned) => {
                            eprintln!("Warning: session state lock was poisoned; recovering.");
                            poisoned.into_inner().echo_enabled
                        }
                    };
                    if echo_enabled {
                        let echo_line = match &item {
                            ScriptItem::Statement(statement) => statement.trim().to_string(),
                            ScriptItem::ToolCommand(command) => {
                                SqlEditorWidget::format_tool_command(command)
                            }
                        };
                        if !echo_line.trim().is_empty() {
                            SqlEditorWidget::emit_script_output(&sender, &session, vec![echo_line]);
                        }
                    }

                    match item {
                        ScriptItem::ToolCommand(command) => {
                            let mut command_error = false;
                            match command {
                                ToolCommand::Var { name, data_type } => {
                                    let normalized = SessionState::normalize_name(&name);
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.binds.insert(
                                            normalized.clone(),
                                            BindVar::new(data_type.clone()),
                                        );
                                    }
                                    let message = format!(
                                        "Variable :{} declared as {}",
                                        normalized,
                                        data_type.display()
                                    );
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        &format!("VAR {} {}", normalized, data_type.display()),
                                        &message,
                                    );
                                }
                                ToolCommand::Print { name } => {
                                    let (heading_enabled, feedback_enabled) =
                                        SqlEditorWidget::current_output_settings(&session);
                                    let (_colsep, null_text, _trimspool_enabled) =
                                        SqlEditorWidget::current_text_output_settings(&session);

                                    if let Some(name) = name {
                                        let key = SessionState::normalize_name(&name);
                                        let named_data = {
                                            let guard = match session.lock() {
                                                Ok(guard) => guard,
                                                Err(poisoned) => {
                                                    eprintln!("Warning: session state lock was poisoned; recovering.");
                                                    poisoned.into_inner()
                                                }
                                            };
                                            SqlEditorWidget::clone_print_named_data(&guard, &key)
                                        };

                                        match named_data {
                                            PrintNamedData::Scalar(value) => {
                                                let columns =
                                                    vec!["NAME".to_string(), "VALUE".to_string()];
                                                let rows = vec![vec![
                                                    key.clone(),
                                                    value.unwrap_or_else(|| null_text.clone()),
                                                ]];
                                                let headers =
                                                    SqlEditorWidget::apply_heading_setting(
                                                        columns,
                                                        heading_enabled,
                                                    );
                                                SqlEditorWidget::emit_select_result(
                                                    &sender,
                                                    &session,
                                                    &conn_name,
                                                    result_index,
                                                    &format!("PRINT {}", key),
                                                    headers,
                                                    rows,
                                                    true,
                                                    feedback_enabled,
                                                );
                                                result_index += 1;
                                            }
                                            PrintNamedData::Cursor(cursor) => {
                                                let headers =
                                                    SqlEditorWidget::apply_heading_setting(
                                                        cursor.columns,
                                                        heading_enabled,
                                                    );
                                                SqlEditorWidget::emit_select_result(
                                                    &sender,
                                                    &session,
                                                    &conn_name,
                                                    result_index,
                                                    &format!("PRINT {}", key),
                                                    headers,
                                                    cursor.rows,
                                                    true,
                                                    feedback_enabled,
                                                );
                                                result_index += 1;
                                            }
                                            PrintNamedData::CursorEmpty => {
                                                SqlEditorWidget::emit_script_message(
                                                    &sender,
                                                    &session,
                                                    &format!("PRINT {}", key),
                                                    &format!(
                                                        "Error: Cursor :{} has no data to print.",
                                                        key
                                                    ),
                                                );
                                                command_error = true;
                                            }
                                            PrintNamedData::Missing => {
                                                SqlEditorWidget::emit_script_message(
                                                    &sender,
                                                    &session,
                                                    &format!("PRINT {}", key),
                                                    &format!(
                                                        "Error: Bind variable :{} is not defined.",
                                                        key
                                                    ),
                                                );
                                                command_error = true;
                                            }
                                        }
                                    } else {
                                        let (summary_rows, cursor_results) = {
                                            let guard = match session.lock() {
                                                Ok(guard) => guard,
                                                Err(poisoned) => {
                                                    eprintln!("Warning: session state lock was poisoned; recovering.");
                                                    poisoned.into_inner()
                                                }
                                            };

                                            if guard.binds.is_empty() {
                                                (Vec::new(), Vec::new())
                                            } else {
                                                SqlEditorWidget::collect_print_all_data(
                                                    &guard, &null_text,
                                                )
                                            }
                                        };

                                        if summary_rows.is_empty() {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                "PRINT",
                                                "No bind variables declared.",
                                            );
                                        } else {
                                            let headers = SqlEditorWidget::apply_heading_setting(
                                                vec![
                                                    "NAME".to_string(),
                                                    "TYPE".to_string(),
                                                    "VALUE".to_string(),
                                                ],
                                                heading_enabled,
                                            );
                                            SqlEditorWidget::emit_select_result(
                                                &sender,
                                                &session,
                                                &conn_name,
                                                result_index,
                                                "PRINT",
                                                headers,
                                                summary_rows,
                                                true,
                                                feedback_enabled,
                                            );
                                            result_index += 1;

                                            for (cursor_name, cursor) in cursor_results {
                                                let headers =
                                                    SqlEditorWidget::apply_heading_setting(
                                                        cursor.columns,
                                                        heading_enabled,
                                                    );
                                                SqlEditorWidget::emit_select_result(
                                                    &sender,
                                                    &session,
                                                    &conn_name,
                                                    result_index,
                                                    &format!("PRINT {}", cursor_name),
                                                    headers,
                                                    cursor.rows,
                                                    true,
                                                    feedback_enabled,
                                                );
                                                result_index += 1;
                                            }
                                        }
                                    }
                                }
                                ToolCommand::SetServerOutput {
                                    enabled,
                                    size,
                                    unlimited,
                                } => {
                                    // This command needs a connection
                                    let conn = match conn_opt.as_ref() {
                                        Some(c) => c,
                                        None => {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                "SET SERVEROUTPUT",
                                                "Error: Not connected to database",
                                            );
                                            continue;
                                        }
                                    };

                                    let default_size = 1_000_000u32;
                                    let current_size = match session.lock() {
                                        Ok(guard) => guard.server_output.size,
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            poisoned.into_inner().server_output.size
                                        }
                                    };
                                    let mut message = String::new();
                                    let mut success = true;

                                    if enabled {
                                        if unlimited {
                                            // SIZE UNLIMITED: pass None to enable unlimited buffer
                                            let enable_result = QueryExecutor::enable_dbms_output(
                                                conn.as_ref(),
                                                None,
                                            );

                                            match enable_result {
                                                Ok(()) => {
                                                    let mut guard = match session.lock() {
                                                        Ok(guard) => guard,
                                                        Err(poisoned) => {
                                                            eprintln!("Warning: session state lock was poisoned; recovering.");
                                                            poisoned.into_inner()
                                                        }
                                                    };
                                                    guard.server_output.enabled = true;
                                                    guard.server_output.size = 0; // 0 indicates unlimited
                                                    message =
                                                        "SERVEROUTPUT enabled (size UNLIMITED)"
                                                            .to_string();
                                                }
                                                Err(err) => {
                                                    success = false;
                                                    message = format!(
                                                        "SERVEROUTPUT enable failed: {}",
                                                        err
                                                    );
                                                }
                                            }
                                        } else {
                                            let desired_size =
                                                Self::resolve_serveroutput_enable_size(
                                                    size,
                                                    current_size,
                                                    default_size,
                                                );
                                            let mut applied_size = desired_size;
                                            let mut enable_result =
                                                QueryExecutor::enable_dbms_output(
                                                    conn.as_ref(),
                                                    Some(desired_size),
                                                );

                                            if enable_result.is_err()
                                                && size.is_some()
                                                && desired_size != default_size
                                                && QueryExecutor::enable_dbms_output(
                                                    conn.as_ref(),
                                                    Some(default_size),
                                                )
                                                .is_ok()
                                            {
                                                applied_size = default_size;
                                                message = format!(
                                                        "SERVEROUTPUT enabled with size {} (requested {} not supported)",
                                                        applied_size, desired_size
                                                    );
                                                enable_result = Ok(());
                                            }

                                            match enable_result {
                                                Ok(()) => {
                                                    let mut guard = match session.lock() {
                                                        Ok(guard) => guard,
                                                        Err(poisoned) => {
                                                            eprintln!("Warning: session state lock was poisoned; recovering.");
                                                            poisoned.into_inner()
                                                        }
                                                    };
                                                    guard.server_output.enabled = true;
                                                    guard.server_output.size = applied_size;
                                                    if message.is_empty() {
                                                        message = format!(
                                                            "SERVEROUTPUT enabled (size {})",
                                                            applied_size
                                                        );
                                                    }
                                                }
                                                Err(err) => {
                                                    success = false;
                                                    message = format!(
                                                        "SERVEROUTPUT enable failed: {}",
                                                        err
                                                    );
                                                }
                                            }
                                        }
                                    } else {
                                        match QueryExecutor::disable_dbms_output(conn.as_ref()) {
                                            Ok(()) => {
                                                let mut guard = match session.lock() {
                                                    Ok(guard) => guard,
                                                    Err(poisoned) => {
                                                        eprintln!("Warning: session state lock was poisoned; recovering.");
                                                        poisoned.into_inner()
                                                    }
                                                };
                                                guard.server_output.enabled = false;
                                                message = "SERVEROUTPUT disabled".to_string();
                                            }
                                            Err(err) => {
                                                success = false;
                                                message =
                                                    format!("SERVEROUTPUT disable failed: {}", err);
                                            }
                                        }
                                    }

                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET SERVEROUTPUT",
                                        &message,
                                    );
                                    if !success {
                                        command_error = true;
                                    }
                                }
                                ToolCommand::ShowErrors {
                                    object_type,
                                    object_name,
                                } => {
                                    // This command needs a connection
                                    let conn = match conn_opt.as_ref() {
                                        Some(c) => c,
                                        None => {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                "SHOW ERRORS",
                                                "Error: Not connected to database",
                                            );
                                            continue;
                                        }
                                    };

                                    let mut target = None;
                                    if object_type.is_none() {
                                        target = match session.lock() {
                                            Ok(guard) => guard.last_compiled.clone(),
                                            Err(poisoned) => {
                                                eprintln!("Warning: session state lock was poisoned; recovering.");
                                                poisoned.into_inner().last_compiled.clone()
                                            }
                                        };
                                    } else if let (Some(obj_type), Some(obj_name)) =
                                        (object_type.clone(), object_name.clone())
                                    {
                                        let (owner, name) = if let Some(dot) = obj_name.find('.') {
                                            let (owner_raw, name_raw) = obj_name.split_at(dot);
                                            (
                                                Some(SqlEditorWidget::normalize_object_name(
                                                    owner_raw,
                                                )),
                                                SqlEditorWidget::normalize_object_name(
                                                    name_raw.trim_start_matches('.'),
                                                ),
                                            )
                                        } else {
                                            (
                                                None,
                                                SqlEditorWidget::normalize_object_name(&obj_name),
                                            )
                                        };

                                        target = Some(crate::db::CompiledObject {
                                            owner,
                                            object_type: obj_type.to_uppercase(),
                                            name,
                                        });
                                    }

                                    if let Some(object) = target {
                                        match QueryExecutor::fetch_compilation_errors(
                                            conn.as_ref(),
                                            &object,
                                        ) {
                                            Ok(rows) => {
                                                if rows.is_empty() {
                                                    SqlEditorWidget::emit_script_message(
                                                        &sender,
                                                        &session,
                                                        "SHOW ERRORS",
                                                        "No errors found.",
                                                    );
                                                } else {
                                                    let (heading_enabled, feedback_enabled) =
                                                        SqlEditorWidget::current_output_settings(
                                                            &session,
                                                        );
                                                    let headers =
                                                        SqlEditorWidget::apply_heading_setting(
                                                            vec![
                                                                "LINE".to_string(),
                                                                "POSITION".to_string(),
                                                                "TEXT".to_string(),
                                                            ],
                                                            heading_enabled,
                                                        );
                                                    SqlEditorWidget::emit_select_result(
                                                        &sender,
                                                        &session,
                                                        &conn_name,
                                                        result_index,
                                                        "SHOW ERRORS",
                                                        headers,
                                                        rows,
                                                        true,
                                                        feedback_enabled,
                                                    );
                                                    result_index += 1;
                                                }
                                            }
                                            Err(err) => {
                                                SqlEditorWidget::emit_script_message(
                                                    &sender,
                                                    &session,
                                                    "SHOW ERRORS",
                                                    &format!("Error: {}", err),
                                                );
                                                command_error = true;
                                            }
                                        }
                                    } else {
                                        SqlEditorWidget::emit_script_message(
                                            &sender,
                                            &session,
                                            "SHOW ERRORS",
                                            "Error: No compiled object found to show errors.",
                                        );
                                        command_error = true;
                                    }
                                }
                                ToolCommand::ShowUser => {
                                    // This command needs a connection
                                    let conn = match conn_opt.as_ref() {
                                        Some(c) => c,
                                        None => {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                "SHOW USER",
                                                "Error: Not connected to database",
                                            );
                                            continue;
                                        }
                                    };

                                    let sql = "SELECT USER FROM DUAL";
                                    let user_result: Result<String, OracleError> = (|| {
                                        let mut stmt = conn.statement(sql).build()?;
                                        let row = stmt.query_row(&[])?;
                                        let user: String = row.get(0)?;
                                        Ok(user)
                                    })(
                                    );
                                    match user_result {
                                        Ok(user) => {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                "SHOW USER",
                                                &format!("USER: {}", user),
                                            );
                                        }
                                        Err(err) => {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                "SHOW USER",
                                                &format!("Error: {}", err),
                                            );
                                            command_error = true;
                                        }
                                    }
                                }
                                ToolCommand::ShowAll => {
                                    let (
                                        server_output,
                                        define_enabled,
                                        define_char,
                                        scan_enabled,
                                        verify_enabled,
                                        echo_enabled,
                                        timing_enabled,
                                        feedback_enabled,
                                        heading_enabled,
                                        pagesize,
                                        linesize,
                                        trimspool_enabled,
                                        trimout_enabled,
                                        sqlblanklines_enabled,
                                        tab_enabled,
                                        colsep,
                                        null_text,
                                        break_column,
                                        compute_config,
                                        continue_on_error,
                                        spool_path,
                                    ) = match session.lock() {
                                        Ok(guard) => (
                                            guard.server_output.clone(),
                                            guard.define_enabled,
                                            guard.define_char,
                                            guard.scan_enabled,
                                            guard.verify_enabled,
                                            guard.echo_enabled,
                                            guard.timing_enabled,
                                            guard.feedback_enabled,
                                            guard.heading_enabled,
                                            guard.pagesize,
                                            guard.linesize,
                                            guard.trimspool_enabled,
                                            guard.trimout_enabled,
                                            guard.sqlblanklines_enabled,
                                            guard.tab_enabled,
                                            guard.colsep.clone(),
                                            guard.null_text.clone(),
                                            guard.break_column.clone(),
                                            guard.compute.clone(),
                                            guard.continue_on_error,
                                            guard.spool_path.clone(),
                                        ),
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let guard = poisoned.into_inner();
                                            (
                                                guard.server_output.clone(),
                                                guard.define_enabled,
                                                guard.define_char,
                                                guard.scan_enabled,
                                                guard.verify_enabled,
                                                guard.echo_enabled,
                                                guard.timing_enabled,
                                                guard.feedback_enabled,
                                                guard.heading_enabled,
                                                guard.pagesize,
                                                guard.linesize,
                                                guard.trimspool_enabled,
                                                guard.trimout_enabled,
                                                guard.sqlblanklines_enabled,
                                                guard.tab_enabled,
                                                guard.colsep.clone(),
                                                guard.null_text.clone(),
                                                guard.break_column.clone(),
                                                guard.compute.clone(),
                                                guard.continue_on_error,
                                                guard.spool_path.clone(),
                                            )
                                        }
                                    };

                                    let autocommit_enabled = auto_commit;

                                    let serveroutput_line = if server_output.enabled {
                                        if server_output.size == 0 {
                                            "SERVEROUTPUT ON SIZE UNLIMITED".to_string()
                                        } else {
                                            format!("SERVEROUTPUT ON SIZE {}", server_output.size)
                                        }
                                    } else {
                                        "SERVEROUTPUT OFF".to_string()
                                    };

                                    let spool_line = match spool_path {
                                        Some(path) => format!("SPOOL {}", path.display()),
                                        None => "SPOOL OFF".to_string(),
                                    };

                                    let lines = vec![
                                        format!(
                                            "AUTOCOMMIT {}",
                                            if autocommit_enabled { "ON" } else { "OFF" }
                                        ),
                                        serveroutput_line,
                                        if define_enabled {
                                            format!("DEFINE '{}'", define_char)
                                        } else {
                                            "DEFINE OFF".to_string()
                                        },
                                        format!("SCAN {}", if scan_enabled { "ON" } else { "OFF" }),
                                        format!(
                                            "VERIFY {}",
                                            if verify_enabled { "ON" } else { "OFF" }
                                        ),
                                        format!("ECHO {}", if echo_enabled { "ON" } else { "OFF" }),
                                        format!(
                                            "TIMING {}",
                                            if timing_enabled { "ON" } else { "OFF" }
                                        ),
                                        format!(
                                            "FEEDBACK {}",
                                            if feedback_enabled { "ON" } else { "OFF" }
                                        ),
                                        format!(
                                            "HEADING {}",
                                            if heading_enabled { "ON" } else { "OFF" }
                                        ),
                                        format!("PAGESIZE {}", pagesize),
                                        format!("LINESIZE {}", linesize),
                                        format!(
                                            "TRIMSPOOL {}",
                                            if trimspool_enabled { "ON" } else { "OFF" }
                                        ),
                                        format!(
                                            "TRIMOUT {}",
                                            if trimout_enabled { "ON" } else { "OFF" }
                                        ),
                                        format!(
                                            "SQLBLANKLINES {}",
                                            if sqlblanklines_enabled { "ON" } else { "OFF" }
                                        ),
                                        format!("TAB {}", if tab_enabled { "ON" } else { "OFF" }),
                                        format!("COLSEP {}", colsep),
                                        format!("NULL {}", null_text),
                                        match break_column {
                                            Some(column) => format!("BREAK ON {}", column),
                                            None => "BREAK OFF".to_string(),
                                        },
                                        match compute_config {
                                            Some(config) => {
                                                let mode_text = match config.mode {
                                                    crate::db::ComputeMode::Sum => "SUM",
                                                    crate::db::ComputeMode::Count => "COUNT",
                                                };
                                                match (
                                                    config.of_column.as_deref(),
                                                    config.on_column.as_deref(),
                                                ) {
                                                    (Some(of_col), Some(on_col)) => format!(
                                                        "COMPUTE {} OF {} ON {}",
                                                        mode_text, of_col, on_col
                                                    ),
                                                    _ => format!("COMPUTE {}", mode_text),
                                                }
                                            }
                                            None => "COMPUTE OFF".to_string(),
                                        },
                                        format!(
                                            "ERRORCONTINUE {}",
                                            if continue_on_error { "ON" } else { "OFF" }
                                        ),
                                        spool_line,
                                    ];

                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SHOW ALL",
                                        &lines.join("\n"),
                                    );
                                }
                                ToolCommand::Describe { name } => {
                                    let conn = match conn_opt.as_ref() {
                                        Some(c) => c,
                                        None => {
                                            if script_mode {
                                                SqlEditorWidget::emit_script_message(
                                                    &sender,
                                                    &session,
                                                    "DESCRIBE",
                                                    "Error: Not connected to database",
                                                );
                                            } else {
                                                let emitted =
                                                    SqlEditorWidget::emit_non_select_result(
                                                        &sender,
                                                        &session,
                                                        &conn_name,
                                                        result_index,
                                                        &format!("DESCRIBE {}", name),
                                                        "Error: Not connected to database"
                                                            .to_string(),
                                                        false,
                                                        false,
                                                        false,
                                                    );
                                                if emitted {
                                                    result_index += 1;
                                                }
                                            }
                                            continue;
                                        }
                                    };
                                    let title = format!("DESCRIBE {}", name);
                                    match QueryExecutor::describe_object(conn.as_ref(), &name) {
                                        Ok(columns) => {
                                            if columns.is_empty() {
                                                if script_mode {
                                                    SqlEditorWidget::emit_script_message(
                                                        &sender,
                                                        &session,
                                                        &title,
                                                        "Error: Object not found.",
                                                    );
                                                } else {
                                                    let emitted =
                                                        SqlEditorWidget::emit_non_select_result(
                                                            &sender,
                                                            &session,
                                                            &conn_name,
                                                            result_index,
                                                            &title,
                                                            "Error: Object not found.".to_string(),
                                                            false,
                                                            false,
                                                            false,
                                                        );
                                                    if emitted {
                                                        result_index += 1;
                                                    }
                                                }
                                                command_error = true;
                                            } else {
                                                let rows = columns
                                                    .into_iter()
                                                    .map(|col| {
                                                        let type_display = col.get_type_display();
                                                        let TableColumnDetail {
                                                            name,
                                                            nullable,
                                                            is_primary_key,
                                                            ..
                                                        } = col;
                                                        vec![
                                                            name,
                                                            type_display,
                                                            if nullable {
                                                                "YES".to_string()
                                                            } else {
                                                                "NO".to_string()
                                                            },
                                                            if is_primary_key {
                                                                "PK".to_string()
                                                            } else {
                                                                String::new()
                                                            },
                                                        ]
                                                    })
                                                    .collect::<Vec<Vec<String>>>();
                                                let (heading_enabled, feedback_enabled) =
                                                    SqlEditorWidget::current_output_settings(
                                                        &session,
                                                    );
                                                let headers =
                                                    SqlEditorWidget::apply_heading_setting(
                                                        vec![
                                                            "COLUMN".to_string(),
                                                            "TYPE".to_string(),
                                                            "NULLABLE".to_string(),
                                                            "PK".to_string(),
                                                        ],
                                                        heading_enabled,
                                                    );
                                                SqlEditorWidget::emit_select_result(
                                                    &sender,
                                                    &session,
                                                    &conn_name,
                                                    result_index,
                                                    &title,
                                                    headers,
                                                    rows,
                                                    true,
                                                    feedback_enabled,
                                                );
                                                result_index += 1;
                                            }
                                        }
                                        Err(err) => {
                                            if script_mode {
                                                SqlEditorWidget::emit_script_message(
                                                    &sender,
                                                    &session,
                                                    &title,
                                                    &format!("Error: {}", err),
                                                );
                                            } else {
                                                let emitted =
                                                    SqlEditorWidget::emit_non_select_result(
                                                        &sender,
                                                        &session,
                                                        &conn_name,
                                                        result_index,
                                                        &title,
                                                        format!("Error: {}", err),
                                                        false,
                                                        false,
                                                        false,
                                                    );
                                                if emitted {
                                                    result_index += 1;
                                                }
                                            }
                                            command_error = true;
                                        }
                                    }
                                }
                                ToolCommand::Prompt { text } => {
                                    let mut output_text = text;
                                    let (define_enabled, scan_enabled) = match session.lock() {
                                        Ok(guard) => (guard.define_enabled, guard.scan_enabled),
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let guard = poisoned.into_inner();
                                            (guard.define_enabled, guard.scan_enabled)
                                        }
                                    };
                                    if define_enabled && scan_enabled && !output_text.is_empty() {
                                        match SqlEditorWidget::apply_define_substitution(
                                            &output_text,
                                            &session,
                                            &sender,
                                        ) {
                                            Ok(updated) => {
                                                output_text = updated;
                                            }
                                            Err(message) => {
                                                SqlEditorWidget::emit_script_message(
                                                    &sender,
                                                    &session,
                                                    "PROMPT",
                                                    &format!("Error: {}", message),
                                                );
                                                command_error = true;
                                            }
                                        }
                                    }
                                    if !command_error {
                                        SqlEditorWidget::emit_script_output(
                                            &sender,
                                            &session,
                                            vec![output_text],
                                        );
                                    }
                                }
                                ToolCommand::Pause { message } => {
                                    let prompt_text = message
                                        .filter(|text| !text.trim().is_empty())
                                        .unwrap_or_else(|| "Press ENTER to continue.".to_string());
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "PAUSE",
                                        &prompt_text,
                                    );
                                    match SqlEditorWidget::prompt_for_input_with_sender(
                                        &sender,
                                        &prompt_text,
                                    ) {
                                        Ok(_) => {}
                                        Err(_) => {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                "PAUSE",
                                                "Error: PAUSE cancelled.",
                                            );
                                            command_error = true;
                                        }
                                    }
                                }
                                ToolCommand::Accept { name, prompt } => {
                                    let prompt_text = prompt
                                        .unwrap_or_else(|| format!("Enter value for {}:", name));
                                    match SqlEditorWidget::prompt_for_input_with_sender(
                                        &sender,
                                        &prompt_text,
                                    ) {
                                        Ok(value) => {
                                            let key = SessionState::normalize_name(&name);
                                            match session.lock() {
                                                Ok(mut guard) => {
                                                    guard
                                                        .define_vars
                                                        .insert(key.clone(), value.clone());
                                                }
                                                Err(poisoned) => {
                                                    eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                    let mut guard = poisoned.into_inner();
                                                    guard
                                                        .define_vars
                                                        .insert(key.clone(), value.clone());
                                                }
                                            }
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                &format!("ACCEPT {}", key),
                                                &format!("Value assigned to {}", key),
                                            );
                                        }
                                        Err(message) => {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                &format!("ACCEPT {}", name),
                                                &format!("Error: {}", message),
                                            );
                                            command_error = true;
                                        }
                                    }
                                }
                                ToolCommand::Define { name, value } => {
                                    let (define_enabled, scan_enabled) = match session.lock() {
                                        Ok(guard) => (guard.define_enabled, guard.scan_enabled),
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let guard = poisoned.into_inner();
                                            (guard.define_enabled, guard.scan_enabled)
                                        }
                                    };
                                    let mut resolved_value = value;
                                    if define_enabled && scan_enabled {
                                        match SqlEditorWidget::apply_define_substitution(
                                            &resolved_value,
                                            &session,
                                            &sender,
                                        ) {
                                            Ok(updated) => {
                                                resolved_value = updated;
                                            }
                                            Err(message) => {
                                                SqlEditorWidget::emit_script_message(
                                                    &sender,
                                                    &session,
                                                    &format!("DEFINE {}", name),
                                                    &format!("Error: {}", message),
                                                );
                                                command_error = true;
                                            }
                                        }
                                    }
                                    let key = SessionState::normalize_name(&name);
                                    if !command_error {
                                        match session.lock() {
                                            Ok(mut guard) => {
                                                guard
                                                    .define_vars
                                                    .insert(key.clone(), resolved_value.clone());
                                            }
                                            Err(poisoned) => {
                                                eprintln!(
                                                "Warning: session state lock was poisoned; recovering."
                                            );
                                                let mut guard = poisoned.into_inner();
                                                guard
                                                    .define_vars
                                                    .insert(key.clone(), resolved_value.clone());
                                            }
                                        }
                                        SqlEditorWidget::emit_script_message(
                                            &sender,
                                            &session,
                                            &format!("DEFINE {}", key),
                                            &format!("Defined {} = {}", key, resolved_value),
                                        );
                                    }
                                }
                                ToolCommand::Undefine { name } => {
                                    let key = SessionState::normalize_name(&name);
                                    match session.lock() {
                                        Ok(mut guard) => {
                                            guard.define_vars.remove(&key);
                                        }
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let mut guard = poisoned.into_inner();
                                            guard.define_vars.remove(&key);
                                        }
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        &format!("UNDEFINE {}", key),
                                        &format!("Undefined {}", key),
                                    );
                                }
                                ToolCommand::ColumnNewValue {
                                    column_name,
                                    variable_name,
                                } => {
                                    let column_key = SessionState::normalize_name(&column_name);
                                    let variable_key = SessionState::normalize_name(&variable_name);
                                    match session.lock() {
                                        Ok(mut guard) => {
                                            guard
                                                .column_new_values
                                                .insert(column_key.clone(), variable_key.clone());
                                        }
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let mut guard = poisoned.into_inner();
                                            guard
                                                .column_new_values
                                                .insert(column_key.clone(), variable_key.clone());
                                        }
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        &format!(
                                            "COLUMN {} NEW_VALUE {}",
                                            column_key, variable_key
                                        ),
                                        &format!(
                                            "Registered NEW_VALUE mapping: {} -> {}",
                                            column_key, variable_key
                                        ),
                                    );
                                }
                                ToolCommand::BreakOn { column_name } => {
                                    let key = SessionState::normalize_name(&column_name);
                                    match session.lock() {
                                        Ok(mut guard) => {
                                            guard.break_column = Some(key.clone());
                                        }
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let mut guard = poisoned.into_inner();
                                            guard.break_column = Some(key.clone());
                                        }
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "BREAK",
                                        &format!("BREAK ON {}", key),
                                    );
                                }
                                ToolCommand::BreakOff => {
                                    match session.lock() {
                                        Ok(mut guard) => {
                                            guard.break_column = None;
                                        }
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let mut guard = poisoned.into_inner();
                                            guard.break_column = None;
                                        }
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "BREAK",
                                        "BREAK OFF",
                                    );
                                }
                                ToolCommand::ClearBreaks => {
                                    match session.lock() {
                                        Ok(mut guard) => {
                                            guard.break_column = None;
                                        }
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let mut guard = poisoned.into_inner();
                                            guard.break_column = None;
                                        }
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "CLEAR",
                                        "BREAKS cleared",
                                    );
                                }
                                ToolCommand::ClearComputes => {
                                    match session.lock() {
                                        Ok(mut guard) => {
                                            guard.compute = None;
                                        }
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let mut guard = poisoned.into_inner();
                                            guard.compute = None;
                                        }
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "CLEAR",
                                        "COMPUTES cleared",
                                    );
                                }
                                ToolCommand::ClearBreaksComputes => {
                                    match session.lock() {
                                        Ok(mut guard) => {
                                            guard.break_column = None;
                                            guard.compute = None;
                                        }
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let mut guard = poisoned.into_inner();
                                            guard.break_column = None;
                                            guard.compute = None;
                                        }
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "CLEAR",
                                        "BREAKS and COMPUTES cleared",
                                    );
                                }
                                ToolCommand::Compute {
                                    mode,
                                    of_column,
                                    on_column,
                                } => {
                                    match session.lock() {
                                        Ok(mut guard) => {
                                            guard.compute = Some(crate::db::ComputeConfig {
                                                mode,
                                                of_column: of_column.clone(),
                                                on_column: on_column.clone(),
                                            });
                                        }
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let mut guard = poisoned.into_inner();
                                            guard.compute = Some(crate::db::ComputeConfig {
                                                mode,
                                                of_column: of_column.clone(),
                                                on_column: on_column.clone(),
                                            });
                                        }
                                    }
                                    let mode_text = match mode {
                                        crate::db::ComputeMode::Sum => "COMPUTE SUM",
                                        crate::db::ComputeMode::Count => "COMPUTE COUNT",
                                    };
                                    let label = match (of_column.as_deref(), on_column.as_deref()) {
                                        (Some(of_col), Some(on_col)) => {
                                            format!("{} OF {} ON {}", mode_text, of_col, on_col)
                                        }
                                        _ => mode_text.to_string(),
                                    };
                                    SqlEditorWidget::emit_script_message(
                                        &sender, &session, "COMPUTE", &label,
                                    );
                                }
                                ToolCommand::ComputeOff => {
                                    match session.lock() {
                                        Ok(mut guard) => {
                                            guard.compute = None;
                                        }
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            let mut guard = poisoned.into_inner();
                                            guard.compute = None;
                                        }
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "COMPUTE",
                                        "COMPUTE OFF",
                                    );
                                }
                                ToolCommand::SetErrorContinue { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.continue_on_error = enabled;
                                    }
                                    continue_on_error = enabled;

                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET ERRORCONTINUE",
                                        &format!(
                                            "ERRORCONTINUE {}",
                                            if enabled { "ON" } else { "OFF" }
                                        ),
                                    );
                                }
                                ToolCommand::SetAutoCommit { enabled } => {
                                    {
                                        let mut conn_guard = lock_connection_with_activity(
                                            &shared_connection,
                                            db_activity.clone(),
                                        );
                                        conn_guard.set_auto_commit(enabled);
                                    }
                                    auto_commit = enabled;
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET AUTOCOMMIT",
                                        if enabled {
                                            "Auto-commit enabled"
                                        } else {
                                            "Auto-commit disabled"
                                        },
                                    );
                                    let _ =
                                        sender.send(QueryProgress::AutoCommitChanged { enabled });
                                    app::awake();
                                }
                                ToolCommand::SetDefine {
                                    enabled,
                                    define_char,
                                } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.define_enabled = enabled;
                                        if let Some(ch) = define_char {
                                            guard.define_char = ch;
                                        }
                                    }
                                    let msg = if let Some(ch) = define_char {
                                        format!("DEFINE '{}'", ch)
                                    } else {
                                        format!("DEFINE {}", if enabled { "ON" } else { "OFF" })
                                    };
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET DEFINE",
                                        &msg,
                                    );
                                }
                                ToolCommand::SetScan { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.scan_enabled = enabled;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET SCAN",
                                        &format!("SCAN {}", if enabled { "ON" } else { "OFF" }),
                                    );
                                }
                                ToolCommand::SetVerify { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.verify_enabled = enabled;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET VERIFY",
                                        &format!("VERIFY {}", if enabled { "ON" } else { "OFF" }),
                                    );
                                }
                                ToolCommand::SetEcho { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.echo_enabled = enabled;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET ECHO",
                                        &format!("ECHO {}", if enabled { "ON" } else { "OFF" }),
                                    );
                                }
                                ToolCommand::SetTiming { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.timing_enabled = enabled;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET TIMING",
                                        &format!("TIMING {}", if enabled { "ON" } else { "OFF" }),
                                    );
                                }
                                ToolCommand::SetFeedback { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.feedback_enabled = enabled;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET FEEDBACK",
                                        &format!("FEEDBACK {}", if enabled { "ON" } else { "OFF" }),
                                    );
                                }
                                ToolCommand::SetHeading { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.heading_enabled = enabled;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET HEADING",
                                        &format!("HEADING {}", if enabled { "ON" } else { "OFF" }),
                                    );
                                }
                                ToolCommand::SetPageSize { size } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.pagesize = size;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET PAGESIZE",
                                        &format!("PAGESIZE {}", size),
                                    );
                                }
                                ToolCommand::SetLineSize { size } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.linesize = size;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET LINESIZE",
                                        &format!("LINESIZE {}", size),
                                    );
                                }
                                ToolCommand::SetTrimSpool { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.trimspool_enabled = enabled;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET TRIMSPOOL",
                                        &format!(
                                            "TRIMSPOOL {}",
                                            if enabled { "ON" } else { "OFF" }
                                        ),
                                    );
                                }
                                ToolCommand::SetTrimOut { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.trimout_enabled = enabled;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET TRIMOUT",
                                        &format!("TRIMOUT {}", if enabled { "ON" } else { "OFF" }),
                                    );
                                }
                                ToolCommand::SetSqlBlankLines { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.sqlblanklines_enabled = enabled;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET SQLBLANKLINES",
                                        &format!(
                                            "SQLBLANKLINES {}",
                                            if enabled { "ON" } else { "OFF" }
                                        ),
                                    );
                                }
                                ToolCommand::SetTab { enabled } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.tab_enabled = enabled;
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET TAB",
                                        &format!("TAB {}", if enabled { "ON" } else { "OFF" }),
                                    );
                                }
                                ToolCommand::SetColSep { separator } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.colsep = separator.clone();
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET COLSEP",
                                        &format!("COLSEP {}", separator),
                                    );
                                }
                                ToolCommand::SetNull { null_text } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.null_text = null_text.clone();
                                    }
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "SET NULL",
                                        &format!("NULL {}", null_text),
                                    );
                                }
                                ToolCommand::Spool { path, append } => match path {
                                    Some(path) => {
                                        let target_path = if Path::new(&path).is_absolute() {
                                            PathBuf::from(&path)
                                        } else {
                                            current_frame_base_dir.join(&path)
                                        };
                                        match session.lock() {
                                            Ok(mut guard) => {
                                                guard.spool_path = Some(target_path.clone());
                                                guard.spool_truncate = !append;
                                            }
                                            Err(poisoned) => {
                                                eprintln!(
                                                "Warning: session state lock was poisoned; recovering."
                                            );
                                                let mut guard = poisoned.into_inner();
                                                guard.spool_path = Some(target_path.clone());
                                                guard.spool_truncate = !append;
                                            }
                                        }
                                        SqlEditorWidget::emit_script_message(
                                            &sender,
                                            &session,
                                            "SPOOL",
                                            &format!(
                                                "Spooling output to {} ({})",
                                                target_path.display(),
                                                if append { "append" } else { "replace" }
                                            ),
                                        );
                                    }
                                    None if append => {
                                        let has_spool_target = match session.lock() {
                                            Ok(mut guard) => {
                                                let has_target = guard.spool_path.is_some();
                                                guard.spool_truncate = false;
                                                has_target
                                            }
                                            Err(poisoned) => {
                                                eprintln!(
                                                "Warning: session state lock was poisoned; recovering."
                                            );
                                                let mut guard = poisoned.into_inner();
                                                let has_target = guard.spool_path.is_some();
                                                guard.spool_truncate = false;
                                                has_target
                                            }
                                        };
                                        if has_spool_target {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                "SPOOL",
                                                "Spooling in append mode",
                                            );
                                        } else {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                "SPOOL APPEND",
                                                "Error: No active spool file. Use SPOOL <file> APPEND.",
                                            );
                                            command_error = true;
                                        }
                                    }
                                    None => {
                                        match session.lock() {
                                            Ok(mut guard) => {
                                                guard.spool_path = None;
                                                guard.spool_truncate = false;
                                            }
                                            Err(poisoned) => {
                                                eprintln!(
                                                "Warning: session state lock was poisoned; recovering."
                                            );
                                                let mut guard = poisoned.into_inner();
                                                guard.spool_path = None;
                                                guard.spool_truncate = false;
                                            }
                                        }
                                        SqlEditorWidget::emit_script_message(
                                            &sender,
                                            &session,
                                            "SPOOL",
                                            "Spooling disabled",
                                        );
                                    }
                                },
                                ToolCommand::WheneverSqlError { exit, action } => {
                                    if exit
                                        && action
                                            .as_deref()
                                            .map(|v| v.trim().eq_ignore_ascii_case("SQL.SQLCODE"))
                                            .unwrap_or(false)
                                        && !script_mode
                                    {
                                        SqlEditorWidget::emit_script_message(
                                            &sender,
                                            &session,
                                            "WHENEVER SQLERROR",
                                            "Error: EXIT SQL.SQLCODE is supported only in batch(script) execution.",
                                        );
                                        command_error = true;
                                    } else {
                                        {
                                            let mut guard = match session.lock() {
                                                Ok(guard) => guard,
                                                Err(poisoned) => {
                                                    eprintln!(
                                                        "Warning: session state lock was poisoned; recovering."
                                                    );
                                                    poisoned.into_inner()
                                                }
                                            };
                                            guard.continue_on_error = !exit;
                                        }
                                        continue_on_error = !exit;
                                        SqlEditorWidget::emit_script_message(
                                            &sender,
                                            &session,
                                            "WHENEVER SQLERROR",
                                            if exit { "Mode EXIT" } else { "Mode CONTINUE" },
                                        );
                                    }
                                }
                                ToolCommand::WheneverOsError { exit } => {
                                    {
                                        let mut guard = match session.lock() {
                                            Ok(guard) => guard,
                                            Err(poisoned) => {
                                                eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                poisoned.into_inner()
                                            }
                                        };
                                        guard.continue_on_error = !exit;
                                    }
                                    continue_on_error = !exit;
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "WHENEVER OSERROR",
                                        if exit { "Mode EXIT" } else { "Mode CONTINUE" },
                                    );
                                }
                                ToolCommand::Exit => {
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "EXIT",
                                        "Execution stopped.",
                                    );
                                    stop_execution = true;
                                }
                                ToolCommand::Quit => {
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "QUIT",
                                        "Execution stopped.",
                                    );
                                    stop_execution = true;
                                }
                                ToolCommand::Connect {
                                    username,
                                    password,
                                    host,
                                    port,
                                    service_name,
                                } => {
                                    let connection_name_target = if host.trim().is_empty() {
                                        service_name.clone()
                                    } else {
                                        host.clone()
                                    };
                                    let conn_info = ConnectionInfo {
                                        name: format!("{}@{}", username, connection_name_target),
                                        username,
                                        password,
                                        host,
                                        port,
                                        service_name,
                                        db_type: crate::db::DatabaseType::Oracle,
                                        advanced: crate::db::ConnectionAdvancedSettings::default_for(
                                            crate::db::DatabaseType::Oracle,
                                        ),
                                    };

                                    let connect_result = {
                                        let mut conn_guard = lock_connection_with_activity(
                                            &shared_connection,
                                            db_activity.clone(),
                                        );
                                        match conn_guard.connect(conn_info.clone()) {
                                            Ok(_) => {
                                                conn_guard.refresh_tracked_connection();
                                                let conn_opt_local = conn_guard.get_connection();
                                                let sanitized =
                                                    SqlEditorWidget::connection_info_for_ui(
                                                        conn_guard.get_info(),
                                                    );
                                                let conn_name_local = if conn_guard.is_connected() {
                                                    conn_guard.get_info().name.clone()
                                                } else {
                                                    String::new()
                                                };
                                                Ok((conn_opt_local, sanitized, conn_name_local))
                                            }
                                            Err(err) => Err(err),
                                        }
                                    };

                                    match connect_result {
                                        Ok((
                                            next_conn_opt,
                                            sanitized_conn_info,
                                            next_conn_name,
                                        )) => {
                                            crate::db::clear_pooled_session_lease(&pooled_db_session);
                                            cleanup.clear_oracle_pooled_session_tracking();
                                            cleanup.clear_oracle_read_only_transaction_tracking();
                                            conn_opt = next_conn_opt;
                                            conn_name = next_conn_name;
                                            // Update cancel connection so break_execution() uses the new connection
                                            if let Some(ref conn) = conn_opt {
                                                SqlEditorWidget::set_current_query_connection(
                                                    &current_query_connection,
                                                    Some(Arc::clone(conn)),
                                                );
                                            }
                                            match session.lock() {
                                                Ok(mut guard) => guard.reset(),
                                                Err(poisoned) => {
                                                    eprintln!(
                                                    "Warning: session state lock was poisoned; recovering."
                                                );
                                                    poisoned.into_inner().reset();
                                                }
                                            }
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                "CONNECT",
                                                &format!("Connected to {}", conn_info.name),
                                            );
                                            if let Some(conn) = conn_opt.as_ref() {
                                                let previous_timeout =
                                                    conn.call_timeout().ok().flatten();
                                                cleanup.track_timeout(
                                                    Arc::clone(conn),
                                                    previous_timeout,
                                                );
                                                if let Err(err) =
                                                    conn.set_call_timeout(query_timeout)
                                                {
                                                    SqlEditorWidget::emit_script_message(
                                                        &sender,
                                                        &session,
                                                        "CONNECT",
                                                        &format!(
                                                            "Error: Failed to apply query timeout after CONNECT: {}",
                                                            err
                                                        ),
                                                    );
                                                    command_error = true;
                                                }
                                                match crate::db::DatabaseConnection::apply_oracle_transaction_mode(
                                                    conn.as_ref(),
                                                    transaction_mode,
                                                ) {
                                                    Ok(()) => {
                                                        if transaction_mode.access_mode
                                                            == crate::db::TransactionAccessMode::ReadOnly
                                                        {
                                                            cleanup.track_oracle_read_only_transaction(
                                                                Arc::clone(conn),
                                                            );
                                                        }
                                                    }
                                                    Err(err) => {
                                                        SqlEditorWidget::emit_script_message(
                                                            &sender,
                                                            &session,
                                                            "CONNECT",
                                                            &format!(
                                                                "Error: Failed to apply transaction mode after CONNECT: {}",
                                                                err
                                                            ),
                                                        );
                                                        command_error = true;
                                                    }
                                                }
                                                if !requires_transaction_first_statement {
                                                    if let Err(err) =
                                                        SqlEditorWidget::sync_serveroutput_with_session(
                                                            conn.as_ref(),
                                                            &session,
                                                        )
                                                    {
                                                        eprintln!(
                                                            "Failed to apply SERVEROUTPUT after CONNECT: {err}"
                                                        );
                                                    }
                                                }
                                            }
                                            let _ = sender.send(QueryProgress::ConnectionChanged {
                                                info: Some(sanitized_conn_info),
                                            });
                                            app::awake();
                                        }
                                        Err(err) => {
                                            let (
                                                preserved_conn_opt,
                                                preserved_conn_name,
                                                preserved_conn_info,
                                            ) = {
                                                let conn_guard = lock_connection_with_activity(
                                                    &shared_connection,
                                                    db_activity.clone(),
                                                );
                                                if conn_guard.is_connected()
                                                    && conn_guard.has_connection_handle()
                                                {
                                                    (
                                                        conn_guard.get_connection(),
                                                        conn_guard.get_info().name.clone(),
                                                        Some(
                                                            SqlEditorWidget::connection_info_for_ui(
                                                                conn_guard.get_info(),
                                                            ),
                                                        ),
                                                    )
                                                } else {
                                                    (None, String::new(), None)
                                                }
                                            };
                                            conn_opt = preserved_conn_opt;
                                            conn_name = preserved_conn_name;
                                            SqlEditorWidget::set_current_query_connection(
                                                &current_query_connection,
                                                conn_opt.as_ref().map(Arc::clone),
                                            );
                                            let error_msg = format!("Connection failed: {}", err);
                                            SqlEditorWidget::emit_script_message(
                                                &sender, &session, "CONNECT", &error_msg,
                                            );
                                            let _ = sender.send(QueryProgress::ConnectionChanged {
                                                info: preserved_conn_info,
                                            });
                                            app::awake();
                                            command_error = true;
                                        }
                                    }
                                }
                                ToolCommand::Disconnect => {
                                    // Treat stale handles (connection exists but connected flag is false)
                                    // as a disconnectable state so UI/session state is fully reset.
                                    let (had_connection, next_conn_opt, next_conn_name) = {
                                        let mut conn_guard = lock_connection_with_activity(
                                            &shared_connection,
                                            db_activity.clone(),
                                        );
                                        let had_connection = conn_guard.is_connected()
                                            || conn_guard.has_connection_handle();
                                        conn_guard.disconnect();
                                        conn_guard.refresh_tracked_connection();
                                        let next_conn_opt = conn_guard.get_connection();
                                        let next_conn_name = if conn_guard.is_connected() {
                                            conn_guard.get_info().name.clone()
                                        } else {
                                            String::new()
                                        };
                                        (had_connection, next_conn_opt, next_conn_name)
                                    };
                                    crate::db::clear_pooled_session_lease(&pooled_db_session);

                                    // Clear cancel connection before disconnect
                                    SqlEditorWidget::set_current_query_connection(
                                        &current_query_connection,
                                        None,
                                    );
                                    conn_opt = next_conn_opt;
                                    conn_name = next_conn_name;
                                    match session.lock() {
                                        Ok(mut guard) => guard.reset(),
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            poisoned.into_inner().reset();
                                        }
                                    }
                                    let disconnect_message = if had_connection {
                                        "Disconnected from database"
                                    } else {
                                        "Not connected to any database"
                                    };
                                    SqlEditorWidget::emit_script_message(
                                        &sender,
                                        &session,
                                        "DISCONNECT",
                                        disconnect_message,
                                    );
                                    cleanup.clear_timeout_tracking();
                                    cleanup.clear_oracle_pooled_session_tracking();
                                    let _ = sender
                                        .send(QueryProgress::ConnectionChanged { info: None });
                                    app::awake();
                                }
                                ToolCommand::RunScript {
                                    path,
                                    relative_to_caller,
                                } => {
                                    let include_base_dir = if relative_to_caller {
                                        current_frame_base_dir.as_path()
                                    } else {
                                        working_dir.as_path()
                                    };
                                    let (target_path, normalized_target_path) =
                                        SqlEditorWidget::resolve_script_include_path(
                                            &path,
                                            relative_to_caller,
                                            current_frame_base_dir.as_path(),
                                            working_dir.as_path(),
                                        );
                                    if let Err(message) =
                                        SqlEditorWidget::validate_script_include_target(
                                            &frames,
                                            normalized_target_path.as_path(),
                                        )
                                    {
                                        SqlEditorWidget::emit_script_message(
                                            &sender,
                                            &session,
                                            if relative_to_caller { "@@" } else { "@" },
                                            &format!("Error: {}", message),
                                        );
                                        if !continue_on_error {
                                            stop_execution = true;
                                        }
                                        continue;
                                    }
                                    match SqlEditorWidget::load_script_include(
                                        target_path.as_path(),
                                        normalized_target_path.as_path(),
                                        include_base_dir,
                                        Some(crate::db::connection::DatabaseType::Oracle),
                                        None,
                                    ) {
                                        Ok(resolved_include) => {
                                            frames.push(ScriptExecutionFrame {
                                                items: resolved_include.items,
                                                index: 0,
                                                base_dir: resolved_include.script_dir,
                                                source_path: Some(resolved_include.source_path),
                                            });
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                if relative_to_caller { "@@" } else { "@" },
                                                &format!(
                                                    "Running script {}",
                                                    target_path.display()
                                                ),
                                            );
                                        }
                                        Err(message) => {
                                            SqlEditorWidget::emit_script_message(
                                                &sender,
                                                &session,
                                                if relative_to_caller { "@@" } else { "@" },
                                                &format!("Error: {}", message),
                                            );
                                            command_error = true;
                                        }
                                    }
                                }
                                ToolCommand::Unsupported {
                                    raw,
                                    message,
                                    is_error,
                                } => {
                                    if is_error {
                                        SqlEditorWidget::emit_script_message(
                                            &sender,
                                            &session,
                                            &raw,
                                            &format!("Error: {}", message),
                                        );
                                        command_error = true;
                                    } else {
                                        SqlEditorWidget::emit_script_message(
                                            &sender,
                                            &session,
                                            &raw,
                                            &format!("Warning: {}", message),
                                        );
                                    }
                                }
                                // MySQL USE command — switch database and refresh metadata
                                ToolCommand::Use { ref database } => {
                                    let use_result = {
                                        let mut cg = lock_connection_with_activity(
                                            &shared_connection,
                                            db_activity.clone(),
                                        );
                                        if let Some(mysql_conn) = cg.get_mysql_connection_mut() {
                                            let use_sql = SqlEditorWidget::format_tool_command(
                                                &ToolCommand::Use {
                                                    database: database.clone(),
                                                },
                                            );
                                            match crate::db::query::mysql_executor::MysqlExecutor::execute(mysql_conn, &use_sql) {
                                                Ok(_) => Ok(()),
                                                Err(e) => Err(format!("Error: {}", e)),
                                            }
                                        } else {
                                            Err("Error: USE command is only supported for MySQL/MariaDB connections".to_string())
                                        }
                                    };
                                    match use_result {
                                        Ok(()) => {
                                            let info =
                                                SqlEditorWidget::sync_mysql_connection_info_for_ui(
                                                    &shared_connection,
                                                    &db_activity,
                                                );
                                            SqlEditorWidget::emit_script_output(
                                                &sender,
                                                &session,
                                                vec![format!("Database changed to '{}'", database)],
                                            );
                                            if let Some(info) = info {
                                                let _ =
                                                    sender.send(QueryProgress::ConnectionChanged {
                                                        info: Some(info),
                                                    });
                                            } else {
                                                let _ = sender
                                                    .send(QueryProgress::MetadataRefreshNeeded);
                                            }
                                            app::awake();
                                        }
                                        Err(msg) => {
                                            SqlEditorWidget::emit_script_message(
                                                &sender, &session, "USE", &msg,
                                            );
                                            command_error = true;
                                        }
                                    }
                                }
                                // MySQL-specific commands — execute as raw SQL via the connection
                                ToolCommand::ShowDatabases
                                | ToolCommand::ShowTables
                                | ToolCommand::ShowColumns { .. }
                                | ToolCommand::ShowCreateTable { .. }
                                | ToolCommand::ShowProcessList
                                | ToolCommand::ShowVariables { .. }
                                | ToolCommand::ShowStatus { .. }
                                | ToolCommand::ShowWarnings
                                | ToolCommand::MysqlShowErrors
                                | ToolCommand::MysqlSource { .. } => {
                                    // These are handled as regular SQL statements
                                    // for MySQL connections; no special script handling needed.
                                }
                                ToolCommand::MysqlDelimiter { ref delimiter } => {
                                    match session.lock() {
                                        Ok(mut guard) => {
                                            if delimiter == ";" {
                                                guard.mysql_delimiter = None;
                                            } else {
                                                guard.mysql_delimiter = Some(delimiter.clone());
                                            }
                                        }
                                        Err(poisoned) => {
                                            let mut guard = poisoned.into_inner();
                                            if delimiter == ";" {
                                                guard.mysql_delimiter = None;
                                            } else {
                                                guard.mysql_delimiter = Some(delimiter.clone());
                                            }
                                        }
                                    }
                                    SqlEditorWidget::emit_script_output(
                                        &sender,
                                        &session,
                                        vec![format!("Delimiter set to '{}'", delimiter)],
                                    );
                                }
                            }

                            if command_error && !continue_on_error {
                                stop_execution = true;
                            }
                        }
                        ScriptItem::Statement(statement) => {
                            // For statements, we need a connection
                            let conn = match conn_opt.as_ref() {
                                Some(c) => c,
                                None => {
                                    // This shouldn't happen as we checked earlier
                                    eprintln!(
                                        "Error: No connection available for statement execution"
                                    );
                                    let emitted = SqlEditorWidget::emit_non_select_result(
                                        &sender,
                                        &session,
                                        &conn_name,
                                        result_index,
                                        &statement,
                                        "Error: Not connected to database".to_string(),
                                        false,
                                        false,
                                        script_mode,
                                    );
                                    if emitted {
                                        result_index += 1;
                                    }
                                    stop_execution = true;
                                    continue;
                                }
                            };

                            let trimmed = statement.trim_start_matches(';').trim();
                            if trimmed.is_empty() {
                                continue;
                            }

                            let mut sql_text = trimmed.to_string();
                            let (define_enabled, scan_enabled, verify_enabled) =
                                match session.lock() {
                                    Ok(guard) => (
                                        guard.define_enabled,
                                        guard.scan_enabled,
                                        guard.verify_enabled,
                                    ),
                                    Err(poisoned) => {
                                        eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                        let guard = poisoned.into_inner();
                                        (
                                            guard.define_enabled,
                                            guard.scan_enabled,
                                            guard.verify_enabled,
                                        )
                                    }
                                };
                            if define_enabled && scan_enabled {
                                let sql_before = sql_text.clone();
                                match SqlEditorWidget::apply_define_substitution(
                                    &sql_text, &session, &sender,
                                ) {
                                    Ok(updated) => {
                                        if verify_enabled && updated != sql_before {
                                            SqlEditorWidget::emit_script_output(
                                                &sender,
                                                &session,
                                                vec![
                                                    format!("old: {}", sql_before),
                                                    format!("new: {}", updated),
                                                ],
                                            );
                                        }
                                        sql_text = updated;
                                    }
                                    Err(message) => {
                                        let emitted = SqlEditorWidget::emit_non_select_result(
                                            &sender,
                                            &session,
                                            &conn_name,
                                            result_index,
                                            trimmed,
                                            format!("Error: {}", message),
                                            false,
                                            false,
                                            script_mode,
                                        );
                                        if emitted {
                                            result_index += 1;
                                        }
                                        if !continue_on_error {
                                            stop_execution = true;
                                        }
                                        continue;
                                    }
                                }
                            }

                            let cleaned = SqlEditorWidget::strip_leading_comments(&sql_text);
                            let upper = cleaned.to_uppercase();

                            if transaction_mode.access_mode
                                == crate::db::TransactionAccessMode::ReadOnly
                                && !SqlEditorWidget::oracle_read_only_allows_statement(&sql_text)
                            {
                                let message = SqlEditorWidget::oracle_read_only_block_message();
                                let emitted = SqlEditorWidget::emit_non_select_result(
                                    &sender,
                                    &session,
                                    &conn_name,
                                    result_index,
                                    &sql_text,
                                    message,
                                    false,
                                    false,
                                    script_mode,
                                );
                                if emitted {
                                    result_index += 1;
                                }
                                if !continue_on_error {
                                    stop_execution = true;
                                }
                                continue;
                            }

                            if QueryExecutor::is_plain_commit(&sql_text) {
                                let mut timed_out = false;
                                let statement_start = Instant::now();
                                let mut result = match conn.commit() {
                                    Ok(()) => QueryResult {
                                        sql: sql_text.to_string(),
                                        columns: vec![],
                                        rows: vec![],
                                        row_count: 0,
                                        execution_time: Duration::from_secs(0),
                                        message: "Commit complete".to_string(),
                                        is_select: false,
                                        success: true,
                                    },
                                    Err(err) => {
                                        timed_out = SqlEditorWidget::is_timeout_error(&err);
                                        SqlEditorWidget::invalidate_oracle_pooled_session_after_error(
                                            &mut cleanup,
                                            &err,
                                        );
                                        QueryResult::new_error(&sql_text, &err.to_string())
                                    }
                                };
                                let timing_duration = statement_start.elapsed();
                                result.execution_time = timing_duration;
                                let result_success = result.success;
                                if script_mode {
                                    if result_success {
                                        SqlEditorWidget::emit_script_lines(
                                            &sender,
                                            &session,
                                            &result.message,
                                        );
                                    }
                                    SqlEditorWidget::emit_script_result(
                                        &sender,
                                        &conn_name,
                                        result_index,
                                        result,
                                        timed_out,
                                    );
                                } else {
                                    let index = result_index;
                                    let _ = sender.send(QueryProgress::StatementStart { index });
                                    app::awake();
                                    if !result.message.trim().is_empty() {
                                        SqlEditorWidget::append_spool_output(
                                            &session,
                                            std::slice::from_ref(&result.message),
                                        );
                                    }
                                    let _ = sender.send(QueryProgress::StatementFinished {
                                        index,
                                        result,
                                        connection_name: conn_name.clone(),
                                        timed_out,
                                    });
                                    app::awake();
                                    result_index += 1;
                                }
                                SqlEditorWidget::emit_timing_if_enabled(
                                    &sender,
                                    &session,
                                    timing_duration,
                                );
                                if load_mutex_bool(&cancel_flag)
                                    || timed_out
                                    || (!result_success && !continue_on_error)
                                {
                                    stop_execution = true;
                                }
                                continue;
                            }

                            if QueryExecutor::is_plain_rollback(&sql_text) {
                                let mut timed_out = false;
                                let statement_start = Instant::now();
                                let mut result = match conn.rollback() {
                                    Ok(()) => QueryResult {
                                        sql: sql_text.to_string(),
                                        columns: vec![],
                                        rows: vec![],
                                        row_count: 0,
                                        execution_time: Duration::from_secs(0),
                                        message: "Rollback complete".to_string(),
                                        is_select: false,
                                        success: true,
                                    },
                                    Err(err) => {
                                        timed_out = SqlEditorWidget::is_timeout_error(&err);
                                        SqlEditorWidget::invalidate_oracle_pooled_session_after_error(
                                            &mut cleanup,
                                            &err,
                                        );
                                        QueryResult::new_error(&sql_text, &err.to_string())
                                    }
                                };
                                let timing_duration = statement_start.elapsed();
                                result.execution_time = timing_duration;
                                let result_success = result.success;
                                if script_mode {
                                    if result_success {
                                        SqlEditorWidget::emit_script_lines(
                                            &sender,
                                            &session,
                                            &result.message,
                                        );
                                    }
                                    SqlEditorWidget::emit_script_result(
                                        &sender,
                                        &conn_name,
                                        result_index,
                                        result,
                                        timed_out,
                                    );
                                } else {
                                    let index = result_index;
                                    let _ = sender.send(QueryProgress::StatementStart { index });
                                    app::awake();
                                    if !result.message.trim().is_empty() {
                                        SqlEditorWidget::append_spool_output(
                                            &session,
                                            std::slice::from_ref(&result.message),
                                        );
                                    }
                                    let _ = sender.send(QueryProgress::StatementFinished {
                                        index,
                                        result,
                                        connection_name: conn_name.clone(),
                                        timed_out,
                                    });
                                    app::awake();
                                    result_index += 1;
                                }
                                SqlEditorWidget::emit_timing_if_enabled(
                                    &sender,
                                    &session,
                                    timing_duration,
                                );
                                if load_mutex_bool(&cancel_flag)
                                    || timed_out
                                    || (!result_success && !continue_on_error)
                                {
                                    stop_execution = true;
                                }
                                continue;
                            }

                            let compiled_object = QueryExecutor::parse_compiled_object(&sql_text);
                            let is_compiled_plsql = compiled_object.is_some();
                            if let Some(object) = compiled_object.clone() {
                                let mut guard = match session.lock() {
                                    Ok(guard) => guard,
                                    Err(poisoned) => {
                                        eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                        poisoned.into_inner()
                                    }
                                };
                                guard.last_compiled = Some(object);
                            }

                            let exec_call = QueryExecutor::normalize_exec_call(&sql_text);
                            if exec_call.is_some() {
                                if let Err(message) =
                                    QueryExecutor::check_named_positional_mix(&sql_text)
                                {
                                    let emitted = SqlEditorWidget::emit_non_select_result(
                                        &sender,
                                        &session,
                                        &conn_name,
                                        result_index,
                                        &sql_text,
                                        format!("Error: {}", message),
                                        false,
                                        false,
                                        script_mode,
                                    );
                                    if emitted {
                                        result_index += 1;
                                    }
                                    if !continue_on_error {
                                        stop_execution = true;
                                    }
                                    continue;
                                }
                            }

                            let is_plsql_block =
                                upper.starts_with("BEGIN") || upper.starts_with("DECLARE");
                            let is_select = QueryExecutor::is_select_statement(&sql_text);

                            if exec_call.is_some() || is_plsql_block {
                                let mut sql_to_execute =
                                    exec_call.unwrap_or_else(|| sql_text.to_string());
                                if is_plsql_block {
                                    sql_to_execute =
                                        SqlEditorWidget::ensure_plsql_terminator(&sql_to_execute);
                                }
                                let binds = match session.lock() {
                                    Ok(guard) => {
                                        QueryExecutor::resolve_binds(&sql_to_execute, &guard)
                                    }
                                    Err(poisoned) => {
                                        eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                        QueryExecutor::resolve_binds(
                                            &sql_to_execute,
                                            &poisoned.into_inner(),
                                        )
                                    }
                                };

                                let binds = match binds {
                                    Ok(binds) => binds,
                                    Err(message) => {
                                        let emitted = SqlEditorWidget::emit_non_select_result(
                                            &sender,
                                            &session,
                                            &conn_name,
                                            result_index,
                                            &sql_text,
                                            format!("Error: {}", message),
                                            false,
                                            false,
                                            script_mode,
                                        );
                                        if emitted {
                                            result_index += 1;
                                        }
                                        if !continue_on_error {
                                            stop_execution = true;
                                        }
                                        continue;
                                    }
                                };

                                let statement_start = Instant::now();
                                let mut timed_out = false;
                                let stmt = match QueryExecutor::execute_with_binds(
                                    conn.as_ref(),
                                    &sql_to_execute,
                                    &binds,
                                ) {
                                    Ok(stmt) => stmt,
                                    Err(err) => {
                                        let cancelled = SqlEditorWidget::is_cancel_error(&err);
                                        timed_out = SqlEditorWidget::is_timeout_error(&err);
                                        SqlEditorWidget::invalidate_oracle_pooled_session_after_error(
                                            &mut cleanup,
                                            &err,
                                        );
                                        let message =
                                            SqlEditorWidget::choose_execution_error_message(
                                                cancelled,
                                                timed_out,
                                                query_timeout,
                                                err.to_string(),
                                            );
                                        if script_mode {
                                            let result =
                                                QueryResult::new_error(&sql_text, &message);
                                            SqlEditorWidget::emit_script_result(
                                                &sender,
                                                &conn_name,
                                                result_index,
                                                result,
                                                timed_out,
                                            );
                                        } else {
                                            let index = result_index;
                                            let _ = sender
                                                .send(QueryProgress::StatementStart { index });
                                            app::awake();
                                            SqlEditorWidget::append_spool_output(
                                                &session,
                                                std::slice::from_ref(&message),
                                            );
                                            let result =
                                                QueryResult::new_error(&sql_text, &message);
                                            let _ = sender.send(QueryProgress::StatementFinished {
                                                index,
                                                result,
                                                connection_name: conn_name.clone(),
                                                timed_out,
                                            });
                                            app::awake();
                                            result_index += 1;
                                        }
                                        SqlEditorWidget::emit_timing_if_enabled(
                                            &sender,
                                            &session,
                                            statement_start.elapsed(),
                                        );
                                        if timed_out || cancelled || !continue_on_error {
                                            stop_execution = true;
                                        }
                                        continue;
                                    }
                                };

                                let timing_duration = statement_start.elapsed();
                                let base_message = if is_plsql_block {
                                    "PL/SQL block executed successfully".to_string()
                                } else {
                                    "Call executed successfully".to_string()
                                };
                                let mut result = QueryResult {
                                    sql: sql_text.to_string(),
                                    columns: vec![],
                                    rows: vec![],
                                    row_count: 0,
                                    execution_time: timing_duration,
                                    message: base_message,
                                    is_select: false,
                                    success: true,
                                };

                                if load_mutex_bool(&cancel_flag) {
                                    stop_execution = true;
                                    continue;
                                }

                                let mut out_messages: Vec<String> = Vec::new();
                                let (_colsep, null_text, _trimspool_enabled) =
                                    SqlEditorWidget::current_text_output_settings(&session);
                                if let Ok(updates) =
                                    QueryExecutor::fetch_scalar_bind_updates(&stmt, &binds)
                                {
                                    let mut guard = match session.lock() {
                                        Ok(guard) => guard,
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            poisoned.into_inner()
                                        }
                                    };
                                    for (name, value) in updates {
                                        if let Some(bind) = guard.binds.get_mut(&name) {
                                            bind.value = value.clone();
                                        }
                                        if let BindValue::Scalar(val) = value {
                                            out_messages.push(format!(
                                                ":{} = {}",
                                                name,
                                                val.unwrap_or_else(|| null_text.clone())
                                            ));
                                        }
                                    }
                                }

                                if !out_messages.is_empty() {
                                    result.message = format!(
                                        "{} | OUT: {}",
                                        result.message,
                                        out_messages.join(", ")
                                    );
                                }

                                if load_mutex_bool(&cancel_flag) {
                                    stop_execution = true;
                                    continue;
                                }

                                if auto_commit {
                                    if let Err(err) = conn.commit() {
                                        result = QueryResult::new_error(
                                            &sql_text,
                                            &format!("Auto-commit failed: {}", err),
                                        );
                                    } else {
                                        result.message =
                                            format!("{} | Auto-commit applied", result.message);
                                    }
                                }

                                if load_mutex_bool(&cancel_flag) {
                                    stop_execution = true;
                                    continue;
                                }

                                let ref_cursors =
                                    match QueryExecutor::extract_ref_cursors(&stmt, &binds) {
                                        Ok(cursors) => cursors,
                                        Err(err) => {
                                            let cancelled = SqlEditorWidget::is_cancel_error(&err);
                                            timed_out = SqlEditorWidget::is_timeout_error(&err);
                                            SqlEditorWidget::invalidate_oracle_pooled_session_after_error(
                                                &mut cleanup,
                                                &err,
                                            );
                                            let message =
                                                SqlEditorWidget::choose_execution_error_message(
                                                    cancelled,
                                                    timed_out,
                                                    query_timeout,
                                                    format!(
                                                        "Failed to fetch REF CURSOR results: {err}"
                                                    ),
                                                );
                                            if script_mode {
                                                let error_result =
                                                    QueryResult::new_error(&sql_text, &message);
                                                SqlEditorWidget::emit_script_result(
                                                    &sender,
                                                    &conn_name,
                                                    result_index,
                                                    error_result,
                                                    timed_out,
                                                );
                                            } else {
                                                let index = result_index;
                                                let _ = sender
                                                    .send(QueryProgress::StatementStart { index });
                                                app::awake();
                                                SqlEditorWidget::append_spool_output(
                                                    &session,
                                                    std::slice::from_ref(&message),
                                                );
                                                let error_result =
                                                    QueryResult::new_error(&sql_text, &message);
                                                let _ =
                                                    sender.send(QueryProgress::StatementFinished {
                                                        index,
                                                        result: error_result,
                                                        connection_name: conn_name.clone(),
                                                        timed_out,
                                                    });
                                                app::awake();
                                                result_index += 1;
                                            }
                                            SqlEditorWidget::emit_timing_if_enabled(
                                                &sender,
                                                &session,
                                                timing_duration,
                                            );
                                            if timed_out || cancelled || !continue_on_error {
                                                stop_execution = true;
                                            }
                                            continue;
                                        }
                                    };
                                if load_mutex_bool(&cancel_flag) {
                                    stop_execution = true;
                                    continue;
                                }
                                let implicit_results = match QueryExecutor::extract_implicit_results(
                                    &stmt,
                                ) {
                                    Ok(cursors) => cursors,
                                    Err(err) => {
                                        let cancelled = SqlEditorWidget::is_cancel_error(&err);
                                        timed_out = SqlEditorWidget::is_timeout_error(&err);
                                        SqlEditorWidget::invalidate_oracle_pooled_session_after_error(
                                            &mut cleanup,
                                            &err,
                                        );
                                        let message =
                                                SqlEditorWidget::choose_execution_error_message(
                                                    cancelled,
                                                    timed_out,
                                                    query_timeout,
                                                    format!(
                                                        "Failed to fetch implicit result cursors: {err}"
                                                    ),
                                                );
                                        if script_mode {
                                            let error_result =
                                                QueryResult::new_error(&sql_text, &message);
                                            SqlEditorWidget::emit_script_result(
                                                &sender,
                                                &conn_name,
                                                result_index,
                                                error_result,
                                                timed_out,
                                            );
                                        } else {
                                            let index = result_index;
                                            let _ = sender
                                                .send(QueryProgress::StatementStart { index });
                                            app::awake();
                                            SqlEditorWidget::append_spool_output(
                                                &session,
                                                std::slice::from_ref(&message),
                                            );
                                            let error_result =
                                                QueryResult::new_error(&sql_text, &message);
                                            let _ = sender.send(QueryProgress::StatementFinished {
                                                index,
                                                result: error_result,
                                                connection_name: conn_name.clone(),
                                                timed_out,
                                            });
                                            app::awake();
                                            result_index += 1;
                                        }
                                        SqlEditorWidget::emit_timing_if_enabled(
                                            &sender,
                                            &session,
                                            timing_duration,
                                        );
                                        if timed_out || cancelled || !continue_on_error {
                                            stop_execution = true;
                                        }
                                        continue;
                                    }
                                };

                                if load_mutex_bool(&cancel_flag) {
                                    stop_execution = true;
                                    continue;
                                }

                                // Capture success before moving result into the channel
                                // to avoid cloning the entire QueryResult.
                                let result_success = result.success;
                                if script_mode {
                                    if result_success {
                                        SqlEditorWidget::emit_script_lines(
                                            &sender,
                                            &session,
                                            &result.message,
                                        );
                                    }
                                    SqlEditorWidget::emit_script_result(
                                        &sender,
                                        &conn_name,
                                        result_index,
                                        result,
                                        timed_out,
                                    );
                                } else {
                                    let index = result_index;
                                    let _ = sender.send(QueryProgress::StatementStart { index });
                                    app::awake();
                                    if !result.message.trim().is_empty() {
                                        SqlEditorWidget::append_spool_output(
                                            &session,
                                            std::slice::from_ref(&result.message),
                                        );
                                    }
                                    let _ = sender.send(QueryProgress::StatementFinished {
                                        index,
                                        result,
                                        connection_name: conn_name.clone(),
                                        timed_out,
                                    });
                                    app::awake();
                                    result_index += 1;
                                }

                                for (cursor_name, mut cursor) in ref_cursors {
                                    if stop_execution || load_mutex_bool(&cancel_flag) {
                                        break;
                                    }
                                    let index = result_index;
                                    let _ = sender.send(QueryProgress::StatementStart { index });
                                    app::awake();

                                    let mut buffered_rows: Vec<Vec<String>> = Vec::new();
                                    let mut cursor_rows: Vec<Vec<String>> = Vec::new();
                                    let mut last_flush = Instant::now();
                                    let mut has_flushed_rows = false;
                                    let cursor_start = Instant::now();
                                    let mut cursor_timed_out = false;
                                    let (heading_enabled, feedback_enabled) =
                                        SqlEditorWidget::current_output_settings(&session);
                                    let (colsep, null_text, _trimspool_enabled) =
                                        SqlEditorWidget::current_text_output_settings(&session);

                                    let cursor_label = format!("REFCURSOR :{}", cursor_name);
                                    let cursor_result = QueryExecutor::execute_ref_cursor_streaming(
                                        &mut cursor,
                                        &cursor_label,
                                        &mut |columns| {
                                            let names = columns
                                                .iter()
                                                .map(|col| col.name.clone())
                                                .collect::<Vec<String>>();
                                            let display_columns =
                                                SqlEditorWidget::apply_heading_setting(
                                                    names,
                                                    heading_enabled,
                                                );
                                            let _ = sender.send(QueryProgress::SelectStart {
                                                index,
                                                columns: display_columns.clone(),
                                                null_text: null_text.clone(),
                                            });
                                            app::awake();
                                            if !display_columns.is_empty() {
                                                SqlEditorWidget::append_spool_output(
                                                    &session,
                                                    &[display_columns.join(&colsep)],
                                                );
                                            }
                                        },
                                        &mut |row| {
                                            if load_mutex_bool(&cancel_flag) {
                                                return false;
                                            }
                                            if let Some(timeout_duration) = query_timeout {
                                                if cursor_start.elapsed() >= timeout_duration {
                                                    cursor_timed_out = true;
                                                    let _ = conn.break_execution();
                                                    return false;
                                                }
                                            }
                                            cursor_rows.push(row.clone());
                                            let mut display_row = row;
                                            SqlEditorWidget::apply_null_text_to_row(
                                                &mut display_row,
                                                &null_text,
                                            );
                                            buffered_rows.push(display_row);
                                            if SqlEditorWidget::should_flush_progress_rows(
                                                last_flush,
                                                buffered_rows.len(),
                                                has_flushed_rows,
                                            ) {
                                                let rows = std::mem::take(&mut buffered_rows);
                                                SqlEditorWidget::append_spool_rows(&session, &rows);
                                                let _ = sender
                                                    .send(QueryProgress::Rows { index, rows });
                                                app::awake();
                                                last_flush = Instant::now();
                                                has_flushed_rows = true;
                                            }
                                            true
                                        },
                                    );

                                    match cursor_result {
                                        Ok((mut query_result, was_cancelled)) => {
                                            let cursor_column_names =
                                                SqlEditorWidget::cursor_result_column_names(
                                                    &query_result.columns,
                                                );
                                            if cursor_timed_out {
                                                query_result.message =
                                                    SqlEditorWidget::timeout_message(query_timeout);
                                                query_result.success = false;
                                                cursor_timed_out = true;
                                                cleanup.invalidate_oracle_pooled_session();
                                            } else if was_cancelled {
                                                query_result.message =
                                                    SqlEditorWidget::cancel_message();
                                                query_result.success = false;
                                            }
                                            let interrupted = cursor_timed_out || was_cancelled;
                                            SqlEditorWidget::flush_buffered_rows(
                                                &sender,
                                                &session,
                                                index,
                                                &mut buffered_rows,
                                                interrupted,
                                            );
                                            SqlEditorWidget::apply_heading_to_result(
                                                &mut query_result,
                                                heading_enabled,
                                            );
                                            if !feedback_enabled {
                                                query_result.message.clear();
                                            }

                                            // Spool output before sending to avoid
                                            // cloning the message string a second time.
                                            if !query_result.message.trim().is_empty() {
                                                SqlEditorWidget::append_spool_output(
                                                    &session,
                                                    std::slice::from_ref(&query_result.message),
                                                );
                                            }
                                            let cursor_success = query_result.success;
                                            let _ = sender.send(QueryProgress::StatementFinished {
                                                index,
                                                result: query_result,
                                                connection_name: conn_name.clone(),
                                                timed_out: cursor_timed_out,
                                            });
                                            app::awake();
                                            result_index += 1;

                                            let mut guard = match session.lock() {
                                                Ok(guard) => guard,
                                                Err(poisoned) => {
                                                    eprintln!("Warning: session state lock was poisoned; recovering.");
                                                    poisoned.into_inner()
                                                }
                                            };
                                            if let Some(bind) = guard.binds.get_mut(&cursor_name) {
                                                bind.value =
                                                    BindValue::Cursor(Some(CursorResult {
                                                        columns: cursor_column_names,
                                                        rows: cursor_rows,
                                                    }));
                                            }

                                            if cursor_timed_out {
                                                stop_execution = true;
                                                break;
                                            }
                                            if !cursor_success && !continue_on_error {
                                                stop_execution = true;
                                                break;
                                            }
                                        }
                                        Err(err) => {
                                            let cancelled = SqlEditorWidget::is_cancel_error(&err);
                                            cursor_timed_out =
                                                SqlEditorWidget::is_timeout_error(&err);
                                            SqlEditorWidget::invalidate_oracle_pooled_session_after_error(
                                                &mut cleanup,
                                                &err,
                                            );
                                            let message =
                                                SqlEditorWidget::choose_execution_error_message(
                                                    cancelled,
                                                    cursor_timed_out,
                                                    query_timeout,
                                                    err.to_string(),
                                                );
                                            SqlEditorWidget::append_spool_output(
                                                &session,
                                                std::slice::from_ref(&message),
                                            );
                                            let _ = sender.send(QueryProgress::StatementFinished {
                                                index,
                                                result: QueryResult::new_error(
                                                    &cursor_label,
                                                    &message,
                                                ),
                                                connection_name: conn_name.clone(),
                                                timed_out: cursor_timed_out,
                                            });
                                            app::awake();
                                            result_index += 1;

                                            if cursor_timed_out || cancelled || !continue_on_error {
                                                stop_execution = true;
                                                break;
                                            }
                                        }
                                    }
                                }

                                for (idx, mut cursor) in implicit_results.into_iter().enumerate() {
                                    if stop_execution || load_mutex_bool(&cancel_flag) {
                                        break;
                                    }
                                    let index = result_index;
                                    let _ = sender.send(QueryProgress::StatementStart { index });
                                    app::awake();

                                    let mut buffered_rows: Vec<Vec<String>> = Vec::new();
                                    let mut last_flush = Instant::now();
                                    let mut has_flushed_rows = false;
                                    let cursor_start = Instant::now();
                                    let mut cursor_timed_out = false;
                                    let (heading_enabled, feedback_enabled) =
                                        SqlEditorWidget::current_output_settings(&session);
                                    let (colsep, null_text, _trimspool_enabled) =
                                        SqlEditorWidget::current_text_output_settings(&session);
                                    let cursor_label = format!("IMPLICIT RESULT {}", idx + 1);

                                    let cursor_result = QueryExecutor::execute_ref_cursor_streaming(
                                        &mut cursor,
                                        &cursor_label,
                                        &mut |columns| {
                                            let names = columns
                                                .iter()
                                                .map(|col| col.name.clone())
                                                .collect::<Vec<String>>();
                                            let display_columns =
                                                SqlEditorWidget::apply_heading_setting(
                                                    names,
                                                    heading_enabled,
                                                );
                                            let _ = sender.send(QueryProgress::SelectStart {
                                                index,
                                                columns: display_columns.clone(),
                                                null_text: null_text.clone(),
                                            });
                                            app::awake();
                                            if !display_columns.is_empty() {
                                                SqlEditorWidget::append_spool_output(
                                                    &session,
                                                    &[display_columns.join(&colsep)],
                                                );
                                            }
                                        },
                                        &mut |row| {
                                            if load_mutex_bool(&cancel_flag) {
                                                return false;
                                            }
                                            if let Some(timeout_duration) = query_timeout {
                                                if cursor_start.elapsed() >= timeout_duration {
                                                    cursor_timed_out = true;
                                                    let _ = conn.break_execution();
                                                    return false;
                                                }
                                            }
                                            let mut display_row = row;
                                            SqlEditorWidget::apply_null_text_to_row(
                                                &mut display_row,
                                                &null_text,
                                            );
                                            buffered_rows.push(display_row);
                                            if SqlEditorWidget::should_flush_progress_rows(
                                                last_flush,
                                                buffered_rows.len(),
                                                has_flushed_rows,
                                            ) {
                                                let rows = std::mem::take(&mut buffered_rows);
                                                SqlEditorWidget::append_spool_rows(&session, &rows);
                                                let _ = sender
                                                    .send(QueryProgress::Rows { index, rows });
                                                app::awake();
                                                last_flush = Instant::now();
                                                has_flushed_rows = true;
                                            }
                                            true
                                        },
                                    );

                                    match cursor_result {
                                        Ok((mut query_result, was_cancelled)) => {
                                            if cursor_timed_out {
                                                query_result.message =
                                                    SqlEditorWidget::timeout_message(query_timeout);
                                                query_result.success = false;
                                                cursor_timed_out = true;
                                                cleanup.invalidate_oracle_pooled_session();
                                            } else if was_cancelled {
                                                query_result.message =
                                                    SqlEditorWidget::cancel_message();
                                                query_result.success = false;
                                            }
                                            let interrupted = cursor_timed_out || was_cancelled;
                                            SqlEditorWidget::flush_buffered_rows(
                                                &sender,
                                                &session,
                                                index,
                                                &mut buffered_rows,
                                                interrupted,
                                            );
                                            SqlEditorWidget::apply_heading_to_result(
                                                &mut query_result,
                                                heading_enabled,
                                            );
                                            if !feedback_enabled {
                                                query_result.message.clear();
                                            }

                                            // Spool output before sending to avoid
                                            // cloning the message string a second time.
                                            if !query_result.message.trim().is_empty() {
                                                SqlEditorWidget::append_spool_output(
                                                    &session,
                                                    std::slice::from_ref(&query_result.message),
                                                );
                                            }
                                            let cursor_success = query_result.success;
                                            let _ = sender.send(QueryProgress::StatementFinished {
                                                index,
                                                result: query_result,
                                                connection_name: conn_name.clone(),
                                                timed_out: cursor_timed_out,
                                            });
                                            app::awake();
                                            result_index += 1;

                                            if cursor_timed_out {
                                                stop_execution = true;
                                                break;
                                            }
                                            if !cursor_success && !continue_on_error {
                                                stop_execution = true;
                                                break;
                                            }
                                        }
                                        Err(err) => {
                                            let cancelled = SqlEditorWidget::is_cancel_error(&err);
                                            cursor_timed_out =
                                                SqlEditorWidget::is_timeout_error(&err);
                                            SqlEditorWidget::invalidate_oracle_pooled_session_after_error(
                                                &mut cleanup,
                                                &err,
                                            );
                                            let message =
                                                SqlEditorWidget::choose_execution_error_message(
                                                    cancelled,
                                                    cursor_timed_out,
                                                    query_timeout,
                                                    err.to_string(),
                                                );
                                            SqlEditorWidget::append_spool_output(
                                                &session,
                                                std::slice::from_ref(&message),
                                            );
                                            let _ = sender.send(QueryProgress::StatementFinished {
                                                index,
                                                result: QueryResult::new_error(
                                                    &cursor_label,
                                                    &message,
                                                ),
                                                connection_name: conn_name.clone(),
                                                timed_out: cursor_timed_out,
                                            });
                                            app::awake();
                                            result_index += 1;

                                            if cursor_timed_out || cancelled || !continue_on_error {
                                                stop_execution = true;
                                                break;
                                            }
                                        }
                                    }
                                }

                                let cancel_requested = load_mutex_bool(&cancel_flag);
                                let should_stop_after_statement = stop_execution
                                    || cancel_requested
                                    || timed_out
                                    || (!result_success && !continue_on_error);
                                if SqlEditorWidget::should_capture_post_execution_output(
                                    cancel_requested,
                                    timed_out,
                                    should_stop_after_statement,
                                ) {
                                    let _ = SqlEditorWidget::emit_dbms_output(
                                        &sender,
                                        &conn_name,
                                        conn.as_ref(),
                                        &session,
                                        &mut result_index,
                                    );
                                }
                                SqlEditorWidget::emit_timing_if_enabled(
                                    &sender,
                                    &session,
                                    timing_duration,
                                );

                                if should_stop_after_statement {
                                    stop_execution = true;
                                }
                            } else if is_select {
                                let sql_to_execute =
                                    sql_text.trim_end_matches(';').trim().to_string();
                                let binds = match session.lock() {
                                    Ok(guard) => {
                                        QueryExecutor::resolve_binds(&sql_to_execute, &guard)
                                    }
                                    Err(poisoned) => {
                                        eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                        QueryExecutor::resolve_binds(
                                            &sql_to_execute,
                                            &poisoned.into_inner(),
                                        )
                                    }
                                };

                                let binds = match binds {
                                    Ok(binds) => binds,
                                    Err(message) => {
                                        let emitted = SqlEditorWidget::emit_non_select_result(
                                            &sender,
                                            &session,
                                            &conn_name,
                                            result_index,
                                            &sql_text,
                                            format!("Error: {}", message),
                                            false,
                                            false,
                                            script_mode,
                                        );
                                        if emitted {
                                            result_index += 1;
                                        }
                                        if !continue_on_error {
                                            stop_execution = true;
                                        }
                                        continue;
                                    }
                                };

                                let index = result_index;
                                let _ = sender.send(QueryProgress::StatementStart { index });
                                app::awake();

                                let (heading_enabled, feedback_enabled) =
                                    SqlEditorWidget::current_output_settings(&session);
                                let mut buffered_rows: Vec<Vec<String>> = Vec::new();
                                let mut select_column_names: Vec<String> = Vec::new();
                                let select_column_count = std::cell::Cell::new(0usize);
                                let mut last_select_row: Option<Vec<String>> = None;
                                let mut last_flush = Instant::now();
                                let mut has_flushed_rows = false;
                                let statement_start = Instant::now();
                                let mut timed_out = false;
                                let mut statement_interrupted = false;
                                let (colsep, null_text, _trimspool_enabled) =
                                    SqlEditorWidget::current_text_output_settings(&session);
                                let (break_column, compute_config) = match session.lock() {
                                    Ok(guard) => {
                                        (guard.break_column.clone(), guard.compute.clone())
                                    }
                                    Err(poisoned) => {
                                        eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                        let guard = poisoned.into_inner();
                                        (guard.break_column.clone(), guard.compute.clone())
                                    }
                                };
                                let transform_state =
                                    std::sync::Mutex::new(SelectTransformState::default());

                                if lazy_fetch_single_statement
                                    && break_column.is_none()
                                    && compute_config.is_none()
                                {
                                    let session_id = next_lazy_fetch_session_id
                                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    match SqlEditorWidget::start_oracle_lazy_select(
                                        Arc::clone(conn),
                                        pooled_db_session.clone(),
                                        connection_generation,
                                        sender.clone(),
                                        session.clone(),
                                        conn_name.clone(),
                                        index,
                                        sql_to_execute.clone(),
                                        binds.clone(),
                                        heading_enabled,
                                        feedback_enabled,
                                        colsep.clone(),
                                        null_text.clone(),
                                        active_lazy_fetch.clone(),
                                        session_id,
                                        query_timeout,
                                        previous_timeout,
                                        should_apply_oracle_transaction_mode
                                            && transaction_mode.access_mode
                                                == crate::db::TransactionAccessMode::ReadOnly,
                                    ) {
                                        Ok(()) => {
                                            // The lazy worker now owns this pooled connection and
                                            // restores its timeout when the cursor closes.
                                            cleanup.clear_timeout_tracking();
                                            cleanup.clear_oracle_pooled_session_tracking();
                                            cleanup.clear_oracle_read_only_transaction_tracking();
                                            result_index += 1;
                                            continue;
                                        }
                                        Err(err) => {
                                            let mut error_result =
                                                QueryResult::new_error(&sql_text, &err.to_string());
                                            error_result.is_select = true;
                                            let _ = sender.send(QueryProgress::StatementFinished {
                                                index,
                                                result: error_result,
                                                connection_name: conn_name.clone(),
                                                timed_out: false,
                                            });
                                            app::awake();
                                            result_index += 1;
                                            if !continue_on_error {
                                                stop_execution = true;
                                            }
                                            continue;
                                        }
                                    }
                                }

                                let result =
                                    match QueryExecutor::execute_select_streaming_with_binds(
                                        conn.as_ref(),
                                        &sql_to_execute,
                                        &binds,
                                        &mut |columns| {
                                            let names = columns
                                                .iter()
                                                .map(|col| col.name.clone())
                                                .collect::<Vec<String>>();
                                            select_column_names = names.clone();
                                            select_column_count.set(names.len());
                                            {
                                                let mut state =
                                                    transform_state.lock().unwrap_or_else(
                                                        |poisoned| poisoned.into_inner(),
                                                    );
                                                state.break_index =
                                                    break_column.as_ref().and_then(|target| {
                                                        let target_key =
                                                            SessionState::normalize_name(target);
                                                        names.iter().position(|column_name| {
                                                            SessionState::normalize_name(
                                                                column_name,
                                                            ) == target_key
                                                        })
                                                    });
                                                state.compute_of_index =
                                                    compute_config
                                                        .as_ref()
                                                        .and_then(|config| {
                                                            config.of_column.as_ref().and_then(
                                                                |target| {
                                                                    let target_key =
                                                                        SessionState::normalize_name(
                                                                            target,
                                                                        );
                                                                    names.iter().position(
                                                                        |column_name| {
                                                                            SessionState::normalize_name(column_name)
                                                                                == target_key
                                                                        },
                                                                    )
                                                                },
                                                            )
                                                        });
                                                state.compute_on_index =
                                                    compute_config
                                                        .as_ref()
                                                        .and_then(|config| {
                                                            config.on_column.as_ref().and_then(
                                                                |target| {
                                                                    let target_key =
                                                                        SessionState::normalize_name(
                                                                            target,
                                                                        );
                                                                    names.iter().position(
                                                                        |column_name| {
                                                                            SessionState::normalize_name(column_name)
                                                                                == target_key
                                                                        },
                                                                    )
                                                                },
                                                            )
                                                        });
                                                if compute_config
                                                    .as_ref()
                                                    .map(|config| {
                                                        config.mode == crate::db::ComputeMode::Sum
                                                            && config.of_column.is_none()
                                                    })
                                                    .unwrap_or(false)
                                                {
                                                    state.compute_sums = vec![0.0; names.len()];
                                                    state.compute_seen_numeric =
                                                        vec![false; names.len()];
                                                }
                                            }
                                            let display_columns =
                                                SqlEditorWidget::apply_heading_setting(
                                                    names,
                                                    heading_enabled,
                                                );
                                            let _ = sender.send(QueryProgress::SelectStart {
                                                index,
                                                columns: display_columns.clone(),
                                                null_text: null_text.clone(),
                                            });
                                            app::awake();
                                            if !display_columns.is_empty() {
                                                SqlEditorWidget::append_spool_output(
                                                    &session,
                                                    &[display_columns.join(&colsep)],
                                                );
                                            }
                                        },
                                        &mut |row| {
                                            if load_mutex_bool(&cancel_flag) {
                                                return false;
                                            }
                                            if let Some(timeout_duration) = query_timeout {
                                                if statement_start.elapsed() >= timeout_duration {
                                                    timed_out = true;
                                                    let _ = conn.break_execution();
                                                    return false;
                                                }
                                            }

                                            let mut row = row;
                                            last_select_row = Some(row.clone());
                                            {
                                                let mut state =
                                                    transform_state.lock().unwrap_or_else(
                                                        |poisoned| poisoned.into_inner(),
                                                    );
                                                if let Some(config) = compute_config.as_ref() {
                                                    let grouped_compute =
                                                        config.of_column.is_some()
                                                            && config.on_column.is_some()
                                                            && state.compute_of_index.is_some()
                                                            && state.compute_on_index.is_some();
                                                    if grouped_compute {
                                                        if let Some(on_idx) = state.compute_on_index
                                                        {
                                                            if let Some(current_group_value) =
                                                                row.get(on_idx).cloned()
                                                            {
                                                                if let Some(previous_group_value) =
                                                                    state
                                                                        .compute_group_value
                                                                        .clone()
                                                                {
                                                                    if previous_group_value
                                                                        != current_group_value
                                                                    {
                                                                        if let Some(summary_row) =
                                                                            SqlEditorWidget::build_compute_summary_row(
                                                                                select_column_count.get(),
                                                                                Some(config),
                                                                                &state,
                                                                            )
                                                                        {
                                                                            let mut display_summary =
                                                                                summary_row;
                                                                            SqlEditorWidget::apply_null_text_to_row(
                                                                                &mut display_summary,
                                                                                &null_text,
                                                                            );
                                                                            buffered_rows
                                                                                .push(display_summary);
                                                                        }
                                                                        state.compute_count = 0;
                                                                        state.compute_sum = 0.0;
                                                                        state.compute_sum_seen =
                                                                            false;
                                                                    }
                                                                }
                                                                state.compute_group_value =
                                                                    Some(current_group_value);
                                                            }
                                                        }
                                                    }
                                                    SqlEditorWidget::accumulate_compute(
                                                        config, &row, &mut state,
                                                    );
                                                }
                                                if let Some(idx) = state.break_index {
                                                    if let Some(current_break_value) =
                                                        row.get(idx).cloned()
                                                    {
                                                        if state
                                                            .previous_break_value
                                                            .as_ref()
                                                            .map(|prev| {
                                                                prev == &current_break_value
                                                            })
                                                            .unwrap_or(false)
                                                        {
                                                            if let Some(cell) = row.get_mut(idx) {
                                                                *cell = String::new();
                                                            }
                                                        } else {
                                                            state.previous_break_value =
                                                                Some(current_break_value);
                                                        }
                                                    }
                                                }
                                            }
                                            SqlEditorWidget::apply_null_text_to_row(
                                                &mut row, &null_text,
                                            );
                                            buffered_rows.push(row);
                                            if SqlEditorWidget::should_flush_progress_rows(
                                                last_flush,
                                                buffered_rows.len(),
                                                has_flushed_rows,
                                            ) {
                                                let rows = std::mem::take(&mut buffered_rows);
                                                SqlEditorWidget::append_spool_rows(&session, &rows);
                                                let _ = sender
                                                    .send(QueryProgress::Rows { index, rows });
                                                app::awake();
                                                last_flush = Instant::now();
                                                has_flushed_rows = true;
                                            }
                                            true
                                        },
                                    ) {
                                        Ok((mut query_result, was_cancelled)) => {
                                            SqlEditorWidget::apply_heading_to_result(
                                                &mut query_result,
                                                heading_enabled,
                                            );
                                            if timed_out {
                                                query_result.message =
                                                    SqlEditorWidget::timeout_message(query_timeout);
                                                query_result.success = false;
                                                timed_out = true;
                                                statement_interrupted = true;
                                                cleanup.invalidate_oracle_pooled_session();
                                            } else if was_cancelled {
                                                query_result.message =
                                                    SqlEditorWidget::cancel_message();
                                                query_result.success = false;
                                                statement_interrupted = true;
                                            }
                                            if !feedback_enabled {
                                                query_result.message.clear();
                                            }
                                            if !query_result.message.trim().is_empty() {
                                                SqlEditorWidget::append_spool_output(
                                                    &session,
                                                    std::slice::from_ref(&query_result.message),
                                                );
                                            }
                                            query_result
                                        }
                                        Err(err) => {
                                            let cancelled = SqlEditorWidget::is_cancel_error(&err);
                                            timed_out = SqlEditorWidget::is_timeout_error(&err);
                                            statement_interrupted = timed_out || cancelled;
                                            SqlEditorWidget::invalidate_oracle_pooled_session_after_error(
                                                &mut cleanup,
                                                &err,
                                            );
                                            let message =
                                                SqlEditorWidget::choose_execution_error_message(
                                                    cancelled,
                                                    timed_out,
                                                    query_timeout,
                                                    err.to_string(),
                                                );
                                            let mut error_result =
                                                QueryResult::new_error(&sql_text, &message);
                                            // Preserve is_select flag so existing streamed data is kept
                                            error_result.is_select = true;
                                            error_result
                                        }
                                    };
                                let transform_state = match transform_state.into_inner() {
                                    Ok(state) => state,
                                    Err(poisoned) => {
                                        eprintln!(
                                            "Warning: transform state lock was poisoned; recovering."
                                        );
                                        poisoned.into_inner()
                                    }
                                };

                                SqlEditorWidget::flush_buffered_rows(
                                    &sender,
                                    &session,
                                    index,
                                    &mut buffered_rows,
                                    statement_interrupted,
                                );

                                if !result.message.trim().is_empty() {
                                    SqlEditorWidget::append_spool_output(
                                        &session,
                                        std::slice::from_ref(&result.message),
                                    );
                                }
                                if result.success {
                                    let grouped_compute = compute_config
                                        .as_ref()
                                        .map(|config| {
                                            config.of_column.is_some()
                                                && config.on_column.is_some()
                                                && transform_state.compute_of_index.is_some()
                                                && transform_state.compute_on_index.is_some()
                                        })
                                        .unwrap_or(false);
                                    if grouped_compute {
                                        if transform_state.compute_group_value.is_some() {
                                            if let Some(summary_row) =
                                                SqlEditorWidget::build_compute_summary_row(
                                                    select_column_names.len(),
                                                    compute_config.as_ref(),
                                                    &transform_state,
                                                )
                                            {
                                                let rows = vec![summary_row];
                                                SqlEditorWidget::append_spool_rows(&session, &rows);
                                                let _ = sender
                                                    .send(QueryProgress::Rows { index, rows });
                                                app::awake();
                                            }
                                        }
                                    } else if let Some(summary_row) =
                                        SqlEditorWidget::build_compute_summary_row(
                                            select_column_names.len(),
                                            compute_config.as_ref(),
                                            &transform_state,
                                        )
                                    {
                                        let rows = vec![summary_row];
                                        SqlEditorWidget::append_spool_rows(&session, &rows);
                                        let _ = sender.send(QueryProgress::Rows { index, rows });
                                        app::awake();
                                    }
                                    SqlEditorWidget::apply_column_new_value_from_row(
                                        &session,
                                        &select_column_names,
                                        last_select_row.as_deref(),
                                    );
                                }
                                let timing_duration = if result.execution_time.is_zero() {
                                    statement_start.elapsed()
                                } else {
                                    result.execution_time
                                };
                                let result_success = result.success;
                                let _ = sender.send(QueryProgress::StatementFinished {
                                    index,
                                    result,
                                    connection_name: conn_name.clone(),
                                    timed_out,
                                });
                                app::awake();
                                result_index += 1;

                                let cancel_requested = load_mutex_bool(&cancel_flag);
                                let should_stop_after_statement = cancel_requested
                                    || timed_out
                                    || (!result_success && !continue_on_error);
                                let skip_post_execution_output =
                                    should_stop_after_statement || statement_interrupted;
                                if SqlEditorWidget::should_capture_post_execution_output(
                                    cancel_requested,
                                    timed_out,
                                    skip_post_execution_output,
                                ) {
                                    let _ = SqlEditorWidget::emit_dbms_output(
                                        &sender,
                                        &conn_name,
                                        conn.as_ref(),
                                        &session,
                                        &mut result_index,
                                    );
                                }
                                SqlEditorWidget::emit_timing_if_enabled(
                                    &sender,
                                    &session,
                                    timing_duration,
                                );

                                if should_stop_after_statement {
                                    stop_execution = true;
                                }
                            } else {
                                let sql_to_execute = if is_compiled_plsql {
                                    SqlEditorWidget::ensure_plsql_terminator(&sql_text)
                                } else {
                                    sql_text.trim_end_matches(';').trim().to_string()
                                };
                                let binds = match session.lock() {
                                    Ok(guard) => {
                                        QueryExecutor::resolve_binds(&sql_to_execute, &guard)
                                    }
                                    Err(poisoned) => {
                                        eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                        QueryExecutor::resolve_binds(
                                            &sql_to_execute,
                                            &poisoned.into_inner(),
                                        )
                                    }
                                };

                                let binds = match binds {
                                    Ok(binds) => binds,
                                    Err(message) => {
                                        let emitted = SqlEditorWidget::emit_non_select_result(
                                            &sender,
                                            &session,
                                            &conn_name,
                                            result_index,
                                            &sql_text,
                                            format!("Error: {}", message),
                                            false,
                                            false,
                                            script_mode,
                                        );
                                        if emitted {
                                            result_index += 1;
                                        }
                                        if !continue_on_error {
                                            stop_execution = true;
                                        }
                                        continue;
                                    }
                                };

                                let statement_start = Instant::now();
                                let mut timed_out = false;
                                let stmt = match QueryExecutor::execute_with_binds(
                                    conn.as_ref(),
                                    &sql_to_execute,
                                    &binds,
                                ) {
                                    Ok(stmt) => stmt,
                                    Err(err) => {
                                        let cancelled = SqlEditorWidget::is_cancel_error(&err);
                                        timed_out = SqlEditorWidget::is_timeout_error(&err);
                                        SqlEditorWidget::invalidate_oracle_pooled_session_after_error(
                                            &mut cleanup,
                                            &err,
                                        );
                                        let message =
                                            SqlEditorWidget::choose_execution_error_message(
                                                cancelled,
                                                timed_out,
                                                query_timeout,
                                                err.to_string(),
                                            );
                                        if script_mode {
                                            let result =
                                                QueryResult::new_error(&sql_text, &message);
                                            SqlEditorWidget::emit_script_result(
                                                &sender,
                                                &conn_name,
                                                result_index,
                                                result,
                                                timed_out,
                                            );
                                        } else {
                                            let index = result_index;
                                            let _ = sender
                                                .send(QueryProgress::StatementStart { index });
                                            app::awake();
                                            let result =
                                                QueryResult::new_error(&sql_text, &message);
                                            let _ = sender.send(QueryProgress::StatementFinished {
                                                index,
                                                result,
                                                connection_name: conn_name.clone(),
                                                timed_out,
                                            });
                                            app::awake();
                                            result_index += 1;
                                        }
                                        SqlEditorWidget::emit_timing_if_enabled(
                                            &sender,
                                            &session,
                                            statement_start.elapsed(),
                                        );
                                        if timed_out || cancelled || !continue_on_error {
                                            stop_execution = true;
                                        }
                                        continue;
                                    }
                                };

                                let execution_time = statement_start.elapsed();
                                let timing_duration = execution_time;
                                let dml_type = if upper.starts_with("INSERT") {
                                    Some("INSERT")
                                } else if upper.starts_with("UPDATE") {
                                    Some("UPDATE")
                                } else if upper.starts_with("DELETE") {
                                    Some("DELETE")
                                } else if upper.starts_with("MERGE") {
                                    Some("MERGE")
                                } else {
                                    None
                                };

                                let mut result = if let Some(statement_type) = dml_type {
                                    let affected_rows = stmt.row_count().unwrap_or(0);
                                    QueryResult::new_dml(
                                        &sql_text,
                                        affected_rows,
                                        execution_time,
                                        statement_type,
                                    )
                                } else {
                                    QueryResult {
                                        sql: sql_text.to_string(),
                                        columns: vec![],
                                        rows: vec![],
                                        row_count: 0,
                                        execution_time,
                                        message: if upper.starts_with("CREATE")
                                            || upper.starts_with("ALTER")
                                            || upper.starts_with("DROP")
                                            || upper.starts_with("TRUNCATE")
                                            || upper.starts_with("RENAME")
                                            || upper.starts_with("GRANT")
                                            || upper.starts_with("REVOKE")
                                            || upper.starts_with("COMMENT")
                                        {
                                            SqlEditorWidget::ddl_message(&upper)
                                        } else {
                                            "Statement executed successfully".to_string()
                                        },
                                        is_select: false,
                                        success: true,
                                    }
                                };

                                if load_mutex_bool(&cancel_flag) {
                                    stop_execution = true;
                                    continue;
                                }

                                let mut out_messages: Vec<String> = Vec::new();
                                let (_colsep, null_text, _trimspool_enabled) =
                                    SqlEditorWidget::current_text_output_settings(&session);
                                if let Ok(updates) =
                                    QueryExecutor::fetch_scalar_bind_updates(&stmt, &binds)
                                {
                                    let mut guard = match session.lock() {
                                        Ok(guard) => guard,
                                        Err(poisoned) => {
                                            eprintln!(
                                            "Warning: session state lock was poisoned; recovering."
                                        );
                                            poisoned.into_inner()
                                        }
                                    };
                                    for (name, value) in updates {
                                        if let Some(bind) = guard.binds.get_mut(&name) {
                                            bind.value = value.clone();
                                        }
                                        if let BindValue::Scalar(val) = value {
                                            out_messages.push(format!(
                                                ":{} = {}",
                                                name,
                                                val.unwrap_or_else(|| null_text.clone())
                                            ));
                                        }
                                    }
                                }

                                if !out_messages.is_empty() {
                                    result.message = format!(
                                        "{} | OUT: {}",
                                        result.message,
                                        out_messages.join(", ")
                                    );
                                }

                                if load_mutex_bool(&cancel_flag) {
                                    stop_execution = true;
                                    continue;
                                }

                                let mut compile_errors: Option<Vec<Vec<String>>> = None;
                                if let Some(object) = compiled_object.clone() {
                                    match QueryExecutor::fetch_compilation_errors(
                                        conn.as_ref(),
                                        &object,
                                    ) {
                                        Ok(rows) => {
                                            if !rows.is_empty() {
                                                result.message = format!(
                                                    "{} | Compiled with errors",
                                                    result.message
                                                );
                                                result.success = false;
                                                compile_errors = Some(rows);
                                            }
                                        }
                                        Err(err) => {
                                            result.message = format!(
                                                "{} | Failed to fetch compilation errors: {}",
                                                result.message, err
                                            );
                                            result.success = false;
                                        }
                                    }
                                }

                                if dml_type.is_some() && !auto_commit && result.success {
                                    result.message =
                                        format!("{} | Commit required", result.message);
                                }

                                if load_mutex_bool(&cancel_flag) {
                                    stop_execution = true;
                                    continue;
                                }

                                if auto_commit && result.success {
                                    if let Err(err) = conn.commit() {
                                        result = QueryResult::new_error(
                                            &sql_text,
                                            &format!("Auto-commit failed: {}", err),
                                        );
                                    } else {
                                        result.message =
                                            format!("{} | Auto-commit applied", result.message);
                                    }
                                }

                                let current_schema_changed = result.success
                                    && SqlEditorWidget::oracle_statement_sets_current_schema(
                                        &sql_to_execute,
                                    );
                                if current_schema_changed
                                    && SqlEditorWidget::sync_oracle_pooled_session_current_schema(
                                        &shared_connection,
                                        conn,
                                        &db_activity,
                                        connection_generation,
                                    )
                                {
                                    let _ = sender.send(QueryProgress::MetadataRefreshNeeded);
                                    app::awake();
                                }

                                // Capture success before moving result into the channel
                                // to avoid cloning the entire QueryResult.
                                let result_success = result.success;
                                if script_mode {
                                    if result_success {
                                        SqlEditorWidget::emit_script_lines(
                                            &sender,
                                            &session,
                                            &result.message,
                                        );
                                    }
                                    SqlEditorWidget::emit_script_result(
                                        &sender,
                                        &conn_name,
                                        result_index,
                                        result,
                                        timed_out,
                                    );
                                } else {
                                    let index = result_index;
                                    let _ = sender.send(QueryProgress::StatementStart { index });
                                    app::awake();
                                    if !result.message.trim().is_empty() {
                                        SqlEditorWidget::append_spool_output(
                                            &session,
                                            std::slice::from_ref(&result.message),
                                        );
                                    }
                                    let _ = sender.send(QueryProgress::StatementFinished {
                                        index,
                                        result,
                                        connection_name: conn_name.clone(),
                                        timed_out,
                                    });
                                    app::awake();
                                    result_index += 1;
                                }

                                if let Some(rows) = compile_errors {
                                    let (heading_enabled, feedback_enabled) =
                                        SqlEditorWidget::current_output_settings(&session);
                                    SqlEditorWidget::emit_select_result(
                                        &sender,
                                        &session,
                                        &conn_name,
                                        result_index,
                                        "COMPILE ERRORS",
                                        SqlEditorWidget::apply_heading_setting(
                                            vec![
                                                "LINE".to_string(),
                                                "POSITION".to_string(),
                                                "TEXT".to_string(),
                                            ],
                                            heading_enabled,
                                        ),
                                        rows,
                                        false,
                                        feedback_enabled,
                                    );
                                    result_index += 1;
                                }

                                let cancel_requested = load_mutex_bool(&cancel_flag);
                                let should_stop_after_statement = cancel_requested
                                    || timed_out
                                    || (!result_success && !continue_on_error);
                                if SqlEditorWidget::should_capture_post_execution_output(
                                    cancel_requested,
                                    timed_out,
                                    should_stop_after_statement,
                                ) {
                                    let _ = SqlEditorWidget::emit_dbms_output(
                                        &sender,
                                        &conn_name,
                                        conn.as_ref(),
                                        &session,
                                        &mut result_index,
                                    );
                                }
                                SqlEditorWidget::emit_timing_if_enabled(
                                    &sender,
                                    &session,
                                    timing_duration,
                                );

                                if should_stop_after_statement {
                                    stop_execution = true;
                                }
                            }
                        }
                    }
                }
            })); // end catch_unwind

            if let Err(e) = result {
                let panic_payload = SqlEditorWidget::panic_payload_to_string(e.as_ref());
                crate::utils::logging::log_error(
                    "sql_editor::execution",
                    &format!("Query worker thread panicked: {panic_payload}"),
                );
                let _ = sender.send(QueryProgress::WorkerPanicked {
                    message: format!("Query execution thread panicked: {panic_payload}"),
                });
                app::awake();
                eprintln!("Query thread panicked: {panic_payload}");
            }
        });
        if let Err(err) = spawn_result {
            let message = format!("Failed to start query execution thread: {err}");
            crate::utils::logging::log_error("sql_editor::execution", &message);
            SqlEditorWidget::set_current_query_connection(
                &spawn_error_current_query_connection,
                None,
            );
            SqlEditorWidget::set_current_mysql_cancel_context(
                &spawn_error_current_mysql_cancel_context,
                None,
            );
            SqlEditorWidget::finalize_execution_state(
                &spawn_error_query_running,
                &spawn_error_cancel_flag,
            );
            let _ = spawn_error_sender.send(QueryProgress::WorkerPanicked { message });
            let _ = spawn_error_sender.send(QueryProgress::BatchFinished);
            app::awake();
            if app::is_ui_thread() {
                set_cursor(Cursor::Default);
                app::flush();
            }
        }
    }

    fn emit_non_select_result(
        sender: &mpsc::Sender<QueryProgress>,
        session: &Arc<Mutex<SessionState>>,
        conn_name: &str,
        index: usize,
        sql: &str,
        message: String,
        success: bool,
        timed_out: bool,
        script_mode: bool,
    ) -> bool {
        if script_mode {
            if success {
                SqlEditorWidget::emit_script_lines(sender, session, &message);
            }
            let result = QueryResult {
                sql: sql.to_string(),
                columns: vec![],
                rows: vec![],
                row_count: 0,
                execution_time: Duration::from_secs(0),
                message,
                is_select: false,
                success,
            };
            SqlEditorWidget::emit_script_result(sender, conn_name, index, result, timed_out);
            return false;
        }

        let _ = sender.send(QueryProgress::StatementStart { index });
        app::awake();
        if !message.trim().is_empty() {
            SqlEditorWidget::append_spool_output(session, std::slice::from_ref(&message));
        }
        let result = QueryResult {
            sql: sql.to_string(),
            columns: vec![],
            rows: vec![],
            row_count: 0,
            execution_time: Duration::from_secs(0),
            message,
            is_select: false,
            success,
        };
        let _ = sender.send(QueryProgress::StatementFinished {
            index,
            result,
            connection_name: conn_name.to_string(),
            timed_out,
        });
        app::awake();
        true
    }

    fn emit_script_result(
        sender: &mpsc::Sender<QueryProgress>,
        conn_name: &str,
        index: usize,
        result: QueryResult,
        timed_out: bool,
    ) {
        let _ = sender.send(QueryProgress::StatementFinished {
            index,
            result,
            connection_name: conn_name.to_string(),
            timed_out,
        });
        app::awake();
    }

    fn apply_column_new_value_from_row(
        session: &Arc<Mutex<SessionState>>,
        column_names: &[String],
        row: Option<&[String]>,
    ) {
        let Some(row_values) = row else {
            return;
        };

        match session.lock() {
            Ok(mut guard) => {
                if guard.column_new_values.is_empty() {
                    return;
                }
                for (idx, column_name) in column_names.iter().enumerate() {
                    let column_key = SessionState::normalize_name(column_name);
                    let Some(variable_key) = guard.column_new_values.get(&column_key).cloned()
                    else {
                        continue;
                    };
                    let Some(value) = row_values.get(idx).cloned() else {
                        continue;
                    };
                    guard.define_vars.insert(variable_key, value);
                }
            }
            Err(poisoned) => {
                eprintln!("Warning: session state lock was poisoned; recovering.");
                let mut guard = poisoned.into_inner();
                if guard.column_new_values.is_empty() {
                    return;
                }
                for (idx, column_name) in column_names.iter().enumerate() {
                    let column_key = SessionState::normalize_name(column_name);
                    let Some(variable_key) = guard.column_new_values.get(&column_key).cloned()
                    else {
                        continue;
                    };
                    let Some(value) = row_values.get(idx).cloned() else {
                        continue;
                    };
                    guard.define_vars.insert(variable_key, value);
                }
            }
        }
    }

    fn current_output_settings(session: &Arc<Mutex<SessionState>>) -> (bool, bool) {
        match session.lock() {
            Ok(guard) => (guard.heading_enabled, guard.feedback_enabled),
            Err(poisoned) => {
                eprintln!("Warning: session state lock was poisoned; recovering.");
                let guard = poisoned.into_inner();
                (guard.heading_enabled, guard.feedback_enabled)
            }
        }
    }

    fn current_text_output_settings(session: &Arc<Mutex<SessionState>>) -> (String, String, bool) {
        match session.lock() {
            Ok(guard) => (
                guard.colsep.clone(),
                guard.null_text.clone(),
                guard.trimspool_enabled,
            ),
            Err(poisoned) => {
                eprintln!("Warning: session state lock was poisoned; recovering.");
                let guard = poisoned.into_inner();
                (
                    guard.colsep.clone(),
                    guard.null_text.clone(),
                    guard.trimspool_enabled,
                )
            }
        }
    }

    fn current_script_output_settings(session: &Arc<Mutex<SessionState>>) -> (bool, bool) {
        match session.lock() {
            Ok(guard) => (guard.trimout_enabled, guard.tab_enabled),
            Err(poisoned) => {
                eprintln!("Warning: session state lock was poisoned; recovering.");
                let guard = poisoned.into_inner();
                (guard.trimout_enabled, guard.tab_enabled)
            }
        }
    }

    fn clone_print_named_data(session: &SessionState, key: &str) -> PrintNamedData {
        match session.binds.get(key) {
            Some(bind) => match &bind.value {
                BindValue::Scalar(value) => PrintNamedData::Scalar(value.clone()),
                BindValue::Cursor(Some(cursor_result)) => {
                    PrintNamedData::Cursor(cursor_result.clone())
                }
                BindValue::Cursor(None) => PrintNamedData::CursorEmpty,
            },
            None => PrintNamedData::Missing,
        }
    }

    fn collect_print_all_data(
        session: &SessionState,
        null_text: &str,
    ) -> (Vec<Vec<String>>, Vec<(String, CursorResult)>) {
        let mut summary_rows = Vec::new();
        let mut cursor_results = Vec::new();

        for (bind_name, bind) in &session.binds {
            let value_display = match &bind.value {
                BindValue::Scalar(value) => value.clone().unwrap_or_else(|| null_text.to_string()),
                BindValue::Cursor(Some(cursor_result)) => {
                    let row_count = cursor_result.rows.len();
                    cursor_results.push((bind_name.clone(), cursor_result.clone()));
                    format!("REFCURSOR ({} rows)", row_count)
                }
                BindValue::Cursor(None) => "REFCURSOR (empty)".to_string(),
            };

            summary_rows.push(vec![
                bind_name.clone(),
                bind.data_type.display(),
                value_display,
            ]);
        }

        (summary_rows, cursor_results)
    }

    fn cursor_result_column_names(columns: &[ColumnInfo]) -> Vec<String> {
        columns.iter().map(|column| column.name.clone()).collect()
    }

    fn has_spool_target(session: &Arc<Mutex<SessionState>>) -> bool {
        match session.lock() {
            Ok(guard) => guard.spool_path.is_some(),
            Err(poisoned) => {
                eprintln!("Warning: session state lock was poisoned; recovering.");
                poisoned.into_inner().spool_path.is_some()
            }
        }
    }

    fn expand_tabs_with_stop(text: &str, tab_stop: usize) -> String {
        let tab_stop = tab_stop.max(1);
        let mut out = String::with_capacity(text.len());
        let mut col = 0usize;

        for ch in text.chars() {
            if ch == '\t' {
                let spaces = tab_stop.saturating_sub(safe_rem(col, tab_stop)).max(1);
                for _ in 0..spaces {
                    out.push(' ');
                }
                col += spaces;
            } else {
                out.push(ch);
                col += 1;
            }
        }

        out
    }

    fn expand_tabs(text: &str) -> String {
        const TAB_STOP: usize = 8;
        Self::expand_tabs_with_stop(text, TAB_STOP)
    }

    pub(super) fn format_script_output_line(
        line: &str,
        trimout_enabled: bool,
        tab_enabled: bool,
    ) -> String {
        let mut rendered = if tab_enabled {
            SqlEditorWidget::expand_tabs(line)
        } else {
            line.to_string()
        };

        if trimout_enabled {
            rendered = rendered.trim_end().to_string();
        }

        rendered
    }

    fn display_cell_value(value: &str, null_text: &str) -> String {
        if value == "NULL" {
            null_text.to_string()
        } else {
            value.to_string()
        }
    }

    fn display_row_values(row: &[String], null_text: &str) -> Vec<String> {
        row.iter()
            .map(|value| SqlEditorWidget::display_cell_value(value, null_text))
            .collect()
    }

    fn apply_null_text_to_row(row: &mut [String], null_text: &str) {
        if null_text == "NULL" {
            return;
        }
        for value in row.iter_mut() {
            if value == "NULL" {
                value.clear();
                value.push_str(null_text);
            }
        }
    }

    fn parse_numeric_value(value: &str) -> Option<f64> {
        let trimmed = value.trim();
        if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("NULL") {
            return None;
        }
        let normalized = trimmed.replace(',', "");
        normalized.parse::<f64>().ok()
    }

    fn format_number(value: f64) -> String {
        let mut text = format!("{}", value);
        if text.contains('.') {
            while text.ends_with('0') {
                text.pop();
            }
            if text.ends_with('.') {
                text.pop();
            }
        }
        text
    }

    fn accumulate_compute(
        config: &crate::db::ComputeConfig,
        row: &[String],
        state: &mut SelectTransformState,
    ) {
        match config.mode {
            crate::db::ComputeMode::Count => {
                if let Some(of_idx) = state.compute_of_index {
                    let is_not_null = row
                        .get(of_idx)
                        .map(|value| !value.eq_ignore_ascii_case("NULL"))
                        .unwrap_or(false);
                    if is_not_null {
                        state.compute_count += 1;
                    }
                } else {
                    state.compute_count += 1;
                }
            }
            crate::db::ComputeMode::Sum => {
                if let Some(of_idx) = state.compute_of_index {
                    if let Some(number) = row
                        .get(of_idx)
                        .and_then(|value| SqlEditorWidget::parse_numeric_value(value))
                    {
                        state.compute_sum += number;
                        state.compute_sum_seen = true;
                    }
                } else {
                    for (idx, value) in row.iter().enumerate() {
                        if let Some(number) = SqlEditorWidget::parse_numeric_value(value) {
                            if let Some(sum_slot) = state.compute_sums.get_mut(idx) {
                                *sum_slot += number;
                            }
                            if let Some(seen_slot) = state.compute_seen_numeric.get_mut(idx) {
                                *seen_slot = true;
                            }
                        }
                    }
                }
            }
        }
    }

    fn build_compute_summary_row(
        column_count: usize,
        compute_config: Option<&crate::db::ComputeConfig>,
        state: &SelectTransformState,
    ) -> Option<Vec<String>> {
        let config = compute_config?;
        let mode = config.mode;
        if column_count == 0 {
            return None;
        }

        let mut row = vec![String::new(); column_count];
        if let (Some(of_idx), Some(on_idx)) = (state.compute_of_index, state.compute_on_index) {
            let label = match mode {
                crate::db::ComputeMode::Count => "COUNT",
                crate::db::ComputeMode::Sum => "SUM",
            };
            if on_idx < column_count {
                row[on_idx] = label.to_string();
            }
            if of_idx < column_count {
                row[of_idx] = match mode {
                    crate::db::ComputeMode::Count => state.compute_count.to_string(),
                    crate::db::ComputeMode::Sum => {
                        if state.compute_sum_seen {
                            SqlEditorWidget::format_number(state.compute_sum)
                        } else {
                            "0".to_string()
                        }
                    }
                };
            }
            return Some(row);
        }

        match mode {
            crate::db::ComputeMode::Count => {
                if column_count == 1 {
                    row[0] = state.compute_count.to_string();
                } else {
                    row[0] = "COUNT".to_string();
                    row[column_count - 1] = state.compute_count.to_string();
                }
            }
            crate::db::ComputeMode::Sum => {
                if let Some(of_idx) = state.compute_of_index {
                    if column_count == 1 {
                        row[0] = if state.compute_sum_seen {
                            SqlEditorWidget::format_number(state.compute_sum)
                        } else {
                            "0".to_string()
                        };
                    } else {
                        row[0] = "SUM".to_string();
                        if of_idx < column_count {
                            row[of_idx] = if state.compute_sum_seen {
                                SqlEditorWidget::format_number(state.compute_sum)
                            } else {
                                "0".to_string()
                            };
                        }
                    }
                } else if column_count == 1 {
                    let total = state.compute_sums.first().copied().unwrap_or(0.0);
                    row[0] = SqlEditorWidget::format_number(total);
                } else {
                    row[0] = "SUM".to_string();
                    let mut has_any_numeric = false;
                    for (idx, cell) in row.iter_mut().enumerate().take(column_count).skip(1) {
                        if state
                            .compute_seen_numeric
                            .get(idx)
                            .copied()
                            .unwrap_or(false)
                        {
                            let total = state.compute_sums.get(idx).copied().unwrap_or(0.0);
                            *cell = SqlEditorWidget::format_number(total);
                            has_any_numeric = true;
                        }
                    }
                    if !has_any_numeric {
                        row[column_count - 1] = "0".to_string();
                    }
                }
            }
        }

        Some(row)
    }

    fn format_row_line(row: &[String], colsep: &str, null_text: &str) -> String {
        let display_row = SqlEditorWidget::display_row_values(row, null_text);
        display_row.join(colsep)
    }

    fn apply_heading_setting(column_names: Vec<String>, heading_enabled: bool) -> Vec<String> {
        if heading_enabled {
            column_names
        } else {
            column_names.into_iter().map(|_| String::new()).collect()
        }
    }

    fn apply_heading_to_result(result: &mut QueryResult, heading_enabled: bool) {
        if heading_enabled {
            return;
        }
        for column in &mut result.columns {
            column.name.clear();
        }
    }

    pub(super) fn should_flush_progress_rows(
        last_flush: Instant,
        buffered_len: usize,
        has_flushed_rows: bool,
    ) -> bool {
        let row_threshold = if has_flushed_rows {
            PROGRESS_ROWS_MAX_BATCH
        } else {
            PROGRESS_ROWS_INITIAL_BATCH
        };

        buffered_len >= row_threshold || last_flush.elapsed() >= PROGRESS_ROWS_FLUSH_INTERVAL
    }

    pub(super) fn should_capture_post_execution_output(
        cancel_requested: bool,
        timed_out: bool,
        stop_execution: bool,
    ) -> bool {
        !cancel_requested && !timed_out && !stop_execution
    }

    pub(super) fn flush_buffered_rows(
        sender: &mpsc::Sender<QueryProgress>,
        session: &Arc<Mutex<SessionState>>,
        index: usize,
        buffered_rows: &mut Vec<Vec<String>>,
        interrupted: bool,
    ) {
        if buffered_rows.is_empty() {
            return;
        }

        if interrupted {
            // Prefer immediate cancel/timeout completion over flushing a large
            // buffered tail that has not been rendered yet.
            buffered_rows.clear();
            return;
        }

        let rows = std::mem::take(buffered_rows);
        SqlEditorWidget::append_spool_rows(session, &rows);
        let _ = sender.send(QueryProgress::Rows { index, rows });
        app::awake();
    }

    pub(super) fn flush_buffered_result_rows(
        sender: &mpsc::Sender<QueryProgress>,
        session: &Arc<Mutex<SessionState>>,
        index: usize,
        buffered_display_rows: &mut Vec<Vec<String>>,
        buffered_raw_rows: &mut Vec<Vec<String>>,
    ) {
        if buffered_display_rows.is_empty() {
            buffered_raw_rows.clear();
            return;
        }

        let rows = std::mem::take(buffered_display_rows);
        let raw_rows = std::mem::take(buffered_raw_rows);
        SqlEditorWidget::append_spool_rows(session, &raw_rows);
        let _ = sender.send(QueryProgress::Rows { index, rows });
        app::awake();
    }

    pub(super) fn emit_select_result(
        sender: &mpsc::Sender<QueryProgress>,
        session: &Arc<Mutex<SessionState>>,
        conn_name: &str,
        index: usize,
        sql: &str,
        column_names: Vec<String>,
        rows: Vec<Vec<String>>,
        success: bool,
        feedback_enabled: bool,
    ) {
        let _ = sender.send(QueryProgress::StatementStart { index });
        app::awake();
        let (colsep, null_text, _trimspool_enabled) =
            SqlEditorWidget::current_text_output_settings(session);
        let _ = sender.send(QueryProgress::SelectStart {
            index,
            columns: column_names.clone(),
            null_text: null_text.clone(),
        });
        app::awake();
        if !column_names.is_empty() {
            SqlEditorWidget::append_spool_output(session, &[column_names.join(&colsep)]);
        }

        let mut row_count = 0usize;
        let mut buffered_display_rows: Vec<Vec<String>> = Vec::new();
        let mut buffered_raw_rows: Vec<Vec<String>> = Vec::new();
        let mut last_flush = Instant::now();
        let mut has_flushed_rows = false;
        if !rows.is_empty() {
            for row in rows {
                row_count += 1;
                buffered_display_rows.push(SqlEditorWidget::display_row_values(&row, &null_text));
                buffered_raw_rows.push(row);
                if SqlEditorWidget::should_flush_progress_rows(
                    last_flush,
                    buffered_display_rows.len(),
                    has_flushed_rows,
                ) {
                    SqlEditorWidget::flush_buffered_result_rows(
                        sender,
                        session,
                        index,
                        &mut buffered_display_rows,
                        &mut buffered_raw_rows,
                    );
                    last_flush = Instant::now();
                    has_flushed_rows = true;
                }
            }
            SqlEditorWidget::flush_buffered_result_rows(
                sender,
                session,
                index,
                &mut buffered_display_rows,
                &mut buffered_raw_rows,
            );
        }
        let column_info: Vec<ColumnInfo> = column_names
            .iter()
            .map(|name| ColumnInfo {
                name: name.clone(),
                data_type: "VARCHAR2".to_string(),
            })
            .collect();
        let mut result =
            QueryResult::new_select_streamed(sql, column_info, row_count, Duration::from_secs(0));
        result.success = success;
        if !feedback_enabled {
            result.message.clear();
        }
        if !result.message.trim().is_empty() {
            SqlEditorWidget::append_spool_output(session, std::slice::from_ref(&result.message));
        }
        let _ = sender.send(QueryProgress::StatementFinished {
            index,
            result,
            connection_name: conn_name.to_string(),
            timed_out: false,
        });
        app::awake();
    }

    fn emit_script_output(
        sender: &mpsc::Sender<QueryProgress>,
        session: &Arc<Mutex<SessionState>>,
        lines: Vec<String>,
    ) {
        if lines.is_empty() {
            return;
        }
        SqlEditorWidget::append_spool_output(session, &lines);
        let (trimout_enabled, tab_enabled) =
            SqlEditorWidget::current_script_output_settings(session);
        let display_lines: Vec<String> = lines
            .into_iter()
            .map(|line| {
                SqlEditorWidget::format_script_output_line(&line, trimout_enabled, tab_enabled)
            })
            .collect();
        let _ = sender.send(QueryProgress::ScriptOutput {
            lines: display_lines,
        });
        app::awake();
    }

    fn emit_timing_if_enabled(
        sender: &mpsc::Sender<QueryProgress>,
        session: &Arc<Mutex<SessionState>>,
        duration: Duration,
    ) {
        let enabled = match session.lock() {
            Ok(guard) => guard.timing_enabled,
            Err(poisoned) => {
                eprintln!("Warning: session state lock was poisoned; recovering.");
                poisoned.into_inner().timing_enabled
            }
        };
        if !enabled {
            return;
        }
        let line = format!("Elapsed: {:.3}s", duration.as_secs_f64());
        SqlEditorWidget::emit_script_output(sender, session, vec![line]);
    }

    fn emit_script_lines(
        sender: &mpsc::Sender<QueryProgress>,
        session: &Arc<Mutex<SessionState>>,
        message: &str,
    ) {
        let lines: Vec<String> = message.lines().map(|line| line.to_string()).collect();
        if lines.is_empty() {
            return;
        }
        SqlEditorWidget::emit_script_output(sender, session, lines);
    }

    fn emit_script_message(
        sender: &mpsc::Sender<QueryProgress>,
        session: &Arc<Mutex<SessionState>>,
        title: &str,
        message: &str,
    ) {
        let mut lines = Vec::new();
        lines.push(format!("[{}]", title));
        for line in message.lines() {
            lines.push(line.to_string());
        }
        SqlEditorWidget::emit_script_output(sender, session, lines);
    }

    fn append_spool_output(session: &Arc<Mutex<SessionState>>, lines: &[String]) {
        if lines.is_empty() {
            return;
        }

        let (path, truncate, trimspool_enabled) = match session.lock() {
            Ok(mut guard) => {
                let path = guard.spool_path.clone();
                let truncate = guard.spool_truncate;
                if truncate {
                    guard.spool_truncate = false;
                }
                (path, truncate, guard.trimspool_enabled)
            }
            Err(poisoned) => {
                eprintln!("Warning: session state lock was poisoned; recovering.");
                let mut guard = poisoned.into_inner();
                let path = guard.spool_path.clone();
                let truncate = guard.spool_truncate;
                if truncate {
                    guard.spool_truncate = false;
                }
                (path, truncate, guard.trimspool_enabled)
            }
        };

        let Some(path) = path else {
            return;
        };

        let mut options = OpenOptions::new();
        options.create(true).write(true);
        if truncate {
            options.truncate(true);
        } else {
            options.append(true);
        }

        let mut file = match options.open(&path) {
            Ok(file) => file,
            Err(err) => {
                eprintln!("Failed to open spool file {}: {}", path.display(), err);
                return;
            }
        };

        for line in lines {
            let line_to_write = if trimspool_enabled {
                line.trim_end()
            } else {
                line.as_str()
            };
            if let Err(err) = writeln!(file, "{}", line_to_write) {
                eprintln!("Failed to write to spool file {}: {}", path.display(), err);
                break;
            }
        }
    }

    fn append_spool_rows(session: &Arc<Mutex<SessionState>>, rows: &[Vec<String>]) {
        if rows.is_empty() {
            return;
        }
        if !SqlEditorWidget::has_spool_target(session) {
            return;
        }
        let (colsep, null_text, _trimspool_enabled) =
            SqlEditorWidget::current_text_output_settings(session);
        let lines: Vec<String> = rows
            .iter()
            .map(|row| SqlEditorWidget::format_row_line(row, &colsep, &null_text))
            .collect();
        SqlEditorWidget::append_spool_output(session, &lines);
    }

    fn apply_define_substitution(
        sql: &str,
        session: &Arc<Mutex<SessionState>>,
        sender: &mpsc::Sender<QueryProgress>,
    ) -> Result<String, String> {
        let define_char = match session.lock() {
            Ok(guard) => guard.define_char,
            Err(poisoned) => {
                eprintln!("Warning: session state lock was poisoned; recovering.");
                poisoned.into_inner().define_char
            }
        };

        let mut result = String::with_capacity(sql.len());
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false;
        let mut in_q_quote = false;
        let mut q_quote_end: Option<char> = None;

        let chars: Vec<char> = sql.chars().collect();
        let len = chars.len();
        let mut i = 0usize;

        while i < len {
            let c = chars[i];
            let next = if i + 1 < len {
                Some(chars[i + 1])
            } else {
                None
            };
            let next2 = if i + 2 < len {
                Some(chars[i + 2])
            } else {
                None
            };

            if in_line_comment {
                result.push(c);
                if c == '\n' {
                    in_line_comment = false;
                }
                i += 1;
                continue;
            }

            if in_block_comment {
                result.push(c);
                if c == '*' && next == Some('/') {
                    result.push('/');
                    in_block_comment = false;
                    i += 2;
                    continue;
                }
                i += 1;
                continue;
            }

            if c == '-' && next == Some('-') {
                result.push('-');
                result.push('-');
                in_line_comment = true;
                i += 2;
                continue;
            }

            if c == '/' && next == Some('*') {
                result.push('/');
                result.push('*');
                in_block_comment = true;
                i += 2;
                continue;
            }

            if c == define_char {
                let is_double = next == Some(define_char);
                let start = if is_double { i + 2 } else { i + 1 };
                let mut j = start;
                while j < len {
                    let ch = chars[j];
                    if sql_text::is_identifier_char(ch) {
                        j += 1;
                    } else {
                        break;
                    }
                }

                if j == start {
                    result.push(c);
                    if is_double {
                        result.push(define_char);
                        i += 2;
                    } else {
                        i += 1;
                    }
                    continue;
                }

                let name: String = chars[start..j].iter().collect();
                let key = SessionState::normalize_name(&name);
                let (define_value, bind_value) = match session.lock() {
                    Ok(guard) => (
                        guard.define_vars.get(&key).cloned(),
                        guard.binds.get(&key).cloned(),
                    ),
                    Err(poisoned) => {
                        eprintln!("Warning: session state lock was poisoned; recovering.");
                        let guard = poisoned.into_inner();
                        (
                            guard.define_vars.get(&key).cloned(),
                            guard.binds.get(&key).cloned(),
                        )
                    }
                };

                let mut replacement = if let Some(value) = define_value {
                    value
                } else if let Some(bind) = bind_value {
                    SqlEditorWidget::format_define_value(&key, &bind)?
                } else {
                    let prompt = format!("Enter value for {}:", name);
                    let input = SqlEditorWidget::prompt_for_input_with_sender(sender, &prompt)?;
                    if is_double {
                        match session.lock() {
                            Ok(mut guard) => {
                                guard.define_vars.insert(key.clone(), input.clone());
                            }
                            Err(poisoned) => {
                                eprintln!("Warning: session state lock was poisoned; recovering.");
                                let mut guard = poisoned.into_inner();
                                guard.define_vars.insert(key.clone(), input.clone());
                            }
                        }
                    }
                    input
                }
                .to_string();

                if in_single_quote || in_q_quote {
                    if let Some(stripped) =
                        SqlEditorWidget::strip_wrapping_single_quotes(&replacement)
                    {
                        replacement = stripped;
                    }
                }

                result.push_str(&replacement);
                i = j;
                continue;
            }

            if in_q_quote {
                result.push(c);
                if Some(c) == q_quote_end && next == Some('\'') {
                    result.push('\'');
                    in_q_quote = false;
                    q_quote_end = None;
                    i += 2;
                    continue;
                }
                i += 1;
                continue;
            }

            if in_single_quote {
                result.push(c);
                if c == '\'' {
                    if next == Some('\'') {
                        result.push('\'');
                        i += 2;
                        continue;
                    }
                    in_single_quote = false;
                }
                i += 1;
                continue;
            }

            if in_double_quote {
                result.push(c);
                if c == '"' {
                    in_double_quote = false;
                }
                i += 1;
                continue;
            }

            // Handle nq'[...]' (National Character q-quoted strings)
            if (c == 'n' || c == 'N')
                && (next == Some('q') || next == Some('Q'))
                && i + 2 < len
                && chars[i + 2] == '\''
                && i + 3 < len
            {
                let delimiter = chars[i + 3];
                result.push(c);
                result.push(chars[i + 1]);
                result.push('\'');
                result.push(delimiter);
                in_q_quote = true;
                q_quote_end = Some(sql_text::q_quote_closing(delimiter));
                i += 4;
                continue;
            }

            // Handle q'[...]' (q-quoted strings)
            if (c == 'q' || c == 'Q') && next == Some('\'') && next2.is_some() {
                let delimiter = chars[i + 2];
                result.push(c);
                result.push('\'');
                result.push(delimiter);
                in_q_quote = true;
                q_quote_end = Some(sql_text::q_quote_closing(delimiter));
                i += 3;
                continue;
            }

            if c == '\'' {
                result.push(c);
                in_single_quote = true;
                i += 1;
                continue;
            }

            if c == '"' {
                result.push(c);
                in_double_quote = true;
                i += 1;
                continue;
            }

            result.push(c);
            i += 1;
        }

        Ok(result)
    }

    fn prompt_for_input_with_sender(
        sender: &mpsc::Sender<QueryProgress>,
        prompt: &str,
    ) -> Result<String, String> {
        let (response_tx, response_rx) = mpsc::channel();
        if sender
            .send(QueryProgress::PromptInput {
                prompt: prompt.to_string(),
                response: response_tx,
            })
            .is_err()
        {
            return Err("Substitution prompt failed: UI disconnected.".to_string());
        }

        match response_rx.recv_timeout(Duration::from_secs(300)) {
            Ok(Some(value)) => Ok(value),
            Ok(None) => Err("Substitution prompt cancelled.".to_string()),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                Err("Substitution prompt timed out.".to_string())
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err("Substitution prompt failed: UI disconnected.".to_string())
            }
        }
    }

    pub fn prompt_input_dialog(prompt: &str) -> Option<String> {
        let current_group = fltk::group::Group::try_current();
        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let mut dialog = fltk::window::Window::default()
            .with_size(420, 150)
            .with_label("Input");
        crate::ui::center_on_main(&mut dialog);
        dialog.set_color(theme::panel_raised());
        dialog.make_modal(true);

        let mut main_flex = Flex::default().with_pos(10, 10).with_size(400, 130);
        main_flex.set_type(FlexType::Column);
        main_flex.set_spacing(8);

        let mut prompt_frame = Frame::default().with_label(prompt);
        prompt_frame.set_label_color(theme::text_primary());
        prompt_frame.set_align(Align::Left | Align::Inside | Align::Wrap);
        main_flex.fixed(&prompt_frame, 48);

        let mut input = Input::default();
        input.set_color(theme::input_bg());
        input.set_text_color(theme::text_primary());
        input.set_trigger(CallbackTrigger::EnterKeyAlways);
        main_flex.fixed(&input, 30);

        let mut button_flex = Flex::default();
        button_flex.set_type(FlexType::Row);
        button_flex.set_spacing(8);

        let _spacer = Frame::default();

        let mut ok_btn = Button::default()
            .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
            .with_label("OK");
        ok_btn.set_color(theme::button_primary());
        ok_btn.set_label_color(theme::text_primary());
        ok_btn.set_frame(FrameType::RFlatBox);

        let mut cancel_btn = Button::default()
            .with_size(BUTTON_WIDTH, BUTTON_HEIGHT)
            .with_label("Cancel");
        cancel_btn.set_color(theme::button_cancel());
        cancel_btn.set_label_color(theme::text_primary());
        cancel_btn.set_frame(FrameType::RFlatBox);

        button_flex.fixed(&ok_btn, BUTTON_WIDTH);
        button_flex.fixed(&cancel_btn, BUTTON_WIDTH);
        button_flex.end();
        main_flex.fixed(&button_flex, BUTTON_ROW_HEIGHT);
        main_flex.end();
        dialog.end();
        fltk::group::Group::set_current(current_group.as_ref());

        let result: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let cancelled = Arc::new(Mutex::new(false));

        {
            let result = result.clone();
            let mut dialog = dialog.clone();
            let input = input.clone();
            ok_btn.set_callback(move |_| {
                *result
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(input.value());
                dialog.hide();
            });
        }

        {
            let cancelled = cancelled.clone();
            let mut dialog = dialog.clone();
            cancel_btn.set_callback(move |_| {
                *cancelled
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
                dialog.hide();
            });
        }

        {
            let result = result.clone();
            let mut input_cb = input.clone();
            let input_value = input.clone();
            let mut dialog_cb = dialog.clone();
            input_cb.set_callback(move |_| {
                *result
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(input_value.value());
                dialog_cb.hide();
            });
        }

        {
            let cancelled = cancelled.clone();
            let mut dialog_cb = dialog.clone();
            let mut dialog_handle = dialog.clone();
            dialog_cb.set_callback(move |_| {
                *cancelled
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
                dialog_handle.hide();
            });
        }

        dialog.show();
        input.take_focus().ok();

        while dialog.shown() {
            app::wait();
        }

        // Explicitly destroy top-level dialog widgets to release native resources.
        fltk::window::Window::delete(dialog);

        if *cancelled
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
        {
            None
        } else {
            result
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    fn format_define_value(name: &str, bind: &BindVar) -> Result<String, String> {
        let BindValue::Scalar(value) = &bind.value else {
            return Err(format!(
                "Substitution variable &{} must be a scalar value.",
                name
            ));
        };

        let value = value
            .as_ref()
            .ok_or_else(|| format!("Substitution variable &{} has no value.", name))?;

        if value.eq_ignore_ascii_case("NULL") {
            return Ok("NULL".to_string());
        }

        match bind.data_type {
            crate::db::session::BindDataType::Number => Ok(value.clone()),
            crate::db::session::BindDataType::Date
            | crate::db::session::BindDataType::Timestamp(_)
            | crate::db::session::BindDataType::Varchar2(_)
            | crate::db::session::BindDataType::Clob => {
                Ok(format!("'{}'", SqlEditorWidget::escape_sql_literal(value)))
            }
            crate::db::session::BindDataType::RefCursor => Err(format!(
                "Substitution variable &{} cannot be a REFCURSOR.",
                name
            )),
        }
    }

    fn strip_wrapping_single_quotes(value: &str) -> Option<String> {
        let trimmed = value.trim();
        if trimmed.len() < 2 {
            return None;
        }
        if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
            Some(trimmed[1..trimmed.len() - 1].to_string())
        } else {
            None
        }
    }

    pub(super) fn requires_transaction_first_statement(items: &[ScriptItem]) -> bool {
        let first_statement = items.iter().find_map(|item| match item {
            ScriptItem::Statement(statement) => Some(statement.as_str()),
            ScriptItem::ToolCommand(_) => None,
        });
        first_statement
            .map(Self::is_transaction_first_statement)
            .unwrap_or(false)
    }

    fn is_transaction_first_statement(statement: &str) -> bool {
        let trimmed = statement.trim().trim_end_matches(';').trim();
        if trimmed.is_empty() {
            return false;
        }
        let upper = trimmed.to_ascii_uppercase();
        crate::sql_text::starts_with_keyword_token(&upper, "SET TRANSACTION")
            || crate::sql_text::starts_with_keyword_token(
                &upper,
                "ALTER SESSION SET ISOLATION_LEVEL",
            )
    }

    fn oracle_read_only_allows_statement(statement: &str) -> bool {
        let stripped = QueryExecutor::strip_leading_comments(statement);
        let trimmed = stripped.trim().trim_end_matches(';').trim();
        if trimmed.is_empty() {
            return true;
        }

        QueryExecutor::is_select_statement(trimmed)
            || QueryExecutor::is_plain_commit(trimmed)
            || QueryExecutor::is_plain_rollback(trimmed)
            || Self::is_transaction_first_statement(trimmed)
    }

    fn oracle_read_only_block_message() -> String {
        "Error: Oracle read-only mode blocks non-query statements. Switch to Read write to run this statement.".to_string()
    }

    fn sync_serveroutput_with_session(
        conn: &Connection,
        session: &Arc<Mutex<SessionState>>,
    ) -> Result<(), OracleError> {
        let (enabled, size) = match session.lock() {
            Ok(guard) => (guard.server_output.enabled, guard.server_output.size),
            Err(poisoned) => {
                eprintln!("Warning: session state lock was poisoned; recovering.");
                let guard = poisoned.into_inner();
                (guard.server_output.enabled, guard.server_output.size)
            }
        };

        if enabled {
            let buffer_size = if size == 0 { None } else { Some(size) };
            QueryExecutor::enable_dbms_output(conn, buffer_size)
        } else {
            QueryExecutor::disable_dbms_output(conn)
        }
    }

    pub(crate) fn resolve_serveroutput_enable_size(
        requested_size: Option<u32>,
        current_size: u32,
        default_size: u32,
    ) -> u32 {
        match requested_size {
            Some(size) => size,
            None if current_size == 0 => default_size,
            None => current_size,
        }
    }

    fn emit_dbms_output(
        sender: &mpsc::Sender<QueryProgress>,
        _conn_name: &str,
        conn: &Connection,
        session: &Arc<Mutex<SessionState>>,
        _result_index: &mut usize,
    ) -> Result<(), OracleError> {
        let (enabled, size) = match session.lock() {
            Ok(guard) => (guard.server_output.enabled, guard.server_output.size),
            Err(poisoned) => {
                eprintln!("Warning: session state lock was poisoned; recovering.");
                let guard = poisoned.into_inner();
                (guard.server_output.enabled, guard.server_output.size)
            }
        };

        if !enabled {
            return Ok(());
        }

        let max_lines = if size == 0 {
            10_000
        } else {
            safe_div(size, 80).clamp(1, 10_000)
        };
        let lines = QueryExecutor::get_dbms_output(conn, max_lines)?;
        if lines.is_empty() {
            return Ok(());
        }

        let mut output_lines = Vec::with_capacity(lines.len() + 1);
        output_lines.push("DBMS_OUTPUT".to_string());
        output_lines.extend(lines);
        SqlEditorWidget::emit_script_output(sender, session, output_lines);
        Ok(())
    }

    fn ensure_plsql_terminator(sql: &str) -> String {
        let trimmed = sql.trim_end();
        if trimmed.ends_with(';') {
            trimmed.to_string()
        } else {
            format!("{};", trimmed)
        }
    }

    fn strip_leading_comments(sql: &str) -> String {
        let mut remaining = sql;

        loop {
            let trimmed = remaining.trim_start();

            if trimmed.starts_with("--") {
                if let Some(line_end) = trimmed.find('\n') {
                    remaining = &trimmed[line_end + 1..];
                    continue;
                }
                return String::new();
            }

            if trimmed.starts_with("/*") {
                if let Some(block_end) = trimmed.find("*/") {
                    remaining = &trimmed[block_end + 2..];
                    continue;
                }
                return String::new();
            }

            if matches!(
                trimmed.split_whitespace().next(),
                Some(first) if first.eq_ignore_ascii_case("REM")
                    || first.eq_ignore_ascii_case("REMARK")
            ) {
                if let Some(line_end) = trimmed.find('\n') {
                    remaining = &trimmed[line_end + 1..];
                    continue;
                }
                return String::new();
            }

            return trimmed.to_string();
        }
    }

    fn oracle_statement_sets_current_schema(sql: &str) -> bool {
        let cleaned = Self::strip_leading_comments(sql);
        let upper = cleaned.to_uppercase();
        if !upper.starts_with("ALTER SESSION") {
            return false;
        }

        upper.split_whitespace().any(|token| {
            token
                .split('=')
                .next()
                .unwrap_or(token)
                .trim_matches(|ch: char| matches!(ch, '"' | '\'' | '(' | ')' | ',' | ';'))
                == "CURRENT_SCHEMA"
        })
    }

    fn normalize_object_name(value: &str) -> String {
        let trimmed = value.trim();
        if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
            trimmed.trim_matches('"').to_string()
        } else {
            trimmed.to_uppercase()
        }
    }

    pub(super) fn oracle_session_may_have_uncommitted_work(
        conn: &Connection,
        log_context: &str,
    ) -> bool {
        let sql = "SELECT DBMS_TRANSACTION.LOCAL_TRANSACTION_ID(FALSE) FROM dual";
        let result = (|| -> Result<bool, OracleError> {
            let mut stmt = conn.statement(sql).build()?;
            let row = stmt.query_row(&[])?;
            let transaction_id: Option<String> = row.get(0)?;
            Ok(transaction_id
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty()))
        })();

        match result {
            Ok(has_transaction) => has_transaction,
            Err(err) => {
                crate::utils::logging::log_error(
                    log_context,
                    &format!("Failed to inspect Oracle pooled session transaction state: {err}"),
                );
                true
            }
        }
    }

    fn mysql_session_may_have_uncommitted_work<C: Queryable>(
        conn: &mut C,
        log_context: &str,
        fallback_on_error: bool,
    ) -> bool {
        let sql = "\
            SELECT COUNT(*) \
            FROM information_schema.innodb_trx \
            WHERE trx_mysql_thread_id = CONNECTION_ID()";
        match conn.query_first::<u64, _>(sql) {
            Ok(Some(value)) => value != 0,
            Ok(None) => false,
            Err(err) => {
                crate::utils::logging::log_error(
                    log_context,
                    &format!("Failed to inspect MySQL pooled session transaction state: {err}"),
                );
                fallback_on_error
            }
        }
    }

    fn mysql_pooled_session_may_need_preservation<C: Queryable>(
        conn: &mut C,
        log_context: &str,
        prior_may_have_uncommitted_work: bool,
        state_hint: MySqlSessionStateHint,
        fallback_on_error: bool,
    ) -> bool {
        let has_active_transaction =
            Self::mysql_session_may_have_uncommitted_work(conn, log_context, fallback_on_error);
        if state_hint.clears_session_state {
            state_hint.may_leave_session_bound_state || has_active_transaction
        } else {
            prior_may_have_uncommitted_work
                || state_hint.may_leave_session_bound_state
                || has_active_transaction
        }
    }

    fn mysql_set_autocommit_value(sql: &str) -> Option<bool> {
        let cleaned = QueryExecutor::strip_leading_comments(sql);
        let mut normalized = cleaned
            .trim()
            .trim_end_matches(';')
            .trim()
            .to_ascii_uppercase();
        normalized.retain(|ch| !ch.is_whitespace());
        let value = normalized
            .strip_prefix("SETAUTOCOMMIT=")
            .or_else(|| normalized.strip_prefix("SETSESSIONAUTOCOMMIT="))?;
        match value {
            "1" | "ON" | "TRUE" => Some(true),
            "0" | "OFF" | "FALSE" => Some(false),
            _ => None,
        }
    }

    fn mysql_create_statement_is_temporary(sql: &str) -> bool {
        let cleaned = QueryExecutor::strip_leading_comments(sql);
        let mut words = cleaned
            .trim()
            .split_whitespace()
            .map(|word| word.trim_matches(|ch: char| !sql_text::is_identifier_char(ch)));
        matches!(words.next(), Some(word) if word.eq_ignore_ascii_case("CREATE"))
            && matches!(words.next(), Some(word) if word.eq_ignore_ascii_case("TEMPORARY"))
    }

    fn mysql_rollback_targets_savepoint(sql: &str) -> bool {
        let cleaned = QueryExecutor::strip_leading_comments(sql);
        let mut words = cleaned
            .trim()
            .trim_end_matches(';')
            .split_whitespace()
            .map(|word| word.trim_matches(|ch: char| !sql_text::is_identifier_char(ch)));
        matches!(words.next(), Some(word) if word.eq_ignore_ascii_case("ROLLBACK"))
            && matches!(words.next(), Some(word) if word.eq_ignore_ascii_case("TO"))
    }

    fn mysql_transaction_control_starts_chain(sql: &str) -> bool {
        let cleaned = QueryExecutor::strip_leading_comments(sql);
        let mut previous_was_and = false;
        for word in cleaned
            .trim()
            .trim_end_matches(';')
            .split_whitespace()
            .skip(1)
            .map(|word| word.trim_matches(|ch: char| !sql_text::is_identifier_char(ch)))
        {
            if previous_was_and && word.eq_ignore_ascii_case("CHAIN") {
                return true;
            }
            previous_was_and = word.eq_ignore_ascii_case("AND");
        }
        false
    }

    pub(super) fn mysql_session_state_hint_for_sql(sql: &str) -> MySqlSessionStateHint {
        if QueryExecutor::is_plain_commit(sql) || QueryExecutor::is_plain_rollback(sql) {
            return MySqlSessionStateHint {
                clears_session_state: true,
                may_leave_session_bound_state: false,
            };
        }

        if let Some(enabled) = Self::mysql_set_autocommit_value(sql) {
            return MySqlSessionStateHint {
                clears_session_state: enabled,
                may_leave_session_bound_state: !enabled,
            };
        }

        let leading = QueryExecutor::leading_keyword(sql);
        match leading.as_deref() {
            Some("COMMIT") => {
                if Self::mysql_transaction_control_starts_chain(sql) {
                    MySqlSessionStateHint {
                        clears_session_state: false,
                        may_leave_session_bound_state: true,
                    }
                } else {
                    MySqlSessionStateHint {
                        clears_session_state: true,
                        may_leave_session_bound_state: false,
                    }
                }
            }
            Some("ROLLBACK") if !Self::mysql_rollback_targets_savepoint(sql) => {
                if Self::mysql_transaction_control_starts_chain(sql) {
                    MySqlSessionStateHint {
                        clears_session_state: false,
                        may_leave_session_bound_state: true,
                    }
                } else {
                    MySqlSessionStateHint {
                        clears_session_state: true,
                        may_leave_session_bound_state: false,
                    }
                }
            }
            Some("START") | Some("BEGIN") | Some("SAVEPOINT") | Some("CALL") | Some("XA") => {
                MySqlSessionStateHint {
                    clears_session_state: false,
                    may_leave_session_bound_state: true,
                }
            }
            Some("LOCK") => MySqlSessionStateHint {
                clears_session_state: true,
                may_leave_session_bound_state: true,
            },
            Some("UNLOCK") => MySqlSessionStateHint {
                clears_session_state: true,
                may_leave_session_bound_state: false,
            },
            Some("CREATE") if Self::mysql_create_statement_is_temporary(sql) => {
                MySqlSessionStateHint {
                    clears_session_state: false,
                    may_leave_session_bound_state: true,
                }
            }
            Some("CREATE") | Some("ALTER") | Some("DROP") | Some("RENAME") | Some("TRUNCATE") => {
                MySqlSessionStateHint {
                    clears_session_state: true,
                    may_leave_session_bound_state: false,
                }
            }
            Some("SET") => MySqlSessionStateHint {
                clears_session_state: false,
                may_leave_session_bound_state: true,
            },
            Some("SELECT") if sql.to_ascii_uppercase().contains("GET_LOCK") => {
                MySqlSessionStateHint {
                    clears_session_state: false,
                    may_leave_session_bound_state: true,
                }
            }
            _ => MySqlSessionStateHint::default(),
        }
    }

    fn ddl_message(sql_upper: &str) -> String {
        QueryExecutor::ddl_message(sql_upper)
    }

    pub(super) fn timeout_error_message_contains_timeout_signal(message: &str) -> bool {
        let lowered = message.trim().to_ascii_lowercase();
        // Keep timeout detection strict so non-call timeout errors
        // (e.g. lock wait timeout expired) are not misclassified.
        lowered.contains("dpi-1067")
            || lowered.contains("call timeout")
            || lowered.contains("query timed out")
            || lowered.contains("timed out after")
    }

    fn is_timeout_error(err: &OracleError) -> bool {
        Self::timeout_error_message_contains_timeout_signal(&err.to_string())
    }

    fn is_cancel_error(err: &OracleError) -> bool {
        let message = err.to_string();
        message.contains("ORA-01013")
    }

    fn timeout_message(timeout: Option<Duration>) -> String {
        match timeout {
            Some(duration) => format!("Query timed out after {} seconds", duration.as_secs()),
            None => "Query timed out".to_string(),
        }
    }

    fn cancel_message() -> String {
        "Query cancelled".to_string()
    }

    fn lazy_fetch_query_timeout(_query_timeout: Option<Duration>) -> Option<Duration> {
        // Match Toad-style grid fetching: an open lazy cursor is kept alive until
        // EOF, explicit cancel, or disconnect. Query timeout applies to normal
        // executions, not to idle/incremental grid fetches.
        None
    }

    fn oracle_error_allows_session_reuse(err: &OracleError) -> bool {
        if Self::is_cancel_error(err) || Self::is_timeout_error(err) {
            return false;
        }

        Self::oracle_error_message_allows_session_reuse(&err.to_string())
    }

    fn invalidate_oracle_pooled_session_after_error(
        cleanup: &mut QueryExecutionCleanupGuard,
        err: &OracleError,
    ) {
        if !Self::oracle_error_allows_session_reuse(err) {
            cleanup.invalidate_oracle_pooled_session();
        }
    }

    pub(super) fn oracle_error_message_allows_session_reuse(message: &str) -> bool {
        let trimmed = message.trim();
        let lower = trimmed.to_ascii_lowercase();
        if trimmed == Self::cancel_message()
            || lower.contains("ora-01013")
            || Self::timeout_error_message_contains_timeout_signal(trimmed)
        {
            return false;
        }

        ![
            "dpi-1010",
            "dpi-1002",
            "dpi-1080",
            "not connected",
            "closed connection",
            "connection closed",
            "connection lost contact",
            "ora-00028",
            "ora-02396",
            "ora-01012",
            "ora-01033",
            "ora-01034",
            "ora-01089",
            "ora-03106",
            "ora-03108",
            "ora-03111",
            "ora-03113",
            "ora-03114",
            "ora-03115",
            "ora-03135",
            "ora-03136",
            "ora-03137",
            "ora-03138",
            "ora-12153",
            "ora-12170",
            "ora-12514",
            "ora-12537",
            "ora-12541",
            "ora-12547",
            "ora-12570",
            "ora-12571",
            "ora-12592",
            "ora-12637",
            "ora-25408",
            "ora-28547",
            "end-of-file on communication channel",
            "exceeded maximum idle time",
            "failed to apply oracle call timeout",
            "failed to reset oracle call timeout",
            "connection reset",
            "connection timed out",
            "broken pipe",
            "dpi-1019",
            "dpi-1041",
            "tns:",
        ]
        .iter()
        .any(|needle| lower.contains(needle))
    }

    fn mysql_error_allows_session_reuse(message: &str) -> bool {
        let trimmed = message.trim();
        if trimmed == Self::cancel_message()
            || Self::timeout_error_message_contains_timeout_signal(trimmed)
        {
            return false;
        }

        let lower = trimmed.to_ascii_lowercase();
        ![
            "bad handshake",
            "broken pipe",
            "connection aborted",
            "connection closed",
            "connection lost",
            "connection refused",
            "connection reset",
            "connection timed out",
            "connection was killed",
            "commands out of sync",
            "communications link failure",
            "can't connect to mysql server",
            "driver error",
            "drivererror",
            "error 2006",
            "error 2013",
            "failed to read packet",
            "failed to read from socket",
            "failed to receive packet",
            "failed to write to socket",
            "lost connection",
            "malformed packet",
            "network is unreachable",
            "no connection available",
            "not connected to database",
            "operation timed out",
            "packet out of order",
            "packets out of order",
            "pool disconnected",
            "query execution was interrupted",
            "query was killed",
            "server closed the connection",
            "server has closed the connection",
            "server has gone away",
            "server shutdown in progress",
            "unexpected eof",
        ]
        .iter()
        .any(|needle| lower.contains(needle))
    }

    pub(super) fn abort_if_cancelled(cancel_flag: &Arc<Mutex<bool>>) -> Result<(), String> {
        if load_mutex_bool(cancel_flag) {
            Err(Self::cancel_message())
        } else {
            Ok(())
        }
    }

    fn reset_mysql_timeout_on_connection(
        mysql_conn: &mut mysql::Conn,
        query_timeout: Option<Duration>,
        log_context: &str,
    ) {
        if query_timeout.is_none() {
            return;
        }

        if let Err(err) =
            crate::db::query::mysql_executor::MysqlExecutor::apply_session_timeout(mysql_conn, None)
        {
            crate::utils::logging::log_error(
                log_context,
                &format!("Failed to reset MySQL session timeout: {err}"),
            );
        }
    }

    fn reset_mysql_timeout(
        conn_guard: &mut crate::db::DatabaseConnection,
        query_timeout: Option<Duration>,
        log_context: &str,
    ) {
        if let Some(mysql_conn) = conn_guard.get_mysql_connection_mut() {
            Self::reset_mysql_timeout_on_connection(mysql_conn, query_timeout, log_context);
        }
    }

    fn reusable_mysql_pooled_session_is_ready(
        conn: &mut mysql::PooledConn,
        current_service_name: &str,
        advanced: &crate::db::ConnectionAdvancedSettings,
        preserve_existing_session_state: bool,
    ) -> Result<bool, String> {
        if conn.as_mut().ping().is_err() {
            return Ok(false);
        }
        if preserve_existing_session_state {
            return Ok(true);
        }
        crate::db::DatabaseConnection::apply_mysql_session_settings(conn, advanced)?;

        match Self::prepare_mysql_pooled_session_database(conn, current_service_name, advanced) {
            Ok(()) => Ok(true),
            Err(message) if Self::mysql_error_allows_session_reuse(&message) => Err(message),
            Err(_) => Ok(false),
        }
    }

    fn prepare_mysql_pooled_session_or_retry_once(
        context: &crate::db::DbPoolSessionContext,
        session_pool_sender: Option<&mpsc::Sender<QueryProgress>>,
        mut conn: mysql::PooledConn,
    ) -> Result<mysql::PooledConn, String> {
        match Self::prepare_mysql_pooled_session_database(
            &mut conn,
            &context.current_service_name,
            &context.connection_info.advanced,
        ) {
            Ok(()) => Ok(conn),
            Err(message) if !Self::mysql_error_allows_session_reuse(&message) => {
                drop(conn);
                let mut conn =
                    Self::acquire_fresh_mysql_pool_session(context, session_pool_sender)?;
                Self::prepare_mysql_pooled_session_database(
                    &mut conn,
                    &context.current_service_name,
                    &context.connection_info.advanced,
                )?;
                Ok(conn)
            }
            Err(message) => Err(message),
        }
    }

    fn acquire_mysql_pooled_session(
        shared_connection: &crate::db::SharedConnection,
        pooled_db_session: &SharedDbSessionLease,
        db_activity: &str,
        auto_commit: bool,
        session_pool_sender: Option<&mpsc::Sender<QueryProgress>>,
    ) -> Result<(u64, ConnectionInfo, mysql::PooledConn, bool), String> {
        let context = {
            let conn_guard =
                lock_connection_with_activity(shared_connection, db_activity.to_string());
            conn_guard.pool_session_context_for(crate::db::DatabaseType::MySQL)?
        };

        let (mut conn, prior_may_have_uncommitted_work) = if let Some((
            lease,
            prior_may_have_uncommitted_work,
        )) =
            crate::db::take_reusable_pooled_session_lease_with_state(
                pooled_db_session,
                context.connection_generation,
                crate::db::DatabaseType::MySQL,
            )
            .and_then(|(lease, prior)| lease.into_mysql_connection().map(|conn| (conn, prior)))
        {
            let mut conn = lease;
            match Self::reusable_mysql_pooled_session_is_ready(
                &mut conn,
                &context.current_service_name,
                &context.connection_info.advanced,
                prior_may_have_uncommitted_work,
            ) {
                Ok(true) => (conn, prior_may_have_uncommitted_work),
                Ok(false) => {
                    drop(conn);
                    let conn =
                        Self::acquire_fresh_mysql_pool_session(&context, session_pool_sender)?;
                    (
                        Self::prepare_mysql_pooled_session_or_retry_once(
                            &context,
                            session_pool_sender,
                            conn,
                        )?,
                        false,
                    )
                }
                Err(message) if Self::mysql_pool_acquire_error_should_retry_fresh(&message) => {
                    crate::utils::logging::log_warning(
                        "mysql pool session",
                        &format!(
                            "Discarding stale reusable MySQL pooled session and retrying with a fresh session: {message}"
                        ),
                    );
                    drop(conn);
                    let conn =
                        Self::acquire_fresh_mysql_pool_session(&context, session_pool_sender)?;
                    (
                        Self::prepare_mysql_pooled_session_or_retry_once(
                            &context,
                            session_pool_sender,
                            conn,
                        )?,
                        false,
                    )
                }
                Err(message) => return Err(message),
            }
        } else {
            let conn = Self::acquire_fresh_mysql_pool_session(&context, session_pool_sender)?;
            (
                Self::prepare_mysql_pooled_session_or_retry_once(
                    &context,
                    session_pool_sender,
                    conn,
                )?,
                false,
            )
        };
        if let Err(message) = Self::apply_mysql_pooled_execution_session_settings(
            &mut conn,
            auto_commit,
            context.transaction_mode,
            prior_may_have_uncommitted_work,
        ) {
            if Self::mysql_pool_acquire_error_should_retry_fresh(&message) {
                crate::utils::logging::log_warning(
                    "mysql pool session",
                    &format!(
                        "MySQL pooled session setup failed with a stale-session error; retrying once: {message}"
                    ),
                );
                drop(conn);
                let mut fresh_conn =
                    Self::acquire_fresh_mysql_pool_session(&context, session_pool_sender)?;
                fresh_conn = Self::prepare_mysql_pooled_session_or_retry_once(
                    &context,
                    session_pool_sender,
                    fresh_conn,
                )?;
                Self::apply_mysql_pooled_execution_session_settings(
                    &mut fresh_conn,
                    auto_commit,
                    context.transaction_mode,
                    false,
                )?;
                return Ok((
                    context.connection_generation,
                    context.connection_info,
                    fresh_conn,
                    false,
                ));
            }

            if prior_may_have_uncommitted_work && Self::mysql_error_allows_session_reuse(&message) {
                Self::release_mysql_pooled_session_if_current(
                    shared_connection,
                    pooled_db_session,
                    context.connection_generation,
                    conn,
                    prior_may_have_uncommitted_work,
                    db_activity,
                );
            } else {
                drop(conn);
            }
            return Err(message);
        }
        Ok((
            context.connection_generation,
            context.connection_info,
            conn,
            prior_may_have_uncommitted_work,
        ))
    }

    fn apply_mysql_pooled_execution_session_settings(
        conn: &mut mysql::PooledConn,
        auto_commit: bool,
        transaction_mode: crate::db::TransactionMode,
        preserve_existing_session_state: bool,
    ) -> Result<(), String> {
        for statement in Self::mysql_pooled_execution_session_setup_statements(
            auto_commit,
            transaction_mode,
            preserve_existing_session_state,
        )? {
            conn.query_drop(statement.as_str()).map_err(|err| {
                let message = SqlEditorWidget::mysql_error_message(&err, None);
                if statement.starts_with("SET SESSION TRANSACTION") {
                    format!("Failed to apply transaction mode: {message}")
                } else {
                    message
                }
            })?;
        }
        Ok(())
    }

    fn mysql_pooled_execution_session_setup_statements(
        auto_commit: bool,
        transaction_mode: crate::db::TransactionMode,
        preserve_existing_session_state: bool,
    ) -> Result<Vec<String>, String> {
        if preserve_existing_session_state {
            return Ok(Vec::new());
        }

        let mut statements = vec![if auto_commit {
            "SET autocommit=1".to_string()
        } else {
            "SET autocommit=0".to_string()
        }];
        statements.extend(
            crate::db::DatabaseConnection::transaction_mode_statements_for(
                crate::db::DatabaseType::MySQL,
                transaction_mode,
            )?,
        );
        Ok(statements)
    }

    fn release_mysql_pooled_session(
        pooled_db_session: &SharedDbSessionLease,
        connection_generation: u64,
        conn: mysql::PooledConn,
        may_have_uncommitted_work: bool,
    ) {
        crate::db::store_pooled_session_lease_if_empty(
            pooled_db_session,
            connection_generation,
            DbSessionLease::MySQL(conn),
            may_have_uncommitted_work,
        );
    }

    fn release_mysql_pooled_session_if_current(
        shared_connection: &crate::db::SharedConnection,
        pooled_db_session: &SharedDbSessionLease,
        connection_generation: u64,
        conn: mysql::PooledConn,
        may_have_uncommitted_work: bool,
        db_activity: &str,
    ) {
        let should_release = {
            let conn_guard =
                lock_connection_with_activity(shared_connection, db_activity.to_string());
            conn_guard.can_reuse_pool_session(connection_generation, crate::db::DatabaseType::MySQL)
        };
        if should_release {
            Self::release_mysql_pooled_session(
                pooled_db_session,
                connection_generation,
                conn,
                may_have_uncommitted_work,
            );
        } else {
            drop(conn);
        }
    }

    fn apply_mysql_autocommit_to_reusable_pooled_session(
        shared_connection: &crate::db::SharedConnection,
        pooled_db_session: &SharedDbSessionLease,
        connection_generation: u64,
        enabled: bool,
        db_activity: &str,
    ) -> Result<(), String> {
        let Some((mut conn, prior_may_have_uncommitted_work)) =
            crate::db::take_reusable_pooled_session_lease_with_state(
                pooled_db_session,
                connection_generation,
                crate::db::DatabaseType::MySQL,
            )
            .and_then(|(lease, prior)| lease.into_mysql_connection().map(|conn| (conn, prior)))
        else {
            return Ok(());
        };

        if let Err(err) = conn.query_drop(if enabled {
            "SET autocommit=1"
        } else {
            "SET autocommit=0"
        }) {
            let message = SqlEditorWidget::mysql_error_message(&err, None);
            if prior_may_have_uncommitted_work && Self::mysql_error_allows_session_reuse(&message) {
                Self::release_mysql_pooled_session_if_current(
                    shared_connection,
                    pooled_db_session,
                    connection_generation,
                    conn,
                    prior_may_have_uncommitted_work,
                    db_activity,
                );
            }
            return Err(message);
        }

        let state_hint = MySqlSessionStateHint {
            clears_session_state: enabled,
            may_leave_session_bound_state: !enabled,
        };
        let fallback_on_error = if enabled {
            false
        } else {
            prior_may_have_uncommitted_work
        };
        let may_have_uncommitted_work = Self::mysql_pooled_session_may_need_preservation(
            &mut conn,
            db_activity,
            prior_may_have_uncommitted_work,
            state_hint,
            fallback_on_error,
        );
        Self::release_mysql_pooled_session_if_current(
            shared_connection,
            pooled_db_session,
            connection_generation,
            conn,
            may_have_uncommitted_work,
            db_activity,
        );
        Ok(())
    }

    fn mysql_pooled_action_can_reuse_session<T>(
        result: &thread::Result<Result<T, String>>,
    ) -> bool {
        match result {
            Ok(Ok(_)) => true,
            Ok(Err(message)) => Self::mysql_error_allows_session_reuse(message),
            Err(_) => false,
        }
    }

    fn sync_mysql_pooled_session_info(
        shared_connection: &crate::db::SharedConnection,
        conn: &mut mysql::PooledConn,
        db_activity: &str,
        connection_generation: u64,
        refresh_encoding: bool,
    ) -> bool {
        let mut conn_guard =
            lock_connection_with_activity(shared_connection, db_activity.to_string());
        if !conn_guard.can_reuse_pool_session(connection_generation, crate::db::DatabaseType::MySQL)
        {
            return false;
        }
        match conn_guard.sync_mysql_current_database_name_from_session(conn, refresh_encoding) {
            Ok(_) => true,
            Err(err) => {
                eprintln!("Warning: failed to sync MySQL pooled session metadata: {err}");
                false
            }
        }
    }

    fn sync_oracle_pooled_session_current_schema(
        shared_connection: &crate::db::SharedConnection,
        conn: &Arc<Connection>,
        db_activity: &str,
        connection_generation: u64,
    ) -> bool {
        let mut conn_guard =
            lock_connection_with_activity(shared_connection, db_activity.to_string());
        if !conn_guard
            .can_reuse_pool_session(connection_generation, crate::db::DatabaseType::Oracle)
        {
            return false;
        }

        match conn_guard.sync_oracle_current_schema_from_session(conn.as_ref()) {
            Ok(_) => true,
            Err(err) => {
                eprintln!("Warning: failed to sync Oracle pooled session schema: {err}");
                false
            }
        }
    }

    pub(super) fn run_mysql_action_with_timeout<T, F>(
        conn_guard: &mut crate::db::DatabaseConnection,
        current_mysql_cancel_context: &Arc<Mutex<Option<MySqlQueryCancelContext>>>,
        cancel_flag: &Arc<Mutex<bool>>,
        query_timeout: Option<Duration>,
        log_context: &str,
        action: F,
    ) -> Result<T, String>
    where
        F: FnOnce(&mut mysql::Conn) -> Result<T, MysqlError>,
    {
        let connection_id = match conn_guard.get_mysql_connection_mut() {
            Some(mysql_conn) => mysql_conn.connection_id(),
            None => return Err(crate::db::NOT_CONNECTED_MESSAGE.to_string()),
        };

        let connection_info = match conn_guard.mysql_runtime_connection_info() {
            Some(info) => info,
            None => return Err(crate::db::NOT_CONNECTED_MESSAGE.to_string()),
        };

        Self::set_current_mysql_cancel_context(
            current_mysql_cancel_context,
            Some(MySqlQueryCancelContext {
                connection_info,
                connection_id,
            }),
        );

        {
            let Some(mysql_conn) = conn_guard.get_mysql_connection_mut() else {
                Self::set_current_mysql_cancel_context(current_mysql_cancel_context, None);
                return Err(crate::db::NOT_CONNECTED_MESSAGE.to_string());
            };
            if let Err(err) = crate::db::query::mysql_executor::MysqlExecutor::apply_session_timeout(
                mysql_conn,
                query_timeout,
            ) {
                Self::set_current_mysql_cancel_context(current_mysql_cancel_context, None);
                return Err(SqlEditorWidget::mysql_error_message(&err, query_timeout));
            }
            if let Err(cancelled) = Self::abort_if_cancelled(cancel_flag) {
                Self::reset_mysql_timeout_on_connection(mysql_conn, query_timeout, log_context);
                Self::set_current_mysql_cancel_context(current_mysql_cancel_context, None);
                return Err(cancelled);
            }
        }

        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            match conn_guard.get_mysql_connection_mut() {
                Some(mysql_conn) => action(mysql_conn)
                    .map_err(|err| SqlEditorWidget::mysql_error_message(&err, query_timeout)),
                None => Err(crate::db::NOT_CONNECTED_MESSAGE.to_string()),
            }
        }));

        Self::reset_mysql_timeout(conn_guard, query_timeout, log_context);
        Self::set_current_mysql_cancel_context(current_mysql_cancel_context, None);
        match result {
            Ok(result) => result,
            Err(payload) => panic::resume_unwind(payload),
        }
    }

    pub(super) fn run_mysql_pooled_action_with_timeout<T, F>(
        shared_connection: &crate::db::SharedConnection,
        pooled_db_session: &SharedDbSessionLease,
        session_pool_sender: Option<&mpsc::Sender<QueryProgress>>,
        current_mysql_cancel_context: &Arc<Mutex<Option<MySqlQueryCancelContext>>>,
        cancel_flag: &Arc<Mutex<bool>>,
        query_timeout: Option<Duration>,
        log_context: &str,
        auto_commit: bool,
        refresh_encoding_after: bool,
        state_hint: MySqlSessionStateHint,
        action: F,
    ) -> Result<T, String>
    where
        F: FnOnce(&mut mysql::PooledConn) -> Result<T, MysqlError>,
    {
        let (connection_generation, connection_info, mut conn, prior_may_have_uncommitted_work) =
            Self::acquire_mysql_pooled_session(
                shared_connection,
                pooled_db_session,
                log_context,
                auto_commit,
                session_pool_sender,
            )?;

        Self::set_current_mysql_cancel_context(
            current_mysql_cancel_context,
            Some(MySqlQueryCancelContext {
                connection_info,
                connection_id: conn.connection_id(),
            }),
        );

        if let Err(err) = crate::db::query::mysql_executor::MysqlExecutor::apply_session_timeout(
            &mut conn,
            query_timeout,
        ) {
            let message = SqlEditorWidget::mysql_error_message(&err, query_timeout);
            Self::set_current_mysql_cancel_context(current_mysql_cancel_context, None);
            if prior_may_have_uncommitted_work && Self::mysql_error_allows_session_reuse(&message) {
                Self::release_mysql_pooled_session_if_current(
                    shared_connection,
                    pooled_db_session,
                    connection_generation,
                    conn,
                    prior_may_have_uncommitted_work,
                    log_context,
                );
            } else {
                drop(conn);
            }
            return Err(message);
        }
        if let Err(cancelled) = Self::abort_if_cancelled(cancel_flag) {
            if crate::db::query::mysql_executor::MysqlExecutor::apply_session_timeout(
                &mut conn, None,
            )
            .is_ok()
            {
                let may_have_uncommitted_work = Self::mysql_pooled_session_may_need_preservation(
                    &mut conn,
                    log_context,
                    prior_may_have_uncommitted_work,
                    MySqlSessionStateHint::default(),
                    prior_may_have_uncommitted_work,
                );
                Self::set_current_mysql_cancel_context(current_mysql_cancel_context, None);
                Self::release_mysql_pooled_session_if_current(
                    shared_connection,
                    pooled_db_session,
                    connection_generation,
                    conn,
                    may_have_uncommitted_work,
                    log_context,
                );
            } else {
                Self::set_current_mysql_cancel_context(current_mysql_cancel_context, None);
                drop(conn);
            }
            return Err(cancelled);
        }

        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            action(&mut conn)
                .map_err(|err| SqlEditorWidget::mysql_error_message(&err, query_timeout))
        }));

        if let Err(err) =
            crate::db::query::mysql_executor::MysqlExecutor::apply_session_timeout(&mut conn, None)
        {
            crate::utils::logging::log_error(
                log_context,
                &format!("Failed to reset MySQL pooled session timeout: {err}"),
            );
            Self::set_current_mysql_cancel_context(current_mysql_cancel_context, None);
            drop(conn);
            return match result {
                Ok(result) => result,
                Err(payload) => panic::resume_unwind(payload),
            };
        }
        let should_release_session = if Self::mysql_pooled_action_can_reuse_session(&result) {
            Self::sync_mysql_pooled_session_info(
                shared_connection,
                &mut conn,
                log_context,
                connection_generation,
                refresh_encoding_after,
            )
        } else {
            false
        };
        Self::set_current_mysql_cancel_context(current_mysql_cancel_context, None);
        if should_release_session {
            let fallback_on_error = if state_hint.clears_session_state {
                state_hint.may_leave_session_bound_state
            } else {
                prior_may_have_uncommitted_work
                    || state_hint.may_leave_session_bound_state
                    || !auto_commit
            };
            let may_have_uncommitted_work = Self::mysql_pooled_session_may_need_preservation(
                &mut conn,
                log_context,
                prior_may_have_uncommitted_work,
                state_hint,
                fallback_on_error,
            );
            Self::release_mysql_pooled_session_if_current(
                shared_connection,
                pooled_db_session,
                connection_generation,
                conn,
                may_have_uncommitted_work,
                log_context,
            );
        } else {
            drop(conn);
        }
        match result {
            Ok(result) => result,
            Err(payload) => panic::resume_unwind(payload),
        }
    }

    pub(super) fn choose_execution_error_message(
        cancelled: bool,
        timed_out: bool,
        timeout: Option<Duration>,
        fallback: String,
    ) -> String {
        if timed_out {
            Self::timeout_message(timeout)
        } else if cancelled {
            Self::cancel_message()
        } else {
            fallback
        }
    }

    pub(super) fn mysql_error_message(err: &MysqlError, timeout: Option<Duration>) -> String {
        let cancelled = crate::db::query::mysql_executor::MysqlExecutor::is_cancel_error(err);
        let timed_out = crate::db::query::mysql_executor::MysqlExecutor::is_timeout_error(err);
        Self::choose_execution_error_message(cancelled, timed_out, timeout, err.to_string())
    }

    pub(super) fn parse_timeout(value: &str) -> Option<Duration> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return None;
        }

        let secs = match trimmed.parse::<u64>() {
            Ok(secs) => secs,
            Err(err) => {
                eprintln!("Invalid timeout value '{trimmed}': {err}");
                return None;
            }
        };
        if secs == 0 {
            None
        } else {
            Some(Duration::from_secs(secs))
        }
    }

    pub fn set_progress_callback<F>(&mut self, callback: F)
    where
        F: FnMut(QueryProgress) + 'static,
    {
        *self
            .progress_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(callback));
    }
}

#[cfg(test)]
mod oracle_current_schema_statement_tests {
    use super::SqlEditorWidget;

    #[test]
    fn oracle_statement_sets_current_schema_detects_alter_session_command() {
        assert!(SqlEditorWidget::oracle_statement_sets_current_schema(
            "ALTER SESSION SET CURRENT_SCHEMA = SYS"
        ));
        assert!(SqlEditorWidget::oracle_statement_sets_current_schema(
            "/* lead */ ALTER SESSION SET CURRENT_SCHEMA=app_user"
        ));
    }

    #[test]
    fn oracle_statement_sets_current_schema_ignores_other_session_changes() {
        assert!(!SqlEditorWidget::oracle_statement_sets_current_schema(
            "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'"
        ));
        assert!(!SqlEditorWidget::oracle_statement_sets_current_schema(
            "BEGIN NULL; END;"
        ));
    }
}

#[cfg(test)]
mod query_execution_cleanup_tests {
    use super::{
        LazyFetchAllTimeout, LazyFetchCommand, LazyFetchHandle, MySqlQueryCancelContext,
        QueryExecutionCleanupGuard, QueryProgress, SqlEditorWidget,
    };
    use crate::db::{ScriptItem, TransactionAccessMode, TransactionIsolation, TransactionMode};
    use mysql::{Error as MysqlError, MySqlError};
    use oracle::{Connection, Error as OracleError, ErrorKind as OracleErrorKind};
    use std::panic::{self, AssertUnwindSafe};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{mpsc, Arc, Mutex};
    use std::time::{Duration, Instant};

    #[test]
    fn cleanup_guard_resets_cancel_and_emits_batch_finished_on_drop() {
        let (sender, receiver) = mpsc::channel();
        let cancel_flag = Arc::new(Mutex::new(true));
        let query_running = Arc::new(Mutex::new(true));
        let current_query_connection: Arc<Mutex<Option<Arc<Connection>>>> =
            Arc::new(Mutex::new(None));
        let current_mysql_cancel_context: Arc<Mutex<Option<MySqlQueryCancelContext>>> =
            Arc::new(Mutex::new(None));

        {
            let _guard = QueryExecutionCleanupGuard::new(
                sender,
                current_query_connection.clone(),
                current_mysql_cancel_context.clone(),
                cancel_flag.clone(),
                query_running.clone(),
            );
        }

        assert!(!cancel_flag
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .to_owned());
        assert!(!query_running
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .to_owned());
        let msg = receiver
            .try_recv()
            .expect("BatchFinished should be emitted");
        assert!(matches!(msg, QueryProgress::BatchFinished));
        assert!(current_query_connection
            .lock()
            .expect("connection mutex should not be poisoned")
            .is_none());
        assert!(current_mysql_cancel_context
            .lock()
            .expect("MySQL cancel context mutex should not be poisoned")
            .is_none());
    }

    #[test]
    fn cleanup_guard_runs_during_panic_unwind() {
        let (sender, receiver) = mpsc::channel();
        let cancel_flag = Arc::new(Mutex::new(true));
        let query_running = Arc::new(Mutex::new(true));
        let current_query_connection: Arc<Mutex<Option<Arc<Connection>>>> =
            Arc::new(Mutex::new(None));
        let current_mysql_cancel_context: Arc<Mutex<Option<MySqlQueryCancelContext>>> =
            Arc::new(Mutex::new(None));

        let unwind_result = panic::catch_unwind(AssertUnwindSafe({
            let cancel_flag = cancel_flag.clone();
            let current_query_connection = current_query_connection;
            let current_mysql_cancel_context = current_mysql_cancel_context;
            let query_running = query_running.clone();
            move || {
                let _guard = QueryExecutionCleanupGuard::new(
                    sender,
                    current_query_connection,
                    current_mysql_cancel_context,
                    cancel_flag,
                    query_running,
                );
                panic!("simulate execution panic");
            }
        }));

        assert!(unwind_result.is_err());
        assert!(!cancel_flag
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .to_owned());
        assert!(!query_running
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .to_owned());
        let msg = receiver
            .try_recv()
            .expect("BatchFinished should be emitted");
        assert!(matches!(msg, QueryProgress::BatchFinished));
    }

    #[test]
    fn cleanup_guard_drop_tolerates_closed_progress_channel() {
        let (sender, receiver) = mpsc::channel();
        drop(receiver);

        let cancel_flag = Arc::new(Mutex::new(true));
        let query_running = Arc::new(Mutex::new(true));
        let current_query_connection: Arc<Mutex<Option<Arc<Connection>>>> =
            Arc::new(Mutex::new(None));
        let current_mysql_cancel_context: Arc<Mutex<Option<MySqlQueryCancelContext>>> =
            Arc::new(Mutex::new(None));

        let drop_result = panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = QueryExecutionCleanupGuard::new(
                sender,
                current_query_connection,
                current_mysql_cancel_context,
                cancel_flag.clone(),
                query_running.clone(),
            );
        }));

        assert!(drop_result.is_ok(), "Drop must ignore send failures");
        assert!(!cancel_flag
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .to_owned());
        assert!(!query_running
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .to_owned());
    }

    #[test]
    fn cleanup_guard_recovers_from_poisoned_connection_mutex() {
        let (sender, receiver) = mpsc::channel();
        let cancel_flag = Arc::new(Mutex::new(true));
        let query_running = Arc::new(Mutex::new(true));
        let current_query_connection: Arc<Mutex<Option<Arc<Connection>>>> =
            Arc::new(Mutex::new(None));
        let current_mysql_cancel_context: Arc<Mutex<Option<MySqlQueryCancelContext>>> =
            Arc::new(Mutex::new(None));

        let poison_target = current_query_connection.clone();
        let _ = panic::catch_unwind(AssertUnwindSafe(move || {
            let _lock = poison_target
                .lock()
                .expect("mutex lock should succeed before poisoning");
            panic!("poison current_query_connection mutex");
        }));

        {
            let _guard = QueryExecutionCleanupGuard::new(
                sender,
                current_query_connection,
                current_mysql_cancel_context,
                cancel_flag.clone(),
                query_running.clone(),
            );
        }

        assert!(!cancel_flag
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .to_owned());
        assert!(!query_running
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .to_owned());
        let msg = receiver
            .try_recv()
            .expect("BatchFinished should be emitted");
        assert!(matches!(msg, QueryProgress::BatchFinished));
    }

    #[test]
    fn abort_if_cancelled_returns_cancel_message_when_flag_is_set() {
        let cancel_flag = Arc::new(Mutex::new(true));

        let result = SqlEditorWidget::abort_if_cancelled(&cancel_flag);

        assert_eq!(result, Err(SqlEditorWidget::cancel_message()));
    }

    #[test]
    fn lazy_fetch_query_timeout_is_disabled_to_match_grid_fetch_policy() {
        assert_eq!(SqlEditorWidget::lazy_fetch_query_timeout(None), None);
        assert_eq!(
            SqlEditorWidget::lazy_fetch_query_timeout(Some(Duration::from_secs(1))),
            None
        );
    }

    #[test]
    fn lazy_fetch_all_timeout_starts_after_first_row() {
        let mut timeout = LazyFetchAllTimeout::new(Some(Duration::from_secs(5)));

        assert!(!timeout.timed_out());
        assert_eq!(timeout.remaining_after_start(), None);

        timeout.note_row_received();

        assert!(timeout.remaining_after_start().is_some());
        assert!(!timeout.timed_out());
    }

    #[test]
    fn lazy_fetch_all_timeout_expires_after_started_elapsed_time() {
        let mut timeout = LazyFetchAllTimeout::new(Some(Duration::from_secs(1)));
        timeout.note_row_received();
        timeout.started_at = Some(Instant::now() - Duration::from_secs(2));

        assert!(timeout.timed_out());
        assert_eq!(timeout.remaining_after_start(), Some(Duration::ZERO));
    }

    #[test]
    fn lazy_fetch_all_timeout_disabled_without_query_timeout() {
        let mut timeout = LazyFetchAllTimeout::new(None);
        timeout.note_row_received();
        timeout.started_at = Some(Instant::now() - Duration::from_secs(2));

        assert!(!timeout.timed_out());
        assert_eq!(timeout.remaining_after_start(), None);
    }

    #[test]
    fn session_pool_exhaustion_detects_ora_24496_message() {
        assert!(SqlEditorWidget::session_pool_error_is_exhausted(
            "ORA-24496: OCISessionGet() timed out waiting for a free connection"
        ));
    }

    #[test]
    fn session_pool_exhaustion_detects_ora_24418_message() {
        assert!(SqlEditorWidget::session_pool_error_is_exhausted(
            "ORA-24418: Cannot open further sessions."
        ));
    }

    #[test]
    fn session_pool_exhaustion_detects_mysql_pool_timeout_message() {
        assert!(SqlEditorWidget::session_pool_error_is_exhausted(
            "Operation timed out. MySQL connection pool appears exhausted."
        ));
    }

    #[test]
    fn session_pool_exhaustion_detects_raw_mysql_driver_timeout() {
        assert!(SqlEditorWidget::session_pool_error_is_exhausted(
            "DriverError { Operation timed out }"
        ));
    }

    #[test]
    fn session_pool_exhaustion_does_not_treat_plain_network_timeout_as_pool_full() {
        assert!(!SqlEditorWidget::session_pool_error_is_exhausted(
            "I/O error: Operation timed out while connecting to server"
        ));
    }

    #[test]
    fn oracle_pool_retry_classifies_stale_connection_errors_only() {
        assert!(SqlEditorWidget::oracle_pool_acquire_error_should_retry_fresh(
            "Failed to apply Oracle session setting `ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'`: DPI-1010: not connected"
        ));
        assert!(
            SqlEditorWidget::oracle_pool_acquire_error_should_retry_fresh(
                "ORA-03113: end-of-file on communication channel"
            )
        );
        assert!(
            !SqlEditorWidget::oracle_pool_acquire_error_should_retry_fresh(
                "ORA-24418: Cannot open further sessions."
            )
        );
        assert!(
            !SqlEditorWidget::oracle_pool_acquire_error_should_retry_fresh(
                "ORA-01435: user does not exist"
            )
        );
    }

    #[test]
    fn mysql_pool_retry_classifies_stale_connection_errors_only() {
        assert!(SqlEditorWidget::mysql_pool_acquire_error_should_retry_fresh(
            "Failed to apply MySQL session setting `SET SESSION sql_mode = 'TRADITIONAL'`: ERROR 2013 (HY000): Lost connection to MySQL server during query"
        ));
        assert!(
            SqlEditorWidget::mysql_pool_acquire_error_should_retry_fresh(
                "DriverError { Malformed packet }"
            )
        );
        assert!(
            !SqlEditorWidget::mysql_pool_acquire_error_should_retry_fresh(
                "Operation timed out. MySQL connection pool appears exhausted."
            )
        );
        assert!(
            !SqlEditorWidget::mysql_pool_acquire_error_should_retry_fresh(
                "ERROR 1064 (42000): You have an error in your SQL syntax"
            )
        );
    }

    #[test]
    fn mysql_missing_current_database_error_detects_unknown_database() {
        let err = MysqlError::MySqlError(MySqlError {
            state: "42000".to_string(),
            code: 1049,
            message: "Unknown database 'dropped_db'".to_string(),
        });

        assert!(SqlEditorWidget::mysql_missing_current_database_error(&err));
    }

    #[test]
    fn mysql_lazy_fetch_cancel_helper_ignores_missing_context() {
        let context = Arc::new(Mutex::new(None));

        SqlEditorWidget::cancel_mysql_lazy_fetch_query(&context, "test mysql lazy fetch cancel");

        assert!(context
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_none());
    }

    #[test]
    fn lazy_fetch_single_statement_policy_allows_script_mode_single_statement() {
        let items = vec![ScriptItem::Statement("select * from dual".to_string())];

        assert!(SqlEditorWidget::should_use_lazy_fetch_for_single_statement(
            &items
        ));
    }

    #[test]
    fn lazy_fetch_single_statement_policy_rejects_multi_statement_scripts() {
        let items = vec![
            ScriptItem::Statement("select 1 from dual".to_string()),
            ScriptItem::Statement("select 2 from dual".to_string()),
        ];

        assert!(!SqlEditorWidget::should_use_lazy_fetch_for_single_statement(&items));
    }

    #[test]
    fn oracle_read_only_statement_guard_allows_queries_and_transaction_control() {
        for sql in [
            "select * from dual",
            "with q as (select 1 id from dual) select * from q",
            "commit",
            "rollback",
            "set transaction read only",
            "alter session set isolation_level = read committed",
        ] {
            assert!(
                SqlEditorWidget::oracle_read_only_allows_statement(sql),
                "expected Oracle read-only mode to allow: {sql}"
            );
        }
    }

    #[test]
    fn oracle_read_only_statement_guard_blocks_writes_and_plsql() {
        for sql in [
            "insert into t values (1)",
            "update t set id = 2",
            "delete from t",
            "merge into t using dual on (1 = 1) when matched then update set id = 1",
            "create table t (id number)",
            "truncate table t",
            "begin insert into t values (1); end;",
            "call p_write_data()",
        ] {
            assert!(
                !SqlEditorWidget::oracle_read_only_allows_statement(sql),
                "expected Oracle read-only mode to block: {sql}"
            );
        }
    }

    #[test]
    fn lazy_cancel_emits_closed_without_statement_finished() {
        let (sender, receiver) = mpsc::channel();

        SqlEditorWidget::emit_lazy_closed_result(&sender, 3, 42, true);
        drop(sender);

        let events = receiver.try_iter().collect::<Vec<_>>();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events.first(),
            Some(QueryProgress::LazyFetchClosed {
                index: 3,
                session_id: 42,
                cancelled: true,
            })
        ));
    }

    #[test]
    fn lazy_fetch_completed_close_event_marks_not_cancelled() {
        let (sender, receiver) = mpsc::channel();

        SqlEditorWidget::emit_lazy_closed_result(&sender, 3, 42, false);
        drop(sender);

        let events = receiver.try_iter().collect::<Vec<_>>();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events.first(),
            Some(QueryProgress::LazyFetchClosed {
                index: 3,
                session_id: 42,
                cancelled: false,
            })
        ));
    }

    #[test]
    fn lazy_waiting_emits_waiting_progress_event() {
        let (sender, receiver) = mpsc::channel();

        SqlEditorWidget::emit_lazy_waiting(&sender, 3, 42);
        drop(sender);

        let events = receiver.try_iter().collect::<Vec<_>>();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events.first(),
            Some(QueryProgress::LazyFetchWaiting {
                index: 3,
                session_id: 42,
            })
        ));
    }

    #[test]
    fn oracle_session_pool_cancel_notification_does_not_wait_for_ui_response() {
        let (sender, receiver) = mpsc::channel();

        assert!(SqlEditorWidget::notify_cancel_oldest_lazy_fetch_for_session_pool(&sender));

        let event = receiver
            .try_recv()
            .expect("cancel-oldest notification should be queued");
        assert!(matches!(
            event,
            QueryProgress::NotifyCancelOldestLazyFetchForSessionPool
        ));
    }

    #[test]
    fn lazy_cancel_drain_preserves_non_cancel_commands() {
        let (sender, receiver) = mpsc::channel();
        sender
            .send(LazyFetchCommand::FetchMore(25))
            .expect("send fetch more");
        sender
            .send(LazyFetchCommand::FetchAll)
            .expect("send fetch all");
        let mut pending = std::collections::VecDeque::new();

        assert!(!SqlEditorWidget::drain_lazy_cancel_request(
            &receiver,
            &mut pending
        ));
        assert!(matches!(
            pending.pop_front(),
            Some(LazyFetchCommand::FetchMore(25))
        ));
        assert!(matches!(
            pending.pop_front(),
            Some(LazyFetchCommand::FetchAll)
        ));
        assert!(pending.is_empty());
    }

    #[test]
    fn lazy_cancel_drain_detects_cancel_after_queued_fetches() {
        let (sender, receiver) = mpsc::channel();
        sender
            .send(LazyFetchCommand::FetchMore(25))
            .expect("send fetch more");
        sender.send(LazyFetchCommand::Cancel).expect("send cancel");
        let mut pending = std::collections::VecDeque::new();

        assert!(SqlEditorWidget::drain_lazy_cancel_request(
            &receiver,
            &mut pending
        ));
        assert!(matches!(
            pending.pop_front(),
            Some(LazyFetchCommand::FetchMore(25))
        ));
        assert!(pending.is_empty());
    }

    #[test]
    fn lazy_fetch_handle_match_requires_current_session() {
        let (sender, _receiver) = mpsc::channel();
        let active = Arc::new(Mutex::new(Some(LazyFetchHandle {
            session_id: 42,
            sender,
            cancel_handle: None,
            cancel_requested: Arc::new(AtomicBool::new(false)),
        })));

        assert!(SqlEditorWidget::lazy_fetch_handle_matches(&active, 42));
        assert!(!SqlEditorWidget::lazy_fetch_handle_matches(&active, 43));

        *active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        assert!(!SqlEditorWidget::lazy_fetch_handle_matches(&active, 42));
    }

    #[test]
    fn cancelling_lazy_fetch_keeps_active_handle_until_worker_closes() {
        let (sender, receiver) = mpsc::channel();
        let active = Arc::new(Mutex::new(Some(LazyFetchHandle {
            session_id: 42,
            sender,
            cancel_handle: None,
            cancel_requested: Arc::new(AtomicBool::new(false)),
        })));
        let pooled_db_session = crate::db::create_shared_db_session_lease();

        assert!(SqlEditorWidget::cancel_lazy_fetch_handle(
            &active,
            &pooled_db_session
        ));

        assert!(SqlEditorWidget::lazy_fetch_handle_matches(&active, 42));
        assert!(SqlEditorWidget::lazy_fetch_cancel_requested(&active, 42));
        assert!(matches!(receiver.try_recv(), Ok(LazyFetchCommand::Cancel)));

        SqlEditorWidget::clear_lazy_fetch_handle(&active, 42);
        assert!(!SqlEditorWidget::lazy_fetch_handle_matches(&active, 42));
    }

    #[test]
    fn repeated_lazy_cancel_keeps_handle_canceling_until_worker_closes() {
        let (sender, receiver) = mpsc::channel();
        let active = Arc::new(Mutex::new(Some(LazyFetchHandle {
            session_id: 42,
            sender,
            cancel_handle: None,
            cancel_requested: Arc::new(AtomicBool::new(false)),
        })));
        let pooled_db_session = crate::db::create_shared_db_session_lease();

        assert!(SqlEditorWidget::cancel_lazy_fetch_handle_for_session(
            &active,
            &pooled_db_session,
            Some(42),
        ));
        assert!(SqlEditorWidget::cancel_lazy_fetch_handle_for_session(
            &active,
            &pooled_db_session,
            Some(42),
        ));

        assert!(SqlEditorWidget::lazy_fetch_handle_matches(&active, 42));
        assert!(SqlEditorWidget::lazy_fetch_cancel_requested(&active, 42));
        assert!(matches!(receiver.try_recv(), Ok(LazyFetchCommand::Cancel)));
        assert!(matches!(receiver.try_recv(), Ok(LazyFetchCommand::Cancel)));
    }

    #[test]
    fn cancelling_lazy_fetch_with_stale_session_id_is_ignored() {
        let (sender, receiver) = mpsc::channel();
        let cancel_requested = Arc::new(AtomicBool::new(false));
        let active = Arc::new(Mutex::new(Some(LazyFetchHandle {
            session_id: 42,
            sender,
            cancel_handle: None,
            cancel_requested: cancel_requested.clone(),
        })));
        let pooled_db_session = crate::db::create_shared_db_session_lease();

        assert!(!SqlEditorWidget::cancel_lazy_fetch_handle_for_session(
            &active,
            &pooled_db_session,
            Some(43),
        ));

        assert!(SqlEditorWidget::lazy_fetch_handle_matches(&active, 42));
        assert!(!cancel_requested.load(Ordering::Relaxed));
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn cancelled_lazy_fetch_cannot_keep_pooled_session() {
        let (sender, _receiver) = mpsc::channel();
        let cancel_requested = Arc::new(AtomicBool::new(false));
        let active = Arc::new(Mutex::new(Some(LazyFetchHandle {
            session_id: 42,
            sender,
            cancel_handle: None,
            cancel_requested: cancel_requested.clone(),
        })));

        assert!(SqlEditorWidget::lazy_fetch_can_keep_session(&active, 42));

        cancel_requested.store(true, Ordering::Relaxed);

        assert!(!SqlEditorWidget::lazy_fetch_can_keep_session(&active, 42));
    }

    #[test]
    fn pooled_session_idle_release_skips_running_or_lazy_editor() {
        assert!(SqlEditorWidget::pooled_session_is_idle_for_release(
            false, None, true
        ));
        assert!(!SqlEditorWidget::pooled_session_is_idle_for_release(
            true, None, true
        ));
        assert!(!SqlEditorWidget::pooled_session_is_idle_for_release(
            false,
            Some(42),
            true
        ));
        assert!(!SqlEditorWidget::pooled_session_is_idle_for_release(
            false, None, false
        ));
    }

    #[test]
    fn mysql_session_state_hints_preserve_manual_transaction_until_commit() {
        let start_hint = SqlEditorWidget::mysql_session_state_hint_for_sql("START TRANSACTION");
        assert!(!start_hint.clears_session_state);
        assert!(start_hint.may_leave_session_bound_state);

        let select_hint = SqlEditorWidget::mysql_session_state_hint_for_sql("SELECT 1");
        assert!(!select_hint.clears_session_state);
        assert!(!select_hint.may_leave_session_bound_state);

        let commit_hint = SqlEditorWidget::mysql_session_state_hint_for_sql("COMMIT");
        assert!(commit_hint.clears_session_state);
        assert!(!commit_hint.may_leave_session_bound_state);

        let commit_with_option_hint =
            SqlEditorWidget::mysql_session_state_hint_for_sql("COMMIT AND CHAIN");
        assert!(!commit_with_option_hint.clears_session_state);
        assert!(commit_with_option_hint.may_leave_session_bound_state);

        let rollback_chain_hint =
            SqlEditorWidget::mysql_session_state_hint_for_sql("ROLLBACK WORK AND CHAIN");
        assert!(!rollback_chain_hint.clears_session_state);
        assert!(rollback_chain_hint.may_leave_session_bound_state);

        let rollback_to_hint =
            SqlEditorWidget::mysql_session_state_hint_for_sql("ROLLBACK TO SAVEPOINT sp1");
        assert!(!rollback_to_hint.clears_session_state);
        assert!(!rollback_to_hint.may_leave_session_bound_state);
    }

    #[test]
    fn mysql_session_state_hints_cover_implicit_commit_and_session_state() {
        let ddl_hint = SqlEditorWidget::mysql_session_state_hint_for_sql("CREATE TABLE t (id INT)");
        assert!(ddl_hint.clears_session_state);
        assert!(!ddl_hint.may_leave_session_bound_state);

        let temp_hint =
            SqlEditorWidget::mysql_session_state_hint_for_sql("CREATE TEMPORARY TABLE t (id INT)");
        assert!(!temp_hint.clears_session_state);
        assert!(temp_hint.may_leave_session_bound_state);

        let lock_hint = SqlEditorWidget::mysql_session_state_hint_for_sql("LOCK TABLES t WRITE");
        assert!(lock_hint.clears_session_state);
        assert!(lock_hint.may_leave_session_bound_state);

        let autocommit_on_hint =
            SqlEditorWidget::mysql_session_state_hint_for_sql("SET SESSION autocommit = 1");
        assert!(autocommit_on_hint.clears_session_state);
        assert!(!autocommit_on_hint.may_leave_session_bound_state);
    }

    #[test]
    fn mysql_pooled_execution_setup_skips_session_changes_when_preserving_state() {
        let mode = TransactionMode::new(
            TransactionIsolation::ReadCommitted,
            TransactionAccessMode::ReadOnly,
        );

        let statements =
            SqlEditorWidget::mysql_pooled_execution_session_setup_statements(false, mode, true)
                .expect("preserved MySQL session setup should be valid");

        assert!(statements.is_empty());
    }

    #[test]
    fn mysql_pooled_execution_setup_applies_expected_statements_for_clean_session() {
        let mode = TransactionMode::new(
            TransactionIsolation::ReadCommitted,
            TransactionAccessMode::ReadOnly,
        );

        let statements =
            SqlEditorWidget::mysql_pooled_execution_session_setup_statements(false, mode, false)
                .expect("clean MySQL session setup should be valid");

        assert_eq!(
            statements,
            vec![
                "SET autocommit=0",
                "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY"
            ]
        );
    }

    #[test]
    fn mysql_pooled_action_reuses_session_after_success_or_nonfatal_sql_error() {
        let ok_result: std::thread::Result<Result<(), String>> = Ok(Ok(()));
        let sql_error: std::thread::Result<Result<(), String>> = Ok(Err(
            "ERROR 1064 (42000): You have an error in your SQL syntax".to_string(),
        ));
        let panic_result: std::thread::Result<Result<(), String>> =
            Err(Box::new("panic while using pooled session"));

        assert!(SqlEditorWidget::mysql_pooled_action_can_reuse_session(
            &ok_result
        ));
        assert!(SqlEditorWidget::mysql_pooled_action_can_reuse_session(
            &sql_error
        ));
        assert!(!SqlEditorWidget::mysql_pooled_action_can_reuse_session(
            &panic_result
        ));
    }

    #[test]
    fn mysql_pooled_action_drops_session_after_fatal_or_interrupted_error() {
        let connection_error: std::thread::Result<Result<(), String>> = Ok(Err(
            "Lost connection to MySQL server during query".to_string(),
        ));
        let alternate_connection_error: std::thread::Result<Result<(), String>> =
            Ok(Err("Connection lost while reading packet".to_string()));
        let driver_error: std::thread::Result<Result<(), String>> =
            Ok(Err("DriverError { Packet out of order }".to_string()));
        let protocol_error: std::thread::Result<Result<(), String>> = Ok(Err(
            "Commands out of sync; you can't run this command now".to_string(),
        ));
        let raw_interrupt_error: std::thread::Result<Result<(), String>> =
            Ok(Err("Query execution was interrupted".to_string()));
        let raw_kill_error: std::thread::Result<Result<(), String>> =
            Ok(Err("Query was killed".to_string()));
        let cancel_error: std::thread::Result<Result<(), String>> =
            Ok(Err(SqlEditorWidget::cancel_message()));
        let timeout_error: std::thread::Result<Result<(), String>> = Ok(Err(
            SqlEditorWidget::timeout_message(Some(Duration::from_secs(5))),
        ));

        assert!(!SqlEditorWidget::mysql_pooled_action_can_reuse_session(
            &connection_error
        ));
        assert!(!SqlEditorWidget::mysql_pooled_action_can_reuse_session(
            &alternate_connection_error
        ));
        assert!(!SqlEditorWidget::mysql_pooled_action_can_reuse_session(
            &driver_error
        ));
        assert!(!SqlEditorWidget::mysql_pooled_action_can_reuse_session(
            &protocol_error
        ));
        assert!(!SqlEditorWidget::mysql_pooled_action_can_reuse_session(
            &raw_interrupt_error
        ));
        assert!(!SqlEditorWidget::mysql_pooled_action_can_reuse_session(
            &raw_kill_error
        ));
        assert!(!SqlEditorWidget::mysql_pooled_action_can_reuse_session(
            &cancel_error
        ));
        assert!(!SqlEditorWidget::mysql_pooled_action_can_reuse_session(
            &timeout_error
        ));
    }

    #[test]
    fn mysql_session_reuse_rejects_server_disconnect_variants() {
        for message in [
            "ERROR 2006 (HY000): MySQL server has gone away",
            "ERROR 2013 (HY000): Lost connection to MySQL server during query",
            "Communications link failure: server closed the connection",
            "DriverError { Malformed packet }",
            "unexpected EOF while reading packet",
            "Can't connect to MySQL server on '127.0.0.1'",
            "Connection timed out (os error 60)",
            "Network is unreachable (os error 51)",
            "No connection available",
            "Pool disconnected before a connection could be acquired",
            "ERROR 1927 (70100): Connection was killed",
            "ERROR 1053 (08S01): Server shutdown in progress",
        ] {
            let result: std::thread::Result<Result<(), String>> = Ok(Err(message.to_string()));
            assert!(
                !SqlEditorWidget::mysql_pooled_action_can_reuse_session(&result),
                "message should force pooled session drop: {message}"
            );
        }
    }

    #[test]
    fn oracle_session_reuse_rejects_connection_loss_errors() {
        let err = OracleError::new(
            OracleErrorKind::InternalError,
            "ORA-03113: end-of-file on communication channel",
        );

        assert!(!SqlEditorWidget::oracle_error_allows_session_reuse(&err));
        assert!(!SqlEditorWidget::oracle_error_message_allows_session_reuse(
            "DPI-1080: connection was closed by ORA-03113"
        ));
        assert!(!SqlEditorWidget::oracle_error_message_allows_session_reuse(
            "DPI-1067: call timeout of 5000 ms exceeded with ORA-01013"
        ));
        assert!(!SqlEditorWidget::oracle_error_message_allows_session_reuse(
            "ORA-02396: exceeded maximum idle time, please connect again"
        ));
        assert!(!SqlEditorWidget::oracle_error_message_allows_session_reuse(
            "Failed to reset Oracle call timeout: DPI-1010: not connected"
        ));
        assert!(!SqlEditorWidget::oracle_error_message_allows_session_reuse(
            "Failed to apply Oracle call timeout: DPI-1010: not connected"
        ));
        assert!(!SqlEditorWidget::oracle_error_message_allows_session_reuse(
            "DPI-1019: not connected"
        ));
        assert!(!SqlEditorWidget::oracle_error_message_allows_session_reuse(
            "TNS-12535: TNS:operation timed out"
        ));
        assert!(!SqlEditorWidget::oracle_error_message_allows_session_reuse(
            "Connection reset by peer"
        ));
        assert!(!SqlEditorWidget::oracle_error_message_allows_session_reuse(
            "Broken pipe"
        ));
    }

    #[test]
    fn oracle_session_reuse_allows_regular_sql_errors() {
        let err = OracleError::new(
            OracleErrorKind::InternalError,
            "ORA-00001: unique constraint violated",
        );

        assert!(SqlEditorWidget::oracle_error_allows_session_reuse(&err));
    }

    #[test]
    fn lazy_worker_panic_closes_session_and_clears_active_handle() {
        let (progress_sender, progress_receiver) = mpsc::channel();
        let (command_sender, _command_receiver) = mpsc::channel();
        let active = Arc::new(Mutex::new(Some(LazyFetchHandle {
            session_id: 42,
            sender: command_sender,
            cancel_handle: None,
            cancel_requested: Arc::new(AtomicBool::new(false)),
        })));
        let panic_payload = "simulated lazy fetch panic";

        SqlEditorWidget::clear_lazy_fetch_after_worker_panic(
            &progress_sender,
            &active,
            3,
            42,
            &panic_payload,
        );
        drop(progress_sender);

        assert!(!SqlEditorWidget::lazy_fetch_handle_matches(&active, 42));
        let events = progress_receiver.try_iter().collect::<Vec<_>>();
        assert!(matches!(
            events.first(),
            Some(QueryProgress::LazyFetchClosed {
                index: 3,
                session_id: 42,
                cancelled: true,
            })
        ));
        assert!(matches!(
            events.get(1),
            Some(QueryProgress::WorkerPanicked { message })
                if message.contains("simulated lazy fetch panic")
        ));
    }

    #[test]
    fn mysql_error_message_normalizes_timeout_and_cancel_errors() {
        let timeout = Some(Duration::from_secs(5));
        let timeout_err = MysqlError::MySqlError(MySqlError {
            state: "HY000".to_string(),
            code: 3024,
            message: "Query execution was interrupted, maximum statement execution time exceeded"
                .to_string(),
        });
        let cancel_err = MysqlError::MySqlError(MySqlError {
            state: "70100".to_string(),
            code: 1317,
            message: "Query execution was interrupted".to_string(),
        });

        assert_eq!(
            SqlEditorWidget::mysql_error_message(&timeout_err, timeout),
            SqlEditorWidget::timeout_message(timeout)
        );
        assert_eq!(
            SqlEditorWidget::mysql_error_message(&cancel_err, timeout),
            SqlEditorWidget::cancel_message()
        );
    }
}

#[cfg(test)]
mod script_include_guard_tests {
    use super::{ScriptExecutionFrame, SqlEditorWidget, MAX_SCRIPT_INCLUDE_DEPTH};
    use crate::db::{connection::DatabaseType, ScriptItem, ToolCommand};
    use std::fs;
    use std::path::Path;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn frame_with_source(path: &str) -> ScriptExecutionFrame {
        ScriptExecutionFrame {
            items: Vec::new(),
            index: 0,
            base_dir: PathBuf::from("."),
            source_path: Some(PathBuf::from(path)),
        }
    }

    #[test]
    fn validate_script_include_target_rejects_recursive_include() {
        let frames = vec![frame_with_source("nested.sql")];
        let candidate = PathBuf::from("nested.sql");

        let err = SqlEditorWidget::validate_script_include_target(&frames, candidate.as_path())
            .expect_err("recursive include should be rejected");
        assert!(
            err.contains("Recursive script include detected"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn validate_script_include_target_rejects_depth_over_limit() {
        let mut frames = Vec::new();
        for i in 0..MAX_SCRIPT_INCLUDE_DEPTH {
            frames.push(frame_with_source(&format!("script_{i}.sql")));
        }

        let candidate = PathBuf::from("script_overflow.sql");
        let err = SqlEditorWidget::validate_script_include_target(&frames, candidate.as_path())
            .expect_err("depth overflow should be rejected");
        assert!(
            err.contains("Maximum nested script depth"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn load_script_include_preserves_mysql_delimiter_context_for_nested_scripts() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let target_path =
            std::env::temp_dir().join(format!("space_query_mysql_include_{unique}.sql"));
        let normalized_target_path = target_path.clone();
        let sql = "CREATE PROCEDURE demo_proc()\nBEGIN\n    SELECT 1;\nEND$$\nSELECT 2$$\n";
        fs::write(&target_path, sql).expect("temp include script should be writable");

        let loaded = SqlEditorWidget::load_script_include(
            target_path.as_path(),
            normalized_target_path.as_path(),
            Path::new("."),
            Some(DatabaseType::MySQL),
            Some("$$"),
        )
        .expect("MySQL include should load with inherited delimiter context");

        let _ = fs::remove_file(&target_path);

        let statements = loaded
            .items
            .iter()
            .filter_map(|item| match item {
                ScriptItem::Statement(statement) => Some(statement.as_str()),
                ScriptItem::ToolCommand(_) => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            statements.len(),
            2,
            "nested MySQL include should keep both END$$-terminated statements executable: {statements:?}"
        );
        assert!(
            statements
                .first()
                .is_some_and(|stmt| stmt.starts_with("CREATE PROCEDURE demo_proc()")),
            "first included statement should remain the stored routine: {statements:?}"
        );
        assert_eq!(
            statements.get(1).copied(),
            Some("SELECT 2"),
            "second included statement should preserve the custom delimiter split: {statements:?}"
        );
    }

    #[test]
    fn build_mysql_batch_items_treats_top_level_script_as_self_contained_without_override() {
        let sql = include_str!("../../../test_mariadb/test1.txt");

        let items = SqlEditorWidget::build_mysql_batch_items(sql, None);
        let statement_count = items
            .iter()
            .filter(|item| matches!(item, ScriptItem::Statement(_)))
            .count();
        let tool_command_count = items
            .iter()
            .filter(|item| matches!(item, ScriptItem::ToolCommand(_)))
            .count();

        assert_eq!(
            statement_count, 23,
            "top-level MySQL batch execution should use the script's own delimiter directives: {items:?}"
        );
        assert_eq!(
            tool_command_count, 3,
            "top-level MySQL batch execution should keep USE + DELIMITER commands intact: {items:?}"
        );
        assert!(
            matches!(items.first(), Some(ScriptItem::Statement(statement)) if statement.starts_with("DROP DATABASE IF EXISTS qt_mysql_final_boss")),
            "top-level batch should keep the leading semicolon-terminated DDL separate from later END$$ routines: {items:?}"
        );
    }

    #[test]
    fn build_mysql_batch_items_keeps_test4_full_script_routines_and_trailing_calls_separate() {
        let sql = include_str!("../../../test_mariadb/test4.txt");

        let items = SqlEditorWidget::build_mysql_batch_items(sql, None);
        let statements = items
            .iter()
            .filter_map(|item| match item {
                ScriptItem::Statement(statement) => Some(statement.as_str()),
                ScriptItem::ToolCommand(_) => None,
            })
            .collect::<Vec<_>>();
        let mysql_delimiters = items
            .iter()
            .filter_map(|item| match item {
                ScriptItem::ToolCommand(ToolCommand::MysqlDelimiter { delimiter }) => {
                    Some(delimiter.as_str())
                }
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            mysql_delimiters,
            vec![";", "$$", ";"],
            "top-level MySQL execution should propagate DELIMITER state changes even when DELIMITER follows earlier DDL/view statements: {items:?}"
        );
        assert!(
            statements
                .iter()
                .any(|stmt| stmt.starts_with("CREATE TRIGGER bi_task_log")),
            "first trigger should remain independently executable in the top-level MySQL batch: {statements:?}"
        );
        assert!(
            statements
                .iter()
                .any(|stmt| stmt.starts_with("CREATE PROCEDURE sp_build_monthly_rollup")),
            "second procedure should remain independently executable in the top-level MySQL batch: {statements:?}"
        );
        assert!(
            statements
                .iter()
                .any(|stmt| stmt.trim_start().starts_with("CALL sp_seed_monster_data()")),
            "post-routine CALL statements should not be absorbed into the preceding END$$ block: {statements:?}"
        );
        assert!(
            statements
                .iter()
                .any(|stmt| stmt.trim_start().starts_with("SELECT 'ALL ASSERTIONS PASSED' AS status")),
            "post-routine assertions/selects should remain separate execution units: {statements:?}"
        );
    }
}

#[cfg(test)]
mod execution_startup_error_tests {
    use super::SqlEditorWidget;
    use crate::ui::sql_editor::QueryProgress;
    use std::sync::mpsc;

    #[test]
    fn emit_execution_startup_error_reports_statement_finished_for_single_sql() {
        let (tx, rx) = mpsc::channel();

        SqlEditorWidget::emit_execution_startup_error(
            &tx,
            false,
            "select 1 from dual",
            "DEV",
            "startup failed",
            None,
        );

        let progress = rx
            .recv()
            .unwrap_or_else(|err| panic!("expected statement error progress event: {err}"));

        match progress {
            QueryProgress::StatementFinished {
                index,
                result,
                connection_name,
                timed_out,
            } => {
                assert_eq!(index, 0);
                assert_eq!(connection_name, "DEV");
                assert!(!timed_out);
                assert_eq!(result.sql, "select 1 from dual");
                assert_eq!(result.message, "Error: startup failed");
                assert!(!result.success);
            }
            _ => panic!("expected StatementFinished progress event"),
        }
    }

    #[test]
    fn emit_execution_startup_error_reports_script_result_in_script_mode() {
        let (tx, rx) = mpsc::channel();

        SqlEditorWidget::emit_execution_startup_error(
            &tx,
            true,
            "begin null; end;",
            "DEV",
            "script startup failed",
            None,
        );

        let progress = rx
            .recv()
            .unwrap_or_else(|err| panic!("expected script error progress event: {err}"));

        match progress {
            QueryProgress::StatementFinished {
                index,
                result,
                connection_name,
                timed_out,
            } => {
                assert_eq!(index, 0);
                assert_eq!(connection_name, "DEV");
                assert!(!timed_out);
                assert_eq!(result.sql, "begin null; end;");
                assert_eq!(result.message, "Error: script startup failed");
                assert!(!result.success);
            }
            _ => panic!("expected StatementFinished progress event"),
        }
    }
}

#[cfg(test)]
mod mysql_batch_execution_regression_tests {
    use super::{
        LazyFetchCommand, LazyFetchHandle, MySqlQueryCancelContext, QueryProgress, SqlEditorWidget,
        PROGRESS_ROWS_INITIAL_BATCH,
    };
    use crate::db::{
        connection::{ConnectionInfo, DatabaseType},
        DatabaseConnection, SessionState, TransactionAccessMode, TransactionIsolation,
        TransactionMode,
    };
    use std::env;
    use std::sync::atomic::AtomicU64;
    use std::sync::{mpsc, Arc, Mutex};
    use std::time::Duration;

    fn mysql_test_env(name: &str) -> Option<String> {
        env::var(name).ok().filter(|value| !value.trim().is_empty())
    }

    fn mysql_test_connection_with_mode(mode: TransactionMode) -> Option<DatabaseConnection> {
        let Some(host) = mysql_test_env("SPACE_QUERY_TEST_MYSQL_HOST") else {
            eprintln!("skipping: SPACE_QUERY_TEST_MYSQL_HOST is not set");
            return None;
        };
        let Some(database) = mysql_test_env("SPACE_QUERY_TEST_MYSQL_DATABASE") else {
            eprintln!("skipping: SPACE_QUERY_TEST_MYSQL_DATABASE is not set");
            return None;
        };
        let Some(user) = mysql_test_env("SPACE_QUERY_TEST_MYSQL_USER") else {
            eprintln!("skipping: SPACE_QUERY_TEST_MYSQL_USER is not set");
            return None;
        };
        let Some(password) = mysql_test_env("SPACE_QUERY_TEST_MYSQL_PASSWORD") else {
            eprintln!("skipping: SPACE_QUERY_TEST_MYSQL_PASSWORD is not set");
            return None;
        };
        let port = mysql_test_env("SPACE_QUERY_TEST_MYSQL_PORT")
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(3306);

        let mut connection = DatabaseConnection::new();
        connection
            .connect(ConnectionInfo::new_with_type(
                "MYSQL_TEST",
                &user,
                &password,
                &host,
                port,
                &database,
                DatabaseType::MySQL,
            ))
            .expect("MySQL/MariaDB test connection should succeed");
        connection
            .set_transaction_mode(mode)
            .expect("transaction mode should be supported by MySQL/MariaDB");
        Some(connection)
    }

    fn mysql_test_connection_with_advanced_transaction_defaults(
        isolation: TransactionIsolation,
        access_mode: TransactionAccessMode,
    ) -> Option<DatabaseConnection> {
        let Some(host) = mysql_test_env("SPACE_QUERY_TEST_MYSQL_HOST") else {
            eprintln!("skipping: SPACE_QUERY_TEST_MYSQL_HOST is not set");
            return None;
        };
        let Some(database) = mysql_test_env("SPACE_QUERY_TEST_MYSQL_DATABASE") else {
            eprintln!("skipping: SPACE_QUERY_TEST_MYSQL_DATABASE is not set");
            return None;
        };
        let Some(user) = mysql_test_env("SPACE_QUERY_TEST_MYSQL_USER") else {
            eprintln!("skipping: SPACE_QUERY_TEST_MYSQL_USER is not set");
            return None;
        };
        let Some(password) = mysql_test_env("SPACE_QUERY_TEST_MYSQL_PASSWORD") else {
            eprintln!("skipping: SPACE_QUERY_TEST_MYSQL_PASSWORD is not set");
            return None;
        };
        let port = mysql_test_env("SPACE_QUERY_TEST_MYSQL_PORT")
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(3306);

        let mut info = ConnectionInfo::new_with_type(
            "MYSQL_TEST",
            &user,
            &password,
            &host,
            port,
            &database,
            DatabaseType::MySQL,
        );
        info.advanced.default_transaction_isolation = isolation;
        info.advanced.default_transaction_access_mode = access_mode;

        let mut connection = DatabaseConnection::new();
        connection
            .connect(info)
            .expect("MySQL/MariaDB test connection should succeed");
        Some(connection)
    }

    fn summarize_progress(progress: &[QueryProgress]) -> String {
        progress
            .iter()
            .map(|message| match message {
                QueryProgress::BatchStart { .. } => "BatchStart".to_string(),
                QueryProgress::StatementStart { index } => {
                    format!("StatementStart({index})")
                }
                QueryProgress::SelectStart { index, columns, .. } => {
                    format!("SelectStart({index}, cols={})", columns.len())
                }
                QueryProgress::Rows { index, rows } => {
                    format!("Rows({index}, count={})", rows.len())
                }
                QueryProgress::LazyFetchSession { index, session_id } => {
                    format!("LazyFetchSession({index}, {session_id})")
                }
                QueryProgress::LazyFetchWaiting { index, session_id } => {
                    format!("LazyFetchWaiting({index}, {session_id})")
                }
                QueryProgress::LazyFetchCanceling { session_id } => {
                    format!("LazyFetchCanceling({session_id})")
                }
                QueryProgress::LazyFetchClosed {
                    index,
                    session_id,
                    cancelled,
                } => format!("LazyFetchClosed({index}, {session_id}, {cancelled})"),
                QueryProgress::ScriptOutput { lines } => {
                    format!("ScriptOutput({})", lines.join(" | "))
                }
                QueryProgress::PromptInput { prompt, .. } => {
                    format!("PromptInput({prompt})")
                }
                QueryProgress::RequestCancelOldestLazyFetchForSessionPool { .. } => {
                    "RequestCancelOldestLazyFetchForSessionPool".to_string()
                }
                QueryProgress::NotifyCancelOldestLazyFetchForSessionPool => {
                    "NotifyCancelOldestLazyFetchForSessionPool".to_string()
                }
                QueryProgress::AutoCommitChanged { enabled } => {
                    format!("AutoCommitChanged({enabled})")
                }
                QueryProgress::ConnectionChanged { info } => format!(
                    "ConnectionChanged({})",
                    info.as_ref()
                        .map(|value| value.connection_string())
                        .unwrap_or_else(|| "None".to_string())
                ),
                QueryProgress::WorkerPanicked { message } => {
                    format!("WorkerPanicked({message})")
                }
                QueryProgress::StatementFinished { index, result, .. } => format!(
                    "StatementFinished({index}, success={}, sql={}, message={})",
                    result.success,
                    result.sql.lines().next().unwrap_or_default(),
                    result.message
                ),
                QueryProgress::BatchFinished => "BatchFinished".to_string(),
                QueryProgress::MetadataRefreshNeeded => "MetadataRefreshNeeded".to_string(),
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn assert_mysql_batch_script_reaches_final_status_pass(script: &str, db_activity: &str) {
        let Some(host) = mysql_test_env("SPACE_QUERY_TEST_MYSQL_HOST") else {
            eprintln!("skipping: SPACE_QUERY_TEST_MYSQL_HOST is not set");
            return;
        };
        let Some(database) = mysql_test_env("SPACE_QUERY_TEST_MYSQL_DATABASE") else {
            eprintln!("skipping: SPACE_QUERY_TEST_MYSQL_DATABASE is not set");
            return;
        };
        let Some(user) = mysql_test_env("SPACE_QUERY_TEST_MYSQL_USER") else {
            eprintln!("skipping: SPACE_QUERY_TEST_MYSQL_USER is not set");
            return;
        };
        let Some(password) = mysql_test_env("SPACE_QUERY_TEST_MYSQL_PASSWORD") else {
            eprintln!("skipping: SPACE_QUERY_TEST_MYSQL_PASSWORD is not set");
            return;
        };
        let port = mysql_test_env("SPACE_QUERY_TEST_MYSQL_PORT")
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(3306);

        let mut connection = DatabaseConnection::new();
        connection.set_auto_commit(true);
        connection
            .connect(ConnectionInfo::new_with_type(
                "MYSQL_TEST",
                &user,
                &password,
                &host,
                port,
                &database,
                DatabaseType::MySQL,
            ))
            .expect("MySQL regression test connection should succeed");

        let shared_connection = Arc::new(Mutex::new(connection));
        let session = Arc::new(Mutex::new(SessionState {
            db_type: DatabaseType::MySQL,
            ..SessionState::default()
        }));
        let current_mysql_cancel_context: Arc<Mutex<Option<MySqlQueryCancelContext>>> =
            Arc::new(Mutex::new(None));
        let pooled_db_session = crate::db::create_shared_db_session_lease();
        let active_lazy_fetch: Arc<Mutex<Option<LazyFetchHandle>>> = Arc::new(Mutex::new(None));
        let next_lazy_fetch_session_id = Arc::new(AtomicU64::new(1));
        let cancel_flag = Arc::new(Mutex::new(false));
        let (sender, receiver) = mpsc::channel();

        SqlEditorWidget::execute_mysql_batch(
            &shared_connection,
            &sender,
            script,
            "MYSQL_TEST",
            &session,
            &pooled_db_session,
            &active_lazy_fetch,
            &next_lazy_fetch_session_id,
            &current_mysql_cancel_context,
            &cancel_flag,
            true,
            None,
            None,
            true,
            db_activity,
        );
        drop(sender);

        let progress = receiver.try_iter().collect::<Vec<_>>();
        let progress_summary = summarize_progress(&progress);
        let final_status_index = progress.iter().find_map(|message| match message {
            QueryProgress::StatementFinished { index, result, .. }
                if result.sql.contains("'FINAL_STATUS' AS section_name") =>
            {
                Some(*index)
            }
            _ => None,
        });

        assert!(
            progress.iter().any(|message| matches!(
                message,
                QueryProgress::AutoCommitChanged { enabled: false }
            )),
            "@TRANSACTION should disable autocommit during batch execution\n{progress_summary}"
        );
        assert!(
            final_status_index.is_some(),
            "batch execution should emit the FINAL_STATUS select\n{progress_summary}"
        );
        assert!(
            progress
                .iter()
                .any(|message| match (final_status_index, message) {
                    (
                        Some(index),
                        QueryProgress::Rows {
                            index: row_index,
                            rows,
                        },
                    ) if *row_index == index => {
                        rows.iter().any(|row| row.iter().any(|cell| cell == "PASS"))
                    }
                    _ => false,
                }),
            "batch execution should reach the FINAL_STATUS PASS result\n{progress_summary}"
        );
    }

    fn assert_mysql_batch_script_reaches_status_pass(script: &str, db_activity: &str) {
        let Some(host) = mysql_test_env("SPACE_QUERY_TEST_MYSQL_HOST") else {
            eprintln!("skipping: SPACE_QUERY_TEST_MYSQL_HOST is not set");
            return;
        };
        let Some(database) = mysql_test_env("SPACE_QUERY_TEST_MYSQL_DATABASE") else {
            eprintln!("skipping: SPACE_QUERY_TEST_MYSQL_DATABASE is not set");
            return;
        };
        let Some(user) = mysql_test_env("SPACE_QUERY_TEST_MYSQL_USER") else {
            eprintln!("skipping: SPACE_QUERY_TEST_MYSQL_USER is not set");
            return;
        };
        let Some(password) = mysql_test_env("SPACE_QUERY_TEST_MYSQL_PASSWORD") else {
            eprintln!("skipping: SPACE_QUERY_TEST_MYSQL_PASSWORD is not set");
            return;
        };
        let port = mysql_test_env("SPACE_QUERY_TEST_MYSQL_PORT")
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(3306);

        let mut connection = DatabaseConnection::new();
        connection.set_auto_commit(true);
        connection
            .connect(ConnectionInfo::new_with_type(
                "MYSQL_TEST",
                &user,
                &password,
                &host,
                port,
                &database,
                DatabaseType::MySQL,
            ))
            .expect("MySQL regression test connection should succeed");

        let shared_connection = Arc::new(Mutex::new(connection));
        let session = Arc::new(Mutex::new(SessionState {
            db_type: DatabaseType::MySQL,
            ..SessionState::default()
        }));
        let current_mysql_cancel_context: Arc<Mutex<Option<MySqlQueryCancelContext>>> =
            Arc::new(Mutex::new(None));
        let pooled_db_session = crate::db::create_shared_db_session_lease();
        let active_lazy_fetch: Arc<Mutex<Option<LazyFetchHandle>>> = Arc::new(Mutex::new(None));
        let next_lazy_fetch_session_id = Arc::new(AtomicU64::new(1));
        let cancel_flag = Arc::new(Mutex::new(false));
        let (sender, receiver) = mpsc::channel();

        SqlEditorWidget::execute_mysql_batch(
            &shared_connection,
            &sender,
            script,
            "MYSQL_TEST",
            &session,
            &pooled_db_session,
            &active_lazy_fetch,
            &next_lazy_fetch_session_id,
            &current_mysql_cancel_context,
            &cancel_flag,
            true,
            None,
            None,
            true,
            db_activity,
        );
        drop(sender);

        let progress = receiver.try_iter().collect::<Vec<_>>();
        let progress_summary = summarize_progress(&progress);
        let failed_statement = progress.iter().find_map(|message| match message {
            QueryProgress::StatementFinished { index, result, .. } if !result.success => {
                Some((*index, result.message.clone()))
            }
            _ => None,
        });

        assert!(
            failed_statement.is_none(),
            "batch execution should not emit a failed statement: {failed_statement:?}\n{progress_summary}"
        );
        let pass_status_index = progress.iter().find_map(|message| match message {
            QueryProgress::StatementFinished { index, result, .. }
                if result.sql.contains("'PASS' AS status") =>
            {
                Some(*index)
            }
            _ => None,
        });

        assert!(
            pass_status_index.is_some(),
            "batch execution should emit the final PASS status select\n{progress_summary}"
        );
        assert!(
            progress
                .iter()
                .any(|message| match (pass_status_index, message) {
                    (
                        Some(index),
                        QueryProgress::Rows {
                            index: row_index,
                            rows,
                        },
                    ) if *row_index == index => {
                        rows.iter().any(|row| row.iter().any(|cell| cell == "PASS"))
                    }
                    _ => false,
                }),
            "batch execution should reach a PASS status row\n{progress_summary}"
        );
    }

    fn assert_mysql_pooled_execution_reports_transaction_mode(
        connection: DatabaseConnection,
        expected_isolation: &str,
        expected_read_only: bool,
    ) {
        let shared_connection = Arc::new(Mutex::new(connection));
        let session = Arc::new(Mutex::new(SessionState {
            db_type: DatabaseType::MySQL,
            ..SessionState::default()
        }));
        let current_mysql_cancel_context: Arc<Mutex<Option<MySqlQueryCancelContext>>> =
            Arc::new(Mutex::new(None));
        let pooled_db_session = crate::db::create_shared_db_session_lease();
        let active_lazy_fetch: Arc<Mutex<Option<LazyFetchHandle>>> = Arc::new(Mutex::new(None));
        let next_lazy_fetch_session_id = Arc::new(AtomicU64::new(1));
        let cancel_flag = Arc::new(Mutex::new(false));
        let (sender, receiver) = mpsc::channel();

        SqlEditorWidget::execute_mysql_batch(
            &shared_connection,
            &sender,
            "SHOW VARIABLES WHERE Variable_name IN ('transaction_isolation', 'tx_isolation', 'transaction_read_only', 'tx_read_only'); SELECT 1 AS done",
            "MYSQL_TEST",
            &session,
            &pooled_db_session,
            &active_lazy_fetch,
            &next_lazy_fetch_session_id,
            &current_mysql_cancel_context,
            &cancel_flag,
            true,
            None,
            None,
            true,
            "mysql transaction mode integration",
        );
        drop(sender);

        let progress = receiver.try_iter().collect::<Vec<_>>();
        let progress_summary = summarize_progress(&progress);
        let rows = progress
            .iter()
            .find_map(|message| match message {
                QueryProgress::Rows { index: 0, rows } => Some(rows.clone()),
                _ => None,
            })
            .unwrap_or_default();
        let mut isolation = None;
        let mut read_only = None;
        for row in rows {
            let Some(name) = row.first().map(|value| value.to_ascii_lowercase()) else {
                continue;
            };
            let Some(value) = row.get(1) else {
                continue;
            };
            match name.as_str() {
                "transaction_isolation" | "tx_isolation" => {
                    isolation = Some(value.replace(['-', '_'], " ").to_ascii_uppercase());
                }
                "transaction_read_only" | "tx_read_only" => {
                    read_only = Some(value.to_ascii_uppercase());
                }
                _ => {}
            }
        }

        assert_eq!(
            isolation.as_deref(),
            Some(expected_isolation),
            "transaction isolation should be applied to pooled session\n{progress_summary}"
        );
        if expected_read_only {
            assert!(
                matches!(read_only.as_deref(), Some("ON" | "1")),
                "transaction read-only mode should be applied to pooled session, got {read_only:?}\n{progress_summary}"
            );
        } else {
            assert!(
                matches!(read_only.as_deref(), Some("OFF" | "0")),
                "transaction read-write mode should be applied to pooled session, got {read_only:?}\n{progress_summary}"
            );
        }
    }

    #[test]
    #[ignore = "requires local MySQL or MariaDB test database via SPACE_QUERY_TEST_MYSQL_* env vars"]
    fn mysql_pooled_execution_applies_transaction_mode_selection() {
        let Some(connection) = mysql_test_connection_with_mode(TransactionMode::new(
            TransactionIsolation::ReadCommitted,
            TransactionAccessMode::ReadOnly,
        )) else {
            return;
        };

        assert_mysql_pooled_execution_reports_transaction_mode(connection, "READ COMMITTED", true);
    }

    #[test]
    #[ignore = "requires local MySQL or MariaDB test database via SPACE_QUERY_TEST_MYSQL_* env vars"]
    fn mysql_pooled_execution_uses_advanced_default_transaction_mode() {
        let Some(connection) = mysql_test_connection_with_advanced_transaction_defaults(
            TransactionIsolation::ReadCommitted,
            TransactionAccessMode::ReadOnly,
        ) else {
            return;
        };

        assert_mysql_pooled_execution_reports_transaction_mode(connection, "READ COMMITTED", true);
    }

    #[test]
    #[ignore = "requires local MariaDB test database via SPACE_QUERY_TEST_MYSQL_* env vars"]
    fn mysql_lazy_cursor_fetches_incrementally_from_local_mariadb() {
        let Some(host) = mysql_test_env("SPACE_QUERY_TEST_MYSQL_HOST") else {
            eprintln!("skipping: SPACE_QUERY_TEST_MYSQL_HOST is not set");
            return;
        };
        let Some(database) = mysql_test_env("SPACE_QUERY_TEST_MYSQL_DATABASE") else {
            eprintln!("skipping: SPACE_QUERY_TEST_MYSQL_DATABASE is not set");
            return;
        };
        let Some(user) = mysql_test_env("SPACE_QUERY_TEST_MYSQL_USER") else {
            eprintln!("skipping: SPACE_QUERY_TEST_MYSQL_USER is not set");
            return;
        };
        let Some(password) = mysql_test_env("SPACE_QUERY_TEST_MYSQL_PASSWORD") else {
            eprintln!("skipping: SPACE_QUERY_TEST_MYSQL_PASSWORD is not set");
            return;
        };
        let port = mysql_test_env("SPACE_QUERY_TEST_MYSQL_PORT")
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(3306);

        let mut connection = DatabaseConnection::new();
        connection.set_auto_commit(true);
        connection
            .connect(ConnectionInfo::new_with_type(
                "MYSQL_TEST",
                &user,
                &password,
                &host,
                port,
                &database,
                DatabaseType::MySQL,
            ))
            .expect("MySQL lazy fetch test connection should succeed");

        let shared_connection = Arc::new(Mutex::new(connection));
        let session = Arc::new(Mutex::new(SessionState {
            db_type: DatabaseType::MySQL,
            ..SessionState::default()
        }));
        let current_mysql_cancel_context: Arc<Mutex<Option<MySqlQueryCancelContext>>> =
            Arc::new(Mutex::new(None));
        let pooled_db_session = crate::db::create_shared_db_session_lease();
        let active_lazy_fetch: Arc<Mutex<Option<LazyFetchHandle>>> = Arc::new(Mutex::new(None));
        let next_lazy_fetch_session_id = Arc::new(AtomicU64::new(1));
        let cancel_flag = Arc::new(Mutex::new(false));
        let (sender, receiver) = mpsc::channel();
        let lazy_fetch_batch = PROGRESS_ROWS_INITIAL_BATCH;
        let total_rows = lazy_fetch_batch * 2 + 50;
        let sql = format!(
            "WITH digits AS (\
                SELECT 0 AS i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 \
                UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9\
             ) \
             SELECT n \
             FROM (\
                SELECT ones.i + tens.i * 10 + hundreds.i * 100 + thousands.i * 1000 + ten_thousands.i * 10000 + 1 AS n \
                FROM digits ones \
                CROSS JOIN digits tens \
                CROSS JOIN digits hundreds \
                CROSS JOIN digits thousands \
                CROSS JOIN digits ten_thousands\
             ) seq \
             WHERE n <= {total_rows} \
             ORDER BY n"
        );

        SqlEditorWidget::execute_mysql_batch(
            &shared_connection,
            &sender,
            &sql,
            "MYSQL_TEST",
            &session,
            &pooled_db_session,
            &active_lazy_fetch,
            &next_lazy_fetch_session_id,
            &current_mysql_cancel_context,
            &cancel_flag,
            false,
            None,
            None,
            true,
            "mysql lazy fetch integration",
        );
        drop(sender);

        let mut progress = Vec::new();
        let mut fetched_rows = 0usize;
        while fetched_rows < lazy_fetch_batch {
            let message = receiver
                .recv_timeout(Duration::from_secs(5))
                .expect("initial lazy fetch progress should arrive");
            if let QueryProgress::Rows { index, rows } = &message {
                if *index == 0 {
                    fetched_rows += rows.len();
                }
            }
            progress.push(message);
        }
        assert_eq!(
            fetched_rows,
            lazy_fetch_batch,
            "initial lazy fetch should stop at {lazy_fetch_batch} rows\n{}",
            summarize_progress(&progress)
        );

        let handle = active_lazy_fetch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .expect("lazy fetch handle should remain active after initial batch");
        handle
            .sender
            .send(LazyFetchCommand::FetchMore(lazy_fetch_batch))
            .expect("send first fetch more");
        while fetched_rows < lazy_fetch_batch * 2 {
            let message = receiver
                .recv_timeout(Duration::from_secs(5))
                .expect("second lazy fetch progress should arrive");
            if let QueryProgress::Rows { index, rows } = &message {
                if *index == 0 {
                    fetched_rows += rows.len();
                }
            }
            progress.push(message);
        }
        assert_eq!(
            fetched_rows,
            lazy_fetch_batch * 2,
            "first fetch more should append exactly {lazy_fetch_batch} rows\n{}",
            summarize_progress(&progress)
        );

        handle
            .sender
            .send(LazyFetchCommand::FetchMore(lazy_fetch_batch))
            .expect("send second fetch more");
        let mut finished_row_count = None;
        let mut closed = false;
        while !closed || finished_row_count.is_none() {
            let message = receiver
                .recv_timeout(Duration::from_secs(5))
                .expect("final lazy fetch progress should arrive");
            match &message {
                QueryProgress::Rows { index, rows } if *index == 0 => {
                    fetched_rows += rows.len();
                }
                QueryProgress::StatementFinished { index, result, .. } if *index == 0 => {
                    finished_row_count = Some(result.row_count);
                }
                QueryProgress::LazyFetchClosed {
                    index, cancelled, ..
                } if *index == 0 => {
                    assert!(!cancelled, "lazy fetch should finish without cancellation");
                    closed = true;
                }
                _ => {}
            }
            progress.push(message);
        }

        assert_eq!(
            fetched_rows,
            total_rows,
            "second fetch more should append the remaining 50 rows\n{}",
            summarize_progress(&progress)
        );
        assert_eq!(finished_row_count, Some(total_rows));
    }

    #[test]
    #[ignore = "requires local MariaDB test database via SPACE_QUERY_TEST_MYSQL_* env vars"]
    fn execute_mysql_batch_test1_reaches_pass_status() {
        assert_mysql_batch_script_reaches_status_pass(
            include_str!("../../../test_mariadb/test1.txt"),
            "mysql test1 regression",
        );
    }

    #[test]
    #[ignore = "requires local MariaDB test database via SPACE_QUERY_TEST_MYSQL_* env vars"]
    fn execute_mysql_batch_test2_reaches_pass_status() {
        assert_mysql_batch_script_reaches_status_pass(
            include_str!("../../../test_mariadb/test2.txt"),
            "mysql test2 regression",
        );
    }

    #[test]
    #[ignore = "requires local MariaDB test database via SPACE_QUERY_TEST_MYSQL_* env vars"]
    fn execute_mysql_batch_test3_reaches_pass_status() {
        assert_mysql_batch_script_reaches_status_pass(
            include_str!("../../../test_mariadb/test3.txt"),
            "mysql test3 regression",
        );
    }

    #[test]
    #[ignore = "requires local MariaDB test database via SPACE_QUERY_TEST_MYSQL_* env vars"]
    fn execute_mysql_batch_test8_reaches_final_status_after_transaction_directive() {
        assert_mysql_batch_script_reaches_final_status_pass(
            include_str!("../../../test_mariadb/test8.txt"),
            "mysql test8 regression",
        );
    }

    #[test]
    #[ignore = "requires local MariaDB test database via SPACE_QUERY_TEST_MYSQL_* env vars"]
    fn execute_mysql_batch_test7_reaches_final_status_after_transaction_directive() {
        assert_mysql_batch_script_reaches_final_status_pass(
            include_str!("../../../test_mariadb/test7.txt"),
            "mysql test7 regression",
        );
    }

    #[test]
    #[ignore = "requires local MariaDB test database via SPACE_QUERY_TEST_MYSQL_* env vars"]
    fn mysql_set_autocommit_on_preserves_and_commits_pooled_transaction() {
        assert_mysql_batch_script_reaches_final_status_pass(
            "\
DROP TABLE IF EXISTS qt_pool_autocommit_regression;
CREATE TABLE qt_pool_autocommit_regression (id INT PRIMARY KEY);
SET AUTOCOMMIT OFF;
INSERT INTO qt_pool_autocommit_regression (id) VALUES (1);
SET AUTOCOMMIT ON;
ROLLBACK;
SELECT 'FINAL_STATUS' AS section_name,
       CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE CONCAT('FAIL count=', COUNT(*)) END AS status
FROM qt_pool_autocommit_regression;
DROP TABLE IF EXISTS qt_pool_autocommit_regression;
",
            "mysql pooled autocommit regression",
        );
    }

    #[test]
    #[ignore = "requires local MySQL or MariaDB test database via SPACE_QUERY_TEST_MYSQL_* env vars"]
    fn mysql_pooled_manual_transaction_commit_preserves_session_until_commit() {
        assert_mysql_batch_script_reaches_final_status_pass(
            "\
DROP TABLE IF EXISTS qt_pool_commit_regression;
CREATE TABLE qt_pool_commit_regression (id INT PRIMARY KEY);
SET AUTOCOMMIT OFF;
INSERT INTO qt_pool_commit_regression (id) VALUES (1);
COMMIT;
SELECT 'FINAL_STATUS' AS section_name,
       CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE CONCAT('FAIL count=', COUNT(*)) END AS status
FROM qt_pool_commit_regression;
DROP TABLE IF EXISTS qt_pool_commit_regression;
",
            "mysql pooled manual transaction commit regression",
        );
    }
}

#[cfg(test)]
mod disconnected_precheck_gate_tests {
    use super::SqlEditorWidget;

    #[test]
    fn precheck_requires_connection_for_non_bootstrap_db_work() {
        assert!(SqlEditorWidget::requires_connected_session_for_precheck(
            false, false
        ));
    }

    #[test]
    fn precheck_allows_connect_bootstrap_while_disconnected() {
        assert!(!SqlEditorWidget::requires_connected_session_for_precheck(
            true, false
        ));
    }

    #[test]
    fn precheck_allows_local_only_commands_while_disconnected() {
        assert!(!SqlEditorWidget::requires_connected_session_for_precheck(
            false, true
        ));
    }

    #[test]
    fn execution_startup_policy_marks_bootstrap_queries_as_disconnected_safe() {
        let policy = SqlEditorWidget::execution_startup_policy("connect user/pass@db");

        assert!(policy.has_connect_command);
        assert!(!policy.requires_connected_session);
    }

    #[test]
    fn execution_startup_policy_requires_connection_for_regular_sql() {
        let policy = SqlEditorWidget::execution_startup_policy("select * from dual");

        assert!(!policy.has_connect_command);
        assert!(policy.requires_connected_session);
    }

    #[test]
    fn expand_tabs_with_stop_clamps_zero_tab_stop() {
        let rendered = SqlEditorWidget::expand_tabs_with_stop("a\tb", 0);
        assert_eq!(rendered, "a b");
    }
}

#[cfg(test)]
mod print_bind_state_tests {
    use super::{PrintNamedData, SqlEditorWidget};
    use crate::db::{BindDataType, BindValue, BindVar, ColumnInfo, CursorResult, SessionState};

    #[test]
    fn clone_print_named_data_preserves_refcursor_in_session() {
        let mut session = SessionState::default();
        session.binds.insert(
            "V_RC".to_string(),
            BindVar {
                data_type: BindDataType::RefCursor,
                value: BindValue::Cursor(Some(CursorResult {
                    columns: vec!["EMPNO".to_string()],
                    rows: vec![vec!["7369".to_string()]],
                })),
            },
        );

        let named = SqlEditorWidget::clone_print_named_data(&session, "V_RC");

        match named {
            PrintNamedData::Cursor(cursor) => {
                assert_eq!(cursor.columns, vec!["EMPNO".to_string()]);
                assert_eq!(cursor.rows, vec![vec!["7369".to_string()]]);
            }
            _ => panic!("expected cursor print data"),
        }

        match session.binds.get("V_RC").map(|bind| &bind.value) {
            Some(BindValue::Cursor(Some(cursor))) => {
                assert_eq!(cursor.columns, vec!["EMPNO".to_string()]);
                assert_eq!(cursor.rows, vec![vec!["7369".to_string()]]);
            }
            _ => panic!("expected refcursor to remain in session after PRINT clone"),
        }
    }

    #[test]
    fn collect_print_all_data_preserves_cursor_results() {
        let mut session = SessionState::default();
        session.binds.insert(
            "V_RC".to_string(),
            BindVar {
                data_type: BindDataType::RefCursor,
                value: BindValue::Cursor(Some(CursorResult {
                    columns: vec!["ENAME".to_string()],
                    rows: vec![vec!["SMITH".to_string()]],
                })),
            },
        );

        let (summary_rows, cursor_results) = SqlEditorWidget::collect_print_all_data(&session, "");

        assert_eq!(summary_rows.len(), 1);
        assert_eq!(
            summary_rows[0],
            vec![
                "V_RC".to_string(),
                BindDataType::RefCursor.display(),
                "REFCURSOR (1 rows)".to_string(),
            ]
        );
        assert_eq!(cursor_results.len(), 1);
        assert_eq!(cursor_results[0].0, "V_RC".to_string());
        assert_eq!(cursor_results[0].1.columns, vec!["ENAME".to_string()]);
        assert_eq!(cursor_results[0].1.rows, vec![vec!["SMITH".to_string()]]);

        match session.binds.get("V_RC").map(|bind| &bind.value) {
            Some(BindValue::Cursor(Some(cursor))) => {
                assert_eq!(cursor.columns, vec!["ENAME".to_string()]);
                assert_eq!(cursor.rows, vec![vec!["SMITH".to_string()]]);
            }
            _ => panic!("expected refcursor to remain in session after PRINT ALL snapshot"),
        }
    }

    #[test]
    fn format_sql_basic_keeps_keyword_like_aliases_inline() {
        let sql = "SELECT amount AS IF, total AS END FROM sales IF";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("AS IF") && formatted.contains("AS END"),
            "keyword-like aliases should stay as aliases, got:
{}",
            formatted
        );
        assert!(
            formatted.contains("FROM sales IF"),
            "table alias IF should remain inline, got:
{}",
            formatted
        );
        assert!(
            !formatted.contains("\nIF,"),
            "alias IF should not be moved to its own block line, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_alias_with_comment_between_as_and_control_keyword() {
        let sql = "SELECT amount AS /* keep */ IF FROM sales";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("AS /* keep */ IF"),
            "alias with inline comment should remain alias, got:
{}",
            formatted
        );
        assert!(
            !formatted.contains(
                "
IF"
            ),
            "comment-separated alias IF should not be moved to block line, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_keyword_like_implicit_select_aliases_inline_lowercase() {
        let sql = "select amount if, total end from sales";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("amount IF,") && formatted.contains("total END"),
            "implicit lowercase keyword-like aliases should remain inline, got:
{}",
            formatted
        );
        assert!(
            !formatted.contains(
                "
IF,"
            ) && !formatted.contains(
                "
END"
            ),
            "implicit lowercase aliases IF/END should not be moved to block lines, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_keyword_like_implicit_select_aliases_inline() {
        let sql = "SELECT amount IF, total END FROM sales";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("amount IF,") && formatted.contains("total END"),
            "implicit keyword-like aliases should remain inline, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("\nIF,") && !formatted.contains("\nEND"),
            "implicit aliases IF/END should not be moved to block lines, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_keyword_like_alias_before_subquery_close_paren() {
        let sql = "SELECT * FROM (SELECT amount IF FROM sales) IF";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("amount IF") && formatted.contains(") IF"),
            "keyword-like aliases near subquery closing paren should remain inline, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("\nIF") && !formatted.contains("\n)"),
            "aliases IF should not be reformatted as block keyword near ')', got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_keyword_like_alias_if_before_case_then_inline() {
        let sql = "SELECT amount IF, CASE WHEN flag = 1 THEN 1 ELSE 0 END score FROM sales";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("amount IF,") && formatted.contains("CASE") && formatted.contains("THEN"),
            "IF alias before CASE expression should remain alias while preserving CASE tokens, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("\nIF,"),
            "IF alias should not be split as PL/SQL block token, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_in_plsql_block_keeps_keyword_like_aliases_inline() {
        let sql = "BEGIN SELECT amount AS IF, total AS END INTO v_amount, v_total FROM sales; END;";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("AS IF") && formatted.contains("AS END"),
            "keyword-like aliases inside PL/SQL SELECT should stay aliases, got:
{}",
            formatted
        );
        assert!(
            !formatted.contains(
                "
        IF"
            ) && !formatted.contains(
                "
        END"
            ),
            "alias IF/END inside PL/SQL block should not be split as control keywords, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_in_plsql_block_keeps_keyword_like_implicit_alias_inline() {
        let sql = "BEGIN SELECT amount IF INTO v_amount FROM sales; END;";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("amount IF") && formatted.contains("INTO v_amount"),
            "implicit alias IF inside PL/SQL SELECT should stay inline, got:
{}",
            formatted
        );
        assert!(
            !formatted.contains(
                "
        IF
"
            ),
            "implicit alias IF inside PL/SQL block should not be split as block keyword, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_keyword_like_member_access_identifiers_inline_in_join_on_clause() {
        let sql = "SELECT IF.amount FROM sales IF JOIN totals END ON IF.id = END.id";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("ON IF.id = END.id"),
            "keyword-like member access identifiers in ON clause should remain inline, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("\nIF.id") && !formatted.contains("\nEND.id"),
            "member access in ON clause should not be split as control keyword block tokens, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_keyword_like_member_access_identifiers_inline_in_where_clause() {
        let sql = "SELECT IF.amount FROM sales IF WHERE IF.amount > 100";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("WHERE IF.amount > 100"),
            "keyword-like member access identifiers in WHERE clause should remain inline, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("\nIF.amount"),
            "member access in WHERE clause should not be split as control keyword block token, got:\n{}",
            formatted
        );
    }

    #[test]
    fn full_auto_formatting_test24_package_body_set_comment_and_comma_follow_set_depth() {
        let source = include_str!("../../../test/test24.sql").to_string();
        let formatted = SqlEditorWidget::format_for_auto_formatting(&source, false);

        assert!(
            formatted.contains(
                "SET abcd = edfg
            -- comment
            ,
            ghij = klmo"
            ),
            "SET-list inline comment and comma should follow SET depth indentation, got:
{}",
            formatted
        );
        assert!(
            formatted.contains("FROM qwer;"),
            "formatted package body should keep the complete UPDATE statement, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_set_comment_and_comma_aligned_to_existing_multiline_set_depth() {
        let sql = "BEGIN
    UPDATE t
    SET a = 1,
        b = 2
        -- comment
        , c = 3
    WHERE id = 1;
END;";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains(
                "SET a = 1,
        b = 2
        -- comment
        ,
        c = 3"
            ),
            "SET-list comment/comma should reuse active multiline SET depth indentation, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_set_block_comment_and_comma_aligned_to_existing_multiline_set_depth()
    {
        let sql = "BEGIN
    UPDATE t
    SET a = 1,
        b = 2
        /* comment */
        , c = 3
    WHERE id = 1;
END;";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains(
                "SET a = 1,
        b = 2
        /* comment */
        ,
        c = 3"
            ),
            "SET-list block comment/comma should reuse active multiline SET depth indentation, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_comma_indent_after_line_comment_in_merge_using_clause() {
        let sql = "MERGE INTO t trg
USING src
ON (trg.id = src.id)
WHEN MATCHED THEN UPDATE SET
    trg.a = src.a -- comment
    , trg.b = src.b;";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains(
                "SET trg.a = src.a -- comment
    ,
    trg.b = src.b;"
            ),
            "line comment/comma after MERGE USING UPDATE SET should keep active list depth, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_realigns_join_on_nested_paren_condition_continuations() {
        let sql = "select *
from a
join b
    on ((1 = 1
                and 2 = 2))
        and 3 = 3;";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains(
                "JOIN b
    ON ((1 = 1
                AND 2 = 2))
        AND 3 = 3;"
            ),
            "nested ON-condition continuation should add one level per open paren before returning to the ON continuation depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn cursor_result_column_names_preserve_raw_headers_for_later_print() {
        let columns = vec![
            ColumnInfo {
                name: "EMPNO".to_string(),
                data_type: "Number".to_string(),
            },
            ColumnInfo {
                name: "ENAME".to_string(),
                data_type: "Varchar2".to_string(),
            },
        ];

        assert_eq!(
            SqlEditorWidget::cursor_result_column_names(&columns),
            vec!["EMPNO".to_string(), "ENAME".to_string()]
        );
    }
}

#[cfg(test)]
mod query_running_reservation_tests {
    use super::QueryRunningReservation;
    use std::sync::{Arc, Mutex};

    #[test]
    fn reservation_drop_releases_query_running_flag() {
        let query_running = Arc::new(Mutex::new(false));

        {
            let _reservation = QueryRunningReservation::acquire(query_running.clone());
            assert!(_reservation.is_some(), "query flag should be reservable");
            let running = query_running
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            assert!(*running);
        }

        let running = query_running
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(!*running);
    }

    #[test]
    fn reservation_disarm_keeps_query_running_flag_set() {
        let query_running = Arc::new(Mutex::new(false));

        {
            let mut reservation = QueryRunningReservation::acquire(query_running.clone());
            assert!(reservation.is_some(), "query flag should be reservable");
            if let Some(active) = reservation.as_mut() {
                active.disarm();
            }
        }

        let running = query_running
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(*running);
    }
}

#[cfg(test)]
mod mysql_transaction_feedback_tests {
    use super::SqlEditorWidget;
    use crate::db::QueryResult;
    use std::time::Duration;

    #[test]
    fn mysql_result_requires_transaction_feedback_for_call_dml_results() {
        let result = QueryResult::new_dml("CALL sync_users()", 3, Duration::from_secs(0), "CALL");

        assert!(SqlEditorWidget::mysql_result_requires_transaction_feedback(
            "CALL sync_users()",
            &result
        ));
    }

    #[test]
    fn apply_mysql_transaction_feedback_marks_call_results_for_manual_commit() {
        let mut result =
            QueryResult::new_dml("CALL sync_users()", 3, Duration::from_secs(0), "CALL");

        SqlEditorWidget::apply_mysql_transaction_feedback(&mut result, "CALL sync_users()", false);

        assert_eq!(result.message, "CALL 3 row(s) affected | Commit required");
    }

    #[test]
    fn apply_mysql_transaction_feedback_ignores_select_results() {
        let mut result =
            QueryResult::new_select("SELECT 1", Vec::new(), Vec::new(), Duration::from_secs(0));
        let original_message = result.message.clone();

        SqlEditorWidget::apply_mysql_transaction_feedback(&mut result, "SELECT 1", false);

        assert_eq!(result.message, original_message);
    }
}
