// Central definitions for cancel/timeout/lazy-fetch session policy described
// in `session.md`. The behavioural rules are already implemented elsewhere
// (see `src/ui/sql_editor/execution.rs` cleanup guard and the MySQL pooled
// action path); this module provides the named types, classifier, and
// decision functions the spec requires so they can be referenced uniformly.

use crate::db::connection::DatabaseType;

/// SQL classification for cancel / session-reuse decisions (session.md §6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlKind {
    SelectLike,
    Dml,
    Ddl,
    PlsqlOrProcedure,
    Script,
    TransactionControl,
    Unknown,
}

impl SqlKind {
    pub fn is_select_like(self) -> bool {
        matches!(self, SqlKind::SelectLike)
    }

    pub fn is_dml_or_ddl_or_plsql_or_script(self) -> bool {
        matches!(
            self,
            SqlKind::Dml | SqlKind::Ddl | SqlKind::PlsqlOrProcedure | SqlKind::Script
        )
    }
}

/// Execution state of a tab's worker (session.md §5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionState {
    Idle,
    RunningStatement,
    RunningScript,
    LazyFetchOnly,
    CancelRequested,
    ClosingCursor,
    Finished,
    Unknown,
}

/// Lazy-fetch lifecycle state (session.md §7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LazyFetchState {
    None,
    Waiting,
    Fetching,
    CloseRequested,
    CancelRequested,
    Closed,
    Unknown,
}

/// Outcome decision for a physical session after cancel/timeout (session.md §15).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionDecision {
    ReuseSamePhysicalSession,
    ReplacePhysicalSessionKeepUiConnected,
    RequireCommitOrRollback,
    MarkDirtyAndBlockNextExecution,
}

/// Snapshot captured at cancel-request time so late-arriving completion events
/// can be matched against the correct (tab, operation) (session.md §4).
#[derive(Clone, Debug)]
pub struct CancelTargetSnapshot {
    pub tab_id: u64,
    pub editor_id: u64,
    pub operation_id: u64,
    pub connection_generation: u64,
    pub db_type: DatabaseType,
    pub sql_kind: SqlKind,
    pub execution_state: ExecutionState,
    pub lazy_state: LazyFetchState,
    pub autocommit: bool,
}

/// Statement-finish payload carrying everything the cancel/timeout decision
/// path needs (session.md §27.4).
#[derive(Clone, Debug)]
pub struct ExecutionFinishedEvent {
    pub tab_id: u64,
    pub operation_id: u64,
    pub connection_generation: u64,
    pub db_type: DatabaseType,
    pub sql_kind: SqlKind,
    pub cancelled: bool,
    pub timed_out: bool,
    pub recoverable_timeout: bool,
    pub has_connection_error: bool,
    pub timeout_settings_restored: bool,
}

impl ExecutionFinishedEvent {
    pub fn new(db_type: DatabaseType) -> Self {
        Self {
            tab_id: 0,
            operation_id: 0,
            connection_generation: 0,
            db_type,
            sql_kind: SqlKind::Unknown,
            cancelled: false,
            timed_out: false,
            recoverable_timeout: false,
            has_connection_error: false,
            timeout_settings_restored: true,
        }
    }
}

/// Inputs required to decide what to do with a physical session after a
/// cancel / timeout / connection error (session.md §16).
#[derive(Clone, Copy, Debug)]
pub struct InterruptDecisionContext {
    pub operation_matches: bool,
    pub connection_generation_matches: bool,
    pub has_connection_error: bool,
    pub sql_kind: SqlKind,
    pub lazy_state: LazyFetchState,
    pub lazy_close_requested: bool,
    pub lazy_cancel_requested: bool,
    pub cursor_closed: bool,
    pub fetch_worker_done: bool,
    pub timed_out: bool,
    pub recoverable_timeout: bool,
    pub timeout_settings_restored: bool,
    pub health_check_ok: bool,
    pub autocommit: bool,
}

/// Implements the §16 decision tree literally so that cancel/timeout
/// post-processing can call a single function and get a consistent answer.
pub fn decide_session_after_interrupt(ctx: InterruptDecisionContext) -> SessionDecision {
    if !ctx.operation_matches || !ctx.connection_generation_matches {
        return SessionDecision::ReplacePhysicalSessionKeepUiConnected;
    }

    if ctx.has_connection_error {
        return SessionDecision::ReplacePhysicalSessionKeepUiConnected;
    }

    if ctx.sql_kind.is_dml_or_ddl_or_plsql_or_script() {
        if !ctx.autocommit {
            return SessionDecision::RequireCommitOrRollback;
        }
        return SessionDecision::ReplacePhysicalSessionKeepUiConnected;
    }

    if !ctx.sql_kind.is_select_like() {
        return SessionDecision::ReplacePhysicalSessionKeepUiConnected;
    }

    match ctx.lazy_state {
        LazyFetchState::None | LazyFetchState::Closed => {}
        LazyFetchState::Waiting => {
            if !ctx.lazy_close_requested || !ctx.cursor_closed {
                return SessionDecision::ReplacePhysicalSessionKeepUiConnected;
            }
        }
        LazyFetchState::Fetching => {
            if !ctx.lazy_cancel_requested || !ctx.fetch_worker_done || !ctx.cursor_closed {
                return SessionDecision::ReplacePhysicalSessionKeepUiConnected;
            }
        }
        LazyFetchState::CloseRequested
        | LazyFetchState::CancelRequested
        | LazyFetchState::Unknown => {
            return SessionDecision::ReplacePhysicalSessionKeepUiConnected;
        }
    }

    if ctx.timed_out && !ctx.recoverable_timeout {
        return SessionDecision::ReplacePhysicalSessionKeepUiConnected;
    }

    if !ctx.timeout_settings_restored {
        return SessionDecision::ReplacePhysicalSessionKeepUiConnected;
    }

    if ctx.health_check_ok {
        SessionDecision::ReuseSamePhysicalSession
    } else {
        SessionDecision::ReplacePhysicalSessionKeepUiConnected
    }
}

/// Hooks for `apply_session_decision` callers to mutate their own
/// logical/physical session state (session.md §27.6). The actual storage of
/// these flags lives in the editor; this trait keeps the decision-application
/// shape consistent across call sites.
pub trait SessionDecisionApplier {
    fn discard_physical_session(&mut self);
    fn mark_connected(&mut self);
    fn mark_replace_pending(&mut self);
    fn clear_replace_pending(&mut self);
    fn mark_transaction_decision_required(&mut self);
    fn mark_dirty_and_block_next_execution(&mut self);
}

/// Apply a §16 decision to the caller's session state (§27.6).
pub fn apply_session_decision<A: SessionDecisionApplier>(
    decision: SessionDecision,
    applier: &mut A,
) {
    match decision {
        SessionDecision::ReuseSamePhysicalSession => {
            applier.mark_connected();
            applier.clear_replace_pending();
        }
        SessionDecision::ReplacePhysicalSessionKeepUiConnected => {
            applier.discard_physical_session();
            applier.mark_connected();
            applier.mark_replace_pending();
        }
        SessionDecision::RequireCommitOrRollback => {
            applier.mark_connected();
            applier.mark_transaction_decision_required();
        }
        SessionDecision::MarkDirtyAndBlockNextExecution => {
            applier.mark_connected();
            applier.mark_dirty_and_block_next_execution();
        }
    }
}

/// Centralised recoverable-timeout check (session.md §12). The detailed
/// per-DB string matchers live in `execution.rs`; this wrapper accepts the
/// inputs the spec lists and delegates to those matchers.
pub fn is_recoverable_timeout(
    db_type: DatabaseType,
    err_msg: &str,
    sql_kind: SqlKind,
    lazy_state: LazyFetchState,
) -> bool {
    if !sql_kind.is_select_like() {
        return false;
    }
    if matches!(lazy_state, LazyFetchState::Unknown) {
        return false;
    }
    is_recoverable_timeout_message(db_type, err_msg)
}

/// Pure string-level recoverable-timeout check used both internally and by
/// callers that already filter by SQL kind / lazy state.
pub fn is_recoverable_timeout_message(db_type: DatabaseType, err_msg: &str) -> bool {
    let trimmed = err_msg.trim();
    let lower = trimmed.to_ascii_lowercase();

    if is_lock_wait_timeout_message(&lower) {
        return false;
    }
    if has_fatal_connection_marker(&lower) {
        return false;
    }

    match db_type {
        DatabaseType::Oracle => trimmed.contains("DPI-1067") || lower.contains("dpi-1067"),
        DatabaseType::MySQL => {
            lower.contains("error 3024")
                || lower.contains("er_query_timeout")
                || lower.contains("max_execution_time")
                || lower.contains("max_statement_time")
                || lower.contains("max statement time exceeded")
                || lower.contains("maximum statement execution time exceeded")
        }
    }
}

fn is_lock_wait_timeout_message(lower: &str) -> bool {
    lower.contains("error 1205") || lower.contains("lock wait timeout exceeded")
}

fn has_fatal_connection_marker(lower: &str) -> bool {
    [
        "ora-3114",
        "ora-03113",
        "ora-03114",
        "ora-03135",
        "error 2006",
        "error 2013",
        "server has gone away",
        "lost connection",
        "commands out of sync",
        "connection reset",
        "broken pipe",
        "socket timeout",
        "network timeout",
        "read timeout",
        "write timeout",
        "connection timeout",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

/// SQL classifier used to populate `CancelTargetSnapshot::sql_kind` and the
/// `decide_session_after_interrupt` `sql_kind` field (session.md §6).
pub fn classify_sql(sql: &str) -> SqlKind {
    let stripped = strip_leading_comments_and_whitespace(sql);
    if stripped.is_empty() {
        return SqlKind::Unknown;
    }

    if contains_multiple_statements(stripped) {
        return SqlKind::Script;
    }

    let upper = stripped.to_ascii_uppercase();
    let first_word = upper.split_whitespace().next().unwrap_or("");

    match first_word {
        "SELECT" | "WITH" | "EXPLAIN" | "DESCRIBE" | "DESC" | "SHOW" => SqlKind::SelectLike,
        "INSERT" | "UPDATE" | "DELETE" | "MERGE" | "REPLACE" | "LOAD" => SqlKind::Dml,
        "CREATE" | "ALTER" | "DROP" | "TRUNCATE" | "RENAME" | "COMMENT" | "GRANT" | "REVOKE" => {
            SqlKind::Ddl
        }
        "BEGIN" | "DECLARE" | "CALL" | "EXEC" | "EXECUTE" => SqlKind::PlsqlOrProcedure,
        "COMMIT" | "ROLLBACK" | "SAVEPOINT" | "SET" | "START" => SqlKind::TransactionControl,
        _ => SqlKind::Unknown,
    }
}

fn strip_leading_comments_and_whitespace(sql: &str) -> &str {
    let mut s = sql.trim_start();
    loop {
        if let Some(rest) = s.strip_prefix("--") {
            match rest.find('\n') {
                Some(idx) => s = rest[idx + 1..].trim_start(),
                None => return "",
            }
        } else if let Some(rest) = s.strip_prefix("/*") {
            match rest.find("*/") {
                Some(idx) => s = rest[idx + 2..].trim_start(),
                None => return "",
            }
        } else {
            break;
        }
    }
    s
}

fn contains_multiple_statements(sql: &str) -> bool {
    // Conservative heuristic: a `;` outside the trailing whitespace suggests
    // a multi-statement script. Strings/quotes are not parsed here; this is
    // intentionally lenient because Script -> ReplacePhysicalSession is the
    // safe direction (session.md §6.5).
    let trimmed = sql.trim_end();
    let trimmed = trimmed.trim_end_matches(';');
    trimmed.contains(';')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_ctx() -> InterruptDecisionContext {
        InterruptDecisionContext {
            operation_matches: true,
            connection_generation_matches: true,
            has_connection_error: false,
            sql_kind: SqlKind::SelectLike,
            lazy_state: LazyFetchState::None,
            lazy_close_requested: false,
            lazy_cancel_requested: false,
            cursor_closed: false,
            fetch_worker_done: false,
            timed_out: false,
            recoverable_timeout: false,
            timeout_settings_restored: true,
            health_check_ok: true,
            autocommit: true,
        }
    }

    #[test]
    fn select_cancel_with_health_check_reuses_session() {
        let decision = decide_session_after_interrupt(base_ctx());
        assert_eq!(decision, SessionDecision::ReuseSamePhysicalSession);
    }

    #[test]
    fn stale_operation_replaces_session() {
        let mut ctx = base_ctx();
        ctx.operation_matches = false;
        assert_eq!(
            decide_session_after_interrupt(ctx),
            SessionDecision::ReplacePhysicalSessionKeepUiConnected
        );
    }

    #[test]
    fn stale_connection_generation_replaces_session() {
        let mut ctx = base_ctx();
        ctx.connection_generation_matches = false;
        assert_eq!(
            decide_session_after_interrupt(ctx),
            SessionDecision::ReplacePhysicalSessionKeepUiConnected
        );
    }

    #[test]
    fn connection_error_replaces_session() {
        let mut ctx = base_ctx();
        ctx.has_connection_error = true;
        assert_eq!(
            decide_session_after_interrupt(ctx),
            SessionDecision::ReplacePhysicalSessionKeepUiConnected
        );
    }

    #[test]
    fn dml_with_autocommit_off_requires_decision() {
        let mut ctx = base_ctx();
        ctx.sql_kind = SqlKind::Dml;
        ctx.autocommit = false;
        assert_eq!(
            decide_session_after_interrupt(ctx),
            SessionDecision::RequireCommitOrRollback
        );
    }

    #[test]
    fn dml_with_autocommit_on_replaces_session() {
        let mut ctx = base_ctx();
        ctx.sql_kind = SqlKind::Dml;
        ctx.autocommit = true;
        assert_eq!(
            decide_session_after_interrupt(ctx),
            SessionDecision::ReplacePhysicalSessionKeepUiConnected
        );
    }

    #[test]
    fn script_replaces_session_even_with_autocommit_on() {
        let mut ctx = base_ctx();
        ctx.sql_kind = SqlKind::Script;
        ctx.autocommit = true;
        assert_eq!(
            decide_session_after_interrupt(ctx),
            SessionDecision::ReplacePhysicalSessionKeepUiConnected
        );
    }

    #[test]
    fn lazy_waiting_without_cursor_close_replaces_session() {
        let mut ctx = base_ctx();
        ctx.lazy_state = LazyFetchState::Waiting;
        ctx.lazy_close_requested = true;
        ctx.cursor_closed = false;
        assert_eq!(
            decide_session_after_interrupt(ctx),
            SessionDecision::ReplacePhysicalSessionKeepUiConnected
        );
    }

    #[test]
    fn lazy_waiting_with_cursor_close_reuses_session() {
        let mut ctx = base_ctx();
        ctx.lazy_state = LazyFetchState::Waiting;
        ctx.lazy_close_requested = true;
        ctx.cursor_closed = true;
        assert_eq!(
            decide_session_after_interrupt(ctx),
            SessionDecision::ReuseSamePhysicalSession
        );
    }

    #[test]
    fn lazy_fetching_without_worker_done_replaces_session() {
        let mut ctx = base_ctx();
        ctx.lazy_state = LazyFetchState::Fetching;
        ctx.lazy_cancel_requested = true;
        ctx.fetch_worker_done = false;
        ctx.cursor_closed = true;
        assert_eq!(
            decide_session_after_interrupt(ctx),
            SessionDecision::ReplacePhysicalSessionKeepUiConnected
        );
    }

    #[test]
    fn lazy_fetching_complete_reuses_session() {
        let mut ctx = base_ctx();
        ctx.lazy_state = LazyFetchState::Fetching;
        ctx.lazy_cancel_requested = true;
        ctx.fetch_worker_done = true;
        ctx.cursor_closed = true;
        assert_eq!(
            decide_session_after_interrupt(ctx),
            SessionDecision::ReuseSamePhysicalSession
        );
    }

    #[test]
    fn unknown_lazy_state_replaces_session() {
        let mut ctx = base_ctx();
        ctx.lazy_state = LazyFetchState::Unknown;
        assert_eq!(
            decide_session_after_interrupt(ctx),
            SessionDecision::ReplacePhysicalSessionKeepUiConnected
        );
    }

    #[test]
    fn non_recoverable_timeout_replaces_session() {
        let mut ctx = base_ctx();
        ctx.timed_out = true;
        ctx.recoverable_timeout = false;
        assert_eq!(
            decide_session_after_interrupt(ctx),
            SessionDecision::ReplacePhysicalSessionKeepUiConnected
        );
    }

    #[test]
    fn recoverable_timeout_select_reuses_session() {
        let mut ctx = base_ctx();
        ctx.timed_out = true;
        ctx.recoverable_timeout = true;
        assert_eq!(
            decide_session_after_interrupt(ctx),
            SessionDecision::ReuseSamePhysicalSession
        );
    }

    #[test]
    fn timeout_restore_failure_replaces_session() {
        let mut ctx = base_ctx();
        ctx.timeout_settings_restored = false;
        assert_eq!(
            decide_session_after_interrupt(ctx),
            SessionDecision::ReplacePhysicalSessionKeepUiConnected
        );
    }

    #[test]
    fn health_check_failure_replaces_session() {
        let mut ctx = base_ctx();
        ctx.health_check_ok = false;
        assert_eq!(
            decide_session_after_interrupt(ctx),
            SessionDecision::ReplacePhysicalSessionKeepUiConnected
        );
    }

    #[test]
    fn classify_select() {
        assert_eq!(classify_sql("SELECT * FROM t"), SqlKind::SelectLike);
        assert_eq!(classify_sql("  with x as (select 1) select * from x"), SqlKind::SelectLike);
        assert_eq!(classify_sql("/* hi */ -- a\n select 1"), SqlKind::SelectLike);
    }

    #[test]
    fn classify_dml() {
        assert_eq!(classify_sql("INSERT INTO t VALUES (1)"), SqlKind::Dml);
        assert_eq!(classify_sql("update t set a=1"), SqlKind::Dml);
        assert_eq!(classify_sql("DELETE FROM t"), SqlKind::Dml);
        assert_eq!(classify_sql("MERGE INTO t USING s ON ..."), SqlKind::Dml);
    }

    #[test]
    fn classify_ddl() {
        assert_eq!(classify_sql("CREATE TABLE t(x int)"), SqlKind::Ddl);
        assert_eq!(classify_sql("ALTER TABLE t ADD c int"), SqlKind::Ddl);
        assert_eq!(classify_sql("DROP TABLE t"), SqlKind::Ddl);
        assert_eq!(classify_sql("TRUNCATE TABLE t"), SqlKind::Ddl);
    }

    #[test]
    fn classify_plsql() {
        assert_eq!(classify_sql("BEGIN NULL; END;"), SqlKind::Script);
        assert_eq!(classify_sql("CALL my_proc(1)"), SqlKind::PlsqlOrProcedure);
        assert_eq!(classify_sql("DECLARE x int"), SqlKind::PlsqlOrProcedure);
    }

    #[test]
    fn classify_transaction_control() {
        assert_eq!(classify_sql("COMMIT"), SqlKind::TransactionControl);
        assert_eq!(classify_sql("rollback"), SqlKind::TransactionControl);
        assert_eq!(classify_sql("SET autocommit = 0"), SqlKind::TransactionControl);
    }

    #[test]
    fn classify_script() {
        assert_eq!(
            classify_sql("SELECT 1; SELECT 2;"),
            SqlKind::Script
        );
    }

    #[test]
    fn classify_unknown() {
        assert_eq!(classify_sql(""), SqlKind::Unknown);
        assert_eq!(classify_sql("/* only comment */"), SqlKind::Unknown);
        assert_eq!(classify_sql("???"), SqlKind::Unknown);
    }

    #[test]
    fn recoverable_timeout_oracle_dpi_1067_select() {
        assert!(is_recoverable_timeout(
            DatabaseType::Oracle,
            "ORA-DPI-1067: call timeout exceeded",
            SqlKind::SelectLike,
            LazyFetchState::None
        ));
    }

    #[test]
    fn recoverable_timeout_mysql_3024_select() {
        assert!(is_recoverable_timeout(
            DatabaseType::MySQL,
            "Error 3024: ER_QUERY_TIMEOUT",
            SqlKind::SelectLike,
            LazyFetchState::None
        ));
    }

    #[test]
    fn recoverable_timeout_dml_returns_false() {
        assert!(!is_recoverable_timeout(
            DatabaseType::MySQL,
            "Error 3024",
            SqlKind::Dml,
            LazyFetchState::None
        ));
    }

    #[test]
    fn recoverable_timeout_unknown_lazy_returns_false() {
        assert!(!is_recoverable_timeout(
            DatabaseType::Oracle,
            "DPI-1067",
            SqlKind::SelectLike,
            LazyFetchState::Unknown
        ));
    }

    #[test]
    fn recoverable_timeout_lock_wait_returns_false() {
        assert!(!is_recoverable_timeout(
            DatabaseType::MySQL,
            "Error 1205: lock wait timeout exceeded",
            SqlKind::SelectLike,
            LazyFetchState::None
        ));
    }

    #[test]
    fn recoverable_timeout_fatal_marker_returns_false() {
        assert!(!is_recoverable_timeout(
            DatabaseType::MySQL,
            "Error 2006: server has gone away (max_execution_time)",
            SqlKind::SelectLike,
            LazyFetchState::None
        ));
        assert!(!is_recoverable_timeout(
            DatabaseType::Oracle,
            "ORA-03113 end-of-file on communication channel; DPI-1067",
            SqlKind::SelectLike,
            LazyFetchState::None
        ));
    }

    struct StubApplier {
        events: Vec<&'static str>,
    }

    impl SessionDecisionApplier for StubApplier {
        fn discard_physical_session(&mut self) {
            self.events.push("discard");
        }
        fn mark_connected(&mut self) {
            self.events.push("connected");
        }
        fn mark_replace_pending(&mut self) {
            self.events.push("replace_pending");
        }
        fn clear_replace_pending(&mut self) {
            self.events.push("clear_replace_pending");
        }
        fn mark_transaction_decision_required(&mut self) {
            self.events.push("transaction_decision");
        }
        fn mark_dirty_and_block_next_execution(&mut self) {
            self.events.push("dirty_block");
        }
    }

    #[test]
    fn apply_reuse_clears_replace_pending() {
        let mut a = StubApplier { events: vec![] };
        apply_session_decision(SessionDecision::ReuseSamePhysicalSession, &mut a);
        assert_eq!(a.events, vec!["connected", "clear_replace_pending"]);
    }

    #[test]
    fn apply_replace_discards_and_marks_pending() {
        let mut a = StubApplier { events: vec![] };
        apply_session_decision(
            SessionDecision::ReplacePhysicalSessionKeepUiConnected,
            &mut a,
        );
        assert_eq!(a.events, vec!["discard", "connected", "replace_pending"]);
    }

    #[test]
    fn apply_require_decision_marks_transaction() {
        let mut a = StubApplier { events: vec![] };
        apply_session_decision(SessionDecision::RequireCommitOrRollback, &mut a);
        assert_eq!(a.events, vec!["connected", "transaction_decision"]);
    }

    #[test]
    fn apply_dirty_marks_block() {
        let mut a = StubApplier { events: vec![] };
        apply_session_decision(SessionDecision::MarkDirtyAndBlockNextExecution, &mut a);
        assert_eq!(a.events, vec!["connected", "dirty_block"]);
    }
}
