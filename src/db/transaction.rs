use serde::{Deserialize, Serialize};

use crate::db::query::QueryExecutor;
use crate::sql_text;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionIsolation {
    #[default]
    Default,
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionAccessMode {
    #[default]
    ReadWrite,
    ReadOnly,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionMode {
    pub isolation: TransactionIsolation,
    pub access_mode: TransactionAccessMode,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionSessionState {
    #[default]
    Clean,
    MaybeDirty,
    DecisionRequired,
    InvalidSession,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TransactionStatementStateHint {
    pub(crate) clears_session_state: bool,
    pub(crate) may_leave_session_bound_state: bool,
    pub(crate) may_hold_session_lock: bool,
    pub(crate) requires_retention_when_autocommit_off: bool,
    pub(crate) requires_transaction_decision_after_success: bool,
    pub(crate) changes_auto_commit: bool,
}

impl TransactionSessionState {
    pub fn from_flags(may_have_uncommitted_work: bool, requires_decision: bool) -> Self {
        if requires_decision {
            Self::DecisionRequired
        } else if may_have_uncommitted_work {
            Self::MaybeDirty
        } else {
            Self::Clean
        }
    }

    pub fn allows_transaction_option_change(self) -> bool {
        matches!(self, Self::Clean)
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Clean => "clean",
            Self::MaybeDirty => "maybe dirty",
            Self::DecisionRequired => "decision required",
            Self::InvalidSession => "invalid session",
        }
    }
}

fn mysql_hint(
    clears_session_state: bool,
    may_leave_session_bound_state: bool,
    may_hold_session_lock: bool,
    requires_retention_when_autocommit_off: bool,
    requires_transaction_decision_after_success: bool,
) -> TransactionStatementStateHint {
    TransactionStatementStateHint {
        clears_session_state,
        may_leave_session_bound_state,
        may_hold_session_lock,
        requires_retention_when_autocommit_off,
        requires_transaction_decision_after_success,
        changes_auto_commit: false,
    }
}

fn mysql_autocommit_hint(enabled: bool) -> TransactionStatementStateHint {
    TransactionStatementStateHint {
        clears_session_state: enabled,
        may_leave_session_bound_state: !enabled,
        may_hold_session_lock: false,
        requires_retention_when_autocommit_off: false,
        requires_transaction_decision_after_success: false,
        changes_auto_commit: true,
    }
}

fn mysql_autocommit_assignment_value(sql: &str) -> Option<String> {
    let cleaned = QueryExecutor::strip_leading_comments(sql);
    let mut normalized = cleaned
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_ascii_uppercase();
    normalized.retain(|ch| !ch.is_whitespace());
    let mut assignments = normalized.strip_prefix("SET")?;
    if assignments.starts_with("GLOBAL") || assignments.starts_with("PERSIST") {
        return None;
    }
    assignments = assignments.strip_prefix("SESSION").unwrap_or(assignments);

    for assignment in assignments.split(',') {
        let Some(value) = [
            "AUTOCOMMIT",
            "@@AUTOCOMMIT",
            "@@SESSION.AUTOCOMMIT",
        ]
        .iter()
        .find_map(|prefix| assignment.strip_prefix(prefix))
        .and_then(|value| value.strip_prefix('=').or_else(|| value.strip_prefix(":=")))
        else {
            continue;
        };
        return Some(value.to_string());
    }
    None
}

pub(crate) fn mysql_set_autocommit_value(sql: &str) -> Option<bool> {
    let value = mysql_autocommit_assignment_value(sql)?;
    match value.as_str() {
        "1" | "ON" | "TRUE" => Some(true),
        "0" | "OFF" | "FALSE" => Some(false),
        _ => None,
    }
}

fn mysql_is_autocommit_assignment(sql: &str) -> bool {
    mysql_autocommit_assignment_value(sql).is_some()
}

pub(crate) fn mysql_create_statement_is_temporary(sql: &str) -> bool {
    let cleaned = QueryExecutor::strip_leading_comments(sql);
    let mut words = cleaned
        .split_whitespace()
        .map(|word| word.trim_matches(|ch: char| !sql_text::is_identifier_char(ch)));
    matches!(words.next(), Some(word) if word.eq_ignore_ascii_case("CREATE"))
        && matches!(words.next(), Some(word) if word.eq_ignore_ascii_case("TEMPORARY"))
}

pub(crate) fn mysql_rollback_targets_savepoint(sql: &str) -> bool {
    let cleaned = QueryExecutor::strip_leading_comments(sql);
    let mut words = cleaned
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .map(|word| word.trim_matches(|ch: char| !sql_text::is_identifier_char(ch)));
    matches!(words.next(), Some(word) if word.eq_ignore_ascii_case("ROLLBACK"))
        && matches!(words.next(), Some(word) if word.eq_ignore_ascii_case("TO"))
}

pub(crate) fn mysql_transaction_control_starts_chain(sql: &str) -> bool {
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

pub(crate) fn mysql_statement_may_leave_uncommitted_work(sql: &str) -> bool {
    matches!(
        QueryExecutor::leading_keyword(sql).as_deref(),
        Some("INSERT")
            | Some("UPDATE")
            | Some("DELETE")
            | Some("REPLACE")
            | Some("WITH")
            | Some("CALL")
            | Some("LOAD")
            | Some("START")
            | Some("BEGIN")
            | Some("SAVEPOINT")
            | Some("XA")
    )
}

pub(crate) fn mysql_statement_acquires_table_lock(sql: &str) -> bool {
    matches!(QueryExecutor::leading_keyword(sql).as_deref(), Some("LOCK"))
}

pub(crate) fn mysql_statement_releases_table_lock(sql: &str) -> bool {
    matches!(
        QueryExecutor::leading_keyword(sql).as_deref(),
        Some("UNLOCK")
    )
}

pub(crate) fn mysql_statement_acquires_named_lock(sql: &str) -> bool {
    matches!(
        QueryExecutor::leading_keyword(sql).as_deref(),
        Some("SELECT")
    ) && sql.to_ascii_uppercase().contains("GET_LOCK")
}

pub(crate) fn mysql_statement_releases_named_lock(sql: &str) -> bool {
    matches!(
        QueryExecutor::leading_keyword(sql).as_deref(),
        Some("SELECT")
    ) && sql.to_ascii_uppercase().contains("RELEASE_LOCK")
}

pub(crate) fn mysql_statement_releases_all_named_locks(sql: &str) -> bool {
    matches!(
        QueryExecutor::leading_keyword(sql).as_deref(),
        Some("SELECT")
    ) && sql.to_ascii_uppercase().contains("RELEASE_ALL_LOCKS")
}

fn mysql_select_assigns_user_variable(sql: &str) -> bool {
    let cleaned = QueryExecutor::strip_leading_comments(sql);
    let mut compact = cleaned.to_ascii_uppercase();
    compact.retain(|ch| !ch.is_whitespace());
    compact.contains("INTO@")
}

pub(crate) fn mysql_session_state_hint_for_sql(sql: &str) -> TransactionStatementStateHint {
    if QueryExecutor::is_plain_commit(sql) || QueryExecutor::is_plain_rollback(sql) {
        return mysql_hint(true, false, false, false, false);
    }

    if let Some(enabled) = mysql_set_autocommit_value(sql) {
        return mysql_autocommit_hint(enabled);
    }

    if mysql_is_autocommit_assignment(sql) {
        return TransactionStatementStateHint {
            changes_auto_commit: true,
            ..mysql_hint(false, true, false, true, true)
        };
    }

    let leading = QueryExecutor::leading_keyword(sql);
    match leading.as_deref() {
        Some("COMMIT") => {
            if mysql_transaction_control_starts_chain(sql) {
                mysql_hint(false, true, false, true, false)
            } else {
                mysql_hint(false, true, false, true, true)
            }
        }
        Some("ROLLBACK") => {
            if mysql_rollback_targets_savepoint(sql) {
                mysql_hint(false, false, false, false, false)
            } else if mysql_transaction_control_starts_chain(sql) {
                mysql_hint(false, true, false, true, false)
            } else {
                mysql_hint(false, true, false, true, true)
            }
        }
        Some("START") | Some("BEGIN") | Some("SAVEPOINT") | Some("CALL") | Some("XA") => {
            mysql_hint(false, true, false, true, false)
        }
        Some("WITH")
            if crate::db::query::mysql_executor::MysqlExecutor::is_select_statement(sql) =>
        {
            TransactionStatementStateHint::default()
        }
        Some("INSERT") | Some("UPDATE") | Some("DELETE") | Some("REPLACE") | Some("WITH") => {
            mysql_hint(false, true, false, true, false)
        }
        Some("LOAD") => mysql_hint(false, true, false, true, false),
        Some("PREPARE") | Some("EXECUTE") | Some("DEALLOCATE") => {
            mysql_hint(false, true, false, false, false)
        }
        Some("LOCK") => mysql_hint(true, true, true, true, false),
        Some("UNLOCK") => mysql_hint(true, false, false, false, false),
        Some("CREATE") if mysql_create_statement_is_temporary(sql) => {
            mysql_hint(false, true, false, false, false)
        }
        Some("CREATE") | Some("ALTER") | Some("DROP") | Some("RENAME") | Some("TRUNCATE") => {
            mysql_hint(true, false, false, false, false)
        }
        Some("SET") => mysql_hint(false, true, false, false, false),
        Some("SELECT") if mysql_select_assigns_user_variable(sql) => {
            mysql_hint(false, true, false, false, false)
        }
        Some("SELECT") if sql.to_ascii_uppercase().contains("GET_LOCK") => {
            mysql_hint(false, true, true, true, false)
        }
        _ => TransactionStatementStateHint::default(),
    }
}

pub(crate) fn mysql_session_may_need_preservation_after_statement(
    prior_may_have_uncommitted_work: bool,
    state_hint: TransactionStatementStateHint,
    server_reports_uncommitted_work: bool,
    statement_failed: bool,
    fallback_on_error: bool,
) -> bool {
    server_reports_uncommitted_work
        || state_hint.may_hold_session_lock
        || (prior_may_have_uncommitted_work
            && (!state_hint.clears_session_state || statement_failed))
        || (fallback_on_error && state_hint.requires_retention_when_autocommit_off)
}

pub(crate) fn mysql_requires_transaction_decision_after_statement(
    prior_requires_transaction_decision: bool,
    state_hint: TransactionStatementStateHint,
    statement_failed: bool,
    interruption_requires_transaction_decision: bool,
) -> bool {
    if interruption_requires_transaction_decision {
        return true;
    }
    if statement_failed {
        return prior_requires_transaction_decision;
    }
    if state_hint.clears_session_state {
        return false;
    }
    prior_requires_transaction_decision || state_hint.requires_transaction_decision_after_success
}

impl TransactionIsolation {
    pub fn label(self) -> &'static str {
        match self {
            Self::Default => "Default",
            Self::ReadUncommitted => "Read uncommitted",
            Self::ReadCommitted => "Read committed",
            Self::RepeatableRead => "Repeatable read",
            Self::Serializable => "Serializable",
        }
    }

    pub(crate) fn sql_level(self) -> Option<&'static str> {
        match self {
            Self::Default => None,
            Self::ReadUncommitted => Some("READ UNCOMMITTED"),
            Self::ReadCommitted => Some("READ COMMITTED"),
            Self::RepeatableRead => Some("REPEATABLE READ"),
            Self::Serializable => Some("SERIALIZABLE"),
        }
    }

    pub(crate) fn from_sql_level(value: &str) -> Option<Self> {
        let normalized = value
            .trim()
            .replace(['-', '_'], " ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_uppercase();

        match normalized.as_str() {
            "READ UNCOMMITTED" => Some(Self::ReadUncommitted),
            "READ COMMITED" => Some(Self::ReadCommitted),
            "READ COMMITTED" => Some(Self::ReadCommitted),
            "REPEATABLE READ" => Some(Self::RepeatableRead),
            "SERIALIZABLE" => Some(Self::Serializable),
            _ => None,
        }
    }
}

impl TransactionAccessMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::ReadWrite => "Read write",
            Self::ReadOnly => "Read only",
        }
    }

    pub(crate) fn sql_clause(self) -> &'static str {
        match self {
            Self::ReadWrite => "READ WRITE",
            Self::ReadOnly => "READ ONLY",
        }
    }
}

impl TransactionMode {
    pub fn new(isolation: TransactionIsolation, access_mode: TransactionAccessMode) -> Self {
        Self {
            isolation,
            access_mode,
        }
    }

    pub fn is_default(self) -> bool {
        self == Self::default()
    }

    pub fn label(self) -> String {
        format!("{}, {}", self.isolation.label(), self.access_mode.label())
    }
}
