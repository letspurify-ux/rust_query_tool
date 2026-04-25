use std::fs;
use std::path::{Path, PathBuf};

fn collect_rust_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let entries = fs::read_dir(&dir)
            .unwrap_or_else(|err| panic!("failed to read directory {}: {err}", dir.display()));

        for entry in entries {
            let entry = entry.unwrap_or_else(|err| {
                panic!("failed to read directory entry in {}: {err}", dir.display())
            });
            let path = entry.path();

            if path.is_dir() {
                stack.push(path);
                continue;
            }

            if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
                files.push(path);
            }
        }
    }

    files
}

#[test]
fn thread_spawn_files_do_not_use_rc_or_refcell() {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();

    for file in collect_rust_files(&src_root) {
        let content = fs::read_to_string(&file)
            .unwrap_or_else(|err| panic!("failed to read source file {}: {err}", file.display()));

        if !content.contains("thread::spawn") {
            continue;
        }

        if content.contains("Rc<")
            || content.contains("std::rc::Rc")
            || content.contains("RefCell")
            || content.contains("std::cell::RefCell")
        {
            offenders.push(file);
        }
    }

    assert!(
        offenders.is_empty(),
        "thread::spawn files must not use Rc/RefCell: {:?}",
        offenders
    );
}

#[test]
fn shared_connection_is_arc_mutex() {
    let file = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/connection.rs");
    let content = fs::read_to_string(&file)
        .unwrap_or_else(|err| panic!("failed to read source file {}: {err}", file.display()));

    assert!(
        content.contains("pub type SharedConnection = Arc<Mutex<DatabaseConnection>>;"),
        "SharedConnection type alias must remain Arc<Mutex<DatabaseConnection>>"
    );
}

#[test]
fn oracle_execution_pool_acquire_happens_outside_connection_mutex() {
    let file = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/ui/sql_editor/execution.rs");
    let content = fs::read_to_string(&file)
        .unwrap_or_else(|err| panic!("failed to read source file {}: {err}", file.display()));

    assert!(
        !content.contains("conn_guard.acquire_pool_session()"),
        "Oracle execution must not acquire a pooled session through ConnectionLockGuard"
    );
    assert!(
        content.contains(
            "let pool_session_result = Self::acquire_fresh_oracle_pool_session(&pool, sender);"
        ),
        "Oracle execution should acquire fresh pooled sessions through the lock-free helper"
    );
}

#[test]
fn oracle_execution_takes_reusable_pool_session_exclusively() {
    let file = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/ui/sql_editor/execution.rs");
    let content = fs::read_to_string(&file)
        .unwrap_or_else(|err| panic!("failed to read source file {}: {err}", file.display()));

    assert!(
        content.contains(
            "pooled_db_session\n            .take_reusable_with_state(connection_generation, crate::db::DatabaseType::Oracle)"
        ),
        "Oracle execution must take the reusable lease out of the shared slot before using it"
    );
    assert!(
        !content.contains(
            "crate::db::current_oracle_pooled_session_lease(pooled_db_session, connection_generation)"
        ),
        "Oracle execution must not clone a reusable lease while leaving it visible to lazy fetch"
    );
    assert!(
        !content.contains("connection_generation,\n                                lease,\n                                false,"),
        "Fresh Oracle pooled sessions must not be stored back before execution/lazy fetch finishes"
    );
}

#[test]
fn oracle_transaction_actions_take_reusable_pool_session_exclusively() {
    let file = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/ui/sql_editor/mod.rs");
    let content = fs::read_to_string(&file)
        .unwrap_or_else(|err| panic!("failed to read source file {}: {err}", file.display()));

    assert!(
        content.contains(".take_reusable_with_state(\n                                connection_generation,\n                                crate::db::DatabaseType::Oracle,"),
        "Oracle transaction actions must take the reusable lease out of the shared slot before using it"
    );
    assert!(
        !content.contains("current_oracle_pooled_session_lease("),
        "Oracle transaction actions must not clone a reusable lease while leaving it visible"
    );
    assert!(
        content.contains("DbSessionLease::Oracle(Arc::clone(&db_conn))"),
        "Reusable Oracle transaction action sessions should be stored back only after cleanup"
    );
}

#[test]
fn db_tab_session_slot_is_shared_abstraction_not_raw_arc_alias() {
    let file = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/connection.rs");
    let content = fs::read_to_string(&file)
        .unwrap_or_else(|err| panic!("failed to read source file {}: {err}", file.display()));

    assert!(
        content.contains("pub struct SharedDbSessionLease"),
        "Tab DB session ownership should be represented by a shared slot abstraction"
    );
    assert!(
        !content.contains("pub type SharedDbSessionLease = Arc<Mutex"),
        "Tab DB session ownership must not leak as a raw Arc<Mutex<...>> alias"
    );
    assert!(
        content.contains("pub fn take_reusable_with_state(")
            && content.contains("pub fn store_if_empty(")
            && content.contains("pub fn clear("),
        "Oracle/MySQL/MariaDB tab sessions should share the same take/store/clear lifecycle API"
    );
}

#[test]
fn oracle_reused_open_transaction_skips_transaction_mode_reapply() {
    let file = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/ui/sql_editor/execution.rs");
    let content = fs::read_to_string(&file)
        .unwrap_or_else(|err| panic!("failed to read source file {}: {err}", file.display()));

    assert!(
        content.contains("let should_apply_oracle_transaction_mode =\n                    !oracle_prior_may_have_uncommitted_work;"),
        "Oracle execution must not reapply SET TRANSACTION on a pooled session with open work"
    );
    assert!(
        content.contains("if should_apply_oracle_transaction_mode {\n                        if let Err(err) =\n                            crate::db::DatabaseConnection::apply_oracle_transaction_mode"),
        "Oracle transaction mode application should be guarded by the open-transaction check"
    );
    assert!(
        !content.contains("track_oracle_read_only_transaction"),
        "Oracle read-only execution should not arm old read-only cleanup; the tab owns the pooled session until commit, rollback, cancel, or close"
    );
}

#[test]
fn mysql_reused_tab_session_does_not_reselect_global_database() {
    let file = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/ui/sql_editor/execution.rs");
    let content = fs::read_to_string(&file)
        .unwrap_or_else(|err| panic!("failed to read source file {}: {err}", file.display()));

    let start = content
        .find("fn reusable_mysql_pooled_session_is_ready(")
        .expect("reusable MySQL session helper should exist");
    let end = content[start..]
        .find("fn prepare_mysql_pooled_session_or_retry_once(")
        .map(|offset| start + offset)
        .expect("fresh MySQL session preparation helper should follow reusable helper");
    let helper_body = &content[start..end];

    assert!(
        !helper_body.contains("current_service_name"),
        "Reusable MySQL/MariaDB tab sessions must not use the global current database"
    );
    assert!(
        !helper_body.contains("prepare_mysql_pooled_session_database"),
        "Reusable MySQL/MariaDB tab sessions must keep their own selected database; only fresh sessions should be prepared from global connection metadata"
    );
}

#[test]
fn mysql_use_refreshes_metadata_without_connection_transition() {
    let file = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/ui/sql_editor/execution.rs");
    let content = fs::read_to_string(&file)
        .unwrap_or_else(|err| panic!("failed to read source file {}: {err}", file.display()));

    for (start_marker, end_marker) in [
        (
            "ToolCommand::Use { database } =>",
            "ToolCommand::MysqlDelimiter",
        ),
        (
            "ToolCommand::Use { ref database } =>",
            "// MySQL-specific commands",
        ),
    ] {
        let start = content
            .find(start_marker)
            .unwrap_or_else(|| panic!("MySQL USE command branch should exist: {start_marker}"));
        let end = content[start..]
            .find(end_marker)
            .map(|offset| start + offset)
            .unwrap_or_else(|| panic!("USE branch end marker should exist: {end_marker}"));
        let use_branch = &content[start..end];

        assert!(
            use_branch.contains("QueryProgress::DatabaseChanged"),
            "USE should update the selected database without being treated as a connection transition"
        );
        assert!(
            use_branch.contains("QueryProgress::MetadataRefreshNeeded"),
            "USE should still fall back to metadata refresh when no UI connection info is available"
        );
        assert!(
            !use_branch.contains("QueryProgress::ConnectionChanged"),
            "USE is a tab-session database change, not a connection transition that clears all tab sessions"
        );
    }
}

#[test]
fn mysql_database_changed_updates_object_browser_without_clearing_sessions() {
    let file = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/ui/main_window.rs");
    let content = fs::read_to_string(&file)
        .unwrap_or_else(|err| panic!("failed to read source file {}: {err}", file.display()));

    let start = content
        .find("QueryProgress::DatabaseChanged { info } =>")
        .expect("DatabaseChanged handler should exist");
    let end = content[start..]
        .find("QueryProgress::StatementFinished")
        .map(|offset| start + offset)
        .expect("StatementFinished handler should follow DatabaseChanged");
    let handler = &content[start..end];

    assert!(
        handler.contains("object_browser.set_selected_scope"),
        "DatabaseChanged should select the new database in the object browser"
    );
    assert!(
        handler.contains("start_connection_metadata_refresh"),
        "DatabaseChanged should reload object browser and schema metadata"
    );
    assert!(
        !handler.contains("release_all_pooled_db_sessions"),
        "DatabaseChanged must not clear tab-owned DB sessions"
    );
}

#[test]
fn mysql_script_autocommit_changes_are_tab_local() {
    let file = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/ui/sql_editor/execution.rs");
    let content = fs::read_to_string(&file)
        .unwrap_or_else(|err| panic!("failed to read source file {}: {err}", file.display()));

    let batch_start = content
        .find("fn execute_mysql_batch(")
        .expect("MySQL batch executor should exist");
    let branch_start = content[batch_start..]
        .find("ToolCommand::SetAutoCommit { enabled } =>")
        .map(|offset| batch_start + offset)
        .expect("MySQL SET AUTOCOMMIT command branch should exist");
    let branch_end = content[branch_start..]
        .find("ToolCommand::Use { database }")
        .map(|offset| branch_start + offset)
        .expect("USE branch should follow SET AUTOCOMMIT branch");
    let autocommit_branch = &content[branch_start..branch_end];

    assert!(
        autocommit_branch
            .contains("store_mutex_bool_option(mysql_auto_commit_override, Some(enabled))"),
        "MySQL/MariaDB script autocommit state should be stored on the editor tab"
    );
    assert!(
        !autocommit_branch.contains("conn_guard.set_auto_commit(enabled)"),
        "MySQL/MariaDB script autocommit changes must not mutate the shared connection default for other tabs"
    );
}
