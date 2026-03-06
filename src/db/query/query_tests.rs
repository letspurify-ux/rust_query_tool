use super::*;

/// Helper to extract statements from ScriptItems
fn get_statements(items: &[ScriptItem]) -> Vec<&str> {
    items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect()
}

#[test]
fn test_statement_bounds_at_cursor_ignores_string_literal_with_previous_statement_text() {
    let sql = "SELECT 1 FROM dual;
SELECT 'SELECT 1 FROM dual' AS txt FROM dual;";
    let cursor = sql.rfind("txt").unwrap_or(sql.len());

    let bounds = QueryExecutor::statement_bounds_at_cursor(sql, cursor)
        .expect("expected statement bounds for second statement");
    let statement = &sql[bounds.0..bounds.1];

    assert!(
        statement.contains("AS txt FROM dual"),
        "expected second statement, got: {statement}"
    );
}

#[test]
fn test_statement_bounds_at_cursor_ignores_comment_with_previous_statement_text() {
    let sql = "SELECT 1 FROM dual;
/* SELECT 1 FROM dual */
SELECT 2 FROM dual;";
    let cursor = sql.rfind("2 FROM dual").unwrap_or(sql.len());

    let bounds = QueryExecutor::statement_bounds_at_cursor(sql, cursor)
        .expect("expected statement bounds for statement after comment");
    let statement = &sql[bounds.0..bounds.1];

    assert!(
        statement.starts_with("SELECT 2 FROM dual"),
        "expected final statement, got: {statement}"
    );
}

#[test]
fn test_statement_bounds_at_cursor_clamps_non_boundary_utf8_offset() {
    let sql = "SELECT 1 FROM dual;\nSELECT 한글 AS txt FROM dual;";
    let utf8_start = sql
        .find('한')
        .expect("expected utf-8 anchor in second statement");
    let mid_char_cursor = utf8_start + 1;
    assert!(
        !sql.is_char_boundary(mid_char_cursor),
        "test requires a non-byte-boundary cursor to validate clamping"
    );

    let bounds = QueryExecutor::statement_bounds_at_cursor(sql, mid_char_cursor)
        .expect("expected statement bounds for UTF-8 cursor offset");
    let statement = &sql[bounds.0..bounds.1];

    assert!(
        statement.contains("한글 AS txt"),
        "expected second statement, got: {statement}"
    );
}

#[test]
fn test_statement_bounds_at_cursor_prefers_next_statement_on_boundary() {
    let sql = "SELECT 1 FROM dual;
SELECT 2 FROM dual;";
    let boundary_cursor = sql.find("SELECT 2").unwrap_or(sql.len());

    let bounds = QueryExecutor::statement_bounds_at_cursor(sql, boundary_cursor)
        .expect("expected statement bounds at boundary cursor");
    let statement = &sql[bounds.0..bounds.1];

    assert!(
        statement.starts_with("SELECT 2 FROM dual"),
        "expected second statement at boundary, got: {statement}"
    );
}

#[test]
fn test_statement_bounds_at_cursor_splits_around_tool_command_line() {
    let sql = "SELECT 1 FROM dual;\nPROMPT section\nSELECT 2 FROM dual;";
    let cursor = sql.rfind("2 FROM dual").unwrap_or(sql.len());

    let bounds = QueryExecutor::statement_bounds_at_cursor(sql, cursor)
        .expect("expected statement bounds after tool command line");
    let statement = &sql[bounds.0..bounds.1];

    assert!(
        statement.trim_start().starts_with("SELECT 2 FROM dual"),
        "expected final SELECT statement, got: {statement}"
    );
    assert!(
        !statement.contains("PROMPT"),
        "tool command line must not leak into SQL statement: {statement}"
    );
}

#[test]
fn test_statement_bounds_at_cursor_splits_after_with_function_and_run_script_command() {
    let sql = "WITH\n  FUNCTION f RETURN NUMBER IS\n  BEGIN\n    RETURN 1;\n  END;\n@child.sql\nSELECT 2 FROM dual;";
    let cursor = sql.rfind("2 FROM dual").unwrap_or(sql.len());

    let bounds = QueryExecutor::statement_bounds_at_cursor(sql, cursor)
        .expect("expected statement bounds after @ script command");
    let statement = &sql[bounds.0..bounds.1];

    assert!(
        statement.trim_start().starts_with("SELECT 2 FROM dual"),
        "expected trailing SELECT statement, got: {statement}"
    );
    assert!(
        !statement.contains("@child.sql"),
        "run-script command line must not leak into SQL statement: {statement}"
    );
}

#[test]
fn test_statement_bounds_at_cursor_keeps_multiline_alter_session_set_clause() {
    let sql = "ALTER SESSION\nSET NLS_DATE_FORMAT = 'YYYY-MM-DD';\nSELECT 1 FROM dual;";
    let cursor = sql.find("NLS_DATE_FORMAT").unwrap_or(0);

    let bounds = QueryExecutor::statement_bounds_at_cursor(sql, cursor)
        .expect("expected ALTER SESSION statement bounds");
    let statement = &sql[bounds.0..bounds.1];

    assert!(
        statement.contains("ALTER SESSION"),
        "ALTER SESSION header should be included: {statement}"
    );
    assert!(
        statement.contains("SET NLS_DATE_FORMAT"),
        "SET clause should remain part of ALTER SESSION: {statement}"
    );
    assert!(
        !statement.contains("SELECT 1 FROM dual"),
        "next statement should not be included: {statement}"
    );
}

#[test]
fn test_statement_bounds_at_cursor_create_java_source_ignores_body_semicolon_until_slash() {
    let sql = r#"CREATE OR REPLACE AND COMPILE JAVA SOURCE NAMED "DemoClass" AS
public class DemoClass {
  public static String hello() {
    return "hello";
  }
}
/
SELECT 2 FROM dual;"#;
    let cursor = sql.find("return \"hello\"").unwrap_or(0);

    let bounds = QueryExecutor::statement_bounds_at_cursor(sql, cursor)
        .expect("expected JAVA SOURCE statement bounds");
    let statement = &sql[bounds.0..bounds.1];

    assert!(
        statement.starts_with("CREATE OR REPLACE AND COMPILE JAVA SOURCE"),
        "JAVA SOURCE header should be included: {statement}"
    );
    assert!(
        statement.contains("return \"hello\";"),
        "JAVA body semicolon should remain inside one statement: {statement}"
    );
    assert!(
        !statement.contains("SELECT 2 FROM dual"),
        "slash-delimited trailing statement must not be merged: {statement}"
    );
}

#[test]
fn test_statement_bounds_at_cursor_external_language_clause_without_external_suffix_with_slash_splits_following_select(
) {
    let sql = r#"CREATE OR REPLACE FUNCTION ext_lang_only_slash RETURN NUMBER
AS LANGUAGE C;
/
SELECT 2 FROM dual;"#;
    let cursor = sql.rfind("SELECT 2").unwrap_or(sql.len());

    let bounds = QueryExecutor::statement_bounds_at_cursor(sql, cursor)
        .expect("expected statement bounds for trailing SELECT");
    let statement = &sql[bounds.0..bounds.1];

    assert!(
        statement.trim_start().starts_with("SELECT 2 FROM dual"),
        "cursor on trailing SELECT should resolve only SELECT statement: {statement}"
    );
}

#[test]
fn test_statement_bounds_at_cursor_external_language_clause_without_external_suffix_splits_following_select_without_slash(
) {
    let sql = r#"CREATE OR REPLACE FUNCTION ext_lang_only RETURN NUMBER
AS LANGUAGE C;
SELECT 3 FROM dual;"#;
    let cursor = sql.rfind("SELECT 3").unwrap_or(sql.len());

    let bounds = QueryExecutor::statement_bounds_at_cursor(sql, cursor)
        .expect("expected statement bounds for trailing SELECT");
    let statement = &sql[bounds.0..bounds.1];

    assert!(
        statement.trim_start().starts_with("SELECT 3 FROM dual"),
        "cursor on trailing SELECT should resolve only SELECT statement: {statement}"
    );
}

#[test]
fn test_statement_bounds_at_cursor_language_identifier_with_external_target_like_type_keeps_procedure_body(
) {
    let sql = r#"CREATE OR REPLACE PROCEDURE proc_shadow_c IS
  language c;
BEGIN
  NULL;
END;
SELECT 1 FROM dual;"#;
    let cursor = sql.find("NULL").unwrap_or(0);

    let bounds = QueryExecutor::statement_bounds_at_cursor(sql, cursor)
        .expect("expected procedure statement bounds");
    let statement = &sql[bounds.0..bounds.1];

    assert!(
        statement.starts_with("CREATE OR REPLACE PROCEDURE proc_shadow_c IS"),
        "procedure header should stay in first statement bounds: {statement}"
    );
    assert!(
        statement.contains("language c;"),
        "LANGUAGE declaration should stay inside procedure body statement: {statement}"
    );
    assert!(
        statement.contains("END"),
        "procedure body should include END token: {statement}"
    );
    assert!(
        !statement.contains("SELECT 1 FROM dual"),
        "trailing SELECT must not be merged into procedure bounds: {statement}"
    );
}

#[test]
fn test_statement_bounds_at_cursor_slash_after_end_without_semicolon_returns_previous_block() {
    let sql = "BEGIN\n  NULL;\nEND\n/\nSELECT 2 FROM dual;";
    let cursor = sql.find('/').unwrap_or(0);

    let bounds = QueryExecutor::statement_bounds_at_cursor(sql, cursor)
        .expect("expected previous PL/SQL statement bounds on slash line");
    let statement = &sql[bounds.0..bounds.1];

    assert!(
        statement.starts_with("BEGIN"),
        "slash line should resolve to preceding block statement: {statement}"
    );
    assert!(
        statement.contains("END"),
        "preceding block statement should include END token: {statement}"
    );
    assert!(
        !statement.contains("SELECT 2 FROM dual"),
        "trailing SELECT must not leak into slash-line statement bounds: {statement}"
    );
}

#[test]
fn test_normalize_sql_for_execute_trims_trailing_semicolon_for_select() {
    let normalized = QueryExecutor::normalize_sql_for_execute("  SELECT 1 FROM dual;   ");
    assert_eq!(normalized, "SELECT 1 FROM dual");
}

#[test]
fn test_normalize_sql_for_execute_keeps_plsql_block_semicolon() {
    let normalized = QueryExecutor::normalize_sql_for_execute("BEGIN NULL; END;  ");
    assert_eq!(normalized, "BEGIN NULL; END;");
}

#[test]
fn test_normalize_sql_for_execute_empty_input_stays_empty() {
    let normalized = QueryExecutor::normalize_sql_for_execute("  ;  \n\t ");
    assert!(normalized.is_empty());
}

#[test]
fn test_normalize_sql_for_execute_collapses_extra_trailing_semicolons_for_plsql() {
    let normalized = QueryExecutor::normalize_sql_for_execute("BEGIN NULL; END;;;   ");
    assert_eq!(normalized, "BEGIN NULL; END;");
}

#[test]
fn test_normalize_sql_for_execute_comment_prefixed_plsql_keeps_single_terminator() {
    let normalized =
        QueryExecutor::normalize_sql_for_execute("/*x*/ DECLARE v NUMBER:=1; BEGIN NULL; END;;; ");
    assert_eq!(normalized, "/*x*/ DECLARE v NUMBER:=1; BEGIN NULL; END;");
}

#[test]
fn test_normalize_sql_for_execute_removes_sqlplus_slash_for_single_statement() {
    let normalized = QueryExecutor::normalize_sql_for_execute("SELECT 1 FROM dual\n/\n");
    assert_eq!(normalized, "SELECT 1 FROM dual");
}

#[test]
fn test_normalize_sql_for_execute_removes_sqlplus_slash_for_plsql_block() {
    let normalized = QueryExecutor::normalize_sql_for_execute("BEGIN NULL; END;\n/\n");
    assert_eq!(normalized, "BEGIN NULL; END;");
}

#[test]
fn test_is_plain_rollback_rejects_savepoint_clause() {
    assert!(!QueryExecutor::is_plain_rollback(
        "ROLLBACK TO SAVEPOINT before_update"
    ));
}

#[test]
fn test_is_plain_commit_rejects_non_plain_commit_variants() {
    assert!(QueryExecutor::is_plain_commit("COMMIT"));
    assert!(QueryExecutor::is_plain_commit("COMMIT WORK"));
    assert!(!QueryExecutor::is_plain_commit("COMMIT FORCE 'txn-id'"));
}

#[test]
fn test_split_script_items_ignores_trailing_remark_comment_line() {
    let sql = "SELECT 1 FROM dual\nREMARK trailing note";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(stmts.len(), 1);
    assert_eq!(stmts[0], "SELECT 1 FROM dual");
}

#[test]
fn test_split_script_items_ignores_trailing_rem_comment_with_indented_comment() {
    let sql = "SELECT 1 FROM dual\n  REM indented note";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(stmts.len(), 1);
    assert_eq!(stmts[0], "SELECT 1 FROM dual");
}

#[test]
fn test_split_script_items_splits_before_inline_trailing_comment_after_semicolon() {
    let sql = "SELECT 1 FROM dual; -- trailing note\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(stmts.len(), 2, "expected two statements, got: {stmts:?}");
    assert_eq!(stmts[0], "SELECT 1 FROM dual");
    assert!(
        stmts[1].contains("SELECT 2 FROM dual"),
        "second statement should not be merged into first, got: {}",
        stmts[1]
    );
}

#[test]
fn test_normalize_sql_for_execute_keeps_division_operator() {
    let normalized = QueryExecutor::normalize_sql_for_execute("SELECT 10/2 FROM dual");
    assert_eq!(normalized, "SELECT 10/2 FROM dual");
}

#[test]
fn test_simple_select() {
    let sql = "SELECT 1 FROM DUAL;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1);
    assert!(stmts[0].contains("SELECT 1 FROM DUAL"));
}

#[test]
fn test_multiple_selects() {
    let sql = "SELECT 1 FROM DUAL;\nSELECT 2 FROM DUAL;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 2);
}

#[test]
fn test_is_select_statement_with_clause_insert_is_not_select() {
    let sql = "WITH t AS (SELECT 1 AS id FROM dual) INSERT INTO t2(id) SELECT id FROM t";
    assert!(
        !QueryExecutor::is_select_statement(sql),
        "WITH ... INSERT should not be treated as SELECT"
    );
}

#[test]
fn test_is_select_statement_with_clause_update_is_not_select() {
    let sql = "WITH t AS (SELECT 1 AS id FROM dual) UPDATE t2 SET id = (SELECT id FROM t)";
    assert!(
        !QueryExecutor::is_select_statement(sql),
        "WITH ... UPDATE should not be treated as SELECT"
    );
}

#[test]
fn test_is_select_statement_with_clause_delete_is_not_select() {
    let sql = "WITH t AS (SELECT 1 AS id FROM dual) DELETE FROM t2 WHERE id IN (SELECT id FROM t)";
    assert!(
        !QueryExecutor::is_select_statement(sql),
        "WITH ... DELETE should not be treated as SELECT"
    );
}

#[test]
fn test_is_select_statement_with_clause_select_is_select() {
    let sql = "WITH t AS (SELECT 1 AS id FROM dual) SELECT id FROM t";
    assert!(
        QueryExecutor::is_select_statement(sql),
        "WITH ... SELECT should be treated as SELECT"
    );
}

#[test]
fn test_is_select_statement_with_clause_merge_is_not_select() {
    let sql = "WITH src AS (SELECT 1 AS id FROM dual) MERGE INTO t2 d USING src s ON (d.id = s.id) WHEN MATCHED THEN UPDATE SET d.id = s.id";
    assert!(
        !QueryExecutor::is_select_statement(sql),
        "WITH ... MERGE should not be treated as SELECT"
    );
}

#[test]
fn test_is_select_statement_with_clause_ignores_comments_and_q_quotes() {
    let sql = "WITH t AS (SELECT q'[INSERT INTO t2 VALUES(1)]' AS txt FROM dual)
/* leading DML keyword in comment: DELETE */
SELECT txt FROM t";
    assert!(
        QueryExecutor::is_select_statement(sql),
        "WITH ... SELECT should remain SELECT even with DML-like text in comments/strings"
    );
}

#[test]
fn test_is_select_statement_with_clause_invalid_q_quote_delimiter_does_not_hide_select() {
    let sql = "WITH t AS (SELECT q' INSERT INTO t2 VALUES(1)' AS txt FROM dual)\nSELECT txt FROM t";
    assert!(
        QueryExecutor::is_select_statement(sql),
        "invalid q-quote delimiter should not leave parser stuck in quote mode"
    );
}

#[test]
fn test_is_select_statement_with_clause_invalid_nq_quote_delimiter_does_not_hide_select() {
    let sql = "WITH t AS (SELECT nq' UPDATE t2 SET c = 1' AS txt FROM dual)\nSELECT txt FROM t";
    assert!(
        QueryExecutor::is_select_statement(sql),
        "invalid nq-quote delimiter should not leave parser stuck in quote mode"
    );
}

#[test]
fn test_maybe_inject_rowid_for_editing_injects_for_simple_single_table_select() {
    let sql = "SELECT ENAME, JOB FROM EMP";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, "SELECT EMP.ROWID, ENAME, JOB FROM EMP");
}

#[test]
fn test_maybe_inject_rowid_for_editing_keeps_existing_rowid() {
    let sql = "SELECT ROWID, ENAME FROM EMP";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_injects_for_join_query() {
    let sql = "SELECT e.ENAME, d.DNAME FROM EMP e JOIN DEPT d ON d.DEPTNO = e.DEPTNO";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_injects_for_multi_table_from_comma_join() {
    let sql = "SELECT ENAME FROM EMP e, DEPT d WHERE e.DEPTNO = d.DEPTNO";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_allows_where_in_comma_values() {
    let sql = "SELECT ENAME FROM EMP WHERE DEPTNO IN (10, 20)";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(
        rewritten,
        "SELECT EMP.ROWID, ENAME FROM EMP WHERE DEPTNO IN (10, 20)"
    );
}

#[test]
fn test_maybe_inject_rowid_for_editing_injects_for_with_clause_referencing_base_table() {
    let sql = "WITH dept_avg AS (SELECT DEPTNO, AVG(SAL) avg_sal FROM EMP GROUP BY DEPTNO) SELECT ENAME, SAL FROM EMP e JOIN dept_avg d ON e.DEPTNO = d.DEPTNO";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_skips_with_clause_only_cte_ref() {
    // When the main SELECT FROM only references a CTE (not a base table), skip.
    let sql = "WITH e AS (SELECT ENAME FROM EMP) SELECT ENAME FROM e";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(
        rewritten,
        "WITH e AS (SELECT ENAME FROM EMP) SELECT e.ROWID, ENAME FROM e"
    );
}

#[test]
fn test_maybe_inject_rowid_for_editing_skips_distinct() {
    let sql = "SELECT DISTINCT ENAME FROM EMP";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_skips_distinct_with_comment_between_select_and_modifier() {
    let sql = "SELECT /* keep dedup */ DISTINCT ENAME FROM EMP";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_skips_unique() {
    let sql = "SELECT UNIQUE ENAME FROM EMP";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_skips_unique_with_newline_and_comment() {
    let sql = "SELECT\n-- preserve unique semantics\nUNIQUE ENAME FROM EMP";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_preserves_leading_hint_position() {
    let sql = "SELECT /*+ INDEX(emp emp_idx1) */ ENAME FROM EMP";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(
        rewritten,
        "SELECT /*+ INDEX(emp emp_idx1) */ EMP.ROWID, ENAME FROM EMP"
    );
}

#[test]
fn test_maybe_inject_rowid_for_editing_preserves_hint_before_all_modifier() {
    let sql = "SELECT /*+ FULL(emp) */ ALL ENAME FROM EMP";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(
        rewritten,
        "SELECT /*+ FULL(emp) */ ALL EMP.ROWID, ENAME FROM EMP"
    );
}

#[test]
fn test_maybe_inject_rowid_for_editing_uses_alias_when_present() {
    let sql = "SELECT ENAME FROM EMP e";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, "SELECT e.ROWID, ENAME FROM EMP e");
}

#[test]
fn test_maybe_inject_rowid_for_editing_qualifies_leading_wildcard_with_alias() {
    let sql = "SELECT * FROM EMP e";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, "SELECT e.ROWID, e.* FROM EMP e");
}

#[test]
fn test_maybe_inject_rowid_for_editing_qualifies_leading_wildcard_with_table_name() {
    let sql = "select * from help;";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, "select help.ROWID, help.* from help;");
}

#[test]
fn test_rowid_safe_execution_sql_rewrites_auto_injected_projection() {
    let source_sql = "SELECT ENAME FROM EMP e";
    let injected = QueryExecutor::maybe_inject_rowid_for_editing(source_sql);
    let safe_sql = QueryExecutor::rowid_safe_execution_sql(source_sql, &injected);
    assert_eq!(
        safe_sql,
        "SELECT ROWIDTOCHAR(e.ROWID) AS SQ_INTERNAL_ROWID, ENAME FROM EMP e"
    );
}

#[test]
fn test_rowid_safe_execution_sql_rewrites_user_rowid_query() {
    let source_sql = "SELECT ROWID, ENAME FROM EMP";
    let safe_sql = QueryExecutor::rowid_safe_execution_sql(source_sql, source_sql);
    assert_eq!(
        safe_sql,
        "SELECT ROWIDTOCHAR(ROWID) AS SQ_INTERNAL_ROWID, ENAME FROM EMP"
    );
}

#[test]
fn test_rowid_safe_execution_sql_keeps_non_rowid_internal_alias_projection() {
    let source_sql = "SELECT ENAME AS SQ_INTERNAL_ROWID, ENAME FROM EMP";
    let safe_sql = QueryExecutor::rowid_safe_execution_sql(source_sql, source_sql);
    assert_eq!(safe_sql, source_sql);
}

#[test]
fn test_rowid_safe_execution_sql_handles_order_by_wildcard_query() {
    let source_sql = "SELECT * FROM oqt_run_log ORDER BY run_ts DESC";
    let injected = QueryExecutor::maybe_inject_rowid_for_editing(source_sql);
    let safe_sql = QueryExecutor::rowid_safe_execution_sql(source_sql, &injected);
    assert_eq!(
        safe_sql,
        "SELECT ROWIDTOCHAR(oqt_run_log.ROWID) AS SQ_INTERNAL_ROWID, oqt_run_log.* FROM oqt_run_log ORDER BY run_ts DESC"
    );
}

#[test]
fn test_maybe_inject_rowid_for_editing_skips_table_collection_expression() {
    let sql = "SELECT * FROM TABLE(oqt_demo_pkg.func_pipe_rows(7000)) ORDER BY sal";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_skips_schema_qualified_relation_invocation() {
    let sql = "SELECT * FROM oqt_demo_pkg.func_pipe_rows(7000) f";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_skips_lateral_relation_invocation() {
    let sql = "SELECT * FROM LATERAL oqt_demo_pkg.func_pipe_rows(7000) f";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

// --- Multi-table / JOIN / CTE / subquery test cases ---

#[test]
fn test_maybe_inject_rowid_for_editing_left_join() {
    let sql = "SELECT e.ENAME, d.DNAME FROM EMP e LEFT JOIN DEPT d ON e.DEPTNO = d.DEPTNO";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_right_join() {
    let sql = "SELECT e.ENAME, d.DNAME FROM EMP e RIGHT JOIN DEPT d ON e.DEPTNO = d.DEPTNO";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_full_outer_join() {
    let sql = "SELECT e.ENAME, d.DNAME FROM EMP e FULL OUTER JOIN DEPT d ON e.DEPTNO = d.DEPTNO";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_cross_join() {
    let sql = "SELECT e.ENAME, d.DNAME FROM EMP e CROSS JOIN DEPT d";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_natural_join() {
    let sql = "SELECT ENAME, DNAME FROM EMP e NATURAL JOIN DEPT d";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_multiple_joins() {
    let sql = "SELECT e.ENAME, d.DNAME, s.GRADE FROM EMP e JOIN DEPT d ON e.DEPTNO = d.DEPTNO JOIN SALGRADE s ON e.SAL BETWEEN s.LOSAL AND s.HISAL";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_join_without_alias() {
    let sql = "SELECT ENAME, DNAME FROM EMP JOIN DEPT ON EMP.DEPTNO = DEPT.DEPTNO";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_join_with_schema_prefix() {
    let sql = "SELECT e.ENAME, d.DNAME FROM SCOTT.EMP e JOIN SCOTT.DEPT d ON e.DEPTNO = d.DEPTNO";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_join_with_quoted_alias() {
    let sql = r#"SELECT "e".ENAME FROM EMP "e" JOIN DEPT "d" ON "e".DEPTNO = "d".DEPTNO"#;
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_with_clause_and_join_to_base_table() {
    let sql = "WITH recent AS (SELECT DEPTNO FROM DEPT WHERE LOC = 'DALLAS') SELECT e.ENAME FROM EMP e JOIN recent r ON e.DEPTNO = r.DEPTNO";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_with_clause_single_base_table() {
    let sql = "WITH dept_info AS (SELECT DEPTNO, DNAME FROM DEPT) SELECT e.ENAME, d.DNAME FROM EMP e, dept_info d WHERE e.DEPTNO = d.DEPTNO";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_with_multiple_ctes() {
    let sql = "WITH cte1 AS (SELECT 1 AS x FROM DUAL), cte2 AS (SELECT 2 AS y FROM DUAL) SELECT ENAME FROM EMP e WHERE e.DEPTNO = 10";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(
        rewritten,
        "WITH cte1 AS (SELECT 1 AS x FROM DUAL), cte2 AS (SELECT 2 AS y FROM DUAL) SELECT e.ROWID, ENAME FROM EMP e WHERE e.DEPTNO = 10"
    );
}

#[test]
fn test_maybe_inject_rowid_for_editing_skips_union() {
    let sql = "SELECT ENAME FROM EMP UNION SELECT DNAME FROM DEPT";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_skips_union_all() {
    let sql = "SELECT ENAME FROM EMP UNION ALL SELECT DNAME FROM DEPT";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_skips_intersect() {
    let sql = "SELECT DEPTNO FROM EMP INTERSECT SELECT DEPTNO FROM DEPT";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_skips_minus() {
    let sql = "SELECT DEPTNO FROM EMP MINUS SELECT DEPTNO FROM DEPT";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_skips_group_by() {
    let sql = "SELECT DEPTNO, COUNT(*) FROM EMP GROUP BY DEPTNO";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_skips_connect_by() {
    let sql =
        "SELECT EMPNO, MGR, LEVEL FROM EMP CONNECT BY PRIOR EMPNO = MGR START WITH MGR IS NULL";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_skips_analytic_over_clause() {
    let sql = "SELECT ENAME, ROW_NUMBER() OVER (PARTITION BY DEPTNO ORDER BY SAL DESC) RN FROM EMP";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_skips_pivot_clause() {
    let sql =
        "SELECT * FROM (SELECT JOB, DEPTNO, SAL FROM EMP) PIVOT (SUM(SAL) FOR DEPTNO IN (10, 20))";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_skips_model_clause() {
    let sql = "SELECT ENAME, DEPTNO, SAL FROM EMP MODEL RETURN UPDATED ROWS DIMENSION BY (DEPTNO) MEASURES (SAL) RULES (SAL[10] = 0)";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_skips_match_recognize_clause() {
    let sql = r#"SELECT *
FROM oqt_t_emp
MATCH_RECOGNIZE (
  PARTITION BY deptno
  ORDER BY hiredate, empno
  MEASURES
    FIRST(ename) AS start_name,
    LAST(ename)  AS end_name,
    COUNT(*)     AS run_len
  ONE ROW PER MATCH
  PATTERN (a b+)
  DEFINE
    b AS b.sal > PREV(b.sal)
)"#;
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_allows_subquery_in_where() {
    let sql =
        "SELECT ENAME FROM EMP e WHERE DEPTNO IN (SELECT DEPTNO FROM DEPT WHERE LOC = 'DALLAS')";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(
        rewritten,
        "SELECT e.ROWID, ENAME FROM EMP e WHERE DEPTNO IN (SELECT DEPTNO FROM DEPT WHERE LOC = 'DALLAS')"
    );
}

#[test]
fn test_maybe_inject_rowid_for_editing_allows_correlated_subquery() {
    let sql = "SELECT ENAME, SAL FROM EMP e WHERE SAL > (SELECT AVG(SAL) FROM EMP WHERE DEPTNO = e.DEPTNO)";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(
        rewritten,
        "SELECT e.ROWID, ENAME, SAL FROM EMP e WHERE SAL > (SELECT AVG(SAL) FROM EMP WHERE DEPTNO = e.DEPTNO)"
    );
}

#[test]
fn test_maybe_inject_rowid_for_editing_allows_exists_subquery() {
    let sql =
        "SELECT ENAME FROM EMP e WHERE EXISTS (SELECT 1 FROM DEPT d WHERE d.DEPTNO = e.DEPTNO)";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(
        rewritten,
        "SELECT e.ROWID, ENAME FROM EMP e WHERE EXISTS (SELECT 1 FROM DEPT d WHERE d.DEPTNO = e.DEPTNO)"
    );
}

#[test]
fn test_maybe_inject_rowid_for_editing_allows_scalar_subquery_in_select() {
    let sql = "SELECT ENAME, (SELECT DNAME FROM DEPT d WHERE d.DEPTNO = e.DEPTNO) AS DEPT_NAME FROM EMP e";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(
        rewritten,
        "SELECT e.ROWID, ENAME, (SELECT DNAME FROM DEPT d WHERE d.DEPTNO = e.DEPTNO) AS DEPT_NAME FROM EMP e"
    );
}

#[test]
fn test_maybe_inject_rowid_for_editing_skips_from_subquery_as_source() {
    // When FROM clause starts with a subquery, ROWID is not available
    let sql = "SELECT x.ENAME FROM (SELECT ENAME FROM EMP) x";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_join_with_as_alias() {
    let sql = "SELECT e.ENAME FROM EMP AS e JOIN DEPT AS d ON e.DEPTNO = d.DEPTNO";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_join_with_where_clause() {
    let sql =
        "SELECT e.ENAME, d.DNAME FROM EMP e JOIN DEPT d ON e.DEPTNO = d.DEPTNO WHERE e.SAL > 1000";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_join_wildcard_qualifies_first_table() {
    let sql = "SELECT * FROM EMP e JOIN DEPT d ON e.DEPTNO = d.DEPTNO";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_keeps_existing_qualified_rowid() {
    let sql = "SELECT e.ROWID, e.ENAME FROM EMP e JOIN DEPT d ON e.DEPTNO = d.DEPTNO";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, sql);
}

#[test]
fn test_maybe_inject_rowid_for_editing_with_recursive_cte() {
    // Recursive CTE with base table in main SELECT
    let sql = "WITH RECURSIVE mgr_chain AS (SELECT EMPNO, ENAME, MGR FROM EMP WHERE EMPNO = 7369 UNION ALL SELECT e.EMPNO, e.ENAME, e.MGR FROM EMP e JOIN mgr_chain m ON e.EMPNO = m.MGR) SELECT ENAME FROM mgr_chain";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    // Main SELECT is from CTE only, UNION inside CTE is inside parens (not top-level)
    assert_eq!(
        rewritten,
        "WITH RECURSIVE mgr_chain AS (SELECT EMPNO, ENAME, MGR FROM EMP WHERE EMPNO = 7369 UNION ALL SELECT e.EMPNO, e.ENAME, e.MGR FROM EMP e JOIN mgr_chain m ON e.EMPNO = m.MGR) SELECT mgr_chain.ROWID, ENAME FROM mgr_chain"
    );
}

#[test]
fn test_maybe_inject_rowid_for_editing_with_quoted_table_name() {
    let sql = r#"SELECT "Employee Name" FROM "My Table" t"#;
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(
        rewritten,
        r#"SELECT t.ROWID, "Employee Name" FROM "My Table" t"#
    );
}

#[test]
fn test_maybe_inject_rowid_for_editing_with_schema_no_alias() {
    let sql = "SELECT ENAME FROM HR.EMPLOYEES";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(
        rewritten,
        "SELECT HR.EMPLOYEES.ROWID, ENAME FROM HR.EMPLOYEES"
    );
}

#[test]
fn test_maybe_inject_rowid_for_editing_allows_having_without_group_by_keyword() {
    // HAVING without GROUP BY is unusual but valid in some dialects
    let sql = "SELECT ENAME FROM EMP e WHERE SAL > 1000 ORDER BY ENAME";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(
        rewritten,
        "SELECT e.ROWID, ENAME FROM EMP e WHERE SAL > 1000 ORDER BY ENAME"
    );
}

#[test]
fn test_maybe_inject_rowid_for_editing_group_by_in_subquery_not_top_level() {
    // GROUP BY inside subquery should NOT block injection (only top-level GROUP matters)
    let sql = "SELECT e.ENAME FROM EMP e WHERE e.DEPTNO IN (SELECT DEPTNO FROM EMP GROUP BY DEPTNO HAVING COUNT(*) > 3)";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(
        rewritten,
        "SELECT e.ROWID, e.ENAME FROM EMP e WHERE e.DEPTNO IN (SELECT DEPTNO FROM EMP GROUP BY DEPTNO HAVING COUNT(*) > 3)"
    );
}

#[test]
fn test_maybe_inject_rowid_for_editing_union_in_subquery_not_top_level() {
    // UNION inside a subquery should NOT block injection
    let sql = "SELECT e.ENAME FROM EMP e WHERE e.DEPTNO IN (SELECT DEPTNO FROM DEPT UNION SELECT DEPTNO FROM EMP)";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(
        rewritten,
        "SELECT e.ROWID, e.ENAME FROM EMP e WHERE e.DEPTNO IN (SELECT DEPTNO FROM DEPT UNION SELECT DEPTNO FROM EMP)"
    );
}

// --- Bug regression tests: identifier boundary / quoted alias ---

#[test]
fn test_maybe_inject_rowid_for_editing_start_date_column_not_blocked() {
    // Bug fix: START_DATE column must NOT be mistaken for "START WITH" hierarchical keyword
    let sql = "SELECT e.ENAME, e.START_DATE FROM EMP e";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(
        rewritten,
        "SELECT e.ROWID, e.ENAME, e.START_DATE FROM EMP e"
    );
}

#[test]
fn test_maybe_inject_rowid_for_editing_group_id_column_not_blocked() {
    // Bug fix: GROUP_ID column must NOT be mistaken for "GROUP BY" keyword
    let sql = "SELECT GROUP_ID, ENAME FROM EMP e";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, "SELECT e.ROWID, GROUP_ID, ENAME FROM EMP e");
}

#[test]
fn test_maybe_inject_rowid_for_editing_connect_string_column_not_blocked() {
    // CONNECT_STRING column must NOT be mistaken for "CONNECT BY"
    let sql = "SELECT CONNECT_STRING FROM CONFIG c";
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, "SELECT c.ROWID, CONNECT_STRING FROM CONFIG c");
}

#[test]
fn test_maybe_inject_rowid_for_editing_quoted_keyword_alias() {
    // Bug fix: quoted alias matching a keyword (e.g. "WHERE") must NOT be rejected
    let sql = r#"SELECT ENAME FROM EMP "where""#;
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, r#"SELECT "where".ROWID, ENAME FROM EMP "where""#);
}

#[test]
fn test_maybe_inject_rowid_for_editing_quoted_join_alias() {
    // Quoted alias "join" must be accepted as an alias
    let sql = r#"SELECT ENAME FROM EMP "join""#;
    let rewritten = QueryExecutor::maybe_inject_rowid_for_editing(sql);
    assert_eq!(rewritten, r#"SELECT "join".ROWID, ENAME FROM EMP "join""#);
}

#[test]
fn test_retryable_rowid_injection_error_detects_non_key_preserved_table() {
    let message =
        "ORA-01445: cannot select ROWID from, or sample, a join view without a key-preserved table";
    assert!(QueryExecutor::is_retryable_rowid_injection_error(message));
}

#[test]
fn test_retryable_rowid_injection_error_detects_rowid_illegal_here() {
    let message =
        "ORA-01446: cannot select ROWID from, or sample, a view with DISTINCT, GROUP BY, etc.";
    assert!(QueryExecutor::is_retryable_rowid_injection_error(message));
}

#[test]
fn test_retryable_rowid_injection_error_detects_invalid_identifier_rowid() {
    let message = "ORA-00904: \"ROWID\": invalid identifier";
    assert!(QueryExecutor::is_retryable_rowid_injection_error(message));
}

#[test]
fn test_retryable_rowid_injection_error_ignores_other_oracle_errors() {
    let message = "ORA-00942: table or view does not exist";
    assert!(!QueryExecutor::is_retryable_rowid_injection_error(message));
}

#[test]
fn test_is_plain_commit_allows_only_commit_variants() {
    assert!(QueryExecutor::is_plain_commit("COMMIT"));
    assert!(QueryExecutor::is_plain_commit("commit work;"));
    assert!(QueryExecutor::is_plain_commit("COMMIT /* trailing */"));
    assert!(QueryExecutor::is_plain_commit(
        "COMMIT -- trailing comment\n"
    ));
    assert!(!QueryExecutor::is_plain_commit("COMMIT FORCE '1.2.3'"));
    assert!(!QueryExecutor::is_plain_commit("COMMIT COMMENT 'done'"));
}

#[test]
fn test_is_plain_rollback_allows_only_rollback_variants() {
    assert!(QueryExecutor::is_plain_rollback("ROLLBACK"));
    assert!(QueryExecutor::is_plain_rollback("rollback work;"));
    assert!(QueryExecutor::is_plain_rollback("ROLLBACK /* trailing */"));
    assert!(QueryExecutor::is_plain_rollback(
        "ROLLBACK -- trailing comment\n"
    ));
    assert!(!QueryExecutor::is_plain_rollback("ROLLBACK TO sp1"));
    assert!(!QueryExecutor::is_plain_rollback("ROLLBACK FORCE '1.2.3'"));
}

#[test]
fn test_double_semicolon() {
    let sql = "SELECT 1 FROM DUAL;;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
    assert!(
        !stmts[0].ends_with(";;"),
        "Should not end with ;;: {}",
        stmts[0]
    );
}

#[test]
fn test_anonymous_block() {
    let sql = "DECLARE x NUMBER; BEGIN x := 1; END;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_create_procedure_simple() {
    let sql = "CREATE PROCEDURE test_proc AS BEGIN NULL; END;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
    assert!(stmts[0].contains("CREATE PROCEDURE"));
    assert!(stmts[0].contains("END"));
}

#[test]
fn test_create_procedure_with_declare() {
    let sql = r#"CREATE PROCEDURE test_proc AS
DECLARE
  v_num NUMBER;
BEGIN
  v_num := 1;
END;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_create_or_replace_procedure() {
    let sql = r#"CREATE OR REPLACE PROCEDURE test_proc IS
BEGIN
  DBMS_OUTPUT.PUT_LINE('Hello');
END;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_create_or_replace_force_procedure() {
    let sql = r#"CREATE OR REPLACE FORCE PROCEDURE test_proc IS
BEGIN
  DBMS_OUTPUT.PUT_LINE('Hello');
END;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
    assert!(stmts[0].contains("CREATE OR REPLACE FORCE PROCEDURE"));
}

#[test]
fn test_normalize_exec_call_handles_leading_comments() {
    let sql = "-- run proc\nEXEC test_proc(:v1);";
    let normalized = QueryExecutor::normalize_exec_call(sql);
    assert_eq!(normalized.as_deref(), Some("BEGIN test_proc(:v1); END;"));
}

#[test]
fn test_normalize_exec_call_handles_leading_whitespace() {
    let sql = "  \n\tEXEC test_proc(:v1);";
    let normalized = QueryExecutor::normalize_exec_call(sql);
    assert_eq!(normalized.as_deref(), Some("BEGIN test_proc(:v1); END;"));
}

#[test]
fn test_check_named_positional_mix_handles_leading_whitespace() {
    let sql = "\n  EXEC test_proc(p_id => 1, 2);";
    assert!(QueryExecutor::check_named_positional_mix(sql).is_err());
}

#[test]
fn test_check_named_positional_mix_ignores_line_comment_arrow() {
    let sql = "EXEC test_proc(1, -- p_id => 1\n 2);";
    assert!(QueryExecutor::check_named_positional_mix(sql).is_ok());
}

#[test]
fn test_check_named_positional_mix_ignores_block_comment_arrow() {
    let sql = "EXEC test_proc(1, /* p_id => 1 */ 2);";
    assert!(QueryExecutor::check_named_positional_mix(sql).is_ok());
}

#[test]
fn test_check_named_positional_mix_call_is_not_validated_as_exec() {
    let sql = "CALL test_proc(p_id => 1, 2)";
    assert!(QueryExecutor::check_named_positional_mix(sql).is_ok());
}

#[test]
fn test_create_external_function_as_non_plsql_block_followed_by_select() {
    let sql = r#"CREATE OR REPLACE FUNCTION ext_fn(
  p_num NUMBER
) RETURN NUMBER
IS
EXTERNAL
  LANGUAGE C
  NAME 'ExtNativeFunction';
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(
        stmts.len(),
        2,
        "External function declaration should split before following SELECT, got: {:?}",
        stmts
    );
    assert!(stmts[0].contains("CREATE OR REPLACE FUNCTION"));
    assert!(stmts[1].contains("SELECT 1 FROM"));
}

#[test]
fn test_package_body_nested_external_procedure_followed_by_select_splits() {
    let sql = r#"CREATE OR REPLACE PACKAGE BODY pkg_ext AS
  PROCEDURE ext_proc IS
    EXTERNAL NAME "ext_proc" LANGUAGE C;
END pkg_ext;
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "Package body with nested EXTERNAL procedure should split before trailing SELECT, got: {:?}",
        stmts
    );
    assert!(
        stmts[0].starts_with("CREATE OR REPLACE PACKAGE BODY pkg_ext AS"),
        "first statement should keep full package body: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_type_body_nested_external_member_function_followed_by_select_splits() {
    let sql = r#"CREATE OR REPLACE TYPE BODY oqt_obj AS
  MEMBER FUNCTION ext_fn RETURN NUMBER
  AS EXTERNAL
  NAME 'ExtFn'
  LANGUAGE C;
END oqt_obj;
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "TYPE BODY with EXTERNAL member function should split before trailing SELECT, got: {:?}",
        stmts
    );
    assert!(
        stmts[0].starts_with("CREATE OR REPLACE TYPE BODY oqt_obj AS"),
        "first statement should keep full TYPE BODY: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("MEMBER FUNCTION ext_fn RETURN NUMBER"),
        "TYPE BODY should include external member function: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("SELECT 1 FROM dual"),
        "trailing SELECT should remain separate statement"
    );
}

#[test]
fn test_type_body_nested_external_member_procedure_followed_by_select_splits() {
    let sql = r#"CREATE OR REPLACE TYPE BODY oqt_obj AS
  MEMBER PROCEDURE ext_proc (p_id NUMBER)
  AS EXTERNAL
  NAME 'ExtProc'
  LANGUAGE C;
END oqt_obj;
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "TYPE BODY with EXTERNAL member procedure should split before trailing SELECT, got: {:?}",
        stmts
    );
    assert!(
        stmts[0].starts_with("CREATE OR REPLACE TYPE BODY oqt_obj AS"),
        "first statement should keep full TYPE BODY: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("MEMBER PROCEDURE ext_proc"),
        "TYPE BODY should include external member procedure: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("SELECT 1 FROM dual"),
        "trailing SELECT should remain separate statement"
    );
}

#[test]
fn test_type_body_local_table_type_declaration_followed_by_select_splits() {
    let sql = r#"CREATE OR REPLACE TYPE BODY t_local_types AS
  MEMBER PROCEDURE p IS
    TYPE num_tab IS TABLE OF NUMBER;
  BEGIN
    NULL;
  END;
END t_local_types;
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "TYPE BODY with local TABLE type declaration should split before trailing SELECT, got: {:?}",
        stmts
    );
    assert!(
        stmts[0].starts_with("CREATE OR REPLACE TYPE BODY t_local_types AS"),
        "first statement should keep full TYPE BODY: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("TYPE num_tab IS TABLE OF NUMBER;"),
        "TYPE BODY should keep local TABLE type declaration: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("SELECT 1 FROM dual"),
        "trailing SELECT should remain separate statement"
    );
}

#[test]
fn test_split_format_items_type_body_local_ref_cursor_type_declaration_splits() {
    let sql = r#"CREATE OR REPLACE TYPE BODY t_local_ref AS
  MEMBER PROCEDURE p IS
    TYPE rc_t IS REF CURSOR;
  BEGIN
    NULL;
  END;
END t_local_ref;
SELECT 2 FROM dual;"#;
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        2,
        "split_format_items should keep TYPE BODY with local REF CURSOR type as one statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("CREATE OR REPLACE TYPE BODY t_local_ref AS"),
        "first formatted statement should keep full TYPE BODY: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("TYPE rc_t IS REF CURSOR;"),
        "first formatted statement should keep local REF CURSOR type declaration: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("SELECT 2 FROM dual"),
        "trailing SELECT should remain separate formatted statement: {}",
        stmts[1]
    );
}

#[test]
fn test_procedure_name_language_library_identifiers_do_not_trigger_external_split() {
    let sql = r#"CREATE OR REPLACE PROCEDURE proc_shadow IS
  name NUMBER := 1;
  language NUMBER := 2;
  library NUMBER := 3;
BEGIN
  NULL;
END;
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "identifier tokens NAME/LANGUAGE/LIBRARY must not trigger EXTERNAL split, got: {:?}",
        stmts
    );
    assert!(
        stmts[0].starts_with("CREATE OR REPLACE PROCEDURE proc_shadow IS"),
        "first statement should keep full procedure body: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("name NUMBER := 1;")
            && stmts[0].contains("language NUMBER := 2;")
            && stmts[0].contains("library NUMBER := 3;")
            && stmts[0].contains("END"),
        "procedure body should preserve NAME/LANGUAGE/LIBRARY declarations: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_split_format_items_name_language_library_identifiers_do_not_trigger_external_split() {
    let sql = r#"CREATE OR REPLACE PROCEDURE proc_shadow IS
  name NUMBER := 1;
  language NUMBER := 2;
  library NUMBER := 3;
BEGIN
  NULL;
END;
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<String> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(s) => Some(s.clone()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        2,
        "split_format_items must keep NAME/LANGUAGE/LIBRARY identifiers inside routine body: {:?}",
        stmts
    );
    assert!(stmts[0].starts_with("CREATE OR REPLACE PROCEDURE proc_shadow IS"));
    assert!(stmts[0].contains("name NUMBER := 1;"));
    assert!(stmts[0].contains("language NUMBER := 2;"));
    assert!(stmts[0].contains("library NUMBER := 3;"));
    assert!(stmts[0].contains("END"));
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_procedure_language_identifier_with_language_targets_do_not_trigger_external_split() {
    let sql = r#"CREATE OR REPLACE PROCEDURE proc_shadow_targets IS
  language c;
  language java;
  language javascript;
  language python;
  marker NUMBER := 1;
BEGIN
  NULL;
END;
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "LANGUAGE + external-target-like datatype declarations must not trigger EXTERNAL split, got: {:?}",
        stmts
    );
    assert!(stmts[0].starts_with("CREATE OR REPLACE PROCEDURE proc_shadow_targets IS"));
    assert!(stmts[0].contains("language c;"));
    assert!(stmts[0].contains("language java;"));
    assert!(stmts[0].contains("language javascript;"));
    assert!(stmts[0].contains("language python;"));
    assert!(stmts[0].contains("marker NUMBER := 1;"));
    assert!(stmts[0].contains("END"));
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_split_format_items_language_identifier_with_language_targets_do_not_trigger_external_split()
{
    let sql = r#"CREATE OR REPLACE PROCEDURE proc_shadow_targets IS
  language c;
  language java;
  language javascript;
  language python;
  marker NUMBER := 1;
BEGIN
  NULL;
END;
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<String> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(s) => Some(s.clone()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        2,
        "split_format_items must keep LANGUAGE + external-target-like datatype declarations inside routine body: {:?}",
        stmts
    );
    assert!(stmts[0].starts_with("CREATE OR REPLACE PROCEDURE proc_shadow_targets IS"));
    assert!(stmts[0].contains("language c;"));
    assert!(stmts[0].contains("language java;"));
    assert!(stmts[0].contains("language javascript;"));
    assert!(stmts[0].contains("language python;"));
    assert!(stmts[0].contains("marker NUMBER := 1;"));
    assert!(stmts[0].contains("END"));
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_procedure_language_assignment_does_not_trigger_external_split() {
    let sql = r#"CREATE OR REPLACE PROCEDURE proc_assign IS
  language := 'C';
BEGIN
  NULL;
END;
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "LANGUAGE assignment must not trigger EXTERNAL split, got: {:?}",
        stmts
    );
    assert!(stmts[0].starts_with("CREATE OR REPLACE PROCEDURE proc_assign IS"));
    assert!(stmts[0].contains("language := 'C';"));
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_split_format_items_language_assignment_does_not_trigger_external_split() {
    let sql = r#"CREATE OR REPLACE PROCEDURE proc_assign IS
  language := 'C';
BEGIN
  NULL;
END;
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<String> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(s) => Some(s.clone()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        2,
        "split_format_items must keep LANGUAGE assignment inside routine body: {:?}",
        stmts
    );
    assert!(stmts[0].starts_with("CREATE OR REPLACE PROCEDURE proc_assign IS"));
    assert!(stmts[0].contains("language := 'C';"));
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_create_external_function_language_clause_without_external_keyword_splits() {
    let sql = r#"CREATE OR REPLACE FUNCTION ext_lang_only RETURN NUMBER
AS LANGUAGE C NAME 'ext_lang_only';
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "LANGUAGE call spec without EXTERNAL keyword should split before trailing SELECT, got: {:?}",
        stmts
    );
    assert!(
        stmts[0].starts_with("CREATE OR REPLACE FUNCTION ext_lang_only RETURN NUMBER"),
        "first statement should keep external call spec function: {}",
        stmts[0]
    );
    assert!(stmts[0].contains("AS LANGUAGE C NAME 'ext_lang_only'"));
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_split_format_items_external_language_clause_without_external_keyword_splits() {
    let sql = r#"CREATE OR REPLACE FUNCTION ext_lang_only RETURN NUMBER
AS LANGUAGE C NAME 'ext_lang_only';
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<String> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(s) => Some(s.clone()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        2,
        "split_format_items should keep LANGUAGE call spec function together and split trailing SELECT: {:?}",
        stmts
    );
    assert!(stmts[0].starts_with("CREATE OR REPLACE FUNCTION ext_lang_only RETURN NUMBER"));
    assert!(stmts[0].contains("AS LANGUAGE C NAME 'ext_lang_only'"));
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_create_external_function_language_clause_without_external_suffix_still_splits() {
    let sql = r#"CREATE OR REPLACE FUNCTION ext_lang_only RETURN NUMBER
AS LANGUAGE C;
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "LANGUAGE target-only call spec should split before trailing SELECT, got: {:?}",
        stmts
    );
    assert!(stmts[0].starts_with("CREATE OR REPLACE FUNCTION ext_lang_only RETURN NUMBER"));
    assert!(stmts[0].contains("AS LANGUAGE C"));
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_create_external_function_language_clause_without_external_suffix_with_slash_still_splits() {
    let sql = r#"CREATE OR REPLACE FUNCTION ext_lang_only_slash RETURN NUMBER
AS LANGUAGE C;
/
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "LANGUAGE target-only call spec with slash delimiter should split before trailing SELECT, got: {:?}",
        stmts
    );
    assert!(stmts[0].starts_with("CREATE OR REPLACE FUNCTION ext_lang_only_slash RETURN NUMBER"));
    assert!(stmts[0].contains("AS LANGUAGE C"));
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_split_format_items_external_language_clause_without_external_suffix_with_slash_still_splits(
) {
    let sql = r#"CREATE OR REPLACE FUNCTION ext_lang_only_slash RETURN NUMBER
AS LANGUAGE C;
/
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<String> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    let slash_count = items
        .iter()
        .filter(|item| matches!(item, FormatItem::Slash))
        .count();

    assert_eq!(
        stmts.len(),
        2,
        "split_format_items should keep LANGUAGE target-only call spec with slash and split trailing SELECT: {:?}",
        stmts
    );
    assert_eq!(
        slash_count, 1,
        "expected one slash terminator, got: {items:?}"
    );
    assert!(stmts[0].starts_with("CREATE OR REPLACE FUNCTION ext_lang_only_slash RETURN NUMBER"));
    assert!(stmts[0].contains("AS LANGUAGE C"));
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_create_external_function_external_clause_without_suffix_still_splits() {
    let sql = r#"CREATE OR REPLACE FUNCTION ext_external_only RETURN NUMBER
AS EXTERNAL;
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "EXTERNAL-only call spec should split before trailing SELECT, got: {:?}",
        stmts
    );
    assert!(stmts[0].starts_with("CREATE OR REPLACE FUNCTION ext_external_only RETURN NUMBER"));
    assert!(stmts[0].contains("AS EXTERNAL"));
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_create_external_function_language_parameters_without_external_keyword_splits() {
    let sql = r#"CREATE OR REPLACE FUNCTION ext_lang_params RETURN NUMBER
AS LANGUAGE C PARAMETERS (CONTEXT);
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "LANGUAGE ... PARAMETERS call spec without EXTERNAL keyword should split before trailing SELECT, got: {:?}",
        stmts
    );
    assert!(stmts[0].starts_with("CREATE OR REPLACE FUNCTION ext_lang_params RETURN NUMBER"));
    assert!(stmts[0].contains("AS LANGUAGE C PARAMETERS (CONTEXT)"));
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_create_external_function_language_calling_standard_without_external_keyword_splits() {
    let sql = r#"CREATE OR REPLACE FUNCTION ext_lang_calling RETURN NUMBER
AS LANGUAGE C CALLING STANDARD;
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "LANGUAGE ... CALLING STANDARD without EXTERNAL keyword should split before trailing SELECT, got: {:?}",
        stmts
    );
    assert!(stmts[0].starts_with("CREATE OR REPLACE FUNCTION ext_lang_calling RETURN NUMBER"));
    assert!(stmts[0].contains("AS LANGUAGE C CALLING STANDARD"));
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_split_format_items_external_language_calling_standard_without_external_keyword_splits() {
    let sql = r#"CREATE OR REPLACE FUNCTION ext_lang_calling RETURN NUMBER
AS LANGUAGE C CALLING STANDARD;
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<String> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(s) => Some(s.clone()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        2,
        "split_format_items should keep LANGUAGE ... CALLING STANDARD function together and split trailing SELECT: {:?}",
        stmts
    );
    assert!(stmts[0].starts_with("CREATE OR REPLACE FUNCTION ext_lang_calling RETURN NUMBER"));
    assert!(stmts[0].contains("AS LANGUAGE C CALLING STANDARD"));
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_create_external_function_language_with_context_without_external_keyword_splits() {
    let sql = r#"CREATE OR REPLACE FUNCTION ext_lang_context RETURN NUMBER
AS LANGUAGE C WITH CONTEXT;
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "LANGUAGE ... WITH CONTEXT without EXTERNAL keyword should split before trailing SELECT, got: {:?}",
        stmts
    );
    assert!(stmts[0].starts_with("CREATE OR REPLACE FUNCTION ext_lang_context RETURN NUMBER"));
    assert!(stmts[0].contains("AS LANGUAGE C WITH CONTEXT"));
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_split_format_items_external_language_with_context_without_external_keyword_splits() {
    let sql = r#"CREATE OR REPLACE FUNCTION ext_lang_context RETURN NUMBER
AS LANGUAGE C WITH CONTEXT;
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<String> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(s) => Some(s.clone()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        2,
        "split_format_items should keep LANGUAGE ... WITH CONTEXT function together and split trailing SELECT: {:?}",
        stmts
    );
    assert!(stmts[0].starts_with("CREATE OR REPLACE FUNCTION ext_lang_context RETURN NUMBER"));
    assert!(stmts[0].contains("AS LANGUAGE C WITH CONTEXT"));
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_split_format_items_external_language_parameters_without_external_keyword_splits() {
    let sql = r#"CREATE OR REPLACE FUNCTION ext_lang_params RETURN NUMBER
AS LANGUAGE C PARAMETERS (CONTEXT);
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<String> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(s) => Some(s.clone()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        2,
        "split_format_items should keep LANGUAGE ... PARAMETERS call spec function together and split trailing SELECT: {:?}",
        stmts
    );
    assert!(stmts[0].starts_with("CREATE OR REPLACE FUNCTION ext_lang_params RETURN NUMBER"));
    assert!(stmts[0].contains("AS LANGUAGE C PARAMETERS (CONTEXT)"));
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_create_external_function_language_agent_in_without_external_keyword_splits() {
    let sql = r#"CREATE OR REPLACE FUNCTION ext_lang_agent RETURN NUMBER
AS LANGUAGE C AGENT IN extproc_agent;
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "LANGUAGE ... AGENT IN without EXTERNAL keyword should split before trailing SELECT, got: {:?}",
        stmts
    );
    assert!(stmts[0].starts_with("CREATE OR REPLACE FUNCTION ext_lang_agent RETURN NUMBER"));
    assert!(stmts[0].contains("AS LANGUAGE C AGENT IN extproc_agent"));
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_split_format_items_external_language_agent_in_without_external_keyword_splits() {
    let sql = r#"CREATE OR REPLACE FUNCTION ext_lang_agent RETURN NUMBER
AS LANGUAGE C AGENT IN extproc_agent;
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<String> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(s) => Some(s.clone()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        2,
        "split_format_items should keep LANGUAGE ... AGENT IN function together and split trailing SELECT: {:?}",
        stmts
    );
    assert!(stmts[0].starts_with("CREATE OR REPLACE FUNCTION ext_lang_agent RETURN NUMBER"));
    assert!(stmts[0].contains("AS LANGUAGE C AGENT IN extproc_agent"));
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_create_function() {
    let sql = r#"CREATE FUNCTION add_nums(a NUMBER, b NUMBER) RETURN NUMBER IS
BEGIN
  RETURN a + b;
END;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_create_package_spec() {
    let sql = r#"CREATE PACKAGE test_pkg AS
  PROCEDURE proc1;
  FUNCTION func1 RETURN NUMBER;
END test_pkg;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
    assert!(stmts[0].contains("CREATE PACKAGE"));
    assert!(stmts[0].contains("END test_pkg"));
}

#[test]
fn test_package_spec_forward_declaration_followed_by_subtype_splits_before_next_statement() {
    let sql = r#"CREATE OR REPLACE PACKAGE test_pkg AS
  PROCEDURE proc1;
  SUBTYPE vc30 IS VARCHAR2(30);
END test_pkg;

SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "package spec with forward declaration + SUBTYPE should split before trailing SELECT: {:?}",
        stmts
    );
    assert!(
        stmts[0].starts_with("CREATE OR REPLACE PACKAGE test_pkg AS"),
        "first statement should preserve package spec: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("SUBTYPE vc30 IS VARCHAR2(30);"),
        "first statement should preserve SUBTYPE declaration: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_split_format_items_package_spec_forward_declaration_followed_by_subtype_splits_before_next_statement(
) {
    let sql = r#"CREATE OR REPLACE PACKAGE test_pkg AS
  PROCEDURE proc1;
  SUBTYPE vc30 IS VARCHAR2(30);
END test_pkg;

SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        2,
        "split_format_items should split package spec and trailing SELECT separately: {:?}",
        stmts
    );
    assert!(stmts[0].starts_with("CREATE OR REPLACE PACKAGE test_pkg AS"));
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_create_package_body_simple() {
    let sql = r#"CREATE PACKAGE BODY test_pkg AS
  PROCEDURE proc1 IS
  BEGIN
NULL;
  END;
END test_pkg;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_create_package_body_complex() {
    let sql = r#"CREATE OR REPLACE PACKAGE BODY oqt_pkg AS
  PROCEDURE log_msg(p_tag IN VARCHAR2, p_msg IN VARCHAR2, p_n1 IN NUMBER DEFAULT NULL) IS
  BEGIN
INSERT INTO oqt_call_log(id, tag, msg, n1, created_at)
VALUES (oqt_call_log_seq.NEXTVAL, p_tag, p_msg, p_n1, SYSDATE);
  END;

  PROCEDURE p_basic(
p_in_num   IN  NUMBER,
p_in_txt   IN  VARCHAR2 DEFAULT 'DEF',
p_out_txt  OUT VARCHAR2,
p_inout_n  IN OUT NUMBER
  ) IS
  BEGIN
p_out_txt := 'IN_NUM='||p_in_num||', IN_TXT='||p_in_txt||', INOUT='||p_inout_n;
p_inout_n := NVL(p_inout_n,0) + p_in_num;

log_msg('P_BASIC', p_out_txt, p_in_num);
DBMS_OUTPUT.PUT_LINE('[p_basic] out='||p_out_txt||' / inout='||p_inout_n);
  END;

  PROCEDURE p_over(p_txt IN VARCHAR2) IS
  BEGIN
log_msg('P_OVER1', p_txt);
DBMS_OUTPUT.PUT_LINE('[p_over(txt)] '||NVL(p_txt,'<NULL>'));
  END;

  PROCEDURE p_over(p_num IN NUMBER, p_txt IN VARCHAR2) IS
  BEGIN
log_msg('P_OVER2', p_txt, p_num);
DBMS_OUTPUT.PUT_LINE('[p_over(num,txt)] '||p_num||' / '||NVL(p_txt,'<NULL>'));
  END;

  PROCEDURE p_refcur(p_tag IN VARCHAR2, p_rc OUT SYS_REFCURSOR) IS
  BEGIN
OPEN p_rc FOR
  SELECT id, tag, msg, n1, created_at
  FROM oqt_call_log
  WHERE tag LIKE p_tag||'%'
  ORDER BY id DESC;
  END;

  PROCEDURE p_raise(p_mode IN VARCHAR2) IS
  BEGIN
IF p_mode = 'NO_DATA_FOUND' THEN
  DECLARE v NUMBER;
  BEGIN
    SELECT n1 INTO v FROM oqt_call_log WHERE id = -9999;
  END;
ELSIF p_mode = 'APP' THEN
  RAISE_APPLICATION_ERROR(-20001, 'oqt_pkg.p_raise app error');
ELSE
  DBMS_OUTPUT.PUT_LINE('[p_raise] ok');
END IF;
  END;

  FUNCTION f_sum(p_a IN NUMBER, p_b IN NUMBER) RETURN NUMBER IS
  BEGIN
RETURN NVL(p_a,0) + NVL(p_b,0);
  END;

  FUNCTION f_echo(p_txt IN VARCHAR2) RETURN VARCHAR2 IS
  BEGIN
RETURN 'ECHO:'||p_txt;
  END;

  FUNCTION f_dateadd(p_d IN DATE, p_days IN NUMBER DEFAULT 1) RETURN DATE IS
  BEGIN
RETURN p_d + p_days;
  END;
END oqt_pkg;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(
        stmts.len(),
        1,
        "Should have 1 statement, got {} statements",
        stmts.len()
    );
    if stmts.len() > 1 {
        for (i, s) in stmts.iter().enumerate() {
            println!("Statement {}: {}", i, &s[..s.len().min(100)]);
        }
    }
    assert!(stmts[0].contains("CREATE OR REPLACE PACKAGE BODY"));
    assert!(stmts[0].contains("END oqt_pkg"));
}

#[test]
fn test_nested_begin_end_in_package() {
    let sql = r#"CREATE PACKAGE BODY test_pkg AS
  PROCEDURE proc1 IS
  BEGIN
IF TRUE THEN
  BEGIN
    NULL;
  END;
END IF;
  END;
END test_pkg;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_if_condition_with_inline_comment_before_then() {
    let sql = r#"BEGIN
  IF (1 = 1) /* inline comment */ THEN
    NULL;
  END IF;
END;
/"#;

    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(
        stmts.len(),
        1,
        "inline comment between IF condition and THEN should keep block depth balanced"
    );
}

#[test]
fn test_package_with_nested_declare() {
    let sql = r#"CREATE PACKAGE BODY test_pkg AS
  PROCEDURE proc1 IS
  BEGIN
DECLARE
  v_temp NUMBER;
BEGIN
  v_temp := 1;
END;
  END;
END test_pkg;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_package_followed_by_select() {
    let sql = r#"CREATE PACKAGE test_pkg AS
  PROCEDURE proc1;
END test_pkg;

SELECT 1 FROM DUAL;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 2, "Should have 2 statements, got: {:?}", stmts);
    assert!(stmts[0].contains("CREATE PACKAGE"));
    assert!(stmts[1].contains("SELECT"));
}

#[test]
fn test_multiple_packages() {
    let sql = r#"CREATE PACKAGE pkg1 AS
  PROCEDURE proc1;
END pkg1;

CREATE PACKAGE pkg2 AS
  PROCEDURE proc2;
END pkg2;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 2, "Should have 2 statements, got: {:?}", stmts);
}

#[test]
fn test_create_trigger() {
    let sql = r#"CREATE TRIGGER test_trg
BEFORE INSERT ON test_table
FOR EACH ROW
BEGIN
  :NEW.created_at := SYSDATE;
END;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_create_editioning_trigger() {
    let sql = r#"CREATE OR REPLACE EDITIONING TRIGGER test_trg
BEFORE INSERT ON test_table
FOR EACH ROW
BEGIN
  :NEW.created_at := SYSDATE;
END;

SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(
        stmts.len(),
        2,
        "EDITIONING TRIGGER body must stay as one statement before trailing SELECT: {:?}",
        stmts
    );
    assert!(
        stmts[0].contains("CREATE OR REPLACE EDITIONING TRIGGER"),
        "first statement should keep EDITIONING TRIGGER header: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_create_type() {
    let sql = r#"CREATE TYPE test_type AS OBJECT (
  id NUMBER,
  name VARCHAR2(100)
);"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_create_type_object_attribute_prefixed_create_does_not_force_split() {
    let sql = r#"CREATE OR REPLACE TYPE test_type AS OBJECT (
  create_flag NUMBER,
  id NUMBER
);
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "CREATE TYPE OBJECT attribute named CREATE_* should not trigger forced split: {:?}",
        stmts
    );
    assert!(
        stmts[0].contains("create_flag NUMBER"),
        "TYPE OBJECT statement should preserve CREATE_* attribute line: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_create_type_enum_splits_before_next_statement() {
    let sql = r#"CREATE OR REPLACE TYPE color_t AS ENUM ('RED', 'GREEN');
SELECT 1 FROM dual;"#;

    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "CREATE TYPE ... AS ENUM should split before trailing SELECT, got: {:?}",
        stmts
    );
    assert!(
        stmts[0].contains("AS ENUM ('RED', 'GREEN')"),
        "first statement should preserve ENUM declaration: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 1 FROM dual"));
}

#[test]
fn test_comments_stripped() {
    let sql = r#"-- This is a comment
SELECT 1 FROM DUAL;
-- Another comment"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
    assert!(
        !stmts[0].starts_with("--"),
        "Leading comment should be stripped"
    );
}

#[test]
fn test_block_comment_stripped() {
    let sql = r#"/* Block comment */
SELECT 1 FROM DUAL;
/* Trailing comment */"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_procedure_with_loop() {
    let sql = r#"CREATE PROCEDURE test_proc AS
BEGIN
  FOR i IN 1..10 LOOP
DBMS_OUTPUT.PUT_LINE(i);
  END LOOP;
END;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_procedure_with_case() {
    let sql = r#"CREATE PROCEDURE test_proc(p_val NUMBER) AS
BEGIN
  CASE p_val
WHEN 1 THEN DBMS_OUTPUT.PUT_LINE('one');
WHEN 2 THEN DBMS_OUTPUT.PUT_LINE('two');
ELSE DBMS_OUTPUT.PUT_LINE('other');
  END CASE;
END;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_slash_terminator() {
    let sql = r#"CREATE PROCEDURE test_proc AS
BEGIN
  NULL;
END;
/
SELECT 1 FROM DUAL;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 2, "Should have 2 statements, got: {:?}", stmts);
}

#[test]
fn test_slash_terminator_after_end_without_semicolon() {
    let sql = r#"CREATE PROCEDURE test_proc AS
BEGIN
  NULL;
END
/
SELECT 1 FROM DUAL;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(
        stmts.len(),
        2,
        "END followed by SQL*Plus slash without semicolon should still split, got: {:?}",
        stmts
    );
}

#[test]
fn test_split_format_items_slash_terminator_after_end_without_semicolon() {
    let sql = r#"CREATE PROCEDURE test_proc AS
BEGIN
  NULL;
END
/
SELECT 1 FROM DUAL;"#;
    let items = QueryExecutor::split_format_items(sql);

    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    let slash_count = items
        .iter()
        .filter(|item| matches!(item, FormatItem::Slash))
        .count();

    assert_eq!(
        statements.len(),
        2,
        "split_format_items should split END + slash without semicolon, got: {:?}",
        statements
    );
    assert_eq!(slash_count, 1, "slash delimiter should be preserved once");
}

#[test]
fn test_compound_trigger_end_timing_point_without_semicolon_before_slash() {
    let sql = r#"CREATE OR REPLACE TRIGGER trg_compound_view
FOR INSERT ON test_view
COMPOUND TRIGGER
  INSTEAD OF EACH ROW IS
  BEGIN
    NULL;
  END INSTEAD OF EACH ROW
END
/
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "compound trigger with END timing point and no semicolon before slash should split, got: {:?}",
        stmts
    );
}

#[test]
fn test_compound_trigger_timing_point_without_is_splits_before_following_select() {
    let sql = r#"CREATE OR REPLACE TRIGGER trg_compound_no_is
FOR INSERT ON test_table
COMPOUND TRIGGER
  BEFORE STATEMENT
  BEGIN
    NULL;
  END BEFORE STATEMENT;
END;
SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "compound trigger timing-point without IS should split, got: {:?}",
        stmts
    );
    assert!(
        stmts[0].contains("END BEFORE STATEMENT"),
        "timing-point END without IS must remain in trigger statement: {}",
        stmts[0]
    );
}

#[test]
fn test_split_script_items_slash_line_inside_q_quote_is_not_terminator() {
    let sql = "SELECT q'[\n/\n]' AS txt FROM dual;\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "slash line inside q-quote must not split statements: {:?}",
        stmts
    );
    assert!(
        stmts[0].contains("q'[\n/\n]'"),
        "first statement should preserve q-quote slash line, got: {}",
        stmts[0]
    );
    assert_eq!(stmts[1], "SELECT 2 FROM dual");
}

#[test]
fn test_split_format_items_slash_line_inside_q_quote_is_not_terminator() {
    let sql = "SELECT q'[\n/\n]' AS txt FROM dual;\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_format_items(sql);

    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        !items.iter().any(|item| matches!(item, FormatItem::Slash)),
        "slash line inside q-quote must not be parsed as format slash item"
    );
    assert_eq!(
        statements.len(),
        2,
        "expected two format statements, got: {:?}",
        statements
    );
    assert!(
        statements[0].contains("q'[\n/\n]'"),
        "first format statement should preserve q-quote slash line, got: {}",
        statements[0]
    );
    assert_eq!(statements[1], "SELECT 2 FROM dual");
}

#[test]
fn test_complete_package_with_spec_and_body() {
    let sql = r#"CREATE OR REPLACE PACKAGE oqt_pkg AS
  PROCEDURE log_msg(p_tag IN VARCHAR2, p_msg IN VARCHAR2, p_n1 IN NUMBER DEFAULT NULL);

  PROCEDURE p_basic(
p_in_num   IN  NUMBER,
p_in_txt   IN  VARCHAR2 DEFAULT 'DEF',
p_out_txt  OUT VARCHAR2,
p_inout_n  IN OUT NUMBER
  );

  PROCEDURE p_over(p_txt IN VARCHAR2);
  PROCEDURE p_over(p_num IN NUMBER, p_txt IN VARCHAR2);

  PROCEDURE p_refcur(p_tag IN VARCHAR2, p_rc OUT SYS_REFCURSOR);

  PROCEDURE p_raise(p_mode IN VARCHAR2);

  FUNCTION f_sum(p_a IN NUMBER, p_b IN NUMBER) RETURN NUMBER;
  FUNCTION f_echo(p_txt IN VARCHAR2) RETURN VARCHAR2;
  FUNCTION f_dateadd(p_d IN DATE, p_days IN NUMBER DEFAULT 1) RETURN DATE;
END oqt_pkg;
/
SHOW ERRORS PACKAGE oqt_pkg;

CREATE OR REPLACE PACKAGE BODY oqt_pkg AS
  PROCEDURE log_msg(p_tag IN VARCHAR2, p_msg IN VARCHAR2, p_n1 IN NUMBER DEFAULT NULL) IS
  BEGIN
INSERT INTO oqt_call_log(id, tag, msg, n1, created_at)
VALUES (oqt_call_log_seq.NEXTVAL, p_tag, p_msg, p_n1, SYSDATE);
  END;

  PROCEDURE p_basic(
p_in_num   IN  NUMBER,
p_in_txt   IN  VARCHAR2 DEFAULT 'DEF',
p_out_txt  OUT VARCHAR2,
p_inout_n  IN OUT NUMBER
  ) IS
  BEGIN
p_out_txt := 'IN_NUM='||p_in_num||', IN_TXT='||p_in_txt||', INOUT='||p_inout_n;
p_inout_n := NVL(p_inout_n,0) + p_in_num;

log_msg('P_BASIC', p_out_txt, p_in_num);
DBMS_OUTPUT.PUT_LINE('[p_basic] out='||p_out_txt||' / inout='||p_inout_n);
  END;

  PROCEDURE p_over(p_txt IN VARCHAR2) IS
  BEGIN
log_msg('P_OVER1', p_txt);
DBMS_OUTPUT.PUT_LINE('[p_over(txt)] '||NVL(p_txt,'<NULL>'));
  END;

  PROCEDURE p_over(p_num IN NUMBER, p_txt IN VARCHAR2) IS
  BEGIN
log_msg('P_OVER2', p_txt, p_num);
DBMS_OUTPUT.PUT_LINE('[p_over(num,txt)] '||p_num||' / '||NVL(p_txt,'<NULL>'));
  END;

  PROCEDURE p_refcur(p_tag IN VARCHAR2, p_rc OUT SYS_REFCURSOR) IS
  BEGIN
OPEN p_rc FOR
  SELECT id, tag, msg, n1, created_at
  FROM oqt_call_log
  WHERE tag LIKE p_tag||'%'
  ORDER BY id DESC;
  END;

  PROCEDURE p_raise(p_mode IN VARCHAR2) IS
  BEGIN
IF p_mode = 'NO_DATA_FOUND' THEN
  DECLARE v NUMBER;
  BEGIN
    SELECT n1 INTO v FROM oqt_call_log WHERE id = -9999;
  END;
ELSIF p_mode = 'APP' THEN
  RAISE_APPLICATION_ERROR(-20001, 'oqt_pkg.p_raise app error');
ELSE
  DBMS_OUTPUT.PUT_LINE('[p_raise] ok');
END IF;
  END;

  FUNCTION f_sum(p_a IN NUMBER, p_b IN NUMBER) RETURN NUMBER IS
  BEGIN
RETURN NVL(p_a,0) + NVL(p_b,0);
  END;

  FUNCTION f_echo(p_txt IN VARCHAR2) RETURN VARCHAR2 IS
  BEGIN
RETURN 'ECHO:'||p_txt;
  END;

  FUNCTION f_dateadd(p_d IN DATE, p_days IN NUMBER DEFAULT 1) RETURN DATE IS
  BEGIN
RETURN p_d + p_days;
  END;
END oqt_pkg;
/
SHOW ERRORS PACKAGE BODY oqt_pkg;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    // Count tool commands (SHOW ERRORS)
    let tool_cmds: Vec<_> = items
        .iter()
        .filter(|item| matches!(item, ScriptItem::ToolCommand(_)))
        .collect();

    if stmts.len() != 2 {
        println!(
            "\n=== FAILED: Expected 2 statements, got {} ===",
            stmts.len()
        );
        for (i, s) in stmts.iter().enumerate() {
            let preview = if s.len() > 100 { &s[..100] } else { s };
            println!("\n--- Statement {} ---\n{}...\n---", i, preview);
        }
    }

    assert_eq!(
        stmts.len(),
        2,
        "Should have 2 statements (package spec + body), got {}",
        stmts.len()
    );
    assert_eq!(
        tool_cmds.len(),
        2,
        "Should have 2 tool commands (SHOW ERRORS), got {}",
        tool_cmds.len()
    );

    // Verify package spec
    assert!(
        stmts[0].contains("CREATE OR REPLACE PACKAGE oqt_pkg AS"),
        "First statement should be package spec"
    );
    assert!(
        stmts[0].contains("END oqt_pkg"),
        "Package spec should end with END oqt_pkg"
    );
    assert!(
        !stmts[0].contains("PACKAGE BODY"),
        "Package spec should not contain BODY"
    );

    // Verify package body
    assert!(
        stmts[1].contains("CREATE OR REPLACE PACKAGE BODY oqt_pkg AS"),
        "Second statement should be package body"
    );
    assert!(
        stmts[1].contains("END oqt_pkg"),
        "Package body should end with END oqt_pkg"
    );
}

#[test]
fn test_show_errors_without_slash() {
    // Test case: SHOW ERRORS without preceding slash (/) separator
    // This simulates the user's issue where SHOW ERRORS is not separated
    // from the CREATE PACKAGE BODY when there's no slash terminator
    let sql = r#"CREATE OR REPLACE PACKAGE BODY oqt_deep_pkg AS

  PROCEDURE log_msg(p_tag IN VARCHAR2, p_msg IN VARCHAR2, p_depth IN NUMBER) IS
  BEGIN
INSERT INTO oqt_t_log(log_id, tag, msg, depth)
VALUES (oqt_seq_log.NEXTVAL, SUBSTR(p_tag,1,30), SUBSTR(p_msg,1,4000), p_depth);
DBMS_OUTPUT.PUT_LINE('[LOG]['||p_tag||'][depth='||p_depth||'] '||p_msg);
  END;

END oqt_deep_pkg;

SHOW ERRORS"#;

    let items = QueryExecutor::split_script_items(sql);

    let stmts: Vec<_> = items
        .iter()
        .filter_map(|item| {
            if let ScriptItem::Statement(s) = item {
                Some(s.clone())
            } else {
                None
            }
        })
        .collect();

    let tool_cmds: Vec<_> = items
        .iter()
        .filter(|item| matches!(item, ScriptItem::ToolCommand(_)))
        .collect();

    // Debug output
    println!("\n=== Test: SHOW ERRORS without slash ===");
    println!("Total items: {}", items.len());
    println!("Statements: {}", stmts.len());
    println!("Tool commands: {}", tool_cmds.len());

    for (i, item) in items.iter().enumerate() {
        match item {
            ScriptItem::Statement(s) => {
                let preview = if s.len() > 80 {
                    format!("{}...", &s[..80])
                } else {
                    s.clone()
                };
                println!("\n[{}] Statement: {}", i, preview);
            }
            ScriptItem::ToolCommand(cmd) => {
                println!("\n[{}] ToolCommand: {:?}", i, cmd);
            }
        }
    }

    // Should have 1 statement (CREATE PACKAGE BODY) and 1 tool command (SHOW ERRORS)
    assert_eq!(
        stmts.len(),
        1,
        "Should have 1 statement (package body), got {}",
        stmts.len()
    );
    assert_eq!(
        tool_cmds.len(),
        1,
        "Should have 1 tool command (SHOW ERRORS), got {}",
        tool_cmds.len()
    );

    // Verify package body doesn't contain SHOW ERRORS
    assert!(
        !stmts[0].contains("SHOW ERRORS"),
        "Package body should NOT contain SHOW ERRORS"
    );
}

#[test]
fn test_show_errors_complex_package_without_slash() {
    // Test case from user: complex package body with nested procedures,
    // CASE, LOOP, DECLARE blocks, followed by SHOW ERRORS without slash
    let sql = r#"CREATE OR REPLACE PACKAGE BODY oqt_deep_pkg AS

  PROCEDURE log_msg(p_tag IN VARCHAR2, p_msg IN VARCHAR2, p_depth IN NUMBER) IS
  BEGIN
INSERT INTO oqt_t_log(log_id, tag, msg, depth)
VALUES (oqt_seq_log.NEXTVAL, SUBSTR(p_tag,1,30), SUBSTR(p_msg,1,4000), p_depth);
DBMS_OUTPUT.PUT_LINE('[LOG]['||p_tag||'][depth='||p_depth||'] '||p_msg);
  END;

  FUNCTION f_calc(p_n IN NUMBER) RETURN NUMBER IS
v NUMBER := 0;
  BEGIN
-- Nested IF + CASE + inner block
IF p_n IS NULL THEN
  v := -1;
ELSE
  CASE
    WHEN p_n < 0 THEN
      v := p_n * p_n;
    WHEN p_n BETWEEN 0 AND 10 THEN
      DECLARE
        x NUMBER := p_n + 100;
      BEGIN
        v := x - 50;
      END;
    ELSE
      v := p_n + 999;
  END CASE;
END IF;

RETURN v;
  EXCEPTION
WHEN OTHERS THEN
  log_msg('F_CALC', 'error='||SQLERRM, 999);
  RETURN NULL;
  END;

  PROCEDURE p_deep_run(p_limit IN NUMBER DEFAULT 7) IS
v_depth NUMBER := 0;

PROCEDURE p_inner(p_i NUMBER, p_j NUMBER) IS
  v_local NUMBER := 0;
BEGIN
  v_depth := v_depth + 1;
  v_local := f_calc(p_i - p_j);

  <<outer_loop>>
  FOR k IN 1..3 LOOP
    v_depth := v_depth + 1;

    CASE MOD(k + p_i + p_j, 4)
      WHEN 0 THEN
        log_msg('INNER', 'case0 k='||k||' local='||v_local, v_depth);
      WHEN 1 THEN
        DECLARE
          z NUMBER := 10;
        BEGIN
          IF z = 10 THEN
            log_msg('INNER', 'case1 -> raise user error', v_depth);
            RAISE_APPLICATION_ERROR(-20001, 'forced error in inner block');
          END IF;
        EXCEPTION
          WHEN OTHERS THEN
            log_msg('INNER', 'handled inner exception: '||SQLERRM, v_depth);
        END;
      WHEN 2 THEN
        log_msg('INNER', 'case2 -> continue outer_loop', v_depth);
        CONTINUE outer_loop;
      ELSE
        log_msg('INNER', 'case3 -> exit outer_loop', v_depth);
        EXIT outer_loop;
    END CASE;

    DECLARE
      w NUMBER := 0;
    BEGIN
      WHILE w < 2 LOOP
        w := w + 1;
        log_msg('INNER', 'while w='||w, v_depth+1);
      END LOOP;
    END;

  END LOOP outer_loop;

  v_depth := v_depth - 1;
END p_inner;

  BEGIN
log_msg('P_DEEP_RUN', 'start limit='||p_limit, v_depth);

FOR r IN (SELECT id, grp, name FROM oqt_t_depth WHERE id <= p_limit ORDER BY id) LOOP
  v_depth := v_depth + 1;

  BEGIN
    IF r.grp = 0 THEN
      log_msg('RUN', 'grp=0 id='||r.id||' name='||r.name, v_depth);

      CASE
        WHEN r.id IN (1,2) THEN
          p_inner(r.id, 1);
        WHEN r.id BETWEEN 3 AND 5 THEN
          p_inner(r.id, 2);
        ELSE
          p_inner(r.id, 3);
      END CASE;

    ELSIF r.grp = 1 THEN
      log_msg('RUN', 'grp=1 id='||r.id||' (dynamic insert)', v_depth);

      EXECUTE IMMEDIATE
        'INSERT INTO oqt_t_log(log_id, tag, msg, depth)
         VALUES (oqt_seq_log.NEXTVAL, :t, :m, :d)'
      USING 'DYN', 'insert from dyn sql id='||r.id, v_depth;

    ELSE
      log_msg('RUN', 'grp=2 id='||r.id||' (raise & catch)', v_depth);
      BEGIN
        IF r.id = 6 THEN
          log_msg('RUN', 'string contains tokens: "BEGIN END; / CASE WHEN"', v_depth);
        END IF;

        IF r.id = 7 THEN
          RAISE NO_DATA_FOUND;
        END IF;

      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          log_msg('RUN', 'caught NO_DATA_FOUND for id='||r.id, v_depth);
      END;
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      log_msg('RUN', 'outer exception caught: '||SQLERRM, v_depth);
  END;

  v_depth := v_depth - 1;
END LOOP;

DECLARE
  t oqt_deep_tab := oqt_deep_tab();
BEGIN
  t.EXTEND(3);
  t(1) := oqt_deep_obj(1, 'A');
  t(2) := oqt_deep_obj(2, 'B');
  t(3) := oqt_deep_obj(3, 'C');

  FOR i IN 1..t.COUNT LOOP
    log_msg('COLL', 't('||i||')='||t(i).k||','||t(i).v, 77);
  END LOOP;
END;

log_msg('P_DEEP_RUN', 'done', v_depth);
  END p_deep_run;

END oqt_deep_pkg;

SHOW ERRORS

--------------------------------------------------------------------------------
PROMPT [5] REFCURSOR test (VARIABLE/PRINT + OUT refcursor)
--------------------------------------------------------------------------------

VAR v_rc REFCURSOR"#;

    let items = QueryExecutor::split_script_items(sql);

    let stmts: Vec<_> = items
        .iter()
        .filter_map(|item| {
            if let ScriptItem::Statement(s) = item {
                Some(s.clone())
            } else {
                None
            }
        })
        .collect();

    let tool_cmds: Vec<_> = items
        .iter()
        .filter(|item| matches!(item, ScriptItem::ToolCommand(_)))
        .collect();

    // Debug output
    println!("\n=== Test: Complex package body with SHOW ERRORS (no slash) ===");
    println!("Total items: {}", items.len());
    println!("Statements: {}", stmts.len());
    println!("Tool commands: {}", tool_cmds.len());

    for (i, item) in items.iter().enumerate() {
        match item {
            ScriptItem::Statement(s) => {
                let preview = if s.len() > 120 {
                    format!("{}...", &s[..120])
                } else {
                    s.clone()
                };
                println!("\n[{}] Statement (len={}): {}", i, s.len(), preview);
            }
            ScriptItem::ToolCommand(cmd) => {
                println!("\n[{}] ToolCommand: {:?}", i, cmd);
            }
        }
    }

    // Should have 1 statement (CREATE PACKAGE BODY)
    // Tool commands: SHOW ERRORS, PROMPT, VAR
    assert_eq!(
        stmts.len(),
        1,
        "Should have 1 statement (package body), got {}",
        stmts.len()
    );

    // Verify package body doesn't contain SHOW ERRORS
    assert!(
        !stmts[0].contains("SHOW ERRORS"),
        "Package body should NOT contain SHOW ERRORS - it was not separated!"
    );

    // Should have at least SHOW ERRORS and VAR commands
    assert!(
        tool_cmds.len() >= 2,
        "Should have at least 2 tool commands (SHOW ERRORS, VAR), got {}",
        tool_cmds.len()
    );
}

#[test]
fn test_show_errors_with_ref_cursor_procedure() {
    // Additional test: package body with REF CURSOR procedure
    let sql = r#"CREATE OR REPLACE PACKAGE BODY oqt_deep_pkg AS

  PROCEDURE log_msg(p_tag IN VARCHAR2, p_msg IN VARCHAR2, p_depth IN NUMBER) IS
  BEGIN
INSERT INTO oqt_t_log(log_id, tag, msg, depth)
VALUES (oqt_seq_log.NEXTVAL, SUBSTR(p_tag,1,30), SUBSTR(p_msg,1,4000), p_depth);
  END;

  PROCEDURE p_open_rc(p_grp IN NUMBER, p_rc OUT t_rc) IS
v_sql VARCHAR2(32767);
  BEGIN
-- Dynamic SQL + bind
v_sql := 'SELECT id, grp, name, created_at
          FROM oqt_t_depth
          WHERE grp = :b1
          ORDER BY id';

OPEN p_rc FOR v_sql USING p_grp;
log_msg('P_OPEN_RC', 'opened rc for grp='||p_grp, 1);
  END;

END oqt_deep_pkg;

SHOW ERRORS"#;

    let items = QueryExecutor::split_script_items(sql);

    let stmts: Vec<_> = items
        .iter()
        .filter_map(|item| {
            if let ScriptItem::Statement(s) = item {
                Some(s.clone())
            } else {
                None
            }
        })
        .collect();

    let tool_cmds: Vec<_> = items
        .iter()
        .filter(|item| matches!(item, ScriptItem::ToolCommand(_)))
        .collect();

    println!("\n=== Test: Package with REF CURSOR procedure ===");
    println!("Total items: {}", items.len());
    println!("Statements: {}", stmts.len());
    println!("Tool commands: {}", tool_cmds.len());

    for (i, item) in items.iter().enumerate() {
        match item {
            ScriptItem::Statement(s) => {
                println!("\n[{}] Statement (len={}):\n{}", i, s.len(), s);
            }
            ScriptItem::ToolCommand(cmd) => {
                println!("\n[{}] ToolCommand: {:?}", i, cmd);
            }
        }
    }

    // Should have 1 statement and 1 tool command
    assert_eq!(stmts.len(), 1, "Should have 1 statement");
    assert_eq!(tool_cmds.len(), 1, "Should have 1 tool command");
    assert!(
        !stmts[0].contains("SHOW ERRORS"),
        "Package body should NOT contain SHOW ERRORS"
    );
}

#[test]
fn test_package_body_show_errors_without_slash_newline_only() {
    // Test case matching user's exact issue:
    // Package body ends with "END package_name;" and newlines,
    // then SHOW ERRORS without a preceding slash
    //
    // Full test with IF, CASE, DECLARE block, and IS NULL expression
    let sql = "CREATE OR REPLACE PACKAGE BODY oqt_deep_pkg AS

  PROCEDURE log_msg(p_tag IN VARCHAR2, p_msg IN VARCHAR2, p_depth IN NUMBER) IS
  BEGIN
DBMS_OUTPUT.PUT_LINE('[LOG]['||p_tag||'][depth='||p_depth||'] '||p_msg);
  END;

  FUNCTION f_calc(p_n IN NUMBER) RETURN NUMBER IS
v NUMBER := 0;
  BEGIN
IF p_n IS NULL THEN
  v := -1;
ELSE
  CASE
    WHEN p_n < 0 THEN
      v := p_n * p_n;
    WHEN p_n BETWEEN 0 AND 10 THEN
      DECLARE
        x NUMBER := p_n + 100;
      BEGIN
        v := x - 50;
      END;
    ELSE
      v := p_n + 999;
  END CASE;
END IF;
RETURN v;
  EXCEPTION
WHEN OTHERS THEN
  log_msg('F_CALC', 'error='||SQLERRM, 999);
  RETURN NULL;
  END;

END oqt_deep_pkg;

SHOW ERRORS";

    let items = QueryExecutor::split_script_items(sql);

    let stmts: Vec<_> = items
        .iter()
        .filter_map(|item| {
            if let ScriptItem::Statement(s) = item {
                Some(s.clone())
            } else {
                None
            }
        })
        .collect();

    let tool_cmds: Vec<_> = items
        .iter()
        .filter(|item| matches!(item, ScriptItem::ToolCommand(_)))
        .collect();

    println!("\n=== Test: Package body + SHOW ERRORS without slash (newline only) ===");
    println!("Total items: {}", items.len());
    println!("Statements: {}", stmts.len());
    println!("Tool commands: {}", tool_cmds.len());

    for (i, item) in items.iter().enumerate() {
        match item {
            ScriptItem::Statement(s) => {
                let lines: Vec<&str> = s.lines().collect();
                let last_lines = if lines.len() > 5 {
                    lines[lines.len() - 5..].join("\n")
                } else {
                    s.clone()
                };
                println!(
                    "\n[{}] Statement (len={}, lines={}):\n...last lines:\n{}",
                    i,
                    s.len(),
                    lines.len(),
                    last_lines
                );
            }
            ScriptItem::ToolCommand(cmd) => {
                println!("\n[{}] ToolCommand: {:?}", i, cmd);
            }
        }
    }

    // Should have 1 statement and 1 tool command
    assert_eq!(
        stmts.len(),
        1,
        "Should have 1 statement (package body), got {}",
        stmts.len()
    );
    assert_eq!(
        tool_cmds.len(),
        1,
        "Should have 1 tool command (SHOW ERRORS), got {}",
        tool_cmds.len()
    );

    // Verify package body doesn't contain SHOW ERRORS
    assert!(
        !stmts[0].contains("SHOW ERRORS"),
        "Package body should NOT contain SHOW ERRORS - statement was not properly separated!"
    );
}

#[test]
fn test_package_spec_ends_with_depth_zero() {
    // Test case: Package SPEC (not BODY) should end with depth 0
    // Package spec has AS/IS but no BEGIN, ends with END package_name;
    let sql = r#"CREATE OR REPLACE PACKAGE oqt_deep_pkg AS
  -- REFCURSOR type
  TYPE t_rc IS REF CURSOR;

  -- simple log
  PROCEDURE log_msg(p_tag IN VARCHAR2, p_msg IN VARCHAR2, p_depth IN NUMBER);

  -- returns scalar with nested control flows
  FUNCTION f_calc(p_n IN NUMBER) RETURN NUMBER;

  -- opens refcursor with dynamic SQL and returns it via OUT
  PROCEDURE p_open_rc(p_grp IN NUMBER, p_rc OUT t_rc);

  -- heavy nested block for depth/parsing test
  PROCEDURE p_deep_run(p_limit IN NUMBER DEFAULT 7);
END oqt_deep_pkg;

SHOW ERRORS"#;

    let items = QueryExecutor::split_script_items(sql);

    let stmts: Vec<_> = items
        .iter()
        .filter_map(|item| {
            if let ScriptItem::Statement(s) = item {
                Some(s.clone())
            } else {
                None
            }
        })
        .collect();

    let tool_cmds: Vec<_> = items
        .iter()
        .filter(|item| matches!(item, ScriptItem::ToolCommand(_)))
        .collect();

    println!("\n=== Test: Package SPEC ends with depth 0 ===");
    println!("Total items: {}", items.len());
    println!("Statements: {}", stmts.len());
    println!("Tool commands: {}", tool_cmds.len());

    for (i, item) in items.iter().enumerate() {
        match item {
            ScriptItem::Statement(s) => {
                println!("\n[{}] Statement (len={}):\n{}", i, s.len(), s);
            }
            ScriptItem::ToolCommand(cmd) => {
                println!("\n[{}] ToolCommand: {:?}", i, cmd);
            }
        }
    }

    // Should have 1 statement (package spec) and 1 tool command (SHOW ERRORS)
    assert_eq!(
        stmts.len(),
        1,
        "Should have 1 statement (package spec), got {}",
        stmts.len()
    );
    assert_eq!(
        tool_cmds.len(),
        1,
        "Should have 1 tool command (SHOW ERRORS), got {}",
        tool_cmds.len()
    );

    // Verify package spec doesn't contain SHOW ERRORS
    assert!(
        !stmts[0].contains("SHOW ERRORS"),
        "Package spec should NOT contain SHOW ERRORS - depth did not return to 0!"
    );
}

#[test]
fn test_package_body_with_declare_blocks() {
    // Test case: Package body with nested procedure
    // This is the minimal case that fails
    let sql = r#"CREATE OR REPLACE PACKAGE BODY test_pkg AS
  PROCEDURE p_outer IS
PROCEDURE p_inner IS
BEGIN
  NULL;
END p_inner;
  BEGIN
NULL;
  END p_outer;
END test_pkg;

SHOW ERRORS"#;

    let items = QueryExecutor::split_script_items(sql);

    let stmts: Vec<_> = items
        .iter()
        .filter_map(|item| {
            if let ScriptItem::Statement(s) = item {
                Some(s.clone())
            } else {
                None
            }
        })
        .collect();

    let tool_cmds: Vec<_> = items
        .iter()
        .filter(|item| matches!(item, ScriptItem::ToolCommand(_)))
        .collect();

    println!("\n=== Test: Package body with DECLARE blocks ===");
    println!("Total items: {}", items.len());
    println!("Statements: {}", stmts.len());
    println!("Tool commands: {}", tool_cmds.len());

    for (i, stmt) in stmts.iter().enumerate() {
        println!("\n[{}] Statement:\n{}", i, stmt);
    }

    assert_eq!(stmts.len(), 1, "Should have 1 statement");
    assert_eq!(tool_cmds.len(), 1, "Should have 1 tool command");
    assert!(
        !stmts[0].contains("SHOW ERRORS"),
        "Package body should NOT contain SHOW ERRORS"
    );
}

#[test]
fn test_anonymous_block_with_nested_procedure() {
    // Test case: Anonymous block with nested procedure declaration
    // The nested DECLARE inside labeled block should not split the statement
    let sql = r#"DECLARE
  v NUMBER := 0;
  PROCEDURE bump(p IN OUT NUMBER) IS
  BEGIN
p := p + 1;
  END;
BEGIN
  <<blk1>>
  DECLARE
a NUMBER := 0;
  BEGIN
FOR i IN 1..3 LOOP
  bump(a);
END LOOP;
  END blk1;
EXCEPTION
  WHEN OTHERS THEN
DBMS_OUTPUT.PUT_LINE('[ANON] top exception handled: '||SQLERRM);
END;"#;

    let items = QueryExecutor::split_script_items(sql);

    let stmts: Vec<_> = items
        .iter()
        .filter_map(|item| {
            if let ScriptItem::Statement(s) = item {
                Some(s.clone())
            } else {
                None
            }
        })
        .collect();

    println!("\n=== Test: Anonymous block with nested procedure ===");
    println!("Total items: {}", items.len());
    println!("Statements: {}", stmts.len());

    for (i, stmt) in stmts.iter().enumerate() {
        println!("\n[{}] Statement (len={}):\n{}", i, stmt.len(), stmt);
    }

    // Should be exactly 1 statement (the entire anonymous block)
    assert_eq!(
        stmts.len(),
        1,
        "Should have exactly 1 statement (anonymous block), got {}. Block was incorrectly split!",
        stmts.len()
    );

    // Verify the statement contains both the procedure and the call
    assert!(
        stmts[0].contains("PROCEDURE bump"),
        "Statement should contain PROCEDURE bump declaration"
    );
    assert!(
        stmts[0].contains("bump(a)"),
        "Statement should contain bump(a) call"
    );
}

#[test]
fn test_select_with_case_when_expression() {
    // Test case: SELECT with CASE WHEN ... END expression
    // The CASE expression END should NOT be treated as a PL/SQL block END
    let sql = "SELECT CASE WHEN 1=1 THEN 'Y' ELSE 'N' END FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
    assert!(
        stmts[0].contains("CASE WHEN"),
        "Statement should contain CASE WHEN"
    );
}

#[test]
fn test_select_with_case_when_as_alias() {
    // Test case: SELECT with CASE WHEN ... END AS alias
    let sql = "SELECT CASE WHEN 1=1 THEN 'Y' ELSE 'N' END AS result FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_select_with_multiple_case_expressions() {
    // Test case: SELECT with multiple CASE expressions
    let sql = "SELECT CASE WHEN a=1 THEN 'one' END, CASE WHEN b=2 THEN 'two' END FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_plsql_block_with_case_expression_select() {
    // Test case: PL/SQL block containing SELECT with CASE expression
    // This is the critical case where block_depth could be incorrectly decremented
    let sql = r#"BEGIN
  SELECT CASE WHEN 1=1 THEN 'Y' ELSE 'N' END INTO v_result FROM dual;
  NULL;
END;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(
        stmts.len(),
        1,
        "Should have 1 statement (entire PL/SQL block), got: {:?}",
        stmts
    );
    assert!(
        stmts[0].contains("NULL"),
        "Statement should contain NULL (proving block wasn't split)"
    );
}

#[test]
fn test_procedure_with_case_expression_in_select() {
    // Test case: CREATE PROCEDURE with SELECT containing CASE expression
    let sql = r#"CREATE PROCEDURE test_proc AS
  v_result VARCHAR2(1);
BEGIN
  SELECT CASE WHEN 1=1 THEN 'Y' ELSE 'N' END INTO v_result FROM dual;
  DBMS_OUTPUT.PUT_LINE(v_result);
END;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_nested_case_expressions() {
    // Test case: Nested CASE expressions
    let sql =
        "SELECT CASE WHEN a=1 THEN CASE WHEN b=2 THEN 'A' ELSE 'B' END ELSE 'C' END FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_case_statement_vs_case_expression() {
    // Test case: PL/SQL CASE statement (with END CASE) vs CASE expression (with just END)
    let sql = r#"BEGIN
  -- CASE expression in SELECT
  SELECT CASE WHEN 1=1 THEN 'Y' END INTO v_val FROM dual;
  -- CASE statement (PL/SQL control flow)
  CASE v_val
WHEN 'Y' THEN DBMS_OUTPUT.PUT_LINE('Yes');
ELSE DBMS_OUTPUT.PUT_LINE('No');
  END CASE;
END;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_case_statement_with_nested_declare_begin_end() {
    // Regression: CASE statement 안의 DECLARE...BEGIN...END 블록이
    // case_depth로 잘못 소비되어 block_depth가 남는 경우
    let sql = r#"BEGIN
  CASE v_val
WHEN 'A' THEN
  DECLARE
    x NUMBER := 0;
  BEGIN
    x := 1;
  END;
ELSE
  NULL;
  END CASE;
END;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_case_statement_with_nested_begin_end() {
    // CASE statement 안 standalone BEGIN...END 블록
    let sql = r#"BEGIN
  CASE v_val
WHEN 1 THEN
  BEGIN
    DBMS_OUTPUT.PUT_LINE('nested');
  END;
  END CASE;
END;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_case_statement_with_nested_block_and_exception() {
    // test5.txt p_inner 패턴: CASE statement 안 DECLARE/BEGIN/EXCEPTION/END
    let sql = r#"BEGIN
  CASE MOD(k, 4)
WHEN 0 THEN
  NULL;
WHEN 1 THEN
  DECLARE
    z NUMBER := 10;
  BEGIN
    IF z = 10 THEN
      RAISE_APPLICATION_ERROR(-20001, 'test');
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      NULL;
  END;
ELSE
  NULL;
  END CASE;
END;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_case_statement_with_case_expression_inside() {
    // CASE statement 안에 CASE expression (SELECT ... CASE ... END)이 중첩
    let sql = r#"BEGIN
  CASE v_val
WHEN 1 THEN
  SELECT CASE WHEN x=1 THEN 'A' ELSE 'B' END INTO v_res FROM dual;
ELSE
  NULL;
  END CASE;
END;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_multiple_case_statements_in_sequence() {
    // 연속 CASE statement 두 개 + 중첩 블록
    let sql = r#"BEGIN
  CASE v1
WHEN 1 THEN
  BEGIN
    NULL;
  END;
  END CASE;
  CASE v2
WHEN 2 THEN
  BEGIN
    NULL;
  END;
  END CASE;
END;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_nested_case_statements() {
    // CASE statement 안에 CASE statement 중첩 (각각 내부 블록 포함)
    let sql = r#"BEGIN
  CASE v1
WHEN 1 THEN
  CASE v2
    WHEN 'A' THEN
      BEGIN
        NULL;
      END;
  END CASE;
  END CASE;
END;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_compound_trigger_basic() {
    // Basic COMPOUND TRIGGER with single timing point
    let sql = r#"CREATE OR REPLACE TRIGGER test_compound_trg
FOR INSERT ON test_table
COMPOUND TRIGGER
  BEFORE STATEMENT IS
  BEGIN
DBMS_OUTPUT.PUT_LINE('Before statement');
  END BEFORE STATEMENT;
END test_compound_trg;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_compound_trigger_multiple_timing_points() {
    // COMPOUND TRIGGER with all four timing points
    let sql = r#"CREATE OR REPLACE TRIGGER test_compound_trg
FOR INSERT OR UPDATE ON test_table
COMPOUND TRIGGER
  v_count NUMBER := 0;

  BEFORE STATEMENT IS
  BEGIN
v_count := 0;
  END BEFORE STATEMENT;

  BEFORE EACH ROW IS
  BEGIN
v_count := v_count + 1;
  END BEFORE EACH ROW;

  AFTER EACH ROW IS
  BEGIN
DBMS_OUTPUT.PUT_LINE('Row ' || v_count);
  END AFTER EACH ROW;

  AFTER STATEMENT IS
  BEGIN
DBMS_OUTPUT.PUT_LINE('Total: ' || v_count);
  END AFTER STATEMENT;
END test_compound_trg;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_compound_trigger_with_declare_in_timing_point() {
    // COMPOUND TRIGGER with local declarations in timing points
    let sql = r#"CREATE OR REPLACE TRIGGER test_compound_trg
FOR INSERT ON test_table
COMPOUND TRIGGER
  BEFORE EACH ROW IS
v_local NUMBER;
  BEGIN
v_local := 1;
:NEW.col1 := v_local;
  END BEFORE EACH ROW;
END test_compound_trg;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_compound_trigger_with_nested_blocks() {
    // COMPOUND TRIGGER with nested BEGIN/END blocks inside timing points
    let sql = r#"CREATE OR REPLACE TRIGGER test_compound_trg
FOR INSERT ON test_table
COMPOUND TRIGGER
  AFTER EACH ROW IS
  BEGIN
IF :NEW.status = 'ACTIVE' THEN
  BEGIN
    INSERT INTO audit_table VALUES (:NEW.id, SYSDATE);
  EXCEPTION
    WHEN OTHERS THEN
      NULL;
  END;
END IF;
  END AFTER EACH ROW;
END test_compound_trg;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_compound_trigger_followed_by_show_errors() {
    // COMPOUND TRIGGER followed by SHOW ERRORS should be separate
    let sql = r#"CREATE OR REPLACE TRIGGER test_compound_trg
FOR INSERT ON test_table
COMPOUND TRIGGER
  BEFORE STATEMENT IS
  BEGIN
NULL;
  END BEFORE STATEMENT;
END test_compound_trg;

SHOW ERRORS"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts: Vec<_> = items
        .iter()
        .filter_map(|item| {
            if let ScriptItem::Statement(s) = item {
                Some(s.clone())
            } else {
                None
            }
        })
        .collect();
    let tool_cmds: Vec<_> = items
        .iter()
        .filter(|item| matches!(item, ScriptItem::ToolCommand(_)))
        .collect();
    assert_eq!(stmts.len(), 1, "Should have 1 statement");
    assert_eq!(
        tool_cmds.len(),
        1,
        "Should have 1 tool command (SHOW ERRORS)"
    );
    assert!(
        !stmts[0].contains("SHOW ERRORS"),
        "COMPOUND TRIGGER should NOT contain SHOW ERRORS"
    );
}

#[test]
fn test_compound_trigger_with_case_statement() {
    // COMPOUND TRIGGER with CASE statement inside timing point
    let sql = r#"CREATE OR REPLACE TRIGGER test_compound_trg
FOR UPDATE ON test_table
COMPOUND TRIGGER
  AFTER EACH ROW IS
  BEGIN
CASE :NEW.type
  WHEN 'A' THEN
    INSERT INTO log_a VALUES (:NEW.id);
  WHEN 'B' THEN
    INSERT INTO log_b VALUES (:NEW.id);
  ELSE
    NULL;
END CASE;
  END AFTER EACH ROW;
END test_compound_trg;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_compound_trigger_instead_of_each_row() {
    // COMPOUND TRIGGER can use INSTEAD OF EACH ROW timing point for views.
    // END INSTEAD OF EACH ROW should close only timing-point depth.
    let sql = r#"CREATE OR REPLACE TRIGGER test_compound_view_trg
INSTEAD OF INSERT ON test_view
COMPOUND TRIGGER
  INSTEAD OF EACH ROW IS
  BEGIN
    INSERT INTO base_table(id) VALUES (:NEW.id);
  END INSTEAD OF EACH ROW;
END test_compound_view_trg;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_compound_trigger_instead_of_followed_by_show_errors() {
    // Ensure END INSTEAD OF ... does not leave depth stale and swallow next command.
    let sql = r#"CREATE OR REPLACE TRIGGER test_compound_view_trg
INSTEAD OF INSERT ON test_view
COMPOUND TRIGGER
  INSTEAD OF EACH ROW IS
  BEGIN
    NULL;
  END INSTEAD OF EACH ROW;
END test_compound_view_trg;

SHOW ERRORS"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts: Vec<_> = items
        .iter()
        .filter_map(|item| {
            if let ScriptItem::Statement(s) = item {
                Some(s.clone())
            } else {
                None
            }
        })
        .collect();
    let tool_cmds: Vec<_> = items
        .iter()
        .filter(|item| matches!(item, ScriptItem::ToolCommand(_)))
        .collect();
    assert_eq!(stmts.len(), 1, "Should have 1 statement");
    assert_eq!(
        tool_cmds.len(),
        1,
        "Should have 1 tool command (SHOW ERRORS)"
    );
    assert!(
        !stmts[0].contains("SHOW ERRORS"),
        "COMPOUND TRIGGER should NOT contain SHOW ERRORS"
    );
}

#[test]
fn test_compound_trigger_after_statement_splits_following_statement() {
    let sql = r#"CREATE OR REPLACE TRIGGER test_compound_after_stmt
FOR UPDATE ON test_tab
COMPOUND TRIGGER
  AFTER STATEMENT IS
  BEGIN
    NULL;
  END AFTER STATEMENT;
END test_compound_after_stmt;

SELECT 1 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(
        stmts.len(),
        2,
        "compound trigger should split before trailing SELECT, got: {stmts:?}"
    );
    assert!(
        stmts[0].contains("END AFTER STATEMENT"),
        "first statement should keep timing-point END: {}",
        stmts[0]
    );
    assert_eq!(stmts[1], "SELECT 1 FROM dual");
}

#[test]
fn test_create_view_with_subqueries_and_like_patterns() {
    // CREATE VIEW with:
    // - Subqueries in CASE WHEN (SELECT ... IN (subquery))
    // - Scalar subquery with COUNT(*)
    // - LIKE patterns containing ';', 'END;', '/ ' (could be misinterpreted)
    // - Multiple nested parentheses and IN clauses
    let sql = r#"CREATE OR REPLACE VIEW oqt_nm_v AS
SELECT
  t.id,
  t.grp,
  CASE
WHEN t.id IN (SELECT id FROM oqt_nm_t WHERE id <= 9) THEN 'IN'
ELSE 'OUT'
  END AS flag,
  (SELECT COUNT(*)
 FROM oqt_nm_t x
WHERE x.grp=t.grp
  AND (x.payload LIKE '%;%' OR x.payload LIKE '%END;%' OR x.payload LIKE '%/ %')
  ) AS cnt_like
FROM oqt_nm_t t
WHERE (t.id BETWEEN 1 AND 999999)
  AND ( (t.grp IN ('G0','G1','G2')) OR (t.grp IN ('G3','G4','G5','G6')) );"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
    assert!(stmts[0].starts_with("CREATE OR REPLACE VIEW"));
    assert!(stmts[0].contains("cnt_like"));
}

#[test]
fn test_create_view_without_trailing_semicolon() {
    // Same CREATE VIEW but without trailing semicolon
    let sql = r#"CREATE OR REPLACE VIEW oqt_nm_v AS
SELECT
  t.id,
  t.grp,
  CASE
WHEN t.id IN (SELECT id FROM oqt_nm_t WHERE id <= 9) THEN 'IN'
ELSE 'OUT'
  END AS flag,
  (SELECT COUNT(*)
 FROM oqt_nm_t x
WHERE x.grp=t.grp
  AND (x.payload LIKE '%;%' OR x.payload LIKE '%END;%' OR x.payload LIKE '%/ %')
  ) AS cnt_like
FROM oqt_nm_t t
WHERE (t.id BETWEEN 1 AND 999999)
  AND ( (t.grp IN ('G0','G1','G2')) OR (t.grp IN ('G3','G4','G5','G6')) )"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
    assert!(stmts[0].starts_with("CREATE OR REPLACE VIEW"));
    assert!(stmts[0].contains("cnt_like"));
}

#[test]
fn test_create_view_followed_by_another_statement() {
    // CREATE VIEW followed by another statement
    let sql = r#"CREATE OR REPLACE VIEW oqt_nm_v AS
SELECT
  t.id,
  t.grp,
  CASE
WHEN t.id IN (SELECT id FROM oqt_nm_t WHERE id <= 9) THEN 'IN'
ELSE 'OUT'
  END AS flag,
  (SELECT COUNT(*)
 FROM oqt_nm_t x
WHERE x.grp=t.grp
  AND (x.payload LIKE '%;%' OR x.payload LIKE '%END;%' OR x.payload LIKE '%/ %')
  ) AS cnt_like
FROM oqt_nm_t t
WHERE (t.id BETWEEN 1 AND 999999)
  AND ( (t.grp IN ('G0','G1','G2')) OR (t.grp IN ('G3','G4','G5','G6')) );

SELECT * FROM oqt_nm_v;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 2, "Should have 2 statements, got: {:?}", stmts);
    assert!(stmts[0].starts_with("CREATE OR REPLACE VIEW"));
    assert!(stmts[0].contains("cnt_like"));
    assert!(stmts[1].contains("SELECT * FROM oqt_nm_v"));
}

#[test]
fn test_create_view_with_slash_terminator() {
    // CREATE VIEW terminated by "/" instead of ";"
    let sql = r#"CREATE OR REPLACE VIEW oqt_nm_v AS
SELECT
  t.id,
  t.grp,
  CASE
WHEN t.id IN (SELECT id FROM oqt_nm_t WHERE id <= 9) THEN 'IN'
ELSE 'OUT'
  END AS flag,
  (SELECT COUNT(*)
 FROM oqt_nm_t x
WHERE x.grp=t.grp
  AND (x.payload LIKE '%;%' OR x.payload LIKE '%END;%' OR x.payload LIKE '%/ %')
  ) AS cnt_like
FROM oqt_nm_t t
WHERE (t.id BETWEEN 1 AND 999999)
  AND ( (t.grp IN ('G0','G1','G2')) OR (t.grp IN ('G3','G4','G5','G6')) )
/

SELECT * FROM oqt_nm_v;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 2, "Should have 2 statements, got: {:?}", stmts);
    assert!(stmts[0].starts_with("CREATE OR REPLACE VIEW"));
    assert!(stmts[0].contains("cnt_like"));
    assert!(stmts[1].contains("SELECT * FROM oqt_nm_v"));
}

#[test]
fn test_extract_bind_names_skips_new_old_in_trigger() {
    // CREATE TRIGGER should NOT extract :NEW and :OLD as bind variables
    let sql = r#"CREATE OR REPLACE TRIGGER test_trg
BEFORE INSERT ON test_table
FOR EACH ROW
BEGIN
  :NEW.created_at := SYSDATE;
  :NEW.created_by := :user_id;
  IF :OLD.status IS NOT NULL THEN
:NEW.modified_at := SYSDATE;
  END IF;
END;"#;
    let names = QueryExecutor::extract_bind_names(sql);
    // :NEW and :OLD should be skipped, only :user_id should be extracted
    assert_eq!(
        names.len(),
        1,
        "Should have 1 bind variable, got: {:?}",
        names
    );
    assert!(
        names.iter().any(|n| n.to_uppercase() == "USER_ID"),
        "Should contain USER_ID, got: {:?}",
        names
    );
    assert!(
        !names.iter().any(|n| n.to_uppercase() == "NEW"),
        "Should NOT contain NEW, got: {:?}",
        names
    );
    assert!(
        !names.iter().any(|n| n.to_uppercase() == "OLD"),
        "Should NOT contain OLD, got: {:?}",
        names
    );
}

#[test]
fn test_extract_bind_names_normal_plsql_includes_new_old() {
    // Regular PL/SQL block (not CREATE TRIGGER) should extract :NEW and :OLD as bind variables
    let sql = r#"BEGIN
  :NEW := 'test';
  :OLD := 'old_value';
END;"#;
    let names = QueryExecutor::extract_bind_names(sql);
    // Both :NEW and :OLD should be extracted as they are regular bind variables here
    assert_eq!(
        names.len(),
        2,
        "Should have 2 bind variables, got: {:?}",
        names
    );
    assert!(
        names.iter().any(|n| n.to_uppercase() == "NEW"),
        "Should contain NEW, got: {:?}",
        names
    );
    assert!(
        names.iter().any(|n| n.to_uppercase() == "OLD"),
        "Should contain OLD, got: {:?}",
        names
    );
}

#[test]
fn test_is_create_trigger() {
    // Positive cases
    assert!(QueryExecutor::is_create_trigger(
        "CREATE TRIGGER trg_test BEFORE INSERT"
    ));
    assert!(QueryExecutor::is_create_trigger(
        "CREATE OR REPLACE TRIGGER trg_test"
    ));
    assert!(QueryExecutor::is_create_trigger(
        "create or replace trigger trg_test"
    ));
    assert!(QueryExecutor::is_create_trigger(
        "CREATE EDITIONABLE TRIGGER trg_test"
    ));
    assert!(QueryExecutor::is_create_trigger(
        "CREATE OR REPLACE EDITIONABLE TRIGGER trg_test"
    ));
    assert!(QueryExecutor::is_create_trigger(
        "CREATE NONEDITIONABLE TRIGGER trg_test"
    ));
    assert!(QueryExecutor::is_create_trigger(
        "  -- comment\n  CREATE OR REPLACE TRIGGER trg_test"
    ));
    assert!(QueryExecutor::is_create_trigger(
        "/* block comment */ CREATE TRIGGER trg_test"
    ));

    // Negative cases
    assert!(!QueryExecutor::is_create_trigger(
        "CREATE PROCEDURE proc_test"
    ));
    assert!(!QueryExecutor::is_create_trigger(
        "CREATE FUNCTION func_test"
    ));
    assert!(!QueryExecutor::is_create_trigger("CREATE PACKAGE pkg_test"));
    assert!(!QueryExecutor::is_create_trigger("CREATE TABLE tbl_test"));
    assert!(!QueryExecutor::is_create_trigger("SELECT * FROM dual"));
    assert!(!QueryExecutor::is_create_trigger("BEGIN :NEW := 1; END;"));
}

#[test]
fn test_compound_trigger_skips_new_old() {
    // COMPOUND TRIGGER should also skip :NEW and :OLD
    let sql = r#"CREATE OR REPLACE TRIGGER test_compound_trg
FOR UPDATE ON test_table
COMPOUND TRIGGER
  AFTER EACH ROW IS
  BEGIN
IF :NEW.status = 'ACTIVE' THEN
  INSERT INTO audit_table VALUES (:NEW.id, :audit_user, SYSDATE);
END IF;
  END AFTER EACH ROW;
END test_compound_trg;"#;
    let names = QueryExecutor::extract_bind_names(sql);
    // Only :audit_user should be extracted
    assert_eq!(
        names.len(),
        1,
        "Should have 1 bind variable, got: {:?}",
        names
    );
    assert!(
        names.iter().any(|n| n.to_uppercase() == "AUDIT_USER"),
        "Should contain AUDIT_USER, got: {:?}",
        names
    );
    assert!(
        !names.iter().any(|n| n.to_uppercase() == "NEW"),
        "Should NOT contain NEW, got: {:?}",
        names
    );
}

#[test]
fn test_connect_by_not_parsed_as_tool_command() {
    // CONNECT BY는 SQL 절이므로 Tool Command로 해석되지 않아야 함
    let sql = r#"INSERT INTO oqt_nm_t (id, grp, payload)
SELECT
  oqt_nm_seq.NEXTVAL,
  'G' || TO_CHAR(MOD(level, 7)),
  TO_CLOB('seed#' || level)
FROM dual
CONNECT BY level <= 20;"#;

    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    let tool_commands: Vec<&ScriptItem> = items
        .iter()
        .filter(|item| matches!(item, ScriptItem::ToolCommand(_)))
        .collect();

    assert_eq!(
        statements.len(),
        1,
        "Should be 1 statement, got: {:?}",
        statements
    );
    assert!(
        statements[0].contains("CONNECT BY"),
        "Statement should contain CONNECT BY"
    );
    assert!(
        tool_commands.is_empty(),
        "Should have no tool commands, got: {:?}",
        tool_commands
    );
}

#[test]
fn test_start_with_not_parsed_as_tool_command() {
    let sql = r#"SELECT
  node_id,
  parent_id,
  node_name,
  LEVEL AS lvl,
  SYS_CONNECT_BY_PATH(node_name, '/') AS path
FROM oqt_t_tree
START WITH parent_id IS NULL
CONNECT BY PRIOR node_id = parent_id
ORDER SIBLINGS BY node_id;"#;

    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    let tool_commands: Vec<&ScriptItem> = items
        .iter()
        .filter(|item| matches!(item, ScriptItem::ToolCommand(_)))
        .collect();

    assert_eq!(
        statements.len(),
        1,
        "Should be 1 statement, got: {:?}",
        statements
    );
    assert!(
        statements[0].contains("START WITH"),
        "Statement should contain START WITH, got: {}",
        statements[0]
    );
    assert!(
        statements[0].contains("ORDER SIBLINGS BY"),
        "Statement should contain ORDER SIBLINGS BY, got: {}",
        statements[0]
    );
    assert!(
        tool_commands.is_empty(),
        "Should have no tool commands, got: {:?}",
        tool_commands
    );
}

#[test]
fn test_print_prefix_word_not_parsed_as_print_tool_command() {
    let sql = "SELECT printable_col FROM dual;";

    let items = QueryExecutor::split_script_items(sql);
    let tool_commands: Vec<&ScriptItem> = items
        .iter()
        .filter(|item| matches!(item, ScriptItem::ToolCommand(_)))
        .collect();

    assert!(
        tool_commands.is_empty(),
        "PRINT prefix in SQL identifier should not become tool command: {:?}",
        tool_commands
    );
}

#[test]
fn test_print_command_rejects_unicode_confusable_keyword() {
    let sql = "PRıNT :b_var";

    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    let tool_commands: Vec<&ScriptItem> = items
        .iter()
        .filter(|item| matches!(item, ScriptItem::ToolCommand(_)))
        .collect();

    assert!(
        tool_commands.is_empty(),
        "Unicode confusable keyword must not be parsed as PRINT tool command: {:?}",
        tool_commands
    );
    assert_eq!(statements, vec![sql]);
}

#[test]
fn test_prompt_prefix_word_not_parsed_as_prompt_tool_command() {
    let sql = "SELECT prompt_col FROM dual;";

    let items = QueryExecutor::split_script_items(sql);
    let tool_commands: Vec<&ScriptItem> = items
        .iter()
        .filter(|item| matches!(item, ScriptItem::ToolCommand(_)))
        .collect();

    assert!(
        tool_commands.is_empty(),
        "PROMPT prefix in SQL identifier should not become tool command: {:?}",
        tool_commands
    );
}

#[test]
fn test_json_table_columns_not_parsed_as_column_tool_command() {
    let sql = r#"SELECT
  jt.order_id,
  jt.cust_name,
  jt.tier,
  it.sku,
  it.qty,
  it.price,
  (it.qty * it.price) AS line_amt
FROM oqt_t_json j
CROSS JOIN JSON_TABLE(
  j.payload,
  '$'
  COLUMNS (
    order_id   NUMBER       PATH '$.order_id',
    cust_name  VARCHAR2(50) PATH '$.customer.name',
    tier       VARCHAR2(20) PATH '$.customer.tier',
    NESTED PATH '$.items[*]'
    COLUMNS (
      sku   VARCHAR2(30) PATH '$.sku',
      qty   NUMBER       PATH '$.qty',
      price NUMBER       PATH '$.price'
    )
  )
) jt
CROSS APPLY (
  SELECT jt.sku, jt.qty, jt.price FROM dual
) it
ORDER BY jt.order_id, it.sku;"#;

    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    let tool_commands: Vec<&ScriptItem> = items
        .iter()
        .filter(|item| matches!(item, ScriptItem::ToolCommand(_)))
        .collect();

    assert_eq!(
        statements.len(),
        1,
        "Should be 1 statement, got: {:?}",
        statements
    );
    assert!(
        statements[0].contains("JSON_TABLE"),
        "Statement should contain JSON_TABLE, got: {}",
        statements[0]
    );
    assert!(
        statements[0].contains("COLUMNS ("),
        "Statement should contain COLUMNS clause, got: {}",
        statements[0]
    );
    assert!(
        tool_commands.is_empty(),
        "Should have no tool commands, got: {:?}",
        tool_commands
    );
}

#[test]
fn test_match_recognize_define_not_parsed_as_tool_command() {
    let sql = r#"SELECT *
FROM oqt_t_emp
MATCH_RECOGNIZE (
  PARTITION BY deptno
  ORDER BY hiredate, empno
  MEASURES
    FIRST(ename) AS start_name,
    LAST(ename)  AS end_name,
    COUNT(*)     AS run_len
  ONE ROW PER MATCH
  PATTERN (a b+)
  DEFINE
    b AS b.sal > PREV(b.sal)
);"#;

    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    let tool_commands: Vec<&ScriptItem> = items
        .iter()
        .filter(|item| matches!(item, ScriptItem::ToolCommand(_)))
        .collect();

    assert_eq!(
        statements.len(),
        1,
        "Should be 1 statement, got: {:?}",
        statements
    );
    assert!(
        statements[0].contains("MATCH_RECOGNIZE"),
        "Statement should contain MATCH_RECOGNIZE, got: {}",
        statements[0]
    );
    assert!(
        statements[0].contains("\n  DEFINE\n"),
        "Statement should contain DEFINE clause marker, got: {}",
        statements[0]
    );
    assert!(
        tool_commands.is_empty(),
        "Should have no tool commands, got: {:?}",
        tool_commands
    );
}

#[test]
fn test_match_recognize_inline_define_not_parsed_as_tool_command() {
    let sql = r#"SELECT *
FROM oqt_t_emp
MATCH_RECOGNIZE (
  PARTITION BY deptno
  ORDER BY hiredate, empno
  PATTERN (a b+)
  DEFINE b AS b.sal > PREV(b.sal)
);"#;

    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    let tool_commands: Vec<&ScriptItem> = items
        .iter()
        .filter(|item| matches!(item, ScriptItem::ToolCommand(_)))
        .collect();

    assert_eq!(
        statements.len(),
        1,
        "Should be 1 statement, got: {:?}",
        statements
    );
    assert!(
        statements[0].contains("DEFINE b AS b.sal > PREV(b.sal)"),
        "Statement should preserve MATCH_RECOGNIZE inline DEFINE clause, got: {}",
        statements[0]
    );
    assert!(
        tool_commands.is_empty(),
        "Inline DEFINE in MATCH_RECOGNIZE should not be parsed as tool command, got: {:?}",
        tool_commands
    );
}

#[test]
fn test_split_format_items_match_recognize_inline_define_not_parsed_as_tool_command() {
    let sql = r#"SELECT *
FROM oqt_t_emp
MATCH_RECOGNIZE (
  PARTITION BY deptno
  ORDER BY hiredate, empno
  PATTERN (a b+)
  DEFINE b AS b.sal > PREV(b.sal)
);"#;

    let items = QueryExecutor::split_format_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();
    let tool_commands: Vec<&FormatItem> = items
        .iter()
        .filter(|item| matches!(item, FormatItem::ToolCommand(_)))
        .collect();

    assert_eq!(
        statements.len(),
        1,
        "split_format_items should keep MATCH_RECOGNIZE inline DEFINE in one statement: {:?}",
        statements
    );
    assert!(
        statements[0].contains("DEFINE b AS b.sal > PREV(b.sal)"),
        "Formatted statement should preserve MATCH_RECOGNIZE inline DEFINE clause: {}",
        statements[0]
    );
    assert!(
        tool_commands.is_empty(),
        "split_format_items should not emit tool commands for MATCH_RECOGNIZE inline DEFINE, got: {:?}",
        tool_commands
    );
}

#[test]
fn test_alter_session_multiline_set_not_parsed_as_tool_command() {
    let sql = r#"ALTER SESSION
SET CURRENT_SCHEMA = APP_USER;
SELECT 1 FROM DUAL;"#;

    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    let tool_commands: Vec<&ScriptItem> = items
        .iter()
        .filter(|item| matches!(item, ScriptItem::ToolCommand(_)))
        .collect();

    assert_eq!(
        statements.len(),
        2,
        "Should split into ALTER SESSION + SELECT, got: {:?}",
        statements
    );
    assert!(
        statements[0].contains("ALTER SESSION")
            && statements[0].contains("SET CURRENT_SCHEMA = APP_USER"),
        "First statement should preserve ALTER SESSION SET clause, got: {}",
        statements[0]
    );
    assert!(
        tool_commands.is_empty(),
        "ALTER SESSION SET clause should not become tool command, got: {:?}",
        tool_commands
    );
}

#[test]
fn test_alter_session_q_quote_with_semicolon_not_split() {
    let sql = r#"ALTER SESSION SET TRACEFILE_IDENTIFIER = q'[trace;session]';
SELECT 1 FROM DUAL;"#;

    let items = QueryExecutor::split_script_items(sql);
    let statements = get_statements(&items);

    assert_eq!(
        statements.len(),
        2,
        "q-quote with semicolon inside ALTER SESSION should remain one statement, got: {:?}",
        statements
    );
    assert!(
        statements[0].contains(r#"q'[trace;session]'"#),
        "ALTER SESSION statement should preserve q-quoted value, got: {}",
        statements[0]
    );
}

#[test]
fn test_connect_tool_command_still_works() {
    // 실제 CONNECT Tool Command는 여전히 동작해야 함
    let sql = "CONNECT user/pass@localhost:1521/ORCL";
    let items = QueryExecutor::split_script_items(sql);

    let has_connect_command = items
        .iter()
        .any(|item| matches!(item, ScriptItem::ToolCommand(ToolCommand::Connect { .. })));
    assert!(
        has_connect_command,
        "CONNECT tool command should be recognized, got: {:?}",
        items
    );
}

#[test]
fn test_conn_tool_command_without_arguments_is_classified_as_tool_command() {
    let sql = "CONN";
    let items = QueryExecutor::split_script_items(sql);

    let has_connect_error = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::Unsupported {
                message,
                is_error: true,
                ..
            }) if message.contains("CONNECT requires connection string")
        )
    });

    assert!(
        has_connect_error,
        "bare CONN should be treated as CONNECT tool command error, got: {:?}",
        items
    );
}

#[test]
fn test_connect_tool_command_supports_at_sign_in_password() {
    let sql = "CONNECT user/p@ss@localhost:1521/ORCL";
    let items = QueryExecutor::split_script_items(sql);

    let has_expected_connect = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::Connect {
                username,
                password,
                host,
                port,
                service_name,
            }) if username == "user"
                && password == "p@ss"
                && host == "localhost"
                && *port == 1521
                && service_name == "ORCL"
        )
    });

    assert!(
        has_expected_connect,
        "CONNECT command with @ in password should parse correctly, got: {:?}",
        items
    );
}

#[test]
fn test_connect_tool_command_supports_slash_in_password() {
    let sql = "CONNECT user/pa/ss@localhost:1521/ORCL";
    let items = QueryExecutor::split_script_items(sql);

    let has_expected_connect = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::Connect {
                username,
                password,
                host,
                port,
                service_name,
            }) if username == "user"
                && password == "pa/ss"
                && host == "localhost"
                && *port == 1521
                && service_name == "ORCL"
        )
    });

    assert!(
        has_expected_connect,
        "CONNECT command with / in password should parse correctly, got: {:?}",
        items
    );
}

#[test]
fn test_column_new_value_tool_command_parsed() {
    let sql = "COLUMN col NEW_VALUE var";
    let items = QueryExecutor::split_script_items(sql);

    let has_column_command = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::ColumnNewValue {
                column_name,
                variable_name
            }) if column_name == "col" && variable_name == "var"
        )
    });
    assert!(
        has_column_command,
        "COLUMN NEW_VALUE tool command should be recognized, got: {:?}",
        items
    );
}

#[test]
fn test_column_without_new_value_is_unsupported() {
    let sql = "COLUMN col HEADING test";
    let items = QueryExecutor::split_script_items(sql);

    let has_unsupported_column = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::Unsupported { raw, .. })
                if raw.eq_ignore_ascii_case("COLUMN col HEADING test")
        )
    });
    assert!(
        has_unsupported_column,
        "Unsupported COLUMN command should be surfaced, got: {:?}",
        items
    );
}

#[test]
fn test_set_trimspool_command_parsed() {
    let sql = "SET TRIMSPOOL ON";
    let items = QueryExecutor::split_script_items(sql);

    let has_trimspool = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::SetTrimSpool { enabled: true })
        )
    });
    assert!(
        has_trimspool,
        "SET TRIMSPOOL should be recognized, got: {:?}",
        items
    );
}

#[test]
fn test_set_trimout_command_parsed() {
    let sql = "SET TRIMOUT OFF";
    let items = QueryExecutor::split_script_items(sql);

    let has_trimout = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::SetTrimOut { enabled: false })
        )
    });
    assert!(
        has_trimout,
        "SET TRIMOUT should be recognized, got: {:?}",
        items
    );
}

#[test]
fn test_set_sqlblanklines_command_parsed() {
    let sql = "SET SQLBLANKLINES ON";
    let items = QueryExecutor::split_script_items(sql);

    let has_sqlblanklines = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::SetSqlBlankLines { enabled: true })
        )
    });
    assert!(
        has_sqlblanklines,
        "SET SQLBLANKLINES should be recognized, got: {:?}",
        items
    );
}

#[test]
fn test_sqlblanklines_off_splits_top_level_statement_on_blank_line() {
    let sql = "SET SQLBLANKLINES OFF\nSELECT 1\n\nFROM dual;";
    let items = QueryExecutor::split_script_items(sql);

    assert_eq!(
        items.len(),
        3,
        "Blank line should split statement when SQLBLANKLINES is OFF"
    );
    assert!(matches!(
        &items[0],
        ScriptItem::ToolCommand(ToolCommand::SetSqlBlankLines { enabled: false })
    ));
    assert!(
        matches!(&items[1], ScriptItem::Statement(stmt) if stmt.eq_ignore_ascii_case("SELECT 1"))
    );
    assert!(
        matches!(&items[2], ScriptItem::Statement(stmt) if stmt.eq_ignore_ascii_case("FROM dual"))
    );
}

#[test]
fn test_default_sqlblanklines_on_keeps_blank_line_inside_statement() {
    let sql = "SELECT *\n\nFROM user_tables;";
    let items = QueryExecutor::split_script_items(sql);

    assert_eq!(
        items.len(),
        1,
        "Blank line should NOT split statement by default (SQLBLANKLINES ON)"
    );
    assert!(
        matches!(&items[0], ScriptItem::Statement(stmt) if stmt.contains("SELECT") && stmt.contains("FROM"))
    );
}

#[test]
fn test_sqlblanklines_on_keeps_blank_line_inside_top_level_statement() {
    let sql = "SET SQLBLANKLINES ON\nSELECT 1\n\nFROM dual;";
    let items = QueryExecutor::split_script_items(sql);

    assert_eq!(
        items.len(),
        2,
        "Expected SET command and one SELECT statement"
    );
    assert!(matches!(
        items[0],
        ScriptItem::ToolCommand(ToolCommand::SetSqlBlankLines { enabled: true })
    ));
    assert!(matches!(
        &items[1],
        ScriptItem::Statement(stmt) if stmt.contains("SELECT 1\n\nFROM dual")
    ));
}

#[test]
fn test_set_tab_command_parsed() {
    let sql = "SET TAB OFF";
    let items = QueryExecutor::split_script_items(sql);

    let has_tab = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::SetTab { enabled: false })
        )
    });
    assert!(has_tab, "SET TAB should be recognized, got: {:?}", items);
}

#[test]
fn test_set_define_single_quoted_char_parsed() {
    let sql = "SET DEFINE '^'";
    let items = QueryExecutor::split_script_items(sql);

    let has_set_define = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::SetDefine {
                enabled: true,
                define_char: Some('^')
            })
        )
    });
    assert!(
        has_set_define,
        "SET DEFINE '^' should be recognized, got: {:?}",
        items
    );
}

#[test]
fn test_set_define_single_quote_only_does_not_panic() {
    let sql = "SET DEFINE '";
    let items = QueryExecutor::split_script_items(sql);

    let has_quoted_define_char = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::SetDefine {
                enabled: true,
                define_char: Some('\'')
            })
        )
    });
    assert!(
        has_quoted_define_char,
        "SET DEFINE with single quote should be handled safely, got: {:?}",
        items
    );
}

#[test]
fn test_set_colsep_command_parsed() {
    let sql = "SET COLSEP ||";
    let items = QueryExecutor::split_script_items(sql);

    let has_colsep = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::SetColSep { separator }) if separator == "||"
        )
    });
    assert!(
        has_colsep,
        "SET COLSEP should be recognized, got: {:?}",
        items
    );
}

#[test]
fn test_set_null_command_parsed() {
    let sql = "SET NULL (null)";
    let items = QueryExecutor::split_script_items(sql);

    let has_set_null = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::SetNull { null_text }) if null_text == "(null)"
        )
    });
    assert!(
        has_set_null,
        "SET NULL should be recognized, got: {:?}",
        items
    );
}

#[test]
fn test_spool_file_command_parsed() {
    let sql = "SPOOL output.log";
    let items = QueryExecutor::split_script_items(sql);

    let has_spool_file = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::Spool { path: Some(path), append: false })
                if path == "output.log"
        )
    });
    assert!(
        has_spool_file,
        "SPOOL file should be recognized, got: {:?}",
        items
    );
}

#[test]
fn test_spool_append_command_parsed() {
    let sql = "SPOOL APPEND";
    let items = QueryExecutor::split_script_items(sql);

    let has_spool_append = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::Spool {
                path: None,
                append: true
            })
        )
    });
    assert!(
        has_spool_append,
        "SPOOL APPEND should be recognized, got: {:?}",
        items
    );
}

#[test]
fn test_spool_off_command_parsed() {
    let sql = "SPOOL OFF";
    let items = QueryExecutor::split_script_items(sql);

    let has_spool_off = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::Spool {
                path: None,
                append: false
            })
        )
    });
    assert!(
        has_spool_off,
        "SPOOL OFF should be recognized, got: {:?}",
        items
    );
}

#[test]
fn test_break_on_command_parsed() {
    let sql = "BREAK ON deptno";
    let items = QueryExecutor::split_script_items(sql);

    let has_break_on = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::BreakOn { column_name }) if column_name == "deptno"
        )
    });
    assert!(
        has_break_on,
        "BREAK ON should be recognized, got: {:?}",
        items
    );
}

#[test]
fn test_break_off_command_parsed() {
    let sql = "BREAK OFF";
    let items = QueryExecutor::split_script_items(sql);

    let has_break_off = items
        .iter()
        .any(|item| matches!(item, ScriptItem::ToolCommand(ToolCommand::BreakOff)));
    assert!(
        has_break_off,
        "BREAK OFF should be recognized, got: {:?}",
        items
    );
}

#[test]
fn test_compute_sum_command_parsed() {
    let sql = "COMPUTE SUM";
    let items = QueryExecutor::split_script_items(sql);

    let has_compute_sum = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::Compute {
                mode: crate::db::ComputeMode::Sum,
                of_column: None,
                on_column: None
            })
        )
    });
    assert!(
        has_compute_sum,
        "COMPUTE SUM should be recognized, got: {:?}",
        items
    );
}

#[test]
fn test_compute_count_command_parsed() {
    let sql = "COMPUTE COUNT";
    let items = QueryExecutor::split_script_items(sql);

    let has_compute_count = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::Compute {
                mode: crate::db::ComputeMode::Count,
                of_column: None,
                on_column: None
            })
        )
    });
    assert!(
        has_compute_count,
        "COMPUTE COUNT should be recognized, got: {:?}",
        items
    );
}

#[test]
fn test_compute_off_command_parsed() {
    let sql = "COMPUTE OFF";
    let items = QueryExecutor::split_script_items(sql);

    let has_compute_off = items
        .iter()
        .any(|item| matches!(item, ScriptItem::ToolCommand(ToolCommand::ComputeOff)));
    assert!(
        has_compute_off,
        "COMPUTE OFF should be recognized, got: {:?}",
        items
    );
}

#[test]
fn test_compute_count_of_on_command_parsed() {
    let sql = "COMPUTE COUNT OF id ON grp";
    let items = QueryExecutor::split_script_items(sql);

    let has_compute_count_of_on = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::Compute {
                mode: crate::db::ComputeMode::Count,
                of_column: Some(of_col),
                on_column: Some(on_col)
            }) if of_col == "id" && on_col == "grp"
        )
    });
    assert!(
        has_compute_count_of_on,
        "COMPUTE COUNT OF ... ON ... should be recognized, got: {:?}",
        items
    );
}

#[test]
fn test_compute_sum_of_on_command_parsed() {
    let sql = "COMPUTE SUM OF val ON grp";
    let items = QueryExecutor::split_script_items(sql);

    let has_compute_sum_of_on = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::Compute {
                mode: crate::db::ComputeMode::Sum,
                of_column: Some(of_col),
                on_column: Some(on_col)
            }) if of_col == "val" && on_col == "grp"
        )
    });
    assert!(
        has_compute_sum_of_on,
        "COMPUTE SUM OF ... ON ... should be recognized, got: {:?}",
        items
    );
}

#[test]
fn test_clear_breaks_computes_parsed() {
    let sql = "CLEAR BREAKS CLEAR COMPUTES";
    let items = QueryExecutor::split_script_items(sql);
    let has_clear_both = items.iter().any(|item| {
        matches!(
            item,
            ScriptItem::ToolCommand(ToolCommand::ClearBreaksComputes)
        )
    });
    assert!(
        has_clear_both,
        "CLEAR BREAKS CLEAR COMPUTES should be recognized, got: {:?}",
        items
    );
}

#[test]
fn test_clear_breaks_parsed() {
    let sql = "CLEAR BREAKS";
    let items = QueryExecutor::split_script_items(sql);
    let has_clear_breaks = items
        .iter()
        .any(|item| matches!(item, ScriptItem::ToolCommand(ToolCommand::ClearBreaks)));
    assert!(
        has_clear_breaks,
        "CLEAR BREAKS should be recognized, got: {:?}",
        items
    );
}

#[test]
fn test_clear_computes_parsed() {
    let sql = "CLEAR COMPUTES";
    let items = QueryExecutor::split_script_items(sql);
    let has_clear_computes = items
        .iter()
        .any(|item| matches!(item, ScriptItem::ToolCommand(ToolCommand::ClearComputes)));
    assert!(
        has_clear_computes,
        "CLEAR COMPUTES should be recognized, got: {:?}",
        items
    );
}

#[test]
fn test_accept_prompt_with_utf8_prefix_before_prompt_keyword() {
    let sql = "ACCEPT v ıprompt '메시지'";
    let items = QueryExecutor::split_script_items(sql);

    let parsed = items.iter().find_map(|item| match item {
        ScriptItem::ToolCommand(ToolCommand::Accept { name, prompt }) => {
            Some((name.as_str(), prompt.as_deref()))
        }
        _ => None,
    });

    assert_eq!(
        parsed,
        Some(("v", Some("메시지"))),
        "UTF-8 text before PROMPT should not break prompt slicing: {:?}",
        items
    );
}

#[test]
fn test_prompt_command_preserves_trailing_semicolon_text() {
    let sql = "PROMPT hello;";
    let items = QueryExecutor::split_script_items(sql);

    let parsed = items.iter().find_map(|item| match item {
        ScriptItem::ToolCommand(ToolCommand::Prompt { text }) => Some(text.as_str()),
        _ => None,
    });

    assert_eq!(
        parsed,
        Some("hello;"),
        "PROMPT payload should preserve trailing semicolon text: {:?}",
        items
    );
}

#[test]
fn test_trigger_with_declare_and_multiline_header() {
    // TRIGGER 헤더에서 이벤트 타입(INSERT)이 별도 행에 있고,
    // DECLARE 블록과 q-quote 내의 가짜 키워드가 포함된 경우
    let sql = r#"CREATE OR REPLACE TRIGGER oqt_nm_trg BEFORE
INSERT
ON oqt_nm_t
FOR EACH ROW
DECLARE
v VARCHAR2 (2000);
BEGIN
v := q '[TRG: fake tokens END; / ; BEGIN CASE LOOP IF THEN ELSE]' || ' + '' ; ''';
:new.payload := NVL (:new.payload, TO_CLOB ('')) || CHR (10) || v;
END;"#;

    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        statements.len(),
        1,
        "Should be 1 statement, got: {:?}",
        statements
    );
    assert!(statements[0].contains("CREATE OR REPLACE TRIGGER oqt_nm_trg"));
    assert!(statements[0].contains("DECLARE"));
    assert!(statements[0].contains("END"));
}

#[test]
fn test_nq_quote_string_parsing() {
    // Test nq'[...]' (National Character q-quoted string) parsing
    let sql = r#"SELECT nq'[한글 문자열]' FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        statements.len(),
        1,
        "Should be 1 statement, got: {:?}",
        statements
    );
    assert!(
        statements[0].contains("nq'[한글 문자열]'"),
        "Statement should contain nq'[...]', got: {}",
        statements[0]
    );
}

#[test]
fn test_nq_quote_with_semicolon_inside() {
    // Test that semicolons inside nq'...' don't split the statement
    let sql = r#"SELECT nq'[text with ; semicolon]' FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        statements.len(),
        1,
        "Should be 1 statement, got: {:?}",
        statements
    );
    assert!(
        statements[0].contains("nq'[text with ; semicolon]'"),
        "Statement should preserve semicolon inside nq'...', got: {}",
        statements[0]
    );
}

#[test]
fn test_split_script_items_dollar_quote_keeps_begin_end_inside_string() {
    let sql = r#"SELECT $$BEGIN END$$ AS text FROM dual;
SELECT 2 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "dollar-quote content should not affect block depth or statement split: {:?}",
        stmts
    );
    assert!(
        stmts[0].contains("$$BEGIN END$$"),
        "first statement should preserve dollar-quoted text, got: {}",
        stmts[0]
    );
    assert!(
        stmts[1].contains("SELECT 2 FROM dual"),
        "second statement should remain independent, got: {}",
        stmts[1]
    );
}

#[test]
fn test_split_script_items_tagged_dollar_quote_ignores_semicolon_and_parenthesis() {
    let sql = r#"SELECT $tag$foo(bar); END IF;$tag$ AS text FROM dual;
SELECT 3 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "tagged dollar-quote should ignore internal semicolon/paren keywords: {:?}",
        stmts
    );
    assert!(
        stmts[0].contains("$tag$foo(bar); END IF;$tag$"),
        "first statement should keep tagged dollar-quote intact, got: {}",
        stmts[0]
    );
    assert!(
        stmts[1].contains("SELECT 3 FROM dual"),
        "second statement should be split at top-level semicolon, got: {}",
        stmts[1]
    );
}

#[test]
fn test_nq_quote_different_delimiters() {
    // Test nq'...' with different delimiters: (), {}, <>
    let sql1 = r#"SELECT nq'(parentheses)' FROM dual"#;
    let sql2 = r#"SELECT nq'{braces}' FROM dual"#;
    let sql3 = r#"SELECT nq'<angle brackets>' FROM dual"#;
    let sql4 = r#"SELECT Nq'!custom delimiter!' FROM dual"#;

    let items1 = QueryExecutor::split_script_items(sql1);
    let items2 = QueryExecutor::split_script_items(sql2);
    let items3 = QueryExecutor::split_script_items(sql3);
    let items4 = QueryExecutor::split_script_items(sql4);

    assert_eq!(items1.len(), 1, "nq'(...)' should parse as 1 statement");
    assert_eq!(items2.len(), 1, "nq'{{...}}' should parse as 1 statement");
    assert_eq!(items3.len(), 1, "nq'<...>' should parse as 1 statement");
    assert_eq!(items4.len(), 1, "Nq'!...!' should parse as 1 statement");
}

#[test]
fn test_nq_quote_case_insensitive() {
    // Test that NQ, Nq, nQ, nq all work
    let sql1 = r#"SELECT nq'[lower]' FROM dual"#;
    let sql2 = r#"SELECT NQ'[upper]' FROM dual"#;
    let sql3 = r#"SELECT Nq'[mixed1]' FROM dual"#;
    let sql4 = r#"SELECT nQ'[mixed2]' FROM dual"#;

    let items1 = QueryExecutor::split_script_items(sql1);
    let items2 = QueryExecutor::split_script_items(sql2);
    let items3 = QueryExecutor::split_script_items(sql3);
    let items4 = QueryExecutor::split_script_items(sql4);

    assert_eq!(items1.len(), 1, "nq'...' should parse correctly");
    assert_eq!(items2.len(), 1, "NQ'...' should parse correctly");
    assert_eq!(items3.len(), 1, "Nq'...' should parse correctly");
    assert_eq!(items4.len(), 1, "nQ'...' should parse correctly");
}

#[test]
fn test_nq_quote_in_plsql_block() {
    // Test nq'...' inside PL/SQL block
    let sql = r#"DECLARE
v_text VARCHAR2(100);
BEGIN
v_text := nq'[Hello; World; End;]';
DBMS_OUTPUT.PUT_LINE(v_text);
END;"#;

    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        statements.len(),
        1,
        "Should be 1 PL/SQL block, got: {:?}",
        statements
    );
    assert!(
        statements[0].contains("nq'[Hello; World; End;]'"),
        "PL/SQL block should contain nq'...' string intact"
    );
}

#[test]
fn test_nq_quote_mixed_with_q_quote() {
    // Test both nq'...' and q'...' in same statement
    let sql = r#"SELECT q'[regular q-quote]', nq'[national q-quote]' FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        statements.len(),
        1,
        "Should be 1 statement with both q'...' and nq'...'"
    );
    assert!(statements[0].contains("q'[regular q-quote]'"));
    assert!(statements[0].contains("nq'[national q-quote]'"));
}

#[test]
fn test_nq_quote_bind_variable_extraction() {
    // Test that bind variables inside nq'...' are NOT extracted
    let sql = r#"SELECT nq'[:not_a_bind]', :real_bind FROM dual"#;
    let names = QueryExecutor::extract_bind_names(sql);

    assert_eq!(
        names.len(),
        1,
        "Should have 1 bind variable, got: {:?}",
        names
    );
    assert!(
        names.iter().any(|n| n.to_uppercase() == "REAL_BIND"),
        "Should contain REAL_BIND, got: {:?}",
        names
    );
    assert!(
        !names.iter().any(|n| n.to_uppercase() == "NOT_A_BIND"),
        "Should NOT contain NOT_A_BIND (inside nq'...'), got: {:?}",
        names
    );
}

#[test]
fn test_hint_in_select_statement() {
    // Test that hints are preserved in statements
    let sql = "SELECT /*+ FULL(t) PARALLEL(t,4) */ * FROM table t;";
    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(statements.len(), 1, "Should be 1 statement");
    assert!(
        statements[0].contains("/*+ FULL(t) PARALLEL(t,4) */"),
        "Hint should be preserved in statement, got: {}",
        statements[0]
    );
}

#[test]
fn test_hint_not_split_statement() {
    // Hint should not cause statement splitting
    let sql = "SELECT /*+ INDEX(t idx1) */ col1, col2 FROM table t WHERE id = 1;";
    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(statements.len(), 1, "Should be 1 statement with hint");
    assert!(statements[0].contains("/*+"));
    assert!(statements[0].contains("*/"));
}

#[test]
fn test_date_literal_parsing() {
    // DATE literals should be parsed correctly
    let sql = "SELECT DATE '2024-01-01' FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(statements.len(), 1, "Should be 1 statement");
    assert!(
        statements[0].contains("DATE '2024-01-01'"),
        "DATE literal should be preserved"
    );
}

#[test]
fn test_timestamp_literal_parsing() {
    // TIMESTAMP literals should be parsed correctly
    let sql = "SELECT TIMESTAMP '2024-01-01 12:30:00' FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(statements.len(), 1, "Should be 1 statement");
    assert!(
        statements[0].contains("TIMESTAMP '2024-01-01 12:30:00'"),
        "TIMESTAMP literal should be preserved"
    );
}

#[test]
fn test_interval_literal_parsing() {
    // INTERVAL literals should be parsed correctly
    let sql = "SELECT INTERVAL '5' DAY FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(statements.len(), 1, "Should be 1 statement");
    assert!(
        statements[0].contains("INTERVAL '5' DAY"),
        "INTERVAL literal should be preserved"
    );
}

#[test]
fn test_interval_year_to_month_literal() {
    // INTERVAL YEAR TO MONTH literals
    let sql = "SELECT INTERVAL '1-6' YEAR TO MONTH FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(statements.len(), 1, "Should be 1 statement");
    assert!(statements[0].contains("INTERVAL '1-6' YEAR TO MONTH"));
}

#[test]
fn test_multiple_datetime_literals() {
    // Multiple datetime literals in one statement
    let sql =
        "SELECT DATE '2024-01-01', TIMESTAMP '2024-01-01 12:00:00', INTERVAL '1' DAY FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(statements.len(), 1, "Should be 1 statement");
    assert!(statements[0].contains("DATE '2024-01-01'"));
    assert!(statements[0].contains("TIMESTAMP '2024-01-01 12:00:00'"));
    assert!(statements[0].contains("INTERVAL '1' DAY"));
}

#[test]
fn test_flashback_query_parsing() {
    // FLASHBACK query with AS OF should parse correctly
    let sql = "SELECT * FROM employees AS OF TIMESTAMP (SYSDATE - 1/24);";
    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(statements.len(), 1, "Should be 1 statement");
    assert!(statements[0].contains("AS OF TIMESTAMP"));
}

#[test]
fn test_fetch_first_rows_parsing() {
    // Oracle 12c+ FETCH FIRST clause
    let sql = "SELECT * FROM employees ORDER BY salary DESC FETCH FIRST 10 ROWS ONLY;";
    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(statements.len(), 1, "Should be 1 statement");
    assert!(statements[0].contains("FETCH FIRST 10 ROWS ONLY"));
}

#[test]
fn test_offset_fetch_parsing() {
    // OFFSET with FETCH
    let sql = "SELECT * FROM employees ORDER BY id OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY;";
    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(statements.len(), 1, "Should be 1 statement");
    assert!(statements[0].contains("OFFSET 10 ROWS"));
    assert!(statements[0].contains("FETCH NEXT 5 ROWS ONLY"));
}

#[test]
fn test_listagg_within_group() {
    // LISTAGG with WITHIN GROUP
    let sql = "SELECT department_id, LISTAGG(employee_name, ', ') WITHIN GROUP (ORDER BY employee_name) AS employees FROM emp GROUP BY department_id;";
    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(statements.len(), 1, "Should be 1 statement");
    assert!(statements[0].contains("LISTAGG"));
    assert!(statements[0].contains("WITHIN GROUP"));
}

#[test]
fn test_keep_dense_rank() {
    // KEEP (DENSE_RANK FIRST/LAST ORDER BY)
    let sql = "SELECT department_id, MAX(salary) KEEP (DENSE_RANK FIRST ORDER BY hire_date) AS first_salary FROM employees GROUP BY department_id;";
    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(statements.len(), 1, "Should be 1 statement");
    assert!(statements[0].contains("KEEP (DENSE_RANK FIRST ORDER BY hire_date)"));
}

#[test]
fn test_pivot_query() {
    // PIVOT query
    let sql = r#"SELECT * FROM sales_data
PIVOT (
SUM(amount)
FOR month IN ('JAN', 'FEB', 'MAR')
);"#;
    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(statements.len(), 1, "Should be 1 statement");
    assert!(statements[0].contains("PIVOT"));
    assert!(statements[0].contains("SUM(amount)"));
}

#[test]
fn test_sample_query() {
    // SAMPLE clause
    let sql = "SELECT * FROM large_table SAMPLE (10) SEED (42);";
    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(statements.len(), 1, "Should be 1 statement");
    assert!(statements[0].contains("SAMPLE (10)"));
    assert!(statements[0].contains("SEED (42)"));
}

#[test]
fn test_for_update_skip_locked() {
    // FOR UPDATE with SKIP LOCKED
    let sql = "SELECT * FROM jobs WHERE status = 'PENDING' FOR UPDATE SKIP LOCKED;";
    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(statements.len(), 1, "Should be 1 statement");
    assert!(statements[0].contains("FOR UPDATE SKIP LOCKED"));
}

#[test]
fn test_analytic_window_frame() {
    // Analytic function with ROWS BETWEEN
    let sql = "SELECT employee_id, salary, SUM(salary) OVER (ORDER BY hire_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_total FROM employees;";
    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(statements.len(), 1, "Should be 1 statement");
    assert!(statements[0].contains("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"));
}

#[test]
fn test_type_body_with_q_quoted_string() {
    // TYPE BODY with q-quoted string containing special characters
    // The q'[...]' syntax allows embedding ; / -- /* */ without escaping
    let sql = r#"CREATE OR REPLACE TYPE BODY oqt_obj AS
  MEMBER FUNCTION peek RETURN VARCHAR2 IS
  BEGIN
RETURN 'peek:'||SUBSTR(txt,1,40)||q'[ | tokens: END; / ; /* */ -- ]';
  END;
END;
/
SHOW ERRORS TYPE BODY oqt_obj"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    // Should have exactly 1 statement (the TYPE BODY)
    // SHOW ERRORS is a tool command, not a statement
    assert_eq!(
        stmts.len(),
        1,
        "Should have 1 statement (TYPE BODY), got {} statements: {:?}",
        stmts.len(),
        stmts
    );

    // The statement should contain the full TYPE BODY
    assert!(
        stmts[0].contains("CREATE OR REPLACE TYPE BODY oqt_obj"),
        "Should contain CREATE OR REPLACE TYPE BODY"
    );
    assert!(
        stmts[0].contains("MEMBER FUNCTION peek"),
        "Should contain MEMBER FUNCTION"
    );
    assert!(
        stmts[0].contains(r#"q'[ | tokens: END; / ; /* */ -- ]'"#),
        "Should contain q-quoted string intact"
    );
    assert!(
        stmts[0].ends_with("END") || stmts[0].ends_with("END;"),
        "Should end with END or END;, got: {}",
        &stmts[0][stmts[0].len().saturating_sub(50)..]
    );

    // Verify SHOW ERRORS is parsed as tool command
    let tool_commands: Vec<&ToolCommand> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::ToolCommand(cmd) => Some(cmd),
            _ => None,
        })
        .collect();
    assert_eq!(
        tool_commands.len(),
        1,
        "Should have 1 tool command (SHOW ERRORS)"
    );
}

#[test]
fn test_package_body_with_comments_does_not_break_depth() {
    let sql = r#"CREATE OR REPLACE PACKAGE BODY oqt_comment_pkg AS
  /* package-level comment with keywords: BEGIN END IF LOOP */
  PROCEDURE p_test (p_id NUMBER) IS
    /* procedure comment */
  BEGIN
    /* begin-block comment */
    NULL;
  END p_test;

  -- another comment mentioning END;
  PROCEDURE p_test2 IS
  BEGIN
    NULL;
  END p_test2;
END oqt_comment_pkg;
/
SELECT 1 FROM dual;"#;

    let items = QueryExecutor::split_script_items(sql);
    let statements: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            ScriptItem::Statement(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        statements.len(),
        2,
        "Comments should not affect depth/splitting; expected package body + select, got: {:?}",
        statements
    );
    assert!(
        statements[0].contains("CREATE OR REPLACE PACKAGE BODY oqt_comment_pkg"),
        "First statement should be package body"
    );
    assert!(
        statements[0].contains("END oqt_comment_pkg"),
        "Package body should end correctly"
    );
    assert!(
        statements[1].contains("SELECT 1 FROM dual"),
        "Second statement should be trailing SELECT"
    );
}

#[test]
fn test_line_block_depths_increase_for_if_and_case() {
    let sql = r#"BEGIN
IF v_flag = 'Y' THEN
CASE
WHEN v_num = 1 THEN
NULL;
ELSE
NULL;
END CASE;
END IF;
END;"#;

    let depths = QueryExecutor::line_block_depths(sql);
    let expected = vec![0, 1, 2, 3, 4, 3, 4, 2, 1, 0];

    assert_eq!(depths, expected, "IF/CASE depth tracking mismatch");
}

#[test]
fn test_line_block_depths_if_with_begin_and_multiple_case_columns() {
    let sql = r#"if 1=1 then
    begin
        select
            case
                when 1=1 then '1'
                else ''
            end,
            case
                when 1=1 then '1'
                else ''
            end
        from dual;
end
end if;"#;

    let depths = QueryExecutor::line_block_depths(sql);
    let expected = vec![0, 1, 2, 2, 3, 3, 2, 2, 3, 3, 2, 2, 1, 0];

    assert_eq!(
        depths, expected,
        "IF/BEGIN + multi-CASE depth tracking mismatch"
    );
}

#[test]
fn test_line_block_depths_mysql_elseif_is_pre_dedented() {
    let sql = r#"IF score >= 90 THEN
  SET grade = 'A';
ELSEIF score >= 80 THEN
  SET grade = 'B';
ELSE
  SET grade = 'C';
END IF;"#;

    let depths = QueryExecutor::line_block_depths(sql);
    let expected = vec![0, 1, 0, 1, 0, 1, 0];

    assert_eq!(
        depths, expected,
        "ELSEIF depth tracking should match ELSE/ELSIF pre-dedent semantics"
    );
}

#[test]
fn test_split_script_items_mysql_elseif_block_remains_single_statement() {
    let sql = r#"IF score >= 90 THEN
  SET grade = 'A';
ELSEIF score >= 80 THEN
  SET grade = 'B';
ELSE
  SET grade = 'C';
END IF;"#;

    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(
        stmts.len(),
        1,
        "IF/ELSEIF/ELSE/END IF block should be a single statement"
    );
}

#[test]
fn test_select_with_case_expressions_separated_by_plus() {
    let sql = "SELECT CASE WHEN a=1 THEN 1 ELSE 0 END + CASE WHEN b=2 THEN 1 ELSE 0 END FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_select_with_case_expressions_separated_by_minus_and_division() {
    let sql = "SELECT CASE WHEN a=1 THEN 1 ELSE 0 END - CASE WHEN b=2 THEN 1 ELSE 0 END / CASE WHEN c=3 THEN 1 ELSE 0 END FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "Should have 1 statement, got: {:?}", stmts);
}

#[test]
fn test_line_block_depths_if_with_case_expressions_separated_by_plus() {
    let sql = r#"if 1=1 then
    begin
        select
            case
                when 1=1 then 1
                else 0
            end + case
                when 1=1 then 1
                else 0
            end
        from dual;
end
end if;"#;

    let depths = QueryExecutor::line_block_depths(sql);
    let expected = vec![0, 1, 2, 2, 3, 3, 2, 3, 3, 2, 2, 1, 0];

    assert_eq!(
        depths, expected,
        "IF/BEGIN + arithmetic CASE depth tracking mismatch"
    );
}

#[test]
fn test_split_script_items_end_comment_if_continuation() {
    let sql = r#"BEGIN
  IF 1 = 1 THEN
    NULL;
  END /* keep continuation */ IF;
END;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "END comment IF should remain one statement");
}

#[test]
fn test_split_script_items_repeat_block() {
    let sql = r#"DECLARE
  v_count NUMBER := 0;
BEGIN
  REPEAT
    v_count := v_count + 1;
  UNTIL v_count >= 3
  END REPEAT;
END;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "REPEAT block should be one statement");
}

#[test]
fn test_split_script_items_repeat_block_with_end_repeat_on_next_line() {
    let sql = r#"DECLARE
  v_count NUMBER := 0;
BEGIN
  REPEAT
    v_count := v_count + 1;
  UNTIL v_count >= 3
  END
  REPEAT;
END;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "REPEAT block should be one statement");
    assert!(stmts[0].contains("END") && stmts[0].contains("REPEAT"));
}

#[test]
fn test_split_script_items_pipelined_function() {
    let sql = r#"CREATE OR REPLACE FUNCTION stream_numbers(
  p_limit NUMBER
) RETURN SYS.ODCINUMBERLIST PIPELINED
IS
BEGIN
  NULL;
END;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(stmts.len(), 1, "PIPELINED function should be one statement");
    assert!(stmts[0].contains("PIPELINED"));
}

#[test]
fn test_line_block_depths_increase_for_repeat_loop() {
    let sql = r#"BEGIN
  REPEAT
    NULL;
  UNTIL i > 1
  END REPEAT;
END;"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let expected = vec![0, 1, 2, 2, 1, 0];
    assert_eq!(depths, expected, "REPEAT depth tracking mismatch");
}

#[test]
fn test_line_block_depths_with_split_end_repeat() {
    let sql = r#"BEGIN
  REPEAT
    NULL;
  UNTIL i > 1
  END
  REPEAT;
END;"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let expected = vec![0, 1, 2, 2, 1, 1, 0];
    assert_eq!(depths, expected, "REPEAT depth tracking mismatch");
}

#[test]
fn test_line_block_depths_increase_for_while_loop() {
    let sql = r#"BEGIN
  WHILE i < 5 LOOP
    i := i + 1;
  END LOOP;
END;"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let expected = vec![0, 1, 2, 1, 0];
    assert_eq!(depths, expected, "WHILE LOOP depth tracking mismatch");
}

#[test]
fn test_line_block_depths_with_split_end_while() {
    let sql = r#"BEGIN
  WHILE i < 5 LOOP
    i := i + 1;
  END
  WHILE;
END;"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let expected = vec![0, 1, 2, 1, 1, 0];
    assert_eq!(depths, expected, "END WHILE depth tracking mismatch");
}

#[test]
fn test_line_block_depths_while_do_loop() {
    let sql = r#"BEGIN
  WHILE i < 5 DO
    i := i + 1;
  END WHILE;
END;"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let expected = vec![0, 1, 2, 1, 0];
    assert_eq!(depths, expected, "WHILE DO depth tracking mismatch");
}

#[test]
fn test_line_block_depths_with_split_end_while_do() {
    let sql = r#"BEGIN
  WHILE i < 5 DO
    i := i + 1;
  END
  WHILE;
END;"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let expected = vec![0, 1, 2, 1, 1, 0];
    assert_eq!(depths, expected, "split END WHILE after WHILE DO mismatch");
}

#[test]
fn test_line_block_depths_end_while_does_not_arm_new_do_block() {
    let sql = r#"BEGIN
  WHILE i < 5 DO
    i := i + 1;
  END WHILE;
  DO 1;
END;"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let expected = vec![0, 1, 2, 1, 1, 0];
    assert_eq!(
        depths, expected,
        "END WHILE must not set pending WHILE-DO state for the next DO"
    );
}

#[test]
fn test_line_block_depths_for_do_loop() {
    let sql = r#"BEGIN
  FOR i IN 1..3 DO
    NULL;
  END FOR;
END;"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let expected = vec![0, 1, 2, 1, 0];
    assert_eq!(depths, expected, "FOR DO depth tracking mismatch");
}

#[test]
fn test_line_block_depths_with_split_end_for_do() {
    let sql = r#"BEGIN
  FOR i IN 1..3 DO
    NULL;
  END
  FOR;
END;"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let expected = vec![0, 1, 2, 1, 1, 0];
    assert_eq!(depths, expected, "split END FOR after FOR DO mismatch");
}

#[test]
fn test_line_block_depths_end_for_does_not_arm_new_do_block() {
    let sql = r#"BEGIN
  FOR i IN 1..3 DO
    NULL;
  END FOR;
  DO 1;
END;"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let expected = vec![0, 1, 2, 1, 1, 0];
    assert_eq!(
        depths, expected,
        "END FOR must not set pending FOR-DO state for the next DO"
    );
}

#[test]
fn test_line_block_depths_preserve_pending_end_across_blank_line() {
    let sql = r#"BEGIN
  WHILE i < 5 LOOP
    i := i + 1;
  END

  WHILE;
END;"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let expected = vec![0, 1, 2, 1, 2, 1, 0];
    assert_eq!(
        depths, expected,
        "blank line between END and WHILE should keep END pending"
    );
}

#[test]
fn test_line_block_depths_preserve_pending_end_across_comment_line() {
    let sql = r#"BEGIN
  WHILE i < 5 LOOP
    i := i + 1;
  END
  -- keep END pending for next keyword
  WHILE;
END;"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let expected = vec![0, 1, 2, 1, 2, 1, 0];
    assert_eq!(
        depths, expected,
        "comment line between END and WHILE should keep END pending"
    );
}

#[test]
fn test_line_block_depths_with_for_update_clause() {
    let sql = r#"SELECT id, status
FROM jobs
WHERE status = 'PENDING'
FOR UPDATE SKIP LOCKED;"#;
    let depths = QueryExecutor::line_block_depths(sql);
    assert_eq!(depths, vec![0, 0, 0, 0]);
}

#[test]
fn test_line_block_depths_for_update_inside_block_does_not_arm_do_block() {
    let sql = r#"BEGIN
  SELECT id
  INTO v_id
  FROM jobs
  WHERE status = 'PENDING'
  FOR UPDATE;
  DO 1;
END;"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let expected = vec![0, 1, 1, 1, 1, 1, 1, 0];
    assert_eq!(
        depths, expected,
        "FOR UPDATE inside a block must not leave pending FOR-DO state for a later DO"
    );
}

#[test]
fn test_split_script_items_trigger_for_each_row_does_not_arm_for_do() {
    let sql = r#"CREATE OR REPLACE TRIGGER trg_jobs
BEFORE INSERT ON jobs
FOR EACH ROW
BEGIN
  DO 1;
END;
/
SELECT 1 FROM dual;"#;

    let items = QueryExecutor::split_script_items(sql);
    let statements = get_statements(&items);

    assert_eq!(
        statements.len(),
        2,
        "FOR EACH ROW in trigger header must not arm FOR ... DO state: {:?}",
        statements
    );
    assert!(
        statements[0].contains("CREATE OR REPLACE TRIGGER"),
        "first statement should be the trigger body"
    );
    assert!(
        statements[1].contains("SELECT 1 FROM dual"),
        "second statement should remain independently split"
    );
}

#[test]
fn test_line_block_depths_with_with_clause_prefixed_by_hint_comment() {
    let sql = r#"/*+ leading_optimizer_hint */ WITH cte AS (
  SELECT 1 AS id FROM dual
)
SELECT * FROM cte;"#;
    let lines: Vec<&str> = sql.lines().collect();
    let depths = QueryExecutor::line_block_depths(sql);

    let mut with_idx = None;
    let mut cte_select_idx = None;
    let mut main_select_idx = None;

    for (idx, line) in lines.iter().enumerate() {
        if with_idx.is_none() && line.to_uppercase().starts_with("/*") {
            if line.to_uppercase().contains(" WITH ") {
                with_idx = Some(idx);
            }
        } else if with_idx.is_none()
            && line.to_uppercase().trim_start().starts_with("WITH ")
            && !line.trim_start().starts_with("--")
        {
            with_idx = Some(idx);
        }

        if line.to_uppercase().trim_start().starts_with("SELECT ") && cte_select_idx.is_none() {
            cte_select_idx = Some(idx);
        } else if line
            .to_uppercase()
            .trim_start()
            .starts_with("SELECT * FROM CTE")
            && main_select_idx.is_none()
        {
            main_select_idx = Some(idx);
        }
    }

    let with_idx = with_idx.expect("expected hint-prefixed WITH line");
    let cte_select_idx = cte_select_idx.expect("expected CTE SELECT");
    let main_select_idx = main_select_idx.expect("expected main SELECT after CTE");

    assert!(
        with_idx + 1 < lines.len(),
        "WITH line should have body line"
    );
    assert!(
        depths[with_idx + 1] > depths[with_idx],
        "WITH body should be indented deeper than hint+WITH line"
    );
    assert!(
        depths[cte_select_idx] > depths[with_idx],
        "CTE SELECT should be indented under WITH"
    );
    assert!(
        depths[main_select_idx] <= depths[with_idx],
        "Main SELECT should be dedented to CTE scope"
    );
}

#[test]
fn test_line_block_depths_works_with_sqlplus_comment_between_with_parenthesis_and_select() {
    let sql = r#"WITH cte AS (
REM first line of cte is comment
SELECT 1 AS id
FROM dual
)
SELECT * FROM cte;"#;
    let lines: Vec<&str> = sql.lines().collect();
    let depths = QueryExecutor::line_block_depths(sql);

    let mut with_idx = None;
    let mut cte_select_idx = None;
    let mut main_select_idx = None;

    for (idx, line) in lines.iter().enumerate() {
        if with_idx.is_none() && line.trim_start().to_uppercase().starts_with("WITH ") {
            with_idx = Some(idx);
        }

        if cte_select_idx.is_none() && line.trim_start().to_uppercase().starts_with("SELECT 1 AS") {
            cte_select_idx = Some(idx);
        } else if main_select_idx.is_none()
            && line
                .trim_start()
                .to_uppercase()
                .starts_with("SELECT * FROM CTE")
        {
            main_select_idx = Some(idx);
        }
    }

    let with_idx = with_idx.expect("expected WITH clause line");
    let cte_select_idx = cte_select_idx.expect("expected CTE SELECT line");
    let main_select_idx = main_select_idx.expect("expected main SELECT line");

    assert!(
        depths[cte_select_idx] > depths[with_idx],
        "CTE SELECT should be indented deeper than WITH line"
    );
    assert!(
        depths[main_select_idx] <= depths[with_idx],
        "Main SELECT should be dedented back to query scope"
    );
    assert!(
        depths[cte_select_idx] > depths[main_select_idx],
        "CTE SELECT should be deeper than main SELECT"
    );
}

#[test]
fn test_line_block_depths_increase_for_loop_subquery_with_and_package() {
    let sql = r#"CREATE OR REPLACE PACKAGE BODY pkg_demo AS
  PROCEDURE run_demo IS
  BEGIN
    FOR r IN (
      SELECT id
      FROM (
        SELECT id FROM dual
      )
    ) LOOP
      NULL;
    END LOOP;
  END run_demo;
END pkg_demo;

WITH cte AS (
  SELECT 1 AS n FROM dual
)
SELECT * FROM cte;"#;

    let depths = QueryExecutor::line_block_depths(sql);

    // PACKAGE BODY +1
    assert!(depths[1] >= 1, "Package body should increase depth");
    // PROCEDURE/FUNCTION BEGIN +1
    assert!(
        depths[3] > depths[2],
        "Procedure BEGIN should increase depth"
    );
    // Subquery (SELECT ...) +1
    assert!(
        depths[6] > depths[5],
        "Nested subquery should increase depth"
    );
    // LOOP ... END LOOP +1
    assert!(
        depths[9] > depths[8],
        "LOOP body should be deeper than LOOP line"
    );
    // WITH CTE block +1
    assert!(
        depths[15] > depths[14],
        "CTE body should be indented under WITH"
    );
}

#[test]
fn test_line_block_depths_with_with_clause_followed_by_update() {
    let sql = r#"WITH cte AS (
  SELECT 1 AS id FROM dual
)
UPDATE demo_table
SET id = 1
WHERE EXISTS (
  SELECT 1 FROM cte
);"#;

    let depths = QueryExecutor::line_block_depths(sql);

    let lines: Vec<&str> = sql.lines().collect();
    let mut with_idx = None;
    let mut update_idx = None;
    let mut where_idx = None;
    let mut exists_select_idx = None;

    for (idx, line) in lines.iter().enumerate() {
        let upper = line.trim().to_uppercase();
        if with_idx.is_none() && upper.starts_with("WITH ") {
            with_idx = Some(idx);
        } else if upper.starts_with("UPDATE ") {
            update_idx = Some(idx);
        } else if upper.starts_with("WHERE ") {
            where_idx = Some(idx);
        } else if idx > 0
            && lines[idx - 1]
                .trim()
                .to_uppercase()
                .starts_with("WHERE EXISTS (")
            && upper.starts_with("SELECT ")
        {
            exists_select_idx = Some(idx);
        }
    }

    let with_idx = with_idx.expect("expected WITH clause line");
    let update_idx = update_idx.expect("expected UPDATE line");
    let where_idx = where_idx.expect("expected WHERE line");
    let exists_select_idx = exists_select_idx.expect("expected nested EXISTS SELECT line");

    assert!(
        with_idx + 1 < depths.len(),
        "CTE should have at least two lines"
    );
    assert!(
        depths[with_idx + 1] > depths[with_idx],
        "CTE body SELECT should be deeper than WITH header"
    );
    assert!(
        depths[update_idx] <= depths[with_idx],
        "Main UPDATE should dedent out of WITH body"
    );
    assert!(
        depths[exists_select_idx] > depths[where_idx],
        "EXISTS subquery SELECT should be nested"
    );
}

#[test]
fn test_line_block_depths_ignores_subquery_pattern_inside_string_literal() {
    let sql = r#"BEGIN
  v_sql := '(SELECT';
  NULL;
END;"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let expected = vec![0, 1, 1, 0];
    assert_eq!(
        depths, expected,
        "String literal '(SELECT' should not affect subquery depth tracking"
    );
}

#[test]
fn test_line_block_depths_ignores_subquery_pattern_inside_dollar_quote() {
    let sql = r#"BEGIN
  v_sql := $tag$(SELECT BEGIN END)$tag$;
  NULL;
END;"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let expected = vec![0, 1, 1, 0];
    assert_eq!(
        depths, expected,
        "Dollar-quoted '(SELECT BEGIN END)' should not affect line depth tracking"
    );
}

#[test]
fn test_line_block_depths_ignores_subquery_pattern_inside_block_comment() {
    let sql = r#"BEGIN
  /* (SELECT */
  NULL;
END;"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let expected = vec![0, 1, 1, 0];
    assert_eq!(
        depths, expected,
        "Block comment '(SELECT' should not affect subquery depth tracking"
    );
}

#[test]
fn test_line_block_depths_detects_subquery_after_inline_block_comment() {
    let sql = r#"SELECT
  col
FROM t
WHERE EXISTS (/* inline note */ SELECT
  1
FROM dual
);"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let lines: Vec<&str> = sql.lines().collect();

    let where_idx = lines
        .iter()
        .position(|line| line.trim_start().starts_with("WHERE EXISTS"))
        .expect("expected WHERE EXISTS line");
    let select_one_idx = lines
        .iter()
        .position(|line| line.trim_start().starts_with('1'))
        .expect("expected SELECT body line");

    assert!(
        depths[select_one_idx] > depths[where_idx],
        "Inline block comment before SELECT should still preserve subquery depth"
    );
}

#[test]
fn test_line_block_depths_detects_subquery_after_leading_block_comment_with_sql_same_line() {
    let sql = r#"SELECT
  col
FROM t
WHERE EXISTS (
  /* comment */ SELECT 1
  FROM dual
);"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let lines: Vec<&str> = sql.lines().collect();

    let where_idx = lines
        .iter()
        .position(|line| line.trim_start().starts_with("WHERE EXISTS"))
        .expect("expected WHERE EXISTS line");
    let nested_select_idx = lines
        .iter()
        .position(|line| line.trim_start().starts_with("/* comment */ SELECT 1"))
        .expect("expected nested SELECT line");

    assert!(
        depths[nested_select_idx] > depths[where_idx],
        "Block comment prefix before SELECT should still preserve subquery depth"
    );
}

#[test]
fn test_line_block_depths_detects_subquery_after_leading_hint_comment_with_sql_same_line() {
    let sql = r#"SELECT
  col
FROM t
WHERE EXISTS (
  /*+ qb_name(inner_q) */ SELECT 1
  FROM dual
);"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let lines: Vec<&str> = sql.lines().collect();

    let where_idx = lines
        .iter()
        .position(|line| line.trim_start().starts_with("WHERE EXISTS"))
        .expect("expected WHERE EXISTS line");
    let nested_select_idx = lines
        .iter()
        .position(|line| {
            line.trim_start()
                .starts_with("/*+ qb_name(inner_q) */ SELECT 1")
        })
        .expect("expected nested SELECT line");

    assert!(
        depths[nested_select_idx] > depths[where_idx],
        "Hint comment prefix before SELECT should still preserve subquery depth"
    );
}

// ── parse_ddl_object_type tests ──

#[test]
fn test_parse_ddl_object_type_create_table() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type("CREATE TABLE MY_TABLE (ID NUMBER)"),
        "Table"
    );
}

#[test]
fn test_parse_ddl_object_type_create_global_temp_table() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type("CREATE GLOBAL TEMPORARY TABLE MY_TABLE (ID NUMBER)"),
        "Table"
    );
}

#[test]
fn test_parse_ddl_object_type_create_view() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type("CREATE VIEW MY_VIEW AS SELECT 1 FROM DUAL"),
        "View"
    );
}

#[test]
fn test_parse_ddl_object_type_create_materialized_view() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type(
            "CREATE MATERIALIZED VIEW MY_MV AS SELECT 1 FROM DUAL"
        ),
        "View"
    );
}

#[test]
fn test_parse_ddl_object_type_create_index() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type("CREATE INDEX MY_IDX ON MY_TABLE(ID)"),
        "Index"
    );
}

#[test]
fn test_parse_ddl_object_type_create_unique_index() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type("CREATE UNIQUE INDEX MY_IDX ON MY_TABLE(ID)"),
        "Index"
    );
}

#[test]
fn test_parse_ddl_object_type_create_procedure() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type("CREATE PROCEDURE MY_PROC AS BEGIN NULL; END;"),
        "Procedure"
    );
}

#[test]
fn test_parse_ddl_object_type_create_or_replace_procedure() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type(
            "CREATE OR REPLACE PROCEDURE MY_PROC AS BEGIN NULL; END;"
        ),
        "Procedure"
    );
}

#[test]
fn test_parse_ddl_object_type_create_or_replace_force_procedure() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type(
            "CREATE OR REPLACE FORCE PROCEDURE MY_PROC AS BEGIN NULL; END;"
        ),
        "Procedure"
    );
}

#[test]
fn test_parse_ddl_object_type_create_no_force_procedure() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type(
            "CREATE NO FORCE PROCEDURE MY_PROC AS BEGIN NULL; END;"
        ),
        "Procedure"
    );
}

#[test]
fn test_parse_ddl_object_type_create_function() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type(
            "CREATE FUNCTION MY_FUNC RETURN NUMBER IS BEGIN RETURN 1; END;"
        ),
        "Function"
    );
}

#[test]
fn test_parse_ddl_object_type_create_or_replace_function() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type(
            "CREATE OR REPLACE FUNCTION MY_FUNC RETURN NUMBER IS BEGIN RETURN 1; END;"
        ),
        "Function"
    );
}

#[test]
fn test_parse_ddl_object_type_create_or_replace_editionable_force_function() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type(
            "CREATE OR REPLACE EDITIONABLE FORCE FUNCTION MY_FUNC RETURN NUMBER IS BEGIN RETURN 1; END;"
        ),
        "Function"
    );
}

#[test]
fn test_parse_ddl_object_type_create_package() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type(
            "CREATE PACKAGE MY_PKG AS PROCEDURE PROC1; END MY_PKG;"
        ),
        "Package"
    );
}

#[test]
fn test_parse_ddl_object_type_create_package_body() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type(
            "CREATE PACKAGE BODY MY_PKG AS PROCEDURE PROC1 IS BEGIN NULL; END; END MY_PKG;"
        ),
        "Package Body"
    );
}

#[test]
fn test_parse_ddl_object_type_create_or_replace_type_body() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type(
            "CREATE OR REPLACE TYPE BODY MY_TYPE AS MEMBER FUNCTION GET_ID RETURN NUMBER IS BEGIN RETURN ID; END;"
        ),
        "Type Body"
    );
}

#[test]
fn test_parse_ddl_object_type_create_trigger() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type(
            "CREATE TRIGGER MY_TRIG BEFORE INSERT ON MY_TABLE BEGIN NULL; END;"
        ),
        "Trigger"
    );
}

#[test]
fn test_parse_ddl_object_type_create_sequence() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type("CREATE SEQUENCE MY_SEQ START WITH 1"),
        "Sequence"
    );
}

#[test]
fn test_parse_ddl_object_type_create_synonym() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type("CREATE SYNONYM MY_SYN FOR OTHER_TABLE"),
        "Synonym"
    );
}

#[test]
fn test_parse_ddl_object_type_create_public_synonym() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type("CREATE PUBLIC SYNONYM MY_SYN FOR OTHER_TABLE"),
        "Synonym"
    );
}

#[test]
fn test_parse_ddl_object_type_create_type() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type("CREATE TYPE MY_TYPE AS OBJECT (ID NUMBER)"),
        "Type"
    );
}

#[test]
fn test_parse_ddl_object_type_create_type_body() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type("CREATE TYPE BODY MY_TYPE AS MEMBER FUNCTION GET_ID RETURN NUMBER IS BEGIN RETURN ID; END; END;"),
        "Type Body"
    );
}

#[test]
fn test_parse_ddl_object_type_create_database_link() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type(
            "CREATE DATABASE LINK MY_LINK CONNECT TO USER IDENTIFIED BY PASS USING 'TNS'"
        ),
        "Database Link"
    );
}

#[test]
fn test_parse_ddl_object_type_create_or_replace_editionable_function() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type(
            "CREATE OR REPLACE EDITIONABLE FUNCTION MY_FUNC RETURN NUMBER IS BEGIN RETURN 1; END;"
        ),
        "Function"
    );
}

#[test]
fn test_parse_ddl_object_type_alter_table() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type("ALTER TABLE MY_TABLE ADD (COL1 NUMBER)"),
        "Table"
    );
}

#[test]
fn test_parse_ddl_object_type_alter_session() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type("ALTER SESSION SET CURRENT_SCHEMA = APP_USER"),
        "Session"
    );
}

#[test]
fn test_parse_ddl_object_type_alter_system() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type("ALTER SYSTEM SET OPEN_CURSORS = 1000"),
        "System"
    );
}

#[test]
fn test_parse_ddl_object_type_create_materialized_view_log() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type("CREATE MATERIALIZED VIEW LOG ON SALES"),
        "Materialized View Log"
    );
}

#[test]
fn test_ddl_message_alter_session_current_schema() {
    assert_eq!(
        QueryExecutor::ddl_message("ALTER SESSION SET CURRENT_SCHEMA = APP_USER"),
        "Current schema changed"
    );
}

#[test]
fn test_ddl_message_alter_session_nls_parameter() {
    assert_eq!(
        QueryExecutor::ddl_message("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'"),
        "Session NLS setting updated"
    );
}

#[test]
fn test_parse_ddl_object_type_drop_procedure() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type("DROP PROCEDURE MY_PROC"),
        "Procedure"
    );
}

#[test]
fn test_parse_ddl_object_type_drop_public_synonym() {
    assert_eq!(
        QueryExecutor::parse_ddl_object_type("DROP PUBLIC SYNONYM MY_SYN"),
        "Synonym"
    );
}

/// Regression: CREATE FUNCTION with PROCEDURE keyword in body should return "Function"
#[test]
fn test_parse_ddl_object_type_function_with_procedure_in_body() {
    let sql = "CREATE OR REPLACE FUNCTION MY_FUNC RETURN NUMBER IS BEGIN EXECUTE IMMEDIATE 'CALL MY_PROCEDURE ()'; RETURN 1; END;";
    assert_eq!(QueryExecutor::parse_ddl_object_type(sql), "Function");
}

/// Regression: CREATE PACKAGE with FUNCTION/PROCEDURE in body should return "Package"
#[test]
fn test_parse_ddl_object_type_package_with_mixed_body() {
    let sql = "CREATE OR REPLACE PACKAGE MY_PKG AS PROCEDURE PROC1; FUNCTION FUNC1 RETURN NUMBER; END MY_PKG;";
    assert_eq!(QueryExecutor::parse_ddl_object_type(sql), "Package");
}

/// Regression: CREATE TRIGGER with TABLE in body should return "Trigger"
#[test]
fn test_parse_ddl_object_type_trigger_with_table_in_body() {
    let sql = "CREATE OR REPLACE TRIGGER MY_TRIG BEFORE INSERT ON MY_TABLE FOR EACH ROW BEGIN INSERT INTO LOG_TABLE VALUES (SYSDATE); END;";
    assert_eq!(QueryExecutor::parse_ddl_object_type(sql), "Trigger");
}

#[test]
fn test_parse_whenever_oserror_continue() {
    let sql = "WHENEVER OSERROR CONTINUE\nSELECT 1 FROM DUAL;";
    let items = QueryExecutor::split_script_items(sql);

    assert!(
        matches!(
            items.first(),
            Some(ScriptItem::ToolCommand(ToolCommand::WheneverOsError {
                exit: false
            }))
        ),
        "Expected WHENEVER OSERROR CONTINUE tool command, got: {:?}",
        items.first()
    );
}

#[test]
fn test_parse_whenever_oserror_exit() {
    let sql = "WHENEVER OSERROR EXIT\nSELECT 1 FROM DUAL;";
    let items = QueryExecutor::split_script_items(sql);

    assert!(
        matches!(
            items.first(),
            Some(ScriptItem::ToolCommand(ToolCommand::WheneverOsError {
                exit: true
            }))
        ),
        "Expected WHENEVER OSERROR EXIT tool command, got: {:?}",
        items.first()
    );
}

#[test]
fn test_parse_whenever_sqlerror_exit_sql_sqlcode() {
    let sql = "WHENEVER SQLERROR EXIT SQL.SQLCODE\nSELECT 1 FROM DUAL;";
    let items = QueryExecutor::split_script_items(sql);

    assert!(
        matches!(
            items.first(),
            Some(ScriptItem::ToolCommand(ToolCommand::WheneverSqlError {
                exit: true,
                action: Some(action)
            })) if action.eq_ignore_ascii_case("SQL.SQLCODE")
        ),
        "Expected WHENEVER SQLERROR EXIT SQL.SQLCODE tool command, got: {:?}",
        items.first()
    );
}

#[test]
fn test_summarize_batch_results_marks_failure_when_dml_batch_has_errors() {
    let result = QueryExecutor::summarize_batch_results(
        "UPDATE t SET c = 1; BAD SQL;",
        2,
        std::time::Duration::from_millis(12),
        None,
        1,
        1,
        vec!["Statement 2: ORA-00900: invalid SQL statement".to_string()],
    );

    assert!(
        !result.success,
        "batch summary should fail when any statement fails"
    );
    assert!(!result.is_select);
    assert!(result.message.contains("Executed 1 of 2 statements"));
    assert!(result.message.contains("Errors:"));
}

#[test]
fn test_summarize_batch_results_marks_failure_when_select_batch_has_errors() {
    let select_result = QueryResult::new_select(
        "SELECT * FROM dual",
        vec![ColumnInfo {
            name: "DUMMY".to_string(),
            data_type: "VARCHAR2".to_string(),
        }],
        vec![vec!["X".to_string()]],
        std::time::Duration::from_millis(2),
    );

    let result = QueryExecutor::summarize_batch_results(
        "SELECT * FROM dual; BAD SQL;",
        2,
        std::time::Duration::from_millis(20),
        Some(select_result),
        0,
        1,
        vec!["Statement 2: ORA-00900: invalid SQL statement".to_string()],
    );

    assert!(
        !result.success,
        "select batch should fail when any statement fails"
    );
    assert!(result.is_select);
    assert!(result.message.contains("Errors:"));
    assert!(result.message.contains("Executed 1 of 2 statements"));
}

// ── q-quote after identifier: depth / split regression ──

#[test]
fn test_split_script_items_identifier_ending_q_followed_by_string() {
    // `seq` ends in 'q' → the q-quote detector must NOT treat `q'text'` as a q-quote.
    let sql = "SELECT seq'text' FROM dual;\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(
        stmts.len(),
        2,
        "identifier ending in q followed by string should not confuse the splitter, got: {stmts:?}"
    );
}

#[test]
fn test_split_script_items_identifier_ending_nq_followed_by_string() {
    // `unq` ends in 'nq' → nq-quote detector must NOT fire.
    let sql = "SELECT unq'val' FROM dual;\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);
    assert_eq!(
        stmts.len(),
        2,
        "identifier ending in nq followed by string should split correctly, got: {stmts:?}"
    );
}

#[test]
fn test_line_block_depths_identifier_ending_q_with_subquery() {
    // Subquery paren after `seq'text'` must still be tracked.
    let sql = "SELECT seq'x', (SELECT 1 FROM dual)\nFROM t;";
    let depths = QueryExecutor::line_block_depths(sql);
    assert_eq!(
        depths,
        vec![0, 0],
        "subquery depth should not be affected by identifier ending in q before string"
    );
}

#[test]
fn test_statement_bounds_at_cursor_identifier_ending_q_string_keeps_second_statement() {
    let sql = "SELECT seq'v' FROM dual;
SELECT 2 FROM dual;";
    let cursor = sql.rfind("2 FROM dual").unwrap_or(sql.len());

    let bounds = QueryExecutor::statement_bounds_at_cursor(sql, cursor)
        .expect("expected second statement bounds after identifier-ending q literal");
    let statement = &sql[bounds.0..bounds.1];

    assert!(
        statement.trim_start().starts_with("SELECT 2 FROM dual"),
        "expected second statement, got: {statement}"
    );
}

#[test]
fn test_statement_bounds_at_cursor_identifier_ending_nq_string_keeps_second_statement() {
    let sql = "SELECT unq'v' FROM dual;
SELECT 2 FROM dual;";
    let cursor = sql.rfind("2 FROM dual").unwrap_or(sql.len());

    let bounds = QueryExecutor::statement_bounds_at_cursor(sql, cursor)
        .expect("expected second statement bounds after identifier-ending nq literal");
    let statement = &sql[bounds.0..bounds.1];

    assert!(
        statement.trim_start().starts_with("SELECT 2 FROM dual"),
        "expected second statement, got: {statement}"
    );
}

#[test]
fn test_line_block_depths_real_q_quote_still_works() {
    // Standalone q-quote must continue to work.
    let sql = "SELECT q'[hello]', (SELECT 1 FROM dual)\nFROM t;";
    let depths = QueryExecutor::line_block_depths(sql);
    assert_eq!(
        depths,
        vec![0, 0],
        "standalone q-quote should not break depth"
    );
}

#[test]
fn test_split_script_items_q_quote_whitespace_delimiter_falls_back_to_normal_quote() {
    let sql = "SELECT q' hello' FROM dual;\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "q' <space>...' must not be treated as q-quote delimiter and should split normally: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("SELECT q' hello' FROM dual"),
        "first statement should preserve regular quote fallback: {}",
        stmts[0]
    );
}

#[test]
fn test_split_script_items_nq_quote_whitespace_delimiter_falls_back_to_normal_quote() {
    let sql = "SELECT nq' hello' FROM dual;\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "nq' <space>...' must not be treated as q-quote delimiter and should split normally: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("SELECT nq' hello' FROM dual"),
        "first statement should preserve regular quote fallback: {}",
        stmts[0]
    );
}

#[test]
fn test_line_block_depths_subquery_headed_by_with_clause_indents_correctly() {
    // When `(` is followed by `WITH cte AS (...) SELECT ...` on the next line,
    // the WITH line and the main SELECT inside the paren must both be at depth 1.
    let sql = "SELECT *\nFROM (\n  WITH cte AS (SELECT 1 FROM dual)\n  SELECT * FROM cte\n);";
    let lines: Vec<&str> = sql.lines().collect();
    let depths = QueryExecutor::line_block_depths(sql);

    let from_idx = lines
        .iter()
        .position(|l| l.trim_start().starts_with("FROM ("))
        .unwrap();
    let with_idx = lines
        .iter()
        .position(|l| l.trim_start().to_uppercase().starts_with("WITH "))
        .unwrap();
    let inner_select_idx = lines
        .iter()
        .position(|l| {
            l.trim_start()
                .to_uppercase()
                .starts_with("SELECT * FROM CTE")
        })
        .unwrap();

    assert!(
        depths[with_idx] > depths[from_idx],
        "WITH inside paren should be deeper than outer FROM line (depths: {:?})",
        depths
    );
    assert_eq!(
        depths[inner_select_idx], depths[with_idx],
        "main SELECT after CTE should stay at same depth as WITH (depths: {:?})",
        depths
    );
}

#[test]
fn test_line_block_depths_subquery_with_clause_multiline_cte_body() {
    // Same as above but with CTE body on its own lines to exercise pending_subquery_paren.
    let sql = "SELECT *\nFROM (\n  WITH cte AS (\n    SELECT 1 AS n FROM dual\n  )\n  SELECT * FROM cte\n);";
    let lines: Vec<&str> = sql.lines().collect();
    let depths = QueryExecutor::line_block_depths(sql);

    let from_idx = lines
        .iter()
        .position(|l| l.trim_start().starts_with("FROM ("))
        .unwrap();
    let with_idx = lines
        .iter()
        .position(|l| l.trim_start().to_uppercase().starts_with("WITH "))
        .unwrap();
    let inner_select_idx = lines
        .iter()
        .position(|l| {
            l.trim_start()
                .to_uppercase()
                .starts_with("SELECT * FROM CTE")
        })
        .unwrap();

    assert!(
        depths[with_idx] > depths[from_idx],
        "WITH inside paren must be deeper than outer FROM (depths: {:?})",
        depths
    );
    assert_eq!(
        depths[inner_select_idx], depths[with_idx],
        "main SELECT after multi-line CTE body must match WITH depth (depths: {:?})",
        depths
    );
}

#[test]
fn test_line_block_depths_subquery_with_clause_on_same_line_as_open_paren() {
    let sql = "SELECT *\nFROM (WITH cte AS (SELECT 1 AS n FROM dual)\n      SELECT * FROM cte\n);";
    let lines: Vec<&str> = sql.lines().collect();
    let depths = QueryExecutor::line_block_depths(sql);

    let from_idx = lines
        .iter()
        .position(|l| l.trim_start().starts_with("FROM (WITH "))
        .unwrap();
    let inner_select_idx = lines
        .iter()
        .position(|l| {
            l.trim_start()
                .to_uppercase()
                .starts_with("SELECT * FROM CTE")
        })
        .unwrap();

    assert!(
        depths[inner_select_idx] > depths[from_idx],
        "main SELECT after same-line (WITH should still stay in nested subquery depth (depths: {:?})",
        depths
    );
}

#[test]
fn test_line_block_depths_subquery_with_clause_after_block_comment_same_line() {
    let sql = "SELECT *\nFROM (/* c */ WITH cte AS (SELECT 1 AS n FROM dual)\n      SELECT * FROM cte\n);";
    let lines: Vec<&str> = sql.lines().collect();
    let depths = QueryExecutor::line_block_depths(sql);

    let from_idx = lines
        .iter()
        .position(|l| l.trim_start().starts_with("FROM (/* c */ WITH "))
        .unwrap();
    let inner_select_idx = lines
        .iter()
        .position(|l| {
            l.trim_start()
                .to_uppercase()
                .starts_with("SELECT * FROM CTE")
        })
        .unwrap();

    assert!(
        depths[inner_select_idx] > depths[from_idx],
        "block comment between ( and WITH should preserve nested subquery depth (depths: {:?})",
        depths
    );
}

#[test]
fn test_line_block_depths_standalone_with_main_select_not_affected_by_fix() {
    // Regression guard: a top-level (non-nested) WITH…SELECT must still give
    // depth 0 for the main SELECT, exactly as before the fix.
    let sql = "WITH cte AS (\n  SELECT 1 AS n FROM dual\n)\nSELECT * FROM cte;";
    let lines: Vec<&str> = sql.lines().collect();
    let depths = QueryExecutor::line_block_depths(sql);

    let with_idx = lines
        .iter()
        .position(|l| l.trim_start().to_uppercase().starts_with("WITH "))
        .unwrap();
    let main_select_idx = lines
        .iter()
        .position(|l| {
            l.trim_start()
                .to_uppercase()
                .starts_with("SELECT * FROM CTE")
        })
        .unwrap();

    assert!(
        depths[with_idx + 1] > depths[with_idx],
        "CTE body must be deeper than WITH line (depths: {:?})",
        depths
    );
    assert!(
        depths[main_select_idx] <= depths[with_idx],
        "main SELECT must dedent back to WITH level (depths: {:?})",
        depths
    );
}

#[test]
fn test_line_block_depths_detects_subquery_after_line_comment_between_paren_and_select() {
    let sql = r#"SELECT
  col
FROM t
WHERE EXISTS (
  -- comment before nested select
  SELECT 1
  FROM dual
);"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let lines: Vec<&str> = sql.lines().collect();

    let where_idx = lines
        .iter()
        .position(|line| line.trim_start().starts_with("WHERE EXISTS"))
        .expect("expected WHERE EXISTS line");
    let nested_select_idx = lines
        .iter()
        .position(|line| line.trim_start().starts_with("SELECT 1"))
        .expect("expected nested SELECT line");

    assert!(
        depths[nested_select_idx] > depths[where_idx],
        "Line comment between '(' and SELECT should not break subquery depth detection"
    );
}

#[test]
fn test_line_block_depths_detects_subquery_after_mixed_comments_between_paren_and_select() {
    let sql = r#"SELECT
  col
FROM t
WHERE EXISTS (
  /* first comment */
  -- second comment
  SELECT 1
  FROM dual
);"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let lines: Vec<&str> = sql.lines().collect();

    let where_idx = lines
        .iter()
        .position(|line| line.trim_start().starts_with("WHERE EXISTS"))
        .expect("expected WHERE EXISTS line");
    let nested_select_idx = lines
        .iter()
        .position(|line| line.trim_start().starts_with("SELECT 1"))
        .expect("expected nested SELECT line");

    assert!(
        depths[nested_select_idx] > depths[where_idx],
        "Mixed block/line comments between '(' and SELECT should preserve subquery depth"
    );
}

#[test]
fn test_line_block_depths_detects_subquery_after_rem_comment_between_paren_and_select() {
    let sql = r#"SELECT
  col
FROM t
WHERE EXISTS (
  REM comment before nested select
  SELECT 1
  FROM dual
);"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let lines: Vec<&str> = sql.lines().collect();

    let where_idx = lines
        .iter()
        .position(|line| line.trim_start().starts_with("WHERE EXISTS"))
        .expect("expected WHERE EXISTS line");
    let nested_select_idx = lines
        .iter()
        .position(|line| line.trim_start().starts_with("SELECT 1"))
        .expect("expected nested SELECT line");

    assert!(
        depths[nested_select_idx] > depths[where_idx],
        "REM comment between '(' and SELECT should preserve subquery depth"
    );
}

#[test]
fn test_line_block_depths_detects_subquery_after_remark_comment_between_paren_and_select() {
    let sql = r#"SELECT
  col
FROM t
WHERE EXISTS (
  REMARK comment before nested select
  SELECT 1
  FROM dual
);"#;
    let depths = QueryExecutor::line_block_depths(sql);
    let lines: Vec<&str> = sql.lines().collect();

    let where_idx = lines
        .iter()
        .position(|line| line.trim_start().starts_with("WHERE EXISTS"))
        .expect("expected WHERE EXISTS line");
    let nested_select_idx = lines
        .iter()
        .position(|line| line.trim_start().starts_with("SELECT 1"))
        .expect("expected nested SELECT line");

    assert!(
        depths[nested_select_idx] > depths[where_idx],
        "REMARK comment between '(' and SELECT should preserve subquery depth"
    );
}

#[test]
fn test_line_block_depths_with_inside_subquery_in_if_block_keeps_main_select_nested() {
    let sql = r#"BEGIN
  IF 1 = 1 THEN
    SELECT *
    INTO v_dummy
    FROM (
      WITH cte AS (
        SELECT 1 AS n FROM dual
      )
      SELECT * FROM cte
    );
  END IF;
END;
"#;

    let depths = QueryExecutor::line_block_depths(sql);
    let lines: Vec<&str> = sql.lines().collect();

    let with_idx = lines
        .iter()
        .position(|line| line.trim_start().to_uppercase().starts_with("WITH "))
        .expect("expected WITH line");
    let inner_select_idx = lines
        .iter()
        .position(|line| {
            line.trim_start()
                .to_uppercase()
                .starts_with("SELECT * FROM CTE")
        })
        .expect("expected SELECT * FROM cte line");

    assert_eq!(
        depths[inner_select_idx], depths[with_idx],
        "main SELECT after WITH inside subquery should keep nested depth inside IF block (depths: {:?})",
        depths
    );
}

#[test]
fn test_line_block_depths_multiple_with_subqueries_in_same_plsql_block() {
    let sql = r#"BEGIN
  SELECT *
  INTO v_one
  FROM (
    WITH c1 AS (SELECT 1 AS n FROM dual)
    SELECT * FROM c1
  );

  SELECT *
  INTO v_two
  FROM (
    WITH c2 AS (SELECT 2 AS n FROM dual)
    SELECT * FROM c2
  );
END;
"#;

    let depths = QueryExecutor::line_block_depths(sql);
    let lines: Vec<&str> = sql.lines().collect();

    let with_lines: Vec<usize> = lines
        .iter()
        .enumerate()
        .filter_map(|(idx, line)| {
            line.trim_start()
                .to_uppercase()
                .starts_with("WITH ")
                .then_some(idx)
        })
        .collect();
    let select_lines: Vec<usize> = lines
        .iter()
        .enumerate()
        .filter_map(|(idx, line)| {
            line.trim_start()
                .to_uppercase()
                .starts_with("SELECT * FROM C")
                .then_some(idx)
        })
        .collect();

    assert_eq!(with_lines.len(), 2, "expected two WITH lines");
    assert_eq!(select_lines.len(), 2, "expected two SELECT * FROM c* lines");

    for (with_idx, select_idx) in with_lines.iter().zip(select_lines.iter()) {
        assert_eq!(
            depths[*select_idx], depths[*with_idx],
            "each main SELECT after WITH inside subquery should remain nested (depths: {:?})",
            depths
        );
    }
}

#[test]
fn test_line_block_depths_with_values_main_query_dedents_to_with_level() {
    let sql = "WITH cte AS (\n  SELECT 1 AS n\n)\nVALUES ((SELECT n FROM cte));";
    let depths = QueryExecutor::line_block_depths(sql);
    let lines: Vec<&str> = sql.lines().collect();

    let with_idx = lines
        .iter()
        .position(|line| line.trim_start().to_uppercase().starts_with("WITH "))
        .expect("expected WITH line");
    let values_idx = lines
        .iter()
        .position(|line| line.trim_start().to_uppercase().starts_with("VALUES"))
        .expect("expected VALUES line");

    assert_eq!(
        depths[values_idx], depths[with_idx],
        "VALUES main query after WITH should dedent back to WITH depth (depths: {:?})",
        depths
    );
}

#[test]
fn test_line_block_depths_detects_values_subquery_head_after_open_paren() {
    let sql = "SELECT *\nFROM (\n  VALUES (1), (2)\n) AS t(n)\nWHERE n > 1;";
    let depths = QueryExecutor::line_block_depths(sql);
    let lines: Vec<&str> = sql.lines().collect();

    let from_idx = lines
        .iter()
        .position(|line| line.trim_start().to_uppercase().starts_with("FROM ("))
        .expect("expected FROM line");
    let values_idx = lines
        .iter()
        .position(|line| line.trim_start().to_uppercase().starts_with("VALUES"))
        .expect("expected VALUES line");

    assert!(
        depths[values_idx] > depths[from_idx],
        "VALUES subquery head should be indented inside FROM parentheses (depths: {:?})",
        depths
    );
}

#[test]
fn test_line_block_depths_detects_values_subquery_after_comment_between_paren_and_values() {
    let sql = "SELECT *\nFROM (\n  -- comment before nested values\n  VALUES (1), (2)\n) AS t(n)\nWHERE n > 1;";
    let depths = QueryExecutor::line_block_depths(sql);
    let lines: Vec<&str> = sql.lines().collect();

    let from_idx = lines
        .iter()
        .position(|line| line.trim_start().to_uppercase().starts_with("FROM ("))
        .expect("expected FROM line");
    let values_idx = lines
        .iter()
        .position(|line| line.trim_start().to_uppercase().starts_with("VALUES"))
        .expect("expected VALUES line");

    assert!(
        depths[values_idx] > depths[from_idx],
        "Comment between '(' and VALUES should preserve nested depth detection"
    );
}

#[test]
fn test_line_block_depths_detects_insert_subquery_head_after_open_paren() {
    let sql = "SELECT *\nFROM (\n  INSERT INTO dst(id) SELECT id FROM src RETURNING id\n) q;";
    let depths = QueryExecutor::line_block_depths(sql);
    let lines: Vec<&str> = sql.lines().collect();

    let from_idx = lines
        .iter()
        .position(|line| line.trim_start().to_uppercase().starts_with("FROM ("))
        .expect("expected FROM line");
    let insert_idx = lines
        .iter()
        .position(|line| line.trim_start().to_uppercase().starts_with("INSERT"))
        .expect("expected INSERT line");

    assert!(
        depths[insert_idx] > depths[from_idx],
        "INSERT subquery head should be indented inside FROM parentheses (depths: {:?})",
        depths
    );
}

#[test]
fn test_line_block_depths_detects_dml_subquery_after_comment_between_paren_and_update() {
    let sql = "SELECT *\nFROM (\n  /* comment before nested update */\n  UPDATE dst SET id = src.id FROM src WHERE dst.id = src.id RETURNING dst.id\n) q;";
    let depths = QueryExecutor::line_block_depths(sql);
    let lines: Vec<&str> = sql.lines().collect();

    let from_idx = lines
        .iter()
        .position(|line| line.trim_start().to_uppercase().starts_with("FROM ("))
        .expect("expected FROM line");
    let update_idx = lines
        .iter()
        .position(|line| line.trim_start().to_uppercase().starts_with("UPDATE"))
        .expect("expected UPDATE line");

    assert!(
        depths[update_idx] > depths[from_idx],
        "Comment between '(' and UPDATE should preserve nested depth detection (depths: {:?})",
        depths
    );
}

#[test]
fn test_line_block_depths_detects_merge_subquery_after_line_comment_between_paren_and_merge() {
    let sql = "SELECT *\nFROM (\n  -- comment before nested merge\n  MERGE INTO dst d USING src s ON (d.id = s.id) WHEN MATCHED THEN UPDATE SET d.id = s.id\n) q;";
    let depths = QueryExecutor::line_block_depths(sql);
    let lines: Vec<&str> = sql.lines().collect();

    let from_idx = lines
        .iter()
        .position(|line| line.trim_start().to_uppercase().starts_with("FROM ("))
        .expect("expected FROM line");
    let merge_idx = lines
        .iter()
        .position(|line| line.trim_start().to_uppercase().starts_with("MERGE"))
        .expect("expected MERGE line");

    assert!(
        depths[merge_idx] > depths[from_idx],
        "Line comment between '(' and MERGE should preserve nested depth detection (depths: {:?})",
        depths
    );
}

#[test]
fn test_split_script_items_mysql_if_function_does_not_open_block_depth() {
    let sql = "SELECT IF(score > 90, 'A', 'B') AS grade FROM exam_scores;\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "IF() function must not keep parser in block depth: {stmts:?}"
    );
    assert!(stmts[0].starts_with("SELECT IF("));
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_mysql_if_function_followed_by_case_then_stays_two_statements() {
    let sql = "SELECT IF(score > 90, 'A', 'B') + CASE WHEN bonus > 0 THEN 1 ELSE 0 END AS grade FROM exam_scores;\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "IF() function must not treat downstream CASE THEN as IF THEN: {stmts:?}"
    );
    assert!(
        stmts[0].contains("CASE WHEN bonus > 0 THEN 1 ELSE 0 END"),
        "First statement should preserve CASE expression: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_mysql_backtick_identifier_with_semicolon_stays_single_statement() {
    let sql = "SELECT `semi;colon` AS c FROM demo;\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "semicolon inside MySQL backtick identifier must not split statement: {stmts:?}"
    );
    assert!(
        stmts[0].contains("`semi;colon`"),
        "first statement should preserve backtick identifier: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_format_items_mysql_if_function_followed_by_case_then_stays_two_statements() {
    let sql = "SELECT IF(score > 90, 'A', 'B') + CASE WHEN bonus > 0 THEN 1 ELSE 0 END AS grade FROM exam_scores;\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        2,
        "split_format_items must match split_script_items for IF() + CASE THEN inputs: {stmts:?}"
    );
    assert!(
        stmts[0].contains("CASE WHEN bonus > 0 THEN 1 ELSE 0 END"),
        "First formatted statement should preserve CASE expression: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_line_block_depths_if_function_line_stays_top_level_without_then() {
    let sql = "SELECT IF(score > 90, 'A', 'B') AS grade\nFROM exam_scores;";
    let depths = QueryExecutor::line_block_depths(sql);

    assert_eq!(
        depths,
        vec![0, 0],
        "IF() scalar function should not affect block depth"
    );
}

#[test]
fn test_line_block_depths_ignores_procedure_keyword_in_comment_for_begin_prededent() {
    let sql = "BEGIN\n  -- PROCEDURE marker in comment\n  BEGIN\n    NULL;\n  END;\nEND;";
    let depths = QueryExecutor::line_block_depths(sql);

    assert_eq!(
        depths,
        vec![0, 1, 1, 2, 1, 0],
        "Comment text must not trigger subprogram BEGIN pre-dedent"
    );
}

#[test]
fn test_line_block_depths_ignores_function_keyword_in_string_for_begin_prededent() {
    let sql = "BEGIN\n  v_sql := 'FUNCTION marker in string';\n  BEGIN\n    NULL;\n  END;\nEND;";
    let depths = QueryExecutor::line_block_depths(sql);

    assert_eq!(
        depths,
        vec![0, 1, 1, 2, 1, 0],
        "String literal text must not trigger subprogram BEGIN pre-dedent"
    );
}

#[test]
fn test_line_block_depths_preserves_subquery_depth_after_non_subquery_parentheses() {
    let sql = "SELECT *\nFROM (\n  SELECT (1 + 2) AS n\n  FROM dual\n) q;";
    let depths = QueryExecutor::line_block_depths(sql);
    let lines: Vec<&str> = sql.lines().collect();

    let from_open_idx = lines
        .iter()
        .position(|line| line.trim_start().to_uppercase().starts_with("FROM ("))
        .expect("expected FROM ( line");
    let nested_select_idx = lines
        .iter()
        .position(|line| {
            line.trim_start()
                .to_uppercase()
                .starts_with("SELECT (1 + 2)")
        })
        .expect("expected nested SELECT line");
    let nested_from_idx = lines
        .iter()
        .position(|line| line.trim_start().to_uppercase().starts_with("FROM DUAL"))
        .expect("expected nested FROM line");

    assert!(
        depths[nested_select_idx] > depths[from_open_idx],
        "Nested SELECT should be deeper than outer FROM (depths: {:?})",
        depths
    );
    assert_eq!(
        depths[nested_from_idx], depths[nested_select_idx],
        "Non-subquery parentheses inside nested SELECT must not prematurely dedent subquery depth (depths: {:?})",
        depths
    );
}

#[test]
fn test_split_script_items_oracle_with_function_keeps_single_statement_until_main_select() {
    let sql = "WITH\n  FUNCTION f RETURN NUMBER IS\n  BEGIN\n    RETURN 1;\n  END;\nSELECT f() FROM dual;\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "WITH FUNCTION declaration must stay attached to main SELECT statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
        "first statement should preserve WITH FUNCTION declaration: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("SELECT f() FROM dual"),
        "first statement should include main SELECT: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_function_as_keeps_single_statement_until_main_select() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER AS
  BEGIN
    RETURN 1;
  END;
SELECT f() FROM dual;
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "WITH FUNCTION ... AS declaration must stay attached to main SELECT statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with(
            "WITH
  FUNCTION f RETURN NUMBER AS"
        ),
        "first statement should preserve WITH FUNCTION AS declaration: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("SELECT f() FROM dual"),
        "first statement should include main SELECT: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_format_items_oracle_with_procedure_keeps_single_statement_until_main_select() {
    let sql = "WITH\n  PROCEDURE p IS\n  BEGIN\n    NULL;\n  END;\nSELECT 1 FROM dual;\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        2,
        "split_format_items must keep WITH PROCEDURE declaration with its main SELECT: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  PROCEDURE p IS"),
        "first formatted statement should preserve WITH PROCEDURE declaration: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("SELECT 1 FROM dual"),
        "first formatted statement should include main SELECT: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_function_keeps_single_statement_until_main_merge() {
    let sql = "WITH\n  FUNCTION normalize_id(p_id NUMBER) RETURN NUMBER IS\n  BEGIN\n    RETURN p_id;\n  END;\nMERGE INTO target t\nUSING (SELECT normalize_id(1) AS id FROM dual) s\nON (t.id = s.id)\nWHEN MATCHED THEN\n  UPDATE SET t.id = s.id;\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "WITH FUNCTION declaration must stay attached to main MERGE statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  FUNCTION normalize_id"),
        "first statement should preserve WITH FUNCTION declaration: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("MERGE INTO target t"),
        "first statement should include main MERGE: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_procedure_keeps_single_statement_until_main_insert() {
    let sql = "WITH\n  PROCEDURE p_log(p_id NUMBER) IS\n  BEGIN\n    NULL;\n  END;\nINSERT INTO target(id)\nSELECT 1 FROM dual;\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "WITH PROCEDURE declaration must stay attached to main INSERT statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  PROCEDURE p_log"),
        "first statement should preserve WITH PROCEDURE declaration: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("INSERT INTO target(id)"),
        "first statement should include main INSERT: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_function_keeps_single_statement_until_main_update() {
    let sql = "WITH\n  FUNCTION f RETURN NUMBER IS\n  BEGIN\n    RETURN 1;\n  END;\nUPDATE target\nSET id = f()\nWHERE id = 1;\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "WITH FUNCTION declaration must stay attached to main UPDATE statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
        "first statement should preserve WITH FUNCTION declaration: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("UPDATE target"),
        "first statement should include main UPDATE: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_procedure_keeps_single_statement_until_main_delete() {
    let sql = "WITH\n  PROCEDURE p_noop IS\n  BEGIN\n    NULL;\n  END;\nDELETE FROM target\nWHERE id IN (SELECT 1 FROM dual);\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "WITH PROCEDURE declaration must stay attached to main DELETE statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  PROCEDURE p_noop IS"),
        "first statement should preserve WITH PROCEDURE declaration: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("DELETE FROM target"),
        "first statement should include main DELETE: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_function_keeps_single_statement_until_main_values() {
    let sql = "WITH\n  FUNCTION f RETURN NUMBER IS\n  BEGIN\n    RETURN 1;\n  END;\nVALUES (f());\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "WITH FUNCTION declaration must stay attached to main VALUES statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
        "first statement should preserve WITH FUNCTION declaration: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("VALUES (f())"),
        "first statement should include main VALUES body: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_procedure_without_semicolon_uses_slash_terminator() {
    let sql = "WITH PROCEDURE p IS\nBEGIN\n  NULL;\nEND\n/\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "expected WITH PROCEDURE declaration and trailing SELECT split, got: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH PROCEDURE p IS"),
        "first statement should preserve WITH PROCEDURE block, got: {}",
        stmts[0]
    );
    assert!(stmts[0].contains("END"));
    assert_eq!(stmts[1], "SELECT 2 FROM dual");
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_create_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
CREATE TABLE t_parser_recover (id NUMBER);
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        3,
        "parser should recover WITH FUNCTION declaration mode when CREATE starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with(
            "WITH
  FUNCTION f RETURN NUMBER IS"
        ),
        "first statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("CREATE TABLE t_parser_recover"),
        "second statement should start at CREATE TABLE after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_procedure_recovers_to_alter_statement_head() {
    let sql = "WITH
  PROCEDURE p IS
  BEGIN
    NULL;
  END;
ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD';
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        3,
        "parser should recover WITH PROCEDURE declaration mode when ALTER starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with(
            "WITH
  PROCEDURE p IS"
        ),
        "first statement should preserve WITH PROCEDURE declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("ALTER SESSION SET NLS_DATE_FORMAT"),
        "second statement should start at ALTER SESSION after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_declare_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
DECLARE
  v NUMBER := 1;
BEGIN
  NULL;
END;
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        3,
        "parser should recover WITH FUNCTION declaration mode when DECLARE starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
        "first statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("DECLARE\n  v NUMBER := 1;"),
        "second statement should start at DECLARE block after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_begin_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
BEGIN
  NULL;
END;
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        3,
        "parser should recover WITH FUNCTION declaration mode when BEGIN starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with(
            "WITH
  FUNCTION f RETURN NUMBER IS"
        ),
        "first statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].contains(
            "BEGIN
  NULL;
END"
        ),
        "second statement should start at BEGIN block after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_procedure_recovers_to_drop_statement_head() {
    let sql = "WITH
  PROCEDURE p IS
  BEGIN
    NULL;
  END;
DROP TABLE t_parser_recover;
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        3,
        "parser should recover WITH PROCEDURE declaration mode when DROP starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  PROCEDURE p IS"),
        "first statement should preserve WITH PROCEDURE declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("DROP TABLE t_parser_recover"),
        "second statement should start at DROP statement after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_savepoint_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
SAVEPOINT before_batch;
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        3,
        "parser should recover WITH FUNCTION declaration mode when SAVEPOINT starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
        "first statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("SAVEPOINT before_batch"),
        "second statement should start at SAVEPOINT after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_lock_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
LOCK TABLE t_parser_recover IN EXCLUSIVE MODE;
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        3,
        "parser should recover WITH FUNCTION declaration mode when LOCK starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
        "first statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("LOCK TABLE t_parser_recover IN EXCLUSIVE MODE"),
        "second statement should start at LOCK TABLE after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_disassociate_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
DISASSOCIATE STATISTICS FROM TABLES t_parser_recover;
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        3,
        "parser should recover WITH FUNCTION declaration mode when DISASSOCIATE starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
        "first statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("DISASSOCIATE STATISTICS FROM TABLES t_parser_recover"),
        "second statement should start at DISASSOCIATE statement after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_associate_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
ASSOCIATE STATISTICS WITH TABLES t_parser_recover DEFAULT COST (10, 20, 30);
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        3,
        "parser should recover WITH FUNCTION declaration mode when ASSOCIATE starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
        "first statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with(
            "ASSOCIATE STATISTICS WITH TABLES t_parser_recover DEFAULT COST (10, 20, 30)"
        ),
        "second statement should start at ASSOCIATE statement after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_purge_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
PURGE TABLE t_parser_recover;
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        3,
        "parser should recover WITH FUNCTION declaration mode when PURGE starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
        "first statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("PURGE TABLE t_parser_recover"),
        "second statement should start at PURGE TABLE after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_flashback_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
FLASHBACK TABLE t_parser_recover TO BEFORE DROP;
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        3,
        "parser should recover WITH FUNCTION declaration mode when FLASHBACK starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
        "first statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("FLASHBACK TABLE t_parser_recover TO BEFORE DROP"),
        "second statement should start at FLASHBACK TABLE after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_audit_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
AUDIT SESSION;
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        3,
        "parser should recover WITH FUNCTION declaration mode when AUDIT starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
        "first statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("AUDIT SESSION"),
        "second statement should start at AUDIT after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_noaudit_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
NOAUDIT SESSION;
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        3,
        "parser should recover WITH FUNCTION declaration mode when NOAUDIT starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
        "first statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("NOAUDIT SESSION"),
        "second statement should start at NOAUDIT after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_format_items_oracle_with_function_recovers_to_lock_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
LOCK TABLE t_parser_recover IN EXCLUSIVE MODE;
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        3,
        "split_format_items should recover WITH FUNCTION declaration mode when LOCK starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
        "first formatted statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("LOCK TABLE t_parser_recover IN EXCLUSIVE MODE"),
        "second formatted statement should start at LOCK TABLE after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_format_items_oracle_with_function_recovers_to_disassociate_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
DISASSOCIATE STATISTICS FROM TABLES t_parser_recover;
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        3,
        "split_format_items should recover WITH FUNCTION declaration mode at DISASSOCIATE statement head: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
        "first formatted statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("DISASSOCIATE STATISTICS FROM TABLES t_parser_recover"),
        "second formatted statement should start at DISASSOCIATE after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_format_items_oracle_with_function_recovers_to_associate_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
ASSOCIATE STATISTICS WITH TABLES t_parser_recover DEFAULT COST (10, 20, 30);
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        3,
        "split_format_items should recover WITH FUNCTION declaration mode at ASSOCIATE statement head: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
        "first formatted statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with(
            "ASSOCIATE STATISTICS WITH TABLES t_parser_recover DEFAULT COST (10, 20, 30)"
        ),
        "second formatted statement should start at ASSOCIATE after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_format_items_oracle_with_function_recovers_to_purge_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
PURGE TABLE t_parser_recover;
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        3,
        "split_format_items should recover WITH FUNCTION declaration mode when PURGE starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
        "first formatted statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("PURGE TABLE t_parser_recover"),
        "second formatted statement should start at PURGE TABLE after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_format_items_oracle_with_function_recovers_to_flashback_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
FLASHBACK TABLE t_parser_recover TO BEFORE DROP;
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        3,
        "split_format_items should recover WITH FUNCTION declaration mode when FLASHBACK starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
        "first formatted statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("FLASHBACK TABLE t_parser_recover TO BEFORE DROP"),
        "second formatted statement should start at FLASHBACK TABLE after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_format_items_oracle_with_function_recovers_to_audit_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
AUDIT SESSION;
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        3,
        "split_format_items should recover WITH FUNCTION declaration mode when AUDIT starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
        "first formatted statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("AUDIT SESSION"),
        "second formatted statement should start at AUDIT after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_comment_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
COMMENT ON TABLE t_parser_recover IS 'recovered';
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        3,
        "parser should recover WITH FUNCTION declaration mode when COMMENT starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with(
            "WITH
  FUNCTION f RETURN NUMBER IS"
        ),
        "first statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("COMMENT ON TABLE t_parser_recover IS 'recovered'"),
        "second statement should start at COMMENT after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_rename_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
RENAME t_parser_recover TO t_parser_recover_new;
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        3,
        "parser should recover WITH FUNCTION declaration mode when RENAME starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with(
            "WITH
  FUNCTION f RETURN NUMBER IS"
        ),
        "first statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("RENAME t_parser_recover TO t_parser_recover_new"),
        "second statement should start at RENAME after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_format_items_oracle_with_function_recovers_to_comment_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
COMMENT ON TABLE t_parser_recover IS 'recovered';
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        3,
        "split_format_items should recover WITH FUNCTION declaration mode when COMMENT starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with(
            "WITH
  FUNCTION f RETURN NUMBER IS"
        ),
        "first formatted statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("COMMENT ON TABLE t_parser_recover IS 'recovered'"),
        "second formatted statement should start at COMMENT after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_format_items_oracle_with_function_recovers_to_rename_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
RENAME t_parser_recover TO t_parser_recover_new;
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        3,
        "split_format_items should recover WITH FUNCTION declaration mode when RENAME starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with(
            "WITH
  FUNCTION f RETURN NUMBER IS"
        ),
        "first formatted statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("RENAME t_parser_recover TO t_parser_recover_new"),
        "second formatted statement should start at RENAME after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}
#[test]
fn test_split_format_items_oracle_with_function_recovers_to_noaudit_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
NOAUDIT SESSION;
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        3,
        "split_format_items should recover WITH FUNCTION declaration mode when NOAUDIT starts a new statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
        "first formatted statement should preserve WITH FUNCTION declaration block: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("NOAUDIT SESSION"),
        "second formatted statement should start at NOAUDIT after recovery: {}",
        stmts[1]
    );
    assert!(stmts[2].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_function_and_cte_keeps_single_statement() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
  cte AS (
    SELECT f() AS n FROM dual
  )
SELECT n FROM cte;
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "WITH FUNCTION declaration + CTE must stay attached to main SELECT statement: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
        "first statement should preserve WITH FUNCTION declaration: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("cte AS ("),
        "first statement should keep the CTE clause after function declaration: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("SELECT n FROM cte"),
        "first statement should include the main SELECT: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_create_type_opaque_keeps_single_statement() {
    let sql = "CREATE OR REPLACE TYPE t_opaque AS OPAQUE (
  STORAGE RAW(16)
);
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "CREATE TYPE ... AS OPAQUE should split at the type terminator: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("CREATE OR REPLACE TYPE t_opaque AS OPAQUE"),
        "first statement should preserve TYPE OPAQUE declaration: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_create_type_json_keeps_single_statement() {
    let sql = "CREATE OR REPLACE TYPE t_json AS JSON\n(\n  STRICT\n);\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "CREATE TYPE ... AS JSON should split at the type terminator: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("CREATE OR REPLACE TYPE t_json AS JSON"),
        "first statement should preserve TYPE JSON declaration: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_create_type_body_with_member_function_keeps_single_statement() {
    let sql = "CREATE OR REPLACE TYPE BODY t_demo AS\n  MEMBER FUNCTION f RETURN NUMBER IS\n  BEGIN\n    RETURN 1;\n  END;\nEND;\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "CREATE TYPE BODY with member function should remain a single statement until final END: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("CREATE OR REPLACE TYPE BODY t_demo AS"),
        "first statement should preserve TYPE BODY header: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("MEMBER FUNCTION f RETURN NUMBER IS"),
        "first statement should preserve member function body: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_create_java_source_keeps_body_until_slash() {
    let sql = r#"CREATE OR REPLACE AND COMPILE JAVA SOURCE NAMED "DemoClass" AS
public class DemoClass {
  public static String hello() {
    return "hello";
  }
}
/
SELECT 2 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "CREATE JAVA SOURCE should keep Java body semicolons inside one statement until slash delimiter: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("CREATE OR REPLACE AND COMPILE JAVA SOURCE NAMED \"DemoClass\" AS"),
        "first statement should preserve JAVA SOURCE header: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("return \"hello\";"),
        "first statement should preserve Java body semicolon: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_format_items_oracle_create_java_source_keeps_body_until_slash() {
    let sql = r#"CREATE OR REPLACE AND COMPILE JAVA SOURCE NAMED "DemoClass" AS
public class DemoClass {
  public static String hello() {
    return "hello";
  }
}
/
SELECT 2 FROM dual;"#;
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        items.iter().any(|item| matches!(item, FormatItem::Slash)),
        "CREATE JAVA SOURCE should keep SQL*Plus slash delimiter in format items"
    );
    assert_eq!(
        stmts.len(),
        2,
        "split_format_items should keep JAVA SOURCE body as one statement and split trailing SELECT: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("CREATE OR REPLACE AND COMPILE JAVA SOURCE NAMED \"DemoClass\" AS"),
        "first formatted statement should preserve JAVA SOURCE header: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("return \"hello\";"),
        "first formatted statement should preserve Java body semicolon: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_create_wrapped_keeps_body_until_slash() {
    let sql = "CREATE OR REPLACE PROCEDURE wrapped_demo
WRAPPED
a000000
1
abcd;
efgh;
/
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "CREATE ... WRAPPED should keep wrapped body semicolons inside one statement until slash delimiter: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with(
            "CREATE OR REPLACE PROCEDURE wrapped_demo
WRAPPED"
        ),
        "first statement should preserve WRAPPED header: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains(
            "abcd;
efgh;"
        ),
        "first statement should preserve wrapped body with internal semicolons: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_format_items_oracle_create_wrapped_keeps_body_until_slash() {
    let sql = "CREATE OR REPLACE PROCEDURE wrapped_demo
WRAPPED
a000000
1
abcd;
efgh;
/
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        items.iter().any(|item| matches!(item, FormatItem::Slash)),
        "CREATE WRAPPED should keep SQL*Plus slash delimiter in format items"
    );
    assert_eq!(
        stmts.len(),
        2,
        "split_format_items should keep WRAPPED body as one statement and split trailing SELECT: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with(
            "CREATE OR REPLACE PROCEDURE wrapped_demo
WRAPPED"
        ),
        "first formatted statement should preserve WRAPPED header: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains(
            "abcd;
efgh;"
        ),
        "first formatted statement should preserve wrapped body semicolons: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_cte_using_function_call_splits_normally() {
    let sql =
        "WITH cte AS (SELECT 1 AS n FROM dual)\nSELECT ABS(n) AS v FROM cte;\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "regular CTE with scalar FUNCTION call should split on first statement terminator: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH cte AS (SELECT 1 AS n FROM dual)"),
        "first statement should preserve CTE query: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("SELECT ABS(n) AS v FROM cte"),
        "first statement should include scalar function call in SELECT list: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_nested_with_function_subquery_splits_normally() {
    let sql = "SELECT *\nFROM (\n  WITH\n    FUNCTION inner_f RETURN NUMBER IS\n    BEGIN\n      RETURN 1;\n    END;\n  SELECT inner_f() AS v FROM dual\n) t;\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "WITH FUNCTION inside subquery must not suppress top-level split: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("SELECT *\nFROM (\n  WITH\n    FUNCTION inner_f RETURN NUMBER IS"),
        "first statement should preserve nested WITH FUNCTION subquery: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("SELECT 2 FROM dual"),
        "second statement should start with trailing SELECT: {}",
        stmts[1]
    );
}

#[test]
fn test_split_script_items_oracle_parenthesized_with_function_cte_splits_normally() {
    let sql = "WITH outer_cte AS (\n  WITH\n    FUNCTION inner_f RETURN NUMBER IS\n    BEGIN\n      RETURN 1;\n    END;\n  SELECT inner_f() AS v FROM dual\n)\nSELECT * FROM outer_cte;\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "parenthesized nested WITH FUNCTION CTE must still split at top-level semicolon: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("WITH outer_cte AS (\n  WITH\n    FUNCTION inner_f RETURN NUMBER IS"),
        "first statement should preserve nested WITH FUNCTION CTE: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("SELECT * FROM outer_cte"),
        "first statement should include main outer SELECT: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_recursive_search_cycle_set_clauses_not_tool_commands() {
    let sql = "WITH t (id, parent_id) AS (\n  SELECT 1, NULL FROM dual\n  UNION ALL\n  SELECT id + 1, id FROM t WHERE id < 3\n)\nSEARCH DEPTH FIRST BY id SET order_col\nCYCLE id SET cycle_mark TO 'Y' DEFAULT 'N'\nSELECT id, parent_id FROM t;\nSELECT 2 FROM dual;";

    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "recursive WITH SEARCH/CYCLE clauses must remain in a single SQL statement: {stmts:?}"
    );
    assert!(
        stmts[0].contains("SEARCH DEPTH FIRST BY id SET order_col"),
        "SEARCH ... SET clause should remain in first statement: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("CYCLE id SET cycle_mark TO 'Y' DEFAULT 'N'"),
        "CYCLE ... SET clause should remain in first statement: {}",
        stmts[0]
    );
    assert_eq!(stmts[1], "SELECT 2 FROM dual");

    assert!(
        items
            .iter()
            .all(|item| !matches!(item, ScriptItem::ToolCommand(_))),
        "SEARCH/CYCLE SET clauses must not be parsed as SQL*Plus SET tool commands: {items:?}"
    );
}

#[test]
fn test_split_script_items_oracle_create_view_as_with_function_keeps_single_statement() {
    let sql = "CREATE OR REPLACE VIEW v_with_fn AS\nWITH\n  FUNCTION f RETURN NUMBER IS\n  BEGIN\n    RETURN 1;\n  END;\nSELECT f() AS v FROM dual;\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "CREATE VIEW ... AS WITH FUNCTION must remain one statement until main SELECT terminator: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("CREATE OR REPLACE VIEW v_with_fn AS"),
        "first statement should preserve CREATE VIEW header: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("FUNCTION f RETURN NUMBER IS"),
        "first statement should keep WITH FUNCTION declaration: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("SELECT f() AS v FROM dual"),
        "first statement should include main SELECT body: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_format_items_oracle_create_view_as_with_function_keeps_single_statement() {
    let sql = "CREATE OR REPLACE VIEW v_with_fn AS\nWITH\n  FUNCTION f RETURN NUMBER IS\n  BEGIN\n    RETURN 1;\n  END;\nSELECT f() AS v FROM dual;\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        stmts.len(),
        2,
        "split_format_items must keep CREATE VIEW ... AS WITH FUNCTION together: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("CREATE OR REPLACE VIEW v_with_fn AS"),
        "first formatted statement should preserve CREATE VIEW header: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("FUNCTION f RETURN NUMBER IS"),
        "first formatted statement should keep WITH FUNCTION declaration: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("SELECT f() AS v FROM dual"),
        "first formatted statement should include main SELECT body: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_simple_trigger_with_compound_identifier_splits_normally() {
    let sql = r#"CREATE OR REPLACE TRIGGER trg_compound_name
BEFORE INSERT ON t
FOR EACH ROW
DECLARE
  v_compound NUMBER := 1;
BEGIN
  IF v_compound = 1 THEN
    NULL;
  END IF;
END;
SELECT 2 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "simple trigger that mentions COMPOUND-like identifier must split after END; got: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("CREATE OR REPLACE TRIGGER trg_compound_name"),
        "first statement should preserve trigger body: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_simple_trigger_when_clause_compound_identifier_splits_normally() {
    let sql = r#"CREATE OR REPLACE TRIGGER trg_compound_when
BEFORE INSERT ON t
FOR EACH ROW
WHEN (NEW.COMPOUND IS NULL)
BEGIN
  NULL;
END;
SELECT 2 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "simple trigger WHEN clause identifier COMPOUND must not be parsed as COMPOUND TRIGGER: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("CREATE OR REPLACE TRIGGER trg_compound_when"),
        "first statement should preserve simple trigger body: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_simple_trigger_referencing_new_old_aliases() {
    let sql = r#"CREATE OR REPLACE TRIGGER trg_ref_alias
BEFORE INSERT OR UPDATE ON t
REFERENCING NEW AS n OLD AS o
FOR EACH ROW
BEGIN
  NULL;
END;
SELECT 2 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "simple trigger REFERENCING ... AS aliases must not create fake AS/IS block depth: {stmts:?}"
    );
    assert!(
        stmts[0].contains("REFERENCING NEW AS n OLD AS o"),
        "first statement should preserve REFERENCING aliases: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_simple_trigger_referencing_new_old_is_aliases() {
    let sql = r#"CREATE OR REPLACE TRIGGER trg_ref_alias_is
BEFORE INSERT OR UPDATE ON t
REFERENCING NEW IS n OLD IS o
FOR EACH ROW
IS
BEGIN
  NULL;
END;
SELECT 5 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "simple trigger REFERENCING ... IS aliases must not consume the body IS header: {stmts:?}"
    );
    assert!(
        stmts[0].contains("REFERENCING NEW IS n OLD IS o"),
        "first statement should preserve REFERENCING IS aliases: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains(
            "FOR EACH ROW
IS
BEGIN"
        ),
        "first statement should preserve trigger body IS header: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 5 FROM dual"));
}

#[test]
fn test_split_script_items_simple_trigger_is_header_splits_normally() {
    let sql = r#"CREATE OR REPLACE TRIGGER trg_is_header
BEFORE INSERT ON t
FOR EACH ROW
IS
BEGIN
  NULL;
END;
SELECT 4 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "simple trigger IS header must keep trigger body as one statement; got: {stmts:?}"
    );
    assert!(
        stmts[0].contains("FOR EACH ROW\nIS\nBEGIN"),
        "first statement should preserve simple trigger IS header: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 4 FROM dual"));
}

#[test]
fn test_split_script_items_simple_trigger_as_header_with_declaration_keeps_single_trigger_statement() {
    let sql = r#"CREATE OR REPLACE TRIGGER trg_as_header_decl
BEFORE INSERT ON t
FOR EACH ROW
AS
  v_count NUMBER;
BEGIN
  v_count := 1;
END;
SELECT 6 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "simple trigger AS header with declaration must not split at declaration semicolon: {stmts:?}"
    );
    assert!(
        stmts[0].contains("AS\n  v_count NUMBER;\nBEGIN"),
        "first statement should preserve AS declarative section: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 6 FROM dual"));
}

#[test]
fn test_split_script_items_simple_trigger_when_with_parenthesized_as_expression() {
    let sql = r#"CREATE OR REPLACE TRIGGER trg_when_case
BEFORE INSERT ON t
FOR EACH ROW
WHEN ((CASE WHEN NEW.status = 'A' THEN 1 ELSE 0 END) = 1)
BEGIN
  NULL;
END;
SELECT 2 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "simple trigger WHEN expression containing nested parentheses must not affect AS/IS block detection: {stmts:?}"
    );
    assert!(
        stmts[0].contains("WHEN ((CASE WHEN NEW.status = 'A' THEN 1 ELSE 0 END) = 1)"),
        "first statement should preserve WHEN expression: {}",
        stmts[0]
    );
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_whenever_statement_head() {
    let sql = "WITH\n  FUNCTION f RETURN NUMBER IS\n  BEGIN\n    RETURN 1;\n  END;\nWHENEVER SQLERROR EXIT SQL.SQLCODE\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);

    assert!(
        matches!(&items[0], ScriptItem::Statement(stmt) if stmt.contains("FUNCTION f RETURN NUMBER IS") && !stmt.contains("WHENEVER SQLERROR")),
        "first item should keep only WITH FUNCTION declaration statement: {items:?}"
    );
    assert!(
        matches!(&items[1], ScriptItem::ToolCommand(ToolCommand::WheneverSqlError { exit, action }) if *exit && action.as_deref() == Some("SQL.SQLCODE")),
        "second item should parse WHENEVER SQLERROR EXIT SQL.SQLCODE: {items:?}"
    );
    assert!(
        matches!(&items[2], ScriptItem::Statement(stmt) if stmt.starts_with("SELECT 2 FROM dual")),
        "third item should be trailing SELECT statement: {items:?}"
    );
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_variable_statement_head() {
    let sql = "WITH\n  FUNCTION f RETURN NUMBER IS\n  BEGIN\n    RETURN 1;\n  END;\nVARIABLE v NUMBER\nPRINT v\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);

    assert!(
        matches!(&items[0], ScriptItem::Statement(stmt) if stmt.contains("FUNCTION f RETURN NUMBER IS") && !stmt.contains("VARIABLE v NUMBER")),
        "first item should keep only WITH FUNCTION declaration statement: {items:?}"
    );
    assert!(
        matches!(&items[1], ScriptItem::ToolCommand(ToolCommand::Var { name, .. }) if name == "v"),
        "second item should parse VARIABLE command: {items:?}"
    );
    assert!(
        matches!(&items[2], ScriptItem::ToolCommand(ToolCommand::Print { name }) if name.as_deref() == Some("v")),
        "third item should parse PRINT command: {items:?}"
    );
    assert!(
        matches!(&items[3], ScriptItem::Statement(stmt) if stmt.starts_with("SELECT 2 FROM dual")),
        "fourth item should be trailing SELECT statement: {items:?}"
    );
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_passw_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
PASSW scott
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);

    assert!(
        matches!(&items[0], ScriptItem::Statement(stmt) if stmt.contains("FUNCTION f RETURN NUMBER IS") && !stmt.contains("PASSW scott")),
        "first item should keep only WITH FUNCTION declaration statement: {items:?}"
    );
    assert!(
        matches!(&items[1], ScriptItem::ToolCommand(ToolCommand::Unsupported { raw, message, is_error }) if raw == "PASSW scott" && message.contains("PASSWORD") && *is_error),
        "second item should classify PASSW command as unsupported SQL*Plus command without leaking into SQL statement: {items:?}"
    );
    assert!(
        matches!(&items[2], ScriptItem::Statement(stmt) if stmt.starts_with("SELECT 2 FROM dual")),
        "third item should be trailing SELECT statement: {items:?}"
    );
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_connect_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
CONNECT scott/tiger@localhost:1521/ORCL
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);

    assert!(
        matches!(&items[0], ScriptItem::Statement(stmt) if stmt.contains("FUNCTION f RETURN NUMBER IS") && !stmt.contains("CONNECT scott/tiger@localhost:1521/ORCL")),
        "first item should keep only WITH FUNCTION declaration statement: {items:?}"
    );
    assert!(
        matches!(&items[1], ScriptItem::ToolCommand(ToolCommand::Connect { username, host, port, service_name, .. }) if username == "scott" && host == "localhost" && *port == 1521 && service_name == "ORCL"),
        "second item should parse CONNECT command: {items:?}"
    );
    assert!(
        matches!(&items[2], ScriptItem::Statement(stmt) if stmt.starts_with("SELECT 2 FROM dual")),
        "third item should be trailing SELECT statement: {items:?}"
    );
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_conn_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
CONN
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);

    assert!(
        matches!(&items[0], ScriptItem::Statement(stmt) if stmt.contains("FUNCTION f RETURN NUMBER IS") && !stmt.contains("CONN")),
        "first item should keep only WITH FUNCTION declaration statement: {items:?}"
    );
    assert!(
        matches!(&items[1], ScriptItem::ToolCommand(ToolCommand::Unsupported { message, is_error: true, .. }) if message.contains("CONNECT requires connection string")),
        "second item should classify bare CONN as CONNECT syntax error command: {items:?}"
    );
    assert!(
        matches!(&items[2], ScriptItem::Statement(stmt) if stmt.starts_with("SELECT 2 FROM dual")),
        "third item should be trailing SELECT statement: {items:?}"
    );
}


#[test]
fn test_split_script_items_oracle_with_function_recovers_to_define_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
DEFINE answer = 42
SELECT &answer FROM dual;";
    let items = QueryExecutor::split_script_items(sql);

    assert!(
        matches!(&items[0], ScriptItem::Statement(stmt) if stmt.contains("FUNCTION f RETURN NUMBER IS") && !stmt.contains("DEFINE answer = 42")),
        "first item should keep only WITH FUNCTION declaration statement: {items:?}"
    );
    assert!(
        matches!(&items[1], ScriptItem::ToolCommand(ToolCommand::Define { name, value }) if name == "answer" && value == "42"),
        "second item should parse DEFINE command: {items:?}"
    );
    assert!(
        matches!(&items[2], ScriptItem::Statement(stmt) if stmt.starts_with("SELECT &answer FROM dual")),
        "third item should be trailing SELECT statement: {items:?}"
    );
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_disconnect_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
DISCONNECT
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);

    assert!(
        matches!(&items[0], ScriptItem::Statement(stmt) if stmt.contains("FUNCTION f RETURN NUMBER IS") && !stmt.contains("DISCONNECT")),
        "first item should keep only WITH FUNCTION declaration statement: {items:?}"
    );
    assert!(
        matches!(&items[1], ScriptItem::ToolCommand(ToolCommand::Disconnect)),
        "second item should parse DISCONNECT command: {items:?}"
    );
    assert!(
        matches!(&items[2], ScriptItem::Statement(stmt) if stmt.starts_with("SELECT 2 FROM dual")),
        "third item should be trailing SELECT statement: {items:?}"
    );
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_password_statement_head() {
    let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
PASSWORD scott
SELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);

    assert!(
        matches!(&items[0], ScriptItem::Statement(stmt) if stmt.contains("FUNCTION f RETURN NUMBER IS") && !stmt.contains("PASSWORD scott")),
        "first item should keep only WITH FUNCTION declaration statement: {items:?}"
    );
    assert!(
        matches!(&items[1], ScriptItem::ToolCommand(ToolCommand::Unsupported { raw, message, is_error }) if raw == "PASSWORD scott" && message.contains("PASSWORD") && *is_error),
        "second item should classify PASSWORD command as unsupported SQL*Plus command without leaking into SQL statement: {items:?}"
    );
    assert!(
        matches!(&items[2], ScriptItem::Statement(stmt) if stmt.starts_with("SELECT 2 FROM dual")),
        "third item should be trailing SELECT statement: {items:?}"
    );
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_run_script_statement_head() {
    let sql = r#"WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
@child.sql
SELECT 2 FROM dual;"#;

    let items = QueryExecutor::split_script_items(sql);

    assert!(
        matches!(items.first(), Some(ScriptItem::Statement(stmt)) if stmt.contains("WITH") && stmt.contains("FUNCTION f")),
        "first item should keep only WITH FUNCTION declaration statement: {items:?}"
    );
    assert!(
        matches!(items.get(1), Some(ScriptItem::ToolCommand(ToolCommand::RunScript { path, relative_to_caller })) if path == "child.sql" && !relative_to_caller),
        "second item should parse @child.sql as run-script command: {items:?}"
    );
    assert!(
        matches!(items.get(2), Some(ScriptItem::Statement(stmt)) if stmt.trim_start().starts_with("SELECT 2 FROM dual")),
        "third item should be trailing SELECT statement: {items:?}"
    );
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_relative_run_script_statement_head() {
    let sql = r#"WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
@@child.sql
SELECT 2 FROM dual;"#;

    let items = QueryExecutor::split_script_items(sql);

    assert!(
        matches!(items.first(), Some(ScriptItem::Statement(stmt)) if stmt.contains("WITH") && stmt.contains("FUNCTION f")),
        "first item should keep only WITH FUNCTION declaration statement: {items:?}"
    );
    assert!(
        matches!(items.get(1), Some(ScriptItem::ToolCommand(ToolCommand::RunScript { path, relative_to_caller })) if path == "child.sql" && *relative_to_caller),
        "second item should parse @@child.sql as relative run-script command: {items:?}"
    );
    assert!(
        matches!(items.get(2), Some(ScriptItem::Statement(stmt)) if stmt.trim_start().starts_with("SELECT 2 FROM dual")),
        "third item should be trailing SELECT statement: {items:?}"
    );
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_start_script_statement_head() {
    let sql = r#"WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
START child.sql
SELECT 2 FROM dual;"#;

    let items = QueryExecutor::split_script_items(sql);

    assert!(
        matches!(items.first(), Some(ScriptItem::Statement(stmt)) if stmt.contains("WITH") && stmt.contains("FUNCTION f")),
        "first item should keep only WITH FUNCTION declaration statement: {items:?}"
    );
    assert!(
        matches!(items.get(1), Some(ScriptItem::ToolCommand(ToolCommand::RunScript { path, relative_to_caller })) if path == "child.sql" && !relative_to_caller),
        "second item should parse START child.sql as run-script command: {items:?}"
    );
    assert!(
        matches!(items.get(2), Some(ScriptItem::Statement(stmt)) if stmt.trim_start().starts_with("SELECT 2 FROM dual")),
        "third item should be trailing SELECT statement: {items:?}"
    );
}

#[test]
fn test_split_format_items_oracle_with_function_recovers_to_start_script_statement_head() {
    let sql = r#"WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
START child.sql
SELECT 2 FROM dual;"#;

    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        stmts.first().is_some_and(|stmt| {
            stmt.contains("WITH")
                && stmt.contains("FUNCTION f")
                && !stmt.contains("START child.sql")
        }),
        "first formatted statement should keep only WITH FUNCTION declaration statement: {stmts:?}"
    );
    assert!(
        matches!(items.get(1), Some(FormatItem::ToolCommand(ToolCommand::RunScript { path, relative_to_caller })) if path == "child.sql" && !relative_to_caller),
        "second item should parse START child.sql as run-script command: {items:?}"
    );
    assert!(
        stmts
            .get(1)
            .is_some_and(|stmt| stmt.trim_start().starts_with("SELECT 2 FROM dual")),
        "second formatted statement should be trailing SELECT statement: {stmts:?}"
    );
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_run_keyword_statement_head() {
    let sql = r#"WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
RUN child.sql
SELECT 2 FROM dual;"#;

    let items = QueryExecutor::split_script_items(sql);

    assert!(
        matches!(items.first(), Some(ScriptItem::Statement(stmt)) if stmt.contains("WITH") && stmt.contains("FUNCTION f")),
        "first item should keep only WITH FUNCTION declaration statement: {items:?}"
    );
    assert!(
        matches!(items.get(1), Some(ScriptItem::ToolCommand(ToolCommand::RunScript { path, relative_to_caller })) if path == "child.sql" && !relative_to_caller),
        "second item should parse RUN child.sql as run-script command: {items:?}"
    );
    assert!(
        matches!(items.get(2), Some(ScriptItem::Statement(stmt)) if stmt.trim_start().starts_with("SELECT 2 FROM dual")),
        "third item should be trailing SELECT statement: {items:?}"
    );
}

#[test]
fn test_split_script_items_oracle_with_function_recovers_to_r_keyword_statement_head() {
    let sql = r#"WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
R child.sql
SELECT 2 FROM dual;"#;

    let items = QueryExecutor::split_script_items(sql);

    assert!(
        matches!(items.first(), Some(ScriptItem::Statement(stmt)) if stmt.contains("WITH") && stmt.contains("FUNCTION f")),
        "first item should keep only WITH FUNCTION declaration statement: {items:?}"
    );
    assert!(
        matches!(items.get(1), Some(ScriptItem::ToolCommand(ToolCommand::RunScript { path, relative_to_caller })) if path == "child.sql" && !relative_to_caller),
        "second item should parse R child.sql as run-script command: {items:?}"
    );
    assert!(
        matches!(items.get(2), Some(ScriptItem::Statement(stmt)) if stmt.trim_start().starts_with("SELECT 2 FROM dual")),
        "third item should be trailing SELECT statement: {items:?}"
    );
}

#[test]
fn test_split_format_items_oracle_with_function_recovers_to_run_keyword_statement_head() {
    let sql = r#"WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
RUN child.sql
SELECT 2 FROM dual;"#;

    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        stmts.first().is_some_and(|stmt| {
            stmt.contains("WITH")
                && stmt.contains("FUNCTION f")
                && !stmt.contains("RUN child.sql")
        }),
        "first formatted statement should keep only WITH FUNCTION declaration statement: {stmts:?}"
    );
    assert!(
        matches!(items.get(1), Some(FormatItem::ToolCommand(ToolCommand::RunScript { path, relative_to_caller })) if path == "child.sql" && !relative_to_caller),
        "second item should parse RUN child.sql as run-script command: {items:?}"
    );
    assert!(
        stmts
            .get(1)
            .is_some_and(|stmt| stmt.trim_start().starts_with("SELECT 2 FROM dual")),
        "second formatted statement should be trailing SELECT statement: {stmts:?}"
    );
}

#[test]
fn test_split_format_items_oracle_with_function_recovers_to_r_keyword_statement_head() {
    let sql = r#"WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
R child.sql
SELECT 2 FROM dual;"#;

    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        stmts.first().is_some_and(|stmt| {
            stmt.contains("WITH")
                && stmt.contains("FUNCTION f")
                && !stmt.contains("R child.sql")
        }),
        "first formatted statement should keep only WITH FUNCTION declaration statement: {stmts:?}"
    );
    assert!(
        matches!(items.get(1), Some(FormatItem::ToolCommand(ToolCommand::RunScript { path, relative_to_caller })) if path == "child.sql" && !relative_to_caller),
        "second item should parse R child.sql as run-script command: {items:?}"
    );
    assert!(
        stmts
            .get(1)
            .is_some_and(|stmt| stmt.trim_start().starts_with("SELECT 2 FROM dual")),
        "second formatted statement should be trailing SELECT statement: {stmts:?}"
    );
}

#[test]
fn test_split_script_items_external_language_clause_splits_before_run_script_command() {
    let sql = r#"CREATE OR REPLACE FUNCTION ext_lang_cmd RETURN NUMBER
AS LANGUAGE C;
@child.sql
SELECT 2 FROM dual;"#;

    let items = QueryExecutor::split_script_items(sql);

    assert!(
        matches!(items.first(), Some(ScriptItem::Statement(stmt)) if stmt.contains("AS LANGUAGE C") && !stmt.contains("@child.sql")),
        "first item should keep only LANGUAGE call spec statement: {items:?}"
    );
    assert!(
        matches!(items.get(1), Some(ScriptItem::ToolCommand(ToolCommand::RunScript { path, relative_to_caller })) if path == "child.sql" && !relative_to_caller),
        "second item should parse @child.sql as run-script command: {items:?}"
    );
    assert!(
        matches!(items.get(2), Some(ScriptItem::Statement(stmt)) if stmt.trim_start().starts_with("SELECT 2 FROM dual")),
        "third item should be trailing SELECT statement: {items:?}"
    );
}

#[test]
fn test_split_format_items_external_language_clause_splits_before_run_script_command() {
    let sql = r#"CREATE OR REPLACE FUNCTION ext_lang_cmd RETURN NUMBER
AS LANGUAGE C;
@child.sql
SELECT 2 FROM dual;"#;

    let items = QueryExecutor::split_format_items(sql);
    let stmts: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            FormatItem::Statement(stmt) => Some(stmt.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        stmts
            .first()
            .is_some_and(|stmt| stmt.contains("AS LANGUAGE C") && !stmt.contains("@child.sql")),
        "first formatted statement should keep only LANGUAGE call spec statement: {stmts:?}"
    );
    assert!(
        matches!(items.get(1), Some(FormatItem::ToolCommand(ToolCommand::RunScript { path, relative_to_caller })) if path == "child.sql" && !relative_to_caller),
        "second item should parse @child.sql as run-script command: {items:?}"
    );
    assert!(
        stmts
            .get(1)
            .is_some_and(|stmt| stmt.trim_start().starts_with("SELECT 2 FROM dual")),
        "second formatted statement should be trailing SELECT statement: {stmts:?}"
    );
}

#[test]
fn test_split_script_items_create_noforce_trigger_splits_before_trailing_select() {
    let sql = r#"CREATE OR REPLACE NOFORCE TRIGGER trg_noforce
BEFORE INSERT ON t
FOR EACH ROW
BEGIN
  NULL;
END;
SELECT 2 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "CREATE NOFORCE TRIGGER should split before trailing SELECT, got: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("CREATE OR REPLACE NOFORCE TRIGGER trg_noforce"),
        "first statement should preserve NOFORCE TRIGGER DDL: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("SELECT 2 FROM dual"),
        "second statement should be trailing SELECT: {}",
        stmts[1]
    );
}

#[test]
fn test_split_script_items_create_forward_crossedition_trigger_splits_before_trailing_select() {
    let sql = r#"CREATE OR REPLACE FORWARD CROSSEDITION TRIGGER trg_forward
BEFORE INSERT ON t
BEGIN
  NULL;
END;
SELECT 2 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "CREATE FORWARD CROSSEDITION TRIGGER should split before trailing SELECT, got: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("CREATE OR REPLACE FORWARD CROSSEDITION TRIGGER trg_forward"),
        "first statement should preserve FORWARD CROSSEDITION TRIGGER DDL: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("SELECT 2 FROM dual"),
        "second statement should be trailing SELECT: {}",
        stmts[1]
    );
}

#[test]
fn test_split_script_items_create_reverse_crossedition_trigger_splits_before_trailing_select() {
    let sql = r#"CREATE OR REPLACE REVERSE CROSSEDITION TRIGGER trg_reverse
BEFORE INSERT ON t
BEGIN
  NULL;
END;
SELECT 3 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "CREATE REVERSE CROSSEDITION TRIGGER should split before trailing SELECT, got: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("CREATE OR REPLACE REVERSE CROSSEDITION TRIGGER trg_reverse"),
        "first statement should preserve REVERSE CROSSEDITION TRIGGER DDL: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("SELECT 3 FROM dual"),
        "second statement should be trailing SELECT: {}",
        stmts[1]
    );
}

#[test]
fn test_split_script_items_create_if_not_exists_procedure_splits_before_trailing_select() {
    let sql = r#"CREATE IF NOT EXISTS PROCEDURE p_if_not_exists
IS
BEGIN
  NULL;
END;
SELECT 2 FROM dual;"#;
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "CREATE IF NOT EXISTS PROCEDURE should split before trailing SELECT, got: {stmts:?}"
    );
    assert!(
        stmts[0].starts_with("CREATE IF NOT EXISTS PROCEDURE p_if_not_exists"),
        "first statement should preserve IF NOT EXISTS PROCEDURE DDL: {}",
        stmts[0]
    );
    assert!(
        stmts[1].starts_with("SELECT 2 FROM dual"),
        "second statement should be trailing SELECT: {}",
        stmts[1]
    );
}

#[test]
fn test_split_script_items_oracle_with_function_without_semicolon_uses_slash_terminator() {
    let sql = "WITH FUNCTION f RETURN NUMBER IS\nBEGIN\n  RETURN 1;\nEND\n/\nSELECT 2 FROM dual;";
    let items = QueryExecutor::split_script_items(sql);
    let stmts = get_statements(&items);

    assert_eq!(
        stmts.len(),
        2,
        "expected WITH FUNCTION and SELECT split, got: {stmts:?}"
    );
    assert!(stmts[0].starts_with("WITH FUNCTION f RETURN NUMBER IS"));
    assert!(stmts[0].contains("RETURN 1;"));
    assert!(stmts[1].starts_with("SELECT 2 FROM dual"));
}
