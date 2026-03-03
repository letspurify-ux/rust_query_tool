use super::*;
use crate::ui::sql_editor::SqlEditorWidget;

fn tokenize(sql: &str) -> Vec<SqlToken> {
    SqlEditorWidget::tokenize_sql(sql)
}

/// Helper: tokenize SQL up to `|` marker (cursor position).
/// Returns (full_tokens, cursor_token_len).
fn split_at_cursor(sql: &str) -> (Vec<SqlToken>, usize) {
    use crate::ui::sql_editor::query_text::tokenize_sql_spanned;

    let cursor_pos = sql
        .find('|')
        .expect("SQL must contain '|' as cursor marker");
    let before = &sql[..cursor_pos];
    let after = &sql[cursor_pos + 1..];
    let full = format!("{}{}", before, after);
    let token_spans = tokenize_sql_spanned(&full);
    let cursor_token_len = token_spans.partition_point(|span| span.end <= cursor_pos);
    let full_tokens = token_spans.into_iter().map(|span| span.token).collect();
    (full_tokens, cursor_token_len)
}

fn analyze(sql: &str) -> CursorContext {
    let (full, cursor_token_len) = split_at_cursor(sql);
    analyze_cursor_context(&full, cursor_token_len)
}

fn table_names(ctx: &CursorContext) -> Vec<String> {
    ctx.tables_in_scope
        .iter()
        .map(|t| t.name.to_uppercase())
        .collect()
}

fn cte_names(ctx: &CursorContext) -> Vec<String> {
    ctx.ctes.iter().map(|c| c.name.to_uppercase()).collect()
}

// ─── Phase detection tests ───────────────────────────────────────────────

#[test]
fn phase_initial_empty() {
    let ctx = analyze("|");
    assert_eq!(ctx.phase, SqlPhase::Initial);
}

#[test]
fn phase_select_list() {
    let ctx = analyze("SELECT |");
    assert_eq!(ctx.phase, SqlPhase::SelectList);
    assert!(ctx.phase.is_column_context());
}

#[test]
fn phase_select_list_after_column() {
    let ctx = analyze("SELECT a, |");
    assert_eq!(ctx.phase, SqlPhase::SelectList);
}

#[test]
fn phase_select_list_inside_plsql_for_in_subquery() {
    let ctx = analyze(
        r#"CREATE OR REPLACE PACKAGE BODY oqt_demo_pkg AS
    PROCEDURE proc_fill_result_table (p_run_id IN NUMBER, p_min_sal IN NUMBER) IS
        v_row_no NUMBER := 0;
    BEGIN
        DELETE FROM oqt_tmp_result WHERE run_id = p_run_id;
        FOR r IN (
            SELECT emp_id,
                |,
                sal
            FROM oqt_emp
            WHERE sal >= p_min_sal
            ORDER BY sal
        ) LOOP
            v_row_no := v_row_no + 1;
        END LOOP;
    END;
END oqt_demo_pkg;"#,
    );

    assert_eq!(ctx.phase, SqlPhase::SelectList);
    let names = table_names(&ctx);
    assert!(
        names.iter().any(|name| name == "OQT_EMP"),
        "expected oqt_emp in scope, got {:?}",
        names
    );
}

#[test]
fn phase_from_clause() {
    let ctx = analyze("SELECT a FROM |");
    assert_eq!(ctx.phase, SqlPhase::FromClause);
    assert!(ctx.phase.is_table_context());
}

#[test]
fn phase_where_clause() {
    let ctx = analyze("SELECT a FROM t WHERE |");
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
    assert!(ctx.phase.is_column_context());
}

#[test]
fn phase_join_on_clause() {
    let ctx = analyze("SELECT a FROM t1 JOIN t2 ON |");
    assert_eq!(ctx.phase, SqlPhase::JoinCondition);
    assert!(ctx.phase.is_column_context());
}

#[test]
fn phase_group_by() {
    let ctx = analyze("SELECT a FROM t GROUP BY |");
    assert_eq!(ctx.phase, SqlPhase::GroupByClause);
    assert!(ctx.phase.is_column_context());
}

#[test]
fn phase_having() {
    let ctx = analyze("SELECT a FROM t GROUP BY a HAVING |");
    assert_eq!(ctx.phase, SqlPhase::HavingClause);
    assert!(ctx.phase.is_column_context());
}

#[test]
fn phase_order_by() {
    let ctx = analyze("SELECT a FROM t ORDER BY |");
    assert_eq!(ctx.phase, SqlPhase::OrderByClause);
    assert!(ctx.phase.is_column_context());
}

#[test]
fn phase_update_set() {
    let ctx = analyze("UPDATE t SET |");
    assert_eq!(ctx.phase, SqlPhase::SetClause);
    assert!(ctx.phase.is_column_context());
}

#[test]
fn phase_insert_into() {
    let ctx = analyze("INSERT INTO |");
    assert_eq!(ctx.phase, SqlPhase::IntoClause);
    assert!(ctx.phase.is_table_context());
}

#[test]
fn phase_values() {
    let ctx = analyze("INSERT INTO t (a) VALUES |");
    assert_eq!(ctx.phase, SqlPhase::ValuesClause);
}

#[test]
fn phase_connect_by() {
    let ctx = analyze("SELECT a FROM t START WITH a = 1 CONNECT BY |");
    assert_eq!(ctx.phase, SqlPhase::ConnectByClause);
    assert!(ctx.phase.is_column_context());
}

#[test]
fn phase_start_with() {
    let ctx = analyze("SELECT a FROM t START WITH |");
    assert_eq!(ctx.phase, SqlPhase::StartWithClause);
    assert!(ctx.phase.is_column_context());
}

// ─── Depth tracking tests ────────────────────────────────────────────────

#[test]
fn depth_zero_at_top_level() {
    let ctx = analyze("SELECT | FROM t");
    assert_eq!(ctx.depth, 0);
}

#[test]
fn depth_one_in_subquery() {
    let ctx = analyze("SELECT * FROM (SELECT |");
    assert_eq!(ctx.depth, 1);
    assert_eq!(ctx.phase, SqlPhase::SelectList);
}

#[test]
fn depth_two_in_nested_subquery() {
    let ctx = analyze("SELECT * FROM (SELECT * FROM (SELECT |");
    assert_eq!(ctx.depth, 2);
    assert_eq!(ctx.phase, SqlPhase::SelectList);
}

#[test]
fn depth_returns_to_zero_after_subquery() {
    let ctx = analyze("SELECT * FROM (SELECT 1 FROM dual) WHERE |");
    assert_eq!(ctx.depth, 0);
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
}

#[test]
fn depth_in_subquery_where_clause() {
    let ctx = analyze("SELECT * FROM (SELECT a FROM t WHERE |");
    assert_eq!(ctx.depth, 1);
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
}

#[test]
fn depth_in_subquery_from_clause() {
    let ctx = analyze("SELECT * FROM (SELECT a FROM |");
    assert_eq!(ctx.depth, 1);
    assert_eq!(ctx.phase, SqlPhase::FromClause);
}

// ─── Table collection tests ──────────────────────────────────────────────

#[test]
fn collect_single_table() {
    let ctx = analyze("SELECT | FROM employees");
    let names = table_names(&ctx);
    assert!(
        names.contains(&"EMPLOYEES".to_string()),
        "tables: {:?}",
        names
    );
}

#[test]
fn collect_multiple_tables() {
    let ctx = analyze("SELECT | FROM employees e, departments d");
    let names = table_names(&ctx);
    assert!(
        names.contains(&"EMPLOYEES".to_string()),
        "tables: {:?}",
        names
    );
    assert!(
        names.contains(&"DEPARTMENTS".to_string()),
        "tables: {:?}",
        names
    );
}

#[test]
fn collect_join_tables() {
    let ctx = analyze("SELECT | FROM employees e JOIN departments d ON e.dept_id = d.id");
    let names = table_names(&ctx);
    assert!(
        names.contains(&"EMPLOYEES".to_string()),
        "tables: {:?}",
        names
    );
    assert!(
        names.contains(&"DEPARTMENTS".to_string()),
        "tables: {:?}",
        names
    );
}

#[test]
fn collect_table_with_schema_prefix() {
    let ctx = analyze("SELECT | FROM hr.employees");
    let names = table_names(&ctx);
    assert!(
        names.contains(&"HR.EMPLOYEES".to_string()),
        "tables: {:?}",
        names
    );
}

#[test]
fn collect_quoted_table_and_alias() {
    let ctx = analyze(r#"SELECT "e".| FROM "Emp Table" "e""#);
    let names = table_names(&ctx);
    assert!(
        names.contains(&"EMP TABLE".to_string()),
        "quoted table should be normalized into scope: {:?}",
        names
    );
    assert!(
        ctx.tables_in_scope
            .iter()
            .any(|t| t.alias.as_deref() == Some("e")),
        "quoted alias should be normalized into scope: {:?}",
        ctx.tables_in_scope
    );
}

#[test]
fn collect_quoted_table_with_dot_keeps_lookup_safe_form() {
    let ctx = analyze(r#"SELECT | FROM "A.B" t"#);
    let names = table_names(&ctx);
    assert!(
        names.contains(&"\"A.B\"".to_string()),
        "quoted dotted table should preserve quoted form to avoid schema fallback ambiguity: {:?}",
        names
    );
}

#[test]
fn collect_table_ignores_numeric_starting_token() {
    let ctx = analyze("SELECT | FROM 123abc");
    let names = table_names(&ctx);
    assert!(
        !names.iter().any(|name| name == "123ABC"),
        "numeric-leading token should not be treated as table identifier: {:?}",
        names
    );
}

#[test]
fn extract_select_list_columns_ignores_numeric_literals() {
    let tokens = tokenize("SELECT 1e3, emp1, 42 FROM dual");
    let columns = extract_select_list_columns(&tokens);
    assert!(columns.iter().any(|name| name.eq_ignore_ascii_case("emp1")));
    assert!(!columns.iter().any(|name| name.eq_ignore_ascii_case("1e3")));
    assert!(!columns.iter().any(|name| name == "42"));
}

#[test]
fn collect_multiple_joins() {
    let ctx = analyze(
        "SELECT | FROM employees e \
         JOIN departments d ON e.dept_id = d.id \
         LEFT JOIN locations l ON d.loc_id = l.id",
    );
    let names = table_names(&ctx);
    assert!(names.contains(&"EMPLOYEES".to_string()));
    assert!(names.contains(&"DEPARTMENTS".to_string()));
    assert!(names.contains(&"LOCATIONS".to_string()));
}

#[test]
fn collect_table_aliases() {
    let ctx = analyze("SELECT | FROM employees e");
    assert!(ctx
        .tables_in_scope
        .iter()
        .any(|t| t.alias.as_deref() == Some("e")));
}

#[test]
fn collect_table_as_alias() {
    let ctx = analyze("SELECT | FROM employees AS emp");
    assert!(ctx
        .tables_in_scope
        .iter()
        .any(|t| t.alias.as_deref() == Some("emp")));
}

// ─── Subquery alias tests ────────────────────────────────────────────────

#[test]
fn subquery_alias_in_from() {
    let ctx = analyze("SELECT u.| FROM (SELECT id, name FROM users) u");
    let names = table_names(&ctx);
    assert!(
        names.iter().any(|n| n.eq_ignore_ascii_case("u")),
        "subquery alias 'u' should be in scope: {:?}",
        names
    );
}

#[test]
fn subquery_alias_with_as() {
    let ctx = analyze("SELECT sub.| FROM (SELECT id FROM t) AS sub");
    let names = table_names(&ctx);
    assert!(
        names.iter().any(|n| n.eq_ignore_ascii_case("sub")),
        "subquery alias 'sub' should be in scope: {:?}",
        names
    );
}

#[test]
fn subquery_alias_with_column_list_is_recognized() {
    let ctx = analyze("SELECT * FROM (SELECT 1 AS n FROM dual) sub(n) WHERE |");
    let names = table_names(&ctx);
    assert!(
        names.contains(&"SUB".to_string()),
        "subquery alias with column list should be in scope: {:?}",
        names
    );
}

#[test]
fn subquery_alias_with_as_and_column_list_is_recognized() {
    let ctx = analyze("SELECT * FROM (SELECT 1 AS n FROM dual) AS sub(n) WHERE |");
    let names = table_names(&ctx);
    assert!(
        names.contains(&"SUB".to_string()),
        "AS subquery alias with column list should be in scope: {:?}",
        names
    );
}

#[test]
fn subquery_alias_mixed_with_table() {
    let ctx = analyze("SELECT | FROM users u, (SELECT id FROM orders) o");
    let names = table_names(&ctx);
    assert!(names.contains(&"USERS".to_string()));
    assert!(
        names.iter().any(|n| n.eq_ignore_ascii_case("o")),
        "subquery alias 'o' should be in scope: {:?}",
        names
    );
}

// ─── CTE (WITH clause) tests ────────────────────────────────────────────

#[test]
fn cte_simple() {
    let ctx = analyze("WITH cte AS (SELECT 1 AS n FROM dual) SELECT | FROM cte");
    let cte_n = cte_names(&ctx);
    assert!(cte_n.contains(&"CTE".to_string()), "CTEs: {:?}", cte_n);
    let names = table_names(&ctx);
    assert!(
        names.iter().any(|n| n.eq_ignore_ascii_case("cte")),
        "CTE should be in table scope: {:?}",
        names
    );
}

#[test]
fn cte_multiple() {
    let ctx =
        analyze("WITH a AS (SELECT 1 FROM dual), b AS (SELECT 2 FROM dual) SELECT | FROM a, b");
    let cte_n = cte_names(&ctx);
    assert!(cte_n.contains(&"A".to_string()), "CTEs: {:?}", cte_n);
    assert!(cte_n.contains(&"B".to_string()), "CTEs: {:?}", cte_n);
}

#[test]
fn cte_with_explicit_columns() {
    let ctx = analyze("WITH cte(x, y) AS (SELECT 1, 2 FROM dual) SELECT | FROM cte");
    let cte_n = cte_names(&ctx);
    assert!(cte_n.contains(&"CTE".to_string()));
    let cte_def = ctx
        .ctes
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case("cte"))
        .unwrap();
    assert_eq!(cte_def.explicit_columns.len(), 2);
}

#[test]
fn cte_cursor_in_main_query_where() {
    let ctx = analyze("WITH temp AS (SELECT id, name FROM users) SELECT * FROM temp WHERE |");
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
    assert_eq!(ctx.depth, 0);
    let names = table_names(&ctx);
    assert!(names.iter().any(|n| n.eq_ignore_ascii_case("temp")));
}

#[test]
fn cte_cursor_in_cte_body() {
    let ctx = analyze("WITH temp AS (SELECT | FROM users) SELECT * FROM temp");
    assert_eq!(ctx.depth, 1);
    assert_eq!(ctx.phase, SqlPhase::SelectList);
}

#[test]
fn cte_with_nested_subquery() {
    let ctx =
        analyze("WITH temp AS (SELECT * FROM (SELECT id FROM inner_t) sub) SELECT | FROM temp");
    assert_eq!(ctx.depth, 0);
    assert_eq!(ctx.phase, SqlPhase::SelectList);
    let names = table_names(&ctx);
    assert!(names.iter().any(|n| n.eq_ignore_ascii_case("temp")));
}

#[test]
fn cte_with_mismatched_close_before_with_is_still_detected() {
    let ctx = analyze(") WITH temp AS (SELECT id FROM users) SELECT | FROM temp");
    let cte_n = cte_names(&ctx);
    assert!(
        cte_n.contains(&"TEMP".to_string()),
        "top-level WITH should be detected after unmatched close paren: {:?}",
        cte_n
    );
}

// ─── Complex nested query tests ─────────────────────────────────────────

#[test]
fn nested_subquery_in_where() {
    let ctx = analyze("SELECT * FROM employees WHERE dept_id IN (SELECT | FROM departments)");
    assert_eq!(ctx.depth, 1);
    assert_eq!(ctx.phase, SqlPhase::SelectList);
}

#[test]
fn nested_subquery_in_where_from() {
    let ctx = analyze("SELECT * FROM employees WHERE dept_id IN (SELECT dept_id FROM |");
    assert_eq!(ctx.depth, 1);
    assert_eq!(ctx.phase, SqlPhase::FromClause);
}

#[test]
fn correlated_subquery() {
    let ctx = analyze(
        "SELECT * FROM employees e WHERE salary > (SELECT AVG(salary) FROM employees e2 WHERE e2.dept_id = e.| )",
    );
    // Cursor is inside the subquery at depth 1
    assert_eq!(ctx.depth, 1);
}

#[test]
fn subquery_in_select_list() {
    let ctx = analyze(
        "SELECT (SELECT | FROM departments d WHERE d.id = e.dept_id) AS dept_name FROM employees e",
    );
    assert_eq!(ctx.depth, 1);
    assert_eq!(ctx.phase, SqlPhase::SelectList);
}

#[test]
fn inline_view_with_join() {
    let ctx = analyze(
        "SELECT | FROM (SELECT e.id, d.name FROM employees e JOIN departments d ON e.dept_id = d.id) v",
    );
    assert_eq!(ctx.depth, 0);
    assert_eq!(ctx.phase, SqlPhase::SelectList);
    let names = table_names(&ctx);
    assert!(names.iter().any(|n| n.eq_ignore_ascii_case("v")));
}

#[test]
fn triple_nested_subquery() {
    let ctx = analyze("SELECT * FROM (SELECT * FROM (SELECT | FROM innermost) mid) outer_q");
    assert_eq!(ctx.depth, 2);
    assert_eq!(ctx.phase, SqlPhase::SelectList);
}

// ─── UNION / set operation tests ─────────────────────────────────────────

#[test]
fn union_resets_phase_for_second_select() {
    let ctx = analyze("SELECT a FROM t1 UNION ALL SELECT | FROM t2");
    assert_eq!(ctx.phase, SqlPhase::SelectList);
    assert_eq!(ctx.depth, 0);
}

#[test]
fn union_collects_tables_from_both_parts() {
    let ctx = analyze("SELECT a FROM t1 UNION ALL SELECT | FROM t2");
    let names = table_names(&ctx);
    assert!(names.contains(&"T2".to_string()), "tables: {:?}", names);
}

// ─── Qualifier resolution tests ──────────────────────────────────────────

#[test]
fn resolve_qualifier_by_alias() {
    let tables = vec![ScopedTableRef {
        name: "employees".to_string(),
        alias: Some("e".to_string()),
        depth: 0,
        is_cte: false,
    }];
    let result = resolve_qualifier_tables("e", &tables);
    assert_eq!(result, vec!["employees"]);
}

#[test]
fn resolve_qualifier_by_table_name() {
    let tables = vec![ScopedTableRef {
        name: "employees".to_string(),
        alias: None,
        depth: 0,
        is_cte: false,
    }];
    let result = resolve_qualifier_tables("employees", &tables);
    assert_eq!(result, vec!["employees"]);
}

#[test]
fn resolve_qualifier_by_unqualified_name_for_schema_qualified_table() {
    let tables = vec![ScopedTableRef {
        name: "hr.employees".to_string(),
        alias: None,
        depth: 0,
        is_cte: false,
    }];
    let result = resolve_qualifier_tables("employees", &tables);
    assert_eq!(result, vec!["hr.employees"]);
}

#[test]
fn resolve_qualifier_case_insensitive() {
    let tables = vec![ScopedTableRef {
        name: "EMPLOYEES".to_string(),
        alias: Some("E".to_string()),
        depth: 0,
        is_cte: false,
    }];
    let result = resolve_qualifier_tables("e", &tables);
    assert_eq!(result, vec!["EMPLOYEES"]);
}

#[test]
fn resolve_qualifier_unknown_falls_back() {
    let tables = vec![ScopedTableRef {
        name: "employees".to_string(),
        alias: Some("e".to_string()),
        depth: 0,
        is_cte: false,
    }];
    let result = resolve_qualifier_tables("unknown", &tables);
    assert_eq!(result, vec!["unknown"]);
}

#[test]
fn resolve_qualifier_prefers_deeper_alias_scope() {
    let tables = vec![
        ScopedTableRef {
            name: "outer_table".to_string(),
            alias: Some("t".to_string()),
            depth: 0,
            is_cte: false,
        },
        ScopedTableRef {
            name: "inner_table".to_string(),
            alias: Some("t".to_string()),
            depth: 1,
            is_cte: false,
        },
    ];
    let result = resolve_qualifier_tables("t", &tables);
    assert_eq!(result, vec!["inner_table"]);
}

#[test]
fn resolve_qualifier_prefers_inner_alias_in_nested_query() {
    let ctx = analyze("SELECT * FROM outer_t t WHERE EXISTS (SELECT 1 FROM inner_t t WHERE t.|)");
    let result = resolve_qualifier_tables("t", &ctx.tables_in_scope);
    assert_eq!(result, vec!["inner_t"]);
}

#[test]
fn resolve_qualifier_matches_quoted_alias() {
    let tables = vec![ScopedTableRef {
        name: "Emp Table".to_string(),
        alias: Some("e".to_string()),
        depth: 0,
        is_cte: false,
    }];
    let result = resolve_qualifier_tables(r#""e""#, &tables);
    assert_eq!(result, vec!["Emp Table"]);
}

// ─── Comment handling tests ──────────────────────────────────────────────

#[test]
fn comments_dont_affect_phase_detection() {
    let ctx = analyze("SELECT /* this is a comment */ a FROM /* another */ t WHERE |");
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
}

#[test]
fn line_comment_doesnt_affect_phase() {
    let ctx = analyze("SELECT a\n-- comment\nFROM t\nWHERE |");
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
}

// ─── String literal handling tests ───────────────────────────────────────

#[test]
fn string_with_keywords_inside() {
    let ctx = analyze("SELECT 'FROM WHERE' FROM t WHERE |");
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
    let names = table_names(&ctx);
    assert!(names.contains(&"T".to_string()));
}

// ─── Multiple statement boundary tests ───────────────────────────────────

#[test]
fn semicolon_resets_state() {
    let ctx = analyze("SELECT 1 FROM dual; SELECT | FROM t2");
    assert_eq!(ctx.phase, SqlPhase::SelectList);
    let names = table_names(&ctx);
    assert!(names.contains(&"T2".to_string()));
    assert!(!names.contains(&"DUAL".to_string()));
}

#[test]
fn trailing_semicolon_preserves_current_statement_table_aliases() {
    let ctx = analyze("SELECT e.| FROM employees e;");
    let names = table_names(&ctx);
    assert!(
        names.contains(&"EMPLOYEES".to_string()),
        "tables: {:?}",
        names
    );

    let result = resolve_qualifier_tables("e", &ctx.tables_in_scope);
    assert_eq!(result, vec!["employees"]);
}

#[test]
fn trailing_semicolon_preserves_cte_alias_resolution() {
    let ctx = analyze(
        "WITH base AS (SELECT empno FROM emp), filtered AS (SELECT * FROM base) SELECT f.| FROM filtered f;",
    );
    let result = resolve_qualifier_tables("f", &ctx.tables_in_scope);
    assert_eq!(result, vec!["filtered"]);
}

// ─── UPDATE statement tests ──────────────────────────────────────────────

#[test]
fn update_target_table() {
    let ctx = analyze("UPDATE employees SET |");
    assert_eq!(ctx.phase, SqlPhase::SetClause);
    let names = table_names(&ctx);
    assert!(names.contains(&"EMPLOYEES".to_string()));
}

#[test]
fn update_with_where() {
    let ctx = analyze("UPDATE employees SET salary = 1000 WHERE |");
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
}

#[test]
fn update_with_alias_qualifier_resolution() {
    let ctx = analyze("UPDATE employees e SET e.| = 1000");
    assert_eq!(ctx.phase, SqlPhase::SetClause);

    let names = table_names(&ctx);
    assert!(
        names.contains(&"EMPLOYEES".to_string()),
        "tables: {:?}",
        names
    );

    let resolved = resolve_qualifier_tables("e", &ctx.tables_in_scope);
    assert_eq!(resolved, vec!["employees"]);
}

// ─── DELETE statement tests ──────────────────────────────────────────────

#[test]
fn delete_from() {
    let ctx = analyze("DELETE FROM employees WHERE |");
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
    let names = table_names(&ctx);
    assert!(names.contains(&"EMPLOYEES".to_string()));
}

#[test]
fn delete_with_alias_qualifier_resolution() {
    let ctx = analyze("DELETE FROM employees e WHERE e.|");
    assert_eq!(ctx.phase, SqlPhase::WhereClause);

    let resolved = resolve_qualifier_tables("e", &ctx.tables_in_scope);
    assert_eq!(resolved, vec!["employees"]);
}

// ─── INSERT statement tests ──────────────────────────────────────────────

#[test]
fn insert_column_list_context_after_target_table() {
    let ctx = analyze("INSERT INTO employees (|) VALUES (1)");
    assert_eq!(ctx.phase, SqlPhase::IntoClause);
    assert!(ctx.phase.is_table_context());

    let names = table_names(&ctx);
    assert!(
        names.contains(&"EMPLOYEES".to_string()),
        "tables: {:?}",
        names
    );
}

#[test]
fn insert_values_keeps_target_table_in_scope() {
    let ctx = analyze("INSERT INTO employees (id, name) VALUES (1, |)");
    assert_eq!(ctx.phase, SqlPhase::ValuesClause);

    let names = table_names(&ctx);
    assert!(
        names.contains(&"EMPLOYEES".to_string()),
        "tables: {:?}",
        names
    );
}

#[test]
fn insert_select_keeps_target_and_source_tables_in_scope() {
    let ctx = analyze("INSERT INTO audit_emp (emp_id) SELECT e.| FROM employees e");
    assert_eq!(ctx.phase, SqlPhase::SelectList);

    let names = table_names(&ctx);
    assert!(
        names.contains(&"AUDIT_EMP".to_string()),
        "tables: {:?}",
        names
    );
    assert!(
        names.contains(&"EMPLOYEES".to_string()),
        "tables: {:?}",
        names
    );

    let resolved = resolve_qualifier_tables("e", &ctx.tables_in_scope);
    assert_eq!(resolved, vec!["employees"]);
}

#[test]
fn insert_subquery_in_values_increases_query_depth() {
    let ctx = analyze("INSERT INTO employees (id) VALUES ((SELECT | FROM dual))");
    assert_eq!(ctx.depth, 1);
    assert_eq!(ctx.phase, SqlPhase::SelectList);
}

#[test]
fn insert_subquery_depth_returns_to_zero_after_closing_values_subquery() {
    let ctx = analyze("INSERT INTO employees (id) VALUES ((SELECT 1 FROM dual)) RETURNING | INTO :id");
    assert_eq!(ctx.depth, 0);
    assert_eq!(ctx.phase, SqlPhase::ValuesClause);
}

// ─── Complex real-world query tests ─────────────────────────────────────

#[test]
fn complex_cte_with_join_and_subquery() {
    let ctx = analyze(
        "WITH dept_stats AS (\
            SELECT dept_id, COUNT(*) cnt FROM employees GROUP BY dept_id\
         ), \
         salary_stats AS (\
            SELECT dept_id, AVG(salary) avg_sal FROM employees GROUP BY dept_id\
         ) \
         SELECT d.dept_name, ds.cnt, ss.avg_sal \
         FROM departments d \
         JOIN dept_stats ds ON d.id = ds.dept_id \
         JOIN salary_stats ss ON d.id = ss.dept_id \
         WHERE |",
    );
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
    assert_eq!(ctx.depth, 0);
    let names = table_names(&ctx);
    assert!(
        names.contains(&"DEPARTMENTS".to_string()),
        "tables: {:?}",
        names
    );
    assert!(
        names.iter().any(|n| n.eq_ignore_ascii_case("dept_stats")),
        "CTE dept_stats should be in scope: {:?}",
        names
    );
    assert!(
        names.iter().any(|n| n.eq_ignore_ascii_case("salary_stats")),
        "CTE salary_stats should be in scope: {:?}",
        names
    );
}

#[test]
fn oracle_hierarchical_query() {
    let ctx = analyze(
        "SELECT employee_id, manager_id, LEVEL \
         FROM employees \
         START WITH manager_id IS NULL \
         CONNECT BY |",
    );
    assert_eq!(ctx.phase, SqlPhase::ConnectByClause);
    let names = table_names(&ctx);
    assert!(names.contains(&"EMPLOYEES".to_string()));
}

#[test]
fn from_clause_with_function_call_in_select() {
    // Ensure parentheses in function calls don't confuse depth tracking
    let ctx = analyze("SELECT NVL(a, 0), COALESCE(b, c, d) FROM |");
    assert_eq!(ctx.phase, SqlPhase::FromClause);
    assert_eq!(ctx.depth, 0);
}

#[test]
fn select_function_arg_cursor_is_column_context() {
    let ctx = analyze("SELECT MAX(|) FROM HELP");
    assert_eq!(ctx.phase, SqlPhase::SelectList);
    assert!(ctx.phase.is_column_context());
    let names = table_names(&ctx);
    assert!(names.contains(&"HELP".to_string()));
}

#[test]
fn select_function_arg_cursor_with_missing_paren_is_column_context() {
    let ctx = analyze("SELECT MAX(| FROM HELP");
    assert_eq!(ctx.phase, SqlPhase::SelectList);
    assert!(ctx.phase.is_column_context());
    let names = table_names(&ctx);
    assert!(names.contains(&"HELP".to_string()));
}

#[test]
fn case_expression_in_select_list() {
    let ctx = analyze("SELECT CASE WHEN a = 1 THEN 'x' ELSE 'y' END, | FROM t");
    assert_eq!(ctx.phase, SqlPhase::SelectList);
    assert_eq!(ctx.depth, 0);
}

#[test]
fn subquery_in_from_with_join_after() {
    let ctx = analyze(
        "SELECT * FROM (SELECT id FROM t1) sub \
         JOIN t2 ON sub.id = t2.id \
         WHERE |",
    );
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
    let names = table_names(&ctx);
    assert!(
        names.iter().any(|n| n.eq_ignore_ascii_case("sub")),
        "tables: {:?}",
        names
    );
    assert!(names.contains(&"T2".to_string()), "tables: {:?}", names);
}

#[test]
fn multiple_subqueries_in_from() {
    let ctx = analyze(
        "SELECT * FROM \
         (SELECT id FROM t1) a, \
         (SELECT id FROM t2) b \
         WHERE |",
    );
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
    let names = table_names(&ctx);
    assert!(
        names.iter().any(|n| n.eq_ignore_ascii_case("a")),
        "tables: {:?}",
        names
    );
    assert!(
        names.iter().any(|n| n.eq_ignore_ascii_case("b")),
        "tables: {:?}",
        names
    );
}

#[test]
fn cte_used_multiple_times() {
    let ctx = analyze(
        "WITH temp AS (SELECT id FROM users) \
         SELECT * FROM temp t1 JOIN temp t2 ON t1.id = t2.id WHERE |",
    );
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
    let names = table_names(&ctx);
    assert!(names.iter().any(|n| n.eq_ignore_ascii_case("temp")));
}

#[test]
fn exists_subquery() {
    let ctx = analyze(
        "SELECT * FROM employees e WHERE EXISTS (SELECT 1 FROM departments d WHERE d.id = e.|)",
    );
    assert_eq!(ctx.depth, 1);
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
}

#[test]
fn in_subquery_from_clause_tables() {
    let ctx = analyze(
        "SELECT * FROM employees WHERE dept_id IN (SELECT dept_id FROM departments WHERE |)",
    );
    assert_eq!(ctx.depth, 1);
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
    let names = table_names(&ctx);
    // Inside the subquery, departments should be visible
    assert!(
        names.contains(&"DEPARTMENTS".to_string()),
        "tables: {:?}",
        names
    );
    // employees from outer query should also be visible (ancestor scope visibility)
    assert!(
        names.contains(&"EMPLOYEES".to_string()),
        "tables: {:?}",
        names
    );
}

// ─── Edge cases ──────────────────────────────────────────────────────────

#[test]
fn empty_from_clause() {
    let ctx = analyze("SELECT 1 FROM |");
    assert_eq!(ctx.phase, SqlPhase::FromClause);
    assert!(ctx.phase.is_table_context());
}

#[test]
fn cursor_right_after_select() {
    let ctx = analyze("SELECT|");
    // After SELECT keyword, we should be in SelectList
    assert_eq!(ctx.phase, SqlPhase::SelectList);
}

#[test]
fn cursor_in_from_before_any_table() {
    let ctx = analyze("SELECT a FROM |");
    assert_eq!(ctx.phase, SqlPhase::FromClause);
    assert!(ctx.phase.is_table_context());
}

#[test]
fn left_outer_join() {
    let ctx =
        analyze("SELECT | FROM employees e LEFT OUTER JOIN departments d ON e.dept_id = d.id");
    let names = table_names(&ctx);
    assert!(
        names.contains(&"EMPLOYEES".to_string()),
        "tables: {:?}",
        names
    );
    assert!(
        names.contains(&"DEPARTMENTS".to_string()),
        "tables: {:?}",
        names
    );
}

#[test]
fn cross_join() {
    let ctx = analyze("SELECT | FROM t1 CROSS JOIN t2");
    let names = table_names(&ctx);
    assert!(names.contains(&"T1".to_string()), "tables: {:?}", names);
    assert!(names.contains(&"T2".to_string()), "tables: {:?}", names);
}

#[test]
fn natural_join() {
    let ctx = analyze("SELECT | FROM t1 NATURAL JOIN t2");
    let names = table_names(&ctx);
    assert!(names.contains(&"T1".to_string()), "tables: {:?}", names);
    assert!(names.contains(&"T2".to_string()), "tables: {:?}", names);
}

#[test]
fn lateral_subquery_can_see_outer_table_scope() {
    let ctx = analyze("SELECT * FROM t1 a, LATERAL (SELECT a.| FROM t2 b) l");
    let names = table_names(&ctx);
    assert!(
        names.contains(&"T1".to_string()),
        "lateral subquery should inherit outer scope table: {:?}",
        names
    );
    assert!(names.contains(&"T2".to_string()), "tables: {:?}", names);
}

#[test]
fn cross_apply_subquery_can_see_outer_table_scope() {
    let ctx = analyze("SELECT * FROM oqt_t_emp jt CROSS APPLY (SELECT jt.| FROM dual) it");
    let names = table_names(&ctx);
    assert!(
        names
            .iter()
            .any(|name| name.eq_ignore_ascii_case("oqt_t_emp")),
        "cross apply subquery should inherit outer scope table: {:?}",
        names
    );
    assert!(
        names.iter().any(|name| name.eq_ignore_ascii_case("dual")),
        "cross apply subquery should keep local table scope: {:?}",
        names
    );
}

#[test]
fn cross_apply_subquery_exposes_alias_in_outer_scope() {
    let ctx = analyze("SELECT a.| FROM t1 CROSS APPLY (SELECT id FROM t2) a");
    let names = table_names(&ctx);
    assert!(names.contains(&"T1".to_string()), "tables: {:?}", names);
    assert!(names.contains(&"A".to_string()), "tables: {:?}", names);
}

#[test]
fn outer_apply_keeps_from_phase_before_right_relation() {
    let ctx = analyze("SELECT * FROM t1 OUTER APPLY |");
    assert_eq!(ctx.phase, SqlPhase::FromClause);
    assert!(ctx.phase.is_table_context());
}

// ─── CTE inside subquery edge case ──────────────────────────────────────

#[test]
fn cte_with_subquery_alias_in_main_query() {
    let ctx = analyze(
        "WITH base AS (SELECT * FROM employees) \
         SELECT * FROM (SELECT id FROM base) sub WHERE sub.|",
    );
    assert_eq!(ctx.depth, 0);
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
    let names = table_names(&ctx);
    assert!(
        names.iter().any(|n| n.eq_ignore_ascii_case("sub")),
        "tables: {:?}",
        names
    );
    assert!(
        names.iter().any(|n| n.eq_ignore_ascii_case("base")),
        "tables: {:?}",
        names
    );
}

#[test]
fn nested_with_inside_subquery_is_not_collected_as_top_level_cte() {
    let ctx = analyze(
        "SELECT * FROM (WITH inner_cte AS (SELECT 1 AS n FROM dual) SELECT n FROM inner_cte) sub WHERE |",
    );
    let cte_n = cte_names(&ctx);
    assert!(
        !cte_n.contains(&"INNER_CTE".to_string()),
        "top-level CTEs should not include nested WITH definitions: {:?}",
        cte_n
    );
}

#[test]
fn depth_one_in_nested_with_subquery_select_list() {
    let ctx = analyze("SELECT * FROM (WITH inner_cte AS (SELECT 1 AS n FROM dual) SELECT | FROM inner_cte) sub");
    assert_eq!(ctx.depth, 1);
    assert_eq!(ctx.phase, SqlPhase::SelectList);
}

#[test]
fn depth_zero_after_nested_with_subquery_closes() {
    let ctx = analyze(
        "SELECT * FROM (WITH inner_cte AS (SELECT 1 AS n FROM dual) SELECT n FROM inner_cte) sub WHERE |",
    );
    assert_eq!(ctx.depth, 0);
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
}

#[test]
fn nested_with_in_where_subquery_cte_body_depth_counts_parent_query() {
    let ctx = analyze(
        "SELECT * FROM outer_t o WHERE o.id IN (WITH cte AS (SELECT | FROM inner_t) SELECT id FROM cte)",
    );
    assert_eq!(ctx.depth, 2);
    assert_eq!(ctx.phase, SqlPhase::SelectList);
}

#[test]
fn nested_with_in_where_subquery_main_select_depth_is_one() {
    let ctx = analyze(
        "SELECT * FROM outer_t o WHERE o.id IN (WITH cte AS (SELECT 1 AS id FROM inner_t) SELECT | FROM cte)",
    );
    assert_eq!(ctx.depth, 1);
    assert_eq!(ctx.phase, SqlPhase::SelectList);
}

#[test]
fn malformed_with_missing_as_in_query_recovers_depth_and_phase() {
    let ctx = analyze("WITH cte (SELECT 1) SELECT | FROM cte");
    assert_eq!(ctx.depth, 0);
    assert_eq!(ctx.phase, SqlPhase::SelectList);
}

#[test]
fn malformed_with_missing_as_in_query_recovers_from_clause() {
    let ctx = analyze("WITH cte (SELECT 1) SELECT * FROM |");
    assert_eq!(ctx.depth, 0);
    assert_eq!(ctx.phase, SqlPhase::FromClause);
}

#[test]
fn malformed_with_missing_as_in_subquery_keeps_nested_depth() {
    let ctx = analyze("SELECT * FROM (WITH cte (SELECT 1) SELECT | FROM cte) x");
    assert_eq!(ctx.depth, 1);
    assert_eq!(ctx.phase, SqlPhase::SelectList);
}

#[test]
fn sibling_subquery_tables_are_not_visible_inside_current_subquery() {
    let ctx = analyze(
        "SELECT * \
         FROM (SELECT a.id FROM t1 a WHERE a.|) sub1, \
              (SELECT b.id FROM t2 b) sub2",
    );
    let names = table_names(&ctx);
    assert!(names.contains(&"T1".to_string()), "tables: {:?}", names);
    assert!(
        !names.contains(&"T2".to_string()),
        "sibling subquery table should not leak into current scope: {:?}",
        names
    );
    assert!(
        !names.iter().any(|n| n.eq_ignore_ascii_case("sub2")),
        "sibling subquery alias should not leak into current scope: {:?}",
        names
    );
}

// ─── Resolve all scope tables ────────────────────────────────────────────

#[test]
fn resolve_all_deduplicates() {
    let tables = vec![
        ScopedTableRef {
            name: "employees".to_string(),
            alias: Some("e".to_string()),
            depth: 0,
            is_cte: false,
        },
        ScopedTableRef {
            name: "employees".to_string(),
            alias: Some("e2".to_string()),
            depth: 0,
            is_cte: false,
        },
    ];
    let result = resolve_all_scope_tables(&tables);
    assert_eq!(result.len(), 1);
}

// ─── MERGE statement ─────────────────────────────────────────────────────

#[test]
fn merge_target_table() {
    let ctx = analyze("MERGE INTO target_table t USING |");
    let names = table_names(&ctx);
    assert!(
        names.contains(&"TARGET_TABLE".to_string()),
        "tables: {:?}",
        names
    );
}

#[test]
fn merge_using_source_table_is_collected() {
    let ctx = analyze(
        "MERGE INTO target_table t USING source_table s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.val = s.val WHERE |",
    );
    let names = table_names(&ctx);
    assert!(
        names.contains(&"TARGET_TABLE".to_string()),
        "tables: {:?}",
        names
    );
    assert!(
        names.contains(&"SOURCE_TABLE".to_string()),
        "tables: {:?}",
        names
    );
}

#[test]
fn merge_using_phase_is_table_context() {
    let ctx = analyze("MERGE INTO target_table t USING |");
    assert!(ctx.phase.is_table_context());
}

// ─── Analytic function with OVER clause ──────────────────────────────────

#[test]
fn analytic_over_clause_doesnt_confuse_depth() {
    let ctx = analyze(
        "SELECT ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary) AS rn, | FROM employees",
    );
    assert_eq!(ctx.depth, 0);
    assert_eq!(ctx.phase, SqlPhase::SelectList);
}

#[test]
fn plain_expression_parentheses_do_not_increase_query_depth() {
    let ctx = analyze("SELECT (salary + bonus) * | FROM employees");
    assert_eq!(ctx.depth, 0);
    assert_eq!(ctx.phase, SqlPhase::SelectList);
}

#[test]
fn nested_function_parentheses_do_not_increase_query_depth() {
    let ctx = analyze("SELECT COALESCE(ROUND(salary, 2), 0) + | FROM employees");
    assert_eq!(ctx.depth, 0);
    assert_eq!(ctx.phase, SqlPhase::SelectList);
}

#[test]
fn values_subquery_in_from_increases_query_depth() {
    let ctx = analyze("SELECT * FROM (VALUES (|)) v(c)");
    assert_eq!(ctx.depth, 1);
    assert_eq!(ctx.phase, SqlPhase::ValuesClause);
}

#[test]
fn values_subquery_depth_returns_to_zero_after_close() {
    let ctx = analyze("SELECT * FROM (VALUES (1)) v(c) WHERE |");
    assert_eq!(ctx.depth, 0);
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
}

#[test]
fn merge_using_subquery_increases_depth_inside_select_body() {
    let ctx = analyze("MERGE INTO target t USING (SELECT | FROM source) s ON (t.id = s.id)");
    assert_eq!(ctx.depth, 1);
    assert_eq!(ctx.phase, SqlPhase::SelectList);
}

#[test]
fn merge_using_subquery_depth_returns_to_zero_after_close() {
    let ctx = analyze("MERGE INTO target t USING (SELECT id FROM source) s ON (t.id = s.id) WHEN MATCHED THEN UPDATE SET t.val = |");
    assert_eq!(ctx.depth, 0);
    assert_eq!(ctx.phase, SqlPhase::SetClause);
}

#[test]
fn lateral_values_subquery_in_from_increases_depth() {
    let ctx = analyze("SELECT * FROM base b CROSS APPLY (VALUES (|)) v(c)");
    assert_eq!(ctx.depth, 1);
    assert_eq!(ctx.phase, SqlPhase::ValuesClause);
}

#[test]
fn from_subquery_with_update_body_increases_depth() {
    let ctx = analyze("SELECT * FROM (UPDATE employees SET salary = salary + 1 WHERE | RETURNING id) u");
    assert_eq!(ctx.depth, 1);
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
}

#[test]
fn from_subquery_with_delete_body_increases_depth() {
    let ctx = analyze("SELECT * FROM (DELETE FROM employees WHERE | RETURNING id) d");
    assert_eq!(ctx.depth, 1);
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
}

#[test]
fn from_subquery_with_merge_body_increases_depth() {
    let ctx = analyze(
        "SELECT * FROM (MERGE INTO tgt t USING src s ON (t.id = s.id) WHEN MATCHED THEN UPDATE SET t.v = s.v WHERE |) m",
    );
    assert_eq!(ctx.depth, 1);
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
}

// ─── Complex CTE with multiple levels ────────────────────────────────────

#[test]
fn recursive_cte_keyword() {
    let ctx = analyze("WITH RECURSIVE tree AS (SELECT 1 AS id FROM dual) SELECT | FROM tree");
    let cte_n = cte_names(&ctx);
    assert!(cte_n.contains(&"TREE".to_string()), "CTEs: {:?}", cte_n);
}

// ─── Oracle-specific: PIVOT/UNPIVOT ──────────────────────────────────────

#[test]
fn pivot_clause_phase() {
    let ctx = analyze("SELECT * FROM sales PIVOT (SUM(amount) FOR product IN ('A', 'B')) WHERE |");
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
}

#[test]
fn pivot_clause_sum_argument_phase() {
    let ctx = analyze(
        "WITH s AS (SELECT DEPTNO, job, sal FROM oqt_t_emp) \
         SELECT * FROM s PIVOT (SUM(|) AS sum_sal FOR DEPTNO IN (10 AS D10))",
    );
    assert_eq!(ctx.phase, SqlPhase::PivotClause);
}

#[test]
fn pivot_clause_for_expression_phase() {
    let ctx = analyze(
        "WITH s AS (SELECT DEPTNO, job, sal FROM oqt_t_emp) \
         SELECT * FROM s PIVOT (SUM(sal) AS sum_sal FOR | IN (10 AS D10))",
    );
    assert_eq!(ctx.phase, SqlPhase::PivotClause);
}

#[test]
fn model_clause_dimension_by_phase_is_column_context() {
    let ctx = analyze(
        "WITH m AS ( \
           SELECT deptno, SUM(sal) AS sum_sal, COUNT(*) AS cnt \
           FROM oqt_t_emp \
           GROUP BY deptno \
         ) \
         SELECT deptno, sum_sal, cnt \
         FROM m \
         MODEL \
           DIMENSION BY (|) \
           MEASURES (sum_sal, cnt, 0 AS avg_sal_calc, 0 AS sum_plus_100) \
           RULES ( \
             avg_sal_calc[ANY] = ROUND(sum_sal[CV()] / NULLIF(cnt[CV()], 0), 2), \
             sum_plus_100[ANY] = sum_sal[CV()] + 100 \
           )",
    );
    assert_eq!(ctx.phase, SqlPhase::ModelClause);
    assert!(ctx.phase.is_column_context());
}

#[test]
fn model_clause_rules_expression_phase_is_column_context() {
    let ctx = analyze(
        "WITH m AS ( \
           SELECT deptno, SUM(sal) AS sum_sal, COUNT(*) AS cnt \
           FROM oqt_t_emp \
           GROUP BY deptno \
         ) \
         SELECT deptno, sum_sal, cnt \
         FROM m \
         MODEL \
           DIMENSION BY (deptno) \
           MEASURES (sum_sal, cnt, 0 AS avg_sal_calc, 0 AS sum_plus_100) \
           RULES ( \
             avg_sal_calc[ANY] = ROUND(|[CV()] / NULLIF(cnt[CV()], 0), 2), \
             sum_plus_100[ANY] = sum_sal[CV()] + 100 \
           )",
    );
    assert_eq!(ctx.phase, SqlPhase::ModelClause);
    assert!(ctx.phase.is_column_context());
}

#[test]
fn match_recognize_partition_by_phase_is_column_context() {
    let ctx = analyze(
        "SELECT * FROM oqt_t_emp \
         MATCH_RECOGNIZE (PARTITION BY | ORDER BY hiredate PATTERN (a b+) DEFINE b AS b.sal > PREV(b.sal))",
    );
    assert_eq!(ctx.phase, SqlPhase::MatchRecognizeClause);
    assert!(ctx.phase.is_column_context());
}

#[test]
fn match_recognize_pattern_variables_extracted() {
    let tokens = tokenize(
        "SELECT * FROM oqt_t_emp \
         MATCH_RECOGNIZE (PARTITION BY deptno ORDER BY hiredate PATTERN (a b+) DEFINE b AS b.sal > PREV(b.sal))",
    );
    let vars = extract_match_recognize_pattern_variables(&tokens);
    assert_eq!(vars, vec!["a", "b"]);
}

#[test]
fn match_recognize_keyword_is_not_parsed_as_table_alias() {
    let ctx = analyze("SELECT * FROM oqt_t_emp MATCH_RECOGNIZE (PATTERN (a)) WHERE |");
    assert!(
        ctx.tables_in_scope
            .iter()
            .all(|t| t.alias.as_deref() != Some("MATCH_RECOGNIZE")),
        "MATCH_RECOGNIZE should not be parsed as table alias: {:?}",
        ctx.tables_in_scope
            .iter()
            .map(|t| (&t.name, &t.alias))
            .collect::<Vec<_>>()
    );
}

#[test]
fn json_table_arguments_can_resolve_left_relation_alias() {
    let ctx = analyze(
        "SELECT * \
         FROM oqt_t_json j \
         CROSS JOIN JSON_TABLE(j.|, '$' COLUMNS (order_id NUMBER PATH '$.order_id')) jt",
    );
    let resolved = resolve_qualifier_tables("j", &ctx.tables_in_scope);
    assert!(
        resolved
            .iter()
            .any(|name| name.eq_ignore_ascii_case("oqt_t_json")),
        "json_table argument should resolve left table alias: {:?}",
        resolved
    );
}

#[test]
fn extract_table_function_columns_includes_nested_columns_clause() {
    let ctx = analyze(
        "SELECT jt.| \
         FROM oqt_t_json j \
         CROSS JOIN JSON_TABLE( \
           j.payload, \
           '$' \
           COLUMNS ( \
             order_id NUMBER PATH '$.order_id', \
             NESTED PATH '$.items[*]' \
             COLUMNS ( \
               sku VARCHAR2(30) PATH '$.sku', \
               qty NUMBER PATH '$.qty', \
               price NUMBER PATH '$.price' \
             ) \
           ) \
         ) jt",
    );

    let subq = ctx
        .subqueries
        .iter()
        .find(|s| s.alias.eq_ignore_ascii_case("jt"))
        .expect("expected json_table alias jt");
    let cols = extract_table_function_columns(token_range_slice(
        ctx.statement_tokens.as_ref(),
        subq.body_range,
    ));

    for expected in ["order_id", "sku", "qty", "price"] {
        assert!(
            cols.iter().any(|c| c.eq_ignore_ascii_case(expected)),
            "expected nested json_table column `{expected}` in {:?}",
            cols
        );
    }
}

#[test]
fn extract_select_list_leading_qualifiers_reads_incomplete_references() {
    let tokens = tokenize("SELECT jt., jt., jt. FROM dual");
    let qualifiers = extract_select_list_leading_qualifiers(&tokens);
    assert_eq!(qualifiers, vec!["jt"]);
}

#[test]
fn extract_oracle_pivot_projection_columns_from_subquery_star_select() {
    let tokens = tokenize(
        "SELECT * FROM (SELECT DEPTNO, job, SAL FROM oqt_t_emp) \
         PIVOT (SUM(SAL) FOR DEPTNO IN (10 AS D10, 20 AS D20, 30 AS D30))",
    );
    let cols = extract_oracle_pivot_unpivot_projection_columns(&tokens);
    assert_eq!(cols, vec!["job", "D10", "D20", "D30"]);
}

#[test]
fn extract_oracle_unpivot_generated_columns_from_clause() {
    let tokens = tokenize(
        "SELECT * FROM p \
         UNPIVOT (sum_sal FOR dept_tag IN (D10 AS '10', D20 AS '20', D30 AS '30'))",
    );
    let cols = extract_oracle_unpivot_generated_columns(&tokens);
    assert_eq!(cols, vec!["sum_sal", "dept_tag"]);
}

#[test]
fn extract_oracle_model_generated_columns_from_measures_clause() {
    let tokens = tokenize(
        "SELECT deptno, sum_sal \
         FROM m \
         MODEL \
           DIMENSION BY (deptno) \
           MEASURES (sum_sal, cnt, 0 AS avg_sal_calc, 0 AS sum_plus_100) \
           RULES ( \
             avg_sal_calc[ANY] = ROUND(sum_sal[CV()] / NULLIF(cnt[CV()], 0), 2), \
             sum_plus_100[ANY] = sum_sal[CV()] + 100 \
           )",
    );
    let cols = extract_oracle_model_generated_columns(&tokens);
    assert_eq!(cols, vec!["sum_sal", "cnt", "avg_sal_calc", "sum_plus_100"]);
}

#[test]
fn extract_oracle_unpivot_projection_with_nested_pivot_source() {
    let tokens = tokenize(
        "SELECT * FROM ( \
            SELECT * FROM (SELECT DEPTNO, job, SAL FROM oqt_t_emp) \
            PIVOT (SUM(SAL) FOR DEPTNO IN (10 AS D10, 20 AS D20, 30 AS D30)) \
         ) \
         UNPIVOT (sum_sal FOR dept_tag IN (D10 AS '10', D20 AS '20', D30 AS '30'))",
    );
    let cols = extract_oracle_pivot_unpivot_projection_columns(&tokens);
    assert_eq!(cols, vec!["job", "sum_sal", "dept_tag"]);
}

// ─── SELECT list column extraction tests ─────────────────────────────────

#[test]
fn extract_simple_columns() {
    let tokens = tokenize("SELECT id, name, age FROM users");
    let cols = extract_select_list_columns(&tokens);
    assert_eq!(cols, vec!["id", "name", "age"]);
}

#[test]
fn extract_qualified_columns() {
    let tokens = tokenize("SELECT e.empno, e.ename FROM emp e");
    let cols = extract_select_list_columns(&tokens);
    assert_eq!(cols, vec!["empno", "ename"]);
}

#[test]
fn extract_aliased_columns() {
    let tokens = tokenize("SELECT COUNT(*) AS cnt, AVG(sal) AS avg_sal FROM emp");
    let cols = extract_select_list_columns(&tokens);
    assert_eq!(cols, vec!["cnt", "avg_sal"]);
}

#[test]
fn extract_implicit_alias() {
    let tokens = tokenize("SELECT e.deptno, COUNT(*) emp_cnt FROM emp e GROUP BY e.deptno");
    let cols = extract_select_list_columns(&tokens);
    assert_eq!(cols, vec!["deptno", "emp_cnt"]);
}

#[test]
fn extract_star_skipped() {
    let tokens = tokenize("SELECT * FROM emp");
    let cols = extract_select_list_columns(&tokens);
    assert!(cols.is_empty());
}

#[test]
fn extract_qualified_star_skipped() {
    let tokens = tokenize("SELECT e.* FROM emp e");
    let cols = extract_select_list_columns(&tokens);
    assert!(cols.is_empty());
}

#[test]
fn extract_mixed_columns_and_star() {
    let tokens = tokenize("SELECT id, e.*, name FROM emp e");
    let cols = extract_select_list_columns(&tokens);
    assert_eq!(cols, vec!["id", "name"]);
}

#[test]
fn extract_wildcard_tables_unqualified_star() {
    let ctx = analyze("SELECT | FROM help");
    let tokens = tokenize("SELECT * FROM help");
    let tables = extract_select_list_wildcard_tables(&tokens, &ctx.tables_in_scope);
    let upper: Vec<String> = tables.into_iter().map(|t| t.to_uppercase()).collect();
    assert_eq!(upper, vec!["HELP"]);
}

#[test]
fn extract_wildcard_tables_qualified_star() {
    let ctx = analyze("SELECT | FROM help h");
    let tokens = tokenize("SELECT h.* FROM help h");
    let tables = extract_select_list_wildcard_tables(&tokens, &ctx.tables_in_scope);
    let upper: Vec<String> = tables.into_iter().map(|t| t.to_uppercase()).collect();
    assert_eq!(upper, vec!["HELP"]);
}

#[test]
fn extract_wildcard_tables_multiple_sources() {
    let ctx = analyze("SELECT | FROM help h JOIN dept d ON d.id = h.id");
    let tokens = tokenize("SELECT h.*, d.* FROM help h JOIN dept d ON d.id = h.id");
    let tables = extract_select_list_wildcard_tables(&tokens, &ctx.tables_in_scope);
    let upper: Vec<String> = tables.into_iter().map(|t| t.to_uppercase()).collect();
    assert_eq!(upper, vec!["HELP", "DEPT"]);
}

#[test]
fn extract_select_distinct() {
    let tokens = tokenize("SELECT DISTINCT id, name FROM users");
    let cols = extract_select_list_columns(&tokens);
    assert_eq!(cols, vec!["id", "name"]);
}

#[test]
fn extract_nested_function_with_alias() {
    let tokens = tokenize("SELECT NVL(COALESCE(a, b), c) AS result FROM t");
    let cols = extract_select_list_columns(&tokens);
    assert_eq!(cols, vec!["result"]);
}

#[test]
fn extract_scalar_subquery_with_alias() {
    let tokens = tokenize(
        "SELECT \
           oh.order_id, \
           (SELECT SUM(oi.qty*oi.unit_price) \
            FROM oqt_t_order_item oi \
            WHERE oi.order_id = oh.order_id) AS amt \
         FROM oqt_t_order_hdr oh",
    );
    let cols = extract_select_list_columns(&tokens);
    assert_eq!(cols, vec!["order_id", "amt"]);
}

#[test]
fn extract_table_function_columns_from_xmltable_columns_clause() {
    let tokens = tokenize(
        "'/root/dept' PASSING t.payload COLUMNS \
         deptno NUMBER PATH '@deptno', \
         name VARCHAR2(30) PATH 'name/text()', \
         loc VARCHAR2(30) PATH 'loc/text()'",
    );
    let cols = extract_table_function_columns(&tokens);
    assert_eq!(cols, vec!["deptno", "name", "loc"]);
}

#[test]
fn extract_table_function_columns_skips_select_subquery_bodies() {
    let tokens =
        tokenize("SELECT id, XMLTABLE('/x' PASSING t.payload COLUMNS c NUMBER PATH '/x') FROM t");
    let cols = extract_table_function_columns(&tokens);
    assert!(cols.is_empty());
}

// ─── CTE body token capture tests ───────────────────────────────────────

#[test]
fn cte_body_tokens_captured() {
    let ctx = analyze(
        "WITH emp_base AS (SELECT e.empno, e.ename, e.deptno FROM emp e) \
         SELECT eb.| FROM emp_base eb",
    );
    let cte = ctx
        .ctes
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case("emp_base"))
        .unwrap();
    assert!(!cte.body_range.is_empty());
    let cols = extract_select_list_columns(token_range_slice(
        ctx.statement_tokens.as_ref(),
        cte.body_range,
    ));
    assert_eq!(cols, vec!["empno", "ename", "deptno"]);
}

#[test]
fn cte_explicit_columns_present() {
    let ctx = analyze("WITH cte(x, y) AS (SELECT id, name FROM users) SELECT c.| FROM cte c");
    let cte = ctx
        .ctes
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case("cte"))
        .unwrap();
    assert_eq!(cte.explicit_columns.len(), 2);
    assert_eq!(cte.explicit_columns, vec!["x", "y"]);
}

#[test]
fn cte_chain_columns_inferred() {
    let sql = "WITH emp_base AS (\
            SELECT e.empno, e.ename, e.deptno, e.sal, e.hiredate FROM emp e\
        ), \
        dept_agg AS (\
            SELECT eb.deptno, COUNT(*) AS emp_cnt, AVG(eb.sal) AS avg_sal \
            FROM emp_base eb GROUP BY eb.deptno\
        ) \
        SELECT d.deptno, c.| FROM dept d JOIN dept_agg c ON c.deptno = d.deptno";
    let ctx = analyze(sql);
    let dept_agg = ctx
        .ctes
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case("dept_agg"))
        .unwrap();
    let cols = extract_select_list_columns(token_range_slice(
        ctx.statement_tokens.as_ref(),
        dept_agg.body_range,
    ));
    assert_eq!(cols, vec!["deptno", "emp_cnt", "avg_sal"]);
}

#[test]
fn cte_emp_base_columns_inferred() {
    let sql = "WITH emp_base AS (\
            SELECT e.empno, e.ename, e.deptno, e.sal, e.hiredate FROM emp e\
        ), \
        dept_agg AS (\
            SELECT eb.deptno, COUNT(*) AS emp_cnt, AVG(eb.sal) AS avg_sal \
            FROM emp_base eb GROUP BY eb.deptno\
        ) \
        SELECT d.deptno, c.avg_sal FROM dept d JOIN dept_agg c ON c.deptno = d.deptno WHERE |";
    let ctx = analyze(sql);
    let emp_base = ctx
        .ctes
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case("emp_base"))
        .unwrap();
    let cols = extract_select_list_columns(token_range_slice(
        ctx.statement_tokens.as_ref(),
        emp_base.body_range,
    ));
    assert_eq!(cols, vec!["empno", "ename", "deptno", "sal", "hiredate"]);
}

// ─── Subquery alias column extraction tests ─────────────────────────────

#[test]
fn subquery_alias_columns_captured() {
    let ctx = analyze("SELECT sub.| FROM (SELECT id, name, age FROM users) sub");
    assert!(!ctx.subqueries.is_empty());
    let subq = ctx
        .subqueries
        .iter()
        .find(|s| s.alias.eq_ignore_ascii_case("sub"))
        .unwrap();
    let cols = extract_select_list_columns(token_range_slice(
        ctx.statement_tokens.as_ref(),
        subq.body_range,
    ));
    assert_eq!(cols, vec!["id", "name", "age"]);
}

#[test]
fn subquery_without_alias_columns_captured() {
    let ctx = analyze("SELECT | FROM (SELECT id, name FROM users)");
    assert_eq!(ctx.subqueries.len(), 1);
    let subq = &ctx.subqueries[0];
    let cols = extract_select_list_columns(token_range_slice(
        ctx.statement_tokens.as_ref(),
        subq.body_range,
    ));
    assert_eq!(cols, vec!["id", "name"]);
}

#[test]
fn subquery_alias_with_expressions() {
    let ctx = analyze(
        "SELECT v.| FROM (SELECT dept_id, COUNT(*) AS cnt, MAX(sal) max_sal FROM emp GROUP BY dept_id) v",
    );
    let subq = ctx
        .subqueries
        .iter()
        .find(|s| s.alias.eq_ignore_ascii_case("v"))
        .unwrap();
    let cols = extract_select_list_columns(token_range_slice(
        ctx.statement_tokens.as_ref(),
        subq.body_range,
    ));
    assert_eq!(cols, vec!["dept_id", "cnt", "max_sal"]);
}

#[test]
fn malformed_subquery_parentheses_do_not_panic() {
    let ctx = analyze("SELECT * FROM (SELECT * FROM emp)) broken_alias |");
    let names = table_names(&ctx);
    assert!(names.contains(&"BROKEN_ALIAS".to_string()));
}

// ─── EXTRACT / TRIM function-internal FROM ───────────────────────────────

#[test]
fn extract_from_does_not_trigger_from_clause() {
    // EXTRACT(YEAR FROM ...) uses FROM as function syntax, not as a SQL clause.
    // The cursor inside EXTRACT should stay in column context (SelectList).
    let ctx = analyze("SELECT EXTRACT(YEAR FROM |) FROM emp");
    assert_eq!(ctx.phase, SqlPhase::SelectList);
    assert!(ctx.phase.is_column_context());
}

#[test]
fn trim_from_does_not_trigger_from_clause() {
    // TRIM(LEADING '0' FROM col) uses FROM as function syntax.
    let ctx = analyze("SELECT TRIM(LEADING '0' FROM |) FROM emp");
    assert_eq!(ctx.phase, SqlPhase::SelectList);
    assert!(ctx.phase.is_column_context());
}

#[test]
fn substring_from_does_not_trigger_from_clause() {
    // SUBSTRING(col FROM start FOR count) uses FROM as function syntax.
    let ctx = analyze("SELECT SUBSTRING(name FROM | FOR 2) FROM emp");
    assert_eq!(ctx.phase, SqlPhase::SelectList);
    assert!(ctx.phase.is_column_context());
}

#[test]
fn overlay_from_does_not_trigger_from_clause() {
    // OVERLAY(col PLACING txt FROM start FOR count) also consumes FROM internally.
    let ctx = analyze("SELECT OVERLAY(name PLACING 'X' FROM | FOR 1) FROM emp");
    assert_eq!(ctx.phase, SqlPhase::SelectList);
    assert!(ctx.phase.is_column_context());
}

#[test]
fn real_from_after_extract_still_works() {
    // The outer FROM clause should still be detected correctly.
    let ctx = analyze("SELECT EXTRACT(YEAR FROM hire_date) FROM |");
    assert_eq!(ctx.phase, SqlPhase::FromClause);
    assert!(ctx.phase.is_table_context());
}

#[test]
fn malformed_trim_missing_close_paren_recovers_real_from_clause() {
    // Recovery case: if TRIM's closing ')' is missing, the parser should still
    // treat the next FROM as a real SQL clause instead of swallowing it as an
    // endless function-internal FROM.
    let ctx = analyze("SELECT TRIM(LEADING '0' FROM name FROM |");
    assert_eq!(ctx.phase, SqlPhase::FromClause);
    assert!(ctx.phase.is_table_context());
}

#[test]
fn malformed_trim_missing_close_paren_still_collects_from_tables() {
    let ctx = analyze("SELECT TRIM(LEADING '0' FROM name FROM employees WHERE |");
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
    let names = table_names(&ctx);
    assert!(
        names.contains(&"EMPLOYEES".to_string()),
        "tables: {:?}",
        names
    );
}

#[test]
fn subquery_from_inside_parens_still_works() {
    // A subquery inside parentheses should still detect FROM correctly.
    let ctx = analyze("SELECT * FROM (SELECT id FROM |");
    assert_eq!(ctx.phase, SqlPhase::FromClause);
    assert_eq!(ctx.depth, 1);
}

#[test]
fn extract_does_not_confuse_table_collection() {
    // Tables referenced after EXTRACT should still be collected.
    let ctx = analyze("SELECT EXTRACT(YEAR FROM hire_date) FROM employees WHERE |");
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
    let names = table_names(&ctx);
    assert!(
        names.contains(&"EMPLOYEES".to_string()),
        "tables: {:?}",
        names
    );
}

// ─── DELETE without FROM ─────────────────────────────────────────────────

#[test]
fn delete_without_from_collects_target_table() {
    // Oracle allows DELETE table_name WHERE ... (without FROM).
    let ctx = analyze("DELETE employees WHERE |");
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
    let names = table_names(&ctx);
    assert!(
        names.contains(&"EMPLOYEES".to_string()),
        "DELETE without FROM should collect target table: {:?}",
        names
    );
}

#[test]
fn delete_with_from_collects_target_table() {
    // Standard DELETE FROM table_name should also work.
    let ctx = analyze("DELETE FROM employees WHERE |");
    assert_eq!(ctx.phase, SqlPhase::WhereClause);
    let names = table_names(&ctx);
    assert!(
        names.contains(&"EMPLOYEES".to_string()),
        "tables: {:?}",
        names
    );
}
