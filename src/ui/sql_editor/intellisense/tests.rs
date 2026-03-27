    fn lock_or_recover<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
        match mutex.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn runtime_state_for_test(
        completion_range: Option<(usize, usize)>,
        pending: Option<PendingIntellisense>,
        keyup_generation: u64,
        parse_generation: u64,
    ) -> Arc<IntellisenseRuntimeState> {
        let runtime = Arc::new(IntellisenseRuntimeState::new());
        runtime.set_completion_range(
            completion_range.map(|(start, end)| IntellisenseCompletionRange::new(start, end)),
        );
        runtime.set_pending_intellisense(pending);
        runtime.set_keyup_generation_for_test(keyup_generation);
        runtime.set_parse_generation_for_test(parse_generation);
        runtime
    }

    fn load_intellisense_test_file(name: &str) -> String {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("test");
        path.push(name);
        std::fs::read_to_string(path).unwrap_or_default()
    }

    fn analyze_full_script_marker(
        script_with_cursor: &str,
    ) -> (String, usize, intellisense_context::CursorContext) {
        const CURSOR_MARKER: &str = "__CODEX_CURSOR__";

        let cursor = script_with_cursor
            .find(CURSOR_MARKER)
            .expect("cursor marker should exist");
        let sql = script_with_cursor.replacen(CURSOR_MARKER, "", 1);
        let (stmt_start, stmt_end) = SqlEditorWidget::statement_bounds_in_text(&sql, cursor);
        let statement = sql
            .get(stmt_start..stmt_end)
            .unwrap_or("")
            .to_string();
        let cursor_in_statement = cursor.saturating_sub(stmt_start).min(statement.len());
        let (normalized_statement, normalized_cursor) =
            SqlEditorWidget::normalize_intellisense_context_with_cursor(
                &statement,
                cursor_in_statement,
            );
        let deep_ctx =
            SqlEditorWidget::analyze_statement_context(&normalized_statement, normalized_cursor);
        (normalized_statement, normalized_cursor, deep_ctx)
    }

    fn assert_has_case_insensitive(values: &[String], expected: &str) {
        assert!(
            values.iter().any(|value| value.eq_ignore_ascii_case(expected)),
            "expected `{expected}` in values: {:?}",
            values
        );
    }

    fn collect_virtual_columns_from_ctes(
        deep_ctx: &intellisense_context::CursorContext,
        data: &Arc<Mutex<IntellisenseData>>,
        sender: &mpsc::Sender<ColumnLoadUpdate>,
        connection: &SharedConnection,
    ) -> HashMap<String, Vec<String>> {
        let mut virtual_table_columns = HashMap::new();
        for cte in &deep_ctx.ctes {
            let (columns, _) = SqlEditorWidget::collect_cte_virtual_columns_for_completion(
                deep_ctx,
                cte,
                &virtual_table_columns,
                data,
                sender,
                connection,
            );
            if !columns.is_empty() {
                virtual_table_columns.insert(cte.name.clone(), columns);
            }
        }
        virtual_table_columns
    }

    #[test]
    fn test7_set_operator_order_by_keeps_compound_statement_context() {
        let script = load_intellisense_test_file("test7.txt");

        for target in [
            "SELECT empno FROM b\nORDER BY __CODEX_CURSOR__empno;",
            "SELECT empno FROM b\nORDER BY __CODEX_CURSOR__empno;\n\nPROMPT [DONE]",
        ] {
            let marked = script.replacen(
                target.replace("__CODEX_CURSOR__", "").as_str(),
                target,
                1,
            );
            assert_ne!(marked, script, "expected target to exist in test7.txt");
            let (statement, _cursor, deep_ctx) = analyze_full_script_marker(&marked);

            assert!(
                statement.contains("INTERSECT") || statement.contains("MINUS"),
                "compound set-operator statement should be preserved, got:\n{statement}"
            );
            assert!(
                statement.contains("ORDER BY empno"),
                "ORDER BY should remain inside the same statement, got:\n{statement}"
            );
            assert_eq!(
                deep_ctx.phase,
                intellisense_context::SqlPhase::OrderByClause,
                "cursor inside set-operator ORDER BY should stay in OrderByClause"
            );
        }
    }

    #[test]
    fn test7_match_recognize_generated_columns_are_extracted_from_full_script_statement() {
        let script = load_intellisense_test_file("test7.txt");
        let marked = script.replacen(
            "FIRST(ename) AS start_name,",
            "FIRST(ename) AS __CODEX_CURSOR__start_name,",
            1,
        );
        assert_ne!(marked, script, "expected MATCH_RECOGNIZE target in test7.txt");
        let (statement, _cursor, deep_ctx) = analyze_full_script_marker(&marked);

        assert!(
            statement.contains("MATCH_RECOGNIZE"),
            "current statement should contain MATCH_RECOGNIZE, got:\n{statement}"
        );

        let generated = intellisense_context::extract_match_recognize_generated_columns(
            deep_ctx.statement_tokens.as_ref(),
        );
        for expected in ["start_name", "end_name", "run_len"] {
            assert_has_case_insensitive(&generated, expected);
        }
    }

    #[test]
    fn test7_nested_inline_view_wildcard_expands_columns_from_nested_cte() {
        let script = load_intellisense_test_file("test7.txt");
        let marked = script.replacen(
            "ORDER BY v.amt DESC, v.order_dt;",
            "ORDER BY v.__CODEX_CURSOR__amt DESC, v.order_dt;",
            1,
        );
        assert_ne!(marked, script, "expected inline-view target in test7.txt");
        let (_statement, _cursor, deep_ctx) = analyze_full_script_marker(&marked);

        let data = Arc::new(Mutex::new(IntellisenseData::new()));
        let (sender, _receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let connection = create_shared_connection();
        let virtual_table_columns =
            collect_virtual_columns_from_ctes(&deep_ctx, &data, &sender, &connection);

        let v_subquery = deep_ctx
            .subqueries
            .iter()
            .find(|subq| subq.alias.eq_ignore_ascii_case("v"))
            .expect("expected inline view alias v");
        let body_tokens = intellisense_context::token_range_slice(
            deep_ctx.statement_tokens.as_ref(),
            v_subquery.body_range,
        );
        let body_ctx = intellisense_context::analyze_cursor_context(body_tokens, body_tokens.len());
        let mut body_virtual_table_columns = virtual_table_columns.clone();
        for cte in &body_ctx.ctes {
            let (columns, _) = SqlEditorWidget::collect_cte_virtual_columns_for_completion(
                &body_ctx,
                cte,
                &body_virtual_table_columns,
                &data,
                &sender,
                &connection,
            );
            if !columns.is_empty() {
                body_virtual_table_columns.insert(cte.name.clone(), columns);
            }
        }
        let body_tables_in_scope = body_ctx.tables_in_scope.clone();
        let (wildcard_columns, wildcard_tables) = SqlEditorWidget::expand_virtual_table_wildcards(
            body_tokens,
            &body_tables_in_scope,
            &body_virtual_table_columns,
            &data,
            &sender,
            &connection,
        );

        assert_eq!(wildcard_tables, vec!["x".to_string()]);
        for expected in ["order_id", "cust_name", "order_dt", "amt"] {
            assert_has_case_insensitive(&wildcard_columns, expected);
        }
    }

    #[test]
    fn test8_package_body_select_context_stays_inside_open_rc_query() {
        let script = load_intellisense_test_file("test8.txt");
        let marked = script.replacen(
            "                t.grp,",
            "                t.__CODEX_CURSOR__grp,",
            1,
        );
        assert_ne!(marked, script, "expected open_rc SELECT target in test8.txt");
        let (statement, _cursor, deep_ctx) = analyze_full_script_marker(&marked);

        assert!(
            statement.contains("PROCEDURE open_rc"),
            "cursor should stay inside package body statement, got:\n{statement}"
        );
        assert!(
            statement.contains("FROM oqt_t_test t"),
            "open_rc query should remain in scope, got:\n{statement}"
        );
        let column_tables =
            intellisense_context::resolve_qualifier_tables("t", &deep_ctx.tables_in_scope);
        assert_eq!(column_tables, vec!["oqt_t_test".to_string()]);
    }

    #[test]
    fn test8_summary_query_statement_isolated_after_plsql_and_print() {
        let script = load_intellisense_test_file("test8.txt");
        let marked = script.replacen(
            "    COUNT (*) AS cnt,",
            "    COUNT (*) AS __CODEX_CURSOR__cnt,",
            1,
        );
        assert_ne!(marked, script, "expected summary query target in test8.txt");
        let (statement, _cursor, deep_ctx) = analyze_full_script_marker(&marked);

        assert!(
            statement.starts_with("SELECT grp,"),
            "summary query should start at final SELECT, got:\n{statement}"
        );
        assert!(
            statement.contains("FROM oqt_t_test"),
            "summary query should include oqt_t_test, got:\n{statement}"
        );
        assert!(
            !statement.contains("PRINT v_rc"),
            "summary query statement should not include preceding PRINT command:\n{statement}"
        );
        assert_eq!(deep_ctx.phase, intellisense_context::SqlPhase::SelectList);
        let tables: Vec<String> = deep_ctx
            .tables_in_scope
            .iter()
            .map(|table| table.name.to_ascii_uppercase())
            .collect();
        assert!(tables.contains(&"OQT_T_TEST".to_string()));
    }

    #[test]
    fn test8_log_query_order_by_statement_isolated_from_previous_summary_query() {
        let script = load_intellisense_test_file("test8.txt");
        let marked = script.replacen(
            "ORDER BY log_id",
            "ORDER BY __CODEX_CURSOR__log_id",
            1,
        );
        assert_ne!(marked, script, "expected log query ORDER BY target in test8.txt");
        let (statement, _cursor, deep_ctx) = analyze_full_script_marker(&marked);

        assert!(
            statement.contains("FROM oqt_t_log"),
            "log query should include oqt_t_log, got:\n{statement}"
        );
        assert!(
            statement.contains("FETCH FIRST 40 ROWS ONLY"),
            "log query should preserve trailing FETCH clause, got:\n{statement}"
        );
        assert!(
            !statement.contains("FROM oqt_t_test"),
            "log query should not leak previous summary query:\n{statement}"
        );
        assert_eq!(deep_ctx.phase, intellisense_context::SqlPhase::OrderByClause);
    }

    #[test]
    fn test10_with_function_statement_isolated_after_bulk_collect_block() {
        let script = load_intellisense_test_file("test10.txt");
        let marked = script.replacen(
            "    calc_bonus (NVL (e.salary, 0)) AS calc_bonus",
            "    calc_bonus (NVL (e.salary, 0)) AS __CODEX_CURSOR__calc_bonus",
            1,
        );
        assert_ne!(marked, script, "expected WITH FUNCTION target in test10.txt");
        let (statement, _cursor, deep_ctx) = analyze_full_script_marker(&marked);

        assert!(
            statement.contains("WITH FUNCTION calc_bonus"),
            "WITH FUNCTION statement should remain isolated, got:\n{statement}"
        );
        assert!(
            statement.contains("FROM qt_emp e"),
            "WITH FUNCTION query should include qt_emp alias e, got:\n{statement}"
        );
        assert!(
            !statement.contains("FETCH c_emp BULK COLLECT"),
            "WITH FUNCTION statement should not include previous PL/SQL block:\n{statement}"
        );
        let column_tables =
            intellisense_context::resolve_qualifier_tables("e", &deep_ctx.tables_in_scope);
        assert_eq!(column_tables, vec!["qt_emp".to_string()]);
    }

    #[test]
    fn test10_recursive_with_statement_keeps_ctes_and_order_by() {
        let script = load_intellisense_test_file("test10.txt");
        let marked = script.replacen(
            "    r.dept_rank,",
            "    r.__CODEX_CURSOR__dept_rank,",
            1,
        );
        assert_ne!(marked, script, "expected recursive WITH target in test10.txt");
        let (statement, _cursor, deep_ctx) = analyze_full_script_marker(&marked);

        assert!(
            statement.contains("WITH dept_tree"),
            "recursive WITH statement should include dept_tree CTE, got:\n{statement}"
        );
        assert!(
            statement.contains("sales_ranked AS"),
            "recursive WITH statement should include sales_ranked CTE, got:\n{statement}"
        );
        assert!(
            statement.contains("ORDER BY t.path_txt"),
            "recursive WITH statement should preserve final ORDER BY, got:\n{statement}"
        );
        let tables: Vec<String> = deep_ctx
            .tables_in_scope
            .iter()
            .map(|table| table.name.to_ascii_uppercase())
            .collect();
        assert!(tables.contains(&"DEPT_TREE".to_string()));
        assert!(tables.contains(&"SALES_RANKED".to_string()));
    }

    #[test]
    fn test10_cross_apply_alias_columns_resolve_in_full_script() {
        let script = load_intellisense_test_file("test10.txt");
        let marked = script.replacen(
            "    x.max_amt,",
            "    x.__CODEX_CURSOR__max_amt,",
            1,
        );
        assert_ne!(marked, script, "expected CROSS APPLY target in test10.txt");
        let (_statement, _cursor, deep_ctx) = analyze_full_script_marker(&marked);

        let column_tables =
            intellisense_context::resolve_qualifier_tables("x", &deep_ctx.tables_in_scope);
        assert_eq!(column_tables, vec!["x".to_string()]);

        let x_subquery = deep_ctx
            .subqueries
            .iter()
            .find(|subq| subq.alias.eq_ignore_ascii_case("x"))
            .expect("expected CROSS APPLY alias x");
        let body_tokens = intellisense_context::token_range_slice(
            deep_ctx.statement_tokens.as_ref(),
            x_subquery.body_range,
        );
        let columns = intellisense_context::extract_select_list_columns(body_tokens);
        for expected in ["max_amt", "min_amt"] {
            assert_has_case_insensitive(&columns, expected);
        }
    }

    #[test]
    fn test10_pipelined_table_query_isolated_from_adjacent_final_queries() {
        let script = load_intellisense_test_file("test10.txt");
        let marked = script.replacen(
            "FROM TABLE (qt_pipe_emp (NULL))\nORDER BY emp_id;",
            "FROM TABLE (qt_pipe_emp (NULL))\nORDER BY __CODEX_CURSOR__emp_id;",
            1,
        );
        assert_ne!(marked, script, "expected final TABLE(...) query target in test10.txt");
        let (statement, _cursor, deep_ctx) = analyze_full_script_marker(&marked);

        assert!(
            statement.contains("FROM TABLE (qt_pipe_emp (NULL))"),
            "TABLE(...) statement should be isolated, got:\n{statement}"
        );
        assert!(
            !statement.contains("json_like_report"),
            "TABLE(...) statement should not include previous final validation query:\n{statement}"
        );
        assert_eq!(deep_ctx.phase, intellisense_context::SqlPhase::OrderByClause);
    }

    #[test]
    fn test11_with_function_statement_isolated_after_package_execution_block() {
        let script = load_intellisense_test_file("test11.txt");
        let marked = script.replacen(
            "    score_fn (e.salary, e.bonus_pct) AS score",
            "    score_fn (e.salary, e.bonus_pct) AS __CODEX_CURSOR__score",
            1,
        );
        assert_ne!(marked, script, "expected WITH FUNCTION target in test11.txt");
        let (statement, _cursor, deep_ctx) = analyze_full_script_marker(&marked);

        assert!(
            statement.contains("WITH FUNCTION score_fn"),
            "WITH FUNCTION statement should remain isolated, got:\n{statement}"
        );
        assert!(
            statement.contains("FROM qt_employees e"),
            "WITH FUNCTION query should include qt_employees alias e, got:\n{statement}"
        );
        assert!(
            !statement.contains("qt_torture_pkg.complex_block"),
            "WITH FUNCTION statement should not include previous PL/SQL block:\n{statement}"
        );
        let column_tables =
            intellisense_context::resolve_qualifier_tables("e", &deep_ctx.tables_in_scope);
        assert_eq!(column_tables, vec!["qt_employees".to_string()]);
    }

    #[test]
    fn test11_recursive_with_search_cycle_statement_keeps_cte_and_order_by() {
        let script = load_intellisense_test_file("test11.txt");
        let marked = script.replacen(
            "    dfs_ord,",
            "    __CODEX_CURSOR__dfs_ord,",
            1,
        );
        assert_ne!(marked, script, "expected recursive WITH target in test11.txt");
        let (statement, _cursor, deep_ctx) = analyze_full_script_marker(&marked);

        assert!(
            statement.contains("WITH dept_tree"),
            "recursive WITH statement should include dept_tree CTE, got:\n{statement}"
        );
        assert!(
            statement.contains("SEARCH DEPTH FIRST BY dept_id"),
            "recursive WITH statement should preserve SEARCH clause, got:\n{statement}"
        );
        assert!(
            statement.contains("ORDER BY dfs_ord"),
            "recursive WITH statement should preserve final ORDER BY, got:\n{statement}"
        );
        let tables: Vec<String> = deep_ctx
            .tables_in_scope
            .iter()
            .map(|table| table.name.to_ascii_uppercase())
            .collect();
        assert!(tables.contains(&"DEPT_TREE".to_string()));
    }

    #[test]
    fn test11_match_recognize_generated_columns_are_extracted_from_full_script_statement() {
        let script = load_intellisense_test_file("test11.txt");
        let marked = script.replacen(
            "MATCH_NUMBER () AS match_no,",
            "MATCH_NUMBER () AS __CODEX_CURSOR__match_no,",
            1,
        );
        assert_ne!(marked, script, "expected MATCH_RECOGNIZE target in test11.txt");
        let (statement, _cursor, deep_ctx) = analyze_full_script_marker(&marked);

        assert!(
            statement.contains("MATCH_RECOGNIZE"),
            "MATCH_RECOGNIZE statement should remain isolated, got:\n{statement}"
        );
        let generated = intellisense_context::extract_match_recognize_generated_columns(
            deep_ctx.statement_tokens.as_ref(),
        );
        for expected in ["match_no", "cls", "start_dt", "end_dt", "total_amt"] {
            assert_has_case_insensitive(&generated, expected);
        }
    }

    #[test]
    fn test11_json_table_statement_exposes_table_function_columns() {
        let script = load_intellisense_test_file("test11.txt");
        let marked = script.replacen(
            "ORDER BY jt.emp_id,",
            "ORDER BY jt.__CODEX_CURSOR__emp_id,",
            1,
        );
        assert_ne!(marked, script, "expected JSON_TABLE target in test11.txt");
        let (statement, _cursor, deep_ctx) = analyze_full_script_marker(&marked);

        assert!(
            statement.contains("FROM JSON_TABLE"),
            "JSON_TABLE statement should remain isolated, got:\n{statement}"
        );
        let column_tables =
            intellisense_context::resolve_qualifier_tables("jt", &deep_ctx.tables_in_scope);
        assert_eq!(column_tables, vec!["jt".to_string()]);
        let jt_subquery = deep_ctx
            .subqueries
            .iter()
            .find(|subq| subq.alias.eq_ignore_ascii_case("jt"))
            .expect("expected JSON_TABLE alias jt");
        let body_tokens = intellisense_context::token_range_slice(
            deep_ctx.statement_tokens.as_ref(),
            jt_subquery.body_range,
        );
        let mut columns = intellisense_context::extract_select_list_columns(body_tokens);
        if columns.is_empty() {
            columns = intellisense_context::extract_table_function_columns(body_tokens);
        }
        for expected in ["emp_id", "skill"] {
            assert_has_case_insensitive(&columns, expected);
        }
    }

    #[test]
    fn test11_table_function_query_isolated_from_adjacent_queries() {
        let script = load_intellisense_test_file("test11.txt");
        let marked = script.replacen(
            "FROM TABLE (qt_torture_pkg.pipe_sales (NULL))\nORDER BY sale_id;",
            "FROM TABLE (qt_torture_pkg.pipe_sales (NULL))\nORDER BY __CODEX_CURSOR__sale_id;",
            1,
        );
        assert_ne!(marked, script, "expected TABLE(...) target in test11.txt");
        let (statement, _cursor, deep_ctx) = analyze_full_script_marker(&marked);

        assert!(
            statement.contains("FROM TABLE (qt_torture_pkg.pipe_sales (NULL))"),
            "TABLE(...) statement should be isolated, got:\n{statement}"
        );
        assert!(
            !statement.contains("XMLTABLE"),
            "TABLE(...) statement should not include previous XMLTABLE query:\n{statement}"
        );
        assert!(
            !statement.contains("qt_complex_v"),
            "TABLE(...) statement should not include following view query:\n{statement}"
        );
        assert_eq!(deep_ctx.phase, intellisense_context::SqlPhase::OrderByClause);
    }

    #[test]
    fn statement_bounds_ignore_semicolon_in_string_literal() {
        let sql = "SELECT 'a;b' AS txt FROM dual; SELECT 2 FROM dual";
        let cursor = sql.find("FROM dual").unwrap_or(0);
        let (start, end) = SqlEditorWidget::statement_bounds_in_text(sql, cursor);
        assert_eq!(
            sql.get(start..end).unwrap_or(""),
            "SELECT 'a;b' AS txt FROM dual"
        );
    }

    #[test]
    fn raw_cursor_byte_offset_clamps_negative_offsets_to_first_statement() {
        let sql = "SELECT 1 FROM dual;\nSELECT 2 FROM dual;";
        let cursor_byte = SqlEditorWidget::raw_cursor_byte_offset(-12, sql.len() as i32);
        assert_eq!(cursor_byte, 0);
        assert_eq!(
            super::query_text::statement_at_cursor(sql, cursor_byte).as_deref(),
            Some("SELECT 1 FROM dual")
        );
    }

    #[test]
    fn statement_bounds_ignore_inner_plsql_semicolons() {
        let sql = "BEGIN\n  v := 1;\n  v := v + 1;\nEND;\nSELECT * FROM dual;";
        let cursor = sql.find("v + 1").unwrap_or(0);
        let (start, end) = SqlEditorWidget::statement_bounds_in_text(sql, cursor);
        assert_eq!(
            sql.get(start..end).unwrap_or(""),
            "BEGIN\n  v := 1;\n  v := v + 1;\nEND"
        );
    }

    #[test]
    fn statement_bounds_slash_terminates_create_plsql_block() {
        // After 'CREATE FUNCTION ... IS BEGIN ... END;\n/\n', a subsequent
        // SELECT should be recognised as a separate statement.
        let sql = "\
CREATE OR REPLACE FUNCTION oqt_f_add(p_a NUMBER, p_b NUMBER)\nRETURN NUMBER\nIS\nBEGIN\n  RETURN NVL(p_a,0) + NVL(p_b,0);\nEND;\n/\nSELECT empno FROM oqt_emp;";
        let cursor = sql.find("empno FROM").unwrap();
        let (start, end) = SqlEditorWidget::statement_bounds_in_text(sql, cursor);
        let stmt = sql.get(start..end).unwrap_or("");
        assert!(
            stmt.contains("SELECT empno FROM oqt_emp"),
            "expected SELECT statement, got: {:?}",
            stmt
        );
        assert!(
            !stmt.contains("CREATE"),
            "CREATE should not leak into the SELECT statement: {:?}",
            stmt
        );
    }

    #[test]
    fn statement_bounds_multiple_create_blocks_with_slash() {
        // Multiple CREATE blocks terminated by '/' followed by a SELECT
        let sql = "\
CREATE OR REPLACE FUNCTION f1 RETURN NUMBER IS\nBEGIN\n  RETURN 1;\nEND;\n/\n\
CREATE OR REPLACE PROCEDURE p1 IS\nBEGIN\n  NULL;\nEND;\n/\n\
SELECT sa FROM oqt_emp ORDER BY empno;";
        let cursor = sql.find("sa FROM").unwrap();
        let (start, end) = SqlEditorWidget::statement_bounds_in_text(sql, cursor);
        let stmt = sql.get(start..end).unwrap_or("");
        assert!(
            stmt.starts_with("SELECT") || stmt.trim_start().starts_with("SELECT"),
            "expected SELECT statement, got: {:?}",
            stmt
        );
        assert!(
            stmt.contains("oqt_emp"),
            "expected oqt_emp in statement: {:?}",
            stmt
        );
    }

    #[test]
    fn statement_bounds_script_with_plsql_blocks_then_select() {
        // Simulates a realistic script: anonymous PL/SQL blocks, CREATE blocks,
        // followed by a SELECT at the end. The cursor is inside the final SELECT.
        let sql = "\
BEGIN\n  EXECUTE IMMEDIATE 'DROP TABLE oqt_emp PURGE';\nEXCEPTION WHEN OTHERS THEN NULL;\nEND;\n/\n\
CREATE TABLE oqt_emp (\n  empno NUMBER PRIMARY KEY,\n  ename VARCHAR2(50),\n  salary NUMBER\n);\n\
INSERT INTO oqt_emp(empno, ename, salary) VALUES (100, 'ALICE', 3000);\nCOMMIT;\n\
CREATE OR REPLACE FUNCTION oqt_f_add(p_a NUMBER, p_b NUMBER)\nRETURN NUMBER\nIS\nBEGIN\n  RETURN NVL(p_a,0) + NVL(p_b,0);\nEND;\n/\n\
PROMPT === final ===\n\
SELECT empno, ename, sa FROM oqt_emp ORDER BY empno;";

        let cursor = sql.find("sa FROM oqt_emp").unwrap();
        let (stmt_start, stmt_end) = SqlEditorWidget::statement_bounds_in_text(sql, cursor);
        let stmt = sql.get(stmt_start..stmt_end).unwrap_or("");
        assert!(
            stmt.contains("oqt_emp"),
            "statement should contain oqt_emp: {:?}",
            stmt
        );
        assert!(
            stmt.contains("SELECT"),
            "statement should contain SELECT: {:?}",
            stmt
        );

        // Now test context analysis for intellisense
        let context_text = SqlEditorWidget::normalize_intellisense_context_text(
            sql.get(stmt_start..cursor).unwrap_or(""),
        );
        let statement_text = SqlEditorWidget::normalize_intellisense_context_text(
            sql.get(stmt_start..stmt_end).unwrap_or(""),
        );

        let token_spans = super::query_text::tokenize_sql_spanned(&statement_text);
        let split_idx = token_spans.partition_point(|span| span.end <= context_text.len());
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        assert_eq!(
            deep_ctx.phase,
            intellisense_context::SqlPhase::SelectList,
            "cursor should be in SelectList phase"
        );

        let table_names: Vec<String> = deep_ctx
            .tables_in_scope
            .iter()
            .map(|t| t.name.to_uppercase())
            .collect();
        assert!(
            table_names.contains(&"OQT_EMP".to_string()),
            "oqt_emp should be in scope: {:?}",
            table_names
        );
    }

    #[test]
    fn qualifier_before_word_supports_quoted_identifier() {
        let sql_with_cursor = r#"SELECT "e".| FROM "Emp Table" "e""#;
        let cursor = sql_with_cursor.find('|').unwrap_or(0);
        let sql = sql_with_cursor.replace('|', "");
        let qualifier = SqlEditorWidget::qualifier_before_word_in_text(&sql, cursor);
        assert_eq!(qualifier.as_deref(), Some("e"));
    }

    #[test]
    fn qualifier_before_word_rejects_whitespace_between_dot_and_cursor() {
        let sql_with_cursor = "SELECT e.   | FROM emp e";
        let cursor = sql_with_cursor.find('|').unwrap_or(0);
        let sql = sql_with_cursor.replace('|', "");
        let qualifier = SqlEditorWidget::qualifier_before_word_in_text(&sql, cursor);
        assert_eq!(qualifier, None);
    }

    #[test]
    fn qualifier_before_word_rejects_whitespace_before_dot() {
        let sql_with_cursor = "SELECT e   .| FROM emp e";
        let cursor = sql_with_cursor.find('|').unwrap_or(0);
        let sql = sql_with_cursor.replace('|', "");
        let qualifier = SqlEditorWidget::qualifier_before_word_in_text(&sql, cursor);
        assert_eq!(qualifier, None);
    }

    #[test]
    fn qualifier_before_word_rejects_whitespace_before_dot_with_quoted_identifier() {
        let sql_with_cursor = r#"SELECT "e"   .| FROM "Emp Table" "e""#;
        let cursor = sql_with_cursor.find('|').unwrap_or(0);
        let sql = sql_with_cursor.replace('|', "");
        let qualifier = SqlEditorWidget::qualifier_before_word_in_text(&sql, cursor);
        assert_eq!(qualifier, None);
    }

    #[test]
    fn qualifier_before_word_supports_unicode_identifier() {
        let sql_with_cursor = "SELECT 사용자.| FROM emp 사용자";
        let cursor = sql_with_cursor.find('|').unwrap_or(0);
        let sql = sql_with_cursor.replace('|', "");
        let qualifier = SqlEditorWidget::qualifier_before_word_in_text(&sql, cursor);
        assert_eq!(qualifier.as_deref(), Some("사용자"));
    }

    #[test]
    fn qualifier_before_word_supports_multi_part_qualifier_chain() {
        let sql_with_cursor = "SELECT schema_a.emp.| FROM schema_a.emp";
        let cursor = sql_with_cursor.find('|').unwrap_or(0);
        let sql = sql_with_cursor.replace('|', "");
        let qualifier = SqlEditorWidget::qualifier_before_word_in_text(&sql, cursor);
        assert_eq!(qualifier.as_deref(), Some("schema_a.emp"));
    }

    #[test]
    fn qualifier_before_word_supports_multi_part_qualifier_chain_with_quotes() {
        let sql_with_cursor = r#"SELECT "schema A"."Emp Table".| FROM "schema A"."Emp Table""#;
        let cursor = sql_with_cursor.find('|').unwrap_or(0);
        let sql = sql_with_cursor.replace('|', "");
        let qualifier = SqlEditorWidget::qualifier_before_word_in_text(&sql, cursor);
        assert_eq!(qualifier.as_deref(), Some("schema A.Emp Table"));
    }

    #[test]
    fn identifier_at_position_supports_unicode_identifier() {
        let sql = "SELECT 사용자 FROM dual";
        let cursor = sql.find("사용자").unwrap_or(0) + "사용자".len();

        let (word, start, end) = SqlEditorWidget::identifier_at_position_in_text(sql, cursor)
            .expect("unicode identifier should be resolved at cursor");
        assert_eq!(word, "사용자");
        assert_eq!(sql.get(start..end), Some("사용자"));
    }

    #[test]
    fn identifier_at_position_supports_quoted_unicode_identifier() {
        let sql = r#"SELECT "사용자"."이름" FROM dual"#;
        let cursor = sql.find(r#""이름""#).unwrap_or(0) + r#""이름""#.len();

        let (word, start, _end) = SqlEditorWidget::identifier_at_position_in_text(sql, cursor)
            .expect("quoted unicode identifier should be resolved at cursor");
        assert_eq!(word, "이름");
        let qualifier = SqlEditorWidget::qualifier_before_word_in_text(sql, start);
        assert_eq!(qualifier.as_deref(), Some("사용자"));
    }

    #[test]
    fn qualifier_before_word_rejects_numeric_identifier_start() {
        let sql_with_cursor = "SELECT 1emp.| FROM emp";
        let cursor = sql_with_cursor.find('|').unwrap_or(0);
        let sql = sql_with_cursor.replace('|', "");
        let qualifier = SqlEditorWidget::qualifier_before_word_in_text(&sql, cursor);
        assert_eq!(qualifier, None);
    }

    #[test]
    fn qualifier_before_word_allows_special_identifier_start_chars() {
        let sql_with_cursor = "SELECT _emp.| FROM emp _emp";
        let cursor = sql_with_cursor.find('|').unwrap_or(0);
        let sql = sql_with_cursor.replace('|', "");
        let qualifier = SqlEditorWidget::qualifier_before_word_in_text(&sql, cursor);
        assert_eq!(qualifier.as_deref(), Some("_emp"));
    }

    #[test]
    fn normalize_intellisense_context_text_skips_leading_prompt_lines() {
        let input = "PROMPT [3] WITH basic + note\n-- separator\nWITH cte AS (SELECT 1 FROM dual)\nSELECT * FROM cte";
        let normalized = SqlEditorWidget::normalize_intellisense_context_text(input);

        assert!(normalized.starts_with("WITH cte AS"));
        assert!(!normalized.starts_with("PROMPT"));
    }

    #[test]
    fn normalize_intellisense_context_text_strips_sqlplus_line_prefixes() {
        let input = "SQL> WITH cte AS (SELECT 1 FROM dual)
  2  SELECT * FROM cte
";
        let normalized = SqlEditorWidget::normalize_intellisense_context_text(input);

        assert_eq!(
            normalized,
            "WITH cte AS (SELECT 1 FROM dual)
SELECT * FROM cte
"
        );
    }


    #[test]
    fn normalize_intellisense_context_text_strips_unindented_sqlplus_numbered_prefixes() {
        let input = "SQL> SELECT e.
2  FROM emp e
";
        let normalized = SqlEditorWidget::normalize_intellisense_context_text(input);

        assert_eq!(normalized, "SELECT e.
FROM emp e
");
    }

    #[test]
    fn normalize_intellisense_context_with_cursor_maps_unindented_numbered_prefixes() {
        let raw = "SQL> SELECT e.
2  FROM emp e
";
        let raw_cursor = raw.find("e.").unwrap_or(0) + 2;
        let (normalized, normalized_cursor) =
            SqlEditorWidget::normalize_intellisense_context_with_cursor(raw, raw_cursor);

        assert_eq!(normalized, "SELECT e.
FROM emp e
");
        assert_eq!(normalized.get(..normalized_cursor).unwrap_or(""), "SELECT e.");
    }

    #[test]
    fn normalize_intellisense_context_text_strips_unindented_sqlplus_line_prefixes() {
        let input = "SQL> SELECT e.\n2  FROM emp e\n";
        let normalized = SqlEditorWidget::normalize_intellisense_context_text(input);

        assert_eq!(normalized, "SELECT e.\nFROM emp e\n");
    }

    #[test]
    fn normalize_intellisense_context_with_cursor_maps_unindented_sqlplus_line_prefixes() {
        let raw = "SQL> SELECT e.\n2  FROM emp e\n";
        let raw_cursor = raw.find("e.").unwrap_or(0) + 2;
        let (normalized, normalized_cursor) =
            SqlEditorWidget::normalize_intellisense_context_with_cursor(raw, raw_cursor);

        assert_eq!(normalized, "SELECT e.\nFROM emp e\n");
        assert_eq!(normalized.get(..normalized_cursor).unwrap_or(""), "SELECT e.");
    }

    #[test]
    fn normalize_intellisense_context_text_keeps_numeric_literal_line_prefixes() {
        let input = "SELECT\n1 + 2 AS total\nFROM dual";
        let normalized = SqlEditorWidget::normalize_intellisense_context_text(input);

        assert_eq!(normalized, input);
    }

    #[test]
    fn normalize_intellisense_context_text_keeps_unindented_numeric_lines_with_wide_spacing() {
        let input = "SELECT\n1  + 2 AS total\nFROM dual";
        let normalized = SqlEditorWidget::normalize_intellisense_context_text(input);

        assert_eq!(normalized, input);
    }

    #[test]
    fn normalize_intellisense_context_text_keeps_indented_numeric_lines_without_sql_prompt() {
        let input = "SELECT\n  1  + 2 AS total\nFROM dual";
        let normalized = SqlEditorWidget::normalize_intellisense_context_text(input);

        assert_eq!(normalized, input);
    }

    #[test]
    fn normalize_intellisense_context_with_cursor_maps_byte_offset_after_prompt_stripping() {
        let raw = "PROMPT header\nSQL> SELECT e.\n  2  FROM emp e\n";
        let raw_cursor = raw.find("e.").expect("cursor anchor should exist") + 2;
        let (normalized, normalized_cursor) =
            SqlEditorWidget::normalize_intellisense_context_with_cursor(raw, raw_cursor);

        assert_eq!(normalized, "SELECT e.\nFROM emp e\n");
        assert_eq!(&normalized[..normalized_cursor], "SELECT e.");

        let full_token_spans = super::query_text::tokenize_sql_spanned(&normalized);
        let split_idx = full_token_spans.partition_point(|span| span.end <= normalized_cursor);
        let full_tokens: Vec<SqlToken> = full_token_spans
            .into_iter()
            .map(|span| span.token)
            .collect();
        let ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);
        assert_eq!(ctx.phase, intellisense_context::SqlPhase::SelectList);
        assert!(
            ctx.tables_in_scope
                .iter()
                .any(|t| t.name.eq_ignore_ascii_case("emp")),
            "emp should remain visible after byte-offset remapping"
        );
    }

    #[test]
    fn normalize_intellisense_context_with_cursor_maps_offset_for_unindented_numbered_lines() {
        let raw = "SQL> SELECT e.\n2  FROM emp e\n";
        let raw_cursor = raw.find("e.").expect("cursor anchor should exist") + 2;
        let (normalized, normalized_cursor) =
            SqlEditorWidget::normalize_intellisense_context_with_cursor(raw, raw_cursor);

        assert_eq!(normalized, "SELECT e.\nFROM emp e\n");
        assert_eq!(&normalized[..normalized_cursor], "SELECT e.");
    }

    #[test]
    fn normalize_intellisense_context_text_matches_cursor_variant_at_end() {
        let raw = "PROMPT header\nSQL> -- skip me\nSQL> SELECT 한글.\n  2  FROM emp 한글\n";
        let normalized_text = SqlEditorWidget::normalize_intellisense_context_text(raw);
        let (normalized_with_cursor, normalized_cursor) =
            SqlEditorWidget::normalize_intellisense_context_with_cursor(raw, raw.len());

        assert_eq!(normalized_with_cursor, normalized_text);
        assert_eq!(
            normalized_with_cursor.get(..normalized_cursor).unwrap_or(""),
            "SELECT 한글.\nFROM emp 한글"
        );
    }

    #[test]
    fn prompt_line_before_with_does_not_break_cte_qualified_column_resolution() {
        let sql_with_cursor = r#"
PROMPT [3] WITH basic + multiple CTE + join + scalar subquery + nested expressions
WITH
  d AS (
    SELECT deptno, dname, loc
    FROM oqt_t_dept
  )
SELECT d.|, d.loc
FROM d
"#;

        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let context_text =
            SqlEditorWidget::normalize_intellisense_context_text(sql.get(..cursor).unwrap_or(""));
        let (stmt_start, stmt_end) = SqlEditorWidget::statement_bounds_in_text(&sql, cursor);
        let statement_text = SqlEditorWidget::normalize_intellisense_context_text(
            sql.get(stmt_start..stmt_end).unwrap_or(""),
        );

        let token_spans = super::query_text::tokenize_sql_spanned(&statement_text);
        let split_idx = token_spans.partition_point(|span| span.end <= context_text.len());
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        assert!(
            deep_ctx
                .ctes
                .iter()
                .any(|cte| cte.name.eq_ignore_ascii_case("d")),
            "expected CTE d in parsed context: {:?}",
            deep_ctx
                .ctes
                .iter()
                .map(|cte| cte.name.clone())
                .collect::<Vec<_>>()
        );

        let column_tables =
            intellisense_context::resolve_qualifier_tables("d", &deep_ctx.tables_in_scope);
        assert_eq!(column_tables, vec!["d".to_string()]);

        let mut data = IntellisenseData::new();
        for cte in &deep_ctx.ctes {
            let body_tokens = intellisense_context::token_range_slice(
                deep_ctx.statement_tokens.as_ref(),
                cte.body_range,
            );
            let mut columns = if !cte.explicit_columns.is_empty() {
                cte.explicit_columns.clone()
            } else if !cte.body_range.is_empty() {
                intellisense_context::extract_select_list_columns(body_tokens)
            } else {
                Vec::new()
            };
            SqlEditorWidget::dedup_column_names_case_insensitive(&mut columns);
            if !columns.is_empty() {
                data.set_virtual_table_columns(&cte.name, columns);
            }
        }

        let suggestions = data.get_column_suggestions("", Some(&column_tables));
        assert!(
            suggestions
                .iter()
                .any(|col| col.eq_ignore_ascii_case("DNAME")),
            "expected DNAME suggestion for d.* scope, got: {:?}",
            suggestions
        );
    }

    #[test]
    fn statement_context_uses_window_slice_for_large_multiline_statement() {
        let mut sql = String::from("SELECT\n");
        for _ in 0..3_000 {
            sql.push_str("col_a, col_b, col_c, col_d, col_e, col_f, col_g,\n");
        }
        sql.push_str("dummy_table.col_h,\n");
        sql.push_str("dummy_table.col_i\n");
        sql.push_str("FROM dummy_schema.dummy_table\n");

        let cursor = sql.len();
        let context = SqlEditorWidget::statement_context_in_text(&sql, cursor);
        assert!(
            context.contains("dummy_table.col_h"),
            "statement_context should include the latest select list columns, got {:?}",
            context.get(0..120).unwrap_or("")
        );
    }

    #[test]
    fn context_before_cursor_uses_window_slice_for_large_multiline_statement() {
        let mut sql = String::from("SELECT\n");
        for _ in 0..3_000 {
            sql.push_str("col_a, col_b, col_c, col_d, col_e, col_f, col_g,\n");
        }
        sql.push_str("dummy_table.col_h,\n");
        sql.push_str("dummy_table.col_i\n");
        sql.push_str("FROM dummy_schema.dummy_table\n");

        let cursor = sql.len();
        let context = SqlEditorWidget::context_before_cursor_in_text(&sql, cursor);
        assert!(
            context.contains("dummy_table.col_i"),
            "context_before_cursor should include the latest select list columns, got {:?}",
            context.get(0..120).unwrap_or("")
        );
    }

    #[test]
    fn statement_context_window_clamps_utf8_start_boundary() {
        let mut sql = String::from("가");
        sql.push_str(&"a".repeat(INTELLISENSE_STATEMENT_WINDOW as usize - 1));
        let cursor = sql.len();

        let context = SqlEditorWidget::statement_context_in_text(&sql, cursor);
        assert!(
            !context.is_empty(),
            "statement_context should not become empty when window starts in UTF-8 middle byte"
        );
        assert!(context.contains('가'));
    }

    #[test]
    fn context_before_cursor_window_clamps_utf8_start_boundary() {
        let mut sql = String::from("가");
        sql.push_str(&"a".repeat(INTELLISENSE_CONTEXT_WINDOW as usize - 1));
        let cursor = sql.len();

        let context = SqlEditorWidget::context_before_cursor_in_text(&sql, cursor);
        assert!(
            !context.is_empty(),
            "context_before_cursor should not become empty when window starts in UTF-8 middle byte"
        );
        assert!(context.contains('가'));
    }

    #[test]
    fn qualifier_before_word_in_text_supports_quoted_identifier_at_text_start() {
        let sql_with_cursor = r#""e".| FROM "Employees" e"#;
        let cursor = sql_with_cursor.find('|').unwrap_or(0);
        let sql = sql_with_cursor.replace('|', "");
        let qualifier = SqlEditorWidget::qualifier_before_word_in_text(&sql, cursor);
        assert_eq!(qualifier.as_deref(), Some("e"));
    }

    #[test]
    fn qualifier_before_word_rejects_unbalanced_quoted_identifier() {
        let sql_with_cursor = r#"SELECT "e.| FROM emp e"#;
        let cursor = sql_with_cursor.find('|').unwrap_or(0);
        let sql = sql_with_cursor.replace('|', "");
        let qualifier = SqlEditorWidget::qualifier_before_word_in_text(&sql, cursor);
        assert_eq!(qualifier, None);
    }

    #[test]
    fn identifier_at_position_rejects_unbalanced_quoted_identifier() {
        let sql = r#"SELECT "사용자 FROM dual"#;
        let cursor = sql.find("사용자").unwrap_or(0) + "사용자".len();

        let resolved = SqlEditorWidget::identifier_at_position_in_text(sql, cursor);
        assert!(
            resolved.is_none(),
            "unbalanced quoted identifier should not be resolved"
        );
    }

    #[test]
    fn parse_dropped_file_token_decodes_utf8_percent_sequences() {
        let token = "file:///tmp/%ED%95%9C%EA%B8%80.sql";
        let parsed = SqlEditorWidget::parse_dropped_file_token(token);
        assert_eq!(parsed, Some(PathBuf::from("/tmp/한글.sql")));
    }

    #[test]
    fn parse_dropped_file_token_handles_case_insensitive_prefixes() {
        let token = "FiLe://LOCALHOST/tmp/My%20File.sql";
        let parsed = SqlEditorWidget::parse_dropped_file_token(token);
        assert_eq!(parsed, Some(PathBuf::from("/tmp/My File.sql")));
    }

    #[test]
    fn parse_dropped_file_token_strips_wrapping_quotes() {
        let token = "\"file:///tmp/Quoted%20Name.sql\"";
        let parsed = SqlEditorWidget::parse_dropped_file_token(token);
        assert_eq!(parsed, Some(PathBuf::from("/tmp/Quoted Name.sql")));

        let single_quoted = "'file:///tmp/Single%20Quoted.sql'";
        let parsed = SqlEditorWidget::parse_dropped_file_token(single_quoted);
        assert_eq!(parsed, Some(PathBuf::from("/tmp/Single Quoted.sql")));
    }

    #[test]
    fn typed_char_from_key_event_falls_back_for_shifted_underscore() {
        let ch = SqlEditorWidget::typed_char_from_key_event("", Key::from_char('-'), true, None);
        assert_eq!(ch, Some('_'));
    }

    #[test]
    fn typed_char_from_key_event_infers_underscore_from_buffer_even_without_shift_state() {
        let ch =
            SqlEditorWidget::typed_char_from_key_event("", Key::from_char('-'), false, Some('_'));
        assert_eq!(ch, Some('_'));
    }

    #[test]
    fn typed_char_from_key_event_keeps_minus_when_minus_was_inserted() {
        let ch =
            SqlEditorWidget::typed_char_from_key_event("", Key::from_char('-'), false, Some('-'));
        assert_eq!(ch, Some('-'));
    }

    #[test]
    fn debounce_cursor_comparison_uses_raw_offsets() {
        assert!(SqlEditorWidget::is_same_raw_cursor_offset(10, 10));
        assert!(!SqlEditorWidget::is_same_raw_cursor_offset(10, 11));
    }

    #[test]
    fn manual_trigger_invalidates_debounce_and_parse_generation() {
        let runtime = runtime_state_for_test(None, None, 17, 23);

        SqlEditorWidget::invalidate_manual_trigger_debounce_state(&runtime);

        assert_eq!(runtime.current_keyup_generation(), 18);
        assert_eq!(runtime.current_parse_generation(), 24);
    }

    #[test]
    fn external_hide_clears_state_and_invalidates_generations() {
        let runtime = runtime_state_for_test(
            Some((3, 5)),
            Some(PendingIntellisense { cursor_pos: 7 }),
            41,
            9,
        );

        SqlEditorWidget::clear_intellisense_state_for_external_hide(&runtime);

        assert_eq!(runtime.current_keyup_generation(), 42);
        assert_eq!(runtime.current_parse_generation(), 10);
        assert!(runtime.completion_range().is_none());
        assert!(runtime.pending_intellisense().is_none());
    }

    #[test]
    fn external_hide_ignores_only_inside_click_when_popup_visible() {
        assert!(SqlEditorWidget::should_ignore_external_hide_click(
            true, true
        ));
        assert!(!SqlEditorWidget::should_ignore_external_hide_click(
            true, false
        ));
        assert!(!SqlEditorWidget::should_ignore_external_hide_click(
            false, true
        ));
        assert!(!SqlEditorWidget::should_ignore_external_hide_click(
            false, false
        ));
    }

    #[test]
    fn unfocus_hide_rule_hides_only_when_pointer_is_outside_visible_popup() {
        assert!(SqlEditorWidget::should_hide_popup_on_unfocus(true, false));
        assert!(!SqlEditorWidget::should_hide_popup_on_unfocus(true, true));
        assert!(!SqlEditorWidget::should_hide_popup_on_unfocus(false, false));
        assert!(!SqlEditorWidget::should_hide_popup_on_unfocus(false, true));
    }

    #[test]
    fn escape_keydown_cancels_pending_even_when_popup_not_visible() {
        let runtime = runtime_state_for_test(
            Some((10, 12)),
            Some(PendingIntellisense { cursor_pos: 14 }),
            5,
            20,
        );

        let consumed = SqlEditorWidget::cancel_intellisense_on_escape_keydown(false, &runtime);

        assert!(!consumed);
        assert!(runtime.completion_range().is_none());
        assert!(runtime.pending_intellisense().is_none());
        assert_eq!(runtime.current_keyup_generation(), 6);
        assert_eq!(runtime.current_parse_generation(), 21);
    }

    #[test]
    fn navigation_shortcut_clears_pending_even_when_popup_not_visible() {
        let runtime = runtime_state_for_test(
            Some((4, 8)),
            Some(PendingIntellisense { cursor_pos: 11 }),
            12,
            33,
        );

        SqlEditorWidget::invalidate_and_clear_pending_intellisense_state(&runtime);

        assert!(runtime.completion_range().is_none());
        assert!(runtime.pending_intellisense().is_none());
        assert_eq!(runtime.current_keyup_generation(), 13);
        assert_eq!(runtime.current_parse_generation(), 34);
    }

    #[test]
    fn escape_keydown_consumes_when_popup_is_visible() {
        let runtime = runtime_state_for_test(
            Some((1, 3)),
            Some(PendingIntellisense { cursor_pos: 3 }),
            0,
            0,
        );

        let consumed = SqlEditorWidget::cancel_intellisense_on_escape_keydown(true, &runtime);

        assert!(consumed);
        assert!(runtime.completion_range().is_none());
        assert!(runtime.pending_intellisense().is_none());
        assert_eq!(runtime.current_keyup_generation(), 1);
        assert_eq!(runtime.current_parse_generation(), 1);
    }

    #[test]
    fn min_intellisense_prefix_uses_character_count() {
        assert!(!SqlEditorWidget::has_min_intellisense_prefix(""));
        assert!(!SqlEditorWidget::has_min_intellisense_prefix("a"));
        assert!(SqlEditorWidget::has_min_intellisense_prefix("ab"));
        assert!(!SqlEditorWidget::has_min_intellisense_prefix("한"));
        assert!(SqlEditorWidget::has_min_intellisense_prefix("한글"));
    }

    #[test]
    fn fast_path_delete_hides_popup_when_prefix_too_short_without_qualifier() {
        assert!(SqlEditorWidget::should_hide_fast_path_after_delete(
            "",
            None,
            Key::BackSpace
        ));
        assert!(SqlEditorWidget::should_hide_fast_path_after_delete(
            "a",
            None,
            Key::Delete
        ));
        assert!(!SqlEditorWidget::should_hide_fast_path_after_delete(
            "ab",
            None,
            Key::BackSpace
        ));
        assert!(!SqlEditorWidget::should_hide_fast_path_after_delete(
            "a",
            Some("t"),
            Key::BackSpace
        ));
        assert!(!SqlEditorWidget::should_hide_fast_path_after_delete(
            "a",
            None,
            Key::from_char('a')
        ));
    }

    #[test]
    fn auto_trigger_forced_char_requires_qualifier_or_two_chars() {
        assert!(!SqlEditorWidget::should_auto_trigger_intellisense_for_forced_char("", None));
        assert!(!SqlEditorWidget::should_auto_trigger_intellisense_for_forced_char("a", None));
        assert!(!SqlEditorWidget::should_auto_trigger_intellisense_for_forced_char("한", None));
        assert!(SqlEditorWidget::should_auto_trigger_intellisense_for_forced_char("ab", None));
        assert!(SqlEditorWidget::should_auto_trigger_intellisense_for_forced_char("한글", None));
        assert!(SqlEditorWidget::should_auto_trigger_intellisense_for_forced_char("", Some("t")));
    }

    #[test]
    fn keyup_after_manual_ctrl_space_trigger_is_ignored() {
        assert!(SqlEditorWidget::should_ignore_keyup_after_manual_trigger(
            Key::from_char(' '),
            Key::from_char(' '),
            true,
        ));
        assert!(!SqlEditorWidget::should_ignore_keyup_after_manual_trigger(
            Key::from_char(' '),
            Key::from_char(' '),
            false,
        ));
        assert!(!SqlEditorWidget::should_ignore_keyup_after_manual_trigger(
            Key::from_char('a'),
            Key::from_char('a'),
            true,
        ));
    }

    #[test]
    fn shortcut_key_for_layout_falls_back_to_original_for_non_ascii_key() {
        assert_eq!(
            SqlEditorWidget::shortcut_key_for_layout(Key::from_char('ㄹ'), Key::from_char('f')),
            Key::from_char('f')
        );
    }

    #[test]
    fn resolved_shortcut_key_matches_all_editor_ctrl_alpha_shortcuts() {
        for ascii in ['f', 'u', 'l', 'h', 'z', 'y'] {
            let resolved = SqlEditorWidget::shortcut_key_for_layout(
                Key::from_char('한'),
                Key::from_char(ascii),
            );
            assert!(SqlEditorWidget::matches_alpha_shortcut(resolved, ascii));
        }
    }

    #[test]
    fn resolved_shortcut_key_preserves_ctrl_space_and_ctrl_slash() {
        let space =
            SqlEditorWidget::shortcut_key_for_layout(Key::from_char('한'), Key::from_char(' '));
        assert_eq!(space, Key::from_char(' '));

        let slash =
            SqlEditorWidget::shortcut_key_for_layout(Key::from_char('한'), Key::from_char('/'));
        assert_eq!(slash, Key::from_char('/'));
    }

    #[test]
    fn matches_alpha_shortcut_accepts_upper_and_lower_case() {
        assert!(SqlEditorWidget::matches_alpha_shortcut(
            Key::from_char('f'),
            'f'
        ));
        assert!(SqlEditorWidget::matches_alpha_shortcut(
            Key::from_char('F'),
            'f'
        ));
        assert!(!SqlEditorWidget::matches_alpha_shortcut(
            Key::from_char('g'),
            'f'
        ));
    }

    #[test]
    fn token_spans_partition_handles_utf8_boundaries() {
        let sql = "SELECT 한글 FROM dual";
        let cursor = "SELECT 한".len();
        let spans = super::query_text::tokenize_sql_spanned(sql);
        let split_idx = spans.partition_point(|span| span.end <= cursor);
        let tokens: Vec<SqlToken> = spans[..split_idx]
            .iter()
            .map(|span| span.token.clone())
            .collect();
        assert_eq!(tokens.len(), 1);
        assert!(matches!(tokens.first(), Some(SqlToken::Word(word)) if word == "SELECT"));
    }

    #[test]
    fn modifier_key_is_detected_for_shift_release() {
        assert!(SqlEditorWidget::is_modifier_key(Key::ShiftL));
        assert!(SqlEditorWidget::is_modifier_key(Key::ShiftR));
        assert!(!SqlEditorWidget::is_modifier_key(Key::from_char('a')));
    }

    #[test]
    fn request_table_columns_releases_loading_when_connection_busy() {
        let data = Arc::new(Mutex::new(IntellisenseData::new()));
        {
            let mut guard = lock_or_recover(&data);
            guard.tables = vec!["EMP".to_string()];
            guard.rebuild_indices();
        }

        let (sender, receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let connection = create_shared_connection();
        let _conn_guard = connection.lock().ok();

        SqlEditorWidget::request_table_columns("EMP", &data, &sender, &connection);

        let update = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("column loader should emit a completion update even when lock is busy");
        assert_eq!(update.table, "EMP");
        assert!(update.columns.is_empty());
        assert!(!update.cache_columns);
    }

    #[test]
    fn request_table_columns_handles_quoted_schema_and_table_names() {
        let data = Arc::new(Mutex::new(IntellisenseData::new()));
        {
            let mut guard = lock_or_recover(&data);
            guard.tables = vec!["SCHEMA.TABLE.NAME".to_string()];
            guard.rebuild_indices();
        }

        let (sender, receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let connection = create_shared_connection();
        let _conn_guard = connection.lock().ok();

        SqlEditorWidget::request_table_columns(
            "\"SCHEMA\".\"TABLE.NAME\"",
            &data,
            &sender,
            &connection,
        );

        let update = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("quoted schema/table names should normalize before relation lookup");
        assert_eq!(update.table, "SCHEMA.TABLE.NAME");
        assert!(!update.cache_columns);
    }
    #[test]
    fn request_table_columns_keeps_exact_dotted_relation_name() {
        let data = Arc::new(Mutex::new(IntellisenseData::new()));
        {
            let mut guard = lock_or_recover(&data);
            guard.tables = vec!["A.B".to_string()];
            guard.rebuild_indices();
        }

        let (sender, receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let connection = create_shared_connection();
        let _conn_guard = connection.lock().ok();

        SqlEditorWidget::request_table_columns("A.B", &data, &sender, &connection);

        let update = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("known dotted relation name should still be used for column loading");
        assert_eq!(update.table, "A.B");
        assert!(!update.cache_columns);
    }

    #[test]
    fn request_table_columns_falls_back_to_unqualified_name() {
        let data = Arc::new(Mutex::new(IntellisenseData::new()));
        {
            let mut guard = lock_or_recover(&data);
            guard.tables = vec!["EMP".to_string()];
            guard.rebuild_indices();
        }

        let (sender, receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let connection = create_shared_connection();
        let _conn_guard = connection.lock().ok();

        SqlEditorWidget::request_table_columns("HR.EMP", &data, &sender, &connection);

        let update = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("schema-qualified names should fall back to relation key when needed");
        assert_eq!(update.table, "EMP");
        assert!(!update.cache_columns);
    }

    #[test]
    fn column_loading_scope_detects_unqualified_pending_refresh() {
        let mut data = IntellisenseData::new();
        data.columns_loading.insert("EMP".to_string());
        let column_tables = vec!["emp".to_string()];
        let deps = HashMap::new();
        assert!(SqlEditorWidget::has_column_loading_for_scope(
            true,
            &column_tables,
            &deps,
            &data
        ));
    }

    #[test]
    fn column_loading_scope_detects_schema_qualified_pending_refresh() {
        let mut data = IntellisenseData::new();
        data.columns_loading.insert("EMP".to_string());
        let column_tables = vec!["hr.emp".to_string()];
        let deps = HashMap::new();
        assert!(SqlEditorWidget::has_column_loading_for_scope(
            true,
            &column_tables,
            &deps,
            &data
        ));
    }

    #[test]
    fn request_table_columns_does_not_fallback_when_dot_is_inside_quoted_identifier() {
        let data = Arc::new(Mutex::new(IntellisenseData::new()));
        {
            let mut guard = lock_or_recover(&data);
            guard.tables = vec!["B".to_string()];
            guard.rebuild_indices();
        }

        let (sender, receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let connection = create_shared_connection();
        let _conn_guard = connection.lock().ok();

        SqlEditorWidget::request_table_columns("\"A.B\"", &data, &sender, &connection);

        let update = receiver.recv_timeout(Duration::from_millis(200));
        assert!(
            update.is_err(),
            "quoted identifier with embedded dot should not fall back to unqualified key"
        );
    }

    #[test]
    fn request_table_columns_does_not_fallback_for_invalid_qualified_identifier() {
        let data = Arc::new(Mutex::new(IntellisenseData::new()));
        {
            let mut guard = lock_or_recover(&data);
            guard.tables = vec!["EMP".to_string()];
            guard.rebuild_indices();
        }

        let (sender, receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let connection = create_shared_connection();
        let _conn_guard = connection.lock().ok();

        SqlEditorWidget::request_table_columns("HR.", &data, &sender, &connection);

        let update = receiver.recv_timeout(Duration::from_millis(200));
        assert!(
            update.is_err(),
            "invalid qualified identifier should not fall back to unrelated relation key"
        );
    }

    #[test]
    fn request_table_columns_ignores_unbalanced_quoted_identifier() {
        let data = Arc::new(Mutex::new(IntellisenseData::new()));
        {
            let mut guard = lock_or_recover(&data);
            guard.tables = vec!["EMP".to_string()];
            guard.rebuild_indices();
        }

        let (sender, receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let connection = create_shared_connection();
        let _conn_guard = connection.lock().ok();

        SqlEditorWidget::request_table_columns("\"HR\".\"EMP", &data, &sender, &connection);

        let update = receiver.recv_timeout(Duration::from_millis(200));
        assert!(
            update.is_err(),
            "unbalanced quoted identifier should not trigger fallback column loading"
        );
    }

    #[test]
    fn intellisense_data_clears_stale_column_loading_entries() {
        let mut data = IntellisenseData::new();
        assert!(data.mark_columns_loading("EMP"));
        std::thread::sleep(Duration::from_millis(20));

        let cleared = data.clear_stale_columns_loading(Duration::from_millis(1));
        assert_eq!(cleared, 1);
        assert!(!data.columns_loading.contains("EMP"));
    }

    #[test]
    fn expand_virtual_table_wildcards_uses_loaded_base_table_columns() {
        let data = Arc::new(Mutex::new(IntellisenseData::new()));
        {
            let mut guard = lock_or_recover(&data);
            guard.tables = vec!["HELP".to_string()];
            guard.rebuild_indices();
            guard.set_columns_for_table("HELP", vec!["TOPIC".to_string(), "TEXT".to_string()]);
        }

        let (sender, _receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let connection = create_shared_connection();
        let tokens = SqlEditorWidget::tokenize_sql("SELECT * FROM help");
        let tables_in_scope = intellisense_context::collect_tables_in_statement(&tokens);

        let (columns, tables) = SqlEditorWidget::expand_virtual_table_wildcards(
            &tokens,
            &tables_in_scope,
            &HashMap::new(),
            &data,
            &sender,
            &connection,
        );

        let upper_tables: Vec<String> = tables.into_iter().map(|t| t.to_uppercase()).collect();
        assert_eq!(upper_tables, vec!["HELP"]);
        assert_eq!(columns, vec!["TOPIC", "TEXT"]);
    }

    #[test]
    fn collect_context_alias_suggestions_includes_table_aliases_and_ctes() {
        let full = SqlEditorWidget::tokenize_sql(
            "WITH recent_emp AS (SELECT empno FROM emp) SELECT  FROM emp e",
        );
        let ctx = intellisense_context::analyze_cursor_context(&full, full.len());

        let suggestions = SqlEditorWidget::collect_context_alias_suggestions("", &ctx);
        let upper: Vec<String> = suggestions.into_iter().map(|s| s.to_uppercase()).collect();

        assert!(upper.contains(&"E".to_string()));
        assert!(upper.contains(&"RECENT_EMP".to_string()));
    }

    #[test]
    fn merge_suggestions_with_context_aliases_prioritizes_aliases_in_table_context() {
        let merged = SqlEditorWidget::merge_suggestions_with_context_aliases(
            vec!["EMP".to_string(), "SELECT".to_string()],
            vec!["e".to_string(), "recent_emp".to_string(), "EMP".to_string()],
            true,
        );

        assert_eq!(merged[0], "e");
        assert_eq!(merged[1], "recent_emp");
        assert!(merged.contains(&"EMP".to_string()));
        assert!(merged.contains(&"SELECT".to_string()));
    }

    #[test]
    fn merge_suggestions_with_context_aliases_limits_to_max_suggestions() {
        let base: Vec<String> = (0..MAX_MERGED_SUGGESTIONS)
            .map(|i| format!("BASE_{:02}", i))
            .collect();
        let aliases = vec!["e".to_string(), "x".to_string()];

        let merged = SqlEditorWidget::merge_suggestions_with_context_aliases(base, aliases, true);

        assert_eq!(merged.len(), MAX_MERGED_SUGGESTIONS);
        assert_eq!(merged[0], "e");
        assert_eq!(merged[1], "x");
        assert!(!merged.contains(&format!("BASE_{:02}", MAX_MERGED_SUGGESTIONS - 1)));
    }

    #[test]
    fn merge_suggestions_with_context_aliases_respects_max_without_aliases() {
        let base: Vec<String> = (0..(MAX_MERGED_SUGGESTIONS + 5))
            .map(|i| format!("BASE_{:02}", i))
            .collect();

        let merged = SqlEditorWidget::merge_suggestions_with_context_aliases(base, vec![], false);

        assert_eq!(merged.len(), MAX_MERGED_SUGGESTIONS);
    }

    #[test]
    fn maybe_merge_suggestions_with_context_aliases_skips_aliases_when_qualified() {
        let base = vec!["EMPNO".to_string(), "ENAME".to_string()];
        let aliases = vec!["e".to_string(), "emp".to_string()];

        let merged = SqlEditorWidget::maybe_merge_suggestions_with_context_aliases(
            base.clone(),
            aliases,
            false,
            true,
        );

        assert_eq!(merged, base);
    }

    #[test]
    fn local_symbol_suggestions_include_var_command_before_cursor() {
        let suggestions = SqlEditorWidget::collect_local_symbol_suggestions_for_test(
            "VAR v_rc REFCURSOR;\nBEGIN\n    __CODEX_CURSOR__NULL;\nEND;",
            &[],
        );

        assert_has_case_insensitive(&suggestions, "V_RC");
    }

    #[test]
    fn local_symbol_suggestions_include_routine_parameters_and_locals() {
        let suggestions = SqlEditorWidget::collect_local_symbol_suggestions_for_test(
            r#"CREATE OR REPLACE PROCEDURE demo_proc (
    p_empno IN NUMBER,
    p_name  IN VARCHAR2
) IS
    v_total NUMBER := 0;
    c_status CONSTANT VARCHAR2(1) := 'Y';
BEGIN
    __CODEX_CURSOR__NULL;
END demo_proc;"#,
            &[],
        );

        for expected in ["p_empno", "p_name", "v_total", "c_status"] {
            assert_has_case_insensitive(&suggestions, expected);
        }
    }

    #[test]
    fn local_symbol_suggestions_keep_only_visible_nested_block_symbols() {
        let inner_suggestions = SqlEditorWidget::collect_local_symbol_suggestions_for_test(
            r#"DECLARE
    v_outer NUMBER := 1;
BEGIN
    DECLARE
        v_outer VARCHAR2(10) := 'inner';
        v_inner NUMBER := 2;
    BEGIN
        __CODEX_CURSOR__NULL;
    END;
END;"#,
            &[],
        );
        let outer_name_count = inner_suggestions
            .iter()
            .filter(|name| name.eq_ignore_ascii_case("v_outer"))
            .count();

        assert_eq!(outer_name_count, 1);
        assert_has_case_insensitive(&inner_suggestions, "v_inner");

        let outer_suggestions = SqlEditorWidget::collect_local_symbol_suggestions_for_test(
            r#"DECLARE
    v_outer NUMBER := 1;
BEGIN
    DECLARE
        v_inner NUMBER := 2;
    BEGIN
        NULL;
    END;

    __CODEX_CURSOR__NULL;
END;"#,
            &[],
        );

        assert_has_case_insensitive(&outer_suggestions, "v_outer");
        assert!(
            !outer_suggestions
                .iter()
                .any(|name| name.eq_ignore_ascii_case("v_inner")),
            "inner block symbol should not remain visible after END: {:?}",
            outer_suggestions
        );
    }

    #[test]
    fn local_symbol_suggestions_include_for_loop_record_only_inside_loop() {
        let loop_suggestions = SqlEditorWidget::collect_local_symbol_suggestions_for_test(
            r#"BEGIN
    FOR rec IN (SELECT empno FROM emp) LOOP
        __CODEX_CURSOR__NULL;
    END LOOP;
END;"#,
            &[],
        );

        assert_has_case_insensitive(&loop_suggestions, "rec");

        let after_loop_suggestions = SqlEditorWidget::collect_local_symbol_suggestions_for_test(
            r#"BEGIN
    FOR rec IN (SELECT empno FROM emp) LOOP
        NULL;
    END LOOP;

    __CODEX_CURSOR__NULL;
END;"#,
            &[],
        );

        assert!(
            !after_loop_suggestions
                .iter()
                .any(|name| name.eq_ignore_ascii_case("rec")),
            "loop record should not remain visible after END LOOP: {:?}",
            after_loop_suggestions
        );
    }

    #[test]
    fn local_symbol_suggestions_include_package_body_outer_declarations() {
        let suggestions = SqlEditorWidget::collect_local_symbol_suggestions_for_test(
            r#"CREATE OR REPLACE PACKAGE BODY demo_pkg AS
    g_cache NUMBER := 0;

    PROCEDURE run_demo IS
        v_local NUMBER := 1;
    BEGIN
        __CODEX_CURSOR__NULL;
    END run_demo;
END demo_pkg;"#,
            &[],
        );

        assert_has_case_insensitive(&suggestions, "g_cache");
        assert_has_case_insensitive(&suggestions, "v_local");
    }

    #[test]
    fn local_symbol_suggestions_include_package_body_routine_in_out_parameters() {
        let procedure_suggestions = SqlEditorWidget::collect_local_symbol_suggestions_for_test(
            r#"CREATE OR REPLACE PACKAGE BODY demo_pkg AS
    PROCEDURE upsert_emp(
        p_empno   IN NUMBER,
        p_ename   IN OUT VARCHAR2,
        p_message OUT VARCHAR2
    ) IS
    BEGIN
        __CODEX_CURSOR__NULL;
    END upsert_emp;
END demo_pkg;"#,
            &[],
        );

        for expected in ["p_empno", "p_ename", "p_message"] {
            assert_has_case_insensitive(&procedure_suggestions, expected);
        }

        let function_suggestions = SqlEditorWidget::collect_local_symbol_suggestions_for_test(
            r#"CREATE OR REPLACE PACKAGE BODY demo_pkg AS
    FUNCTION calc_bonus(
        p_base    IN NUMBER,
        p_percent IN OUT NUMBER,
        p_error   OUT VARCHAR2
    ) RETURN NUMBER IS
    BEGIN
        __CODEX_CURSOR__NULL;
        RETURN p_base;
    END calc_bonus;
END demo_pkg;"#,
            &[],
        );

        for expected in ["p_base", "p_percent", "p_error"] {
            assert_has_case_insensitive(&function_suggestions, expected);
        }
    }

    #[test]
    fn local_symbol_suggestions_include_package_body_parameters_when_comment_separates_name_and_paren(
    ) {
        let suggestions = SqlEditorWidget::collect_local_symbol_suggestions_for_test(
            r#"CREATE OR REPLACE PACKAGE BODY demo_pkg AS
    PROCEDURE run_demo
    -- keep implementation note
    (
        p_input  IN NUMBER,
        p_output OUT VARCHAR2
    ) IS
    BEGIN
        __CODEX_CURSOR__NULL;
    END run_demo;
END demo_pkg;"#,
            &[],
        );

        assert_has_case_insensitive(&suggestions, "p_input");
        assert_has_case_insensitive(&suggestions, "p_output");
    }

    #[test]
    fn local_symbol_suggestions_support_select_into_and_returning_into_targets() {
        let select_into = SqlEditorWidget::collect_local_symbol_suggestions_for_test(
            r#"DECLARE
    v_empno NUMBER;
BEGIN
    SELECT empno INTO __CODEX_CURSOR__ FROM emp WHERE rownum = 1;
END;"#,
            &[],
        );
        assert_has_case_insensitive(&select_into, "v_empno");

        let returning_into = SqlEditorWidget::collect_local_symbol_suggestions_for_test(
            r#"DECLARE
    v_empno NUMBER;
BEGIN
    DELETE FROM emp WHERE empno = 1 RETURNING empno INTO __CODEX_CURSOR__;
END;"#,
            &[],
        );
        assert_has_case_insensitive(&returning_into, "v_empno");
    }

    #[test]
    fn local_symbol_suggestions_merge_session_binds_without_duplicates() {
        let suggestions = SqlEditorWidget::collect_local_symbol_suggestions_for_test(
            "VAR v_text NUMBER;\nBEGIN\n    __CODEX_CURSOR__NULL;\nEND;",
            &["V_TEXT", "V_SESSION"],
        );
        let v_text_count = suggestions
            .iter()
            .filter(|name| name.eq_ignore_ascii_case("V_TEXT"))
            .count();

        assert_eq!(v_text_count, 1);
        assert_has_case_insensitive(&suggestions, "V_SESSION");
    }

    #[test]
    fn large_routine_cache_analysis_keeps_far_declarations_visible() {
        let mut sql = String::from("CREATE OR REPLACE PROCEDURE demo_proc IS\n");
        sql.push_str("    v_far NUMBER := 1;\n");
        for idx in 0..10_000 {
            sql.push_str(&format!("    v_pad_{idx} NUMBER := {idx};\n"));
        }
        sql.push_str("BEGIN\n");
        sql.push_str("    __CODEX_CURSOR__NULL;\n");
        sql.push_str("END demo_proc;");

        let cursor = sql
            .find("__CODEX_CURSOR__")
            .expect("cursor marker should exist");
        let sql = sql.replacen("__CODEX_CURSOR__", "", 1);
        let routine_cache = SqlEditorWidget::build_routine_symbol_cache_entry_for_test(&sql, cursor);
        let expanded = SqlEditorWidget::expanded_statement_window_in_text(&sql, cursor);
        let analysis = SqlEditorWidget::build_intellisense_analysis_from_routine_cache(
            &routine_cache,
            expanded.cursor_in_statement,
        );
        let suggestions = SqlEditorWidget::collect_local_symbol_suggestions(
            "",
            expanded.cursor_in_statement,
            &analysis,
            &[],
        );

        assert!(
            sql.len() > INTELLISENSE_STATEMENT_WINDOW as usize,
            "generated procedure should exceed the default statement window"
        );
        assert_has_case_insensitive(&suggestions, "v_far");
    }

    #[test]
    fn xmltable_alias_qualified_column_suggestions_include_columns_clause_names() {
        let sql_with_cursor = r#"
SELECT
  x.|,
  x.name
FROM oqt_t_xml t,
     XMLTABLE(
       '/root/dept'
       PASSING t.payload
       COLUMNS
         deptno NUMBER       PATH '@deptno',
         name   VARCHAR2(30) PATH 'name/text()',
         loc    VARCHAR2(30) PATH 'loc/text()'
     ) x
ORDER BY x.deptno
"#;

        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");
        let (stmt_start, stmt_end) = SqlEditorWidget::statement_bounds_in_text(&sql, cursor);
        let statement_text = sql.get(stmt_start..stmt_end).unwrap_or("");
        let cursor_in_statement = cursor.saturating_sub(stmt_start);
        let token_spans = super::query_text::tokenize_sql_spanned(statement_text);
        let split_idx = token_spans.partition_point(|span| span.end <= cursor_in_statement);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        let column_tables =
            intellisense_context::resolve_qualifier_tables("x", &deep_ctx.tables_in_scope);
        assert_eq!(column_tables, vec!["x".to_string()]);

        let data = Arc::new(Mutex::new(IntellisenseData::new()));
        let (sender, _receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let connection = create_shared_connection();

        for subq in &deep_ctx.subqueries {
            let body_tokens = intellisense_context::token_range_slice(
                deep_ctx.statement_tokens.as_ref(),
                subq.body_range,
            );
            let mut columns = intellisense_context::extract_select_list_columns(body_tokens);
            if columns.is_empty() {
                columns = intellisense_context::extract_table_function_columns(body_tokens);
            }
            let body_tables_in_scope =
                intellisense_context::collect_tables_in_statement(body_tokens);
            let (wildcard_columns, _wildcard_tables) =
                SqlEditorWidget::expand_virtual_table_wildcards(
                    body_tokens,
                    &body_tables_in_scope,
                    &HashMap::new(),
                    &data,
                    &sender,
                    &connection,
                );
            columns.extend(wildcard_columns);
            SqlEditorWidget::dedup_column_names_case_insensitive(&mut columns);
            if !columns.is_empty() {
                lock_or_recover(&data).set_virtual_table_columns(&subq.alias, columns);
            }
        }

        let mut guard = lock_or_recover(&data);
        let suggestions = guard.get_column_suggestions("", Some(&column_tables));
        assert!(
            suggestions.iter().any(|c| c.eq_ignore_ascii_case("deptno")),
            "expected deptno suggestion, got: {:?}",
            suggestions
        );
        assert!(
            suggestions.iter().any(|c| c.eq_ignore_ascii_case("name")),
            "expected name suggestion, got: {:?}",
            suggestions
        );
        assert!(
            suggestions.iter().any(|c| c.eq_ignore_ascii_case("loc")),
            "expected loc suggestion, got: {:?}",
            suggestions
        );
    }

    #[test]
    fn cte_chain_qualified_column_suggestions_include_wildcard_expansion() {
        let sql_with_cursor = r#"
WITH
  base AS (
    SELECT e.empno, e.ename, e.job, e.deptno, e.sal,
           REGEXP_REPLACE(e.ename, '[AEIOU]', '*') AS masked_name
    FROM oqt_t_emp e
  ),
  enriched AS (
    SELECT
      b.*,
      (SELECT d.dname FROM oqt_t_dept d WHERE d.deptno = b.deptno) AS dname,
      NTILE(3) OVER (PARTITION BY b.deptno ORDER BY b.sal DESC) AS sal_band
    FROM base b
  ),
  filtered AS (
    SELECT *
    FROM enriched
    WHERE (sal > (SELECT AVG(sal) FROM oqt_t_emp WHERE deptno = enriched.deptno))
       OR (job IN ('MANAGER','ANALYST') AND sal >= 2500)
  )
SELECT
  f.|,
  f.dname,
  f.empno,
  f.ename,
  f.masked_name,
  f.job,
  f.sal,
  f.sal_band,
  -- window frame with last_value (needs careful frame)
  LAST_VALUE(f.sal) OVER (
    PARTITION BY f.deptno
    ORDER BY f.sal
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS max_sal_via_last_value
FROM filtered f
ORDER BY f.deptno, f.sal DESC, f.empno;
"#;

        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");
        let (stmt_start, stmt_end) = SqlEditorWidget::statement_bounds_in_text(&sql, cursor);
        let statement_text = sql.get(stmt_start..stmt_end).unwrap_or("");
        let cursor_in_statement = cursor.saturating_sub(stmt_start);
        let token_spans = super::query_text::tokenize_sql_spanned(statement_text);
        let split_idx = token_spans.partition_point(|span| span.end <= cursor_in_statement);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        let column_tables =
            intellisense_context::resolve_qualifier_tables("f", &deep_ctx.tables_in_scope);
        assert_eq!(
            column_tables,
            vec!["filtered".to_string()],
            "qualifier should resolve to filtered CTE alias"
        );

        let data = Arc::new(Mutex::new(IntellisenseData::new()));
        let (sender, _receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let connection = create_shared_connection();

        for cte in &deep_ctx.ctes {
            let body_tokens = intellisense_context::token_range_slice(
                deep_ctx.statement_tokens.as_ref(),
                cte.body_range,
            );
            let mut columns = if !cte.explicit_columns.is_empty() {
                cte.explicit_columns.clone()
            } else if !cte.body_range.is_empty() {
                intellisense_context::extract_select_list_columns(body_tokens)
            } else {
                Vec::new()
            };
            if cte.explicit_columns.is_empty() && !cte.body_range.is_empty() {
                let body_tables_in_scope =
                    intellisense_context::collect_tables_in_statement(body_tokens);
                let (wildcard_columns, _wildcard_tables) =
                    SqlEditorWidget::expand_virtual_table_wildcards(
                        body_tokens,
                        &body_tables_in_scope,
                        &HashMap::new(),
                        &data,
                        &sender,
                        &connection,
                    );
                columns.extend(wildcard_columns);
            }
            SqlEditorWidget::dedup_column_names_case_insensitive(&mut columns);
            if !columns.is_empty() {
                lock_or_recover(&data).set_virtual_table_columns(&cte.name, columns);
            }
        }

        let mut guard = lock_or_recover(&data);
        let suggestions = guard.get_column_suggestions("", Some(&column_tables));

        assert!(
            suggestions.iter().any(|c| c.eq_ignore_ascii_case("EMPNO")),
            "expected EMPNO in suggestions: {:?}",
            suggestions
        );
        assert!(
            suggestions.iter().any(|c| c.eq_ignore_ascii_case("DNAME")),
            "expected DNAME in suggestions: {:?}",
            suggestions
        );
        assert!(
            suggestions
                .iter()
                .any(|c| c.eq_ignore_ascii_case("SAL_BAND")),
            "expected SAL_BAND in suggestions: {:?}",
            suggestions
        );
    }

    #[test]
    fn popup_confirm_key_without_selection_does_not_consume_editor_keys() {
        assert!(!SqlEditorWidget::should_consume_popup_confirm_key(
            Key::Tab,
            false,
        ));
        assert!(!SqlEditorWidget::should_consume_popup_confirm_key(
            Key::Enter,
            false,
        ));
        assert!(!SqlEditorWidget::should_consume_popup_confirm_key(
            Key::KPEnter,
            false,
        ));
    }

    #[test]
    fn popup_confirm_key_with_selection_consumes_enter_and_tab() {
        assert!(SqlEditorWidget::should_consume_popup_confirm_key(
            Key::Tab,
            true,
        ));
        assert!(SqlEditorWidget::should_consume_popup_confirm_key(
            Key::Enter,
            true,
        ));
        assert!(SqlEditorWidget::should_consume_popup_confirm_key(
            Key::KPEnter,
            true,
        ));
    }

    #[test]
    fn leading_indent_prefix_returns_leading_spaces_and_tabs_only() {
        assert_eq!(
            SqlEditorWidget::leading_indent_prefix("    select * from dual"),
            "    "
        );
        assert_eq!(
            SqlEditorWidget::leading_indent_prefix("\t\tselect * from dual"),
            "\t\t"
        );
        assert_eq!(
            SqlEditorWidget::leading_indent_prefix("  \t  select"),
            "  \t  "
        );
    }

    #[test]
    fn leading_indent_prefix_stops_at_first_non_indent_byte() {
        assert_eq!(SqlEditorWidget::leading_indent_prefix("select"), "");
        assert_eq!(SqlEditorWidget::leading_indent_prefix("  -- comment"), "  ");
        assert_eq!(SqlEditorWidget::leading_indent_prefix("  가나다"), "  ");
    }

    #[test]
    fn non_whitespace_char_before_cursor_in_text_detects_semicolon_before_cursor_marker() {
        let sql_with_cursor = "select * from help;|";
        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let ch = SqlEditorWidget::non_whitespace_char_before_cursor_in_text(&sql, cursor);
        assert_eq!(ch, Some(';'));
    }

    #[test]
    fn non_whitespace_char_before_cursor_in_text_skips_whitespace_after_semicolon() {
        let sql_with_cursor = "select * from help;   |";
        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let ch = SqlEditorWidget::non_whitespace_char_before_cursor_in_text(&sql, cursor);
        assert_eq!(ch, Some(';'));
    }

    #[test]
    fn invoke_void_callback_restores_slot_even_when_callback_panics() {
        let calls = Arc::new(Mutex::new(0usize));
        let calls_for_cb = calls.clone();
        let callback_slot: Arc<Mutex<Option<Box<dyn FnMut()>>>> =
            Arc::new(Mutex::new(Some(Box::new(move || {
                *lock_or_recover(&calls_for_cb) += 1;
                panic!("expected callback panic");
            }))));

        let invoked = SqlEditorWidget::invoke_void_callback(&callback_slot);

        assert!(invoked);
        assert!(lock_or_recover(&callback_slot).is_some());
        assert_eq!(*lock_or_recover(&calls), 1);
    }

    #[test]
    fn invoke_void_callback_can_run_again_after_panic() {
        let calls = Arc::new(Mutex::new(0usize));
        let calls_for_cb = calls.clone();
        let callback_slot: Arc<Mutex<Option<Box<dyn FnMut()>>>> =
            Arc::new(Mutex::new(Some(Box::new(move || {
                let mut count = lock_or_recover(&calls_for_cb);
                *count += 1;
                if *count == 1 {
                    panic!("expected first callback panic");
                }
            }))));

        let first_call = SqlEditorWidget::invoke_void_callback(&callback_slot);
        assert!(first_call);
        assert!(lock_or_recover(&callback_slot).is_some());

        let second_call = SqlEditorWidget::invoke_void_callback(&callback_slot);
        assert!(second_call);
        assert_eq!(*lock_or_recover(&calls), 2);
        assert!(lock_or_recover(&callback_slot).is_some());
    }

    #[test]
    fn invoke_void_callback_returns_false_when_slot_is_empty() {
        let callback_slot: Arc<Mutex<Option<Box<dyn FnMut()>>>> = Arc::new(Mutex::new(None));

        let invoked = SqlEditorWidget::invoke_void_callback(&callback_slot);

        assert!(!invoked);
        assert!(lock_or_recover(&callback_slot).is_none());
    }

    #[test]
    fn invoke_void_callback_keeps_replaced_callback_when_original_panics() {
        let callback_slot: Arc<Mutex<Option<Box<dyn FnMut()>>>> = Arc::new(Mutex::new(None));
        let replacement_ran = Arc::new(Mutex::new(false));
        let replacement_ran_for_cb = replacement_ran.clone();
        let callback_slot_for_cb = callback_slot.clone();

        *lock_or_recover(&callback_slot) = Some(Box::new(move || {
            let replacement_ran_for_replacement = replacement_ran_for_cb.clone();
            *lock_or_recover(&callback_slot_for_cb) = Some(Box::new(move || {
                *lock_or_recover(&replacement_ran_for_replacement) = true;
            }));
            panic!("expected panic after replacement");
        }));

        let first_call = SqlEditorWidget::invoke_void_callback(&callback_slot);
        assert!(first_call);
        assert!(lock_or_recover(&callback_slot).is_some());

        let second_call = SqlEditorWidget::invoke_void_callback(&callback_slot);
        assert!(second_call);
        assert!(*lock_or_recover(&replacement_ran));
    }

    #[test]
    fn invoke_file_drop_callback_restores_slot_even_when_callback_panics() {
        let calls = Arc::new(Mutex::new(Vec::<PathBuf>::new()));
        let calls_for_cb = calls.clone();
        let callback_slot: Arc<Mutex<Option<Box<dyn FnMut(PathBuf)>>>> =
            Arc::new(Mutex::new(Some(Box::new(move |path: PathBuf| {
                lock_or_recover(&calls_for_cb).push(path);
                panic!("expected callback panic");
            }))));

        let expected_path = PathBuf::from("/tmp/panic.sql");
        let invoked =
            SqlEditorWidget::invoke_file_drop_callback(&callback_slot, expected_path.clone());

        assert!(invoked);
        assert!(lock_or_recover(&callback_slot).is_some());
        assert_eq!(lock_or_recover(&calls).as_slice(), &[expected_path]);
    }

    #[test]
    fn invoke_file_drop_callback_can_run_again_after_panic() {
        let calls = Arc::new(Mutex::new(Vec::<PathBuf>::new()));
        let calls_for_cb = calls.clone();
        let callback_slot: Arc<Mutex<Option<Box<dyn FnMut(PathBuf)>>>> =
            Arc::new(Mutex::new(Some(Box::new(move |path: PathBuf| {
                let mut events = lock_or_recover(&calls_for_cb);
                let should_panic = events.is_empty();
                events.push(path);
                if should_panic {
                    panic!("expected first callback panic");
                }
            }))));

        let first_path = PathBuf::from("/tmp/first.sql");
        let second_path = PathBuf::from("/tmp/second.sql");

        let first_call =
            SqlEditorWidget::invoke_file_drop_callback(&callback_slot, first_path.clone());
        assert!(first_call);
        assert!(lock_or_recover(&callback_slot).is_some());

        let second_call =
            SqlEditorWidget::invoke_file_drop_callback(&callback_slot, second_path.clone());
        assert!(second_call);
        assert!(lock_or_recover(&callback_slot).is_some());
        assert_eq!(
            lock_or_recover(&calls).as_slice(),
            &[first_path, second_path]
        );
    }

    #[test]
    fn invoke_file_drop_callback_returns_false_when_slot_is_empty() {
        let callback_slot: Arc<Mutex<Option<Box<dyn FnMut(PathBuf)>>>> = Arc::new(Mutex::new(None));
        let path = PathBuf::from("/tmp/ignored.sql");

        let invoked = SqlEditorWidget::invoke_file_drop_callback(&callback_slot, path);

        assert!(!invoked);
        assert!(lock_or_recover(&callback_slot).is_none());
    }

    #[test]
    fn invoke_file_drop_callback_keeps_replaced_callback_when_original_panics() {
        let callback_slot: Arc<Mutex<Option<Box<dyn FnMut(PathBuf)>>>> = Arc::new(Mutex::new(None));
        let captured_paths = Arc::new(Mutex::new(Vec::<PathBuf>::new()));
        let captured_paths_for_cb = captured_paths.clone();
        let callback_slot_for_cb = callback_slot.clone();

        *lock_or_recover(&callback_slot) = Some(Box::new(move |_path: PathBuf| {
            let captured_paths_for_replacement = captured_paths_for_cb.clone();
            *lock_or_recover(&callback_slot_for_cb) = Some(Box::new(move |path: PathBuf| {
                lock_or_recover(&captured_paths_for_replacement).push(path);
            }));
            panic!("expected panic after replacement");
        }));

        let first_path = PathBuf::from("/tmp/first-replace.sql");
        let second_path = PathBuf::from("/tmp/second-replace.sql");

        let first_call = SqlEditorWidget::invoke_file_drop_callback(&callback_slot, first_path);
        assert!(first_call);
        assert!(lock_or_recover(&callback_slot).is_some());

        let second_call =
            SqlEditorWidget::invoke_file_drop_callback(&callback_slot, second_path.clone());
        assert!(second_call);
        assert_eq!(lock_or_recover(&captured_paths).as_slice(), &[second_path]);
    }

    #[test]
    fn classify_intellisense_context_treats_insert_column_list_as_column_context() {
        let sql_with_cursor = "INSERT INTO employees (|) VALUES (1)";
        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let token_spans = super::query_text::tokenize_sql_spanned(&sql);
        let split_idx = token_spans.partition_point(|span| span.end <= cursor);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        assert_eq!(deep_ctx.phase, intellisense_context::SqlPhase::SetClause);
        assert!(SqlEditorWidget::is_insert_column_list_context(
            deep_ctx.statement_tokens.as_ref(),
            deep_ctx.cursor_token_len
        ));

        let context = SqlEditorWidget::classify_intellisense_context(
            &deep_ctx,
            deep_ctx.statement_tokens.as_ref(),
        );
        assert_eq!(context, SqlContext::ColumnName);
    }

    #[test]
    fn classify_intellisense_context_treats_insert_all_second_column_list_as_column_context() {
        let sql_with_cursor =
            "INSERT ALL INTO emp_a (id) VALUES (1) INTO emp_b (|) VALUES (2) SELECT 1 FROM dual";
        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let token_spans = super::query_text::tokenize_sql_spanned(&sql);
        let split_idx = token_spans.partition_point(|span| span.end <= cursor);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        assert_eq!(deep_ctx.phase, intellisense_context::SqlPhase::SetClause);
        assert!(SqlEditorWidget::is_insert_column_list_context(
            deep_ctx.statement_tokens.as_ref(),
            deep_ctx.cursor_token_len
        ));

        let context = SqlEditorWidget::classify_intellisense_context(
            &deep_ctx,
            deep_ctx.statement_tokens.as_ref(),
        );
        assert_eq!(context, SqlContext::ColumnName);
    }

    #[test]
    fn classify_intellisense_context_treats_insert_first_second_column_list_as_column_context() {
        let sql_with_cursor = "INSERT FIRST WHEN 1 = 1 THEN INTO emp_a (id) VALUES (1) \
             WHEN 2 = 2 THEN INTO emp_b (|) VALUES (2) SELECT 1 FROM dual";
        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let token_spans = super::query_text::tokenize_sql_spanned(&sql);
        let split_idx = token_spans.partition_point(|span| span.end <= cursor);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        assert_eq!(deep_ctx.phase, intellisense_context::SqlPhase::SetClause);
        assert!(SqlEditorWidget::is_insert_column_list_context(
            deep_ctx.statement_tokens.as_ref(),
            deep_ctx.cursor_token_len
        ));

        let context = SqlEditorWidget::classify_intellisense_context(
            &deep_ctx,
            deep_ctx.statement_tokens.as_ref(),
        );
        assert_eq!(context, SqlContext::ColumnName);
    }

    #[test]
    fn insert_column_list_context_ignores_parentheses_after_select_body_starts() {
        let sql_with_cursor =
            "INSERT INTO audit_emp (emp_id) SELECT * FROM (SELECT | FROM oqt_t_emp) src";
        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let token_spans = super::query_text::tokenize_sql_spanned(&sql);
        let split_idx = token_spans.partition_point(|span| span.end <= cursor);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();

        assert!(
            !SqlEditorWidget::is_insert_column_list_context(&full_tokens, split_idx),
            "subquery parentheses after INSERT ... SELECT should not be treated as target column-list context"
        );
    }

    #[test]
    fn classify_intellisense_context_treats_with_cte_column_list_as_column_context() {
        let sql_with_cursor = "WITH r (|) AS (SELECT node_id FROM oqt_t_tree) SELECT * FROM r";
        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let token_spans = super::query_text::tokenize_sql_spanned(&sql);
        let split_idx = token_spans.partition_point(|span| span.end <= cursor);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        assert!(SqlEditorWidget::is_with_cte_column_list_context(&deep_ctx));

        let context = SqlEditorWidget::classify_intellisense_context(
            &deep_ctx,
            deep_ctx.statement_tokens.as_ref(),
        );
        assert_eq!(context, SqlContext::ColumnName);
    }

    #[test]
    fn cte_column_list_completion_prefers_body_projection_columns() {
        let sql_with_cursor = r#"
WITH r (node_id, |) AS (
  SELECT NODE_ID, parent_id, node_name, 1 AS lvl, '/'||node_name AS path
  FROM oqt_t_tree
  WHERE parent_id IS NULL
  UNION ALL
  SELECT t.NODE_ID, t.parent_id, t.node_name, r.lvl + 1,
         r.path || '/' || t.node_name
  FROM oqt_t_tree t
  JOIN r ON t.PARENT_ID = r.node_id
)
SELECT * FROM r
"#;

        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let token_spans = super::query_text::tokenize_sql_spanned(&sql);
        let split_idx = token_spans.partition_point(|span| span.end <= cursor);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        let cte = deep_ctx
            .ctes
            .iter()
            .find(|cte| cte.name.eq_ignore_ascii_case("r"))
            .expect("expected CTE r");
        assert!(SqlEditorWidget::is_cursor_inside_cte_explicit_column_list(
            &deep_ctx, cte
        ));

        let data = Arc::new(Mutex::new(IntellisenseData::new()));
        let (sender, _receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let connection = create_shared_connection();

        let (columns, _) = SqlEditorWidget::collect_cte_virtual_columns_for_completion(
            &deep_ctx,
            cte,
            &HashMap::new(),
            &data,
            &sender,
            &connection,
        );

        for expected in ["node_id", "parent_id", "node_name", "lvl", "path"] {
            assert!(
                columns.iter().any(|col| col.eq_ignore_ascii_case(expected)),
                "expected `{expected}` in CTE explicit-column completion candidates: {:?}",
                columns
            );
        }
    }

    #[test]
    fn classify_intellisense_context_treats_model_clause_as_column_context() {
        let sql_with_cursor = "WITH m AS ( \
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
             )";
        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let token_spans = super::query_text::tokenize_sql_spanned(&sql);
        let split_idx = token_spans.partition_point(|span| span.end <= cursor);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        assert_eq!(deep_ctx.phase, intellisense_context::SqlPhase::ModelClause);

        let context = SqlEditorWidget::classify_intellisense_context(
            &deep_ctx,
            deep_ctx.statement_tokens.as_ref(),
        );
        assert_eq!(context, SqlContext::ColumnName);
    }

    #[test]
    fn resolve_column_tables_maps_match_recognize_pattern_variable_to_scope_tables() {
        let sql_with_cursor = r#"
	SELECT *
	FROM oqt_t_emp
MATCH_RECOGNIZE (
  PARTITION BY deptno
  ORDER BY hiredate, empno
  MEASURES
    FIRST(ename) AS start_name,
    LAST(ename) AS end_name
  ONE ROW PER MATCH
  PATTERN (a b+)
  DEFINE
    b AS b.| > PREV(b.sal)
)
"#;

        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let token_spans = super::query_text::tokenize_sql_spanned(&sql);
        let split_idx = token_spans.partition_point(|span| span.end <= cursor);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        let column_tables =
            SqlEditorWidget::resolve_column_tables_for_context(Some("b"), &deep_ctx);
        assert!(
            column_tables
                .iter()
                .any(|table| table.eq_ignore_ascii_case("oqt_t_emp")),
            "pattern variable b should resolve to source tables, got: {:?}",
            column_tables
        );
        assert!(
            !column_tables
                .iter()
                .any(|table| table.eq_ignore_ascii_case("b")),
            "pattern variable should not fall back to raw qualifier table key: {:?}",
            column_tables
        );
    }

    #[test]
    fn merge_derived_columns_includes_model_measure_aliases() {
        let tokens = SqlEditorWidget::tokenize_sql(
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

        let mut derived_columns =
            intellisense_context::extract_oracle_unpivot_generated_columns(&tokens);
        derived_columns
            .extend(intellisense_context::extract_oracle_model_generated_columns(&tokens));

        let merged = SqlEditorWidget::merge_suggestions_with_derived_columns(
            vec!["deptno".to_string(), "sum_sal".to_string()],
            "",
            derived_columns,
        );

        assert!(
            merged
                .iter()
                .any(|c| c.eq_ignore_ascii_case("avg_sal_calc")),
            "expected avg_sal_calc in merged suggestions, got: {:?}",
            merged
        );
        assert!(
            merged
                .iter()
                .any(|c| c.eq_ignore_ascii_case("sum_plus_100")),
            "expected sum_plus_100 in merged suggestions, got: {:?}",
            merged
        );
    }

    #[test]
    fn merge_derived_columns_includes_match_recognize_measures_aliases() {
        let tokens = SqlEditorWidget::tokenize_sql(
            "SELECT * \
             FROM emp \
             MATCH_RECOGNIZE ( \
               MEASURES FIRST(ename) AS start_name, LAST(ename) AS end_name \
               PATTERN (a b+) \
               DEFINE b AS b.sal > PREV(b.sal) \
             ) mr",
        );

        let derived_columns = intellisense_context::extract_match_recognize_generated_columns(&tokens);
        let merged = SqlEditorWidget::merge_suggestions_with_derived_columns(
            vec!["empno".to_string()],
            "",
            derived_columns,
        );

        assert!(
            merged
                .iter()
                .any(|c| c.eq_ignore_ascii_case("start_name")),
            "expected start_name in merged suggestions, got: {:?}",
            merged
        );
        assert!(
            merged
                .iter()
                .any(|c| c.eq_ignore_ascii_case("end_name")),
            "expected end_name in merged suggestions, got: {:?}",
            merged
        );
    }

    #[test]
    fn collect_derived_columns_for_order_by_includes_select_aliases() {
        let sql_with_cursor = "SELECT \
             oh.order_id, \
             oh.cust_name, \
             oh.order_dt, \
             (SELECT SUM(oi.qty*oi.unit_price) FROM oqt_t_order_item oi WHERE oi.ORDER_ID = oh.order_id) AS amt \
           FROM oqt_t_order_hdr oh \
           ORDER BY \
             (SELECT COUNT(*) FROM oqt_t_order_item oi WHERE oi.order_id = oh.order_id) DESC, \
             | DESC NULLS LAST \
           FETCH FIRST 3 ROWS ONLY";
        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let token_spans = super::query_text::tokenize_sql_spanned(&sql);
        let split_idx = token_spans.partition_point(|span| span.end <= cursor);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        assert_eq!(
            deep_ctx.phase,
            intellisense_context::SqlPhase::OrderByClause
        );

        let derived = SqlEditorWidget::collect_derived_columns_for_context(&deep_ctx);
        assert!(
            derived.iter().any(|c| c.eq_ignore_ascii_case("amt")),
            "expected select-list alias `amt` in derived columns: {:?}",
            derived
        );
    }

    #[test]
    fn infer_columns_from_partial_select_qualifier_uses_virtual_table_columns() {
        let sql_with_cursor = r#"
SELECT
  jt.order_id,
  it.|,
  (it.qty * it.price) AS line_amt
FROM oqt_t_json j
CROSS JOIN JSON_TABLE(
  j.payload,
  '$'
  COLUMNS (
    order_id NUMBER PATH '$.order_id',
    NESTED PATH '$.items[*]'
    COLUMNS (
      sku   VARCHAR2(30) PATH '$.sku',
      qty   NUMBER       PATH '$.qty',
      price NUMBER       PATH '$.price'
    )
  )
) jt
CROSS APPLY (
  SELECT jt., jt., jt. FROM dual
) it
"#;

        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let token_spans = super::query_text::tokenize_sql_spanned(&sql);
        let split_idx = token_spans.partition_point(|span| span.end <= cursor);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        let it_subq = deep_ctx
            .subqueries
            .iter()
            .find(|s| s.alias.eq_ignore_ascii_case("it"))
            .expect("expected apply subquery alias it");
        let body_tokens = intellisense_context::token_range_slice(
            deep_ctx.statement_tokens.as_ref(),
            it_subq.body_range,
        );
        let body_tables_in_scope = intellisense_context::collect_tables_in_statement(body_tokens);

        let mut virtual_table_columns = HashMap::new();
        virtual_table_columns.insert(
            "jt".to_string(),
            vec![
                "order_id".to_string(),
                "sku".to_string(),
                "qty".to_string(),
                "price".to_string(),
            ],
        );

        let data = Arc::new(Mutex::new(IntellisenseData::new()));
        let (sender, _receiver) = mpsc::channel::<ColumnLoadUpdate>();
        let connection = create_shared_connection();
        let inferred = SqlEditorWidget::infer_columns_from_partial_select_qualifiers(
            body_tokens,
            &body_tables_in_scope,
            &deep_ctx.tables_in_scope,
            &virtual_table_columns,
            &data,
            &sender,
            &connection,
        );

        for expected in ["order_id", "sku", "qty", "price"] {
            assert!(
                inferred.iter().any(|c| c.eq_ignore_ascii_case(expected)),
                "expected inferred column `{expected}` in {:?}",
                inferred
            );
        }
    }

    #[test]
    fn classify_intellisense_context_keeps_insert_into_target_as_table_context() {
        let sql_with_cursor = "INSERT INTO |";
        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let token_spans = super::query_text::tokenize_sql_spanned(&sql);
        let split_idx = token_spans.partition_point(|span| span.end <= cursor);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        assert_eq!(deep_ctx.phase, intellisense_context::SqlPhase::IntoClause);
        assert!(!SqlEditorWidget::is_insert_column_list_context(
            deep_ctx.statement_tokens.as_ref(),
            deep_ctx.cursor_token_len
        ));

        let context = SqlEditorWidget::classify_intellisense_context(
            &deep_ctx,
            deep_ctx.statement_tokens.as_ref(),
        );
        assert_eq!(context, SqlContext::TableName);
    }

    #[test]
    fn classify_intellisense_context_treats_insert_values_expression_as_column_context() {
        let sql_with_cursor = "INSERT INTO target (id) VALUES (|)";
        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let token_spans = super::query_text::tokenize_sql_spanned(&sql);
        let split_idx = token_spans.partition_point(|span| span.end <= cursor);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        assert_eq!(deep_ctx.phase, intellisense_context::SqlPhase::ValuesClause);
        assert!(
            deep_ctx.phase.is_column_context(),
            "phase: {:?}",
            deep_ctx.phase
        );

        let context = SqlEditorWidget::classify_intellisense_context(
            &deep_ctx,
            deep_ctx.statement_tokens.as_ref(),
        );
        assert_eq!(context, SqlContext::ColumnName);
    }

    #[test]
    fn classify_intellisense_context_treats_merge_insert_column_list_as_column_context() {
        let sql_with_cursor =
            "MERGE INTO target t USING source s ON (t.id = s.id) WHEN NOT MATCHED THEN INSERT (|) VALUES (s.id)";
        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let token_spans = super::query_text::tokenize_sql_spanned(&sql);
        let split_idx = token_spans.partition_point(|span| span.end <= cursor);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        assert!(
            deep_ctx.phase.is_column_context(),
            "phase should be column-oriented in MERGE insert action: {:?}",
            deep_ctx.phase
        );

        let context = SqlEditorWidget::classify_intellisense_context(
            &deep_ctx,
            deep_ctx.statement_tokens.as_ref(),
        );
        assert_eq!(context, SqlContext::ColumnName);
    }

    #[test]
    fn classify_intellisense_context_treats_merge_update_set_as_column_context() {
        let sql_with_cursor =
            "MERGE INTO target t USING source s ON (t.id = s.id) WHEN MATCHED THEN UPDATE SET t.value = |";
        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let token_spans = super::query_text::tokenize_sql_spanned(&sql);
        let split_idx = token_spans.partition_point(|span| span.end <= cursor);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        assert!(
            deep_ctx.phase.is_column_context(),
            "phase: {:?}",
            deep_ctx.phase
        );

        let context = SqlEditorWidget::classify_intellisense_context(
            &deep_ctx,
            deep_ctx.statement_tokens.as_ref(),
        );
        assert_eq!(context, SqlContext::ColumnName);
    }

    #[test]
    fn classify_intellisense_context_treats_merge_delete_where_as_column_context() {
        let sql_with_cursor =
            "MERGE INTO target t USING source s ON (t.id = s.id) WHEN MATCHED THEN DELETE WHERE |";
        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let token_spans = super::query_text::tokenize_sql_spanned(&sql);
        let split_idx = token_spans.partition_point(|span| span.end <= cursor);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        assert!(
            deep_ctx.phase.is_column_context(),
            "phase: {:?}",
            deep_ctx.phase
        );

        let context = SqlEditorWidget::classify_intellisense_context(
            &deep_ctx,
            deep_ctx.statement_tokens.as_ref(),
        );
        assert_eq!(context, SqlContext::ColumnName);
    }

    #[test]
    fn classify_intellisense_context_treats_select_into_target_as_general_context() {
        let sql_with_cursor = "BEGIN SELECT empno INTO | FROM emp WHERE rownum = 1; END;";
        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let token_spans = super::query_text::tokenize_sql_spanned(&sql);
        let split_idx = token_spans.partition_point(|span| span.end <= cursor);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        assert!(SqlEditorWidget::is_variable_target_into_context(
            deep_ctx.statement_tokens.as_ref(),
            deep_ctx.cursor_token_len
        ));

        let context = SqlEditorWidget::classify_intellisense_context(
            &deep_ctx,
            deep_ctx.statement_tokens.as_ref(),
        );
        assert_eq!(context, SqlContext::General);
    }

    #[test]
    fn classify_intellisense_context_treats_bulk_collect_into_target_as_general_context() {
        let sql_with_cursor = "BEGIN SELECT empno BULK COLLECT INTO | FROM emp; END;";
        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let token_spans = super::query_text::tokenize_sql_spanned(&sql);
        let split_idx = token_spans.partition_point(|span| span.end <= cursor);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        assert!(SqlEditorWidget::is_variable_target_into_context(
            deep_ctx.statement_tokens.as_ref(),
            deep_ctx.cursor_token_len
        ));

        let context = SqlEditorWidget::classify_intellisense_context(
            &deep_ctx,
            deep_ctx.statement_tokens.as_ref(),
        );
        assert_eq!(context, SqlContext::General);
    }

    #[test]
    fn classify_intellisense_context_treats_insert_returning_expression_as_column_context() {
        let sql_with_cursor = "INSERT INTO emp (empno, ename) VALUES (1, 'ICE') RETURNING | INTO :v_empno";
        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let token_spans = super::query_text::tokenize_sql_spanned(&sql);
        let split_idx = token_spans.partition_point(|span| span.end <= cursor);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        assert!(
            deep_ctx.phase.is_column_context(),
            "RETURNING list should be column context, got {:?}",
            deep_ctx.phase
        );

        let context = SqlEditorWidget::classify_intellisense_context(
            &deep_ctx,
            deep_ctx.statement_tokens.as_ref(),
        );
        assert_eq!(context, SqlContext::ColumnName);
    }

    #[test]
    fn resolve_column_tables_for_merge_insert_column_list_prefers_merge_target() {
        let sql_with_cursor =
            "MERGE INTO target_table t USING source_table s ON (t.id = s.id) WHEN NOT MATCHED THEN INSERT (|) VALUES (s.id)";
        let cursor = sql_with_cursor
            .find('|')
            .expect("cursor marker should exist");
        let sql = sql_with_cursor.replace('|', "");

        let token_spans = super::query_text::tokenize_sql_spanned(&sql);
        let split_idx = token_spans.partition_point(|span| span.end <= cursor);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let deep_ctx = intellisense_context::analyze_cursor_context(&full_tokens, split_idx);

        let tables = SqlEditorWidget::resolve_column_tables_for_context(None, &deep_ctx);
        assert_eq!(tables, vec!["target_table".to_string()]);
    }

    #[test]
    fn extract_select_list_columns_supports_literal_implicit_alias_in_cte() {
        let sql = "SELECT 'Y' flag FROM dual";
        let token_spans = super::query_text::tokenize_sql_spanned(sql);
        let full_tokens: Vec<SqlToken> = token_spans.into_iter().map(|span| span.token).collect();
        let columns = intellisense_context::extract_select_list_columns(&full_tokens);

        assert!(
            columns.iter().any(|col| col.eq_ignore_ascii_case("flag")),
            "expected implicit literal alias in columns: {:?}",
            columns
        );
    }

    #[test]
    fn finalize_completion_after_selection_clears_pending_and_invalidates_generation() {
        let runtime = runtime_state_for_test(
            Some((5, 10)),
            Some(PendingIntellisense { cursor_pos: 10 }),
            3,
            9,
        );

        SqlEditorWidget::finalize_completion_after_selection(&runtime);

        assert!(runtime.completion_range().is_none());
        assert!(runtime.pending_intellisense().is_none());
        assert_eq!(runtime.current_keyup_generation(), 4);
        assert_eq!(runtime.current_parse_generation(), 10);
    }
