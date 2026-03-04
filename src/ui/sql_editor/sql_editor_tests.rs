use super::*;
use crate::ui::syntax_highlight::{STYLE_COMMENT, STYLE_DEFAULT, STYLE_KEYWORD, STYLE_STRING};

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

fn load_test_file(name: &str) -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("test");
    path.push(name);
    fs::read_to_string(path).unwrap_or_default()
}

fn count_slash_lines(text: &str) -> usize {
    text.lines().filter(|line| line.trim() == "/").count()
}

fn assert_contains_all(haystack: &str, needles: &[&str]) {
    for needle in needles {
        assert!(
            haystack.contains(needle),
            "Expected output to contain: {}",
            needle
        );
    }
}

#[test]
fn update_alert_pump_state_after_display_reschedules_when_queue_not_empty() {
    let mut pump_scheduled = true;
    let should_schedule = update_alert_pump_state_after_display(false, &mut pump_scheduled);
    assert!(should_schedule);
    assert!(pump_scheduled);

    let should_schedule_empty = update_alert_pump_state_after_display(true, &mut pump_scheduled);
    assert!(!should_schedule_empty);
    assert!(!pump_scheduled);
}

#[test]
fn column_poll_pending_action_state_machine_transitions_and_clear_rules() {
    let mut action = ColumnPollPendingAction::None;
    action.request_refresh();
    assert_eq!(action, ColumnPollPendingAction::Refresh);
    assert!(action.should_refresh());
    assert!(!action.should_clear(false));

    let mut action = ColumnPollPendingAction::None;
    action.request_clear();
    assert_eq!(action, ColumnPollPendingAction::Clear);
    assert!(!action.should_refresh());
    assert!(action.should_clear(false));
    assert!(!action.should_clear(true));

    let mut action = ColumnPollPendingAction::Refresh;
    action.request_clear();
    assert_eq!(action, ColumnPollPendingAction::RefreshThenClear);
    assert!(action.should_refresh());
    assert!(action.should_clear(false));
    assert!(!action.should_clear(true));
}

#[test]
fn is_window_shown_and_visible_requires_both_flags() {
    assert!(is_window_shown_and_visible(true, true));
    assert!(!is_window_shown_and_visible(true, false));
    assert!(!is_window_shown_and_visible(false, true));
    assert!(!is_window_shown_and_visible(false, false));
}

#[test]
fn default_style_text_for_len_matches_requested_length() {
    let styles = SqlEditorWidget::default_style_text_for_len(8);
    assert_eq!(styles.chars().count(), 8);
    assert!(styles.chars().all(|ch| ch == STYLE_DEFAULT));
}

#[test]
fn sql_editor_alert_calls_use_wrapper_function() {
    let mod_src = include_str!("mod.rs");
    assert!(
        mod_src.contains("pub(crate) fn show_alert_dialog"),
        "SqlEditorWidget::show_alert_dialog helper must be defined in mod.rs"
    );
    assert!(
        mod_src.contains("struct PendingAlertState"),
        "mod.rs should keep a single shared alert queue state"
    );
    assert!(
        mod_src.contains("fn drain_pending_alerts()"),
        "mod.rs should process alerts through a single drain function"
    );
    assert!(
        mod_src.contains("is_window_shown_and_visible(window.shown(), window.visible())"),
        "main window visibility check should require shown() and visible()"
    );
    assert!(
        !mod_src.contains("fn show_alert_when_main_window_visible"),
        "legacy per-alert recursive retry helper should not remain"
    );
    assert_eq!(
        mod_src.matches("fltk::dialog::alert_default(").count(),
        1,
        "mod.rs should call fltk::dialog::alert_default only inside queue drain"
    );

    let file_checks = [
        ("execution.rs", include_str!("execution.rs")),
        ("dba_tools.rs", include_str!("dba_tools.rs")),
        ("session_monitor.rs", include_str!("session_monitor.rs")),
    ];

    for (name, source) in file_checks {
        assert_eq!(
            source.matches("fltk::dialog::alert_default(").count(),
            0,
            "{name} should route alerts through SqlEditorWidget::show_alert_dialog"
        );
    }
}

#[test]
fn format_sql_preserves_script_commands_and_slashes() {
    let cases = [
        (
            "test1.txt",
            vec![
                "PROMPT 프로시저 테스트1",
                "SET SERVEROUTPUT ON",
                "SHOW ERRORS",
            ],
            vec![
                "OQT(Oracle Query Tool) - Procedure/Function Test Script",
                "-- 1) TEST DATA / TABLES",
            ],
        ),
        (
            "test2.txt",
            vec![
                "PROMPT 프로시저 테스트 4",
                "SET SERVEROUTPUT ON SIZE UNLIMITED",
                "SHOW ERRORS PACKAGE oqt_pkg",
                "SHOW ERRORS PACKAGE BODY oqt_pkg",
            ],
            vec![
                "PROMPT === [5] CALL VARIANTS: EXEC/BEGIN/DEFAULT/NAMED/POSITIONAL/NULL/UNICODE ===",
            ],
        ),
        (
            "test3.txt",
            vec![
                "PROMPT 프로시저 테스트3",
                "SET DEFINE OFF",
                "PROMPT === [B] Cleanup ===",
                "SHOW ERRORS",
            ],
            vec![
                "OQT (Oracle Query Tool) Compatibility Test Script (TOAD-like)",
            ],
        ),
    ];

    for (file, expected_lines, comment_snippets) in cases {
        let input = load_test_file(file);
        let formatted = SqlEditorWidget::format_sql_basic(&input);

        assert_contains_all(&formatted, &expected_lines);
        assert_contains_all(&formatted, &comment_snippets);

        let input_slashes = count_slash_lines(&input);
        let output_slashes = count_slash_lines(&formatted);
        assert_eq!(
            input_slashes, output_slashes,
            "Slash terminator count differs for {}",
            file
        );

        let formatted_again = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            formatted, formatted_again,
            "Formatting should be idempotent for {}",
            file
        );
    }
}

#[test]
fn format_sql_preserves_connect_password_with_at_sign() {
    let input = "CONNECT user/p@ss@localhost:1521/ORCL\nSELECT 1 FROM dual;";

    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains("CONNECT user/p@ss@localhost:1521/ORCL"),
        "CONNECT password containing @ should be preserved, got:\n{}",
        formatted
    );
    assert!(
        formatted.contains("SELECT 1\nFROM DUAL;"),
        "SELECT statement should still be formatted normally, got:\n{}",
        formatted
    );
}

#[test]
fn format_sql_preserves_connect_password_with_slash() {
    let input = "CONNECT user/pa/ss@localhost:1521/ORCL
SELECT 1 FROM dual;";

    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains("CONNECT user/pa/ss@localhost:1521/ORCL"),
        "CONNECT password containing / should be preserved, got:
{}",
        formatted
    );
    assert!(
        formatted.contains(
            "SELECT 1
FROM DUAL;"
        ),
        "SELECT statement should still be formatted normally, got:
{}",
        formatted
    );
}

#[test]
fn format_sql_preserves_mega_torture_script() {
    let input = load_test_file("mega_torture.txt");
    let formatted = SqlEditorWidget::format_sql_basic(&input);

    let expected_lines = vec![
        "PROMPT [0] bind/substitution setup",
        "WHENEVER SQLERROR EXIT SQL.SQLCODE",
        "SHOW ERRORS PACKAGE BODY oqt_mega_pkg",
        "PROMPT [6] trigger (extra nesting surface)",
        "PROMPT [DONE]",
    ];
    let comment_snippets = vec![
        "q'[ | tokens: END; / ; /* */ -- ]'",
        "q'[ |trg tokens: END; / ; /* */ -- ]'",
        "q'[ |q-quote: END; / ; /* */ -- ]'",
    ];

    assert_contains_all(&formatted, &expected_lines);
    assert_contains_all(&formatted, &comment_snippets);

    let input_slashes = count_slash_lines(&input);
    let output_slashes = count_slash_lines(&formatted);
    assert_eq!(
        input_slashes, output_slashes,
        "Slash terminator count differs for mega_torture.txt"
    );

    let formatted_again = SqlEditorWidget::format_sql_basic(&formatted);
    assert_eq!(
        formatted, formatted_again,
        "Formatting should be idempotent for mega_torture.txt"
    );
}

#[test]
fn format_sql_preserves_whenever_sqlerror_options() {
    let input = [
        "WHENEVER SQLERROR EXIT SQL.SQLCODE",
        "WHENEVER SQLERROR EXIT FAILURE ROLLBACK",
        "WHENEVER SQLERROR EXIT SUCCESS",
        "WHENEVER SQLERROR EXIT WARNING",
        "WHENEVER SQLERROR EXIT 1",
        "WHENEVER SQLERROR CONTINUE",
        "WHENEVER SQLERROR CONTINUE ROLLBACK",
    ]
    .join("\n");

    let formatted = SqlEditorWidget::format_sql_basic(&input);
    let expected_lines = vec![
        "WHENEVER SQLERROR EXIT SQL.SQLCODE",
        "WHENEVER SQLERROR EXIT FAILURE ROLLBACK",
        "WHENEVER SQLERROR EXIT SUCCESS",
        "WHENEVER SQLERROR EXIT WARNING",
        "WHENEVER SQLERROR EXIT 1",
        "WHENEVER SQLERROR CONTINUE",
        "WHENEVER SQLERROR CONTINUE ROLLBACK",
    ];

    assert_contains_all(&formatted, &expected_lines);

    let formatted_again = SqlEditorWidget::format_sql_basic(&formatted);
    assert_eq!(
        formatted, formatted_again,
        "Formatting should be idempotent for WHENEVER SQLERROR variants"
    );
}

#[test]
fn format_sql_breaks_minified_package_body_members() {
    let input = "CREATE OR REPLACE PACKAGE BODY pkg AS PROCEDURE p IS BEGIN NULL; END; FUNCTION f RETURN NUMBER IS BEGIN RETURN 1; END; END pkg;";
    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains("PACKAGE BODY pkg AS\n    PROCEDURE p IS"),
        "Package body should break before first procedure, got: {}",
        formatted
    );
    assert!(
        formatted.contains("END;\n\n    FUNCTION f RETURN NUMBER IS"),
        "Package body members should be separated by blank line, got: {}",
        formatted
    );
}

#[test]
fn format_sql_preserves_oracle_labels() {
    // Test <<loop_label>> preservation
    let input = "<<outer_loop>>\nFOR i IN 1..10 LOOP\n<<inner_loop>>\nFOR j IN 1..5 LOOP\nNULL;\nEND LOOP inner_loop;\nEND LOOP outer_loop;";
    let formatted = SqlEditorWidget::format_sql_basic(input);

    // Labels should be preserved without extra spaces
    assert!(
        formatted.contains("<<outer_loop>>"),
        "Label <<outer_loop>> should be preserved, got: {}",
        formatted
    );
    assert!(
        formatted.contains("<<inner_loop>>"),
        "Label <<inner_loop>> should be preserved, got: {}",
        formatted
    );

    // Idempotent test
    let formatted_again = SqlEditorWidget::format_sql_basic(&formatted);
    assert_eq!(
        formatted, formatted_again,
        "Formatting should be idempotent for labels"
    );
}

#[test]
fn format_sql_preserves_q_quoted_strings() {
    // Test q'[...]' quote literal preservation
    let cases = [
        ("SELECT q'[It's a test]' FROM dual", "q'[It's a test]'"),
        ("SELECT q'{Hello World}' FROM dual", "q'{Hello World}'"),
        (
            "SELECT q'(Text with 'quotes')' FROM dual",
            "q'(Text with 'quotes')'",
        ),
        (
            "SELECT q'<Value with <brackets>>'",
            "q'<Value with <brackets>>'",
        ),
        (
            "SELECT Q'!Delimiter test!' FROM dual",
            "Q'!Delimiter test!'",
        ),
    ];

    for (input, expected_literal) in cases {
        let formatted = SqlEditorWidget::format_sql_basic(input);
        assert!(
            formatted.contains(expected_literal),
            "Q-quoted literal {} should be preserved in: {}",
            expected_literal,
            formatted
        );

        // Idempotent test
        let formatted_again = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            formatted, formatted_again,
            "Formatting should be idempotent for q-quoted string: {}",
            input
        );
    }
}

#[test]
fn format_sql_preserves_combined_special_syntax() {
    // Test combination of labels and q-quoted strings
    let input = r#"<<process_data>>
BEGIN
v_sql := q'[SELECT * FROM table WHERE name = 'test']';
EXECUTE IMMEDIATE v_sql;
END;
"#;
    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains("<<process_data>>"),
        "Label should be preserved"
    );
    assert!(
        formatted.contains("q'[SELECT * FROM table WHERE name = 'test']'"),
        "Q-quoted string should be preserved exactly"
    );
}

#[test]
fn format_sql_preserves_nq_quoted_strings() {
    // Test nq'[...]' (National Character q-quoted strings)
    let test_cases = [
        (
            "SELECT nq'[한글 문자열]' FROM dual",
            "nq'[한글 문자열]'",
            "basic nq'[...]' preservation",
        ),
        (
            "SELECT NQ'[UPPERCASE]' FROM dual",
            "NQ'[UPPERCASE]'",
            "uppercase NQ'[...]' preservation",
        ),
        (
            "SELECT Nq'[mixed case]' FROM dual",
            "Nq'[mixed case]'",
            "mixed case Nq'[...]' preservation",
        ),
        (
            "SELECT nq'(parentheses)' FROM dual",
            "nq'(parentheses)'",
            "nq'(...)' with parentheses",
        ),
        (
            "SELECT nq'{braces}' FROM dual",
            "nq'{braces}'",
            "nq'{...}' with braces",
        ),
        (
            "SELECT nq'<angle brackets>' FROM dual",
            "nq'<angle brackets>'",
            "nq'<...>' with angle brackets",
        ),
        (
            "SELECT nq'!custom!' FROM dual",
            "nq'!custom!'",
            "nq'!...!' with custom delimiter",
        ),
    ];

    for (input, expected, description) in test_cases {
        let formatted = SqlEditorWidget::format_sql_basic(input);
        assert!(
            formatted.contains(expected),
            "{}: expected '{}' in formatted output, got: {}",
            description,
            expected,
            formatted
        );
    }
}

#[test]
fn format_sql_preserves_nq_quote_with_semicolon() {
    // Test that semicolons inside nq'...' are preserved
    let input = "SELECT nq'[text with ; semicolon]' FROM dual";
    let formatted = SqlEditorWidget::format_sql_basic(input);
    assert!(
        formatted.contains("nq'[text with ; semicolon]'"),
        "nq'...' with semicolon should be preserved exactly, got: {}",
        formatted
    );
}

#[test]
fn format_sql_preserves_mixed_q_and_nq_quotes() {
    // Test both q'...' and nq'...' in same statement
    let input = "SELECT q'[regular]', nq'[national]' FROM dual";
    let formatted = SqlEditorWidget::format_sql_basic(input);
    assert!(
        formatted.contains("q'[regular]'"),
        "q'...' should be preserved, got: {}",
        formatted
    );
    assert!(
        formatted.contains("nq'[national]'"),
        "nq'...' should be preserved, got: {}",
        formatted
    );
}

#[test]
fn tokenize_sql_handles_nq_quotes() {
    // Direct test of tokenization for nq'...'
    let sql = "SELECT nq'[test string]' FROM dual";
    let tokens = SqlEditorWidget::tokenize_sql(sql);

    // Should have tokens: SELECT, nq'[test string]', FROM, dual
    let has_nq_string = tokens.iter().any(|t| {
        if let SqlToken::String(s) = t {
            s.contains("nq'[test string]'")
        } else {
            false
        }
    });
    assert!(
        has_nq_string,
        "Tokenizer should produce String token for nq'[...]', got: {:?}",
        tokens
    );
}

#[test]
fn format_sql_places_newline_after_inline_block_comment() {
    let input = "/* 헤더 주석 */SELECT 1 FROM dual";
    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains("/* 헤더 주석 */\nSELECT 1\nFROM DUAL;"),
        "Inline block comment should be followed by newline before SQL, got: {}",
        formatted
    );
}

#[test]
fn format_sql_does_not_merge_end_statement_with_following_if() {
    let input = "BEGIN\nNULL;\nEND;\nIF 1 = 1 THEN\nNULL;\nEND IF;";
    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains("END;\n\nIF 1 = 1 THEN"),
        "END; and following IF must remain separate, got: {}",
        formatted
    );
}

#[test]
fn format_sql_preserves_newline_after_block_comment_end() {
    let input = "SELECT 1 /* trailing */\nFROM dual";
    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains("/* trailing */\nFROM DUAL;"),
        "newline after */ should be preserved, got: {}",
        formatted
    );
}

#[test]
fn format_sql_preserves_newline_before_line_comment() {
    let input = "SELECT 1\n-- comment\nFROM dual";
    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains("SELECT 1\n-- comment\nFROM DUAL;"),
        "newline before -- should be preserved, got: {}",
        formatted
    );
}

#[test]
fn format_sql_preserves_newline_before_block_comment() {
    let input = "SELECT 1\n/* comment */\nFROM dual";
    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains("SELECT 1\n/* comment */\nFROM DUAL;"),
        "newline before /* should be preserved, got: {}",
        formatted
    );
}

#[test]
fn format_sql_indents_select_list_item_starting_with_parenthesis() {
    let input = "SELECT (a + b) AS sum_value, c FROM dual";
    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains("SELECT\n    (a + b) AS sum_value,"),
        "Select list item starting with '(' should be indented under SELECT, got: {}",
        formatted
    );
}

#[test]
fn format_sql_indents_case_expression_inside_select_clause() {
    let input = "SELECT CASE WHEN a = 1 THEN 'Y' ELSE 'N' END AS flag FROM dual";
    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains("SELECT\n    CASE\n        WHEN a = 1 THEN 'Y'"),
        "CASE inside SELECT should start deeper than SELECT and WHEN should be deeper than CASE, got: {}",
        formatted
    );
}

#[test]
fn format_sql_case_when_does_not_insert_extra_blank_lines() {
    let input =
        "SELECT CASE WHEN a = 1 THEN 'A' WHEN a = 2 THEN 'B' ELSE 'C' END AS flag FROM dual";
    let formatted = SqlEditorWidget::format_sql_basic(input);

    let expected = [
        "SELECT",
        "    CASE",
        "        WHEN a = 1 THEN 'A'",
        "        WHEN a = 2 THEN 'B'",
        "        ELSE 'C'",
        "    END AS flag",
        "FROM DUAL;",
    ]
    .join("\n");

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_open_cursor_for_select_indentation() {
    let input = r#"BEGIN
OPEN p_rc
FOR
SELECT empno,
ename,
deptno,
salary
FROM oqt_emp
WHERE deptno = p_deptno
ORDER BY empno;
END;"#;

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = [
        "BEGIN",
        "    OPEN p_rc FOR",
        "        SELECT empno,",
        "            ename,",
        "            deptno,",
        "            salary",
        "        FROM oqt_emp",
        "        WHERE deptno = p_deptno",
        "        ORDER BY empno;",
        "END;",
    ]
    .join("\n");

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_open_cursor_for_with_clause() {
    let input = r#"BEGIN
OPEN p_rc
FOR
WITH cte AS (
    SELECT empno,
        deptno
    FROM oqt_t_emp
)
SELECT empno,
    deptno
FROM cte;
END;"#;

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let lines = formatted.lines().collect::<Vec<_>>();

    let open_idx = lines
        .iter()
        .position(|line| line.contains("OPEN P_RC FOR"))
        .or_else(|| lines.iter().position(|line| line.contains("OPEN p_rc FOR")))
        .expect("expected OPEN ... FOR line");

    let with_idx = lines
        .iter()
        .position(|line| {
            let upper = line.trim_start().to_ascii_uppercase();
            upper.starts_with("WITH CTE AS (") || upper.contains("OPEN P_RC FOR WITH CTE AS (")
        })
        .expect("expected WITH CTE line");

    let main_from_idx = lines
        .iter()
        .position(|line| {
            line.trim_start()
                .to_ascii_uppercase()
                .starts_with("FROM CTE")
        })
        .expect("expected main SELECT FROM line");

    let cte_from_idx = lines
        .iter()
        .position(|line| line.to_ascii_uppercase().contains("FROM OQT_T_EMP"))
        .expect("expected CTE body FROM line");

    let open_indent = lines[open_idx]
        .chars()
        .take_while(|c| c.is_whitespace())
        .count();
    let with_indent = lines[with_idx]
        .chars()
        .take_while(|c| c.is_whitespace())
        .count();
    let with_line = lines[with_idx].to_ascii_uppercase();
    let main_from_indent = lines[main_from_idx]
        .chars()
        .take_while(|c| c.is_whitespace())
        .count();

    if with_line.contains("OPEN P_RC FOR") {
        assert!(
            with_indent > open_indent,
            "OPEN ... FOR WITH should still indent WITH"
        );
    } else {
        assert_eq!(with_idx, open_idx + 1, "WITH should follow OPEN FOR");
    }

    assert!(with_line.trim_start().contains("WITH CTE AS ("));
    assert!(
        main_from_indent >= with_indent,
        "FROM CTE should align with/inside main SELECT depth"
    );
    assert_eq!(
        lines[cte_from_idx].trim_start().to_ascii_uppercase(),
        "FROM OQT_T_EMP"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.contains("OPEN p_rc FOR") || line.contains("OPEN P_RC FOR")),
        "OPEN ... FOR should remain"
    );
}

#[test]
fn format_sql_fetch_into_list_indentation() {
    let input = r#"BEGIN
FETCH c
INTO v_empno,
v_ename,
v_dept,
v_sal;
END;"#;

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = [
        "BEGIN",
        "    FETCH c",
        "    INTO v_empno,",
        "        v_ename,",
        "        v_dept,",
        "        v_sal;",
        "END;",
    ]
    .join("\n");

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_keeps_insert_into_together() {
    let input = "INSERT\nINTO oqt_call_log (id, tag, msg, n1)\nVALUES (1, 'T', 'M', 10)";
    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = [
        "INSERT INTO oqt_call_log (id, tag, msg, n1)",
        "VALUES (1, 'T', 'M', 10);",
    ]
    .join("\n");

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_select_and_into_exact_keyword_lines_keep_dml_indentation() {
    let input = r#"BEGIN
SELECT
CASE
WHEN 1 = 1 THEN 'Y'
ELSE 'N'
END
INTO
v_flag
FROM dual;
END;"#;

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = [
        "BEGIN",
        "    SELECT",
        "        CASE",
        "            WHEN 1 = 1 THEN 'Y'",
        "            ELSE 'N'",
        "        END",
        "    INTO v_flag",
        "    FROM DUAL;",
        "END;",
    ]
    .join("\n");

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_where_exists_and_not_exists_layout_regression() {
    let input = "SELECT * FROM asdf WHERE EXISTS (SELECT 1 FROM oqt_t_order_item oi WHERE oi.order_id = v.order_id AND oi.sku LIKE 'SKU-%') AND NOT EXISTS (SELECT 1 FROM oqt_t_order_item oi WHERE oi.order_id = v.order_id AND oi.qty <= 0);";

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = [
        "SELECT *",
        "FROM asdf",
        "WHERE EXISTS (",
        "        SELECT 1",
        "        FROM oqt_t_order_item oi",
        "        WHERE oi.order_id = v.order_id",
        "            AND oi.sku LIKE 'SKU-%'",
        "    )",
        "    AND NOT EXISTS (",
        "        SELECT 1",
        "        FROM oqt_t_order_item oi",
        "        WHERE oi.order_id = v.order_id",
        "            AND oi.qty <= 0",
        "    );",
    ]
    .join("\n");

    assert_eq!(formatted, expected);
}

#[test]
fn compute_edited_range_handles_insert_delete_and_replace() {
    assert_eq!(compute_edited_range(5, 3, 0, 20), Some((5, 8)));
    assert_eq!(compute_edited_range(5, 0, 4, 20), Some((5, 9)));
    assert_eq!(compute_edited_range(5, 2, 6, 20), Some((5, 11)));
}

#[test]
fn compute_edited_range_clamps_and_handles_invalid_pos() {
    assert_eq!(compute_edited_range(-1, 3, 0, 20), None);
    assert_eq!(compute_edited_range(50, 3, 0, 20), Some((20, 20)));
    assert_eq!(compute_edited_range(18, 10, 0, 20), Some((18, 20)));
}

#[test]
fn has_stateful_sql_delimiter_detects_comment_and_string_tokens() {
    assert!(has_stateful_sql_delimiter("/* comment"));
    assert!(has_stateful_sql_delimiter("end */"));
    assert!(has_stateful_sql_delimiter("-- line"));
    assert!(has_stateful_sql_delimiter("'text'"));
    assert!(has_stateful_sql_delimiter("q'[x]'"));
    assert!(has_stateful_sql_delimiter("NQ'[x]'"));
    assert!(!has_stateful_sql_delimiter("SELECT col FROM tab"));
}

#[test]
fn has_stateful_sql_delimiter_treats_partial_tokens_as_safe() {
    assert!(!has_stateful_sql_delimiter("SELECT / value"));
    assert!(!has_stateful_sql_delimiter("SELECT - value"));
    assert!(!has_stateful_sql_delimiter("SELECT * value"));
}

#[test]
fn has_stateful_sql_delimiter_detects_mixed_sequence_patterns() {
    assert!(has_stateful_sql_delimiter("a/*b"));
    assert!(has_stateful_sql_delimiter("a*/b"));
    assert!(has_stateful_sql_delimiter("a--b"));
}

#[test]
fn is_string_or_comment_style_matches_only_comment_or_string() {
    assert!(is_string_or_comment_style(STYLE_COMMENT));
    assert!(is_string_or_comment_style(STYLE_STRING));
    assert!(!is_string_or_comment_style(STYLE_DEFAULT));
    assert!(!is_string_or_comment_style(STYLE_KEYWORD));
}

#[test]
fn format_sql_uses_parser_depth_for_plsql_blocks() {
    let input = r#"BEGIN
IF 1 = 1 THEN
BEGIN
NULL;
END;
END IF;
END;"#;

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = [
        "BEGIN",
        "    IF 1 = 1 THEN",
        "        BEGIN",
        "            NULL;",
        "        END;",
        "    END IF;",
        "END;",
    ]
    .join("\n");

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_pre_dedents_else_elsif_exception_lines() {
    let input = r#"BEGIN
IF v_flag = 'Y' THEN
NULL;
ELSIF v_flag = 'N' THEN
NULL;
ELSE
NULL;
END IF;
EXCEPTION
WHEN OTHERS THEN
NULL;
END;"#;

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = [
        "BEGIN",
        "    IF v_flag = 'Y' THEN",
        "        NULL;",
        "    ELSIF v_flag = 'N' THEN",
        "        NULL;",
        "    ELSE",
        "        NULL;",
        "    END IF;",
        "EXCEPTION",
        "    WHEN OTHERS THEN",
        "        NULL;",
        "END;",
    ]
    .join("\n");

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_parser_depth_indents_if_and_case_one_level_more() {
    let input = r#"BEGIN
IF v_flag = 'Y' THEN
CASE
WHEN v_num = 1 THEN
NULL;
ELSE
NULL;
END CASE;
END IF;
END;"#;

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = [
        "BEGIN",
        "    IF v_flag = 'Y' THEN",
        "        CASE",
        "            WHEN v_num = 1 THEN",
        "                NULL;",
        "            ELSE",
        "                NULL;",
        "        END CASE;",
        "    END IF;",
        "END;",
    ]
    .join("\n");

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_case_branches_with_blank_lines() {
    let input = r#"BEGIN
CASE
WHEN p_n < 0 THEN
v := p_n * p_n;
WHEN p_n BETWEEN 0 AND 10 THEN
x := p_n + 100;
v := x - 50;
ELSE
v := p_n + 999;
END CASE;
END;"#;

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = [
        "BEGIN",
        "    CASE",
        "        WHEN p_n < 0 THEN",
        "            v := p_n * p_n;",
        "        WHEN p_n BETWEEN 0 AND 10 THEN",
        "            x := p_n + 100;",
        "            v := x - 50;",
        "        ELSE",
        "            v := p_n + 999;",
        "    END CASE;",
        "END;",
    ]
    .join("\n");

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_keeps_comments_together() {
    let input = r#"BEGIN
-- first
-- second
NULL;
END;"#;

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = [
        "BEGIN",
        "    -- first",
        "    -- second",
        "    NULL;",
        "END;",
    ]
    .join("\n");

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_does_not_insert_blank_line_between_line_comments() {
    let input = "-- first\n-- second\nSELECT 1 FROM dual;";

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = ["-- first", "-- second", "", "SELECT 1", "FROM DUAL;"].join("\n");

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_keeps_consecutive_sqlplus_comments_together() {
    let input = "REM first\nREMARK second\nSELECT 1 FROM dual;";

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = ["REM FIRST", "REMARK SECOND", "", "SELECT 1", "FROM DUAL;"].join("\n");

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_keeps_statement_boundary_when_semicolon_has_trailing_line_comment() {
    let input = "SELECT 1 FROM dual; -- trailing note\nSELECT 2 FROM dual;";
    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains("SELECT 1\nFROM DUAL;"),
        "first statement should remain independent, got: {formatted}"
    );
    assert!(
        formatted.contains("SELECT 2\nFROM DUAL;"),
        "second statement should remain independent, got: {formatted}"
    );
}

#[test]
fn format_sql_does_not_insert_blank_line_between_prompt_commands() {
    let input = "PROMPT one\nPROMPT two\nSELECT 1 FROM dual;";

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = ["PROMPT one", "PROMPT two", "", "SELECT 1", "FROM DUAL;"].join("\n");

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_keeps_clear_breaks_and_computes_on_separate_lines() {
    let input = "CLEAR BREAKS\nCLEAR COMPUTES;";

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = "CLEAR BREAKS\nCLEAR COMPUTES";

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_splits_combined_clear_breaks_and_computes() {
    let input = "CLEAR BREAKS CLEAR COMPUTES;";

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = "CLEAR BREAKS\nCLEAR COMPUTES";

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_indents_line_comments_to_depth() {
    let input = r#"BEGIN
IF 1 = 1 THEN
-- inside if
NULL;
END IF;
END;"#;

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = [
        "BEGIN",
        "    IF 1 = 1 THEN",
        "        -- inside if",
        "        NULL;",
        "    END IF;",
        "END;",
    ]
    .join("\n");

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_does_not_apply_depth_indent_to_block_comments() {
    let input = r#"BEGIN
IF 1 = 1 THEN
/* block comment
still block comment */
NULL;
END IF;
END;"#;

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = [
        "BEGIN",
        "    IF 1 = 1 THEN",
        "        /* block comment",
        "still block comment */",
        "        NULL;",
        "    END IF;",
        "END;",
    ]
    .join("\n");

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_select_case_inside_sum_is_indented() {
    let input = r#"SELECT grp,
COUNT (*) AS cnt,
SUM (
CASE
WHEN MOD (n, 2) = 0 THEN 1
ELSE 0
END) AS even_cnt,
SUM (
CASE
WHEN INSTR (txt, 'END;') > 0 THEN 1
ELSE 0
END) AS has_end_token_cnt
FROM oqt_t_test
GROUP BY grp
ORDER BY grp;"#;

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = [
        "SELECT grp,",
        "    COUNT (*) AS cnt,",
        "    SUM (",
        "        CASE",
        "            WHEN MOD (n, 2) = 0 THEN 1",
        "            ELSE 0",
        "        END",
        "    ) AS even_cnt,",
        "    SUM (",
        "        CASE",
        "            WHEN INSTR (txt, 'END;') > 0 THEN 1",
        "            ELSE 0",
        "        END",
        "    ) AS has_end_token_cnt",
        "FROM oqt_t_test",
        "GROUP BY grp",
        "ORDER BY grp;",
    ]
    .join("\n");

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_nested_case_expression_in_select_keeps_newlines() {
    let input = r#"SELECT
CASE
WHEN a = 1 THEN CASE WHEN b = 2 THEN 'X' ELSE 'Y' END
ELSE CASE WHEN c = 3 THEN 'Z' ELSE 'W' END
END AS result_value,
col2
FROM dual;"#;

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = [
        "SELECT",
        "    CASE",
        "        WHEN a = 1 THEN",
        "        CASE",
        "            WHEN b = 2 THEN 'X'",
        "            ELSE 'Y'",
        "        END",
        "        ELSE",
        "        CASE",
        "            WHEN c = 3 THEN 'Z'",
        "            ELSE 'W'",
        "        END",
        "    END AS result_value,",
        "    col2",
        "FROM DUAL;",
    ]
    .join("\n");

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_package_body_with_nested_case_keeps_block_newlines() {
    let input = "CREATE OR REPLACE PACKAGE BODY pkg_case AS PROCEDURE run_demo IS BEGIN CASE v_mode WHEN 1 THEN CASE WHEN v_flag = 'Y' THEN NULL; ELSE NULL; END CASE; ELSE NULL; END CASE; END run_demo; END pkg_case;";

    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains(
            "CASE v_mode
            WHEN 1 THEN
                CASE
                    WHEN v_flag = 'Y' THEN"
        ),
        "Nested CASE in package body should keep multi-line layout, got: {}",
        formatted
    );
    assert!(
        formatted.contains(
            "END CASE;
            ELSE
                NULL;
        END CASE;"
        ),
        "Outer CASE branches should remain separated by new lines, got: {}",
        formatted
    );
}

#[test]
fn format_sql_package_body_case_inside_parentheses_keeps_newlines() {
    let input = "CREATE OR REPLACE PACKAGE BODY pkg_case_paren AS PROCEDURE run_demo IS v_val NUMBER; BEGIN v_val := fn_calc((CASE WHEN v_mode = 1 THEN CASE WHEN v_flag = 'Y' THEN 100 ELSE 200 END ELSE 0 END)); END run_demo; END pkg_case_paren;";

    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains(
            "v_val := fn_calc ((\n            CASE\n                WHEN v_mode = 1 THEN"
        ),
        "CASE expression inside parentheses should still expand to multiline layout, got: {}",
        formatted
    );
    assert!(
        formatted.contains(
            "WHEN v_flag = 'Y' THEN\n                        100\n                        ELSE\n                        200\n                    END"
        ),
        "Nested CASE branches inside parenthesis should stay on separate lines, got: {}",
        formatted
    );
}

#[test]
fn format_sql_package_body_type_table_is_not_misdetected_as_create_table() {
    let input = "CREATE OR REPLACE PACKAGE BODY pkg_case_type AS TYPE num_tab IS TABLE OF NUMBER INDEX BY PLS_INTEGER; FUNCTION run_demo RETURN NUMBER IS v_out NUMBER; BEGIN v_out := fn_calc((CASE WHEN v_mode = 1 THEN CASE WHEN v_flag = 'Y' THEN 10 ELSE 20 END ELSE 0 END)); RETURN v_out; END run_demo; END pkg_case_type;";

    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.starts_with("CREATE OR REPLACE PACKAGE BODY"),
        "Package body should keep CREATE PACKAGE prefix, got: {}",
        formatted
    );
    assert!(
        formatted.contains("TYPE num_tab IS TABLE OF NUMBER INDEX BY PLS_INTEGER;"),
        "TYPE ... IS TABLE declaration should be preserved, got: {}",
        formatted
    );
    assert!(
        formatted.contains(
            "BEGIN
        v_out := fn_calc ((\n            CASE"
        ),
        "Nested CASE inside function body should remain multiline, got: {}",
        formatted
    );
}

#[test]
fn format_sql_package_body_type_table_with_nested_case_keeps_newlines() {
    let input = "CREATE OR REPLACE PACKAGE BODY pkg_case_type AS TYPE num_tab IS TABLE OF NUMBER INDEX BY PLS_INTEGER; FUNCTION run_demo RETURN NUMBER IS BEGIN CASE WHEN v_mode = 1 THEN CASE WHEN v_flag = 'Y' THEN 10 ELSE 20 END ELSE 0 END CASE; RETURN 1; END run_demo; END pkg_case_type;";

    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains(
            "TYPE num_tab IS TABLE OF NUMBER INDEX BY PLS_INTEGER;\n    FUNCTION run_demo RETURN NUMBER IS"
        ),
        "TYPE ... IS TABLE declaration should not collapse following routine block, got: {}",
        formatted
    );
    assert!(
        formatted.contains(
            "BEGIN
        CASE
            WHEN v_mode = 1 THEN
                CASE
                    WHEN v_flag = 'Y' THEN"
        ),
        "Nested CASE after TYPE ... IS TABLE should remain multiline, got: {}",
        formatted
    );
}
#[test]
fn format_sql_declare_begin_pre_dedent() {
    let input = r#"DECLARE
v_old_sal NUMBER;
BEGIN
NULL;
END;"#;

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = [
        "DECLARE",
        "    v_old_sal NUMBER;",
        "BEGIN",
        "    NULL;",
        "END;",
    ]
    .join("\n");

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_parser_depth_covers_loop_subquery_with_and_package_body() {
    let input = r#"CREATE OR REPLACE PACKAGE BODY pkg_demo AS
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

    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains("PACKAGE BODY pkg_demo AS\n    PROCEDURE run_demo IS"),
        "Package body scope should increase depth, got: {}",
        formatted
    );
    assert!(
        formatted.contains("PROCEDURE run_demo IS\n    BEGIN"),
        "Procedure BEGIN should align with procedure declaration, got: {}",
        formatted
    );
    assert!(
        formatted.contains("        FOR r IN (\n            SELECT id"),
        "Subquery SELECT should increase depth, got: {}",
        formatted
    );
    assert!(
        formatted.contains("        ) LOOP\n            NULL;\n        END LOOP;"),
        "LOOP body should be indented one level deeper, got: {}",
        formatted
    );
    assert!(
        formatted
            .contains("WITH cte AS (\n    SELECT 1 AS n\n    FROM DUAL\n)\nSELECT *\nFROM cte;"),
        "WITH CTE block should increase depth and restore on main SELECT, got: {}",
        formatted
    );
}

#[test]
fn format_sql_formats_multi_cte_join_subquery_depth_consistently() {
    let input = "WITH emp_base AS (SELECT e.empno, e.ename, e.deptno, e.sal, e.hiredate FROM emp e WHERE e.hiredate >= DATE '2010-01-01'), dept_agg AS (SELECT eb.deptno, COUNT(*) AS emp_cnt, AVG(eb.sal) AS avg_sal FROM emp_base eb GROUP BY eb.deptno) SELECT d.deptno, d.dname, d.loc, c.emp_cnt, c.avg_sal, (SELECT MAX(eb2.sal) FROM emp_base eb2 WHERE eb2.deptno = c.deptno) AS max_sal_in_dept FROM dept d JOIN dept_agg c ON c.deptno = d.deptno WHERE d.loc = 'SEOUL' AND c.emp_cnt > 3 ORDER BY c.avg_sal DESC;";

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = r#"WITH emp_base AS (
    SELECT
        e.empno,
        e.ename,
        e.deptno,
        e.sal,
        e.hiredate
    FROM emp e
    WHERE e.hiredate >= DATE '2010-01-01'
),
dept_agg AS (
    SELECT
        eb.deptno,
        COUNT (*) AS emp_cnt,
        AVG (eb.sal) AS avg_sal
    FROM emp_base eb
    GROUP BY eb.deptno
)
SELECT
    d.deptno,
    d.dname,
    d.loc,
    c.emp_cnt,
    c.avg_sal,
    (
        SELECT MAX (eb2.sal)
        FROM emp_base eb2
        WHERE eb2.deptno = c.deptno
    ) AS max_sal_in_dept
FROM dept d
JOIN dept_agg c
    ON c.deptno = d.deptno
WHERE d.loc = 'SEOUL'
    AND c.emp_cnt > 3
ORDER BY c.avg_sal DESC;"#;

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_formats_multi_cte_with_comments_and_scalar_subquery_exactly() {
    let input = "WITH e AS (SELECT empno, ename, job, mgr, hiredate, sal, comm, deptno FROM oqt_t_emp), d AS (SELECT deptno, dname, loc FROM oqt_t_dept), stats AS (SELECT deptno, COUNT(*) cnt, AVG(sal) avg_sal, SUM(NVL(comm, 0)) sum_comm FROM e GROUP BY deptno) SELECT d.deptno, d.dname, d.loc, s.cnt, ROUND(s.avg_sal, 2) AS avg_sal, s.sum_comm, -- scalar subquery (correlated)\n(SELECT MAX(e2.sal) FROM e e2 WHERE e2.deptno = d.deptno) AS max_sal_in_dept, -- case + analytic in select list via scalar subquery\nCASE WHEN s.cnt = 0 THEN 'EMPTY' WHEN s.avg_sal >= 2500 THEN 'HIGH' ELSE 'NORMAL' END AS dept_grade FROM d LEFT JOIN stats s ON s.deptno = d.deptno ORDER BY d.deptno;";

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = r#"WITH e AS (
    SELECT
        empno,
        ename,
        job,
        mgr,
        hiredate,
        sal,
        comm,
        deptno
    FROM oqt_t_emp
),
d AS (
    SELECT
        deptno,
        dname,
        loc
    FROM oqt_t_dept
),
stats AS (
    SELECT
        deptno,
        COUNT (*) cnt,
        AVG (sal) avg_sal,
        SUM (NVL (comm, 0)) sum_comm
    FROM e
    GROUP BY deptno
)
SELECT
    d.deptno,
    d.dname,
    d.loc,
    s.cnt,
    ROUND (s.avg_sal, 2) AS avg_sal,
    s.sum_comm,
    -- scalar subquery (correlated)
    (
        SELECT MAX (e2.sal)
        FROM e e2
        WHERE e2.deptno = d.deptno
    ) AS max_sal_in_dept,
    -- case + analytic in select list via scalar subquery
    CASE
        WHEN s.cnt = 0 THEN 'EMPTY'
        WHEN s.avg_sal >= 2500 THEN 'HIGH'
        ELSE 'NORMAL'
    END AS dept_grade
FROM d
LEFT JOIN stats s
    ON s.deptno = d.deptno
ORDER BY d.deptno;"#;

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_cte_comment_layout_is_idempotent() {
    let input = r#"WITH e AS (
    SELECT
        empno,
        ename,
        job,
        mgr,
        hiredate,
        sal,
        comm,
        deptno
    FROM oqt_t_emp
),
d AS (
    SELECT
        deptno,
        dname,
        loc
    FROM oqt_t_dept
),
stats AS (
    SELECT
        deptno,
        COUNT (*) cnt,
        AVG (sal) avg_sal,
        SUM (NVL (comm, 0)) sum_comm
    FROM e
    GROUP BY deptno
)
SELECT
    d.deptno,
    d.dname,
    d.loc,
    s.cnt,
    ROUND (s.avg_sal, 2) AS avg_sal,
    s.sum_comm,
    -- scalar subquery (correlated)
    (
        SELECT MAX (e2.sal)
        FROM e e2
        WHERE e2.deptno = d.deptno
    ) AS max_sal_in_dept,
    -- case + analytic in select list via scalar subquery
    CASE
        WHEN s.cnt = 0 THEN 'EMPTY'
        WHEN s.avg_sal >= 2500 THEN 'HIGH'
        ELSE 'NORMAL'
    END AS dept_grade
FROM d
LEFT JOIN stats s
    ON s.deptno = d.deptno
ORDER BY d.deptno;"#;

    let formatted = SqlEditorWidget::format_sql_basic(input);
    assert_eq!(formatted, input);
}

#[test]
fn format_sql_from_subqueries_with_comma_aligns_as_expected() {
    let input =
        "SELECT * FROM (SELECT * FROM help) a, (SELECT * FROM help) b WHERE a.TOPIC = b.TOPIC;";

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = r#"SELECT *
FROM (
        SELECT *
        FROM help
    ) a,
    (
        SELECT *
        FROM help
    ) b
WHERE a.TOPIC = b.TOPIC;"#;

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_filtered_cte_with_window_function_exact_layout() {
    let input = "filtered AS (SELECT * FROM enriched WHERE (sal > (SELECT AVG(sal) FROM oqt_t_emp WHERE deptno = enriched.deptno)) OR (job IN ('MANAGER', 'ANALYST') AND sal >= 2500)) SELECT f.deptno, f.dname, f.empno, f.ename, f.masked_name, f.job, f.sal, f.sal_band, -- window frame with last_value (needs careful frame)\nLAST_VALUE(f.sal) OVER (PARTITION BY f.deptno ORDER BY f.sal ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sal_via_last_value FROM filtered f ORDER BY f.deptno, f.sal DESC, f.empno;";

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = r#"filtered AS (
    SELECT *
    FROM enriched
    WHERE (sal > (
        SELECT AVG (sal)
        FROM oqt_t_emp
        WHERE deptno = enriched.deptno
    ))
        OR (job IN ('MANAGER', 'ANALYST')
            AND sal >= 2500)
)
SELECT f.deptno,
    f.dname,
    f.empno,
    f.ename,
    f.masked_name,
    f.job,
    f.sal,
    f.sal_band,
    -- window frame with last_value (needs careful frame)
    LAST_VALUE (f.sal) OVER (PARTITION BY f.deptno ORDER BY f.sal ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sal_via_last_value
FROM filtered f
ORDER BY f.deptno,
    f.sal DESC,
    f.empno;"#;

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_window_functions_and_listagg_exact_layout() {
    let input = "WITH base AS (SELECT e.empno, e.ename, e.deptno, e.sal, e.hiredate FROM oqt_t_emp e) SELECT b.*, RANK() OVER (PARTITION BY deptno ORDER BY sal DESC) AS rnk, DENSE_RANK() OVER (PARTITION BY deptno ORDER BY sal DESC) AS drnk, ROW_NUMBER() OVER (PARTITION BY deptno ORDER BY hiredate, empno) AS rn, SUM(sal) OVER (PARTITION BY deptno) AS sum_sal_dept, AVG(sal) OVER (PARTITION BY deptno) AS avg_sal_dept, PERCENT_RANK() OVER (PARTITION BY deptno ORDER BY sal) AS pct_rank, CUME_DIST() OVER (PARTITION BY deptno ORDER BY sal) AS CUME_DIST, -- running total with frame\nSUM(sal) OVER (PARTITION BY deptno ORDER BY hiredate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sal, -- windowed listagg\nLISTAGG(ename, ',') WITHIN GROUP (ORDER BY ename) OVER (PARTITION BY deptno) AS names_in_dept FROM base b ORDER BY deptno, rnk, empno;";

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = r#"WITH base AS (
    SELECT
        e.empno,
        e.ename,
        e.deptno,
        e.sal,
        e.hiredate
    FROM oqt_t_emp e
)
SELECT
    b.*,
    RANK () OVER (PARTITION BY deptno ORDER BY sal DESC) AS rnk,
    DENSE_RANK () OVER (PARTITION BY deptno ORDER BY sal DESC) AS drnk,
    ROW_NUMBER () OVER (PARTITION BY deptno ORDER BY hiredate, empno) AS rn,
    SUM (sal) OVER (PARTITION BY deptno) AS sum_sal_dept,
    AVG (sal) OVER (PARTITION BY deptno) AS avg_sal_dept,
    PERCENT_RANK () OVER (PARTITION BY deptno ORDER BY sal) AS pct_rank,
    CUME_DIST () OVER (PARTITION BY deptno ORDER BY sal) AS CUME_DIST,
    -- running total with frame
    SUM (sal) OVER (PARTITION BY deptno ORDER BY hiredate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sal,
    -- windowed listagg
    LISTAGG (ename, ',') WITHIN
    GROUP (ORDER BY ename) OVER (PARTITION BY deptno) AS names_in_dept
FROM base b
ORDER BY deptno,
    rnk,
    empno;"#;

    assert_eq!(formatted, expected);
}

#[test]
fn format_sql_resets_paren_comma_suppression_after_top_level_semicolon() {
    let input = "SELECT func(a, b;\nSELECT c, d FROM dual";
    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains("SELECT\n    c,\n    d\nFROM DUAL;"),
        "Comma wrapping should recover for next top-level statement after ';', got: {}",
        formatted
    );
}

#[test]
fn format_sql_recovers_when_slash_appears_in_comment() {
    let input = "SELECT 1 FROM dual;
-- /
SELECT a, b FROM dual;";
    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains("SELECT a,\n    b\nFROM DUAL;"),
        "Formatter should recover top-level statement splitting after comment slash, got: {}",
        formatted
    );
}
#[test]
fn format_sql_comment_parenthesis_does_not_affect_comma_newline() {
    let input = "SELECT a /* (comment) */, b FROM dual";
    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains("/* (comment) */,\n    b"),
        "Parenthesis inside comments must not keep comma on one line, got: {}",
        formatted
    );
}

#[test]
fn format_sql_cursor_loop_with_case_and_exit_when_keeps_loop_body_layout() {
    let input = r#"FOR r IN (
SELECT id,
grp,
n,
CASE
WHEN n < 0 THEN 'NEG'
WHEN n = 0 THEN 'ZERO'
WHEN MOD (n, 2) = 0 THEN 'EVEN'
ELSE 'ODD'
END AS kind
FROM oqt_t_test
WHERE grp = p_grp
ORDER BY id FETCH FIRST 6 ROWS ONLY
) LOOP v_depth := v_depth + 1;
log_msg ('RUN', 'loop', v_depth, 'id=' || r.id || ' n=' || r.n || ' kind=' || r.kind);
EXIT
WHEN v_depth > 10;
END LOOP;"#;

    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains(") LOOP\n    v_depth := v_depth + 1;"),
        "Loop body should start on a new line after LOOP, got: {}",
        formatted
    );
    assert!(
        formatted.contains("EXIT WHEN v_depth > 10;"),
        "EXIT WHEN should stay on one line, got: {}",
        formatted
    );
}

#[test]
fn format_sql_exit_when_with_label_stays_on_one_line() {
    let input = r#"FOR i IN 1..10 LOOP
EXIT outer_loop
WHEN i > 5;
END LOOP;"#;

    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains("EXIT outer_loop WHEN i > 5;"),
        "Labeled EXIT WHEN should stay on one line, got: {}",
        formatted
    );
}

#[test]
fn format_sql_package_loop_select_case_end_alignment_regression() {
    let input = r#"CREATE OR REPLACE PACKAGE BODY oqt_mega_pkg AS
    PROCEDURE run_torture (p_grp IN NUMBER, p_n IN NUMBER, p_txt IN VARCHAR2) IS
        PROCEDURE jumpy (p IN NUMBER) IS
            -- 커서 루프 + EXIT WHEN + CASE(expression)
            FOR r IN (
                SELECT id,
                    grp,
                    n,
                    CASE
                        WHEN n < 0 THEN 'NEG'
                        WHEN n = 0 THEN 'ZERO'
                        WHEN MOD (n, 2) = 0 THEN 'EVEN'
                        ELSE 'ODD'
                     END AS kind
                FROM oqt_t_test
                WHERE grp = p_grp
                ORDER BY id FETCH FIRST 6 ROWS ONLY
            ) LOOP
                v_depth := v_depth + 1;
                log_msg ('RUN', 'loop', v_depth, 'id=' || r.id || ' n=' || r.n || ' kind=' || r.kind);
                EXIT WHEN v_depth > 10;
            END LOOP;
        END run_torture;
    END oqt_mega_pkg;"#;

    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains("CASE\n                        WHEN n < 0 THEN 'NEG'"),
        "CASE/WHEN alignment in SELECT list is broken, got: {}",
        formatted
    );
    assert!(
        formatted.contains("ELSE 'ODD'\n                    END AS kind"),
        "CASE END should align with CASE in SELECT list, got: {}",
        formatted
    );
}

#[test]
fn format_sql_open_cursor_nested_case_expression_alignment_regression() {
    let input = r#"CREATE OR REPLACE PACKAGE BODY oqt_mega_pkg AS
    PROCEDURE open_rc (p_grp IN NUMBER, p_rc OUT SYS_REFCURSOR) IS
    BEGIN
        OPEN p_rc FOR
            SELECT t.id,
                t.grp,
                t.n,
                CASE
                WHEN t.grp = 0 THEN
                CASE
                    WHEN t.n > 10 THEN 'G0_BIG'
                    ELSE 'G0_SMALL'
                END
                WHEN t.grp IN (1, 2) THEN 'G12'
                ELSE 'GOTHER'
            END AS bucket,
                SUBSTR (t.txt, 1, 200) AS txt
            FROM oqt_t_test t
            WHERE t.grp = p_grp
            ORDER BY t.id;
    END open_rc;
END oqt_mega_pkg;"#;

    let formatted = SqlEditorWidget::format_sql_basic(input);

    assert!(
        formatted.contains("CASE\n                    WHEN t.grp = 0 THEN"),
        "Outer CASE/WHEN alignment in OPEN FOR SELECT is broken, got: {}",
        formatted
    );
    assert!(
        formatted.contains("WHEN t.grp IN (1, 2) THEN 'G12'\n                    ELSE 'GOTHER'"),
        "CASE branches should align in OPEN FOR SELECT, got: {}",
        formatted
    );
    assert!(
        formatted.contains("ELSE 'GOTHER'\n                END AS bucket,"),
        "CASE END should align with CASE in OPEN FOR SELECT, got: {}",
        formatted
    );
}

#[test]
fn format_sql_case_end_parenthesis_breaks_line_in_plsql_expression() {
    let input = r#"BEGIN
FOR i IN 1..3 LOOP
v_sum := v_sum + (
CASE
WHEN MOD (i, 2) = 0 THEN
i * 10
ELSE
i
END);
END LOOP;
END;"#;

    let formatted = SqlEditorWidget::format_sql_basic(input);
    assert!(
        formatted.contains(
            "v_sum := v_sum + (\n            CASE\n                WHEN MOD (i, 2) = 0 THEN"
        ),
        "CASE block inside parenthesized expression should be one depth deeper, got: {}",
        formatted
    );
    assert!(
        formatted.contains("END\n            );"),
        "CASE END and closing parenthesis should be split across lines, got: {}",
        formatted
    );
    assert!(
        !formatted.contains("END);"),
        "END); should not stay on one line in this pattern, got: {}",
        formatted
    );
}

#[test]
fn format_sql_trigger_if_elsif_alignment_matches_expected() {
    let input = r#"CREATE OR REPLACE NONEDITIONABLE TRIGGER "SYSTEM"."OQT_TRG_CHILD_BIU"
BEFORE
    INSERT OR UPDATE ON oqt_t_child
FOR EACH ROW
BEGIN
    IF INSERTING THEN
        :NEW.updated_at := NULL;
        IF :NEW.note IS
                NULL THEN
                :NEW.note := 'auto-note:' || :NEW.sku;
        END IF;
    ELSIF UPDATING THEN
            :NEW.updated_at := SYSDATE;
    END IF;
END;"#;

    let formatted = SqlEditorWidget::format_sql_basic(input);
    let expected = r#"CREATE OR REPLACE NONEDITIONABLE TRIGGER "SYSTEM"."OQT_TRG_CHILD_BIU"
    BEFORE INSERT OR UPDATE ON oqt_t_child
    FOR EACH ROW
BEGIN
    IF INSERTING THEN
        :NEW.updated_at := NULL;
        IF :NEW.note IS
                NULL THEN
                :NEW.note := 'auto-note:' || :NEW.sku;
        END IF;
    ELSIF UPDATING THEN
            :NEW.updated_at := SYSDATE;
    END IF;
END;"#;

    assert_eq!(formatted, expected);
}

#[test]
fn finalize_execution_state_clears_running_and_cancel_flags() {
    let query_running = Arc::new(Mutex::new(true));
    let cancel_flag = Arc::new(Mutex::new(true));

    SqlEditorWidget::finalize_execution_state(&query_running, &cancel_flag);

    assert!(!load_mutex_bool(&query_running));
    assert!(!load_mutex_bool(&cancel_flag));
}

// ── q-quote after identifier: tokenizer regression ──

#[test]
fn tokenize_sql_identifier_ending_q_not_treated_as_q_quote() {
    // `seq` is one identifier; the following `'text'` is a regular string.
    let tokens = SqlEditorWidget::tokenize_sql("SELECT seq'text' FROM dual");
    let has_word_seq = tokens
        .iter()
        .any(|t| matches!(t, SqlToken::Word(w) if w == "seq"));
    assert!(
        has_word_seq,
        "Identifier 'seq' should be a single Word token, got: {:?}",
        tokens
    );
    let has_q_quote_string = tokens
        .iter()
        .any(|t| matches!(t, SqlToken::String(s) if s.starts_with("q'")));
    assert!(
        !has_q_quote_string,
        "Should NOT produce a q-quote String token when q is part of an identifier, got: {:?}",
        tokens
    );
}

#[test]
fn tokenize_sql_identifier_ending_nq_not_treated_as_nq_quote() {
    // `unq` is one identifier; the following `'val'` is a regular string.
    let tokens = SqlEditorWidget::tokenize_sql("SELECT unq'val' FROM dual");
    let has_word_unq = tokens
        .iter()
        .any(|t| matches!(t, SqlToken::Word(w) if w == "unq"));
    assert!(
        has_word_unq,
        "Identifier 'unq' should be a single Word token, got: {:?}",
        tokens
    );
}

#[test]
fn tokenize_sql_standalone_q_quote_still_works() {
    // Standalone q'[...]' must still be recognized.
    let tokens = SqlEditorWidget::tokenize_sql("SELECT q'[hello]' FROM dual");
    let has_q_string = tokens
        .iter()
        .any(|t| matches!(t, SqlToken::String(s) if s.starts_with("q'")));
    assert!(
        has_q_string,
        "Standalone q-quote should produce a String token, got: {:?}",
        tokens
    );
}

#[test]
fn tokenize_sql_standalone_nq_quote_still_works() {
    let tokens = SqlEditorWidget::tokenize_sql("SELECT nq'[test]' FROM dual");
    let has_nq_string = tokens
        .iter()
        .any(|t| matches!(t, SqlToken::String(s) if s.starts_with("nq'")));
    assert!(
        has_nq_string,
        "Standalone nq-quote should produce a String token, got: {:?}",
        tokens
    );
}
