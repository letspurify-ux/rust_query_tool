use super::*;

fn windowed_range_for_test(text: &str, cursor_pos: usize) -> (usize, usize) {
    let start_candidate = cursor_pos.saturating_sub(HIGHLIGHT_WINDOW_RADIUS);
    let end_candidate = (cursor_pos + HIGHLIGHT_WINDOW_RADIUS).min(text.len());

    let start = match text.get(..start_candidate).and_then(|s| s.rfind('\n')) {
        Some(pos) => pos + 1,
        None => 0,
    };
    let end = match text.get(end_candidate..).and_then(|s| s.find('\n')) {
        Some(pos) => end_candidate + pos,
        None => text.len(),
    };

    (start, end)
}

fn generate_styles_windowed_for_test(
    highlighter: &SqlHighlighter,
    text: &str,
    cursor_pos: usize,
) -> String {
    if text.len() <= HIGHLIGHT_WINDOW_THRESHOLD {
        return highlighter.generate_styles(text);
    }

    let cursor_pos = cursor_pos.min(text.len());
    let (range_start, range_end) = windowed_range_for_test(text, cursor_pos);
    let window_text = &text[range_start..range_end];
    let window_styles = highlighter.generate_styles(window_text);
    let mut styles: Vec<char> = vec![STYLE_DEFAULT; text.len()];
    for (offset, style_char) in window_styles.chars().enumerate() {
        styles[range_start + offset] = style_char;
    }
    styles.into_iter().collect()
}

#[test]
fn test_number_highlighting_supports_single_decimal_point() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT 12.34 FROM dual";
    let styles = highlighter.generate_styles(text);

    let start = text.find("12.34").unwrap_or(0);
    let end = start + "12.34".len();
    assert!(styles[start..end].chars().all(|c| c == STYLE_NUMBER));
}

#[test]
fn test_number_highlighting_stops_before_second_decimal_point() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT 12.34.56 FROM dual";
    let styles = highlighter.generate_styles(text);

    let first_number_start = text.find("12.34").unwrap_or(0);
    let first_number_end = first_number_start + "12.34".len();
    assert!(styles[first_number_start..first_number_end]
        .chars()
        .all(|c| c == STYLE_NUMBER));

    let second_dot_pos = first_number_end;
    let second_number_end = second_dot_pos + ".56".len();
    assert!(styles[second_dot_pos..second_number_end]
        .chars()
        .all(|c| c == STYLE_NUMBER));
}

#[test]
fn test_keyword_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT * FROM";
    let styles = highlighter.generate_styles(text);

    // "SELECT" should be keyword (B)
    assert!(styles.starts_with("BBBBBB"));
}

#[test]
fn test_string_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "'hello world'";
    let styles = highlighter.generate_styles(text);

    // Entire string should be string style (D)
    assert!(styles.chars().all(|c| c == STYLE_STRING));
}

#[test]
fn test_comment_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "-- this is a comment";
    let styles = highlighter.generate_styles(text);

    // Entire line should be comment style (E)
    assert!(styles.chars().all(|c| c == STYLE_COMMENT));
}

#[test]
fn test_prompt_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "PROMPT Enter value for id";
    let styles = highlighter.generate_styles(text);

    assert!(styles.chars().all(|c| c == STYLE_COMMENT));
}

#[test]
fn test_prompt_highlighting_with_leading_whitespace() {
    let highlighter = SqlHighlighter::new();
    let text = "  prompt Enter value\nSELECT * FROM dual";
    let styles = highlighter.generate_styles(text);

    let first_line_end = text.find('\n').unwrap();
    assert!(styles[..first_line_end].chars().all(|c| c == STYLE_COMMENT));
    assert!(styles[first_line_end + 1..]
        .chars()
        .any(|c| c != STYLE_COMMENT));
}

#[test]
fn test_connect_line_disables_rest_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "CONNECT system/password@localhost:1521/FREE\nSELECT * FROM dual";
    let styles = highlighter.generate_styles(text);

    let first_line_end = text.find('\n').unwrap();
    assert!(styles[0..7].chars().all(|c| c == STYLE_KEYWORD));
    assert!(styles[7..first_line_end]
        .chars()
        .all(|c| c == STYLE_DEFAULT));

    let select_start = text.find("SELECT").unwrap();
    assert!(styles[select_start..select_start + 6]
        .chars()
        .all(|c| c == STYLE_KEYWORD));
}

#[test]
fn test_connect_line_with_leading_whitespace_disables_rest_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "  connect system/password@localhost:1521/FREE";
    let styles = highlighter.generate_styles(text);

    assert!(styles[..2].chars().all(|c| c == STYLE_DEFAULT));
    assert!(styles[2..9].chars().all(|c| c == STYLE_KEYWORD));
    assert!(styles[9..].chars().all(|c| c == STYLE_DEFAULT));
}

#[test]
fn test_connect_by_prior_keywords_are_highlighted() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT node_id FROM oqt_t_tree CONNECT BY PRIOR node_id = parent_id";
    let styles = highlighter.generate_styles(text);

    for keyword in ["CONNECT", "BY", "PRIOR"] {
        let start = text
            .find(keyword)
            .unwrap_or_else(|| panic!("missing keyword: {keyword}"));
        let end = start + keyword.len();
        assert!(
            styles[start..end].chars().all(|c| c == STYLE_KEYWORD),
            "{keyword} should be highlighted as keyword"
        );
    }
}

#[test]
fn test_connect_with_comment_then_by_is_not_sqlplus_connect() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT level FROM dual CONNECT /*comment*/ BY level <= 2";
    let styles = highlighter.generate_styles(text);

    for keyword in ["CONNECT", "BY"] {
        let start = text
            .find(keyword)
            .unwrap_or_else(|| panic!("missing keyword: {keyword}"));
        let end = start + keyword.len();
        assert!(
            styles[start..end].chars().all(|c| c == STYLE_KEYWORD),
            "{keyword} should be highlighted as keyword"
        );
    }
}

#[test]
fn test_parse_connect_continuation_detects_by_with_comment() {
    let bytes = b"CONNECT /*+ hint */ BY PRIOR id = parent_id";
    let connect_end = "CONNECT".len();
    assert_eq!(
        parse_connect_continuation(bytes, connect_end),
        ConnectContinuation::ByClause
    );
}

#[test]
fn test_parse_connect_continuation_detects_sqlplus_connect_line() {
    let bytes = b"CONNECT system/password@db";
    let connect_end = "CONNECT".len();
    assert_eq!(
        parse_connect_continuation(bytes, connect_end),
        ConnectContinuation::Other
    );
}

#[test]
fn test_prompt_keyword_with_large_start_is_safe() {
    assert!(!is_prompt_keyword(b"PROMPT", usize::MAX));
}

#[test]
fn test_connect_keyword_with_large_start_is_safe() {
    assert!(!is_connect_keyword(b"CONNECT", usize::MAX));
}

#[test]
fn test_sqlplus_break_compute_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "BREAK ON deptno\nCOMPUTE SUM\nCOLUMN col NEW_VALUE var";
    let styles = highlighter.generate_styles(text);

    let break_pos = text.find("BREAK").unwrap();
    assert!(styles[break_pos..break_pos + 5]
        .chars()
        .all(|c| c == STYLE_KEYWORD));

    let compute_pos = text.find("COMPUTE").unwrap();
    assert!(styles[compute_pos..compute_pos + 7]
        .chars()
        .all(|c| c == STYLE_KEYWORD));

    let new_value_pos = text.find("NEW_VALUE").unwrap();
    assert!(styles[new_value_pos..new_value_pos + 9]
        .chars()
        .all(|c| c == STYLE_KEYWORD));
}

#[test]
fn test_sqlplus_set_spool_keywords_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "SET TRIMSPOOL ON\nSET COLSEP ||\nSET NULL (null)\nSPOOL APPEND\nSPOOL OFF";
    let styles = highlighter.generate_styles(text);

    let trimspool_pos = text.find("TRIMSPOOL").unwrap();
    assert!(styles[trimspool_pos..trimspool_pos + 9]
        .chars()
        .all(|c| c == STYLE_KEYWORD));

    let colsep_pos = text.find("COLSEP").unwrap();
    assert!(styles[colsep_pos..colsep_pos + 6]
        .chars()
        .all(|c| c == STYLE_KEYWORD));

    let spool_append_pos = text.find("SPOOL APPEND").unwrap();
    assert!(styles[spool_append_pos..spool_append_pos + 5]
        .chars()
        .all(|c| c == STYLE_KEYWORD));
    assert!(styles[spool_append_pos + 6..spool_append_pos + 12]
        .chars()
        .all(|c| c == STYLE_KEYWORD));

    let spool_off_pos = text.rfind("SPOOL OFF").unwrap();
    assert!(styles[spool_off_pos..spool_off_pos + 5]
        .chars()
        .all(|c| c == STYLE_KEYWORD));
    assert!(styles[spool_off_pos + 6..spool_off_pos + 9]
        .chars()
        .all(|c| c == STYLE_KEYWORD));
}

#[test]
fn test_alter_session_keywords_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "ALTER SESSION SET CURRENT_SCHEMA = app_user";
    let styles = highlighter.generate_styles(text);

    for keyword in ["ALTER", "SESSION", "SET", "CURRENT_SCHEMA"] {
        let start = text.find(keyword).unwrap();
        let end = start + keyword.len();
        assert!(
            styles[start..end].chars().all(|c| c == STYLE_KEYWORD),
            "{} should be highlighted as keyword",
            keyword
        );
    }
}

#[test]
fn test_minus_keyword_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT 1 FROM dual MINUS SELECT 2 FROM dual";
    let styles = highlighter.generate_styles(text);

    let minus_pos = text.find("MINUS").unwrap();
    assert!(
        styles[minus_pos..minus_pos + 5]
            .chars()
            .all(|c| c == STYLE_KEYWORD),
        "MINUS should be highlighted as keyword"
    );
}

#[test]
fn test_match_recognize_keywords_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT * FROM oqt_t_emp MATCH_RECOGNIZE (ONE ROW PER MATCH PATTERN (a b+))";
    let styles = highlighter.generate_styles(text);

    for keyword in ["MATCH_RECOGNIZE", "ONE", "PER", "MATCH", "PATTERN"] {
        let start = text
            .find(keyword)
            .unwrap_or_else(|| panic!("missing keyword: {keyword}"));
        let end = start + keyword.len();
        assert!(
            styles[start..end].chars().all(|c| c == STYLE_KEYWORD),
            "{keyword} should be highlighted as keyword"
        );
    }
}

#[test]
fn test_path_in_recursive_cte_column_list_is_not_keyword() {
    let highlighter = SqlHighlighter::new();
    let text = "WITH r (node_id, parent_id, node_name, lvl, path) AS (\n\
  SELECT node_id, parent_id, node_name, 1 AS lvl, '/'||node_name AS path\n\
  FROM oqt_t_tree\n\
)\n\
SELECT r.path FROM r";
    let styles = highlighter.generate_styles(text);

    let cte_path_start = text.find("path) AS").unwrap();
    assert!(
        styles[cte_path_start..cte_path_start + 4]
            .chars()
            .all(|c| c == STYLE_DEFAULT),
        "CTE explicit column name `path` should not be keyword"
    );

    let alias_path_start = text.find("AS path").unwrap() + 3;
    assert!(
        styles[alias_path_start..alias_path_start + 4]
            .chars()
            .all(|c| c == STYLE_DEFAULT),
        "SELECT alias `path` should not be keyword"
    );

    let qualified_path_start = text.find("r.path").unwrap() + 2;
    assert!(
        styles[qualified_path_start..qualified_path_start + 4]
            .chars()
            .all(|c| c == STYLE_DEFAULT),
        "qualified column `r.path` should not be keyword"
    );
}

#[test]
fn test_path_keyword_in_xmltable_remains_keyword() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT * FROM XMLTABLE('/x' PASSING payload COLUMNS id NUMBER PATH '$.id') t";
    let styles = highlighter.generate_styles(text);

    let path_start = text.find("PATH").unwrap();
    assert!(
        styles[path_start..path_start + 4]
            .chars()
            .all(|c| c == STYLE_KEYWORD),
        "XMLTABLE PATH clause should stay keyword style"
    );
}

#[test]
fn test_windowed_highlighting_limits_scope() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT col FROM table;\n".repeat(2000);
    assert!(text.len() > HIGHLIGHT_WINDOW_THRESHOLD);
    let cursor_pos = text.len() / 2;
    let styles = generate_styles_windowed_for_test(&highlighter, &text, cursor_pos);

    assert_eq!(styles.len(), text.len());

    let (range_start, range_end) = windowed_range_for_test(&text, cursor_pos);
    assert!(range_start > 0);
    assert!(range_end <= text.len());

    let outside_select_pos = text.find("SELECT").unwrap();
    if outside_select_pos + 6 < range_start {
        assert!(styles[outside_select_pos..outside_select_pos + 6]
            .chars()
            .all(|c| c == STYLE_DEFAULT));
    }

    let inside_select_pos = text[range_start..range_end]
        .find("SELECT")
        .map(|pos| range_start + pos)
        .unwrap();
    assert!(styles[inside_select_pos..inside_select_pos + 6]
        .chars()
        .all(|c| c == STYLE_KEYWORD));
}

#[test]
fn test_q_quote_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT q'[test string]' FROM dual";
    let styles = highlighter.generate_styles(text);

    // "SELECT" (0-5) should be keyword (B)
    assert!(
        styles[0..6].chars().all(|c| c == STYLE_KEYWORD),
        "SELECT should be keyword, got: {}",
        &styles[0..6]
    );

    // "q'[test string]'" (7-22) should be string (D)
    // Find the position of q'[
    let q_start = text.find("q'[").unwrap();
    let q_end = text.find("]'").unwrap() + 2;
    assert!(
        styles[q_start..q_end].chars().all(|c| c == STYLE_STRING),
        "q'[...]' should be string style, got: {}",
        &styles[q_start..q_end]
    );
}

#[test]
fn test_nq_quote_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT nq'[national string]' FROM dual";
    let styles = highlighter.generate_styles(text);

    // "SELECT" should be keyword (B)
    assert!(
        styles[0..6].chars().all(|c| c == STYLE_KEYWORD),
        "SELECT should be keyword"
    );

    // "nq'[national string]'" should be string (D)
    let nq_start = text.find("nq'[").unwrap();
    let nq_end = text.find("]'").unwrap() + 2;
    assert!(
        styles[nq_start..nq_end].chars().all(|c| c == STYLE_STRING),
        "nq'[...]' should be string style, got: {}",
        &styles[nq_start..nq_end]
    );
}

#[test]
fn test_nq_quote_case_insensitive_highlighting() {
    let highlighter = SqlHighlighter::new();

    // Test NQ (uppercase)
    let text1 = "SELECT NQ'[test]' FROM dual";
    let styles1 = highlighter.generate_styles(text1);
    let nq_start1 = text1.find("NQ'[").unwrap();
    let nq_end1 = text1.find("]'").unwrap() + 2;
    assert!(
        styles1[nq_start1..nq_end1]
            .chars()
            .all(|c| c == STYLE_STRING),
        "NQ'[...]' should be string style"
    );

    // Test Nq (mixed case)
    let text2 = "SELECT Nq'[test]' FROM dual";
    let styles2 = highlighter.generate_styles(text2);
    let nq_start2 = text2.find("Nq'[").unwrap();
    let nq_end2 = text2.find("]'").unwrap() + 2;
    assert!(
        styles2[nq_start2..nq_end2]
            .chars()
            .all(|c| c == STYLE_STRING),
        "Nq'[...]' should be string style"
    );
}

#[test]
fn test_uq_quote_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT uq'[unicode string]' FROM dual";
    let styles = highlighter.generate_styles(text);

    let uq_start = text.find("uq'[").unwrap();
    let uq_end = text.find("]'").unwrap() + 2;
    assert!(
        styles[uq_start..uq_end].chars().all(|c| c == STYLE_STRING),
        "uq'[...]' should be string style, got: {}",
        &styles[uq_start..uq_end]
    );
}

#[test]
fn test_unicode_q_quote_delimiter_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT q'가한글가' AS txt FROM dual";
    let styles = highlighter.generate_styles(text);

    let q_start = text.find("q'가").unwrap();
    let q_end = text.find("가'").unwrap() + "가'".len();
    assert!(
        styles[q_start..q_end].chars().all(|c| c == STYLE_STRING),
        "unicode q-quote should remain one string span, got: {}",
        &styles[q_start..q_end]
    );
}

#[test]
fn test_unicode_uq_quote_delimiter_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT uq'가문자열가' AS txt FROM dual";
    let styles = highlighter.generate_styles(text);

    let uq_start = text.find("uq'가").unwrap();
    let uq_end = text.find("가'").unwrap() + "가'".len();
    assert!(
        styles[uq_start..uq_end].chars().all(|c| c == STYLE_STRING),
        "unicode uq-quote should remain one string span, got: {}",
        &styles[uq_start..uq_end]
    );
}

#[test]
fn test_prefixed_single_quote_literals_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT n'가', b'0101', x'FF', u'유니코드', u&'\\0041' FROM dual";
    let styles = highlighter.generate_styles(text);

    for literal in ["n'가'", "b'0101'", "x'FF'", "u'유니코드'", "u&'\\0041'"] {
        let start = text.find(literal).unwrap();
        let end = start + literal.len();
        assert!(
            styles[start..end].chars().all(|c| c == STYLE_STRING),
            "prefixed literal should be a single string span: {literal}"
        );
    }
}

#[test]
fn test_q_quote_different_delimiters() {
    let highlighter = SqlHighlighter::new();

    // Test q'(...)'
    let text1 = "SELECT q'(parentheses)' FROM dual";
    let styles1 = highlighter.generate_styles(text1);
    let q_start1 = text1.find("q'(").unwrap();
    let q_end1 = text1.find(")'").unwrap() + 2;
    assert!(
        styles1[q_start1..q_end1].chars().all(|c| c == STYLE_STRING),
        "q'(...)' should be string style"
    );

    // Test q'{...}'
    let text2 = "SELECT q'{braces}' FROM dual";
    let styles2 = highlighter.generate_styles(text2);
    let q_start2 = text2.find("q'{").unwrap();
    let q_end2 = text2.find("}'").unwrap() + 2;
    assert!(
        styles2[q_start2..q_end2].chars().all(|c| c == STYLE_STRING),
        "q'{{...}}' should be string style"
    );

    // Test q'<...>'
    let text3 = "SELECT q'<angle>' FROM dual";
    let styles3 = highlighter.generate_styles(text3);
    let q_start3 = text3.find("q'<").unwrap();
    let q_end3 = text3.find(">'").unwrap() + 2;
    assert!(
        styles3[q_start3..q_end3].chars().all(|c| c == STYLE_STRING),
        "q'<...>' should be string style"
    );
}

#[test]
fn test_q_quote_with_embedded_quotes() {
    let highlighter = SqlHighlighter::new();
    // q-quoted strings can contain single quotes without escaping
    let text = "SELECT q'[It's a test]' FROM dual";
    let styles = highlighter.generate_styles(text);

    let q_start = text.find("q'[").unwrap();
    let q_end = text.find("]'").unwrap() + 2;
    assert!(
        styles[q_start..q_end].chars().all(|c| c == STYLE_STRING),
        "q'[...]' with embedded quote should be string style"
    );
}

#[test]
fn test_hint_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT /*+ FULL(t) */ * FROM table t";
    let styles = highlighter.generate_styles(text);

    // Find the hint position
    let hint_start = text.find("/*+").unwrap();
    let hint_end = text.find("*/").unwrap() + 2;

    assert!(
        styles[hint_start..hint_end]
            .chars()
            .all(|c| c == STYLE_HINT),
        "Hint /*+ ... */ should be styled as hint, got: {}",
        &styles[hint_start..hint_end]
    );
}

#[test]
fn test_hint_vs_regular_comment() {
    let highlighter = SqlHighlighter::new();

    // Regular comment should be comment style
    let text1 = "SELECT /* comment */ * FROM dual";
    let styles1 = highlighter.generate_styles(text1);
    let comment_start = text1.find("/*").unwrap();
    let comment_end = text1.find("*/").unwrap() + 2;
    assert!(
        styles1[comment_start..comment_end]
            .chars()
            .all(|c| c == STYLE_COMMENT),
        "Regular comment should be comment style"
    );

    // Hint should be hint style
    let text2 = "SELECT /*+ INDEX(t) */ * FROM dual";
    let styles2 = highlighter.generate_styles(text2);
    let hint_start = text2.find("/*+").unwrap();
    let hint_end = text2.find("*/").unwrap() + 2;
    assert!(
        styles2[hint_start..hint_end]
            .chars()
            .all(|c| c == STYLE_HINT),
        "Hint should be hint style"
    );
}

#[test]
fn test_complex_hint_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT /*+ PARALLEL(t,4) FULL(t) INDEX(x idx_name) */ * FROM table t";
    let styles = highlighter.generate_styles(text);

    let hint_start = text.find("/*+").unwrap();
    let hint_end = text.find("*/").unwrap() + 2;
    assert!(
        styles[hint_start..hint_end]
            .chars()
            .all(|c| c == STYLE_HINT),
        "Complex hint should be fully styled as hint"
    );
}

#[test]
fn test_date_literal_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT DATE '2024-01-01' FROM dual";
    let styles = highlighter.generate_styles(text);

    // Find DATE literal position
    let date_start = text.find("DATE").unwrap();
    let date_end = text.find("'2024-01-01'").unwrap() + "'2024-01-01'".len();

    assert!(
        styles[date_start..date_end]
            .chars()
            .all(|c| c == STYLE_DATETIME_LITERAL),
        "DATE literal should be styled as datetime literal, got: {}",
        &styles[date_start..date_end]
    );
}

#[test]
fn test_timestamp_literal_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT TIMESTAMP '2024-01-01 12:30:00' FROM dual";
    let styles = highlighter.generate_styles(text);

    let ts_start = text.find("TIMESTAMP").unwrap();
    let ts_end = text.find("'2024-01-01 12:30:00'").unwrap() + "'2024-01-01 12:30:00'".len();

    assert!(
        styles[ts_start..ts_end]
            .chars()
            .all(|c| c == STYLE_DATETIME_LITERAL),
        "TIMESTAMP literal should be styled as datetime literal"
    );
}

#[test]
fn test_interval_literal_highlighting() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT INTERVAL '5' DAY FROM dual";
    let styles = highlighter.generate_styles(text);

    let int_start = text.find("INTERVAL").unwrap();
    let int_end = text.find("'5'").unwrap() + "'5'".len();

    assert!(
        styles[int_start..int_end]
            .chars()
            .all(|c| c == STYLE_DATETIME_LITERAL),
        "INTERVAL literal should be styled as datetime literal"
    );
}

#[test]
fn test_date_keyword_without_literal() {
    let highlighter = SqlHighlighter::new();
    // DATE as column name or keyword should be keyword style
    let text = "SELECT hire_date FROM employees";
    let styles = highlighter.generate_styles(text);

    // "date" in "hire_date" should not be specially styled
    // The whole identifier should be default
    let hire_date_start = text.find("hire_date").unwrap();
    let hire_date_end = hire_date_start + "hire_date".len();
    // hire_date is not a keyword or function, should be default
    assert!(
        styles[hire_date_start..hire_date_end]
            .chars()
            .all(|c| c == STYLE_DEFAULT),
        "hire_date should be default style"
    );
}

#[test]
fn test_lowercase_date_literal() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT date '2024-01-01' FROM dual";
    let styles = highlighter.generate_styles(text);

    let date_start = text.find("date").unwrap();
    let date_end = text.find("'2024-01-01'").unwrap() + "'2024-01-01'".len();

    assert!(
        styles[date_start..date_end]
            .chars()
            .all(|c| c == STYLE_DATETIME_LITERAL),
        "Lowercase date literal should be styled as datetime literal"
    );
}

#[test]
fn test_quoted_identifier_does_not_trigger_keyword_or_comment() {
    let highlighter = SqlHighlighter::new();
    let text = r#"SELECT "FROM" AS "A--B" FROM dual"#;
    let styles = highlighter.generate_styles(text);

    let from_ident_start = text.find(r#""FROM""#).unwrap();
    let from_ident_end = from_ident_start + r#""FROM""#.len();
    assert!(
        styles[from_ident_start..from_ident_end]
            .chars()
            .all(|c| c == STYLE_IDENTIFIER),
        "quoted identifier should be identifier style"
    );

    let comment_like_start = text.find(r#""A--B""#).unwrap();
    let comment_like_end = comment_like_start + r#""A--B""#.len();
    assert!(
        styles[comment_like_start..comment_like_end]
            .chars()
            .all(|c| c == STYLE_IDENTIFIER),
        "double dash inside quoted identifier must not start comment"
    );
}

#[test]
fn test_quoted_identifier_with_escaped_quote_is_identifier_style() {
    let highlighter = SqlHighlighter::new();
    let text = r#"SELECT "A""B" FROM dual"#;
    let styles = highlighter.generate_styles(text);

    let quoted_start = text.find(r#""A""B""#).unwrap();
    let quoted_end = quoted_start + r#""A""B""#.len();
    assert!(
        styles[quoted_start..quoted_end]
            .chars()
            .all(|c| c == STYLE_IDENTIFIER),
        "escaped quote in quoted identifier should remain identifier style"
    );
}

#[test]
fn test_prioritize_ranges_keeps_focus_window_when_truncating() {
    let ranges = vec![
        (0, 100),
        (200, 300),
        (400, 500),
        (600, 700),
        (800, 900),
        (1000, 1100),
        (5000, 5100),
    ];
    let focus_points = vec![5050];
    let prioritized =
        prioritize_ranges_for_focus(ranges, &focus_points, MAX_HIGHLIGHT_WINDOWS_PER_PASS);

    assert_eq!(prioritized.len(), MAX_HIGHLIGHT_WINDOWS_PER_PASS);
    assert!(
        prioritized
            .iter()
            .any(|(start, end)| *start <= 5050 && 5050 <= *end),
        "focus-adjacent range should be retained after truncation"
    );
}

#[test]
fn test_columns_and_relations_use_different_styles() {
    let mut highlighter = SqlHighlighter::new();
    highlighter.set_highlight_data(HighlightData {
        tables: vec!["EMP".to_string()],
        views: Vec::new(),
        columns: vec!["ENAME".to_string()],
    });

    let text = "SELECT ENAME FROM EMP";
    let styles = highlighter.generate_styles(text);

    let col_start = text.find("ENAME").unwrap();
    let col_end = col_start + "ENAME".len();
    assert!(
        styles[col_start..col_end]
            .chars()
            .all(|c| c == STYLE_COLUMN),
        "columns should use column style"
    );

    let table_start = text.find("EMP").unwrap();
    let table_end = table_start + "EMP".len();
    assert!(
        styles[table_start..table_end]
            .chars()
            .all(|c| c == STYLE_IDENTIFIER),
        "relations should use identifier style"
    );
}

#[test]
fn test_multibyte_text_preserves_byte_length_styles() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT '한글🙂' AS 이름 FROM dual";
    let styles = highlighter.generate_styles(text);

    assert_eq!(
        styles.len(),
        text.len(),
        "style length must match byte length"
    );

    let string_start = text.find("'").unwrap();
    let string_end = text[string_start + 1..].find("'").unwrap() + string_start + 2;
    assert!(
        styles[string_start..string_end]
            .chars()
            .all(|c| c == STYLE_STRING),
        "multibyte string literal should be string style"
    );
}

// ── LexerState / generate_styles_with_state tests ─────────────────────

#[test]
fn test_generate_styles_with_state_normal_matches_generate_styles() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT 'hello' FROM dual -- comment";
    let plain = highlighter.generate_styles(text);
    let (stateful, exit) = highlighter.generate_styles_with_state(text, LexerState::Normal);
    assert_eq!(plain, stateful);
    assert_eq!(exit, LexerState::Normal);
}

#[test]
fn test_exit_state_unclosed_block_comment() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT /* unclosed comment";
    let (styles, exit) = highlighter.generate_styles_with_state(text, LexerState::Normal);
    assert_eq!(exit, LexerState::InBlockComment);
    let comment_start = text.find("/*").unwrap();
    assert!(
        styles[comment_start..].chars().all(|c| c == STYLE_COMMENT),
        "unclosed block comment should be COMMENT style"
    );
}

#[test]
fn test_exit_state_unclosed_hint() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT /*+ FULL(t)";
    let (_styles, exit) = highlighter.generate_styles_with_state(text, LexerState::Normal);
    assert_eq!(exit, LexerState::InHintComment);
}

#[test]
fn test_exit_state_unclosed_string() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT 'unclosed string";
    let (styles, exit) = highlighter.generate_styles_with_state(text, LexerState::Normal);
    assert_eq!(exit, LexerState::InSingleQuote);
    let str_start = text.find("'").unwrap();
    assert!(
        styles[str_start..].chars().all(|c| c == STYLE_STRING),
        "unclosed string should be STRING style"
    );
}

#[test]
fn test_exit_state_unclosed_q_quote() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT q'[unclosed q-string";
    let (_styles, exit) = highlighter.generate_styles_with_state(text, LexerState::Normal);
    assert!(
        matches!(exit, LexerState::InQQuote { closing: ']' }),
        "expected InQQuote with ']', got {:?}",
        exit
    );
}

#[test]
fn test_exit_state_unclosed_double_quote() {
    let highlighter = SqlHighlighter::new();
    let text = r#"SELECT "unclosed_ident"#;
    let (_styles, exit) = highlighter.generate_styles_with_state(text, LexerState::Normal);
    assert_eq!(exit, LexerState::InDoubleQuote);
}

#[test]
fn test_entry_state_in_block_comment_continues() {
    let highlighter = SqlHighlighter::new();
    // Simulate a window that starts in the middle of a block comment
    let text = "still commenting */ SELECT 1";
    let (styles, exit) = highlighter.generate_styles_with_state(text, LexerState::InBlockComment);
    assert_eq!(exit, LexerState::Normal);
    // "still commenting */" should be comment
    let comment_end = text.find("*/").unwrap() + 2;
    assert!(
        styles[..comment_end].chars().all(|c| c == STYLE_COMMENT),
        "continued block comment should be COMMENT"
    );
    // "SELECT" after should be keyword
    let select_pos = text.find("SELECT").unwrap();
    assert!(
        styles[select_pos..select_pos + 6]
            .chars()
            .all(|c| c == STYLE_KEYWORD),
        "SELECT after comment close should be KEYWORD"
    );
}

#[test]
fn test_entry_state_in_hint_continues() {
    let highlighter = SqlHighlighter::new();
    let text = "FULL(t) */ SELECT 1";
    let (styles, exit) = highlighter.generate_styles_with_state(text, LexerState::InHintComment);
    assert_eq!(exit, LexerState::Normal);
    let hint_end = text.find("*/").unwrap() + 2;
    assert!(
        styles[..hint_end].chars().all(|c| c == STYLE_HINT),
        "continued hint should be HINT"
    );
}

#[test]
fn test_entry_state_in_single_quote_continues() {
    let highlighter = SqlHighlighter::new();
    let text = "still in string' FROM dual";
    let (styles, exit) = highlighter.generate_styles_with_state(text, LexerState::InSingleQuote);
    assert_eq!(exit, LexerState::Normal);
    let str_end = text.find("'").unwrap() + 1;
    assert!(
        styles[..str_end].chars().all(|c| c == STYLE_STRING),
        "continued string should be STRING"
    );
}

#[test]
fn test_entry_state_in_q_quote_continues() {
    let highlighter = SqlHighlighter::new();
    let text = "still in q-string]' FROM dual";
    let (styles, exit) =
        highlighter.generate_styles_with_state(text, LexerState::InQQuote { closing: ']' });
    assert_eq!(exit, LexerState::Normal);
    let q_end = text.find("]'").unwrap() + 2;
    assert!(
        styles[..q_end].chars().all(|c| c == STYLE_STRING),
        "continued q-quote should be STRING"
    );
}

#[test]
fn test_entry_state_in_double_quote_continues() {
    let highlighter = SqlHighlighter::new();
    let text = r#"continued_ident" FROM dual"#;
    let (styles, exit) = highlighter.generate_styles_with_state(text, LexerState::InDoubleQuote);
    assert_eq!(exit, LexerState::Normal);
    let ident_end = text.find('"').unwrap() + 1;
    assert!(
        styles[..ident_end].chars().all(|c| c == STYLE_IDENTIFIER),
        "continued quoted identifier should be IDENTIFIER"
    );
}

#[test]
fn test_entry_state_block_comment_never_closes() {
    let highlighter = SqlHighlighter::new();
    let text = "all of this is inside the comment";
    let (styles, exit) = highlighter.generate_styles_with_state(text, LexerState::InBlockComment);
    assert_eq!(exit, LexerState::InBlockComment);
    assert!(
        styles.chars().all(|c| c == STYLE_COMMENT),
        "entire text should be COMMENT when starting InBlockComment and no close"
    );
}

#[test]
fn test_cross_window_block_comment_round_trip() {
    let highlighter = SqlHighlighter::new();
    // Window 1: opens comment
    let window1 = "SELECT 1; /* long comment starts here";
    let (_s1, state1) = highlighter.generate_styles_with_state(window1, LexerState::Normal);
    assert_eq!(state1, LexerState::InBlockComment);

    // Window 2: continues comment
    let window2 = "...still commenting...\nmore comment text";
    let (_s2, state2) = highlighter.generate_styles_with_state(window2, state1);
    assert_eq!(state2, LexerState::InBlockComment);

    // Window 3: closes comment
    let window3 = "end of comment */ SELECT 2 FROM dual";
    let (s3, state3) = highlighter.generate_styles_with_state(window3, state2);
    assert_eq!(state3, LexerState::Normal);
    let select_pos = window3.find("SELECT").unwrap();
    assert!(
        s3[select_pos..select_pos + 6]
            .chars()
            .all(|c| c == STYLE_KEYWORD),
        "SELECT after comment close should be KEYWORD"
    );
}

#[test]
fn test_probe_entry_state_recovers_from_stale_default_inside_comment() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT 1; /* open comment\ncontinued comment\nSELECT 2";
    let style_text = std::iter::repeat_n(STYLE_DEFAULT, text.len()).collect::<String>();

    let pos = text.find("continued").unwrap_or(0);
    let entry = highlighter.probe_entry_state_for_text(text, &style_text, pos);
    assert_eq!(entry, LexerState::InBlockComment);
}

#[test]
fn test_select_highlight_ranges_drops_empty_ranges_when_line_end_is_before_anchor() {
    let text = "SELECT 1";
    let ranges = select_highlight_ranges_for_text(text, 0, Some((3, 3)), None);
    assert!(ranges.iter().all(|(start, end)| start < end));
}

#[test]
fn test_probe_entry_state_clamps_mid_byte_cursor_inside_comment() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT 1; /* 한글\n계속 */";
    let style_text = std::iter::repeat_n(STYLE_DEFAULT, text.len()).collect::<String>();
    let comment_pos = text.find("한").unwrap_or(0);
    let mid_byte_pos = comment_pos + 1;

    assert!(!text.is_char_boundary(mid_byte_pos));

    let entry = highlighter.probe_entry_state_for_text(text, &style_text, mid_byte_pos);
    assert_eq!(entry, LexerState::InBlockComment);
}

#[test]
fn test_probe_entry_state_recovers_q_quote_state_inside_scroll_window() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT q'[first line\nsecond line with ' quote\nthird line]'\nFROM dual";
    let style_text = std::iter::repeat_n(STYLE_DEFAULT, text.len()).collect::<String>();
    let pos = text.find("second line").unwrap();

    let entry = highlighter.probe_entry_state_for_text(text, &style_text, pos);
    assert_eq!(entry, LexerState::InQQuote { closing: ']' });
}

#[test]
fn test_probe_entry_state_recovers_single_quote_state_inside_scroll_window() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT 'first line\nsecond line with '' quote\nthird line'\nFROM dual";
    let style_text = std::iter::repeat_n(STYLE_DEFAULT, text.len()).collect::<String>();
    let pos = text.find("second line").unwrap();

    let entry = highlighter.probe_entry_state_for_text(text, &style_text, pos);
    assert_eq!(entry, LexerState::InSingleQuote);
}

#[test]
fn test_probe_entry_state_returns_normal_for_non_multiline_prev_style() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT abc FROM dual";
    let style_text = highlighter.generate_styles(text);
    let pos = text.find("FROM").unwrap();

    let entry = highlighter.probe_entry_state_for_text(text, &style_text, pos);
    assert_eq!(entry, LexerState::Normal);
}

#[test]
fn test_clamp_buffer_boundary_keeps_ascii_boundary() {
    let mut buffer = TextBuffer::default();
    buffer.set_text("SELECT 1");

    let idx = "SELECT".len();
    assert_eq!(clamp_buffer_boundary(&buffer, idx), idx);
}

#[test]
fn test_clamp_buffer_boundary_clamps_mid_byte_utf8() {
    let mut buffer = TextBuffer::default();
    let text = "SELECT 한글";
    buffer.set_text(text);

    let char_start = text.find('한').unwrap_or(0);
    let mid_byte = char_start + 1;
    assert!(!text.is_char_boundary(mid_byte));

    assert_eq!(clamp_buffer_boundary(&buffer, mid_byte), char_start);
}

#[test]
fn test_probe_entry_state_recovers_long_offscreen_block_comment() {
    let highlighter = SqlHighlighter::new();
    let filler = "a".repeat(STATE_PROBE_DISTANCE + 128);
    let text = format!("/*{}{}", filler, "\ncontinued comment");
    let style_text = std::iter::repeat_n(STYLE_DEFAULT, text.len()).collect::<String>();
    let pos = text.find("continued").unwrap();

    let entry = highlighter.probe_entry_state_for_text(&text, &style_text, pos);
    assert_eq!(entry, LexerState::InBlockComment);
}

#[test]
fn test_probe_entry_state_recovers_long_offscreen_q_quote() {
    let highlighter = SqlHighlighter::new();
    let filler = "가".repeat((STATE_PROBE_DISTANCE / 3) + 64);
    let text = format!("SELECT uq'가{}계속가' FROM dual", filler);
    let style_text = std::iter::repeat_n(STYLE_DEFAULT, text.len()).collect::<String>();
    let pos = text.find("계속").unwrap();

    let entry = highlighter.probe_entry_state_for_text(&text, &style_text, pos);
    assert_eq!(entry, LexerState::InQQuote { closing: '가' });
}

#[test]
fn test_prepare_window_requests_clamps_mid_byte_inputs_to_utf8_boundaries() {
    let highlighter = SqlHighlighter::new();
    let text = "SELECT '한글 문자열' FROM dual\nWHERE col = q'[값]';";
    let mut buffer = TextBuffer::default();
    buffer.set_text(text);
    let mut style_buffer = TextBuffer::default();
    style_buffer.set_text(&std::iter::repeat_n(STYLE_DEFAULT, text.len()).collect::<String>());

    let char_pos = text.find('한').unwrap_or(0);
    let mid_byte_pos = char_pos + 1;
    assert!(!text.is_char_boundary(mid_byte_pos));

    let requests = highlighter.prepare_window_highlight_requests(
        &buffer,
        &style_buffer,
        mid_byte_pos,
        Some((mid_byte_pos, mid_byte_pos + 5)),
        Some((mid_byte_pos, text.len())),
    );

    assert!(!requests.is_empty());
    for request in requests {
        assert!(text.is_char_boundary(request.start));
        assert!(text.is_char_boundary(request.end));
        assert!(request.start < request.end);
        assert_eq!(request.text.len(), request.end - request.start);
    }
}
