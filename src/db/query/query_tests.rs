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
fn test_line_block_depths_with_for_update_clause() {
    let sql = r#"SELECT id, status
FROM jobs
WHERE status = 'PENDING'
FOR UPDATE SKIP LOCKED;"#;
    let depths = QueryExecutor::line_block_depths(sql);
    assert_eq!(depths, vec![0, 0, 0, 0]);
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

    assert!(!result.success, "batch summary should fail when any statement fails");
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

    assert!(!result.success, "select batch should fail when any statement fails");
    assert!(result.is_select);
    assert!(result.message.contains("Errors:"));
    assert!(result.message.contains("Executed 1 of 2 statements"));
}
