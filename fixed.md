# 예외 처리 보완 내역

## 2026-03-04 인텔리센스 누락 구문 보완 (`ONLY(...)` / `TABLE(...)` relation wrapper)

### [중] FROM 절 relation wrapper를 테이블명으로 해석하지 못해 별칭 기반 컬럼 추천이 누락되던 문제 수정
- **증상**:
  - `SELECT o.| FROM ONLY (hr.orders) o`
  - `SELECT c.| FROM TABLE(hr.order_rows) c`
  - 위 형태에서 wrapper 내부 relation 경로를 수집하지 못해 인텔리센스 스코프에서 테이블/별칭 해석이 불완전했습니다.
- **원인**:
  - `src/ui/intellisense_context.rs`의 relation 수집 경로(`parse_table_name_deep`)가 일반 식별자 경로만 처리하고,
  - Oracle wrapper 문법인 `ONLY (...)` / `TABLE (...)`를 relation 토큰으로 정규화하지 않았습니다.
- **수정**:
  - `parse_relation_wrapper_table_name` 헬퍼를 추가해 `ONLY (...)` / `TABLE (...)`를 우선 파싱하도록 확장했습니다.
  - `ONLY (...)`는 내부 식별자 경로를 필수로 해석하고, `TABLE (...)`는 식별자 인자면 경로를, 표현식 인자면 fallback relation key(`TABLE`)를 사용하도록 정리했습니다.

### [유사 케이스] wrapper 인자 형태별 일괄 검증
- `TABLE(collection_expression)`처럼 식별자 경로가 아닌 인자도 별칭 기반 추천이 유지되는지 회귀 테스트로 함께 검증했습니다.

### [테스트] 회귀 테스트 추가
- `only_wrapper_relation_is_collected_and_visible`
- `table_wrapper_relation_with_identifier_argument_is_collected`
- `table_wrapper_collection_expression_keeps_alias`

### [검증]
- `cargo test -q only_wrapper_relation_is_collected_and_visible -- --nocapture` 통과
- `cargo test -q table_wrapper_ -- --nocapture` 통과
- `cargo test` 전체 통과


## 2026-03-05 Oracle 공통 파서 엔진 오탐 수정 (`NAME/LANGUAGE/LIBRARY` 식별자)

### [중] 일반 식별자 `NAME/LANGUAGE/LIBRARY`를 `EXTERNAL` call spec으로 오인식해 문장을 조기 분리하던 문제 수정
- **증상**:
  - `CREATE OR REPLACE PROCEDURE ... IS name NUMBER; language NUMBER; library NUMBER; BEGIN ... END; SELECT ...;` 형태에서
  - 선언부 식별자 `name/language/library`가 외부 루틴 키워드로 오인식되어 statement가 `name ...;`, `language ...;`, `library ...;` 단위로 쪼개졌습니다.
- **원인**:
  - `src/sql_parser_engine.rs`의 `handle_routine_is_external`가 루틴 depth에서 `EXTERNAL/LANGUAGE/NAME/LIBRARY` 토큰을 문맥 없이 즉시 외부 루틴으로 확정했습니다.
- **수정**:
  - `RoutineFrame`에 `external_clause_state` 상태 머신(`SawExternalKeyword`, `AwaitingLanguageTarget`, `Confirmed`)을 추가했습니다.
  - `LANGUAGE`는 다음 토큰이 실제 언어 타깃(`C/JAVA/JAVASCRIPT/PYTHON`)일 때만 외부 루틴으로 확정하도록 변경했습니다.
  - `NAME`/`LIBRARY`는 `EXTERNAL` 문맥(`SawExternalKeyword`/`Confirmed`)에서만 외부 루틴 확정에 반영되도록 제한했습니다.
  - 결과적으로 일반 식별자 `name/language/library`는 더 이상 외부 루틴 세미콜론 정책을 오염시키지 않습니다.

### [유사 케이스] `EXTERNAL` 없이 `AS LANGUAGE C NAME ...` 형태도 함께 검증
- 외부 루틴 문법 호환성 회귀를 막기 위해 `EXTERNAL` 키워드가 없는 call spec도 테스트로 보강했습니다.

### [테스트] 회귀/호환 케이스 추가
- `test_procedure_name_language_library_identifiers_do_not_trigger_external_split`
- `test_split_format_items_name_language_library_identifiers_do_not_trigger_external_split`
- `test_create_external_function_language_clause_without_external_keyword_splits`
- `test_split_format_items_external_language_clause_without_external_keyword_splits`
- `sql_parser_engine::tests::name_language_library_identifiers_do_not_activate_external_clause_policy`
- `sql_parser_engine::tests::language_clause_without_external_keyword_still_marks_external_routine_split`

### [검증]
- `cargo test -q name_language_library_identifiers_do_not_trigger_external_split -- --nocapture` 통과
- `cargo test -q language_clause_without_external_keyword -- --nocapture` 통과
- `cargo test -q external -- --nocapture` 통과
- `cargo test` 전체 통과

## 2026-03-05 Oracle 공통 파서 엔진 `WITH FUNCTION` 복구 키워드 보강 (`AUDIT` / `NOAUDIT`)

### [중] `WITH FUNCTION/PROCEDURE` 복구 경로에서 `AUDIT`/`NOAUDIT`를 새 문장 시작으로 인식하지 못하던 문제 수정
- **증상**:
  - `WITH FUNCTION ... END; AUDIT SESSION; SELECT ...;`
  - `WITH FUNCTION ... END; NOAUDIT SESSION; SELECT ...;`
  - 위 형태에서 `AUDIT`/`NOAUDIT`가 새 top-level statement로 분리되지 않고, `WITH FUNCTION` 블록에 붙어 하나의 statement로 병합됐습니다.
- **원인**:
  - `src/sql_text.rs`의 `is_statement_head_keyword`에 `AUDIT`, `NOAUDIT`가 누락되어
  - `WITH FUNCTION/PROCEDURE` 복구 상태(`AwaitingMainQuery`)에서 새 statement 시작을 감지하지 못했습니다.
- **수정**:
  - `is_statement_head_keyword`에 `AUDIT`, `NOAUDIT`를 추가해 복구 분리 조건을 보강했습니다.
  - 회귀 테스트 4건(`split_script_items` 2건, `split_format_items` 2건)을 추가했습니다.

### [테스트] 회귀 테스트 추가
- `test_split_script_items_oracle_with_function_recovers_to_audit_statement_head`
- `test_split_script_items_oracle_with_function_recovers_to_noaudit_statement_head`
- `test_split_format_items_oracle_with_function_recovers_to_audit_statement_head`
- `test_split_format_items_oracle_with_function_recovers_to_noaudit_statement_head`

### [검증]
- `cargo test test_split_script_items_oracle_with_function_recovers_to_audit_statement_head -- --nocapture` (수정 전 실패 확인)
- `cargo test recovers_to_audit_statement_head -- --nocapture`
- `cargo test recovers_to_noaudit_statement_head -- --nocapture`
- `cargo test oracle_with_function_recovers_to_ -- --nocapture`
- `cargo test` 전체 통과

## 2026-03-05 Oracle 공통 파서 엔진 `WITH FUNCTION` 복구 키워드 보강 (`PURGE` / `FLASHBACK`)

### [중] `WITH FUNCTION/PROCEDURE` 복구 경로에서 `PURGE`/`FLASHBACK`를 새 문장 시작으로 인식하지 못하던 문제 수정
- **증상**:
  - `WITH FUNCTION ... END; PURGE TABLE ...; SELECT ...;`
  - `WITH FUNCTION ... END; FLASHBACK TABLE ...; SELECT ...;`
  - 위 형태에서 `PURGE`/`FLASHBACK`가 새 top-level statement로 분리되지 않고, `WITH FUNCTION` 블록에 붙어 하나의 statement로 병합됐습니다.
- **원인**:
  - `src/sql_text.rs`의 `is_statement_head_keyword`에 `PURGE`, `FLASHBACK`가 누락되어
  - `WITH FUNCTION/PROCEDURE` 복구 상태(`AwaitingMainQuery`)에서 새 statement 시작을 감지하지 못했습니다.
- **수정**:
  - `is_statement_head_keyword`에 `PURGE`, `FLASHBACK`를 추가해 복구 분리 조건을 보강했습니다.
  - 회귀 테스트 4건(`split_script_items` 2건, `split_format_items` 2건)을 추가했습니다.

### [테스트] 회귀 테스트 추가
- `test_split_script_items_oracle_with_function_recovers_to_purge_statement_head`
- `test_split_script_items_oracle_with_function_recovers_to_flashback_statement_head`
- `test_split_format_items_oracle_with_function_recovers_to_purge_statement_head`
- `test_split_format_items_oracle_with_function_recovers_to_flashback_statement_head`

### [검증]
- `cargo test -q recovers_to_purge_statement_head -- --nocapture`
- `cargo test -q recovers_to_flashback_statement_head -- --nocapture`
- `cargo test -q recovers_to_ -- --nocapture`
- `cargo test` 전체 통과

## 2026-03-05 Oracle 공통 파서 엔진 누락 문법 보완 (`SIMPLE TRIGGER WHEN (NEW.COMPOUND ...)`)

### [중] SIMPLE TRIGGER `WHEN` 절 식별자 `COMPOUND`를 `COMPOUND TRIGGER`로 오인식하던 문제 수정
- **증상**:
  - `CREATE OR REPLACE TRIGGER ... WHEN (NEW.COMPOUND IS NULL) ... END; SELECT ...;` 형태에서
  - `COMPOUND` 식별자가 compound trigger 시작으로 오인식되어 `END;` 이후 statement split이 일어나지 않고 뒤 `SELECT`가 병합될 수 있었습니다.
- **원인**:
  - `src/sql_parser_engine.rs`의 `handle_block_openers`가
  - trigger 헤더(`block_depth == 0`)에서 `COMPOUND` 토큰 단독으로 compound trigger 진입을 확정했습니다.
  - 즉, 실제 문법인 `COMPOUND TRIGGER` 키워드 쌍 검증 없이 `BlockKind::Compound`를 push했습니다.
- **수정**:
  - `SplitState`에 `saw_compound_keyword` 상태를 추가해 `COMPOUND` 단독 감지를 임시 상태로만 저장했습니다.
  - 다음 토큰이 `TRIGGER`일 때만 compound trigger로 확정(`mark_compound_trigger` + `BlockKind::Compound` push)하도록 변경했습니다.
  - `TRIGGER`가 아닌 토큰이 오면 후보 상태를 즉시 해제하고, `reset_create_state`에서도 상태를 정리하도록 반영했습니다.

### [테스트] 회귀 테스트 추가
- `test_split_script_items_simple_trigger_when_clause_compound_identifier_splits_normally`
- `compound_trigger_requires_compound_trigger_keyword_pair`
- `compound_trigger_header_still_splits_after_end`

### [검증]
- `cargo test test_split_script_items_simple_trigger_when_clause_compound_identifier_splits_normally -- --nocapture`
- `cargo test compound_trigger_requires_compound_trigger_keyword_pair -- --nocapture`
- `cargo test compound_trigger_header_still_splits_after_end -- --nocapture`
- `cargo test` 전체 통과

## 2026-03-05 인텔리센스 누락 구문 보완 (`JOIN ... USING (...)`)

### [중] `JOIN ... USING (...)`에서 인텔리센스가 컬럼 컨텍스트를 잃던 문제 수정
- **증상**:
  - `SELECT * FROM employees e JOIN departments d USING (|)` 위치에서
  - 문맥이 `JoinCondition`이 아닌 `Initial`로 계산되어 컬럼 추천이 비정상 동작할 수 있었습니다.
- **원인**:
  - `src/ui/intellisense_context.rs`의 `USING` 분기가 `MERGE ... USING`과 `JOIN ... USING`을 동일하게 처리했습니다.
  - `JOIN ... USING`에서도 `FromClause + expect_table`로 전이되어, 괄호 내부를 조인 조건이 아닌 일반 구간으로 해석했습니다.
- **수정**:
  - `USING` 처리 로직을 분리했습니다.
  - `MergeTarget/IntoClause`에서의 `USING`은 기존대로 `FromClause`(테이블 컨텍스트) 유지.
  - `FromClause`에서의 `USING`은 `JoinCondition`(컬럼 컨텍스트)으로 전이하도록 변경.

### [테스트] 회귀 테스트 추가
- `phase_join_using_clause` (`src/ui/intellisense_context/tests.rs`)
- `detect_sql_context_join_using_clause_is_column_name` (`src/ui/intellisense.rs`)

### [검증]
- `cargo test phase_join_using_clause -- --nocapture` (수정 전 실패 확인)
- `cargo test join_using_clause -- --nocapture`
- `cargo test`

## 2026-03-04 Oracle 공통 파서 엔진 누락 문법 보완 (`MATCH_RECOGNIZE inline DEFINE` / `CREATE TYPE OBJECT` 속성명)

### [중] `MATCH_RECOGNIZE (...)` 내부 `DEFINE b AS ...`가 SQL*Plus `DEFINE` 명령으로 오인식되던 문제 수정
- **증상**:
  - `MATCH_RECOGNIZE` 절에서 `DEFINE b AS ...`를 한 줄에 작성하면 statement가 중간에서 끊기고,
  - `DEFINE`가 `ToolCommand::Define`으로 분류되어 SQL 본문이 손상될 수 있었습니다.
- **원인**:
  - `src/db/query/script.rs`의 `split_script_items`/`split_format_items`가
  - 진행 중 statement에서도 `block_depth == 0`만 확인하고 도구 명령 판별(`parse_tool_command`)을 수행했습니다.
  - `MATCH_RECOGNIZE (...)` 내부는 `block_depth == 0`이지만 `paren_depth > 0`인 컨텍스트라 오인식이 가능했습니다.
- **수정**:
  - 도구 명령 판별 및 CREATE-PL/SQL 강제 종료 분기에 `paren_depth == 0` 조건을 추가했습니다.
  - `src/sql_parser_engine.rs`에 `SqlParserEngine::paren_depth()` 접근자를 추가해 호출부에서 안전하게 depth를 참조하도록 정리했습니다.
- **효과**:
  - 괄호 내부 SQL 절(`MATCH_RECOGNIZE`, 객체 정의 속성 목록 등)에서는 명령어 오인식/강제 분리가 발생하지 않습니다.

### [중] `CREATE TYPE ... AS OBJECT (...)` 속성명이 `CREATE_*`로 시작할 때 강제 분리되던 문제 수정
- **증상**:
  - 객체 타입 속성 라인이 `create_flag NUMBER`처럼 `CREATE` 접두를 가지면,
  - 다음 statement 시작으로 오인되어 `CREATE TYPE` 문장이 중간에서 분리될 수 있었습니다.
- **원인**:
  - CREATE-PL/SQL 복구 분기에서 `trimmed_upper.starts_with("CREATE")`를 괄호 depth 검증 없이 적용했습니다.
- **수정**:
  - 동일 분기에 `paren_depth == 0` 가드를 추가해 괄호 내부 라인에서는 복구 분기가 동작하지 않도록 보정했습니다.

### [테스트] 회귀 테스트 추가
- `test_match_recognize_inline_define_not_parsed_as_tool_command`
- `test_split_format_items_match_recognize_inline_define_not_parsed_as_tool_command`
- `test_create_type_object_attribute_prefixed_create_does_not_force_split`

### [검증]
- `cargo test test_match_recognize_inline_define_not_parsed_as_tool_command`
- `cargo test inline_define_not_parsed_as_tool_command`
- `cargo test test_create_type_object_attribute_prefixed_create_does_not_force_split`
- `cargo test` 전체 통과

## 2026-03-04 Oracle 공통 파서 엔진 누락 구문 보완 (중첩 EXTERNAL 루틴)

### [중] `PACKAGE/TYPE BODY` 내부 `PROCEDURE ... IS EXTERNAL ...;` 뒤 후속 문장이 분리되지 않던 문제 수정
- **증상**:
  - `CREATE OR REPLACE PACKAGE BODY ... PROCEDURE ... IS EXTERNAL ...; END pkg; SELECT ...;` 형태에서
  - 내부 `EXTERNAL` 루틴 선언이 세미콜론으로 종료되어도 내부 루틴 블록이 닫히지 않아, 뒤 `SELECT`가 같은 statement로 병합될 수 있었습니다.
- **원인**:
  - `src/sql_parser_engine.rs`의 `handle_routine_is_external`가 `block_depth == 1`(top-level)에서만 semicolon policy를 설정했습니다.
  - 그 결과 중첩 루틴(`block_depth > 1`)은 `BEGIN` 없는 `EXTERNAL` 선언 종료 시점을 인식하지 못해 `AsIs` 블록이 남았습니다.
- **수정**:
  - semicolon 정책을 확장해 중첩 루틴은 `CloseRoutineBlock`으로 처리하도록 추가했습니다.
  - 세미콜론 처리 시 top-level `EXTERNAL`은 기존처럼 statement split을 유지하고,
  - 중첩 `EXTERNAL`은 statement split 없이 내부 루틴 블록만 닫도록 `close_external_routine_on_semicolon` 경로를 추가했습니다.
  - 관련 상태(`routine_is_stack`, `pending_subprogram_begins`, `block_stack`)가 함께 정리되도록 보강했습니다.

### [테스트] 회귀 테스트 추가/보강
- `test_package_body_nested_external_procedure_followed_by_select_splits`
- `semicolon_action_closes_nested_external_routine_without_split`
- `close_external_routine_semicolon_only_closes_nested_routine_block`
- 기존 단위 테스트 기대값 보정:
  - `package_with_nested_external_procedure_does_not_split_mid_statement`

### [검증]
- `cargo test test_package_body_nested_external_procedure_followed_by_select_splits -- --nocapture`
- `cargo test nested_external_routine -- --nocapture`
- `cargo test close_external_routine_semicolon_only_closes_nested_routine_block -- --nocapture`
- `cargo test`

## 2026-03-05 Oracle 공통 파서 엔진 보강 (`TYPE BODY MEMBER PROCEDURE EXTERNAL`)

### [중] `TYPE BODY` 내부 `MEMBER PROCEDURE ... EXTERNAL` 구문 미검증 항목 보강
- **증상**:
  - 기존 회귀 테스트는 `TYPE BODY`의 `MEMBER FUNCTION` 및 일부 `EXTERNAL` 케이스를 검증했지만,
  - `MEMBER PROCEDURE ... EXTERNAL` 조합은 별도 테스트가 없어 분할 동작 미검증 상태였습니다.
- **원인/판단**:
  - 파서 상태 머신은 `EXTERNAL` 처리 자체를 공유 경로로 처리하므로,
  - 현재 엔진에서는 별도 코드 수정 없이 동일 동작으로 처리될 것으로 판단했습니다.
- **수정**:
  - `src/db/query/query_tests.rs`에 회귀 테스트를 추가해 구문을 명시적으로 커버했습니다.
    - `test_type_body_nested_external_member_procedure_followed_by_select_splits`

### [테스트] 회귀 테스트 추가
- `test_type_body_nested_external_member_procedure_followed_by_select_splits`

### [검증]
- `cargo test test_type_body_nested_external_member_procedure_followed_by_select_splits -- --nocapture`
- `cargo test`

## 2026-03-03 쿼리 depth 로직 누락 구문 케이스 보완 (FROM-소비 함수 미닫힘 복구)

### [중] `TRIM/EXTRACT/...` 괄호 미닫힘 시 실제 `FROM` 절이 함수 내부 `FROM`으로 계속 오인되던 문제 수정
- **증상**:
  - `SELECT TRIM(LEADING '0' FROM name FROM ...`처럼 함수 호출의 닫는 `)`가 빠진 입력에서,
  - 뒤쪽 실제 `FROM`이 계속 함수 내부 문법으로 처리되어 `phase/table scope` 복구가 늦거나 실패할 수 있었습니다.
- **원인**:
  - `src/ui/intellisense_context.rs`의 `analyze_phase` / `collect_tables_deep`에서
  - `is_from_consuming_function` 분기(`EXTRACT/TRIM/SUBSTRING/OVERLAY`)가 해당 괄호 depth에서 `FROM`을 무조건 suppress 했습니다.
  - 따라서 malformed 입력에서 두 번째 `FROM`(실제 SQL clause)도 계속 무시되었습니다.
- **수정**:
  - 괄호 depth별로 `FROM` 소비 여부를 추적하는 `paren_func_from_consumed_stack`을 추가했습니다.
  - FROM-소비 함수에서는 **첫 번째 `FROM`만** 내부 문법으로 소비하고,
  - 같은 depth에서 이후 `FROM`은 실제 `SqlPhase::FromClause` 전이로 처리해 복구하도록 변경했습니다.
  - 동일 로직을 table 수집 경로(`collect_tables_deep`)에도 적용해 phase/테이블 스코프 일관성을 맞췄습니다.

### [테스트] 회귀 테스트 추가
- `malformed_trim_missing_close_paren_recovers_real_from_clause`
- `malformed_trim_missing_close_paren_still_collects_from_tables`

### [검증]
- `cargo test malformed_trim_missing_close_paren -- --nocapture`
- `cargo test`

## 2026-03-04 Oracle 공통 파서 엔진 누락 문법 보완 (`CREATE JAVA SOURCE`)

### [중] `CREATE ... JAVA SOURCE` 본문 내부 세미콜론(`;`)으로 statement가 잘못 분리되던 문제 수정
- **증상**:
  - `CREATE OR REPLACE AND COMPILE JAVA SOURCE ... AS` 본문(Java 코드) 내부 `;`를 top-level SQL terminator로 오인해 문장이 중간 분리되거나,
  - 반대로 `AS`를 PL/SQL `AS/IS` 블록 시작으로 잘못 해석해 `block_depth`가 남아 `/` 구분자 처리까지 꼬일 수 있었습니다.
- **원인**:
  - `src/sql_parser_engine.rs`의 `track_create_plsql`가 `JAVA SOURCE`를 CREATE 상태로 추적하지 못했고,
  - `SemicolonAction`/`AsIsBlockStart`가 `JAVA SOURCE` 컨텍스트를 별도로 고려하지 않았습니다.
- **수정**:
  - `CreateState::AwaitingJavaTarget`, `CreatePlsqlKind::JavaSource` 상태를 추가해 `CREATE ... JAVA SOURCE`를 명시적으로 인식.
  - `SemicolonAction::from_state`에서 `JAVA SOURCE` 컨텍스트일 때 세미콜론을 분리 트리거로 사용하지 않도록 보강.
  - `AsIsBlockStart::from_token`에서 `JAVA SOURCE ... AS`는 PL/SQL 블록 opener로 취급하지 않도록 제외.
  - `CREATE` modifier 파싱에 `AND/COMPILE/RESOLVE`를 반영해 `CREATE OR REPLACE AND COMPILE JAVA SOURCE` 패턴을 정상 인식.
  - `statement_bounds_at_cursor` 내부 span collector(`src/db/query/executor.rs`)에도 동일 규칙을 반영해, `JAVA SOURCE` 본문 세미콜론에서 커서 문장 경계가 잘리지 않도록 정합성을 맞췄습니다.
- **효과**:
  - Java 본문 내부 `;`가 statement를 깨뜨리지 않고, SQL*Plus slash(`"/"`) 또는 다음 statement 경계에서 안정적으로 분리됩니다.

### [테스트] 회귀 테스트 추가
- `create_state_transitions_to_java_source_on_create_and_compile_java_source`
- `semicolon_action_keeps_java_source_statement_open_at_top_level`
- `test_split_script_items_oracle_create_java_source_keeps_body_until_slash`
- `test_split_format_items_oracle_create_java_source_keeps_body_until_slash`
- `test_statement_bounds_at_cursor_create_java_source_ignores_body_semicolon_until_slash`

### [검증]
- `cargo test create_state_transitions_to_java_source_on_create_and_compile_java_source -- --nocapture`
- `cargo test semicolon_action_keeps_java_source_statement_open_at_top_level -- --nocapture`
- `cargo test test_split_script_items_oracle_create_java_source_keeps_body_until_slash -- --nocapture`
- `cargo test test_split_format_items_oracle_create_java_source_keeps_body_until_slash -- --nocapture`
- `cargo test test_statement_bounds_at_cursor_create_java_source_ignores_body_semicolon_until_slash -- --nocapture`
- `cargo test` 전체 통과

## 2026-03-03 쿼리 depth 로직 누락 구문 케이스 보완 (서브쿼리 내부 일반 괄호)

### [중] 서브쿼리 내부의 일반 괄호 `()`가 닫힐 때 query depth가 조기 감소하던 문제 수정
- **증상**: `FROM ( SELECT (1 + 2) ... FROM ... )`처럼 서브쿼리 내부에 일반 수식 괄호가 있을 때, 내부 `FROM` 라인의 `line_block_depths`가 바깥 depth로 잘못 내려갈 수 있었습니다.
- **원인**:
  - `src/db/query/script.rs`의 `line_block_depths`에서 서브쿼리 depth(`subquery_paren_depth`)는 `(` 뒤에 `SELECT/WITH/...`일 때만 증가했습니다.
  - 반면 `)`는 괄호 종류 구분 없이 항상 `subquery_paren_depth`를 감소시켜, 일반 수식 괄호 닫힘이 서브쿼리 depth까지 소비되는 경로가 있었습니다.
- **수정**:
  - `SubqueryParenKind` 스택(`NonSubquery`, `Pending`, `Subquery`)을 도입해 괄호별 성격을 추적하도록 변경했습니다.
  - `(` 처리 시 서브쿼리 시작 괄호만 `Subquery`로 기록하고 depth를 증가시킵니다.
  - `)` 처리 시 pop된 종류가 `Subquery`일 때만 `subquery_paren_depth`를 감소시키도록 보정했습니다.
  - `Pending` 괄호는 다음 유효 라인에서 실제 subquery head 여부에 따라 `Subquery`/`NonSubquery`로 승격/확정하도록 처리했습니다.

### [테스트] 회귀 테스트 추가
- `test_line_block_depths_preserves_subquery_depth_after_non_subquery_parentheses`

### [검증]
- `cargo test test_line_block_depths_preserves_subquery_depth_after_non_subquery_parentheses -- --nocapture`
- `cargo test line_block_depths -- --nocapture`
- `cargo test`

## 2026-03-03 쿼리 depth 로직 누락 구문 케이스 보완 (괄호 내부 WITH 시작점)

### [중] `WHERE ... IN (WITH ... )` 형태에서 CTE body depth 과소 계산 보정
- **증상**: `WHERE ... IN (` 다음에 바로 `WITH`가 시작되는 서브쿼리에서, CTE body 내부 커서의 `query depth`가 한 단계 낮게 계산될 수 있었습니다.
  - 예: `SELECT * FROM outer_t o WHERE o.id IN (WITH cte AS (SELECT | FROM inner_t) SELECT id FROM cte)`
- **원인**:
  - `src/ui/intellisense_context.rs`의 `WITH` 진입 조건이 `current_phase == Initial`로만 제한되어 있었습니다.
  - 괄호 내부가 `WHERE`/`SELECT` 같은 상위 phase를 상속한 경우(첫 토큰이 `WITH`여도), `WITH`를 쿼리 시작으로 표시하지 못해 depth가 과소 집계되었습니다.
- **수정**:
  - `should_enter_with_clause(...)` 헬퍼를 추가해 `WITH` 진입 조건을 보완했습니다.
  - 허용: `Initial` phase 또는 `depth > 0`에서 괄호 시작 직후(`last_word.is_none()`)의 `WITH`
  - 예외: `START WITH` 계층 쿼리 구문은 기존처럼 CTE `WITH`로 오인하지 않도록 제외
  - 적용 위치:
    - `analyze_phase` (cursor depth 계산)
    - `collect_tables_deep` (동일 phase/CTE 상태 전이 일관성 유지)

### [테스트] 회귀 테스트 추가
- `nested_with_in_where_subquery_cte_body_depth_counts_parent_query`
- `nested_with_in_where_subquery_main_select_depth_is_one`

### [검증]
- `cargo test nested_with_in_where_subquery -- --nocapture`
- `cargo test`

## 2026-03-03 쿼리 depth 로직 누락 구문 케이스 보완 (블록 코멘트 prefix + same-line 서브쿼리 헤더)

### [중] `(` 다음 줄이 `/* ... */ SELECT ...` 형태일 때 nested subquery depth 승격 누락 수정
- **증상**: `WHERE EXISTS (` 다음 줄이 블록 코멘트로 시작하면서 같은 줄에 `SELECT`가 이어지는 경우, `line_block_depths`가 해당 줄을 comment-only로 잘못 분류해 서브쿼리 depth를 올리지 못했습니다.
- **원인**:
  - `line_block_depths` 내부 `is_comment_or_blank` 판정이 `trim_start().starts_with("/*")`만으로 comment line으로 처리했습니다.
  - 이 때문에 `pending_subquery_paren` 해소 시점에서 `/* ... */ SELECT ...` 줄이 제외되어 nested head(`SELECT/WITH/...`) 인식이 누락되었습니다.
- **수정**:
  - `src/db/query/script.rs`의 `is_comment_or_blank` 판정을 보정해,
    - 블록 코멘트 시작(`/*`/`*/`)이더라도
    - `leading_keyword_after_comments` 결과가 존재하면 comment-only로 간주하지 않도록 변경했습니다.
- **효과**: 코멘트 prefix 뒤에 같은 줄에서 시작하는 서브쿼리 헤더(`SELECT`, `WITH`, `VALUES`, DML head)가 정상적으로 nested depth에 반영됩니다.

### [테스트] 회귀 테스트 추가
- `test_line_block_depths_detects_subquery_after_leading_block_comment_with_sql_same_line`
- `test_line_block_depths_detects_subquery_after_leading_hint_comment_with_sql_same_line`

### [검증]
- `cargo test test_line_block_depths_detects_subquery_after_leading_block_comment_with_sql_same_line -- --nocapture`
- `cargo test line_block_depths_detects_subquery_after -- --nocapture`
- `cargo test`

## 2026-02-26 결과 테이블 편집/저장 이벤트 경합 추가 보완 (save 성공 응답 SQL 누락 케이스)

### [중] save 성공 응답에서 SQL 텍스트가 비어 있으면 pending 잠금이 남을 수 있던 경로 수정
- **증상**: 결과 테이블 편집 저장(save) 후, 드물게 성공 응답이 `result.sql=""` 형태로 들어오면 기존 판정 로직이 save terminal 결과로 인식하지 못해 `pending_save_request`가 배치 정리 시점까지 유지될 수 있었습니다.
- **원인**:
  - 기존 fallback은 `취소/타임아웃/연결손실` 실패 메시지 중심이라, SQL이 누락된 성공 응답은 매칭하지 않았습니다.
- **수정**:
  - `ResultTableWidget::is_pending_save_terminal_result`에
    - `non-select + empty SQL + save tracking metadata 존재` 조건에서,
    - 성공 메시지가 DML/COMMIT/ROLLBACK/PLSQL 성공류(`row(s) affected`, `statement executed successfully`, `commit complete` 등)일 때도 terminal로 판정하는 분기를 추가했습니다.
  - 오탐을 줄이기 위해 일반 성공 문구(`statement complete`)는 그대로 미매칭 유지했습니다.
- **효과**: 편집 입력/삭제/저장 직후 쿼리 성공 이벤트가 SQL 누락 형태로 들어와도 save pending이 즉시 해제되어 편집 체크/삽입/삭제/취소 버튼이 불필요하게 잠기지 않습니다.

### [테스트] 회귀 테스트 추가
- `display_result_clears_save_pending_for_terminal_success_with_empty_sql`
- `empty_sql_success_dml_message_matches_pending_save_fallback`

### [테스트 안정화] 누락된 포맷터 회귀 입력 파일 복구
- `cargo test`에서 실패하던 `format_sql_preserves_mega_torture_script`는 `test/mega_torture.txt` 입력 파일 누락으로 재현되었습니다.
- `test/test8.txt`와 동일한 스크립트를 `test/mega_torture.txt`로 복구해 테스트 입력을 정상화했습니다.

### [검증]
- `cargo test`

## 2026-02-26 결과 테이블 편집/쿼리 이벤트 경합 추가 보완 (콜백 큐 경합 시 편집 액션 차단 누락)

### [중] 쿼리 실행 중에도 결과 편집 콜백이 늦게 실행될 수 있던 경로 보강
- **증상**: 편집 버튼이 비활성화되기 직전에 콜백이 이벤트 큐에 들어간 경우, 쿼리 실행 중인데도 결과 테이블 편집 체크/삽입/삭제/저장/취소 액션이 실행될 수 있었습니다.
- **원인**:
  - 기존 보호는 `refresh_result_edit_controls()`의 UI 비활성화 중심이어서, 콜백 실행 시점의 최종 상태를 강제 검증하지 않았습니다.
- **수정**:
  - `main_window`에 `validate_result_edit_action_allowed` + `clone_result_tabs_for_edit_action` 가드를 추가해, 편집 콜백 실행 직전에 `is_any_query_running()`을 공통 검증하도록 변경.
  - 차단 시 상태바 메시지/툴바 상태를 즉시 갱신하고 액션 실행을 중단.
  - 편집 체크(`CheckButton`) 오류 경로에서 강제 `clear()`를 제거하고, `refresh_result_edit_controls()` 기준으로 실제 상태와 UI를 동기화.
- **효과**: 편집 체크/입력/삭제/취소와 쿼리 실행/취소/실패 이벤트가 교차해도, 실행 중 쿼리와 결과 편집 상태가 엇갈려 staged 데이터가 예상치 않게 바뀌는 위험을 줄였습니다.

### [테스트] 회귀 테스트 추가
- `validate_result_edit_action_allows_when_no_query_is_running`
- `validate_result_edit_action_blocks_when_query_is_running`

### [검증]
- `cargo test validate_result_edit_action`
- `cargo test`

## 2026-02-26 결과 테이블 편집/실행 이벤트 경합 추가 보완 (조기 실패 시 탭 오프셋 오염)

### [중] `BatchStart` 없이 `StatementFinished`가 먼저 도착하는 조기 실패 경로에서 결과 탭 매핑 보정
- **증상**: 연결 급단절/초기 실행 실패처럼 `BatchStart` 이전에 `StatementFinished`가 먼저 도착하면, 이전 실행에서 남은 `result_tab_offset`가 재사용되어 결과가 잘못된 탭에 표시되거나 의도치 않은 새 탭이 생성될 수 있었습니다.
- **원인**:
  - `StatementStart`/`SelectStart`/`Rows`/`StatementFinished`에서 탭 인덱스를 단순히 `result_tab_offset + index`로 계산해, 배치 시작 이벤트가 누락된 예외 경로에서 stale 오프셋을 그대로 신뢰했습니다.
- **수정**:
  - 진행 이벤트별 탭 인덱스 계산 헬퍼(`resolve_progress_tab_index`)를 추가해:
    - `result_grid_execution_target`가 유효하면 해당 타깃 우선 사용
    - 그렇지 않으면 `result_tab_offset`을 현재 `tab_count` 범위로 clamp한 뒤 사용
  - `StatementStart`/`SelectStart`/`Rows`/`StatementFinished` 경로에 공통 적용.
- **효과**: 편집 체크/입력/삭제/취소 이후 쿼리 실행·취소·실패가 조기 오류와 교차해도, 결과 탭 매핑이 이전 실행 상태에 오염되어 엉뚱한 탭을 덮어쓰는 위험을 줄였습니다.

### [테스트] 회귀 테스트 추가
- `resolve_progress_tab_index_uses_valid_target_for_grid_execution`
- `resolve_progress_tab_index_clamps_stale_offset_when_target_is_missing`
- `resolve_progress_tab_index_keeps_batch_offset_when_tabs_grow`

## 2026-02-26 결과 테이블 편집 이벤트 경합 추가 보완 (stale backup 복구 오동작)

### [중] 편집 세션 없이 새 쿼리 시작 후 실패 시, 과거 backup이 복구되던 경로 차단
- **증상**: 이전 중단 실행에서 남은 `query_edit_backup`이 있는 상태에서, 현재 편집 세션 없이 새 쿼리를 시작한 뒤 실패/취소가 발생하면 오래된 staged edit가 복구될 수 있었습니다.
- **원인**:
  - `start_streaming()`이 활성 편집 세션이 없는 경우 기존 `query_edit_backup`를 유지해,
    이후 `display_result()` 실패 경로가 해당 backup을 현재 실행 실패로 오인해 복구할 수 있었습니다.
- **수정**:
  - `start_streaming()`에서 **활성 편집 세션이 없는 경우** stale `query_edit_backup`를 즉시 제거하도록 변경.
  - 이때만 backup을 제거하고, 편집 세션이 있는 정상 시나리오에서는 기존처럼 backup 스냅샷을 유지.
- **효과**: 편집 체크/입력/삭제/취소 이후 쿼리 실행·취소·실패 이벤트가 교차해도, 현재 실행과 무관한 과거 staged edit가 예기치 않게 되살아나는 문제를 방지합니다.

### [테스트] 회귀 테스트 추가
- `start_streaming_without_edit_session_clears_stale_backup_before_failure_result`

## 2026-02-26 결과 테이블 편집/저장 이벤트 경합 추가 보완

### [중] Save 실패/취소 응답의 SQL 형태 변형(블록→단일 statement) 시 pending 잠금이 남을 수 있던 경로 수정
- **증상**: 결과 테이블 편집 저장 시 다건 DML이 `BEGIN ... END` 블록으로 실행될 때, 실패 응답이 태그/블록 SQL이 아닌 내부 단일 DML SQL로 전달되면 save 완료 매칭이 실패해 `pending_save_request`가 필요 이상 유지될 수 있었습니다.
- **원인**:
  - 기존 매칭은 `request tag` 또는 블록 기준 SQL 시그니처 중심이라, 응답 SQL이 내부 statement로 축약되는 경로를 충분히 흡수하지 못했습니다.
- **수정**:
  - 저장 시작 시 블록 시그니처와 별도로 각 DML statement의 정규화 시그니처 목록(`pending_save_statement_signatures`)을 함께 보관하도록 확장.
  - `display_result`의 save terminal 판정에 statement 시그니처 매칭을 추가해, 태그/블록 시그니처가 누락된 실패 응답도 안전하게 save 종료로 처리.
  - `begin_edit_mode`, `cancel_edit_mode`, `clear`, `clear_orphaned_save_request`, save 콜백 실패 경로 등 상태 전환 지점에서 새 추적 메타데이터를 함께 정리하도록 보강.
- **효과**: 편집 입력/삭제 후 저장, 쿼리 실패/취소 이벤트가 교차할 때 save pending 잠금이 불필요하게 남아 편집 액션이 막히는 시나리오를 줄였습니다.

### [테스트] 회귀 테스트 추가
- `display_result_clears_save_pending_when_statement_signature_matches`
- `pending_save_terminal_matches_statement_signature_when_block_signature_differs`
- `pending_save_terminal_does_not_match_statement_signature_for_select_packets`

### [검증]
- `cargo test result_table::`
- `cargo test`

## 2026-02-25 결과 테이블 편집 이벤트 경합 안정화

### [중] `SELECT` 실패/취소 시 staged edit 유실 가능성 수정
- **증상**: 결과 테이블 편집 모드에서 일반 쿼리 실행 후 `SELECT`가 실패/취소되면, `start_streaming`에서 편집 세션이 먼저 해제되어 staged edit를 복구하지 못할 수 있었습니다.
- **원인**:
  - `StatementFinished`의 `SELECT` 경로가 `display_result`를 통과하지 않아 편집 보존/복구 로직이 누락됨.
  - `start_streaming` 시점에 편집 세션/데이터가 바로 초기화되어 실패 시 되돌릴 원본 상태가 사라짐.
- **수정**:
  - `ResultTableWidget`에 쿼리 전환용 백업 상태(`query_edit_backup`)를 추가해, 편집 세션이 있는 상태에서 `SELECT` 스트리밍 시작 시 원본 편집 상태를 저장.
  - `display_result`에서 일반 쿼리 실패/취소 시 백업이 있으면 편집 세션/헤더/데이터/source SQL을 원복하도록 보강.
  - `BatchFinished`에서 최종 결과 이벤트가 누락된 경우를 대비해 고아 백업 복구 경로(`clear_orphaned_query_edit_backup`)를 추가.
  - `main_window`의 `StatementFinished` 처리에서 `SELECT`도 `finish_streaming` 후 `display_result`를 호출하도록 통일.
  - 더 이상 사용되지 않는 `set_source_sql` 메서드(`result_tabs`, `result_table`) 제거.
- **효과**: 편집 체크/입력 후 쿼리 실행, 쿼리 취소, 쿼리 실패 이벤트가 교차해도 staged edit가 예기치 않게 소실되지 않고 복구됩니다.

### [테스트] 회귀 테스트 추가
- `display_result_restores_staged_edits_after_select_failure_during_streaming`
- `clear_orphaned_query_edit_backup_recovers_select_start_interruption`

### [검증]
- `cargo test` 전체 통과

## 2026-02-25 결과 테이블 편집 이벤트 경합 안정화 (후속)

### [중] save pending 중 `start_streaming` 진입 시 인라인 편집값 유실/그리드 초기화 가능성 수정
- **증상**: save 응답 대기 중(`pending_save_request=true`)에 예외적으로 `start_streaming` 이벤트가 들어오면, 활성 인라인 편집 입력값이 커밋되지 않고 위젯이 제거되거나 그리드 상태가 예상치 못하게 초기화될 수 있었습니다.
- **원인**: `start_streaming`이 save pending일 때 편집 세션 스냅샷을 건너뛰면서 인라인 편집 커밋 경로를 타지 않았고, 스트리밍 초기화 로직이 동일하게 동작했습니다.
- **수정**:
  - `start_streaming` 초입에서 save pending 여부와 무관하게 활성 인라인 편집을 먼저 커밋하도록 정리.
  - save pending 중에는 스트리밍 초기화/행 반영을 건너뛰고 기존 staged 그리드 상태를 유지하도록 가드 추가.
  - `append_rows`/`flush_pending`에도 save pending 가드를 추가해 out-of-order row 이벤트가 staged 데이터에 섞이지 않도록 보강.
- **효과**: 편집 입력, 저장 요청, 쿼리 이벤트가 교차해도 마지막 셀 입력 유실 및 그리드 상태 오염 가능성이 줄어듭니다.

### [중] save 응답 매칭을 SQL 시그니처 기반으로 보강
- **증상**: save 결과 식별을 단일 bool(`pending_save_request`)에 의존하면, 교차 이벤트에서 비-save 결과를 save 결과로 오인할 여지가 있었습니다.
- **원인**: `display_result`가 pending flag만 보고 save 후처리를 수행했습니다.
- **수정**:
  - save 시작 시 실행 SQL의 정규화 시그니처(`pending_save_sql_signature`)를 저장.
  - `display_result`에서 결과 SQL 시그니처가 일치할 때만 save 응답으로 소비하도록 변경.
  - `clear_orphaned_save_request`, `clear`, `begin/cancel_edit_mode` 등 상태 전환 지점에서 시그니처를 함께 정리.
- **효과**: 쿼리 취소/실패/재실행 경계에서 save 상태 오판으로 인한 편집 세션 종료/복구 오류 위험이 낮아집니다.

### [테스트] 회귀 테스트 추가
- `display_result_ignores_non_matching_result_while_save_is_pending`
- `canonical_sql_signature_normalizes_whitespace_and_trailing_semicolon`
- `matches_pending_save_signature_uses_normalized_sql`

### [검증]
- `cargo test` 전체 통과 (880 passed, 12 ignored)

## 중(이상) 우선 수정

### [중] Clippy 경고(문서 주석 위치)로 인한 품질 게이트 실패
- **증상**: `cargo clippy --all-targets --all-features -- -D warnings` 실행 시 `src/sql_text.rs`의 파일 상단 doc comment 다음 빈 줄로 인해 `clippy::empty_line_after_doc_comments` 에러 발생.
- **원인**: 파일 모듈 설명을 outer doc(`///`)로 선언해 실제 함수 doc로 해석되는 형태였고, 뒤에 빈 줄이 있어 lint 위반.
- **수정**: 파일 상단 주석을 모듈 내부 문서 주석(`//!`)으로 변경.
- **효과**: 해당 lint 에러는 해소됨.

## 2026-02-24 결과 테이블 그리드 편집 기능 개선

### [중] `Save` 실행 콜백 누락 시 staged edit가 소실되던 문제 수정
- **증상**: 결과 테이블 편집 모드에서 `Save`를 눌렀을 때 SQL 실행 콜백이 연결되지 않은 경우에도, 에러 반환 없이 세션이 종료되어 staged edit가 사라질 수 있었습니다.
- **원인**: `try_execute_sql`이 내부에서 알림만 띄우고 성공/실패를 호출자에 반환하지 않았고, `save_edit_mode`가 SQL 전달 성공 여부와 무관하게 `edit_session`을 먼저 해제했습니다.
- **수정**:
  - `try_execute_sql`을 `Result<(), String>` 반환으로 변경해 콜백 누락을 명시적으로 전파하도록 수정.
  - `save_edit_mode`에서 SQL 전달 성공 후에만 `edit_session`을 종료하도록 순서를 조정.
  - 다이얼로그 기반 편집 경로(`show_update_cell_dialog`, `show_delete_row_dialog`, `show_insert_row_dialog`)도 콜백 오류를 사용자에게 알리도록 보강.
- **효과**: 실행 경로가 준비되지 않은 상태에서 저장해도 staged edit가 조용히 유실되지 않고, 오류를 확인한 뒤 같은 세션에서 재시도할 수 있습니다.

### [테스트] SQL 실행 콜백 오류 전파 회귀 테스트 추가
- `try_execute_sql_returns_error_when_callback_is_missing`
- `try_execute_sql_invokes_registered_callback`

### [중] 문자열 셀 편집 시 앞/뒤 공백이 저장 SQL에서 소실되던 문제 수정
- **증상**: 결과 테이블 편집 모드에서 문자열 셀 값을 수정할 때, 입력값 앞/뒤 공백이 있어도 `Save` 시 생성되는 SQL literal에서 공백이 제거되어 의도와 다른 값으로 저장될 수 있었습니다.
- **원인**: `sql_literal_from_input`이 문자열 경로에서도 `trim()`된 값을 그대로 quote 처리해, 문자열의 유효 공백까지 함께 제거했습니다.
- **수정**:
  - `sql_literal_from_input`에서 `NULL/수치/표현식(=...)` 판별은 기존처럼 trim 기반으로 유지하고,
  - 일반 문자열 literal 생성은 원본 입력(`input`)을 quote 하도록 변경해 공백을 보존했습니다.
- **효과**: 그리드 편집에서 문자열 값의 의미 있는 leading/trailing whitespace가 저장 SQL에 정확히 반영됩니다.

### [테스트] 문자열 공백 보존 회귀 테스트 추가
- `sql_literal_from_input_preserves_significant_string_whitespace`

## 2026-02-23 그리드(결과 테이블) 편집 안정화

### [중] Enter/KPEnter/F2 단축키로 셀 편집 진입 및 빈 삽입 행 롤백 보완
- **증상**: 결과 테이블에서 편집 모드 중 셀 단일 선택 시 키보드 진입으로 즉시 편집을 시작할 수 없었고, 행 추가 후 첫 입력을 취소하면 임시 행이 세션 데이터에 남아 삭제/저장 플로우에 영향을 줄 수 있었습니다.
- **수정**:
  - `table.handle`의 `Event::KeyDown`에서 `Enter`, `KPEnter`, `F2`를 단일 셀 편집 진입 키로 허용하고, 활성 편집 모드에서 `show_inline_cell_editor`를 즉시 호출하도록 개선했습니다.
  - `insert_row_in_edit_mode`에서 삽입 창 취소 시 새로 추가된 임시 행을 `full_data`와 `row_states`에서 제거해 화면/상태를 원복하고,
  - 편집 세션이 동시 소멸되는 경우에도 삽입 직후 `full_data`를 롤백해 일관성을 보장했습니다.
- **효과**: 키보드 편집 진입이 안정화되고, 빈 삽입 행이 잔류하는 상태 오염을 방지해 staged edit 동작의 신뢰성이 향상됩니다.

### [중] 선택 범위 바운드 미검증으로 인한 편집 액션 오동작 보완
- **증상**: 데이터 수가 줄거나 변경된 뒤에도 `Table`의 선택 상태가 이전 범위를 가리키면, 복사/붙여넣기/단일 셀 업데이트 동작이 실제 행·열 범위를 벗어난 값으로 처리되어 잘못된 셀 수 계산이나 무의미한 편집 시도가 발생할 수 있었습니다.
- **수정**: 선택 범위 정규화에 테이블 행/열 한계를 적용하는 클램프 경로를 추가하고(표 범위 밖 선택은 교집합 없으면 무시), 아래 경로에 적용했습니다.
  - `selected_anchor_cell`
  - `selected_row_range`
  - `paste_clipboard_text_into_edit_mode`
  - `copy_selected_to_clipboard`
  - `copy_selected_with_headers`
  - `get_selected_data`
  - `resolve_update_target_cell`
- **효과**: 정렬/삭제/새로고침 후 잔존 선택 상태에서 셀 편집 기능이 잘못된 범위를 따라가며 동작하지 않고, 유효 범위 기반으로 일관되게 동작합니다.
- **테스트**: `normalized_selection_bounds_with_limits` 및 `resolve_update_target_cell` 관련 단위 테스트를 추가했습니다.

## 추가 확인 사항
- 전체 clippy에는 기존 코드 전반의 다수 lint(`unnecessary_map_or`, `arc_with_non_send_sync`, `items_after_test_module` 등)가 남아 있습니다. 이번 작업에서는 요청 즉시 조치 대상으로 확인된 항목을 우선 수정했습니다.

### [중] Clippy 경고(`filter(...).next_back()`/`map_or(false, ...)`) 정리
- **증상**: `cargo clippy --all-targets --all-features -- -D warnings`에서 `src/db/query/executor.rs`의 `clippy::filter_next`, `clippy::unnecessary_map_or`가 다수 보고됨.
- **원인**: 역방향 탐색 패턴을 `filter().next_back()`로 작성했고, `Option` 비교에서 `map_or(false, ...)`를 반복 사용.
- **수정**:
  - 역방향 탐색을 `iter().rfind(...)`로 치환.
  - `map_or(false, predicate)`를 `is_some_and(predicate)`로 치환.
- **효과**: 해당 구간의 Clippy 경고가 제거되어 품질 게이트 실패 요인 일부를 해소.
## 2026-02-22 추가 다건 수정 내역

### [중] DDL 오브젝트 타입 파싱 분기 단순화 및 오탐 여지 축소
- **증상**: DDL 헤더 파싱에서 이전 statement 탐색이 `filter().next_back()` 형태로 표현되어 가독성이 떨어지고, 조건식 유지보수 시 실수 가능성이 컸음.
- **수정**: 이전 span 검색을 `rfind(...)`로 변경하고, 헤더 키워드 검사 로직의 `map_or(false, ...)` 체인을 `is_some_and(...)`로 정리.
- **효과**: 조건 의도가 명확해져 DDL 타입 판별 분기 수정 시 회귀 가능성을 낮춤.

### [하] 인텔리센스 테스트의 불필요한 임시 벡터 제거
- **증상**: 테스트에서 단일 테이블 전달 시 `Some(&vec![...])`를 사용해 불필요한 힙 할당 발생.
- **수정**: `Some(&[...])` 슬라이스로 교체.
- **효과**: 테스트 코드 단순화 및 불필요한 할당 제거.
1. 연결 저장 실패 시 keyring 롤백 실패가 묵살되던 문제를 보완했습니다.
   - `DialogMessage::Save`와 `DialogMessage::Connect(save_connection=true)` 경로에서
     `delete_password` 실패를 무시하지 않고 에러 메시지에 함께 노출하도록 수정했습니다.
   - 이제 설정 저장 실패와 keyring 롤백 실패가 동시에 발생하면 사용자에게 두 실패 원인이 모두 표시됩니다.

## 2026-02-22 추가 다건 수정 내역 (2)

### [중] 연결 삭제 시 keyring 오류가 UI 플로우를 실패로 만들던 문제 수정
- **증상**: 저장된 연결 삭제 시, 실제 설정 목록에서는 연결이 제거되었더라도 keyring 삭제 실패가 있으면 전체 동작이 `Err`로 전달되어 UI에서 삭제 실패처럼 보일 수 있었음.
- **수정**: `AppConfig::remove_connection` 경로를 정리해, 연결 목록 정리는 항상 우선 완료하고 keyring 삭제 실패는 경고 로그/표준에러로 기록한 뒤 성공(`Ok`)을 유지하도록 변경.
- **효과**: keyring 백엔드 일시 오류가 있어도 사용자는 연결 항목을 정상적으로 삭제 가능.

### [중] 삭제된 연결이 마지막 선택 연결로 남는 상태 불일치 수정
- **증상**: `last_connection`이 삭제 대상 이름과 동일할 때도 값이 유지되어, 다음 실행에서 존재하지 않는 연결명이 마지막 연결로 남을 수 있었음.
- **수정**: 연결 삭제 시 `last_connection`이 삭제 대상이면 `None`으로 정리하도록 보강.
- **효과**: 최근 연결 상태와 마지막 선택 연결 상태 간 불일치 제거.

### [하] 불필요한 keyring 삭제 호출 제거
- **증상**: 존재하지 않는 연결명 삭제 요청에도 keyring 삭제를 시도해 불필요한 호출/오류 로그가 발생할 수 있었음.
- **수정**: 실제로 연결 항목이 존재해 제거된 경우에만 keyring 삭제를 시도하도록 조건 추가.
- **효과**: 불필요한 keyring 호출 감소 및 노이즈 로그 완화.

### [테스트] 회귀 방지 단위 테스트 추가
- 삭제 시 마지막 연결 정리,
- keyring 실패 시에도 리스트 제거 성공 유지,
- 미존재 연결 삭제 시 keyring 미호출
시나리오를 검증하는 테스트를 `src/utils/config.rs`에 추가.

## 2026-02-22 DBA 유저 기능 다건 수정

### [중] Security 뷰 로드 시 불필요한 Profile 검증으로 조회가 막히던 문제 수정
- **증상**: Role/System/Object/Summary 뷰에서도 `Profile` 입력값을 항상 검증해, `Profile` 칸에 과거 오타가 남아 있으면 사용자 조회가 실패할 수 있었음.
- **수정**: 뷰별 입력 정규화 로직(`normalize_security_view_filters`)을 추가해 Users/Profiles 뷰에서만 Profile 필터를 검증/적용하도록 변경.
- **효과**: User 기반 보안 조회(요약/권한/오브젝트)가 Profile 입력 상태와 독립적으로 동작.

### [중] Security 결과 행 선택 자동 채움이 컬럼 순서 의존이던 문제 수정
- **증상**: 자동 채움이 `row.get(1)`, `row.get(2)` 같은 고정 인덱스를 사용해 쿼리 컬럼 순서가 달라지면 User/Role/Profile에 잘못된 값이 들어갈 수 있었음.
- **수정**: 컬럼명 기반 파서(`security_autofill_values`)로 전환해 `USERNAME/GRANTEE`, `GRANTED_ROLE/PRIVILEGE`, `PROFILE`을 안전하게 추출하도록 변경.
- **효과**: 뷰별 SELECT 컬럼 순서 변경이나 변형 쿼리에서도 자동 입력 회귀 위험 감소.

### [테스트] Security 입력/자동채움 회귀 테스트 추가
- RoleGrants 모드에서 Profile 입력값이 무시되는지 검증.
- Users 모드에서 컬럼 순서가 뒤바뀐 행에서도 USERNAME/PROFILE을 정확히 자동 채움하는지 검증.
## 2026-02-22 추가 다건 수정 내역 (3)

### [중] 히스토리/오류 메시지의 user-only URI 자격증명 미마스킹 수정
- **증상**: `https://user@host/...` 형태는 `:`가 없는 userinfo라는 이유로 그대로 노출되어, 히스토리/오류 메시지에 계정 식별자가 남을 수 있었음.
- **수정**: URI 마스킹 로직에서 user-only authority도 `user:<redacted>@host`로 통일 마스킹하도록 변경.
- **효과**: URI 내 자격증명 표기 변형(`user:pass@host`, `user@host`) 모두에서 민감 정보 노출 위험 감소.

### [하] crash log 소비 후 파일 삭제 실패가 묵살되던 문제 보완
- **증상**: crash log를 읽은 뒤 파일 삭제(`remove_file`) 실패가 무시되어, 장애 분석/재현 시 stale crash log가 반복 노출될 수 있었음.
- **수정**: 삭제 실패 시 경로/원인 에러를 표준에러로 기록하도록 변경.
- **효과**: 운영 환경에서 crash log 수명주기 이상 징후를 추적 가능.

## 2026-02-22 추가 다건 수정 내역 (4)

### [중] 저장 연결 선택 시 keyring 미저장 비밀번호가 빈 문자열로 덮이던 문제 수정
- **증상**: 저장된 연결을 선택할 때 keyring에 비밀번호가 없는 경우 `unwrap_or_default()`로 `""`가 주입되어, 사용자가 이미 입력해 둔 비밀번호가 의도치 않게 지워질 수 있었음.
- **수정**: keyring 조회 결과가 `None`이면 기존 입력값을 유지하는 보조 함수(`resolved_password_for_saved_connection`)를 추가하고, 선택 콜백에서 해당 로직을 사용하도록 변경.
- **효과**: keyring 미등록 상태에서도 사용자가 수동 입력한 비밀번호가 유지되어 연결 시도 실패/혼란을 줄임.

### [중] 저장 연결 더블클릭 즉시 연결 시 빈 비밀번호로 시도되던 문제 수정
- **증상**: keyring 조회는 성공했지만 저장 비밀번호가 비어 있는 경우, 더블클릭 시 빈 비밀번호로 즉시 연결이 시도되어 불필요한 실패가 발생할 수 있었음.
- **수정**: 더블클릭 직전 비밀번호가 비어 있으면 연결 시도를 중단하고 안내 메시지를 표시하도록 가드 추가.
- **효과**: 잘못된 자격증명으로 즉시 연결 요청을 보내는 경로를 차단.

### [중] 연결 삭제 후 저장 실패 시 `last_connection` 롤백 누락 문제 수정
- **증상**: 삭제 후 `cfg.save()` 실패 시 연결 목록만 복원하고 `last_connection` 등 다른 설정 필드는 복원되지 않아 메모리 상태 불일치가 남을 수 있었음.
- **수정**: 삭제 직전 `AppConfig` 전체를 clone한 뒤 저장 실패 시 전체 설정(`*cfg = previous_config`)을 원복하도록 변경.
- **효과**: 저장 실패 시 설정 상태 일관성이 유지되어 후속 동작 회귀 위험 감소.

### [테스트] 저장 연결 비밀번호 해석 로직 회귀 테스트 추가
- keyring 비밀번호 존재 시 우선 적용,
- keyring 비밀번호 부재 시 기존 입력값 유지
시나리오를 `src/ui/connection_dialog.rs` 테스트에 추가.

## 2026-02-22 DBA 유저 기능 다건 수정 (추가)

### [중] Quick Action 기본값이 즉시 권한 변경 동작으로 실행되던 UX 버그 수정
- **증상**: Security Manager의 Quick Action 드롭다운 기본값이 `Grant Role`이라, 사용자가 항목 선택 없이 바로 실행하면 의도치 않은 권한 변경 요청이 전송될 수 있었음.
- **수정**: Quick Action 첫 항목을 `Select action...` 플레이스홀더로 추가하고, 액션 매핑 인덱스를 1~12로 재정렬.
- **효과**: 명시적 액션 선택 전에는 실행되지 않아 오조작 가능성을 줄임.

### [중] Security 뷰별 필드 사용 규칙을 공용 헬퍼로 통일
- **증상**: `user/profile` 필터 사용 조건이 함수별로 분산되어 있어, 추후 뷰 추가/수정 시 일부 경로만 갱신되는 회귀 위험이 있었음.
- **수정**: `security_mode_uses_user/role/profile` 헬퍼를 도입해 필터 정규화 및 자동채움 로직이 동일 규칙을 공유하도록 정리.
- **효과**: 뷰별 입력 사용 정책이 한 곳에서 관리되어 유지보수성과 일관성이 향상됨.

### [하] Security 액션 힌트 문구와 테스트를 새 인덱스 규칙에 맞게 보강
- **증상**: Quick Action 인덱스 재배치 전제의 힌트/테스트가 남아 있어, 기본값/마지막 항목 검증이 실제 UI와 어긋날 수 있었음.
- **수정**: 힌트 매핑을 `Select action...` 포함 버전으로 갱신하고, 관련 단위 테스트를 업데이트/추가.
- **효과**: UI 라벨-동작 매핑 변경 시 회귀를 테스트에서 조기 감지 가능.
## 2026-02-22 추가 다건 수정 내역 (5)

### [중] CONNECT 감지 오탐(주석/문자열) 수정
- **증상**: 실행 전 부트스트랩 명령 감지(`has_connection_bootstrap_command`)가 라인 단위 텍스트를 직접 검사해, `/* CONNECT ... */` 주석이나 `'CONNECT ...'` 문자열도 연결 명령으로 오탐할 수 있었음.
- **수정**: 감지 경로를 `QueryExecutor::split_script_items` 기반으로 전환해 실제 파싱된 `ToolCommand`만 대상으로 판단하도록 변경.
- **효과**: 주석/리터럴 내 CONNECT 텍스트로 인한 잘못된 연결 허용/분기 진입을 방지.

### [중] 커서 오프셋 경계 미검증으로 인한 UTF-8 경계 취약점 보완
- **증상**: `statement_at_cursor`/`statement_bounds_in_text`가 전달받은 `cursor_pos`를 그대로 사용해, 잘못된 mid-byte 오프셋이 들어오면 하위 경계 계산에서 잠재적 오류/예상치 못한 동작 여지가 있었음.
- **수정**: `clamp_cursor_to_char_boundary`를 추가해 `cursor_pos`를 문자열 길이 내로 clamp하고, UTF-8 유효 경계(`is_char_boundary`)로 보정 후 실행기 호출.
- **효과**: 바이트 오프셋 정책을 준수하며, 잘못된 커서 입력에서도 안전하게 가장 가까운 유효 경계로 복구.

### [테스트] 회귀 테스트 추가
- 주석/문자열 내 CONNECT 오탐 방지,
- mid-byte 커서 보정,
- out-of-bounds 커서 보정,
- 경계 보정 유틸 동작
시나리오를 `src/ui/sql_editor/query_text.rs` 테스트로 추가.
### [중] 자동완성 언어 아이템 후보 생성 중 불필요한 대문자 변환/할당 제거
- **증상**: `get_suggestions`에서 키워드/함수 후보를 추가할 때마다 `to_uppercase()`/`format!` 기반 비교를 반복해, 입력마다 발생하는 hot path 할당 비용이 커질 수 있었음.
- **수정**: SQL 키워드/함수는 원본이 이미 대문자라는 점을 활용해 직접 비교로 exact-match 제외를 처리하고, dedup key도 재가공 없이 재사용하도록 정리.
- **효과**: 자동완성 후보 계산 시 문자열 재할당 빈도를 낮춰 입력 반응성 개선.

### [중] 하이라이트용 컬럼 집계에서 중복 대문자 변환 제거
- **증상**: `get_all_columns_for_highlighting`가 `String::to_uppercase()`를 반복 호출해 컬럼 수가 많을수록 불필요한 변환/할당이 누적됐음.
- **수정**: 이미 인덱싱 단계에서 보유 중인 `NameEntry.upper`를 직접 재사용하고, `HashSet<&str>`로 dedup하여 임시 문자열 생성 없이 처리.
- **효과**: 컬럼 하이라이트 데이터 생성 비용 감소.

### [하] 테이블 컬럼 캐시 갱신 시 불필요한 `Vec<String>` clone 제거
- **증상**: `set_columns_for_table`에서 동일 컬럼 벡터를 map 저장/엔트리 생성에 각각 사용하면서 `columns.clone()`이 항상 발생했음.
- **수정**: 엔트리는 먼저 생성하고 원본 `columns` 소유권을 map에 그대로 이동시키도록 순서를 조정.
- **효과**: 메타데이터 캐시 갱신 시 불필요한 전체 벡터 복사를 제거.

## 2026-02-22 DBA 유저 기능 다건 수정 (추가)

### [중] 타 사용자 조회 시 USER_* fallback이 현재 로그인 사용자 데이터로 오인될 수 있던 문제 수정
- **증상**: `dba_role_privs`/`dba_sys_privs` 조회 실패 시 `user_role_privs`/`user_sys_privs`로 내려가면서도 요청 사용자명을 `WHERE USER='...'`로 필터링해, 사실상 필터가 무의미하고 현재 세션 사용자 데이터가 타 사용자 조회 결과처럼 보일 수 있었음.
- **수정**: USER_* fallback 전 `SELECT USER FROM dual`로 현재 세션 사용자와 요청 사용자를 비교하는 가드를 추가하고, 불일치 시 명시적 오류를 반환하도록 변경.
- **효과**: 권한 부족 환경에서 타 사용자 권한 조회가 잘못된 사용자 데이터로 오인되는 위험 제거.

### [중] DBA 유저/권한/프로파일 fallback 결과의 출처 식별 불가 문제 수정
- **증상**: `dba_*`에서 `all_*`/`user_*`로 fallback되어도 결과만 보면 출처 뷰를 구분하기 어려워 운영 판단이 혼동될 수 있었음.
- **수정**: users overview, role/system/object grants, profile limits 조회 결과에 `Source view: ...` 메시지를 일관되게 부여하도록 보강.
- **효과**: 동일 화면에서도 데이터 가시 범위(권한 스코프)를 즉시 식별 가능.

### [중] 다단 fallback 실패가 보안 입력 오류로 분류되던 문제 수정
- **증상**: 조회 실패 체인(`chained_fallback_error`)이 `InvalidArgument`로 래핑되어, 실제 DB 조회/권한 오류를 입력값 문제로 오인할 수 있었음.
- **수정**: fallback 체인 오류를 `OracleError::InternalError`로 분류해 원인 성격에 맞게 전달하도록 조정.
- **효과**: UI/로그에서 실패 원인 분류 정확도 향상.

### [중] Session Monitor 선택 파서가 INST_ID 파싱 실패를 조용히 무시하던 문제 수정
- **증상**: `INST_ID` 컬럼이 존재해도 숫자 파싱 실패 시 `None`으로 무시되어 인스턴스 정보 없는 세션 종료 경로로 진행될 수 있었음.
- **수정**: `INST_ID` 컬럼이 존재할 때 파싱 실패 시 선택 자체를 무효(`None`) 처리하도록 강화.
- **효과**: 잘못된 인스턴스 식별값으로 인한 오동작 가능성 감소.

### [중] Cursor Plan SQL_ID/CHILD 파서의 CHILD 기본값 강제 보정 제거
- **증상**: `CHILD_NUMBER/CHILD#` 컬럼이 없을 때 `0`으로 자동 보정되어 의도치 않은 child cursor 조회로 이어질 수 있었음.
- **수정**: CHILD 컬럼이 없으면 행 파싱을 실패 처리하도록 변경.
- **효과**: 컬럼 누락/스키마 변형 상황에서 잘못된 child cursor 조회 차단.
## 2026-02-22 추가 다건 수정 내역 (6)

### [중] 인텔리센스 함수 후보(`()`)가 키워드와 중복 제거되어 누락되던 버그 수정
- **증상**: `TO_` 같은 prefix에서 `TO_CHAR`는 보이지만 `TO_CHAR()` 함수 후보가 사라져, 함수 형태 자동완성 선택이 불가능했음.
- **원인**: 함수 후보 dedup key를 함수명(`TO_CHAR`)으로 사용해, 앞서 추가된 키워드 후보와 충돌하면서 `TO_CHAR()`가 drop됨.
- **수정**: 함수 후보 dedup key를 실제 렌더링 문자열(`TO_CHAR()`) 기준으로 변경하고, 최종 dedup도 입력 순서를 보존하도록 조정.
- **효과**: 같은 prefix에서 키워드/함수 후보가 모두 유지되어 자동완성 정확도 개선.

### [중] 에러 라인 파서가 `command line` 같은 문구를 라인 번호로 오탐하던 문제 완화
- **증상**: 에러 메시지 내 일반 문구(`command line 8`)가 실제 SQL 에러 라인으로 파싱될 수 있었음.
- **원인**: 패턴에 너무 포괄적인 `" line "`이 포함되어 문맥 구분 없이 숫자를 수집함.
- **수정**: 일반 패턴을 `" at line "`으로 축소해, 에러 문맥이 명확한 라인 참조만 우선 파싱하도록 조정.
- **효과**: 비에러 문맥 숫자 오탐 가능성을 줄이고 하이라이트 정확도 개선.

### [테스트] 회귀 테스트 추가
- `get_suggestions_keeps_to_underscore_matches`가 `TO_CHAR`와 `TO_CHAR()`를 모두 반환하는지 검증.
- `parse_error_line_ignores_non_error_line_wording`를 추가해 `command line` 노이즈를 배제하고 `at line` 라인을 선택하는지 검증.
### [중] 토큰 그룹 분리 시 불필요한 전체 depth 벡터 생성 제거
- **증상**: `split_top_level_symbol_groups` / `split_top_level_keyword_groups`가 매 호출마다 `paren_depths(tokens)`를 만들어 전체 토큰 길이만큼 추가 메모리를 할당하고 한 번 더 순회함.
- **수정**: depth 벡터 사전 생성 방식 대신, 단일 순회 중 `depth`를 직접 갱신하는 스트리밍 방식으로 변경.
- **효과**: 토큰 분할 hot path에서 메모리 할당과 2-pass 순회를 제거해 대용량 SQL에서 파싱 전처리 비용 감소.

### [중] SQL 파서/컨텍스트 분석의 대문자 변환 비용 절감
- **증상**: 키워드 판별 중심 로직에서 `to_uppercase()`를 광범위하게 사용해, 유니코드 케이스 매핑 비용과 추가 할당이 불필요하게 발생함.
- **수정**: 키워드 비교/명령 파싱 경로를 `to_ascii_uppercase()`로 전환.
- **효과**: ASCII 기반 SQL 키워드 처리 경로에서 문자열 정규화 비용을 낮춰 반복 파싱 성능 개선.

## 2026-02-22 추가 다건 수정 내역 (7)

### [중] 로그/히스토리 미리보기 문자열 정규화의 중복 순회/할당 축소
- **증상**: `truncate_message`/`truncate_sql`이 공백 정규화 후 `trim()` + `chars().count()` + `char_indices().nth(...)`를 추가로 수행해 문자열을 여러 번 순회했고, 잘라내기 시 `format!`으로 추가 할당이 발생했음.
- **수정**:
  - 정규화 단계에서 선행/연속 공백을 직접 제거하고 후행 공백을 `pop()`으로 정리하도록 변경.
  - 길이 판별/절단 지점을 단일 `char_indices()` 순회로 계산.
  - 접미 `...` 추가를 `String::with_capacity` + `push_str`로 처리해 불필요한 포맷팅 할당 제거.
- **효과**: 로그/히스토리 리스트 렌더링의 hot path에서 문자 스캔 횟수와 임시 문자열 할당이 줄어 입력/필터 반응성을 개선.

### [하] 로그 목록 필터 인덱스 벡터 사전 용량 예약
- **증상**: `populate_browser`의 인덱스 벡터가 기본 용량 0에서 시작해 엔트리 수 증가에 따라 재할당될 수 있었음.
- **수정**: `Vec::with_capacity(entries.len())`로 초기화.
- **효과**: 대량 로그 표시 시 재할당 횟수 감소.

## 2026-02-22 DBA 유저 기능 다건 수정 (추가)

### [중] 사용자 요약 조회에 데이터 출처 라벨이 빠져 운영 판단이 모호하던 문제 수정
- **증상**: `get_user_summary_snapshot`이 `dba_users`/`all_users` 어느 뷰를 사용해 반환했는지 메시지로 구분되지 않아, 권한 범위를 즉시 식별하기 어려웠음.
- **수정**: 사용자 요약 조회 결과에 `Source view: ...`를 부여하고, `all_users` 폴백 실패 시에도 원인 경로를 누적해 `User summary snapshot` 실패 메시지로 반환하도록 보강.
- **효과**: 화면에서 동일 사용자 요약이 `dba_users` 기반인지 `all_users` 기반인지 즉시 확인 가능해지고, 폴백 오류 추적이 쉬워짐.

### [중] DBA 뷰 fallback 판정을 에러 문자열 오탐 가능성에서 분리
- **증상**: `should_fallback_from_global_view`가 오류 문자열 패턴 검색까지 함께 사용해, 텍스트에 유사 코드가 섞일 때 오탐성 fallback가 발생할 수 있었음.
- **수정**: fallback 판단을 `extract_ora_error_code` 기반 ORA 코드 매칭으로 축소.
- **효과**: DBA 뷰 접근 실패 여부 판단이 더 명시적으로 동작해, 잘못된 폴백 경로 전환 가능성이 낮아짐.

## 2026-02-22 DBA 유저 기능 다건 수정 (8)

### [중] SQL Monitor 세션 타깃 파싱에서 INST_ID 문자열 정합성 깨짐 시 즉시 실패 처리
- **증상**: `INST_ID` 컬럼이 존재하지만 `-`/비숫자처럼 잘못된 값이 들어와도 `parse_sql_monitor_session_target`이 `(None, sid, serial)`를 반환해 RAC 환경에서 인스턴스 미지정 kill이 진행될 수 있었음.
- **수정**: `INST_ID` 컬럼이 존재할 때 파싱 실패하면 전체 세션 타깃 파싱을 `None` 처리하도록 변경해 kill 경로를 차단하도록 강화.
- **효과**: 인스턴스 식별이 모호한 상태에서 잘못된 세션 종료 시도를 방지.

### [중] RMAN 작업명 기본값 충돌 위험 완화
- **증상**: `default_rman_job_name`가 PID + 타임스탬프 + 증분 순번만 사용해, 프로세스 재시작 직후 동일 포맷이 극히 짧은 구간에서 중복될 수 있었음.
- **수정**: 프로세스 시작 시점 기준 토큰을 `OnceLock` 기반으로 1회 생성해 이름에 포함하고, `(prefix, pid, process_token, timestamp, sequence)` 형식으로 확대.
- **효과**: 동일 프로세스/동일 ms 구간에서도 충돌 가능성이 줄어 RMAN job/job restore 이름 중복 위험이 낮아짐.

### [중] `row.get(...).unwrap_or(None)` 기반의 DB 타입 변환 실패 은닉 제거
- **증상**: 일부 조회 루틴에서 `row.get(...).unwrap_or(None)`로 변환 실패를 `NULL` 문자열로 바꿔 버려 진짜 커넥션/권한/타입 이슈 원인을 가리고 있었음.
- **수정**: DBA 사용자/모니터링 조회에서 사용하는 행 파싱 경로를 `row.get(...)?`로 전환해 변환 실패를 즉시 상위로 전파.
- **효과**: 오류 추적성이 개선되고, 잘못된 결과/오탐 데이터 노출이 감소함.

## 2026-02-22 DBA 유저 기능 다건 수정 (9)

### [중] `Users` 뷰 fallback 시 Profile 필터가 빠지는 문제 수정
- **증상**: `DBA USERS` 조회가 `dba_users`에서 실패하고 `all_users`로 fallback할 때, `Profile` 입력값은 이미 유효성 검사에서 걸러지는데도 실제 fallback SQL에는 반영되지 않아 프로파일 필터가 무시되었음.
- **수정**: `get_users_overview_snapshot`의 `all_users` fallback WHERE 절에 `username`/`profile` 조건을 모두 적용하도록 변경.
- **효과**: DBA 권한이 없어도 `Users` 뷰에서 `profile` 기준 조회가 동작해 관리자가 의도한 범위를 유지할 수 있음.

### [중] Summary 모드에서 미사용 Profile 값으로 로드가 차단되던 문제 수정
- **증상**: `Summary` 로드 시 `Profile` 입력값이 조회에서 사용되지 않음에도 정규화 단계에서 검증돼, 잘못된 Profile 문자열이 있으면 `Summary` 조회가 실패함.
- **수정**: `normalize_security_view_filters`에서 `Profile` 유효성 검사 대상을 `Users`, `Profiles` 모드로 한정하고 `Summary`는 `Profile` 필터를 무시하도록 조정.
- **효과**: 잘못된 Profile 텍스트가 있어도 요약 조회는 정상 수행되며, 뷰별 입력 규칙이 실제 쿼리 동작과 일치.

## 2026-02-22 DBA 유저 기능 다건 수정 (10)

### [중] `dba_tab_privs` 폴백에서 현재 사용자 검증 누락 문제 수정
- **증상**: Object privileges 조회가 `dba_tab_privs`를 사용할 수 없을 때 `all_tab_privs`로 바로 이동하면서, 현재 세션 사용자가 아닌 타 사용자 요청도 실패 없이 현재 사용자 데이터로 오인될 수 있었음.
- **수정**: `get_user_object_grants_snapshot`에서 `dba_tab_privs` 폴백 전 `ensure_user_view_matches_target_user`를 호출해 대상 사용자 일치 여부를 확인.
- **효과**: 다른 사용자 권한 조회 시 오판 결과가 노출될 수 있던 동작을 차단하고, 실패 사유를 명확하게 안내.

### [중] DBA 사용자 조회 함수의 fallback 전환 조건 정밀화
- **증상**: 일부 DBA 사용자 조회 함수가 `dba_*` 쿼리 실패 시 오류 코드와 무관하게 바로 `all_*`/`user_*` 폴백을 시도해, 실제 DB 에러를 의미론적으로 은폐할 수 있었음.
- **수정**: `get_user_summary_snapshot`, `get_users_overview_snapshot`, `get_user_role_grants_snapshot`, `get_user_system_grants_snapshot`, `get_user_object_grants_snapshot`, `get_profile_limits_snapshot`에서 `should_fallback_from_global_view`로 폴백 가능 코드만 허용.
- **효과**: 조회 실패 원인 보존이 강화되고, 권한/뷰 접근 오류 외의 SQL 실행 오류는 즉시 상위로 전달되어 오탐/잘못된 fallback 동작을 줄임.

## 2026-02-22 DBA 유저 기능 다건 수정 (11)

### [중] Security 관리자에서 중요 역할 생성/삭제 동작의 실수 실행 위험 방지
- **증상**: Security Manager에서 `Create Role`, `Drop Role`, Quick `Create User/Role`이 입력만 있을 때 바로 실행되어, 버튼 클릭 실수나 잘못된 텍스트로 즉시 DDL이 수행될 수 있었음.
- **수정**: Quick Action 및 역할 버튼 경로에 사용자 확인 다이얼로그를 추가하고, 대상명 공백 검증을 선행해 실제 실행 전 취소할 수 있도록 변경.
  - `Create User` Quick Action: 사용자명 비어 있음 방지 + 생성 확인
  - `Create Role` Quick Action / 버튼: 역할명 비어 있음 방지 + 생성 확인
  - `Drop Role` Quick Action / 버튼: 역할명 비어 있음 방지 + 삭제 확인
- **효과**: 의도치 않은 권한 관련 DDL 실행 확률이 줄고, 잘못된 입력으로 인한 실패 루프를 즉시 차단 가능.

## 2026-02-22 DBA 유저 기능 다건 수정 (12)

### [중] RMAN 작업명 규격 테스트가 접두사 언더스코어를 오판하던 문제 수정
- **증상**: `default_rman_job_name`의 접미사 파트(타이밍/시퀀스) 존재를 검증하는 테스트가 `split('_')` 길이 비교에 의존해, 접두사에 언더스코어가 포함된 경우 실제로는 5개가 아닌 더 많은 구간으로 분리되어 실패했습니다.
- **수정**: 테스트를 접두사 전체를 보존하면서 마지막 4개 토큰만 역순으로 분리(`rsplitn`)해 검증하도록 변경했습니다.
- **효과**: 접두사 형태가 달라져도 RMAN 기본 작업명 생성 규격(접두사 + PID + 토큰 + 타임스탬프 + 시퀀스)이 안정적으로 검증됩니다.

### [하] DBA 사용자 작업 버튼에서 공백 사용자명 사전 차단
- **증상**: `Create User`/`Drop User` 버튼과 Quick Action의 `Drop User`는 사용자명 미입력 시 실행 메시지 루트를 거쳐 즉시 에러로 귀결되어 불필요한 처리만 수행했습니다.
- **수정**: 사용자명 미입력 시 백엔드 전송 전에 즉시 경고 다이얼로그로 차단하도록 사전 가드를 추가했습니다.
- **효과**: 빈 입력으로 인한 불필요한 비동기 작업 전송을 줄이고 잘못된 클릭 실수를 감소시켰습니다.

## 2026-02-22 DBA 유저 기능 다건 수정 (13)

### [중] `Users` 뷰 `all_users` 폴백에서 PROFILE 필터 미지원 버전에 대한 재시도 보강
- **증상**: `dba_users` 폴백 시 `all_users`에 `PROFILE` 컬럼이 없거나 접근 불가하면 ORA-00904가 발생하면서 `profile` 필터가 있을 때 사용자 목록 조회가 실패해 버림.
- **수정**: `get_users_overview_snapshot`에서 `all_users` 폴백 실패가 `ORA-00904`일 때 `profile` 조건을 제외한 동일 사용자 조건으로 1회 재시도하도록 변경하고, 실패 경로는 추적 가능한 메시지로 보강.
- **효과**: DBA 권한 제한 또는 뷰 스키마 차이 환경에서도 `Users` 뷰가 가능한 범위 내에서 동작을 복구.

### [중] 보안 결과 행 자동채움 시 값 누락 컬럼에 대한 stale 값 잔존 제거
- **증상**: Security 테이블 행을 선택할 때 일부 컬럼 값이 없으면 기존 입력값을 유지해 사용자/역할/프로파일 필드가 오염되어 잘못된 Quick/Action 실행으로 이어질 수 있었음.
- **수정**: `security_autofill_values` 적용 시 해당 컬럼 값이 없으면 해당 입력 필드를 빈 값으로 명시 초기화하고, 행 파싱 실패 시도 전체 입력을 초기화.
- **효과**: 행 변경 시 입력 상태 오염을 줄여 잘못된 값 기반 자동 실행 위험을 낮춤.

### [테스트] Users 뷰 조회 조건 빌더 회귀 테스트 추가
- `build_users_overview_where_clause`가 사용자명/프로파일/attention_only 조합에서 기대된 WHERE 절을 생성하는지 검증하는 단위 테스트를 추가해 폴백/필터 빌더 회귀를 조기에 탐지.

## 2026-02-22 DBA 유저 기능 다건 수정 (14)

### [중] Security 액션 경로에서 필수 입력값 미입력 전송 버그 수정
- **증상**: 빠른 실행(Quick Action) 및 버튼 동작에서 사용자/역할/권한/프로파일이 비어 있는 상태로도 메시지가 전송되어 백엔드 정규화에서만 오류가 반환되어 사용자가 동작 실패 원인을 한 단계 늦게 인지했습니다.
- **수정**: Quick Action(Grant/Revoke Role, System Privilege, Set Profile, Lock/Unlock/Expire)와 버튼 경로에 입력값 사전 검증을 추가해, 필수 항목이 비어 있으면 즉시 알림하고 전송하지 않도록 변경했습니다.
- **효과**: 불필요한 메시지 전송/스레드 동작을 줄이고, 사용자 입력 오류를 즉시 차단해 오조작 위험과 불필요한 처리 경로를 감소시킵니다.

### [하] 역할 생성/삭제 확인창 중복 제거
- **증상**: Role 생성/삭제를 UI 버튼과 백엔드 둘 다에서 확인창을 띄워, 같은 동작에서 확인 다이얼로그가 2회 연속 표시되는 UX 결함이 있었습니다.
- **수정**: UI 쪽 Role 생성/삭제의 중복 확인창을 제거하고 백엔드 확인 흐름으로 일원화했습니다(Quick Action 및 버튼 공통).
- **효과**: 사용자 확인 플로우가 단일화되어 의도치 않은 중복 클릭 부담이 사라지고 동작 예측성이 개선됩니다.

## 2026-02-22 DBA 유저 기능 다건 수정 (15)

### [중] Cursor Plan 선택 시 CHILD 컬럼 부재 행에서 SQL_ID가 채워지지 않던 문제 수정
- **증상**: `V$SQL`/`GV$SQL` 기반 결과에는 `CHILD_NUMBER`가 없을 수 있는데, 기존에는 해당 컬럼 부재면 `SQL_ID`까지 파싱 실패해 선택한 SQL_ID가 입력창에 자동 채워지지 않았습니다.
- **수정**: `parse_sql_id_child_row`를 `SQL_ID + Option<CHILD>` 형태로 개선하고, Child 컬럼이 없을 때는 `sql_id`만 채우고 `child`는 비움으로 처리하도록 변경했습니다.
- **효과**: 컬럼 구성이 다른 결과 집합에서도 최근 SQL 조회/로드 흐름이 멈추지 않고 동작합니다.

### [중] Data Pump Import FULL 모드에서 스키마 값 오유입 차단
- **증상**: FULL 모드에서는 스키마 필터가 허용되지 않는데, 사용자 입력 스키마 값이 그대로 전달되어 백엔드에서 즉시 `Schema filter must be empty when Data Pump job mode is FULL`로 실패했습니다.
- **수정**: Data Pump Import 요청 처리에서 `job_mode`를 먼저 정규화한 뒤 FULL 모드일 경우 `schema_name`을 강제로 `None`으로 변환해 executor로 전달하도록 보강했습니다.

## 2026-02-22 DBA 유저 기능 다건 수정 (16)

### [중] Data Pump 모드 검증 및 SCHEMA/FULL 스키마 규칙 불일치 수정
- **증상**: UI에서는 `SCHEMA/FULL` 모드를 안내하면서도 실행 엔진에는 `TABLE/TABLESPACE` 허용/거부가 혼재해 동작 경로가 모순되었고, Import는 SCHEMA 모드에서 스키마 미입력 시 런타임 에러로만 실패할 수 있었습니다.
- **수정**:
    - `QueryExecutor::start_datapump_job`에서 허용 모드를 `SCHEMA/FULL`로 축소하고 예외 메시지를 정합화했습니다.
    - `src/ui/sql_editor/dba_tools.rs`의 Data Pump Export/Import 처리에서 모드 정규화 헬퍼(`normalize_datapump_mode`)를 통해 지원 모드만 허용하고, SCHEMA 모드에서는 스키마를 필수로, FULL 모드에서는 스키마를 금지하도록 선검증했습니다.
    - Data Pump 시작 요청 시 정규화된 `normalized_mode`를 executor로 전달해 백엔드/UI의 검증 경로를 동일하게 유지했습니다.
- **효과**: FULL 모드에서의 불필요한 즉시 실패를 줄이고, 사용자 입력값과 실제 실행 파라미터의 불일치를 예방합니다.

## 2026-02-22 DBA 유저 기능 다건 수정 (17)

### [중] DBA 사용자 식별자 앞글자 숫자 시작 검증 누락 보완
- **증상**: `normalize_required_security_identifier`가 SQL 주입 차단은 했지만, 식별자 맨 앞이 숫자인 경우 에러 메시지 경로를 통과해 생성 SQL이 예상 범위를 벗어났습니다.
- **수정**: 식별자 정규화에서 ASCII 대문자 변환은 유지하되 시작 문자가 숫자인 경우를 조기에 차단하도록 하고, DBA 사용자/권한/역할 파서에서 동일한 규칙이 적용되도록 경로를 고정했습니다.
- **효과**: 사용자/역할/권한 이름이 Oracle 규칙에서 허용되지 않는 형태로 생성되거나 요청되는 경로를 차단했습니다.

### [중] `normalize_required_password`의 문법 오류 및 공백 비밀번호 처리 정합성 보완
- **증상**: 동일 함수에 남아 있던 `trimmed` 식별자 오염 코드(`if trimmed value...`)로 컴파일이 깨질 수 있었고, 비밀번호 공백 보존이 의도대로 동작하지 않을 가능성이 있었습니다.
- **수정**: `normalize_required_password`의 잘못된 참조를 제거하고, 입력된 비밀번호 원문을 그대로 반환해 앞뒤 공백을 보존하도록 정리했습니다.
- **효과**: 비밀번호가 공백을 포함한 경우 정상 처리되며, 동작 정합성이 개선됩니다.

### [테스트] DBA 사용자 정규화 회귀 테스트 추가
- `normalize_required_security_identifier`의 선두 숫자 차단 케이스를 추가했습니다.
- `normalize_required_password`가 양끝 공백을 유지하는지 확인하는 케이스를 추가했습니다.

## 2026-02-22 DBA 유저 기능 다건 수정 (18)

### [중] Security 식별자 검증 일관성 및 사용자 생성 입력 가드 보완
- **증상**: `Security` 화면의 사용자/역할/프로파일 정규화(`normalize_required_identifier`)가 앞글자 숫자 허용을 막지 못해 실행 단계에서만 실패가 발생했고, `CREATE USER`에서 공백 비밀번호가 백그라운드로 전달되어 액션 스레드가 불필요하게 동작한 뒤 실패했습니다.
- **수정**: 
  - `normalize_required_identifier`에 `시작 문자 숫자 제한` 검증을 추가해 잘못된 식별자를 UI 단계에서 즉시 차단했습니다.
  - `CREATE USER` 버튼/Quick Action에서 비밀번호 `trim` 검사로 `공백/빈 문자열`을 전송 전에 차단했습니다.
  - 추가 회귀 테스트로 선행 숫자 식별자 거부 케이스를 커버했습니다.
- **효과**: 사용자/역할/프로파일 입력 오입력과 무의미한 비동기 작업 전송이 줄고, DBA 유저 생성 플로우의 즉시 피드백이 개선됩니다.

## 2026-02-22 DBA 유저 기능 다건 수정 (19)

### [중] Security 뷰 전환/초기 로드 시 필터 검증 경로 통합
- **증상**: 사용자/프로파일 입력값이 뷰별로 무시되지 않아, 프로파일 입력값 잔류 상태에서 Role/System/Object/Summary 조회를 시도하면 불필요하게 로드가 중단되는 구간이 있었습니다.
- **수정**:
  - 뷰별 필터 규칙을 `normalize_security_view_filters`로 정리하고,
  - `SecurityMessage::LoadRequested` 전송은 모두 `enqueue_security_view_load`를 거치도록 통합했습니다.
  - 초기 로드, 버튼 이동, 뷰 변경 시 동일하게 검증 후 전송하도록 보정했습니다.
- **효과**: 뷰별로 필요한 입력만 검증되며, 비필요한 사용자 입력 유효성 오류로 인한 조회 실패를 줄였습니다.

### [중] DBA 액션 완료 후 재조회도 동일 검증으로 정합성 강화
- **증상**: 액션 성공 직후 자동 재조회가 기존 코드 경로를 그대로 사용해, 현재 뷰의 유효성 규칙과 충돌할 때 다시 즉시 실패하거나 불필요한 오류 메시지가 노출될 수 있었습니다.
- **수정**:
  - `ActionFinished` 성공 분기에서 재조회 요청도 `enqueue_security_view_load`를 사용하게 변경했습니다.
  - 재조회 조건 검증 실패 시 상태 라벨에 스킵 사유를 남기고, 채널 전송은 시도하지 않도록 보완했습니다.
- **효과**: 액션-조회 연속 흐름에서 검증 규칙 불일치로 인한 연쇄 오류가 줄고, 재시도 가능한 상태로 안정적으로 복귀합니다.

### [테스트] Security 뷰 필터 경로 회귀 방지
- `normalize_security_view_filters`의 뷰별 동작이 기대대로 유지되는지 확인하는 기존 테스트를 기반으로, UI 레벨 재조회 호출부는 동일 경로로 통일했습니다.

## 2026-02-23 결과 테이블 그리드 편집 기능 개선

### [중] 다중 선택 상태에서 `Update Cell`이 의도와 다른 셀/행을 갱신하던 문제 수정
- **증상**: 다중 셀 선택 상태에서 우클릭 `Update Cell` 실행 시, 클릭한 셀이 아니라 선택 영역 앵커(좌상단) 기준으로 업데이트 대상이 결정되어 다른 행/컬럼이 갱신될 수 있었음.
- **수정**: 컨텍스트 메뉴 호출 시 마우스 위치의 셀을 우선 편집 대상으로 전달하고, 컨텍스트 셀이 없는 경우에는 단일 셀 선택일 때만 업데이트를 허용하도록 `resolve_update_target_cell` 로직을 추가.
- **효과**: 우클릭 기준 셀 편집 동작이 직관적으로 일치하고, 다중 선택에서의 오갱신(잘못된 행/셀 수정) 가능성을 차단.

### [중] 결과 헤더가 `E.COL` 형태일 때 UPDATE/INSERT 컬럼 식별자가 깨지던 문제 수정
- **증상**: 컬럼 헤더가 `E.ENAME`처럼 수식/한정자 형태일 때 기존 로직이 그대로 `"E"."ENAME"`을 DML 컬럼에 사용해 `SET`/`INSERT` SQL이 실패할 수 있었음.
- **수정**: `last_identifier_segment`/`editable_column_identifier`를 추가해, DML 컬럼은 헤더의 마지막 식별자 세그먼트만 안전하게 추출해 사용하도록 변경.
- **효과**: 한정자 포함 헤더에서도 편집 SQL 생성 안정성이 개선되고, 잘못된 컬럼 식별자로 인한 즉시 실패를 줄임.

### [테스트] 결과 그리드 편집 회귀 테스트 추가
- `last_identifier_segment`의 한정자/인용 식별자 분리 동작 검증.
- `editable_column_identifier`의 DML 컬럼명 정규화 검증.
- `resolve_update_target_cell`의 컨텍스트 셀 우선/단일 셀 선택 강제 동작 검증.

## 2026-02-23 결과 테이블 그리드 편집 기능 개선 (후속)

### [중] `Delete Row` 대상 `ROWID`를 대소문자 무시로 중복 제거하던 문제 수정
- **증상**: 다중 행 삭제 시 `ROWID`를 `to_ascii_uppercase()` 기준으로 dedupe 하면서, 대소문자만 다른 유효 `ROWID`가 같은 값으로 합쳐져 일부 선택 행이 삭제 SQL에서 누락될 수 있었습니다.
- **수정**: `selected_rowids` 경로를 정리해 `ROWID` dedupe를 원문(trim 후) 기준으로 처리하고, 공백/중복 처리 로직은 `push_unique_rowid` 헬퍼로 통합했습니다.
- **효과**: 실제 `ROWID` 값이 대소문자를 구분하는 경우에도 선택한 행이 정확히 `DELETE` 대상에 반영됩니다.

### [중] 인용 식별자 기반 `ROWID` 별칭 해석 및 편집 SQL 식별자 quoting 안정성 보강
- **증상**: `SELECT "e"."ROWID" ...` 같은 쿼리에서 `ROWID` 토큰이 `"ROWID"`로 들어오면 별칭 해석이 실패해 다중 테이블 결과 편집 대상 테이블을 결정하지 못할 수 있었습니다. 또한 테이블명에 인용된 dot(`"A.B"`)이 포함되면 식별자 분해가 잘못될 여지가 있었습니다.
- **수정**:
  - `find_rowid_qualifier`에서 `ROWID` 판별 시 인용 제거 후 비교하도록 보완했습니다.
  - `quote_qualified_identifier`에 `split_qualified_identifier`를 추가해, 인용 구간 내부 dot는 분리하지 않도록 처리했습니다.
- **효과**: 인용 식별자를 사용하는 SQL에서도 `Update/Delete/Insert` 편집 SQL 생성이 더 안정적으로 동작합니다.

### [테스트] 결과 그리드 편집 회귀 테스트 보강
- `resolve_target_table_uses_quoted_rowid_alias_resolution` 추가.
- `quote_qualified_identifier_preserves_dots_inside_quoted_segments` 추가.
- `push_unique_rowid_preserves_case_sensitive_values` 추가.

## 2026-02-23 결과 테이블 그리드 편집 기능 개선 (후속 2)

### [중] 셀 외 영역 우클릭 시 이전 선택으로 편집 SQL이 실행될 수 있던 문제 수정
- **증상**: 결과 테이블에서 셀/행 헤더가 아닌 위치를 우클릭해 컨텍스트 메뉴를 열면, 현재 마우스 위치와 무관하게 이전 선택 상태로 `Update Cell`/`Delete Row`가 실행될 수 있어 오편집 위험이 있었습니다.
- **수정**:
  - 컨텍스트 메뉴 진입 조건을 보강해, 셀 또는 row header에서 우클릭한 경우에만 메뉴를 표시하도록 변경했습니다.
  - 셀 우클릭 시에는 선택 포함 판정을 정규화된 selection bounds 기준(`selection_contains_cell`)으로 처리해 선택 역방향/경계 케이스에서도 안정적으로 동작하도록 보강했습니다.
- **효과**: 마우스 위치와 다른 이전 선택을 재사용해 편집 SQL이 실행되는 오동작 가능성을 차단했습니다.

### [중] row header 우클릭 편집 대상 행 정합성 보강
- **증상**: row header 우클릭 시 선택이 명시적으로 갱신되지 않아, `Delete Row`/`Insert Row` 기본값이 사용자가 클릭한 행이 아닌 이전 선택 행을 참조할 수 있었습니다.
- **수정**:
  - `get_row_header_at_mouse`를 추가해 row header 우클릭 행을 정확히 식별합니다.
  - row header 우클릭 시 해당 행 전체를 즉시 선택하도록 변경했습니다.
  - row header 컨텍스트 메뉴에서는 `Update Cell`을 제외해 행 단위 동작과 메뉴 의미를 일치시켰습니다.
- **효과**: row header 기반 편집 동작이 클릭한 행 기준으로 일관되며, 행 컨텍스트에서의 오편집 가능성이 줄었습니다.

### [테스트] selection 경계 처리 회귀 테스트 추가
- `selection_contains_cell_normalizes_reversed_bounds` 추가.
- `selection_contains_cell_rejects_negative_or_empty_selection` 추가.

## 2026-02-23 결과 테이블 그리드 편집 기능 개선 (후속 3)

### [중] 편집 불가 결과에서 편집 메뉴가 노출되던 UX/오동작 가능성 수정
- **증상**: `ROWID`가 없거나 원본 SQL에서 단일 대상 테이블 해석이 불가능한 결과에서도 `Insert/Update/Delete` 메뉴가 노출되어, 실행 시점에만 에러 팝업으로 실패했습니다.
- **수정**: 컨텍스트 메뉴 구성 전에 `can_show_row_edit_actions` 검사(`source_sql` 존재, `ROWID` 컬럼 존재, 대상 테이블 해석 가능)를 추가해, 편집 가능한 결과에서만 편집 메뉴를 노출하도록 변경했습니다.
- **효과**: 편집 불가능한 결과에서의 불필요한 오류 팝업과 오조작 가능성을 줄였습니다.

### [중] 대용량 결과에서 그리드 편집 진입 시 전체 데이터 clone으로 UI 멈춤 가능성 완화
- **증상**: `Update/Delete/Insert` 경로가 `full_data` 전체를 매번 clone해, 큰 결과셋에서 우클릭 편집 진입 시 메모리 급증/지연이 발생할 수 있었습니다.
- **수정**:
  - `show_update_cell_dialog`: 선택 행의 `ROWID`/현재 셀 값만 잠금 구간에서 추출.
  - `show_delete_row_dialog`: 선택 범위의 `ROWID`만 잠금 구간에서 수집.
  - `show_insert_row_dialog`: 기본값용 선택 행 1개만 복제.
- **효과**: 편집 동작 진입 시 불필요한 전체 데이터 복제를 제거해 대용량 결과에서 응답성이 개선됩니다.

### [중] `Delete Row`가 선택 행 일부를 조용히 건너뛰던 동작 수정
- **증상**: 선택 범위 내 일부 행에 `ROWID` 셀이 없거나 공백인 경우, 기존 구현은 해당 행을 조용히 건너뛰고 나머지 행만 삭제 SQL에 반영할 수 있었습니다.
- **수정**: `collect_rowids_in_range`를 추가해 선택 범위를 엄격 검증하도록 변경하고, `ROWID` 누락/공백 행이 하나라도 있으면 즉시 오류로 중단하도록 보강했습니다.
- **효과**: 부분 삭제(의도와 다른 일부 행만 삭제) 위험을 줄이고 삭제 대상 정합성을 높였습니다.

### [테스트] 결과 그리드 편집 회귀 테스트 추가
- `collect_rowids_in_range_errors_when_selected_row_lacks_rowid_cell`
- `collect_rowids_in_range_errors_when_selected_row_has_empty_rowid`
- `can_show_row_edit_actions_requires_rowid_and_resolved_target`

## 2026-02-23 결과 테이블 그리드 편집 기능 개선 (후속 4)

### [중] `Insert Row` 메뉴가 `ROWID` 부재 결과에서 함께 숨겨지던 조건 결합 버그 수정
- **증상**: `ROWID`가 없는 단순 조회 결과(`SELECT ENAME FROM EMP`)에서는 행 삽입이 기술적으로 가능해도, 컨텍스트 메뉴 가드가 `Insert/Update/Delete`를 한 조건으로 묶어 `Insert Row`까지 비노출 처리했습니다.
- **수정**:
  - 메뉴 노출 조건을 분리해 `Insert Row`는 `source_sql`에서 단일 대상 테이블 해석 가능 여부만으로 판단하도록 변경했습니다.
  - `Update Cell`/`Delete Row`는 기존처럼 `ROWID` 컬럼 요구 조건을 별도 유지하도록 `can_show_rowid_edit_actions`를 추가했습니다.
- **효과**: `ROWID`가 없는 결과에서도 가능한 경우 `Insert Row`를 바로 사용할 수 있고, `ROWID`가 필요한 편집 액션은 계속 안전하게 차단됩니다.

### [테스트] 편집 메뉴 가드 회귀 테스트 보강
- `can_show_insert_row_action_requires_resolved_target` 추가.
- `can_show_rowid_edit_actions_requires_rowid_and_resolved_target` 추가.

## 2026-02-23 결과 테이블 그리드 편집 기능 개선 (후속 5)

### [중] 셀 선택 범위 역방향/비정상 selection에서 복사/선택 데이터 처리 오작동 가능성 수정
- **증상**: 셀 영역이 역순으로 잡히거나 selection 값이 역전된 상태에서 `Copy`/`Copy with Headers`/`Get selected data` 경로가 음수 폭을 usize 변환하면서 예측 불가능한 값으로 동작할 수 있었습니다.
- **수정**:
  - `copy_selected_to_clipboard`, `copy_selected_with_headers`, `copy`, `get_selected_data`가 기존 `get_selection()` 직접 사용을 모두 `normalized_selection_bounds`로 통일했습니다.
  - `normalized_selection_bounds`에 대한 회귀 테스트를 추가해 역순/음수 selection 경계 처리를 검증했습니다.
- **효과**: 선택 영역 순서와 상관없이 셀 복사 및 선택 데이터 추출이 일관적으로 동작하며, 편집 흐름에서 범위 기반 붙여넣기/수정의 기본 입력 데이터 계산 안정성이 향상됩니다.

## 2026-02-23 결과 테이블 그리드 편집 기능 개선 (후속 6)

### [중] 키보드 편집 진입 동선 누락 및 취소 시 빈 삽입 행 잔존 버그 수정
- **증상**:
  - 편집 모드에서도 `Enter`/`F2`로 셀을 즉시 편집할 수 없어 마우스 더블클릭에만 의존해야 했고,
  - 새 행 삽입에서 첫 편집을 `Esc`로 취소해도 빈 행이 `Inserted` 상태로 남아 저장/취소 흐름을 어지럽혔습니다.
- **수정**:
  - `Event::KeyDown`에 `Enter`/`KPEnter`/`F2` 처리 추가로, 단일 셀 선택 상태에서 즉시 `show_inline_cell_editor`를 호출하도록 했습니다.
  - `insert_row_in_edit_mode`에서 첫 번째 컬럼 편집이 취소되면 해당 행과 `row_states`의 마지막 항목을 제거하고, 행 개수/선택 상태를 되돌리도록 했습니다.
- **효과**:
  - 키보드 작업 중심 편집 UX가 가능해졌고,
  - 의도치 않게 추가된 빈 `Inserted` 행이 편집 상태에 남는 현상이 사라집니다.

### [테스트] 그리드 편집 단위 테스트 보강
- `resolved_selection_bounds_with_limits_clamps_to_current_table_size`
- `resolve_update_target_cell_prefers_context_and_requires_single_selection_without_it`

## 2026-02-24 결과 테이블 그리드 편집 기능 개선 (후속)

### [중] 행 헤더/ROWID 앵커 붙여넣기 시 값이 한 칸 밀려 적용되던 버그 수정
- **증상**: 편집 모드에서 행 헤더 선택 또는 `ROWID` 컬럼이 선택 앵커인 상태로 다중 셀 붙여넣기를 하면, 첫 값이 `ROWID`에 매핑되며 건너뛰어져 실제 편집 컬럼 반영이 오른쪽으로 밀릴 수 있었습니다.
- **수정**: 붙여넣기 전에 앵커 컬럼을 보정하는 `resolve_paste_anchor_column`을 추가해, 앵커가 비편집 컬럼/`ROWID`인 경우 선택 범위 내 첫 편집 가능 컬럼을 우선 대상으로 사용하도록 변경했습니다.
- **효과**: 행 단위 선택(특히 row header 선택)에서도 붙여넣기 컬럼 정렬이 의도와 일치하고, 값 밀림/누락 가능성을 줄였습니다.

### [테스트] 붙여넣기 앵커 보정 회귀 테스트 추가
- `resolve_paste_anchor_column_prefers_editable_col_when_anchor_is_rowid`
- `resolve_paste_anchor_column_keeps_anchor_when_already_editable`

## 2026-02-25 결과 테이블 편집 이벤트 경합 안정화

### [중] 쿼리 시작 이벤트에서 인라인 편집 입력값이 유실되던 경로 수정
- **증상**: 편집 모드에서 셀 인라인 입력 중 쿼리 실행 이벤트(`start_streaming`)가 먼저 들어오면, 활성 입력 위젯을 즉시 삭제하면서 마지막 입력 텍스트가 staged 데이터에 반영되지 않을 수 있었습니다.
- **원인**: `start_streaming`이 활성 인라인 에디터를 항상 `clear`만 하고, 편집 중 값을 커밋하지 않았습니다.
- **수정**:
  - 편집 세션이 살아있는 경우 `start_streaming` 진입 시 `commit_active_inline_edit()`를 먼저 수행하도록 변경.
  - 편집 세션이 없을 때만 기존처럼 인라인 위젯을 정리(`clear_active_inline_edit_widget`)하도록 분기.
- **효과**: 편집 입력 + 쿼리 실행/취소/실패 이벤트가 겹쳐도 마지막 셀 입력값이 조용히 사라지는 경로를 차단했습니다.

### [중] 쿼리 실행 중 결과 편집 액션 허용으로 인한 상태 경합 가능성 완화
- **증상**: 쿼리 실행 중에도 결과 편집 체크/액션 버튼이 활성화될 수 있어, 편집/저장/취소 이벤트가 실행 상태와 충돌할 여지가 있었습니다.
- **수정**: `refresh_result_edit_controls`에서 `is_any_query_running()` 상태를 반영해, 실행 중에는 결과 편집 체크 및 편집 액션 버튼을 비활성화하도록 조정.
- **효과**: 쿼리 실행/취소/실패 처리 중 편집 이벤트가 겹치며 발생할 수 있는 UI 상태 경합을 줄였습니다.

### [테스트] 회귀 테스트 추가
- `start_streaming_commits_active_inline_edit_while_save_is_pending`
  - save pending 상태에서 `start_streaming` 진입 시 활성 인라인 편집값이 `full_data`로 커밋되는지 검증.

### [검증]
- `cargo test` 전체 통과

## 2026-03-04 Oracle 공통 파서 엔진 ENUM 타입 선언 분리 보완

### [중] `CREATE TYPE ... AS ENUM` 구문이 후속 문장과 합쳐지던 분리 오류 수정
- **증상**: `CREATE OR REPLACE TYPE color_t AS ENUM (...); SELECT ...;` 형태에서 `AS ENUM` 선언을 블록으로 오인식해, trailing `SELECT`가 같은 문장으로 합쳐졌습니다.
- **원인**: `TYPE AS/IS` 후속 declarative kind 판별 목록에 `ENUM` 키워드가 없어 `AS/IS`로 열린 내부 상태를 해제하지 못했습니다.
- **수정**: `src/sql_parser_engine.rs`의 type declarative kind 매칭 목록에 `ENUM`을 추가해 `AS/IS` 임시 블록을 즉시 해제하도록 보완했습니다.
- **효과**: Oracle 23c `CREATE TYPE ... AS ENUM (...)` 선언이 세미콜론에서 정상 종료되고, 다음 top-level 문장이 정확히 분리됩니다.

### [테스트] 회귀 케이스 추가
- `sql_parser_engine::tests::type_enum_declaration_splits_at_semicolon`
- `db::query::query_tests::test_create_type_enum_splits_before_next_statement`

### [검증]
- `cargo test type_enum -- --nocapture` 통과
- `cargo test` 전체 통과

## 2026-02-26 결과 테이블 편집 이벤트 경계 보강

### [중] 스트리밍(쿼리 실행/취소/실패 전환 구간) 중 편집 액션이 상태를 변경할 수 있던 경로 차단
- **증상**: 결과 그리드가 `streaming_in_progress` 상태인 경계 구간에서 `Insert/Delete/Save/Cancel` 편집 액션이 호출되면, 쿼리 이벤트와 편집 이벤트가 교차하며 사용자 의도와 다른 상태 전이가 발생할 여지가 있었습니다.
- **수정**:
  - `insert_row_in_edit_mode`에 스트리밍 중 차단 가드 추가
  - `delete_selected_rows_in_edit_mode`에 스트리밍 중 차단 가드 추가
  - `save_edit_mode`에 스트리밍 중 차단 가드 추가
  - `cancel_edit_mode`에 스트리밍 중 차단 가드 추가
- **효과**: 편집 체크/입력/삭제/취소와 쿼리 실행/취소/실패 이벤트가 겹치는 타이밍에서 결과 테이블 상태가 예기치 않게 변하는 경로를 선제 차단했습니다.

### [테스트] 스트리밍-편집 경합 회귀 케이스 추가
- `insert_and_delete_are_blocked_while_streaming_is_in_progress`
- `save_edit_mode_returns_error_while_streaming_is_in_progress`
- `cancel_edit_mode_returns_error_while_streaming_is_in_progress`

### [검증]
- `cargo test` 전체 통과

## 2026-02-26 결과 테이블 편집 이벤트 경계 보강 (후속 2)

### [중] 편집 모드에서 비저장(non-save) non-select 성공 결과가 staged 편집 상태를 덮어쓰던 버그 수정
- **증상**: 결과 테이블 편집 모드에서 일반 쿼리 실행 후 `COMMIT/ROLLBACK/DDL` 같은 non-select 문이 성공하면, `display_result`가 편집 세션을 종료하고 결과 그리드를 단일 메시지 행으로 바꿔 staged 편집 데이터가 사라질 수 있었습니다.
- **수정**:
  - `display_result`에 분기를 추가해, `save_edit_mode`로 발생한 저장 응답이 아닌 non-select 성공 결과가 편집 모드 중 도착한 경우 편집 세션/스테이징 데이터(`edit_session`, `full_data`, `source_sql`)를 유지하도록 변경했습니다.
  - 이 경로에서는 pending stream 버퍼만 정리하고 기존 편집 그리드를 유지해, 편집 체크/입력/삭제/취소 상태가 쿼리 실행 성공 이벤트로 의도치 않게 초기화되지 않게 했습니다.
- **효과**: 편집 중 일반 쿼리 실행 성공 이벤트가 섞여도 결과 테이블 편집 상태가 보존되어, 사용자 입력 유실 가능성을 줄였습니다.

### [테스트] 회귀 테스트 추가
- `display_result_keeps_staged_edits_when_non_save_non_select_query_succeeds`
  - 편집 모드에서 `COMMIT` 성공 결과 수신 시 edit session과 staged row/source SQL이 유지되는지 검증.

### [검증]
- `cargo test` 전체 통과

## 2026-03-04 Oracle 공통 파서 엔진 문법 회귀 수정

### [중] PACKAGE 스펙 전방 선언 뒤 `SUBTYPE ... IS ...` 오인식으로 인한 문장 분리 실패 수정
- **증상**: `CREATE PACKAGE ... PROCEDURE p; SUBTYPE t IS ...; END ...;` 형태에서, `PROCEDURE` 전방 선언 이후 남아 있던 nested subprogram 대기 상태 때문에 `SUBTYPE ... IS`의 `IS`를 서브프로그램 본문 시작으로 잘못 인식했습니다.
- **영향**: `block_depth`가 과도하게 유지되어 패키지 종료 후 다음 `SELECT`까지 하나의 문장으로 합쳐지는 분리 오류가 발생했습니다.
- **수정**: 세미콜론(`;`) 처리 시 `PROCEDURE/FUNCTION` 전방 선언 대기 상태(`AwaitingNestedSubprogram`)를 즉시 해제하도록 `clear_forward_subprogram_declaration_state_on_semicolon` 경로를 추가했습니다.
- **효과**: PACKAGE 스펙 내 `SUBTYPE/TYPE ... IS ...` 선언이 정상 해석되고, 후속 top-level 문장이 올바르게 분리됩니다.

### [테스트] Oracle PACKAGE 전방 선언 + SUBTYPE 회귀 케이스 추가
- `test_package_spec_forward_declaration_followed_by_subtype_splits_before_next_statement`
- `test_split_format_items_package_spec_forward_declaration_followed_by_subtype_splits_before_next_statement`

### [검증]
- `cargo test package_spec_forward_declaration_followed_by_subtype -- --nocapture` 통과

## 2026-03-04 Oracle 공통 파서 엔진 `WITH FUNCTION` 문장 중간 위치 회귀 수정

### [중] `CREATE VIEW ... AS WITH FUNCTION ...` 구문에서 내부 `END;` 뒤 오분리되던 버그 수정
- **증상**: Oracle의 `WITH FUNCTION/PROCEDURE` 선언이 문장 첫 토큰이 아닌 위치(예: `CREATE VIEW ... AS WITH ...`)에 올 때, 내부 함수 `END;`를 문장 종결로 잘못 판단해 다음 `SELECT`를 별도 문장으로 분리했습니다.
- **원인**: `track_top_level_with_plsql`가 top-level `WITH`를 문장 시작(`at_statement_start`)에서만 감지해 `WITH FUNCTION` 선언 모드로 전환하지 못했습니다.
- **수정**: top-level에서 `WITH`가 등장하면(기존 `WITH` 상태를 덮어쓰지 않는 조건으로) `PendingClause`로 진입하도록 감지 조건을 확장했습니다.
- **효과**: `CREATE VIEW ... AS WITH FUNCTION ... SELECT ...;` 형태에서 선언부와 메인 쿼리가 하나의 DDL 문장으로 유지되고, 이후 문장만 정상 분리됩니다.

### [테스트] 회귀 케이스 추가
- `sql_parser_engine::tests::create_view_as_with_function_keeps_statement_open_until_main_select_terminator`
- `test_split_script_items_oracle_create_view_as_with_function_keeps_single_statement`
- `test_split_format_items_oracle_create_view_as_with_function_keeps_single_statement`

### [검증]
- `cargo test create_view_as_with_function -- --nocapture` 통과
- `cargo test` 전체 통과

## 2026-03-04 인텔리센스 문맥 인식 보강 (QUALIFY / WINDOW / RETURNING)

### [중] `QUALIFY` 절에서 테이블 컨텍스트로 오인식되던 문제 수정
- **증상**: `SELECT ... FROM ... QUALIFY ...` 구문에서 커서가 `QUALIFY` 식 내부에 있어도 phase가 `FromClause`로 유지되어 테이블 추천이 우선되는 문제가 있었습니다.
- **수정**: `scan_cursor_context`의 키워드 전이에 `QUALIFY` 분기를 추가해 `WhereClause`로 전환되도록 변경했습니다.
- **효과**: `QUALIFY` 식 내부에서 컬럼 컨텍스트로 안정적으로 분류됩니다.

### [중] `WINDOW` 절에서 컬럼 식 컨텍스트가 반영되지 않던 문제 수정
- **증상**: `WINDOW w AS (PARTITION BY ... ORDER BY ...)` 절에서 phase 전이가 없어 이전 상태가 유지될 수 있었습니다.
- **수정**: `WINDOW` 키워드 진입 시 `OrderByClause`로 전환하도록 처리했습니다.
- **효과**: `WINDOW` 절 내부의 컬럼/식 입력 시 컬럼 추천 컨텍스트가 유지됩니다.

### [중] DML `RETURNING` 절이 값 절(`VALUES`)로 남아 컬럼 추천이 제한되던 문제 수정
- **증상**: `INSERT ... VALUES ... RETURNING ...`에서 커서가 `RETURNING` 목록에 있어도 phase가 `ValuesClause`로 남았습니다.
- **수정**: `RETURNING` 키워드 진입 시 `SetClause`로 전환하도록 처리하고, 관련 테스트 기대값을 업데이트했습니다.
- **효과**: `RETURNING` 목록에서 컬럼 컨텍스트 기반 추천이 동작합니다.

### [테스트] 회귀/보강 테스트 추가
- `ui::intellisense_context::tests::phase_window_clause_is_column_context`
- `ui::intellisense_context::tests::phase_qualify_clause_is_column_context`
- `ui::intellisense::intellisense_tests::detect_sql_context_qualify_clause_is_column_name`
- `ui::intellisense::intellisense_tests::detect_sql_context_returning_clause_is_column_name`
- `ui::intellisense_context::tests::insert_subquery_depth_returns_to_zero_after_closing_values_subquery` 기대 phase를 `SetClause`로 보강

### [검증]
- `cargo test` 전체 통과

## 2026-03-05 Oracle 공통 파서 엔진 `WITH FUNCTION` 복구 키워드 보강

### [중] `WITH FUNCTION/PROCEDURE` 복구 경로에서 `SAVEPOINT`를 새 문장 시작으로 인식하지 못하던 문제 수정
- **증상**: `WITH FUNCTION ... END; SAVEPOINT ...;` 형태에서 `SAVEPOINT`가 새 top-level 문장으로 분리되지 않고, `WITH FUNCTION` 선언 문장에 계속 붙어 하나의 문장으로 합쳐졌습니다.
- **원인**: `WITH FUNCTION/PROCEDURE` 선언 모드 복구에 사용하는 문장 시작 키워드 집합(`is_statement_head_keyword`)에 `SAVEPOINT`가 누락되어 있었습니다.
- **수정**: `src/sql_text.rs`의 `is_statement_head_keyword`에 `SAVEPOINT`를 추가해, `WITH FUNCTION/PROCEDURE` 대기 상태에서 `SAVEPOINT`를 만나면 즉시 새 문장으로 복구 분리되도록 수정했습니다.
- **효과**: 비정상 `WITH FUNCTION/PROCEDURE` 이후 이어지는 트랜잭션 제어 문장이 정확히 분리되어 후속 파싱/실행 오염을 방지합니다.

### [테스트] 회귀 케이스 추가
- `test_split_script_items_oracle_with_function_recovers_to_savepoint_statement_head`

### [검증]
- `cargo test -q recovers_to_` 통과
- `cargo test -q` 전체 통과

## 2026-03-05 Oracle 공통 파서 엔진 `WITH FUNCTION` 복구 키워드 보강 (`LOCK TABLE`)

### [중] `WITH FUNCTION/PROCEDURE` 복구 경로에서 `LOCK TABLE`을 새 문장 시작으로 인식하지 못하던 문제 수정
- **증상**: `WITH FUNCTION ... END; LOCK TABLE ...;` 형태에서 `LOCK TABLE`이 새 top-level 문장으로 분리되지 않고 `WITH FUNCTION` 선언 블록에 붙어 하나의 문장으로 합쳐졌습니다.
- **원인**: `WITH FUNCTION/PROCEDURE` 선언 모드 복구에 사용하는 문장 시작 키워드 집합(`is_statement_head_keyword`)에 `LOCK`이 누락되어 있었습니다.
- **수정**: `src/sql_text.rs`의 `is_statement_head_keyword`에 `LOCK`을 추가해, `WITH FUNCTION/PROCEDURE` 대기 상태에서 `LOCK TABLE`을 만나면 즉시 새 문장으로 복구 분리되도록 수정했습니다.
- **효과**: Oracle 잠금 구문(`LOCK TABLE ...`)이 후속 문장과 정상 분리되어 실행 단위 오염을 방지합니다.

### [테스트] 회귀 케이스 추가
- `test_split_script_items_oracle_with_function_recovers_to_lock_statement_head`
- `test_split_format_items_oracle_with_function_recovers_to_lock_statement_head`

### [검증]
- `cargo test recovers_to_lock_statement_head -- --nocapture` 통과
- `cargo test` 전체 통과

## 2026-03-05 Oracle 공통 파서 엔진 `WITH FUNCTION` 복구 키워드 보강 (`ASSOCIATE` / `DISASSOCIATE`)

### [중] `WITH FUNCTION/PROCEDURE` 복구 경로에서 `ASSOCIATE`/`DISASSOCIATE`를 새 문장 시작으로 인식하지 못하던 문제 수정
- **증상**:
  - `WITH FUNCTION ... END; ASSOCIATE STATISTICS ...; SELECT ...;`
  - `WITH FUNCTION ... END; DISASSOCIATE STATISTICS ...; SELECT ...;`
  - 위 형태에서 `ASSOCIATE`/`DISASSOCIATE`가 새 top-level statement로 분리되지 않고, `WITH FUNCTION` 블록에 붙어 하나의 statement로 병합됐습니다.
- **원인**:
  - `src/sql_text.rs`의 `is_statement_head_keyword`에 `ASSOCIATE`, `DISASSOCIATE`가 누락되어
  - `WITH FUNCTION/PROCEDURE` 복구 상태(`AwaitingMainQuery`)에서 새 statement 시작을 감지하지 못했습니다.
- **수정**:
  - `is_statement_head_keyword`에 `ASSOCIATE`, `DISASSOCIATE`를 추가해 복구 분리 조건을 보강했습니다.
  - 발견된 버그와 유사한 `split_script_items`/`split_format_items` 경로를 함께 회귀 테스트로 일괄 보강했습니다.

### [테스트] 회귀 테스트 추가
- `test_split_script_items_oracle_with_function_recovers_to_associate_statement_head`
- `test_split_script_items_oracle_with_function_recovers_to_disassociate_statement_head`
- `test_split_format_items_oracle_with_function_recovers_to_associate_statement_head`
- `test_split_format_items_oracle_with_function_recovers_to_disassociate_statement_head`

### [검증]
- `cargo test -q recovers_to_disassociate_statement_head -- --nocapture` 통과
- `cargo test -q recovers_to_associate_statement_head -- --nocapture` 통과
- `cargo test` 전체 통과

## 2026-03-04 인텔리센스 FROM 확장 구문 별칭 파싱 보완 (`PARTITION` / `SUBPARTITION` / `SAMPLE` / `AS OF`)

### [중] 테이블 postfix 절을 별칭으로 오인식해 스코프 해석이 깨지던 문제 수정
- **증상**:
  - `SELECT * FROM sales PARTITION (p202401) s WHERE s.|`
  - `SELECT * FROM employees AS OF SCN (12345) e WHERE e.|`
  - 위 형태에서 `PARTITION` 또는 `AS`가 별칭으로 잘못 파싱되거나, 실제 별칭(`s`, `e`) 수집이 누락될 수 있었습니다.
- **원인**:
  - `src/ui/intellisense_context.rs`의 `parse_alias_deep`가 테이블명 직후 토큰만 확인해 별칭을 해석하고,
  - Oracle FROM postfix 절(`PARTITION(...)`, `SUBPARTITION(...)`, `SAMPLE(...)`, `SEED(...)`, `AS OF ...`)을 건너뛰지 않았습니다.
- **수정**:
  - `skip_relation_postfix_clauses`를 추가하고 `parse_alias_deep` 시작 지점에서 postfix 절을 먼저 건너뛰도록 변경했습니다.
  - `AS` 키워드는 `AS OF`(flashback clause)일 때 별칭 분기로 진입하지 않도록 가드 처리했습니다.

### [유사 케이스] Oracle relation extension 일괄 보강
- 발견된 버그와 동일 계열인 `PARTITION/SUBPARTITION`, `SAMPLE/SEED`, `AS OF`를 공통 스킵 경로로 묶어 후속 오탐 가능성을 함께 차단했습니다.

### [테스트] 회귀 테스트 추가
- `partition_extension_before_alias_is_not_parsed_as_alias`
- `flashback_as_of_before_alias_is_not_parsed_as_alias`

### [검증]
- `cargo test -q partition_extension_before_alias_is_not_parsed_as_alias -- --nocapture` 통과
- `cargo test -q flashback_as_of_before_alias_is_not_parsed_as_alias -- --nocapture` 통과
- `cargo test -- --test-threads=1` 전체 통과
## 2026-03-04 Oracle 공통 파서 엔진 `WITH FUNCTION/PROCEDURE` 복구 로직 일괄 보강 (`is_statement_head_keyword` 재사용)

### [중] `split_script_items` / `split_format_items`가 복구 대상 키워드를 하드코딩해 누락 구문이 계속 재발하던 문제 수정
- **증상**:
  - `WITH FUNCTION ... END; COMMENT ON ...; SELECT ...;`
  - `WITH FUNCTION ... END; RENAME ...; SELECT ...;`
  - 위 형태에서 `COMMENT`, `RENAME`가 새 문장 시작으로 인식되지 않아 앞 `WITH FUNCTION` 블록과 합쳐질 수 있었습니다.
- **원인**:
  - `src/sql_parser_engine.rs`는 `is_statement_head_keyword` 기반으로 복구 키워드를 폭넓게 인식하지만,
  - `src/db/query/script.rs`의 조기 복구 분기(`split_script_items`, `split_format_items`)는 별도 하드코딩 목록을 사용해 동기화가 깨져 있었습니다.
- **수정**:
  - 두 분기 모두 `trimmed_upper.split_whitespace().next()`로 선두 토큰을 추출한 뒤,
  - `sql_text::is_statement_head_keyword`를 공통으로 재사용하도록 변경했습니다.
  - 이로써 기존에 누락되던 `COMMENT`, `RENAME`뿐 아니라 동일 클래스의 누락 가능 구문이 일괄적으로 예방됩니다.

### [유사 케이스 점검] 하드코딩 목록과 공통 분류기 불일치 구조 제거
- `CREATE/ALTER/DROP/.../WITH`만 나열하던 중복 조건을 제거해,
- 파서 엔진의 복구 기준과 스크립트 분할기의 복구 기준이 동일한 소스(`is_statement_head_keyword`)를 따르도록 정렬했습니다.

### [테스트] 회귀 테스트 추가
- `test_split_script_items_oracle_with_function_recovers_to_comment_statement_head`
- `test_split_script_items_oracle_with_function_recovers_to_rename_statement_head`
- `test_split_format_items_oracle_with_function_recovers_to_comment_statement_head`
- `test_split_format_items_oracle_with_function_recovers_to_rename_statement_head`

### [검증]
- `cargo test -q recovers_to_comment_statement_head` 통과
- `cargo test -q recovers_to_rename_statement_head` 통과
- `cargo test -q` 전체 통과
