# 예외 처리 보완 내역

## 중(이상) 우선 수정

### [중] Clippy 경고(문서 주석 위치)로 인한 품질 게이트 실패
- **증상**: `cargo clippy --all-targets --all-features -- -D warnings` 실행 시 `src/sql_text.rs`의 파일 상단 doc comment 다음 빈 줄로 인해 `clippy::empty_line_after_doc_comments` 에러 발생.
- **원인**: 파일 모듈 설명을 outer doc(`///`)로 선언해 실제 함수 doc로 해석되는 형태였고, 뒤에 빈 줄이 있어 lint 위반.
- **수정**: 파일 상단 주석을 모듈 내부 문서 주석(`//!`)으로 변경.
- **효과**: 해당 lint 에러는 해소됨.

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
