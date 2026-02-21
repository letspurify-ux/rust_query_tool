# DBA 기능 버그 리뷰 (10건)

검토 범위: `src/ui/sql_editor/dba_tools.rs`, `src/db/query/executor.rs`

## 1) SQL_ID 길이 검증이 "정확히 13자"가 아니라 "최대 13자"로 되어 있음
- 현상: UI/DB 레이어 모두 SQL_ID를 1~13자까지 허용합니다.
- 근거: UI는 `upper.len() > 13`만 검사하고, DB도 `normalized.len() > 13`만 검사합니다.
- 영향: 잘못된 짧은 SQL_ID가 정상 입력으로 통과해 "조회 결과 없음" 형태의 오동작(사용자 혼란)을 유발합니다.

## 2) Cursor 후보 조회가 RAC 비대응(`v$sql` 고정)
- 현상: 최근 SQL cursor 후보 조회가 `v$sql`만 사용합니다.
- 근거: `get_recent_sql_cursor_candidates` 쿼리의 FROM 절이 `v$sql` 고정입니다.
- 영향: RAC 환경에서 다른 인스턴스의 hot cursor를 누락할 수 있습니다.

## 3) SQL Text 조회가 4000 byte에서 강제 절단됨
- 현상: SQL text를 `DBMS_LOB.SUBSTR(..., 4000, 1)`로만 읽습니다.
- 근거: `get_sql_text_by_sql_id`의 `gv$sql`, `v$sql` 쿼리 모두 동일하게 4000 제한.
- 영향: 긴 SQL 분석 시 핵심 후반부가 잘려 DBA 분석 정확도가 떨어집니다.

## 4) GV$ 실패 시 원인 무시 후 V$로 무조건 폴백
- 현상: `gv$` 질의 실패 원인을 구분하지 않고 무조건 `v$` 재시도합니다.
- 근거: `if let Ok(result) = ... { return Ok(result); }` 패턴으로 모든 오류를 일괄 무시.
- 영향: 실제 장애(권한 문제/구문 오류/세션 오류)가 RAC 미지원처럼 위장되어 진단을 어렵게 만듭니다.

## 5) SQL Monitor의 `active_only` 조건에 사실상 완료 상태가 포함됨
- 현상: active_only인데 `DONE (FIRST N ROWS)`도 포함됩니다.
- 근거: `m.status IN ('EXECUTING', 'QUEUED', 'DONE (FIRST N ROWS)')`.
- 영향: "활성 세션만" 기대한 사용자가 완료된 항목을 보게 되어 필터 의미가 흐려집니다.

## 6) Data Guard Start Apply: 서버측 역할 검증 부재
- 현상: `start_dataguard_apply`는 역할 확인 없이 ALTER를 바로 수행합니다.
- 근거: SQL 실행 전 `database_role` 검증 로직이 없습니다(반면 archive log switch는 검증 존재).
- 영향: UI 외 경로/API 호출 시 잘못된 role에서 실행되어 ORA 오류를 유발합니다.

## 7) Data Guard Stop Apply: 서버측 역할 검증 부재
- 현상: `stop_dataguard_apply`도 역할 검증 없이 즉시 실행합니다.
- 근거: `ALTER DATABASE RECOVER ... CANCEL` 직접 실행.
- 영향: 동일하게 잘못된 role에서 실패하며, 방어로직이 UI에만 존재합니다.

## 8) Data Guard Switchover: 서버측 role/state/target 사전 검증 부재
- 현상: `switchover_dataguard`는 SQL 문자열 생성 후 즉시 실행합니다.
- 근거: 함수 내 검증은 식별자 포맷뿐이며 role/status/자기 자신 target 여부는 검증하지 않습니다.
- 영향: 잘못된 상태에서 호출 시 런타임 오류를 유발하고, 실패 원인이 사용자에게 늦게 노출됩니다.

## 9) Data Guard Failover: 서버측 role/state/target 사전 검증 부재
- 현상: `failover_dataguard`도 사전 검증 없이 실행합니다.
- 근거: switchover와 동일 패턴.
- 영향: 운영 중 오조작 위험 및 오류 메시지 지연.

## 10) Data Guard UI에서 Switchover/Failover는 role 체크가 없음
- 현상: Start/Stop Apply는 role을 체크하지만 Switchover/Failover 분기에는 role 검증이 없습니다.
- 근거: Start/Stop에는 `dataguard_role_allows_apply_control(...)` 체크가 있으나 Switchover/Failover 분기에는 없음.
- 영향: UI 레벨에서도 부적절한 role에서 위험 액션 버튼 실행 경로가 열려 있습니다.
