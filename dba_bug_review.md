# DBA 기능 버그 리뷰 (20건, 보강판)

검토 범위:
- `src/ui/sql_editor/dba_tools.rs`
- `src/ui/sql_editor/session_monitor.rs`
- `src/db/query/executor.rs`

검토 방법(정적 분석):
- `rg -n "should_fallback_from_global_view|get_session_lock_snapshot|get_heavy_execution_snapshot|get_ash_session_activity_snapshot|get_ash_top_sql_snapshot|get_awr_top_sql_snapshot|get_dataguard_overview_snapshot|get_dataguard_destination_snapshot|get_dataguard_apply_process_snapshot|get_dataguard_archive_gap_snapshot|get_scheduler_jobs_snapshot|get_scheduler_job_history_snapshot|get_datapump_jobs_snapshot|start_datapump_job|create_and_run_shell_job|parse_selected_session_identity|query_sql_monitor_rows" src/db/query/executor.rs src/ui/sql_editor/dba_tools.rs src/ui/sql_editor/session_monitor.rs`

> 아래 항목은 **재현 전 정적 분석 관점의 잠재 버그/운영 리스크**이며, 각 항목에 코드 근거 위치를 함께 명시했습니다.

## 1) fallback 분기 기준이 에러코드 문자열 매칭에 과도 의존
- 근거: `should_fallback_from_global_view`가 `ORA-00942/01031/02030` 포함 여부만 검사.
- 위치: `src/db/query/executor.rs` (`fn should_fallback_from_global_view`).
- 영향: 동일 코드의 다른 원인을 구분하지 못해 오진 가능.

## 2) Session/Lock의 v$ fallback은 INST_ID를 `-`로 고정
- 근거: fallback SQL에서 `'-' AS inst_id` 사용.
- 위치: `src/db/query/executor.rs` (`get_session_lock_snapshot`의 `sql_v`).
- 영향: RAC에서 대상 인스턴스 식별성 저하.

## 3) Session/Lock fallback의 blocking serial 조회에 인스턴스 조건 부재
- 근거: `v$session bs WHERE bs.sid = s.blocking_session`.
- 위치: `src/db/query/executor.rs` (`get_session_lock_snapshot`의 `sql_v`).
- 영향: RAC SID 충돌 시 blocking serial 오매칭 가능.

## 4) Session/Lock 결과 메타 SQL이 fallback 실행 SQL과 불일치
- 근거: fallback 실행 가능해도 `QueryResult::new_select(sql_gv, ...)`로 고정 저장.
- 위치: `src/db/query/executor.rs` (`get_session_lock_snapshot` 반환부).
- 영향: 로그/디버깅에서 실제 실행 경로 추적 어려움.

## 5) Session/Lock row 파싱에서 NULL이 빈 문자열로 소거
- 근거: `value.unwrap_or_default()`.
- 위치: `src/db/query/executor.rs` (`get_session_lock_snapshot` row loop).
- 영향: NULL/빈값 의미 구분 소실.

## 6) Heavy execution의 v$ fallback도 INST_ID를 `-`로 고정
- 근거: `sql_v`에서 `'-' AS inst_id`.
- 위치: `src/db/query/executor.rs` (`get_heavy_execution_snapshot`의 `sql_v`).
- 영향: RAC 식별 품질 저하.

## 7) Heavy execution 결과 메타 SQL도 fallback 경로와 불일치 가능
- 근거: 반환 시 SQL 텍스트가 `sql_gv`로 고정될 가능성.
- 위치: `src/db/query/executor.rs` (`get_heavy_execution_snapshot` 반환부).
- 영향: 운영 장애 분석 시 실행 쿼리 추적 혼선.

## 8) Heavy execution row 파싱도 NULL을 빈 문자열로 치환
- 근거: `value.unwrap_or_default()`.
- 위치: `src/db/query/executor.rs` (`get_heavy_execution_snapshot` row loop).
- 영향: 결측값/실제 공백 구분 어려움.

## 9) SQL Monitor fallback 경로에서 INST_ID를 `-`로 통일
- 근거: `use_global_view=false`일 때 `inst_id_expr`이 `"'-'"`.
- 위치: `src/db/query/executor.rs` (`query_sql_monitor_rows`).
- 영향: RAC에서 세션 대상 식별 약화.

## 10) SQL Monitor row 파싱도 NULL 의미 소실
- 근거: `value.unwrap_or_default()`.
- 위치: `src/db/query/executor.rs` (`query_sql_monitor_rows` row loop).
- 영향: 식별 키(`sql_exec_id` 등) 품질 저하 시 원인 분석 난이도 증가.

## 11) ASH Session Activity에서 gv$ 실패 원인을 버림
- 근거: fallback 분기에서 `Err(_) => {}`로 상세 오류 폐기.
- 위치: `src/db/query/executor.rs` (`get_ash_session_activity_snapshot`).
- 영향: 사용자 메시지에 1차 실패 문맥이 남지 않음.

## 12) ASH Session Activity fallback 시 INST_ID 의미 변화
- 근거: gv$는 `ash.inst_id`, v$는 `'-' AS inst_id`.
- 위치: `src/db/query/executor.rs` (`get_ash_session_activity_snapshot`).
- 영향: 동일 컬럼의 의미가 경로별로 달라져 UI 해석 혼동.

## 13) ASH Top SQL도 gv$ 실패 원인을 버림
- 근거: 동일 fallback 패턴(`Err(_) => {}`).
- 위치: `src/db/query/executor.rs` (`get_ash_top_sql_snapshot`).
- 영향: 권한/뷰 문제와 SQL 문제 구분 어려움.

## 14) AWR Top SQL 실패 시 상세 원인 보존 없이 고정 에러 반환
- 근거: fallback 최종 에러 메시지가 고정 문자열.
- 위치: `src/db/query/executor.rs` (`get_awr_top_sql_snapshot`).
- 영향: 실환경 실패 원인(권한/객체/세션 상태) 파악 지연.

## 15) Data Guard overview 실패를 `-` 최소 결과로 대체
- 근거: 최종 `sql_minimal`이 `'-'` 채움 행 반환.
- 위치: `src/db/query/executor.rs` (`get_dataguard_overview_snapshot`).
- 영향: 조회 실패가 정상 빈 데이터처럼 보일 수 있음.

## 16) Data Guard destination 실패를 `N/A` 정보행으로 대체
- 근거: 실패 시 `sql_minimal` 반환.
- 위치: `src/db/query/executor.rs` (`get_dataguard_destination_snapshot`).
- 영향: 경보 탐지 누락/지연 가능.

## 17) Data Guard apply process 실패를 `N/A`로 대체
- 근거: 마지막 fallback이 `N/A` 행 반환.
- 위치: `src/db/query/executor.rs` (`get_dataguard_apply_process_snapshot`).
- 영향: 실제 privilege/view 실패가 은폐됨.

## 18) Data Guard archive gap 실패를 `N/A`로 대체
- 근거: 실패 시 `'-', 'N/A', '-'` 행 반환.
- 위치: `src/db/query/executor.rs` (`get_dataguard_archive_gap_snapshot`).
- 영향: “조회 실패”와 “정상 무갭”이 혼동될 수 있음.

## 19) Session Monitor 선택 파서가 컬럼 개수 추정(`len>=11`)에 의존
- 근거: `uses_inst_id = row_values.len() >= 11`.
- 위치: `src/ui/sql_editor/session_monitor.rs` (`parse_selected_session_identity`).
- 영향: 컬럼 순서/개수 변동 시 SID/SERIAL 오파싱 위험.

## 20) Data Pump 시작 API가 모드별 필수 입력 검증을 충분히 강제하지 않음
- 근거: 모드는 검증하지만(`SCHEMA/TABLE/TABLESPACE/FULL`) 실제 모드별 필수 필터 분기가 약함.
- 위치: `src/db/query/executor.rs` (`start_datapump_job`), `src/ui/sql_editor/dba_tools.rs` (Data Pump 요청 분기).
- 영향: 잘못된 조합이 런타임 ORA 오류로 늦게 표면화.

---

추가 개선 제안:
1. fallback 체인별 원인 누적(`Vec<String>`)을 ASH/AWR/DataGuard 함수에도 일관 적용.
2. 결과 메타에 `executed_view`(gv$/v$) 별도 필드 추가.
3. RAC 환경 kill/추적 기능에서 `inst_id` 미확정 시 위험 작업 차단 UX 추가.
