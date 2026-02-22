# DBA 기능 버그 리뷰 (20건)

검토 범위:
- `src/ui/sql_editor/dba_tools.rs`
- `src/ui/sql_editor/session_monitor.rs`
- `src/db/query/executor.rs`

검토 방법(정적 분석):
- `rg -n "should_fallback_from_global_view|get_session_lock_snapshot|get_heavy_execution_snapshot|get_ash_session_activity_snapshot|get_ash_top_sql_snapshot|get_awr_top_sql_snapshot|get_dataguard_overview_snapshot|get_dataguard_archive_gap_snapshot|get_scheduler_jobs_snapshot|get_scheduler_job_history_snapshot|get_datapump_jobs_snapshot|start_datapump_job|create_and_run_shell_job|parse_selected_session_identity|query_sql_monitor_rows" src/db/query/executor.rs src/ui/sql_editor/dba_tools.rs src/ui/sql_editor/session_monitor.rs`

## 1) fallback 에러 분류가 지나치게 단순함
- `should_fallback_from_global_view`가 `ORA-00942/01031/02030` 문자열 포함 여부만으로 fallback을 결정.
- 같은 에러 코드라도 원인(권한/오타/환경)이 다른 경우를 구분하지 못함.

## 2) Session/Lock 조회의 v$ fallback은 INST_ID를 `-`로 고정
- fallback SQL에서 inst_id를 문자열 `'-'`로 채움.
- RAC에서 kill 대상 인스턴스 식별력이 사라짐.

## 3) Session/Lock v$ fallback의 blocking serial 조회는 인스턴스 구분 없음
- `v$session bs WHERE bs.sid = s.blocking_session`만 사용.
- RAC에서 SID 충돌 시 blocking serial 오매칭 위험.

## 4) Session/Lock 결과는 항상 `sql_gv`를 SQL 텍스트로 기록
- fallback으로 `sql_v`를 실행해도 `QueryResult::new_select(sql_gv, ...)`로 저장.
- 화면/로그에 실제 실행 SQL과 표시 SQL이 달라 디버깅 혼란.

## 5) Session/Lock 결과 파싱이 NULL과 빈 문자열을 동일시
- `value.unwrap_or_default()`로 모든 NULL을 `""`로 변환.
- 미수집/미존재/공백 구분이 사라져 운영 판단이 왜곡될 수 있음.

## 6) Heavy execution fallback도 INST_ID를 `-`로 고정
- 단일 뷰 fallback 시 inst_id 손실.
- RAC에서 세션 식별/추적 정확도 저하.

## 7) Heavy execution도 실행 SQL 추적 정보가 왜곡됨
- fallback으로 v$를 써도 결과 메타에는 gv$ SQL이 남음.
- 문제 재현 시 어떤 쿼리가 실제 실행됐는지 확인 어려움.

## 8) Heavy execution 파싱도 NULL을 빈 문자열로 소거
- `unwrap_or_default` 동일 패턴.
- 숫자/텍스트 컬럼의 결측 상태가 모두 빈 문자열로 합쳐짐.

## 9) SQL Monitor fallback 경로도 INST_ID를 `-`로 고정
- `use_global_view=false`면 inst_id_expr이 `'-'`.
- RAC kill/추적 시 cross-instance 식별 불가.

## 10) SQL Monitor 파싱도 NULL 구분 손실
- `query_sql_monitor_rows`에서 `unwrap_or_default` 사용.
- `sql_exec_id` 등 식별 키가 빈 문자열로 치환되어 품질 진단이 어려움.

## 11) ASH Session Activity는 fallback 중간 실패 원인을 버림
- gv$ 실패 시 `Err(_) => {}`로 원인 문자열을 폐기.
- 사용자에게 실패 맥락이 전달되지 않음.

## 12) ASH Session Activity fallback 시 INST_ID 의미가 바뀜
- gv$는 실제 inst_id, v$는 `'-'`.
- 같은 UI 컬럼이 환경/권한에 따라 다른 의미를 갖게 됨.

## 13) ASH Top SQL도 동일하게 중간 fallback 에러를 폐기
- gv$ 실패 원인이 최종 에러 메시지에 남지 않음.
- 운영자가 권한 문제인지 질의 결함인지 분리하기 어려움.

## 14) AWR Top SQL도 fallback 원인을 버리고 고정 메시지 반환
- gv 시도 실패 세부 원인을 남기지 않고 일반 메시지로 반환.
- 실제 장애 분석 정보가 유실됨.

## 15) Data Guard overview는 실패를 `-` 채움 데이터로 은폐
- 최종 minimal 결과를 정상 조회처럼 반환.
- 조회 실패와 실제 값 부재를 구분하기 어려움.

## 16) Data Guard destination도 실패를 `N/A` 행으로 은폐
- 권한/뷰 접근 실패 시 informational row를 반환.
- 모니터링 화면에서 장애가 정상값처럼 보일 수 있음.

## 17) Data Guard apply process도 실패를 `N/A`로 대체
- `v$managed_standby`/`v$dataguard_process` 모두 실패해도 에러 대신 최소행 반환.
- 실제 실패 원인 노출이 사라짐.

## 18) Data Guard archive gap도 실패 시 `N/A`로 대체
- 조회 실패와 “진짜 gap 없음”이 명확히 분리되지 않음.
- 운영 오판 가능성 존재.

## 19) Session Monitor의 선택 row 파서는 컬럼 개수 추정에 의존
- `row_values.len() >= 11`이면 INST_ID 포함으로 간주.
- 컬럼 구성 변경(추가/숨김)에 매우 취약하고 잘못된 SID/SERIAL 파싱 위험.

## 20) Data Pump 시작 API가 모드별 필수 파라미터 제약을 검증하지 않음
- `TABLE/TABLESPACE/FULL` 모드에서도 schema_expr만 선택적으로 적용.
- 모드별 필터 요구사항 부재로 런타임 ORA 오류를 늦게 맞게 됨.

---

참고: 본 문서는 코드 정적 검토 결과이며, Oracle 버전/옵션/권한 정책에 따라 실제 재현 양상은 달라질 수 있습니다.
