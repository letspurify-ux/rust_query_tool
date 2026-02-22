# DBA 관련 기능 버그 점검 리포트 (20건)

검토 범위:
- `src/db/query/executor.rs`
- `src/ui/sql_editor/dba_tools.rs`
- `src/ui/sql_editor/session_monitor.rs`

## 발견 사항
1. `get_session_lock_snapshot`가 `gv$` 실패 시 `v$session`/`v$lock`로 강등되어 RAC 전체 락/세션 관측이 누락될 수 있음.
2. 같은 fallback 경로에서 blocking serial 서브쿼리가 `bs.sid = s.blocking_session`만 사용해 SID 중복 오매칭 가능성이 있음.
3. Session/Lock 파싱에서 `row.get(idx)?` 결과의 `None`을 전부 `"-"`로 바꿔 NULL/데이터오류 구분이 불가능함.
4. `get_heavy_execution_snapshot`도 `gv$` 실패 시 `v$session`/`v$sql`로 강등되어 멀티 인스턴스 진단 정확도가 떨어짐.
5. Heavy execution 파싱도 `unwrap_or_else(|| "-".to_string())`로 값 누락 원인을 평탄화함.
6. `get_sql_monitor_snapshot` 내부 row 파싱도 동일 패턴으로 NULL/파싱오류/미존재를 구분하지 못함.
7. `get_ash_session_activity_snapshot`는 오류 원인 분리 없이 `gv$`에서 `v$`로 단순 fallback만 수행함.
8. `get_ash_top_sql_snapshot`도 동일한 단순 fallback으로 권한/객체/구문 오류의 진단 컨텍스트가 약함.
9. `get_awr_top_sql_snapshot`에서 AWR 실패도 `should_fallback_from_global_view` 코드셋(904/942/1031/2030)에 의존하여 AWR 전용 오류 분류가 부족함.
10. `chained_fallback_error`가 조회 실패를 `invalid_security_input_error`로 래핑해 오류 타입이 왜곡됨.
11. `get_dataguard_overview_snapshot` fallback 쿼리가 lag 컬럼을 `'-'` 상수로 채워 실패를 정상 빈값처럼 보이게 함.
12. `get_dataguard_archive_gap_snapshot`은 `v$archive_gap` 단일 경로만 사용하고 대체 뷰/우회 경로가 없어 취약함.
13. `get_scheduler_jobs_snapshot`는 dba/all/user fallback 소스가 컬럼에 반영되지 않아 동일 스키마로 오해하기 쉬움.
14. `get_scheduler_job_history_snapshot`도 동일하게 fallback 소스 정보가 결과 데이터 열에 드러나지 않음.
15. `get_datapump_jobs_snapshot` 역시 dba/user fallback 소스를 결과 컬럼에 표기하지 않아 권한 축소와 실제 부재를 구분하기 어려움.
16. `start_datapump_job`가 `TABLE`/`TABLESPACE` 모드를 허용 목록에 넣고도 즉시 unsupported 에러로 거절해 API 계약이 불일치함.
17. 같은 함수에서 메타데이터 필터는 `SCHEMA_EXPR`만 구현되어 모드별 필터(`NAME_EXPR`, `TABLESPACE_EXPR`) 확장이 불가함.
18. `get_cursor_plan_snapshot`에서 `row.get(0).unwrap_or(None)`로 변환 오류를 빈값 처리하여 원인 소실 가능성이 있음.
19. `parse_selected_session_identity`는 `INST_ID` 파싱 실패를 `None`으로 묵살해 잘못된 인스턴스 생략 kill 요청으로 이어질 수 있음.
20. `parse_sql_id_child_row`는 `CHILD_NUMBER`/`CHILD#` 컬럼이 없으면 기본값 `"0"`을 사용해 의도하지 않은 child cursor를 선택할 수 있음.

## 참고
- 상기 항목은 정적 코드 리뷰 기반이며 DB 버전/권한/RAC 여부에 따라 체감 영향은 달라질 수 있음.
