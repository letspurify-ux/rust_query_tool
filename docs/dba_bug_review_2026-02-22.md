# DBA 기능 버그 검토 리포트 (20건)

검토 범위:
- `src/ui/sql_editor/dba_tools.rs`
- `src/ui/sql_editor/session_monitor.rs`
- `src/db/query/executor.rs`

## 1) 전역 뷰 fallback 판단이 과도하게 넓어 실제 오류를 가림
- `should_fallback_from_global_view`는 `ORA-00904`/`ORA-00942`/`ORA-01031`뿐 아니라 문자열에 `GV$`가 포함되면 무조건 fallback합니다.
- 이로 인해 SQL 오타/컬럼 변경 등 실제 결함이 권한 이슈처럼 은폐될 수 있습니다.

## 2) ASH Session Activity가 오류 원인 구분 없이 단계적 fallback
- `get_ash_session_activity_snapshot`는 `gv$active_session_history` 실패 시 에러 타입 구분 없이 `v$`, 다시 `gv$session`, `v$session`으로 내려갑니다.
- 결과의 의미(ASH 표본 vs 현재 세션 스냅샷)가 달라지는데 사용자에게 명시되지 않습니다.

## 3) ASH Top SQL도 동일한 무차별 fallback
- `get_ash_top_sql_snapshot` 역시 실패 원인 구분 없이 ASH -> 현재 세션 집계로 대체합니다.
- 라이선스/권한 이슈와 실제 SQL 결함을 구분하기 어렵습니다.

## 4) AWR Top SQL 실패 시 Shared Pool로 의미가 바뀜
- `get_awr_top_sql_snapshot`은 AWR 실패 시 `gv$sql`/`v$sql` 실시간 캐시 집계로 대체합니다.
- "과거 AWR 분석"과 "현재 캐시"는 성격이 달라 같은 화면에서 오해를 유발합니다.

## 5) Data Guard overview 조회 실패 시 정보성 행으로 침묵 대체
- `get_dataguard_overview_snapshot`는 실패 시 `'-'` 값으로 채운 최소 결과를 반환합니다.
- 운영 장애/권한 오류가 정상적인 빈 데이터처럼 보일 수 있습니다.

## 6) Archive gap 조회 실패 시 `NO_GAP`/`N/A`로 폴백
- `get_dataguard_archive_gap_snapshot` 실패 시 gap 없음처럼 보이는 fallback 결과를 반환합니다.
- 실제 조회 실패와 진짜 gap 부재가 구분되지 않습니다.

## 7) Session/Lock 모니터링이 RAC 비대응 (`v$session`, `v$lock` 고정)
- `get_session_lock_snapshot`는 단일 인스턴스 뷰만 사용합니다.
- RAC에서 다른 인스턴스 세션/락은 누락됩니다.

## 8) Blocking serial 조회에서 인스턴스 매칭 누락
- blocking serial 서브쿼리는 `WHERE bs.sid = s.blocking_session`만 사용하고 `inst_id` 조건이 없습니다.
- RAC에서 동일 SID가 다른 인스턴스에 있으면 오매칭 위험이 있습니다.

## 9) Heavy execution 스냅샷도 RAC 비대응 (`v$session`, `v$sql` 고정)
- `get_heavy_execution_snapshot`은 전역 뷰를 사용하지 않습니다.
- 다중 인스턴스 부하 분석의 완전성이 떨어집니다.

## 10) 별도 Session Monitor UI는 인스턴스 지정 kill 미지원
- `session_monitor.rs`는 `QueryExecutor::kill_session`(instance 없음)만 호출합니다.
- RAC 환경에서 원격 인스턴스 세션 kill 시 실패 가능성이 큽니다.

## 11) Session/Lock row 파싱에서 타입 변환 오류를 침묵 처리
- `row.get(idx).unwrap_or(None)` 패턴으로 변환 실패가 빈 문자열로 대체됩니다.
- 데이터 품질 문제(타입 불일치, 드라이버 변환 오류) 탐지가 어렵습니다.

## 12) Heavy execution row 파싱도 동일한 침묵 처리
- `get_heavy_execution_snapshot`에서 동일 패턴을 사용합니다.
- 숫자 컬럼 변환 실패가 조용히 사라집니다.

## 13) SQL Monitor row 파싱도 동일한 침묵 처리
- `query_sql_monitor_rows` 역시 `unwrap_or(None)`로 오류를 숨깁니다.
- 특히 kill 대상 식별 컬럼이 비정상일 때 원인 파악이 어렵습니다.

## 14) Scheduler jobs 조회 fallback에서 원인 정보 손실
- `get_scheduler_jobs_snapshot`은 `DBA -> ALL -> USER` 순으로 내려가며 중간 에러를 버립니다.
- 마지막 뷰 결과만 노출되어 최초 실패 원인을 추적하기 어렵습니다.

## 15) Scheduler history 조회 fallback에서도 원인 정보 손실
- `get_scheduler_job_history_snapshot`도 동일 패턴입니다.
- 운영자가 권한 문제인지, SQL 결함인지 구분하기 어렵습니다.

## 16) Data Pump jobs 조회 fallback에서도 원인 정보 손실
- `get_datapump_jobs_snapshot`에서 `DBA -> USER` fallback 중 원인 에러가 누락됩니다.
- 장애 분석 시 디버깅 정보가 부족합니다.

## 17) Data Pump 시작이 `job_mode => 'SCHEMA'`로 고정
- `start_datapump_job`에서 모드를 고정해 테이블/테이블스페이스/FULL 시나리오 확장이 불가합니다.
- 잘못된 dump 유형에서 런타임 실패 가능성이 큽니다.

## 18) Data Pump 중지 시 `keep_master => 1` 고정
- `stop_datapump_job`이 master table을 항상 유지합니다.
- 장기 운용 시 Data Pump 메타데이터 누적 부담이 생깁니다.

## 19) RMAN shell job이 `auto_drop => FALSE` 고정
- `create_and_run_shell_job`은 잡 완료 후 자동 정리되지 않습니다.
- 반복 실행 시 스케줄러 객체가 계속 누적됩니다.

## 20) Cursor Plan 행 파서가 `CHILD#`를 필수로 강제
- `parse_sql_id_child_row`는 `CHILD_NUMBER/CHILD#` 파싱 실패 시 SQL_ID까지 버립니다.
- SQL_ID만으로 조회 가능한 시나리오에서도 행 선택 자동 입력이 실패할 수 있습니다.

---

참고: 본 리포트는 코드 정적 검토 기준이며, DB 권한/버전/라이선스별 동작 차이는 별도 실환경 재현이 필요합니다.
