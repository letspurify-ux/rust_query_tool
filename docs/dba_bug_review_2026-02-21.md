# DBA 기능 버그 리뷰 (20건)

1. **Cursor Plan 행 선택 파싱 컬럼 불일치**  
   `parse_sql_id_child_row`는 0/1번 컬럼을 `SQL_ID`/`CHILD#`로 가정하지만, 최근 SQL 후보 쿼리 결과는 선두에 `INST_ID`가 포함됩니다. 선택 시 SQL_ID 자동 채움이 실패하거나 잘못된 값이 들어갈 수 있습니다.

2. **Cursor Plan 파싱이 CHILD# NULL/공백 행을 모두 버림**  
   `parse_sql_id_child_row`에서 `CHILD#`를 필수 숫자로 강제(`flatten()?`)하여, child 번호가 비어 있거나 표현 불가한 행은 SQL_ID조차 반영되지 않습니다.

3. **Session/Lock 스냅샷이 RAC 고려 없이 `v$session`만 사용**  
   모니터는 단일 인스턴스 뷰만 사용하므로 RAC 환경에서 다른 인스턴스 세션/락 정보를 누락합니다.

4. **Blocking serial 조회가 `inst_id` 없이 SID만 매칭**  
   blocking session serial 조회 서브쿼리에서 인스턴스 조건이 없어 RAC에서 다른 인스턴스의 동일 SID와 오매칭될 수 있습니다.

5. **Session Monitor Kill이 인스턴스 ID를 받지 않음**  
   UI kill 경로는 `kill_session`(instance 없음)만 호출하여 RAC에서 원격 인스턴스 세션 kill 실패 가능성이 큽니다.

6. **SQL Monitor kill 대상 파서가 0 SID/SERIAL 허용**  
   `parse_non_negative_i64`는 0을 허용합니다. 잘못된 데이터가 들어오면 비정상 `ALTER SYSTEM KILL SESSION '0,0'` 형태를 만들 수 있습니다.

7. **Scheduler Jobs 조회가 오류 원인 구분 없이 무조건 fallback**  
   DBA/ALL/USER 순서로 모든 에러를 삼키며 내려가므로 실제 SQL 문법/타입 오류가 권한 부족처럼 은폐됩니다.

8. **Scheduler History 조회도 무조건 fallback**  
   위와 동일하게 실제 장애 원인이 사라져 운영자가 원인 파악을 어렵게 만듭니다.

9. **Data Pump Jobs 조회도 무조건 fallback**  
   권한 이슈 외의 런타임 오류가 USER 뷰 fallback으로 가려집니다.

10. **AWR Top SQL 조회가 실패 시 shared pool로 의미를 바꿔 fallback**  
    AWR 실패가 단순 권한 문제인지 SQL 문제인지 구분 없이 실시간 shared pool 데이터로 대체되어 결과 의미가 달라집니다.

11. **ASH Session Activity 조회도 실패 원인 구분 없이 단계적 fallback**  
    `gv$active_session_history` 실패 시 `v$`, `gv$session`, `v$session`으로 무조건 내려가며 오류 원인이 은폐됩니다.

12. **ASH Top SQL 조회도 동일한 무조건 fallback 패턴**  
    권한 문제와 실제 쿼리 결함이 구분되지 않아 오탐/누락 가능성이 있습니다.

13. **SQL Monitor auto-refresh가 요청 중첩을 차단하지 않음**  
    tick마다 refresh를 보내며 이전 요청 취소/합치기 없이 새 스레드를 만들 수 있어 DB 및 UI 이벤트 큐 압박이 발생할 수 있습니다.

14. **RMAN job 생성 시 기존 동일 job_name 충돌 처리 부재**  
    `CREATE_JOB` 전에 기존 잡 존재 확인/정리 로직이 없어 재실행 시 ORA-27477로 실패할 수 있습니다.

15. **RMAN/Shell job `auto_drop => FALSE` 고정으로 잡 누적**  
    완료 후 잡이 자동 정리되지 않아 장기 운영 시 스케줄러 메타데이터가 계속 쌓입니다.

16. **Data Pump stop에서 `keep_master => 1` 고정**  
    stop 시 master table을 항상 남겨 객체 누적/정리 부담을 유발합니다.

17. **Data Pump import/export가 항상 `job_mode => 'SCHEMA'`**  
    입력 dump 성격(테이블/풀 DB)과 무관하게 스키마 모드 강제되어 일부 시나리오에서 동작 실패 가능성이 큽니다.

18. **보안 대시보드/스케줄러 owner 필터가 USER 뷰 fallback에서 의미가 왜곡**  
    USER 뷰에서는 본인 스키마만 보이는데도 owner 필터 UI를 그대로 받아 결과 0건으로 보일 수 있어 오판 유발.

19. **Session/Heavy snapshot row 추출에서 컬럼 타입 변환 에러를 침묵 처리**  
    `row.get(idx).unwrap_or(None)` 패턴으로 변환 오류를 빈 문자열로 삼켜 데이터 품질 문제를 숨깁니다.

20. **선택 행 기반 자동 입력 로직이 컬럼명보다 인덱스 의존적인 경로가 남아있음**  
    일부 파서는 인덱스 고정 방식(예: SQL_ID/CHILD#, SID/SERIAL)이라 쿼리 컬럼 순서 변경 시 즉시 오동작합니다.

---

## 참고 코드 위치
- `src/ui/sql_editor/dba_tools.rs`
- `src/ui/sql_editor/session_monitor.rs`
- `src/db/query/executor.rs`
