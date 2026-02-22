# DBA 기능 버그 점검 리포트 (20건, 정적 분석)

검토 대상:
- `src/ui/sql_editor/dba_tools.rs`
- `src/ui/sql_editor/session_monitor.rs`
- `src/db/query/executor.rs`

## 1) 글로벌 뷰 fallback 판정이 ORA 코드 문자열 매칭에만 의존
`should_fallback_from_global_view`가 에러 코드 enum 분해가 아닌 메시지 문자열 contains 판정만 사용합니다. 드라이버 메시지 포맷 변경 시 fallback 동작이 깨질 수 있습니다.

## 2) ASH Session Activity fallback 시 INST_ID 정보 손실
`gv$active_session_history` 실패 후 `v$active_session_history` 경로에서 `inst_id`를 `'-'`로 고정해 RAC 분석 정확도가 떨어집니다.

## 3) ASH Top SQL fallback 시 INST_ID 정보 손실
ASH Top SQL도 fallback 경로에서 `inst_id`를 `'-'`로 반환해 인스턴스별 쏠림 분석이 어렵습니다.

## 4) SQL Monitor fallback(v$)의 INST_ID가 세션 컨텍스트 단일값
`v$sql_monitor` fallback에서 `SYS_CONTEXT('USERENV','INSTANCE')`를 모든 행에 동일 적용합니다. 백엔드가 다중 인스턴스 정보를 포함해도 구분이 불가능합니다.

## 5) Session/Lock fallback(v$)의 blocking serial 조회가 인스턴스 구분 불가
`v$session` fallback의 blocking serial 서브쿼리는 inst_id 컨텍스트가 없어 RAC 유사 시나리오에서 오해 가능성이 있습니다.

## 6) Session/Lock 스냅샷 row 파싱이 NULL/변환 실패를 `-`로 일괄 대체
`row.get(idx)?` 후 `unwrap_or_else("-")`로 처리해 데이터 이상과 진짜 NULL을 구분하지 못합니다.

## 7) Heavy Execution 스냅샷 row 파싱도 동일 문제
동일한 `unwrap_or_else("-")` 처리로 타입/변환 이상이 관찰 불가능합니다.

## 8) SQL Monitor row 파싱도 동일 문제
세션 킬 대상 컬럼 파싱 전에 정보가 `-`로 평탄화되어 원인 추적이 어렵습니다.

## 9) Data Guard overview가 조회 실패 시 지표를 `'-'`로 침묵 대체
`v$dataguard_stats` 실패 시 fallback이 성공하면 transport/apply lag를 전부 `-`로 표시해 장애와 단순 미수집이 구분되지 않습니다.

## 10) Data Guard destination fallback이 에러 유형 구분 없이 v$archive_dest로 다운그레이드
`v$archive_dest_status` 실패 시 조건부 분기 없이 `v$archive_dest`로 내려가므로 지표 의미가 바뀌는 것을 UI가 충분히 표시하지 못합니다.

## 11) Data Guard apply snapshot fallback이 서로 다른 뷰 스키마를 단일 화면에 혼합
`v$managed_standby` -> `v$dataguard_process` fallback에서 컬럼 의미가 다르지만 동일 테이블 포맷으로 제시되어 해석 오류 가능성이 있습니다.

## 12) Data Guard archive gap 결과에 `NO_GAP` 센티널 문자열 주입
실제 데이터 행과 센티널 행이 같은 컬럼으로 제공되어 후속 자동화가 숫자 컬럼으로 처리 시 깨질 수 있습니다.

## 13) Scheduler jobs fallback 체인의 원인 노출이 최종 에러 문자열에 과도 의존
중간 단계 실패를 구조화된 코드로 노출하지 않아 운영 자동화에서 권한 문제/객체 부재를 안정적으로 분류하기 어렵습니다.

## 14) Scheduler history fallback도 동일하게 구조화된 에러 코드 손실
`dba/all/user` 체인 실패 원인이 문자열 결합으로만 전달되어 프로그래매틱 처리에 취약합니다.

## 15) Data Pump jobs fallback 역시 동일 문제
`dba_datapump_jobs` 실패 후 `user_datapump_jobs`로 내려갈 때 에러 의미가 문자열로만 누적됩니다.

## 16) Data Pump start: TABLE/TABLESPACE 모드를 UI에서 제시하지만 서버에서 즉시 거부
UI 프롬프트는 `SCHEMA/TABLE/TABLESPACE/FULL`을 안내하지만 실행부는 TABLE/TABLESPACE를 "not supported"로 에러 처리해 UX 불일치 버그가 있습니다.

## 17) Data Pump start: schema_expr를 빈 문자열로 bind
schema 미지정(full 모드 등) 시 `schema_expr`를 빈 문자열로 bind하고 PL/SQL에서 `IS NOT NULL` 조건을 사용합니다. Oracle에서 빈 문자열은 NULL로 취급되지만 드라이버/타입 계층 변화 시 오동작 여지가 있습니다.

## 18) Session Monitor 수동 킬은 선택된 인스턴스 상태(selected_instance_id)에 의존
테이블 선택 상태와 `selected_instance_id` 상태가 어긋나면 잘못된 인스턴스로 kill 시도가 갈 수 있습니다.

## 19) SQL Monitor kill 대상 파싱에서 INST_ID 실패 시 None으로 강등
`parse_sql_monitor_session_target`가 INST_ID 파싱 실패를 에러로 처리하지 않고 None 처리해 RAC에서 인스턴스 지정 없는 kill SQL이 생성될 수 있습니다.

## 20) RMAN 잡명 자동 생성이 초 단위 timestamp + 시퀀스에 의존
프로세스 재시작 시 시퀀스가 0으로 리셋되고 timestamp 접두가 같으면 이름 충돌 가능성이 남아 있습니다(짧은 시간 내 반복 실행 환경).

---

비고:
- 본 문서는 정적 분석 결과이며, 실제 영향도는 DB 버전/권한/RAC 구성에 따라 달라질 수 있습니다.
