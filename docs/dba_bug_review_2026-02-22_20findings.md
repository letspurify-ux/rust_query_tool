# DBA 관련 기능 버그 검토 (20건)

검토 범위(정적 분석):
- `src/db/query/executor.rs`
- `src/ui/sql_editor/dba_tools.rs`
- `src/ui/sql_editor/session_monitor.rs`

## 1) Session/Lock fallback SQL이 RAC 전체가 아닌 현재 인스턴스만 조회
- `gv$` 실패 시 `v$session`/`v$lock`으로 다운그레이드되어 다중 인스턴스 세션/락이 누락됩니다.

## 2) Session/Lock fallback에서 blocking serial 조회가 인스턴스 미지정
- fallback 서브쿼리가 `v$session bs WHERE bs.sid = s.blocking_session`만 사용해 SID 중복 상황에서 오매칭 위험이 있습니다.

## 3) Session/Lock 스냅샷 값 수집 시 변환 실패 원인을 잃음
- `row.get(idx)?` 이후 `None`을 모두 `"-"`로 대체해 NULL과 실제 데이터 이상을 구분하기 어렵습니다.

## 4) Heavy execution fallback SQL도 RAC 전체가 아닌 현재 인스턴스만 조회
- `gv$` 실패 시 `v$session`/`v$sql`로 다운그레이드되어 클러스터 전체 관측이 깨집니다.

## 5) Heavy execution 스냅샷도 값 수집 시 오류 원인 손실
- `unwrap_or_else("-")` 패턴으로 변환 실패/NULL이 동일 문자열로 평탄화됩니다.

## 6) SQL Monitor(v$/gv$) 결과도 값 수집 시 오류 원인 손실
- SQL Monitor row parsing에서도 동일하게 `"-"`로 대체되어 진단성이 떨어집니다.

## 7) ASH Session Activity가 실패 원인별 분기 없이 단순 fallback
- `gv$` 실패 시 곧바로 `v$`로 내려가므로 권한 오류/객체 부재/쿼리 결함을 세밀하게 구분하지 못합니다.

## 8) ASH Top SQL도 동일한 단순 fallback 패턴
- `gv$` 실패 후 `v$`로 내려가며 실패 원인별 복구 전략이 없습니다.

## 9) AWR Top SQL fallback 조건이 글로벌뷰 fallback 헬퍼를 재사용
- AWR 관련 오류도 `should_fallback_from_global_view` 판정(942/1031/2030)에 의존해 컨텍스트 맞춤 분기가 부족합니다.

## 10) fallback 에러를 invalid_security_input_error로 래핑
- `chained_fallback_error`가 조회 실패를 보안 입력 오류 계열로 래핑하여 오류 분류가 왜곡됩니다.

## 11) Data Guard overview는 stats 실패 시 지표를 `-`로 채워 성공 처리
- fallback 쿼리에서 lag/finish time을 상수 `'-'`로 반환해 실제 실패를 빈 데이터처럼 보이게 만듭니다.

## 12) Data Guard archive gap는 `v$archive_gap` 단일 경로만 사용
- `gv$` 대안이나 권한별 대체 경로 없이 단일 쿼리 실패 시 전체 기능 실패로 이어집니다.

## 13) Scheduler jobs는 DBA/ALL/USER fallback 중간 의미를 결과 스키마에 표시하지 않음
- 조회가 어떤 뷰에서 나왔는지 결과 컬럼/메시지로 분리되지 않아 운영 해석이 어렵습니다.

## 14) Scheduler history도 DBA/ALL/USER fallback 출처가 결과에 드러나지 않음
- 동일 화면/컬럼으로 보이지만 데이터 가시 범위가 달라 오판 가능성이 있습니다.

## 15) Data Pump jobs도 DBA/USER fallback 출처가 결과에 드러나지 않음
- 권한 축소로 줄어든 결과인지 실제 데이터 부재인지 사용자 관점에서 식별이 어렵습니다.

## 16) Data Pump export API가 schema_name을 필수 인자로 강제
- `start_datapump_export_job(... schema_name: &str, job_mode: &str)` 시그니처로 FULL/TABLE/TABLESPACE 모드 UX와 충돌합니다.

## 17) Data Pump start에서 SCHEMA_EXPR 외 모드 필터 부재
- `TABLE`/`TABLESPACE` 모드에서도 메타데이터 필터를 추가하지 않아 실제로 원하는 범위 제어가 불가능합니다.

## 18) Session Monitor 선택 파서가 컬럼 미존재 시 인덱스 기본값(0/1) 사용
- SID/SERIAL# 컬럼이 없을 때도 첫 두 컬럼을 SID/SERIAL#로 간주하여 오탐지 위험이 있습니다.

## 19) Session Monitor에서 INST_ID 파싱 실패를 조용히 None 처리
- `parse_selected_session_identity`가 INST_ID 오류를 무시해 인스턴스 없는 kill 경로로 내려갈 수 있습니다.

## 20) Cursor Plan 행 파서가 CHILD 파싱 실패 시 0으로 강제 보정
- `parse_sql_id_child_row`는 잘못된 CHILD 값을 `0`으로 치환해 의도치 않은 child cursor를 조회할 수 있습니다.

---

비고: 본 문서는 코드 정적 분석 기준이며, 실제 영향도는 DB 버전/권한/RAC 구성에 따라 달라질 수 있습니다.
