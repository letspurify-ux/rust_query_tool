# Result Table 편집 이벤트 시나리오 점검 (2026-02-25)

## 점검 범위
- `src/ui/result_table.rs`
- `src/ui/result_tabs.rs`
- `src/ui/main_window.rs`

## 결론 요약
- 편집 체크/입력/삭제/취소/쿼리 실행/쿼리 취소/쿼리 실패/set null의 핵심 경합은 전반적으로 방어 코드가 잘 들어가 있다.
- 특히 save pending 상태에서 입력/삽입/삭제/취소/set null를 막는 경로와, 실패 시 staged edit를 보존하는 경로가 확인된다.
- 다만 아래 3개 시나리오는 여전히 주의가 필요하다.
  1) save 결과 시그니처 정합성에 의존하는 상태 해제 경로
  2) 쿼리 시작 시점 backup 복구가 `BatchFinished` 이벤트 전달에 의존하는 경로
  3) FLTK 위젯 테스트에서 inline editor 해제 시 비결정적 abort가 발생하는 경로(재현됨)

---

## 이벤트별 현행 방어 상태

### 1) Edit check on/off
- on: `begin_edit_mode()`에서 ROWID/대상 테이블/편집 가능 컬럼/중복 ROWID를 모두 검증한다.
- off: `cancel_edit_mode()`에서 원본 row order 기반 복원 수행.
- save pending 중에는 cancel을 차단한다.

### 2) 셀 입력(인라인 편집)
- save pending 중 진입 차단.
- Save/삭제/쿼리 시작/결과 반영 시점에 `commit_active_inline_edit()`를 호출해 마지막 입력 유실을 줄인다.

### 3) 삭제 / 삽입
- save pending 중 삽입/삭제 차단.
- 삭제 전 inline edit commit 처리로 stale index write를 피한다.

### 4) Set Null
- save pending 중 차단.
- editable 셀만 대상으로 하며 ROWID 컬럼은 제외한다.
- explicit null 플래그를 별도 추적해 SQL 생성 시 `NULL` literal로 반영한다.

### 5) 쿼리 실행/취소/실패
- save 실행 시 `pending_save_request + pending_save_sql_signature`를 세팅한다.
- save 응답은 SQL 시그니처 매칭 시에만 pending을 해제한다.
- save 실패 시 edit session을 유지하고, 일반 쿼리 실패 시에도 edit active이면 staged edit를 유지한다.
- select 시작 시 임시 backup을 저장하고, `BatchFinished`에서 orphan 정리로 복구를 시도한다.

---

## 잠재 버그 시나리오 (우선순위)

### P1) Save 응답 SQL 시그니처가 어긋나면 pending 해제가 늦어질 수 있음
**현상**
- `display_result()`는 `pending_save_sql_signature`와 `result.sql` 정규화 값이 일치할 때만 save 완료로 처리한다.
- 미일치 시 결과를 무시하고 save pending 상태를 유지한다.

**리스크**
- 실제 save가 DB에서 끝났는데도 UI는 save pending으로 남아 편집 액션이 잠길 수 있다.
- 현재는 `BatchFinished`의 orphan 정리로 회복하지만, 사용자는 일시적으로 멈춘 것처럼 느낄 수 있다.

**권장 보완**
- SQL 텍스트 매칭 외에 실행 request id(증분 시퀀스) 기반 매칭을 병행.
- save 시작/완료/정리 타임스탬프를 남겨 디버깅 가능성 강화.

### P2) Select 시작 후 `BatchFinished` 누락 시 backup 복구 타이밍 지연
**현상**
- `start_streaming()`에서 edit session backup 저장 후, 최종 정리는 statement result 또는 `clear_orphaned_query_edit_backup()`(BatchFinished 후) 경로에 의존한다.

**리스크**
- 예외 종료로 batch finish 이벤트가 누락되면 복구가 지연될 수 있다.

**권장 보완**
- 쿼리 취소/연결 전환 등 강제 종료 경로에서 orphan 정리를 즉시 트리거.
- backup 스테이트에 생성 시각/원인 태그를 넣어 stale backup 자동 청소 조건 강화.

### P3) (재현) result_table 테스트 묶음 실행 시 FLTK Input delete abort 가능
**현상**
- `cargo test result_table::` 실행 중 `start_streaming_commits_active_inline_edit_while_save_is_pending` 부근에서 FLTK native panic/abort가 간헐이 아닌 반복 재현된다.
- 동일 테스트 단독 실행은 통과한다.

**리스크**
- 런타임 치명 버그라기보다 테스트 환경/위젯 생명주기 경합 성격이 크지만, inline editor delete 경로 안정성 검증이 어렵다.

**권장 보완**
- 해당 테스트 그룹을 UI-thread 직렬 구동하도록 분리(`serial` 전략 또는 단일 UI test harness).
- `commit_active_inline_edit()`의 Input delete 전후 조건을 더 엄격히 로깅해 재현 시 추적 가능성 확보.

---

## 빠른 회귀 체크리스트
1. save 클릭 직후 result toolbar(삽입/삭제/save/cancel/edit check)가 비활성화되는지.
2. save 실패 후 staged 데이터가 남고 즉시 재시도 가능한지.
3. 일반 쿼리 실패/취소가 edit session을 의도치 않게 날리지 않는지.
4. set null 이후 저장 SQL이 `NULL` literal을 정확히 생성하는지.
5. batch 중단/취소 후 orphan recovery 메시지와 상태가 일치하는지.

## 참고 실행 로그
- 단일 시나리오 테스트는 대체로 통과.
- `cargo test result_table::` 및 `cargo test result_table:: -- --test-threads=1`은 현재 환경에서 FLTK abort를 재현함.
