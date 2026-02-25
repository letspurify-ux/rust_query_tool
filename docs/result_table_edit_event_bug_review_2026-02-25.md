# Result Table 편집 이벤트 시나리오 점검 (2026-02-25)

## 범위
- `src/ui/result_table.rs`
- `src/ui/main_window.rs`
- `src/ui/sql_editor/mod.rs`

## 결론 요약
- 치명적 크래시는 보이지 않지만, **저장 요청 진행 중 동시 편집/새 쿼리/취소 이벤트가 섞일 때 상태 불일치 가능성**이 남아있다.
- 특히 `pending_save_request`를 사용하는 save 흐름은 실패 복구는 어느 정도 잘 되어 있으나, **요청 식별자(run id) 없이 단일 bool 플래그로만 연동**되어 있어 교차 이벤트에 취약하다.

---

## 잠재 버그 시나리오

### 1) Save 요청 후 결과 도착 전 추가 편집(입력/행삭제/행삽입) 허용
**관찰 근거**
- Save 시작 시 `pending_save_request = true`만 세팅한다.
- `insert_row_in_edit_mode`, `delete_selected_rows_in_edit_mode`, 셀 인라인 편집 경로에는 `pending_save_request` 차단 로직이 없다.
- 툴바 갱신도 `edit_active && can_edit` 기준이라 save pending 동안 액션 버튼이 계속 보일 수 있다.

**리스크**
- DB에는 Save 시작 시점 스냅샷이 반영되지만, 그 이후 사용자가 추가로 변경한 staged 상태는 같은 save 요청에 포함되지 않는다.
- 사용자는 "방금 수정한 것도 저장된 것"으로 오인할 수 있다.

**재현 예시**
1. Edit mode에서 1개 셀 수정
2. Save 클릭
3. 결과 도착 전 다른 행 삭제/삽입/수정
4. Save 성공 메시지 후 결과 재조회 시 일부 변경만 반영

**개선 방향**
- save pending 동안 편집 액션(입력/삽입/삭제/재-save)을 일시 비활성화.
- 또는 save 시작 시점부터 결과 도착까지 UI를 read-only로 전환.

---

### 2) Save 실패 결과와 일반 쿼리 실패 결과가 bool 플래그 하나로만 구분됨
**관찰 근거**
- `display_result`는 `pending_save_request`가 true였는지 여부만 보고 save 성공/실패 후처리를 결정한다.
- 실행 요청 단위를 구분하는 request id(또는 seq)가 없다.

**리스크**
- 이벤트 순서가 꼬이면(예: save 직후 별도 실행, 취소 이벤트 경합) 잘못된 결과를 save 결과로 처리할 여지가 있다.
- 현재는 단일 실행 제한(`is_query_running`)이 있어 확률을 낮추지만, 미래 확장(병렬 탭 실행/비동기 개선) 시 회귀 가능성이 높다.

**개선 방향**
- `pending_save_request: bool` → `pending_save_request_id: Option<u64>`로 변경.
- `QueryProgress::StatementFinished` 또는 `QueryResult`에 request id를 같이 전달해 정확히 매칭.

---

### 3) Save 진행 중 새 쿼리 시작 시 start_streaming이 테이블 데이터를 먼저 초기화
**관찰 근거**
- `start_streaming`은 save pending 여부와 상관없이 `pending_rows/pending_widths/full_data/source_sql`를 먼저 clear한다.
- save pending이면 `edit_session`은 유지하지만 데이터는 이미 비워진다.

**리스크**
- save 요청 직후(또는 취소/실패 경합) 사용자가 다른 실행을 시작하면 staged 화면 문맥이 먼저 사라져 디버깅/복구 UX가 급격히 나빠진다.
- save 실패가 뒤늦게 오더라도 사용자가 직전 staged 데이터 맥락을 잃었을 수 있다.

**개선 방향**
- save pending 상태에서는 새 실행을 차단하거나,
- 최소한 save 성공/실패 확정 전에는 기존 `full_data`를 보존하고 오버레이 상태로 표시.

---

### 4) Save pending 중 Cancel Query 이후 편집 UX의 모호성
**관찰 근거**
- 편집 취소(`cancel_edit_mode`)는 save pending이면 막는다.
- query cancel은 별도 경로(`cancel_all_running_queries`)로 동작하며, 결과 도착 타이밍에 따라 save pending 해제가 늦을 수 있다.

**리스크**
- 사용자는 쿼리 취소를 눌렀는데도 "Cannot cancel edit mode while save is in progress."를 반복해서 볼 수 있다.
- 내부적으로는 일관성을 지키는 동작이지만 UX 관점에서는 교착처럼 보일 수 있다.

**개선 방향**
- save pending + cancel query 상태 전용 메시지("저장 취소 응답 대기 중") 제공.
- 일정 시간 초과 시 pending 상태를 안전하게 해제하는 fail-safe 정책 검토.

---

### 5) Save 성공 후 후속 자동 refresh 실패 시 사용자 체감 불일치
**관찰 근거**
- save 성공 시 edit session은 종료되지만, 후속 SELECT/표시 갱신은 별도 실행 결과에 의존한다.
- 네트워크/세션 이슈로 refresh 쿼리 실패 시 DB 반영과 UI 표시가 일시 불일치할 수 있다.

**리스크**
- "저장은 되었는데 화면은 실패" 또는 반대로 인지될 수 있어 운영 혼선을 유발.

**개선 방향**
- Save DML 성공/실패와 Refresh 성공/실패를 분리 표기.
- 상태바/토스트에 "DML 적용 완료, 결과 재조회 실패" 같은 2단계 메시지 제공.

---

## 권장 회귀 테스트 (우선순위 순)
1. Save 클릭 직후(응답 전) 셀 수정/행삭제/행삽입이 차단되는지.
2. Save 실패 후 staged 데이터가 보존되고 재-save 가능 상태인지.
3. Save pending 중 Query Cancel 시 pending 해제/버튼 상태/에러 메시지가 일관적인지.
4. Save 요청과 일반 실행 요청의 결과가 request id로 정확히 매칭되는지(향후 개선 시).
5. Save 성공 + refresh 실패를 분리 상태로 사용자에게 노출하는지.

## 현재 코드에서 긍정적으로 보이는 방어 로직
- Save 실패 시 staged edits를 유지하는 로직이 존재.
- Save 중 edit mode 취소를 금지해 중간 롤백/유실을 방지.
- "다른 쿼리 실행 중" 체크로 동시 실행 가능성을 낮춤.
