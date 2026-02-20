# AGENTS.md — SPACE Query 에이전트 작업 표준

> 최종 업데이트: 2026-02-20  
> 적용 범위: 이 파일이 위치한 디렉터리(저장소 루트) 전체

이 문서는 SPACE Query 저장소에서 작업하는 에이전트의 **실행 기준**입니다.  
목표는 다음 3가지입니다.

1. 사용자 요구사항을 정확히 반영한다.
2. UTF-8/바이트 오프셋 관련 버그를 방지한다.
3. FLTK UI 변경 시 회귀를 최소화한다.

---

## 1) 작업 우선순위

충돌 시 아래 우선순위를 따른다.

1. 시스템 / 개발자 / 사용자의 현재 지시
2. 하위 경로에 있는 더 구체적인 `AGENTS.md`
3. 이 문서
4. 일반적인 관례

---

## 2) Rust 문자열 처리 정책 (필수)

문자열/커서/토큰 위치 계산은 **항상 바이트 오프셋 기준**으로 처리한다.

- 모든 인덱스(`start`, `end`, cursor, selection)는 byte offset으로 저장/전달한다.
- 문자열 슬라이스 전에는 `is_char_boundary()`를 검증한다.
- 잘못된 오프셋은 panic 대신 가장 가까운 유효 경계로 보정(clamp)한다.
- 토큰 span은 `start..end`를 바이트 기준으로 유지한다.
- 성능 민감 경로에서 전체 버퍼 복제(`to_string`, `clone`)를 피한다.

금지 사항:

- 커서/범위 계산에 `.chars()` 기반 인덱싱 사용
- "글자 수" 기반으로 슬라이스 경계 계산
- UTF-8 경계 미검증 슬라이싱

위 규칙을 위반하는 변경은 반려 대상이다.

---

## 3) 코드베이스 빠른 맵

```text
src/
├── app.rs
├── main.rs
├── sql_text.rs
├── db/
│   ├── connection.rs
│   ├── session.rs
│   └── query/
│       ├── executor.rs
│       ├── script.rs
│       └── types.rs
├── ui/
│   ├── main_window.rs
│   ├── query_tabs.rs
│   ├── result_table.rs
│   ├── sql_editor/
│   │   ├── execution.rs
│   │   ├── intellisense.rs
│   │   └── query_text.rs
│   └── ...
└── utils/
    ├── config.rs
    ├── credential_store.rs
    └── logging.rs
```

팁:

- SQL 실행 플로우: `ui/sql_editor/execution.rs` ↔ `db/query/executor.rs`
- 에디터 텍스트/선택 관련: `ui/sql_editor/query_text.rs`
- 자동완성 컨텍스트: `ui/intellisense*`, `ui/sql_editor/intellisense.rs`

---

## 4) FLTK 작업 규칙

### 4.1 UI 갱신/스레드

- 상태 변경 후 UI 미반영 시 `widget.redraw()` → 필요 시 `app::redraw()` 점검
- 긴 작업(DB, 파일 I/O, 파싱)은 워커 스레드에서 수행
- UI 반영은 메인 루프(`app::awake()` 또는 채널)에서만 수행

### 4.2 콜백/상태 공유

- 기본 패턴은 `Rc<RefCell<T>>`
- 중첩 borrow는 짧게 유지하고 지역 변수로 분리
- 콜백 캡처가 위젯 수명주기를 넘지 않도록 close 시 정리

### 4.3 위젯 생성/레이아웃

- 임시 위젯/팝업 생성 전:
  1) `Group::try_current()`로 기존 그룹 저장
  2) `Group::set_current(None)`로 분리
  3) 생성 후 반드시 원복
- 탭 스크롤/닫기 후 위치 이상 시 `handle_overflow(TabsOverflow::Pulldown)` 재적용 고려
- 폰트/레이아웃 변경 직후 redraw 및 이벤트 flush로 메트릭 재계산 유도

### 4.4 이벤트 처리

- `Event::KeyDown`에서는 필요한 키만 consume
- 나머지 이벤트는 `false` 반환으로 기본 동작 보존

### 4.5 그리기 콜백

- draw 콜백은 렌더링 전용으로 유지
- 데이터 변경은 별도 경로에서 처리(재귀 redraw 방지)

### 4.6 Window 수명/해제 (메모리 누수 방지)

- `Window::default()`로 만든 top-level 다이얼로그/팝업은 `hide()`만으로 끝내지 말고, 종료 루프(`while dialog.shown()`) 이후 `Window::delete(dialog)`까지 호출한다.
- 팝업 레지스트리(`Rc<RefCell<Vec<Window>>>`)를 쓰는 경우, 먼저 `retain(...)`으로 레지스트리에서 제거한 뒤 `Window::delete(...)`를 호출한다.
- 탭/에디터 종료 시 재사용하지 않는 popup window(예: intellisense)는 콜백/데이터 슬롯 정리 후 명시 삭제한다.
- 삭제 전에는 필요 시 `was_deleted()`로 중복 삭제를 방지한다.
- `MenuButton::new(...)` 등 부모 없는 임시 위젯을 만들었으면 같은 스코프에서 반드시 `MenuButton::delete(...)`/`Widget::delete(...)`를 보장한다.
- 임시 위젯 생성 이후 함수 본문에서 조기 `return`으로 delete 경로를 건너뛰지 않는다. 필요하면:
  1) 선택 처리 로직을 클로저/내부 함수로 분리해 내부 `return`이 바깥 함수를 종료하지 않게 한다.
  2) 또는 정리 코드(delete)가 항상 실행되는 단일 종료 지점으로 제어 흐름을 구성한다.

---

## 5) 테스트/검증 원칙

- 변경 범위와 가장 가까운 테스트부터 실행한다.
- 문자열/커서 로직 수정 시:
  - UTF-8 다국어 문자열
  - 경계값(0, len, len-1, invalid mid-byte)
  - 선택 범위 역전/빈 범위
  를 최소 케이스로 검증한다.
- UI 변경 시 재현 가능한 수동 검증 절차를 커밋/PR 설명에 남긴다.

---

## 6) 변경 작성 원칙

- 작은 단위로 수정하고, 의도/이유가 드러나는 이름을 사용한다.
- 기존 스타일을 우선 존중하고, 대규모 리포맷은 목적이 있을 때만 수행한다.
- 핫패스에서 불필요한 할당/복제를 만들지 않는다.
- 패닉 유발 가능 코드(`unwrap`, 인덱스 슬라이싱)는 경계 검증으로 대체한다.

---

## 7) 커밋/PR 작성 가이드

- 커밋 메시지는 "무엇을/왜"가 드러나도록 작성한다.
- PR 본문에는 최소 다음을 포함한다.
  - 배경(문제 상황)
  - 변경 요약
  - 검증 방법(실행 명령)
  - 리스크/후속 과제

권장 프리픽스 예시:

- `fix:` 버그 수정
- `refactor:` 동작 변경 없는 구조 개선
- `feat:` 기능 추가
- `test:` 테스트 추가/수정
- `docs:` 문서 수정

---

## 8) 체크리스트 (작업 종료 전)

- [ ] 사용자 요구사항이 모두 반영되었는가?
- [ ] 문자열 인덱스/커서 로직이 바이트 오프셋 기준인가?
- [ ] UTF-8 경계 검증(`is_char_boundary`)이 필요한 위치에 있는가?
- [ ] UI 변경 시 redraw/스레드 경계가 안전한가?
- [ ] top-level FLTK window/dialog가 `hide` 후 `Window::delete`까지 수행되는가?
- [ ] `MenuButton`/부모 없는 임시 위젯이 조기 `return` 경로 없이 `delete`까지 항상 도달하는가?
- [ ] 관련 테스트/검증 명령을 실행했는가?
- [ ] 불필요한 파일/디버그 코드가 제거되었는가?
- [ ] cargo test 모두 통과했는가?

---

## 9) AGENTS.md 자체 수정 규칙

- `AGENTS.md`를 수정할 때는 상단의 `최종 업데이트` 날짜를 함께 갱신한다.
- 변경 의도가 모호하지 않도록, 규칙 추가/수정 시 "왜 필요한지"를 문장 단위로 명시한다.
- 동일한 지침을 중복으로 추가하지 말고, 기존 섹션으로 통합 가능한지 먼저 확인한다.
- 저장소 루트 규칙과 하위 경로 규칙이 충돌할 가능성이 있으면 우선순위(1절)를 기준으로 범위를 명시한다.
