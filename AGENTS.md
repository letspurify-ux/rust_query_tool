# AGENTS.md - SPACE Query 작업 가이드

> 최종 업데이트: 2026-02-19

이 문서는 SPACE Query 저장소에서 작업하는 에이전트를 위한 기준 문서입니다.

## Rust String Policy (Mandatory)

All string processing MUST use byte offsets.

- Treat all indices and cursor positions as byte offsets.
- Token spans must store `start` and `end` as byte indices.
- Never use character-based indexing for cursor math.
- Do NOT use `.chars()` or character counts for slicing or position logic.
- Always validate with `is_char_boundary()` before slicing.
- No full-buffer cloning in hot paths.

If a change introduces character-based offset logic, it must be rejected.

## 프로젝트 구조

```text
src/
├── app.rs
├── main.rs
├── sql_text.rs
├── db/
│   ├── mod.rs
│   ├── connection.rs
│   ├── session.rs
│   └── query/
│       ├── mod.rs
│       ├── executor.rs
│       ├── query_tests.rs
│       ├── script.rs
│       └── types.rs
├── ui/
│   ├── mod.rs
│   ├── connection_dialog.rs
│   ├── constants.rs
│   ├── find_replace.rs
│   ├── font_settings.rs
│   ├── intellisense.rs
│   ├── intellisense_context.rs
│   ├── intellisense_context/
│   │   └── tests.rs
│   ├── log_viewer.rs
│   ├── main_window.rs
│   ├── menu.rs
│   ├── object_browser.rs
│   ├── query_history.rs
│   ├── query_tabs.rs
│   ├── result_table.rs
│   ├── result_tabs.rs
│   ├── settings_dialog.rs
│   ├── sql_depth.rs
│   ├── sql_editor/
│   │   ├── mod.rs
│   │   ├── execution.rs
│   │   ├── intellisense.rs
│   │   ├── query_text.rs
│   │   └── sql_editor_tests.rs
│   ├── syntax_highlight.rs
│   ├── syntax_highlight/
│   │   └── syntax_highlight_tests.rs
│   └── theme.rs
└── utils/
    ├── mod.rs
    ├── config.rs
    ├── credential_store.rs
    └── logging.rs
```

## FLTK 작업 노하우

- UI 상태 변경 후 화면이 갱신되지 않으면 `widget.redraw()` 또는 필요 시 `app::redraw()` 호출 여부를 먼저 확인합니다.
- 콜백 내부에서 공유 상태를 다룰 때는 `Rc<RefCell<T>>` 패턴을 기본으로 사용하고, 중첩 borrow가 길어지지 않도록 지역 변수로 분리합니다.
- 긴 작업(쿼리 실행, 파일 I/O 등)은 UI 스레드를 블로킹하지 않도록 워커 스레드에서 처리하고, UI 반영은 `app::awake()`/채널 기반 메시지로 메인 루프에서 수행합니다.
- `TableRow`, `TextEditor` 같은 위젯 커스터마이징 시 draw 콜백에서는 그리기만 수행하고, 데이터 변경 로직은 별도 경로로 분리해 재귀 redraw를 피합니다.
- 키 이벤트(`Event::KeyDown`) 처리 시 기본 핸들러와 충돌하지 않도록 필요한 키만 선별적으로 consume하고, 나머지는 `false`를 반환해 기본 동작을 유지합니다.
- 다이얼로그/모달 창은 생성 직후 `set_modal()`과 `show()` 호출 순서를 일관되게 유지하고, 닫힘 시점에 콜백/핸들러 캡처를 정리해 누수성 참조를 방지합니다.
- **위젯 생성 전 current group 분리/복구를 습관화**: 팝업/다이얼로그/임시 위젯 생성 시 `Group::try_current()`로 기존 parent를 저장하고, `Group::set_current(None)`으로 분리한 뒤 생성이 끝나면 반드시 원복합니다. 부모가 암묵적으로 잘못 잡히면 레이아웃 깨짐/생명주기 버그가 자주 납니다.
- **`Tabs` 오프셋은 overflow 재적용으로 안정화**: 탭 닫기나 휠 스크롤 이후 탭 스트립 위치가 틀어질 수 있어 `handle_overflow(TabsOverflow::Pulldown)` 재호출로 내부 오프셋을 리셋하는 패턴이 유효합니다.
- **`TextBuffer` 인덱스는 UTF-8 문자 인덱스가 아니라 바이트 오프셋**: 커서/선택 범위를 문자열 인덱스로 바로 쓰지 말고, 항상 바이트 경계를 검증하고 잘못된 중간 바이트는 이전 유효 경계로 clamp 합니다.
- **레이아웃/폰트 변경 직후에는 FLTK 재계산을 강제**: 테이블/에디터/폰트 설정 변경 후 즉시 반영이 필요하면 redraw와 이벤트 처리(예: pending redraw flush)를 통해 내부 메트릭 재계산 타이밍을 맞춥니다.
- **수동 생성한 FLTK 위젯은 정리 순서까지 고려**: parent 없는 위젯, 탭 내부 동적 위젯, 콜백이 걸린 컨트롤은 close/삭제 시 데이터 정리 후 위젯 삭제 순서를 명확히 지켜 use-after-free 성격의 문제를 피합니다.
