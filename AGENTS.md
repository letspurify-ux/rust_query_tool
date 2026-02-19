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
