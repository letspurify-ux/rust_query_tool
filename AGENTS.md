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
