# SPACE Query

Rust + FLTK 기반의 Oracle SQL 클라이언트입니다. 데스크톱 환경에서 SQL 작성/실행, 결과 조회, 오브젝트 탐색을 지원합니다.

## 주요 기능

- 다중 SQL 탭 편집 및 실행
- Oracle 오브젝트 브라우저 (Table/View/Procedure/Function/Sequence/Trigger/Synonym/Package)
- SQL 문법 하이라이팅 및 IntelliSense
- DBMS Output 조회, Explain Plan 실행
- 세션 상태 기반 스크립트 처리 (`DEFINE`, `ACCEPT`, `PROMPT`, `WHENEVER`, bind 변수 등)
- 결과 탭 분리(데이터/메시지) 및 쿼리 히스토리
- 저장된 연결 정보 + OS Keyring 기반 비밀번호 저장

## 프로젝트 구조

```text
src/
├── app.rs
├── main.rs
├── db/
│   ├── connection.rs
│   ├── session.rs
│   └── query/
│       ├── executor.rs
│       ├── script.rs
│       ├── types.rs
│       └── query_tests.rs
├── ui/
│   ├── main_window.rs
│   ├── sql_editor/
│   ├── object_browser.rs
│   ├── result_table.rs
│   ├── result_tabs.rs
│   ├── query_tabs.rs
│   ├── syntax_highlight/
│   ├── intellisense*.rs
│   ├── connection_dialog.rs
│   ├── settings_dialog.rs
│   └── ...
└── utils/
    ├── config.rs
    └── credential_store.rs
```

## 실행

```bash
./run.sh
```

> macOS에서는 `run.sh`가 `DYLD_LIBRARY_PATH`를 설정해 Oracle Instant Client를 참조합니다.

## 빌드 (`SPACE Query` 실행파일 생성)

```bash
./build_space_query.sh
```

릴리즈 빌드:

```bash
./build_space_query.sh --release
```

빌드 후 `target/<profile>/SPACE Query` 실행파일이 생성됩니다.

## 테스트

```bash
cargo test
```
