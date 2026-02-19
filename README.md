# SPACE Query

> 최종 업데이트: 2026-02-19

Rust + FLTK 기반의 Oracle SQL 클라이언트입니다. 데스크톱 환경에서 SQL 작성/실행, 결과 조회, 오브젝트 탐색을 지원합니다.

## 주요 기능

- 다중 SQL 탭 편집 및 실행
- Oracle 오브젝트 브라우저 (Table/View/Procedure/Function/Sequence/Trigger/Synonym/Package)
- SQL 문법 하이라이팅 및 IntelliSense
- DBMS Output 조회, Explain Plan 실행
- 세션 상태 기반 스크립트 처리 (`DEFINE`, `ACCEPT`, `PROMPT`, `WHENEVER`, bind 변수 등)
- 결과 탭 분리(데이터/메시지) 및 쿼리 히스토리
- 저장된 연결 정보 + OS Keyring 기반 비밀번호 저장
- 애플리케이션 로그 뷰어

## 프로젝트 구조

```text
src/
├── app.rs
├── main.rs
├── sql_text.rs
├── db/
│   ├── mod.rs
│   ├── connection.rs       # 연결 정보 + Oracle 연결 래핑
│   ├── session.rs          # SQL*Plus 유사 세션 상태(변수/설정)
│   └── query/
│       ├── mod.rs
│       ├── executor.rs     # 쿼리 실행/스트리밍/오브젝트 메타데이터
│       ├── script.rs       # 스크립트 파싱/분리/툴 커맨드 해석
│       ├── types.rs        # QueryResult/결과 타입
│       └── query_tests.rs
├── ui/
│   ├── mod.rs
│   ├── main_window.rs      # 앱 오케스트레이션(탭/메뉴/상태)
│   ├── menu.rs             # 메뉴바 구성
│   ├── sql_depth.rs        # SQL 토큰 괄호 depth 유틸
│   ├── sql_editor/
│   │   ├── mod.rs
│   │   ├── execution.rs    # SQL 실행 처리
│   │   ├── intellisense.rs # 에디터 IntelliSense
│   │   ├── query_text.rs   # 에디터 텍스트/커서 유틸
│   │   └── sql_editor_tests.rs
│   ├── object_browser.rs   # DB 오브젝트 트리
│   ├── result_table.rs     # 결과 테이블 렌더링
│   ├── result_tabs.rs      # 데이터/메시지 탭 관리
│   ├── query_tabs.rs       # 다중 쿼리 탭
│   ├── query_history.rs    # 쿼리 히스토리
│   ├── syntax_highlight.rs
│   ├── syntax_highlight/   # 하이라이트 테스트
│   │   └── syntax_highlight_tests.rs
│   ├── intellisense.rs
│   ├── intellisense_context.rs
│   ├── intellisense_context/
│   │   └── tests.rs
│   ├── connection_dialog.rs
│   ├── settings_dialog.rs
│   ├── find_replace.rs     # 찾기/바꾸기 다이얼로그
│   ├── log_viewer.rs       # 앱 로그 뷰어
│   ├── font_settings.rs    # 폰트 설정
│   ├── theme.rs            # 색상/테마 정의
│   └── constants.rs        # UI 크기 상수
└── utils/
    ├── mod.rs
    ├── config.rs           # 설정/연결목록/히스토리 저장
    ├── credential_store.rs # keyring 연동
    ├── logging.rs          # 애플리케이션 로그 저장/조회
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

`build_space_query.sh`는 빌드 후 `target/<profile>/SPACE Query`를 생성합니다.

## 테스트

```bash
cargo test
```
