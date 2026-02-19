# CLAUDE.md - AI Assistant Guide for SPACE Query

> 최종 업데이트: 2026-02-19

이 문서는 SPACE Query 코드베이스에서 작업할 때 AI 어시스턴트가 빠르게 현재 구조를 파악할 수 있도록 정리한 가이드입니다.

## 1) 현재 코드베이스 개요

- 언어/빌드: Rust 2021 + Cargo
- GUI: FLTK (`fltk = 1.5`, `no-pango` feature)
- DB: Oracle Rust Driver (`oracle = 0.6`)
- 설정 저장: `serde`, `serde_json`, `dirs`
- 날짜/시간: `chrono = 0.4`
- 지연 초기화: `once_cell = 1.19`
- 비밀번호 저장: `keyring = 3` (플랫폼별 native backend)
  - macOS: `apple-native`
  - Windows: `windows-native`
  - Linux: `sync-secret-service` + `crypto-rust` (libdbus vendored)
- 빌드: `cc = 1.2` (`build.rs`)

엔트리포인트는 `src/main.rs`이며, `App::new()`에서 설정을 로드한 뒤 `MainWindow::run()`으로 UI를 시작합니다.

## 2) 실제 모듈 구조

```text
src/
├── app.rs
├── main.rs
├── sql_text.rs
├── db/
│   ├── mod.rs
│   ├── connection.rs           # 연결 정보 + Oracle 연결 래핑
│   ├── session.rs              # SQL*Plus 유사 세션 상태(변수/설정)
│   └── query/
│       ├── mod.rs
│       ├── executor.rs         # 쿼리 실행/스트리밍/오브젝트 메타데이터
│       ├── script.rs           # 스크립트 파싱/분리/툴 커맨드 해석
│       ├── types.rs            # QueryResult/결과 타입
│       └── query_tests.rs
├── ui/
│   ├── mod.rs
│   ├── main_window.rs          # 앱 오케스트레이션(탭/메뉴/상태)
│   ├── menu.rs                 # 메뉴바 구성 및 액션 바인딩
│   ├── sql_editor/
│   │   ├── mod.rs
│   │   ├── execution.rs        # 실행 버튼/단축키 처리 및 결과 전달
│   │   ├── intellisense.rs     # 에디터 IntelliSense 훅
│   │   ├── query_text.rs       # 에디터 텍스트/커서 위치 유틸
│   │   └── sql_editor_tests.rs
│   ├── sql_depth.rs            # SQL 토큰 괄호 depth 계산 유틸
│   ├── object_browser.rs       # DB 오브젝트 트리
│   ├── result_table.rs         # 결과 테이블 렌더링
│   ├── result_tabs.rs          # 데이터/메시지 탭 관리
│   ├── query_tabs.rs           # 다중 쿼리 탭
│   ├── query_history.rs        # 쿼리 히스토리 다이얼로그
│   ├── syntax_highlight.rs
│   ├── syntax_highlight/
│   │   └── syntax_highlight_tests.rs
│   ├── intellisense.rs         # IntelliSense 팝업/완성 목록
│   ├── intellisense_context.rs  # IntelliSense 컨텍스트 분석
│   ├── intellisense_context/
│   │   └── tests.rs
│   ├── connection_dialog.rs    # 연결 정보 입력 다이얼로그
│   ├── settings_dialog.rs      # 앱 설정 다이얼로그
│   ├── find_replace.rs         # 찾기/바꾸기 다이얼로그
│   ├── log_viewer.rs           # 애플리케이션 로그 뷰어
│   ├── font_settings.rs        # 폰트 목록 조회 및 선택
│   ├── theme.rs                # 앱 색상/테마 팔레트 정의
│   └── constants.rs            # UI 크기/레이아웃 상수
└── utils/
    ├── mod.rs
    ├── config.rs               # 설정/연결목록/히스토리 저장
    ├── credential_store.rs     # keyring 연동
    └── logging.rs             # 앱 로그 저장/조회(비동기 파일 쓰기)
```

## Rust String Policy (Mandatory)

All string processing MUST use byte offsets.

- Treat all indices and cursor positions as byte offsets.
- Token spans must store `start` and `end` as byte indices.
- Never use character-based indexing for cursor math.
- Do NOT use `.chars()` or character counts for slicing or position logic.
- Always validate with `is_char_boundary()` before slicing.
- No full-buffer cloning in hot paths.

If a change introduces character-based offset logic, it must be rejected.

## 3) 핵심 동작 포인트

### Query 실행 레이어

- `db/query/executor.rs`
  - 단건/배치 실행
  - SELECT 스트리밍 처리
  - DBMS Output on/off + fetch
  - Explain Plan 조회
  - 오브젝트 브라우징용 메타데이터/DDL 조회
- `db/query/script.rs`
  - SQL/PLSQL 블록 분리
  - 주석/문자열/블록 깊이를 고려한 statement split
  - SQL*Plus 스타일 명령 파싱 (`SET`, `DEFINE`, `PROMPT`, `WHENEVER`, `ACCEPT` 등)
- `db/session.rs`
  - 세션 변수, bind, 서버출력, compute 설정 등 실행 컨텍스트 보관
- `src/sql_text.rs`, `ui/sql_depth.rs`
  - SQL 식별자/키워드 판별 및 괄호 depth 계산 등 공통 유틸

### UI 레이어

- `ui/main_window.rs`
  - 앱 상태의 중심
  - 연결/해제, 메뉴 액션, 쿼리 탭/결과 탭/브라우저 동기화
- `ui/menu.rs`
  - 메뉴바 항목 정의 및 단축키 바인딩
- `ui/sql_editor/`
  - `mod.rs`: SQL 에디터 위젯 조합 및 초기화
  - `execution.rs`: 실행 버튼/단축키 처리, 쿼리 분리 후 결과 탭 전달
  - `intellisense.rs`: 타이핑 중 IntelliSense 트리거/팝업
- `ui/object_browser.rs`
  - 스키마 오브젝트 조회 및 선택 액션
- `ui/log_viewer.rs`
  - Application log 뷰어(레벨 필터/상세 표시/내보내기/삭제)
- `ui/find_replace.rs`
  - 에디터 내 찾기/바꾸기 다이얼로그
- `ui/font_settings.rs`
  - 시스템 폰트 목록 조회 및 에디터 폰트 적용
- `ui/theme.rs`
  - 앱 색상 팔레트 및 스타일 상수
- `ui/constants.rs`
  - 버튼/다이얼로그 등 UI 전반에 사용되는 크기/레이아웃 상수

### 설정/보안

- 연결 정보/최근 기록/히스토리는 `utils/config.rs`
- 비밀번호는 평문 config 대신 `utils/credential_store.rs`를 통해 OS keyring 사용
- 런타임 로그는 `utils/logging.rs`의 `AppLog` + 비동기 writer로 유지/영속화

## 4) 작업 시 권장 워크플로우

1. 변경 범위를 먼저 확정 (`db`, `ui`, `utils` 중 어디인지).
2. 관련 테스트 파일이 있으면 우선 확인:
   - `src/db/query/query_tests.rs`
   - `src/ui/sql_editor/sql_editor_tests.rs`
   - `src/ui/intellisense_context/tests.rs`
   - `src/ui/syntax_highlight/syntax_highlight_tests.rs`
3. 최소 변경 원칙 유지:
   - UI 변경은 `main_window.rs`에서 상태 전달 경로를 먼저 확인
   - 실행 로직 변경은 `executor.rs`와 `script.rs` 경계 유지
4. 변경 후 `cargo test`로 회귀 확인.

## 5) 자주 하는 실수

- 존재하지 않는 예전 모듈 경로를 기준으로 수정 시도
- SQL split 로직 변경 시 주석/문자열/PLSQL 블록 케이스 누락
- 실행 결과 타입(`types.rs`)과 UI 결과 렌더링(`result_table.rs`, `result_tabs.rs`) 불일치
- 로그 정책 변경 시 `utils/logging.rs`와 `ui/log_viewer.rs` 일관성 누락
- 연결 정보 수정 시 keyring 동기화 누락

## 6) 빠른 명령어

```bash
# 실행
./run.sh

# 빌드
./build_space_query.sh --release

# 테스트
cargo test
```
