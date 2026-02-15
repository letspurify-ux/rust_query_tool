# CLAUDE.md - AI Assistant Guide for SPACE Query

이 문서는 SPACE Query 코드베이스에서 작업할 때 AI 어시스턴트가 빠르게 현재 구조를 파악할 수 있도록 정리한 가이드입니다.

## 1) 현재 코드베이스 개요

- 언어/빌드: Rust 2021 + Cargo
- GUI: FLTK (`fltk = 1.5`)
- DB: Oracle Rust Driver (`oracle = 0.6`)
- 설정 저장: `serde`, `serde_json`, `dirs`
- 비밀번호 저장: `keyring` (플랫폼별 native backend)

엔트리포인트는 `src/main.rs`이며, `App::new()`에서 설정을 로드한 뒤 `MainWindow::run()`으로 UI를 시작합니다.

## 2) 실제 모듈 구조 (중요)

```text
src/
├── app.rs
├── main.rs
├── db/
│   ├── connection.rs       # 연결 정보 + Oracle 연결 래핑
│   ├── session.rs          # SQL*Plus 유사 세션 상태(변수/설정)
│   └── query/
│       ├── executor.rs     # 쿼리 실행/스트리밍/오브젝트 메타데이터
│       ├── script.rs       # 스크립트 파싱/분리/툴 커맨드 해석
│       ├── types.rs        # QueryResult/결과 타입
│       └── query_tests.rs
├── ui/
│   ├── main_window.rs      # 앱 오케스트레이션(탭/메뉴/상태)
│   ├── sql_editor/         # 에디터 + 실행/인텔리센스 결합
│   ├── object_browser.rs   # DB 오브젝트 트리
│   ├── result_table.rs     # 결과 테이블 렌더링
│   ├── result_tabs.rs      # 데이터/메시지 탭 관리
│   ├── query_tabs.rs       # 다중 쿼리 탭
│   ├── syntax_highlight.rs
│   ├── syntax_highlight/   # 하이라이트 테스트
│   ├── intellisense.rs
│   ├── intellisense_context.rs
│   ├── connection_dialog.rs
│   ├── query_history.rs
│   ├── settings_dialog.rs
│   └── 기타 UI 보조 모듈
└── utils/
    ├── config.rs           # 설정/연결목록/히스토리 저장
    └── credential_store.rs # keyring 연동
```

> 과거 문서에 있던 `src/ui/feature_catalog.rs`, `src/utils/feature_catalog.rs` 등은 현재 트리에 존재하지 않습니다.

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

### UI 레이어

- `ui/main_window.rs`
  - 앱 상태의 중심
  - 연결/해제, 메뉴 액션, 쿼리 탭/결과 탭/브라우저 동기화
- `ui/sql_editor/*`
  - SQL 입력, 실행 트리거, 인텔리센스 훅
- `ui/object_browser.rs`
  - 스키마 오브젝트 조회 및 선택 액션

### 설정/보안

- 연결 정보/최근 기록/히스토리는 `utils/config.rs`
- 비밀번호는 평문 config 대신 `utils/credential_store.rs`를 통해 OS keyring 사용

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
