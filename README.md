# SPACE Query

Rust + FLTK 기반 데스크톱 SQL 클라이언트입니다. 현재 소스 기준으로 Oracle과 MySQL/MariaDB를 함께 지원하며, 다중 탭 SQL 편집기, 오브젝트 브라우저, SQL 포매터, 스크립트 실행기, 결과 그리드, 로그/크래시 복구 기능을 한 애플리케이션에 묶고 있습니다.

## 현재 지원 범위

- Oracle
  - `oracle` crate 사용
  - Oracle Instant Client 필요
- MySQL / MariaDB
  - `mysql` crate 사용
  - Oracle Client 불필요

## 주요 기능

### 1. SQL 편집기

- 다중 SQL 탭
- `.sql` 파일 열기 / 저장 / 다른 이름으로 저장 / 닫기
- 문법 하이라이팅
- IntelliSense 팝업
- Find / Replace
- SQL 자동 포맷
- 주석 토글, 대소문자 변환
- 현재 문장 선택, 선택 영역 실행, 스크립트 전체 실행
- 실행 타임아웃 입력창 (`Timeout(s)`, 기본값 `60`, 비우면 제한 없음)

### 2. 실행 및 세션 제어

- `Ctrl+Enter`, `F5`, `F9` 기반 실행
- `F4` Quick Describe
- `F6` Explain Plan / EXPLAIN
- `F7` Commit, `F8` Rollback
- 메뉴 기반 Auto-Commit 토글
- 실행 중 연결 바쁨 상태 추적 및 중복 작업 방지

### 3. 스크립트 엔진

현재 코드는 단순 세미콜론 분리기가 아니라 별도 스크립트 파서와 세션 상태를 사용합니다.

- Oracle / SQL*Plus 계열 명령
  - `VAR`, `PRINT`
  - `SET SERVEROUTPUT`
  - `SHOW ERRORS`, `SHOW USER`, `SHOW ALL`
  - `DESC`
  - `PROMPT`, `PAUSE`, `ACCEPT`
  - `DEFINE`, `UNDEFINE`
  - `BREAK`, `COMPUTE`
  - `SPOOL`
  - `WHENEVER SQLERROR`, `WHENEVER OSERROR`
  - `@`, `START`, `CONNECT`, `DISCONNECT`, `EXIT`, `QUIT`
- MySQL / MariaDB 계열 명령
  - `USE`
  - `SHOW DATABASES`, `SHOW TABLES`, `SHOW COLUMNS`
  - `SHOW CREATE TABLE`
  - `SHOW PROCESSLIST`, `SHOW VARIABLES`, `SHOW STATUS`
  - `SHOW WARNINGS`, `SHOW ERRORS`
  - `DELIMITER`
  - `SOURCE`

### 4. 오브젝트 브라우저

- 필터 가능한 트리 UI
- DB 종류에 따라 루트 카테고리가 달라집니다.

Oracle:
- Tables
- Views
- Procedures
- Functions
- Sequences
- Triggers
- Synonyms
- Packages / Package Routines

MySQL / MariaDB:
- Tables
- Views
- Procedures
- Functions
- Triggers
- Events
- Sequences가 실제로 보일 때만 Sequences 카테고리 노출

### 5. 결과 뷰

- 데이터 탭 / 메시지 탭 분리
- CSV 내보내기
- 헤더 포함 복사
- 긴 셀 값 미리보기 제한 설정
- Oracle 단일 테이블 `ROWID` 기반 결과셋에 대해 staged edit 모드 지원
  - Insert
  - Delete
  - Save
  - Cancel
  - `Set Null`

주의:
- 결과 그리드 편집은 아무 쿼리나 되는 기능이 아닙니다.
- 현재 구현은 `ROWID`가 포함된 편집 가능한 Oracle 결과셋을 전제로 합니다.
- JOIN 결과나 `ROWID`를 안전하게 식별할 수 없는 결과는 편집 모드가 열리지 않습니다.

### 6. 저장/복구/진단

- 최근 연결 정보 저장
- 비밀번호는 OS Keyring에 저장
- 애플리케이션 로그 뷰어
- 로그 내보내기 / 로그 비우기
- panic 시 `crash.log` 기록
- 다음 실행 시 이전 크래시 리포트 표시
- 레거시 `oracle_query_tool` 설정/키링 네임스페이스에서 마이그레이션 지원

## 실행 방법

### 개발 실행

```bash
cargo run
```

### 릴리스 실행

```bash
cargo run --release
```

### 테스트

```bash
cargo test
```

## 빌드 및 런타임 참고

### Oracle Instant Client

Oracle 연결은 Instant Client가 필요합니다.

- macOS에서는 다음 경로를 자동 탐색합니다.
  - `/opt/oracle/instantclient_*`
  - `~/Downloads/instantclient_*`
- 자동 탐색이 맞지 않으면 `ORACLE_CLIENT_LIB_DIR`로 라이브러리 디렉터리를 직접 지정할 수 있습니다.

예시:

```bash
export ORACLE_CLIENT_LIB_DIR=/opt/oracle/instantclient_23_3
cargo run --release
```

### Linux 빌드 메모

`build.rs`는 제한된 환경에서 일부 X11 개발 라이브러리가 없을 때 테스트/빌드가 진행되도록 stub 라이브러리를 연결할 수 있습니다. 다만 실제 GUI 실행 환경에서는 일반적인 X11/데스크톱 런타임 의존성이 여전히 필요합니다.

## 데이터 저장 위치

OS별 실제 루트 경로는 `dirs` crate가 결정하고, 앱 이름은 `space_query`를 사용합니다.

- 설정 파일
  - `config_dir()/space_query/config.json`
- 앱 로그
  - `data_dir()/space_query/app.log.json`
- 크래시 로그
  - `data_dir()/space_query/crash.log`
- 비밀번호
  - OS Keyring (`space_query` 서비스명)

주의:
- 쿼리 히스토리 다이얼로그는 현재 코드상 세션 메모리 기반입니다.
- 연결 정보와 로그는 파일/키링에 저장되지만, 쿼리 히스토리는 영구 저장되지 않습니다.

## 소스 구조

```text
src/
├── main.rs                # panic hook + 앱 시작/종료
├── app.rs                 # 부트스트랩, 설정 로드, FLTK 초기화
├── db/
│   ├── connection.rs      # Oracle/MySQL 연결, Oracle Client 초기화, 세션/락 관리
│   ├── session.rs         # bind/define/spool/break/compute 등 세션 상태
│   └── query/
│       ├── executor.rs    # Oracle 실행기, 스트리밍, bind, ref cursor 처리
│       ├── mysql_executor.rs
│       ├── script.rs      # 스크립트 파서와 툴 명령 해석
│       └── types.rs
├── ui/
│   ├── main_window.rs     # 메인 창과 메뉴/툴바/상태바 오케스트레이션
│   ├── connection_dialog.rs
│   ├── object_browser.rs
│   ├── result_table.rs
│   ├── result_tabs.rs
│   ├── query_tabs.rs
│   ├── query_history.rs
│   ├── log_viewer.rs
│   ├── settings_dialog.rs
│   ├── find_replace.rs
│   └── sql_editor/        # 편집기, 실행, 포매터, IntelliSense
├── sql_text.rs            # 키워드/토큰/문자열/주석 규칙
├── sql_parser_engine/     # 구조 인식용 파서 엔진
├── sql_delimiter.rs       # delimiter/frame 보조 로직
├── sql_format.rs          # 포맷 frame context 유틸
└── utils/
    ├── config.rs
    ├── credential_store.rs
    └── logging.rs
```

추가 디렉터리:

- `tests/`
  - 멀티스레드/패닉 가드 회귀 테스트
- `test/`, `test_mariadb/`
  - 포매터/파서/분리기 회귀용 SQL 샘플

## 프로젝트 성격

이 코드는 단순 DB 연결 도구라기보다 아래 세 축이 큰 비중을 차지합니다.

- 데스크톱 UI (`FLTK`)
- SQL 구조 해석 / 포맷 / IntelliSense
- Oracle + MySQL/MariaDB 공존을 위한 실행기와 스크립트 상태 관리

그래서 변경 포인트를 찾을 때는 보통 다음 순서가 맞습니다.

1. UI 문제: `src/ui/`
2. 실행/스크립트 문제: `src/db/query/`, `src/db/session.rs`
3. 파싱/포맷 문제: `src/sql_text.rs`, `src/sql_parser_engine/`, `src/ui/sql_editor/formatter.rs`
