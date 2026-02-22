# SPACE Query 기능 기록

> 작성일: 2026-02-22  
> 목적: 현재 코드베이스에 반영된 기능을 한눈에 확인할 수 있도록 문서화

## 1) SQL 편집/실행

- 다중 쿼리 탭 생성/닫기/전환
- SQL 작성, 선택 영역 실행, 전체 실행
- SQL*Plus 스타일 스크립트 처리
  - `DEFINE`, `ACCEPT`, `PROMPT`, `WHENEVER` 지원
  - bind 변수/치환 변수 해석
- 쿼리 실행 결과를 데이터/메시지 탭으로 분리 표시
- 실행 이력(쿼리 히스토리) 저장 및 재사용

## 2) 데이터베이스 탐색/분석

- Oracle 오브젝트 브라우저 제공
  - Table, View, Procedure, Function, Sequence, Trigger, Synonym, Package
- Explain Plan 실행 및 결과 확인
- DBMS Output 조회

## 3) DBA 운영 도구

- Session/Lock 모니터링
- SQL Monitor
- Storage 점검
- Scheduler 관리 정보 조회
- Security 점검
- RMAN 관련 조회
- AWR/ASH 조회
- Data Guard 상태 조회

## 4) 에디터 생산성

- SQL 문법 하이라이팅
- IntelliSense(자동완성) 및 컨텍스트 기반 제안
- 찾기/바꾸기 다이얼로그
- 폰트 설정 및 테마 관리

## 5) 결과/로그/운영 편의 기능

- 결과 테이블 렌더링 및 결과 탭 관리
- 앱 로그 뷰어 제공
- 연결 정보 저장 및 불러오기
- OS Keyring 기반 비밀번호 저장

## 6) 안정성/품질 기반

- UTF-8 바이트 오프셋 기반 문자열/커서 처리 구조
- SQL 파서/스크립트/하이라이트/인텔리센스 관련 테스트 코드 보유
- 동시성 안전성 및 패닉 방지 가드 테스트 보유

## 7) 참고 소스

- 기능 개요: `README.md`
- SQL 실행 계층: `src/ui/sql_editor/execution.rs`, `src/db/query/executor.rs`
- 스크립트 처리: `src/db/query/script.rs`, `src/db/session.rs`
- DBA 도구: `src/ui/sql_editor/dba_tools.rs`, `src/ui/sql_editor/session_monitor.rs`
- 에디터/인텔리센스: `src/ui/sql_editor/query_text.rs`, `src/ui/sql_editor/intellisense.rs`, `src/ui/intellisense_context.rs`
- 결과/이력/로그: `src/ui/result_table.rs`, `src/ui/result_tabs.rs`, `src/ui/query_history.rs`, `src/ui/log_viewer.rs`
