# 예외 처리 보완 내역

## 중(이상) 우선 수정

### [중] Clippy 경고(문서 주석 위치)로 인한 품질 게이트 실패
- **증상**: `cargo clippy --all-targets --all-features -- -D warnings` 실행 시 `src/sql_text.rs`의 파일 상단 doc comment 다음 빈 줄로 인해 `clippy::empty_line_after_doc_comments` 에러 발생.
- **원인**: 파일 모듈 설명을 outer doc(`///`)로 선언해 실제 함수 doc로 해석되는 형태였고, 뒤에 빈 줄이 있어 lint 위반.
- **수정**: 파일 상단 주석을 모듈 내부 문서 주석(`//!`)으로 변경.
- **효과**: 해당 lint 에러는 해소됨.

## 추가 확인 사항
- 전체 clippy에는 기존 코드 전반의 다수 lint(`unnecessary_map_or`, `arc_with_non_send_sync`, `items_after_test_module` 등)가 남아 있습니다. 이번 작업에서는 요청 즉시 조치 대상으로 확인된 항목을 우선 수정했습니다.

### [중] Clippy 경고(`filter(...).next_back()`/`map_or(false, ...)`) 정리
- **증상**: `cargo clippy --all-targets --all-features -- -D warnings`에서 `src/db/query/executor.rs`의 `clippy::filter_next`, `clippy::unnecessary_map_or`가 다수 보고됨.
- **원인**: 역방향 탐색 패턴을 `filter().next_back()`로 작성했고, `Option` 비교에서 `map_or(false, ...)`를 반복 사용.
- **수정**:
  - 역방향 탐색을 `iter().rfind(...)`로 치환.
  - `map_or(false, predicate)`를 `is_some_and(predicate)`로 치환.
- **효과**: 해당 구간의 Clippy 경고가 제거되어 품질 게이트 실패 요인 일부를 해소.
## 2026-02-22 추가 다건 수정 내역

### [중] DDL 오브젝트 타입 파싱 분기 단순화 및 오탐 여지 축소
- **증상**: DDL 헤더 파싱에서 이전 statement 탐색이 `filter().next_back()` 형태로 표현되어 가독성이 떨어지고, 조건식 유지보수 시 실수 가능성이 컸음.
- **수정**: 이전 span 검색을 `rfind(...)`로 변경하고, 헤더 키워드 검사 로직의 `map_or(false, ...)` 체인을 `is_some_and(...)`로 정리.
- **효과**: 조건 의도가 명확해져 DDL 타입 판별 분기 수정 시 회귀 가능성을 낮춤.

### [하] 인텔리센스 테스트의 불필요한 임시 벡터 제거
- **증상**: 테스트에서 단일 테이블 전달 시 `Some(&vec![...])`를 사용해 불필요한 힙 할당 발생.
- **수정**: `Some(&[...])` 슬라이스로 교체.
- **효과**: 테스트 코드 단순화 및 불필요한 할당 제거.
1. 연결 저장 실패 시 keyring 롤백 실패가 묵살되던 문제를 보완했습니다.
   - `DialogMessage::Save`와 `DialogMessage::Connect(save_connection=true)` 경로에서
     `delete_password` 실패를 무시하지 않고 에러 메시지에 함께 노출하도록 수정했습니다.
   - 이제 설정 저장 실패와 keyring 롤백 실패가 동시에 발생하면 사용자에게 두 실패 원인이 모두 표시됩니다.
