# 일반 유저 기능 버그 점검 20건 (2026-02-22)

아래 항목은 일반 사용자 기능(연결/히스토리/로그/설정/UI 상호작용) 중심의 정적 코드 리뷰 결과입니다.

1. **히스토리 writer 스레드가 죽으면 자동 복구가 불가**  
   `OnceLock`에 sender를 1회만 저장하고, writer 종료 후에도 동일 sender를 계속 사용합니다. 이후 모든 히스토리 저장이 fallback 경로에 의존합니다.

2. **로그 writer 스레드도 동일한 단일 sender 고정 문제**  
   로그 writer도 `OnceLock` 단일 sender 구조라 writer가 종료되면 영구적으로 끊긴 상태가 됩니다.

3. **히스토리 추가 실패가 호출자에게 전달되지 않음**  
   `add_to_history`는 `Result`를 반환하지 않아 실패해도 호출 측에서 실패 상태를 알 수 없습니다.

4. **히스토리 fallback에서 UI alert를 직접 호출**  
   `add_to_history` fallback은 즉시 `fltk::dialog::alert_default`를 호출하는데, 이 함수가 메인 UI 루프 외 스레드에서 호출될 가능성이 있습니다.

5. **히스토리 snapshot timeout 시 stale 데이터 표시 가능**  
   snapshot 수신 timeout이면 디스크를 다시 읽는데, 최근 메모리 변경분이 아직 flush되지 않았다면 최신 목록이 누락됩니다.

6. **히스토리 flush timeout이 고정 5초**  
   느린 디스크/백신 스캔 환경에서 정상 저장도 timeout으로 오판될 수 있습니다.

7. **오류 라인 파싱이 마지막 숫자를 선택하는 구조**  
   `parse_error_line`이 후보를 모두 모은 뒤 `last()`를 쓰므로, 실제 원인 line이 아닌 후행 stack line(예: ORA-06512)로 강조될 수 있습니다.

8. **URI 자격증명 마스킹이 입력을 변형할 수 있음**  
   `userinfo`에 `:`가 없으면 강제로 `:<redacted>`를 삽입합니다. 로그/히스토리 미리보기의 원문 포맷이 바뀝니다.

9. **CONNECT fallback 마스킹이 마지막 `@`에 의존**  
   비밀번호/식별자에 `@`가 포함된 케이스에서 경계 인식이 깨져 오탐 또는 과마스킹 가능성이 있습니다.

10. **히스토리 검색의 대소문자 무시가 ASCII 한정**  
    `to_ascii_lowercase` 기반이라 한글/다국어 검색은 기대한 case-insensitive 동작을 보장하지 못합니다.

11. **설정 폰트 검색도 ASCII 한정**  
    폰트 검색 역시 `to_ascii_lowercase` 기반이라 비ASCII 폰트명 검색 정확도가 떨어집니다.

12. **Find/Replace 대소문자 무시가 ASCII 한정**  
    비ASCII 문자열에서 case-insensitive 검색이 정확하지 않습니다.

13. **연결 테스트 중복 실행 방어 약함**  
    Test 클릭 시 스레드를 새로 띄우고, 비활성화는 메시지 큐를 통해 지연 적용됩니다. 빠른 연타로 중복 테스트가 들어갈 수 있습니다.

14. **연결 저장 실패 시 키링 상태 롤백 부재**  
    `add_recent_connection`은 먼저 keyring에 저장한 뒤 config에 반영합니다. 이후 `cfg.save()` 실패 시 keyring에는 남고 config에는 없는 불일치가 생깁니다.

15. **저장 연결 삭제 실패(키링) 시 부분 실패 UX 불명확**  
    삭제는 keyring 삭제 실패 시 즉시 에러를 반환해 config 항목 삭제도 중단합니다. 사용자는 “목록 삭제 실패/성공”이 뒤섞인 상태 원인을 이해하기 어렵습니다.

16. **패스워드 메모리 삭제가 `unsafe` + shrink 패턴에 의존**  
    문자열 내부 버퍼 overwrite 후 `clear/shrink_to_fit`를 수행하지만, 중간 복사본/allocator 동작까지 완전 보장되지 않아 민감정보 잔존 위험이 있습니다.

17. **연결 다이얼로그에서 keyring 조회 에러 시 빈 비밀번호로 폼 갱신**  
    조회 실패 시 에러 alert 뒤 `String::new()`를 패스워드 칸에 넣어 기존 입력을 잃게 만듭니다.

18. **연결 검증에서 host 허용 문자가 과도하게 넓음**  
    host에 `_`/`:` 등을 광범위 허용해 실제 접속 불가능 문자열이 저장 단계에서 통과될 수 있습니다.

19. **히스토리 표시 SQL 길이 제한이 글자수 + `...` 부가 방식**  
    `max_len`으로 자른 뒤 항상 `...`를 붙여 최종 표시 길이가 사용자가 기대하는 제한값을 초과합니다.

20. **로그 flush도 고정 timeout 구조**  
    로그 flush는 고정 timeout 이후 즉시 실패를 반환해, 환경 지연을 영구 실패처럼 보이게 만들 수 있습니다.

## 근거 파일
- `src/ui/query_history.rs`
- `src/utils/logging.rs`
- `src/ui/connection_dialog.rs`
- `src/utils/config.rs`
- `src/db/connection.rs`
- `src/ui/settings_dialog.rs`
- `src/ui/find_replace.rs`
