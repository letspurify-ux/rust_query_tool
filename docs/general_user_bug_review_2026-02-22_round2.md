# 일반 유저 기능 정밀 버그 점검 20건 (2026-02-22, 라운드2)

아래는 일반 사용자 기능(연결/설정/로그/히스토리/찾기) 중심 정적 리뷰에서 확인한 **잠재 버그 20건**입니다.

1. **로그 저장 경로를 못 구해도 `Ok(())`를 반환하는 silent failure**  
   `AppLog::save()`는 `log_path()`가 `None`이면 저장 없이 그대로 `Ok(())`를 반환합니다.

2. **로그 저장 임시 파일명이 고정(`json.tmp`)이라 멀티 인스턴스 충돌 위험**  
   서로 다른 프로세스가 같은 임시 파일명을 사용합니다.

3. **`rename_overwrite`가 대상 파일 삭제 후 rename하는 비원자 경로를 사용**  
   교체 중간에 장애가 나면 로그 파일이 사라질 수 있습니다.

4. **로그 전송 실패를 호출자/사용자에게 전달하지 않음**  
   `log()`에서 writer 전송 실패를 무시(`let _ = ...`)합니다.

5. **`clear_log()`는 재시작 복구 로직 없이 직접 sender에 전송**  
   `send_log_command()`를 쓰지 않아 writer가 죽은 경우의 자동 재생성이 적용되지 않습니다.

6. **로그 flush timeout이 환경 의존 지연에서 오탐 가능**  
   `recv_timeout` 기반 실패로 느린 디스크/백신 환경에서 false negative가 날 수 있습니다.

7. **연결 저장 직후 설정 저장 실패 시 keyring 롤백 실패를 무시**  
   설정 저장 실패 시 keyring 삭제를 시도하지만 그 실패는 무시됩니다.

8. **연결 삭제에서 keyring 삭제 실패 시 config 항목 삭제까지 중단**  
   일부 상태만 남는 부분 실패 상태가 발생할 수 있습니다.

9. **호스트 유효성 검사 규칙이 과도하게 느슨함**  
   `:`를 일반 host 문자열에서도 허용해 잘못된 host 형식이 통과할 수 있습니다.

10. **연결 정보 생성 시 비밀번호 빈 값 허용**  
   필수 입력으로 다루지 않아 빈 비밀번호 연결 시도가 가능합니다.

11. **연결 테스트 동시 실행 방지 상태가 단일 스레드 상태 메시지에 의존**  
   메시지 큐 지연 상황에서 중복 테스트 요청이 누적될 여지가 있습니다.

12. **URI 마스킹이 userinfo에 `:`가 없을 때 원문을 변형**  
   `user@host` 형태를 `user:<redacted>@host`로 바꿔 원문 포맷을 변경합니다.

13. **URI credential 경계 판별이 마지막 `@` 의존**  
   userinfo에 `@`가 포함된 케이스에서 오검출 가능성이 있습니다.

14. **히스토리 snapshot 타임아웃 시 디스크 fallback으로 최신 메모리 상태 누락 가능**  
   writer 메모리 최신 상태가 파일로 flush되기 전이면 stale 목록이 표시됩니다.

15. **히스토리 검색이 locale-aware 비교가 아닌 단순 소문자화 기반**  
   언어별 case-folding 기대와 다를 수 있습니다.

16. **Find/Replace의 case-insensitive 검색이 ASCII 전용**  
   비ASCII 대소문자 검색 정확도가 떨어집니다.

17. **설정 폰트 검색은 locale-aware가 아닌 `to_lowercase` 단순 포함 비교**  
   일부 언어/정규화 조합에서 검색 체감이 부정확할 수 있습니다.

18. **로그 뷰어 메시지 절단이 실제 표시 길이 제한을 넘길 수 있음**  
   본문을 잘라낸 뒤 `...`를 추가해 최종 길이가 `max_len`을 초과합니다.

19. **히스토리 SQL 절단도 동일하게 최종 길이 초과 가능**  
   `visible_len` 계산 뒤 `...`를 붙여 체감 제한과 불일치가 생길 수 있습니다.

20. **히스토리 표시용 오류 라인 파싱이 문자열 패턴 의존**  
   DB/드라이버 메시지 형식 변형 시 잘못된 라인을 강조하거나 미강조될 수 있습니다.

---

## 근거 코드 위치

- `src/utils/logging.rs` (`AppLog::save`, `rename_overwrite`, `log`, `clear_log`, `flush_log_writer`)
- `src/ui/connection_dialog.rs` (`build_connection_info`, 저장/삭제/테스트 흐름)
- `src/utils/config.rs` (`add_recent_connection`, `remove_connection`)
- `src/ui/query_history.rs` (`redact_uri_credentials`, `load_snapshot`, `truncate_sql`, 검색/오류라인 파싱)
- `src/ui/find_replace.rs` (`find_ascii_case_insensitive`)
- `src/ui/settings_dialog.rs` (`filter_font_names`)
- `src/ui/log_viewer.rs` (`truncate_message`)
