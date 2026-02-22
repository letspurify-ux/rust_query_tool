# 일반 유저 기능 버그 후보 20건 (코드 리뷰)

> 범위: 저장소 코드 정적 리뷰(실행 기반 재현 전 단계)

1. **연결 문자열 표시 오타(`@@`)**  
   `display_string()`이 `username@@host` 형태로 표시되어 UI상 연결 정보가 잘못 보입니다. (`@`가 2개) 사용자 혼란 유발.  
   - 근거: `src/db/connection.rs`의 `format!("{} ({}@@{}:{}/{})", ...)`.

2. **키링 저장 실패 시 비밀번호 유실 가능**  
   저장소 저장(`add_recent_connection`)에서 키링 저장 실패해도 즉시 `clear_password()`를 호출해 비밀번호를 지워 버립니다. 이후 재연결 시 비밀번호가 사라질 수 있습니다.  
   - 근거: `src/utils/config.rs` 170~175행.

3. **레거시 비밀번호 마이그레이션 실패 시에도 비밀번호 삭제**  
   레거시 평문 비밀번호 마이그레이션에서 keyring 저장 실패를 로그만 남기고, 곧바로 `clear_password()` 후 재저장합니다. 결과적으로 비밀번호 복구 불가 상태 가능.  
   - 근거: `src/utils/config.rs` 93~100행.

4. **config 경로 미결정 시 저장 실패를 성공으로 처리**  
   `config_path()`가 `None`이면 `save()`가 실제 저장 없이 `Ok(())`를 반환합니다. 설정이 사라지는데 UI는 저장 성공으로 인지할 수 있습니다.  
   - 근거: `src/utils/config.rs` 118~166행.

5. **history 경로 미결정 시 저장 실패를 성공으로 처리**  
   Query history도 path가 없으면 저장 동작 없이 `Ok(())` 반환합니다. 기록이 남지 않는 silent failure 가능.  
   - 근거: `src/utils/config.rs` 382~446행.

6. **히스토리 임시파일명이 고정이라 동시 실행 시 경합 위험**  
   `path.with_extension("json.tmp")`를 고정 사용해 다중 프로세스에서 임시파일 충돌 가능성이 있습니다.  
   - 근거: `src/utils/config.rs` 396행.

7. **저장된 연결 더블클릭 시 자동 저장 플래그 강제(true)**  
   리스트 더블클릭 연결에서 `Connect(info, true)`로 보내 기존 저장 정보를 의도치 않게 덮어쓸 수 있습니다(사용자 확인 없음).  
   - 근거: `src/ui/connection_dialog.rs` 309~320행.

8. **키링 조회 실패를 빈 문자열로 치환해 연결 실패 유발 가능**  
   저장 연결 선택 시 keyring 로드 실패를 `unwrap_or_default()`로 빈 비밀번호로 대체합니다. 즉시 더블클릭 연결 시 잘못된 자격증명으로 실패할 수 있습니다.  
   - 근거: `src/ui/connection_dialog.rs` 302~304행.

9. **백그라운드 히스토리 writer 실패 fallback에서 저장 오류 무시**  
   채널 단절 fallback 경로에서 `let _ = history.save();`로 에러를 버립니다. 사용자에게 실패 전파가 안 됩니다.  
   - 근거: `src/ui/query_history.rs` 910~917행.

10. **history flush 타임아웃(1.5s) 고정으로 오탐 실패 가능**  
    디스크/IO가 느린 환경에서 정상 저장 중에도 timeout 오류를 사용자에게 반환할 수 있습니다.  
    - 근거: `src/ui/query_history.rs` 29행, 123~130행.

11. **오류 라인 파서가 문맥 구분 없이 첫 `line` 숫자만 취함**  
    에러 메시지에 "line" 텍스트가 복수 존재하면 실제 SQL line이 아닌 숫자를 집을 수 있습니다(하이라이트 오작동).  
    - 근거: `src/ui/query_history.rs` 138~158행.

12. **`IDENTIFIED BY` 마스킹이 SQL 문맥(주석/문자열) 비인식**  
    단순 텍스트 탐색으로 마스킹하여, 주석/문자열 내부의 텍스트도 변형될 수 있습니다. history 미리보기 정확도 저하.  
    - 근거: `src/ui/query_history.rs` 256~345행.

13. **URI 자격증명 마스킹이 `user@host` 형태 미마스킹**  
    `userinfo`에 `:`가 없으면 그대로 통과시켜 일부 credential 패턴이 노출될 수 있습니다.  
    - 근거: `src/ui/query_history.rs` 394~403행.

14. **CONNECT 마스킹 fallback이 `@` 포함 비밀번호 케이스 취약**  
    `rfind('@')` 기반 파싱이라 비밀번호 내 `@`가 있을 때 경계 판단이 깨져 마스킹 실패/오탐 가능성이 있습니다.  
    - 근거: `src/ui/query_history.rs` 241~253행.

15. **히스토리 목록 로딩 실패 시 디스크 fallback으로 stale 데이터 표시 가능**  
    writer 스레드 snapshot timeout이면 바로 디스크 재로드를 사용해 방금 추가된 메모리 데이터가 반영되지 않을 수 있습니다.  
    - 근거: `src/ui/query_history.rs` 475~488행.

16. **연결 테스트 병렬 실행 제한 없음(버튼 연타)**  
    Test 버튼 클릭마다 스레드를 생성하며 중복 테스트 방지/disable이 없습니다. 느린 네트워크에서 테스트 폭주 가능.  
    - 근거: `src/ui/connection_dialog.rs` 464~471행.

17. **연결 삭제 시 키링 삭제 실패를 사용자에게 알리지 않음**  
    `remove_connection`에서 keyring delete 실패를 stderr만 기록하고 UI에 실패 상태를 전달하지 않습니다. 계정 잔존 가능.  
    - 근거: `src/utils/config.rs` 204~209행.

18. **`build_connection_info`가 host/service 기본 유효성(문자 집합) 검증 부재**  
    공백 trim 외 형식 검증이 없어 잘못된 값이 저장/재사용됩니다. 일반 사용자 입장에서 반복 실패 원인이 숨겨집니다.  
    - 근거: `src/ui/connection_dialog.rs` 24~62행.

19. **`QueryHistory::load`에서 legacy 마이그레이션 저장 실패 무시**  
    `let _ = history.save();`로 실패를 버려 마이그레이션 실패를 감지하기 어렵습니다.  
    - 근거: `src/utils/config.rs` 372~375행.

20. **연결 저장 개수 10개 고정으로 오래된 즐겨찾기 자동 소실**  
    사용자가 인지하지 못한 채 `truncate(10)`으로 과거 연결이 삭제됩니다(설정 옵션 없음). 일반 사용자 UX상 데이터 손실로 체감될 수 있습니다.  
    - 근거: `src/utils/config.rs` 183~184행.

