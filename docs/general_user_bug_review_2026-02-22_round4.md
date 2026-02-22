# 일반 유저 기능 버그 정밀 점검 20건 (2026-02-22, round4)

정적 코드 리뷰 기준으로, 일반 유저가 직접 접하는 연결/검색/히스토리/로그/설정 기능에서 **재현 가능성이 높은 잠재 버그 20건**을 정리했습니다.

1. `redact_uri_credentials()`가 `userinfo` 내부 `@`를 구분하지 못해 마스킹 경계가 틀어질 수 있음 (`rfind('@')` 기반).  
2. URI authority 종료 문자를 하드코딩해(`'/' '?' '#' ...`) 비정형 URI에서 마스킹 범위가 과/소 처리될 수 있음.  
3. `parse_error_line()`가 파싱한 라인 번호를 SQL 전체 라인 수로 clamp 하지 않아, 잘못된 라인 하이라이트 가능성이 있음.  
4. 히스토리 스냅샷 요청 timeout 시 즉시 디스크 fallback(`QueryHistory::load`)으로 전환되어 최신 메모리 상태 누락 가능성이 있음.  
5. 히스토리 필터가 `to_lowercase` 기반 fold만 사용해서 locale-specific case mapping(예: 터키어)에서 기대 검색과 어긋날 수 있음.  
6. `truncate_sql()`이 길이 제한 후 `...`를 붙이는 정책이라 고정폭 UI에서 사용자 기대 문자열 길이와 실제 표시가 다를 수 있음.  
7. `find_unicode_case_insensitive_bounds()`가 시작/끝 후보를 전부 순회하는 O(n²) 구조라 긴 문서에서 Find 동작이 급격히 느려질 수 있음.  
8. Find/Replace `ReplaceAll` 경로는 새 문자열 구성 + 카운트 산출을 별도로 수행해(2회 스캔) 대용량 텍스트에서 지연이 커질 수 있음.  
9. Find/Replace에서 대소문자 무시 비교는 유니코드 fold를 쓰지만, 같은 옵션을 쓰는 다른 모듈은 ASCII 비교를 사용해 사용자 경험이 모듈별로 불일치함.  
10. 연결 삭제 시 `remove_connection()`이 keyring 삭제 실패를 반환해도 목록은 이미 `retain`으로 제거되어 부분 실패 상태가 UX에 혼란을 줄 수 있음.  
11. 연결 삭제 후 `cfg.save()` 실패 시 메모리 롤백은 하지만 keyring 삭제는 복구하지 않아 config/keyring 상태 불일치가 남을 수 있음.  
12. Host 검증이 ASCII 문자+`.`+`-`만 허용해 IDN/특수 표기 호스트를 과도하게 거부할 수 있음.  
13. IPv6는 bracketed + hex/`:`만 허용해 zone id(`%eth0`) 같은 유효 표기를 막음.  
14. Service name 검증 허용 집합이 고정되어 실제 환경별 서비스 식별자를 거부할 가능성이 있음.  
15. 연결 테스트는 백그라운드 스레드 결과를 채널로 보내는데, 다이얼로그가 먼저 닫히면 결과 메시지가 소실됨(사용자 피드백 누락).  
16. 테스트 중 다이얼로그 종료 시 스레드 취소/중단 제어가 없어 불필요한 백그라운드 작업이 계속됨.  
17. 로그 상세 프리뷰는 항목이 사라진 경우(`entry_index` 미존재) 이전 상세 텍스트를 유지해 stale detail 표시 가능성이 있음.  
18. `truncate_message()`가 모든 whitespace를 단일 공백으로 정규화해 원본 로그 포맷(개행/탭) 기반 문제 분석에 필요한 단서가 리스트에서 손실됨.  
19. 로그 export는 전체 출력을 한 번에 메모리 `String`으로 누적해 대량 로그에서 메모리 급증 위험이 있음.  
20. `take_crash_log()`가 `remove_file` 실패를 무시(`let _ = ...`)해 crash log가 반복 노출/잔존될 수 있음.

## 근거 파일

- `src/ui/query_history.rs`
- `src/ui/find_replace.rs`
- `src/ui/connection_dialog.rs`
- `src/ui/log_viewer.rs`
- `src/utils/config.rs`
- `src/utils/logging.rs`
- `src/ui/settings_dialog.rs`
