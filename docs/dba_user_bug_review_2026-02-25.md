# DBA 유저 기능 버그 검토 리포트 (Security Manager 중심)

검토 범위:
- `src/ui/sql_editor/dba_tools.rs` (Security Manager UI/액션 플로우)
- `src/db/query/executor.rs` (유저/권한/프로파일 조회 및 DDL 실행)

검토 방법(정적 분석):
- `rg -n "show_security_manager|normalize_security_view_filters|security_autofill_values|refresh_security_action_controls" src/ui/sql_editor/dba_tools.rs`
- `rg -n "get_users_overview_snapshot|get_user_summary_snapshot|get_user_role_grants_snapshot|get_user_system_grants_snapshot|get_user_object_grants_snapshot|get_profile_limits_snapshot|create_user|drop_user|grant_role_to_user|revoke_role_from_user|grant_system_priv_to_user|revoke_system_priv_from_user|lock_user_account|unlock_user_account|expire_user_password" src/db/query/executor.rs`
- `cargo test dba_feature_tests -- --nocapture`

## 주요 점검 결과

1. **Users 화면에서 `profile` 필터를 입력한 비-DBA 사용자는 조회 시 1차 실패를 항상 경험할 수 있음**
   - `all_users`에는 `profile` 컬럼이 없는데도, fallback 1차 SQL에서 `profile` 조건이 포함되어 ORA-00904를 유발한 뒤 재시도합니다.
   - 동작은 복구되지만, 매 요청마다 불필요한 실패/지연/로그 노이즈가 발생합니다.

2. **fallback 판단이 권한/뷰 부재 외 시나리오까지 넓어 실제 결함을 가릴 위험이 있음**
   - 보안 뷰 조회 로직들이 공통 fallback 정책(`should_fallback_from_global_view`)에 의존합니다.
   - SQL 변경/컬럼 오타와 권한 부족이 유사하게 처리되어 문제 분류 정확도가 떨어질 수 있습니다.

3. **Role/System/Object Grants 조회는 비-DBA fallback 시 "현재 로그인 사용자"만 조회 가능**
   - `ensure_user_view_matches_target_user` 제약으로 타 사용자 조회는 차단됩니다.
   - UX 관점에서는 정상 제한이지만, 입력한 타깃 유저와 실제 조회 대상이 다를 때 "권한 부족"이 아닌 "대상 불일치"로 안내가 부족해 오해 여지가 있습니다.

4. **Security Manager 입력 검증이 비quoted ASCII identifier 전제라 quoted 사용자/롤 운영 환경과 충돌 가능**
   - `normalize_required_identifier` / `is_ascii_identifier` 제약으로 공백/소문자 보존/특수문자가 필요한 quoted 식별자를 다룰 수 없습니다.
   - 표준 Oracle 운용 정책에서는 권장되지 않지만, 이미 quoted 식별자를 사용하는 레거시 환경에서는 기능 제약으로 나타납니다.

5. **빠른 액션(Quick Action)과 개별 버튼이 동일 액션을 중복 제공하면서 모드별 가드가 약함**
   - Profiles 뷰 외에는 대부분 액션 버튼이 활성화되어 있어, 현재 조회 컨텍스트와 무관한 DDL/DCL을 실수로 실행할 가능성이 있습니다.
   - 즉시 오류로 막히더라도 운영 UX에서는 "조회 컨텍스트 기반 가드" 강화 여지가 있습니다.

6. **Create User 비밀번호 입력은 공백 trim 없이 전달되어 오입력 복구가 어려움**
   - 비밀번호는 공백만 검사하고 원문을 그대로 SQL builder로 전달됩니다.
   - 의도된 정책일 수 있으나, 앞/뒤 공백이 섞인 오입력에 대한 경고가 없어 계정 생성 직후 로그인 실패(사용자 오해)로 이어질 수 있습니다.

7. **액션 성공 후 자동 재조회는 직전 필터를 그대로 재사용하여, 작업 결과가 즉시 안 보이는 것처럼 보일 수 있음**
   - 예: 유저 생성 직후 `user/profile` 필터가 기존 값으로 남아 있으면 결과 리스트에 변화가 안 보일 수 있습니다.
   - 기능 오동작은 아니지만 운영자 관점에서는 "액션 반영 실패"로 인식될 수 있는 UX 리스크입니다.

## 결론
- 현재 구현은 안전한 식별자 검증과 확인 다이얼로그를 통해 위험한 입력을 상당 부분 차단하고 있습니다.
- 다만 비-DBA fallback 경로의 불필요한 1차 실패, quoted 식별자 비호환, 모드/컨텍스트 기반 액션 가드 약화는 실제 현장에서 "버그처럼 체감"될 가능성이 큽니다.
- 후속 개선 우선순위는 다음을 권장합니다.
  1) `all_users` 경로에서 profile 조건 사전 제거(재시도 제거)
  2) fallback 사유 분류(권한/객체부재/SQL결함) 고도화
  3) 모드별 액션 enable 정책 세분화 및 실행 전 컨텍스트 경고 강화
