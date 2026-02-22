# 예외 처리 보완 내역

## 중(이상) 우선 수정

1. 연결 저장 실패 시 keyring 롤백 실패가 묵살되던 문제를 보완했습니다.
   - `DialogMessage::Save`와 `DialogMessage::Connect(save_connection=true)` 경로에서
     `delete_password` 실패를 무시하지 않고 에러 메시지에 함께 노출하도록 수정했습니다.
   - 이제 설정 저장 실패와 keyring 롤백 실패가 동시에 발생하면 사용자에게 두 실패 원인이 모두 표시됩니다.
