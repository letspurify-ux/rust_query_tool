# Oracle Thin Response Rules

기준 소스:

- `python-oracledb`: `/tmp/python-oracledb/src/oracledb/impl/thin/protocol.pyx`
- `python-oracledb`: `/tmp/python-oracledb/src/oracledb/impl/thin/messages/base.pyx`
- 현재 구현: `vendor/oracle-rs/src/connection.rs`

## 1. python-oracledb thin의 실제 응답 소비 순서

sync thin 기준 핵심 흐름은 `BaseProtocol._process_message()` 하나로 묶여 있다.

1. `message.send()`
2. `_receive_packet(message, check_request_boundary=True)`
3. `message.process(read_buf)`
4. `flush_out_binds`가 있으면 추가 요청/응답 1회
5. `_break_in_progress`가 살아 있으면:
   - `supports_oob`일 때만 `INTERRUPT` marker 추가 전송
   - 응답을 한 번 더 받음
   - `message.process(read_buf)`를 다시 실행
6. `call_status` 처리
7. `message.error_occurred`면 raise/retry

즉, python thin은 "첫 응답 1개 읽고 끝"이 아니다. marker/reset과 external cancel 후속 응답까지 같은 호출 안에서 마저 소비한다.

## 2. `_receive_packet()` 규칙

`_receive_packet()`은 단순 recv wrapper가 아니다.

핵심 규칙:

1. request boundary 검사는 `supports_end_of_response`와 함께 켠다.
2. 현재 packet이 `MARKER`면 caller로 그대로 넘기지 않고 즉시 `_reset()`을 수행한다.
3. 현재 packet이 `REFUSE`면 refuse payload를 끝까지 읽어서 에러 메시지로 만든다.

중요한 점:

- caller는 marker packet을 직접 처리하는 것이 아니라, `_receive_packet()`이 정리해 둔 "다음 정상 packet"을 처리하는 구조다.
- 이 규칙이 깨지면 어떤 경로는 marker를 직접 보고, 어떤 경로는 reset 후 payload를 보는 식으로 분기되어 cancel/에러 처리가 흔들린다.

## 3. `_reset()` 규칙

python thin의 `_reset()`은 아래 순서를 반드시 지킨다.

1. `RESET` marker 전송
2. `RESET` marker가 돌아올 때까지 marker packet 반복 소비
3. 그 뒤에도 추가 marker가 오면 계속 건너뜀
4. 첫 non-marker packet을 다음 `message.process()`가 읽을 수 있는 상태로 남겨 둠
5. `_break_in_progress = False`

실무적으로 중요한 규칙은 두 가지다.

- reset은 "marker 1개 받고 끝"이 아니다. `RESET` echo 이후의 연속 marker까지 버려야 한다.
- reset 이후 첫 non-marker packet이 실제 error/data payload다.

## 4. external cancel과 응답 소비의 관계

python thin의 external cancel은 `_break_external()`에서 시작한다.

- OOB 가능:
  - urgent OOB byte만 먼저 보냄
  - `_break_in_progress = True`
- OOB 불가:
  - `INTERRUPT` marker를 바로 보냄
  - `_break_in_progress = True`

그 다음 실제 호출 스레드에서 응답을 처리하면서:

1. 첫 응답을 받음
2. 필요하면 `_reset()`으로 marker 정리
3. `_break_in_progress`가 남아 있으면
   - OOB 경로에서는 여기서 `INTERRUPT` marker를 추가 전송
   - 응답을 한 번 더 읽음
   - 최종 ORA-01013 등을 소비

즉, cancel은 "보내는 쪽 한 번"으로 끝나지 않는다. 수신 측이 후속 응답까지 같은 호출 안에서 끝까지 먹어야 한다.

## 5. 공통 message type 소비 규칙

`message.process()` 안에서는 message type 경계를 정확히 맞춰야 한다.

| Message Type | 의미 | 소비 규칙 |
| --- | --- | --- |
| `4` | Error | `_process_error_info()` |
| `8` | Parameter | return parameter payload 전체 소비 |
| `9` | Status | `ub4 call_status` + `ub2 end_to_end_seq_num` |
| `15` | Warning | warning payload 전체 소비 |
| `23` | ServerSidePiggyback | opcode별 payload 전체 소비 |
| `29` | EndOfResponse | 응답 종료 |
| `33` | Token | `ub8` 1개 소비 |

여기서 한 필드라도 덜 읽으면 다음 message type 경계가 틀어져 이후 packet 전체가 어긋난다.

## 6. 우리 구현 점검 기준

우리 쪽 Oracle thin이 python thin과 맞으려면 최소한 아래가 성립해야 한다.

1. OOB check:
   - ACCEPT의 `CHECK_OOB` flag를 읽어야 한다.
   - handshake에서 urgent OOB byte + `RESET` marker를 보내되, 여기서 임의로 응답을 더 읽으면 안 된다.
2. marker/reset:
   - marker를 본 경로마다 제각각 처리하지 말고 같은 규칙으로 `RESET` 후 first non-marker packet까지 정리해야 한다.
3. external cancel:
   - `interrupt()`는 연결 종료가 아니라 cancel 의도를 기록하고 break를 보내야 한다.
   - 실제 실행 경로에서 후속 `INTERRUPT`/응답 소비까지 마무리해야 한다.
4. common message consumption:
   - `Warning`, `Token`, `Piggyback`, `Parameter`, `Status`, `EndOfResponse`를 각 파서가 빠짐없이 소비해야 한다.

## 7. 이번 검토에서 확인한 문제

cancel 관련 핵심 문제는 두 가지였다.

1. handshake OOB check가 python thin보다 다르게 동작했다.
   - urgent OOB byte + reset marker 전송 후 추가 응답을 기다리고 있었다.
   - python thin은 여기서 응답을 따로 읽지 않는다.
2. external cancel 후 후속 응답 소비가 중앙화되어 있지 않았다.
   - break를 보내도 실행 경로가 "첫 응답"만 읽고 끝날 수 있었다.
   - 그래서 ORA-01013이 와야 하는 후속 packet이 소비되지 않거나, marker/reset 이후 `INTERRUPT` follow-up이 빠질 수 있었다.

## 8. 이번 수정 방향

현재 수정 방향은 다음이다.

1. ACCEPT의 `supports_oob_check`를 저장
2. handshake OOB check를 python thin 순서에 맞춤
3. `interrupt()`는 connection close fallback 대신 `break_in_progress`를 세팅
4. query / plsql / dml / batch 응답 수신 경로에서:
   - marker면 reset 정리
   - pending break가 있으면 후속 `INTERRUPT` + 추가 응답 소비

## 9. 검증 기준

cancel 구현이 맞다고 보려면 최소 아래가 통과해야 한다.

1. `DBMS_LOCK.SLEEP(30)`이 1초 내외로 ORA-01013류 에러로 끝난다.
2. 같은 연결에서 바로 `SELECT 1 FROM dual`이 다시 성공한다.
3. `test/test_all.sql`이 Oracle thin으로 오류 없이 끝난다.

이 세 가지를 함께 만족해야 "연결 종료 fallback"이 아니라 정상 cancel이라고 볼 수 있다.

## 10. 이 Oracle XE에서 실제 관찰한 점

2026-04-18, 로컬 `system/password@localhost:1521/FREE` 기준으로 확인한 값:

- `protocol_version = 319`
- `supports_oob = false`
- `supports_oob_check = false`

즉 이 환경에서는 python-oracledb thin도 OOB urgent break를 쓰지 못하고 marker 기반 cancel 경로를 탄다.

실제 `python-oracledb` 3.4.2 thin으로 같은 `DBMS_LOCK.SLEEP(30)` + `conn.cancel()`을 테스트한 결과:

- 즉시 끊기지 않음
- 최종적으로 `ORA-01013`
- connection 재사용 가능

따라서 이 Oracle XE에서는 "즉시 중단"보다 "ORA-01013으로 종료되고 세션이 계속 살아 있어야 한다"가 python thin과 같은 동작 기준이다.
