# Oracle Thin Cancel Rules

기준 레퍼런스:

- `python-oracledb`: `/tmp/python-oracledb/src/oracledb/impl/thin/connection.pyx`
- `python-oracledb`: `/tmp/python-oracledb/src/oracledb/impl/thin/protocol.pyx`
- 현재 구현: `vendor/oracle-rs/src/connection.rs`

## 1. python-oracledb thin의 cancel 진입점

`connection.cancel()`은 thin 모드에서 바로 `protocol._break_external()`을 호출한다.

핵심 규칙:

1. `_break_in_progress`를 `True`로 세팅한다.
2. sync thin:
   - OOB 가능하면 `send_oob_break()`
   - 아니면 `INTERRUPT` marker 전송
3. async thin:
   - OOB를 쓰지 않는다.
   - marker 기반으로만 복구한다.

중요 포인트:

- python은 "cancel 요청 전송"과 "응답 복구/소비"를 분리한다.
- cancel을 보냈다고 끝이 아니라, 요청을 처리하던 스레드/루프가 이후 응답 스트림을 끝까지 정리해야 한다.

## 2. BREAK / RESET 응답 소비 규칙

`protocol._reset()` 기준:

1. `RESET` marker 전송
2. `RESET` marker가 돌아올 때까지 marker packet 반복 소비
3. 추가 marker packet들을 더 건너뜀
4. 다음 non-marker packet부터 원래 message parser가 이어서 소비

즉, cancel 이후 응답 소비 규칙은:

- marker를 보면 바로 일반 응답 파싱을 계속하면 안 된다.
- 반드시 `RESET` handshake를 완료하고,
- 남은 marker들을 비운 뒤,
- 다음 non-marker packet을 실제 error/data response로 넘겨야 한다.

## 3. sync thin의 OOB 경로 추가 규칙

sync thin의 `_process_message()`는 `_break_in_progress`가 남아 있으면:

1. OOB 지원 시 `INTERRUPT` marker를 한 번 더 전송하고
2. 다음 packet을 수신한 뒤
3. `message.process()`를 다시 태운다.

즉, python sync thin은 OOB byte 하나만 보내고 끝내지 않는다.
요청 처리 스레드가 후속 marker/response 소비까지 책임진다.

## 4. 우리 Rust 구현에서 확인된 문제

기존 `oracle-rs` 구현은 외부 `interrupt()` 호출 시:

- duplicated socket으로 OOB/marker를 보내려고 했지만,
- local Oracle XE (`localhost:1521/FREE`) 기준 `DBMS_LOCK.SLEEP(30)`가 전혀 끊기지 않았고,
- `cargo test --lib oracle_thin_interrupts_long_running_plsql -- --ignored --nocapture`가 약 `30.13s` 후 실패했다.

즉, "cancel 전송"은 있어도 "long-running call을 즉시 탈출시키는 동작"은 성립하지 않았다.

## 5. 이번 수정 방침

python 규칙과의 관계를 정리하면:

1. best-effort로 python thin과 같은 cancel 신호를 보낸다.
   - OOB 지원 시 urgent break 시도
   - 이어서 `INTERRUPT` marker 전송
2. 그 다음 transport를 `shutdown()` 해서 hard-stop fallback을 건다.
3. 연결 객체는 즉시 `closed` 상태로 마킹한다.

이 방침을 택한 이유:

- 현재 Rust 구현은 tokio 기반 비동기 transport 위에 올라가 있어,
  python sync thin처럼 "요청 처리 중인 동일 소켓 write path"와
  "후속 marker/response 복구 루프"를 그대로 재현하지 못한다.
- 반면 사용자 관점에서 cancel의 1차 요구는 "오래 걸리는 호출이 즉시 풀려야 한다"는 점이다.
- 현재 테스트도 `ORA-01013`뿐 아니라 `CONNECTION CLOSED` / `EARLY EOF`를 유효한 cancel 결과로 인정하고 있다.

## 6. 현재 최종 구현 의미

현재 oracle thin cancel의 의미는 다음과 같다.

- 우선 protocol cancel을 시도한다.
- 서버가 즉시 협조하지 않더라도 transport shutdown으로 호출을 빠르게 탈출시킨다.
- 따라서 cancel 이후 해당 connection은 재사용하지 않는다.

즉:

- "즉시 중단"은 보장한다.
- "세션 유지 후 계속 재사용"은 아직 보장하지 않는다.

## 7. 적용 파일

- `vendor/oracle-rs/src/connection.rs`

핵심 변경:

- `InterruptTransport::send_interrupt()`
  - OOB 가능 시 urgent break 시도
  - `INTERRUPT` marker 전송
  - transport `shutdown()` fallback
- `Connection::interrupt()`
  - interrupt 직후 connection을 `closed`로 마킹

## 8. 실검증

2026-04-18, 로컬 Oracle XE (`system/password@localhost:1521/FREE`) 기준:

```bash
cargo test --lib oracle_thin_interrupts_long_running_plsql -- --ignored --nocapture
```

결과:

- 수정 전: 실패, 약 `30.13s`
- 수정 후: 통과, 약 `0.91s`
