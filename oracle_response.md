# Oracle Thin Response Rules

기준 소스:

- `python-oracledb`: `/tmp/python-oracledb/src/oracledb/impl/thin/protocol.pyx`
- `python-oracledb`: `/tmp/python-oracledb/src/oracledb/impl/thin/messages/base.pyx`
- 현재 구현: `vendor/oracle-rs/src/connection.rs`

## 1. python-oracledb thin의 실제 응답 소비 순서

sync thin 기준 핵심 흐름은 `BaseProtocol._process_message()` 하나로 묶여 있다.

1. `reset_packets()` (write/read buffer 초기화)
2. `message.send(write_buf)`
3. `_receive_packet(message, check_request_boundary=True)`
4. `message.process(read_buf)`
5. `flush_out_binds`가 있으면 추가 요청/응답 1회
6. `_break_in_progress`가 살아 있으면:
   - `supports_oob`일 때만 `INTERRUPT` marker 추가 전송
   - 응답을 한 번 더 받음 (`_receive_packet`)
   - `message.process(read_buf)`를 다시 실행
   - `_break_in_progress = False`
7. `_process_call_status(conn_impl, message.call_status)`
8. `message.error_occurred`일 때:
   - `message.retry`면 `error_occurred = False`로 리셋 후 `_process_message`를 **재귀 호출** (1번부터 다시)
   - 아니면 `message._check_and_raise_exception()`

즉, python thin은 "첫 응답 1개 읽고 끝"이 아니다. marker/reset과 external cancel 후속 응답까지 같은 호출 안에서 마저 소비하고, retry 시에도 동일 루틴으로 재진입한다.

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
2. `RESET` marker가 돌아올 때까지 모든 packet 반복 소비 (marker가 아닌 패킷도 건너뜀)
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

**idempotent 보장**: `_break_external()`은 `if not self._break_in_progress:` 가드로 시작한다. 이미 break가 진행 중이면 no-op. cancel 경로가 레이스로 두 번 진입해도 OOB/`INTERRUPT` 전송은 한 번만 일어난다.

## 5. 공통 message type 소비 규칙

`message.process()` 안에서는 message type 경계를 정확히 맞춰야 한다.

### 5.1 `Message` 기본 dispatch

| Message Type | 의미 | 소비 규칙 |
| --- | --- | --- |
| `TNS_MSG_TYPE_ERROR` | Error | `_process_error_info()` |
| `TNS_MSG_TYPE_WARNING` | Warning | `_process_warning_info()` |
| `TNS_MSG_TYPE_TOKEN` | Token | `ub8` 1개 소비 (토큰 일치 검증) |
| `TNS_MSG_TYPE_STATUS` | Status | `ub4 call_status` + `ub2 end_to_end_seq_num`. `supports_end_of_response=False`면 `end_of_response=True` 전환 |
| `TNS_MSG_TYPE_PARAMETER` | Parameter | `_process_return_parameters()` |
| `TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK` | ServerSidePiggyback | `_process_server_side_piggyback()` (opcode 분기) |
| `TNS_MSG_TYPE_END_OF_RESPONSE` | EndOfResponse | `end_of_response = True` |
| 기타 | — | `ERR_MESSAGE_TYPE_UNKNOWN` raise |

### 5.2 `MessageWithData` 추가 dispatch (쿼리/DML/PLSQL 응답 전용)

`MessageWithData._process_message()`는 base dispatch를 오버라이드해서 fetch/bind 관련 type을 먼저 처리하고, 매치 안 되면 `Message._process_message()`로 위임한다.

| Message Type | 의미 | 소비 규칙 |
| --- | --- | --- |
| `TNS_MSG_TYPE_DESCRIBE_INFO` | 컬럼 메타 | `ub1 (skip)` 후 `_process_describe_info()` |
| `TNS_MSG_TYPE_ROW_HEADER` | row 헤더 | `_process_row_header()` |
| `TNS_MSG_TYPE_ROW_DATA` | row 바디 | `_process_row_data()` |
| `TNS_MSG_TYPE_BIT_VECTOR` | 중복 컬럼 비트맵 | `_process_bit_vector()` |
| `TNS_MSG_TYPE_IO_VECTOR` | bind in/out 구분 | `_process_io_vector()` |
| `TNS_MSG_TYPE_FLUSH_OUT_BINDS` | PL/SQL out flush | flush flag 세팅 후 `end_of_response=True` |
| `TNS_MSG_TYPE_IMPLICIT_RESULTSET` | DBMS_SQL.RETURN_RESULT | `_process_implicit_result()` |

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

## 11. `_process_message()` 예외 처리 규칙

python thin `_process_message()`는 예외 종류별로 다르게 반응한다.

```
send() + _receive_packet() + message.process() 실행 중:

socket.timeout 발생:
  → _break_external()
  → _receive_packet()  (두 번째 timeout이면 _disconnect() 후 CONNECTION_CLOSED)
  → _break_in_progress = False
  → ERR_CALL_TIMEOUT_EXCEEDED raise

MarkerDetected 발생:
  → _reset()
  → message.process() 다시 실행
  (이 경우 flush/break 후처리는 동일하게 진행됨)

BaseException 발생 (in_connect=False AND packet_sent=True AND transport 살아있음):
  → BREAK marker 전송
  → _reset()
  → 원래 예외 re-raise
```

중요: `BaseException` 경로에서 `_reset()`은 에러를 삼키는 게 아니라 서버 상태를 정리하는 것이다. 이후 연결은 계속 살아 있다.

## 12. `process()` 루프 세부 규칙

`Message.process()`는 `MessageWithData`도 오버라이드하지 않는다. 루프 구조는 하나다.

```cython
while not self.end_of_response:
    buf.save_point()           # <-- 매 message_type 읽기 전에 save point 저장
    buf.read_ub1(&message_type)
    self._process_message(buf, message_type)
```

`save_point()` / `restore_point()`는 async 경로에서 `OutOfPackets` 예외 발생 시 현재 message type부터 재처리하기 위해 쓴다.

`end_of_response = True`가 되는 경우는 두 가지뿐이다:
- `TNS_MSG_TYPE_END_OF_RESPONSE` 수신
- `supports_end_of_response = False`일 때 `TNS_MSG_TYPE_STATUS` 또는 `TNS_MSG_TYPE_ERROR` 수신

즉 `supports_end_of_response = True` 환경에서는 STATUS를 받아도 루프가 끝나지 않고 `END_OF_RESPONSE`까지 계속 읽는다.

## 13. async `OutOfPackets` 처리

`BaseAsyncProtocol._process_message_helper()`는 sync와 다른 루프를 가진다.

```cython
while True:
    try:
        message.process(self._read_buf)
        break
    except OutOfPackets:
        await self._receive_packet(message)  # 추가 패킷 수신
        message.on_out_of_packets()
        self._read_buf.restore_point()       # save_point로 되감기 후 재처리
```

sync 경로는 단일 `message.process()` 호출 후 끝이지만, async는 패킷이 부족하면 더 받아서 이어 처리한다.

## 14. `_send_marker()` 내부 구조

```cython
buf.start_request(TNS_PACKET_TYPE_MARKER)
buf.write_uint8(1)           # 고정값
buf.write_uint8(0)           # 고정값
buf.write_uint8(marker_type) # BREAK=1, RESET=2, INTERRUPT=3
buf.end_request()
```

`_reset()` 내에서 MARKER 패킷 수신 시 읽는 순서도 동일하다:
```
skip_raw_bytes(2)   # 위의 1, 0 두 바이트 건너뜀
read_ub1(marker_type)
```

## 15. `_process_error_info()` 전체 필드 맵

```
ub4   call_status          (self.call_status 저장)
ub2   (skip) end_to_end_seq
ub4   (skip) current_row_num
ub2   (skip) error_num_1
ub2   (skip) array_elem_error_1
ub2   (skip) array_elem_error_2
ub2   cursor_id
sb2   error_pos
ub1   (skip) sql_type        (19c 이하)
ub1   (skip) fatal
ub1   (skip) flags_1
ub1   (skip) user_cursor_options
ub1   (skip) upi_parameter
ub1   flags                 (0x20 = compilation warning)
rowid (read_rowid)
ub4   (skip) os_error
ub1   (skip) statement_num
ub1   (skip) call_num
ub2   (skip) padding
ub4   (skip) success_iters
bytes_with_length (skip) oerrdd

ub2   num_errors            (batch error codes)
if num_errors > 0:
  ub1  first_byte
  loop num_errors:
    if first_byte == LONG_LENGTH_INDICATOR: ub4 (skip chunk len)
    ub2  error_code
  if first_byte == LONG_LENGTH_INDICATOR: raw(1) (skip end marker)

ub4   num_offsets           (batch error row offsets)
if num_offsets > 0:
  ub1  first_byte
  loop num_offsets:
    if first_byte == LONG_LENGTH_INDICATOR: ub4 (skip chunk len)
    ub4  offset
  if first_byte == LONG_LENGTH_INDICATOR: raw(1) (skip end marker)

ub2   temp16                (batch error messages)
if temp16 > 0:
  raw(1) (skip packed size)
  loop temp16:
    ub2   (skip chunk len)
    str   message            (read_str, rstrip)
    raw(2) (skip end marker)

ub4   error_num             (extended, info.num)
ub8   rowcount              (extended, info.rowcount)

if ttc_field_version >= TNS_CCAP_FIELD_VERSION_20_1:
  ub4   (skip) sql_type
  ub4   (skip) server_checksum

if info.num != 0:
  error_occurred = True
  if error_pos > 0:
    info.pos = error_pos
  str   error_message       (read_str, rstrip → info.message)

if !supports_end_of_response:
  end_of_response = True
```

## 16. `_process_warning_info()` 필드 맵

```
ub2  error_num
ub2  num_bytes              (메시지 길이)
ub2  (skip) flags
if error_num != 0 and num_bytes > 0:
  str  message              (read_str, rstrip)
```

## 17. `_process_return_parameters()` 필드 맵

```
ub2  num_params             (al8o4l, 무시)
loop num_params: ub4 (skip)

ub2  num_bytes              (al8txl)
if num_bytes > 0: raw(num_bytes) (skip)

ub2  num_pairs
→ _process_keyword_value_pairs(buf, num_pairs)

ub2  num_bytes              (registration info)
if num_bytes > 0:
  raw(num_bytes)            (마지막 8바이트에서 query_id 추출)
  query_id_msb = decode_uint32be(ptr[num_bytes-4])
  query_id_lsb = decode_uint32be(ptr[num_bytes-8])
  cursor_impl._query_id = (msb << 32) | lsb

if arraydmlrowcounts:
  ub4  num_rows
  loop num_rows: ub8 rowcount
```

`_process_keyword_value_pairs()`:
```
loop num_pairs:
  ub2  num_bytes; if > 0: read_bytes() → text_value
  ub2  num_bytes; if > 0: read_bytes() → binary_value
  ub2  keyword_num
  → TNS_KEYWORD_NUM_CURRENT_SCHEMA / EDITION / TRANSACTION_ID 처리
```

## 18. `_process_server_side_piggyback()` opcode별 필드 맵

opcode 분기 전체를 테이블로 소비해야 한다. 매치 안 되면 즉시 `ERR_UNKNOWN_SERVER_PIGGYBACK`이 발생하므로, opcode별 payload 크기를 정확히 맞추는 것이 경계 유지의 전제 조건이다.

```
ub1  opcode

QUERY_CACHE_INVALIDATION (1), TRACE_EVENT (3):
  (pass, 아무것도 읽지 않음)

OS_PID_MTS (2):
  ub2  temp16
  skip_bytes()

SESS_RET (4):
  ub2  (skip)
  ub1  (skip)
  ub2  num_elements
  if num_elements > 0:
    ub1 (skip)
    loop num_elements:
      ub2 len; if > 0: skip_bytes()   (key)
      ub2 len; if > 0: skip_bytes()   (value)
      ub2 (skip) flags
  ub4  session_flags
    if (session_flags & TNS_SESSGET_SESSION_CHANGED) AND drcp_establish_session:
      statement_cache.clear_open_cursors()
    drcp_establish_session = False
  ub4  session_id
  ub2  serial_num

SYNC (5):
  ub2  (skip) num_dtys
  ub1  (skip) len_dtys
  ub2  num_elements
  ub1  (skip) len
  _process_keyword_value_pairs(buf, num_elements)
  ub4  (skip) overall_flags

LTXID (7):
  bytes_with_length → conn_impl._ltxid

AC_REPLAY_CONTEXT (8):
  ub2  (skip) num_dtys
  ub1  (skip) len_dtys
  ub4  (skip) flags
  ub4  (skip) error_code
  ub1  (skip) queue
  bytes_with_length (skip) replay_context

EXT_SYNC (9):
  ub2  (skip) num_dtys
  ub1  (skip) len_dtys

SESS_SIGNATURE (10):
  ub2  (skip) num_dtys
  ub1  (skip) len_dty
  ub8  (skip) signature_flags
  ub8  (skip) client_signature
  ub8  (skip) server_signature
```

**주의**: opcode가 위 목록에 없으면 `ERR_UNKNOWN_SERVER_PIGGYBACK` 에러. 즉 piggyback 전체를 소비하지 않으면 이후 메시지 경계가 어긋난다.

## 19. `_process_describe_info()` / `_process_metadata()` 필드 맵

`_process_describe_info()` (컬럼 메타데이터 블록):
```
ub4  (skip) max_row_size
ub4  num_columns
if num_columns > 0:
  ub1 (skip)
loop num_columns:
  → _process_metadata(buf) 호출
bytes_with_length (skip) current_date
ub4  (skip) dcbflag
ub4  (skip) dcbmdbz
ub4  (skip) dcbmnpr
ub4  (skip) dcbmxpr
bytes_with_length (skip) dcbqcky
```

`_process_metadata()` (컬럼 하나):
```
ub1  ora_type_num
ub1  (skip) flags
sb1  precision
sb1  scale
ub4  buffer_size
ub4  (skip) max_array_elements
ub8  (skip) cont_flags
bytes_with_length oid
ub2  (skip) version
ub2  (skip) charset_id
ub1  csfrm               → DbType 결정
ub4  max_size
if ora_type_num == ORA_TYPE_NUM_RAW:
  max_size = buffer_size       (read 아님, override)
if ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_2:
  ub4  (skip) oaccolid
ub1  nulls_allowed
ub1  (skip) v7_len_of_name
str_with_length  name
str_with_length  schema
str_with_length  object_name
ub2  (skip) column_position
ub4  uds_flags           (IS_JSON 0x1, IS_OSON 0x2)

if ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1:
  str_with_length  domain_schema
  str_with_length  domain_name

if ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1_EXT_3:
  ub4  num_annotations
  if num_annotations > 0:
    ub1  (skip)
    ub4  num_annotations         (chunk length)
    ub1  (skip)
    loop num_annotations:
      str_with_length  key
      str_with_length  value     (NULL이면 "")
      ub4  (skip) flags
    ub4  (skip) outer_flags

if ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_4:
  ub4  vector_dimensions
  ub1  vector_format
  ub1  vector_flags

if ora_type_num == ORA_TYPE_NUM_OBJECT:
  type_cache.get_type_for_info(oid, schema, None, type_name)
    → is_xml_type이면 DB_TYPE_XMLTYPE, 아니면 objtype 저장
```

**주의**: 23.x 서버에서는 `domain_schema/domain_name`, `annotations`, `vector_*` 필드가 반드시 소비돼야 한다. 이 블록을 빠뜨리면 metadata 경계가 밀려 이후 모든 packet이 깨진다.

## 20. `_process_row_header()` 필드 맵

```
ub1  (skip) flags
ub2  (skip) num_requests
ub4  (skip) iteration_number
ub4  (skip) num_iters
ub2  (skip) buffer_length
ub4  num_bytes
if num_bytes > 0:
  ub1 (skip) repeated_length
  bit_vector(num_bytes)
bytes_with_length (skip) rxhrid
```

## 21. `_process_bit_vector()` 필드

```
ub2  num_columns_sent
num_bytes = num_columns // 8 + (1 if num_columns % 8 > 0 else 0)
raw(num_bytes)             (bit_vector, 각 비트 = 해당 컬럼 중복 여부)
```

## 22. `_process_call_status()` 플래그

```
TNS_EOCS_FLAGS_TXN_IN_PROGRESS  → _txn_in_progress = True
TNS_EOCS_FLAGS_SESS_RELEASE     → statement_cache.clear_open_cursors()
```

STATUS 메시지(`TNS_MSG_TYPE_STATUS`)에서 읽은 `call_status` 값이 이 함수로 넘어간다. `_process_error_info()`도 첫 필드로 `call_status`를 읽어 `self.call_status`에 저장하므로 동일하게 처리된다.

## 23. OOB check handshake 정확한 순서

`_connect_phase_two()`:
```
if supports_oob AND supports_oob_check:
    transport.send_oob_break()              # urgent OOB byte 전송
    _send_marker(write_buf, MARKER_RESET)   # RESET marker 전송
    # 응답 읽기 없음 ← 중요
```

이후 일반 `_process_message(protocol_message)` 호출로 흘러간다. OOB check에서 별도 응답을 읽지 않는 것이 핵심이다.

## 24. `_process_row_data()` 규칙

쿼리/PLSQL 응답에서 실제 row bytes를 먹는 함수다. `out_var_impls`(describe 단계에서 세팅)를 인덱스 순으로 순회한다.

각 `var_impl`에 대해:

```
if var_impl.is_array:
    ub4  num_elements_in_array
    loop num_elements_in_array:
        _process_column_data(buf, var_impl, elem_index)
elif var_impl._is_returning:
    ub4  num_rows
    loop num_rows:
        _process_column_data(buf, var_impl, row)
elif var_impl.fetching_arrow:
    if _is_duplicate_data(i): append duplicate
    else: _process_column_data(buf, var_impl, row_index)
elif _is_duplicate_data(i):
    # 직전 row의 값을 그대로 복제
    (읽지 않음)
else:
    _process_column_data(buf, var_impl, row_index)
```

루프가 끝나면:
1. `row_index += 1`
2. cursor 상태 갱신
3. `on_row_completed()` 호출

**핵심**: `_is_duplicate_data(i)`는 직전 `_process_bit_vector` 결과를 참조한다. bit_vector가 컬럼을 "중복"으로 표시하면 그 컬럼은 payload가 생략되어 있다. 이 경로에서 `read_*`를 호출하면 안 된다.

## 25. `_process_column_data()` 규칙

`ora_type_num`별 분기로 컬럼 하나를 소비한다.

| ora_type_num | 소비 필드 |
| --- | --- |
| `ROWID` | `ub1 num_bytes`; >0이면 `read_rowid` |
| `UROWID` | `read_urowid` |
| `CURSOR` | `ub1 (skip)` → `_create_cursor_from_describe(buf, column_value)` → `ub2 cursor_id` |
| `CLOB` / `BLOB` / `BFILE` | `read_lob_with_length` |
| `JSON` | `read_oson` |
| `VECTOR` | `read_vector` (fetching_arrow면 추가 변환) |
| `OBJECT` | `read_dbobject(typ_impl)` 또는 (xml_type면) `read_xmltype` |
| 그 외 | `read_oracle_data` |

fetch가 아닌 경로(out bind)에서는 끝에 다음이 붙는다:
```
sb4  actual_num_bytes     (값이 음수이거나 max_size 초과면 truncated 에러)
```

LONG/LONG RAW 타입은 본 값 뒤에 고정 skip 2개:
```
sb4  (skip) chunk_size
ub4  (skip) chunk_count
```

**핵심**: `_process_column_data`는 describe 정보(`metadata.dbtype._ora_type_num`)로 분기한다. 즉 §19 describe가 정확해야 §25가 맞는 경로를 탄다. 하나가 어긋나면 row 바디 전체가 틀어진다.

## 26. `_process_io_vector()` 필드 맵

DML/PLSQL bind 응답에서 in/out 구분을 서버가 알려주는 블록.

```
ub1  (skip) flag
ub2  temp16              (num_requests)
ub4  temp32              (num_iters)
num_binds = temp32 * 256 + temp16
ub4  (skip) num_iters_this_time
ub2  (skip) uac_buffer_length
ub2  num_bytes           (bit vector for fast fetch)
if num_bytes > 0: raw(num_bytes)
ub2  num_bytes           (rowid)
if num_bytes > 0: raw(num_bytes)

out_var_impls = []
loop num_binds:
  ub1  bind_dir
  if bind_dir != TNS_BIND_DIR_INPUT:
    out_var_impls.append(bind_info._bind_var_impl)
```

`_process_row_data`가 쓰는 `out_var_impls`가 여기서 만들어진다. 즉 io_vector를 빠뜨리면 이후 `ROW_DATA`에서 어떤 var를 읽어야 할지 결정할 수 없다.

## 27. `_process_implicit_result()` 개요

`DBMS_SQL.RETURN_RESULT`로 반환되는 암시적 result set(즉, bind되지 않은 cursor)을 받는 경로. 서버가 `TNS_MSG_TYPE_IMPLICIT_RESULTSET`을 보내면 내부 cursor list에 새 cursor를 append하고 describe 정보를 받아들인다. bind 경로와 달리 `out_var_impls`를 쓰지 않고, 새 cursor가 자체 fetch 상태를 가진다.

**핵심**: PL/SQL 블록 응답에서 이 타입이 등장할 수 있으므로, 구현이 `MessageWithData` dispatch에 이 분기를 두지 않으면 해당 PL/SQL 결과가 누락되거나 이후 message 경계가 깨진다.
