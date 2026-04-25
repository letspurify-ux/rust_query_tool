# Oracle Instant Client 접속 및 테스트 정리

## 설치된 클라이언트

- 다운로드 출처: Oracle Instant Client Downloads for macOS ARM64
  - https://www.oracle.com/database/technologies/instant-client/macos-arm64-downloads.html
- 설치 경로:
  - `/Users/iceblue/Downloads/instantclient_23_26`
- 설치 패키지:
  - Basic Package `23.26.1.0.0`
  - SQL*Plus Package `23.26.1.0.0`

설치 확인:

```sh
file /Users/iceblue/Downloads/instantclient_23_26/libclntsh.dylib.23.1
/Users/iceblue/Downloads/instantclient_23_26/genezi -v
/Users/iceblue/Downloads/instantclient_23_26/sqlplus -v
```

정상 결과:

- `libclntsh.dylib.23.1`이 `Mach-O 64-bit ... arm64`로 표시된다.
- `genezi -v`가 `Client Shared Library 64-bit - 23.26.1.0.0`를 표시한다.
- `sqlplus -v`가 `SQL*Plus: Release 23.26.1.0.0`를 표시한다.

## 앱 접속 설정

앱은 `oracle-rs`/ODPI-C를 통해 Oracle Client를 초기화한다. macOS에서는 `~/Downloads/instantclient_*` 경로를 자동 탐색하지만, 테스트나 터미널 실행에서는 명시적으로 지정하는 편이 안전하다.

```sh
export ORACLE_CLIENT_LIB_DIR=/Users/iceblue/Downloads/instantclient_23_26
```

현재 로컬 Docker Oracle 테스트 DB 기준 직접 접속 정보:

- Container: `oracle`
- Image: `gvenzl/oracle-free`
- Server version: `Oracle AI Database 26ai Free Release 23.26.0.0.0`
- Host: `127.0.0.1`
- Port: `1521`
- Service: `FREE`
- Username: `system`
- Password: `password`
- 앱 DB 타입: `Oracle`

TNS alias 모드에서는 `Host`와 `Port`를 비우고 `Service Name` 자리에 TNS alias를 입력한다. 이 경우 SSL/protocol은 앱의 직접 접속 옵션이 아니라 Oracle Net 설정(`tnsnames.ora`, `sqlnet.ora`)을 따른다.

SQL*Plus 접속 확인:

```sh
/Users/iceblue/Downloads/instantclient_23_26/sqlplus -L 'system/password@//127.0.0.1:1521/FREE'
```

Docker listener는 `FREE`, `freepdb1` 서비스가 `READY`로 보여야 한다.

```sh
docker exec oracle bash -lc "lsnrctl status"
```

## 선택적 Oracle Net 설정

별도 `TNS_ADMIN`을 사용할 때는 다음 `sqlnet.ora`를 둘 수 있다.

```text
NAMES.DIRECTORY_PATH = (EZCONNECT, TNSNAMES)
DISABLE_OOB=ON
BREAK_POLL_SKIP=1000
```

예시:

```sh
mkdir -p /tmp/oracle_net_admin
printf '%s\n' \
  'NAMES.DIRECTORY_PATH = (EZCONNECT, TNSNAMES)' \
  'DISABLE_OOB=ON' \
  'BREAK_POLL_SKIP=1000' \
  > /tmp/oracle_net_admin/sqlnet.ora
```

기본 접속 테스트:

```sh
env \
  TNS_ADMIN=/tmp/oracle_net_admin \
  ORACLE_CLIENT_LIB_DIR=/Users/iceblue/Downloads/instantclient_23_26 \
  ORACLE_TEST_USERNAME=system \
  ORACLE_TEST_PASSWORD=password \
  ORACLE_TEST_HOST=127.0.0.1 \
  ORACLE_TEST_PORT=1521 \
  ORACLE_TEST_SERVICE_NAME=FREE \
  cargo test oracle_test_connection_supports_direct_local_xe --lib -- --ignored --nocapture
```

## 고급 옵션 적용 테스트

Oracle ignored 통합 테스트는 로컬 Docker listener에 접근해야 하므로 Codex sandbox 밖에서 실행해야 한다.

메인 연결의 고급 옵션 적용 확인:

```sh
env \
  TNS_ADMIN=/tmp/oracle_net_admin \
  ORACLE_CLIENT_LIB_DIR=/Users/iceblue/Downloads/instantclient_23_26 \
  ORACLE_TEST_USERNAME=system \
  ORACLE_TEST_PASSWORD=password \
  ORACLE_TEST_HOST=127.0.0.1 \
  ORACLE_TEST_PORT=1521 \
  ORACLE_TEST_SERVICE_NAME=FREE \
  cargo test oracle_connect_applies_advanced_session_settings_from_local_xe --lib -- --ignored --nocapture
```

쿼리 실행에서 사용하는 풀 세션의 고급 옵션 적용 확인:

```sh
env \
  TNS_ADMIN=/tmp/oracle_net_admin \
  ORACLE_CLIENT_LIB_DIR=/Users/iceblue/Downloads/instantclient_23_26 \
  ORACLE_TEST_USERNAME=system \
  ORACLE_TEST_PASSWORD=password \
  ORACLE_TEST_HOST=127.0.0.1 \
  ORACLE_TEST_PORT=1521 \
  ORACLE_TEST_SERVICE_NAME=FREE \
  cargo test oracle_pool_session_applies_advanced_session_settings_from_local_xe --lib -- --ignored --nocapture
```

위 테스트들은 다음 Oracle 세션 설정이 메인 연결과 풀 세션 모두에 적용되는지 확인한다.

- `ALTER SESSION SET NLS_TIMESTAMP_FORMAT`
- `ALTER SESSION SET NLS_DATE_FORMAT`
- `ALTER SESSION SET ISOLATION_LEVEL`
- `ALTER SESSION SET TIME_ZONE`

## Session Time Zone 범위

Oracle 로컬 서버에서 offset 경계값을 확인했다.

- 허용: `-14:59`, `+14:59`
- 거부 대상으로 앱에서 막는 값: `+15:00` 이상

직접 확인:

```sh
docker exec oracle bash -lc "printf \"ALTER SESSION SET TIME_ZONE = '+14:59';\nSELECT SESSIONTIMEZONE FROM dual;\nEXIT\n\" | sqlplus -s system/password@localhost:1521/FREE"
```

MySQL/MariaDB와 허용 범위가 다르므로 앱 검증도 DB 타입별로 분리한다.

## 테스트 중 확인한 문제

### ARM64 설치 전 DPI-1047

증상:

```text
DPI-1047: Cannot locate a 64-bit Oracle Client library
incompatible architecture (have 'x86_64', need 'arm64')
```

원인:

- ARM64 앱/런타임이 x86_64 Oracle Instant Client를 찾고 있었다.

해결:

- macOS ARM64 Instant Client를 설치한다.
- 자동 탐색이 잘못된 클라이언트를 잡으면 `ORACLE_CLIENT_LIB_DIR`를 ARM64 클라이언트 디렉터리로 지정한다.

### Codex sandbox 내부 ORA-12560

sandbox 안에서 SQL*Plus나 Rust Oracle ignored 테스트를 실행하면 다음 오류가 발생했다.

```text
ORA-12560: Database communication protocol error
```

확인 결과:

- 같은 SQL*Plus 명령은 sandbox 밖에서 정상 접속된다.
- Docker Oracle listener와 컨테이너 내부 SQL*Plus 접속은 정상이다.
- 따라서 이 문제는 앱 코드나 Instant Client 아키텍처 문제가 아니라, sandbox 안 프로세스가 로컬 Docker listener로 정상 Oracle Net 연결을 만들지 못한 문제였다.

해결 또는 우회:

- 로컬 Oracle 통합 테스트는 sandbox 밖에서 실행한다.
- Codex에서는 로컬 Docker listener 접속이 필요한 Oracle 테스트를 escalated command로 실행한다.

### 호스트명 해석은 최종 원인이 아니었음

macOS 호스트명 `iceblueui-noteubug.local`이 `dscacheutil`에서 해석되지 않고 `/etc/hosts`에도 없어서 원인 후보로 보였다. Oracle client connect data에 로컬 호스트명이 포함되기 때문이다.

하지만 같은 SQL*Plus 명령이 sandbox 밖에서는 `/etc/hosts` 수정 없이 성공했다. 동일 문제가 sandbox 밖에서도 재현될 때만 `/etc/hosts` 수정을 검토한다.

### Session Time Zone 범위가 MySQL/MariaDB와 다름

Oracle은 `+14:59`, `-14:59`를 허용했지만 MySQL/MariaDB는 같은 값을 거부했다. 기존의 공통 형식 검증만으로는 DB별 차이를 반영할 수 없었다.

해결:

- Oracle, MySQL/MariaDB 시간대 offset 검증 범위를 분리했다.
- Oracle은 `-14:59`부터 `+14:59`까지 허용한다.
