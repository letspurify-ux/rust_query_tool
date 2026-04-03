# 포맷팅 수정 요약

## 2026-04-03 자동 포맷팅 continuation/depth 공용화 보정

- 원칙 검토
  - `formatting.md`의 큰 방향은 맞다. depth는 공백 모양이 아니라 semantic family, shared classifier, typed anchor로 계산돼야 한다.
  - 다만 "comment는 구조 이벤트가 아니다"를 continuation classifier에도 더 직접적으로 적을 필요가 있어, comment 유무만 다른 line이 같은 classifier를 써야 한다는 원칙을 보강했다.

- 구현 보정
  - `src/db/query/script.rs`에서 analyzer가 line continuation / inline-comment continuation을 각각 자체 조합하던 경로를 제거하고 `sql_text`의 shared continuation classifier로 통합했다.
  - analyzer의 numeric depth 해석도 raw `query_base_depth`만 쓰지 않고 synthetic query-base anchor를 거치도록 바꿨다. 이로써 `WHERE col =`, `WHERE col IS`, `AND col <=`, `:=` 같은 header+operator / pure operator line이 formatter와 같은 structural ladder를 따른다.
  - 결과적으로 inline comment 유무에 따라 continuation kind나 depth가 달라지던 drift를 없앴다.

- 검증
  - 실행 명령: `cargo test --quiet`
  - 결과: 전체 통과 (`3474 passed`, `46 ignored` + 추가 test target 통과)

## 2026-04-03 자동 포맷팅 depth 원칙 검토 및 구현 보정

- 원칙 검토 결론
  - 방향은 맞다. depth는 공백 모양이나 토큰별 예외가 아니라 semantic family, typed pending state, shared continuation resolver로 결정되어야 한다.
  - 특히 "어떤 family인가"와 "어느 anchor를 기준으로 숫자 depth를 계산하는가"를 분리한 현재 원칙이 핵심이다. 이 둘을 한 단계에서 섞으면 `JOIN`, owner-relative header, completed owner anchor에서 phase drift가 다시 생긴다.

- 문서 보강
  - `formatting.md`에 semantic family 중심 원칙을 보강했다.
  - `SameDepth` / `OneDeeperThanQueryBase` / `OneDeeperThanCurrentLine`는 analyzer/formatter가 각각 해석하지 않고 공용 resolver로 숫자 depth를 계산해야 한다는 원칙을 명시했다.
  - completed owner anchor는 단일 숫자가 아니라 `Exact(owner depth)`와 `Floor(owner depth + 1)`처럼 소비 방식까지 포함한 typed state로 유지해야 한다는 점을 명시했다.
  - 자동 포맷팅 결과는 canonical form으로 수렴하고, 같은 SQL에 formatter를 두 번 적용해도 결과가 바뀌지 않아야 한다는 idempotent 원칙을 추가했다.

- 구현 보정
  - plain `JOIN`과 modifier-completed `INNER/NATURAL/... JOIN`을 같은 literal `JOIN` 규칙으로 처리하지 않고 semantic family로 분리했다.
  - `KEEP` 계열 split body header(`DENSE_RANK`, `DENSE_RANK LAST` 등)는 raw literal 예외가 아니라 shared owner-relative sequence matcher를 통해 continuation kind를 계산하도록 정리했다.
  - formatter 내부의 header/comment continuation depth 계산을 shared resolver 기반으로 통합했다.
  - split/completed `OPEN ... FOR`, `CURSOR ... IS`는 standalone `(` / leading `)` / close line에서는 owner depth에 정확히 정렬되고, 일반 body line은 owner-relative floor를 따르도록 typed alignment로 분리했다.
  - exact bare structural family가 이미 판정된 line에서는 generic token-pair fallback이 depth를 덮어쓰지 않도록 원칙과 회귀 테스트를 추가했다.

- 검증
  - 실행 명령: `cargo test`
  - 결과: unit test `3472 passed`, `46 ignored`; guard/integration test `5 passed`; doc test `0 passed`
