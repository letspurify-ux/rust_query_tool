# SQL Auto Formatting Depth Principles

## 1. Depth의 정의

depth = 현재 시점에 열려 있는 구문 소유자(active syntactic owners) 스택의 높이.

소유자 종류:

- 일반 괄호 표현식
- 서브쿼리 소유 괄호
- `BEGIN … END`, `CASE … END`, `IF … END IF` 블록
- `OVER (…)`, `WITHIN GROUP (…)`, `MATCH_RECOGNIZE (…)`, `PIVOT (…)` 다중행 clause owner
- `THEN`, `ELSE`, `EXCEPTION` 분기/핸들러 body opener

구현에서 구분해야 하는 5개 값:

| 이름 | 의미 |
|---|---|
| `owner depth` | owner header line 자신의 구조 depth |
| `body depth` | owner가 여는 본문 depth. 항상 `owner depth + 1` |
| `list body depth` | comma-separated sibling list depth. mixed line에서는 render depth와 분리된 별도 상태 |
| `close depth` | 닫히는 줄의 depth. 항상 pop된 owner의 `owner depth` |
| `render indent` | 최종 렌더링 공백 수. non-verbatim line에서는 항상 `structural depth * 4` |

구조 계산 = `owner/body/list body/close depth`. 렌더링 계산 = `render indent`.

현재 구현 대응:

- `parser_depth`: lexical leading close를 먼저 소비한 뒤의 기본 structural depth
- `auto_depth`: analyzer가 계산한 현재 code line의 structural depth
- `query_base_depth`: 현재 활성 query frame의 base depth
- `next_query_head_depth`: 현재 owner가 여는 다음 child query head depth
- pending split owner: split owner/header chain이 아직 완성되지 않은 structural state
- completed owner anchor: owner는 완성됐지만 첫 child body/query line이 아직 시작되지 않은 structural state

`existing_indent`는 입력 텍스트의 과거 시각 정보일 뿐이다.

- 구조 계산에서는 owner stack / query frame / condition frame / multiline owner frame / 정규화된 line continuation state 같은 의미 상태만 사용한다.
- 이전 줄의 `final_depth`, analyzer가 제공한 `parser_depth`/`auto_depth`/`query_base_depth`, pending owner frame처럼 이미 구조 규칙으로 계산된 값은 재사용 가능한 구조 상태다.
- `existing_indent`는 포맷 결과의 code-line render indent 결정에도 사용하면 안 된다.
- `existing_indent`는 raw/verbatim passthrough line의 원문 보존 여부 판단 같은 비구조 예외 처리에서만 허용된다.
- `existing_indent`는 구조 depth의 fallback, soft floor, tie-breaker, "조금만 더 들여쓰기 되어 있으면 유지" 같은 보정 규칙으로도 사용하면 안 된다.
- 구조 계산이나 code-line render 계산에서 `existing_indent`를 참조해야 한다면, 그 줄은 아직 frame/state 모델이 빠진 임시 브릿지로 간주해야 하며 제거 대상이다.

## 2. 핵심 공리

### 2.1 구조 depth는 시각 indent로부터 역산 금지

구조 depth 결정 입력:

- 어떤 owner가 열렸는가 / 닫혔는가
- split owner/header chain이 어느 owner를 계속 들고 가는가
- 다음 child query head가 어떤 owner에서 파생되었는가

기존 줄의 공백 수, 수동 정렬, hanging indent 공백 수 → 구조 판단 근거가 될 수 없다.

금지 예시:

- `parser_depth.max(existing_indent)`
- `existing_indent > parser_depth + n` 같은 임계치 기반 soft clamp
- "조금만 더 들여쓰기 되어 있으면 continuation로 간주" 같은 휴리스틱

### 2.2 모든 open event는 정확히 +1

한 번의 opener가 두 단계 이상을 만들면 안 된다. 다단계 점프처럼 보여도 실제 전이는 항상 "owner frame 하나 push"의 합성이어야 한다.

예시:

- `FROM (` 뒤의 child query head가 query base 대비 `+2`처럼 보이더라도, 실제 구조 전이는 `FROM body +1` 후 `child query head +1`의 합성이다.
- `THEN` 다음 `BEGIN`도 branch body frame `+1`과 block frame `+1`을 분리해서 해석해야 한다.

### 2.3 모든 close event는 pop된 owner의 depth로 정렬

- `)` → frame 하나 `-1`
- `END`, `END CASE`, `END IF`, `END LOOP` → 블록 frame 하나 `-1`

닫힘 depth = "이전 줄 indent − 1" 추정치가 아니라, **실제로 pop된 owner의 depth**.

- query close line은 popped query frame의 `close_align_depth` 또는 stored owner depth를 사용한다.
- general `)` close line은 popped general paren frame의 `owner depth`를 사용한다.
- multiline owner close line은 popped multiline owner frame의 `owner depth`를 사용한다.
- condition closer(`) THEN`, `) LOOP`)는 stored condition header depth를 사용한다.

### 2.4 선두 close 소비가 먼저

줄이 `)` 로 시작하면, 그 close event를 먼저 소비한 뒤 나머지 토큰을 해석한다.

### 2.5 주석 / 문자열 / quoted literal 내부는 depth event가 아니다

문자열·주석·q-quote·quoted identifier 내부의 괄호는 depth를 바꾸면 안 된다.

## 3. 모든 괄호/블록은 같은 push/pop 모델

| 구분 | open | close | close line 정렬 |
|---|---|---|---|
| 일반 괄호 | frame push | frame pop | pop된 owner depth |
| 서브쿼리 괄호 | frame push | frame pop | pop된 owner depth |
| multiline clause 괄호 | frame push | frame pop | pop된 owner depth |
| `BEGIN`/`END` 블록 | frame push | frame pop | pop된 owner depth |
| `CASE`/`END` | frame push | frame pop | pop된 owner depth |
| `THEN`/`ELSE`/`EXCEPTION` | body frame push | 분기 종료 시 pop | — |

종류만 다르고 전이 규칙(push +1, pop −1, close = owner depth)은 동일하다.

추가 원칙:

- `),` 뒤의 다음 sibling도 새 owner가 아니라 기존 list body 위에서 해석한다.
- comment-only / comma-only line은 frame을 열거나 닫지 않으며, 인접 code line의 구조 depth를 빌려 렌더링만 한다.
- non-verbatim code line의 render indent는 항상 `final structural depth * 4`다.

## 4. continuation line

owner를 열지도 닫지도 않으면, 현재 활성 stack의 depth를 그대로 사용한다.

- 시각 정렬을 위해 기존 공백을 보존하지 않는다. continuation line도 최종 출력은 구조 depth만으로 canonicalize한다.
- split owner/header chain (`WITHIN → GROUP`, `LEFT OUTER → JOIN`, `OPEN → FOR`, `CURSOR → IS`)은 최초 owner line의 구조 depth를 보존한다.
- comma는 push/pop event가 아니다. sibling 판단 기준은 활성 owner stack과 list 위치뿐이다.

### 4.1 completed owner anchor

split owner/header chain이 현재 줄에서 완성되었는데 child body/query/open wrapper가 다음 code line에서 시작하면,
formatter는 그 owner를 즉시 잊으면 안 된다.

- completed owner anchor는 "완성된 owner line의 owner depth"를 그대로 보존하는 pending structural state다.
- comment-only / blank / comma-only line은 anchor를 소비하지 않는다.
- standalone `(`, leading `)` wrapper line도 anchor를 먼저 소비하지 않고 owner depth 정렬만 수행한다.
- 첫 real child body/query line이 나타날 때만 anchor가 body/query depth로 전이된다.
- 이 anchor를 현재 줄의 `existing_indent`나 다음 줄 공백으로 재구성하면 안 된다.

## 5. 구현 순서

1. 의미 있는 open / close event를 lexical하게 식별
2. 선두 close event를 먼저 소비
3. 남은 토큰으로 현재 줄의 owner/body/header 분류
4. 분류 결과를 활성 owner stack 위에 투영
5. 줄 끝에서 새 open event를 stack에 반영

split owner/header가 다음 줄까지 이어지는 경우:

- pending owner는 원래 owner depth를 그대로 들고 간다
- owner header가 현재 줄에서 완성되어도 child body/query/open wrapper가 다음 줄에 시작하면, 그 첫 child line이 확정될 때까지 completed owner anchor를 pending frame으로 유지한다
- 현재 줄의 기존 indent로 pending depth를 재보정하면 안 된다

query/body/continuation depth는 이미 정규화된 구조 컨텍스트(`parser_depth`, `auto_depth`, `query_base_depth`, `next_query_head_depth`, pending/completed owner state, explicit continuation state)로만 전달해야 한다.

렌더링 단계는 구조 depth를 그대로 공백으로 직렬화하는 단계다.
- non-verbatim code line: `render indent = final_depth * 4`
- comment-only / comma-only line: 빌린 structural depth를 동일하게 직렬화
- raw/verbatim line만 원문 공백을 유지할 수 있다
