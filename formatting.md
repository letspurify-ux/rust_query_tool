# SQL Auto Formatting Depth Principles

## 1. Depth의 정의

depth = 현재 시점에 열려 있는 구문 소유자(active syntactic owners) 스택의 높이.

소유자 종류:

- 일반 괄호 표현식
- 서브쿼리 소유 괄호
- `BEGIN … END`, `CASE … END`, `IF … END IF` 블록
- `OVER (…)`, `WITHIN GROUP (…)`, `MATCH_RECOGNIZE (…)`, `PIVOT (…)` 다중행 clause owner
- `THEN`, `ELSE`, `EXCEPTION` 분기/핸들러 body opener
- `MERGE WHEN ... THEN`, `INSERT ALL/FIRST` branch, `FORALL` 같은 DML/PLSQL body owner
- `CREATE TRIGGER` header (`BEFORE`, `REFERENCING`, `FOR EACH ROW`, `WHEN`)처럼 `BEGIN` 전까지 유지되는 header body owner

주의:

- depth 자체는 "active owner stack height"이지만, 모든 줄 정렬 anchor가 owner push/pop을 의미하는 것은 아니다.
- `SELECT`, `FROM`, `SEARCH`, `CYCLE`, `FOR UPDATE`, `RETURNING`, `WINDOW`, `QUALIFY`, `OFFSET/FETCH/LIMIT` 같은 stable layout anchor는 기존 query base 또는 owner-relative body depth를 재사용할 수 있다.
- 즉 "anchor line"과 "owner line"은 구분해야 한다. owner가 아닌 anchor는 stack을 늘리지 않고도 depth를 다시 고정할 수 있다.
- 다만 `FOR UPDATE`처럼 일반 `FOR`와 충돌 가능한 구문은 공통 keyword helper 하나로 억지 통합하지 말고, analyzer/formatter 양 phase에서 같은 push/pop 의미를 유지하는 전용 판별식으로 다뤄야 한다.

구현에서 구분해야 하는 6개 값:

| 이름 | 의미 |
|---|---|
| `owner depth` | owner header line 자신의 구조 depth |
| `body depth` | owner가 여는 본문 depth. 항상 `owner depth + 1` |
| `list body depth` | comma-separated sibling list depth. mixed line에서는 render depth와 분리된 별도 상태 |
| `close align depth` | leading close를 먼저 소비했을 때 구조적으로 참조하는 정렬 기준 depth. pure close line에서는 pop된 owner의 `owner depth` |
| `final depth` | leading close를 먼저 소비한 뒤, 남은 토큰까지 해석해서 얻는 해당 줄의 최종 structural depth |
| `render indent` | 최종 렌더링 공백 수. non-verbatim line에서는 항상 `structural depth * 4` |

구조 계산 = `owner/body/list body/close align/final depth`. 렌더링 계산 = `render indent`.

현재 구현 대응:

- `parser_depth`: lexical leading close를 먼저 소비한 뒤의 기본 structural depth
- `auto_depth`: analyzer가 계산한 현재 code line의 structural depth
- `query_base_depth`: 현재 활성 query frame의 base depth
- `next_query_head_depth`: 현재 owner가 여는 다음 child query head depth
- `final_depth`: formatter phase 2가 leading close / owner-relative / continuation 정규화를 모두 반영한 최종 structural depth
- pending split owner: split owner/header chain이 아직 완성되지 않은 structural state
  이 state는 최소한 `owner align depth`와 `next_query_head_depth`를 함께 보존해야 한다.
- completed owner anchor: owner는 완성됐지만 첫 child body/query line이 아직 시작되지 않은 structural state
  이 state는 `owner depth`와 child query를 위한 `owner base depth`를 혼합하지 않고 별도로 들고 있어야 한다.
- explicit continuation state: clause/list/body가 다음 code line까지 구조 depth를 이어갈 때 쓰는 pending/active state
  예: branch body depth, select/list item depth, bare header continuation depth, operator/comment continuation depth

정리:

- `owner align depth`와 `owner base depth`는 같은 값으로 시작할 수 있어도 의미가 다르다.
  wrapper / split-header / close alignment는 `owner align depth`, child query head 계산은 `owner base depth`를 써야 한다.
- 일반 표현식 괄호 안에서 시작한 multiline owner (`OVER`, `WITHIN GROUP`, `KEEP`, `COLUMNS` 등)는
  활성 general-paren continuation depth를 그대로 상속해야 한다.

`existing_indent`는 입력 텍스트의 과거 시각 정보일 뿐이다.

- 구조 계산에서는 owner stack / query frame / condition frame / multiline owner frame / 정규화된 line continuation state 같은 의미 상태만 사용한다.
- 이전 줄의 `final_depth`, analyzer가 제공한 `parser_depth`/`auto_depth`/`query_base_depth`, pending owner frame처럼 이미 구조 규칙으로 계산된 값은 재사용 가능한 구조 상태다.
- 단, 이전 줄 정보를 쓸 때는 반드시 이름 붙은 pending/active structural state로 승격해서 써야 한다.
  `previous_line_is_*`, `직전 줄이 콤마였다`, `직전 줄이 THEN이었다` 같은 anonymous shape heuristic로 직접 depth를 복원하면 제거 대상이다.
  다만 이미 유지 중인 pending/active frame을 "지금 소비할 차례인가" 확인하는 lexical adjacency 판별 자체는 허용된다.
- `existing_indent`는 포맷 결과의 code-line render indent 결정에도 사용하면 안 된다.
- `existing_indent`는 raw/verbatim passthrough line의 원문 보존 여부 판단 같은 비구조 예외 처리에서만 허용된다.
- `existing_indent`는 구조 depth의 fallback, soft floor, tie-breaker, "조금만 더 들여쓰기 되어 있으면 유지" 같은 보정 규칙으로도 사용하면 안 된다.
- split owner/header가 이미 `pending` frame으로 살아 있다면, 즉시 이전 code line depth를 다시 읽어 owner depth를 복원하면 안 된다. 그 경우의 진짜 구조 상태는 `pending` frame이다.
- bare clause/header line (`SELECT`, `FROM`, `WHERE`, `JOIN`, `ON`, `USING`, `GROUP BY`, `ORDER BY`, `SEARCH`, `CYCLE`, `FOR UPDATE` 등)도 예외가 아니다.
  다음 code line이 body/item/operand라면 line shape가 아니라 explicit continuation state로 이어져야 한다.
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
- `LATERAL (`, `TABLE (` 같은 direct from-item owner는 clause owner와 다르다. 이 경우 child query head는 owner line 대비 `+1`로 보일 수 있으며, 이는 "from-item owner + child query head" 전이만 있는 것이다.
- `THEN` 다음 `BEGIN`도 branch body frame `+1`과 block frame `+1`을 분리해서 해석해야 한다.

중요:

- 인접 두 줄의 `final depth` 차이가 2 이상으로 보이는 것은 위 원칙과 모순이 아니다.
- 한 줄이 이미 여러 active owner/body frame 아래에서 시작하거나, leading close를 소비한 뒤 다른 continuation/body/header depth로 착지하면 줄 단위 결과는 여러 `+1/-1` 이벤트의 합성값으로 보일 수 있다.
- 금지되는 것은 "한 개의 이벤트를 근거 없이 +2/+3으로 모델링"하는 것이지, 여러 이벤트가 한 줄에서 함께 반영되는 것 자체가 아니다.

### 2.3 모든 close event는 pop된 owner의 depth로 정렬

- `)` → frame 하나 `-1`
- `END`, `END CASE`, `END IF`, `END LOOP` → 블록 frame 하나 `-1`

닫힘 구조 전이 = "이전 줄 indent − 1" 추정치가 아니라, **실제로 pop된 owner의 depth**.

- query close line은 popped query frame의 `close_align_depth` 또는 stored owner depth를 사용한다.
- general `)` close line은 popped general paren frame의 `owner depth`를 사용한다.
- multiline owner close line은 popped multiline owner frame의 `owner depth`를 사용한다.
- condition closer(`) THEN`, `) LOOP`)는 stored condition header depth를 사용한다.

단, mixed leading-close line은 별도로 본다.

- `) AND ...`, `) OR ...`, `) IS ...`, `), value`, `) ORDER BY ...` 같은 줄은 먼저 close event를 소비한다.
- 이때 구조적으로는 pop된 owner depth를 먼저 소비한다.
- 하지만 formatter는 mixed leading-close line을 token-level 2단 정렬로 렌더링하지 않는다. 한 줄에는 한 개의 canonical line indent만 직렬화한다.
- 따라서 줄의 `final depth`는 close를 소비한 뒤 남은 토큰을 continuation/body/header 규칙으로 다시 해석한 결과이며, mixed leading-close line의 실제 render indent도 이 `final depth`를 따른다.
- 특히 parenthesized control condition에서는 `) AND ...`, `) OR ...`만 condition continuation으로 본다. `) = 1 THEN`, `) IS NULL LOOP`처럼 닫힌 조건식을 terminator/header 방향으로 마무리하는 줄은 continuation가 아니라 condition closer로 본다.
- 따라서 pure close line에서는 `final depth == close align depth`지만, mixed leading-close line에서는 `close align depth`가 내부 구조 전이용 값으로만 남고, 렌더링은 `final depth`를 따른다.

### 2.4 선두 close 소비가 먼저

줄이 `)` 로 시작하면, 그 close event를 먼저 소비한 뒤 나머지 토큰을 해석한다.

### 2.5 주석 / 문자열 / quoted literal 내부는 depth event가 아니다

문자열·주석·q-quote·quoted identifier 내부의 괄호는 depth를 바꾸면 안 된다.

### 2.6 analyzer / formatter phase parity

`auto_format_line_contexts`(analyzer)와 `apply_parser_depth_indentation`(formatter phase 2)는
동일한 structural owner taxonomy를 공유해야 한다.

- 한 phase에만 존재하는 owner/frame은 허용하지 않는다.
- analyzer에만 있고 formatter에 없으면 render 단계가 body/query/close 정렬 semantics를 잃는다.
- formatter에만 있고 analyzer에 없으면 `auto_depth` / `query_base_depth` / `next_query_head_depth`가 불완전해지고, phase 2가 analyzer 밖의 임시 보정 규칙을 다시 품게 된다.
- 새 구문을 추가할 때는 `sql_text` helper, analyzer frame, formatter frame, 문서를 같은 owner family로 함께 갱신해야 한다.
- stable layout anchor도 예외가 아니다. `SEARCH/CYCLE`처럼 owner는 아니지만 depth를 재고정하는 clause는 `sql_text` 공통 helper와 analyzer/formatter clause 판별식에 함께 반영해야 한다.
- 단, `CREATE TRIGGER` header나 `FORALL`처럼 line taxonomy보다 statement lifecycle로 관리되는 owner는 `sql_text` helper 대신 각 phase의 전용 frame으로 둘 수 있다.
  이 경우에도 push/pop semantics와 close alignment는 analyzer/formatter 양쪽에서 동일해야 한다.
- continuation/operator RHS가 "다음 줄이 구조 경계인가"를 판단할 때도 동일한 공통 taxonomy를 써야 한다.
  한 phase만 `format_query_owner_*` 같은 부분집합을 직접 열거하고 다른 phase는 shared helper를 쓰면, split multiline owner / PL/SQL child-query owner / stable layout anchor가 continuation depth를 잘못 이어받는 로컬 휴리스틱으로 퇴행한다.
  단, `MULTISET`처럼 generic expression owner는 연산자 RHS continuation을 의도적으로 유지해야 하는 경우가 있으므로, 이런 예외는 `sql_text`의 전용 shared helper 이름으로 문서화하고 양 phase가 똑같이 그 helper를 호출해야 한다.

## 3. 모든 괄호/블록은 같은 push/pop 모델

| 구분 | open | close | close line 정렬 |
|---|---|---|---|
| 일반 괄호 | frame push | frame pop | pop된 owner depth |
| 서브쿼리 괄호 | frame push | frame pop | pop된 owner depth |
| multiline clause 괄호 | frame push | frame pop | pop된 owner depth |
| `BEGIN`/`END` 블록 | frame push | frame pop | pop된 owner depth |
| `CASE`/`END` | frame push | frame pop | pop된 owner depth |
| `THEN`/`ELSE`/`EXCEPTION` | body frame push | 분기 종료 시 pop | — |
| `MERGE WHEN ... THEN` | body frame push | 다음 branch / statement boundary에서 pop | — |
| `INSERT ALL/FIRST` branch | body frame push | 다음 sibling branch / driving query에서 pop | — |
| `FORALL` body | body frame push | body DML statement 종료 시 pop | — |
| `CREATE TRIGGER` header | header/body frame push | `BEGIN` 또는 statement boundary에서 pop | — |

종류만 다르고 전이 규칙(push +1, pop −1, pure close align = owner depth)은 동일하다.

추가 원칙:

- `),` 뒤의 다음 sibling도 새 owner가 아니라 기존 list body 위에서 해석한다.
- comment-only / comma-only line은 frame을 열거나 닫지 않으며, 인접 code line의 구조 depth를 빌려 렌더링만 한다.
- 단, multiline string continuation line과 block comment 내부 raw line처럼 토큰 안전성이 우선인 예외는 verbatim/raw passthrough로 남길 수 있다.
- non-verbatim code line의 render indent는 항상 `final structural depth * 4`다.
- pure close line만 "line depth = close align depth"로 볼 수 있다. leading close 뒤에 continuation/body/header가 이어지는 mixed line은 close 소비와 final depth를 분리해서 해석하되, 실제 렌더링은 `final depth` 기준으로 canonicalize한다.

## 4. continuation line

owner를 열지도 닫지도 않으면, 현재 활성 stack의 depth를 그대로 사용한다.

- 시각 정렬을 위해 기존 공백을 보존하지 않는다. continuation line도 최종 출력은 구조 depth만으로 canonicalize한다.
- split owner/header chain (`WITHIN → GROUP`, `LEFT OUTER → JOIN`, `OPEN → FOR`, `CURSOR → IS`)은 최초 owner line의 구조 depth를 보존한다.
- split PL/SQL child-query header의 중간 식별자/인자 조각도 예외가 아니다. `OPEN → c_cur → FOR`, `CURSOR → c_emp → IS`처럼 header가 아직 완성되지 않았다면 각 code line은 모두 owner depth를 유지하고, 첫 child query/body line에서만 `+1` 전이한다.
- comma는 push/pop event가 아니다. sibling 판단 기준은 활성 owner stack과 list 위치뿐이다.
- continuation state는 blank/comment-only/comma-only line에서 소비되지 않는다.
- continuation state는 첫 consuming code line 또는 새 owner/clause/query boundary가 나타났을 때만 소비되거나 해제된다.
- inline comment split과 bare header split은 다른 feature가 아니라 같은 structural continuation taxonomy를 공유해야 한다.
  analyzer/formatter는 같은 shared helper로 "현재 줄이 다음 code line을 same-depth / query-base+1 / current-line+1로 이어 주는가"를 판단해야 한다.

### 4.2 structural continuation boundary

continuation/operator RHS/header carry를 중단하는 경계는 analyzer/formatter가 같은 shared taxonomy를 사용해야 한다.

- stable clause/query anchor: `SELECT`, `WITH`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `CALL`, `VALUES`, `TABLE`, `FROM`, `WHERE`, `GROUP`, `HAVING`, `ORDER`, `SET`, `INTO`, `USING`, `WINDOW`, `MATCH_RECOGNIZE`, `PIVOT`, `UNPIVOT`, `SEARCH`, `CYCLE`, `RETURNING`, `OFFSET`, `FETCH`, `LIMIT`, `QUALIFY`
- join boundary: `JOIN`, `APPLY`, `LEFT/RIGHT/FULL/CROSS/NATURAL/OUTER ... JOIN|APPLY`
- join condition boundary: `ON`, `USING`
- dedicated clause boundary: `FOR UPDATE`
- owner boundary: shared `sql_text` owner helper가 인식하는 query owner / multiline owner / PL/SQL child-query owner
- standalone `(` wrapper line

그리고 bare header continuation depth 분류도 shared taxonomy여야 한다.

- same-depth header: `WITH` 같은 owner/header chain 조각
- query-base+1 header: `FROM`, `WHERE`, `HAVING`, `USING`, `INTO`, `ON`, `CONNECT`, `START`, `UNION/INTERSECT/MINUS/EXCEPT`, `MODEL`, `WINDOW`, `MATCH_RECOGNIZE`, `PIVOT`, `UNPIVOT`, `QUALIFY`, `SEARCH`, `CYCLE`
- current-line+1 header: `SELECT`, `VALUES`, `SET`, `RETURNING`, `OFFSET/FETCH/LIMIT`, `MEASURES`, `REFERENCE`, `SUBSET`, `PATTERN`, `DEFINE`, `RULES`, `COLUMNS`, `KEEP`, split `JOIN/APPLY`, `GROUP BY`, `ORDER BY`, `PARTITION BY`, `WITHIN GROUP`, `DENSE_RANK FIRST/LAST`, `AFTER MATCH`, `MATCH SKIP`, `START WITH`, `CONNECT BY`

이 분류는 comment split 전용 로직이 아니라 bare header split, inline first-item split, wrapper `(` 앞뒤 continuation에 공통으로 재사용해야 한다.

operator RHS continuation도 shared taxonomy여야 한다.

- shared trailing operator set: `:=`, `=>`, `=`, `<`, `>`, `<=`, `>=`, `<>`, `!=`, `+`, `-`, `*`, `/`, `%`, `||`, `|`, `^`, `AND`, `OR`, `IN`, `IS`, `LIKE`, `BETWEEN`, `NOT`, `EXISTS`
- analyzer와 formatter phase 2는 이 trailing operator 집합을 같은 helper로 판정해야 한다.
- `SELECT *`처럼 projection marker인 `*`는 operator RHS로 오인하면 안 된다.

주의:

- `FOR UPDATE`는 일반 `FOR`와 충돌하므로 generic keyword helper에 섞어 추론하지 말고, 전용 판별식을 shared helper에서 명시적으로 포함한다.
- `MULTISET` 같은 generic expression owner는 operator RHS continuation을 의도적으로 유지할 수 있으므로, boundary helper의 기본 집합에는 넣지 않는다.

### 4.1 completed owner anchor

split owner/header chain이 현재 줄에서 완성되었는데 child body/query/open wrapper가 다음 code line에서 시작하면,
formatter는 그 owner를 즉시 잊으면 안 된다.

- completed owner anchor는 "완성된 owner line의 owner depth"를 그대로 보존하는 pending structural state다.
- completed owner anchor는 child query용 `owner base depth` 또는 `next_query_head_depth`를 추가로 들고 있을 수 있지만,
  wrapper line 정렬에 쓰는 값은 항상 `owner depth`다.
- 특히 `WHERE EXISTS`, `WHERE IN`, `... > ANY`처럼 condition owner는 owner line 자체는 clause/condition depth에 남아 있으면서도,
  child query head만 그보다 한 단계 더 깊어질 수 있다.
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

새 owner family는 `sql_text` 분류 helper 추가 → analyzer context 반영 → formatter phase 2 반영 순서로 넣고, 세 단계가 동일한 push/pop 의미를 공유해야 한다.

렌더링 단계는 구조 depth를 그대로 공백으로 직렬화하는 단계다.
- non-verbatim code line: `render indent = final_depth * 4`
- comment-only / comma-only line: 빌린 structural depth를 동일하게 직렬화
- mixed leading-close code line도 line-level render indent는 `final_depth * 4`를 사용한다. popped owner의 close align depth는 별도 토큰 정렬로 직렬화하지 않는다.
- raw/verbatim line만 원문 공백을 유지할 수 있다
