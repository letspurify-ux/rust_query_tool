# SQL Auto Formatting Depth Principles

## 0. 이 문서의 역할

이 문서는 아래 3층을 구분해서 적는다.

- 근본 원칙: 새 구문이 추가돼도 쉽게 바뀌면 안 되는 규칙
- 구조 계약: analyzer와 formatter가 반드시 보존해야 하는 상태 모델
- 현재 taxonomy / policy: 지금 코드베이스가 채택한 keyword 집합, owner family, 렌더링 정책

정리 기준:

- "왜 항상 그래야 하는가"는 근본 원칙에 둔다.
- "어떤 상태를 들고 가야 하는가"는 구조 계약에 둔다.
- "현재는 어떤 keyword를 이렇게 분류하는가"는 taxonomy / policy에 둔다.

## 1. 근본 원칙

### 1.1 depth는 시각 indent가 아니라 구조 상태에서 결정된다

depth는 현재 시점에 활성화된 syntactic owner stack의 높이다.

- depth는 기존 공백 수, hanging indent, 수동 정렬에서 역산하면 안 된다.
- `existing_indent`는 구조 계산의 fallback, soft floor, tie-breaker가 될 수 없다.
- 이전 줄 정보를 쓰더라도 반드시 이름 붙은 pending/active structural state로 승격된 값만 써야 한다.
- `직전 줄이 콤마였다`, `직전 줄이 THEN이었다`, `직전 줄이 '('였다` 같은 anonymous line-shape heuristic로 depth를 복원하면 안 된다.
- line-shape 정보는 "지금 pending state를 소비할 차례인가"를 판별하는 lexical adjacency 용도로만 허용된다.

### 1.2 모든 open event는 정확히 +1이다

한 번의 opener가 구조적으로 두 단계 이상을 열면 안 된다.

- 다단계 깊이 변화처럼 보여도 실제 모델은 owner/frame push의 합성이어야 한다.
- 줄 단위 `final depth` 차이가 2 이상으로 보이는 것은 허용되지만, 그것은 여러 open/close/anchor event가 한 줄에 함께 반영된 결과여야 한다.

### 1.3 pure close align은 pop된 owner depth를 따른다

닫힘 정렬은 "이전 줄보다 한 단계 덜 들여쓰기"가 아니라 실제로 pop된 owner의 depth를 기준으로 한다.

- `)`는 pop된 paren owner depth에 정렬한다.
- `END`, `END CASE`, `END IF`, `END LOOP`도 pop된 block owner depth에 정렬한다.
- query close line은 stored query close alignment를 사용한다.

### 1.4 leading close는 항상 먼저 소비한다

줄이 leading `)`로 시작하면 그 close event를 먼저 소비하고, 남은 tail을 현재 줄 구조로 다시 해석해야 한다.

- `) ORDER BY ...` 는 raw line 전체가 아니라 `ORDER BY ...`를 구조 tail로 분류한다.
- `) AND ...`, `) OR ...`, `) FOR UPDATE ...`도 같은 규칙을 따른다.
- mixed leading-close line은 close align과 final depth를 구분해서 해석해야 한다.
- 이 정규화는 caller 습관이 아니라 shared owner/header helper의 책임이어야 한다. caller 한 곳만 `structural tail`로 전처리하고 helper 본문이 raw line을 가정하면 `) REFERENCE ... ON`, `) WINDOW ... AS`, `) OPEN ... FOR` 같은 mixed leading-close owner/header가 phase마다 다른 depth를 만들게 된다.
- 단, close를 소비한 뒤 structural tail이 비었다고 해서 close event 자체가 사라지는 것은 아니다. nested paren을 추적하는 pending header/owner는 pure `)` line을 "빈 줄"이 아니라 "wrapper close continuation step"으로 해석해야 한다.

### 1.5 deferred structural effect는 typed pending state로 유지한다

한 줄에서 끝나지 않는 구조 효과는 "깊이 하나"로 축약하면 안 된다.

- split owner/header
- split `MERGE WHEN ... THEN` branch header
- completed owner anchor
- deferred query head
- pure/mixed close align
- split plain `END` suffix/label carry
- standalone wrapper carry
- parenthesized expression carry

이런 상태는 필요한 의미를 잃지 않는 typed pending state로 유지해야 한다.

- 같은 line이 dedicated pending family와 generic owner/header carry에 동시에 등록되면 안 된다.
- 예를 들어 active `MERGE` branch header의 `WHEN NOT` / standalone `NOT` fragment는 merge-header pending state로만 남아야지, generic `NOT EXISTS` owner carry로 중복 승격되면 안 된다.

### 1.6 analyzer와 formatter는 같은 semantics를 공유해야 한다

`auto_format_line_contexts`와 formatter phase 2는 같은 구조 의미를 공유해야 한다.

- 한 phase만 아는 owner/frame은 허용하지 않는다.
- stable anchor, owner-relative family, continuation boundary도 같은 semantics를 따라야 한다.
- exact bare keyword-only header line의 continuation taxonomy도 예외가 아니다. inline comment split용 prefix classifier와 bare-line classifier가 서로 다른 hand-maintained keyword list를 가지면 안 된다.
- 다만 "같은 prefix taxonomy를 쓴다"가 "inline comment split과 exact bare line이 항상 같은 continuation depth를 가진다"를 의미하지는 않는다. exact bare line이 여전히 dedicated same-depth owner/header chain fragment라면 (`WITHIN GROUP`, `DENSE_RANK LAST`, `AFTER MATCH SKIP`, `LEFT OUTER`, `REFERENCE`, `CURSOR` 등) bare carry는 same-depth여야 하고, inline comment split만 body depth를 빌릴 수 있다.
- same token이 carry를 열고/닫거나 frame reset을 일으키는 경우도 예외가 아니다. semicolon/comma/standalone `(` 같은 punctuation-driven state transition은 analyzer와 formatter가 같은 trailing/standalone structural helper를 공유해야 한다.
- 특정 구문이 애매하면 "같은 semantic decision을 양쪽에서 재현한다"가 원칙이고, helper를 하나로 합칠지 전용 판별식을 둘지는 구현 전략이다.

### 1.7 comment와 quoted literal은 구조 이벤트가 아니다

주석/문자열/quoted identifier 안의 토큰은 depth event를 만들거나 지우면 안 된다.

- trailing inline comment나 inline block comment가 split owner/open/close recognition을 끊으면 안 된다.
- leading block comment가 붙은 code line도 comment-only line으로 취급하면 안 된다. `/* note */ ON ...`, `/* note */ ORDER BY ...`, `/* note */ BEGIN ...` 같은 line은 첫 meaningful structural token부터 다시 분류해야 한다.
- line head prefix 판정도 예외가 아니다. `CREATE /* gap */ MATERIALIZED /* gap */ VIEW ... AS`, `OPEN c /* gap */ FOR`, `CURSOR c /* gap */ IS`, `WHEN /* gap */ NOT /* gap */ MATCHED THEN`, `MATCH /* gap */ RECOGNIZE` 같은 multi-keyword owner/header는 raw prefix string이나 `contains()`가 아니라 comment-stripped meaningful identifier sequence로 판정해야 한다.
- control-branch owner/header 판정도 예외가 아니다. exact `ELSE`, exact `EXCEPTION`, `ELSIF ... THEN`, `ELSEIF ... THEN`, `CASE` 같은 PL/SQL branch header는 `ELSE/* gap */`, `EXCEPTION/* gap */`, `ELSIF/* gap */ cond THEN` 형태여도 raw `trim()`/`starts_with()`가 아니라 comment-stripped structural token sequence로 판정해야 한다.
- underscore를 가진 composite keyword(`MATCH_RECOGNIZE`, `DENSE_RANK` 등)도 예외가 아니다. 구현 내부 표현이 단일 identifier이든, comment/whitespace 때문에 여러 identifier segment로 보이든 structural classifier는 같은 keyword sequence로 취급해야 한다.
- 이 원칙은 leading/trailing header continuation classifier와 owner-relative split body-header matcher에도 동일하게 적용된다. 특정 phase만 raw word pair 비교를 쓰면 같은 composite keyword가 줄 위치에 따라 다른 depth를 만들게 된다.
- same-line classifier뿐 아니라 next/previous-line lookahead도 예외가 아니다. split owner lookahead, blank-line suppression, case-close alignment, control-condition close 판정처럼 "인접 line의 첫 구조 토큰"을 보는 로직도 raw `trim()`/`starts_with()`가 아니라 comment-stripped structural tail 기준으로 판정해야 한다.
- `END /* gap */ IF`, `END -- gap` 다음 suffix line, `) /* gap */ ORDER BY` 같은 형태도 주석을 제거한 structural token sequence로 판정해야 한다.
- line tail/suffix 판정도 예외가 아니다. `GROUP /* gap */ BY -- ...`, `FOR /* gap */ UPDATE -- ...`, `IF ... /* gap */ THEN`처럼 split header나 trailing terminator를 판정할 때도 raw whitespace split이 아니라 comment-stripped meaningful identifier sequence를 사용해야 한다.
- statement terminator 판정도 예외가 아니다. `OPEN c_emp; -- done`, `CURSOR c_emp IS; /* impossible but lexical */`, `END; -- block close` 같은 line은 raw `trim_end().ends_with(';')`가 아니라 inline comment를 제거한 뒤의 마지막 meaningful token으로 닫힘 여부를 판정해야 한다.
- exact bare header / standalone wrapper 판정도 예외가 아니다. `FROM /* gap */`, `WHERE /* gap */`, `ON /* gap */`, `( -- wrapper` 같은 line은 raw `trim()` / `==` 비교가 아니라 shared structural token / wrapper helper로 판정해야 한다.
- 구조 helper는 필요하면 "원문 문자열"이 아니라 "comment를 제거한 meaningful token sequence"를 기준으로 동작해야 한다.

## 2. 구조 계약

### 2.1 owner line과 anchor line은 구분한다

모든 depth 재고정 line이 owner push/pop을 의미하는 것은 아니다.

- owner line: 실제로 owner/frame을 열거나 닫는 line
- anchor line: stack 높이는 바꾸지 않지만 현재 line depth나 다음 body/query depth를 다시 고정하는 line

즉 formatter는 "owner가 열렸는가"와 "정렬 anchor가 바뀌었는가"를 분리해서 다뤄야 한다.

### 2.2 구조 계산에서 구분해야 하는 값

| 이름 | 의미 |
|---|---|
| `owner depth` | owner header line 자신의 구조 depth |
| `body depth` | owner가 여는 본문 depth. 항상 `owner depth + 1` |
| `list body depth` | sibling list가 이어지는 body depth |
| `close align depth` | leading close를 먼저 소비했을 때 참조하는 구조 정렬 depth |
| `final depth` | leading close 소비와 continuation/body/header 해석까지 끝난 현재 줄의 최종 structural depth |
| `render indent` | 렌더링 공백 수 |

현재 구현 대응:

- `parser_depth`: lexical leading close를 먼저 소비한 뒤의 기본 structural depth
- `auto_depth`: analyzer가 계산한 현재 code line의 structural depth
- `query_base_depth`: 현재 query frame의 base depth
- `next_query_head_depth`: 현재 owner가 여는 다음 child query head depth
- `final_depth`: formatter phase 2가 정규화를 마친 최종 structural depth

### 2.3 owner align depth와 owner base depth는 다르다

같은 숫자로 시작할 수는 있어도 의미가 다르다.

- wrapper / split-header / close alignment에는 `owner align depth`를 쓴다.
- child query head 계산에는 `owner base depth`를 쓴다.
- 두 값을 섞으면 split owner와 child query indentation이 쉽게 뒤틀린다.

### 2.4 completed owner anchor는 별도 상태로 유지한다

split owner/header가 현재 줄에서 완성됐더라도, child body/query/open wrapper가 다음 code line에서 시작하면 owner를 즉시 잊으면 안 된다.

- completed owner anchor는 완성된 owner line의 `owner depth`를 유지하는 pending state다.
- child query용 `owner base depth` 또는 `next_query_head_depth`를 추가로 들고 갈 수 있다.
- 하지만 wrapper line 정렬 기준은 항상 `owner depth`다.
- comment-only / blank / comma-only line은 anchor를 소비하지 않는다.
- standalone `(`, leading `)` wrapper line도 anchor를 먼저 소비하지 않는다.

### 2.5 continuation carry는 explicit state로만 이어진다

owner를 열지도 닫지도 않는 line은 활성 stack과 explicit continuation state에 따라 해석한다.

- split owner/header chain은 최초 owner line의 구조 depth를 보존한다.
- split PL/SQL child-query header도 header 완성 전까지 owner depth를 유지한다.
- split `MERGE WHEN ... THEN` header도 `WHEN`/`WHEN NOT`/`MATCHED` fragment와 standalone `WHEN MATCHED`/`WHEN NOT MATCHED` header line은 owner depth를 유지하고, 그 다음 header condition/standalone `THEN` consuming line만 `owner depth + 1`을 사용한다.
- split header condition 안에 nested child query/owner가 들어오면 retained header state는 즉시 해제되지 않고 suspend 된다. nested child가 닫힌 뒤 merge-header를 다시 소비하는 line(`THEN`, `) THEN`, 이어지는 condition line)에서 resume 되어야 한다.
- split plain `END` suffix/label carry도 qualifier/label token이 comment에 가려져도 owner depth를 잃지 않는다.
- continuation state는 blank/comment-only/comma-only line에서 소비되지 않는다.
- continuation state는 첫 consuming code line 또는 새 owner/clause/query boundary에서만 소비되거나 해제된다.

### 2.6 mixed leading-close line은 close align과 final depth를 분리한다

- pure close line에서는 `final depth == close align depth`로 볼 수 있다.
- mixed leading-close line에서는 close를 먼저 소비한 뒤 continuation/body/header 규칙으로 다시 해석한 결과가 `final depth`다.
- 실제 렌더링은 token-level 2단 정렬이 아니라 line-level canonical depth를 사용한다.

## 3. 현재 taxonomy / policy

이 절은 현재 코드베이스의 taxonomy와 policy를 적는다.
새 구문이 늘거나 분류가 바뀌면 이 절은 바뀔 수 있지만, 1절과 2절의 원칙은 가능한 유지해야 한다.

### 3.1 현재 owner family

현재 depth 모델이 명시적으로 다루는 owner family:

- 일반 괄호 표현식
- 서브쿼리 소유 괄호
- `BEGIN … END`, `CASE … END`, `IF … END IF` 블록
- `OVER (…)`, `WITHIN GROUP (…)`, `WINDOW (…)`, `MATCH_RECOGNIZE (…)`, `PIVOT (…)`, `UNPIVOT (…)`, `MODEL (…)`, `JSON_TABLE ... NESTED/COLUMNS (...)` 같은 multiline clause owner
- `CURSOR ... IS`, `OPEN ... FOR`, control-body query owner 같은 PL/SQL child-query owner
- `THEN`, `ELSE`, `EXCEPTION` body owner
- `MERGE WHEN ... THEN`, `INSERT ALL/FIRST`, `FORALL` 같은 DML/PLSQL body owner
- `CREATE TRIGGER` header body owner
- `CREATE TABLE ... AS`, `CREATE [MATERIALIZED] VIEW ... AS` 같은 DDL header body owner

### 3.2 current structural continuation boundary taxonomy

현재 continuation/operator RHS/header carry를 끊는 boundary는 다음 taxonomy를 사용한다.

- stable clause/query anchor: `SELECT`, `WITH`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `CALL`, `VALUES`, `TABLE`, `FROM`, `WHERE`, `GROUP`, `HAVING`, `ORDER`, `SET`, `INTO`, `USING`, `WINDOW`, `MATCH_RECOGNIZE`, `PIVOT`, `UNPIVOT`, `SEARCH`, `CYCLE`, `RETURNING`, `OFFSET`, `FETCH`, `LIMIT`, `QUALIFY`
- join boundary: `JOIN`, `APPLY`, `LEFT/RIGHT/FULL/CROSS/NATURAL/OUTER ... JOIN|APPLY`
- join condition boundary: `ON`, `USING`
- dedicated clause boundary: `FOR UPDATE`
- owner boundary: shared `sql_text` helper가 인식하는 query owner / multiline owner / PL/SQL child-query owner
- merge branch header boundary: `WHEN`, `WHEN NOT`, `WHEN MATCHED`, `WHEN NOT MATCHED`
- standalone `(` wrapper line

주의:

- mixed leading-close line은 raw line이 아니라 close를 소비한 structural tail로 위 taxonomy에 대입한다.
- `FOR UPDATE`처럼 다른 keyword와 충돌 가능한 구문은 현재 policy상 dedicated handling을 둔다.
- split `MERGE` branch header는 generic condition continuation이 아니라 dedicated pending merge-branch-header state로 처리한다.
- incomplete split `MERGE` fragment(`WHEN`, `WHEN NOT`)도 shared structural boundary helper가 먼저 끊어줘야 한다. 그래야 generic continuation carry와 dedicated merge-header state가 충돌하지 않는다.

### 3.3 current bare header continuation taxonomy

현재 bare header continuation depth 분류:

- bare-header carry는 comment를 제거한 structural tail이 "exact keyword-only line"일 때만 켠다. `SELECT DISTINCT`는 bare header지만 `SELECT DISTINCT empno`는 bare header가 아니다.
- same-depth header: `WITH` 같은 owner/header chain 조각
- same-depth merge-header header line: retained `WHEN`, `WHEN NOT`, pending state가 넘겨주는 split `MATCHED`, standalone `WHEN MATCHED`, standalone `WHEN NOT MATCHED`
- query-base+1 header: `FROM`, `WHERE`, `HAVING`, `USING`, `INTO`, `ON`, `UNION/INTERSECT/MINUS/EXCEPT`, `QUALIFY`, `SEARCH`, `CYCLE`, `FOR UPDATE`
- current-line+1 merge-header condition: retained merge-header state가 소비하는 `AND`/`OR` condition line, standalone `THEN`, mixed close tail `) THEN`
- current-line+1 generic header: `SELECT`, `SELECT DISTINCT/UNIQUE/ALL`, `VALUES`, `SET`, `RETURNING`, `OFFSET/FETCH/LIMIT`, exact bare `JOIN/APPLY` modifier chain, `GROUP BY`, `ORDER BY`, `PARTITION BY`, `START WITH`, `CONNECT BY`
- owner-relative body header family: `MEASURES`, `REFERENCE`, `SUBSET`, `PATTERN`, `DEFINE`, `RULES`, `COLUMNS`, `KEEP`
- owner-relative split-header state machine: active multiline owner 안에서는 `WITHIN -> GROUP`, `DENSE_RANK -> FIRST/LAST -> ORDER -> BY`, `AFTER -> MATCH SKIP -> TO NEXT ROW`, `ROWS -> BETWEEN ...`, `RETURN -> UPDATED/ALL -> ROWS`, `RULES -> AUTOMATIC/SEQUENTIAL -> ORDER` 같은 multi-step sequence를 dedicated state로 추적한다. 이 family는 bare-header carry가 있더라도 final canonical depth를 active owner body depth 기준으로 다시 snap 할 수 있다.

주의:

- `WINDOW`, `MATCH_RECOGNIZE`, `PIVOT`, `UNPIVOT`, `MODEL`은 generic bare-header continuation consumer가 아니라 dedicated owner-relative / subclause family로 관리한다.
- `MEASURES`, `REFERENCE`, `SUBSET`, `PATTERN`, `DEFINE`, `RULES`, `COLUMNS`, `KEEP`은 generic query-base carry가 아니라 active owner frame의 body depth에 먼저 snap 되어야 한다.
- split multiline owner/modifier의 exact bare keyword-only line도 generic carry와 dedicated owner-relative state를 같은 shared prefix taxonomy 위에서 해석해야 한다. 단, 최종 `final depth`는 active owner state가 canonicalize한다.
- split `MERGE` header의 standalone `THEN`은 generic bare-header consumer가 아니라 retained merge-branch-header condition depth를 그대로 사용한다.

### 3.4 current operator RHS continuation policy

현재 trailing operator set:

- `:=`, `=>`, `=`, `<`, `>`, `<=`, `>=`, `<>`, `!=`, `+`, `-`, `*`, `/`, `%`, `||`, `|`, `^`, `AND`, `OR`, `IN`, `IS`, `LIKE`, `BETWEEN`, `NOT`, `EXISTS`

주의:

- `SELECT *`의 `*`는 projection marker이므로 operator RHS continuation으로 보면 안 된다.
- `MULTISET` 같은 generic expression owner는 현재 policy상 boundary helper 기본 집합에 넣지 않는다.

### 3.5 current render policy

현재 렌더링 정책:

- non-verbatim code line: `render indent = final_depth * 4`
- comment-only / comma-only line: 빌린 structural depth를 같은 폭으로 직렬화
- mixed leading-close code line도 line-level render indent는 `final_depth * 4`를 사용한다.
- raw/verbatim line만 원문 공백을 유지할 수 있다.

`4`는 현재 policy이며, 근본 원칙은 "render indent는 final depth와 configured indent width의 함수여야 한다"이다.

## 4. 구현 및 유지보수 규칙

### 4.1 한 줄 처리 순서

1. 의미 있는 open / close event를 lexical하게 식별한다.
2. 선두 close event를 먼저 소비한다.
3. 남은 토큰으로 owner/body/header/continuation을 분류한다.
4. 분류 결과를 활성 owner stack과 pending state 위에 투영한다.
5. 줄 끝에서 새 open event와 새 pending state를 반영한다.

### 4.2 split owner / header carry 규칙

- pending owner는 원래 owner depth를 그대로 들고 간다.
- owner header가 현재 줄에서 완성되어도 child body/query/open wrapper가 다음 줄에 시작하면 completed owner anchor를 유지한다.
- `MERGE WHEN ... THEN` split header는 header pending state가 `THEN`에서 body-depth token을 방출하기 전까지 branch body depth를 조기 확정하면 안 된다.
- `MERGE WHEN ... THEN` split header가 nested child query 안으로 들어가면 merge-header pending state는 child query line에서 line-local heuristic로 재구성하면 안 되고, child query가 닫힐 때까지 유지한 뒤 `THEN` 또는 이어지는 merge-header condition line에서 다시 소비해야 한다.
- plain `END` 뒤 qualifier/label split도 explicit pending state로 유지한다.
- qualified `END IF;`/`END LOOP;` 뒤 comment gap이 있어도, 다음 code line을 line-shape으로 추측하지 말고 retained scope state로 정렬한다.

### 4.3 lossy pending state 금지

- deferred query head
- completed owner anchor
- pure/mixed close align
- parenthesized `CASE` close
- standalone `(` wrapper carry

이런 상태는 "깊이 하나"만 들고 가지 말고, 필요한 의미를 보존하는 typed state로 유지해야 한다.

예:

- `owner align depth`
- `owner base depth`
- `next query head depth`
- `close align depth`
- `general paren floor`

### 4.4 새 owner family 추가 순서

새 owner family를 추가할 때는 아래 순서를 지킨다.

1. `sql_text` 분류 helper를 추가한다.
2. analyzer context/state에 반영한다.
3. formatter phase 2에 같은 semantics로 반영한다.
4. 이 문서의 3절 taxonomy / policy를 갱신한다.

검증 기준은 "같은 입력에서 analyzer와 formatter가 같은 구조 이야기를 하는가"다.
