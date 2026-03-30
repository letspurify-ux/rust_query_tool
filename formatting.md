# SQL Auto Formatting Depth Principles

이 문서는 특정 구현이 아니라, 자동 포맷팅 depth가 따라야 하는 이론적 규칙을 정리한다.

## 1. Depth의 정의

depth는 "현재 줄이 몇 칸 들여쓰기되어 보이는가"가 아니라, 현재 시점에 **열려 있는 구문 소유자(active syntactic owners)** 가 몇 겹인가를 나타내는 구조 값이다.

여기서 소유자는 다음처럼 "몸체를 열고, 나중에 닫히는" 단위를 말한다.

- 일반 괄호 표현식
- 서브쿼리 소유 괄호
- `BEGIN ... END`, `CASE ... END`, `IF ... END IF` 같은 블록
- `OVER (...)`, `WITHIN GROUP (...)`, `MATCH_RECOGNIZE (...)`, `PIVOT (...)` 같은 다중행 clause owner
- `THEN`, `ELSE`, `EXCEPTION` 처럼 하위 body를 여는 분기/핸들러

즉, depth는 "활성 owner 스택의 높이"다.

추가로 구현에서는 아래 5개 값을 반드시 구분해야 한다.

- `owner depth`: owner header line 자신이 놓이는 구조 depth
- `body depth`: 그 owner가 여는 본문 첫 레벨의 depth. 항상 `owner depth + 1`
- `list body depth`: comma-separated sibling list가 놓이는 구조 depth. 보통 enclosing clause/body의 첫 레벨이지만, `FROM dept d,` 처럼 owner render line과 첫 item이 한 줄에 함께 있는 mixed line에서는 현재 줄 render depth와 분리된 별도 상태로 유지해야 한다
- `close depth`: 닫히는 줄이 돌아가야 하는 depth. 항상 pop된 owner의 `owner depth`
- `render indent`: 최종 렌더링에서 보이는 공백 수. hanging indent나 odd indent 보존은 여기에만 속하고, 구조 depth를 바꾸면 안 된다

즉, 구조 계산 단계는 `owner/body/list body/close depth`만 다루고, 렌더링 단계만 `render indent`를 다뤄야 한다.

## 2. 핵심 공리

### 2.1 구조 depth는 시각 indent로부터 역산하면 안 된다

이전 줄이 우연히 많이 들여쓰기되어 있었다고 해서 현재 줄의 구조 depth가 더 깊어지면 안 된다.

구조 depth는 오직 다음 두 정보로만 결정되어야 한다.

- 어떤 owner가 열렸는가
- 어떤 owner가 닫혔는가
- split owner/header chain이 어느 owner를 계속 들고 가는가
- 다음 child query head가 어떤 owner에서 파생되었는가

기존 줄의 공백 수, 수동 정렬, 임시 과도 들여쓰기는 구조 판단의 근거가 될 수 없다.

여기에는 다음도 포함된다.

- `NOT` → `EXISTS`, `LEFT OUTER` → `JOIN`, `CURSOR` → `(`, `OPEN` → `FOR` 같은 split owner/header chain
- analyzer가 미리 계산해 둔 다음 child query head depth

이 값들은 모두 이미 구조 정보의 일부이므로, 현재 줄의 기존 indent와의 차이로 다시 보정하면 안 된다.

특히 다음 경우도 예외가 아니다.

- 줄 끝 inline comment가 다음 줄 continuation을 유도하더라도, continuation depth는 현재 줄의 **구조 depth** 에서만 계산해야 한다.
- 이미 형성된 hanging indent를 렌더링 단계에서 보존하더라도, 그 공백 수를 다음 줄의 owner/body depth 계산에 재사용하면 안 된다.

### 2.2 모든 open event는 정확히 1단계만 깊어진다

어떤 구문이 하위 body를 열면 depth는 정확히 `+1` 된다.

- `(` 는 일반 괄호 frame 하나를 연다.
- 서브쿼리를 여는 `(` 도 query owner frame 하나를 연다.
- `BEGIN`, `CASE`, `THEN`, `ELSE`, `EXCEPTION` 같은 body opener도 각자 frame 하나를 연다.
- `WITH cte AS (` 같은 child-query owner도 CTE header 자체는 현재 owner depth를 유지하고, 이어지는 child query head만 정확히 `+1` 된다.
- `WHEN MATCHED THEN`, `WHEN NOT MATCHED THEN` 같은 MERGE branch header도 body frame 하나만 연다.

한 번의 opener가 두 단계 이상을 만들면 안 된다.

주의:

- `query base depth`, `owner depth`, `child query head depth`는 서로 다른 이름이다.
- 어떤 줄이 결과적으로 `query base + 2`에 놓이더라도, 그 이유는 "이미 owner line이 `query base + 1`이고 child head가 owner 기준 `+1`"이기 때문이어야 한다.
- 즉, 다단계 점프처럼 보여도 실제 전이는 항상 "owner frame 하나 push"의 합성으로 설명 가능해야 한다.

### 2.3 모든 close event는 자신이 연 frame 하나만 닫는다

닫힘은 항상 대응되는 opener가 만든 frame 하나를 제거해야 한다.

- `)` 는 자신이 닫는 frame 하나만 `-1` 한다.
- `END` 는 자신이 닫는 블록 하나만 `-1` 한다.
- `END CASE`, `END IF`, `END LOOP` 역시 동일하다.

닫힘 depth는 "이전 줄 indent - 1" 같은 추정치가 아니라, **실제로 pop된 owner의 깊이** 로 정렬되어야 한다.

특히 `END LOOP`, `END WHILE`, `END FOR`, `END REPEAT` 같은 suffix terminator도 예외가 아니다.
이 줄들이 기존 source indent를 조금 더 가지고 있었다고 해서 `owner depth + 1`에 머물러서는 안 된다.

### 2.4 같은 줄의 분류보다 선두 close 소비가 먼저다

줄이 `)` 로 시작하거나, 주석/공백 뒤에 `)` 가 먼저 나타나면, 그 close event를 먼저 소비한 뒤 나머지 토큰을 해석해야 한다.

예를 들어 다음 줄은 이미 한 단계 닫힌 상태에서 해석되어야 한다.

```sql
/* note */ ) AND status = 'A'
```

즉, "선두 닫힘을 먼저 pop하고, 남은 토큰을 분류한다"가 원칙이다.

### 2.5 comments / 문자열 / quoted literal 내부 문자는 depth event가 아니다

다음은 depth를 바꾸면 안 된다.

- 문자열 안의 `(` `)`
- 주석 안의 `(` `)`
- q-quote, quoted identifier 내부의 괄호

depth는 **의미 있는(significant)** 구분자만 소비해야 한다.

## 3. 일반 괄호의 이론

일반 괄호는 가장 단순한 stack discipline을 따른다.

1. 의미 있는 `(` 를 만나면 현재 일반 괄호 stack 위에 frame 하나를 push한다.
2. 그 body 내부 줄은 현재 활성 일반 괄호 수만큼 깊어진다.
3. 의미 있는 `)` 를 만나면 frame 하나를 pop한다.
4. `)` 뒤에 식이 이어지면, 이어지는 토큰은 "닫힌 뒤 남아 있는 stack" 기준으로 해석한다.

따라서 일반 괄호 depth는 본질적으로 다음과 같다.

- open count = `+1`
- close count = `-1`
- 연속된 `(((` 는 3단계 progressive depth
- `)) AND ...` 는 두 번 pop한 뒤 남은 frame 수 기준 depth

## 4. 괄호 owner의 종류만 다르고 전이 규칙은 같다

일반 괄호와 서브쿼리 괄호는 역할이 다를 뿐, 전이 규칙은 동일하다.

- 일반 괄호: expression continuation depth를 만든다.
- query 괄호: child query owner depth를 만든다.
- multiline clause 괄호: owner-relative body depth를 만든다.

하지만 셋 다 본질은 같다.

- 열릴 때 frame 하나 push
- 닫힐 때 frame 하나 pop
- close line은 pop된 owner depth에 정렬

즉, depth의 증감 규칙은 동일하고, 달라지는 것은 "그 frame이 body를 어떤 이름으로 해석하느냐" 뿐이다.

## 5. 괄호 밖 구조도 같은 모델로 봐야 한다

블록 구문도 괄호와 같은 추상 모델로 정리할 수 있다.

- `BEGIN` 은 block frame open
- `END` 는 block frame close
- `CASE` 는 case frame open
- `WHEN ... THEN`, `ELSE` 는 branch body frame open
- `WHEN MATCHED THEN`, `WHEN NOT MATCHED THEN` 도 branch body frame open
- branch가 끝나면 해당 frame close
- `EXCEPTION` 은 handler frame open

즉, 모든 depth 전이는 "owner stack의 push / pop"으로 환원되어야 한다.

## 6. continuation line의 원칙

continuation line은 새로운 owner를 열지도 닫지도 않으면, 현재 활성 stack의 depth를 그대로 사용해야 한다.

예외는 없다.

- continuation의 시각 정렬은 허용될 수 있다.
- 그러나 그 정렬이 구조 depth를 바꾸면 안 된다.
- inline comment 뒤 continuation이나 split owner/header fragment도 동일하다. 이전 줄이 우연히 과도하게 들여쓰기되어 있었다고 해서 다음 줄 depth가 더 깊어지면 안 된다.

따라서 continuation 계산은 "현재 활성 frame 집합"의 투영이어야 하며, 이전 줄의 우연한 공백을 재사용하면 안 된다.

split owner/header chain도 continuation의 한 종류로 본다.

- `WITHIN` → `GROUP`
- `LEFT OUTER` → `JOIN`
- `OPEN` → `FOR`
- `CURSOR` → `IS`

이 체인은 중간 줄이 얼마나 들여쓰기되어 있었는지와 무관하게, 최초 owner line의 구조 depth를 그대로 보존해야 한다.

### 6.1 comma-separated sibling도 구조로만 정렬해야 한다

`,` 자체는 owner frame을 열거나 닫지 않는다.

- comma는 push/pop event가 아니다.
- comma 다음 줄이 새로운 sibling item인지, 같은 owner의 continuation인지, child query owner인지 판단하는 기준은 현재 활성 owner stack과 list 위치뿐이다.
- 이 판단에 기존 source의 공백 수를 사용하면 안 된다.

특히 multiline `FROM` item list는 이 규칙을 직접 따른다.

- standalone `FROM` header 다음의 첫 item line도, comma 다음 sibling item도, 모두 enclosing `FROM` list의 **구조적 item depth** 로 정렬되어야 한다.
- `LATERAL`, `TABLE`, split `JOIN/APPLY` owner는 그 list 안의 sibling item으로 해석되어야 하며, 원본 공백이 아니라 구조적 item depth를 따라야 한다.
- `FROM dept d,` 처럼 clause owner와 첫 item이 한 줄에 같이 렌더링되는 mixed line도 예외가 아니다. 이 경우 현재 줄 render depth는 owner depth일 수 있지만, formatter/analyzer는 **다음 sibling이 붙을 list body depth를 별도 상태로 유지** 해야 한다.
- 원본 줄이 우연히 과도하게 들여쓰기되어 있었거나, 반대로 덜 들여쓰기되어 있었다는 사실은 item depth를 바꾸는 근거가 될 수 없다.
- comment/comma run 사이에 끼어 있는 줄도 동일하다. run 전체는 인접 sibling들의 구조 depth에 붙어야 한다.

## 7. 이 원칙으로 판단한 잘못된 구현 신호

다음 구현은 이론적으로 잘못된 신호다.

- 구조 depth를 이전 줄 indent나 수동 공백 수에서 가져오는 것
- close 여부를 raw `starts_with(')')` 같은 문자 접두 검사로만 판단하는 것
- 하나의 opener가 상황에 따라 `+2`, `+3`처럼 여러 단계 depth를 직접 만드는 것
- close line 정렬을 "직전 줄 depth - 1" 같은 휴리스틱으로 계산하는 것
- 주석이나 문자열 안의 괄호를 실제 close/open event로 취급하는 것

## 8. 구현이 따라야 할 결론

자동 포맷팅 구현은 결국 다음 순서를 따라야 한다.

1. 의미 있는 open / close event를 lexical하게 식별한다.
2. 선두 close event를 먼저 소비한다.
3. 남은 토큰으로 현재 줄이 어떤 owner/body/header인지 분류한다.
4. 분류 결과를 현재 활성 owner stack 위에 투영한다.
5. 줄 끝에서 새 open event를 stack에 반영한다.

추가로, split owner/header가 다음 줄까지 이어지는 경우에도 같은 원칙을 유지해야 한다.

- pending owner는 원래 owner depth를 그대로 들고 간다.
- 다음 child query head depth도 구조적으로 계산된 값을 그대로 소비한다.
- 현재 줄이 우연히 덜/더 들여쓰기되어 있었다는 이유로 pending depth를 재보정하면 안 된다.

마지막으로, 렌더링 단계에서 odd hanging indent를 보존하더라도 이는 어디까지나 "시각적 표현"이어야 한다.

- 구조 depth 계산은 먼저 끝나 있어야 한다.
- hanging indent 보존 여부는 계산된 구조 depth를 바꾸지 않는 범위에서만 결정되어야 한다.

이 순서를 벗어나는 예외 규칙은 대부분 depth 불일치의 원인이다.
