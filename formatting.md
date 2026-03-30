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
| `render indent` | 최종 렌더링 공백 수. hanging indent 보존은 여기에만 속하며, 구조 depth를 바꾸면 안 됨 |

구조 계산 = `owner/body/list body/close depth`. 렌더링 계산 = `render indent`.

## 2. 핵심 공리

### 2.1 구조 depth는 시각 indent로부터 역산 금지

구조 depth 결정 입력:

- 어떤 owner가 열렸는가 / 닫혔는가
- split owner/header chain이 어느 owner를 계속 들고 가는가
- 다음 child query head가 어떤 owner에서 파생되었는가

기존 줄의 공백 수, 수동 정렬, hanging indent 공백 수 → 구조 판단 근거가 될 수 없다.

### 2.2 모든 open event는 정확히 +1

한 번의 opener가 두 단계 이상을 만들면 안 된다. 다단계 점프처럼 보여도 실제 전이는 항상 "owner frame 하나 push"의 합성이어야 한다.

### 2.3 모든 close event는 pop된 owner의 depth로 정렬

- `)` → frame 하나 `-1`
- `END`, `END CASE`, `END IF`, `END LOOP` → 블록 frame 하나 `-1`

닫힘 depth = "이전 줄 indent − 1" 추정치가 아니라, **실제로 pop된 owner의 depth**.

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

## 4. continuation line

owner를 열지도 닫지도 않으면, 현재 활성 stack의 depth를 그대로 사용한다.

- 시각 정렬은 허용되지만, 구조 depth를 바꾸면 안 된다.
- split owner/header chain (`WITHIN → GROUP`, `LEFT OUTER → JOIN`, `OPEN → FOR`, `CURSOR → IS`)은 최초 owner line의 구조 depth를 보존한다.
- comma는 push/pop event가 아니다. sibling 판단 기준은 활성 owner stack과 list 위치뿐이다.

## 5. 구현 순서

1. 의미 있는 open / close event를 lexical하게 식별
2. 선두 close event를 먼저 소비
3. 남은 토큰으로 현재 줄의 owner/body/header 분류
4. 분류 결과를 활성 owner stack 위에 투영
5. 줄 끝에서 새 open event를 stack에 반영

split owner/header가 다음 줄까지 이어지는 경우:

- pending owner는 원래 owner depth를 그대로 들고 간다
- 현재 줄의 기존 indent로 pending depth를 재보정하면 안 된다

렌더링 단계의 hanging indent 보존은 구조 depth 계산이 끝난 뒤에만 적용한다.

## 6. 현재 구현의 한계 (Phase 1 브릿지)

Phase 2(resolve_code_line_layouts)가 모든 구문의 구조 정보를 갖고 있지는 않다. 다음 경우에는 Phase 1(format_statement)이 부여한 `existing_indent`를 bounded fallback으로 참조한다:

- 트리거 헤더 (BEFORE, REFERENCING, FOR EACH ROW, WHEN)
- FORALL body
- DML fallback (query clause body 등 parser_depth만으로 표현 불가한 depth)

향후 제거 방법:

1. 트리거 헤더 owner frame 추가
2. FORALL body owner frame 추가
3. query clause body depth의 Phase 2 독립 추적
