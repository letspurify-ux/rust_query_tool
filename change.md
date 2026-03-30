# Auto Formatting Paren Depth Changes

## 기준 규칙

- 조건 continuation 기본 depth는 owner line보다 `+1`이다.
- 일반 괄호 `(` 가 열릴 때마다 depth가 `+1` 된다.
- 일반 괄호 `)` 가 닫히면 남아 있는 열린 괄호 수만큼만 유지되고, 닫힌 괄호 depth는 제거된다.
- 이 규칙은 `JOIN ... ON`, `WHERE`, `CASE`, `OPEN ... FOR` 안쪽 표현식에도 동일하게 적용된다.

## 왜 테스트 기대값이 바뀌었는가

기존 일부 테스트는 다음과 같은 예외 동작을 기대하고 있었다.

- `ON (` 안쪽 첫 `AND`가 괄호 depth를 반영하지 않음
- `ON (...)` 뒤 `OR`가 continuation depth가 아니라 `ON` depth로 복귀함
- `SELECT (` 다음 `CASE`가 괄호 body depth로 들어가지 않음
- nested `CASE`의 body depth와 close `)` depth가 같은 기준으로 섞여 계산됨

이번 수정 후에는 위 기대값이 구조 규칙과 맞지 않아서, 테스트를 새 규칙 기준으로 바로잡았다.

## 예시 1. JOIN ON double wrapper

입력:

```sql
SELECT *
FROM a
JOIN b
ON ((1 = 1
AND 2 = 2))
AND 3 = 3;
```

이전 기대:

```sql
SELECT *
FROM a
JOIN b
    ON ((1 = 1
            AND 2 = 2))
        AND 3 = 3;
```

현재 기대:

```sql
SELECT *
FROM a
JOIN b
    ON ((1 = 1
                AND 2 = 2))
        AND 3 = 3;
```

설명:

- `ON` continuation: `+1`
- `((` wrapper: `+2`
- 따라서 inner `AND`는 `ON` 기준 총 `+3` depth가 된다.

## 예시 2. OPEN FOR + inline comment + CASE

입력:

```sql
BEGIN
OPEN p_rc FOR
SELECT ( -- inline comment
CASE
WHEN score > 10 THEN 'HIGH'
ELSE 'LOW'
END
) AS bucket
FROM dual;
END;
```

이전 기대:

```sql
BEGIN
    OPEN p_rc FOR
        SELECT ( -- inline comment
            CASE
                WHEN score > 10 THEN 'HIGH'
                ELSE 'LOW'
            END
        ) AS bucket
        FROM DUAL;
END;
```

현재 기대:

```sql
BEGIN
    OPEN p_rc FOR
        SELECT ( -- inline comment
                CASE
                    WHEN score > 10 THEN 'HIGH'
                    ELSE 'LOW'
                END
            ) AS bucket
        FROM DUAL;
END;
```

설명:

- `SELECT (` 다음 `CASE`는 괄호 body이므로 wrapper line보다 `+1`
- close `)`는 wrapper line depth로 복귀

## 예시 3. nested CASE inside parens

입력:

```sql
BEGIN
OPEN p_rc FOR
SELECT (
CASE
WHEN score > 10 THEN (
CASE -- nested expression
WHEN score > 20 THEN 'HIGH+'
ELSE 'HIGH'
END
)
ELSE 'LOW'
END
) AS bucket
FROM dual;
END;
```

현재 기대:

```sql
BEGIN
    OPEN p_rc FOR
        SELECT
            (
                CASE
                    WHEN score > 10 THEN
                        (
                            CASE -- nested expression
                                WHEN score > 20 THEN 'HIGH+'
                                ELSE 'HIGH'
                            END
                        )
                    ELSE 'LOW'
                END
            ) AS bucket
        FROM DUAL;
END;
```

설명:

- outer `CASE`는 outer `(` 보다 `+1`
- inner `CASE`는 inner `(` 보다 `+1`
- inner `)`는 surrounding branch depth로 복귀
- outer `)`는 outer wrapper depth로 복귀

## 예시 4. JOIN ON group close 뒤 OR

입력:

```sql
SELECT *
FROM emp e
JOIN dept d
ON (e.dept_id = d.dept_id
AND e.loc_id = d.loc_id)
OR (e.alt_dept_id = d.dept_id
AND e.alt_loc_id = d.loc_id)
JOIN region r
ON r.region_id = d.region_id;
```

이전 기대:

```sql
SELECT *
FROM emp e
JOIN dept d
    ON (e.dept_id = d.dept_id
            AND e.loc_id = d.loc_id)
    OR (e.alt_dept_id = d.dept_id
            AND e.alt_loc_id = d.loc_id)
JOIN region r
    ON r.region_id = d.region_id;
```

현재 기대:

```sql
SELECT *
FROM emp e
JOIN dept d
    ON (e.dept_id = d.dept_id
            AND e.loc_id = d.loc_id)
        OR (e.alt_dept_id = d.dept_id
            AND e.alt_loc_id = d.loc_id)
JOIN region r
    ON r.region_id = d.region_id;
```

설명:

- `OR`는 `ON` block의 다음 조건 continuation이므로 `ON + 1`
- 이전처럼 `ON`과 같은 depth로 돌아가는 기대값은 구조 규칙과 맞지 않는다

## 테스트 변경 요약

기존 테스트 기대값 수정:

- `src/ui/sql_editor/execution.rs`
- `src/ui/sql_editor/sql_editor_tests.rs`
- `src/ui/sql_editor/formatter.rs`

새 회귀 테스트 추가:

- repeated wrapper paren depth
- inner close 후 outer paren depth 유지
- standalone open paren body/close depth
- JOIN ON nested paren with inline comments

핵심은 테스트를 느슨하게 만든 것이 아니라, 실제 규칙을 공백 문자열보다 직접 검증하도록 바꾼 것이다.
