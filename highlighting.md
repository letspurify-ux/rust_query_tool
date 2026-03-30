# SQL Highlighting 동작 원리

이 문서는 현재 코드베이스 기준으로 SQL 하이라이팅이 어떻게 동작하는지 정리한 구현 문서다.
목표는 다음 2가지다.

- 실제 편집기에서 하이라이팅이 갱신되는 경로를 정확히 설명한다.
- 현재 설계가 이론적으로 타당한지 검토하고, 오해하기 쉬운 지점을 분리해서 기록한다.

## 1. 관련 소스 파일

- `src/ui/syntax_highlight.rs`
  - 토큰 분류기와 lexer state를 가진 핵심 하이라이터 구현
- `src/ui/sql_editor/highlighting.rs`
  - 편집기 증분 하이라이팅, shadow 상태, style buffer 동기화
- `src/ui/sql_editor/intellisense_host.rs`
  - 버퍼 수정 callback 연결, 컬럼 메타데이터 갱신 후 재하이라이팅
- `src/ui/main_window.rs`
  - 스키마 메타데이터를 `HighlightData`로 구성해 각 에디터 탭에 주입
- `src/ui/text_buffer_access.rs`
  - `HighlightShadowState`를 활용해 UTF-8 안전한 텍스트/라인 경계 조회

## 2. 핵심 개념

### 2.1 논리 스타일과 FLTK 스타일 버퍼는 다르다

코드 내부에서는 먼저 "논리 스타일 문자열"을 만든다.

- 길이: `text.len()`와 동일
- 기준: 문자 수가 아니라 바이트 수
- 각 바이트 위치마다 스타일 문자 하나를 둔다
- 스타일 문자는 `A`..`N` 범위의 ASCII 태그다

대표 스타일 태그는 다음과 같다.

- `A`: 기본 텍스트
- `B`: SQL keyword
- `C`: Oracle built-in function
- `D`: single quote string
- `E`: line comment 또는 한 줄 block comment
- `F`: number
- `G`: operator
- `H`: relation identifier
- `I`: hint comment
- `J`: `DATE`/`TIMESTAMP`/`INTERVAL` literal
- `K`: column
- `L`: multi-line block comment
- `M`: q-quote string
- `N`: quoted identifier

FLTK `TextBuffer`에 넣을 때는 이 논리 스타일 문자열을 그대로 쓰지 않는다.
멀티바이트 UTF-8 문자에 대해 첫 바이트만 스타일 태그를 두고, 나머지 continuation byte에는 `0`을 채운 뒤 style buffer에 기록한다.

즉, 내부 규칙은 다음 두 단계다.

1. 논리 계산: 바이트마다 스타일 태그를 만든다.
2. FLTK 인코딩: 멀티바이트 문자의 후속 바이트는 `0`으로 바꿔 넣는다.

이 구조가 필요한 이유는 FLTK `TextBuffer`가 바이트 인덱스를 기준으로 움직이기 때문이다.

### 2.2 실제 증분 하이라이팅은 line state 기반이다

실제 편집기 경로에서 중요한 상태는 `HighlightShadowState`다.

- `text`: 현재 편집기 텍스트 shadow
- `styles`: 현재 논리 스타일 shadow
- `newline_positions`: 줄 끝 바이트 위치 캐시
- `line_exit_states`: 각 줄을 스캔한 뒤 lexer가 어떤 상태로 끝났는지 저장

여기서 핵심은 `line_exit_states`다.
증분 하이라이팅은 "이전 줄이 끝날 때 문자열/블록 주석/q-quote 안에 있었는가"를 이 배열로 전파한다.
즉, 현재 구현은 "스타일만 보고 이어받는 방식"이 아니라 "줄 종료 lexer state를 이어받는 방식"이다.

## 3. 초기화와 메타데이터 주입

에디터 생성 시 `SqlEditorWidget::new`에서 다음이 준비된다.

1. 일반 텍스트용 `buffer`
2. 스타일용 `style_buffer`
3. `TextEditor::set_highlight_data(style_buffer, style_table)`
4. `SqlHighlighter`
5. `HighlightShadowState`

스키마 메타데이터는 `HighlightData`로 주입된다.

- `tables`
- `views`
- `columns`

테이블/뷰 목록은 주로 `main_window.rs`에서 스키마 로딩 시 채워진다.
컬럼 목록은 IntelliSense 컬럼 로더가 비동기로 채우고, 값이 바뀌면 `rehighlight_full_buffer()`가 다시 호출된다.

즉, relation/column 스타일은 정적인 키워드 목록만으로 결정되지 않고, 현재 연결의 메타데이터도 반영된다.

## 4. 전체 재하이라이팅 경로

전체 재하이라이팅은 `rehighlight_full_buffer()`가 담당한다.

흐름은 다음과 같다.

1. `buffer.text()`로 전체 텍스트를 읽는다.
2. `build_logical_styles_and_line_states()`가 텍스트를 줄 단위로 순회한다.
3. 각 줄마다 `SqlHighlighter::generate_styles_for_window(line_text, entry_state)`를 호출한다.
4. 줄별 스타일을 이어 붙이고, 줄 종료 state를 `line_exit_states`에 저장한다.
5. 논리 스타일을 FLTK style buffer용 raw bytes로 인코딩한다.
6. `style_buffer`를 통째로 갱신한다.
7. `HighlightShadowState`를 새 텍스트/스타일/줄 상태로 재구성한다.
8. `editor.redraw()`를 호출한다.

여기서 중요한 점은 전체 하이라이팅도 "텍스트 전체를 한 번에 스캔"하는 것이 아니라 "줄 단위 스캔 + 이전 줄 exit state 전달" 방식이라는 점이다.

## 5. 증분 재하이라이팅 경로

실제 편집 중에는 `buffer.add_modify_callback2(...)`가 동작한다.
callback은 `SqlEditorWidget::handle_buffer_highlight_update(...)`를 호출한다.

증분 갱신 순서는 다음과 같다.

1. 수정 직후 버퍼에서 실제 inserted text를 읽는다.
2. `style_buffer`에는 우선 default 스타일 placeholder를 같은 길이로 반영한다.
3. `HighlightShadowState::apply_edit(...)`로 shadow text/styles/newline cache/line state 길이를 먼저 맞춘다.
4. 재하이라이팅 시작 위치는 `incremental_rehighlight_start(...)`로 계산한다.
   - 현재 구현은 항상 "수정된 위치가 속한 현재 줄의 시작"이다.
5. 강제로 덮어야 하는 최소 범위는 `incremental_direct_rehighlight_end(...)`로 계산한다.
   - 수정된 span이 끝나는 줄의 끝까지는 무조건 본다.
6. 그 줄부터 앞으로 진행하면서 줄 단위로 다시 lexing한다.
   - 시작 줄 entry state는 이전 줄의 `line_exit_states`에서 가져온다.
   - 각 줄의 새 스타일과 이전 shadow 스타일을 비교한다.
   - 바뀐 줄만 shadow에 덮어쓴다.
   - 새 exit state도 shadow에 기록한다.
7. 어느 시점에서 다음 조건이 모두 만족되면 중단한다.
   - 최소 강제 범위를 넘겼다.
   - 현재 줄 스타일이 안 바뀌었다.
   - 현재 줄 exit state도 안 바뀌었다.
8. 실제로 바뀐 최소 구간만 style buffer에 다시 써 넣는다.

즉, 현재 증분 하이라이팅의 핵심 정지 조건은 "스타일과 lexer exit state가 안정화되었는가"다.
이 설계 덕분에 멀티라인 문자열이나 블록 주석이 한 줄 이상 뒤로 전파되는 경우도 필요한 만큼만 더 스캔할 수 있다.

## 6. 토큰 분류 규칙

`SqlHighlighter::generate_styles_with_state(...)`의 주된 분류 순서는 다음과 같다.

1. 이전 줄에서 넘어온 continuation state 처리
   - block comment
   - hint comment
   - single quote string
   - q-quote string
   - quoted identifier
2. 줄 시작 special command 처리
   - `PROMPT`: 해당 줄 전체를 comment 스타일
   - `CONNECT`: SQL*Plus `CONNECT` 문맥이면 keyword만 스타일링하고 그 줄 나머지는 일반 SQL lexing 대상에서 제외
3. line comment `--`
4. block comment `/* ... */`
   - `/*+`는 hint
   - 여러 줄에 걸친 일반 block comment는 내부적으로 `STYLE_BLOCK_COMMENT`를 사용
5. q-quote / prefixed single quote literal
6. 일반 single quote string
7. double quote identifier
8. number
9. identifier / keyword / function / relation / column
10. operator

### 6.1 identifier 분류는 완전한 parser가 아니라 휴리스틱이다

identifier 계열은 다음 데이터를 조합해 분류한다.

- Oracle keyword 집합
- Oracle built-in function 집합
- 현재 스키마 relation lookup
- 현재 로딩된 column lookup
- alias/member access 문맥 휴리스틱

예를 들어 다음 같은 보정이 들어간다.

- `if.a` 같은 member access에서 `if`를 keyword로 보지 않음
- `trim.a` 같은 qualified name에서 `trim`을 function으로 보지 않음
- `AS <identifier>` 뒤는 identifier 문맥으로 간주
- `DATE '...'` 같은 literal은 keyword가 아니라 datetime literal 전체로 간주

따라서 이 하이라이터는 "정적 키워드 색칠기"보다 똑똑하지만, AST 기반 parser는 아니다.

## 7. UTF-8 / 바이트 오프셋 처리 방식

현재 구현은 하이라이팅 경로에서도 AGENTS 규칙과 맞게 바이트 오프셋 기준을 일관되게 유지한다.

- 모든 range 계산은 `usize` 바이트 오프셋으로 수행
- mid-byte 위치는 `clamp_boundary()` 또는 `clamp_to_utf8_boundary()`로 뒤로 보정
- 줄 시작/끝 계산도 byte index 기반
- style 길이도 항상 `text.len()` 기준
- FLTK style buffer에는 continuation byte를 `0`으로 채워 byte length를 유지

이 설계는 특히 다음 상황에서 중요하다.

- 한글 등 멀티바이트 문자를 포함한 문자열 편집
- 삽입/삭제 위치가 UTF-8 중간 바이트에 걸릴 수 있는 callback 입력
- style buffer와 text buffer의 길이가 항상 동일해야 하는 FLTK 제약

테스트도 이 부분을 직접 확인한다.

- `encode_fltk_style_bytes_zeroes_utf8_continuations`
- `compute_incremental_start_clamps_to_utf8_boundary`
- `encoded_style_bytes_preserve_utf8_byte_length_after_multibyte_edit`

## 8. 이론 검토 결과

### 8.1 현재 실제 구현은 이론적으로 대체로 맞다

현재 실제 편집기 경로만 놓고 보면 설계는 타당하다.

- 바이트 오프셋 기준이 일관된다.
- 멀티라인 토큰의 상태를 `line_exit_states`로 보존하므로 줄 단위 증분 재하이라이팅이 성립한다.
- 증분 갱신이 실패하거나 길이가 어긋나면 전체 재하이라이팅으로 안전하게 복구한다.
- 멀티바이트 UTF-8과 FLTK style buffer의 byte-length 동기화를 별도 인코딩 단계로 분리했다.

특히 "멀티라인 토큰이 있을 때도 증분 하이라이팅이 왜 안전한가"라는 질문에 대한 답은 명확하다.

- 시작 줄 entry state를 이전 줄 exit state에서 가져오고
- 줄 끝 exit state가 바뀌면 다음 줄을 계속 다시 스캔하므로
- comment/string/q-quote가 뒤 줄로 전파되는 효과를 보존할 수 있다

즉, 현재 구현은 "라인 단위 상태 전이 시스템"으로 이해하는 것이 맞다.

### 8.2 문서화 시 반드시 구분해야 하는 점

다음 API는 이름만 보면 실제 편집기 핵심 경로처럼 보이지만, 현재 메인 경로는 아니다.

- `SqlHighlighter::generate_incremental_styles(...)`
- `SqlHighlighter::probe_entry_state_for_style_text(...)`
- `SqlHighlighter::entry_state_from_continuation_style(...)`

실제 편집기 증분 갱신은 이 API들보다 `HighlightShadowState + line_exit_states + apply_main_thread_incremental_highlighting()` 경로를 사용한다.

문서에서 이 구분을 빼면 "style char만 보고 다음 window entry state를 복원한다"는 식으로 잘못 이해할 가능성이 있다.

### 8.3 당장 수정이 꼭 필요해 보이는 치명적 오류는 보이지 않는다

현재 확인한 테스트 범위에서는 하이라이팅 설계와 구현이 서로 맞물려 있다.

- `cargo test syntax_highlight --lib`
- `cargo test incremental_highlighting --lib`
- `cargo test encode_fltk_style_bytes_zeroes_utf8_continuations --lib`
- `cargo test compute_incremental_start --lib`

위 범위는 모두 통과했다.

따라서 "현재 구현 설명을 문서화한다"는 목적 기준으로는 소스 수정이 먼저 필요한 상황으로 보이지 않는다.

## 9. 다만 남아 있는 설계상 주의점

### 9.1 q-quote는 style 문자만으로 continuation state를 복원할 수 없다

`LexerState::InQQuote`는 `closing` 문자와 `depth`를 함께 가진다.
반면 style 문자 `M`만으로는 이 정보를 복원할 수 없다.

그래서 `entry_state_from_continuation_style(STYLE_Q_QUOTE_STRING)`는 `Normal`을 반환한다.
이건 현재 편집기 경로에서는 문제가 아니다.
실제 경로는 style 문자에서 상태를 역산하지 않고 `line_exit_states`를 별도로 들고 있기 때문이다.

하지만 향후 누군가 style buffer만으로 증분 재시작을 하려 하면 이 부분은 이론적으로 부족하다.

### 9.2 현재 하이라이터는 parser가 아니라 고급 휴리스틱 lexer다

alias, member access, relation context를 많이 보정하고 있지만, 완전한 SQL AST를 만든 뒤 의미 해석하는 구조는 아니다.
따라서 다음 같은 경계는 남는다.

- 메타데이터가 아직 로딩되지 않은 identifier
- built-in function 이름과 충돌하는 암시적 alias
- 문맥상 identifier이지만 lexer 수준 휴리스틱으로만 구분해야 하는 케이스

지금 코드가 틀렸다는 뜻은 아니다.
다만 문서에는 "문법 의미를 완전하게 증명하는 parser"가 아니라는 점을 분명히 적는 편이 맞다.

### 9.3 미래에 멀티라인 상태 종류가 늘어나면 `line_exit_states`도 같이 확장돼야 한다

현재는 다음 상태만 줄 사이에 전파하면 충분하다.

- block comment
- hint
- single quote string
- q-quote string
- quoted identifier

만약 앞으로 다른 multi-line lexical construct를 추가하면, 증분 하이라이팅의 정합성은 `LexerState`와 `line_exit_states`에 그 상태를 함께 반영하는지에 달려 있다.
즉, 이 설계의 핵심 불변식은 "다음 줄 해석에 필요한 lexical state는 반드시 line exit state에 보존된다"이다.

## 10. 결론

현재 하이라이팅 기능은 다음처럼 이해하는 것이 가장 정확하다.

- 기본 엔진은 `SqlHighlighter`가 담당한다.
- 실제 편집기 증분 갱신은 `HighlightShadowState`가 담당한다.
- 안전성의 핵심은 바이트 오프셋 일관성과 `line_exit_states` 전파다.
- relation/column 색상은 스키마/IntelliSense 메타데이터에 의해 동적으로 바뀐다.

정리하면, 현재 구현은 이론적으로 대체로 맞고, 즉시 소스를 수정해야 할 명백한 오류는 이번 검토 범위에서는 보이지 않았다.
대신 문서화할 때는 "보조 API"와 "실제 편집기 경로"를 섞어 설명하지 않는 것이 가장 중요하다.
