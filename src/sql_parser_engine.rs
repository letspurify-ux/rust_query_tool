use crate::sql_text;

#[derive(Default)]
pub(crate) struct SplitState {
    pub(crate) in_single_quote: bool,
    pub(crate) in_double_quote: bool,
    pub(crate) in_line_comment: bool,
    pub(crate) in_block_comment: bool,
    pub(crate) in_q_quote: bool,
    pub(crate) q_quote_end: Option<char>,
    pub(crate) in_dollar_quote: bool,
    pub(crate) dollar_quote_tag: String,
    pub(crate) block_depth: usize,
    pub(crate) pending_end: bool,
    pub(crate) token: String,
    pub(crate) in_create_plsql: bool,
    pub(crate) create_pending: bool,
    create_or_seen: bool,
    pub(crate) after_declare: bool, // Track if we're inside DECLARE block waiting for BEGIN
    after_as_is: bool, // Track if we've seen AS/IS in CREATE PL/SQL (for BEGIN handling)
    nested_subprogram: bool, // Track nested PROCEDURE/FUNCTION inside DECLARE block
    /// Count of nested subprogram declarations awaiting their BEGIN.
    /// In package bodies, nested PROCEDURE/FUNCTION IS increments this,
    /// and BEGIN decrements it. This allows the outer procedure's BEGIN
    /// to still be recognized even after nested procedure's END.
    pub(crate) pending_subprogram_begins: usize,
    /// Tracks CREATE routine headers opened by AS/IS while waiting for a BEGIN.
    /// Each tuple is (header_depth, split_on_semicolon).
    routine_is_stack: Vec<(usize, bool)>,
    /// True when we're creating a PACKAGE (spec or body), not PROCEDURE/FUNCTION/TRIGGER/TYPE
    /// Packages don't have a BEGIN at the AS level, only their contained procedures do.
    is_package: bool,
    /// Stack recording the block_depth at which each CASE keyword was opened.
    /// Used to distinguish CASE expression END (plain END at same block_depth)
    /// from nested block END (plain END at deeper block_depth) inside a CASE statement.
    /// CASE expressions end with plain END; PL/SQL CASE statements end with END CASE.
    pub(crate) case_depth_stack: Vec<usize>,
    /// Parenthesis nesting depth. Tracked by the execution-layer parser so that
    /// formatting and intellisense can derive their own depth from this base
    /// without duplicating quote/comment-aware character scanning.
    pub(crate) paren_depth: usize,
    /// True when we're creating a TRIGGER (not PROCEDURE/FUNCTION/PACKAGE/TYPE).
    /// TRIGGER headers can contain INSERT/UPDATE/DELETE/SELECT keywords as event types
    /// before block_depth increases, so we must not force-terminate on those keywords.
    pub(crate) is_trigger: bool,
    /// True when we're inside a COMPOUND TRIGGER definition.
    /// COMPOUND TRIGGERs have timing points like BEFORE STATEMENT IS...END BEFORE STATEMENT;
    in_compound_trigger: bool,
    /// True when we've seen BEFORE or AFTER in a COMPOUND TRIGGER context,
    /// waiting for IS to start the timing point block.
    pending_timing_point_is: bool,
    /// True when we've just seen TYPE in CREATE context, waiting to check for BODY.
    /// TYPE BODY should be treated like PACKAGE BODY (is_package = true).
    after_type: bool,
    /// True when parsing a CREATE TYPE statement (not TYPE BODY).
    /// Restricts TYPE ... AS/IS OBJECT|VARRAY|TABLE handling to real type DDL.
    is_type_create: bool,
    /// Reusable buffer for uppercase token comparisons.
    /// Avoids a heap allocation on every `flush_token` call.
    token_upper_buf: String,
    /// True after WHILE, waiting for a MySQL-style DO token.
    /// Used for `WHILE condition DO ... END WHILE;` where LOOP does not appear.
    pending_while_do: bool,
    /// True after IF, waiting for a control-flow THEN token.
    /// This prevents `IF(expr, ...)` scalar functions from being misclassified
    /// as block openers in non-PL/SQL SQL statements.
    pending_if_then: bool,
}

impl SplitState {
    pub(crate) fn is_idle(&self) -> bool {
        !self.in_single_quote
            && !self.in_double_quote
            && !self.in_block_comment
            && !self.in_q_quote
            && !self.in_dollar_quote
            && !self.in_line_comment
    }

    pub(crate) fn flush_token(&mut self) {
        if self.token.is_empty() {
            return;
        }
        // Reuse the pre-allocated buffer to avoid a new heap allocation per token.
        // mem::take leaves token_upper_buf as an empty String so &mut self borrows
        // inside this function remain valid.
        let mut upper_buf = std::mem::take(&mut self.token_upper_buf);
        upper_buf.clear();
        upper_buf.push_str(&self.token);
        upper_buf.make_ascii_uppercase();
        let upper = upper_buf.as_str();

        if matches!(upper, "EXTERNAL" | "LANGUAGE" | "NAME" | "LIBRARY")
            && self
                .routine_is_stack
                .last()
                .is_some_and(|(depth, _)| *depth == self.block_depth)
        {
            if let Some((_, split_on_semicolon)) = self.routine_is_stack.last_mut() {
                *split_on_semicolon = true;
            }
        }

        self.track_create_plsql(upper);

        // Check if this is "END CASE" / "END IF" / "END LOOP" before processing pending_end
        let is_end_case = self.pending_end && upper == "CASE";
        let is_end_if = self.pending_end && upper == "IF";
        let is_end_loop = self.pending_end && upper == "LOOP";
        let is_end_while = self.pending_end && upper == "WHILE";
        let is_end_repeat = self.pending_end && upper == "REPEAT";

        if self.pending_end {
            if upper == "CASE" {
                // END CASE - PL/SQL CASE statement 종료
                // stack에서 해당 CASE를 제거하고 depth 감소
                self.case_depth_stack.pop();
                if self.block_depth > 0 {
                    self.block_depth -= 1;
                }
            } else if upper == "IF" {
                // END IF
                if self.block_depth > 0 {
                    self.block_depth -= 1;
                }
            } else if upper == "LOOP" {
                // END LOOP
                if self.block_depth > 0 {
                    self.block_depth -= 1;
                }
            } else if upper == "WHILE" {
                // END WHILE
                if self.block_depth > 0 {
                    self.block_depth -= 1;
                }
            } else if upper == "REPEAT" {
                // END REPEAT
                if self.block_depth > 0 {
                    self.block_depth -= 1;
                }
            } else if matches!(upper, "BEFORE" | "AFTER" | "INSTEAD") && self.in_compound_trigger {
                // END BEFORE ..., END AFTER ..., END INSTEAD ... - COMPOUND TRIGGER timing point 종료
                // depth 감소 (타이밍 포인트 블록 종료)
                if self.block_depth > 0 {
                    self.block_depth -= 1;
                }
            } else {
                // 일반 END - CASE expression END 또는 PL/SQL block END
                // stack.last()가 현재 block_depth와 같으면 CASE expression의 END;
                // 아니면 (더 깊은 block_depth) PL/SQL 블록의 END
                if self
                    .case_depth_stack
                    .last()
                    .is_some_and(|depth| *depth + 1 == self.block_depth)
                {
                    self.case_depth_stack.pop();
                    if self.block_depth > 0 {
                        self.block_depth -= 1;
                    }
                } else if self.block_depth > 0 {
                    self.block_depth -= 1;
                }
            }
            self.pending_end = false;
        }

        // CASE 키워드 발견 시 현재 block_depth를 stack에 push
        // END CASE의 CASE는 제외 (is_end_case로 체크)
        if upper == "CASE" && !is_end_case {
            self.case_depth_stack.push(self.block_depth);
            self.block_depth += 1;
        }

        if upper == "IF" && !is_end_if {
            self.pending_if_then = true;
        }

        if upper == "THEN" && self.pending_if_then {
            self.block_depth += 1;
            self.pending_if_then = false;
        }

        if upper == "LOOP" && !is_end_loop {
            self.block_depth += 1;
            self.pending_while_do = false;
        }

        if upper == "REPEAT" && !is_end_repeat {
            self.block_depth += 1;
        }

        if upper == "WHILE" && !self.pending_end && !is_end_while {
            self.pending_while_do = true;
        } else if self.pending_while_do && upper == "DO" {
            self.block_depth += 1;
            self.pending_while_do = false;
        }

        // Handle TYPE declarations that don't create a block:
        // TYPE ... AS OBJECT/VARRAY/TABLE - these are type definitions, not blocks
        // TYPE ... IS REF CURSOR - this is a REF CURSOR type definition in package spec
        if self.after_as_is && matches!(upper, "OBJECT" | "VARRAY" | "TABLE" | "REF" | "RECORD") {
            if self.block_depth > 0 {
                self.block_depth -= 1;
            } else {
                eprintln!(
                    "Warning: encountered TYPE body terminator while block depth was already zero."
                );
            }
            self.after_as_is = false;
        }

        // Track nested PROCEDURE/FUNCTION inside DECLARE blocks (anonymous blocks)
        // These need IS to start their body block
        // Only track when NOT in CREATE PL/SQL (packages already handle nested subprograms via in_create_plsql)
        if self.block_depth > 0 && matches!(upper, "PROCEDURE" | "FUNCTION") {
            self.nested_subprogram = true;
        }

        // For CREATE PL/SQL (PACKAGE, PROCEDURE, FUNCTION, TYPE, TRIGGER),
        // AS or IS starts the body/specification block
        // For nested procedures/functions inside DECLARE blocks (anonymous blocks),
        // IS also increments block_depth
        //
        // IMPORTANT: Distinguish between:
        // - "name IS" (starts a block): nested_subprogram=true, or first AS/IS in CREATE
        // - "value IS NULL" (expression): just a comparison, don't start block
        //
        // We use nested_subprogram to track when IS should start a block.
        // For the first AS/IS in CREATE, we use block_depth==0 as indicator.
        // For COMPOUND TRIGGER timing points, pending_timing_point_is indicates IS starts a block.
        let is_block_starting_as_is = matches!(upper, "AS" | "IS")
            && (self.pending_timing_point_is
                || self.nested_subprogram
                || (self.in_create_plsql && self.block_depth == 0));

        if is_block_starting_as_is {
            self.block_depth += 1;
            let split_on_semicolon = false;
            // Only set after_as_is for TYPE declarations (CREATE TYPE or TYPE inside package)
            // Don't set for package AS (which doesn't need REF/OBJECT/etc handling)
            // Don't set for procedure/function IS (which has BEGIN instead)
            // We leave after_as_is = false for packages to avoid incorrect depth decrements
            // when encountering REF CURSOR type declarations inside the package
            // Don't set for COMPOUND TRIGGER timing points either
            if self.is_type_create && !self.nested_subprogram && !self.pending_timing_point_is {
                // This might be CREATE TYPE ... AS/IS OBJECT/VARRAY/etc
                self.after_as_is = true;
            }
            self.nested_subprogram = false; // Reset after seeing IS
            self.pending_timing_point_is = false; // Reset after seeing IS in COMPOUND TRIGGER
                                                  // Track that we're waiting for a BEGIN for this subprogram
                                                  // Use counter to handle nested PROCEDURE/FUNCTION declarations
                                                  // For packages: depth=1 is the package AS level (no BEGIN expected)
                                                  //              depth>1 means we're inside a procedure/function that expects BEGIN
                                                  // For procedures/functions: any depth needs BEGIN tracking
                                                  // For COMPOUND TRIGGER timing points: always need BEGIN tracking
            let needs_begin_tracking = if self.is_package {
                self.block_depth > 1 // Inside package, nested proc/func
            } else {
                true // Standalone procedure/function/trigger or COMPOUND TRIGGER timing point
            };
            if needs_begin_tracking {
                self.routine_is_stack
                    .push((self.block_depth, split_on_semicolon));
                self.pending_subprogram_begins += 1;
            }
        } else if upper == "DECLARE" {
            // Standalone DECLARE block
            self.block_depth += 1;
            self.after_declare = true;
        } else if upper == "BEGIN" {
            if self.after_declare {
                // DECLARE ... BEGIN - same block, don't increase depth
                self.after_declare = false;
            } else if self.pending_subprogram_begins > 0 {
                // AS/IS ... BEGIN - same block for CREATE PL/SQL, don't increase depth
                // Decrement the pending counter - this BEGIN matches one of the pending subprograms
                if self
                    .routine_is_stack
                    .last()
                    .is_some_and(|(depth, _)| *depth == self.block_depth)
                {
                    let _ = self.routine_is_stack.pop();
                }
                self.pending_subprogram_begins -= 1;
            } else {
                // Standalone BEGIN block
                self.block_depth += 1;
            }
        } else if upper == "END" {
            // Set pending_end and determine in next token whether this is:
            // - END CASE (PL/SQL CASE statement)
            // - END IF / END LOOP
            // - END BEFORE / END AFTER / END INSTEAD (COMPOUND TRIGGER timing point)
            // - END (CASE expression or PL/SQL block)
            self.pending_end = true;
        } else if upper == "COMPOUND" && self.in_create_plsql {
            // COMPOUND TRIGGER - set flag to track timing points.
            // block_depth를 1 증가시켜 COMPOUND TRIGGER 본문의 외부 블록을 추적한다.
            // 타이밍 포인트(BEFORE/AFTER ... IS)는 depth+1에서 열리고, END <timing> 시 depth로 돌아오며,
            // 최종 END trigger_name이 depth 1→0으로 내려서 문장을 종료한다.
            self.in_compound_trigger = true;
            self.block_depth += 1;
        } else if matches!(upper, "BEFORE" | "AFTER" | "INSTEAD") && self.in_compound_trigger {
            // BEFORE/AFTER/INSTEAD in COMPOUND TRIGGER context - prepare for timing point IS
            self.pending_timing_point_is = true;
        }

        // Return the uppercase buffer so its capacity is reused on the next call.
        let _ = upper;
        self.token_upper_buf = upper_buf;
        self.token.clear();
    }

    fn resolve_pending_end(&mut self) {
        if self
            .case_depth_stack
            .last()
            .is_some_and(|depth| *depth + 1 == self.block_depth)
        {
            // CASE expression 종료 (stack.last() == block_depth)
            self.case_depth_stack.pop();
            self.block_depth = self.block_depth.saturating_sub(1);
        } else if self.block_depth > 0 {
            // PL/SQL block 종료
            self.block_depth -= 1;
        }
    }

    pub(crate) fn resolve_pending_end_on_separator(&mut self) {
        if self.pending_end {
            // END followed by a non-keyword separator - determine what it closes.
            // '-' and '/' are treated as separators only when they are not comment starters,
            // so continuation forms like END /*c*/ IF and END --c ... IF still work.
            self.resolve_pending_end();
            self.pending_end = false;
        }
    }

    pub(crate) fn resolve_pending_end_on_terminator(&mut self) {
        if self.pending_end {
            // END followed by terminator (;) - determine what it closes
            self.resolve_pending_end();
            // Reset create state when we reach depth 0 (end of CREATE statement)
            if self.block_depth == 0 {
                self.reset_create_state();
            }
            self.pending_end = false;
        }
    }

    pub(crate) fn should_split_on_semicolon(&self) -> bool {
        self.routine_is_stack
            .last()
            .is_some_and(|(depth, split_on_semicolon)| {
                *depth == self.block_depth && *split_on_semicolon
            })
    }

    pub(crate) fn resolve_pending_end_on_eof(&mut self) {
        if self.pending_end {
            // END at EOF - determine what it closes
            self.resolve_pending_end();
            // Reset create state when we reach depth 0 (end of CREATE statement)
            if self.block_depth == 0 {
                self.reset_create_state();
            }
            self.pending_end = false;
        }
    }

    pub(crate) fn reset_create_state(&mut self) {
        self.in_create_plsql = false;
        self.create_pending = false;
        self.create_or_seen = false;
        self.after_as_is = false;
        self.nested_subprogram = false;
        self.pending_subprogram_begins = 0;
        self.routine_is_stack.clear();
        self.is_package = false;
        self.is_trigger = false;
        self.in_compound_trigger = false;
        self.pending_timing_point_is = false;
        self.after_type = false;
        self.is_type_create = false;
        self.pending_while_do = false;
        self.pending_if_then = false;
    }

    fn track_create_plsql(&mut self, upper: &str) {
        // Check for BODY after TYPE - TYPE BODY should be treated like PACKAGE BODY
        if self.in_create_plsql && self.after_type && upper == "BODY" {
            self.is_package = true;
            self.after_type = false;
            return;
        }

        // Reset after_type if we see any other token
        if self.after_type && upper != "BODY" {
            self.after_type = false;
        }

        if self.in_create_plsql {
            return;
        }

        if self.create_pending {
            match upper {
                "OR" => {
                    self.create_or_seen = true;
                    return;
                }
                "NO" => {
                    return;
                }
                "FORCE" => {
                    return;
                }
                "REPLACE" => {
                    return;
                }
                "EDITIONABLE" | "NONEDITIONABLE" => {
                    return;
                }
                "PROCEDURE" | "FUNCTION" | "PACKAGE" | "TYPE" | "TRIGGER" => {
                    self.in_create_plsql = true;
                    self.is_package = upper == "PACKAGE";
                    self.is_trigger = upper == "TRIGGER";
                    self.is_type_create = upper == "TYPE";
                    // Track when we just saw TYPE to detect TYPE BODY
                    self.after_type = upper == "TYPE";
                    self.create_pending = false;
                    self.create_or_seen = false;
                    return;
                }
                _ => {
                    self.create_pending = false;
                    self.create_or_seen = false;
                }
            }
        }

        if upper == "CREATE" {
            self.create_pending = true;
            self.create_or_seen = false;
        }
    }

    pub(crate) fn start_q_quote(&mut self, delimiter: char) {
        self.in_q_quote = true;
        self.q_quote_end = Some(sql_text::q_quote_closing(delimiter));
    }

    pub(crate) fn q_quote_end(&self) -> Option<char> {
        self.q_quote_end
    }
}

#[inline]
fn is_dollar_quote_tag_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn parse_dollar_quote_tag(chars: &[char], start: usize) -> Option<String> {
    if chars.get(start).copied() != Some('$') {
        return None;
    }

    let mut i = start + 1;
    while let Some(ch) = chars.get(i).copied() {
        if ch == '$' {
            return Some(chars[start..=i].iter().collect());
        }
        if !is_dollar_quote_tag_char(ch) {
            return None;
        }
        i += 1;
    }

    None
}

fn chars_starts_with(chars: &[char], start: usize, pattern: &str) -> bool {
    let mut idx = start;
    for pattern_ch in pattern.chars() {
        if chars.get(idx).copied() != Some(pattern_ch) {
            return false;
        }
        idx += 1;
    }
    true
}

pub(crate) struct SqlParserEngine {
    pub(crate) state: SplitState,
    current: String,
    statements: Vec<String>,
}

impl SqlParserEngine {
    pub(crate) fn new() -> Self {
        Self {
            state: SplitState::default(),
            current: String::new(),
            statements: Vec::new(),
        }
    }

    pub(crate) fn is_idle(&self) -> bool {
        self.state.is_idle()
    }

    pub(crate) fn current_is_empty(&self) -> bool {
        self.current.trim().is_empty()
    }

    pub(crate) fn in_create_plsql(&self) -> bool {
        self.state.in_create_plsql
    }

    pub(crate) fn block_depth(&self) -> usize {
        self.state.block_depth
    }

    pub(crate) fn is_trigger(&self) -> bool {
        self.state.is_trigger
    }

    pub(crate) fn starts_with_alter_session(&self) -> bool {
        let mut remaining = self.current.as_str();

        loop {
            let trimmed = remaining.trim_start();
            if trimmed.is_empty() {
                return false;
            }

            if trimmed.starts_with("/*") {
                let Some(block_end) = trimmed.find("*/") else {
                    return false;
                };
                remaining = &trimmed[block_end + 2..];
                continue;
            }

            if trimmed.starts_with("--") || sql_text::is_sqlplus_remark_comment_line(trimmed) {
                let Some(line_end) = trimmed.find('\n') else {
                    return false;
                };
                remaining = &trimmed[line_end + 1..];
                continue;
            }

            let mut words = trimmed.split_whitespace();
            return matches!(
                (words.next(), words.next()),
                (Some(first), Some(second))
                    if first.eq_ignore_ascii_case("ALTER")
                        && second.eq_ignore_ascii_case("SESSION")
            );
        }
    }

    pub(crate) fn process_text(&mut self, text: &str) {
        self.process_text_with_observer(text, |_, _, _, _| {});
    }

    pub(crate) fn process_line(&mut self, line: &str) {
        let mut line_with_newline = String::from(line);
        line_with_newline.push('\n');
        self.process_text(&line_with_newline);
    }

    pub(crate) fn process_text_with_observer<F>(&mut self, text: &str, mut on_symbol: F)
    where
        F: FnMut(&[char], usize, char, Option<char>),
    {
        let chars: Vec<char> = text.chars().collect();
        let len = chars.len();
        let mut i = 0usize;

        while i < len {
            let c = chars[i];
            let next = if i + 1 < len {
                Some(chars[i + 1])
            } else {
                None
            };
            let next2 = if i + 2 < len {
                Some(chars[i + 2])
            } else {
                None
            };

            if self.state.in_line_comment {
                self.current.push(c);
                if c == '\n' {
                    self.state.in_line_comment = false;
                }
                i += 1;
                continue;
            }

            if self.state.in_block_comment {
                self.current.push(c);
                if c == '*' && next == Some('/') {
                    self.current.push('/');
                    self.state.in_block_comment = false;
                    i += 2;
                    continue;
                }
                i += 1;
                continue;
            }

            if self.state.in_q_quote {
                self.current.push(c);
                if Some(c) == self.state.q_quote_end() && next == Some('\'') {
                    self.current.push('\'');
                    self.state.in_q_quote = false;
                    self.state.q_quote_end = None;
                    i += 2;
                    continue;
                }
                i += 1;
                continue;
            }

            if self.state.in_dollar_quote {
                if c == '$' && chars_starts_with(&chars, i, &self.state.dollar_quote_tag) {
                    let tag_len = self.state.dollar_quote_tag.len();
                    for quote_ch in self.state.dollar_quote_tag.chars() {
                        self.current.push(quote_ch);
                    }
                    self.state.in_dollar_quote = false;
                    self.state.dollar_quote_tag.clear();
                    i += tag_len;
                    continue;
                }
                self.current.push(c);
                i += 1;
                continue;
            }

            if self.state.in_single_quote {
                self.current.push(c);
                if c == '\'' {
                    if next == Some('\'') {
                        self.current.push('\'');
                        i += 2;
                        continue;
                    }
                    self.state.in_single_quote = false;
                }
                i += 1;
                continue;
            }

            if self.state.in_double_quote {
                self.current.push(c);
                if c == '"' {
                    if next == Some('"') {
                        self.current.push('"');
                        i += 2;
                        continue;
                    }
                    self.state.in_double_quote = false;
                }
                i += 1;
                continue;
            }

            if c == '-' && next == Some('-') {
                self.state.flush_token();
                self.state.in_line_comment = true;
                self.current.push('-');
                self.current.push('-');
                i += 2;
                continue;
            }

            if c == '/' && next == Some('*') {
                self.state.flush_token();
                self.state.in_block_comment = true;
                self.current.push('/');
                self.current.push('*');
                i += 2;
                continue;
            }

            // Handle nq'[...]' (National Character q-quoted strings)
            if self.state.token.is_empty()
                && (c == 'n' || c == 'N')
                && (next == Some('q') || next == Some('Q'))
                && i + 2 < len
                && chars[i + 2] == '\''
            {
                if let Some(&delimiter) = chars.get(i + 3) {
                    self.state.flush_token();
                    self.state.start_q_quote(delimiter);
                    self.current.push(c);
                    self.current.push(chars[i + 1]);
                    self.current.push('\'');
                    self.current.push(delimiter);
                    i += 4;
                    continue;
                }
            }

            // Handle q'[...]' (q-quoted strings)
            if self.state.token.is_empty() && (c == 'q' || c == 'Q') && next == Some('\'') {
                if let Some(delimiter) = next2 {
                    self.state.flush_token();
                    self.state.start_q_quote(delimiter);
                    self.current.push(c);
                    self.current.push('\'');
                    self.current.push(delimiter);
                    i += 3;
                    continue;
                }
            }

            if self.state.token.is_empty() && c == '$' {
                if let Some(tag) = parse_dollar_quote_tag(&chars, i) {
                    let tag_len = tag.len();
                    self.state.flush_token();
                    self.state.in_dollar_quote = true;
                    self.state.dollar_quote_tag = tag;
                    for quote_ch in self.state.dollar_quote_tag.chars() {
                        self.current.push(quote_ch);
                    }
                    i += tag_len;
                    continue;
                }
            }

            if c == '\'' {
                self.state.flush_token();
                self.state.in_single_quote = true;
                self.current.push(c);
                i += 1;
                continue;
            }

            if c == '"' {
                self.state.flush_token();
                self.state.in_double_quote = true;
                self.current.push(c);
                i += 1;
                continue;
            }

            if sql_text::is_identifier_char(c) {
                self.state.token.push(c);
                self.current.push(c);
                i += 1;
                continue;
            }

            self.state.flush_token();
            on_symbol(&chars, i, c, next);

            // Track parenthesis depth at the execution layer so that
            // formatting/intellisense can build on this base.
            if c == '(' {
                self.state.paren_depth += 1;
            } else if c == ')' {
                self.state.paren_depth = self.state.paren_depth.saturating_sub(1);
            }

            if self.state.pending_end {
                let separator = matches!(
                    c,
                    ',' | ')' | ']' | '}' | '+' | '*' | '%' | '=' | '<' | '>' | '|'
                ) || (c == '-' && next != Some('-'))
                    || (c == '/' && next != Some('*'));
                if separator {
                    self.state.resolve_pending_end_on_separator();
                }
            }

            if c == ';' {
                self.state.resolve_pending_end_on_terminator();
                if self.state.block_depth == 0 {
                    let trimmed = self.current.trim();
                    if !trimmed.is_empty() {
                        self.statements.push(trimmed.to_string());
                    }
                    self.current.clear();
                    // "END name;" 패턴에서 pending_end는 flush_token 내부에서 이미 해제되어
                    // resolve_pending_end_on_terminator가 reset을 호출하지 못한다.
                    // 여기서 명시적으로 초기화하여 다음 문장 파싱이 깨끗하게 시작된다.
                    self.state.reset_create_state();
                } else if self.state.should_split_on_semicolon() {
                    self.state.reset_create_state();
                    self.state.block_depth = 0;
                    self.state.case_depth_stack.clear();
                    let trimmed = self.current.trim();
                    if !trimmed.is_empty() {
                        self.statements.push(trimmed.to_string());
                    }
                    self.current.clear();
                } else {
                    self.current.push(c);
                }
                i += 1;
                continue;
            }

            self.current.push(c);
            i += 1;
        }
    }

    pub(crate) fn process_line_with_observer<F>(&mut self, line: &str, on_symbol: F)
    where
        F: FnMut(&[char], usize, char, Option<char>),
    {
        let mut line_with_newline = String::from(line);
        line_with_newline.push('\n');
        self.process_text_with_observer(&line_with_newline, on_symbol);
    }

    pub(crate) fn force_terminate(&mut self) {
        self.state.flush_token();
        self.state.resolve_pending_end_on_eof();
        self.state.reset_create_state();
        self.state.in_single_quote = false;
        self.state.in_double_quote = false;
        self.state.in_line_comment = false;
        self.state.in_block_comment = false;
        self.state.in_q_quote = false;
        self.state.q_quote_end = None;
        self.state.in_dollar_quote = false;
        self.state.dollar_quote_tag.clear();
        self.state.pending_end = false;
        self.state.token.clear();
        self.state.block_depth = 0;
        self.state.paren_depth = 0;
        self.state.case_depth_stack.clear();
        let trimmed = self.current.trim();
        if !trimmed.is_empty() {
            self.statements.push(trimmed.to_string());
        }
        self.current.clear();
    }

    pub(crate) fn finalize(&mut self) {
        self.state.flush_token();
        self.state.resolve_pending_end_on_eof();
        self.state.reset_create_state();
        let trimmed = self.current.trim();
        if !trimmed.is_empty() {
            self.statements.push(trimmed.to_string());
        }
        self.current.clear();
    }

    pub(crate) fn take_statements(&mut self) -> Vec<String> {
        std::mem::take(&mut self.statements)
    }

    pub(crate) fn force_terminate_and_take_statements(&mut self) -> Vec<String> {
        self.force_terminate();
        self.take_statements()
    }

    pub(crate) fn finalize_and_take_statements(&mut self) -> Vec<String> {
        self.finalize();
        self.take_statements()
    }

    pub(crate) fn process_line_and_take_statements(&mut self, line: &str) -> Vec<String> {
        self.process_line(line);
        self.take_statements()
    }
}
