use crate::db::session::{BindDataType, ComputeMode};
use crate::sql_text;

use super::{FormatItem, QueryExecutor, ScriptItem, ToolCommand};

#[derive(Default)]
pub(crate) struct SplitState {
    pub(crate) in_single_quote: bool,
    pub(crate) in_double_quote: bool,
    pub(crate) in_line_comment: bool,
    pub(crate) in_block_comment: bool,
    pub(crate) in_q_quote: bool,
    pub(crate) q_quote_end: Option<char>,
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
}

impl SplitState {
    pub(crate) fn is_idle(&self) -> bool {
        !self.in_single_quote
            && !self.in_double_quote
            && !self.in_block_comment
            && !self.in_q_quote
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
            self.block_depth += 1;
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

struct StatementBuilder {
    state: SplitState,
    current: String,
    statements: Vec<String>,
}

impl StatementBuilder {
    fn new() -> Self {
        Self {
            state: SplitState::default(),
            current: String::new(),
            statements: Vec::new(),
        }
    }

    fn is_idle(&self) -> bool {
        self.state.is_idle()
    }

    fn current_is_empty(&self) -> bool {
        self.current.trim().is_empty()
    }

    fn in_create_plsql(&self) -> bool {
        self.state.in_create_plsql
    }

    fn block_depth(&self) -> usize {
        self.state.block_depth
    }

    fn is_trigger(&self) -> bool {
        self.state.is_trigger
    }

    fn starts_with_alter_session(&self) -> bool {
        let cleaned = QueryExecutor::strip_leading_comments(&self.current);
        let mut tokens = cleaned.split_whitespace();
        matches!(
            (tokens.next(), tokens.next()),
            (Some(first), Some(second))
                if first.eq_ignore_ascii_case("ALTER")
                    && second.eq_ignore_ascii_case("SESSION")
        )
    }

    fn process_text(&mut self, text: &str) {
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

    fn force_terminate(&mut self) {
        self.state.flush_token();
        self.state.resolve_pending_end_on_eof();
        self.state.reset_create_state();
        self.state.in_single_quote = false;
        self.state.in_double_quote = false;
        self.state.in_line_comment = false;
        self.state.in_block_comment = false;
        self.state.in_q_quote = false;
        self.state.q_quote_end = None;
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

    fn finalize(&mut self) {
        self.state.flush_token();
        self.state.resolve_pending_end_on_eof();
        self.state.reset_create_state();
        let trimmed = self.current.trim();
        if !trimmed.is_empty() {
            self.statements.push(trimmed.to_string());
        }
        self.current.clear();
    }

    fn take_statements(&mut self) -> Vec<String> {
        std::mem::take(&mut self.statements)
    }
}

impl QueryExecutor {
    fn is_sqlplus_comment_line(line: &str) -> bool {
        let trimmed = line.trim_start();
        if trimmed.starts_with("--") {
            return true;
        }

        matches!(
            trimmed.split_whitespace().next(),
            Some(first) if first.eq_ignore_ascii_case("REM") || first.eq_ignore_ascii_case("REMARK")
        )
    }

    pub fn line_block_depths(sql: &str) -> Vec<usize> {
        fn skip_ws_and_comments(chars: &[char], mut idx: usize) -> usize {
            loop {
                while idx < chars.len() && chars[idx].is_whitespace() {
                    idx += 1;
                }

                if idx + 1 < chars.len() && chars[idx] == '/' && chars[idx + 1] == '*' {
                    idx += 2;
                    while idx + 1 < chars.len() {
                        if chars[idx] == '*' && chars[idx + 1] == '/' {
                            idx += 2;
                            break;
                        }
                        idx += 1;
                    }
                    continue;
                }

                if idx + 1 < chars.len() && chars[idx] == '-' && chars[idx + 1] == '-' {
                    idx += 2;
                    while idx < chars.len() && chars[idx] != '\n' {
                        idx += 1;
                    }
                    continue;
                }

                // SQL*Plus comment command (REM/REMARK) can appear after
                // an opening parenthesis on the same line, and the nested
                // SELECT/WITH may start on the next line.
                if idx < chars.len() && sql_text::is_identifier_char(chars[idx]) {
                    let start = idx;
                    while idx < chars.len() && sql_text::is_identifier_char(chars[idx]) {
                        idx += 1;
                    }
                    let word: String = chars[start..idx].iter().collect();
                    if word.eq_ignore_ascii_case("REM") || word.eq_ignore_ascii_case("REMARK") {
                        while idx < chars.len() && chars[idx] != '\n' {
                            idx += 1;
                        }
                        continue;
                    }
                    idx = start;
                }

                break;
            }

            idx
        }

        fn should_pre_dedent(leading_word: &str) -> bool {
            matches!(
                leading_word,
                "END" | "ELSE" | "ELSIF" | "ELSEIF" | "EXCEPTION"
            )
        }
        let is_with_main_query_keyword = |word: &str| -> bool {
            matches!(word, "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "MERGE" | "VALUES")
        };
        let leading_keyword_after_comments = |line: &str| -> Option<String> {
            let trimmed = line.trim_start();
            if Self::is_sqlplus_comment_line(trimmed) {
                return None;
            }

            let bytes = line.as_bytes();
            let mut i = 0usize;

            while i < bytes.len() {
                let b = bytes[i];

                if b.is_ascii_whitespace() {
                    i += 1;
                    continue;
                }

                if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
                    return None;
                }

                if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                    i += 2;
                    while i + 1 < bytes.len() {
                        if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                            i += 2;
                            break;
                        }
                        i += 1;
                    }
                    continue;
                }

                if sql_text::is_identifier_byte(b) {
                    let start = i;
                    i += 1;
                    while i < bytes.len() && sql_text::is_identifier_byte(bytes[i]) {
                        i += 1;
                    }
                    return Some(line[start..i].to_ascii_uppercase());
                }

                i += 1;
            }

            None
        };

        let mut builder = StatementBuilder::new();
        let mut depths = Vec::new();

        // Extra indentation state for SQL formatting depth that should not affect splitting.
        let mut subquery_paren_depth = 0usize;
        let mut pending_subquery_paren = 0usize;
        let mut with_cte_depth = 0usize;
        let mut with_cte_paren = 0isize;
        let mut pending_with = false;
        let mut pending_subprogram_begin = false;
        let mut exception_depth_stack: Vec<usize> = Vec::new();
        let mut exception_handler_body = false;
        let mut case_branch_stack: Vec<bool> = Vec::new();

        for line in sql.lines() {
            let words = if builder.is_idle() {
                sql_text::leading_words_upper(line)
            } else {
                Vec::new()
            };
            let leading_keyword = if builder.is_idle() {
                leading_keyword_after_comments(line)
            } else {
                None
            };
            let leading_word = leading_keyword
                .as_deref()
                .or_else(|| words.first().map(String::as_str));

            let trimmed_start = line.trim_start();
            let is_comment_or_blank = trimmed_start.is_empty()
                || Self::is_sqlplus_comment_line(trimmed_start)
                || trimmed_start.starts_with("/*")
                || trimmed_start.starts_with("*/");

            if pending_subquery_paren > 0 && !is_comment_or_blank {
                // WITH is also a valid subquery head (e.g. `( WITH cte AS (...) SELECT ... )`).
                // VALUES can head a nested query block in dialects that support table value
                // constructors in FROM/subquery positions.
                if leading_word.is_some_and(|w| w == "SELECT" || w == "WITH" || w == "VALUES") {
                    subquery_paren_depth =
                        subquery_paren_depth.saturating_add(pending_subquery_paren);
                }
                pending_subquery_paren = 0;
            }

            // Eagerly resolve pending_end when the current line does NOT continue an
            // END CASE / END IF / END LOOP / END BEFORE / END AFTER / END INSTEAD sequence.
            // Without this, a bare "END" on its own line (e.g. CASE expression end)
            // leaves block_depth and case_depth_stack stale for the next line's depth
            // calculation, causing incorrect indentation for ELSE/WHEN that follow.
            if builder.state.pending_end
                && !is_comment_or_blank
                && !matches!(
                    leading_word,
                    Some(
                        "CASE"
                            | "IF"
                            | "LOOP"
                            | "WHILE"
                            | "BEFORE"
                            | "AFTER"
                            | "INSTEAD"
                            | "REPEAT"
                    )
                )
            {
                if builder
                    .state
                    .case_depth_stack
                    .last()
                    .is_some_and(|d| *d + 1 == builder.state.block_depth)
                {
                    builder.state.case_depth_stack.pop();
                    builder.state.block_depth = builder.state.block_depth.saturating_sub(1);
                } else if builder.state.block_depth > 0 {
                    builder.state.block_depth -= 1;
                }
                builder.state.pending_end = false;
            }

            let open_cases = builder.state.case_depth_stack.len();
            if case_branch_stack.len() < open_cases {
                case_branch_stack.resize(open_cases, false);
            } else if case_branch_stack.len() > open_cases {
                case_branch_stack.truncate(open_cases);
            }
            let innermost_case_depth = builder.state.case_depth_stack.last().copied();
            let at_case_header_level =
                innermost_case_depth.is_some_and(|depth| depth + 1 == builder.block_depth());
            let exception_end_line = exception_depth_stack
                .last()
                .is_some_and(|depth| *depth == builder.block_depth())
                && matches!(leading_word, Some("END"));
            let mut depth = if leading_word.is_some_and(should_pre_dedent) {
                builder.block_depth().saturating_sub(1)
            } else {
                builder.block_depth()
            };
            if builder.state.pending_end
                && matches!(
                    leading_word,
                    Some(
                        "CASE"
                            | "IF"
                            | "LOOP"
                            | "WHILE"
                            | "BEFORE"
                            | "AFTER"
                            | "INSTEAD"
                            | "REPEAT"
                    )
                )
            {
                depth = depth.saturating_sub(1);
            }

            if at_case_header_level && matches!(leading_word, Some("WHEN" | "ELSE")) {
                depth = builder.block_depth();
            }

            if matches!(leading_word, Some("BEGIN"))
                && (pending_subprogram_begin || builder.state.after_declare)
            {
                depth = depth.saturating_sub(1);
            }

            if exception_handler_body
                && !matches!(leading_word, Some("WHEN"))
                && !exception_end_line
            {
                depth = depth.saturating_add(1);
            }

            let mut case_branch_indent = 0usize;
            for (case_depth, branch_active) in builder
                .state
                .case_depth_stack
                .iter()
                .zip(case_branch_stack.iter())
            {
                if !*branch_active {
                    continue;
                }
                let is_header_line = builder.block_depth() == *case_depth + 1
                    && matches!(leading_word, Some("WHEN" | "ELSE" | "END"));
                if !is_header_line {
                    case_branch_indent += 1;
                }
            }
            if case_branch_indent > 0 {
                depth = depth.saturating_add(case_branch_indent);
            }

            // Pre-dedent additional virtual depths for closing lines.
            if line.trim_start().starts_with(')') && subquery_paren_depth > 0 {
                depth = depth.saturating_add(subquery_paren_depth.saturating_sub(1));
            } else {
                depth = depth.saturating_add(subquery_paren_depth);
            }

            if with_cte_depth > 0 {
                let starts_main_select = leading_word.is_some_and(&is_with_main_query_keyword)
                    && with_cte_paren <= 0;
                // For the main query line that follows a WITH clause, do not add with_cte_depth.
                // This brings depth back to the WITH line's level without touching any
                // subquery_paren_depth that is already embedded in the current depth value.
                // (Previously depth.saturating_sub(1) was used, which incorrectly cancelled
                // subquery_paren_depth when the WITH clause appeared inside parentheses.)
                if !starts_main_select {
                    depth = depth.saturating_add(with_cte_depth);
                }
            }

            // No extra subprogram body depth: declarations and statements share the same level.

            depths.push(depth);

            // Update additional depth state with a very lightweight token pass.
            // Instead of maintaining duplicate quote/comment state machines,
            // derive the carry-over literal state from SplitState (the execution
            // base). process_text runs at the end of each iteration, so at this
            // point builder.state reflects the end of the previous line.
            let raw = line;
            let with_line = matches!(leading_word, Some("WITH"));

            if with_line {
                pending_with = true;
                with_cte_depth = with_cte_depth.max(1);
                with_cte_paren = 0;
            }

            let chars: Vec<char> = raw.chars().collect();
            let mut i = 0usize;
            // Base literal/comment state from the execution-layer parser.
            // SplitState already tracks these across lines via process_text,
            // so we read the carry-over state instead of duplicating the state machine.
            let mut in_block_comment = builder.state.in_block_comment;
            let mut in_q_quote = builder.state.in_q_quote;
            let mut q_quote_end = builder.state.q_quote_end;
            let mut in_single_quote = builder.state.in_single_quote;
            let mut in_double_quote = builder.state.in_double_quote;
            while i < chars.len() {
                let c = chars[i];
                let next = chars.get(i + 1).copied();

                if in_block_comment {
                    if c == '*' && next == Some('/') {
                        in_block_comment = false;
                        i += 2;
                        continue;
                    }
                    i += 1;
                    continue;
                }

                if in_q_quote {
                    if Some(c) == q_quote_end && next == Some('\'') {
                        in_q_quote = false;
                        q_quote_end = None;
                        i += 2;
                        continue;
                    }
                    i += 1;
                    continue;
                }

                if in_single_quote {
                    if c == '\'' {
                        if next == Some('\'') {
                            i += 2;
                            continue;
                        }
                        in_single_quote = false;
                    }
                    i += 1;
                    continue;
                }

                if in_double_quote {
                    if c == '"' {
                        if next == Some('"') {
                            i += 2;
                            continue;
                        }
                        in_double_quote = false;
                    }
                    i += 1;
                    continue;
                }

                if c == '-' && next == Some('-') {
                    break;
                }

                if c == '/' && next == Some('*') {
                    in_block_comment = true;
                    i += 2;
                    continue;
                }

                // Guard: q/nq-quote detection must not fire when the
                // current character is part of a longer identifier
                // (e.g. `seq'text'` should NOT start a q-quote at `q`).
                let prev_is_ident = i > 0 && sql_text::is_identifier_char(chars[i - 1]);

                if !prev_is_ident
                    && (c == 'n' || c == 'N')
                    && matches!(next, Some('q') | Some('Q'))
                    && chars.get(i + 2) == Some(&'\'')
                    && chars.get(i + 3).is_some()
                {
                    let delimiter = chars[i + 3];
                    in_q_quote = true;
                    q_quote_end = Some(sql_text::q_quote_closing(delimiter));
                    i += 4;
                    continue;
                }

                if !prev_is_ident
                    && (c == 'q' || c == 'Q')
                    && next == Some('\'')
                    && chars.get(i + 2).is_some()
                {
                    let delimiter = chars[i + 2];
                    in_q_quote = true;
                    q_quote_end = Some(sql_text::q_quote_closing(delimiter));
                    i += 3;
                    continue;
                }

                if c == '\'' {
                    in_single_quote = true;
                    i += 1;
                    continue;
                }

                if c == '"' {
                    in_double_quote = true;
                    i += 1;
                    continue;
                }

                if c == '(' {
                    let j = skip_ws_and_comments(&chars, i + 1);
                    let mut k = j;
                    while k < chars.len() && (chars[k].is_ascii_alphanumeric() || chars[k] == '_') {
                        k += 1;
                    }
                    if k > j {
                        let word: String = chars[j..k].iter().collect();
                        let word = word.to_ascii_uppercase();
                        if word == "SELECT" || word == "WITH" || word == "VALUES" {
                            subquery_paren_depth += 1;
                        }
                    } else if j >= chars.len()
                        || (chars[j] == '-' && j + 1 < chars.len() && chars[j + 1] == '-')
                        || (chars[j] == '/' && j + 1 < chars.len() && chars[j + 1] == '*')
                    {
                        pending_subquery_paren += 1;
                    }
                    if with_cte_depth > 0 {
                        with_cte_paren += 1;
                    }
                } else if c == ')' {
                    subquery_paren_depth = subquery_paren_depth.saturating_sub(1);
                    if with_cte_depth > 0 {
                        with_cte_paren -= 1;
                    }
                }
                i += 1;
            }

            let mut idx = 0usize;
            while idx < words.len() {
                let word = words[idx].as_str();
                let next = words.get(idx + 1).map(String::as_str);

                if matches!(word, "PROCEDURE" | "FUNCTION") {
                    pending_subprogram_begin = true;
                } else if pending_subprogram_begin && word == "BEGIN" {
                    pending_subprogram_begin = false;
                } else if word == "END"
                    && next != Some("IF")
                    && next != Some("LOOP")
                    && next != Some("CASE")
                {
                    // No subprogram body depth tracking.
                }

                idx += 1;
            }

            if pending_with
                && leading_word.is_some_and(&is_with_main_query_keyword)
                && with_cte_paren <= 0
            {
                with_cte_depth = 0;
                pending_with = false;
            }

            if matches!(leading_word, Some("EXCEPTION")) {
                exception_depth_stack.push(builder.block_depth());
                exception_handler_body = false;
            } else if !exception_depth_stack.is_empty() && matches!(leading_word, Some("WHEN")) {
                exception_handler_body = true;
            } else if exception_depth_stack
                .last()
                .is_some_and(|depth| *depth == builder.block_depth())
                && matches!(leading_word, Some("END"))
            {
                exception_depth_stack.pop();
                exception_handler_body = false;
            }
            if at_case_header_level && matches!(leading_word, Some("WHEN" | "ELSE")) {
                if let Some(last) = case_branch_stack.last_mut() {
                    *last = true;
                }
            } else if at_case_header_level && matches!(leading_word, Some("END")) {
                if let Some(last) = case_branch_stack.last_mut() {
                    *last = false;
                }
            }

            let mut line_with_newline = String::from(line);
            line_with_newline.push('\n');
            builder.process_text(&line_with_newline);
        }

        depths
    }

    pub fn strip_leading_comments(sql: &str) -> String {
        let mut remaining = sql;

        loop {
            let trimmed = remaining.trim_start();

            if Self::is_sqlplus_comment_line(trimmed) {
                if let Some(line_end) = trimmed.find('\n') {
                    remaining = &trimmed[line_end + 1..];
                    continue;
                }
                return String::new();
            }

            if trimmed.starts_with("/*") {
                if let Some(block_end) = trimmed.find("*/") {
                    remaining = &trimmed[block_end + 2..];
                    continue;
                }
                return String::new();
            }

            return trimmed.to_string();
        }
    }

    fn strip_trailing_comments(sql: &str) -> String {
        let mut result = sql.to_string();

        loop {
            let trimmed = result.trim_end();
            if trimmed.is_empty() {
                return String::new();
            }

            // Check for trailing line comment (-- ... at end of line)
            // Find the last line and check if it's only a comment
            if let Some(last_newline) = trimmed.rfind('\n') {
                let last_line = trimmed[last_newline + 1..].trim();
                if Self::is_sqlplus_comment_line(last_line) {
                    result = trimmed[..last_newline].to_string();
                    continue;
                }
            } else {
                // Single line - check if entire thing is a line comment
                if Self::is_sqlplus_comment_line(trimmed) {
                    return String::new();
                }
            }

            // Check for trailing block comment
            if trimmed.ends_with("*/") {
                // Find matching /*
                // Need to scan backwards to find the opening /*
                let bytes = trimmed.as_bytes();
                let mut depth = 0;
                let mut i = bytes.len();
                let mut found_start = None;

                while i > 0 {
                    i -= 1;
                    if i > 0 && bytes[i - 1] == b'/' && bytes[i] == b'*' {
                        depth -= 1;
                        if depth < 0 {
                            found_start = Some(i - 1);
                            break;
                        }
                        i -= 1;
                    } else if i > 0 && bytes[i - 1] == b'*' && bytes[i] == b'/' {
                        depth += 1;
                        i -= 1;
                    }
                }

                if let Some(start) = found_start {
                    // Check if this block comment is at the end (only whitespace before it)
                    let before = trimmed[..start].trim_end();
                    if before.is_empty() {
                        return String::new();
                    }
                    result = before.to_string();
                    continue;
                }
            }

            return trimmed.to_string();
        }
    }

    fn strip_comments(sql: &str) -> String {
        let without_leading = Self::strip_leading_comments(sql);
        Self::strip_trailing_comments(&without_leading)
    }

    /// Strip extra trailing semicolons from a statement.
    /// "END;;" -> "END;", "SELECT 1;;" -> "SELECT 1"
    /// Preserves single trailing semicolon for PL/SQL statements.
    fn strip_extra_trailing_semicolons(sql: &str) -> String {
        let trimmed = sql.trim_end();
        if trimmed.is_empty() {
            return String::new();
        }

        // Count trailing semicolons
        let mut semicolon_count = 0;
        for c in trimmed.chars().rev() {
            if c == ';' {
                semicolon_count += 1;
            } else if c.is_whitespace() {
                continue;
            } else {
                break;
            }
        }

        if semicolon_count <= 1 {
            return trimmed.to_string();
        }

        // Remove all trailing semicolons and whitespace, then check if we need to add one back
        let without_semis = trimmed.trim_end_matches(|c: char| c == ';' || c.is_whitespace());
        if without_semis.is_empty() {
            return String::new();
        }

        // Check if this is a PL/SQL statement that needs trailing semicolon
        let upper = without_semis.to_ascii_uppercase();
        if upper.ends_with("END") || upper.contains("END ") {
            format!("{};", without_semis)
        } else {
            without_semis.to_string()
        }
    }

    pub fn leading_keyword(sql: &str) -> Option<String> {
        let cleaned = Self::strip_leading_comments(sql);
        cleaned
            .split_whitespace()
            .next()
            .map(|token| token.to_ascii_uppercase())
    }

    pub fn is_select_statement(sql: &str) -> bool {
        match Self::leading_keyword(sql).as_deref() {
            Some("SELECT") => true,
            Some("WITH") => Self::with_clause_starts_with_select(sql),
            _ => false,
        }
    }

    pub fn maybe_inject_rowid_for_editing(sql: &str) -> String {
        if !Self::is_select_statement(sql) {
            return sql.to_string();
        }

        let leading_kw = Self::leading_keyword(sql);
        let leading = leading_kw.as_deref();

        // For WITH (CTE) queries, find the main SELECT's FROM clause
        let effective_sql: &str;
        let with_prefix_len: usize;
        if leading == Some("WITH") {
            if let Some(main_select_idx) = Self::find_main_select_after_with(sql) {
                effective_sql = &sql[main_select_idx..];
                with_prefix_len = main_select_idx;
            } else {
                return sql.to_string();
            }
        } else if leading == Some("SELECT") {
            effective_sql = sql;
            with_prefix_len = 0;
        } else {
            return sql.to_string();
        }

        let Some(from_idx_in_effective) = Self::find_top_level_keyword(effective_sql, "FROM")
        else {
            return sql.to_string();
        };

        if !Self::is_rowid_edit_eligible_query(effective_sql) {
            return sql.to_string();
        }

        let Some(rowid_expr) =
            Self::single_table_rowid_expression(effective_sql, from_idx_in_effective)
        else {
            return sql.to_string();
        };
        let rowid_qualifier = rowid_expr.strip_suffix(".ROWID").unwrap_or("").trim();

        let select_body_start_in_effective =
            Self::find_select_body_start(effective_sql).unwrap_or(from_idx_in_effective);
        if select_body_start_in_effective >= from_idx_in_effective {
            return sql.to_string();
        }

        let select_list_upper = effective_sql
            [select_body_start_in_effective..from_idx_in_effective]
            .to_ascii_uppercase();
        if select_list_upper.contains("ROWID") {
            return sql.to_string();
        }

        // Build the rewritten SQL
        let injection = format!("{rowid_expr}, ");
        let global_select_body_start = with_prefix_len + select_body_start_in_effective;
        let mut rewritten = String::with_capacity(sql.len().saturating_add(injection.len()));
        rewritten.push_str(&sql[..global_select_body_start]);
        rewritten.push_str(&injection);
        let select_body = &sql[global_select_body_start..];
        if rowid_qualifier.is_empty() {
            rewritten.push_str(select_body);
            return rewritten;
        }

        if let Some((wildcard_start, wildcard_end)) =
            Self::find_leading_wildcard_in_select_list(select_body)
        {
            rewritten.push_str(&select_body[..wildcard_start]);
            rewritten.push_str(rowid_qualifier);
            rewritten.push_str(".*");
            rewritten.push_str(&select_body[wildcard_end..]);
        } else {
            rewritten.push_str(select_body);
        }
        rewritten
    }

    pub(crate) fn rowid_safe_execution_sql(_original_sql: &str, rewritten_sql: &str) -> String {
        if !Self::is_select_statement(rewritten_sql) {
            return rewritten_sql.to_string();
        }

        let leading_kw = Self::leading_keyword(rewritten_sql);
        let leading = leading_kw.as_deref();

        let effective_sql: &str;
        let with_prefix_len: usize;
        if leading == Some("WITH") {
            if let Some(main_select_idx) = Self::find_main_select_after_with(rewritten_sql) {
                effective_sql = &rewritten_sql[main_select_idx..];
                with_prefix_len = main_select_idx;
            } else {
                return rewritten_sql.to_string();
            }
        } else if leading == Some("SELECT") {
            effective_sql = rewritten_sql;
            with_prefix_len = 0;
        } else {
            return rewritten_sql.to_string();
        }

        let Some(from_idx_in_effective) = Self::find_top_level_keyword(effective_sql, "FROM")
        else {
            return rewritten_sql.to_string();
        };
        let global_from_idx = with_prefix_len.saturating_add(from_idx_in_effective);
        if !rewritten_sql.is_char_boundary(global_from_idx) {
            return rewritten_sql.to_string();
        }

        let select_body_start_in_effective =
            Self::find_select_body_start(effective_sql).unwrap_or(from_idx_in_effective);
        if select_body_start_in_effective >= from_idx_in_effective {
            return rewritten_sql.to_string();
        }

        let global_select_body_start = with_prefix_len + select_body_start_in_effective;
        if !rewritten_sql.is_char_boundary(global_select_body_start) {
            return rewritten_sql.to_string();
        }
        let select_list = match rewritten_sql.get(global_select_body_start..global_from_idx) {
            Some(select_list) => select_list,
            None => return rewritten_sql.to_string(),
        };

        let mut rewritten_select_list = String::with_capacity(select_list.len().saturating_add(64));
        let mut cursor = 0usize;
        let mut projection_index = 0usize;
        let mut changed = false;

        while cursor < select_list.len() {
            let tail = match select_list.get(cursor..) {
                Some(tail) => tail,
                None => return rewritten_sql.to_string(),
            };
            let projection_end = if let Some(comma_offset) = Self::find_first_top_level_comma(tail)
            {
                cursor.saturating_add(comma_offset)
            } else {
                select_list.len()
            };
            if !select_list.is_char_boundary(projection_end) {
                return rewritten_sql.to_string();
            }

            let projection = match select_list.get(cursor..projection_end) {
                Some(projection) => projection,
                None => return rewritten_sql.to_string(),
            };
            let projection_trimmed = projection.trim();
            let projection_trimmed_end = projection.trim_end();
            let trailing_ws_len = projection
                .len()
                .saturating_sub(projection_trimmed_end.len());
            let suffix_ws = if trailing_ws_len == 0 {
                ""
            } else {
                projection
                    .get(projection.len().saturating_sub(trailing_ws_len)..)
                    .unwrap_or("")
            };

            let normalized_projection = if let Some(expr_token) =
                Self::leading_projection_token(projection_trimmed)
            {
                let expr_upper = expr_token.to_ascii_uppercase();
                if !expr_upper.starts_with("ROWIDTOCHAR(")
                    && (expr_upper == "ROWID" || expr_upper.ends_with(".ROWID"))
                {
                    changed = true;
                    if projection_index == 0 {
                        let mut replaced =
                            "ROWIDTOCHAR(".to_string() + expr_token + ") AS SQ_INTERNAL_ROWID";
                        replaced.push_str(suffix_ws);
                        replaced
                    } else {
                        let trailing = projection_trimmed.get(expr_token.len()..).unwrap_or("");
                        let mut replaced = "ROWIDTOCHAR(".to_string() + expr_token + ")" + trailing;
                        replaced.push_str(suffix_ws);
                        replaced
                    }
                } else {
                    projection.trim_start().to_string()
                }
            } else {
                projection.trim_start().to_string()
            };

            if !rewritten_select_list.is_empty() {
                rewritten_select_list.push_str(", ");
            }
            rewritten_select_list.push_str(&normalized_projection);

            if projection_end == select_list.len() {
                break;
            }
            cursor = projection_end.saturating_add(1);
            projection_index = projection_index.saturating_add(1);
        }

        if !changed {
            return rewritten_sql.to_string();
        }

        let mut normalized = String::with_capacity(
            rewritten_sql
                .len()
                .saturating_sub(select_list.len())
                .saturating_add(rewritten_select_list.len()),
        );
        normalized.push_str(&rewritten_sql[..global_select_body_start]);
        normalized.push_str(&rewritten_select_list);
        normalized.push_str(&rewritten_sql[global_from_idx..]);
        normalized
    }

    fn leading_projection_token(expr: &str) -> Option<&str> {
        let trimmed = expr.trim();
        if trimmed.is_empty() {
            return None;
        }
        for (idx, ch) in trimmed.char_indices() {
            if ch.is_whitespace() {
                if idx == 0 {
                    continue;
                }
                return trimmed
                    .get(..idx)
                    .map(str::trim)
                    .filter(|token| !token.is_empty());
            }
        }
        Some(trimmed)
    }

    fn find_first_top_level_comma(sql: &str) -> Option<usize> {
        let mut chars = sql.char_indices().peekable();
        let mut depth = 0usize;
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false;

        while let Some((byte_idx, c)) = chars.next() {
            let next = chars.peek().map(|(_, ch)| *ch);

            if in_line_comment {
                if c == '\n' {
                    in_line_comment = false;
                }
                continue;
            }

            if in_block_comment {
                if c == '*' && next == Some('/') {
                    in_block_comment = false;
                    chars.next(); // consume '/'
                }
                continue;
            }

            if in_single_quote {
                if c == '\'' {
                    if next == Some('\'') {
                        chars.next(); // consume escaped quote
                        continue;
                    }
                    in_single_quote = false;
                }
                continue;
            }

            if in_double_quote {
                if c == '"' {
                    if next == Some('"') {
                        chars.next(); // consume escaped quote
                        continue;
                    }
                    in_double_quote = false;
                }
                continue;
            }

            if c == '-' && next == Some('-') {
                in_line_comment = true;
                chars.next(); // consume second '-'
                continue;
            }
            if c == '/' && next == Some('*') {
                in_block_comment = true;
                chars.next(); // consume '*'
                continue;
            }
            if c == '\'' {
                in_single_quote = true;
                continue;
            }
            if c == '"' {
                in_double_quote = true;
                continue;
            }

            if c == '(' {
                depth = depth.saturating_add(1);
                continue;
            }
            if c == ')' {
                depth = depth.saturating_sub(1);
                continue;
            }

            if depth == 0 && c == ',' {
                return Some(byte_idx);
            }
        }

        None
    }

    pub fn is_rowid_edit_eligible_query(sql: &str) -> bool {
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            return false;
        }
        let Some(from_idx) = Self::find_top_level_keyword(trimmed, "FROM") else {
            return false;
        };
        let select_idx = Self::find_top_level_keyword(trimmed, "SELECT").unwrap_or(0);
        Self::is_rowid_edit_eligible_select(trimmed, select_idx, from_idx)
    }

    fn is_rowid_edit_eligible_select(sql: &str, select_idx: usize, from_idx: usize) -> bool {
        if Self::has_top_level_set_operator(sql)
            || Self::has_top_level_identifier_keyword(sql, "GROUP")
            || Self::has_top_level_connect_by(sql)
            || Self::has_top_level_identifier_keyword(sql, "MATCH_RECOGNIZE")
            || Self::has_top_level_identifier_keyword(sql, "PIVOT")
            || Self::has_top_level_identifier_keyword(sql, "UNPIVOT")
            || Self::has_top_level_identifier_keyword(sql, "MODEL")
            || !Self::is_single_table_from_clause(sql, from_idx)
            || Self::select_clause_has_distinct_or_unique(sql, select_idx, from_idx)
            || Self::select_clause_has_top_level_aggregate(sql, select_idx, from_idx)
            || Self::select_clause_has_top_level_analytic(sql, select_idx, from_idx)
        {
            return false;
        }
        true
    }

    fn select_clause_has_top_level_aggregate(
        sql: &str,
        select_idx: usize,
        from_idx: usize,
    ) -> bool {
        if from_idx <= select_idx
            || !sql.is_char_boundary(select_idx)
            || !sql.is_char_boundary(from_idx)
        {
            return false;
        }

        let select_body_start = Self::find_select_body_start(sql).unwrap_or(select_idx);
        if select_body_start >= from_idx || !sql.is_char_boundary(select_body_start) {
            return false;
        }

        let select_list = &sql[select_body_start..from_idx];
        let chars: Vec<(usize, char)> = select_list.char_indices().collect();
        let len = chars.len();
        let mut i = 0usize;
        let mut depth = 0usize;
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false;

        while i < len {
            let (_, c) = chars[i];
            let next = chars.get(i + 1).map(|(_, ch)| *ch);

            if in_line_comment {
                if c == '\n' {
                    in_line_comment = false;
                }
                i += 1;
                continue;
            }

            if in_block_comment {
                if c == '*' && next == Some('/') {
                    in_block_comment = false;
                    i += 2;
                    continue;
                }
                i += 1;
                continue;
            }

            if in_single_quote {
                if c == '\'' {
                    if next == Some('\'') {
                        i += 2;
                        continue;
                    }
                    in_single_quote = false;
                }
                i += 1;
                continue;
            }

            if in_double_quote {
                if c == '"' {
                    if next == Some('"') {
                        i += 2;
                        continue;
                    }
                    in_double_quote = false;
                }
                i += 1;
                continue;
            }

            if c == '-' && next == Some('-') {
                in_line_comment = true;
                i += 2;
                continue;
            }
            if c == '/' && next == Some('*') {
                in_block_comment = true;
                i += 2;
                continue;
            }
            if c == '\'' {
                in_single_quote = true;
                i += 1;
                continue;
            }
            if c == '"' {
                in_double_quote = true;
                i += 1;
                continue;
            }

            if c == '(' {
                depth = depth.saturating_add(1);
                i += 1;
                continue;
            }
            if c == ')' {
                depth = depth.saturating_sub(1);
                i += 1;
                continue;
            }

            if depth == 0 && (c.is_ascii_alphabetic() || c == '_') {
                let start = chars[i].0;
                let mut end_i = i + 1;
                while end_i < len {
                    let ch = chars[end_i].1;
                    if ch.is_ascii_alphanumeric() || ch == '_' || ch == '$' || ch == '#' {
                        end_i += 1;
                    } else {
                        break;
                    }
                }
                let end = if end_i < len {
                    chars[end_i].0
                } else {
                    select_list.len()
                };
                let word = &select_list[start..end];
                let mut lookahead = end_i;
                while lookahead < len && chars[lookahead].1.is_whitespace() {
                    lookahead += 1;
                }
                let has_open_paren = lookahead < len && chars[lookahead].1 == '(';
                if has_open_paren && Self::is_aggregate_function_name(word) {
                    return true;
                }
                i = end_i;
                continue;
            }

            i += 1;
        }

        false
    }

    fn is_aggregate_function_name(word: &str) -> bool {
        matches!(
            word.to_ascii_uppercase().as_str(),
            "COUNT"
                | "SUM"
                | "AVG"
                | "MIN"
                | "MAX"
                | "LISTAGG"
                | "JSON_ARRAYAGG"
                | "JSON_OBJECTAGG"
                | "STDDEV"
                | "STDDEV_POP"
                | "STDDEV_SAMP"
                | "VARIANCE"
                | "VAR_POP"
                | "VAR_SAMP"
                | "MEDIAN"
                | "CORR"
                | "COVAR_POP"
                | "COVAR_SAMP"
                | "REGR_SLOPE"
                | "REGR_INTERCEPT"
                | "REGR_COUNT"
                | "REGR_R2"
                | "REGR_AVGX"
                | "REGR_AVGY"
                | "REGR_SXX"
                | "REGR_SYY"
                | "REGR_SXY"
        )
    }

    fn select_clause_has_top_level_analytic(sql: &str, select_idx: usize, from_idx: usize) -> bool {
        if from_idx <= select_idx
            || !sql.is_char_boundary(select_idx)
            || !sql.is_char_boundary(from_idx)
        {
            return false;
        }

        let select_body_start = Self::find_select_body_start(sql).unwrap_or(select_idx);
        if select_body_start >= from_idx || !sql.is_char_boundary(select_body_start) {
            return false;
        }

        let select_list = &sql[select_body_start..from_idx];
        let chars: Vec<(usize, char)> = select_list.char_indices().collect();
        let len = chars.len();
        let mut i = 0usize;
        let mut depth = 0usize;
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false;

        while i < len {
            let (_, c) = chars[i];
            let next = chars.get(i + 1).map(|(_, ch)| *ch);

            if in_line_comment {
                if c == '\n' {
                    in_line_comment = false;
                }
                i += 1;
                continue;
            }

            if in_block_comment {
                if c == '*' && next == Some('/') {
                    in_block_comment = false;
                    i += 2;
                    continue;
                }
                i += 1;
                continue;
            }

            if in_single_quote {
                if c == '\'' {
                    if next == Some('\'') {
                        i += 2;
                        continue;
                    }
                    in_single_quote = false;
                }
                i += 1;
                continue;
            }

            if in_double_quote {
                if c == '"' {
                    if next == Some('"') {
                        i += 2;
                        continue;
                    }
                    in_double_quote = false;
                }
                i += 1;
                continue;
            }

            if c == '-' && next == Some('-') {
                in_line_comment = true;
                i += 2;
                continue;
            }
            if c == '/' && next == Some('*') {
                in_block_comment = true;
                i += 2;
                continue;
            }
            if c == '\'' {
                in_single_quote = true;
                i += 1;
                continue;
            }
            if c == '"' {
                in_double_quote = true;
                i += 1;
                continue;
            }

            if c == '(' {
                depth = depth.saturating_add(1);
                i += 1;
                continue;
            }
            if c == ')' {
                depth = depth.saturating_sub(1);
                i += 1;
                continue;
            }

            if depth == 0 && (c.is_ascii_alphabetic() || c == '_') {
                let start = chars[i].0;
                let mut end_i = i + 1;
                while end_i < len {
                    let ch = chars[end_i].1;
                    if ch.is_ascii_alphanumeric() || ch == '_' || ch == '$' || ch == '#' {
                        end_i += 1;
                    } else {
                        break;
                    }
                }
                let end = if end_i < len {
                    chars[end_i].0
                } else {
                    select_list.len()
                };
                let word = &select_list[start..end];
                if word.eq_ignore_ascii_case("OVER") {
                    let mut lookahead = end_i;
                    while lookahead < len && chars[lookahead].1.is_whitespace() {
                        lookahead += 1;
                    }
                    if lookahead < len && chars[lookahead].1 == '(' {
                        return true;
                    }
                }
                i = end_i;
                continue;
            }

            i += 1;
        }

        false
    }

    /// Find the byte index of the main (final) SELECT keyword after a WITH clause.
    /// This skips over all CTE definitions to find the top-level SELECT that follows.
    fn find_main_select_after_with(sql: &str) -> Option<usize> {
        let chars: Vec<(usize, char)> = sql.char_indices().collect();
        let len = chars.len();
        let mut i = 0usize;
        let mut depth = 0usize;
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false;

        // Skip past the WITH keyword
        while i < len {
            let (byte_idx, c) = chars[i];
            if c.is_ascii_alphabetic() {
                let start = byte_idx;
                let mut end_i = i;
                while end_i < len
                    && (chars[end_i].1.is_ascii_alphanumeric() || chars[end_i].1 == '_')
                {
                    end_i += 1;
                }
                let end_byte = if end_i < len {
                    chars[end_i].0
                } else {
                    sql.len()
                };
                let word = &sql[start..end_byte];
                if word.eq_ignore_ascii_case("WITH") {
                    i = end_i;
                    break;
                }
                i = end_i;
                continue;
            }
            i += 1;
        }

        // Now scan for top-level SELECT at depth 0
        while i < len {
            let (byte_idx, c) = chars[i];
            let next = chars.get(i + 1).map(|(_, ch)| *ch);

            if in_line_comment {
                if c == '\n' {
                    in_line_comment = false;
                }
                i += 1;
                continue;
            }
            if in_block_comment {
                if c == '*' && next == Some('/') {
                    in_block_comment = false;
                    i += 2;
                    continue;
                }
                i += 1;
                continue;
            }
            if in_single_quote {
                if c == '\'' {
                    if next == Some('\'') {
                        i += 2;
                        continue;
                    }
                    in_single_quote = false;
                }
                i += 1;
                continue;
            }
            if in_double_quote {
                if c == '"' {
                    if next == Some('"') {
                        i += 2;
                        continue;
                    }
                    in_double_quote = false;
                }
                i += 1;
                continue;
            }

            if c == '-' && next == Some('-') {
                in_line_comment = true;
                i += 2;
                continue;
            }
            if c == '/' && next == Some('*') {
                in_block_comment = true;
                i += 2;
                continue;
            }
            if c == '\'' {
                in_single_quote = true;
                i += 1;
                continue;
            }
            if c == '"' {
                in_double_quote = true;
                i += 1;
                continue;
            }

            if c == '(' {
                depth = depth.saturating_add(1);
                i += 1;
                continue;
            }
            if c == ')' {
                depth = depth.saturating_sub(1);
                i += 1;
                continue;
            }

            if depth == 0 && c.is_ascii_alphabetic() {
                let start = byte_idx;
                let mut end_i = i;
                while end_i < len
                    && (chars[end_i].1.is_ascii_alphanumeric() || chars[end_i].1 == '_')
                {
                    end_i += 1;
                }
                let end_byte = if end_i < len {
                    chars[end_i].0
                } else {
                    sql.len()
                };
                let word = &sql[start..end_byte];
                if word.eq_ignore_ascii_case("SELECT") {
                    return Some(start);
                }
                i = end_i;
                continue;
            }

            i += 1;
        }

        None
    }

    /// Check if the effective SQL has a top-level set operator (UNION, INTERSECT, MINUS, EXCEPT).
    fn has_top_level_set_operator(sql: &str) -> bool {
        Self::has_top_level_identifier_keyword(sql, "UNION")
            || Self::has_top_level_identifier_keyword(sql, "INTERSECT")
            || Self::has_top_level_identifier_keyword(sql, "MINUS")
            || Self::has_top_level_identifier_keyword(sql, "EXCEPT")
    }

    /// Check if the effective SQL has a top-level CONNECT BY clause.
    fn has_top_level_connect_by(sql: &str) -> bool {
        Self::has_top_level_identifier_keyword(sql, "CONNECT")
            || Self::has_top_level_identifier_keyword(sql, "START")
    }

    /// Like `find_top_level_keyword`, but additionally validates that the
    /// character immediately after the matched word is not `_`, `$`, or `#`.
    /// `find_top_level_keyword` splits on `is_ascii_alphanumeric()` only,
    /// so `START_DATE` would match `START`.  This helper rejects such cases.
    fn has_top_level_identifier_keyword(sql: &str, keyword: &str) -> bool {
        if keyword.contains('_') {
            return Self::has_top_level_identifier_token(sql, keyword);
        }
        let Some(idx) = Self::find_top_level_keyword(sql, keyword) else {
            return false;
        };
        let after = idx.saturating_add(keyword.len());
        if after >= sql.len() {
            return true;
        }
        !matches!(
            sql.as_bytes().get(after),
            Some(b'_') | Some(b'$') | Some(b'#')
        )
    }

    /// Check if the effective SQL contains a top-level identifier token that
    /// exactly matches `keyword` (supports underscores in keyword).
    fn has_top_level_identifier_token(sql: &str, keyword: &str) -> bool {
        let chars: Vec<(usize, char)> = sql.char_indices().collect();
        let len = chars.len();
        let keyword_upper = keyword.to_ascii_uppercase();
        let mut i = 0usize;
        let mut depth = 0usize;
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false;

        while i < len {
            let (byte_idx, c) = chars[i];
            let next = chars.get(i + 1).map(|(_, ch)| *ch);

            if in_line_comment {
                if c == '\n' {
                    in_line_comment = false;
                }
                i += 1;
                continue;
            }

            if in_block_comment {
                if c == '*' && next == Some('/') {
                    in_block_comment = false;
                    i += 2;
                    continue;
                }
                i += 1;
                continue;
            }

            if in_single_quote {
                if c == '\'' {
                    if next == Some('\'') {
                        i += 2;
                        continue;
                    }
                    in_single_quote = false;
                }
                i += 1;
                continue;
            }

            if in_double_quote {
                if c == '"' {
                    if next == Some('"') {
                        i += 2;
                        continue;
                    }
                    in_double_quote = false;
                }
                i += 1;
                continue;
            }

            if c == '-' && next == Some('-') {
                in_line_comment = true;
                i += 2;
                continue;
            }
            if c == '/' && next == Some('*') {
                in_block_comment = true;
                i += 2;
                continue;
            }
            if c == '\'' {
                in_single_quote = true;
                i += 1;
                continue;
            }
            if c == '"' {
                in_double_quote = true;
                i += 1;
                continue;
            }

            if c == '(' {
                depth = depth.saturating_add(1);
                i += 1;
                continue;
            }
            if c == ')' {
                depth = depth.saturating_sub(1);
                i += 1;
                continue;
            }

            if depth == 0 && c.is_ascii_alphabetic() {
                let start = byte_idx;
                let mut end_i = i;
                while end_i < len {
                    let ch = chars[end_i].1;
                    if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '$' | '#') {
                        end_i += 1;
                    } else {
                        break;
                    }
                }
                let end_byte = if end_i < len {
                    chars[end_i].0
                } else {
                    sql.len()
                };
                if sql[start..end_byte].to_ascii_uppercase() == keyword_upper {
                    return true;
                }
                i = end_i;
                continue;
            }

            i += 1;
        }

        false
    }

    /// Like `find_top_level_keyword`, but validates identifier boundary.
    #[allow(dead_code)]
    fn find_top_level_identifier_keyword(sql: &str, keyword: &str) -> Option<usize> {
        if Self::has_top_level_identifier_keyword(sql, keyword) {
            Self::find_top_level_keyword(sql, keyword)
        } else {
            None
        }
    }

    /// Extract the ROWID expression for the first real (non-subquery) table
    /// in the FROM clause. Works for single tables, JOINs, and comma-joins.
    /// Returns `Some("alias.ROWID")` or `Some("TABLE_NAME.ROWID")` or `None`.
    #[allow(dead_code)]
    fn first_from_table_rowid_expression(sql: &str, from_idx: usize) -> Option<String> {
        let from_body_start = from_idx.saturating_add("FROM".len());
        if from_body_start >= sql.len() || !sql.is_char_boundary(from_body_start) {
            return None;
        }

        let from_clause = &sql[from_body_start..];
        let trimmed = from_clause.trim_start();
        if trimmed.is_empty() {
            return None;
        }

        // If FROM starts with '(' it's a subquery - scan past it to check
        // but first, try to extract table from the first non-subquery reference.
        Self::extract_first_table_ref_rowid(trimmed)
    }

    /// Extract the first table reference (name + optional alias) from a FROM clause fragment.
    /// Handles: plain table, schema.table, quoted identifiers, subqueries (skipped),
    /// LATERAL keyword, ONLY keyword.
    #[allow(dead_code)]
    fn extract_first_table_ref_rowid(from_body: &str) -> Option<String> {
        let chars: Vec<(usize, char)> = from_body.char_indices().collect();
        let len = chars.len();
        let mut i = 0usize;

        // Skip leading whitespace
        while i < len && chars[i].1.is_whitespace() {
            i += 1;
        }
        if i >= len {
            return None;
        }

        // Skip ONLY keyword if present (e.g., FROM ONLY (table_name))
        if i < len && chars[i].1.is_ascii_alphabetic() {
            let word_start = chars[i].0;
            let mut wi = i;
            while wi < len
                && (chars[wi].1.is_ascii_alphanumeric()
                    || chars[wi].1 == '_'
                    || chars[wi].1 == '$'
                    || chars[wi].1 == '#')
            {
                wi += 1;
            }
            let word_end = if wi < len {
                chars[wi].0
            } else {
                from_body.len()
            };
            let word = &from_body[word_start..word_end];
            if word.eq_ignore_ascii_case("LATERAL") || word.eq_ignore_ascii_case("ONLY") {
                i = wi;
                while i < len && chars[i].1.is_whitespace() {
                    i += 1;
                }
            }
        }

        if i >= len {
            return None;
        }

        // If starts with '(' it's a subquery/inline view — cannot use ROWID directly
        if chars[i].1 == '(' {
            return None;
        }

        // Parse the table name (possibly schema.table)
        let table_name = Self::parse_identifier_at(&chars, from_body, &mut i)?;

        // Check for schema.table pattern
        let full_name = if i < len && chars[i].1 == '.' {
            i += 1; // skip dot
            if let Some(second_part) = Self::parse_identifier_at(&chars, from_body, &mut i) {
                format!("{}.{}", table_name, second_part)
            } else {
                table_name
            }
        } else {
            table_name
        };

        // Skip whitespace after table name
        while i < len && chars[i].1.is_whitespace() {
            i += 1;
        }

        // Check for optional alias
        let alias = Self::parse_optional_alias(&chars, from_body, &mut i);

        if let Some(alias_str) = alias {
            Some(format!("{alias_str}.ROWID"))
        } else {
            Some(format!("{full_name}.ROWID"))
        }
    }

    /// Parse an identifier (possibly quoted) at the current position.
    fn parse_identifier_at(chars: &[(usize, char)], text: &str, pos: &mut usize) -> Option<String> {
        let len = chars.len();
        if *pos >= len {
            return None;
        }

        let start_char = chars[*pos].1;

        if start_char == '"' {
            // Quoted identifier
            let start_byte = chars[*pos].0;
            *pos += 1; // skip opening quote
            while *pos < len {
                if chars[*pos].1 == '"' {
                    if *pos + 1 < len && chars[*pos + 1].1 == '"' {
                        *pos += 2; // escaped quote
                        continue;
                    }
                    *pos += 1; // closing quote
                    let end_byte = if *pos < len {
                        chars[*pos].0
                    } else {
                        text.len()
                    };
                    return Some(text[start_byte..end_byte].to_string());
                }
                *pos += 1;
            }
            // Unterminated quote — return what we have
            return Some(text[start_byte..].to_string());
        }

        if start_char.is_ascii_alphabetic() || start_char == '_' {
            let start_byte = chars[*pos].0;
            while *pos < len
                && (chars[*pos].1.is_ascii_alphanumeric()
                    || chars[*pos].1 == '_'
                    || chars[*pos].1 == '$'
                    || chars[*pos].1 == '#')
            {
                *pos += 1;
            }
            let end_byte = if *pos < len {
                chars[*pos].0
            } else {
                text.len()
            };
            return Some(text[start_byte..end_byte].to_string());
        }

        None
    }

    /// Parse an optional alias after a table name.
    /// Skips AS keyword if present. Returns None if the next keyword is a
    /// SQL clause keyword (JOIN, ON, WHERE, etc.) or if no alias follows.
    #[allow(dead_code)]
    fn parse_optional_alias(
        chars: &[(usize, char)],
        text: &str,
        pos: &mut usize,
    ) -> Option<String> {
        let len = chars.len();
        if *pos >= len {
            return None;
        }

        // Check what follows the table name
        let c = chars[*pos].1;
        if c == ',' || c == ';' || c == ')' {
            return None;
        }

        // Try to read a word
        if c.is_ascii_alphabetic() || c == '_' || c == '"' {
            let is_quoted = c == '"';
            let save_pos = *pos;
            let word = Self::parse_identifier_at(chars, text, pos)?;

            // Quoted identifiers (e.g. "WHERE", "JOIN") are never keywords — always valid aliases
            if !is_quoted {
                let word_upper = word.to_ascii_uppercase();

                // If the word is a SQL keyword that terminates table references, it's not an alias
                if Self::is_from_stop_keyword(&word_upper) {
                    *pos = save_pos;
                    return None;
                }

                // "AS" keyword — skip it and read the actual alias
                if word_upper == "AS" {
                    while *pos < len && chars[*pos].1.is_whitespace() {
                        *pos += 1;
                    }
                    if *pos < len {
                        return Self::parse_identifier_at(chars, text, pos);
                    }
                    return None;
                }
            }

            // Otherwise this word is the alias
            return Some(word);
        }

        None
    }

    /// Check if a word is a keyword that terminates FROM clause table references.
    #[allow(dead_code)]
    fn is_from_stop_keyword(word_upper: &str) -> bool {
        matches!(
            word_upper,
            "WHERE"
                | "ORDER"
                | "GROUP"
                | "HAVING"
                | "FETCH"
                | "OFFSET"
                | "FOR"
                | "UNION"
                | "INTERSECT"
                | "MINUS"
                | "EXCEPT"
                | "CONNECT"
                | "START"
                | "PIVOT"
                | "UNPIVOT"
                | "MODEL"
                | "RETURNING"
                | "JOIN"
                | "INNER"
                | "LEFT"
                | "RIGHT"
                | "FULL"
                | "CROSS"
                | "NATURAL"
                | "ON"
                | "USING"
                | "PARTITION"
                | "SAMPLE"
                | "LATERAL"
        )
    }

    fn find_leading_wildcard_in_select_list(select_body: &str) -> Option<(usize, usize)> {
        let mut chars = select_body.char_indices().peekable();
        let mut in_line_comment = false;
        let mut in_block_comment = false;

        while let Some((byte_idx, c)) = chars.next() {
            let next = chars.peek().map(|(_, ch)| *ch);

            if in_line_comment {
                if c == '\n' {
                    in_line_comment = false;
                }
                continue;
            }

            if in_block_comment {
                if c == '*' && next == Some('/') {
                    in_block_comment = false;
                    chars.next(); // consume '/'
                }
                continue;
            }

            if c == '-' && next == Some('-') {
                in_line_comment = true;
                chars.next(); // consume second '-'
                continue;
            }

            if c == '/' && next == Some('*') {
                in_block_comment = true;
                chars.next(); // consume '*'
                continue;
            }

            if c.is_whitespace() {
                continue;
            }

            if c == '*' {
                let wildcard_end = byte_idx.saturating_add(c.len_utf8());
                if select_body.is_char_boundary(byte_idx)
                    && select_body.is_char_boundary(wildcard_end)
                {
                    return Some((byte_idx, wildcard_end));
                }
                return None;
            }

            return None;
        }

        None
    }

    fn select_clause_has_distinct_or_unique(sql: &str, select_idx: usize, from_idx: usize) -> bool {
        let select_keyword_end = select_idx.saturating_add("SELECT".len());
        if select_keyword_end >= from_idx || select_keyword_end >= sql.len() {
            return false;
        }

        let chars: Vec<(usize, char)> = sql.char_indices().collect();
        let len = chars.len();
        let mut i = 0usize;
        while i < len && chars[i].0 < select_keyword_end {
            i += 1;
        }

        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false;

        while i < len {
            let (byte_idx, c) = chars[i];
            if byte_idx >= from_idx {
                break;
            }
            let next = chars.get(i + 1).map(|(_, ch)| *ch);

            if in_line_comment {
                if c == '\n' {
                    in_line_comment = false;
                }
                i += 1;
                continue;
            }

            if in_block_comment {
                if c == '*' && next == Some('/') {
                    in_block_comment = false;
                    i += 2;
                    continue;
                }
                i += 1;
                continue;
            }

            if in_single_quote {
                if c == '\'' {
                    if next == Some('\'') {
                        i += 2;
                        continue;
                    }
                    in_single_quote = false;
                }
                i += 1;
                continue;
            }

            if in_double_quote {
                if c == '"' {
                    if next == Some('"') {
                        i += 2;
                        continue;
                    }
                    in_double_quote = false;
                }
                i += 1;
                continue;
            }

            if c == '-' && next == Some('-') {
                in_line_comment = true;
                i += 2;
                continue;
            }
            if c == '/' && next == Some('*') {
                in_block_comment = true;
                i += 2;
                continue;
            }
            if c == '\'' {
                in_single_quote = true;
                i += 1;
                continue;
            }
            if c == '"' {
                in_double_quote = true;
                i += 1;
                continue;
            }

            if c.is_whitespace() {
                i += 1;
                continue;
            }

            if c.is_ascii_alphabetic() {
                let start_byte = byte_idx;
                let mut end_i = i;
                while end_i < len && chars[end_i].0 < from_idx {
                    let token_char = chars[end_i].1;
                    if !token_char.is_ascii_alphanumeric() && token_char != '_' {
                        break;
                    }
                    end_i += 1;
                }
                let end_byte = if end_i < len {
                    chars[end_i].0
                } else {
                    sql.len()
                };
                if !sql.is_char_boundary(start_byte) || !sql.is_char_boundary(end_byte) {
                    return false;
                }

                let token_upper = sql[start_byte..end_byte].to_ascii_uppercase();
                if token_upper == "DISTINCT" || token_upper == "UNIQUE" {
                    return true;
                }
                return false;
            }

            return false;
        }

        false
    }

    fn find_select_body_start(sql: &str) -> Option<usize> {
        let select_idx = Self::find_top_level_keyword(sql, "SELECT")?;
        let select_end = select_idx.saturating_add("SELECT".len());
        let mut idx = Self::skip_select_prefix_whitespace_and_hint(sql, select_end);

        for modifier in ["DISTINCT", "UNIQUE", "ALL"] {
            if Self::starts_with_keyword_at(sql, idx, modifier) {
                idx = idx.saturating_add(modifier.len());
                idx = Self::skip_ascii_whitespace(sql, idx);
                break;
            }
        }

        Some(idx.min(sql.len()))
    }

    fn skip_ascii_whitespace(sql: &str, mut idx: usize) -> usize {
        while idx < sql.len() {
            let Some(slice) = sql.get(idx..) else {
                break;
            };
            let Some((_, ch)) = slice.char_indices().next() else {
                break;
            };
            if !ch.is_whitespace() {
                break;
            }
            idx = idx.saturating_add(ch.len_utf8());
        }

        idx
    }

    fn skip_select_prefix_whitespace_and_hint(sql: &str, start_idx: usize) -> usize {
        let mut idx = Self::skip_ascii_whitespace(sql, start_idx);
        let Some(after_prefix) = sql.get(idx..) else {
            return sql.len();
        };

        if !after_prefix.starts_with("/*+") {
            return idx;
        }

        if let Some(end_rel) = after_prefix.find("*/") {
            idx = idx.saturating_add(end_rel + 2);
            idx = Self::skip_ascii_whitespace(sql, idx);
        }

        idx
    }

    fn starts_with_keyword_at(sql: &str, idx: usize, keyword: &str) -> bool {
        let Some(tail) = sql.get(idx..) else {
            return false;
        };

        if !tail
            .get(..keyword.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
        {
            return false;
        }

        let boundary_idx = idx.saturating_add(keyword.len());
        if boundary_idx >= sql.len() {
            return true;
        }

        sql.get(boundary_idx..)
            .and_then(|rest| rest.chars().next())
            .is_none_or(|ch| !ch.is_ascii_alphanumeric() && ch != '_')
    }

    fn find_top_level_keyword(sql: &str, keyword: &str) -> Option<usize> {
        let keyword_upper = keyword.to_ascii_uppercase();
        let chars: Vec<(usize, char)> = sql.char_indices().collect();
        let len = chars.len();

        let mut i = 0usize;
        let mut depth = 0usize;
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false;

        while i < len {
            let (_, c) = chars[i];
            let next = chars.get(i + 1).map(|(_, ch)| *ch);

            if in_line_comment {
                if c == '\n' {
                    in_line_comment = false;
                }
                i += 1;
                continue;
            }

            if in_block_comment {
                if c == '*' && next == Some('/') {
                    in_block_comment = false;
                    i += 2;
                    continue;
                }
                i += 1;
                continue;
            }

            if in_single_quote {
                if c == '\'' {
                    if next == Some('\'') {
                        i += 2;
                        continue;
                    }
                    in_single_quote = false;
                }
                i += 1;
                continue;
            }

            if in_double_quote {
                if c == '"' {
                    if next == Some('"') {
                        i += 2;
                        continue;
                    }
                    in_double_quote = false;
                }
                i += 1;
                continue;
            }

            if c == '-' && next == Some('-') {
                in_line_comment = true;
                i += 2;
                continue;
            }
            if c == '/' && next == Some('*') {
                in_block_comment = true;
                i += 2;
                continue;
            }
            if c == '\'' {
                in_single_quote = true;
                i += 1;
                continue;
            }
            if c == '"' {
                in_double_quote = true;
                i += 1;
                continue;
            }

            if c == '(' {
                depth = depth.saturating_add(1);
                i += 1;
                continue;
            }
            if c == ')' {
                depth = depth.saturating_sub(1);
                i += 1;
                continue;
            }

            if depth == 0 && c.is_ascii_alphabetic() {
                let start_byte = chars[i].0;
                let mut end_i = i;
                while end_i < len && chars[end_i].1.is_ascii_alphanumeric() {
                    end_i += 1;
                }
                let end_byte = if end_i < len {
                    chars[end_i].0
                } else {
                    sql.len()
                };
                if sql[start_byte..end_byte].to_ascii_uppercase() == keyword_upper {
                    return Some(start_byte);
                }
                i = end_i;
                continue;
            }

            i += 1;
        }

        None
    }

    fn single_table_rowid_expression(sql: &str, from_idx: usize) -> Option<String> {
        let from_body_start = from_idx.saturating_add("FROM".len());
        if from_body_start >= sql.len() || !sql.is_char_boundary(from_body_start) {
            return None;
        }

        let from_body_end = Self::find_top_level_keyword(sql, "WHERE")
            .or_else(|| Self::find_top_level_keyword(sql, "ORDER"))
            .or_else(|| Self::find_top_level_keyword(sql, "FETCH"))
            .or_else(|| Self::find_top_level_keyword(sql, "OFFSET"))
            .or_else(|| Self::find_top_level_keyword(sql, "FOR"))
            .unwrap_or(sql.len());
        if from_body_end <= from_body_start || !sql.is_char_boundary(from_body_end) {
            return None;
        }

        let from_clause = &sql[from_body_start..from_body_end];
        let table_ref = Self::strip_leading_relation_modifiers(from_clause);
        if table_ref.is_empty() {
            return None;
        }
        if Self::starts_with_relation_invocation(table_ref) {
            return None;
        }

        let upper = table_ref.to_ascii_uppercase();
        if upper.starts_with("(") {
            return None;
        }

        let alias_start = if upper.starts_with('"') {
            let bytes = table_ref.as_bytes();
            let mut idx = 1usize;
            while idx < bytes.len() {
                if bytes[idx] == b'"' {
                    if idx + 1 < bytes.len() && bytes[idx + 1] == b'"' {
                        idx += 2;
                        continue;
                    }
                    idx += 1;
                    break;
                }
                idx += 1;
            }
            idx
        } else {
            table_ref
                .char_indices()
                .find_map(|(idx, ch)| {
                    if ch.is_whitespace() || ch == ';' {
                        Some(idx)
                    } else {
                        None
                    }
                })
                .unwrap_or(table_ref.len())
        };

        let table_name = table_ref
            .get(..alias_start)?
            .trim_end_matches(';')
            .trim_end();
        if table_name.is_empty() {
            return None;
        }

        let mut alias_source = table_ref.get(alias_start..).unwrap_or("").trim_start();
        if alias_source.is_empty() {
            return Some(format!("{table_name}.ROWID"));
        }

        if alias_source
            .get(..2)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("AS"))
        {
            alias_source = alias_source.get(2..).unwrap_or("").trim_start();
        }

        let alias_len = alias_source
            .char_indices()
            .find_map(|(idx, ch)| {
                if ch.is_whitespace() || ch == ';' {
                    Some(idx)
                } else {
                    None
                }
            })
            .unwrap_or(alias_source.len());
        let alias = alias_source.get(..alias_len).unwrap_or("").trim();

        if alias.is_empty() {
            Some(format!("{table_name}.ROWID"))
        } else {
            Some(format!("{alias}.ROWID"))
        }
    }

    fn is_single_table_from_clause(sql: &str, from_idx: usize) -> bool {
        let from_body_start = from_idx.saturating_add("FROM".len());
        if from_body_start >= sql.len() || !sql.is_char_boundary(from_body_start) {
            return false;
        }

        let from_body_end = Self::find_top_level_keyword(sql, "WHERE")
            .or_else(|| Self::find_top_level_keyword(sql, "ORDER"))
            .or_else(|| Self::find_top_level_keyword(sql, "FETCH"))
            .or_else(|| Self::find_top_level_keyword(sql, "OFFSET"))
            .or_else(|| Self::find_top_level_keyword(sql, "FOR"))
            .unwrap_or(sql.len());
        if from_body_end <= from_body_start || !sql.is_char_boundary(from_body_end) {
            return false;
        }

        let from_clause = &sql[from_body_start..from_body_end];
        let relation_head = Self::strip_leading_relation_modifiers(from_clause);
        if Self::starts_with_relation_invocation(relation_head) {
            return false;
        }
        let clause_upper = from_clause.to_ascii_uppercase();
        if clause_upper.contains(" JOIN ") {
            return false;
        }

        let mut chars = from_clause.char_indices().peekable();
        let mut depth = 0usize;
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false;

        while let Some((_, c)) = chars.next() {
            let next = chars.peek().map(|(_, ch)| *ch);

            if in_line_comment {
                if c == '\n' {
                    in_line_comment = false;
                }
                continue;
            }

            if in_block_comment {
                if c == '*' && next == Some('/') {
                    in_block_comment = false;
                    chars.next(); // consume '/'
                }
                continue;
            }

            if in_single_quote {
                if c == '\'' {
                    if next == Some('\'') {
                        chars.next(); // consume escaped quote
                        continue;
                    }
                    in_single_quote = false;
                }
                continue;
            }

            if in_double_quote {
                if c == '"' {
                    if next == Some('"') {
                        chars.next(); // consume escaped quote
                        continue;
                    }
                    in_double_quote = false;
                }
                continue;
            }

            if c == '-' && next == Some('-') {
                in_line_comment = true;
                chars.next(); // consume second '-'
                continue;
            }
            if c == '/' && next == Some('*') {
                in_block_comment = true;
                chars.next(); // consume '*'
                continue;
            }
            if c == '\'' {
                in_single_quote = true;
                continue;
            }
            if c == '"' {
                in_double_quote = true;
                continue;
            }

            if c == '(' {
                depth = depth.saturating_add(1);
                continue;
            }
            if c == ')' {
                depth = depth.saturating_sub(1);
                continue;
            }

            if depth == 0 && c == ',' {
                return false;
            }
        }

        true
    }

    fn strip_leading_relation_modifiers(text: &str) -> &str {
        let mut trimmed = text.trim_start();
        loop {
            let Some(rest) = Self::strip_leading_keyword(trimmed, "LATERAL")
                .or_else(|| Self::strip_leading_keyword(trimmed, "ONLY"))
            else {
                break;
            };
            trimmed = rest.trim_start();
        }
        trimmed
    }

    fn strip_leading_keyword<'a>(text: &'a str, keyword: &str) -> Option<&'a str> {
        let trimmed = text.trim_start();
        if trimmed.len() < keyword.len() {
            return None;
        }
        let prefix = trimmed.get(..keyword.len())?;
        if !prefix.eq_ignore_ascii_case(keyword) {
            return None;
        }
        let rest = trimmed.get(keyword.len()..).unwrap_or("");
        let next = rest.chars().next();
        if next.is_some_and(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '$' | '#')) {
            return None;
        }
        Some(rest)
    }

    fn starts_with_relation_invocation(text: &str) -> bool {
        let trimmed = text.trim_start();
        if trimmed.is_empty() {
            return false;
        }

        let chars: Vec<(usize, char)> = trimmed.char_indices().collect();
        let len = chars.len();
        if chars.first().is_some_and(|(_, ch)| *ch == '(') {
            return true;
        }

        let mut pos = 0usize;
        if Self::parse_identifier_at(&chars, trimmed, &mut pos).is_none() {
            return false;
        }

        loop {
            while pos < len && chars[pos].1.is_whitespace() {
                pos += 1;
            }
            if pos >= len || chars[pos].1 != '.' {
                break;
            }
            pos += 1;
            while pos < len && chars[pos].1.is_whitespace() {
                pos += 1;
            }
            if Self::parse_identifier_at(&chars, trimmed, &mut pos).is_none() {
                return false;
            }
        }

        while pos < len && chars[pos].1.is_whitespace() {
            pos += 1;
        }

        pos < len && chars[pos].1 == '('
    }

    fn with_clause_starts_with_select(sql: &str) -> bool {
        let stripped = Self::strip_leading_comments(sql);
        let chars: Vec<char> = stripped.chars().collect();
        let len = chars.len();

        let mut i = 0usize;
        let mut depth = 0usize;
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false;
        let mut in_q_quote = false;
        let mut q_quote_end: Option<char> = None;

        while i < len {
            let c = chars[i];
            let next = chars.get(i + 1).copied();

            if in_line_comment {
                if c == '\n' {
                    in_line_comment = false;
                }
                i += 1;
                continue;
            }

            if in_block_comment {
                if c == '*' && next == Some('/') {
                    in_block_comment = false;
                    i += 2;
                    continue;
                }
                i += 1;
                continue;
            }

            if in_q_quote {
                if Some(c) == q_quote_end && next == Some('\'') {
                    in_q_quote = false;
                    q_quote_end = None;
                    i += 2;
                    continue;
                }
                i += 1;
                continue;
            }

            if in_single_quote {
                if c == '\'' {
                    if next == Some('\'') {
                        i += 2;
                        continue;
                    }
                    in_single_quote = false;
                }
                i += 1;
                continue;
            }

            if in_double_quote {
                if c == '"' {
                    if next == Some('"') {
                        i += 2;
                        continue;
                    }
                    in_double_quote = false;
                }
                i += 1;
                continue;
            }

            if c == '-' && next == Some('-') {
                in_line_comment = true;
                i += 2;
                continue;
            }

            if c == '/' && next == Some('*') {
                in_block_comment = true;
                i += 2;
                continue;
            }

            if (c == 'n' || c == 'N')
                && matches!(next, Some('q') | Some('Q'))
                && chars.get(i + 2) == Some(&'\'')
            {
                if let Some(&delimiter) = chars.get(i + 3) {
                    in_q_quote = true;
                    q_quote_end = Some(sql_text::q_quote_closing(delimiter));
                    i += 4;
                    continue;
                }
            }

            if (c == 'q' || c == 'Q') && next == Some('\'') {
                if let Some(&delimiter) = chars.get(i + 2) {
                    in_q_quote = true;
                    q_quote_end = Some(sql_text::q_quote_closing(delimiter));
                    i += 3;
                    continue;
                }
            }

            if c == '\'' {
                in_single_quote = true;
                i += 1;
                continue;
            }

            if c == '"' {
                in_double_quote = true;
                i += 1;
                continue;
            }

            if c == '(' {
                depth += 1;
                i += 1;
                continue;
            }

            if c == ')' {
                depth = depth.saturating_sub(1);
                i += 1;
                continue;
            }

            if depth == 0 && (c.is_ascii_alphabetic() || c == '_') {
                let start = i;
                i += 1;
                while i < len
                    && (chars[i].is_ascii_alphanumeric()
                        || chars[i] == '_'
                        || chars[i] == '$'
                        || chars[i] == '#')
                {
                    i += 1;
                }
                let token: String = chars[start..i].iter().collect();
                if token.eq_ignore_ascii_case("SELECT") {
                    return true;
                }
                if token.eq_ignore_ascii_case("INSERT")
                    || token.eq_ignore_ascii_case("UPDATE")
                    || token.eq_ignore_ascii_case("DELETE")
                    || token.eq_ignore_ascii_case("MERGE")
                {
                    return false;
                }
                continue;
            }

            i += 1;
        }

        false
    }

    pub fn split_script_items(sql: &str) -> Vec<ScriptItem> {
        let mut items: Vec<ScriptItem> = Vec::new();
        let mut builder = StatementBuilder::new();
        let mut sqlblanklines_enabled = true;

        // Helper to add statement with comment stripping and extra semicolon removal
        let add_statement = |stmt: String, items: &mut Vec<ScriptItem>| {
            let stripped = Self::strip_comments(&stmt);
            let cleaned = Self::strip_extra_trailing_semicolons(&stripped);
            if !cleaned.is_empty() {
                items.push(ScriptItem::Statement(cleaned));
            }
        };

        for line in sql.lines() {
            let trimmed = line.trim();
            let trimmed_upper = trimmed.to_ascii_uppercase();

            if !sqlblanklines_enabled
                && trimmed.is_empty()
                && builder.is_idle()
                && builder.block_depth() == 0
                && !builder.current_is_empty()
            {
                builder.force_terminate();
                for stmt in builder.take_statements() {
                    add_statement(stmt, &mut items);
                }
                continue;
            }

            // TRIGGER 헤더에서는 INSERT/UPDATE/DELETE/SELECT 등이 이벤트 타입으로
            // block_depth == 0 상태에서 나올 수 있으므로, TRIGGER의 block_depth == 0 구간에서는
            // 이 강제 종료를 건너뜀
            if builder.is_idle()
                && builder.in_create_plsql()
                && builder.block_depth() == 0
                && !builder.current_is_empty()
                && !builder.is_trigger()
                && (trimmed_upper.starts_with("CREATE")
                    || trimmed_upper.starts_with("ALTER")
                    || trimmed_upper.starts_with("DROP")
                    || trimmed_upper.starts_with("TRUNCATE")
                    || trimmed_upper.starts_with("GRANT")
                    || trimmed_upper.starts_with("REVOKE")
                    || trimmed_upper.starts_with("COMMIT")
                    || trimmed_upper.starts_with("ROLLBACK")
                    || trimmed_upper.starts_with("SAVEPOINT")
                    || trimmed_upper.starts_with("SELECT")
                    || trimmed_upper.starts_with("INSERT")
                    || trimmed_upper.starts_with("UPDATE")
                    || trimmed_upper.starts_with("DELETE")
                    || trimmed_upper.starts_with("MERGE")
                    || trimmed_upper.starts_with("WITH"))
            {
                builder.force_terminate();
                for stmt in builder.take_statements() {
                    add_statement(stmt, &mut items);
                }
            }

            if builder.is_idle() && trimmed == "/" && builder.block_depth() == 0 {
                if !builder.current_is_empty() {
                    builder.force_terminate();
                    for stmt in builder.take_statements() {
                        add_statement(stmt, &mut items);
                    }
                }
                continue;
            }

            // Handle lone semicolon line after CREATE PL/SQL statement
            // This prevents ";;" issue when extra ";" is on its own line
            if builder.is_idle()
                && trimmed == ";"
                && builder.in_create_plsql()
                && builder.block_depth() == 0
                && !builder.current_is_empty()
            {
                builder.force_terminate();
                for stmt in builder.take_statements() {
                    add_statement(stmt, &mut items);
                }
                continue;
            }

            let is_alter_session_set_clause = builder.starts_with_alter_session()
                && (trimmed_upper == "SET" || trimmed_upper.starts_with("SET "));
            if builder.is_idle()
                && !builder.current_is_empty()
                && builder.block_depth() == 0
                && !is_alter_session_set_clause
            {
                if let Some(command) = Self::parse_tool_command(trimmed) {
                    builder.force_terminate();
                    for stmt in builder.take_statements() {
                        add_statement(stmt, &mut items);
                    }
                    if let ToolCommand::SetSqlBlankLines { enabled } = &command {
                        sqlblanklines_enabled = *enabled;
                    }
                    items.push(ScriptItem::ToolCommand(command));
                    continue;
                }
            }

            if builder.is_idle() && builder.current_is_empty() && builder.block_depth() == 0 {
                if let Some(command) = Self::parse_tool_command(trimmed) {
                    if let ToolCommand::SetSqlBlankLines { enabled } = &command {
                        sqlblanklines_enabled = *enabled;
                    }
                    items.push(ScriptItem::ToolCommand(command));
                    continue;
                }
            }

            let mut line_with_newline = String::from(line);
            line_with_newline.push('\n');
            builder.process_text(&line_with_newline);
            for stmt in builder.take_statements() {
                add_statement(stmt, &mut items);
            }
        }

        builder.finalize();
        for stmt in builder.take_statements() {
            add_statement(stmt, &mut items);
        }

        items
    }

    pub fn split_format_items(sql: &str) -> Vec<FormatItem> {
        let mut items: Vec<FormatItem> = Vec::new();
        let mut builder = StatementBuilder::new();
        let mut sqlblanklines_enabled = true;

        let add_statement = |stmt: String, items: &mut Vec<FormatItem>| {
            let cleaned = stmt.trim();
            if !cleaned.is_empty() {
                items.push(FormatItem::Statement(cleaned.to_string()));
            }
        };

        let mut lines = sql.lines().peekable();
        while let Some(line) = lines.next() {
            let trimmed = line.trim();
            let trimmed_upper = trimmed.to_ascii_uppercase();
            let is_remark_line = Self::is_sqlplus_comment_line(trimmed);

            if !sqlblanklines_enabled
                && trimmed.is_empty()
                && builder.is_idle()
                && builder.block_depth() == 0
                && !builder.current_is_empty()
            {
                builder.force_terminate();
                for stmt in builder.take_statements() {
                    add_statement(stmt, &mut items);
                }
                continue;
            }

            if builder.is_idle() && builder.current_is_empty() {
                if trimmed.starts_with("--") {
                    items.push(FormatItem::Statement(line.to_string()));
                    continue;
                }
                if is_remark_line {
                    items.push(FormatItem::Statement(line.to_string()));
                    continue;
                }
                if trimmed.starts_with("/*") {
                    let mut comment = String::new();
                    comment.push_str(line);
                    if !trimmed.contains("*/") {
                        for next_line in lines.by_ref() {
                            comment.push('\n');
                            comment.push_str(next_line);
                            if next_line.contains("*/") {
                                break;
                            }
                        }
                    }
                    items.push(FormatItem::Statement(comment));
                    continue;
                }
            }

            if builder.is_idle()
                && builder.in_create_plsql()
                && builder.block_depth() == 0
                && !builder.current_is_empty()
                && !builder.is_trigger()
                && (trimmed_upper.starts_with("CREATE")
                    || trimmed_upper.starts_with("ALTER")
                    || trimmed_upper.starts_with("DROP")
                    || trimmed_upper.starts_with("TRUNCATE")
                    || trimmed_upper.starts_with("GRANT")
                    || trimmed_upper.starts_with("REVOKE")
                    || trimmed_upper.starts_with("COMMIT")
                    || trimmed_upper.starts_with("ROLLBACK")
                    || trimmed_upper.starts_with("SAVEPOINT")
                    || trimmed_upper.starts_with("SELECT")
                    || trimmed_upper.starts_with("INSERT")
                    || trimmed_upper.starts_with("UPDATE")
                    || trimmed_upper.starts_with("DELETE")
                    || trimmed_upper.starts_with("MERGE")
                    || trimmed_upper.starts_with("WITH"))
            {
                builder.force_terminate();
                for stmt in builder.take_statements() {
                    add_statement(stmt, &mut items);
                }
            }

            if builder.is_idle() && trimmed == "/" && builder.block_depth() == 0 {
                if !builder.current_is_empty() {
                    builder.force_terminate();
                    for stmt in builder.take_statements() {
                        add_statement(stmt, &mut items);
                    }
                }
                items.push(FormatItem::Slash);
                continue;
            }

            if builder.is_idle()
                && trimmed == ";"
                && builder.in_create_plsql()
                && builder.block_depth() == 0
                && !builder.current_is_empty()
            {
                builder.force_terminate();
                for stmt in builder.take_statements() {
                    add_statement(stmt, &mut items);
                }
                continue;
            }

            let is_alter_session_set_clause = builder.starts_with_alter_session()
                && (trimmed_upper == "SET" || trimmed_upper.starts_with("SET "));
            if builder.is_idle()
                && !builder.current_is_empty()
                && builder.block_depth() == 0
                && !is_alter_session_set_clause
            {
                if let Some(command) = Self::parse_tool_command(trimmed) {
                    builder.force_terminate();
                    for stmt in builder.take_statements() {
                        add_statement(stmt, &mut items);
                    }
                    if let ToolCommand::SetSqlBlankLines { enabled } = &command {
                        sqlblanklines_enabled = *enabled;
                    }
                    items.push(FormatItem::ToolCommand(command));
                    continue;
                }
            }

            if builder.is_idle() && builder.current_is_empty() && builder.block_depth() == 0 {
                if let Some(command) = Self::parse_tool_command(trimmed) {
                    if let ToolCommand::SetSqlBlankLines { enabled } = &command {
                        sqlblanklines_enabled = *enabled;
                    }
                    items.push(FormatItem::ToolCommand(command));
                    continue;
                }
            }

            let mut line_with_newline = String::from(line);
            line_with_newline.push('\n');
            builder.process_text(&line_with_newline);
            for stmt in builder.take_statements() {
                add_statement(stmt, &mut items);
            }
        }

        builder.finalize();
        for stmt in builder.take_statements() {
            add_statement(stmt, &mut items);
        }

        items
    }

    pub fn parse_tool_command(line: &str) -> Option<ToolCommand> {
        let trimmed_line = line.trim();
        if trimmed_line.is_empty() {
            return None;
        }
        // PROMPT는 명령 본문 전체를 출력하므로, 문장 끝 세미콜론도 payload로 보존한다.
        // 다른 도구 명령은 SQL terminator 스타일 입력(예: SET ECHO ON;)을 허용하기 위해
        // 트레일링 세미콜론을 제거한다.
        let upper_line = trimmed_line.to_ascii_uppercase();
        let trimmed = if Self::is_word_command(&upper_line, "PROMPT") {
            trimmed_line
        } else {
            trimmed_line.trim_end_matches(';').trim()
        };
        if trimmed.is_empty() {
            return None;
        }

        let upper = trimmed.to_ascii_uppercase();

        if upper == "VAR" || upper.starts_with("VAR ") || upper.starts_with("VARIABLE ") {
            return Some(Self::parse_var_command(trimmed));
        }

        if Self::is_word_command(&upper, "PRINT") {
            let rest = trimmed[5..].trim();
            let name = if rest.is_empty() {
                None
            } else {
                Some(rest.trim_start_matches(':').to_string())
            };
            return Some(ToolCommand::Print { name });
        }

        if upper.starts_with("SET SERVEROUTPUT") {
            return Some(Self::parse_serveroutput_command(trimmed));
        }

        if upper.starts_with("SHOW ERRORS") {
            return Some(Self::parse_show_errors_command(trimmed));
        }

        if upper.starts_with("SHOW ") || upper == "SHOW" {
            return Some(Self::parse_show_command(trimmed));
        }

        if upper == "DESC"
            || upper.starts_with("DESC ")
            || upper == "DESCRIBE"
            || upper.starts_with("DESCRIBE ")
        {
            return Some(Self::parse_describe_command(trimmed));
        }

        if Self::is_word_command(&upper, "PROMPT") {
            let text = trimmed[6..].trim().to_string();
            return Some(ToolCommand::Prompt { text });
        }

        if Self::is_word_command(&upper, "PAUSE") {
            return Some(Self::parse_pause_command(trimmed));
        }

        if Self::is_word_command(&upper, "ACCEPT") {
            return Some(Self::parse_accept_command(trimmed));
        }

        if Self::is_word_command(&upper, "DEFINE") {
            let rest = trimmed.get(6..).unwrap_or_default().trim();
            if rest.is_empty() {
                // MATCH_RECOGNIZE DEFINE clause marker: keep it as SQL text.
                return None;
            }
            return Some(Self::parse_define_assign_command(trimmed));
        }

        if Self::is_word_command(&upper, "UNDEFINE") {
            return Some(Self::parse_undefine_command(trimmed));
        }

        if Self::is_word_command(&upper, "COLUMN") {
            return Some(Self::parse_column_new_value_command(trimmed));
        }

        if Self::is_word_command(&upper, "CLEAR") {
            return Some(Self::parse_clear_command(trimmed));
        }

        if Self::is_word_command(&upper, "BREAK") {
            return Some(Self::parse_break_command(trimmed));
        }

        if Self::is_word_command(&upper, "COMPUTE") {
            return Some(Self::parse_compute_command(trimmed));
        }

        if Self::is_word_command(&upper, "SPOOL") {
            return Some(Self::parse_spool_command(trimmed));
        }

        if upper.starts_with("SET ERRORCONTINUE") {
            return Some(Self::parse_errorcontinue_command(trimmed));
        }

        if upper.starts_with("SET AUTOCOMMIT") {
            return Some(Self::parse_autocommit_command(trimmed));
        }

        if upper.starts_with("SET DEFINE") {
            return Some(Self::parse_define_command(trimmed));
        }

        if upper.starts_with("SET SCAN") {
            return Some(Self::parse_scan_command(trimmed));
        }

        if upper.starts_with("SET VERIFY") {
            return Some(Self::parse_verify_command(trimmed));
        }

        if upper.starts_with("SET ECHO") {
            return Some(Self::parse_echo_command(trimmed));
        }

        if upper.starts_with("SET TIMING") {
            return Some(Self::parse_timing_command(trimmed));
        }

        if upper.starts_with("SET FEEDBACK") {
            return Some(Self::parse_feedback_command(trimmed));
        }

        if upper.starts_with("SET HEADING") {
            return Some(Self::parse_heading_command(trimmed));
        }

        if upper.starts_with("SET PAGESIZE") {
            return Some(Self::parse_pagesize_command(trimmed));
        }

        if upper.starts_with("SET LINESIZE") {
            return Some(Self::parse_linesize_command(trimmed));
        }

        if upper.starts_with("SET TRIMSPOOL") {
            return Some(Self::parse_trimspool_command(trimmed));
        }

        if upper.starts_with("SET TRIMOUT") {
            return Some(Self::parse_trimout_command(trimmed));
        }

        if upper.starts_with("SET SQLBLANKLINES") {
            return Some(Self::parse_sqlblanklines_command(trimmed));
        }

        if upper.starts_with("SET TAB") {
            return Some(Self::parse_tab_command(trimmed));
        }

        if upper.starts_with("SET COLSEP") {
            return Some(Self::parse_colsep_command(trimmed));
        }

        if upper.starts_with("SET NULL") {
            return Some(Self::parse_null_command(trimmed));
        }

        if trimmed.starts_with("@@")
            || trimmed.starts_with('@')
            || Self::is_start_script_command(trimmed)
        {
            return Some(Self::parse_script_command(trimmed));
        }

        if upper.starts_with("WHENEVER SQLERROR") {
            return Some(Self::parse_whenever_sqlerror_command(trimmed));
        }

        if upper.starts_with("WHENEVER OSERROR") {
            return Some(Self::parse_whenever_oserror_command(trimmed));
        }

        if upper == "EXIT" || upper.starts_with("EXIT ") {
            return Some(ToolCommand::Exit);
        }

        if upper == "QUIT" || upper.starts_with("QUIT ") {
            return Some(ToolCommand::Quit);
        }

        if (upper == "CONNECT"
            || (upper.starts_with("CONNECT ") && !upper.starts_with("CONNECT BY")))
            || upper.starts_with("CONN ")
        {
            return Some(Self::parse_connect_command(trimmed));
        }

        if upper == "DISCONNECT" || upper == "DISC" {
            return Some(ToolCommand::Disconnect);
        }

        None
    }

    fn parse_var_command(raw: &str) -> ToolCommand {
        let mut parts = raw.split_whitespace();
        let _ = parts.next(); // VAR or VARIABLE
        let name = parts.next().unwrap_or_default();
        let type_str = parts.collect::<Vec<&str>>().join(" ");

        if name.is_empty() || type_str.trim().is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "VAR requires a variable name and type.".to_string(),
                is_error: true,
            };
        }

        match Self::parse_bind_type(&type_str) {
            Ok(data_type) => ToolCommand::Var {
                name: name.trim_start_matches(':').to_string(),
                data_type,
            },
            Err(message) => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message,
                is_error: true,
            },
        }
    }

    fn parse_serveroutput_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 3 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET SERVEROUTPUT requires ON or OFF.".to_string(),
                is_error: true,
            };
        }

        let mode = tokens[2].to_ascii_uppercase();
        if mode == "OFF" {
            return ToolCommand::SetServerOutput {
                enabled: false,
                size: None,
                unlimited: false,
            };
        }

        if mode != "ON" {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET SERVEROUTPUT supports only ON or OFF.".to_string(),
                is_error: true,
            };
        }

        let mut size: Option<u32> = None;
        let mut unlimited = false;
        let mut idx = 3usize;
        while idx + 1 < tokens.len() {
            if tokens[idx].eq_ignore_ascii_case("SIZE") {
                let size_val = tokens[idx + 1];
                if size_val.eq_ignore_ascii_case("UNLIMITED") {
                    unlimited = true;
                } else {
                    match size_val.parse::<u32>() {
                        Ok(val) => size = Some(val),
                        Err(_) => {
                            return ToolCommand::Unsupported {
                                raw: raw.to_string(),
                                message: "SET SERVEROUTPUT SIZE must be a number or UNLIMITED."
                                    .to_string(),
                                is_error: true,
                            };
                        }
                    }
                }
                break;
            }
            idx += 1;
        }

        ToolCommand::SetServerOutput {
            enabled: true,
            size,
            unlimited,
        }
    }

    fn parse_show_errors_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() <= 2 {
            return ToolCommand::ShowErrors {
                object_type: None,
                object_name: None,
            };
        }

        let mut idx = 2usize;
        let mut object_type = tokens[idx].to_ascii_uppercase();
        if object_type == "PACKAGE"
            && tokens
                .get(idx + 1)
                .map(|t| t.eq_ignore_ascii_case("BODY"))
                .unwrap_or(false)
        {
            object_type = "PACKAGE BODY".to_string();
            idx += 2;
        } else if object_type == "TYPE"
            && tokens
                .get(idx + 1)
                .map(|t| t.eq_ignore_ascii_case("BODY"))
                .unwrap_or(false)
        {
            object_type = "TYPE BODY".to_string();
            idx += 2;
        } else {
            idx += 1;
        }

        let name = tokens
            .get(idx)
            .map(|v| v.trim_start_matches(':').to_string());
        if name.is_none() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SHOW ERRORS requires an object name when a type is specified."
                    .to_string(),
                is_error: true,
            };
        }

        ToolCommand::ShowErrors {
            object_type: Some(object_type),
            object_name: name,
        }
    }

    fn parse_show_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 2 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SHOW requires a topic (USER, ALL, ERRORS).".to_string(),
                is_error: true,
            };
        }

        let topic = tokens[1].to_ascii_uppercase();
        match topic.as_str() {
            "USER" => ToolCommand::ShowUser,
            "ALL" => ToolCommand::ShowAll,
            "ERRORS" => Self::parse_show_errors_command(raw),
            _ => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SHOW supports USER, ALL, or ERRORS.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_describe_command(raw: &str) -> ToolCommand {
        let mut parts = raw.splitn(2, char::is_whitespace);
        let _ = parts.next(); // DESC/DESCRIBE
        let target = parts.next().unwrap_or("").trim();
        if target.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "DESCRIBE requires an object name.".to_string(),
                is_error: true,
            };
        }
        ToolCommand::Describe {
            name: target.to_string(),
        }
    }

    fn parse_accept_command(raw: &str) -> ToolCommand {
        let rest = raw[6..].trim();
        if rest.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "ACCEPT requires a variable name.".to_string(),
                is_error: true,
            };
        }

        let mut parts = rest.splitn(2, char::is_whitespace);
        let name = parts.next().unwrap_or_default();
        if name.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "ACCEPT requires a variable name.".to_string(),
                is_error: true,
            };
        }
        let remainder = parts.next().unwrap_or("").trim();
        let prompt = if remainder.is_empty() {
            None
        } else {
            // Keep byte offsets aligned with `remainder` when slicing `prompt_raw`.
            let upper = remainder.to_ascii_uppercase();
            if let Some(idx) = upper.find("PROMPT") {
                let prompt_raw = remainder[idx + 6..].trim();
                let cleaned = prompt_raw.trim_matches('"').trim_matches('\'').to_string();
                if cleaned.is_empty() {
                    None
                } else {
                    Some(cleaned)
                }
            } else {
                None
            }
        };

        ToolCommand::Accept {
            name: name.trim_start_matches(':').to_string(),
            prompt,
        }
    }

    fn parse_pause_command(raw: &str) -> ToolCommand {
        let rest = raw[5..].trim();
        let message = if rest.is_empty() {
            None
        } else {
            Some(rest.to_string())
        };

        ToolCommand::Pause { message }
    }

    fn parse_define_assign_command(raw: &str) -> ToolCommand {
        let rest = raw[6..].trim();
        if rest.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "DEFINE requires a variable name and value.".to_string(),
                is_error: true,
            };
        }

        let (name, value) = if let Some(eq_idx) = rest.find('=') {
            let (left, right) = rest.split_at(eq_idx);
            (left.trim(), right.trim_start_matches('=').trim())
        } else {
            let mut parts = rest.splitn(2, char::is_whitespace);
            let name = parts.next().unwrap_or_default();
            let value = parts.next().unwrap_or("").trim();
            (name, value)
        };

        if name.is_empty() || value.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "DEFINE requires a variable name and value.".to_string(),
                is_error: true,
            };
        }

        ToolCommand::Define {
            name: name.trim_start_matches(':').to_string(),
            value: value.to_string(),
        }
    }

    fn parse_undefine_command(raw: &str) -> ToolCommand {
        let rest = raw[8..].trim();
        if rest.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "UNDEFINE requires a variable name.".to_string(),
                is_error: true,
            };
        }

        ToolCommand::Undefine {
            name: rest.trim_start_matches(':').to_string(),
        }
    }

    fn parse_column_new_value_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 4 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "COLUMN requires syntax: COLUMN <column> NEW_VALUE <variable>."
                    .to_string(),
                is_error: true,
            };
        }

        if !tokens[2].eq_ignore_ascii_case("NEW_VALUE") {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "Only COLUMN ... NEW_VALUE ... is supported.".to_string(),
                is_error: true,
            };
        }

        if tokens.len() > 4 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "COLUMN NEW_VALUE accepts exactly one column and one variable."
                    .to_string(),
                is_error: true,
            };
        }

        let column_name = tokens[1].trim();
        let variable_name = tokens[3].trim_start_matches(':').trim();
        if column_name.is_empty() || variable_name.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "COLUMN requires syntax: COLUMN <column> NEW_VALUE <variable>."
                    .to_string(),
                is_error: true,
            };
        }

        ToolCommand::ColumnNewValue {
            column_name: column_name.to_string(),
            variable_name: variable_name.to_string(),
        }
    }

    fn parse_break_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 2 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "BREAK requires ON <column> or OFF.".to_string(),
                is_error: true,
            };
        }

        if tokens[1].eq_ignore_ascii_case("OFF") {
            return ToolCommand::BreakOff;
        }

        if tokens.len() == 3 && tokens[1].eq_ignore_ascii_case("ON") {
            let column_name = tokens[2].trim();
            if column_name.is_empty() {
                return ToolCommand::Unsupported {
                    raw: raw.to_string(),
                    message: "BREAK ON requires a column name.".to_string(),
                    is_error: true,
                };
            }
            return ToolCommand::BreakOn {
                column_name: column_name.to_string(),
            };
        }

        ToolCommand::Unsupported {
            raw: raw.to_string(),
            message: "BREAK supports only: BREAK ON <column> or BREAK OFF.".to_string(),
            is_error: true,
        }
    }

    fn parse_clear_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 2 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message:
                    "CLEAR supports: CLEAR BREAKS, CLEAR COMPUTES, CLEAR BREAKS CLEAR COMPUTES."
                        .to_string(),
                is_error: true,
            };
        }

        if tokens.len() == 2 && tokens[1].eq_ignore_ascii_case("BREAKS") {
            return ToolCommand::ClearBreaks;
        }

        if tokens.len() == 2 && tokens[1].eq_ignore_ascii_case("COMPUTES") {
            return ToolCommand::ClearComputes;
        }

        let is_breaks_computes = tokens.len() == 4
            && tokens[1].eq_ignore_ascii_case("BREAKS")
            && tokens[2].eq_ignore_ascii_case("CLEAR")
            && tokens[3].eq_ignore_ascii_case("COMPUTES");
        let is_computes_breaks = tokens.len() == 4
            && tokens[1].eq_ignore_ascii_case("COMPUTES")
            && tokens[2].eq_ignore_ascii_case("CLEAR")
            && tokens[3].eq_ignore_ascii_case("BREAKS");

        if is_breaks_computes || is_computes_breaks {
            return ToolCommand::ClearBreaksComputes;
        }

        ToolCommand::Unsupported {
            raw: raw.to_string(),
            message: "CLEAR supports: CLEAR BREAKS, CLEAR COMPUTES, CLEAR BREAKS CLEAR COMPUTES."
                .to_string(),
            is_error: true,
        }
    }

    fn parse_compute_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 2 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "COMPUTE requires SUM, COUNT, or OFF.".to_string(),
                is_error: true,
            };
        }

        match tokens[1].to_ascii_uppercase().as_str() {
            "SUM" | "COUNT" => {
                let mode = if tokens[1].eq_ignore_ascii_case("SUM") {
                    ComputeMode::Sum
                } else {
                    ComputeMode::Count
                };
                if tokens.len() == 2 {
                    return ToolCommand::Compute {
                        mode,
                        of_column: None,
                        on_column: None,
                    };
                }
                if tokens.len() == 6
                    && tokens[2].eq_ignore_ascii_case("OF")
                    && tokens[4].eq_ignore_ascii_case("ON")
                {
                    let of_column = tokens[3].trim();
                    let on_column = tokens[5].trim();
                    if of_column.is_empty() || on_column.is_empty() {
                        return ToolCommand::Unsupported {
                            raw: raw.to_string(),
                            message: "COMPUTE <SUM|COUNT> OF <column> ON <group_column>."
                                .to_string(),
                            is_error: true,
                        };
                    }
                    return ToolCommand::Compute {
                        mode,
                        of_column: Some(of_column.to_string()),
                        on_column: Some(on_column.to_string()),
                    };
                }
                ToolCommand::Unsupported {
                    raw: raw.to_string(),
                    message: "COMPUTE supports: COMPUTE SUM, COMPUTE COUNT, COMPUTE OFF, COMPUTE <SUM|COUNT> OF <column> ON <group_column>.".to_string(),
                    is_error: true,
                }
            }
            "OFF" => ToolCommand::ComputeOff,
            _ => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "COMPUTE requires SUM, COUNT, or OFF.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_spool_command(raw: &str) -> ToolCommand {
        let rest = raw[5..].trim();
        if rest.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SPOOL requires a file path, APPEND, or OFF.".to_string(),
                is_error: true,
            };
        }

        if rest.eq_ignore_ascii_case("OFF") {
            return ToolCommand::Spool {
                path: None,
                append: false,
            };
        }

        if rest.eq_ignore_ascii_case("APPEND") {
            return ToolCommand::Spool {
                path: None,
                append: true,
            };
        }

        let mut append = false;
        let path_part = if rest.to_ascii_uppercase().ends_with(" APPEND") {
            append = true;
            rest[..rest.len() - "APPEND".len()].trim()
        } else {
            rest
        };

        let cleaned = path_part.trim_matches('"').trim_matches('\'').to_string();
        if cleaned.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SPOOL requires a file path.".to_string(),
                is_error: true,
            };
        }

        ToolCommand::Spool {
            path: Some(cleaned),
            append,
        }
    }

    fn parse_whenever_sqlerror_command(raw: &str) -> ToolCommand {
        let rest = raw[17..].trim();
        if rest.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "WHENEVER SQLERROR requires EXIT or CONTINUE.".to_string(),
                is_error: true,
            };
        }
        let mut parts = rest.splitn(2, char::is_whitespace);
        let token_raw = parts.next().unwrap_or("");
        let token = token_raw.to_ascii_uppercase();
        let action = parts
            .next()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string());
        match token.as_str() {
            "EXIT" => ToolCommand::WheneverSqlError { exit: true, action },
            "CONTINUE" => ToolCommand::WheneverSqlError {
                exit: false,
                action,
            },
            _ => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "WHENEVER SQLERROR supports EXIT or CONTINUE.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_whenever_oserror_command(raw: &str) -> ToolCommand {
        let rest = raw[16..].trim();
        if rest.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "WHENEVER OSERROR requires EXIT or CONTINUE.".to_string(),
                is_error: true,
            };
        }

        let mut parts = rest.splitn(2, char::is_whitespace);
        let token = parts.next().unwrap_or("").to_ascii_uppercase();
        let extra = parts.next().map(str::trim).unwrap_or("");

        if !extra.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "WHENEVER OSERROR supports only EXIT or CONTINUE.".to_string(),
                is_error: true,
            };
        }

        match token.as_str() {
            "EXIT" => ToolCommand::WheneverOsError { exit: true },
            "CONTINUE" => ToolCommand::WheneverOsError { exit: false },
            _ => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "WHENEVER OSERROR supports EXIT or CONTINUE.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_connect_command(raw: &str) -> ToolCommand {
        // CONNECT syntax: CONNECT user/password@host:port/service_name
        // or: CONNECT user/password@//host:port/service_name
        let raw_upper = raw.to_ascii_uppercase();
        let rest = if raw_upper.starts_with("CONNECT") {
            raw[7..].trim()
        } else if raw_upper.starts_with("CONN") {
            raw[4..].trim()
        } else {
            raw.trim()
        };

        if rest.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "CONNECT requires connection string: user/password@host:port/service_name"
                    .to_string(),
                is_error: true,
            };
        }

        // Split by the last @ so passwords containing @ are preserved.
        let Some((credentials_raw, conn_str_raw)) = rest.rsplit_once('@') else {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "Invalid CONNECT syntax. Expected: user/password@host:port/service_name"
                    .to_string(),
                is_error: true,
            };
        };

        // Parse credentials (user/password)
        // Split at the first slash so passwords containing / are preserved.
        let Some((username_raw, password_raw)) = credentials_raw.split_once('/') else {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "Invalid credentials. Expected: user/password".to_string(),
                is_error: true,
            };
        };

        let username = username_raw.trim().to_string();
        let password = password_raw.trim().to_string();

        if username.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "Username cannot be empty".to_string(),
                is_error: true,
            };
        }

        // Parse connection string (//host:port/service_name or host:port/service_name)
        let conn_str = conn_str_raw.trim();
        let conn_str = conn_str.strip_prefix("//").unwrap_or(conn_str);

        // Split by / to separate host:port from service_name
        let conn_parts: Vec<&str> = conn_str.splitn(2, '/').collect();
        if conn_parts.len() != 2 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "Invalid connection string. Expected: host:port/service_name".to_string(),
                is_error: true,
            };
        }

        let service_name = conn_parts[1].trim().to_string();

        // Parse host:port
        let host_port: Vec<&str> = conn_parts[0].splitn(2, ':').collect();
        let host = host_port[0].trim().to_string();
        let port = if host_port.len() == 2 {
            match host_port[1].trim().parse::<u16>() {
                Ok(p) => p,
                Err(_) => {
                    return ToolCommand::Unsupported {
                        raw: raw.to_string(),
                        message: "Invalid port number".to_string(),
                        is_error: true,
                    };
                }
            }
        } else {
            1521 // Default Oracle port
        };

        if host.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "Host cannot be empty".to_string(),
                is_error: true,
            };
        }

        if service_name.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "Service name cannot be empty".to_string(),
                is_error: true,
            };
        }

        ToolCommand::Connect {
            username,
            password,
            host,
            port,
            service_name,
        }
    }

    fn parse_errorcontinue_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 3 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET ERRORCONTINUE requires ON or OFF.".to_string(),
                is_error: true,
            };
        }

        let mode = tokens[2].to_ascii_uppercase();
        match mode.as_str() {
            "ON" => ToolCommand::SetErrorContinue { enabled: true },
            "OFF" => ToolCommand::SetErrorContinue { enabled: false },
            _ => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET ERRORCONTINUE supports only ON or OFF.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_autocommit_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 3 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET AUTOCOMMIT requires ON or OFF.".to_string(),
                is_error: true,
            };
        }

        let mode = tokens[2].to_ascii_uppercase();
        match mode.as_str() {
            "ON" => ToolCommand::SetAutoCommit { enabled: true },
            "OFF" => ToolCommand::SetAutoCommit { enabled: false },
            _ => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET AUTOCOMMIT supports only ON or OFF.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_define_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 3 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET DEFINE requires ON, OFF, or a substitution character (e.g. '^')."
                    .to_string(),
                is_error: true,
            };
        }

        let mode = tokens[2].to_ascii_uppercase();
        match mode.as_str() {
            "ON" => ToolCommand::SetDefine {
                enabled: true,
                define_char: None,
            },
            "OFF" => ToolCommand::SetDefine {
                enabled: false,
                define_char: None,
            },
            _ => {
                // Accept a single character, optionally wrapped in single quotes: '^' or ^
                let raw_arg = tokens[2];
                let ch = if let Some(inner) = raw_arg
                    .strip_prefix('\'')
                    .and_then(|value| value.strip_suffix('\''))
                {
                    let mut chars = inner.chars();
                    match (chars.next(), chars.next()) {
                        (Some(ch), None) => Some(ch),
                        _ => None,
                    }
                } else {
                    let mut chars = raw_arg.chars();
                    match (chars.next(), chars.next()) {
                        (Some(ch), None) => Some(ch),
                        _ => None,
                    }
                };

                match ch {
                    Some(c) => ToolCommand::SetDefine { enabled: true, define_char: Some(c) },
                    None => ToolCommand::Unsupported {
                        raw: raw.to_string(),
                        message: "SET DEFINE requires ON, OFF, or a single substitution character (e.g. '^').".to_string(),
                        is_error: true,
                    },
                }
            }
        }
    }

    fn parse_scan_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 3 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET SCAN requires ON or OFF.".to_string(),
                is_error: true,
            };
        }

        let mode = tokens[2].to_ascii_uppercase();
        match mode.as_str() {
            "ON" => ToolCommand::SetScan { enabled: true },
            "OFF" => ToolCommand::SetScan { enabled: false },
            _ => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET SCAN supports only ON or OFF.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_verify_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 3 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET VERIFY requires ON or OFF.".to_string(),
                is_error: true,
            };
        }

        let mode = tokens[2].to_ascii_uppercase();
        match mode.as_str() {
            "ON" => ToolCommand::SetVerify { enabled: true },
            "OFF" => ToolCommand::SetVerify { enabled: false },
            _ => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET VERIFY supports only ON or OFF.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_echo_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 3 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET ECHO requires ON or OFF.".to_string(),
                is_error: true,
            };
        }

        let mode = tokens[2].to_ascii_uppercase();
        match mode.as_str() {
            "ON" => ToolCommand::SetEcho { enabled: true },
            "OFF" => ToolCommand::SetEcho { enabled: false },
            _ => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET ECHO supports only ON or OFF.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_timing_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 3 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET TIMING requires ON or OFF.".to_string(),
                is_error: true,
            };
        }

        let mode = tokens[2].to_ascii_uppercase();
        match mode.as_str() {
            "ON" => ToolCommand::SetTiming { enabled: true },
            "OFF" => ToolCommand::SetTiming { enabled: false },
            _ => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET TIMING supports only ON or OFF.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_feedback_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 3 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET FEEDBACK requires ON or OFF.".to_string(),
                is_error: true,
            };
        }

        let mode = tokens[2].to_ascii_uppercase();
        match mode.as_str() {
            "ON" => ToolCommand::SetFeedback { enabled: true },
            "OFF" => ToolCommand::SetFeedback { enabled: false },
            _ => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET FEEDBACK supports only ON or OFF.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_heading_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 3 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET HEADING requires ON or OFF.".to_string(),
                is_error: true,
            };
        }

        let mode = tokens[2].to_ascii_uppercase();
        match mode.as_str() {
            "ON" => ToolCommand::SetHeading { enabled: true },
            "OFF" => ToolCommand::SetHeading { enabled: false },
            _ => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET HEADING supports only ON or OFF.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_pagesize_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 3 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET PAGESIZE requires a number.".to_string(),
                is_error: true,
            };
        }

        match tokens[2].parse::<u32>() {
            Ok(size) => ToolCommand::SetPageSize { size },
            Err(_) => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET PAGESIZE requires a number.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_linesize_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 3 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET LINESIZE requires a number.".to_string(),
                is_error: true,
            };
        }

        match tokens[2].parse::<u32>() {
            Ok(size) => ToolCommand::SetLineSize { size },
            Err(_) => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET LINESIZE requires a number.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_trimspool_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 3 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET TRIMSPOOL requires ON or OFF.".to_string(),
                is_error: true,
            };
        }

        let mode = tokens[2].to_ascii_uppercase();
        match mode.as_str() {
            "ON" => ToolCommand::SetTrimSpool { enabled: true },
            "OFF" => ToolCommand::SetTrimSpool { enabled: false },
            _ => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET TRIMSPOOL supports only ON or OFF.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_trimout_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 3 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET TRIMOUT requires ON or OFF.".to_string(),
                is_error: true,
            };
        }

        let mode = tokens[2].to_ascii_uppercase();
        match mode.as_str() {
            "ON" => ToolCommand::SetTrimOut { enabled: true },
            "OFF" => ToolCommand::SetTrimOut { enabled: false },
            _ => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET TRIMOUT supports only ON or OFF.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_sqlblanklines_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 3 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET SQLBLANKLINES requires ON or OFF.".to_string(),
                is_error: true,
            };
        }

        let mode = tokens[2].to_ascii_uppercase();
        match mode.as_str() {
            "ON" => ToolCommand::SetSqlBlankLines { enabled: true },
            "OFF" => ToolCommand::SetSqlBlankLines { enabled: false },
            _ => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET SQLBLANKLINES supports only ON or OFF.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_tab_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 3 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET TAB requires ON or OFF.".to_string(),
                is_error: true,
            };
        }

        let mode = tokens[2].to_ascii_uppercase();
        match mode.as_str() {
            "ON" => ToolCommand::SetTab { enabled: true },
            "OFF" => ToolCommand::SetTab { enabled: false },
            _ => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET TAB supports only ON or OFF.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_colsep_command(raw: &str) -> ToolCommand {
        let rest = raw[10..].trim();
        if rest.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET COLSEP requires a separator string.".to_string(),
                is_error: true,
            };
        }

        let separator = rest.trim_matches('\'').trim_matches('"').to_string();
        ToolCommand::SetColSep { separator }
    }

    fn parse_null_command(raw: &str) -> ToolCommand {
        let rest = raw[8..].trim();
        if rest.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET NULL requires a display value.".to_string(),
                is_error: true,
            };
        }

        let null_text = rest.trim_matches('\'').trim_matches('"').to_string();
        ToolCommand::SetNull { null_text }
    }

    fn parse_script_command(raw: &str) -> ToolCommand {
        let trimmed = raw.trim();
        let (relative_to_caller, command_label, path) = if trimmed.starts_with("@@") {
            (true, "@@", trimmed.trim_start_matches("@@").trim())
        } else if trimmed.starts_with('@') {
            (false, "@", trimmed.trim_start_matches('@').trim())
        } else if Self::is_start_script_command(trimmed) {
            (false, "START", trimmed.get(5..).unwrap_or_default().trim())
        } else {
            (false, "@", "")
        };

        if path.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: if command_label == "START" {
                    "START requires a path.".to_string()
                } else {
                    "@file.sql requires a path.".to_string()
                },
                is_error: true,
            };
        }

        let cleaned = path.trim_matches('"').trim_matches('\'').to_string();

        ToolCommand::RunScript {
            path: cleaned,
            relative_to_caller,
        }
    }

    fn is_start_script_command(trimmed: &str) -> bool {
        if trimmed.len() < 5 {
            return false;
        }
        let head = match trimmed.get(0..5) {
            Some(head) => head,
            None => return false,
        };
        if !head.eq_ignore_ascii_case("START") {
            return false;
        }
        let tail = match trimmed.get(5..) {
            Some(tail) => tail,
            None => return false,
        };
        if tail.is_empty()
            || !tail
                .chars()
                .next()
                .map(|ch| ch.is_whitespace())
                .unwrap_or(false)
        {
            return tail.is_empty();
        }

        // Hierarchical query clause "START WITH" must stay as SQL, not SQL*Plus START command.
        let first_word = tail.split_whitespace().next().unwrap_or_default();
        !first_word.eq_ignore_ascii_case("WITH")
    }

    fn is_word_command(upper: &str, command: &str) -> bool {
        if upper == command {
            return true;
        }
        upper
            .strip_prefix(command)
            .and_then(|tail| tail.chars().next())
            .map(|ch| ch.is_whitespace())
            .unwrap_or(false)
    }

    fn parse_bind_type(type_str: &str) -> Result<BindDataType, String> {
        let trimmed = type_str.trim();
        if trimmed.is_empty() {
            return Err("VAR requires a data type.".to_string());
        }

        let upper = trimmed.to_ascii_uppercase();
        let compact = upper.replace(' ', "");

        if compact == "REFCURSOR" || compact == "SYS_REFCURSOR" {
            return Ok(BindDataType::RefCursor);
        }

        if upper.starts_with("NUMBER") || upper.starts_with("NUMERIC") {
            return Ok(BindDataType::Number);
        }

        if upper.starts_with("DATE") {
            return Ok(BindDataType::Date);
        }

        if upper.starts_with("TIMESTAMP") {
            let precision = Self::parse_parenthesized_u8(&upper).unwrap_or(6);
            return Ok(BindDataType::Timestamp(precision));
        }

        if upper.starts_with("CLOB") {
            return Ok(BindDataType::Clob);
        }

        if upper.starts_with("VARCHAR2")
            || upper.starts_with("VARCHAR")
            || upper.starts_with("NVARCHAR2")
        {
            let size = Self::parse_parenthesized_u32(&upper).unwrap_or(4000);
            return Ok(BindDataType::Varchar2(size));
        }

        if upper.starts_with("CHAR") || upper.starts_with("NCHAR") {
            let size = Self::parse_parenthesized_u32(&upper).unwrap_or(2000);
            return Ok(BindDataType::Varchar2(size));
        }

        Err(format!("Unsupported VAR type: {}", trimmed))
    }

    fn parse_parenthesized_u32(value: &str) -> Option<u32> {
        let start = value.find('(')?;
        let end = value[start + 1..].find(')')? + start + 1;
        value[start + 1..end].trim().parse::<u32>().ok()
    }

    fn parse_parenthesized_u8(value: &str) -> Option<u8> {
        let start = value.find('(')?;
        let end = value[start + 1..].find(')')? + start + 1;
        value[start + 1..end].trim().parse::<u8>().ok()
    }
}
