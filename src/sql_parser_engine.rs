use crate::sql_text;

// ---------------------------------------------------------------------------
// 1) LexMode – replaces 6 boolean flags with a single enum.
//    Illegal states (e.g. in_single_quote && in_block_comment) are now
//    structurally impossible.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LexMode {
    /// Normal code – not inside any string literal or comment.
    Idle,
    SingleQuote,
    DoubleQuote,
    BacktickQuote,
    LineComment,
    BlockComment,
    QQuote {
        end_char: char,
    },
    DollarQuote {
        tag: String,
    },
}

impl Default for LexMode {
    fn default() -> Self {
        Self::Idle
    }
}

// ---------------------------------------------------------------------------
// 2) BlockKind stack – replaces (block_depth: usize, case_depth_stack: Vec).
//    Each entry records *what* opened the block so END resolution is
//    unambiguous – no more guessing based on depth arithmetic.
// ---------------------------------------------------------------------------

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum BlockKind {
    /// BEGIN ... END  (standalone or DECLARE ... BEGIN ... END)
    Begin,
    /// DECLARE ... (waiting for BEGIN, shares depth with subsequent BEGIN)
    Declare,
    /// AS/IS ... BEGIN ... END  (CREATE PL/SQL body)
    AsIs,
    /// CASE ... END [CASE]  (could be expression or statement)
    Case,
    /// IF ... THEN ... END IF
    If,
    /// LOOP ... END LOOP  /  FOR ... LOOP ... END LOOP
    Loop,
    /// WHILE ... DO ... END WHILE  (MySQL-style)
    While,
    /// FOR ... DO ... END FOR (MySQL-style)
    For,
    /// REPEAT ... END REPEAT
    Repeat,
    /// COMPOUND (TRIGGER body outer block)
    Compound,
    /// BEFORE/AFTER/INSTEAD timing point IS ... END <timing>
    TimingPoint,
}

// ---------------------------------------------------------------------------
// 3) PendingState – replaces pending_end, pending_if_*, pending_while_do,
//    pending_timing_point_is.  Only one pending state is active at a time.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PendingEnd {
    None,
    /// Saw END, waiting for next token to determine what it closes.
    End,
}

impl Default for PendingEnd {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IfState {
    None,
    /// Saw IF, waiting for the first meaningful character after IF.
    ExpectConditionStart,
    /// Saw IF followed by `(`, tracking condition paren depth.
    InConditionParen {
        depth: usize,
    },
    /// Condition paren closed, waiting for THEN.
    AfterConditionParen,
    /// Saw IF (no paren), waiting for THEN.
    AwaitingThen,
}

impl Default for IfState {
    fn default() -> Self {
        Self::None
    }
}

// ---------------------------------------------------------------------------
// SplitState – the main parser state, now using the types above.
// ---------------------------------------------------------------------------

#[derive(Default)]
pub(crate) struct SplitState {
    // -- Lexer state (was 6 booleans + 2 associated fields) --
    pub(crate) lex_mode: LexMode,

    // -- Block depth (was block_depth: usize + case_depth_stack: Vec<usize>) --
    pub(crate) block_stack: Vec<BlockKind>,

    // -- Pending END resolution --
    pub(crate) pending_end: PendingEnd,

    // -- IF state machine --
    pub(crate) if_state: IfState,

    // -- WHILE ... DO state --
    pub(crate) pending_while_do: bool,
    pub(crate) pending_for_do: bool,

    // -- Token accumulator --
    pub(crate) token: String,

    // -- CREATE PL/SQL tracking --
    pub(crate) in_create_plsql: bool,
    pub(crate) create_pending: bool,
    create_or_seen: bool,
    pub(crate) after_declare: bool,
    after_as_is: bool,
    nested_subprogram: bool,
    pub(crate) pending_subprogram_begins: usize,
    routine_is_stack: Vec<(usize, bool)>,
    pub(crate) is_package: bool,
    pub(crate) is_trigger: bool,
    in_compound_trigger: bool,
    pending_timing_point_is: bool,
    after_type: bool,
    is_type_create: bool,

    // -- Parenthesis depth (for formatting / intellisense) --
    pub(crate) paren_depth: usize,

    // -- Oracle top-level WITH FUNCTION/PROCEDURE declarations --
    pending_with_clause: bool,
    in_with_plsql_declaration: bool,

    // -- Reusable buffer --
    token_upper_buf: String,
}

impl SplitState {
    // -- Convenience accessors --------------------------------------------------

    pub(crate) fn is_idle(&self) -> bool {
        self.lex_mode == LexMode::Idle
    }

    /// Derived block depth – equivalent to the old `block_depth` field.
    pub(crate) fn block_depth(&self) -> usize {
        self.block_stack.len()
    }

    /// Number of open CASE blocks on the stack (replaces case_depth_stack.len()).
    pub(crate) fn case_count(&self) -> usize {
        self.block_stack
            .iter()
            .filter(|k| **k == BlockKind::Case)
            .count()
    }

    /// Returns the depth at which the innermost CASE was opened, if any.
    /// This is the index in block_stack of the last Case entry.
    pub(crate) fn innermost_case_depth(&self) -> Option<usize> {
        self.block_stack.iter().rposition(|k| *k == BlockKind::Case)
    }

    /// Whether the top of the block stack is a CASE (used for END resolution).
    fn top_is_case(&self) -> bool {
        self.block_stack.last() == Some(&BlockKind::Case)
    }

    // -- LexMode helpers --------------------------------------------------------

    pub(crate) fn start_q_quote(&mut self, delimiter: char) {
        self.lex_mode = LexMode::QQuote {
            end_char: sql_text::q_quote_closing(delimiter),
        };
    }

    pub(crate) fn q_quote_end(&self) -> Option<char> {
        match &self.lex_mode {
            LexMode::QQuote { end_char } => Some(*end_char),
            _ => None,
        }
    }

    // -- Token handling (split into sub-handlers) --------------------------------

    pub(crate) fn flush_token(&mut self) {
        if self.token.is_empty() {
            return;
        }
        let mut upper_buf = std::mem::take(&mut self.token_upper_buf);
        upper_buf.clear();
        upper_buf.push_str(&self.token);
        upper_buf.make_ascii_uppercase();
        let upper = upper_buf.as_str();

        self.handle_routine_is_external(upper);
        self.track_create_plsql(upper);
        self.track_top_level_with_plsql(upper);

        let was_pending_end = self.pending_end == PendingEnd::End;
        let is_end_case = was_pending_end && upper == "CASE";
        let is_end_if = was_pending_end && upper == "IF";
        let is_end_loop = was_pending_end && upper == "LOOP";
        let is_end_while = was_pending_end && upper == "WHILE";
        let is_end_repeat = was_pending_end && upper == "REPEAT";
        let is_end_for = was_pending_end && upper == "FOR";

        self.handle_if_state_on_token(upper);
        self.handle_pending_end_on_token(upper);
        self.handle_block_openers(
            upper,
            is_end_case,
            is_end_if,
            is_end_loop,
            is_end_while,
            is_end_repeat,
            is_end_for,
        );

        // Return the uppercase buffer so its capacity is reused.
        let _ = upper;
        self.token_upper_buf = upper_buf;
        self.token.clear();
    }

    /// Sub-handler: mark EXTERNAL/LANGUAGE/NAME/LIBRARY as split-on-semicolon.
    fn handle_routine_is_external(&mut self, upper: &str) {
        if matches!(upper, "EXTERNAL" | "LANGUAGE" | "NAME" | "LIBRARY")
            && self
                .routine_is_stack
                .last()
                .is_some_and(|(depth, _)| *depth == self.block_depth())
        {
            if let Some((_, split_on_semicolon)) = self.routine_is_stack.last_mut() {
                *split_on_semicolon = true;
            }
        }
    }

    /// Sub-handler: IF state machine transitions on keyword tokens.
    fn handle_if_state_on_token(&mut self, upper: &str) {
        match &self.if_state {
            IfState::AfterConditionParen => {
                if upper != "THEN" {
                    self.if_state = IfState::None;
                }
            }
            IfState::ExpectConditionStart => {
                if upper != "IF" {
                    // Saw a keyword (not another IF), so no paren – just wait for THEN.
                    self.if_state = IfState::AwaitingThen;
                }
            }
            _ => {}
        }
    }

    /// Sub-handler: resolve pending END based on the following keyword.
    fn handle_pending_end_on_token(&mut self, upper: &str) {
        if self.pending_end != PendingEnd::End {
            return;
        }
        match upper {
            "CASE" => {
                // END CASE – pop CASE from stack
                if self.top_is_case() {
                    self.block_stack.pop();
                } else {
                    // Fallback: pop topmost CASE if any
                    if let Some(pos) = self.block_stack.iter().rposition(|k| *k == BlockKind::Case)
                    {
                        self.block_stack.remove(pos);
                    } else {
                        self.block_stack.pop();
                    }
                }
            }
            "IF" => {
                self.pop_block_of_kind(BlockKind::If);
            }
            "LOOP" => {
                self.pop_block_of_kind(BlockKind::Loop);
            }
            "WHILE" => {
                self.pop_block_of_kind(BlockKind::While);
            }
            "REPEAT" => {
                self.pop_block_of_kind(BlockKind::Repeat);
            }
            "FOR" => {
                self.pop_block_of_kind(BlockKind::For);
            }
            "BEFORE" | "AFTER" | "INSTEAD" if self.in_compound_trigger => {
                self.pop_block_of_kind(BlockKind::TimingPoint);
            }
            _ => {
                // Plain END – CASE expression or PL/SQL block
                self.resolve_plain_end();
            }
        }
        self.pending_end = PendingEnd::None;
    }

    /// Sub-handler: process block-opening keywords (CASE, IF/THEN, LOOP, etc.).
    fn handle_block_openers(
        &mut self,
        upper: &str,
        is_end_case: bool,
        is_end_if: bool,
        is_end_loop: bool,
        is_end_while: bool,
        is_end_repeat: bool,
        is_end_for: bool,
    ) {
        // CASE (opening, not END CASE)
        if upper == "CASE" && !is_end_case {
            self.block_stack.push(BlockKind::Case);
        }

        // IF (opening, not END IF)
        if upper == "IF" && !is_end_if {
            self.if_state = IfState::ExpectConditionStart;
        }

        // THEN resolves IF → block open
        if upper == "THEN" {
            match &self.if_state {
                IfState::AwaitingThen | IfState::AfterConditionParen => {
                    self.block_stack.push(BlockKind::If);
                    self.if_state = IfState::None;
                }
                _ => {}
            }
        }

        // LOOP (opening, not END LOOP)
        if upper == "LOOP" && !is_end_loop {
            self.block_stack.push(BlockKind::Loop);
            self.pending_while_do = false;
            self.pending_for_do = false;
        }

        // REPEAT (opening, not END REPEAT)
        if upper == "REPEAT" && !is_end_repeat {
            self.block_stack.push(BlockKind::Repeat);
        }

        // WHILE ... DO
        if upper == "WHILE" && self.pending_end == PendingEnd::None && !is_end_while {
            self.pending_while_do = true;
        } else if self.pending_while_do && upper == "DO" {
            self.block_stack.push(BlockKind::While);
            self.pending_while_do = false;
        }

        if upper == "FOR" && self.pending_end == PendingEnd::None && !is_end_for {
            let is_trigger_header_for =
                self.in_create_plsql && self.is_trigger && self.block_depth() == 0;
            if !is_trigger_header_for {
                self.pending_for_do = true;
            }
        } else if self.pending_for_do && upper == "DO" {
            self.block_stack.push(BlockKind::For);
            self.pending_for_do = false;
        }

        // TYPE AS/IS OBJECT/VARRAY/TABLE/REF/RECORD – not a real block
        if self.after_as_is && matches!(upper, "OBJECT" | "VARRAY" | "TABLE" | "REF" | "RECORD") {
            self.block_stack.pop(); // undo the AS/IS push
            self.after_as_is = false;
        }

        // Nested PROCEDURE/FUNCTION
        if self.block_depth() > 0 && matches!(upper, "PROCEDURE" | "FUNCTION") {
            self.nested_subprogram = true;
        }

        // AS/IS block start
        let is_block_starting_as_is = matches!(upper, "AS" | "IS")
            && (self.pending_timing_point_is
                || self.nested_subprogram
                || (self.in_create_plsql && self.block_depth() == 0));

        if is_block_starting_as_is {
            self.block_stack.push(BlockKind::AsIs);
            let split_on_semicolon = false;
            if self.is_type_create && !self.nested_subprogram && !self.pending_timing_point_is {
                self.after_as_is = true;
            }
            self.nested_subprogram = false;
            self.pending_timing_point_is = false;
            let needs_begin_tracking = if self.is_package {
                self.block_depth() > 1
            } else {
                true
            };
            if needs_begin_tracking {
                self.routine_is_stack
                    .push((self.block_depth(), split_on_semicolon));
                self.pending_subprogram_begins += 1;
            }
        } else if upper == "DECLARE" {
            self.block_stack.push(BlockKind::Declare);
            self.after_declare = true;
        } else if upper == "BEGIN" {
            if self.after_declare {
                // DECLARE ... BEGIN – same block, don't push
                self.after_declare = false;
            } else if self.pending_subprogram_begins > 0 {
                // AS/IS ... BEGIN – same block
                if self
                    .routine_is_stack
                    .last()
                    .is_some_and(|(depth, _)| *depth == self.block_depth())
                {
                    let _ = self.routine_is_stack.pop();
                }
                self.pending_subprogram_begins -= 1;
            } else {
                self.block_stack.push(BlockKind::Begin);
            }
        } else if upper == "END" {
            self.pending_end = PendingEnd::End;
        } else if upper == "COMPOUND" && self.in_create_plsql {
            self.in_compound_trigger = true;
            self.block_stack.push(BlockKind::Compound);
        } else if matches!(upper, "BEFORE" | "AFTER" | "INSTEAD") && self.in_compound_trigger {
            self.pending_timing_point_is = true;
        }
    }

    // -- END resolution helpers -------------------------------------------------

    /// Pop the topmost block of the given kind, or just pop the top if not found.
    fn pop_block_of_kind(&mut self, kind: BlockKind) {
        if self.block_stack.last() == Some(&kind) {
            self.block_stack.pop();
        } else if let Some(pos) = self.block_stack.iter().rposition(|k| *k == kind) {
            self.block_stack.remove(pos);
        } else if !self.block_stack.is_empty() {
            self.block_stack.pop();
        }
    }

    /// Plain END (not END CASE/IF/LOOP/WHILE/REPEAT/timing).
    /// If top is Case, treat as CASE expression end. Otherwise pop a PL/SQL block.
    fn resolve_plain_end(&mut self) {
        if self.top_is_case() {
            self.block_stack.pop();
        } else if !self.block_stack.is_empty() {
            self.block_stack.pop();
        }
    }

    pub(crate) fn resolve_pending_end_on_separator(&mut self) {
        if self.pending_end == PendingEnd::End {
            self.resolve_plain_end();
            self.pending_end = PendingEnd::None;
        }
    }

    pub(crate) fn resolve_pending_end_on_terminator(&mut self) {
        if self.pending_end == PendingEnd::End {
            self.resolve_plain_end();
            if self.block_depth() == 0 && !self.in_with_plsql_declaration {
                self.reset_create_state();
            }
            self.pending_end = PendingEnd::None;
        }
    }

    pub(crate) fn resolve_pending_end_on_eof(&mut self) {
        if self.pending_end == PendingEnd::End {
            self.resolve_plain_end();
            if self.block_depth() == 0 && !self.in_with_plsql_declaration {
                self.reset_create_state();
            }
            self.pending_end = PendingEnd::None;
        }
    }

    pub(crate) fn should_split_on_semicolon(&self) -> bool {
        self.routine_is_stack
            .last()
            .is_some_and(|(depth, split_on_semicolon)| {
                *depth == self.block_depth() && *split_on_semicolon
            })
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
        self.pending_for_do = false;
        self.if_state = IfState::None;
        self.pending_with_clause = false;
        self.in_with_plsql_declaration = false;
    }

    /// Reset all state to idle for force-terminate scenarios.
    pub(crate) fn force_reset_all(&mut self) {
        self.flush_token();
        self.resolve_pending_end_on_eof();
        self.reset_create_state();
        self.lex_mode = LexMode::Idle;
        self.pending_end = PendingEnd::None;
        self.token.clear();
        self.block_stack.clear();
        self.paren_depth = 0;
    }

    fn track_create_plsql(&mut self, upper: &str) {
        if self.in_create_plsql && self.after_type && upper == "BODY" {
            self.is_package = true;
            self.after_type = false;
            return;
        }

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
                "NO" | "FORCE" | "REPLACE" => {
                    return;
                }
                "EDITIONABLE" | "NONEDITIONABLE" | "EDITIONING" | "NONEDITIONING" => {
                    return;
                }
                "PROCEDURE" | "FUNCTION" | "PACKAGE" | "TYPE" | "TRIGGER" => {
                    self.in_create_plsql = true;
                    self.is_package = upper == "PACKAGE";
                    self.is_trigger = upper == "TRIGGER";
                    self.is_type_create = upper == "TYPE";
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

    fn track_top_level_with_plsql(&mut self, upper: &str) {
        if self.block_depth() != 0 {
            return;
        }

        if upper == "WITH" {
            self.pending_with_clause = true;
            return;
        }

        if !self.pending_with_clause {
            return;
        }

        if matches!(upper, "FUNCTION" | "PROCEDURE") {
            self.in_with_plsql_declaration = true;
            return;
        }

        // Standard CTE shape (`WITH name AS (...)`) means this is not a
        // top-level PL/SQL declaration prefix. But Oracle allows
        // `WITH FUNCTION/PROCEDURE ... AS`, so keep declaration mode once
        // a PL/SQL declaration keyword has already been seen.
        if upper == "AS" && !self.in_with_plsql_declaration {
            self.pending_with_clause = false;
            self.in_with_plsql_declaration = false;
            return;
        }

        if sql_text::is_with_main_query_keyword(upper) {
            self.pending_with_clause = false;
            self.in_with_plsql_declaration = false;
        }
    }
}

// ---------------------------------------------------------------------------
// Free functions (unchanged)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// SqlParserEngine
// ---------------------------------------------------------------------------

pub(crate) struct SqlParserEngine {
    pub(crate) state: SplitState,
    current: String,
    statements: Vec<String>,
    scratch_chars: Vec<char>,
}

impl SqlParserEngine {
    pub(crate) fn new() -> Self {
        Self {
            state: SplitState::default(),
            current: String::new(),
            statements: Vec::new(),
            scratch_chars: Vec::new(),
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
        self.state.block_depth()
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

    pub(crate) fn process_line(&mut self, line: &str) {
        self.process_line_with_observer(line, |_, _, _, _| {});
    }

    fn process_chars_with_observer<F>(&mut self, chars: &[char], on_symbol: &mut F)
    where
        F: FnMut(&[char], usize, char, Option<char>),
    {
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

            // ---- Dispatch on LexMode (replaces 6 if-chains) ----
            match &self.state.lex_mode {
                LexMode::LineComment => {
                    self.current.push(c);
                    if c == '\n' {
                        self.state.lex_mode = LexMode::Idle;
                    }
                    i += 1;
                    continue;
                }
                LexMode::BlockComment => {
                    self.current.push(c);
                    if c == '*' && next == Some('/') {
                        self.current.push('/');
                        self.state.lex_mode = LexMode::Idle;
                        i += 2;
                        continue;
                    }
                    i += 1;
                    continue;
                }
                LexMode::QQuote { end_char } => {
                    let end_char = *end_char;
                    self.current.push(c);
                    if c == end_char && next == Some('\'') {
                        self.current.push('\'');
                        self.state.lex_mode = LexMode::Idle;
                        i += 2;
                        continue;
                    }
                    i += 1;
                    continue;
                }
                LexMode::DollarQuote { .. } => {
                    // Need to extract tag to check for closing.
                    // We reborrow via a match to satisfy the borrow checker.
                    let tag_matches = if let LexMode::DollarQuote { tag } = &self.state.lex_mode {
                        c == '$' && chars_starts_with(chars, i, tag)
                    } else {
                        false
                    };
                    if tag_matches {
                        let tag_len = if let LexMode::DollarQuote { tag } = &self.state.lex_mode {
                            let tl = tag.len();
                            for quote_ch in tag.chars() {
                                self.current.push(quote_ch);
                            }
                            tl
                        } else {
                            0
                        };
                        self.state.lex_mode = LexMode::Idle;
                        i += tag_len;
                        continue;
                    }
                    self.current.push(c);
                    i += 1;
                    continue;
                }
                LexMode::SingleQuote => {
                    self.current.push(c);
                    if c == '\'' {
                        if next == Some('\'') {
                            self.current.push('\'');
                            i += 2;
                            continue;
                        }
                        self.state.lex_mode = LexMode::Idle;
                    }
                    i += 1;
                    continue;
                }
                LexMode::DoubleQuote => {
                    self.current.push(c);
                    if c == '"' {
                        if next == Some('"') {
                            self.current.push('"');
                            i += 2;
                            continue;
                        }
                        self.state.lex_mode = LexMode::Idle;
                    }
                    i += 1;
                    continue;
                }
                LexMode::BacktickQuote => {
                    self.current.push(c);
                    if c == '`' {
                        if next == Some('`') {
                            self.current.push('`');
                            i += 2;
                            continue;
                        }
                        self.state.lex_mode = LexMode::Idle;
                    }
                    i += 1;
                    continue;
                }
                LexMode::Idle => {
                    // Fall through to normal code processing below.
                }
            }

            // ---- Normal (Idle) code processing ----

            if c == '-' && next == Some('-') {
                self.state.flush_token();
                self.state.lex_mode = LexMode::LineComment;
                self.current.push('-');
                self.current.push('-');
                i += 2;
                continue;
            }

            if c == '/' && next == Some('*') {
                self.state.flush_token();
                self.state.lex_mode = LexMode::BlockComment;
                self.current.push('/');
                self.current.push('*');
                i += 2;
                continue;
            }

            // nq'[...]'
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

            // q'[...]'
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

            // $$tag$$
            if self.state.token.is_empty() && c == '$' {
                if let Some(tag) = parse_dollar_quote_tag(chars, i) {
                    let tag_len = tag.len();
                    self.state.flush_token();
                    self.state.lex_mode = LexMode::DollarQuote { tag };
                    if let LexMode::DollarQuote { tag } = &self.state.lex_mode {
                        for quote_ch in tag.chars() {
                            self.current.push(quote_ch);
                        }
                    }
                    i += tag_len;
                    continue;
                }
            }

            if c == '\'' {
                self.state.flush_token();
                self.state.lex_mode = LexMode::SingleQuote;
                self.current.push(c);
                i += 1;
                continue;
            }

            if c == '"' {
                self.state.flush_token();
                self.state.lex_mode = LexMode::DoubleQuote;
                self.current.push(c);
                i += 1;
                continue;
            }

            if c == '`' {
                self.state.flush_token();
                self.state.lex_mode = LexMode::BacktickQuote;
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
            on_symbol(chars, i, c, next);

            // IF state machine on symbol characters
            match &self.state.if_state {
                IfState::ExpectConditionStart => {
                    if c.is_whitespace() {
                        // Keep waiting.
                    } else if c == '(' {
                        let condition_depth = self.state.paren_depth.saturating_add(1);
                        self.state.if_state = IfState::InConditionParen {
                            depth: condition_depth,
                        };
                    } else {
                        self.state.if_state = IfState::AwaitingThen;
                    }
                }
                IfState::AfterConditionParen => {
                    if !c.is_whitespace() {
                        self.state.if_state = IfState::None;
                    }
                }
                _ => {}
            }

            // Check if closing paren matches IF condition paren
            if c == ')' {
                if let IfState::InConditionParen { depth } = self.state.if_state {
                    if depth == self.state.paren_depth {
                        self.state.if_state = IfState::AfterConditionParen;
                    }
                }
            }

            // Track parenthesis depth
            if c == '(' {
                self.state.paren_depth += 1;
            } else if c == ')' {
                self.state.paren_depth = self.state.paren_depth.saturating_sub(1);
            }

            // Pending END on separator
            if self.state.pending_end == PendingEnd::End {
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
                // FOR/WHILE ... DO candidates cannot span statement terminators.
                // Reset them so keywords like `FOR UPDATE; DO ...` don't create false loop depth.
                self.state.pending_for_do = false;
                self.state.pending_while_do = false;
                self.state.resolve_pending_end_on_terminator();
                if self.state.block_depth() == 0 && !self.state.in_with_plsql_declaration {
                    let trimmed = self.current.trim();
                    if !trimmed.is_empty() {
                        self.statements.push(trimmed.to_string());
                    }
                    self.current.clear();
                    self.state.reset_create_state();
                } else if self.state.should_split_on_semicolon() {
                    self.state.reset_create_state();
                    self.state.block_stack.clear();
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
        let mut on_symbol = on_symbol;
        let mut scratch_chars = std::mem::take(&mut self.scratch_chars);
        scratch_chars.clear();
        scratch_chars.extend(line.chars());
        scratch_chars.push('\n');
        self.process_chars_with_observer(&scratch_chars, &mut on_symbol);
        self.scratch_chars = scratch_chars;
    }

    pub(crate) fn force_terminate(&mut self) {
        self.state.force_reset_all();
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
