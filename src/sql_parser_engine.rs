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
// 3) Pending state machines – replaces pending_end, pending_if_*,
//    pending_while_do/pending_for_do, pending_timing_point_is.
// ---------------------------------------------------------------------------

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum PendingEnd {
    None,
    /// Saw END, waiting for next token to determine what it closes.
    End,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum PendingEndSuffix {
    Case,
    If,
    Loop,
    While,
    Repeat,
    For,
    TimingPoint,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum EndTokenRole {
    None,
    Suffix(PendingEndSuffix),
}

impl EndTokenRole {
    fn from_token(token_upper: &str, pending_end: PendingEnd, in_compound_trigger: bool) -> Self {
        if pending_end != PendingEnd::End {
            return Self::None;
        }

        PendingEndSuffix::parse(token_upper, in_compound_trigger)
            .map(Self::Suffix)
            .unwrap_or(Self::None)
    }

    fn suffix(self) -> Option<PendingEndSuffix> {
        match self {
            Self::Suffix(suffix) => Some(suffix),
            Self::None => None,
        }
    }

    fn is_suffix(self, suffix: PendingEndSuffix) -> bool {
        self.suffix() == Some(suffix)
    }
}

impl PendingEndSuffix {
    fn parse(token_upper: &str, in_compound_trigger: bool) -> Option<Self> {
        match token_upper {
            "CASE" => Some(Self::Case),
            "IF" => Some(Self::If),
            "LOOP" => Some(Self::Loop),
            "WHILE" => Some(Self::While),
            "REPEAT" => Some(Self::Repeat),
            "FOR" => Some(Self::For),
            "BEFORE" | "AFTER" | "INSTEAD" if in_compound_trigger => Some(Self::TimingPoint),
            _ => None,
        }
    }
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PendingDo {
    None,
    While,
    For,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct RoutineFrame {
    block_depth: usize,
    split_on_semicolon: bool,
}

impl RoutineFrame {
    fn new(block_depth: usize) -> Self {
        Self {
            block_depth,
            split_on_semicolon: false,
        }
    }
}

impl Default for PendingDo {
    fn default() -> Self {
        Self::None
    }
}

impl PendingDo {
    fn arm_for_while(self) -> Self {
        match self {
            Self::None => Self::While,
            active => active,
        }
    }

    fn arm_for_for(self) -> Self {
        match self {
            Self::None => Self::For,
            active => active,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum WithClauseState {
    None,
    PendingClause,
    InPlsqlDeclaration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TimingPointState {
    None,
    AwaitingAsOrIs,
}

impl Default for TimingPointState {
    fn default() -> Self {
        Self::None
    }
}

impl Default for WithClauseState {
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

    // -- WHILE/FOR ... DO pending state --
    pub(crate) pending_do: PendingDo,

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
    routine_is_stack: Vec<RoutineFrame>,
    pub(crate) is_package: bool,
    pub(crate) is_trigger: bool,
    in_compound_trigger: bool,
    timing_point_state: TimingPointState,
    after_type: bool,
    is_type_create: bool,

    // -- Parenthesis depth (for formatting / intellisense) --
    pub(crate) paren_depth: usize,

    // -- Oracle top-level WITH FUNCTION/PROCEDURE declarations --
    with_clause_state: WithClauseState,

    // -- Reusable buffer --
    token_upper_buf: String,
}

impl SplitState {
    fn resolve_pending_end_with_policy(&mut self, reset_create_state_when_top_level: bool) {
        if self.pending_end != PendingEnd::End {
            return;
        }

        self.resolve_plain_end();
        if reset_create_state_when_top_level
            && self.block_depth() == 0
            && !self.in_with_plsql_declaration()
        {
            self.reset_create_state();
        }
        self.pending_end = PendingEnd::None;
    }

    // -- Convenience accessors --------------------------------------------------

    pub(crate) fn is_idle(&self) -> bool {
        self.lex_mode == LexMode::Idle
    }

    pub(crate) fn in_with_plsql_declaration(&self) -> bool {
        self.with_clause_state == WithClauseState::InPlsqlDeclaration
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

        let end_token_role =
            EndTokenRole::from_token(upper, self.pending_end, self.in_compound_trigger);

        self.handle_if_state_on_token(upper);
        self.handle_pending_end_on_token(end_token_role.suffix());
        self.handle_block_openers(upper, end_token_role);

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
                .is_some_and(|frame| frame.block_depth == self.block_depth())
        {
            if let Some(frame) = self.routine_is_stack.last_mut() {
                frame.split_on_semicolon = true;
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
    fn handle_pending_end_on_token(&mut self, suffix: Option<PendingEndSuffix>) {
        if self.pending_end != PendingEnd::End {
            return;
        }
        match suffix {
            Some(PendingEndSuffix::Case) => {
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
            Some(PendingEndSuffix::If) => {
                self.pop_block_of_kind(BlockKind::If);
            }
            Some(PendingEndSuffix::Loop) => {
                self.pop_block_of_kind(BlockKind::Loop);
            }
            Some(PendingEndSuffix::While) => {
                self.pop_block_of_kind(BlockKind::While);
            }
            Some(PendingEndSuffix::Repeat) => {
                self.pop_block_of_kind(BlockKind::Repeat);
            }
            Some(PendingEndSuffix::For) => {
                self.pop_block_of_kind(BlockKind::For);
            }
            Some(PendingEndSuffix::TimingPoint) => {
                self.pop_block_of_kind(BlockKind::TimingPoint);
                self.timing_point_state = TimingPointState::None;
            }
            None => {
                // Plain END – CASE expression or PL/SQL block
                self.resolve_plain_end();
            }
        }
        self.pending_end = PendingEnd::None;
    }

    /// Sub-handler: process block-opening keywords (CASE, IF/THEN, LOOP, etc.).
    fn handle_block_openers(&mut self, upper: &str, end_token_role: EndTokenRole) {
        // CASE (opening, not END CASE)
        if upper == "CASE" && !end_token_role.is_suffix(PendingEndSuffix::Case) {
            self.block_stack.push(BlockKind::Case);
        }

        // IF (opening, not END IF)
        if upper == "IF" && !end_token_role.is_suffix(PendingEndSuffix::If) {
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
        if upper == "LOOP" && !end_token_role.is_suffix(PendingEndSuffix::Loop) {
            self.block_stack.push(BlockKind::Loop);
            self.pending_do = PendingDo::None;
        }

        // REPEAT (opening, not END REPEAT)
        if upper == "REPEAT" && !end_token_role.is_suffix(PendingEndSuffix::Repeat) {
            self.block_stack.push(BlockKind::Repeat);
        }

        // WHILE ... DO
        if upper == "WHILE"
            && self.pending_end == PendingEnd::None
            && !end_token_role.is_suffix(PendingEndSuffix::While)
        {
            self.pending_do = std::mem::take(&mut self.pending_do).arm_for_while();
        } else if self.pending_do == PendingDo::While && upper == "DO" {
            self.block_stack.push(BlockKind::While);
            self.pending_do = PendingDo::None;
        }

        if upper == "FOR"
            && self.pending_end == PendingEnd::None
            && !end_token_role.is_suffix(PendingEndSuffix::For)
        {
            let is_trigger_header_for =
                self.in_create_plsql && self.is_trigger && self.block_depth() == 0;
            if !is_trigger_header_for {
                self.pending_do = std::mem::take(&mut self.pending_do).arm_for_for();
            }
        } else if self.pending_do == PendingDo::For && upper == "DO" {
            self.block_stack.push(BlockKind::For);
            self.pending_do = PendingDo::None;
        }

        // TYPE AS/IS OBJECT/VARRAY/TABLE/REF/RECORD/OPAQUE – not a real block
        if self.after_as_is
            && matches!(
                upper,
                "OBJECT" | "VARRAY" | "TABLE" | "REF" | "RECORD" | "OPAQUE"
            )
        {
            self.block_stack.pop(); // undo the AS/IS push
            self.after_as_is = false;
        }

        // Nested PROCEDURE/FUNCTION
        if self.block_depth() > 0 && matches!(upper, "PROCEDURE" | "FUNCTION") {
            self.nested_subprogram = true;
        }

        // AS/IS block start
        let is_timing_point_block_start = matches!(upper, "AS" | "IS")
            && self.timing_point_state == TimingPointState::AwaitingAsOrIs;

        let is_block_starting_as_is = matches!(upper, "AS" | "IS")
            && (self.timing_point_state == TimingPointState::AwaitingAsOrIs
                || self.nested_subprogram
                || (self.in_create_plsql && self.block_depth() == 0));

        if is_block_starting_as_is {
            if is_timing_point_block_start {
                self.block_stack.push(BlockKind::TimingPoint);
            } else {
                self.block_stack.push(BlockKind::AsIs);
            }
            if self.is_type_create
                && !self.nested_subprogram
                && self.timing_point_state != TimingPointState::AwaitingAsOrIs
            {
                self.after_as_is = true;
            }
            self.nested_subprogram = false;
            self.timing_point_state = TimingPointState::None;
            let needs_begin_tracking = if self.is_package {
                self.block_depth() > 1
            } else {
                true
            };
            if needs_begin_tracking {
                self.routine_is_stack
                    .push(RoutineFrame::new(self.block_depth()));
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
                    .is_some_and(|frame| frame.block_depth == self.block_depth())
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
        } else if matches!(upper, "BEFORE" | "AFTER" | "INSTEAD")
            && self.in_compound_trigger
            && !end_token_role.is_suffix(PendingEndSuffix::TimingPoint)
        {
            self.timing_point_state = TimingPointState::AwaitingAsOrIs;
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
        self.resolve_pending_end_with_policy(false);
    }

    pub(crate) fn resolve_pending_end_on_terminator(&mut self) {
        self.resolve_pending_end_with_policy(true);
    }

    pub(crate) fn resolve_pending_end_on_eof(&mut self) {
        self.resolve_pending_end_with_policy(true);
    }

    pub(crate) fn should_split_on_semicolon(&self) -> bool {
        self.routine_is_stack.last().is_some_and(|frame| {
            frame.block_depth == self.block_depth() && frame.split_on_semicolon
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
        self.timing_point_state = TimingPointState::None;
        self.after_type = false;
        self.is_type_create = false;
        self.pending_do = PendingDo::None;
        self.if_state = IfState::None;
        self.with_clause_state = WithClauseState::None;
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
            self.with_clause_state = WithClauseState::PendingClause;
            return;
        }

        if self.with_clause_state == WithClauseState::None {
            return;
        }

        if matches!(upper, "FUNCTION" | "PROCEDURE") {
            self.with_clause_state = WithClauseState::InPlsqlDeclaration;
            return;
        }

        // Standard CTE shape (`WITH name AS (...)`) means this is not a
        // top-level PL/SQL declaration prefix. But Oracle allows
        // `WITH FUNCTION/PROCEDURE ... AS`, so keep declaration mode once
        // a PL/SQL declaration keyword has already been seen.
        if upper == "AS" && self.with_clause_state == WithClauseState::PendingClause {
            self.with_clause_state = WithClauseState::None;
            return;
        }

        if sql_text::is_with_main_query_keyword(upper) {
            self.with_clause_state = WithClauseState::None;
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

#[inline]
fn is_valid_q_quote_delimiter(delimiter: char) -> bool {
    !delimiter.is_whitespace()
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

    fn reset_statement_local_state(&mut self) {
        self.state.pending_end = PendingEnd::None;
        self.state.pending_do = PendingDo::None;
        self.state.if_state = IfState::None;
        self.state.paren_depth = 0;
    }

    fn push_current_statement(&mut self) {
        let trimmed = self.current.trim();
        if !trimmed.is_empty() {
            self.statements.push(trimmed.to_string());
        }
        self.current.clear();
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
                    if !is_valid_q_quote_delimiter(delimiter) {
                        // Oracle q-quote delimiters cannot be whitespace.
                        // Fall back to regular token/quote parsing.
                        self.current.push(c);
                        self.state.token.push(c);
                        i += 1;
                        continue;
                    }
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
                    if !is_valid_q_quote_delimiter(delimiter) {
                        self.current.push(c);
                        self.state.token.push(c);
                        i += 1;
                        continue;
                    }
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
                self.state.pending_do = PendingDo::None;
                self.state.resolve_pending_end_on_terminator();
                if self.state.block_depth() == 0 && !self.state.in_with_plsql_declaration() {
                    self.push_current_statement();
                    self.reset_statement_local_state();
                    self.state.reset_create_state();
                } else if self.state.should_split_on_semicolon() {
                    self.push_current_statement();
                    self.reset_statement_local_state();
                    self.state.reset_create_state();
                    self.state.block_stack.clear();
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
        self.push_current_statement();
        self.reset_statement_local_state();
    }

    pub(crate) fn finalize(&mut self) {
        self.state.flush_token();
        self.state.resolve_pending_end_on_eof();
        self.state.reset_create_state();
        self.push_current_statement();
        self.reset_statement_local_state();
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

#[cfg(test)]
mod tests {
    use super::{
        BlockKind, EndTokenRole, IfState, PendingDo, PendingEnd, PendingEndSuffix, RoutineFrame,
        SplitState, SqlParserEngine, TimingPointState, WithClauseState,
    };

    #[test]
    fn end_token_role_requires_pending_end_state() {
        assert_eq!(
            EndTokenRole::from_token("CASE", PendingEnd::None, false),
            EndTokenRole::None
        );
    }

    #[test]
    fn end_token_role_maps_suffix_with_compound_trigger_scope() {
        assert_eq!(
            EndTokenRole::from_token("CASE", PendingEnd::End, false).suffix(),
            Some(PendingEndSuffix::Case)
        );
        assert_eq!(
            EndTokenRole::from_token("AFTER", PendingEnd::End, false).suffix(),
            None
        );
        assert_eq!(
            EndTokenRole::from_token("AFTER", PendingEnd::End, true).suffix(),
            Some(PendingEndSuffix::TimingPoint)
        );
    }

    #[test]
    fn end_token_role_reports_matching_suffix() {
        let suffix_role = EndTokenRole::Suffix(PendingEndSuffix::Loop);

        assert!(suffix_role.is_suffix(PendingEndSuffix::Loop));
        assert!(!suffix_role.is_suffix(PendingEndSuffix::If));
        assert!(!EndTokenRole::None.is_suffix(PendingEndSuffix::Case));
    }

    #[test]
    fn pending_end_suffix_parse_covers_supported_keywords() {
        assert_eq!(
            PendingEndSuffix::parse("CASE", false),
            Some(PendingEndSuffix::Case)
        );
        assert_eq!(
            PendingEndSuffix::parse("IF", false),
            Some(PendingEndSuffix::If)
        );
        assert_eq!(
            PendingEndSuffix::parse("LOOP", false),
            Some(PendingEndSuffix::Loop)
        );
        assert_eq!(
            PendingEndSuffix::parse("WHILE", false),
            Some(PendingEndSuffix::While)
        );
        assert_eq!(
            PendingEndSuffix::parse("REPEAT", false),
            Some(PendingEndSuffix::Repeat)
        );
        assert_eq!(
            PendingEndSuffix::parse("FOR", false),
            Some(PendingEndSuffix::For)
        );
    }

    #[test]
    fn pending_end_suffix_parse_scopes_timing_point_keywords() {
        assert_eq!(PendingEndSuffix::parse("BEFORE", false), None);
        assert_eq!(
            PendingEndSuffix::parse("AFTER", true),
            Some(PendingEndSuffix::TimingPoint)
        );
    }

    #[test]
    fn end_timing_point_suffix_clears_pending_timing_point_state() {
        let mut state = SplitState {
            pending_end: PendingEnd::End,
            timing_point_state: TimingPointState::AwaitingAsOrIs,
            block_stack: vec![BlockKind::TimingPoint],
            ..SplitState::default()
        };

        state.handle_pending_end_on_token(Some(PendingEndSuffix::TimingPoint));

        assert_eq!(state.pending_end, PendingEnd::None);
        assert_eq!(state.timing_point_state, TimingPointState::None);
        assert!(state.block_stack.is_empty());
    }

    #[test]
    fn semicolon_split_resets_transient_state_at_top_level() {
        let mut engine = SqlParserEngine::new();
        engine.current.push_str("SELECT 1");
        engine.state.pending_end = PendingEnd::End;
        engine.state.pending_do = PendingDo::For;
        engine.state.if_state = IfState::AwaitingThen;
        engine.state.paren_depth = 2;

        engine.process_chars_with_observer(&[';'], &mut |_, _, _, _| {});

        assert_eq!(engine.take_statements(), vec!["SELECT 1".to_string()]);
        assert!(engine.current.is_empty());
        assert_eq!(engine.state.pending_end, PendingEnd::None);
        assert_eq!(engine.state.pending_do, PendingDo::None);
        assert_eq!(engine.state.if_state, IfState::None);
        assert_eq!(engine.state.paren_depth, 0);
    }

    #[test]
    fn pending_do_does_not_get_overwritten_by_new_candidates() {
        let mut state = SplitState {
            pending_do: PendingDo::While,
            ..SplitState::default()
        };

        state.handle_block_openers("FOR", EndTokenRole::None);
        assert_eq!(state.pending_do, PendingDo::While);

        state.handle_block_openers("DO", EndTokenRole::None);
        assert_eq!(state.block_depth(), 1);
        assert_eq!(state.block_stack.last(), Some(&BlockKind::While));
        assert_eq!(state.pending_do, PendingDo::None);
    }

    #[test]
    fn pending_do_arms_when_no_active_candidate_exists() {
        let mut state = SplitState::default();

        state.handle_block_openers("FOR", EndTokenRole::None);
        assert_eq!(state.pending_do, PendingDo::For);

        state.handle_block_openers("DO", EndTokenRole::None);
        assert_eq!(state.block_stack.last(), Some(&BlockKind::For));
        assert_eq!(state.pending_do, PendingDo::None);
    }

    #[test]
    fn semicolon_split_for_external_routine_resets_transient_state() {
        let mut engine = SqlParserEngine::new();
        engine.current.push_str("LANGUAGE C");
        engine.state.block_stack.push(BlockKind::AsIs);
        engine.state.routine_is_stack.push(RoutineFrame {
            block_depth: 1,
            split_on_semicolon: true,
        });
        engine.state.pending_end = PendingEnd::End;
        engine.state.pending_do = PendingDo::While;
        engine.state.if_state = IfState::AfterConditionParen;
        engine.state.paren_depth = 1;

        engine.process_chars_with_observer(&[';'], &mut |_, _, _, _| {});

        assert_eq!(engine.take_statements(), vec!["LANGUAGE C".to_string()]);
        assert!(engine.current.is_empty());
        assert_eq!(engine.state.block_depth(), 0);
        assert_eq!(engine.state.pending_end, PendingEnd::None);
        assert_eq!(engine.state.pending_do, PendingDo::None);
        assert_eq!(engine.state.if_state, IfState::None);
        assert_eq!(engine.state.paren_depth, 0);
    }
    #[test]
    fn separator_resolution_keeps_create_state() {
        let mut state = SplitState {
            pending_end: PendingEnd::End,
            in_create_plsql: true,
            block_stack: vec![BlockKind::Begin],
            ..SplitState::default()
        };

        state.resolve_pending_end_on_separator();

        assert_eq!(state.pending_end, PendingEnd::None);
        assert_eq!(state.block_depth(), 0);
        assert!(state.in_create_plsql);
    }

    #[test]
    fn terminator_resolution_resets_create_state_at_top_level() {
        let mut state = SplitState {
            pending_end: PendingEnd::End,
            in_create_plsql: true,
            block_stack: vec![BlockKind::Begin],
            ..SplitState::default()
        };

        state.resolve_pending_end_on_terminator();

        assert_eq!(state.pending_end, PendingEnd::None);
        assert_eq!(state.block_depth(), 0);
        assert!(!state.in_create_plsql);
    }

    #[test]
    fn eof_resolution_preserves_with_plsql_declaration_mode() {
        let mut state = SplitState {
            pending_end: PendingEnd::End,
            in_create_plsql: true,
            with_clause_state: WithClauseState::InPlsqlDeclaration,
            block_stack: vec![BlockKind::Begin],
            ..SplitState::default()
        };

        state.resolve_pending_end_on_eof();

        assert_eq!(state.pending_end, PendingEnd::None);
        assert_eq!(state.block_depth(), 0);
        assert!(state.in_create_plsql);
        assert!(state.in_with_plsql_declaration());
    }

    #[test]
    fn finalize_clears_transient_parser_state_for_reuse() {
        let mut engine = SqlParserEngine::new();
        engine.process_line("FOR i IN 1..10");
        engine.process_line("IF flag");
        engine.state.paren_depth = 3;

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements, vec!["FOR i IN 1..10\nIF flag".to_string()]);
        assert_eq!(engine.state.pending_do, PendingDo::None);
        assert_eq!(engine.state.if_state, IfState::None);
        assert_eq!(engine.state.paren_depth, 0);
    }
    #[test]
    fn compound_trigger_timing_point_uses_dedicated_block_kind() {
        let mut state = SplitState {
            in_create_plsql: true,
            in_compound_trigger: true,
            timing_point_state: TimingPointState::AwaitingAsOrIs,
            ..SplitState::default()
        };

        state.handle_block_openers("IS", EndTokenRole::None);

        assert_eq!(state.block_stack.last(), Some(&BlockKind::TimingPoint));
        assert_eq!(state.timing_point_state, TimingPointState::None);

        state.pending_end = PendingEnd::End;
        state.handle_pending_end_on_token(Some(PendingEndSuffix::TimingPoint));

        assert!(state.block_stack.is_empty());
        assert_eq!(state.pending_end, PendingEnd::None);
    }
}
