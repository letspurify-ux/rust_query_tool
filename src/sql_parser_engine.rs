use crate::sql_text;

// ---------------------------------------------------------------------------
// 1) LexMode – replaces 6 boolean flags with a single enum.
//    Illegal states (e.g. in_single_quote && in_block_comment) are now
//    structurally impossible.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub(crate) enum LexMode {
    /// Normal code – not inside any string literal or comment.
    #[default]
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

#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
pub(crate) enum PendingEnd {
    #[default]
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

    fn closing_block_kind(self) -> Option<BlockKind> {
        match self {
            Self::Case => None,
            Self::If => Some(BlockKind::If),
            Self::Loop => Some(BlockKind::Loop),
            Self::While => Some(BlockKind::While),
            Self::Repeat => Some(BlockKind::Repeat),
            Self::For => Some(BlockKind::For),
            Self::TimingPoint => Some(BlockKind::TimingPoint),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub(crate) enum IfState {
    #[default]
    None,
    /// Saw IF, waiting for the first meaningful character after IF.
    ExpectConditionStart,
    /// Saw IF followed by `(`, tracking condition paren depth.
    InConditionParen { depth: usize },
    /// Condition paren closed, waiting for THEN.
    AfterConditionParen,
    /// Saw IF (no paren), waiting for THEN.
    AwaitingThen,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub(crate) enum PendingDo {
    #[default]
    None,
    While {
        armed_at_block_depth: usize,
    },
    For {
        armed_at_block_depth: usize,
    },
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct RoutineFrame {
    block_depth: usize,
    semicolon_policy: SemicolonPolicy,
    external_clause_state: ExternalClauseState,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum SemicolonPolicy {
    Default,
    ForceSplit,
    CloseRoutineBlock,
    AwaitingImplicitTopLevelDecision,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ExternalClauseState {
    None,
    SawExternalKeyword,
    AwaitingLanguageTargetFromExternal,
    AwaitingLanguageTargetImplicit,
    SawImplicitLanguageTarget,
    Confirmed,
}

impl RoutineFrame {
    fn new(block_depth: usize) -> Self {
        Self {
            block_depth,
            semicolon_policy: SemicolonPolicy::Default,
            external_clause_state: ExternalClauseState::None,
        }
    }

    fn should_split_on_semicolon(self, current_block_depth: usize) -> bool {
        self.block_depth == current_block_depth
            && self.semicolon_policy == SemicolonPolicy::ForceSplit
    }

    fn should_close_routine_block_on_semicolon(self, current_block_depth: usize) -> bool {
        self.block_depth == current_block_depth
            && self.semicolon_policy == SemicolonPolicy::CloseRoutineBlock
    }

    fn mark_external_clause(&mut self) {
        self.semicolon_policy = if self.block_depth == 1 {
            SemicolonPolicy::ForceSplit
        } else {
            SemicolonPolicy::CloseRoutineBlock
        };
        self.external_clause_state = ExternalClauseState::Confirmed;
    }

    fn mark_implicit_language_target_on_semicolon(&mut self) {
        self.semicolon_policy = if self.block_depth == 1 {
            SemicolonPolicy::AwaitingImplicitTopLevelDecision
        } else {
            SemicolonPolicy::CloseRoutineBlock
        };
        self.external_clause_state = ExternalClauseState::Confirmed;
    }

    fn observe_external_clause_token(&mut self, token_upper: &str) {
        if matches!(
            self.external_clause_state,
            ExternalClauseState::AwaitingLanguageTargetFromExternal
                | ExternalClauseState::AwaitingLanguageTargetImplicit
        ) {
            let from_external = self.external_clause_state
                == ExternalClauseState::AwaitingLanguageTargetFromExternal;
            self.external_clause_state = ExternalClauseState::None;
            if is_external_language_target(token_upper) {
                if from_external {
                    self.mark_external_clause();
                } else {
                    self.external_clause_state = ExternalClauseState::SawImplicitLanguageTarget;
                }
                return;
            }
        }

        if token_upper == "EXTERNAL" {
            self.external_clause_state = ExternalClauseState::SawExternalKeyword;
            return;
        }

        if token_upper == "LANGUAGE" {
            self.external_clause_state =
                if self.external_clause_state == ExternalClauseState::SawExternalKeyword {
                    ExternalClauseState::AwaitingLanguageTargetFromExternal
                } else {
                    ExternalClauseState::AwaitingLanguageTargetImplicit
                };
            return;
        }

        if sql_text::is_external_language_clause_keyword(token_upper) {
            if matches!(
                self.external_clause_state,
                ExternalClauseState::SawExternalKeyword
                    | ExternalClauseState::SawImplicitLanguageTarget
                    | ExternalClauseState::Confirmed
            ) {
                self.mark_external_clause();
            }
            return;
        }

        if matches!(
            self.external_clause_state,
            ExternalClauseState::SawExternalKeyword
                | ExternalClauseState::SawImplicitLanguageTarget
        ) {
            self.external_clause_state = ExternalClauseState::None;
        }
    }

    fn finalize_external_clause_on_semicolon(&mut self) {
        if self.external_clause_state == ExternalClauseState::SawExternalKeyword {
            self.mark_external_clause();
            return;
        }

        if self.external_clause_state == ExternalClauseState::SawImplicitLanguageTarget {
            self.mark_implicit_language_target_on_semicolon();
        }
    }
}

impl PendingDo {
    fn arm_for_while(self, armed_at_block_depth: usize) -> Self {
        match self {
            Self::None => Self::While {
                armed_at_block_depth,
            },
            active => active,
        }
    }

    fn arm_for_for(self, armed_at_block_depth: usize) -> Self {
        match self {
            Self::None => Self::For {
                armed_at_block_depth,
            },
            active => active,
        }
    }

    fn resolve_do(self, current_block_depth: usize) -> Option<BlockKind> {
        match self {
            Self::While {
                armed_at_block_depth,
            } if armed_at_block_depth == current_block_depth => Some(BlockKind::While),
            Self::For {
                armed_at_block_depth,
            } if armed_at_block_depth == current_block_depth => Some(BlockKind::For),
            _ => None,
        }
    }

    fn arm_for_token(self, token_upper: &str, armed_at_block_depth: usize) -> Self {
        match token_upper {
            "WHILE" => self.arm_for_while(armed_at_block_depth),
            "FOR" => self.arm_for_for(armed_at_block_depth),
            _ => self,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
enum WithClauseState {
    #[default]
    None,
    PendingClause,
    InPlsqlDeclaration(WithDeclarationState),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum WithDeclarationState {
    CollectingDeclaration,
    AwaitingMainQuery,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
enum TopLevelTokenState {
    #[default]
    NoneSeen,
    Seen,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
enum TimingPointState {
    #[default]
    None,
    AwaitingAsOrIs,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
enum BeginState {
    #[default]
    None,
    AfterDeclare,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
enum AsIsState {
    #[default]
    None,
    AwaitingNestedSubprogram,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
enum AsIsFollowState {
    #[default]
    None,
    AwaitingTypeDeclarativeKind,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum IfSymbolEvent {
    Whitespace,
    OpenParen,
    Other,
}

impl IfSymbolEvent {
    fn from_char(ch: char) -> Self {
        if ch.is_whitespace() {
            return Self::Whitespace;
        }

        if ch == '(' {
            return Self::OpenParen;
        }

        Self::Other
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum SymbolRole {
    Semicolon,
    PendingEndSeparator,
    OpenParen,
    CloseParen,
    Other,
}

impl SymbolRole {
    fn from_char(ch: char, next: Option<char>) -> Self {
        if ch == ';' {
            return Self::Semicolon;
        }

        if ch == '(' {
            return Self::OpenParen;
        }

        if ch == ')' {
            return Self::CloseParen;
        }

        let is_pending_end_separator = matches!(
            ch,
            ',' | ')' | ']' | '}' | '+' | '*' | '%' | '=' | '<' | '>' | '|'
        ) || (ch == '-' && next != Some('-'))
            || (ch == '/' && next != Some('*'));

        if is_pending_end_separator {
            return Self::PendingEndSeparator;
        }

        Self::Other
    }

    fn resolves_pending_end(self) -> bool {
        matches!(self, Self::PendingEndSeparator | Self::CloseParen)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum EndResolutionPolicy {
    KeepCreateState,
    ResetCreateStateWhenTopLevel,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum AsIsBlockStart {
    None,
    Regular,
    TimingPoint,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum SemicolonAction {
    AppendToCurrent,
    SplitTopLevel,
    SplitForcedRoutine,
    CloseRoutineBlock,
}

impl SemicolonAction {
    pub(crate) fn from_state(state: &SplitState) -> Self {
        if state.keep_semicolons_inside_create_body() {
            return Self::AppendToCurrent;
        }

        if state.block_depth() == 0 && state.paren_depth == 0 && !state.in_with_plsql_declaration()
        {
            return Self::SplitTopLevel;
        }

        if state.paren_depth == 0 && state.should_split_on_semicolon() {
            return Self::SplitForcedRoutine;
        }

        if state.paren_depth == 0 && state.should_close_routine_block_on_semicolon() {
            return Self::CloseRoutineBlock;
        }

        Self::AppendToCurrent
    }
}

impl AsIsBlockStart {
    fn from_token(upper: &str, state: &SplitState) -> Self {
        if !matches!(upper, "AS" | "IS") {
            return Self::None;
        }

        if state.paren_depth != 0 {
            return Self::None;
        }

        if state.timing_point_state == TimingPointState::AwaitingAsOrIs {
            return Self::TimingPoint;
        }

        if state.is_trigger() && !state.in_compound_trigger() && state.block_depth() == 0 {
            // Simple trigger headers can legally include `REFERENCING ... AS ...` aliases.
            // Treating that `AS` as a routine body opener keeps the parser stuck inside
            // a synthetic block and prevents semicolon splitting for `CALL`-style bodies.
            return Self::None;
        }

        if state.as_is_state == AsIsState::AwaitingNestedSubprogram
            || (state.in_create_plsql()
                && !state.in_java_source_create()
                && state.block_depth() == 0)
        {
            return Self::Regular;
        }

        Self::None
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub(crate) enum CreateState {
    #[default]
    None,
    AwaitingObjectType,
    AwaitingJavaTarget,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
enum CreatePlsqlKind {
    #[default]
    None,
    Procedure,
    Function,
    Package,
    TypeSpecAwaitingBody,
    TypeSpec,
    TypeBody,
    Trigger(TriggerKind),
    JavaSource,
    Wrapped,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum TriggerKind {
    Simple,
    Compound,
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
    create_plsql_kind: CreatePlsqlKind,
    pub(crate) create_state: CreateState,
    begin_state: BeginState,
    as_is_follow_state: AsIsFollowState,
    as_is_state: AsIsState,
    pub(crate) pending_subprogram_begins: usize,
    routine_is_stack: Vec<RoutineFrame>,
    timing_point_state: TimingPointState,
    saw_compound_keyword: bool,

    // -- Parenthesis depth (for formatting / intellisense) --
    pub(crate) paren_depth: usize,

    // -- Oracle top-level WITH FUNCTION/PROCEDURE declarations --
    with_clause_state: WithClauseState,
    top_level_token_state: TopLevelTokenState,

    // -- Reusable buffer --
    token_upper_buf: String,
    pending_implicit_external_top_level_split: bool,
}

impl SplitState {
    fn active_routine_frame_mut(&mut self) -> Option<&mut RoutineFrame> {
        let current_depth = self.block_depth();
        self.routine_is_stack
            .last_mut()
            .filter(|frame| frame.block_depth == current_depth)
    }

    fn pop_case_block(&mut self) {
        if self.top_is_case() {
            let _ = self.block_stack.pop();
            return;
        }

        if let Some(pos) = self.block_stack.iter().rposition(|k| *k == BlockKind::Case) {
            self.block_stack.remove(pos);
            return;
        }

        let _ = self.block_stack.pop();
    }

    fn resolve_pending_end_with_policy(&mut self, policy: EndResolutionPolicy) {
        if self.pending_end != PendingEnd::End {
            return;
        }

        self.resolve_plain_end();
        if policy == EndResolutionPolicy::ResetCreateStateWhenTopLevel
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
        matches!(
            self.with_clause_state,
            WithClauseState::InPlsqlDeclaration(_)
        )
    }

    fn with_clause_waiting_main_query(&self) -> bool {
        matches!(
            self.with_clause_state,
            WithClauseState::InPlsqlDeclaration(WithDeclarationState::AwaitingMainQuery)
        )
    }

    pub(crate) fn has_pending_declare_begin(&self) -> bool {
        self.begin_state == BeginState::AfterDeclare
    }

    pub(crate) fn in_create_plsql(&self) -> bool {
        self.create_plsql_kind != CreatePlsqlKind::None
    }

    pub(crate) fn is_trigger(&self) -> bool {
        matches!(self.create_plsql_kind, CreatePlsqlKind::Trigger(_))
    }

    pub(crate) fn in_java_source_create(&self) -> bool {
        self.create_plsql_kind == CreatePlsqlKind::JavaSource
    }

    pub(crate) fn in_wrapped_create(&self) -> bool {
        self.create_plsql_kind == CreatePlsqlKind::Wrapped
    }

    fn keep_semicolons_inside_create_body(&self) -> bool {
        self.in_java_source_create() || self.in_wrapped_create()
    }

    fn in_compound_trigger(&self) -> bool {
        self.create_plsql_kind == CreatePlsqlKind::Trigger(TriggerKind::Compound)
    }

    fn mark_compound_trigger(&mut self) {
        if self.is_trigger() {
            self.create_plsql_kind = CreatePlsqlKind::Trigger(TriggerKind::Compound);
        }
    }

    fn type_as_is_awaits_declarative_kind(&self) -> bool {
        matches!(
            self.create_plsql_kind,
            CreatePlsqlKind::TypeSpecAwaitingBody | CreatePlsqlKind::TypeSpec
        )
    }

    fn needs_nested_begin_tracking(&self) -> bool {
        matches!(
            self.create_plsql_kind,
            CreatePlsqlKind::Package | CreatePlsqlKind::TypeBody
        )
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
        let at_top_level = self.block_depth() == 0 && self.paren_depth == 0;
        let at_statement_start =
            at_top_level && self.top_level_token_state == TopLevelTokenState::NoneSeen;
        let mut upper_buf = std::mem::take(&mut self.token_upper_buf);
        upper_buf.clear();
        upper_buf.push_str(&self.token);
        upper_buf.make_ascii_uppercase();
        let upper = upper_buf.as_str();

        self.handle_routine_is_external(upper);
        self.track_create_plsql(upper);
        self.track_top_level_with_plsql(upper, at_statement_start);

        let end_token_role =
            EndTokenRole::from_token(upper, self.pending_end, self.in_compound_trigger());

        self.handle_if_state_on_token(upper);
        self.handle_pending_end_on_token(end_token_role.suffix());
        self.handle_block_openers(upper, end_token_role);

        // Return the uppercase buffer so its capacity is reused.
        let _ = upper;
        self.token_upper_buf = upper_buf;
        self.token.clear();
        if at_top_level {
            self.top_level_token_state = TopLevelTokenState::Seen;
        }
    }

    /// Sub-handler: mark EXTERNAL/LANGUAGE/NAME/LIBRARY semicolon behavior.
    fn handle_routine_is_external(&mut self, upper: &str) {
        if let Some(frame) = self.active_routine_frame_mut() {
            frame.observe_external_clause_token(upper);
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

        if let Some(suffix) = suffix {
            if suffix == PendingEndSuffix::Case {
                self.pop_case_block();
            } else if let Some(kind) = suffix.closing_block_kind() {
                self.pop_block_of_kind(kind);
            }

            if suffix == PendingEndSuffix::TimingPoint {
                self.timing_point_state = TimingPointState::None;
            }
        } else {
            // Plain END – CASE expression or PL/SQL block
            self.resolve_plain_end();
        }

        self.pending_end = PendingEnd::None;
    }

    /// Sub-handler: process block-opening keywords (CASE, IF/THEN, LOOP, etc.).
    fn handle_block_openers(&mut self, upper: &str, end_token_role: EndTokenRole) {
        if self.saw_compound_keyword && upper != "TRIGGER" {
            self.saw_compound_keyword = false;
        }

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

        // WHILE/FOR ... DO
        if matches!(upper, "WHILE" | "FOR")
            && self.pending_end == PendingEnd::None
            && !(end_token_role.is_suffix(PendingEndSuffix::While)
                || end_token_role.is_suffix(PendingEndSuffix::For))
        {
            let is_trigger_header_for = upper == "FOR"
                && self.in_create_plsql()
                && self.is_trigger()
                && self.block_depth() == 0;
            if !is_trigger_header_for {
                self.pending_do =
                    std::mem::take(&mut self.pending_do).arm_for_token(upper, self.block_depth());
            }
        }

        if upper == "DO" {
            if let Some(block_kind) =
                std::mem::take(&mut self.pending_do).resolve_do(self.block_depth())
            {
                self.block_stack.push(block_kind);
            }
            self.pending_do = PendingDo::None;
        }

        // TYPE AS/IS OBJECT/VARRAY/TABLE/REF/RECORD/OPAQUE/ENUM – not a real block
        if self.as_is_follow_state == AsIsFollowState::AwaitingTypeDeclarativeKind
            && matches!(
                upper,
                "OBJECT"
                    | "VARRAY"
                    | "TABLE"
                    | "REF"
                    | "RECORD"
                    | "OPAQUE"
                    | "JSON"
                    | "VARYING"
                    | "ENUM"
                    | "RANGE"
            )
        {
            self.block_stack.pop(); // undo the AS/IS push
            self.as_is_follow_state = AsIsFollowState::None;
        }

        // Nested PROCEDURE/FUNCTION
        if self.block_depth() > 0 && matches!(upper, "PROCEDURE" | "FUNCTION") {
            self.as_is_state = AsIsState::AwaitingNestedSubprogram;
        }

        // AS/IS block start
        let as_is_block_start = AsIsBlockStart::from_token(upper, self);

        if as_is_block_start != AsIsBlockStart::None {
            if as_is_block_start == AsIsBlockStart::TimingPoint {
                self.block_stack.push(BlockKind::TimingPoint);
            } else {
                self.block_stack.push(BlockKind::AsIs);
            }
            if self.type_as_is_awaits_declarative_kind()
                && self.as_is_state != AsIsState::AwaitingNestedSubprogram
                && as_is_block_start != AsIsBlockStart::TimingPoint
            {
                self.as_is_follow_state = AsIsFollowState::AwaitingTypeDeclarativeKind;
            }
            self.as_is_state = AsIsState::None;
            self.timing_point_state = TimingPointState::None;
            let needs_begin_tracking = if self.needs_nested_begin_tracking() {
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
            self.begin_state = BeginState::AfterDeclare;
        } else if upper == "BEGIN" {
            if self.begin_state == BeginState::AfterDeclare {
                // DECLARE ... BEGIN – same block, don't push
                self.begin_state = BeginState::None;
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
        } else if upper == "COMPOUND" && self.is_trigger() && self.block_depth() == 0 {
            self.saw_compound_keyword = true;
        } else if upper == "TRIGGER"
            && self.saw_compound_keyword
            && self.is_trigger()
            && self.block_depth() == 0
        {
            self.mark_compound_trigger();
            self.block_stack.push(BlockKind::Compound);
            self.saw_compound_keyword = false;
        } else if matches!(upper, "BEFORE" | "AFTER" | "INSTEAD")
            && self.in_compound_trigger()
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
        let _ = self.block_stack.pop();
    }

    pub(crate) fn resolve_pending_end_on_separator(&mut self) {
        self.resolve_pending_end_with_policy(EndResolutionPolicy::KeepCreateState);
    }

    pub(crate) fn resolve_pending_end_on_terminator(&mut self) {
        self.resolve_pending_end_with_policy(EndResolutionPolicy::ResetCreateStateWhenTopLevel);
    }

    pub(crate) fn resolve_pending_end_on_eof(&mut self) {
        self.resolve_pending_end_with_policy(EndResolutionPolicy::ResetCreateStateWhenTopLevel);
    }

    fn advance_with_clause_after_semicolon(&mut self) {
        if self.in_with_plsql_declaration() && self.block_depth() == 0 && self.paren_depth == 0 {
            self.with_clause_state =
                WithClauseState::InPlsqlDeclaration(WithDeclarationState::AwaitingMainQuery);
        }
    }

    pub(crate) fn prepare_semicolon_action(&mut self) -> SemicolonAction {
        // FOR/WHILE ... DO candidates cannot span statement terminators.
        // Reset them so keywords like `FOR UPDATE; DO ...` don't create false loop depth.
        self.pending_do = PendingDo::None;
        self.finalize_external_clause_on_semicolon();
        self.resolve_pending_end_on_terminator();
        self.clear_forward_subprogram_declaration_state_on_semicolon();
        self.advance_with_clause_after_semicolon();
        SemicolonAction::from_state(self)
    }

    pub(crate) fn should_split_on_semicolon(&self) -> bool {
        self.routine_is_stack
            .last()
            .is_some_and(|frame| frame.should_split_on_semicolon(self.block_depth()))
    }

    pub(crate) fn can_terminate_on_slash(&self) -> bool {
        self.block_depth() == 0 || self.pending_implicit_external_top_level_split
    }

    fn should_close_routine_block_on_semicolon(&self) -> bool {
        self.routine_is_stack
            .last()
            .is_some_and(|frame| frame.should_close_routine_block_on_semicolon(self.block_depth()))
    }

    fn close_external_routine_on_semicolon(&mut self) {
        if !self.should_close_routine_block_on_semicolon() {
            return;
        }

        let _ = self.routine_is_stack.pop();
        self.pending_subprogram_begins = self.pending_subprogram_begins.saturating_sub(1);

        if self.block_stack.last() == Some(&BlockKind::AsIs) {
            let _ = self.block_stack.pop();
            return;
        }

        if let Some(pos) = self
            .block_stack
            .iter()
            .rposition(|kind| *kind == BlockKind::AsIs)
        {
            self.block_stack.remove(pos);
        }
    }

    pub(crate) fn apply_close_routine_block_on_semicolon(&mut self) {
        self.close_external_routine_on_semicolon();
    }

    fn clear_forward_subprogram_declaration_state_on_semicolon(&mut self) {
        // `PROCEDURE/FUNCTION name;` forward declarations inside package/type specs
        // should not leave nested-subprogram state armed for later `TYPE/SUBTYPE ... IS`.
        if self.as_is_state == AsIsState::AwaitingNestedSubprogram {
            self.as_is_state = AsIsState::None;
        }
    }

    fn finalize_external_clause_on_semicolon(&mut self) {
        if let Some(frame) = self.active_routine_frame_mut() {
            frame.finalize_external_clause_on_semicolon();
            if frame.semicolon_policy == SemicolonPolicy::AwaitingImplicitTopLevelDecision
                && frame.block_depth == 1
            {
                self.pending_implicit_external_top_level_split = true;
            }
        }
    }

    pub(crate) fn reset_create_state(&mut self) {
        self.create_plsql_kind = CreatePlsqlKind::None;
        self.create_state = CreateState::None;
        self.as_is_follow_state = AsIsFollowState::None;
        self.begin_state = BeginState::None;
        self.as_is_state = AsIsState::None;
        self.pending_subprogram_begins = 0;
        self.routine_is_stack.clear();
        self.timing_point_state = TimingPointState::None;
        self.saw_compound_keyword = false;
        self.pending_do = PendingDo::None;
        self.if_state = IfState::None;
        self.with_clause_state = WithClauseState::None;
        self.top_level_token_state = TopLevelTokenState::NoneSeen;
        self.pending_implicit_external_top_level_split = false;
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
        if self.create_plsql_kind == CreatePlsqlKind::TypeSpecAwaitingBody && upper == "BODY" {
            self.create_plsql_kind = CreatePlsqlKind::TypeBody;
            return;
        }

        if self.create_plsql_kind == CreatePlsqlKind::TypeSpecAwaitingBody && upper != "BODY" {
            self.create_plsql_kind = CreatePlsqlKind::TypeSpec;
        }

        if self.in_create_plsql() {
            if self.block_depth() == 0 && upper == "WRAPPED" {
                self.create_plsql_kind = CreatePlsqlKind::Wrapped;
                self.create_state = CreateState::None;
            }
            return;
        }

        if self.create_state == CreateState::AwaitingJavaTarget {
            match upper {
                "SOURCE" => {
                    self.create_plsql_kind = CreatePlsqlKind::JavaSource;
                    self.create_state = CreateState::None;
                    return;
                }
                "CLASS" | "RESOURCE" => {
                    self.create_state = CreateState::None;
                    return;
                }
                _ => {
                    self.create_state = CreateState::None;
                }
            }
        }

        if self.create_state == CreateState::AwaitingObjectType {
            match upper {
                "OR" => {
                    return;
                }
                "NO" | "FORCE" | "NOFORCE" | "REPLACE" | "AND" | "COMPILE" | "RESOLVE" => {
                    return;
                }
                "IF" | "NOT" | "EXISTS" => {
                    return;
                }
                "EDITIONABLE"
                | "NONEDITIONABLE"
                | "EDITIONING"
                | "NONEDITIONING"
                | "FORWARD"
                | "REVERSE"
                | "CROSSEDITION" => {
                    return;
                }
                "JAVA" => {
                    self.create_state = CreateState::AwaitingJavaTarget;
                    return;
                }
                "PROCEDURE" | "FUNCTION" | "PACKAGE" | "TYPE" | "TRIGGER" => {
                    self.create_plsql_kind = match upper {
                        "PROCEDURE" => CreatePlsqlKind::Procedure,
                        "FUNCTION" => CreatePlsqlKind::Function,
                        "PACKAGE" => CreatePlsqlKind::Package,
                        "TYPE" => CreatePlsqlKind::TypeSpecAwaitingBody,
                        "TRIGGER" => CreatePlsqlKind::Trigger(TriggerKind::Simple),
                        _ => CreatePlsqlKind::None,
                    };
                    self.create_state = CreateState::None;
                    return;
                }
                _ => {
                    self.create_state = CreateState::None;
                }
            }
        }

        if upper == "CREATE" {
            self.create_state = CreateState::AwaitingObjectType;
        }
    }

    fn track_top_level_with_plsql(&mut self, upper: &str, at_statement_start: bool) {
        if self.block_depth() != 0 || self.paren_depth != 0 {
            return;
        }

        // Oracle allows `WITH FUNCTION/PROCEDURE` inside top-level query contexts
        // that are not the very first token (e.g. CREATE VIEW ... AS WITH ...).
        // Start tracking on any top-level WITH while preserving active WITH states.
        let can_start_with_clause =
            at_statement_start || self.with_clause_state == WithClauseState::None;
        if upper == "WITH" && can_start_with_clause {
            self.with_clause_state = WithClauseState::PendingClause;
            return;
        }

        if self.with_clause_state == WithClauseState::None {
            return;
        }

        if sql_text::is_with_plsql_declaration_keyword(upper) {
            self.with_clause_state =
                WithClauseState::InPlsqlDeclaration(WithDeclarationState::CollectingDeclaration);
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
            return;
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

fn preview_identifier_upper(chars: &[char], start: usize) -> Option<String> {
    let first = chars.get(start).copied()?;
    if !sql_text::is_identifier_char(first) {
        return None;
    }

    let mut idx = start;
    let mut token = String::new();
    while let Some(ch) = chars.get(idx).copied() {
        if !sql_text::is_identifier_char(ch) {
            break;
        }
        token.push(ch);
        idx += 1;
    }
    token.make_ascii_uppercase();
    Some(token)
}

#[inline]
fn is_valid_q_quote_delimiter(delimiter: char) -> bool {
    !delimiter.is_whitespace()
}

#[inline]
fn is_external_language_target(token_upper: &str) -> bool {
    sql_text::is_external_language_target_keyword(token_upper)
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
        self.state.in_create_plsql()
    }

    pub(crate) fn block_depth(&self) -> usize {
        self.state.block_depth()
    }

    pub(crate) fn paren_depth(&self) -> usize {
        self.state.paren_depth
    }

    pub(crate) fn can_terminate_on_slash(&self) -> bool {
        self.state.can_terminate_on_slash()
    }

    pub(crate) fn is_trigger(&self) -> bool {
        self.state.is_trigger()
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

    fn apply_semicolon_action(&mut self, action: SemicolonAction, semicolon: char) {
        match action {
            SemicolonAction::AppendToCurrent => {
                self.current.push(semicolon);
            }
            SemicolonAction::SplitTopLevel => {
                self.push_current_statement();
                self.reset_statement_local_state();
                self.state.reset_create_state();
            }
            SemicolonAction::SplitForcedRoutine => {
                self.push_current_statement();
                self.reset_statement_local_state();
                self.state.reset_create_state();
                self.state.block_stack.clear();
            }
            SemicolonAction::CloseRoutineBlock => {
                self.current.push(semicolon);
                self.state.close_external_routine_on_semicolon();
            }
        }
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
                if self.state.pending_implicit_external_top_level_split
                    && self.state.block_depth() == 1
                    && self.state.paren_depth == 0
                    && self.state.token.is_empty()
                {
                    if let Some(candidate_upper) = preview_identifier_upper(chars, i) {
                        if candidate_upper == "BEGIN" {
                            self.state.pending_implicit_external_top_level_split = false;
                        } else if sql_text::is_with_main_query_keyword(&candidate_upper)
                            || sql_text::is_statement_head_keyword(&candidate_upper)
                        {
                            self.push_current_statement();
                            self.reset_statement_local_state();
                            self.state.reset_create_state();
                        } else {
                            self.state.pending_implicit_external_top_level_split = false;
                        }
                    }
                }

                if self.state.in_with_plsql_declaration()
                    && self.state.with_clause_waiting_main_query()
                    && self.state.block_depth() == 0
                    && self.state.paren_depth == 0
                {
                    if let Some(candidate_upper) = preview_identifier_upper(chars, i) {
                        if sql_text::is_statement_head_keyword(&candidate_upper)
                            && !sql_text::is_with_main_query_keyword(&candidate_upper)
                        {
                            self.push_current_statement();
                            self.reset_statement_local_state();
                            self.state.reset_create_state();
                        }
                    }
                }
                self.state.token.push(c);
                self.current.push(c);
                i += 1;
                continue;
            }

            self.state.flush_token();
            on_symbol(chars, i, c, next);
            let symbol_role = SymbolRole::from_char(c, next);

            // IF state machine on symbol characters
            match &self.state.if_state {
                IfState::ExpectConditionStart => {
                    match IfSymbolEvent::from_char(c) {
                        IfSymbolEvent::Whitespace => {
                            // Keep waiting.
                        }
                        IfSymbolEvent::OpenParen => {
                            let condition_depth = self.state.paren_depth.saturating_add(1);
                            self.state.if_state = IfState::InConditionParen {
                                depth: condition_depth,
                            };
                        }
                        IfSymbolEvent::Other => {
                            self.state.if_state = IfState::AwaitingThen;
                        }
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
            if symbol_role == SymbolRole::CloseParen {
                if let IfState::InConditionParen { depth } = self.state.if_state {
                    if depth == self.state.paren_depth {
                        self.state.if_state = IfState::AfterConditionParen;
                    }
                }
            }

            // Track parenthesis depth
            match symbol_role {
                SymbolRole::OpenParen => {
                    self.state.paren_depth += 1;
                }
                SymbolRole::CloseParen => {
                    self.state.paren_depth = self.state.paren_depth.saturating_sub(1);
                }
                _ => {}
            }

            // Pending END on separator
            if self.state.pending_end == PendingEnd::End && symbol_role.resolves_pending_end() {
                self.state.resolve_pending_end_on_separator();
            }

            if symbol_role == SymbolRole::Semicolon {
                let semicolon_action = self.state.prepare_semicolon_action();
                self.apply_semicolon_action(semicolon_action, c);
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

        if self.state.is_idle()
            && self.state.block_depth() == 0
            && self.state.paren_depth == 0
            && !self.state.in_with_plsql_declaration()
            && sql_text::is_auto_terminated_tool_command(line)
        {
            self.push_current_statement();
            self.reset_statement_local_state();
            self.state.reset_create_state();
        }

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

    pub(crate) fn prepare_slash_terminator(&mut self) {
        if self.state.pending_end == PendingEnd::End && self.state.is_idle() {
            self.state.resolve_pending_end_on_terminator();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BlockKind, CreatePlsqlKind, CreateState, EndTokenRole, ExternalClauseState, IfState,
        IfSymbolEvent, PendingDo, PendingEnd, PendingEndSuffix, RoutineFrame, SemicolonAction,
        SemicolonPolicy, SplitState, SqlParserEngine, SymbolRole, TimingPointState, TriggerKind,
        WithClauseState, WithDeclarationState,
    };

    #[test]
    fn semicolon_action_classifies_top_level_split() {
        let state = SplitState::default();
        assert_eq!(
            SemicolonAction::from_state(&state),
            SemicolonAction::SplitTopLevel
        );
    }

    #[test]
    fn semicolon_action_keeps_with_clause_declaration_statement_open() {
        let state = SplitState {
            with_clause_state: WithClauseState::InPlsqlDeclaration(
                WithDeclarationState::AwaitingMainQuery,
            ),
            ..SplitState::default()
        };
        assert_eq!(
            SemicolonAction::from_state(&state),
            SemicolonAction::AppendToCurrent
        );
    }

    #[test]
    fn semicolon_action_detects_forced_routine_split() {
        let mut state = SplitState::default();
        state.block_stack.push(BlockKind::AsIs);
        state.routine_is_stack.push(RoutineFrame {
            block_depth: 1,
            semicolon_policy: SemicolonPolicy::ForceSplit,
            external_clause_state: ExternalClauseState::Confirmed,
        });
        assert_eq!(
            SemicolonAction::from_state(&state),
            SemicolonAction::SplitForcedRoutine
        );
    }

    #[test]
    fn semicolon_action_closes_nested_external_routine_without_split() {
        let mut state = SplitState::default();
        state.block_stack.push(BlockKind::AsIs);
        state.block_stack.push(BlockKind::AsIs);
        state.routine_is_stack.push(RoutineFrame {
            block_depth: 2,
            semicolon_policy: SemicolonPolicy::CloseRoutineBlock,
            external_clause_state: ExternalClauseState::Confirmed,
        });
        assert_eq!(
            SemicolonAction::from_state(&state),
            SemicolonAction::CloseRoutineBlock
        );
    }

    #[test]
    fn semicolon_action_keeps_java_source_statement_open_at_top_level() {
        let state = SplitState {
            create_plsql_kind: CreatePlsqlKind::JavaSource,
            ..SplitState::default()
        };

        assert_eq!(
            SemicolonAction::from_state(&state),
            SemicolonAction::AppendToCurrent
        );
    }

    #[test]
    fn if_symbol_event_classifies_characters() {
        assert_eq!(IfSymbolEvent::from_char(' '), IfSymbolEvent::Whitespace);
        assert_eq!(IfSymbolEvent::from_char('('), IfSymbolEvent::OpenParen);
        assert_eq!(IfSymbolEvent::from_char('A'), IfSymbolEvent::Other);
    }

    #[test]
    fn symbol_role_classifies_semicolon_and_pending_end_separators() {
        assert_eq!(SymbolRole::from_char(';', None), SymbolRole::Semicolon);
        assert_eq!(SymbolRole::from_char('/', Some('*')), SymbolRole::Other);
        assert_eq!(
            SymbolRole::from_char('/', Some('1')),
            SymbolRole::PendingEndSeparator
        );
        assert_eq!(SymbolRole::from_char(')', None), SymbolRole::CloseParen);
        assert!(SymbolRole::from_char(')', None).resolves_pending_end());
        assert!(SymbolRole::from_char('/', Some('1')).resolves_pending_end());
        assert!(!SymbolRole::from_char('(', None).resolves_pending_end());
    }

    #[test]
    fn create_state_transitions_to_plsql_on_create_or_replace_function() {
        let mut state = SplitState::default();

        state.track_create_plsql("CREATE");
        assert_eq!(state.create_state, CreateState::AwaitingObjectType);

        state.track_create_plsql("OR");
        assert_eq!(state.create_state, CreateState::AwaitingObjectType);

        state.track_create_plsql("REPLACE");
        assert_eq!(state.create_state, CreateState::AwaitingObjectType);

        state.track_create_plsql("FUNCTION");

        assert!(state.in_create_plsql());
        assert_eq!(state.create_state, CreateState::None);
    }

    #[test]
    fn create_state_clears_when_non_plsql_target_follows_create() {
        let mut state = SplitState::default();

        state.track_create_plsql("CREATE");
        assert_eq!(state.create_state, CreateState::AwaitingObjectType);

        state.track_create_plsql("TABLE");

        assert!(!state.in_create_plsql());
        assert_eq!(state.create_state, CreateState::None);
    }

    #[test]
    fn create_state_transitions_to_java_source_on_create_and_compile_java_source() {
        let mut state = SplitState::default();

        state.track_create_plsql("CREATE");
        assert_eq!(state.create_state, CreateState::AwaitingObjectType);

        state.track_create_plsql("OR");
        assert_eq!(state.create_state, CreateState::AwaitingObjectType);

        state.track_create_plsql("REPLACE");
        assert_eq!(state.create_state, CreateState::AwaitingObjectType);

        state.track_create_plsql("AND");
        assert_eq!(state.create_state, CreateState::AwaitingObjectType);

        state.track_create_plsql("COMPILE");
        assert_eq!(state.create_state, CreateState::AwaitingObjectType);

        state.track_create_plsql("JAVA");
        assert_eq!(state.create_state, CreateState::AwaitingJavaTarget);

        state.track_create_plsql("SOURCE");

        assert!(state.in_create_plsql());
        assert_eq!(state.create_plsql_kind, CreatePlsqlKind::JavaSource);
        assert_eq!(state.create_state, CreateState::None);
    }

    #[test]
    fn create_state_accepts_noforce_modifier_before_trigger() {
        let mut state = SplitState::default();

        state.track_create_plsql("CREATE");
        assert_eq!(state.create_state, CreateState::AwaitingObjectType);

        state.track_create_plsql("NOFORCE");
        assert_eq!(state.create_state, CreateState::AwaitingObjectType);

        state.track_create_plsql("TRIGGER");

        assert!(state.in_create_plsql());
        assert_eq!(
            state.create_plsql_kind,
            CreatePlsqlKind::Trigger(TriggerKind::Simple)
        );
    }

    #[test]
    fn create_state_accepts_if_not_exists_before_procedure() {
        let mut state = SplitState::default();

        state.track_create_plsql("CREATE");
        assert_eq!(state.create_state, CreateState::AwaitingObjectType);

        state.track_create_plsql("IF");
        assert_eq!(state.create_state, CreateState::AwaitingObjectType);

        state.track_create_plsql("NOT");
        assert_eq!(state.create_state, CreateState::AwaitingObjectType);

        state.track_create_plsql("EXISTS");
        assert_eq!(state.create_state, CreateState::AwaitingObjectType);

        state.track_create_plsql("PROCEDURE");

        assert!(state.in_create_plsql());
        assert_eq!(state.create_plsql_kind, CreatePlsqlKind::Procedure);
    }

    #[test]
    fn declare_begin_state_machine_tracks_pending_begin() {
        let mut state = SplitState::default();

        state.handle_block_openers("DECLARE", EndTokenRole::None);
        assert!(state.has_pending_declare_begin());
        assert_eq!(state.block_depth(), 1);

        state.handle_block_openers("BEGIN", EndTokenRole::None);
        assert!(!state.has_pending_declare_begin());
        assert_eq!(state.block_depth(), 1);
    }

    #[test]
    fn nested_subprogram_as_is_state_machine_resets_after_is() {
        let mut state = SplitState {
            block_stack: vec![BlockKind::Begin],
            ..SplitState::default()
        };

        state.handle_block_openers("PROCEDURE", EndTokenRole::None);
        state.handle_block_openers("IS", EndTokenRole::None);

        assert_eq!(state.block_depth(), 2);
    }

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
        engine.state.pending_do = PendingDo::For {
            armed_at_block_depth: 0,
        };
        engine.state.if_state = IfState::AwaitingThen;
        engine.state.paren_depth = 0;

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
            pending_do: PendingDo::While {
                armed_at_block_depth: 0,
            },
            ..SplitState::default()
        };

        state.handle_block_openers("FOR", EndTokenRole::None);
        assert_eq!(
            state.pending_do,
            PendingDo::While {
                armed_at_block_depth: 0
            }
        );

        state.handle_block_openers("DO", EndTokenRole::None);
        assert_eq!(state.block_depth(), 1);
        assert_eq!(state.block_stack.last(), Some(&BlockKind::While));
        assert_eq!(state.pending_do, PendingDo::None);
    }

    #[test]
    fn pending_do_arms_when_no_active_candidate_exists() {
        let mut state = SplitState::default();

        state.handle_block_openers("FOR", EndTokenRole::None);
        assert_eq!(
            state.pending_do,
            PendingDo::For {
                armed_at_block_depth: 0
            }
        );

        state.handle_block_openers("DO", EndTokenRole::None);
        assert_eq!(state.block_stack.last(), Some(&BlockKind::For));
        assert_eq!(state.pending_do, PendingDo::None);
    }

    #[test]
    fn pending_do_requires_matching_block_depth_for_do_resolution() {
        let mut state = SplitState::default();

        state.handle_block_openers("FOR", EndTokenRole::None);
        state.block_stack.push(BlockKind::Begin);
        state.handle_block_openers("DO", EndTokenRole::None);

        assert_eq!(state.block_depth(), 1);
        assert_eq!(state.block_stack.last(), Some(&BlockKind::Begin));
        assert_eq!(state.pending_do, PendingDo::None);
    }

    #[test]
    fn semicolon_split_for_external_routine_resets_transient_state() {
        let mut engine = SqlParserEngine::new();
        engine.current.push_str("LANGUAGE C");
        engine.state.block_stack.push(BlockKind::AsIs);
        engine.state.routine_is_stack.push(RoutineFrame {
            block_depth: 1,
            semicolon_policy: SemicolonPolicy::ForceSplit,
            external_clause_state: ExternalClauseState::Confirmed,
        });
        engine.state.pending_end = PendingEnd::End;
        engine.state.pending_do = PendingDo::While {
            armed_at_block_depth: 1,
        };
        engine.state.if_state = IfState::AfterConditionParen;
        engine.state.paren_depth = 0;

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
    fn close_external_routine_semicolon_only_closes_nested_routine_block() {
        let mut state = SplitState {
            block_stack: vec![BlockKind::AsIs, BlockKind::AsIs],
            pending_subprogram_begins: 1,
            routine_is_stack: vec![RoutineFrame {
                block_depth: 2,
                semicolon_policy: SemicolonPolicy::CloseRoutineBlock,
                external_clause_state: ExternalClauseState::Confirmed,
            }],
            ..SplitState::default()
        };

        state.close_external_routine_on_semicolon();

        assert_eq!(state.block_stack, vec![BlockKind::AsIs]);
        assert_eq!(state.pending_subprogram_begins, 0);
        assert!(state.routine_is_stack.is_empty());
    }
    #[test]
    fn separator_resolution_keeps_create_state() {
        let mut state = SplitState {
            pending_end: PendingEnd::End,
            create_plsql_kind: CreatePlsqlKind::Procedure,
            block_stack: vec![BlockKind::Begin],
            ..SplitState::default()
        };

        state.resolve_pending_end_on_separator();

        assert_eq!(state.pending_end, PendingEnd::None);
        assert_eq!(state.block_depth(), 0);
        assert!(state.in_create_plsql());
    }

    #[test]
    fn terminator_resolution_resets_create_state_at_top_level() {
        let mut state = SplitState {
            pending_end: PendingEnd::End,
            create_plsql_kind: CreatePlsqlKind::Procedure,
            block_stack: vec![BlockKind::Begin],
            ..SplitState::default()
        };

        state.resolve_pending_end_on_terminator();

        assert_eq!(state.pending_end, PendingEnd::None);
        assert_eq!(state.block_depth(), 0);
        assert!(!state.in_create_plsql());
    }

    #[test]
    fn eof_resolution_preserves_with_plsql_declaration_mode() {
        let mut state = SplitState {
            pending_end: PendingEnd::End,
            create_plsql_kind: CreatePlsqlKind::Procedure,
            with_clause_state: WithClauseState::InPlsqlDeclaration(
                WithDeclarationState::AwaitingMainQuery,
            ),
            block_stack: vec![BlockKind::Begin],
            ..SplitState::default()
        };

        state.resolve_pending_end_on_eof();

        assert_eq!(state.pending_end, PendingEnd::None);
        assert_eq!(state.block_depth(), 0);
        assert!(state.in_create_plsql());
        assert!(state.in_with_plsql_declaration());
    }

    #[test]
    fn statement_with_midstream_with_keyword_does_not_enter_with_plsql_mode() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("SELECT col WITH FROM t;");

        assert_eq!(
            engine.take_statements(),
            vec!["SELECT col WITH FROM t".to_string()]
        );
        assert!(!engine.state.in_with_plsql_declaration());
    }

    #[test]
    fn with_function_waiting_main_query_recovers_on_new_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH");
        engine.process_line("  FUNCTION f RETURN NUMBER IS");
        engine.process_line("  BEGIN");
        engine.process_line("    RETURN 1;");
        engine.process_line("  END;");
        engine.process_line("CREATE TABLE t_recover_with_fn (id NUMBER);");
        engine.process_line("SELECT 2 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
            "first statement should keep only WITH declaration: {}",
            statements[0]
        );
        assert_eq!(
            statements[1],
            "CREATE TABLE t_recover_with_fn (id NUMBER)".to_string()
        );
        assert_eq!(statements[2], "SELECT 2 FROM dual".to_string());
    }

    #[test]
    fn with_function_waiting_main_query_recovers_on_conn_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH");
        engine.process_line("  FUNCTION f RETURN NUMBER IS");
        engine.process_line("  BEGIN");
        engine.process_line("    RETURN 1;");
        engine.process_line("  END;");
        engine.process_line("CONN scott/tiger");
        engine.process_line("SELECT 2 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with(
                "WITH
  FUNCTION f RETURN NUMBER IS"
            ),
            "first statement should keep only WITH declaration: {}",
            statements[0]
        );
        assert_eq!(statements[1], "CONN scott/tiger".to_string());
        assert_eq!(statements[2], "SELECT 2 FROM dual".to_string());
    }

    #[test]
    fn with_function_waiting_main_query_recovers_on_disc_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH");
        engine.process_line("  FUNCTION f RETURN NUMBER IS");
        engine.process_line("  BEGIN");
        engine.process_line("    RETURN 1;");
        engine.process_line("  END;");
        engine.process_line("DISC");
        engine.process_line("SELECT 2 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with(
                "WITH
  FUNCTION f RETURN NUMBER IS"
            ),
            "first statement should keep only WITH declaration: {}",
            statements[0]
        );
        assert_eq!(statements[1], "DISC".to_string());
        assert_eq!(statements[2], "SELECT 2 FROM dual".to_string());
    }
    #[test]
    fn create_view_as_with_function_keeps_statement_open_until_main_select_terminator() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE VIEW v_with_fn AS");
        engine.process_line("WITH");
        engine.process_line("  FUNCTION f RETURN NUMBER IS");
        engine.process_line("  BEGIN");
        engine.process_line("    RETURN 1;");
        engine.process_line("  END;");
        engine.process_line("SELECT f() AS v FROM dual;");
        engine.process_line("SELECT 2 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("CREATE OR REPLACE VIEW v_with_fn AS"),
            "first statement should preserve CREATE VIEW header: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("FUNCTION f RETURN NUMBER IS"),
            "first statement should preserve WITH FUNCTION declaration: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("SELECT f() AS v FROM dual"),
            "first statement should include main SELECT body: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 2 FROM dual"));
    }

    #[test]
    fn create_view_as_with_procedure_keeps_statement_open_until_main_select_terminator() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE VIEW v_with_proc AS");
        engine.process_line("WITH");
        engine.process_line("  PROCEDURE p IS");
        engine.process_line("  BEGIN");
        engine.process_line("    NULL;");
        engine.process_line("  END;");
        engine.process_line("SELECT 1 AS v FROM dual;");
        engine.process_line("SELECT 2 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("CREATE OR REPLACE VIEW v_with_proc AS"),
            "first statement should preserve CREATE VIEW header: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("PROCEDURE p IS"),
            "first statement should preserve WITH PROCEDURE declaration: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("SELECT 1 AS v FROM dual"),
            "first statement should include main SELECT body: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 2 FROM dual"));
    }

    #[test]
    fn with_function_keeps_statement_open_until_main_merge_terminator() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH FUNCTION pick_id RETURN NUMBER IS");
        engine.process_line("BEGIN");
        engine.process_line("  RETURN 1;");
        engine.process_line("END;");
        engine.process_line("MERGE INTO target_table t");
        engine.process_line("USING dual d");
        engine.process_line("ON (t.id = pick_id())");
        engine.process_line("WHEN MATCHED THEN UPDATE SET t.val = 'Y';");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "expected merge + select split");
        assert!(
            statements[0].starts_with("WITH FUNCTION pick_id RETURN NUMBER IS"),
            "first statement should preserve WITH FUNCTION header: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("WHEN MATCHED THEN UPDATE SET t.val = 'Y'"),
            "first statement should include MERGE body: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn create_noneditionable_package_body_with_external_library_stays_single_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE NONEDITIONABLE PACKAGE BODY pkg_ext AS");
        engine.process_line("  FUNCTION ext_call RETURN NUMBER IS");
        engine.process_line("  EXTERNAL LIBRARY extlib LANGUAGE C;");
        engine.process_line("END pkg_ext;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "expected package body + select split");
        assert_eq!(
            statements[0],
            "CREATE OR REPLACE NONEDITIONABLE PACKAGE BODY pkg_ext AS\n  FUNCTION ext_call RETURN NUMBER IS\n  EXTERNAL LIBRARY extlib LANGUAGE C;\nEND pkg_ext".to_string()
        );
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn compound_trigger_with_each_row_timing_point_splits_on_outer_end() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_compound_each_row");
        engine.process_line("FOR INSERT ON t");
        engine.process_line("COMPOUND TRIGGER");
        engine.process_line("  BEFORE EACH ROW IS");
        engine.process_line("  BEGIN");
        engine.process_line("    NULL;");
        engine.process_line("  END BEFORE EACH ROW;");
        engine.process_line("END;");
        engine.process_line("SELECT 3 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END BEFORE EACH ROW"),
            "first statement should preserve EACH ROW timing point closure: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 3 FROM dual"));
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
    fn type_spec_as_is_follow_state_is_cleared_by_declarative_kind_token() {
        let mut state = SplitState {
            create_plsql_kind: CreatePlsqlKind::TypeSpec,
            ..SplitState::default()
        };

        state.handle_block_openers("AS", EndTokenRole::None);
        assert_eq!(state.block_stack.last(), Some(&BlockKind::AsIs));

        state.handle_block_openers("OBJECT", EndTokenRole::None);
        assert!(state.block_stack.is_empty());
    }

    #[test]
    fn type_body_as_is_does_not_clear_on_type_declarative_kind_tokens() {
        let mut state = SplitState {
            create_plsql_kind: CreatePlsqlKind::TypeBody,
            ..SplitState::default()
        };

        state.handle_block_openers("AS", EndTokenRole::None);
        assert_eq!(state.block_stack.last(), Some(&BlockKind::AsIs));

        state.handle_block_openers("TABLE", EndTokenRole::None);
        assert_eq!(state.block_stack.last(), Some(&BlockKind::AsIs));
    }

    #[test]
    fn compound_trigger_timing_point_uses_dedicated_block_kind() {
        let mut state = SplitState {
            create_plsql_kind: CreatePlsqlKind::Trigger(TriggerKind::Compound),
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

    #[test]
    fn compound_trigger_requires_compound_trigger_keyword_pair() {
        let mut state = SplitState {
            create_plsql_kind: CreatePlsqlKind::Trigger(TriggerKind::Simple),
            ..SplitState::default()
        };

        state.handle_block_openers("COMPOUND", EndTokenRole::None);
        assert!(!state.block_stack.contains(&BlockKind::Compound));
        assert_eq!(
            state.create_plsql_kind,
            CreatePlsqlKind::Trigger(TriggerKind::Simple)
        );

        state.handle_block_openers("IS", EndTokenRole::None);
        assert!(!state.block_stack.contains(&BlockKind::Compound));
        assert_eq!(
            state.create_plsql_kind,
            CreatePlsqlKind::Trigger(TriggerKind::Simple)
        );
    }

    #[test]
    fn compound_trigger_header_still_splits_after_end() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_compound");
        engine.process_line("FOR INSERT ON t");
        engine.process_line("COMPOUND TRIGGER");
        engine.process_line("  BEFORE STATEMENT IS");
        engine.process_line("  BEGIN");
        engine.process_line("    NULL;");
        engine.process_line("  END BEFORE STATEMENT;");
        engine.process_line("END;");
        engine.process_line("SELECT 2 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("CREATE OR REPLACE TRIGGER trg_compound"),
            "first statement should preserve COMPOUND TRIGGER body: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 2 FROM dual"));
    }

    #[test]
    fn package_with_nested_external_procedure_does_not_split_mid_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PACKAGE BODY pkg AS");
        engine.process_line("  PROCEDURE ext_proc IS");
        engine.process_line("  EXTERNAL NAME \"ext_proc\" LANGUAGE C;");
        engine.process_line("END pkg;");

        assert_eq!(
            engine.finalize_and_take_statements(),
            vec![
                "CREATE OR REPLACE PACKAGE BODY pkg AS\n  PROCEDURE ext_proc IS\n  EXTERNAL NAME \"ext_proc\" LANGUAGE C;\nEND pkg".to_string()
            ]
        );
    }

    #[test]
    fn name_language_library_identifiers_do_not_activate_external_clause_policy() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PROCEDURE proc_shadow IS");
        engine.process_line("  name NUMBER := 1;");
        engine.process_line("  language NUMBER := 2;");
        engine.process_line("  library NUMBER := 3;");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].starts_with("CREATE OR REPLACE PROCEDURE proc_shadow IS"));
        assert!(statements[0].contains("name NUMBER := 1;"));
        assert!(statements[0].contains("language NUMBER := 2;"));
        assert!(statements[0].contains("library NUMBER := 3;"));
        assert!(statements[0].contains("END"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn external_clause_keywords_used_as_identifiers_do_not_force_external_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PROCEDURE proc_shadow_external IS");
        engine.process_line("  external NUMBER := 1;");
        engine.process_line("  parameters NUMBER := 2;");
        engine.process_line("  calling NUMBER := 3;");
        engine.process_line("  with NUMBER := 4;");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].starts_with("CREATE OR REPLACE PROCEDURE proc_shadow_external IS"));
        assert!(statements[0].contains("external NUMBER := 1;"));
        assert!(statements[0].contains("parameters NUMBER := 2;"));
        assert!(statements[0].contains("calling NUMBER := 3;"));
        assert!(statements[0].contains("with NUMBER := 4;"));
        assert!(statements[0].contains("END"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn language_identifier_with_language_target_like_datatype_does_not_force_external_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PROCEDURE proc_shadow_c IS");
        engine.process_line("  language c;");
        engine.process_line("  language java;");
        engine.process_line("  language javascript;");
        engine.process_line("  language python;");
        engine.process_line("  marker NUMBER := 1;");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].starts_with("CREATE OR REPLACE PROCEDURE proc_shadow_c IS"));
        assert!(statements[0].contains("language c;"));
        assert!(statements[0].contains("language java;"));
        assert!(statements[0].contains("language javascript;"));
        assert!(statements[0].contains("language python;"));
        assert!(statements[0].contains("marker NUMBER := 1;"));
        assert!(statements[0].contains("END"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn language_clause_with_parameters_without_external_keyword_still_marks_external_routine_split()
    {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_lang_params RETURN NUMBER");
        engine.process_line("AS LANGUAGE C PARAMETERS (CONTEXT) ;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE C PARAMETERS (CONTEXT)"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn language_clause_without_external_name_or_parameters_still_marks_external_routine_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_lang_only RETURN NUMBER");
        engine.process_line("AS LANGUAGE C;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE C"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn external_clause_without_language_target_still_marks_external_routine_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_external_only RETURN NUMBER");
        engine.process_line("AS EXTERNAL;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS EXTERNAL"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn language_clause_without_external_keyword_still_marks_external_routine_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_lang_only RETURN NUMBER");
        engine.process_line("AS LANGUAGE C NAME 'ext_lang_only';");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE C NAME 'ext_lang_only'"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn language_clause_with_calling_standard_without_external_keyword_marks_external_routine_split()
    {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_calling RETURN NUMBER");
        engine.process_line("AS LANGUAGE C CALLING STANDARD;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("LANGUAGE C CALLING STANDARD"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn simple_trigger_call_body_splits_on_semicolon_without_slash() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_call");
        engine.process_line("BEFORE INSERT ON t");
        engine.process_line("CALL do_work;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("CALL do_work"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn simple_trigger_when_clause_splits_on_semicolon_without_slash() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_when");
        engine.process_line("BEFORE INSERT ON t");
        engine.process_line("FOR EACH ROW");
        engine.process_line("WHEN (NEW.id > 0)");
        engine.process_line("CALL do_work;");
        engine.process_line("SELECT 2 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("WHEN (NEW.id > 0)"));
        assert!(statements[0].contains("CALL do_work"));
        assert!(statements[1].starts_with("SELECT 2 FROM dual"));
    }

    #[test]
    fn trigger_referencing_alias_as_does_not_block_call_body_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_ref_alias");
        engine.process_line("BEFORE INSERT ON t");
        engine.process_line("REFERENCING NEW AS n OLD AS o");
        engine.process_line("FOR EACH ROW");
        engine.process_line("CALL do_work;");
        engine.process_line("SELECT 3 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("REFERENCING NEW AS n OLD AS o"));
        assert!(statements[0].contains("CALL do_work"));
        assert!(statements[1].starts_with("SELECT 3 FROM dual"));
    }

    #[test]
    fn language_clause_with_with_context_without_external_keyword_marks_external_routine_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_with_context RETURN NUMBER");
        engine.process_line("AS LANGUAGE C WITH CONTEXT;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("LANGUAGE C WITH CONTEXT"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn create_forward_crossedition_trigger_splits_before_trailing_select() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FORWARD CROSSEDITION TRIGGER trg_forward");
        engine.process_line("BEFORE INSERT ON t");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("CREATE OR REPLACE FORWARD CROSSEDITION TRIGGER"),
            "first statement should preserve trigger header: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn create_reverse_crossedition_trigger_splits_before_trailing_select() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE REVERSE CROSSEDITION TRIGGER trg_reverse");
        engine.process_line("BEFORE INSERT ON t");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END;");
        engine.process_line("SELECT 2 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("CREATE OR REPLACE REVERSE CROSSEDITION TRIGGER"),
            "first statement should preserve trigger header: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 2 FROM dual"));
    }

    #[test]
    fn type_varying_array_declaration_splits_at_semicolon() {
        let mut engine = SqlParserEngine::new();

        engine.process_line(
            "CREATE OR REPLACE TYPE phone_list_t IS VARYING ARRAY(10) OF VARCHAR2(25);",
        );
        engine.process_line("SELECT 1 FROM dual;");

        assert_eq!(
            engine.finalize_and_take_statements(),
            vec![
                "CREATE OR REPLACE TYPE phone_list_t IS VARYING ARRAY(10) OF VARCHAR2(25)"
                    .to_string(),
                "SELECT 1 FROM dual".to_string(),
            ]
        );
    }

    #[test]
    fn type_enum_declaration_splits_at_semicolon() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TYPE color_t AS ENUM ('RED', 'GREEN');");
        engine.process_line("SELECT 1 FROM dual;");

        assert_eq!(
            engine.finalize_and_take_statements(),
            vec![
                "CREATE OR REPLACE TYPE color_t AS ENUM ('RED', 'GREEN')".to_string(),
                "SELECT 1 FROM dual".to_string(),
            ]
        );
    }

    #[test]
    fn type_range_declaration_splits_at_semicolon() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TYPE age_t AS RANGE (SUBTYPE = NUMBER);");
        engine.process_line("SELECT 1 FROM dual;");

        assert_eq!(
            engine.finalize_and_take_statements(),
            vec![
                "CREATE OR REPLACE TYPE age_t AS RANGE (SUBTYPE = NUMBER)".to_string(),
                "SELECT 1 FROM dual".to_string(),
            ]
        );
    }

    #[test]
    fn type_range_declaration_with_is_keyword_splits_at_semicolon() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TYPE age_t IS RANGE (SUBTYPE = NUMBER);");
        engine.process_line("SELECT 1 FROM dual;");

        assert_eq!(
            engine.finalize_and_take_statements(),
            vec![
                "CREATE OR REPLACE TYPE age_t IS RANGE (SUBTYPE = NUMBER)".to_string(),
                "SELECT 1 FROM dual".to_string(),
            ]
        );
    }

    #[test]
    fn type_body_local_table_type_declaration_does_not_split_member_body() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TYPE BODY t_local_types AS");
        engine.process_line("  MEMBER PROCEDURE p IS");
        engine.process_line("    TYPE num_tab IS TABLE OF NUMBER;");
        engine.process_line("  BEGIN");
        engine.process_line("    NULL;");
        engine.process_line("  END;");
        engine.process_line("END t_local_types;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("CREATE OR REPLACE TYPE BODY t_local_types AS"),
            "first statement should preserve TYPE BODY header: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("TYPE num_tab IS TABLE OF NUMBER;"),
            "local TABLE type declaration should remain in TYPE BODY: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("END t_local_types"),
            "TYPE BODY should close at final END: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn type_body_local_ref_cursor_type_declaration_does_not_split_member_body() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TYPE BODY t_local_ref AS");
        engine.process_line("  MEMBER PROCEDURE p IS");
        engine.process_line("    TYPE rc_t IS REF CURSOR;");
        engine.process_line("  BEGIN");
        engine.process_line("    NULL;");
        engine.process_line("  END;");
        engine.process_line("END t_local_ref;");
        engine.process_line("SELECT 2 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("CREATE OR REPLACE TYPE BODY t_local_ref AS"),
            "first statement should preserve TYPE BODY header: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("TYPE rc_t IS REF CURSOR;"),
            "local REF CURSOR type declaration should remain in TYPE BODY: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("END t_local_ref"),
            "TYPE BODY should close at final END: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 2 FROM dual"));
    }

    #[test]
    fn end_with_label_closes_block_and_splits_next_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END done_label;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2);
        assert!(statements[0].contains("END done_label"));
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn compound_trigger_for_each_row_header_does_not_affect_statement_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_compound_each_row");
        engine.process_line("FOR UPDATE ON t");
        engine.process_line("COMPOUND TRIGGER");
        engine.process_line("  BEFORE EACH ROW IS");
        engine.process_line("  BEGIN");
        engine.process_line("    NULL;");
        engine.process_line("  END BEFORE EACH ROW;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "expected trigger + select split");
        assert!(
            statements[0].contains("END BEFORE EACH ROW"),
            "compound trigger body should remain intact: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn with_function_followed_by_recursive_with_query_stays_single_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH FUNCTION f RETURN NUMBER IS");
        engine.process_line("BEGIN");
        engine.process_line("  RETURN 1;");
        engine.process_line("END;");
        engine.process_line("WITH r (n) AS (");
        engine.process_line("  SELECT 1 FROM dual");
        engine.process_line("  UNION ALL");
        engine.process_line("  SELECT n + 1 FROM r WHERE n < 3");
        engine.process_line(")");
        engine.process_line("SELECT * FROM r;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 1, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("WITH r (n) AS"),
            "recursive WITH should stay attached to WITH FUNCTION statement: {}",
            statements[0]
        );
        assert!(
            statements[0].ends_with("SELECT * FROM r"),
            "main query should remain attached: {}",
            statements[0]
        );
    }

    #[test]
    fn with_function_followed_by_non_recursive_with_query_stays_single_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH FUNCTION f RETURN NUMBER IS");
        engine.process_line("BEGIN");
        engine.process_line("  RETURN 1;");
        engine.process_line("END;");
        engine.process_line("WITH cte AS (SELECT f() AS v FROM dual)");
        engine.process_line("SELECT v FROM cte;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 1, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("WITH cte AS"),
            "CTE WITH should be treated as a valid main query head: {}",
            statements[0]
        );
    }

    #[test]
    fn with_clause_multiple_plsql_declarations_keep_main_query_attached() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH");
        engine.process_line("  FUNCTION f RETURN NUMBER IS");
        engine.process_line("  BEGIN");
        engine.process_line("    RETURN 1;");
        engine.process_line("  END;");
        engine.process_line("  PROCEDURE p IS");
        engine.process_line("  BEGIN");
        engine.process_line("    NULL;");
        engine.process_line("  END;");
        engine.process_line("SELECT f() FROM dual;");
        engine.process_line("SELECT 2 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("FUNCTION f RETURN NUMBER IS"),
            "first statement should contain WITH FUNCTION declaration: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("PROCEDURE p IS"),
            "first statement should contain WITH PROCEDURE declaration: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("SELECT f() FROM dual"),
            "first statement should include the main query: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 2 FROM dual"));
    }

    #[test]
    fn compound_trigger_instead_of_each_row_section_splits_on_outer_end() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_compound_instead");
        engine.process_line("INSTEAD OF INSERT ON v_orders");
        engine.process_line("COMPOUND TRIGGER");
        engine.process_line("  INSTEAD OF EACH ROW IS");
        engine.process_line("  BEGIN");
        engine.process_line("    NULL;");
        engine.process_line("  END INSTEAD OF EACH ROW;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END INSTEAD OF EACH ROW"),
            "compound trigger timing-point END must stay inside trigger body: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn compound_trigger_after_statement_section_splits_on_outer_end() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_compound_after_stmt");
        engine.process_line("FOR UPDATE ON t");
        engine.process_line("COMPOUND TRIGGER");
        engine.process_line("  AFTER STATEMENT IS");
        engine.process_line("  BEGIN");
        engine.process_line("    NULL;");
        engine.process_line("  END AFTER STATEMENT;");
        engine.process_line("END;");
        engine.process_line("SELECT 7 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END AFTER STATEMENT"),
            "compound trigger statement timing-point END must stay inside trigger body: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 7 FROM dual".to_string());
    }

    #[test]
    fn with_function_followed_by_insert_all_stays_single_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH FUNCTION normalize_id(p_id NUMBER) RETURN NUMBER IS");
        engine.process_line("BEGIN");
        engine.process_line("  RETURN p_id;");
        engine.process_line("END;");
        engine.process_line("INSERT ALL");
        engine.process_line("  INTO audit_log(id) VALUES (normalize_id(1))");
        engine.process_line("SELECT 1 FROM dual;");
        engine.process_line("SELECT 2 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("WITH FUNCTION normalize_id"),
            "first statement should preserve WITH FUNCTION declaration: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("INSERT ALL"),
            "main INSERT ALL query should remain attached: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("VALUES (normalize_id(1))"),
            "INSERT ALL branches should remain attached: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 2 FROM dual".to_string());
    }

    #[test]
    fn with_procedure_followed_by_values_statement_stays_single_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH PROCEDURE touch_ctx IS");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END;");
        engine.process_line("VALUES (1);");
        engine.process_line("SELECT 3 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("WITH PROCEDURE touch_ctx IS"),
            "first statement should preserve WITH PROCEDURE declaration: {}",
            statements[0]
        );
        assert!(
            statements[0].ends_with("VALUES (1)"),
            "VALUES main query should remain attached: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 3 FROM dual".to_string());
    }
}
