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
    fn from_token(
        token_upper: &str,
        pending_end: PendingEnd,
        allow_timing_point_suffix: bool,
    ) -> Self {
        if pending_end != PendingEnd::End {
            return Self::None;
        }

        PendingEndSuffix::parse(token_upper, allow_timing_point_suffix)
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
    fn parse(token_upper: &str, allow_timing_point_suffix: bool) -> Option<Self> {
        match token_upper {
            "CASE" => Some(Self::Case),
            "IF" => Some(Self::If),
            "LOOP" => Some(Self::Loop),
            "WHILE" => Some(Self::While),
            "REPEAT" => Some(Self::Repeat),
            "FOR" => Some(Self::For),
            "BEFORE" | "AFTER" | "INSTEAD" if allow_timing_point_suffix => Some(Self::TimingPoint),
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

    fn apply_to_state(self, state: &mut SplitState) {
        if self == Self::Case {
            state.pop_case_block();
        } else if self == Self::TimingPoint {
            state.pop_timing_point_block();
        } else if let Some(kind) = self.closing_block_kind() {
            state.pop_block_of_kind(kind);
        }

        if self == Self::TimingPoint {
            state.timing_point_state = TimingPointState::None;
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
    SawUsingClauseSubject,
    SawMleKeyword,
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

    fn observe_external_clause_token(&mut self, token_upper: &str, allow_implicit_language: bool) {
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

            if from_external && token_upper.bytes().all(sql_text::is_identifier_byte) {
                self.mark_external_clause();
                return;
            }

            if from_external && sql_text::is_external_language_clause_keyword(token_upper) {
                // Be permissive for malformed call specs such as
                // `EXTERNAL LANGUAGE PARAMETERS ...` without an explicit
                // language target. Once `EXTERNAL` was observed, subsequent
                // call-spec tokens still belong to an external routine clause
                // and semicolon handling should keep routine boundaries stable.
                self.mark_external_clause();
                return;
            }
        }

        if token_upper == "EXTERNAL" {
            self.external_clause_state = ExternalClauseState::SawExternalKeyword;
            return;
        }

        if matches!(token_upper, "AGGREGATE" | "PIPELINED") {
            self.external_clause_state = ExternalClauseState::SawUsingClauseSubject;
            return;
        }

        if token_upper == "MLE" {
            if matches!(
                self.external_clause_state,
                ExternalClauseState::SawImplicitLanguageTarget | ExternalClauseState::Confirmed
            ) {
                self.mark_external_clause();
            } else {
                self.external_clause_state = ExternalClauseState::SawMleKeyword;
            }
            return;
        }

        if matches!(
            token_upper,
            "MODULE" | "SIGNATURE" | "ENV" | "ENVIRONMENT"
        ) {
            if matches!(
                self.external_clause_state,
                ExternalClauseState::SawMleKeyword | ExternalClauseState::Confirmed
            ) {
                self.mark_external_clause();
            }
            return;
        }

        if token_upper == "USING" {
            if self.external_clause_state == ExternalClauseState::SawUsingClauseSubject {
                self.mark_external_clause();
            }
            return;
        }

        if self.external_clause_state == ExternalClauseState::SawUsingClauseSubject
            && matches!(token_upper, "ROW" | "SCALAR" | "TABLE" | "POLYMORPHIC")
        {
            return;
        }

        if token_upper == "LANGUAGE" {
            if self.external_clause_state == ExternalClauseState::SawExternalKeyword {
                self.external_clause_state =
                    ExternalClauseState::AwaitingLanguageTargetFromExternal;
            } else if allow_implicit_language {
                self.external_clause_state = ExternalClauseState::AwaitingLanguageTargetImplicit;
            } else {
                self.external_clause_state = ExternalClauseState::None;
            }
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
                | ExternalClauseState::SawUsingClauseSubject
                | ExternalClauseState::SawMleKeyword
        ) {
            self.external_clause_state = ExternalClauseState::None;
        }
    }

    fn observe_external_clause_literal_target(&mut self, allow_implicit_target: bool) {
        let from_external = match self.external_clause_state {
            ExternalClauseState::AwaitingLanguageTargetFromExternal => true,
            ExternalClauseState::AwaitingLanguageTargetImplicit => false,
            _ => return,
        };

        self.external_clause_state = ExternalClauseState::None;
        if from_external {
            self.mark_external_clause();
        } else if allow_implicit_target {
            self.external_clause_state = ExternalClauseState::SawImplicitLanguageTarget;
        }
    }

    fn observe_external_clause_symbol(&mut self, ch: char, next: Option<char>) {
        if !matches!(
            self.external_clause_state,
            ExternalClauseState::AwaitingLanguageTargetFromExternal
                | ExternalClauseState::AwaitingLanguageTargetImplicit
        ) {
            return;
        }

        if ch.is_whitespace() {
            return;
        }

        let is_canceling_symbol = matches!(
            ch,
            ':' | '='
                | '+'
                | '*'
                | '%'
                | '<'
                | '>'
                | '|'
                | ','
                | '.'
                | '('
                | ')'
                | '['
                | ']'
                | '{'
                | '}'
        ) || (ch == '-' && next != Some('-'))
            || (ch == '/' && next != Some('*'));

        if is_canceling_symbol {
            self.external_clause_state = ExternalClauseState::None;
        }
    }

    fn finalize_external_clause_on_semicolon(&mut self, allow_implicit_target_split: bool) {
        match self.external_clause_state {
            ExternalClauseState::SawExternalKeyword
            | ExternalClauseState::AwaitingLanguageTargetFromExternal => {
                self.mark_external_clause();
            }
            ExternalClauseState::SawImplicitLanguageTarget
            | ExternalClauseState::AwaitingLanguageTargetImplicit => {
                if allow_implicit_target_split {
                    self.mark_implicit_language_target_on_semicolon();
                } else {
                    self.external_clause_state = ExternalClauseState::None;
                }
            }
            _ => {}
        }
    }
}

impl PendingDo {
    fn arm_for_token(self, token_upper: &str, armed_at_block_depth: usize) -> Self {
        if self != Self::None {
            return self;
        }

        match token_upper {
            "WHILE" => Self::While {
                armed_at_block_depth,
            },
            "FOR" => Self::For {
                armed_at_block_depth,
            },
            _ => Self::None,
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

        if state.is_trigger()
            && !state.in_compound_trigger()
            && state.block_depth() == 0
            && state.saw_trigger_alias_subject
            && matches!(upper, "AS" | "IS")
        {
            // In simple trigger headers, `REFERENCING NEW/OLD/PARENT AS|IS alias`
            // uses AS/IS for alias clauses, not for opening the declarative section.
            // Ignore only that alias form so `FOR EACH ROW AS ... BEGIN ... END;`
            // still enters the trigger body block correctly.
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
    token_prefixed_with_dollar: bool,

    // -- CREATE PL/SQL tracking --
    create_plsql_kind: CreatePlsqlKind,
    pub(crate) create_state: CreateState,
    begin_state: BeginState,
    as_is_follow_state: AsIsFollowState,
    as_is_state: AsIsState,
    pub(crate) pending_subprogram_begins: usize,
    pending_sql_macro_call_spec: bool,
    routine_is_stack: Vec<RoutineFrame>,
    timing_point_state: TimingPointState,
    saw_compound_keyword: bool,
    saw_trigger_alias_subject: bool,

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
    fn active_routine_frame(&self) -> Option<&RoutineFrame> {
        let current_depth = self.block_depth();
        self.routine_is_stack
            .last()
            .filter(|frame| frame.block_depth == current_depth)
    }

    fn active_routine_frame_mut(&mut self) -> Option<&mut RoutineFrame> {
        let current_depth = self.block_depth();
        self.routine_is_stack
            .last_mut()
            .filter(|frame| frame.block_depth == current_depth)
    }

    fn should_split_before_implicit_external_begin_block(&self, token_upper: &str) -> bool {
        if token_upper != "BEGIN" {
            return false;
        }

        if self.block_depth() != 1 || self.paren_depth != 0 {
            return false;
        }

        self.active_routine_frame().is_some_and(|frame| {
            matches!(
                frame.external_clause_state,
                ExternalClauseState::SawImplicitLanguageTarget
                    | ExternalClauseState::AwaitingLanguageTargetImplicit
            ) && (sql_text::is_with_main_query_keyword(token_upper)
                || sql_text::is_statement_head_keyword(token_upper))
        })
    }

    fn should_split_before_implicit_external_statement_head(&self, token_upper: &str) -> bool {
        if self.block_depth() != 1 || self.paren_depth != 0 {
            return false;
        }

        if !sql_text::is_statement_head_keyword(token_upper)
            || sql_text::is_external_language_clause_keyword(token_upper)
        {
            return false;
        }

        self.active_routine_frame().is_some_and(|frame| {
            matches!(
                frame.external_clause_state,
                ExternalClauseState::SawImplicitLanguageTarget
                    | ExternalClauseState::AwaitingLanguageTargetImplicit
            )
        })
    }

    fn should_split_begin_after_implicit_external_semicolon(&self, token_upper: &str) -> bool {
        if token_upper != "BEGIN"
            || self.block_depth() != 1
            || self.paren_depth != 0
            || !self.pending_implicit_external_top_level_split
        {
            return false;
        }

        if !matches!(self.create_plsql_kind, CreatePlsqlKind::Function) {
            return false;
        }

        self.active_routine_frame().is_some_and(|frame| {
            frame.semicolon_policy == SemicolonPolicy::AwaitingImplicitTopLevelDecision
        })
    }

    fn pop_case_block(&mut self) {
        if self.top_is_case() {
            let _ = self.block_stack.pop();
            return;
        }

        if let Some(pos) = self.block_stack.iter().rposition(|k| *k == BlockKind::Case) {
            self.block_stack.remove(pos);
        }
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

    fn awaiting_external_language_target(&self) -> bool {
        self.routine_is_stack.last().is_some_and(|frame| {
            matches!(
                frame.external_clause_state,
                ExternalClauseState::AwaitingLanguageTargetFromExternal
                    | ExternalClauseState::AwaitingLanguageTargetImplicit
            )
        })
    }

    fn keep_semicolons_inside_create_body(&self) -> bool {
        self.in_java_source_create() || self.in_wrapped_create()
    }

    fn in_compound_trigger(&self) -> bool {
        self.create_plsql_kind == CreatePlsqlKind::Trigger(TriggerKind::Compound)
    }

    fn allow_timing_point_end_suffix(&self) -> bool {
        if !self.in_compound_trigger() {
            return false;
        }

        self.block_stack
            .iter()
            .rev()
            .find(|kind| !matches!(**kind, BlockKind::Begin | BlockKind::Declare))
            .is_some_and(|kind| *kind == BlockKind::TimingPoint)
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
        self.track_sql_macro_call_spec(upper);
        self.track_top_level_with_plsql(upper, at_statement_start);

        let token_prefixed_with_dollar = self.token_prefixed_with_dollar;
        let end_token_role = if token_prefixed_with_dollar {
            EndTokenRole::None
        } else {
            EndTokenRole::from_token(
                upper,
                self.pending_end,
                self.allow_timing_point_end_suffix(),
            )
        };

        if !token_prefixed_with_dollar {
            self.handle_if_state_on_token(upper);
            self.handle_pending_end_on_token(end_token_role.suffix());
            self.handle_block_openers(upper, end_token_role);
        }

        // Return the uppercase buffer so its capacity is reused.
        let _ = upper;
        self.token_upper_buf = upper_buf;
        self.token.clear();
        self.token_prefixed_with_dollar = false;
        if at_top_level {
            self.top_level_token_state = TopLevelTokenState::Seen;
        }
    }

    /// Sub-handler: mark EXTERNAL/LANGUAGE/NAME/LIBRARY semicolon behavior.
    fn handle_routine_is_external(&mut self, upper: &str) {
        let should_track = self.block_depth() > 1
            || matches!(
                self.create_plsql_kind,
                CreatePlsqlKind::Procedure | CreatePlsqlKind::Function
            );

        if !should_track {
            return;
        }

        let allow_implicit_language = self.block_depth() > 1
            || (self.block_depth() == 1
                && matches!(
                    self.create_plsql_kind,
                    CreatePlsqlKind::Function | CreatePlsqlKind::Procedure
                ));

        if let Some(frame) = self.active_routine_frame_mut() {
            frame.observe_external_clause_token(upper, allow_implicit_language);
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
            suffix.apply_to_state(self);
        } else {
            // Plain END – CASE expression or PL/SQL block
            self.resolve_plain_end();
        }

        self.pending_end = PendingEnd::None;
    }

    /// Sub-handler: process block-opening keywords (CASE, IF/THEN, LOOP, etc.).
    fn handle_block_openers(&mut self, upper: &str, end_token_role: EndTokenRole) {
        if self.is_trigger() && !self.in_compound_trigger() && self.block_depth() == 0 {
            if matches!(upper, "NEW" | "OLD" | "PARENT") {
                self.saw_trigger_alias_subject = true;
            } else if !matches!(upper, "AS" | "IS") {
                self.saw_trigger_alias_subject = false;
            }
        } else {
            self.saw_trigger_alias_subject = false;
        }

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

        if upper == "CALL"
            && self.is_trigger()
            && !self.in_compound_trigger()
            && self.block_depth() == 1
            && self.block_stack.last() == Some(&BlockKind::AsIs)
            && self.pending_subprogram_begins > 0
        {
            if let Some(frame) = self.active_routine_frame_mut() {
                frame.semicolon_policy = SemicolonPolicy::ForceSplit;
            }
            self.pending_subprogram_begins = 0;
        }

        if upper == "BEGIN"
            && self.timing_point_state == TimingPointState::AwaitingAsOrIs
            && self.in_compound_trigger()
        {
            self.block_stack.push(BlockKind::TimingPoint);
            self.as_is_state = AsIsState::None;
            self.timing_point_state = TimingPointState::None;
            self.routine_is_stack
                .push(RoutineFrame::new(self.block_depth()));
            self.pending_subprogram_begins += 1;
        }

        // CREATE TYPE (spec) AS/IS <declarative-kind> is never a PL/SQL block opener.
        // We still keep an allow-list for known Oracle kinds, but also fall back to
        // the same behavior for forward-compatible kinds that may appear in newer
        // Oracle versions.
        if self.as_is_follow_state == AsIsFollowState::AwaitingTypeDeclarativeKind {
            let known_type_declarative_kind = matches!(
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
            );

            // `BODY` is handled by CREATE TYPE BODY classification and should not
            // appear as a declarative kind token for CREATE TYPE specs.
            if known_type_declarative_kind || upper != "BODY" {
                self.block_stack.pop(); // undo the AS/IS push
                self.as_is_follow_state = AsIsFollowState::None;
            }
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
                if self.pending_sql_macro_call_spec {
                    if let Some(frame) = self.routine_is_stack.last_mut() {
                        frame.mark_external_clause();
                    }
                    self.pending_sql_macro_call_spec = false;
                }
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
            && self.block_stack.last() == Some(&BlockKind::Compound)
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

    fn pop_timing_point_block(&mut self) {
        if let Some(pos) = self
            .block_stack
            .iter()
            .rposition(|kind| *kind == BlockKind::TimingPoint)
        {
            self.block_stack.truncate(pos);
        } else {
            self.pop_block_of_kind(BlockKind::TimingPoint);
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
            return;
        }

        if self.with_clause_state == WithClauseState::PendingClause
            && self.block_depth() == 0
            && self.paren_depth == 0
        {
            self.with_clause_state = WithClauseState::None;
        }
    }

    pub(crate) fn prepare_semicolon_action(&mut self) -> SemicolonAction {
        self.pending_sql_macro_call_spec = false;
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
        self.block_depth() == 0
            || self.pending_implicit_external_top_level_split
            || (self.paren_depth == 0 && self.should_split_on_semicolon())
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
        let allow_implicit_target_split = true;

        if let Some(frame) = self.active_routine_frame_mut() {
            frame.finalize_external_clause_on_semicolon(allow_implicit_target_split);
            if frame.semicolon_policy == SemicolonPolicy::AwaitingImplicitTopLevelDecision
                && frame.block_depth == 1
            {
                self.pending_implicit_external_top_level_split = true;
            }
        }
    }

    fn observe_external_clause_literal_target(&mut self, allow_implicit_target: bool) {
        let should_track = self.block_depth() > 1
            || matches!(
                self.create_plsql_kind,
                CreatePlsqlKind::Procedure | CreatePlsqlKind::Function
            );

        if !should_track {
            return;
        }

        if let Some(frame) = self.active_routine_frame_mut() {
            frame.observe_external_clause_literal_target(allow_implicit_target);
        }
    }

    fn allow_implicit_external_literal_target(&self) -> bool {
        self.active_routine_frame().is_some_and(|frame| {
            frame.external_clause_state == ExternalClauseState::AwaitingLanguageTargetImplicit
        })
    }

    fn observe_external_clause_symbol(&mut self, ch: char, next: Option<char>) {
        if let Some(frame) = self.active_routine_frame_mut() {
            frame.observe_external_clause_symbol(ch, next);
        }
    }

    fn consume_trigger_alias_subject_on_quoted_identifier(&mut self) {
        if self.is_trigger() && !self.in_compound_trigger() && self.block_depth() == 0 {
            self.saw_trigger_alias_subject = false;
        }
    }

    pub(crate) fn reset_create_state(&mut self) {
        self.create_plsql_kind = CreatePlsqlKind::None;
        self.create_state = CreateState::None;
        self.as_is_follow_state = AsIsFollowState::None;
        self.begin_state = BeginState::None;
        self.as_is_state = AsIsState::None;
        self.pending_subprogram_begins = 0;
        self.pending_sql_macro_call_spec = false;
        self.routine_is_stack.clear();
        self.timing_point_state = TimingPointState::None;
        self.saw_compound_keyword = false;
        self.saw_trigger_alias_subject = false;
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
                // Modifiers that appear between CREATE and the object type keyword
                "OR" | "NO" | "FORCE" | "NOFORCE" | "REPLACE" | "AND" | "COMPILE" | "RESOLVE"
                | "IF" | "NOT" | "EXISTS" | "EDITIONABLE" | "NONEDITIONABLE" | "EDITIONING"
                | "NONEDITIONING" | "FORWARD" | "REVERSE" | "CROSSEDITION" | "SHARING"
                | "METADATA" | "DATA" | "EXTENDED" | "NONE" => return,

                // TYPE BODY member modifiers — only skip when inside type body
                "MEMBER" | "STATIC" | "CONSTRUCTOR" | "MAP" | "ORDER" | "FINAL"
                | "INSTANTIABLE" | "OVERRIDING"
                    if self.create_plsql_kind == CreatePlsqlKind::TypeBody =>
                {
                    return
                }

                "JAVA" => {
                    self.create_state = CreateState::AwaitingJavaTarget;
                    return;
                }
                "PROCEDURE" => {
                    self.create_plsql_kind = CreatePlsqlKind::Procedure;
                }
                "FUNCTION" => {
                    self.create_plsql_kind = CreatePlsqlKind::Function;
                }
                "PACKAGE" => {
                    self.create_plsql_kind = CreatePlsqlKind::Package;
                }
                "TYPE" => {
                    self.create_plsql_kind = CreatePlsqlKind::TypeSpecAwaitingBody;
                }
                "TRIGGER" => {
                    self.create_plsql_kind = CreatePlsqlKind::Trigger(TriggerKind::Simple);
                }
                _ => {}
            }
            self.create_state = CreateState::None;
        }

        if upper == "CREATE" {
            self.create_state = CreateState::AwaitingObjectType;
        }
    }

    fn track_sql_macro_call_spec(&mut self, upper: &str) {
        if upper != "SQL_MACRO" {
            return;
        }

        let top_level_function_macro =
            self.create_plsql_kind == CreatePlsqlKind::Function && self.in_create_plsql();
        let nested_function_macro =
            self.block_depth() > 0 && self.as_is_state == AsIsState::AwaitingNestedSubprogram;
        let function_call_spec_macro =
            self.block_stack.last() == Some(&BlockKind::AsIs) && self.pending_subprogram_begins > 0;

        if !(top_level_function_macro || nested_function_macro || function_call_spec_macro) {
            return;
        }

        self.pending_sql_macro_call_spec = true;
        if let Some(frame) = self.active_routine_frame_mut() {
            frame.mark_external_clause();
            self.pending_sql_macro_call_spec = false;
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

        if self.with_clause_state == WithClauseState::PendingClause
            && sql_text::is_with_non_plsql_clause_keyword(upper)
        {
            self.with_clause_state = WithClauseState::None;
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
            // `TABLE` can appear inside a WITH FUNCTION declaration signature,
            // e.g. `RETURN VARCHAR2 SQL_MACRO(TABLE)`. Only switch out of
            // declaration mode once the declaration has been closed by `;`
            // and we are explicitly awaiting the main query.
            if self.with_clause_state
                == WithClauseState::InPlsqlDeclaration(WithDeclarationState::CollectingDeclaration)
            {
                return;
            }

            self.with_clause_state = WithClauseState::None;
            return;
        }
    }

    fn track_with_main_query_symbol(&mut self, ch: char) {
        if !self.with_clause_waiting_main_query()
            || self.block_depth() != 0
            || self.paren_depth != 0
        {
            return;
        }

        if ch == '(' {
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

#[inline]
fn looks_like_oracle_conditional_compilation_flag(chars: &[char], start: usize) -> bool {
    if chars.get(start).copied() != Some('$') || chars.get(start + 1).copied() != Some('$') {
        return false;
    }

    chars
        .get(start + 2)
        .copied()
        .is_some_and(sql_text::is_identifier_start_char)
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

fn chars_starts_with_ascii_case_insensitive(chars: &[char], start: usize, pattern: &str) -> bool {
    let mut idx = start;
    for pattern_ch in pattern.chars() {
        let Some(candidate) = chars.get(idx).copied() else {
            return false;
        };

        if !candidate.eq_ignore_ascii_case(&pattern_ch) {
            return false;
        }

        idx += 1;
    }

    true
}

#[inline]
fn is_valid_q_quote_delimiter(delimiter: char) -> bool {
    !delimiter.is_whitespace() && delimiter != '\''
}

#[inline]
fn is_external_language_target(token_upper: &str) -> bool {
    sql_text::is_external_language_target_keyword(token_upper)
}

/// Check if `chars[marker_idx]` is the first non-whitespace character on its
/// line, i.e. only whitespace (or start-of-input) precedes it since the last
/// newline.  The expected character must match `expected`.
fn is_line_leading_char(chars: &[char], marker_idx: usize, expected: char) -> bool {
    if chars.get(marker_idx).copied() != Some(expected) {
        return false;
    }

    let mut lookbehind = marker_idx;
    while lookbehind > 0 {
        let prev_idx = lookbehind - 1;
        let Some(prev) = chars.get(prev_idx).copied() else {
            break;
        };
        if prev == '\n' {
            break;
        }
        if !prev.is_whitespace() {
            return false;
        }
        lookbehind = prev_idx;
    }

    true
}

fn is_line_leading_slash_marker(chars: &[char], marker_idx: usize) -> bool {
    if !is_line_leading_char(chars, marker_idx, '/') {
        return false;
    }

    // After `/`, only whitespace, newline, block/line comment, or REM/REMARK is allowed.
    let mut idx = marker_idx + 1;
    loop {
        while idx < chars.len() && chars[idx] != '\n' && chars[idx].is_whitespace() {
            idx += 1;
        }

        if idx >= chars.len() || chars[idx] == '\n' {
            return true;
        }

        if chars[idx] == '/' && chars.get(idx + 1).copied() == Some('*') {
            let mut lookahead = idx + 2;
            let mut closed = false;
            while lookahead + 1 < chars.len() {
                if chars[lookahead] == '*' && chars[lookahead + 1] == '/' {
                    idx = lookahead + 2;
                    closed = true;
                    break;
                }
                lookahead += 1;
            }

            if !closed {
                return false;
            }

            continue;
        }

        break;
    }


    if chars[idx] == '-' && chars.get(idx + 1).copied() == Some('-') {
        return true;
    }

    let is_ascii_boundary = |offset: usize| {
        chars
            .get(offset)
            .copied()
            .is_none_or(|ch| ch.is_whitespace())
    };

    chars_starts_with_ascii_case_insensitive(chars, idx, "REM") && is_ascii_boundary(idx + 3)
        || chars_starts_with_ascii_case_insensitive(chars, idx, "REMARK")
            && is_ascii_boundary(idx + 6)
}

// ---------------------------------------------------------------------------
// SqlParserEngine
// ---------------------------------------------------------------------------

pub(crate) struct SqlParserEngine {
    pub(crate) state: SplitState,
    current: String,
    statements: Vec<String>,
    scratch_chars: Vec<char>,
    preview_identifier_upper_buf: String,
}

impl SqlParserEngine {
    pub(crate) fn new() -> Self {
        Self {
            state: SplitState::default(),
            current: String::new(),
            statements: Vec::new(),
            scratch_chars: Vec::new(),
            preview_identifier_upper_buf: String::new(),
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

    pub(crate) fn has_pending_end(&self) -> bool {
        self.state.pending_end == PendingEnd::End
    }

    pub(crate) fn is_trigger(&self) -> bool {
        self.state.is_trigger()
    }

    fn reset_statement_local_state(&mut self) {
        self.state.pending_end = PendingEnd::None;
        self.state.pending_do = PendingDo::None;
        self.state.if_state = IfState::None;
        self.state.paren_depth = 0;
        self.state.with_clause_state = WithClauseState::None;
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

    fn split_current_statement(&mut self) {
        self.push_current_statement();
        self.reset_statement_local_state();
        self.state.reset_create_state();
    }

    fn split_current_and_reset_external_boundary(&mut self) {
        self.split_current_statement();
        self.state.block_stack.clear();
    }

    fn with_preview_identifier_upper<R, F>(
        &mut self,
        chars: &[char],
        start: usize,
        f: F,
    ) -> Option<R>
    where
        F: FnOnce(&str, &mut Self) -> R,
    {
        let mut upper_buf = std::mem::take(&mut self.preview_identifier_upper_buf);
        upper_buf.clear();

        let first = chars.get(start).copied()?;
        if !sql_text::is_identifier_char(first) {
            self.preview_identifier_upper_buf = upper_buf;
            return None;
        }

        let mut idx = start;
        while let Some(ch) = chars.get(idx).copied() {
            if !sql_text::is_identifier_char(ch) {
                break;
            }
            upper_buf.push(ch);
            idx += 1;
        }
        upper_buf.make_ascii_uppercase();

        let result = f(upper_buf.as_str(), self);
        self.preview_identifier_upper_buf = upper_buf;
        Some(result)
    }

    fn handle_identifier_start_candidate(&mut self, chars: &[char], i: usize) {
        let should_preview = self.state.token.is_empty()
            && ((self.state.block_depth() == 1 && self.state.paren_depth == 0)
                || (self.state.in_with_plsql_declaration()
                    && self.state.block_depth() == 0
                    && self.state.paren_depth == 0));

        if !should_preview {
            return;
        }

        let _ = self.with_preview_identifier_upper(chars, i, |candidate_upper, this| {
            if this.state.block_depth() == 1 && this.state.paren_depth == 0 {
                if this
                    .state
                    .should_split_begin_after_implicit_external_semicolon(candidate_upper)
                {
                    this.split_current_and_reset_external_boundary();
                } else if candidate_upper == "BEGIN"
                    && this.state.pending_implicit_external_top_level_split
                {
                    this.state.pending_implicit_external_top_level_split = false;
                } else if this.state.pending_implicit_external_top_level_split
                    && (sql_text::is_with_main_query_keyword(candidate_upper)
                        || sql_text::is_statement_head_keyword(candidate_upper))
                {
                    this.split_current_and_reset_external_boundary();
                } else if this
                    .state
                    .should_split_before_implicit_external_begin_block(candidate_upper)
                {
                    this.split_current_and_reset_external_boundary();
                } else if this
                    .state
                    .should_split_before_implicit_external_statement_head(candidate_upper)
                {
                    this.split_current_and_reset_external_boundary();
                } else if this.state.pending_implicit_external_top_level_split {
                    this.state.pending_implicit_external_top_level_split = false;
                }
            }

            if this.state.in_with_plsql_declaration()
                && this.state.paren_depth == 0
                && sql_text::is_statement_head_keyword(candidate_upper)
            {
                let should_recover_with_clause = if this.state.with_clause_waiting_main_query() {
                    !sql_text::is_with_main_query_keyword(candidate_upper)
                } else if matches!(
                    this.state.with_clause_state,
                    WithClauseState::InPlsqlDeclaration(WithDeclarationState::CollectingDeclaration)
                ) {
                    !sql_text::is_with_main_query_keyword(candidate_upper)
                        && !sql_text::is_with_plsql_declaration_keyword(candidate_upper)
                        && !matches!(candidate_upper, "BEGIN" | "DECLARE")
                } else {
                    false
                };

                if should_recover_with_clause {
                    if this.state.pending_end == PendingEnd::End {
                        this.state.resolve_pending_end_on_separator();
                    }
                    if this.state.block_depth() == 0 {
                        this.split_current_statement();
                    }
                }
            }
        });
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
                LexMode::DollarQuote { tag } => {
                    if c == '$' && chars_starts_with(chars, i, tag) {
                        let tag_len = tag.len();
                        for k in 0..tag_len {
                            self.current.push(chars[i + k]);
                        }
                        self.state.lex_mode = LexMode::Idle;
                        i += tag_len;
                    } else {
                        self.current.push(c);
                        i += 1;
                    }
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
                        if self.state.pending_end == PendingEnd::End {
                            self.state.resolve_pending_end_on_separator();
                        }
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
                        if self.state.pending_end == PendingEnd::End {
                            self.state.resolve_pending_end_on_separator();
                        }
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
                if self.state.pending_implicit_external_top_level_split
                    && self.state.block_depth() == 1
                    && self.state.paren_depth == 0
                    && self.state.token.is_empty()
                {
                    self.split_current_statement();
                }
                self.state.lex_mode = LexMode::LineComment;
                self.current.push('-');
                self.current.push('-');
                i += 2;
                continue;
            }

            if c == '/' && next == Some('*') {
                self.state.flush_token();
                if self.state.pending_implicit_external_top_level_split
                    && self.state.block_depth() == 1
                    && self.state.paren_depth == 0
                    && self.state.token.is_empty()
                {
                    self.split_current_statement();
                }
                self.state.lex_mode = LexMode::BlockComment;
                self.current.push('/');
                self.current.push('*');
                i += 2;
                continue;
            }

            // Q-quote literals: q'[...]' and nq'[...]'/uq'[...]'
            // Detect the start position of the q/Q character and the delimiter.
            if self.state.token.is_empty() {
                let (q_prefix_len, q_idx) = if matches!(c, 'n' | 'N' | 'u' | 'U')
                    && matches!(next, Some('q' | 'Q'))
                    && i + 2 < len
                    && chars[i + 2] == '\''
                {
                    (4, i + 3) // nq'D or uq'D
                } else if matches!(c, 'q' | 'Q') && next == Some('\'') {
                    (3, i + 2) // q'D
                } else {
                    (0, 0)
                };

                if q_prefix_len > 0 {
                    if let Some(&delimiter) = chars.get(q_idx) {
                        if !is_valid_q_quote_delimiter(delimiter) {
                            self.current.push(c);
                            self.state.token.push(c);
                            i += 1;
                            continue;
                        }
                        self.state.flush_token();
                        let allow_implicit_target =
                            self.state.allow_implicit_external_literal_target();
                        self.state
                            .observe_external_clause_literal_target(allow_implicit_target);
                        self.state.start_q_quote(delimiter);
                        for k in 0..q_prefix_len {
                            self.current.push(chars[i + k]);
                        }
                        i += q_prefix_len;
                        continue;
                    }
                }
            }

            // Prefixed string literals: n'...', b'...', x'...', u'...', u&'...'
            if self.state.token.is_empty()
                && matches!(c, 'n' | 'N' | 'b' | 'B' | 'x' | 'X' | 'u' | 'U')
            {
                // u&'...' (3-char prefix)
                let (is_prefixed_quote, prefix_len) =
                    if (c == 'u' || c == 'U') && next == Some('&') && next2 == Some('\'') {
                        (true, 3)
                    } else if next == Some('\'') {
                        (true, 2)
                    } else {
                        (false, 0)
                    };

                if is_prefixed_quote {
                    self.state.flush_token();
                    let allow_implicit_target = self.state.allow_implicit_external_literal_target();
                    self.state
                        .observe_external_clause_literal_target(allow_implicit_target);
                    self.state.lex_mode = LexMode::SingleQuote;
                    for k in 0..prefix_len {
                        self.current.push(chars[i + k]);
                    }
                    i += prefix_len;
                    continue;
                }
            }

            // $$tag$$
            if self.state.token.is_empty()
                && c == '$'
                && (!looks_like_oracle_conditional_compilation_flag(chars, i)
                    || self.state.awaiting_external_language_target())
            {
                if let Some(tag) = parse_dollar_quote_tag(chars, i) {
                    let tag_len = tag.len();
                    self.state.flush_token();
                    let allow_implicit_target =
                        self.state.allow_implicit_external_literal_target();
                    self.state
                        .observe_external_clause_literal_target(allow_implicit_target);
                    // Push tag chars to current before moving tag into lex_mode.
                    for k in 0..tag_len {
                        self.current.push(chars[i + k]);
                    }
                    self.state.lex_mode = LexMode::DollarQuote { tag };
                    i += tag_len;
                    continue;
                }
            }

            if c == '\'' {
                self.state.flush_token();
                let allow_implicit_target = self.state.allow_implicit_external_literal_target();
                self.state
                    .observe_external_clause_literal_target(allow_implicit_target);
                self.state.lex_mode = LexMode::SingleQuote;
                self.current.push(c);
                i += 1;
                continue;
            }

            if c == '"' {
                self.state.flush_token();
                let allow_implicit_target = self.state.allow_implicit_external_literal_target();
                self.state
                    .observe_external_clause_literal_target(allow_implicit_target);
                self.state
                    .consume_trigger_alias_subject_on_quoted_identifier();
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
                self.handle_identifier_start_candidate(chars, i);
                if self.state.token.is_empty() {
                    self.state.token_prefixed_with_dollar = i > 0 && chars[i - 1] == '$';
                }
                self.state.token.push(c);
                self.current.push(c);
                i += 1;
                continue;
            }

            if self.state.in_with_plsql_declaration()
                && self.state.with_clause_waiting_main_query()
                && self.state.block_depth() == 0
                && self.state.paren_depth == 0
                && self.state.token.is_empty()
                && ((c == '@' && is_line_leading_char(chars, i, '@'))
                    || (c == '!' && is_line_leading_char(chars, i, '!'))
                    || (c == '/' && is_line_leading_slash_marker(chars, i)))
            {
                self.push_current_statement();
                self.reset_statement_local_state();
                self.state.reset_create_state();
            }

            let slash_statement_delimiter =
                c == '/' && self.state.token.is_empty() && is_line_leading_slash_marker(chars, i);
            let mut consumed_slash_statement_delimiter = false;

            let should_split_pending_implicit_external =
                self.state.pending_implicit_external_top_level_split
                    && self.state.block_depth() == 1
                    && self.state.paren_depth == 0;
            let should_split_forced_external_on_slash = self.state.block_depth() == 1
                && self.state.paren_depth == 0
                && self.state.should_split_on_semicolon();

            if self.state.token.is_empty()
                && ((should_split_pending_implicit_external
                    && ((c == '@' && is_line_leading_char(chars, i, '@'))
                        || (c == '!' && is_line_leading_char(chars, i, '!'))
                        || slash_statement_delimiter
                        || (c == '(' && is_line_leading_char(chars, i, '('))))
                    || (should_split_forced_external_on_slash
                        && slash_statement_delimiter))
            {
                self.split_current_statement();
                consumed_slash_statement_delimiter = slash_statement_delimiter;
            }

            if self.state.in_create_plsql()
                && self.state.block_depth() == 0
                && self.state.paren_depth == 0
                && self.state.token.is_empty()
                && slash_statement_delimiter
            {
                self.split_current_statement();
                self.state.reset_create_state();
                consumed_slash_statement_delimiter = true;
            }

            if consumed_slash_statement_delimiter {
                while i < len && chars[i] != '\n' {
                    i += 1;
                }
                continue;
            }

            let has_timing_point_label_end = c == ';'
                && self.state.pending_end == PendingEnd::End
                && self.state.allow_timing_point_end_suffix()
                && matches!(
                    self.state.token.as_str(),
                    token if token.eq_ignore_ascii_case("BEFORE")
                        || token.eq_ignore_ascii_case("AFTER")
                        || token.eq_ignore_ascii_case("INSTEAD")
                );

            if has_timing_point_label_end {
                self.state.pending_end = PendingEnd::None;
                self.state.token.clear();
                self.state.token_prefixed_with_dollar = false;
            } else {
                self.state.flush_token();
            }
            self.state.track_with_main_query_symbol(c);
            self.state.observe_external_clause_symbol(c, next);
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
        let line_started_with_empty_current = self.current.trim().is_empty();
        let line_started_in_with_waiting_main_query = self.state.in_with_plsql_declaration()
            && self.state.with_clause_waiting_main_query()
            && self.state.block_depth() == 0
            && self.state.paren_depth == 0;
        let line_starts_at_statement_boundary = self.state.is_idle()
            && self.state.block_depth() == 0
            && self.state.paren_depth == 0
            && !self.state.in_with_plsql_declaration()
            && line_started_with_empty_current;
        let can_fast_path_tool_command = line_starts_at_statement_boundary;
        if can_fast_path_tool_command && sql_text::is_auto_terminated_tool_command(line) {
            self.current.push_str(line);
            self.current.push('\n');
            self.push_current_statement();
            self.reset_statement_local_state();
            self.state.reset_create_state();
            return;
        }

        let mut on_symbol = on_symbol;
        let mut scratch_chars = std::mem::take(&mut self.scratch_chars);
        scratch_chars.clear();
        scratch_chars.extend(line.chars());
        scratch_chars.push('\n');
        self.process_chars_with_observer(&scratch_chars, &mut on_symbol);

        if (line_started_with_empty_current || line_started_in_with_waiting_main_query)
            && self.state.is_idle()
            && self.state.block_depth() == 0
            && self.state.paren_depth == 0
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
        if !self.state.is_idle() {
            return;
        }

        // SQL*Plus slash terminator should behave like a statement terminator for
        // external routine call specs (e.g. `AS LANGUAGE C` without trailing `;`).
        self.state.finalize_external_clause_on_semicolon();

        if self.state.pending_end == PendingEnd::End {
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
    fn create_type_body_member_modifier_is_not_treated_as_new_create_target() {
        let mut state = SplitState {
            create_plsql_kind: CreatePlsqlKind::TypeBody,
            create_state: CreateState::AwaitingObjectType,
            ..SplitState::default()
        };

        state.track_create_plsql("MEMBER");
        assert!(state.in_create_plsql());
        assert_eq!(state.create_plsql_kind, CreatePlsqlKind::TypeBody);
        assert_eq!(state.create_state, CreateState::AwaitingObjectType);

        state.track_create_plsql("FUNCTION");
        assert!(state.in_create_plsql());
        assert_eq!(state.create_plsql_kind, CreatePlsqlKind::TypeBody);
        assert_eq!(state.create_state, CreateState::AwaitingObjectType);
    }

    #[test]
    fn create_type_body_member_function_splits_before_trailing_select() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TYPE BODY t_member AS");
        engine.process_line("  MEMBER FUNCTION f RETURN NUMBER IS");
        engine.process_line("  BEGIN");
        engine.process_line("    RETURN 1;");
        engine.process_line("  END f;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("CREATE OR REPLACE TYPE BODY t_member AS"),
            "first statement should preserve type body text: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("MEMBER FUNCTION f RETURN NUMBER IS"),
            "first statement should include member function declarative header: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
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
    fn with_function_waiting_main_query_recovers_on_run_script_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH");
        engine.process_line("  FUNCTION f RETURN NUMBER IS");
        engine.process_line("  BEGIN");
        engine.process_line("    RETURN 1;");
        engine.process_line("  END;");
        engine.process_line("@child.sql");
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
        assert_eq!(statements[1], "@child.sql".to_string());
        assert_eq!(statements[2], "SELECT 2 FROM dual".to_string());
    }

    #[test]
    fn with_function_waiting_main_query_recovers_on_start_script_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH");
        engine.process_line("  FUNCTION f RETURN NUMBER IS");
        engine.process_line("  BEGIN");
        engine.process_line("    RETURN 1;");
        engine.process_line("  END;");
        engine.process_line("START child.sql");
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
        assert_eq!(statements[1], "START child.sql".to_string());
        assert_eq!(statements[2], "SELECT 2 FROM dual".to_string());
    }

    #[test]
    fn with_function_waiting_main_query_recovers_on_relative_run_script_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH");
        engine.process_line("  FUNCTION f RETURN NUMBER IS");
        engine.process_line("  BEGIN");
        engine.process_line("    RETURN 1;");
        engine.process_line("  END;");
        engine.process_line("@@child.sql");
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
        assert_eq!(statements[1], "@@child.sql".to_string());
        assert_eq!(statements[2], "SELECT 2 FROM dual".to_string());
    }

    #[test]
    fn with_function_waiting_main_query_recovers_on_bang_host_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH");
        engine.process_line("  FUNCTION f RETURN NUMBER IS");
        engine.process_line("  BEGIN");
        engine.process_line("    RETURN 1;");
        engine.process_line("  END;");
        engine.process_line("! ls");
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
        assert_eq!(statements[1], "! ls".to_string());
        assert_eq!(statements[2], "SELECT 2 FROM dual".to_string());
    }

    #[test]
    fn with_function_waiting_main_query_recovers_on_sqlplus_report_statement_heads() {
        for report_command in [
            "TIMING START parser_check",
            "TTITLE LEFT 'SPACE Query'",
            "BTITLE LEFT 'Footer'",
            "REPHEADER PAGE",
            "REPFOOTER OFF",
        ] {
            let mut engine = SqlParserEngine::new();

            engine.process_line("WITH");
            engine.process_line("  FUNCTION f RETURN NUMBER IS");
            engine.process_line("  BEGIN");
            engine.process_line("    RETURN 1;");
            engine.process_line("  END;");
            engine.process_line(report_command);
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
            assert_eq!(statements[1], report_command.to_string());
            assert_eq!(statements[2], "SELECT 2 FROM dual".to_string());
        }
    }

    #[test]
    fn with_function_waiting_main_query_recovers_on_password_command_abbreviations() {
        for password_command in ["PASSWO app_user", "PASSWOR app_user", "PASSWORD app_user"] {
            let mut engine = SqlParserEngine::new();

            engine.process_line("WITH");
            engine.process_line("  FUNCTION f RETURN NUMBER IS");
            engine.process_line("  BEGIN");
            engine.process_line("    RETURN 1;");
            engine.process_line("  END;");
            engine.process_line(password_command);
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
            assert_eq!(statements[1], password_command.to_string());
            assert_eq!(statements[2], "SELECT 2 FROM dual".to_string());
        }
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
    fn compound_trigger_with_statement_timing_point_splits_on_outer_end() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_compound_stmt");
        engine.process_line("FOR INSERT ON t");
        engine.process_line("COMPOUND TRIGGER");
        engine.process_line("  BEFORE STATEMENT IS");
        engine.process_line("  BEGIN");
        engine.process_line("    NULL;");
        engine.process_line("  END BEFORE STATEMENT;");
        engine.process_line("END;");
        engine.process_line("SELECT 4 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END BEFORE STATEMENT"),
            "first statement should preserve STATEMENT timing point closure: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 4 FROM dual"));
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
    fn package_spec_with_external_procedure_declaration_does_not_split_mid_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PACKAGE pkg_spec_ext AS");
        engine.process_line("  PROCEDURE ext_proc LANGUAGE C;");
        engine.process_line("END pkg_spec_ext;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("CREATE OR REPLACE PACKAGE pkg_spec_ext AS"),
            "first statement should preserve package specification body: {}",
            statements[0]
        );
        assert!(statements[0].contains("PROCEDURE ext_proc LANGUAGE C;"));
        assert!(statements[0].contains("END pkg_spec_ext"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn package_spec_with_external_name_clause_does_not_split_mid_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PACKAGE pkg_spec_call AS");
        engine.process_line(r#"  PROCEDURE ext_proc IS EXTERNAL NAME "ext_proc" LANGUAGE C;"#);
        engine.process_line("END pkg_spec_call;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("CREATE OR REPLACE PACKAGE pkg_spec_call AS"),
            "first statement should preserve package specification body: {}",
            statements[0]
        );
        assert!(
            statements[0].contains(r#"PROCEDURE ext_proc IS EXTERNAL NAME "ext_proc" LANGUAGE C;"#),
            "call-spec declaration should stay in package spec statement: {}",
            statements[0]
        );
        assert!(statements[0].contains("END pkg_spec_call"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn package_spec_procedure_language_clause_without_external_keyword_does_not_split_mid_statement(
    ) {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PACKAGE pkg_spec_lang AS");
        engine.process_line("  PROCEDURE p IS LANGUAGE C;");
        engine.process_line("END pkg_spec_lang;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("PROCEDURE p IS LANGUAGE C;"));
        assert!(statements[0].contains("END pkg_spec_lang"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
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
        engine.process_line("  language mle;");
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
        assert!(statements[0].contains("language mle;"));
        assert!(statements[0].contains("marker NUMBER := 1;"));
        assert!(statements[0].contains("END"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn language_assignment_operator_cancels_implicit_external_detection() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PROCEDURE proc_assign IS");
        engine.process_line("  language := 'C';");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].starts_with("CREATE OR REPLACE PROCEDURE proc_assign IS"));
        assert!(statements[0].contains("language := 'C';"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn language_followed_by_line_comment_does_not_cancel_external_clause_detection() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_lang_comment RETURN NUMBER");
        engine.process_line("AS LANGUAGE -- keep parsing as external call spec");
        engine.process_line("C;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("CREATE OR REPLACE FUNCTION ext_lang_comment RETURN NUMBER")
        );
        assert!(statements[0].contains("AS LANGUAGE -- keep parsing as external call spec"));
        assert!(statements[0].contains("C;"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn language_followed_by_block_comment_does_not_cancel_external_clause_detection() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_lang_block_comment RETURN NUMBER");
        engine.process_line("AS LANGUAGE /* keep parsing as external call spec */");
        engine.process_line("C;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0]
            .starts_with("CREATE OR REPLACE FUNCTION ext_lang_block_comment RETURN NUMBER"));
        assert!(statements[0].contains("AS LANGUAGE /* keep parsing as external call spec */"));
        assert!(statements[0].contains("C;"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn language_followed_by_single_quoted_identifier_literal_does_not_force_external_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PROCEDURE proc_language_literal IS");
        engine.process_line("  language 'C';");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].starts_with("CREATE OR REPLACE PROCEDURE proc_language_literal IS"));
        assert!(statements[0].contains("language 'C';"));
        assert!(statements[0].contains("END"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn language_followed_by_double_quoted_identifier_literal_does_not_force_external_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PROCEDURE proc_language_qident IS");
        engine.process_line("  language \"C\";");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].starts_with("CREATE OR REPLACE PROCEDURE proc_language_qident IS"));
        assert!(statements[0].contains("language \"C\";"));
        assert!(statements[0].contains("END"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn nested_language_identifier_targets_do_not_force_external_split() {
        for target in ["C", "JAVA", "JAVASCRIPT", "PYTHON", "MLE"] {
            let mut engine = SqlParserEngine::new();

            engine.process_line("CREATE OR REPLACE PROCEDURE proc_language_ident IS");
            engine.process_line(&format!("  language {target};"));
            engine.process_line("BEGIN");
            engine.process_line("  NULL;");
            engine.process_line("END;");
            engine.process_line("SELECT 1 FROM dual;");

            let statements = engine.finalize_and_take_statements();
            assert_eq!(
                statements.len(),
                2,
                "unexpected statements for {target}: {statements:?}"
            );
            assert!(
                statements[0].starts_with("CREATE OR REPLACE PROCEDURE proc_language_ident IS"),
                "first statement should keep procedure body for {target}: {}",
                statements[0]
            );
            assert!(
                statements[0].contains(&format!("language {target};")),
                "first statement should keep language declaration for {target}: {}",
                statements[0]
            );
            assert!(
                statements[0].contains("END"),
                "first statement should contain END for {target}: {}",
                statements[0]
            );
            assert!(
                statements[1].starts_with("SELECT 1 FROM dual"),
                "second statement should remain standalone for {target}: {}",
                statements[1]
            );
        }
    }

    #[test]
    fn nested_language_dollar_quoted_targets_do_not_force_external_split() {
        for target in ["$$C$$", "$lang$JAVA$lang$", "$lang$PYTHON$lang$"] {
            let mut engine = SqlParserEngine::new();

            engine.process_line("CREATE OR REPLACE PROCEDURE proc_language_dollar_ident IS");
            engine.process_line(&format!("  language {target};"));
            engine.process_line("BEGIN");
            engine.process_line("  NULL;");
            engine.process_line("END;");
            engine.process_line("SELECT 1 FROM dual;");

            let statements = engine.finalize_and_take_statements();
            assert_eq!(
                statements.len(),
                2,
                "unexpected statements for {target}: {statements:?}"
            );
            assert!(
                statements[0].starts_with(
                    "CREATE OR REPLACE PROCEDURE proc_language_dollar_ident IS"
                ),
                "first statement should keep procedure body for {target}: {}",
                statements[0]
            );
            assert!(
                statements[0].contains(&format!("language {target};")),
                "first statement should keep language declaration for {target}: {}",
                statements[0]
            );
            assert!(
                statements[0].contains("END"),
                "first statement should contain END for {target}: {}",
                statements[0]
            );
            assert!(
                statements[1].starts_with("SELECT 1 FROM dual"),
                "second statement should remain standalone for {target}: {}",
                statements[1]
            );
        }
    }

    #[test]
    fn nested_language_dollar_quoted_targets_in_package_body_do_not_close_nested_routine() {
        for target in ["$$C$$", "$lang$JAVASCRIPT$lang$", "$lang$JAVA$lang$"] {
            let mut engine = SqlParserEngine::new();

            engine.process_line("CREATE OR REPLACE PACKAGE BODY pkg_language_dollar_ident AS");
            engine.process_line("  PROCEDURE p IS");
            engine.process_line(&format!("    language {target};"));
            engine.process_line("  BEGIN");
            engine.process_line("    NULL;");
            engine.process_line("  END p;");
            engine.process_line("END pkg_language_dollar_ident;");
            engine.process_line("SELECT 1 FROM dual;");

            let statements = engine.finalize_and_take_statements();
            assert_eq!(
                statements.len(),
                2,
                "unexpected statements for nested target {target}: {statements:?}"
            );
            assert!(
                statements[0].contains(&format!("language {target};")),
                "package body should keep nested language declaration for {target}: {}",
                statements[0]
            );
            assert!(
                statements[0].contains("END p;"),
                "package body should keep nested procedure END for {target}: {}",
                statements[0]
            );
            assert!(
                statements[0].contains("END pkg_language_dollar_ident"),
                "package body should close normally for {target}: {}",
                statements[0]
            );
            assert!(
                statements[1].starts_with("SELECT 1 FROM dual"),
                "trailing SELECT should split for {target}: {}",
                statements[1]
            );
        }
    }

    #[test]
    fn nested_language_identifier_targets_in_package_body_do_not_close_nested_routine() {
        for target in ["C", "JAVA", "JAVASCRIPT", "PYTHON", "MLE"] {
            let mut engine = SqlParserEngine::new();

            engine.process_line("CREATE OR REPLACE PACKAGE BODY pkg_language_ident AS");
            engine.process_line("  PROCEDURE p IS");
            engine.process_line(&format!("    language {target};"));
            engine.process_line("  BEGIN");
            engine.process_line("    NULL;");
            engine.process_line("  END p;");
            engine.process_line("END pkg_language_ident;");
            engine.process_line("SELECT 1 FROM dual;");

            let statements = engine.finalize_and_take_statements();
            assert_eq!(
                statements.len(),
                2,
                "unexpected statements for nested target {target}: {statements:?}"
            );
            assert!(
                statements[0].contains(&format!("language {target};")),
                "package body should keep nested language declaration for {target}: {}",
                statements[0]
            );
            assert!(
                statements[0].contains("END p;"),
                "package body should keep nested procedure END for {target}: {}",
                statements[0]
            );
            assert!(
                statements[0].contains("END pkg_language_ident"),
                "package body should close normally for {target}: {}",
                statements[0]
            );
            assert!(
                statements[1].starts_with("SELECT 1 FROM dual"),
                "trailing SELECT should split for {target}: {}",
                statements[1]
            );
        }
    }

    #[test]
    fn package_body_nested_language_identifier_declaration_keeps_following_nested_subprograms() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PACKAGE BODY pkg_language_chain AS");
        engine.process_line("  PROCEDURE p1 IS");
        engine.process_line("    language c;");
        engine.process_line("  BEGIN");
        engine.process_line("    NULL;");
        engine.process_line("  END p1;");
        engine.process_line("  PROCEDURE p2 IS");
        engine.process_line("  BEGIN");
        engine.process_line("    NULL;");
        engine.process_line("  END p2;");
        engine.process_line("END pkg_language_chain;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("PROCEDURE p1 IS"));
        assert!(statements[0].contains("language c;"));
        assert!(statements[0].contains("END p1;"));
        assert!(statements[0].contains("PROCEDURE p2 IS"));
        assert!(statements[0].contains("END p2;"));
        assert!(statements[0].contains("END pkg_language_chain"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn nested_language_identifier_declaration_with_following_local_variable_keeps_routine_structure(
    ) {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PACKAGE BODY pkg_language_locals AS");
        engine.process_line("  PROCEDURE p IS");
        engine.process_line("    language c;");
        engine.process_line("    n NUMBER := 1;");
        engine.process_line("  BEGIN");
        engine.process_line("    NULL;");
        engine.process_line("  END p;");
        engine.process_line("END pkg_language_locals;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("language c;"));
        assert!(statements[0].contains("n NUMBER := 1;"));
        assert!(statements[0].contains("END p;"));
        assert!(statements[0].contains("END pkg_language_locals"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn language_comparison_operator_cancels_implicit_external_detection() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PROCEDURE proc_compare IS");
        engine.process_line("  language = 'C';");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].starts_with("CREATE OR REPLACE PROCEDURE proc_compare IS"));
        assert!(statements[0].contains("language = 'C';"));
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
    fn external_clause_with_credential_keyword_still_marks_external_routine_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_cred RETURN NUMBER");
        engine.process_line("AS EXTERNAL CREDENTIAL ext_credential NAME 'ext_cred';");
        engine.process_line("SELECT 101 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("CREDENTIAL ext_credential"),
            "external clause with credential should remain in first statement: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 101 FROM dual"));
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
    fn language_clause_with_single_quoted_target_without_external_keyword_marks_external_routine_split(
    ) {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_lang_quoted RETURN NUMBER");
        engine.process_line("AS LANGUAGE 'C' NAME 'ext_lang_quoted';");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE 'C' NAME 'ext_lang_quoted'"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn language_clause_with_national_single_quoted_target_without_external_keyword_marks_external_routine_split(
    ) {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_lang_nquoted RETURN NUMBER");
        engine.process_line("AS LANGUAGE N'C' NAME 'ext_lang_nquoted';");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE N'C' NAME 'ext_lang_nquoted'"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn language_clause_with_unicode_single_quoted_target_without_external_keyword_marks_external_routine_split(
    ) {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_lang_uquoted RETURN NUMBER");
        engine.process_line("AS LANGUAGE U'C' NAME 'ext_lang_uquoted';");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE U'C' NAME 'ext_lang_uquoted'"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn language_clause_with_unicode_escape_quoted_target_without_external_keyword_marks_external_routine_split(
    ) {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_lang_uesc RETURN NUMBER");
        engine.process_line("AS LANGUAGE U&'C' NAME 'ext_lang_uesc';");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE U&'C' NAME 'ext_lang_uesc'"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }
    #[test]
    fn language_clause_with_q_quoted_target_without_external_keyword_marks_external_routine_split()
    {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_lang_qquoted RETURN NUMBER");
        engine.process_line("AS LANGUAGE q'[C]' NAME 'ext_lang_qquoted';");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE q'[C]' NAME 'ext_lang_qquoted'"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn language_clause_with_nq_quoted_target_without_external_keyword_marks_external_routine_split()
    {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_lang_nqquoted RETURN NUMBER");
        engine.process_line("AS LANGUAGE nq'[C]' NAME 'ext_lang_nqquoted';");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE nq'[C]' NAME 'ext_lang_nqquoted'"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn language_clause_with_uq_quoted_target_without_external_keyword_marks_external_routine_split()
    {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_lang_uqquoted RETURN NUMBER");
        engine.process_line("AS LANGUAGE uq'[C]' NAME 'ext_lang_uqquoted';");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE uq'[C]' NAME 'ext_lang_uqquoted'"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn language_clause_with_binary_single_quoted_target_without_external_keyword_marks_external_routine_split(
    ) {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_lang_bquoted RETURN NUMBER");
        engine.process_line("AS LANGUAGE B'C' NAME 'ext_lang_bquoted';");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE B'C' NAME 'ext_lang_bquoted'"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn language_clause_with_hex_single_quoted_target_without_external_keyword_marks_external_routine_split(
    ) {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_lang_xquoted RETURN NUMBER");
        engine.process_line("AS LANGUAGE X'C' NAME 'ext_lang_xquoted';");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE X'C' NAME 'ext_lang_xquoted'"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn apostrophe_cannot_start_q_quote_delimiter_and_does_not_swallow_semicolon_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("SELECT q'' FROM dual;");
        engine.process_line("SELECT 2 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert_eq!(statements[0], "SELECT q'' FROM dual".to_string());
        assert_eq!(statements[1], "SELECT 2 FROM dual".to_string());
    }

    #[test]
    fn non_ascii_q_quote_delimiter_is_treated_as_q_quote_and_preserves_semicolon_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("SELECT q'가문자열가' FROM dual;");
        engine.process_line("SELECT 2 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert_eq!(statements[0], "SELECT q'가문자열가' FROM dual".to_string());
        assert_eq!(statements[1], "SELECT 2 FROM dual".to_string());
    }

    #[test]
    fn non_ascii_nq_quote_delimiter_is_treated_as_q_quote_and_preserves_semicolon_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("SELECT nq'가문자열가' FROM dual;");
        engine.process_line("SELECT 3 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert_eq!(statements[0], "SELECT nq'가문자열가' FROM dual".to_string());
        assert_eq!(statements[1], "SELECT 3 FROM dual".to_string());
    }

    #[test]
    fn oracle_conditional_compilation_flag_does_not_enter_dollar_quote_mode() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("BEGIN");
        engine.process_line("  IF $$PLSQL_UNIT IS NOT NULL THEN");
        engine.process_line("    NULL;");
        engine.process_line("  END IF;");
        engine.process_line("END;");
        engine.process_line("SELECT 11 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("IF $$PLSQL_UNIT IS NOT NULL THEN"));
        assert!(statements[1].starts_with("SELECT 11 FROM dual"));
    }

    #[test]
    fn dollar_prefixed_numeric_token_does_not_trigger_conditional_compilation_mode() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("SELECT $$1$$ FROM dual;");
        engine.process_line("SELECT 2 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert_eq!(statements[0], "SELECT $$1$$ FROM dual".to_string());
        assert_eq!(statements[1], "SELECT 2 FROM dual".to_string());
    }

    #[test]
    fn oracle_conditional_compilation_flag_with_numeric_suffix_does_not_hang_statement_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("BEGIN");
        engine.process_line("  IF $$PLSQL_LINE_1 > 0 THEN");
        engine.process_line("    NULL;");
        engine.process_line("  END IF;");
        engine.process_line("END;");
        engine.process_line("SELECT 12 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("IF $$PLSQL_LINE_1 > 0 THEN"));
        assert_eq!(statements[1], "SELECT 12 FROM dual".to_string());
    }

    #[test]
    fn language_clause_with_dollar_quoted_target_without_external_keyword_marks_external_routine_split(
    ) {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_lang_dollar RETURN NUMBER");
        engine.process_line("AS LANGUAGE $lang$C$lang$ NAME 'ext_lang_dollar';");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE $lang$C$lang$ NAME 'ext_lang_dollar'"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn mle_module_clause_without_external_keyword_marks_external_routine_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_mle RETURN NUMBER");
        engine.process_line("AS MLE MODULE ext_mod SIGNATURE 'run(number)';");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].starts_with("CREATE OR REPLACE FUNCTION ext_mle RETURN NUMBER"));
        assert!(statements[0].contains("AS MLE MODULE ext_mod SIGNATURE 'run(number)'"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn mle_language_target_with_module_clause_marks_external_routine_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_mle_lang RETURN NUMBER");
        engine.process_line("AS LANGUAGE JAVASCRIPT MLE MODULE ext_mod SIGNATURE 'run(number)';");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].starts_with("CREATE OR REPLACE FUNCTION ext_mle_lang RETURN NUMBER"));
        assert!(statements[0]
            .contains("AS LANGUAGE JAVASCRIPT MLE MODULE ext_mod SIGNATURE 'run(number)'"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn external_language_name_clause_without_semicolon_splits_on_slash_terminator() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_name_slash RETURN NUMBER");
        engine.process_line("AS LANGUAGE C NAME 'ext_name_slash'");
        engine.process_line("/");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("CREATE OR REPLACE FUNCTION ext_name_slash RETURN NUMBER")
        );
        assert!(statements[0].contains("AS LANGUAGE C NAME 'ext_name_slash'"));
        assert!(
            statements[1].starts_with("SELECT 1 FROM dual"),
            "slash delimiter line should not leak into next statement: {}",
            statements[1]
        );
    }

    #[test]
    fn mle_module_clause_without_semicolon_splits_on_slash_terminator() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_mle_slash RETURN NUMBER");
        engine.process_line("AS MLE MODULE ext_mod SIGNATURE 'run(number)'");
        engine.process_line("/");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].starts_with("CREATE OR REPLACE FUNCTION ext_mle_slash RETURN NUMBER"));
        assert!(statements[0].contains("AS MLE MODULE ext_mod SIGNATURE 'run(number)'"));
        assert!(
            statements[1].starts_with("SELECT 1 FROM dual"),
            "slash delimiter line should not leak into next statement: {}",
            statements[1]
        );
    }

    #[test]
    fn language_clause_with_empty_dollar_quoted_target_still_marks_external_routine_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_lang_dollar_empty RETURN NUMBER");
        engine.process_line("AS LANGUAGE $$C$$ NAME 'ext_lang_dollar_empty';");
        engine.process_line("SELECT 12 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE $$C$$ NAME 'ext_lang_dollar_empty'"));
        assert!(statements[1].starts_with("SELECT 12 FROM dual"));
    }

    #[test]
    fn external_language_clause_splits_before_parenthesized_query_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_lang_paren RETURN NUMBER");
        engine.process_line("AS LANGUAGE U'C';");
        engine.process_line("(SELECT ext_lang_paren() AS v FROM dual)");
        engine.process_line("UNION ALL");
        engine.process_line("SELECT 2 AS v FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE U'C'"));
        assert!(statements[1].starts_with("(SELECT ext_lang_paren() AS v FROM dual)"));
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
    fn trigger_referencing_alias_is_does_not_block_is_header_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_ref_alias_is");
        engine.process_line("BEFORE INSERT ON t");
        engine.process_line("REFERENCING NEW IS n OLD IS o");
        engine.process_line("FOR EACH ROW");
        engine.process_line("IS");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END;");
        engine.process_line("SELECT 5 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("REFERENCING NEW IS n OLD IS o"));
        assert!(statements[0].contains(
            "FOR EACH ROW
IS
BEGIN"
        ));
        assert!(statements[1].starts_with("SELECT 5 FROM dual"));
    }

    #[test]
    fn trigger_header_is_still_opens_simple_trigger_body() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_is_header");
        engine.process_line("BEFORE INSERT ON t");
        engine.process_line("FOR EACH ROW");
        engine.process_line("IS");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END;");
        engine.process_line("SELECT 4 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("CREATE OR REPLACE TRIGGER trg_is_header"),
            "first statement should preserve trigger header: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("FOR EACH ROW\nIS\nBEGIN"),
            "IS header must remain attached to trigger body: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 4 FROM dual"));
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
    fn language_clause_with_future_tokens_without_external_keyword_still_splits() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_future RETURN NUMBER");
        engine.process_line("AS LANGUAGE JAVASCRIPT MODULE ext_future_impl;");
        engine.process_line("SELECT 6 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("LANGUAGE JAVASCRIPT MODULE ext_future_impl"),
            "first statement should keep future LANGUAGE clause tokens: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 6 FROM dual"));
    }

    #[test]
    fn package_body_nested_language_clause_with_future_tokens_closes_on_semicolon() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PACKAGE BODY pkg_future AS");
        engine.process_line("  PROCEDURE p IS LANGUAGE JAVASCRIPT MODULE impl;");
        engine.process_line("END pkg_future;");
        engine.process_line("SELECT 7 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("PROCEDURE p IS LANGUAGE JAVASCRIPT MODULE impl;"),
            "nested LANGUAGE clause should stay inside package body: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("END pkg_future"),
            "package body should close normally after nested routine: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 7 FROM dual"));
    }

    #[test]
    fn language_clause_with_language_mle_module_without_external_keyword_still_splits() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_language_mle_module RETURN NUMBER");
        engine.process_line("AS LANGUAGE MLE MODULE ext_language_mle_impl;");
        engine.process_line("SELECT 9 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS LANGUAGE MLE MODULE ext_language_mle_impl"),
            "first statement should keep LANGUAGE MLE MODULE clause tokens: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 9 FROM dual"));
    }

    #[test]
    fn language_clause_with_mle_module_without_external_keyword_still_splits() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_mle_module RETURN NUMBER");
        engine.process_line("AS MLE MODULE ext_mle_impl;");
        engine.process_line("SELECT 8 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS MLE MODULE ext_mle_impl"),
            "first statement should keep MLE MODULE clause tokens: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 8 FROM dual"));
    }

    #[test]
    fn language_clause_with_mle_signature_without_external_keyword_still_splits() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_mle_sig RETURN NUMBER");
        engine.process_line("AS MLE SIGNATURE ext_sig_impl;");
        engine.process_line("SELECT 10 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS MLE SIGNATURE ext_sig_impl"),
            "first statement should keep MLE SIGNATURE clause tokens: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 10 FROM dual"));
    }

    #[test]
    fn language_clause_with_mle_environment_without_external_keyword_still_splits() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_mle_env RETURN NUMBER");
        engine.process_line("AS MLE ENV ext_env_impl;");
        engine.process_line("SELECT 12 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS MLE ENV ext_env_impl"),
            "first statement should keep MLE ENV clause tokens: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 12 FROM dual"));
    }

    #[test]
    fn language_clause_with_mle_marker_after_language_target_still_splits() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_mle_marker RETURN NUMBER");
        engine.process_line("AS LANGUAGE JAVASCRIPT MLE;");
        engine.process_line("SELECT 11 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS LANGUAGE JAVASCRIPT MLE"),
            "first statement should keep LANGUAGE ... MLE clause tokens: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 11 FROM dual"));
    }

    #[test]
    fn package_body_nested_language_clause_with_mle_marker_closes_on_semicolon() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PACKAGE BODY pkg_mle_marker AS");
        engine.process_line("  PROCEDURE p IS LANGUAGE JAVASCRIPT MLE;");
        engine.process_line("END pkg_mle_marker;");
        engine.process_line("SELECT 12 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("PROCEDURE p IS LANGUAGE JAVASCRIPT MLE;"),
            "nested LANGUAGE ... MLE clause should stay in package body: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("END pkg_mle_marker"),
            "package body should close normally after nested routine: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 12 FROM dual"));
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
    fn type_declaration_with_unknown_declarative_kind_splits_at_semicolon() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TYPE t_future AS FUTURE_KIND (");
        engine.process_line("  attr NUMBER");
        engine.process_line(");");
        engine.process_line("SELECT 1 FROM dual;");

        assert_eq!(
            engine.finalize_and_take_statements(),
            vec![
                "CREATE OR REPLACE TYPE t_future AS FUTURE_KIND (\n  attr NUMBER\n)".to_string(),
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
    fn end_with_quoted_label_closes_block_and_splits_next_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END \"done_label\";");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("END \"done_label\""));
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn implicit_external_split_clears_routine_boundary_before_next_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_name_first RETURN NUMBER");
        engine.process_line("AS EXTERNAL");
        engine.process_line("NAME \"ext_name_first\" LIBRARY extlib LANGUAGE C;");
        engine.process_line("SELECT 1 FROM dual;");

        assert_eq!(
            engine.finalize_and_take_statements(),
            vec![
                "CREATE OR REPLACE FUNCTION ext_name_first RETURN NUMBER\nAS EXTERNAL\nNAME \"ext_name_first\" LIBRARY extlib LANGUAGE C;".to_string(),
                "SELECT 1 FROM dual".to_string(),
            ]
        );
    }

    #[test]
    fn end_if_with_label_closes_block_and_splits_next_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("BEGIN");
        engine.process_line("  IF 1 = 1 THEN");
        engine.process_line("    NULL;");
        engine.process_line("  END IF done_flag;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END IF done_flag;"),
            "first statement should include END IF label: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn end_if_with_quoted_label_closes_block_and_splits_next_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("BEGIN");
        engine.process_line("  IF 1 = 1 THEN");
        engine.process_line("    NULL;");
        engine.process_line("  END IF \"done_flag\";");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("END IF \"done_flag\";"));
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn end_loop_with_label_closes_block_and_splits_next_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("BEGIN");
        engine.process_line("  LOOP");
        engine.process_line("    EXIT;");
        engine.process_line("  END LOOP loop_done;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("END LOOP loop_done;"));
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn end_loop_with_quoted_label_closes_block_and_splits_next_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("BEGIN");
        engine.process_line("  LOOP");
        engine.process_line("    EXIT;");
        engine.process_line("  END LOOP \"loop_done\";");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("END LOOP \"loop_done\";"));
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn end_case_with_label_closes_block_and_splits_next_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("BEGIN");
        engine.process_line("  CASE");
        engine.process_line("    WHEN 1 = 1 THEN NULL;");
        engine.process_line("  END CASE case_done;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("END CASE case_done;"));
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn end_case_with_quoted_label_closes_block_and_splits_next_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("BEGIN");
        engine.process_line("  CASE");
        engine.process_line("    WHEN 1 = 1 THEN NULL;");
        engine.process_line("  END CASE \"case_done\";");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("END CASE \"case_done\";"));
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn end_case_with_inner_end_if_label_stays_in_same_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("BEGIN");
        engine.process_line("  CASE");
        engine.process_line("    WHEN 1 = 1 THEN");
        engine.process_line("      IF 1 = 1 THEN");
        engine.process_line("        NULL;");
        engine.process_line("      END IF cond_done;");
        engine.process_line("  END CASE case_done;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END IF cond_done;"),
            "END IF label should stay in first statement: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("END CASE case_done;"),
            "END CASE should remain in first statement: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn end_if_with_nested_case_label_stays_in_same_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("BEGIN");
        engine.process_line("  IF 1 = 1 THEN");
        engine.process_line("    CASE");
        engine.process_line("      WHEN 1 = 1 THEN NULL;");
        engine.process_line("    END CASE case_done;");
        engine.process_line("  END IF cond_done;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END CASE case_done;"),
            "END CASE label should stay in first statement: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("END IF cond_done;"),
            "END IF should remain in first statement: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn trigger_referencing_alias_with_quoted_identifier_does_not_block_body_as_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_ref_alias_quoted");
        engine.process_line("BEFORE INSERT ON t");
        engine.process_line("REFERENCING NEW AS \"N\"");
        engine.process_line("FOR EACH ROW");
        engine.process_line("AS");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("REFERENCING NEW AS \"N\""),
            "first statement should preserve quoted alias clause: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("AS\nBEGIN"),
            "trigger body AS should remain part of trigger statement: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn trigger_referencing_alias_with_quoted_identifier_does_not_block_body_is_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_ref_alias_quoted_is");
        engine.process_line("BEFORE INSERT ON t");
        engine.process_line("REFERENCING NEW IS \"N\"");
        engine.process_line("FOR EACH ROW");
        engine.process_line("IS");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("REFERENCING NEW IS \"N\""),
            "first statement should preserve quoted alias clause: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("IS\nBEGIN"),
            "trigger body IS should remain part of trigger statement: {}",
            statements[0]
        );
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
    fn compound_trigger_nested_subprogram_named_before_does_not_start_new_timing_point() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_nested_before");
        engine.process_line("FOR INSERT ON t");
        engine.process_line("COMPOUND TRIGGER");
        engine.process_line("  BEFORE STATEMENT IS");
        engine.process_line("    PROCEDURE before IS");
        engine.process_line("    BEGIN");
        engine.process_line("      NULL;");
        engine.process_line("    END before;");
        engine.process_line("  BEGIN");
        engine.process_line("    before;");
        engine.process_line("  END BEFORE STATEMENT;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("CREATE OR REPLACE TRIGGER trg_nested_before"),
            "compound trigger should stay in a single statement: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn compound_trigger_nested_subprogram_named_after_keeps_timing_point_balance() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_nested_after");
        engine.process_line("FOR INSERT ON t");
        engine.process_line("COMPOUND TRIGGER");
        engine.process_line("  BEFORE STATEMENT IS");
        engine.process_line("    PROCEDURE after IS");
        engine.process_line("    BEGIN");
        engine.process_line("      NULL;");
        engine.process_line("    END after;");
        engine.process_line("  BEGIN");
        engine.process_line("    after;");
        engine.process_line("  END BEFORE STATEMENT;");
        engine.process_line("  AFTER STATEMENT IS");
        engine.process_line("  BEGIN");
        engine.process_line("    NULL;");
        engine.process_line("  END AFTER STATEMENT;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END BEFORE STATEMENT"),
            "first timing-point END should stay in trigger statement: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("END AFTER STATEMENT"),
            "second timing-point END should stay in trigger statement: {}",
            statements[0]
        );
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn compound_trigger_nested_labeled_block_named_before_does_not_close_timing_point() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_label_before");
        engine.process_line("FOR INSERT ON t");
        engine.process_line("COMPOUND TRIGGER");
        engine.process_line("  BEFORE STATEMENT IS");
        engine.process_line("    <<before>>");
        engine.process_line("    BEGIN");
        engine.process_line("      NULL;");
        engine.process_line("    END before;");
        engine.process_line("  END BEFORE STATEMENT;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END before"),
            "labeled nested block should stay inside trigger body: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("END BEFORE STATEMENT"),
            "timing-point close should remain attached to compound trigger: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn compound_trigger_nested_labeled_block_named_after_does_not_close_timing_point() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_label_after");
        engine.process_line("FOR INSERT ON t");
        engine.process_line("COMPOUND TRIGGER");
        engine.process_line("  AFTER STATEMENT IS");
        engine.process_line("    <<after>>");
        engine.process_line("    BEGIN");
        engine.process_line("      NULL;");
        engine.process_line("    END after;");
        engine.process_line("  END AFTER STATEMENT;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END after"),
            "labeled nested block should stay inside trigger body: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("END AFTER STATEMENT"),
            "timing-point close should remain attached to compound trigger: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn compound_trigger_nested_labeled_block_named_instead_does_not_close_timing_point() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_label_instead");
        engine.process_line("INSTEAD OF INSERT ON v_orders");
        engine.process_line("COMPOUND TRIGGER");
        engine.process_line("  INSTEAD OF EACH ROW IS");
        engine.process_line("    <<instead>>");
        engine.process_line("    BEGIN");
        engine.process_line("      NULL;");
        engine.process_line("    END instead;");
        engine.process_line("  END INSTEAD OF EACH ROW;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END instead"),
            "labeled nested block should stay inside trigger body: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("END INSTEAD OF EACH ROW"),
            "timing-point close should remain attached to compound trigger: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn compound_trigger_body_identifier_before_followed_by_is_does_not_open_timing_point() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_before_ident");
        engine.process_line("FOR UPDATE ON t");
        engine.process_line("COMPOUND TRIGGER");
        engine.process_line("  BEFORE STATEMENT IS");
        engine.process_line("  BEGIN");
        engine.process_line("    IF before_value IS NULL THEN");
        engine.process_line("      NULL;");
        engine.process_line("    END IF;");
        engine.process_line("  END BEFORE STATEMENT;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
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
    fn non_plsql_with_clause_resets_pending_with_declaration_mode() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE VIEW v_read_only AS");
        engine.process_line("SELECT * FROM dual WITH READ ONLY;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("WITH READ ONLY"),
            "first statement should preserve trailing WITH READ ONLY clause: {}",
            statements[0]
        );
        assert_eq!(
            engine.state.with_clause_state,
            WithClauseState::None,
            "non-PL/SQL WITH clauses should not leave declaration tracking armed"
        );
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn non_plsql_with_check_option_clause_resets_pending_with_declaration_mode() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE VIEW v_checked AS");
        engine.process_line("SELECT * FROM dual WITH CHECK OPTION;");
        engine.process_line("SELECT 2 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("WITH CHECK OPTION"),
            "first statement should preserve WITH CHECK OPTION clause: {}",
            statements[0]
        );
        assert_eq!(
            engine.state.with_clause_state,
            WithClauseState::None,
            "WITH CHECK OPTION should not leave declaration tracking armed"
        );
        assert_eq!(statements[1], "SELECT 2 FROM dual".to_string());
    }

    #[test]
    fn non_plsql_with_rowid_clause_resets_pending_with_declaration_mode() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE VIEW v_rowid AS");
        engine.process_line("SELECT rowid rid, t.* FROM t WITH ROWID;");
        engine.process_line("SELECT 3 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("WITH ROWID"),
            "first statement should preserve WITH ROWID clause: {}",
            statements[0]
        );
        assert_eq!(
            engine.state.with_clause_state,
            WithClauseState::None,
            "WITH ROWID should not leave declaration tracking armed"
        );
        assert_eq!(statements[1], "SELECT 3 FROM dual".to_string());
    }

    #[test]
    fn non_plsql_with_clause_variants_reset_pending_with_declaration_mode() {
        for (suffix, marker) in [
            ("WITH NO DATA", "NO DATA"),
            ("WITH TIES", "TIES"),
        ] {
            let mut engine = SqlParserEngine::new();

            engine.process_line("CREATE OR REPLACE VIEW v_non_plsql_clause AS");
            engine.process_line(&format!("SELECT 1 AS v FROM dual {suffix};"));
            engine.process_line("SELECT 4 FROM dual;");

            let statements = engine.finalize_and_take_statements();

            assert_eq!(statements.len(), 2, "unexpected statements for {suffix}: {statements:?}");
            assert!(
                statements[0].contains(marker),
                "first statement should preserve trailing {marker} clause: {}",
                statements[0]
            );
            assert_eq!(
                engine.state.with_clause_state,
                WithClauseState::None,
                "{marker} should not leave declaration tracking armed"
            );
            assert_eq!(statements[1], "SELECT 4 FROM dual".to_string());
        }
    }

    #[test]
    fn materialized_view_log_with_sequence_resets_pending_with_declaration_mode() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE MATERIALIZED VIEW LOG ON mv_test WITH SEQUENCE;");
        engine.process_line("SELECT 9 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("WITH SEQUENCE"),
            "first statement should preserve WITH SEQUENCE clause: {}",
            statements[0]
        );
        assert_eq!(
            engine.state.with_clause_state,
            WithClauseState::None,
            "WITH SEQUENCE should not leave declaration tracking armed"
        );
        assert_eq!(statements[1], "SELECT 9 FROM dual".to_string());
    }

    #[test]
    fn materialized_view_log_with_commit_scn_resets_pending_with_declaration_mode() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE MATERIALIZED VIEW LOG ON mv_test WITH COMMIT SCN;");
        engine.process_line("SELECT 10 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("WITH COMMIT SCN"),
            "first statement should preserve WITH COMMIT SCN clause: {}",
            statements[0]
        );
        assert_eq!(
            engine.state.with_clause_state,
            WithClauseState::None,
            "WITH COMMIT SCN should not leave declaration tracking armed"
        );
        assert_eq!(statements[1], "SELECT 10 FROM dual".to_string());
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
    fn with_function_supports_all_oracle_main_query_heads() {
        let cases = [
            (
                "SELECT",
                vec!["SELECT f() FROM dual;", "SELECT 2 FROM dual;"],
            ),
            (
                "INSERT",
                vec![
                    "INSERT INTO t_result(v) VALUES (f());",
                    "SELECT 3 FROM dual;",
                ],
            ),
            (
                "UPDATE",
                vec!["UPDATE t_result SET v = f();", "SELECT 4 FROM dual;"],
            ),
            (
                "DELETE",
                vec!["DELETE FROM t_result WHERE v = f();", "SELECT 5 FROM dual;"],
            ),
            (
                "MERGE",
                vec![
                    "MERGE INTO t_result d USING (SELECT f() AS v FROM dual) s ON (d.v = s.v)",
                    "WHEN MATCHED THEN UPDATE SET d.v = s.v",
                    "WHEN NOT MATCHED THEN INSERT (v) VALUES (s.v);",
                    "SELECT 6 FROM dual;",
                ],
            ),
            ("VALUES", vec!["VALUES (f());", "SELECT 7 FROM dual;"]),
            (
                "TABLE",
                vec!["TABLE(sys.odcinumberlist(f()));", "SELECT 8 FROM dual;"],
            ),
        ];

        for (head, body_lines) in cases {
            let mut engine = SqlParserEngine::new();
            engine.process_line("WITH FUNCTION f RETURN NUMBER IS");
            engine.process_line("BEGIN");
            engine.process_line("  RETURN 1;");
            engine.process_line("END;");

            for line in body_lines {
                engine.process_line(line);
            }

            let statements = engine.finalize_and_take_statements();
            assert_eq!(
                statements.len(),
                2,
                "{head} main query head should keep WITH FUNCTION block attached: {statements:?}"
            );
            assert!(
                statements[0].contains("WITH FUNCTION f RETURN NUMBER IS"),
                "first statement should include WITH FUNCTION declaration for {head}: {}",
                statements[0]
            );
        }
    }

    #[test]
    fn with_function_recovery_splits_before_non_main_query_statement_heads() {
        let cases = [
            (
                "CREATE TABLE",
                vec![
                    "CREATE TABLE wf_recovery_t (id NUMBER);",
                    "SELECT 42 FROM dual;",
                ],
            ),
            (
                "ALTER SESSION",
                vec![
                    "ALTER SESSION SET NLS_DATE_FORMAT = ''YYYY-MM-DD'';",
                    "SELECT 43 FROM dual;",
                ],
            ),
            (
                "AUDIT",
                vec!["AUDIT SESSION;", "SELECT 44 FROM dual;"],
            ),
        ];

        for (head, lines) in cases {
            let mut engine = SqlParserEngine::new();

            engine.process_line("WITH FUNCTION f RETURN NUMBER IS");
            engine.process_line("BEGIN");
            engine.process_line("  RETURN 1;");
            engine.process_line("END;");

            for line in lines {
                engine.process_line(line);
            }

            let statements = engine.finalize_and_take_statements();
            assert_eq!(
                statements.len(),
                3,
                "{head} should be parsed as a standalone statement after WITH FUNCTION recovery: {statements:?}"
            );
            assert!(
                statements[0].contains("WITH FUNCTION f RETURN NUMBER IS"),
                "first statement should remain the completed WITH FUNCTION declaration for {head}: {}",
                statements[0]
            );
            assert!(
                statements[1].starts_with(head),
                "second statement should start with {head}: {}",
                statements[1]
            );
            assert!(
                statements[2].starts_with("SELECT"),
                "third statement should preserve trailing SELECT after {head}: {}",
                statements[2]
            );
        }
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
    fn compound_trigger_timing_point_without_is_still_splits_on_outer_end() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_compound_no_is");
        engine.process_line("FOR INSERT ON t");
        engine.process_line("COMPOUND TRIGGER");
        engine.process_line("  BEFORE STATEMENT");
        engine.process_line("  BEGIN");
        engine.process_line("    NULL;");
        engine.process_line("  END BEFORE STATEMENT;");
        engine.process_line("END;");
        engine.process_line("SELECT 9 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END BEFORE STATEMENT"),
            "timing-point END without IS must stay inside trigger body: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 9 FROM dual".to_string());
    }

    #[test]
    fn compound_trigger_timing_point_with_declare_section_splits_on_outer_end() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_compound_decl");
        engine.process_line("FOR INSERT ON t");
        engine.process_line("COMPOUND TRIGGER");
        engine.process_line("  BEFORE EACH ROW IS");
        engine.process_line("    DECLARE");
        engine.process_line("      v_local NUMBER := 1;");
        engine.process_line("    BEGIN");
        engine.process_line("      :NEW.id := v_local;");
        engine.process_line("    END BEFORE EACH ROW;");
        engine.process_line("END;");
        engine.process_line("SELECT 3 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("DECLARE\n      v_local NUMBER := 1;"),
            "timing-point declare section should stay inside compound trigger: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("END BEFORE EACH ROW"),
            "timing-point END BEFORE should stay inside trigger statement: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 3 FROM dual".to_string());
    }

    #[test]
    fn compound_trigger_timing_point_end_with_label_stays_in_trigger_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_compound_label");
        engine.process_line("FOR UPDATE ON t");
        engine.process_line("COMPOUND TRIGGER");
        engine.process_line("  BEFORE EACH ROW IS");
        engine.process_line("  BEGIN");
        engine.process_line("    NULL;");
        engine.process_line("  END BEFORE EACH ROW tp_done;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END BEFORE EACH ROW tp_done;"),
            "timing-point END label should stay in compound trigger statement: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn case_expression_followed_by_for_update_keeps_same_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("BEGIN");
        engine.process_line("  SELECT CASE WHEN status = 'READY' THEN id ELSE 0 END");
        engine.process_line("    INTO v_id");
        engine.process_line("    FROM jobs");
        engine.process_line("    FOR UPDATE SKIP LOCKED;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END\n    INTO v_id\n    FROM jobs\n    FOR UPDATE SKIP LOCKED;\nEND"),
            "FOR UPDATE clause should remain in the same PL/SQL block after CASE END: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn external_language_parameters_without_semicolon_splits_on_slash() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_params RETURN NUMBER");
        engine.process_line("AS LANGUAGE C PARAMETERS (CONTEXT)");
        engine.process_line("/");
        engine.process_line("SELECT 2 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS LANGUAGE C PARAMETERS (CONTEXT)"),
            "call specification should stay in routine statement: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("SELECT 2 FROM dual"),
            "trailing query should remain standalone after slash delimiter: {}",
            statements[1]
        );
    }

    #[test]
    fn aggregate_using_clause_without_external_keyword_marks_external_routine_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_agg RETURN NUMBER");
        engine.process_line("AS AGGREGATE USING ext_agg_impl;");
        engine.process_line("SELECT 11 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS AGGREGATE USING ext_agg_impl"));
        assert!(statements[1].starts_with("SELECT 11 FROM dual"));
    }

    #[test]
    fn pipelined_using_clause_without_external_keyword_marks_external_routine_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_pipe RETURN sys.odcinumberlist");
        engine.process_line("AS PIPELINED USING ext_pipe_impl;");
        engine.process_line("SELECT 12 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS PIPELINED USING ext_pipe_impl"));
        assert!(statements[1].starts_with("SELECT 12 FROM dual"));
    }

    #[test]
    fn sql_macro_call_spec_without_external_keyword_splits_before_following_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_macro RETURN VARCHAR2");
        engine.process_line("AS SQL_MACRO;");
        engine.process_line("SELECT 12 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS SQL_MACRO"));
        assert!(statements[1].starts_with("SELECT 12 FROM dual"));
    }

    #[test]
    fn package_nested_sql_macro_call_spec_closes_nested_function_block_on_semicolon() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PACKAGE BODY pkg_sql_macro AS");
        engine.process_line("  FUNCTION f RETURN VARCHAR2");
        engine.process_line("  IS SQL_MACRO;");
        engine.process_line("END pkg_sql_macro;");
        engine.process_line("SELECT 12 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("FUNCTION f RETURN VARCHAR2"));
        assert!(statements[0].contains("IS SQL_MACRO"));
        assert!(statements[0].contains("END pkg_sql_macro"));
        assert!(statements[1].starts_with("SELECT 12 FROM dual"));
    }

    #[test]
    fn external_language_without_target_but_clause_keywords_still_splits() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn RETURN NUMBER");
        engine.process_line("AS EXTERNAL LANGUAGE PARAMETERS('x') NAME 'ext_fn';");
        engine.process_line("SELECT 13 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS EXTERNAL LANGUAGE PARAMETERS('x') NAME 'ext_fn'"),
            "external call spec should stay in first statement: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("SELECT 13 FROM dual"),
            "SELECT should be split into next statement: {}",
            statements[1]
        );
    }

    #[test]
    fn external_language_without_target_still_splits_at_top_level() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn_missing_target RETURN NUMBER");
        engine.process_line("AS EXTERNAL LANGUAGE;");
        engine.process_line("SELECT 13 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS EXTERNAL LANGUAGE"),
            "external call spec should stay in first statement: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("SELECT 13 FROM dual"),
            "SELECT should be split into next statement: {}",
            statements[1]
        );
    }

    #[test]
    fn package_nested_external_without_language_target_closes_on_semicolon() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PACKAGE BODY pkg_ext_missing_target AS");
        engine.process_line("  PROCEDURE p IS EXTERNAL LANGUAGE PARAMETERS('x') NAME 'p';");
        engine.process_line("END pkg_ext_missing_target;");
        engine.process_line("SELECT 14 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("PROCEDURE p IS EXTERNAL LANGUAGE PARAMETERS('x') NAME 'p'"),
            "nested external routine should remain inside package body: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("END pkg_ext_missing_target"),
            "package body END should stay in first statement: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 14 FROM dual".to_string());
    }

    #[test]
    fn package_nested_external_language_without_target_closes_on_semicolon() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PACKAGE BODY pkg_ext_missing_lang_target AS");
        engine.process_line("  PROCEDURE p IS EXTERNAL LANGUAGE;");
        engine.process_line("END pkg_ext_missing_lang_target;");
        engine.process_line("SELECT 14 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("PROCEDURE p IS EXTERNAL LANGUAGE"),
            "nested external routine should remain inside package body: {}",
            statements[0]
        );
        assert!(
            statements[0].contains("END pkg_ext_missing_lang_target"),
            "package body END should stay in first statement: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 14 FROM dual".to_string());
    }

    #[test]
    fn external_language_clause_splits_before_trailing_line_comment_and_select() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn RETURN NUMBER");
        engine.process_line("AS LANGUAGE C;");
        engine.process_line("-- next statement comment");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS LANGUAGE C"),
            "first statement should keep EXTERNAL call spec: {}",
            statements[0]
        );
        assert!(
            !statements[0].contains("next statement comment"),
            "line comment after external routine should belong to next statement: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("-- next statement comment\nSELECT 1 FROM dual"),
            "line comment should stay with the following statement: {}",
            statements[1]
        );
    }

    #[test]
    fn with_function_recovers_to_rem_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH FUNCTION local_fn RETURN NUMBER IS");
        engine.process_line("BEGIN");
        engine.process_line("  RETURN 1;");
        engine.process_line("END local_fn;");
        engine.process_line("REM trailing sqlplus comment");
        engine.process_line("SELECT local_fn() FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END local_fn"),
            "first statement should keep WITH FUNCTION declaration: {}",
            statements[0]
        );
        assert_eq!(
            statements[1],
            "REM trailing sqlplus comment".to_string(),
            "REM command should be auto-terminated as standalone statement: {}",
            statements[1]
        );
        assert!(
            statements[2].starts_with("SELECT local_fn() FROM dual"),
            "SELECT should remain standalone after REM command split: {}",
            statements[2]
        );
    }

    #[test]
    fn with_function_recovers_to_remark_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH PROCEDURE local_proc IS");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END local_proc;");
        engine.process_line("REMARK trailing sqlplus comment");
        engine.process_line("SELECT 13 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END local_proc"),
            "first statement should keep WITH PROCEDURE declaration: {}",
            statements[0]
        );
        assert_eq!(
            statements[1],
            "REMARK trailing sqlplus comment".to_string(),
            "REMARK command should be auto-terminated as standalone statement: {}",
            statements[1]
        );
        assert!(
            statements[2].starts_with("SELECT 13 FROM dual"),
            "SELECT should remain standalone after REMARK command split: {}",
            statements[2]
        );
    }

    #[test]
    fn sqlplus_connect_command_keeps_following_statement_separate_without_semicolon() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CONNECT scott/tiger");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert_eq!(statements[0], "CONNECT scott/tiger".to_string());
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn sqlplus_start_command_keeps_following_statement_separate_without_semicolon() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("START child.sql");
        engine.process_line("SELECT 2 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert_eq!(statements[0], "START child.sql".to_string());
        assert_eq!(statements[1], "SELECT 2 FROM dual".to_string());
    }

    #[test]
    fn bare_start_line_is_not_misclassified_as_sqlplus_start_command() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("SELECT employee_id");
        engine.process_line("FROM employees");
        engine.process_line("START");
        engine.process_line("WITH manager_id IS NULL");
        engine.process_line("CONNECT BY PRIOR employee_id = manager_id;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 1, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("START
WITH manager_id IS NULL"),
            "multi-line START WITH clause should remain in the SELECT statement: {}",
            statements[0]
        );
    }

    #[test]
    fn bare_connect_line_is_not_misclassified_as_sqlplus_connect_command() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("SELECT employee_id");
        engine.process_line("FROM employees");
        engine.process_line("START WITH manager_id IS NULL");
        engine.process_line("CONNECT");
        engine.process_line("BY PRIOR employee_id = manager_id;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 1, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("CONNECT
BY PRIOR employee_id = manager_id"),
            "multi-line CONNECT BY clause should remain in the SELECT statement: {}",
            statements[0]
        );
    }

    #[test]
    fn oracle_select_identifier_prompt_is_not_misclassified_as_sqlplus_prompt_command() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("SELECT");
        engine.process_line("  PROMPT");
        engine.process_line("FROM tool_words;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 1, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("PROMPT"),
            "column identifier should remain in SELECT statement: {}",
            statements[0]
        );
    }

    #[test]
    fn oracle_start_with_clause_is_not_misclassified_as_sqlplus_start_command() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("SELECT employee_id");
        engine.process_line("FROM employees");
        engine.process_line("START WITH manager_id IS NULL");
        engine.process_line("CONNECT BY PRIOR employee_id = manager_id;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 1, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("START WITH manager_id IS NULL"),
            "hierarchical START WITH clause should remain in the SELECT statement: {}",
            statements[0]
        );
    }

    #[test]
    fn oracle_start_with_clause_with_inline_comment_is_not_misclassified_as_sqlplus_start_command()
    {
        let mut engine = SqlParserEngine::new();

        engine.process_line("SELECT employee_id");
        engine.process_line("FROM employees");
        engine.process_line("START /*tree root*/ WITH manager_id IS NULL");
        engine.process_line("CONNECT BY PRIOR employee_id = manager_id;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 1, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("START /*tree root*/ WITH manager_id IS NULL"),
            "hierarchical START WITH clause should remain in the SELECT statement: {}",
            statements[0]
        );
    }

    #[test]
    fn oracle_connect_by_clause_with_inline_comment_is_not_misclassified_as_sqlplus_connect_command(
    ) {
        let mut engine = SqlParserEngine::new();

        engine.process_line("SELECT employee_id");
        engine.process_line("FROM employees");
        engine.process_line("START WITH manager_id IS NULL");
        engine.process_line("CONNECT /*hierarchical*/ BY PRIOR employee_id = manager_id;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 1, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("CONNECT /*hierarchical*/ BY PRIOR employee_id = manager_id"),
            "hierarchical CONNECT BY clause should remain in the SELECT statement: {}",
            statements[0]
        );
    }

    #[test]
    fn external_language_clause_splits_before_trailing_block_comment_and_select() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn2 RETURN NUMBER");
        engine.process_line("AS LANGUAGE C;");
        engine.process_line("/* next statement comment */");
        engine.process_line("SELECT 2 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            !statements[0].contains("next statement comment"),
            "block comment after external routine should belong to next statement: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("/* next statement comment */\nSELECT 2 FROM dual"),
            "block comment should stay with the following statement: {}",
            statements[1]
        );
    }

    #[test]
    fn non_cte_with_clause_keyword_does_not_leak_into_following_comment_on_function() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("GRANT CREATE SESSION TO app_user WITH ADMIN OPTION;");
        engine.process_line("COMMENT ON FUNCTION app_user.f IS 'ok';");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("GRANT CREATE SESSION TO app_user WITH ADMIN OPTION"),
            "first statement should remain the GRANT statement: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("COMMENT ON FUNCTION app_user.f IS 'ok'"),
            "second statement should remain a standalone COMMENT ON FUNCTION statement: {}",
            statements[1]
        );
        assert!(
            statements[2].starts_with("SELECT 1 FROM dual"),
            "third statement should remain a standalone SELECT statement: {}",
            statements[2]
        );
    }

    #[test]
    fn non_cte_with_clause_keyword_does_not_leak_into_following_comment_on_procedure() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("GRANT CREATE SESSION TO app_user WITH ADMIN OPTION;");
        engine.process_line("COMMENT ON PROCEDURE app_user.p IS 'ok';");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("GRANT CREATE SESSION TO app_user WITH ADMIN OPTION"),
            "first statement should remain the GRANT statement: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("COMMENT ON PROCEDURE app_user.p IS 'ok'"),
            "second statement should remain a standalone COMMENT ON PROCEDURE statement: {}",
            statements[1]
        );
        assert!(
            statements[2].starts_with("SELECT 1 FROM dual"),
            "third statement should remain a standalone SELECT statement: {}",
            statements[2]
        );
    }

    #[test]
    fn non_cte_with_delegate_option_does_not_leak_into_following_comment_on_function() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("GRANT READ ON DIRECTORY app_dir TO app_user WITH DELEGATE OPTION;");
        engine.process_line("COMMENT ON FUNCTION app_user.f IS 'ok';");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("GRANT READ ON DIRECTORY app_dir TO app_user WITH DELEGATE OPTION"),
            "first statement should remain the GRANT statement: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("COMMENT ON FUNCTION app_user.f IS 'ok'"),
            "second statement should remain a standalone COMMENT ON FUNCTION statement: {}",
            statements[1]
        );
        assert!(
            statements[2].starts_with("SELECT 1 FROM dual"),
            "third statement should remain a standalone SELECT statement: {}",
            statements[2]
        );
    }

    #[test]
    fn non_plsql_grant_with_clause_exits_pending_with_mode_without_semicolon() {
        let mut state = SplitState::default();

        state.track_top_level_with_plsql("GRANT", true);
        state.track_top_level_with_plsql("SELECT", false);
        state.track_top_level_with_plsql("ON", false);
        state.track_top_level_with_plsql("DUAL", false);
        state.track_top_level_with_plsql("TO", false);
        state.track_top_level_with_plsql("APP_USER", false);
        state.track_top_level_with_plsql("WITH", false);
        state.track_top_level_with_plsql("GRANT", false);

        assert_eq!(
            state.with_clause_state,
            WithClauseState::None,
            "WITH GRANT OPTION should immediately exit WITH FUNCTION/PROCEDURE tracking"
        );
    }

    #[test]
    fn non_plsql_grant_with_delegate_option_exits_pending_with_mode_without_semicolon() {
        let mut state = SplitState::default();

        state.track_top_level_with_plsql("GRANT", true);
        state.track_top_level_with_plsql("APP_ROLE", false);
        state.track_top_level_with_plsql("TO", false);
        state.track_top_level_with_plsql("APP_USER", false);
        state.track_top_level_with_plsql("WITH", false);
        state.track_top_level_with_plsql("DELEGATE", false);

        assert_eq!(
            state.with_clause_state,
            WithClauseState::None,
            "WITH DELEGATE OPTION should immediately exit WITH FUNCTION/PROCEDURE tracking"
        );
    }

    #[test]
    fn implicit_external_language_clause_splits_before_following_begin_block() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn_begin RETURN NUMBER");
        engine.process_line("AS LANGUAGE C");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE C"));
        assert!(statements[1].starts_with("BEGIN\n  NULL;\nEND"));
        assert!(statements[2].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn implicit_external_language_clause_on_procedure_splits_before_following_begin_block() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PROCEDURE ext_proc_begin");
        engine.process_line("AS LANGUAGE C");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END;");
        engine.process_line("SELECT 101 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE C"));
        assert!(statements[1].starts_with("BEGIN\n  NULL;\nEND"));
        assert!(statements[2].starts_with("SELECT 101 FROM dual"));
    }

    #[test]
    fn implicit_external_literal_target_clause_on_procedure_with_semicolon_keeps_block_together() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PROCEDURE ext_proc_begin_literal");
        engine.process_line("AS LANGUAGE 'C';");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END;");
        engine.process_line("SELECT 102 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE 'C'"));
        assert!(statements[0].contains("BEGIN\n  NULL;\nEND"));
        assert!(statements[1].starts_with("SELECT 102 FROM dual"));
    }

    #[test]
    fn explicit_external_language_clause_splits_before_following_begin_block() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn_begin_explicit RETURN NUMBER");
        engine.process_line("AS EXTERNAL LANGUAGE C;");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END;");
        engine.process_line("SELECT 39 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS EXTERNAL LANGUAGE C"));
        assert!(statements[1].starts_with("BEGIN\n  NULL;\nEND"));
        assert!(statements[2].starts_with("SELECT 39 FROM dual"));
    }

    #[test]
    fn implicit_external_literal_target_clause_splits_before_following_begin_block() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn_begin_literal RETURN NUMBER");
        engine.process_line("AS LANGUAGE 'C';");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END;");
        engine.process_line("SELECT 40 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE 'C'"));
        assert!(statements[1].starts_with("BEGIN\n  NULL;\nEND"));
        assert!(statements[2].starts_with("SELECT 40 FROM dual"));
    }

    #[test]
    fn external_language_clause_splits_before_run_script_marker_at_sign() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn_at RETURN NUMBER");
        engine.process_line("AS LANGUAGE C;");
        engine.process_line("@next_script.sql");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS LANGUAGE C"),
            "first statement should keep EXTERNAL call spec: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("@next_script.sql"),
            "run-script marker should start the next statement after external routine split: {}",
            statements[1]
        );
    }

    #[test]
    fn external_language_clause_splits_before_run_script_marker_double_at() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn_double_at RETURN NUMBER");
        engine.process_line("AS LANGUAGE C;");
        engine.process_line("@@child_script.sql");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS LANGUAGE C"),
            "first statement should keep EXTERNAL call spec: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("@@child_script.sql"),
            "double run-script marker should start the next statement after external routine split: {}",
            statements[1]
        );
    }

    #[test]
    fn with_function_waiting_main_query_recovers_on_slash_line_with_sqlplus_comment() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH");
        engine.process_line("  FUNCTION f RETURN NUMBER IS");
        engine.process_line("  BEGIN");
        engine.process_line("    RETURN 1;");
        engine.process_line("  END;");
        engine.process_line("/");
        engine.process_line("-- rerun statement");
        engine.process_line("SELECT f() FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
            "WITH FUNCTION declaration should remain the first statement: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("/\n-- rerun statement\nSELECT f() FROM dual"),
            "slash terminator with SQL*Plus comment should start the next statement: {}",
            statements[1]
        );
    }

    #[test]
    fn external_language_clause_splits_before_slash_line_with_sqlplus_remark() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn_slash_rem RETURN NUMBER");
        engine.process_line("AS LANGUAGE C;");
        engine.process_line("/ REM rerun external");
        engine.process_line("SELECT 52 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE C"));
        assert!(
            statements[1].starts_with("SELECT 52 FROM dual"),
            "slash delimiter line should not leak into next statement: {}",
            statements[1]
        );
    }

    #[test]
    fn external_language_clause_splits_before_lowercase_sqlplus_remark_on_slash_line() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn_slash_rem_lower RETURN NUMBER");
        engine.process_line("AS LANGUAGE C;");
        engine.process_line("/ remark rerun external");
        engine.process_line("SELECT 152 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE C"));
        assert!(
            statements[1].starts_with("SELECT 152 FROM dual"),
            "slash delimiter line should not leak into next statement: {}",
            statements[1]
        );
    }

    #[test]
    fn with_function_waiting_main_query_recovers_on_lowercase_sqlplus_remark_slash_line() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH");
        engine.process_line("  FUNCTION f RETURN NUMBER IS");
        engine.process_line("  BEGIN");
        engine.process_line("    RETURN 1;");
        engine.process_line("  END;");
        engine.process_line("/ rem keep parsing");
        engine.process_line("SELECT f() FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].starts_with("WITH\n  FUNCTION f RETURN NUMBER IS"),
            "WITH FUNCTION declaration should remain the first statement: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("/ rem keep parsing\nSELECT f() FROM dual"),
            "slash line with lowercase rem should start the next statement: {}",
            statements[1]
        );
    }

    #[test]
    fn external_language_clause_splits_before_sqlplus_slash_terminator_line() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn_slash RETURN NUMBER");
        engine.process_line("AS LANGUAGE C;");
        engine.process_line("/");
        engine.process_line("SELECT 51 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE C"));
        assert!(
            statements[1].starts_with("SELECT 51 FROM dual"),
            "slash delimiter line should not leak into next statement: {}",
            statements[1]
        );
    }

    #[test]
    fn external_language_clause_splits_before_slash_line_with_block_comment() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn_slash_block RETURN NUMBER");
        engine.process_line("AS LANGUAGE C;");
        engine.process_line("/ /* rerun external */");
        engine.process_line("SELECT 251 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE C"));
        assert!(
            statements[1].starts_with("/ /* rerun external */\nSELECT 251 FROM dual"),
            "slash line with block comment should start the next statement: {}",
            statements[1]
        );
    }

    #[test]
    fn external_language_clause_splits_before_prompt_command() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn_prompt RETURN NUMBER");
        engine.process_line("AS LANGUAGE C;");
        engine.process_line("PROMPT after external");
        engine.process_line("SELECT 33 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS LANGUAGE C"),
            "first statement should keep EXTERNAL call spec: {}",
            statements[0]
        );
        assert_eq!(
            statements[1],
            "PROMPT after external\nSELECT 33 FROM dual".to_string()
        );
    }

    #[test]
    fn external_language_clause_splits_before_host_command() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn_host RETURN NUMBER");
        engine.process_line("AS LANGUAGE C;");
        engine.process_line("HOST ls");
        engine.process_line("SELECT 34 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS LANGUAGE C"),
            "first statement should keep EXTERNAL call spec: {}",
            statements[0]
        );
        assert_eq!(statements[1], "HOST ls\nSELECT 34 FROM dual".to_string());
    }

    #[test]
    fn external_language_clause_splits_before_bang_host_command() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn_bang_host RETURN NUMBER");
        engine.process_line("AS LANGUAGE C;");
        engine.process_line("! ls");
        engine.process_line("SELECT 35 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS LANGUAGE C"),
            "first statement should keep EXTERNAL call spec: {}",
            statements[0]
        );
        assert_eq!(statements[1], "! ls\nSELECT 35 FROM dual;".to_string());
    }

    #[test]
    fn external_language_clause_splits_before_exit_command() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn_exit RETURN NUMBER");
        engine.process_line("AS LANGUAGE C;");
        engine.process_line("EXIT");
        engine.process_line("SELECT 36 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS LANGUAGE C"),
            "first statement should keep EXTERNAL call spec: {}",
            statements[0]
        );
        assert_eq!(statements[1], "EXIT\nSELECT 36 FROM dual".to_string());
    }

    #[test]
    fn external_language_clause_splits_before_create_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn_next_create RETURN NUMBER");
        engine.process_line("AS LANGUAGE C;");
        engine.process_line("CREATE TABLE t_after_ext (id NUMBER);");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS LANGUAGE C"),
            "first statement should keep EXTERNAL call spec: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("CREATE TABLE t_after_ext"),
            "CREATE statement should begin a new statement after external routine split: {}",
            statements[1]
        );
    }

    #[test]
    fn trigger_referencing_alias_with_when_clause_splits_before_next_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_ref_alias");
        engine.process_line("BEFORE INSERT ON t");
        engine.process_line("REFERENCING NEW AS n");
        engine.process_line("FOR EACH ROW");
        engine.process_line("WHEN (n.id IS NULL)");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn simple_trigger_call_body_without_as_is_splits_before_next_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_call_only");
        engine.process_line("BEFORE INSERT ON t");
        engine.process_line("FOR EACH ROW");
        engine.process_line("CALL pkg_trg.fire();");
        engine.process_line("SELECT 42 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("CALL pkg_trg.fire()"));
        assert_eq!(statements[1], "SELECT 42 FROM dual".to_string());
    }

    #[test]
    fn package_spec_with_subprogram_declarations_keeps_single_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PACKAGE pkg_tmp IS");
        engine.process_line("  FUNCTION f RETURN NUMBER;");
        engine.process_line("  PROCEDURE p;");
        engine.process_line("END pkg_tmp;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("FUNCTION f RETURN NUMBER;"));
        assert!(statements[1].starts_with("SELECT 1 FROM dual"));
    }

    #[test]
    fn with_function_followed_by_lock_statement_recovers() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH FUNCTION f RETURN NUMBER IS");
        engine.process_line("BEGIN");
        engine.process_line("  RETURN 1;");
        engine.process_line("END;");
        engine.process_line("LOCK TABLE emp IN EXCLUSIVE MODE;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(statements[1].starts_with("LOCK TABLE emp IN EXCLUSIVE MODE"));
    }

    #[test]
    fn with_function_followed_by_run_script_marker_recovers() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH FUNCTION f RETURN NUMBER IS");
        engine.process_line("BEGIN");
        engine.process_line("  RETURN 1;");
        engine.process_line("END;");
        engine.process_line("@child.sql");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(statements[1].starts_with("@child.sql"));
    }

    #[test]
    fn with_function_waiting_main_query_recovers_on_sqlplus_slash_terminator_line() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH FUNCTION f RETURN NUMBER IS");
        engine.process_line("BEGIN");
        engine.process_line("  RETURN 1;");
        engine.process_line("END;");
        engine.process_line("/");
        engine.process_line("SELECT 52 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("END"));
        assert!(
            statements[1].starts_with("/\nSELECT 52 FROM dual"),
            "slash marker line should start the next statement: {}",
            statements[1]
        );
    }

    #[test]
    fn sqlplus_spool_command_is_auto_terminated() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("SPOOL out.log");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(
            statements,
            vec![
                "SPOOL out.log".to_string(),
                "SELECT 1 FROM dual".to_string()
            ]
        );
    }

    #[test]
    fn sqlplus_set_command_is_auto_terminated() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("SET SERVEROUTPUT ON");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(
            statements,
            vec![
                "SET SERVEROUTPUT ON".to_string(),
                "SELECT 1 FROM dual".to_string()
            ]
        );
    }

    #[test]
    fn sqlplus_set_command_with_block_comment_is_auto_terminated() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("SET /*sqlplus*/ SERVEROUTPUT ON");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(
            statements,
            vec![
                "SET /*sqlplus*/ SERVEROUTPUT ON".to_string(),
                "SELECT 1 FROM dual".to_string()
            ]
        );
    }

    #[test]
    fn sqlplus_show_command_is_auto_terminated() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("SHOW USER");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(
            statements,
            vec!["SHOW USER".to_string(), "SELECT 1 FROM dual".to_string()]
        );
    }

    #[test]
    fn sqlplus_describe_command_is_auto_terminated() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("DESC emp");
        engine.process_line("SELECT 53 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(
            statements,
            vec!["DESC emp".to_string(), "SELECT 53 FROM dual".to_string()]
        );
    }

    #[test]
    fn sqlplus_execute_command_is_auto_terminated() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("EXEC dbms_output.put_line('x')");
        engine.process_line("SELECT 54 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(
            statements,
            vec![
                "EXEC dbms_output.put_line('x')".to_string(),
                "SELECT 54 FROM dual".to_string(),
            ]
        );
    }

    #[test]
    fn external_language_clause_splits_before_alter_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn_next_alter RETURN NUMBER");
        engine.process_line("AS LANGUAGE C;");
        engine.process_line("ALTER SESSION SET optimizer_mode = ALL_ROWS;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS LANGUAGE C"),
            "first statement should keep EXTERNAL call spec: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("ALTER SESSION SET optimizer_mode = ALL_ROWS"),
            "ALTER statement should begin a new statement after external routine split: {}",
            statements[1]
        );
    }

    #[test]
    fn external_language_clause_splits_before_startup_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn_next_startup RETURN NUMBER");
        engine.process_line("AS LANGUAGE C;");
        engine.process_line("STARTUP;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS LANGUAGE C"),
            "first statement should keep EXTERNAL call spec: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("STARTUP"),
            "STARTUP command should begin a new statement after external routine split: {}",
            statements[1]
        );
    }

    #[test]
    fn sqlplus_startup_command_keeps_following_statement_separate_without_semicolon() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("STARTUP");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert_eq!(statements[0], "STARTUP".to_string());
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn sqlplus_shutdown_command_keeps_following_statement_separate_without_semicolon() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("SHUTDOWN IMMEDIATE");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert_eq!(statements[0], "SHUTDOWN IMMEDIATE".to_string());
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn sqlplus_archive_command_keeps_following_statement_separate_without_semicolon() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("ARCHIVE LOG LIST");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert_eq!(statements[0], "ARCHIVE LOG LIST".to_string());
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }

    #[test]
    fn sqlplus_recover_command_keeps_following_statement_separate_without_semicolon() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("RECOVER DATABASE");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();

        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert_eq!(statements[0], "RECOVER DATABASE".to_string());
        assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
    }
    #[test]
    fn external_language_clause_splits_before_shutdown_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn_next_shutdown RETURN NUMBER");
        engine.process_line("AS LANGUAGE C;");
        engine.process_line("SHUTDOWN;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS LANGUAGE C"),
            "first statement should keep EXTERNAL call spec: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("SHUTDOWN"),
            "SHUTDOWN command should begin a new statement after external routine split: {}",
            statements[1]
        );
    }

    #[test]
    fn procedure_with_implicit_language_target_splits_before_following_statement() {
        for target in ["C", "JAVASCRIPT", "MLE"] {
            let mut engine = SqlParserEngine::new();

            engine.process_line("CREATE OR REPLACE PROCEDURE ext_proc_implicit");
            engine.process_line(&format!("AS LANGUAGE {target};"));
            engine.process_line("SELECT 1 FROM dual;");

            let statements = engine.finalize_and_take_statements();

            assert_eq!(statements.len(), 2, "unexpected statements for {target}: {statements:?}");
            assert!(
                statements[0].contains(&format!("AS LANGUAGE {target};")),
                "first statement should keep implicit language target clause for {target}: {}",
                statements[0]
            );
            assert_eq!(statements[1], "SELECT 1 FROM dual".to_string());
        }
    }

    #[test]
    fn external_language_clause_splits_before_recover_statement_head_with_following_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_recover_head RETURN NUMBER");
        engine.process_line("AS LANGUAGE C;");
        engine.process_line("RECOVER DATABASE;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS LANGUAGE C"),
            "first statement should keep external routine: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("RECOVER DATABASE"),
            "RECOVER should begin a new statement after external routine split: {}",
            statements[1]
        );
        assert!(
            statements[2].starts_with("SELECT 1 FROM dual"),
            "SELECT should remain standalone after RECOVER recovery split: {}",
            statements[2]
        );
    }

    #[test]
    fn external_language_clause_splits_before_archive_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_archive_head RETURN NUMBER");
        engine.process_line("AS LANGUAGE C;");
        engine.process_line("ARCHIVE LOG LIST;");
        engine.process_line("SELECT 1 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS LANGUAGE C"),
            "first statement should keep external routine: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("ARCHIVE LOG LIST"),
            "ARCHIVE command should begin a new statement after external routine split: {}",
            statements[1]
        );
        assert!(
            statements[2].starts_with("SELECT 1 FROM dual"),
            "SELECT should remain standalone after ARCHIVE recovery split: {}",
            statements[2]
        );
    }

    #[test]
    fn external_language_clause_splits_before_recover_statement_head_without_following_select() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_fn_next_recover RETURN NUMBER");
        engine.process_line("AS LANGUAGE C;");
        engine.process_line("RECOVER DATABASE;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AS LANGUAGE C"),
            "first statement should keep EXTERNAL call spec: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("RECOVER DATABASE"),
            "RECOVER statement should begin a new statement after external routine split: {}",
            statements[1]
        );
    }

    #[test]
    fn with_function_recovers_before_alter_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH FUNCTION local_fn RETURN NUMBER IS");
        engine.process_line("BEGIN");
        engine.process_line("  RETURN 1;");
        engine.process_line("END local_fn;");
        engine.process_line("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD';");
        engine.process_line("SELECT local_fn() FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END local_fn"),
            "first statement should keep WITH FUNCTION declaration: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'"),
            "ALTER statement should start a new statement after WITH FUNCTION recovery: {}",
            statements[1]
        );
        assert!(
            statements[2].starts_with("SELECT local_fn() FROM dual"),
            "SELECT statement should remain standalone after ALTER recovery split: {}",
            statements[2]
        );
    }

    #[test]
    fn with_function_recovers_before_create_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH FUNCTION local_fn RETURN NUMBER IS");
        engine.process_line("BEGIN");
        engine.process_line("  RETURN 1;");
        engine.process_line("END local_fn;");
        engine.process_line("CREATE TABLE t_recovery_head (id NUMBER);");
        engine.process_line("SELECT local_fn() FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END local_fn"),
            "first statement should keep WITH FUNCTION declaration: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("CREATE TABLE t_recovery_head (id NUMBER)"),
            "CREATE statement should start a new statement after WITH FUNCTION recovery: {}",
            statements[1]
        );
        assert!(
            statements[2].starts_with("SELECT local_fn() FROM dual"),
            "SELECT statement should remain standalone after CREATE recovery split: {}",
            statements[2]
        );
    }

    #[test]
    fn with_function_recovers_before_startup_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH FUNCTION local_fn RETURN NUMBER IS");
        engine.process_line("BEGIN");
        engine.process_line("  RETURN 1;");
        engine.process_line("END local_fn;");
        engine.process_line("STARTUP;");
        engine.process_line("SELECT local_fn() FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END local_fn"),
            "first statement should keep WITH FUNCTION declaration: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("STARTUP"),
            "STARTUP command should start a new statement after WITH FUNCTION recovery: {}",
            statements[1]
        );
        assert!(
            statements[2].starts_with("SELECT local_fn() FROM dual"),
            "SELECT statement should remain standalone after STARTUP recovery split: {}",
            statements[2]
        );
    }

    #[test]
    fn with_function_recovers_before_shutdown_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH FUNCTION local_fn RETURN NUMBER IS");
        engine.process_line("BEGIN");
        engine.process_line("  RETURN 1;");
        engine.process_line("END local_fn;");
        engine.process_line("SHUTDOWN;");
        engine.process_line("SELECT local_fn() FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END local_fn"),
            "first statement should keep WITH FUNCTION declaration: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("SHUTDOWN"),
            "SHUTDOWN command should start a new statement after WITH FUNCTION recovery: {}",
            statements[1]
        );
        assert!(
            statements[2].starts_with("SELECT local_fn() FROM dual"),
            "SELECT statement should remain standalone after SHUTDOWN recovery split: {}",
            statements[2]
        );
    }

    #[test]
    fn with_function_recovers_before_administer_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH FUNCTION local_fn RETURN NUMBER IS");
        engine.process_line("BEGIN");
        engine.process_line("  RETURN 1;");
        engine.process_line("END local_fn;");
        engine.process_line("ADMINISTER KEY MANAGEMENT SET KEY IDENTIFIED BY \"pwd\";");
        engine.process_line("SELECT local_fn() FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END local_fn"),
            "first statement should keep WITH FUNCTION declaration: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("ADMINISTER KEY MANAGEMENT"),
            "ADMINISTER statement should start a new statement after WITH FUNCTION recovery: {}",
            statements[1]
        );
        assert!(
            statements[2].starts_with("SELECT local_fn() FROM dual"),
            "SELECT statement should remain standalone after ADMINISTER recovery split: {}",
            statements[2]
        );
    }

    #[test]
    fn with_function_recovers_before_recover_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH FUNCTION local_fn RETURN NUMBER IS");
        engine.process_line("BEGIN");
        engine.process_line("  RETURN 1;");
        engine.process_line("END local_fn;");
        engine.process_line("RECOVER DATABASE;");
        engine.process_line("SELECT local_fn() FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 3, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("END local_fn"),
            "first statement should keep WITH FUNCTION declaration: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with("RECOVER DATABASE"),
            "RECOVER statement should start a new statement after WITH FUNCTION recovery: {}",
            statements[1]
        );
        assert!(
            statements[2].starts_with("SELECT local_fn() FROM dual"),
            "SELECT statement should remain standalone after RECOVER recovery split: {}",
            statements[2]
        );
    }

    #[test]
    fn with_function_followed_by_parenthesized_main_query_stays_single_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH FUNCTION f RETURN NUMBER IS");
        engine.process_line("BEGIN");
        engine.process_line("  RETURN 1;");
        engine.process_line("END;");
        engine.process_line("(SELECT f() AS v FROM dual)");
        engine.process_line("UNION ALL");
        engine.process_line("SELECT 2 AS v FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 1, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("(SELECT f() AS v FROM dual)"),
            "parenthesized main query should remain attached to WITH FUNCTION statement: {}",
            statements[0]
        );
        assert!(
            statements[0].ends_with("SELECT 2 AS v FROM dual"),
            "union tail should remain attached: {}",
            statements[0]
        );
    }

    #[test]
    fn with_procedure_followed_by_parenthesized_main_query_stays_single_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("WITH PROCEDURE p IS");
        engine.process_line("BEGIN");
        engine.process_line("  NULL;");
        engine.process_line("END;");
        engine.process_line("(SELECT 1 AS v FROM dual)");
        engine.process_line("UNION ALL");
        engine.process_line("SELECT 2 AS v FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 1, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("(SELECT 1 AS v FROM dual)"),
            "parenthesized main query should remain attached to WITH PROCEDURE statement: {}",
            statements[0]
        );
        assert!(
            statements[0].ends_with("SELECT 2 AS v FROM dual"),
            "union tail should remain attached: {}",
            statements[0]
        );
    }

    #[test]
    fn trigger_follows_precedes_and_instead_of_forms_split_normally() {
        let cases = [
            (
                vec![
                    "CREATE OR REPLACE TRIGGER trg_follows",
                    "AFTER INSERT ON emp",
                    "FOLLOWS trg_base",
                    "BEGIN",
                    "  NULL;",
                    "END;",
                    "SELECT 1 FROM dual;",
                ],
                "SELECT 1 FROM dual",
            ),
            (
                vec![
                    "CREATE OR REPLACE TRIGGER trg_precedes",
                    "BEFORE UPDATE ON emp",
                    "PRECEDES trg_base",
                    "BEGIN",
                    "  NULL;",
                    "END;",
                    "SELECT 2 FROM dual;",
                ],
                "SELECT 2 FROM dual",
            ),
            (
                vec![
                    "CREATE OR REPLACE TRIGGER trg_instead_view",
                    "INSTEAD OF INSERT ON emp_v",
                    "BEGIN",
                    "  NULL;",
                    "END;",
                    "SELECT 3 FROM dual;",
                ],
                "SELECT 3 FROM dual",
            ),
        ];

        for (lines, tail_head) in cases {
            let mut engine = SqlParserEngine::new();
            for line in lines {
                engine.process_line(line);
            }

            let statements = engine.finalize_and_take_statements();
            assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
            assert!(
                statements[1].starts_with(tail_head),
                "trailing SELECT should split from trigger DDL: {}",
                statements[1]
            );
        }
    }

    #[test]
    fn trigger_referencing_alias_with_quoted_identifier_keeps_call_body_is_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_ref_alias_quoted_call_is");
        engine.process_line("BEFORE INSERT ON t");
        engine.process_line("REFERENCING NEW IS \"N\"");
        engine.process_line("FOR EACH ROW");
        engine.process_line("IS");
        engine.process_line("CALL pkg_trg.fire();");
        engine.process_line("SELECT 37 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("CALL pkg_trg.fire()"),
            "trigger CALL body should remain in first statement: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 37 FROM dual".to_string());
    }

    #[test]
    fn trigger_referencing_alias_with_quoted_identifier_keeps_call_body_as_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TRIGGER trg_ref_alias_quoted_call_as");
        engine.process_line("BEFORE INSERT ON t");
        engine.process_line("REFERENCING NEW AS \"N\"");
        engine.process_line("FOR EACH ROW");
        engine.process_line("AS");
        engine.process_line("CALL pkg_trg.fire();");
        engine.process_line("SELECT 38 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("CALL pkg_trg.fire()"),
            "trigger CALL body should remain in first statement: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 38 FROM dual".to_string());
    }

    #[test]
    fn create_function_aggregate_using_clause_splits_before_following_statement() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION median_agg(x NUMBER)");
        engine.process_line("RETURN NUMBER");
        engine.process_line("AGGREGATE USING median_impl;");
        engine.process_line("SELECT 39 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("AGGREGATE USING median_impl"),
            "AGGREGATE USING call spec should stay in CREATE statement: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 39 FROM dual".to_string());
    }

    #[test]
    fn create_function_pipelined_using_clause_without_semicolon_uses_slash_terminator() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION stream_rows");
        engine.process_line("RETURN row_tab_t PIPELINED");
        engine.process_line("USING stream_rows_impl");
        engine.process_line("/");
        engine.process_line("SELECT 40 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("USING stream_rows_impl"),
            "PIPELINED USING clause should stay in CREATE statement: {}",
            statements[0]
        );
        assert!(
            statements[1].contains("SELECT 40 FROM dual"),
            "trailing SELECT should split after slash terminator: {}",
            statements[1]
        );
    }

    #[test]
    fn package_body_polymorphic_pipelined_using_clause_closes_nested_routine() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PACKAGE BODY pkg_poly AS");
        engine.process_line("  FUNCTION stream_rows RETURN row_tab_t");
        engine.process_line("  IS PIPELINED ROW POLYMORPHIC USING stream_rows_impl;");
        engine.process_line("END pkg_poly;");
        engine.process_line("SELECT 41 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("PIPELINED ROW POLYMORPHIC USING stream_rows_impl"),
            "polymorphic PIPELINED USING call spec should remain in package body: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 41 FROM dual".to_string());
    }

    #[test]
    fn package_body_table_polymorphic_pipelined_using_clause_closes_nested_routine() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PACKAGE BODY pkg_poly_table AS");
        engine.process_line("  FUNCTION stream_rows RETURN row_tab_t");
        engine.process_line("  IS PIPELINED TABLE POLYMORPHIC USING stream_rows_impl;");
        engine.process_line("END pkg_poly_table;");
        engine.process_line("SELECT 42 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(
            statements[0].contains("PIPELINED TABLE POLYMORPHIC USING stream_rows_impl"),
            "table polymorphic PIPELINED USING call spec should remain in package body: {}",
            statements[0]
        );
        assert_eq!(statements[1], "SELECT 42 FROM dual".to_string());
    }

    #[test]
    fn conditional_compilation_directives_do_not_break_following_statement_split() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("BEGIN");
        engine.process_line("  $IF $$PLSQL_DEBUG $THEN");
        engine.process_line("    NULL;");
        engine.process_line("  $ELSE");
        engine.process_line("    NULL;");
        engine.process_line("  $END");
        engine.process_line("END;");
        engine.process_line("SELECT 41 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("$IF $$PLSQL_DEBUG $THEN"));
        assert_eq!(statements[1], "SELECT 41 FROM dual".to_string());
    }

    #[test]
    fn language_javascript_mle_module_clause_splits_following_select() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_lang_mle RETURN NUMBER");
        engine.process_line("AS LANGUAGE JAVASCRIPT MLE MODULE ext_mod SIGNATURE 'sig';");
        engine.process_line("SELECT 42 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE JAVASCRIPT MLE MODULE ext_mod"));
        assert_eq!(statements[1], "SELECT 42 FROM dual".to_string());
    }

    #[test]
    fn nested_external_function_in_package_body_keeps_package_statement_intact() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PACKAGE BODY pkg_ext AS");
        engine.process_line("  FUNCTION f RETURN NUMBER");
        engine.process_line("  AS LANGUAGE C NAME 'f';");
        engine.process_line("END pkg_ext;");
        engine.process_line("SELECT 43 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("CREATE OR REPLACE PACKAGE BODY pkg_ext AS"));
        assert!(statements[0].contains("AS LANGUAGE C NAME 'f';"));
        assert!(statements[0].contains("END pkg_ext"));
        assert_eq!(statements[1], "SELECT 43 FROM dual".to_string());
    }

    #[test]
    fn nested_external_function_with_quoted_language_target_closes_subprogram_block() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE PACKAGE BODY pkg_ext_quote AS");
        engine.process_line("  FUNCTION f RETURN NUMBER");
        engine.process_line("  AS LANGUAGE 'C' NAME 'f';");
        engine.process_line("END pkg_ext_quote;");
        engine.process_line("SELECT 44 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE 'C' NAME 'f';"));
        assert!(statements[0].contains("END pkg_ext_quote"));
        assert_eq!(statements[1], "SELECT 44 FROM dual".to_string());
    }

    #[test]
    fn type_body_member_external_call_spec_does_not_split_before_type_end() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE TYPE BODY t_ext_member AS");
        engine.process_line("  MAP MEMBER FUNCTION f RETURN NUMBER");
        engine.process_line("  IS LANGUAGE C NAME 'f';");
        engine.process_line("END;");
        engine.process_line("SELECT 45 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("CREATE OR REPLACE TYPE BODY t_ext_member AS"));
        assert!(statements[0].contains("MAP MEMBER FUNCTION f RETURN NUMBER"));
        assert!(statements[0].contains("IS LANGUAGE C NAME 'f';"));
        assert!(statements[0].contains("END"));
        assert_eq!(statements[1], "SELECT 45 FROM dual".to_string());
    }

    #[test]
    fn external_language_target_without_semicolon_splits_before_following_statement_head() {
        let mut engine = SqlParserEngine::new();

        engine.process_line("CREATE OR REPLACE FUNCTION ext_plain_lang RETURN NUMBER");
        engine.process_line("AS LANGUAGE C");
        engine.process_line("SELECT 46 FROM dual;");

        let statements = engine.finalize_and_take_statements();
        assert_eq!(statements.len(), 2, "unexpected statements: {statements:?}");
        assert!(statements[0].contains("AS LANGUAGE C"));
        assert!(
            statements[1].starts_with("SELECT 46 FROM dual"),
            "SELECT should begin a new statement after implicit external call spec: {}",
            statements[1]
        );
    }
}
