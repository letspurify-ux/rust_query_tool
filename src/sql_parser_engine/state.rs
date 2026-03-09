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
    package_body_name: Option<String>,
    awaiting_package_body_name: bool,

    // -- Parenthesis depth (for formatting / intellisense) --
    pub(crate) paren_depth: usize,

    // -- Oracle top-level WITH FUNCTION/PROCEDURE declarations --
    with_clause_state: WithClauseState,
    top_level_token_state: TopLevelTokenState,

    // -- Reusable buffer --
    token_upper_buf: String,
    quoted_identifier_buf: String,
    pending_implicit_external_top_level_split: bool,
    pending_end_label_token: Option<String>,
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

    fn should_split_before_external_begin_block(&self, token_upper: &str) -> bool {
        if token_upper != "BEGIN" {
            return false;
        }

        if self.block_depth() != 1 || self.paren_depth != 0 {
            return false;
        }

        if self.should_split_on_semicolon() {
            return true;
        }

        self.active_routine_frame().is_some_and(|frame| {
            matches!(
                frame.external_clause_state,
                ExternalClauseState::SawImplicitLanguageTarget
                    | ExternalClauseState::AwaitingLanguageTargetImplicit
            )
        })
    }

    fn should_split_before_external_statement_head(&self, token_upper: &str) -> bool {
        if self.block_depth() != 1 || self.paren_depth != 0 {
            return false;
        }

        if !sql_text::is_statement_head_keyword(token_upper)
            || sql_text::is_external_language_clause_keyword(token_upper)
        {
            return false;
        }

        if self.should_split_on_semicolon() {
            return true;
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

    fn line_boundary_action(
        &self,
        marker: LineLeadingMarker,
        current_is_empty: bool,
    ) -> LineBoundaryAction {
        if marker == LineLeadingMarker::None || !self.token.is_empty() {
            return LineBoundaryAction::None;
        }

        if self.in_with_plsql_declaration()
            && self.with_clause_waiting_main_query()
            && self.block_depth() == 0
            && self.paren_depth == 0
        {
            return match marker {
                LineLeadingMarker::RunScriptAt
                | LineLeadingMarker::BangHost
                | LineLeadingMarker::Slash(_) => LineBoundaryAction::SplitBeforeLine,
                LineLeadingMarker::None | LineLeadingMarker::OpenParen => LineBoundaryAction::None,
            };
        }

        if self.pending_implicit_external_top_level_split
            && self.block_depth() == 1
            && self.paren_depth == 0
        {
            return match marker {
                LineLeadingMarker::RunScriptAt
                | LineLeadingMarker::BangHost
                | LineLeadingMarker::OpenParen => LineBoundaryAction::SplitBeforeLine,
                LineLeadingMarker::Slash(kind) => {
                    if kind.consumes_as_terminator() {
                        LineBoundaryAction::SplitAndConsumeLine
                    } else {
                        LineBoundaryAction::SplitBeforeLine
                    }
                }
                LineLeadingMarker::None => LineBoundaryAction::None,
            };
        }

        if let LineLeadingMarker::Slash(kind) = marker {
            if self.block_depth() == 1 && self.paren_depth == 0 && self.should_split_on_semicolon()
            {
                return if kind.consumes_as_terminator() {
                    LineBoundaryAction::SplitAndConsumeLine
                } else {
                    LineBoundaryAction::SplitBeforeLine
                };
            }

            if self.in_create_plsql() && self.block_depth() == 0 && self.paren_depth == 0 {
                return if kind.consumes_as_terminator() {
                    LineBoundaryAction::SplitAndConsumeLine
                } else {
                    LineBoundaryAction::SplitBeforeLine
                };
            }

            if current_is_empty
                && self.is_idle()
                && self.block_depth() == 0
                && self.paren_depth == 0
                && kind.consumes_as_terminator()
            {
                return LineBoundaryAction::ConsumeLine;
            }
        }

        LineBoundaryAction::None
    }

    pub(crate) fn line_boundary_action_for_line(
        &self,
        line: &str,
        current_is_empty: bool,
    ) -> LineBoundaryAction {
        let action = self.line_boundary_action(classify_line_leading_marker(line), current_is_empty);
        if action != LineBoundaryAction::None {
            return action;
        }

        if self.is_idle()
            && self.in_wrapped_create()
            && !current_is_empty
            && self.block_depth() == 0
            && self.paren_depth == 0
        {
            let first_word = line.split_whitespace().next();
            if first_word.is_some_and(sql_text::is_statement_head_keyword)
                && !first_word.is_some_and(|word| word.eq_ignore_ascii_case("BEGIN"))
            {
                return LineBoundaryAction::SplitBeforeLine;
            }
        }

        LineBoundaryAction::None
    }

    pub(crate) fn splitter_line_boundary_action_for_line(
        &self,
        line: &str,
        current_is_empty: bool,
    ) -> LineBoundaryAction {
        let action = self.line_boundary_action_for_line(line, current_is_empty);
        if action != LineBoundaryAction::None {
            return action;
        }

        let statement_head_action =
            self.splitter_statement_head_action_for_line(line, current_is_empty);
        if statement_head_action != LineBoundaryAction::None {
            return statement_head_action;
        }

        match classify_line_leading_marker(line) {
            LineLeadingMarker::Slash(kind)
                if self.is_idle() && self.can_terminate_on_slash() && kind.consumes_as_terminator() =>
            {
                if current_is_empty {
                    LineBoundaryAction::ConsumeLine
                } else {
                    LineBoundaryAction::SplitAndConsumeLine
                }
            }
            _ => LineBoundaryAction::None,
        }
    }

    fn splitter_statement_head_action_for_line(
        &self,
        line: &str,
        current_is_empty: bool,
    ) -> LineBoundaryAction {
        if !self.is_idle() || !self.in_create_plsql() || current_is_empty || self.is_trigger() {
            return LineBoundaryAction::None;
        }

        if !(self.can_terminate_on_slash() || self.pending_end == PendingEnd::End) {
            return LineBoundaryAction::None;
        }

        let first_word = line.split_whitespace().next();
        if first_word.is_some_and(|word| word.eq_ignore_ascii_case("BEGIN")) {
            return LineBoundaryAction::None;
        }

        let starts_statement_head = first_word.is_some_and(sql_text::is_statement_head_keyword);

        if starts_statement_head {
            LineBoundaryAction::SplitBeforeLine
        } else {
            LineBoundaryAction::None
        }
    }

    pub(crate) fn prepare_splitter_line_boundary(&mut self, line: &str) {
        let LineLeadingMarker::Slash(kind) = classify_line_leading_marker(line) else {
            return;
        };

        if !kind.consumes_as_terminator() || !self.is_idle() {
            return;
        }

        self.finalize_external_clause_on_semicolon();

        if self.pending_end == PendingEnd::End {
            self.resolve_pending_end_on_terminator();
        }
    }

    fn pop_case_block(&mut self) -> bool {
        if self.top_is_case() {
            let _ = self.block_stack.pop();
            return true;
        }

        false
    }

    fn package_body_init_end_context(&self) -> bool {
        self.create_plsql_kind == CreatePlsqlKind::PackageBody
            && self.block_depth() == 2
            && self.block_stack.last() == Some(&BlockKind::Begin)
            && self.block_stack.get(self.block_stack.len().saturating_sub(2)) == Some(&BlockKind::AsIs)
    }

    fn package_body_end_label_matches(&self, token_upper: &str) -> bool {
        let Some(package_name) = self.package_body_name.as_deref() else {
            return false;
        };

        if package_name == token_upper {
            return true;
        }

        token_upper
            .rsplit('.')
            .next()
            .is_some_and(|segment| segment == package_name)
    }

    fn push_pending_end_label_segment(&mut self, token_upper: &str) {
        if token_upper.is_empty() {
            return;
        }

        match self.pending_end_label_token.as_mut() {
            Some(label) if !label.is_empty() => {
                label.push('.');
                label.push_str(token_upper);
            }
            Some(label) => {
                label.push_str(token_upper);
            }
            None => {
                self.pending_end_label_token = Some(token_upper.to_string());
            }
        }
    }

    fn resolve_pending_end_with_policy(&mut self, policy: EndResolutionPolicy) {
        if self.pending_end != PendingEnd::End {
            return;
        }

        let pending_end_label = self.pending_end_label_token.clone().unwrap_or_default();
        self.resolve_plain_end(&pending_end_label);
        if policy == EndResolutionPolicy::ResetCreateStateWhenTopLevel
            && self.block_depth() == 0
            && !self.in_with_plsql_declaration()
        {
            self.reset_create_tracking_state();
        }
        self.pending_end = PendingEnd::None;
        self.pending_end_label_token = None;
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
            CreatePlsqlKind::PackageBody | CreatePlsqlKind::TypeBody
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
            self.handle_pending_end_on_token(upper, end_token_role.suffix());
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
    fn handle_pending_end_on_token(&mut self, token_upper: &str, suffix: Option<PendingEndSuffix>) {
        if self.pending_end != PendingEnd::End {
            return;
        }

        // Package body initialization blocks may close with `END <package_name>` where
        // `<package_name>` can itself be a keyword token (`IF`, `CASE`, ...). In that
        // context we must treat following identifier tokens as an END label segment,
        // not as END-suffix keywords.
        if self.package_body_init_end_context() {
            self.push_pending_end_label_segment(token_upper);
            return;
        }

        if let Some(suffix) = suffix {
            let resolved_by_suffix = suffix.apply_to_state(self);
            if !resolved_by_suffix {
                self.resolve_plain_end(token_upper);
            }
            self.pending_end = PendingEnd::None;
            self.pending_end_label_token = None;
            return;
        }

        // Plain END – CASE expression or PL/SQL block
        self.resolve_plain_end(token_upper);
        self.pending_end = PendingEnd::None;
        self.pending_end_label_token = None;
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

    /// Pop the topmost block of the given kind when it matches exactly.
    fn pop_block_of_kind(&mut self, kind: BlockKind) -> bool {
        if self.block_stack.last() == Some(&kind) {
            let _ = self.block_stack.pop();
            return true;
        }

        false
    }

    fn pop_top_matching_block(&mut self, kinds: &[BlockKind]) -> bool {
        if self
            .block_stack
            .last()
            .is_some_and(|kind| kinds.iter().any(|candidate| kind == candidate))
        {
            let _ = self.block_stack.pop();
            return true;
        }

        false
    }

    fn pop_timing_point_block(&mut self) -> bool {
        if let Some(pos) = self
            .block_stack
            .iter()
            .rposition(|kind| *kind == BlockKind::TimingPoint)
        {
            self.block_stack.truncate(pos);
            true
        } else {
            false
        }
    }

    /// Plain END (not END CASE/IF/LOOP/WHILE/REPEAT/timing).
    /// If top is Case, treat as CASE expression end. Otherwise pop a PL/SQL block.
    fn resolve_plain_end(&mut self, token_upper: &str) {
        let top = self.block_stack.last().copied();
        let _ = self.block_stack.pop();

        if top == Some(BlockKind::Declare)
            && self.block_stack.last() == Some(&BlockKind::AsIs)
            && self.pending_subprogram_begins > 0
        {
            let _ = self.block_stack.pop();
            self.pending_subprogram_begins -= 1;
            return;
        }

        if top == Some(BlockKind::Begin) && self.block_stack.last() == Some(&BlockKind::AsIs) {
            let as_is_depth = self
                .block_stack
                .iter()
                .filter(|kind| **kind == BlockKind::AsIs)
                .count();

            let should_close_as_is = match self.create_plsql_kind {
                CreatePlsqlKind::PackageBody => {
                    if as_is_depth > 1 {
                        true
                    } else {
                        let matches_named_end = self.package_body_end_label_matches(token_upper);
                        let unlabeled_end = token_upper.is_empty();
                        matches_named_end || unlabeled_end
                    }
                }
                _ => true,
            };

            if should_close_as_is {
                let _ = self.block_stack.pop();
            }
        }
    }

    pub(crate) fn plain_end_closes_parent_scope(&self, token_upper: &str) -> bool {
        let top = self.block_stack.last().copied();

        if top == Some(BlockKind::Declare)
            && self.block_stack.iter().rev().nth(1) == Some(&BlockKind::AsIs)
            && self.pending_subprogram_begins > 0
        {
            return true;
        }

        if top == Some(BlockKind::Begin)
            && self.block_stack.iter().rev().nth(1) == Some(&BlockKind::AsIs)
            && self.create_plsql_kind == CreatePlsqlKind::PackageBody
            && self.block_depth() <= 2
        {
            let matches_named_end = self.package_body_end_label_matches(token_upper);
            let unlabeled_end = token_upper.is_empty();
            return matches_named_end || unlabeled_end;
        }

        false
    }

    pub(crate) fn resolve_pending_end_on_separator(&mut self) {
        self.resolve_pending_end_with_policy(EndResolutionPolicy::KeepCreateState);
    }

    pub(crate) fn resolve_pending_end_on_separator_with_token(&mut self, token_upper: &str) {
        if self.pending_end != PendingEnd::End {
            return;
        }

        if self.package_body_init_end_context() {
            self.push_pending_end_label_segment(token_upper);
            return;
        }

        self.resolve_plain_end(token_upper);
        self.pending_end = PendingEnd::None;
        self.pending_end_label_token = None;
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

    fn observe_external_clause_quoted_identifier_target(&mut self) {
        let should_track = self.block_depth() > 1
            || matches!(
                self.create_plsql_kind,
                CreatePlsqlKind::Procedure | CreatePlsqlKind::Function
            );

        if !should_track {
            return;
        }

        if let Some(frame) = self.active_routine_frame_mut() {
            frame.observe_external_clause_quoted_identifier_target();
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

    fn reset_statement_boundary_state(&mut self) {
        self.pending_end = PendingEnd::None;
        self.pending_end_label_token = None;
        self.pending_do = PendingDo::None;
        self.if_state = IfState::None;
        self.paren_depth = 0;
        self.with_clause_state = WithClauseState::None;
        self.top_level_token_state = TopLevelTokenState::NoneSeen;
        self.pending_implicit_external_top_level_split = false;
    }

    pub(crate) fn reset_create_tracking_state(&mut self) {
        self.create_plsql_kind = CreatePlsqlKind::None;
        self.create_state = CreateState::None;
        self.package_body_name = None;
        self.awaiting_package_body_name = false;
        self.as_is_follow_state = AsIsFollowState::None;
        self.begin_state = BeginState::None;
        self.as_is_state = AsIsState::None;
        self.pending_subprogram_begins = 0;
        self.pending_sql_macro_call_spec = false;
        self.routine_is_stack.clear();
        self.timing_point_state = TimingPointState::None;
        self.saw_compound_keyword = false;
        self.saw_trigger_alias_subject = false;
    }

    pub(crate) fn reset_after_statement_boundary(&mut self) {
        self.reset_statement_boundary_state();
        self.reset_create_tracking_state();
    }

    pub(crate) fn reset_create_state(&mut self) {
        self.reset_after_statement_boundary();
    }

    /// Reset all state to idle for force-terminate scenarios.
    pub(crate) fn force_reset_all(&mut self) {
        self.flush_token();
        self.resolve_pending_end_on_eof();
        self.reset_after_statement_boundary();
        self.lex_mode = LexMode::Idle;
        self.token.clear();
        self.quoted_identifier_buf.clear();
        self.block_stack.clear();
    }

    pub(crate) fn begin_quoted_identifier(&mut self) {
        self.quoted_identifier_buf.clear();
    }

    pub(crate) fn push_quoted_identifier_char(&mut self, ch: char) {
        self.quoted_identifier_buf.push(ch);
    }

    pub(crate) fn finish_quoted_identifier(&mut self) -> Option<String> {
        if self.quoted_identifier_buf.is_empty() {
            return None;
        }

        let mut upper = std::mem::take(&mut self.quoted_identifier_buf);
        upper.make_ascii_uppercase();
        self.observe_quoted_identifier(&upper);
        Some(upper)
    }

    fn observe_quoted_identifier(&mut self, upper: &str) {
        if self.block_depth() == 0
            && self.create_plsql_kind == CreatePlsqlKind::PackageBody
            && self.awaiting_package_body_name
        {
            self.package_body_name = Some(upper.to_string());
        }
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
            if self.block_depth() == 0
                && self.create_plsql_kind == CreatePlsqlKind::Package
                && upper == "BODY"
            {
                self.create_plsql_kind = CreatePlsqlKind::PackageBody;
                self.package_body_name = None;
                self.awaiting_package_body_name = true;
                return;
            }

            if self.block_depth() == 0
                && self.create_plsql_kind == CreatePlsqlKind::PackageBody
                && self.awaiting_package_body_name
            {
                if matches!(upper, "AS" | "IS") {
                    self.awaiting_package_body_name = false;
                } else {
                    self.package_body_name = Some(upper.to_string());
                }
            }

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
