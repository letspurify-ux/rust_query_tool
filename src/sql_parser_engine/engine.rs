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

#[inline]
fn is_valid_q_quote_delimiter(delimiter: char) -> bool {
    !delimiter.is_whitespace() && delimiter != '\''
}

#[inline]
fn is_external_language_target(token_upper: &str) -> bool {
    sql_text::is_external_language_target_keyword(token_upper)
}

pub(crate) fn classify_line_leading_slash_marker(line: &str) -> Option<SlashLineKind> {
    let trimmed = line.trim_start();
    let rest = trimmed.strip_prefix('/')?;
    let mut rest = rest.trim_start();

    if rest.is_empty() {
        return Some(SlashLineKind::PureTerminator);
    }

    if rest.starts_with("--") {
        return Some(SlashLineKind::LineComment);
    }

    if sql_text::is_sqlplus_remark_comment_line(rest) {
        return Some(SlashLineKind::SqlPlusRemark);
    }

    let mut saw_block_comment = false;
    while let Some(after_block_comment) = rest.strip_prefix("/*") {
        let comment_end = after_block_comment.find("*/")?;
        rest = after_block_comment[comment_end + 2..].trim_start();
        saw_block_comment = true;

        if rest.is_empty() {
            return Some(SlashLineKind::BlockComment);
        }

        if rest.starts_with("--") || sql_text::is_sqlplus_remark_comment_line(rest) {
            return Some(SlashLineKind::PureTerminator);
        }
    }

    if saw_block_comment && rest.is_empty() {
        Some(SlashLineKind::BlockComment)
    } else {
        None
    }
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

    fn push_current_statement(&mut self) {
        let trimmed = self.current.trim();
        if !trimmed.is_empty() {
            self.statements.push(trimmed.to_string());
        }
        self.current.clear();
    }

    fn finish_current_statement(&mut self) {
        self.push_current_statement();
        self.state.reset_after_statement_boundary();
    }

    fn apply_semicolon_action(&mut self, action: SemicolonAction, semicolon: char) {
        match action {
            SemicolonAction::AppendToCurrent => {
                self.current.push(semicolon);
            }
            SemicolonAction::SplitTopLevel => {
                self.finish_current_statement();
            }
            SemicolonAction::SplitForcedRoutine => {
                self.finish_current_statement();
                self.state.block_stack.clear();
            }
            SemicolonAction::CloseRoutineBlock => {
                self.current.push(semicolon);
                self.state.close_external_routine_on_semicolon();
            }
        }
    }

    fn split_current_statement(&mut self) {
        self.finish_current_statement();
    }

    fn split_current_and_reset_external_boundary(&mut self) {
        self.split_current_statement();
        self.state.block_stack.clear();
    }

    fn apply_line_boundary_action(&mut self, action: LineBoundaryAction) -> bool {
        match action {
            LineBoundaryAction::None => false,
            LineBoundaryAction::SplitBeforeLine => {
                self.split_current_statement();
                false
            }
            LineBoundaryAction::SplitAndConsumeLine => {
                self.split_current_statement();
                true
            }
            LineBoundaryAction::ConsumeLine => true,
        }
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
                    && self.state.paren_depth == 0)
                || (self.state.pending_end_top_level_split
                    && !self.state.in_with_plsql_declaration()
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
                    .should_split_before_external_begin_block(candidate_upper)
                {
                    this.split_current_and_reset_external_boundary();
                } else if this
                    .state
                    .should_split_before_external_statement_head(candidate_upper)
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
                    WithClauseState::InPlsqlDeclaration(
                        WithDeclarationState::CollectingDeclaration
                    )
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


            if this.state.pending_end_top_level_split
                && !this.state.in_with_plsql_declaration()
                && this.state.block_depth() == 0
                && this.state.paren_depth == 0
            {
                if sql_text::is_statement_head_keyword(candidate_upper) {
                    this.split_current_statement();
                }
                this.state.pending_end_top_level_split = false;
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
                            self.state.push_quoted_identifier_char('"');
                            i += 2;
                            continue;
                        }
                        self.state.lex_mode = LexMode::Idle;
                        if let Some(identifier_upper) = self.state.finish_quoted_identifier() {
                            self.state
                                .resolve_pending_end_on_separator_with_token(&identifier_upper);
                        } else if self.state.pending_end == PendingEnd::End {
                            self.state.resolve_pending_end_on_separator();
                        }
                    } else {
                        self.state.push_quoted_identifier_char(c);
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
                if self.state.pending_end == PendingEnd::End {
                    self.state.resolve_pending_end_on_separator();
                }
                if ((self.state.pending_implicit_external_top_level_split
                    && self.state.block_depth() == 1
                    && self.state.paren_depth == 0)
                    || (self.state.pending_end_top_level_split
                        && !self.state.in_with_plsql_declaration()
                        && self.state.block_depth() == 0
                        && self.state.paren_depth == 0))
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
                if self.state.pending_end == PendingEnd::End {
                    self.state.resolve_pending_end_on_separator();
                }
                if ((self.state.pending_implicit_external_top_level_split
                    && self.state.block_depth() == 1
                    && self.state.paren_depth == 0)
                    || (self.state.pending_end_top_level_split
                        && !self.state.in_with_plsql_declaration()
                        && self.state.block_depth() == 0
                        && self.state.paren_depth == 0))
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
                    let allow_implicit_target = self.state.allow_implicit_external_literal_target();
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
                self.state.begin_quoted_identifier();
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
        let mut on_symbol = on_symbol;
        let mut scratch_chars = std::mem::take(&mut self.scratch_chars);
        scratch_chars.clear();
        scratch_chars.extend(line.chars());
        scratch_chars.push('\n');

        let line_boundary_action = self
            .state
            .line_boundary_action_for_line(line, line_started_with_empty_current);
        if self.apply_line_boundary_action(line_boundary_action) {
            self.scratch_chars = scratch_chars;
            return;
        }

        let line_starts_at_statement_boundary = self.state.is_idle()
            && self.state.block_depth() == 0
            && self.state.paren_depth == 0
            && !self.state.in_with_plsql_declaration()
            && self.current.trim().is_empty();
        if line_starts_at_statement_boundary && sql_text::is_auto_terminated_tool_command(line) {
            self.current.push_str(line);
            self.current.push('\n');
            self.finish_current_statement();
            self.scratch_chars = scratch_chars;
            return;
        }

        self.process_chars_with_observer(&scratch_chars, &mut on_symbol);

        if (line_started_with_empty_current || line_started_in_with_waiting_main_query)
            && self.state.is_idle()
            && self.state.block_depth() == 0
            && self.state.paren_depth == 0
            && sql_text::is_auto_terminated_tool_command(line)
        {
            self.finish_current_statement();
        }

        self.scratch_chars = scratch_chars;
    }

    pub(crate) fn force_terminate(&mut self) {
        self.state.force_reset_all();
        self.finish_current_statement();
    }

    pub(crate) fn finalize(&mut self) {
        self.state.flush_token();
        self.state.resolve_pending_end_on_eof();
        self.finish_current_statement();
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
