use crate::db::session::{BindDataType, ComputeMode};
use crate::sql_parser_engine::{LineBoundaryAction, SqlParserEngine};
use crate::sql_text;

use super::{FormatItem, QueryExecutor, ScriptItem, ToolCommand};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum AutoFormatQueryRole {
    #[default]
    None,
    Base,
    Continuation,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum AutoFormatConditionRole {
    #[default]
    None,
    Header,
    Continuation,
    Closer,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum AutoFormatLineSemantic {
    #[default]
    None,
    Clause(AutoFormatClauseKind),
    JoinClause,
    JoinConditionClause,
    ConditionContinuation,
}

impl AutoFormatLineSemantic {
    fn from_analysis(
        clause_kind: Option<AutoFormatClauseKind>,
        query_role: AutoFormatQueryRole,
        is_join_clause: bool,
        is_join_condition_clause: bool,
        is_query_condition_continuation_clause: bool,
    ) -> Self {
        if query_role == AutoFormatQueryRole::Continuation && is_join_condition_clause {
            Self::JoinConditionClause
        } else if query_role == AutoFormatQueryRole::Continuation
            && is_query_condition_continuation_clause
        {
            Self::ConditionContinuation
        } else if query_role == AutoFormatQueryRole::Base && is_join_clause {
            Self::JoinClause
        } else if let Some(kind) = clause_kind {
            Self::Clause(kind)
        } else {
            Self::None
        }
    }

    pub(crate) fn is_clause(self) -> bool {
        matches!(self, Self::Clause(_))
    }

    pub(crate) fn is_join_clause(self) -> bool {
        matches!(self, Self::JoinClause)
    }

    pub(crate) fn is_join_condition_clause(self) -> bool {
        matches!(self, Self::JoinConditionClause)
    }

    pub(crate) fn is_condition_continuation(self) -> bool {
        matches!(self, Self::ConditionContinuation)
    }

    pub(crate) fn is_clause_boundary(self) -> bool {
        self.is_clause() || self.is_join_clause()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct AutoFormatLineContext {
    pub(crate) parser_depth: usize,
    pub(crate) auto_depth: usize,
    pub(crate) query_role: AutoFormatQueryRole,
    pub(crate) line_semantic: AutoFormatLineSemantic,
    pub(crate) query_base_depth: Option<usize>,
    pub(crate) starts_query_frame: bool,
    pub(crate) next_query_head_depth: Option<usize>,
    pub(crate) condition_header_line: Option<usize>,
    pub(crate) condition_header_depth: Option<usize>,
    pub(crate) condition_role: AutoFormatConditionRole,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AutoFormatConditionTerminator {
    Then,
    Loop,
}

impl AutoFormatConditionTerminator {
    fn matches_keyword(self, upper: &str) -> bool {
        matches!((self, upper), (Self::Then, "THEN") | (Self::Loop, "LOOP"))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingConditionHeader {
    header_line_idx: usize,
    header_depth: usize,
    terminator: AutoFormatConditionTerminator,
    requires_in_keyword: bool,
    saw_in_keyword: bool,
}

impl PendingConditionHeader {
    fn is_ready_for_open_paren(self) -> bool {
        !self.requires_in_keyword || self.saw_in_keyword
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ActiveConditionFrame {
    header_line_idx: usize,
    header_depth: usize,
    terminator: AutoFormatConditionTerminator,
    paren_depth: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct InlineCommentLineContinuation {
    depth: usize,
    query_base_depth: Option<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InlineCommentContinuationKind {
    SameDepth,
    OneDeeperThanQueryBase,
    OneDeeperThanCurrentDepth,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ConditionLineAnnotation {
    header_line_idx: Option<usize>,
    header_depth: Option<usize>,
    role: AutoFormatConditionRole,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AutoFormatClauseKind {
    With,
    Select,
    Insert,
    Update,
    Delete,
    Merge,
    Call,
    Values,
    Table,
    From,
    Where,
    Group,
    Having,
    Order,
    Connect,
    Start,
    Union,
    Intersect,
    Minus,
    Except,
    Set,
    Into,
    Offset,
    Fetch,
    Limit,
    Returning,
    Model,
    Window,
    MatchRecognize,
    Qualify,
    Pivot,
    Unpivot,
    Search,
    Cycle,
}

impl AutoFormatClauseKind {
    fn is_query_head(self) -> bool {
        matches!(
            self,
            Self::With
                | Self::Select
                | Self::Insert
                | Self::Update
                | Self::Delete
                | Self::Merge
                | Self::Call
                | Self::Values
                | Self::Table
        )
    }

    fn is_set_operator(self) -> bool {
        matches!(
            self,
            Self::Union | Self::Intersect | Self::Minus | Self::Except
        )
    }

    fn ends_into_continuation(self) -> bool {
        matches!(
            self,
            Self::From
                | Self::Where
                | Self::Group
                | Self::Having
                | Self::Order
                | Self::Connect
                | Self::Union
                | Self::Intersect
                | Self::Minus
                | Self::Except
        )
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct QueryBaseDepthFrame {
    query_base_depth: usize,
    close_align_depth: usize,
    start_parser_depth: usize,
    head_kind: Option<AutoFormatClauseKind>,
    pending_same_depth_set_operator_head: bool,
    into_continuation: bool,
    trailing_comma_continuation: bool,
    from_item_list_body_depth: Option<usize>,
    pending_from_item_body: bool,
    multitable_insert_branch_depth: usize,
    is_multitable_insert: bool,
    merge_branch_body_depth: Option<usize>,
    merge_branch_action: Option<MergeBranchAction>,
    pending_for_update_clause_update_line: bool,
    pending_join_condition_continuation: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TriggerHeaderDepthFrame {
    body_depth: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ForallBodyDepthFrame {
    owner_depth: usize,
}

#[derive(Clone, Copy, Debug)]
enum OwnerRelativeDepthFrameKind {
    ModelClause {
        start_parser_depth: usize,
    },
    MultilineClause {
        kind: sql_text::FormatIndentedParenOwnerKind,
        nested_paren_depth: usize,
    },
}

#[derive(Clone, Copy, Debug)]
struct OwnerRelativeDepthFrame {
    owner_depth: usize,
    kind: OwnerRelativeDepthFrameKind,
    pending_body_header_continuation: Option<sql_text::FormatBodyHeaderContinuationState>,
}

#[derive(Clone, Copy, Debug)]
struct PendingMultilineClauseOwnerFrame {
    kind: sql_text::FormatIndentedParenOwnerKind,
    owner_depth: usize,
}

#[derive(Clone, Copy, Debug)]
struct PendingPartialMultilineClauseOwnerFrame {
    kind: sql_text::PendingFormatIndentedParenOwnerHeaderKind,
    owner_depth: usize,
}

impl OwnerRelativeDepthFrame {
    fn model_clause(owner_depth: usize, start_parser_depth: usize) -> Self {
        Self {
            owner_depth,
            kind: OwnerRelativeDepthFrameKind::ModelClause { start_parser_depth },
            pending_body_header_continuation: None,
        }
    }

    fn multiline_clause(kind: sql_text::FormatIndentedParenOwnerKind, owner_depth: usize) -> Self {
        Self {
            owner_depth,
            kind: OwnerRelativeDepthFrameKind::MultilineClause {
                kind,
                nested_paren_depth: 1,
            },
            pending_body_header_continuation: None,
        }
    }

    fn body_depth(self) -> usize {
        match self.kind {
            OwnerRelativeDepthFrameKind::ModelClause { .. } => {
                sql_text::FormatIndentedParenOwnerKind::ModelSubclause.body_depth(self.owner_depth)
            }
            OwnerRelativeDepthFrameKind::MultilineClause { kind, .. } => {
                kind.body_depth(self.owner_depth)
            }
        }
    }

    fn owner_depth(self) -> usize {
        self.owner_depth
    }

    fn owner_kind(self) -> sql_text::FormatIndentedParenOwnerKind {
        match self.kind {
            OwnerRelativeDepthFrameKind::ModelClause { .. } => {
                sql_text::FormatIndentedParenOwnerKind::ModelSubclause
            }
            OwnerRelativeDepthFrameKind::MultilineClause { kind, .. } => kind,
        }
    }

    fn body_header_line_state(self, text_upper: &str) -> sql_text::FormatBodyHeaderLineState {
        self.owner_kind()
            .body_header_line_state(text_upper, self.pending_body_header_continuation)
    }

    fn note_body_header_line(&mut self, text_upper: &str) {
        self.pending_body_header_continuation = self
            .owner_kind()
            .body_header_line_state(text_upper, self.pending_body_header_continuation)
            .next_state;
    }

    fn note_multiline_paren_event(&mut self, event: sql_text::SignificantParenEvent) {
        if let OwnerRelativeDepthFrameKind::MultilineClause {
            nested_paren_depth, ..
        } = &mut self.kind
        {
            match event {
                sql_text::SignificantParenEvent::Open => {
                    *nested_paren_depth = nested_paren_depth.saturating_add(1);
                }
                sql_text::SignificantParenEvent::Close => {
                    *nested_paren_depth = nested_paren_depth.saturating_sub(1);
                }
            }
        }
    }

    fn is_closed_multiline_clause(self) -> bool {
        matches!(
            self.kind,
            OwnerRelativeDepthFrameKind::MultilineClause {
                nested_paren_depth: 0,
                ..
            }
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingQueryBaseFrame {
    owner_base_depth: usize,
    close_align_depth: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingSplitQueryOwnerFrame {
    owner_align_depth: usize,
    owner_base_depth: usize,
    next_query_head_depth: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingPartialQueryOwnerFrame {
    kind: sql_text::PendingFormatQueryOwnerHeaderKind,
    owner_align_depth: usize,
    owner_base_depth: usize,
    next_query_head_depth: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingPlsqlChildQueryOwnerFrame {
    kind: sql_text::PendingFormatPlsqlChildQueryOwnerHeaderKind,
    owner_align_depth: usize,
    owner_base_depth: usize,
    next_query_head_depth: usize,
    nested_paren_depth: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MergeBranchAction {
    Update,
    Delete,
    Insert,
}

// ── TopLevelScanner ─────────────────────────────────────────────────────────
//
// Byte-based SQL scanner that yields top-level tokens while skipping
// comments, string literals, and tracking parenthesis depth.
//
// This replaces 8+ near-identical scanning state machines that were
// duplicated across `find_top_level_keyword`, `has_top_level_identifier_token`,
// `find_first_top_level_comma`, `select_clause_has_top_level_aggregate`,
// `select_clause_has_top_level_analytic`, `is_single_table_from_clause`,
// and `select_clause_has_distinct_or_unique`.

/// Token yielded by [`TopLevelScanner`] at parenthesis depth 0.
enum ScanToken<'a> {
    /// An identifier or keyword at depth 0.
    Word { text: &'a str, start: usize },
    /// A non-structural symbol at depth 0 (e.g. `,`, `*`, `;`).
    Symbol { byte: u8, pos: usize },
}

/// Scans SQL text yielding only meaningful tokens at parenthesis depth 0,
/// automatically skipping comments (`--`, `/* */`), string literals (`'...'`),
/// quoted identifiers (`"..."`), and nested parenthesised expressions.
///
/// Uses byte-level iteration (no `Vec<(usize, char)>` allocation) since all
/// SQL delimiters and keywords are ASCII.
struct TopLevelScanner<'a> {
    sql: &'a str,
    bytes: &'a [u8],
    pos: usize,
    depth: usize,
}

impl<'a> TopLevelScanner<'a> {
    #[inline]
    fn new(sql: &'a str) -> Self {
        Self {
            sql,
            bytes: sql.as_bytes(),
            pos: 0,
            depth: 0,
        }
    }

    /// Peek at the next non-whitespace byte without consuming it.
    fn peek_next_non_ws_byte(&self) -> Option<u8> {
        let mut pos = self.pos;
        while pos < self.bytes.len() {
            if !self.bytes[pos].is_ascii_whitespace() {
                return Some(self.bytes[pos]);
            }
            pos += 1;
        }
        None
    }

    fn skip_single_quoted(&mut self) {
        self.pos += 1;
        while self.pos < self.bytes.len() {
            if self.bytes[self.pos] == b'\'' {
                self.pos += 1;
                if self.pos < self.bytes.len() && self.bytes[self.pos] == b'\'' {
                    self.pos += 1;
                    continue;
                }
                return;
            }
            self.pos += 1;
        }
    }

    fn skip_double_quoted(&mut self) {
        self.pos += 1;
        while self.pos < self.bytes.len() {
            if self.bytes[self.pos] == b'"' {
                self.pos += 1;
                if self.pos < self.bytes.len() && self.bytes[self.pos] == b'"' {
                    self.pos += 1;
                    continue;
                }
                return;
            }
            self.pos += 1;
        }
    }

    fn skip_q_quoted(&mut self, delimiter: u8, prefix_len: usize) {
        self.pos += prefix_len;
        let end_delimiter = sql_text::q_quote_closing_byte(delimiter);
        while self.pos + 1 < self.bytes.len() {
            if self.bytes[self.pos] == end_delimiter && self.bytes[self.pos + 1] == b'\'' {
                self.pos += 2;
                return;
            }
            self.pos += 1;
        }
        self.pos = self.bytes.len();
    }

    fn skip_line_comment(&mut self) {
        self.pos += 2;
        while self.pos < self.bytes.len() && self.bytes[self.pos] != b'\n' {
            self.pos += 1;
        }
    }

    fn skip_block_comment(&mut self) {
        self.pos += 2;
        while self.pos + 1 < self.bytes.len() {
            if self.bytes[self.pos] == b'*' && self.bytes[self.pos + 1] == b'/' {
                self.pos += 2;
                return;
            }
            self.pos += 1;
        }
        self.pos = self.bytes.len();
    }
}

impl<'a> Iterator for TopLevelScanner<'a> {
    type Item = ScanToken<'a>;

    fn next(&mut self) -> Option<ScanToken<'a>> {
        while self.pos < self.bytes.len() {
            let b = self.bytes[self.pos];

            if (b == b'n' || b == b'N' || b == b'u' || b == b'U')
                && self
                    .bytes
                    .get(self.pos + 1)
                    .is_some_and(|&next_b| next_b == b'q' || next_b == b'Q')
                && self.bytes.get(self.pos + 2) == Some(&b'\'')
            {
                if let Some(&delimiter) = self.bytes.get(self.pos + 3) {
                    if sql_text::is_valid_q_quote_delimiter_byte(delimiter) {
                        self.skip_q_quoted(delimiter, 4);
                        continue;
                    }
                }
            }

            if (b == b'q' || b == b'Q') && self.bytes.get(self.pos + 1) == Some(&b'\'') {
                if let Some(&delimiter) = self.bytes.get(self.pos + 2) {
                    if sql_text::is_valid_q_quote_delimiter_byte(delimiter) {
                        self.skip_q_quoted(delimiter, 3);
                        continue;
                    }
                }
            }

            if b == b'-' && self.bytes.get(self.pos + 1) == Some(&b'-') {
                self.skip_line_comment();
                continue;
            }
            if b == b'/' && self.bytes.get(self.pos + 1) == Some(&b'*') {
                self.skip_block_comment();
                continue;
            }
            if b == b'\'' {
                self.skip_single_quoted();
                continue;
            }
            if b == b'"' {
                self.skip_double_quoted();
                continue;
            }

            if b == b'(' {
                self.depth += 1;
                self.pos += 1;
                continue;
            }
            if b == b')' {
                self.depth = self.depth.saturating_sub(1);
                self.pos += 1;
                continue;
            }

            if b.is_ascii_whitespace() {
                self.pos += 1;
                continue;
            }

            // At depth > 0, skip tokens without yielding
            if self.depth > 0 {
                if sql_text::is_identifier_start_byte(b) {
                    self.pos += 1;
                    while self.pos < self.bytes.len()
                        && sql_text::is_identifier_byte(self.bytes[self.pos])
                    {
                        self.pos += 1;
                    }
                } else {
                    self.pos += 1;
                }
                continue;
            }

            // Depth 0: yield tokens
            if sql_text::is_identifier_start_byte(b) {
                let start = self.pos;
                self.pos += 1;
                while self.pos < self.bytes.len()
                    && sql_text::is_identifier_byte(self.bytes[self.pos])
                {
                    self.pos += 1;
                }
                let text = &self.sql[start..self.pos];
                return Some(ScanToken::Word { text, start });
            }

            let pos = self.pos;
            self.pos += 1;
            return Some(ScanToken::Symbol { byte: b, pos });
        }
        None
    }
}

impl QueryExecutor {
    pub fn line_block_depths(sql: &str) -> Vec<usize> {
        #[derive(Copy, Clone, Eq, PartialEq)]
        enum SubqueryParenKind {
            NonSubquery,
            Pending,
            Subquery,
        }

        #[derive(Copy, Clone, Default)]
        struct DepthComponents {
            block_depth: usize,
            query_paren_depth: usize,
            with_cte_depth: usize,
            case_branch_depth: usize,
            exception_handler_depth: usize,
        }

        impl DepthComponents {
            fn total(self) -> usize {
                self.block_depth
                    .saturating_add(self.query_paren_depth)
                    .saturating_add(self.with_cte_depth)
                    .saturating_add(self.case_branch_depth)
                    .saturating_add(self.exception_handler_depth)
            }
        }

        fn bytes_word_eq_ignore_ascii_case(
            bytes: &[u8],
            start: usize,
            end: usize,
            keyword: &str,
        ) -> bool {
            let span = end.saturating_sub(start);
            if span != keyword.len() {
                return false;
            }
            bytes[start..end]
                .iter()
                .zip(keyword.as_bytes())
                .all(|(left, right)| left.eq_ignore_ascii_case(right))
        }

        fn bytes_word_is_subquery_head_keyword(bytes: &[u8], start: usize, end: usize) -> bool {
            sql_text::SUBQUERY_HEAD_KEYWORDS
                .iter()
                .any(|keyword| bytes_word_eq_ignore_ascii_case(bytes, start, end, keyword))
        }

        fn leading_keyword_after_comments<'a>(
            line: &'a str,
            in_block_comment: &mut bool,
        ) -> Option<&'a str> {
            let trimmed = line.trim_start();
            if !*in_block_comment && sql_text::is_sqlplus_comment_line(trimmed) {
                return None;
            }

            let bytes = line.as_bytes();
            let mut i = 0usize;
            while i < bytes.len() {
                if *in_block_comment {
                    let mut closed = false;
                    while i + 1 < bytes.len() {
                        if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                            i += 2;
                            *in_block_comment = false;
                            closed = true;
                            break;
                        }
                        i += 1;
                    }
                    if !closed {
                        return None;
                    }
                    continue;
                }

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
                    *in_block_comment = true;
                    continue;
                }
                if sql_text::is_identifier_byte(b) {
                    let start = i;
                    i += 1;
                    while i < bytes.len() && sql_text::is_identifier_byte(bytes[i]) {
                        i += 1;
                    }

                    // `alias.column` at the beginning of a line (e.g. `if.a`) must not be
                    // interpreted as a control-flow keyword for indentation depth.
                    let mut lookahead = i;
                    while lookahead < bytes.len() && bytes[lookahead].is_ascii_whitespace() {
                        lookahead += 1;
                    }
                    if bytes.get(lookahead) == Some(&b'.') {
                        return None;
                    }

                    return line.get(start..i);
                }
                i += 1;
            }
            None
        }

        fn leading_close_paren_count(line: &str) -> usize {
            let bytes = line.as_bytes();
            let mut idx = 0usize;
            let mut close_count = 0usize;

            loop {
                while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                    idx += 1;
                }

                if idx + 1 < bytes.len() && bytes[idx] == b'/' && bytes[idx + 1] == b'*' {
                    idx += 2;
                    while idx + 1 < bytes.len() {
                        if bytes[idx] == b'*' && bytes[idx + 1] == b'/' {
                            idx += 2;
                            break;
                        }
                        idx += 1;
                    }
                    continue;
                }

                if idx + 1 < bytes.len() && bytes[idx] == b'-' && bytes[idx + 1] == b'-' {
                    return close_count;
                }

                if idx < bytes.len() && sql_text::is_identifier_start_byte(bytes[idx]) {
                    let start = idx;
                    idx += 1;
                    while idx < bytes.len() && sql_text::is_identifier_byte(bytes[idx]) {
                        idx += 1;
                    }
                    let token = &line[start..idx];
                    if token.eq_ignore_ascii_case("REM") || token.eq_ignore_ascii_case("REMARK") {
                        return close_count;
                    }
                    return close_count;
                }

                if idx < bytes.len() && bytes[idx] == b')' {
                    close_count += 1;
                    idx += 1;
                    continue;
                }

                return close_count;
            }
        }

        fn leading_subquery_close_paren_count(
            line: &str,
            subquery_paren_stack: &[SubqueryParenKind],
        ) -> usize {
            let close_count = leading_close_paren_count(line);
            if close_count == 0 || subquery_paren_stack.is_empty() {
                return 0;
            }

            let mut subquery_closes = 0usize;
            for kind in subquery_paren_stack.iter().rev().take(close_count) {
                if *kind == SubqueryParenKind::Subquery {
                    subquery_closes += 1;
                }
            }
            subquery_closes
        }

        fn skip_ws_and_comments_bytes(bytes: &[u8], mut idx: usize) -> usize {
            loop {
                while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                    idx += 1;
                }

                if idx + 1 < bytes.len() && bytes[idx] == b'/' && bytes[idx + 1] == b'*' {
                    idx += 2;
                    while idx + 1 < bytes.len() {
                        if bytes[idx] == b'*' && bytes[idx + 1] == b'/' {
                            idx += 2;
                            break;
                        }
                        idx += 1;
                    }
                    continue;
                }

                if idx + 1 < bytes.len() && bytes[idx] == b'-' && bytes[idx + 1] == b'-' {
                    idx += 2;
                    while idx < bytes.len() && bytes[idx] != b'\n' {
                        idx += 1;
                    }
                    continue;
                }

                // SQL*Plus comment command (REM/REMARK) can appear after
                // an opening parenthesis on the same line, and the nested
                // SELECT/WITH may start on the next line.
                if idx < bytes.len() && sql_text::is_identifier_byte(bytes[idx]) {
                    let start = idx;
                    while idx < bytes.len() && sql_text::is_identifier_byte(bytes[idx]) {
                        idx += 1;
                    }
                    if bytes_word_eq_ignore_ascii_case(bytes, start, idx, "REM")
                        || bytes_word_eq_ignore_ascii_case(bytes, start, idx, "REMARK")
                    {
                        while idx < bytes.len() && bytes[idx] != b'\n' {
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
            leading_word.eq_ignore_ascii_case("END")
                || leading_word.eq_ignore_ascii_case("ELSE")
                || leading_word.eq_ignore_ascii_case("ELSIF")
                || leading_word.eq_ignore_ascii_case("ELSEIF")
                || leading_word.eq_ignore_ascii_case("EXCEPTION")
        }

        fn is_end_suffix_keyword(leading_word: Option<&str>) -> bool {
            leading_word.is_some_and(|word| {
                matches!(
                    word,
                    _ if word.eq_ignore_ascii_case("CASE")
                        || word.eq_ignore_ascii_case("IF")
                        || word.eq_ignore_ascii_case("LOOP")
                        || word.eq_ignore_ascii_case("WHILE")
                        || word.eq_ignore_ascii_case("FOR")
                        || word.eq_ignore_ascii_case("BEFORE")
                        || word.eq_ignore_ascii_case("AFTER")
                        || word.eq_ignore_ascii_case("INSTEAD")
                        || word.eq_ignore_ascii_case("REPEAT")
                )
            })
        }

        fn is_non_label_control_keyword(leading_word: Option<&str>) -> bool {
            leading_word.is_some_and(|word| {
                matches!(
                    word,
                    _ if word.eq_ignore_ascii_case("END")
                        || word.eq_ignore_ascii_case("CASE")
                        || word.eq_ignore_ascii_case("IF")
                        || word.eq_ignore_ascii_case("LOOP")
                        || word.eq_ignore_ascii_case("WHILE")
                        || word.eq_ignore_ascii_case("FOR")
                        || word.eq_ignore_ascii_case("REPEAT")
                        || word.eq_ignore_ascii_case("ELSE")
                        || word.eq_ignore_ascii_case("ELSIF")
                        || word.eq_ignore_ascii_case("ELSEIF")
                        || word.eq_ignore_ascii_case("WHEN")
                        || word.eq_ignore_ascii_case("EXCEPTION")
                        || word.eq_ignore_ascii_case("BEGIN")
                        || word.eq_ignore_ascii_case("DECLARE")
                        || word.eq_ignore_ascii_case("THEN")
                        || word.eq_ignore_ascii_case("IS")
                        || word.eq_ignore_ascii_case("AS")
                )
            })
        }

        struct IdentifierChain {
            upper: String,
            is_line_tail: bool,
        }

        fn parse_identifier_chain(line: &str) -> Option<IdentifierChain> {
            let bytes = line.as_bytes();
            let mut i = 0usize;

            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }

            if i >= bytes.len() {
                return None;
            }

            let mut segments: Vec<String> = Vec::new();

            loop {
                while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                    i += 1;
                }
                if i >= bytes.len() || bytes[i] == b';' {
                    break;
                }
                if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
                    break;
                }

                if bytes[i] == b'"' {
                    i += 1;
                    let mut segment = String::new();
                    while i < bytes.len() {
                        let ch_opt = line.get(i..).and_then(|rest| rest.chars().next());
                        let ch = match ch_opt {
                            Some(ch) => ch,
                            None => break,
                        };

                        if ch == '"' {
                            let next_idx = i + ch.len_utf8();
                            let next_ch = line.get(next_idx..).and_then(|rest| rest.chars().next());
                            if next_ch == Some('"') {
                                segment.push('"');
                                i = next_idx + '"'.len_utf8();
                                continue;
                            }
                            i = next_idx;
                            break;
                        }

                        segment.push(ch);
                        i += ch.len_utf8();
                    }
                    if segment.is_empty() {
                        break;
                    }
                    segment.make_ascii_uppercase();
                    segments.push(segment);
                } else {
                    if !sql_text::is_identifier_start_byte(bytes[i]) {
                        break;
                    }
                    let start = i;
                    i += 1;
                    while i < bytes.len() && sql_text::is_identifier_byte(bytes[i]) {
                        i += 1;
                    }
                    segments.push(line[start..i].to_ascii_uppercase());
                }

                while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                    i += 1;
                }
                if i < bytes.len() && bytes[i] == b'.' {
                    i += 1;
                    continue;
                }
                break;
            }

            if segments.is_empty() {
                None
            } else {
                let chain_end = i;
                let rest = line.get(chain_end..).unwrap_or_default().trim_start();
                let is_line_tail = rest.is_empty()
                    || rest.starts_with(';')
                    || rest.starts_with("--")
                    || rest.starts_with("/*");
                Some(IdentifierChain {
                    upper: segments.join("."),
                    is_line_tail,
                })
            }
        }

        #[derive(Default)]
        struct EndSuffixOrLabel {
            upper: String,
            quoted_label: bool,
            leading_unquoted_segment: Option<String>,
            segment_count: usize,
        }

        fn parse_end_suffix_or_label(line: &str) -> Option<EndSuffixOrLabel> {
            let bytes = line.as_bytes();
            let mut i = 0usize;
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if i + 3 > bytes.len() || !bytes[i..i + 3].eq_ignore_ascii_case(b"END") {
                return None;
            }
            i += 3;

            let skip_ws_and_inline_comments = |bytes: &[u8], mut i: usize| loop {
                while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                    i += 1;
                }
                if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
                    return i;
                }
                if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
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
                return i;
            };

            i = skip_ws_and_inline_comments(bytes, i);
            if i >= bytes.len() || bytes[i] == b';' {
                return Some(EndSuffixOrLabel::default());
            }
            if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
                return None;
            }

            let mut segments: Vec<String> = Vec::new();
            let mut quoted_label = false;
            let mut leading_unquoted_segment: Option<String> = None;

            loop {
                i = skip_ws_and_inline_comments(bytes, i);
                if i >= bytes.len() || bytes[i] == b';' {
                    break;
                }
                if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
                    break;
                }

                if bytes[i] == b'"' {
                    quoted_label = true;
                    i += 1;
                    let mut segment = String::new();
                    while i < bytes.len() {
                        let ch_opt = line.get(i..).and_then(|rest| rest.chars().next());
                        let ch = match ch_opt {
                            Some(ch) => ch,
                            None => break,
                        };

                        if ch == '"' {
                            let next_idx = i + ch.len_utf8();
                            let next_ch = line.get(next_idx..).and_then(|rest| rest.chars().next());
                            if next_ch == Some('"') {
                                segment.push('"');
                                i = next_idx + '"'.len_utf8();
                                continue;
                            }
                            i = next_idx;
                            break;
                        }

                        segment.push(ch);
                        i += ch.len_utf8();
                    }
                    if segment.is_empty() {
                        break;
                    }
                    segment.make_ascii_uppercase();
                    segments.push(segment);
                } else {
                    if !sql_text::is_identifier_start_byte(bytes[i]) {
                        break;
                    }
                    let start = i;
                    i += 1;
                    while i < bytes.len() && sql_text::is_identifier_byte(bytes[i]) {
                        i += 1;
                    }
                    let upper = line[start..i].to_ascii_uppercase();
                    if leading_unquoted_segment.is_none() {
                        leading_unquoted_segment = Some(upper.clone());
                    }
                    segments.push(upper);
                }

                i = skip_ws_and_inline_comments(bytes, i);
                if i < bytes.len() && bytes[i] == b'.' {
                    i += 1;
                    continue;
                }
                break;
            }

            if segments.is_empty() {
                return Some(EndSuffixOrLabel::default());
            }

            Some(EndSuffixOrLabel {
                upper: segments.join("."),
                quoted_label,
                leading_unquoted_segment,
                segment_count: segments.len(),
            })
        }
        let is_with_main_query_keyword = sql_text::is_with_main_query_keyword;

        let mut builder = SqlParserEngine::new();
        let mut depths = Vec::new();

        // Extra indentation state for SQL formatting depth that should not affect splitting.
        let mut subquery_paren_depth = 0usize;
        let mut pending_subquery_paren = 0usize;
        let mut subquery_paren_stack: Vec<SubqueryParenKind> = Vec::new();
        let mut with_cte_depth = 0usize;
        let mut with_cte_paren_stack: Vec<isize> = Vec::new();
        let mut exception_depth_stack: Vec<usize> = Vec::new();
        let mut exception_handler_body_stack: Vec<bool> = Vec::new();
        let mut case_branch_stack: Vec<bool> = Vec::new();
        let mut in_leading_block_comment = false;

        for line in sql.lines() {
            let was_in_leading_block_comment = in_leading_block_comment;
            let leading_word = leading_keyword_after_comments(line, &mut in_leading_block_comment);
            let leading_identifier_chain = parse_identifier_chain(line);
            let pending_end_label_continuation =
                leading_identifier_chain
                    .as_ref()
                    .is_some_and(|identifier_chain| {
                        if !identifier_chain.is_line_tail {
                            return false;
                        }

                        identifier_chain.upper.contains('.')
                            || !is_non_label_control_keyword(leading_word)
                    });
            let leading_is =
                |keyword: &str| leading_word.is_some_and(|word| word.eq_ignore_ascii_case(keyword));
            let leading_is_any = |keywords: &[&str]| {
                leading_word.is_some_and(|word| {
                    keywords
                        .iter()
                        .any(|keyword| word.eq_ignore_ascii_case(keyword))
                })
            };

            let trimmed_start = line.trim_start();
            let in_leading_block_comment_line = leading_word.is_none()
                && (was_in_leading_block_comment || in_leading_block_comment);
            let is_comment_or_blank = trimmed_start.is_empty()
                || sql_text::is_sqlplus_comment_line(trimmed_start)
                || ((trimmed_start.starts_with("/*") || trimmed_start.starts_with("*/"))
                    && leading_word.is_none())
                || in_leading_block_comment_line;

            if pending_subquery_paren > 0 && !is_comment_or_blank {
                // WITH is also a valid subquery head (e.g. `( WITH cte AS (...) SELECT ... )`).
                // VALUES can head a nested query block in dialects that support table value
                // constructors in FROM/subquery positions.
                let promote_to_subquery = leading_is_any(sql_text::SUBQUERY_HEAD_KEYWORDS);
                if promote_to_subquery {
                    subquery_paren_depth =
                        subquery_paren_depth.saturating_add(pending_subquery_paren);
                }
                let mut unresolved = pending_subquery_paren;
                for paren_kind in subquery_paren_stack.iter_mut().rev() {
                    if unresolved == 0 {
                        break;
                    }
                    if *paren_kind == SubqueryParenKind::Pending {
                        *paren_kind = if promote_to_subquery {
                            SubqueryParenKind::Subquery
                        } else {
                            SubqueryParenKind::NonSubquery
                        };
                        unresolved -= 1;
                    }
                }
                pending_subquery_paren = 0;
            }

            // Eagerly resolve pending_end when the current line does NOT continue an
            // END CASE / END IF / END LOOP / END BEFORE / END AFTER / END INSTEAD sequence.
            // Without this, a bare "END" on its own line (e.g. CASE expression end)
            // leaves block_stack stale for the next line's depth calculation,
            // causing incorrect indentation for ELSE/WHEN that follow.
            {
                use crate::sql_parser_engine::PendingEnd;
                if builder.state.pending_end == PendingEnd::End
                    && !is_comment_or_blank
                    && !is_end_suffix_keyword(leading_word)
                    && !pending_end_label_continuation
                {
                    builder.state.resolve_pending_end_on_separator();
                }
            }

            let open_cases = builder.state.case_count();
            if case_branch_stack.len() < open_cases {
                case_branch_stack.resize(open_cases, false);
            } else if case_branch_stack.len() > open_cases {
                case_branch_stack.truncate(open_cases);
            }
            let innermost_case_depth = builder.state.innermost_case_depth();
            let at_case_header_level =
                innermost_case_depth.is_some_and(|depth| depth + 1 == builder.block_depth());
            let end_suffix_or_label = if leading_is("END") {
                parse_end_suffix_or_label(line)
            } else {
                None
            };
            let end_has_suffix = end_suffix_or_label.as_ref().is_some_and(|tail| {
                !tail.quoted_label
                    && tail.segment_count == 1
                    && is_end_suffix_keyword(tail.leading_unquoted_segment.as_deref())
            });
            let exception_end_line = exception_depth_stack
                .last()
                .is_some_and(|depth| *depth == builder.block_depth())
                && leading_is("END")
                && !end_has_suffix;

            let mut block_depth_component = if leading_word.is_some_and(should_pre_dedent) {
                builder.block_depth().saturating_sub(1)
            } else {
                builder.block_depth()
            };

            if leading_is("END")
                && !end_has_suffix
                && builder.state.plain_end_closes_parent_scope(
                    end_suffix_or_label
                        .as_ref()
                        .map_or("", |tail| tail.upper.as_str()),
                )
            {
                block_depth_component = block_depth_component.saturating_sub(1);
            }
            {
                use crate::sql_parser_engine::PendingEnd;
                if builder.state.pending_end == PendingEnd::End
                    && is_end_suffix_keyword(leading_word)
                {
                    block_depth_component = block_depth_component.saturating_sub(1);
                } else if builder.state.pending_end == PendingEnd::End
                    && pending_end_label_continuation
                {
                    let label_upper = leading_identifier_chain
                        .as_ref()
                        .map_or_else(String::new, |identifier_chain| {
                            identifier_chain.upper.clone()
                        });
                    let pop_count = builder
                        .state
                        .plain_end_scope_pop_count(label_upper.as_str());
                    if pop_count > 0 {
                        block_depth_component = block_depth_component.saturating_sub(pop_count);
                    }
                } else if builder.state.pending_end == PendingEnd::End {
                    let label_upper = leading_identifier_chain
                        .as_ref()
                        .map(|identifier_chain| identifier_chain.upper.clone())
                        .or_else(|| leading_word.map(|word| word.to_ascii_uppercase()))
                        .unwrap_or_default();
                    if builder
                        .state
                        .plain_end_closes_parent_scope(label_upper.as_str())
                    {
                        block_depth_component = block_depth_component.saturating_sub(1);
                    }
                }
            }

            if at_case_header_level && leading_is_any(&["WHEN", "ELSE"]) {
                block_depth_component = builder.block_depth();
            }

            if leading_is("BEGIN")
                && (builder.state.pending_subprogram_begins > 0
                    || builder.state.has_pending_declare_begin())
            {
                block_depth_component = block_depth_component.saturating_sub(1);
            }
            if builder.state.in_package_body_initializer_body()
                || (leading_is("BEGIN")
                    && builder.state.is_package_body_initializer_begin_context())
            {
                block_depth_component = block_depth_component.saturating_sub(1);
            }

            // Compute CASE branch indentation from the block_stack.
            let mut case_branch_indent = 0usize;
            {
                use crate::sql_parser_engine::BlockKind;
                let mut case_idx = 0usize;
                for (stack_idx, kind) in builder.state.block_stack.iter().enumerate() {
                    if *kind != BlockKind::Case {
                        continue;
                    }
                    if let Some(&branch_active) = case_branch_stack.get(case_idx) {
                        if branch_active {
                            let case_depth = stack_idx;
                            let is_header_line = builder.block_depth() == case_depth + 1
                                && leading_is_any(&["WHEN", "ELSE", "END"]);
                            if !is_header_line {
                                case_branch_indent += 1;
                            }
                        }
                    }
                    case_idx += 1;
                }
            }

            let leading_subquery_close_parens =
                leading_subquery_close_paren_count(line, &subquery_paren_stack);
            let query_paren_component = if subquery_paren_depth > 0 {
                subquery_paren_depth.saturating_sub(leading_subquery_close_parens)
            } else {
                0
            };

            let starts_with_main_query = leading_word.is_some_and(&is_with_main_query_keyword)
                && with_cte_paren_stack
                    .last()
                    .is_some_and(|paren_depth| *paren_depth <= 0);
            let with_cte_component = if starts_with_main_query {
                with_cte_depth.saturating_sub(1)
            } else {
                with_cte_depth
            };

            let in_exception_handler_body = exception_handler_body_stack
                .last()
                .copied()
                .unwrap_or(false);
            let exception_handler_component =
                if in_exception_handler_body && !leading_is("WHEN") && !exception_end_line {
                    1
                } else {
                    0
                };

            let depth = DepthComponents {
                block_depth: block_depth_component,
                query_paren_depth: query_paren_component,
                with_cte_depth: with_cte_component,
                case_branch_depth: case_branch_indent,
                exception_handler_depth: exception_handler_component,
            }
            .total();

            // No extra subprogram body depth: declarations and statements share the same level.

            depths.push(depth);

            // Keep WITH-clause indentation context separate from block depth.
            if leading_is("WITH") {
                with_cte_depth = with_cte_depth.saturating_add(1);
                with_cte_paren_stack.push(0);
            } else if starts_with_main_query && with_cte_depth > 0 {
                with_cte_depth = with_cte_depth.saturating_sub(1);
                with_cte_paren_stack.pop();
            }

            if leading_is("EXCEPTION") {
                exception_depth_stack.push(builder.block_depth());
                exception_handler_body_stack.push(false);
            } else if !exception_depth_stack.is_empty() && leading_is("WHEN") {
                if let Some(in_handler_body) = exception_handler_body_stack.last_mut() {
                    *in_handler_body = true;
                }
            } else if leading_is("END") {
                while exception_depth_stack
                    .last()
                    .is_some_and(|depth| *depth >= builder.block_depth())
                {
                    exception_depth_stack.pop();
                    exception_handler_body_stack.pop();
                }
            }
            if at_case_header_level {
                if leading_is_any(&["WHEN", "ELSE"]) {
                    if let Some(last) = case_branch_stack.last_mut() {
                        *last = true;
                    }
                } else if leading_is("END") {
                    if let Some(last) = case_branch_stack.last_mut() {
                        *last = false;
                    }
                }
            }

            builder.process_line_with_byte_observer(line, |bytes, byte_idx, symbol| {
                if symbol == b'(' {
                    let j = skip_ws_and_comments_bytes(bytes, byte_idx.saturating_add(1));
                    let mut k = j;
                    let mut paren_kind = SubqueryParenKind::NonSubquery;
                    while k < bytes.len() && (bytes[k].is_ascii_alphanumeric() || bytes[k] == b'_')
                    {
                        k += 1;
                    }
                    if k > j {
                        if bytes_word_is_subquery_head_keyword(bytes, j, k) {
                            subquery_paren_depth = subquery_paren_depth.saturating_add(1);
                            paren_kind = SubqueryParenKind::Subquery;
                        }
                    } else if j >= bytes.len()
                        || (bytes[j] == b'-' && j + 1 < bytes.len() && bytes[j + 1] == b'-')
                        || (bytes[j] == b'/' && j + 1 < bytes.len() && bytes[j + 1] == b'*')
                    {
                        pending_subquery_paren = pending_subquery_paren.saturating_add(1);
                        paren_kind = SubqueryParenKind::Pending;
                    }
                    subquery_paren_stack.push(paren_kind);
                    if with_cte_depth > 0 {
                        for paren_depth in &mut with_cte_paren_stack {
                            *paren_depth += 1;
                        }
                    }
                } else if symbol == b')' {
                    let closed_kind = subquery_paren_stack.pop();
                    if closed_kind == Some(SubqueryParenKind::Subquery) {
                        subquery_paren_depth = subquery_paren_depth.saturating_sub(1);
                    } else if closed_kind == Some(SubqueryParenKind::Pending) {
                        pending_subquery_paren = pending_subquery_paren.saturating_sub(1);
                    }
                    if with_cte_depth > 0 {
                        for paren_depth in &mut with_cte_paren_stack {
                            *paren_depth = paren_depth.saturating_sub(1);
                        }
                    }
                }
            });
        }

        depths
    }

    /// Returns line depths tailored for auto-format indentation.
    ///
    /// This builds on [`line_block_depths`] and folds in formatter-specific
    /// continuation depth while normalizing query base depth from parent
    /// query ancestry instead of context-specific formatter heuristics.
    pub(crate) fn auto_format_line_contexts(sql: &str) -> Vec<AutoFormatLineContext> {
        let parser_depths = Self::line_block_depths(sql);
        let lines: Vec<&str> = sql.lines().collect();
        if parser_depths.len() != lines.len() {
            return parser_depths
                .into_iter()
                .map(|depth| AutoFormatLineContext {
                    parser_depth: depth,
                    auto_depth: depth,
                    ..AutoFormatLineContext::default()
                })
                .collect();
        }

        let next_code_indices = Self::auto_format_next_code_line_indices(&lines);
        let mut contexts = Vec::with_capacity(lines.len());
        let mut query_frames: Vec<QueryBaseDepthFrame> = Vec::new();
        let mut pending_query_bases: Vec<PendingQueryBaseFrame> = Vec::new();
        let mut pending_split_query_owner: Option<PendingSplitQueryOwnerFrame> = None;
        let mut pending_partial_query_owner: Option<PendingPartialQueryOwnerFrame> = None;
        let mut pending_plsql_child_query_owner: Option<PendingPlsqlChildQueryOwnerFrame> = None;
        let mut in_block_comment = false;
        let mut non_query_into_continuation_depth: Option<usize> = None;
        let mut pending_condition_headers: Vec<PendingConditionHeader> = Vec::new();
        let mut active_condition_frames: Vec<ActiveConditionFrame> = Vec::new();
        let mut owner_relative_frames: Vec<OwnerRelativeDepthFrame> = Vec::new();
        let mut pending_multiline_clause_owner: Option<PendingMultilineClauseOwnerFrame> = None;
        let mut pending_partial_multiline_clause_owner: Option<
            PendingPartialMultilineClauseOwnerFrame,
        > = None;
        let mut pending_line_continuation: Option<InlineCommentLineContinuation> = None;
        let mut pending_inline_comment_line_continuation: Option<InlineCommentLineContinuation> =
            None;
        let mut trigger_header_frame: Option<TriggerHeaderDepthFrame> = None;
        let mut forall_body_frame: Option<ForallBodyDepthFrame> = None;

        for (idx, line) in lines.iter().enumerate() {
            let parser_depth = parser_depths.get(idx).copied().unwrap_or(0);
            let trimmed = line.trim_start();
            let mut context = AutoFormatLineContext {
                parser_depth,
                auto_depth: parser_depth,
                ..AutoFormatLineContext::default()
            };
            let mut current_line_is_join_clause = false;
            let mut current_line_is_join_condition_clause = false;
            let mut current_line_is_query_condition_continuation_clause = false;

            if trimmed.is_empty() {
                contexts.push(context);
                continue;
            }

            let was_in_block_comment = in_block_comment;
            if in_block_comment {
                sql_text::update_block_comment_state(trimmed, &mut in_block_comment);
                contexts.push(context);
                continue;
            }
            if trimmed.starts_with("/*") {
                sql_text::update_block_comment_state(trimmed, &mut in_block_comment);
                contexts.push(context);
                continue;
            }
            if trimmed.starts_with("--")
                || sql_text::is_sqlplus_comment_line(trimmed)
                || (was_in_block_comment && trimmed.starts_with("*/"))
            {
                contexts.push(context);
                continue;
            }

            let mut closing_query_close_align_depth = None;
            while query_frames
                .last()
                .is_some_and(|frame| parser_depth < frame.start_parser_depth)
            {
                if let Some(frame) = query_frames.pop() {
                    closing_query_close_align_depth = Some(frame.close_align_depth);
                }
            }

            let trimmed_upper = trimmed.to_ascii_uppercase();
            let line_has_leading_close_paren =
                sql_text::line_has_leading_significant_close_paren(trimmed);
            let leading_close_has_mixed_continuation = line_has_leading_close_paren
                && sql_text::line_has_mixed_leading_close_continuation(trimmed);
            let clause_detection_trimmed = if leading_close_has_mixed_continuation {
                sql_text::trim_after_leading_close_parens(trimmed)
            } else {
                trimmed
            };
            let clause_detection_upper = clause_detection_trimmed.to_ascii_uppercase();
            let is_trigger_header_begin = trigger_header_frame.is_some()
                && parser_depth == 0
                && sql_text::starts_with_keyword_token(&trimmed_upper, "BEGIN");
            let is_trigger_header_body_line = trigger_header_frame.is_some()
                && parser_depth == 0
                && !sql_text::starts_with_keyword_token(&trimmed_upper, "BEGIN")
                && !sql_text::starts_with_keyword_token(&trimmed_upper, "DECLARE")
                && !sql_text::starts_with_keyword_token(&trimmed_upper, "CREATE")
                && !sql_text::starts_with_keyword_token(&trimmed_upper, "END");
            let forall_body_depth =
                forall_body_frame.map(|frame| frame.owner_depth.saturating_add(1));
            let clause_kind = Self::auto_format_clause_kind(&clause_detection_upper);
            let split_query_owner_lookahead_kind =
                Self::split_query_owner_lookahead_kind(&lines, idx, &next_code_indices, trimmed);
            let current_line_is_generic_split_query_owner = matches!(
                split_query_owner_lookahead_kind,
                Some(sql_text::SplitQueryOwnerLookaheadKind::GenericExpression)
            );
            let current_line_is_direct_split_from_item_query_owner = matches!(
                split_query_owner_lookahead_kind,
                Some(sql_text::SplitQueryOwnerLookaheadKind::DirectFromItem)
            );
            Self::pop_expired_owner_relative_depth_frames(
                &mut owner_relative_frames,
                parser_depth,
                clause_kind,
                &trimmed_upper,
            );
            let active_frame = query_frames.last().copied();
            let active_line_continuation = pending_line_continuation.take();
            let active_inline_comment_line_continuation =
                pending_inline_comment_line_continuation.take();
            let next_code_trimmed = next_code_indices
                .get(idx)
                .copied()
                .flatten()
                .and_then(|next_idx| lines.get(next_idx).copied());
            let current_line_is_standalone_open_paren =
                Self::line_is_standalone_open_paren_before_inline_comment(trimmed);
            let blocks_structural_line_continuation =
                (Self::line_starts_continuation_boundary(trimmed)
                    || (leading_close_has_mixed_continuation
                        && Self::line_starts_continuation_boundary(clause_detection_trimmed)))
                    && !current_line_is_standalone_open_paren;
            let pending_split_query_owner_for_line = if current_line_is_standalone_open_paren {
                pending_split_query_owner.take()
            } else {
                None
            };
            let pending_multiline_clause_for_line = if current_line_is_standalone_open_paren {
                pending_multiline_clause_owner.take()
            } else {
                None
            };
            if current_line_is_standalone_open_paren {
                pending_partial_query_owner = None;
                pending_partial_multiline_clause_owner = None;
            }
            let multiline_clause_paren_profile = sql_text::significant_paren_profile(trimmed);
            let pending_plsql_child_query_owner_for_line = pending_plsql_child_query_owner;
            let pending_plsql_child_query_owner_nested_paren_depth_after_line =
                pending_plsql_child_query_owner_for_line.map(|frame| {
                    Self::pending_plsql_child_query_owner_nested_paren_depth_after_line(
                        frame.nested_paren_depth,
                        &multiline_clause_paren_profile,
                    )
                });
            let owner_relative_detection_trimmed =
                sql_text::trim_after_leading_close_parens(trimmed);
            let owner_relative_detection_upper =
                owner_relative_detection_trimmed.to_ascii_uppercase();
            let closes_multiline_clause_owner_depth =
                Self::consume_leading_multiline_clause_owner_relative_paren_closes(
                    &mut owner_relative_frames,
                    &multiline_clause_paren_profile,
                );
            let multiline_clause_owner_kind =
                Self::line_multiline_clause_owner_kind(owner_relative_detection_trimmed)
                    .or(pending_multiline_clause_for_line.map(|frame| frame.kind));
            let starts_multiline_clause = multiline_clause_owner_kind.is_some();
            let active_owner_relative_frame =
                Self::active_owner_relative_depth_frame(&owner_relative_frames);
            let owner_relative_body_header_line =
                active_owner_relative_frame.is_some_and(|frame| {
                    frame
                        .body_header_line_state(&owner_relative_detection_upper)
                        .is_header
                });
            if let Some(frame) = active_frame {
                context.query_base_depth = Some(frame.query_base_depth);
            }
            let starts_new_query_frame = clause_kind
                .filter(|kind| kind.is_query_head())
                .is_some_and(|head_kind| {
                    Self::line_starts_new_query_frame(
                        head_kind,
                        parser_depth,
                        active_frame,
                        !pending_query_bases.is_empty(),
                    )
                });

            if starts_new_query_frame {
                if let Some(frame) = query_frames.last_mut() {
                    frame.pending_same_depth_set_operator_head = false;
                }
                let parent_base_depth = pending_query_bases
                    .last()
                    .map(|frame| frame.owner_base_depth)
                    .or_else(|| forall_body_frame.map(|frame| frame.owner_depth))
                    .or_else(|| active_frame.map(|frame| frame.query_base_depth));
                let query_base_depth = parent_base_depth
                    .map(|depth| depth.saturating_add(1))
                    .unwrap_or(parser_depth);
                let close_align_depth = pending_query_bases
                    .last()
                    .map(|frame| frame.close_align_depth)
                    .unwrap_or_else(|| query_base_depth.saturating_sub(1));
                context.auto_depth = query_base_depth;
                context.query_role = AutoFormatQueryRole::Base;
                context.query_base_depth = Some(query_base_depth);
                context.starts_query_frame = true;
                pending_query_bases.clear();
                query_frames.push(QueryBaseDepthFrame {
                    query_base_depth,
                    close_align_depth,
                    start_parser_depth: parser_depth,
                    head_kind: clause_kind,
                    pending_same_depth_set_operator_head: false,
                    into_continuation: false,
                    trailing_comma_continuation: false,
                    from_item_list_body_depth: None,
                    pending_from_item_body: false,
                    multitable_insert_branch_depth: 0,
                    is_multitable_insert: Self::line_is_multitable_insert_header(&trimmed_upper),
                    merge_branch_body_depth: None,
                    merge_branch_action: None,
                    pending_for_update_clause_update_line: false,
                    pending_join_condition_continuation: false,
                });
            } else if let Some(frame) = query_frames.last().copied() {
                let reuses_active_query_base = clause_kind.is_some_and(|kind| {
                    !kind.is_query_head()
                        || parser_depth == frame.query_base_depth
                        || (frame.head_kind == Some(AutoFormatClauseKind::With)
                            && kind != AutoFormatClauseKind::With
                            && parser_depth == frame.start_parser_depth)
                        || (frame.pending_same_depth_set_operator_head
                            && parser_depth == frame.start_parser_depth)
                });
                let is_merge_using_clause = frame.head_kind == Some(AutoFormatClauseKind::Merge)
                    && Self::auto_format_is_merge_using_clause(&clause_detection_upper);
                let is_merge_on_clause = frame.head_kind == Some(AutoFormatClauseKind::Merge)
                    && Self::auto_format_is_merge_on_clause(&clause_detection_upper);
                let is_merge_branch_header = frame.head_kind == Some(AutoFormatClauseKind::Merge)
                    && Self::auto_format_is_merge_branch_header(&clause_detection_upper);
                let merge_branch_body_depth = frame
                    .merge_branch_body_depth
                    .unwrap_or_else(|| frame.query_base_depth.saturating_add(1));
                let is_merge_branch_action_clause = frame.head_kind
                    == Some(AutoFormatClauseKind::Merge)
                    && Self::merge_branch_action_from_clause_kind(clause_kind).is_some();
                let is_merge_branch_base_clause = frame.merge_branch_body_depth.is_some()
                    && matches!(
                        (frame.merge_branch_action, clause_kind),
                        (
                            Some(MergeBranchAction::Update),
                            Some(AutoFormatClauseKind::Set)
                        ) | (
                            Some(MergeBranchAction::Update),
                            Some(AutoFormatClauseKind::Where)
                        ) | (
                            Some(MergeBranchAction::Delete),
                            Some(AutoFormatClauseKind::Where)
                        ) | (
                            Some(MergeBranchAction::Insert),
                            Some(AutoFormatClauseKind::Into)
                        ) | (
                            Some(MergeBranchAction::Insert),
                            Some(AutoFormatClauseKind::Values)
                        ) | (
                            Some(MergeBranchAction::Insert),
                            Some(AutoFormatClauseKind::Where)
                        )
                    );
                let is_merge_branch_condition_clause = frame.merge_branch_body_depth.is_some()
                    && Self::auto_format_is_merge_branch_condition_clause(&clause_detection_upper);
                let is_merge_branch_dml = frame.head_kind == Some(AutoFormatClauseKind::Merge)
                    && matches!(
                        clause_kind,
                        Some(
                            AutoFormatClauseKind::Update
                                | AutoFormatClauseKind::Delete
                                | AutoFormatClauseKind::Insert
                        )
                    );
                let is_multitable_insert_branch_clause = frame.is_multitable_insert
                    && matches!(
                        clause_kind,
                        Some(AutoFormatClauseKind::Into | AutoFormatClauseKind::Values)
                    );
                let is_multitable_insert_branch_header = frame.is_multitable_insert
                    && (trimmed_upper.starts_with("WHEN ") || trimmed_upper.starts_with("ELSE"));
                let is_join_clause = Self::auto_format_is_join_clause(&clause_detection_upper);
                let is_join_condition_clause =
                    Self::auto_format_is_join_condition_clause(&clause_detection_upper);
                current_line_is_join_clause = is_join_clause;
                current_line_is_join_condition_clause = is_join_condition_clause;
                let from_item_list_body_depth = frame
                    .from_item_list_body_depth
                    .unwrap_or_else(|| frame.query_base_depth.saturating_add(1));
                let current_line_is_bare_direct_from_item_query_owner =
                    current_line_is_direct_split_from_item_query_owner
                        && Self::line_starts_with_bare_direct_from_item_query_owner(&trimmed_upper);
                let current_line_is_pending_from_item_body = frame.pending_from_item_body
                    && matches!(clause_kind, None | Some(AutoFormatClauseKind::Table))
                    && !sql_text::line_has_leading_significant_close_paren(trimmed);
                let is_query_condition_continuation_clause =
                    Self::auto_format_is_query_condition_continuation_clause(
                        &clause_detection_upper,
                    );
                current_line_is_query_condition_continuation_clause =
                    is_query_condition_continuation_clause;
                let is_for_update_clause = frame.head_kind == Some(AutoFormatClauseKind::Select)
                    && Self::auto_format_is_for_update_clause(&clause_detection_upper);
                let is_for_update_update_continuation = frame.pending_for_update_clause_update_line
                    && clause_kind == Some(AutoFormatClauseKind::Update);

                if frame.head_kind == Some(AutoFormatClauseKind::With)
                    && Self::line_is_cte_definition_header(trimmed)
                {
                    let cte_base_depth = frame.query_base_depth;
                    context.auto_depth = cte_base_depth;
                    context.query_role = AutoFormatQueryRole::Base;
                    context.query_base_depth = Some(cte_base_depth);
                } else if is_merge_using_clause || is_merge_on_clause || is_merge_branch_header {
                    context.auto_depth = frame.query_base_depth;
                    context.query_role = AutoFormatQueryRole::Base;
                    context.query_base_depth = Some(frame.query_base_depth);
                } else if is_merge_branch_action_clause || is_merge_branch_base_clause {
                    context.auto_depth = merge_branch_body_depth;
                    context.query_role = AutoFormatQueryRole::Base;
                    context.query_base_depth = Some(merge_branch_body_depth);
                } else if is_merge_branch_condition_clause {
                    context.auto_depth = merge_branch_body_depth.saturating_add(1);
                    context.query_role = AutoFormatQueryRole::Continuation;
                    context.query_base_depth = Some(merge_branch_body_depth);
                } else if is_join_clause
                    || is_for_update_clause
                    || is_for_update_update_continuation
                {
                    context.auto_depth = frame.query_base_depth;
                    context.query_role = AutoFormatQueryRole::Base;
                    context.query_base_depth = Some(frame.query_base_depth);
                } else if is_join_condition_clause || is_query_condition_continuation_clause {
                    context.auto_depth = if is_query_condition_continuation_clause
                        && frame.pending_join_condition_continuation
                    {
                        frame.query_base_depth.saturating_add(2)
                    } else {
                        frame.query_base_depth.saturating_add(1)
                    };
                    context.query_role = AutoFormatQueryRole::Continuation;
                    context.query_base_depth = Some(frame.query_base_depth);
                } else if current_line_is_pending_from_item_body {
                    context.auto_depth = from_item_list_body_depth;
                    context.query_role = AutoFormatQueryRole::Continuation;
                    context.query_base_depth = Some(frame.query_base_depth);
                } else if is_multitable_insert_branch_header {
                    context.auto_depth = frame.query_base_depth.saturating_add(1);
                } else if is_multitable_insert_branch_clause {
                    context.auto_depth = frame
                        .query_base_depth
                        .saturating_add(1)
                        .saturating_add(frame.multitable_insert_branch_depth);
                    context.query_role = AutoFormatQueryRole::Continuation;
                    context.query_base_depth = Some(frame.query_base_depth);
                } else if current_line_is_bare_direct_from_item_query_owner
                    && frame.trailing_comma_continuation
                {
                    context.auto_depth = from_item_list_body_depth;
                    context.query_role = AutoFormatQueryRole::Continuation;
                    context.query_base_depth = Some(frame.query_base_depth);
                } else if reuses_active_query_base && !is_merge_branch_dml {
                    if clause_kind.is_some() {
                        context.auto_depth = frame.query_base_depth;
                        context.query_role = AutoFormatQueryRole::Base;
                        context.query_base_depth = Some(frame.query_base_depth);
                    }
                } else if frame.into_continuation || frame.trailing_comma_continuation {
                    context.auto_depth = frame.query_base_depth.saturating_add(1);
                    context.query_role = AutoFormatQueryRole::Continuation;
                    context.query_base_depth = Some(frame.query_base_depth);
                }
            } else if let Some(into_depth) = non_query_into_continuation_depth {
                context.auto_depth = into_depth.saturating_add(1);
                context.query_role = AutoFormatQueryRole::Continuation;
            } else if let Some(body_depth) = forall_body_depth {
                if clause_kind.is_some_and(AutoFormatClauseKind::is_query_head) {
                    context.auto_depth = body_depth;
                }
            }

            if let Some(frame) = trigger_header_frame {
                if is_trigger_header_body_line {
                    context.auto_depth = frame.body_depth;
                } else if is_trigger_header_begin {
                    context.auto_depth = parser_depth;
                }
            }

            if clause_kind.is_none() && !blocks_structural_line_continuation {
                if let Some(continuation) = active_line_continuation {
                    context.auto_depth = context.auto_depth.max(continuation.depth);
                    context.query_role = AutoFormatQueryRole::Continuation;
                    context.query_base_depth =
                        context.query_base_depth.or(continuation.query_base_depth);
                }
            }

            if clause_kind.is_none() && !blocks_structural_line_continuation {
                if let Some(continuation) = active_inline_comment_line_continuation {
                    context.auto_depth = context.auto_depth.max(continuation.depth);
                    context.query_role = AutoFormatQueryRole::Continuation;
                    context.query_base_depth =
                        context.query_base_depth.or(continuation.query_base_depth);
                }
            }

            context.line_semantic = AutoFormatLineSemantic::from_analysis(
                clause_kind,
                context.query_role,
                current_line_is_join_clause,
                current_line_is_join_condition_clause,
                current_line_is_query_condition_continuation_clause,
            );

            if let Some(frame) = query_frames.last_mut() {
                if context
                    .query_base_depth
                    .is_some_and(|depth| depth == frame.query_base_depth)
                    || parser_depth >= frame.start_parser_depth
                {
                    if let Some(kind) = clause_kind {
                        frame.pending_same_depth_set_operator_head = kind.is_set_operator();
                        if kind == AutoFormatClauseKind::Into {
                            frame.into_continuation = true;
                        } else if kind.ends_into_continuation() {
                            frame.into_continuation = false;
                        }
                    }
                    frame.trailing_comma_continuation =
                        Self::line_ends_with_comma_before_inline_comment(trimmed)
                            && !sql_text::line_has_leading_significant_close_paren(trimmed);
                    if clause_kind == Some(AutoFormatClauseKind::From) {
                        frame.from_item_list_body_depth =
                            Some(frame.query_base_depth.saturating_add(1));
                        frame.pending_from_item_body =
                            Self::line_is_standalone_from_clause_header(&trimmed_upper);
                    } else if frame.pending_from_item_body {
                        frame.pending_from_item_body = false;
                    }
                    if frame.is_multitable_insert {
                        if trimmed_upper.starts_with("WHEN ") || trimmed_upper.starts_with("ELSE") {
                            frame.multitable_insert_branch_depth = 1;
                        } else if clause_kind == Some(AutoFormatClauseKind::Select) {
                            frame.multitable_insert_branch_depth = 0;
                        }
                    }
                    if frame.head_kind == Some(AutoFormatClauseKind::Merge) {
                        if Self::auto_format_is_merge_branch_header(&trimmed_upper) {
                            frame.merge_branch_body_depth =
                                Some(frame.query_base_depth.saturating_add(1));
                            frame.merge_branch_action = None;
                        } else if let Some(action) =
                            Self::merge_branch_action_from_clause_kind(clause_kind)
                        {
                            frame
                                .merge_branch_body_depth
                                .get_or_insert_with(|| frame.query_base_depth.saturating_add(1));
                            frame.merge_branch_action = Some(action);
                        }
                    }
                    if frame.head_kind == Some(AutoFormatClauseKind::Select) {
                        if Self::auto_format_is_for_update_split_header(&trimmed_upper) {
                            frame.pending_for_update_clause_update_line = true;
                        } else if (frame.pending_for_update_clause_update_line
                            && clause_kind == Some(AutoFormatClauseKind::Update))
                            || (!trimmed.starts_with("--")
                                && !sql_text::is_sqlplus_comment_line(trimmed))
                        {
                            frame.pending_for_update_clause_update_line = false;
                        }
                    }
                    if Self::auto_format_is_join_condition_clause(&clause_detection_upper) {
                        frame.pending_join_condition_continuation = true;
                    } else if Self::auto_format_is_join_clause(&clause_detection_upper)
                        || (!Self::auto_format_is_query_condition_continuation_clause(
                            &clause_detection_upper,
                        ) && clause_kind.is_some())
                    {
                        frame.pending_join_condition_continuation = false;
                    }
                }
            }

            if query_frames.is_empty() {
                if clause_kind == Some(AutoFormatClauseKind::Into) {
                    non_query_into_continuation_depth = Some(context.auto_depth);
                } else if non_query_into_continuation_depth.is_some() {
                    let continues_into_list =
                        Self::line_ends_with_comma_before_inline_comment(trimmed);
                    if !continues_into_list {
                        non_query_into_continuation_depth = None;
                    }
                }
            } else {
                non_query_into_continuation_depth = None;
            }

            if line_has_leading_close_paren && !leading_close_has_mixed_continuation {
                if let Some(close_align_depth) = closing_query_close_align_depth {
                    context.auto_depth = close_align_depth;
                }
            }

            if let Some(frame) = pending_split_query_owner_for_line {
                // A standalone `(` after a split owner such as `WHERE EXISTS`
                // or `FROM` remains part of that owner context, not the child
                // query yet. Keep the opener aligned with the completed
                // owner's own depth while still carrying the child-query base
                // separately through `next_query_head_depth`.
                context.auto_depth = frame.owner_align_depth;
            }

            if let Some(frame) = pending_multiline_clause_for_line {
                context.auto_depth = frame.owner_depth;
            }

            if let (Some(frame), Some(nested_paren_depth_after_line)) = (
                pending_plsql_child_query_owner_for_line,
                pending_plsql_child_query_owner_nested_paren_depth_after_line,
            ) {
                if let Some(owner_depth) = Self::pending_plsql_child_query_owner_alignment_depth(
                    frame,
                    trimmed,
                    nested_paren_depth_after_line,
                ) {
                    context.auto_depth = owner_depth;
                }
            }

            if let Some(frame) = active_owner_relative_frame {
                match frame.kind {
                    OwnerRelativeDepthFrameKind::ModelClause { .. } => {
                        if owner_relative_body_header_line {
                            let model_subclause_depth = frame.body_depth();
                            context.auto_depth = context.auto_depth.max(model_subclause_depth);
                            context.query_role = AutoFormatQueryRole::Continuation;
                            context.query_base_depth =
                                context.query_base_depth.or(Some(frame.owner_depth()));
                        }
                    }
                    OwnerRelativeDepthFrameKind::MultilineClause { .. } => {
                        if owner_relative_body_header_line {
                            context.auto_depth = context.auto_depth.max(frame.body_depth());
                        } else if let Some(owner_depth) = closes_multiline_clause_owner_depth {
                            context.auto_depth = owner_depth;
                        } else {
                            context.auto_depth = context.auto_depth.max(frame.body_depth());
                        }
                    }
                }
            } else if !owner_relative_body_header_line {
                if let Some(owner_depth) = closes_multiline_clause_owner_depth {
                    context.auto_depth = owner_depth;
                }
            }
            if starts_multiline_clause {
                if let Some(kind) = multiline_clause_owner_kind {
                    context.auto_depth = Self::auto_format_multiline_owner_depth(
                        kind,
                        context.auto_depth,
                        context.query_base_depth,
                    );
                }
            }

            let mut continued_partial_query_owner = None;
            let mut completed_partial_query_owner = None;
            let mut continued_plsql_child_query_owner = None;
            let mut completed_plsql_child_query_owner = None;
            let mut continued_multiline_clause_owner = None;
            let mut continued_partial_multiline_clause_owner = None;
            let mut completed_partial_multiline_clause_owner = None;
            let split_model_multiline_owner_tail =
                active_owner_relative_frame.is_some_and(|frame| {
                    matches!(frame.kind, OwnerRelativeDepthFrameKind::ModelClause { .. })
                }) && owner_relative_body_header_line
                    && next_code_indices
                        .get(idx)
                        .copied()
                        .flatten()
                        .is_some_and(|next_idx| {
                            Self::line_is_standalone_open_paren_before_inline_comment(
                                lines[next_idx],
                            )
                        })
                    && sql_text::starts_with_format_model_multiline_owner_tail(
                        &owner_relative_detection_upper,
                    );

            if let (Some(frame), Some(nested_paren_depth_after_line)) = (
                pending_plsql_child_query_owner_for_line,
                pending_plsql_child_query_owner_nested_paren_depth_after_line,
            ) {
                if frame.kind.line_can_continue(trimmed) {
                    if frame.kind.line_completes(trimmed) && nested_paren_depth_after_line == 0 {
                        completed_plsql_child_query_owner = Some(PendingSplitQueryOwnerFrame {
                            owner_align_depth: frame.owner_align_depth,
                            owner_base_depth: frame.owner_base_depth,
                            next_query_head_depth: frame.next_query_head_depth,
                        });
                    } else {
                        continued_plsql_child_query_owner =
                            Some(PendingPlsqlChildQueryOwnerFrame {
                                kind: frame.kind,
                                owner_align_depth: frame.owner_align_depth,
                                owner_base_depth: frame.owner_base_depth,
                                next_query_head_depth: frame.next_query_head_depth,
                                nested_paren_depth: nested_paren_depth_after_line,
                            });
                    }
                }
            }

            if !current_line_is_standalone_open_paren {
                if let Some(frame) = pending_partial_query_owner {
                    if frame.kind.line_can_continue(trimmed) {
                        // Split owner chains keep the original structural
                        // anchor; raw line indent must not become the new base.
                        let owner_align_depth = frame.owner_align_depth;
                        let owner_base_depth = frame.owner_base_depth;
                        let next_query_head_depth = frame
                            .next_query_head_depth
                            .max(owner_base_depth.saturating_add(1));
                        if owner_align_depth > context.auto_depth {
                            context.auto_depth = owner_align_depth;
                        }
                        if frame.kind.line_completes(trimmed) {
                            completed_partial_query_owner = Some(PendingSplitQueryOwnerFrame {
                                owner_align_depth,
                                owner_base_depth,
                                next_query_head_depth,
                            });
                        } else {
                            continued_partial_query_owner = Some(PendingPartialQueryOwnerFrame {
                                kind: frame.kind,
                                owner_align_depth,
                                owner_base_depth,
                                next_query_head_depth,
                            });
                        }
                    }
                }

                if let Some(frame) = pending_multiline_clause_owner {
                    if sql_text::format_indented_paren_owner_header_continues(frame.kind, trimmed) {
                        let owner_depth = frame.owner_depth;
                        if owner_depth > context.auto_depth {
                            context.auto_depth = owner_depth;
                        }
                        continued_multiline_clause_owner = Some(PendingMultilineClauseOwnerFrame {
                            kind: frame.kind,
                            owner_depth,
                        });
                    }
                } else if let Some(frame) = pending_partial_multiline_clause_owner {
                    if frame.kind.line_can_continue(trimmed) {
                        let owner_depth = frame.owner_depth;
                        if owner_depth > context.auto_depth {
                            context.auto_depth = owner_depth;
                        }
                        if frame.kind.line_completes(trimmed) {
                            completed_partial_multiline_clause_owner =
                                Some(PendingMultilineClauseOwnerFrame {
                                    kind: frame.kind.owner_kind(),
                                    owner_depth,
                                });
                        } else {
                            continued_partial_multiline_clause_owner =
                                Some(PendingPartialMultilineClauseOwnerFrame {
                                    kind: frame.kind,
                                    owner_depth,
                                });
                        }
                    }
                }
            }

            let condition_annotation = Self::annotate_parenthesized_condition_line(
                line,
                idx,
                context.auto_depth,
                owner_relative_frames.last().is_some_and(|frame| {
                    matches!(
                        frame.kind,
                        OwnerRelativeDepthFrameKind::MultilineClause { .. }
                    )
                }),
                &mut pending_condition_headers,
                &mut active_condition_frames,
            );
            context.condition_header_line = condition_annotation.header_line_idx;
            context.condition_header_depth = condition_annotation.header_depth;
            context.condition_role = condition_annotation.role;
            let leading_close_condition_continuation = context.condition_role
                == AutoFormatConditionRole::Continuation
                && sql_text::line_has_leading_significant_close_paren(line)
                && Self::auto_format_is_query_condition_continuation_clause(
                    &sql_text::trim_after_leading_close_parens(line).to_ascii_uppercase(),
                );
            if context.condition_role == AutoFormatConditionRole::Closer {
                if let Some(header_depth) = context.condition_header_depth {
                    context.auto_depth = header_depth;
                }
            } else if leading_close_condition_continuation {
                if let Some(header_depth) = context.condition_header_depth {
                    context.auto_depth = header_depth.saturating_add(1);
                }
            }
            if clause_kind.is_none()
                && !starts_multiline_clause
                && !owner_relative_body_header_line
                && !Self::line_ends_with_open_paren_before_inline_comment(trimmed)
            {
                if let Some(owner_kind) = sql_text::format_query_owner_header_kind(trimmed) {
                    if owner_kind == sql_text::FormatQueryOwnerKind::Condition {
                        if let Some(depth_floor) = owner_kind.header_depth_floor(
                            context.query_base_depth,
                            context.condition_header_depth,
                        ) {
                            context.auto_depth = context.auto_depth.max(depth_floor);
                        }
                    }
                }
            }
            if let Some(pending_kind) = sql_text::format_query_owner_pending_header_kind(trimmed) {
                context.auto_depth = pending_kind.normalized_current_line_depth(
                    context.auto_depth,
                    context.query_base_depth,
                    context.condition_header_depth,
                );
            }
            if current_line_is_direct_split_from_item_query_owner
                && !Self::line_starts_with_bare_direct_from_item_query_owner(&trimmed_upper)
            {
                if let Some(query_base_depth) = context.query_base_depth {
                    context.auto_depth = query_base_depth;
                    context.query_role = AutoFormatQueryRole::Base;
                }
            }

            let base_depth_for_child_query = pending_split_query_owner_for_line
                .map(|frame| frame.owner_base_depth)
                .or_else(|| completed_partial_query_owner.map(|frame| frame.owner_base_depth))
                .or_else(|| completed_plsql_child_query_owner.map(|frame| frame.owner_base_depth))
                .unwrap_or_else(|| Self::pending_query_owner_base_depth(context, &trimmed_upper));
            let next_query_head_depth = pending_split_query_owner_for_line
                .map(|frame| frame.next_query_head_depth)
                .or_else(|| completed_partial_query_owner.map(|frame| frame.next_query_head_depth))
                .or_else(|| {
                    completed_plsql_child_query_owner.map(|frame| frame.next_query_head_depth)
                })
                .unwrap_or_else(|| Self::next_query_head_depth(context, &trimmed_upper));
            let line_opens_child_query =
                Self::line_ends_with_open_paren_before_inline_comment(trimmed)
                    && continued_plsql_child_query_owner.is_none();
            let owns_next_query = Self::line_owns_next_query(&trimmed_upper)
                || current_line_is_generic_split_query_owner
                || Self::line_ends_with_then_before_inline_comment(trimmed)
                || Self::line_ends_with_keyword_before_inline_comment_owns_query(trimmed)
                || completed_partial_query_owner.is_some()
                || completed_plsql_child_query_owner.is_some()
                || line_opens_child_query;
            if owns_next_query {
                if query_frames.is_empty() && !starts_new_query_frame {
                    pending_query_bases.clear();
                }
                context.next_query_head_depth = Some(next_query_head_depth);
                pending_query_bases.push(PendingQueryBaseFrame {
                    owner_base_depth: base_depth_for_child_query,
                    close_align_depth: context.auto_depth,
                });
                if !Self::line_ends_with_open_paren_before_inline_comment(trimmed) {
                    pending_split_query_owner = Some(PendingSplitQueryOwnerFrame {
                        owner_align_depth: context.auto_depth,
                        owner_base_depth: base_depth_for_child_query,
                        next_query_head_depth,
                    });
                } else {
                    pending_split_query_owner = None;
                }
            } else if !starts_new_query_frame
                && !current_line_is_standalone_open_paren
                && !pending_query_bases.is_empty()
            {
                pending_query_bases.clear();
            }

            if !owns_next_query && !current_line_is_standalone_open_paren {
                pending_split_query_owner = None;
                pending_partial_query_owner = continued_partial_query_owner;
                if pending_split_query_owner.is_none() && pending_partial_query_owner.is_none() {
                    pending_partial_query_owner =
                        sql_text::format_query_owner_pending_header_kind(trimmed).map(|kind| {
                            let owner_base_depth = context.auto_depth;
                            PendingPartialQueryOwnerFrame {
                                kind,
                                owner_align_depth: owner_base_depth,
                                owner_base_depth,
                                next_query_head_depth: owner_base_depth.saturating_add(1),
                            }
                        });
                }
            } else {
                pending_partial_query_owner = None;
            }

            pending_plsql_child_query_owner =
                if completed_plsql_child_query_owner.is_some() || owns_next_query {
                    None
                } else if let Some(frame) = continued_plsql_child_query_owner {
                    Some(frame)
                } else {
                    sql_text::format_plsql_child_query_owner_pending_header_kind(trimmed).map(
                        |kind| PendingPlsqlChildQueryOwnerFrame {
                            kind,
                            owner_align_depth: context.auto_depth,
                            owner_base_depth: context.auto_depth,
                            next_query_head_depth: context.auto_depth.saturating_add(1),
                            nested_paren_depth:
                                Self::pending_plsql_child_query_owner_nested_paren_depth_after_line(
                                    0,
                                    &multiline_clause_paren_profile,
                                ),
                        },
                    )
                };

            if let Some(frame) = owner_relative_frames.last_mut() {
                frame.note_body_header_line(&owner_relative_detection_upper);
            }
            Self::apply_remaining_multiline_owner_relative_paren_profile(
                &mut owner_relative_frames,
                &multiline_clause_paren_profile,
            );
            if starts_multiline_clause {
                if let Some(kind) = multiline_clause_owner_kind {
                    owner_relative_frames.push(OwnerRelativeDepthFrame::multiline_clause(
                        kind,
                        pending_multiline_clause_for_line
                            .map(|frame| frame.owner_depth)
                            .unwrap_or_else(|| {
                                Self::auto_format_multiline_owner_depth(
                                    kind,
                                    context.auto_depth,
                                    context.query_base_depth,
                                )
                            }),
                    ));
                }
            }

            if starts_multiline_clause || current_line_is_standalone_open_paren {
                pending_multiline_clause_owner = None;
                pending_partial_multiline_clause_owner = None;
            } else {
                pending_multiline_clause_owner = continued_multiline_clause_owner
                    .or(completed_partial_multiline_clause_owner)
                    .or_else(|| {
                        Self::line_multiline_clause_owner_header_kind(trimmed).map(|kind| {
                            PendingMultilineClauseOwnerFrame {
                                kind,
                                owner_depth: Self::auto_format_multiline_owner_depth(
                                    kind,
                                    context.auto_depth,
                                    context.query_base_depth,
                                ),
                            }
                        })
                    })
                    .or_else(|| {
                        split_model_multiline_owner_tail.then_some(
                            PendingMultilineClauseOwnerFrame {
                                kind: sql_text::FormatIndentedParenOwnerKind::ModelSubclause,
                                owner_depth: context.auto_depth,
                            },
                        )
                    });
                pending_partial_multiline_clause_owner = if pending_multiline_clause_owner.is_some()
                {
                    None
                } else {
                    continued_partial_multiline_clause_owner.or_else(|| {
                        sql_text::format_indented_paren_pending_header_kind(trimmed).map(|kind| {
                            PendingPartialMultilineClauseOwnerFrame {
                                kind,
                                owner_depth: Self::auto_format_multiline_owner_depth(
                                    kind.owner_kind(),
                                    context.auto_depth,
                                    context.query_base_depth,
                                ),
                            }
                        })
                    })
                };
            }

            if sql_text::starts_with_keyword_token(&owner_relative_detection_upper, "MODEL") {
                owner_relative_frames.push(OwnerRelativeDepthFrame::model_clause(
                    context.auto_depth,
                    parser_depth,
                ));
            }

            pending_line_continuation =
                if context.query_base_depth.is_some() || clause_kind.is_some() {
                    Self::line_continuation_for_line(
                        trimmed,
                        context.auto_depth,
                        context.query_base_depth,
                        next_code_trimmed,
                    )
                } else {
                    None
                };
            pending_inline_comment_line_continuation =
                Self::inline_comment_line_continuation_for_line(
                    trimmed,
                    context.auto_depth,
                    context.query_base_depth,
                    next_code_trimmed,
                );

            if parser_depth == 0 && Self::is_create_trigger(trimmed) {
                trigger_header_frame = Some(TriggerHeaderDepthFrame { body_depth: 1 });
            }
            if is_trigger_header_begin {
                trigger_header_frame = None;
            }
            if sql_text::starts_with_keyword_token(&trimmed_upper, "FORALL") {
                forall_body_frame = Some(ForallBodyDepthFrame {
                    owner_depth: context.auto_depth,
                });
            }

            if trimmed.ends_with(';') {
                query_frames.pop();
                pending_query_bases.clear();
                pending_split_query_owner = None;
                pending_partial_query_owner = None;
                pending_plsql_child_query_owner = None;
                owner_relative_frames.clear();
                pending_multiline_clause_owner = None;
                pending_partial_multiline_clause_owner = None;
                pending_line_continuation = None;
                pending_inline_comment_line_continuation = None;
                trigger_header_frame = None;
                forall_body_frame = None;
            }

            contexts.push(context);
        }

        contexts
    }

    pub fn line_auto_format_depths(sql: &str) -> Vec<usize> {
        Self::auto_format_line_contexts(sql)
            .into_iter()
            .map(|context| context.auto_depth)
            .collect()
    }

    fn auto_format_clause_kind(trimmed_upper: &str) -> Option<AutoFormatClauseKind> {
        if sql_text::starts_with_keyword_token(trimmed_upper, "WITH") {
            Some(AutoFormatClauseKind::With)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "SELECT") {
            Some(AutoFormatClauseKind::Select)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "INSERT") {
            Some(AutoFormatClauseKind::Insert)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "UPDATE") {
            Some(AutoFormatClauseKind::Update)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "DELETE") {
            Some(AutoFormatClauseKind::Delete)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "MERGE") {
            Some(AutoFormatClauseKind::Merge)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "CALL") {
            Some(AutoFormatClauseKind::Call)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "VALUES") {
            Some(AutoFormatClauseKind::Values)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "TABLE") {
            Some(AutoFormatClauseKind::Table)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "FROM") {
            Some(AutoFormatClauseKind::From)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "WHERE") {
            Some(AutoFormatClauseKind::Where)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "GROUP") {
            Some(AutoFormatClauseKind::Group)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "HAVING") {
            Some(AutoFormatClauseKind::Having)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "ORDER") {
            Some(AutoFormatClauseKind::Order)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "CONNECT") {
            Some(AutoFormatClauseKind::Connect)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "START") {
            Some(AutoFormatClauseKind::Start)
        } else if let Some(set_operator) =
            sql_text::FormatSetOperatorKind::from_clause_start(trimmed_upper)
        {
            Some(match set_operator {
                sql_text::FormatSetOperatorKind::Union => AutoFormatClauseKind::Union,
                sql_text::FormatSetOperatorKind::Intersect => AutoFormatClauseKind::Intersect,
                sql_text::FormatSetOperatorKind::Minus => AutoFormatClauseKind::Minus,
                sql_text::FormatSetOperatorKind::Except => AutoFormatClauseKind::Except,
            })
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "SET") {
            Some(AutoFormatClauseKind::Set)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "INTO") {
            Some(AutoFormatClauseKind::Into)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "OFFSET") {
            Some(AutoFormatClauseKind::Offset)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "FETCH") {
            Some(AutoFormatClauseKind::Fetch)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "LIMIT") {
            Some(AutoFormatClauseKind::Limit)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "RETURNING") {
            Some(AutoFormatClauseKind::Returning)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "MODEL") {
            Some(AutoFormatClauseKind::Model)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "WINDOW") {
            Some(AutoFormatClauseKind::Window)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "MATCH_RECOGNIZE") {
            Some(AutoFormatClauseKind::MatchRecognize)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "QUALIFY") {
            Some(AutoFormatClauseKind::Qualify)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "PIVOT") {
            Some(AutoFormatClauseKind::Pivot)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "UNPIVOT") {
            Some(AutoFormatClauseKind::Unpivot)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "SEARCH") {
            Some(AutoFormatClauseKind::Search)
        } else if sql_text::starts_with_keyword_token(trimmed_upper, "CYCLE") {
            Some(AutoFormatClauseKind::Cycle)
        } else {
            None
        }
    }

    pub(crate) fn auto_format_is_join_clause(trimmed_upper: &str) -> bool {
        sql_text::starts_with_format_join_clause(trimmed_upper)
    }

    fn auto_format_is_join_condition_clause(trimmed_upper: &str) -> bool {
        sql_text::is_format_join_condition_clause(trimmed_upper)
    }

    fn auto_format_is_query_condition_continuation_clause(trimmed_upper: &str) -> bool {
        sql_text::starts_with_keyword_token(trimmed_upper, "AND")
            || sql_text::starts_with_keyword_token(trimmed_upper, "OR")
    }

    fn auto_format_is_for_update_clause(trimmed_upper: &str) -> bool {
        sql_text::starts_with_format_for_update_clause(trimmed_upper)
    }

    fn auto_format_is_for_update_split_header(trimmed_upper: &str) -> bool {
        sql_text::starts_with_format_for_update_split_header(trimmed_upper)
    }

    fn pending_condition_header_for_word(
        word_upper: &str,
        header_line_idx: usize,
        header_depth: usize,
    ) -> Option<PendingConditionHeader> {
        let (terminator, requires_in_keyword) = match word_upper {
            "IF" | "ELSIF" | "ELSEIF" | "WHEN" => (AutoFormatConditionTerminator::Then, false),
            "WHILE" => (AutoFormatConditionTerminator::Loop, false),
            "FOR" => (AutoFormatConditionTerminator::Loop, true),
            _ => return None,
        };

        Some(PendingConditionHeader {
            header_line_idx,
            header_depth,
            terminator,
            requires_in_keyword,
            saw_in_keyword: false,
        })
    }

    fn annotate_parenthesized_condition_line(
        line: &str,
        line_idx: usize,
        line_owner_depth: usize,
        in_multiline_clause: bool,
        pending_headers: &mut Vec<PendingConditionHeader>,
        active_frames: &mut Vec<ActiveConditionFrame>,
    ) -> ConditionLineAnnotation {
        let bytes = line.as_bytes();
        let mut idx = 0usize;
        let mut annotation =
            active_frames
                .last()
                .copied()
                .map_or_else(ConditionLineAnnotation::default, |frame| {
                    ConditionLineAnnotation {
                        header_line_idx: Some(frame.header_line_idx),
                        header_depth: Some(frame.header_depth),
                        ..ConditionLineAnnotation::default()
                    }
                });
        let mut saw_significant_token = false;
        let mut saw_leading_close_paren = false;

        while idx < bytes.len() {
            let byte = bytes[idx];

            if byte.is_ascii_whitespace() {
                idx += 1;
                continue;
            }

            if byte == b'-' && bytes.get(idx + 1) == Some(&b'-') {
                break;
            }

            if byte == b'/' && bytes.get(idx + 1) == Some(&b'*') {
                idx += 2;
                while idx + 1 < bytes.len() {
                    if bytes[idx] == b'*' && bytes[idx + 1] == b'/' {
                        idx += 2;
                        break;
                    }
                    idx += 1;
                }
                continue;
            }

            if (byte == b'q' || byte == b'Q') && bytes.get(idx + 1) == Some(&b'\'') {
                if let Some(&delimiter) = bytes.get(idx + 2) {
                    if sql_text::is_valid_q_quote_delimiter_byte(delimiter) {
                        idx += 3;
                        let closing = sql_text::q_quote_closing_byte(delimiter);
                        while idx + 1 < bytes.len() {
                            if bytes[idx] == closing && bytes[idx + 1] == b'\'' {
                                idx += 2;
                                break;
                            }
                            idx += 1;
                        }
                        continue;
                    }
                }
            }

            if (byte == b'n' || byte == b'N' || byte == b'u' || byte == b'U')
                && matches!(bytes.get(idx + 1), Some(b'q' | b'Q'))
                && bytes.get(idx + 2) == Some(&b'\'')
            {
                if let Some(&delimiter) = bytes.get(idx + 3) {
                    if sql_text::is_valid_q_quote_delimiter_byte(delimiter) {
                        idx += 4;
                        let closing = sql_text::q_quote_closing_byte(delimiter);
                        while idx + 1 < bytes.len() {
                            if bytes[idx] == closing && bytes[idx + 1] == b'\'' {
                                idx += 2;
                                break;
                            }
                            idx += 1;
                        }
                        continue;
                    }
                }
            }

            if byte == b'\'' {
                idx += 1;
                while idx < bytes.len() {
                    if bytes[idx] == b'\'' {
                        idx += 1;
                        if bytes.get(idx) == Some(&b'\'') {
                            idx += 1;
                            continue;
                        }
                        break;
                    }
                    idx += 1;
                }
                continue;
            }

            if byte == b'"' {
                idx += 1;
                while idx < bytes.len() {
                    if bytes[idx] == b'"' {
                        idx += 1;
                        if bytes.get(idx) == Some(&b'"') {
                            idx += 1;
                            continue;
                        }
                        break;
                    }
                    idx += 1;
                }
                continue;
            }

            if sql_text::is_identifier_start_byte(byte) {
                let start = idx;
                idx += 1;
                while idx < bytes.len() && sql_text::is_identifier_byte(bytes[idx]) {
                    idx += 1;
                }

                let is_leading_word = !saw_significant_token;
                if !saw_significant_token {
                    saw_significant_token = true;
                }

                let word_upper = line[start..idx].to_ascii_uppercase();

                if word_upper == "IN" {
                    if let Some(header) = pending_headers
                        .iter_mut()
                        .rev()
                        .find(|header| header.requires_in_keyword && !header.saw_in_keyword)
                    {
                        header.saw_in_keyword = true;
                    }
                }

                if active_frames.last().is_some_and(|frame| {
                    frame.paren_depth == 0 && frame.terminator.matches_keyword(&word_upper)
                }) {
                    if let Some(frame) = active_frames.pop() {
                        annotation.header_line_idx = Some(frame.header_line_idx);
                        annotation.header_depth = Some(frame.header_depth);
                    }
                }

                if pending_headers
                    .last()
                    .is_some_and(|header| header.terminator.matches_keyword(&word_upper))
                {
                    pending_headers.pop();
                }

                let should_track_header =
                    is_leading_word && !(in_multiline_clause && word_upper == "FOR");
                if should_track_header {
                    if let Some(header) = Self::pending_condition_header_for_word(
                        &word_upper,
                        line_idx,
                        line_owner_depth,
                    ) {
                        pending_headers.push(header);
                    }
                }

                continue;
            }

            if byte == b'(' {
                if !saw_significant_token {
                    saw_significant_token = true;
                }

                for frame in active_frames.iter_mut() {
                    frame.paren_depth = frame.paren_depth.saturating_add(1);
                }

                if let Some(header_idx) = pending_headers
                    .iter()
                    .rposition(|header| header.is_ready_for_open_paren())
                {
                    let header = pending_headers.remove(header_idx);
                    annotation.header_line_idx = Some(header.header_line_idx);
                    annotation.header_depth = Some(header.header_depth);
                    if header.header_line_idx == line_idx {
                        annotation.role = AutoFormatConditionRole::Header;
                    }
                    active_frames.push(ActiveConditionFrame {
                        header_line_idx: header.header_line_idx,
                        header_depth: header.header_depth,
                        terminator: header.terminator,
                        paren_depth: 1,
                    });
                } else if let Some(frame) = active_frames.last().copied() {
                    annotation.header_line_idx = Some(frame.header_line_idx);
                    annotation.header_depth = Some(frame.header_depth);
                }

                idx += 1;
                continue;
            }

            if byte == b')' {
                if !saw_significant_token {
                    saw_significant_token = true;
                    saw_leading_close_paren = true;
                }

                if let Some(frame) = active_frames.last().copied() {
                    annotation.header_line_idx = Some(frame.header_line_idx);
                    annotation.header_depth = Some(frame.header_depth);
                }

                for frame in active_frames.iter_mut() {
                    frame.paren_depth = frame.paren_depth.saturating_sub(1);
                }

                idx += 1;
                continue;
            }

            if !saw_significant_token {
                saw_significant_token = true;
            }

            if byte == b';' {
                pending_headers.clear();
                active_frames.clear();
                break;
            }

            idx += 1;
        }

        let leading_close_condition_continuation = saw_leading_close_paren
            && Self::auto_format_is_query_condition_continuation_clause(
                &sql_text::trim_after_leading_close_parens(line).to_ascii_uppercase(),
            );

        if annotation.role == AutoFormatConditionRole::None && annotation.header_line_idx.is_some()
        {
            annotation.role = if saw_leading_close_paren && !leading_close_condition_continuation {
                AutoFormatConditionRole::Closer
            } else {
                AutoFormatConditionRole::Continuation
            };
        }

        annotation
    }

    fn line_starts_new_query_frame(
        head_kind: AutoFormatClauseKind,
        parser_depth: usize,
        active_frame: Option<QueryBaseDepthFrame>,
        has_pending_query_base: bool,
    ) -> bool {
        if has_pending_query_base {
            if active_frame.is_some_and(|frame| {
                frame.head_kind == Some(AutoFormatClauseKind::Merge)
                    && matches!(
                        head_kind,
                        AutoFormatClauseKind::Update
                            | AutoFormatClauseKind::Delete
                            | AutoFormatClauseKind::Insert
                    )
            }) {
                return false;
            }
            return true;
        }

        let Some(frame) = active_frame else {
            return true;
        };

        if frame.head_kind == Some(AutoFormatClauseKind::With)
            && head_kind != AutoFormatClauseKind::With
            && parser_depth == frame.start_parser_depth
        {
            return false;
        }

        if parser_depth > frame.query_base_depth
            && !(frame.pending_same_depth_set_operator_head
                && parser_depth == frame.start_parser_depth)
        {
            return true;
        }

        head_kind == AutoFormatClauseKind::With
            && !(frame.head_kind == Some(AutoFormatClauseKind::With)
                && parser_depth == frame.query_base_depth)
    }

    fn line_owns_next_query(trimmed_upper: &str) -> bool {
        sql_text::format_plsql_child_query_owner_kind(trimmed_upper).is_some()
            || Self::line_ends_with_then_before_inline_comment(trimmed_upper)
    }

    fn auto_format_is_merge_using_clause(trimmed_upper: &str) -> bool {
        sql_text::starts_with_keyword_token(trimmed_upper, "USING")
    }

    fn auto_format_is_merge_on_clause(trimmed_upper: &str) -> bool {
        sql_text::starts_with_keyword_token(trimmed_upper, "ON")
    }

    fn auto_format_is_merge_branch_header(trimmed_upper: &str) -> bool {
        trimmed_upper.starts_with("WHEN MATCHED") || trimmed_upper.starts_with("WHEN NOT MATCHED")
    }

    fn auto_format_is_merge_branch_condition_clause(trimmed_upper: &str) -> bool {
        trimmed_upper.starts_with("AND ") || trimmed_upper.starts_with("OR ")
    }

    fn merge_branch_action_from_clause_kind(
        clause_kind: Option<AutoFormatClauseKind>,
    ) -> Option<MergeBranchAction> {
        match clause_kind {
            Some(AutoFormatClauseKind::Update) => Some(MergeBranchAction::Update),
            Some(AutoFormatClauseKind::Delete) => Some(MergeBranchAction::Delete),
            Some(AutoFormatClauseKind::Insert) => Some(MergeBranchAction::Insert),
            _ => None,
        }
    }

    fn line_ends_with_then_before_inline_comment(line: &str) -> bool {
        Self::trailing_identifier_before_inline_comment(line)
            .is_some_and(|identifier| identifier.eq_ignore_ascii_case("THEN"))
    }

    fn line_ends_with_open_paren_before_inline_comment(line: &str) -> bool {
        Self::trailing_significant_byte_before_inline_comment(line) == Some(b'(')
    }

    fn line_is_standalone_open_paren_before_inline_comment(line: &str) -> bool {
        let prefix = Self::trailing_inline_comment_prefix(line).unwrap_or(line);
        prefix.trim() == "("
    }

    fn line_multiline_clause_owner_kind(
        line: &str,
    ) -> Option<sql_text::FormatIndentedParenOwnerKind> {
        Self::line_ends_with_open_paren_before_inline_comment(line)
            .then(|| sql_text::format_indented_paren_owner_kind(line))
            .flatten()
    }

    fn line_multiline_clause_owner_header_kind(
        line: &str,
    ) -> Option<sql_text::FormatIndentedParenOwnerKind> {
        (!Self::line_ends_with_open_paren_before_inline_comment(line))
            .then(|| sql_text::format_indented_paren_owner_header_kind(line))
            .flatten()
    }

    fn line_ends_with_keyword_before_inline_comment_owns_query(line: &str) -> bool {
        !Self::line_ends_with_open_paren_before_inline_comment(line)
            && (sql_text::format_query_owner_header_kind(line).is_some()
                || sql_text::line_is_create_query_body_header(line))
    }

    fn auto_format_next_code_line_indices(lines: &[&str]) -> Vec<Option<usize>> {
        let mut is_code_line = vec![false; lines.len()];
        let mut in_block_comment = false;

        for (idx, line) in lines.iter().enumerate() {
            let trimmed = line.trim_start();
            let was_in_block_comment = in_block_comment;

            if in_block_comment {
                sql_text::update_block_comment_state(trimmed, &mut in_block_comment);
                continue;
            }

            if trimmed.is_empty() {
                continue;
            }

            if trimmed.starts_with("/*") {
                sql_text::update_block_comment_state(trimmed, &mut in_block_comment);
                continue;
            }

            if trimmed.starts_with("--")
                || sql_text::is_sqlplus_comment_line(trimmed)
                || (was_in_block_comment && trimmed.starts_with("*/"))
            {
                continue;
            }

            is_code_line[idx] = true;
        }

        let mut next_code_indices = vec![None; lines.len()];
        let mut next_code_idx = None;
        for idx in (0..lines.len()).rev() {
            next_code_indices[idx] = next_code_idx;
            if is_code_line[idx] {
                next_code_idx = Some(idx);
            }
        }

        next_code_indices
    }

    fn split_query_owner_lookahead_kind(
        lines: &[&str],
        idx: usize,
        next_code_indices: &[Option<usize>],
        line: &str,
    ) -> Option<sql_text::SplitQueryOwnerLookaheadKind> {
        let open_idx = next_code_indices.get(idx).copied().flatten()?;
        let head_idx = next_code_indices.get(open_idx).copied().flatten()?;
        let head_upper = lines[head_idx].trim_start().to_ascii_uppercase();
        sql_text::split_query_owner_lookahead_kind(
            line,
            Self::line_is_standalone_open_paren_before_inline_comment(lines[open_idx]),
            Some(&head_upper),
        )
    }

    fn auto_format_multiline_owner_depth(
        kind: sql_text::FormatIndentedParenOwnerKind,
        fallback_depth: usize,
        query_base_depth: Option<usize>,
    ) -> usize {
        match kind {
            // Analyzer phase has already promoted MODEL subclauses to the
            // active MODEL body depth before we classify multiline owners.
            sql_text::FormatIndentedParenOwnerKind::ModelSubclause => fallback_depth,
            _ => kind.formatter_owner_depth(fallback_depth, query_base_depth, None),
        }
    }

    fn pop_expired_owner_relative_depth_frames(
        owner_relative_frames: &mut Vec<OwnerRelativeDepthFrame>,
        parser_depth: usize,
        clause_kind: Option<AutoFormatClauseKind>,
        trimmed_upper: &str,
    ) {
        while owner_relative_frames
            .last()
            .is_some_and(|frame| match frame.kind {
                OwnerRelativeDepthFrameKind::ModelClause { start_parser_depth } => {
                    let continues_model_header =
                        frame.body_header_line_state(trimmed_upper).is_header;
                    parser_depth < start_parser_depth
                        || (parser_depth <= start_parser_depth
                            && clause_kind.is_some()
                            && !continues_model_header
                            && !sql_text::starts_with_keyword_token(trimmed_upper, "MODEL")
                            && !sql_text::starts_with_format_model_subclause(trimmed_upper))
                }
                OwnerRelativeDepthFrameKind::MultilineClause { .. } => false,
            })
        {
            owner_relative_frames.pop();
        }
    }

    fn active_owner_relative_depth_frame(
        owner_relative_frames: &[OwnerRelativeDepthFrame],
    ) -> Option<OwnerRelativeDepthFrame> {
        owner_relative_frames.last().copied()
    }

    fn pop_closed_multiline_owner_relative_depth_frames(
        owner_relative_frames: &mut Vec<OwnerRelativeDepthFrame>,
    ) -> Option<usize> {
        let mut last_closed_owner_depth = None;

        while owner_relative_frames
            .last()
            .copied()
            .is_some_and(OwnerRelativeDepthFrame::is_closed_multiline_clause)
        {
            if let Some(frame) = owner_relative_frames.pop() {
                last_closed_owner_depth = Some(frame.owner_depth());
            }
        }

        last_closed_owner_depth
    }

    fn apply_multiline_owner_relative_paren_event(
        owner_relative_frames: &mut Vec<OwnerRelativeDepthFrame>,
        event: sql_text::SignificantParenEvent,
    ) -> Option<usize> {
        owner_relative_frames
            .iter_mut()
            .for_each(|frame| frame.note_multiline_paren_event(event));

        if event == sql_text::SignificantParenEvent::Close {
            Self::pop_closed_multiline_owner_relative_depth_frames(owner_relative_frames)
        } else {
            None
        }
    }

    fn consume_leading_multiline_clause_owner_relative_paren_closes(
        owner_relative_frames: &mut Vec<OwnerRelativeDepthFrame>,
        paren_profile: &sql_text::SignificantParenProfile,
    ) -> Option<usize> {
        let mut last_closed_owner_depth = None;

        for _ in 0..paren_profile.leading_close_count {
            if let Some(owner_depth) = Self::apply_multiline_owner_relative_paren_event(
                owner_relative_frames,
                sql_text::SignificantParenEvent::Close,
            ) {
                last_closed_owner_depth = Some(owner_depth);
            }
        }

        last_closed_owner_depth
    }

    fn apply_remaining_multiline_owner_relative_paren_profile(
        owner_relative_frames: &mut Vec<OwnerRelativeDepthFrame>,
        paren_profile: &sql_text::SignificantParenProfile,
    ) {
        for event in paren_profile
            .events
            .iter()
            .skip(paren_profile.leading_close_count)
        {
            let _ = Self::apply_multiline_owner_relative_paren_event(owner_relative_frames, *event);
        }
    }

    fn pending_plsql_child_query_owner_nested_paren_depth_after_line(
        nested_paren_depth: usize,
        paren_profile: &sql_text::SignificantParenProfile,
    ) -> usize {
        sql_text::significant_paren_depth_after_profile(nested_paren_depth, paren_profile)
    }

    fn pending_plsql_child_query_owner_alignment_depth(
        frame: PendingPlsqlChildQueryOwnerFrame,
        line: &str,
        nested_paren_depth_after_line: usize,
    ) -> Option<usize> {
        if !frame.kind.line_can_continue(line) {
            return None;
        }

        let trimmed = line.trim_start();
        let aligns_owner_boundary = frame.nested_paren_depth == 0
            || nested_paren_depth_after_line == 0
            || Self::line_is_standalone_open_paren_before_inline_comment(trimmed)
            || sql_text::line_has_leading_significant_close_paren(trimmed);

        aligns_owner_boundary.then_some(frame.owner_align_depth)
    }

    fn pending_query_owner_base_depth(
        context: AutoFormatLineContext,
        trimmed_upper: &str,
    ) -> usize {
        let normalized = trimmed_upper.trim_end();
        let structural_owner_base = context.auto_depth;
        if context.condition_role != AutoFormatConditionRole::None {
            if let Some(header_depth) = context.condition_header_depth {
                if Self::line_ends_with_open_paren_before_inline_comment(normalized)
                    && normalized == "("
                    && structural_owner_base > header_depth
                {
                    return structural_owner_base;
                }
                return header_depth;
            }
        }

        sql_text::format_query_owner_kind(normalized)
            .or_else(|| sql_text::format_query_owner_header_kind(normalized))
            .map(|kind| {
                kind.auto_format_child_query_owner_base_depth(
                    structural_owner_base,
                    context.query_base_depth,
                )
            })
            .unwrap_or(structural_owner_base)
    }

    fn next_query_head_depth(context: AutoFormatLineContext, trimmed_upper: &str) -> usize {
        Self::pending_query_owner_base_depth(context, trimmed_upper).saturating_add(1)
    }

    fn line_is_multitable_insert_header(trimmed_upper: &str) -> bool {
        trimmed_upper.starts_with("INSERT ALL") || trimmed_upper.starts_with("INSERT FIRST")
    }

    fn line_is_cte_definition_header(line: &str) -> bool {
        let trimmed = line.trim();
        if trimmed.is_empty() || !Self::line_ends_with_open_paren_before_inline_comment(trimmed) {
            return false;
        }

        let upper = trimmed.to_ascii_uppercase();
        if sql_text::starts_with_keyword_token(&upper, "WITH") {
            return upper.contains(" AS ");
        }

        upper.contains(" AS (")
    }

    fn line_ends_with_comma_before_inline_comment(line: &str) -> bool {
        Self::trailing_significant_byte_before_inline_comment(line) == Some(b',')
    }

    fn line_is_standalone_from_clause_header(trimmed_upper: &str) -> bool {
        trimmed_upper.trim() == "FROM"
    }

    fn line_starts_with_bare_direct_from_item_query_owner(trimmed_upper: &str) -> bool {
        crate::sql_text::starts_with_keyword_token(trimmed_upper, "LATERAL")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "TABLE")
    }

    fn line_starts_continuation_boundary(line: &str) -> bool {
        let trimmed = line.trim_start();
        if trimmed.is_empty() {
            return true;
        }

        let trimmed_upper = trimmed.to_ascii_uppercase();
        sql_text::starts_with_auto_format_structural_continuation_boundary_without_expression_owner(
            trimmed,
        ) || trimmed_upper.starts_with("WHEN MATCHED")
            || trimmed_upper.starts_with("WHEN NOT MATCHED")
            || Self::line_is_standalone_open_paren_before_inline_comment(trimmed)
    }

    fn line_continuation_for_line(
        line: &str,
        depth: usize,
        query_base_depth: Option<usize>,
        next_code_trimmed: Option<&str>,
    ) -> Option<InlineCommentLineContinuation> {
        let trimmed = line.trim_end();
        if trimmed.is_empty() || trimmed.ends_with(';') {
            return None;
        }

        let next_line = next_code_trimmed?;
        let next_line_is_standalone_open_paren =
            Self::line_is_standalone_open_paren_before_inline_comment(next_line);
        if Self::line_starts_continuation_boundary(next_line)
            && !(next_line_is_standalone_open_paren
                && Self::line_can_continue_across_standalone_open_boundary(trimmed))
        {
            return None;
        }

        let kind = Self::line_continuation_kind(trimmed)?;
        let continuation_depth = match kind {
            InlineCommentContinuationKind::SameDepth => depth,
            InlineCommentContinuationKind::OneDeeperThanQueryBase => {
                query_base_depth.unwrap_or(depth).saturating_add(1)
            }
            InlineCommentContinuationKind::OneDeeperThanCurrentDepth => depth.saturating_add(1),
        };
        Some(InlineCommentLineContinuation {
            depth: continuation_depth,
            query_base_depth,
        })
    }

    fn line_can_continue_across_standalone_open_boundary(line: &str) -> bool {
        Self::line_has_trailing_continuation_operator(line)
            || sql_text::format_bare_structural_header_continuation_kind(line).is_some()
    }

    fn inline_comment_line_continuation_for_line(
        line: &str,
        depth: usize,
        query_base_depth: Option<usize>,
        next_code_trimmed: Option<&str>,
    ) -> Option<InlineCommentLineContinuation> {
        let next_line = next_code_trimmed?;
        if Self::line_starts_continuation_boundary(next_line) {
            return None;
        }

        let prefix = Self::trailing_inline_comment_prefix(line)?;
        let trimmed = prefix.trim_end();
        if trimmed.is_empty() || trimmed.ends_with(';') {
            return None;
        }

        let kind = Self::inline_comment_line_continuation_kind(trimmed)?;
        let continuation_depth = match kind {
            InlineCommentContinuationKind::SameDepth => depth,
            InlineCommentContinuationKind::OneDeeperThanQueryBase => {
                query_base_depth.unwrap_or(depth).saturating_add(1)
            }
            InlineCommentContinuationKind::OneDeeperThanCurrentDepth => depth.saturating_add(1),
        };
        Some(InlineCommentLineContinuation {
            depth: continuation_depth,
            query_base_depth,
        })
    }

    fn line_continuation_kind(trimmed_prefix: &str) -> Option<InlineCommentContinuationKind> {
        if Self::line_has_trailing_continuation_operator(trimmed_prefix) {
            return Some(
                Self::leading_header_line_continuation_kind(trimmed_prefix)
                    .unwrap_or(InlineCommentContinuationKind::SameDepth),
            );
        }

        Self::bare_structural_header_line_continuation_kind(trimmed_prefix)
    }

    fn bare_structural_header_line_continuation_kind(
        trimmed_prefix: &str,
    ) -> Option<InlineCommentContinuationKind> {
        sql_text::format_bare_structural_header_continuation_kind(trimmed_prefix).map(|kind| {
            match kind {
                sql_text::FormatInlineCommentHeaderContinuationKind::SameDepth => {
                    InlineCommentContinuationKind::SameDepth
                }
                sql_text::FormatInlineCommentHeaderContinuationKind::OneDeeperThanQueryBase => {
                    InlineCommentContinuationKind::OneDeeperThanQueryBase
                }
                sql_text::FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine => {
                    InlineCommentContinuationKind::OneDeeperThanCurrentDepth
                }
            }
        })
    }

    fn inline_comment_line_continuation_kind(
        trimmed_prefix: &str,
    ) -> Option<InlineCommentContinuationKind> {
        if Self::line_has_trailing_continuation_operator(trimmed_prefix) {
            return Some(InlineCommentContinuationKind::SameDepth);
        }

        let words: Vec<&str> = trimmed_prefix
            .split_whitespace()
            .filter(|word| !word.is_empty())
            .collect();
        let last_word = words.last().copied()?;
        let previous_word = words.get(words.len().saturating_sub(2)).copied();
        sql_text::format_inline_comment_header_continuation_kind(previous_word, last_word).map(
            |kind| match kind {
                sql_text::FormatInlineCommentHeaderContinuationKind::SameDepth => {
                    InlineCommentContinuationKind::SameDepth
                }
                sql_text::FormatInlineCommentHeaderContinuationKind::OneDeeperThanQueryBase => {
                    InlineCommentContinuationKind::OneDeeperThanQueryBase
                }
                sql_text::FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine => {
                    InlineCommentContinuationKind::OneDeeperThanCurrentDepth
                }
            },
        )
    }

    fn line_has_trailing_continuation_operator(trimmed_prefix: &str) -> bool {
        sql_text::line_has_trailing_format_continuation_operator(trimmed_prefix)
    }

    fn leading_header_line_continuation_kind(
        trimmed_prefix: &str,
    ) -> Option<InlineCommentContinuationKind> {
        sql_text::format_structural_header_continuation_kind(trimmed_prefix).map(
            |kind| match kind {
                sql_text::FormatInlineCommentHeaderContinuationKind::SameDepth => {
                    InlineCommentContinuationKind::SameDepth
                }
                sql_text::FormatInlineCommentHeaderContinuationKind::OneDeeperThanQueryBase => {
                    InlineCommentContinuationKind::OneDeeperThanQueryBase
                }
                sql_text::FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine => {
                    InlineCommentContinuationKind::OneDeeperThanCurrentDepth
                }
            },
        )
    }

    fn trailing_inline_comment_prefix(line: &str) -> Option<&str> {
        let bytes = line.as_bytes();
        let mut idx = 0usize;
        let mut in_single_quote = false;
        let mut in_double_quote = false;

        while idx < bytes.len() {
            let current = bytes[idx];

            if in_single_quote {
                if current == b'\'' {
                    if idx + 1 < bytes.len() && bytes[idx + 1] == b'\'' {
                        idx += 2;
                        continue;
                    }
                    in_single_quote = false;
                }
                idx += 1;
                continue;
            }

            if in_double_quote {
                if current == b'"' {
                    in_double_quote = false;
                }
                idx += 1;
                continue;
            }

            if current == b'\'' {
                in_single_quote = true;
                idx += 1;
                continue;
            }
            if current == b'"' {
                in_double_quote = true;
                idx += 1;
                continue;
            }

            if current == b'-' && idx + 1 < bytes.len() && bytes[idx + 1] == b'-' {
                return line.get(..idx);
            }

            if current == b'/' && idx + 1 < bytes.len() && bytes[idx + 1] == b'*' {
                let comment_start = idx;
                idx += 2;
                while idx + 1 < bytes.len() {
                    if bytes[idx] == b'*' && bytes[idx + 1] == b'/' {
                        let comment_end = idx + 2;
                        let suffix = line.get(comment_end..).unwrap_or_default().trim();
                        if suffix.is_empty() {
                            return line.get(..comment_start);
                        }
                        return None;
                    }
                    idx += 1;
                }
                return None;
            }

            idx += 1;
        }

        None
    }

    fn trailing_identifier_before_inline_comment(line: &str) -> Option<&str> {
        let bytes = line.as_bytes();
        let mut idx = 0usize;
        let mut last_identifier: Option<(usize, usize)> = None;
        let mut in_single_quote = false;
        let mut in_double_quote = false;

        while idx < bytes.len() {
            let current = bytes[idx];

            if in_single_quote {
                if current == b'\'' {
                    if idx + 1 < bytes.len() && bytes[idx + 1] == b'\'' {
                        idx += 2;
                        continue;
                    }
                    in_single_quote = false;
                }
                idx += 1;
                continue;
            }

            if in_double_quote {
                if current == b'"' {
                    in_double_quote = false;
                }
                idx += 1;
                continue;
            }

            if current == b'-' && idx + 1 < bytes.len() && bytes[idx + 1] == b'-' {
                break;
            }
            if current == b'/' && idx + 1 < bytes.len() && bytes[idx + 1] == b'*' {
                break;
            }
            if current == b'\'' {
                in_single_quote = true;
                idx += 1;
                continue;
            }
            if current == b'"' {
                in_double_quote = true;
                idx += 1;
                continue;
            }
            if sql_text::is_identifier_start_byte(current) {
                let start = idx;
                idx += 1;
                while idx < bytes.len() && sql_text::is_identifier_byte(bytes[idx]) {
                    idx += 1;
                }
                last_identifier = Some((start, idx));
                continue;
            }

            idx += 1;
        }

        last_identifier.and_then(|(start, end)| line.get(start..end))
    }

    fn trailing_significant_byte_before_inline_comment(line: &str) -> Option<u8> {
        let bytes = line.as_bytes();
        let mut idx = 0usize;
        let mut last_non_ws: Option<u8> = None;
        let mut in_single_quote = false;
        let mut in_double_quote = false;

        while idx < bytes.len() {
            let current = bytes[idx];

            if in_single_quote {
                if current == b'\'' {
                    if idx + 1 < bytes.len() && bytes[idx + 1] == b'\'' {
                        idx += 2;
                        continue;
                    }
                    in_single_quote = false;
                }
                idx += 1;
                continue;
            }

            if in_double_quote {
                if current == b'"' {
                    in_double_quote = false;
                }
                idx += 1;
                continue;
            }

            if current == b'-' && idx + 1 < bytes.len() && bytes[idx + 1] == b'-' {
                break;
            }
            if current == b'/' && idx + 1 < bytes.len() && bytes[idx + 1] == b'*' {
                break;
            }
            if current == b'\'' {
                in_single_quote = true;
                idx += 1;
                continue;
            }
            if current == b'"' {
                in_double_quote = true;
                idx += 1;
                continue;
            }

            if !current.is_ascii_whitespace() {
                last_non_ws = Some(current);
            }
            idx += 1;
        }

        last_non_ws
    }

    pub fn strip_leading_comments(sql: &str) -> String {
        let mut remaining = sql;

        loop {
            let trimmed = remaining.trim_start();

            if sql_text::is_sqlplus_comment_line(trimmed) {
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
                if sql_text::is_sqlplus_comment_line(last_line) {
                    result = trimmed[..last_newline].to_string();
                    continue;
                }
            } else {
                // Single line - check if entire thing is a line comment
                if sql_text::is_sqlplus_comment_line(trimmed) {
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
                        if depth == 0 {
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

    pub(crate) fn strip_comments(sql: &str) -> String {
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

        if Self::should_preserve_single_trailing_semicolon(without_semis) {
            format!("{};", without_semis)
        } else {
            without_semis.to_string()
        }
    }

    fn should_preserve_single_trailing_semicolon(sql: &str) -> bool {
        let mut trailing_tokens = sql
            .split_whitespace()
            .rev()
            .map(|token| token.trim_matches(|ch: char| !sql_text::is_identifier_char(ch)))
            .filter(|token| !token.is_empty());

        let Some(last_token) = trailing_tokens.next() else {
            return false;
        };

        if last_token.eq_ignore_ascii_case("END") {
            return true;
        }

        trailing_tokens
            .next()
            .is_some_and(|token| token.eq_ignore_ascii_case("END"))
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
            Some("SELECT") | Some("VALUES") | Some("TABLE") => true,
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
                if Self::is_parenthesized_select_start(sql, main_select_idx) {
                    return sql.to_string();
                }
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
                if Self::is_parenthesized_select_start(rewritten_sql, main_select_idx) {
                    return rewritten_sql.to_string();
                }
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
        for token in TopLevelScanner::new(sql) {
            if let ScanToken::Symbol { byte: b',', pos } = token {
                return Some(pos);
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
        let mut scanner = TopLevelScanner::new(select_list);
        while let Some(token) = scanner.next() {
            if let ScanToken::Word { text, .. } = token {
                if Self::is_aggregate_function_name(text)
                    && scanner.peek_next_non_ws_byte() == Some(b'(')
                {
                    return true;
                }
            }
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
        let mut scanner = TopLevelScanner::new(select_list);
        while let Some(token) = scanner.next() {
            if let ScanToken::Word { text, .. } = token {
                if text.eq_ignore_ascii_case("OVER")
                    && scanner.peek_next_non_ws_byte() == Some(b'(')
                {
                    return true;
                }
            }
        }
        false
    }

    /// Find the byte index of the main (final) SELECT keyword after a WITH clause.
    /// This skips over all CTE definitions to find the top-level SELECT that follows.
    fn find_main_select_after_with(sql: &str) -> Option<usize> {
        let bytes = sql.as_bytes();
        let len = bytes.len();
        let mut pos = 0usize;

        // Skip past the WITH keyword
        while pos < len {
            let b = bytes[pos];
            if b.is_ascii_alphabetic() {
                let start = pos;
                pos += 1;
                while pos < len && (bytes[pos].is_ascii_alphanumeric() || bytes[pos] == b'_') {
                    pos += 1;
                }
                if sql
                    .get(start..pos)
                    .is_some_and(|w| w.eq_ignore_ascii_case("WITH"))
                {
                    break;
                }
                continue;
            }
            pos += 1;
        }

        // Scan for top-level SELECT at depth 0
        let mut depth = 0usize;
        let mut top_level_closed_paren_recently = false;
        let mut parenthesized_main_query_depth: Option<usize> = None;

        while pos < len {
            let b = bytes[pos];

            // Line comment
            if b == b'-' && bytes.get(pos + 1) == Some(&b'-') {
                pos += 2;
                while pos < len && bytes[pos] != b'\n' {
                    pos += 1;
                }
                continue;
            }
            // Block comment
            if b == b'/' && bytes.get(pos + 1) == Some(&b'*') {
                pos += 2;
                while pos + 1 < len {
                    if bytes[pos] == b'*' && bytes[pos + 1] == b'/' {
                        pos += 2;
                        break;
                    }
                    pos += 1;
                }
                continue;
            }
            // Single-quoted string
            if b == b'\'' {
                pos += 1;
                while pos < len {
                    if bytes[pos] == b'\'' {
                        pos += 1;
                        if pos < len && bytes[pos] == b'\'' {
                            pos += 1;
                            continue;
                        }
                        break;
                    }
                    pos += 1;
                }
                continue;
            }
            // Double-quoted identifier
            if b == b'"' {
                pos += 1;
                while pos < len {
                    if bytes[pos] == b'"' {
                        pos += 1;
                        if pos < len && bytes[pos] == b'"' {
                            pos += 1;
                            continue;
                        }
                        break;
                    }
                    pos += 1;
                }
                continue;
            }

            if b == b'(' {
                if depth == 0
                    && top_level_closed_paren_recently
                    && parenthesized_main_query_depth.is_none()
                {
                    parenthesized_main_query_depth = Some(1);
                }
                depth += 1;
                pos += 1;
                continue;
            }
            if b == b')' {
                if depth == 1 {
                    top_level_closed_paren_recently = true;
                }
                depth = depth.saturating_sub(1);
                if parenthesized_main_query_depth.is_some_and(|wd| depth < wd) {
                    parenthesized_main_query_depth = None;
                }
                pos += 1;
                continue;
            }

            if b.is_ascii_whitespace() {
                pos += 1;
                continue;
            }

            if !b.is_ascii_whitespace() {
                top_level_closed_paren_recently = false;
            }

            let at_query_depth =
                depth == 0 || parenthesized_main_query_depth.is_some_and(|wd| depth == wd);

            if at_query_depth && b.is_ascii_alphabetic() {
                let start = pos;
                pos += 1;
                while pos < len && (bytes[pos].is_ascii_alphanumeric() || bytes[pos] == b'_') {
                    pos += 1;
                }
                if sql
                    .get(start..pos)
                    .is_some_and(|w| w.eq_ignore_ascii_case("SELECT"))
                {
                    return Some(start);
                }
                continue;
            }

            pos += 1;
        }

        None
    }

    fn is_parenthesized_select_start(sql: &str, select_idx: usize) -> bool {
        if !sql.is_char_boundary(select_idx) {
            return false;
        }
        let prefix = &sql[..select_idx];
        for ch in prefix.chars().rev() {
            if ch.is_whitespace() {
                continue;
            }
            return ch == '(';
        }
        false
    }

    /// Check if the effective SQL has a top-level set operator (UNION, INTERSECT, MINUS, EXCEPT).
    fn has_top_level_set_operator(sql: &str) -> bool {
        sql_text::FORMAT_SET_OPERATOR_KEYWORDS
            .iter()
            .any(|keyword| Self::has_top_level_identifier_keyword(sql, keyword))
    }

    /// Check if the effective SQL has a top-level CONNECT BY clause.
    fn has_top_level_connect_by(sql: &str) -> bool {
        Self::has_top_level_identifier_keyword(sql, "CONNECT")
            || Self::has_top_level_identifier_keyword(sql, "START")
    }

    /// Check if the SQL contains a top-level identifier token matching `keyword`.
    ///
    /// `find_top_level_keyword` already uses full identifier-char boundaries
    /// (`_`, `$`, `#`), so this is a simple presence check.
    fn has_top_level_identifier_keyword(sql: &str, keyword: &str) -> bool {
        Self::find_top_level_keyword(sql, keyword).is_some()
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
        sql_text::is_format_set_operator_keyword(word_upper)
            || matches!(
                word_upper,
                "WHERE"
                    | "ORDER"
                    | "GROUP"
                    | "HAVING"
                    | "FETCH"
                    | "OFFSET"
                    | "FOR"
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
        let bytes = select_body.as_bytes();
        let mut pos = 0usize;

        while pos < bytes.len() {
            let b = bytes[pos];

            if b == b'-' && bytes.get(pos + 1) == Some(&b'-') {
                pos += 2;
                while pos < bytes.len() && bytes[pos] != b'\n' {
                    pos += 1;
                }
                continue;
            }

            if b == b'/' && bytes.get(pos + 1) == Some(&b'*') {
                pos += 2;
                while pos + 1 < bytes.len() {
                    if bytes[pos] == b'*' && bytes[pos + 1] == b'/' {
                        pos += 2;
                        break;
                    }
                    pos += 1;
                }
                continue;
            }

            if b.is_ascii_whitespace() {
                pos += 1;
                continue;
            }

            if b == b'*' {
                return Some((pos, pos + 1));
            }

            return None;
        }

        None
    }

    fn select_clause_has_distinct_or_unique(sql: &str, select_idx: usize, from_idx: usize) -> bool {
        let start = select_idx.saturating_add("SELECT".len());
        if start >= from_idx || start >= sql.len() || !sql.is_char_boundary(from_idx) {
            return false;
        }
        let slice = match sql.get(start..from_idx) {
            Some(s) => s,
            None => return false,
        };
        if let Some(token) = TopLevelScanner::new(slice).next() {
            if let ScanToken::Word { text, .. } = token {
                return text.eq_ignore_ascii_case("DISTINCT")
                    || text.eq_ignore_ascii_case("UNIQUE");
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
        let bytes = sql.as_bytes();
        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
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
        for token in TopLevelScanner::new(sql) {
            if let ScanToken::Word { text, start } = token {
                if text.eq_ignore_ascii_case(keyword) {
                    return Some(start);
                }
            }
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

        for token in TopLevelScanner::new(from_clause) {
            match token {
                ScanToken::Symbol { byte: b',', .. } => return false,
                ScanToken::Word { text, .. } if text.eq_ignore_ascii_case("JOIN") => return false,
                _ => {}
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
        let bytes = trimmed.as_bytes();
        if bytes.is_empty() {
            return false;
        }

        if bytes[0] == b'(' {
            return true;
        }

        let mut pos = 0usize;

        // Skip first identifier (plain or quoted)
        if !Self::skip_identifier_bytes(bytes, &mut pos) {
            return false;
        }

        // Skip optional schema qualifiers: .ident.ident...
        loop {
            let save = pos;
            while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
                pos += 1;
            }
            if pos >= bytes.len() || bytes[pos] != b'.' {
                pos = save;
                break;
            }
            pos += 1; // skip dot
            while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
                pos += 1;
            }
            if !Self::skip_identifier_bytes(bytes, &mut pos) {
                return false;
            }
        }

        // Skip trailing whitespace
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }

        pos < bytes.len() && bytes[pos] == b'('
    }

    /// Skip an identifier (plain or double-quoted) at `pos` in byte slice.
    /// Returns true if an identifier was found and skipped.
    fn skip_identifier_bytes(bytes: &[u8], pos: &mut usize) -> bool {
        if *pos >= bytes.len() {
            return false;
        }
        if bytes[*pos] == b'"' {
            *pos += 1;
            while *pos < bytes.len() {
                if bytes[*pos] == b'"' {
                    *pos += 1;
                    if *pos < bytes.len() && bytes[*pos] == b'"' {
                        *pos += 1; // escaped quote
                        continue;
                    }
                    return true;
                }
                *pos += 1;
            }
            return true; // unterminated
        }
        if sql_text::is_identifier_start_byte(bytes[*pos]) {
            *pos += 1;
            while *pos < bytes.len() && sql_text::is_identifier_byte(bytes[*pos]) {
                *pos += 1;
            }
            return true;
        }
        false
    }

    fn with_clause_starts_with_select(sql: &str) -> bool {
        let stripped = Self::strip_leading_comments(sql);
        let bytes = stripped.as_bytes();
        let len = bytes.len();
        let mut pos = 0usize;

        let mut depth = 0usize;
        let mut in_with_plsql_declaration = false;
        let mut with_plsql_waiting_main_query = false;
        let mut with_plsql_block_depth = 0usize;
        let mut with_plsql_pending_end = false;
        let mut with_plsql_starts_routine_body = false;
        let mut with_plsql_pending_routine_begin = false;
        let mut top_level_closed_paren_recently = false;
        let mut parenthesized_main_query_depth: Option<usize> = None;
        let mut q_quote_end_byte: Option<u8> = None;

        let resolve_pending = |pending: &mut bool, bd: &mut usize| {
            if *pending {
                *bd = bd.saturating_sub(1);
                *pending = false;
            }
        };

        while pos < len {
            let b = bytes[pos];

            // Q-quote mode
            if let Some(end_b) = q_quote_end_byte {
                if b == end_b && bytes.get(pos + 1) == Some(&b'\'') {
                    q_quote_end_byte = None;
                    pos += 2;
                    continue;
                }
                pos += 1;
                continue;
            }

            // Line comment
            if b == b'-' && bytes.get(pos + 1) == Some(&b'-') {
                pos += 2;
                while pos < len && bytes[pos] != b'\n' {
                    pos += 1;
                }
                continue;
            }
            // Block comment
            if b == b'/' && bytes.get(pos + 1) == Some(&b'*') {
                pos += 2;
                while pos + 1 < len {
                    if bytes[pos] == b'*' && bytes[pos + 1] == b'/' {
                        pos += 2;
                        break;
                    }
                    pos += 1;
                }
                continue;
            }

            // NQ'...' or Q'...' (q-quoted strings)
            if (b == b'n' || b == b'N')
                && bytes.get(pos + 1).is_some_and(|&c| c == b'q' || c == b'Q')
                && bytes.get(pos + 2) == Some(&b'\'')
            {
                if let Some(&delim) = bytes.get(pos + 3) {
                    if sql_text::is_valid_q_quote_delimiter_byte(delim) {
                        q_quote_end_byte = Some(sql_text::q_quote_closing_byte(delim));
                        pos += 4;
                        continue;
                    }
                }
            }
            if (b == b'q' || b == b'Q') && bytes.get(pos + 1) == Some(&b'\'') {
                if let Some(&delim) = bytes.get(pos + 2) {
                    if sql_text::is_valid_q_quote_delimiter_byte(delim) {
                        q_quote_end_byte = Some(sql_text::q_quote_closing_byte(delim));
                        pos += 3;
                        continue;
                    }
                }
            }

            // Single-quoted string
            if b == b'\'' {
                pos += 1;
                while pos < len {
                    if bytes[pos] == b'\'' {
                        pos += 1;
                        if pos < len && bytes[pos] == b'\'' {
                            pos += 1;
                            continue;
                        }
                        break;
                    }
                    pos += 1;
                }
                continue;
            }
            // Double-quoted identifier
            if b == b'"' {
                pos += 1;
                while pos < len {
                    if bytes[pos] == b'"' {
                        pos += 1;
                        if pos < len && bytes[pos] == b'"' {
                            pos += 1;
                            continue;
                        }
                        break;
                    }
                    pos += 1;
                }
                continue;
            }

            if b == b'(' {
                if depth == 0
                    && (with_plsql_waiting_main_query || top_level_closed_paren_recently)
                    && parenthesized_main_query_depth.is_none()
                {
                    parenthesized_main_query_depth = Some(1);
                }
                depth += 1;
                pos += 1;
                continue;
            }
            if b == b')' {
                if depth == 1 {
                    top_level_closed_paren_recently = true;
                }
                depth = depth.saturating_sub(1);
                if parenthesized_main_query_depth.is_some_and(|wd| depth < wd) {
                    parenthesized_main_query_depth = None;
                }
                pos += 1;
                continue;
            }
            if b == b';' {
                resolve_pending(&mut with_plsql_pending_end, &mut with_plsql_block_depth);
                if in_with_plsql_declaration && with_plsql_block_depth == 0 {
                    in_with_plsql_declaration = false;
                    with_plsql_waiting_main_query = true;
                    with_plsql_starts_routine_body = false;
                    with_plsql_pending_routine_begin = false;
                }
                pos += 1;
                continue;
            }

            if b.is_ascii_whitespace() {
                pos += 1;
                continue;
            }

            top_level_closed_paren_recently = false;

            let at_query_depth =
                depth == 0 || parenthesized_main_query_depth.is_some_and(|wd| depth == wd);

            if at_query_depth && sql_text::is_identifier_start_byte(b) {
                let start = pos;
                pos += 1;
                while pos < len && sql_text::is_identifier_byte(bytes[pos]) {
                    pos += 1;
                }
                let Some(token) = stripped.get(start..pos) else {
                    continue;
                };

                if in_with_plsql_declaration {
                    if (token.eq_ignore_ascii_case("AS") || token.eq_ignore_ascii_case("IS"))
                        && with_plsql_starts_routine_body
                        && with_plsql_block_depth == 0
                    {
                        with_plsql_block_depth = 1;
                        with_plsql_pending_routine_begin = true;
                        continue;
                    }
                    if token.eq_ignore_ascii_case("BEGIN")
                        || token.eq_ignore_ascii_case("DECLARE")
                        || token.eq_ignore_ascii_case("CASE")
                        || token.eq_ignore_ascii_case("IF")
                        || token.eq_ignore_ascii_case("LOOP")
                    {
                        if token.eq_ignore_ascii_case("BEGIN")
                            && with_plsql_pending_routine_begin
                            && with_plsql_block_depth == 1
                        {
                            with_plsql_pending_routine_begin = false;
                            continue;
                        }
                        resolve_pending(&mut with_plsql_pending_end, &mut with_plsql_block_depth);
                        with_plsql_block_depth += 1;
                        continue;
                    }
                    if token.eq_ignore_ascii_case("END") {
                        with_plsql_pending_end = true;
                        continue;
                    }
                    if with_plsql_pending_end
                        && !(token.eq_ignore_ascii_case("CASE")
                            || token.eq_ignore_ascii_case("IF")
                            || token.eq_ignore_ascii_case("LOOP"))
                    {
                        resolve_pending(&mut with_plsql_pending_end, &mut with_plsql_block_depth);
                    }
                    continue;
                }

                if token.eq_ignore_ascii_case("FUNCTION") || token.eq_ignore_ascii_case("PROCEDURE")
                {
                    in_with_plsql_declaration = true;
                    with_plsql_waiting_main_query = false;
                    with_plsql_block_depth = 0;
                    with_plsql_pending_end = false;
                    with_plsql_starts_routine_body = true;
                    with_plsql_pending_routine_begin = false;
                    continue;
                }

                if token.eq_ignore_ascii_case("PACKAGE") || token.eq_ignore_ascii_case("TYPE") {
                    in_with_plsql_declaration = true;
                    with_plsql_waiting_main_query = false;
                    with_plsql_block_depth = 0;
                    with_plsql_pending_end = false;
                    with_plsql_starts_routine_body = false;
                    with_plsql_pending_routine_begin = false;
                    continue;
                }

                if with_plsql_waiting_main_query {
                    if token.eq_ignore_ascii_case("SELECT")
                        || token.eq_ignore_ascii_case("VALUES")
                        || token.eq_ignore_ascii_case("TABLE")
                    {
                        return true;
                    }
                    if token.eq_ignore_ascii_case("INSERT")
                        || token.eq_ignore_ascii_case("UPDATE")
                        || token.eq_ignore_ascii_case("DELETE")
                        || token.eq_ignore_ascii_case("MERGE")
                    {
                        return false;
                    }
                    if sql_text::is_statement_head_keyword(token)
                        && !sql_text::is_with_main_query_keyword(token)
                    {
                        return false;
                    }
                }

                if token.eq_ignore_ascii_case("SELECT")
                    || token.eq_ignore_ascii_case("VALUES")
                    || token.eq_ignore_ascii_case("TABLE")
                {
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

            pos += 1;
        }

        false
    }

    pub fn split_script_items(sql: &str) -> Vec<ScriptItem> {
        let mut items: Vec<ScriptItem> = Vec::new();
        let add_statement = |stmt: String, items: &mut Vec<ScriptItem>| {
            let stripped = Self::strip_comments(&stmt);
            let cleaned = Self::strip_extra_trailing_semicolons(&stripped);
            if !cleaned.is_empty() {
                items.push(ScriptItem::Statement(cleaned));
            }
        };
        let on_tool_command = |cmd: ToolCommand, _raw_line: &str, items: &mut Vec<ScriptItem>| {
            items.push(ScriptItem::ToolCommand(cmd));
        };

        Self::split_items_core(sql, &mut items, add_statement, on_tool_command, |_, _| {});
        let items = Self::merge_fragmented_standalone_routine_script_statements(items);
        Self::merge_fragmented_with_single_letter_cte_script_items(items)
    }

    fn merge_fragmented_with_single_letter_cte_script_items(
        items: Vec<ScriptItem>,
    ) -> Vec<ScriptItem> {
        let mut merged: Vec<ScriptItem> = Vec::with_capacity(items.len());
        let mut index = 0usize;

        while index < items.len() {
            if let Some((combined, consumed)) =
                Self::combine_fragmented_with_single_letter_cte_script_item(&items, index)
            {
                merged.push(ScriptItem::Statement(combined));
                index += consumed;
                continue;
            }

            match items.get(index) {
                Some(ScriptItem::Statement(statement)) => {
                    merged.push(ScriptItem::Statement(statement.clone()));
                }
                Some(other) => merged.push(other.clone()),
                None => {}
            }
            index += 1;
        }

        merged
    }

    fn combine_fragmented_with_single_letter_cte_script_item(
        items: &[ScriptItem],
        start_index: usize,
    ) -> Option<(String, usize)> {
        let with_head = match items.get(start_index) {
            Some(ScriptItem::Statement(statement))
                if statement.trim().eq_ignore_ascii_case("WITH") =>
            {
                statement.trim()
            }
            _ => return None,
        };

        let cte_name = match items.get(start_index + 1) {
            Some(ScriptItem::ToolCommand(ToolCommand::Unsupported { raw, .. })) => raw.trim(),
            _ => return None,
        };
        if !Self::is_single_letter_cte_identifier(cte_name) {
            return None;
        }

        let trailing = match items.get(start_index + 2) {
            Some(ScriptItem::Statement(statement)) => statement.trim_start(),
            _ => return None,
        };
        if !trailing.to_ascii_uppercase().starts_with("AS") {
            return None;
        }

        Some((format!("{with_head} {cte_name}\n{trailing}"), 3))
    }

    fn is_single_letter_cte_identifier(value: &str) -> bool {
        let mut chars = value.chars();
        let Some(ch) = chars.next() else {
            return false;
        };
        chars.next().is_none() && (ch.is_ascii_alphabetic() || ch == '_')
    }

    fn merge_fragmented_standalone_routine_script_statements(
        items: Vec<ScriptItem>,
    ) -> Vec<ScriptItem> {
        let mut merged: Vec<ScriptItem> = Vec::with_capacity(items.len());
        let mut index = 0usize;

        while index < items.len() {
            if let Some((combined, end_index)) =
                Self::combine_fragmented_standalone_routine_statement(&items, index)
            {
                merged.push(ScriptItem::Statement(combined));
                index = end_index + 1;
                continue;
            }

            match items.get(index) {
                Some(ScriptItem::Statement(statement)) => {
                    merged.push(ScriptItem::Statement(statement.clone()));
                }
                Some(other) => merged.push(other.clone()),
                None => {}
            }
            index += 1;
        }

        merged
    }

    fn combine_fragmented_standalone_routine_statement(
        items: &[ScriptItem],
        start_index: usize,
    ) -> Option<(String, usize)> {
        let statement = match items.get(start_index) {
            Some(ScriptItem::Statement(statement)) => statement,
            _ => return None,
        };

        let routine_name = Self::extract_standalone_routine_name(statement)?;
        if Self::statement_has_matching_end_label(statement, routine_name.as_str()) {
            return None;
        }

        let mut end_index = start_index + 1;
        while end_index < items.len() {
            match items.get(end_index) {
                Some(ScriptItem::ToolCommand(_)) => return None,
                Some(ScriptItem::Statement(next_statement)) => {
                    if Self::is_orphan_end_label_statement_matching_routine(
                        next_statement,
                        routine_name.as_str(),
                    ) {
                        return Self::combine_statement_range(items, start_index, end_index)
                            .map(|combined| (combined, end_index));
                    }

                    if Self::starts_new_top_level_create_statement(next_statement) {
                        return None;
                    }
                }
                None => return None,
            }
            end_index += 1;
        }

        None
    }

    fn combine_statement_range(
        items: &[ScriptItem],
        start_index: usize,
        end_index: usize,
    ) -> Option<String> {
        let combined = items[start_index..=end_index]
            .iter()
            .filter_map(|item| match item {
                ScriptItem::Statement(statement) => Some(statement.trim()),
                ScriptItem::ToolCommand(_) => None,
            })
            .filter(|statement| !statement.is_empty())
            .collect::<Vec<_>>()
            .join(";\n");

        if combined.is_empty() {
            return None;
        }

        let stripped = Self::strip_comments(combined.as_str());
        let cleaned = Self::strip_extra_trailing_semicolons(stripped.as_str());
        if cleaned.is_empty() {
            None
        } else {
            Some(cleaned)
        }
    }

    fn extract_standalone_routine_name(statement: &str) -> Option<String> {
        let chars: Vec<(usize, char)> = statement.char_indices().collect();
        let mut pos = 0usize;

        let first = Self::parse_keyword_token_at(chars.as_slice(), statement, &mut pos)?;
        if first != "CREATE" {
            return None;
        }

        let next = Self::parse_keyword_token_at(chars.as_slice(), statement, &mut pos)?;
        let routine_keyword = if next == "OR" {
            let replace = Self::parse_keyword_token_at(chars.as_slice(), statement, &mut pos)?;
            if replace != "REPLACE" {
                return None;
            }
            Self::parse_keyword_token_at(chars.as_slice(), statement, &mut pos)?
        } else {
            next
        };

        if !matches!(routine_keyword.as_str(), "PROCEDURE" | "FUNCTION") {
            return None;
        }

        Self::parse_identifier_chain_upper_at(chars.as_slice(), statement, &mut pos)
    }

    fn statement_has_matching_end_label(statement: &str, routine_name: &str) -> bool {
        Self::extract_end_label_chain(statement).is_some_and(|end_label| {
            Self::identifier_chain_matches_routine(&end_label, routine_name)
        })
    }

    fn is_orphan_end_label_statement_matching_routine(statement: &str, routine_name: &str) -> bool {
        Self::extract_end_label_chain(statement).is_some_and(|end_label| {
            Self::identifier_chain_matches_routine(&end_label, routine_name)
                && Self::trimmed_statement_starts_with_end(statement)
        })
    }

    fn trimmed_statement_starts_with_end(statement: &str) -> bool {
        statement
            .trim_start()
            .get(..3)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("END"))
    }

    fn starts_new_top_level_create_statement(statement: &str) -> bool {
        statement
            .trim_start()
            .get(..6)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("CREATE"))
    }

    fn identifier_chain_matches_routine(label: &str, routine_name: &str) -> bool {
        if label == routine_name {
            return true;
        }

        routine_name
            .rsplit('.')
            .next()
            .is_some_and(|tail| tail == label)
    }

    fn extract_end_label_chain(statement: &str) -> Option<String> {
        let trimmed = statement.trim();
        if !Self::trimmed_statement_starts_with_end(trimmed) {
            return None;
        }

        let chars: Vec<(usize, char)> = trimmed.char_indices().collect();
        let mut pos = 0usize;
        let keyword = Self::parse_keyword_token_at(chars.as_slice(), trimmed, &mut pos)?;
        if keyword != "END" {
            return None;
        }

        Self::parse_identifier_chain_upper_at(chars.as_slice(), trimmed, &mut pos)
    }

    fn parse_keyword_token_at(
        chars: &[(usize, char)],
        text: &str,
        pos: &mut usize,
    ) -> Option<String> {
        Self::skip_whitespace_chars(chars, pos);
        let save_pos = *pos;
        let token = Self::parse_identifier_at(chars, text, pos)?;
        if token.starts_with('"') {
            *pos = save_pos;
            return None;
        }
        Some(token.to_ascii_uppercase())
    }

    fn parse_identifier_chain_upper_at(
        chars: &[(usize, char)],
        text: &str,
        pos: &mut usize,
    ) -> Option<String> {
        let mut segments: Vec<String> = Vec::new();

        loop {
            Self::skip_whitespace_chars(chars, pos);
            let segment = Self::parse_identifier_at(chars, text, pos)?;
            segments.push(Self::normalize_identifier_segment(segment.as_str()));
            Self::skip_whitespace_chars(chars, pos);

            if chars.get(*pos).is_some_and(|(_, ch)| *ch == '.') {
                *pos += 1;
                continue;
            }
            break;
        }

        if segments.is_empty() {
            None
        } else {
            Some(segments.join("."))
        }
    }

    fn normalize_identifier_segment(segment: &str) -> String {
        if segment.starts_with('"') && segment.ends_with('"') && segment.len() >= 2 {
            let inner = segment
                .get(1..segment.len().saturating_sub(1))
                .unwrap_or_default()
                .replace("\"\"", "\"");
            return inner.to_ascii_uppercase();
        }

        segment.to_ascii_uppercase()
    }

    fn skip_whitespace_chars(chars: &[(usize, char)], pos: &mut usize) {
        while chars.get(*pos).is_some_and(|(_, ch)| ch.is_whitespace()) {
            *pos += 1;
        }
    }

    pub fn split_format_items(sql: &str) -> Vec<FormatItem> {
        // Standalone comments between statements need special handling in format
        // mode: line comments, remark lines, and multi-line block comments are
        // preserved as separate items so the formatter can keep them intact.
        // Collect these first, then splice them back into position.
        //
        // We pre-scan for standalone block comments that span multiple lines
        // and merge them into single-line markers that the core loop can handle
        // via the on_idle_line callback.

        let mut items: Vec<FormatItem> = Vec::new();
        let mut builder = SqlParserEngine::new();
        let mut sqlblanklines_enabled = true;

        let mut add_statement = |stmt: String, items: &mut Vec<FormatItem>| {
            let cleaned = stmt.trim();
            if !cleaned.is_empty() {
                items.push(FormatItem::Statement(cleaned.to_string()));
            }
        };

        let mut lines = sql.lines().peekable();
        while let Some(line) = lines.next() {
            let logical_line = if Self::can_collect_multiline_tool_command(&builder) {
                Self::collect_multiline_tool_command(line, &mut lines)
            } else {
                None
            };
            let line = logical_line.as_deref().unwrap_or(line);
            let trimmed = line.trim();

            // Blank-line termination
            if Self::should_force_terminate_on_blank_line(
                sqlblanklines_enabled,
                trimmed,
                builder.is_idle(),
                builder.block_depth(),
                builder.current_is_empty(),
            ) {
                for stmt in builder.force_terminate_and_take_statements() {
                    add_statement(stmt, &mut items);
                }
                continue;
            }

            // Standalone comment handling (format mode only)
            if builder.is_idle() && builder.current_is_empty() {
                if trimmed.starts_with("--") || sql_text::is_sqlplus_comment_line(trimmed) {
                    items.push(FormatItem::Statement(line.to_string()));
                    continue;
                }
                if trimmed.starts_with("/*") {
                    let mut comment = String::new();
                    let mut trailing_after_comment: Option<String> = None;
                    let extract_trailing_segment = |trailing_raw: &str| {
                        let trailing_trimmed = trailing_raw.trim_start();
                        if trailing_trimmed.is_empty() {
                            None
                        } else {
                            Some(trailing_trimmed.to_string())
                        }
                    };
                    let is_standalone_trailing_item = |trailing_line: &str| {
                        if trailing_line == "/" {
                            return true;
                        }

                        if let Some(command) = Self::parse_tool_command(trailing_line) {
                            return !matches!(command, ToolCommand::Unsupported { .. });
                        }

                        false
                    };

                    if let Some(close_idx) = line.find("*/") {
                        let close_end = close_idx + 2;
                        comment.push_str(&line[..close_end]);
                        let trailing_raw = &line[close_end..];
                        if let Some(trailing_line) = extract_trailing_segment(trailing_raw) {
                            if is_standalone_trailing_item(&trailing_line) {
                                trailing_after_comment = Some(trailing_line);
                            } else {
                                comment.push('\n');
                                comment.push_str(&trailing_line);
                            }
                        }
                    } else {
                        comment.push_str(line);
                        for next_line in lines.by_ref() {
                            if let Some(close_idx) = next_line.find("*/") {
                                let close_end = close_idx + 2;
                                comment.push('\n');
                                comment.push_str(&next_line[..close_end]);
                                let trailing_raw = &next_line[close_end..];
                                if let Some(trailing_line) = extract_trailing_segment(trailing_raw)
                                {
                                    if is_standalone_trailing_item(&trailing_line) {
                                        trailing_after_comment = Some(trailing_line);
                                    } else {
                                        comment.push('\n');
                                        comment.push_str(&trailing_line);
                                    }
                                }
                                break;
                            }

                            comment.push('\n');
                            comment.push_str(next_line);
                        }
                    }
                    items.push(FormatItem::Statement(comment));

                    if let Some(trailing_line) = trailing_after_comment {
                        items.extend(Self::split_format_items(&trailing_line));
                    }
                    continue;
                }
            }

            if builder.block_depth() == 0 && !builder.in_create_plsql() {
                if let Some((statement_segment, trailing_comment)) =
                    Self::split_inline_trailing_line_comment_after_semicolon(line)
                {
                    let statement_trimmed = statement_segment.trim();
                    let mut line_items: Vec<FormatItem> = Vec::new();
                    Self::process_split_line(
                        statement_segment,
                        statement_trimmed,
                        &mut builder,
                        &mut sqlblanklines_enabled,
                        &mut line_items,
                        &mut add_statement,
                        &mut |cmd: ToolCommand, raw_line: &str, items: &mut Vec<FormatItem>| {
                            if matches!(cmd, ToolCommand::Prompt { .. }) {
                                items.push(FormatItem::Verbatim(raw_line.to_string()));
                            } else {
                                items.push(FormatItem::ToolCommand(cmd));
                            }
                        },
                        &mut |items: &mut Vec<FormatItem>, _| items.push(FormatItem::Slash),
                    );

                    if let Some(FormatItem::Statement(statement)) = line_items.last_mut() {
                        if !statement.trim_end().ends_with(';') {
                            statement.push(';');
                        }
                        if !statement.ends_with(' ') {
                            statement.push(' ');
                        }
                        statement.push_str(trailing_comment.trim_start());
                        items.extend(line_items);
                        continue;
                    }

                    items.extend(line_items);
                    items.push(FormatItem::Statement(
                        trailing_comment.trim_start().to_string(),
                    ));
                    continue;
                }
            }

            // Delegate to the shared termination-check sequence
            Self::process_split_line(
                line,
                trimmed,
                &mut builder,
                &mut sqlblanklines_enabled,
                &mut items,
                &mut add_statement,
                &mut |cmd: ToolCommand, raw_line: &str, items: &mut Vec<FormatItem>| {
                    if matches!(cmd, ToolCommand::Prompt { .. }) {
                        items.push(FormatItem::Verbatim(raw_line.to_string()));
                    } else {
                        items.push(FormatItem::ToolCommand(cmd));
                    }
                },
                &mut |items: &mut Vec<FormatItem>, _| items.push(FormatItem::Slash),
            );
        }

        for stmt in builder.finalize_and_take_statements() {
            add_statement(stmt, &mut items);
        }
        Self::merge_fragmented_standalone_routine_format_items(items)
    }

    fn split_inline_trailing_line_comment_after_semicolon(line: &str) -> Option<(&str, &str)> {
        let bytes = line.as_bytes();
        let mut idx = 0usize;
        let mut last_semicolon_idx: Option<usize> = None;
        let mut in_single_quote = false;
        let mut in_double_quote = false;

        while idx < bytes.len() {
            let current = bytes[idx];

            if in_single_quote {
                if current == b'\'' {
                    if bytes.get(idx + 1) == Some(&b'\'') {
                        idx += 2;
                        continue;
                    }
                    in_single_quote = false;
                }
                idx += 1;
                continue;
            }

            if in_double_quote {
                if current == b'"' {
                    in_double_quote = false;
                }
                idx += 1;
                continue;
            }

            if current == b'\'' {
                in_single_quote = true;
                idx += 1;
                continue;
            }

            if current == b'"' {
                in_double_quote = true;
                idx += 1;
                continue;
            }

            if current == b'/' && bytes.get(idx + 1) == Some(&b'*') {
                idx += 2;
                while idx + 1 < bytes.len() {
                    if bytes[idx] == b'*' && bytes[idx + 1] == b'/' {
                        idx += 2;
                        break;
                    }
                    idx += 1;
                }
                continue;
            }

            if current == b';' {
                last_semicolon_idx = Some(idx);
                idx += 1;
                continue;
            }

            if current == b'-' && bytes.get(idx + 1) == Some(&b'-') {
                let semicolon_idx = last_semicolon_idx?;
                let between = line.get(semicolon_idx + 1..idx)?;
                if between.trim().is_empty() {
                    return Some((line.get(..=semicolon_idx)?, line.get(idx..)?));
                }
                return None;
            }

            idx += 1;
        }

        None
    }

    fn can_collect_multiline_tool_command(builder: &SqlParserEngine) -> bool {
        builder.is_idle()
            && builder.current_is_empty()
            && builder.block_depth() == 0
            && builder.paren_depth() == 0
    }

    fn collect_multiline_tool_command<'a, I>(
        first_line: &'a str,
        lines: &mut std::iter::Peekable<I>,
    ) -> Option<String>
    where
        I: Iterator<Item = &'a str> + Clone,
    {
        let trimmed = first_line.trim();
        let parsed = Self::parse_tool_command(trimmed)?;
        if !Self::tool_command_can_continue(trimmed, &parsed) {
            return None;
        }

        let mut candidate = trimmed.to_string();
        let preview = lines.clone();
        let mut consumed = 0usize;

        for next_line in preview {
            if !Self::is_tool_command_continuation_line(next_line) {
                break;
            }

            candidate.push('\n');
            candidate.push_str(next_line.trim());
            consumed = consumed.saturating_add(1);

            let Some(parsed_candidate) = Self::parse_tool_command(&candidate) else {
                break;
            };

            if Self::tool_command_can_continue(&candidate, &parsed_candidate) {
                continue;
            }

            for _ in 0..consumed {
                let _ = lines.next();
            }
            return Some(candidate);
        }

        if consumed == 0 {
            return None;
        }

        for _ in 0..consumed {
            let _ = lines.next();
        }
        Some(candidate)
    }

    fn tool_command_can_continue(raw: &str, command: &ToolCommand) -> bool {
        match command {
            ToolCommand::Unsupported { message, .. } => {
                Self::unsupported_tool_command_can_continue(message)
            }
            ToolCommand::SetServerOutput {
                enabled: true,
                size: None,
                unlimited: false,
            } => {
                let upper = raw.to_ascii_uppercase();
                upper.contains("SERVEROUTPUT") && upper.contains("SIZE")
            }
            ToolCommand::ShowErrors {
                object_type: None,
                object_name: None,
            } => true,
            ToolCommand::WheneverSqlError { action: None, .. } => true,
            _ => false,
        }
    }

    fn unsupported_tool_command_can_continue(message: &str) -> bool {
        let lower = message.to_ascii_lowercase();
        lower.contains(" requires ")
            || lower.starts_with("requires ")
            || lower.contains("requires:")
            || lower.contains("syntax:")
            || lower.contains("expected:")
            || lower.contains("cannot be empty")
            || lower.contains("without script path")
    }

    fn is_tool_command_continuation_line(line: &str) -> bool {
        let trimmed = line.trim();
        if trimmed.is_empty()
            || sql_text::is_sqlplus_comment_line(trimmed)
            || crate::sql_parser_engine::line_starts_with_consumed_slash_terminator(line)
            || sql_text::is_auto_terminated_tool_command(trimmed)
        {
            return false;
        }

        sql_text::first_meaningful_word(trimmed)
            .is_none_or(|word| !sql_text::is_statement_head_keyword(word))
    }

    fn merge_fragmented_standalone_routine_format_items(items: Vec<FormatItem>) -> Vec<FormatItem> {
        let mut merged: Vec<FormatItem> = Vec::with_capacity(items.len());
        let mut index = 0usize;

        while index < items.len() {
            if let Some((combined, end_index)) =
                Self::combine_fragmented_standalone_routine_format_statement(&items, index)
            {
                merged.push(FormatItem::Statement(combined));
                index = end_index + 1;
                continue;
            }

            if let Some(item) = items.get(index) {
                merged.push(item.clone());
            }
            index += 1;
        }

        merged
    }

    fn combine_fragmented_standalone_routine_format_statement(
        items: &[FormatItem],
        start_index: usize,
    ) -> Option<(String, usize)> {
        let statement = match items.get(start_index) {
            Some(FormatItem::Statement(statement)) => statement,
            _ => return None,
        };

        let routine_name = Self::extract_standalone_routine_name(statement)?;
        if Self::statement_has_matching_end_label(statement, routine_name.as_str()) {
            return None;
        }

        let mut end_index = start_index + 1;
        while end_index < items.len() {
            match items.get(end_index) {
                Some(FormatItem::Statement(next_statement)) => {
                    if Self::is_orphan_end_label_statement_matching_routine(
                        next_statement,
                        routine_name.as_str(),
                    ) {
                        return Self::combine_format_statement_range(items, start_index, end_index)
                            .map(|combined| (combined, end_index));
                    }

                    if Self::starts_new_top_level_create_statement(next_statement) {
                        return None;
                    }
                }
                Some(FormatItem::Slash)
                | Some(FormatItem::ToolCommand(_))
                | Some(FormatItem::Verbatim(_))
                | None => return None,
            }
            end_index += 1;
        }

        None
    }

    fn combine_format_statement_range(
        items: &[FormatItem],
        start_index: usize,
        end_index: usize,
    ) -> Option<String> {
        let fragments = items[start_index..=end_index]
            .iter()
            .filter_map(|item| match item {
                FormatItem::Statement(statement) => Some(statement.trim()),
                FormatItem::ToolCommand(_) | FormatItem::Verbatim(_) | FormatItem::Slash => None,
            })
            .filter(|statement| !statement.is_empty())
            .collect::<Vec<_>>();

        if fragments.is_empty() {
            return None;
        }

        let mut combined = fragments.join(";\n");
        combined.push(';');
        Some(combined)
    }

    /// Core split loop used by `split_script_items`.
    ///
    /// `split_format_items` handles standalone comments before delegating each
    /// non-comment line to `process_split_line` directly.
    fn split_items_core<T>(
        sql: &str,
        items: &mut Vec<T>,
        mut add_statement: impl FnMut(String, &mut Vec<T>),
        mut on_tool_command: impl FnMut(ToolCommand, &str, &mut Vec<T>),
        mut on_slash: impl FnMut(&mut Vec<T>, &SqlParserEngine),
    ) {
        let mut builder = SqlParserEngine::new();
        let mut sqlblanklines_enabled = true;

        let mut lines = sql.lines().peekable();
        while let Some(line) = lines.next() {
            let logical_line = if Self::can_collect_multiline_tool_command(&builder) {
                Self::collect_multiline_tool_command(line, &mut lines)
            } else {
                None
            };
            let line = logical_line.as_deref().unwrap_or(line);
            let trimmed = line.trim();

            // Blank-line termination (SET SQLBLANKLINES OFF)
            if Self::should_force_terminate_on_blank_line(
                sqlblanklines_enabled,
                trimmed,
                builder.is_idle(),
                builder.block_depth(),
                builder.current_is_empty(),
            ) {
                for stmt in builder.force_terminate_and_take_statements() {
                    add_statement(stmt, items);
                }
                continue;
            }

            Self::process_split_line(
                line,
                trimmed,
                &mut builder,
                &mut sqlblanklines_enabled,
                items,
                &mut add_statement,
                &mut on_tool_command,
                &mut on_slash,
            );
        }

        for stmt in builder.finalize_and_take_statements() {
            add_statement(stmt, items);
        }
    }

    /// Shared per-line termination-check sequence.
    ///
    /// Handles: incomplete CREATE recovery, slash termination, lone semicolon,
    /// tool-command detection, and parser engine feeding.  Callers are
    /// responsible for blank-line termination and any mode-specific
    /// pre-processing (e.g. standalone comment collection in format mode).
    fn process_split_line<T>(
        line: &str,
        trimmed: &str,
        builder: &mut SqlParserEngine,
        sqlblanklines_enabled: &mut bool,
        items: &mut Vec<T>,
        add_statement: &mut impl FnMut(String, &mut Vec<T>),
        on_tool_command: &mut impl FnMut(ToolCommand, &str, &mut Vec<T>),
        on_slash: &mut impl FnMut(&mut Vec<T>, &SqlParserEngine),
    ) {
        let mut parser_is_top_level = builder.block_depth() == 0 && builder.paren_depth() == 0;

        builder.prepare_splitter_line_boundary(line);
        match builder
            .state
            .splitter_line_boundary_action_for_line(line, builder.current_is_empty())
        {
            LineBoundaryAction::None => {}
            LineBoundaryAction::SplitBeforeLine => {
                if !builder.current_is_empty() {
                    for stmt in builder.force_terminate_and_take_statements() {
                        add_statement(stmt, items);
                    }
                }
                parser_is_top_level = builder.block_depth() == 0 && builder.paren_depth() == 0;
            }
            LineBoundaryAction::SplitAndConsumeLine => {
                if !builder.current_is_empty() {
                    for stmt in builder.force_terminate_and_take_statements() {
                        add_statement(stmt, items);
                    }
                }
                on_slash(items, builder);
                return;
            }
            LineBoundaryAction::ConsumeLine => {
                on_slash(items, builder);
                return;
            }
        }

        // Lone semicolon after CREATE PL/SQL (prevents `;;`)
        if Self::should_force_terminate_lone_semicolon(
            builder.is_idle(),
            trimmed,
            builder.in_create_plsql(),
            builder.block_depth(),
            builder.current_is_empty(),
        ) {
            for stmt in builder.force_terminate_and_take_statements() {
                add_statement(stmt, items);
            }
            return;
        }

        let is_set_clause = Self::is_set_clause_line(trimmed);
        let is_alter_set_clause = is_set_clause && builder.starts_with_alter_set_context();
        let is_sql_set_statement = Self::is_sql_set_statement_line(trimmed);
        let is_sql_set_clause_context = is_alter_set_clause || is_sql_set_statement;

        // ORDER BY modifiers (DESC, ASC, NULLS FIRST/LAST) on their own line
        // must not be mistaken for SQL*Plus tool commands (e.g. DESC → DESCRIBE).
        let is_order_by_modifier_line = Self::is_order_by_modifier_line(trimmed)
            && !builder.current_is_empty()
            && builder.current_has_order_by_context();

        // Tool command appearing after a slash-terminable open statement
        if builder.is_idle()
            && !builder.current_is_empty()
            && builder.paren_depth() == 0
            && builder.can_terminate_on_slash()
            && !is_sql_set_clause_context
            && !is_order_by_modifier_line
            && Self::parse_tool_command(trimmed).is_some()
        {
            for stmt in builder.force_terminate_and_take_statements() {
                add_statement(stmt, items);
            }
            parser_is_top_level = builder.block_depth() == 0 && builder.paren_depth() == 0;
        }

        // Tool command with an open (non-empty) statement
        if Self::should_try_tool_command_with_open_statement(
            builder.is_idle(),
            builder.current_is_empty(),
            parser_is_top_level,
            is_sql_set_clause_context,
        ) && !is_order_by_modifier_line
        {
            if let Some(command) = Self::parse_tool_command(trimmed) {
                for stmt in builder.force_terminate_and_take_statements() {
                    add_statement(stmt, items);
                }
                if let ToolCommand::SetSqlBlankLines { enabled } = &command {
                    *sqlblanklines_enabled = *enabled;
                }
                on_tool_command(command, line, items);
                return;
            }
        }

        // Tool command without an open statement
        if Self::should_try_tool_command_without_open_statement(
            builder.is_idle(),
            builder.current_is_empty(),
            parser_is_top_level,
        ) && !is_sql_set_statement
        {
            if let Some(command) = Self::parse_tool_command(trimmed) {
                if let ToolCommand::SetSqlBlankLines { enabled } = &command {
                    *sqlblanklines_enabled = *enabled;
                }
                on_tool_command(command, line, items);
                return;
            }
        }

        // Feed line to the parser engine
        for stmt in builder.process_line_and_take_statements(line) {
            add_statement(stmt, items);
        }
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
            if Self::looks_like_match_recognize_define_clause(rest) {
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

        if Self::is_word_command(&upper, "HOST") || trimmed.starts_with('!') {
            return Some(ToolCommand::Unsupported {
                raw: trimmed.to_string(),
                message: "HOST command is not supported in this client.".to_string(),
                is_error: true,
            });
        }

        if Self::is_word_command(&upper, "STARTUP")
            || Self::is_word_command(&upper, "SHUTDOWN")
            || Self::is_word_command(&upper, "ARCHIVE")
            || Self::is_word_command(&upper, "RECOVER")
        {
            return Some(ToolCommand::Unsupported {
                raw: trimmed.to_string(),
                message: "SQL*Plus admin command is not supported in this client.".to_string(),
                is_error: true,
            });
        }

        if Self::is_word_command(&upper, "TIMING")
            || Self::is_word_command(&upper, "TTITLE")
            || Self::is_word_command(&upper, "BTITLE")
            || Self::is_word_command(&upper, "REPHEADER")
            || Self::is_word_command(&upper, "REPFOOTER")
        {
            return Some(ToolCommand::Unsupported {
                raw: trimmed.to_string(),
                message: "SQL*Plus report command is not supported in this client.".to_string(),
                is_error: true,
            });
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

        if Self::is_sqlplus_set_command(upper.as_str()) {
            return Some(ToolCommand::Unsupported {
                raw: trimmed.to_string(),
                message: "SQL*Plus SET command is not supported in this client.".to_string(),
                is_error: true,
            });
        }

        if trimmed.starts_with("@@")
            || trimmed.starts_with('@')
            || Self::is_start_script_command(trimmed)
            || Self::is_run_script_command(trimmed)
        {
            return Some(Self::parse_script_command(trimmed));
        }

        if upper == "RUN" || upper == "R" {
            return Some(ToolCommand::Unsupported {
                raw: trimmed.to_string(),
                message: "RUN without script path is not supported in this client.".to_string(),
                is_error: true,
            });
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

        if Self::is_connect_command(trimmed) || upper == "CONN" || upper.starts_with("CONN ") {
            return Some(Self::parse_connect_command(trimmed));
        }

        if upper == "DISCONNECT" || upper == "DISC" {
            return Some(ToolCommand::Disconnect);
        }

        if upper == "PASSWORD"
            || upper.starts_with("PASSWORD ")
            || upper == "PASSW"
            || upper.starts_with("PASSW ")
        {
            return Some(ToolCommand::Unsupported {
                raw: trimmed.to_string(),
                message: "PASSWORD command is not supported in this client.".to_string(),
                is_error: true,
            });
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
        while idx < tokens.len() {
            if tokens[idx].eq_ignore_ascii_case("SIZE") {
                let Some(size_val) = tokens.get(idx + 1) else {
                    return ToolCommand::Unsupported {
                        raw: raw.to_string(),
                        message: "SET SERVEROUTPUT SIZE must be a number or UNLIMITED.".to_string(),
                        is_error: true,
                    };
                };
                if size_val.eq_ignore_ascii_case("UNLIMITED") {
                    unlimited = true;
                } else {
                    match (*size_val).parse::<u32>() {
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
        Self::parse_set_on_off_command(raw, "SET ERRORCONTINUE", |enabled| {
            ToolCommand::SetErrorContinue { enabled }
        })
    }

    fn parse_autocommit_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET AUTOCOMMIT", |enabled| {
            ToolCommand::SetAutoCommit { enabled }
        })
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
        Self::parse_set_on_off_command(raw, "SET SCAN", |enabled| ToolCommand::SetScan { enabled })
    }

    fn parse_verify_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET VERIFY", |enabled| ToolCommand::SetVerify {
            enabled,
        })
    }

    fn parse_echo_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET ECHO", |enabled| ToolCommand::SetEcho { enabled })
    }

    fn parse_timing_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET TIMING", |enabled| ToolCommand::SetTiming {
            enabled,
        })
    }

    fn parse_feedback_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET FEEDBACK", |enabled| ToolCommand::SetFeedback {
            enabled,
        })
    }

    fn parse_heading_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET HEADING", |enabled| ToolCommand::SetHeading {
            enabled,
        })
    }

    fn parse_pagesize_command(raw: &str) -> ToolCommand {
        Self::parse_set_number_command(raw, "SET PAGESIZE", |size| ToolCommand::SetPageSize {
            size,
        })
    }

    fn parse_linesize_command(raw: &str) -> ToolCommand {
        Self::parse_set_number_command(raw, "SET LINESIZE", |size| ToolCommand::SetLineSize {
            size,
        })
    }

    fn parse_trimspool_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET TRIMSPOOL", |enabled| ToolCommand::SetTrimSpool {
            enabled,
        })
    }

    fn parse_trimout_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET TRIMOUT", |enabled| ToolCommand::SetTrimOut {
            enabled,
        })
    }

    fn parse_sqlblanklines_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET SQLBLANKLINES", |enabled| {
            ToolCommand::SetSqlBlankLines { enabled }
        })
    }

    fn parse_tab_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET TAB", |enabled| ToolCommand::SetTab { enabled })
    }

    fn parse_set_on_off_command<F>(
        raw: &str,
        command_name: &str,
        enabled_to_command: F,
    ) -> ToolCommand
    where
        F: FnOnce(bool) -> ToolCommand,
    {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        let Some(mode) = tokens.get(2) else {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: format!("{command_name} requires ON or OFF."),
                is_error: true,
            };
        };

        if mode.eq_ignore_ascii_case("ON") {
            return enabled_to_command(true);
        }

        if mode.eq_ignore_ascii_case("OFF") {
            return enabled_to_command(false);
        }

        ToolCommand::Unsupported {
            raw: raw.to_string(),
            message: format!("{command_name} supports only ON or OFF."),
            is_error: true,
        }
    }

    fn is_sqlplus_set_command(upper_line: &str) -> bool {
        if !upper_line.starts_with("SET") {
            return false;
        }

        let mut parts = upper_line.split_whitespace();
        let Some(first) = parts.next() else {
            return false;
        };

        if first != "SET" {
            return false;
        }

        let Some(second) = parts.next() else {
            return false;
        };

        sql_text::is_sqlplus_set_option_keyword(second)
    }

    fn parse_set_number_command<F>(
        raw: &str,
        command_name: &str,
        value_to_command: F,
    ) -> ToolCommand
    where
        F: FnOnce(u32) -> ToolCommand,
    {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        let Some(raw_value) = tokens.get(2) else {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: format!("{command_name} requires a number."),
                is_error: true,
            };
        };

        match raw_value.parse::<u32>() {
            Ok(value) => value_to_command(value),
            Err(_) => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: format!("{command_name} requires a number."),
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
            let remainder = trimmed.trim_start_matches("@@").trim();
            (true, "@@", Self::extract_script_path(remainder))
        } else if trimmed.starts_with('@') {
            let remainder = trimmed.trim_start_matches('@').trim();
            (false, "@", Self::extract_script_path(remainder))
        } else if Self::is_start_script_command(trimmed) {
            let start = Self::remainder_after_first_word(trimmed).unwrap_or(trimmed.len());
            let remainder = trimmed.get(start..).unwrap_or_default().trim();
            (false, "START", Self::extract_script_path(remainder))
        } else if Self::is_run_script_command(trimmed) {
            let start = Self::remainder_after_first_word(trimmed).unwrap_or(trimmed.len());
            let remainder = trimmed.get(start..).unwrap_or_default().trim();
            (false, "RUN", Self::extract_script_path(remainder))
        } else {
            (false, "@", "")
        };

        if path.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: if command_label == "START" {
                    "START requires a path.".to_string()
                } else if command_label == "RUN" {
                    "RUN requires a path.".to_string()
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

    fn remainder_after_first_word(line: &str) -> Option<usize> {
        let mut in_word = false;
        for (idx, ch) in line.char_indices() {
            if !in_word {
                if ch.is_whitespace() {
                    continue;
                }
                in_word = true;
                continue;
            }

            if ch.is_whitespace() {
                return Some(idx);
            }
        }

        in_word.then_some(line.len())
    }

    fn extract_script_path(remainder: &str) -> &str {
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut idx = 0usize;

        while idx < remainder.len() {
            let tail = match remainder.get(idx..) {
                Some(value) => value,
                None => break,
            };
            let Some(ch) = tail.chars().next() else {
                break;
            };

            if !in_single_quote && !in_double_quote {
                if tail.starts_with("--") {
                    return remainder.get(..idx).unwrap_or_default().trim();
                }
                if tail.starts_with("/*") {
                    return remainder.get(..idx).unwrap_or_default().trim();
                }
            }

            if ch == '\'' && !in_double_quote {
                in_single_quote = !in_single_quote;
            } else if ch == '"' && !in_single_quote {
                in_double_quote = !in_double_quote;
            }

            idx += ch.len_utf8();
        }

        remainder.trim()
    }

    fn is_start_script_command(trimmed: &str) -> bool {
        let Some(first_word) = Self::next_meaningful_word(trimmed, 0) else {
            return false;
        };

        if !first_word.eq_ignore_ascii_case("START") {
            return false;
        }

        let second_word = Self::next_meaningful_word(trimmed, 1);
        if second_word.is_none() {
            return true;
        }

        if second_word.is_some_and(|word| word == "WITH") {
            return false;
        }

        // Hierarchical query clause `START WITH <expr>` must stay as SQL.
        let third_word = Self::next_meaningful_word(trimmed, 2);
        !(second_word.is_some_and(|word| word.eq_ignore_ascii_case("WITH")) && third_word.is_some())
    }

    fn is_connect_command(trimmed: &str) -> bool {
        let Some(first_word) = Self::next_meaningful_word(trimmed, 0) else {
            return false;
        };

        if !first_word.eq_ignore_ascii_case("CONNECT") {
            return false;
        }

        !Self::next_meaningful_word(trimmed, 1).is_some_and(|word| word.eq_ignore_ascii_case("BY"))
    }

    fn next_meaningful_word(line: &str, skip_words: usize) -> Option<&str> {
        let mut idx = 0usize;
        let mut seen_words = 0usize;

        while idx < line.len() {
            let ch = line.get(idx..)?.chars().next()?;
            let ch_len = ch.len_utf8();

            if ch.is_whitespace() {
                idx += ch_len;
                continue;
            }

            if line.get(idx..)?.starts_with("--") {
                return None;
            }

            if line.get(idx..)?.starts_with("/*") {
                let comment_start = idx + 2;
                let comment_tail = line.get(comment_start..)?;
                let comment_len = comment_tail.find("*/")?;
                idx = comment_start + comment_len + 2;
                continue;
            }

            let mut end = idx;
            while end < line.len() {
                let word_ch = line.get(end..)?.chars().next()?;
                if word_ch.is_whitespace()
                    || line.get(end..)?.starts_with("/*")
                    || line.get(end..)?.starts_with("--")
                {
                    break;
                }
                end += word_ch.len_utf8();
            }

            if seen_words == skip_words {
                return line.get(idx..end);
            }

            seen_words += 1;
            idx = end;
        }

        None
    }

    fn is_run_script_command(trimmed: &str) -> bool {
        let mut parts = trimmed.split_whitespace();
        let Some(first) = parts.next() else {
            return false;
        };

        let is_full = first.eq_ignore_ascii_case("RUN");
        let is_abbrev = first.eq_ignore_ascii_case("R");
        if !is_full && !is_abbrev {
            return false;
        }

        let Some(second) = parts.next() else {
            return false;
        };

        // When the abbreviation `R` is used (single char), reject patterns that
        // are clearly SQL rather than script paths:
        //   - `r AS (...)` → CTE definition
        //   - `r (col1, col2) AS ...` → recursive CTE with column list
        if is_abbrev && !is_full && (second.starts_with('(') || second.eq_ignore_ascii_case("AS")) {
            return false;
        }

        true
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

    fn looks_like_match_recognize_define_clause(rest: &str) -> bool {
        let mut words = rest.split_whitespace();
        let Some(first) = words.next() else {
            return false;
        };
        let Some(second) = words.next() else {
            return false;
        };
        second.eq_ignore_ascii_case("AS") && !first.contains('=')
    }
}

#[cfg(test)]
mod tests {
    use crate::sql_text;

    use super::{AutoFormatConditionRole, AutoFormatQueryRole, FormatItem, QueryExecutor};

    #[test]
    fn strip_extra_trailing_semicolons_preserves_plsql_end_terminator() {
        assert_eq!(
            QueryExecutor::strip_extra_trailing_semicolons("BEGIN NULL; END;;"),
            "BEGIN NULL; END;"
        );
        assert_eq!(
            QueryExecutor::strip_extra_trailing_semicolons("END pkg_name;;"),
            "END pkg_name;"
        );
    }

    #[test]
    fn strip_extra_trailing_semicolons_does_not_keep_semicolon_for_end_substring() {
        assert_eq!(
            QueryExecutor::strip_extra_trailing_semicolons("SELECT 'WEEKEND REPORT' FROM dual;;"),
            "SELECT 'WEEKEND REPORT' FROM dual"
        );
    }

    #[test]
    fn with_function_select_is_recognized_as_select_statement() {
        let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
SELECT f() FROM dual";

        assert!(
            QueryExecutor::is_select_statement(sql),
            "WITH FUNCTION ... SELECT should be treated as SELECT"
        );
    }

    #[test]
    fn with_procedure_select_is_recognized_as_select_statement() {
        let sql = "WITH
  PROCEDURE p IS
  BEGIN
    NULL;
  END;
SELECT 1 FROM dual";

        assert!(
            QueryExecutor::is_select_statement(sql),
            "WITH PROCEDURE ... SELECT should be treated as SELECT"
        );
    }

    #[test]
    fn with_function_followed_by_new_statement_head_is_not_select_statement() {
        let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'";

        assert!(
            !QueryExecutor::is_select_statement(sql),
            "WITH FUNCTION followed by non-query statement head should not be SELECT"
        );
    }

    #[test]
    fn with_function_end_label_and_trailing_ctes_is_recognized_as_select_statement() {
        let sql = "WITH
    FUNCTION calc_depth (p_id NUMBER) RETURN NUMBER IS v_depth NUMBER;

BEGIN
    SELECT MAX (LEVEL)
    INTO v_depth
    FROM org_tree
    START WITH parent_id IS NULL
    CONNECT BY PRIOR node_id = parent_id;
    RETURN v_depth;
END calc_depth;

recursive_tree (node_id, parent_id, node_name, DEPTH, PATH) AS (
    SELECT node_id,
        parent_id,
        node_name,
        1 AS DEPTH,
        CAST (node_name AS VARCHAR2 (4000)) AS PATH
    FROM org_tree
    WHERE parent_id IS NULL
)
SELECT *
FROM recursive_tree";

        assert!(
            QueryExecutor::is_select_statement(sql),
            "WITH FUNCTION + labeled END + trailing CTEs should be treated as SELECT"
        );
    }

    #[test]
    fn parse_tool_command_start_with_inline_comment_is_not_script_command() {
        assert!(
            QueryExecutor::parse_tool_command("START /*tree*/ WITH manager_id IS NULL").is_none(),
            "hierarchical START WITH with inline comment must remain SQL"
        );
    }

    #[test]
    fn parse_tool_command_bare_start_with_is_not_script_command() {
        assert!(
            QueryExecutor::parse_tool_command("START WITH").is_none(),
            "hierarchical START WITH header must remain SQL"
        );
    }

    #[test]
    fn parse_tool_command_match_recognize_define_clause_is_not_script_define() {
        assert!(
            QueryExecutor::parse_tool_command("DEFINE DOWN AS price < PREV(price)").is_none(),
            "MATCH_RECOGNIZE DEFINE clause must remain SQL instead of SQL*Plus DEFINE command"
        );
    }

    #[test]
    fn split_script_items_keeps_with_attached_to_single_letter_cte_name() {
        let sql =
            "WITH\n    r\n    AS\n    (\n        SELECT 1 AS id\n        FROM dual\n    )\nSELECT *\nFROM r\n;";
        let items = QueryExecutor::split_script_items(sql);

        assert_eq!(
            items.len(),
            1,
            "single-letter CTE should stay a single statement"
        );
        let statement = match items.first() {
            Some(super::ScriptItem::Statement(statement)) => statement,
            other => panic!("expected single statement item, got: {other:?}"),
        };
        assert!(
            statement.contains("WITH r")
                && statement.contains("AS")
                && statement.contains("SELECT *"),
            "single-letter CTE should remain attached to WITH, got:\n{}",
            statement
        );
    }

    #[test]
    fn split_items_keep_with_function_and_following_ctes_in_one_statement() {
        let sql = r#"WITH
    FUNCTION calc_depth (p_id NUMBER) RETURN NUMBER IS
        v_depth NUMBER;
    BEGIN
            SELECT MAX (LEVEL)
            INTO v_depth
            FROM org_tree
            START WITH parent_id IS NULL
            CONNECT BY PRIOR node_id = parent_id;
    RETURN v_depth;
END calc_depth;

recursive_tree (node_id, parent_id, node_name, DEPTH, PATH) AS (
    SELECT
        node_id,
        parent_id,
        node_name,
        1 AS DEPTH,
        CAST (node_name AS VARCHAR2 (4000)) AS PATH
    FROM org_tree
    WHERE parent_id IS NULL
    UNION ALL
    SELECT
        t.node_id,
        t.parent_id,
        t.node_name,
        rt.DEPTH + 1,
        rt.PATH || ' > ' || t.node_name
    FROM org_tree t
    JOIN recursive_tree rt
        ON t.parent_id = rt.node_id
    WHERE rt.DEPTH < calc_depth (t.node_id)
),
aggregated AS (
    SELECT
        parent_id,
        COUNT (*) AS child_count,
        MAX (DEPTH) AS max_depth,
        LISTAGG (node_name, ', ') WITHIN GROUP (ORDER BY node_name) AS children
    FROM recursive_tree
    WHERE DEPTH > 1
    GROUP BY parent_id
)
SELECT
    rt.*,
    a.child_count,
    a.max_depth,
    a.children
FROM recursive_tree rt
LEFT JOIN aggregated a
    ON rt.node_id = a.parent_id
ORDER BY rt.PATH;"#;

        let script_items = QueryExecutor::split_script_items(sql);
        assert_eq!(
            script_items.len(),
            1,
            "WITH FUNCTION + CTE chain must remain one executable statement, got: {script_items:?}"
        );

        let format_items = QueryExecutor::split_format_items(sql);
        let statement_count = format_items
            .iter()
            .filter(|item| matches!(item, FormatItem::Statement(_)))
            .count();
        assert_eq!(
            statement_count, 1,
            "format split must keep WITH FUNCTION + CTE chain together, got: {format_items:?}"
        );
    }

    #[test]
    fn parse_tool_command_connect_by_with_inline_comment_is_not_connect_command() {
        assert!(
            QueryExecutor::parse_tool_command(
                "CONNECT /*hierarchical*/ BY PRIOR employee_id = manager_id"
            )
            .is_none(),
            "hierarchical CONNECT BY with inline comment must remain SQL"
        );
    }

    #[test]
    fn line_auto_format_depths_adds_into_list_continuation_depth() {
        let sql = "SELECT col\nINTO v_a,\nv_b\nFROM dual;";
        let block_depths = QueryExecutor::line_block_depths(sql);
        let auto_depths = QueryExecutor::line_auto_format_depths(sql);

        assert_eq!(block_depths.len(), auto_depths.len());
        assert_eq!(auto_depths[2], block_depths[2].saturating_add(1));
    }

    #[test]
    fn line_auto_format_depths_adds_dml_comma_continuation_depth() {
        let sql = "UPDATE t\nSET a = 1,\nb = 2\nWHERE id = 1;";
        let block_depths = QueryExecutor::line_block_depths(sql);
        let auto_depths = QueryExecutor::line_auto_format_depths(sql);

        assert_eq!(block_depths.len(), auto_depths.len());
        assert_eq!(auto_depths[2], block_depths[2].saturating_add(1));
    }

    #[test]
    fn line_auto_format_depths_keeps_comma_continuation_after_comment_line() {
        let sql = "UPDATE t\nSET a = 1,\n-- keep comma depth\nb = 2\nWHERE id = 1;";
        let block_depths = QueryExecutor::line_block_depths(sql);
        let auto_depths = QueryExecutor::line_auto_format_depths(sql);

        assert_eq!(block_depths.len(), auto_depths.len());
        assert_eq!(auto_depths[3], block_depths[3].saturating_add(1));
    }

    #[test]
    fn auto_format_line_contexts_keep_with_main_query_on_same_base_after_then() {
        let sql = r#"BEGIN
  IF 1 = 1 THEN
    WITH filt AS (
      SELECT id
      FROM src_t
    )
    SELECT id
    INTO v_id
    FROM filt;
  END IF;
END;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();

        let then_idx = lines
            .iter()
            .position(|line| line.trim_start().ends_with("THEN"))
            .unwrap_or(0);
        let with_idx = lines
            .iter()
            .position(|line| line.trim_start().to_ascii_uppercase().starts_with("WITH "))
            .unwrap_or(0);
        let cte_select_idx = lines
            .iter()
            .position(|line| {
                line.trim_start()
                    .to_ascii_uppercase()
                    .starts_with("SELECT ID")
            })
            .unwrap_or(0);
        let main_select_idx = lines
            .iter()
            .enumerate()
            .filter_map(|(idx, line)| {
                line.trim_start()
                    .to_ascii_uppercase()
                    .starts_with("SELECT ID")
                    .then_some(idx)
            })
            .nth(1)
            .unwrap_or(0);
        let from_idx = lines
            .iter()
            .position(|line| {
                line.trim_start()
                    .to_ascii_uppercase()
                    .starts_with("FROM FILT")
            })
            .unwrap_or(0);

        assert_eq!(
            contexts[with_idx].auto_depth,
            contexts[then_idx].auto_depth.saturating_add(1),
            "WITH after THEN should start exactly one level deeper than its parent block"
        );
        assert_eq!(
            contexts[main_select_idx].auto_depth, contexts[with_idx].auto_depth,
            "Main SELECT after WITH should reuse the WITH base depth"
        );
        assert_eq!(
            contexts[from_idx].auto_depth, contexts[with_idx].auto_depth,
            "Clause starters inside the same query should stay on the shared base depth"
        );
        assert_eq!(
            contexts[cte_select_idx].auto_depth,
            contexts[with_idx].auto_depth.saturating_add(1),
            "CTE body SELECT should be one level deeper than the WITH base"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_nested_with_main_query_on_with_base_after_multiple_ctes() {
        let sql = r#"WITH outer_1 AS (
    WITH inner_1 AS (
        SELECT 1 AS id
        FROM dual
    ),
    inner_2 AS (
        SELECT id
        FROM inner_1
    )
    SELECT id
    FROM inner_2
),
outer_2 AS (
    SELECT id
    FROM outer_1
)
SELECT id
FROM outer_2;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();

        let inner_with_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("WITH inner_1 AS ("))
            .unwrap_or(0);
        let inner_cte_two_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("inner_2 AS ("))
            .unwrap_or(0);
        let inner_main_select_idx = lines
            .iter()
            .enumerate()
            .filter_map(|(idx, line)| (line.trim_start() == "SELECT id").then_some(idx))
            .nth(2)
            .unwrap_or(0);
        let inner_main_from_idx = lines
            .iter()
            .position(|line| line.trim_start() == "FROM inner_2")
            .unwrap_or(0);

        assert_eq!(
            contexts[inner_cte_two_idx].auto_depth, contexts[inner_with_idx].auto_depth,
            "sibling inner CTE headers should stay on the same nested WITH base depth"
        );
        assert_eq!(
            contexts[inner_main_select_idx].auto_depth, contexts[inner_with_idx].auto_depth,
            "main SELECT after nested WITH CTEs should return to the nested WITH base depth"
        );
        assert_eq!(
            contexts[inner_main_from_idx].auto_depth, contexts[inner_main_select_idx].auto_depth,
            "main FROM after nested WITH CTEs should stay on the same query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_normalize_overindented_cte_header_to_with_owner_depth() {
        let sql = r#"WITH
            dept_stats AS (
                SELECT deptno
                FROM emp
            )
SELECT *
FROM dept_stats;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let find_line = |needle: &str| -> usize {
            lines
                .iter()
                .position(|line| line.trim_start() == needle)
                .unwrap_or(0)
        };
        let with_idx = find_line("WITH");
        let cte_idx = find_line("dept_stats AS (");
        let select_idx = find_line("SELECT deptno");
        let close_idx = find_line(")");
        let main_select_idx = lines
            .iter()
            .enumerate()
            .skip(close_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "SELECT *")
            .map(|(idx, _)| idx)
            .unwrap_or(0);

        assert_eq!(
            contexts[cte_idx].auto_depth, contexts[with_idx].auto_depth,
            "overindented CTE header should snap back to the WITH owner depth"
        );
        assert_eq!(
            contexts[select_idx].auto_depth,
            contexts[cte_idx].auto_depth.saturating_add(1),
            "CTE body SELECT should stay exactly one level deeper than the CTE owner"
        );
        assert_eq!(
            contexts[close_idx].auto_depth, contexts[cte_idx].auto_depth,
            "CTE closing paren should realign with the CTE owner depth"
        );
        assert_eq!(
            contexts[main_select_idx].auto_depth, contexts[with_idx].auto_depth,
            "main SELECT after an overindented CTE should return to the WITH base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_case_after_then_on_branch_body_depth() {
        let sql = r#"SELECT
    CASE
        WHEN score > avg_score THEN
        CASE
            WHEN bonus >= 300 THEN 'TOP_WITH_BONUS'
            ELSE 'TOP_NO_BIG_BONUS'
        END
        ELSE
        CASE
            WHEN grade IN ('A', 'B') THEN 'MID_GOOD_GRADE'
            ELSE 'MID_OTHER'
        END
    END AS emp_class
FROM dual;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();

        let outer_when_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("WHEN score > avg_score THEN"))
            .unwrap_or(0);
        let first_inner_case_idx = lines
            .iter()
            .enumerate()
            .skip(outer_when_idx + 1)
            .find(|(_, line)| line.trim_start() == "CASE")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let outer_else_idx = lines
            .iter()
            .enumerate()
            .skip(first_inner_case_idx + 1)
            .find(|(_, line)| line.trim_start() == "ELSE")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let second_inner_case_idx = lines
            .iter()
            .enumerate()
            .skip(outer_else_idx + 1)
            .find(|(_, line)| line.trim_start() == "CASE")
            .map(|(idx, _)| idx)
            .unwrap_or(0);

        assert_eq!(
            contexts[first_inner_case_idx].auto_depth,
            contexts[outer_when_idx].auto_depth.saturating_add(1),
            "CASE after THEN should inherit one extra branch-body depth"
        );
        assert_eq!(
            contexts[second_inner_case_idx].auto_depth,
            contexts[outer_else_idx].auto_depth.saturating_add(1),
            "CASE after ELSE should inherit one extra branch-body depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_trigger_header_body_uses_structural_owner_depth() {
        let sql = r#"CREATE OR REPLACE TRIGGER trg_emp_audit
BEFORE INSERT OR UPDATE
ON emp
FOR EACH ROW
WHEN (NEW.sal > 0)
BEGIN
    :NEW.updated_at := SYSTIMESTAMP;
END;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let find_line = |prefix: &str| {
            lines
                .iter()
                .position(|line| line.trim_start().starts_with(prefix))
                .unwrap_or(0)
        };

        let create_idx = find_line("CREATE OR REPLACE TRIGGER");
        let before_idx = find_line("BEFORE INSERT OR UPDATE");
        let on_idx = find_line("ON emp");
        let for_each_idx = find_line("FOR EACH ROW");
        let when_idx = find_line("WHEN (NEW.sal > 0)");
        let begin_idx = find_line("BEGIN");
        let assign_idx = find_line(":NEW.updated_at := SYSTIMESTAMP;");

        assert_eq!(
            contexts[before_idx].auto_depth,
            contexts[create_idx].auto_depth.saturating_add(1),
            "trigger header body lines should indent one level deeper than CREATE TRIGGER"
        );
        assert_eq!(
            contexts[on_idx].auto_depth, contexts[before_idx].auto_depth,
            "ON line should stay on the trigger header body depth"
        );
        assert_eq!(
            contexts[for_each_idx].auto_depth, contexts[before_idx].auto_depth,
            "FOR EACH ROW should stay on the trigger header body depth"
        );
        assert_eq!(
            contexts[when_idx].auto_depth, contexts[before_idx].auto_depth,
            "WHEN header should stay on the trigger header body depth"
        );
        assert_eq!(
            contexts[begin_idx].auto_depth, contexts[create_idx].auto_depth,
            "BEGIN should close the trigger header owner and return to the trigger base depth"
        );
        assert_eq!(
            contexts[assign_idx].auto_depth,
            contexts[begin_idx].auto_depth.saturating_add(1),
            "trigger body statements should indent one level deeper than BEGIN"
        );
    }

    #[test]
    fn auto_format_line_contexts_forall_body_uses_structural_owner_depth() {
        let sql = r#"BEGIN
    IF v_ready THEN
        FORALL i IN 1..v_ids.COUNT
            UPDATE emp
            SET sal = v_sals(i)
            WHERE empno = v_ids(i);
    END IF;
END;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let find_line = |prefix: &str| {
            lines
                .iter()
                .position(|line| line.trim_start().starts_with(prefix))
                .unwrap_or(0)
        };

        let if_idx = find_line("IF v_ready THEN");
        let forall_idx = find_line("FORALL i IN 1..v_ids.COUNT");
        let update_idx = find_line("UPDATE emp");
        let set_idx = find_line("SET sal = v_sals(i)");
        let where_idx = find_line("WHERE empno = v_ids(i);");

        assert_eq!(
            contexts[forall_idx].auto_depth,
            contexts[if_idx].auto_depth.saturating_add(1),
            "FORALL should open one structural body level under the IF body"
        );
        assert_eq!(
            contexts[update_idx].auto_depth,
            contexts[forall_idx].auto_depth.saturating_add(1),
            "FORALL DML body should start one level deeper than the FORALL owner"
        );
        assert_eq!(
            contexts[set_idx].auto_depth, contexts[update_idx].auto_depth,
            "SET should stay on the UPDATE query base depth inside FORALL"
        );
        assert_eq!(
            contexts[where_idx].auto_depth, contexts[update_idx].auto_depth,
            "WHERE should stay on the UPDATE query base depth inside FORALL"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_recursive_cte_set_operator_select_on_cte_body_base() {
        let sql = r#"WITH r (node_id, parent_id, node_name, lvl, PATH) AS (
    SELECT
        node_id,
        parent_id,
        node_name,
        1 AS lvl,
        '/' || node_name AS PATH
    FROM oqt_t_tree
    WHERE parent_id IS NULL
    UNION ALL
        SELECT
            t.node_id,
            t.parent_id,
            t.node_name,
            r.lvl + 1,
            r.PATH || '/' || t.node_name
        FROM oqt_t_tree t
        JOIN r
            ON t.parent_id = r.node_id
)
SELECT *
FROM r
ORDER BY lvl,
    node_id;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let first_select_idx = lines
            .iter()
            .position(|line| line.trim() == "SELECT")
            .unwrap_or(0);
        let union_idx = lines
            .iter()
            .position(|line| line.trim() == "UNION ALL")
            .unwrap_or(0);
        let recursive_select_idx = lines
            .iter()
            .enumerate()
            .skip(union_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "SELECT")
            .map(|(idx, _)| idx)
            .unwrap_or(0);

        assert_eq!(
            contexts[union_idx].auto_depth,
            contexts[first_select_idx].auto_depth,
            "UNION ALL inside recursive CTE should stay on the same body base depth as the first SELECT"
        );
        assert_eq!(
            contexts[recursive_select_idx].parser_depth,
            contexts[first_select_idx].parser_depth,
            "Recursive branch SELECT should stay on the same structural parser depth as the first CTE SELECT"
        );
        assert_eq!(
            contexts[recursive_select_idx].auto_depth,
            contexts[first_select_idx].auto_depth,
            "Recursive branch SELECT should reuse the same CTE body base depth instead of becoming a nested child query"
        );
        assert_eq!(
            contexts[recursive_select_idx].query_base_depth,
            contexts[first_select_idx].query_base_depth,
            "Recursive branch SELECT should keep the same query base depth as the first CTE SELECT"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_non_recursive_cte_set_operator_select_on_cte_body_base() {
        let sql = r#"WITH src AS (
    SELECT
        10 AS dept_id,
        'DEV' AS dept_name
    FROM DUAL
    UNION ALL
        SELECT
            20 AS dept_id,
            'OPS' AS dept_name
        FROM DUAL
)
SELECT *
FROM src;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let first_select_idx = lines
            .iter()
            .position(|line| line.trim() == "SELECT")
            .unwrap_or(0);
        let union_idx = lines
            .iter()
            .position(|line| line.trim() == "UNION ALL")
            .unwrap_or(0);
        let second_select_idx = lines
            .iter()
            .enumerate()
            .skip(union_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "SELECT")
            .map(|(idx, _)| idx)
            .unwrap_or(0);

        assert_eq!(
            contexts[union_idx].auto_depth,
            contexts[first_select_idx].auto_depth,
            "UNION ALL inside a plain CTE should stay on the same body base depth as the first SELECT"
        );
        assert_eq!(
            contexts[second_select_idx].parser_depth,
            contexts[first_select_idx].parser_depth,
            "The second SELECT in a plain CTE compound query should stay on the same parser depth as the first SELECT"
        );
        assert_eq!(
            contexts[second_select_idx].auto_depth,
            contexts[first_select_idx].auto_depth,
            "The second SELECT in a plain CTE compound query should reuse the same body base depth instead of becoming a nested child query"
        );
        assert_eq!(
            contexts[second_select_idx].query_base_depth,
            contexts[first_select_idx].query_base_depth,
            "The second SELECT in a plain CTE compound query should keep the same query base depth as the first SELECT"
        );
    }

    #[test]
    fn auto_format_line_contexts_treat_recursive_cte_search_cycle_as_stable_query_base_clauses() {
        let sql = r#"WITH r (n) AS (
    SELECT 1
    FROM dual
    UNION ALL
    SELECT n + 1
    FROM r
    WHERE n < 3
)
                SEARCH DEPTH FIRST BY n SET ord
                CYCLE n SET is_cycle TO 'Y' DEFAULT 'N'
SELECT n, ord, is_cycle
FROM r;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let with_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("WITH r"))
            .unwrap_or(0);
        let search_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("SEARCH"))
            .unwrap_or(0);
        let cycle_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("CYCLE"))
            .unwrap_or(0);
        let select_idx = lines
            .iter()
            .enumerate()
            .skip(cycle_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start().starts_with("SELECT"))
            .map(|(idx, _)| idx)
            .unwrap_or(0);

        assert_eq!(
            contexts[search_idx].auto_depth,
            contexts[with_idx].auto_depth,
            "SEARCH clause should reuse the recursive WITH query-base depth instead of preserving manual overindent"
        );
        assert_eq!(
            contexts[cycle_idx].auto_depth,
            contexts[with_idx].auto_depth,
            "CYCLE clause should reuse the recursive WITH query-base depth instead of preserving manual overindent"
        );
        assert_eq!(
            contexts[search_idx].query_base_depth, contexts[with_idx].query_base_depth,
            "SEARCH clause should stay attached to the active recursive WITH query frame"
        );
        assert_eq!(
            contexts[cycle_idx].query_base_depth, contexts[with_idx].query_base_depth,
            "CYCLE clause should stay attached to the active recursive WITH query frame"
        );
        assert_eq!(
            contexts[select_idx].auto_depth,
            contexts[with_idx].auto_depth,
            "Main SELECT after SEARCH/CYCLE should still return to the recursive WITH query-base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_non_recursive_cte_except_select_on_cte_body_base() {
        let sql = r#"WITH src AS (
    SELECT
        dept_id
    FROM current_emp
    EXCEPT
    SELECT
        dept_id
    FROM former_emp
)
SELECT *
FROM src;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let first_select_idx = lines
            .iter()
            .position(|line| line.trim() == "SELECT")
            .unwrap_or(0);
        let except_idx = lines
            .iter()
            .position(|line| line.trim() == "EXCEPT")
            .unwrap_or(0);
        let second_select_idx = lines
            .iter()
            .enumerate()
            .skip(except_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "SELECT")
            .map(|(idx, _)| idx)
            .unwrap_or(0);

        assert_eq!(
            contexts[except_idx].auto_depth, contexts[first_select_idx].auto_depth,
            "EXCEPT inside a plain CTE should stay on the same body base depth as the first SELECT"
        );
        assert_eq!(
            contexts[second_select_idx].parser_depth,
            contexts[first_select_idx].parser_depth,
            "The second SELECT after EXCEPT in a plain CTE should stay on the same parser depth as the first SELECT"
        );
        assert_eq!(
            contexts[second_select_idx].auto_depth,
            contexts[first_select_idx].auto_depth,
            "The second SELECT after EXCEPT in a plain CTE should reuse the same body base depth instead of becoming a nested child query"
        );
        assert_eq!(
            contexts[second_select_idx].query_base_depth,
            contexts[first_select_idx].query_base_depth,
            "The second SELECT after EXCEPT in a plain CTE should keep the same query base depth as the first SELECT"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_nested_except_branch_select_on_condition_query_base() {
        let sql = r#"SELECT *
FROM dept d
WHERE EXISTS (
    SELECT d.deptno
    FROM dual
    EXCEPT
    SELECT e.deptno
    FROM emp e
);"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let first_select_idx = lines
            .iter()
            .position(|line| line.trim() == "SELECT d.deptno")
            .unwrap_or(0);
        let except_idx = lines
            .iter()
            .position(|line| line.trim() == "EXCEPT")
            .unwrap_or(0);
        let second_select_idx = lines
            .iter()
            .enumerate()
            .skip(except_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "SELECT e.deptno")
            .map(|(idx, _)| idx)
            .unwrap_or(0);

        assert!(
            contexts[first_select_idx].auto_depth > contexts[first_select_idx].parser_depth,
            "nested EXISTS child SELECT should already be offset from parser depth so the regression is observable"
        );
        assert_eq!(
            contexts[except_idx].auto_depth, contexts[first_select_idx].auto_depth,
            "EXCEPT inside a nested EXISTS query should stay on the child query base depth"
        );
        assert_eq!(
            contexts[second_select_idx].parser_depth,
            contexts[first_select_idx].parser_depth,
            "second SELECT after nested EXCEPT should remain on the same structural parser depth as the first child SELECT"
        );
        assert_eq!(
            contexts[second_select_idx].auto_depth,
            contexts[first_select_idx].auto_depth,
            "second SELECT after nested EXCEPT should reuse the child query base depth instead of falling back to raw parser depth"
        );
        assert_eq!(
            contexts[second_select_idx].query_base_depth,
            contexts[first_select_idx].query_base_depth,
            "nested EXCEPT branches should share the same query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_merge_branch_depths_on_branch_body_base() {
        let sql = r#"MERGE INTO emp_bonus b
    USING src_bonus s
    ON (b.empno = s.empno)
WHEN MATCHED THEN
    UPDATE SET b.bonus_amount = s.calc_bonus
WHERE s.sal > 0
    AND b.bonus_amount <> s.calc_bonus
    DELETE
WHERE s.sal < 500
WHEN NOT MATCHED THEN
    INSERT (empno)
    VALUES (s.empno);"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();

        let merge_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("MERGE INTO"))
            .unwrap_or(0);
        let using_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("USING"))
            .unwrap_or(0);
        let on_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("ON ("))
            .unwrap_or(0);
        let when_matched_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHEN MATCHED THEN")
            .unwrap_or(0);
        let update_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("UPDATE SET"))
            .unwrap_or(0);
        let update_where_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE s.sal > 0")
            .unwrap_or(0);
        let update_and_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND b.bonus_amount"))
            .unwrap_or(0);
        let delete_idx = lines
            .iter()
            .position(|line| line.trim_start() == "DELETE")
            .unwrap_or(0);
        let delete_where_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE s.sal < 500")
            .unwrap_or(0);
        let when_not_matched_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHEN NOT MATCHED THEN")
            .unwrap_or(0);
        let insert_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("INSERT ("))
            .unwrap_or(0);
        let values_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("VALUES ("))
            .unwrap_or(0);

        assert_eq!(
            contexts[using_idx].auto_depth, contexts[merge_idx].auto_depth,
            "MERGE USING should stay on the MERGE base depth"
        );
        assert_eq!(
            contexts[on_idx].auto_depth, contexts[merge_idx].auto_depth,
            "MERGE ON should stay on the MERGE base depth"
        );
        assert_eq!(
            contexts[when_matched_idx].auto_depth, contexts[merge_idx].auto_depth,
            "WHEN MATCHED should stay on the MERGE base depth"
        );
        assert_eq!(
            contexts[update_idx].auto_depth,
            contexts[when_matched_idx].auto_depth.saturating_add(1),
            "MERGE UPDATE action should be exactly one level deeper than WHEN MATCHED"
        );
        assert_eq!(
            contexts[update_where_idx].auto_depth, contexts[update_idx].auto_depth,
            "UPDATE WHERE should stay on the branch body depth"
        );
        assert_eq!(
            contexts[update_and_idx].auto_depth,
            contexts[update_where_idx].auto_depth.saturating_add(1),
            "branch condition continuations should be one level deeper than branch WHERE"
        );
        assert_eq!(
            contexts[delete_idx].auto_depth, contexts[update_idx].auto_depth,
            "DELETE should reuse the current branch body depth"
        );
        assert_eq!(
            contexts[delete_where_idx].auto_depth, contexts[delete_idx].auto_depth,
            "DELETE WHERE should stay on the DELETE branch depth"
        );
        assert_eq!(
            contexts[when_not_matched_idx].auto_depth, contexts[merge_idx].auto_depth,
            "WHEN NOT MATCHED should stay on the MERGE base depth"
        );
        assert_eq!(
            contexts[insert_idx].auto_depth,
            contexts[when_not_matched_idx].auto_depth.saturating_add(1),
            "MERGE INSERT action should be exactly one level deeper than WHEN NOT MATCHED"
        );
        assert_eq!(
            contexts[values_idx].auto_depth, contexts[insert_idx].auto_depth,
            "VALUES should stay on the INSERT branch depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_normalize_overindented_merge_branch_body_to_structural_depth() {
        let sql = r#"MERGE INTO target_table t
USING source_table s
ON (t.id = s.id)
WHEN MATCHED THEN
            UPDATE
                    SET t.name = s.name
WHEN NOT MATCHED THEN
            INSERT (id, name)
                    VALUES (s.id, s.name);"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let find_line = |needle: &str| -> usize {
            lines
                .iter()
                .position(|line| line.trim_start() == needle)
                .unwrap_or(0)
        };
        let when_matched_idx = find_line("WHEN MATCHED THEN");
        let update_idx = find_line("UPDATE");
        let set_idx = find_line("SET t.name = s.name");
        let when_not_matched_idx = find_line("WHEN NOT MATCHED THEN");
        let insert_idx = find_line("INSERT (id, name)");
        let values_idx = find_line("VALUES (s.id, s.name);");

        assert_eq!(
            contexts[update_idx].auto_depth,
            contexts[when_matched_idx].auto_depth.saturating_add(1),
            "overindented MERGE UPDATE action should stay exactly one level deeper than WHEN MATCHED"
        );
        assert_eq!(
            contexts[set_idx].auto_depth, contexts[update_idx].auto_depth,
            "SET in MERGE UPDATE branch should stay on the UPDATE branch depth"
        );
        assert_eq!(
            contexts[insert_idx].auto_depth,
            contexts[when_not_matched_idx].auto_depth.saturating_add(1),
            "overindented MERGE INSERT action should stay exactly one level deeper than WHEN NOT MATCHED"
        );
        assert_eq!(
            contexts[values_idx].auto_depth, contexts[insert_idx].auto_depth,
            "VALUES in MERGE INSERT branch should stay on the INSERT branch depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_normalize_overindented_merge_branch_headers_to_merge_base() {
        let sql = r#"MERGE INTO target_table t
USING source_table s
ON (t.id = s.id)
            WHEN MATCHED THEN
UPDATE
SET t.name = s.name
            WHEN NOT MATCHED THEN
INSERT (id, name)
VALUES (s.id, s.name);"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let find_line = |needle: &str| -> usize {
            lines
                .iter()
                .position(|line| line.trim_start() == needle)
                .unwrap_or(0)
        };
        let merge_idx = find_line("MERGE INTO target_table t");
        let when_matched_idx = find_line("WHEN MATCHED THEN");
        let update_idx = find_line("UPDATE");
        let set_idx = find_line("SET t.name = s.name");
        let when_not_matched_idx = find_line("WHEN NOT MATCHED THEN");
        let insert_idx = find_line("INSERT (id, name)");
        let values_idx = find_line("VALUES (s.id, s.name);");

        assert_eq!(
            contexts[when_matched_idx].auto_depth, contexts[merge_idx].auto_depth,
            "overindented WHEN MATCHED should snap back to the MERGE base depth"
        );
        assert_eq!(
            contexts[update_idx].auto_depth,
            contexts[when_matched_idx].auto_depth.saturating_add(1),
            "UPDATE under an overindented WHEN MATCHED should stay exactly one level deeper than the MERGE branch header"
        );
        assert_eq!(
            contexts[set_idx].auto_depth, contexts[update_idx].auto_depth,
            "SET in MERGE UPDATE branch should stay on the branch body depth"
        );
        assert_eq!(
            contexts[when_not_matched_idx].auto_depth, contexts[merge_idx].auto_depth,
            "overindented WHEN NOT MATCHED should snap back to the MERGE base depth"
        );
        assert_eq!(
            contexts[insert_idx].auto_depth,
            contexts[when_not_matched_idx].auto_depth.saturating_add(1),
            "INSERT under an overindented WHEN NOT MATCHED should stay exactly one level deeper than the MERGE branch header"
        );
        assert_eq!(
            contexts[values_idx].auto_depth, contexts[insert_idx].auto_depth,
            "VALUES in MERGE INSERT branch should stay on the branch body depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_for_update_on_select_base_depth() {
        let sql = r#"SELECT e.empno,
       e.sal
FROM emp e
FOR
UPDATE OF e.sal NOWAIT;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();

        let from_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("FROM "))
            .unwrap_or(0);
        let for_idx = lines
            .iter()
            .position(|line| line.trim_start() == "FOR")
            .unwrap_or(0);
        let update_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("UPDATE OF"))
            .unwrap_or(0);

        assert_eq!(
            contexts[for_idx].auto_depth, contexts[from_idx].auto_depth,
            "FOR header in SELECT FOR UPDATE should stay on query base depth"
        );
        assert_eq!(
            contexts[update_idx].auto_depth, contexts[from_idx].auto_depth,
            "split UPDATE line in SELECT FOR UPDATE should stay on query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_for_update_inline_comment_body_on_shared_clause_depth() {
        let sql = r#"SELECT e.empno
FROM emp e
FOR UPDATE -- lock mode
SKIP LOCKED;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let for_update_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("FOR UPDATE --"))
            .unwrap_or(0);
        let skip_locked_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SKIP LOCKED;")
            .unwrap_or(0);

        assert_eq!(
            contexts[skip_locked_idx].auto_depth,
            contexts[for_update_idx]
                .query_base_depth
                .unwrap_or(contexts[for_update_idx].auto_depth)
                .saturating_add(1),
            "line after `FOR UPDATE -- ...` should stay on the dedicated FOR UPDATE body depth"
        );
        assert_eq!(
            contexts[skip_locked_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "FOR UPDATE inline-comment continuation should stay marked as continuation"
        );
        assert_eq!(
            contexts[skip_locked_idx].query_base_depth, contexts[for_update_idx].query_base_depth,
            "FOR UPDATE inline-comment continuation should preserve the active query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_nested_join_and_condition_depths_on_query_base() {
        let sql = r#"SELECT D
FROM E
WHERE F IN (
    SELECT G
    FROM (
        SELECT H
        FROM J
        INNER JOIN K
            ON 1 = 1
                AND 2 = 2
                OR 3 = 3
        OUTER JOIN K
            ON 1 = 1
                AND 2 = 2
                OR 3 = 3
    ) I
);"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();

        let inner_select_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("SELECT H"))
            .unwrap_or(0);
        let from_j_idx = lines
            .iter()
            .position(|line| line.trim_start() == "FROM J")
            .unwrap_or(0);
        let inner_join_idx = lines
            .iter()
            .position(|line| line.trim_start() == "INNER JOIN K")
            .unwrap_or(0);
        let on_idx = lines
            .iter()
            .position(|line| line.trim_start() == "ON 1 = 1")
            .unwrap_or(0);
        let outer_join_idx = lines
            .iter()
            .position(|line| line.trim_start() == "OUTER JOIN K")
            .unwrap_or(0);

        let query_base_depth = contexts[inner_select_idx].auto_depth;
        assert_eq!(
            contexts[from_j_idx].auto_depth, query_base_depth,
            "Nested FROM should stay on the child query base depth"
        );
        assert_eq!(
            contexts[inner_join_idx].auto_depth, query_base_depth,
            "JOIN should reuse the child query base depth instead of falling back to parser depth"
        );
        assert_eq!(
            contexts[outer_join_idx].auto_depth, query_base_depth,
            "Subsequent JOIN branches should stay aligned to the same child query base"
        );
        assert_eq!(
            contexts[on_idx].auto_depth,
            query_base_depth.saturating_add(1),
            "ON should be one level deeper than the query base"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_incomplete_nested_join_on_one_level_deeper_than_join() {
        let sql = r#"select 1
from (a
    join (
        select 1
        from a
        inner join a
    on 1 = 1"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let inner_join_idx = lines
            .iter()
            .position(|line| line.trim_start().eq_ignore_ascii_case("inner join a"))
            .unwrap_or(0);
        let on_idx = lines
            .iter()
            .position(|line| line.trim_start().to_ascii_uppercase().starts_with("ON "))
            .unwrap_or(0);

        assert_eq!(
            contexts[on_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "Incomplete nested JOIN ON should still stay in continuation role"
        );
        assert_eq!(
            contexts[inner_join_idx].line_semantic,
            crate::db::AutoFormatLineSemantic::JoinClause,
            "INNER JOIN line should be classified once by the analyzer as a join clause"
        );
        assert_eq!(
            contexts[on_idx].line_semantic,
            crate::db::AutoFormatLineSemantic::JoinConditionClause,
            "ON line should be classified once by the analyzer as a join condition clause"
        );
        assert_eq!(
            contexts[on_idx].query_base_depth, contexts[inner_join_idx].query_base_depth,
            "Incomplete nested JOIN ON should keep the active child query base"
        );
        assert_eq!(
            contexts[on_idx].auto_depth,
            contexts[inner_join_idx].auto_depth.saturating_add(1),
            "Incomplete nested JOIN ON should stay exactly one structural level deeper than INNER JOIN"
        );
    }

    #[test]
    fn auto_format_line_contexts_indent_all_supported_child_query_heads_from_parent_base() {
        let scenarios = [
            (
                "VALUES",
                "SELECT *\nFROM (\n  VALUES (1), (2)\n) AS t(n);",
            ),
            (
                "INSERT",
                "SELECT *\nFROM (\n  INSERT INTO dst(id) SELECT id FROM src RETURNING id\n) q;",
            ),
            (
                "UPDATE",
                "SELECT *\nFROM (\n  UPDATE dst SET id = src.id FROM src WHERE dst.id = src.id RETURNING dst.id\n) q;",
            ),
            (
                "MERGE",
                "SELECT *\nFROM (\n  MERGE INTO dst d USING src s ON (d.id = s.id) WHEN MATCHED THEN UPDATE SET d.id = s.id\n) q;",
            ),
            (
                "TABLE",
                "SELECT *\nFROM (\n  TABLE(pkg_rows())\n) q;",
            ),
            (
                "CALL",
                "BEGIN\n  OPEN rc FOR (\n    CALL pkg_do_work()\n  );\nEND;",
            ),
        ];

        for (head, sql) in scenarios {
            let contexts = QueryExecutor::auto_format_line_contexts(sql);
            let lines: Vec<&str> = sql.lines().collect();
            let parent_idx = lines
                .iter()
                .position(|line| line.trim_start().ends_with('('))
                .unwrap_or(0);
            let head_idx = lines
                .iter()
                .position(|line| line.trim_start().to_ascii_uppercase().starts_with(head))
                .unwrap_or(0);

            assert_eq!(
                contexts[head_idx].auto_depth,
                contexts[parent_idx].auto_depth.saturating_add(1),
                "{head} child query head should inherit parent base depth + 1"
            );
        }
    }

    #[test]
    fn auto_format_line_contexts_keep_scalar_subquery_under_with_function_cte_on_parent_base() {
        let sql = r#"WITH
    FUNCTION fmt_mask (p_txt IN VARCHAR2) RETURN VARCHAR2 IS
    BEGIN
        RETURN p_txt;
    END fmt_mask,
    PROCEDURE noop (p_msg IN VARCHAR2) IS
    BEGIN
        NULL;
    END noop,
    base_emp AS (
        SELECT
            e.empno,
            (
                SELECT MAX (x.sal)
                FROM emp x
                WHERE x.deptno = e.deptno
            ) AS max_sal
        FROM emp e
    )
SELECT 1
FROM dual;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let cte_header_idx = lines
            .iter()
            .position(|line| line.trim_start() == "base_emp AS (")
            .unwrap_or(0);
        let cte_select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT")
            .unwrap_or(0);
        let scalar_open_idx = lines
            .iter()
            .position(|line| line.trim_start() == "(")
            .unwrap_or(0);
        let scalar_select_idx = lines
            .iter()
            .enumerate()
            .skip(scalar_open_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start().starts_with("SELECT MAX"))
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let scalar_from_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("FROM emp x"))
            .unwrap_or(0);

        assert_eq!(
            contexts[cte_select_idx].auto_depth,
            contexts[cte_header_idx].auto_depth.saturating_add(1),
            "CTE body SELECT should be exactly one level deeper than the CTE header base"
        );
        assert_eq!(
            contexts[scalar_select_idx].auto_depth,
            contexts[scalar_open_idx].auto_depth.saturating_add(1),
            "scalar subquery SELECT should be exactly one level deeper than its owner line base"
        );
        assert_eq!(
            contexts[scalar_from_idx].auto_depth, contexts[scalar_select_idx].auto_depth,
            "scalar subquery clauses should reuse the same query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_for_in_subquery_on_for_header_depth() {
        let sql = r#"BEGIN
    FOR rec IN
    (
        SELECT 1
        FROM dual
    ) LOOP
        NULL;
    END LOOP;
END;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();

        let for_idx = lines
            .iter()
            .position(|line| line.trim_start() == "FOR rec IN")
            .expect("formatted source should contain FOR header");
        let open_idx = lines
            .iter()
            .position(|line| line.trim_start() == "(")
            .expect("formatted source should contain split open paren");
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT 1")
            .expect("formatted source should contain child SELECT");
        let close_idx = lines
            .iter()
            .position(|line| line.trim_start() == ") LOOP")
            .expect("formatted source should contain close paren LOOP line");

        assert_eq!(
            contexts[open_idx].condition_header_line,
            Some(for_idx),
            "split IN open-paren line should retain the FOR owner"
        );
        assert_eq!(
            contexts[select_idx].condition_header_line,
            Some(for_idx),
            "child SELECT should stay attached to the FOR condition owner"
        );
        assert_eq!(
            contexts[close_idx].condition_role,
            AutoFormatConditionRole::Closer,
            "close paren line should be marked as a condition closer"
        );
        assert_eq!(
            contexts[select_idx].auto_depth,
            contexts[for_idx].auto_depth.saturating_add(1),
            "split FOR ... IN subquery should inherit the FOR header base depth"
        );
        assert_eq!(
            contexts[select_idx].query_base_depth,
            Some(contexts[for_idx].auto_depth.saturating_add(1)),
            "child query base depth should be anchored from the FOR header"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_case_when_exists_subquery_on_shared_condition_base() {
        let sql = r#"SELECT
    (
        SELECT MAX (b0.bonus_amt)
        FROM bonus_data b0
        WHERE b0.emp_id = x.emp_id
    ) AS latest_bonus,
    CASE
        WHEN x.salary > x.dept_avg_salary THEN
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM bonus_data b
                    WHERE b.emp_id = x.emp_id
                      AND b.bonus_amt >= 300
                ) THEN 'TOP_WITH_BONUS'
                ELSE 'TOP_NO_BIG_BONUS'
            END
        ELSE
            'MID_OTHER'
    END AS emp_class
FROM emp_data x;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();

        let exists_when_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHEN EXISTS (")
            .unwrap_or(0);
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT 1")
            .unwrap_or(0);
        let from_idx = lines
            .iter()
            .position(|line| line.trim_start() == "FROM bonus_data b")
            .unwrap_or(0);
        let where_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE b.emp_id = x.emp_id")
            .unwrap_or(0);
        let and_idx = lines
            .iter()
            .position(|line| line.trim_start() == "AND b.bonus_amt >= 300")
            .unwrap_or(0);
        let close_then_idx = lines
            .iter()
            .position(|line| line.trim_start() == ") THEN 'TOP_WITH_BONUS'")
            .unwrap_or(0);

        assert_eq!(
            contexts[exists_when_idx].condition_header_line,
            Some(exists_when_idx),
            "WHEN EXISTS line should start its own searched CASE condition owner"
        );
        assert_eq!(
            contexts[select_idx].condition_header_line,
            Some(exists_when_idx),
            "EXISTS child SELECT should stay attached to the inner WHEN EXISTS owner"
        );
        assert_eq!(
            contexts[select_idx].auto_depth,
            contexts[exists_when_idx].auto_depth.saturating_add(1),
            "EXISTS child SELECT should inherit the inner WHEN EXISTS base depth"
        );
        assert_eq!(
            contexts[from_idx].auto_depth, contexts[select_idx].auto_depth,
            "EXISTS child FROM should stay on the same query base depth as the child SELECT"
        );
        assert_eq!(
            contexts[where_idx].auto_depth, contexts[select_idx].auto_depth,
            "EXISTS child WHERE should stay on the same query base depth as the child SELECT"
        );
        assert_eq!(
            contexts[and_idx].query_base_depth, contexts[where_idx].query_base_depth,
            "AND continuation inside EXISTS should preserve the child query base depth"
        );
        assert_eq!(
            contexts[and_idx].auto_depth,
            contexts[where_idx].auto_depth.saturating_add(1),
            "AND continuation inside EXISTS should be exactly one level deeper than the child WHERE"
        );
        assert_eq!(
            contexts[close_then_idx].condition_role,
            AutoFormatConditionRole::Closer,
            "close-paren THEN line should be tracked as a condition closer"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_exists_open_paren_on_condition_owner_depth() {
        let sql = r#"SELECT e.empno
FROM emp e
WHERE EXISTS
(
    SELECT 1
    FROM bonus b
    WHERE b.empno = e.empno
)
AND e.status = 'A';"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let exists_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE EXISTS")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .position(|line| line.trim() == "(")
            .unwrap_or(0);
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT 1")
            .unwrap_or(0);
        let close_idx = lines
            .iter()
            .position(|line| line.trim() == ")")
            .unwrap_or(0);
        let and_idx = lines
            .iter()
            .position(|line| line.trim_start() == "AND e.status = 'A';")
            .unwrap_or(0);
        let expected_select_depth = sql_text::FormatQueryOwnerKind::Condition
            .auto_format_child_query_owner_base_depth(
                contexts[exists_idx].auto_depth,
                contexts[exists_idx].query_base_depth,
            )
            .saturating_add(1);
        assert_eq!(
            contexts[open_idx].next_query_head_depth,
            Some(expected_select_depth),
            "standalone open paren after WHERE EXISTS should preserve the owner's promoted child-query head depth"
        );
        assert_eq!(
            contexts[open_idx].auto_depth, contexts[exists_idx].auto_depth,
            "standalone open paren after WHERE EXISTS should stay on the completed owner depth"
        );
        assert_eq!(
            contexts[select_idx].auto_depth, expected_select_depth,
            "split EXISTS child SELECT should stay anchored to the condition owner base depth"
        );
        assert_eq!(
            contexts[and_idx].auto_depth,
            contexts[exists_idx].auto_depth.saturating_add(1),
            "AND after split EXISTS subquery should return to the outer condition continuation depth"
        );
        assert_eq!(
            contexts[close_idx].auto_depth, contexts[exists_idx].auto_depth,
            "split EXISTS closing paren should stay on the completed owner depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_not_exists_header_on_condition_depth() {
        let sql = r#"SELECT e.empno
FROM emp e
WHERE
NOT EXISTS
(
    SELECT 1
    FROM bonus b
    WHERE b.empno = e.empno
)
AND e.status = 'A';"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let where_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE")
            .unwrap_or(0);
        let not_exists_idx = lines
            .iter()
            .position(|line| line.trim_start() == "NOT EXISTS")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .position(|line| line.trim() == "(")
            .unwrap_or(0);
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT 1")
            .unwrap_or(0);
        let close_idx = lines
            .iter()
            .position(|line| line.trim() == ")")
            .unwrap_or(0);
        let and_idx = lines
            .iter()
            .position(|line| line.trim_start() == "AND e.status = 'A';")
            .unwrap_or(0);

        assert_eq!(
            contexts[not_exists_idx].auto_depth,
            contexts[where_idx].auto_depth.saturating_add(1),
            "split NOT EXISTS header should stay on the active condition depth instead of falling back to parser depth"
        );
        assert_eq!(
            contexts[open_idx].auto_depth, contexts[not_exists_idx].auto_depth,
            "split NOT EXISTS opener should stay aligned with the NOT EXISTS owner depth"
        );
        assert_eq!(
            contexts[select_idx].query_base_depth,
            Some(contexts[not_exists_idx].auto_depth.saturating_add(1)),
            "child SELECT under split NOT EXISTS should anchor its query base from the owner depth"
        );
        assert_eq!(
            contexts[select_idx].auto_depth,
            contexts[not_exists_idx].auto_depth.saturating_add(1),
            "child SELECT under split NOT EXISTS should stay exactly one level deeper than the owner depth"
        );
        assert_eq!(
            contexts[close_idx].auto_depth, contexts[not_exists_idx].auto_depth,
            "split NOT EXISTS closing paren should stay on the owner depth"
        );
        assert_eq!(
            contexts[and_idx].auto_depth,
            contexts[where_idx].auto_depth.saturating_add(1),
            "AND after split NOT EXISTS should return to the outer condition continuation depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_not_exists_chain_relative_to_nested_query_base() {
        let sql = r#"SELECT d.deptno
FROM dept d
WHERE EXISTS (
    SELECT 1
    FROM emp e
    WHERE
    NOT
    EXISTS
    (
        SELECT 1
        FROM bonus b
        WHERE b.empno = e.empno
    )
    AND e.deptno = d.deptno
)
AND d.status = 'A';"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let where_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE")
            .expect("source should contain split inner WHERE");
        let not_idx = lines
            .iter()
            .position(|line| line.trim_start() == "NOT")
            .expect("source should contain split NOT fragment");
        let exists_idx = lines
            .iter()
            .position(|line| line.trim_start() == "EXISTS")
            .expect("source should contain split EXISTS fragment");
        let open_idx = lines
            .iter()
            .position(|line| line.trim() == "(")
            .expect("source should contain split open paren");
        let select_idx = lines
            .iter()
            .enumerate()
            .find(|(idx, line)| *idx > open_idx && line.trim_start() == "SELECT 1")
            .map(|(idx, _)| idx)
            .expect("source should contain nested child SELECT");
        let close_idx = lines
            .iter()
            .enumerate()
            .find(|(idx, line)| *idx > select_idx && line.trim() == ")")
            .map(|(idx, _)| idx)
            .expect("source should contain nested close paren");
        let and_idx = lines
            .iter()
            .position(|line| line.trim_start() == "AND e.deptno = d.deptno")
            .expect("source should contain nested AND continuation");

        assert_eq!(
            contexts[not_idx].auto_depth,
            contexts[where_idx].auto_depth.saturating_add(1),
            "split NOT fragment should stay on the inner condition owner depth instead of falling back to parser depth"
        );
        assert_eq!(
            contexts[exists_idx].auto_depth, contexts[not_idx].auto_depth,
            "split EXISTS fragment should stay aligned with the preceding NOT owner fragment"
        );
        assert_eq!(
            contexts[open_idx].auto_depth, contexts[exists_idx].auto_depth,
            "split open paren after NOT/EXISTS should stay aligned with the completed owner depth"
        );
        assert_eq!(
            contexts[select_idx].query_base_depth,
            Some(contexts[exists_idx].auto_depth.saturating_add(1)),
            "child SELECT under split NOT/EXISTS should anchor its query base from the completed owner depth"
        );
        assert_eq!(
            contexts[select_idx].auto_depth,
            contexts[exists_idx].auto_depth.saturating_add(1),
            "child SELECT under split NOT/EXISTS should stay exactly one level deeper than the completed owner depth"
        );
        assert_eq!(
            contexts[close_idx].auto_depth, contexts[exists_idx].auto_depth,
            "split NOT/EXISTS close paren should realign with the completed owner depth"
        );
        assert_eq!(
            contexts[and_idx].auto_depth,
            contexts[where_idx].auto_depth.saturating_add(1),
            "AND after split NOT/EXISTS should return to the inner condition continuation depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_not_in_chain_relative_to_nested_query_base() {
        let sql = r#"SELECT d.deptno
FROM dept d
WHERE EXISTS (
    SELECT 1
    FROM emp e
    WHERE e.deptno NOT
    IN
    (
        SELECT b.deptno
        FROM bonus b
        WHERE b.empno = e.empno
    )
    AND e.active = 'Y'
)
AND d.status = 'A';"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let not_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE e.deptno NOT")
            .expect("source should contain split NOT owner line");
        let in_idx = lines
            .iter()
            .position(|line| line.trim_start() == "IN")
            .expect("source should contain split IN fragment");
        let open_idx = lines
            .iter()
            .position(|line| line.trim() == "(")
            .expect("source should contain split open paren");
        let select_idx = lines
            .iter()
            .enumerate()
            .find(|(idx, line)| *idx > open_idx && line.trim_start() == "SELECT b.deptno")
            .map(|(idx, _)| idx)
            .expect("source should contain nested child SELECT");
        let close_idx = lines
            .iter()
            .enumerate()
            .find(|(idx, line)| *idx > select_idx && line.trim() == ")")
            .map(|(idx, _)| idx)
            .expect("source should contain nested close paren");
        let and_idx = lines
            .iter()
            .position(|line| line.trim_start() == "AND e.active = 'Y'")
            .expect("source should contain nested AND continuation");

        assert_eq!(
            contexts[in_idx].auto_depth, contexts[not_idx].auto_depth,
            "split IN fragment should stay aligned with the preceding NOT owner line"
        );
        assert_eq!(
            contexts[open_idx].auto_depth, contexts[in_idx].auto_depth,
            "split open paren after NOT/IN should stay aligned with the completed owner depth"
        );
        assert_eq!(
            contexts[select_idx].query_base_depth,
            Some(contexts[in_idx].auto_depth.saturating_add(1)),
            "child SELECT under split NOT/IN should anchor its query base from the completed owner depth"
        );
        assert_eq!(
            contexts[select_idx].auto_depth,
            contexts[in_idx].auto_depth.saturating_add(1),
            "child SELECT under split NOT/IN should stay exactly one level deeper than the completed owner depth"
        );
        assert_eq!(
            contexts[close_idx].auto_depth, contexts[in_idx].auto_depth,
            "split NOT/IN close paren should realign with the completed owner depth"
        );
        assert_eq!(
            contexts[and_idx].auto_depth, contexts[not_idx].auto_depth,
            "AND after split NOT/IN should return to the same nested condition continuation depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_nested_split_exists_depths_relative_to_each_owner() {
        let sql = r#"SELECT d.deptno
FROM dept d
WHERE EXISTS
(
    SELECT 1
    FROM emp e
    WHERE EXISTS
    (
        SELECT 1
        FROM bonus b
        WHERE b.empno = e.empno
    )
    AND e.deptno = d.deptno
)
AND d.status = 'A';"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let outer_exists_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE EXISTS")
            .unwrap_or(0);
        let inner_exists_idx = lines
            .iter()
            .enumerate()
            .skip(outer_exists_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "WHERE EXISTS")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let open_indices: Vec<usize> = lines
            .iter()
            .enumerate()
            .filter_map(|(idx, line)| (line.trim() == "(").then_some(idx))
            .collect();
        let close_indices: Vec<usize> = lines
            .iter()
            .enumerate()
            .filter_map(|(idx, line)| (line.trim() == ")").then_some(idx))
            .collect();
        let inner_select_idx = lines
            .iter()
            .enumerate()
            .skip(open_indices.get(1).copied().unwrap_or(0).saturating_add(1))
            .find(|(_, line)| line.trim_start() == "SELECT 1")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let inner_and_idx = lines
            .iter()
            .position(|line| line.trim_start() == "AND e.deptno = d.deptno")
            .unwrap_or(0);
        let outer_and_idx = lines
            .iter()
            .position(|line| line.trim_start() == "AND d.status = 'A';")
            .unwrap_or(0);
        assert_eq!(
            contexts[open_indices[0]].auto_depth, contexts[outer_exists_idx].auto_depth,
            "outer split EXISTS opener should stay aligned with the outer owner depth"
        );
        assert_eq!(
            contexts[open_indices[1]].auto_depth, contexts[inner_exists_idx].auto_depth,
            "inner split EXISTS opener should stay aligned with the inner owner depth"
        );
        assert!(
            contexts[inner_select_idx].auto_depth > contexts[open_indices[1]].auto_depth,
            "inner split EXISTS child SELECT should stay deeper than the standalone opener"
        );
        assert_eq!(
            contexts[close_indices[0]].auto_depth, contexts[inner_exists_idx].auto_depth,
            "inner split EXISTS closer should stay on the inner owner depth"
        );
        assert_eq!(
            contexts[close_indices[1]].auto_depth, contexts[outer_exists_idx].auto_depth,
            "outer split EXISTS closer should stay on the outer owner depth"
        );
        assert_eq!(
            contexts[inner_and_idx].auto_depth,
            contexts[inner_exists_idx].auto_depth.saturating_add(1),
            "AND after the inner split EXISTS should stay relative to the inner condition base"
        );
        assert_eq!(
            contexts[outer_and_idx].auto_depth,
            contexts[outer_exists_idx].auto_depth.saturating_add(1),
            "AND after the outer split EXISTS should return to the outer condition base"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_nested_split_from_depths_relative_to_each_owner() {
        let sql = r#"SELECT outer_q.id
FROM
(
    SELECT inner_q.id
    FROM
    (
        SELECT 1 AS id
        FROM dual
    ) inner_q
) outer_q;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let from_indices: Vec<usize> = lines
            .iter()
            .enumerate()
            .filter_map(|(idx, line)| (line.trim_start() == "FROM").then_some(idx))
            .collect();
        let open_indices: Vec<usize> = lines
            .iter()
            .enumerate()
            .filter_map(|(idx, line)| (line.trim() == "(").then_some(idx))
            .collect();
        let deepest_select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT 1 AS id")
            .unwrap_or(0);
        let inner_close_idx = lines
            .iter()
            .position(|line| line.trim_start() == ") inner_q")
            .unwrap_or(0);
        let outer_close_idx = lines
            .iter()
            .position(|line| line.trim_start() == ") outer_q;")
            .unwrap_or(0);

        assert_eq!(
            contexts[open_indices[0]].auto_depth, contexts[from_indices[0]].auto_depth,
            "outer split FROM opener should stay on the outer FROM base depth"
        );
        assert_eq!(
            contexts[open_indices[1]].auto_depth, contexts[from_indices[1]].auto_depth,
            "inner split FROM opener should stay on the inner FROM base depth"
        );
        assert_eq!(
            contexts[deepest_select_idx].auto_depth,
            contexts[open_indices[1]].auto_depth.saturating_add(1),
            "SELECT under nested split FROM should stay exactly one level deeper than its opener"
        );
        assert_eq!(
            contexts[inner_close_idx].auto_depth, contexts[from_indices[1]].auto_depth,
            "inner split FROM closer should realign with the inner FROM base depth"
        );
        assert_eq!(
            contexts[outer_close_idx].auto_depth, contexts[from_indices[0]].auto_depth,
            "outer split FROM closer should realign with the outer FROM base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_all_header_relative_to_nested_query_base() {
        let sql = r#"SELECT d.deptno
FROM dept d
WHERE d.deptno IN (
    SELECT e.deptno
    FROM emp e
    WHERE e.sal >
    ALL
    (
        SELECT b.sal
        FROM bonus b
        WHERE b.empno = e.empno
    )
    AND e.active = 'Y'
);"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let where_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE e.sal >")
            .unwrap_or(0);
        let all_idx = lines
            .iter()
            .position(|line| line.trim_start() == "ALL")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(all_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT b.sal")
            .unwrap_or(0);
        let close_idx = lines
            .iter()
            .enumerate()
            .skip(select_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == ")")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let and_idx = lines
            .iter()
            .position(|line| line.trim_start() == "AND e.active = 'Y'")
            .unwrap_or(0);

        assert_eq!(
            contexts[all_idx].auto_depth,
            contexts[where_idx].auto_depth.saturating_add(1),
            "split ALL header should stay relative to the nested WHERE condition depth"
        );
        assert_eq!(
            contexts[open_idx].auto_depth, contexts[all_idx].auto_depth,
            "split ALL opener should stay aligned with the ALL owner depth"
        );
        assert_eq!(
            contexts[select_idx].query_base_depth,
            Some(contexts[all_idx].auto_depth.saturating_add(1)),
            "child SELECT under split ALL should anchor its query base from the ALL owner depth"
        );
        assert_eq!(
            contexts[select_idx].auto_depth,
            contexts[all_idx].auto_depth.saturating_add(1),
            "child SELECT under split ALL should stay exactly one level deeper than the ALL owner depth"
        );
        assert_eq!(
            contexts[close_idx].auto_depth, contexts[all_idx].auto_depth,
            "split ALL closing paren should stay on the ALL owner depth"
        );
        assert_eq!(
            contexts[and_idx].auto_depth,
            contexts[where_idx].auto_depth.saturating_add(1),
            "AND after split ALL should return to the nested WHERE continuation depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_nested_in_child_query_depth_with_trailing_owner_spaces() {
        let sql = "SELECT 1\nFROM a\nWHERE b IN (   \n    SELECT 1\n    FROM a\n    WHERE b IN (   \n        SELECT 1\n        FROM a\n    )\n    AND 2\n);";
        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();

        let where_indices: Vec<usize> = lines
            .iter()
            .enumerate()
            .filter_map(|(idx, line)| line.trim_start().starts_with("WHERE b IN (").then_some(idx))
            .collect();
        let select_indices: Vec<usize> = lines
            .iter()
            .enumerate()
            .filter_map(|(idx, line)| (line.trim_start() == "SELECT 1").then_some(idx))
            .collect();

        let outer_where_idx = where_indices.first().copied().unwrap_or(0);
        let inner_where_idx = where_indices.get(1).copied().unwrap_or(0);
        let second_select_idx = select_indices.get(1).copied().unwrap_or(0);
        let third_select_idx = select_indices.get(2).copied().unwrap_or(0);
        let outer_where_upper = lines[outer_where_idx].trim_start().to_ascii_uppercase();
        let inner_where_upper = lines[inner_where_idx].trim_start().to_ascii_uppercase();

        assert!(
            sql_text::format_query_owner_kind(&outer_where_upper)
                == Some(sql_text::FormatQueryOwnerKind::Condition),
            "outer WHERE IN should be recognized as a direct child-query owner"
        );
        assert!(
            sql_text::format_query_owner_kind(&inner_where_upper)
                == Some(sql_text::FormatQueryOwnerKind::Condition),
            "inner WHERE IN should be recognized as a direct child-query owner"
        );

        assert_eq!(
            contexts[second_select_idx].auto_depth,
            contexts[outer_where_idx].auto_depth.saturating_add(2),
            "outer WHERE IN owner with trailing spaces must still promote child SELECT depth"
        );
        assert_eq!(
            contexts[third_select_idx].auto_depth,
            contexts[inner_where_idx].auto_depth.saturating_add(2),
            "nested WHERE IN owner with trailing spaces must still promote grandchild SELECT depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_any_some_all_child_query_depth_with_trailing_owner_spaces() {
        let scenarios = [("= ANY", "ANY"), ("< SOME", "SOME"), ("> ALL", "ALL")];

        for (comparison, keyword) in scenarios {
            let sql = format!(
                "SELECT 1\nFROM a\nWHERE b {comparison} (   \n    SELECT 1\n    FROM a\n    WHERE c {comparison} (   \n        SELECT 1\n        FROM a\n    )\n    AND 2 = 2\n);"
            );
            let contexts = QueryExecutor::auto_format_line_contexts(&sql);
            let lines: Vec<&str> = sql.lines().collect();
            let outer_owner_prefix = format!("WHERE b {comparison} (");
            let inner_owner_prefix = format!("WHERE c {comparison} (");

            let outer_owner_idx = lines
                .iter()
                .position(|line| line.trim_start().starts_with(&outer_owner_prefix))
                .unwrap_or(0);
            let inner_owner_idx = lines
                .iter()
                .position(|line| line.trim_start().starts_with(&inner_owner_prefix))
                .unwrap_or(0);
            let select_indices: Vec<usize> = lines
                .iter()
                .enumerate()
                .filter_map(|(idx, line)| (line.trim_start() == "SELECT 1").then_some(idx))
                .collect();
            let second_select_idx = select_indices.get(1).copied().unwrap_or(0);
            let third_select_idx = select_indices.get(2).copied().unwrap_or(0);
            let outer_owner_upper = lines[outer_owner_idx].trim_start().to_ascii_uppercase();
            let inner_owner_upper = lines[inner_owner_idx].trim_start().to_ascii_uppercase();

            assert!(
                sql_text::format_query_owner_kind(&outer_owner_upper)
                    == Some(sql_text::FormatQueryOwnerKind::Condition),
                "outer WHERE {keyword} should be recognized as a direct child-query owner"
            );
            assert!(
                sql_text::format_query_owner_kind(&inner_owner_upper)
                    == Some(sql_text::FormatQueryOwnerKind::Condition),
                "inner WHERE {keyword} should be recognized as a direct child-query owner"
            );
            assert_eq!(
                contexts[second_select_idx].auto_depth,
                contexts[outer_owner_idx].auto_depth.saturating_add(2),
                "outer WHERE {keyword} owner with trailing spaces must still promote child SELECT depth"
            );
            assert_eq!(
                contexts[third_select_idx].auto_depth,
                contexts[inner_owner_idx].auto_depth.saturating_add(2),
                "nested WHERE {keyword} owner with trailing spaces must still promote grandchild SELECT depth"
            );
        }
    }

    #[test]
    fn auto_format_line_contexts_keep_split_any_some_all_owner_depth_relative_to_each_owner() {
        let scenarios = [("= ANY", "ANY"), ("< SOME", "SOME"), ("> ALL", "ALL")];

        for (comparison, keyword) in scenarios {
            let sql = format!(
                "SELECT 1\nFROM outer_t o\nWHERE o.score {comparison}\n(\n    SELECT 1\n    FROM inner_t i\n    WHERE i.score {comparison}\n    (\n        SELECT 1\n        FROM leaf_t l\n        WHERE l.outer_id = o.id\n    )\n    AND i.outer_id = o.id\n)\nAND o.active = 1;"
            );
            let contexts = QueryExecutor::auto_format_line_contexts(&sql);
            let lines: Vec<&str> = sql.lines().collect();
            let outer_owner_prefix = format!("WHERE o.score {comparison}");
            let inner_owner_prefix = format!("WHERE i.score {comparison}");

            let outer_owner_idx = lines
                .iter()
                .position(|line| line.trim_start().starts_with(&outer_owner_prefix))
                .unwrap_or(0);
            let inner_owner_idx = lines
                .iter()
                .position(|line| line.trim_start().starts_with(&inner_owner_prefix))
                .unwrap_or(0);
            let open_indices: Vec<usize> = lines
                .iter()
                .enumerate()
                .filter_map(|(idx, line)| (line.trim() == "(").then_some(idx))
                .collect();
            let outer_open_idx = open_indices.first().copied().unwrap_or(0);
            let inner_open_idx = open_indices.get(1).copied().unwrap_or(0);
            let select_indices: Vec<usize> = lines
                .iter()
                .enumerate()
                .filter_map(|(idx, line)| (line.trim_start() == "SELECT 1").then_some(idx))
                .collect();
            let inner_select_idx = select_indices.get(1).copied().unwrap_or(0);
            let leaf_select_idx = select_indices.get(2).copied().unwrap_or(0);
            let close_indices: Vec<usize> = lines
                .iter()
                .enumerate()
                .filter_map(|(idx, line)| (line.trim() == ")").then_some(idx))
                .collect();
            let inner_close_idx = close_indices.first().copied().unwrap_or(0);
            let outer_close_idx = close_indices.get(1).copied().unwrap_or(0);
            let inner_and_idx = lines
                .iter()
                .position(|line| line.trim_start() == "AND i.outer_id = o.id")
                .unwrap_or(0);
            let outer_and_idx = lines
                .iter()
                .position(|line| line.trim_start() == "AND o.active = 1;")
                .unwrap_or(0);
            let outer_owner_base_depth = sql_text::FormatQueryOwnerKind::Condition
                .auto_format_child_query_owner_base_depth(
                    contexts[outer_owner_idx].auto_depth,
                    contexts[outer_owner_idx].query_base_depth,
                );
            let inner_owner_base_depth = sql_text::FormatQueryOwnerKind::Condition
                .auto_format_child_query_owner_base_depth(
                    contexts[inner_owner_idx].auto_depth,
                    contexts[inner_owner_idx].query_base_depth,
                );

            assert_eq!(
                contexts[outer_open_idx].auto_depth, contexts[outer_owner_idx].auto_depth,
                "split {keyword} opener should stay aligned with the outer owner depth"
            );
            assert_eq!(
                contexts[inner_open_idx].auto_depth, contexts[inner_owner_idx].auto_depth,
                "nested split {keyword} opener should stay aligned with the inner owner depth"
            );
            assert_eq!(
                contexts[inner_select_idx].auto_depth,
                outer_owner_base_depth.saturating_add(1),
                "child SELECT under split outer {keyword} should be one level deeper than the outer promoted owner base depth"
            );
            assert_eq!(
                contexts[leaf_select_idx].auto_depth,
                inner_owner_base_depth.saturating_add(1),
                "grandchild SELECT under split inner {keyword} should be one level deeper than the inner promoted owner base depth"
            );
            assert_eq!(
                contexts[inner_close_idx].auto_depth, contexts[inner_owner_idx].auto_depth,
                "nested split {keyword} closer should realign with the inner owner depth"
            );
            assert_eq!(
                contexts[outer_close_idx].auto_depth, contexts[outer_owner_idx].auto_depth,
                "split outer {keyword} closer should realign with the outer owner depth"
            );
            assert_eq!(
                contexts[inner_and_idx].auto_depth,
                contexts[inner_owner_idx].auto_depth.saturating_add(1),
                "AND after the inner split {keyword} should return to the inner condition continuation depth"
            );
            assert_eq!(
                contexts[outer_and_idx].auto_depth,
                contexts[outer_owner_idx].auto_depth.saturating_add(1),
                "AND after the outer split {keyword} should return to the outer condition continuation depth"
            );
        }
    }

    #[test]
    fn auto_format_line_contexts_keep_operator_continuation_after_inline_block_comment() {
        let sql = r#"SELECT 1
FROM order_item oi
WHERE oi.order_id = v.order_id
    AND oi.qty <= /* X: 0 이하 */
    0;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let and_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND oi.qty <="))
            .unwrap_or(0);
        let operand_idx = lines
            .iter()
            .position(|line| line.trim() == "0;")
            .unwrap_or(0);

        assert_eq!(
            contexts[operand_idx].auto_depth,
            contexts[and_idx]
                .query_base_depth
                .unwrap_or(contexts[and_idx].auto_depth)
                .saturating_add(1),
            "inline block comment after an infix operator should promote the next operand to the condition continuation depth"
        );
        assert_eq!(
            contexts[operand_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "line following an inline-comment operator split should stay marked as query continuation"
        );
        assert_eq!(
            contexts[operand_idx].query_base_depth, contexts[and_idx].query_base_depth,
            "operator continuation should preserve the active query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_operator_continuation_inside_not_exists_subquery() {
        let sql = r#"SELECT *
FROM (
        /* Q: 인라인뷰 시작 */
        WITH x AS (
            SELECT
                p.order_id,
                p.cust_name,
                p.order_dt,
                a.amt
            FROM paid p
            JOIN amounts a
                ON /* R: join key */
                a.order_id = p.order_id
            WHERE a.amt > /* S: threshold */
                50
        )
        SELECT
            x.*,
            (
                -- [T] 라인수 서브쿼리
                SELECT COUNT (*)
                FROM order_item oi
                WHERE oi.order_id = x.order_id
            ) AS line_cnt
        FROM x
    ) v
WHERE EXISTS (
        /* U: SKU 존재 조건 */
        SELECT 1
        FROM order_item oi
        WHERE oi.order_id = v.order_id
            AND oi.sku LIKE 'SKU-%' -- [V] SKU 패턴
    )
    AND NOT EXISTS (
        -- [W] 음수 수량 배제
        SELECT 1
        FROM order_item oi
        WHERE oi.order_id = v.order_id
            AND oi.qty <= /* X: 0 이하 */
        0
    )
ORDER BY v.amt DESC;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let and_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND oi.qty <="))
            .unwrap_or(0);
        let operand_idx = lines
            .iter()
            .position(|line| line.trim() == "0")
            .unwrap_or(0);

        assert_eq!(
            contexts[and_idx].auto_depth,
            contexts[and_idx]
                .query_base_depth
                .unwrap_or(contexts[and_idx].auto_depth)
                .saturating_add(1),
            "AND continuation inside NOT EXISTS should be one level deeper than the child query base depth"
        );
        assert_eq!(
            contexts[operand_idx].auto_depth,
            contexts[and_idx].auto_depth,
            "operand after inline-comment operator inside NOT EXISTS should keep the active condition depth"
        );
        assert_eq!(
            contexts[operand_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "operand line inside NOT EXISTS should stay marked as query continuation"
        );
        assert_eq!(
            contexts[operand_idx].query_base_depth, contexts[and_idx].query_base_depth,
            "operand line inside NOT EXISTS should preserve the child query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_join_condition_depth_after_inline_block_comment_on_clause() {
        let sql = r#"SELECT *
FROM paid p
JOIN amounts a
    ON /* join key */
    a.order_id = p.order_id;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let on_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("ON /*"))
            .unwrap_or(0);
        let condition_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("a.order_id ="))
            .unwrap_or(0);

        assert_eq!(
            contexts[condition_idx].auto_depth, contexts[on_idx].auto_depth,
            "line after inline block comment on ON clause should stay on the ON clause depth"
        );
        assert_eq!(
            contexts[condition_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "line after inline block comment on ON clause should stay marked as continuation"
        );
        assert_eq!(
            contexts[condition_idx].query_base_depth, contexts[on_idx].query_base_depth,
            "ON-clause continuation must preserve the active query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_join_condition_depth_after_inline_line_comment_on_clause() {
        let sql = r#"SELECT *
FROM paid p
JOIN amounts a
    ON -- join key
    a.order_id = p.order_id;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let on_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("ON --"))
            .unwrap_or(0);
        let condition_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("a.order_id ="))
            .unwrap_or(0);

        assert_eq!(
            contexts[condition_idx].auto_depth, contexts[on_idx].auto_depth,
            "line after inline line comment on ON clause should stay on the ON clause depth"
        );
        assert_eq!(
            contexts[condition_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "line after inline line comment on ON clause should stay marked as continuation"
        );
        assert_eq!(
            contexts[condition_idx].query_base_depth, contexts[on_idx].query_base_depth,
            "ON-clause line-comment continuation must preserve the active query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_keyword_operator_rhs_on_shared_condition_depth() {
        let sql = r#"SELECT e.empno
FROM emp e
WHERE e.empno IS
NULL;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let is_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE e.empno IS")
            .unwrap_or(0);
        let null_idx = lines
            .iter()
            .position(|line| line.trim_start() == "NULL;")
            .unwrap_or(0);

        assert_eq!(
            contexts[null_idx].auto_depth,
            contexts[is_idx]
                .query_base_depth
                .unwrap_or(contexts[is_idx].auto_depth)
                .saturating_add(1),
            "keyword operator RHS should stay on the shared clause-body depth under the active query base"
        );
        assert_eq!(
            contexts[null_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "keyword operator RHS should stay marked as continuation"
        );
        assert_eq!(
            contexts[null_idx].query_base_depth, contexts[is_idx].query_base_depth,
            "keyword operator RHS should preserve the active query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_promote_select_list_after_inline_comment_on_select_header() {
        let sql = r#"CREATE OR REPLACE PROCEDURE test_open_with_proc IS
    p_rc SYS_REFCURSOR;
BEGIN
    OPEN p_rc FOR
        WITH /* A: dept 집계 CTE */
        dept_stats AS (
            SELECT /* B: dept 집계 */
            deptno,
                COUNT(*) AS cnt,
                AVG(sal) AS avg_sal,
                SUM (NVL (comm, /* C: NULL→0 */
                        0)) AS sum_comm
            FROM emp
            GROUP BY deptno
        )
        SELECT *
        FROM dept_stats;
END;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();

        let with_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WITH /* A: dept 집계 CTE */")
            .unwrap_or(0);
        let cte_header_idx = lines
            .iter()
            .position(|line| line.trim_start() == "dept_stats AS (")
            .unwrap_or(0);
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT /* B: dept 집계 */")
            .unwrap_or(0);
        let deptno_idx = lines
            .iter()
            .position(|line| line.trim_start() == "deptno,")
            .unwrap_or(0);

        assert_eq!(
            contexts[cte_header_idx].auto_depth, contexts[with_idx].auto_depth,
            "CTE name line after inline comment on WITH should stay on the WITH header depth"
        );
        assert_eq!(
            contexts[select_idx].auto_depth,
            contexts[cte_header_idx].auto_depth.saturating_add(1),
            "CTE body SELECT should stay one level deeper than the CTE header"
        );
        assert_eq!(
            contexts[deptno_idx].auto_depth,
            contexts[select_idx].auto_depth.saturating_add(1),
            "first select-list item after inline comment on SELECT should promote to list depth"
        );
        assert_eq!(
            contexts[deptno_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "select-list item after inline SELECT comment should stay marked as continuation"
        );
        assert_eq!(
            contexts[deptno_idx].query_base_depth, contexts[select_idx].query_base_depth,
            "inline SELECT comment should preserve the active query base depth for the next list item"
        );
    }

    #[test]
    fn auto_format_line_contexts_replace_outer_control_owner_before_open_for_query_owner() {
        let sql = r#"BEGIN
NULL;
EXCEPTION
WHEN OTHERS THEN
OPEN c_emp FOR
SELECT empno
FROM (
SELECT empno
FROM emp
);
END;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();

        let when_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHEN OTHERS THEN")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .position(|line| line.trim_start() == "OPEN c_emp FOR")
            .unwrap_or(0);
        let select_indices = lines
            .iter()
            .enumerate()
            .filter(|(_, line)| line.trim_start() == "SELECT empno")
            .map(|(idx, _)| idx)
            .collect::<Vec<_>>();
        let select_idx = *select_indices.first().unwrap_or(&0);
        let nested_select_idx = *select_indices.get(1).unwrap_or(&0);
        let from_idx = lines
            .iter()
            .position(|line| line.trim_start() == "FROM (")
            .unwrap_or(0);

        assert_eq!(
            contexts[open_idx].auto_depth,
            contexts[when_idx].auto_depth.saturating_add(1),
            "OPEN ... FOR in EXCEPTION handler should stay one level deeper than WHEN OTHERS"
        );
        assert_eq!(
            contexts[select_idx].auto_depth,
            contexts[open_idx].auto_depth.saturating_add(1),
            "SELECT after OPEN ... FOR should use the OPEN owner depth without stacking the outer control owner again"
        );
        assert_eq!(
            contexts[from_idx].query_base_depth, contexts[select_idx].query_base_depth,
            "FROM under OPEN ... FOR should keep the same query base as its SELECT head"
        );
        assert_eq!(
            contexts[nested_select_idx].auto_depth,
            contexts[from_idx].auto_depth.saturating_add(1),
            "Nested SELECT under OPEN ... FOR should still nest relative to FROM after owner replacement"
        );
    }

    #[test]
    fn auto_format_line_contexts_replace_then_owner_before_nested_begin_and_open_for_query_owner() {
        let sql = r#"BEGIN
IF l_ready THEN
BEGIN
OPEN c_emp FOR
SELECT empno
FROM emp;
END;
END IF;
END;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();

        let if_idx = lines
            .iter()
            .position(|line| line.trim_start() == "IF l_ready THEN")
            .unwrap_or(0);
        let begin_idx = lines
            .iter()
            .enumerate()
            .skip(if_idx + 1)
            .find(|(_, line)| line.trim_start() == "BEGIN")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .position(|line| line.trim_start() == "OPEN c_emp FOR")
            .unwrap_or(0);
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT empno")
            .unwrap_or(0);
        let from_idx = lines
            .iter()
            .position(|line| line.trim_start() == "FROM emp;")
            .unwrap_or(0);

        assert_eq!(
            contexts[begin_idx].auto_depth,
            contexts[if_idx].auto_depth.saturating_add(1),
            "nested BEGIN should stay one level deeper than IF"
        );
        assert_eq!(
            contexts[open_idx].auto_depth,
            contexts[begin_idx].auto_depth.saturating_add(1),
            "OPEN ... FOR after nested BEGIN should replace the outer THEN owner and stay one level deeper than BEGIN"
        );
        assert_eq!(
            contexts[select_idx].auto_depth,
            contexts[open_idx].auto_depth.saturating_add(1),
            "SELECT after OPEN ... FOR should inherit the OPEN owner depth without stacking stale control owners"
        );
        assert_eq!(
            contexts[from_idx].query_base_depth, contexts[select_idx].query_base_depth,
            "FROM under OPEN ... FOR should keep the same query base as its SELECT head after nested owner replacement"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_open_for_owner_chain_relative_to_exception_body() {
        let sql = r#"BEGIN
NULL;
EXCEPTION
WHEN OTHERS THEN
OPEN c_emp
FOR
SELECT empno
FROM (
SELECT empno
FROM emp
);
END;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();

        let when_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHEN OTHERS THEN")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .position(|line| line.trim_start() == "OPEN c_emp")
            .unwrap_or(0);
        let for_idx = lines
            .iter()
            .position(|line| line.trim_start() == "FOR")
            .unwrap_or(0);
        let select_indices = lines
            .iter()
            .enumerate()
            .filter(|(_, line)| line.trim_start() == "SELECT empno")
            .map(|(idx, _)| idx)
            .collect::<Vec<_>>();
        let select_idx = *select_indices.first().unwrap_or(&0);
        let nested_select_idx = *select_indices.get(1).unwrap_or(&0);
        let from_idx = lines
            .iter()
            .position(|line| line.trim_start() == "FROM (")
            .unwrap_or(0);

        assert_eq!(
            contexts[open_idx].auto_depth,
            contexts[when_idx].auto_depth.saturating_add(1),
            "split OPEN line should stay one level deeper than WHEN OTHERS"
        );
        assert_eq!(
            contexts[for_idx].auto_depth, contexts[open_idx].auto_depth,
            "split FOR line should stay aligned with the OPEN owner depth"
        );
        assert_eq!(
            contexts[select_idx].auto_depth,
            contexts[for_idx].auto_depth.saturating_add(1),
            "SELECT after split OPEN ... FOR should still use the completed owner depth"
        );
        assert_eq!(
            contexts[from_idx].query_base_depth, contexts[select_idx].query_base_depth,
            "FROM under split OPEN ... FOR should keep the same query base as its SELECT head"
        );
        assert_eq!(
            contexts[nested_select_idx].auto_depth,
            contexts[from_idx].auto_depth.saturating_add(1),
            "nested SELECT under split OPEN ... FOR should still nest relative to FROM"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_cursor_is_owner_chain_across_parameter_parens() {
        let sql = r#"DECLARE
CURSOR c_emp
(
p_deptno NUMBER
)
IS
SELECT empno
FROM emp
WHERE deptno = p_deptno;
BEGIN
NULL;
END;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();

        let cursor_idx = lines
            .iter()
            .position(|line| line.trim_start() == "CURSOR c_emp")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .position(|line| line.trim_start() == "(")
            .unwrap_or(0);
        let close_idx = lines
            .iter()
            .position(|line| line.trim_start() == ")")
            .unwrap_or(0);
        let is_idx = lines
            .iter()
            .position(|line| line.trim_start() == "IS")
            .unwrap_or(0);
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT empno")
            .unwrap_or(0);
        let from_idx = lines
            .iter()
            .position(|line| line.trim_start() == "FROM emp")
            .unwrap_or(0);

        assert_eq!(
            contexts[open_idx].auto_depth, contexts[cursor_idx].auto_depth,
            "split CURSOR parameter opener should stay aligned with the CURSOR owner depth"
        );
        assert_eq!(
            contexts[close_idx].auto_depth, contexts[cursor_idx].auto_depth,
            "split CURSOR parameter closer should return to the CURSOR owner depth"
        );
        assert_eq!(
            contexts[is_idx].auto_depth, contexts[cursor_idx].auto_depth,
            "split IS line should stay aligned with the CURSOR owner depth after parameter parens"
        );
        assert_eq!(
            contexts[select_idx].auto_depth,
            contexts[is_idx].auto_depth.saturating_add(1),
            "SELECT after split CURSOR ... IS should stay one level deeper than the completed owner"
        );
        assert_eq!(
            contexts[from_idx].query_base_depth, contexts[select_idx].query_base_depth,
            "FROM under split CURSOR ... IS should keep the same query base as its SELECT head"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_open_owner_fragments_on_owner_depth() {
        let sql = r#"BEGIN
OPEN
c_emp
FOR
SELECT empno
FROM emp;
END;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();

        let open_idx = lines
            .iter()
            .position(|line| line.trim_start() == "OPEN")
            .unwrap_or(0);
        let cursor_idx = lines
            .iter()
            .position(|line| line.trim_start() == "c_emp")
            .unwrap_or(0);
        let for_idx = lines
            .iter()
            .position(|line| line.trim_start() == "FOR")
            .unwrap_or(0);
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT empno")
            .unwrap_or(0);

        assert_eq!(
            contexts[cursor_idx].auto_depth, contexts[open_idx].auto_depth,
            "split OPEN cursor-name fragment should stay aligned with the OPEN owner depth"
        );
        assert_eq!(
            contexts[for_idx].auto_depth, contexts[open_idx].auto_depth,
            "split FOR line should stay aligned with the OPEN owner depth"
        );
        assert_eq!(
            contexts[select_idx].auto_depth,
            contexts[for_idx].auto_depth.saturating_add(1),
            "SELECT after split OPEN ... FOR should stay one level deeper than the completed owner"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_cursor_owner_name_fragment_on_owner_depth() {
        let sql = r#"DECLARE
CURSOR
c_emp
IS
SELECT empno
FROM emp;
BEGIN
NULL;
END;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();

        let cursor_header_idx = lines
            .iter()
            .position(|line| line.trim_start() == "CURSOR")
            .unwrap_or(0);
        let cursor_name_idx = lines
            .iter()
            .position(|line| line.trim_start() == "c_emp")
            .unwrap_or(0);
        let is_idx = lines
            .iter()
            .position(|line| line.trim_start() == "IS")
            .unwrap_or(0);
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT empno")
            .unwrap_or(0);

        assert_eq!(
            contexts[cursor_name_idx].auto_depth, contexts[cursor_header_idx].auto_depth,
            "split CURSOR name fragment should stay aligned with the CURSOR owner depth"
        );
        assert_eq!(
            contexts[is_idx].auto_depth, contexts[cursor_header_idx].auto_depth,
            "split IS line should stay aligned with the CURSOR owner depth"
        );
        assert_eq!(
            contexts[select_idx].auto_depth,
            contexts[is_idx].auto_depth.saturating_add(1),
            "SELECT after split CURSOR ... IS should stay one level deeper than the completed owner"
        );
    }

    #[test]
    fn auto_format_line_contexts_promote_group_by_item_after_inline_comment_on_header() {
        let sql = r#"SELECT deptno,
    COUNT(*) AS cnt
FROM emp
GROUP BY /* keep */
deptno;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();

        let group_by_idx = lines
            .iter()
            .position(|line| line.trim_start() == "GROUP BY /* keep */")
            .unwrap_or(0);
        let deptno_idx = lines
            .iter()
            .position(|line| line.trim_start() == "deptno;")
            .unwrap_or(0);

        assert_eq!(
            contexts[deptno_idx].auto_depth,
            contexts[group_by_idx].auto_depth.saturating_add(1),
            "GROUP BY item after inline header comment should use list-item depth"
        );
        assert_eq!(
            contexts[deptno_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "GROUP BY item after inline header comment should stay marked as continuation"
        );
        assert_eq!(
            contexts[deptno_idx].query_base_depth, contexts[group_by_idx].query_base_depth,
            "GROUP BY inline-header continuation should preserve the query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_ignore_existing_indent_for_inline_comment_header_continuation() {
        let sql = r#"SELECT deptno,
    COUNT(*) AS cnt
FROM emp
GROUP BY /* keep */
                deptno;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();

        let group_by_idx = lines
            .iter()
            .position(|line| line.trim_start() == "GROUP BY /* keep */")
            .unwrap_or(0);
        let deptno_idx = lines
            .iter()
            .position(|line| line.trim_start() == "deptno;")
            .unwrap_or(0);

        assert_eq!(
            contexts[deptno_idx].auto_depth,
            contexts[group_by_idx].auto_depth.saturating_add(1),
            "inline-comment continuation depth must come from the GROUP BY header depth, not the raw existing indent"
        );
        assert_eq!(
            contexts[deptno_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "inline-comment continuation should still be tracked as a continuation"
        );
        assert_eq!(
            contexts[deptno_idx].query_base_depth, contexts[group_by_idx].query_base_depth,
            "inline-comment continuation should preserve the query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_limit_comment_continuation_uses_operand_depth() {
        let sql = r#"SELECT e.empno
FROM emp e
ORDER BY e.empno
LIMIT -- page size
10;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let limit_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("LIMIT --"))
            .unwrap_or(0);
        let operand_idx = lines
            .iter()
            .position(|line| line.trim() == "10;")
            .unwrap_or(0);

        assert_eq!(
            contexts[operand_idx].auto_depth,
            contexts[limit_idx]
                .query_base_depth
                .unwrap_or(contexts[limit_idx].auto_depth)
                .saturating_add(1),
            "LIMIT operand after inline header comment should use one deeper continuation depth"
        );
        assert_eq!(
            contexts[operand_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "LIMIT operand after inline header comment should stay marked as continuation"
        );
        assert_eq!(
            contexts[operand_idx].query_base_depth, contexts[limit_idx].query_base_depth,
            "LIMIT inline-header continuation should preserve the query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_limit_split_operand_uses_structural_continuation_depth() {
        let sql = r#"SELECT e.empno
FROM emp e
ORDER BY e.empno
LIMIT
10;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let limit_idx = lines
            .iter()
            .position(|line| line.trim() == "LIMIT")
            .unwrap_or(0);
        let operand_idx = lines
            .iter()
            .position(|line| line.trim() == "10;")
            .unwrap_or(0);

        assert_eq!(
            contexts[operand_idx].auto_depth,
            contexts[limit_idx].auto_depth.saturating_add(1),
            "LIMIT operand on the next line should derive from the LIMIT clause depth, not raw source indent"
        );
        assert_eq!(
            contexts[operand_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "split LIMIT operand should stay marked as continuation"
        );
        assert_eq!(
            contexts[operand_idx].query_base_depth, contexts[limit_idx].query_base_depth,
            "split LIMIT operand should preserve the query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_where_operator_rhs_uses_structural_continuation_depth() {
        let sql = r#"SELECT e.empno
FROM emp e
WHERE e.empno =
10;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let where_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("WHERE e.empno ="))
            .unwrap_or(0);
        let operand_idx = lines
            .iter()
            .position(|line| line.trim() == "10;")
            .unwrap_or(0);

        assert_eq!(
            contexts[operand_idx].auto_depth,
            contexts[where_idx]
                .query_base_depth
                .unwrap_or(contexts[where_idx].auto_depth)
                .saturating_add(1),
            "split WHERE rhs should use query-base continuation depth instead of raw indent"
        );
        assert_eq!(
            contexts[operand_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "split WHERE rhs should stay marked as continuation"
        );
        assert_eq!(
            contexts[operand_idx].query_base_depth, contexts[where_idx].query_base_depth,
            "split WHERE rhs should preserve the active query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_bare_where_header_uses_shared_structural_continuation_depth() {
        let sql = r#"SELECT e.empno
FROM emp e
WHERE
e.empno = 10;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let where_idx = lines
            .iter()
            .position(|line| line.trim() == "WHERE")
            .unwrap_or(0);
        let predicate_idx = lines
            .iter()
            .position(|line| line.trim() == "e.empno = 10;")
            .unwrap_or(0);

        assert_eq!(
            contexts[predicate_idx].auto_depth,
            contexts[where_idx].auto_depth.saturating_add(1),
            "bare WHERE header should open the same structural continuation depth that phase 2 renders"
        );
        assert_eq!(
            contexts[predicate_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "predicate after bare WHERE should stay marked as continuation"
        );
        assert_eq!(
            contexts[predicate_idx].query_base_depth, contexts[where_idx].query_base_depth,
            "bare WHERE continuation should preserve the active query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_bare_from_header_uses_shared_structural_continuation_depth() {
        let sql = r#"SELECT e.empno
FROM
emp e
WHERE e.empno = 10;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let from_idx = lines
            .iter()
            .position(|line| line.trim() == "FROM")
            .unwrap_or(0);
        let item_idx = lines
            .iter()
            .position(|line| line.trim() == "emp e")
            .unwrap_or(0);

        assert_eq!(
            contexts[item_idx].auto_depth,
            contexts[from_idx].auto_depth.saturating_add(1),
            "bare FROM header should push the next from-item onto the shared list/body depth"
        );
        assert_eq!(
            contexts[item_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "from-item after bare FROM should stay marked as continuation"
        );
        assert_eq!(
            contexts[item_idx].query_base_depth, contexts[from_idx].query_base_depth,
            "bare FROM continuation should preserve the active query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_bare_on_header_uses_shared_structural_continuation_depth() {
        let sql = r#"SELECT e.empno
FROM emp e
JOIN dept d
ON
e.deptno = d.deptno
AND e.status = d.status;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let on_idx = lines
            .iter()
            .position(|line| line.trim() == "ON")
            .unwrap_or(0);
        let predicate_idx = lines
            .iter()
            .position(|line| line.trim() == "e.deptno = d.deptno")
            .unwrap_or(0);
        let and_idx = lines
            .iter()
            .position(|line| line.trim() == "AND e.status = d.status;")
            .unwrap_or(0);

        assert_eq!(
            contexts[predicate_idx].auto_depth, contexts[on_idx].auto_depth,
            "bare ON header should keep the first predicate on the shared ON-clause depth"
        );
        assert_eq!(
            contexts[predicate_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "predicate after bare ON should stay marked as continuation"
        );
        assert_eq!(
            contexts[predicate_idx].query_base_depth, contexts[on_idx].query_base_depth,
            "bare ON continuation should preserve the active query base depth"
        );
        assert_eq!(
            contexts[and_idx].auto_depth,
            contexts[predicate_idx].auto_depth.saturating_add(1),
            "subsequent join-condition continuations should stay one level deeper than the ON-clause depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_values_comment_continuation_uses_list_depth() {
        let sql = r#"INSERT INTO t_log (id, msg)
VALUES -- tuple payload
(1, 'x');"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let values_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("VALUES --"))
            .unwrap_or(0);
        let tuple_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("(1, 'x');"))
            .unwrap_or(0);

        assert_eq!(
            contexts[tuple_idx].auto_depth,
            contexts[values_idx]
                .query_base_depth
                .unwrap_or(contexts[values_idx].auto_depth)
                .saturating_add(1),
            "VALUES tuple after inline header comment should use one deeper continuation depth"
        );
        assert_eq!(
            contexts[tuple_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "VALUES tuple after inline header comment should stay marked as continuation"
        );
        assert_eq!(
            contexts[tuple_idx].query_base_depth, contexts[values_idx].query_base_depth,
            "VALUES inline-header continuation should preserve the query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_match_recognize_measures_comment_continuation_uses_subclause_depth(
    ) {
        let sql = r#"SELECT *
FROM sales
MATCH_RECOGNIZE (
    MEASURES -- derived columns
    FIRST(A.sale_date) AS first_dt
    PATTERN (A+)
    DEFINE A AS amount < 100
);"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let measures_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("MEASURES --"))
            .unwrap_or(0);
        let expr_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("FIRST(A.sale_date)"))
            .unwrap_or(0);

        assert_eq!(
            contexts[expr_idx].auto_depth,
            contexts[measures_idx].auto_depth.saturating_add(1),
            "MATCH_RECOGNIZE MEASURES body after inline comment should use one level deeper than the header line"
        );
        assert_eq!(
            contexts[expr_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "MATCH_RECOGNIZE MEASURES body after inline comment should stay marked as continuation"
        );
        assert_eq!(
            contexts[expr_idx].query_base_depth, contexts[measures_idx].query_base_depth,
            "MATCH_RECOGNIZE MEASURES continuation should preserve the query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_match_recognize_after_match_and_subset_comment_continuation_use_subclause_depth(
    ) {
        let sql = r#"SELECT *
FROM sales
MATCH_RECOGNIZE (
    AFTER MATCH SKIP -- skip strategy
    TO NEXT ROW
    SUBSET -- grouped variables
    ab = (A, B)
    PATTERN (A B+)
    DEFINE A AS amount < 50,
           B AS amount >= 50
);"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let after_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AFTER MATCH SKIP --"))
            .unwrap_or(0);
        let to_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("TO NEXT ROW"))
            .unwrap_or(0);
        let subset_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("SUBSET --"))
            .unwrap_or(0);
        let subset_body_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("ab = (A, B)"))
            .unwrap_or(0);

        assert_eq!(
            contexts[to_idx].auto_depth,
            contexts[after_idx].auto_depth.saturating_add(1),
            "AFTER MATCH SKIP continuation after inline comment should use one level deeper than the header line"
        );
        assert_eq!(
            contexts[to_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "AFTER MATCH SKIP continuation should stay marked as continuation"
        );
        assert_eq!(
            contexts[to_idx].query_base_depth, contexts[after_idx].query_base_depth,
            "AFTER MATCH SKIP continuation should preserve the query base depth"
        );
        assert_eq!(
            contexts[subset_body_idx].auto_depth,
            contexts[subset_idx].auto_depth.saturating_add(1),
            "MATCH_RECOGNIZE SUBSET body after inline comment should use one level deeper than the header line"
        );
        assert_eq!(
            contexts[subset_body_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "MATCH_RECOGNIZE SUBSET continuation should stay marked as continuation"
        );
        assert_eq!(
            contexts[subset_body_idx].query_base_depth, contexts[subset_idx].query_base_depth,
            "MATCH_RECOGNIZE SUBSET continuation should preserve the query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_model_rules_comment_continuation_uses_subclause_depth() {
        let sql = r#"SELECT *
FROM sales
MODEL
    PARTITION BY (deptno)
    DIMENSION BY (month_key)
    RULES -- calc rules
    (amt[1] = amt[CV(month_key)] * 1.1);"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let rules_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("RULES --"))
            .unwrap_or(0);
        let body_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("(amt[1] ="))
            .unwrap_or(0);

        assert!(
            contexts[body_idx].auto_depth >= contexts[rules_idx].auto_depth.saturating_add(1),
            "MODEL RULES body after inline comment should stay at least one level deeper than the header line"
        );
        assert_eq!(
            contexts[body_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "MODEL RULES body after inline comment should stay marked as continuation"
        );
        assert_eq!(
            contexts[body_idx].query_base_depth, contexts[rules_idx].query_base_depth,
            "MODEL RULES continuation should preserve the query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_split_model_reference_on_uses_reference_relative_child_query_depth(
    ) {
        let sql = r#"SELECT *
FROM (
    SELECT deptno,
        amount
    FROM sales
    MODEL
        REFERENCE ref_limits ON
        (
            SELECT limit_amt
            FROM limits l
            WHERE l.deptno = sales.deptno
        )
        DIMENSION BY (month_key)
        MEASURES (amount)
        RULES UPDATE (amount[ANY] = amount[CV()] + 1)
) modeled
WHERE modeled.amount > 0;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let reference_idx = lines
            .iter()
            .position(|line| line.trim_start() == "REFERENCE ref_limits ON")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(reference_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT limit_amt")
            .unwrap_or(0);
        let dimension_idx = lines
            .iter()
            .position(|line| line.trim_start() == "DIMENSION BY (month_key)")
            .unwrap_or(0);

        assert_eq!(
            contexts[open_idx].auto_depth, contexts[reference_idx].auto_depth,
            "split MODEL REFERENCE opener should stay aligned with the REFERENCE owner depth"
        );
        assert_eq!(
            contexts[select_idx].auto_depth,
            contexts[reference_idx].auto_depth.saturating_add(1),
            "split MODEL REFERENCE child SELECT should be one level deeper than the REFERENCE owner"
        );
        assert_eq!(
            contexts[select_idx].query_base_depth,
            Some(contexts[reference_idx].auto_depth.saturating_add(1)),
            "split MODEL REFERENCE child SELECT should derive its query base one level below the REFERENCE owner"
        );
        assert_eq!(
            contexts[dimension_idx].auto_depth, contexts[reference_idx].auto_depth,
            "DIMENSION BY after split MODEL REFERENCE should return to the REFERENCE subclause depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_model_reference_on_header_chain_relative_to_owner() {
        let sql = r#"SELECT *
FROM (
    SELECT deptno,
        amount
    FROM sales
    MODEL
        REFERENCE ref_limits
        ON
        (
            SELECT limit_amt
            FROM limits l
            WHERE l.deptno = sales.deptno
        )
        DIMENSION BY (month_key)
        MEASURES (amount)
        RULES UPDATE (amount[ANY] = amount[CV()] + 1)
) modeled
WHERE modeled.amount > 0;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let reference_idx = lines
            .iter()
            .position(|line| line.trim_start() == "REFERENCE ref_limits")
            .unwrap_or(0);
        let on_idx = lines
            .iter()
            .position(|line| line.trim_start() == "ON")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(on_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT limit_amt")
            .unwrap_or(0);
        let dimension_idx = lines
            .iter()
            .position(|line| line.trim_start() == "DIMENSION BY (month_key)")
            .unwrap_or(0);

        assert_eq!(
            contexts[on_idx].auto_depth, contexts[reference_idx].auto_depth,
            "split MODEL REFERENCE ON line should stay aligned with the REFERENCE owner depth"
        );
        assert_eq!(
            contexts[open_idx].auto_depth, contexts[reference_idx].auto_depth,
            "standalone open paren after split MODEL REFERENCE ON should stay on the REFERENCE owner depth"
        );
        assert_eq!(
            contexts[select_idx].auto_depth,
            contexts[reference_idx].auto_depth.saturating_add(1),
            "split MODEL REFERENCE child SELECT should still be one level deeper than the REFERENCE owner"
        );
        assert_eq!(
            contexts[select_idx].query_base_depth,
            Some(contexts[reference_idx].auto_depth.saturating_add(1)),
            "split MODEL REFERENCE child SELECT should keep a query base one level below the REFERENCE owner"
        );
        assert_eq!(
            contexts[dimension_idx].auto_depth, contexts[reference_idx].auto_depth,
            "DIMENSION BY after split MODEL REFERENCE ON should return to the REFERENCE subclause depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_normalize_overindented_split_model_reference_on_chain_to_structural_owner_depth(
    ) {
        let sql = r#"SELECT *
FROM (
    SELECT deptno,
        amount
    FROM sales
    MODEL
        REFERENCE ref_limits
                ON
                        (
                            SELECT limit_amt
                            FROM limits l
                            WHERE l.deptno = sales.deptno
                        )
        DIMENSION BY (month_key)
        MEASURES (amount)
        RULES UPDATE (amount[ANY] = amount[CV()] + 1)
) modeled
WHERE modeled.amount > 0;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let reference_idx = lines
            .iter()
            .position(|line| line.trim_start() == "REFERENCE ref_limits")
            .unwrap_or(0);
        let on_idx = lines
            .iter()
            .position(|line| line.trim_start() == "ON")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(on_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT limit_amt")
            .unwrap_or(0);
        let dimension_idx = lines
            .iter()
            .position(|line| line.trim_start() == "DIMENSION BY (month_key)")
            .unwrap_or(0);

        assert_eq!(
            contexts[on_idx].auto_depth, contexts[reference_idx].auto_depth,
            "overindented MODEL REFERENCE ON should snap back to the REFERENCE owner depth"
        );
        assert_eq!(
            contexts[open_idx].auto_depth, contexts[reference_idx].auto_depth,
            "overindented standalone open paren after MODEL REFERENCE ON should stay on the REFERENCE owner depth"
        );
        assert_eq!(
            contexts[select_idx].auto_depth,
            contexts[reference_idx].auto_depth.saturating_add(1),
            "overindented MODEL REFERENCE child SELECT should still derive one level below the REFERENCE owner"
        );
        assert_eq!(
            contexts[dimension_idx].auto_depth, contexts[reference_idx].auto_depth,
            "MODEL subclauses after an overindented REFERENCE chain should realign with the structural owner depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_match_recognize_owner_stack_across_standalone_open_paren(
    ) {
        let sql = r#"SELECT *
FROM (
    SELECT *
    FROM sales
    MATCH_RECOGNIZE
    (
        PARTITION BY cust_id
        ORDER BY sale_date
        MEASURES MATCH_NUMBER () AS mno
        PATTERN (A B+)
        DEFINE A AS amount < 50,
               B AS amount >= 50
    )
    WHERE amount > 0
) ranked
WHERE ranked.mno > 0;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let match_idx = lines
            .iter()
            .position(|line| line.trim_start() == "MATCH_RECOGNIZE")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(match_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let partition_idx = lines
            .iter()
            .position(|line| line.trim_start() == "PARTITION BY cust_id")
            .unwrap_or(0);
        let measures_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("MEASURES MATCH_NUMBER"))
            .unwrap_or(0);
        let pattern_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("PATTERN (A B+)"))
            .unwrap_or(0);
        let define_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("DEFINE A AS amount < 50"))
            .unwrap_or(0);
        let inner_where_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE amount > 0")
            .unwrap_or(0);
        let outer_where_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE ranked.mno > 0;")
            .unwrap_or(0);

        assert_eq!(
            contexts[open_idx].auto_depth, contexts[match_idx].auto_depth,
            "standalone open paren after MATCH_RECOGNIZE should stay on the owner depth"
        );
        assert_eq!(
            contexts[partition_idx].auto_depth,
            contexts[match_idx].auto_depth.saturating_add(1),
            "MATCH_RECOGNIZE body should stay one level deeper than the split owner line"
        );
        assert_eq!(
            contexts[measures_idx].auto_depth, contexts[partition_idx].auto_depth,
            "MATCH_RECOGNIZE MEASURES should stay aligned with sibling body headers"
        );
        assert_eq!(
            contexts[pattern_idx].auto_depth, contexts[partition_idx].auto_depth,
            "MATCH_RECOGNIZE PATTERN should stay aligned with sibling body headers"
        );
        assert_eq!(
            contexts[define_idx].auto_depth, contexts[partition_idx].auto_depth,
            "MATCH_RECOGNIZE DEFINE should stay aligned with sibling body headers"
        );
        assert_eq!(
            contexts[inner_where_idx].auto_depth, contexts[match_idx].auto_depth,
            "WHERE after split MATCH_RECOGNIZE should realign with the nested query base"
        );
        assert_eq!(
            contexts[outer_where_idx].auto_depth, 0,
            "outer WHERE should return to the top-level query depth after the nested MATCH_RECOGNIZE query closes"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_extended_match_recognize_modifier_continuations_on_owner_depth(
    ) {
        let sql = r#"SELECT *
FROM sales
MATCH_RECOGNIZE (
    MEASURES (
        SELECT COUNT (*)
        FROM (
            SELECT *
            FROM sales
            MATCH_RECOGNIZE (
                PARTITION BY cust_id
                ORDER BY sale_date
                PATTERN (A)
                DEFINE A AS amount > 0
            )
        ) nested_rows
    ) AS inner_cnt
    WITHOUT
            UNMATCHED
                    ROWS
    SHOW
            EMPTY
                    MATCHES
    PATTERN (A B+)
    DEFINE A AS amount < 50,
           B AS amount >= 50
) mr;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let without_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WITHOUT")
            .unwrap_or(0);
        let unmatched_idx = lines
            .iter()
            .position(|line| line.trim_start() == "UNMATCHED")
            .unwrap_or(0);
        let rows_idx = lines
            .iter()
            .position(|line| line.trim_start() == "ROWS")
            .unwrap_or(0);
        let show_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SHOW")
            .unwrap_or(0);
        let empty_idx = lines
            .iter()
            .position(|line| line.trim_start() == "EMPTY")
            .unwrap_or(0);
        let matches_idx = lines
            .iter()
            .position(|line| line.trim_start() == "MATCHES")
            .unwrap_or(0);
        let pattern_idx = lines
            .iter()
            .rposition(|line| line.trim_start().starts_with("PATTERN (A B+)"))
            .unwrap_or(0);
        let define_idx = lines
            .iter()
            .rposition(|line| line.trim_start().starts_with("DEFINE A AS amount < 50"))
            .unwrap_or(0);
        let inner_pattern_idx = lines
            .iter()
            .position(|line| line.trim_start() == "PATTERN (A)")
            .unwrap_or(0);

        assert_eq!(
            contexts[unmatched_idx].auto_depth, contexts[without_idx].auto_depth,
            "split MATCH_RECOGNIZE WITHOUT/UNMATCHED continuation should stay on the owner-relative modifier depth"
        );
        assert_eq!(
            contexts[rows_idx].auto_depth, contexts[without_idx].auto_depth,
            "split MATCH_RECOGNIZE WITHOUT/UNMATCHED/ROWS continuation should stay on the owner-relative modifier depth"
        );
        assert_eq!(
            contexts[empty_idx].auto_depth, contexts[show_idx].auto_depth,
            "split MATCH_RECOGNIZE SHOW/EMPTY continuation should stay on the owner-relative modifier depth"
        );
        assert_eq!(
            contexts[matches_idx].auto_depth, contexts[show_idx].auto_depth,
            "split MATCH_RECOGNIZE SHOW/EMPTY/MATCHES continuation should stay on the owner-relative modifier depth"
        );
        assert_eq!(
            contexts[show_idx].auto_depth, contexts[without_idx].auto_depth,
            "extended MATCH_RECOGNIZE output modifiers should share the same owner-relative depth"
        );
        assert_eq!(
            contexts[pattern_idx].auto_depth, contexts[without_idx].auto_depth,
            "PATTERN after split extended MATCH_RECOGNIZE modifiers should realign with the owner-relative depth"
        );
        assert_eq!(
            contexts[define_idx].auto_depth, contexts[without_idx].auto_depth,
            "DEFINE after split extended MATCH_RECOGNIZE modifiers should realign with the owner-relative depth"
        );
        assert!(
            contexts[inner_pattern_idx].auto_depth > contexts[without_idx].auto_depth,
            "nested MATCH_RECOGNIZE subclauses should stay deeper than the outer extended modifiers"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_pivot_body_one_level_deeper_than_owner_line() {
        let sql = r#"SELECT pvt.deptno,
    pvt."CLERK" AS clerk_cnt,
    pvt."MANAGER" AS manager_cnt,
    pvt."ANALYST" AS analyst_cnt,
    pvt."SALESMAN" AS salesman_cnt,
    pvt."PRESIDENT" AS president_cnt
FROM (
        SELECT e.deptno,
            e.job
        FROM emp e
    ) PIVOT (
        COUNT (*)
        FOR job IN ('CLERK' AS "CLERK", 'MANAGER' AS "MANAGER", 'ANALYST' AS "ANALYST", 'SALESMAN' AS "SALESMAN", 'PRESIDENT' AS "PRESIDENT")
    ) pvt
ORDER BY pvt.deptno;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();

        let pivot_idx = lines
            .iter()
            .position(|line| line.trim_start().contains(") PIVOT ("))
            .unwrap_or(0);
        let count_idx = lines
            .iter()
            .position(|line| line.trim_start() == "COUNT (*)")
            .unwrap_or(0);
        let for_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("FOR job IN"))
            .unwrap_or(0);
        let close_idx = lines
            .iter()
            .position(|line| line.trim_start() == ") pvt")
            .unwrap_or(0);

        assert_eq!(
            contexts[count_idx].auto_depth,
            contexts[pivot_idx].auto_depth.saturating_add(1),
            "PIVOT aggregate line should be exactly one level deeper than the PIVOT owner line"
        );
        assert_eq!(
            contexts[for_idx].auto_depth,
            contexts[pivot_idx].auto_depth.saturating_add(1),
            "PIVOT FOR line should stay aligned with the aggregate line"
        );
        assert_eq!(
            contexts[close_idx].auto_depth, contexts[pivot_idx].auto_depth,
            "PIVOT closing line should realign with the PIVOT owner line"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_from_before_unpivot_on_select_base_depth() {
        let sql = r#"WITH src AS (
    SELECT deptno,
        job,
        sal
    FROM emp
),
pivoted AS (
    SELECT *
    FROM src PIVOT (
        SUM (sal) AS sum_sal FOR
        deptno IN (10 AS D10, 20 AS D20, 30 AS D30)
    )
)
SELECT job,
    dept_tag,
    sal_amt
FROM pivoted UNPIVOT (
    sal_amt
    FOR dept_tag IN (D10 AS '10', D20 AS '20', D30 AS '30')
)
WHERE sal_amt IS NOT NULL
ORDER BY job,
    dept_tag;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();

        let select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT job,")
            .unwrap_or(0);
        let item_idx = lines
            .iter()
            .position(|line| line.trim_start() == "sal_amt")
            .unwrap_or(0);
        let from_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("FROM pivoted UNPIVOT"))
            .unwrap_or(0);
        let unpivot_value_idx = lines
            .iter()
            .enumerate()
            .skip(from_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "sal_amt")
            .map(|(idx, _)| idx)
            .unwrap_or(0);

        assert_eq!(
            contexts[item_idx].auto_depth,
            contexts[select_idx].auto_depth.saturating_add(1),
            "select-list item should be one level deeper than the SELECT base"
        );
        assert_eq!(
            contexts[from_idx].auto_depth, contexts[select_idx].auto_depth,
            "FROM before UNPIVOT should return to the SELECT base depth instead of staying on the select-list continuation depth"
        );
        assert_eq!(
            contexts[unpivot_value_idx].auto_depth,
            contexts[from_idx].auto_depth.saturating_add(1),
            "UNPIVOT body should be exactly one level deeper than its FROM owner line"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_pivot_xml_owner_stack_across_standalone_open_paren() {
        let sql = r#"SELECT *
FROM (
    SELECT *
    FROM src
    PIVOT XML
    (
        SUM (amt) AS total_amt
        FOR deptno IN (10 AS D10, 20 AS D20)
    )
) pvt
WHERE pvt.total_amt_D10 IS NOT NULL;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let pivot_idx = lines
            .iter()
            .position(|line| line.trim_start() == "PIVOT XML")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(pivot_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let sum_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SUM (amt) AS total_amt")
            .unwrap_or(0);
        let for_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("FOR deptno IN"))
            .unwrap_or(0);
        let outer_where_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE pvt.total_amt_D10 IS NOT NULL;")
            .unwrap_or(0);

        assert_eq!(
            contexts[open_idx].auto_depth, contexts[pivot_idx].auto_depth,
            "split PIVOT XML opener should stay aligned with the modified owner line"
        );
        assert_eq!(
            contexts[sum_idx].auto_depth,
            contexts[pivot_idx].auto_depth.saturating_add(1),
            "PIVOT XML body should stay one level deeper than the modified owner line"
        );
        assert_eq!(
            contexts[for_idx].auto_depth, contexts[sum_idx].auto_depth,
            "PIVOT XML FOR line should stay aligned with the aggregate line"
        );
        assert_eq!(
            contexts[outer_where_idx].auto_depth, 0,
            "outer WHERE should return to the top-level query depth after nested PIVOT XML closes"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_pivot_xml_header_chain_owner_stack_stable() {
        let sql = r#"SELECT *
FROM (
    SELECT *
    FROM src
    PIVOT
    XML
    (
        SUM (amt) AS total_amt
        FOR deptno IN (10 AS D10, 20 AS D20)
    )
) pvt
WHERE pvt.total_amt_D10 IS NOT NULL;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let pivot_idx = lines
            .iter()
            .position(|line| line.trim_start() == "PIVOT")
            .unwrap_or(0);
        let xml_idx = lines
            .iter()
            .position(|line| line.trim_start() == "XML")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(xml_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let sum_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SUM (amt) AS total_amt")
            .unwrap_or(0);
        let outer_where_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE pvt.total_amt_D10 IS NOT NULL;")
            .unwrap_or(0);

        assert_eq!(
            contexts[xml_idx].auto_depth, contexts[pivot_idx].auto_depth,
            "PIVOT XML modifier line should stay aligned with the PIVOT owner depth"
        );
        assert_eq!(
            contexts[open_idx].auto_depth, contexts[pivot_idx].auto_depth,
            "standalone open paren after split PIVOT XML header chain should stay on the owner depth"
        );
        assert_eq!(
            contexts[sum_idx].auto_depth,
            contexts[pivot_idx].auto_depth.saturating_add(1),
            "split PIVOT XML body should stay one level deeper than the original owner line"
        );
        assert_eq!(
            contexts[outer_where_idx].auto_depth, 0,
            "outer WHERE should return to the top-level query depth after split PIVOT XML header chain closes"
        );
    }

    #[test]
    fn auto_format_line_contexts_normalize_overindented_split_pivot_xml_header_chain_to_structural_owner_depth(
    ) {
        let sql = r#"SELECT *
FROM (
    SELECT *
    FROM src
    PIVOT
            XML
                    (
                        SUM (amt) AS total_amt
                        FOR deptno IN (10 AS D10, 20 AS D20)
                    )
) pvt
WHERE pvt.total_amt_D10 IS NOT NULL;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let pivot_idx = lines
            .iter()
            .position(|line| line.trim_start() == "PIVOT")
            .unwrap_or(0);
        let xml_idx = lines
            .iter()
            .position(|line| line.trim_start() == "XML")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(xml_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let sum_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SUM (amt) AS total_amt")
            .unwrap_or(0);

        assert_eq!(
            contexts[xml_idx].auto_depth, contexts[pivot_idx].auto_depth,
            "overindented PIVOT XML modifier should snap back to the PIVOT owner depth"
        );
        assert_eq!(
            contexts[open_idx].auto_depth, contexts[pivot_idx].auto_depth,
            "overindented standalone open paren after PIVOT XML should stay on the PIVOT owner depth"
        );
        assert_eq!(
            contexts[sum_idx].auto_depth,
            contexts[pivot_idx].auto_depth.saturating_add(1),
            "overindented PIVOT XML body should still be one level deeper than the structural owner"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_unpivot_include_nulls_owner_stack_across_standalone_open_paren(
    ) {
        let sql = r#"SELECT *
FROM (
    SELECT job,
        dept_tag,
        sal_amt
    FROM pivoted
    UNPIVOT INCLUDE NULLS
    (
        sal_amt
        FOR dept_tag IN (D10 AS '10', D20 AS '20')
    )
) depivoted
WHERE depivoted.sal_amt IS NOT NULL;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let unpivot_idx = lines
            .iter()
            .position(|line| line.trim_start() == "UNPIVOT INCLUDE NULLS")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(unpivot_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let value_idx = lines
            .iter()
            .enumerate()
            .skip(open_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "sal_amt")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let for_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("FOR dept_tag IN"))
            .unwrap_or(0);
        let outer_where_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE depivoted.sal_amt IS NOT NULL;")
            .unwrap_or(0);

        assert_eq!(
            contexts[open_idx].auto_depth, contexts[unpivot_idx].auto_depth,
            "split UNPIVOT INCLUDE NULLS opener should stay aligned with the modified owner line"
        );
        assert_eq!(
            contexts[value_idx].auto_depth,
            contexts[unpivot_idx].auto_depth.saturating_add(1),
            "UNPIVOT INCLUDE NULLS body should stay one level deeper than the modified owner line"
        );
        assert_eq!(
            contexts[for_idx].auto_depth, contexts[value_idx].auto_depth,
            "UNPIVOT INCLUDE NULLS FOR line should stay aligned with the body line"
        );
        assert_eq!(
            contexts[outer_where_idx].auto_depth, 0,
            "outer WHERE should return to the top-level query depth after nested UNPIVOT INCLUDE NULLS closes"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_unpivot_include_nulls_header_chain_owner_stack_stable()
    {
        let sql = r#"SELECT *
FROM (
    SELECT job,
        dept_tag,
        sal_amt
    FROM pivoted
    UNPIVOT
    INCLUDE NULLS
    (
        sal_amt
        FOR dept_tag IN (D10 AS '10', D20 AS '20')
    )
) depivoted
WHERE depivoted.sal_amt IS NOT NULL;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let unpivot_idx = lines
            .iter()
            .position(|line| line.trim_start() == "UNPIVOT")
            .unwrap_or(0);
        let include_idx = lines
            .iter()
            .position(|line| line.trim_start() == "INCLUDE NULLS")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(include_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let value_idx = lines
            .iter()
            .enumerate()
            .skip(open_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "sal_amt")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let outer_where_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE depivoted.sal_amt IS NOT NULL;")
            .unwrap_or(0);

        assert_eq!(
            contexts[include_idx].auto_depth, contexts[unpivot_idx].auto_depth,
            "UNPIVOT INCLUDE NULLS modifier line should stay aligned with the UNPIVOT owner depth"
        );
        assert_eq!(
            contexts[open_idx].auto_depth, contexts[unpivot_idx].auto_depth,
            "standalone open paren after split UNPIVOT INCLUDE NULLS header chain should stay on the owner depth"
        );
        assert_eq!(
            contexts[value_idx].auto_depth,
            contexts[unpivot_idx].auto_depth.saturating_add(1),
            "split UNPIVOT INCLUDE NULLS body should stay one level deeper than the original owner line"
        );
        assert_eq!(
            contexts[outer_where_idx].auto_depth, 0,
            "outer WHERE should return to the top-level query depth after split UNPIVOT INCLUDE NULLS header chain closes"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_nested_condition_and_wrapper_query_owner_chain() {
        let sql = r#"SELECT
    CASE
        WHEN EXISTS (
            (
                SELECT 1
                FROM dual
            )
        ) THEN 'Y'
        ELSE 'N'
    END AS flag
FROM dual;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let when_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHEN EXISTS (")
            .unwrap_or(0);
        let wrapper_idx = lines
            .iter()
            .position(|line| line.trim_start() == "(")
            .unwrap_or(0);
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT 1")
            .unwrap_or(0);
        let close_then_idx = lines
            .iter()
            .position(|line| line.trim_start() == ") THEN 'Y'")
            .unwrap_or(0);

        assert_eq!(
            contexts[wrapper_idx].condition_header_line,
            Some(when_idx),
            "wrapper paren under WHEN EXISTS should stay attached to the outer condition owner"
        );
        assert_eq!(
            contexts[select_idx].condition_header_line,
            Some(when_idx),
            "nested SELECT should remain attached to the outer WHEN EXISTS owner chain"
        );
        assert_eq!(
            contexts[select_idx].auto_depth,
            contexts[wrapper_idx].auto_depth.saturating_add(1),
            "nested SELECT should be one level deeper than the innermost wrapper owner depth"
        );
        assert_eq!(
            contexts[close_then_idx].condition_role,
            AutoFormatConditionRole::Closer,
            "outer close-paren THEN line should still be treated as the condition closer"
        );
    }

    #[test]
    fn auto_format_line_contexts_treat_same_line_close_and_condition_keyword_as_continuation() {
        let sql = r#"BEGIN
    IF (
        v_ready = 'Y'
    ) AND v_dept = 10 THEN
        NULL;
    END IF;
END;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let if_idx = lines
            .iter()
            .position(|line| line.trim_start() == "IF (")
            .unwrap_or(0);
        let close_and_idx = lines
            .iter()
            .position(|line| line.trim_start() == ") AND v_dept = 10 THEN")
            .unwrap_or(0);
        let null_idx = lines
            .iter()
            .position(|line| line.trim_start() == "NULL;")
            .unwrap_or(0);

        assert_eq!(
            contexts[close_and_idx].condition_role,
            AutoFormatConditionRole::Continuation,
            "same-line `) AND ...` under IF should continue the condition instead of becoming a pure closer"
        );
        assert_eq!(
            contexts[close_and_idx].auto_depth,
            contexts[if_idx].auto_depth.saturating_add(1),
            "same-line `) AND ...` should stay on the IF condition continuation depth"
        );
        assert_eq!(
            contexts[null_idx].auto_depth,
            contexts[if_idx].auto_depth.saturating_add(1),
            "the THEN body should still open exactly one level deeper than the IF owner"
        );
    }

    #[test]
    fn auto_format_line_contexts_treat_query_same_line_close_and_condition_keyword_as_continuation()
    {
        let sql = r#"SELECT o.id
FROM outer_t o
WHERE EXISTS (
    SELECT 1
    FROM inner_t i
    WHERE i.outer_id = o.id
) AND EXISTS (
    SELECT 1
    FROM bonus b
    WHERE b.outer_id = o.id
);"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let where_exists_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE EXISTS (")
            .unwrap_or(0);
        let same_line_and_exists_idx = lines
            .iter()
            .position(|line| line.trim_start() == ") AND EXISTS (")
            .unwrap_or(0);
        let nested_select_idx = lines
            .iter()
            .enumerate()
            .skip(same_line_and_exists_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "SELECT 1")
            .map(|(idx, _)| idx)
            .unwrap_or(0);

        assert_eq!(
            contexts[same_line_and_exists_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "same-line `) AND EXISTS (` should keep the outer WHERE continuation role"
        );
        assert_eq!(
            contexts[same_line_and_exists_idx].auto_depth,
            contexts[where_exists_idx].auto_depth.saturating_add(1),
            "same-line `) AND EXISTS (` should stay on the outer WHERE continuation depth"
        );
        assert_eq!(
            contexts[nested_select_idx].query_base_depth,
            Some(contexts[same_line_and_exists_idx].auto_depth.saturating_add(1)),
            "child SELECT after same-line `) AND EXISTS (` should anchor from the continued owner depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_treat_same_line_close_and_order_by_as_outer_clause_base() {
        let sql = r#"SELECT empno
FROM emp
WHERE deptno IN (
    SELECT deptno
    FROM dept
) ORDER BY empno;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT empno")
            .unwrap_or(0);
        let order_by_idx = lines
            .iter()
            .position(|line| line.trim_start() == ") ORDER BY empno;")
            .unwrap_or(0);

        assert_eq!(
            contexts[order_by_idx].query_role,
            AutoFormatQueryRole::Base,
            "same-line `) ORDER BY ...` should re-enter the outer query clause stack"
        );
        assert_eq!(
            contexts[order_by_idx].query_base_depth,
            Some(contexts[select_idx].auto_depth),
            "same-line `) ORDER BY ...` should keep the outer query base depth"
        );
        assert_eq!(
            contexts[order_by_idx].auto_depth, contexts[select_idx].auto_depth,
            "same-line `) ORDER BY ...` should align with the outer query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_treat_join_same_line_close_and_on_as_join_condition_continuation()
    {
        let sql = r#"SELECT e.empno
FROM emp e
JOIN (
    SELECT d.deptno
    FROM dept d
) ON d.deptno = e.deptno
AND e.status = 'A';"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let join_idx = lines
            .iter()
            .position(|line| line.trim_start() == "JOIN (")
            .unwrap_or(0);
        let on_idx = lines
            .iter()
            .position(|line| line.trim_start() == ") ON d.deptno = e.deptno")
            .unwrap_or(0);
        let and_idx = lines
            .iter()
            .position(|line| line.trim_start() == "AND e.status = 'A';")
            .unwrap_or(0);

        assert!(
            contexts[on_idx].line_semantic.is_join_condition_clause(),
            "same-line `) ON ...` should re-enter the JOIN condition taxonomy"
        );
        assert_eq!(
            contexts[on_idx].query_role,
            AutoFormatQueryRole::Continuation,
            "same-line `) ON ...` should stay on the outer JOIN condition continuation role"
        );
        assert_eq!(
            contexts[on_idx].auto_depth,
            contexts[join_idx].auto_depth.saturating_add(1),
            "same-line `) ON ...` should stay one level deeper than the JOIN owner depth"
        );
        assert_eq!(
            contexts[and_idx].auto_depth,
            contexts[on_idx].auto_depth.saturating_add(1),
            "AND after same-line `) ON ...` should stay one level deeper than the outer JOIN condition depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_window_body_one_level_deeper_than_owner_line() {
        let sql = r#"SELECT e.deptno,
    e.empno,
    SUM (e.sal) OVER w_dept AS dept_sum
FROM emp e
WINDOW w_dept AS (
    PARTITION BY e.deptno
    ORDER BY e.sal DESC, e.empno
)
QUALIFY ROW_NUMBER () OVER w_dept = 1;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let window_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("WINDOW w_dept AS ("))
            .unwrap_or(0);
        let partition_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("PARTITION BY e.deptno"))
            .unwrap_or(0);
        let order_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("ORDER BY e.sal DESC"))
            .unwrap_or(0);
        let close_idx = lines
            .iter()
            .position(|line| line.trim_start() == ")")
            .unwrap_or(0);

        assert_eq!(
            contexts[partition_idx].auto_depth,
            contexts[window_idx].auto_depth.saturating_add(1),
            "WINDOW PARTITION BY should be exactly one level deeper than the WINDOW owner line"
        );
        assert_eq!(
            contexts[order_idx].auto_depth, contexts[partition_idx].auto_depth,
            "WINDOW ORDER BY should stay aligned with WINDOW PARTITION BY"
        );
        assert_eq!(
            contexts[close_idx].auto_depth, contexts[window_idx].auto_depth,
            "WINDOW closing line should realign with the WINDOW owner line"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_window_owner_stack_across_standalone_open_paren() {
        let sql = r#"SELECT *
FROM (
    SELECT e.deptno,
        SUM (e.sal) OVER w_dept AS dept_sum
    FROM emp e
    WINDOW w_dept AS
    (
        PARTITION BY e.deptno
        ORDER BY e.sal DESC, e.empno
    )
    QUALIFY ROW_NUMBER () OVER w_dept = 1
) ranked
WHERE ranked.dept_sum > 0;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let window_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WINDOW w_dept AS")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(window_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let partition_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("PARTITION BY e.deptno"))
            .unwrap_or(0);
        let order_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("ORDER BY e.sal DESC"))
            .unwrap_or(0);
        let qualify_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("QUALIFY ROW_NUMBER"))
            .unwrap_or(0);

        assert_eq!(
            contexts[open_idx].auto_depth, contexts[window_idx].auto_depth,
            "standalone open paren after WINDOW AS should stay on the WINDOW owner depth"
        );
        assert_eq!(
            contexts[partition_idx].auto_depth,
            contexts[window_idx].auto_depth.saturating_add(1),
            "WINDOW body should stay one level deeper than the split WINDOW owner line"
        );
        assert_eq!(
            contexts[order_idx].auto_depth, contexts[partition_idx].auto_depth,
            "split WINDOW ORDER BY should stay aligned with PARTITION BY"
        );
        assert_eq!(
            contexts[qualify_idx].auto_depth, contexts[window_idx].auto_depth,
            "QUALIFY after split WINDOW body should realign to the WINDOW owner depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_window_as_header_chain_owner_stack_stable() {
        let sql = r#"SELECT *
FROM (
    SELECT e.deptno,
        SUM (e.sal) OVER w_dept AS dept_sum
    FROM emp e
    WINDOW w_dept
    AS
    (
        PARTITION BY e.deptno
        ORDER BY e.sal DESC, e.empno
    )
    QUALIFY ROW_NUMBER () OVER w_dept = 1
) ranked
WHERE ranked.dept_sum > 0;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let window_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WINDOW w_dept")
            .unwrap_or(0);
        let as_idx = lines
            .iter()
            .position(|line| line.trim_start() == "AS")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(as_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let partition_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("PARTITION BY e.deptno"))
            .unwrap_or(0);
        let qualify_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("QUALIFY ROW_NUMBER"))
            .unwrap_or(0);

        assert_eq!(
            contexts[as_idx].auto_depth, contexts[window_idx].auto_depth,
            "split WINDOW AS line should stay aligned with the WINDOW owner depth"
        );
        assert_eq!(
            contexts[open_idx].auto_depth, contexts[window_idx].auto_depth,
            "standalone open paren after split WINDOW AS header chain should stay on the WINDOW owner depth"
        );
        assert_eq!(
            contexts[partition_idx].auto_depth,
            contexts[window_idx].auto_depth.saturating_add(1),
            "split WINDOW body should stay one level deeper than the original owner line"
        );
        assert_eq!(
            contexts[qualify_idx].auto_depth, contexts[window_idx].auto_depth,
            "QUALIFY after split WINDOW AS header chain should realign to the WINDOW owner depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_normalize_overindented_split_window_as_chain_to_structural_owner_depth(
    ) {
        let sql = r#"SELECT *
FROM (
    SELECT e.deptno,
        SUM (e.sal) OVER w_dept AS dept_sum
    FROM emp e
    WINDOW w_dept
            AS
                    (
                        PARTITION BY e.deptno
                        ORDER BY e.sal DESC, e.empno
                    )
    QUALIFY ROW_NUMBER () OVER w_dept = 1
) ranked
WHERE ranked.dept_sum > 0;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let window_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WINDOW w_dept")
            .unwrap_or(0);
        let as_idx = lines
            .iter()
            .position(|line| line.trim_start() == "AS")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(as_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let partition_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("PARTITION BY e.deptno"))
            .unwrap_or(0);
        let qualify_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("QUALIFY ROW_NUMBER"))
            .unwrap_or(0);

        assert_eq!(
            contexts[as_idx].auto_depth, contexts[window_idx].auto_depth,
            "overindented WINDOW AS should snap back to the WINDOW owner depth"
        );
        assert_eq!(
            contexts[open_idx].auto_depth, contexts[window_idx].auto_depth,
            "overindented standalone open paren after WINDOW AS should stay on the WINDOW owner depth"
        );
        assert_eq!(
            contexts[partition_idx].auto_depth,
            contexts[window_idx].auto_depth.saturating_add(1),
            "overindented WINDOW body should still derive one level below the structural owner"
        );
        assert_eq!(
            contexts[qualify_idx].auto_depth, contexts[window_idx].auto_depth,
            "QUALIFY after an overindented WINDOW AS chain should realign to the WINDOW owner depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_nested_window_clause_restores_outer_query_depth() {
        let sql = r#"SELECT *
FROM (
    SELECT e.deptno,
        SUM (e.sal) OVER w_dept AS dept_sum
    FROM emp e
    WINDOW w_dept AS (
        PARTITION BY e.deptno
        ORDER BY e.sal DESC, e.empno
    )
    QUALIFY ROW_NUMBER () OVER w_dept = 1
) ranked
WHERE ranked.dept_sum > 0;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let window_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("WINDOW w_dept AS ("))
            .unwrap_or(0);
        let partition_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("PARTITION BY e.deptno"))
            .unwrap_or(0);
        let order_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("ORDER BY e.sal DESC"))
            .unwrap_or(0);
        let qualify_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("QUALIFY ROW_NUMBER"))
            .unwrap_or(0);
        let close_idx = lines
            .iter()
            .position(|line| line.trim_start() == ") ranked")
            .unwrap_or(0);
        let where_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("WHERE ranked.dept_sum"))
            .unwrap_or(0);

        assert_eq!(
            contexts[partition_idx].auto_depth,
            contexts[window_idx].auto_depth.saturating_add(1),
            "nested WINDOW PARTITION BY should stay one level deeper than the nested WINDOW owner"
        );
        assert_eq!(
            contexts[order_idx].auto_depth, contexts[partition_idx].auto_depth,
            "nested WINDOW ORDER BY should stay aligned with nested WINDOW PARTITION BY"
        );
        assert_eq!(
            contexts[qualify_idx].auto_depth, contexts[window_idx].auto_depth,
            "nested QUALIFY should return to the nested WINDOW owner depth after the body closes"
        );
        assert_eq!(
            contexts[close_idx].auto_depth, contexts[where_idx].auto_depth,
            "nested query closing line should return to the outer query base before the outer WHERE resumes"
        );
        assert_eq!(
            contexts[where_idx].auto_depth, 0,
            "outer WHERE should return to the outer query base after the nested WINDOW query closes"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_analytic_over_body_on_owner_stack_after_nested_over() {
        let sql = r#"SELECT
    SUM (sal) OVER (
        PARTITION BY deptno
        ORDER BY (
            SELECT MAX (inner_val) OVER (
                PARTITION BY grp
                ORDER BY inner_val
            )
            FROM dual
        )
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_sal
FROM emp;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let over_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("SUM (sal) OVER ("))
            .unwrap_or(0);
        let outer_partition_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("PARTITION BY deptno"))
            .unwrap_or(0);
        let inner_partition_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("PARTITION BY grp"))
            .unwrap_or(0);
        let rows_idx = lines
            .iter()
            .position(|line| {
                line.trim_start()
                    .starts_with("ROWS BETWEEN UNBOUNDED PRECEDING")
            })
            .unwrap_or(0);
        let close_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with(") AS running_sal"))
            .unwrap_or(0);

        assert_eq!(
            contexts[outer_partition_idx].auto_depth,
            contexts[over_idx].auto_depth.saturating_add(1),
            "analytic OVER subclauses should be exactly one level deeper than the OVER owner line"
        );
        assert_eq!(
            contexts[rows_idx].auto_depth, contexts[outer_partition_idx].auto_depth,
            "ROWS BETWEEN should realign to the outer OVER subclause depth after nested OVER closes"
        );
        assert!(
            contexts[inner_partition_idx].auto_depth > contexts[outer_partition_idx].auto_depth,
            "nested OVER subclauses should stay deeper than the outer OVER subclauses"
        );
        assert_eq!(
            contexts[close_idx].auto_depth, contexts[over_idx].auto_depth,
            "analytic OVER closing line should realign with the OVER owner line"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_within_group_owner_depth_relative_to_nested_query_base()
    {
        let sql = r#"SELECT *
FROM (
    SELECT
        LISTAGG (e.ename, ', ')
        WITHIN GROUP
        (
            ORDER
            BY e.ename
        ) AS names
    FROM emp e
) grouped
WHERE grouped.names IS NOT NULL;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let within_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WITHIN GROUP")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(within_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let order_idx = lines
            .iter()
            .position(|line| line.trim_start() == "ORDER")
            .unwrap_or(0);
        let by_idx = lines
            .iter()
            .position(|line| line.trim_start() == "BY e.ename")
            .unwrap_or(0);
        let close_idx = lines
            .iter()
            .position(|line| line.trim_start() == ") AS names")
            .unwrap_or(0);
        let outer_where_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE grouped.names IS NOT NULL;")
            .unwrap_or(0);

        assert_eq!(
            contexts[open_idx].auto_depth, contexts[within_idx].auto_depth,
            "split WITHIN GROUP opener should stay aligned with the owner line"
        );
        assert_eq!(
            contexts[order_idx].auto_depth,
            contexts[within_idx].auto_depth.saturating_add(1),
            "WITHIN GROUP ORDER should be exactly one level deeper than the owner line"
        );
        assert_eq!(
            contexts[by_idx].auto_depth, contexts[order_idx].auto_depth,
            "split WITHIN GROUP BY should stay aligned with the owner-relative body depth"
        );
        assert_eq!(
            contexts[close_idx].auto_depth, contexts[within_idx].auto_depth,
            "WITHIN GROUP closing line should realign with the owner line"
        );
        assert_eq!(
            contexts[outer_where_idx].auto_depth, 0,
            "outer WHERE should return to the top-level query depth after the nested WITHIN GROUP query closes"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_partial_within_group_owner_chain_relative_to_nested_query_base(
    ) {
        let sql = r#"SELECT *
FROM (
    SELECT
        LISTAGG (e.ename, ', ')
        WITHIN
        GROUP
        (
            ORDER
            BY e.ename
        ) AS names
    FROM emp e
) grouped
WHERE grouped.names IS NOT NULL;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let within_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WITHIN")
            .unwrap_or(0);
        let group_idx = lines
            .iter()
            .position(|line| line.trim_start() == "GROUP")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(group_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let order_idx = lines
            .iter()
            .position(|line| line.trim_start() == "ORDER")
            .unwrap_or(0);
        let by_idx = lines
            .iter()
            .position(|line| line.trim_start() == "BY e.ename")
            .unwrap_or(0);
        let close_idx = lines
            .iter()
            .position(|line| line.trim_start() == ") AS names")
            .unwrap_or(0);
        let outer_where_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE grouped.names IS NOT NULL;")
            .unwrap_or(0);

        assert_eq!(
            contexts[group_idx].auto_depth, contexts[within_idx].auto_depth,
            "split WITHIN/GROUP owner chain should keep the original owner depth"
        );
        assert_eq!(
            contexts[open_idx].auto_depth, contexts[within_idx].auto_depth,
            "standalone open paren after split WITHIN/GROUP should stay aligned with the owner depth"
        );
        assert_eq!(
            contexts[order_idx].auto_depth,
            contexts[within_idx].auto_depth.saturating_add(1),
            "WITHIN/GROUP body should stay one level deeper than the owner line"
        );
        assert_eq!(
            contexts[by_idx].auto_depth, contexts[order_idx].auto_depth,
            "split WITHIN GROUP ORDER/BY should stay aligned with the owner-relative body depth"
        );
        assert_eq!(
            contexts[close_idx].auto_depth, contexts[within_idx].auto_depth,
            "WITHIN/GROUP closing line should realign with the owner depth"
        );
        assert_eq!(
            contexts[outer_where_idx].auto_depth, 0,
            "outer WHERE should return to the top-level query depth after the partial WITHIN/GROUP owner closes"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_keep_body_relative_after_nested_subquery() {
        let sql = r#"SELECT *
FROM (
    SELECT
        MAX (e.sal)
        KEEP
        (
            DENSE_RANK
            LAST
            ORDER
            BY (
                SELECT MAX (b.sal)
                FROM bonus b
                WHERE b.empno = e.empno
            ),
            e.empno
        ) AS top_sal
    FROM emp e
) ranked
WHERE ranked.top_sal > 0;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let keep_idx = lines
            .iter()
            .position(|line| line.trim_start() == "KEEP")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(keep_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let dense_rank_idx = lines
            .iter()
            .position(|line| line.trim_start() == "DENSE_RANK")
            .unwrap_or(0);
        let last_idx = lines
            .iter()
            .position(|line| line.trim_start() == "LAST")
            .unwrap_or(0);
        let order_idx = lines
            .iter()
            .position(|line| line.trim_start() == "ORDER")
            .unwrap_or(0);
        let by_idx = lines
            .iter()
            .position(|line| line.trim_start() == "BY (")
            .unwrap_or(0);
        let inner_select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT MAX (b.sal)")
            .unwrap_or(0);
        let empno_idx = lines
            .iter()
            .position(|line| line.trim_start() == "e.empno")
            .unwrap_or(0);
        let close_idx = lines
            .iter()
            .position(|line| line.trim_start() == ") AS top_sal")
            .unwrap_or(0);
        let outer_where_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE ranked.top_sal > 0;")
            .unwrap_or(0);

        assert_eq!(
            contexts[open_idx].auto_depth, contexts[keep_idx].auto_depth,
            "split KEEP opener should stay aligned with the owner line"
        );
        assert_eq!(
            contexts[dense_rank_idx].auto_depth,
            contexts[keep_idx].auto_depth.saturating_add(1),
            "KEEP body should be exactly one level deeper than the owner line"
        );
        assert_eq!(
            contexts[last_idx].auto_depth, contexts[dense_rank_idx].auto_depth,
            "split KEEP LAST should stay aligned with the owner-relative body depth"
        );
        assert_eq!(
            contexts[order_idx].auto_depth, contexts[dense_rank_idx].auto_depth,
            "split KEEP ORDER should stay aligned with the owner-relative body depth"
        );
        assert_eq!(
            contexts[by_idx].auto_depth, contexts[dense_rank_idx].auto_depth,
            "split KEEP BY should stay aligned with the owner-relative body depth"
        );
        assert!(
            contexts[inner_select_idx].auto_depth > contexts[by_idx].auto_depth,
            "nested SELECT inside KEEP ORDER BY should stay deeper than the KEEP owner body"
        );
        assert_eq!(
            contexts[empno_idx].auto_depth, contexts[dense_rank_idx].auto_depth,
            "KEEP sibling ORDER BY items should realign with the owner-relative body depth after nested subqueries close"
        );
        assert_eq!(
            contexts[close_idx].auto_depth, contexts[keep_idx].auto_depth,
            "KEEP closing line should realign with the owner line"
        );
        assert_eq!(
            contexts[outer_where_idx].auto_depth, 0,
            "outer WHERE should return to the top-level query depth after the nested KEEP query closes"
        );
    }

    #[test]
    fn auto_format_line_contexts_pop_nested_multiline_owner_frames_when_close_parens_share_line() {
        let sql = r#"SELECT
    SUM (sal) OVER (
        PARTITION BY (
            SELECT deptno
            FROM dual
        ))
FROM emp;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let over_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("SUM (sal) OVER ("))
            .unwrap_or(0);
        let close_idx = lines
            .iter()
            .position(|line| line.trim_start() == "))")
            .unwrap_or(0);
        let from_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("FROM emp"))
            .unwrap_or(0);

        assert_eq!(
            contexts[close_idx].auto_depth, contexts[over_idx].auto_depth,
            "shared-line closes should fully pop nested multiline owners and align the close line with the outer owner"
        );
        assert_eq!(
            contexts[from_idx].auto_depth, 0,
            "FROM after a shared-line nested close should return to the query base instead of inheriting a stale multiline owner depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_nested_model_inside_over_uses_innermost_owner_relative_stack() {
        let sql = r#"SELECT
    SUM (sal) OVER (
        PARTITION BY deptno
        ORDER BY (
            SELECT amount
            FROM sales
            MODEL
                PARTITION BY (deptno)
                DIMENSION BY (month_key)
                RETURN ALL ROWS
                RULES
                UPSERT ALL
                AUTOMATIC ORDER
                ITERATE (3)
                UNTIL (
                    SELECT 1
                    FROM dual
                )
                (
                    amount[ANY] = amount[CV()] + 1
                )
        )
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_sal
FROM emp;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let partition_indices: Vec<usize> = lines
            .iter()
            .enumerate()
            .filter_map(|(idx, line)| line.trim_start().starts_with("PARTITION BY").then_some(idx))
            .collect();
        let outer_partition_idx = partition_indices.first().copied().unwrap_or(0);
        let inner_partition_idx = partition_indices.get(1).copied().unwrap_or(0);
        let return_idx = lines
            .iter()
            .position(|line| line.trim_start() == "RETURN ALL ROWS")
            .unwrap_or(0);
        let upsert_idx = lines
            .iter()
            .position(|line| line.trim_start() == "UPSERT ALL")
            .unwrap_or(0);
        let automatic_idx = lines
            .iter()
            .position(|line| line.trim_start() == "AUTOMATIC ORDER")
            .unwrap_or(0);
        let iterate_idx = lines
            .iter()
            .position(|line| line.trim_start() == "ITERATE (3)")
            .unwrap_or(0);
        let until_idx = lines
            .iter()
            .position(|line| line.trim_start() == "UNTIL (")
            .unwrap_or(0);
        let until_select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT 1")
            .unwrap_or(0);
        let rows_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("ROWS BETWEEN"))
            .unwrap_or(0);

        assert!(
            contexts[inner_partition_idx].auto_depth > contexts[outer_partition_idx].auto_depth,
            "inner MODEL subclauses inside OVER should stay deeper than the outer OVER body"
        );
        assert_eq!(
            contexts[return_idx].auto_depth, contexts[inner_partition_idx].auto_depth,
            "RETURN ALL ROWS should stay on the inner MODEL owner-relative subclause depth"
        );
        assert_eq!(
            contexts[upsert_idx].auto_depth, contexts[inner_partition_idx].auto_depth,
            "UPSERT ALL should stay on the inner MODEL owner-relative subclause depth"
        );
        assert_eq!(
            contexts[automatic_idx].auto_depth, contexts[inner_partition_idx].auto_depth,
            "AUTOMATIC ORDER should stay on the inner MODEL owner-relative subclause depth"
        );
        assert_eq!(
            contexts[iterate_idx].auto_depth, contexts[inner_partition_idx].auto_depth,
            "ITERATE should stay on the inner MODEL owner-relative subclause depth"
        );
        assert_eq!(
            contexts[until_idx].auto_depth, contexts[inner_partition_idx].auto_depth,
            "UNTIL should stay on the inner MODEL owner-relative subclause depth"
        );
        assert!(
            contexts[until_select_idx].auto_depth > contexts[until_idx].auto_depth,
            "nested SELECT inside MODEL UNTIL should stay deeper than the UNTIL owner line"
        );
        assert_eq!(
            contexts[rows_idx].auto_depth, contexts[outer_partition_idx].auto_depth,
            "outer OVER frame clauses should realign to the outer owner-relative depth after the inner MODEL closes"
        );
    }

    #[test]
    fn auto_format_line_contexts_model_reference_subclauses_restore_outer_depth_after_nested_subquery(
    ) {
        let sql = r#"SELECT deptno,
    amount
FROM sales
MODEL
    REFERENCE ref_limits ON (
        SELECT limit_amt
        FROM limits l
        WHERE l.deptno = sales.deptno
    )
    DIMENSION BY (month_key)
    MEASURES (amount)
    RULES UPDATE (
        amount[ANY] = amount[CV()] + 1
    )
ORDER BY deptno;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let reference_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("REFERENCE ref_limits ON ("))
            .unwrap_or(0);
        let inner_select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT limit_amt")
            .unwrap_or(0);
        let dimension_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("DIMENSION BY (month_key)"))
            .unwrap_or(0);
        let rules_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("RULES UPDATE ("))
            .unwrap_or(0);
        let rules_body_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("amount[ANY] ="))
            .unwrap_or(0);
        let order_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("ORDER BY deptno;"))
            .unwrap_or(0);

        assert!(
            contexts[inner_select_idx].auto_depth > contexts[reference_idx].auto_depth,
            "nested SELECT inside MODEL REFERENCE ON should stay deeper than the REFERENCE owner line"
        );
        assert_eq!(
            contexts[dimension_idx].auto_depth, contexts[reference_idx].auto_depth,
            "DIMENSION BY after MODEL REFERENCE ON should realign to the REFERENCE owner depth after nested subquery closes"
        );
        assert_eq!(
            contexts[rules_body_idx].auto_depth,
            contexts[rules_idx].auto_depth.saturating_add(1),
            "MODEL RULES body after REFERENCE should stay exactly one level deeper than the RULES owner line"
        );
        assert_eq!(
            contexts[order_idx].auto_depth, 0,
            "ORDER BY after MODEL REFERENCE should return to the top-level clause depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_model_subclauses_restore_outer_depth_after_nested_subquery() {
        let sql = r#"SELECT deptno,
    amount
FROM sales
MODEL
    PARTITION BY (deptno)
    DIMENSION BY (month_key)
    MEASURES (
        (
            SELECT limit_amt
            FROM limits l
            WHERE l.deptno = sales.deptno
        ) cap,
        amount
    )
    RULES UPDATE (
        amount[ANY] = cap[CV()] * 1.1
    )
ORDER BY deptno;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let measures_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("MEASURES ("))
            .unwrap_or(0);
        let inner_select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT limit_amt")
            .unwrap_or(0);
        let rules_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("RULES UPDATE ("))
            .unwrap_or(0);
        let rules_body_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("amount[ANY] ="))
            .unwrap_or(0);
        let order_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("ORDER BY deptno;"))
            .unwrap_or(0);

        assert!(
            contexts[inner_select_idx].auto_depth > contexts[measures_idx].auto_depth,
            "nested SELECT inside MODEL MEASURES should stay deeper than the MEASURES owner line"
        );
        assert_eq!(
            contexts[rules_body_idx].auto_depth,
            contexts[rules_idx].auto_depth.saturating_add(1),
            "MODEL RULES body should stay exactly one level deeper than the RULES owner line"
        );
        assert_eq!(
            contexts[order_idx].auto_depth, 0,
            "ORDER BY after MODEL should return to the top-level clause depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_extended_model_rule_modifiers_restore_owner_depth_after_nested_until_query(
    ) {
        let sql = r#"SELECT deptno,
    amount
FROM sales
MODEL
    PARTITION BY (deptno)
    DIMENSION BY (month_key)
    MEASURES (
        (
            SELECT limit_amt
            FROM limits l
            WHERE l.deptno = sales.deptno
        ) cap,
        amount
    )
    RETURN ALL ROWS
    RULES
    UPSERT ALL
    AUTOMATIC ORDER
    ITERATE (3)
    UNTIL (
        SELECT 1
        FROM dual
    )
    (
        amount[ANY] = cap[CV()] * 1.1
    )
ORDER BY deptno;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let partition_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("PARTITION BY"))
            .unwrap_or(0);
        let upsert_idx = lines
            .iter()
            .position(|line| line.trim_start() == "UPSERT ALL")
            .unwrap_or(0);
        let return_idx = lines
            .iter()
            .position(|line| line.trim_start() == "RETURN ALL ROWS")
            .unwrap_or(0);
        let automatic_idx = lines
            .iter()
            .position(|line| line.trim_start() == "AUTOMATIC ORDER")
            .unwrap_or(0);
        let iterate_idx = lines
            .iter()
            .position(|line| line.trim_start() == "ITERATE (3)")
            .unwrap_or(0);
        let until_idx = lines
            .iter()
            .position(|line| line.trim_start() == "UNTIL (")
            .unwrap_or(0);
        let inner_select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT 1")
            .unwrap_or(0);
        let order_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("ORDER BY deptno;"))
            .unwrap_or(0);

        assert_eq!(
            contexts[return_idx].auto_depth, contexts[partition_idx].auto_depth,
            "RETURN ALL ROWS should stay on the MODEL owner-relative subclause depth"
        );
        assert_eq!(
            contexts[upsert_idx].auto_depth, contexts[partition_idx].auto_depth,
            "UPSERT ALL should stay on the MODEL owner-relative subclause depth"
        );
        assert_eq!(
            contexts[automatic_idx].auto_depth, contexts[partition_idx].auto_depth,
            "AUTOMATIC ORDER should stay on the MODEL owner-relative subclause depth"
        );
        assert_eq!(
            contexts[iterate_idx].auto_depth, contexts[partition_idx].auto_depth,
            "ITERATE should stay on the MODEL owner-relative subclause depth"
        );
        assert_eq!(
            contexts[until_idx].auto_depth, contexts[partition_idx].auto_depth,
            "UNTIL should stay on the MODEL owner-relative subclause depth"
        );
        assert!(
            contexts[inner_select_idx].auto_depth > contexts[until_idx].auto_depth,
            "nested SELECT inside MODEL UNTIL should stay deeper than the UNTIL owner line"
        );
        assert_eq!(
            contexts[order_idx].auto_depth, 0,
            "ORDER BY after MODEL should return to the top-level clause depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_update_and_sequential_model_rule_modifiers_restore_owner_depth_after_nested_until_query(
    ) {
        let sql = r#"SELECT deptno,
    amount
FROM sales
MODEL
    PARTITION BY (deptno)
    DIMENSION BY (month_key)
    MEASURES (
        (
            SELECT limit_amt
            FROM limits l
            WHERE l.deptno = sales.deptno
        ) cap,
        amount
    )
    RETURN UPDATED ROWS
    RULES
    UPDATE
    SEQUENTIAL ORDER
    ITERATE (3)
    UNTIL (
        SELECT 1
        FROM dual
    )
    (
        amount[ANY] = cap[CV()] * 1.1
    )
ORDER BY deptno;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let partition_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("PARTITION BY"))
            .unwrap_or(0);
        let return_idx = lines
            .iter()
            .position(|line| line.trim_start() == "RETURN UPDATED ROWS")
            .unwrap_or(0);
        let update_idx = lines
            .iter()
            .position(|line| line.trim_start() == "UPDATE")
            .unwrap_or(0);
        let sequential_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SEQUENTIAL ORDER")
            .unwrap_or(0);
        let iterate_idx = lines
            .iter()
            .position(|line| line.trim_start() == "ITERATE (3)")
            .unwrap_or(0);
        let until_idx = lines
            .iter()
            .position(|line| line.trim_start() == "UNTIL (")
            .unwrap_or(0);
        let inner_select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT 1")
            .unwrap_or(0);
        let order_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("ORDER BY deptno;"))
            .unwrap_or(0);

        assert_eq!(
            contexts[return_idx].auto_depth, contexts[partition_idx].auto_depth,
            "RETURN UPDATED ROWS should stay on the MODEL owner-relative subclause depth"
        );
        assert_eq!(
            contexts[update_idx].auto_depth, contexts[partition_idx].auto_depth,
            "UPDATE should stay on the MODEL owner-relative subclause depth"
        );
        assert_eq!(
            contexts[sequential_idx].auto_depth, contexts[partition_idx].auto_depth,
            "SEQUENTIAL ORDER should stay on the MODEL owner-relative subclause depth"
        );
        assert_eq!(
            contexts[iterate_idx].auto_depth, contexts[partition_idx].auto_depth,
            "ITERATE should stay on the MODEL owner-relative subclause depth"
        );
        assert_eq!(
            contexts[until_idx].auto_depth, contexts[partition_idx].auto_depth,
            "UNTIL should stay on the MODEL owner-relative subclause depth"
        );
        assert!(
            contexts[inner_select_idx].auto_depth > contexts[until_idx].auto_depth,
            "nested SELECT inside MODEL UNTIL should stay deeper than the UNTIL owner line"
        );
        assert_eq!(
            contexts[order_idx].auto_depth, 0,
            "ORDER BY after MODEL should return to the top-level clause depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_nested_columns_bodies_on_owner_stack() {
        let sql = r#"SELECT jt.order_id,
    jt.sku
FROM JSON_TABLE(
    payload,
    '$' COLUMNS (
        order_id NUMBER PATH '$.order_id',
        NESTED PATH '$.items[*]' COLUMNS (
            sku VARCHAR2 (30) PATH '$.sku',
            qty NUMBER PATH '$.qty'
        )
    )
) jt;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let columns_idx = lines
            .iter()
            .position(|line| line.trim_start().contains("COLUMNS ("))
            .unwrap_or(0);
        let nested_columns_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("NESTED PATH"))
            .unwrap_or(0);
        let order_id_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("order_id NUMBER"))
            .unwrap_or(0);
        let sku_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("sku VARCHAR2"))
            .unwrap_or(0);
        let close_indices: Vec<usize> = lines
            .iter()
            .enumerate()
            .filter_map(|(idx, line)| (line.trim_start() == ")").then_some(idx))
            .collect();
        let nested_close_idx = close_indices.first().copied().unwrap_or(0);
        let outer_close_idx = close_indices.get(1).copied().unwrap_or(0);

        assert_eq!(
            contexts[order_id_idx].auto_depth,
            contexts[columns_idx].auto_depth.saturating_add(1),
            "table-function COLUMNS items should be exactly one level deeper than the outer COLUMNS owner"
        );
        assert_eq!(
            contexts[nested_columns_idx].auto_depth, contexts[order_id_idx].auto_depth,
            "nested COLUMNS headers should align with sibling outer COLUMNS items"
        );
        assert_eq!(
            contexts[sku_idx].auto_depth,
            contexts[nested_columns_idx].auto_depth.saturating_add(1),
            "nested COLUMNS items should be exactly one level deeper than their nested owner"
        );
        assert_eq!(
            contexts[nested_close_idx].auto_depth, contexts[nested_columns_idx].auto_depth,
            "nested COLUMNS closing paren should realign with the nested owner"
        );
        assert_eq!(
            contexts[outer_close_idx].auto_depth, contexts[columns_idx].auto_depth,
            "outer COLUMNS closing paren should realign with the outer owner"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_nested_path_columns_header_chain_on_owner_stack() {
        let sql = r#"SELECT jt.order_id,
    jt.sku
FROM JSON_TABLE(
    payload,
    '$' COLUMNS (
        order_id NUMBER PATH '$.order_id',
        NESTED PATH '$.items[*]'
        COLUMNS
        (
            sku VARCHAR2 (30) PATH '$.sku',
            qty NUMBER PATH '$.qty'
        )
    )
) jt;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let nested_path_idx = lines
            .iter()
            .position(|line| line.trim_start() == "NESTED PATH '$.items[*]'")
            .unwrap_or(0);
        let columns_idx = lines
            .iter()
            .position(|line| line.trim_start() == "COLUMNS")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(columns_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let sku_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("sku VARCHAR2"))
            .unwrap_or(0);

        assert_eq!(
            contexts[columns_idx].auto_depth, contexts[nested_path_idx].auto_depth,
            "split nested COLUMNS header should stay aligned with the NESTED PATH owner depth"
        );
        assert_eq!(
            contexts[open_idx].auto_depth, contexts[columns_idx].auto_depth,
            "split nested COLUMNS opener should stay aligned with the completed COLUMNS owner"
        );
        assert_eq!(
            contexts[sku_idx].auto_depth,
            contexts[columns_idx].auto_depth.saturating_add(1),
            "split nested COLUMNS items should be exactly one level deeper than their nested owner"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_nested_columns_header_without_path_keyword_on_owner_stack(
    ) {
        let sql = r#"SELECT jt.order_id,
    jt.sku
FROM JSON_TABLE(
    payload,
    '$' COLUMNS (
        order_id NUMBER PATH '$.order_id',
        NESTED '$.items[*]'
        COLUMNS
        (
            sku VARCHAR2 (30) PATH '$.sku',
            qty NUMBER PATH '$.qty'
        )
    )
) jt;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let order_id_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("order_id NUMBER"))
            .unwrap_or(0);
        let nested_idx = lines
            .iter()
            .position(|line| line.trim_start() == "NESTED '$.items[*]'")
            .unwrap_or(0);
        let columns_idx = lines
            .iter()
            .position(|line| line.trim_start() == "COLUMNS")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(columns_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let sku_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("sku VARCHAR2"))
            .unwrap_or(0);

        assert_eq!(
            contexts[nested_idx].auto_depth, contexts[order_id_idx].auto_depth,
            "PATH 생략형 nested owner should stay aligned with sibling outer COLUMNS items"
        );
        assert_eq!(
            contexts[columns_idx].auto_depth, contexts[nested_idx].auto_depth,
            "split nested COLUMNS header without PATH should stay on the NESTED owner depth"
        );
        assert_eq!(
            contexts[open_idx].auto_depth, contexts[columns_idx].auto_depth,
            "split nested COLUMNS opener without PATH should stay aligned with the completed owner"
        );
        assert_eq!(
            contexts[sku_idx].auto_depth,
            contexts[columns_idx].auto_depth.saturating_add(1),
            "split nested COLUMNS body without PATH should stay one level deeper than the nested owner"
        );
    }

    #[test]
    fn line_block_depths_dedents_all_leading_closing_parens_on_line() {
        let sql = "SELECT *\nFROM (\nSELECT *\nFROM (\nSELECT 1 FROM dual\n))\nWHERE 1 = 1;";

        let depths = QueryExecutor::line_block_depths(sql);
        assert_eq!(depths, vec![0, 0, 1, 1, 2, 0, 0]);
    }

    #[test]
    fn line_block_depths_dedents_with_leading_block_comment_before_closing_paren() {
        let sql = "SELECT *
FROM (
SELECT 1 FROM dual
/* close */ )
WHERE 1 = 1;";

        let depths = QueryExecutor::line_block_depths(sql);
        assert_eq!(depths, vec![0, 0, 1, 0, 0]);
    }

    #[test]
    fn line_block_depths_dedents_with_leading_whitespace_and_comment_between_closing_parens() {
        let sql = "SELECT *
FROM (
SELECT *
FROM (
SELECT 1 FROM dual
) /* mid */ )
WHERE 1 = 1;";

        let depths = QueryExecutor::line_block_depths(sql);
        assert_eq!(depths, vec![0, 0, 1, 1, 2, 0, 0]);
    }

    #[test]
    fn auto_format_line_contexts_align_comment_prefixed_query_close_with_owner_depth() {
        let sql = "SELECT *
FROM (
SELECT 1
FROM dual
/* close */ )
WHERE 1 = 1;";
        let lines: Vec<&str> = sql.lines().collect();
        let contexts = QueryExecutor::auto_format_line_contexts(sql);

        let from_idx = lines
            .iter()
            .position(|line| line.trim_start() == "FROM (")
            .unwrap_or(0);
        let close_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("/* close */ )"))
            .unwrap_or(0);
        let where_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("WHERE"))
            .unwrap_or(0);

        assert_eq!(
            contexts[close_idx].auto_depth, contexts[from_idx].auto_depth,
            "comment-prefixed close paren should return to the query owner depth"
        );
        assert_eq!(
            contexts[where_idx].auto_depth, contexts[from_idx].auto_depth,
            "outer WHERE should stay on the query owner depth after the comment-prefixed close"
        );
    }

    #[test]
    fn line_block_depths_dedents_stops_counting_at_leading_line_comment() {
        let sql = "SELECT *
FROM (
SELECT 1 FROM dual
-- )
)
WHERE 1 = 1;";

        let depths = QueryExecutor::line_block_depths(sql);
        assert_eq!(depths, vec![0, 0, 1, 1, 0, 0]);
    }
    #[test]
    fn line_block_depths_saturates_when_leading_closing_parens_exceed_depth() {
        let sql = "SELECT 1\nFROM dual\n)))\nSELECT 2\nFROM dual";

        let depths = QueryExecutor::line_block_depths(sql);
        assert_eq!(depths, vec![0, 0, 0, 0, 0]);
    }

    #[test]
    fn line_block_depths_leading_closes_only_dedent_subquery_parens() {
        let sql = "SELECT *
FROM (
  SELECT *
  FROM (
    SELECT NVL(
      (
        SELECT MAX(col)
        FROM t
      )
    ) AS max_col
    FROM dual
  )
)
WHERE 1 = 1;";

        let depths = QueryExecutor::line_block_depths(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let close_expr_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with(") AS max_col"))
            .unwrap_or(0);
        let from_dual_idx = lines
            .iter()
            .position(|line| line.trim_start() == "FROM dual")
            .unwrap_or(0);

        assert_eq!(
            depths[close_expr_idx], depths[from_dual_idx],
            "closing scalar-expression parens must not dedent outer FROM-subquery depth"
        );
    }

    #[test]
    fn line_block_depths_keeps_depth_with_multiple_non_subquery_leading_closes() {
        let sql = "SELECT *
FROM (
  SELECT *
  FROM (
    SELECT COALESCE(
      (
        SELECT MAX(col)
        FROM t
      )
    , 0
    ) AS max_col
    FROM dual
  )
)
WHERE 1 = 1;";

        let depths = QueryExecutor::line_block_depths(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let close_expr_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with(") AS max_col"))
            .unwrap_or(0);
        let from_dual_idx = lines
            .iter()
            .position(|line| line.trim_start() == "FROM dual")
            .unwrap_or(0);

        assert_eq!(
            depths[close_expr_idx], depths[from_dual_idx],
            "multiple non-subquery closes on same line must not over-dedent outer subquery"
        );
    }

    #[test]
    fn line_block_depths_dedents_package_body_initializer_scope_by_one_level() {
        let sql = r#"CREATE OR REPLACE PACKAGE BODY fmt_pkg_extreme AS
g_last_mode VARCHAR2 (30) := 'BOOT';
FUNCTION calc_mode RETURN VARCHAR2 IS
BEGIN
    RETURN
    CASE
        WHEN 1 = 1 THEN
            'WEEKDAY_BOOT'
        ELSE
            'WEEKEND_BOOT'
    END;
END calc_mode;

BEGIN
    g_last_mode :=
    CASE
        WHEN 1 = 1 THEN
            'WEEKDAY_BOOT'
        ELSE
            'WEEKEND_BOOT'
    END;
END fmt_pkg_extreme;"#;

        let depths = QueryExecutor::line_block_depths(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let begin_idx = lines
            .windows(2)
            .position(|pair| pair[0].trim() == "BEGIN" && pair[1].trim() == "g_last_mode :=")
            .unwrap_or(0);
        let assign_idx = lines
            .iter()
            .position(|line| line.trim() == "g_last_mode :=")
            .unwrap_or(0);
        let end_idx = lines
            .iter()
            .position(|line| line.trim() == "END fmt_pkg_extreme;")
            .unwrap_or(0);

        assert_eq!(
            depths[begin_idx], 0,
            "package body initializer BEGIN should align with package scope"
        );
        assert_eq!(
            depths[assign_idx], 1,
            "initializer body statements should be indented exactly one level"
        );
        assert_eq!(
            depths[end_idx], 0,
            "package body END label should return to top-level depth"
        );
    }

    #[test]
    fn line_block_depths_keep_if_scope_after_parenthesized_case_expression_continues() {
        let sql = r#"BEGIN
    IF (
        CASE
            WHEN flag = 'Y' THEN 1
            ELSE 0
        END
    ) = 1 THEN
        NULL;
    END IF;
END;"#;

        let depths = QueryExecutor::line_block_depths(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let if_idx = lines
            .iter()
            .position(|line| line.trim() == "IF (")
            .unwrap_or(0);
        let close_paren_idx = lines
            .iter()
            .position(|line| line.trim() == ") = 1 THEN")
            .unwrap_or(0);
        let null_idx = lines
            .iter()
            .position(|line| line.trim() == "NULL;")
            .unwrap_or(0);
        let end_if_idx = lines
            .iter()
            .position(|line| line.trim() == "END IF;")
            .unwrap_or(0);

        assert_eq!(
            depths[close_paren_idx], depths[if_idx],
            "closing a parenthesized CASE inside IF condition should not end the IF scope early"
        );
        assert_eq!(
            depths[null_idx],
            depths[if_idx].saturating_add(1),
            "statement after THEN should still be one level deeper than IF"
        );
        assert_eq!(
            depths[end_if_idx], depths[if_idx],
            "END IF should stay aligned with IF after parenthesized CASE condition continuations"
        );
    }

    #[test]
    fn line_block_depths_close_case_before_for_loop_header_loop() {
        let sql = r#"DECLARE
    v_x NUMBER := 1;
BEGIN
    FOR i IN 1..CASE WHEN v_x = 1 THEN 5 ELSE 10 END LOOP
        NULL;
    END LOOP;
END;"#;

        let depths = QueryExecutor::line_block_depths(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let for_idx = lines
            .iter()
            .position(|line| line.trim() == "FOR i IN 1..CASE WHEN v_x = 1 THEN 5 ELSE 10 END LOOP")
            .unwrap_or(0);
        let body_idx = lines
            .iter()
            .position(|line| line.trim() == "NULL;")
            .unwrap_or(0);
        let end_loop_idx = lines
            .iter()
            .position(|line| line.trim() == "END LOOP;")
            .unwrap_or(0);
        let end_idx = lines
            .iter()
            .rposition(|line| line.trim() == "END;")
            .unwrap_or(0);

        assert_eq!(
            depths[for_idx], 1,
            "FOR header should stay at outer BEGIN body depth"
        );
        assert_eq!(
            depths[body_idx],
            depths[for_idx].saturating_add(1),
            "loop body should indent exactly one level deeper than the FOR header"
        );
        assert_eq!(
            depths[end_loop_idx], depths[for_idx],
            "END LOOP should align with the FOR header after CASE expression range"
        );
        assert_eq!(
            depths[end_idx], 0,
            "final END should return to top-level depth after CASE expression loop header"
        );
    }

    #[test]
    fn line_block_depths_treats_if_alias_like_regular_identifier_alias() {
        let sql_with_if_alias = "SELECT\nif.a,\nif.b\nFROM tablename if\nWHERE if.a IS NOT NULL;";
        let sql_with_regular_alias =
            "SELECT\nt1.a,\nt1.b\nFROM tablename t1\nWHERE t1.a IS NOT NULL;";

        let if_alias_depths = QueryExecutor::line_block_depths(sql_with_if_alias);
        let regular_alias_depths = QueryExecutor::line_block_depths(sql_with_regular_alias);

        assert_eq!(
            if_alias_depths, regular_alias_depths,
            "IF alias depth calculation should match non-keyword aliases"
        );
    }

    #[test]
    fn line_block_depths_package_body_procedure_select_if_dot_alias_does_not_open_if_block() {
        let sql = r#"CREATE OR REPLACE PACKAGE BODY pkg_test AS
  PROCEDURE p_test IS
  BEGIN
    SELECT if_tab.name
      INTO v_name
      FROM employees if_tab;
  END p_test;
END pkg_test;"#;

        let depths = QueryExecutor::line_block_depths(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("SELECT if_tab.name"))
            .unwrap_or(0);
        let into_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("INTO v_name"))
            .unwrap_or(0);
        let from_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("FROM employees if_tab"))
            .unwrap_or(0);

        assert_eq!(depths[select_idx], 2);
        assert_eq!(depths[into_idx], 2);
        assert_eq!(depths[from_idx], 2);
    }

    #[test]
    fn auto_format_line_contexts_keep_nested_from_subquery_on_parent_query_base_depth() {
        let sql = r#"create package body a as
    procedure b (c in number) as
    begin
        open cv for
            select 1
            from e
            where f in (
                    select 1
                    from (
                            select g
                            from dual
                        )
                );
    end b;
end a;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let find_line = |needle: &str| -> usize {
            lines
                .iter()
                .position(|line| line.trim() == needle)
                .unwrap_or(0)
        };
        let inner_from_paren_idx = find_line("from (");
        let inner_select_idx = (0..=inner_from_paren_idx)
            .rev()
            .find(|idx| lines[*idx].trim() == "select 1")
            .unwrap_or(0);
        let deepest_select_idx = find_line("select g");

        assert_eq!(
            contexts[inner_from_paren_idx].auto_depth, contexts[inner_select_idx].auto_depth,
            "FROM ( line inside nested query should stay on the parent query base depth"
        );
        assert_eq!(
            contexts[deepest_select_idx].auto_depth,
            contexts[inner_from_paren_idx].auto_depth.saturating_add(1),
            "SELECT under FROM ( should stay exactly one child-query level below FROM owner depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_generic_split_expression_query_owners_relative_to_each_owner()
    {
        let sql = r#"SELECT
    d.deptno,
    CURSOR -- employees
    (
        SELECT
            e.empno,
            MULTISET -- bonuses
            (
                SELECT
                    b.bonus
                FROM bonus b
                WHERE b.empno = e.empno
            ) AS bonus_list
        FROM emp e
        WHERE e.deptno = d.deptno
    ) AS emp_cur
FROM dept d;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let find_line = |needle: &str| -> usize {
            lines
                .iter()
                .position(|line| line.trim_start() == needle)
                .unwrap_or(0)
        };
        let cursor_idx = find_line("CURSOR -- employees");
        let cursor_open_idx = find_line("(");
        let nested_select_idx = lines
            .iter()
            .enumerate()
            .skip(cursor_open_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "SELECT")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let multiset_idx = find_line("MULTISET -- bonuses");
        let multiset_open_idx = lines
            .iter()
            .enumerate()
            .skip(multiset_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let deepest_select_idx = lines
            .iter()
            .enumerate()
            .skip(multiset_open_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "SELECT")
            .map(|(idx, _)| idx)
            .unwrap_or(0);

        assert_eq!(
            contexts[cursor_open_idx].auto_depth, contexts[cursor_idx].auto_depth,
            "split CURSOR opener should stay aligned with the CURSOR owner depth"
        );
        assert_eq!(
            contexts[nested_select_idx].auto_depth,
            contexts[cursor_idx].auto_depth.saturating_add(1),
            "SELECT under split CURSOR should stay exactly one level deeper than the CURSOR owner"
        );
        assert_eq!(
            contexts[multiset_open_idx].auto_depth, contexts[multiset_idx].auto_depth,
            "split MULTISET opener should stay aligned with the MULTISET owner depth"
        );
        assert_eq!(
            contexts[deepest_select_idx].auto_depth,
            contexts[multiset_idx].auto_depth.saturating_add(1),
            "SELECT under split MULTISET should stay exactly one level deeper than the MULTISET owner"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_join_modifier_owner_chain_relative_to_nested_query_base(
    ) {
        let sql = r#"SELECT *
FROM dept d
WHERE EXISTS (
    SELECT 1
    FROM emp e
    LEFT OUTER
    JOIN
    (
        SELECT b.empno
        FROM bonus b
        WHERE b.empno = e.empno
    ) bonus_view
    ON bonus_view.empno = e.empno
)
AND d.status = 'A';"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let find_line = |needle: &str| -> usize {
            lines
                .iter()
                .position(|line| line.trim_start() == needle)
                .unwrap_or(0)
        };
        let from_idx = find_line("FROM emp e");
        let left_outer_idx = find_line("LEFT OUTER");
        let join_idx = find_line("JOIN");
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(join_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let nested_select_idx = lines
            .iter()
            .enumerate()
            .skip(open_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "SELECT b.empno")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let close_idx = lines
            .iter()
            .enumerate()
            .skip(nested_select_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start().starts_with(") bonus_view"))
            .map(|(idx, _)| idx)
            .unwrap_or(0);

        assert_eq!(
            contexts[left_outer_idx].auto_depth, contexts[from_idx].auto_depth,
            "split LEFT OUTER modifier line should stay on the nested FROM-item owner depth"
        );
        assert_eq!(
            contexts[join_idx].auto_depth, contexts[left_outer_idx].auto_depth,
            "split JOIN line should stay aligned with the preserved modifier-owner depth"
        );
        assert_eq!(
            contexts[open_idx].auto_depth, contexts[join_idx].auto_depth,
            "standalone open paren after split JOIN should stay on the JOIN owner depth"
        );
        assert_eq!(
            contexts[nested_select_idx].query_base_depth,
            Some(contexts[join_idx].auto_depth.saturating_add(1)),
            "child SELECT under split JOIN should anchor its query base from the JOIN owner depth"
        );
        assert_eq!(
            contexts[nested_select_idx].auto_depth,
            contexts[join_idx].auto_depth.saturating_add(1),
            "child SELECT under split JOIN should stay exactly one level deeper than the JOIN owner"
        );
        assert_eq!(
            contexts[close_idx].auto_depth, contexts[join_idx].auto_depth,
            "split JOIN closing paren should realign with the JOIN owner depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_apply_modifier_owner_chain_relative_to_nested_query_base(
    ) {
        let sql = r#"SELECT *
FROM dept d
WHERE EXISTS (
    SELECT 1
    FROM emp e
    CROSS
    APPLY
    (
        SELECT MAX (b.sal) AS max_sal
        FROM bonus b
        WHERE b.empno = e.empno
    ) bonus_view
)
AND d.status = 'A';"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let find_line = |needle: &str| -> usize {
            lines
                .iter()
                .position(|line| line.trim_start() == needle)
                .unwrap_or(0)
        };
        let from_idx = find_line("FROM emp e");
        let cross_idx = find_line("CROSS");
        let apply_idx = find_line("APPLY");
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(apply_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let nested_select_idx = lines
            .iter()
            .enumerate()
            .skip(open_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "SELECT MAX (b.sal) AS max_sal")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let close_idx = lines
            .iter()
            .enumerate()
            .skip(nested_select_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start().starts_with(") bonus_view"))
            .map(|(idx, _)| idx)
            .unwrap_or(0);

        assert_eq!(
            contexts[cross_idx].auto_depth, contexts[from_idx].auto_depth,
            "split CROSS modifier line should stay on the nested FROM-item owner depth"
        );
        assert_eq!(
            contexts[apply_idx].auto_depth, contexts[cross_idx].auto_depth,
            "split APPLY line should stay aligned with the preserved modifier-owner depth"
        );
        assert_eq!(
            contexts[open_idx].auto_depth, contexts[apply_idx].auto_depth,
            "standalone open paren after split APPLY should stay on the APPLY owner depth"
        );
        assert_eq!(
            contexts[nested_select_idx].query_base_depth,
            Some(contexts[apply_idx].auto_depth.saturating_add(1)),
            "child SELECT under split APPLY should anchor its query base from the APPLY owner depth"
        );
        assert_eq!(
            contexts[nested_select_idx].auto_depth,
            contexts[apply_idx].auto_depth.saturating_add(1),
            "child SELECT under split APPLY should stay exactly one level deeper than the APPLY owner"
        );
        assert_eq!(
            contexts[close_idx].auto_depth, contexts[apply_idx].auto_depth,
            "split APPLY closing paren should realign with the APPLY owner depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_lateral_owner_depth_relative_to_nested_query_base() {
        let sql = r#"SELECT *
FROM dept d
WHERE EXISTS (
    SELECT 1
    FROM emp e,
            LATERAL
                    (
                        SELECT MAX (b.sal) AS max_sal
                        FROM bonus b
                        WHERE b.empno = e.empno
                    ) bonus_view
    WHERE e.deptno = d.deptno
)
AND d.status = 'A';"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let find_line = |needle: &str| -> usize {
            lines
                .iter()
                .position(|line| line.trim_start() == needle)
                .unwrap_or(0)
        };
        let from_idx = find_line("FROM emp e,");
        let lateral_idx = find_line("LATERAL");
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(lateral_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let nested_select_idx = lines
            .iter()
            .enumerate()
            .skip(open_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "SELECT MAX (b.sal) AS max_sal")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let close_idx = lines
            .iter()
            .enumerate()
            .skip(nested_select_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start().starts_with(") bonus_view"))
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let inner_where_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE e.deptno = d.deptno")
            .unwrap_or(0);

        assert_eq!(
            contexts[lateral_idx].auto_depth,
            contexts[from_idx].auto_depth.saturating_add(1),
            "split LATERAL owner line should step into the structural FROM-item sibling depth"
        );
        assert_eq!(
            contexts[open_idx].auto_depth, contexts[lateral_idx].auto_depth,
            "standalone open paren after split LATERAL should stay on the LATERAL owner depth"
        );
        assert_eq!(
            contexts[nested_select_idx].query_base_depth,
            Some(contexts[lateral_idx].auto_depth.saturating_add(1)),
            "child SELECT under split LATERAL should anchor its query base from the LATERAL owner depth"
        );
        assert_eq!(
            contexts[nested_select_idx].auto_depth,
            contexts[lateral_idx].auto_depth.saturating_add(1),
            "child SELECT under split LATERAL should stay exactly one level deeper than the LATERAL owner"
        );
        assert_eq!(
            contexts[close_idx].auto_depth, contexts[lateral_idx].auto_depth,
            "split LATERAL closing paren should realign with the LATERAL owner depth"
        );
        assert_eq!(
            contexts[inner_where_idx].auto_depth, contexts[from_idx].auto_depth,
            "query clauses after the split LATERAL subquery should restore the inner query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_table_owner_depth_relative_to_nested_query_base() {
        let sql = r#"SELECT *
FROM dept d
WHERE EXISTS (
    SELECT 1
    FROM TABLE
    (
        SELECT b.deptno AS bonus_deptno
        FROM bonus b
        WHERE b.empno = d.mgr
    ) bonus_view
    WHERE bonus_view.bonus_deptno = d.deptno
)
AND d.status = 'A';"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let from_table_idx = lines
            .iter()
            .position(|line| line.trim_start() == "FROM TABLE")
            .unwrap_or(0);
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(from_table_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let nested_select_idx = lines
            .iter()
            .enumerate()
            .skip(open_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start().starts_with("SELECT b.deptno"))
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let close_idx = lines
            .iter()
            .enumerate()
            .skip(nested_select_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start().starts_with(") bonus_view"))
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let inner_where_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE bonus_view.bonus_deptno = d.deptno")
            .unwrap_or(0);

        assert_eq!(
            contexts[open_idx].auto_depth, contexts[from_table_idx].auto_depth,
            "standalone open paren after split TABLE should stay aligned with the TABLE owner depth"
        );
        assert_eq!(
            contexts[nested_select_idx].query_base_depth,
            Some(contexts[from_table_idx].auto_depth.saturating_add(1)),
            "child SELECT under split TABLE should anchor its query base from the TABLE owner depth"
        );
        assert_eq!(
            contexts[nested_select_idx].auto_depth,
            contexts[from_table_idx].auto_depth.saturating_add(1),
            "child SELECT under split TABLE should stay exactly one level deeper than the TABLE owner"
        );
        assert_eq!(
            contexts[close_idx].auto_depth, contexts[from_table_idx].auto_depth,
            "split TABLE closing paren should realign with the TABLE owner depth"
        );
        assert_eq!(
            contexts[inner_where_idx].auto_depth, contexts[from_table_idx].auto_depth,
            "query clauses after the split TABLE subquery should restore the inner query base depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_standalone_from_item_body_on_structural_list_depth() {
        let sql = r#"SELECT d.deptno
FROM
    dept d,
            LATERAL
                    (
                        SELECT MAX (e.sal) AS max_sal
                        FROM emp e
                        WHERE e.deptno = d.deptno
                    ) lat
WHERE d.deptno > 0;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let find_line = |needle: &str| -> usize {
            lines
                .iter()
                .position(|line| line.trim_start() == needle)
                .unwrap_or(0)
        };
        let from_idx = find_line("FROM");
        let dept_idx = find_line("dept d,");
        let lateral_idx = find_line("LATERAL");
        let open_idx = lines
            .iter()
            .enumerate()
            .skip(lateral_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let nested_select_idx = lines
            .iter()
            .enumerate()
            .skip(open_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start().starts_with("SELECT MAX"))
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let close_idx = lines
            .iter()
            .enumerate()
            .skip(nested_select_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start().starts_with(") lat"))
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let where_idx = find_line("WHERE d.deptno > 0;");

        assert_eq!(
            contexts[dept_idx].auto_depth,
            contexts[from_idx].auto_depth.saturating_add(1),
            "first FROM item after a standalone header should use the structural list body depth"
        );
        assert_eq!(
            contexts[lateral_idx].auto_depth, contexts[dept_idx].auto_depth,
            "comma sibling after a mixed FROM item should reuse the structural list body depth"
        );
        assert_eq!(
            contexts[open_idx].auto_depth, contexts[lateral_idx].auto_depth,
            "standalone open paren after split LATERAL should stay on the structural owner depth"
        );
        assert_eq!(
            contexts[nested_select_idx].auto_depth,
            contexts[lateral_idx].auto_depth.saturating_add(1),
            "child SELECT under split LATERAL should stay one level deeper than the structural owner"
        );
        assert_eq!(
            contexts[close_idx].auto_depth, contexts[lateral_idx].auto_depth,
            "split LATERAL closing paren should realign with the structural owner depth"
        );
        assert_eq!(
            contexts[where_idx].auto_depth, contexts[from_idx].auto_depth,
            "WHERE after the comma sibling should return to the FROM owner depth"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_multi_step_model_modifier_continuations_on_owner_depth() {
        let sql = r#"SELECT *
FROM sales
MODEL
    RETURN
            UPDATED
            ROWS
    RULES
            AUTOMATIC
            ORDER
    (
        amount[ANY] = amount[CV()] + 1
    )
WHERE amount > 0;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let return_idx = lines
            .iter()
            .position(|line| line.trim_start() == "RETURN")
            .unwrap_or(0);
        let updated_idx = lines
            .iter()
            .position(|line| line.trim_start() == "UPDATED")
            .unwrap_or(0);
        let rows_idx = lines
            .iter()
            .position(|line| line.trim_start() == "ROWS")
            .unwrap_or(0);
        let rules_idx = lines
            .iter()
            .position(|line| line.trim_start() == "RULES")
            .unwrap_or(0);
        let automatic_idx = lines
            .iter()
            .position(|line| line.trim_start() == "AUTOMATIC")
            .unwrap_or(0);
        let order_idx = lines
            .iter()
            .position(|line| line.trim_start() == "ORDER")
            .unwrap_or(0);
        let outer_where_idx = lines
            .iter()
            .rposition(|line| line.trim_start().starts_with("WHERE amount > 0"))
            .unwrap_or(0);

        assert_eq!(
            contexts[updated_idx].auto_depth, contexts[return_idx].auto_depth,
            "UPDATED should stay on the MODEL RETURN owner-relative depth even when the modifier chain is split across three lines"
        );
        assert_eq!(
            contexts[rows_idx].auto_depth, contexts[return_idx].auto_depth,
            "ROWS should stay on the MODEL RETURN owner-relative depth after UPDATED"
        );
        assert_eq!(
            contexts[automatic_idx].auto_depth, contexts[rules_idx].auto_depth,
            "AUTOMATIC should stay on the MODEL RULES owner-relative depth even when AUTOMATIC ORDER is split"
        );
        assert_eq!(
            contexts[order_idx].auto_depth, contexts[rules_idx].auto_depth,
            "ORDER should stay on the MODEL RULES owner-relative depth after AUTOMATIC"
        );
        assert_eq!(
            contexts[outer_where_idx].auto_depth, 0,
            "outer WHERE should return to the top-level query depth after the MODEL clause closes"
        );
    }

    #[test]
    fn auto_format_line_contexts_keep_split_model_owner_headers_relative_to_nested_query_base() {
        let sql = r#"SELECT *
FROM (
    SELECT *
    FROM sales
    MODEL
        PARTITION
                BY
        (
            deptno
        )
        DIMENSION
        BY
        (
            prod_id
        )
        MEASURES
        (
            amount
        )
        RULES
                AUTOMATIC
                        ORDER
        (
            amount[ANY, ANY] = amount[CV(), CV()] + 1
        )
    WHERE amount > 0
) nested_sales
WHERE 1 = 1;"#;

        let contexts = QueryExecutor::auto_format_line_contexts(sql);
        let lines: Vec<&str> = sql.lines().collect();
        let partition_idx = lines
            .iter()
            .position(|line| line.trim_start() == "PARTITION")
            .unwrap_or(0);
        let partition_by_idx = lines
            .iter()
            .position(|line| line.trim_start() == "BY")
            .unwrap_or(0);
        let partition_open_idx = lines
            .iter()
            .enumerate()
            .skip(partition_by_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let dimension_idx = lines
            .iter()
            .position(|line| line.trim_start() == "DIMENSION")
            .unwrap_or(0);
        let model_idx = lines
            .iter()
            .position(|line| line.trim_start() == "MODEL")
            .unwrap_or(0);
        let dimension_by_idx = lines
            .iter()
            .enumerate()
            .skip(dimension_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "BY")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let dimension_open_idx = lines
            .iter()
            .enumerate()
            .skip(dimension_by_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let rules_idx = lines
            .iter()
            .position(|line| line.trim_start() == "RULES")
            .unwrap_or(0);
        let automatic_idx = lines
            .iter()
            .position(|line| line.trim_start() == "AUTOMATIC")
            .unwrap_or(0);
        let order_idx = lines
            .iter()
            .position(|line| line.trim_start() == "ORDER")
            .unwrap_or(0);
        let rules_open_idx = lines
            .iter()
            .enumerate()
            .skip(order_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let rules_body_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("amount[ANY, ANY]"))
            .unwrap_or(0);
        let inner_where_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE amount > 0")
            .unwrap_or(0);
        let outer_where_idx = lines
            .iter()
            .rposition(|line| line.trim_start() == "WHERE 1 = 1;")
            .unwrap_or(0);

        assert_eq!(
            contexts[partition_by_idx].auto_depth, contexts[partition_idx].auto_depth,
            "split MODEL PARTITION/BY should stay on the same owner-relative depth"
        );
        assert_eq!(
            contexts[partition_open_idx].auto_depth, contexts[partition_idx].auto_depth,
            "standalone open paren after split MODEL PARTITION BY should stay aligned with the owner depth"
        );
        assert_eq!(
            contexts[dimension_by_idx].auto_depth, contexts[dimension_idx].auto_depth,
            "split MODEL DIMENSION/BY should stay on the same owner-relative depth"
        );
        assert_eq!(
            contexts[dimension_open_idx].auto_depth, contexts[dimension_idx].auto_depth,
            "standalone open paren after split MODEL DIMENSION BY should stay aligned with the owner depth"
        );
        assert_eq!(
            contexts[automatic_idx].auto_depth, contexts[rules_idx].auto_depth,
            "split MODEL RULES/AUTOMATIC should stay on the same owner-relative depth"
        );
        assert_eq!(
            contexts[order_idx].auto_depth, contexts[rules_idx].auto_depth,
            "split MODEL AUTOMATIC/ORDER should stay on the same owner-relative depth"
        );
        assert_eq!(
            contexts[rules_open_idx].auto_depth, contexts[rules_idx].auto_depth,
            "standalone open paren after split MODEL RULES AUTOMATIC ORDER should stay aligned with the owner depth"
        );
        assert!(
            contexts[rules_body_idx].auto_depth > contexts[rules_open_idx].auto_depth,
            "MODEL RULES body should stay deeper than its owner header"
        );
        assert_eq!(
            contexts[inner_where_idx].auto_depth, contexts[model_idx].auto_depth,
            "inner WHERE should return to the nested query base after the MODEL clause closes"
        );
        assert_eq!(
            contexts[outer_where_idx].auto_depth, 0,
            "outer WHERE should return to the top-level query depth after the nested query closes"
        );
    }
}
