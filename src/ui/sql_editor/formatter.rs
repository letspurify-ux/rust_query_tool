use crate::db::{
    AutoFormatConditionRole, AutoFormatLineContext, AutoFormatQueryRole, FormatItem, QueryExecutor,
    ScriptItem, ToolCommand,
};
use crate::sql_text::{
    self, FORMAT_BLOCK_END_QUALIFIER_KEYWORDS, FORMAT_BLOCK_START_KEYWORDS, FORMAT_CLAUSE_KEYWORDS,
    FORMAT_CONDITION_KEYWORDS, FORMAT_CREATE_SUFFIX_BREAK_KEYWORDS, FORMAT_JOIN_MODIFIER_KEYWORDS,
};
use crate::ui::sql_depth::{
    is_depth, is_top_level_depth, paren_depths, split_top_level_keyword_groups,
    split_top_level_symbol_groups,
};

use super::SqlEditorWidget;
use super::SqlToken;

#[derive(Clone, Copy, PartialEq, Eq)]
enum LineLayoutKind {
    Blank,
    Code,
    CommentOnly,
    CommaOnly,
    Verbatim,
}

struct LineLayout<'a> {
    raw: &'a str,
    trimmed: &'a str,
    kind: LineLayoutKind,
    preserve_raw: bool,
    parser_depth: usize,
    auto_depth: usize,
    query_role: AutoFormatQueryRole,
    query_base_depth: Option<usize>,
    starts_query_frame: bool,
    next_query_head_depth: Option<usize>,
    condition_header_line: Option<usize>,
    condition_role: AutoFormatConditionRole,
    existing_indent: usize,
    existing_indent_spaces: usize,
    final_depth: usize,
    anchor_group: Option<usize>,
    dml_case_expression_close_depth: Option<usize>,
}

#[derive(Clone, Copy)]
struct MultilineClauseLayoutFrame {
    owner_depth: usize,
    nested_paren_depth: usize,
}

#[derive(Clone, Copy)]
struct DmlCaseLayoutFrame {
    case_depth: usize,
    expression_owner_depth: Option<usize>,
}

#[derive(Clone, Copy)]
struct DmlCaseConditionLayoutFrame {
    parser_depth: usize,
    continuation_depth: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum QueryHeadLayoutOrigin {
    Other,
    ClauseOwner,
    ConditionOwner,
    FromItemOwner,
}

#[derive(Clone, Copy)]
struct ResolvedQueryBaseLayoutFrame {
    raw_base_depth: usize,
    resolved_base_depth: usize,
    start_parser_depth: usize,
    close_align_depth: usize,
    origin: QueryHeadLayoutOrigin,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ParenLayoutFrameKind {
    General,
    Query,
    ConditionQuery,
    MultilineClause,
}

#[derive(Clone, Copy)]
struct ParenLayoutFrame {
    kind: ParenLayoutFrameKind,
    owner_depth: usize,
    continuation_depth: usize,
    standalone_owner: bool,
}

// For huge buffers, avoid an additional full/prefix reformat pass when remapping cursor position.
const CURSOR_MAPPING_FULL_REFORMAT_THRESHOLD_BYTES: usize = 2 * 1024 * 1024;

/// SQL 구문별 포맷팅 상태를 캡슐화한 구조체.
/// 각 SQL 구문(CREATE, MERGE, GRANT 등)이 활성 상태인지 추적하며,
/// 절(clause) 키워드의 줄바꿈 억제 여부를 한 곳에서 판단합니다.
#[derive(Default)]
struct ConstructState {
    create_pending: bool,
    create_object: Option<String>,
    routine_decl_pending: bool,
    create_table_paren_expected: bool,
    create_index_pending: bool,
    create_sequence_active: bool,
    create_synonym_active: bool,
    grant_revoke_active: bool,
    comment_on_active: bool,
    merge_active: bool,
    merge_when_branch_active: bool,
    returning_active: bool,
    search_cycle_clause_active: bool,
    match_recognize_paren_depth: Option<usize>,
    analytic_over_paren_depth: Option<usize>,
    fetch_active: bool,
    bulk_collect_active: bool,
    insert_all_active: bool,
    referential_action_pending: bool,
    referential_on_active: bool,
    execute_immediate_active: bool,
    cursor_decl_pending: bool,
    cursor_sql_active: bool,
    forall_pending: bool,
    forall_body_active: bool,
}

impl ConstructState {
    /// clause 키워드(SELECT, FROM, WHERE 등)의 줄바꿈을 억제해야 하는지 판단합니다.
    /// 새 SQL 구문을 추가할 때는 이 메서드에만 규칙을 추가하면 됩니다.
    fn suppresses_clause_break(
        &self,
        tokens: &[SqlToken],
        idx: usize,
        keyword: &str,
        current_clause: Option<&str>,
        prev_word_upper: Option<&str>,
        in_plsql_block: bool,
        suppress_comma_break_depth: usize,
        has_subquery_in_paren_stack: bool,
        trigger_header_state: TriggerHeaderState,
        is_analytic_within_group: bool,
        is_fetch_into_single_target: bool,
        in_analytic_over_paren: bool,
    ) -> bool {
        // Inside analytic OVER(): clause keywords are handled by the
        // analytic_over_paren_depth branch instead.
        if in_analytic_over_paren {
            return true;
        }
        if SqlEditorWidget::is_inline_clause_phrase(
            tokens,
            idx,
            keyword,
            current_clause,
            prev_word_upper,
            self,
        ) {
            return true;
        }
        // INSERT INTO stays on same line
        if keyword == "INTO" && matches!(prev_word_upper, Some("INSERT")) {
            return true;
        }
        // MERGE INTO stays on same line
        if keyword == "INTO" && matches!(prev_word_upper, Some("MERGE")) {
            return true;
        }
        // START WITH stays on same line
        if keyword == "WITH" && matches!(prev_word_upper, Some("START")) {
            return true;
        }
        // FETCH FIRST n ROW[S] WITH TIES stays on same line
        if keyword == "WITH" && matches!(prev_word_upper, Some("ROW" | "ROWS")) {
            return true;
        }
        // ORDER BY after SEQUENTIAL/AUTOMATIC suppressed
        if keyword == "ORDER"
            && (suppress_comma_break_depth > 0
                || matches!(prev_word_upper, Some("SEQUENTIAL" | "AUTOMATIC")))
        {
            return true;
        }
        // Trigger event keywords (INSERT/UPDATE/DELETE in trigger header)
        if trigger_header_state.is_active() && matches!(keyword, "INSERT" | "UPDATE" | "DELETE") {
            return true;
        }
        // FOR UPDATE (SQL, not PL/SQL)
        if keyword == "UPDATE" && matches!(prev_word_upper, Some("FOR")) && !in_plsql_block {
            return true;
        }
        // RETURNING INTO
        if self.returning_active && keyword == "INTO" {
            return true;
        }
        // FETCH INTO (single target)
        if self.fetch_active && keyword == "INTO" && is_fetch_into_single_target {
            return true;
        }
        // BULK COLLECT INTO
        if self.bulk_collect_active && keyword == "INTO" {
            return true;
        }
        // GRANT/REVOKE privilege keywords
        if self.grant_revoke_active
            && matches!(
                keyword,
                "SELECT"
                    | "INSERT"
                    | "UPDATE"
                    | "DELETE"
                    | "EXECUTE"
                    | "ALTER"
                    | "DROP"
                    | "CREATE"
                    | "INDEX"
                    | "REFERENCES"
                    | "ALL"
                    | "PRIVILEGES"
                    | "MERGE"
            )
        {
            return true;
        }
        // GRANT/REVOKE ON
        if self.grant_revoke_active && keyword == "ON" {
            return true;
        }
        // CREATE SEQUENCE: suppress START/CONNECT
        if self.create_sequence_active && matches!(keyword, "START" | "CONNECT") {
            return true;
        }
        // FETCH: suppress LIMIT/BULK
        if self.fetch_active && matches!(keyword, "LIMIT" | "BULK") {
            return true;
        }
        // EXECUTE IMMEDIATE: suppress INTO/USING
        if self.execute_immediate_active && matches!(keyword, "INTO" | "USING") {
            return true;
        }
        // MERGE WHEN ... THEN UPDATE/INSERT: suppress SET/VALUES/INTO
        if self.merge_when_branch_active && matches!(keyword, "SET" | "VALUES" | "INTO") {
            return true;
        }
        // Referential action: suppress DELETE/UPDATE/SET after ON DELETE/UPDATE
        if self.referential_on_active && matches!(keyword, "DELETE" | "UPDATE" | "SET") {
            return true;
        }
        // Non-subquery paren context: suppress FROM/RETURNING
        if suppress_comma_break_depth > 0
            && !has_subquery_in_paren_stack
            && matches!(keyword, "FROM" | "RETURNING")
        {
            return true;
        }
        // SEARCH/CYCLE ... SET
        if self.search_cycle_clause_active && keyword == "SET" {
            return true;
        }
        // Analytic WITHIN GROUP
        if is_analytic_within_group {
            return true;
        }
        false
    }

    /// condition 키워드(ON, AND, OR, WHEN)의 줄바꿈을 억제해야 하는지 판단합니다.
    fn suppresses_condition_break(
        &self,
        keyword: &str,
        prev_word_upper: Option<&str>,
        trigger_header_state: TriggerHeaderState,
    ) -> bool {
        // OR in CREATE OR REPLACE
        if keyword == "OR" && matches!(prev_word_upper, Some("CREATE")) {
            return true;
        }
        // Trigger: OR/ON in trigger header
        if trigger_header_state.is_active() && matches!(keyword, "OR" | "ON") {
            return true;
        }
        // CREATE INDEX ON
        if keyword == "ON" && self.create_index_pending {
            return true;
        }
        // COMMENT ON
        if keyword == "ON" && self.comment_on_active {
            return true;
        }
        // GRANT/REVOKE ON
        if self.grant_revoke_active && keyword == "ON" {
            return true;
        }
        // ON after REFERENCES (referential constraint)
        if keyword == "ON"
            && (matches!(prev_word_upper, Some("REFERENCES")) || self.referential_action_pending)
        {
            return true;
        }
        false
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum OpenCursorFormatState {
    None,
    AwaitingFor,
    InSelect {
        anchor_indent: usize,
        select_paren_depth: Option<usize>,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SelectListBreakState {
    None,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SelectListLayoutState {
    Inactive,
    Pending {
        anchor: usize,
        indent: usize,
    },
    Multiline {
        indent: usize,
        hanging_indent_spaces: Option<usize>,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum InlineCommentContinuationState {
    None,
    Operand { indent: usize },
}

impl SelectListLayoutState {
    fn activate(&mut self, anchor: usize, indent: usize) {
        *self = Self::Pending { anchor, indent };
    }

    fn clear(&mut self) {
        *self = Self::Inactive;
    }

    fn is_multiline(self) -> bool {
        matches!(self, Self::Multiline { .. })
    }

    fn has_active_indent(self) -> bool {
        matches!(self, Self::Pending { .. } | Self::Multiline { .. })
    }

    fn indentation_or(self, fallback: usize) -> usize {
        match self {
            Self::Pending { indent, .. } | Self::Multiline { indent, .. } => indent,
            Self::Inactive => fallback,
        }
    }

    fn hanging_indent_spaces(self, out: &str, fallback_indent: usize) -> usize {
        match self {
            Self::Pending { anchor, indent } => {
                if anchor < out.len() && out.as_bytes().get(anchor) == Some(&b' ') {
                    let line_start = out[..anchor].rfind('\n').map(|idx| idx + 1).unwrap_or(0);
                    anchor
                        .saturating_sub(line_start)
                        .saturating_add(1)
                        .max(indent * 4)
                } else {
                    indent.max(fallback_indent) * 4
                }
            }
            Self::Multiline {
                indent,
                hanging_indent_spaces,
            } => hanging_indent_spaces.unwrap_or_else(|| indent.max(fallback_indent) * 4),
            Self::Inactive => fallback_indent * 4,
        }
    }

    fn force_newline_if_possible(self, out: &mut String) -> Self {
        match self {
            Self::Pending { anchor, indent } => {
                if anchor < out.len() && out.as_bytes().get(anchor) == Some(&b' ') {
                    let indentation = " ".repeat(indent * 4);
                    out.replace_range(anchor..anchor + 1, &format!("\n{indentation}"));
                    Self::Multiline {
                        indent,
                        hanging_indent_spaces: None,
                    }
                } else {
                    self
                }
            }
            _ => self,
        }
    }
}

impl SelectListBreakState {
    fn clear(&mut self) {
        *self = Self::None;
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExitConditionState {
    None,
    AwaitingWhen,
}

impl ExitConditionState {
    fn on_keyword(&mut self, keyword: &str) {
        match (keyword, *self) {
            ("EXIT" | "CONTINUE", _) => {
                *self = Self::AwaitingWhen;
            }
            ("WHEN", Self::AwaitingWhen) => {
                *self = Self::None;
            }
            _ => {}
        }
    }

    fn is_exit_when(self, keyword: &str) -> bool {
        keyword == "WHEN" && matches!(self, Self::AwaitingWhen)
    }

    fn clear(&mut self) {
        *self = Self::None;
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum WithCteFormatState {
    None,
    InDefinitions {
        paren_depth: usize,
        plsql_state: WithPlsqlFormatState,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum WithPlsqlFormatState {
    None,
    Collecting {
        block_depth: usize,
        starts_routine_body: bool,
        pending_routine_begin: bool,
        pending_end: bool,
    },
    AwaitingMainQuery,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TriggerHeaderState {
    None,
    InHeader,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CompoundTriggerState {
    None,
    AwaitingOuterBodyStart,
    InOuterBody,
    AwaitingTimingPointBodyStart,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CommentAttachment {
    Previous,
    Next,
    Block,
    FileHeader,
}

#[derive(Clone, Copy)]
struct QueryApplyFrame {
    start_paren_depth: usize,
    started_inside_paren: bool,
    has_apply: bool,
    in_apply_clause: bool,
}

impl TriggerHeaderState {
    fn is_active(self) -> bool {
        matches!(self, Self::InHeader)
    }

    fn start(&mut self) {
        *self = Self::InHeader;
    }

    fn clear(&mut self) {
        *self = Self::None;
    }
}

impl CompoundTriggerState {
    fn awaiting_outer_body_start(self) -> bool {
        matches!(self, Self::AwaitingOuterBodyStart)
    }

    fn mark_compound_header(&mut self) {
        *self = Self::AwaitingOuterBodyStart;
    }

    fn enter_outer_body(&mut self) {
        *self = Self::InOuterBody;
    }

    fn is_in_outer_body(self) -> bool {
        matches!(self, Self::InOuterBody | Self::AwaitingTimingPointBodyStart)
    }

    fn start_timing_point(&mut self) {
        *self = Self::AwaitingTimingPointBodyStart;
    }

    fn begin_timing_point_body(&mut self) {
        if matches!(self, Self::AwaitingTimingPointBodyStart) {
            *self = Self::InOuterBody;
        }
    }

    fn clear(&mut self) {
        *self = Self::None;
    }
}

impl WithCteFormatState {
    fn on_word(&mut self, keyword: &str) {
        let upper = keyword.to_ascii_uppercase();

        match self {
            Self::None => {
                if upper == "WITH" {
                    *self = Self::InDefinitions {
                        paren_depth: 0,
                        plsql_state: WithPlsqlFormatState::None,
                    };
                }
            }
            Self::InDefinitions {
                paren_depth,
                plsql_state,
            } => match plsql_state {
                WithPlsqlFormatState::None | WithPlsqlFormatState::AwaitingMainQuery => {
                    if *paren_depth == 0
                        && sql_text::is_with_plsql_declaration_keyword(upper.as_str())
                    {
                        *plsql_state = WithPlsqlFormatState::Collecting {
                            block_depth: 0,
                            starts_routine_body:
                                sql_text::with_plsql_declaration_starts_routine_body(upper.as_str()),
                            pending_routine_begin: false,
                            pending_end: false,
                        };
                        return;
                    }

                    if matches!(
                        upper.as_str(),
                        "SELECT" | "UPDATE" | "DELETE" | "INSERT" | "MERGE"
                    ) && *paren_depth == 0
                    {
                        *self = Self::None;
                    }
                }
                WithPlsqlFormatState::Collecting {
                    block_depth,
                    starts_routine_body,
                    pending_routine_begin,
                    pending_end,
                } => {
                    if matches!(upper.as_str(), "AS" | "IS")
                        && *starts_routine_body
                        && *block_depth == 0
                    {
                        *block_depth = 1;
                        *pending_routine_begin = true;
                        return;
                    }

                    if upper == "BEGIN" && *pending_routine_begin && *block_depth == 1 {
                        *pending_routine_begin = false;
                        return;
                    }

                    if matches!(upper.as_str(), "BEGIN" | "DECLARE" | "CASE" | "IF" | "LOOP") {
                        if *pending_end && *block_depth > 0 {
                            *block_depth = block_depth.saturating_sub(1);
                            *pending_end = false;
                        }
                        *block_depth = block_depth.saturating_add(1);
                        return;
                    }

                    if upper == "END" {
                        *pending_end = true;
                        return;
                    }

                    if *pending_end && !matches!(upper.as_str(), "CASE" | "IF" | "LOOP") {
                        if *block_depth > 0 {
                            *block_depth = block_depth.saturating_sub(1);
                        }
                        *pending_end = false;
                    }
                }
            },
        }
    }

    fn on_open_paren(&mut self) {
        if let Self::InDefinitions { paren_depth, .. } = self {
            *paren_depth = paren_depth.saturating_add(1);
        }
    }

    fn on_close_paren(&mut self) {
        if let Self::InDefinitions { paren_depth, .. } = self {
            *paren_depth = paren_depth.saturating_sub(1);
        }
    }

    fn on_separator(&mut self) {
        if let Self::InDefinitions { plsql_state, .. } = self {
            if let WithPlsqlFormatState::Collecting {
                block_depth,
                pending_end,
                ..
            } = plsql_state
            {
                if *pending_end && *block_depth > 0 {
                    *block_depth = block_depth.saturating_sub(1);
                    *pending_end = false;
                }

                if *block_depth == 0 {
                    *plsql_state = WithPlsqlFormatState::AwaitingMainQuery;
                }
            }
        }
    }

    fn can_close_on_select(self) -> bool {
        matches!(
            self,
            Self::InDefinitions {
                paren_depth: 0,
                plsql_state: WithPlsqlFormatState::None | WithPlsqlFormatState::AwaitingMainQuery,
            }
        )
    }

    fn collecting_routine_declaration_body_start(self) -> bool {
        matches!(
            self,
            Self::InDefinitions {
                plsql_state: WithPlsqlFormatState::Collecting {
                    block_depth: 0,
                    starts_routine_body: true,
                    ..
                },
                ..
            }
        )
    }

    fn keeps_top_level_semicolon_inside_with_definitions(self) -> bool {
        matches!(
            self,
            Self::InDefinitions {
                plsql_state: WithPlsqlFormatState::AwaitingMainQuery,
                ..
            }
        )
    }
}

impl OpenCursorFormatState {
    fn base_indent(self, indent_level: usize) -> usize {
        match self {
            Self::InSelect { anchor_indent, .. } => indent_level.max(anchor_indent),
            _ => indent_level,
        }
    }

    fn in_select(self) -> bool {
        matches!(self, Self::InSelect { .. })
    }

    fn set_select_depth(&mut self, depth: usize) {
        if let Self::InSelect {
            select_paren_depth, ..
        } = self
        {
            if select_paren_depth.is_none() {
                *select_paren_depth = Some(depth);
            }
        }
    }

    fn select_depth(self) -> Option<usize> {
        match self {
            Self::InSelect {
                select_paren_depth, ..
            } => select_paren_depth,
            _ => None,
        }
    }
}

impl SqlEditorWidget {
    fn previous_word_upper(tokens: &[SqlToken], start_idx: usize) -> Option<(String, usize)> {
        let mut idx = start_idx;
        while idx > 0 {
            idx = idx.saturating_sub(1);
            match tokens.get(idx) {
                Some(SqlToken::Comment(_)) => continue,
                Some(SqlToken::Word(word)) => return Some((word.to_ascii_uppercase(), idx)),
                _ => return None,
            }
        }

        None
    }

    fn is_log_errors_into_clause(tokens: &[SqlToken], into_idx: usize) -> bool {
        let Some((prev_word, prev_idx)) = Self::previous_word_upper(tokens, into_idx) else {
            return false;
        };
        if prev_word != "ERRORS" {
            return false;
        }

        matches!(
            Self::previous_word_upper(tokens, prev_idx),
            Some((word, _)) if word == "LOG"
        )
    }

    fn is_multiset_set_operator(tokens: &[SqlToken], idx: usize) -> bool {
        matches!(
            Self::previous_word_upper(tokens, idx),
            Some((prev, _)) if prev == "MULTISET"
        )
    }

    fn is_inline_clause_phrase(
        tokens: &[SqlToken],
        idx: usize,
        keyword: &str,
        current_clause: Option<&str>,
        prev_word_upper: Option<&str>,
        construct: &ConstructState,
    ) -> bool {
        if construct.forall_pending && keyword == "VALUES" {
            return true;
        }
        if keyword == "INTO" && Self::is_log_errors_into_clause(tokens, idx) {
            return true;
        }
        if keyword == "LIMIT" && matches!(prev_word_upper, Some("REJECT")) {
            return true;
        }
        if matches!(keyword, "UNION" | "INTERSECT" | "EXCEPT")
            && Self::is_multiset_set_operator(tokens, idx)
        {
            return true;
        }
        if current_clause == Some("MODEL")
            && keyword == "UPDATE"
            && matches!(prev_word_upper, Some("RULES"))
        {
            return true;
        }

        false
    }

    fn leading_indent_columns(line: &str) -> usize {
        const INDENT_TAB_WIDTH: usize = 4;
        let mut columns = 0usize;

        for ch in line.chars() {
            match ch {
                ' ' => {
                    columns = columns.saturating_add(1);
                }
                '\t' => {
                    let next_tab_stop =
                        ((columns / INDENT_TAB_WIDTH).saturating_add(1)) * INDENT_TAB_WIDTH;
                    columns = next_tab_stop;
                }
                _ if ch.is_whitespace() => {
                    columns = columns.saturating_add(1);
                }
                _ => break,
            }
        }

        columns
    }

    fn starts_with_end_suffix_terminator(trimmed_upper: &str) -> bool {
        if !crate::sql_text::starts_with_keyword_token(trimmed_upper, "END") {
            return false;
        }

        let Some(rest) = trimmed_upper.strip_prefix("END") else {
            return false;
        };
        let rest = rest.trim_start();

        FORMAT_BLOCK_END_QUALIFIER_KEYWORDS
            .iter()
            .any(|keyword| crate::sql_text::starts_with_keyword_token(rest, keyword))
    }

    fn tokens_continue_plsql_condition_terminator(tokens: &[SqlToken], idx: usize) -> bool {
        for token in tokens.iter().skip(idx.saturating_add(1)) {
            match token {
                SqlToken::Comment(comment) if comment.contains('\n') => break,
                SqlToken::Symbol(sym) if sym == ";" => break,
                SqlToken::Word(word)
                    if word.eq_ignore_ascii_case("THEN") || word.eq_ignore_ascii_case("LOOP") =>
                {
                    return true;
                }
                _ => {}
            }
        }

        false
    }

    fn starts_with_plain_end(trimmed_upper: &str) -> bool {
        crate::sql_text::starts_with_keyword_token(trimmed_upper, "END")
            && !Self::starts_with_end_suffix_terminator(trimmed_upper)
    }

    fn starts_with_case_terminator(trimmed_upper: &str) -> bool {
        if Self::starts_with_plain_end(trimmed_upper) {
            return true;
        }

        if !Self::starts_with_end_suffix_terminator(trimmed_upper) {
            return false;
        }

        let Some(rest) = trimmed_upper.strip_prefix("END") else {
            return false;
        };

        crate::sql_text::starts_with_keyword_token(rest.trim_start(), "CASE")
    }

    fn starts_with_bare_end(trimmed_upper: &str) -> bool {
        if !Self::starts_with_plain_end(trimmed_upper) {
            return false;
        }

        let Some(rest) = trimmed_upper.strip_prefix("END") else {
            return false;
        };
        let rest = rest.trim_start();

        rest.is_empty() || rest.starts_with(';')
    }

    fn paren_opens_analytic_layout(
        current_clause: Option<&str>,
        prev_word_upper: Option<&str>,
    ) -> bool {
        matches!(prev_word_upper, Some("OVER"))
            || (matches!(current_clause, Some("WINDOW"))
                && matches!(prev_word_upper, Some("AS")))
    }

    fn query_apply_flags(tokens: &[SqlToken]) -> Vec<bool> {
        let mut flags = vec![false; tokens.len()];
        let mut frames: Vec<QueryApplyFrame> = Vec::new();
        let mut active_frame_ids: Vec<usize> = Vec::new();
        let mut token_frame_ids: Vec<Option<usize>> = vec![None; tokens.len()];
        let mut paren_depth = 0usize;
        let mut apply_paren_pending = false;
        let mut apply_clause_paren_depths: Vec<usize> = Vec::new();

        for (idx, token) in tokens.iter().enumerate() {
            token_frame_ids[idx] = active_frame_ids.last().copied();

            match token {
                SqlToken::Word(word) if word.eq_ignore_ascii_case("SELECT") => {
                    let frame_id = frames.len();
                    frames.push(QueryApplyFrame {
                        start_paren_depth: paren_depth,
                        started_inside_paren: paren_depth > 0,
                        has_apply: false,
                        in_apply_clause: apply_clause_paren_depths
                            .last()
                            .is_some_and(|depth| *depth == paren_depth),
                    });
                    active_frame_ids.push(frame_id);
                    token_frame_ids[idx] = Some(frame_id);
                    apply_paren_pending = false;
                }
                SqlToken::Word(word) if word.eq_ignore_ascii_case("APPLY") => {
                    if let Some(frame_id) = active_frame_ids.last().copied() {
                        if let Some(frame) = frames.get_mut(frame_id) {
                            frame.has_apply = true;
                        }
                    }
                    apply_paren_pending = true;
                }
                SqlToken::Symbol(symbol) if symbol == "(" => {
                    paren_depth = paren_depth.saturating_add(1);
                    if apply_paren_pending {
                        apply_clause_paren_depths.push(paren_depth);
                        apply_paren_pending = false;
                    }
                }
                SqlToken::Symbol(symbol) if symbol == ")" => {
                    apply_paren_pending = false;
                    paren_depth = paren_depth.saturating_sub(1);
                    while apply_clause_paren_depths
                        .last()
                        .is_some_and(|depth| paren_depth < *depth)
                    {
                        apply_clause_paren_depths.pop();
                    }
                    while active_frame_ids.last().is_some_and(|frame_id| {
                        frames.get(*frame_id).is_some_and(|frame| {
                            frame.started_inside_paren && paren_depth < frame.start_paren_depth
                        })
                    }) {
                        active_frame_ids.pop();
                    }
                }
                SqlToken::Symbol(symbol) if symbol == ";" => {
                    apply_paren_pending = false;
                    while active_frame_ids.last().is_some_and(|frame_id| {
                        frames
                            .get(*frame_id)
                            .is_some_and(|frame| !frame.started_inside_paren)
                    }) {
                        active_frame_ids.pop();
                    }
                }
                SqlToken::Comment(_) => {}
                _ => {
                    apply_paren_pending = false;
                }
            }
        }

        for (idx, frame_id) in token_frame_ids.into_iter().enumerate() {
            if let Some(frame_id) = frame_id {
                if let Some(frame) = frames.get(frame_id) {
                    flags[idx] = frame.has_apply || frame.in_apply_clause;
                }
            }
        }

        flags
    }

    pub(super) fn format_for_auto_formatting(source: &str, selected_only: bool) -> String {
        // A selected SQL fragment should keep canonical statement terminators.
        // Otherwise select-all / partial-selection formatting can drop semicolons
        // that `split_format_items()` strips internally, which then breaks
        // statement boundaries for Ctrl+Enter execution on the formatted text.
        let preserve_missing_selection_terminator =
            selected_only && !Self::selected_formatting_has_statement(source);

        let formatted = if preserve_missing_selection_terminator {
            Self::format_sql_basic_with_terminator_policy(source, false)
        } else {
            Self::format_sql_basic(source)
        };

        if preserve_missing_selection_terminator {
            Self::preserve_selected_text_terminator(source, formatted)
        } else {
            formatted
        }
    }

    pub(super) fn selected_formatting_has_statement(source: &str) -> bool {
        // Comment-only / tool-command-only selections still use the
        // "preserve original missing terminator" path. Any real SQL statement
        // should format to the same canonical shape as whole-buffer formatting.
        super::query_text::split_script_items(source)
            .iter()
            .any(|item| matches!(item, ScriptItem::Statement(_)))
    }

    pub(super) fn normalize_index(text: &str, index: i32) -> usize {
        if index <= 0 {
            0
        } else {
            Self::clamp_to_char_boundary(text, index as usize)
        }
    }

    pub(super) fn clamp_to_char_boundary(text: &str, index: usize) -> usize {
        let mut idx = index.min(text.len());
        if text.is_char_boundary(idx) {
            return idx;
        }

        // Clamp invalid UTF-8 byte offsets to the previous valid boundary.
        while idx > 0 && !text.is_char_boundary(idx) {
            idx -= 1;
        }
        idx
    }

    pub(super) fn map_cursor_after_format(source: &str, formatted: &str, original_pos: i32) -> i32 {
        Self::map_cursor_after_format_with_policy(source, formatted, original_pos, false)
    }

    pub(super) fn map_cursor_after_format_with_policy(
        source: &str,
        formatted: &str,
        original_pos: i32,
        selected_only: bool,
    ) -> i32 {
        if original_pos <= 0 {
            return 0;
        }

        let source_pos = Self::clamp_to_char_boundary(source, original_pos as usize);
        if source.len() >= CURSOR_MAPPING_FULL_REFORMAT_THRESHOLD_BYTES {
            if source.is_empty() || formatted.is_empty() {
                return 0;
            }
            let scaled_pos =
                (source_pos as u128).saturating_mul(formatted.len() as u128) / source.len() as u128;
            return Self::clamp_to_char_boundary(formatted, scaled_pos as usize) as i32;
        }

        let source_prefix = &source[..source_pos];
        let formatted_prefix = if selected_only {
            Self::format_for_auto_formatting(source_prefix, true)
        } else {
            Self::format_sql_basic(source_prefix)
        };
        let mut formatted_pos = formatted_prefix.len().min(formatted.len());
        formatted_pos = Self::clamp_to_char_boundary(formatted, formatted_pos);
        formatted_pos = Self::advance_over_inserted_layout_whitespace(
            source,
            source_pos,
            formatted,
            formatted_pos,
        );
        formatted_pos as i32
    }

    pub(super) fn advance_over_inserted_layout_whitespace(
        source: &str,
        source_pos: usize,
        formatted: &str,
        formatted_pos: usize,
    ) -> usize {
        let source_byte = match source.as_bytes().get(source_pos) {
            Some(byte) => *byte,
            None => return formatted_pos,
        };

        if source_byte.is_ascii_whitespace() {
            return formatted_pos;
        }

        let mut cursor = formatted_pos;
        while let Some(byte) = formatted.as_bytes().get(cursor) {
            if !byte.is_ascii_whitespace() {
                break;
            }
            cursor += 1;
        }

        Self::clamp_to_char_boundary(formatted, cursor)
    }

    pub(super) fn preserve_selected_text_terminator(source: &str, formatted: String) -> String {
        if Self::source_has_explicit_semicolon_terminator(source) {
            return formatted;
        }

        if let Some(without_semicolon) = Self::remove_trailing_statement_semicolon(&formatted) {
            return without_semicolon;
        }

        formatted
    }

    fn removable_trailing_semicolon_span(formatted: &str) -> Option<(usize, usize)> {
        let mut trimmed_len = formatted.trim_end().len();
        while trimmed_len > 0 {
            let prefix = &formatted[..trimmed_len];
            let line_start = prefix.rfind('\n').map_or(0, |idx| idx + 1);
            let line = &prefix[line_start..trimmed_len];
            let line_trimmed = line.trim_start();

            let is_trailing_comment_line = line_trimmed.starts_with("--")
                || Self::is_sqlplus_remark_comment_statement(line_trimmed);

            if !is_trailing_comment_line {
                break;
            }

            trimmed_len = prefix[..line_start].trim_end().len();
        }

        if trimmed_len == 0 {
            return None;
        }
        let trimmed = &formatted[..trimmed_len];
        if let Some(last_line_start) = trimmed.rfind('\n').map(|idx| idx + 1).or(Some(0)) {
            let last_line = &trimmed[last_line_start..];
            let leading_ws = last_line.len().saturating_sub(last_line.trim_start().len());
            let last_line_trimmed = &last_line[leading_ws..];
            if let Some(rest) = last_line_trimmed.strip_prefix(';') {
                let rest_trimmed = rest.trim_start();
                if Self::is_sqlplus_remark_comment_statement(rest_trimmed) {
                    let semicolon_start = last_line_start + leading_ws;
                    return Some((semicolon_start, semicolon_start + 1));
                }
            }
        }

        let spans = super::query_text::tokenize_sql_spanned(trimmed);

        let mut semicolon_span: Option<(usize, usize)> = None;

        for span in spans.iter().rev() {
            match &span.token {
                SqlToken::Comment(comment_text) if comment_text.starts_with("--") => continue,
                SqlToken::Comment(_) => continue,
                SqlToken::Symbol(sym) if sym == "/" => continue,
                SqlToken::Symbol(sym) if sym == ";" => {
                    if Self::semicolon_belongs_to_non_sql_line(trimmed, span.start) {
                        continue;
                    }
                    semicolon_span = Some((span.start, span.end));
                    break;
                }
                _ => break,
            }
        }

        semicolon_span
    }

    fn remove_trailing_statement_semicolon(formatted: &str) -> Option<String> {
        let (semicolon_start, semicolon_end) = Self::removable_trailing_semicolon_span(formatted)?;

        let mut out = String::with_capacity(
            formatted
                .len()
                .saturating_sub(semicolon_end.saturating_sub(semicolon_start)),
        );
        out.push_str(&formatted[..semicolon_start]);
        out.push_str(&formatted[semicolon_end..]);
        Some(out)
    }

    fn append_missing_statement_terminator(formatted_statement: &mut String) {
        let trim_len = formatted_statement.trim_end().len();
        if trim_len == 0 {
            return;
        }

        let trimmed = &formatted_statement[..trim_len];
        let mut insert_at = trim_len;
        let mut has_trailing_comment = false;

        let mut trailing_comment_start = trim_len;
        loop {
            let prefix = &trimmed[..trailing_comment_start];
            let line_start = prefix.rfind('\n').map_or(0, |idx| idx + 1);
            let line = &prefix[line_start..trailing_comment_start];
            let line_trimmed = line.trim_start();
            if Self::is_sqlplus_remark_comment_statement(line_trimmed) {
                has_trailing_comment = true;
                insert_at = line_start;
                trailing_comment_start = prefix[..line_start].trim_end().len();
                if trailing_comment_start == 0 {
                    break;
                }
                continue;
            }
            break;
        }

        let spans = super::query_text::tokenize_sql_spanned(trimmed);
        for span in spans.iter().rev() {
            match &span.token {
                SqlToken::Comment(_) => {
                    has_trailing_comment = true;
                    insert_at = insert_at.min(span.start);
                }
                SqlToken::Symbol(sym) if sym == "/" => continue,
                _ => break,
            }
        }

        if has_trailing_comment {
            while insert_at > 0 {
                match trimmed.as_bytes().get(insert_at - 1) {
                    Some(b' ' | b'\t' | b'\n' | b'\r') => insert_at -= 1,
                    _ => break,
                }
            }
            let suffix = &formatted_statement[insert_at..trim_len];
            let separator = match suffix.as_bytes().first() {
                Some(b' ' | b'\t' | b'\n' | b'\r') => ";",
                _ => "; ",
            };
            formatted_statement.insert_str(insert_at, separator);
        } else {
            formatted_statement.insert(insert_at, ';');
        }
    }
    pub(crate) fn format_sql_basic(sql: &str) -> String {
        Self::format_sql_basic_with_terminator_policy(sql, true)
    }

    fn normalize_format_items(items: Vec<FormatItem>) -> Vec<FormatItem> {
        let mut normalized: Vec<FormatItem> = Vec::with_capacity(items.len());
        let mut index = 0usize;

        while index < items.len() {
            if let Some((combined, consumed)) =
                Self::merge_fragmented_with_single_letter_cte(&items, index)
            {
                normalized.push(FormatItem::Statement(combined));
                index += consumed;
                continue;
            }

            let item = items[index].clone();
            let should_merge = match (normalized.last(), &item) {
                (Some(FormatItem::Statement(previous)), FormatItem::Statement(fragment)) => {
                    Self::should_merge_order_modifier_fragment(previous, fragment)
                }
                _ => false,
            };

            if should_merge {
                if let Some(FormatItem::Statement(previous)) = normalized.last_mut() {
                    Self::append_statement_fragment(previous, &item);
                    index += 1;
                    continue;
                }
            }

            normalized.push(item);
            index += 1;
        }

        normalized
    }

    fn merge_fragmented_with_single_letter_cte(
        items: &[FormatItem],
        start_index: usize,
    ) -> Option<(String, usize)> {
        let with_head = match items.get(start_index) {
            Some(FormatItem::Statement(statement))
                if statement.trim().eq_ignore_ascii_case("WITH") =>
            {
                statement.trim()
            }
            _ => return None,
        };

        let cte_name = match items.get(start_index + 1) {
            Some(FormatItem::ToolCommand(ToolCommand::Unsupported { raw, .. })) => raw.trim(),
            _ => return None,
        };
        if !Self::is_single_letter_cte_identifier(cte_name) {
            return None;
        }

        let trailing = match items.get(start_index + 2) {
            Some(FormatItem::Statement(statement)) => statement.trim_start(),
            _ => return None,
        };
        if !Self::starts_with_ascii_keyword_token_ci(trailing, "AS") {
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

    fn should_merge_order_modifier_fragment(previous: &str, fragment: &str) -> bool {
        let previous_trimmed = previous.trim_end();
        if previous_trimmed.is_empty() || previous_trimmed.ends_with(';') {
            return false;
        }

        let fragment_trimmed = fragment.trim_start();
        if fragment_trimmed.is_empty() {
            return false;
        }

        let starts_order_modifier =
            Self::starts_with_ascii_keyword_token_ci(fragment_trimmed, "ASC")
                || Self::starts_with_ascii_keyword_token_ci(fragment_trimmed, "DESC")
                || Self::starts_with_ascii_keyword_token_ci(fragment_trimmed, "NULLS");
        if !starts_order_modifier {
            return false;
        }

        let previous_search_window =
            Self::tail_search_window(previous_trimmed, 4096).unwrap_or(previous_trimmed);
        Self::contains_ascii_case_insensitive(previous_search_window, "ORDER BY")
            || Self::contains_ascii_case_insensitive(previous_search_window, "ORDER SIBLINGS BY")
    }

    fn starts_with_ascii_keyword_token_ci(text: &str, keyword: &str) -> bool {
        let text_bytes = text.as_bytes();
        let keyword_bytes = keyword.as_bytes();
        if text_bytes.len() < keyword_bytes.len() {
            return false;
        }
        if !text_bytes[..keyword_bytes.len()].eq_ignore_ascii_case(keyword_bytes) {
            return false;
        }

        !text_bytes
            .get(keyword_bytes.len())
            .is_some_and(|next| next.is_ascii_alphanumeric() || *next == b'_')
    }

    fn contains_ascii_case_insensitive(haystack: &str, needle: &str) -> bool {
        let haystack_bytes = haystack.as_bytes();
        let needle_bytes = needle.as_bytes();
        if needle_bytes.is_empty() {
            return true;
        }
        if haystack_bytes.len() < needle_bytes.len() {
            return false;
        }

        haystack_bytes
            .windows(needle_bytes.len())
            .rev()
            .any(|window| window.eq_ignore_ascii_case(needle_bytes))
    }

    fn tail_search_window(text: &str, max_bytes: usize) -> Option<&str> {
        if text.len() <= max_bytes {
            return None;
        }

        let mut start = text.len().saturating_sub(max_bytes);
        while start < text.len() && !text.is_char_boundary(start) {
            start = start.saturating_add(1);
        }
        text.get(start..)
    }

    fn append_statement_fragment(previous: &mut String, item: &FormatItem) {
        let FormatItem::Statement(fragment) = item else {
            return;
        };

        let fragment_trimmed = fragment.trim_start();
        if fragment_trimmed.is_empty() {
            return;
        }

        let needs_space = previous
            .chars()
            .last()
            .is_some_and(|ch| !ch.is_whitespace());
        if needs_space {
            previous.push(' ');
        }
        previous.push_str(fragment_trimmed);
    }

    fn format_sql_basic_with_terminator_policy(
        sql: &str,
        append_missing_terminator: bool,
    ) -> String {
        let mut formatted = String::with_capacity(sql.len().saturating_add(64));
        let items = Self::normalize_format_items(super::query_text::split_format_items(sql));
        if items.is_empty() {
            return String::new();
        }

        let mut select_list_break_state = SelectListBreakState::None;
        for (idx, item) in items.iter().enumerate() {
            let next_item = items.get(idx + 1);

            match item {
                FormatItem::Statement(statement) => {
                    let statement_tokens = Self::tokenize_sql(statement);
                    let formatted_statement = Self::format_statement(
                        statement,
                        &statement_tokens,
                        select_list_break_state,
                    );
                    let has_code = Self::statement_has_code(statement, &statement_tokens);
                    let mut formatted_statement = formatted_statement;
                    if append_missing_terminator
                        && has_code
                        && !Self::statement_ends_with_semicolon_tokens(&statement_tokens)
                        && Self::should_append_missing_statement_terminator(&statement_tokens)
                    {
                        Self::append_missing_statement_terminator(&mut formatted_statement);
                    }
                    formatted.push_str(&formatted_statement);
                }
                FormatItem::ToolCommand(command) => {
                    formatted.push_str(&Self::format_tool_command(command));
                    select_list_break_state.clear();
                }
                FormatItem::Verbatim(text) => {
                    formatted.push_str(text);
                    select_list_break_state.clear();
                }
                FormatItem::Slash => {
                    formatted.push('/');
                    select_list_break_state.clear();
                }
            }

            if let Some(next_item) = next_item {
                formatted.push_str(Self::item_separator(item, next_item));
            }
        }

        formatted
    }

    fn item_separator(current: &FormatItem, next: &FormatItem) -> &'static str {
        if matches!(next, FormatItem::Slash) || Self::keeps_tight_spacing(current, next) {
            "\n"
        } else {
            "\n\n"
        }
    }

    fn keeps_tight_spacing(current: &FormatItem, next: &FormatItem) -> bool {
        match (current, next) {
            (FormatItem::Statement(left), FormatItem::Statement(right)) => {
                (Self::is_sqlplus_comment_line(left) && Self::is_sqlplus_comment_line(right))
                    || (Self::is_create_trigger_statement(left)
                        && Self::is_alter_trigger_statement(right))
            }
            (FormatItem::Slash, FormatItem::Statement(right)) => {
                Self::is_alter_trigger_statement(right)
            }
            _ if Self::is_prompt_format_item(current) && Self::is_prompt_format_item(next) => true,
            (
                FormatItem::ToolCommand(ToolCommand::ClearBreaks),
                FormatItem::ToolCommand(ToolCommand::ClearComputes),
            )
            | (
                FormatItem::ToolCommand(ToolCommand::ClearComputes),
                FormatItem::ToolCommand(ToolCommand::ClearBreaks),
            ) => true,
            _ => false,
        }
    }

    fn is_prompt_format_item(item: &FormatItem) -> bool {
        match item {
            FormatItem::ToolCommand(ToolCommand::Prompt { .. }) => true,
            FormatItem::Verbatim(text) => QueryExecutor::parse_tool_command(text)
                .is_some_and(|cmd| matches!(cmd, ToolCommand::Prompt { .. })),
            _ => false,
        }
    }

    fn is_sqlplus_comment_line(statement: &str) -> bool {
        crate::sql_text::is_sqlplus_comment_line(statement)
    }

    fn is_create_trigger_statement(statement: &str) -> bool {
        let mut word_idx = 0usize;
        let mut has_trigger_in_prefix = false;

        for token in Self::tokenize_sql(statement) {
            let SqlToken::Word(word) = token else {
                continue;
            };

            if word_idx == 0 && !word.eq_ignore_ascii_case("CREATE") {
                return false;
            }

            if word_idx < 8 && word.eq_ignore_ascii_case("TRIGGER") {
                has_trigger_in_prefix = true;
            }

            word_idx += 1;
        }

        word_idx > 0 && has_trigger_in_prefix
    }

    fn is_alter_trigger_statement(statement: &str) -> bool {
        let mut words = Self::tokenize_sql(statement)
            .into_iter()
            .filter_map(|token| match token {
                SqlToken::Word(word) => Some(word),
                _ => None,
            });

        matches!(
            (words.next(), words.next()),
            (Some(first), Some(second))
                if first.eq_ignore_ascii_case("ALTER")
                    && second.eq_ignore_ascii_case("TRIGGER")
        )
    }

    fn statement_has_code(statement: &str, tokens: &[SqlToken]) -> bool {
        let trimmed = statement.trim_start();
        if let Some(first_word) = trimmed.split_whitespace().next() {
            if first_word.eq_ignore_ascii_case("REM") || first_word.eq_ignore_ascii_case("REMARK") {
                return false;
            }
        }

        tokens
            .iter()
            .any(|token| !matches!(token, SqlToken::Comment(_)))
    }

    fn is_sqlplus_remark_comment_statement(statement: &str) -> bool {
        statement
            .split_whitespace()
            .next()
            .is_some_and(|first_word| {
                first_word.eq_ignore_ascii_case("REM") || first_word.eq_ignore_ascii_case("REMARK")
            })
    }

    fn source_has_explicit_semicolon_terminator(statement: &str) -> bool {
        let trimmed = statement.trim_end();
        if trimmed.is_empty() {
            return false;
        }

        let spans = super::query_text::tokenize_sql_spanned(trimmed);
        spans.iter().rev().any(|span| {
            matches!(&span.token, SqlToken::Symbol(sym) if sym == ";")
                && !Self::semicolon_belongs_to_non_sql_line(trimmed, span.start)
                && Self::trailing_segment_allows_semicolon_terminator(&trimmed[span.end..])
        })
    }

    fn semicolon_belongs_to_non_sql_line(statement: &str, semicolon_start: usize) -> bool {
        let line_start = statement[..semicolon_start]
            .rfind('\n')
            .map_or(0, |idx| idx + 1);
        let line_end = statement[semicolon_start..]
            .find('\n')
            .map_or(statement.len(), |idx| semicolon_start + idx);
        let line = statement[line_start..line_end].trim();

        if line.starts_with("--") || Self::is_sqlplus_remark_comment_statement(line) {
            return true;
        }

        QueryExecutor::parse_tool_command(line)
            .is_some_and(|command| !matches!(command, ToolCommand::Unsupported { .. }))
    }

    fn trailing_segment_allows_semicolon_terminator(suffix: &str) -> bool {
        let mut rest = suffix.trim_start();
        if rest.is_empty() {
            return true;
        }

        loop {
            let line_end = rest.find('\n').unwrap_or(rest.len());
            let line = rest[..line_end].trim();

            if line.is_empty() {
                rest = rest[line_end..].trim_start();
                if rest.is_empty() {
                    return true;
                }
                continue;
            }

            if line == "/" {
                rest = rest[line_end..].trim_start();
                if rest.is_empty() {
                    return true;
                }
                continue;
            }

            if line.starts_with("--") || Self::is_sqlplus_remark_comment_statement(line) {
                rest = rest[line_end..].trim_start();
                if rest.is_empty() {
                    return true;
                }
                continue;
            }

            return false;
        }
    }

    #[cfg(test)]
    fn statement_ends_with_semicolon(statement: &str) -> bool {
        let tokens = Self::tokenize_sql(statement);
        if Self::statement_ends_with_semicolon_tokens(&tokens) {
            return true;
        }

        let trimmed = statement.trim_end();
        if trimmed.is_empty() {
            return false;
        }

        let spans = super::query_text::tokenize_sql_spanned(trimmed);
        spans
            .iter()
            .rev()
            .find(|span| matches!(&span.token, SqlToken::Symbol(sym) if sym == ";"))
            .is_some_and(|span| {
                Self::trailing_segment_allows_semicolon_terminator(&trimmed[span.end..])
            })
    }

    fn statement_ends_with_semicolon_tokens(tokens: &[SqlToken]) -> bool {
        let mut saw_sqlplus_slash = false;
        for token in tokens.iter().rev() {
            match token {
                SqlToken::Comment(_) => continue,
                SqlToken::Symbol(sym) if sym == "/" => {
                    saw_sqlplus_slash = true;
                }
                SqlToken::Symbol(sym) if sym == ";" => return true,
                _ => return saw_sqlplus_slash,
            }
        }
        false
    }

    fn should_append_missing_statement_terminator(tokens: &[SqlToken]) -> bool {
        let trailing_tokens = Self::tokens_after_last_semicolon(tokens);
        if !Self::delimiter_pairs_are_balanced(trailing_tokens) {
            return false;
        }

        let mut trailing_token = None;

        for token in trailing_tokens.iter().rev() {
            match token {
                SqlToken::Comment(_) => continue,
                SqlToken::Symbol(sym) if sym == "/" => continue,
                other => {
                    trailing_token = Some(other);
                    break;
                }
            }
        }

        trailing_token.is_some_and(Self::token_can_terminate_statement)
    }

    fn tokens_after_last_semicolon(tokens: &[SqlToken]) -> &[SqlToken] {
        let mut last_semicolon_idx = None;

        for (idx, token) in tokens.iter().enumerate() {
            if matches!(token, SqlToken::Symbol(sym) if sym == ";") {
                last_semicolon_idx = Some(idx);
            }
        }

        if let Some(semicolon_idx) = last_semicolon_idx {
            &tokens[semicolon_idx.saturating_add(1)..]
        } else {
            tokens
        }
    }

    fn delimiter_pairs_are_balanced(tokens: &[SqlToken]) -> bool {
        let mut paren_depth = 0usize;
        let mut bracket_depth = 0usize;

        for token in tokens {
            let SqlToken::Symbol(symbol) = token else {
                continue;
            };

            match symbol.as_str() {
                "(" => paren_depth = paren_depth.saturating_add(1),
                ")" => {
                    if paren_depth == 0 {
                        return false;
                    }
                    paren_depth -= 1;
                }
                "[" => bracket_depth = bracket_depth.saturating_add(1),
                "]" => {
                    if bracket_depth == 0 {
                        return false;
                    }
                    bracket_depth -= 1;
                }
                _ => {}
            }
        }

        paren_depth == 0 && bracket_depth == 0
    }

    fn token_can_terminate_statement(token: &SqlToken) -> bool {
        match token {
            SqlToken::Word(word) => Self::word_token_can_terminate_statement(word),
            SqlToken::String(literal) => Self::string_token_can_terminate_statement(literal),
            SqlToken::Symbol(symbol) => matches!(symbol.as_str(), ")" | "]"),
            SqlToken::Comment(_) => false,
        }
    }

    fn word_token_can_terminate_statement(word: &str) -> bool {
        if word.starts_with('"') {
            return word.ends_with('"') && word.len() > 1;
        }

        if word.starts_with("<<") {
            return word.ends_with(">>") && word.len() > 3;
        }

        true
    }

    fn string_token_can_terminate_statement(literal: &str) -> bool {
        if literal.starts_with('$') {
            return Self::has_closed_dollar_quote(literal);
        }

        literal.ends_with('\'') && literal.len() > 1
    }

    fn has_closed_dollar_quote(literal: &str) -> bool {
        let bytes = literal.as_bytes();
        if bytes.first().copied() != Some(b'$') {
            return false;
        }

        let close_tag_idx =
            bytes.iter().enumerate().skip(1).find_map(
                |(idx, byte)| {
                    if *byte == b'$' {
                        Some(idx)
                    } else {
                        None
                    }
                },
            );

        let Some(close_tag_idx) = close_tag_idx else {
            return false;
        };

        let tag_end = close_tag_idx + 1;
        let Some(tag) = literal.get(..tag_end) else {
            return false;
        };

        literal.len() >= tag.len().saturating_mul(2) && literal.ends_with(tag)
    }

    fn classify_comment_attachment(
        out: &str,
        at_line_start: bool,
        has_leading_newline: bool,
        is_multiline_block_comment: bool,
    ) -> CommentAttachment {
        if out.trim().is_empty() {
            return CommentAttachment::FileHeader;
        }

        if has_leading_newline {
            return CommentAttachment::Next;
        }

        if at_line_start {
            if is_multiline_block_comment {
                CommentAttachment::Block
            } else {
                CommentAttachment::Next
            }
        } else {
            CommentAttachment::Previous
        }
    }

    pub(super) fn format_tool_command(command: &ToolCommand) -> String {
        match command {
            ToolCommand::Var { name, data_type } => {
                format!("VAR {} {}", name, data_type.display())
            }
            ToolCommand::Print { name } => match name {
                Some(name) => format!("PRINT {}", name),
                None => "PRINT".to_string(),
            },
            ToolCommand::SetServerOutput {
                enabled,
                size,
                unlimited,
            } => {
                if !*enabled {
                    "SET SERVEROUTPUT OFF".to_string()
                } else if *unlimited {
                    "SET SERVEROUTPUT ON SIZE UNLIMITED".to_string()
                } else if let Some(size) = size {
                    format!("SET SERVEROUTPUT ON SIZE {}", size)
                } else {
                    "SET SERVEROUTPUT ON".to_string()
                }
            }
            ToolCommand::ShowErrors {
                object_type,
                object_name,
            } => {
                if let (Some(obj_type), Some(obj_name)) = (object_type, object_name) {
                    format!("SHOW ERRORS {} {}", obj_type, obj_name)
                } else {
                    "SHOW ERRORS".to_string()
                }
            }
            ToolCommand::ShowUser => "SHOW USER".to_string(),
            ToolCommand::ShowAll => "SHOW ALL".to_string(),
            ToolCommand::Describe { name } => format!("DESCRIBE {}", name),
            ToolCommand::Prompt { text } => {
                if text.trim().is_empty() {
                    "PROMPT".to_string()
                } else {
                    format!("PROMPT {}", text)
                }
            }
            ToolCommand::Pause { message } => match message {
                Some(text) if !text.trim().is_empty() => format!("PAUSE {}", text),
                _ => "PAUSE".to_string(),
            },
            ToolCommand::Accept { name, prompt } => match prompt {
                Some(text) => {
                    format!(
                        "ACCEPT {} PROMPT '{}'",
                        name,
                        Self::escape_sql_literal(text)
                    )
                }
                None => format!("ACCEPT {}", name),
            },
            ToolCommand::Define { name, value } => format!("DEFINE {} = {}", name, value),
            ToolCommand::Undefine { name } => format!("UNDEFINE {}", name),
            ToolCommand::ColumnNewValue {
                column_name,
                variable_name,
            } => format!("COLUMN {} NEW_VALUE {}", column_name, variable_name),
            ToolCommand::BreakOn { column_name } => format!("BREAK ON {}", column_name),
            ToolCommand::BreakOff => "BREAK OFF".to_string(),
            ToolCommand::ClearBreaks => "CLEAR BREAKS".to_string(),
            ToolCommand::ClearComputes => "CLEAR COMPUTES".to_string(),
            ToolCommand::ClearBreaksComputes => "CLEAR BREAKS\nCLEAR COMPUTES".to_string(),
            ToolCommand::Compute {
                mode,
                of_column,
                on_column,
            } => {
                let mode_text = match mode {
                    crate::db::ComputeMode::Sum => "SUM",
                    crate::db::ComputeMode::Count => "COUNT",
                };
                match (of_column.as_deref(), on_column.as_deref()) {
                    (Some(of_col), Some(on_col)) => {
                        format!("COMPUTE {} OF {} ON {}", mode_text, of_col, on_col)
                    }
                    _ => format!("COMPUTE {}", mode_text),
                }
            }
            ToolCommand::ComputeOff => "COMPUTE OFF".to_string(),
            ToolCommand::SetErrorContinue { enabled } => {
                if *enabled {
                    "SET ERRORCONTINUE ON".to_string()
                } else {
                    "SET ERRORCONTINUE OFF".to_string()
                }
            }
            ToolCommand::SetAutoCommit { enabled } => {
                if *enabled {
                    "SET AUTOCOMMIT ON".to_string()
                } else {
                    "SET AUTOCOMMIT OFF".to_string()
                }
            }
            ToolCommand::SetDefine {
                enabled,
                define_char,
            } => {
                if let Some(ch) = define_char {
                    format!("SET DEFINE '{}'", ch)
                } else if *enabled {
                    "SET DEFINE ON".to_string()
                } else {
                    "SET DEFINE OFF".to_string()
                }
            }
            ToolCommand::SetScan { enabled } => {
                if *enabled {
                    "SET SCAN ON".to_string()
                } else {
                    "SET SCAN OFF".to_string()
                }
            }
            ToolCommand::SetVerify { enabled } => {
                if *enabled {
                    "SET VERIFY ON".to_string()
                } else {
                    "SET VERIFY OFF".to_string()
                }
            }
            ToolCommand::SetEcho { enabled } => {
                if *enabled {
                    "SET ECHO ON".to_string()
                } else {
                    "SET ECHO OFF".to_string()
                }
            }
            ToolCommand::SetTiming { enabled } => {
                if *enabled {
                    "SET TIMING ON".to_string()
                } else {
                    "SET TIMING OFF".to_string()
                }
            }
            ToolCommand::SetFeedback { enabled } => {
                if *enabled {
                    "SET FEEDBACK ON".to_string()
                } else {
                    "SET FEEDBACK OFF".to_string()
                }
            }
            ToolCommand::SetHeading { enabled } => {
                if *enabled {
                    "SET HEADING ON".to_string()
                } else {
                    "SET HEADING OFF".to_string()
                }
            }
            ToolCommand::SetPageSize { size } => format!("SET PAGESIZE {}", size),
            ToolCommand::SetLineSize { size } => format!("SET LINESIZE {}", size),
            ToolCommand::SetTrimSpool { enabled } => {
                if *enabled {
                    "SET TRIMSPOOL ON".to_string()
                } else {
                    "SET TRIMSPOOL OFF".to_string()
                }
            }
            ToolCommand::SetTrimOut { enabled } => {
                if *enabled {
                    "SET TRIMOUT ON".to_string()
                } else {
                    "SET TRIMOUT OFF".to_string()
                }
            }
            ToolCommand::SetSqlBlankLines { enabled } => {
                if *enabled {
                    "SET SQLBLANKLINES ON".to_string()
                } else {
                    "SET SQLBLANKLINES OFF".to_string()
                }
            }
            ToolCommand::SetTab { enabled } => {
                if *enabled {
                    "SET TAB ON".to_string()
                } else {
                    "SET TAB OFF".to_string()
                }
            }
            ToolCommand::SetColSep { separator } => format!("SET COLSEP {}", separator),
            ToolCommand::SetNull { null_text } => format!("SET NULL {}", null_text),
            ToolCommand::Spool { path, append } => match path {
                Some(path) if *append => format!("SPOOL {} APPEND", path),
                Some(path) => format!("SPOOL {}", path),
                None if *append => "SPOOL APPEND".to_string(),
                None => "SPOOL OFF".to_string(),
            },
            ToolCommand::WheneverSqlError { exit, action } => {
                let mode = if *exit { "EXIT" } else { "CONTINUE" };
                match action.as_deref() {
                    Some(extra) if !extra.trim().is_empty() => {
                        format!("WHENEVER SQLERROR {} {}", mode, extra.trim())
                    }
                    _ => format!("WHENEVER SQLERROR {}", mode),
                }
            }
            ToolCommand::WheneverOsError { exit } => {
                if *exit {
                    "WHENEVER OSERROR EXIT".to_string()
                } else {
                    "WHENEVER OSERROR CONTINUE".to_string()
                }
            }
            ToolCommand::Exit => "EXIT".to_string(),
            ToolCommand::Quit => "QUIT".to_string(),
            ToolCommand::RunScript {
                path,
                relative_to_caller,
            } => {
                if *relative_to_caller {
                    format!("@@{}", path)
                } else {
                    format!("@{}", path)
                }
            }
            ToolCommand::Connect {
                username,
                password,
                host,
                port,
                service_name,
            } => {
                // 자동 포맷팅 결과를 AI(Codex/Claude)가 재마스킹하지 않도록 실제 비밀번호를 그대로 유지한다.
                format!(
                    "CONNECT {}/{}@{}:{}/{}",
                    username, password, host, port, service_name
                )
            }
            ToolCommand::Disconnect => "DISCONNECT".to_string(),
            ToolCommand::Unsupported { raw, .. } => raw.clone(),
        }
    }

    fn format_statement(
        statement: &str,
        tokens: &[SqlToken],
        select_list_break_state_on_start: SelectListBreakState,
    ) -> String {
        if Self::is_sqlplus_remark_comment_statement(statement) {
            return statement.to_string();
        }

        if let Some(formatted) = Self::format_create_table(statement) {
            return formatted;
        }

        let clause_keywords = FORMAT_CLAUSE_KEYWORDS;
        let join_modifiers = FORMAT_JOIN_MODIFIER_KEYWORDS;
        let join_keyword = "JOIN";
        let outer_keyword = "OUTER";
        let condition_keywords = FORMAT_CONDITION_KEYWORDS; // ELSE handled separately for IF blocks
                                                            // BEGIN is handled separately to support DECLARE ... BEGIN ... END blocks
                                                            // CASE is handled separately for SELECT vs PL/SQL context
                                                            // LOOP is handled separately for FOR ... LOOP on same line
        let block_start_keywords = FORMAT_BLOCK_START_KEYWORDS;
        let block_end_qualifiers = FORMAT_BLOCK_END_QUALIFIER_KEYWORDS; // END LOOP, END IF, END CASE, END REPEAT

        let mut out = String::new();
        let mut indent_level = 0usize;
        let mut suppress_comma_break_depth = 0usize;
        let mut paren_stack: Vec<bool> = Vec::new();
        let mut paren_clause_restore_stack: Vec<Option<String>> = Vec::new();
        let mut block_stack: Vec<String> = Vec::new(); // Track which block keywords started blocks
        let mut at_line_start = true;
        let mut needs_space = false;
        let mut line_indent = 0usize;
        let mut join_modifier_active = false;
        let mut after_for_while = false; // Track FOR/WHILE for LOOP on same line
        let mut in_plsql_block = false; // Track if we're in PL/SQL block (for CASE handling)
        let mut prev_word_upper: Option<String> = None;
        let mut construct = ConstructState::default();
        let mut column_list_stack: Vec<bool> = Vec::new();
        let mut current_clause: Option<String> = None;
        let mut pending_package_member_separator = false;
        let mut open_cursor_state = OpenCursorFormatState::None;
        let mut open_for_select_stack: Vec<OpenCursorFormatState> = Vec::new();
        let mut case_branch_started: Vec<bool> = Vec::new();
        let mut between_pending = false;
        let mut select_list_layout_state = SelectListLayoutState::Inactive;
        let mut select_list_break_state = select_list_break_state_on_start;
        let mut exit_condition_state = ExitConditionState::None;
        let mut with_cte_state = WithCteFormatState::None;
        let mut statement_has_with_clause = false;
        let mut paren_indent_increase_stack: Vec<usize> = Vec::new();
        let mut trigger_header_state = TriggerHeaderState::None;
        let mut compound_trigger_state = CompoundTriggerState::None;
        let mut inline_comment_continuation_state = InlineCommentContinuationState::None;
        let is_package_body_statement = {
            let upper = statement.to_ascii_uppercase();
            upper.contains("CREATE OR REPLACE PACKAGE BODY")
                || upper.contains("CREATE PACKAGE BODY")
        };
        let statement_has_apply = statement.to_ascii_uppercase().contains(" APPLY");
        let query_apply_flags = Self::query_apply_flags(tokens);

        let newline_with = |out: &mut String,
                            indent_level: usize,
                            extra: usize,
                            at_line_start: &mut bool,
                            needs_space: &mut bool,
                            line_indent: &mut usize| {
            if !out.is_empty() && !out.ends_with('\n') {
                out.push('\n');
            }
            *line_indent = indent_level + extra;
            *at_line_start = true;
            *needs_space = false;
        };
        let newline_with_spaces = |out: &mut String,
                                   indent_spaces: usize,
                                   at_line_start: &mut bool,
                                   needs_space: &mut bool,
                                   line_indent: &mut usize| {
            if !out.is_empty() && !out.ends_with('\n') {
                out.push('\n');
            }
            out.push_str(&" ".repeat(indent_spaces));
            *line_indent = indent_spaces / 4;
            *at_line_start = false;
            *needs_space = false;
        };

        let base_indent = |indent_level: usize, open_cursor_state: OpenCursorFormatState| {
            open_cursor_state.base_indent(indent_level)
        };

        let clause_indent =
            |indent_level: usize,
             open_cursor_state: OpenCursorFormatState,
             _keyword: &str,
             _open_for_select_active: bool,
             _in_cursor_sql: bool| base_indent(indent_level, open_cursor_state);

        let list_item_indent =
            |indent_level: usize,
             open_cursor_state: OpenCursorFormatState,
             select_list_layout_state: SelectListLayoutState| {
                let base = base_indent(indent_level, open_cursor_state);
                select_list_layout_state.indentation_or(base + 1)
            };
        let clause_list_indent = |indent_level: usize,
                                  open_cursor_state: OpenCursorFormatState,
                                  select_list_layout_state: SelectListLayoutState,
                                  current_clause: Option<&str>,
                                  merge_active: bool| {
            let base = base_indent(indent_level, open_cursor_state);
            if matches!(current_clause, Some("SET")) && merge_active {
                base + 1
            } else {
                list_item_indent(indent_level, open_cursor_state, select_list_layout_state)
                    .max(base + 1)
            }
        };
        let active_list_indent = |indent_level: usize,
                                  open_cursor_state: OpenCursorFormatState,
                                  select_list_layout_state: SelectListLayoutState,
                                  current_clause: Option<&str>,
                                  merge_active: bool,
                                  in_column_list: bool| {
            if in_column_list {
                base_indent(indent_level, open_cursor_state)
            } else {
                clause_list_indent(
                    indent_level,
                    open_cursor_state,
                    select_list_layout_state,
                    current_clause,
                    merge_active,
                )
            }
        };
        let ensure_indent = |out: &mut String, at_line_start: &mut bool, line_indent: usize| {
            if *at_line_start {
                out.push_str(&" ".repeat(line_indent * 4));
                *at_line_start = false;
            }
        };

        let trim_trailing_space = |out: &mut String| {
            while out.ends_with(' ') {
                out.pop();
            }
        };

        let force_select_list_newline =
            |out: &mut String, select_list_layout_state: &mut SelectListLayoutState| {
                *select_list_layout_state = select_list_layout_state.force_newline_if_possible(out);
            };

        let mut idx = 0;
        while idx < tokens.len() {
            let current_query_has_apply = query_apply_flags.get(idx).copied().unwrap_or(false);
            if at_line_start && !matches!(tokens[idx], SqlToken::Comment(_)) {
                if let InlineCommentContinuationState::Operand { indent } =
                    inline_comment_continuation_state
                {
                    line_indent = indent;
                    inline_comment_continuation_state = InlineCommentContinuationState::None;
                }
            }
            let next_word = tokens[idx + 1..].iter().find_map(|t| match t {
                SqlToken::Word(w) => Some(w.as_str()),
                _ => None,
            });
            let next_non_comment = tokens[idx + 1..]
                .iter()
                .find(|t| !matches!(t, SqlToken::Comment(_)));
            let next_word_is =
                |expected: &str| next_word.is_some_and(|word| word.eq_ignore_ascii_case(expected));

            match &tokens[idx] {
                SqlToken::Word(word) => {
                    let upper = word.to_uppercase();
                    let with_plsql_body_starts_here = matches!(upper.as_str(), "AS" | "IS")
                        && with_cte_state.collecting_routine_declaration_body_start();
                    with_cte_state.on_word(upper.as_str());
                    let in_sql_case_clause = matches!(
                        current_clause.as_deref(),
                        Some(
                            "SELECT"
                                | "WHERE"
                                | "ORDER"
                                | "GROUP"
                                | "HAVING"
                                | "VALUES"
                                | "SET"
                                | "INTO"
                        )
                    );
                    let is_keyword = sql_text::is_oracle_sql_keyword(upper.as_str());
                    let is_analytic_within_group = upper == "GROUP"
                        && matches!(prev_word_upper.as_deref(), Some("WITHIN"))
                        && {
                            let mut paren_depth = 0usize;
                            let mut saw_group_paren = false;
                            let mut lookahead_idx = idx.saturating_add(1);
                            let mut has_over = false;
                            while lookahead_idx < tokens.len() {
                                match &tokens[lookahead_idx] {
                                    SqlToken::Comment(_) => {}
                                    SqlToken::Symbol(sym) if sym == "(" => {
                                        paren_depth = paren_depth.saturating_add(1);
                                        saw_group_paren = true;
                                    }
                                    SqlToken::Symbol(sym) if sym == ")" => {
                                        if paren_depth == 0 {
                                            break;
                                        }
                                        paren_depth = paren_depth.saturating_sub(1);
                                    }
                                    SqlToken::Symbol(sym)
                                        if paren_depth == 0 && (sym == "," || sym == ";") =>
                                    {
                                        break;
                                    }
                                    SqlToken::Word(word)
                                        if paren_depth == 0
                                            && saw_group_paren
                                            && word.eq_ignore_ascii_case("OVER") =>
                                    {
                                        has_over = true;
                                        break;
                                    }
                                    _ => {}
                                }
                                lookahead_idx = lookahead_idx.saturating_add(1);
                            }
                            has_over
                        };
                    let is_within_group = upper == "GROUP"
                        && matches!(prev_word_upper.as_deref(), Some("WITHIN"))
                        && !is_analytic_within_group;
                    let mut newline_after_keyword = false;
                    let mut newline_after_keyword_extra = 0usize;
                    let is_between_and = upper == "AND" && between_pending;
                    let is_exit_when = exit_condition_state.is_exit_when(upper.as_str());
                    let is_trigger_event_keyword = trigger_header_state.is_active()
                        && matches!(upper.as_str(), "INSERT" | "UPDATE" | "DELETE");
                    let is_compound_trigger_timing_header = compound_trigger_state
                        .is_in_outer_body()
                        && block_stack.last().is_some_and(|s| s == "COMPOUND_TRIGGER")
                        && (matches!(upper.as_str(), "BEFORE" | "AFTER")
                            && next_word.is_some_and(|word| {
                                matches!(word.to_ascii_uppercase().as_str(), "STATEMENT" | "EACH")
                            })
                            || (upper == "INSTEAD" && next_word_is("OF")));
                    let is_create_index_on = upper == "ON" && construct.create_index_pending;
                    let follows_alias_keyword =
                        matches!(prev_word_upper.as_deref(), Some("AS" | "IS"));
                    let in_table_alias_clause = matches!(
                        current_clause.as_deref(),
                        Some("FROM" | "UPDATE" | "INTO" | "MERGE" | "USING")
                    );
                    let in_select_clause = matches!(current_clause.as_deref(), Some("SELECT"));
                    let next_word_is_clause_keyword = next_word.is_none_or(|word| {
                        let next_upper = word.to_ascii_uppercase();
                        sql_text::is_oracle_sql_keyword(next_upper.as_str())
                    });
                    let next_token_ends_select_item = matches!(
                        next_non_comment,
                        Some(SqlToken::Symbol(sym)) if matches!(sym.as_str(), "," | ")")
                    ) || next_word_is_clause_keyword;
                    let next_token_ends_from_alias = matches!(
                        next_non_comment,
                        Some(SqlToken::Symbol(sym)) if matches!(sym.as_str(), "," | ")")
                    ) || next_word_is_clause_keyword;
                    let next_token_is_dot =
                        matches!(next_non_comment, Some(SqlToken::Symbol(sym)) if sym == ".");
                    let closes_case_expression =
                        upper == "END" && block_stack.last().is_some_and(|s| s == "CASE");
                    let treat_control_keyword_as_identifier =
                        sql_text::is_plsql_control_keyword(upper.as_str())
                            && !closes_case_expression
                            && !next_word_is("THEN")
                            && (follows_alias_keyword
                                || (in_table_alias_clause && next_token_ends_from_alias)
                                || (in_select_clause
                                    && !next_word_is("AS")
                                    && next_token_ends_select_item)
                                || (next_token_is_dot
                                    && matches!(
                                        current_clause.as_deref(),
                                        Some(
                                            "SELECT"
                                                | "FROM"
                                                | "WHERE"
                                                | "ON"
                                                | "GROUP"
                                                | "HAVING"
                                                | "ORDER"
                                        )
                                    )));
                    let should_treat_as_block_start = block_start_keywords
                        .contains(&upper.as_str())
                        && !treat_control_keyword_as_identifier
                        && !(follows_alias_keyword
                            && sql_text::is_plsql_control_keyword(upper.as_str())
                            && !next_word_is("THEN"));
                    let at_package_body_member_depth =
                        is_package_body_statement && indent_level == 1;
                    if upper == "END" && !treat_control_keyword_as_identifier {
                        let active_end_block = block_stack.last().map(String::as_str);
                        let end_qualifier = {
                            let mut qualifier = None;
                            for t in &tokens[idx + 1..] {
                                match t {
                                    SqlToken::Comment(comment) => {
                                        if comment.contains('\n') {
                                            break;
                                        }
                                    }
                                    SqlToken::Word(w) => {
                                        qualifier = Some(w.to_uppercase());
                                        break;
                                    }
                                    SqlToken::Symbol(sym) if sym == ";" => break,
                                    _ => break,
                                }
                            }
                            qualifier
                        };
                        // Check if this is END LOOP, END IF, END CASE, etc.
                        let mut end_tail: Vec<String> = Vec::new();
                        if let Some(qualifier) = end_qualifier.as_deref() {
                            match qualifier {
                                "LOOP" | "IF" | "CASE" | "REPEAT" | "FOR" | "WHILE" => {
                                    let qualifier_belongs_to_case_end =
                                        active_end_block == Some("CASE") && qualifier == "CASE";
                                    let qualifier_is_case_expression_follower =
                                        active_end_block == Some("CASE") && qualifier != "CASE";
                                    if qualifier_belongs_to_case_end
                                        || !qualifier_is_case_expression_follower
                                    {
                                        end_tail.push(qualifier.to_string());
                                    }
                                }
                                "BEFORE" | "AFTER" => {
                                    end_tail.push(qualifier.to_string());
                                    let mut lookahead = idx + 1;
                                    while lookahead < tokens.len() {
                                        match &tokens[lookahead] {
                                            SqlToken::Comment(comment) => {
                                                if !comment.contains('\n') {
                                                    lookahead += 1;
                                                    continue;
                                                }
                                                break;
                                            }
                                            SqlToken::Word(word) => {
                                                let qualifier_part = word.to_uppercase();
                                                if end_tail
                                                    .last()
                                                    .is_some_and(|value| value == "EACH")
                                                {
                                                    if qualifier_part == "ROW" {
                                                        end_tail.push(qualifier_part);
                                                    }
                                                    break;
                                                }
                                                if matches!(
                                                    qualifier_part.as_str(),
                                                    "EACH" | "STATEMENT"
                                                ) {
                                                    end_tail.push(qualifier_part);
                                                    lookahead += 1;
                                                    continue;
                                                }
                                                break;
                                            }
                                            SqlToken::Symbol(sym) if sym == ";" => break,
                                            _ => break,
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                        let is_qualified_end = matches!(
                            end_tail.first().map(String::as_str),
                            Some("LOOP" | "IF" | "CASE" | "REPEAT" | "FOR" | "WHILE")
                        );
                        let paren_extra = usize::from(suppress_comma_break_depth > 0);

                        let case_expression_end =
                            !is_qualified_end && block_stack.last().is_some_and(|s| s == "CASE");

                        if is_qualified_end {
                            // END LOOP, END IF, END CASE - pop matching block
                            if let Some(top) = block_stack.last() {
                                if block_end_qualifiers.contains(&top.as_str()) {
                                    block_stack.pop();
                                }
                            }
                            if end_tail.first().is_some_and(|q| q == "CASE")
                                && !case_branch_started.is_empty()
                            {
                                case_branch_started.pop();
                            }
                        } else if case_expression_end {
                            block_stack.pop();
                            if !case_branch_started.is_empty() {
                                case_branch_started.pop();
                            }
                        } else {
                            // Plain END - closes BEGIN or DECLARE/PACKAGE_BODY block
                            // Pop until we find BEGIN or DECLARE/PACKAGE_BODY
                            let mut closed_block = None;
                            while let Some(top) = block_stack.pop() {
                                if top == "BEGIN"
                                    || top == "DECLARE"
                                    || top == "PACKAGE_BODY"
                                    || top == "COMPOUND_TRIGGER"
                                {
                                    closed_block = Some(top);
                                    break;
                                }
                            }
                            if matches!(closed_block.as_deref(), Some("BEGIN" | "DECLARE"))
                                && (block_stack.last().is_some_and(|s| s == "PACKAGE_BODY")
                                    || at_package_body_member_depth)
                            {
                                pending_package_member_separator = true;
                            }
                        }

                        indent_level = indent_level.saturating_sub(1);
                        if !block_stack.iter().any(|s| s == "COMPOUND_TRIGGER") {
                            compound_trigger_state.clear();
                        }
                        if is_package_body_statement
                            && !is_qualified_end
                            && !case_expression_end
                            && indent_level == 1
                        {
                            pending_package_member_separator = true;
                        }
                        let end_extra =
                            if case_expression_end && (in_sql_case_clause || !in_plsql_block) {
                                1
                            } else {
                                0
                            };
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            end_extra + paren_extra,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );

                        // Output "END"
                        ensure_indent(&mut out, &mut at_line_start, line_indent);
                        out.push_str("END");

                        // If qualified (END LOOP/IF/CASE/REPEAT/BEFORE/AFTER), output tail and skip it.
                        let skip_count = end_tail.len();
                        for qualifier in end_tail.iter() {
                            out.push(' ');
                            out.push_str(qualifier);
                        }
                        needs_space = true;
                        if skip_count == 0 {
                            idx += 1;
                        } else {
                            let mut lookahead = idx + 1;
                            let mut words_skipped = 0usize;
                            while lookahead < tokens.len() && words_skipped < skip_count {
                                match &tokens[lookahead] {
                                    SqlToken::Word(_) => {
                                        words_skipped += 1;
                                    }
                                    SqlToken::Comment(comment) => {
                                        if comment.contains('\n') {
                                            break;
                                        }
                                    }
                                    _ => {}
                                }
                                lookahead += 1;
                            }
                            idx = lookahead;
                        }
                        continue;
                    } else if is_compound_trigger_timing_header {
                        compound_trigger_state.start_timing_point();
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            0,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    } else if trigger_header_state.is_active()
                        && matches!(upper.as_str(), "BEFORE" | "AFTER" | "INSTEAD" | "REFERENCING")
                    {
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            1,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    } else if is_trigger_event_keyword
                        && matches!(prev_word_upper.as_deref(), Some("BEFORE" | "AFTER" | "OF"))
                    {
                        // Keep trigger event verbs on the same line as BEFORE/AFTER/INSTEAD OF.
                    } else if construct.merge_when_branch_active && upper == "SET" {
                        // MERGE UPDATE SET keeps SET inline with UPDATE, but we still need
                        // SET clause context so following list comments/commas align correctly.
                        current_clause = Some(upper.clone());
                        select_list_layout_state.clear();
                    } else if clause_keywords.contains(&upper.as_str())
                        && !construct.suppresses_clause_break(
                            tokens,
                            idx,
                            upper.as_str(),
                            current_clause.as_deref(),
                            prev_word_upper.as_deref(),
                            in_plsql_block,
                            suppress_comma_break_depth,
                            paren_stack.iter().any(|is_subquery| *is_subquery),
                            trigger_header_state,
                            is_analytic_within_group,
                            !Self::fetch_into_has_multiple_targets(tokens, idx),
                            construct.analytic_over_paren_depth.is_some_and(|depth| {
                                paren_stack.len() >= depth
                            }),
                        )
                    {
                        // Keep shallow FORALL bodies one level deeper, but do not
                        // keep drifting nested package-body DML further right.
                        if construct.forall_pending
                            && matches!(upper.as_str(), "INSERT" | "UPDATE" | "DELETE" | "MERGE")
                        {
                            let forall_body_extra = usize::from(indent_level <= 1);
                            indent_level += forall_body_extra;
                            construct.forall_pending = false;
                            construct.forall_body_active = forall_body_extra > 0;
                        }
                        if !is_within_group {
                            let insert_all_extra = if construct.insert_all_active
                                && matches!(upper.as_str(), "INTO" | "VALUES")
                            {
                                2
                            } else {
                                0
                            };
                            // INSERT ALL: stop at SELECT (the trailing query)
                            if construct.insert_all_active && upper == "SELECT" {
                                construct.insert_all_active = false;
                            }
                            let cursor_clause_dedent = construct.cursor_sql_active
                                && !paren_stack.iter().any(|is_subquery| *is_subquery)
                                && matches!(
                                    upper.as_str(),
                                    "FROM"
                                        | "WHERE"
                                        | "GROUP"
                                        | "ORDER"
                                        | "CONNECT"
                                        | "HAVING"
                                        | "UNION"
                                        | "INTERSECT"
                                        | "MINUS"
                                );
                            let clause_indent_level = if cursor_clause_dedent {
                                indent_level.saturating_sub(1)
                            } else {
                                indent_level
                            };
                            newline_with(
                                &mut out,
                                clause_indent(
                                    clause_indent_level,
                                    open_cursor_state,
                                    upper.as_str(),
                                    open_cursor_state
                                        .select_depth()
                                        .is_some_and(|depth| paren_stack.len() == depth),
                                    construct.cursor_sql_active,
                                ),
                                insert_all_extra,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        }
                        if !is_within_group {
                            current_clause = Some(upper.clone());
                            if upper != "SELECT" {
                                select_list_layout_state.clear();
                            }
                            if upper == "SELECT"
                                && open_cursor_state.in_select()
                                && !matches!(
                                    with_cte_state,
                                    WithCteFormatState::InDefinitions { .. }
                                )
                            {
                                // OPEN ... FOR WITH ... SELECT should anchor to the main query
                                // head after CTE definitions, not to inner SELECTs inside the
                                // WITH body. Otherwise a later `FOR` in PIVOT/UNPIVOT or
                                // similar syntax can be misread as a new OPEN ... FOR.
                                open_cursor_state.set_select_depth(paren_stack.len());
                            }
                            if upper == "WITH" {
                                statement_has_with_clause = true;
                            }
                            if matches!(
                                upper.as_str(),
                                "SELECT"
                                    | "FROM"
                                    | "WHERE"
                                    | "GROUP"
                                    | "HAVING"
                                    | "ORDER"
                                    | "UNION"
                                    | "INTERSECT"
                                    | "MINUS"
                                    | "EXCEPT"
                            ) {
                                construct.search_cycle_clause_active = false;
                            }
                        }
                    } else if is_analytic_within_group {
                        newline_with(
                            &mut out,
                            list_item_indent(
                                indent_level,
                                open_cursor_state,
                                select_list_layout_state,
                            ),
                            0,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    } else if trigger_header_state.is_active()
                        && upper == "WHEN"
                    {
                        // Trigger WHEN clause: align with other trigger header keywords
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            1,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    } else if condition_keywords.contains(&upper.as_str())
                        && !is_between_and
                        && !is_exit_when
                        && !construct.suppresses_condition_break(
                            upper.as_str(),
                            prev_word_upper.as_deref(),
                            trigger_header_state,
                        )
                    {
                        let clause_base_indent = clause_indent(
                            indent_level,
                            open_cursor_state,
                            upper.as_str(),
                            open_cursor_state
                                .select_depth()
                                .is_some_and(|depth| paren_stack.len() == depth),
                            construct.cursor_sql_active,
                        );
                        let paren_extra = usize::from(suppress_comma_break_depth > 0);
                        if upper == "WHEN"
                            && block_stack.last().is_some_and(|s| s == "CASE")
                            && case_branch_started.last().is_some()
                        {
                            if let Some(last) = case_branch_started.last_mut() {
                                *last = true;
                            }
                        }
                        let is_merge_when = upper == "WHEN"
                            && construct.merge_active
                            && block_stack.last().is_none_or(|s| s != "CASE");
                        let uses_where_hanging_condition_indent =
                            matches!(upper.as_str(), "AND" | "OR")
                                && matches!(current_clause.as_deref(), Some("WHERE" | "HAVING"));
                        if is_merge_when {
                            // MERGE WHEN MATCHED/NOT MATCHED: at base indent
                            if construct.merge_when_branch_active {
                                indent_level = indent_level.saturating_sub(1);
                                construct.merge_when_branch_active = false;
                            }
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                0,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        } else if current_query_has_apply && uses_where_hanging_condition_indent {
                            newline_with_spaces(
                                &mut out,
                                clause_base_indent
                                    .saturating_mul(4)
                                    .saturating_add(2)
                                    .saturating_add(paren_extra.saturating_mul(4)),
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        } else {
                            newline_with(
                                &mut out,
                                clause_base_indent,
                                1 + paren_extra,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        }
                    } else if upper == "CREATE" {
                        construct.create_pending = true;
                        construct.create_object = None;
                    } else if construct.create_pending && (upper == "OR" || upper == "REPLACE") {
                        // part of CREATE OR REPLACE
                    } else if construct.create_pending && upper == "PACKAGE" {
                        if next_word_is("BODY") {
                            construct.create_object = Some("PACKAGE_BODY".to_string());
                        } else {
                            construct.create_object = Some("PACKAGE".to_string());
                        }
                        construct.create_pending = false;
                    } else if construct.create_pending && upper == "TABLE" {
                        construct.create_table_paren_expected = true;
                        construct.create_pending = false;
                    } else if construct.create_pending
                        && matches!(upper.as_str(), "INDEX" | "UNIQUE")
                    {
                        if upper == "INDEX" {
                            construct.create_index_pending = true;
                            construct.create_pending = false;
                        }
                        // UNIQUE stays in construct.create_pending to catch UNIQUE INDEX
                    } else if construct.create_pending && upper == "SEQUENCE" {
                        construct.create_sequence_active = true;
                        construct.create_pending = false;
                    } else if construct.create_pending && upper == "SYNONYM" {
                        construct.create_synonym_active = true;
                        construct.create_pending = false;
                    } else if construct.create_pending
                        && matches!(
                            upper.as_str(),
                            "PROCEDURE" | "FUNCTION" | "TYPE" | "TRIGGER"
                        )
                    {
                        construct.create_object = Some(upper.clone());
                        if upper == "TRIGGER" {
                            trigger_header_state.start();
                        }
                        construct.create_pending = false;
                    } else if sql_text::is_with_plsql_declaration_keyword(upper.as_str())
                        && prev_word_upper.as_deref() == Some("WITH")
                    {
                        if !at_line_start {
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                1,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        }
                    } else if matches!(upper.as_str(), "PROCEDURE" | "FUNCTION")
                        && (block_stack.last().is_some_and(|s| s == "DECLARE")
                            || block_stack.iter().any(|s| s == "PACKAGE_BODY")
                            || at_package_body_member_depth)
                    {
                        if !at_line_start {
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                0,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        }
                        construct.routine_decl_pending = true;
                    } else if upper == "ELSE" || upper == "ELSIF" {
                        // ELSE/ELSIF in IF block: same level as IF
                        let in_if_block = block_stack.last().is_some_and(|s| s == "IF");
                        let in_case_block = block_stack.last().is_some_and(|s| s == "CASE");
                        let paren_extra = if suppress_comma_break_depth > 0 { 1 } else { 0 };
                        if upper == "ELSE"
                            && in_case_block
                            && case_branch_started.last().is_some()
                            && !in_if_block
                        {
                            if let Some(last) = case_branch_started.last_mut() {
                                *last = true;
                            }
                        }
                        if in_if_block {
                            newline_with(
                                &mut out,
                                base_indent(indent_level.saturating_sub(1), open_cursor_state),
                                0,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        } else {
                            // ELSE in CASE or other context
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                1 + paren_extra,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        }
                        if upper == "ELSE"
                            && in_plsql_block
                            && !matches!(current_clause.as_deref(), Some("SELECT"))
                        {
                            newline_after_keyword = true;
                        } else if upper == "ELSE" && in_sql_case_clause && next_word_is("CASE") {
                            // Keep ELSE CASE from collapsing into one long SQL expression line.
                            newline_after_keyword = true;
                            newline_after_keyword_extra = 1;
                        }
                    } else if upper == "THEN" {
                        if construct.merge_active && block_stack.last().is_none_or(|s| s != "CASE")
                        {
                            // MERGE WHEN MATCHED THEN / WHEN NOT MATCHED THEN
                            indent_level += 1;
                            construct.merge_when_branch_active = true;
                            newline_after_keyword = true;
                        } else if in_plsql_block
                            && !matches!(current_clause.as_deref(), Some("SELECT"))
                        {
                            newline_after_keyword = true;
                            if block_stack.last().is_some_and(|s| s == "CASE") {
                                newline_after_keyword_extra = 1;
                            }
                        } else if in_sql_case_clause && next_word_is("CASE") {
                            // Nested CASE in SQL expressions should start on its own line.
                            newline_after_keyword = true;
                            newline_after_keyword_extra = 1;
                        }
                    } else if upper == join_keyword || upper == "APPLY" {
                        if !join_modifier_active {
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                0,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        }
                        join_modifier_active = false;
                    } else if join_modifiers.contains(&upper.as_str()) {
                        let next_leads_to_join = next_word_is("JOIN")
                            || next_word_is("OUTER")
                            || next_word_is("APPLY")
                            || next_word
                                .is_some_and(|w| join_modifiers.iter().any(|m| w.eq_ignore_ascii_case(m)));
                        if next_leads_to_join {
                            if !join_modifier_active {
                                newline_with(
                                    &mut out,
                                    base_indent(indent_level, open_cursor_state),
                                    0,
                                    &mut at_line_start,
                                    &mut needs_space,
                                    &mut line_indent,
                                );
                            }
                            join_modifier_active = true;
                        }
                    } else if upper == outer_keyword {
                        if (next_word_is("JOIN") || next_word_is("APPLY")) && !join_modifier_active
                        {
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                0,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                            join_modifier_active = true;
                        }
                    } else if upper == "SEARCH" || upper == "CYCLE" {
                        construct.search_cycle_clause_active = true;
                    } else if construct.match_recognize_paren_depth.is_some_and(|depth| {
                        paren_stack.len() >= depth
                            && (matches!(upper.as_str(), "MEASURES" | "PATTERN" | "DEFINE")
                                || (upper == "ONE" && next_word_is("ROW"))
                                || (upper == "ALL" && next_word_is("ROWS")))
                    }) {
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            0,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    } else if construct.analytic_over_paren_depth.is_some_and(|depth| {
                        paren_stack.len() >= depth
                            && (matches!(upper.as_str(), "PARTITION" | "ORDER" | "ROWS" | "RANGE" | "GROUPS"))
                    }) {
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            0,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    } else if matches!(current_clause.as_deref(), Some("MODEL"))
                        && matches!(
                            upper.as_str(),
                            "PARTITION" | "DIMENSION" | "MEASURES" | "RULES"
                        )
                    {
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            1,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    } else if upper == "OPEN" {
                        open_cursor_state = OpenCursorFormatState::AwaitingFor;
                    } else if upper == "FOR"
                        && next_word_is("UPDATE")
                        && !in_plsql_block
                        && suppress_comma_break_depth == 0
                    {
                        // FOR UPDATE gets its own line at clause base indent.
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            0,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    } else if (upper == "FOR" || upper == "WHILE")
                        && !(upper == "FOR"
                            && (construct.create_synonym_active
                                || suppress_comma_break_depth > 0
                                || (next_word_is("UPDATE") && !in_plsql_block)))
                    {
                        if upper == "FOR" && trigger_header_state.is_active() {
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                1,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                            after_for_while = false;
                        } else if upper == "FOR"
                            && open_cursor_state == OpenCursorFormatState::AwaitingFor
                        {
                            open_for_select_stack.push(open_cursor_state);
                            open_cursor_state = OpenCursorFormatState::InSelect {
                                anchor_indent: indent_level.saturating_add(1),
                                select_paren_depth: None,
                            };
                            newline_after_keyword = true;
                        } else {
                            // FOR/WHILE starts a line, LOOP will follow on same line
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                0,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                            after_for_while = true;
                        }
                    } else if upper == "LOOP" {
                        // LOOP after FOR/WHILE stays on same line
                        if !after_for_while {
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                0,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        }
                        after_for_while = false;
                        // LOOP always starts a block body on the next line.
                        newline_after_keyword = true;
                    } else if upper == "REPEAT" {
                        // REPEAT starts a block body on the next line.
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            0,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    } else if upper == "CASE" {
                        // CASE in PL/SQL block vs SELECT context
                        if in_sql_case_clause {
                            let paren_extra = if suppress_comma_break_depth > 0 { 1 } else { 0 };
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                1 + paren_extra,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        } else if in_plsql_block {
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                0,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        }
                        // In SELECT context, CASE stays inline
                    } else if should_treat_as_block_start {
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            0,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    } else if upper == "BEGIN" {
                        // BEGIN handling: check if we're inside a DECLARE block
                        let inside_declare = block_stack
                            .last()
                            .is_some_and(|s| s == "DECLARE" || s == "PACKAGE_BODY");
                        let begin_after_if_then =
                            matches!(prev_word_upper.as_deref(), Some("THEN"))
                                && block_stack.last().is_some_and(|s| s == "IF");
                        let inside_package_body = block_stack.iter().any(|s| s == "PACKAGE_BODY");
                        if inside_declare {
                            // DECLARE ... BEGIN - BEGIN is at same level as DECLARE
                            // Don't increase indent, just newline at current level
                            newline_with(
                                &mut out,
                                base_indent(indent_level.saturating_sub(1), open_cursor_state),
                                0,
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        } else {
                            // BEGIN inside block statements should align with current block depth.
                            newline_with(
                                &mut out,
                                base_indent(indent_level, open_cursor_state),
                                usize::from(begin_after_if_then && !inside_package_body),
                                &mut at_line_start,
                                &mut needs_space,
                                &mut line_indent,
                            );
                        }
                    }

                    ensure_indent(&mut out, &mut at_line_start, line_indent);
                    if needs_space {
                        out.push(' ');
                    }
                    if is_keyword {
                        out.push_str(&upper);
                    } else {
                        out.push_str(word);
                    }
                    needs_space = true;
                    if upper == "SELECT" {
                        select_list_layout_state
                            .activate(out.len(), base_indent(indent_level, open_cursor_state) + 1);
                    }

                    if prev_word_upper.as_deref() == Some("COMPOUND") && upper == "TRIGGER" {
                        compound_trigger_state.mark_compound_header();
                        trigger_header_state.clear();
                    }

                    if matches!(upper.as_str(), "IS" | "AS" | "BEGIN" | "DECLARE") {
                        compound_trigger_state.begin_timing_point_body();
                    }

                    if construct.create_table_paren_expected
                        && upper == "AS"
                        && (next_word_is("SELECT")
                            || next_word_is("WITH")
                            || next_word_is("VALUES"))
                    {
                        construct.create_table_paren_expected = false;
                    }

                    // CURSOR ... IS/AS → indent the SQL body
                    if upper == "CURSOR" && in_plsql_block {
                        construct.cursor_decl_pending = true;
                    }
                    if matches!(upper.as_str(), "IS" | "AS") && construct.cursor_decl_pending {
                        construct.cursor_decl_pending = false;
                        construct.cursor_sql_active = true;
                        indent_level += 1;
                        newline_after_keyword = true;
                    }

                    // FORALL → indent the DML body
                    if upper == "FORALL" && in_plsql_block {
                        construct.forall_pending = true;
                    }

                    // EXECUTE IMMEDIATE tracking
                    if upper == "EXECUTE" && next_word_is("IMMEDIATE") {
                        construct.execute_immediate_active = true;
                    }

                    // REFERENCES → suppress ON DELETE/UPDATE (referential actions)
                    if upper == "REFERENCES" {
                        construct.referential_action_pending = true;
                    }
                    if construct.referential_action_pending && upper == "ON" {
                        construct.referential_action_pending = false;
                        construct.referential_on_active = true;
                    }
                    if construct.referential_on_active
                        && !matches!(
                            upper.as_str(),
                            "ON" | "DELETE"
                                | "UPDATE"
                                | "SET"
                                | "CASCADE"
                                | "RESTRICT"
                                | "NO"
                                | "ACTION"
                                | "NULL"
                        )
                    {
                        construct.referential_on_active = false;
                    }

                    // COMMENT ON tracking
                    if upper == "COMMENT" && next_word_is("ON") {
                        construct.comment_on_active = true;
                    }
                    if construct.comment_on_active && upper == "ON" {
                        construct.comment_on_active = false; // one-shot: suppress ON, then done
                    }

                    // GRANT/REVOKE tracking
                    if matches!(upper.as_str(), "GRANT" | "REVOKE") && indent_level == 0 {
                        construct.grant_revoke_active = true;
                    }
                    // GRANT/REVOKE ends at ON (for privilege grants) or TO/FROM
                    if construct.grant_revoke_active && matches!(upper.as_str(), "TO" | "FROM") {
                        construct.grant_revoke_active = false;
                    }

                    // RETURNING → suppress next INTO
                    if upper == "RETURNING" {
                        construct.returning_active = true;
                    }
                    if construct.returning_active && upper == "INTO" {
                        construct.returning_active = false;
                    }

                    // FETCH cursor → suppress INTO/LIMIT/BULK (PL/SQL FETCH, not SQL FETCH FIRST)
                    if upper == "FETCH" && in_plsql_block {
                        construct.fetch_active = true;
                    }

                    // BULK COLLECT → suppress next INTO
                    if upper == "BULK" && next_word_is("COLLECT") {
                        construct.bulk_collect_active = true;
                    }
                    if construct.bulk_collect_active && upper == "INTO" {
                        construct.bulk_collect_active = false;
                    }

                    // MERGE tracking
                    if upper == "MERGE" && matches!(current_clause.as_deref(), None | Some("MERGE"))
                    {
                        construct.merge_active = true;
                    }

                    // INSERT ALL/FIRST tracking
                    if matches!(upper.as_str(), "ALL" | "FIRST")
                        && matches!(prev_word_upper.as_deref(), Some("INSERT"))
                    {
                        construct.insert_all_active = true;
                    }

                    let starts_create_block = matches!(upper.as_str(), "AS" | "IS")
                        && !trigger_header_state.is_active()
                        && (construct.create_object.is_some()
                            || construct.routine_decl_pending
                            || with_plsql_body_starts_here);
                    let starts_compound_trigger_body = starts_create_block
                        && compound_trigger_state.awaiting_outer_body_start()
                        && matches!(construct.create_object.as_deref(), Some("TRIGGER"));

                    // Handle block start - push to stack and increase indent
                    if should_treat_as_block_start {
                        block_stack.push(upper.clone());
                        indent_level += 1;
                        if upper == "DECLARE" || upper == "IF" {
                            in_plsql_block = true;
                        }
                    } else if upper == "BEGIN" {
                        let inside_declare = block_stack
                            .last()
                            .is_some_and(|s| s == "DECLARE" || s == "PACKAGE_BODY");
                        if inside_declare {
                            // DECLARE ... BEGIN - same block depth.
                            // PACKAGE BODY initialization BEGIN is also same depth as PACKAGE_BODY.
                            if block_stack.last().is_some_and(|s| s == "DECLARE") {
                                block_stack.pop();
                                block_stack.push("BEGIN".to_string());
                            }
                            // indent_level stays the same for both cases
                        } else {
                            // Standalone BEGIN block
                            block_stack.push("BEGIN".to_string());
                            indent_level += 1;
                        }
                        in_plsql_block = true;
                    } else if upper == "LOOP" {
                        block_stack.push("LOOP".to_string());
                        indent_level += 1;
                    } else if upper == "REPEAT" {
                        block_stack.push("REPEAT".to_string());
                        indent_level += 1;
                        in_plsql_block = true;
                    } else if upper == "CASE" {
                        block_stack.push("CASE".to_string());
                        if in_plsql_block && current_clause.is_none() {
                            case_branch_started.push(false);
                        }
                        indent_level += 1;
                    } else if starts_create_block {
                        // Treat AS/IS in CREATE PACKAGE/PROC/FUNC/TYPE/TRIGGER and package-body routines as declaration section start
                        let is_package_body =
                            matches!(construct.create_object.as_deref(), Some("PACKAGE_BODY"));
                        let is_trigger_body =
                            matches!(construct.create_object.as_deref(), Some("TRIGGER"));
                        if starts_compound_trigger_body {
                            block_stack.push("COMPOUND_TRIGGER".to_string());
                            compound_trigger_state.enter_outer_body();
                        } else if is_package_body {
                            block_stack.push("PACKAGE_BODY".to_string());
                        } else {
                            block_stack.push("DECLARE".to_string());
                        }
                        indent_level += 1;
                        in_plsql_block = true;
                        if is_trigger_body {
                            trigger_header_state.clear();
                        }
                        construct.create_object = None;
                        construct.routine_decl_pending = false;
                        newline_with(
                            &mut out,
                            indent_level,
                            0,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    }

                    if upper == "DECLARE" || upper == "BEGIN" {
                        if upper == "BEGIN" {
                            trigger_header_state.clear();
                        }
                        compound_trigger_state.begin_timing_point_body();
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            0,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    }

                    if newline_after_keyword {
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            newline_after_keyword_extra,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    }

                    if upper == "BETWEEN" {
                        between_pending = true;
                    } else if upper == "AND" && between_pending {
                        between_pending = false;
                    }
                    if is_create_index_on {
                        construct.create_index_pending = false;
                    }
                    exit_condition_state.on_keyword(upper.as_str());

                    prev_word_upper = Some(upper);
                }
                SqlToken::String(literal) => {
                    ensure_indent(&mut out, &mut at_line_start, line_indent);
                    if needs_space {
                        out.push(' ');
                    }
                    out.push_str(literal);
                    needs_space = true;
                    if literal.contains('\n') {
                        at_line_start = literal.ends_with('\n');
                    }
                }
                SqlToken::Comment(comment) => {
                    let mut has_leading_newline = comment.starts_with('\n');
                    let raw_comment_body = if has_leading_newline {
                        &comment[1..]
                    } else {
                        comment.as_str()
                    };
                    let trimmed_comment = raw_comment_body.trim_end_matches('\n');
                    let is_block_comment =
                        trimmed_comment.starts_with("/*") && trimmed_comment.ends_with("*/");
                    let is_hint_comment = trimmed_comment.starts_with("/*+");
                    let hint_after_select =
                        is_hint_comment && matches!(prev_word_upper.as_deref(), Some("SELECT"));
                    if hint_after_select {
                        has_leading_newline = false;
                    }
                    let comment_body = if has_leading_newline {
                        &comment[1..]
                    } else {
                        raw_comment_body
                    };
                    let is_multiline_block_comment =
                        is_block_comment && comment_body.contains('\n');
                    let next_is_word_like = matches!(
                        tokens.get(idx + 1),
                        Some(SqlToken::Word(_) | SqlToken::String(_))
                    );
                    let in_select_list = matches!(current_clause.as_deref(), Some("SELECT"));
                    let in_set_clause = matches!(current_clause.as_deref(), Some("SET"));
                    let top_level_select_list =
                        in_select_list && suppress_comma_break_depth == 0 && paren_stack.is_empty();
                    let top_level_set_list =
                        in_set_clause && suppress_comma_break_depth == 0 && paren_stack.is_empty();
                    let active_list_layout = select_list_layout_state.has_active_indent();
                    let keeps_next_line_continuation =
                        Self::comment_keeps_next_line_continuation(tokens, idx);
                    let attachment = Self::classify_comment_attachment(
                        &out,
                        at_line_start,
                        has_leading_newline,
                        is_multiline_block_comment,
                    );
                    let comment_ends_source_line = comment_body.contains('\n');
                    let attached_previous_keeps_inline = matches!(
                        attachment,
                        CommentAttachment::Previous
                    ) && !(comment_ends_source_line
                        && matches!(next_non_comment, Some(SqlToken::Symbol(s)) if s == ","));
                    if (top_level_select_list || top_level_set_list)
                        && !has_leading_newline
                        && !is_hint_comment
                        && !attached_previous_keeps_inline
                    {
                        force_select_list_newline(&mut out, &mut select_list_layout_state);
                    }

                    if is_multiline_block_comment
                        && !at_line_start
                        && !matches!(attachment, CommentAttachment::Previous)
                    {
                        newline_with(
                            &mut out,
                            0,
                            0,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    }

                    if matches!(
                        attachment,
                        CommentAttachment::Next | CommentAttachment::Block
                    ) && has_leading_newline
                        && !at_line_start
                    {
                        trim_trailing_space(&mut out);
                        newline_with(
                            &mut out,
                            base_indent(indent_level, open_cursor_state),
                            0,
                            &mut at_line_start,
                            &mut needs_space,
                            &mut line_indent,
                        );
                    } else if matches!(attachment, CommentAttachment::Previous) && !at_line_start {
                        trim_trailing_space(&mut out);
                        if !out.ends_with('\n') {
                            out.push(' ');
                        }
                    }

                    let comment_starts_line = at_line_start;
                    if comment_starts_line {
                        let base = base_indent(indent_level, open_cursor_state);
                        let paren_extra = if suppress_comma_break_depth > 0 { 1 } else { 0 };
                        let in_column_list = column_list_stack.last().copied().unwrap_or(false);
                        let current_list_indent = active_list_indent(
                            indent_level,
                            open_cursor_state,
                            select_list_layout_state,
                            current_clause.as_deref(),
                            construct.merge_active,
                            in_column_list,
                        );
                        let next_is_comma = matches!(
                            next_non_comment,
                            Some(SqlToken::Symbol(s)) if s == ","
                        );
                        let next_is_condition_keyword =
                            if let Some(SqlToken::Word(w)) = next_non_comment {
                                let upper = w.to_ascii_uppercase();
                                condition_keywords.contains(&upper.as_str())
                                    && !(upper == "AND" && between_pending)
                            } else {
                                false
                            };
                        let next_is_else = matches!(
                            next_non_comment,
                            Some(SqlToken::Word(w))
                                if w.eq_ignore_ascii_case("ELSE")
                                    || w.eq_ignore_ascii_case("ELSIF")
                                    || w.eq_ignore_ascii_case("EXCEPTION")
                        );
                        let in_confirmed_list = in_set_clause
                            || in_column_list
                            || ((in_select_list || active_list_layout)
                                && select_list_layout_state.is_multiline())
                            || next_is_comma
                            || next_is_condition_keyword;
                        if next_is_else {
                            // Align comment with ELSE/ELSIF/EXCEPTION which
                            // renders at one level below the current body in
                            // IF blocks, or at base+1 in CASE blocks.
                            // EXCEPTION drops to the enclosing BEGIN level
                            // just like ELSE does in IF blocks.
                            let in_if_block = block_stack.last().is_some_and(|s| s == "IF");
                            let next_is_exception = matches!(
                                next_non_comment,
                                Some(SqlToken::Word(w))
                                    if w.eq_ignore_ascii_case("EXCEPTION")
                            );
                            line_indent = if next_is_exception {
                                // EXCEPTION always drops to the enclosing
                                // BEGIN level regardless of block type.
                                base_indent(indent_level.saturating_sub(1), open_cursor_state)
                            } else if in_if_block {
                                base_indent(indent_level.saturating_sub(1), open_cursor_state)
                            } else {
                                base + 1 + paren_extra
                            };
                        } else if next_is_condition_keyword {
                            // Condition keywords (AND/OR/WHEN/ON) are
                            // indented at base+1, matching the token
                            // handler's paren_extra offset.
                            line_indent = base + 1 + paren_extra;
                        } else if in_confirmed_list {
                            line_indent = current_list_indent;
                        } else if line_indent == 0 {
                            line_indent = base;
                        }
                        ensure_indent(&mut out, &mut at_line_start, line_indent);
                    }

                    let rendered_comment_body = if matches!(attachment, CommentAttachment::Previous)
                        && !comment_starts_line
                        && comment_body.trim_start().starts_with("--")
                    {
                        comment_body.trim_start()
                    } else {
                        comment_body
                    };

                    out.push_str(rendered_comment_body);

                    needs_space = true;
                    if is_multiline_block_comment {
                        at_line_start = true;
                        needs_space = false;
                        if !out.ends_with('\n') {
                            out.push('\n');
                        }
                        let in_column_list = column_list_stack.last().copied().unwrap_or(false);
                        if in_select_list
                            || in_set_clause
                            || active_list_layout
                            || in_column_list
                            || hint_after_select
                        {
                            let list_indent = active_list_indent(
                                indent_level,
                                open_cursor_state,
                                select_list_layout_state,
                                current_clause.as_deref(),
                                construct.merge_active,
                                in_column_list,
                            );
                            line_indent = list_indent;
                            if hint_after_select {
                                select_list_layout_state = SelectListLayoutState::Multiline {
                                    indent: list_indent,
                                    hanging_indent_spaces: None,
                                };
                            }
                        } else {
                            line_indent = base_indent(indent_level, open_cursor_state);
                        }
                    } else if comment_body.ends_with('\n') || comment_body.contains('\n') {
                        at_line_start = true;
                        needs_space = false;
                        if keeps_next_line_continuation {
                            let in_column_list =
                                column_list_stack.last().copied().unwrap_or(false);
                            let continuation_indent = if in_select_list
                                || in_set_clause
                                || active_list_layout
                                || in_column_list
                                || hint_after_select
                            {
                                active_list_indent(
                                    indent_level,
                                    open_cursor_state,
                                    select_list_layout_state,
                                    current_clause.as_deref(),
                                    construct.merge_active,
                                    in_column_list,
                                )
                            } else {
                                line_indent.max(
                                    base_indent(indent_level, open_cursor_state)
                                        .saturating_add(usize::from(
                                            current_clause.is_some()
                                                || suppress_comma_break_depth > 0,
                                        )),
                                )
                            };
                            inline_comment_continuation_state =
                                InlineCommentContinuationState::Operand {
                                    indent: continuation_indent,
                                };
                        } else {
                            let in_column_list = column_list_stack.last().copied().unwrap_or(false);
                            if in_select_list
                                || in_set_clause
                                || active_list_layout
                                || in_column_list
                                || hint_after_select
                            {
                                let list_indent = active_list_indent(
                                    indent_level,
                                    open_cursor_state,
                                    select_list_layout_state,
                                    current_clause.as_deref(),
                                    construct.merge_active,
                                    in_column_list,
                                );
                                line_indent = list_indent;
                                if (in_select_list || in_set_clause)
                                    && !select_list_layout_state.is_multiline()
                                    && comment_starts_line
                                {
                                    select_list_layout_state = SelectListLayoutState::Multiline {
                                        indent: list_indent,
                                        hanging_indent_spaces: None,
                                    };
                                }
                            }
                        }
                    } else if is_block_comment && next_is_word_like {
                        let keep_inline_alias_comment = matches!(
                            (prev_word_upper.as_deref(), tokens.get(idx + 1)),
                            (Some("AS" | "IS"), Some(SqlToken::Word(_)))
                        );
                        if !keep_inline_alias_comment {
                            if keeps_next_line_continuation {
                                let in_column_list =
                                    column_list_stack.last().copied().unwrap_or(false);
                                let continuation_indent = if in_select_list
                                    || in_set_clause
                                    || active_list_layout
                                    || in_column_list
                                    || hint_after_select
                                {
                                    active_list_indent(
                                        indent_level,
                                        open_cursor_state,
                                        select_list_layout_state,
                                        current_clause.as_deref(),
                                        construct.merge_active,
                                        in_column_list,
                                    )
                                } else {
                                    line_indent.max(
                                        base_indent(indent_level, open_cursor_state)
                                            .saturating_add(usize::from(
                                                current_clause.is_some()
                                                    || suppress_comma_break_depth > 0,
                                            )),
                                    )
                                };
                                newline_with(
                                    &mut out,
                                    0,
                                    0,
                                    &mut at_line_start,
                                    &mut needs_space,
                                    &mut line_indent,
                                );
                                inline_comment_continuation_state =
                                    InlineCommentContinuationState::Operand {
                                        indent: continuation_indent,
                                    };
                            } else {
                                let in_column_list =
                                    column_list_stack.last().copied().unwrap_or(false);
                                let list_extra = if in_select_list
                                    || in_set_clause
                                    || active_list_layout
                                    || in_column_list
                                    || hint_after_select
                                {
                                    active_list_indent(
                                        indent_level,
                                        open_cursor_state,
                                        select_list_layout_state,
                                        current_clause.as_deref(),
                                        construct.merge_active,
                                        in_column_list,
                                    )
                                    .saturating_sub(base_indent(indent_level, open_cursor_state))
                                } else {
                                    0
                                };
                                newline_with(
                                    &mut out,
                                    base_indent(indent_level, open_cursor_state),
                                    list_extra,
                                    &mut at_line_start,
                                    &mut needs_space,
                                    &mut line_indent,
                                );
                            }
                            if hint_after_select {
                                select_list_layout_state = SelectListLayoutState::Multiline {
                                    indent: base_indent(indent_level, open_cursor_state) + 1,
                                    hanging_indent_spaces: None,
                                };
                            }
                        }
                    } else if comment_starts_line {
                    }
                }
                SqlToken::Symbol(sym) => {
                    match sym.as_str() {
                        "," => {
                            with_cte_state.on_separator();
                            let next_is_inline_comment = matches!(
                                tokens.get(idx + 1),
                                Some(SqlToken::Comment(comment))
                                    if !comment.starts_with('\n') && comment.contains('\n')
                            );
                            let next_is_newline_attached_comment = matches!(
                                tokens.get(idx + 1),
                                Some(SqlToken::Comment(comment)) if comment.starts_with('\n')
                            );
                            if statement_has_with_clause
                                && matches!(current_clause.as_deref(), Some("SELECT"))
                                && !open_cursor_state.in_select()
                                && suppress_comma_break_depth == 0
                            {
                                force_select_list_newline(&mut out, &mut select_list_layout_state);
                            }
                            trim_trailing_space(&mut out);
                            if out.ends_with('\n') || at_line_start {
                                let in_column_list =
                                    column_list_stack.last().copied().unwrap_or(false);
                                if line_indent == 0
                                    && (matches!(current_clause.as_deref(), Some("SELECT" | "SET"))
                                        || select_list_layout_state.has_active_indent()
                                        || in_column_list
                                        || matches!(
                                            tokens.get(idx.saturating_sub(1)),
                                            Some(SqlToken::Comment(comment))
                                                if comment.trim_start().starts_with("--")
                                        ))
                                {
                                    line_indent = active_list_indent(
                                        indent_level,
                                        open_cursor_state,
                                        select_list_layout_state,
                                        current_clause.as_deref(),
                                        construct.merge_active,
                                        in_column_list,
                                    );
                                }
                                ensure_indent(&mut out, &mut at_line_start, line_indent);
                            }
                            out.push(',');
                            between_pending = false;
                            let is_with_cte_separator = with_cte_state.can_close_on_select();
                            if column_list_stack.last().copied().unwrap_or(false)
                                || is_with_cte_separator
                            {
                                newline_with(
                                    &mut out,
                                    base_indent(indent_level, open_cursor_state),
                                    0,
                                    &mut at_line_start,
                                    &mut needs_space,
                                    &mut line_indent,
                                );
                            } else if next_is_inline_comment {
                                needs_space = true;
                                if matches!(current_clause.as_deref(), Some("SELECT")) {
                                    let select_list_indent =
                                        base_indent(indent_level, open_cursor_state) + 1;
                                    select_list_layout_state = SelectListLayoutState::Multiline {
                                        indent: select_list_indent,
                                        hanging_indent_spaces: current_query_has_apply.then_some(
                                            select_list_layout_state
                                                .hanging_indent_spaces(&out, select_list_indent),
                                        ),
                                    };
                                }
                            } else if suppress_comma_break_depth == 0
                                && !construct.execute_immediate_active
                                && !construct.grant_revoke_active
                                && !trigger_header_state.is_active()
                            {
                                let comma_extra_indent =
                                    if (matches!(current_clause.as_deref(), Some("SET"))
                                        && construct.merge_active)
                                        || (matches!(current_clause.as_deref(), Some("SELECT"))
                                            && construct.cursor_sql_active)
                                    {
                                        0
                                    } else {
                                        1
                                    };
                                if current_query_has_apply
                                    && matches!(current_clause.as_deref(), Some("SELECT"))
                                {
                                    // The select list is already multiline after the first comma.
                                    let select_list_indent =
                                        base_indent(indent_level, open_cursor_state) + 1;
                                    let hanging_indent_spaces = select_list_layout_state
                                        .hanging_indent_spaces(&out, select_list_indent);
                                    newline_with_spaces(
                                        &mut out,
                                        hanging_indent_spaces,
                                        &mut at_line_start,
                                        &mut needs_space,
                                        &mut line_indent,
                                    );
                                    select_list_layout_state = SelectListLayoutState::Multiline {
                                        indent: select_list_indent,
                                        hanging_indent_spaces: Some(hanging_indent_spaces),
                                    };
                                } else {
                                    newline_with(
                                        &mut out,
                                        base_indent(indent_level, open_cursor_state),
                                        comma_extra_indent,
                                        &mut at_line_start,
                                        &mut needs_space,
                                        &mut line_indent,
                                    );
                                }
                            } else {
                                if !next_is_newline_attached_comment {
                                    out.push(' ');
                                }
                                needs_space = false;
                            }
                        }
                        ";" => {
                            with_cte_state.on_separator();
                            let keep_tight_top_level_spacing =
                                with_cte_state.keeps_top_level_semicolon_inside_with_definitions();
                            trim_trailing_space(&mut out);
                            out.push(';');
                            current_clause = None;
                            select_list_layout_state.clear();
                            open_cursor_state = OpenCursorFormatState::None;
                            open_for_select_stack.clear();
                            between_pending = false;
                            trigger_header_state.clear();
                            exit_condition_state.clear();
                            construct.execute_immediate_active = false;
                            construct.cursor_decl_pending = false;
                            construct.create_index_pending = false;
                            construct.create_sequence_active = false;
                            construct.create_synonym_active = false;
                            construct.grant_revoke_active = false;
                            construct.comment_on_active = false;
                            construct.returning_active = false;
                            construct.fetch_active = false;
                            construct.bulk_collect_active = false;
                            construct.insert_all_active = false;
                            construct.referential_action_pending = false;
                            construct.referential_on_active = false;
                            if construct.merge_when_branch_active {
                                indent_level = indent_level.saturating_sub(1);
                                construct.merge_when_branch_active = false;
                            }
                            construct.merge_active = false;
                            if construct.cursor_sql_active {
                                indent_level = indent_level.saturating_sub(1);
                                construct.cursor_sql_active = false;
                            }
                            if construct.forall_body_active {
                                indent_level = indent_level.saturating_sub(1);
                                construct.forall_body_active = false;
                            }
                            if pending_package_member_separator
                                && (next_word_is("PROCEDURE") || next_word_is("FUNCTION"))
                            {
                                out.push_str("\n\n");
                            }
                            pending_package_member_separator = false;
                            construct.routine_decl_pending = false;
                            let should_reset_paren_tracking =
                                indent_level == 0 || block_stack.is_empty();
                            if should_reset_paren_tracking {
                                suppress_comma_break_depth = 0;
                                paren_stack.clear();
                                paren_clause_restore_stack.clear();
                                column_list_stack.clear();
                                paren_indent_increase_stack.clear();
                                select_list_break_state.clear();
                                compound_trigger_state.clear();
                            }
                            let next_is_inline_line_comment = matches!(
                                tokens.get(idx + 1),
                                Some(SqlToken::Comment(comment))
                                    if !comment.starts_with('\n')
                                        && comment.trim_start().starts_with("--")
                            );
                            if !next_is_inline_line_comment {
                                newline_with(
                                    &mut out,
                                    indent_level,
                                    0,
                                    &mut at_line_start,
                                    &mut needs_space,
                                    &mut line_indent,
                                );
                                if indent_level == 0 && !keep_tight_top_level_spacing {
                                    out.push('\n');
                                    at_line_start = true;
                                    needs_space = false;
                                }
                            }
                        }
                        "(" => {
                            with_cte_state.on_open_paren();
                            let is_query_paren =
                                next_word.is_some_and(crate::sql_text::is_subquery_head_keyword);
                            if Self::paren_starts_first_clause_list_item(
                                current_clause.as_deref(),
                                prev_word_upper.as_deref(),
                                is_query_paren,
                            ) {
                                newline_with(
                                    &mut out,
                                    base_indent(indent_level, open_cursor_state),
                                    1,
                                    &mut at_line_start,
                                    &mut needs_space,
                                    &mut line_indent,
                                );
                            }

                            ensure_indent(&mut out, &mut at_line_start, line_indent);
                            let is_analytic_over_paren = Self::paren_opens_analytic_layout(
                                current_clause.as_deref(),
                                prev_word_upper.as_deref(),
                            );
                            let is_multiline_clause_paren = matches!(
                                prev_word_upper.as_deref(),
                                Some("MATCH_RECOGNIZE" | "PIVOT" | "UNPIVOT" | "WINDOW")
                            ) || is_analytic_over_paren;
                            let is_subquery = is_query_paren || is_multiline_clause_paren;
                            let keeps_aggregate_call_tight = statement_has_apply
                                && matches!(
                                    prev_word_upper.as_deref(),
                                    Some("AVG" | "COUNT" | "MAX" | "MIN")
                                );
                            if needs_space && !keeps_aggregate_call_tight {
                                out.push(' ');
                            }
                            out.push('(');
                            let is_column_list = Self::paren_opens_structured_column_list(
                                prev_word_upper.as_deref(),
                                construct.create_table_paren_expected,
                            );
                            construct.create_table_paren_expected = false;
                            paren_stack.push(is_subquery);
                            if matches!(prev_word_upper.as_deref(), Some("MATCH_RECOGNIZE")) {
                                construct.match_recognize_paren_depth = Some(paren_stack.len());
                            }
                            if is_analytic_over_paren {
                                construct.analytic_over_paren_depth = Some(paren_stack.len());
                            }
                            paren_clause_restore_stack.push(if is_subquery {
                                current_clause.clone()
                            } else {
                                None
                            });
                            column_list_stack.push(is_column_list);
                            let indent_increase = if is_subquery || is_column_list { 1 } else { 0 };
                            paren_indent_increase_stack.push(indent_increase);
                            if indent_increase > 0 {
                                indent_level += indent_increase;
                                let keeps_inline_comment_after_open_paren = matches!(
                                    tokens.get(idx + 1),
                                    Some(SqlToken::Comment(comment))
                                        if !comment.starts_with('\n') && comment.contains('\n')
                                );
                                if keeps_inline_comment_after_open_paren {
                                    line_indent = base_indent(indent_level, open_cursor_state);
                                } else {
                                    newline_with(
                                        &mut out,
                                        base_indent(indent_level, open_cursor_state),
                                        0,
                                        &mut at_line_start,
                                        &mut needs_space,
                                        &mut line_indent,
                                    );
                                }
                            } else {
                                suppress_comma_break_depth += 1;
                            }
                            needs_space = false;
                        }
                        ")" => {
                            with_cte_state.on_close_paren();
                            trim_trailing_space(&mut out);
                            let was_subquery = paren_stack.pop().unwrap_or(false);
                            let restore_clause = paren_clause_restore_stack.pop().unwrap_or(None);
                            let was_column_list = column_list_stack.pop().unwrap_or(false);
                            let indent_increase = paren_indent_increase_stack.pop().unwrap_or(0);
                            let close_case_paren_on_newline = !was_subquery
                                && !was_column_list
                                && suppress_comma_break_depth > 0
                                && out.trim_end().ends_with("END");
                            if was_subquery || was_column_list {
                                if indent_level > 0 && indent_increase > 0 {
                                    indent_level = indent_level.saturating_sub(indent_increase);
                                }
                                newline_with(
                                    &mut out,
                                    base_indent(indent_level, open_cursor_state),
                                    indent_increase.saturating_sub(1),
                                    &mut at_line_start,
                                    &mut needs_space,
                                    &mut line_indent,
                                );
                                ensure_indent(&mut out, &mut at_line_start, line_indent);
                            } else {
                                suppress_comma_break_depth =
                                    suppress_comma_break_depth.saturating_sub(1);
                            }
                            if close_case_paren_on_newline {
                                let closes_plsql_condition_terminator =
                                    Self::tokens_continue_plsql_condition_terminator(tokens, idx);
                                let close_case_extra_indent = if closes_plsql_condition_terminator {
                                    0
                                } else {
                                    usize::from(!open_cursor_state.in_select())
                                };
                                let close_case_indent_level = if closes_plsql_condition_terminator
                                    || next_word_is("ELSE")
                                    || next_word_is("WHEN")
                                {
                                    indent_level.saturating_sub(1)
                                } else {
                                    indent_level
                                };
                                newline_with(
                                    &mut out,
                                    close_case_indent_level,
                                    close_case_extra_indent,
                                    &mut at_line_start,
                                    &mut needs_space,
                                    &mut line_indent,
                                );
                                ensure_indent(&mut out, &mut at_line_start, line_indent);
                            }
                            if open_cursor_state
                                .select_depth()
                                .is_some_and(|depth| paren_stack.len() < depth)
                            {
                                open_cursor_state = open_for_select_stack
                                    .pop()
                                    .unwrap_or(OpenCursorFormatState::None);
                            }
                            if was_subquery {
                                current_clause = restore_clause;
                            }
                            if construct
                                .match_recognize_paren_depth
                                .is_some_and(|depth| paren_stack.len() < depth)
                            {
                                construct.match_recognize_paren_depth = None;
                            }
                            if construct
                                .analytic_over_paren_depth
                                .is_some_and(|depth| paren_stack.len() < depth)
                            {
                                construct.analytic_over_paren_depth = None;
                            }
                            ensure_indent(&mut out, &mut at_line_start, line_indent);
                            out.push(')');
                            needs_space = true;
                        }
                        "." => {
                            trim_trailing_space(&mut out);
                            out.push('.');
                            needs_space = false;
                        }
                        _ => {
                            ensure_indent(&mut out, &mut at_line_start, line_indent);
                            let is_plsql_attribute_prefix =
                                Self::is_plsql_attribute_prefix(sym, tokens.get(idx + 1));
                            // Don't add space between consecutive ampersands (&&var substitution)
                            if needs_space
                                && !(sym == "&" && out.ends_with('&'))
                                && !is_plsql_attribute_prefix
                            {
                                out.push(' ');
                            }
                            out.push_str(sym);
                            // For bind variables (:name) and assignment (:=), don't add space after colon
                            // Check if this is ":" and next token is a Word (bind variable)
                            let is_bind_var_colon = sym == ":"
                                && tokens
                                    .get(idx + 1)
                                    .is_some_and(|t| matches!(t, SqlToken::Word(_)));
                            // For substitution variables (&var, &&var), don't add space after &
                            let is_ampersand_prefix = sym == "&"
                                && tokens.get(idx + 1).is_some_and(|t| {
                                    matches!(t, SqlToken::Word(_))
                                        || matches!(t, SqlToken::Symbol(s) if s == "&")
                                });
                            needs_space = !is_bind_var_colon
                                && !is_ampersand_prefix
                                && !is_plsql_attribute_prefix;
                        }
                    }
                }
            }

            idx += 1;
        }

        Self::apply_parser_depth_indentation(out.trim_end())
    }

    fn apply_parser_depth_indentation(formatted: &str) -> String {
        if formatted.is_empty() {
            return formatted.to_string();
        }

        let line_count = formatted.lines().count();
        let contexts = QueryExecutor::auto_format_line_contexts(formatted);
        if contexts.len() != line_count {
            return formatted.to_string();
        }

        let multiline_string_continuation_lines =
            Self::multiline_string_continuation_lines(formatted, line_count);
        let mut layouts =
            Self::build_line_layouts(formatted, &contexts, &multiline_string_continuation_lines);
        let (previous_code_indices, next_code_indices) = Self::line_layout_code_neighbors(&layouts);

        Self::resolve_code_line_layouts(&mut layouts, &previous_code_indices, &next_code_indices);
        Self::align_case_close_paren_layouts(
            &mut layouts,
            &previous_code_indices,
            &next_code_indices,
        );
        Self::resolve_non_code_line_layouts(
            &mut layouts,
            &previous_code_indices,
            &next_code_indices,
        );

        Self::render_line_layouts(&layouts)
    }

    fn build_line_layouts<'a>(
        formatted: &'a str,
        contexts: &[AutoFormatLineContext],
        multiline_string_continuation_lines: &[bool],
    ) -> Vec<LineLayout<'a>> {
        let lines: Vec<&'a str> = formatted.lines().collect();
        let mut layouts = Vec::with_capacity(lines.len());
        let mut in_block_comment = false;

        for (idx, line) in lines.iter().enumerate() {
            let raw = *line;
            let trimmed = raw.trim_start();
            let continuation_line = multiline_string_continuation_lines
                .get(idx)
                .copied()
                .unwrap_or(false);
            let (kind, preserve_raw) = if continuation_line {
                (LineLayoutKind::Verbatim, true)
            } else {
                let was_in_block_comment = in_block_comment;
                let is_comment_only = if was_in_block_comment && trimmed.is_empty() {
                    true
                } else if trimmed.is_empty() {
                    false
                } else {
                    Self::line_is_comment_only_with_block_state(raw, &mut in_block_comment)
                };
                if !is_comment_only && !trimmed.is_empty() {
                    crate::sql_text::update_block_comment_state(trimmed, &mut in_block_comment);
                }

                if is_comment_only {
                    (LineLayoutKind::CommentOnly, was_in_block_comment)
                } else if trimmed.is_empty() {
                    (LineLayoutKind::Blank, false)
                } else if trimmed == "," {
                    (LineLayoutKind::CommaOnly, false)
                } else {
                    (LineLayoutKind::Code, false)
                }
            };

            let leading_indent_columns = Self::leading_indent_columns(raw);

            layouts.push(LineLayout {
                raw,
                trimmed,
                kind,
                preserve_raw,
                parser_depth: contexts.get(idx).map(|ctx| ctx.parser_depth).unwrap_or(0),
                auto_depth: contexts.get(idx).map(|ctx| ctx.auto_depth).unwrap_or(0),
                query_role: contexts
                    .get(idx)
                    .map(|ctx| ctx.query_role)
                    .unwrap_or(AutoFormatQueryRole::None),
                query_base_depth: contexts.get(idx).and_then(|ctx| ctx.query_base_depth),
                starts_query_frame: contexts
                    .get(idx)
                    .map(|ctx| ctx.starts_query_frame)
                    .unwrap_or(false),
                next_query_head_depth: contexts.get(idx).and_then(|ctx| ctx.next_query_head_depth),
                condition_header_line: contexts.get(idx).and_then(|ctx| ctx.condition_header_line),
                condition_role: contexts
                    .get(idx)
                    .map(|ctx| ctx.condition_role)
                    .unwrap_or(AutoFormatConditionRole::None),
                existing_indent: leading_indent_columns / 4,
                existing_indent_spaces: leading_indent_columns,
                final_depth: 0,
                anchor_group: None,
                dml_case_expression_close_depth: None,
            });
        }

        layouts
    }

    fn line_layout_code_neighbors(
        layouts: &[LineLayout<'_>],
    ) -> (Vec<Option<usize>>, Vec<Option<usize>>) {
        let mut previous_code_indices = vec![None; layouts.len()];
        let mut last_code_idx = None;
        for (idx, layout) in layouts.iter().enumerate() {
            previous_code_indices[idx] = last_code_idx;
            if layout.kind == LineLayoutKind::Code {
                last_code_idx = Some(idx);
            }
        }

        let mut next_code_indices = vec![None; layouts.len()];
        let mut next_code_idx = None;
        for idx in (0..layouts.len()).rev() {
            next_code_indices[idx] = next_code_idx;
            if layouts[idx].kind == LineLayoutKind::Code {
                next_code_idx = Some(idx);
            }
        }

        (previous_code_indices, next_code_indices)
    }

    fn line_layout_preceding_comment_or_comma_run_has_comma(
        layouts: &[LineLayout<'_>],
        idx: usize,
    ) -> bool {
        let mut scan_idx = idx;
        let mut saw_comma = false;

        while scan_idx > 0 {
            scan_idx = scan_idx.saturating_sub(1);
            match layouts[scan_idx].kind {
                LineLayoutKind::CommentOnly => continue,
                LineLayoutKind::CommaOnly => {
                    saw_comma = true;
                }
                LineLayoutKind::Blank | LineLayoutKind::Verbatim | LineLayoutKind::Code => break,
            }
        }

        saw_comma
    }

    fn previous_dml_clause_starter_depth(
        layouts: &[LineLayout<'_>],
        idx: usize,
        target_depth: usize,
    ) -> Option<usize> {
        for prev_idx in (0..idx).rev() {
            if layouts[prev_idx].kind != LineLayoutKind::Code {
                continue;
            }

            let prev_upper = layouts[prev_idx].trimmed.to_ascii_uppercase();
            if Self::is_dml_clause_starter(&prev_upper)
                && layouts[prev_idx].final_depth == target_depth
            {
                return Some(target_depth);
            }
        }

        None
    }

    fn parenthesized_condition_header_depth(
        layouts: &[LineLayout<'_>],
        idx: usize,
    ) -> Option<usize> {
        layouts[idx]
            .condition_header_line
            .and_then(|header_idx| layouts.get(header_idx))
            .map(|header| header.final_depth)
    }

    fn line_has_clause_query_owner(trimmed_upper: &str) -> bool {
        trimmed_upper.starts_with("FROM (")
            || trimmed_upper.starts_with("USING (")
            || trimmed_upper.contains(" JOIN (")
    }

    fn line_has_from_item_query_owner(trimmed_upper: &str) -> bool {
        trimmed_upper.starts_with("LATERAL (") || trimmed_upper.contains(" APPLY (")
    }

    fn line_has_condition_query_owner(trimmed_upper: &str) -> bool {
        (trimmed_upper.ends_with(" IN (")
            && !crate::sql_text::starts_with_keyword_token(trimmed_upper, "FOR"))
            || trimmed_upper.ends_with(" EXISTS (")
            || trimmed_upper.ends_with(" NOT EXISTS (")
    }

    fn line_starts_query_head(trimmed_upper: &str) -> bool {
        crate::sql_text::first_meaningful_word(trimmed_upper)
            .is_some_and(crate::sql_text::is_subquery_head_keyword)
    }

    fn line_has_inline_comment_after_case_terminator(line: &str) -> bool {
        let trimmed = line.trim_start();
        let trimmed_upper = trimmed.to_ascii_uppercase();
        Self::starts_with_case_terminator(&trimmed_upper)
            && (trimmed.contains("--") || trimmed.contains("/*"))
    }

    fn structural_query_head_origin(trimmed_upper: &str) -> QueryHeadLayoutOrigin {
        if Self::line_has_condition_query_owner(trimmed_upper) {
            QueryHeadLayoutOrigin::ConditionOwner
        } else if Self::line_has_from_item_query_owner(trimmed_upper) {
            QueryHeadLayoutOrigin::FromItemOwner
        } else if Self::line_has_clause_query_owner(trimmed_upper) {
            QueryHeadLayoutOrigin::ClauseOwner
        } else {
            QueryHeadLayoutOrigin::Other
        }
    }

    fn tokens_continue_expression_after_leading_closes(
        tokens: &[SqlToken],
        start_idx: usize,
    ) -> bool {
        tokens
            .iter()
            .skip(start_idx)
            .find(|token| !matches!(token, SqlToken::Comment(_)))
            .is_some_and(|token| match token {
                SqlToken::Symbol(sym) => {
                    matches!(
                        sym.as_str(),
                        "," | "+"
                            | "-"
                            | "*"
                            | "/"
                            | "%"
                            | "^"
                            | "="
                            | "<"
                            | ">"
                            | "<="
                            | ">="
                            | "<>"
                            | "!="
                            | "||"
                            | "|"
                    )
                }
                SqlToken::Word(word) => matches!(
                    word.to_ascii_uppercase().as_str(),
                    "AND" | "OR" | "IS" | "IN" | "LIKE" | "BETWEEN" | "NOT"
                ),
                _ => false,
            })
    }

    fn classify_paren_layout_frame_kind(
        tokens: &[SqlToken],
        open_idx: usize,
        next_code_trimmed: Option<&str>,
        is_multiline_clause_owner: bool,
        is_condition_query_owner: bool,
    ) -> ParenLayoutFrameKind {
        let next_non_comment = tokens
            .iter()
            .skip(open_idx.saturating_add(1))
            .find(|token| !matches!(token, SqlToken::Comment(_)));

        if let Some(SqlToken::Word(word)) = next_non_comment {
            if Self::line_starts_query_head(&word.to_ascii_uppercase()) {
                return if is_condition_query_owner {
                    ParenLayoutFrameKind::ConditionQuery
                } else {
                    ParenLayoutFrameKind::Query
                };
            }
        }

        if next_non_comment.is_none() {
            if next_code_trimmed
                .is_some_and(|next| Self::line_starts_query_head(&next.to_ascii_uppercase()))
            {
                return if is_condition_query_owner {
                    ParenLayoutFrameKind::ConditionQuery
                } else {
                    ParenLayoutFrameKind::Query
                };
            }
            if is_multiline_clause_owner {
                return ParenLayoutFrameKind::MultilineClause;
            }
        }

        ParenLayoutFrameKind::General
    }

    fn consume_leading_paren_layout_frames(
        tokens: &[SqlToken],
        paren_layout_frames: &mut Vec<ParenLayoutFrame>,
    ) -> (
        usize,
        Option<ParenLayoutFrame>,
        Option<ParenLayoutFrame>,
        bool,
    ) {
        let mut token_idx = 0usize;
        let mut saw_leading_close = false;
        let mut last_popped_non_multiline_frame = None;
        let mut last_popped_general_frame = None;

        loop {
            match tokens.get(token_idx) {
                Some(SqlToken::Comment(_)) => {
                    token_idx = token_idx.saturating_add(1);
                }
                Some(SqlToken::Symbol(sym)) if sym == ")" => {
                    saw_leading_close = true;
                    if let Some(frame) = paren_layout_frames.pop() {
                        if frame.kind != ParenLayoutFrameKind::MultilineClause {
                            last_popped_non_multiline_frame = Some(frame);
                        }
                        if frame.kind == ParenLayoutFrameKind::General {
                            last_popped_general_frame = Some(frame);
                        }
                    }
                    token_idx = token_idx.saturating_add(1);
                }
                _ => break,
            }
        }

        let continues_expression = saw_leading_close
            && Self::tokens_continue_expression_after_leading_closes(tokens, token_idx);

        (
            token_idx,
            last_popped_non_multiline_frame,
            last_popped_general_frame,
            continues_expression,
        )
    }

    fn update_paren_layout_frames_for_line(
        tokens: &[SqlToken],
        start_idx: usize,
        next_code_trimmed: Option<&str>,
        line_depth: usize,
        is_multiline_clause_owner: bool,
        is_condition_query_owner: bool,
        is_standalone_open_paren_owner: bool,
        paren_layout_frames: &mut Vec<ParenLayoutFrame>,
    ) {
        for token_idx in start_idx..tokens.len() {
            match &tokens[token_idx] {
                SqlToken::Comment(_) => {}
                SqlToken::Symbol(sym) if sym == "(" => {
                    let kind = Self::classify_paren_layout_frame_kind(
                        tokens,
                        token_idx,
                        next_code_trimmed,
                        is_multiline_clause_owner,
                        is_condition_query_owner,
                    );
                    // Each General `(` derives its depth from the deeper of:
                    //  - the previous General frame's continuation (for
                    //    consecutive parens like `(((` on the same line), or
                    //  - the current line's depth (for parens opened on a
                    //    deeper line, e.g. `AND (` where AND is already +1'd).
                    // This lets the frame stack naturally produce progressive
                    // depth without explicit counting.
                    let base_depth = if kind == ParenLayoutFrameKind::General {
                        paren_layout_frames
                            .iter()
                            .rev()
                            .find(|f| f.kind == ParenLayoutFrameKind::General)
                            .map(|f| f.continuation_depth.max(line_depth))
                            .unwrap_or(line_depth)
                    } else {
                        line_depth
                    };
                    let continuation_depth = match kind {
                        ParenLayoutFrameKind::General => {
                            if is_standalone_open_paren_owner {
                                base_depth
                            } else {
                                base_depth.saturating_add(1)
                            }
                        }
                        ParenLayoutFrameKind::Query
                        | ParenLayoutFrameKind::ConditionQuery
                        | ParenLayoutFrameKind::MultilineClause => line_depth,
                    };
                    let owner_depth = if kind == ParenLayoutFrameKind::General
                        && is_standalone_open_paren_owner
                    {
                        line_depth.saturating_sub(1)
                    } else {
                        line_depth
                    };
                    paren_layout_frames.push(ParenLayoutFrame {
                        kind,
                        owner_depth,
                        continuation_depth,
                        standalone_owner: kind == ParenLayoutFrameKind::General
                            && is_standalone_open_paren_owner,
                    });
                }
                SqlToken::Symbol(sym) if sym == ")" => {
                    let _ = paren_layout_frames.pop();
                }
                _ => {}
            }
        }
    }

    fn structural_next_query_head_depth(
        layout: &LineLayout<'_>,
        trimmed_upper: &str,
        resolved_query_base_depth: Option<usize>,
        current_condition_header_depth: Option<usize>,
        current_query_origin: Option<QueryHeadLayoutOrigin>,
    ) -> Option<usize> {
        if !Self::line_ends_with_open_paren_before_inline_comment(layout.trimmed) {
            return None;
        }

        if Self::line_has_condition_query_owner(trimmed_upper) {
            if let Some(header_depth) = current_condition_header_depth {
                Some(header_depth.saturating_add(1))
            } else if let Some(resolved_base_depth) =
                resolved_query_base_depth.or(layout.query_base_depth)
            {
                let nested_query_extra_depth = if current_query_origin
                    == Some(QueryHeadLayoutOrigin::ConditionOwner)
                    && layout
                        .query_base_depth
                        .is_some_and(|raw_base_depth| resolved_base_depth > raw_base_depth)
                {
                    1
                } else {
                    2
                };
                Some(resolved_base_depth.saturating_add(nested_query_extra_depth))
            } else {
                Some(layout.final_depth.saturating_add(1))
            }
        } else if Self::line_has_clause_query_owner(trimmed_upper) {
            if let Some(resolved_base_depth) = resolved_query_base_depth.or(layout.query_base_depth)
            {
                Some(resolved_base_depth.saturating_add(2))
            } else {
                Some(layout.final_depth.saturating_add(2))
            }
        } else if Self::line_has_from_item_query_owner(trimmed_upper) {
            Some(layout.final_depth.saturating_add(1))
        } else {
            Some(layout.final_depth.saturating_add(1))
        }
    }

    fn clause_starter_uses_general_paren_continuation(
        trimmed_upper: &str,
        starts_query_head: bool,
        active_general_paren_frame: Option<ParenLayoutFrame>,
    ) -> bool {
        if starts_query_head || active_general_paren_frame.is_none() {
            return false;
        }

        let starts_clause_keyword = Self::is_dml_clause_starter(trimmed_upper)
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "INTO");
        if !starts_clause_keyword {
            return false;
        }
        true
    }

    fn resolve_code_line_layouts(
        layouts: &mut [LineLayout<'_>],
        previous_code_indices: &[Option<usize>],
        next_code_indices: &[Option<usize>],
    ) {
        let mut in_dml_statement = false;
        let mut paren_case_expression_depth = 0usize;
        let mut pending_paren_case_closer_indent = false;
        let mut last_code_idx: Option<usize> = None;
        let mut next_anchor_group = 0usize;
        let mut dml_case_frames: Vec<DmlCaseLayoutFrame> = Vec::new();
        let mut dml_case_condition_frames: Vec<DmlCaseConditionLayoutFrame> = Vec::new();
        let mut pending_dml_case_expression_close_depth: Option<usize> = None;
        let mut pending_case_branch_body_depth: Option<usize> = None;
        let mut pending_query_head_depth: Option<usize> = None;
        let mut pending_query_head_origin: Option<QueryHeadLayoutOrigin> = None;
        let mut resolved_query_base_depths: Vec<ResolvedQueryBaseLayoutFrame> = Vec::new();
        let mut multiline_clause_frames: Vec<MultilineClauseLayoutFrame> = Vec::new();
        let mut paren_layout_frames: Vec<ParenLayoutFrame> = Vec::new();
        let mut prev_general_paren_frame_count: usize = 0;
        // Tracks the AND/OR depth while inside a JOIN ON/USING condition block.
        // Set when we resolve the indent for ON/USING in a join, cleared on new
        // clause or JOIN at the same parser depth level.  Used so that AND/OR
        // after a subquery close paren within the condition keeps the correct
        // (deeper-than-ON) indent.
        // Stores (and_depth, parser_depth_of_on_line).
        let mut join_on_condition_and_depth: Option<(usize, usize)> = None;

        for idx in 0..layouts.len() {
            if layouts[idx].kind != LineLayoutKind::Code {
                continue;
            }

            let trimmed = layouts[idx].trimmed;
            let depth = layouts[idx].parser_depth;
            let existing_indent = layouts[idx].existing_indent;
            let trimmed_upper = trimmed.to_ascii_uppercase();
            let pending_case_branch_body_depth_for_line = pending_case_branch_body_depth;
            let closing_query_frame_count = resolved_query_base_depths
                .iter()
                .rev()
                .take_while(|frame| depth < frame.start_parser_depth)
                .count();

            let previous_line_ends_with_open_paren = last_code_idx.is_some_and(|prev_idx| {
                Self::line_ends_with_open_paren_before_inline_comment(layouts[prev_idx].trimmed)
            });
            let previous_line_is_cte_definition_header = last_code_idx.is_some_and(|prev_idx| {
                Self::line_is_cte_definition_header(layouts[prev_idx].trimmed)
            });
            let previous_line_is_standalone_open_paren = last_code_idx.is_some_and(|prev_idx| {
                Self::line_is_standalone_open_paren_before_inline_comment(layouts[prev_idx].trimmed)
            });
            let current_line_is_standalone_open_paren =
                Self::line_is_standalone_open_paren_before_inline_comment(trimmed);
            let starts_paren_case_expression =
                crate::sql_text::starts_with_keyword_token(&trimmed_upper, "CASE")
                    && previous_line_ends_with_open_paren;
            if starts_paren_case_expression {
                paren_case_expression_depth += 1;
            }
            let in_paren_case_expression = paren_case_expression_depth > 0;
            let starts_dml = crate::sql_text::starts_with_keyword_token(&trimmed_upper, "SELECT")
                || crate::sql_text::starts_with_keyword_token(&trimmed_upper, "INSERT")
                || crate::sql_text::starts_with_keyword_token(&trimmed_upper, "UPDATE")
                || crate::sql_text::starts_with_keyword_token(&trimmed_upper, "DELETE")
                || crate::sql_text::starts_with_keyword_token(&trimmed_upper, "MERGE");
            let starts_subquery_head =
                starts_dml || crate::sql_text::starts_with_keyword_token(&trimmed_upper, "WITH");
            if starts_dml {
                in_dml_statement = true;
            }
            let paren_case_extra_indent = if !in_dml_statement
                && in_paren_case_expression
                && (crate::sql_text::starts_with_keyword_token(&trimmed_upper, "CASE")
                    || trimmed_upper.starts_with("WHEN ")
                    || trimmed_upper.starts_with("ELSE")
                    || crate::sql_text::starts_with_keyword_token(&trimmed_upper, "END"))
            {
                1
            } else {
                0
            };
            let previous_line_is_plain_end = last_code_idx.is_some_and(|prev_idx| {
                let prev_upper = layouts[prev_idx].trimmed.to_ascii_uppercase();
                Self::starts_with_plain_end(&prev_upper)
            });
            let previous_line_has_inline_comment_after_case_terminator =
                last_code_idx.is_some_and(|prev_idx| {
                    Self::line_has_inline_comment_after_case_terminator(layouts[prev_idx].trimmed)
                });
            let previous_line_starts_with_using_clause = last_code_idx.is_some_and(|prev_idx| {
                Self::line_starts_with_using_clause(layouts[prev_idx].trimmed)
            });
            let previous_line_ends_with_trailing_comma = last_code_idx.is_some_and(|prev_idx| {
                Self::line_ends_with_comma_before_inline_comment(layouts[prev_idx].trimmed)
                    || layouts[prev_idx].trimmed.trim_end().ends_with(',')
            });
            let previous_line_is_close_paren_with_trailing_comma =
                last_code_idx.is_some_and(|prev_idx| {
                    layouts[prev_idx].trimmed.starts_with(')')
                        && (Self::line_ends_with_comma_before_inline_comment(
                            layouts[prev_idx].trimmed,
                        ) || layouts[prev_idx].trimmed.trim_end().ends_with(','))
                });
            let previous_line_ends_with_assignment = last_code_idx.is_some_and(|prev_idx| {
                Self::line_ends_with_assignment_before_inline_comment(layouts[prev_idx].trimmed)
            });
            let previous_line_ends_with_then = last_code_idx.is_some_and(|prev_idx| {
                Self::line_ends_with_then_before_inline_comment(layouts[prev_idx].trimmed)
            });
            let previous_line_is_dml_clause_line = last_code_idx.is_some_and(|prev_idx| {
                let prev_upper = layouts[prev_idx].trimmed.to_ascii_uppercase();
                Self::is_dml_clause_starter(&prev_upper)
                    || crate::sql_text::starts_with_keyword_token(&prev_upper, "INTO")
            });
            let previous_line_is_dml_clause_starter = last_code_idx.is_some_and(|prev_idx| {
                let prev_upper = layouts[prev_idx].trimmed.to_ascii_uppercase();
                Self::is_dml_clause_starter(&prev_upper)
            });
            let previous_line_is_select_header = last_code_idx.is_some_and(|prev_idx| {
                let prev_upper = layouts[prev_idx].trimmed.to_ascii_uppercase();
                crate::sql_text::starts_with_keyword_token(&prev_upper, "SELECT")
            });
            let previous_line_is_order_by_clause = last_code_idx.is_some_and(|prev_idx| {
                let prev_upper = layouts[prev_idx].trimmed.to_ascii_uppercase();
                prev_upper.starts_with("ORDER BY")
            });
            let previous_code_is_cursor_header = last_code_idx.is_some_and(|prev_idx| {
                let cursor_upper = layouts[prev_idx].trimmed.to_ascii_uppercase();
                crate::sql_text::starts_with_keyword_token(&cursor_upper, "CURSOR")
                    && (cursor_upper.contains(" IS") || cursor_upper.contains(" AS"))
            });
            let previous_select_follows_cursor_header = last_code_idx
                .and_then(|prev_idx| previous_code_indices.get(prev_idx).copied().flatten())
                .is_some_and(|cursor_idx| {
                    let cursor_upper = layouts[cursor_idx].trimmed.to_ascii_uppercase();
                    crate::sql_text::starts_with_keyword_token(&cursor_upper, "CURSOR")
                        && (cursor_upper.contains(" IS") || cursor_upper.contains(" AS"))
                });
            let previous_line_has_trailing_unclosed_case = last_code_idx.is_some_and(|prev_idx| {
                Self::line_has_trailing_unclosed_case(layouts[prev_idx].trimmed)
            });
            let previous_line_is_case_header = last_code_idx.is_some_and(|prev_idx| {
                let prev_upper = layouts[prev_idx].trimmed.to_ascii_uppercase();
                crate::sql_text::starts_with_keyword_token(&prev_upper, "CASE")
            });
            let previous_line_is_else = last_code_idx.is_some_and(|prev_idx| {
                layouts[prev_idx]
                    .trimmed
                    .to_ascii_uppercase()
                    .trim()
                    .eq("ELSE")
            });
            let previous_line_ends_with_within = last_code_idx.is_some_and(|prev_idx| {
                layouts[prev_idx]
                    .trimmed
                    .to_ascii_uppercase()
                    .contains("WITHIN")
            });
            let previous_line_is_match_recognize_order_by = last_code_idx.is_some_and(|prev_idx| {
                let prev_upper = layouts[prev_idx].trimmed.to_ascii_uppercase();
                prev_upper.starts_with("ORDER BY")
                    && previous_code_indices[prev_idx].is_some_and(|anchor_idx| {
                        layouts[anchor_idx]
                            .trimmed
                            .to_ascii_uppercase()
                            .contains("MATCH_RECOGNIZE")
                            || Self::is_match_recognize_subclause(
                                &layouts[anchor_idx].trimmed.to_ascii_uppercase(),
                            )
                    })
            });
            let current_line_is_match_recognize_subclause =
                Self::is_match_recognize_subclause(&trimmed_upper)
                    && last_code_idx.is_some_and(|prev_idx| {
                        let prev_upper = layouts[prev_idx].trimmed.to_ascii_uppercase();
                        prev_upper.contains("MATCH_RECOGNIZE")
                            || Self::is_match_recognize_subclause(&prev_upper)
                    });
            let previous_line_has_unclosed_open_paren = last_code_idx.is_some_and(|prev_idx| {
                Self::line_has_unclosed_open_paren_before_inline_comment(layouts[prev_idx].trimmed)
            });
            let previous_line_is_forall_header = last_code_idx.is_some_and(|prev_idx| {
                let prev_upper = layouts[prev_idx].trimmed.to_ascii_uppercase();
                crate::sql_text::starts_with_keyword_token(&prev_upper, "FORALL")
            });
            let previous_line_is_condition_keyword = last_code_idx.is_some_and(|prev_idx| {
                let prev_upper = layouts[prev_idx].trimmed.to_ascii_uppercase();
                Self::starts_with_condition_keyword(&prev_upper)
            });
            let previous_line_is_join_condition_clause = last_code_idx.is_some_and(|prev_idx| {
                let prev_upper = layouts[prev_idx].trimmed.to_ascii_uppercase();
                crate::sql_text::starts_with_keyword_token(&prev_upper, "ON")
                    || crate::sql_text::starts_with_keyword_token(&prev_upper, "USING")
            });
            let previous_code_is_inline_merge_update_set = last_code_idx.is_some_and(|prev_idx| {
                let prev_upper = layouts[prev_idx].trimmed.to_ascii_uppercase();
                prev_upper.starts_with("UPDATE SET ") || prev_upper.eq("UPDATE SET")
            });
            let previous_line_starts_with_close_paren =
                last_code_idx.is_some_and(|prev_idx| layouts[prev_idx].trimmed.starts_with(')'));
            let next_code_trimmed =
                next_code_indices[idx].map(|next_idx| layouts[next_idx].trimmed);
            let line_tokens = Self::tokenize_sql(trimmed);
            let next_line_is_named_plain_end = next_code_trimmed.is_some_and(|next| {
                let next_upper = next.to_ascii_uppercase();
                Self::starts_with_plain_end(&next_upper) && !Self::starts_with_bare_end(&next_upper)
            });
            let next_line_is_case_branch = next_code_trimmed.is_some_and(|next| {
                let next_upper = next.to_ascii_uppercase();
                next_upper.starts_with("WHEN ")
                    || (next_upper.starts_with("ELSE")
                        && !next_upper.starts_with("ELSIF")
                        && !next_upper.starts_with("ELSEIF"))
            });
            let next_line_existing_indent =
                next_code_indices[idx].map(|next_idx| layouts[next_idx].existing_indent);
            let force_end_suffix_depth = Self::starts_with_end_suffix_terminator(&trimmed_upper)
                && !previous_line_is_plain_end
                && !next_line_is_named_plain_end;
            // Trigger header WHEN (e.g. WHEN (n.sal > 0)) appears at parser_depth 0
            // but is indented by phase 1 to align with other trigger header clauses.
            // Detect this by checking if WHEN at depth 0 follows a trigger header line
            // (previous line has higher existing_indent, indicating trigger header context).
            let is_trigger_header_when = trimmed_upper.starts_with("WHEN ")
                && depth == 0
                && existing_indent > 0
                && last_code_idx.is_some_and(|prev_idx| {
                    let prev_trimmed_upper = layouts[prev_idx].trimmed.to_ascii_uppercase();
                    prev_trimmed_upper.ends_with("ROW")
                        || prev_trimmed_upper.starts_with("REFERENCING ")
                        || prev_trimmed_upper.starts_with("FOR EACH ROW")
                        || (layouts[prev_idx].parser_depth == 0
                            && layouts[prev_idx].existing_indent > 0)
                });
            let force_block_depth = !in_dml_statement
                && !is_trigger_header_when
                && (trimmed_upper.starts_with("EXCEPTION")
                    || trimmed_upper.starts_with("WHEN ")
                    || trimmed_upper.starts_with("ELSE")
                    || trimmed_upper.starts_with("ELSIF")
                    || trimmed_upper.starts_with("ELSEIF")
                    || trimmed_upper.starts_with("CASE")
                    || Self::starts_with_bare_end(&trimmed_upper)
                    || force_end_suffix_depth);

            let parser_depth = depth + paren_case_extra_indent;
            let starts_with_close_paren = trimmed.starts_with(')');
            let current_line_is_parenthesized_condition =
                layouts[idx].condition_header_line.is_some();
            let current_line_is_parenthesized_condition_header =
                layouts[idx].condition_role == AutoFormatConditionRole::Header;
            let current_line_is_parenthesized_condition_close =
                layouts[idx].condition_role == AutoFormatConditionRole::Closer;
            let current_line_is_control_condition_close =
                current_line_is_parenthesized_condition_close
                    && layouts[idx]
                        .condition_header_line
                        .and_then(|header_idx| layouts.get(header_idx))
                        .is_some_and(|header| {
                            let header_upper = header.trimmed.to_ascii_uppercase();
                            crate::sql_text::starts_with_keyword_token(&header_upper, "IF")
                                || crate::sql_text::starts_with_keyword_token(
                                    &header_upper,
                                    "ELSIF",
                                )
                                || crate::sql_text::starts_with_keyword_token(
                                    &header_upper,
                                    "ELSEIF",
                                )
                                || crate::sql_text::starts_with_keyword_token(
                                    &header_upper,
                                    "WHILE",
                                )
                                || crate::sql_text::starts_with_keyword_token(&header_upper, "FOR")
                        });
            let is_paren_case_closer = pending_paren_case_closer_indent
                && starts_with_close_paren
                && !current_line_is_control_condition_close;
            let paren_case_base_depth = parser_depth;
            let parser_depth = parser_depth + usize::from(is_paren_case_closer);
            let last_code_indent = last_code_idx.map(|prev_idx| layouts[prev_idx].final_depth);
            let follows_comma_run =
                Self::line_layout_preceding_comment_or_comma_run_has_comma(layouts, idx);
            let current_line_starts_case =
                crate::sql_text::starts_with_keyword_token(&trimmed_upper, "CASE");
            let current_line_starts_dml_case_expression = in_dml_statement
                && current_line_starts_case
                && !previous_line_ends_with_then
                && !previous_line_is_else
                && previous_line_has_unclosed_open_paren;
            let current_line_dml_case_expression_owner_depth =
                if current_line_starts_dml_case_expression
                    && (previous_line_is_dml_clause_starter
                        || previous_line_ends_with_trailing_comma
                        || follows_comma_run)
                {
                    last_code_indent
                } else {
                    None
                };
            let current_line_is_condition_keyword =
                Self::starts_with_condition_keyword(&trimmed_upper);
            let current_line_is_condition_query_owner =
                Self::line_has_condition_query_owner(&trimmed_upper);
            let active_dml_case_condition_frame = dml_case_condition_frames
                .iter()
                .rev()
                .find(|frame| frame.parser_depth == depth)
                .copied();
            let current_line_starts_multiline_clause =
                in_dml_statement && Self::line_starts_multiline_clause_block(trimmed);
            let current_line_closes_multiline_clause = multiline_clause_frames
                .last()
                .copied()
                .is_some_and(|frame| starts_with_close_paren && frame.nested_paren_depth == 1);
            let active_multiline_clause = multiline_clause_frames.last().copied();
            let (
                paren_frame_scan_start_idx,
                last_popped_non_multiline_paren_frame,
                last_popped_general_paren_frame,
                leading_close_continues_expression,
            ) = Self::consume_leading_paren_layout_frames(&line_tokens, &mut paren_layout_frames);
            let popped_query_paren_frame = last_popped_non_multiline_paren_frame.filter(|frame| {
                matches!(
                    frame.kind,
                    ParenLayoutFrameKind::Query | ParenLayoutFrameKind::ConditionQuery
                )
            });
            let active_general_paren_frame = paren_layout_frames
                .iter()
                .rev()
                .find(|frame| frame.kind == ParenLayoutFrameKind::General)
                .copied();
            let current_general_paren_frame_count = paren_layout_frames
                .iter()
                .filter(|frame| frame.kind == ParenLayoutFrameKind::General)
                .count();
            let paren_case_close_frame_depth = last_popped_general_paren_frame.map(|frame| {
                let preferred_close_depth = if in_dml_statement
                    || layouts[idx].query_base_depth.is_some()
                    || previous_line_has_inline_comment_after_case_terminator
                    || frame.standalone_owner
                {
                    frame.owner_depth
                } else {
                    frame.continuation_depth
                };
                preferred_close_depth.max(paren_case_base_depth)
            });
            let starts_query_head = layouts[idx].starts_query_frame;
            let use_cursor_parser_depth = in_dml_statement
                && crate::sql_text::starts_with_keyword_token(&trimmed_upper, "SELECT")
                && previous_code_is_cursor_header
                && parser_depth > 1
                && next_code_trimmed.is_some_and(|next| {
                    let next_upper = next.to_ascii_uppercase();
                    !Self::is_dml_clause_starter(&next_upper)
                        && !crate::sql_text::starts_with_keyword_token(&next_upper, "INTO")
                        && !next.starts_with(')')
                });
            let uses_analyzer_query_depth = layouts[idx].query_role != AutoFormatQueryRole::None
                && !current_line_is_condition_keyword
                && !current_line_is_match_recognize_subclause
                && !previous_line_is_forall_header
                && !use_cursor_parser_depth;
            let in_query_statement = in_dml_statement || layouts[idx].query_base_depth.is_some();
            let current_line_query_close_align_depth = resolved_query_base_depths
                .iter()
                .rev()
                .take(closing_query_frame_count)
                .next_back()
                .map(|frame| frame.close_align_depth);
            let parenthesized_condition_header_depth =
                Self::parenthesized_condition_header_depth(layouts, idx);
            let resolved_query_base_frame =
                layouts[idx].query_base_depth.and_then(|query_base_depth| {
                    resolved_query_base_depths
                        .iter()
                        .rev()
                        .find(|frame| frame.raw_base_depth == query_base_depth)
                        .copied()
                });
            let resolved_query_base_depth = resolved_query_base_frame
                .map(|frame| frame.resolved_base_depth)
                .or(layouts[idx].query_base_depth);
            let resolved_query_origin = resolved_query_base_frame.map(|frame| frame.origin);
            let analyzer_query_depth = if let Some(query_base_depth) = layouts[idx].query_base_depth
            {
                let extra_depth = layouts[idx].auto_depth.saturating_sub(query_base_depth);
                resolved_query_base_depth
                    .unwrap_or(query_base_depth)
                    .saturating_add(extra_depth)
            } else {
                layouts[idx].auto_depth
            };
            let mut current_line_dml_case_expression_close_depth = None;
            // BEGIN after trigger header (WHEN/FOR EACH ROW) should stay at
            // parser_depth (base indent), not inherit the trigger header's +1 depth.
            let is_trigger_header_begin = !in_dml_statement
                && crate::sql_text::starts_with_keyword_token(&trimmed_upper, "BEGIN")
                && depth == 0
                && existing_indent == 0
                && last_code_idx.is_some_and(|prev_idx| {
                    let prev = layouts[prev_idx].trimmed.to_ascii_uppercase();
                    layouts[prev_idx].existing_indent > 0
                        && (prev.ends_with(')')
                            || prev.ends_with("ROW")
                            || prev.starts_with("REFERENCING ")
                            || prev.starts_with("FOR EACH"))
                });
            let mut effective_depth = if is_trigger_header_begin {
                parser_depth
            } else if !in_dml_statement
                && crate::sql_text::starts_with_keyword_token(&trimmed_upper, "BEGIN")
                && previous_line_ends_with_then
            {
                last_code_indent
                    .map(|indent| indent.saturating_add(1).max(parser_depth))
                    .unwrap_or(parser_depth.saturating_add(1))
            } else if !in_dml_statement
                && crate::sql_text::starts_with_keyword_token(&trimmed_upper, "CASE")
                && previous_line_ends_with_assignment
            {
                last_code_indent.unwrap_or(parser_depth)
            } else if in_dml_statement && previous_line_ends_with_then && !starts_query_head {
                last_code_indent
                    .map(|indent| {
                        indent
                            .saturating_add(1)
                            .max(existing_indent)
                            .max(parser_depth)
                    })
                    .unwrap_or(existing_indent.max(parser_depth.saturating_add(1)))
            } else if in_dml_statement && previous_line_is_else {
                last_code_indent
                    .map(|indent| {
                        indent
                            .saturating_add(1)
                            .max(existing_indent)
                            .max(parser_depth)
                    })
                    .unwrap_or(existing_indent.max(parser_depth.saturating_add(1)))
            } else if in_dml_statement
                && starts_with_close_paren
                && pending_dml_case_expression_close_depth.is_some()
            {
                let owner_depth = pending_dml_case_expression_close_depth.unwrap_or(parser_depth);
                current_line_dml_case_expression_close_depth = Some(owner_depth);
                owner_depth.max(parser_depth)
            } else if starts_with_close_paren && is_paren_case_closer {
                paren_case_close_frame_depth.unwrap_or_else(|| {
                    last_code_indent
                        .map(|indent| indent.saturating_sub(1).max(paren_case_base_depth))
                        .unwrap_or(paren_case_base_depth)
                })
            } else if let Some(owner_depth) = current_line_dml_case_expression_owner_depth {
                owner_depth
                    .saturating_add(1)
                    .max(existing_indent)
                    .max(parser_depth)
            } else if uses_analyzer_query_depth {
                if starts_query_head {
                    pending_query_head_depth.unwrap_or(analyzer_query_depth)
                } else {
                    analyzer_query_depth
                }
            } else if in_dml_statement
                && current_line_starts_case
                && previous_line_ends_with_open_paren
                && !previous_line_is_standalone_open_paren
            {
                last_code_indent
                    .map(|indent| indent.saturating_add(1).max(parser_depth))
                    .unwrap_or(parser_depth.saturating_add(1))
            } else if in_dml_statement
                && trimmed == "("
                && previous_line_ends_with_open_paren
                && next_code_trimmed.is_some_and(|next| {
                    let next_upper = next.to_ascii_uppercase();
                    Self::is_dml_clause_starter(&next_upper)
                })
            {
                last_code_indent
                    .map(|indent| {
                        indent
                            .saturating_add(1)
                            .max(parser_depth)
                            .max(existing_indent)
                    })
                    .unwrap_or(existing_indent.max(parser_depth.saturating_add(1)))
            } else if in_dml_statement
                && current_line_starts_case
                && previous_line_is_dml_clause_starter
            {
                last_code_indent
                    .map(|indent| indent.saturating_add(1).max(parser_depth))
                    .unwrap_or(parser_depth.saturating_add(1))
            } else if in_dml_statement
                && crate::sql_text::starts_with_keyword_token(&trimmed_upper, "SELECT")
                && use_cursor_parser_depth
            {
                parser_depth
            } else if Self::starts_with_end_suffix_terminator(&trimmed_upper) {
                if trimmed_upper.starts_with("END LOOP")
                    || trimmed_upper.starts_with("END REPEAT")
                    || trimmed_upper.starts_with("END WHILE")
                    || trimmed_upper.starts_with("END FOR")
                {
                    existing_indent.clamp(parser_depth, parser_depth.saturating_add(1))
                } else {
                    parser_depth
                }
            } else if (current_line_is_parenthesized_condition_close
                && !is_paren_case_closer
                && popped_query_paren_frame.is_none())
                || (current_line_is_parenthesized_condition
                    && current_line_is_condition_keyword
                    && previous_line_starts_with_close_paren)
            {
                parenthesized_condition_header_depth.unwrap_or(parser_depth)
            } else if force_block_depth {
                parser_depth
            } else if !in_dml_statement && previous_line_ends_with_trailing_comma {
                if previous_line_starts_with_using_clause {
                    last_code_indent
                        .map(|indent| indent.saturating_add(1).max(parser_depth))
                        .unwrap_or(parser_depth.saturating_add(1).max(existing_indent))
                } else {
                    last_code_indent
                        .map(|indent| indent.max(parser_depth))
                        .unwrap_or(parser_depth.max(existing_indent))
                }
            } else if in_dml_statement && starts_subquery_head && previous_line_ends_with_open_paren
            {
                let nested_subquery_depth = if previous_line_is_cte_definition_header
                    || current_line_is_parenthesized_condition
                {
                    parser_depth
                } else {
                    parser_depth.saturating_add(1)
                };
                last_code_indent
                    .map(|indent| indent.saturating_add(1).max(nested_subquery_depth))
                    .unwrap_or(nested_subquery_depth)
            } else if !in_dml_statement
                && current_line_is_parenthesized_condition
                && !current_line_is_parenthesized_condition_header
                && !current_line_is_parenthesized_condition_close
            {
                parser_depth.saturating_add(1)
            } else if in_dml_statement
                && previous_line_is_forall_header
                && Self::is_dml_clause_starter(&trimmed_upper)
            {
                last_code_indent
                    .map(|indent| {
                        if indent <= 1 {
                            indent.saturating_add(1)
                        } else {
                            indent
                        }
                    })
                    .unwrap_or(parser_depth)
            } else if in_dml_statement
                && previous_line_is_select_header
                && previous_select_follows_cursor_header
                && !Self::is_dml_clause_starter(&trimmed_upper)
                && !crate::sql_text::starts_with_keyword_token(&trimmed_upper, "INTO")
                && !starts_with_close_paren
            {
                last_code_indent
                    .map(|indent| indent.saturating_add(1))
                    .unwrap_or(parser_depth.saturating_add(1))
            } else if in_dml_statement
                && previous_line_is_select_header
                && !Self::is_dml_clause_starter(&trimmed_upper)
                && !crate::sql_text::starts_with_keyword_token(&trimmed_upper, "INTO")
                && !starts_with_close_paren
            {
                last_code_indent
                    .map(|indent| indent.saturating_add(1).max(parser_depth))
                    .unwrap_or(parser_depth.saturating_add(1))
            } else if in_dml_statement
                && (previous_line_is_case_header || previous_line_has_trailing_unclosed_case)
                && (trimmed_upper.starts_with("WHEN ") || trimmed_upper.starts_with("ELSE"))
            {
                last_code_indent
                    .map(|indent| indent.saturating_add(1).max(parser_depth))
                    .unwrap_or(parser_depth.saturating_add(1))
            } else if crate::sql_text::starts_with_keyword_token(&trimmed_upper, "GROUP")
                && previous_line_ends_with_within
            {
                last_code_indent.unwrap_or(existing_indent.max(parser_depth))
            } else if previous_line_is_order_by_clause
                && previous_line_ends_with_trailing_comma
                && !Self::is_dml_clause_starter(&trimmed_upper)
                && !Self::is_match_recognize_subclause(&trimmed_upper)
                && !crate::sql_text::starts_with_keyword_token(&trimmed_upper, "INTO")
            {
                last_code_indent
                    .map(|indent| indent.saturating_add(1).max(parser_depth))
                    .unwrap_or(parser_depth.saturating_add(1))
            } else if previous_line_is_match_recognize_order_by
                && previous_line_ends_with_trailing_comma
                && !Self::is_match_recognize_subclause(&trimmed_upper)
            {
                last_code_indent
                    .map(|indent| indent.saturating_add(1).max(parser_depth))
                    .unwrap_or(parser_depth.saturating_add(1))
            } else if Self::is_match_recognize_subclause(&trimmed_upper)
                && existing_indent > parser_depth
            {
                existing_indent
            } else if in_query_statement && current_line_is_condition_keyword {
                let condition_indent = resolved_query_base_depth
                    .map(|depth| depth.saturating_add(1))
                    .unwrap_or(parser_depth.saturating_add(1));
                if previous_line_starts_with_close_paren {
                    if let Some((join_and_depth, _)) = join_on_condition_and_depth {
                        // Inside a JOIN ON condition block: AND/OR after a
                        // close paren (e.g. subquery) should stay at the
                        // same depth as other AND/OR in this ON block.
                        join_and_depth.max(condition_indent)
                    } else {
                        last_code_indent
                            .map(|indent| indent.max(condition_indent))
                            .unwrap_or(condition_indent)
                    }
                } else if previous_line_is_join_condition_clause {
                    last_code_indent
                        .map(|indent| {
                            indent
                                .saturating_add(1)
                                .max(condition_indent.saturating_add(1))
                                .max(parser_depth)
                        })
                        .unwrap_or(condition_indent.saturating_add(1).max(parser_depth))
                } else if previous_line_is_condition_keyword {
                    let paren_context_stable =
                        current_general_paren_frame_count == prev_general_paren_frame_count;
                    if paren_context_stable {
                        last_code_indent
                            .map(|indent| indent.max(condition_indent))
                            .unwrap_or(condition_indent)
                    } else if let Some(frame) = active_general_paren_frame {
                        let base = frame.continuation_depth.max(condition_indent);
                        // Add +1 when the paren was opened at or above the
                        // query base (owner_depth < condition_indent), meaning
                        // it is a condition-grouping paren on the WHERE/ON/
                        // HAVING line.  Parens opened deeper (on an AND/OR
                        // line that was already +1'd) have continuation that
                        // already accounts for the extra level.
                        if frame.owner_depth < condition_indent {
                            base.saturating_add(1)
                        } else {
                            base
                        }
                    } else {
                        condition_indent
                    }
                } else if let Some(frame) = active_general_paren_frame {
                    // Inside a General paren opened at query base level,
                    // AND/OR should indent one more level so that grouping
                    // parens visually add depth:
                    //   WHERE (col = 1          -- paren at condition base
                    //           AND col2 = 2)   -- AND one level deeper
                    let base = frame.continuation_depth.max(condition_indent);
                    if frame.owner_depth < condition_indent {
                        base.saturating_add(1)
                    } else {
                        base
                    }
                } else {
                    condition_indent.max(parser_depth)
                }
            } else if in_query_statement
                && previous_line_is_close_paren_with_trailing_comma
                && existing_indent > parser_depth
                && !Self::is_dml_clause_starter(&trimmed_upper)
                && !crate::sql_text::starts_with_keyword_token(&trimmed_upper, "INTO")
                && !starts_with_close_paren
            {
                last_code_indent
                    .map(|indent| indent.max(existing_indent))
                    .unwrap_or(existing_indent)
            } else if in_dml_statement
                && follows_comma_run
                && previous_code_is_inline_merge_update_set
                && !Self::is_dml_clause_starter(&trimmed_upper)
                && !crate::sql_text::starts_with_keyword_token(&trimmed_upper, "INTO")
            {
                last_code_indent.unwrap_or(parser_depth.max(existing_indent))
            } else if in_dml_statement
                && follows_comma_run
                && !Self::is_dml_clause_starter(&trimmed_upper)
                && !crate::sql_text::starts_with_keyword_token(&trimmed_upper, "INTO")
            {
                last_code_indent
                    .map(|indent| indent.max(parser_depth))
                    .unwrap_or(parser_depth.max(existing_indent))
            } else if in_dml_statement
                && previous_line_ends_with_trailing_comma
                && !previous_line_is_dml_clause_line
            {
                last_code_indent.unwrap_or(parser_depth)
            } else if in_dml_statement
                && Self::is_dml_clause_starter(&trimmed_upper)
                && previous_line_is_dml_clause_starter
            {
                if last_code_indent.is_some_and(|indent| indent > parser_depth) {
                    last_code_indent.unwrap_or(parser_depth)
                } else {
                    existing_indent.clamp(parser_depth, parser_depth.saturating_add(1))
                }
            } else if in_dml_statement
                && Self::is_dml_clause_starter(&trimmed_upper)
                && previous_line_starts_with_close_paren
            {
                resolved_query_base_depth
                    .or(layouts[idx].query_base_depth)
                    .unwrap_or(parser_depth)
            } else if in_dml_statement
                && starts_with_close_paren
                && previous_line_is_plain_end
                && !is_paren_case_closer
            {
                last_code_indent
                    .map(|indent| indent.saturating_sub(1).max(parser_depth))
                    .unwrap_or(parser_depth)
            } else if in_query_statement
                && starts_with_close_paren
                && popped_query_paren_frame.is_some()
                && current_line_query_close_align_depth.is_some()
            {
                current_line_query_close_align_depth.unwrap_or(parser_depth)
            } else if in_query_statement
                && starts_with_close_paren
                && popped_query_paren_frame.is_some()
                && last_code_idx.is_some_and(|prev_idx| {
                    layouts[prev_idx].query_role == AutoFormatQueryRole::None
                })
                && existing_indent > parser_depth
            {
                last_code_indent
                    .map(|indent| indent.saturating_sub(1).max(existing_indent))
                    .unwrap_or(existing_indent)
            } else if in_query_statement
                && starts_with_close_paren
                && (previous_line_is_dml_clause_line
                    || previous_line_is_plain_end
                    || next_line_is_case_branch
                    || is_paren_case_closer)
            {
                if next_line_is_case_branch {
                    next_line_existing_indent.unwrap_or(parser_depth)
                } else if is_paren_case_closer {
                    paren_case_close_frame_depth.unwrap_or_else(|| {
                        last_code_indent
                            .map(|indent| indent.saturating_sub(1).max(paren_case_base_depth))
                            .unwrap_or(paren_case_base_depth)
                    })
                } else if previous_line_is_dml_clause_line {
                    last_code_indent
                        .map(|indent| indent.saturating_sub(1).max(parser_depth))
                        .unwrap_or(parser_depth)
                } else if previous_line_is_plain_end {
                    parser_depth.saturating_add(2)
                } else {
                    last_code_indent
                        .map(|indent| indent.saturating_sub(1).max(parser_depth))
                        .unwrap_or_else(|| {
                            existing_indent.clamp(parser_depth, parser_depth.saturating_add(1))
                        })
                }
            } else if in_query_statement && starts_with_close_paren {
                last_code_indent
                    .map(|indent| {
                        indent
                            .saturating_sub(1)
                            .max(existing_indent)
                            .max(parser_depth)
                    })
                    .unwrap_or(existing_indent.max(parser_depth))
            } else if in_query_statement
                && previous_line_is_close_paren_with_trailing_comma
                && !Self::is_dml_clause_starter(&trimmed_upper)
                && !crate::sql_text::starts_with_keyword_token(&trimmed_upper, "INTO")
            {
                last_code_indent
                    .map(|indent| indent.max(existing_indent))
                    .unwrap_or(existing_indent.max(parser_depth))
            } else if in_dml_statement {
                let closes_into_list = Self::is_into_continuation_ender(&trimmed_upper);
                let max_extra = if closes_into_list || follows_comma_run {
                    0
                } else {
                    2
                };
                existing_indent.clamp(parser_depth, parser_depth.saturating_add(max_extra))
            } else if existing_indent > parser_depth.saturating_add(3) {
                parser_depth
            } else {
                parser_depth.max(existing_indent)
            };
            if previous_line_is_order_by_clause
                && previous_line_ends_with_trailing_comma
                && !Self::is_dml_clause_starter(&trimmed_upper)
                && !Self::is_match_recognize_subclause(&trimmed_upper)
                && !crate::sql_text::starts_with_keyword_token(&trimmed_upper, "INTO")
            {
                effective_depth = last_code_indent
                    .map(|indent| indent.saturating_add(1).max(parser_depth))
                    .unwrap_or(parser_depth.saturating_add(1));
            }
            if in_dml_statement {
                if let Some(case_frame) = dml_case_frames.last().copied() {
                    if trimmed_upper.starts_with("WHEN ") || trimmed_upper.starts_with("ELSE") {
                        effective_depth =
                            effective_depth.max(case_frame.case_depth.saturating_add(1));
                    } else if Self::starts_with_case_terminator(&trimmed_upper) {
                        effective_depth = effective_depth.max(case_frame.case_depth);
                    }
                }
            }
            if current_line_is_condition_keyword {
                if let Some(frame) = active_dml_case_condition_frame {
                    effective_depth = effective_depth.max(frame.continuation_depth);
                }
            }

            let previous_line_is_select_hint = last_code_idx.is_some_and(|prev_idx| {
                layouts[prev_idx]
                    .trimmed
                    .to_ascii_uppercase()
                    .starts_with("SELECT /*+")
            });
            let starts_clause_keyword = Self::is_dml_clause_starter(&trimmed_upper)
                || crate::sql_text::starts_with_keyword_token(&trimmed_upper, "INTO")
                || crate::sql_text::starts_with_keyword_token(&trimmed_upper, "SELECT");
            let effective_depth = if starts_query_head && previous_line_ends_with_open_paren {
                // Query heads opened on the next line should inherit any still-open
                // general paren continuation depth from the owner line, not just the
                // analyzer's single pending query-base level.
                active_general_paren_frame
                    .map(|frame| effective_depth.max(frame.continuation_depth))
                    .unwrap_or(effective_depth)
            } else {
                effective_depth
            };
            let effective_depth = if previous_line_is_select_hint && !starts_clause_keyword {
                effective_depth.max(1)
            } else {
                effective_depth
            };
            let effective_depth =
                if let Some(branch_body_depth) = pending_case_branch_body_depth_for_line {
                    if trimmed_upper.starts_with("WHEN ")
                        || trimmed_upper.starts_with("ELSE")
                        || Self::starts_with_case_terminator(&trimmed_upper)
                    {
                        effective_depth
                    } else {
                        effective_depth.max(branch_body_depth)
                    }
                } else {
                    effective_depth
                };
            let clause_anchor_depth = if !uses_analyzer_query_depth
                && !previous_line_starts_with_close_paren
                && Self::is_dml_clause_starter(&trimmed_upper)
            {
                Self::previous_dml_clause_starter_depth(layouts, idx, parser_depth).or_else(|| {
                    Self::previous_dml_clause_starter_depth(
                        layouts,
                        idx,
                        parser_depth.saturating_add(1),
                    )
                })
            } else {
                None
            };
            let effective_depth = clause_anchor_depth
                .map(|anchor_depth| effective_depth.max(anchor_depth))
                .unwrap_or(effective_depth);
            let effective_depth =
                if crate::sql_text::starts_with_keyword_token(&trimmed_upper, "GROUP")
                    && last_code_idx.is_some_and(|prev_idx| {
                        layouts[prev_idx]
                            .trimmed
                            .to_ascii_uppercase()
                            .contains("WITHIN")
                    })
                {
                    last_code_indent.unwrap_or(effective_depth.max(existing_indent))
                } else {
                    effective_depth
                };
            let effective_depth = if let Some(frame) = active_multiline_clause {
                if current_line_closes_multiline_clause {
                    frame.owner_depth
                } else {
                    effective_depth.max(frame.owner_depth.saturating_add(1))
                }
            } else {
                effective_depth
            };
            let close_continuation_frame =
                active_general_paren_frame.or(last_popped_general_paren_frame);
            let condition_close_alignment_active = current_line_is_parenthesized_condition_close
                || (current_line_is_parenthesized_condition
                    && current_line_is_condition_keyword
                    && previous_line_starts_with_close_paren);
            let defers_to_condition_close_alignment =
                condition_close_alignment_active && popped_query_paren_frame.is_none();
            let clause_starter_uses_general_paren_continuation =
                Self::clause_starter_uses_general_paren_continuation(
                    &trimmed_upper,
                    starts_query_head,
                    active_general_paren_frame,
                );
            let effective_depth = if leading_close_continues_expression
                && popped_query_paren_frame.is_none()
                && !defers_to_condition_close_alignment
                && !is_paren_case_closer
                && close_continuation_frame.is_some()
            {
                close_continuation_frame
                    .map(|frame| effective_depth.max(frame.continuation_depth))
                    .unwrap_or(effective_depth)
            } else if starts_with_close_paren
                && !defers_to_condition_close_alignment
                && !is_paren_case_closer
            {
                if let Some(frame) = popped_query_paren_frame {
                    current_line_query_close_align_depth.unwrap_or(frame.owner_depth)
                } else {
                    last_popped_non_multiline_paren_frame
                        .map(|frame| frame.owner_depth)
                        .unwrap_or(effective_depth)
                }
            } else if !starts_with_close_paren
                && (clause_starter_uses_general_paren_continuation
                    || (!Self::is_dml_clause_starter(&trimmed_upper)
                        && !crate::sql_text::starts_with_keyword_token(&trimmed_upper, "INTO")))
            {
                // Clause-shaped keywords can appear in expression-level parens
                // too (for example `OVER (... ORDER BY ...)` or
                // `OVERLAY (... FROM ... FOR ...)`). When that happens, keep
                // the general-paren continuation depth instead of snapping back
                // to the surrounding query clause depth.
                active_general_paren_frame
                    .map(|frame| effective_depth.max(frame.continuation_depth))
                    .unwrap_or(effective_depth)
            } else {
                effective_depth
            };
            if starts_query_head {
                if let Some(query_base_depth) = layouts[idx].query_base_depth {
                    let extra_depth = layouts[idx].auto_depth.saturating_sub(query_base_depth);
                    let resolved_base_depth = effective_depth.saturating_sub(extra_depth);
                    let close_align_depth = resolved_base_depth.saturating_sub(1);
                    let query_head_origin =
                        pending_query_head_origin.unwrap_or(QueryHeadLayoutOrigin::Other);
                    let should_store_resolved_query_base = pending_query_head_depth.is_some()
                        || resolved_base_depth != query_base_depth;
                    if should_store_resolved_query_base {
                        if let Some(frame) = resolved_query_base_depths
                            .iter_mut()
                            .rev()
                            .find(|frame| frame.raw_base_depth == query_base_depth)
                        {
                            frame.resolved_base_depth = resolved_base_depth;
                            frame.start_parser_depth = depth;
                            frame.close_align_depth = close_align_depth;
                            frame.origin = query_head_origin;
                        } else {
                            resolved_query_base_depths.push(ResolvedQueryBaseLayoutFrame {
                                raw_base_depth: query_base_depth,
                                resolved_base_depth,
                                start_parser_depth: depth,
                                close_align_depth,
                                origin: query_head_origin,
                            });
                        }
                    }
                }
            }

            layouts[idx].final_depth = effective_depth;
            layouts[idx].dml_case_expression_close_depth =
                current_line_dml_case_expression_close_depth;

            // Track JOIN ON condition AND depth: set when we see ON/USING
            // in a join context, or when an AND/OR is processed that
            // continues a join condition.  Clear on a new clause/JOIN
            // at the same or lower parser depth (not inside subqueries).
            {
                let is_join_condition_line =
                    crate::sql_text::starts_with_keyword_token(&trimmed_upper, "ON")
                        || crate::sql_text::starts_with_keyword_token(&trimmed_upper, "USING");
                let is_join_clause_line = QueryExecutor::auto_format_is_join_clause(&trimmed_upper);
                let is_dml_clause = Self::is_dml_clause_starter(&trimmed_upper)
                    || crate::sql_text::starts_with_keyword_token(&trimmed_upper, "SELECT")
                    || crate::sql_text::starts_with_keyword_token(&trimmed_upper, "INTO");
                if is_join_condition_line
                    && layouts[idx].query_role == AutoFormatQueryRole::Continuation
                {
                    join_on_condition_and_depth = Some((effective_depth.saturating_add(1), depth));
                } else if current_line_is_condition_keyword && join_on_condition_and_depth.is_some()
                {
                    // AND/OR continues the join condition block; keep the
                    // tracked depth (don't clear).
                } else if (is_join_clause_line || is_dml_clause || starts_query_head)
                    && join_on_condition_and_depth.is_some_and(|(_, on_depth)| depth <= on_depth)
                {
                    // Only clear when the new clause is at the same or
                    // lower parser depth — subquery heads inside the ON
                    // condition should not clear this state.
                    join_on_condition_and_depth = None;
                }
            }
            if pending_case_branch_body_depth_for_line.is_some() {
                pending_case_branch_body_depth = None;
            }
            if in_dml_statement
                && dml_case_frames.last().is_some()
                && (Self::line_ends_with_then_before_inline_comment(trimmed)
                    || trimmed_upper.trim() == "ELSE")
            {
                pending_case_branch_body_depth = Some(layouts[idx].final_depth.saturating_add(1));
            }
            let current_condition_header_depth = if current_line_is_parenthesized_condition_header {
                Some(effective_depth)
            } else {
                parenthesized_condition_header_depth
            };
            if let Some(frame) = multiline_clause_frames.last_mut() {
                if !current_line_starts_multiline_clause {
                    let (open_count, close_count) = Self::line_significant_paren_counts(trimmed);
                    frame.nested_paren_depth = frame
                        .nested_paren_depth
                        .saturating_add(open_count)
                        .saturating_sub(close_count);
                }
            }
            if current_line_closes_multiline_clause {
                multiline_clause_frames.pop();
            }
            if current_line_starts_multiline_clause {
                multiline_clause_frames.push(MultilineClauseLayoutFrame {
                    owner_depth: effective_depth,
                    nested_paren_depth: 1,
                });
            }
            let anchor_group = if !in_dml_statement
                && previous_line_ends_with_trailing_comma
                && last_code_indent.is_some_and(|indent| indent == effective_depth)
            {
                if let Some(group) =
                    last_code_idx.and_then(|prev_idx| layouts[prev_idx].anchor_group)
                {
                    group
                } else {
                    let group = next_anchor_group;
                    next_anchor_group = next_anchor_group.saturating_add(1);
                    group
                }
            } else {
                let group = next_anchor_group;
                next_anchor_group = next_anchor_group.saturating_add(1);
                group
            };
            layouts[idx].anchor_group = Some(anchor_group);

            if in_dml_statement && current_line_starts_case {
                dml_case_frames.push(DmlCaseLayoutFrame {
                    case_depth: effective_depth,
                    expression_owner_depth: current_line_dml_case_expression_owner_depth,
                });
            } else if in_dml_statement
                && !current_line_starts_case
                && previous_line_has_trailing_unclosed_case
                && dml_case_frames.is_empty()
                && (trimmed_upper.starts_with("WHEN ") || trimmed_upper.starts_with("ELSE"))
            {
                // CASE opened mid-line on the previous code line (e.g. `SELECT col, CASE`
                // or `SET col = CASE`).  Retroactively push a frame so that WHEN/ELSE/END
                // on subsequent lines get the correct minimum depth enforcement.
                let inferred_case_depth = last_code_idx
                    .map(|prev_idx| layouts[prev_idx].final_depth)
                    .unwrap_or(effective_depth.saturating_sub(1));
                dml_case_frames.push(DmlCaseLayoutFrame {
                    case_depth: inferred_case_depth,
                    expression_owner_depth: None,
                });
            }
            if in_dml_statement
                && Self::starts_with_case_terminator(&trimmed_upper)
                && !dml_case_frames.is_empty()
            {
                if let Some(frame) = dml_case_frames.pop() {
                    if frame.expression_owner_depth.is_some()
                        && next_code_trimmed.is_some_and(|next| next.trim_start().starts_with(')'))
                    {
                        pending_dml_case_expression_close_depth = frame.expression_owner_depth;
                    }
                }
                while dml_case_condition_frames
                    .last()
                    .is_some_and(|frame| frame.parser_depth >= depth)
                {
                    dml_case_condition_frames.pop();
                }
            }
            let line_ends_current_case_condition = dml_case_condition_frames
                .last()
                .copied()
                .is_some_and(|frame| {
                    Self::line_contains_dml_case_condition_terminator(
                        &line_tokens,
                        depth,
                        frame.parser_depth,
                    )
                });
            if line_ends_current_case_condition {
                dml_case_condition_frames.pop();
            }
            let starts_multiline_dml_case_condition = in_dml_statement
                && trimmed_upper.starts_with("WHEN ")
                && dml_case_frames.last().is_some()
                && !Self::line_contains_dml_case_condition_terminator(&line_tokens, depth, depth);
            if starts_multiline_dml_case_condition {
                dml_case_condition_frames.push(DmlCaseConditionLayoutFrame {
                    parser_depth: depth.saturating_add(1),
                    continuation_depth: layouts[idx].final_depth.saturating_add(1),
                });
            }

            if in_paren_case_expression && Self::starts_with_case_terminator(&trimmed_upper) {
                paren_case_expression_depth = paren_case_expression_depth.saturating_sub(1);
                if paren_case_expression_depth == 0 {
                    pending_paren_case_closer_indent = true;
                }
            }

            if pending_paren_case_closer_indent && starts_with_close_paren {
                pending_paren_case_closer_indent = false;
            }
            if pending_dml_case_expression_close_depth.is_some() && starts_with_close_paren {
                pending_dml_case_expression_close_depth = None;
            }
            Self::update_paren_layout_frames_for_line(
                &line_tokens,
                paren_frame_scan_start_idx,
                next_code_trimmed,
                effective_depth,
                current_line_starts_multiline_clause,
                current_line_is_condition_query_owner,
                current_line_is_standalone_open_paren,
                &mut paren_layout_frames,
            );

            if starts_query_head || pending_query_head_depth.is_some() {
                pending_query_head_depth = None;
                pending_query_head_origin = None;
            }
            if let Some(next_query_head_depth) = layouts[idx].next_query_head_depth {
                let case_condition_query_owner_depth = if current_line_is_condition_query_owner
                    && current_line_is_condition_keyword
                    && active_dml_case_condition_frame.is_some()
                {
                    Some(layouts[idx].final_depth.saturating_add(1))
                } else {
                    None
                };
                let structural_query_owner_depth = case_condition_query_owner_depth.or_else(|| {
                    Self::structural_next_query_head_depth(
                        &layouts[idx],
                        &trimmed_upper,
                        resolved_query_base_depth,
                        current_condition_header_depth,
                        resolved_query_origin,
                    )
                });
                let adjusted_next_query_head_depth =
                    if let Some(owner_depth) = structural_query_owner_depth {
                        owner_depth
                    } else if layouts[idx].final_depth >= layouts[idx].existing_indent {
                        next_query_head_depth.saturating_add(
                            layouts[idx]
                                .final_depth
                                .saturating_sub(layouts[idx].existing_indent),
                        )
                    } else {
                        next_query_head_depth.saturating_sub(
                            layouts[idx]
                                .existing_indent
                                .saturating_sub(layouts[idx].final_depth),
                        )
                    };
                pending_query_head_depth = Some(adjusted_next_query_head_depth);
                pending_query_head_origin =
                    Some(Self::structural_query_head_origin(&trimmed_upper));
            }

            if trimmed.ends_with(';') {
                in_dml_statement = false;
                pending_query_head_depth = None;
                pending_query_head_origin = None;
                resolved_query_base_depths.clear();
                multiline_clause_frames.clear();
                paren_layout_frames.clear();
                dml_case_frames.clear();
                dml_case_condition_frames.clear();
                paren_case_expression_depth = 0;
                pending_paren_case_closer_indent = false;
                pending_dml_case_expression_close_depth = None;
                pending_case_branch_body_depth = None;
                join_on_condition_and_depth = None;
            } else {
                for _ in 0..closing_query_frame_count {
                    resolved_query_base_depths.pop();
                }
            }
            prev_general_paren_frame_count = current_general_paren_frame_count;
            last_code_idx = Some(idx);
        }
    }

    fn align_case_close_paren_layouts(
        layouts: &mut [LineLayout<'_>],
        previous_code_indices: &[Option<usize>],
        next_code_indices: &[Option<usize>],
    ) {
        for idx in 0..layouts.len() {
            if layouts[idx].kind != LineLayoutKind::Code || !layouts[idx].trimmed.starts_with(')') {
                continue;
            }

            let Some(prev_idx) = previous_code_indices[idx] else {
                continue;
            };
            let Some(next_idx) = next_code_indices[idx] else {
                continue;
            };
            if layouts[idx].dml_case_expression_close_depth.is_some() {
                continue;
            }

            let previous_upper = layouts[prev_idx].trimmed.to_ascii_uppercase();
            let next_upper = layouts[next_idx].trimmed.to_ascii_uppercase();
            let next_is_case_branch = next_upper.starts_with("WHEN ")
                || (next_upper.starts_with("ELSE")
                    && !next_upper.starts_with("ELSIF")
                    && !next_upper.starts_with("ELSEIF"));
            if Self::starts_with_case_terminator(&previous_upper) && next_is_case_branch {
                layouts[idx].final_depth = layouts[next_idx].final_depth;
                layouts[idx].anchor_group = layouts[next_idx].anchor_group;
            }
        }
    }

    fn resolve_non_code_line_layouts(
        layouts: &mut [LineLayout<'_>],
        previous_code_indices: &[Option<usize>],
        next_code_indices: &[Option<usize>],
    ) {
        let anchor_depths = Self::line_layout_anchor_depths(layouts);
        let mut idx = 0usize;

        while idx < layouts.len() {
            let kind = layouts[idx].kind;
            if kind != LineLayoutKind::CommentOnly && kind != LineLayoutKind::CommaOnly {
                idx += 1;
                continue;
            }

            let start = idx;
            while idx < layouts.len() {
                let run_kind = layouts[idx].kind;
                if run_kind != LineLayoutKind::CommentOnly && run_kind != LineLayoutKind::CommaOnly
                {
                    break;
                }
                idx += 1;
            }

            let previous_code_idx = previous_code_indices[start];
            let next_code_idx = next_code_indices[start];
            let previous_code_is_using_continuation_anchor =
                previous_code_idx.is_some_and(|prev_idx| {
                    Self::line_starts_with_using_clause(layouts[prev_idx].trimmed)
                        && Self::line_ends_with_comma_before_inline_comment(
                            layouts[prev_idx].trimmed,
                        )
                });
            let prefer_previous_scope = match (previous_code_idx, next_code_idx) {
                (Some(prev_idx), Some(next_idx)) => {
                    let next_upper = layouts[next_idx].trimmed.to_ascii_uppercase();
                    let next_is_named_plain_end = Self::starts_with_plain_end(&next_upper)
                        && !Self::starts_with_bare_end(&next_upper);
                    next_is_named_plain_end
                        && layouts[prev_idx].final_depth > layouts[next_idx].final_depth
                }
                _ => false,
            };
            let anchor_group = if prefer_previous_scope {
                previous_code_idx.and_then(|prev_idx| layouts[prev_idx].anchor_group)
            } else {
                next_code_idx
                    .and_then(|next_idx| layouts[next_idx].anchor_group)
                    .or_else(|| {
                        previous_code_idx.and_then(|prev_idx| layouts[prev_idx].anchor_group)
                    })
            };
            let target_depth = if previous_code_is_using_continuation_anchor {
                previous_code_idx
                    .map(|prev_idx| layouts[prev_idx].final_depth.saturating_add(1))
                    .unwrap_or(0)
            } else {
                anchor_group
                    .and_then(|group| anchor_depths.get(group).copied().flatten())
                    .or_else(|| next_code_idx.map(|next_idx| layouts[next_idx].final_depth))
                    .or_else(|| previous_code_idx.map(|prev_idx| layouts[prev_idx].final_depth))
                    .unwrap_or(0)
            };

            for layout in &mut layouts[start..idx] {
                layout.anchor_group = anchor_group;
                layout.final_depth = target_depth;
            }
        }
    }

    fn line_layout_anchor_depths(layouts: &[LineLayout<'_>]) -> Vec<Option<usize>> {
        let max_anchor_group = layouts
            .iter()
            .filter_map(|layout| layout.anchor_group)
            .max();
        let Some(max_anchor_group) = max_anchor_group else {
            return Vec::new();
        };

        let mut anchor_depths = vec![None; max_anchor_group.saturating_add(1)];
        for layout in layouts {
            if layout.kind != LineLayoutKind::Code {
                continue;
            }
            if let Some(anchor_group) = layout.anchor_group {
                anchor_depths[anchor_group] = Some(layout.final_depth);
            }
        }

        anchor_depths
    }

    fn line_preserves_existing_odd_hanging_indent(layouts: &[LineLayout<'_>], idx: usize) -> bool {
        let Some(layout) = layouts.get(idx) else {
            return false;
        };
        if layout.kind != LineLayoutKind::Code
            || layout.existing_indent_spaces == 0
            || layout.existing_indent_spaces % 4 == 0
        {
            return false;
        }

        let depth_indent = layout.final_depth.saturating_mul(4);
        let trimmed_upper = layout.trimmed.to_ascii_uppercase();
        let previous_code_idx = (0..idx)
            .rev()
            .find(|candidate| layouts[*candidate].kind == LineLayoutKind::Code);
        let previous_code = previous_code_idx.and_then(|prev_idx| layouts.get(prev_idx));

        let preserves_condition_hanging_indent =
            Self::starts_with_condition_keyword(&trimmed_upper)
                && layout.existing_indent_spaces.saturating_add(2) == depth_indent
                && previous_code.is_some_and(|previous| {
                    let previous_upper = previous.trimmed.to_ascii_uppercase();
                    layout.query_base_depth == previous.query_base_depth
                        && (crate::sql_text::starts_with_keyword_token(&previous_upper, "WHERE")
                            || crate::sql_text::starts_with_keyword_token(
                                &previous_upper,
                                "HAVING",
                            )
                            || crate::sql_text::starts_with_keyword_token(&previous_upper, "ON")
                            || Self::starts_with_condition_keyword(&previous_upper))
                });
        if preserves_condition_hanging_indent {
            return true;
        }

        layout.existing_indent_spaces == depth_indent.saturating_add(3)
            && !Self::is_dml_clause_starter(&trimmed_upper)
            && !crate::sql_text::starts_with_keyword_token(&trimmed_upper, "INTO")
            && previous_code_idx.is_some_and(|prev_idx| {
                let Some(previous) = layouts.get(prev_idx) else {
                    return false;
                };
                if layout.query_base_depth != previous.query_base_depth
                    || !Self::line_ends_with_comma_before_inline_comment(previous.trimmed)
                {
                    return false;
                }

                // Preserve a +3 hanging indent only when that visual alignment
                // is already established by the anchor line:
                //  - the previous line is an inline clause/header owner
                //    (`SELECT first_col,` -> `   second_col,`), or
                //  - the previous sibling also preserves that same odd hanging indent.
                previous.final_depth.saturating_add(1) == layout.final_depth
                    || (previous.existing_indent_spaces == depth_indent.saturating_add(3)
                        && Self::line_preserves_existing_odd_hanging_indent(layouts, prev_idx))
            })
    }

    fn render_line_layouts(layouts: &[LineLayout<'_>]) -> String {
        let mut out = String::new();

        for (idx, layout) in layouts.iter().enumerate() {
            if Self::should_skip_blank_line_layout(layouts, idx) {
                continue;
            }
            if idx > 0 {
                out.push('\n');
            }

            if layout.preserve_raw || layout.kind == LineLayoutKind::Verbatim {
                out.push_str(layout.raw);
                continue;
            }

            match layout.kind {
                LineLayoutKind::Blank => {}
                LineLayoutKind::Code
                | LineLayoutKind::CommentOnly
                | LineLayoutKind::CommaOnly
                | LineLayoutKind::Verbatim => {
                    let depth_indent = layout.final_depth * 4;
                    let render_indent =
                        if Self::line_preserves_existing_odd_hanging_indent(layouts, idx) {
                            layout.existing_indent_spaces
                        } else {
                            depth_indent
                        };
                    out.push_str(&" ".repeat(render_indent));
                    out.push_str(layout.trimmed);
                }
            }
        }

        out
    }

    fn should_skip_blank_line_layout(layouts: &[LineLayout<'_>], idx: usize) -> bool {
        let Some(layout) = layouts.get(idx) else {
            return false;
        };
        if layout.kind != LineLayoutKind::Blank {
            return false;
        }

        let previous_code_idx = (0..idx)
            .rev()
            .find(|candidate| layouts[*candidate].kind == LineLayoutKind::Code);
        let next_code_idx = ((idx + 1)..layouts.len())
            .find(|candidate| layouts[*candidate].kind == LineLayoutKind::Code);
        let (Some(prev_idx), Some(next_idx)) = (previous_code_idx, next_code_idx) else {
            return false;
        };

        let previous = &layouts[prev_idx];
        let next = &layouts[next_idx];
        let next_non_blank_idx = ((idx + 1)..layouts.len())
            .find(|candidate| layouts[*candidate].kind != LineLayoutKind::Blank);
        if next_non_blank_idx.is_some_and(|candidate| {
            matches!(
                layouts[candidate].kind,
                LineLayoutKind::CommentOnly | LineLayoutKind::CommaOnly
            )
        }) && Self::line_ends_with_comma_before_inline_comment(previous.trimmed)
            && previous.query_base_depth.is_some()
            && previous.query_base_depth == next.query_base_depth
            && previous.final_depth <= next.final_depth
        {
            return true;
        }

        let next_upper = next.trimmed.to_ascii_uppercase();
        if !crate::sql_text::starts_with_keyword_token(&next_upper, "CASE") {
            return false;
        }
        if !Self::line_ends_with_comma_before_inline_comment(previous.trimmed) {
            return false;
        }

        previous.query_base_depth.is_some()
            && next.query_base_depth.is_some()
            && previous.final_depth <= next.final_depth
    }

    fn line_is_comment_only_with_block_state(line: &str, in_block_comment: &mut bool) -> bool {
        let trimmed = line.trim_start();
        if trimmed.is_empty() {
            return false;
        }
        if !*in_block_comment && Self::is_sqlplus_comment_line(trimmed) {
            return true;
        }

        let bytes = trimmed.as_bytes();
        let mut idx = 0usize;
        while idx < bytes.len() {
            if *in_block_comment {
                let mut closed = false;
                while idx + 1 < bytes.len() {
                    if bytes[idx] == b'*' && bytes[idx + 1] == b'/' {
                        *in_block_comment = false;
                        idx += 2;
                        closed = true;
                        break;
                    }
                    idx += 1;
                }
                if !closed {
                    return true;
                }
                continue;
            }

            while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                idx += 1;
            }

            if idx >= bytes.len() {
                return true;
            }

            let tail = trimmed.get(idx..).unwrap_or_default();
            if Self::is_sqlplus_comment_line(tail) {
                return true;
            }

            if idx + 1 < bytes.len() && bytes[idx] == b'/' && bytes[idx + 1] == b'*' {
                *in_block_comment = true;
                idx += 2;
                continue;
            }

            return false;
        }

        true
    }

    fn is_dml_clause_starter(trimmed_upper: &str) -> bool {
        crate::sql_text::starts_with_format_layout_clause(trimmed_upper)
    }

    fn starts_with_condition_keyword(trimmed_upper: &str) -> bool {
        crate::sql_text::starts_with_keyword_token(trimmed_upper, "AND")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "OR")
    }

    fn is_into_continuation_ender(trimmed_upper: &str) -> bool {
        crate::sql_text::starts_with_keyword_token(trimmed_upper, "FROM")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "WHERE")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "GROUP")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "ORDER")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "CONNECT")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "HAVING")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "UNION")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "INTERSECT")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "MINUS")
    }

    fn is_match_recognize_subclause(trimmed_upper: &str) -> bool {
        trimmed_upper.starts_with("PARTITION BY")
            || trimmed_upper.starts_with("ORDER BY")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "MEASURES")
            || trimmed_upper.starts_with("ONE ROW PER MATCH")
            || trimmed_upper.starts_with("ALL ROWS PER MATCH")
            || trimmed_upper.starts_with("AFTER MATCH SKIP")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "PATTERN")
            || crate::sql_text::starts_with_keyword_token(trimmed_upper, "DEFINE")
    }

    fn fetch_into_has_multiple_targets(tokens: &[SqlToken], into_idx: usize) -> bool {
        let mut paren_depth = 0usize;
        let mut lookahead = into_idx.saturating_add(1);

        while lookahead < tokens.len() {
            match &tokens[lookahead] {
                SqlToken::Comment(comment) if comment.contains('\n') => {}
                SqlToken::Comment(_) => {}
                SqlToken::Symbol(sym) => {
                    for ch in sym.chars() {
                        match ch {
                            '(' => paren_depth = paren_depth.saturating_add(1),
                            ')' => paren_depth = paren_depth.saturating_sub(1),
                            ',' if paren_depth == 0 => return true,
                            ';' if paren_depth == 0 => return false,
                            _ => {}
                        }
                    }
                }
                SqlToken::Word(word) if paren_depth == 0 => {
                    if word.eq_ignore_ascii_case("LIMIT")
                        || word.eq_ignore_ascii_case("BULK")
                        || word.eq_ignore_ascii_case("FROM")
                        || word.eq_ignore_ascii_case("WHERE")
                    {
                        return false;
                    }
                }
                _ => {}
            }

            lookahead = lookahead.saturating_add(1);
        }

        false
    }

    fn multiline_string_continuation_lines(formatted: &str, line_count: usize) -> Vec<bool> {
        let mut continuation_lines = vec![false; line_count];
        if line_count == 0 {
            return continuation_lines;
        }

        // Use byte-based scanning per Rust String Policy.
        // All delimiters checked here are ASCII, so byte comparison is safe.
        let bytes = formatted.as_bytes();
        let mut i = 0usize;
        let mut line = 0usize;

        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false;
        let mut in_q_quote = false;
        let mut q_quote_end: Option<u8> = None;

        while i < bytes.len() {
            let c = bytes[i];
            let next = bytes.get(i + 1).copied();

            if in_line_comment {
                if c == b'\n' {
                    in_line_comment = false;
                    line += 1;
                }
                i += 1;
                continue;
            }

            if in_block_comment {
                if c == b'*' && next == Some(b'/') {
                    in_block_comment = false;
                    i += 2;
                    continue;
                }
                if c == b'\n' {
                    line += 1;
                }
                i += 1;
                continue;
            }

            if in_q_quote {
                if Some(c) == q_quote_end && next == Some(b'\'') {
                    in_q_quote = false;
                    q_quote_end = None;
                    i += 2;
                    continue;
                }
                if c == b'\n' {
                    if line + 1 < line_count {
                        continuation_lines[line + 1] = true;
                    }
                    line += 1;
                }
                i += 1;
                continue;
            }

            if in_single_quote {
                if c == b'\'' {
                    if next == Some(b'\'') {
                        i += 2;
                        continue;
                    }
                    in_single_quote = false;
                    i += 1;
                    continue;
                }
                if c == b'\n' {
                    if line + 1 < line_count {
                        continuation_lines[line + 1] = true;
                    }
                    line += 1;
                }
                i += 1;
                continue;
            }

            if in_double_quote {
                if c == b'"' {
                    if next == Some(b'"') {
                        i += 2;
                        continue;
                    }
                    in_double_quote = false;
                    i += 1;
                    continue;
                }
                if c == b'\n' {
                    if line + 1 < line_count {
                        continuation_lines[line + 1] = true;
                    }
                    line += 1;
                }
                i += 1;
                continue;
            }

            if c == b'\n' {
                line += 1;
                i += 1;
                continue;
            }

            if c == b'-' && next == Some(b'-') {
                in_line_comment = true;
                i += 2;
                continue;
            }

            if c == b'/' && next == Some(b'*') {
                in_block_comment = true;
                i += 2;
                continue;
            }

            if (c == b'n' || c == b'N')
                && matches!(next, Some(b'q') | Some(b'Q'))
                && bytes.get(i + 2) == Some(&b'\'')
                && bytes.get(i + 3).is_some()
            {
                let delimiter = bytes[i + 3];
                in_q_quote = true;
                q_quote_end = Some(sql_text::q_quote_closing(delimiter as char) as u8);
                i += 4;
                continue;
            }

            if (c == b'q' || c == b'Q') && next == Some(b'\'') && bytes.get(i + 2).is_some() {
                let delimiter = bytes[i + 2];
                in_q_quote = true;
                q_quote_end = Some(sql_text::q_quote_closing(delimiter as char) as u8);
                i += 3;
                continue;
            }

            if c == b'\'' {
                in_single_quote = true;
                i += 1;
                continue;
            }

            if c == b'"' {
                in_double_quote = true;
                i += 1;
                continue;
            }

            i += 1;
        }

        continuation_lines
    }

    fn line_ends_with_open_paren_before_inline_comment(line: &str) -> bool {
        let tokens = super::query_text::tokenize_sql(line);
        for token in tokens.iter().rev() {
            match token {
                SqlToken::Comment(_) => continue,
                SqlToken::Symbol(sym) => {
                    let trailing_symbol = sym.trim_end();
                    return trailing_symbol.ends_with('(');
                }
                _ => return false,
            }
        }

        line.trim_end().ends_with('(')
    }

    fn line_ends_with_comma_before_inline_comment(line: &str) -> bool {
        let tokens = super::query_text::tokenize_sql(line);
        for token in tokens.iter().rev() {
            match token {
                SqlToken::Comment(_) => continue,
                SqlToken::Symbol(sym) => {
                    let trailing_symbol = sym.trim_end();
                    return trailing_symbol.ends_with(',');
                }
                _ => return false,
            }
        }

        false
    }

    fn line_is_standalone_open_paren_before_inline_comment(line: &str) -> bool {
        let mut non_comment_tokens = super::query_text::tokenize_sql(line)
            .into_iter()
            .filter(|token| !matches!(token, SqlToken::Comment(_)));

        matches!(
            (non_comment_tokens.next(), non_comment_tokens.next()),
            (Some(SqlToken::Symbol(sym)), None) if sym.trim() == "("
        )
    }

    fn line_ends_with_assignment_before_inline_comment(line: &str) -> bool {
        let tokens = super::query_text::tokenize_sql(line);
        for token in tokens.iter().rev() {
            match token {
                SqlToken::Comment(_) => continue,
                SqlToken::Symbol(sym) => {
                    return sym.trim_end().ends_with(":=");
                }
                _ => return false,
            }
        }

        false
    }

    fn line_ends_with_then_before_inline_comment(line: &str) -> bool {
        let tokens = super::query_text::tokenize_sql(line);
        for token in tokens.iter().rev() {
            match token {
                SqlToken::Comment(_) => continue,
                SqlToken::Word(word) => return word.eq_ignore_ascii_case("THEN"),
                _ => return false,
            }
        }

        false
    }

    fn line_has_unclosed_open_paren_before_inline_comment(line: &str) -> bool {
        let tokens = super::query_text::tokenize_sql(line);
        let mut paren_balance = 0usize;

        for token in tokens {
            if let SqlToken::Symbol(sym) = token {
                for ch in sym.chars() {
                    match ch {
                        '(' => paren_balance = paren_balance.saturating_add(1),
                        ')' => paren_balance = paren_balance.saturating_sub(1),
                        _ => {}
                    }
                }
            }
        }

        paren_balance > 0
    }

    /// Returns `true` when `line` contains a CASE keyword that is not closed
    /// by a matching END on the same line.  This detects mid-line CASE
    /// expressions such as `SELECT col, CASE` or `SET col = CASE` so that
    /// the formatter can open a [`DmlCaseLayoutFrame`] even when the CASE
    /// keyword does not start the line.
    fn line_has_trailing_unclosed_case(line: &str) -> bool {
        let tokens = super::query_text::tokenize_sql(line);
        let mut open_cases = 0usize;
        let mut prev_was_end = false;

        for token in &tokens {
            if let SqlToken::Word(word) = token {
                if word.eq_ignore_ascii_case("CASE") {
                    if prev_was_end {
                        // `END CASE` — this closes a CASE, not opens one.
                        open_cases = open_cases.saturating_sub(1);
                        prev_was_end = false;
                    } else {
                        open_cases += 1;
                    }
                } else {
                    if word.eq_ignore_ascii_case("END") {
                        // Bare END (without CASE qualifier) also closes a CASE.
                        open_cases = open_cases.saturating_sub(1);
                    }
                    prev_was_end = word.eq_ignore_ascii_case("END");
                }
            } else {
                prev_was_end = false;
            }
        }

        open_cases > 0
    }

    fn line_contains_dml_case_condition_terminator(
        tokens: &[SqlToken],
        initial_paren_depth: usize,
        target_paren_depth: usize,
    ) -> bool {
        let mut paren_depth = initial_paren_depth;
        let mut open_cases = 0usize;
        let mut pending_end = false;

        for token in tokens {
            match token {
                SqlToken::Comment(_) => continue,
                SqlToken::Word(word) => {
                    let upper = word.to_ascii_uppercase();

                    if pending_end {
                        if upper.eq("CASE") {
                            open_cases = open_cases.saturating_sub(1);
                            pending_end = false;
                            continue;
                        }
                        open_cases = open_cases.saturating_sub(1);
                        pending_end = false;
                    }

                    if upper.eq("THEN") && paren_depth == target_paren_depth && open_cases == 0 {
                        return true;
                    }

                    if upper.eq("CASE") {
                        open_cases = open_cases.saturating_add(1);
                    } else if upper.eq("END") {
                        pending_end = true;
                    }
                }
                SqlToken::Symbol(symbol) => {
                    if pending_end {
                        open_cases = open_cases.saturating_sub(1);
                        pending_end = false;
                    }

                    for ch in symbol.chars() {
                        match ch {
                            '(' => {
                                paren_depth = paren_depth.saturating_add(1);
                            }
                            ')' => {
                                paren_depth = paren_depth.saturating_sub(1);
                            }
                            _ => {}
                        }
                    }
                }
                _ => {
                    if pending_end {
                        open_cases = open_cases.saturating_sub(1);
                        pending_end = false;
                    }
                }
            }
        }

        false
    }

    fn line_is_cte_definition_header(line: &str) -> bool {
        let trimmed = line.trim();
        if trimmed.is_empty() || !Self::line_ends_with_open_paren_before_inline_comment(trimmed) {
            return false;
        }

        let upper = trimmed.to_ascii_uppercase();
        if upper.starts_with("WITH ") {
            return upper.contains(" AS ");
        }

        upper.contains(" AS (")
    }

    fn line_starts_with_using_clause(line: &str) -> bool {
        let trimmed_upper = line.trim_start().to_ascii_uppercase();
        crate::sql_text::starts_with_keyword_token(&trimmed_upper, "USING")
    }

    fn line_starts_multiline_clause_block(line: &str) -> bool {
        if !Self::line_ends_with_open_paren_before_inline_comment(line) {
            return false;
        }

        super::query_text::tokenize_sql(line)
            .into_iter()
            .any(|token| match token {
                SqlToken::Word(word) => {
                    word.eq_ignore_ascii_case("MATCH_RECOGNIZE")
                        || word.eq_ignore_ascii_case("PIVOT")
                        || word.eq_ignore_ascii_case("UNPIVOT")
                        || word.eq_ignore_ascii_case("WINDOW")
                }
                _ => false,
            })
    }

    fn paren_starts_first_clause_list_item(
        current_clause: Option<&str>,
        prev_word_upper: Option<&str>,
        is_query_paren: bool,
    ) -> bool {
        matches!(
            (current_clause, prev_word_upper),
            (Some("SELECT"), Some("SELECT"))
        ) || (is_query_paren
            && matches!(
                (current_clause, prev_word_upper),
                (Some("GROUP" | "ORDER"), Some("BY"))
            ))
    }

    fn line_significant_paren_counts(line: &str) -> (usize, usize) {
        super::query_text::tokenize_sql(line).into_iter().fold(
            (0usize, 0usize),
            |(open_count, close_count), token| {
                if let SqlToken::Symbol(symbol) = token {
                    match symbol.as_str() {
                        "(" => (open_count.saturating_add(1), close_count),
                        ")" => (open_count, close_count.saturating_add(1)),
                        _ => (open_count, close_count),
                    }
                } else {
                    (open_count, close_count)
                }
            },
        )
    }

    fn comment_keeps_next_line_continuation(tokens: &[SqlToken], idx: usize) -> bool {
        let significant_tokens: Vec<&SqlToken> = tokens[..idx]
            .iter()
            .filter(|token| !matches!(token, SqlToken::Comment(_)))
            .collect();

        let Some(last_token) = significant_tokens.last().copied() else {
            return false;
        };

        if matches!(
            last_token,
            SqlToken::Symbol(symbol)
                if matches!(
                    symbol.as_str(),
                    "=" | "<"
                        | ">"
                        | "<="
                        | ">="
                        | "<>"
                        | "!="
                        | "+"
                        | "-"
                        | "*"
                        | "/"
                        | "%"
                        | "||"
                        | "^"
                        | ":="
                        | "=>"
                )
        ) {
            return true;
        }

        let SqlToken::Word(last_word) = last_token else {
            return false;
        };
        let last_upper = last_word.to_ascii_uppercase();
        if crate::sql_text::is_format_comment_continuation_keyword(last_upper.as_str()) {
            return true;
        }

        let significant_words: Vec<&str> = significant_tokens
            .iter()
            .filter_map(|token| match token {
                SqlToken::Word(word) => Some(word.as_str()),
                _ => None,
            })
            .collect();

        if significant_words.len() >= 2 {
            let previous_upper =
                significant_words[significant_words.len().saturating_sub(2)].to_ascii_uppercase();
            if matches!(
                (previous_upper.as_str(), last_upper.as_str()),
                ("GROUP", "BY")
                    | ("ORDER", "BY")
                    | ("CONNECT", "BY")
                    | ("PARTITION", "BY")
                    | ("DIMENSION", "BY")
                    | ("START", "WITH")
            ) {
                return true;
            }

            if matches!(previous_upper.as_str(), "BETWEEN" | "OF")
                && crate::sql_text::is_format_temporal_boundary_keyword(last_upper.as_str())
            {
                return true;
            }

            if FORMAT_JOIN_MODIFIER_KEYWORDS.contains(&previous_upper.as_str())
                && last_upper == "JOIN"
            {
                return true;
            }

            if previous_upper == "SELECT"
                && matches!(last_upper.as_str(), "DISTINCT" | "UNIQUE" | "ALL")
            {
                return true;
            }
        }

        false
    }

    #[cfg(test)]
    fn is_plsql_like_tokens(statement: &str, tokens: &[SqlToken]) -> bool {
        let words: Vec<&str> = tokens
            .iter()
            .filter_map(|token| match token {
                SqlToken::Word(word) => Some(word.as_str()),
                _ => None,
            })
            .collect();

        if let Some(first) = words.first().copied() {
            if first.eq_ignore_ascii_case("SELECT")
                || first.eq_ignore_ascii_case("INSERT")
                || first.eq_ignore_ascii_case("UPDATE")
                || first.eq_ignore_ascii_case("DELETE")
                || first.eq_ignore_ascii_case("MERGE")
            {
                return false;
            }
            if first.eq_ignore_ascii_case("WITH") {
                let mut next_index = 1usize;
                if words
                    .get(next_index)
                    .is_some_and(|word| word.eq_ignore_ascii_case("RECURSIVE"))
                {
                    next_index += 1;
                }
                if words.get(next_index).is_some_and(|word| {
                    word.eq_ignore_ascii_case("FUNCTION") || word.eq_ignore_ascii_case("PROCEDURE")
                }) {
                    return true;
                }
                return false;
            }
        }

        for word in &words {
            if word.eq_ignore_ascii_case("BEGIN") || word.eq_ignore_ascii_case("DECLARE") {
                return true;
            }
            if word.eq_ignore_ascii_case("CREATE") {
                let object_type = Self::parse_ddl_object_type(statement);
                return matches!(
                    object_type,
                    "Procedure"
                        | "Function"
                        | "Package"
                        | "Package Body"
                        | "Type"
                        | "Type Body"
                        | "Trigger"
                );
            }
        }

        Self::is_plsql_control_fragment(&words)
    }

    #[cfg(test)]
    fn is_plsql_control_fragment(words: &[&str]) -> bool {
        if words.is_empty() {
            return false;
        }

        let first = words[0];
        if first.eq_ignore_ascii_case("IF")
            && words.iter().any(|word| word.eq_ignore_ascii_case("THEN"))
        {
            return true;
        }

        if first.eq_ignore_ascii_case("ELSIF") || first.eq_ignore_ascii_case("ELSEIF") {
            return true;
        }

        if first.eq_ignore_ascii_case("WHILE")
            && words.iter().any(|word| word.eq_ignore_ascii_case("LOOP"))
        {
            return true;
        }

        words.windows(2).any(|pair| {
            pair[0].eq_ignore_ascii_case("END")
                && (pair[1].eq_ignore_ascii_case("IF")
                    || pair[1].eq_ignore_ascii_case("LOOP")
                    || pair[1].eq_ignore_ascii_case("CASE"))
        })
    }

    #[cfg(test)]
    fn is_plsql_like_statement(statement: &str) -> bool {
        let tokens = Self::tokenize_sql(statement);
        Self::is_plsql_like_tokens(statement, &tokens)
    }

    fn paren_opens_structured_column_list(
        prev_word_upper: Option<&str>,
        create_table_paren_expected: bool,
    ) -> bool {
        // Oracle table functions expose nested column definitions through
        // `... COLUMNS (...)`; those parentheses should format like a column
        // list instead of a generic function-argument group.
        create_table_paren_expected || matches!(prev_word_upper, Some("COLUMNS"))
    }

    #[cfg(test)]
    fn parse_ddl_object_type(statement: &str) -> &'static str {
        let upper = statement.to_uppercase();
        QueryExecutor::parse_ddl_object_type(&upper)
    }

    fn format_create_table(statement: &str) -> Option<String> {
        let trimmed = statement.trim();
        if trimmed.is_empty() {
            return None;
        }

        let tokens = Self::tokenize_sql(trimmed);
        if tokens.is_empty() {
            return None;
        }

        // Guard: only apply CREATE TABLE formatting when TABLE is the actual
        // object keyword in the CREATE header. This avoids false matches like
        // CREATE PACKAGE BODY ... TYPE ... IS TABLE OF ...
        let mut word_positions: Vec<(usize, String)> = Vec::new();
        for (idx, token) in tokens.iter().enumerate() {
            if let SqlToken::Word(word) = token {
                word_positions.push((idx, word.to_uppercase()));
            }
        }

        let create_word_idx = word_positions
            .iter()
            .position(|(_, word)| word == "CREATE")?;

        let mut header_idx = create_word_idx + 1;
        while let Some((_, word)) = word_positions.get(header_idx) {
            if matches!(
                word.as_str(),
                "OR" | "REPLACE" | "EDITIONABLE" | "NONEDITIONABLE"
            ) {
                header_idx += 1;
                continue;
            }
            break;
        }

        if (word_positions
            .get(header_idx)
            .is_some_and(|(_, word)| word == "GLOBAL")
            || word_positions
                .get(header_idx)
                .is_some_and(|(_, word)| word == "PRIVATE"))
            && word_positions
                .get(header_idx + 1)
                .is_some_and(|(_, word)| word == "TEMPORARY")
        {
            header_idx += 2;
        }

        let (_, create_object) = word_positions.get(header_idx)?;
        if create_object != "TABLE" {
            return None;
        }

        let mut seen_table = false;
        let mut ctas = false;
        let mut open_idx: Option<usize> = None;
        let mut close_idx: Option<usize> = None;
        let token_depths = paren_depths(&tokens);
        let mut idx = 0usize;

        while idx < tokens.len() {
            let token = &tokens[idx];
            match token {
                SqlToken::Word(word) => {
                    let upper = word.to_uppercase();
                    if !seen_table && upper == "TABLE" {
                        seen_table = true;
                    } else if seen_table
                        && upper == "AS"
                        && tokens[idx + 1..]
                            .iter()
                            .find_map(|t| match t {
                                SqlToken::Word(w) => Some(w.to_uppercase()),
                                _ => None,
                            })
                            .is_some_and(|w| w == "SELECT" || w == "WITH")
                    {
                        ctas = true;
                    }
                }
                SqlToken::Symbol(sym) if sym == "(" => {
                    if is_top_level_depth(&token_depths, idx)
                        && seen_table
                        && !ctas
                        && open_idx.is_none()
                    {
                        open_idx = Some(idx);
                    }
                }
                SqlToken::Symbol(sym) if sym == ")" => {
                    if is_depth(&token_depths, idx, 1) && open_idx.is_some() && close_idx.is_none()
                    {
                        close_idx = Some(idx);
                        break;
                    }
                }
                _ => {}
            }
            idx += 1;
        }

        let (open_idx, close_idx) = match (open_idx, close_idx) {
            (Some(open_idx), Some(close_idx)) => (open_idx, close_idx),
            _ => return None,
        };

        let prefix_tokens = &tokens[..open_idx];
        let column_tokens = &tokens[open_idx + 1..close_idx];
        let suffix_tokens = &tokens[close_idx + 1..];

        let mut columns: Vec<Vec<SqlToken>> = Vec::new();
        for group in split_top_level_symbol_groups(column_tokens, ",") {
            columns.push(group.into_iter().cloned().collect());
        }

        if columns.is_empty() {
            return None;
        }

        // (is_constraint, name, type_str, rest_str, leading_comments, trailing_comments)
        let mut formatted_cols: Vec<(bool, String, String, String, Vec<String>, Vec<String>)> =
            Vec::new();
        let mut max_name = 0usize;
        let mut max_type = 0usize;

        for column in &columns {
            let mut iter = column.iter().filter(|t| !matches!(t, SqlToken::Comment(_)));
            let first = iter.next();
            let is_constraint = match first {
                Some(SqlToken::Word(word)) => {
                    matches!(
                        word.to_uppercase().as_str(),
                        "CONSTRAINT" | "PRIMARY" | "UNIQUE" | "FOREIGN" | "CHECK"
                    )
                }
                _ => false,
            };

            // Separate leading comments (before first non-comment token)
            // from trailing comments (after first non-comment token)
            let mut leading_comments: Vec<String> = Vec::new();
            let mut trailing_comments: Vec<String> = Vec::new();
            let mut seen_code = false;
            for token in column {
                match token {
                    SqlToken::Comment(comment) => {
                        let body = comment.strip_prefix('\n').unwrap_or(comment);
                        let trimmed = body.trim_end_matches('\n');
                        if !trimmed.is_empty() {
                            if seen_code {
                                trailing_comments.push(trimmed.to_string());
                            } else {
                                leading_comments.push(trimmed.to_string());
                            }
                        }
                    }
                    _ => {
                        seen_code = true;
                    }
                }
            }

            if is_constraint {
                let non_comment_tokens: Vec<SqlToken> = column
                    .iter()
                    .filter(|t| !matches!(t, SqlToken::Comment(_)))
                    .cloned()
                    .collect();
                let text = Self::join_tokens_spaced(&non_comment_tokens, 0);
                formatted_cols.push((
                    true,
                    text,
                    String::new(),
                    String::new(),
                    leading_comments,
                    trailing_comments,
                ));
                continue;
            }

            let mut tokens_iter = column
                .iter()
                .filter(|t| !matches!(t, SqlToken::Comment(_)))
                .peekable();
            let name_token = tokens_iter.next();
            let name = name_token.map(Self::token_text).unwrap_or_default();

            let mut type_tokens: Vec<SqlToken> = Vec::new();
            let mut rest_tokens: Vec<SqlToken> = Vec::new();
            let mut in_type = true;

            for token in tokens_iter {
                let is_constraint_token = match token {
                    SqlToken::Word(word) => {
                        sql_text::is_format_column_constraint_keyword(word.as_str())
                    }
                    _ => false,
                };
                if in_type && is_constraint_token {
                    in_type = false;
                }
                if in_type {
                    type_tokens.push(token.clone());
                } else {
                    rest_tokens.push(token.clone());
                }
            }

            let type_str = Self::join_tokens_compact(&type_tokens);
            let rest_str = Self::join_tokens_spaced(&rest_tokens, 0);

            max_name = max_name.max(name.len());
            max_type = max_type.max(type_str.len());
            formatted_cols.push((
                false,
                name,
                type_str,
                rest_str,
                leading_comments,
                trailing_comments,
            ));
        }

        let mut out = String::new();
        let prefix = Self::join_tokens_spaced(prefix_tokens, 0);
        out.push_str(prefix.trim_end());
        out.push_str(" (\n");

        let indent = " ".repeat(4);
        for (idx, (is_constraint, name, type_str, rest_str, leading_comments, trailing_comments)) in
            formatted_cols.into_iter().enumerate()
        {
            // Output leading comments (originally before the column)
            for comment in &leading_comments {
                out.push_str(&indent);
                out.push_str(comment);
                out.push('\n');
            }
            out.push_str(&indent);
            if is_constraint {
                out.push_str(&name);
            } else {
                let name_pad = max_name.saturating_sub(name.len());
                let type_pad = max_type.saturating_sub(type_str.len());
                out.push_str(&name);
                if !type_str.is_empty() {
                    out.push_str(&" ".repeat(name_pad + 1));
                    out.push_str(&type_str);
                    if !rest_str.is_empty() {
                        out.push_str(&" ".repeat(type_pad + 1));
                        out.push_str(&rest_str);
                    }
                }
            }
            if idx + 1 < columns.len() {
                out.push(',');
            }
            out.push('\n');
            // Output trailing comments (originally after the column definition)
            for comment in &trailing_comments {
                out.push_str(&indent);
                out.push_str(comment);
                out.push('\n');
            }
        }
        out.push(')');

        let suffix = Self::format_create_suffix(suffix_tokens);
        if !suffix.is_empty() {
            out.push('\n');
            out.push_str(&suffix);
        }

        Some(out.trim_end().to_string())
    }

    fn token_text(token: &SqlToken) -> String {
        match token {
            SqlToken::Word(word) => {
                let upper = word.to_uppercase();
                if sql_text::is_oracle_sql_keyword(upper.as_str()) {
                    upper
                } else {
                    word.clone()
                }
            }
            SqlToken::String(literal) => literal.clone(),
            SqlToken::Comment(comment) => comment.clone(),
            SqlToken::Symbol(sym) => sym.clone(),
        }
    }

    fn token_is_word_like(token: &SqlToken) -> bool {
        matches!(token, SqlToken::Word(_))
    }

    fn is_plsql_attribute_prefix(sym: &str, next_token: Option<&SqlToken>) -> bool {
        sym == "%" && next_token.map(Self::token_is_word_like).unwrap_or(false)
    }

    fn join_tokens_compact(tokens: &[SqlToken]) -> String {
        let mut out = String::new();
        let mut needs_space = false;
        for (idx, token) in tokens.iter().enumerate() {
            let text = Self::token_text(token);
            match token {
                SqlToken::Symbol(sym) if sym == "(" => {
                    out.push_str(&text);
                    needs_space = false;
                }
                SqlToken::Symbol(sym) if sym == ")" => {
                    out.push_str(&text);
                    needs_space = true;
                }
                SqlToken::Symbol(sym) if sym == "," => {
                    out.push_str(&text);
                    out.push(' ');
                    needs_space = false;
                }
                SqlToken::Symbol(sym)
                    if Self::is_plsql_attribute_prefix(sym, tokens.get(idx + 1)) =>
                {
                    out.push_str(&text);
                    needs_space = false;
                }
                _ => {
                    if needs_space {
                        out.push(' ');
                    }
                    out.push_str(&text);
                    needs_space = true;
                }
            }
        }
        out.trim().to_string()
    }

    fn join_tokens_spaced(tokens: &[SqlToken], indent_level: usize) -> String {
        let mut out = String::new();
        let mut needs_space = false;
        let indent = " ".repeat(indent_level * 4);
        let mut at_line_start = true;

        for (idx, token) in tokens.iter().enumerate() {
            let text = Self::token_text(token);
            match token {
                SqlToken::Comment(comment) => {
                    if !at_line_start {
                        out.push(' ');
                    } else if !indent.is_empty() {
                        out.push_str(&indent);
                    }
                    out.push_str(comment);
                    if comment.ends_with('\n') {
                        at_line_start = true;
                        needs_space = false;
                    } else {
                        at_line_start = false;
                        needs_space = true;
                    }
                }
                SqlToken::Symbol(sym) if sym == "." => {
                    out.push('.');
                    needs_space = false;
                    at_line_start = false;
                }
                SqlToken::Symbol(sym) if sym == "(" => {
                    out.push('(');
                    needs_space = false;
                    at_line_start = false;
                }
                SqlToken::Symbol(sym) if sym == ")" => {
                    out.push(')');
                    needs_space = true;
                    at_line_start = false;
                }
                SqlToken::Symbol(sym) if sym == "," => {
                    out.push(',');
                    out.push(' ');
                    needs_space = false;
                    at_line_start = false;
                }
                SqlToken::Symbol(sym)
                    if Self::is_plsql_attribute_prefix(sym, tokens.get(idx + 1)) =>
                {
                    out.push('%');
                    needs_space = false;
                    at_line_start = false;
                }
                SqlToken::Symbol(sym) => {
                    if needs_space {
                        out.push(' ');
                    }
                    out.push_str(sym);
                    needs_space = true;
                    at_line_start = false;
                }
                _ => {
                    if at_line_start && !indent.is_empty() {
                        out.push_str(&indent);
                    }
                    if needs_space {
                        out.push(' ');
                    }
                    out.push_str(&text);
                    needs_space = true;
                    at_line_start = false;
                }
            }
        }

        out.trim().to_string()
    }

    fn format_create_suffix(tokens: &[SqlToken]) -> String {
        if tokens.is_empty() {
            return String::new();
        }

        let break_keywords = FORMAT_CREATE_SUFFIX_BREAK_KEYWORDS;

        let mut parts: Vec<Vec<SqlToken>> = Vec::new();

        for part in split_top_level_keyword_groups(tokens, break_keywords) {
            parts.push(part.into_iter().cloned().collect());
        }

        let mut out = String::new();
        for (idx, part) in parts.iter().enumerate() {
            if idx > 0 {
                out.push('\n');
            }
            out.push_str(&Self::join_tokens_spaced(part, 0));
        }
        out.trim().to_string()
    }

    /// 토크나이저는 공통 로직(`query_text`)로 위임합니다.
    pub(crate) fn tokenize_sql(sql: &str) -> Vec<SqlToken> {
        super::query_text::tokenize_sql(sql)
    }

    pub(super) fn escape_sql_literal(value: &str) -> String {
        value.replace('\'', "''")
    }
}

#[cfg(test)]
mod formatter_regression_tests {
    use super::SqlEditorWidget;
    use crate::db::{FormatItem, QueryExecutor, ScriptItem, SessionState};
    use crate::ui::sql_editor::execution::PROGRESS_ROWS_INITIAL_BATCH;
    use crate::ui::sql_editor::QueryProgress;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{mpsc, Arc, Mutex};
    use std::time::Duration;

    fn load_formatter_test_file(name: &str) -> String {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("test");
        path.push(name);
        fs::read_to_string(path).unwrap_or_default()
    }

    fn count_statement_items(items: &[ScriptItem]) -> usize {
        items
            .iter()
            .filter(|item| matches!(item, ScriptItem::Statement(_)))
            .count()
    }

    fn count_tool_command_items(items: &[ScriptItem]) -> usize {
        items
            .iter()
            .filter(|item| matches!(item, ScriptItem::ToolCommand(_)))
            .count()
    }

    #[test]
    fn does_not_force_select_line_break_after_malformed_statement() {
        let sql = "select fn(a, b;\nselect x, y from dual;";
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("SELECT x,\n    y\nFROM DUAL;"),
            "Subsequent valid statement should return to normal comma layout, got: {}",
            formatted
        );
        assert!(
            !formatted.contains("SELECT\n    x,\n    y\nFROM DUAL;"),
            "Malformed prior statement must not force legacy recovery layout on next statement, got: {}",
            formatted
        );
    }

    #[test]
    fn malformed_statement_resets_paren_tracking_before_following_statement() {
        let sql = "SELECT fn(a, b;\nSELECT c, d, e FROM dual;";
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("SELECT c,\n    d,\n    e\nFROM DUAL;"),
            "Following statement should format with normal SELECT-list wrapping, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("SELECT\n    c,\n    d,\n    e\nFROM DUAL;"),
            "Formatter should not keep stale malformed-state recovery layout, got:\n{}",
            formatted
        );
    }

    #[test]
    fn comments_do_not_change_paren_tracking_state() {
        let sql = "select a, /* comment with (, ), and , */ b from dual;";
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(formatted.contains("/* comment with (, ), and , */"));
        assert!(
            formatted
                .contains("SELECT\n    a,\n    /* comment with (, ), and , */\n    b\nFROM DUAL;")
                || formatted
                    .contains("SELECT a,\n    /* comment with (, ), and , */\n    b\nFROM DUAL;"),
            "Comment-preserving select formatting should remain stable, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_preserves_prompt_banner_lines_verbatim() {
        let sql =
            "PROMPT =======================================================================\n\
PROMPT [END] If you saw outputs + cursor print + summary selects, parsing/execution is OK\n\
PROMPT =======================================================================";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert_eq!(formatted, sql);
    }

    #[test]
    fn format_sql_basic_preserves_prompt_lines_with_original_case_and_indentation() {
        let sql = "  prompt first line\n\
prompt second line\n\
    PROMPT third line";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert_eq!(formatted, sql);

        let formatted_again = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(formatted_again, sql);
    }

    #[test]
    fn keeps_multiline_string_continuation_lines_without_depth_reindent() {
        let sql = "BEGIN
DBMS_OUTPUT.PUT_LINE('first line
second line
third line');
END;";
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(formatted.contains("DBMS_OUTPUT.PUT_LINE ('first line\nsecond line\nthird line');"));
    }

    #[test]
    fn keeps_ampersand_substitution_variables_together() {
        let sql = "SELECT &&pp FROM dual";
        let formatted = SqlEditorWidget::format_sql_basic(sql);
        assert!(
            formatted.contains("&&pp"),
            "&&pp should stay together, got: {}",
            formatted
        );

        let sql2 = "SELECT &var1 FROM dual";
        let formatted2 = SqlEditorWidget::format_sql_basic(sql2);
        assert!(
            formatted2.contains("&var1"),
            "&var1 should stay together, got: {}",
            formatted2
        );
    }

    #[test]
    fn keeps_merge_into_together() {
        let sql = "MERGE INTO target_table t USING source_table s ON (t.id = s.id) WHEN MATCHED THEN UPDATE SET t.name = s.name";
        let formatted = SqlEditorWidget::format_sql_basic(sql);
        assert!(
            formatted.contains("MERGE INTO target_table"),
            "MERGE INTO should stay on the same line, got: {}",
            formatted
        );
    }

    #[test]
    fn keeps_start_with_together() {
        let sql = "SELECT employee_id, manager_id FROM employees START WITH manager_id IS NULL CONNECT BY PRIOR employee_id = manager_id";
        let formatted = SqlEditorWidget::format_sql_basic(sql);
        assert!(
            formatted.contains("START WITH"),
            "START WITH should stay on the same line, got: {}",
            formatted
        );
    }

    #[test]
    fn formats_where_in_subquery_with_deep_indent_and_alias() {
        let source = "select a.topic, a.TOPIC from help a where a.SEQ in (select seq from help) b";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);

        assert_eq!(
            preserved.trim_end(),
            "SELECT a.topic,\n    a.TOPIC\nFROM help a\nWHERE a.SEQ IN (\n        SELECT seq\n        FROM help\n    ) b"
        );
    }

    #[test]
    fn deeply_nested_subqueries_keep_progressive_depth_indentation() {
        let source = r#"BEGIN
  SELECT col1
  INTO v_col
  FROM t1
  WHERE EXISTS (
    SELECT 1
    FROM t2
    WHERE t2.id IN (
      SELECT t3.id
      FROM t3
      WHERE t3.flag = 'Y'
    )
  );
END;"#;

        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            formatted.contains("WHERE EXISTS (\n            SELECT 1"),
            "first nested subquery should stay indented under EXISTS, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("WHERE t2.id IN (\n                SELECT t3.id"),
            "second nested subquery should stay indented under IN, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("WHERE t3.flag = 'Y'\n            )"),
            "closing parenthesis should dedent one level from deepest query body, got:\n{}",
            formatted
        );
    }

    #[test]
    fn keeps_repeat_block_as_single_indented_block() {
        let sql = r#"BEGIN
  REPEAT
    DBMS_OUTPUT.PUT_LINE('start');
    i := i + 1;
  UNTIL i >= 3
  END REPEAT;
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("END REPEAT;"),
            "REPEAT block terminator should stay on a single line, got: {}",
            formatted
        );

        let repeat_end_line = formatted
            .lines()
            .find(|line| line.trim().starts_with("END REPEAT;"))
            .expect("formatted output should contain END REPEAT line");
        let end_line = formatted.lines().find(|line| line.trim() == "END");

        assert!(
            end_line.unwrap_or("    ").starts_with("    "),
            "END should be indented"
        );
        assert!(
            formatted.contains("DBMS_OUTPUT.PUT_LINE"),
            "REPEAT body should remain present, got: {}",
            formatted
        );
        assert!(
            repeat_end_line.starts_with("    "),
            "END REPEAT should match block indent"
        );

        let formatted_again = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            formatted, formatted_again,
            "Formatting should be idempotent for REPEAT blocks"
        );
    }

    #[test]
    fn tab_off_keeps_tab_character_in_script_output() {
        let line = "A\tB";
        let rendered = SqlEditorWidget::format_script_output_line(line, false, false);
        assert_eq!(rendered, "A\tB");
    }

    #[test]
    fn tab_on_expands_tab_character_in_script_output() {
        let line = "A\tB";
        let rendered = SqlEditorWidget::format_script_output_line(line, false, true);
        assert_eq!(rendered, "A       B");
    }

    #[test]
    fn nested_case_expression_in_plsql_aligns_else_correctly() {
        let sql = r#"CREATE OR REPLACE PACKAGE BODY oqt_mega_pkg AS
FUNCTION f_deep(p_grp IN NUMBER, p_n IN NUMBER, p_txt IN VARCHAR2) RETURN NUMBER IS
  v NUMBER := 0;
BEGIN
  v :=
    CASE
      WHEN p_grp < 0 THEN -1000
      WHEN p_grp = 0 THEN
        CASE
          WHEN p_n > 10 THEN 100
          ELSE 10
        END
      ELSE
        CASE
          WHEN INSTR(NVL(p_txt,'x'), 'END;') > 0 THEN 777
          ELSE LENGTH(NVL(p_txt,'')) + p_n
        END
    END;
  RETURN v;
END;
END oqt_mega_pkg;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        // ELSE after inner CASE END should align with outer WHEN
        assert!(
            formatted.contains("END\n            ELSE\n                CASE"),
            "ELSE should align with outer WHEN after inner CASE END, got:\n{}",
            formatted
        );

        // Outer END should close the outer CASE properly
        assert!(
            formatted.contains("END\n        END;"),
            "Outer CASE END should be properly indented, got:\n{}",
            formatted
        );

        // Idempotent
        let formatted_again = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            formatted, formatted_again,
            "Formatting should be idempotent for nested CASE expressions"
        );
    }

    #[test]
    fn package_body_named_end_with_if_prefix_is_not_treated_as_end_if_suffix() {
        let sql = r#"CREATE OR REPLACE PACKAGE BODY if_owner AS
FUNCTION run_check RETURN NUMBER IS
BEGIN
  IF 1 = 1 THEN
    RETURN 1;
  END IF;
END run_check;
BEGIN
  NULL;
END if_owner;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        let end_if_owner_line = formatted
            .lines()
            .find(|line| line.trim().eq_ignore_ascii_case("END if_owner;"));
        assert!(
            end_if_owner_line.is_some_and(|line| !line.starts_with(' ')),
            "Package named END label should close package body at top-level depth, got:
{}",
            formatted
        );

        let end_if_line = formatted
            .lines()
            .find(|line| line.trim().eq_ignore_ascii_case("END IF;"));
        assert!(
            end_if_line.is_some_and(|line| line.starts_with("        ")),
            "Nested END IF should remain more indented than END package label, got:
{}",
            formatted
        );
    }

    #[test]
    fn package_body_final_end_label_aligns_to_top_level() {
        let sql = r#"CREATE OR REPLACE PACKAGE BODY fmt_pkg_extreme AS
PROCEDURE run_extreme IS
BEGIN
  NULL;
END run_extreme;

BEGIN
  NULL;
END fmt_pkg_extreme;"#;

        let formatted = SqlEditorWidget::format_sql_basic(sql);
        let end_pkg_line = formatted
            .lines()
            .find(|line| line.trim().eq_ignore_ascii_case("END fmt_pkg_extreme;"));

        assert!(
            end_pkg_line.is_some_and(|line| !line.starts_with(' ')),
            "Final package END label should align with CREATE at top-level, got:\n{}",
            formatted
        );
    }

    #[test]
    fn package_body_initializer_begin_stays_at_top_level_after_case_declaration() {
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
        WHEN TO_CHAR (SYSDATE, 'DY', 'NLS_DATE_LANGUAGE=ENGLISH') IN ('SAT', 'SUN') THEN
            'WEEKEND_BOOT'
        ELSE
            'WEEKDAY_BOOT'
    END;
    AUDIT ('INIT', 'package initialized. mode=' || g_last_mode);
END fmt_pkg_extreme;"#;

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains(
                "END calc_mode;\nBEGIN\n    g_last_mode :=\n    CASE"
            ),
            "package body initializer BEGIN should not remain indented under declarations, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("END calc_mode;\n\n    BEGIN\n        g_last_mode :="),
            "initializer BEGIN/body should not be shifted one extra level, got:\n{}",
            formatted
        );
    }

    #[test]
    fn package_procedure_if_then_nested_begin_aligns_under_then() {
        let sql = r#"CREATE PACKAGE a AS
    PROCEDURE b (c IN VARCHAR2) IS
    BEGIN
        IF (1 = 1) THEN
        BEGIN
                SELECT * FROM d;
            END;
        END IF;
    END b;
END a;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let if_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("IF (1 = 1) THEN"))
            .unwrap_or(0);
        let begin_idx = lines
            .iter()
            .enumerate()
            .find(|(idx, line)| *idx > if_idx && line.trim_start() == "BEGIN")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("SELECT *"))
            .unwrap_or(0);
        let inner_end_idx = lines
            .iter()
            .enumerate()
            .find(|(idx, line)| *idx > select_idx && line.trim_start() == "END;")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let end_if_idx = lines
            .iter()
            .position(|line| line.trim_start() == "END IF;")
            .unwrap_or(0);

        assert!(
            indent(lines[begin_idx]) > indent(lines[if_idx]),
            "BEGIN inside IF THEN should indent deeper than IF, got:\n{}",
            formatted
        );
        assert!(
            indent(lines[select_idx]) > indent(lines[begin_idx]),
            "SELECT inside nested BEGIN should indent deeper than BEGIN, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[inner_end_idx]),
            indent(lines[begin_idx]),
            "END; for nested BEGIN should align with BEGIN, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[end_if_idx]),
            indent(lines[if_idx]),
            "END IF should align with IF, got:\n{}",
            formatted
        );
    }

    #[test]
    fn package_body_procedure_if_then_nested_begin_uses_expected_alignment() {
        let input = r#"create package body a as
    procedure b (c in varchar2) as
    begin
        if (1 = 1) then
            begin
                select * from d;
            end;
        end if;
    end b;
end a;"#;
        let expected = r#"CREATE PACKAGE BODY a AS
    PROCEDURE b (c IN VARCHAR2) AS
    BEGIN
        IF (1 = 1) THEN
            BEGIN
                SELECT *
                FROM d;
            END;
        END IF;
    END b;
END a;"#;

        let formatted = SqlEditorWidget::format_sql_basic(input);
        assert_eq!(
            formatted.trim(),
            expected.trim(),
            "package body procedure nested BEGIN alignment regression, got:\n{}",
            formatted
        );
    }

    #[test]
    fn plsql_if_condition_with_parenthesized_case_keeps_case_and_close_paren_depths_stable() {
        let input = r#"BEGIN
    IF (
        CASE
            WHEN flag = 'Y' THEN 1
            ELSE 0
        END
    ) = 1 THEN
        NULL;
    END IF;
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());
        let if_idx = lines
            .iter()
            .position(|line| line.trim_start() == "IF (")
            .unwrap_or(0);
        let case_idx = lines
            .iter()
            .position(|line| line.trim_start() == "CASE")
            .unwrap_or(0);
        let close_paren_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with(") = 1 THEN"))
            .unwrap_or(0);
        let end_if_idx = lines
            .iter()
            .position(|line| line.trim_start() == "END IF;")
            .unwrap_or(0);

        assert!(
            indent(lines[case_idx]) > indent(lines[if_idx]),
            "CASE inside IF condition should still indent deeper than the IF header, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[close_paren_idx]),
            indent(lines[if_idx]),
            "close paren line should return to the IF header depth, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[end_if_idx]),
            indent(lines[if_idx]),
            "END IF should stay aligned with IF after parenthesized CASE condition continuations, got:\n{}",
            formatted
        );
    }

    #[test]
    fn plsql_while_condition_with_parenthesized_case_keeps_case_and_close_paren_depths_stable() {
        let input = r#"BEGIN
    WHILE (
        CASE
            WHEN flag = 'Y' THEN 1
            ELSE 0
        END
    ) = 1 LOOP
        NULL;
    END LOOP;
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());
        let while_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHILE (")
            .unwrap_or(0);
        let case_idx = lines
            .iter()
            .position(|line| line.trim_start() == "CASE")
            .unwrap_or(0);
        let close_paren_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with(") = 1 LOOP"))
            .unwrap_or(0);
        let end_loop_idx = lines
            .iter()
            .position(|line| line.trim_start() == "END LOOP;")
            .unwrap_or(0);

        assert!(
            indent(lines[case_idx]) > indent(lines[while_idx]),
            "CASE inside WHILE condition should still indent deeper than the WHILE header, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[close_paren_idx]),
            indent(lines[while_idx]),
            "close paren line should return to the WHILE header depth, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[end_loop_idx]),
            indent(lines[while_idx]),
            "END LOOP should stay aligned with WHILE after parenthesized CASE condition continuations, got:\n{}",
            formatted
        );
    }

    #[test]
    fn plsql_elsif_condition_with_parenthesized_case_keeps_close_paren_depth_stable() {
        let input = r#"BEGIN
    IF flag = 'N' THEN
        NULL;
    ELSIF (
        CASE
            WHEN flag = 'Y' THEN 1
            ELSE 0
        END
    ) = 1 THEN
        NULL;
    END IF;
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());
        let elsif_idx = lines
            .iter()
            .position(|line| line.trim_start() == "ELSIF (")
            .unwrap_or(0);
        let close_paren_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with(") = 1 THEN"))
            .unwrap_or(0);
        let end_if_idx = lines
            .iter()
            .position(|line| line.trim_start() == "END IF;")
            .unwrap_or(0);

        assert_eq!(
            indent(lines[close_paren_idx]),
            indent(lines[elsif_idx]),
            "ELSIF close paren line should return to the ELSIF header depth, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[end_if_idx]),
            indent(lines[elsif_idx]),
            "END IF should stay aligned with ELSIF/IF block after parenthesized CASE continuations, got:\n{}",
            formatted
        );
    }

    #[test]
    fn plsql_if_multigroup_exists_conditions_keep_owner_depths_stable() {
        let input = r#"BEGIN
    IF EXISTS (
        SELECT 1
        FROM dual
    ) AND EXISTS (
        SELECT 1
        FROM dual
    ) THEN
        NULL;
    END IF;
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());
        let if_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("IF EXISTS ("))
            .expect("formatted output should contain IF EXISTS header");
        let and_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND EXISTS ("))
            .expect("formatted output should contain AND EXISTS continuation");
        let first_select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT 1")
            .expect("formatted output should contain first child SELECT");
        let second_select_idx = lines
            .iter()
            .enumerate()
            .skip(and_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "SELECT 1")
            .map(|(idx, _)| idx)
            .expect("formatted output should contain second child SELECT");
        let close_then_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with(") THEN"))
            .expect("formatted output should contain close THEN line");
        let end_if_idx = lines
            .iter()
            .position(|line| line.trim_start() == "END IF;")
            .expect("formatted output should contain END IF");

        assert_eq!(
            indent(lines[and_idx]),
            indent(lines[if_idx]),
            "AND/OR continuation inside the same parenthesized IF condition should stay at the IF header depth, got:\n{}",
            formatted
        );
        assert!(
            indent(lines[first_select_idx]) > indent(lines[if_idx]),
            "first EXISTS subquery should stay nested under IF, got:\n{}",
            formatted
        );
        assert!(
            indent(lines[second_select_idx]) > indent(lines[if_idx]),
            "second EXISTS subquery should stay nested under IF, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[close_then_idx]),
            indent(lines[if_idx]),
            "final close paren line should return to the IF header depth, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[end_if_idx]),
            indent(lines[if_idx]),
            "END IF should stay aligned after multi-group parenthesized conditions, got:\n{}",
            formatted
        );
    }

    #[test]
    fn searched_case_when_exists_subquery_keeps_close_paren_at_when_depth() {
        let input = r#"SELECT
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM dual
        ) THEN 1
        ELSE 0
    END AS flag
FROM dual;"#;
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());
        let when_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("WHEN EXISTS ("))
            .expect("formatted output should contain WHEN EXISTS header");
        let close_then_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with(") THEN 1"))
            .expect("formatted output should contain close THEN line");
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT 1")
            .expect("formatted output should contain EXISTS child SELECT");

        assert!(
            indent(lines[select_idx]) > indent(lines[when_idx]),
            "searched CASE EXISTS subquery should stay nested under WHEN, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[close_then_idx]),
            indent(lines[when_idx]),
            "close paren THEN line should return to the WHEN header depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_for_auto_formatting_searched_case_multiline_when_condition_uses_case_continuation_depth(
    ) {
        let source = r#"SELECT
    CASE
        WHEN e.salary >= 100000
        AND EXISTS (
            SELECT 1
            FROM qt_fmt_bonus b
            WHERE b.emp_id = e.emp_id
        ) THEN 'ELITE'
        ELSE 'OTHER'
    END AS deep_case_label
FROM qt_fmt_emp e;"#;

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, false);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let when_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHEN e.salary >= 100000")
            .expect("formatted output should contain searched CASE WHEN header");
        let and_idx = lines
            .iter()
            .position(|line| line.trim_start() == "AND EXISTS (")
            .expect("formatted output should contain multiline WHEN condition continuation");
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT 1")
            .expect("formatted output should contain EXISTS child SELECT");
        let close_then_idx = lines
            .iter()
            .position(|line| line.trim_start() == ") THEN 'ELITE'")
            .expect("formatted output should contain EXISTS close THEN line");

        assert_eq!(
            indent(lines[and_idx]),
            indent(lines[when_idx]).saturating_add(4),
            "searched CASE condition continuations should be one extra level deeper than WHEN, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[select_idx]),
            indent(lines[and_idx]).saturating_add(4),
            "EXISTS child SELECT should use the promoted AND-owner base depth, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[close_then_idx]),
            indent(lines[and_idx]),
            "EXISTS close-paren THEN line should return to the promoted AND-owner depth, got:\n{}",
            formatted
        );
        assert_eq!(
            SqlEditorWidget::format_for_auto_formatting(&formatted, false),
            formatted,
            "auto formatting should stay stable for multiline searched CASE conditions"
        );
    }

    #[test]
    fn format_for_auto_formatting_normalizes_unrelated_odd_exists_indent_even_with_apply() {
        let source = r#"SELECT
    d.department_name,
    emp_stats.avg_sal,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM bonus_data b
            WHERE b.emp_id = d.department_id
      AND b.bonus_amt >= 300
        ) THEN 'Y'
        ELSE 'N'
    END AS has_bonus
FROM departments d
CROSS APPLY (
    SELECT AVG(e.salary) AS avg_sal
    FROM employees e
    WHERE e.department_id = d.department_id
) emp_stats;"#;

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, false);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let select_list_idx = lines
            .iter()
            .position(|line| line.trim_start() == "emp_stats.avg_sal,")
            .expect("formatted output should keep APPLY select-list continuation");
        let exists_when_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHEN EXISTS (")
            .expect("formatted output should contain EXISTS WHEN header");
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT 1")
            .expect("formatted output should contain EXISTS child SELECT");
        let where_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE b.emp_id = d.department_id")
            .expect("formatted output should contain EXISTS child WHERE");
        let and_idx = lines
            .iter()
            .position(|line| line.trim_start() == "AND b.bonus_amt >= 300")
            .expect("formatted output should contain EXISTS child AND");
        let close_then_idx = lines
            .iter()
            .position(|line| line.trim_start() == ") THEN 'Y'")
            .expect("formatted output should contain EXISTS close THEN line");

        assert_eq!(
            indent(lines[select_list_idx]),
            7,
            "APPLY-driven top-level select-list hanging indent should still be preserved, got:\n{}",
            formatted
        );
        assert!(
            indent(lines[select_idx]) > indent(lines[exists_when_idx]),
            "EXISTS child SELECT should stay nested under the WHEN EXISTS header, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[where_idx]),
            indent(lines[select_idx]),
            "EXISTS child WHERE should reuse the child query base depth, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[and_idx]),
            indent(lines[where_idx]).saturating_add(4),
            "odd manual AND indent inside EXISTS should be normalized to the computed child-query continuation depth even when APPLY exists elsewhere, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[close_then_idx]),
            indent(lines[exists_when_idx]),
            "EXISTS close-paren THEN line should return to the WHEN EXISTS header depth, got:\n{}",
            formatted
        );
        assert_eq!(
            SqlEditorWidget::format_for_auto_formatting(&formatted, false),
            formatted,
            "auto formatting should stay stable after normalizing EXISTS indentation in APPLY statements"
        );
    }

    #[test]
    fn format_for_auto_formatting_normalizes_mixed_tab_space_hanging_indent_in_exists_condition() {
        let source = "SELECT\n    d.department_name\nFROM departments d\nWHERE EXISTS (\n    SELECT 1\n    FROM bonus_data b\n    WHERE b.emp_id = d.department_id\n\t     AND b.bonus_amt >= 300\n);";

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, false);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let where_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE b.emp_id = d.department_id")
            .expect("formatted output should contain EXISTS child WHERE");
        let and_idx = lines
            .iter()
            .position(|line| line.trim_start() == "AND b.bonus_amt >= 300")
            .expect("formatted output should contain EXISTS child AND");

        assert_eq!(
            indent(lines[and_idx]),
            indent(lines[where_idx]).saturating_add(4),
            "mixed tab+space indentation should not be preserved as odd hanging indent, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_for_auto_formatting_does_not_preserve_tab_derived_odd_select_list_indent() {
        let source = "SELECT employee_id,\n\t      first_name,\n    last_name\nFROM employees;";

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, false);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let first_name_idx = lines
            .iter()
            .position(|line| {
                line.trim_start()
                    .trim_end_matches(',')
                    .eq_ignore_ascii_case("first_name")
            })
            .unwrap_or_else(|| {
                panic!(
                    "formatted output should contain first_name, got:\n{}",
                    formatted
                )
            });
        let last_name_idx = lines
            .iter()
            .position(|line| {
                line.trim_start()
                    .trim_end_matches(',')
                    .eq_ignore_ascii_case("last_name")
            })
            .unwrap_or_else(|| {
                panic!(
                    "formatted output should contain last_name, got:\n{}",
                    formatted
                )
            });

        assert_eq!(
            indent(lines[first_name_idx]),
            indent(lines[last_name_idx]),
            "tab-derived odd indentation in SELECT list should normalize to canonical continuation depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_for_auto_formatting_preserves_visual_odd_hanging_indent_with_tabs() {
        let source = "SELECT department_id
FROM departments d
WHERE EXISTS (
        SELECT 1
        FROM bonus_data b
        WHERE b.emp_id = d.department_id
\t      AND b.bonus_amt >= 300
    );";

        let formatted = SqlEditorWidget::apply_parser_depth_indentation(source);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let and_idx = lines
            .iter()
            .position(|line| line.trim_start() == "AND b.bonus_amt >= 300")
            .expect("formatted output should contain EXISTS child AND");

        let where_idx = lines
            .iter()
            .position(|line| line.trim_start() == "WHERE b.emp_id = d.department_id")
            .expect("formatted output should contain EXISTS child WHERE");

        assert_eq!(
            indent(lines[and_idx]),
            indent(lines[where_idx]).saturating_add(2),
            "tab + six spaces should be treated as visual ten-space odd hanging indent in parser-depth layout pass, got:\n{}",
            formatted
        );
    }

    #[test]
    fn plsql_for_split_in_subquery_keeps_child_query_on_for_depth() {
        let input = r#"BEGIN
    FOR rec IN
    (
        SELECT 1
        FROM dual
    ) LOOP
        NULL;
    END LOOP;
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());
        let for_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("FOR rec IN"))
            .expect("formatted output should contain FOR header");
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SELECT 1")
            .expect("formatted output should contain child SELECT");
        let close_loop_idx = lines
            .iter()
            .position(|line| line.trim_start() == ") LOOP")
            .expect("formatted output should contain close LOOP line");
        let end_loop_idx = lines
            .iter()
            .position(|line| line.trim_start() == "END LOOP;")
            .expect("formatted output should contain END LOOP");

        assert!(
            indent(lines[select_idx]) > indent(lines[for_idx]),
            "split FOR ... IN subquery should stay nested under FOR, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[close_loop_idx]),
            indent(lines[for_idx]),
            "close LOOP line should return to the FOR header depth, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[end_loop_idx]),
            indent(lines[for_idx]),
            "END LOOP should stay aligned with FOR after split IN subquery, got:\n{}",
            formatted
        );
    }

    #[test]
    fn plsql_else_if_clause_uses_block_depth_without_extra_into_indent() {
        let sql = r#"BEGIN
  IF 1 = 1 THEN
    SELECT col1,
           col2
      INTO v_col1,
           v_col2
      FROM dual;
  ELSEIF v_col1 = 1 THEN
      NULL;
  END IF;
  NULL;
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("    ELSEIF v_col1 = 1 THEN"),
            "ELSEIF should align to block depth, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("        ELSEIF v_col1 = 1 THEN"),
            "ELSEIF should not keep stale INTO-list extra indent, got:\n{}",
            formatted
        );
    }

    #[test]
    fn plsql_elseif_clause_uses_block_depth_without_extra_into_indent() {
        let sql = r#"BEGIN
  IF 1 = 1 THEN
    SELECT col1,
           col2
      INTO v_col1,
           v_col2
      FROM dual;
  ELSIF v_col1 = 1 THEN
      NULL;
  END IF;
  NULL;
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("    ELSIF v_col1 = 1 THEN"),
            "ELSIF should align to block depth, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("        ELSIF v_col1 = 1 THEN"),
            "ELSIF should not keep stale INTO-list extra indent, got:\n{}",
            formatted
        );
    }

    #[test]
    fn plsql_statement_after_returning_into_does_not_keep_extra_into_indent() {
        let sql = r#"BEGIN
  UPDATE emp
  SET sal = sal + 1
  RETURNING empno
  INTO v_empno;
  NULL;
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("    NULL;"),
            "line after RETURNING INTO should align to block depth, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("        NULL;"),
            "line after RETURNING INTO should not keep stale INTO-list extra indent, got:\n{}",
            formatted
        );
    }

    #[test]
    fn plsql_statement_after_fetch_into_does_not_keep_extra_into_indent() {
        let sql = r#"BEGIN
  FETCH c1
  INTO v_empno;
  v_total := v_total + 1;
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("    v_total := v_total + 1;"),
            "line after FETCH INTO should align to block depth, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("        v_total := v_total + 1;"),
            "line after FETCH INTO should not keep stale INTO-list extra indent, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_statement_preserves_end_for_and_end_while_suffixes() {
        let for_sql = "BEGIN\n  FOR i IN 1..3 LOOP\n    NULL;\n  END FOR;\nEND;";
        let for_formatted = SqlEditorWidget::format_sql_basic(for_sql);
        assert!(
            for_formatted.contains("END FOR;"),
            "END FOR suffix should be preserved as a single terminator line, got:\n{}",
            for_formatted
        );

        let while_sql = "BEGIN\n  WHILE i < 3 LOOP\n    i := i + 1;\n  END WHILE;\nEND;";
        let while_formatted = SqlEditorWidget::format_sql_basic(while_sql);
        assert!(
            while_formatted.contains("END WHILE;"),
            "END WHILE suffix should be preserved as a single terminator line, got:\n{}",
            while_formatted
        );
    }

    #[test]
    fn paren_case_expression_tracks_searched_case_headers() {
        let sql = r#"BEGIN
  v_val := (
    CASE v_mode
      WHEN 1 THEN 10
      ELSE 0
    END
  );
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("(\n        CASE v_mode\n            WHEN 1 THEN"),
            "CASE <expr> after '(' should get parenthesized CASE depth indent, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("ELSE\n            0\n        END"),
            "CASE <expr> END should keep CASE-block depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn paren_case_expression_tracks_end_case_terminator() {
        let sql = r#"BEGIN
  v_val := (
    CASE
      WHEN v_mode = 1 THEN 10
      ELSE 0
    END CASE
  );
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("CASE\n            WHEN v_mode = 1 THEN"),
            "Parenthesized CASE should still format branch depth, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("ELSE\n            0\n        END CASE);"),
            "END CASE terminator should stay aligned with CASE header in parenthesized expression, got:\n{}",
            formatted
        );
    }

    #[test]
    fn starts_with_end_suffix_terminator_requires_keyword_boundary() {
        assert!(SqlEditorWidget::starts_with_end_suffix_terminator(
            "END IF;"
        ));
        assert!(SqlEditorWidget::starts_with_end_suffix_terminator(
            "END LOOP"
        ));
        assert!(SqlEditorWidget::starts_with_end_suffix_terminator(
            "END CASE"
        ));
        assert!(SqlEditorWidget::starts_with_end_suffix_terminator(
            "END REPEAT"
        ));
        assert!(!SqlEditorWidget::starts_with_end_suffix_terminator("END"));
        assert!(SqlEditorWidget::starts_with_end_suffix_terminator(
            "END FOR"
        ));
        assert!(SqlEditorWidget::starts_with_end_suffix_terminator(
            "END WHILE"
        ));
        assert!(!SqlEditorWidget::starts_with_end_suffix_terminator(
            "END IF_OWNER;"
        ));
        assert!(!SqlEditorWidget::starts_with_end_suffix_terminator(
            "END FORWARD;"
        ));
    }

    #[test]
    fn starts_with_plain_end_excludes_qualified_end_suffixes() {
        assert!(SqlEditorWidget::starts_with_plain_end("END"));
        assert!(SqlEditorWidget::starts_with_plain_end("END pkg;"));
        assert!(!SqlEditorWidget::starts_with_plain_end("END FOR"));
        assert!(!SqlEditorWidget::starts_with_plain_end("END WHILE"));
        assert!(!SqlEditorWidget::starts_with_plain_end("END IF;"));
        assert!(!SqlEditorWidget::starts_with_plain_end("END LOOP"));
        assert!(!SqlEditorWidget::starts_with_plain_end("END CASE"));
        assert!(!SqlEditorWidget::starts_with_plain_end("END REPEAT"));
    }

    #[test]
    fn starts_with_bare_end_matches_only_unqualified_end() {
        assert!(SqlEditorWidget::starts_with_bare_end("END"));
        assert!(SqlEditorWidget::starts_with_bare_end("END;"));
        assert!(!SqlEditorWidget::starts_with_bare_end("END pkg;"));
        assert!(!SqlEditorWidget::starts_with_bare_end("END IF;"));
    }

    #[test]
    fn keyword_token_match_handles_exact_keyword_lines() {
        assert!(crate::sql_text::starts_with_keyword_token(
            "SELECT", "SELECT"
        ));
        assert!(crate::sql_text::starts_with_keyword_token("INTO", "INTO"));
        assert!(crate::sql_text::starts_with_keyword_token(
            "SELECT x", "SELECT"
        ));
        assert!(!crate::sql_text::starts_with_keyword_token(
            "SELECTED", "SELECT"
        ));
    }

    #[test]
    fn detects_set_transaction_as_first_statement() {
        let items = vec![ScriptItem::Statement(
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;".to_string(),
        )];
        assert!(SqlEditorWidget::requires_transaction_first_statement(
            &items
        ));
    }

    #[test]
    fn detects_alter_session_isolation_level_as_first_statement() {
        let items = vec![ScriptItem::Statement(
            "ALTER SESSION SET ISOLATION_LEVEL = SERIALIZABLE;".to_string(),
        )];
        assert!(SqlEditorWidget::requires_transaction_first_statement(
            &items
        ));
    }

    #[test]
    fn cursor_mapping_tracks_prefix_after_full_reformat() {
        let source = "SELECT a, b FROM dual";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let source_pos = source
            .find("b FROM")
            .expect("source cursor anchor should exist") as i32;
        let mapped = SqlEditorWidget::map_cursor_after_format(source, &formatted, source_pos);
        let mapped_slice = &formatted[mapped as usize..];
        assert!(
            mapped_slice.trim_start().starts_with("b\nFROM DUAL;"),
            "Mapped cursor should stay near the same token after reformat, got: {}",
            mapped_slice
        );
    }

    #[test]
    fn cursor_mapping_large_source_uses_fast_path_and_keeps_utf8_boundary() {
        let source = "x".repeat(super::CURSOR_MAPPING_FULL_REFORMAT_THRESHOLD_BYTES + 128);
        let formatted = "SELECT\n    1\nFROM DUAL;";
        let source_pos = (source.len() / 2) as i32;

        let mapped =
            SqlEditorWidget::map_cursor_after_format(&source, formatted, source_pos) as usize;

        assert!(
            mapped <= formatted.len(),
            "mapped cursor should stay in bounds"
        );
        assert!(
            formatted.is_char_boundary(mapped),
            "mapped cursor should stay on UTF-8 boundary"
        );
    }

    #[test]
    fn cursor_mapping_selection_uses_selection_relative_offset() {
        let source = "SELECT a, b FROM dual";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let source_pos_within_selection = source
            .find("b FROM")
            .expect("source cursor anchor should exist")
            as i32;
        let mapped_within_selection = SqlEditorWidget::map_cursor_after_format(
            source,
            &formatted,
            source_pos_within_selection,
        );
        let selection_start = 25i32;
        let final_cursor_pos = selection_start + mapped_within_selection;
        let formatted_slice = &formatted[mapped_within_selection as usize..];

        assert!(
            formatted_slice.trim_start().starts_with("b\nFROM DUAL;"),
            "Mapped cursor inside selection should stay near the same token after reformat, got: {}",
            formatted_slice
        );
        assert_eq!(
            final_cursor_pos,
            selection_start + mapped_within_selection,
            "Selection-relative mapping should compose with selection offset"
        );
    }

    #[test]
    fn cursor_mapping_selection_keeps_token_anchor_with_canonical_terminator() {
        let source = "SELECT a, b FROM dual";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let source_pos_within_selection = source
            .find("b FROM")
            .expect("source cursor anchor should exist")
            as i32;

        let mapped_within_selection = SqlEditorWidget::map_cursor_after_format(
            source,
            &formatted,
            source_pos_within_selection,
        );
        let formatted_slice = &formatted[mapped_within_selection as usize..];

        assert!(
            formatted_slice.trim_start().starts_with("b\nFROM DUAL"),
            "Mapped cursor should stay near same token after selection format, got: {}",
            formatted_slice
        );
        assert!(
            formatted.trim_end().ends_with(';'),
            "Selection formatting should keep canonical semicolon terminator"
        );
    }

    #[test]
    fn cursor_mapping_selected_auto_format_without_terminator_keeps_inline_comment_anchor() {
        let source = "SELECT 1 FROM dual -- trailing note";
        let formatted = SqlEditorWidget::format_for_auto_formatting(source, true);
        let source_pos_within_selection = source
            .find("-- trailing note")
            .expect("source inline comment anchor should exist")
            as i32;

        let mapped_within_selection = SqlEditorWidget::map_cursor_after_format(
            source,
            &formatted,
            source_pos_within_selection,
        );
        let mapped_slice = &formatted[mapped_within_selection as usize..];

        assert!(
            mapped_slice.starts_with("-- trailing note"),
            "Selected formatting without terminator should keep inline-comment cursor anchor, got: {}",
            mapped_slice
        );
    }

    #[test]
    fn cursor_mapping_uses_utf8_byte_offsets() {
        let source = "SELECT 한글, b FROM dual";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let byte_offset = source
            .find("b FROM")
            .expect("source cursor anchor should exist") as i32;

        let mapped = SqlEditorWidget::map_cursor_after_format(source, &formatted, byte_offset);
        let mapped_slice = &formatted[mapped as usize..];
        assert!(
            mapped_slice.trim_start().starts_with("b\nFROM DUAL;"),
            "Mapped cursor should stay near token with byte-offset mapping, got: {}",
            mapped_slice
        );
    }

    #[test]
    fn cursor_mapping_selected_auto_format_keeps_slash_terminator_anchor() {
        let source = "SELECT 1 FROM dual\n/";
        let formatted = SqlEditorWidget::format_for_auto_formatting(source, true);
        let source_pos = source.find('/').expect("slash anchor should exist") as i32;

        let mapped = SqlEditorWidget::map_cursor_after_format_with_policy(
            source, &formatted, source_pos, true,
        );
        let mapped_slice = &formatted[mapped as usize..];

        assert!(
            mapped_slice.trim_start().starts_with('/'),
            "Selected auto-format cursor mapping should keep slash terminator anchor, got: {}",
            mapped_slice
        );
        assert!(
            formatted.contains("DUAL;\n/"),
            "Selected auto-format should keep the canonical semicolon before SQL*Plus slash terminator, got:\n{}",
            formatted
        );
    }

    #[test]
    fn normalize_index_treats_input_as_byte_offset() {
        let source = "SELECT éa, b FROM dual";
        let byte_offset = source
            .find('b')
            .expect("expected cursor anchor should exist") as i32;

        let normalized = SqlEditorWidget::normalize_index(source, byte_offset);
        assert_eq!(
            normalized, byte_offset as usize,
            "normalize_index should preserve byte offsets as-is"
        );
    }

    #[test]
    fn normalize_index_clamps_non_boundary_utf8_byte_offset() {
        let source = "SELECT 한글, b FROM dual";
        let utf8_start = source.find('한').expect("expected utf-8 anchor");
        let mid_char_offset = utf8_start + 1;
        let normalized = SqlEditorWidget::normalize_index(source, mid_char_offset as i32);
        assert_eq!(
            normalized, utf8_start,
            "normalize_index should clamp invalid UTF-8 byte offsets"
        );
    }

    #[test]
    fn normalize_index_clamps_invalid_utf8_boundary_without_panic() {
        let source = "SELECT 한글, b FROM dual";
        let mid_char_index = source.find("한").expect("expected unicode anchor") + 1;

        let normalized = SqlEditorWidget::normalize_index(source, mid_char_index as i32);
        assert!(source.is_char_boundary(normalized));
        assert!(normalized <= source.len());
    }

    #[test]
    fn format_sql_basic_handles_unterminated_q_quote_without_panic() {
        let sql = "SELECT q'[unterminated FROM dual";
        let result = std::panic::catch_unwind(|| SqlEditorWidget::format_sql_basic(sql));
        assert!(
            result.is_ok(),
            "formatter should not panic on unterminated q-quote literal"
        );
    }

    #[test]
    fn selected_auto_format_cursor_mapping_matches_selected_terminator_policy() {
        let source = "select 1 from dual";
        let formatted = SqlEditorWidget::format_for_auto_formatting(source, true);
        let source_pos = source.find("dual").unwrap_or(0) as i32;

        let mapped = SqlEditorWidget::map_cursor_after_format_with_policy(
            source, &formatted, source_pos, true,
        ) as usize;

        assert!(
            mapped <= formatted.len() && formatted.is_char_boundary(mapped),
            "mapped cursor should stay on a valid UTF-8 boundary"
        );
        assert!(
            formatted[mapped..].trim_start().starts_with("DUAL"),
            "cursor should remain anchored near DUAL token after selected formatting, got: {}",
            &formatted[mapped..]
        );
    }

    #[test]
    fn selected_auto_format_cursor_mapping_handles_trailing_inline_comment_without_injected_semicolon(
    ) {
        let source = "select 1 from dual -- trailing note";
        let formatted = SqlEditorWidget::format_for_auto_formatting(source, true);
        let source_pos = source.find("-- trailing note").unwrap_or(0) as i32;

        let mapped = SqlEditorWidget::map_cursor_after_format_with_policy(
            source, &formatted, source_pos, true,
        ) as usize;

        assert!(
            formatted[mapped..]
                .trim_start()
                .starts_with("-- trailing note"),
            "cursor should remain near inline comment after selected formatting, got: {}",
            &formatted[mapped..]
        );
        assert!(
            !formatted.trim_end().ends_with(';'),
            "selected formatting should preserve missing semicolon"
        );
    }
    #[test]
    fn map_cursor_after_format_clamps_mid_utf8_byte_without_panic() {
        let source = "SELECT 한글, b FROM dual";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let mid_byte = source.find('한').unwrap_or(0) + 1;

        let result = std::panic::catch_unwind(|| {
            SqlEditorWidget::map_cursor_after_format(source, &formatted, mid_byte as i32)
        });

        assert!(
            result.is_ok(),
            "cursor mapping should not panic on mid-byte cursor offset"
        );
    }

    #[test]
    fn choose_execution_error_message_prioritizes_timeout_over_cancel() {
        let message = SqlEditorWidget::choose_execution_error_message(
            true,
            true,
            Some(Duration::from_secs(9)),
            "ORA-01013".to_string(),
        );
        assert_eq!(message, "Query timed out after 9 seconds");
    }

    #[test]
    fn choose_execution_error_message_uses_cancel_when_not_timed_out() {
        let message = SqlEditorWidget::choose_execution_error_message(
            true,
            false,
            Some(Duration::from_secs(9)),
            "ORA-01013".to_string(),
        );
        assert_eq!(message, "Query cancelled");
    }

    #[test]
    fn choose_execution_error_message_falls_back_to_original_error() {
        let message = SqlEditorWidget::choose_execution_error_message(
            false,
            false,
            Some(Duration::from_secs(9)),
            "ORA-00001: unique constraint".to_string(),
        );
        assert_eq!(message, "ORA-00001: unique constraint");
    }

    #[test]
    fn timeout_error_message_signal_detects_dpi_call_timeout() {
        assert!(
            SqlEditorWidget::timeout_error_message_contains_timeout_signal(
                "DPI-1067: call timeout of 5000 ms reached",
            )
        );
    }

    #[test]
    fn timeout_error_message_signal_detects_timeout_keyword_with_ora_01013() {
        assert!(
            SqlEditorWidget::timeout_error_message_contains_timeout_signal(
                "ORA-01013: user requested cancel of current operation (call timeout reached)",
            )
        );
    }

    #[test]
    fn timeout_error_message_signal_does_not_treat_plain_cancel_as_timeout() {
        assert!(
            !SqlEditorWidget::timeout_error_message_contains_timeout_signal(
                "ORA-01013: user requested cancel of current operation",
            )
        );
    }

    #[test]
    fn timeout_error_message_signal_does_not_treat_lock_wait_timeout_expired_as_call_timeout() {
        assert!(
            !SqlEditorWidget::timeout_error_message_contains_timeout_signal(
                "ORA-00054: resource busy and acquire with NOWAIT specified or timeout expired",
            )
        );
    }

    #[test]
    fn post_execution_output_is_skipped_when_cancel_requested() {
        assert!(!SqlEditorWidget::should_capture_post_execution_output(
            true, false, false
        ));
    }

    #[test]
    fn post_execution_output_is_skipped_when_timed_out() {
        assert!(!SqlEditorWidget::should_capture_post_execution_output(
            false, true, false
        ));
    }

    #[test]
    fn post_execution_output_is_skipped_when_execution_should_stop() {
        assert!(!SqlEditorWidget::should_capture_post_execution_output(
            false, false, true
        ));
    }

    #[test]
    fn post_execution_output_is_allowed_for_normal_completion() {
        assert!(SqlEditorWidget::should_capture_post_execution_output(
            false, false, false
        ));
    }

    #[test]
    fn flush_buffered_rows_drops_pending_rows_when_interrupted() {
        let (sender, receiver) = mpsc::channel();
        let session = Arc::new(Mutex::new(SessionState::default()));
        let mut buffered_rows = vec![vec!["1".to_string()], vec!["2".to_string()]];

        SqlEditorWidget::flush_buffered_rows(&sender, &session, 7, &mut buffered_rows, true);

        assert!(buffered_rows.is_empty());
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn flush_buffered_rows_emits_rows_when_not_interrupted() {
        let (sender, receiver) = mpsc::channel();
        let session = Arc::new(Mutex::new(SessionState::default()));
        let mut buffered_rows = vec![vec!["1".to_string()], vec!["2".to_string()]];

        SqlEditorWidget::flush_buffered_rows(&sender, &session, 3, &mut buffered_rows, false);

        assert!(buffered_rows.is_empty());
        let message = receiver
            .try_recv()
            .unwrap_or_else(|err| panic!("expected buffered rows progress message: {err}"));
        match message {
            QueryProgress::Rows { index, rows } => {
                assert_eq!(index, 3);
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("expected QueryProgress::Rows"),
        }
    }

    #[test]
    fn flush_buffered_result_rows_emits_display_rows() {
        let (sender, receiver) = mpsc::channel();
        let session = Arc::new(Mutex::new(SessionState::default()));
        let mut buffered_display_rows = vec![vec!["(null)".to_string()], vec!["2".to_string()]];
        let mut buffered_raw_rows = vec![vec!["".to_string()], vec!["2".to_string()]];

        SqlEditorWidget::flush_buffered_result_rows(
            &sender,
            &session,
            9,
            &mut buffered_display_rows,
            &mut buffered_raw_rows,
        );

        assert!(buffered_display_rows.is_empty());
        assert!(buffered_raw_rows.is_empty());
        let message = receiver
            .try_recv()
            .unwrap_or_else(|err| panic!("expected result rows progress message: {err}"));
        match message {
            QueryProgress::Rows { index, rows } => {
                assert_eq!(index, 9);
                assert_eq!(
                    rows,
                    vec![vec!["(null)".to_string()], vec!["2".to_string()]]
                );
            }
            _ => panic!("expected QueryProgress::Rows"),
        }
    }

    #[test]
    fn emit_select_result_uses_streaming_sized_initial_batch() {
        let (sender, receiver) = mpsc::channel();
        let session = Arc::new(Mutex::new(SessionState::default()));
        let rows = (0..101)
            .map(|index| vec![format!("value_{index}")])
            .collect::<Vec<Vec<String>>>();

        SqlEditorWidget::emit_select_result(
            &sender,
            &session,
            "LOCAL",
            4,
            "select * from dual",
            vec!["COL1".to_string()],
            rows,
            true,
            true,
        );

        let messages = receiver.try_iter().collect::<Vec<QueryProgress>>();
        assert_eq!(messages.len(), 5);
        match &messages[0] {
            QueryProgress::StatementStart { index } => assert_eq!(*index, 4),
            _ => panic!("expected QueryProgress::StatementStart"),
        }
        match &messages[1] {
            QueryProgress::SelectStart {
                index,
                columns,
                null_text: _,
            } => {
                assert_eq!(*index, 4);
                assert_eq!(columns, &vec!["COL1".to_string()]);
            }
            _ => panic!("expected QueryProgress::SelectStart"),
        }
        match &messages[2] {
            QueryProgress::Rows { index, rows } => {
                assert_eq!(*index, 4);
                assert_eq!(rows.len(), PROGRESS_ROWS_INITIAL_BATCH);
            }
            _ => panic!("expected initial QueryProgress::Rows"),
        }
        match &messages[3] {
            QueryProgress::Rows { index, rows } => {
                assert_eq!(*index, 4);
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("expected trailing QueryProgress::Rows"),
        }
        match &messages[4] {
            QueryProgress::StatementFinished {
                index,
                result,
                connection_name,
                timed_out,
            } => {
                assert_eq!(*index, 4);
                assert_eq!(connection_name, "LOCAL");
                assert!(!timed_out);
                assert_eq!(result.row_count, 101);
            }
            _ => panic!("expected QueryProgress::StatementFinished"),
        }
    }

    #[test]
    fn plsql_like_detection_ignores_begin_inside_strings_or_comments() {
        assert!(!SqlEditorWidget::is_plsql_like_statement(
            "SELECT 'BEGIN' AS txt FROM dual;"
        ));
        assert!(!SqlEditorWidget::is_plsql_like_statement(
            "/* DECLARE */ SELECT 1 FROM dual;"
        ));
        assert!(SqlEditorWidget::is_plsql_like_statement(
            "CREATE OR REPLACE PROCEDURE p IS BEGIN NULL; END;"
        ));
    }

    #[test]
    fn plsql_like_detection_ignores_explain_and_open_for() {
        assert!(!SqlEditorWidget::is_plsql_like_statement(
            "EXPLAIN PLAN FOR SELECT 1 FROM dual;"
        ));
        assert!(!SqlEditorWidget::is_plsql_like_statement(
            "OPEN p_rc FOR SELECT empno FROM oqt_t_emp;"
        ));
    }

    #[test]
    fn plsql_like_detection_supports_with_function_factoring() {
        assert!(SqlEditorWidget::is_plsql_like_statement(
            "WITH FUNCTION format_name(p_name IN VARCHAR2) RETURN VARCHAR2 IS\nBEGIN\n  RETURN INITCAP(p_name);\nEND;\nSELECT * FROM dual;"
        ));
    }

    #[test]
    fn plsql_like_detection_supports_or_replace_force_procedure() {
        assert!(SqlEditorWidget::is_plsql_like_statement(
            "CREATE OR REPLACE FORCE PROCEDURE test_proc AS\nBEGIN\n  NULL;\nEND;"
        ));
    }

    #[test]
    fn plsql_like_detection_supports_or_replace_editionable_function() {
        assert!(SqlEditorWidget::is_plsql_like_statement(
            "CREATE OR REPLACE EDITIONABLE FUNCTION test_fn RETURN NUMBER IS\nBEGIN\n  RETURN 1;\nEND;"
        ));
    }

    #[test]
    fn plsql_like_detection_supports_package_body() {
        assert!(SqlEditorWidget::is_plsql_like_statement(
            "CREATE PACKAGE BODY test_pkg AS\n  PROCEDURE proc IS\n  BEGIN\n    NULL;\n  END;\nEND;"
        ));
    }

    #[test]
    fn plsql_like_detection_supports_no_force_function() {
        assert!(SqlEditorWidget::is_plsql_like_statement(
            "CREATE NO FORCE FUNCTION test_fn RETURN NUMBER IS\nBEGIN\n  RETURN 1;\nEND;"
        ));
    }

    #[test]
    fn plsql_like_detection_rejects_create_materialized_view() {
        assert!(!SqlEditorWidget::is_plsql_like_statement(
            "CREATE MATERIALIZED VIEW test_mv AS SELECT 1 FROM dual"
        ));
    }

    #[test]
    fn plsql_like_detection_rejects_create_materialized_view_log() {
        assert!(!SqlEditorWidget::is_plsql_like_statement(
            "CREATE MATERIALIZED VIEW LOG ON test_table"
        ));
    }

    #[test]
    fn plsql_like_detection_rejects_create_view() {
        assert!(!SqlEditorWidget::is_plsql_like_statement(
            "CREATE OR REPLACE VIEW test_view AS SELECT 1 FROM dual"
        ));
    }

    #[test]
    fn trigger_audit_block_keeps_expected_header_and_values_alignment() {
        let sql = r#"create or replace noneditionable trigger "SYSTEM"."OQT_TRG_MEG_CUD" after insert or update or delete on oqt_meg_master for each row begin if inserting then insert into oqt_meg_audit(event_type, table_name, pk_text, detail_text) values ('INSERT', 'OQT_MEG_MASTER', 'master_id='||:NEW.master_id, 'key='||:NEW.master_key||', status='||:NEW.status||', amount='||TO_CHAR(:NEW.amount)); elsif updating then insert into oqt_meg_audit(event_type, table_name, pk_text, detail_text) values ('UPDATE', 'OQT_MEG_MASTER', 'master_id='||:NEW.master_id, 'status:'||:OLD.status||'->'||:NEW.status||', amount:'||TO_CHAR(:OLD.amount)||'->'||TO_CHAR(:NEW.amount)); elsif deleting then insert into oqt_meg_audit(event_type, table_name, pk_text, detail_text) values ('DELETE', 'OQT_MEG_MASTER', 'master_id='||:OLD.master_id, 'key='||:OLD.master_key||', status='||:OLD.status||', amount='||TO_CHAR(:OLD.amount)); end if; end; alter trigger "SYSTEM"."OQT_TRG_MEG_CUD" enable"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);
        assert!(
            formatted.contains("\n    AFTER INSERT OR UPDATE OR DELETE ON oqt_meg_master"),
            "Trigger timing/event header should stay on one indented line, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("\n    FOR EACH ROW\nBEGIN"),
            "FOR EACH ROW should align with trigger header indentation, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("IF INSERTING THEN")
                && formatted.contains("ELSIF UPDATING THEN")
                && formatted.contains("ELSIF DELETING THEN"),
            "Conditional trigger predicates should be uppercased in IF/ELSIF blocks, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("END;\nALTER TRIGGER \"SYSTEM\".\"OQT_TRG_MEG_CUD\" ENABLE;"),
            "CREATE/ALTER trigger pair should not be separated by a blank line, got:\n{}",
            formatted
        );
    }

    #[test]
    fn preserve_selected_text_terminator_does_not_add_semicolon_when_selection_had_none() {
        let source = "SELECT 1 FROM dual";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);
        assert_eq!(
            preserved.trim_end(),
            "SELECT 1
FROM DUAL"
        );
        assert!(!preserved.trim_end().ends_with(';'));
    }

    #[test]
    fn selected_auto_formatting_path_keeps_terminator_before_inline_comment() {
        let source = "select 1 from dual -- trailing note";

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, true);

        assert_eq!(
            formatted,
            "SELECT 1
FROM DUAL; -- trailing note",
            "Selected formatting should keep the canonical statement terminator before inline comment, got:
{}",
            formatted
        );
    }

    #[test]
    fn selected_auto_formatting_path_keeps_terminator_before_newline_comment() {
        let source = "select 1 from dual
-- trailing note";

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, true);

        assert_eq!(
            formatted,
            "SELECT 1
FROM DUAL;
-- trailing note",
            "Selected formatting should keep the canonical statement terminator before newline comment, got:
{}",
            formatted
        );
    }

    #[test]
    fn selected_auto_formatting_path_keeps_canonical_semicolon_for_single_statement() {
        let source = "SELECT 1 FROM dual";

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, true);

        assert_eq!(
            formatted.trim_end(),
            "SELECT 1
FROM DUAL;"
        );
        assert!(
            formatted.trim_end().ends_with(';'),
            "Selected auto-formatting should keep the canonical terminator for a selected statement, got:
{}",
            formatted
        );
    }

    #[test]
    fn full_auto_formatting_path_keeps_canonical_statement_semicolon() {
        let source = "SELECT 1 FROM dual";

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, false);

        assert!(
            formatted.trim_end().ends_with(';'),
            "Full-buffer formatting should keep canonical statement semicolon, got:
{}",
            formatted
        );
    }

    #[test]
    fn selected_auto_formatting_keeps_semicolons_for_multi_statement_selection() {
        let source = "SELECT 1 FROM dual;\nSELECT 2 FROM dual;\nCOMMIT;";

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, true);

        assert!(
            formatted.contains("SELECT 1\nFROM DUAL;"),
            "First statement semicolon should remain in multi-statement selection, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("SELECT 2\nFROM DUAL;"),
            "Second statement semicolon should remain in multi-statement selection, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("COMMIT;"),
            "Trailing COMMIT semicolon should remain in multi-statement selection, got:\n{}",
            formatted
        );

        let items = QueryExecutor::split_script_items(&formatted);
        assert_eq!(
            count_statement_items(&items),
            3,
            "Selected formatting must keep all statement boundaries intact, got: {items:?}"
        );
    }

    #[test]
    fn selected_auto_formatting_test15_keeps_statement_boundaries_for_ctrl_enter() {
        let source = load_formatter_test_file("test15.sql");
        let formatted = SqlEditorWidget::format_for_auto_formatting(&source, true);

        // Re-splitting the formatted text must preserve execution boundaries so
        // statement-at-cursor keeps working after select-all formatting.
        let original_items = QueryExecutor::split_script_items(&source);
        let formatted_items = QueryExecutor::split_script_items(&formatted);

        assert_eq!(
            count_statement_items(&formatted_items),
            count_statement_items(&original_items),
            "Selected formatting changed test15.sql statement count"
        );
        assert_eq!(
            count_tool_command_items(&formatted_items),
            count_tool_command_items(&original_items),
            "Selected formatting changed test15.sql tool command count"
        );

        let cursor = formatted
            .find("qt_splitter_boss IS")
            .expect("expected COMMENT ON TABLE statement after selected formatting");
        let bounds = QueryExecutor::statement_bounds_at_cursor(&formatted, cursor)
            .expect("expected COMMENT ON TABLE bounds after selected formatting");
        let statement = &formatted[bounds.0..bounds.1];

        assert!(
            statement.trim_start().starts_with("COMMENT")
                && statement.contains("ON TABLE qt_splitter_boss IS"),
            "Ctrl+Enter boundary should stay on COMMENT statement after selected formatting, got:\n{}",
            statement
        );
        assert!(
            !statement.contains("CREATE OR REPLACE PROCEDURE qt_splitter_proc"),
            "Selected formatting must not merge earlier routine into COMMENT statement, got:\n{}",
            statement
        );
    }

    #[test]
    fn selected_auto_formatting_test16_keeps_statement_boundaries_for_ctrl_enter() {
        let source = load_formatter_test_file("test16.sql");
        let formatted = SqlEditorWidget::format_for_auto_formatting(&source, true);

        // Re-splitting the formatted text must preserve execution boundaries so
        // statement-at-cursor keeps working after select-all formatting.
        let original_items = QueryExecutor::split_script_items(&source);
        let formatted_items = QueryExecutor::split_script_items(&formatted);

        assert_eq!(
            count_statement_items(&formatted_items),
            count_statement_items(&original_items),
            "Selected formatting changed test16.sql statement count"
        );
        assert_eq!(
            count_tool_command_items(&formatted_items),
            count_tool_command_items(&original_items),
            "Selected formatting changed test16.sql tool command count"
        );

        let cursor = formatted
            .find("qt_splitter_ultimate IS")
            .expect("expected COMMENT ON TABLE statement after selected formatting");
        let bounds = QueryExecutor::statement_bounds_at_cursor(&formatted, cursor)
            .expect("expected COMMENT ON TABLE bounds after selected formatting");
        let statement = &formatted[bounds.0..bounds.1];

        assert!(
            statement.trim_start().starts_with("COMMENT")
                && statement.contains("ON TABLE qt_splitter_ultimate IS"),
            "Ctrl+Enter boundary should stay on COMMENT statement after selected formatting, got:\n{}",
            statement
        );
        assert!(
            !statement.contains("CREATE OR REPLACE PACKAGE BODY qt_splitter_ultimate_pkg"),
            "Selected formatting must not merge earlier package body into COMMENT statement, got:\n{}",
            statement
        );
    }

    #[test]
    fn full_auto_formatting_test20_keeps_statement_boundaries() {
        let source = load_formatter_test_file("test20.sql");
        let formatted = SqlEditorWidget::format_for_auto_formatting(&source, false);

        let original_items = QueryExecutor::split_script_items(&source);
        let formatted_items = QueryExecutor::split_script_items(&formatted);

        assert_eq!(
            count_statement_items(&formatted_items),
            count_statement_items(&original_items),
            "Full auto-formatting changed test20.sql statement count"
        );
        assert_eq!(
            count_tool_command_items(&formatted_items),
            count_tool_command_items(&original_items),
            "Full auto-formatting changed test20.sql tool command count"
        );

        let cursor = formatted
            .find("AS msg_preview")
            .expect("expected final audit preview query after full auto-formatting");
        let bounds = QueryExecutor::statement_bounds_at_cursor(&formatted, cursor)
            .expect("expected final audit preview query bounds after full auto-formatting");
        let statement = &formatted[bounds.0..bounds.1];

        assert!(
            statement.trim_start().starts_with("SELECT")
                && statement.contains("FROM qt_fb_audit")
                && statement.contains("AS msg_preview"),
            "Full auto-formatting should keep the final audit preview query isolated, got:\n{}",
            statement
        );
        assert!(
            !statement.contains("FROM qt_fb_view"),
            "Full auto-formatting must not merge the preceding view query into the audit preview query, got:\n{}",
            statement
        );
        assert!(
            !statement.contains("p_module => 'final_validation'"),
            "Full auto-formatting must not merge the final validation block into the audit preview query, got:\n{}",
            statement
        );
    }

    #[test]
    fn full_auto_formatting_test21_keeps_statement_boundaries() {
        let source = load_formatter_test_file("test21.sql");
        let formatted = SqlEditorWidget::format_for_auto_formatting(&source, false);

        let original_items = QueryExecutor::split_script_items(&source);
        let formatted_items = QueryExecutor::split_script_items(&formatted);

        assert_eq!(
            count_statement_items(&formatted_items),
            count_statement_items(&original_items),
            "Full auto-formatting changed test21.sql statement count"
        );
        assert_eq!(
            count_tool_command_items(&formatted_items),
            count_tool_command_items(&original_items),
            "Full auto-formatting changed test21.sql tool command count"
        );

        let cursor = formatted
            .find("AS err_msg_preview")
            .expect("expected final error preview query after full auto-formatting");
        let bounds = QueryExecutor::statement_bounds_at_cursor(&formatted, cursor)
            .expect("expected final error preview query bounds after full auto-formatting");
        let statement = &formatted[bounds.0..bounds.1];

        assert!(
            statement.trim_start().starts_with("SELECT")
                && statement.contains("FROM qt_x_err_log")
                && statement.contains("AS err_msg_preview"),
            "Full auto-formatting should keep the final error preview query isolated, got:\n{}",
            statement
        );
        assert!(
            !statement.contains("FROM qt_x_audit"),
            "Full auto-formatting must not merge the preceding audit preview query into the error preview query, got:\n{}",
            statement
        );
        assert!(
            !statement.contains("p_module => 'final_validation'"),
            "Full auto-formatting must not merge the final validation block into the error preview query, got:\n{}",
            statement
        );
    }

    #[test]
    fn format_sql_basic_keeps_trigger_slash_and_alter_trigger_tightly_grouped() {
        let sql = r#"CREATE OR REPLACE TRIGGER trg_demo
BEFORE INSERT ON demo
BEGIN
    NULL;
END;
/
ALTER TRIGGER trg_demo ENABLE;"#;

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("END;\n/\nALTER TRIGGER trg_demo ENABLE;"),
            "CREATE TRIGGER + slash + ALTER TRIGGER should stay tightly grouped, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("END;\n/\n\nALTER TRIGGER"),
            "Unexpected blank line inserted between slash and ALTER TRIGGER, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_places_statement_semicolon_before_trailing_line_comment() {
        let source = "SELECT 1 FROM dual -- trailing note";

        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            formatted.contains("FROM DUAL; -- trailing note"),
            "Formatter should place statement terminator before trailing line comment, got:
{}",
            formatted
        );
        assert!(
            !formatted.trim_end().ends_with("note;"),
            "Formatter should not append semicolon into trailing line comment text, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_places_statement_semicolon_before_trailing_block_comment() {
        let source = "SELECT 1 FROM dual /* trailing block */";

        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            formatted.contains("FROM DUAL;"),
            "Formatter should place statement terminator before trailing block comment, got:\n{}",
            formatted
        );
        let semicolon_idx = formatted.find(';');
        let comment_idx = formatted.find("/* trailing block */");
        assert!(
            matches!((semicolon_idx, comment_idx), (Some(semicolon), Some(comment)) if semicolon < comment),
            "Statement terminator must appear before trailing block comment, got:\n{}",
            formatted
        );
        assert!(
            !formatted.trim_end().ends_with("*/;"),
            "Formatter should not append semicolon after trailing block comment, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_does_not_insert_space_before_newline_line_comment() {
        let source = "SELECT 1 FROM dual\n-- trailing note";

        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            formatted.contains("FROM DUAL;\n-- trailing note"),
            "Formatter should insert semicolon without trailing space before newline comment, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("; \n-- trailing note"),
            "Formatter should not leave whitespace before newline-attached line comment, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_comma_indent_after_line_comment_in_select_list() {
        let source = "select abc -- comment\n, def\nfrom efg;";

        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            formatted.contains("SELECT\n    abc -- comment\n    ,\n    def\nFROM efg;"),
            "Formatter should keep select-list depth for leading comma after line comment, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_does_not_insert_space_before_newline_block_comment() {
        let source = "SELECT 1 FROM dual\n/* trailing block */";

        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            formatted.contains("FROM DUAL;\n/* trailing block */"),
            "Formatter should insert semicolon without trailing space before newline block comment, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("; \n/* trailing block */"),
            "Formatter should not leave whitespace before newline-attached block comment, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_does_not_insert_space_before_newline_comments_in_select_clause_from_user_case(
    ) {
        let source = r#"SELECT
d.deptno,
d.dname,
-- [D] scalar subquery
(
/* [E] correlated max */
SELECT MAX(e2.sal)
FROM emp e2
WHERE e2.deptno = d.deptno
) AS max_sal
FROM dept d;"#;

        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            !formatted.contains("d.dname,\n\n"),
            "Formatter inserted an extra blank line before a newline-attached line comment in SELECT, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("(\n\n"),
            "Formatter inserted an extra blank line before a newline-attached block comment in SELECT, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_join_condition_depth_after_inline_block_comment_on_clause() {
        let source =
            "select * from paid p join amounts a on /* join key */\na.order_id = p.order_id;";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            formatted.contains("JOIN amounts a\n    ON /* join key */\n    a.order_id = p.order_id;"),
            "Inline block comment after ON should keep the following join condition on ON-clause depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_join_condition_depth_after_inline_line_comment_on_clause() {
        let source = "select * from paid p join amounts a on -- join key\na.order_id = p.order_id;";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            formatted.contains("JOIN amounts a\n    ON -- join key\n    a.order_id = p.order_id;"),
            "Inline line comment after ON should keep the following join condition on ON-clause depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn preserve_selected_text_terminator_removes_inserted_semicolon_before_trailing_comment() {
        let source = "SELECT 1 FROM dual -- trailing note";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);
        assert!(
            !SqlEditorWidget::statement_ends_with_semicolon(&preserved),
            "Semicolon should be removed when original selection had no terminator, got:
{}",
            preserved
        );
        assert!(
            preserved.trim_end().ends_with("-- trailing note"),
            "Trailing comment should be preserved, got:
{}",
            preserved
        );
    }

    #[test]
    fn preserve_selected_text_terminator_removes_inserted_semicolon_before_newline_comment() {
        let source = "SELECT 1 FROM dual\n-- trailing note";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);
        assert!(
            !preserved.contains(";\n-- trailing note"),
            "Selection-preserved formatting should remove inserted semicolon for newline comment attachment, got:\n{}",
            preserved
        );
    }

    #[test]
    fn preserve_selected_text_terminator_removes_inserted_semicolon_before_trailing_block_comment()
    {
        let source = "SELECT 1 FROM dual /* trailing block */";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);
        assert!(
            !SqlEditorWidget::statement_ends_with_semicolon(&preserved),
            "Semicolon should be removed when original selection had no terminator, got:\n{}",
            preserved
        );
        assert!(
            preserved.trim_end().ends_with("/* trailing block */"),
            "Trailing block comment should be preserved, got:\n{}",
            preserved
        );
    }

    #[test]
    fn preserve_selected_text_terminator_removes_inserted_semicolon_before_newline_block_comment() {
        let source = "SELECT 1 FROM dual\n/* trailing block */";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);
        assert!(
            !preserved.contains(";\n/* trailing block */"),
            "Selection-preserved formatting should remove inserted semicolon for newline block comment attachment, got:\n{}",
            preserved
        );
    }

    #[test]
    fn preserve_selected_text_terminator_removes_inserted_semicolon_when_string_has_comment_markers(
    ) {
        let source = "SELECT '-- keep literal' AS txt FROM dual";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);

        assert!(
            !SqlEditorWidget::statement_ends_with_semicolon(&preserved),
            "Semicolon should be removed when original selection had no terminator, got:\n{}",
            preserved
        );
        assert!(
            preserved.contains("'-- keep literal'"),
            "String literal containing comment markers should be preserved, got:\n{}",
            preserved
        );
    }

    #[test]
    fn preserve_selected_text_terminator_handles_multibyte_text_before_comment() {
        let formatted = "SELECT '한글' FROM dual;".to_string();
        let without_semicolon = SqlEditorWidget::remove_trailing_statement_semicolon(&formatted)
            .expect("trailing semicolon should be removable");
        assert_eq!(without_semicolon, "SELECT '한글' FROM dual");
    }

    #[test]
    fn preserve_selected_text_terminator_does_not_remove_semicolon_inside_string_literal() {
        let source = "SELECT 'a;b' AS txt FROM dual";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);

        assert!(
            preserved.contains("'a;b'"),
            "Semicolon inside string literal must remain unchanged, got:\n{}",
            preserved
        );
        assert!(
            !SqlEditorWidget::statement_ends_with_semicolon(&preserved),
            "Formatter should not append semicolon when original selection had none, got:\n{}",
            preserved
        );
    }

    #[test]
    fn preserve_selected_text_terminator_removes_inserted_semicolon_before_trailing_rem_line() {
        let source = "SELECT 1 FROM dual
REM trailing script comment";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);

        assert!(
            !preserved.contains(';'),
            "Inserted semicolon should be removed when trailing REM line follows a statement with no terminator, got:
{}",
            preserved
        );
        assert!(
            preserved.contains("REM trailing script comment"),
            "Trailing REM line should be preserved, got:
{}",
            preserved
        );
    }

    #[test]
    fn preserve_selected_text_terminator_removes_inserted_semicolon_before_trailing_remark_line() {
        let source = "SELECT 1 FROM dual
REMARK trailing script comment";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);

        assert!(
            !preserved.contains(';'),
            "Inserted semicolon should be removed when trailing REMARK line follows a statement with no terminator, got:
{}",
            preserved
        );
        assert!(
            preserved.contains("REMARK trailing script comment"),
            "Trailing REMARK line should be preserved, got:
{}",
            preserved
        );
    }

    #[test]
    fn format_sql_basic_places_semicolon_before_trailing_rem_line() {
        let source = "SELECT 1 FROM dual
REM trailing script comment";

        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            formatted.contains("FROM DUAL;\nREM trailing script comment"),
            "Formatter should place statement terminator before trailing REM line, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("REM trailing script comment;"),
            "Formatter should not append semicolon to REM comment text, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_places_semicolon_before_trailing_remark_line() {
        let source = "SELECT 1 FROM dual
REMARK trailing script comment";

        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            formatted.contains("FROM DUAL;\nREMARK trailing script comment"),
            "Formatter should place statement terminator before trailing REMARK line, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("REMARK trailing script comment;"),
            "Formatter should not append semicolon to REMARK comment text, got:\n{}",
            formatted
        );
    }

    #[test]
    fn append_missing_statement_terminator_places_semicolon_before_trailing_indented_rem_line() {
        let mut formatted = "SELECT 1
FROM DUAL
    REM trailing script comment"
            .to_string();

        SqlEditorWidget::append_missing_statement_terminator(&mut formatted);

        assert_eq!(
            formatted,
            "SELECT 1
FROM DUAL;
    REM trailing script comment"
        );
    }

    #[test]
    fn append_missing_statement_terminator_places_semicolon_before_trailing_indented_remark_line() {
        let mut formatted = "SELECT 1
FROM DUAL
	REMARK trailing script comment"
            .to_string();

        SqlEditorWidget::append_missing_statement_terminator(&mut formatted);

        assert_eq!(
            formatted,
            "SELECT 1
FROM DUAL;
	REMARK trailing script comment"
        );
    }

    #[test]
    fn format_sql_basic_does_not_append_semicolon_for_incomplete_trailing_operator() {
        let source = "SELECT 1 +";

        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert_eq!(formatted, "SELECT 1 +");
    }

    #[test]
    fn format_sql_basic_does_not_append_semicolon_for_incomplete_trailing_comma() {
        let source = "SELECT a,";

        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert_eq!(formatted, "SELECT a,");
    }

    #[test]
    fn format_sql_basic_does_not_append_semicolon_for_unbalanced_open_paren() {
        let source = "SELECT (1 + 2 FROM dual";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            !formatted.trim_end().ends_with(';'),
            "formatter must not append semicolon to malformed statement with unbalanced open paren, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_does_not_append_semicolon_for_unbalanced_closing_paren() {
        let source = "SELECT 1 + 2) FROM dual";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            !formatted.trim_end().ends_with(';'),
            "formatter must not append semicolon to malformed statement with unmatched closing paren, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_does_not_append_semicolon_for_unbalanced_brackets() {
        let source = "SELECT arr[1 FROM dual";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            !formatted.trim_end().ends_with(';'),
            "formatter must not append semicolon to malformed statement with unbalanced brackets, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_appends_semicolon_for_tail_statement_after_malformed_segment() {
        let source = "SELECT func(a, b;
SELECT c, d FROM dual";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            formatted.contains(
                "SELECT c,
    d
FROM DUAL;"
            ),
            "formatter should keep canonical terminator for trailing valid statement, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_does_not_append_semicolon_for_incomplete_trailing_dot() {
        let source = "SELECT t.";

        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert_eq!(formatted, "SELECT t.");
    }

    #[test]
    fn format_sql_basic_does_not_append_semicolon_for_unterminated_single_quote() {
        let source = "SELECT 'unterminated";

        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert_eq!(formatted, "SELECT 'unterminated");
    }

    #[test]
    fn format_sql_basic_does_not_append_semicolon_for_unterminated_quoted_identifier() {
        let source = "SELECT \"unterminated";

        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert_eq!(formatted, "SELECT \"unterminated");
    }

    #[test]
    fn format_sql_basic_does_not_append_semicolon_for_unterminated_dollar_quote() {
        let source = "SELECT $$unterminated";

        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert_eq!(formatted, "SELECT $$unterminated");
    }

    #[test]
    fn format_sql_basic_does_not_append_semicolon_for_unterminated_plsql_label() {
        let source = "BEGIN <<label
NULL";

        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert_eq!(
            formatted,
            "BEGIN
    <<label
    NULL"
        );
        assert!(
            !formatted.trim_end().ends_with(';'),
            "unterminated PL/SQL label should not receive a terminator, got:
{}",
            formatted
        );
    }

    #[test]
    fn preserve_selected_text_terminator_keeps_semicolon_when_selection_had_one() {
        let source = "SELECT 1 FROM dual;";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);
        assert!(preserved.trim_end().ends_with(';'));
    }

    #[test]
    fn preserve_selected_text_terminator_respects_trailing_comment_after_semicolon() {
        let source = "SELECT 1 FROM dual; -- keep terminator";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);
        assert!(
            preserved.trim_end().ends_with("-- keep terminator"),
            "Trailing comment should be preserved, got:
{}",
            preserved
        );
        assert!(
            SqlEditorWidget::statement_ends_with_semicolon(&preserved),
            "Semicolon should remain when selection already ended with semicolon before comment, got:
{}",
            preserved
        );
    }

    #[test]
    fn format_sql_basic_does_not_append_extra_semicolon_into_trailing_comment_text() {
        let source = "SELECT 1 FROM dual -- existing; comment semicolon";

        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            formatted.contains("FROM DUAL; -- existing; comment semicolon"),
            "Formatter should preserve trailing comment text while inserting SQL terminator, got:
{}",
            formatted
        );
        assert_eq!(
            formatted.matches(';').count(),
            2,
            "Expected one SQL terminator + one comment semicolon only, got:
{}",
            formatted
        );
    }

    #[test]
    fn preserve_selected_text_terminator_ignores_semicolon_inside_trailing_comment() {
        let source = "SELECT 1 FROM dual -- existing; comment semicolon";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);
        assert!(
            preserved
                .trim_end()
                .ends_with("-- existing; comment semicolon"),
            "Trailing comment text should remain unchanged, got:\n{}",
            preserved
        );
        assert_eq!(
            preserved.matches(';').count(),
            1,
            "No extra semicolon should be appended when source had only comment semicolon, got:\n{}",
            preserved
        );
        assert!(
            !SqlEditorWidget::statement_ends_with_semicolon(&preserved),
            "Statement terminator should stay absent, got:\n{}",
            preserved
        );
    }

    #[test]
    fn preserve_selected_text_terminator_keeps_semicolon_in_comment_only_selection() {
        let source = "-- existing; comment semicolon";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);
        assert_eq!(
            preserved, "-- existing; comment semicolon",
            "Semicolon inside comment-only selections should not be removed"
        );
    }

    #[test]
    fn preserve_selected_text_terminator_keeps_semicolon_in_sqlplus_remark_comment() {
        let source = "REM existing; comment semicolon";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);
        assert_eq!(
            preserved, source,
            "Semicolon inside SQL*Plus remark comment should stay untouched, got:\n{}",
            preserved
        );
    }

    #[test]
    fn selected_auto_formatting_keeps_canonical_terminator_before_trailing_prompt() {
        let source = "select 1 from dual
PROMPT done;";

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, true);

        assert!(
            formatted.contains(
                "FROM DUAL;\n\nPROMPT done;"
            ),
            "Selected auto-format should keep the trailing PROMPT line separated from SQL by a blank line, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_inserts_blank_line_before_trailing_prompt_after_set_operator_query() {
        let source = "SELECT empno
FROM a
MINUS
SELECT empno
FROM b
ORDER BY empno;
PROMPT [DONE] Hardcore SELECT tests finished.";

        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            formatted.contains(
                "ORDER BY empno;\n\nPROMPT [DONE] Hardcore SELECT tests finished."
            ),
            "Formatter should separate trailing PROMPT from the SQL statement with a blank line, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_for_auto_formatting_reassembles_multiline_sqlplus_set_commands() {
        let source = "SET SERVEROUTPUT
    ON SIZE UNLIMITED;
SET TIMING
    ON;
SELECT 1 FROM dual;";

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, false);
        let expected = [
            "SET SERVEROUTPUT ON SIZE UNLIMITED",
            "",
            "SET TIMING ON",
            "",
            "SELECT 1",
            "FROM DUAL;",
        ]
        .join("\n");

        assert_eq!(formatted, expected);
    }

    #[test]
    fn format_for_auto_formatting_keeps_parenthesized_top_level_set_operators_left_aligned() {
        let source = r#"(
    SELECT e.dept_id,
        'HIGH' AS bucket,
        COUNT (*) AS cnt
    FROM qt_fmt_emp e
    WHERE e.salary >= (
        SELECT AVG (x.salary)
        FROM qt_fmt_emp x
        WHERE x.dept_id = e.dept_id
    )
    GROUP BY e.dept_id
    HAVING COUNT (*) > 0
)
    UNION ALL (
        SELECT e.dept_id,
            'LOW' AS bucket,
            COUNT (*) AS cnt
        FROM qt_fmt_emp e
        WHERE e.salary < (
            SELECT AVG (x.salary)
            FROM qt_fmt_emp x
            WHERE x.dept_id = e.dept_id
        )
        GROUP BY e.dept_id
        HAVING COUNT (*) > 0
    )
    MINUS (
        SELECT 9999 AS dept_id,
            'LOW' AS bucket,
            0 AS cnt
        FROM DUAL
    )
    ORDER BY 1,
        2;"#;
        let formatted = SqlEditorWidget::format_for_auto_formatting(source, false);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());
        let union_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("UNION ALL"))
            .unwrap_or(0);
        let minus_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("MINUS"))
            .unwrap_or(0);
        let order_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("ORDER BY"))
            .unwrap_or(0);
        let union_select_idx = lines
            .iter()
            .enumerate()
            .skip(union_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start().starts_with("SELECT e.dept_id"))
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let minus_select_idx = lines
            .iter()
            .enumerate()
            .skip(minus_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start().starts_with("SELECT 9999 AS dept_id"))
            .map(|(idx, _)| idx)
            .unwrap_or(0);

        assert_eq!(
            indent(lines[union_idx]),
            0,
            "UNION ALL after a top-level ')' should return to depth 0, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[minus_idx]),
            0,
            "MINUS after a top-level ')' should return to depth 0, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[order_idx]),
            0,
            "ORDER BY after parenthesized set operands should stay on depth 0, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[union_select_idx]),
            4,
            "SELECT under UNION ALL should remain one level deeper than the set operator, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[minus_select_idx]),
            4,
            "SELECT under MINUS should remain one level deeper than the set operator, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_for_auto_formatting_keeps_parenthesized_top_level_intersect_left_aligned() {
        let source = r#"(
    SELECT dept_id
    FROM qt_fmt_emp
)
    INTERSECT (
        SELECT dept_id
        FROM qt_fmt_emp_hist
    )
    ORDER BY 1;"#;

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, false);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());
        let intersect_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("INTERSECT"))
            .unwrap_or(0);
        let order_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("ORDER BY"))
            .unwrap_or(0);
        let rhs_select_idx = lines
            .iter()
            .enumerate()
            .skip(intersect_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start().starts_with("SELECT dept_id"))
            .map(|(idx, _)| idx)
            .unwrap_or(0);

        assert_eq!(
            indent(lines[intersect_idx]),
            0,
            "INTERSECT after a top-level ')' should return to depth 0, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[order_idx]),
            0,
            "ORDER BY after parenthesized INTERSECT operands should stay on depth 0, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[rhs_select_idx]),
            4,
            "SELECT under INTERSECT should remain one level deeper than the set operator, got:\n{}",
            formatted
        );
    }

    #[test]
    fn source_has_explicit_semicolon_terminator_ignores_trailing_prompt_line() {
        let source = "SELECT 1 FROM dual
PROMPT done;";

        assert!(
            !SqlEditorWidget::source_has_explicit_semicolon_terminator(source),
            "Semicolon in trailing PROMPT line should not count as SQL terminator"
        );
    }

    #[test]
    fn format_sql_basic_preserves_sqlplus_remark_comment_text_case() {
        let sql = "REMARK Keep MixedCase ; punctuation";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert_eq!(formatted, sql);
    }

    #[test]
    fn format_sql_basic_preserves_sqlplus_remark_comment_indentation() {
        let sql = "    REMARK Keep indentation\n\tREM tab-indented";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert_eq!(formatted, sql);
    }

    #[test]
    fn format_sql_basic_preserves_sqlplus_rem_comments_between_statements() {
        let sql = "REM keep this exact comment\nSELECT 1 FROM dual;\nREMARK Keep;This;Too";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("REM keep this exact comment"),
            "{}",
            formatted
        );
        assert!(formatted.contains("REMARK Keep;This;Too"), "{}", formatted);
        assert!(formatted.contains("SELECT 1\nFROM DUAL;"), "{}", formatted);
    }

    #[test]
    fn format_sql_basic_preserves_comment_attachment_depth_and_hint_layout() {
        let sql = r#"-- file header keep
CREATE OR REPLACE PACKAGE pkg AS
-- package line comment
PROCEDURE p;
END pkg;
/

CREATE OR REPLACE TRIGGER trg
BEFORE INSERT ON t
BEGIN
-- before if
IF :NEW.id IS NULL THEN
-- if branch
:NEW.id := 1; -- inline   keep    spacing
ELSIF :NEW.id < 0 THEN
/* commented-out code
SELECT *
FROM dual;
*/
:NEW.id := 0;
ELSE
CASE WHEN :NEW.flag = 'Y' THEN
NULL;
END CASE;
END IF;
FOR i IN 1..2 LOOP
-- in loop
NULL;
END LOOP;
EXCEPTION
WHEN OTHERS THEN
-- in exception
NULL;
END;
/
SELECT /*+ INDEX(t idx_t) */ col -- inline tail keep
FROM t;
/* block
  layout
    keep
*/
"#;

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.starts_with("-- file header keep\n"),
            "{}",
            formatted
        );
        assert!(formatted.contains("    -- before if"), "{}", formatted);
        assert!(formatted.contains("        -- if branch"), "{}", formatted);
        assert!(formatted.contains("        -- in loop"), "{}", formatted);
        assert!(
            formatted.contains("        -- in exception"),
            "{}",
            formatted
        );
        assert!(
            formatted.contains(":= 1; -- inline   keep    spacing"),
            "{}",
            formatted
        );
        assert!(
            formatted.contains("        /* commented-out code\nSELECT *\nFROM dual;\n*/"),
            "{}",
            formatted
        );
        assert!(formatted.contains("/*+ INDEX(t idx_t) */"), "{}", formatted);
        assert!(
            formatted.contains("col -- inline tail keep"),
            "{}",
            formatted
        );
        assert!(
            formatted.contains("/* block\n  layout\n    keep\n*/"),
            "{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_preserves_oracle_hint_position_in_case_expression() {
        let sql = "SELECT CASE /*+ NO_EXPAND */ WHEN a = 1 THEN b ELSE c END -- keep\nFROM t";

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(formatted.contains("/*+ NO_EXPAND */"), "{}", formatted);
        assert!(formatted.contains("END -- keep"), "{}", formatted);
    }

    #[test]
    fn format_tool_command_accept_escapes_single_quote_prompt() {
        let rendered = SqlEditorWidget::format_tool_command(&crate::db::ToolCommand::Accept {
            name: "v_name".to_string(),
            prompt: Some("Owner's value?".to_string()),
        });

        assert_eq!(rendered, "ACCEPT v_name PROMPT 'Owner''s value?'");
    }

    #[test]
    fn statement_ends_with_semicolon_recognizes_sqlplus_slash_terminator() {
        assert!(SqlEditorWidget::statement_ends_with_semicolon(
            "SELECT 1 FROM dual;
/"
        ));
    }

    #[test]
    fn statement_ends_with_semicolon_recognizes_sqlplus_slash_without_semicolon() {
        assert!(SqlEditorWidget::statement_ends_with_semicolon(
            "SELECT 1 FROM dual
/"
        ));
    }

    #[test]
    fn statement_ends_with_semicolon_recognizes_sqlplus_slash_with_trailing_comment() {
        assert!(SqlEditorWidget::statement_ends_with_semicolon(
            "SELECT 1 FROM dual
/
REM keep"
        ));
    }

    #[test]
    fn preserve_selected_text_terminator_removes_inserted_semicolon_before_sqlplus_slash() {
        let source = "BEGIN
  NULL;
END
/";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);

        assert!(
            preserved.contains("END
/"),
            "Formatter should not keep an inserted semicolon before SQL*Plus slash terminator when source had none, got:
{}",
            preserved
        );
        assert!(
            !preserved.contains(
                "END;
/"
            ),
            "Inserted semicolon before SQL*Plus slash terminator should be removed, got:
{}",
            preserved
        );
    }

    #[test]
    fn preserve_selected_text_terminator_keeps_existing_semicolon_before_sqlplus_slash() {
        let source = "BEGIN
  NULL;
END;
/";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);

        assert!(
            preserved.contains(
                "END;
/"
            ),
            "Existing semicolon before SQL*Plus slash terminator should remain, got:
{}",
            preserved
        );
    }

    #[test]
    fn statement_ends_with_semicolon_ignores_sqlplus_remark_comment_text() {
        assert!(!SqlEditorWidget::statement_ends_with_semicolon(
            "REM only a comment"
        ));
        assert!(!SqlEditorWidget::statement_ends_with_semicolon(
            "REMARK this is a comment with ; semicolon"
        ));
    }

    #[test]
    fn statement_ends_with_semicolon_recognizes_semicolon_before_inline_sqlplus_remark_comment() {
        assert!(SqlEditorWidget::statement_ends_with_semicolon(
            "SELECT 1 FROM dual; REM trailing comment"
        ));
        assert!(SqlEditorWidget::statement_ends_with_semicolon(
            "SELECT 1 FROM dual; REMARK trailing comment"
        ));
    }

    #[test]
    fn statement_ends_with_semicolon_rejects_semicolon_before_remark_with_following_statement() {
        assert!(!SqlEditorWidget::statement_ends_with_semicolon(
            "SELECT 1 FROM dual; REM note
SELECT 2 FROM dual"
        ));
    }

    #[test]
    fn statement_ends_with_semicolon_recognizes_semicolon_before_line_comment() {
        assert!(SqlEditorWidget::statement_ends_with_semicolon(
            "SELECT 1 FROM dual; -- trailing comment"
        ));
    }

    #[test]
    fn statement_ends_with_semicolon_recognizes_semicolon_before_slash_and_line_comment() {
        assert!(SqlEditorWidget::statement_ends_with_semicolon(
            "BEGIN\n  NULL;\nEND;\n/\n-- keep"
        ));
    }

    #[test]
    fn preserve_selected_text_terminator_keeps_semicolon_before_inline_sqlplus_remark_comment() {
        let source = "SELECT 1 FROM dual; REM trailing comment";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);

        assert!(
            preserved.contains("FROM DUAL;") && preserved.contains("REM trailing comment"),
            "Existing semicolon and SQL*Plus REM comment should remain, got:\n{}",
            preserved
        );
        assert!(
            SqlEditorWidget::statement_ends_with_semicolon(&preserved),
            "Existing statement terminator before inline SQL*Plus REM comment should remain, got:\n{}",
            preserved
        );
    }

    #[test]
    fn preserve_selected_text_terminator_keeps_semicolon_before_line_comment() {
        let source = "SELECT 1 FROM dual; -- keep terminator";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);

        assert!(
            preserved.contains("FROM DUAL;"),
            "Existing semicolon before line comment should remain, got:\n{}",
            preserved
        );
        assert!(
            preserved.trim_end().ends_with("-- keep terminator"),
            "Trailing line comment should remain, got:\n{}",
            preserved
        );
        assert!(
            SqlEditorWidget::statement_ends_with_semicolon(&preserved),
            "Existing terminator before line comment should remain explicit, got:\n{}",
            preserved
        );
    }

    #[test]
    fn preserve_selected_text_terminator_keeps_semicolon_before_slash_and_trailing_comment() {
        let source = "BEGIN\n  NULL;\nEND;\n/\n-- keep";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        let preserved = SqlEditorWidget::preserve_selected_text_terminator(source, formatted);

        assert!(
            preserved.contains("END;\n/"),
            "Semicolon before SQL*Plus slash should remain when source had explicit terminator, got:\n{}",
            preserved
        );
        assert!(
            preserved.trim_end().ends_with("-- keep"),
            "Trailing line comment should remain, got:\n{}",
            preserved
        );
    }

    #[test]
    fn format_statement_preserves_compound_trigger_timing_end_qualifier() {
        let sql = r#"CREATE OR REPLACE TRIGGER test_compound_trg
  FOR INSERT ON test_table
  COMPOUND TRIGGER
    BEFORE EACH ROW IS
    BEGIN
      :NEW.status := 'new';
    END BEFORE EACH ROW;
    AFTER STATEMENT IS
    BEGIN
      NULL;
    END AFTER STATEMENT;
  END test_compound_trg;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);
        assert!(
            formatted.contains("END BEFORE EACH ROW;"),
            "Compound trigger BEFORE timing qualifier should be preserved, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("END AFTER STATEMENT;"),
            "Compound trigger AFTER timing qualifier should be preserved, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_compound_trigger_declaration_and_timing_sections_use_stable_base_depth() {
        let sql = r#"CREATE OR REPLACE TRIGGER trg_employee_compound
    FOR INSERT OR UPDATE OR DELETE ON employees COMPOUND TRIGGER TYPE emp_id_set_t IS
    TABLE OF NUMBER INDEX BY PLS_INTEGER;
    g_emp_ids emp_id_set_t;
    g_idx PLS_INTEGER := 0;
    BEFORE STATEMENT IS
    BEGIN
        g_emp_ids.
        DELETE;
        g_idx := 0;
        DBMS_OUTPUT.PUT_LINE ('--- Statement Start ---');
    END BEFORE STATEMENT;

    AFTER EACH ROW IS
    BEGIN
        INSERT INTO employee_history (employee_id, action, old_salary, new_salary, change_date)
        VALUES (NVL (:NEW.employee_id, :OLD.employee_id), 
            CASE
                WHEN INSERTING THEN
                    'INSERT'
                WHEN UPDATING THEN
                    'UPDATE'
                WHEN DELETING THEN
                    'DELETE'
            END, :OLD.salary, :NEW.salary, SYSTIMESTAMP);
    END AFTER EACH ROW;"#;

        let formatted = SqlEditorWidget::format_sql_basic(sql);
        let lines: Vec<&str> = formatted.lines().collect();
        let find_line = |prefix: &str| -> Option<&str> {
            lines
                .iter()
                .copied()
                .find(|line| line.trim_start().starts_with(prefix))
        };
        let leading_spaces = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let before_statement = find_line("BEFORE STATEMENT IS").unwrap_or("");
        let before_begin = lines
            .windows(2)
            .find(|pair| {
                pair[0].trim_start() == "BEFORE STATEMENT IS" && pair[1].trim_start() == "BEGIN"
            })
            .map(|pair| pair[1])
            .unwrap_or("");
        let before_body = find_line("g_emp_ids.").unwrap_or("");
        let after_each_row = find_line("AFTER EACH ROW IS").unwrap_or("");
        let after_begin = lines
            .windows(2)
            .find(|pair| {
                pair[0].trim_start() == "AFTER EACH ROW IS" && pair[1].trim_start() == "BEGIN"
            })
            .map(|pair| pair[1])
            .unwrap_or("");
        let insert_line = find_line("INSERT INTO employee_history").unwrap_or("");
        let values_line = find_line("VALUES (").unwrap_or("");
        let end_before = find_line("END BEFORE STATEMENT;").unwrap_or("");
        let end_after = find_line("END AFTER EACH ROW;").unwrap_or("");

        assert_eq!(
            leading_spaces(before_statement),
            4,
            "compound trigger timing header should stay at outer body depth, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(before_begin),
            4,
            "timing section BEGIN should align with its header, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(before_body),
            8,
            "statements in BEFORE section should be exactly one level deeper than BEGIN, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(after_each_row),
            4,
            "subsequent timing headers should keep the same compound-trigger base depth, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(after_begin),
            4,
            "subsequent timing BEGIN should align with its header, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(insert_line),
            8,
            "DML head inside timing section should stay one level deeper than BEGIN, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(values_line),
            8,
            "DML clause starters inside timing section should reuse the same query base depth, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(end_before),
            4,
            "END BEFORE STATEMENT should return to timing header depth, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(end_after),
            4,
            "END AFTER EACH ROW should return to timing header depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_compound_trigger_single_line_timing_sections_break_on_outer_body_state() {
        let sql = "CREATE OR REPLACE TRIGGER trg_employee_compound FOR INSERT OR UPDATE OR DELETE ON employees COMPOUND TRIGGER TYPE emp_id_set_t IS TABLE OF NUMBER INDEX BY PLS_INTEGER; g_emp_ids emp_id_set_t; AFTER EACH ROW IS BEGIN INSERT INTO employee_history (employee_id, action) VALUES (:NEW.employee_id, 'INSERT'); END AFTER EACH ROW; END trg_employee_compound;";
        let formatted = SqlEditorWidget::format_sql_basic(sql);
        let lines: Vec<&str> = formatted.lines().collect();
        let find_line = |prefix: &str| -> Option<&str> {
            lines
                .iter()
                .copied()
                .find(|line| line.trim_start().starts_with(prefix))
        };
        let leading_spaces = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let after_each_row = find_line("AFTER EACH ROW IS").unwrap_or("");
        let begin_line = lines
            .windows(2)
            .find(|pair| {
                pair[0].trim_start() == "AFTER EACH ROW IS" && pair[1].trim_start() == "BEGIN"
            })
            .map(|pair| pair[1])
            .unwrap_or("");
        let insert_line = find_line("INSERT INTO employee_history").unwrap_or("");
        let values_line = find_line("VALUES (").unwrap_or("");

        assert_eq!(
            leading_spaces(after_each_row),
            4,
            "single-line timing header should break at the compound trigger outer body depth, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(begin_line),
            4,
            "timing BEGIN from single-line input should align with the timing header, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(insert_line),
            8,
            "single-line timing DML head should stay one level deeper than BEGIN, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(values_line),
            8,
            "single-line timing DML clauses should reuse the same base depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn formats_values_subquery_with_nested_depth() {
        let sql = "SELECT 1 FROM dual WHERE EXISTS (VALUES (1));";
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        let expected = [
            "SELECT 1",
            "FROM DUAL",
            "WHERE EXISTS (",
            "        VALUES (1)",
            "    );",
        ]
        .join("\n");

        assert_eq!(formatted, expected);
    }

    #[test]
    fn formats_deeply_nested_subqueries_with_consistent_depth() {
        let sql = "SELECT outer_col FROM outer_t o WHERE EXISTS (SELECT 1 FROM (SELECT inner_col FROM inner_t i WHERE i.id IN (SELECT id FROM leaf_t WHERE status = 'Y')) nested_q WHERE nested_q.inner_col = o.outer_col);";
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        let expected = [
            "SELECT outer_col",
            "FROM outer_t o",
            "WHERE EXISTS (",
            "        SELECT 1",
            "        FROM (",
            "                SELECT inner_col",
            "                FROM inner_t i",
            "                WHERE i.id IN (",
            "                        SELECT id",
            "                        FROM leaf_t",
            "                        WHERE status = 'Y'",
            "                    )",
            "            ) nested_q",
            "        WHERE nested_q.inner_col = o.outer_col",
            "    );",
        ]
        .join("\n");

        assert_eq!(formatted, expected);
    }

    #[test]
    fn formats_nested_union_subquery_with_consistent_depth() {
        let sql = "SELECT o.id FROM outer_t o WHERE EXISTS (SELECT 1 FROM (SELECT i.id FROM inner_a i WHERE i.flag = 'Y' UNION ALL SELECT j.id FROM inner_b j WHERE j.flag = 'N') merged WHERE merged.id = o.id);";
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        let expected = [
            "SELECT o.id",
            "FROM outer_t o",
            "WHERE EXISTS (",
            "        SELECT 1",
            "        FROM (",
            "                SELECT i.id",
            "                FROM inner_a i",
            "                WHERE i.flag = 'Y'",
            "                UNION ALL",
            "                SELECT j.id",
            "                FROM inner_b j",
            "                WHERE j.flag = 'N'",
            "            ) merged",
            "        WHERE merged.id = o.id",
            "    );",
        ]
        .join("\n");

        assert_eq!(formatted, expected);
    }

    #[test]
    fn plsql_nested_with_subquery_keeps_cte_body_depth() {
        let sql = r#"BEGIN
  SELECT o.id
  INTO v_id
  FROM outer_t o
  WHERE EXISTS (
    WITH filt AS (
      SELECT id
      FROM inner_t
      WHERE flag = 'Y'
    )
    SELECT id
    FROM filt
    WHERE filt.id = o.id
  );
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains(
                "WITH filt AS (
                SELECT id
                FROM inner_t
                WHERE flag = 'Y'
            )"
            ),
            "CTE body inside nested subquery should indent one level deeper than WITH header, got:
{}",
            formatted
        );
    }

    #[test]
    fn plsql_if_then_with_query_uses_shared_base_depth_for_all_clauses() {
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
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains(
                "THEN\n        WITH filt AS (\n            SELECT id\n            FROM src_t"
            ),
            "WITH header and CTE body after THEN should keep a stable base/child depth split, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("\n        SELECT id\n        INTO v_id\n        FROM filt;"),
            "Main SELECT/INTO/FROM after THEN should reuse the shared query base depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn plsql_nested_union_subquery_keeps_consistent_depth() {
        let sql = r#"BEGIN
  SELECT o.id
  INTO v_id
  FROM outer_t o
  WHERE EXISTS (
    SELECT 1
    FROM (
      SELECT i.id
      FROM inner_a i
      WHERE i.flag = 'Y'
      UNION ALL
      SELECT j.id
      FROM inner_b j
      WHERE j.flag = 'N'
    ) merged
    WHERE merged.id = o.id
  );
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.chars().take_while(|c| *c == ' ').count();
        let from_idx = lines
            .iter()
            .position(|line| line.trim_start() == "FROM (")
            .unwrap_or(0);
        let nested_select_idx = lines
            .iter()
            .enumerate()
            .skip(from_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start().starts_with("SELECT i.id"))
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let union_idx = lines
            .iter()
            .position(|line| line.trim_start() == "UNION ALL")
            .unwrap_or(0);
        let union_select_idx = lines
            .iter()
            .enumerate()
            .skip(union_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start().starts_with("SELECT j.id"))
            .map(|(idx, _)| idx)
            .unwrap_or(0);

        assert!(
            indent(lines[nested_select_idx]) > indent(lines[from_idx]),
            "Nested subquery under FROM should increase depth, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[union_select_idx]),
            indent(lines[nested_select_idx]),
            "Set operator branch inside nested subquery should keep same nested depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn plsql_nested_with_multiple_ctes_keeps_cte_depth_aligned() {
        let sql = r#"BEGIN
  SELECT o.id
  INTO v_id
  FROM outer_t o
  WHERE EXISTS (
    WITH a AS (
      SELECT id
      FROM inner_t
    ), b AS (
      SELECT id
      FROM a
    )
    SELECT id
    FROM b
    WHERE b.id = o.id
  );
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        let expected = [
            "BEGIN",
            "    SELECT o.id",
            "    INTO v_id",
            "    FROM outer_t o",
            "    WHERE EXISTS (",
            "            WITH a AS (",
            "                SELECT id",
            "                FROM inner_t",
            "            ),",
            "            b AS (",
            "                SELECT id",
            "                FROM a",
            "            )",
            "            SELECT id",
            "            FROM b",
            "            WHERE b.id = o.id",
            "        );",
            "END;",
        ]
        .join("\n");

        assert_eq!(formatted, expected);
    }

    #[test]
    fn plsql_nested_with_clause_resets_excess_manual_indent() {
        let sql = r#"BEGIN
  SELECT o.id
  INTO v_id
  FROM outer_t o
  WHERE EXISTS (
                    WITH filt AS (
      SELECT id
      FROM inner_t
      WHERE flag = 'Y'
    )
    SELECT id
    FROM filt
    WHERE filt.id = o.id
  );
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains("WHERE EXISTS (
            WITH filt AS ("),
            "WITH clause should align to nested query depth instead of preserving excess manual indent, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_test26_reference_layout_exactly() {
        let expected = r#"PROCEDURE A (B IN NUMBER) AS
BEGIN
    SELECT D --4
    FROM E --4
    WHERE F IN (
            --4
            SELECT G -- 12
            FROM ( -- 12
                    SELECT H -- 20
                    FROM J -- 20
                    INNER JOIN K -- 20
                        ON 1 = 1 -- 24
                            AND 2 = 2 -- 28
                            OR 3 = 3 -- 28
                    OUTER JOIN K -- 20
                        ON 1 = 1 -- 24
                            AND 2 = 2 -- 28
                            OR 3 = 3 -- 28
                ) I -- 16
        ); -- 8
END A;

SELECT D
FROM E
WHERE F IN (
        SELECT G --8
        FROM ( --8
                SELECT H --16
                FROM J --16
                INNER JOIN K --16
                    ON 1 = 1 --20
                        AND 2 = 2 -- 24
                OUTER JOIN K -- 16
                    ON 1 = 1 --20
                        AND 2 = 2 -- 24
            ) I --12
    ); --4"#;

        let formatted = SqlEditorWidget::format_sql_basic(expected);

        assert_eq!(
            formatted, expected,
            "Formatting must preserve the reference base-depth layout exactly for nested IN/FROM subqueries with JOIN conditions"
        );
    }

    #[test]
    fn split_format_items_keeps_existing_inline_line_comment_after_semicolon() {
        let items = QueryExecutor::split_format_items("SELECT 1 FROM dual; -- keep terminator");

        assert_eq!(
            items.len(),
            1,
            "Trailing inline comment must stay in the same format item"
        );
        match &items[0] {
            FormatItem::Statement(statement) => {
                assert!(
                    statement.contains("; -- keep terminator"),
                    "Trailing inline comment should remain attached to the statement, got:\n{}",
                    statement
                );
            }
            other => panic!("Expected a statement item, got: {other:?}"),
        }
    }

    #[test]
    fn open_for_with_inline_block_comment_keeps_comment_inline_and_subquery_indented() {
        let sql = r#"FUNCTION get_employee_report (p_dept_id NUMBER, p_min_salary NUMBER DEFAULT 0) RETURN t_refcur IS
    l_rc t_refcur;
BEGIN
    OPEN l_rc FOR
        WITH base AS (
            SELECT e.emp_id,
                e.emp_name,
                e.salary,
                e.bonus_pct,
                d.dept_name, /*asdf*/
                NVL (
                    (
                    SELECT SUM (s.amount)
                    FROM qt_sales s
                    WHERE s.emp_id = e.emp_id
                ),
                0
                ) AS total_sales
            FROM qt_emp e
        )
        SELECT *
        FROM base;
END;"#;

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains(
                "d.dept_name, /*asdf*/\n                NVL (\n                    (\n                        SELECT SUM (s.amount)\n                        FROM qt_sales s\n                        WHERE s.emp_id = e.emp_id\n                    ),\n                    0\n                ) AS total_sales"
            ),
            "{}",
            formatted
        );
    }

    #[test]
    fn open_for_with_inline_line_comment_keeps_next_select_item_depth() {
        let sql = r#"FUNCTION get_employee_report (p_dept_id NUMBER, p_min_salary NUMBER DEFAULT 0) RETURN t_refcur IS
    l_rc t_refcur;
BEGIN
    OPEN l_rc FOR
        WITH base AS (
            SELECT e.emp_id,
                e.emp_name,
                e.salary,
                e.bonus_pct,
                d.dept_name, --asdf
                NVL (
                    (
                    SELECT SUM (s.amount)
                    FROM qt_sales s
                    WHERE s.emp_id = e.emp_id
                ),
                0
                ) AS total_sales
            FROM qt_emp e
        )
        SELECT *
        FROM base;
END;"#;

        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains(
                "d.dept_name, --asdf\n                NVL (\n                    (\n                        SELECT SUM (s.amount)\n                        FROM qt_sales s\n                        WHERE s.emp_id = e.emp_id\n                    ),\n                    0\n                ) AS total_sales"
            ),
            "{}",
            formatted
        );
    }

    #[test]
    fn open_for_with_inline_block_comment_after_select_keeps_first_select_item_depth() {
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

        let formatted = SqlEditorWidget::format_for_auto_formatting(sql, false);

        assert!(
            formatted.contains(
                "WITH /* A: dept 집계 CTE */\n        dept_stats AS (\n            SELECT /* B: dept 집계 */\n                deptno,"
            ),
            "inline block comment after SELECT in CTE body should keep the first select item on list depth, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains(
                "SELECT /* B: dept 집계 */\n            deptno,"
            ),
            "first select-list item after inline SELECT comment must not stay on the SELECT header depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn plsql_nested_with_clause_does_not_keep_two_level_extra_indent() {
        let sql = r#"BEGIN
    SELECT o.id
    INTO v_id
    FROM outer_t o
    WHERE EXISTS (
                WITH filt AS (
                    SELECT id
                    FROM inner_t
                    WHERE flag = 'Y'
                )
            SELECT id
            FROM filt
            WHERE filt.id = o.id
        );
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        assert!(
            formatted.contains(
                "WHERE EXISTS (
            WITH filt AS ("
            ),
            "WITH clause should not keep two-level extra indent in nested DML depth, got:
{}",
            formatted
        );
    }
    #[test]
    fn formats_nested_with_subquery_with_consistent_depth() {
        let sql = "SELECT o.id FROM outer_t o WHERE EXISTS (WITH filt AS (SELECT id FROM inner_t WHERE flag = 'Y') SELECT id FROM filt WHERE filt.id = o.id);";
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        let expected = [
            "SELECT o.id",
            "FROM outer_t o",
            "WHERE EXISTS (",
            "        WITH filt AS (",
            "            SELECT id",
            "            FROM inner_t",
            "            WHERE flag = 'Y'",
            "        )",
            "        SELECT id",
            "        FROM filt",
            "        WHERE filt.id = o.id",
            "    );",
        ]
        .join("\n");

        assert_eq!(formatted, expected);
    }

    #[test]
    fn formats_nested_cursor_expression_with_consistent_depth() {
        let sql = r#"SELECT
    d.dept_id,
    d.dept_name,
    CURSOR
    (
        SELECT
            e.emp_id,
            e.emp_no,
            e.emp_name,
            CURSOR
            (
                SELECT
                    s.sale_year,
                    SUM(s.sale_amount) AS total_sales
                FROM qt_sales s
                WHERE s.emp_id = e.emp_id
                GROUP BY s.sale_year
                ORDER BY s.sale_year
            ) AS sales_cur
        FROM qt_emp e
        WHERE e.dept_id = d.dept_id
        ORDER BY e.emp_id
    ) AS emp_cur
FROM qt_dept d
ORDER BY d.dept_id"#;
        let formatted = SqlEditorWidget::format_sql_basic(sql);

        let expected = [
            "SELECT d.dept_id,",
            "    d.dept_name,",
            "    CURSOR (",
            "        SELECT e.emp_id,",
            "            e.emp_no,",
            "            e.emp_name,",
            "            CURSOR (",
            "                SELECT s.sale_year,",
            "                    SUM (s.sale_amount) AS total_sales",
            "                FROM qt_sales s",
            "                WHERE s.emp_id = e.emp_id",
            "                GROUP BY s.sale_year",
            "                ORDER BY s.sale_year",
            "            ) AS sales_cur",
            "        FROM qt_emp e",
            "        WHERE e.dept_id = d.dept_id",
            "        ORDER BY e.emp_id",
            "    ) AS emp_cur",
            "FROM qt_dept d",
            "ORDER BY d.dept_id;",
        ]
        .join("\n");

        assert_eq!(formatted, expected);
    }
}

#[cfg(test)]
mod format_comment_indent_tests {
    use crate::ui::sql_editor::SqlEditorWidget;

    fn leading_spaces(line: &str) -> usize {
        line.len().saturating_sub(line.trim_start().len())
    }

    #[test]
    fn format_sql_basic_indents_line_comment_in_select_list_before_comma() {
        let source = "select col1\n-- comment\n,col2\nfrom t1;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("SELECT col1\n    -- comment\n    ,\n    col2\nFROM t1;"),
            "Line comment between select items should be indented at list item depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_indents_mid_list_comment_at_list_depth() {
        let source = "select col1\n-- comment2\n,col2\nfrom t1;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("    -- comment2\n    ,\n    col2"),
            "Mid-list comment (before comma) should be indented at list item depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_comment_at_base_indent_before_from_single_item() {
        let source = "select col1\n-- last column\nfrom t1;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("SELECT col1\n-- last column\nFROM t1;"),
            "Inter-clause comment before FROM should stay at base indent, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_indents_comment_in_subquery_select_list() {
        let source = "select * from (select col1\n-- comment\n,col2\nfrom t1);";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted
                .contains("SELECT col1\n            -- comment\n            ,\n            col2"),
            "Comment in subquery select list should be at list item depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_indents_comment_after_comma_in_open_for_select_list() {
        let source = "begin\n    open c for\n        select b,\n        -- comment\n            d\n        from e;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            formatted.contains("SELECT b,\n            -- comment\n            d\n        FROM e;"),
            "Line comment after a trailing comma should keep select-list depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_inline_comment_alignment_after_multiline_string_literal() {
        let source = "begin\n    a := 'b\n             b';     -- c\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();
        let assign_idx = lines
            .iter()
            .position(|line| line.contains("b';") && line.contains("-- c"))
            .unwrap_or(0);
        let assign_line = lines.get(assign_idx).copied().unwrap_or_default();

        assert!(
            assign_line.contains("b'; -- c"),
            "Inline comment spacing after multiline string literal should be preserved, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_indents_open_for_select_clauses_inside_cursor_body() {
        let source = "begin\n    select a\n    from b\n    where 1=1;\n    open cv for\n        select a,\n            b\n    from c,\n            d\n    where 1=1;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            formatted.contains(
                "OPEN cv FOR\n        SELECT a,\n            b\n        FROM c,\n            d\n        WHERE 1 = 1;"
            ),
            "OPEN ... FOR nested SELECT should keep FROM/WHERE aligned with SELECT body depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_connect_by_aligned_in_open_for_parenthesized_select() {
        let source = "BEGIN\n    OPEN cv FOR (SELECT employee_id\n                 FROM employees\n                 START WITH manager_id IS NULL\n                 CONNECT BY PRIOR employee_id = manager_id);\nEND;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();

        let select_indent = lines
            .iter()
            .find(|line| line.trim_start().starts_with("SELECT employee_id"))
            .map(|line| leading_spaces(line))
            .unwrap_or(0);
        let from_indent = lines
            .iter()
            .find(|line| line.trim_start().starts_with("FROM employees"))
            .map(|line| leading_spaces(line))
            .unwrap_or(0);
        let connect_by_indent = lines
            .iter()
            .find(|line| line.trim_start().starts_with("CONNECT BY PRIOR"))
            .map(|line| leading_spaces(line))
            .unwrap_or(0);

        assert!(
            select_indent > 0,
            "OPEN ... FOR SELECT line should exist, got:\n{}",
            formatted
        );
        assert_eq!(
            from_indent, select_indent,
            "FROM in OPEN ... FOR SELECT should align with SELECT, got:\n{}",
            formatted
        );
        assert_eq!(
            connect_by_indent, select_indent,
            "CONNECT BY in OPEN ... FOR SELECT should align with SELECT, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_nested_open_for_subquery_closing_paren_at_open_paren_indent() {
        let source = "BEGIN\n    OPEN cv FOR (SELECT e.employee_id\n                 FROM employees e\n                 WHERE EXISTS (SELECT 1\n                               FROM dual\n                               WHERE 1 = 1));\nEND;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();

        let open_paren_indent = lines
            .iter()
            .find(|line| line.trim() == "(")
            .map(|line| leading_spaces(line))
            .unwrap_or(0);
        let close_paren_indent = lines
            .iter()
            .rev()
            .find(|line| line.trim() == ");")
            .map(|line| leading_spaces(line))
            .unwrap_or(0);

        assert!(
            open_paren_indent > 0,
            "OPEN ... FOR parenthesized SELECT should include opening parenthesis line, got:\n{}",
            formatted
        );
        assert_eq!(
            close_paren_indent, open_paren_indent,
            "Closing parenthesis of OPEN ... FOR SELECT should align with opening parenthesis, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_nested_subquery_connect_by_under_inner_select_in_open_for() {
        let source = "BEGIN\n    OPEN cv FOR (SELECT root_id\n                 FROM roots r\n                 WHERE EXISTS (SELECT child_id\n                               FROM children c\n                               START WITH c.parent_id = r.root_id\n                               CONNECT BY PRIOR c.child_id = c.parent_id));\nEND;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();

        let inner_select_indent = lines
            .iter()
            .find(|line| line.trim_start().starts_with("SELECT child_id"))
            .map(|line| leading_spaces(line))
            .unwrap_or(0);
        let inner_connect_by_indent = lines
            .iter()
            .find(|line| line.trim_start().starts_with("CONNECT BY PRIOR c.child_id"))
            .map(|line| leading_spaces(line))
            .unwrap_or(0);

        assert!(
            inner_select_indent > 0,
            "Nested SELECT inside OPEN ... FOR should exist, got:\n{}",
            formatted
        );
        assert_eq!(
            inner_connect_by_indent, inner_select_indent,
            "Nested CONNECT BY should align with nested SELECT, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_non_select_parenthesis_pairs_aligned_in_open_for_select() {
        let source = "BEGIN
    OPEN cv FOR (SELECT e.employee_id
                 FROM employees e
                 WHERE (e.manager_id IS NULL
                        OR e.manager_id = 100));
END;";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            formatted.contains("WHERE (e.manager_id IS NULL\n                    OR e.manager_id = 100)"),
            "Non-SELECT parenthesis pair inside OPEN ... FOR WHERE should stay paired and indented, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_nested_non_select_parenthesis_alignment_in_open_for_select() {
        let source = "BEGIN
    OPEN cv FOR (SELECT e.employee_id
                 FROM employees e
                 WHERE (e.manager_id IN (100, 200)
                        OR e.department_id = 90));
END;";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            formatted.contains("WHERE (e.manager_id IN (100, 200)\n                    OR e.department_id = 90)"),
            "Nested non-SELECT parenthesis pairs in OPEN ... FOR WHERE should stay paired and indented, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_cursor_select_comment_at_select_list_depth_in_package_body() {
        let source = "create or replace package body pkg_fmt as\n    procedure validate_and_process (p_root_id in number, p_mode in varchar2 default 'NORMAL') is\n        cursor c_units (cp_root_id number) is\n        select id,\n            parent_id\n        -- comment\n            ,\n            code,\n            qty,\n            pri\n        from fmtx_unit;\n    begin\n        null;\n    end validate_and_process;\nend pkg_fmt;";
        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            formatted.contains(
                "SELECT id,\n            parent_id\n            -- comment\n            ,\n            code,\n            qty,\n            pri\n        FROM fmtx_unit;"
            ),
            "Cursor SELECT comment in package body should match select-list depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_cursor_select_block_comment_at_select_list_depth_in_package_body() {
        let source = "create or replace package body pkg_fmt as\n    procedure validate_and_process is\n        cursor c_units is\n        select id,\n            parent_id\n        /* keep\n           block */\n            ,\n            code\n        from fmtx_unit;\n    begin\n        null;\n    end validate_and_process;\nend pkg_fmt;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();
        let block_open_idx = lines
            .iter()
            .position(|line| line.trim() == "/* keep")
            .unwrap_or(0);
        let block_close_idx = lines
            .iter()
            .position(|line| line.trim() == "block */")
            .unwrap_or(0);
        let comma_idx = lines
            .iter()
            .position(|line| line.trim() == ",")
            .unwrap_or(0);
        let code_idx = lines
            .iter()
            .position(|line| line.trim() == "code")
            .unwrap_or(0);

        assert_eq!(
            leading_spaces(lines[block_open_idx]),
            leading_spaces(lines[code_idx]),
            "Block comment opener should match code depth, got:\n{}",
            formatted
        );
        assert_eq!(
            lines[block_close_idx], "           block */",
            "Multiline block comment internal line should preserve original indentation, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[comma_idx]),
            leading_spaces(lines[code_idx]),
            "Comma line should stay aligned with code after block comment, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_comment_aligned_before_case_close_paren() {
        let source = "begin\n    v_val := case\n        when flag = 1 then (\n            case\n                when score > 0 then 1\n                else 0\n            end\n            -- keep\n        )\n        else 2\n    end;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();
        let comment_idx = lines
            .iter()
            .position(|line| line.trim() == "-- keep")
            .unwrap_or(0);
        let close_paren_idx = lines
            .iter()
            .position(|line| line.trim() == ")")
            .unwrap_or(0);
        let else_idx = lines
            .iter()
            .enumerate()
            .skip(close_paren_idx.saturating_add(1))
            .find_map(|(idx, line)| {
                if line.trim() == "ELSE" {
                    Some(idx)
                } else {
                    None
                }
            })
            .unwrap_or(0);

        assert_eq!(
            leading_spaces(lines[comment_idx]),
            leading_spaces(lines[close_paren_idx]),
            "Comment before parenthesized CASE closer should match close-paren depth, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[close_paren_idx]),
            leading_spaces(lines[else_idx]),
            "Parenthesized CASE closer should align with following ELSE branch depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_comment_aligned_before_end_case_close_paren() {
        let source = "begin\n    v_val := case\n        when flag = 1 then (\n            case\n                when score > 0 then 1\n                else 0\n            end case\n            -- keep\n        )\n        else 2\n    end;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();
        let comment_idx = lines
            .iter()
            .position(|line| line.trim() == "-- keep")
            .unwrap_or(0);
        let close_paren_idx = lines
            .iter()
            .position(|line| line.trim() == ")")
            .unwrap_or(0);
        let else_idx = lines
            .iter()
            .enumerate()
            .skip(close_paren_idx.saturating_add(1))
            .find_map(|(idx, line)| {
                if line.trim() == "ELSE" {
                    Some(idx)
                } else {
                    None
                }
            })
            .unwrap_or(0);

        assert_eq!(
            leading_spaces(lines[comment_idx]),
            leading_spaces(lines[close_paren_idx]),
            "Comment before parenthesized END CASE closer should match close-paren depth, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[close_paren_idx]),
            leading_spaces(lines[else_idx]),
            "Parenthesized END CASE closer should align with following ELSE branch depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_open_cursor_using_comment_aligned_with_following_items() {
        let source = "BEGIN\n    IF a IS NOT NULL THEN\n        OPEN c FOR\n            b,\n            USING d,\n            -- e\n            f,\n                g;\n    END IF;\nEND;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert_eq!(
            formatted,
            "BEGIN\n    IF a IS NOT NULL THEN\n        OPEN c FOR\n            b,\n            USING d,\n                -- e\n                f,\n                g;\n    END IF;\nEND;",
            "OPEN ... FOR USING comment and continuation items should align one level deeper than USING, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_selected_if_fragment_using_comment_aligned_with_following_items() {
        let source = "IF a IS NOT NULL THEN\n    OPEN c FOR\n        b,\n        USING d,\n        -- e\n        f,\n            g;\nEND IF;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert_eq!(
            formatted,
            "IF a IS NOT NULL THEN\n    OPEN c FOR\n        b,\n        USING d,\n            -- e\n            f,\n            g;\nEND IF;",
            "Selected IF fragment should keep USING comment and continuation items one level deeper than USING, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_indents_comment_in_update_set_clause() {
        let source = "update t1 set col1 = 1\n-- comment\n,col2 = 2\n,col3 = 3;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("SET col1 = 1\n    -- comment\n    ,\n    col2 = 2"),
            "Comment in UPDATE SET clause should be at list item depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_indents_interleaved_comments_and_commas_in_select() {
        let source =
            "select\ncol1\n-- first section\n,col2\n,col3\n-- second section\n,col4\nfrom t1;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("    -- first section\n"),
            "First section comment should be indented, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("    -- second section\n"),
            "Second section comment should be indented, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_indents_comment_in_group_by_list() {
        let source = "select col1, col2, count(*) from t1 group by col1\n-- comment\n,col2;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("GROUP BY col1\n    -- comment\n    ,\n    col2;"),
            "Comment in GROUP BY list should be at list item depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_indents_comment_in_order_by_list() {
        let source = "select col1, col2 from t1 order by col1\n-- comment\n,col2;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("ORDER BY col1\n    -- comment\n    ,\n    col2;"),
            "Comment in ORDER BY list should be at list item depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_parenthesized_order_by_scalar_subquery_on_list_depth() {
        let source = r#"CREATE OR REPLACE PROCEDURE test_open_with_proc IS
    p_rc SYS_REFCURSOR;
BEGIN
    OPEN p_rc FOR
        WITH base AS (
            SELECT e.empno,
                e.ename,
                e.job,
                e.deptno,
                e.sal,
                e.comm,
                e.mgr
            FROM emp e
        ),
        enriched AS (
            SELECT b.*,
                NVL (b.comm, /* AZ: comm NULL→0 */
                    0) AS eff_comm,
                CASE
                    WHEN b.job = 'PRESIDENT' THEN
                        CASE
                            WHEN b.sal > /* BB: elite 기준 */
                            4000 THEN 'ELITE'
                            ELSE 'SENIOR'
                        END
                    WHEN b.job IN (
                        'MANAGER', 'ANALYST') THEN
                            CASE
                                WHEN b.sal >= 3000 THEN 'HIGH'
                                WHEN b.sal >= /* BD: mid 기준 */
                            2000 THEN 'MID'
                                ELSE 'LOW'
                            END
                    ELSE
                        CASE
                            WHEN NVL (b.comm, /* BE: NULL→0 */
                                0) > 0 THEN 'COMMISSION'
                            ELSE 'BASE'
                        END
                END AS pay_tier
            FROM base b
        )
        SELECT e.empno,
            e.ename,
            e.job,
            e.deptno,
            e.sal,
            e.pay_tier,
            (
                SELECT d.dname
                FROM dept d
                WHERE d.deptno = e.deptno
            ) AS dname
        FROM enriched e
        ORDER BY
        (
            SELECT COUNT(*)
            FROM emp e2
            WHERE e2.deptno = e.deptno
        ) DESC,
            e.sal DESC NULLS LAST;
END;
/"#;

        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();
        let leading_spaces = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let order_by_idx = lines
            .iter()
            .position(|line| line.trim_start() == "ORDER BY")
            .unwrap_or(0);
        let open_paren_idx = lines
            .iter()
            .enumerate()
            .skip(order_by_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "(")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let scalar_select_idx = lines
            .iter()
            .enumerate()
            .skip(open_paren_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start().starts_with("SELECT COUNT"))
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let close_paren_idx = lines
            .iter()
            .enumerate()
            .skip(scalar_select_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start().starts_with(") DESC,"))
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let second_sort_key_idx = lines
            .iter()
            .enumerate()
            .skip(close_paren_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start().starts_with("e.sal DESC NULLS LAST;"))
            .map(|(idx, _)| idx)
            .unwrap_or(0);

        assert!(
            leading_spaces(lines[open_paren_idx]) > leading_spaces(lines[order_by_idx]),
            "ORDER BY scalar-subquery sort key should indent deeper than ORDER BY header, got:\n{}",
            formatted
        );
        assert!(
            leading_spaces(lines[scalar_select_idx]) > leading_spaces(lines[open_paren_idx]),
            "Scalar subquery SELECT should indent deeper than its opening paren, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[close_paren_idx]),
            leading_spaces(lines[open_paren_idx]),
            "Scalar subquery close paren in ORDER BY should align with its opening paren, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[second_sort_key_idx]),
            leading_spaces(lines[open_paren_idx]),
            "Subsequent ORDER BY sort keys should align with the parenthesized sort key, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_indents_comment_in_from_list() {
        let source = "select * from t1\n-- join next table\n,t2\n,t3 where t1.id = t2.id;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("FROM t1\n    -- join next table\n    ,\n    t2"),
            "Comment in FROM table list should be at list item depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_indents_comment_in_returning_list() {
        let source = "insert into t1 values (1, 2) returning col1\n-- comment\n,col2 into v1, v2;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("RETURNING col1\n    -- comment\n    ,\n    col2"),
            "Comment in RETURNING list should be at list item depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_indents_comment_before_and_in_where() {
        let source = "select * from t1 where col1 = 1\n-- check col2\nand col2 = 2\n-- check col3\nand col3 = 3;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("    -- check col2\n    AND col2 = 2"),
            "Comment before AND should match AND indent, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("    -- check col3\n    AND col3 = 3"),
            "Comment before second AND should match AND indent, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_indents_comment_before_and_in_join_on() {
        let source =
            "select * from t1 join t2 on t1.id = t2.id\n-- extra condition\nand t1.status = 'A';";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("        -- extra condition\n        AND t1.status"),
            "Comment before AND in ON clause should be indented deeper than ON, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_indents_comment_in_insert_column_list() {
        let source = "insert into t1 (col1\n-- comment\n,col2\n,col3) values (1, 2, 3);";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("    -- comment\n    ,"),
            "Comment in INSERT column list should be indented, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_indents_comment_in_values_list() {
        let source = "insert into t1 (col1, col2, col3) values (1\n-- comment\n,2\n,3);";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("    -- comment\n    ,"),
            "Comment in VALUES list should be indented, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_indents_comment_in_create_table_columns() {
        let source = "create table t1 (col1 number\n-- comment\n,col2 varchar2(100)\n,col3 date);";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("col1 NUMBER,\n    -- comment\n    col2"),
            "Comment in CREATE TABLE column list should be at column indent, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_aligns_comment_with_else_in_plsql_if() {
        let source = "begin\nif true then\n-- do something\nnull;\n-- else branch\nelse\nnull;\nend if;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        // Comment before ELSE should be at same indent as ELSE (4 spaces)
        assert!(
            formatted.contains("\n    -- else branch\n    ELSE"),
            "Comment before ELSE should align with ELSE keyword, got:\n{}",
            formatted
        );
        // Comment inside IF body should be at body indent (8 spaces)
        assert!(
            formatted.contains("\n        -- do something\n"),
            "Comment inside IF body should be at body indent, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_aligns_comment_with_when_in_case() {
        let source = "select case\n-- first case\nwhen col1 = 1 then 'a'\n-- second case\nwhen col1 = 2 then 'b'\n-- default\nelse 'c'\nend from t1;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        // Comments before WHEN should be at same indent as WHEN (8 spaces)
        assert!(
            formatted.contains("        -- first case\n        WHEN"),
            "Comment before WHEN should align with WHEN keyword, got:\n{}",
            formatted
        );
        // Comment before ELSE in CASE should align with ELSE
        assert!(
            formatted.contains("        -- default\n        ELSE"),
            "Comment before ELSE in CASE should align with ELSE, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_aligns_comment_before_or_in_where() {
        let source = "select * from t1 where col1 = 1\n-- or branch\nor col2 = 2;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("    -- or branch\n    OR"),
            "Comment before OR should align with OR keyword, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_nested_plsql_comment_depth() {
        let source = "begin\nif true then\nfor i in 1..10 loop\n-- deep comment\nnull;\nend loop;\nend if;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("            -- deep comment\n            NULL;"),
            "Comment in nested PL/SQL block should be at correct depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_create_table_leading_comment_placement() {
        let source = "create table t1 (\n-- primary key\ncol1 number\n-- description\n,col2 varchar2(100)\n,col3 date);";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        // Leading comment before col1 should appear before col1
        assert!(
            formatted.contains("    -- primary key\n    col1 NUMBER,"),
            "Leading comment should appear before its column, got:\n{}",
            formatted
        );
        // Comment before col2 should appear before col2
        assert!(
            formatted.contains("    -- description\n    col2"),
            "Comment before col2 should appear before col2, got:\n{}",
            formatted
        );
    }

    // ── Pattern 1: next_is_else 누락 케이스 ──

    #[test]
    fn format_sql_basic_aligns_comment_before_exception() {
        let source = "begin\nnull;\n-- handle errors\nexception\nwhen others then\nnull;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        // EXCEPTION is at BEGIN level (indent 0), comment should match
        assert!(
            formatted.contains("\n-- handle errors\nEXCEPTION"),
            "Comment before EXCEPTION should align with EXCEPTION at base indent, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_aligns_comment_before_exception_nested() {
        let source =
            "begin\nif true then\nbegin\nnull;\n-- nested exception\nexception\nwhen others then\nnull;\nend;\nend if;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        // Nested BEGIN..EXCEPTION: comment should be at nested BEGIN level (8 spaces)
        assert!(
            formatted.contains("        -- nested exception\n        EXCEPTION"),
            "Comment before nested EXCEPTION should align with EXCEPTION, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_aligns_comment_before_elsif() {
        let source =
            "begin\nif a = 1 then\nnull;\n-- check second\nelsif a = 2 then\nnull;\n-- fallback\nelse\nnull;\nend if;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("    -- check second\n    ELSIF"),
            "Comment before ELSIF should align with ELSIF, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("    -- fallback\n    ELSE"),
            "Comment before ELSE should align with ELSE, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_aligns_block_comment_before_else_in_plsql_if() {
        let source = "begin\nif true then\nnull;\n/* else branch */\nelse\nnull;\nend if;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("    /* else branch */\n    ELSE"),
            "Block comment before ELSE should align with ELSE keyword, got:\n{}",
            formatted
        );
    }

    // ── Pattern 2: next_is_condition_keyword 누락 케이스 ──

    #[test]
    fn format_sql_basic_aligns_comment_before_when_in_plsql_exception() {
        let source =
            "begin\nnull;\nexception\n-- not found\nwhen no_data_found then\nnull;\n-- others\nwhen others then\nnull;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("    -- not found\n    WHEN"),
            "Comment before WHEN in EXCEPTION should align with WHEN, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("    -- others\n    WHEN"),
            "Comment before second WHEN in EXCEPTION should align with WHEN, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_aligns_comment_before_on_in_join() {
        let source = "select * from t1 join t2\n-- join condition\non t1.id = t2.id;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("    -- join condition\n    ON"),
            "Comment before ON in JOIN should align with ON keyword, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_aligns_comment_before_and_in_having() {
        let source =
            "select col1, count(*) from t1 group by col1 having count(*) > 1\n-- extra filter\nand sum(col2) > 10;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("    -- extra filter\n    AND"),
            "Comment before AND in HAVING should align with AND keyword, got:\n{}",
            formatted
        );
    }

    // ── Pattern 3: in_confirmed_list 누락 케이스 ──

    #[test]
    fn format_sql_basic_indents_comment_in_having_list() {
        let source = "select col1, col2, count(*) from t1 group by col1\n-- comment\n,col2;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("    -- comment\n    ,"),
            "Comment in GROUP BY list should be at list item depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_indents_comment_in_window_clause_list() {
        let source = "select col1 from t1 order by col1\n-- secondary sort\n,col2;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("    -- secondary sort\n    ,"),
            "Comment between ORDER BY items should be at list item depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_indents_comment_in_merge_matched_set_list() {
        let source = "merge into t1 using t2 on (t1.id = t2.id) when matched then update set t1.col1 = t2.col1\n-- update col2\n,t1.col2 = t2.col2;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("    -- update col2\n    ,"),
            "Comment in MERGE UPDATE SET list should be at list item depth, got:\n{}",
            formatted
        );
    }

    // ── paren_extra: 괄호 내부에서 condition/ELSE 주석 정렬 ──

    #[test]
    fn format_sql_basic_aligns_comment_before_and_inside_parens() {
        let source = "select * from t1 where func(col1 = 1\n-- extra\nand col2 = 2);";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();
        let comment_line = lines.iter().find(|l| l.contains("-- extra")).unwrap();
        let and_line = lines
            .iter()
            .find(|l| l.trim_start().starts_with("AND col2"))
            .unwrap();
        assert_eq!(
            leading_spaces(comment_line),
            leading_spaces(and_line),
            "Comment before AND inside parens should match AND indent, got:\n{}",
            formatted
        );
    }

    #[test]
    fn apply_parser_depth_indentation_keeps_window_order_by_under_over_paren_after_comment() {
        let source = r#"SELECT o.order_id,
    x.max_price,
    -- [AU] 분위수 계산
    NTILE (4) OVER (
/* AV: 금액 기준 분위 */
ORDER BY x.total_amt DESC NULLS LAST) AS amt_quartile
FROM recent_orders o;"#;

        let formatted = SqlEditorWidget::apply_parser_depth_indentation(source);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let ntile_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("NTILE (4) OVER ("))
            .unwrap_or(0);
        let comment_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("/* AV: 금액 기준 분위 */"))
            .unwrap_or(0);
        let order_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("ORDER BY x.total_amt"))
            .unwrap_or(0);

        assert!(
            indent(lines[comment_idx]) > indent(lines[ntile_idx]),
            "window comment line should stay nested under OVER (, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[comment_idx]),
            indent(lines[order_idx]),
            "comment and ORDER BY inside OVER (...) should share the same continuation depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn apply_parser_depth_indentation_keeps_from_under_general_function_paren() {
        let source = r#"SELECT
    OVERLAY (
        name
        PLACING 'X'
FROM start_pos
FOR 1
    ) AS masked_name
FROM emp;"#;

        let formatted = SqlEditorWidget::apply_parser_depth_indentation(source);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let overlay_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("OVERLAY ("))
            .unwrap_or(0);
        let placing_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("PLACING 'X'"))
            .unwrap_or(0);
        let from_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("FROM start_pos"))
            .unwrap_or(0);
        let for_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("FOR 1"))
            .unwrap_or(0);

        assert!(
            indent(lines[from_idx]) > indent(lines[overlay_idx]),
            "FROM inside OVERLAY (...) should stay nested under the function paren, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[placing_idx]),
            indent(lines[from_idx]),
            "clause-shaped keywords inside a general function paren should align with sibling continuation lines, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[from_idx]),
            indent(lines[for_idx]),
            "FROM/FOR inside OVERLAY (...) should share one continuation depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_aligns_comment_before_else_in_case_inside_parens() {
        let source = "select func(case when col1 = 1 then 'a'\n-- default\nelse 'b' end) from t1;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();
        let comment_line = lines.iter().find(|l| l.contains("-- default")).unwrap();
        let else_line = lines
            .iter()
            .find(|l| l.trim_start().starts_with("ELSE"))
            .unwrap();
        assert_eq!(
            leading_spaces(comment_line),
            leading_spaces(else_line),
            "Comment before ELSE in CASE inside parens should match ELSE indent, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_aligns_comment_before_when_in_case_inside_parens() {
        let source =
            "select func(case\n-- first branch\nwhen col1 = 1 then 'a'\n-- second branch\nwhen col1 = 2 then 'b' end) from t1;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();
        let comment_line = lines
            .iter()
            .find(|l| l.contains("-- first branch"))
            .unwrap();
        let when_line = lines
            .iter()
            .find(|l| l.trim_start().starts_with("WHEN col1 = 1"))
            .unwrap();
        assert_eq!(
            leading_spaces(comment_line),
            leading_spaces(when_line),
            "Comment before WHEN in CASE inside parens should match WHEN indent, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_aligns_comment_before_or_inside_parens() {
        let source = "select * from t1 where (col1 = 1\n-- alt condition\nor col2 = 2);";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();
        let comment_line = lines
            .iter()
            .find(|l| l.contains("-- alt condition"))
            .unwrap();
        let or_line = lines
            .iter()
            .find(|l| l.trim_start().starts_with("OR col2"))
            .unwrap();
        assert_eq!(
            leading_spaces(comment_line),
            leading_spaces(or_line),
            "Comment before OR inside parens should match OR indent, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_condition_operand_after_inline_block_comment_on_condition_depth() {
        let source = "select 1 from order_item oi where oi.order_id = v.order_id and oi.qty <= /* X: 0 이하 */ 0;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let expected = [
            "SELECT 1",
            "FROM order_item oi",
            "WHERE oi.order_id = v.order_id",
            "    AND oi.qty <= /* X: 0 이하 */",
            "    0;",
        ]
        .join("\n");

        assert_eq!(formatted, expected);
    }

    #[test]
    fn format_sql_basic_keeps_inline_comment_operand_depth_inside_not_exists_subquery() {
        let source = r#"SELECT *
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
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());
        let and_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND oi.qty <="))
            .expect("formatted SQL should contain the NOT EXISTS operand owner line");
        let operand_idx = lines
            .iter()
            .position(|line| line.trim() == "0")
            .expect("formatted SQL should contain the split operand line");
        let close_idx = lines
            .iter()
            .enumerate()
            .skip(operand_idx.saturating_add(1))
            .find(|(_, line)| line.trim() == ")")
            .map(|(idx, _)| idx)
            .expect("formatted SQL should contain the NOT EXISTS closing parenthesis");

        assert_eq!(
            indent(lines[operand_idx]),
            indent(lines[and_idx]),
            "operand after inline block comment inside NOT EXISTS should stay at the active condition indent, got:\n{}",
            formatted
        );
        assert!(
            indent(lines[close_idx]) < indent(lines[operand_idx]),
            "closing parenthesis after the operand should dedent from the NOT EXISTS condition body, got:\n{}",
            formatted
        );

        let reformatted = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            reformatted, formatted,
            "formatting should be stable for NOT EXISTS inline-comment operand continuation"
        );
    }
}

#[cfg(test)]
mod format_indent_gap_tests {
    use crate::ui::sql_editor::SqlEditorWidget;

    fn leading_spaces(line: &str) -> usize {
        line.len().saturating_sub(line.trim_start().len())
    }

    fn find_line_starting_with(lines: &[&str], prefix: &str) -> Option<usize> {
        lines
            .iter()
            .position(|line| line.trim_start().starts_with(prefix))
    }

    // ── CURSOR IS/AS SELECT body indent ──

    #[test]
    fn format_sql_basic_indents_cursor_is_select_body() {
        let source = "declare\ncursor c1 is\nselect col1, col2\nfrom t1\nwhere col1 > 0;\nbegin\nnull;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("    CURSOR c1 IS\n        SELECT"),
            "SELECT body should be indented under CURSOR IS, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("        FROM t1"),
            "FROM should be at CURSOR body depth, got:\n{}",
            formatted
        );
        // BEGIN should return to DECLARE level
        assert!(
            formatted.contains("BEGIN\n    NULL;"),
            "BEGIN should return to top level after CURSOR, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_indents_cursor_with_params_is_select_body() {
        let source = "declare\ncursor c1 (p1 number) is\nselect col1\nfrom t1\nwhere col1 = p1;\nbegin\nnull;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("    CURSOR c1 (p1 NUMBER) IS\n        SELECT"),
            "Parameterized CURSOR SELECT should be indented, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_indents_nested_cursor_declaration() {
        let source =
            "begin\ndeclare\ncursor c1 is\nselect col1 from t1;\nbegin\nopen c1;\nend;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("        CURSOR c1 IS\n            SELECT"),
            "Nested CURSOR SELECT should be indented, got:\n{}",
            formatted
        );
    }

    // ── FORALL body indent ──

    #[test]
    fn format_sql_basic_indents_forall_insert_body() {
        let source = "begin\nforall i in 1..v_arr.count\ninsert into t1 values (v_arr(i));\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("    FORALL i IN 1..v_arr.COUNT\n        INSERT INTO"),
            "FORALL INSERT body should be indented, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_indents_forall_update_body() {
        let source = "begin\nforall i in 1..v_arr.count\nupdate t1 set col1 = v_arr(i)\nwhere id = v_id(i);\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("    FORALL i IN 1..v_arr.COUNT\n        UPDATE"),
            "FORALL UPDATE body should be indented, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_indents_forall_delete_body() {
        let source = "begin\nforall i in 1..v_arr.count\ndelete from t1 where id = v_arr(i);\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("    FORALL i IN 1..v_arr.COUNT\n        DELETE"),
            "FORALL DELETE body should be indented, got:\n{}",
            formatted
        );
    }

    // ── EXECUTE IMMEDIATE INTO/USING inline ──

    #[test]
    fn format_sql_basic_keeps_execute_immediate_into_using_inline() {
        let source =
            "begin\nexecute immediate 'select 1 from dual' into v_result using v_param;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted
                .contains("EXECUTE IMMEDIATE 'select 1 from dual' INTO v_result USING v_param;"),
            "EXECUTE IMMEDIATE INTO/USING should stay inline, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_execute_immediate_using_only_inline() {
        let source = "begin\nexecute immediate v_sql using v_param1, v_param2;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("EXECUTE IMMEDIATE v_sql USING v_param1, v_param2;"),
            "EXECUTE IMMEDIATE USING should stay inline, got:\n{}",
            formatted
        );
    }

    // ── CREATE INDEX ON inline ──

    #[test]
    fn format_sql_basic_keeps_create_index_on_inline() {
        let source = "create index idx1 on t1 (col1, col2);";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("CREATE INDEX idx1 ON t1"),
            "CREATE INDEX ON should stay on same line, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_create_unique_index_on_inline() {
        let source = "create unique index idx1 on t1 (col1, col2) tablespace users;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("CREATE UNIQUE INDEX idx1 ON t1"),
            "CREATE UNIQUE INDEX ON should stay on same line, got:\n{}",
            formatted
        );
    }

    // ── COMMENT ON inline ──

    #[test]
    fn format_sql_basic_keeps_comment_on_table_inline() {
        let source = "comment on table t1 is 'This is a table';";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("COMMENT ON TABLE"),
            "COMMENT ON TABLE should stay inline, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_comment_on_column_inline() {
        let source = "comment on column t1.col1 is 'description';";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("COMMENT ON COLUMN"),
            "COMMENT ON COLUMN should stay inline, got:\n{}",
            formatted
        );
    }

    // ── GRANT/REVOKE inline ──

    #[test]
    fn format_sql_basic_keeps_grant_inline() {
        let source = "grant select, insert on schema1.t1 to user1;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("GRANT SELECT, INSERT ON"),
            "GRANT privileges should stay inline, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_revoke_inline() {
        let source = "revoke select on schema1.t1 from user1;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("REVOKE SELECT ON"),
            "REVOKE privileges should stay inline, got:\n{}",
            formatted
        );
    }

    // ── MERGE WHEN MATCHED indent ──

    #[test]
    fn format_sql_basic_merge_when_matched_indent() {
        let source = "merge into t1 using t2 on (t1.id = t2.id) when matched then update set t1.col = t2.col when not matched then insert (col) values (t2.col);";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();
        // WHEN MATCHED should be at base indent
        let when_matched = lines.iter().find(|l| l.contains("WHEN MATCHED")).unwrap();
        assert!(
            !when_matched.starts_with("    "),
            "WHEN MATCHED should not be deeply indented, got:\n{}",
            formatted
        );
        // UPDATE should be indented under WHEN MATCHED
        let update_line = lines
            .iter()
            .find(|l| l.trim_start().starts_with("UPDATE"))
            .unwrap();
        assert!(
            update_line.starts_with("    "),
            "UPDATE should be indented under WHEN MATCHED, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_merge_branches_keep_branch_depths_stable() {
        let source = "merge into emp_bonus b using src_bonus s on (b.empno = s.empno) when matched then update set b.bonus_amount = case when s.calc_bonus > 1000 then s.calc_bonus else s.calc_bonus + 100 end, b.updated_at = systimestamp where s.sal > 0 and (b.bonus_amount is null or b.bonus_amount <> s.calc_bonus) delete where s.sal < 500 when not matched then insert (b.empno, b.deptno, b.bonus_amount, b.created_at, b.note_text) values (s.empno, s.deptno, s.calc_bonus, systimestamp, case when s.calc_bonus >= 500 then 'HIGH' else 'LOW' end);";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();
        let leading_spaces = |line: &str| line.len().saturating_sub(line.trim_start().len());
        let find_line = |prefix: &str| -> Option<&str> {
            lines
                .iter()
                .copied()
                .find(|line| line.trim_start().starts_with(prefix))
        };

        let when_matched = find_line("WHEN MATCHED THEN").unwrap_or("");
        let update_set = find_line("UPDATE SET").unwrap_or("");
        let update_where = find_line("WHERE s.sal > 0").unwrap_or("");
        let delete = find_line("DELETE").unwrap_or("");
        let delete_where = find_line("WHERE s.sal < 500").unwrap_or("");
        let when_not_matched = find_line("WHEN NOT MATCHED THEN").unwrap_or("");
        let insert = find_line("INSERT (").unwrap_or("");
        let insert_values_inline = lines.iter().copied().find(|line| {
            let trimmed = line.trim_start();
            trimmed.starts_with("INSERT (") && trimmed.contains(" VALUES (")
        });
        let values = find_line("VALUES (").or(insert_values_inline).unwrap_or("");

        assert!(
            !when_matched.is_empty()
                && !update_set.is_empty()
                && !update_where.is_empty()
                && !delete.is_empty()
                && !delete_where.is_empty()
                && !when_not_matched.is_empty()
                && !insert.is_empty()
                && !values.is_empty(),
            "expected all MERGE branch lines to be present, got:\n{}",
            formatted
        );

        assert_eq!(
            leading_spaces(when_matched),
            leading_spaces(when_not_matched),
            "MERGE branch headers should share the same base depth, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(update_set),
            leading_spaces(update_where),
            "UPDATE and its WHERE should stay on the same branch depth, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(delete),
            leading_spaces(delete_where),
            "DELETE and its WHERE should stay on the same branch depth, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(insert),
            leading_spaces(values),
            "INSERT branch body should keep VALUES on the same depth, got:\n{}",
            formatted
        );
        assert!(
            leading_spaces(update_set) > leading_spaces(when_matched),
            "UPDATE branch body should be deeper than WHEN MATCHED, got:\n{}",
            formatted
        );
        assert!(
            leading_spaces(insert) > leading_spaces(when_not_matched),
            "INSERT branch body should be deeper than WHEN NOT MATCHED, got:\n{}",
            formatted
        );
    }

    // ── FETCH INTO inline ──

    #[test]
    fn format_sql_basic_keeps_fetch_into_inline() {
        let source = "begin\nfetch cur into v_rec;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("FETCH cur INTO v_rec;"),
            "FETCH INTO should stay inline, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_fetch_bulk_collect_into_inline() {
        let source = "begin\nfetch cur bulk collect into v_tab limit 100;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("FETCH cur BULK COLLECT INTO v_tab LIMIT 100;"),
            "FETCH BULK COLLECT INTO LIMIT should stay inline, got:\n{}",
            formatted
        );
    }

    // ── RETURNING INTO inline ──

    #[test]
    fn format_sql_basic_keeps_returning_into_inline() {
        let source = "begin\ninsert into t1 (a) values (1) returning id into v_id;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("RETURNING id INTO v_id;"),
            "RETURNING INTO should stay inline, got:\n{}",
            formatted
        );
    }

    // ── BULK COLLECT INTO inline (in SELECT) ──

    #[test]
    fn format_sql_basic_keeps_bulk_collect_into_inline() {
        let source = "begin\nselect col bulk collect into v_tab from t1;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("BULK COLLECT INTO v_tab"),
            "BULK COLLECT INTO should stay inline, got:\n{}",
            formatted
        );
    }

    // ── SELECT FOR UPDATE inline ──

    #[test]
    fn format_sql_basic_keeps_for_update_inline() {
        let source = "select a, b from t1 where id = 1 for update of a nowait;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("FOR UPDATE OF"),
            "FOR UPDATE should stay inline, got:\n{}",
            formatted
        );
    }

    // ── ON DELETE CASCADE inline ──

    #[test]
    fn format_sql_basic_keeps_on_delete_cascade_inline() {
        let source = "alter table t1 add constraint fk1 foreign key (col1) references t2(id) on delete cascade;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("ON DELETE CASCADE"),
            "ON DELETE CASCADE should stay inline, got:\n{}",
            formatted
        );
    }

    // ── CREATE SEQUENCE inline ──

    #[test]
    fn format_sql_basic_keeps_create_sequence_inline() {
        let source = "create sequence seq1 start with 1 increment by 1 nocache;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            !formatted.contains("\nSTART"),
            "CREATE SEQUENCE START WITH should not break, got:\n{}",
            formatted
        );
    }

    // ── CREATE SYNONYM FOR inline ──

    #[test]
    fn format_sql_basic_keeps_create_synonym_for_inline() {
        let source = "create or replace synonym syn1 for schema1.t1;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("SYNONYM syn1 FOR schema1"),
            "CREATE SYNONYM FOR should stay inline, got:\n{}",
            formatted
        );
    }

    // ── LISTAGG WITHIN GROUP inline ──

    #[test]
    fn format_sql_basic_keeps_listagg_within_group_inline() {
        let source = "select deptno, listagg(ename, ', ') within group (order by ename) as names from emp group by deptno;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("WITHIN GROUP"),
            "LISTAGG WITHIN GROUP should stay inline, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_sum_case_indented_inside_with_plsql_cte() {
        let source = r#"WITH
    FUNCTION fmt_mask (p_txt IN VARCHAR2) RETURN VARCHAR2 IS
    BEGIN
        RETURN REGEXP_REPLACE (NVL (p_txt, 'NULL'), '([[:alnum:]])', '*');
    END fmt_mask,
    PROCEDURE noop (p_msg IN VARCHAR2) IS
    BEGIN
        NULL;
    END noop,
    dept_stat AS (
        SELECT
            f.deptno,
            COUNT (*) AS cnt_emp,
            SUM (
            CASE
                WHEN f.band = 'TOP' THEN 1
                ELSE 0
            END
            ) AS cnt_top,
            LISTAGG (f.ename, ', ') WITHIN GROUP (ORDER BY f.sal DESC, f.empno) AS emp_list
        FROM filtered_emp f
        GROUP BY f.deptno
    )
SELECT
    *
FROM dept_stat;"#;
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();

        let leading_spaces = |line: &str| line.len().saturating_sub(line.trim_start().len());
        let sum_idx = lines
            .iter()
            .position(|line| line.trim_start() == "SUM (")
            .unwrap_or(0);
        let case_idx = lines
            .iter()
            .enumerate()
            .skip(sum_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "CASE")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let when_idx = lines
            .iter()
            .enumerate()
            .skip(case_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start().starts_with("WHEN f.band = 'TOP' THEN 1"))
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let else_idx = lines
            .iter()
            .enumerate()
            .skip(when_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "ELSE 0")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let end_idx = lines
            .iter()
            .enumerate()
            .skip(else_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "END")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let close_idx = lines
            .iter()
            .enumerate()
            .skip(end_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start().starts_with(") AS cnt_top"))
            .map(|(idx, _)| idx)
            .unwrap_or(0);

        assert!(
            leading_spaces(lines[case_idx]) > leading_spaces(lines[sum_idx]),
            "CASE inside SUM should indent deeper than SUM header, got:\n{}",
            formatted
        );
        assert!(
            leading_spaces(lines[when_idx]) > leading_spaces(lines[case_idx]),
            "WHEN inside aggregate CASE should indent deeper than CASE, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[else_idx]),
            leading_spaces(lines[when_idx]),
            "ELSE inside aggregate CASE should align with WHEN, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[end_idx]),
            leading_spaces(lines[case_idx]),
            "END inside aggregate CASE should align with CASE, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[close_idx]),
            leading_spaces(lines[sum_idx]),
            "closing SUM parenthesis should align with SUM header, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_does_not_insert_blank_line_after_with_function_terminator_before_cte() {
        let source = r#"WITH
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

        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            !formatted.contains("END calc_depth;\n\nrecursive_tree"),
            "WITH FUNCTION terminator should stay attached to the following CTE, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_for_auto_formatting_keeps_recursive_cte_union_branch_select_on_cte_body_depth() {
        let source = r#"WITH r (node_id, parent_id, node_name, lvl, PATH) AS (
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

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, false);
        let expected = r#"WITH r (node_id, parent_id, node_name, lvl, PATH) AS (
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

        assert_eq!(
            formatted, expected,
            "Recursive CTE set-operator branches should keep the branch SELECT on the same CTE body depth"
        );
    }

    #[test]
    fn format_for_auto_formatting_keeps_non_recursive_cte_union_branch_select_on_cte_body_depth() {
        let source = r#"WITH src AS (
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

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, false);
        let expected = r#"WITH src AS (
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

        assert_eq!(
            formatted, expected,
            "Non-recursive CTE set-operator branches should keep the branch SELECT on the same CTE body depth"
        );
    }

    #[test]
    fn format_sql_basic_collapses_blank_line_before_case_select_item() {
        let source = r#"WITH dept_data AS (
    SELECT
        10 AS dept_id,
        'DEV' AS dept_name
    FROM DUAL
),
emp_data AS (
    SELECT
        1001 AS emp_id,
        10 AS dept_id,
        'ALICE' AS emp_name,
        9000 AS salary
    FROM DUAL
)
SELECT
    d.dept_id,
    e.emp_id,
    e.salary,

    CASE
        WHEN e.salary >= 9000 THEN 'TOP'
        ELSE 'OTHER'
    END AS salary_band
FROM dept_data d
JOIN emp_data e
    ON e.dept_id = d.dept_id;"#;

        let formatted = SqlEditorWidget::format_sql_basic(source);

        assert!(
            formatted.contains("e.salary,\n    CASE"),
            "CASE select item should stay directly after the previous comma-terminated item, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("e.salary,\n\n    CASE"),
            "Formatter should not keep an extra blank line before CASE in the SELECT list, got:\n{}",
            formatted
        );
    }

    // ── PIVOT FOR inline ──

    #[test]
    fn format_sql_basic_keeps_pivot_for_inline() {
        let source = "select * from t1 pivot (sum(amount) for category in ('A' as a, 'B' as b));";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("FOR category"),
            "PIVOT FOR should stay inline, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_extract_from_inline_inside_function() {
        let source = "select extract(year from d.dt) as yyyy from dual d;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("YEAR FROM d.dt")
                && !formatted.contains(
                    "YEAR
FROM"
                ),
            "EXTRACT(... FROM ...) should stay inline inside function, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_json_returning_inline_inside_function() {
        let source =
            "select json_value(e.json_profile, '$.level' returning varchar2(30)) as profile_level from emp e;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("'$.level' RETURNING VARCHAR2 (30)"),
            "JSON RETURNING should stay inline inside function parens, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_breaks_cross_apply_after_join_condition() {
        let source = "select * from org_enriched oe join qt_fmt_dept d on d.dept_id = oe.dept_id cross apply (select 1 as x from dual) ca;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains(
                "ON d.dept_id = oe.dept_id
CROSS APPLY ("
            ),
            "CROSS APPLY should start on a new line after JOIN ON, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_for_auto_formatting_keeps_cross_apply_aggregate_subquery_layout_exactly() {
        let source = "select d.department_name, emp_stats.avg_sal, emp_stats.emp_count, top_emp.employee_name, top_emp.salary from departments d cross apply (select avg(e.salary) as avg_sal, count(*) as emp_count, max(e.salary) as max_sal from employees e where e.department_id = d.department_id having count(*) > 5) emp_stats outer apply (select e2.first_name || ' ' || e2.last_name as employee_name, e2.salary from employees e2 where e2.department_id = d.department_id and e2.salary = emp_stats.max_sal fetch first 1 row only) top_emp where emp_stats.avg_sal > (select avg(salary) from employees);";
        let expected = r#"SELECT d.department_name,
       emp_stats.avg_sal,
       emp_stats.emp_count,
       top_emp.employee_name,
       top_emp.salary
FROM departments d
CROSS APPLY (
    SELECT AVG(e.salary) AS avg_sal,
           COUNT(*) AS emp_count,
           MAX(e.salary) AS max_sal
    FROM employees e
    WHERE e.department_id = d.department_id
    HAVING COUNT(*) > 5
) emp_stats
OUTER APPLY (
    SELECT e2.first_name || ' ' || e2.last_name AS employee_name,
           e2.salary
    FROM employees e2
    WHERE e2.department_id = d.department_id
      AND e2.salary = emp_stats.max_sal
    FETCH FIRST 1 ROW ONLY
) top_emp
WHERE emp_stats.avg_sal > (
    SELECT AVG(salary)
    FROM employees
);"#;

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, false);

        assert_eq!(
            formatted, expected,
            "Auto formatting should keep APPLY aggregate subqueries on the exact expected base depths"
        );
        assert_eq!(
            SqlEditorWidget::format_for_auto_formatting(expected, false),
            expected,
            "Auto formatting should be stable for the expected APPLY layout"
        );
    }

    #[test]
    fn format_for_auto_formatting_keeps_general_paren_frame_depth_for_close_continuations() {
        let source = r#"SELECT 'DEPT=' || d.dept_name || ' | EMP=' || e.emp_name || ' | SALES=' || TO_CHAR (NVL (
    (
        SELECT SUM ((s.qty * s.unit_price) - s.discount_amt + s.tax_amt)
        FROM qt_fmt_sales s
        WHERE s.emp_id = e.emp_id
    ), 0
)) || ' | JSON_LEVEL=' || JSON_VALUE (e.json_profile, '$.level' RETURNING VARCHAR2 (30)) || ' | HIER=' || (
    SELECT MAX (SYS_CONNECT_BY_PATH (x.dept_code, '/'))
    FROM qt_fmt_dept x
    START WITH x.dept_id = d.dept_id
    CONNECT BY PRIOR x.parent_dept_id = x.dept_id
) AS summary_line
FROM qt_fmt_emp e
JOIN qt_fmt_dept d
    ON d.dept_id = e.dept_id
ORDER BY e.emp_id;"#;

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, false);
        let expected = r#"SELECT 'DEPT=' || d.dept_name || ' | EMP=' || e.emp_name || ' | SALES=' || TO_CHAR (NVL (
        (
            SELECT SUM ((s.qty * s.unit_price) - s.discount_amt + s.tax_amt)
            FROM qt_fmt_sales s
            WHERE s.emp_id = e.emp_id
        ), 0
    )) || ' | JSON_LEVEL=' || JSON_VALUE (e.json_profile, '$.level' RETURNING VARCHAR2 (30)) || ' | HIER=' || (
        SELECT MAX (SYS_CONNECT_BY_PATH (x.dept_code, '/'))
        FROM qt_fmt_dept x
        START WITH x.dept_id = d.dept_id
        CONNECT BY PRIOR x.parent_dept_id = x.dept_id
    ) AS summary_line
FROM qt_fmt_emp e
JOIN qt_fmt_dept d
    ON d.dept_id = e.dept_id
ORDER BY e.emp_id;"#;

        assert_eq!(
            formatted, expected,
            "General parenthesis frames should preserve close-paren continuation depth and nested child-query base depth"
        );
        assert_eq!(
            SqlEditorWidget::format_for_auto_formatting(expected, false),
            expected,
            "Auto formatting should stay stable after general parenthesis frame normalization"
        );
    }

    #[test]
    fn format_for_auto_formatting_keeps_json_object_value_scalar_subquery_on_paren_frame_depth() {
        let source = r#"SELECT d.dept_id,
    d.dept_name,
    JSON_ARRAYAGG (JSON_OBJECT ('empId' VALUE e.emp_id, 'name' VALUE e.emp_name, 'job' VALUE e.job_title, 'salary' VALUE e.salary, 'sales' VALUE (
        SELECT NVL (SUM ((s.qty * s.unit_price) - s.discount_amt + s.tax_amt), 0)
        FROM qt_fmt_sales s
        WHERE s.emp_id = e.emp_id
            ), 'meta' VALUE JSON_OBJECT ('grade' VALUE e.grade_no, 'status' VALUE e.status, 'hireDate' VALUE TO_CHAR (e.hire_date, 'YYYY-MM-DD')) RETURNING CLOB) ORDER BY e.salary DESC, e.emp_id RETURNING CLOB) AS dept_emp_json
FROM qt_fmt_dept d
LEFT JOIN qt_fmt_emp e
    ON e.dept_id = d.dept_id
GROUP BY d.dept_id,
    d.dept_name
ORDER BY d.dept_id;"#;

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, false);
        let expected = r#"SELECT d.dept_id,
    d.dept_name,
    JSON_ARRAYAGG (JSON_OBJECT ('empId' VALUE e.emp_id, 'name' VALUE e.emp_name, 'job' VALUE e.job_title, 'salary' VALUE e.salary, 'sales' VALUE (
            SELECT NVL (SUM ((s.qty * s.unit_price) - s.discount_amt + s.tax_amt), 0)
            FROM qt_fmt_sales s
            WHERE s.emp_id = e.emp_id
        ), 'meta' VALUE JSON_OBJECT ('grade' VALUE e.grade_no, 'status' VALUE e.status, 'hireDate' VALUE TO_CHAR (e.hire_date, 'YYYY-MM-DD')) RETURNING CLOB) ORDER BY e.salary DESC, e.emp_id RETURNING CLOB) AS dept_emp_json
FROM qt_fmt_dept d
LEFT JOIN qt_fmt_emp e
    ON e.dept_id = d.dept_id
GROUP BY d.dept_id,
    d.dept_name
ORDER BY d.dept_id;"#;

        assert_eq!(
            formatted, expected,
            "Scalar subquery under JSON_OBJECT VALUE should inherit the active parenthesis frame depth"
        );
        assert_eq!(
            SqlEditorWidget::format_for_auto_formatting(expected, false),
            expected,
            "JSON_OBJECT VALUE scalar subquery formatting should remain stable"
        );
    }

    #[test]
    fn format_for_auto_formatting_keeps_cte_close_depth_after_pivot_and_unpivot_blocks() {
        let source = r#"WITH src AS (
    SELECT
        10 AS dept_id,
        'ALICE' AS emp_name,
        'Q1' AS qtr,
        100 AS amt
    FROM DUAL
    UNION ALL
        SELECT
            10,
            'ALICE',
            'Q2',
            120
        FROM DUAL
        UNION ALL
        SELECT
            10,
            'BOB',
            'Q1',
            90
        FROM DUAL
        UNION ALL
        SELECT
            10,
            'BOB',
            'Q2',
            150
        FROM DUAL
        UNION ALL
        SELECT
            20,
            'DAVE',
            'Q1',
            80
        FROM DUAL
        UNION ALL
        SELECT
            20,
            'DAVE',
            'Q2',
            110
        FROM DUAL
        UNION ALL
        SELECT
            20,
            'ERIN',
            'Q1',
            140
        FROM DUAL
        UNION ALL
        SELECT
            20,
            'ERIN',
            'Q2',
            130
        FROM DUAL
),
p AS (
    SELECT *
    FROM src PIVOT (
        SUM (amt)
        FOR qtr IN ('Q1' AS q1, 'Q2' AS q2)
    )
        ),
u AS (
            SELECT *
            FROM p UNPIVOT (
                amt
                FOR qtr IN (q1 AS 'Q1', q2 AS 'Q2')
            )
                )
SELECT
    u.dept_id,
    u.emp_name,
    MAX (
        CASE
            WHEN u.qtr = 'Q1' THEN u.amt
        END
                ) AS q1_amt,
                MAX (
                    CASE
                        WHEN u.qtr = 'Q2' THEN u.amt
                    END
                ) AS q2_amt,
                CASE
                    WHEN MAX (
                        CASE
                            WHEN u.qtr = 'Q2' THEN u.amt
                        END
                    ) > MAX (
                        CASE
                            WHEN u.qtr = 'Q1' THEN u.amt
                        END
                    ) THEN 'UP'
                    WHEN MAX (
                        CASE
                            WHEN u.qtr = 'Q2' THEN u.amt
                        END
                    ) < MAX (
                        CASE
                            WHEN u.qtr = 'Q1' THEN u.amt
                        END
                    ) THEN 'DOWN'
                    ELSE 'SAME'
                END AS trend_flag
FROM u
GROUP BY u.dept_id,
    u.emp_name
ORDER BY u.dept_id,
    u.emp_name;"#;

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, false);
        let expected = r#"WITH src AS (
    SELECT
        10 AS dept_id,
        'ALICE' AS emp_name,
        'Q1' AS qtr,
        100 AS amt
    FROM DUAL
    UNION ALL
    SELECT
        10,
        'ALICE',
        'Q2',
        120
    FROM DUAL
    UNION ALL
    SELECT
        10,
        'BOB',
        'Q1',
        90
    FROM DUAL
    UNION ALL
    SELECT
        10,
        'BOB',
        'Q2',
        150
    FROM DUAL
    UNION ALL
    SELECT
        20,
        'DAVE',
        'Q1',
        80
    FROM DUAL
    UNION ALL
    SELECT
        20,
        'DAVE',
        'Q2',
        110
    FROM DUAL
    UNION ALL
    SELECT
        20,
        'ERIN',
        'Q1',
        140
    FROM DUAL
    UNION ALL
    SELECT
        20,
        'ERIN',
        'Q2',
        130
    FROM DUAL
),
p AS (
    SELECT *
    FROM src PIVOT (
        SUM (amt)
        FOR qtr IN ('Q1' AS q1, 'Q2' AS q2)
    )
),
u AS (
    SELECT *
    FROM p UNPIVOT (
        amt
        FOR qtr IN (q1 AS 'Q1', q2 AS 'Q2')
    )
)
SELECT
    u.dept_id,
    u.emp_name,
    MAX (
        CASE
            WHEN u.qtr = 'Q1' THEN u.amt
        END
    ) AS q1_amt,
    MAX (
        CASE
            WHEN u.qtr = 'Q2' THEN u.amt
        END
    ) AS q2_amt,
    CASE
        WHEN MAX (
            CASE
                WHEN u.qtr = 'Q2' THEN u.amt
            END
        ) > MAX (
            CASE
                WHEN u.qtr = 'Q1' THEN u.amt
            END
        ) THEN 'UP'
        WHEN MAX (
            CASE
                WHEN u.qtr = 'Q2' THEN u.amt
            END
        ) < MAX (
            CASE
                WHEN u.qtr = 'Q1' THEN u.amt
            END
        ) THEN 'DOWN'
        ELSE 'SAME'
    END AS trend_flag
FROM u
GROUP BY u.dept_id,
    u.emp_name
ORDER BY u.dept_id,
    u.emp_name;"#;

        assert_eq!(
            formatted, expected,
            "CTE close parens after PIVOT/UNPIVOT blocks should return to the CTE owner depth instead of inheriting inner block depth"
        );
        assert_eq!(
            SqlEditorWidget::format_for_auto_formatting(expected, false),
            expected,
            "Auto formatting should stay stable for CTE close-paren depth after PIVOT/UNPIVOT blocks"
        );
    }

    #[test]
    fn format_sql_basic_keeps_inline_comment_after_cross_apply_open_paren() {
        let source = "select * from org_enriched oe cross apply (-- inline\nselect oe.dept_id from dual\n) ca;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("CROSS APPLY ( -- inline"),
            "inline comment after CROSS APPLY ( should stay on the opener line, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("SELECT oe.dept_id"),
            "CROSS APPLY body should still be formatted as a nested query, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_inline_comment_after_merge_using_open_paren() {
        let source = "merge into dst d using (-- inline\nselect 1 as id from dual\n) s on (d.id = s.id) when matched then update set d.id = s.id;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("USING ( -- inline"),
            "inline comment after MERGE USING ( should stay on the opener line, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("SELECT 1 AS id"),
            "MERGE USING body should still be formatted as a nested query, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_keeps_search_cycle_set_inline() {
        let source = "with t as (select 1 as dept_id, 'X' as dept_name from dual union all select 2, 'Y' from dual) search depth first by dept_name set dfs_order cycle dept_id set is_cycle to 'Y' default 'N' select * from t;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("SEARCH DEPTH FIRST BY dept_name SET dfs_order"),
            "SEARCH ... SET should stay in a single clause line, got:
{}",
            formatted
        );
        assert!(
            formatted.contains("CYCLE dept_id SET is_cycle TO 'Y' DEFAULT 'N'"),
            "CYCLE ... SET should stay in a single clause line, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_breaks_match_recognize_subclauses() {
        let source = "select * from sales match_recognize (partition by emp_id order by sale_date, sale_id measures match_number() as match_no all rows per match pattern (low+ mid* high) define low as amount < 100, mid as amount between 100 and 500, high as amount > 500);";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains(
                "ORDER BY sale_date,
        sale_id
    MEASURES"
            ),
            "MEASURES should start on its own line inside MATCH_RECOGNIZE, got:
{}",
            formatted
        );
        assert!(
            formatted.contains(
                "ALL ROWS PER MATCH
    PATTERN"
            ),
            "PATTERN should start on its own line inside MATCH_RECOGNIZE, got:
{}",
            formatted
        );
        assert!(
            formatted.contains(
                ")
    DEFINE"
            ),
            "DEFINE should start on its own line inside MATCH_RECOGNIZE, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_breaks_model_subclauses() {
        let source = "select * from sales model partition by (year_key, channel_code) dimension by (month_key) measures (base_amt, proj_amt) rules sequential order (proj_amt[any] = base_amt[cv(month_key)]);";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains(
                "MODEL
    PARTITION BY"
            ),
            "MODEL PARTITION BY should break to a new line, got:
{}",
            formatted
        );
        assert!(
            formatted.contains(
                ")
    DIMENSION BY"
            ),
            "MODEL DIMENSION BY should break to a new line, got:
{}",
            formatted
        );
        assert!(
            formatted.contains(
                ")
    MEASURES"
            ),
            "MODEL MEASURES should break to a new line, got:
{}",
            formatted
        );
        assert!(
            formatted.contains(
                ")
    RULES"
            ),
            "MODEL RULES should break to a new line, got:
{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_breaks_pivot_and_unpivot_blocks() {
        let pivot_source =
            "select * from sales pivot (sum(amount) for category in ('A' as a, 'B' as b));";
        let pivot_formatted = SqlEditorWidget::format_sql_basic(pivot_source);
        assert!(
            pivot_formatted.contains(
                "PIVOT (
"
            ),
            "PIVOT block should be treated as subquery-like block for formatting, got:
{}",
            pivot_formatted
        );

        let unpivot_source =
            "select * from t unpivot (comp_value for comp_type in (salary as 'SALARY', bonus as 'BONUS'));";
        let unpivot_formatted = SqlEditorWidget::format_sql_basic(unpivot_source);
        assert!(
            unpivot_formatted.contains(
                "UNPIVOT (
"
            ),
            "UNPIVOT block should be treated as subquery-like block for formatting, got:
{}",
            unpivot_formatted
        );
    }

    #[test]
    fn format_for_auto_formatting_keeps_from_aligned_before_unpivot_after_multiline_select_list() {
        let source = r#"CREATE OR REPLACE PROCEDURE test_open_with_proc IS
    p_rc SYS_REFCURSOR;
BEGIN
    OPEN p_rc FOR
        WITH src AS (
            SELECT deptno,
                job,
                sal
            FROM emp
        ),
        pivoted AS (
            SELECT *
            FROM src PIVOT (
                SUM (sal) AS sum_sal FOR
                deptno IN (
                    10 AS D10, 20 AS D20, 30 AS D30)
            )
        )
        SELECT job,
            dept_tag,
            sal_amt
        FROM pivoted UNPIVOT (
            sal_amt
            FOR dept_tag IN (
                D10 AS '10', D20 AS '20', D30 AS '30')
        )
        WHERE sal_amt IS NOT NULL
        ORDER BY job,
            dept_tag;
END;"#;

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, false);

        assert!(
            formatted.contains(
                "        SELECT job,\n            dept_tag,\n            sal_amt\n        FROM pivoted UNPIVOT ("
            ),
            "FROM before UNPIVOT should realign to the SELECT base depth instead of staying at select-item depth, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains(
                "        SELECT job,\n            dept_tag,\n            sal_amt\n            FROM pivoted UNPIVOT ("
            ),
            "FROM before UNPIVOT must not remain indented like a select-list continuation, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_for_auto_formatting_keeps_already_aligned_from_before_unpivot_stable() {
        let expected = r#"CREATE OR REPLACE PROCEDURE test_open_with_proc IS
    p_rc SYS_REFCURSOR;
BEGIN
    OPEN p_rc FOR
        WITH src AS (
            SELECT deptno,
                job,
                sal
            FROM emp
        ),
        pivoted AS (
            SELECT *
            FROM src PIVOT (
                SUM (sal) AS sum_sal
                FOR deptno IN (10 AS D10, 20 AS D20, 30 AS D30)
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
            dept_tag;
END;"#;

        let reformatted = SqlEditorWidget::format_for_auto_formatting(expected, false);

        assert_eq!(
            reformatted, expected,
            "already aligned FROM before UNPIVOT should remain stable"
        );
    }

    #[test]
    fn apply_parser_depth_indentation_keeps_from_before_unpivot_stable() {
        let expected = r#"CREATE OR REPLACE PROCEDURE test_open_with_proc IS
    p_rc SYS_REFCURSOR;
BEGIN
    OPEN p_rc FOR
        WITH src AS (
            SELECT deptno,
                job,
                sal
            FROM emp
        ),
        pivoted AS (
            SELECT *
            FROM src PIVOT (
                SUM (sal) AS sum_sal
                FOR deptno IN (10 AS D10, 20 AS D20, 30 AS D30)
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
            dept_tag;
END;"#;

        let indented = SqlEditorWidget::apply_parser_depth_indentation(expected);

        assert_eq!(
            indented, expected,
            "parser-depth alignment phase should not over-indent FROM before UNPIVOT"
        );
    }

    #[test]
    fn format_for_auto_formatting_keeps_json_table_comment_attached_and_select_items_aligned() {
        let source = r#"CREATE OR REPLACE PROCEDURE test_open_with_proc IS
    p_rc SYS_REFCURSOR;
BEGIN
       OPEN p_rc FOR
        WITH jdocs AS (
            SELECT id,
                payload
            FROM json_docs
            WHERE /* AL: 활성 문서만 */
            active_flag = 1
        )
        SELECT jd.id,

            /* AM: JSON 파싱 결과 */
            jt.order_id,
               jt.cust_name,
               jt.tier,
               it.sku,
               it.qty,
               it.price,
               (it.qty * it.price) AS line_amt
        FROM jdocs jd
        CROSS JOIN JSON_TABLE (jd.payload, 
            /* AN: root path */
            '$' COLUMNS (
                -- [AO] 최상위 컬럼
                order_id NUMBER PATH '$.order_id', cust_name VARCHAR2 (100) PATH '$.customer.name', tier VARCHAR2 (20) PATH '$.customer.tier', NESTED PATH '$.items[*]' COLUMNS (
                    /* AP: 아이템 컬럼 */
                    sku VARCHAR2 (30) PATH '$.sku', qty NUMBER PATH '$.qty', price NUMBER PATH '$.price') -- [AQ] nested columns 끝
            )        -- [AR] outer columns 끝
        )         jt
        CROSS APPLY (
            -- [AS] item alias
            SELECT jt.sku,
                jt.qty,
                jt.price
            FROM DUAL
        ) it
        ORDER BY jd.id,
            it.sku;
END;"#;

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, false);

        assert!(
            formatted.contains(
                "        SELECT jd.id,\n            /* AM: JSON 파싱 결과 */\n            jt.order_id,\n            jt.cust_name,\n            jt.tier,"
            ),
            "JSON_TABLE select-list comment should stay attached to the preceding SELECT item block and the following items should share one depth, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("SELECT jd.id,\n\n            /* AM: JSON 파싱 결과 */"),
            "formatter should not insert a blank line before the JSON_TABLE select-list comment, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_for_auto_formatting_breaks_json_table_columns_clause_into_aligned_items() {
        let source = r#"CREATE OR REPLACE PROCEDURE test_open_with_proc IS
    p_rc SYS_REFCURSOR;
BEGIN
     OPEN p_rc FOR
        WITH jdocs AS (
            SELECT id,
                payload
            FROM json_docs
            WHERE /* AL: 활성 문서만 */
            active_flag = 1
        )
        SELECT jd.id,
            /* AM: JSON 파싱 결과 */
            jt.order_id,
            jt.cust_name,
            jt.tier,
            it.sku,
            it.qty,
            it.price,
            (it.qty * it.price) AS line_amt
        FROM jdocs jd
        CROSS JOIN JSON_TABLE (jd.payload, 
            /* AN: root path */
            '$' COLUMNS (
                -- [AO] 최상위 컬럼
                order_id NUMBER PATH '$.order_id', cust_name VARCHAR2 (100) PATH '$.customer.name', tier VARCHAR2 (20) PATH '$.customer.tier', NESTED PATH '$.items[*]' COLUMNS (
                    /* AP: 아이템 컬럼 */
                    sku VARCHAR2 (30) PATH '$.sku', qty NUMBER PATH '$.qty', price NUMBER PATH '$.price') -- [AQ] nested columns 끝
            )        -- [AR] outer columns 끝
        )         jt
        CROSS APPLY (
            -- [AS] item alias
            SELECT jt.sku,
                jt.qty,
                jt.price
            FROM DUAL
        ) it
        ORDER BY jd.id,
            it.sku;
END;"#;

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, false);

        assert!(
            formatted.contains(
                "        CROSS JOIN JSON_TABLE (jd.payload,\n            /* AN: root path */\n            '$' COLUMNS (\n                -- [AO] 최상위 컬럼\n                order_id NUMBER PATH '$.order_id',\n                cust_name VARCHAR2 (100) PATH '$.customer.name',\n                tier VARCHAR2 (20) PATH '$.customer.tier',\n                NESTED PATH '$.items[*]' COLUMNS (\n                    /* AP: 아이템 컬럼 */\n                    sku VARCHAR2 (30) PATH '$.sku',\n                    qty NUMBER PATH '$.qty',\n                    price NUMBER PATH '$.price'\n                ) -- [AQ] nested columns 끝\n            ) -- [AR] outer columns 끝\n        ) jt"
            ),
            "JSON_TABLE COLUMNS clause should reuse structured column-list layout, got:\n{}",
            formatted
        );
    }

    // ── INSERT ALL INTO indent ──

    #[test]
    fn format_sql_basic_insert_all_into_indented() {
        let source = "insert all into t1 (a) values (1) into t2 (b) values (2) select * from dual;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();
        let into_line = lines
            .iter()
            .find(|l| l.trim_start().starts_with("INTO t1"))
            .unwrap();
        assert!(
            into_line.starts_with("    "),
            "INSERT ALL INTO should be indented, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_insert_all_case_inside_values_keeps_expression_depth() {
        let source = r#"INSERT ALL
    WHEN total_amount > 10000 THEN
        INTO high_value_orders (order_id, customer_id, amount, order_date)
        VALUES (oid, cid, total_amount, odate)
        INTO vip_notifications (customer_id, message, created)
        VALUES (cid, 'High value order: ' || TO_CHAR (total_amount, 'FM$999,999.00'), SYSDATE)
    WHEN total_amount BETWEEN 1000 AND 10000 THEN
        INTO medium_value_orders (order_id, customer_id, amount)
        VALUES (oid, cid, total_amount)
    WHEN category = 'ELECTRONICS' THEN
        INTO electronics_orders (order_id, amount, warranty_end)
        VALUES (oid, total_amount, ADD_MONTHS (odate,
    CASE
        WHEN total_amount > 5000 THEN 24
            ELSE 12
    END
    ))
    ELSE
        INTO standard_orders (order_id, customer_id, amount)
        VALUES (oid, cid, total_amount)
SELECT o.order_id AS oid,
    o.customer_id AS cid,
    o.total_amount,
    o.order_date AS odate,
    p.category
FROM orders o
JOIN products p
    ON o.product_id = p.product_id
WHERE o.order_date >= TRUNC (SYSDATE, 'MM');"#;

        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();
        let leading_spaces = |line: &str| line.len().saturating_sub(line.trim_start().len());
        let values_idx = lines
            .iter()
            .position(|line| {
                line.trim_start()
                    .starts_with("VALUES (oid, total_amount, ADD_MONTHS (odate,")
            })
            .unwrap_or(0);
        let case_idx = lines
            .iter()
            .enumerate()
            .skip(values_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "CASE")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let when_idx = lines
            .iter()
            .enumerate()
            .skip(case_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "WHEN total_amount > 5000 THEN 24")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let else_idx = lines
            .iter()
            .enumerate()
            .skip(when_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "ELSE 12")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let end_idx = lines
            .iter()
            .enumerate()
            .skip(else_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "END")
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let close_idx = lines
            .iter()
            .enumerate()
            .skip(end_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start() == "))")
            .map(|(idx, _)| idx)
            .unwrap_or(0);

        assert!(
            leading_spaces(lines[case_idx]) > leading_spaces(lines[values_idx]),
            "CASE inside INSERT ALL VALUES should indent deeper than VALUES, got:\n{}",
            formatted
        );
        assert!(
            leading_spaces(lines[when_idx]) > leading_spaces(lines[case_idx]),
            "WHEN inside INSERT ALL VALUES CASE should indent deeper than CASE, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[else_idx]),
            leading_spaces(lines[when_idx]),
            "ELSE inside INSERT ALL VALUES CASE should align with WHEN, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[end_idx]),
            leading_spaces(lines[case_idx]),
            "END inside INSERT ALL VALUES CASE should align with CASE, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[close_idx]),
            leading_spaces(lines[values_idx]),
            ")) after INSERT ALL VALUES CASE should realign with VALUES, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_package_body_open_cursor_nested_subquery_keeps_expected_query_base_depth() {
        let input = r#"create package body a as
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
        let expected = r#"create package body a as
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

        let formatted = SqlEditorWidget::format_sql_basic(input);
        assert_eq!(
            formatted.trim().to_ascii_lowercase(),
            expected.trim().to_ascii_lowercase(),
            "package body OPEN cursor nested subquery should keep expected query base depth (case-insensitive), got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_end_case_in_dml_aligns_with_case_keyword() {
        let source = r#"SELECT
    col1,
    CASE
        WHEN col2 = 1 THEN 'a'
        ELSE 'b'
    END CASE AS result
FROM t1;"#;

        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();
        let leading_spaces = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let case_idx = lines
            .iter()
            .position(|line| line.trim_start() == "CASE")
            .expect("CASE line should exist");
        let end_case_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("END CASE"))
            .expect("END CASE line should exist");

        assert_eq!(
            leading_spaces(lines[case_idx]),
            leading_spaces(lines[end_case_idx]),
            "END CASE should align with CASE keyword, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_nested_case_end_case_in_dml_preserves_depth() {
        let source = r#"SELECT
    CASE
        WHEN a = 1 THEN
            CASE
                WHEN b = 1 THEN 'x'
                ELSE 'y'
            END CASE
        ELSE 'z'
    END AS result
FROM t1;"#;

        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();
        let leading_spaces = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let outer_case_idx = lines
            .iter()
            .position(|line| line.trim_start() == "CASE")
            .expect("outer CASE should exist");
        let inner_case_idx = lines
            .iter()
            .enumerate()
            .skip(outer_case_idx + 1)
            .find(|(_, line)| line.trim_start() == "CASE")
            .map(|(idx, _)| idx)
            .expect("inner CASE should exist");
        let end_case_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("END CASE"))
            .expect("END CASE should exist");
        let outer_else_idx = lines
            .iter()
            .enumerate()
            .skip(end_case_idx + 1)
            .find(|(_, line)| line.trim_start().starts_with("ELSE"))
            .map(|(idx, _)| idx)
            .expect("outer ELSE should exist");

        assert_eq!(
            leading_spaces(lines[inner_case_idx]),
            leading_spaces(lines[end_case_idx]),
            "END CASE should align with inner CASE, got:\n{}",
            formatted
        );
        assert!(
            leading_spaces(lines[inner_case_idx]) > leading_spaces(lines[outer_case_idx]),
            "Inner CASE should indent deeper than outer CASE, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[outer_else_idx]),
            leading_spaces(lines[outer_case_idx]).saturating_add(4),
            "Outer ELSE should indent one level deeper than outer CASE, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_for_auto_formatting_nested_case_after_then_uses_branch_body_depth() {
        let source = r#"SELECT CASE WHEN score > avg_score THEN CASE WHEN bonus >= 300 THEN 'TOP_WITH_BONUS' ELSE 'TOP_NO_BIG_BONUS' END ELSE CASE WHEN grade IN ('A', 'B') THEN 'MID_GOOD_GRADE' ELSE 'MID_OTHER' END END AS emp_class FROM dual;"#;

        let formatted = SqlEditorWidget::format_for_auto_formatting(source, false);
        let lines: Vec<&str> = formatted.lines().collect();
        let leading_spaces = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let outer_when_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("WHEN score > avg_score THEN"))
            .expect("outer WHEN should exist");
        let first_inner_case_idx = lines
            .iter()
            .enumerate()
            .skip(outer_when_idx + 1)
            .find(|(_, line)| line.trim_start() == "CASE")
            .map(|(idx, _)| idx)
            .expect("inner CASE after THEN should exist");
        let outer_else_idx = lines
            .iter()
            .enumerate()
            .skip(first_inner_case_idx + 1)
            .find(|(_, line)| line.trim_start() == "ELSE")
            .map(|(idx, _)| idx)
            .expect("outer ELSE should exist");
        let second_inner_case_idx = lines
            .iter()
            .enumerate()
            .skip(outer_else_idx + 1)
            .find(|(_, line)| line.trim_start() == "CASE")
            .map(|(idx, _)| idx)
            .expect("inner CASE after ELSE should exist");

        assert_eq!(
            leading_spaces(lines[first_inner_case_idx]),
            leading_spaces(lines[outer_when_idx]).saturating_add(4),
            "CASE after THEN should indent one level deeper than the WHEN branch header, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[second_inner_case_idx]),
            leading_spaces(lines[outer_else_idx]).saturating_add(4),
            "CASE after ELSE should indent one level deeper than the ELSE branch header, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_case_when_after_clause_starter_opens_frame() {
        let source = r#"SELECT
    CASE
        WHEN col1 = 1 THEN 'a'
        WHEN col1 = 2 THEN 'b'
        ELSE 'c'
    END AS result,
    col2
FROM t1;"#;

        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();
        let leading_spaces = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let case_idx = lines
            .iter()
            .position(|line| line.trim_start() == "CASE")
            .expect("CASE should exist");
        let when1_idx = lines
            .iter()
            .enumerate()
            .skip(case_idx + 1)
            .find(|(_, line)| line.trim_start().starts_with("WHEN col1 = 1"))
            .map(|(idx, _)| idx)
            .expect("first WHEN should exist");
        let when2_idx = lines
            .iter()
            .enumerate()
            .skip(when1_idx + 1)
            .find(|(_, line)| line.trim_start().starts_with("WHEN col1 = 2"))
            .map(|(idx, _)| idx)
            .expect("second WHEN should exist");
        let else_idx = lines
            .iter()
            .enumerate()
            .skip(when2_idx + 1)
            .find(|(_, line)| line.trim_start().starts_with("ELSE"))
            .map(|(idx, _)| idx)
            .expect("ELSE should exist");
        let end_idx = lines
            .iter()
            .enumerate()
            .skip(else_idx + 1)
            .find(|(_, line)| line.trim_start().starts_with("END"))
            .map(|(idx, _)| idx)
            .expect("END should exist");

        assert!(
            leading_spaces(lines[when1_idx]) > leading_spaces(lines[case_idx]),
            "WHEN should indent deeper than CASE, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[when1_idx]),
            leading_spaces(lines[when2_idx]),
            "All WHEN branches should align, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[when1_idx]),
            leading_spaces(lines[else_idx]),
            "ELSE should align with WHEN, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[end_idx]),
            leading_spaces(lines[case_idx]),
            "END should align with CASE, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_mid_line_case_after_comma_opens_frame() {
        // Stage 1 keeps CASE on the same line as the preceding comma item,
        // so the DmlCaseLayoutFrame must be pushed retroactively.
        let source = "SELECT col1, CASE\nWHEN col2 = 1 THEN 'a'\nELSE 'b'\nEND AS result\nFROM t1;";

        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();
        let leading_spaces = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let when_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("WHEN"))
            .expect("WHEN should exist");
        let else_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("ELSE"))
            .expect("ELSE should exist");
        let end_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("END"))
            .expect("END should exist");

        assert_eq!(
            leading_spaces(lines[when_idx]),
            leading_spaces(lines[else_idx]),
            "ELSE should align with WHEN when CASE opened mid-line, got:\n{}",
            formatted
        );
        assert!(
            leading_spaces(lines[when_idx]) > leading_spaces(lines[end_idx]),
            "WHEN should indent deeper than END when CASE opened mid-line, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_mid_line_case_in_update_set_opens_frame() {
        let source = "UPDATE t1\nSET col1 = CASE\nWHEN col2 = 1 THEN 'a'\nELSE 'b'\nEND;";

        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();
        let leading_spaces = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let when_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("WHEN"))
            .expect("WHEN should exist");
        let else_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("ELSE"))
            .expect("ELSE should exist");
        let end_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("END"))
            .expect("END should exist");

        assert_eq!(
            leading_spaces(lines[when_idx]),
            leading_spaces(lines[else_idx]),
            "ELSE should align with WHEN in UPDATE SET CASE, got:\n{}",
            formatted
        );
        assert!(
            leading_spaces(lines[when_idx]) > leading_spaces(lines[end_idx]),
            "WHEN should indent deeper than END in UPDATE SET CASE, got:\n{}",
            formatted
        );
    }

    #[test]
    fn line_has_trailing_unclosed_case_detects_mid_line_case() {
        assert!(SqlEditorWidget::line_has_trailing_unclosed_case(
            "SELECT col1, CASE"
        ));
        assert!(SqlEditorWidget::line_has_trailing_unclosed_case(
            "SET col1 = CASE"
        ));
        assert!(SqlEditorWidget::line_has_trailing_unclosed_case(
            "NVL(col, CASE"
        ));
    }

    #[test]
    fn line_has_trailing_unclosed_case_ignores_closed_case() {
        assert!(!SqlEditorWidget::line_has_trailing_unclosed_case(
            "CASE WHEN a = 1 THEN 'x' ELSE 'y' END"
        ));
        assert!(!SqlEditorWidget::line_has_trailing_unclosed_case(
            "SELECT CASE WHEN a THEN b END AS c"
        ));
    }

    #[test]
    fn line_has_trailing_unclosed_case_returns_false_for_no_case() {
        assert!(!SqlEditorWidget::line_has_trailing_unclosed_case(
            "SELECT col1, col2"
        ));
        assert!(!SqlEditorWidget::line_has_trailing_unclosed_case(
            "WHERE col1 = 1"
        ));
    }

    #[test]
    fn nested_paren_and_or_drops_depth_after_close_paren() {
        let input = r#"SELECT *
FROM emp
WHERE (
    (
        col1 = 1
        AND col2 = 2
    )
    OR (
        col3 = 3
        AND col4 = 4
    )
)
AND (
    (
        col5 = 5
        OR col6 = 6
    )
    AND col7 = 7
);"#;
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let and_outer = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND ((col5"))
            .expect("should contain outer AND ((col5");
        let where_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("WHERE"))
            .expect("should contain WHERE");
        let condition_base = indent(lines[where_idx]) + 4;

        assert_eq!(
            indent(lines[and_outer]),
            condition_base,
            "top-level AND after closed paren group should return to condition base depth, got:\n{}",
            formatted
        );

        let reformatted = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            reformatted, formatted,
            "formatting should be stable for deeply nested parenthesized conditions"
        );
    }

    #[test]
    fn mixed_nested_paren_and_or_keeps_correct_depths() {
        let input = r#"SELECT *
FROM emp
WHERE status = 'A'
AND (
    dept_id = 10
    OR (
        dept_id = 20
        AND role = 'MGR'
    )
    OR dept_id = 30
)
AND active = 1;"#;
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let and_open = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND (dept_id = 10"))
            .expect("should contain AND (dept_id = 10");
        let or_inner = lines
            .iter()
            .position(|line| line.trim_start().starts_with("OR (dept_id = 20"))
            .expect("should contain OR (dept_id = 20");
        let or_flat = lines
            .iter()
            .position(|line| line.trim_start().starts_with("OR dept_id = 30"))
            .expect("should contain OR dept_id = 30");
        let and_final = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND active"))
            .expect("should contain AND active");

        assert_eq!(
            indent(lines[or_inner]),
            indent(lines[or_flat]),
            "OR inside same paren group should align, got:\n{}",
            formatted
        );
        assert!(
            indent(lines[or_inner]) > indent(lines[and_open]),
            "OR inside AND's paren should be deeper than AND, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[and_final]),
            indent(lines[and_open]),
            "AND after closed paren should return to same depth as opening AND, got:\n{}",
            formatted
        );

        let reformatted = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            reformatted, formatted,
            "formatting should be stable for mixed nested conditions"
        );
    }

    #[test]
    fn triple_nested_paren_conditions_keep_progressive_depth() {
        let input = r#"SELECT *
FROM emp
WHERE (
    a = 1
    AND (
        b = 2
        OR (
            c = 3
            AND d = 4
        )
    )
);"#;
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let and_inner = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND (b"))
            .or_else(|| {
                lines
                    .iter()
                    .position(|line| line.trim_start().starts_with("AND ("))
            })
            .expect("should contain AND (b or AND (");
        let or_deepest = lines
            .iter()
            .position(|line| {
                line.trim_start().starts_with("OR (c") || line.trim_start().starts_with("OR (")
            })
            .expect("should contain OR (c or OR (");

        assert!(
            indent(lines[or_deepest]) > indent(lines[and_inner]),
            "deeper nested OR should have greater indent than AND, got:\n{}",
            formatted
        );

        let reformatted = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            reformatted, formatted,
            "formatting should be stable for triple nested conditions"
        );
    }

    #[test]
    fn join_on_nested_paren_and_or_keeps_correct_depths() {
        let input = r#"SELECT *
FROM emp e
JOIN dept d
ON (e.dept_id = d.dept_id
    AND e.loc_id = d.loc_id)
OR (e.alt_dept_id = d.dept_id
    AND e.alt_loc_id = d.loc_id)
JOIN region r
ON r.region_id = d.region_id;"#;
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let on_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("ON (e.dept_id"))
            .expect("should contain ON (e.dept_id");
        let or_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("OR (e.alt_dept_id"))
            .expect("should contain OR (e.alt_dept_id");
        let join_region = lines
            .iter()
            .position(|line| line.trim_start().starts_with("JOIN region"))
            .expect("should contain JOIN region");

        assert_eq!(
            indent(lines[or_idx]),
            indent(lines[on_idx]),
            "OR after closed paren group in JOIN ON should return to ON depth, got:\n{}",
            formatted
        );
        assert!(
            indent(lines[join_region]) < indent(lines[on_idx]),
            "next JOIN should be at base depth, less than ON, got:\n{}",
            formatted
        );

        let reformatted = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            reformatted, formatted,
            "formatting should be stable for JOIN ON with nested paren conditions"
        );
    }

    #[test]
    fn having_nested_paren_and_or_keeps_correct_depths() {
        let input = r#"SELECT dept_id, COUNT(*)
FROM emp
GROUP BY dept_id
HAVING (COUNT(*) > 5
    AND SUM(salary) > 100000)
OR (COUNT(*) > 10
    AND AVG(salary) > 50000);"#;
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let having_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("HAVING"))
            .expect("should contain HAVING");
        let and_first = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND SUM"))
            .expect("should contain AND SUM");
        let or_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("OR (COUNT"))
            .expect("should contain OR (COUNT");

        assert!(
            indent(lines[and_first]) > indent(lines[having_idx]),
            "AND inside HAVING paren should be deeper than HAVING, got:\n{}",
            formatted
        );
        assert!(
            indent(lines[and_first]) > indent(lines[or_idx]),
            "AND inside paren should be deeper than OR outside paren, got:\n{}",
            formatted
        );
        assert!(
            indent(lines[or_idx]) > indent(lines[having_idx]),
            "OR at condition level should be deeper than HAVING, got:\n{}",
            formatted
        );

        let reformatted = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            reformatted, formatted,
            "formatting should be stable for HAVING with nested paren conditions"
        );
    }

    #[test]
    fn case_when_nested_paren_and_or_keeps_correct_depths() {
        let input = r#"SELECT
    CASE
        WHEN (status = 'A'
            AND (dept_id = 10
                OR dept_id = 20))
            OR priority = 'HIGH' THEN 'YES'
        ELSE 'NO'
    END AS result
FROM emp;"#;
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let when_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("WHEN (status"))
            .expect("should contain WHEN (status");
        let and_nested = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND (dept_id = 10"))
            .expect("should contain AND (dept_id = 10");
        let or_outer = lines
            .iter()
            .position(|line| line.trim_start().starts_with("OR priority"))
            .expect("should contain OR priority");

        assert!(
            indent(lines[and_nested]) > indent(lines[when_idx]),
            "AND inside WHEN paren should be deeper than WHEN, got:\n{}",
            formatted
        );
        assert!(
            indent(lines[or_outer]) <= indent(lines[and_nested]),
            "OR after closed nested parens should be at or shallower than AND inside paren, got:\n{}",
            formatted
        );

        let reformatted = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            reformatted, formatted,
            "formatting should be stable for CASE WHEN with nested paren conditions"
        );
    }

    #[test]
    fn exists_subquery_nested_paren_and_or_keeps_correct_depths() {
        let input = r#"SELECT *
FROM dept d
WHERE EXISTS (
    SELECT 1
    FROM emp e
    WHERE e.dept_id = d.dept_id
    AND (e.status = 'A'
        OR (e.status = 'B'
            AND e.hire_date > DATE '2020-01-01'))
)
AND d.active = 1;"#;
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let outer_where = lines
            .iter()
            .position(|line| line.trim_start().starts_with("WHERE EXISTS"))
            .expect("should contain WHERE EXISTS");
        let and_outer = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND d.active"))
            .expect("should contain AND d.active");
        let outer_condition_base = indent(lines[outer_where]) + 4;

        assert_eq!(
            indent(lines[and_outer]),
            outer_condition_base,
            "AND after EXISTS subquery should return to outer condition depth, got:\n{}",
            formatted
        );

        let inner_and = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND (e.status"))
            .expect("should contain AND (e.status");
        let inner_or = lines
            .iter()
            .position(|line| line.trim_start().starts_with("OR (e.status"))
            .expect("should contain OR (e.status");

        assert!(
            indent(lines[inner_or]) > indent(lines[inner_and]),
            "OR inside AND's paren should be deeper than AND in subquery, got:\n{}",
            formatted
        );

        let reformatted = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            reformatted, formatted,
            "formatting should be stable for EXISTS subquery with nested paren conditions"
        );
    }

    #[test]
    fn merge_on_nested_paren_and_or_keeps_correct_depths() {
        let input = r#"MERGE INTO target t
USING source s
ON (t.id = s.id
    AND (t.type = s.type
        OR t.alt_type = s.type))
WHEN MATCHED THEN
    UPDATE SET t.val = s.val
WHEN NOT MATCHED THEN
    INSERT (id, val) VALUES (s.id, s.val);"#;
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let on_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("ON (t.id"))
            .expect("should contain ON (t.id");
        let and_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND (t.TYPE"))
            .or_else(|| {
                lines
                    .iter()
                    .position(|line| line.trim_start().starts_with("AND (t.type"))
            })
            .expect("should contain AND (t.type");
        let or_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("OR t.alt_type"))
            .expect("should contain OR t.alt_type");
        let when_matched = lines
            .iter()
            .position(|line| line.trim_start().starts_with("WHEN MATCHED"))
            .expect("should contain WHEN MATCHED");

        assert!(
            indent(lines[and_idx]) > indent(lines[on_idx]),
            "AND inside MERGE ON paren should be deeper than ON, got:\n{}",
            formatted
        );
        assert!(
            indent(lines[or_idx]) > indent(lines[and_idx]),
            "OR inside nested paren should be deeper than AND, got:\n{}",
            formatted
        );
        assert!(
            indent(lines[when_matched]) <= indent(lines[on_idx]),
            "WHEN MATCHED should be at base depth after ON clause, got:\n{}",
            formatted
        );

        let reformatted = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            reformatted, formatted,
            "formatting should be stable for MERGE ON with nested paren conditions"
        );
    }

    #[test]
    fn plsql_if_nested_paren_and_or_keeps_correct_depths() {
        let input = r#"BEGIN
    IF (v_status = 'A'
        AND (v_dept = 10
            OR v_dept = 20))
        OR v_override = 'Y' THEN
        NULL;
    END IF;
END;"#;
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let if_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("IF (v_status"))
            .expect("should contain IF (v_status");
        let and_nested = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND (v_dept = 10"))
            .expect("should contain AND (v_dept = 10");
        let or_outer = lines
            .iter()
            .position(|line| line.trim_start().starts_with("OR v_override"))
            .expect("should contain OR v_override");
        let end_if = lines
            .iter()
            .position(|line| line.trim_start() == "END IF;")
            .expect("should contain END IF");

        assert!(
            indent(lines[and_nested]) > indent(lines[if_idx]),
            "AND inside IF paren should be deeper than IF, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[or_outer]),
            indent(lines[and_nested]),
            "OR after closed nested parens should be at condition continuation depth, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[end_if]),
            indent(lines[if_idx]),
            "END IF should align with IF, got:\n{}",
            formatted
        );

        let reformatted = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            reformatted, formatted,
            "formatting should be stable for PL/SQL IF with nested paren conditions"
        );
    }

    #[test]
    fn where_in_subquery_nested_paren_and_or_keeps_correct_depths() {
        let input = r#"SELECT *
FROM emp
WHERE dept_id IN (
    SELECT dept_id
    FROM dept
    WHERE (region = 'EAST'
        AND active = 1)
    OR (region = 'WEST'
        AND priority = 'HIGH')
)
AND status = 'A';"#;
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let outer_where = lines
            .iter()
            .position(|line| line.trim_start().starts_with("WHERE dept_id IN"))
            .expect("should contain WHERE dept_id IN");
        let and_final = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND status"))
            .expect("should contain AND status");
        let outer_condition_base = indent(lines[outer_where]) + 4;

        assert_eq!(
            indent(lines[and_final]),
            outer_condition_base,
            "AND after IN subquery should return to outer condition depth, got:\n{}",
            formatted
        );

        let inner_or = lines
            .iter()
            .position(|line| {
                line.trim_start().starts_with("OR (region = 'WEST'")
                    || line.trim_start().starts_with("OR (region")
            })
            .expect("should contain OR in inner query");
        let inner_and_first = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND active"))
            .expect("should contain AND active");

        assert!(
            indent(lines[inner_and_first]) > indent(lines[inner_or]),
            "AND inside paren should be deeper than OR outside paren in subquery, got:\n{}",
            formatted
        );

        let reformatted = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            reformatted, formatted,
            "formatting should be stable for WHERE IN subquery with nested paren conditions"
        );
    }

    #[test]
    fn comments_inside_nested_paren_and_or_preserve_depth() {
        let input = r#"SELECT *
FROM emp
WHERE (
    dept_id = 10
    -- check status
    AND status = 'A'
)
OR (
    dept_id = 20
    /* alt condition */
    AND status = 'B'
);"#;
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let and_first = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND status = 'A'"))
            .expect("should contain AND status = 'A'");
        let or_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("OR (dept_id = 20"))
            .expect("should contain OR (dept_id = 20");
        let comment_line = lines
            .iter()
            .position(|line| line.trim_start().starts_with("-- check status"))
            .expect("should contain -- check status");

        assert_eq!(
            indent(lines[comment_line]),
            indent(lines[and_first]),
            "line comment should align with following AND inside paren, got:\n{}",
            formatted
        );
        assert!(
            indent(lines[and_first]) > indent(lines[or_idx]),
            "AND inside paren should be deeper than OR outside paren, got:\n{}",
            formatted
        );

        let reformatted = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            reformatted, formatted,
            "formatting should be stable for comments inside nested paren conditions"
        );
    }

    #[test]
    fn double_consecutive_open_parens_get_progressive_depth() {
        let input = "SELECT * FROM emp WHERE ((col1 = 1 AND col2 = 2) AND col3 = 3);";
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let and_inner = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND col2"))
            .expect("should contain AND col2");
        let and_outer = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND col3"))
            .expect("should contain AND col3");

        assert_eq!(
            indent(lines[and_inner]),
            12,
            "AND inside (( should be at depth 3 (12 spaces): WHERE base(1) + 2 parens, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[and_outer]),
            8,
            "AND inside ( should be at depth 2 (8 spaces): WHERE base(1) + 1 paren, got:\n{}",
            formatted
        );

        let reformatted = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            reformatted, formatted,
            "formatting should be stable for (( double parens"
        );
    }

    #[test]
    fn triple_consecutive_open_parens_get_progressive_depth() {
        let input =
            "SELECT * FROM emp WHERE (((col1 = 1 AND col2 = 2) AND col3 = 3) AND col4 = 4);";
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let and_innermost = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND col2"))
            .expect("should contain AND col2");
        let and_middle = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND col3"))
            .expect("should contain AND col3");
        let and_outer = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND col4"))
            .expect("should contain AND col4");

        assert_eq!(
            indent(lines[and_innermost]),
            16,
            "AND inside ((( should be at depth 4 (16 spaces): WHERE base(1) + 3 parens, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[and_middle]),
            12,
            "AND inside (( should be at depth 3 (12 spaces): WHERE base(1) + 2 parens, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[and_outer]),
            8,
            "AND inside ( should be at depth 2 (8 spaces): WHERE base(1) + 1 paren, got:\n{}",
            formatted
        );

        let reformatted = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            reformatted, formatted,
            "formatting should be stable for ((( triple parens"
        );
    }

    #[test]
    fn triple_consecutive_parens_with_or_get_progressive_depth() {
        let input = r#"SELECT *
FROM emp
WHERE (((status = 'A' OR status = 'B')
    AND dept_id = 10)
    OR region = 'WEST');"#;
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let or_inner = lines
            .iter()
            .position(|line| line.trim_start().starts_with("OR status = 'B'"))
            .expect("should contain OR status = 'B'");
        let and_middle = lines
            .iter()
            .position(|line| line.trim_start().starts_with("AND dept_id"))
            .expect("should contain AND dept_id");
        let or_outer = lines
            .iter()
            .position(|line| line.trim_start().starts_with("OR region"))
            .expect("should contain OR region");

        assert_eq!(
            indent(lines[or_inner]),
            16,
            "OR inside ((( should be at depth 4 (16 spaces): WHERE base(1) + 3 parens, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[and_middle]),
            12,
            "AND inside (( should be at depth 3 (12 spaces): WHERE base(1) + 2 parens, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[or_outer]),
            8,
            "OR inside ( should be at depth 2 (8 spaces): WHERE base(1) + 1 paren, got:\n{}",
            formatted
        );

        let reformatted = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            reformatted, formatted,
            "formatting should be stable for ((( with OR at various depths"
        );
    }

    #[test]
    fn func_call_parens_do_not_get_progressive_depth() {
        // func(func2( should NOT get progressive depth — only consecutive ((( should
        let input = "SELECT TO_CHAR(NVL(col1, 0), 'FM9999') FROM emp;";
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let reformatted = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            reformatted, formatted,
            "function call nesting should be stable:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_open_for_select_after_block_comment_in_package_body() {
        let sql = "create package body a as\nprocedure b (c in number) as\nbegin\n/* */\nopen cv for\nselect 1\nfrom dual;\nend b;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(sql);
        assert!(
            formatted.contains("OPEN cv FOR\n            SELECT 1\n            FROM DUAL;"),
            "SELECT after OPEN FOR should be indented exactly 1 level deeper than OPEN, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_basic_open_for_select_after_block_comment_in_begin() {
        let sql = "begin\n/* */\nopen cv for\nselect 1\nfrom dual;\nend;";
        let formatted = SqlEditorWidget::format_sql_basic(sql);
        assert!(
            formatted.contains("OPEN cv FOR\n        SELECT 1\n        FROM DUAL;"),
            "SELECT after OPEN FOR should be indented exactly 1 level deeper than OPEN, got:\n{}",
            formatted
        );
    }

    // ---- JOIN ON indentation: AND/OR after ON should be one level deeper ----

    #[test]
    fn join_on_and_indented_deeper_than_on() {
        let input = "select * from a join b on 1 = 1 and 2 = 2;";
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let on_idx = lines
            .iter()
            .position(|l| l.trim_start().starts_with("ON "))
            .expect("ON line");
        let and_idx = lines
            .iter()
            .position(|l| l.trim_start().starts_with("AND "))
            .expect("AND line");

        assert_eq!(
            indent(lines[and_idx]),
            indent(lines[on_idx]) + 4,
            "AND after ON should be indented one level deeper than ON, got:\n{}",
            formatted
        );
    }

    #[test]
    fn join_on_multiple_and_or_indented_deeper_than_on() {
        let input = "select * from a join b on a.id = b.id and a.x = b.x or a.y = b.y;";
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let on_idx = lines
            .iter()
            .position(|l| l.trim_start().starts_with("ON "))
            .expect("ON line");
        let and_idx = lines
            .iter()
            .position(|l| l.trim_start().starts_with("AND "))
            .expect("AND line");
        let or_idx = lines
            .iter()
            .position(|l| l.trim_start().starts_with("OR "))
            .expect("OR line");

        let on_indent = indent(lines[on_idx]);
        assert_eq!(
            indent(lines[and_idx]),
            on_indent + 4,
            "AND after ON should be one level deeper, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[or_idx]),
            indent(lines[and_idx]),
            "OR should be at same depth as AND, got:\n{}",
            formatted
        );
    }

    #[test]
    fn join_on_condition_keywords_without_space_are_aligned_as_conditions() {
        let input = "select * from a join b on a.id = b.id and(a.flag = 'Y') or(b.flag = 'N');";
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let on_idx = lines
            .iter()
            .position(|l| l.trim_start().starts_with("ON "))
            .expect("ON line");
        let and_idx = lines
            .iter()
            .position(|l| {
                crate::sql_text::starts_with_keyword_token(
                    &l.trim_start().to_ascii_uppercase(),
                    "AND",
                )
            })
            .expect("AND line");
        let or_idx = lines
            .iter()
            .position(|l| {
                crate::sql_text::starts_with_keyword_token(
                    &l.trim_start().to_ascii_uppercase(),
                    "OR",
                )
            })
            .expect("OR line");

        assert_eq!(
            indent(lines[and_idx]),
            indent(lines[on_idx]) + 4,
            "AND(...) should be treated as JOIN condition continuation, got:\n{}",
            formatted
        );
        assert_eq!(
            indent(lines[or_idx]),
            indent(lines[and_idx]),
            "OR(...) should align with sibling condition keyword, got:\n{}",
            formatted
        );
    }

    #[test]
    fn join_on_and_deeper_with_multiple_joins() {
        let input = r#"select *
from a
join b on a.id = b.id and a.x = b.x
join c on b.id = c.id and b.y = c.y;"#;
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let on_lines: Vec<usize> = lines
            .iter()
            .enumerate()
            .filter(|(_, l)| l.trim_start().starts_with("ON "))
            .map(|(i, _)| i)
            .collect();
        let and_lines: Vec<usize> = lines
            .iter()
            .enumerate()
            .filter(|(_, l)| l.trim_start().starts_with("AND "))
            .map(|(i, _)| i)
            .collect();

        assert_eq!(
            on_lines.len(),
            2,
            "should have two ON lines, got:\n{}",
            formatted
        );
        assert_eq!(
            and_lines.len(),
            2,
            "should have two AND lines, got:\n{}",
            formatted
        );

        for (on_idx, and_idx) in on_lines.iter().zip(and_lines.iter()) {
            assert_eq!(
                indent(lines[*and_idx]),
                indent(lines[*on_idx]) + 4,
                "AND after ON should be one level deeper for each join, got:\n{}",
                formatted
            );
        }
    }

    #[test]
    fn join_on_and_deeper_in_subquery() {
        let input = r#"select * from (
select * from a
join b on a.id = b.id and a.x = b.x
) sub;"#;
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let on_idx = lines
            .iter()
            .position(|l| l.trim_start().starts_with("ON "))
            .expect("ON line");
        let and_idx = lines
            .iter()
            .position(|l| l.trim_start().starts_with("AND "))
            .expect("AND line");

        assert_eq!(
            indent(lines[and_idx]),
            indent(lines[on_idx]) + 4,
            "AND after ON should be one level deeper even in subquery, got:\n{}",
            formatted
        );
    }

    #[test]
    fn join_on_and_deeper_is_idempotent() {
        let input = "select * from a join b on 1 = 1 and 2 = 2;";
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let reformatted = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            reformatted, formatted,
            "formatting should be idempotent for JOIN ON with AND"
        );
    }

    #[test]
    fn left_join_on_and_deeper_than_on() {
        let input = "select * from a left join b on a.id = b.id and a.x = b.x;";
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let on_idx = lines
            .iter()
            .position(|l| l.trim_start().starts_with("ON "))
            .expect("ON line");
        let and_idx = lines
            .iter()
            .position(|l| l.trim_start().starts_with("AND "))
            .expect("AND line");

        assert_eq!(
            indent(lines[and_idx]),
            indent(lines[on_idx]) + 4,
            "AND after ON in LEFT JOIN should be one level deeper, got:\n{}",
            formatted
        );
    }

    #[test]
    fn full_outer_join_on_and_deeper_than_on() {
        let input = "select * from a full outer join b on a.id = b.id and a.x = b.x;";
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let on_idx = lines
            .iter()
            .position(|l| l.trim_start().starts_with("ON "))
            .expect("ON line");
        let and_idx = lines
            .iter()
            .position(|l| l.trim_start().starts_with("AND "))
            .expect("AND line");

        assert_eq!(
            indent(lines[and_idx]),
            indent(lines[on_idx]) + 4,
            "AND after ON in FULL OUTER JOIN should be one level deeper, got:\n{}",
            formatted
        );
    }

    #[test]
    fn join_on_with_where_and_both_correctly_indented() {
        let input =
            "select * from a join b on a.id = b.id and a.x = b.x where a.y = 1 and a.z = 2;";
        let formatted = SqlEditorWidget::format_sql_basic(input);
        let lines: Vec<&str> = formatted.lines().collect();
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        let on_idx = lines
            .iter()
            .position(|l| l.trim_start().starts_with("ON "))
            .expect("ON line");
        let where_idx = lines
            .iter()
            .position(|l| l.trim_start().starts_with("WHERE "))
            .expect("WHERE line");
        let and_lines: Vec<usize> = lines
            .iter()
            .enumerate()
            .filter(|(_, l)| l.trim_start().starts_with("AND "))
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            and_lines.len(),
            2,
            "should have two AND lines, got:\n{}",
            formatted
        );

        // First AND after ON
        assert_eq!(
            indent(lines[and_lines[0]]),
            indent(lines[on_idx]) + 4,
            "AND after ON should be deeper than ON, got:\n{}",
            formatted
        );
        // Second AND after WHERE - indented as continuation
        assert!(
            indent(lines[and_lines[1]]) >= indent(lines[where_idx]),
            "AND after WHERE should be at or deeper than WHERE depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn join_on_and_edge_case_survey() {
        let indent = |line: &str| line.len().saturating_sub(line.trim_start().len());

        // Helper: format + check idempotent + return formatted
        let format_check = |label: &str, input: &str| -> String {
            let formatted = SqlEditorWidget::format_sql_basic(input);
            let reformatted = SqlEditorWidget::format_sql_basic(&formatted);
            assert_eq!(
                reformatted, formatted,
                "[{}] formatting should be idempotent.\nInput:\n{}\nFormatted:\n{}\nReformatted:\n{}",
                label, input, formatted, reformatted
            );
            formatted
        };

        // 1. ON with OR then AND (operator precedence edge case)
        let formatted = format_check(
            "OR-then-AND",
            "select * from a join b on a.id = b.id or a.x = b.x and a.y = b.y;",
        );
        {
            let lines: Vec<&str> = formatted.lines().collect();
            let on_idx = lines
                .iter()
                .position(|l| l.trim_start().starts_with("ON "))
                .unwrap();
            let or_idx = lines
                .iter()
                .position(|l| l.trim_start().starts_with("OR "))
                .unwrap();
            let and_idx = lines
                .iter()
                .position(|l| l.trim_start().starts_with("AND "))
                .unwrap();
            let on_ind = indent(lines[on_idx]);
            assert_eq!(
                indent(lines[or_idx]),
                on_ind + 4,
                "[OR-then-AND] OR should be deeper than ON:\n{}",
                formatted
            );
            assert_eq!(
                indent(lines[and_idx]),
                indent(lines[or_idx]),
                "[OR-then-AND] AND should be same as OR:\n{}",
                formatted
            );
        }

        // 2. Many ANDs (3+)
        let formatted = format_check(
            "Many-ANDs",
            "select * from a join b on a.id = b.id and a.x = b.x and a.y = b.y and a.z = b.z;",
        );
        {
            let lines: Vec<&str> = formatted.lines().collect();
            let on_idx = lines
                .iter()
                .position(|l| l.trim_start().starts_with("ON "))
                .unwrap();
            let and_lines: Vec<usize> = lines
                .iter()
                .enumerate()
                .filter(|(_, l)| l.trim_start().starts_with("AND "))
                .map(|(i, _)| i)
                .collect();
            assert_eq!(
                and_lines.len(),
                3,
                "should have 3 AND lines:\n{}",
                formatted
            );
            for &ai in &and_lines {
                assert_eq!(
                    indent(lines[ai]),
                    indent(lines[on_idx]) + 4,
                    "all ANDs should be at same depth (ON + 4):\n{}",
                    formatted
                );
            }
        }

        // 3. CROSS JOIN (no ON) then regular JOIN ON AND
        let formatted = format_check(
            "CROSS-then-JOIN",
            "select * from a cross join b join c on a.id = c.id and b.id = c.bid;",
        );
        {
            let lines: Vec<&str> = formatted.lines().collect();
            let on_idx = lines
                .iter()
                .position(|l| l.trim_start().starts_with("ON "))
                .unwrap();
            let and_idx = lines
                .iter()
                .position(|l| l.trim_start().starts_with("AND "))
                .unwrap();
            assert_eq!(
                indent(lines[and_idx]),
                indent(lines[on_idx]) + 4,
                "AND after ON should be deeper even after CROSS JOIN:\n{}",
                formatted
            );
        }

        // 4. LEFT OUTER JOIN
        let formatted = format_check(
            "LEFT-OUTER",
            "select * from a left outer join b on a.id = b.id and a.x = b.x;",
        );
        {
            let lines: Vec<&str> = formatted.lines().collect();
            let on_idx = lines
                .iter()
                .position(|l| l.trim_start().starts_with("ON "))
                .unwrap();
            let and_idx = lines
                .iter()
                .position(|l| l.trim_start().starts_with("AND "))
                .unwrap();
            assert_eq!(
                indent(lines[and_idx]),
                indent(lines[on_idx]) + 4,
                "AND after ON in LEFT OUTER JOIN:\n{}",
                formatted
            );
        }

        // 5. USING clause then JOIN ON AND
        let formatted = format_check(
            "USING-then-JOIN-ON",
            "select * from a join b using (id) join c on b.name = c.name and b.x = c.x;",
        );
        {
            let lines: Vec<&str> = formatted.lines().collect();
            let on_idx = lines
                .iter()
                .position(|l| l.trim_start().starts_with("ON "))
                .unwrap();
            let and_idx = lines
                .iter()
                .position(|l| l.trim_start().starts_with("AND "))
                .unwrap();
            assert_eq!(
                indent(lines[and_idx]),
                indent(lines[on_idx]) + 4,
                "AND after ON when previous JOIN used USING:\n{}",
                formatted
            );
        }

        // 6. Function calls in ON condition
        let formatted = format_check("Function-in-ON",
            "select * from a join b on upper(a.name) = upper(b.name) and nvl(a.id, 0) = nvl(b.id, 0);");
        {
            let lines: Vec<&str> = formatted.lines().collect();
            let on_idx = lines
                .iter()
                .position(|l| l.trim_start().starts_with("ON "))
                .unwrap();
            let and_idx = lines
                .iter()
                .position(|l| l.trim_start().starts_with("AND "))
                .unwrap();
            assert_eq!(
                indent(lines[and_idx]),
                indent(lines[on_idx]) + 4,
                "AND after ON with function calls:\n{}",
                formatted
            );
        }

        // 7. CTE with JOIN ON AND
        let formatted = format_check("CTE-with-JOIN",
            "with cte as (\nselect * from a join b on a.id = b.id and a.x = b.x\n)\nselect * from cte;");
        {
            let lines: Vec<&str> = formatted.lines().collect();
            let on_idx = lines
                .iter()
                .position(|l| l.trim_start().starts_with("ON "))
                .unwrap();
            let and_idx = lines
                .iter()
                .position(|l| l.trim_start().starts_with("AND "))
                .unwrap();
            assert_eq!(
                indent(lines[and_idx]),
                indent(lines[on_idx]) + 4,
                "AND after ON inside CTE:\n{}",
                formatted
            );
        }

        // 8. Already-formatted input (stability)
        let _formatted = format_check("Already-formatted",
            "SELECT *\nFROM a\nJOIN b\n    ON a.id = b.id\n        AND a.x = b.x\n        OR a.y = b.y\nJOIN c\n    ON b.id = c.id\n        AND b.x = c.x;");

        // 9. Deep subquery nesting
        let formatted = format_check("Deep-subquery",
            "select * from (\nselect * from (\nselect * from a join b on a.id = b.id and a.x = b.x\n) inner_sub\n) outer_sub;");
        {
            let lines: Vec<&str> = formatted.lines().collect();
            let on_idx = lines
                .iter()
                .position(|l| l.trim_start().starts_with("ON "))
                .unwrap();
            let and_idx = lines
                .iter()
                .position(|l| l.trim_start().starts_with("AND "))
                .unwrap();
            assert_eq!(
                indent(lines[and_idx]),
                indent(lines[on_idx]) + 4,
                "AND after ON in deeply nested subquery:\n{}",
                formatted
            );
        }

        // 10. JOIN ON with subquery in the condition
        let formatted = format_check("Subquery-in-ON",
            "select * from a join b on a.id = b.id and a.type in (select type from types) and a.x = b.x;");
        {
            let lines: Vec<&str> = formatted.lines().collect();
            let on_idx = lines
                .iter()
                .position(|l| l.trim_start().starts_with("ON "))
                .unwrap();
            let and_lines: Vec<usize> = lines
                .iter()
                .enumerate()
                .filter(|(_, l)| l.trim_start().starts_with("AND "))
                .map(|(i, _)| i)
                .collect();
            assert!(
                and_lines.len() >= 2,
                "should have at least 2 AND lines:\n{}",
                formatted
            );
            for &ai in &and_lines {
                assert_eq!(
                    indent(lines[ai]),
                    indent(lines[on_idx]) + 4,
                    "AND after ON with subquery in condition:\n{}",
                    formatted
                );
            }
        }

        // 11. WHERE with subquery then AND (should NOT be affected by join fix)
        let formatted = format_check(
            "WHERE-subquery-AND",
            "select * from a where a.id in (select id from b) and a.x = 1;",
        );
        {
            let lines: Vec<&str> = formatted.lines().collect();
            let where_idx = lines
                .iter()
                .position(|l| l.trim_start().starts_with("WHERE "))
                .unwrap();
            let and_idx = lines
                .iter()
                .position(|l| l.trim_start().starts_with("AND "))
                .unwrap();
            // WHERE AND should be at or deeper than WHERE level
            assert!(
                indent(lines[and_idx]) >= indent(lines[where_idx]),
                "AND after WHERE with subquery should be at or deeper than WHERE:\n{}",
                formatted
            );
        }

        // 12. JOIN ON with EXISTS subquery then AND
        let formatted = format_check("EXISTS-in-ON",
            "select * from a join b on a.id = b.id and exists (select 1 from c where c.id = a.id) and a.x = b.x;");
        {
            let lines: Vec<&str> = formatted.lines().collect();
            let on_idx = lines
                .iter()
                .position(|l| l.trim_start().starts_with("ON "))
                .unwrap();
            let and_lines: Vec<usize> = lines
                .iter()
                .enumerate()
                .filter(|(_, l)| l.trim_start().starts_with("AND "))
                .map(|(i, _)| i)
                .collect();
            assert!(
                and_lines.len() >= 2,
                "should have at least 2 AND lines:\n{}",
                formatted
            );
            for &ai in &and_lines {
                assert_eq!(
                    indent(lines[ai]),
                    indent(lines[on_idx]) + 4,
                    "AND after ON with EXISTS subquery:\n{}",
                    formatted
                );
            }
        }

        // 13. JOIN ON subquery then WHERE (state should be cleared for WHERE)
        let formatted = format_check("ON-subquery-then-WHERE",
            "select * from a join b on a.id = b.id and a.type in (select type from types) where a.x = 1 and a.y = 2;");
        {
            let lines: Vec<&str> = formatted.lines().collect();
            let where_idx = lines
                .iter()
                .position(|l| l.trim_start().starts_with("WHERE "))
                .unwrap();
            let where_and_idx = lines
                .iter()
                .enumerate()
                .filter(|(i, l)| *i > where_idx && l.trim_start().starts_with("AND "))
                .map(|(i, _)| i)
                .next()
                .expect("should have AND after WHERE");
            // WHERE AND should NOT be at join ON AND depth
            assert!(
                indent(lines[where_and_idx]) <= indent(lines[where_idx]) + 4,
                "AND after WHERE should be at WHERE continuation depth, not JOIN ON depth:\n{}",
                formatted
            );
        }
    }

    // ── NATURAL LEFT/RIGHT JOIN stays on one line ──

    #[test]
    fn format_sql_natural_left_join_stays_together() {
        let source = "SELECT * FROM emp e NATURAL LEFT JOIN dept d JOIN bonus b USING (deptno);";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("NATURAL LEFT JOIN"),
            "NATURAL LEFT JOIN should stay on one line, got:\n{}",
            formatted
        );
        // Idempotent
        let formatted2 = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(formatted, formatted2, "NATURAL LEFT JOIN formatting should be idempotent");
    }

    #[test]
    fn format_sql_natural_right_join_stays_together() {
        let source = "SELECT * FROM t1 NATURAL RIGHT JOIN t2;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("NATURAL RIGHT JOIN"),
            "NATURAL RIGHT JOIN should stay on one line, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_natural_full_join_stays_together() {
        let source = "SELECT * FROM t1 NATURAL FULL JOIN t2;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("NATURAL FULL JOIN"),
            "NATURAL FULL JOIN should stay on one line, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_natural_inner_join_stays_together() {
        let source = "SELECT * FROM t1 NATURAL INNER JOIN t2;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("NATURAL INNER JOIN"),
            "NATURAL INNER JOIN should stay on one line, got:\n{}",
            formatted
        );
    }

    // ── Trigger REFERENCING / FOR EACH ROW / WHEN ──

    #[test]
    fn format_sql_trigger_referencing_and_when_alignment() {
        let source = "CREATE OR REPLACE TRIGGER trg_emp_biu BEFORE INSERT OR UPDATE OF sal, comm ON emp REFERENCING NEW AS n OLD AS o FOR EACH ROW WHEN (n.sal > 0) BEGIN :n.comm := NVL(:n.comm, 0); END;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        // Comma in trigger header should NOT cause a line break
        assert!(
            formatted.contains("OF sal, comm ON emp"),
            "UPDATE OF column list commas should stay inline in trigger header, got:\n{}",
            formatted
        );
        // REFERENCING should start a new line at trigger header indent
        assert!(
            formatted.contains("\n    REFERENCING"),
            "REFERENCING should start a new line at trigger header indent, got:\n{}",
            formatted
        );
        // REFERENCING clause should stay on one line
        assert!(
            formatted.contains("REFERENCING NEW AS n OLD AS o"),
            "REFERENCING NEW AS n OLD AS o should stay on one line, got:\n{}",
            formatted
        );
        // FOR EACH ROW on its own line
        assert!(
            formatted.contains("\n    FOR EACH ROW"),
            "FOR EACH ROW should be on its own line at trigger header indent, got:\n{}",
            formatted
        );
        // WHEN on its own line at trigger header indent
        assert!(
            formatted.contains("\n    WHEN (n.sal > 0)"),
            "WHEN clause should be on its own line at trigger header indent, got:\n{}",
            formatted
        );
        // BEGIN at base indent
        assert!(
            formatted.contains("\nBEGIN"),
            "BEGIN should be at base indent after trigger WHEN, got:\n{}",
            formatted
        );
        // Idempotent
        let formatted2 = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(formatted, formatted2, "Trigger REFERENCING/WHEN formatting should be idempotent");
    }

    #[test]
    fn format_sql_trigger_update_of_multi_column_comma_stays_inline() {
        let source = "CREATE OR REPLACE TRIGGER trg_test AFTER UPDATE OF col1, col2, col3 ON my_table FOR EACH ROW BEGIN NULL; END;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("OF col1, col2, col3 ON my_table"),
            "Multiple columns in trigger UPDATE OF should stay inline, got:\n{}",
            formatted
        );
    }

    // ── MATCH_RECOGNIZE ONE ROW PER MATCH ──

    #[test]
    fn format_sql_match_recognize_one_row_per_match_on_own_line() {
        let source = "SELECT * FROM sales MATCH_RECOGNIZE (PARTITION BY customer_id ORDER BY sale_date MEASURES FIRST(A.sale_date) AS first_dt, LAST(B.sale_date) AS last_dt ONE ROW PER MATCH PATTERN (A B+) DEFINE A AS amount < 100, B AS amount >= 100);";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("\n    ONE ROW PER MATCH"),
            "ONE ROW PER MATCH should be on its own line inside MATCH_RECOGNIZE, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("\n    PATTERN"),
            "PATTERN should be on its own line inside MATCH_RECOGNIZE, got:\n{}",
            formatted
        );
        // Idempotent
        let formatted2 = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(formatted, formatted2, "MATCH_RECOGNIZE ONE ROW PER MATCH formatting should be idempotent");
    }

    #[test]
    fn format_sql_match_recognize_all_rows_per_match_on_own_line() {
        let source = "SELECT * FROM sales MATCH_RECOGNIZE (PARTITION BY cust_id ORDER BY sale_date MEASURES MATCH_NUMBER() AS mno ALL ROWS PER MATCH PATTERN (A B+) DEFINE A AS amount < 50, B AS amount >= 50);";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("\n    ALL ROWS PER MATCH"),
            "ALL ROWS PER MATCH should be on its own line inside MATCH_RECOGNIZE, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_analytic_over_clause_breaks_subclauses() {
        let source = "SELECT empno, SUM(sal) OVER (PARTITION BY deptno ORDER BY hiredate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sal FROM emp;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let expected = r#"SELECT empno,
    SUM (sal) OVER (
        PARTITION BY deptno
        ORDER BY hiredate
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_sal
FROM emp;"#;
        assert_eq!(formatted, expected);
        // Idempotent
        let formatted2 = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(formatted, formatted2, "OVER clause formatting should be idempotent");
    }

    #[test]
    fn format_sql_analytic_over_with_range_and_groups() {
        let source = "SELECT id, AVG(val) OVER (ORDER BY id RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW) AS avg7d, COUNT(*) OVER (PARTITION BY grp ORDER BY id GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS cnt3 FROM t;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("RANGE BETWEEN"),
            "RANGE BETWEEN should appear in formatted output, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("\n        RANGE BETWEEN"),
            "RANGE should be on its own indented line inside OVER, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("\n        GROUPS BETWEEN"),
            "GROUPS should be on its own indented line inside OVER, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_analytic_over_partition_only() {
        let source = "SELECT deptno, SUM(sal) OVER (PARTITION BY deptno) AS dept_total FROM emp;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("OVER (\n        PARTITION BY deptno\n    )"),
            "Single PARTITION BY in OVER should be on its own line, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_window_clause_and_qualify_keep_clause_depths_stable() {
        let source = "SELECT e.deptno, e.empno, SUM(e.sal) OVER w_dept AS dept_sum FROM emp e WINDOW w_dept AS (PARTITION BY e.deptno ORDER BY e.sal DESC, e.empno) QUALIFY ROW_NUMBER() OVER w_dept = 1;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();

        let window_idx =
            find_line_starting_with(&lines, "WINDOW w_dept AS (").expect("WINDOW clause line");
        let partition_idx =
            find_line_starting_with(&lines, "PARTITION BY e.deptno").expect("WINDOW PARTITION BY");
        let order_idx =
            find_line_starting_with(&lines, "ORDER BY e.sal DESC").expect("WINDOW ORDER BY");
        let qualify_idx =
            find_line_starting_with(&lines, "QUALIFY ROW_NUMBER").expect("QUALIFY clause line");
        let close_idx = lines
            .iter()
            .enumerate()
            .skip(order_idx.saturating_add(1))
            .find(|(idx, line)| *idx < qualify_idx && line.trim() == ")")
            .map(|(idx, _)| idx)
            .expect("WINDOW closing parenthesis");

        assert!(
            formatted.contains("\nWINDOW w_dept AS ("),
            "WINDOW clause should start on its own clause line, got:\n{}",
            formatted
        );
        assert!(
            leading_spaces(lines[partition_idx]) > leading_spaces(lines[window_idx]),
            "WINDOW PARTITION BY should be indented under WINDOW, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[partition_idx]),
            leading_spaces(lines[order_idx]),
            "WINDOW PARTITION BY / ORDER BY should share the same continuation depth, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[close_idx]),
            leading_spaces(lines[window_idx]),
            "WINDOW closing parenthesis should realign with WINDOW clause, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[qualify_idx]),
            leading_spaces(lines[window_idx]),
            "QUALIFY should align with top-level clause anchors, got:\n{}",
            formatted
        );

        let formatted_again = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            formatted, formatted_again,
            "WINDOW / QUALIFY formatting should be idempotent"
        );
    }

    // ── Bug regression: ORDER BY ... DESC split ──

    #[test]
    fn format_sql_order_by_desc_not_split_as_describe_command() {
        // DESC on its own line must NOT be misidentified as SQL*Plus DESCRIBE.
        let source = "SELECT empno, sal\nFROM emp\nORDER BY sal\nDESC;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("ORDER BY sal DESC"),
            "ORDER BY sal DESC should stay together, got:\n{}",
            formatted
        );
        // Idempotence
        let formatted_again = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            formatted, formatted_again,
            "Formatting ORDER BY DESC should be idempotent, got:\n{}",
            formatted_again
        );
    }

    #[test]
    fn format_sql_order_by_desc_with_slash_terminator() {
        // DESC on its own line with / terminator must not be split off.
        let source = "SELECT empno, sal\nFROM emp\nORDER BY sal\nDESC\n/";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("ORDER BY sal DESC"),
            "ORDER BY sal DESC with slash terminator should stay together, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_order_by_desc_nulls_last_multiline() {
        let source = "SELECT empno, sal\nFROM emp\nORDER BY sal\nDESC\nNULLS LAST;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("ORDER BY sal DESC NULLS LAST"),
            "ORDER BY sal DESC NULLS LAST should stay together, got:\n{}",
            formatted
        );
    }

    // ── Bug regression: MATCH_RECOGNIZE DEFINE with content ──

    #[test]
    fn format_sql_match_recognize_define_with_content_not_split() {
        let source = r#"SELECT *
FROM ticks
MATCH_RECOGNIZE (
    PARTITION BY symbol
    ORDER BY ts
    PATTERN (A B+)
    DEFINE B AS B.price > PREV(B.price)
);"#;
        let formatted = SqlEditorWidget::format_sql_basic(source);
        // DEFINE must be kept as SQL, not rewritten as SQL*Plus DEFINE assignment
        assert!(
            !formatted.contains("DEFINE B = "),
            "DEFINE should not be rewritten as SQL*Plus DEFINE assignment, got:\n{}",
            formatted
        );
        // The DEFINE line must remain in the statement (formatter may add spaces around parens)
        assert!(
            formatted.contains("DEFINE B AS B.price > PREV"),
            "DEFINE inside MATCH_RECOGNIZE should be kept as SQL, got:\n{}",
            formatted
        );
        let formatted_again = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            formatted, formatted_again,
            "MATCH_RECOGNIZE formatting should be idempotent, got:\n{}",
            formatted_again
        );
    }

    // ── Bug regression: FOR UPDATE ──

    #[test]
    fn format_sql_for_update_not_split() {
        let source = "SELECT * FROM emp FOR UPDATE OF sal SKIP LOCKED;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("FOR UPDATE OF sal SKIP LOCKED"),
            "FOR UPDATE should stay on one line, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("FOR\nUPDATE") && !formatted.contains("FOR\n    UPDATE"),
            "FOR and UPDATE should not be split across lines, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_for_update_multiline_input() {
        // Even when input has FOR and UPDATE on separate lines, output should join them.
        let source = "SELECT *\nFROM emp\nFOR\nUPDATE OF sal SKIP LOCKED;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("FOR UPDATE"),
            "FOR UPDATE should be on same line, got:\n{}",
            formatted
        );
    }

    // ── Bug regression: MERGE USING alignment ──

    #[test]
    fn format_sql_merge_using_gets_clause_break() {
        let source = "MERGE INTO tgt t USING src s ON (t.id = s.id) WHEN MATCHED THEN UPDATE SET t.val = s.val WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val);";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("\nUSING src s"),
            "USING should start on a new line like a clause keyword, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_join_using_gets_condition_break() {
        let source = "SELECT e.empno, d.dname FROM emp e JOIN dept d USING (deptno) WHERE e.empno > 0;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("\n    USING (deptno)") || formatted.contains("\nUSING (deptno)"),
            "JOIN USING should get alignment similar to ON, got:\n{}",
            formatted
        );
    }

    // ── Bug regression: CROSS APPLY / OUTER APPLY ──

    #[test]
    fn format_sql_cross_apply_gets_join_like_break() {
        let source = "SELECT e.empno, x.cnt FROM emp e CROSS APPLY (SELECT COUNT(*) AS cnt FROM bonus b WHERE b.empno = e.empno) x;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("\nCROSS APPLY ("),
            "CROSS APPLY should start on a new line like a JOIN, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_outer_apply_gets_join_like_break() {
        let source = "SELECT e.empno, x.cnt FROM emp e OUTER APPLY (SELECT COUNT(*) AS cnt FROM bonus b WHERE b.empno = e.empno) x;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("\nOUTER APPLY ("),
            "OUTER APPLY should start on a new line like a JOIN, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_lateral_derived_table_uses_from_item_query_depth() {
        let source = "SELECT d.deptno, x.max_sal FROM dept d, LATERAL (SELECT MAX(e.sal) AS max_sal FROM emp e WHERE e.deptno = d.deptno) x;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();

        let from_idx = find_line_starting_with(&lines, "FROM dept d,").expect("outer FROM line");
        let lateral_idx =
            find_line_starting_with(&lines, "LATERAL (").expect("LATERAL owner line");
        let select_idx =
            find_line_starting_with(&lines, "SELECT MAX").expect("LATERAL inner SELECT line");
        let inner_from_idx =
            find_line_starting_with(&lines, "FROM emp e").expect("LATERAL inner FROM line");
        let close_idx = lines
            .iter()
            .enumerate()
            .skip(inner_from_idx.saturating_add(1))
            .find(|(_, line)| line.trim_start().starts_with(") x"))
            .map(|(idx, _)| idx)
            .expect("LATERAL closing line");

        assert!(
            leading_spaces(lines[lateral_idx]) > leading_spaces(lines[from_idx]),
            "LATERAL derived table should indent under the FROM item list, got:\n{}",
            formatted
        );
        assert!(
            leading_spaces(lines[select_idx]) > leading_spaces(lines[lateral_idx]),
            "SELECT inside LATERAL should indent deeper than the owner line, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[select_idx]),
            leading_spaces(lines[inner_from_idx]),
            "Nested query clauses inside LATERAL should share one query depth, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[close_idx]),
            leading_spaces(lines[lateral_idx]),
            "LATERAL closing parenthesis should realign with the owner line, got:\n{}",
            formatted
        );
    }

    // ── Bug regression: PIVOT/UNPIVOT alignment ──

    #[test]
    fn format_sql_pivot_stays_intact() {
        let source = r#"SELECT * FROM (SELECT deptno, job, sal FROM emp) PIVOT (SUM(sal) FOR job IN ('CLERK' AS clerk, 'MANAGER' AS manager));"#;
        let formatted = SqlEditorWidget::format_sql_basic(source);
        // PIVOT block must be present and correctly formatted
        assert!(
            formatted.contains("PIVOT ("),
            "PIVOT keyword and opening paren should be present, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("SUM (sal)") || formatted.contains("SUM(sal)"),
            "PIVOT aggregate should be present, got:\n{}",
            formatted
        );
        let formatted_again = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            formatted, formatted_again,
            "PIVOT formatting should be idempotent, got:\n{}",
            formatted_again
        );
    }

    // ── Bug regression: FETCH FIRST ... WITH TIES ──

    #[test]
    fn format_sql_fetch_first_with_ties_not_split() {
        let source = "SELECT * FROM emp ORDER BY sal DESC FETCH FIRST 3 ROWS WITH TIES;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        // WITH TIES must not become a separate WITH clause
        assert!(
            !formatted.contains("\nWITH TIES"),
            "WITH TIES should not be split to a new line as a WITH clause, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("ROWS WITH TIES"),
            "WITH TIES should stay on the FETCH line, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_offset_and_fetch_comments_keep_clause_continuation_indent() {
        let source = "SELECT e.empno, e.ename FROM emp e ORDER BY e.empno OFFSET -- skip first page\n10 ROWS FETCH -- page size\nFIRST 5 ROWS ONLY;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();

        let offset_idx = find_line_starting_with(&lines, "OFFSET").expect("OFFSET clause line");
        let offset_rows_idx =
            find_line_starting_with(&lines, "10 ROWS").expect("OFFSET operand line");
        let fetch_idx = find_line_starting_with(&lines, "FETCH").expect("FETCH clause line");
        let fetch_rows_idx =
            find_line_starting_with(&lines, "FIRST 5 ROWS ONLY;").expect("FETCH operand line");

        assert!(
            lines[offset_idx].contains("-- skip first page")
                || lines.iter().any(|line| line.trim_start().starts_with("-- skip first page")),
            "OFFSET comment should be preserved in formatted output, got:\n{}",
            formatted
        );
        assert!(
            leading_spaces(lines[offset_rows_idx]) > leading_spaces(lines[offset_idx]),
            "OFFSET operand should stay deeper than the OFFSET clause line, got:\n{}",
            formatted
        );
        assert!(
            lines[fetch_idx].contains("-- page size")
                || lines.iter().any(|line| line.trim_start().starts_with("-- page size")),
            "FETCH comment should be preserved in formatted output, got:\n{}",
            formatted
        );
        assert!(
            leading_spaces(lines[fetch_rows_idx]) > leading_spaces(lines[fetch_idx]),
            "FETCH operand should stay deeper than the FETCH clause line, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_versions_between_comment_continuation_stays_attached_to_from_clause() {
        let source = "SELECT versions_starttime, versions_endtime, employee_id, salary FROM employees VERSIONS BETWEEN TIMESTAMP -- keep temporal boundary\nSYSTIMESTAMP - INTERVAL '7' DAY AND SYSTIMESTAMP WHERE employee_id = 100;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        let lines: Vec<&str> = formatted.lines().collect();

        let from_idx = find_line_starting_with(&lines, "FROM employees VERSIONS BETWEEN TIMESTAMP")
            .expect("VERSIONS BETWEEN owner line");
        let boundary_idx = find_line_starting_with(
            &lines,
            "SYSTIMESTAMP - INTERVAL '7' DAY AND SYSTIMESTAMP",
        )
        .expect("VERSIONS BETWEEN boundary line");
        let where_idx = find_line_starting_with(&lines, "WHERE employee_id = 100;")
            .expect("WHERE line after VERSIONS BETWEEN");

        assert!(
            lines[from_idx].contains("-- keep temporal boundary")
                || lines
                    .iter()
                    .any(|line| line.trim_start().starts_with("-- keep temporal boundary")),
            "Temporal clause comment should be preserved in formatted output, got:\n{}",
            formatted
        );
        assert!(
            leading_spaces(lines[boundary_idx]) > leading_spaces(lines[from_idx]),
            "VERSIONS BETWEEN boundary should stay deeper than the FROM owner line, got:\n{}",
            formatted
        );
        assert_eq!(
            leading_spaces(lines[where_idx]),
            leading_spaces(lines[from_idx]),
            "WHERE should realign with the FROM clause after temporal continuation, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_log_errors_into_and_reject_limit_stay_inline() {
        let source = "INSERT INTO target_emp (empno, ename) SELECT empno, ename FROM staging_emp LOG ERRORS INTO err$_target_emp ('LOAD1') REJECT LIMIT UNLIMITED;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("LOG ERRORS INTO err$_target_emp ('LOAD1')"),
            "LOG ERRORS INTO should stay on one clause line, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("REJECT LIMIT UNLIMITED"),
            "REJECT LIMIT should stay on one clause line, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("\nINTO err$_target_emp") && !formatted.contains("\nLIMIT UNLIMITED"),
            "LOG ERRORS INTO / REJECT LIMIT must not be split by clause keywords, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_model_rules_update_stays_on_rules_line() {
        let source = "SELECT * FROM sales MODEL PARTITION BY (deptno) DIMENSION BY (month_key) MEASURES (amt) RULES UPDATE (amt[1] = amt[CV(month_key)] * 1.1);";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("\n    RULES UPDATE ("),
            "MODEL RULES UPDATE should stay in the same subclause header, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("\nUPDATE ("),
            "MODEL RULES UPDATE must not split UPDATE as a top-level clause, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_forall_values_of_stays_in_header() {
        let source = "DECLARE TYPE idx_tab IS TABLE OF PLS_INTEGER; l_idx idx_tab := idx_tab(1, 3, 5); BEGIN FORALL i IN VALUES OF l_idx INSERT INTO t_log (id, msg) VALUES (i, 'x'); END; /";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("FORALL i IN VALUES OF l_idx"),
            "FORALL header should keep VALUES OF inline, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("IN\n        VALUES OF") && !formatted.contains("IN\n    VALUES OF"),
            "VALUES OF must not be split out of the FORALL header, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_multiset_except_stays_inline() {
        let source = "DECLARE TYPE num_nt IS TABLE OF NUMBER; nt_a num_nt := num_nt(1, 2, 3); nt_b num_nt := num_nt(2, 3); v_diff num_nt; BEGIN v_diff := nt_a MULTISET EXCEPT DISTINCT nt_b; END; /";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("MULTISET EXCEPT DISTINCT nt_b"),
            "MULTISET EXCEPT should remain an expression operator, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("MULTISET\n") && !formatted.contains("\nEXCEPT DISTINCT nt_b"),
            "MULTISET EXCEPT must not be treated as a set-operator clause break, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_multiset_union_stays_inline() {
        let source = "DECLARE TYPE num_nt IS TABLE OF NUMBER; nt_a num_nt := num_nt(1, 2); nt_b num_nt := num_nt(3, 4); v_all num_nt; BEGIN v_all := nt_a MULTISET UNION DISTINCT nt_b; END; /";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("MULTISET UNION DISTINCT nt_b"),
            "MULTISET UNION should remain an expression operator, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("MULTISET\n") && !formatted.contains("\nUNION DISTINCT nt_b"),
            "MULTISET UNION must not be treated as a top-level set operator, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_json_object_with_unique_keys_stays_inside_function() {
        let source = "SELECT JSON_OBJECT('id' VALUE e.empno, 'name' VALUE e.ename WITH UNIQUE KEYS) AS j FROM emp e;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("WITH UNIQUE KEYS"),
            "JSON_OBJECT WITH UNIQUE KEYS should remain inside the function call, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("\nWITH UNIQUE KEYS"),
            "JSON_OBJECT WITH UNIQUE KEYS must not be treated as a new WITH clause, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_json_query_with_wrapper_stays_inside_function() {
        let source = "SELECT JSON_QUERY(e.payload, '$.items[*]' WITH WRAPPER) AS items_json FROM emp_json e;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("WITH WRAPPER"),
            "JSON_QUERY WITH WRAPPER should remain inside the function call, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("\nWITH WRAPPER"),
            "JSON_QUERY WITH WRAPPER must not be treated as a new WITH clause, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_json_transform_set_and_insert_stay_inside_function() {
        let source = "SELECT JSON_TRANSFORM(e.payload, SET '$.status' = 'DONE', INSERT '$.audit.user' = USER) AS payload2 FROM emp_json e;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("SET '$.status' = 'DONE'"),
            "JSON_TRANSFORM SET operation should remain inside the function call, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("INSERT '$.audit.user' = USER"),
            "JSON_TRANSFORM INSERT operation should remain inside the function call, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("\nSET '$.status' = 'DONE'\nFROM")
                && !formatted.contains("\nINSERT '$.audit.user' = USER\nFROM"),
            "JSON_TRANSFORM operations must not escape the function argument context, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_with_package_declaration_stays_structured() {
        let source = "WITH PACKAGE pkg_demo AS FUNCTION f RETURN NUMBER; END pkg_demo; SELECT 1 FROM dual;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("WITH\n    PACKAGE pkg_demo AS"),
            "WITH PACKAGE should start a structured declaration block, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("\n        FUNCTION f RETURN NUMBER;"),
            "WITH PACKAGE members should be indented under the declaration, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("\n    END pkg_demo;\nSELECT 1"),
            "WITH PACKAGE should close before the main query at the correct depth, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_with_type_declaration_stays_attached_to_main_query() {
        let source = "WITH TYPE t_num IS TABLE OF NUMBER; SELECT * FROM TABLE(t_num(1, 2, 3));";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        assert!(
            formatted.contains("WITH\n    TYPE t_num IS TABLE OF NUMBER;\nSELECT *"),
            "WITH TYPE declaration should stay attached to the main query, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("FROM TABLE (t_num (1, 2, 3))")
                || formatted.contains("FROM TABLE(t_num(1, 2, 3))"),
            "Main query after WITH TYPE should remain intact, got:\n{}",
            formatted
        );
    }

    // ── Bug regression: WITH CTE + UPDATE/MERGE/DELETE/INSERT comma indent ──

    #[test]
    fn format_sql_with_update_cte_keeps_set_indent() {
        let source = "WITH src AS (\nSELECT 1 AS c1, 2 AS c2 FROM dual\n)\nUPDATE tgt t\nSET t.c1 = (SELECT c1 FROM src),\nt.c2 = (SELECT c2 FROM src),\nt.c3 = 3\nWHERE EXISTS (SELECT 1 FROM src);";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        // After the CTE, the UPDATE SET commas should indent under SET, not at root depth
        let lines: Vec<&str> = formatted.lines().collect();
        let leading_spaces = |line: &str| line.len().saturating_sub(line.trim_start().len());
        let set_line = lines.iter().find(|l| l.trim_start().starts_with("SET")).unwrap();
        let c2_line = lines
            .iter()
            .find(|l| l.trim_start().starts_with("t.c2"))
            .unwrap();
        let c3_line = lines
            .iter()
            .find(|l| l.trim_start().starts_with("t.c3"))
            .unwrap();
        let set_indent = leading_spaces(set_line);
        let c2_indent = leading_spaces(c2_line);
        let c3_indent = leading_spaces(c3_line);
        assert!(
            c2_indent > 0,
            "t.c2 should be indented (not at root depth), got:\n{}",
            formatted
        );
        assert_eq!(
            c2_indent, c3_indent,
            "SET list items should have consistent indent, got:\n{}",
            formatted
        );
        assert!(
            c2_indent >= set_indent,
            "SET list items should be at or deeper than SET clause, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_with_merge_cte_keeps_set_indent() {
        let source = "WITH src AS (\nSELECT 1 AS id, 2 AS val FROM dual\n)\nMERGE INTO tgt t\nUSING src s\nON (t.id = s.id)\nWHEN MATCHED THEN\nUPDATE SET t.val = s.val, t.extra = 0\nWHEN NOT MATCHED THEN\nINSERT (id, val) VALUES (s.id, s.val);";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        // Commas in UPDATE SET inside MERGE after WITH should not be treated as CTE separators
        assert!(
            !formatted.contains("\nt.extra"),
            "SET list commas in WITH+MERGE should not drop to root indent, got:\n{}",
            formatted
        );
    }

    #[test]
    fn format_sql_with_delete_cte_keeps_where_indent() {
        let source = "WITH old AS (\nSELECT id FROM archive WHERE ts < SYSDATE - 30\n)\nDELETE FROM main_tbl\nWHERE id IN (SELECT id FROM old);";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        // WITH + DELETE: the CTE state must close so WHERE gets proper clause indent
        assert!(
            formatted.contains("\nWHERE"),
            "WHERE should start on its own line, got:\n{}",
            formatted
        );
        // Idempotency check
        let formatted_again = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            formatted, formatted_again,
            "WITH+DELETE formatting should be idempotent, got:\n{}",
            formatted_again
        );
    }

    #[test]
    fn format_sql_with_insert_cte_keeps_values_indent() {
        let source = "WITH src AS (\nSELECT 1 AS c1, 2 AS c2 FROM dual\n)\nINSERT INTO tgt (c1, c2)\nSELECT c1, c2 FROM src;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        // WITH + INSERT ... SELECT: the CTE should close properly
        assert!(
            formatted.contains("INSERT INTO tgt"),
            "INSERT after CTE should have proper layout, got:\n{}",
            formatted
        );
        // Idempotency check
        let formatted_again = SqlEditorWidget::format_sql_basic(&formatted);
        assert_eq!(
            formatted, formatted_again,
            "WITH+INSERT formatting should be idempotent, got:\n{}",
            formatted_again
        );
    }

    // ── Bug regression: split_format_items DESC is not DESCRIBE ──

    #[test]
    fn split_format_items_desc_in_order_by_is_not_describe() {
        let source = "SELECT e.empno, e.sal\nFROM emp e\nORDER BY\ne.sal\nDESC;";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        // DESC must remain part of the SELECT statement, not split off as DESCRIBE
        assert!(
            formatted.contains("DESC;") || formatted.contains("DESC\n"),
            "DESC should remain in the statement, got:\n{}",
            formatted
        );
        assert!(
            !formatted.contains("DESCRIBE"),
            "DESC in ORDER BY must not be treated as DESCRIBE command, got:\n{}",
            formatted
        );
    }

    // ── Bug regression: MATCH_RECOGNIZE DEFINE with arguments ──

    #[test]
    fn split_format_items_match_recognize_define_with_args_is_sql() {
        let source = "SELECT *\nFROM ticks\nMATCH_RECOGNIZE (\nPARTITION BY symbol\nORDER BY ts\nPATTERN (A+)\nDEFINE A AS price > PREV(price)\n);";
        let formatted = SqlEditorWidget::format_sql_basic(source);
        // DEFINE A AS ... inside MATCH_RECOGNIZE must not be mistaken for SQL*Plus DEFINE
        assert!(
            formatted.contains("DEFINE"),
            "DEFINE should remain in the statement, got:\n{}",
            formatted
        );
        assert!(
            formatted.contains("MATCH_RECOGNIZE"),
            "MATCH_RECOGNIZE should be present, got:\n{}",
            formatted
        );
        // Should be a single formatted statement, not split
        let items = crate::db::QueryExecutor::split_format_items(source);
        let stmt_count = items
            .iter()
            .filter(|item| matches!(item, crate::db::FormatItem::Statement(_)))
            .count();
        assert_eq!(
            stmt_count, 1,
            "MATCH_RECOGNIZE with DEFINE should be one statement, got {} statements from: {:?}",
            stmt_count, items
        );
    }

}
