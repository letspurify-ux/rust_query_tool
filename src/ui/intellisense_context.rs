use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::sql_parser_engine::SplitState;
use crate::sql_text;
use crate::ui::sql_depth::{
    apply_paren_token, is_top_level_depth, paren_depths, split_top_level_symbol_groups,
    ParenDepthState,
};
use crate::ui::sql_editor::SqlToken;

/// SQL clause phase within a query at a specific depth level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlPhase {
    Initial,
    WithClause,
    SelectList,
    IntoClause,
    FromClause,
    JoinCondition,
    WhereClause,
    GroupByClause,
    HavingClause,
    OrderByClause,
    SetClause,
    ConnectByClause,
    StartWithClause,
    MatchRecognizeClause,
    ValuesClause,
    UpdateTarget,
    DeleteTarget,
    MergeTarget,
    PivotClause,
    ModelClause,
}

impl SqlPhase {
    pub fn is_column_context(&self) -> bool {
        matches!(
            self,
            SqlPhase::SelectList
                | SqlPhase::WhereClause
                | SqlPhase::JoinCondition
                | SqlPhase::GroupByClause
                | SqlPhase::HavingClause
                | SqlPhase::OrderByClause
                | SqlPhase::SetClause
                | SqlPhase::ValuesClause
                | SqlPhase::ConnectByClause
                | SqlPhase::StartWithClause
                | SqlPhase::MatchRecognizeClause
                | SqlPhase::PivotClause
                | SqlPhase::ModelClause
        )
    }

    pub fn is_table_context(&self) -> bool {
        matches!(
            self,
            SqlPhase::FromClause
                | SqlPhase::IntoClause
                | SqlPhase::UpdateTarget
                | SqlPhase::DeleteTarget
                | SqlPhase::MergeTarget
        )
    }
}

/// A table/view reference with optional alias, collected from a query scope.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ScopedTableRef {
    pub name: String,
    pub alias: Option<String>,
    pub depth: usize,
    pub is_cte: bool,
}

/// CTE definition parsed from WITH clause.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CteDefinition {
    pub name: String,
    pub explicit_columns: Vec<String>,
    /// Token range for explicit column list inside `WITH cte(col1, col2) ...`.
    pub explicit_column_range: Option<TokenRange>,
    /// Token range inside `CursorContext.statement_tokens` for the CTE body.
    pub body_range: TokenRange,
}

/// A subquery alias with its body token range, for column inference.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SubqueryDefinition {
    pub alias: String,
    pub body_range: TokenRange,
    pub depth: usize,
}

/// Inclusive-exclusive token range `[start, end)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenRange {
    pub start: usize,
    pub end: usize,
}

impl TokenRange {
    pub fn empty() -> Self {
        Self { start: 0, end: 0 }
    }

    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }
}

/// Result of deep context analysis at cursor position.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CursorContext {
    /// Full token stream for the normalized statement.
    pub statement_tokens: Arc<[SqlToken]>,
    /// Number of tokens located before/at cursor in `statement_tokens`.
    pub cursor_token_len: usize,
    /// Current SQL phase at cursor position
    pub phase: SqlPhase,
    /// Current parenthesis nesting depth (0 = top level)
    pub depth: usize,
    /// All tables visible at cursor position (current scope + parent scopes + CTEs)
    pub tables_in_scope: Vec<ScopedTableRef>,
    /// CTEs defined in WITH clause
    pub ctes: Vec<CteDefinition>,
    /// Subquery aliases with their body tokens for column inference
    pub subqueries: Vec<SubqueryDefinition>,
    /// The qualifier before cursor (e.g., "t" in "t.col")
    pub qualifier: Option<String>,
    /// Resolved table names for the qualifier
    pub qualifier_tables: Vec<String>,
}

/// CTE parsing state machine
#[derive(Debug, Clone, Copy, PartialEq)]
enum CteState {
    Inactive,
    ExpectName,
    AfterName,
    ExpectAs,
    ExpectBody,
    InBody { body_depth: usize },
}

impl CteState {
    fn enter_body(self, body_depth: usize) -> Self {
        if matches!(self, Self::ExpectBody) {
            Self::InBody { body_depth }
        } else {
            self
        }
    }

    fn enter_explicit_column_list(self) -> Self {
        if matches!(self, Self::AfterName) {
            Self::ExpectAs
        } else {
            self
        }
    }

    fn close_parenthesis(self, current_depth: usize) -> Self {
        match self {
            Self::InBody { body_depth } if body_depth == current_depth => Self::Inactive,
            other => other,
        }
    }
}

/// FROM/INTO/JOIN relation parsing state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelationParseState {
    Idle,
    ExpectTable,
}

impl RelationParseState {
    fn expect_table(&mut self) {
        *self = Self::ExpectTable;
    }

    fn clear(&mut self) {
        *self = Self::Idle;
    }

    fn is_expect_table(self) -> bool {
        matches!(self, Self::ExpectTable)
    }
}

/// Analyze the SQL text from statement start to cursor position.
/// Returns a `CursorContext` describing the phase, depth, and available tables.
///
/// `full_statement` is the complete statement token stream.
/// `cursor_token_len` is the count of tokens before/at cursor.
pub fn analyze_cursor_context(
    full_statement: &[SqlToken],
    cursor_token_len: usize,
) -> CursorContext {
    let clamped_cursor_token_len = cursor_token_len.min(full_statement.len());
    let statement_tokens: Arc<[SqlToken]> = full_statement.to_vec().into();
    let parse_result = scan_cursor_context(statement_tokens.as_ref(), clamped_cursor_token_len);
    let table_analysis = filter_scope_entries(
        &parse_result.parsed_tables,
        &parse_result.parsed_subqueries,
        &parse_result.visible_scope_chain,
    );
    let ctes = parse_ctes(statement_tokens.as_ref());

    let mut tables_in_scope = table_analysis.tables;
    for cte in &ctes {
        let already = tables_in_scope
            .iter()
            .any(|t| t.name.eq_ignore_ascii_case(&cte.name));
        if !already {
            tables_in_scope.push(ScopedTableRef {
                name: cte.name.clone(),
                alias: None,
                depth: 0,
                is_cte: true,
            });
        }
    }

    CursorContext {
        statement_tokens,
        cursor_token_len: clamped_cursor_token_len,
        phase: parse_result.phase,
        depth: parse_result.depth,
        tables_in_scope,
        ctes,
        subqueries: table_analysis.subqueries,
        qualifier: None,
        qualifier_tables: Vec::new(),
    }
}

/// Returns true for functions whose syntax includes a FROM keyword as part of
/// the function call rather than a SQL clause (e.g. `EXTRACT(YEAR FROM ...)`,
/// `TRIM(LEADING '0' FROM ...)`, `SUBSTRING(col FROM ...)`).
fn is_from_consuming_function(name: &str) -> bool {
    matches!(
        name,
        "EXTRACT" | "TRIM" | "SUBSTRING" | "OVERLAY" | "POSITION" | "NORMALIZE" | "TRIM_ARRAY"
    )
}

/// FROM-clause table functions that may reference left-side row source aliases.
fn is_from_lateral_table_function(name: &str) -> bool {
    matches!(name, "JSON_TABLE" | "XMLTABLE" | "UNNEST" | "TABLE")
}

fn relation_function_name_hint(table_name: &str) -> Option<String> {
    table_name
        .split('@')
        .next()
        .and_then(|name_without_dblink| {
            name_without_dblink
                .rsplit('.')
                .find(|segment| !segment.trim().is_empty())
        })
        .map(strip_identifier_quotes)
        .map(|name| name.to_ascii_uppercase())
}

fn is_table_target_statement_keyword(word: &str) -> bool {
    matches!(
        word,
        "ALTER"
            | "DROP"
            | "LOCK"
            | "TRUNCATE"
            | "FLASHBACK"
            | "RENAME"
            | "ANALYZE"
            | "OPTIMIZE"
            | "CHECK"
            | "REPAIR"
    )
}

fn is_comment_on_target(tokens: &[SqlToken], idx: usize, last_word: Option<&str>) -> bool {
    if !matches!(last_word, Some("ON")) {
        return false;
    }

    let mut saw_on_keyword = false;
    let mut scan_idx = idx;
    while scan_idx > 0 {
        scan_idx -= 1;
        match tokens.get(scan_idx) {
            Some(SqlToken::Comment(_)) => continue,
            Some(SqlToken::Word(word)) => {
                if !saw_on_keyword && word.eq_ignore_ascii_case("ON") {
                    saw_on_keyword = true;
                    continue;
                }
                return saw_on_keyword && word.eq_ignore_ascii_case("COMMENT");
            }
            _ => return false,
        }
    }

    false
}

fn is_comment_on_materialized_view_target(
    tokens: &[SqlToken],
    idx: usize,
    last_word: Option<&str>,
) -> bool {
    if !matches!(last_word, Some(prev) if prev.eq_ignore_ascii_case("MATERIALIZED")) {
        return false;
    }

    let mut significant_words = Vec::with_capacity(3);
    let mut scan_idx = idx;
    while scan_idx > 0 && significant_words.len() < 3 {
        scan_idx -= 1;
        match tokens.get(scan_idx) {
            Some(SqlToken::Comment(_)) => continue,
            Some(SqlToken::Word(word)) => significant_words.push(word.to_ascii_uppercase()),
            _ => break,
        }
    }

    matches!(
        significant_words.as_slice(),
        [first, second, third] if first == "MATERIALIZED" && second == "ON" && third == "COMMENT"
    )
}
fn is_create_on_table_target(tokens: &[SqlToken], idx: usize) -> bool {
    let mut scan_idx = idx;
    let mut saw_create_keyword = false;
    let mut saw_object_keyword = false;

    while scan_idx > 0 {
        scan_idx -= 1;
        match tokens.get(scan_idx) {
            Some(SqlToken::Comment(_)) => continue,
            Some(SqlToken::Symbol(sym)) if sym == ";" => break,
            Some(SqlToken::Word(word)) => {
                let upper = word.to_ascii_uppercase();
                if upper == "CREATE" {
                    saw_create_keyword = true;
                    break;
                }

                if matches!(upper.as_str(), "INDEX" | "TRIGGER") {
                    saw_object_keyword = true;
                }

                // CREATE INDEX / CREATE TRIGGER statements often include
                // additional modifiers before the ON-target table name
                // (e.g. `CREATE UNIQUE INDEX ... ON`,
                // `CREATE OR REPLACE TRIGGER ... ON`).
                // Keep scanning until statement start instead of treating
                // intermediate keywords as hard failures.
            }
            _ => {}
        }
    }

    saw_create_keyword && saw_object_keyword
}

fn is_create_table_target(tokens: &[SqlToken], idx: usize) -> bool {
    let mut scan_idx = idx;
    let mut saw_create_keyword = false;

    while scan_idx > 0 {
        scan_idx -= 1;
        match tokens.get(scan_idx) {
            Some(SqlToken::Comment(_)) => continue,
            Some(SqlToken::Symbol(sym)) if sym == ";" => break,
            Some(SqlToken::Word(word)) => {
                let upper = word.to_ascii_uppercase();
                if upper == "CREATE" {
                    saw_create_keyword = true;
                    break;
                }

                if matches!(
                    upper.as_str(),
                    "GLOBAL" | "LOCAL" | "TEMP" | "TEMPORARY" | "UNLOGGED" | "TRANSIENT"
                ) {
                    continue;
                }

                return false;
            }
            _ => return false,
        }
    }

    saw_create_keyword
}

fn is_with_plsql_declaration_keyword(keyword: &str) -> bool {
    matches!(keyword, "FUNCTION" | "PROCEDURE")
}

fn should_enter_with_clause(
    current_phase: SqlPhase,
    depth: usize,
    last_word: Option<&str>,
) -> bool {
    if matches!(current_phase, SqlPhase::Initial) {
        return true;
    }
    // Preserve hierarchical-query `START WITH` semantics.
    if matches!(last_word, Some(prev) if prev.eq_ignore_ascii_case("START")) {
        return false;
    }
    // Nested subqueries can inherit a non-Initial parent phase (e.g. WHERE),
    // but a leading WITH right after `(` still starts a query scope.
    depth > 0 && last_word.is_none()
}

fn find_order_by_keyword(tokens: &[SqlToken], start_idx: usize) -> Option<usize> {
    let (next_keyword, next_idx) = next_word_upper(tokens, start_idx)?;
    if next_keyword == "BY" {
        return Some(next_idx);
    }
    if next_keyword == "SIBLINGS" {
        let (tail_keyword, tail_idx) = next_word_upper(tokens, next_idx + 1)?;
        if tail_keyword == "BY" {
            return Some(tail_idx);
        }
    }
    None
}

fn is_locking_for_clause(tokens: &[SqlToken], start_idx: usize) -> bool {
    let Some((first_keyword, first_idx)) = next_word_upper(tokens, start_idx) else {
        return false;
    };

    if matches!(first_keyword.as_str(), "UPDATE" | "SHARE") {
        return true;
    }

    if first_keyword == "NO" {
        return matches!(
            next_word_upper(tokens, first_idx + 1),
            Some((next_keyword, _)) if next_keyword == "KEY"
        ) && matches!(
            next_word_upper(tokens, first_idx + 2),
            Some((tail_keyword, _)) if tail_keyword == "UPDATE"
        );
    }

    if first_keyword == "KEY" {
        return matches!(
            next_word_upper(tokens, first_idx + 1),
            Some((tail_keyword, _)) if tail_keyword == "SHARE"
        );
    }

    false
}

fn locking_for_clause_has_of_target(tokens: &[SqlToken], start_idx: usize) -> bool {
    let Some((first_keyword, first_idx)) = next_word_upper(tokens, start_idx) else {
        return false;
    };

    let after_locking_idx = match first_keyword.as_str() {
        "UPDATE" | "SHARE" => first_idx + 1,
        "NO" => {
            let Some((second_keyword, second_idx)) = next_word_upper(tokens, first_idx + 1) else {
                return false;
            };
            if second_keyword != "KEY" {
                return false;
            }

            let Some((third_keyword, third_idx)) = next_word_upper(tokens, second_idx + 1) else {
                return false;
            };
            if third_keyword != "UPDATE" {
                return false;
            }
            third_idx + 1
        }
        "KEY" => {
            let Some((second_keyword, second_idx)) = next_word_upper(tokens, first_idx + 1) else {
                return false;
            };
            if second_keyword != "SHARE" {
                return false;
            }
            second_idx + 1
        }
        _ => return false,
    };

    matches!(
        next_word_upper(tokens, after_locking_idx),
        Some((keyword, _)) if keyword == "OF"
    )
}

fn is_read_consistency_for_clause(tokens: &[SqlToken], start_idx: usize) -> bool {
    let Some((first_keyword, first_idx)) = next_word_upper(tokens, start_idx) else {
        return false;
    };

    if first_keyword != "READ" {
        return false;
    }

    matches!(
        next_word_upper(tokens, first_idx + 1),
        Some((second_keyword, _)) if second_keyword == "ONLY" || second_keyword == "WRITE"
    )
}

fn is_post_query_for_clause(tokens: &[SqlToken], start_idx: usize) -> bool {
    let Some((first_keyword, _)) = next_word_upper(tokens, start_idx) else {
        return false;
    };

    matches!(first_keyword.as_str(), "JSON" | "XML" | "BROWSE")
}

fn is_query_expression_start(tokens: &[SqlToken], start_idx: usize) -> bool {
    let mut idx = skip_comment_tokens(tokens, start_idx);

    while matches!(tokens.get(idx), Some(SqlToken::Symbol(sym)) if sym == "(") {
        idx = skip_comment_tokens(tokens, idx + 1);
    }

    matches!(
        tokens.get(idx),
        Some(SqlToken::Word(word))
            if matches!(
                word.to_ascii_uppercase().as_str(),
                "SELECT"
                    | "WITH"
                    | "VALUES"
                    | "TABLE"
                    | "ONLY"
                    | "CONTAINERS"
                    | "SHARDS"
                    | "ROWS"
            )
    )
}

#[derive(Debug, Clone)]
struct ParsedTableEntry {
    table: ScopedTableRef,
    scope_id: usize,
}

#[derive(Debug, Clone)]
struct ParsedSubqueryEntry {
    subquery: SubqueryDefinition,
    scope_id: usize,
}

#[derive(Debug, Clone)]
struct CursorScanResult {
    phase: SqlPhase,
    depth: usize,
    visible_scope_chain: Vec<usize>,
    parsed_tables: Vec<ParsedTableEntry>,
    parsed_subqueries: Vec<ParsedSubqueryEntry>,
}

/// Build visible scope chain from current scope to root.
fn build_visible_scope_chain(
    scope_stack: &[usize],
    visible_parent: &HashMap<usize, Option<usize>>,
) -> Vec<usize> {
    let mut visible_scope_chain = Vec::new();
    let mut scope_id = *scope_stack.last().unwrap_or(&0);
    visible_scope_chain.push(scope_id);
    while let Some(Some(parent_id)) = visible_parent.get(&scope_id) {
        visible_scope_chain.push(*parent_id);
        scope_id = *parent_id;
    }
    visible_scope_chain.reverse();
    visible_scope_chain
}

fn snapshot_cursor_state(
    depth: usize,
    query_depth: usize,
    depth_frames: &[ParserDepthFrame],
    scope_stack: &[usize],
    visible_parent: &HashMap<usize, Option<usize>>,
) -> (SqlPhase, usize, Vec<usize>) {
    (
        depth_frames
            .get(depth)
            .map(|frame| frame.phase)
            .unwrap_or(SqlPhase::Initial),
        query_depth,
        build_visible_scope_chain(scope_stack, visible_parent),
    )
}

#[derive(Debug, Clone)]
struct ParserDepthFrame {
    phase: SqlPhase,
    is_query_scope: bool,
    statement_kind: StatementKind,
    paren_func: Option<String>,
    function_from_state: FunctionFromState,
    returning_clause_active: bool,
    locking_clause_active: bool,
}

fn reset_relation_lookbehind(
    relation_modifier_state: &mut RelationModifierState,
    relation_state: &mut RelationParseState,
    last_word: &mut Option<String>,
) {
    relation_modifier_state.clear();
    relation_state.clear();
    *last_word = None;
}

fn close_parenthesis_scope(
    parser_state: &mut SplitState,
    depth: &mut usize,
    query_depth: &mut usize,
    depth_frames: &mut Vec<ParserDepthFrame>,
    scope_stack: &mut Vec<usize>,
) {
    if depth_frames
        .get(*depth)
        .map(|frame| frame.is_query_scope)
        .unwrap_or(false)
        && *depth > 0
    {
        *query_depth = query_depth.saturating_sub(1);
    }

    parser_state.paren_depth = parser_state.paren_depth.saturating_sub(1);
    *depth = parser_state.paren_depth;

    if scope_stack.len() > 1 {
        scope_stack.pop();
    }
    if depth_frames.len() > 1 {
        depth_frames.pop();
    }
}

fn begin_set_operator_operand_scope(
    scope_stack: &mut [usize],
    next_scope_id: &mut usize,
    visible_parent: &mut HashMap<usize, Option<usize>>,
) {
    let Some(current_scope) = scope_stack.last_mut() else {
        return;
    };

    let parent_scope = visible_parent.get(current_scope).copied().unwrap_or(None);
    let operand_scope = *next_scope_id;
    *next_scope_id = next_scope_id.saturating_add(1);
    visible_parent.insert(operand_scope, parent_scope);
    *current_scope = operand_scope;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatementKind {
    Unknown,
    Delete,
    Merge,
    Rename,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FunctionFromState {
    NotApplicable,
    Available,
    Consumed,
}

impl FunctionFromState {
    fn from_function_name(function_name: Option<&str>) -> Self {
        if function_name.is_some_and(is_from_consuming_function) {
            Self::Available
        } else {
            Self::NotApplicable
        }
    }

    fn consume(&mut self) {
        if matches!(self, Self::Available) {
            *self = Self::Consumed;
        }
    }

    fn should_treat_from_as_function_argument(self) -> bool {
        matches!(self, Self::Available)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelationModifierState {
    None,
    LateralLikePending,
}

impl RelationModifierState {
    fn mark_lateral_like(&mut self) {
        *self = Self::LateralLikePending;
    }

    fn clear(&mut self) {
        *self = Self::None;
    }

    fn blocks_outer_scope_cutoff(self) -> bool {
        matches!(self, Self::LateralLikePending)
    }
}

impl Default for ParserDepthFrame {
    fn default() -> Self {
        Self {
            phase: SqlPhase::Initial,
            is_query_scope: false,
            statement_kind: StatementKind::Unknown,
            paren_func: None,
            function_from_state: FunctionFromState::NotApplicable,
            returning_clause_active: false,
            locking_clause_active: false,
        }
    }
}

/// Single-pass cursor parser:
/// - Tracks SQL phase/query depth at cursor
/// - Collects relation/subquery entries with scope ids
/// - Shares one keyword transition table for both phase and table collection
fn scan_cursor_context(tokens: &[SqlToken], cursor_token_len: usize) -> CursorScanResult {
    let mut parser_state = SplitState::default();
    let mut depth: usize = parser_state.paren_depth;
    let mut query_depth: usize = 0;
    let mut depth_frames: Vec<ParserDepthFrame> = vec![ParserDepthFrame::default()];
    let mut last_word: Option<String> = None;
    let mut relation_state = RelationParseState::Idle;
    let mut all_tables: Vec<ParsedTableEntry> = Vec::new();
    let mut all_subqueries: Vec<ParsedSubqueryEntry> = Vec::new();
    let mut subquery_tracks: Vec<(usize, usize)> = Vec::new(); // (depth, start_idx)

    let mut next_scope_id = 1usize;
    let mut scope_stack = vec![0usize];
    let mut visible_parent: HashMap<usize, Option<usize>> = HashMap::new();
    visible_parent.insert(0, None);

    let mut relation_modifier_state = RelationModifierState::None;
    let mut cte_state = CteState::Inactive;

    let mut cursor_snapshot: Option<(SqlPhase, usize, Vec<usize>)> = None;
    let mut idx = 0;

    let mark_query_scope =
        |depth: usize, depth_frames: &mut Vec<ParserDepthFrame>, query_depth: &mut usize| {
            if depth > 0
                && depth_frames
                    .get(depth)
                    .is_some_and(|frame| !frame.is_query_scope)
            {
                if let Some(frame) = depth_frames.get_mut(depth) {
                    frame.is_query_scope = true;
                }
                *query_depth = query_depth.saturating_add(1);
            }
        };

    while idx < tokens.len() {
        if cursor_snapshot.is_none() && idx == cursor_token_len {
            cursor_snapshot = Some(snapshot_cursor_state(
                depth,
                query_depth,
                &depth_frames,
                &scope_stack,
                &visible_parent,
            ));
        }

        let token = &tokens[idx];

        match token {
            SqlToken::Symbol(sym) if sym == "(" => {
                let parent_phase = depth_frames
                    .get(depth)
                    .map(|frame| frame.phase)
                    .unwrap_or(SqlPhase::Initial);
                let parent_scope_id = *scope_stack.last().unwrap_or(&0);
                parser_state.paren_depth = parser_state.paren_depth.saturating_add(1);
                depth = parser_state.paren_depth;

                let inherited_phase = if parent_phase.is_column_context()
                    || matches!(
                        parent_phase,
                        SqlPhase::ValuesClause | SqlPhase::IntoClause | SqlPhase::PivotClause
                    ) {
                    parent_phase
                } else {
                    SqlPhase::Initial
                };
                if depth_frames.len() <= depth {
                    depth_frames.push(ParserDepthFrame::default());
                }
                if let Some(frame) = depth_frames.get_mut(depth) {
                    frame.phase = inherited_phase;
                    frame.is_query_scope = false;
                    // Record the function name that preceded this '(' so we can
                    // distinguish function-internal FROM from SQL FROM clauses.
                    frame.paren_func = last_word.take().map(|w| w.to_ascii_uppercase());
                    frame.function_from_state =
                        FunctionFromState::from_function_name(frame.paren_func.as_deref());
                    frame.returning_clause_active = false;
                    frame.locking_clause_active = false;
                }

                let scope_id = next_scope_id;
                next_scope_id += 1;
                scope_stack.push(scope_id);

                let is_from_lateral_function = depth_frames
                    .get(depth)
                    .and_then(|frame| frame.paren_func.as_deref())
                    .is_some_and(is_from_lateral_table_function);
                let inherited_visible_parent = if matches!(parent_phase, SqlPhase::FromClause)
                    && !relation_modifier_state.blocks_outer_scope_cutoff()
                    && !is_from_lateral_function
                {
                    None
                } else {
                    Some(parent_scope_id)
                };
                visible_parent.insert(scope_id, inherited_visible_parent);

                relation_modifier_state.clear();
                relation_state.clear();

                if matches!(parent_phase, SqlPhase::FromClause)
                    && is_query_expression_start(tokens, idx + 1)
                {
                    subquery_tracks.push((depth, idx + 1));
                }

                cte_state = cte_state.enter_body(depth).enter_explicit_column_list();
                idx += 1;
                continue;
            }
            SqlToken::Symbol(sym) if sym == ")" => {
                cte_state = cte_state.close_parenthesis(depth);

                while subquery_tracks.last().is_some_and(|track| track.0 > depth) {
                    // Recover from malformed SQL with stale tracks.
                    subquery_tracks.pop();
                }

                let was_subquery = subquery_tracks.last().map(|t| t.0) == Some(depth);
                if let Some((_, start_idx)) = was_subquery.then(|| subquery_tracks.pop()).flatten()
                {
                    if start_idx <= idx {
                        let parent_scope_id = if scope_stack.len() >= 2 {
                            scope_stack[scope_stack.len() - 2]
                        } else {
                            0
                        };
                        let body_range = TokenRange {
                            start: start_idx,
                            end: idx,
                        };
                        if let Some((alias, next_idx)) = parse_subquery_alias(tokens, idx + 1) {
                            all_subqueries.push(ParsedSubqueryEntry {
                                subquery: SubqueryDefinition {
                                    alias: alias.clone(),
                                    body_range,
                                    depth: depth.saturating_sub(1),
                                },
                                scope_id: parent_scope_id,
                            });
                            all_tables.push(ParsedTableEntry {
                                table: ScopedTableRef {
                                    name: alias.clone(),
                                    alias: Some(alias),
                                    depth: depth.saturating_sub(1),
                                    is_cte: false,
                                },
                                scope_id: parent_scope_id,
                            });
                            idx = next_idx;
                            close_parenthesis_scope(
                                &mut parser_state,
                                &mut depth,
                                &mut query_depth,
                                &mut depth_frames,
                                &mut scope_stack,
                            );
                            reset_relation_lookbehind(
                                &mut relation_modifier_state,
                                &mut relation_state,
                                &mut last_word,
                            );
                            continue;
                        }

                        let generated_name = anonymous_subquery_name(start_idx, depth);
                        all_subqueries.push(ParsedSubqueryEntry {
                            subquery: SubqueryDefinition {
                                alias: generated_name.clone(),
                                body_range,
                                depth: depth.saturating_sub(1),
                            },
                            scope_id: parent_scope_id,
                        });
                        all_tables.push(ParsedTableEntry {
                            table: ScopedTableRef {
                                name: generated_name,
                                alias: None,
                                depth: depth.saturating_sub(1),
                                is_cte: false,
                            },
                            scope_id: parent_scope_id,
                        });
                    }
                }

                close_parenthesis_scope(
                    &mut parser_state,
                    &mut depth,
                    &mut query_depth,
                    &mut depth_frames,
                    &mut scope_stack,
                );
                reset_relation_lookbehind(
                    &mut relation_modifier_state,
                    &mut relation_state,
                    &mut last_word,
                );
                idx += 1;
                continue;
            }
            SqlToken::Comment(_) => {
                // Keep parser lookbehind state across comments so syntactic pairs like
                // `EXTRACT /*...*/ (...)` and `LATERAL /*...*/ (...)` are treated the
                // same as comment-free statements.
                idx += 1;
                continue;
            }
            SqlToken::String(_) => {
                idx += 1;
                continue;
            }
            SqlToken::Symbol(sym) if sym == "," => {
                relation_modifier_state.clear();
                let current_phase = depth_frames
                    .get(depth)
                    .map(|frame| frame.phase)
                    .unwrap_or(SqlPhase::Initial);
                if matches!(
                    current_phase,
                    SqlPhase::FromClause
                        | SqlPhase::PivotClause
                        | SqlPhase::ModelClause
                        | SqlPhase::MatchRecognizeClause
                ) {
                    depth_frames[depth].phase = SqlPhase::FromClause;
                    relation_state.expect_table();
                }
                if matches!(cte_state, CteState::Inactive)
                    && depth_frames
                        .get(depth)
                        .is_some_and(|frame| matches!(frame.phase, SqlPhase::WithClause))
                {
                    cte_state = CteState::ExpectName;
                }
                idx += 1;
                continue;
            }
            SqlToken::Symbol(sym) if sym == ";" => {
                let has_following_statement = tokens[idx + 1..]
                    .iter()
                    .any(|t| !matches!(t, SqlToken::Comment(_)));
                if idx >= cursor_token_len || !has_following_statement {
                    break;
                }

                all_tables.clear();
                all_subqueries.clear();
                subquery_tracks.clear();

                query_depth = 0;
                depth_frames = vec![ParserDepthFrame::default()];
                last_word = None;
                relation_state.clear();
                cte_state = CteState::Inactive;
                parser_state.paren_depth = 0;
                depth = 0;

                next_scope_id = 1;
                scope_stack = vec![0usize];
                visible_parent.clear();
                visible_parent.insert(0, None);
                relation_modifier_state.clear();

                idx += 1;
                continue;
            }
            SqlToken::Word(word) => {
                let upper = word.to_ascii_uppercase();

                // CTE state machine
                match cte_state {
                    CteState::ExpectName if upper != "RECURSIVE" => {
                        if is_with_plsql_declaration_keyword(upper.as_str()) {
                            cte_state = CteState::Inactive;
                        } else {
                            cte_state = CteState::AfterName;
                        }
                        idx += 1;
                        continue;
                    }
                    CteState::AfterName => {
                        if upper == "AS" {
                            cte_state = CteState::ExpectBody;
                        } else if sql_text::is_cte_recovery_keyword(&upper) {
                            cte_state = CteState::Inactive;
                            continue;
                        }
                        idx += 1;
                        continue;
                    }
                    CteState::ExpectAs => {
                        if upper == "AS" {
                            cte_state = CteState::ExpectBody;
                        } else if sql_text::is_cte_recovery_keyword(&upper) {
                            cte_state = CteState::Inactive;
                            continue;
                        }
                        idx += 1;
                        continue;
                    }
                    CteState::InBody { .. } => {
                        // Inside CTE body, process normally for phase tracking at this depth
                        // but don't break out of CTE state
                    }
                    CteState::Inactive => {}
                    _ => {
                        idx += 1;
                        continue;
                    }
                }

                // Ensure phase_stack has entry for current depth
                while depth_frames.len() <= depth {
                    depth_frames.push(ParserDepthFrame::default());
                }

                let current_phase = depth_frames[depth].phase;

                if upper == "LATERAL" && matches!(current_phase, SqlPhase::FromClause) {
                    relation_modifier_state.mark_lateral_like();
                    idx += 1;
                    continue;
                }
                if !(relation_modifier_state.blocks_outer_scope_cutoff()
                    && relation_state.is_expect_table())
                {
                    relation_modifier_state.clear();
                }

                match upper.as_str() {
                    "INSERT" => {
                        depth_frames[depth].returning_clause_active = false;
                        let current_statement_kind = depth_frames
                            .get(depth)
                            .map(|frame| frame.statement_kind)
                            .unwrap_or(StatementKind::Unknown);
                        let is_expression_context = current_phase.is_column_context()
                            || matches!(current_phase, SqlPhase::ValuesClause);
                        let is_merge_action_keyword =
                            matches!(current_statement_kind, StatementKind::Merge)
                                && matches!(current_phase, SqlPhase::JoinCondition);
                        if is_expression_context {
                            // Inside expressions, INSERT can be a valid identifier/token.
                            relation_state.clear();
                        } else if is_merge_action_keyword {
                            // `MERGE ... WHEN ... THEN INSERT (...) VALUES (...)` reuses
                            // INSERT as an action keyword (no target table). Keep it in
                            // expression/column context instead of table-target context.
                            depth_frames[depth].phase = SqlPhase::SetClause;
                            relation_state.clear();
                        } else {
                            mark_query_scope(depth, &mut depth_frames, &mut query_depth);
                            relation_state.clear();
                        }
                    }
                    "REPLACE" => {
                        depth_frames[depth].returning_clause_active = false;
                        let is_expression_context = current_phase.is_column_context()
                            || matches!(current_phase, SqlPhase::ValuesClause);
                        if is_expression_context {
                            // Inside expressions, REPLACE can be a scalar function name.
                            relation_state.clear();
                        } else {
                            // MySQL `REPLACE [INTO] table ...` behaves like INSERT for
                            // completion purposes: expect a target relation right after
                            // REPLACE, even when INTO is omitted.
                            depth_frames[depth].phase = SqlPhase::IntoClause;
                            depth_frames[depth].statement_kind = StatementKind::Unknown;
                            mark_query_scope(depth, &mut depth_frames, &mut query_depth);
                            relation_state.expect_table();
                        }
                    }
                    "WITH"
                        if should_enter_with_clause(current_phase, depth, last_word.as_deref()) =>
                    {
                        depth_frames[depth].phase = SqlPhase::WithClause;
                        mark_query_scope(depth, &mut depth_frames, &mut query_depth);
                        cte_state = CteState::ExpectName;
                        relation_state.clear();
                    }
                    "SELECT" => {
                        depth_frames[depth].phase = SqlPhase::SelectList;
                        depth_frames[depth].statement_kind = StatementKind::Unknown;
                        depth_frames[depth].returning_clause_active = false;
                        mark_query_scope(depth, &mut depth_frames, &mut query_depth);
                        relation_state.clear();
                    }
                    "FROM" => {
                        let from_belongs_to_distinct_predicate =
                            is_distinct_from_operator(tokens, idx)
                                && current_phase.is_column_context();
                        let should_treat_as_function_from = depth_frames
                            .get(depth)
                            .map(|frame| {
                                frame
                                    .function_from_state
                                    .should_treat_from_as_function_argument()
                            })
                            .unwrap_or(false);
                        if should_treat_as_function_from {
                            if let Some(frame) = depth_frames.get_mut(depth) {
                                frame.function_from_state.consume();
                            }
                        } else if from_belongs_to_distinct_predicate {
                            relation_state.clear();
                        } else {
                            depth_frames[depth].phase = SqlPhase::FromClause;
                            relation_state.expect_table();
                        }
                    }
                    "INTO" => {
                        let in_returning_clause = depth_frames
                            .get(depth)
                            .map(|frame| frame.returning_clause_active)
                            .unwrap_or(false);
                        if matches!(
                            current_phase,
                            SqlPhase::SelectList
                                | SqlPhase::Initial
                                | SqlPhase::MergeTarget
                                | SqlPhase::ValuesClause
                        ) || (matches!(current_phase, SqlPhase::SetClause)
                            && !in_returning_clause)
                        {
                            depth_frames[depth].phase = SqlPhase::IntoClause;
                            relation_state.expect_table();
                        } else {
                            relation_state.clear();
                        }
                    }
                    "OVERWRITE" if matches!(last_word.as_deref(), Some("INSERT")) => {
                        // Hive/Spark-style `INSERT OVERWRITE TABLE ...` keeps
                        // target relation context after OVERWRITE.
                        depth_frames[depth].phase = SqlPhase::IntoClause;
                        relation_state.expect_table();
                    }
                    "DIRECTORY" if matches!(last_word.as_deref(), Some("OVERWRITE")) => {
                        // `INSERT OVERWRITE DIRECTORY ...` targets a filesystem
                        // location rather than a table relation.
                        depth_frames[depth].phase = SqlPhase::Initial;
                        relation_state.clear();
                    }
                    "USING" => {
                        let current_statement_kind = depth_frames
                            .get(depth)
                            .map(|frame| frame.statement_kind)
                            .unwrap_or(StatementKind::Unknown);
                        if matches!(current_phase, SqlPhase::MergeTarget | SqlPhase::IntoClause)
                            || matches!(current_statement_kind, StatementKind::Delete)
                        {
                            depth_frames[depth].phase = SqlPhase::FromClause;
                            relation_state.expect_table();
                        } else if matches!(current_phase, SqlPhase::FromClause) {
                            // JOIN ... USING (...) is a join-condition context, not a relation target.
                            depth_frames[depth].phase = SqlPhase::JoinCondition;
                            relation_state.clear();
                        }
                    }
                    "TABLE"
                        if last_word
                            .as_deref()
                            .is_some_and(is_table_target_statement_keyword)
                            || is_comment_on_target(tokens, idx, last_word.as_deref())
                            || is_create_table_target(tokens, idx) =>
                    {
                        // DDL/DCL target object position (`TRUNCATE TABLE ...`,
                        // `LOCK TABLE ...`, `ALTER TABLE ...`, `DROP TABLE ...`,
                        // `FLASHBACK TABLE ...`, `COMMENT ON TABLE ...`,
                        // `CREATE [GLOBAL TEMPORARY] TABLE ...`)
                        // should provide table-name completion.
                        depth_frames[depth].phase = SqlPhase::IntoClause;
                        relation_state.expect_table();
                    }
                    "TABLE"
                        if matches!(current_phase, SqlPhase::IntoClause)
                            && relation_state.is_expect_table()
                            && !matches!(
                                tokens.get(skip_comment_tokens(tokens, idx + 1)),
                                Some(SqlToken::Symbol(sym)) if sym == "("
                            ) =>
                    {
                        // Optional `TABLE` introducer in DML target syntax
                        // (`INSERT INTO TABLE t`, `INSERT OVERWRITE TABLE t`).
                        relation_state.expect_table();
                    }

                    "VIEW"
                        if is_comment_on_target(tokens, idx, last_word.as_deref())
                            || is_comment_on_materialized_view_target(
                                tokens,
                                idx,
                                last_word.as_deref(),
                            ) =>
                    {
                        // `COMMENT ON VIEW ...` and `COMMENT ON MATERIALIZED VIEW ...`
                        // use the same object-target position as COMMENT ON TABLE.
                        depth_frames[depth].phase = SqlPhase::IntoClause;
                        relation_state.expect_table();
                    }
                    "JOIN" | "APPLY" => {
                        if upper == "APPLY" {
                            relation_modifier_state.mark_lateral_like();
                        }
                        depth_frames[depth].phase = SqlPhase::FromClause;
                        relation_state.expect_table();
                    }
                    "STRAIGHT_JOIN" => {
                        if matches!(current_phase, SqlPhase::FromClause) {
                            depth_frames[depth].phase = SqlPhase::FromClause;
                            relation_state.expect_table();
                        }
                    }
                    "ON" => {
                        if matches!(current_phase, SqlPhase::FromClause) {
                            depth_frames[depth].phase = SqlPhase::JoinCondition;
                        } else if is_create_on_table_target(tokens, idx) {
                            depth_frames[depth].phase = SqlPhase::IntoClause;
                            relation_state.expect_table();
                        } else {
                            relation_state.clear();
                        }
                    }
                    "WHERE" => {
                        depth_frames[depth].phase = SqlPhase::WhereClause;
                        relation_state.clear();
                    }
                    "GROUP" => {
                        if !is_within_group_keyword(tokens, idx) {
                            if let Some((next_keyword, next_idx)) = next_word_upper(tokens, idx + 1)
                            {
                                if next_keyword == "BY" {
                                    depth_frames[depth].phase = SqlPhase::GroupByClause;
                                    idx = next_idx; // skip BY (and any interleaved comments)
                                }
                            }
                        }
                        relation_state.clear();
                    }
                    "HAVING" => {
                        depth_frames[depth].phase = SqlPhase::HavingClause;
                        relation_state.clear();
                    }
                    "ORDER" => {
                        if let Some(by_idx) = find_order_by_keyword(tokens, idx + 1) {
                            depth_frames[depth].phase = SqlPhase::OrderByClause;
                            idx = by_idx; // skip BY (and any interleaved comments)
                        }
                        relation_state.clear();
                    }
                    "FOR" => {
                        if is_locking_for_clause(tokens, idx + 1) {
                            depth_frames[depth].locking_clause_active = true;
                            // Locking clauses (`FOR UPDATE [OF ...]`, `FOR SHARE [OF ...]`,
                            // `FOR NO KEY UPDATE [OF ...]`, `FOR KEY SHARE [OF ...]`)
                            // can accept column references after `OF`.
                            if locking_for_clause_has_of_target(tokens, idx + 1) {
                                depth_frames[depth].phase = SqlPhase::SetClause;
                            } else {
                                depth_frames[depth].phase = SqlPhase::OrderByClause;
                            }
                        } else if is_read_consistency_for_clause(tokens, idx + 1) {
                            // Read-consistency qualifiers (`FOR READ ONLY`, `FOR READ WRITE`)
                            // are end-of-query boundaries and must not keep table context.
                            depth_frames[depth].phase = SqlPhase::OrderByClause;
                        } else if is_post_query_for_clause(tokens, idx + 1)
                            || matches!(current_phase, SqlPhase::FromClause)
                        {
                            // Dialect-specific trailing clauses such as SQL Server
                            // `FOR JSON` / `FOR XML` / `FOR BROWSE` appear after
                            // FROM/WHERE and are not
                            // relation-target contexts.
                            depth_frames[depth].phase = SqlPhase::OrderByClause;
                        }
                        relation_state.clear();
                    }
                    "WAIT" | "NOWAIT" => {
                        if depth_frames
                            .get(depth)
                            .is_some_and(|frame| frame.locking_clause_active)
                        {
                            // Oracle lock options after `FOR UPDATE [OF ...]` are trailing
                            // modifiers, not expression/table contexts.
                            depth_frames[depth].phase = SqlPhase::OrderByClause;
                            relation_state.clear();
                        }
                    }
                    "SKIP" => {
                        if depth_frames
                            .get(depth)
                            .is_some_and(|frame| frame.locking_clause_active)
                        {
                            let next_is_locked = matches!(
                                next_word_upper(tokens, idx + 1),
                                Some((next, _)) if next == "LOCKED"
                            );
                            let prev_idx = prev_non_comment_index(tokens, idx);
                            let follows_of_item_separator = matches!(
                                tokens.get(prev_idx),
                                Some(SqlToken::Word(word)) if word.eq_ignore_ascii_case("OF")
                            ) || matches!(tokens.get(prev_idx), Some(SqlToken::Symbol(sym)) if sym == ",");

                            if next_is_locked || !follows_of_item_separator {
                                // `SKIP` begins `SKIP LOCKED` after at least one OF-target item,
                                // but can still be an identifier right after `OF` or `,`.
                                depth_frames[depth].phase = SqlPhase::OrderByClause;
                                relation_state.clear();
                            }
                        }
                    }
                    "LOCKED" => {
                        if depth_frames
                            .get(depth)
                            .is_some_and(|frame| frame.locking_clause_active)
                            && matches!(last_word.as_deref(), Some("SKIP"))
                        {
                            depth_frames[depth].phase = SqlPhase::OrderByClause;
                            relation_state.clear();
                        }
                    }
                    "WINDOW" => {
                        // Treat SQL-standard WINDOW clause expressions as column context.
                        depth_frames[depth].phase = SqlPhase::OrderByClause;
                        relation_state.clear();
                    }
                    "QUALIFY" => {
                        // QUALIFY filters rows using analytic expressions, similar to WHERE.
                        depth_frames[depth].phase = SqlPhase::WhereClause;
                        relation_state.clear();
                    }
                    "LIMIT" | "OFFSET" | "FETCH" => {
                        // Pagination clauses are post-FROM boundaries; they must not keep
                        // relation-target parsing active even when ORDER BY is omitted.
                        depth_frames[depth].phase = SqlPhase::OrderByClause;
                        relation_state.clear();
                    }
                    "SET" => {
                        if matches!(
                            current_phase,
                            SqlPhase::WithClause | SqlPhase::OrderByClause
                        ) && matches!(cte_state, CteState::Inactive)
                        {
                            // Recursive CTE SEARCH/CYCLE clauses use `... BY ... SET ...`
                            // where SET is not a DML SET clause.
                            depth_frames[depth].phase = SqlPhase::WithClause;
                        } else {
                            depth_frames[depth].phase = SqlPhase::SetClause;
                        }
                        relation_state.clear();
                    }
                    "SEARCH" | "CYCLE" => {
                        if matches!(current_phase, SqlPhase::WithClause) {
                            // Oracle recursive CTE clauses (`SEARCH ... BY ...`,
                            // `CYCLE ... SET ...`) expect column expressions.
                            depth_frames[depth].phase = SqlPhase::OrderByClause;
                        }
                        relation_state.clear();
                    }
                    "RETURNING" => {
                        // DML RETURNING lists target columns/expressions.
                        depth_frames[depth].phase = SqlPhase::SetClause;
                        depth_frames[depth].returning_clause_active = true;
                        depth_frames[depth].locking_clause_active = false;
                        relation_state.clear();
                    }
                    "UPDATE" => {
                        depth_frames[depth].returning_clause_active = false;
                        let current_statement_kind = depth_frames
                            .get(depth)
                            .map(|frame| frame.statement_kind)
                            .unwrap_or(StatementKind::Unknown);
                        let is_expression_context = current_phase.is_column_context()
                            || matches!(current_phase, SqlPhase::ValuesClause);
                        let is_merge_action_keyword =
                            matches!(current_statement_kind, StatementKind::Merge)
                                && matches!(current_phase, SqlPhase::JoinCondition);
                        let is_mysql_conflict_update =
                            is_mysql_on_duplicate_key_update(tokens, idx);
                        let is_postgres_conflict_update =
                            is_postgres_on_conflict_do_update(tokens, idx);
                        let is_locking_update_keyword = matches!(last_word.as_deref(), Some("FOR"));
                        if is_locking_update_keyword {
                            // `FOR UPDATE OF ...` lock clause inside SELECT statements.
                            if locking_for_clause_has_of_target(tokens, idx) {
                                depth_frames[depth].phase = SqlPhase::SetClause;
                            } else {
                                depth_frames[depth].phase = SqlPhase::OrderByClause;
                            }
                            relation_state.clear();
                        } else if is_merge_action_keyword
                            || is_mysql_conflict_update
                            || is_postgres_conflict_update
                        {
                            depth_frames[depth].locking_clause_active = false;
                            // `... ON DUPLICATE KEY UPDATE ...` and
                            // `... ON CONFLICT ... DO UPDATE ...` use UPDATE as an action keyword.
                            depth_frames[depth].phase = SqlPhase::SetClause;
                            relation_state.clear();
                        } else if is_expression_context {
                            depth_frames[depth].locking_clause_active = false;
                            // Inside expressions, UPDATE can be a valid identifier/token.
                            relation_state.clear();
                        } else {
                            depth_frames[depth].locking_clause_active = false;
                            depth_frames[depth].phase = SqlPhase::UpdateTarget;
                            depth_frames[depth].statement_kind = StatementKind::Unknown;
                            mark_query_scope(depth, &mut depth_frames, &mut query_depth);
                            relation_state.expect_table();
                        }
                    }
                    "DELETE" => {
                        depth_frames[depth].returning_clause_active = false;
                        depth_frames[depth].locking_clause_active = false;
                        let current_statement_kind = depth_frames
                            .get(depth)
                            .map(|frame| frame.statement_kind)
                            .unwrap_or(StatementKind::Unknown);
                        let is_expression_context = current_phase.is_column_context()
                            || matches!(current_phase, SqlPhase::ValuesClause);
                        let is_merge_action_keyword =
                            matches!(current_statement_kind, StatementKind::Merge)
                                && matches!(current_phase, SqlPhase::JoinCondition);
                        if is_expression_context {
                            // Inside expressions, DELETE can be a valid identifier/token.
                            relation_state.clear();
                        } else if is_merge_action_keyword {
                            // `MERGE ... WHEN MATCHED THEN DELETE WHERE ...` DELETE is an
                            // action keyword, not a standalone DML target clause.
                            depth_frames[depth].phase = SqlPhase::WhereClause;
                            relation_state.clear();
                        } else {
                            depth_frames[depth].phase = SqlPhase::DeleteTarget;
                            depth_frames[depth].statement_kind = StatementKind::Delete;
                            mark_query_scope(depth, &mut depth_frames, &mut query_depth);
                            relation_state.expect_table();
                        }
                    }
                    "MERGE" => {
                        depth_frames[depth].returning_clause_active = false;
                        depth_frames[depth].locking_clause_active = false;
                        depth_frames[depth].phase = SqlPhase::MergeTarget;
                        depth_frames[depth].statement_kind = StatementKind::Merge;
                        mark_query_scope(depth, &mut depth_frames, &mut query_depth);
                        relation_state.clear();
                    }
                    "RENAME" => {
                        let is_expression_context = current_phase.is_column_context()
                            || matches!(current_phase, SqlPhase::ValuesClause);
                        if is_expression_context {
                            relation_state.clear();
                        } else {
                            depth_frames[depth].phase = SqlPhase::IntoClause;
                            depth_frames[depth].statement_kind = StatementKind::Rename;
                            relation_state.expect_table();
                        }
                    }
                    "TO" => {
                        let current_statement_kind = depth_frames
                            .get(depth)
                            .map(|frame| frame.statement_kind)
                            .unwrap_or(StatementKind::Unknown);
                        if matches!(current_statement_kind, StatementKind::Rename)
                            && matches!(current_phase, SqlPhase::IntoClause)
                        {
                            depth_frames[depth].phase = SqlPhase::Initial;
                            depth_frames[depth].statement_kind = StatementKind::Unknown;
                        }
                        relation_state.clear();
                    }
                    "CONNECT" => {
                        if let Some((next_keyword, next_idx)) = next_word_upper(tokens, idx + 1) {
                            if next_keyword == "BY" {
                                depth_frames[depth].phase = SqlPhase::ConnectByClause;
                                idx = next_idx;
                            }
                        }
                        relation_state.clear();
                    }
                    "START" => {
                        if let Some((next_keyword, next_idx)) = next_word_upper(tokens, idx + 1) {
                            if next_keyword == "WITH" {
                                depth_frames[depth].phase = SqlPhase::StartWithClause;
                                idx = next_idx;
                            }
                        }
                        relation_state.clear();
                    }
                    "VALUES" => {
                        depth_frames[depth].phase = SqlPhase::ValuesClause;
                        mark_query_scope(depth, &mut depth_frames, &mut query_depth);
                        relation_state.clear();
                    }
                    "MATCH_RECOGNIZE" => {
                        depth_frames[depth].phase = SqlPhase::MatchRecognizeClause;
                        relation_state.clear();
                    }
                    "MATCH" => {
                        if let Some((next_keyword, next_idx)) = next_word_upper(tokens, idx + 1) {
                            if next_keyword == "RECOGNIZE" {
                                depth_frames[depth].phase = SqlPhase::MatchRecognizeClause;
                                relation_state.clear();
                                idx = next_idx;
                            }
                        }
                    }
                    "PIVOT" | "UNPIVOT" => {
                        depth_frames[depth].phase = SqlPhase::PivotClause;
                        relation_state.clear();
                    }
                    "MODEL" => {
                        depth_frames[depth].phase = SqlPhase::ModelClause;
                        relation_state.clear();
                    }
                    "UNION" | "INTERSECT" | "EXCEPT" | "MINUS" => {
                        depth_frames[depth].phase = SqlPhase::Initial;
                        relation_state.clear();
                        begin_set_operator_operand_scope(
                            &mut scope_stack,
                            &mut next_scope_id,
                            &mut visible_parent,
                        );
                    }
                    kw if is_table_stop_keyword(kw) && relation_state.is_expect_table() => {
                        relation_state.clear();
                    }
                    _ => {
                        if relation_state.is_expect_table() {
                            if let Some((table_name, next_idx)) = parse_table_name_deep(tokens, idx)
                            {
                                let relation_name_hint = relation_function_name_hint(&table_name);
                                let has_immediate_argument_list = matches!(
                                    tokens.get(next_idx),
                                    Some(SqlToken::Symbol(sym)) if sym == "("
                                );
                                let can_consume_relation_arguments =
                                    matches!(current_phase, SqlPhase::FromClause)
                                        && has_immediate_argument_list
                                        && (relation_modifier_state.blocks_outer_scope_cutoff()
                                            || relation_name_hint.is_some());
                                let relation_arg_parsed = if can_consume_relation_arguments
                                    && has_immediate_argument_list
                                {
                                    extract_parenthesized_range(tokens, next_idx)
                                } else {
                                    None
                                };
                                let relation_arg_range =
                                    relation_arg_parsed.map(|(range, _)| range);
                                let relation_arg_end = relation_arg_parsed
                                    .map(|(_, arg_end_idx)| arg_end_idx)
                                    .unwrap_or(next_idx);
                                let (alias, after_alias) =
                                    parse_alias_deep(tokens, relation_arg_end);
                                let alias = alias.or_else(|| {
                                    parse_alias_after_derived_relation_clauses(
                                        tokens,
                                        relation_arg_end,
                                    )
                                });
                                let alias_present = alias.is_some();
                                let scope_id = *scope_stack.last().unwrap_or(&0);
                                let is_lateral_table_function = relation_name_hint
                                    .as_deref()
                                    .is_some_and(is_from_lateral_table_function);
                                let table_scope_name = if is_lateral_table_function {
                                    alias.clone().unwrap_or_else(|| table_name.clone())
                                } else {
                                    table_name.clone()
                                };
                                if let (Some(alias_name), Some(body_range), Some(function_name)) = (
                                    alias.as_ref(),
                                    relation_arg_range,
                                    relation_name_hint.as_deref(),
                                ) {
                                    if is_from_lateral_table_function(function_name) {
                                        all_subqueries.push(ParsedSubqueryEntry {
                                            subquery: SubqueryDefinition {
                                                alias: alias_name.clone(),
                                                body_range,
                                                depth,
                                            },
                                            scope_id,
                                        });
                                    }
                                }
                                all_tables.push(ParsedTableEntry {
                                    table: ScopedTableRef {
                                        name: table_scope_name,
                                        alias,
                                        depth,
                                        is_cte: false,
                                    },
                                    scope_id,
                                });
                                if let Some(SqlToken::Symbol(sym)) = tokens.get(after_alias) {
                                    if sym == "," {
                                        relation_modifier_state.clear();
                                        relation_state.expect_table();
                                        last_word = None;
                                        idx = after_alias + 1;
                                        continue;
                                    }
                                    if sym == "(" && !alias_present {
                                        // Preserve table-function name for immediate
                                        // parenthesized argument scope handling.
                                        last_word = relation_name_hint;
                                        relation_modifier_state.mark_lateral_like();
                                    } else {
                                        last_word = None;
                                    }
                                } else {
                                    last_word = None;
                                }
                                if !matches!(tokens.get(after_alias), Some(SqlToken::Symbol(sym)) if sym == "(")
                                {
                                    relation_modifier_state.clear();
                                }
                                relation_state.clear();
                                idx = after_alias;
                                continue;
                            }
                            relation_state.clear();
                        }
                    }
                }
                last_word = Some(upper);
            }
            _ => {
                relation_modifier_state.clear();
                last_word = None;
            }
        }
        idx += 1;
    }

    if cursor_snapshot.is_none() {
        cursor_snapshot = Some(snapshot_cursor_state(
            depth,
            query_depth,
            &depth_frames,
            &scope_stack,
            &visible_parent,
        ));
    }
    let (phase, cursor_query_depth, cursor_visible_scope_chain) =
        cursor_snapshot.unwrap_or((SqlPhase::Initial, 0usize, vec![0usize]));

    CursorScanResult {
        phase,
        depth: cursor_query_depth,
        visible_scope_chain: cursor_visible_scope_chain,
        parsed_tables: all_tables,
        parsed_subqueries: all_subqueries,
    }
}

struct TableAnalysis {
    tables: Vec<ScopedTableRef>,
    subqueries: Vec<SubqueryDefinition>,
}

fn anonymous_subquery_name(start_idx: usize, depth: usize) -> String {
    format!("__SUBQUERY_{}_{}", depth, start_idx)
}

/// Collect all table references from the full statement, tracking depth.
/// Returns tables visible from the cursor's active scope chain.
fn collect_tables_deep(
    tokens: &[SqlToken],
    cursor_scope_chain: &[usize],
    cursor_token_len: usize,
) -> TableAnalysis {
    let scan_result = scan_cursor_context(tokens, cursor_token_len);
    filter_scope_entries(
        &scan_result.parsed_tables,
        &scan_result.parsed_subqueries,
        cursor_scope_chain,
    )
}

fn filter_scope_entries(
    parsed_tables: &[ParsedTableEntry],
    parsed_subqueries: &[ParsedSubqueryEntry],
    visible_scope_chain: &[usize],
) -> TableAnalysis {
    let visible_scope_ids: HashSet<usize> = visible_scope_chain.iter().copied().collect();

    let tables = parsed_tables
        .iter()
        .filter(|entry| visible_scope_ids.contains(&entry.scope_id))
        .map(|entry| entry.table.clone())
        .collect();

    let subqueries = parsed_subqueries
        .iter()
        .filter(|entry| visible_scope_ids.contains(&entry.scope_id))
        .map(|entry| entry.subquery.clone())
        .collect();

    TableAnalysis { tables, subqueries }
}

pub fn token_range_slice(tokens: &[SqlToken], range: TokenRange) -> &[SqlToken] {
    let start = range.start.min(tokens.len());
    let end = range.end.min(tokens.len());
    if start >= end {
        &tokens[0..0]
    } else {
        &tokens[start..end]
    }
}

fn extract_parenthesized_range(
    tokens: &[SqlToken],
    open_idx: usize,
) -> Option<(TokenRange, usize)> {
    match tokens.get(open_idx) {
        Some(SqlToken::Symbol(sym)) if sym == "(" => {}
        _ => return None,
    }

    let mut depth = 1usize;
    let mut idx = open_idx + 1;
    while idx < tokens.len() {
        match &tokens[idx] {
            SqlToken::Symbol(sym) if sym == "(" => depth = depth.saturating_add(1),
            SqlToken::Symbol(sym) if sym == ")" => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some((
                        TokenRange {
                            start: open_idx + 1,
                            end: idx,
                        },
                        idx + 1,
                    ));
                }
            }
            _ => {}
        }
        idx += 1;
    }

    Some((
        TokenRange {
            start: open_idx.saturating_add(1).min(tokens.len()),
            end: tokens.len(),
        },
        tokens.len(),
    ))
}

/// Parse CTE definitions from WITH clause.
fn parse_ctes(tokens: &[SqlToken]) -> Vec<CteDefinition> {
    let mut ctes = Vec::new();
    let mut idx = 0;

    // Find top-level WITH keyword
    let mut paren_state = ParenDepthState::default();
    let mut found_with = false;
    while idx < tokens.len() {
        let token = &tokens[idx];
        match token {
            SqlToken::Word(w) if paren_state.depth() == 0 && w.eq_ignore_ascii_case("WITH") => {
                idx += 1;
                found_with = true;
                break;
            }
            // If we hit a top-level statement keyword before WITH, no CTEs.
            SqlToken::Word(w) if paren_state.depth() == 0 => {
                let u = w.to_ascii_uppercase();
                if sql_text::is_with_main_query_keyword(&u) {
                    return ctes;
                }
            }
            _ => {}
        }
        apply_paren_token(&mut paren_state, token);
        idx += 1;
    }

    if !found_with {
        return ctes;
    }

    idx = skip_comment_tokens(tokens, idx);

    // Skip RECURSIVE if present
    if let Some(SqlToken::Word(w)) = tokens.get(idx) {
        if w.eq_ignore_ascii_case("RECURSIVE") {
            idx += 1;
        }
    }

    // Parse CTE definitions
    loop {
        if idx >= tokens.len() {
            break;
        }

        idx = skip_comment_tokens(tokens, idx);

        // Expect CTE name
        let cte_name = match tokens.get(idx) {
            Some(SqlToken::Word(w)) => {
                let u = w.to_ascii_uppercase();
                if matches!(
                    u.as_str(),
                    "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "MERGE"
                ) {
                    break;
                }
                if is_with_plsql_declaration_keyword(u.as_str()) {
                    break;
                }
                w.clone()
            }
            _ => break,
        };
        idx += 1;
        idx = skip_comment_tokens(tokens, idx);

        let mut explicit_columns = Vec::new();
        let mut explicit_column_range = None;

        // Check for explicit column list: cte_name(col1, col2)
        if let Some(SqlToken::Symbol(s)) = tokens.get(idx) {
            if s == "(" {
                if let Some((expr_range, next_idx)) = extract_parenthesized_range(tokens, idx) {
                    explicit_column_range = Some(expr_range);
                    let expr_tokens = token_range_slice(tokens, expr_range);
                    let expr_depths = paren_depths(expr_tokens);
                    idx = next_idx;
                    for (expr_idx, token) in expr_tokens.iter().enumerate() {
                        if !is_top_level_depth(&expr_depths, expr_idx) {
                            continue;
                        }
                        if let SqlToken::Word(w) = token {
                            explicit_columns.push(w.clone());
                        }
                    }
                }
            }
        }

        idx = skip_comment_tokens(tokens, idx);

        // Expect AS
        if let Some(SqlToken::Word(w)) = tokens.get(idx) {
            if w.eq_ignore_ascii_case("AS") {
                idx += 1;
            }
        }

        idx = skip_comment_tokens(tokens, idx);
        if let Some(SqlToken::Word(w)) = tokens.get(idx) {
            if w.eq_ignore_ascii_case("NOT") {
                let materialized_idx = skip_comment_tokens(tokens, idx + 1);
                if matches!(tokens.get(materialized_idx), Some(SqlToken::Word(next)) if next.eq_ignore_ascii_case("MATERIALIZED"))
                {
                    idx = materialized_idx + 1;
                }
            } else if w.eq_ignore_ascii_case("MATERIALIZED") {
                idx += 1;
            }
        }

        idx = skip_comment_tokens(tokens, idx);

        // Capture CTE body token range (balanced parens).
        let mut body_range = TokenRange::empty();
        if let Some(SqlToken::Symbol(s)) = tokens.get(idx) {
            if s == "(" {
                if let Some((captured_range, next_idx)) = extract_parenthesized_range(tokens, idx) {
                    idx = next_idx;
                    body_range = captured_range;
                }
            }
        }

        ctes.push(CteDefinition {
            name: cte_name,
            explicit_columns,
            explicit_column_range,
            body_range,
        });

        // Check for comma (another CTE) or end
        idx = skip_comment_tokens(tokens, idx);
        match tokens.get(idx) {
            Some(SqlToken::Symbol(s)) if s == "," => {
                idx += 1;
                continue;
            }
            _ => break,
        }
    }

    ctes
}

/// Peek at the next word token (skipping comments) and return its uppercase form.
fn next_word_upper(tokens: &[SqlToken], idx: usize) -> Option<(String, usize)> {
    let mut current_idx = idx;
    while current_idx < tokens.len() {
        match &tokens[current_idx] {
            SqlToken::Comment(_) => {
                current_idx += 1;
                continue;
            }
            SqlToken::Word(word) => {
                return Some((word.to_ascii_uppercase(), current_idx));
            }
            _ => return None,
        }
    }
    None
}

fn prev_word_upper(tokens: &[SqlToken], before_idx: usize) -> Option<(String, usize)> {
    let mut current_idx = before_idx;
    while current_idx > 0 {
        current_idx -= 1;
        match &tokens[current_idx] {
            SqlToken::Comment(_) => continue,
            SqlToken::Word(word) => return Some((word.to_ascii_uppercase(), current_idx)),
            _ => continue,
        }
    }
    None
}

fn is_distinct_from_operator(tokens: &[SqlToken], from_idx: usize) -> bool {
    let Some((prev_word, prev_idx)) = prev_word_upper(tokens, from_idx) else {
        return false;
    };
    if prev_word != "DISTINCT" {
        return false;
    }

    let Some((second_prev_word, _)) = prev_word_upper(tokens, prev_idx) else {
        return false;
    };

    second_prev_word == "IS" || second_prev_word == "NOT"
}

fn is_within_group_keyword(tokens: &[SqlToken], group_idx: usize) -> bool {
    let Some((prev_word, _)) = prev_word_upper(tokens, group_idx) else {
        return false;
    };

    prev_word == "WITHIN"
}

fn previous_word_chain_matches(tokens: &[SqlToken], before_idx: usize, chain: &[&str]) -> bool {
    let mut cursor = before_idx;
    for expected in chain {
        let Some((word, word_idx)) = prev_word_upper(tokens, cursor) else {
            return false;
        };
        if word != *expected {
            return false;
        }
        cursor = word_idx;
    }
    true
}

fn is_mysql_on_duplicate_key_update(tokens: &[SqlToken], update_idx: usize) -> bool {
    previous_word_chain_matches(tokens, update_idx, &["KEY", "DUPLICATE", "ON"])
}

fn is_postgres_on_conflict_do_update(tokens: &[SqlToken], update_idx: usize) -> bool {
    let Some((prev_word, do_idx)) = prev_word_upper(tokens, update_idx) else {
        return false;
    };
    if prev_word != "DO" {
        return false;
    }

    let mut cursor = do_idx;
    while let Some((word, word_idx)) = prev_word_upper(tokens, cursor) {
        if word == "CONFLICT" {
            return prev_word_upper(tokens, word_idx).is_some_and(|(maybe_on, _)| maybe_on == "ON");
        }
        cursor = word_idx;
    }

    false
}

fn strip_identifier_quotes(value: &str) -> String {
    crate::sql_text::strip_identifier_quotes(value)
}

fn normalize_identifier_for_lookup(value: &str) -> String {
    strip_identifier_quotes(value).to_ascii_uppercase()
}

fn split_identifier_parts_for_lookup(value: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut chars = value.trim().chars().peekable();
    let mut in_quotes = false;

    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                current.push(ch);
                if in_quotes {
                    if chars.peek().copied() == Some('"') {
                        current.push('"');
                        chars.next();
                    } else {
                        in_quotes = false;
                    }
                } else {
                    in_quotes = true;
                }
            }
            '.' if !in_quotes => {
                let segment = strip_identifier_quotes(current.trim());
                if !segment.is_empty() {
                    parts.push(segment);
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    let segment = strip_identifier_quotes(current.trim());
    if !segment.is_empty() {
        parts.push(segment);
    }

    parts
}

fn last_identifier_part_for_lookup(value: &str) -> Option<String> {
    split_identifier_parts_for_lookup(value).into_iter().last()
}

fn is_quoted_identifier(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"')
}

fn is_identifier_word_token(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return false;
    }
    if is_quoted_identifier(trimmed) {
        return !strip_identifier_quotes(trimmed).is_empty();
    }
    trimmed
        .chars()
        .next()
        .is_some_and(sql_text::is_identifier_start_char)
}

fn normalize_table_name_part(value: &str) -> String {
    let trimmed = value.trim();
    let unquoted = strip_identifier_quotes(trimmed);
    let is_quoted = is_quoted_identifier(trimmed);
    if is_quoted && unquoted.contains('.') {
        trimmed.to_string()
    } else {
        unquoted
    }
}

/// Parse a table name at the given position (handling schema.table format).
fn parse_table_name_deep(tokens: &[SqlToken], start: usize) -> Option<(String, usize)> {
    match tokens.get(start) {
        Some(SqlToken::Symbol(sym)) if sym == "(" => None,
        Some(SqlToken::Word(word)) => {
            if let Some((wrapped_name, next_idx)) =
                parse_relation_wrapper_table_name(tokens, start, word)
            {
                return Some((wrapped_name, next_idx));
            }
            let is_quoted = word.trim().starts_with('"') && word.trim().ends_with('"');
            let upper = word.to_ascii_uppercase();
            // Skip if this is a keyword rather than a table name
            if !is_quoted && (is_join_keyword(&upper) || is_table_stop_keyword(&upper)) {
                return None;
            }
            if !is_identifier_word_token(word) {
                return None;
            }
            let mut parts = vec![normalize_table_name_part(word)];
            let mut idx = start + 1;
            // Handle dotted relation names like schema.table.
            while matches!(tokens.get(idx), Some(SqlToken::Symbol(sym)) if sym == ".") {
                if let Some(SqlToken::Word(name)) = tokens.get(idx + 1) {
                    if !is_identifier_word_token(name) {
                        break;
                    }
                    parts.push(normalize_table_name_part(name));
                    idx += 2;
                    continue;
                }
                break;
            }
            let mut table = parts.join(".");

            // Handle database-link suffixes like `schema.table@remote_link`.
            if matches!(tokens.get(idx), Some(SqlToken::Symbol(sym)) if sym == "@") {
                let mut dblink_idx = idx + 1;
                if let Some(SqlToken::Word(link_part)) = tokens.get(dblink_idx) {
                    if is_identifier_word_token(link_part) {
                        let mut dblink_parts = vec![normalize_table_name_part(link_part)];
                        dblink_idx += 1;

                        while matches!(tokens.get(dblink_idx), Some(SqlToken::Symbol(sym)) if sym == ".")
                        {
                            if let Some(SqlToken::Word(link_part)) = tokens.get(dblink_idx + 1) {
                                if !is_identifier_word_token(link_part) {
                                    break;
                                }
                                dblink_parts.push(normalize_table_name_part(link_part));
                                dblink_idx += 2;
                                continue;
                            }
                            break;
                        }

                        table.push('@');
                        table.push_str(&dblink_parts.join("."));
                        idx = dblink_idx;
                    }
                }
            }

            Some((table, idx))
        }
        _ => None,
    }
}

fn parse_relation_wrapper_table_name(
    tokens: &[SqlToken],
    start: usize,
    relation_word: &str,
) -> Option<(String, usize)> {
    let relation_upper = relation_word.to_ascii_uppercase();
    if relation_upper == "ROWS" {
        let from_idx = skip_comment_tokens(tokens, start + 1);
        if !matches!(tokens.get(from_idx), Some(SqlToken::Word(next)) if next.eq_ignore_ascii_case("FROM"))
        {
            return None;
        }

        let open_idx = skip_comment_tokens(tokens, from_idx + 1);
        if !matches!(tokens.get(open_idx), Some(SqlToken::Symbol(sym)) if sym == "(") {
            return None;
        }

        let (_, next_idx) = extract_parenthesized_range(tokens, open_idx)?;
        return Some((relation_upper, next_idx));
    }

    if !matches!(
        relation_upper.as_str(),
        "ONLY" | "TABLE" | "THE" | "CONTAINERS" | "SHARDS"
    ) {
        return None;
    }

    let open_idx = skip_comment_tokens(tokens, start + 1);

    if relation_upper == "ONLY" {
        if matches!(tokens.get(open_idx), Some(SqlToken::Symbol(sym)) if sym == "(") {
            let (inner_range, next_idx) = extract_parenthesized_range(tokens, open_idx)?;
            let inner_tokens = token_range_slice(tokens, inner_range);
            let (relation_name, _) = parse_table_name_deep(inner_tokens, 0)?;
            return Some((relation_name, next_idx));
        }

        return parse_table_name_deep(tokens, open_idx);
    }

    let Some(SqlToken::Symbol(sym)) = tokens.get(open_idx) else {
        return None;
    };
    if sym != "(" {
        return None;
    }

    let (inner_range, next_idx) = extract_parenthesized_range(tokens, open_idx)?;
    let inner_tokens = token_range_slice(tokens, inner_range);

    // TABLE/THE/CONTAINERS/SHARDS wrappers may contain collection function calls
    // or scalar subqueries. For identifier-like forms
    // (`TABLE(schema.collection_col)`, `CONTAINERS(schema.table)`) keep the
    // underlying name so alias resolution can target stable relation keys.
    if let Some((relation_name, _)) = parse_table_name_deep(inner_tokens, 0) {
        Some((relation_name, next_idx))
    } else {
        Some((relation_upper, next_idx))
    }
}

fn consume_optional_alias_column_list(tokens: &[SqlToken], start: usize) -> usize {
    let idx = skip_comment_tokens(tokens, start);
    match tokens.get(idx) {
        Some(SqlToken::Symbol(sym)) if sym == "(" => extract_parenthesized_range(tokens, idx)
            .map(|(_, next_idx)| next_idx)
            .unwrap_or(idx),
        _ => idx,
    }
}

fn parse_relation_alias_at(
    tokens: &[SqlToken],
    start: usize,
    allow_alias_column_list: bool,
) -> (Option<String>, usize) {
    let idx = skip_comment_tokens(tokens, start);
    let Some(SqlToken::Word(word)) = tokens.get(idx) else {
        return (None, idx);
    };

    let is_quoted = word.trim().starts_with('"') && word.trim().ends_with('"');
    let upper = word.to_ascii_uppercase();

    if upper == "AS" {
        if matches!(next_word_upper(tokens, idx + 1), Some((next, _)) if next == "OF") {
            return (None, idx);
        }
        let alias_idx = skip_comment_tokens(tokens, idx + 1);
        let Some(SqlToken::Word(alias_word)) = tokens.get(alias_idx) else {
            return (None, alias_idx);
        };
        if !is_identifier_word_token(alias_word) {
            return (None, alias_idx + 1);
        }
        let alias_is_quoted =
            alias_word.trim().starts_with('"') && alias_word.trim().ends_with('"');
        let alias_upper = alias_word.to_ascii_uppercase();
        if !alias_is_quoted && is_relation_alias_breaker(&alias_upper) {
            return (None, alias_idx);
        }
        let next_idx = if allow_alias_column_list {
            consume_optional_alias_column_list(tokens, alias_idx + 1)
        } else {
            alias_idx + 1
        };
        return (Some(strip_identifier_quotes(alias_word)), next_idx);
    }

    if !is_identifier_word_token(word) {
        return (None, idx);
    }
    if is_quoted || !is_relation_alias_breaker(&upper) {
        let next_idx = if allow_alias_column_list {
            consume_optional_alias_column_list(tokens, idx + 1)
        } else {
            idx + 1
        };
        return (Some(strip_identifier_quotes(word)), next_idx);
    }

    (None, idx)
}

/// Parse an optional alias after a table name.
fn parse_alias_deep(tokens: &[SqlToken], start: usize) -> (Option<String>, usize) {
    let start = skip_relation_postfix_clauses(tokens, start);
    parse_relation_alias_at(tokens, start, false)
}

fn parse_alias_after_derived_relation_clauses(tokens: &[SqlToken], start: usize) -> Option<String> {
    let derived_end = skip_derived_relation_postfix_clauses(tokens, start);
    if derived_end == start {
        return None;
    }
    let alias_start = skip_relation_postfix_clauses(tokens, derived_end);
    parse_relation_alias_at(tokens, alias_start, false).0
}

fn skip_relation_postfix_clauses(tokens: &[SqlToken], start: usize) -> usize {
    let mut idx = skip_comment_tokens(tokens, start);

    loop {
        let Some(SqlToken::Word(word)) = tokens.get(idx) else {
            break;
        };

        let upper = word.to_ascii_uppercase();
        match upper.as_str() {
            "INDEXED" => {
                let by_idx = skip_comment_tokens(tokens, idx + 1);
                if !matches!(tokens.get(by_idx), Some(SqlToken::Word(next)) if next.eq_ignore_ascii_case("BY"))
                {
                    break;
                }

                let index_name_idx = skip_comment_tokens(tokens, by_idx + 1);
                if !matches!(tokens.get(index_name_idx), Some(SqlToken::Word(index_name)) if is_identifier_word_token(index_name))
                {
                    break;
                }

                idx = skip_comment_tokens(tokens, index_name_idx + 1);
                continue;
            }
            "NOT" => {
                let indexed_idx = skip_comment_tokens(tokens, idx + 1);
                if !matches!(tokens.get(indexed_idx), Some(SqlToken::Word(next)) if next.eq_ignore_ascii_case("INDEXED"))
                {
                    break;
                }

                idx = skip_comment_tokens(tokens, indexed_idx + 1);
                continue;
            }
            "PARTITION" | "SUBPARTITION" | "SAMPLE" | "SEED" | "TABLESAMPLE" | "WITH" => {
                if upper == "WITH"
                    && matches!(
                        next_word_upper(tokens, idx + 1),
                        Some((next, _)) if next == "ORDINALITY"
                    )
                {
                    idx = skip_comment_tokens(tokens, idx + 1);
                    idx = skip_comment_tokens(tokens, idx + 1);
                    continue;
                }
                if upper == "WITH"
                    && matches!(
                        next_word_upper(tokens, idx + 1),
                        Some((next, _)) if next == "OFFSET"
                    )
                {
                    idx = skip_comment_tokens(tokens, idx + 1);
                    idx = skip_comment_tokens(tokens, idx + 1);

                    if matches!(
                        tokens.get(idx),
                        Some(SqlToken::Word(word)) if word.eq_ignore_ascii_case("AS")
                    ) {
                        idx = skip_comment_tokens(tokens, idx + 1);
                    }

                    if matches!(tokens.get(idx), Some(SqlToken::Word(word)) if is_identifier_word_token(word))
                    {
                        idx = skip_comment_tokens(tokens, idx + 1);
                    }

                    continue;
                }
                let mut open_idx = skip_comment_tokens(tokens, idx + 1);
                if matches!(upper.as_str(), "PARTITION" | "SUBPARTITION")
                    && matches!(tokens.get(open_idx), Some(SqlToken::Word(next)) if next.eq_ignore_ascii_case("FOR"))
                {
                    open_idx = skip_comment_tokens(tokens, open_idx + 1);
                }
                if upper == "TABLESAMPLE" {
                    if matches!(tokens.get(open_idx), Some(SqlToken::Word(_))) {
                        open_idx = skip_comment_tokens(tokens, open_idx + 1);
                    }
                } else if upper == "SAMPLE"
                    && matches!(tokens.get(open_idx), Some(SqlToken::Word(next)) if next.eq_ignore_ascii_case("BLOCK"))
                {
                    open_idx = skip_comment_tokens(tokens, open_idx + 1);
                } else if upper == "SAMPLE"
                    && matches!(
                        tokens.get(open_idx),
                        Some(SqlToken::Word(next))
                            if next.eq_ignore_ascii_case("BERNOULLI")
                                || next.eq_ignore_ascii_case("SYSTEM")
                    )
                {
                    open_idx = skip_comment_tokens(tokens, open_idx + 1);
                }
                if matches!(tokens.get(open_idx), Some(SqlToken::Symbol(sym)) if sym == "(") {
                    idx = skip_parenthesized_clause(tokens, open_idx);
                    if upper == "TABLESAMPLE" {
                        loop {
                            let option = next_word_upper(tokens, idx).map(|(word, _)| word);
                            let Some(option) = option else {
                                break;
                            };

                            if option != "REPEATABLE" && option != "SEED" {
                                break;
                            }

                            let option_idx = skip_comment_tokens(tokens, idx + 1);
                            if matches!(tokens.get(option_idx), Some(SqlToken::Symbol(sym)) if sym == "(")
                            {
                                idx = skip_parenthesized_clause(tokens, option_idx);
                                continue;
                            }

                            break;
                        }
                    }
                    idx = skip_comment_tokens(tokens, idx);
                    continue;
                }
                break;
            }
            "FOR" => {
                if let Some(next_idx) = skip_relation_temporal_clause(tokens, idx) {
                    idx = skip_comment_tokens(tokens, next_idx);
                    continue;
                }
                break;
            }
            "VERSIONS" => {
                let mut between_idx = skip_comment_tokens(tokens, idx + 1);
                if matches!(tokens.get(between_idx), Some(SqlToken::Word(next)) if next.eq_ignore_ascii_case("PERIOD"))
                {
                    let for_idx = skip_comment_tokens(tokens, between_idx + 1);
                    if !matches!(tokens.get(for_idx), Some(SqlToken::Word(next)) if next.eq_ignore_ascii_case("FOR"))
                    {
                        break;
                    }
                    between_idx = skip_comment_tokens(tokens, for_idx + 1);
                    if !matches!(tokens.get(between_idx), Some(SqlToken::Word(period_name)) if is_identifier_word_token(period_name))
                    {
                        break;
                    }
                    between_idx = skip_comment_tokens(tokens, between_idx + 1);
                }

                if !matches!(tokens.get(between_idx), Some(SqlToken::Word(next)) if next.eq_ignore_ascii_case("BETWEEN"))
                {
                    break;
                }

                let and_idx = find_top_level_keyword(tokens, between_idx + 1, "AND");
                let Some(and_idx) = and_idx else {
                    break;
                };

                idx = skip_flashback_bound_expression(tokens, and_idx + 1);
                idx = skip_comment_tokens(tokens, idx);
                continue;
            }
            "AS" => {
                let of_idx = skip_comment_tokens(tokens, idx + 1);
                if !matches!(tokens.get(of_idx), Some(SqlToken::Word(next)) if next.eq_ignore_ascii_case("OF"))
                {
                    break;
                }

                let mut cursor = skip_comment_tokens(tokens, of_idx + 1);
                if matches!(tokens.get(cursor), Some(SqlToken::Word(keyword)) if keyword.eq_ignore_ascii_case("PERIOD"))
                {
                    let for_idx = skip_comment_tokens(tokens, cursor + 1);
                    if !matches!(tokens.get(for_idx), Some(SqlToken::Word(next)) if next.eq_ignore_ascii_case("FOR"))
                    {
                        break;
                    }
                    let period_name_idx = skip_comment_tokens(tokens, for_idx + 1);
                    if !matches!(tokens.get(period_name_idx), Some(SqlToken::Word(period_name)) if is_identifier_word_token(period_name))
                    {
                        break;
                    }
                    cursor = skip_comment_tokens(tokens, period_name_idx + 1);
                }

                idx = skip_flashback_bound_expression(tokens, cursor);
                idx = skip_comment_tokens(tokens, idx);
                continue;
            }
            _ => break,
        }
    }

    idx
}

fn find_top_level_keyword(
    tokens: &[SqlToken],
    start: usize,
    target_keyword: &str,
) -> Option<usize> {
    let mut idx = start;
    let mut paren_depth = 0usize;

    while idx < tokens.len() {
        match &tokens[idx] {
            SqlToken::Symbol(sym) if sym == "(" => {
                paren_depth = paren_depth.saturating_add(1);
            }
            SqlToken::Symbol(sym) if sym == ")" => {
                paren_depth = paren_depth.saturating_sub(1);
            }
            SqlToken::Word(word)
                if paren_depth == 0 && word.eq_ignore_ascii_case(target_keyword) =>
            {
                return Some(idx);
            }
            _ => {}
        }
        idx += 1;
    }

    None
}

fn skip_relation_temporal_clause(tokens: &[SqlToken], start: usize) -> Option<usize> {
    let mut idx = skip_comment_tokens(tokens, start);
    if !matches!(tokens.get(idx), Some(SqlToken::Word(word)) if word.eq_ignore_ascii_case("FOR")) {
        return None;
    }

    idx = skip_comment_tokens(tokens, idx + 1);
    if !matches!(tokens.get(idx), Some(SqlToken::Word(word)) if word.eq_ignore_ascii_case("SYSTEM_TIME") || word.eq_ignore_ascii_case("APPLICATION_TIME"))
    {
        return None;
    }

    idx = skip_comment_tokens(tokens, idx + 1);
    let keyword = match tokens.get(idx) {
        Some(SqlToken::Word(word)) => word.to_ascii_uppercase(),
        _ => return None,
    };

    match keyword.as_str() {
        "AS" => {
            idx = skip_comment_tokens(tokens, idx + 1);
            if !matches!(tokens.get(idx), Some(SqlToken::Word(word)) if word.eq_ignore_ascii_case("OF"))
            {
                return None;
            }
            Some(skip_flashback_bound_expression(tokens, idx + 1))
        }
        "BETWEEN" => {
            let and_idx = find_top_level_keyword(tokens, idx + 1, "AND")?;
            Some(skip_flashback_bound_expression(tokens, and_idx + 1))
        }
        "FROM" => {
            let to_idx = find_top_level_keyword(tokens, idx + 1, "TO")?;
            Some(skip_flashback_bound_expression(tokens, to_idx + 1))
        }
        "CONTAINED" => {
            idx = skip_comment_tokens(tokens, idx + 1);
            if !matches!(tokens.get(idx), Some(SqlToken::Word(word)) if word.eq_ignore_ascii_case("IN"))
            {
                return None;
            }
            Some(skip_flashback_bound_expression(tokens, idx + 1))
        }
        "ALL" => Some(skip_comment_tokens(tokens, idx + 1)),
        _ => None,
    }
}

fn skip_flashback_bound_expression(tokens: &[SqlToken], start: usize) -> usize {
    let mut idx = skip_comment_tokens(tokens, start);

    if matches!(tokens.get(idx), Some(SqlToken::Word(word)) if word.eq_ignore_ascii_case("SCN") || word.eq_ignore_ascii_case("TIMESTAMP"))
    {
        idx = skip_comment_tokens(tokens, idx + 1);
    }

    // Parenthesized bound expressions are unambiguous and can be consumed in one step.
    if matches!(tokens.get(idx), Some(SqlToken::Symbol(sym)) if sym == "(") {
        return skip_parenthesized_clause(tokens, idx);
    }

    // Consume a simple leading operand.
    idx = consume_flashback_operand(tokens, idx);

    // Also consume optional arithmetic/interval suffixes often used in Oracle
    // flashback clauses, e.g.:
    //   AS OF TIMESTAMP SYSTIMESTAMP - INTERVAL '1' HOUR
    //   AS OF SCN 100 * 2
    //   VERSIONS BETWEEN SCN 1 * 2 AND SCN 3 * 4
    loop {
        let operator_idx = skip_comment_tokens(tokens, idx);
        let Some(SqlToken::Symbol(op)) = tokens.get(operator_idx) else {
            break;
        };
        if op != "+" && op != "-" && op != "*" && op != "/" && op != "||" {
            break;
        }

        let rhs_idx = skip_comment_tokens(tokens, operator_idx + 1);
        let Some(rhs_token) = tokens.get(rhs_idx) else {
            idx = rhs_idx;
            break;
        };

        let consumed_rhs = match rhs_token {
            SqlToken::Word(word) if word.eq_ignore_ascii_case("INTERVAL") => {
                consume_interval_literal(tokens, rhs_idx)
            }
            _ => consume_flashback_operand(tokens, rhs_idx),
        };

        if consumed_rhs == rhs_idx {
            idx = rhs_idx;
            break;
        }
        idx = consumed_rhs;
    }

    idx
}

fn consume_flashback_operand(tokens: &[SqlToken], start: usize) -> usize {
    let idx = skip_comment_tokens(tokens, start);
    match tokens.get(idx) {
        Some(SqlToken::Symbol(sym)) if sym == "(" => skip_parenthesized_clause(tokens, idx),
        Some(SqlToken::Word(_)) => {
            let next_idx = skip_comment_tokens(tokens, idx + 1);
            if matches!(tokens.get(next_idx), Some(SqlToken::Symbol(sym)) if sym == "(") {
                skip_parenthesized_clause(tokens, next_idx)
            } else {
                next_idx
            }
        }
        Some(SqlToken::String(_)) => skip_comment_tokens(tokens, idx + 1),
        Some(SqlToken::Symbol(sym)) if sym == ":" => {
            // Bind variable form `:b1`
            let bind_name_idx = skip_comment_tokens(tokens, idx + 1);
            if matches!(tokens.get(bind_name_idx), Some(SqlToken::Word(_))) {
                skip_comment_tokens(tokens, bind_name_idx + 1)
            } else {
                bind_name_idx
            }
        }
        Some(_) => idx.saturating_add(1),
        None => idx,
    }
}

fn consume_interval_literal(tokens: &[SqlToken], interval_idx: usize) -> usize {
    let mut idx = skip_comment_tokens(tokens, interval_idx + 1);

    if matches!(tokens.get(idx), Some(SqlToken::String(_))) {
        idx = skip_comment_tokens(tokens, idx + 1);
    }

    if matches!(tokens.get(idx), Some(SqlToken::Symbol(sym)) if sym == "(") {
        idx = skip_parenthesized_clause(tokens, idx);
    }

    if matches!(tokens.get(idx), Some(SqlToken::Word(_))) {
        idx = skip_comment_tokens(tokens, idx + 1);
        if matches!(tokens.get(idx), Some(SqlToken::Symbol(sym)) if sym == "(") {
            idx = skip_parenthesized_clause(tokens, idx);
        }
    }

    let maybe_to_idx = skip_comment_tokens(tokens, idx);
    if matches!(tokens.get(maybe_to_idx), Some(SqlToken::Word(word)) if word.eq_ignore_ascii_case("TO"))
    {
        idx = skip_comment_tokens(tokens, maybe_to_idx + 1);
        if matches!(tokens.get(idx), Some(SqlToken::Word(_))) {
            idx = skip_comment_tokens(tokens, idx + 1);
        }
    }

    idx
}

fn skip_parenthesized_clause(tokens: &[SqlToken], open_paren_idx: usize) -> usize {
    extract_parenthesized_range(tokens, open_paren_idx)
        .map(|(_, next_idx)| next_idx)
        .unwrap_or(open_paren_idx.saturating_add(1))
}

fn skip_comment_tokens(tokens: &[SqlToken], mut idx: usize) -> usize {
    while idx < tokens.len() {
        if let SqlToken::Comment(_) = &tokens[idx] {
            idx += 1;
            continue;
        }
        break;
    }
    idx
}

fn skip_derived_relation_postfix_clauses(tokens: &[SqlToken], start: usize) -> usize {
    let mut idx = start;

    loop {
        idx = skip_comment_tokens(tokens, idx);
        let Some(SqlToken::Word(word)) = tokens.get(idx) else {
            break;
        };

        let upper = word.to_ascii_uppercase();
        if upper == "MODEL" {
            idx = skip_model_clause(tokens, idx + 1);
            continue;
        }

        let clause_open_idx = match upper.as_str() {
            "PIVOT" => {
                let mut open_idx = skip_comment_tokens(tokens, idx + 1);
                if matches!(tokens.get(open_idx), Some(SqlToken::Word(next)) if next.eq_ignore_ascii_case("XML"))
                {
                    open_idx = skip_comment_tokens(tokens, open_idx + 1);
                }
                open_idx
            }
            "UNPIVOT" => {
                let mut open_idx = skip_comment_tokens(tokens, idx + 1);
                if matches!(tokens.get(open_idx), Some(SqlToken::Word(next)) if next.eq_ignore_ascii_case("INCLUDE") || next.eq_ignore_ascii_case("EXCLUDE"))
                {
                    open_idx = skip_comment_tokens(tokens, open_idx + 1);
                    if matches!(tokens.get(open_idx), Some(SqlToken::Word(next)) if next.eq_ignore_ascii_case("NULLS"))
                    {
                        open_idx = skip_comment_tokens(tokens, open_idx + 1);
                    }
                }
                open_idx
            }
            "MATCH_RECOGNIZE" => skip_comment_tokens(tokens, idx + 1),
            "MATCH" => {
                let recognize_idx = skip_comment_tokens(tokens, idx + 1);
                if !matches!(tokens.get(recognize_idx), Some(SqlToken::Word(next)) if next.eq_ignore_ascii_case("RECOGNIZE"))
                {
                    break;
                }
                skip_comment_tokens(tokens, recognize_idx + 1)
            }
            _ => break,
        };

        if !matches!(tokens.get(clause_open_idx), Some(SqlToken::Symbol(sym)) if sym == "(") {
            break;
        }

        idx = extract_parenthesized_range(tokens, clause_open_idx)
            .map(|(_, next_idx)| next_idx)
            .unwrap_or(clause_open_idx.saturating_add(1));
    }

    idx
}

fn skip_model_clause(tokens: &[SqlToken], start: usize) -> usize {
    let mut idx = skip_comment_tokens(tokens, start);
    let mut saw_rules = false;
    let mut expect_rules_option_paren = false;

    while idx < tokens.len() {
        idx = skip_comment_tokens(tokens, idx);
        let Some(token) = tokens.get(idx) else {
            break;
        };

        match token {
            SqlToken::Word(word) => {
                let upper = word.to_ascii_uppercase();
                if !saw_rules {
                    if upper == "RULES" {
                        saw_rules = true;
                        idx += 1;
                        continue;
                    }
                    // Malformed MODEL clause recovery: stop when another relation
                    // boundary starts before RULES appears.
                    if is_join_keyword(&upper) || is_table_stop_keyword(&upper) {
                        break;
                    }
                    idx += 1;
                    continue;
                }

                if matches!(upper.as_str(), "ITERATE" | "UNTIL") {
                    expect_rules_option_paren = true;
                }
                idx += 1;
            }
            SqlToken::Symbol(sym) if sym == "(" => {
                let next_idx = extract_parenthesized_range(tokens, idx)
                    .map(|(_, next_idx)| next_idx)
                    .unwrap_or(idx.saturating_add(1));
                if saw_rules && !expect_rules_option_paren {
                    // First non-option parenthesized block after RULES is the
                    // model rules body; alias, if any, starts right after it.
                    return next_idx;
                }
                expect_rules_option_paren = false;
                idx = next_idx;
            }
            SqlToken::Symbol(sym) if sym == "," || sym == ")" => break,
            _ => {
                idx += 1;
            }
        }
    }

    idx
}

/// Parse an alias after a subquery closing ')'.
fn parse_subquery_alias(tokens: &[SqlToken], start: usize) -> Option<(String, usize)> {
    let mut idx = start;
    // Skip comments and stray closing parens to recover from malformed SQL like:
    // `FROM (SELECT ...) ) alias`
    while idx < tokens.len() {
        match &tokens[idx] {
            SqlToken::Comment(_) => {
                idx += 1;
                continue;
            }
            SqlToken::Symbol(sym) if sym == ")" => {
                idx += 1;
                continue;
            }
            _ => {}
        }
        break;
    }

    idx = skip_relation_postfix_clauses(tokens, idx);
    idx = skip_derived_relation_postfix_clauses(tokens, idx);
    let (alias, next_idx) = parse_relation_alias_at(tokens, idx, true);
    alias.map(|name| (name, next_idx))
}

fn is_join_keyword(word: &str) -> bool {
    matches!(
        word,
        "JOIN"
            | "INNER"
            | "LEFT"
            | "RIGHT"
            | "FULL"
            | "CROSS"
            | "OUTER"
            | "NATURAL"
            | "SEMI"
            | "ANTI"
            | "ASOF"
            | "HASH"
            | "LOOP"
            | "MERGE"
            | "LATERAL"
            | "APPLY"
            | "STRAIGHT_JOIN"
    )
}

fn is_table_stop_keyword(word: &str) -> bool {
    matches!(
        word,
        "WHERE"
            | "GROUP"
            | "ORDER"
            | "HAVING"
            | "CONNECT"
            | "START"
            | "WITH"
            | "UNION"
            | "INTERSECT"
            | "EXCEPT"
            | "MINUS"
            | "FETCH"
            | "FOR"
            | "WINDOW"
            | "QUALIFY"
            | "LIMIT"
            | "OFFSET"
            | "RETURNING"
            | "VALUES"
            | "SET"
            | "ON"
            | "STRAIGHT_JOIN"
            | "PIVOT"
            | "UNPIVOT"
            | "MODEL"
            | "MATCH_RECOGNIZE"
            | "MATCH"
            | "RECOGNIZE"
            | "USING"
            | "WHEN"
            | "SAMPLE"
            | "TABLESAMPLE"
            | "PARTITION"
            | "SUBPARTITION"
            | "VERSIONS"
            | "DROP"
            | "REUSE"
            | "CASCADE"
            | "RESTRICT"
            | "PURGE"
            | "STORAGE"
            | "MATERIALIZED"
            | "TO"
    )
}

/// Keywords that must not be interpreted as relation aliases.
///
/// This is intentionally broader than `is_table_stop_keyword` and also includes
/// join modifiers such as `LATERAL` so table/subquery alias parsing follows the
/// same boundary rules.
fn is_relation_alias_breaker(word: &str) -> bool {
    is_join_keyword(word)
        || is_table_stop_keyword(word)
        || matches!(word, "SEARCH" | "CYCLE")
        || matches!(word, "ON" | "SELECT" | "FROM" | "INTO")
}

/// Collect top-level tables visible within a standalone statement.
/// This avoids full cursor-phase analysis when only table scope is needed.
pub fn collect_tables_in_statement(tokens: &[SqlToken]) -> Vec<ScopedTableRef> {
    collect_tables_deep(tokens, &[0], tokens.len()).tables
}

/// Resolve which tables are relevant for a given qualifier (alias or table name).
pub fn resolve_qualifier_tables(
    qualifier: &str,
    tables_in_scope: &[ScopedTableRef],
) -> Vec<String> {
    fn update_match_if_deeper(
        slot: &mut Option<(usize, String)>,
        candidate_depth: usize,
        candidate_name: &str,
    ) {
        if slot
            .as_ref()
            .is_none_or(|(depth, _)| candidate_depth >= *depth)
        {
            *slot = Some((candidate_depth, candidate_name.to_string()));
        }
    }

    fn push_first_unique(
        seen: &mut HashSet<String>,
        candidate: Option<(usize, String)>,
    ) -> Option<Vec<String>> {
        if let Some((_, name)) = candidate {
            let normalized = name.to_ascii_uppercase();
            if seen.insert(normalized) {
                return Some(vec![name]);
            }
        }
        None
    }

    let qualifier_upper = normalize_identifier_for_lookup(qualifier);
    let mut alias_match: Option<(usize, String)> = None;
    let mut name_match: Option<(usize, String)> = None;
    let mut short_name_match: Option<(usize, String)> = None;
    let mut seen = HashSet::new();

    for table_ref in tables_in_scope {
        let name_upper = normalize_identifier_for_lookup(&table_ref.name);
        let alias_upper = table_ref
            .alias
            .as_ref()
            .map(|a| normalize_identifier_for_lookup(a));

        if alias_upper.as_deref() == Some(qualifier_upper.as_str()) {
            update_match_if_deeper(&mut alias_match, table_ref.depth, &table_ref.name);
            continue;
        }

        if name_upper == qualifier_upper {
            update_match_if_deeper(&mut name_match, table_ref.depth, &table_ref.name);
            continue;
        }

        if last_identifier_part_for_lookup(&table_ref.name)
            .is_some_and(|short| short.eq_ignore_ascii_case(&qualifier_upper))
        {
            update_match_if_deeper(&mut short_name_match, table_ref.depth, &table_ref.name);
        }
    }

    if let Some(result) = push_first_unique(&mut seen, alias_match) {
        return result;
    }

    if let Some(result) = push_first_unique(&mut seen, name_match) {
        return result;
    }

    if let Some(result) = push_first_unique(&mut seen, short_name_match) {
        return result;
    }

    // If no match found, try the qualifier as a direct table name
    let normalized = strip_identifier_quotes(qualifier);
    if seen.insert(normalized.to_ascii_uppercase()) {
        return vec![normalized];
    }

    Vec::new()
}

/// Resolve all table names from scope (for unqualified column suggestions).
pub fn resolve_all_scope_tables(tables_in_scope: &[ScopedTableRef]) -> Vec<String> {
    let mut result = Vec::new();
    let mut seen = HashSet::new();

    for table_ref in tables_in_scope {
        let upper = table_ref.name.to_ascii_uppercase();
        if seen.insert(upper) {
            result.push(table_ref.name.clone());
        }
    }

    result
}

/// Extract projected column names from a SELECT statement's token stream.
/// Returns column names/aliases in the order they appear in the SELECT list.
/// Items that cannot be resolved (e.g., `*`, expressions without aliases) are omitted.
pub fn extract_select_list_columns(tokens: &[SqlToken]) -> Vec<String> {
    let mut columns = Vec::new();
    let select_list_tokens = extract_select_list_tokens(tokens);
    for item_tokens in split_top_level_symbol_groups(select_list_tokens, ",") {
        if let Some(col) = resolve_item_column_name(&item_tokens) {
            columns.push(col);
        }
    }

    columns
}

/// Resolve source table names referenced by wildcard items (`*`, `t.*`) in a
/// SELECT list. Returned names are deduplicated in appearance order.
pub fn extract_select_list_wildcard_tables(
    tokens: &[SqlToken],
    tables_in_scope: &[ScopedTableRef],
) -> Vec<String> {
    let mut tables = Vec::new();
    let mut seen = HashSet::new();
    let select_list_tokens = extract_select_list_tokens(tokens);
    for item_tokens in split_top_level_symbol_groups(select_list_tokens, ",") {
        append_wildcard_item_tables(&item_tokens, tables_in_scope, &mut tables, &mut seen);
    }

    tables
}

/// Extract column names from table-function `COLUMNS` clauses such as
/// `XMLTABLE(... COLUMNS col1 NUMBER PATH '...', col2 VARCHAR2(30) PATH '...')`.
/// Returns discovered column names in appearance order.
pub fn extract_table_function_columns(tokens: &[SqlToken]) -> Vec<String> {
    let token_depths = paren_depths(tokens);
    for (idx, token) in tokens.iter().enumerate() {
        if !is_top_level_depth(&token_depths, idx) {
            continue;
        }
        if let SqlToken::Word(word) = token {
            // If this body is a normal subquery (SELECT ...), let SELECT-list
            // extraction handle it instead of mixing in function-internal tokens.
            if word.eq_ignore_ascii_case("SELECT") {
                return Vec::new();
            }
        }
    }

    let mut columns = Vec::new();
    let mut seen = HashSet::new();
    collect_table_function_columns(tokens, &mut columns, &mut seen);
    columns
}

/// Extract qualifiers from incomplete select-list items like `alias.`.
pub fn extract_select_list_leading_qualifiers(tokens: &[SqlToken]) -> Vec<String> {
    let mut qualifiers = Vec::new();
    let mut seen = HashSet::new();
    let select_list_tokens = extract_select_list_tokens(tokens);

    for item_tokens in split_top_level_symbol_groups(select_list_tokens, ",") {
        if let Some(qualifier) = extract_incomplete_qualified_item_prefix(&item_tokens) {
            let key = qualifier.to_ascii_uppercase();
            if seen.insert(key) {
                qualifiers.push(qualifier);
            }
        }
    }

    qualifiers
}

#[derive(Debug, Default)]
struct PivotClauseColumns {
    clause_index: usize,
    for_columns: Vec<String>,
    aggregate_columns: Vec<String>,
    generated_columns: Vec<String>,
}

#[derive(Debug, Default)]
struct UnpivotClauseColumns {
    clause_index: usize,
    source_columns: Vec<String>,
    measure_columns: Vec<String>,
    for_columns: Vec<String>,
}

#[derive(Debug, Default)]
struct ModelClauseColumns {
    measure_columns: Vec<String>,
}

/// Extract Oracle PIVOT/UNPIVOT-projected columns from a query token stream.
/// This is primarily used when the SELECT list contains `*` and normal
/// select-list extraction cannot determine output columns.
pub fn extract_oracle_pivot_unpivot_projection_columns(tokens: &[SqlToken]) -> Vec<String> {
    let pivot = parse_top_level_pivot_clause(tokens);
    let unpivot = parse_top_level_unpivot_clause(tokens);
    if pivot.is_none() && unpivot.is_none() {
        return Vec::new();
    }

    let first_clause_idx = match (pivot.as_ref(), unpivot.as_ref()) {
        (Some(p), Some(u)) => Some(p.clause_index.min(u.clause_index)),
        (Some(p), None) => Some(p.clause_index),
        (None, Some(u)) => Some(u.clause_index),
        (None, None) => None,
    };

    let mut columns = if let Some(clause_idx) = first_clause_idx {
        infer_source_columns_before_clause(tokens, clause_idx)
    } else {
        Vec::new()
    };

    if let Some(pivot_info) = pivot {
        remove_columns_case_insensitive(&mut columns, &pivot_info.for_columns);
        remove_columns_case_insensitive(&mut columns, &pivot_info.aggregate_columns);
        columns.extend(pivot_info.generated_columns);
    }

    if let Some(unpivot_info) = unpivot {
        remove_columns_case_insensitive(&mut columns, &unpivot_info.source_columns);
        columns.extend(unpivot_info.measure_columns);
        columns.extend(unpivot_info.for_columns);
    }

    dedup_columns_case_insensitive(&mut columns);
    columns
}

/// Extract Oracle UNPIVOT-introduced columns (measure + FOR target).
pub fn extract_oracle_unpivot_generated_columns(tokens: &[SqlToken]) -> Vec<String> {
    let Some(unpivot_info) = parse_top_level_unpivot_clause(tokens) else {
        return Vec::new();
    };

    let mut columns = unpivot_info.measure_columns;
    columns.extend(unpivot_info.for_columns);
    dedup_columns_case_insensitive(&mut columns);
    columns
}

/// Extract Oracle MODEL-introduced measure columns from `MEASURES (...)`.
pub fn extract_oracle_model_generated_columns(tokens: &[SqlToken]) -> Vec<String> {
    let Some(model_info) = parse_top_level_model_clause(tokens) else {
        return Vec::new();
    };
    model_info.measure_columns
}

/// Extract MATCH_RECOGNIZE-generated columns from a query token stream.
/// This includes MEASURES aliases and PATTERN/SUBSET variables.
pub fn extract_match_recognize_generated_columns(tokens: &[SqlToken]) -> Vec<String> {
    let mut columns = extract_match_recognize_measure_columns(tokens);
    columns.extend(extract_match_recognize_pattern_variables(tokens));
    dedup_columns_case_insensitive(&mut columns);
    columns
}

fn extract_match_recognize_measure_columns(tokens: &[SqlToken]) -> Vec<String> {
    let Some(clause_tokens) = extract_match_recognize_clause_tokens(tokens) else {
        return Vec::new();
    };
    let token_depths = paren_depths(clause_tokens);

    let mut measures_idx = None;
    for (idx, token) in clause_tokens.iter().enumerate() {
        if !is_top_level_depth(&token_depths, idx) {
            continue;
        }
        if let SqlToken::Word(word) = token {
            if word.eq_ignore_ascii_case("MEASURES") {
                measures_idx = Some(idx);
                break;
            }
        }
    }

    let Some(measures_idx) = measures_idx else {
        return Vec::new();
    };

    let measures_start = next_non_comment_index(clause_tokens, measures_idx.saturating_add(1));
    if measures_start >= clause_tokens.len() {
        return Vec::new();
    }

    let mut measures_end = clause_tokens.len();
    for idx in measures_start..clause_tokens.len() {
        if !is_top_level_depth(&token_depths, idx) {
            continue;
        }
        if let SqlToken::Word(word) = &clause_tokens[idx] {
            let upper = word.to_ascii_uppercase();
            if is_match_recognize_clause_boundary_keyword(&upper) {
                measures_end = idx;
                break;
            }
        }
    }

    let mut columns = Vec::new();
    for item_tokens in
        split_top_level_symbol_groups(&clause_tokens[measures_start..measures_end], ",")
    {
        if let Some(column) = parse_model_measure_output_column(&item_tokens) {
            columns.push(column);
        }
    }

    dedup_columns_case_insensitive(&mut columns);
    columns
}

/// Extract MATCH_RECOGNIZE pattern variables from `PATTERN (...)` and
/// subset variables from `SUBSET ...`.
/// Example: `PATTERN (a b+) SUBSET u = (a, b)` -> `["a", "b", "u"]`.
pub fn extract_match_recognize_pattern_variables(tokens: &[SqlToken]) -> Vec<String> {
    let Some(clause_tokens) = extract_match_recognize_clause_tokens(tokens) else {
        return Vec::new();
    };
    let token_depths = paren_depths(clause_tokens);

    let mut pattern_idx = None;
    for (idx, token) in clause_tokens.iter().enumerate() {
        if !is_top_level_depth(&token_depths, idx) {
            continue;
        }
        if let SqlToken::Word(word) = token {
            if word.eq_ignore_ascii_case("PATTERN") {
                pattern_idx = Some(idx);
                break;
            }
        }
    }

    let Some(pattern_idx) = pattern_idx else {
        return Vec::new();
    };
    let pattern_open_idx = next_non_comment_index(clause_tokens, pattern_idx.saturating_add(1));
    let Some(SqlToken::Symbol(sym)) = clause_tokens.get(pattern_open_idx) else {
        return Vec::new();
    };
    if sym != "(" {
        return Vec::new();
    }

    let Some((pattern_range, _)) = extract_parenthesized_range(clause_tokens, pattern_open_idx)
    else {
        return Vec::new();
    };

    let mut variables = Vec::new();
    let pattern_tokens = token_range_slice(clause_tokens, pattern_range);
    for token in pattern_tokens {
        if let SqlToken::Word(word) = token {
            if !is_identifier_word_token(word) {
                continue;
            }
            let upper = word.to_ascii_uppercase();
            if is_match_recognize_pattern_keyword(&upper) {
                continue;
            }
            variables.push(strip_identifier_quotes(word));
        }
    }

    if let Some(subset_idx) = clause_tokens.iter().enumerate().find_map(|(idx, token)| {
        if !is_top_level_depth(&token_depths, idx) {
            return None;
        }

        match token {
            SqlToken::Word(word) if word.eq_ignore_ascii_case("SUBSET") => Some(idx),
            _ => None,
        }
    }) {
        let mut idx = next_non_comment_index(clause_tokens, subset_idx.saturating_add(1));
        while idx < clause_tokens.len() {
            if let Some(SqlToken::Word(word)) = clause_tokens.get(idx) {
                let upper = word.to_ascii_uppercase();
                if is_match_recognize_clause_boundary_keyword(&upper) {
                    break;
                }

                let assign_idx = next_non_comment_index(clause_tokens, idx.saturating_add(1));
                if is_identifier_word_token(word)
                    && matches!(clause_tokens.get(assign_idx), Some(SqlToken::Symbol(sym)) if sym == "=")
                {
                    variables.push(strip_identifier_quotes(word));

                    let rhs_idx = next_non_comment_index(clause_tokens, assign_idx + 1);
                    idx = if matches!(clause_tokens.get(rhs_idx), Some(SqlToken::Symbol(sym)) if sym == "(")
                    {
                        extract_parenthesized_range(clause_tokens, rhs_idx)
                            .map(|(_, next_idx)| next_idx)
                            .unwrap_or(rhs_idx.saturating_add(1))
                    } else {
                        rhs_idx.saturating_add(1)
                    };
                } else {
                    idx = idx.saturating_add(1);
                }
            } else {
                idx = idx.saturating_add(1);
            }

            idx = next_non_comment_index(clause_tokens, idx);
            if matches!(clause_tokens.get(idx), Some(SqlToken::Symbol(sym)) if sym == ",") {
                idx = next_non_comment_index(clause_tokens, idx + 1);
            }
        }
    }

    dedup_columns_case_insensitive(&mut variables);
    variables
}

fn extract_match_recognize_clause_tokens(tokens: &[SqlToken]) -> Option<&[SqlToken]> {
    let match_idx = find_top_level_word_index(tokens, "MATCH_RECOGNIZE")
        .or_else(|| find_top_level_keyword_pair_index(tokens, "MATCH", "RECOGNIZE"))?;

    let mut clause_start_idx = match_idx.saturating_add(1);
    if let Some((next_keyword, next_idx)) = next_word_upper(tokens, clause_start_idx) {
        if next_keyword == "RECOGNIZE" {
            clause_start_idx = next_idx.saturating_add(1);
        }
    }

    let clause_open_idx = next_non_comment_index(tokens, clause_start_idx);
    let SqlToken::Symbol(sym) = tokens.get(clause_open_idx)? else {
        return None;
    };
    if sym != "(" {
        return None;
    }

    let (clause_range, _) = extract_parenthesized_range(tokens, clause_open_idx)?;
    Some(token_range_slice(tokens, clause_range))
}

fn is_match_recognize_clause_boundary_keyword(word: &str) -> bool {
    matches!(word, "MEASURES" | "PATTERN" | "DEFINE" | "AFTER")
}

fn find_top_level_keyword_pair_index(
    tokens: &[SqlToken],
    first: &str,
    second: &str,
) -> Option<usize> {
    let mut paren_state = ParenDepthState::default();

    for (idx, token) in tokens.iter().enumerate() {
        apply_paren_token(&mut paren_state, token);
        if paren_state.depth() != 0 {
            continue;
        }
        let SqlToken::Word(word) = token else {
            continue;
        };
        if !word.eq_ignore_ascii_case(first) {
            continue;
        }
        let Some((next_word, _)) = next_word_upper(tokens, idx + 1) else {
            continue;
        };
        if next_word.eq_ignore_ascii_case(second) {
            return Some(idx);
        }
    }

    None
}

fn infer_source_columns_before_clause(tokens: &[SqlToken], clause_idx: usize) -> Vec<String> {
    let analysis = collect_tables_deep(tokens, &[0], tokens.len());
    let mut selected_subquery: Option<&SubqueryDefinition> = None;

    for subq in &analysis.subqueries {
        if subq.depth != 0 || subq.body_range.end > clause_idx {
            continue;
        }
        if selected_subquery
            .as_ref()
            .is_none_or(|existing| subq.body_range.end > existing.body_range.end)
        {
            selected_subquery = Some(subq);
        }
    }

    if let Some(subq) = selected_subquery {
        let body_tokens = token_range_slice(tokens, subq.body_range);
        let mut columns = extract_select_list_columns(body_tokens);
        if columns.is_empty() {
            columns = extract_table_function_columns(body_tokens);
        }
        if columns.is_empty() {
            columns = extract_oracle_pivot_unpivot_projection_columns(body_tokens);
        }
        if columns.is_empty() {
            columns = extract_oracle_model_generated_columns(body_tokens);
        }
        if columns.is_empty() {
            columns = extract_match_recognize_generated_columns(body_tokens);
        }
        dedup_columns_case_insensitive(&mut columns);
        return columns;
    }

    let mut columns = extract_select_list_columns(tokens);
    if columns.is_empty() {
        columns = extract_table_function_columns(tokens);
    }
    if columns.is_empty() {
        columns = extract_oracle_pivot_unpivot_projection_columns(tokens);
    }
    if columns.is_empty() {
        columns = extract_oracle_model_generated_columns(tokens);
    }
    if columns.is_empty() {
        columns = extract_match_recognize_generated_columns(tokens);
    }
    dedup_columns_case_insensitive(&mut columns);
    columns
}

fn parse_top_level_pivot_clause(tokens: &[SqlToken]) -> Option<PivotClauseColumns> {
    let pivot_idx = find_top_level_word_index(tokens, "PIVOT")?;
    let mut idx = next_non_comment_index(tokens, pivot_idx.saturating_add(1));
    let mut pivot_mode = PivotMode::Regular;

    if let Some(SqlToken::Word(word)) = tokens.get(idx) {
        if word.eq_ignore_ascii_case("XML") {
            pivot_mode = PivotMode::Xml;
            idx = next_non_comment_index(tokens, idx.saturating_add(1));
        }
    }

    let open_idx = match tokens.get(idx) {
        Some(SqlToken::Symbol(sym)) if sym == "(" => idx,
        _ => return None,
    };

    let (range, _) = extract_parenthesized_range(tokens, open_idx)?;
    let clause_tokens = token_range_slice(tokens, range);
    let (for_idx, in_idx) = find_clause_for_in_indices(clause_tokens)?;

    let aggregate_columns = parse_pivot_aggregate_columns(&clause_tokens[..for_idx]);
    let for_columns = parse_identifier_segment(&clause_tokens[for_idx + 1..in_idx]);
    let generated_columns = if pivot_mode.should_skip_generated_columns() {
        Vec::new()
    } else {
        parse_pivot_generated_columns_from_in_segment(&clause_tokens[in_idx + 1..])
    };

    let mut result = PivotClauseColumns {
        clause_index: pivot_idx,
        for_columns,
        aggregate_columns,
        generated_columns,
    };
    dedup_columns_case_insensitive(&mut result.for_columns);
    dedup_columns_case_insensitive(&mut result.aggregate_columns);
    dedup_columns_case_insensitive(&mut result.generated_columns);
    Some(result)
}

fn parse_top_level_unpivot_clause(tokens: &[SqlToken]) -> Option<UnpivotClauseColumns> {
    let unpivot_idx = find_top_level_word_index(tokens, "UNPIVOT")?;
    let mut idx = next_non_comment_index(tokens, unpivot_idx.saturating_add(1));

    if let Some(SqlToken::Word(word)) = tokens.get(idx) {
        if word.eq_ignore_ascii_case("INCLUDE") || word.eq_ignore_ascii_case("EXCLUDE") {
            idx = next_non_comment_index(tokens, idx.saturating_add(1));
            if let Some(SqlToken::Word(nulls)) = tokens.get(idx) {
                if nulls.eq_ignore_ascii_case("NULLS") {
                    idx = next_non_comment_index(tokens, idx.saturating_add(1));
                }
            }
        }
    }

    let open_idx = match tokens.get(idx) {
        Some(SqlToken::Symbol(sym)) if sym == "(" => idx,
        _ => return None,
    };

    let (range, _) = extract_parenthesized_range(tokens, open_idx)?;
    let clause_tokens = token_range_slice(tokens, range);
    let (for_idx, in_idx) = find_clause_for_in_indices(clause_tokens)?;

    let measure_columns = parse_unpivot_output_segment(&clause_tokens[..for_idx]);
    let for_columns = parse_unpivot_output_segment(&clause_tokens[for_idx + 1..in_idx]);
    let source_columns = parse_unpivot_source_columns_from_in_segment(&clause_tokens[in_idx + 1..]);

    let mut result = UnpivotClauseColumns {
        clause_index: unpivot_idx,
        source_columns,
        measure_columns,
        for_columns,
    };
    dedup_columns_case_insensitive(&mut result.source_columns);
    dedup_columns_case_insensitive(&mut result.measure_columns);
    dedup_columns_case_insensitive(&mut result.for_columns);
    Some(result)
}

fn parse_top_level_model_clause(tokens: &[SqlToken]) -> Option<ModelClauseColumns> {
    let model_idx = find_top_level_word_index(tokens, "MODEL")?;
    let token_depths = paren_depths(tokens);
    let mut idx = model_idx.saturating_add(1);

    while idx < tokens.len() {
        if !is_top_level_depth(&token_depths, idx) {
            idx += 1;
            continue;
        }
        if let SqlToken::Word(word) = &tokens[idx] {
            if !word.eq_ignore_ascii_case("MEASURES") {
                idx += 1;
                continue;
            }

            let open_idx = next_non_comment_index(tokens, idx.saturating_add(1));
            let Some(SqlToken::Symbol(sym)) = tokens.get(open_idx) else {
                return None;
            };
            if sym != "(" {
                return None;
            }

            let (measure_range, _) = extract_parenthesized_range(tokens, open_idx)?;
            let mut result = ModelClauseColumns {
                measure_columns: parse_model_measure_columns(token_range_slice(
                    tokens,
                    measure_range,
                )),
            };
            dedup_columns_case_insensitive(&mut result.measure_columns);
            return Some(result);
        }
        idx += 1;
    }

    None
}

fn find_top_level_word_index(tokens: &[SqlToken], keyword: &str) -> Option<usize> {
    let token_depths = paren_depths(tokens);
    let mut idx = 0usize;
    while idx < tokens.len() {
        if !is_top_level_depth(&token_depths, idx) {
            idx += 1;
            continue;
        }
        if let SqlToken::Word(word) = &tokens[idx] {
            if word.eq_ignore_ascii_case(keyword) {
                return Some(idx);
            }
        }
        idx += 1;
    }
    None
}

fn find_clause_for_in_indices(clause_tokens: &[SqlToken]) -> Option<(usize, usize)> {
    let token_depths = paren_depths(clause_tokens);
    let mut for_idx = None;
    let mut in_idx = None;
    let mut idx = 0usize;

    while idx < clause_tokens.len() {
        if !is_top_level_depth(&token_depths, idx) {
            idx += 1;
            continue;
        }
        if let SqlToken::Word(word) = &clause_tokens[idx] {
            if for_idx.is_none() && word.eq_ignore_ascii_case("FOR") {
                for_idx = Some(idx);
            } else if for_idx.is_some() && word.eq_ignore_ascii_case("IN") {
                in_idx = Some(idx);
                break;
            }
        }
        idx += 1;
    }

    match (for_idx, in_idx) {
        (Some(for_pos), Some(in_pos)) if for_pos < in_pos => Some((for_pos, in_pos)),
        _ => None,
    }
}

fn parse_pivot_aggregate_columns(tokens: &[SqlToken]) -> Vec<String> {
    let mut columns = Vec::new();
    let mut idx = 0usize;

    while idx < tokens.len() {
        let token = &tokens[idx];
        if let SqlToken::Word(func_name) = token {
            if !is_identifier_word_token(func_name) {
                idx += 1;
                continue;
            }

            let open_idx = next_non_comment_index(tokens, idx.saturating_add(1));
            let Some(SqlToken::Symbol(sym)) = tokens.get(open_idx) else {
                idx += 1;
                continue;
            };
            if sym != "(" {
                idx += 1;
                continue;
            }

            if let Some((args_range, next_idx)) = extract_parenthesized_range(tokens, open_idx) {
                let args_tokens = token_range_slice(tokens, args_range);
                for arg_item in split_top_level_symbol_groups(args_tokens, ",") {
                    if let Some(column) = parse_identifier_from_expression_tokens(&arg_item) {
                        columns.push(column);
                    }
                }
                idx = next_idx;
                continue;
            }
        }

        idx += 1;
    }

    dedup_columns_case_insensitive(&mut columns);
    columns
}

fn parse_identifier_from_expression_tokens(tokens: &[&SqlToken]) -> Option<String> {
    let meaningful: Vec<&SqlToken> = tokens
        .iter()
        .copied()
        .filter(|token| !matches!(token, SqlToken::Comment(_)))
        .collect();
    if meaningful.is_empty() {
        return None;
    }

    for token in meaningful.iter().rev().copied() {
        if let SqlToken::Word(word) = token {
            if !is_identifier_word_token(word) {
                continue;
            }
            let upper = word.to_ascii_uppercase();
            if is_expression_keyword(&upper) {
                continue;
            }
            return Some(strip_identifier_quotes(word));
        }
    }

    None
}

fn collect_table_function_columns(
    tokens: &[SqlToken],
    columns: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    let mut idx = 0usize;
    while idx < tokens.len() {
        let Some(SqlToken::Word(word)) = tokens.get(idx) else {
            idx += 1;
            continue;
        };
        if !word.eq_ignore_ascii_case("COLUMNS") {
            idx += 1;
            continue;
        }

        let next_idx = next_non_comment_index(tokens, idx.saturating_add(1));
        if matches!(tokens.get(next_idx), Some(SqlToken::Symbol(sym)) if sym == "(") {
            if let Some((range, after_paren)) = extract_parenthesized_range(tokens, next_idx) {
                let range_tokens = token_range_slice(tokens, range);
                append_table_function_column_items(range_tokens, columns, seen);
                // Recurse to capture nested `... COLUMNS (...)` clauses.
                collect_table_function_columns(range_tokens, columns, seen);
                idx = after_paren;
                continue;
            }
            idx += 1;
            continue;
        }

        if next_idx < tokens.len() {
            let tail = &tokens[next_idx..];
            append_table_function_column_items(tail, columns, seen);
            collect_table_function_columns(tail, columns, seen);
        }
        break;
    }
}

fn append_table_function_column_items(
    tokens: &[SqlToken],
    columns: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    for item_tokens in split_top_level_symbol_groups(tokens, ",") {
        if let Some(column) = resolve_table_function_column_name(&item_tokens) {
            let key = column.to_ascii_uppercase();
            if seen.insert(key) {
                columns.push(column);
            }
        }
    }
}

fn extract_incomplete_qualified_item_prefix(item_tokens: &[&SqlToken]) -> Option<String> {
    let meaningful: Vec<&SqlToken> = item_tokens
        .iter()
        .copied()
        .filter(|t| !matches!(t, SqlToken::Comment(_)))
        .collect();
    if meaningful.len() < 2 {
        return None;
    }

    let qualifier = match meaningful.first().copied() {
        Some(SqlToken::Word(word)) if is_identifier_word_token(word) => {
            strip_identifier_quotes(word)
        }
        _ => return None,
    };
    match meaningful.get(1) {
        Some(SqlToken::Symbol(dot)) if dot == "." => {}
        _ => return None,
    }

    // `qualifier.column` is a complete reference and should not be treated as
    // an incomplete prefix for fallback inference.
    if let Some(SqlToken::Word(word)) = meaningful.get(2).copied() {
        if is_identifier_word_token(word) {
            return None;
        }
    }

    Some(qualifier)
}

fn is_expression_keyword(word: &str) -> bool {
    matches!(
        word,
        "AS" | "DISTINCT"
            | "CASE"
            | "WHEN"
            | "THEN"
            | "ELSE"
            | "END"
            | "NULL"
            | "AND"
            | "OR"
            | "NOT"
            | "IN"
            | "IS"
            | "LIKE"
            | "BETWEEN"
            | "OVER"
            | "PARTITION"
            | "ORDER"
            | "BY"
            | "ROWS"
            | "RANGE"
            | "CURRENT"
            | "ROW"
            | "UNBOUNDED"
            | "PRECEDING"
            | "FOLLOWING"
    )
}

fn is_match_recognize_pattern_keyword(word: &str) -> bool {
    matches!(word, "PERMUTE" | "SUBSET")
}

fn parse_identifier_segment(tokens: &[SqlToken]) -> Vec<String> {
    let mut columns = Vec::new();
    let mut meaningful_start = None;
    for (idx, token) in tokens.iter().enumerate() {
        if !matches!(token, SqlToken::Comment(_)) {
            meaningful_start = Some(idx);
            break;
        }
    }
    let Some(start_idx) = meaningful_start else {
        return columns;
    };

    if matches!(tokens.get(start_idx), Some(SqlToken::Symbol(sym)) if sym == "(") {
        if let Some((range, _)) = extract_parenthesized_range(tokens, start_idx) {
            columns = parse_identifier_words_top_level(token_range_slice(tokens, range));
            dedup_columns_case_insensitive(&mut columns);
            return columns;
        }
    }

    if let Some(name) = parse_first_identifier_word(tokens) {
        columns.push(name);
    }
    columns
}

fn parse_first_identifier_word(tokens: &[SqlToken]) -> Option<String> {
    for token in tokens {
        if let SqlToken::Word(word) = token {
            if is_identifier_word_token(word) {
                return Some(strip_identifier_quotes(word));
            }
        }
    }
    None
}

fn parse_identifier_words_top_level(tokens: &[SqlToken]) -> Vec<String> {
    let token_depths = paren_depths(tokens);
    let mut columns = Vec::new();

    for (idx, token) in tokens.iter().enumerate() {
        if !is_top_level_depth(&token_depths, idx) {
            continue;
        }
        if let SqlToken::Word(word) = token {
            if !is_identifier_word_token(word) {
                continue;
            }
            let upper = word.to_ascii_uppercase();
            if upper == "AS" {
                continue;
            }
            columns.push(strip_identifier_quotes(word));
        }
    }

    dedup_columns_case_insensitive(&mut columns);
    columns
}

fn parse_pivot_generated_columns_from_in_segment(tokens: &[SqlToken]) -> Vec<String> {
    let open_idx = next_non_comment_index(tokens, 0);
    let Some(SqlToken::Symbol(sym)) = tokens.get(open_idx) else {
        return Vec::new();
    };
    if sym != "(" {
        return Vec::new();
    }

    let Some((range, _)) = extract_parenthesized_range(tokens, open_idx) else {
        return Vec::new();
    };
    let in_list_tokens = token_range_slice(tokens, range);
    let mut columns = Vec::new();

    for item_tokens in split_top_level_symbol_groups(in_list_tokens, ",") {
        if let Some(column) = parse_pivot_in_item_output_column(&item_tokens) {
            columns.push(column);
        }
    }

    dedup_columns_case_insensitive(&mut columns);
    columns
}

fn parse_pivot_in_item_output_column(item_tokens: &[&SqlToken]) -> Option<String> {
    let meaningful: Vec<&SqlToken> = item_tokens
        .iter()
        .copied()
        .filter(|token| !matches!(token, SqlToken::Comment(_)))
        .collect();
    if meaningful.is_empty() {
        return None;
    }

    let mut idx = 0usize;
    while idx < meaningful.len() {
        if let SqlToken::Word(word) = meaningful[idx] {
            if word.eq_ignore_ascii_case("AS") {
                let mut alias_idx = idx.saturating_add(1);
                while alias_idx < meaningful.len() {
                    if let SqlToken::Word(alias) = meaningful[alias_idx] {
                        if is_identifier_word_token(alias) {
                            return Some(strip_identifier_quotes(alias));
                        }
                    }
                    if !matches!(meaningful[alias_idx], SqlToken::Comment(_)) {
                        break;
                    }
                    alias_idx += 1;
                }
                break;
            }
        }
        idx += 1;
    }

    if let Some(SqlToken::Word(last_word)) = meaningful.last().copied() {
        if is_identifier_word_token(last_word) {
            return Some(strip_identifier_quotes(last_word));
        }
    }

    if let Some(SqlToken::Word(first_word)) = meaningful.first().copied() {
        if is_identifier_word_token(first_word) {
            return Some(strip_identifier_quotes(first_word));
        }
    }

    None
}

fn parse_unpivot_output_segment(tokens: &[SqlToken]) -> Vec<String> {
    let start_idx = next_non_comment_index(tokens, 0);
    if start_idx >= tokens.len() {
        return Vec::new();
    }

    if matches!(tokens.get(start_idx), Some(SqlToken::Symbol(sym)) if sym == "(") {
        if let Some((range, _)) = extract_parenthesized_range(tokens, start_idx) {
            let mut columns = parse_identifier_words_top_level(token_range_slice(tokens, range));
            dedup_columns_case_insensitive(&mut columns);
            return columns;
        }
    }

    if let Some(column) = parse_first_identifier_word(&tokens[start_idx..]) {
        return vec![column];
    }

    Vec::new()
}

fn parse_unpivot_source_columns_from_in_segment(tokens: &[SqlToken]) -> Vec<String> {
    let open_idx = next_non_comment_index(tokens, 0);
    let Some(SqlToken::Symbol(sym)) = tokens.get(open_idx) else {
        return Vec::new();
    };
    if sym != "(" {
        return Vec::new();
    }

    let Some((range, _)) = extract_parenthesized_range(tokens, open_idx) else {
        return Vec::new();
    };
    let list_tokens = token_range_slice(tokens, range);
    let mut columns = Vec::new();

    for item_tokens in split_top_level_symbol_groups(list_tokens, ",") {
        columns.extend(parse_unpivot_in_item_source_columns(&item_tokens));
    }

    dedup_columns_case_insensitive(&mut columns);
    columns
}

fn parse_unpivot_in_item_source_columns(item_tokens: &[&SqlToken]) -> Vec<String> {
    let meaningful: Vec<&SqlToken> = item_tokens
        .iter()
        .copied()
        .filter(|token| !matches!(token, SqlToken::Comment(_)))
        .collect();
    if meaningful.is_empty() {
        return Vec::new();
    }

    let starts_with_tuple =
        matches!(meaningful.first().copied(), Some(SqlToken::Symbol(sym)) if sym == "(");
    let target_depth = if starts_with_tuple { 1usize } else { 0usize };
    let mut depth = 0usize;
    let mut columns = Vec::new();

    for token in meaningful {
        match token {
            SqlToken::Symbol(sym) if sym == "(" => {
                depth = depth.saturating_add(1);
            }
            SqlToken::Symbol(sym) if sym == ")" => {
                depth = depth.saturating_sub(1);
            }
            SqlToken::Word(word) => {
                if depth == 0 && word.eq_ignore_ascii_case("AS") {
                    break;
                }
                if depth == target_depth && is_identifier_word_token(word) {
                    columns.push(strip_identifier_quotes(word));
                }
            }
            _ => {}
        }
    }

    dedup_columns_case_insensitive(&mut columns);
    columns
}

fn parse_model_measure_columns(tokens: &[SqlToken]) -> Vec<String> {
    let mut columns = Vec::new();
    for item_tokens in split_top_level_symbol_groups(tokens, ",") {
        if let Some(column) = parse_model_measure_output_column(&item_tokens) {
            columns.push(column);
        }
    }
    dedup_columns_case_insensitive(&mut columns);
    columns
}

fn parse_model_measure_output_column(item_tokens: &[&SqlToken]) -> Option<String> {
    let meaningful: Vec<&SqlToken> = item_tokens
        .iter()
        .copied()
        .filter(|token| !matches!(token, SqlToken::Comment(_)))
        .collect();
    if meaningful.is_empty() {
        return None;
    }

    let mut depth = 0usize;
    let mut idx = 0usize;
    while idx < meaningful.len() {
        match meaningful[idx] {
            SqlToken::Symbol(sym) if sym == "(" => {
                depth = depth.saturating_add(1);
            }
            SqlToken::Symbol(sym) if sym == ")" => {
                depth = depth.saturating_sub(1);
            }
            SqlToken::Word(word) if depth == 0 && word.eq_ignore_ascii_case("AS") => {
                if let Some(SqlToken::Word(alias)) = meaningful.get(idx.saturating_add(1)).copied()
                {
                    if is_identifier_word_token(alias) {
                        return Some(strip_identifier_quotes(alias));
                    }
                }
                return None;
            }
            _ => {}
        }
        idx += 1;
    }

    parse_simple_identifier_path_output_column(&meaningful)
}

fn parse_simple_identifier_path_output_column(tokens: &[&SqlToken]) -> Option<String> {
    if tokens.is_empty() {
        return None;
    }

    let mut parser_state = IdentifierPathState::ExpectIdentifier;
    let mut last_identifier = None;
    for token in tokens {
        match token {
            SqlToken::Word(word)
                if parser_state.expects_identifier() && is_identifier_word_token(word) =>
            {
                last_identifier = Some(strip_identifier_quotes(word));
                parser_state = IdentifierPathState::ExpectDot;
            }
            SqlToken::Symbol(sym) if parser_state.expects_dot() && sym == "." => {
                parser_state = IdentifierPathState::ExpectIdentifier;
            }
            _ => return None,
        }
    }

    if parser_state.expects_identifier() {
        return None;
    }
    last_identifier
}

fn next_non_comment_index(tokens: &[SqlToken], start: usize) -> usize {
    let mut idx = start.min(tokens.len());
    while idx < tokens.len() {
        if !matches!(tokens[idx], SqlToken::Comment(_)) {
            break;
        }
        idx += 1;
    }
    idx
}

fn prev_non_comment_index(tokens: &[SqlToken], before_idx: usize) -> usize {
    let mut idx = before_idx;
    while idx > 0 {
        idx -= 1;
        if !matches!(tokens[idx], SqlToken::Comment(_)) {
            return idx;
        }
    }
    before_idx
}

fn dedup_columns_case_insensitive(columns: &mut Vec<String>) {
    let mut seen = HashSet::new();
    columns.retain(|column| seen.insert(column.to_ascii_uppercase()));
}

fn remove_columns_case_insensitive(columns: &mut Vec<String>, remove: &[String]) {
    if columns.is_empty() || remove.is_empty() {
        return;
    }
    let remove_set: HashSet<String> = remove
        .iter()
        .map(|name| name.to_ascii_uppercase())
        .collect();
    columns.retain(|column| !remove_set.contains(&column.to_ascii_uppercase()));
}

fn resolve_table_function_column_name(item_tokens: &[&SqlToken]) -> Option<String> {
    let first_word = item_tokens.iter().copied().find_map(|token| match token {
        SqlToken::Comment(_) => None,
        SqlToken::Word(word) => Some(word.as_str()),
        _ => None,
    })?;

    let upper = first_word.to_ascii_uppercase();
    if is_table_function_item_leading_keyword(&upper) {
        return None;
    }
    if !is_identifier_word_token(first_word) {
        return None;
    }

    Some(strip_identifier_quotes(first_word))
}

fn is_table_function_item_leading_keyword(word: &str) -> bool {
    sql_text::is_table_function_item_leading_keyword(word)
}

fn extract_select_list_tokens(tokens: &[SqlToken]) -> &[SqlToken] {
    let start = select_list_start_index(tokens);
    let end = select_list_end_index(tokens, start);
    &tokens[start..end]
}

fn select_list_start_index(tokens: &[SqlToken]) -> usize {
    let mut idx = 0usize;

    // Find SELECT keyword.
    while idx < tokens.len() {
        match &tokens[idx] {
            SqlToken::Word(w) if w.eq_ignore_ascii_case("SELECT") => {
                idx += 1;
                break;
            }
            SqlToken::Comment(_) => {
                idx += 1;
            }
            _ => {
                idx += 1;
            }
        }
    }

    // Skip DISTINCT / ALL / UNIQUE.
    while idx < tokens.len() {
        match &tokens[idx] {
            SqlToken::Word(w) => {
                let upper = w.to_ascii_uppercase();
                if matches!(upper.as_str(), "DISTINCT" | "ALL" | "UNIQUE") {
                    idx += 1;
                } else {
                    break;
                }
            }
            SqlToken::Comment(_) => {
                idx += 1;
            }
            _ => break,
        }
    }

    idx
}

fn select_list_end_index(tokens: &[SqlToken], start: usize) -> usize {
    let token_depths = paren_depths(tokens);
    let mut idx = start;

    while idx < tokens.len() {
        let token = &tokens[idx];
        if is_top_level_depth(&token_depths, idx) {
            if let SqlToken::Word(w) = token {
                let upper = w.to_ascii_uppercase();
                if matches!(upper.as_str(), "FROM" | "INTO" | "BULK") {
                    break;
                }
            }
        }

        idx += 1;
    }

    idx
}

fn append_wildcard_item_tables(
    item_tokens: &[&SqlToken],
    tables_in_scope: &[ScopedTableRef],
    tables: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    let meaningful: Vec<&SqlToken> = item_tokens
        .iter()
        .copied()
        .filter(|t| !matches!(t, SqlToken::Comment(_)))
        .collect();

    if meaningful.is_empty() {
        return;
    }

    // Unqualified wildcard: `*`
    if meaningful.len() == 1 {
        if let SqlToken::Symbol(s) = meaningful[0] {
            if s == "*" {
                for table in resolve_all_scope_tables(tables_in_scope) {
                    let key = table.to_ascii_uppercase();
                    if seen.insert(key) {
                        tables.push(table);
                    }
                }
                return;
            }
        }
    }

    // Qualified wildcard: `alias.*` or dotted qualifiers like `schema.table.*`.
    if meaningful.len() >= 3 {
        let last = meaningful[meaningful.len() - 1];
        let dot = meaningful[meaningful.len() - 2];
        if let (SqlToken::Symbol(star), SqlToken::Symbol(dot_sym)) = (last, dot) {
            if star == "*" && dot_sym == "." {
                if let Some(normalized) =
                    normalize_dotted_identifier_tokens(&meaningful[..meaningful.len() - 2])
                {
                    for table in resolve_qualifier_tables(&normalized, tables_in_scope) {
                        let key = table.to_ascii_uppercase();
                        if seen.insert(key) {
                            tables.push(table);
                        }
                    }
                }
            }
        }
    }
}

fn normalize_dotted_identifier_tokens(tokens: &[&SqlToken]) -> Option<String> {
    if tokens.is_empty() {
        return None;
    }

    let mut parts = Vec::new();
    let mut parser_state = IdentifierPathState::ExpectIdentifier;
    for token in tokens {
        if parser_state.expects_identifier() {
            if let SqlToken::Word(word) = token {
                let segment = strip_identifier_quotes(word);
                if segment.is_empty() {
                    return None;
                }
                parts.push(segment);
                parser_state = IdentifierPathState::ExpectDot;
            } else {
                return None;
            }
        } else if let SqlToken::Symbol(sym) = token {
            if sym == "." {
                parser_state = IdentifierPathState::ExpectIdentifier;
            } else {
                return None;
            }
        } else {
            return None;
        }
    }

    if parser_state.expects_identifier() || parts.is_empty() {
        None
    } else {
        Some(parts.join("."))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PivotMode {
    Regular,
    Xml,
}

impl PivotMode {
    fn should_skip_generated_columns(self) -> bool {
        matches!(self, Self::Xml)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IdentifierPathState {
    ExpectIdentifier,
    ExpectDot,
}

impl IdentifierPathState {
    fn expects_identifier(self) -> bool {
        matches!(self, Self::ExpectIdentifier)
    }

    fn expects_dot(self) -> bool {
        matches!(self, Self::ExpectDot)
    }
}

/// Given the tokens of a single SELECT item, determine the output column name.
fn resolve_item_column_name(item_tokens: &[&SqlToken]) -> Option<String> {
    let meaningful: Vec<&SqlToken> = item_tokens
        .iter()
        .copied()
        .filter(|t| !matches!(t, SqlToken::Comment(_)))
        .collect();

    if meaningful.is_empty() {
        return None;
    }

    // Check for lone `*`
    if meaningful.len() == 1 {
        if let SqlToken::Symbol(s) = meaningful[0] {
            if s == "*" {
                return None;
            }
        }
    }

    // Check for `qualifier.*` pattern
    if meaningful.len() >= 2 {
        if let SqlToken::Symbol(s) = meaningful[meaningful.len() - 1] {
            if s == "*" {
                return None;
            }
        }
    }

    let last = meaningful.last()?;
    let second_last = if meaningful.len() >= 2 {
        Some(meaningful[meaningful.len() - 2])
    } else {
        None
    };

    // Case 1: Explicit alias `... AS alias_name`
    if let SqlToken::Word(alias) = last {
        if !is_identifier_word_token(alias) {
            return None;
        }
        if let Some(SqlToken::Word(kw)) = second_last {
            if kw.eq_ignore_ascii_case("AS") {
                return Some(strip_identifier_quotes(alias));
            }
        }
    }

    // Case 2: Implicit alias — last token is a Word following `)` or another Word
    if let SqlToken::Word(alias) = last {
        let alias_upper = alias.to_ascii_uppercase();
        if !is_select_item_trailing_keyword(&alias_upper) {
            if let Some(prev) = second_last {
                let is_implicit = match prev {
                    SqlToken::Symbol(s) if s == ")" => true,
                    SqlToken::Word(_) => {
                        // Two consecutive words: the second is an implicit alias
                        // unless the first is AS (already handled above)
                        meaningful.len() > 1
                    }
                    SqlToken::Symbol(s) if s == "." => false, // qualifier.column, not alias
                    _ => false,
                };
                if is_implicit {
                    return Some(strip_identifier_quotes(alias));
                }
            }
        }
    }

    // Case 3: Simple column reference (single word)
    if meaningful.len() == 1 {
        if let SqlToken::Word(name) = meaningful[0] {
            if !is_identifier_word_token(name) {
                return None;
            }
            return Some(strip_identifier_quotes(name));
        }
    }

    // Case 4: Qualified column `qualifier.column`
    if meaningful.len() == 3 {
        if let (SqlToken::Word(_), SqlToken::Symbol(dot), SqlToken::Word(col)) =
            (meaningful[0], meaningful[1], meaningful[2])
        {
            if dot == "." {
                if !is_identifier_word_token(col) {
                    return None;
                }
                return Some(strip_identifier_quotes(col));
            }
        }
    }

    // Expression without alias — cannot determine column name
    None
}

fn is_select_item_trailing_keyword(word: &str) -> bool {
    matches!(
        word,
        "FROM"
            | "WHERE"
            | "GROUP"
            | "ORDER"
            | "HAVING"
            | "INTO"
            | "UNION"
            | "INTERSECT"
            | "EXCEPT"
            | "MINUS"
            | "FETCH"
            | "FOR"
            | "LIMIT"
            | "OFFSET"
            | "CONNECT"
            | "START"
            | "BULK"
    )
}

#[cfg(test)]
mod wildcard_resolution_tests {
    use super::*;
    use crate::ui::sql_editor::SqlEditorWidget;

    #[test]
    fn dotted_qualified_wildcard_prefers_full_table_name_over_alias_match() {
        let sql =
            "SELECT schema_a.emp.* FROM schema_a.emp e JOIN dept emp ON e.deptno = emp.deptno";
        let tokens = SqlEditorWidget::tokenize_sql(sql);
        let ctx = analyze_cursor_context(&tokens, tokens.len());

        let wildcard_tables = extract_select_list_wildcard_tables(&tokens, &ctx.tables_in_scope);

        assert_eq!(wildcard_tables, vec!["schema_a.emp".to_string()]);
    }
}

#[cfg(test)]
mod tests;
