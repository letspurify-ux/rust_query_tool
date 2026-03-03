use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
                | SqlPhase::ConnectByClause
                | SqlPhase::StartWithClause
                | SqlPhase::MatchRecognizeClause
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
    None,
    ExpectName,
    AfterName,
    ExpectAs,
    ExpectBody,
    InBody,
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
    let phase_analysis = analyze_phase(&statement_tokens[..clamped_cursor_token_len]);
    let table_analysis = collect_tables_deep(
        statement_tokens.as_ref(),
        &phase_analysis.visible_scope_chain,
        clamped_cursor_token_len,
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
        phase: phase_analysis.phase,
        depth: phase_analysis.depth,
        tables_in_scope,
        ctes,
        subqueries: table_analysis.subqueries,
        qualifier: None,
        qualifier_tables: Vec::new(),
    }
}

struct PhaseAnalysis {
    phase: SqlPhase,
    depth: usize,
    visible_scope_chain: Vec<usize>,
}

/// Returns true for functions whose syntax includes a FROM keyword as part of
/// the function call rather than a SQL clause (e.g. `EXTRACT(YEAR FROM ...)`,
/// `TRIM(LEADING '0' FROM ...)`, `SUBSTRING(col FROM ...)`).
fn is_from_consuming_function(name: &str) -> bool {
    matches!(
        name,
        "EXTRACT" | "TRIM" | "XMLCAST" | "SUBSTRING" | "OVERLAY"
    )
}

/// FROM-clause table functions that may reference left-side row source aliases.
fn is_from_lateral_table_function(name: &str) -> bool {
    matches!(name, "JSON_TABLE" | "XMLTABLE")
}

/// Walk tokens up to cursor to determine the current SQL phase and depth.
fn analyze_phase(tokens: &[SqlToken]) -> PhaseAnalysis {
    let mut depth: usize = 0;
    // Track phase at each depth level
    let mut phase_stack: Vec<SqlPhase> = vec![SqlPhase::Initial];
    // Track the function name before '(' at each depth level, used to
    // distinguish function-internal FROM (EXTRACT, TRIM) from SQL FROM clauses.
    let mut paren_func_stack: Vec<Option<String>> = vec![None];
    let mut last_word: Option<String> = None;
    let mut next_scope_id = 1usize;
    let mut scope_stack = vec![0usize];
    let mut visible_parent: HashMap<usize, Option<usize>> = HashMap::new();
    visible_parent.insert(0, None);
    let mut pending_lateral_subquery = false;
    let mut cte_state = CteState::None;
    let mut cte_paren_depth: usize = 0;
    let mut idx = 0;

    while idx < tokens.len() {
        let token = &tokens[idx];

        match token {
            SqlToken::Symbol(sym) if sym == "(" => {
                let parent_phase = phase_stack.get(depth).copied().unwrap_or(SqlPhase::Initial);
                let parent_scope_id = *scope_stack.last().unwrap_or(&0);
                depth += 1;
                let inherited_phase = if parent_phase.is_column_context()
                    || matches!(
                        parent_phase,
                        SqlPhase::ValuesClause | SqlPhase::IntoClause | SqlPhase::PivotClause
                    ) {
                    parent_phase
                } else {
                    SqlPhase::Initial
                };
                if phase_stack.len() <= depth {
                    phase_stack.push(inherited_phase);
                } else {
                    phase_stack[depth] = inherited_phase;
                }
                // Record the function name that preceded this '(' so we can
                // distinguish function-internal FROM from SQL FROM clauses.
                let func_name = last_word.take().map(|w| w.to_ascii_uppercase());
                if paren_func_stack.len() <= depth {
                    paren_func_stack.push(func_name);
                } else {
                    paren_func_stack[depth] = func_name;
                }
                let scope_id = next_scope_id;
                next_scope_id += 1;
                scope_stack.push(scope_id);
                // Derived table/subquery in FROM introduces an isolated scope.
                let is_from_lateral_function = paren_func_stack
                    .get(depth)
                    .and_then(|name| name.as_deref())
                    .is_some_and(is_from_lateral_table_function);
                let inherited_visible_parent = if matches!(parent_phase, SqlPhase::FromClause)
                    && !pending_lateral_subquery
                    && !is_from_lateral_function
                {
                    None
                } else {
                    Some(parent_scope_id)
                };
                visible_parent.insert(scope_id, inherited_visible_parent);
                pending_lateral_subquery = false;
                if matches!(cte_state, CteState::ExpectBody) {
                    cte_state = CteState::InBody;
                    cte_paren_depth = depth;
                }
                if matches!(cte_state, CteState::AfterName) {
                    // CTE explicit columns: WITH cte(col1, col2) AS (...)
                    // Skip until matching ')'
                    cte_state = CteState::ExpectAs;
                }
                idx += 1;
                continue;
            }
            SqlToken::Symbol(sym) if sym == ")" => {
                if matches!(cte_state, CteState::InBody) && depth == cte_paren_depth {
                    cte_state = CteState::None;
                }
                depth = depth.saturating_sub(1);
                if scope_stack.len() > 1 {
                    scope_stack.pop();
                }
                pending_lateral_subquery = false;
                last_word = None;
                idx += 1;
                continue;
            }
            SqlToken::Comment(_) | SqlToken::String(_) => {
                idx += 1;
                continue;
            }
            SqlToken::Word(word) => {
                let upper = word.to_ascii_uppercase();

                // CTE state machine
                match cte_state {
                    CteState::ExpectName if upper != "RECURSIVE" => {
                        cte_state = CteState::AfterName;
                        idx += 1;
                        continue;
                    }
                    CteState::AfterName => {
                        if upper == "AS" {
                            cte_state = CteState::ExpectBody;
                        }
                        idx += 1;
                        continue;
                    }
                    CteState::ExpectAs => {
                        if upper == "AS" {
                            cte_state = CteState::ExpectBody;
                        }
                        idx += 1;
                        continue;
                    }
                    CteState::InBody => {
                        // Inside CTE body, process normally for phase tracking at this depth
                        // but don't break out of CTE state
                    }
                    CteState::None => {}
                    _ => {
                        idx += 1;
                        continue;
                    }
                }

                // Ensure phase_stack has entry for current depth
                while phase_stack.len() <= depth {
                    phase_stack.push(SqlPhase::Initial);
                }

                let current_phase = phase_stack[depth];

                if (upper == "LATERAL" || upper == "APPLY")
                    && matches!(current_phase, SqlPhase::FromClause)
                {
                    pending_lateral_subquery = true;
                    idx += 1;
                    continue;
                }
                pending_lateral_subquery = false;

                match upper.as_str() {
                    "WITH" if matches!(current_phase, SqlPhase::Initial) => {
                        phase_stack[depth] = SqlPhase::WithClause;
                        cte_state = CteState::ExpectName;
                    }
                    "SELECT" => {
                        phase_stack[depth] = SqlPhase::SelectList;
                    }
                    "FROM" => {
                        // Only suppress FROM transition when the enclosing '('
                        // belongs to a function that uses FROM as part of its
                        // syntax (EXTRACT, TRIM, XMLCAST). All other cases —
                        // including incomplete SQL with unclosed parens — treat
                        // FROM as a real SQL clause.
                        let is_func_from = depth > 0
                            && paren_func_stack
                                .get(depth)
                                .and_then(|name| name.as_deref())
                                .is_some_and(is_from_consuming_function);
                        if !is_func_from {
                            phase_stack[depth] = SqlPhase::FromClause;
                        }
                    }
                    "INTO" => {
                        if matches!(
                            current_phase,
                            SqlPhase::SelectList | SqlPhase::Initial | SqlPhase::MergeTarget
                        ) {
                            phase_stack[depth] = SqlPhase::IntoClause;
                        }
                    }
                    "USING" => {
                        if matches!(
                            current_phase,
                            SqlPhase::MergeTarget | SqlPhase::IntoClause | SqlPhase::FromClause
                        ) {
                            phase_stack[depth] = SqlPhase::FromClause;
                        }
                    }
                    "JOIN" | "APPLY" => {
                        // JOIN resets to FROM context for next table
                        phase_stack[depth] = SqlPhase::FromClause;
                    }
                    "ON" => {
                        if matches!(current_phase, SqlPhase::FromClause) {
                            phase_stack[depth] = SqlPhase::JoinCondition;
                        }
                    }
                    "WHERE" => {
                        phase_stack[depth] = SqlPhase::WhereClause;
                    }
                    "GROUP" => {
                        if peek_word_upper(tokens, idx + 1) == Some("BY") {
                            phase_stack[depth] = SqlPhase::GroupByClause;
                            idx += 1; // skip BY
                        }
                    }
                    "HAVING" => {
                        phase_stack[depth] = SqlPhase::HavingClause;
                    }
                    "ORDER" => {
                        if peek_word_upper(tokens, idx + 1) == Some("BY") {
                            phase_stack[depth] = SqlPhase::OrderByClause;
                            idx += 1; // skip BY
                        }
                    }
                    "SET" => {
                        phase_stack[depth] = SqlPhase::SetClause;
                    }
                    "UPDATE" => {
                        phase_stack[depth] = SqlPhase::UpdateTarget;
                    }
                    "DELETE" => {
                        phase_stack[depth] = SqlPhase::DeleteTarget;
                    }
                    "MERGE" => {
                        phase_stack[depth] = SqlPhase::MergeTarget;
                    }
                    "CONNECT" => {
                        if peek_word_upper(tokens, idx + 1) == Some("BY") {
                            phase_stack[depth] = SqlPhase::ConnectByClause;
                            idx += 1;
                        }
                    }
                    "START" => {
                        if peek_word_upper(tokens, idx + 1) == Some("WITH") {
                            phase_stack[depth] = SqlPhase::StartWithClause;
                            idx += 1;
                        }
                    }
                    "VALUES" => {
                        phase_stack[depth] = SqlPhase::ValuesClause;
                    }
                    "MATCH_RECOGNIZE" => {
                        phase_stack[depth] = SqlPhase::MatchRecognizeClause;
                    }
                    "PIVOT" | "UNPIVOT" => {
                        phase_stack[depth] = SqlPhase::PivotClause;
                    }
                    "MODEL" => {
                        phase_stack[depth] = SqlPhase::ModelClause;
                    }
                    // Set operations reset to Initial for next SELECT
                    "UNION" | "INTERSECT" | "EXCEPT" | "MINUS" => {
                        phase_stack[depth] = SqlPhase::Initial;
                    }
                    // After comma in WITH clause, expect next CTE name
                    _ => {
                        if matches!(cte_state, CteState::None)
                            && matches!(phase_stack.first(), Some(SqlPhase::WithClause))
                            && depth == 0
                        {
                            // We might be between CTE definitions
                        }
                    }
                }
                last_word = Some(upper);
            }
            SqlToken::Symbol(sym) if sym == "," => {
                pending_lateral_subquery = false;
                // After comma in WITH clause at depth 0, expect next CTE name
                if matches!(cte_state, CteState::None)
                    && depth == 0
                    && matches!(phase_stack.first(), Some(SqlPhase::WithClause))
                {
                    cte_state = CteState::ExpectName;
                }
            }
            SqlToken::Symbol(sym) if sym == ";" => {
                // Keep scope numbering aligned with collect_tables_deep so
                // visible_scope_chain matches table scope IDs across PL/SQL.
                let has_following_statement = tokens[idx + 1..]
                    .iter()
                    .any(|t| !matches!(t, SqlToken::Comment(_)));
                if !has_following_statement {
                    break;
                }

                depth = 0;
                phase_stack = vec![SqlPhase::Initial];
                paren_func_stack = vec![None];
                last_word = None;
                next_scope_id = 1;
                scope_stack = vec![0usize];
                visible_parent.clear();
                visible_parent.insert(0, None);
                pending_lateral_subquery = false;
                cte_state = CteState::None;
                cte_paren_depth = 0;
                idx += 1;
                continue;
            }
            _ => {
                pending_lateral_subquery = false;
            }
        }
        idx += 1;
    }

    let phase = phase_stack.get(depth).copied().unwrap_or(SqlPhase::Initial);
    let mut visible_scope_chain = Vec::new();
    let mut scope_id = *scope_stack.last().unwrap_or(&0);
    visible_scope_chain.push(scope_id);
    while let Some(Some(parent_id)) = visible_parent.get(&scope_id) {
        visible_scope_chain.push(*parent_id);
        scope_id = *parent_id;
    }
    visible_scope_chain.reverse();

    PhaseAnalysis {
        phase,
        depth,
        visible_scope_chain,
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
    struct ParsedTable {
        table: ScopedTableRef,
        scope_id: usize,
    }

    struct ParsedSubquery {
        subquery: SubqueryDefinition,
        scope_id: usize,
    }

    let mut all_tables: Vec<ParsedTable> = Vec::new();
    let mut all_subqueries: Vec<ParsedSubquery> = Vec::new();
    let mut depth: usize = 0;
    let mut phase_stack: Vec<SqlPhase> = vec![SqlPhase::Initial];
    let mut paren_func_stack: Vec<Option<String>> = vec![None];
    let mut last_word: Option<String> = None;
    let mut expect_table = false;
    let mut cte_state = CteState::None;
    let mut cte_paren_depth: usize = 0;
    let mut next_scope_id = 1usize;
    let mut scope_stack = vec![0usize];
    // Track subquery aliases: when we close a paren at a certain depth in FROM context,
    // store (depth, start_token_idx) so we can capture body tokens
    let mut subquery_tracks: Vec<(usize, usize)> = Vec::new(); // (depth, start_idx)
    let mut idx = 0;

    while idx < tokens.len() {
        let token = &tokens[idx];

        match token {
            SqlToken::Symbol(sym) if sym == "(" => {
                let parent_phase = phase_stack.get(depth).copied().unwrap_or(SqlPhase::Initial);
                depth += 1;
                while phase_stack.len() <= depth {
                    phase_stack.push(SqlPhase::Initial);
                }
                phase_stack[depth] = SqlPhase::Initial;
                let func_name = last_word.take().map(|w| w.to_ascii_uppercase());
                if paren_func_stack.len() <= depth {
                    paren_func_stack.push(func_name);
                } else {
                    paren_func_stack[depth] = func_name;
                }
                expect_table = false;
                scope_stack.push(next_scope_id);
                next_scope_id += 1;

                if matches!(parent_phase, SqlPhase::FromClause) {
                    subquery_tracks.push((depth, idx + 1)); // depth after increment, token after '('
                }
                if matches!(cte_state, CteState::ExpectBody) {
                    cte_state = CteState::InBody;
                    cte_paren_depth = depth;
                }
                idx += 1;
                continue;
            }
            SqlToken::Symbol(sym) if sym == ")" => {
                if matches!(cte_state, CteState::InBody) && depth == cte_paren_depth {
                    cte_state = CteState::None;
                }

                while subquery_tracks.last().is_some_and(|track| track.0 > depth) {
                    // Recover gracefully from malformed SQL with unbalanced parentheses.
                    // Stale entries can otherwise trigger panics when slicing token ranges.
                    subquery_tracks.pop();
                }

                let was_subquery = subquery_tracks.last().map(|t| t.0) == Some(depth);
                if let Some((_, start_idx)) = was_subquery.then(|| subquery_tracks.pop()).flatten()
                {
                    if start_idx > idx {
                        depth = depth.saturating_sub(1);
                        idx += 1;
                        continue;
                    }
                    // Look for alias after the closing paren
                    if let Some((alias, next_idx)) = parse_subquery_alias(tokens, idx + 1) {
                        let parent_scope_id = if scope_stack.len() >= 2 {
                            scope_stack[scope_stack.len() - 2]
                        } else {
                            0
                        };
                        // Capture body token range for column inference.
                        let body_range = TokenRange {
                            start: start_idx,
                            end: idx,
                        };
                        all_subqueries.push(ParsedSubquery {
                            subquery: SubqueryDefinition {
                                alias: alias.clone(),
                                body_range,
                                depth: depth.saturating_sub(1),
                            },
                            scope_id: parent_scope_id,
                        });
                        all_tables.push(ParsedTable {
                            table: ScopedTableRef {
                                name: alias.clone(),
                                alias: Some(alias),
                                depth: depth.saturating_sub(1),
                                is_cte: false,
                            },
                            scope_id: parent_scope_id,
                        });
                        idx = next_idx;
                        depth = depth.saturating_sub(1);
                        if scope_stack.len() > 1 {
                            scope_stack.pop();
                        }
                        continue;
                    }

                    // Subquery without alias: still expose projected columns for unqualified
                    // column completion (e.g. SELECT | FROM (SELECT ...)).
                    let parent_scope_id = if scope_stack.len() >= 2 {
                        scope_stack[scope_stack.len() - 2]
                    } else {
                        0
                    };
                    let generated_name = anonymous_subquery_name(start_idx, depth);
                    let body_range = TokenRange {
                        start: start_idx,
                        end: idx,
                    };
                    all_subqueries.push(ParsedSubquery {
                        subquery: SubqueryDefinition {
                            alias: generated_name.clone(),
                            body_range,
                            depth: depth.saturating_sub(1),
                        },
                        scope_id: parent_scope_id,
                    });
                    all_tables.push(ParsedTable {
                        table: ScopedTableRef {
                            name: generated_name,
                            alias: None,
                            depth: depth.saturating_sub(1),
                            is_cte: false,
                        },
                        scope_id: parent_scope_id,
                    });
                }

                depth = depth.saturating_sub(1);
                if scope_stack.len() > 1 {
                    scope_stack.pop();
                }
                last_word = None;
                idx += 1;
                continue;
            }
            SqlToken::Comment(_) | SqlToken::String(_) => {
                idx += 1;
                continue;
            }
            SqlToken::Symbol(sym) if sym == "," => {
                // After comma in FROM clause, expect another table
                let current_phase = phase_stack.get(depth).copied().unwrap_or(SqlPhase::Initial);
                if matches!(current_phase, SqlPhase::FromClause) {
                    expect_table = true;
                }
                // After comma in WITH clause, expect next CTE
                if matches!(cte_state, CteState::None)
                    && depth == 0
                    && matches!(phase_stack.first(), Some(SqlPhase::WithClause))
                {
                    cte_state = CteState::ExpectName;
                }
                idx += 1;
                continue;
            }
            SqlToken::Symbol(sym) if sym == ";" => {
                if idx >= cursor_token_len {
                    break;
                }
                // Statement boundary - reset only when another statement follows.
                // Keep collected state for trailing terminators in the final statement.
                let has_following_statement = tokens[idx + 1..]
                    .iter()
                    .any(|t| !matches!(t, SqlToken::Comment(_)));
                if !has_following_statement {
                    break;
                }

                all_tables.clear();
                all_subqueries.clear();
                depth = 0;
                phase_stack = vec![SqlPhase::Initial];
                paren_func_stack = vec![None];
                last_word = None;
                expect_table = false;
                cte_state = CteState::None;
                subquery_tracks.clear();
                next_scope_id = 1;
                scope_stack = vec![0usize];
                idx += 1;
                continue;
            }
            SqlToken::Word(word) => {
                let upper = word.to_ascii_uppercase();

                // CTE state machine for table collection
                match cte_state {
                    CteState::ExpectName if upper != "RECURSIVE" => {
                        cte_state = CteState::AfterName;
                        idx += 1;
                        continue;
                    }
                    CteState::AfterName => {
                        if upper == "AS" {
                            cte_state = CteState::ExpectBody;
                        }
                        idx += 1;
                        continue;
                    }
                    CteState::ExpectAs => {
                        if upper == "AS" {
                            cte_state = CteState::ExpectBody;
                        }
                        idx += 1;
                        continue;
                    }
                    CteState::InBody => {
                        // Process normally inside CTE body
                    }
                    CteState::None => {}
                    _ => {
                        idx += 1;
                        continue;
                    }
                }

                while phase_stack.len() <= depth {
                    phase_stack.push(SqlPhase::Initial);
                }

                // Phase transitions
                match upper.as_str() {
                    "WITH" if matches!(phase_stack[depth], SqlPhase::Initial) => {
                        phase_stack[depth] = SqlPhase::WithClause;
                        cte_state = CteState::ExpectName;
                        expect_table = false;
                    }
                    "MERGE" => {
                        phase_stack[depth] = SqlPhase::MergeTarget;
                        expect_table = false;
                    }
                    "SELECT" => {
                        phase_stack[depth] = SqlPhase::SelectList;
                        expect_table = false;
                    }
                    "FROM" => {
                        let is_func_from = depth > 0
                            && paren_func_stack
                                .get(depth)
                                .and_then(|name| name.as_deref())
                                .is_some_and(is_from_consuming_function);
                        if !is_func_from {
                            phase_stack[depth] = SqlPhase::FromClause;
                            expect_table = true;
                        }
                    }
                    "JOIN" | "APPLY" => {
                        phase_stack[depth] = SqlPhase::FromClause;
                        expect_table = true;
                    }
                    "INTO"
                        if matches!(
                            phase_stack[depth],
                            SqlPhase::SelectList | SqlPhase::Initial | SqlPhase::MergeTarget
                        ) =>
                    {
                        phase_stack[depth] = SqlPhase::IntoClause;
                        expect_table = true;
                    }
                    "USING"
                        if matches!(
                            phase_stack[depth],
                            SqlPhase::MergeTarget | SqlPhase::IntoClause | SqlPhase::FromClause
                        ) =>
                    {
                        phase_stack[depth] = SqlPhase::FromClause;
                        expect_table = true;
                    }
                    "UPDATE" => {
                        phase_stack[depth] = SqlPhase::UpdateTarget;
                        expect_table = true;
                    }
                    "DELETE" => {
                        phase_stack[depth] = SqlPhase::DeleteTarget;
                        expect_table = true;
                    }
                    "ON" if matches!(phase_stack[depth], SqlPhase::FromClause) => {
                        phase_stack[depth] = SqlPhase::JoinCondition;
                        expect_table = false;
                    }
                    "WHERE" | "HAVING" => {
                        phase_stack[depth] = if upper == "WHERE" {
                            SqlPhase::WhereClause
                        } else {
                            SqlPhase::HavingClause
                        };
                        expect_table = false;
                    }
                    "GROUP" if peek_word_upper(tokens, idx + 1) == Some("BY") => {
                        phase_stack[depth] = SqlPhase::GroupByClause;
                        expect_table = false;
                        idx += 1;
                    }
                    "ORDER" if peek_word_upper(tokens, idx + 1) == Some("BY") => {
                        phase_stack[depth] = SqlPhase::OrderByClause;
                        expect_table = false;
                        idx += 1;
                    }
                    "SET" => {
                        phase_stack[depth] = SqlPhase::SetClause;
                        expect_table = false;
                    }
                    "CONNECT" if peek_word_upper(tokens, idx + 1) == Some("BY") => {
                        phase_stack[depth] = SqlPhase::ConnectByClause;
                        expect_table = false;
                        idx += 1;
                    }
                    "START" if peek_word_upper(tokens, idx + 1) == Some("WITH") => {
                        phase_stack[depth] = SqlPhase::StartWithClause;
                        expect_table = false;
                        idx += 1;
                    }
                    "VALUES" => {
                        phase_stack[depth] = SqlPhase::ValuesClause;
                        expect_table = false;
                    }
                    "MATCH_RECOGNIZE" => {
                        phase_stack[depth] = SqlPhase::MatchRecognizeClause;
                        expect_table = false;
                    }
                    "UNION" | "INTERSECT" | "EXCEPT" | "MINUS" => {
                        phase_stack[depth] = SqlPhase::Initial;
                        expect_table = false;
                    }
                    // Keywords that signal end of FROM clause table collection
                    kw if is_table_stop_keyword(kw) && expect_table => {
                        expect_table = false;
                    }
                    _ => {
                        if expect_table {
                            // Try to parse a table name
                            if let Some((table_name, next_idx)) = parse_table_name_deep(tokens, idx)
                            {
                                let (alias, after_alias) = parse_alias_deep(tokens, next_idx);
                                let scope_id = *scope_stack.last().unwrap_or(&0);
                                all_tables.push(ParsedTable {
                                    table: ScopedTableRef {
                                        name: table_name,
                                        alias,
                                        depth,
                                        is_cte: false,
                                    },
                                    scope_id,
                                });
                                // Check if next is comma (continue expecting tables)
                                if let Some(SqlToken::Symbol(sym)) = tokens.get(after_alias) {
                                    if sym == "," {
                                        expect_table = true;
                                        idx = after_alias + 1;
                                        continue;
                                    }
                                }
                                expect_table = false;
                                idx = after_alias;
                                continue;
                            }
                            expect_table = false;
                        }
                    }
                }
                last_word = Some(upper);
            }
            _ => {
                last_word = None;
            }
        }
        idx += 1;
    }

    let visible_scope_ids: HashSet<usize> = cursor_scope_chain.iter().copied().collect();

    // Visible objects are those defined in current scope or any ancestor scope.
    let visible: Vec<ScopedTableRef> = all_tables
        .into_iter()
        .filter(|entry| visible_scope_ids.contains(&entry.scope_id))
        .map(|entry| entry.table)
        .collect();

    let visible_subqueries: Vec<SubqueryDefinition> = all_subqueries
        .into_iter()
        .filter(|entry| visible_scope_ids.contains(&entry.scope_id))
        .map(|entry| entry.subquery)
        .collect();

    TableAnalysis {
        tables: visible,
        subqueries: visible_subqueries,
    }
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
                if matches!(
                    u.as_str(),
                    "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "MERGE"
                ) {
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
                w.clone()
            }
            _ => break,
        };
        idx += 1;

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

        // Expect AS
        if let Some(SqlToken::Word(w)) = tokens.get(idx) {
            if w.eq_ignore_ascii_case("AS") {
                idx += 1;
            }
        }

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
fn peek_word_upper(tokens: &[SqlToken], idx: usize) -> Option<&'static str> {
    let mut i = idx;
    while i < tokens.len() {
        match &tokens[i] {
            SqlToken::Comment(_) => {
                i += 1;
                continue;
            }
            SqlToken::Word(w) => {
                let upper = w.to_ascii_uppercase();
                // Return a static str by matching known keywords
                return match upper.as_str() {
                    "BY" => Some("BY"),
                    "WITH" => Some("WITH"),
                    "AS" => Some("AS"),
                    _ => None,
                };
            }
            _ => return None,
        }
    }
    None
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
            let table = parts.join(".");
            Some((table, idx))
        }
        _ => None,
    }
}

/// Parse an optional alias after a table name.
fn parse_alias_deep(tokens: &[SqlToken], start: usize) -> (Option<String>, usize) {
    if let Some(SqlToken::Word(word)) = tokens.get(start) {
        let is_quoted = word.trim().starts_with('"') && word.trim().ends_with('"');
        let upper = word.to_ascii_uppercase();
        if upper == "AS" {
            if let Some(SqlToken::Word(alias)) = tokens.get(start + 1) {
                if !is_identifier_word_token(alias) {
                    return (None, start + 2);
                }
                return (Some(strip_identifier_quotes(alias)), start + 2);
            }
            return (None, start + 1);
        }
        if !is_identifier_word_token(word) {
            return (None, start);
        }
        if is_quoted || !is_alias_breaker(&upper) {
            return (Some(strip_identifier_quotes(word)), start + 1);
        }
    }
    (None, start)
}

/// Parse an alias after a subquery closing ')'.
fn parse_subquery_alias(tokens: &[SqlToken], start: usize) -> Option<(String, usize)> {
    fn skip_comments(tokens: &[SqlToken], mut idx: usize) -> usize {
        while idx < tokens.len() {
            if let SqlToken::Comment(_) = &tokens[idx] {
                idx += 1;
                continue;
            }
            break;
        }
        idx
    }

    fn consume_optional_alias_column_list(tokens: &[SqlToken], start: usize) -> usize {
        let idx = skip_comments(tokens, start);
        match tokens.get(idx) {
            Some(SqlToken::Symbol(sym)) if sym == "(" => extract_parenthesized_range(tokens, idx)
                .map(|(_, next_idx)| next_idx)
                .unwrap_or(idx),
            _ => idx,
        }
    }

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

    match tokens.get(idx) {
        Some(SqlToken::Word(word)) => {
            let is_quoted = word.trim().starts_with('"') && word.trim().ends_with('"');
            let upper = word.to_ascii_uppercase();
            if upper == "AS" {
                idx += 1;
                // Skip comments after AS
                idx = skip_comments(tokens, idx);
                if let Some(SqlToken::Word(alias)) = tokens.get(idx) {
                    if !is_identifier_word_token(alias) {
                        return None;
                    }
                    let next_idx = consume_optional_alias_column_list(tokens, idx + 1);
                    return Some((strip_identifier_quotes(alias), next_idx));
                }
                return None;
            }
            if !is_identifier_word_token(word) {
                return None;
            }
            if is_quoted || (!is_alias_breaker(&upper) && !is_join_keyword(&upper)) {
                let next_idx = consume_optional_alias_column_list(tokens, idx + 1);
                return Some((strip_identifier_quotes(word), next_idx));
            }
            None
        }
        _ => None,
    }
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
            | "LATERAL"
            | "APPLY"
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
            | "PIVOT"
            | "UNPIVOT"
            | "MODEL"
            | "MATCH_RECOGNIZE"
            | "USING"
    )
}

fn is_alias_breaker(word: &str) -> bool {
    matches!(
        word,
        "ON" | "JOIN"
            | "INNER"
            | "LEFT"
            | "RIGHT"
            | "FULL"
            | "CROSS"
            | "OUTER"
            | "NATURAL"
            | "APPLY"
            | "WHERE"
            | "GROUP"
            | "ORDER"
            | "HAVING"
            | "CONNECT"
            | "START"
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
            | "USING"
            | "PIVOT"
            | "UNPIVOT"
            | "MODEL"
            | "MATCH_RECOGNIZE"
            | "SELECT"
            | "FROM"
            | "INTO"
    )
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
            if alias_match
                .as_ref()
                .is_none_or(|(depth, _)| table_ref.depth >= *depth)
            {
                alias_match = Some((table_ref.depth, table_ref.name.clone()));
            }
            continue;
        }

        if name_upper == qualifier_upper
            && name_match
                .as_ref()
                .is_none_or(|(depth, _)| table_ref.depth >= *depth)
        {
            name_match = Some((table_ref.depth, table_ref.name.clone()));
            continue;
        }

        if last_identifier_part_for_lookup(&table_ref.name)
            .is_some_and(|short| short.eq_ignore_ascii_case(&qualifier_upper))
            && short_name_match
                .as_ref()
                .is_none_or(|(depth, _)| table_ref.depth >= *depth)
        {
            short_name_match = Some((table_ref.depth, table_ref.name.clone()));
        }
    }

    if let Some((_, name)) = alias_match {
        if seen.insert(name.to_ascii_uppercase()) {
            return vec![name];
        }
    }

    if let Some((_, name)) = name_match {
        if seen.insert(name.to_ascii_uppercase()) {
            return vec![name];
        }
    }

    if let Some((_, name)) = short_name_match {
        if seen.insert(name.to_ascii_uppercase()) {
            return vec![name];
        }
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

/// Extract MATCH_RECOGNIZE pattern variables from `PATTERN (...)`.
/// Example: `PATTERN (a b+)` -> `["a", "b"]`.
pub fn extract_match_recognize_pattern_variables(tokens: &[SqlToken]) -> Vec<String> {
    let Some(match_idx) = find_top_level_word_index(tokens, "MATCH_RECOGNIZE") else {
        return Vec::new();
    };

    let clause_open_idx = next_non_comment_index(tokens, match_idx.saturating_add(1));
    let Some(SqlToken::Symbol(sym)) = tokens.get(clause_open_idx) else {
        return Vec::new();
    };
    if sym != "(" {
        return Vec::new();
    }

    let Some((clause_range, _)) = extract_parenthesized_range(tokens, clause_open_idx) else {
        return Vec::new();
    };
    let clause_tokens = token_range_slice(tokens, clause_range);
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

    dedup_columns_case_insensitive(&mut variables);
    variables
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
        dedup_columns_case_insensitive(&mut columns);
        return columns;
    }

    let mut columns = extract_select_list_columns(tokens);
    if columns.is_empty() {
        columns = extract_table_function_columns(tokens);
    }
    dedup_columns_case_insensitive(&mut columns);
    columns
}

fn parse_top_level_pivot_clause(tokens: &[SqlToken]) -> Option<PivotClauseColumns> {
    let pivot_idx = find_top_level_word_index(tokens, "PIVOT")?;
    let mut idx = next_non_comment_index(tokens, pivot_idx.saturating_add(1));

    if let Some(SqlToken::Word(word)) = tokens.get(idx) {
        if word.eq_ignore_ascii_case("XML") {
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
    let generated_columns =
        parse_pivot_generated_columns_from_in_segment(&clause_tokens[in_idx + 1..]);

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

    let mut expect_word = true;
    let mut last_identifier = None;
    for token in tokens {
        match token {
            SqlToken::Word(word) if expect_word && is_identifier_word_token(word) => {
                last_identifier = Some(strip_identifier_quotes(word));
                expect_word = false;
            }
            SqlToken::Symbol(sym) if !expect_word && sym == "." => {
                expect_word = true;
            }
            _ => return None,
        }
    }

    if expect_word {
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
    matches!(
        word,
        "NESTED"
            | "PATH"
            | "COLUMNS"
            | "EXISTS"
            | "FOR"
            | "ORDINALITY"
            | "ERROR"
            | "NULL"
            | "DEFAULT"
            | "ON"
            | "FORMAT"
            | "WRAPPER"
            | "WITHOUT"
            | "WITH"
            | "CONDITIONAL"
            | "UNCONDITIONAL"
            | "KEEP"
            | "OMIT"
            | "QUOTES"
    )
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
    let mut expect_word = true;
    for token in tokens {
        if expect_word {
            if let SqlToken::Word(word) = token {
                let segment = strip_identifier_quotes(word);
                if segment.is_empty() {
                    return None;
                }
                parts.push(segment);
                expect_word = false;
            } else {
                return None;
            }
        } else if let SqlToken::Symbol(sym) = token {
            if sym == "." {
                expect_word = true;
            } else {
                return None;
            }
        } else {
            return None;
        }
    }

    if expect_word || parts.is_empty() {
        None
    } else {
        Some(parts.join("."))
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
                return Some(alias.clone());
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
                    return Some(alias.clone());
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
            return Some(name.clone());
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
                return Some(col.clone());
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
