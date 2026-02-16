use std::collections::{HashMap, HashSet};

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
    /// Tokens inside the CTE body parentheses (the SELECT statement).
    pub body_tokens: Vec<SqlToken>,
}

/// A subquery alias with its body tokens, for column inference.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SubqueryDefinition {
    pub alias: String,
    pub body_tokens: Vec<SqlToken>,
    pub depth: usize,
}

/// Result of deep context analysis at cursor position.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CursorContext {
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
/// `before_cursor` is the SQL text from statement start up to cursor.
/// `full_statement` is the complete statement text (for collecting all table references).
/// `tokenize` is used to tokenize the SQL.
pub fn analyze_cursor_context(
    before_cursor: &[SqlToken],
    full_statement: &[SqlToken],
) -> CursorContext {
    let phase_analysis = analyze_phase(before_cursor);
    let table_analysis = collect_tables_deep(full_statement, &phase_analysis.visible_scope_chain);
    let ctes = parse_ctes(full_statement);

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

/// Walk tokens up to cursor to determine the current SQL phase and depth.
fn analyze_phase(tokens: &[SqlToken]) -> PhaseAnalysis {
    let mut depth: usize = 0;
    // Track phase at each depth level
    let mut phase_stack: Vec<SqlPhase> = vec![SqlPhase::Initial];
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
                let inherited_phase = if parent_phase.is_column_context() {
                    parent_phase
                } else {
                    SqlPhase::Initial
                };
                if phase_stack.len() <= depth {
                    phase_stack.push(inherited_phase);
                } else {
                    phase_stack[depth] = inherited_phase;
                }
                let scope_id = next_scope_id;
                next_scope_id += 1;
                scope_stack.push(scope_id);
                // Derived table/subquery in FROM introduces an isolated scope.
                let inherited_visible_parent =
                    if matches!(parent_phase, SqlPhase::FromClause) && !pending_lateral_subquery {
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
                if depth > 0 {
                    depth -= 1;
                }
                if scope_stack.len() > 1 {
                    scope_stack.pop();
                }
                pending_lateral_subquery = false;
                idx += 1;
                continue;
            }
            SqlToken::Comment(_) | SqlToken::String(_) => {
                idx += 1;
                continue;
            }
            SqlToken::Word(word) => {
                let upper = word.to_uppercase();

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

                if upper == "LATERAL" && matches!(current_phase, SqlPhase::FromClause) {
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
                        // Avoid transition for EXTRACT(... FROM ...)
                        if !matches!(current_phase, SqlPhase::Initial) || depth > 0 {
                            phase_stack[depth] = SqlPhase::FromClause;
                        } else {
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
                    "JOIN" => {
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
                            && matches!(phase_stack.get(0), Some(SqlPhase::WithClause))
                            && depth == 0
                        {
                            // We might be between CTE definitions
                        }
                    }
                }
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
fn collect_tables_deep(tokens: &[SqlToken], cursor_scope_chain: &[usize]) -> TableAnalysis {
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
                        if depth > 0 {
                            depth -= 1;
                        }
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
                        // Capture body tokens for column inference
                        let body_tokens: Vec<SqlToken> =
                            tokens[start_idx..idx].iter().cloned().collect();
                        all_subqueries.push(ParsedSubquery {
                            subquery: SubqueryDefinition {
                                alias: alias.clone(),
                                body_tokens,
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
                        if depth > 0 {
                            depth -= 1;
                        }
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
                    let body_tokens: Vec<SqlToken> =
                        tokens[start_idx..idx].iter().cloned().collect();
                    all_subqueries.push(ParsedSubquery {
                        subquery: SubqueryDefinition {
                            alias: generated_name.clone(),
                            body_tokens,
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

                if depth > 0 {
                    depth -= 1;
                }
                if scope_stack.len() > 1 {
                    scope_stack.pop();
                }
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
                expect_table = false;
                cte_state = CteState::None;
                subquery_tracks.clear();
                next_scope_id = 1;
                scope_stack = vec![0usize];
                idx += 1;
                continue;
            }
            SqlToken::Word(word) => {
                let upper = word.to_uppercase();

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
                        phase_stack[depth] = SqlPhase::FromClause;
                        expect_table = true;
                    }
                    "JOIN" => {
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
            }
            _ => {}
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

/// Parse CTE definitions from WITH clause.
fn parse_ctes(tokens: &[SqlToken]) -> Vec<CteDefinition> {
    let mut ctes = Vec::new();
    let mut idx = 0;

    // Find top-level WITH keyword
    let mut depth = 0usize;
    let mut found_with = false;
    while idx < tokens.len() {
        match &tokens[idx] {
            SqlToken::Symbol(sym) if sym == "(" => depth += 1,
            SqlToken::Symbol(sym) if sym == ")" => depth = depth.saturating_sub(1),
            SqlToken::Word(w) if depth == 0 && w.eq_ignore_ascii_case("WITH") => {
                idx += 1;
                found_with = true;
                break;
            }
            // If we hit a top-level statement keyword before WITH, no CTEs.
            SqlToken::Word(w) if depth == 0 => {
                let u = w.to_uppercase();
                if matches!(
                    u.as_str(),
                    "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "MERGE"
                ) {
                    return ctes;
                }
            }
            _ => {}
        }
        idx += 1;
    }

    if !found_with {
        return ctes;
    }

    // Skip RECURSIVE if present
    if let Some(SqlToken::Word(w)) = tokens.get(idx) {
        if w.to_uppercase() == "RECURSIVE" {
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
                let u = w.to_uppercase();
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

        // Check for explicit column list: cte_name(col1, col2)
        if let Some(SqlToken::Symbol(s)) = tokens.get(idx) {
            if s == "(" {
                idx += 1;
                let mut paren_depth = 1;
                while idx < tokens.len() && paren_depth > 0 {
                    match &tokens[idx] {
                        SqlToken::Symbol(s) if s == "(" => paren_depth += 1,
                        SqlToken::Symbol(s) if s == ")" => {
                            paren_depth -= 1;
                            if paren_depth == 0 {
                                idx += 1;
                                break;
                            }
                        }
                        SqlToken::Word(w) if paren_depth == 1 => {
                            explicit_columns.push(w.clone());
                        }
                        _ => {}
                    }
                    idx += 1;
                }
            }
        }

        // Expect AS
        if let Some(SqlToken::Word(w)) = tokens.get(idx) {
            if w.to_uppercase() == "AS" {
                idx += 1;
            }
        }

        // Capture CTE body tokens (balanced parens)
        let mut body_tokens = Vec::new();
        if let Some(SqlToken::Symbol(s)) = tokens.get(idx) {
            if s == "(" {
                idx += 1;
                let mut paren_depth = 1;
                while idx < tokens.len() && paren_depth > 0 {
                    match &tokens[idx] {
                        SqlToken::Symbol(s) if s == "(" => {
                            paren_depth += 1;
                            body_tokens.push(tokens[idx].clone());
                        }
                        SqlToken::Symbol(s) if s == ")" => {
                            paren_depth -= 1;
                            if paren_depth > 0 {
                                body_tokens.push(tokens[idx].clone());
                            }
                        }
                        _ => {
                            body_tokens.push(tokens[idx].clone());
                        }
                    }
                    idx += 1;
                }
            }
        }

        ctes.push(CteDefinition {
            name: cte_name,
            explicit_columns,
            body_tokens,
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
                let upper = w.to_uppercase();
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
    let trimmed = value.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        trimmed[1..trimmed.len() - 1].replace("\"\"", "\"")
    } else {
        trimmed.to_string()
    }
}

fn normalize_identifier_for_lookup(value: &str) -> String {
    strip_identifier_quotes(value).to_uppercase()
}

/// Parse a table name at the given position (handling schema.table format).
fn parse_table_name_deep(tokens: &[SqlToken], start: usize) -> Option<(String, usize)> {
    match tokens.get(start) {
        Some(SqlToken::Symbol(sym)) if sym == "(" => None,
        Some(SqlToken::Word(word)) => {
            let is_quoted = word.trim().starts_with('"') && word.trim().ends_with('"');
            let upper = word.to_uppercase();
            // Skip if this is a keyword rather than a table name
            if !is_quoted && (is_join_keyword(&upper) || is_table_stop_keyword(&upper)) {
                return None;
            }
            let mut parts = vec![strip_identifier_quotes(word)];
            let mut idx = start + 1;
            // Handle dotted relation names like schema.table.
            while matches!(tokens.get(idx), Some(SqlToken::Symbol(sym)) if sym == ".") {
                if let Some(SqlToken::Word(name)) = tokens.get(idx + 1) {
                    parts.push(strip_identifier_quotes(name));
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
    match tokens.get(start) {
        Some(SqlToken::Word(word)) => {
            let is_quoted = word.trim().starts_with('"') && word.trim().ends_with('"');
            let upper = word.to_uppercase();
            if upper == "AS" {
                if let Some(SqlToken::Word(alias)) = tokens.get(start + 1) {
                    return (Some(strip_identifier_quotes(alias)), start + 2);
                }
                return (None, start + 1);
            }
            if is_quoted || !is_alias_breaker(&upper) {
                return (Some(strip_identifier_quotes(word)), start + 1);
            }
        }
        _ => {}
    }
    (None, start)
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

    match tokens.get(idx) {
        Some(SqlToken::Word(word)) => {
            let is_quoted = word.trim().starts_with('"') && word.trim().ends_with('"');
            let upper = word.to_uppercase();
            if upper == "AS" {
                idx += 1;
                // Skip comments after AS
                while idx < tokens.len() {
                    if let SqlToken::Comment(_) = &tokens[idx] {
                        idx += 1;
                        continue;
                    }
                    break;
                }
                if let Some(SqlToken::Word(alias)) = tokens.get(idx) {
                    return Some((strip_identifier_quotes(alias), idx + 1));
                }
                return None;
            }
            if is_quoted || (!is_alias_breaker(&upper) && !is_join_keyword(&upper)) {
                return Some((strip_identifier_quotes(word), idx + 1));
            }
            None
        }
        _ => None,
    }
}

fn is_join_keyword(word: &str) -> bool {
    matches!(
        word,
        "JOIN" | "INNER" | "LEFT" | "RIGHT" | "FULL" | "CROSS" | "OUTER" | "NATURAL" | "LATERAL"
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
            | "SELECT"
            | "FROM"
            | "INTO"
    )
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
                .map_or(true, |(depth, _)| table_ref.depth >= *depth)
            {
                alias_match = Some((table_ref.depth, table_ref.name.clone()));
            }
            continue;
        }

        if name_upper == qualifier_upper
            && name_match
                .as_ref()
                .map_or(true, |(depth, _)| table_ref.depth >= *depth)
        {
            name_match = Some((table_ref.depth, table_ref.name.clone()));
            continue;
        }

        if name_upper
            .rsplit('.')
            .next()
            .is_some_and(|short| short == qualifier_upper)
            && short_name_match
                .as_ref()
                .map_or(true, |(depth, _)| table_ref.depth >= *depth)
        {
            short_name_match = Some((table_ref.depth, table_ref.name.clone()));
        }
    }

    if let Some((_, name)) = alias_match {
        if seen.insert(name.to_uppercase()) {
            return vec![name];
        }
    }

    if let Some((_, name)) = name_match {
        if seen.insert(name.to_uppercase()) {
            return vec![name];
        }
    }

    if let Some((_, name)) = short_name_match {
        if seen.insert(name.to_uppercase()) {
            return vec![name];
        }
    }

    // If no match found, try the qualifier as a direct table name
    let normalized = strip_identifier_quotes(qualifier);
    if seen.insert(normalized.to_uppercase()) {
        return vec![normalized];
    }

    Vec::new()
}

/// Resolve all table names from scope (for unqualified column suggestions).
pub fn resolve_all_scope_tables(tables_in_scope: &[ScopedTableRef]) -> Vec<String> {
    let mut result = Vec::new();
    let mut seen = HashSet::new();

    for table_ref in tables_in_scope {
        let upper = table_ref.name.to_uppercase();
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
    let mut idx = 0;

    // Find SELECT keyword
    while idx < tokens.len() {
        match &tokens[idx] {
            SqlToken::Word(w) if w.eq_ignore_ascii_case("SELECT") => {
                idx += 1;
                break;
            }
            SqlToken::Comment(_) => {
                idx += 1;
                continue;
            }
            _ => {
                idx += 1;
                continue;
            }
        }
    }

    // Skip DISTINCT / ALL / UNIQUE
    while idx < tokens.len() {
        match &tokens[idx] {
            SqlToken::Word(w) => {
                let u = w.to_uppercase();
                if matches!(u.as_str(), "DISTINCT" | "ALL" | "UNIQUE") {
                    idx += 1;
                    continue;
                }
                break;
            }
            SqlToken::Comment(_) => {
                idx += 1;
                continue;
            }
            _ => break,
        }
    }

    // Collect tokens for each SELECT item (delimited by comma at depth 0)
    let mut depth = 0usize;
    let mut item_tokens: Vec<&SqlToken> = Vec::new();

    while idx < tokens.len() {
        let token = &tokens[idx];

        match token {
            SqlToken::Symbol(s) if s == "(" => {
                depth += 1;
                item_tokens.push(token);
            }
            SqlToken::Symbol(s) if s == ")" => {
                depth = depth.saturating_sub(1);
                item_tokens.push(token);
            }
            SqlToken::Symbol(s) if s == "," && depth == 0 => {
                if let Some(col) = resolve_item_column_name(&item_tokens) {
                    columns.push(col);
                }
                item_tokens.clear();
            }
            SqlToken::Word(w) if depth == 0 => {
                let u = w.to_uppercase();
                if matches!(u.as_str(), "FROM" | "INTO" | "BULK") {
                    if let Some(col) = resolve_item_column_name(&item_tokens) {
                        columns.push(col);
                    }
                    item_tokens.clear();
                    break;
                }
                item_tokens.push(token);
            }
            SqlToken::Comment(_) => { /* skip */ }
            _ => {
                item_tokens.push(token);
            }
        }
        idx += 1;
    }

    // Handle last item if we ran out of tokens (incomplete SQL)
    if !item_tokens.is_empty() {
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
    let mut idx = 0;

    // Find SELECT keyword
    while idx < tokens.len() {
        match &tokens[idx] {
            SqlToken::Word(w) if w.eq_ignore_ascii_case("SELECT") => {
                idx += 1;
                break;
            }
            SqlToken::Comment(_) => {
                idx += 1;
                continue;
            }
            _ => {
                idx += 1;
                continue;
            }
        }
    }

    // Skip DISTINCT / ALL / UNIQUE
    while idx < tokens.len() {
        match &tokens[idx] {
            SqlToken::Word(w) => {
                let u = w.to_uppercase();
                if matches!(u.as_str(), "DISTINCT" | "ALL" | "UNIQUE") {
                    idx += 1;
                    continue;
                }
                break;
            }
            SqlToken::Comment(_) => {
                idx += 1;
                continue;
            }
            _ => break,
        }
    }

    // Collect tokens for each SELECT item (delimited by comma at depth 0)
    let mut depth = 0usize;
    let mut item_tokens: Vec<&SqlToken> = Vec::new();

    while idx < tokens.len() {
        let token = &tokens[idx];

        match token {
            SqlToken::Symbol(s) if s == "(" => {
                depth += 1;
                item_tokens.push(token);
            }
            SqlToken::Symbol(s) if s == ")" => {
                depth = depth.saturating_sub(1);
                item_tokens.push(token);
            }
            SqlToken::Symbol(s) if s == "," && depth == 0 => {
                append_wildcard_item_tables(&item_tokens, tables_in_scope, &mut tables, &mut seen);
                item_tokens.clear();
            }
            SqlToken::Word(w) if depth == 0 => {
                let u = w.to_uppercase();
                if matches!(u.as_str(), "FROM" | "INTO" | "BULK") {
                    append_wildcard_item_tables(
                        &item_tokens,
                        tables_in_scope,
                        &mut tables,
                        &mut seen,
                    );
                    item_tokens.clear();
                    break;
                }
                item_tokens.push(token);
            }
            SqlToken::Comment(_) => { /* skip */ }
            _ => {
                item_tokens.push(token);
            }
        }
        idx += 1;
    }

    if !item_tokens.is_empty() {
        append_wildcard_item_tables(&item_tokens, tables_in_scope, &mut tables, &mut seen);
    }

    tables
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
                    let key = table.to_uppercase();
                    if seen.insert(key) {
                        tables.push(table);
                    }
                }
                return;
            }
        }
    }

    // Qualified wildcard: `alias.*` (or `schema.table.*`; we resolve by the
    // identifier immediately before the final dot).
    if meaningful.len() >= 3 {
        let last = meaningful[meaningful.len() - 1];
        let dot = meaningful[meaningful.len() - 2];
        let qualifier = meaningful[meaningful.len() - 3];
        if let (SqlToken::Symbol(star), SqlToken::Symbol(dot_sym), SqlToken::Word(q)) =
            (last, dot, qualifier)
        {
            if star == "*" && dot_sym == "." {
                let normalized = strip_identifier_quotes(q);
                for table in resolve_qualifier_tables(&normalized, tables_in_scope) {
                    let key = table.to_uppercase();
                    if seen.insert(key) {
                        tables.push(table);
                    }
                }
            }
        }
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
        if let Some(SqlToken::Word(kw)) = second_last {
            if kw.eq_ignore_ascii_case("AS") {
                return Some(alias.clone());
            }
        }
    }

    // Case 2: Implicit alias — last token is a Word following `)` or another Word
    if let SqlToken::Word(alias) = last {
        let alias_upper = alias.to_uppercase();
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
            return Some(name.clone());
        }
    }

    // Case 4: Qualified column `qualifier.column`
    if meaningful.len() == 3 {
        if let (SqlToken::Word(_), SqlToken::Symbol(dot), SqlToken::Word(col)) =
            (meaningful[0], meaningful[1], meaningful[2])
        {
            if dot == "." {
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
mod tests;
