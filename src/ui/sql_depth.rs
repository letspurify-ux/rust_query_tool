use crate::ui::sql_editor::SqlToken;

#[derive(Default)]
pub(crate) struct ParenDepthState {
    stack: Vec<char>,
}

impl ParenDepthState {
    #[inline]
    pub(crate) fn depth(&self) -> usize {
        self.stack.len()
    }

    pub(crate) fn apply_token(&mut self, token: &SqlToken) {
        let symbol = match token {
            SqlToken::Symbol(sym) => sym.as_str(),
            _ => return,
        };

        match symbol {
            "(" | "[" | "{" => {
                if let Some(ch) = symbol.chars().next() {
                    self.stack.push(ch);
                }
            }
            ")" | "]" | "}" => {
                if let Some(close_ch) = symbol.chars().next() {
                    self.consume_close(close_ch);
                }
            }
            _ => {}
        }
    }

    fn consume_close(&mut self, close_ch: char) {
        let Some(expected_open) = matching_open_for_close(close_ch) else {
            return;
        };

        if self.stack.last().copied() == Some(expected_open) {
            self.stack.pop();
            return;
        }

        if let Some(match_idx) = self.stack.iter().rposition(|&open| open == expected_open) {
            self.stack.truncate(match_idx);
        }
    }
}

#[inline]
fn matching_open_for_close(close_ch: char) -> Option<char> {
    match close_ch {
        ')' => Some('('),
        ']' => Some('['),
        '}' => Some('{'),
        _ => None,
    }
}

/// Returns the parenthesis depth *before* each token is processed.
///
/// Depth changes for grouping symbols (`()`, `[]`, `{}`) and never goes below zero.
pub(crate) fn paren_depths(tokens: &[SqlToken]) -> Vec<usize> {
    let mut depths = Vec::with_capacity(tokens.len());
    let mut state = ParenDepthState::default();

    for token in tokens {
        depths.push(state.depth());
        state.apply_token(token);
    }

    depths
}

/// Applies parenthesis depth transition for a single token.
#[inline]
pub(crate) fn apply_paren_token(state: &mut ParenDepthState, token: &SqlToken) {
    state.apply_token(token);
}

/// Returns the final parenthesis depth after all tokens are processed.
pub(crate) fn paren_depth_after(tokens: &[SqlToken]) -> usize {
    let mut state = ParenDepthState::default();
    for token in tokens {
        state.apply_token(token);
    }
    state.depth()
}

/// Returns token depth at `idx`, treating out-of-range indices as depth 0.
#[inline]
pub(crate) fn depth_at(depths: &[usize], idx: usize) -> usize {
    depths.get(idx).copied().unwrap_or(0)
}

/// Returns true when a token is at top-level (depth 0).
#[inline]
pub(crate) fn is_top_level_depth(depths: &[usize], idx: usize) -> bool {
    depth_at(depths, idx) == 0
}

/// Returns true when a token is at a specific depth.
#[inline]
pub(crate) fn is_depth(depths: &[usize], idx: usize, depth: usize) -> bool {
    depth_at(depths, idx) == depth
}

/// Splits tokens by a top-level delimiter symbol (예: `,`) while ignoring nested
/// parenthesis depth.
///
/// Empty segments are skipped to match existing parser behavior.
pub(crate) fn split_top_level_symbol_groups<'a>(
    tokens: &'a [SqlToken],
    delimiter: &str,
) -> Vec<Vec<&'a SqlToken>> {
    let mut groups: Vec<Vec<&'a SqlToken>> = Vec::new();
    let mut current: Vec<&'a SqlToken> = Vec::new();
    let mut state = ParenDepthState::default();

    for token in tokens {
        let at_root = state.depth() == 0;
        if let SqlToken::Symbol(sym) = token {
            if sym == delimiter && at_root {
                if !current.is_empty() {
                    groups.push(std::mem::take(&mut current));
                }
                continue;
            }
        }

        current.push(token);
        state.apply_token(token);
    }

    if !current.is_empty() {
        groups.push(current);
    }

    groups
}

/// Splits tokens by top-level SQL keyword boundaries while preserving the keyword
/// as the first token of the next group.
///
/// Empty segments are skipped to match existing parser behavior.
pub(crate) fn split_top_level_keyword_groups<'a>(
    tokens: &'a [SqlToken],
    break_keywords: &[&str],
) -> Vec<Vec<&'a SqlToken>> {
    let mut groups: Vec<Vec<&'a SqlToken>> = Vec::new();
    let mut current: Vec<&'a SqlToken> = Vec::new();
    let mut state = ParenDepthState::default();

    for token in tokens {
        let is_break = match token {
            SqlToken::Word(word) => {
                state.depth() == 0
                    && break_keywords
                        .iter()
                        .any(|keyword| word.eq_ignore_ascii_case(keyword))
            }
            _ => false,
        };

        if is_break && !current.is_empty() {
            groups.push(std::mem::take(&mut current));
        }

        current.push(token);
        state.apply_token(token);
    }

    if !current.is_empty() {
        groups.push(current);
    }

    groups
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ui::sql_editor::query_text::tokenize_sql;

    fn sym(s: &str) -> SqlToken {
        SqlToken::Symbol(s.to_string())
    }
    fn word(s: &str) -> SqlToken {
        SqlToken::Word(s.to_string())
    }

    // ── paren_depths ──────────────────────────────────────────────────────────

    #[test]
    fn paren_depths_empty_returns_empty() {
        assert_eq!(paren_depths(&[]), Vec::<usize>::new());
    }

    #[test]
    fn paren_depths_records_pre_token_depth() {
        // For tokens: word, (, word, ), word
        // depths before each:  0,  0,  1,  1,  0
        let tokens = [word("a"), sym("("), word("b"), sym(")"), word("c")];
        assert_eq!(paren_depths(&tokens), vec![0, 0, 1, 1, 0]);
    }

    #[test]
    fn paren_depths_nested_parens() {
        // ((x)) → depths before each token: (, (, x, ), )
        //                                    0  1  2  2  1
        let tokens = [sym("("), sym("("), word("x"), sym(")"), sym(")")];
        assert_eq!(paren_depths(&tokens), vec![0, 1, 2, 2, 1]);
    }

    #[test]
    fn paren_depths_tracks_brackets_and_braces() {
        let tokens = [sym("["), sym("{"), word("x"), sym("}"), sym("]"), word("y")];
        assert_eq!(paren_depths(&tokens), vec![0, 1, 2, 2, 1, 0]);
    }

    #[test]
    fn paren_depths_saturates_at_zero_for_unbalanced_close() {
        // ) at depth 0 must not underflow
        let tokens = [sym(")"), word("x")];
        assert_eq!(paren_depths(&tokens), vec![0, 0]);
    }

    #[test]
    fn paren_depths_ignores_string_and_comment_tokens() {
        // Parens inside string/comment variants must not affect depth
        let tokens = [
            SqlToken::String("(text)".to_string()),
            SqlToken::Comment("-- (note)".to_string()),
            word("x"),
        ];
        assert_eq!(paren_depths(&tokens), vec![0, 0, 0]);
    }

    // ── paren_depth_after ─────────────────────────────────────────────────────

    #[test]
    fn paren_depth_after_balanced_is_zero() {
        let tokens = [sym("("), word("x"), sym(")")];
        assert_eq!(paren_depth_after(&tokens), 0);
    }

    #[test]
    fn paren_depth_after_open_paren_returns_one() {
        let tokens = [sym("("), word("x")];
        assert_eq!(paren_depth_after(&tokens), 1);
    }

    #[test]
    fn paren_depth_after_detects_unbalanced_open_via_tokenizer() {
        // Ensure the tokenizer + paren_depth_after pipeline works end-to-end
        let tokens = tokenize_sql("SELECT (a + b FROM dual");
        assert!(
            paren_depth_after(&tokens) > 0,
            "unbalanced open paren should yield depth > 0"
        );
    }

    #[test]
    fn paren_depth_after_balanced_via_tokenizer() {
        let tokens = tokenize_sql("SELECT (a + b) FROM dual");
        assert_eq!(paren_depth_after(&tokens), 0);
    }

    // ── split_top_level_symbol_groups ─────────────────────────────────────────

    fn group_words(groups: Vec<Vec<&SqlToken>>) -> Vec<Vec<String>> {
        groups
            .into_iter()
            .map(|g| {
                g.into_iter()
                    .map(|t| match t {
                        SqlToken::Word(w) => w.clone(),
                        SqlToken::Symbol(s) => s.clone(),
                        SqlToken::String(s) => s.clone(),
                        SqlToken::Comment(c) => c.clone(),
                    })
                    .collect()
            })
            .collect()
    }

    #[test]
    fn split_top_level_symbol_groups_splits_by_comma() {
        let tokens = tokenize_sql("a, b, c");
        let groups = split_top_level_symbol_groups(&tokens, ",");
        assert_eq!(groups.len(), 3);
    }

    #[test]
    fn split_top_level_symbol_groups_ignores_nested_comma() {
        // The `,` inside `(b, c)` must not split at top level
        let tokens = tokenize_sql("a, (b, c), d");
        let groups = split_top_level_symbol_groups(&tokens, ",");
        assert_eq!(
            groups.len(),
            3,
            "expected 3 groups, got {:?}",
            group_words(groups)
        );
    }

    #[test]
    fn split_top_level_symbol_groups_empty_input_returns_empty() {
        let groups = split_top_level_symbol_groups(&[], ",");
        assert!(groups.is_empty());
    }

    #[test]
    fn split_top_level_symbol_groups_no_delimiter_returns_one_group() {
        let tokens = tokenize_sql("a b c");
        let groups = split_top_level_symbol_groups(&tokens, ",");
        assert_eq!(groups.len(), 1);
    }

    #[test]
    fn split_top_level_symbol_groups_string_literal_comma_not_split() {
        // A `,` inside a string literal must not cause a split
        let tokens = tokenize_sql("'a,b', c");
        let groups = split_top_level_symbol_groups(&tokens, ",");
        assert_eq!(
            groups.len(),
            2,
            "string literal comma must not split, got {:?}",
            group_words(groups)
        );
    }

    #[test]
    fn split_top_level_symbol_groups_ignores_nested_comma_in_brackets() {
        let tokens = tokenize_sql("a, [b, c], d");
        let groups = split_top_level_symbol_groups(&tokens, ",");
        assert_eq!(
            groups.len(),
            3,
            "expected bracket depth to block split, got {:?}",
            group_words(groups)
        );
    }

    #[test]
    fn split_top_level_symbol_groups_ignores_nested_comma_in_braces() {
        let tokens = tokenize_sql("a, {b, c}, d");
        let groups = split_top_level_symbol_groups(&tokens, ",");
        assert_eq!(
            groups.len(),
            3,
            "expected brace depth to block split, got {:?}",
            group_words(groups)
        );
    }

    // ── split_top_level_keyword_groups ────────────────────────────────────────

    #[test]
    fn split_top_level_keyword_groups_splits_select_from_where() {
        let tokens = tokenize_sql("SELECT a FROM t WHERE x = 1");
        let groups = split_top_level_keyword_groups(&tokens, &["FROM", "WHERE"]);
        assert_eq!(groups.len(), 3, "expected SELECT/FROM/WHERE groups");
    }

    #[test]
    fn split_top_level_keyword_groups_ignores_nested_keyword() {
        // FROM inside subquery parens must not split the outer token stream
        let tokens = tokenize_sql("SELECT a, (SELECT b FROM inner_t) FROM outer_t");
        let groups = split_top_level_keyword_groups(&tokens, &["FROM"]);
        assert_eq!(
            groups.len(),
            2,
            "inner FROM must not split outer groups, got {:?}",
            group_words(groups)
        );
    }

    #[test]
    fn split_top_level_keyword_groups_case_insensitive() {
        let tokens = tokenize_sql("select a from t");
        let groups = split_top_level_keyword_groups(&tokens, &["FROM"]);
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn split_top_level_keyword_groups_empty_input_returns_empty() {
        let groups = split_top_level_keyword_groups(&[], &["FROM"]);
        assert!(groups.is_empty());
    }

    // ── paren_depth_after: additional edge cases ──────────────────────────────

    #[test]
    fn paren_depth_after_all_unbalanced_close_parens_is_zero() {
        // Excess `)` tokens must saturate at 0 and not underflow.
        let tokens = [sym(")"), sym(")"), sym(")")];
        assert_eq!(paren_depth_after(&tokens), 0);
    }

    #[test]
    fn paren_depth_after_mismatched_closer_unwinds_to_matching_open() {
        let tokens = [sym("("), sym("["), word("x"), sym(")")];
        assert_eq!(paren_depth_after(&tokens), 0);
    }

    #[test]
    fn split_top_level_symbol_groups_mismatched_closer_restores_root_split() {
        // In "([x), y" the ')' should unwind both '[' and '(' so the comma
        // is treated as top-level delimiter.
        let tokens = tokenize_sql("([x), y");
        let groups = split_top_level_symbol_groups(&tokens, ",");
        assert_eq!(
            groups.len(),
            2,
            "mismatched closer should restore root split, got {:?}",
            group_words(groups)
        );
    }

    // ── split_top_level_symbol_groups: additional edge cases ──────────────────

    #[test]
    fn split_top_level_symbol_groups_skips_leading_delimiter() {
        // A delimiter at the very start produces no empty leading group.
        let tokens = tokenize_sql(",a");
        let groups = split_top_level_symbol_groups(&tokens, ",");
        assert_eq!(
            groups.len(),
            1,
            "leading delimiter must not create an empty prefix group"
        );
    }

    #[test]
    fn split_top_level_symbol_groups_skips_trailing_delimiter() {
        // A delimiter at the very end produces no empty trailing group.
        let tokens = tokenize_sql("a,");
        let groups = split_top_level_symbol_groups(&tokens, ",");
        assert_eq!(
            groups.len(),
            1,
            "trailing delimiter must not create an empty suffix group"
        );
    }

    #[test]
    fn split_top_level_symbol_groups_skips_consecutive_delimiters() {
        // Two consecutive top-level delimiters produce no empty middle group.
        let tokens = tokenize_sql("a,,b");
        let groups = split_top_level_symbol_groups(&tokens, ",");
        assert_eq!(
            groups.len(),
            2,
            "consecutive delimiters must not create empty intermediate segments, got {:?}",
            group_words(groups)
        );
    }

    #[test]
    fn split_top_level_symbol_groups_unbalanced_close_paren_at_root_stays_in_group() {
        // An unmatched `)` at depth 0 does not affect depth (saturates at 0).
        // It is treated as a plain symbol and ends up in the current group.
        let tokens = [
            word("a"),
            sym(","),
            word("b"),
            sym(")"),
            sym(","),
            word("c"),
        ];
        let groups = split_top_level_symbol_groups(&tokens, ",");
        // Expected: [a], [b, )], [c]
        assert_eq!(
            groups.len(),
            3,
            "unmatched ')' at root should not collapse into adjacent groups, got {:?}",
            group_words(groups)
        );
        assert_eq!(
            groups[1].len(),
            2,
            "the unmatched ')' must be included in its own group, got {:?}",
            group_words(groups)
        );
    }

    // ── split_top_level_keyword_groups: additional edge cases ─────────────────

    #[test]
    fn split_top_level_keyword_groups_leading_break_keyword_no_empty_prefix() {
        // When the very first token is a break keyword, no empty group is emitted
        // before it — the keyword simply starts the first group.
        let tokens = tokenize_sql("FROM t");
        let groups = split_top_level_keyword_groups(&tokens, &["FROM"]);
        assert_eq!(
            groups.len(),
            1,
            "no empty group before a leading break keyword"
        );
        assert!(
            matches!(&groups[0][0], SqlToken::Word(w) if w.eq_ignore_ascii_case("FROM")),
            "the break keyword must be the first token of the first group"
        );
    }

    #[test]
    fn split_top_level_keyword_groups_trailing_break_keyword_forms_singleton_group() {
        // When the last token is a break keyword it must become its own group.
        let tokens = tokenize_sql("SELECT a FROM");
        let groups = split_top_level_keyword_groups(&tokens, &["FROM"]);
        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups[1].len(),
            1,
            "trailing break keyword must form a singleton group, got {:?}",
            group_words(groups)
        );
    }

    #[test]
    fn split_top_level_keyword_groups_keyword_is_first_token_of_each_group() {
        // Each break keyword must be preserved as the first token of its group.
        let tokens = tokenize_sql("SELECT a FROM t WHERE x = 1");
        let groups = split_top_level_keyword_groups(&tokens, &["FROM", "WHERE"]);
        assert_eq!(groups.len(), 3);
        assert!(
            matches!(&groups[1][0], SqlToken::Word(w) if w.eq_ignore_ascii_case("FROM")),
            "FROM must be the first token of group[1]"
        );
        assert!(
            matches!(&groups[2][0], SqlToken::Word(w) if w.eq_ignore_ascii_case("WHERE")),
            "WHERE must be the first token of group[2]"
        );
    }

    // ── depth_at / is_top_level_depth / is_depth ──────────────────────────────

    #[test]
    fn depth_at_out_of_range_returns_zero() {
        assert_eq!(depth_at(&[1, 2], 5), 0);
    }

    #[test]
    fn is_top_level_depth_true_when_zero() {
        let tokens = tokenize_sql("SELECT a FROM t");
        let depths = paren_depths(&tokens);
        assert!(is_top_level_depth(&depths, 0));
    }

    #[test]
    fn is_top_level_depth_false_inside_parens() {
        // Tokens: SELECT, (, a, ), FROM, t
        // Depths:       0   0   1  1    0   0
        // index 2 (a) is at depth 1
        let tokens = tokenize_sql("SELECT (a) FROM t");
        let depths = paren_depths(&tokens);
        let paren_open_idx = depths
            .iter()
            .position(|&d| d == 1)
            .expect("depth 1 not found");
        assert!(!is_top_level_depth(&depths, paren_open_idx));
    }
}
