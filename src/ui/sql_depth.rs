use crate::ui::sql_editor::SqlToken;

/// Returns the parenthesis depth *before* each token is processed.
///
/// Depth changes for grouping symbols (`()`, `[]`, `{}`) and never goes below zero.
pub(crate) fn paren_depths(tokens: &[SqlToken]) -> Vec<usize> {
    let mut depths = Vec::with_capacity(tokens.len());
    let mut depth = 0usize;

    for token in tokens {
        depths.push(depth);
        apply_paren_token(&mut depth, token);
    }

    depths
}

/// Applies parenthesis depth transition for a single token.
#[inline]
pub(crate) fn apply_paren_token(depth: &mut usize, token: &SqlToken) {
    match token {
        SqlToken::Symbol(sym) if matches!(sym.as_str(), "(" | "[" | "{") => *depth += 1,
        SqlToken::Symbol(sym) if matches!(sym.as_str(), ")" | "]" | "}") => {
            *depth = depth.saturating_sub(1)
        }
        _ => {}
    }
}

/// Returns the final parenthesis depth after all tokens are processed.
pub(crate) fn paren_depth_after(tokens: &[SqlToken]) -> usize {
    let mut depth = 0usize;
    for token in tokens {
        apply_paren_token(&mut depth, token);
    }
    depth
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
    let mut depth = 0usize;

    for token in tokens {
        let at_root = depth == 0;
        if let SqlToken::Symbol(sym) = token {
            if sym == delimiter && at_root {
                if !current.is_empty() {
                    groups.push(std::mem::take(&mut current));
                }
                continue;
            }
        }

        current.push(token);
        apply_paren_token(&mut depth, token);
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
    let mut depth = 0usize;

    for token in tokens {
        let is_break = match token {
            SqlToken::Word(word) => {
                depth == 0
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
        apply_paren_token(&mut depth, token);
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
        let tokens = [
            sym("["),
            sym("{"),
            word("x"),
            sym("}"),
            sym("]"),
            word("y"),
        ];
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
        assert!(paren_depth_after(&tokens) > 0, "unbalanced open paren should yield depth > 0");
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
        assert_eq!(groups.len(), 3, "expected 3 groups, got {:?}", group_words(groups));
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
        assert_eq!(groups.len(), 2, "string literal comma must not split, got {:?}", group_words(groups));
    }

    #[test]
    fn split_top_level_symbol_groups_ignores_nested_comma_in_brackets() {
        let tokens = tokenize_sql("a, [b, c], d");
        let groups = split_top_level_symbol_groups(&tokens, ",");
        assert_eq!(groups.len(), 3, "expected bracket depth to block split, got {:?}", group_words(groups));
    }

    #[test]
    fn split_top_level_symbol_groups_ignores_nested_comma_in_braces() {
        let tokens = tokenize_sql("a, {b, c}, d");
        let groups = split_top_level_symbol_groups(&tokens, ",");
        assert_eq!(groups.len(), 3, "expected brace depth to block split, got {:?}", group_words(groups));
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
        assert_eq!(groups.len(), 2, "inner FROM must not split outer groups, got {:?}", group_words(groups));
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
        let paren_open_idx = depths.iter().position(|&d| d == 1).expect("depth 1 not found");
        assert!(!is_top_level_depth(&depths, paren_open_idx));
    }
}
