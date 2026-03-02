use crate::ui::sql_editor::SqlToken;

/// Returns the parenthesis depth *before* each token is processed.
///
/// Depth changes only for `(` and `)` symbols and never goes below zero.
pub(crate) fn paren_depths(tokens: &[SqlToken]) -> Vec<usize> {
    let mut depths = Vec::with_capacity(tokens.len());
    let mut depth = 0usize;

    for token in tokens {
        depths.push(depth);
        match token {
            SqlToken::Symbol(sym) if sym == "(" => depth += 1,
            SqlToken::Symbol(sym) if sym == ")" => depth = depth.saturating_sub(1),
            _ => {}
        }
    }

    depths
}

/// Applies parenthesis depth transition for a single token.
#[inline]
pub(crate) fn apply_paren_token(depth: &mut usize, token: &SqlToken) {
    match token {
        SqlToken::Symbol(sym) if sym == "(" => *depth += 1,
        SqlToken::Symbol(sym) if sym == ")" => *depth = depth.saturating_sub(1),
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
