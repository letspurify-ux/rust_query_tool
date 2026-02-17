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
