/// Shared SQL text helpers used across execution, formatting, and IntelliSense.

#[inline]
pub(crate) fn is_identifier_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_' || ch == '$' || ch == '#'
}

#[inline]
pub(crate) fn is_identifier_byte(byte: u8) -> bool {
    byte.is_ascii() && is_identifier_char(byte as char)
}

/// Returns true when `text_upper` starts with `keyword` as a standalone token.
///
/// `text_upper` and `keyword` are expected to already be uppercased.
pub(crate) fn starts_with_keyword_token(text_upper: &str, keyword: &str) -> bool {
    if text_upper == keyword {
        return true;
    }
    let Some(rest) = text_upper.strip_prefix(keyword) else {
        return false;
    };
    let Some(next) = rest.chars().next() else {
        return true;
    };
    next.is_whitespace() || matches!(next, ';' | ',' | '(' | ')')
}

/// Returns normalized leading words from a line in uppercase.
pub(crate) fn leading_words_upper(line: &str) -> Vec<String> {
    line.trim_start()
        .split_whitespace()
        .map(|w| {
            w.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                .to_uppercase()
        })
        .filter(|w| !w.is_empty())
        .collect()
}
