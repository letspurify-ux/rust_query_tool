use fltk::{
    enums::Color,
    text::{StyleTableEntry, TextBuffer},
};
use once_cell::sync::Lazy;
use std::collections::HashSet;

use super::intellisense::{ORACLE_FUNCTIONS, SQL_KEYWORDS};
use crate::sql_text;
use crate::ui::font_settings::FontProfile;
use crate::ui::theme;

// Style characters for different token types
pub const STYLE_DEFAULT: char = 'A';
pub const STYLE_KEYWORD: char = 'B';
pub const STYLE_FUNCTION: char = 'C';
pub const STYLE_STRING: char = 'D';
pub const STYLE_COMMENT: char = 'E';
pub const STYLE_NUMBER: char = 'F';
pub const STYLE_OPERATOR: char = 'G';
pub const STYLE_IDENTIFIER: char = 'H';
pub const STYLE_HINT: char = 'I';
pub const STYLE_DATETIME_LITERAL: char = 'J';
pub const STYLE_COLUMN: char = 'K';

static SQL_KEYWORDS_SET: Lazy<HashSet<&'static str>> =
    Lazy::new(|| SQL_KEYWORDS.iter().copied().collect());
static ORACLE_FUNCTIONS_SET: Lazy<HashSet<&'static str>> =
    Lazy::new(|| ORACLE_FUNCTIONS.iter().copied().collect());

pub fn create_style_table_with(profile: FontProfile, size: u32) -> Vec<StyleTableEntry> {
    vec![
        // A - Default text (light gray)
        StyleTableEntry {
            color: theme::text_primary(),
            font: profile.normal,
            size: size as i32,
        },
        // B - SQL Keywords (blue)
        StyleTableEntry {
            color: Color::from_rgb(86, 156, 214),
            font: profile.bold,
            size: size as i32,
        },
        // C - Functions (light purple/magenta)
        StyleTableEntry {
            color: Color::from_rgb(220, 220, 170),
            font: profile.normal,
            size: size as i32,
        },
        // D - Strings (orange)
        StyleTableEntry {
            color: Color::from_rgb(206, 145, 120),
            font: profile.normal,
            size: size as i32,
        },
        // E - Comments (green)
        StyleTableEntry {
            color: Color::from_rgb(106, 153, 85),
            font: profile.italic,
            size: size as i32,
        },
        // F - Numbers (light green)
        StyleTableEntry {
            color: Color::from_rgb(181, 206, 168),
            font: profile.normal,
            size: size as i32,
        },
        // G - Operators (white)
        StyleTableEntry {
            color: theme::text_secondary(),
            font: profile.normal,
            size: size as i32,
        },
        // H - Identifiers/Table names (cyan)
        StyleTableEntry {
            color: Color::from_rgb(78, 201, 176),
            font: profile.normal,
            size: size as i32,
        },
        // I - Hints (gold/yellow)
        StyleTableEntry {
            color: Color::from_rgb(255, 215, 0),
            font: profile.italic,
            size: size as i32,
        },
        // J - DateTime literals (DATE '...', TIMESTAMP '...', INTERVAL '...')
        StyleTableEntry {
            color: Color::from_rgb(255, 160, 122),
            font: profile.normal,
            size: size as i32,
        },
        // K - Columns (near-white)
        StyleTableEntry {
            color: Color::from_rgb(225, 235, 242),
            font: profile.normal,
            size: size as i32,
        },
    ]
}

/// SQL Token types
#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(dead_code)]
enum TokenType {
    Default,
    Keyword,
    Function,
    String,
    Comment,
    Number,
    Operator,
    Identifier,
    Column,
}

impl TokenType {
    fn to_style_char(self) -> char {
        match self {
            TokenType::Default => STYLE_DEFAULT,
            TokenType::Keyword => STYLE_KEYWORD,
            TokenType::Function => STYLE_FUNCTION,
            TokenType::String => STYLE_STRING,
            TokenType::Comment => STYLE_COMMENT,
            TokenType::Number => STYLE_NUMBER,
            TokenType::Operator => STYLE_OPERATOR,
            TokenType::Identifier => STYLE_IDENTIFIER,
            TokenType::Column => STYLE_COLUMN,
        }
    }
}

/// Holds additional identifiers for highlighting (tables, views, etc.)
#[derive(Clone, Default)]
pub struct HighlightData {
    pub tables: Vec<String>,
    pub views: Vec<String>,
    pub columns: Vec<String>,
}

impl HighlightData {
    pub fn new() -> Self {
        Self {
            tables: Vec::new(),
            views: Vec::new(),
            columns: Vec::new(),
        }
    }
}

/// SQL Syntax Highlighter
pub struct SqlHighlighter {
    highlight_data: HighlightData,
    relation_lookup: HashSet<String>,
    column_lookup: HashSet<String>,
}

const HIGHLIGHT_WINDOW_THRESHOLD: usize = 20_000;
const HIGHLIGHT_WINDOW_RADIUS: usize = 8_000;
const MAX_HIGHLIGHT_WINDOWS_PER_PASS: usize = 6;

impl SqlHighlighter {
    pub fn new() -> Self {
        Self {
            highlight_data: HighlightData::new(),
            relation_lookup: HashSet::new(),
            column_lookup: HashSet::new(),
        }
    }

    pub fn set_highlight_data(&mut self, data: HighlightData) {
        self.highlight_data = data;
        self.rebuild_identifier_lookup();
    }

    pub fn get_highlight_data(&self) -> HighlightData {
        self.highlight_data.clone()
    }

    fn rebuild_identifier_lookup(&mut self) {
        let mut relation_lookup = HashSet::new();
        for name in self
            .highlight_data
            .tables
            .iter()
            .chain(self.highlight_data.views.iter())
        {
            relation_lookup.insert(name.to_uppercase());
        }
        self.relation_lookup = relation_lookup;
        self.column_lookup = self
            .highlight_data
            .columns
            .iter()
            .map(|name| name.to_uppercase())
            .collect();
    }

    /// Highlights using a windowed range from the buffer to avoid full-buffer scans.
    pub fn highlight_buffer_window(
        &self,
        buffer: &TextBuffer,
        style_buffer: &mut TextBuffer,
        cursor_pos: usize,
        edited_range: Option<(usize, usize)>,
    ) {
        let text_len = buffer.length().max(0) as usize;
        if text_len == 0 {
            style_buffer.set_text("");
            return;
        }
        if text_len <= HIGHLIGHT_WINDOW_THRESHOLD {
            let text = buffer.text();
            let style_text = self.generate_styles(&text);
            style_buffer.set_text(&style_text);
            return;
        }

        if style_buffer.length() != text_len as i32 {
            let default_styles: String = std::iter::repeat(STYLE_DEFAULT).take(text_len).collect();
            style_buffer.set_text(&default_styles);
        }

        let ranges = select_highlight_ranges(buffer, text_len, cursor_pos, edited_range);
        for (range_start, range_end) in ranges {
            if range_start >= range_end {
                continue;
            }
            let Some(window_text) = buffer.text_range(range_start as i32, range_end as i32) else {
                continue;
            };
            let window_styles = self.generate_styles(&window_text);
            if window_styles.len() != range_end - range_start {
                continue;
            }
            style_buffer.replace(range_start as i32, range_end as i32, &window_styles);
        }
    }

    /// Generates the style string for the given text
    ///
    /// IMPORTANT: FLTK TextBuffer uses byte-based indexing, so the style buffer
    /// must have one style character per byte.
    fn generate_styles(&self, text: &str) -> String {
        let mut styles: Vec<char> = vec![STYLE_DEFAULT; text.len()];
        let bytes = text.as_bytes();
        let mut idx = 0usize;

        while let Some(&byte) = bytes.get(idx) {
            // Check for PROMPT command at the start of a line (SQL*Plus style)
            if idx == 0 || bytes.get(idx.saturating_sub(1)) == Some(&b'\n') {
                let mut scan = idx;
                while bytes.get(scan).map_or(false, |&b| b == b' ' || b == b'\t') {
                    scan += 1;
                }
                if is_prompt_keyword(bytes, scan) {
                    let line_start = idx;
                    let mut end = scan;
                    while let Some(&b) = bytes.get(end) {
                        if b == b'\n' {
                            break;
                        }
                        end += 1;
                    }
                    for b in line_start..end {
                        if let Some(style) = styles.get_mut(b) {
                            *style = STYLE_COMMENT;
                        }
                    }
                    idx = end;
                    continue;
                }
                if is_connect_keyword(bytes, scan) {
                    // SQL*Plus CONNECT lines (e.g. CONNECT user/pass@host:1521/FREE)
                    // contain many punctuation tokens; style only CONNECT keyword.
                    let keyword_end = scan + 7;
                    for b in scan..keyword_end {
                        if let Some(style) = styles.get_mut(b) {
                            *style = STYLE_KEYWORD;
                        }
                    }
                    let mut end = scan;
                    while let Some(&b) = bytes.get(end) {
                        if b == b'\n' {
                            break;
                        }
                        end += 1;
                    }
                    idx = end;
                    continue;
                }
            }

            // Check for single-line comment (--)
            if byte == b'-' && bytes.get(idx + 1) == Some(&b'-') {
                let start = idx;
                idx += 2;
                while let Some(&b) = bytes.get(idx) {
                    if b == b'\n' {
                        break;
                    }
                    idx += 1;
                }
                for b in start..idx {
                    if let Some(style) = styles.get_mut(b) {
                        *style = STYLE_COMMENT;
                    }
                }
                continue;
            }

            // Check for multi-line comment (/* */) or hint (/*+ */)
            if byte == b'/' && bytes.get(idx + 1) == Some(&b'*') {
                let start = idx;
                // Check if this is a hint (/*+ ...)
                let is_hint = bytes.get(idx + 2) == Some(&b'+');
                idx += 2;
                loop {
                    match (bytes.get(idx), bytes.get(idx + 1)) {
                        (Some(&b'*'), Some(&b'/')) => {
                            idx += 2;
                            break;
                        }
                        (Some(_), _) => idx += 1,
                        (None, _) => break,
                    }
                }
                let style_char = if is_hint { STYLE_HINT } else { STYLE_COMMENT };
                for b in start..idx {
                    if let Some(style) = styles.get_mut(b) {
                        *style = style_char;
                    }
                }
                continue;
            }

            // Check for nq-quoted strings: nq'[...]', nq'{...}', etc. (National Character Set)
            if (byte == b'n' || byte == b'N')
                && (bytes.get(idx + 1) == Some(&b'q') || bytes.get(idx + 1) == Some(&b'Q'))
                && bytes.get(idx + 2) == Some(&b'\'')
            {
                if let Some(&delimiter) = bytes.get(idx + 3) {
                    let closing = sql_text::q_quote_closing_byte(delimiter);
                    let start = idx;
                    idx += 4; // Skip nq'[ and find closing delimiter followed by '
                    while idx < bytes.len() {
                        if bytes.get(idx) == Some(&closing) && bytes.get(idx + 1) == Some(&b'\'') {
                            idx += 2; // Include ]'
                            break;
                        }
                        idx += 1;
                    }
                    for b in start..idx {
                        if let Some(style) = styles.get_mut(b) {
                            *style = STYLE_STRING;
                        }
                    }
                    continue;
                }
            }

            // Check for q-quoted strings: q'[...]', q'{...}', etc.
            if (byte == b'q' || byte == b'Q') && bytes.get(idx + 1) == Some(&b'\'') {
                if let Some(&delimiter) = bytes.get(idx + 2) {
                    let closing = sql_text::q_quote_closing_byte(delimiter);
                    let start = idx;
                    idx += 3; // Skip q'[ and find closing delimiter followed by '
                    while idx < bytes.len() {
                        if bytes.get(idx) == Some(&closing) && bytes.get(idx + 1) == Some(&b'\'') {
                            idx += 2; // Include ]'
                            break;
                        }
                        idx += 1;
                    }
                    for b in start..idx {
                        if let Some(style) = styles.get_mut(b) {
                            *style = STYLE_STRING;
                        }
                    }
                    continue;
                }
            }

            // Check for string literals ('...')
            if byte == b'\'' {
                let start = idx;
                idx += 1;
                while let Some(&b) = bytes.get(idx) {
                    if b == b'\'' {
                        if bytes.get(idx + 1) == Some(&b'\'') {
                            idx += 2;
                            continue;
                        }
                        idx += 1;
                        break;
                    }
                    idx += 1;
                }
                for b in start..idx {
                    if let Some(style) = styles.get_mut(b) {
                        *style = STYLE_STRING;
                    }
                }
                continue;
            }

            // Check for quoted identifiers ("..."), including escaped quotes ("")
            if byte == b'"' {
                let start = idx;
                idx += 1;
                while let Some(&b) = bytes.get(idx) {
                    if b == b'"' {
                        if bytes.get(idx + 1) == Some(&b'"') {
                            idx += 2;
                            continue;
                        }
                        idx += 1;
                        break;
                    }
                    idx += 1;
                }
                for b in start..idx {
                    if let Some(style) = styles.get_mut(b) {
                        *style = STYLE_IDENTIFIER;
                    }
                }
                continue;
            }

            // Check for numbers
            if byte.is_ascii_digit()
                || (byte == b'.' && bytes.get(idx + 1).map_or(false, |b| b.is_ascii_digit()))
            {
                let start = idx;
                let mut has_dot = byte == b'.';
                idx += 1;
                while let Some(&next_byte) = bytes.get(idx) {
                    if next_byte.is_ascii_digit() {
                        idx += 1;
                    } else if next_byte == b'.' && !has_dot {
                        has_dot = true;
                        idx += 1;
                    } else {
                        break;
                    }
                }
                for b in start..idx {
                    if let Some(style) = styles.get_mut(b) {
                        *style = STYLE_NUMBER;
                    }
                }
                continue;
            }

            // Check for identifiers/keywords
            if sql_text::is_identifier_start_byte(byte) {
                let start = idx;
                idx += 1;
                while bytes
                    .get(idx)
                    .map_or(false, |&b| sql_text::is_identifier_byte(b))
                {
                    idx += 1;
                }
                let word = text.get(start..idx).unwrap_or("");
                let upper_word = word.to_ascii_uppercase();

                // Check for DATE/TIMESTAMP/INTERVAL literals (e.g., DATE '2024-01-01')
                if matches!(upper_word.as_str(), "DATE" | "TIMESTAMP" | "INTERVAL") {
                    // Skip whitespace to find potential string literal
                    let mut look_ahead = idx;
                    while bytes
                        .get(look_ahead)
                        .map_or(false, |&b| b == b' ' || b == b'\t')
                    {
                        look_ahead += 1;
                    }
                    // Check if followed by a single quote (string literal)
                    if bytes.get(look_ahead) == Some(&b'\'') {
                        // Find the end of the string literal
                        look_ahead += 1;
                        while let Some(&b) = bytes.get(look_ahead) {
                            if b == b'\'' {
                                if bytes.get(look_ahead + 1) == Some(&b'\'') {
                                    look_ahead += 2; // escaped quote
                                    continue;
                                }
                                look_ahead += 1; // include closing quote
                                break;
                            }
                            look_ahead += 1;
                        }
                        // Style the keyword and string as datetime literal
                        for b in start..look_ahead {
                            if let Some(style) = styles.get_mut(b) {
                                *style = STYLE_DATETIME_LITERAL;
                            }
                        }
                        idx = look_ahead;
                        continue;
                    }
                }

                let token_type = self.classify_word(word);
                for b in start..idx {
                    if let Some(style) = styles.get_mut(b) {
                        *style = token_type.to_style_char();
                    }
                }
                continue;
            }

            // Check for operators
            if is_operator_byte(byte) {
                if let Some(style) = styles.get_mut(idx) {
                    *style = STYLE_OPERATOR;
                }
                idx += 1;
                continue;
            }

            idx += 1;
        }

        styles.into_iter().collect()
    }

    /// Classifies a word as keyword, function, identifier, or default
    fn classify_word(&self, word: &str) -> TokenType {
        let upper = word.to_ascii_uppercase();

        // Check if it's a SQL keyword
        if SQL_KEYWORDS_SET.contains(upper.as_str()) {
            return TokenType::Keyword;
        }

        // Check if it's an Oracle function
        if ORACLE_FUNCTIONS_SET.contains(upper.as_str()) {
            return TokenType::Function;
        }

        // Check if it's a known identifier (table, view, column)
        if self.relation_lookup.contains(&upper) {
            return TokenType::Identifier;
        }
        if self.column_lookup.contains(&upper) {
            return TokenType::Column;
        }

        TokenType::Default
    }
}

impl Default for SqlHighlighter {
    fn default() -> Self {
        Self::new()
    }
}

fn windowed_range_from_buffer(
    buffer: &TextBuffer,
    cursor_pos: usize,
    text_len: usize,
    full_text: Option<&str>,
) -> (usize, usize) {
    let start_candidate = cursor_pos.saturating_sub(HIGHLIGHT_WINDOW_RADIUS);
    let end_candidate = (cursor_pos + HIGHLIGHT_WINDOW_RADIUS).min(text_len);

    let mut start = buffer.line_start(start_candidate as i32).max(0) as usize;
    let mut end = buffer.line_end(end_candidate as i32).max(0) as usize;

    let owned_full_text = if full_text.is_none() {
        buffer.text_range(0, text_len as i32)
    } else {
        None
    };
    let full_text = full_text.or_else(|| owned_full_text.as_deref());

    if let Some(text) = full_text {
        let bytes = text.as_bytes();
        let mut idx = 0usize;
        let mut last_ws_before_start: Option<usize> = None;
        let mut first_ws_after_end: Option<usize> = None;

        while idx < bytes.len() {
            let byte = bytes[idx];

            if byte == b'-' && bytes.get(idx + 1) == Some(&b'-') {
                idx += 2;
                while idx < bytes.len() && bytes[idx] != b'\n' {
                    idx += 1;
                }
                continue;
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

            if byte == b'\'' {
                idx += 1;
                while idx < bytes.len() {
                    if bytes[idx] == b'\'' {
                        if bytes.get(idx + 1) == Some(&b'\'') {
                            idx += 2;
                            continue;
                        }
                        idx += 1;
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
                        if bytes.get(idx + 1) == Some(&b'"') {
                            idx += 2;
                            continue;
                        }
                        idx += 1;
                        break;
                    }
                    idx += 1;
                }
                continue;
            }

            if byte.is_ascii_whitespace() {
                if idx <= start_candidate {
                    last_ws_before_start = Some(idx);
                }
                if idx >= end_candidate && first_ws_after_end.is_none() {
                    first_ws_after_end = Some(idx);
                }
            }

            idx += 1;
        }

        if let Some(ws_start) = last_ws_before_start {
            start = ws_start;
        }
        if let Some(ws_end) = first_ws_after_end {
            end = ws_end.saturating_add(1);
        }
    }

    (start.min(text_len), end.min(text_len))
}

fn select_highlight_ranges(
    buffer: &TextBuffer,
    text_len: usize,
    cursor_pos: usize,
    edited_range: Option<(usize, usize)>,
) -> Vec<(usize, usize)> {
    let mut anchors = vec![cursor_pos.min(text_len)];

    if let Some((edit_start, edit_end)) = edited_range {
        let mut start = edit_start.min(text_len);
        let mut end = edit_end.min(text_len);
        if start > end {
            std::mem::swap(&mut start, &mut end);
        }

        if start == 0 && end == text_len {
            return vec![(0, text_len)];
        }

        if start == end {
            anchors.push(start);
        } else {
            let span = end - start;
            let step = (HIGHLIGHT_WINDOW_RADIUS * 2).max(1);
            let mut windows = span.div_ceil(step).max(1);
            windows = windows.min(MAX_HIGHLIGHT_WINDOWS_PER_PASS.saturating_sub(1).max(1));

            for i in 0..=windows {
                let offset = span.saturating_mul(i) / windows;
                anchors.push(start + offset);
            }
        }
    }

    // For multi-window passes, fetch full text once and reuse it when expanding
    // lexical boundaries so we don't rescan/clone the full buffer per window.
    let full_text = if anchors.len() > 1 {
        buffer.text_range(0, text_len as i32)
    } else {
        None
    };

    let mut ranges: Vec<(usize, usize)> = anchors
        .into_iter()
        .map(|anchor| windowed_range_from_buffer(buffer, anchor, text_len, full_text.as_deref()))
        .collect();

    ranges.sort_unstable_by_key(|(start, _)| *start);
    let mut merged: Vec<(usize, usize)> = Vec::with_capacity(ranges.len());
    for (start, end) in ranges {
        if let Some((_, prev_end)) = merged.last_mut() {
            if start <= *prev_end {
                *prev_end = (*prev_end).max(end);
                continue;
            }
        }
        merged.push((start, end));
    }

    if merged.len() > MAX_HIGHLIGHT_WINDOWS_PER_PASS {
        let mut focus_points = vec![cursor_pos.min(text_len)];
        if let Some((edit_start, edit_end)) = edited_range {
            focus_points.push(edit_start.min(text_len));
            focus_points.push(edit_end.min(text_len));
        }
        merged = prioritize_ranges_for_focus(merged, &focus_points, MAX_HIGHLIGHT_WINDOWS_PER_PASS);
    }

    merged
}

fn prioritize_ranges_for_focus(
    mut ranges: Vec<(usize, usize)>,
    focus_points: &[usize],
    max_ranges: usize,
) -> Vec<(usize, usize)> {
    if ranges.len() <= max_ranges {
        return ranges;
    }

    // Keep windows closest to current editing focus (cursor/edited range).
    ranges.sort_unstable_by(|(start_a, end_a), (start_b, end_b)| {
        let dist_a = range_focus_distance(*start_a, *end_a, focus_points);
        let dist_b = range_focus_distance(*start_b, *end_b, focus_points);
        dist_a.cmp(&dist_b).then_with(|| start_a.cmp(start_b))
    });
    ranges.truncate(max_ranges);
    ranges.sort_unstable_by_key(|(start, _)| *start);
    ranges
}

fn range_focus_distance(start: usize, end: usize, focus_points: &[usize]) -> usize {
    focus_points
        .iter()
        .map(|&point| {
            if point < start {
                start - point
            } else if point > end {
                point - end
            } else {
                0
            }
        })
        .min()
        .unwrap_or(0)
}

fn is_operator_byte(byte: u8) -> bool {
    matches!(
        byte,
        b'+' | b'-'
            | b'*'
            | b'/'
            | b'='
            | b'<'
            | b'>'
            | b'!'
            | b'&'
            | b'|'
            | b'^'
            | b'%'
            | b'('
            | b')'
            | b'['
            | b']'
            | b'{'
            | b'}'
            | b','
            | b';'
            | b':'
            | b'.'
    )
}

fn is_prompt_keyword(bytes: &[u8], start: usize) -> bool {
    let Some(end) = start.checked_add(6) else {
        return false;
    };
    if bytes.len() < end {
        return false;
    }
    if !bytes[start..end]
        .iter()
        .zip(b"PROMPT")
        .all(|(b, c)| b.to_ascii_uppercase() == *c)
    {
        return false;
    }
    matches!(
        bytes.get(end),
        None | Some(b' ') | Some(b'\t') | Some(b'\n')
    )
}

fn is_connect_keyword(bytes: &[u8], start: usize) -> bool {
    let Some(end) = start.checked_add(7) else {
        return false;
    };
    if bytes.len() < end {
        return false;
    }
    if !bytes[start..end]
        .iter()
        .zip(b"CONNECT")
        .all(|(b, c)| b.to_ascii_uppercase() == *c)
    {
        return false;
    }
    matches!(
        bytes.get(end),
        None | Some(b' ') | Some(b'\t') | Some(b'\n')
    )
}

#[cfg(test)]
mod syntax_highlight_tests;
