use fltk::{enums::Color, text::StyleTableEntry};
use once_cell::sync::Lazy;
use std::borrow::Cow;
use std::collections::HashSet;

use super::intellisense::ORACLE_FUNCTIONS;
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
pub const STYLE_BLOCK_COMMENT: char = 'L';
pub const STYLE_Q_QUOTE_STRING: char = 'M';
pub const STYLE_QUOTED_IDENTIFIER: char = 'N';

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
        // L - Block comments (green)
        StyleTableEntry {
            color: Color::from_rgb(106, 153, 85),
            font: profile.italic,
            size: size as i32,
        },
        // M - Q-Quote strings (orange)
        StyleTableEntry {
            color: Color::from_rgb(206, 145, 120),
            font: profile.normal,
            size: size as i32,
        },
        // N - Quoted identifiers (cyan)
        StyleTableEntry {
            color: Color::from_rgb(78, 201, 176),
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

    fn to_style_byte(self) -> u8 {
        self.to_style_char() as u8
    }
}

/// Lexer state at a given position in the text.
/// Used to correctly highlight windows that start mid-token (e.g., inside a block comment).
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub enum LexerState {
    #[default]
    Normal,
    InBlockComment,
    InHintComment,
    InSingleQuote,
    InQQuote {
        closing: char,
    },
    InDoubleQuote,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanResult {
    Closed { next_idx: usize },
    Unterminated { next_idx: usize, state: LexerState },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BlockCommentKind {
    Regular,
    Hint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConnectContinuation {
    EndOfLine,
    ByClause,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConnectContinuationScanState {
    SkipTrivia,
    ScanToken,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NumberScanState {
    Integer,
    Fraction,
}

impl BlockCommentKind {
    fn from_after_open(bytes: &[u8], idx_after_open: usize) -> Self {
        if bytes.get(idx_after_open) == Some(&b'+') {
            Self::Hint
        } else {
            Self::Regular
        }
    }

    fn style_byte(self) -> u8 {
        match self {
            Self::Regular => STYLE_BLOCK_COMMENT as u8,
            Self::Hint => STYLE_HINT as u8,
        }
    }

    fn closed_style_byte(self) -> u8 {
        match self {
            Self::Regular => STYLE_COMMENT as u8,
            Self::Hint => STYLE_HINT as u8,
        }
    }

    fn unterminated_state(self) -> LexerState {
        match self {
            Self::Regular => LexerState::InBlockComment,
            Self::Hint => LexerState::InHintComment,
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

#[derive(Clone)]
pub struct IncrementalHighlightRequest {
    pub start: usize,
    pub tail_text: String,
    pub previous_tail_styles: String,
    pub entry_state: LexerState,
}

#[derive(Clone)]
pub struct IncrementalHighlightResult {
    pub start: usize,
    pub end: usize,
    pub styles: String,
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

/// Maximum backward probe distance to determine lexer state at a window boundary.
const STATE_PROBE_DISTANCE: usize = 32_768;

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
        let relation_capacity = self.highlight_data.tables.len() + self.highlight_data.views.len();
        let mut relation_lookup = HashSet::with_capacity(relation_capacity);
        for name in self
            .highlight_data
            .tables
            .iter()
            .chain(self.highlight_data.views.iter())
        {
            relation_lookup.insert(name.to_uppercase());
        }
        self.relation_lookup = relation_lookup;
        let mut column_lookup = HashSet::with_capacity(self.highlight_data.columns.len());
        for name in &self.highlight_data.columns {
            column_lookup.insert(name.to_uppercase());
        }
        self.column_lookup = column_lookup;
    }

    pub fn generate_incremental_styles(
        &self,
        request: IncrementalHighlightRequest,
    ) -> Option<IncrementalHighlightResult> {
        if request.tail_text.is_empty() {
            return Some(IncrementalHighlightResult {
                start: request.start,
                end: request.start,
                styles: String::new(),
            });
        }

        let (new_tail_styles, _exit_state) =
            self.generate_styles_with_state(&request.tail_text, request.entry_state);
        if new_tail_styles.len() != request.tail_text.len() {
            return None;
        }

        let previous_tail = request.previous_tail_styles.as_str();
        let mut last_changed = 0usize;
        let mut saw_change = false;
        for (idx, (new_style, old_style)) in new_tail_styles
            .bytes()
            .zip(previous_tail.bytes().chain(std::iter::repeat(0)))
            .enumerate()
        {
            if new_style != old_style {
                saw_change = true;
                last_changed = idx;
            }
        }

        if !saw_change {
            return Some(IncrementalHighlightResult {
                start: request.start,
                end: request.start,
                styles: String::new(),
            });
        }

        let changed_end = request.start.saturating_add(last_changed + 1);
        let styles = new_tail_styles.get(..=last_changed)?.to_owned();
        Some(IncrementalHighlightResult {
            start: request.start,
            end: changed_end,
            styles,
        })
    }

    pub(crate) fn generate_styles_for_window(
        &self,
        text: &str,
        entry_state: LexerState,
    ) -> (String, LexerState) {
        self.generate_styles_with_state(text, entry_state)
    }

    fn resolve_entry_state_by_probe(&self, source_text: &str, pos: usize) -> LexerState {
        let max_pos = clamp_to_utf8_boundary(source_text, pos.min(source_text.len()));
        let mut probe_distance = STATE_PROBE_DISTANCE.max(1);

        loop {
            let raw_start = max_pos.saturating_sub(probe_distance);
            let start = clamp_to_utf8_boundary(source_text, raw_start);
            let end = max_pos;
            if end <= start {
                return LexerState::Normal;
            }

            let Some(probe_text) = source_text.get(start..end) else {
                return LexerState::Normal;
            };
            let state = self
                .generate_styles_with_state(probe_text, LexerState::Normal)
                .1;
            if state != LexerState::Normal || start == 0 {
                return state;
            }

            let next_probe_distance = probe_distance.saturating_mul(2);
            if next_probe_distance <= probe_distance {
                return state;
            }
            probe_distance = next_probe_distance;
        }
    }

    pub fn entry_state_from_continuation_style(&self, style: char) -> LexerState {
        match style {
            STYLE_BLOCK_COMMENT => LexerState::InBlockComment,
            STYLE_HINT => LexerState::InHintComment,
            STYLE_STRING => LexerState::InSingleQuote,
            STYLE_QUOTED_IDENTIFIER => LexerState::InDoubleQuote,
            _ => LexerState::Normal,
        }
    }

    pub fn probe_entry_state_for_style_text(
        &self,
        text: &str,
        style_text: &str,
        pos: usize,
    ) -> LexerState {
        let pos = clamp_to_utf8_boundary(text, pos.min(text.len()));
        if pos == 0 {
            return LexerState::Normal;
        }

        let prev_style = style_text
            .as_bytes()
            .get(pos.saturating_sub(1))
            .copied()
            .map(char::from)
            .unwrap_or(STYLE_DEFAULT);
        if !requires_entry_state_probe(prev_style) {
            return LexerState::Normal;
        }

        self.resolve_entry_state_by_probe(text, pos)
    }

    #[cfg(test)]
    fn probe_entry_state_for_text(&self, text: &str, style_text: &str, pos: usize) -> LexerState {
        let pos = clamp_to_char_boundary(text, pos.min(text.len()));
        if pos == 0 {
            return LexerState::Normal;
        }

        let prev_style = style_text
            .as_bytes()
            .get(pos - 1)
            .copied()
            .map(char::from)
            .unwrap_or(STYLE_DEFAULT);
        if !requires_entry_state_probe(prev_style) {
            return LexerState::Normal;
        }

        self.resolve_entry_state_by_probe_for_text(text, pos)
    }

    #[cfg(test)]
    fn resolve_entry_state_by_probe_for_text(&self, text: &str, pos: usize) -> LexerState {
        let max_pos = clamp_to_char_boundary(text, pos.min(text.len()));
        let mut probe_distance = STATE_PROBE_DISTANCE.max(1);

        loop {
            let raw_start = max_pos.saturating_sub(probe_distance);
            let start = clamp_to_char_boundary(text, raw_start);
            if max_pos <= start {
                return LexerState::Normal;
            }

            let Some(probe_text) = text.get(start..max_pos) else {
                return LexerState::Normal;
            };
            let state = self
                .generate_styles_with_state(probe_text, LexerState::Normal)
                .1;
            if state != LexerState::Normal || start == 0 {
                return state;
            }

            let next_probe_distance = probe_distance.saturating_mul(2);
            if next_probe_distance <= probe_distance {
                return state;
            }
            probe_distance = next_probe_distance;
        }
    }

    /// Generates the style string for the given text, starting from Normal state.
    ///
    /// IMPORTANT: FLTK TextBuffer uses byte-based indexing, so the style buffer
    /// must have one style character per byte.
    fn generate_styles(&self, text: &str) -> String {
        self.generate_styles_with_state(text, LexerState::Normal).0
    }

    pub fn generate_styles_for_text(&self, text: &str) -> String {
        self.generate_styles(text)
    }

    /// State-aware version of `generate_styles`.
    /// Accepts the lexer state at the start of `text` and returns both the
    /// styled output and the lexer state at the end.
    fn generate_styles_with_state(
        &self,
        text: &str,
        initial_state: LexerState,
    ) -> (String, LexerState) {
        let len = text.len();
        if len == 0 {
            return (String::new(), initial_state);
        }
        let mut styles: Vec<u8> = vec![STYLE_DEFAULT as u8; len];
        let bytes = text.as_bytes();
        let mut idx = 0usize;
        let mut expect_alias_identifier = false;
        let mut exit_state = LexerState::Normal;

        // ── Handle continuation of unclosed multi-line tokens ──────────
        match initial_state {
            LexerState::InBlockComment => {
                match scan_until_block_comment_end(bytes, idx, BlockCommentKind::Regular) {
                    ScanResult::Closed { next_idx } => {
                        idx = next_idx;
                        styles[..idx].fill(STYLE_COMMENT as u8);
                    }
                    ScanResult::Unterminated { state, .. } => {
                        styles[..].fill(STYLE_BLOCK_COMMENT as u8);
                        return (style_bytes_to_string(styles), state);
                    }
                }
            }
            LexerState::InHintComment => {
                match scan_until_block_comment_end(bytes, idx, BlockCommentKind::Hint) {
                    ScanResult::Closed { next_idx } => {
                        idx = next_idx;
                        styles[..idx].fill(STYLE_HINT as u8);
                    }
                    ScanResult::Unterminated { state, .. } => {
                        styles[..].fill(STYLE_HINT as u8);
                        return (style_bytes_to_string(styles), state);
                    }
                }
            }
            LexerState::InSingleQuote => match scan_until_single_quote_end(bytes, idx) {
                ScanResult::Closed { next_idx } => {
                    idx = next_idx;
                    styles[..idx].fill(STYLE_STRING as u8);
                }
                ScanResult::Unterminated { state, .. } => {
                    styles[..].fill(STYLE_STRING as u8);
                    return (style_bytes_to_string(styles), state);
                }
            },
            LexerState::InQQuote { closing } => match scan_until_q_quote_end(bytes, idx, closing) {
                ScanResult::Closed { next_idx } => {
                    idx = next_idx;
                    styles[..idx].fill(STYLE_Q_QUOTE_STRING as u8);
                }
                ScanResult::Unterminated { state, .. } => {
                    styles[..].fill(STYLE_Q_QUOTE_STRING as u8);
                    return (style_bytes_to_string(styles), state);
                }
            },
            LexerState::InDoubleQuote => match scan_until_double_quote_end(bytes, idx) {
                ScanResult::Closed { next_idx } => {
                    idx = next_idx;
                    styles[..idx].fill(STYLE_QUOTED_IDENTIFIER as u8);
                }
                ScanResult::Unterminated { state, .. } => {
                    styles[..].fill(STYLE_QUOTED_IDENTIFIER as u8);
                    return (style_bytes_to_string(styles), state);
                }
            },
            LexerState::Normal => {}
        }

        // ── Main scanning loop ─────────────────────────────────────────
        while let Some(&byte) = bytes.get(idx) {
            // Check for PROMPT command at the start of a line (SQL*Plus style)
            if is_line_start(bytes, idx) {
                let mut scan = idx;
                while bytes.get(scan).is_some_and(|&b| b == b' ' || b == b'\t') {
                    scan += 1;
                }
                if is_prompt_keyword(bytes, scan) {
                    let line_start = idx;
                    let mut end = scan;
                    while let Some(&b) = bytes.get(end) {
                        if is_line_terminator(b) {
                            break;
                        }
                        end += 1;
                    }
                    styles[line_start..end].fill(STYLE_COMMENT as u8);
                    idx = end;
                    continue;
                }
                if is_connect_keyword(bytes, scan) {
                    let keyword_end = scan + 7;
                    if parse_connect_continuation(bytes, keyword_end)
                        != ConnectContinuation::ByClause
                    {
                        styles[scan..keyword_end].fill(STYLE_KEYWORD as u8);
                        let mut end = scan;
                        while let Some(&b) = bytes.get(end) {
                            if is_line_terminator(b) {
                                break;
                            }
                            end += 1;
                        }
                        idx = end;
                        continue;
                    }
                }
            }

            // Single-line comment (--)
            if byte == b'-' && bytes.get(idx + 1) == Some(&b'-') {
                let start = idx;
                idx += 2;
                while let Some(&b) = bytes.get(idx) {
                    if is_line_terminator(b) {
                        break;
                    }
                    idx += 1;
                }
                styles[start..idx].fill(STYLE_COMMENT as u8);
                expect_alias_identifier = false;
                continue;
            }

            // Multi-line comment (/* */) or hint (/*+ */)
            if byte == b'/' && bytes.get(idx + 1) == Some(&b'*') {
                let start = idx;
                let comment_kind = BlockCommentKind::from_after_open(bytes, idx + 2);
                idx += 2;
                let scan_result = scan_until_block_comment_end(bytes, idx, comment_kind);
                idx = match scan_result {
                    ScanResult::Closed { next_idx } | ScanResult::Unterminated { next_idx, .. } => {
                        next_idx
                    }
                };
                let style_byte = match scan_result {
                    ScanResult::Closed { .. } => comment_kind.closed_style_byte(),
                    ScanResult::Unterminated { .. } => comment_kind.style_byte(),
                };
                styles[start..idx].fill(style_byte);
                if let ScanResult::Unterminated { state, .. } = scan_result {
                    exit_state = state;
                }
                if text
                    .get(start..idx)
                    .is_some_and(|comment| comment.bytes().any(is_line_terminator))
                {
                    expect_alias_identifier = false;
                }
                continue;
            }

            if is_literal_prefix_boundary(bytes, idx) {
                if let Some(q_quote_start) = detect_q_quote_start(text, idx) {
                    let start = idx;
                    idx += q_quote_start.prefix_len;
                    let scan_result = scan_until_q_quote_end(bytes, idx, q_quote_start.closing);
                    idx = match scan_result {
                        ScanResult::Closed { next_idx }
                        | ScanResult::Unterminated { next_idx, .. } => next_idx,
                    };
                    styles[start..idx].fill(STYLE_Q_QUOTE_STRING as u8);
                    if let ScanResult::Unterminated { state, .. } = scan_result {
                        exit_state = state;
                    }
                    expect_alias_identifier = false;
                    continue;
                }

                if let Some(prefix_len) = detect_prefixed_single_quote_start(text, idx) {
                    let start = idx;
                    idx += prefix_len;
                    let scan_result = scan_until_single_quote_end(bytes, idx);
                    idx = match scan_result {
                        ScanResult::Closed { next_idx }
                        | ScanResult::Unterminated { next_idx, .. } => next_idx,
                    };
                    styles[start..idx].fill(STYLE_STRING as u8);
                    if let ScanResult::Unterminated { state, .. } = scan_result {
                        exit_state = state;
                    }
                    expect_alias_identifier = false;
                    continue;
                }
            }

            // String literals ('...')
            if byte == b'\'' {
                let start = idx;
                idx += 1;
                let scan_result = scan_until_single_quote_end(bytes, idx);
                idx = match scan_result {
                    ScanResult::Closed { next_idx } | ScanResult::Unterminated { next_idx, .. } => {
                        next_idx
                    }
                };
                styles[start..idx].fill(STYLE_STRING as u8);
                if let ScanResult::Unterminated { state, .. } = scan_result {
                    exit_state = state;
                }
                expect_alias_identifier = false;
                continue;
            }

            // Quoted identifiers ("..."), including escaped quotes ("")
            if byte == b'"' {
                let start = idx;
                idx += 1;
                let scan_result = scan_until_double_quote_end(bytes, idx);
                idx = match scan_result {
                    ScanResult::Closed { next_idx } | ScanResult::Unterminated { next_idx, .. } => {
                        next_idx
                    }
                };
                styles[start..idx].fill(STYLE_QUOTED_IDENTIFIER as u8);
                if let ScanResult::Unterminated { state, .. } = scan_result {
                    exit_state = state;
                }
                if expect_alias_identifier {
                    expect_alias_identifier = false;
                }
                continue;
            }

            // Numbers
            if byte.is_ascii_digit()
                || (byte == b'.' && bytes.get(idx + 1).is_some_and(|b| b.is_ascii_digit()))
            {
                let start = idx;
                let mut number_state = if byte == b'.' {
                    NumberScanState::Fraction
                } else {
                    NumberScanState::Integer
                };
                idx += 1;
                while let Some(&next_byte) = bytes.get(idx) {
                    if next_byte.is_ascii_digit() {
                        idx += 1;
                    } else if next_byte == b'.' && number_state == NumberScanState::Integer {
                        number_state = NumberScanState::Fraction;
                        idx += 1;
                    } else {
                        break;
                    }
                }
                styles[start..idx].fill(STYLE_NUMBER as u8);
                expect_alias_identifier = false;
                continue;
            }

            // Identifiers / keywords
            if sql_text::is_identifier_start_byte(byte) {
                let start = idx;
                idx += 1;
                while bytes
                    .get(idx)
                    .is_some_and(|&b| sql_text::is_identifier_byte(b))
                {
                    idx += 1;
                }
                let word = text.get(start..idx).unwrap_or("");

                // DATE / TIMESTAMP / INTERVAL literals
                if word.eq_ignore_ascii_case("DATE")
                    || word.eq_ignore_ascii_case("TIMESTAMP")
                    || word.eq_ignore_ascii_case("INTERVAL")
                {
                    let mut look_ahead = idx;
                    while bytes
                        .get(look_ahead)
                        .is_some_and(|&b| b == b' ' || b == b'\t')
                    {
                        look_ahead += 1;
                    }
                    if bytes.get(look_ahead) == Some(&b'\'') {
                        look_ahead += 1;
                        let scan_result = scan_until_single_quote_end(bytes, look_ahead);
                        look_ahead = match scan_result {
                            ScanResult::Closed { next_idx }
                            | ScanResult::Unterminated { next_idx, .. } => next_idx,
                        };
                        styles[start..look_ahead].fill(STYLE_DATETIME_LITERAL as u8);
                        idx = look_ahead;
                        if let ScanResult::Unterminated { state, .. } = scan_result {
                            exit_state = state;
                        }
                        continue;
                    }
                }

                let treat_control_keyword_as_alias = expect_alias_identifier
                    || should_treat_control_keyword_as_implicit_alias(
                        text, bytes, start, idx, word,
                    );
                let token_type = if expect_alias_identifier
                    || should_treat_function_name_as_identifier(text, bytes, start, idx, word)
                {
                    self.classify_identifier_like_word(word)
                } else if word.eq_ignore_ascii_case("PATH") && !is_path_keyword_usage(bytes, idx) {
                    self.classify_non_keyword_word(word)
                } else {
                    self.classify_word(word, treat_control_keyword_as_alias)
                };
                styles[start..idx].fill(token_type.to_style_byte());
                expect_alias_identifier = word.eq_ignore_ascii_case("AS");
                continue;
            }

            // Operators
            if is_operator_byte(byte) {
                styles[idx] = STYLE_OPERATOR as u8;
                idx += 1;
                expect_alias_identifier = false;
                continue;
            }

            if is_line_terminator(byte) {
                expect_alias_identifier = false;
            }
            idx += 1;
        }

        (style_bytes_to_string(styles), exit_state)
    }

    /// Classifies a word as keyword, function, identifier, or default
    fn classify_word(&self, word: &str, treat_control_keyword_as_alias: bool) -> TokenType {
        let upper: Cow<'_, str> = if word.bytes().any(|b| b.is_ascii_lowercase()) {
            Cow::Owned(word.to_ascii_uppercase())
        } else {
            Cow::Borrowed(word)
        };
        let upper = upper.as_ref();

        if treat_control_keyword_as_alias && sql_text::is_plsql_control_keyword(upper) {
            return self.classify_non_keyword_word(upper);
        }

        // Check if it's a SQL keyword
        if sql_text::is_oracle_sql_keyword(upper) {
            return TokenType::Keyword;
        }

        // Check if it's an Oracle function
        if ORACLE_FUNCTIONS_SET.contains(upper) {
            return TokenType::Function;
        }

        // Check if it's a known identifier (table, view, column)
        if self.relation_lookup.contains(upper) {
            return TokenType::Identifier;
        }
        if self.column_lookup.contains(upper) {
            return TokenType::Column;
        }

        TokenType::Default
    }

    fn classify_non_keyword_word(&self, word: &str) -> TokenType {
        let upper: Cow<'_, str> = if word.bytes().any(|b| b.is_ascii_lowercase()) {
            Cow::Owned(word.to_ascii_uppercase())
        } else {
            Cow::Borrowed(word)
        };
        let upper = upper.as_ref();

        if ORACLE_FUNCTIONS_SET.contains(upper) {
            return TokenType::Function;
        }
        if self.relation_lookup.contains(upper) {
            return TokenType::Identifier;
        }
        if self.column_lookup.contains(upper) {
            return TokenType::Column;
        }

        TokenType::Default
    }

    fn classify_identifier_like_word(&self, word: &str) -> TokenType {
        let upper: Cow<'_, str> = if word.bytes().any(|b| b.is_ascii_lowercase()) {
            Cow::Owned(word.to_ascii_uppercase())
        } else {
            Cow::Borrowed(word)
        };
        let upper = upper.as_ref();

        if self.relation_lookup.contains(upper) {
            return TokenType::Identifier;
        }
        if self.column_lookup.contains(upper) {
            return TokenType::Column;
        }

        TokenType::Default
    }
}

fn should_treat_control_keyword_as_implicit_alias(
    text: &str,
    bytes: &[u8],
    word_start: usize,
    word_end: usize,
    word: &str,
) -> bool {
    if !sql_text::is_plsql_control_keyword(word) || word.eq_ignore_ascii_case("THEN") {
        return false;
    }
    if has_significant_line_break_before(bytes, word_start) {
        return false;
    }

    let Some(next_kind) = next_significant_token_kind(bytes, word_end) else {
        return false;
    };
    if !matches!(
        next_kind,
        SignificantTokenKind::Comma
            | SignificantTokenKind::Dot
            | SignificantTokenKind::ClauseWord
            | SignificantTokenKind::RightParen
    ) {
        return false;
    }

    let Some(prev_kind) = prev_significant_token_kind(text, bytes, word_start) else {
        return false;
    };

    matches!(
        prev_kind,
        SignificantTokenKind::Identifier
            | SignificantTokenKind::Number
            | SignificantTokenKind::String
            | SignificantTokenKind::RightParen
            | SignificantTokenKind::ClauseWord
            | SignificantTokenKind::Comma
    )
}

fn has_significant_line_break_before(bytes: &[u8], mut idx: usize) -> bool {
    let mut saw_line_break = false;

    while idx > 0 {
        let Some(&prev) = bytes.get(idx - 1) else {
            break;
        };
        if prev == b' ' || prev == b'\t' {
            idx -= 1;
            continue;
        }
        if is_line_terminator(prev) {
            saw_line_break = true;
            idx -= 1;
            continue;
        }
        if idx >= 2 && bytes.get(idx - 2) == Some(&b'-') && bytes.get(idx - 1) == Some(&b'-') {
            idx -= 2;
            while idx > 0 {
                let Some(&comment_byte) = bytes.get(idx - 1) else {
                    break;
                };
                idx -= 1;
                if is_line_terminator(comment_byte) {
                    saw_line_break = true;
                    break;
                }
            }
            continue;
        }
        if idx >= 2 && bytes.get(idx - 2) == Some(&b'*') && bytes.get(idx - 1) == Some(&b'/') {
            idx -= 2;
            let mut comment_has_line_break = false;
            while idx > 1 {
                let Some(&comment_byte) = bytes.get(idx - 1) else {
                    break;
                };
                if is_line_terminator(comment_byte) {
                    comment_has_line_break = true;
                }
                if bytes.get(idx - 2) == Some(&b'/') && bytes.get(idx - 1) == Some(&b'*') {
                    idx -= 2;
                    break;
                }
                idx -= 1;
            }
            saw_line_break |= comment_has_line_break;
            continue;
        }

        break;
    }

    saw_line_break
}

fn should_treat_function_name_as_identifier(
    text: &str,
    bytes: &[u8],
    word_start: usize,
    word_end: usize,
    word: &str,
) -> bool {
    let upper: Cow<'_, str> = if word.bytes().any(|b| b.is_ascii_lowercase()) {
        Cow::Owned(word.to_ascii_uppercase())
    } else {
        Cow::Borrowed(word)
    };
    let upper = upper.as_ref();

    if !ORACLE_FUNCTIONS_SET.contains(upper) {
        return false;
    }

    if matches!(
        next_significant_token_kind(bytes, word_end),
        Some(SignificantTokenKind::Dot)
    ) {
        return true;
    }

    if matches!(
        prev_significant_token_kind(text, bytes, word_start),
        Some(SignificantTokenKind::Dot)
    ) {
        return true;
    }

    prev_significant_word_upper(text, bytes, word_start)
        .is_some_and(|prev_word| is_relation_identifier_context_word(prev_word.as_str()))
}

fn prev_significant_word_upper(text: &str, bytes: &[u8], mut idx: usize) -> Option<String> {
    while idx > 0 {
        let prev = *bytes.get(idx - 1)?;
        if prev == b' ' || prev == b'\t' || prev == b'\r' || prev == b'\n' {
            idx -= 1;
            continue;
        }
        if idx >= 2 && bytes.get(idx - 2) == Some(&b'-') && bytes.get(idx - 1) == Some(&b'-') {
            idx -= 2;
            while idx > 0
                && bytes
                    .get(idx - 1)
                    .copied()
                    .is_some_and(|byte| !is_line_terminator(byte))
            {
                idx -= 1;
            }
            continue;
        }
        if idx >= 2 && bytes.get(idx - 2) == Some(&b'*') && bytes.get(idx - 1) == Some(&b'/') {
            idx -= 2;
            while idx > 1 {
                if bytes.get(idx - 2) == Some(&b'/') && bytes.get(idx - 1) == Some(&b'*') {
                    idx -= 2;
                    break;
                }
                idx -= 1;
            }
            continue;
        }
        if !sql_text::is_identifier_byte(prev) {
            return None;
        }

        let mut start = idx - 1;
        while start > 0
            && bytes
                .get(start - 1)
                .is_some_and(|&b| sql_text::is_identifier_byte(b))
        {
            start -= 1;
        }

        let word = text.get(start..idx)?;
        let upper: Cow<'_, str> = if word.bytes().any(|b| b.is_ascii_lowercase()) {
            Cow::Owned(word.to_ascii_uppercase())
        } else {
            Cow::Borrowed(word)
        };
        return Some(upper.into_owned());
    }

    None
}

fn is_relation_identifier_context_word(word: &str) -> bool {
    matches!(
        word,
        "WITH" | "FROM" | "JOIN" | "UPDATE" | "INTO" | "USING" | "TABLE"
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SignificantTokenKind {
    Identifier,
    Number,
    String,
    RightParen,
    Comma,
    Dot,
    ClauseWord,
}

fn next_significant_token_kind(bytes: &[u8], mut idx: usize) -> Option<SignificantTokenKind> {
    while let Some(&byte) = bytes.get(idx) {
        if byte == b' ' || byte == b'\t' || byte == b'\r' || byte == b'\n' {
            idx += 1;
            continue;
        }
        if byte == b'-' && bytes.get(idx + 1) == Some(&b'-') {
            idx += 2;
            while let Some(&b) = bytes.get(idx) {
                idx += 1;
                if is_line_terminator(b) {
                    break;
                }
            }
            continue;
        }
        if byte == b'/' && bytes.get(idx + 1) == Some(&b'*') {
            idx += 2;
            while bytes.get(idx).is_some() {
                if bytes.get(idx) == Some(&b'*') && bytes.get(idx + 1) == Some(&b'/') {
                    idx += 2;
                    break;
                }
                idx += 1;
            }
            continue;
        }

        return match byte {
            b',' => Some(SignificantTokenKind::Comma),
            b'.' => Some(SignificantTokenKind::Dot),
            b')' => Some(SignificantTokenKind::RightParen),
            b'A'..=b'Z' | b'a'..=b'z' | b'_' | b'$' | b'#' => {
                let start = idx;
                idx += 1;
                while bytes
                    .get(idx)
                    .is_some_and(|&b| sql_text::is_identifier_byte(b))
                {
                    idx += 1;
                }
                let word = std::str::from_utf8(bytes.get(start..idx)?).ok()?;
                let upper = word.to_ascii_uppercase();
                if sql_text::is_oracle_sql_keyword(upper.as_str()) {
                    Some(SignificantTokenKind::ClauseWord)
                } else {
                    Some(SignificantTokenKind::Identifier)
                }
            }
            _ => None,
        };
    }

    None
}

fn prev_significant_token_kind(
    text: &str,
    bytes: &[u8],
    mut idx: usize,
) -> Option<SignificantTokenKind> {
    while idx > 0 {
        let prev = *bytes.get(idx - 1)?;
        if prev == b' ' || prev == b'\t' || prev == b'\r' || prev == b'\n' {
            idx -= 1;
            continue;
        }
        if idx >= 2 && bytes.get(idx - 2) == Some(&b'-') && bytes.get(idx - 1) == Some(&b'-') {
            idx -= 2;
            while idx > 0
                && bytes
                    .get(idx - 1)
                    .copied()
                    .is_some_and(|byte| !is_line_terminator(byte))
            {
                idx -= 1;
            }
            continue;
        }
        if idx >= 2 && bytes.get(idx - 2) == Some(&b'*') && bytes.get(idx - 1) == Some(&b'/') {
            idx -= 2;
            while idx > 1 {
                if bytes.get(idx - 2) == Some(&b'/') && bytes.get(idx - 1) == Some(&b'*') {
                    idx -= 2;
                    break;
                }
                idx -= 1;
            }
            continue;
        }

        if prev == b')' {
            return Some(SignificantTokenKind::RightParen);
        }
        if prev == b'.' {
            return Some(SignificantTokenKind::Dot);
        }
        if prev == b',' {
            return Some(SignificantTokenKind::Comma);
        }
        if prev.is_ascii_digit() {
            return Some(SignificantTokenKind::Number);
        }
        if prev == b'\'' || prev == b'"' {
            return Some(SignificantTokenKind::String);
        }
        if sql_text::is_identifier_byte(prev) {
            let mut start = idx - 1;
            while start > 0
                && bytes
                    .get(start - 1)
                    .is_some_and(|&b| sql_text::is_identifier_byte(b))
            {
                start -= 1;
            }
            let word = text.get(start..idx)?;
            let upper = word.to_ascii_uppercase();
            if sql_text::is_oracle_sql_keyword(upper.as_str()) {
                return Some(SignificantTokenKind::ClauseWord);
            }
            return Some(SignificantTokenKind::Identifier);
        }

        return None;
    }

    None
}

impl Default for SqlHighlighter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
fn clamp_to_char_boundary(text: &str, idx: usize) -> usize {
    clamp_to_utf8_boundary(text, idx)
}

fn scan_until_block_comment_end(
    bytes: &[u8],
    mut idx: usize,
    comment_kind: BlockCommentKind,
) -> ScanResult {
    loop {
        match (bytes.get(idx), bytes.get(idx + 1)) {
            (Some(&b'*'), Some(&b'/')) => {
                idx += 2;
                return ScanResult::Closed { next_idx: idx };
            }
            (Some(_), _) => idx += 1,
            (None, _) => {
                return ScanResult::Unterminated {
                    next_idx: idx,
                    state: comment_kind.unterminated_state(),
                };
            }
        }
    }
}

fn scan_until_single_quote_end(bytes: &[u8], mut idx: usize) -> ScanResult {
    loop {
        match bytes.get(idx) {
            Some(&b'\'') => {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                } else {
                    idx += 1;
                    return ScanResult::Closed { next_idx: idx };
                }
            }
            Some(_) => idx += 1,
            None => {
                return ScanResult::Unterminated {
                    next_idx: idx,
                    state: LexerState::InSingleQuote,
                };
            }
        }
    }
}

fn scan_until_q_quote_end(bytes: &[u8], mut idx: usize, closing: char) -> ScanResult {
    let mut closing_buf = [0u8; 4];
    let closing_bytes = closing.encode_utf8(&mut closing_buf).as_bytes();
    loop {
        if bytes
            .get(idx..)
            .is_some_and(|remaining| remaining.starts_with(closing_bytes))
            && bytes.get(idx + closing_bytes.len()) == Some(&b'\'')
        {
            idx += closing_bytes.len() + 1;
            return ScanResult::Closed { next_idx: idx };
        }

        match bytes.get(idx) {
            Some(_) => idx += 1,
            None => {
                return ScanResult::Unterminated {
                    next_idx: idx,
                    state: LexerState::InQQuote { closing },
                };
            }
        }
    }
}

fn scan_until_double_quote_end(bytes: &[u8], mut idx: usize) -> ScanResult {
    loop {
        match bytes.get(idx) {
            Some(&b'"') => {
                if bytes.get(idx + 1) == Some(&b'"') {
                    idx += 2;
                } else {
                    idx += 1;
                    return ScanResult::Closed { next_idx: idx };
                }
            }
            Some(_) => idx += 1,
            None => {
                return ScanResult::Unterminated {
                    next_idx: idx,
                    state: LexerState::InDoubleQuote,
                };
            }
        }
    }
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

fn requires_entry_state_probe(style: char) -> bool {
    matches!(
        style,
        STYLE_COMMENT
            | STYLE_BLOCK_COMMENT
            | STYLE_STRING
            | STYLE_Q_QUOTE_STRING
            | STYLE_QUOTED_IDENTIFIER
            | STYLE_HINT
    )
}

fn clamp_to_utf8_boundary(text: &str, idx: usize) -> usize {
    let mut clamped = idx.min(text.len());
    while clamped > 0 && !text.is_char_boundary(clamped) {
        clamped -= 1;
    }
    clamped
}

fn is_literal_prefix_boundary(bytes: &[u8], idx: usize) -> bool {
    idx == 0
        || !bytes
            .get(idx - 1)
            .copied()
            .is_some_and(sql_text::is_identifier_byte)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct QQuoteStart {
    prefix_len: usize,
    closing: char,
}

fn detect_q_quote_start(text: &str, idx: usize) -> Option<QQuoteStart> {
    let suffix = text.get(idx..)?;
    let mut chars = suffix.char_indices();
    let (_, first) = chars.next()?;

    let delimiter = match first {
        'q' | 'Q' => {
            let (_, quote) = chars.next()?;
            if quote != '\'' {
                return None;
            }
            chars.next()?
        }
        'n' | 'N' | 'u' | 'U' => {
            let (_, q_char) = chars.next()?;
            let (_, quote) = chars.next()?;
            if !matches!(q_char, 'q' | 'Q') || quote != '\'' {
                return None;
            }
            chars.next()?
        }
        _ => return None,
    };

    let (delimiter_offset, delimiter_char) = delimiter;
    if !sql_text::is_valid_q_quote_delimiter(delimiter_char) {
        return None;
    }

    Some(QQuoteStart {
        prefix_len: delimiter_offset + delimiter_char.len_utf8(),
        closing: sql_text::q_quote_closing(delimiter_char),
    })
}

fn detect_prefixed_single_quote_start(text: &str, idx: usize) -> Option<usize> {
    let suffix = text.get(idx..)?;
    let mut chars = suffix.char_indices();
    let (_, first) = chars.next()?;
    if !matches!(first, 'n' | 'N' | 'b' | 'B' | 'x' | 'X' | 'u' | 'U') {
        return None;
    }

    let (second_offset, second_char) = chars.next()?;
    if matches!(first, 'u' | 'U') && second_char == '&' {
        let (quote_offset, quote_char) = chars.next()?;
        if quote_char != '\'' {
            return None;
        }
        return Some(quote_offset + quote_char.len_utf8());
    }

    if second_char == '\'' {
        return Some(second_offset + second_char.len_utf8());
    }

    None
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
        None | Some(b' ') | Some(b'\t') | Some(b'\n') | Some(b'\r')
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
        None | Some(b' ') | Some(b'\t') | Some(b'\n') | Some(b'\r')
    )
}

fn is_line_start(bytes: &[u8], idx: usize) -> bool {
    idx == 0
        || matches!(
            bytes.get(idx.saturating_sub(1)),
            Some(b'\n') | Some(b'\r')
        )
}

fn is_line_terminator(byte: u8) -> bool {
    matches!(byte, b'\n' | b'\r')
}

fn parse_connect_continuation(bytes: &[u8], connect_end: usize) -> ConnectContinuation {
    let mut idx = connect_end.min(bytes.len());
    let mut state = ConnectContinuationScanState::SkipTrivia;
    let mut token_start = idx;

    while idx <= bytes.len() {
        match state {
            ConnectContinuationScanState::SkipTrivia => {
                while matches!(bytes.get(idx), Some(b' ' | b'\t' | b'\r' | b'\n')) {
                    idx += 1;
                }

                if bytes.get(idx).is_none() {
                    return ConnectContinuation::EndOfLine;
                }

                if bytes.get(idx) == Some(&b'-') && bytes.get(idx + 1) == Some(&b'-') {
                    while let Some(&b) = bytes.get(idx) {
                        idx += 1;
                        if is_line_terminator(b) {
                            break;
                        }
                    }
                    continue;
                }

                if bytes.get(idx) == Some(&b'/') && bytes.get(idx + 1) == Some(&b'*') {
                    idx += 2;
                    while bytes.get(idx).is_some() {
                        if bytes.get(idx) == Some(&b'*') && bytes.get(idx + 1) == Some(&b'/') {
                            idx += 2;
                            break;
                        }
                        idx += 1;
                    }
                    continue;
                }

                token_start = idx;
                state = ConnectContinuationScanState::ScanToken;
            }
            ConnectContinuationScanState::ScanToken => {
                while bytes
                    .get(idx)
                    .is_some_and(|&byte| sql_text::is_identifier_byte(byte))
                {
                    idx += 1;
                }
                if idx == token_start {
                    return ConnectContinuation::Other;
                }

                let Some(token) = bytes.get(token_start..idx) else {
                    return ConnectContinuation::Other;
                };

                if token.eq_ignore_ascii_case(b"BY") {
                    if matches!(bytes.get(idx), None | Some(b' ' | b'\t' | b'\n' | b'\r')) {
                        return ConnectContinuation::ByClause;
                    }
                    return ConnectContinuation::Other;
                }

                return ConnectContinuation::Other;
            }
        }
    }

    ConnectContinuation::EndOfLine
}

fn skip_trivia_and_comments(bytes: &[u8], mut idx: usize) -> usize {
    while idx < bytes.len() {
        while matches!(bytes.get(idx), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            idx += 1;
        }

        if bytes.get(idx) == Some(&b'-') && bytes.get(idx + 1) == Some(&b'-') {
            idx += 2;
            while let Some(&b) = bytes.get(idx) {
                idx += 1;
                if is_line_terminator(b) {
                    break;
                }
            }
            continue;
        }

        if bytes.get(idx) == Some(&b'/') && bytes.get(idx + 1) == Some(&b'*') {
            idx += 2;
            while idx < bytes.len() {
                if bytes.get(idx) == Some(&b'*') && bytes.get(idx + 1) == Some(&b'/') {
                    idx += 2;
                    break;
                }
                idx += 1;
            }
            continue;
        }

        break;
    }

    idx
}

fn is_path_keyword_usage(bytes: &[u8], word_end: usize) -> bool {
    let look_ahead = skip_trivia_and_comments(bytes, word_end);
    match bytes.get(look_ahead) {
        Some(b'\'') => true,
        Some(b'q' | b'Q') => bytes.get(look_ahead + 1) == Some(&b'\''),
        Some(b'n' | b'N' | b'u' | b'U') => {
            matches!(bytes.get(look_ahead + 1), Some(b'q' | b'Q'))
                && bytes.get(look_ahead + 2) == Some(&b'\'')
        }
        _ => false,
    }
}

fn style_bytes_to_string(styles: Vec<u8>) -> String {
    // Styles use ASCII tags ('A'..'K'), so UTF-8 validation is unnecessary.
    // In debug builds, verify the invariant so programming errors surface immediately.
    debug_assert!(
        styles.iter().all(|&b| b.is_ascii()),
        "style bytes must be valid ASCII"
    );
    // SAFETY: All style bytes are ASCII character codes ('A'..'K') which are
    // valid single-byte UTF-8 code points.
    unsafe { String::from_utf8_unchecked(styles) }
}

#[cfg(test)]
mod syntax_highlight_tests;
