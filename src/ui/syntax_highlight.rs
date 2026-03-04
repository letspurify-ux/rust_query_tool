use fltk::{
    enums::Color,
    text::{StyleTableEntry, TextBuffer},
};
use once_cell::sync::Lazy;
use std::borrow::Cow;
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
        closing: u8,
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
pub struct WindowHighlightRequest {
    pub start: usize,
    pub end: usize,
    pub text: String,
    pub entry_state: LexerState,
}

#[derive(Clone)]
pub struct WindowHighlightResult {
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

const HIGHLIGHT_WINDOW_THRESHOLD: usize = 20_000;
pub(crate) const WINDOWED_HIGHLIGHT_THRESHOLD: usize = HIGHLIGHT_WINDOW_THRESHOLD;
const HIGHLIGHT_WINDOW_RADIUS: usize = 8_000;
const MAX_HIGHLIGHT_WINDOWS_PER_PASS: usize = 6;
const LARGE_EDIT_SPAN_FOCUS_THRESHOLD: usize = 128_000;
const MAX_HIGHLIGHT_WINDOWS_FOR_LARGE_EDIT: usize = 3;
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

    /// Highlights using a windowed range with optional viewport hint.
    /// `viewport` is the visible byte range `(start, end)` in the editor;
    /// when provided the visible area is always included in the highlight pass.
    #[allow(dead_code)]
    pub fn highlight_buffer_window_viewport(
        &self,
        buffer: &TextBuffer,
        style_buffer: &mut TextBuffer,
        cursor_pos: usize,
        edited_range: Option<(usize, usize)>,
        viewport: Option<(usize, usize)>,
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
            let default_styles = style_bytes_to_string(vec![STYLE_DEFAULT as u8; text_len]);
            style_buffer.set_text(&default_styles);
        }

        let requests = self.prepare_window_highlight_requests(
            buffer,
            style_buffer,
            cursor_pos,
            edited_range,
            viewport,
        );
        let results = self.generate_window_styles(requests);
        for window in results {
            if window.start >= window.end {
                continue;
            }
            style_buffer.replace(window.start as i32, window.end as i32, &window.styles);
        }
    }

    pub fn prepare_window_highlight_requests(
        &self,
        buffer: &TextBuffer,
        style_buffer: &TextBuffer,
        cursor_pos: usize,
        edited_range: Option<(usize, usize)>,
        viewport: Option<(usize, usize)>,
    ) -> Vec<WindowHighlightRequest> {
        let text_len = buffer.length().max(0) as usize;
        if text_len == 0 {
            return Vec::new();
        }

        let ranges = select_highlight_ranges(buffer, text_len, cursor_pos, edited_range, viewport);
        let mut requests = Vec::with_capacity(ranges.len());
        for (range_start, range_end) in ranges {
            if range_start >= range_end {
                continue;
            }
            let entry_state = self.probe_entry_state(buffer, style_buffer, range_start);
            let Some(window_text) = buffer.text_range(range_start as i32, range_end as i32) else {
                continue;
            };
            requests.push(WindowHighlightRequest {
                start: range_start,
                end: range_end,
                text: window_text,
                entry_state,
            });
        }
        requests
    }

    pub fn generate_window_styles(
        &self,
        requests: Vec<WindowHighlightRequest>,
    ) -> Vec<WindowHighlightResult> {
        let mut results = Vec::with_capacity(requests.len());
        for request in requests {
            let expected_len = request.end.saturating_sub(request.start);
            if expected_len == 0 {
                continue;
            }
            let (styles, _exit_state) =
                self.generate_styles_with_state(&request.text, request.entry_state);
            if styles.len() != expected_len {
                continue;
            }
            results.push(WindowHighlightResult {
                start: request.start,
                end: request.end,
                styles,
            });
        }
        results
    }

    /// Probe backward from `pos` to determine the lexer state at that position.
    /// Uses the style buffer for a quick check and falls back to re-lexing a
    /// limited backward window when the position appears to be inside a
    /// multi-line token (comment, string, quoted identifier).
    fn probe_entry_state(
        &self,
        buffer: &TextBuffer,
        style_buffer: &TextBuffer,
        pos: usize,
    ) -> LexerState {
        if pos == 0 {
            return LexerState::Normal;
        }

        // Quick check: read the style byte immediately before the window.
        let prev_pos = (pos - 1) as i32;
        let prev_style = style_buffer
            .text_range(prev_pos, prev_pos + 1)
            .and_then(|s| s.bytes().next())
            .map(|b| b as char)
            .unwrap_or(STYLE_DEFAULT);

        match prev_style {
            // These styles never span across window boundaries.
            // NOTE: STYLE_DATETIME_LITERAL is intentionally excluded here
            // because an unclosed DATE/TIMESTAMP/INTERVAL literal can span a
            // window boundary and must be re-lexed to detect InSingleQuote.
            STYLE_DEFAULT | STYLE_KEYWORD | STYLE_FUNCTION | STYLE_NUMBER | STYLE_OPERATOR
            | STYLE_COLUMN => return LexerState::Normal,
            _ => {}
        }

        // The previous byte looks like a multi-line token (COMMENT, HINT,
        // STRING, or IDENTIFIER).  Re-lex a backward window to determine the
        // exact state.
        let probe_start = pos.saturating_sub(STATE_PROBE_DISTANCE);
        let Some(probe_text) = buffer.text_range(probe_start as i32, pos as i32) else {
            return LexerState::Normal;
        };
        self.generate_styles_with_state(&probe_text, LexerState::Normal)
            .1
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
                        styles[..].fill(STYLE_COMMENT as u8);
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
                    styles[..idx].fill(STYLE_STRING as u8);
                }
                ScanResult::Unterminated { state, .. } => {
                    styles[..].fill(STYLE_STRING as u8);
                    return (style_bytes_to_string(styles), state);
                }
            },
            LexerState::InDoubleQuote => match scan_until_double_quote_end(bytes, idx) {
                ScanResult::Closed { next_idx } => {
                    idx = next_idx;
                    styles[..idx].fill(STYLE_IDENTIFIER as u8);
                }
                ScanResult::Unterminated { state, .. } => {
                    styles[..].fill(STYLE_IDENTIFIER as u8);
                    return (style_bytes_to_string(styles), state);
                }
            },
            LexerState::Normal => {}
        }

        // ── Main scanning loop ─────────────────────────────────────────
        while let Some(&byte) = bytes.get(idx) {
            // Check for PROMPT command at the start of a line (SQL*Plus style)
            if idx == 0 || bytes.get(idx.saturating_sub(1)) == Some(&b'\n') {
                let mut scan = idx;
                while bytes.get(scan).is_some_and(|&b| b == b' ' || b == b'\t') {
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
                            if b == b'\n' {
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
                    if b == b'\n' {
                        break;
                    }
                    idx += 1;
                }
                styles[start..idx].fill(STYLE_COMMENT as u8);
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
                styles[start..idx].fill(comment_kind.style_byte());
                if let ScanResult::Unterminated { state, .. } = scan_result {
                    exit_state = state;
                }
                continue;
            }

            // nq-quoted strings: nq'[...]', nq'{...}', etc.
            if (byte == b'n' || byte == b'N')
                && (bytes.get(idx + 1) == Some(&b'q') || bytes.get(idx + 1) == Some(&b'Q'))
                && bytes.get(idx + 2) == Some(&b'\'')
            {
                if let Some(&delimiter) = bytes.get(idx + 3) {
                    let closing = sql_text::q_quote_closing_byte(delimiter);
                    let start = idx;
                    idx += 4;
                    let scan_result = scan_until_q_quote_end(bytes, idx, closing);
                    idx = match scan_result {
                        ScanResult::Closed { next_idx }
                        | ScanResult::Unterminated { next_idx, .. } => next_idx,
                    };
                    styles[start..idx].fill(STYLE_STRING as u8);
                    if let ScanResult::Unterminated { state, .. } = scan_result {
                        exit_state = state;
                    }
                    continue;
                }
            }

            // q-quoted strings: q'[...]', q'{...}', etc.
            if (byte == b'q' || byte == b'Q') && bytes.get(idx + 1) == Some(&b'\'') {
                if let Some(&delimiter) = bytes.get(idx + 2) {
                    let closing = sql_text::q_quote_closing_byte(delimiter);
                    let start = idx;
                    idx += 3;
                    let scan_result = scan_until_q_quote_end(bytes, idx, closing);
                    idx = match scan_result {
                        ScanResult::Closed { next_idx }
                        | ScanResult::Unterminated { next_idx, .. } => next_idx,
                    };
                    styles[start..idx].fill(STYLE_STRING as u8);
                    if let ScanResult::Unterminated { state, .. } = scan_result {
                        exit_state = state;
                    }
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
                styles[start..idx].fill(STYLE_IDENTIFIER as u8);
                if let ScanResult::Unterminated { state, .. } = scan_result {
                    exit_state = state;
                }
                continue;
            }

            // Numbers
            if byte.is_ascii_digit()
                || (byte == b'.' && bytes.get(idx + 1).is_some_and(|b| b.is_ascii_digit()))
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
                styles[start..idx].fill(STYLE_NUMBER as u8);
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

                let token_type =
                    if word.eq_ignore_ascii_case("PATH") && !is_path_keyword_usage(bytes, idx) {
                        self.classify_non_keyword_word(word)
                    } else {
                        self.classify_word(word)
                    };
                styles[start..idx].fill(token_type.to_style_byte());
                continue;
            }

            // Operators
            if is_operator_byte(byte) {
                styles[idx] = STYLE_OPERATOR as u8;
                idx += 1;
                continue;
            }

            idx += 1;
        }

        (style_bytes_to_string(styles), exit_state)
    }

    /// Classifies a word as keyword, function, identifier, or default
    fn classify_word(&self, word: &str) -> TokenType {
        let upper: Cow<'_, str> = if word.bytes().any(|b| b.is_ascii_lowercase()) {
            Cow::Owned(word.to_ascii_uppercase())
        } else {
            Cow::Borrowed(word)
        };
        let upper = upper.as_ref();

        // Check if it's a SQL keyword
        if SQL_KEYWORDS_SET.contains(upper) {
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
) -> (usize, usize) {
    let start_candidate = cursor_pos.saturating_sub(HIGHLIGHT_WINDOW_RADIUS);
    let end_candidate = (cursor_pos + HIGHLIGHT_WINDOW_RADIUS).min(text_len);

    let start = buffer.line_start(start_candidate as i32).max(0) as usize;
    let end = buffer.line_end(end_candidate as i32).max(0) as usize;

    (start.min(text_len), end.min(text_len))
}

fn select_highlight_ranges(
    buffer: &TextBuffer,
    text_len: usize,
    cursor_pos: usize,
    edited_range: Option<(usize, usize)>,
    viewport: Option<(usize, usize)>,
) -> Vec<(usize, usize)> {
    let mut anchors = vec![cursor_pos.min(text_len)];
    let mut large_edit_focus_only = false;

    // Always include viewport center as an anchor so visible area is highlighted.
    if let Some((vp_start, vp_end)) = viewport {
        let vp_mid = ((vp_start.min(text_len)) + (vp_end.min(text_len))) / 2;
        anchors.push(vp_mid);
    }

    if let Some((edit_start, edit_end)) = edited_range {
        let mut start = edit_start.min(text_len);
        let mut end = edit_end.min(text_len);
        if start > end {
            std::mem::swap(&mut start, &mut end);
        }

        // Never return the entire file as one range — always apply windowing.
        if start == end {
            anchors.push(start);
        } else {
            let span = end - start;
            if span >= LARGE_EDIT_SPAN_FOCUS_THRESHOLD {
                // Very large edits can span the whole document (paste/load).
                // Keep anchors focused near current cursor/viewport instead of
                // distributing windows across the entire edit span.
                large_edit_focus_only = true;
                anchors.push(start);
                anchors.push(end);
            } else {
                let step = (HIGHLIGHT_WINDOW_RADIUS * 2).max(1);
                let mut windows = span.div_ceil(step).max(1);
                windows = windows.min(MAX_HIGHLIGHT_WINDOWS_PER_PASS.saturating_sub(1).max(1));

                for i in 0..=windows {
                    let offset = span.saturating_mul(i) / windows;
                    anchors.push(start + offset);
                }
            }
        }
    }

    let mut ranges: Vec<(usize, usize)> = anchors
        .into_iter()
        .map(|anchor| windowed_range_from_buffer(buffer, anchor, text_len))
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

    if large_edit_focus_only {
        let mut focus_points = vec![cursor_pos.min(text_len)];
        if let Some((vp_start, vp_end)) = viewport {
            let clamped_start = vp_start.min(text_len);
            let clamped_end = vp_end.min(text_len);
            focus_points.push(clamped_start);
            focus_points.push(clamped_end);
            focus_points.push((clamped_start + clamped_end) / 2);
        }
        let max_ranges = MAX_HIGHLIGHT_WINDOWS_FOR_LARGE_EDIT.min(MAX_HIGHLIGHT_WINDOWS_PER_PASS);
        merged = prioritize_ranges_for_focus(merged, &focus_points, max_ranges.max(1));
    } else if merged.len() > MAX_HIGHLIGHT_WINDOWS_PER_PASS {
        let mut focus_points = vec![cursor_pos.min(text_len)];
        if let Some((edit_start, edit_end)) = edited_range {
            focus_points.push(edit_start.min(text_len));
            focus_points.push(edit_end.min(text_len));
        }
        if let Some((vp_start, vp_end)) = viewport {
            focus_points.push(vp_start.min(text_len));
            focus_points.push(vp_end.min(text_len));
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
            } else {
                point.saturating_sub(end)
            }
        })
        .min()
        .unwrap_or(0)
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

fn scan_until_q_quote_end(bytes: &[u8], mut idx: usize, closing: u8) -> ScanResult {
    loop {
        match bytes.get(idx) {
            Some(&b) if b == closing => {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                    return ScanResult::Closed { next_idx: idx };
                }
                idx += 1;
            }
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

fn parse_connect_continuation(bytes: &[u8], connect_end: usize) -> ConnectContinuation {
    let mut idx = connect_end.min(bytes.len());
    let mut state = ConnectContinuationScanState::SkipTrivia;
    let mut token_start = idx;

    while idx <= bytes.len() {
        match state {
            ConnectContinuationScanState::SkipTrivia => {
                while matches!(bytes.get(idx), Some(b' ' | b'\t' | b'\r')) {
                    idx += 1;
                }

                if matches!(bytes.get(idx), None | Some(b'\n')) {
                    return ConnectContinuation::EndOfLine;
                }

                if bytes.get(idx) == Some(&b'-') && bytes.get(idx + 1) == Some(&b'-') {
                    while let Some(&b) = bytes.get(idx) {
                        idx += 1;
                        if b == b'\n' {
                            return ConnectContinuation::EndOfLine;
                        }
                    }
                    return ConnectContinuation::EndOfLine;
                }

                if bytes.get(idx) == Some(&b'/') && bytes.get(idx + 1) == Some(&b'*') {
                    idx += 2;
                    while let Some(_) = bytes.get(idx) {
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
                if b == b'\n' {
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
        Some(b'n' | b'N') => {
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
