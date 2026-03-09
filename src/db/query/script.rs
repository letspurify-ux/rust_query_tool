use crate::db::session::{BindDataType, ComputeMode};
use crate::sql_parser_engine::{LineBoundaryAction, SqlParserEngine};
use crate::sql_text;

use super::{FormatItem, QueryExecutor, ScriptItem, ToolCommand};

// ── TopLevelScanner ─────────────────────────────────────────────────────────
//
// Byte-based SQL scanner that yields top-level tokens while skipping
// comments, string literals, and tracking parenthesis depth.
//
// This replaces 8+ near-identical scanning state machines that were
// duplicated across `find_top_level_keyword`, `has_top_level_identifier_token`,
// `find_first_top_level_comma`, `select_clause_has_top_level_aggregate`,
// `select_clause_has_top_level_analytic`, `is_single_table_from_clause`,
// and `select_clause_has_distinct_or_unique`.

/// Token yielded by [`TopLevelScanner`] at parenthesis depth 0.
enum ScanToken<'a> {
    /// An identifier or keyword at depth 0.
    Word { text: &'a str, start: usize },
    /// A non-structural symbol at depth 0 (e.g. `,`, `*`, `;`).
    Symbol { byte: u8, pos: usize },
}

/// Scans SQL text yielding only meaningful tokens at parenthesis depth 0,
/// automatically skipping comments (`--`, `/* */`), string literals (`'...'`),
/// quoted identifiers (`"..."`), and nested parenthesised expressions.
///
/// Uses byte-level iteration (no `Vec<(usize, char)>` allocation) since all
/// SQL delimiters and keywords are ASCII.
struct TopLevelScanner<'a> {
    sql: &'a str,
    bytes: &'a [u8],
    pos: usize,
    depth: usize,
}

impl<'a> TopLevelScanner<'a> {
    #[inline]
    fn new(sql: &'a str) -> Self {
        Self {
            sql,
            bytes: sql.as_bytes(),
            pos: 0,
            depth: 0,
        }
    }

    /// Peek at the next non-whitespace byte without consuming it.
    fn peek_next_non_ws_byte(&self) -> Option<u8> {
        let mut pos = self.pos;
        while pos < self.bytes.len() {
            if !self.bytes[pos].is_ascii_whitespace() {
                return Some(self.bytes[pos]);
            }
            pos += 1;
        }
        None
    }

    fn skip_single_quoted(&mut self) {
        self.pos += 1;
        while self.pos < self.bytes.len() {
            if self.bytes[self.pos] == b'\'' {
                self.pos += 1;
                if self.pos < self.bytes.len() && self.bytes[self.pos] == b'\'' {
                    self.pos += 1;
                    continue;
                }
                return;
            }
            self.pos += 1;
        }
    }

    fn skip_double_quoted(&mut self) {
        self.pos += 1;
        while self.pos < self.bytes.len() {
            if self.bytes[self.pos] == b'"' {
                self.pos += 1;
                if self.pos < self.bytes.len() && self.bytes[self.pos] == b'"' {
                    self.pos += 1;
                    continue;
                }
                return;
            }
            self.pos += 1;
        }
    }

    fn skip_q_quoted(&mut self, delimiter: u8, prefix_len: usize) {
        self.pos += prefix_len;
        let end_delimiter = sql_text::q_quote_closing_byte(delimiter);
        while self.pos + 1 < self.bytes.len() {
            if self.bytes[self.pos] == end_delimiter && self.bytes[self.pos + 1] == b'\'' {
                self.pos += 2;
                return;
            }
            self.pos += 1;
        }
        self.pos = self.bytes.len();
    }

    fn skip_line_comment(&mut self) {
        self.pos += 2;
        while self.pos < self.bytes.len() && self.bytes[self.pos] != b'\n' {
            self.pos += 1;
        }
    }

    fn skip_block_comment(&mut self) {
        self.pos += 2;
        while self.pos + 1 < self.bytes.len() {
            if self.bytes[self.pos] == b'*' && self.bytes[self.pos + 1] == b'/' {
                self.pos += 2;
                return;
            }
            self.pos += 1;
        }
        self.pos = self.bytes.len();
    }
}

impl<'a> Iterator for TopLevelScanner<'a> {
    type Item = ScanToken<'a>;

    fn next(&mut self) -> Option<ScanToken<'a>> {
        while self.pos < self.bytes.len() {
            let b = self.bytes[self.pos];

            if (b == b'n' || b == b'N' || b == b'u' || b == b'U')
                && self
                    .bytes
                    .get(self.pos + 1)
                    .is_some_and(|&next_b| next_b == b'q' || next_b == b'Q')
                && self.bytes.get(self.pos + 2) == Some(&b'\'')
            {
                if let Some(&delimiter) = self.bytes.get(self.pos + 3) {
                    if sql_text::is_valid_q_quote_delimiter_byte(delimiter) {
                        self.skip_q_quoted(delimiter, 4);
                        continue;
                    }
                }
            }

            if (b == b'q' || b == b'Q') && self.bytes.get(self.pos + 1) == Some(&b'\'') {
                if let Some(&delimiter) = self.bytes.get(self.pos + 2) {
                    if sql_text::is_valid_q_quote_delimiter_byte(delimiter) {
                        self.skip_q_quoted(delimiter, 3);
                        continue;
                    }
                }
            }

            if b == b'-' && self.bytes.get(self.pos + 1) == Some(&b'-') {
                self.skip_line_comment();
                continue;
            }
            if b == b'/' && self.bytes.get(self.pos + 1) == Some(&b'*') {
                self.skip_block_comment();
                continue;
            }
            if b == b'\'' {
                self.skip_single_quoted();
                continue;
            }
            if b == b'"' {
                self.skip_double_quoted();
                continue;
            }

            if b == b'(' {
                self.depth += 1;
                self.pos += 1;
                continue;
            }
            if b == b')' {
                self.depth = self.depth.saturating_sub(1);
                self.pos += 1;
                continue;
            }

            if b.is_ascii_whitespace() {
                self.pos += 1;
                continue;
            }

            // At depth > 0, skip tokens without yielding
            if self.depth > 0 {
                if sql_text::is_identifier_start_byte(b) {
                    self.pos += 1;
                    while self.pos < self.bytes.len()
                        && sql_text::is_identifier_byte(self.bytes[self.pos])
                    {
                        self.pos += 1;
                    }
                } else {
                    self.pos += 1;
                }
                continue;
            }

            // Depth 0: yield tokens
            if sql_text::is_identifier_start_byte(b) {
                let start = self.pos;
                self.pos += 1;
                while self.pos < self.bytes.len()
                    && sql_text::is_identifier_byte(self.bytes[self.pos])
                {
                    self.pos += 1;
                }
                let text = &self.sql[start..self.pos];
                return Some(ScanToken::Word { text, start });
            }

            let pos = self.pos;
            self.pos += 1;
            return Some(ScanToken::Symbol { byte: b, pos });
        }
        None
    }
}

impl QueryExecutor {
    pub fn line_block_depths(sql: &str) -> Vec<usize> {
        #[derive(Copy, Clone, Eq, PartialEq)]
        enum SubqueryParenKind {
            NonSubquery,
            Pending,
            Subquery,
        }

        #[derive(Copy, Clone, Default)]
        struct DepthComponents {
            block_depth: usize,
            query_paren_depth: usize,
            with_cte_depth: usize,
            case_branch_depth: usize,
            exception_handler_depth: usize,
        }

        impl DepthComponents {
            fn total(self) -> usize {
                self.block_depth
                    .saturating_add(self.query_paren_depth)
                    .saturating_add(self.with_cte_depth)
                    .saturating_add(self.case_branch_depth)
                    .saturating_add(self.exception_handler_depth)
            }
        }

        fn chars_word_eq_ignore_ascii_case(
            chars: &[char],
            start: usize,
            end: usize,
            keyword: &str,
        ) -> bool {
            let expected_len = keyword.chars().count();
            if end.saturating_sub(start) != expected_len {
                return false;
            }
            chars[start..end]
                .iter()
                .zip(keyword.chars())
                .all(|(left, right)| left.eq_ignore_ascii_case(&right))
        }

        fn chars_word_is_subquery_head_keyword(chars: &[char], start: usize, end: usize) -> bool {
            sql_text::SUBQUERY_HEAD_KEYWORDS
                .iter()
                .any(|keyword| chars_word_eq_ignore_ascii_case(chars, start, end, keyword))
        }

        fn leading_keyword_after_comments(line: &str) -> Option<&str> {
            let trimmed = line.trim_start();
            if sql_text::is_sqlplus_comment_line(trimmed) {
                return None;
            }

            let bytes = line.as_bytes();
            let mut i = 0usize;
            while i < bytes.len() {
                let b = bytes[i];
                if b.is_ascii_whitespace() {
                    i += 1;
                    continue;
                }
                if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
                    return None;
                }
                if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                    i += 2;
                    while i + 1 < bytes.len() {
                        if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                            i += 2;
                            break;
                        }
                        i += 1;
                    }
                    continue;
                }
                if sql_text::is_identifier_byte(b) {
                    let start = i;
                    i += 1;
                    while i < bytes.len() && sql_text::is_identifier_byte(bytes[i]) {
                        i += 1;
                    }
                    return line.get(start..i);
                }
                i += 1;
            }
            None
        }

        fn skip_ws_and_comments(chars: &[char], mut idx: usize) -> usize {
            loop {
                while idx < chars.len() && chars[idx].is_whitespace() {
                    idx += 1;
                }

                if idx + 1 < chars.len() && chars[idx] == '/' && chars[idx + 1] == '*' {
                    idx += 2;
                    while idx + 1 < chars.len() {
                        if chars[idx] == '*' && chars[idx + 1] == '/' {
                            idx += 2;
                            break;
                        }
                        idx += 1;
                    }
                    continue;
                }

                if idx + 1 < chars.len() && chars[idx] == '-' && chars[idx + 1] == '-' {
                    idx += 2;
                    while idx < chars.len() && chars[idx] != '\n' {
                        idx += 1;
                    }
                    continue;
                }

                // SQL*Plus comment command (REM/REMARK) can appear after
                // an opening parenthesis on the same line, and the nested
                // SELECT/WITH may start on the next line.
                if idx < chars.len() && sql_text::is_identifier_char(chars[idx]) {
                    let start = idx;
                    while idx < chars.len() && sql_text::is_identifier_char(chars[idx]) {
                        idx += 1;
                    }
                    if chars_word_eq_ignore_ascii_case(chars, start, idx, "REM")
                        || chars_word_eq_ignore_ascii_case(chars, start, idx, "REMARK")
                    {
                        while idx < chars.len() && chars[idx] != '\n' {
                            idx += 1;
                        }
                        continue;
                    }
                    idx = start;
                }

                break;
            }

            idx
        }

        fn should_pre_dedent(leading_word: &str) -> bool {
            leading_word.eq_ignore_ascii_case("END")
                || leading_word.eq_ignore_ascii_case("ELSE")
                || leading_word.eq_ignore_ascii_case("ELSIF")
                || leading_word.eq_ignore_ascii_case("ELSEIF")
                || leading_word.eq_ignore_ascii_case("EXCEPTION")
        }

        fn is_end_suffix_keyword(leading_word: Option<&str>) -> bool {
            leading_word.is_some_and(|word| {
                matches!(
                    word,
                    _ if word.eq_ignore_ascii_case("CASE")
                        || word.eq_ignore_ascii_case("IF")
                        || word.eq_ignore_ascii_case("LOOP")
                        || word.eq_ignore_ascii_case("WHILE")
                        || word.eq_ignore_ascii_case("FOR")
                        || word.eq_ignore_ascii_case("BEFORE")
                        || word.eq_ignore_ascii_case("AFTER")
                        || word.eq_ignore_ascii_case("INSTEAD")
                        || word.eq_ignore_ascii_case("REPEAT")
                )
            })
        }

        fn is_non_label_control_keyword(leading_word: Option<&str>) -> bool {
            leading_word.is_some_and(|word| {
                matches!(
                    word,
                    _ if word.eq_ignore_ascii_case("END")
                        || word.eq_ignore_ascii_case("CASE")
                        || word.eq_ignore_ascii_case("IF")
                        || word.eq_ignore_ascii_case("LOOP")
                        || word.eq_ignore_ascii_case("WHILE")
                        || word.eq_ignore_ascii_case("FOR")
                        || word.eq_ignore_ascii_case("REPEAT")
                        || word.eq_ignore_ascii_case("ELSE")
                        || word.eq_ignore_ascii_case("ELSIF")
                        || word.eq_ignore_ascii_case("ELSEIF")
                        || word.eq_ignore_ascii_case("WHEN")
                        || word.eq_ignore_ascii_case("EXCEPTION")
                        || word.eq_ignore_ascii_case("BEGIN")
                        || word.eq_ignore_ascii_case("DECLARE")
                        || word.eq_ignore_ascii_case("THEN")
                        || word.eq_ignore_ascii_case("IS")
                        || word.eq_ignore_ascii_case("AS")
                )
            })
        }

        fn parse_identifier_chain(line: &str) -> Option<String> {
            let bytes = line.as_bytes();
            let mut i = 0usize;

            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }

            if i >= bytes.len() {
                return None;
            }

            let mut segments: Vec<String> = Vec::new();

            loop {
                while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                    i += 1;
                }
                if i >= bytes.len() || bytes[i] == b';' {
                    break;
                }
                if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
                    break;
                }

                if bytes[i] == b'"' {
                    i += 1;
                    let mut segment = String::new();
                    while i < bytes.len() {
                        let ch_opt = line.get(i..).and_then(|rest| rest.chars().next());
                        let ch = match ch_opt {
                            Some(ch) => ch,
                            None => break,
                        };

                        if ch == '"' {
                            let next_idx = i + ch.len_utf8();
                            let next_ch = line.get(next_idx..).and_then(|rest| rest.chars().next());
                            if next_ch == Some('"') {
                                segment.push('"');
                                i = next_idx + '"'.len_utf8();
                                continue;
                            }
                            i = next_idx;
                            break;
                        }

                        segment.push(ch);
                        i += ch.len_utf8();
                    }
                    if segment.is_empty() {
                        break;
                    }
                    segment.make_ascii_uppercase();
                    segments.push(segment);
                } else {
                    if !sql_text::is_identifier_start_byte(bytes[i]) {
                        break;
                    }
                    let start = i;
                    i += 1;
                    while i < bytes.len() && sql_text::is_identifier_byte(bytes[i]) {
                        i += 1;
                    }
                    segments.push(line[start..i].to_ascii_uppercase());
                }

                while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                    i += 1;
                }
                if i < bytes.len() && bytes[i] == b'.' {
                    i += 1;
                    continue;
                }
                break;
            }

            if segments.is_empty() {
                None
            } else {
                Some(segments.join("."))
            }
        }

        #[derive(Default)]
        struct EndSuffixOrLabel {
            upper: String,
            quoted_label: bool,
            leading_unquoted_segment: Option<String>,
            segment_count: usize,
        }

        fn parse_end_suffix_or_label(line: &str) -> Option<EndSuffixOrLabel> {
            let bytes = line.as_bytes();
            let mut i = 0usize;
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if i + 3 > bytes.len() || !bytes[i..i + 3].eq_ignore_ascii_case(b"END") {
                return None;
            }
            i += 3;

            let skip_ws_and_inline_comments = |bytes: &[u8], mut i: usize| loop {
                while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                    i += 1;
                }
                if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
                    return i;
                }
                if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                    i += 2;
                    while i + 1 < bytes.len() {
                        if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                            i += 2;
                            break;
                        }
                        i += 1;
                    }
                    continue;
                }
                return i;
            };

            i = skip_ws_and_inline_comments(bytes, i);
            if i >= bytes.len() || bytes[i] == b';' {
                return Some(EndSuffixOrLabel::default());
            }
            if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
                return None;
            }

            let mut segments: Vec<String> = Vec::new();
            let mut quoted_label = false;
            let mut leading_unquoted_segment: Option<String> = None;

            loop {
                i = skip_ws_and_inline_comments(bytes, i);
                if i >= bytes.len() || bytes[i] == b';' {
                    break;
                }
                if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
                    break;
                }

                if bytes[i] == b'"' {
                    quoted_label = true;
                    i += 1;
                    let mut segment = String::new();
                    while i < bytes.len() {
                        let ch_opt = line.get(i..).and_then(|rest| rest.chars().next());
                        let ch = match ch_opt {
                            Some(ch) => ch,
                            None => break,
                        };

                        if ch == '"' {
                            let next_idx = i + ch.len_utf8();
                            let next_ch = line.get(next_idx..).and_then(|rest| rest.chars().next());
                            if next_ch == Some('"') {
                                segment.push('"');
                                i = next_idx + '"'.len_utf8();
                                continue;
                            }
                            i = next_idx;
                            break;
                        }

                        segment.push(ch);
                        i += ch.len_utf8();
                    }
                    if segment.is_empty() {
                        break;
                    }
                    segment.make_ascii_uppercase();
                    segments.push(segment);
                } else {
                    if !sql_text::is_identifier_start_byte(bytes[i]) {
                        break;
                    }
                    let start = i;
                    i += 1;
                    while i < bytes.len() && sql_text::is_identifier_byte(bytes[i]) {
                        i += 1;
                    }
                    let upper = line[start..i].to_ascii_uppercase();
                    if leading_unquoted_segment.is_none() {
                        leading_unquoted_segment = Some(upper.clone());
                    }
                    segments.push(upper);
                }

                i = skip_ws_and_inline_comments(bytes, i);
                if i < bytes.len() && bytes[i] == b'.' {
                    i += 1;
                    continue;
                }
                break;
            }

            if segments.is_empty() {
                return Some(EndSuffixOrLabel::default());
            }

            Some(EndSuffixOrLabel {
                upper: segments.join("."),
                quoted_label,
                leading_unquoted_segment,
                segment_count: segments.len(),
            })
        }
        let is_with_main_query_keyword = sql_text::is_with_main_query_keyword;

        let mut builder = SqlParserEngine::new();
        let mut depths = Vec::new();

        // Extra indentation state for SQL formatting depth that should not affect splitting.
        let mut subquery_paren_depth = 0usize;
        let mut pending_subquery_paren = 0usize;
        let mut subquery_paren_stack: Vec<SubqueryParenKind> = Vec::new();
        let mut with_cte_depth = 0usize;
        let mut with_cte_paren = 0isize;
        let mut pending_with = false;
        let mut exception_depth_stack: Vec<usize> = Vec::new();
        let mut exception_handler_body = false;
        let mut case_branch_stack: Vec<bool> = Vec::new();

        for line in sql.lines() {
            let leading_word = if builder.is_idle() {
                leading_keyword_after_comments(line)
            } else {
                None
            };
            let leading_identifier_chain = if builder.is_idle() {
                parse_identifier_chain(line)
            } else {
                None
            };
            let pending_end_label_continuation =
                leading_identifier_chain
                    .as_ref()
                    .is_some_and(|identifier_chain| {
                        identifier_chain.contains('.')
                            || !is_non_label_control_keyword(leading_word)
                    });
            let leading_is =
                |keyword: &str| leading_word.is_some_and(|word| word.eq_ignore_ascii_case(keyword));
            let leading_is_any = |keywords: &[&str]| {
                leading_word.is_some_and(|word| {
                    keywords
                        .iter()
                        .any(|keyword| word.eq_ignore_ascii_case(keyword))
                })
            };

            let trimmed_start = line.trim_start();
            let is_comment_or_blank = trimmed_start.is_empty()
                || sql_text::is_sqlplus_comment_line(trimmed_start)
                || ((trimmed_start.starts_with("/*") || trimmed_start.starts_with("*/"))
                    && leading_word.is_none());

            if pending_subquery_paren > 0 && !is_comment_or_blank {
                // WITH is also a valid subquery head (e.g. `( WITH cte AS (...) SELECT ... )`).
                // VALUES can head a nested query block in dialects that support table value
                // constructors in FROM/subquery positions.
                let promote_to_subquery = leading_is_any(sql_text::SUBQUERY_HEAD_KEYWORDS);
                if promote_to_subquery {
                    subquery_paren_depth =
                        subquery_paren_depth.saturating_add(pending_subquery_paren);
                }
                let mut unresolved = pending_subquery_paren;
                for paren_kind in subquery_paren_stack.iter_mut().rev() {
                    if unresolved == 0 {
                        break;
                    }
                    if *paren_kind == SubqueryParenKind::Pending {
                        *paren_kind = if promote_to_subquery {
                            SubqueryParenKind::Subquery
                        } else {
                            SubqueryParenKind::NonSubquery
                        };
                        unresolved -= 1;
                    }
                }
                pending_subquery_paren = 0;
            }

            // Eagerly resolve pending_end when the current line does NOT continue an
            // END CASE / END IF / END LOOP / END BEFORE / END AFTER / END INSTEAD sequence.
            // Without this, a bare "END" on its own line (e.g. CASE expression end)
            // leaves block_stack stale for the next line's depth calculation,
            // causing incorrect indentation for ELSE/WHEN that follow.
            {
                use crate::sql_parser_engine::PendingEnd;
                if builder.state.pending_end == PendingEnd::End
                    && !is_comment_or_blank
                    && !is_end_suffix_keyword(leading_word)
                    && !pending_end_label_continuation
                {
                    builder.state.resolve_pending_end_on_separator();
                }
            }

            let open_cases = builder.state.case_count();
            if case_branch_stack.len() < open_cases {
                case_branch_stack.resize(open_cases, false);
            } else if case_branch_stack.len() > open_cases {
                case_branch_stack.truncate(open_cases);
            }
            let innermost_case_depth = builder.state.innermost_case_depth();
            let at_case_header_level =
                innermost_case_depth.is_some_and(|depth| depth + 1 == builder.block_depth());
            let end_suffix_or_label = if leading_is("END") {
                parse_end_suffix_or_label(line)
            } else {
                None
            };
            let end_has_suffix = end_suffix_or_label.as_ref().is_some_and(|tail| {
                !tail.quoted_label
                    && tail.segment_count == 1
                    && is_end_suffix_keyword(tail.leading_unquoted_segment.as_deref())
            });
            let exception_end_line = exception_depth_stack
                .last()
                .is_some_and(|depth| *depth == builder.block_depth())
                && leading_is("END")
                && !end_has_suffix;

            let package_scope_asis_count = builder
                .state
                .block_stack
                .iter()
                .filter(|kind| **kind == crate::sql_parser_engine::BlockKind::AsIs)
                .count();
            let in_package_subprogram_scope = package_scope_asis_count >= 2;

            let package_member_exception_line = if leading_is("EXCEPTION") {
                let asis_count = builder
                    .state
                    .block_stack
                    .iter()
                    .filter(|kind| **kind == crate::sql_parser_engine::BlockKind::AsIs)
                    .count();
                let begin_count = builder
                    .state
                    .block_stack
                    .iter()
                    .filter(|kind| **kind == crate::sql_parser_engine::BlockKind::Begin)
                    .count();
                asis_count >= 2 && begin_count == 1
            } else {
                false
            };

            let mut block_depth_component =
                if leading_word.is_some_and(should_pre_dedent) && !package_member_exception_line {
                    builder.block_depth().saturating_sub(1)
                } else {
                    builder.block_depth()
                };

            if leading_is("END")
                && !end_has_suffix
                && !in_package_subprogram_scope
                && builder.state.plain_end_closes_parent_scope(
                    end_suffix_or_label
                        .as_ref()
                        .map_or("", |tail| tail.upper.as_str()),
                )
            {
                block_depth_component = block_depth_component.saturating_sub(1);
            }
            {
                use crate::sql_parser_engine::PendingEnd;
                if builder.state.pending_end == PendingEnd::End
                    && is_end_suffix_keyword(leading_word)
                {
                    block_depth_component = block_depth_component.saturating_sub(1);
                } else if builder.state.pending_end == PendingEnd::End
                    && pending_end_label_continuation
                {
                    block_depth_component = block_depth_component.saturating_sub(1);
                    let label_upper = leading_identifier_chain.clone().unwrap_or_default();
                    if !in_package_subprogram_scope
                        && builder
                            .state
                            .plain_end_closes_parent_scope(label_upper.as_str())
                    {
                        block_depth_component = block_depth_component.saturating_sub(1);
                    }
                } else if builder.state.pending_end == PendingEnd::End {
                    let label_upper = leading_identifier_chain
                        .clone()
                        .or_else(|| leading_word.map(|word| word.to_ascii_uppercase()))
                        .unwrap_or_default();
                    if !in_package_subprogram_scope
                        && builder
                            .state
                            .plain_end_closes_parent_scope(label_upper.as_str())
                    {
                        block_depth_component = block_depth_component.saturating_sub(1);
                    }
                }
            }

            if at_case_header_level && leading_is_any(&["WHEN", "ELSE"]) {
                block_depth_component = builder.block_depth();
            }

            if leading_is("BEGIN")
                && (builder.state.pending_subprogram_begins > 0
                    || builder.state.has_pending_declare_begin())
            {
                block_depth_component = block_depth_component.saturating_sub(1);
            }

            // Compute CASE branch indentation from the block_stack.
            let mut case_branch_indent = 0usize;
            {
                use crate::sql_parser_engine::BlockKind;
                let mut case_idx = 0usize;
                for (stack_idx, kind) in builder.state.block_stack.iter().enumerate() {
                    if *kind != BlockKind::Case {
                        continue;
                    }
                    if let Some(&branch_active) = case_branch_stack.get(case_idx) {
                        if branch_active {
                            let case_depth = stack_idx;
                            let is_header_line = builder.block_depth() == case_depth + 1
                                && leading_is_any(&["WHEN", "ELSE", "END"]);
                            if !is_header_line {
                                case_branch_indent += 1;
                            }
                        }
                    }
                    case_idx += 1;
                }
            }

            let query_paren_component =
                if line.trim_start().starts_with(')') && subquery_paren_depth > 0 {
                    subquery_paren_depth.saturating_sub(1)
                } else {
                    subquery_paren_depth
                };

            let with_cte_component = if with_cte_depth > 0 {
                let starts_main_select =
                    leading_word.is_some_and(&is_with_main_query_keyword) && with_cte_paren <= 0;
                if starts_main_select {
                    0
                } else {
                    with_cte_depth
                }
            } else {
                0
            };

            let exception_handler_component =
                if exception_handler_body && !leading_is("WHEN") && !exception_end_line {
                    1
                } else {
                    0
                };

            let depth = DepthComponents {
                block_depth: block_depth_component,
                query_paren_depth: query_paren_component,
                with_cte_depth: with_cte_component,
                case_branch_depth: case_branch_indent,
                exception_handler_depth: exception_handler_component,
            }
            .total();

            // No extra subprogram body depth: declarations and statements share the same level.

            depths.push(depth);

            // Keep WITH-clause indentation context separate from block depth.
            let with_line = leading_is("WITH");

            if with_line {
                pending_with = true;
                with_cte_depth = with_cte_depth.max(1);
                with_cte_paren = 0;
            }

            if pending_with
                && leading_word.is_some_and(&is_with_main_query_keyword)
                && with_cte_paren <= 0
            {
                with_cte_depth = 0;
                pending_with = false;
            }

            if leading_is("EXCEPTION") {
                exception_depth_stack.push(builder.block_depth());
                exception_handler_body = false;
            } else if !exception_depth_stack.is_empty() && leading_is("WHEN") {
                exception_handler_body = true;
            } else if exception_depth_stack
                .last()
                .is_some_and(|depth| *depth == builder.block_depth())
                && leading_is("END")
            {
                exception_depth_stack.pop();
                exception_handler_body = false;
            }
            if at_case_header_level {
                if leading_is_any(&["WHEN", "ELSE"]) {
                    if let Some(last) = case_branch_stack.last_mut() {
                        *last = true;
                    }
                } else if leading_is("END") {
                    if let Some(last) = case_branch_stack.last_mut() {
                        *last = false;
                    }
                }
            }

            builder.process_line_with_observer(line, |chars, symbol_idx, symbol, _next| {
                if symbol == '(' {
                    let j = skip_ws_and_comments(chars, symbol_idx.saturating_add(1));
                    let mut k = j;
                    let mut paren_kind = SubqueryParenKind::NonSubquery;
                    while k < chars.len() && (chars[k].is_ascii_alphanumeric() || chars[k] == '_') {
                        k += 1;
                    }
                    if k > j {
                        if chars_word_is_subquery_head_keyword(chars, j, k) {
                            subquery_paren_depth = subquery_paren_depth.saturating_add(1);
                            paren_kind = SubqueryParenKind::Subquery;
                        }
                    } else if j >= chars.len()
                        || (chars[j] == '-' && j + 1 < chars.len() && chars[j + 1] == '-')
                        || (chars[j] == '/' && j + 1 < chars.len() && chars[j + 1] == '*')
                    {
                        pending_subquery_paren = pending_subquery_paren.saturating_add(1);
                        paren_kind = SubqueryParenKind::Pending;
                    }
                    subquery_paren_stack.push(paren_kind);
                    if with_cte_depth > 0 {
                        with_cte_paren += 1;
                    }
                } else if symbol == ')' {
                    let closed_kind = subquery_paren_stack.pop();
                    if closed_kind == Some(SubqueryParenKind::Subquery) {
                        subquery_paren_depth = subquery_paren_depth.saturating_sub(1);
                    } else if closed_kind == Some(SubqueryParenKind::Pending) {
                        pending_subquery_paren = pending_subquery_paren.saturating_sub(1);
                    } else if closed_kind.is_none() {
                        // Malformed SQL recovery path: keep depth accounting monotonic.
                        subquery_paren_depth = subquery_paren_depth.saturating_sub(1);
                    }
                    if with_cte_depth > 0 {
                        with_cte_paren -= 1;
                    }
                }
            });
        }

        depths
    }

    pub fn strip_leading_comments(sql: &str) -> String {
        let mut remaining = sql;

        loop {
            let trimmed = remaining.trim_start();

            if sql_text::is_sqlplus_comment_line(trimmed) {
                if let Some(line_end) = trimmed.find('\n') {
                    remaining = &trimmed[line_end + 1..];
                    continue;
                }
                return String::new();
            }

            if trimmed.starts_with("/*") {
                if let Some(block_end) = trimmed.find("*/") {
                    remaining = &trimmed[block_end + 2..];
                    continue;
                }
                return String::new();
            }

            return trimmed.to_string();
        }
    }

    fn strip_trailing_comments(sql: &str) -> String {
        let mut result = sql.to_string();

        loop {
            let trimmed = result.trim_end();
            if trimmed.is_empty() {
                return String::new();
            }

            // Check for trailing line comment (-- ... at end of line)
            // Find the last line and check if it's only a comment
            if let Some(last_newline) = trimmed.rfind('\n') {
                let last_line = trimmed[last_newline + 1..].trim();
                if sql_text::is_sqlplus_comment_line(last_line) {
                    result = trimmed[..last_newline].to_string();
                    continue;
                }
            } else {
                // Single line - check if entire thing is a line comment
                if sql_text::is_sqlplus_comment_line(trimmed) {
                    return String::new();
                }
            }

            // Check for trailing block comment
            if trimmed.ends_with("*/") {
                // Find matching /*
                // Need to scan backwards to find the opening /*
                let bytes = trimmed.as_bytes();
                let mut depth = 0;
                let mut i = bytes.len();
                let mut found_start = None;

                while i > 0 {
                    i -= 1;
                    if i > 0 && bytes[i - 1] == b'/' && bytes[i] == b'*' {
                        depth -= 1;
                        if depth == 0 {
                            found_start = Some(i - 1);
                            break;
                        }
                        i -= 1;
                    } else if i > 0 && bytes[i - 1] == b'*' && bytes[i] == b'/' {
                        depth += 1;
                        i -= 1;
                    }
                }

                if let Some(start) = found_start {
                    // Check if this block comment is at the end (only whitespace before it)
                    let before = trimmed[..start].trim_end();
                    if before.is_empty() {
                        return String::new();
                    }
                    result = before.to_string();
                    continue;
                }
            }

            return trimmed.to_string();
        }
    }

    pub(crate) fn strip_comments(sql: &str) -> String {
        let without_leading = Self::strip_leading_comments(sql);
        Self::strip_trailing_comments(&without_leading)
    }

    /// Strip extra trailing semicolons from a statement.
    /// "END;;" -> "END;", "SELECT 1;;" -> "SELECT 1"
    /// Preserves single trailing semicolon for PL/SQL statements.
    fn strip_extra_trailing_semicolons(sql: &str) -> String {
        let trimmed = sql.trim_end();
        if trimmed.is_empty() {
            return String::new();
        }

        // Count trailing semicolons
        let mut semicolon_count = 0;
        for c in trimmed.chars().rev() {
            if c == ';' {
                semicolon_count += 1;
            } else if c.is_whitespace() {
                continue;
            } else {
                break;
            }
        }

        if semicolon_count <= 1 {
            return trimmed.to_string();
        }

        // Remove all trailing semicolons and whitespace, then check if we need to add one back
        let without_semis = trimmed.trim_end_matches(|c: char| c == ';' || c.is_whitespace());
        if without_semis.is_empty() {
            return String::new();
        }

        if Self::should_preserve_single_trailing_semicolon(without_semis) {
            format!("{};", without_semis)
        } else {
            without_semis.to_string()
        }
    }

    fn should_preserve_single_trailing_semicolon(sql: &str) -> bool {
        let mut trailing_tokens = sql
            .split_whitespace()
            .rev()
            .map(|token| token.trim_matches(|ch: char| !sql_text::is_identifier_char(ch)))
            .filter(|token| !token.is_empty());

        let Some(last_token) = trailing_tokens.next() else {
            return false;
        };

        if last_token.eq_ignore_ascii_case("END") {
            return true;
        }

        trailing_tokens
            .next()
            .is_some_and(|token| token.eq_ignore_ascii_case("END"))
    }

    pub fn leading_keyword(sql: &str) -> Option<String> {
        let cleaned = Self::strip_leading_comments(sql);
        cleaned
            .split_whitespace()
            .next()
            .map(|token| token.to_ascii_uppercase())
    }

    pub fn is_select_statement(sql: &str) -> bool {
        match Self::leading_keyword(sql).as_deref() {
            Some("SELECT") | Some("VALUES") | Some("TABLE") => true,
            Some("WITH") => Self::with_clause_starts_with_select(sql),
            _ => false,
        }
    }

    pub fn maybe_inject_rowid_for_editing(sql: &str) -> String {
        if !Self::is_select_statement(sql) {
            return sql.to_string();
        }

        let leading_kw = Self::leading_keyword(sql);
        let leading = leading_kw.as_deref();

        // For WITH (CTE) queries, find the main SELECT's FROM clause
        let effective_sql: &str;
        let with_prefix_len: usize;
        if leading == Some("WITH") {
            if let Some(main_select_idx) = Self::find_main_select_after_with(sql) {
                if Self::is_parenthesized_select_start(sql, main_select_idx) {
                    return sql.to_string();
                }
                effective_sql = &sql[main_select_idx..];
                with_prefix_len = main_select_idx;
            } else {
                return sql.to_string();
            }
        } else if leading == Some("SELECT") {
            effective_sql = sql;
            with_prefix_len = 0;
        } else {
            return sql.to_string();
        }

        let Some(from_idx_in_effective) = Self::find_top_level_keyword(effective_sql, "FROM")
        else {
            return sql.to_string();
        };

        if !Self::is_rowid_edit_eligible_query(effective_sql) {
            return sql.to_string();
        }

        let Some(rowid_expr) =
            Self::single_table_rowid_expression(effective_sql, from_idx_in_effective)
        else {
            return sql.to_string();
        };
        let rowid_qualifier = rowid_expr.strip_suffix(".ROWID").unwrap_or("").trim();

        let select_body_start_in_effective =
            Self::find_select_body_start(effective_sql).unwrap_or(from_idx_in_effective);
        if select_body_start_in_effective >= from_idx_in_effective {
            return sql.to_string();
        }

        let select_list_upper = effective_sql
            [select_body_start_in_effective..from_idx_in_effective]
            .to_ascii_uppercase();
        if select_list_upper.contains("ROWID") {
            return sql.to_string();
        }

        // Build the rewritten SQL
        let injection = format!("{rowid_expr}, ");
        let global_select_body_start = with_prefix_len + select_body_start_in_effective;
        let mut rewritten = String::with_capacity(sql.len().saturating_add(injection.len()));
        rewritten.push_str(&sql[..global_select_body_start]);
        rewritten.push_str(&injection);
        let select_body = &sql[global_select_body_start..];
        if rowid_qualifier.is_empty() {
            rewritten.push_str(select_body);
            return rewritten;
        }

        if let Some((wildcard_start, wildcard_end)) =
            Self::find_leading_wildcard_in_select_list(select_body)
        {
            rewritten.push_str(&select_body[..wildcard_start]);
            rewritten.push_str(rowid_qualifier);
            rewritten.push_str(".*");
            rewritten.push_str(&select_body[wildcard_end..]);
        } else {
            rewritten.push_str(select_body);
        }
        rewritten
    }

    pub(crate) fn rowid_safe_execution_sql(_original_sql: &str, rewritten_sql: &str) -> String {
        if !Self::is_select_statement(rewritten_sql) {
            return rewritten_sql.to_string();
        }

        let leading_kw = Self::leading_keyword(rewritten_sql);
        let leading = leading_kw.as_deref();

        let effective_sql: &str;
        let with_prefix_len: usize;
        if leading == Some("WITH") {
            if let Some(main_select_idx) = Self::find_main_select_after_with(rewritten_sql) {
                if Self::is_parenthesized_select_start(rewritten_sql, main_select_idx) {
                    return rewritten_sql.to_string();
                }
                effective_sql = &rewritten_sql[main_select_idx..];
                with_prefix_len = main_select_idx;
            } else {
                return rewritten_sql.to_string();
            }
        } else if leading == Some("SELECT") {
            effective_sql = rewritten_sql;
            with_prefix_len = 0;
        } else {
            return rewritten_sql.to_string();
        }

        let Some(from_idx_in_effective) = Self::find_top_level_keyword(effective_sql, "FROM")
        else {
            return rewritten_sql.to_string();
        };
        let global_from_idx = with_prefix_len.saturating_add(from_idx_in_effective);
        if !rewritten_sql.is_char_boundary(global_from_idx) {
            return rewritten_sql.to_string();
        }

        let select_body_start_in_effective =
            Self::find_select_body_start(effective_sql).unwrap_or(from_idx_in_effective);
        if select_body_start_in_effective >= from_idx_in_effective {
            return rewritten_sql.to_string();
        }

        let global_select_body_start = with_prefix_len + select_body_start_in_effective;
        if !rewritten_sql.is_char_boundary(global_select_body_start) {
            return rewritten_sql.to_string();
        }
        let select_list = match rewritten_sql.get(global_select_body_start..global_from_idx) {
            Some(select_list) => select_list,
            None => return rewritten_sql.to_string(),
        };

        let mut rewritten_select_list = String::with_capacity(select_list.len().saturating_add(64));
        let mut cursor = 0usize;
        let mut projection_index = 0usize;
        let mut changed = false;

        while cursor < select_list.len() {
            let tail = match select_list.get(cursor..) {
                Some(tail) => tail,
                None => return rewritten_sql.to_string(),
            };
            let projection_end = if let Some(comma_offset) = Self::find_first_top_level_comma(tail)
            {
                cursor.saturating_add(comma_offset)
            } else {
                select_list.len()
            };
            if !select_list.is_char_boundary(projection_end) {
                return rewritten_sql.to_string();
            }

            let projection = match select_list.get(cursor..projection_end) {
                Some(projection) => projection,
                None => return rewritten_sql.to_string(),
            };
            let projection_trimmed = projection.trim();
            let projection_trimmed_end = projection.trim_end();
            let trailing_ws_len = projection
                .len()
                .saturating_sub(projection_trimmed_end.len());
            let suffix_ws = if trailing_ws_len == 0 {
                ""
            } else {
                projection
                    .get(projection.len().saturating_sub(trailing_ws_len)..)
                    .unwrap_or("")
            };

            let normalized_projection = if let Some(expr_token) =
                Self::leading_projection_token(projection_trimmed)
            {
                let expr_upper = expr_token.to_ascii_uppercase();
                if !expr_upper.starts_with("ROWIDTOCHAR(")
                    && (expr_upper == "ROWID" || expr_upper.ends_with(".ROWID"))
                {
                    changed = true;
                    if projection_index == 0 {
                        let mut replaced =
                            "ROWIDTOCHAR(".to_string() + expr_token + ") AS SQ_INTERNAL_ROWID";
                        replaced.push_str(suffix_ws);
                        replaced
                    } else {
                        let trailing = projection_trimmed.get(expr_token.len()..).unwrap_or("");
                        let mut replaced = "ROWIDTOCHAR(".to_string() + expr_token + ")" + trailing;
                        replaced.push_str(suffix_ws);
                        replaced
                    }
                } else {
                    projection.trim_start().to_string()
                }
            } else {
                projection.trim_start().to_string()
            };

            if !rewritten_select_list.is_empty() {
                rewritten_select_list.push_str(", ");
            }
            rewritten_select_list.push_str(&normalized_projection);

            if projection_end == select_list.len() {
                break;
            }
            cursor = projection_end.saturating_add(1);
            projection_index = projection_index.saturating_add(1);
        }

        if !changed {
            return rewritten_sql.to_string();
        }

        let mut normalized = String::with_capacity(
            rewritten_sql
                .len()
                .saturating_sub(select_list.len())
                .saturating_add(rewritten_select_list.len()),
        );
        normalized.push_str(&rewritten_sql[..global_select_body_start]);
        normalized.push_str(&rewritten_select_list);
        normalized.push_str(&rewritten_sql[global_from_idx..]);
        normalized
    }

    fn leading_projection_token(expr: &str) -> Option<&str> {
        let trimmed = expr.trim();
        if trimmed.is_empty() {
            return None;
        }
        for (idx, ch) in trimmed.char_indices() {
            if ch.is_whitespace() {
                if idx == 0 {
                    continue;
                }
                return trimmed
                    .get(..idx)
                    .map(str::trim)
                    .filter(|token| !token.is_empty());
            }
        }
        Some(trimmed)
    }

    fn find_first_top_level_comma(sql: &str) -> Option<usize> {
        for token in TopLevelScanner::new(sql) {
            if let ScanToken::Symbol { byte: b',', pos } = token {
                return Some(pos);
            }
        }
        None
    }

    pub fn is_rowid_edit_eligible_query(sql: &str) -> bool {
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            return false;
        }
        let Some(from_idx) = Self::find_top_level_keyword(trimmed, "FROM") else {
            return false;
        };
        let select_idx = Self::find_top_level_keyword(trimmed, "SELECT").unwrap_or(0);
        Self::is_rowid_edit_eligible_select(trimmed, select_idx, from_idx)
    }

    fn is_rowid_edit_eligible_select(sql: &str, select_idx: usize, from_idx: usize) -> bool {
        if Self::has_top_level_set_operator(sql)
            || Self::has_top_level_identifier_keyword(sql, "GROUP")
            || Self::has_top_level_connect_by(sql)
            || Self::has_top_level_identifier_keyword(sql, "MATCH_RECOGNIZE")
            || Self::has_top_level_identifier_keyword(sql, "PIVOT")
            || Self::has_top_level_identifier_keyword(sql, "UNPIVOT")
            || Self::has_top_level_identifier_keyword(sql, "MODEL")
            || !Self::is_single_table_from_clause(sql, from_idx)
            || Self::select_clause_has_distinct_or_unique(sql, select_idx, from_idx)
            || Self::select_clause_has_top_level_aggregate(sql, select_idx, from_idx)
            || Self::select_clause_has_top_level_analytic(sql, select_idx, from_idx)
        {
            return false;
        }
        true
    }

    fn select_clause_has_top_level_aggregate(
        sql: &str,
        select_idx: usize,
        from_idx: usize,
    ) -> bool {
        if from_idx <= select_idx
            || !sql.is_char_boundary(select_idx)
            || !sql.is_char_boundary(from_idx)
        {
            return false;
        }

        let select_body_start = Self::find_select_body_start(sql).unwrap_or(select_idx);
        if select_body_start >= from_idx || !sql.is_char_boundary(select_body_start) {
            return false;
        }

        let select_list = &sql[select_body_start..from_idx];
        let mut scanner = TopLevelScanner::new(select_list);
        while let Some(token) = scanner.next() {
            if let ScanToken::Word { text, .. } = token {
                if Self::is_aggregate_function_name(text)
                    && scanner.peek_next_non_ws_byte() == Some(b'(')
                {
                    return true;
                }
            }
        }
        false
    }

    fn is_aggregate_function_name(word: &str) -> bool {
        matches!(
            word.to_ascii_uppercase().as_str(),
            "COUNT"
                | "SUM"
                | "AVG"
                | "MIN"
                | "MAX"
                | "LISTAGG"
                | "JSON_ARRAYAGG"
                | "JSON_OBJECTAGG"
                | "STDDEV"
                | "STDDEV_POP"
                | "STDDEV_SAMP"
                | "VARIANCE"
                | "VAR_POP"
                | "VAR_SAMP"
                | "MEDIAN"
                | "CORR"
                | "COVAR_POP"
                | "COVAR_SAMP"
                | "REGR_SLOPE"
                | "REGR_INTERCEPT"
                | "REGR_COUNT"
                | "REGR_R2"
                | "REGR_AVGX"
                | "REGR_AVGY"
                | "REGR_SXX"
                | "REGR_SYY"
                | "REGR_SXY"
        )
    }

    fn select_clause_has_top_level_analytic(sql: &str, select_idx: usize, from_idx: usize) -> bool {
        if from_idx <= select_idx
            || !sql.is_char_boundary(select_idx)
            || !sql.is_char_boundary(from_idx)
        {
            return false;
        }

        let select_body_start = Self::find_select_body_start(sql).unwrap_or(select_idx);
        if select_body_start >= from_idx || !sql.is_char_boundary(select_body_start) {
            return false;
        }

        let select_list = &sql[select_body_start..from_idx];
        let mut scanner = TopLevelScanner::new(select_list);
        while let Some(token) = scanner.next() {
            if let ScanToken::Word { text, .. } = token {
                if text.eq_ignore_ascii_case("OVER")
                    && scanner.peek_next_non_ws_byte() == Some(b'(')
                {
                    return true;
                }
            }
        }
        false
    }

    /// Find the byte index of the main (final) SELECT keyword after a WITH clause.
    /// This skips over all CTE definitions to find the top-level SELECT that follows.
    fn find_main_select_after_with(sql: &str) -> Option<usize> {
        let bytes = sql.as_bytes();
        let len = bytes.len();
        let mut pos = 0usize;

        // Skip past the WITH keyword
        while pos < len {
            let b = bytes[pos];
            if b.is_ascii_alphabetic() {
                let start = pos;
                pos += 1;
                while pos < len && (bytes[pos].is_ascii_alphanumeric() || bytes[pos] == b'_') {
                    pos += 1;
                }
                if sql
                    .get(start..pos)
                    .is_some_and(|w| w.eq_ignore_ascii_case("WITH"))
                {
                    break;
                }
                continue;
            }
            pos += 1;
        }

        // Scan for top-level SELECT at depth 0
        let mut depth = 0usize;
        let mut top_level_closed_paren_recently = false;
        let mut parenthesized_main_query_depth: Option<usize> = None;

        while pos < len {
            let b = bytes[pos];

            // Line comment
            if b == b'-' && bytes.get(pos + 1) == Some(&b'-') {
                pos += 2;
                while pos < len && bytes[pos] != b'\n' {
                    pos += 1;
                }
                continue;
            }
            // Block comment
            if b == b'/' && bytes.get(pos + 1) == Some(&b'*') {
                pos += 2;
                while pos + 1 < len {
                    if bytes[pos] == b'*' && bytes[pos + 1] == b'/' {
                        pos += 2;
                        break;
                    }
                    pos += 1;
                }
                continue;
            }
            // Single-quoted string
            if b == b'\'' {
                pos += 1;
                while pos < len {
                    if bytes[pos] == b'\'' {
                        pos += 1;
                        if pos < len && bytes[pos] == b'\'' {
                            pos += 1;
                            continue;
                        }
                        break;
                    }
                    pos += 1;
                }
                continue;
            }
            // Double-quoted identifier
            if b == b'"' {
                pos += 1;
                while pos < len {
                    if bytes[pos] == b'"' {
                        pos += 1;
                        if pos < len && bytes[pos] == b'"' {
                            pos += 1;
                            continue;
                        }
                        break;
                    }
                    pos += 1;
                }
                continue;
            }

            if b == b'(' {
                if depth == 0
                    && top_level_closed_paren_recently
                    && parenthesized_main_query_depth.is_none()
                {
                    parenthesized_main_query_depth = Some(1);
                }
                depth += 1;
                pos += 1;
                continue;
            }
            if b == b')' {
                if depth == 1 {
                    top_level_closed_paren_recently = true;
                }
                depth = depth.saturating_sub(1);
                if parenthesized_main_query_depth.is_some_and(|wd| depth < wd) {
                    parenthesized_main_query_depth = None;
                }
                pos += 1;
                continue;
            }

            if b.is_ascii_whitespace() {
                pos += 1;
                continue;
            }

            if !b.is_ascii_whitespace() {
                top_level_closed_paren_recently = false;
            }

            let at_query_depth =
                depth == 0 || parenthesized_main_query_depth.is_some_and(|wd| depth == wd);

            if at_query_depth && b.is_ascii_alphabetic() {
                let start = pos;
                pos += 1;
                while pos < len && (bytes[pos].is_ascii_alphanumeric() || bytes[pos] == b'_') {
                    pos += 1;
                }
                if sql
                    .get(start..pos)
                    .is_some_and(|w| w.eq_ignore_ascii_case("SELECT"))
                {
                    return Some(start);
                }
                continue;
            }

            pos += 1;
        }

        None
    }

    fn is_parenthesized_select_start(sql: &str, select_idx: usize) -> bool {
        if !sql.is_char_boundary(select_idx) {
            return false;
        }
        let prefix = &sql[..select_idx];
        for ch in prefix.chars().rev() {
            if ch.is_whitespace() {
                continue;
            }
            return ch == '(';
        }
        false
    }

    /// Check if the effective SQL has a top-level set operator (UNION, INTERSECT, MINUS, EXCEPT).
    fn has_top_level_set_operator(sql: &str) -> bool {
        Self::has_top_level_identifier_keyword(sql, "UNION")
            || Self::has_top_level_identifier_keyword(sql, "INTERSECT")
            || Self::has_top_level_identifier_keyword(sql, "MINUS")
            || Self::has_top_level_identifier_keyword(sql, "EXCEPT")
    }

    /// Check if the effective SQL has a top-level CONNECT BY clause.
    fn has_top_level_connect_by(sql: &str) -> bool {
        Self::has_top_level_identifier_keyword(sql, "CONNECT")
            || Self::has_top_level_identifier_keyword(sql, "START")
    }

    /// Check if the SQL contains a top-level identifier token matching `keyword`.
    ///
    /// `find_top_level_keyword` already uses full identifier-char boundaries
    /// (`_`, `$`, `#`), so this is a simple presence check.
    fn has_top_level_identifier_keyword(sql: &str, keyword: &str) -> bool {
        Self::find_top_level_keyword(sql, keyword).is_some()
    }

    /// Extract the ROWID expression for the first real (non-subquery) table
    /// in the FROM clause. Works for single tables, JOINs, and comma-joins.
    /// Returns `Some("alias.ROWID")` or `Some("TABLE_NAME.ROWID")` or `None`.
    #[allow(dead_code)]
    fn first_from_table_rowid_expression(sql: &str, from_idx: usize) -> Option<String> {
        let from_body_start = from_idx.saturating_add("FROM".len());
        if from_body_start >= sql.len() || !sql.is_char_boundary(from_body_start) {
            return None;
        }

        let from_clause = &sql[from_body_start..];
        let trimmed = from_clause.trim_start();
        if trimmed.is_empty() {
            return None;
        }

        // If FROM starts with '(' it's a subquery - scan past it to check
        // but first, try to extract table from the first non-subquery reference.
        Self::extract_first_table_ref_rowid(trimmed)
    }

    /// Extract the first table reference (name + optional alias) from a FROM clause fragment.
    /// Handles: plain table, schema.table, quoted identifiers, subqueries (skipped),
    /// LATERAL keyword, ONLY keyword.
    #[allow(dead_code)]
    fn extract_first_table_ref_rowid(from_body: &str) -> Option<String> {
        let chars: Vec<(usize, char)> = from_body.char_indices().collect();
        let len = chars.len();
        let mut i = 0usize;

        // Skip leading whitespace
        while i < len && chars[i].1.is_whitespace() {
            i += 1;
        }
        if i >= len {
            return None;
        }

        // Skip ONLY keyword if present (e.g., FROM ONLY (table_name))
        if i < len && chars[i].1.is_ascii_alphabetic() {
            let word_start = chars[i].0;
            let mut wi = i;
            while wi < len
                && (chars[wi].1.is_ascii_alphanumeric()
                    || chars[wi].1 == '_'
                    || chars[wi].1 == '$'
                    || chars[wi].1 == '#')
            {
                wi += 1;
            }
            let word_end = if wi < len {
                chars[wi].0
            } else {
                from_body.len()
            };
            let word = &from_body[word_start..word_end];
            if word.eq_ignore_ascii_case("LATERAL") || word.eq_ignore_ascii_case("ONLY") {
                i = wi;
                while i < len && chars[i].1.is_whitespace() {
                    i += 1;
                }
            }
        }

        if i >= len {
            return None;
        }

        // If starts with '(' it's a subquery/inline view — cannot use ROWID directly
        if chars[i].1 == '(' {
            return None;
        }

        // Parse the table name (possibly schema.table)
        let table_name = Self::parse_identifier_at(&chars, from_body, &mut i)?;

        // Check for schema.table pattern
        let full_name = if i < len && chars[i].1 == '.' {
            i += 1; // skip dot
            if let Some(second_part) = Self::parse_identifier_at(&chars, from_body, &mut i) {
                format!("{}.{}", table_name, second_part)
            } else {
                table_name
            }
        } else {
            table_name
        };

        // Skip whitespace after table name
        while i < len && chars[i].1.is_whitespace() {
            i += 1;
        }

        // Check for optional alias
        let alias = Self::parse_optional_alias(&chars, from_body, &mut i);

        if let Some(alias_str) = alias {
            Some(format!("{alias_str}.ROWID"))
        } else {
            Some(format!("{full_name}.ROWID"))
        }
    }

    /// Parse an identifier (possibly quoted) at the current position.
    fn parse_identifier_at(chars: &[(usize, char)], text: &str, pos: &mut usize) -> Option<String> {
        let len = chars.len();
        if *pos >= len {
            return None;
        }

        let start_char = chars[*pos].1;

        if start_char == '"' {
            // Quoted identifier
            let start_byte = chars[*pos].0;
            *pos += 1; // skip opening quote
            while *pos < len {
                if chars[*pos].1 == '"' {
                    if *pos + 1 < len && chars[*pos + 1].1 == '"' {
                        *pos += 2; // escaped quote
                        continue;
                    }
                    *pos += 1; // closing quote
                    let end_byte = if *pos < len {
                        chars[*pos].0
                    } else {
                        text.len()
                    };
                    return Some(text[start_byte..end_byte].to_string());
                }
                *pos += 1;
            }
            // Unterminated quote — return what we have
            return Some(text[start_byte..].to_string());
        }

        if start_char.is_ascii_alphabetic() || start_char == '_' {
            let start_byte = chars[*pos].0;
            while *pos < len
                && (chars[*pos].1.is_ascii_alphanumeric()
                    || chars[*pos].1 == '_'
                    || chars[*pos].1 == '$'
                    || chars[*pos].1 == '#')
            {
                *pos += 1;
            }
            let end_byte = if *pos < len {
                chars[*pos].0
            } else {
                text.len()
            };
            return Some(text[start_byte..end_byte].to_string());
        }

        None
    }

    /// Parse an optional alias after a table name.
    /// Skips AS keyword if present. Returns None if the next keyword is a
    /// SQL clause keyword (JOIN, ON, WHERE, etc.) or if no alias follows.
    #[allow(dead_code)]
    fn parse_optional_alias(
        chars: &[(usize, char)],
        text: &str,
        pos: &mut usize,
    ) -> Option<String> {
        let len = chars.len();
        if *pos >= len {
            return None;
        }

        // Check what follows the table name
        let c = chars[*pos].1;
        if c == ',' || c == ';' || c == ')' {
            return None;
        }

        // Try to read a word
        if c.is_ascii_alphabetic() || c == '_' || c == '"' {
            let is_quoted = c == '"';
            let save_pos = *pos;
            let word = Self::parse_identifier_at(chars, text, pos)?;

            // Quoted identifiers (e.g. "WHERE", "JOIN") are never keywords — always valid aliases
            if !is_quoted {
                let word_upper = word.to_ascii_uppercase();

                // If the word is a SQL keyword that terminates table references, it's not an alias
                if Self::is_from_stop_keyword(&word_upper) {
                    *pos = save_pos;
                    return None;
                }

                // "AS" keyword — skip it and read the actual alias
                if word_upper == "AS" {
                    while *pos < len && chars[*pos].1.is_whitespace() {
                        *pos += 1;
                    }
                    if *pos < len {
                        return Self::parse_identifier_at(chars, text, pos);
                    }
                    return None;
                }
            }

            // Otherwise this word is the alias
            return Some(word);
        }

        None
    }

    /// Check if a word is a keyword that terminates FROM clause table references.
    #[allow(dead_code)]
    fn is_from_stop_keyword(word_upper: &str) -> bool {
        matches!(
            word_upper,
            "WHERE"
                | "ORDER"
                | "GROUP"
                | "HAVING"
                | "FETCH"
                | "OFFSET"
                | "FOR"
                | "UNION"
                | "INTERSECT"
                | "MINUS"
                | "EXCEPT"
                | "CONNECT"
                | "START"
                | "PIVOT"
                | "UNPIVOT"
                | "MODEL"
                | "RETURNING"
                | "JOIN"
                | "INNER"
                | "LEFT"
                | "RIGHT"
                | "FULL"
                | "CROSS"
                | "NATURAL"
                | "ON"
                | "USING"
                | "PARTITION"
                | "SAMPLE"
                | "LATERAL"
        )
    }

    fn find_leading_wildcard_in_select_list(select_body: &str) -> Option<(usize, usize)> {
        let bytes = select_body.as_bytes();
        let mut pos = 0usize;

        while pos < bytes.len() {
            let b = bytes[pos];

            if b == b'-' && bytes.get(pos + 1) == Some(&b'-') {
                pos += 2;
                while pos < bytes.len() && bytes[pos] != b'\n' {
                    pos += 1;
                }
                continue;
            }

            if b == b'/' && bytes.get(pos + 1) == Some(&b'*') {
                pos += 2;
                while pos + 1 < bytes.len() {
                    if bytes[pos] == b'*' && bytes[pos + 1] == b'/' {
                        pos += 2;
                        break;
                    }
                    pos += 1;
                }
                continue;
            }

            if b.is_ascii_whitespace() {
                pos += 1;
                continue;
            }

            if b == b'*' {
                return Some((pos, pos + 1));
            }

            return None;
        }

        None
    }

    fn select_clause_has_distinct_or_unique(sql: &str, select_idx: usize, from_idx: usize) -> bool {
        let start = select_idx.saturating_add("SELECT".len());
        if start >= from_idx || start >= sql.len() || !sql.is_char_boundary(from_idx) {
            return false;
        }
        let slice = match sql.get(start..from_idx) {
            Some(s) => s,
            None => return false,
        };
        if let Some(token) = TopLevelScanner::new(slice).next() {
            if let ScanToken::Word { text, .. } = token {
                return text.eq_ignore_ascii_case("DISTINCT")
                    || text.eq_ignore_ascii_case("UNIQUE");
            }
            return false;
        }
        false
    }

    fn find_select_body_start(sql: &str) -> Option<usize> {
        let select_idx = Self::find_top_level_keyword(sql, "SELECT")?;
        let select_end = select_idx.saturating_add("SELECT".len());
        let mut idx = Self::skip_select_prefix_whitespace_and_hint(sql, select_end);

        for modifier in ["DISTINCT", "UNIQUE", "ALL"] {
            if Self::starts_with_keyword_at(sql, idx, modifier) {
                idx = idx.saturating_add(modifier.len());
                idx = Self::skip_ascii_whitespace(sql, idx);
                break;
            }
        }

        Some(idx.min(sql.len()))
    }

    fn skip_ascii_whitespace(sql: &str, mut idx: usize) -> usize {
        let bytes = sql.as_bytes();
        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        idx
    }

    fn skip_select_prefix_whitespace_and_hint(sql: &str, start_idx: usize) -> usize {
        let mut idx = Self::skip_ascii_whitespace(sql, start_idx);
        let Some(after_prefix) = sql.get(idx..) else {
            return sql.len();
        };

        if !after_prefix.starts_with("/*+") {
            return idx;
        }

        if let Some(end_rel) = after_prefix.find("*/") {
            idx = idx.saturating_add(end_rel + 2);
            idx = Self::skip_ascii_whitespace(sql, idx);
        }

        idx
    }

    fn starts_with_keyword_at(sql: &str, idx: usize, keyword: &str) -> bool {
        let Some(tail) = sql.get(idx..) else {
            return false;
        };

        if !tail
            .get(..keyword.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
        {
            return false;
        }

        let boundary_idx = idx.saturating_add(keyword.len());
        if boundary_idx >= sql.len() {
            return true;
        }

        sql.get(boundary_idx..)
            .and_then(|rest| rest.chars().next())
            .is_none_or(|ch| !ch.is_ascii_alphanumeric() && ch != '_')
    }

    fn find_top_level_keyword(sql: &str, keyword: &str) -> Option<usize> {
        for token in TopLevelScanner::new(sql) {
            if let ScanToken::Word { text, start } = token {
                if text.eq_ignore_ascii_case(keyword) {
                    return Some(start);
                }
            }
        }
        None
    }

    fn single_table_rowid_expression(sql: &str, from_idx: usize) -> Option<String> {
        let from_body_start = from_idx.saturating_add("FROM".len());
        if from_body_start >= sql.len() || !sql.is_char_boundary(from_body_start) {
            return None;
        }

        let from_body_end = Self::find_top_level_keyword(sql, "WHERE")
            .or_else(|| Self::find_top_level_keyword(sql, "ORDER"))
            .or_else(|| Self::find_top_level_keyword(sql, "FETCH"))
            .or_else(|| Self::find_top_level_keyword(sql, "OFFSET"))
            .or_else(|| Self::find_top_level_keyword(sql, "FOR"))
            .unwrap_or(sql.len());
        if from_body_end <= from_body_start || !sql.is_char_boundary(from_body_end) {
            return None;
        }

        let from_clause = &sql[from_body_start..from_body_end];
        let table_ref = Self::strip_leading_relation_modifiers(from_clause);
        if table_ref.is_empty() {
            return None;
        }
        if Self::starts_with_relation_invocation(table_ref) {
            return None;
        }

        let upper = table_ref.to_ascii_uppercase();
        if upper.starts_with("(") {
            return None;
        }

        let alias_start = if upper.starts_with('"') {
            let bytes = table_ref.as_bytes();
            let mut idx = 1usize;
            while idx < bytes.len() {
                if bytes[idx] == b'"' {
                    if idx + 1 < bytes.len() && bytes[idx + 1] == b'"' {
                        idx += 2;
                        continue;
                    }
                    idx += 1;
                    break;
                }
                idx += 1;
            }
            idx
        } else {
            table_ref
                .char_indices()
                .find_map(|(idx, ch)| {
                    if ch.is_whitespace() || ch == ';' {
                        Some(idx)
                    } else {
                        None
                    }
                })
                .unwrap_or(table_ref.len())
        };

        let table_name = table_ref
            .get(..alias_start)?
            .trim_end_matches(';')
            .trim_end();
        if table_name.is_empty() {
            return None;
        }

        let mut alias_source = table_ref.get(alias_start..).unwrap_or("").trim_start();
        if alias_source.is_empty() {
            return Some(format!("{table_name}.ROWID"));
        }

        if alias_source
            .get(..2)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("AS"))
        {
            alias_source = alias_source.get(2..).unwrap_or("").trim_start();
        }

        let alias_len = alias_source
            .char_indices()
            .find_map(|(idx, ch)| {
                if ch.is_whitespace() || ch == ';' {
                    Some(idx)
                } else {
                    None
                }
            })
            .unwrap_or(alias_source.len());
        let alias = alias_source.get(..alias_len).unwrap_or("").trim();

        if alias.is_empty() {
            Some(format!("{table_name}.ROWID"))
        } else {
            Some(format!("{alias}.ROWID"))
        }
    }

    fn is_single_table_from_clause(sql: &str, from_idx: usize) -> bool {
        let from_body_start = from_idx.saturating_add("FROM".len());
        if from_body_start >= sql.len() || !sql.is_char_boundary(from_body_start) {
            return false;
        }

        let from_body_end = Self::find_top_level_keyword(sql, "WHERE")
            .or_else(|| Self::find_top_level_keyword(sql, "ORDER"))
            .or_else(|| Self::find_top_level_keyword(sql, "FETCH"))
            .or_else(|| Self::find_top_level_keyword(sql, "OFFSET"))
            .or_else(|| Self::find_top_level_keyword(sql, "FOR"))
            .unwrap_or(sql.len());
        if from_body_end <= from_body_start || !sql.is_char_boundary(from_body_end) {
            return false;
        }

        let from_clause = &sql[from_body_start..from_body_end];
        let relation_head = Self::strip_leading_relation_modifiers(from_clause);
        if Self::starts_with_relation_invocation(relation_head) {
            return false;
        }

        for token in TopLevelScanner::new(from_clause) {
            match token {
                ScanToken::Symbol { byte: b',', .. } => return false,
                ScanToken::Word { text, .. } if text.eq_ignore_ascii_case("JOIN") => return false,
                _ => {}
            }
        }

        true
    }

    fn strip_leading_relation_modifiers(text: &str) -> &str {
        let mut trimmed = text.trim_start();
        loop {
            let Some(rest) = Self::strip_leading_keyword(trimmed, "LATERAL")
                .or_else(|| Self::strip_leading_keyword(trimmed, "ONLY"))
            else {
                break;
            };
            trimmed = rest.trim_start();
        }
        trimmed
    }

    fn strip_leading_keyword<'a>(text: &'a str, keyword: &str) -> Option<&'a str> {
        let trimmed = text.trim_start();
        if trimmed.len() < keyword.len() {
            return None;
        }
        let prefix = trimmed.get(..keyword.len())?;
        if !prefix.eq_ignore_ascii_case(keyword) {
            return None;
        }
        let rest = trimmed.get(keyword.len()..).unwrap_or("");
        let next = rest.chars().next();
        if next.is_some_and(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '$' | '#')) {
            return None;
        }
        Some(rest)
    }

    fn starts_with_relation_invocation(text: &str) -> bool {
        let trimmed = text.trim_start();
        let bytes = trimmed.as_bytes();
        if bytes.is_empty() {
            return false;
        }

        if bytes[0] == b'(' {
            return true;
        }

        let mut pos = 0usize;

        // Skip first identifier (plain or quoted)
        if !Self::skip_identifier_bytes(bytes, &mut pos) {
            return false;
        }

        // Skip optional schema qualifiers: .ident.ident...
        loop {
            let save = pos;
            while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
                pos += 1;
            }
            if pos >= bytes.len() || bytes[pos] != b'.' {
                pos = save;
                break;
            }
            pos += 1; // skip dot
            while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
                pos += 1;
            }
            if !Self::skip_identifier_bytes(bytes, &mut pos) {
                return false;
            }
        }

        // Skip trailing whitespace
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }

        pos < bytes.len() && bytes[pos] == b'('
    }

    /// Skip an identifier (plain or double-quoted) at `pos` in byte slice.
    /// Returns true if an identifier was found and skipped.
    fn skip_identifier_bytes(bytes: &[u8], pos: &mut usize) -> bool {
        if *pos >= bytes.len() {
            return false;
        }
        if bytes[*pos] == b'"' {
            *pos += 1;
            while *pos < bytes.len() {
                if bytes[*pos] == b'"' {
                    *pos += 1;
                    if *pos < bytes.len() && bytes[*pos] == b'"' {
                        *pos += 1; // escaped quote
                        continue;
                    }
                    return true;
                }
                *pos += 1;
            }
            return true; // unterminated
        }
        if sql_text::is_identifier_start_byte(bytes[*pos]) {
            *pos += 1;
            while *pos < bytes.len() && sql_text::is_identifier_byte(bytes[*pos]) {
                *pos += 1;
            }
            return true;
        }
        false
    }

    fn with_clause_starts_with_select(sql: &str) -> bool {
        let stripped = Self::strip_leading_comments(sql);
        let bytes = stripped.as_bytes();
        let len = bytes.len();
        let mut pos = 0usize;

        let mut depth = 0usize;
        let mut in_with_plsql_declaration = false;
        let mut with_plsql_waiting_main_query = false;
        let mut with_plsql_block_depth = 0usize;
        let mut with_plsql_pending_end = false;
        let mut top_level_closed_paren_recently = false;
        let mut parenthesized_main_query_depth: Option<usize> = None;
        let mut q_quote_end_byte: Option<u8> = None;

        let resolve_pending = |pending: &mut bool, bd: &mut usize| {
            if *pending {
                *bd = bd.saturating_sub(1);
                *pending = false;
            }
        };

        while pos < len {
            let b = bytes[pos];

            // Q-quote mode
            if let Some(end_b) = q_quote_end_byte {
                if b == end_b && bytes.get(pos + 1) == Some(&b'\'') {
                    q_quote_end_byte = None;
                    pos += 2;
                    continue;
                }
                pos += 1;
                continue;
            }

            // Line comment
            if b == b'-' && bytes.get(pos + 1) == Some(&b'-') {
                pos += 2;
                while pos < len && bytes[pos] != b'\n' {
                    pos += 1;
                }
                continue;
            }
            // Block comment
            if b == b'/' && bytes.get(pos + 1) == Some(&b'*') {
                pos += 2;
                while pos + 1 < len {
                    if bytes[pos] == b'*' && bytes[pos + 1] == b'/' {
                        pos += 2;
                        break;
                    }
                    pos += 1;
                }
                continue;
            }

            // NQ'...' or Q'...' (q-quoted strings)
            if (b == b'n' || b == b'N')
                && bytes.get(pos + 1).is_some_and(|&c| c == b'q' || c == b'Q')
                && bytes.get(pos + 2) == Some(&b'\'')
            {
                if let Some(&delim) = bytes.get(pos + 3) {
                    if sql_text::is_valid_q_quote_delimiter_byte(delim) {
                        q_quote_end_byte = Some(sql_text::q_quote_closing_byte(delim));
                        pos += 4;
                        continue;
                    }
                }
            }
            if (b == b'q' || b == b'Q') && bytes.get(pos + 1) == Some(&b'\'') {
                if let Some(&delim) = bytes.get(pos + 2) {
                    if sql_text::is_valid_q_quote_delimiter_byte(delim) {
                        q_quote_end_byte = Some(sql_text::q_quote_closing_byte(delim));
                        pos += 3;
                        continue;
                    }
                }
            }

            // Single-quoted string
            if b == b'\'' {
                pos += 1;
                while pos < len {
                    if bytes[pos] == b'\'' {
                        pos += 1;
                        if pos < len && bytes[pos] == b'\'' {
                            pos += 1;
                            continue;
                        }
                        break;
                    }
                    pos += 1;
                }
                continue;
            }
            // Double-quoted identifier
            if b == b'"' {
                pos += 1;
                while pos < len {
                    if bytes[pos] == b'"' {
                        pos += 1;
                        if pos < len && bytes[pos] == b'"' {
                            pos += 1;
                            continue;
                        }
                        break;
                    }
                    pos += 1;
                }
                continue;
            }

            if b == b'(' {
                if depth == 0
                    && (with_plsql_waiting_main_query || top_level_closed_paren_recently)
                    && parenthesized_main_query_depth.is_none()
                {
                    parenthesized_main_query_depth = Some(1);
                }
                depth += 1;
                pos += 1;
                continue;
            }
            if b == b')' {
                if depth == 1 {
                    top_level_closed_paren_recently = true;
                }
                depth = depth.saturating_sub(1);
                if parenthesized_main_query_depth.is_some_and(|wd| depth < wd) {
                    parenthesized_main_query_depth = None;
                }
                pos += 1;
                continue;
            }
            if b == b';' {
                resolve_pending(&mut with_plsql_pending_end, &mut with_plsql_block_depth);
                if in_with_plsql_declaration && with_plsql_block_depth == 0 {
                    in_with_plsql_declaration = false;
                    with_plsql_waiting_main_query = true;
                }
                pos += 1;
                continue;
            }

            if b.is_ascii_whitespace() {
                pos += 1;
                continue;
            }

            top_level_closed_paren_recently = false;

            let at_query_depth =
                depth == 0 || parenthesized_main_query_depth.is_some_and(|wd| depth == wd);

            if at_query_depth && sql_text::is_identifier_start_byte(b) {
                let start = pos;
                pos += 1;
                while pos < len && sql_text::is_identifier_byte(bytes[pos]) {
                    pos += 1;
                }
                let Some(token) = stripped.get(start..pos) else {
                    continue;
                };

                if in_with_plsql_declaration {
                    if token.eq_ignore_ascii_case("BEGIN")
                        || token.eq_ignore_ascii_case("DECLARE")
                        || token.eq_ignore_ascii_case("CASE")
                        || token.eq_ignore_ascii_case("IF")
                        || token.eq_ignore_ascii_case("LOOP")
                    {
                        resolve_pending(&mut with_plsql_pending_end, &mut with_plsql_block_depth);
                        with_plsql_block_depth += 1;
                        continue;
                    }
                    if token.eq_ignore_ascii_case("END") {
                        with_plsql_pending_end = true;
                        continue;
                    }
                    if with_plsql_pending_end
                        && !(token.eq_ignore_ascii_case("CASE")
                            || token.eq_ignore_ascii_case("IF")
                            || token.eq_ignore_ascii_case("LOOP"))
                    {
                        resolve_pending(&mut with_plsql_pending_end, &mut with_plsql_block_depth);
                    }
                    continue;
                }

                if token.eq_ignore_ascii_case("FUNCTION") || token.eq_ignore_ascii_case("PROCEDURE")
                {
                    in_with_plsql_declaration = true;
                    with_plsql_waiting_main_query = false;
                    with_plsql_block_depth = 0;
                    with_plsql_pending_end = false;
                    continue;
                }

                if with_plsql_waiting_main_query {
                    if token.eq_ignore_ascii_case("SELECT")
                        || token.eq_ignore_ascii_case("VALUES")
                        || token.eq_ignore_ascii_case("TABLE")
                    {
                        return true;
                    }
                    if token.eq_ignore_ascii_case("INSERT")
                        || token.eq_ignore_ascii_case("UPDATE")
                        || token.eq_ignore_ascii_case("DELETE")
                        || token.eq_ignore_ascii_case("MERGE")
                    {
                        return false;
                    }
                    if sql_text::is_statement_head_keyword(token)
                        && !sql_text::is_with_main_query_keyword(token)
                    {
                        return false;
                    }
                }

                if token.eq_ignore_ascii_case("SELECT")
                    || token.eq_ignore_ascii_case("VALUES")
                    || token.eq_ignore_ascii_case("TABLE")
                {
                    return true;
                }
                if token.eq_ignore_ascii_case("INSERT")
                    || token.eq_ignore_ascii_case("UPDATE")
                    || token.eq_ignore_ascii_case("DELETE")
                    || token.eq_ignore_ascii_case("MERGE")
                {
                    return false;
                }
                continue;
            }

            pos += 1;
        }

        false
    }

    pub fn split_script_items(sql: &str) -> Vec<ScriptItem> {
        let mut items: Vec<ScriptItem> = Vec::new();
        let add_statement = |stmt: String, items: &mut Vec<ScriptItem>| {
            let stripped = Self::strip_comments(&stmt);
            let cleaned = Self::strip_extra_trailing_semicolons(&stripped);
            if !cleaned.is_empty() {
                items.push(ScriptItem::Statement(cleaned));
            }
        };
        let on_tool_command = |cmd: ToolCommand, items: &mut Vec<ScriptItem>| {
            items.push(ScriptItem::ToolCommand(cmd));
        };

        Self::split_items_core(sql, &mut items, add_statement, on_tool_command, |_, _| {});
        items
    }

    pub fn split_format_items(sql: &str) -> Vec<FormatItem> {
        // Standalone comments between statements need special handling in format
        // mode: line comments, remark lines, and multi-line block comments are
        // preserved as separate items so the formatter can keep them intact.
        // Collect these first, then splice them back into position.
        //
        // We pre-scan for standalone block comments that span multiple lines
        // and merge them into single-line markers that the core loop can handle
        // via the on_idle_line callback.

        let mut items: Vec<FormatItem> = Vec::new();
        let mut builder = SqlParserEngine::new();
        let mut sqlblanklines_enabled = true;

        let mut add_statement = |stmt: String, items: &mut Vec<FormatItem>| {
            let cleaned = stmt.trim();
            if !cleaned.is_empty() {
                items.push(FormatItem::Statement(cleaned.to_string()));
            }
        };

        let mut lines = sql.lines().peekable();
        while let Some(line) = lines.next() {
            let trimmed = line.trim();

            // Blank-line termination
            if Self::should_force_terminate_on_blank_line(
                sqlblanklines_enabled,
                trimmed,
                builder.is_idle(),
                builder.block_depth(),
                builder.current_is_empty(),
            ) {
                for stmt in builder.force_terminate_and_take_statements() {
                    add_statement(stmt, &mut items);
                }
                continue;
            }

            // Standalone comment handling (format mode only)
            if builder.is_idle() && builder.current_is_empty() {
                if trimmed.starts_with("--") || sql_text::is_sqlplus_comment_line(trimmed) {
                    items.push(FormatItem::Statement(line.to_string()));
                    continue;
                }
                if trimmed.starts_with("/*") {
                    let mut comment = String::new();
                    comment.push_str(line);
                    if !trimmed.contains("*/") {
                        for next_line in lines.by_ref() {
                            comment.push('\n');
                            comment.push_str(next_line);
                            if next_line.contains("*/") {
                                break;
                            }
                        }
                    }
                    items.push(FormatItem::Statement(comment));
                    continue;
                }
            }

            // Delegate to the shared termination-check sequence
            Self::process_split_line(
                line,
                trimmed,
                &mut builder,
                &mut sqlblanklines_enabled,
                &mut items,
                &mut add_statement,
                &mut |cmd: ToolCommand, items: &mut Vec<FormatItem>| {
                    items.push(FormatItem::ToolCommand(cmd))
                },
                &mut |items: &mut Vec<FormatItem>, _| items.push(FormatItem::Slash),
            );
        }

        for stmt in builder.finalize_and_take_statements() {
            add_statement(stmt, &mut items);
        }
        items
    }

    /// Core split loop used by `split_script_items`.
    ///
    /// `split_format_items` handles standalone comments before delegating each
    /// non-comment line to `process_split_line` directly.
    fn split_items_core<T>(
        sql: &str,
        items: &mut Vec<T>,
        mut add_statement: impl FnMut(String, &mut Vec<T>),
        mut on_tool_command: impl FnMut(ToolCommand, &mut Vec<T>),
        mut on_slash: impl FnMut(&mut Vec<T>, &SqlParserEngine),
    ) {
        let mut builder = SqlParserEngine::new();
        let mut sqlblanklines_enabled = true;

        for line in sql.lines() {
            let trimmed = line.trim();

            // Blank-line termination (SET SQLBLANKLINES OFF)
            if Self::should_force_terminate_on_blank_line(
                sqlblanklines_enabled,
                trimmed,
                builder.is_idle(),
                builder.block_depth(),
                builder.current_is_empty(),
            ) {
                for stmt in builder.force_terminate_and_take_statements() {
                    add_statement(stmt, items);
                }
                continue;
            }

            Self::process_split_line(
                line,
                trimmed,
                &mut builder,
                &mut sqlblanklines_enabled,
                items,
                &mut add_statement,
                &mut on_tool_command,
                &mut on_slash,
            );
        }

        for stmt in builder.finalize_and_take_statements() {
            add_statement(stmt, items);
        }
    }

    /// Shared per-line termination-check sequence.
    ///
    /// Handles: incomplete CREATE recovery, slash termination, lone semicolon,
    /// tool-command detection, and parser engine feeding.  Callers are
    /// responsible for blank-line termination and any mode-specific
    /// pre-processing (e.g. standalone comment collection in format mode).
    fn process_split_line<T>(
        line: &str,
        trimmed: &str,
        builder: &mut SqlParserEngine,
        sqlblanklines_enabled: &mut bool,
        items: &mut Vec<T>,
        add_statement: &mut impl FnMut(String, &mut Vec<T>),
        on_tool_command: &mut impl FnMut(ToolCommand, &mut Vec<T>),
        on_slash: &mut impl FnMut(&mut Vec<T>, &SqlParserEngine),
    ) {
        let mut parser_is_top_level = builder.block_depth() == 0 && builder.paren_depth() == 0;

        builder.state.prepare_splitter_line_boundary(line);
        match builder
            .state
            .splitter_line_boundary_action_for_line(line, builder.current_is_empty())
        {
            LineBoundaryAction::None => {}
            LineBoundaryAction::SplitBeforeLine => {
                if !builder.current_is_empty() {
                    for stmt in builder.force_terminate_and_take_statements() {
                        add_statement(stmt, items);
                    }
                }
                parser_is_top_level = builder.block_depth() == 0 && builder.paren_depth() == 0;
            }
            LineBoundaryAction::SplitAndConsumeLine => {
                if !builder.current_is_empty() {
                    for stmt in builder.force_terminate_and_take_statements() {
                        add_statement(stmt, items);
                    }
                }
                on_slash(items, builder);
                return;
            }
            LineBoundaryAction::ConsumeLine => {
                on_slash(items, builder);
                return;
            }
        }

        // Lone semicolon after CREATE PL/SQL (prevents `;;`)
        if Self::should_force_terminate_lone_semicolon(
            builder.is_idle(),
            trimmed,
            builder.in_create_plsql(),
            builder.block_depth(),
            builder.current_is_empty(),
        ) {
            for stmt in builder.force_terminate_and_take_statements() {
                add_statement(stmt, items);
            }
            return;
        }

        // Tool command appearing after a slash-terminable open statement
        if builder.is_idle()
            && !builder.current_is_empty()
            && builder.paren_depth() == 0
            && builder.can_terminate_on_slash()
            && Self::parse_tool_command(trimmed).is_some()
        {
            for stmt in builder.force_terminate_and_take_statements() {
                add_statement(stmt, items);
            }
            parser_is_top_level = builder.block_depth() == 0 && builder.paren_depth() == 0;
        }

        // Tool command with an open (non-empty) statement
        let is_set_clause = Self::is_set_clause_line(trimmed);
        let is_alter_session_set_clause = is_set_clause && builder.starts_with_alter_session();
        let is_sql_set_statement = Self::is_sql_set_statement_line(trimmed);
        if Self::should_try_tool_command_with_open_statement(
            builder.is_idle(),
            builder.current_is_empty(),
            parser_is_top_level,
            is_alter_session_set_clause || is_sql_set_statement,
        ) {
            if let Some(command) = Self::parse_tool_command(trimmed) {
                for stmt in builder.force_terminate_and_take_statements() {
                    add_statement(stmt, items);
                }
                if let ToolCommand::SetSqlBlankLines { enabled } = &command {
                    *sqlblanklines_enabled = *enabled;
                }
                on_tool_command(command, items);
                return;
            }
        }

        // Tool command without an open statement
        if Self::should_try_tool_command_without_open_statement(
            builder.is_idle(),
            builder.current_is_empty(),
            parser_is_top_level,
        ) && !is_sql_set_statement
        {
            if let Some(command) = Self::parse_tool_command(trimmed) {
                if let ToolCommand::SetSqlBlankLines { enabled } = &command {
                    *sqlblanklines_enabled = *enabled;
                }
                on_tool_command(command, items);
                return;
            }
        }

        // Feed line to the parser engine
        for stmt in builder.process_line_and_take_statements(line) {
            add_statement(stmt, items);
        }
    }

    pub fn parse_tool_command(line: &str) -> Option<ToolCommand> {
        let trimmed_line = line.trim();
        if trimmed_line.is_empty() {
            return None;
        }
        // PROMPT는 명령 본문 전체를 출력하므로, 문장 끝 세미콜론도 payload로 보존한다.
        // 다른 도구 명령은 SQL terminator 스타일 입력(예: SET ECHO ON;)을 허용하기 위해
        // 트레일링 세미콜론을 제거한다.
        let upper_line = trimmed_line.to_ascii_uppercase();
        let trimmed = if Self::is_word_command(&upper_line, "PROMPT") {
            trimmed_line
        } else {
            trimmed_line.trim_end_matches(';').trim()
        };
        if trimmed.is_empty() {
            return None;
        }

        let upper = trimmed.to_ascii_uppercase();

        if upper == "VAR" || upper.starts_with("VAR ") || upper.starts_with("VARIABLE ") {
            return Some(Self::parse_var_command(trimmed));
        }

        if Self::is_word_command(&upper, "PRINT") {
            let rest = trimmed[5..].trim();
            let name = if rest.is_empty() {
                None
            } else {
                Some(rest.trim_start_matches(':').to_string())
            };
            return Some(ToolCommand::Print { name });
        }

        if upper.starts_with("SET SERVEROUTPUT") {
            return Some(Self::parse_serveroutput_command(trimmed));
        }

        if upper.starts_with("SHOW ERRORS") {
            return Some(Self::parse_show_errors_command(trimmed));
        }

        if upper.starts_with("SHOW ") || upper == "SHOW" {
            return Some(Self::parse_show_command(trimmed));
        }

        if upper == "DESC"
            || upper.starts_with("DESC ")
            || upper == "DESCRIBE"
            || upper.starts_with("DESCRIBE ")
        {
            return Some(Self::parse_describe_command(trimmed));
        }

        if Self::is_word_command(&upper, "PROMPT") {
            let text = trimmed[6..].trim().to_string();
            return Some(ToolCommand::Prompt { text });
        }

        if Self::is_word_command(&upper, "PAUSE") {
            return Some(Self::parse_pause_command(trimmed));
        }

        if Self::is_word_command(&upper, "ACCEPT") {
            return Some(Self::parse_accept_command(trimmed));
        }

        if Self::is_word_command(&upper, "DEFINE") {
            let rest = trimmed.get(6..).unwrap_or_default().trim();
            if rest.is_empty() {
                // MATCH_RECOGNIZE DEFINE clause marker: keep it as SQL text.
                return None;
            }
            return Some(Self::parse_define_assign_command(trimmed));
        }

        if Self::is_word_command(&upper, "UNDEFINE") {
            return Some(Self::parse_undefine_command(trimmed));
        }

        if Self::is_word_command(&upper, "COLUMN") {
            return Some(Self::parse_column_new_value_command(trimmed));
        }

        if Self::is_word_command(&upper, "CLEAR") {
            return Some(Self::parse_clear_command(trimmed));
        }

        if Self::is_word_command(&upper, "BREAK") {
            return Some(Self::parse_break_command(trimmed));
        }

        if Self::is_word_command(&upper, "COMPUTE") {
            return Some(Self::parse_compute_command(trimmed));
        }

        if Self::is_word_command(&upper, "SPOOL") {
            return Some(Self::parse_spool_command(trimmed));
        }

        if Self::is_word_command(&upper, "HOST") || trimmed.starts_with('!') {
            return Some(ToolCommand::Unsupported {
                raw: trimmed.to_string(),
                message: "HOST command is not supported in this client.".to_string(),
                is_error: true,
            });
        }

        if Self::is_word_command(&upper, "STARTUP")
            || Self::is_word_command(&upper, "SHUTDOWN")
            || Self::is_word_command(&upper, "ARCHIVE")
            || Self::is_word_command(&upper, "RECOVER")
        {
            return Some(ToolCommand::Unsupported {
                raw: trimmed.to_string(),
                message: "SQL*Plus admin command is not supported in this client.".to_string(),
                is_error: true,
            });
        }

        if Self::is_word_command(&upper, "TIMING")
            || Self::is_word_command(&upper, "TTITLE")
            || Self::is_word_command(&upper, "BTITLE")
            || Self::is_word_command(&upper, "REPHEADER")
            || Self::is_word_command(&upper, "REPFOOTER")
        {
            return Some(ToolCommand::Unsupported {
                raw: trimmed.to_string(),
                message: "SQL*Plus report command is not supported in this client.".to_string(),
                is_error: true,
            });
        }

        if upper.starts_with("SET ERRORCONTINUE") {
            return Some(Self::parse_errorcontinue_command(trimmed));
        }

        if upper.starts_with("SET AUTOCOMMIT") {
            return Some(Self::parse_autocommit_command(trimmed));
        }

        if upper.starts_with("SET DEFINE") {
            return Some(Self::parse_define_command(trimmed));
        }

        if upper.starts_with("SET SCAN") {
            return Some(Self::parse_scan_command(trimmed));
        }

        if upper.starts_with("SET VERIFY") {
            return Some(Self::parse_verify_command(trimmed));
        }

        if upper.starts_with("SET ECHO") {
            return Some(Self::parse_echo_command(trimmed));
        }

        if upper.starts_with("SET TIMING") {
            return Some(Self::parse_timing_command(trimmed));
        }

        if upper.starts_with("SET FEEDBACK") {
            return Some(Self::parse_feedback_command(trimmed));
        }

        if upper.starts_with("SET HEADING") {
            return Some(Self::parse_heading_command(trimmed));
        }

        if upper.starts_with("SET PAGESIZE") {
            return Some(Self::parse_pagesize_command(trimmed));
        }

        if upper.starts_with("SET LINESIZE") {
            return Some(Self::parse_linesize_command(trimmed));
        }

        if upper.starts_with("SET TRIMSPOOL") {
            return Some(Self::parse_trimspool_command(trimmed));
        }

        if upper.starts_with("SET TRIMOUT") {
            return Some(Self::parse_trimout_command(trimmed));
        }

        if upper.starts_with("SET SQLBLANKLINES") {
            return Some(Self::parse_sqlblanklines_command(trimmed));
        }

        if upper.starts_with("SET TAB") {
            return Some(Self::parse_tab_command(trimmed));
        }

        if upper.starts_with("SET COLSEP") {
            return Some(Self::parse_colsep_command(trimmed));
        }

        if upper.starts_with("SET NULL") {
            return Some(Self::parse_null_command(trimmed));
        }

        if Self::is_sqlplus_set_command(upper.as_str()) {
            return Some(ToolCommand::Unsupported {
                raw: trimmed.to_string(),
                message: "SQL*Plus SET command is not supported in this client.".to_string(),
                is_error: true,
            });
        }

        if trimmed.starts_with("@@")
            || trimmed.starts_with('@')
            || Self::is_start_script_command(trimmed)
            || Self::is_run_script_command(trimmed)
        {
            return Some(Self::parse_script_command(trimmed));
        }

        if upper == "RUN" || upper == "R" {
            return Some(ToolCommand::Unsupported {
                raw: trimmed.to_string(),
                message: "RUN without script path is not supported in this client.".to_string(),
                is_error: true,
            });
        }

        if upper.starts_with("WHENEVER SQLERROR") {
            return Some(Self::parse_whenever_sqlerror_command(trimmed));
        }

        if upper.starts_with("WHENEVER OSERROR") {
            return Some(Self::parse_whenever_oserror_command(trimmed));
        }

        if upper == "EXIT" || upper.starts_with("EXIT ") {
            return Some(ToolCommand::Exit);
        }

        if upper == "QUIT" || upper.starts_with("QUIT ") {
            return Some(ToolCommand::Quit);
        }

        if Self::is_connect_command(trimmed) || upper == "CONN" || upper.starts_with("CONN ") {
            return Some(Self::parse_connect_command(trimmed));
        }

        if upper == "DISCONNECT" || upper == "DISC" {
            return Some(ToolCommand::Disconnect);
        }

        if upper == "PASSWORD"
            || upper.starts_with("PASSWORD ")
            || upper == "PASSW"
            || upper.starts_with("PASSW ")
        {
            return Some(ToolCommand::Unsupported {
                raw: trimmed.to_string(),
                message: "PASSWORD command is not supported in this client.".to_string(),
                is_error: true,
            });
        }

        None
    }

    fn parse_var_command(raw: &str) -> ToolCommand {
        let mut parts = raw.split_whitespace();
        let _ = parts.next(); // VAR or VARIABLE
        let name = parts.next().unwrap_or_default();
        let type_str = parts.collect::<Vec<&str>>().join(" ");

        if name.is_empty() || type_str.trim().is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "VAR requires a variable name and type.".to_string(),
                is_error: true,
            };
        }

        match Self::parse_bind_type(&type_str) {
            Ok(data_type) => ToolCommand::Var {
                name: name.trim_start_matches(':').to_string(),
                data_type,
            },
            Err(message) => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message,
                is_error: true,
            },
        }
    }

    fn parse_serveroutput_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 3 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET SERVEROUTPUT requires ON or OFF.".to_string(),
                is_error: true,
            };
        }

        let mode = tokens[2].to_ascii_uppercase();
        if mode == "OFF" {
            return ToolCommand::SetServerOutput {
                enabled: false,
                size: None,
                unlimited: false,
            };
        }

        if mode != "ON" {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET SERVEROUTPUT supports only ON or OFF.".to_string(),
                is_error: true,
            };
        }

        let mut size: Option<u32> = None;
        let mut unlimited = false;
        let mut idx = 3usize;
        while idx + 1 < tokens.len() {
            if tokens[idx].eq_ignore_ascii_case("SIZE") {
                let size_val = tokens[idx + 1];
                if size_val.eq_ignore_ascii_case("UNLIMITED") {
                    unlimited = true;
                } else {
                    match size_val.parse::<u32>() {
                        Ok(val) => size = Some(val),
                        Err(_) => {
                            return ToolCommand::Unsupported {
                                raw: raw.to_string(),
                                message: "SET SERVEROUTPUT SIZE must be a number or UNLIMITED."
                                    .to_string(),
                                is_error: true,
                            };
                        }
                    }
                }
                break;
            }
            idx += 1;
        }

        ToolCommand::SetServerOutput {
            enabled: true,
            size,
            unlimited,
        }
    }

    fn parse_show_errors_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() <= 2 {
            return ToolCommand::ShowErrors {
                object_type: None,
                object_name: None,
            };
        }

        let mut idx = 2usize;
        let mut object_type = tokens[idx].to_ascii_uppercase();
        if object_type == "PACKAGE"
            && tokens
                .get(idx + 1)
                .map(|t| t.eq_ignore_ascii_case("BODY"))
                .unwrap_or(false)
        {
            object_type = "PACKAGE BODY".to_string();
            idx += 2;
        } else if object_type == "TYPE"
            && tokens
                .get(idx + 1)
                .map(|t| t.eq_ignore_ascii_case("BODY"))
                .unwrap_or(false)
        {
            object_type = "TYPE BODY".to_string();
            idx += 2;
        } else {
            idx += 1;
        }

        let name = tokens
            .get(idx)
            .map(|v| v.trim_start_matches(':').to_string());
        if name.is_none() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SHOW ERRORS requires an object name when a type is specified."
                    .to_string(),
                is_error: true,
            };
        }

        ToolCommand::ShowErrors {
            object_type: Some(object_type),
            object_name: name,
        }
    }

    fn parse_show_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 2 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SHOW requires a topic (USER, ALL, ERRORS).".to_string(),
                is_error: true,
            };
        }

        let topic = tokens[1].to_ascii_uppercase();
        match topic.as_str() {
            "USER" => ToolCommand::ShowUser,
            "ALL" => ToolCommand::ShowAll,
            "ERRORS" => Self::parse_show_errors_command(raw),
            _ => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SHOW supports USER, ALL, or ERRORS.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_describe_command(raw: &str) -> ToolCommand {
        let mut parts = raw.splitn(2, char::is_whitespace);
        let _ = parts.next(); // DESC/DESCRIBE
        let target = parts.next().unwrap_or("").trim();
        if target.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "DESCRIBE requires an object name.".to_string(),
                is_error: true,
            };
        }
        ToolCommand::Describe {
            name: target.to_string(),
        }
    }

    fn parse_accept_command(raw: &str) -> ToolCommand {
        let rest = raw[6..].trim();
        if rest.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "ACCEPT requires a variable name.".to_string(),
                is_error: true,
            };
        }

        let mut parts = rest.splitn(2, char::is_whitespace);
        let name = parts.next().unwrap_or_default();
        if name.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "ACCEPT requires a variable name.".to_string(),
                is_error: true,
            };
        }
        let remainder = parts.next().unwrap_or("").trim();
        let prompt = if remainder.is_empty() {
            None
        } else {
            // Keep byte offsets aligned with `remainder` when slicing `prompt_raw`.
            let upper = remainder.to_ascii_uppercase();
            if let Some(idx) = upper.find("PROMPT") {
                let prompt_raw = remainder[idx + 6..].trim();
                let cleaned = prompt_raw.trim_matches('"').trim_matches('\'').to_string();
                if cleaned.is_empty() {
                    None
                } else {
                    Some(cleaned)
                }
            } else {
                None
            }
        };

        ToolCommand::Accept {
            name: name.trim_start_matches(':').to_string(),
            prompt,
        }
    }

    fn parse_pause_command(raw: &str) -> ToolCommand {
        let rest = raw[5..].trim();
        let message = if rest.is_empty() {
            None
        } else {
            Some(rest.to_string())
        };

        ToolCommand::Pause { message }
    }

    fn parse_define_assign_command(raw: &str) -> ToolCommand {
        let rest = raw[6..].trim();
        if rest.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "DEFINE requires a variable name and value.".to_string(),
                is_error: true,
            };
        }

        let (name, value) = if let Some(eq_idx) = rest.find('=') {
            let (left, right) = rest.split_at(eq_idx);
            (left.trim(), right.trim_start_matches('=').trim())
        } else {
            let mut parts = rest.splitn(2, char::is_whitespace);
            let name = parts.next().unwrap_or_default();
            let value = parts.next().unwrap_or("").trim();
            (name, value)
        };

        if name.is_empty() || value.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "DEFINE requires a variable name and value.".to_string(),
                is_error: true,
            };
        }

        ToolCommand::Define {
            name: name.trim_start_matches(':').to_string(),
            value: value.to_string(),
        }
    }

    fn parse_undefine_command(raw: &str) -> ToolCommand {
        let rest = raw[8..].trim();
        if rest.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "UNDEFINE requires a variable name.".to_string(),
                is_error: true,
            };
        }

        ToolCommand::Undefine {
            name: rest.trim_start_matches(':').to_string(),
        }
    }

    fn parse_column_new_value_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 4 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "COLUMN requires syntax: COLUMN <column> NEW_VALUE <variable>."
                    .to_string(),
                is_error: true,
            };
        }

        if !tokens[2].eq_ignore_ascii_case("NEW_VALUE") {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "Only COLUMN ... NEW_VALUE ... is supported.".to_string(),
                is_error: true,
            };
        }

        if tokens.len() > 4 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "COLUMN NEW_VALUE accepts exactly one column and one variable."
                    .to_string(),
                is_error: true,
            };
        }

        let column_name = tokens[1].trim();
        let variable_name = tokens[3].trim_start_matches(':').trim();
        if column_name.is_empty() || variable_name.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "COLUMN requires syntax: COLUMN <column> NEW_VALUE <variable>."
                    .to_string(),
                is_error: true,
            };
        }

        ToolCommand::ColumnNewValue {
            column_name: column_name.to_string(),
            variable_name: variable_name.to_string(),
        }
    }

    fn parse_break_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 2 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "BREAK requires ON <column> or OFF.".to_string(),
                is_error: true,
            };
        }

        if tokens[1].eq_ignore_ascii_case("OFF") {
            return ToolCommand::BreakOff;
        }

        if tokens.len() == 3 && tokens[1].eq_ignore_ascii_case("ON") {
            let column_name = tokens[2].trim();
            if column_name.is_empty() {
                return ToolCommand::Unsupported {
                    raw: raw.to_string(),
                    message: "BREAK ON requires a column name.".to_string(),
                    is_error: true,
                };
            }
            return ToolCommand::BreakOn {
                column_name: column_name.to_string(),
            };
        }

        ToolCommand::Unsupported {
            raw: raw.to_string(),
            message: "BREAK supports only: BREAK ON <column> or BREAK OFF.".to_string(),
            is_error: true,
        }
    }

    fn parse_clear_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 2 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message:
                    "CLEAR supports: CLEAR BREAKS, CLEAR COMPUTES, CLEAR BREAKS CLEAR COMPUTES."
                        .to_string(),
                is_error: true,
            };
        }

        if tokens.len() == 2 && tokens[1].eq_ignore_ascii_case("BREAKS") {
            return ToolCommand::ClearBreaks;
        }

        if tokens.len() == 2 && tokens[1].eq_ignore_ascii_case("COMPUTES") {
            return ToolCommand::ClearComputes;
        }

        let is_breaks_computes = tokens.len() == 4
            && tokens[1].eq_ignore_ascii_case("BREAKS")
            && tokens[2].eq_ignore_ascii_case("CLEAR")
            && tokens[3].eq_ignore_ascii_case("COMPUTES");
        let is_computes_breaks = tokens.len() == 4
            && tokens[1].eq_ignore_ascii_case("COMPUTES")
            && tokens[2].eq_ignore_ascii_case("CLEAR")
            && tokens[3].eq_ignore_ascii_case("BREAKS");

        if is_breaks_computes || is_computes_breaks {
            return ToolCommand::ClearBreaksComputes;
        }

        ToolCommand::Unsupported {
            raw: raw.to_string(),
            message: "CLEAR supports: CLEAR BREAKS, CLEAR COMPUTES, CLEAR BREAKS CLEAR COMPUTES."
                .to_string(),
            is_error: true,
        }
    }

    fn parse_compute_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 2 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "COMPUTE requires SUM, COUNT, or OFF.".to_string(),
                is_error: true,
            };
        }

        match tokens[1].to_ascii_uppercase().as_str() {
            "SUM" | "COUNT" => {
                let mode = if tokens[1].eq_ignore_ascii_case("SUM") {
                    ComputeMode::Sum
                } else {
                    ComputeMode::Count
                };
                if tokens.len() == 2 {
                    return ToolCommand::Compute {
                        mode,
                        of_column: None,
                        on_column: None,
                    };
                }
                if tokens.len() == 6
                    && tokens[2].eq_ignore_ascii_case("OF")
                    && tokens[4].eq_ignore_ascii_case("ON")
                {
                    let of_column = tokens[3].trim();
                    let on_column = tokens[5].trim();
                    if of_column.is_empty() || on_column.is_empty() {
                        return ToolCommand::Unsupported {
                            raw: raw.to_string(),
                            message: "COMPUTE <SUM|COUNT> OF <column> ON <group_column>."
                                .to_string(),
                            is_error: true,
                        };
                    }
                    return ToolCommand::Compute {
                        mode,
                        of_column: Some(of_column.to_string()),
                        on_column: Some(on_column.to_string()),
                    };
                }
                ToolCommand::Unsupported {
                    raw: raw.to_string(),
                    message: "COMPUTE supports: COMPUTE SUM, COMPUTE COUNT, COMPUTE OFF, COMPUTE <SUM|COUNT> OF <column> ON <group_column>.".to_string(),
                    is_error: true,
                }
            }
            "OFF" => ToolCommand::ComputeOff,
            _ => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "COMPUTE requires SUM, COUNT, or OFF.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_spool_command(raw: &str) -> ToolCommand {
        let rest = raw[5..].trim();
        if rest.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SPOOL requires a file path, APPEND, or OFF.".to_string(),
                is_error: true,
            };
        }

        if rest.eq_ignore_ascii_case("OFF") {
            return ToolCommand::Spool {
                path: None,
                append: false,
            };
        }

        if rest.eq_ignore_ascii_case("APPEND") {
            return ToolCommand::Spool {
                path: None,
                append: true,
            };
        }

        let mut append = false;
        let path_part = if rest.to_ascii_uppercase().ends_with(" APPEND") {
            append = true;
            rest[..rest.len() - "APPEND".len()].trim()
        } else {
            rest
        };

        let cleaned = path_part.trim_matches('"').trim_matches('\'').to_string();
        if cleaned.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SPOOL requires a file path.".to_string(),
                is_error: true,
            };
        }

        ToolCommand::Spool {
            path: Some(cleaned),
            append,
        }
    }

    fn parse_whenever_sqlerror_command(raw: &str) -> ToolCommand {
        let rest = raw[17..].trim();
        if rest.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "WHENEVER SQLERROR requires EXIT or CONTINUE.".to_string(),
                is_error: true,
            };
        }
        let mut parts = rest.splitn(2, char::is_whitespace);
        let token_raw = parts.next().unwrap_or("");
        let token = token_raw.to_ascii_uppercase();
        let action = parts
            .next()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string());
        match token.as_str() {
            "EXIT" => ToolCommand::WheneverSqlError { exit: true, action },
            "CONTINUE" => ToolCommand::WheneverSqlError {
                exit: false,
                action,
            },
            _ => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "WHENEVER SQLERROR supports EXIT or CONTINUE.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_whenever_oserror_command(raw: &str) -> ToolCommand {
        let rest = raw[16..].trim();
        if rest.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "WHENEVER OSERROR requires EXIT or CONTINUE.".to_string(),
                is_error: true,
            };
        }

        let mut parts = rest.splitn(2, char::is_whitespace);
        let token = parts.next().unwrap_or("").to_ascii_uppercase();
        let extra = parts.next().map(str::trim).unwrap_or("");

        if !extra.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "WHENEVER OSERROR supports only EXIT or CONTINUE.".to_string(),
                is_error: true,
            };
        }

        match token.as_str() {
            "EXIT" => ToolCommand::WheneverOsError { exit: true },
            "CONTINUE" => ToolCommand::WheneverOsError { exit: false },
            _ => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "WHENEVER OSERROR supports EXIT or CONTINUE.".to_string(),
                is_error: true,
            },
        }
    }

    fn parse_connect_command(raw: &str) -> ToolCommand {
        // CONNECT syntax: CONNECT user/password@host:port/service_name
        // or: CONNECT user/password@//host:port/service_name
        let raw_upper = raw.to_ascii_uppercase();
        let rest = if raw_upper.starts_with("CONNECT") {
            raw[7..].trim()
        } else if raw_upper.starts_with("CONN") {
            raw[4..].trim()
        } else {
            raw.trim()
        };

        if rest.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "CONNECT requires connection string: user/password@host:port/service_name"
                    .to_string(),
                is_error: true,
            };
        }

        // Split by the last @ so passwords containing @ are preserved.
        let Some((credentials_raw, conn_str_raw)) = rest.rsplit_once('@') else {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "Invalid CONNECT syntax. Expected: user/password@host:port/service_name"
                    .to_string(),
                is_error: true,
            };
        };

        // Parse credentials (user/password)
        // Split at the first slash so passwords containing / are preserved.
        let Some((username_raw, password_raw)) = credentials_raw.split_once('/') else {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "Invalid credentials. Expected: user/password".to_string(),
                is_error: true,
            };
        };

        let username = username_raw.trim().to_string();
        let password = password_raw.trim().to_string();

        if username.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "Username cannot be empty".to_string(),
                is_error: true,
            };
        }

        // Parse connection string (//host:port/service_name or host:port/service_name)
        let conn_str = conn_str_raw.trim();
        let conn_str = conn_str.strip_prefix("//").unwrap_or(conn_str);

        // Split by / to separate host:port from service_name
        let conn_parts: Vec<&str> = conn_str.splitn(2, '/').collect();
        if conn_parts.len() != 2 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "Invalid connection string. Expected: host:port/service_name".to_string(),
                is_error: true,
            };
        }

        let service_name = conn_parts[1].trim().to_string();

        // Parse host:port
        let host_port: Vec<&str> = conn_parts[0].splitn(2, ':').collect();
        let host = host_port[0].trim().to_string();
        let port = if host_port.len() == 2 {
            match host_port[1].trim().parse::<u16>() {
                Ok(p) => p,
                Err(_) => {
                    return ToolCommand::Unsupported {
                        raw: raw.to_string(),
                        message: "Invalid port number".to_string(),
                        is_error: true,
                    };
                }
            }
        } else {
            1521 // Default Oracle port
        };

        if host.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "Host cannot be empty".to_string(),
                is_error: true,
            };
        }

        if service_name.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "Service name cannot be empty".to_string(),
                is_error: true,
            };
        }

        ToolCommand::Connect {
            username,
            password,
            host,
            port,
            service_name,
        }
    }

    fn parse_errorcontinue_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET ERRORCONTINUE", |enabled| {
            ToolCommand::SetErrorContinue { enabled }
        })
    }

    fn parse_autocommit_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET AUTOCOMMIT", |enabled| {
            ToolCommand::SetAutoCommit { enabled }
        })
    }

    fn parse_define_command(raw: &str) -> ToolCommand {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        if tokens.len() < 3 {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET DEFINE requires ON, OFF, or a substitution character (e.g. '^')."
                    .to_string(),
                is_error: true,
            };
        }

        let mode = tokens[2].to_ascii_uppercase();
        match mode.as_str() {
            "ON" => ToolCommand::SetDefine {
                enabled: true,
                define_char: None,
            },
            "OFF" => ToolCommand::SetDefine {
                enabled: false,
                define_char: None,
            },
            _ => {
                // Accept a single character, optionally wrapped in single quotes: '^' or ^
                let raw_arg = tokens[2];
                let ch = if let Some(inner) = raw_arg
                    .strip_prefix('\'')
                    .and_then(|value| value.strip_suffix('\''))
                {
                    let mut chars = inner.chars();
                    match (chars.next(), chars.next()) {
                        (Some(ch), None) => Some(ch),
                        _ => None,
                    }
                } else {
                    let mut chars = raw_arg.chars();
                    match (chars.next(), chars.next()) {
                        (Some(ch), None) => Some(ch),
                        _ => None,
                    }
                };

                match ch {
                    Some(c) => ToolCommand::SetDefine { enabled: true, define_char: Some(c) },
                    None => ToolCommand::Unsupported {
                        raw: raw.to_string(),
                        message: "SET DEFINE requires ON, OFF, or a single substitution character (e.g. '^').".to_string(),
                        is_error: true,
                    },
                }
            }
        }
    }

    fn parse_scan_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET SCAN", |enabled| ToolCommand::SetScan { enabled })
    }

    fn parse_verify_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET VERIFY", |enabled| ToolCommand::SetVerify {
            enabled,
        })
    }

    fn parse_echo_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET ECHO", |enabled| ToolCommand::SetEcho { enabled })
    }

    fn parse_timing_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET TIMING", |enabled| ToolCommand::SetTiming {
            enabled,
        })
    }

    fn parse_feedback_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET FEEDBACK", |enabled| ToolCommand::SetFeedback {
            enabled,
        })
    }

    fn parse_heading_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET HEADING", |enabled| ToolCommand::SetHeading {
            enabled,
        })
    }

    fn parse_pagesize_command(raw: &str) -> ToolCommand {
        Self::parse_set_number_command(raw, "SET PAGESIZE", |size| ToolCommand::SetPageSize {
            size,
        })
    }

    fn parse_linesize_command(raw: &str) -> ToolCommand {
        Self::parse_set_number_command(raw, "SET LINESIZE", |size| ToolCommand::SetLineSize {
            size,
        })
    }

    fn parse_trimspool_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET TRIMSPOOL", |enabled| ToolCommand::SetTrimSpool {
            enabled,
        })
    }

    fn parse_trimout_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET TRIMOUT", |enabled| ToolCommand::SetTrimOut {
            enabled,
        })
    }

    fn parse_sqlblanklines_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET SQLBLANKLINES", |enabled| {
            ToolCommand::SetSqlBlankLines { enabled }
        })
    }

    fn parse_tab_command(raw: &str) -> ToolCommand {
        Self::parse_set_on_off_command(raw, "SET TAB", |enabled| ToolCommand::SetTab { enabled })
    }

    fn parse_set_on_off_command<F>(
        raw: &str,
        command_name: &str,
        enabled_to_command: F,
    ) -> ToolCommand
    where
        F: FnOnce(bool) -> ToolCommand,
    {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        let Some(mode) = tokens.get(2) else {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: format!("{command_name} requires ON or OFF."),
                is_error: true,
            };
        };

        if mode.eq_ignore_ascii_case("ON") {
            return enabled_to_command(true);
        }

        if mode.eq_ignore_ascii_case("OFF") {
            return enabled_to_command(false);
        }

        ToolCommand::Unsupported {
            raw: raw.to_string(),
            message: format!("{command_name} supports only ON or OFF."),
            is_error: true,
        }
    }

    fn is_sqlplus_set_command(upper_line: &str) -> bool {
        if !upper_line.starts_with("SET") {
            return false;
        }

        let mut parts = upper_line.split_whitespace();
        let Some(first) = parts.next() else {
            return false;
        };

        if first != "SET" {
            return false;
        }

        let Some(second) = parts.next() else {
            return false;
        };

        sql_text::is_sqlplus_set_option_keyword(second)
    }

    fn parse_set_number_command<F>(
        raw: &str,
        command_name: &str,
        value_to_command: F,
    ) -> ToolCommand
    where
        F: FnOnce(u32) -> ToolCommand,
    {
        let tokens: Vec<&str> = raw.split_whitespace().collect();
        let Some(raw_value) = tokens.get(2) else {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: format!("{command_name} requires a number."),
                is_error: true,
            };
        };

        match raw_value.parse::<u32>() {
            Ok(value) => value_to_command(value),
            Err(_) => ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: format!("{command_name} requires a number."),
                is_error: true,
            },
        }
    }

    fn parse_colsep_command(raw: &str) -> ToolCommand {
        let rest = raw[10..].trim();
        if rest.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET COLSEP requires a separator string.".to_string(),
                is_error: true,
            };
        }

        let separator = rest.trim_matches('\'').trim_matches('"').to_string();
        ToolCommand::SetColSep { separator }
    }

    fn parse_null_command(raw: &str) -> ToolCommand {
        let rest = raw[8..].trim();
        if rest.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: "SET NULL requires a display value.".to_string(),
                is_error: true,
            };
        }

        let null_text = rest.trim_matches('\'').trim_matches('"').to_string();
        ToolCommand::SetNull { null_text }
    }

    fn parse_script_command(raw: &str) -> ToolCommand {
        let trimmed = raw.trim();
        let (relative_to_caller, command_label, path) = if trimmed.starts_with("@@") {
            let remainder = trimmed.trim_start_matches("@@").trim();
            (true, "@@", Self::extract_script_path(remainder))
        } else if trimmed.starts_with('@') {
            let remainder = trimmed.trim_start_matches('@').trim();
            (false, "@", Self::extract_script_path(remainder))
        } else if Self::is_start_script_command(trimmed) {
            let start = Self::remainder_after_first_word(trimmed).unwrap_or(trimmed.len());
            let remainder = trimmed.get(start..).unwrap_or_default().trim();
            (false, "START", Self::extract_script_path(remainder))
        } else if Self::is_run_script_command(trimmed) {
            let start = Self::remainder_after_first_word(trimmed).unwrap_or(trimmed.len());
            let remainder = trimmed.get(start..).unwrap_or_default().trim();
            (false, "RUN", Self::extract_script_path(remainder))
        } else {
            (false, "@", "")
        };

        if path.is_empty() {
            return ToolCommand::Unsupported {
                raw: raw.to_string(),
                message: if command_label == "START" {
                    "START requires a path.".to_string()
                } else if command_label == "RUN" {
                    "RUN requires a path.".to_string()
                } else {
                    "@file.sql requires a path.".to_string()
                },
                is_error: true,
            };
        }

        let cleaned = path.trim_matches('"').trim_matches('\'').to_string();

        ToolCommand::RunScript {
            path: cleaned,
            relative_to_caller,
        }
    }

    fn remainder_after_first_word(line: &str) -> Option<usize> {
        let mut in_word = false;
        for (idx, ch) in line.char_indices() {
            if !in_word {
                if ch.is_whitespace() {
                    continue;
                }
                in_word = true;
                continue;
            }

            if ch.is_whitespace() {
                return Some(idx);
            }
        }

        in_word.then_some(line.len())
    }

    fn extract_script_path(remainder: &str) -> &str {
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut idx = 0usize;

        while idx < remainder.len() {
            let tail = match remainder.get(idx..) {
                Some(value) => value,
                None => break,
            };
            let Some(ch) = tail.chars().next() else {
                break;
            };

            if !in_single_quote && !in_double_quote {
                if tail.starts_with("--") {
                    return remainder.get(..idx).unwrap_or_default().trim();
                }
                if tail.starts_with("/*") {
                    return remainder.get(..idx).unwrap_or_default().trim();
                }
            }

            if ch == '\'' && !in_double_quote {
                in_single_quote = !in_single_quote;
            } else if ch == '"' && !in_single_quote {
                in_double_quote = !in_double_quote;
            }

            idx += ch.len_utf8();
        }

        remainder.trim()
    }

    fn is_start_script_command(trimmed: &str) -> bool {
        let Some(first_word) = Self::next_meaningful_word(trimmed, 0) else {
            return false;
        };

        if !first_word.eq_ignore_ascii_case("START") {
            return false;
        }

        let second_word = Self::next_meaningful_word(trimmed, 1);
        if second_word.is_none() {
            return true;
        }

        // Hierarchical query clause "START WITH <expr>" must stay as SQL,
        // but SQL*Plus still allows a script file literally named "with".
        let third_word = Self::next_meaningful_word(trimmed, 2);
        !(second_word.is_some_and(|word| word.eq_ignore_ascii_case("WITH")) && third_word.is_some())
    }

    fn is_connect_command(trimmed: &str) -> bool {
        let Some(first_word) = Self::next_meaningful_word(trimmed, 0) else {
            return false;
        };

        if !first_word.eq_ignore_ascii_case("CONNECT") {
            return false;
        }

        !Self::next_meaningful_word(trimmed, 1).is_some_and(|word| word.eq_ignore_ascii_case("BY"))
    }

    fn next_meaningful_word(line: &str, skip_words: usize) -> Option<&str> {
        let mut idx = 0usize;
        let mut seen_words = 0usize;

        while idx < line.len() {
            let ch = line.get(idx..)?.chars().next()?;
            let ch_len = ch.len_utf8();

            if ch.is_whitespace() {
                idx += ch_len;
                continue;
            }

            if line.get(idx..)?.starts_with("--") {
                return None;
            }

            if line.get(idx..)?.starts_with("/*") {
                let comment_start = idx + 2;
                let comment_tail = line.get(comment_start..)?;
                let comment_len = comment_tail.find("*/")?;
                idx = comment_start + comment_len + 2;
                continue;
            }

            let mut end = idx;
            while end < line.len() {
                let word_ch = line.get(end..)?.chars().next()?;
                if word_ch.is_whitespace()
                    || line.get(end..)?.starts_with("/*")
                    || line.get(end..)?.starts_with("--")
                {
                    break;
                }
                end += word_ch.len_utf8();
            }

            if seen_words == skip_words {
                return line.get(idx..end);
            }

            seen_words += 1;
            idx = end;
        }

        None
    }

    fn is_run_script_command(trimmed: &str) -> bool {
        let mut parts = trimmed.split_whitespace();
        let Some(first) = parts.next() else {
            return false;
        };

        let is_alias = first.eq_ignore_ascii_case("RUN") || first.eq_ignore_ascii_case("R");
        if !is_alias {
            return false;
        }

        parts.next().is_some()
    }

    fn is_word_command(upper: &str, command: &str) -> bool {
        if upper == command {
            return true;
        }
        upper
            .strip_prefix(command)
            .and_then(|tail| tail.chars().next())
            .map(|ch| ch.is_whitespace())
            .unwrap_or(false)
    }

    fn parse_bind_type(type_str: &str) -> Result<BindDataType, String> {
        let trimmed = type_str.trim();
        if trimmed.is_empty() {
            return Err("VAR requires a data type.".to_string());
        }

        let upper = trimmed.to_ascii_uppercase();
        let compact = upper.replace(' ', "");

        if compact == "REFCURSOR" || compact == "SYS_REFCURSOR" {
            return Ok(BindDataType::RefCursor);
        }

        if upper.starts_with("NUMBER") || upper.starts_with("NUMERIC") {
            return Ok(BindDataType::Number);
        }

        if upper.starts_with("DATE") {
            return Ok(BindDataType::Date);
        }

        if upper.starts_with("TIMESTAMP") {
            let precision = Self::parse_parenthesized_u8(&upper).unwrap_or(6);
            return Ok(BindDataType::Timestamp(precision));
        }

        if upper.starts_with("CLOB") {
            return Ok(BindDataType::Clob);
        }

        if upper.starts_with("VARCHAR2")
            || upper.starts_with("VARCHAR")
            || upper.starts_with("NVARCHAR2")
        {
            let size = Self::parse_parenthesized_u32(&upper).unwrap_or(4000);
            return Ok(BindDataType::Varchar2(size));
        }

        if upper.starts_with("CHAR") || upper.starts_with("NCHAR") {
            let size = Self::parse_parenthesized_u32(&upper).unwrap_or(2000);
            return Ok(BindDataType::Varchar2(size));
        }

        Err(format!("Unsupported VAR type: {}", trimmed))
    }

    fn parse_parenthesized_u32(value: &str) -> Option<u32> {
        let start = value.find('(')?;
        let end = value[start + 1..].find(')')? + start + 1;
        value[start + 1..end].trim().parse::<u32>().ok()
    }

    fn parse_parenthesized_u8(value: &str) -> Option<u8> {
        let start = value.find('(')?;
        let end = value[start + 1..].find(')')? + start + 1;
        value[start + 1..end].trim().parse::<u8>().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::QueryExecutor;

    #[test]
    fn strip_extra_trailing_semicolons_preserves_plsql_end_terminator() {
        assert_eq!(
            QueryExecutor::strip_extra_trailing_semicolons("BEGIN NULL; END;;"),
            "BEGIN NULL; END;"
        );
        assert_eq!(
            QueryExecutor::strip_extra_trailing_semicolons("END pkg_name;;"),
            "END pkg_name;"
        );
    }

    #[test]
    fn strip_extra_trailing_semicolons_does_not_keep_semicolon_for_end_substring() {
        assert_eq!(
            QueryExecutor::strip_extra_trailing_semicolons("SELECT 'WEEKEND REPORT' FROM dual;;"),
            "SELECT 'WEEKEND REPORT' FROM dual"
        );
    }

    #[test]
    fn with_function_select_is_recognized_as_select_statement() {
        let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
SELECT f() FROM dual";

        assert!(
            QueryExecutor::is_select_statement(sql),
            "WITH FUNCTION ... SELECT should be treated as SELECT"
        );
    }

    #[test]
    fn with_procedure_select_is_recognized_as_select_statement() {
        let sql = "WITH
  PROCEDURE p IS
  BEGIN
    NULL;
  END;
SELECT 1 FROM dual";

        assert!(
            QueryExecutor::is_select_statement(sql),
            "WITH PROCEDURE ... SELECT should be treated as SELECT"
        );
    }

    #[test]
    fn with_function_followed_by_new_statement_head_is_not_select_statement() {
        let sql = "WITH
  FUNCTION f RETURN NUMBER IS
  BEGIN
    RETURN 1;
  END;
ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'";

        assert!(
            !QueryExecutor::is_select_statement(sql),
            "WITH FUNCTION followed by non-query statement head should not be SELECT"
        );
    }

    #[test]
    fn parse_tool_command_start_with_inline_comment_is_not_script_command() {
        assert!(
            QueryExecutor::parse_tool_command("START /*tree*/ WITH manager_id IS NULL").is_none(),
            "hierarchical START WITH with inline comment must remain SQL"
        );
    }

    #[test]
    fn parse_tool_command_connect_by_with_inline_comment_is_not_connect_command() {
        assert!(
            QueryExecutor::parse_tool_command(
                "CONNECT /*hierarchical*/ BY PRIOR employee_id = manager_id"
            )
            .is_none(),
            "hierarchical CONNECT BY with inline comment must remain SQL"
        );
    }
}
