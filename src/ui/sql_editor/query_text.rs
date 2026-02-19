//! 공통 SQL 텍스트 파싱 유틸리티
//!
//! 실행, 인텔리센스, 포맷팅에서 공통으로 쓰는 SQL 텍스트 분석 로직을
//! 한 곳에 모아 중복을 줄입니다.
use crate::db::{FormatItem, QueryExecutor, ScriptItem, SplitState, ToolCommand};
use crate::sql_text;
use crate::ui::sql_editor::{SqlToken, SqlTokenSpan};

/// SQL 문자열을 토큰 단위로 분해합니다.
///
/// 기존 에디터 토크나이저 동작(문자열, 주석, 라벨, 심벌 처리)을 유지합니다.
pub(crate) fn tokenize_sql(sql: &str) -> Vec<SqlToken> {
    tokenize_sql_spanned(sql)
        .into_iter()
        .map(|span| span.token)
        .collect()
}

pub(crate) fn tokenize_sql_spanned(sql: &str) -> Vec<SqlTokenSpan> {
    let mut tokens = Vec::new();
    let mut iter = sql.char_indices().peekable();
    let mut current = String::new();
    let mut current_start = 0usize;
    let mut scan_state = SplitState::default();
    let mut pending_newline = true;

    let flush_word = |current: &mut String,
                      current_start: &mut usize,
                      end: usize,
                      tokens: &mut Vec<SqlTokenSpan>| {
        if !current.is_empty() {
            tokens.push(SqlTokenSpan {
                token: SqlToken::Word(std::mem::take(current)),
                start: *current_start,
                end,
            });
        }
    };

    let is_sqlplus_line_comment = |sql: &str, start: usize, keyword: &str| -> bool {
        if start >= sql.len() {
            return false;
        }
        let rest = &sql[start..];
        if rest.len() < keyword.len() {
            return false;
        }
        for (left, right) in rest.bytes().zip(keyword.bytes()).take(keyword.len()) {
            if !left.eq_ignore_ascii_case(&right) {
                return false;
            }
        }
        match rest[keyword.len()..].chars().next() {
            None => true,
            Some(ch) => ch.is_whitespace(),
        }
    };

    let peek_n_char = |iter: &std::iter::Peekable<std::str::CharIndices<'_>>, n: usize| {
        let mut lookahead = iter.clone();
        for _ in 0..n {
            lookahead.next()?;
        }
        lookahead.next().map(|(_, ch)| ch)
    };

    while let Some((idx, c)) = iter.next() {
        let next = iter.peek().map(|(_, ch)| *ch);

        if scan_state.in_line_comment {
            current.push(c);
            if c == '\n' {
                tokens.push(SqlTokenSpan {
                    token: SqlToken::Comment(std::mem::take(&mut current)),
                    start: current_start,
                    end: idx + c.len_utf8(),
                });
                scan_state.in_line_comment = false;
                pending_newline = true;
            }
            continue;
        }

        if scan_state.in_block_comment {
            current.push(c);
            if c == '*' && next == Some('/') {
                iter.next();
                current.push('/');
                let mut end = idx + 2;
                if let Some((nl_idx, '\n')) = iter.peek().copied() {
                    iter.next();
                    current.push('\n');
                    end = nl_idx + 1;
                }
                tokens.push(SqlTokenSpan {
                    token: SqlToken::Comment(std::mem::take(&mut current)),
                    start: current_start,
                    end,
                });
                scan_state.in_block_comment = false;
                continue;
            }
            continue;
        }

        if scan_state.in_q_quote {
            current.push(c);
            if Some(c) == scan_state.q_quote_end() && next == Some('\'') {
                iter.next();
                current.push('\'');
                tokens.push(SqlTokenSpan {
                    token: SqlToken::String(std::mem::take(&mut current)),
                    start: current_start,
                    end: idx + 2,
                });
                scan_state.in_q_quote = false;
                scan_state.q_quote_end = None;
                continue;
            }
            continue;
        }

        if scan_state.in_single_quote {
            current.push(c);
            if c == '\'' {
                if next == Some('\'') {
                    iter.next();
                    current.push('\'');
                    continue;
                }
                tokens.push(SqlTokenSpan {
                    token: SqlToken::String(std::mem::take(&mut current)),
                    start: current_start,
                    end: idx + 1,
                });
                scan_state.in_single_quote = false;
                continue;
            }
            continue;
        }

        if scan_state.in_double_quote {
            current.push(c);
            if c == '"' {
                if next == Some('"') {
                    iter.next();
                    current.push('"');
                    continue;
                }
                tokens.push(SqlTokenSpan {
                    token: SqlToken::Word(std::mem::take(&mut current)),
                    start: current_start,
                    end: idx + 1,
                });
                scan_state.in_double_quote = false;
                continue;
            }
            continue;
        }

        if c.is_whitespace() {
            flush_word(&mut current, &mut current_start, idx, &mut tokens);
            if c == '\n' {
                pending_newline = true;
            }
            continue;
        }

        if pending_newline
            && (is_sqlplus_line_comment(sql, idx, "REMARK")
                || is_sqlplus_line_comment(sql, idx, "REM"))
        {
            flush_word(&mut current, &mut current_start, idx, &mut tokens);
            scan_state.in_line_comment = true;
            current_start = idx;
            if pending_newline {
                current.push('\n');
            }
            current.push(c);
            pending_newline = false;
            continue;
        }

        if c == '-' && next == Some('-') {
            flush_word(&mut current, &mut current_start, idx, &mut tokens);
            scan_state.in_line_comment = true;
            current_start = idx;
            if pending_newline {
                current.push('\n');
            }
            current.push('-');
            current.push('-');
            pending_newline = false;
            iter.next();
            continue;
        }

        if c == '/' && next == Some('*') {
            flush_word(&mut current, &mut current_start, idx, &mut tokens);
            scan_state.in_block_comment = true;
            current_start = idx;
            if pending_newline {
                current.push('\n');
            }
            current.push('/');
            current.push('*');
            pending_newline = false;
            iter.next();
            continue;
        }

        pending_newline = false;

        if (c == 'n' || c == 'N')
            && (next == Some('q') || next == Some('Q'))
            && peek_n_char(&iter, 1) == Some('\'')
            && peek_n_char(&iter, 2).is_some()
        {
            let delimiter = peek_n_char(&iter, 2).expect("checked is_some");
            flush_word(&mut current, &mut current_start, idx, &mut tokens);
            current_start = idx;
            current.push(c);
            current.push(next.expect("checked above"));
            current.push('\'');
            current.push(delimiter);
            scan_state.start_q_quote(delimiter);
            debug_assert_eq!(
                scan_state.q_quote_end(),
                Some(sql_text::q_quote_closing(delimiter))
            );
            iter.next();
            iter.next();
            iter.next();
            continue;
        }

        if (c == 'q' || c == 'Q') && next == Some('\'') && peek_n_char(&iter, 1).is_some() {
            let delimiter = peek_n_char(&iter, 1).expect("checked is_some");
            flush_word(&mut current, &mut current_start, idx, &mut tokens);
            current_start = idx;
            current.push(c);
            current.push('\'');
            current.push(delimiter);
            scan_state.start_q_quote(delimiter);
            debug_assert_eq!(
                scan_state.q_quote_end(),
                Some(sql_text::q_quote_closing(delimiter))
            );
            iter.next();
            iter.next();
            continue;
        }

        if c == '\'' {
            flush_word(&mut current, &mut current_start, idx, &mut tokens);
            current_start = idx;
            scan_state.in_single_quote = true;
            current.push('\'');
            continue;
        }

        if c == '"' {
            flush_word(&mut current, &mut current_start, idx, &mut tokens);
            current_start = idx;
            scan_state.in_double_quote = true;
            current.push('"');
            continue;
        }

        if sql_text::is_identifier_char(c) {
            if current.is_empty() {
                current_start = idx;
            }
            current.push(c);
            continue;
        }

        flush_word(&mut current, &mut current_start, idx, &mut tokens);

        if c == '<' && next == Some('<') {
            let mut label = String::from("<<");
            iter.next();
            let mut end = idx + 2;
            while let Some((label_idx, ch)) = iter.next() {
                label.push(ch);
                end = label_idx + ch.len_utf8();
                if ch == '>' && iter.peek().map(|(_, c)| *c) == Some('>') {
                    iter.next();
                    label.push('>');
                    end += 1;
                    break;
                }
            }
            tokens.push(SqlTokenSpan {
                token: SqlToken::Word(label),
                start: idx,
                end,
            });
            continue;
        }

        let sym = match (c, next) {
            ('<', Some('=')) => Some("<=".to_string()),
            ('>', Some('=')) => Some(">=".to_string()),
            ('<', Some('>')) => Some("<>".to_string()),
            ('!', Some('=')) => Some("!=".to_string()),
            ('|', Some('|')) => Some("||".to_string()),
            (':', Some('=')) => Some(":=".to_string()),
            ('=', Some('>')) => Some("=>".to_string()),
            _ => None,
        };

        if let Some(sym) = sym {
            tokens.push(SqlTokenSpan {
                token: SqlToken::Symbol(sym),
                start: idx,
                end: idx + 2,
            });
            iter.next();
            continue;
        }

        tokens.push(SqlTokenSpan {
            token: SqlToken::Symbol(c.to_string()),
            start: idx,
            end: idx + c.len_utf8(),
        });
    }

    if scan_state.in_line_comment || scan_state.in_block_comment {
        if !current.is_empty() {
            tokens.push(SqlTokenSpan {
                token: SqlToken::Comment(std::mem::take(&mut current)),
                start: current_start,
                end: sql.len(),
            });
        }
    } else if scan_state.in_single_quote || scan_state.in_q_quote {
        if !current.is_empty() {
            tokens.push(SqlTokenSpan {
                token: SqlToken::String(std::mem::take(&mut current)),
                start: current_start,
                end: sql.len(),
            });
        }
    } else if scan_state.in_double_quote {
        if !current.is_empty() {
            tokens.push(SqlTokenSpan {
                token: SqlToken::Word(std::mem::take(&mut current)),
                start: current_start,
                end: sql.len(),
            });
        }
    } else {
        flush_word(&mut current, &mut current_start, sql.len(), &mut tokens);
    }

    tokens
}

/// 현재 커서 위치가 포함된 문장을 문자열로 반환합니다.
///
/// 실제 구문 경계 계산은 실행기 쪽 규칙을 그대로 사용해 동작 일관성을 유지합니다.
pub(crate) fn statement_at_cursor(sql: &str, cursor_pos: usize) -> Option<String> {
    QueryExecutor::statement_at_cursor(sql, cursor_pos)
}

/// 현재 커서 위치가 속한 문장의 바이트 범위를 반환합니다.
///
/// SQL*Plus 스타일 단독 `/` 구분자, tool command 문맥까지 포함한 경계 판정은
/// `QueryExecutor`의 기존 규칙을 재사용합니다.
pub(crate) fn statement_bounds_in_text(sql: &str, cursor_pos: usize) -> (usize, usize) {
    QueryExecutor::statement_bounds_at_cursor(sql, cursor_pos).unwrap_or((0, sql.len()))
}

/// 쿼리 실행 전 선처리에서 `CONNECT`, `DISCONNECT`, 또는 `@` 스크립트 실행 명령이
/// 포함되는지 판별합니다. 해당 라인은 기존 연결 유무와 무관하게 실행을 허용합니다.
pub(crate) fn has_connection_bootstrap_command(sql: &str) -> bool {
    sql.lines().any(|line| {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return false;
        }

        match QueryExecutor::parse_tool_command(trimmed) {
            Some(ToolCommand::Connect { .. })
            | Some(ToolCommand::Disconnect)
            | Some(ToolCommand::RunScript { .. }) => true,
            Some(ToolCommand::Unsupported { raw, .. }) => {
                let upper = raw.trim().to_uppercase();
                upper == "CONNECT"
                    || (upper.starts_with("CONNECT ") && !upper.starts_with("CONNECT BY"))
                    || upper.starts_with("CONN ")
                    || upper == "DISCONNECT"
                    || upper == "DISC"
                    || raw.trim_start().starts_with('@')
            }
            _ => false,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{has_connection_bootstrap_command, tokenize_sql};
    use crate::db::SplitState;
    use crate::sql_text;
    use crate::ui::sql_editor::SqlToken;

    fn tokenize_sql_reference(sql: &str) -> Vec<SqlToken> {
        let mut tokens = Vec::new();
        let chars: Vec<char> = sql.chars().collect();
        let mut i = 0;
        let mut current = String::new();
        let mut scan_state = SplitState::default();
        let mut pending_newline = true;

        let flush_word = |current: &mut String, tokens: &mut Vec<SqlToken>| {
            if !current.is_empty() {
                tokens.push(SqlToken::Word(std::mem::take(current)));
            }
        };

        let is_sqlplus_line_comment = |chars: &[char], start: usize, keyword: &str| -> bool {
            if start >= chars.len() {
                return false;
            }
            let keyword_chars = keyword.chars().collect::<Vec<_>>();
            if start + keyword_chars.len() > chars.len() {
                return false;
            }
            for (idx, kw_char) in keyword_chars.iter().enumerate() {
                if !chars[start + idx].eq_ignore_ascii_case(kw_char) {
                    return false;
                }
            }
            match chars.get(start + keyword_chars.len()) {
                None => true,
                Some(ch) => ch.is_whitespace(),
            }
        };

        while i < chars.len() {
            let c = chars[i];
            let next = if i + 1 < chars.len() {
                Some(chars[i + 1])
            } else {
                None
            };

            if scan_state.in_line_comment {
                current.push(c);
                if c == '\n' {
                    tokens.push(SqlToken::Comment(std::mem::take(&mut current)));
                    scan_state.in_line_comment = false;
                    pending_newline = true;
                }
                i += 1;
                continue;
            }

            if scan_state.in_block_comment {
                current.push(c);
                if c == '*' && next == Some('/') {
                    current.push('/');
                    if i + 2 < chars.len() && chars[i + 2] == '\n' {
                        current.push('\n');
                        i += 1;
                    }
                    tokens.push(SqlToken::Comment(std::mem::take(&mut current)));
                    scan_state.in_block_comment = false;
                    i += 2;
                    continue;
                }
                i += 1;
                continue;
            }

            if scan_state.in_q_quote {
                current.push(c);
                if Some(c) == scan_state.q_quote_end() && next == Some('\'') {
                    current.push('\'');
                    tokens.push(SqlToken::String(std::mem::take(&mut current)));
                    scan_state.in_q_quote = false;
                    scan_state.q_quote_end = None;
                    i += 2;
                    continue;
                }
                i += 1;
                continue;
            }

            if scan_state.in_single_quote {
                current.push(c);
                if c == '\'' {
                    if next == Some('\'') {
                        current.push('\'');
                        i += 2;
                        continue;
                    }
                    tokens.push(SqlToken::String(std::mem::take(&mut current)));
                    scan_state.in_single_quote = false;
                    i += 1;
                    continue;
                }
                i += 1;
                continue;
            }

            if scan_state.in_double_quote {
                current.push(c);
                if c == '"' {
                    if next == Some('"') {
                        current.push('"');
                        i += 2;
                        continue;
                    }
                    tokens.push(SqlToken::Word(std::mem::take(&mut current)));
                    scan_state.in_double_quote = false;
                    i += 1;
                    continue;
                }
                i += 1;
                continue;
            }

            if c.is_whitespace() {
                flush_word(&mut current, &mut tokens);
                if c == '\n' {
                    pending_newline = true;
                }
                i += 1;
                continue;
            }

            if pending_newline
                && (is_sqlplus_line_comment(&chars, i, "REMARK")
                    || is_sqlplus_line_comment(&chars, i, "REM"))
            {
                flush_word(&mut current, &mut tokens);
                scan_state.in_line_comment = true;
                if pending_newline {
                    current.push('\n');
                }
                current.push(c);
                pending_newline = false;
                i += 1;
                continue;
            }

            if c == '-' && next == Some('-') {
                flush_word(&mut current, &mut tokens);
                scan_state.in_line_comment = true;
                if pending_newline {
                    current.push('\n');
                }
                current.push('-');
                current.push('-');
                pending_newline = false;
                i += 2;
                continue;
            }

            if c == '/' && next == Some('*') {
                flush_word(&mut current, &mut tokens);
                scan_state.in_block_comment = true;
                if pending_newline {
                    current.push('\n');
                }
                current.push('/');
                current.push('*');
                pending_newline = false;
                i += 2;
                continue;
            }

            pending_newline = false;

            if (c == 'n' || c == 'N')
                && (next == Some('q') || next == Some('Q'))
                && i + 2 < chars.len()
                && chars[i + 2] == '\''
                && i + 3 < chars.len()
            {
                let delimiter = chars[i + 3];
                flush_word(&mut current, &mut tokens);
                current.push(c);
                current.push(chars[i + 1]);
                current.push('\'');
                current.push(delimiter);
                scan_state.start_q_quote(delimiter);
                debug_assert_eq!(
                    scan_state.q_quote_end(),
                    Some(sql_text::q_quote_closing(delimiter))
                );
                i += 4;
                continue;
            }

            if (c == 'q' || c == 'Q') && next == Some('\'') && i + 2 < chars.len() {
                let delimiter = chars[i + 2];
                flush_word(&mut current, &mut tokens);
                current.push(c);
                current.push('\'');
                current.push(delimiter);
                scan_state.start_q_quote(delimiter);
                debug_assert_eq!(
                    scan_state.q_quote_end(),
                    Some(sql_text::q_quote_closing(delimiter))
                );
                i += 3;
                continue;
            }

            if c == '\'' {
                flush_word(&mut current, &mut tokens);
                scan_state.in_single_quote = true;
                current.push('\'');
                i += 1;
                continue;
            }

            if c == '"' {
                flush_word(&mut current, &mut tokens);
                scan_state.in_double_quote = true;
                current.push('"');
                i += 1;
                continue;
            }

            if sql_text::is_identifier_char(c) {
                current.push(c);
                i += 1;
                continue;
            }

            flush_word(&mut current, &mut tokens);

            if c == '<' && next == Some('<') {
                let mut label = String::from("<<");
                let mut j = i + 2;
                while j < chars.len() {
                    let ch = chars[j];
                    label.push(ch);
                    if ch == '>' && j + 1 < chars.len() && chars[j + 1] == '>' {
                        label.push('>');
                        j += 2;
                        break;
                    }
                    j += 1;
                }
                tokens.push(SqlToken::Word(label));
                i = j;
                continue;
            }

            let sym = match (c, next) {
                ('<', Some('=')) => Some("<=".to_string()),
                ('>', Some('=')) => Some(">=".to_string()),
                ('<', Some('>')) => Some("<>".to_string()),
                ('!', Some('=')) => Some("!=".to_string()),
                ('|', Some('|')) => Some("||".to_string()),
                (':', Some('=')) => Some(":=".to_string()),
                ('=', Some('>')) => Some("=>".to_string()),
                _ => None,
            };

            if let Some(sym) = sym {
                tokens.push(SqlToken::Symbol(sym));
                i += 2;
                continue;
            }

            tokens.push(SqlToken::Symbol(c.to_string()));
            i += 1;
        }

        if scan_state.in_line_comment || scan_state.in_block_comment {
            if !current.is_empty() {
                tokens.push(SqlToken::Comment(std::mem::take(&mut current)));
            }
        } else if scan_state.in_single_quote || scan_state.in_q_quote {
            if !current.is_empty() {
                tokens.push(SqlToken::String(std::mem::take(&mut current)));
            }
        } else if scan_state.in_double_quote {
            if !current.is_empty() {
                tokens.push(SqlToken::Word(std::mem::take(&mut current)));
            }
        } else {
            flush_word(&mut current, &mut tokens);
        }

        tokens
    }

    #[test]
    fn has_connection_bootstrap_command_detects_connect_and_script_lines() {
        let sql = "SELECT 1 FROM dual;\nCONNECT scott/tiger@localhost:1521/FREE\n@next.sql";
        assert!(has_connection_bootstrap_command(sql));
    }

    #[test]
    fn has_connection_bootstrap_command_ignores_connect_by_clause() {
        let sql = "SELECT level FROM dual CONNECT BY level <= 10";
        assert!(!has_connection_bootstrap_command(sql));
    }

    #[test]
    fn tokenize_sql_treats_sqlplus_rem_comment_as_comment_token() {
        let tokens = tokenize_sql("REM comment line");
        assert!(matches!(tokens.first(), Some(SqlToken::Comment(_))));
    }

    #[test]
    fn tokenize_sql_treats_sqlplus_remark_comment_as_comment_token() {
        let tokens = tokenize_sql("  REMARK line with leading spaces");
        assert!(matches!(tokens.first(), Some(SqlToken::Comment(_))));
    }

    fn normalize_tokens(tokens: Vec<SqlToken>) -> Vec<(&'static str, String)> {
        tokens
            .into_iter()
            .map(|token| match token {
                SqlToken::Word(v) => ("word", v),
                SqlToken::Symbol(v) => ("symbol", v),
                SqlToken::String(v) => ("string", v),
                SqlToken::Comment(v) => ("comment", v),
            })
            .collect()
    }

    #[test]
    fn tokenize_sql_matches_reference_for_regression_cases() {
        let cases = [
            "REM comment line\nSELECT 1 FROM dual;",
            "REMARK 한글 코멘트\nSELECT '가나다' FROM dual;",
            "-- line comment\n/* block comment */\nSELECT q'[한글;테스트]' FROM dual;",
            "BEGIN\n  <<outer_label>>\n  NULL;\nEND;\n/",
            "SELECT nq'{문자열 ''보존''}' AS txt, a<=b, a<>b, a!=b, a||b, c:=d, e=>f FROM dual;",
            "/* 한글 블록\n코멘트 */\nSELECT \"컬럼명\" FROM \"테이블\";",
        ];

        for sql in cases {
            assert_eq!(
                normalize_tokens(tokenize_sql(sql)),
                normalize_tokens(tokenize_sql_reference(sql)),
                "sql: {sql}"
            );
        }
    }
}

/// SQL*Plus 커맨드 라인인지 판별합니다.
///
/// 포맷팅/실행/인텔리센스에서 공통으로 사용하는 선행/단독 라인 규칙을 공유하도록
/// 기존 파서 규칙을 재사용합니다.
pub(crate) fn is_sqlplus_command_line(trimmed_line: &str) -> bool {
    let trimmed = trimmed_line.trim();
    if trimmed.is_empty() {
        return false;
    }
    if trimmed == "/" || trimmed.starts_with("@@") || trimmed.starts_with('@') {
        return true;
    }
    if let Some(first) = trimmed.split_whitespace().next() {
        let first = first.to_uppercase();
        if matches!(first.as_str(), "REM" | "REMARK") {
            return true;
        }
    }
    QueryExecutor::parse_tool_command(trimmed).is_some()
}

/// `QueryExecutor`의 스크립트 분할 규칙을 UI 공통 경로로 위임해
/// 실행/포맷/인텔리센스에서 동일한 기준의 첫 문장을 사용합니다.
pub(crate) fn normalize_single_statement(statement: &str) -> String {
    let items = QueryExecutor::split_script_items(statement);
    if items.len() > 1 {
        if let Some(ScriptItem::Statement(stmt)) = items
            .into_iter()
            .find(|item| matches!(item, ScriptItem::Statement(_)))
        {
            return stmt;
        }
    }
    statement.to_string()
}

pub(crate) fn split_format_items(sql: &str) -> Vec<FormatItem> {
    QueryExecutor::split_format_items(sql)
}
