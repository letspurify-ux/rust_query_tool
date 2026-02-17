//! 공통 SQL 텍스트 파싱 유틸리티
//!
//! 실행, 인텔리센스, 포맷팅에서 공통으로 쓰는 SQL 텍스트 분석 로직을
//! 한 곳에 모아 중복을 줄입니다.
use crate::db::{FormatItem, QueryExecutor, ScriptItem, SplitState};
use crate::ui::sql_editor::SqlToken;

/// SQL 문자열을 토큰 단위로 분해합니다.
///
/// 기존 에디터 토크나이저 동작(문자열, 주석, 라벨, 심벌 처리)을 유지합니다.
pub(crate) fn tokenize_sql(sql: &str) -> Vec<SqlToken> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;
    let mut current = String::new();
    let mut scan_state = SplitState::default();
    let mut pending_newline = false;

    let flush_word = |current: &mut String, tokens: &mut Vec<SqlToken>| {
        if !current.is_empty() {
            tokens.push(SqlToken::Word(std::mem::take(current)));
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

        // Handle nq-quoted strings: nq'[...]', nq'{...}', etc. (National Character Set)
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
            debug_assert_eq!(scan_state.q_quote_end(), Some(match delimiter {
                '[' => ']',
                '{' => '}',
                '(' => ')',
                '<' => '>',
                _ => delimiter,
            }));
            i += 4;
            continue;
        }

        // Handle q-quoted strings: q'[...]', q'{...}', q'(...)', q'<...>', q'!...!'
        if (c == 'q' || c == 'Q') && next == Some('\'') && i + 2 < chars.len() {
            let delimiter = chars[i + 2];
            flush_word(&mut current, &mut tokens);
            current.push(c);
            current.push('\'');
            current.push(delimiter);
            scan_state.start_q_quote(delimiter);
            debug_assert_eq!(scan_state.q_quote_end(), Some(match delimiter {
                '[' => ']',
                '{' => '}',
                '(' => ')',
                '<' => '>',
                _ => delimiter,
            }));
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

        if c.is_ascii_alphanumeric() || c == '_' || c == '$' || c == '#' {
            current.push(c);
            i += 1;
            continue;
        }

        flush_word(&mut current, &mut tokens);

        // Handle <<label>> (Oracle PL/SQL labels)
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
        let upper = trimmed.to_uppercase();
        upper.starts_with("CONNECT")
            || upper.starts_with("CONN ")
            || upper.starts_with("DISCONNECT")
            || upper.starts_with("DISC")
            || trimmed.starts_with('@')
    })
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
        if let Some(ScriptItem::Statement(stmt)) = items.into_iter().find(|item| {
            matches!(item, ScriptItem::Statement(_))
        }) {
            return stmt;
        }
    }
    statement.to_string()
}

pub(crate) fn split_format_items(sql: &str) -> Vec<FormatItem> {
    QueryExecutor::split_format_items(sql)
}
