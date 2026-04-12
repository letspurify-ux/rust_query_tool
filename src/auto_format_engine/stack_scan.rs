use crate::db::QueryExecutor;
use crate::sql_text;

// AutoFormatClauseKind lives in script.rs (canonical definition); re-exported
// through mod.rs so the import path stays module-local.
use super::{AutoFormatClauseKind, EngineLineRecord, Frame, FrameKind};

fn detect_clause_kind(trimmed_upper: &str) -> Option<AutoFormatClauseKind> {
    if sql_text::starts_with_keyword_token(trimmed_upper, "WITH") {
        Some(AutoFormatClauseKind::With)
    } else if sql_text::starts_with_keyword_token(trimmed_upper, "SELECT") {
        Some(AutoFormatClauseKind::Select)
    } else if sql_text::starts_with_keyword_token(trimmed_upper, "INSERT") {
        Some(AutoFormatClauseKind::Insert)
    } else if sql_text::starts_with_keyword_token(trimmed_upper, "UPDATE") {
        Some(AutoFormatClauseKind::Update)
    } else if sql_text::starts_with_keyword_token(trimmed_upper, "DELETE") {
        Some(AutoFormatClauseKind::Delete)
    } else if sql_text::starts_with_keyword_token(trimmed_upper, "MERGE") {
        Some(AutoFormatClauseKind::Merge)
    } else if sql_text::starts_with_keyword_token(trimmed_upper, "CALL") {
        Some(AutoFormatClauseKind::Call)
    } else if sql_text::starts_with_keyword_token(trimmed_upper, "VALUES") {
        Some(AutoFormatClauseKind::Values)
    } else if sql_text::starts_with_keyword_token(trimmed_upper, "TABLE") {
        Some(AutoFormatClauseKind::Table)
    } else if sql_text::starts_with_keyword_token(trimmed_upper, "FROM") {
        Some(AutoFormatClauseKind::From)
    } else if sql_text::starts_with_keyword_token(trimmed_upper, "WHERE") {
        Some(AutoFormatClauseKind::Where)
    } else if sql_text::starts_with_keyword_token(trimmed_upper, "GROUP") {
        Some(AutoFormatClauseKind::Group)
    } else if sql_text::starts_with_keyword_token(trimmed_upper, "ORDER") {
        Some(AutoFormatClauseKind::Order)
    } else if sql_text::starts_with_keyword_token(trimmed_upper, "WINDOW") {
        Some(AutoFormatClauseKind::Window)
    } else if sql_text::starts_with_keyword_token(trimmed_upper, "MODEL") {
        Some(AutoFormatClauseKind::Model)
    } else if sql_text::line_starts_with_identifier_sequence_before_inline_comment(
        trimmed_upper,
        &["MATCH_RECOGNIZE"],
    ) {
        Some(AutoFormatClauseKind::MatchRecognize)
    } else if sql_text::starts_with_keyword_token(trimmed_upper, "INTO") {
        Some(AutoFormatClauseKind::Into)
    } else {
        None
    }
}

fn clause_frame_kind(clause_kind: AutoFormatClauseKind) -> Option<FrameKind> {
    match clause_kind {
        AutoFormatClauseKind::With => Some(FrameKind::WithBody),
        AutoFormatClauseKind::Select => Some(FrameKind::SelectBody),
        AutoFormatClauseKind::From => Some(FrameKind::FromBody),
        AutoFormatClauseKind::Where => Some(FrameKind::WhereBody),
        AutoFormatClauseKind::Group => Some(FrameKind::GroupByBody),
        AutoFormatClauseKind::Order => Some(FrameKind::OrderByBody),
        AutoFormatClauseKind::Window => Some(FrameKind::WindowOwner),
        AutoFormatClauseKind::Model => Some(FrameKind::ModelOwner),
        AutoFormatClauseKind::MatchRecognize => Some(FrameKind::MatchRecognizeOwner),
        AutoFormatClauseKind::Values => Some(FrameKind::ValuesBody),
        AutoFormatClauseKind::Into => Some(FrameKind::IntoBody),
        AutoFormatClauseKind::Merge => Some(FrameKind::MergeBranch),
        _ => None,
    }
}

fn pop_to_parser_depth(stack: &mut Vec<Frame>, parser_depth: usize) {
    // Remove every frame anchored deeper than the current parser depth, even
    // when a Paren frame currently sits on top of the stack. Depth
    // normalization must be frame-anchor based, not "top frame kind" based.
    while let Some(pos) = stack
        .iter()
        .rposition(|frame| frame.parser_anchor_depth > parser_depth)
    {
        stack.remove(pos);
    }
}

fn pop_latest_paren_frame(stack: &mut Vec<Frame>) {
    if let Some(pos) = stack
        .iter()
        .rposition(|frame| frame.kind == FrameKind::Paren)
    {
        stack.remove(pos);
    }
}

fn current_query_base_depth(stack: &[Frame]) -> Option<usize> {
    stack
        .iter()
        .rev()
        .find(|frame| frame.kind == FrameKind::QueryBase)
        .and_then(|frame| frame.query_base_depth)
}

fn current_owner_depth(stack: &[Frame], fallback: usize) -> usize {
    stack
        .last()
        .map(|frame| frame.owner_depth)
        .unwrap_or(fallback)
}

fn current_body_depth(stack: &[Frame], fallback: usize) -> usize {
    stack
        .last()
        .map(|frame| frame.body_depth)
        .unwrap_or(fallback)
}

fn current_close_align_depth(stack: &[Frame], fallback: usize) -> usize {
    stack
        .last()
        .map(|frame| frame.close_align_depth)
        .unwrap_or(fallback)
}

fn push_frame(
    stack: &mut Vec<Frame>,
    kind: FrameKind,
    parser_depth: usize,
    paren_depth: usize,
    line_idx: usize,
    query_base_depth: Option<usize>,
) {
    let owner_depth = parser_depth;
    let body_depth = parser_depth.saturating_add(1);
    stack.push(Frame {
        kind,
        owner_depth,
        body_depth,
        query_base_depth,
        close_align_depth: owner_depth,
        parser_anchor_depth: parser_depth,
        paren_anchor_depth: paren_depth,
        line_idx: Some(line_idx),
        header_continuation_depth: None,
        pending: false,
        flags: 0,
    });
}

fn push_semantic_frames(
    stack: &mut Vec<Frame>,
    trimmed_upper: &str,
    parser_depth: usize,
    paren_depth: usize,
    line_idx: usize,
) {
    if QueryExecutor::auto_format_is_join_clause(trimmed_upper) {
        push_frame(
            stack,
            FrameKind::JoinHeader,
            parser_depth,
            paren_depth,
            line_idx,
            current_query_base_depth(stack),
        );
    } else if sql_text::is_format_join_condition_clause(trimmed_upper) {
        push_frame(
            stack,
            FrameKind::JoinCondition,
            parser_depth,
            paren_depth,
            line_idx,
            current_query_base_depth(stack),
        );
    } else if sql_text::starts_with_keyword_token(trimmed_upper, "CASE") {
        push_frame(
            stack,
            FrameKind::CaseOwner,
            parser_depth,
            paren_depth,
            line_idx,
            current_query_base_depth(stack),
        );
    } else if sql_text::starts_with_keyword_token(trimmed_upper, "WHEN")
        || sql_text::starts_with_keyword_token(trimmed_upper, "ELSE")
    {
        push_frame(
            stack,
            FrameKind::CaseBranch,
            parser_depth,
            paren_depth,
            line_idx,
            current_query_base_depth(stack),
        );
    }

    if sql_text::starts_with_keyword_token(trimmed_upper, "IF")
        || sql_text::starts_with_keyword_token(trimmed_upper, "WHILE")
        || sql_text::starts_with_keyword_token(trimmed_upper, "FOR")
    {
        push_frame(
            stack,
            FrameKind::ControlCondition,
            parser_depth,
            paren_depth,
            line_idx,
            current_query_base_depth(stack),
        );
    }

    if sql_text::starts_with_keyword_token(trimmed_upper, "FORALL") {
        push_frame(
            stack,
            FrameKind::ForallBody,
            parser_depth,
            paren_depth,
            line_idx,
            current_query_base_depth(stack),
        );
    }

    if sql_text::line_starts_with_identifier_sequence_before_inline_comment(
        trimmed_upper,
        &["DECLARE", "CONTINUE", "HANDLER"],
    ) || sql_text::line_starts_with_identifier_sequence_before_inline_comment(
        trimmed_upper,
        &["DECLARE", "EXIT", "HANDLER"],
    ) {
        push_frame(
            stack,
            FrameKind::HandlerBody,
            parser_depth,
            paren_depth,
            line_idx,
            current_query_base_depth(stack),
        );
    }

    if sql_text::line_starts_with_identifier_sequence_before_inline_comment(
        trimmed_upper,
        &["CREATE", "TRIGGER"],
    ) {
        push_frame(
            stack,
            FrameKind::TriggerBody,
            parser_depth,
            paren_depth,
            line_idx,
            current_query_base_depth(stack),
        );
    }
}

pub(crate) fn scan_once(sql: &str) -> Vec<EngineLineRecord> {
    let lines: Vec<&str> = sql.lines().collect();
    if lines.is_empty() {
        return Vec::new();
    }
    let multiline_string_prefix_lengths =
        sql_text::multiline_string_continuation_prefix_lengths(sql, lines.len());
    let analysis_lines: Vec<&str> = lines
        .iter()
        .enumerate()
        .map(|(idx, line)| {
            multiline_string_prefix_lengths
                .get(idx)
                .copied()
                .flatten()
                .and_then(|prefix_len| line.get(prefix_len..))
                .unwrap_or(line)
        })
        .collect();

    let parser_depths = QueryExecutor::line_block_depths(sql);
    let mut records = Vec::with_capacity(lines.len());
    let mut stack: Vec<Frame> = Vec::new();
    let mut prev_parser_depth = 0usize;
    let mut active_paren_depth = 0usize;

    for (line_idx, line) in analysis_lines.iter().enumerate() {
        let parser_depth = parser_depths
            .get(line_idx)
            .copied()
            .or_else(|| parser_depths.last().copied())
            .unwrap_or(0);

        let trimmed = line.trim_start();

        // Step 1: Consume leading close events FIRST before any structural frame
        // manipulation (formatting.md 1.4 / 4.1: leading close는 항상 먼저 소비한다).
        // Paren profile must be computed before parser_depth frame changes so that
        // close events are applied in the same token order they appear on the line.
        let paren_profile = if !trimmed.is_empty() {
            let profile = sql_text::significant_paren_profile(trimmed);
            for _ in 0..profile.leading_close_count {
                active_paren_depth = active_paren_depth.saturating_sub(1);
                pop_latest_paren_frame(&mut stack);
            }
            profile
        } else {
            sql_text::SignificantParenProfile::default()
        };

        // Step 2: Apply parser_depth changes after leading close (4.1).
        // Pop non-Paren frames whose structural scope has ended.
        pop_to_parser_depth(&mut stack, parser_depth);
        // Push one Block frame per depth level increase (formatting.md 1.2:
        // 모든 open event는 정확히 +1이다 — a jump of N levels must produce N
        // separate frame push events, not a single multi-step push).
        for intermediate_depth in (prev_parser_depth.saturating_add(1))..=parser_depth {
            let query_base_depth = current_query_base_depth(&stack);
            push_frame(
                &mut stack,
                FrameKind::Block,
                intermediate_depth,
                active_paren_depth,
                line_idx,
                query_base_depth,
            );
        }
        prev_parser_depth = parser_depth;

        if trimmed.is_empty() {
            records.push(EngineLineRecord {
                line_idx,
                parser_depth,
                stack_depth: stack.len(),
                owner_depth: current_owner_depth(&stack, parser_depth),
                body_depth: current_body_depth(&stack, parser_depth),
                query_base_depth: current_query_base_depth(&stack),
                close_align_depth: current_close_align_depth(&stack, parser_depth),
            });
            continue;
        }

        // Step 3: Classify the surviving structural tail after leading close.
        let structural = sql_text::auto_format_structural_tail(trimmed);
        let trimmed_upper = structural.to_ascii_uppercase();

        if let Some(clause_kind) = detect_clause_kind(&trimmed_upper) {
            if clause_kind.is_query_head() {
                push_frame(
                    &mut stack,
                    FrameKind::QueryBase,
                    parser_depth,
                    active_paren_depth,
                    line_idx,
                    Some(parser_depth),
                );
            }
            if let Some(frame_kind) = clause_frame_kind(clause_kind) {
                let query_base_depth = current_query_base_depth(&stack);
                push_frame(
                    &mut stack,
                    frame_kind,
                    parser_depth,
                    active_paren_depth,
                    line_idx,
                    query_base_depth,
                );
            }
        }

        push_semantic_frames(
            &mut stack,
            &trimmed_upper,
            parser_depth,
            active_paren_depth,
            line_idx,
        );

        records.push(EngineLineRecord {
            line_idx,
            parser_depth,
            stack_depth: stack.len(),
            owner_depth: current_owner_depth(&stack, parser_depth),
            body_depth: current_body_depth(&stack, parser_depth),
            query_base_depth: current_query_base_depth(&stack),
            close_align_depth: current_close_align_depth(&stack, parser_depth),
        });

        for event in paren_profile
            .events
            .iter()
            .skip(paren_profile.leading_close_count)
        {
            match event {
                sql_text::SignificantParenEvent::Open => {
                    active_paren_depth = active_paren_depth.saturating_add(1);
                    let query_base_depth = current_query_base_depth(&stack);
                    push_frame(
                        &mut stack,
                        FrameKind::Paren,
                        parser_depth,
                        active_paren_depth,
                        line_idx,
                        query_base_depth,
                    );
                }
                sql_text::SignificantParenEvent::Close => {
                    active_paren_depth = active_paren_depth.saturating_sub(1);
                    pop_latest_paren_frame(&mut stack);
                }
            }
        }

        if sql_text::line_ends_with_semicolon_before_inline_comment(trimmed) {
            stack.clear();
            active_paren_depth = 0;
            prev_parser_depth = 0;
        }
    }

    records
}

#[cfg(test)]
mod tests {
    use super::{pop_to_parser_depth, scan_once, Frame, FrameKind};

    fn test_frame(kind: FrameKind, parser_anchor_depth: usize) -> Frame {
        Frame {
            kind,
            owner_depth: parser_anchor_depth,
            body_depth: parser_anchor_depth.saturating_add(1),
            query_base_depth: None,
            close_align_depth: parser_anchor_depth,
            parser_anchor_depth,
            paren_anchor_depth: 0,
            line_idx: None,
            header_continuation_depth: None,
            pending: false,
            flags: 0,
        }
    }

    #[test]
    fn scan_once_tracks_stack_depth_and_line_count() {
        let sql = "SELECT\n    col\nFROM t\nWHERE EXISTS (\n    SELECT 1\n    FROM dual\n);";
        let records = scan_once(sql);

        assert_eq!(records.len(), sql.lines().count());
        assert!(records.iter().any(|record| record.stack_depth > 0));
    }

    #[test]
    fn scan_once_ignores_leading_close_inside_multiline_backtick_payload() {
        let sql = "SELECT\n    JSON_OBJECT (\n        `\n)field`,\n        1\n    ) AS payload,\n    2 AS next_payload\nFROM dual;";
        let lines: Vec<&str> = sql.lines().collect();
        let content_idx = lines
            .iter()
            .position(|line| line.trim_start() == ")field`,")
            .unwrap_or(0);
        let sibling_idx = lines
            .iter()
            .position(|line| line.trim_start() == "1")
            .unwrap_or(0);

        let records = scan_once(sql);

        assert_eq!(
            records[content_idx].stack_depth, records[sibling_idx].stack_depth,
            "leading `)` inside multiline backtick payload must not pop structural frame stack depth"
        );
    }

    // Bug regression: leading close must be consumed BEFORE any structural
    // frame manipulation (formatting.md 1.4 / 4.1).  The Paren frame opened
    // by a trailing `(` on one line must already be gone from the stack by the
    // time the leading `)` line is recorded, regardless of what other structural
    // events (clause pushes, block-depth changes) happen on that same line.
    #[test]
    fn scan_once_processes_leading_close_before_block_frame_push() {
        // The `SELECT (` line opens a Paren frame after its record is captured.
        // The `) col` line has a leading `)` that must consume that Paren before
        // the record is taken — the `) col` record should therefore have the
        // SAME stack depth as the `SELECT (` record (both measured after clause
        // frames but before the dangling open-paren is counted).
        let sql = "BEGIN\n    SELECT (\n    ) col\n    FROM dual;\nEND;";
        let lines: Vec<&str> = sql.lines().collect();
        let select_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("SELECT"))
            .unwrap_or(0);
        let close_line_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with(')'))
            .unwrap_or(0);

        let records = scan_once(sql);

        // `SELECT (` record is captured before the trailing `(` is processed,
        // so its stack_depth reflects {Block, QueryBase, SelectBody} = 3.
        // `) col` must also show stack_depth = 3 because the leading `)` pops
        // the Paren pushed by `SELECT (` before the record is taken.
        assert_eq!(
            records[close_line_idx].stack_depth, records[select_idx].stack_depth,
            "`) col` stack_depth ({}) must equal `SELECT (` stack_depth ({}) — \
             leading `)` must consume the trailing paren before the record is captured",
            records[close_line_idx].stack_depth, records[select_idx].stack_depth,
        );
    }

    // Bug regression: a parser_depth jump of N must push N separate Block
    // frames (formatting.md 1.2: every open event is exactly +1).  A single
    // push for a multi-level jump leaves the stack_depth lower than expected
    // and means later pop_to_parser_depth calls remove the wrong frame.
    #[test]
    fn scan_once_pushes_one_block_frame_per_depth_level() {
        // Simulate two nested BEGIN blocks opened before the first code line.
        // parser_depth for "SELECT 1" would be 2, jumping from 0 in one shot
        // if the nesting happens in header lines that scan_once skips as empty.
        // Use a two-level nested PL/SQL anonymous block to exercise this.
        let sql = "BEGIN\n    BEGIN\n        SELECT 1 FROM dual;\n    END;\nEND;";
        let lines: Vec<&str> = sql.lines().collect();
        let inner_select_idx = lines
            .iter()
            .position(|line| line.trim_start().starts_with("SELECT"))
            .unwrap_or(0);
        let outer_end_idx = lines
            .iter()
            .rposition(|line| line.trim_start().starts_with("END"))
            .unwrap_or(0);

        let records = scan_once(sql);

        // The SELECT inside the double-nested block must have a higher
        // stack_depth than the outer END line.
        assert!(
            records[inner_select_idx].stack_depth > records[outer_end_idx].stack_depth,
            "inner SELECT stack_depth ({}) must be greater than outer END \
             stack_depth ({}) when each depth level pushes its own Block frame",
            records[inner_select_idx].stack_depth,
            records[outer_end_idx].stack_depth,
        );
    }

    #[test]
    fn pop_to_parser_depth_removes_deeper_frames_even_when_paren_is_on_top() {
        let mut stack = vec![
            test_frame(FrameKind::Block, 1),
            test_frame(FrameKind::Block, 2),
            test_frame(FrameKind::Paren, 2),
        ];

        pop_to_parser_depth(&mut stack, 1);

        assert_eq!(
            stack.len(),
            1,
            "parser-depth pop should remove every deeper frame even if the top frame is Paren"
        );
        assert_eq!(
            stack[0].kind,
            FrameKind::Block,
            "the surviving frame should be the parser-depth-1 Block anchor"
        );
        assert!(
            stack.iter().all(|frame| frame.parser_anchor_depth <= 1),
            "no frame anchored deeper than the current parser depth may survive"
        );
    }
}
