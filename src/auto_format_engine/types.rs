/// Stack frame kind used by the structural scanner.
///
/// Each token-order structural event pushes a typed frame onto the
/// `Vec<Frame>` stack.  The kind determines how the frame is consumed
/// by `pop_to_parser_depth` and `pop_latest_paren_frame`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[allow(dead_code)]
pub(crate) enum FrameKind {
    #[default]
    Paren,
    Block,
    SelectBody,
    FromBody,
    JoinHeader,
    JoinCondition,
    WhereBody,
    GroupByBody,
    OrderByBody,
    WindowOwner,
    WindowBody,
    ModelOwner,
    MatchRecognizeOwner,
    ControlCondition,
    WithBody,
    ValuesBody,
    IntoBody,
    CaseOwner,
    CaseBranch,
    RoutineBody,
    IfBody,
    LoopBody,
    MergeBranch,
    HandlerBody,
    ForallBody,
    QueryBase,
    QueryPending,
    OwnerRelative,
    TriggerBody,
    LineCarry,
    InlineCommentCarry,
    Unknown,
}

/// A single frame on the structural owner stack maintained by `scan_once`.
///
/// `parser_anchor_depth` and `paren_anchor_depth` record the lexical nesting
/// depths at push time so that `pop_to_parser_depth` and
/// `pop_latest_paren_frame` can selectively remove frames whose structural
/// scope has ended.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[allow(dead_code)]
pub(crate) struct Frame {
    pub(crate) kind: FrameKind,
    pub(crate) owner_depth: usize,
    pub(crate) body_depth: usize,
    pub(crate) query_base_depth: Option<usize>,
    pub(crate) close_align_depth: usize,
    pub(crate) parser_anchor_depth: usize,
    pub(crate) paren_anchor_depth: usize,
    pub(crate) line_idx: Option<usize>,
    pub(crate) header_continuation_depth: Option<usize>,
    pub(crate) pending: bool,
    pub(crate) flags: u32,
}

/// Per-line structural record produced by `scan_once`.
///
/// Each record captures the stack state at the START of the line (after
/// leading-close events have been consumed but before trailing open events
/// are applied), giving downstream consumers a stable anchor for
/// depth-based indentation decisions.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct EngineLineRecord {
    pub(crate) line_idx: usize,
    pub(crate) parser_depth: usize,
    pub(crate) stack_depth: usize,
    pub(crate) owner_depth: usize,
    pub(crate) body_depth: usize,
    pub(crate) query_base_depth: Option<usize>,
    pub(crate) close_align_depth: usize,
}
