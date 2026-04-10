#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum AutoFormatQueryRole {
    #[default]
    None,
    Base,
    Continuation,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum AutoFormatConditionRole {
    #[default]
    None,
    Header,
    Continuation,
    Closer,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AutoFormatClauseKind {
    With,
    Select,
    Insert,
    Update,
    Delete,
    Merge,
    Call,
    Values,
    Table,
    From,
    Where,
    Group,
    Having,
    Order,
    Connect,
    Start,
    Union,
    Intersect,
    Minus,
    Except,
    Set,
    Into,
    Offset,
    Fetch,
    Limit,
    Returning,
    Model,
    Window,
    MatchRecognize,
    Qualify,
    Pivot,
    Unpivot,
    Search,
    Cycle,
}

impl AutoFormatClauseKind {
    pub(crate) fn is_query_head(self) -> bool {
        matches!(
            self,
            Self::With
                | Self::Select
                | Self::Insert
                | Self::Update
                | Self::Delete
                | Self::Merge
                | Self::Call
                | Self::Values
                | Self::Table
        )
    }

    pub(crate) fn is_set_operator(self) -> bool {
        matches!(
            self,
            Self::Union | Self::Intersect | Self::Minus | Self::Except
        )
    }

    pub(crate) fn ends_into_continuation(self) -> bool {
        matches!(
            self,
            Self::From
                | Self::Where
                | Self::Group
                | Self::Having
                | Self::Order
                | Self::Connect
                | Self::Union
                | Self::Intersect
                | Self::Minus
                | Self::Except
        )
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum AutoFormatLineSemantic {
    #[default]
    None,
    Clause(AutoFormatClauseKind),
    JoinClause,
    JoinConditionClause,
    ConditionContinuation,
    MySqlDeclareHandlerHeader,
    MySqlDeclareHandlerBody,
    MySqlDeclareHandlerBlockEnd,
}

impl AutoFormatLineSemantic {
    pub(crate) fn from_analysis(
        clause_kind: Option<AutoFormatClauseKind>,
        query_role: AutoFormatQueryRole,
        is_join_clause: bool,
        is_join_condition_clause: bool,
        is_query_condition_continuation_clause: bool,
    ) -> Self {
        if query_role == AutoFormatQueryRole::Continuation && is_join_condition_clause {
            Self::JoinConditionClause
        } else if query_role == AutoFormatQueryRole::Continuation
            && is_query_condition_continuation_clause
        {
            Self::ConditionContinuation
        } else if query_role == AutoFormatQueryRole::Base && is_join_clause {
            Self::JoinClause
        } else if let Some(kind) = clause_kind {
            Self::Clause(kind)
        } else {
            Self::None
        }
    }

    pub(crate) fn is_clause(self) -> bool {
        matches!(self, Self::Clause(_))
    }

    pub(crate) fn is_join_clause(self) -> bool {
        matches!(self, Self::JoinClause)
    }

    pub(crate) fn is_join_condition_clause(self) -> bool {
        matches!(self, Self::JoinConditionClause)
    }

    pub(crate) fn is_condition_continuation(self) -> bool {
        matches!(self, Self::ConditionContinuation)
    }

    pub(crate) fn is_mysql_declare_handler_header(self) -> bool {
        matches!(self, Self::MySqlDeclareHandlerHeader)
    }

    pub(crate) fn is_mysql_declare_handler_body(self) -> bool {
        matches!(self, Self::MySqlDeclareHandlerBody)
    }

    pub(crate) fn is_mysql_declare_handler_block_end(self) -> bool {
        matches!(self, Self::MySqlDeclareHandlerBlockEnd)
    }

    pub(crate) fn is_clause_boundary(self) -> bool {
        self.is_clause() || self.is_join_clause()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AutoFormatConditionTerminator {
    Then,
    Loop,
}

impl AutoFormatConditionTerminator {
    pub(crate) fn matches_keyword(self, upper: &str) -> bool {
        matches!(
            (self, upper),
            (Self::Then, "THEN") | (Self::Loop, "LOOP") | (Self::Loop, "DO")
        )
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct AutoFormatLineContext {
    pub(crate) parser_depth: usize,
    pub(crate) auto_depth: usize,
    pub(crate) render_depth: usize,
    pub(crate) carry_depth: usize,
    pub(crate) query_role: AutoFormatQueryRole,
    pub(crate) line_semantic: AutoFormatLineSemantic,
    pub(crate) query_base_depth: Option<usize>,
    pub(crate) starts_query_frame: bool,
    pub(crate) next_query_head_depth: Option<usize>,
    pub(crate) condition_header_line: Option<usize>,
    pub(crate) condition_header_depth: Option<usize>,
    pub(crate) condition_header_terminator: Option<AutoFormatConditionTerminator>,
    pub(crate) condition_role: AutoFormatConditionRole,
}

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
