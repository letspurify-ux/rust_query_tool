use crate::db::connection::DatabaseType;
use crate::db::QueryExecutor;
use crate::ui::sql_editor::{SelectListBreakState, SqlEditorWidget, SqlToken};

mod stack_scan;
mod types;

pub(crate) use types::{
    AutoFormatClauseKind, AutoFormatConditionRole, AutoFormatConditionTerminator,
    AutoFormatLineContext, AutoFormatLineSemantic, AutoFormatQueryRole, EngineLineRecord, Frame,
    FrameKind,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EngineMode {
    AnalyzeLines,
    ReindentExistingLayout,
    FormatStatement,
    FormatScript,
}

struct AutoFormatEngine<'a> {
    mode: EngineMode,
    input: &'a str,
    records: Vec<EngineLineRecord>,
}

impl<'a> AutoFormatEngine<'a> {
    fn new(mode: EngineMode, input: &'a str) -> Self {
        Self {
            mode,
            input,
            records: Vec::new(),
        }
    }

    fn scan_once(&mut self) {
        // The shared streaming scanner always runs left-to-right with a
        // single Vec<Frame> stack. Mode-specific entrypoints can then build
        // compatibility outputs on top of these records.
        self.records = stack_scan::scan_once(self.input);
    }

    fn records(&self) -> &[EngineLineRecord] {
        self.records.as_slice()
    }

    fn mode(&self) -> EngineMode {
        self.mode
    }
}

pub(crate) fn analyze_lines(sql: &str) -> Vec<AutoFormatLineContext> {
    let mut engine = AutoFormatEngine::new(EngineMode::AnalyzeLines, sql);
    engine.scan_once();

    let contexts = QueryExecutor::auto_format_line_contexts_impl(sql);
    if !engine.records().is_empty() && contexts.len() != engine.records().len() {
        // Keep legacy compatibility behavior even when record and context
        // counts diverge for edge script fragments.
        return contexts;
    }

    contexts
}

pub(crate) fn line_auto_format_depths(sql: &str) -> Vec<usize> {
    analyze_lines(sql)
        .into_iter()
        .map(|context| context.auto_depth)
        .collect()
}

pub(crate) fn reindent_existing_layout(formatted: &str, mysql_compatible: bool) -> String {
    let mut engine = AutoFormatEngine::new(EngineMode::ReindentExistingLayout, formatted);
    engine.scan_once();
    let _mode = engine.mode();
    SqlEditorWidget::reindent_existing_layout_impl(formatted, mysql_compatible)
}

pub(crate) fn format_statement(
    statement: &str,
    tokens: &[SqlToken],
    select_list_break_state_on_start: SelectListBreakState,
    mysql_compatible: bool,
) -> String {
    let mut engine = AutoFormatEngine::new(EngineMode::FormatStatement, statement);
    engine.scan_once();
    let provisional = SqlEditorWidget::format_statement_impl(
        statement,
        tokens,
        select_list_break_state_on_start,
        mysql_compatible,
    );
    reindent_existing_layout(&provisional, mysql_compatible)
}

pub(crate) fn format_script(
    sql: &str,
    append_missing_terminator: bool,
    preferred_db_type: Option<DatabaseType>,
) -> String {
    let mut engine = AutoFormatEngine::new(EngineMode::FormatScript, sql);
    engine.scan_once();
    let _mode = engine.mode();
    SqlEditorWidget::format_sql_basic_with_terminator_policy_impl(
        sql,
        append_missing_terminator,
        preferred_db_type,
    )
}
