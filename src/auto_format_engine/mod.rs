use crate::db::connection::DatabaseType;
use crate::db::QueryExecutor;
use crate::ui::sql_editor::SqlEditorWidget;

mod stack_scan;
mod types;

// Structural scanner types defined only in this module.
pub(crate) use types::{EngineLineRecord, Frame, FrameKind};

// Shared formatting taxonomy types — canonical definitions live in
// `crate::db::query::script` and are re-exported through `crate::db::query`.
pub(crate) use crate::db::query::{
    AutoFormatClauseKind, AutoFormatConditionRole, AutoFormatLineContext, AutoFormatLineSemantic,
    AutoFormatQueryRole,
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

    let contexts = QueryExecutor::auto_format_line_contexts(sql);
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

/// Re-applies canonical indentation to an already-formatted SQL string.
///
/// Uses `mysql_compatible` to select the appropriate formatter path: when
/// true, the MySQL/MariaDB formatting rules are applied; otherwise the
/// default Oracle/ANSI path is used.
pub(crate) fn reindent_existing_layout(formatted: &str, mysql_compatible: bool) -> String {
    let mut engine = AutoFormatEngine::new(EngineMode::ReindentExistingLayout, formatted);
    engine.scan_once();
    let _mode = engine.mode();
    if mysql_compatible {
        SqlEditorWidget::format_sql_basic_for_db_type(formatted, DatabaseType::MySQL)
    } else {
        SqlEditorWidget::format_sql_basic(formatted)
    }
}

/// Formats a single SQL statement and re-applies canonical indentation.
pub(crate) fn format_statement(statement: &str, mysql_compatible: bool) -> String {
    let mut engine = AutoFormatEngine::new(EngineMode::FormatStatement, statement);
    engine.scan_once();
    let provisional = if mysql_compatible {
        SqlEditorWidget::format_sql_basic_for_db_type(statement, DatabaseType::MySQL)
    } else {
        SqlEditorWidget::format_sql_basic(statement)
    };
    reindent_existing_layout(&provisional, mysql_compatible)
}

pub(crate) fn format_script(
    sql: &str,
    _append_missing_terminator: bool,
    preferred_db_type: Option<DatabaseType>,
) -> String {
    let mut engine = AutoFormatEngine::new(EngineMode::FormatScript, sql);
    engine.scan_once();
    let _mode = engine.mode();
    if let Some(db_type) = preferred_db_type {
        SqlEditorWidget::format_sql_basic_for_db_type(sql, db_type)
    } else {
        SqlEditorWidget::format_sql_basic(sql)
    }
}
