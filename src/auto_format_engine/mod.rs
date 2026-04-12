use crate::db::QueryExecutor;

mod stack_scan;
mod types;

// Structural scanner types defined only in this module.
pub(crate) use types::{EngineLineRecord, Frame, FrameKind};

// Shared formatting taxonomy types — canonical definitions live in
// `crate::db::query::script` and are re-exported through `crate::db::query`.
pub(crate) use crate::db::query::{AutoFormatClauseKind, AutoFormatLineContext};

struct AutoFormatEngine<'a> {
    input: &'a str,
    records: Vec<EngineLineRecord>,
}

impl<'a> AutoFormatEngine<'a> {
    fn new(input: &'a str) -> Self {
        Self {
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
}

pub(crate) fn analyze_lines(sql: &str) -> Vec<AutoFormatLineContext> {
    let mut engine = AutoFormatEngine::new(sql);
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
