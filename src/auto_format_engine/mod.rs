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

    // Derive AutoFormatLineContext from the unified single stack records.
    // All depth values are based solely on the current stack height at the
    // time each line is recorded, removing any depth management that lives
    // outside the stack structure.
    engine
        .records()
        .iter()
        .map(|record| {
            let mut ctx = AutoFormatLineContext::default();
            ctx.parser_depth = record.parser_depth;
            ctx.auto_depth = record.stack_depth;
            ctx.render_depth = record.stack_depth;
            ctx.carry_depth = record.stack_depth;
            ctx.query_base_depth = record.query_base_depth;
            ctx
        })
        .collect()
}

pub(crate) fn line_auto_format_depths(sql: &str) -> Vec<usize> {
    analyze_lines(sql)
        .into_iter()
        .map(|context| context.auto_depth)
        .collect()
}
