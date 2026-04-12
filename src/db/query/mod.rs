mod executor;
pub mod mysql_executor;
mod script;
mod types;

pub(crate) use crate::sql_parser_engine::SplitState;
pub use executor::*;
pub use types::*;

// Re-export auto-format taxonomy types so modules outside `db::query` (such
// as `auto_format_engine`) can reference them without naming the private
// `script` submodule directly.
pub(crate) use script::{
    AutoFormatClauseKind, AutoFormatLineContext,
};

#[cfg(test)]
mod query_tests;
