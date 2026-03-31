use quote::ToTokens;
use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use syn::spanned::Spanned;
use syn::visit::{self, Visit};
use syn::{Attribute, ExprBinary, ExprMethodCall, ImplItemFn, ItemFn, ItemMod, TraitItemFn};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct ArithmeticSite {
    path: String,
    line: usize,
    kind: &'static str,
    expr: String,
}

fn collect_rust_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let entries = fs::read_dir(&dir)
            .unwrap_or_else(|err| panic!("failed to read directory {}: {err}", dir.display()));

        for entry in entries {
            let entry = entry.unwrap_or_else(|err| {
                panic!("failed to read directory entry in {}: {err}", dir.display())
            });
            let path = entry.path();

            if path.is_dir() {
                stack.push(path);
                continue;
            }

            if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
                files.push(path);
            }
        }
    }

    files.sort();
    files
}

fn is_test_source_file(path: &Path) -> bool {
    let file_name = path.file_name().and_then(|name| name.to_str());
    if matches!(file_name, Some("tests.rs")) {
        return true;
    }

    if file_name.is_some_and(|name| name.ends_with("_tests.rs")) {
        return true;
    }

    path.components()
        .any(|component| component.as_os_str() == OsStr::new("tests"))
}

fn attribute_marks_test(attr: &Attribute) -> bool {
    if attr.path().is_ident("test") {
        return true;
    }

    if attr.path().is_ident("cfg") {
        let tokens = attr.meta.to_token_stream().to_string();
        return tokens.contains("test");
    }

    false
}

fn attrs_mark_test(attrs: &[Attribute]) -> bool {
    attrs.iter().any(attribute_marks_test)
}

struct ArithmeticVisitor<'a> {
    file: &'a str,
    sites: Vec<ArithmeticSite>,
    skip_depth: usize,
}

impl<'a> ArithmeticVisitor<'a> {
    fn new(file: &'a str) -> Self {
        Self {
            file,
            sites: Vec::new(),
            skip_depth: 0,
        }
    }

    fn with_skip<T>(&mut self, should_skip: bool, visit_fn: impl FnOnce(&mut Self) -> T) -> T {
        if should_skip {
            self.skip_depth += 1;
        }
        let result = visit_fn(self);
        if should_skip {
            self.skip_depth = self.skip_depth.saturating_sub(1);
        }
        result
    }

    fn record(&mut self, span: proc_macro2::Span, kind: &'static str, expr: String) {
        if self.skip_depth > 0 {
            return;
        }

        self.sites.push(ArithmeticSite {
            path: self.file.to_string(),
            line: span.start().line,
            kind,
            expr,
        });
    }
}

impl<'ast> Visit<'ast> for ArithmeticVisitor<'_> {
    fn visit_item_mod(&mut self, node: &'ast ItemMod) {
        self.with_skip(attrs_mark_test(&node.attrs), |this| {
            visit::visit_item_mod(this, node);
        });
    }

    fn visit_item_fn(&mut self, node: &'ast ItemFn) {
        self.with_skip(attrs_mark_test(&node.attrs), |this| {
            visit::visit_item_fn(this, node);
        });
    }

    fn visit_impl_item_fn(&mut self, node: &'ast ImplItemFn) {
        self.with_skip(attrs_mark_test(&node.attrs), |this| {
            visit::visit_impl_item_fn(this, node);
        });
    }

    fn visit_trait_item_fn(&mut self, node: &'ast TraitItemFn) {
        self.with_skip(attrs_mark_test(&node.attrs), |this| {
            visit::visit_trait_item_fn(this, node);
        });
    }

    fn visit_expr_binary(&mut self, node: &'ast ExprBinary) {
        if self.skip_depth == 0 {
            let kind = match node.op {
                syn::BinOp::Div(_) => Some("div"),
                syn::BinOp::Rem(_) => Some("rem"),
                _ => None,
            };

            if let Some(kind) = kind {
                self.record(node.span(), kind, node.to_token_stream().to_string());
            }
        }

        visit::visit_expr_binary(self, node);
    }

    fn visit_expr_method_call(&mut self, node: &'ast ExprMethodCall) {
        if self.skip_depth == 0 {
            let kind = match node.method.to_string().as_str() {
                "checked_div" => Some("checked_div"),
                "checked_rem" => Some("checked_rem"),
                "div_ceil" => Some("div_ceil"),
                "div_euclid" => Some("div_euclid"),
                "rem_euclid" => Some("rem_euclid"),
                "saturating_div" => Some("saturating_div"),
                _ => None,
            };

            if let Some(kind) = kind {
                self.record(node.span(), kind, node.to_token_stream().to_string());
            }
        }

        visit::visit_expr_method_call(self, node);
    }
}

fn collect_arithmetic_sites() -> BTreeSet<ArithmeticSite> {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut sites = BTreeSet::new();

    for file in collect_rust_files(&src_root) {
        if is_test_source_file(&file) {
            continue;
        }

        let raw = fs::read_to_string(&file)
            .unwrap_or_else(|err| panic!("failed to read source file {}: {err}", file.display()));
        let parsed = syn::parse_file(&raw)
            .unwrap_or_else(|err| panic!("failed to parse source file {}: {err}", file.display()));

        let relative = file
            .strip_prefix(Path::new(env!("CARGO_MANIFEST_DIR")))
            .unwrap_or_else(|err| panic!("failed to relativize {}: {err}", file.display()))
            .display()
            .to_string();

        let mut visitor = ArithmeticVisitor::new(&relative);
        visitor.visit_file(&parsed);
        sites.extend(visitor.sites);
    }

    sites
}

macro_rules! site {
    ($path:literal, $line:literal, $kind:literal, $expr:literal) => {
        ArithmeticSite {
            path: $path.to_string(),
            line: $line,
            kind: $kind,
            expr: $expr.to_string(),
        }
    };
}

fn expected_arithmetic_sites() -> BTreeSet<ArithmeticSite> {
    BTreeSet::from([
        site!("src/icon_fill.rs", 34, "div", "2.2 / size_f"),
        site!("src/icon_fill.rs", 39, "div", "(x as f32 + 0.5) / size_f"),
        site!("src/icon_fill.rs", 40, "div", "(y as f32 + 0.5) / size_f"),
        site!(
            "src/icon_fill.rs",
            64,
            "div",
            "(px * px + py * py) . sqrt () / 1.2"
        ),
        site!("src/icon_fill.rs", 97, "div", "(py + 0.50) / 1.0"),
        site!("src/icon_fill.rs", 147, "div", "sa / 255.0"),
        site!("src/icon_fill.rs", 148, "div", "sa / 255.0"),
        site!("src/icon_fill.rs", 149, "div", "sa / 255.0"),
        site!("src/icon_fill.rs", 150, "div", "sa / 255.0"),
        site!("src/icon_fill.rs", 155, "div", "(aa - distance) / aa"),
        site!(
            "src/icon_fill.rs",
            167,
            "div",
            "(apx * abx + apy * aby) / ab_len_sq"
        ),
        site!("src/splash/renderer.rs", 684, "div", "sum / 4"),
        site!(
            "src/splash/renderer.rs",
            701,
            "div",
            "sw . saturating_mul (9) / 10"
        ),
        site!(
            "src/splash/renderer.rs",
            702,
            "div",
            "sh . saturating_mul (13) / 32"
        ),
        site!(
            "src/splash/renderer.rs",
            703,
            "div",
            "title_usable_width / title_columns . max (1)"
        ),
        site!(
            "src/splash/renderer.rs",
            704,
            "div",
            "title_usable_height / GLYPH_ROWS"
        ),
        site!(
            "src/splash/renderer.rs",
            708,
            "div",
            "sw . saturating_sub (title_width) / 2"
        ),
        site!(
            "src/splash/renderer.rs",
            709,
            "div",
            "sh . saturating_mul (7) / 32"
        ),
        site!(
            "src/splash/renderer.rs",
            721,
            "div",
            "sw . saturating_mul (7) / 10"
        ),
        site!(
            "src/splash/renderer.rs",
            722,
            "div",
            "sh . saturating_mul (5) / 32"
        ),
        site!(
            "src/splash/renderer.rs",
            723,
            "div",
            "subtitle_usable_width / subtitle_columns . max (1)"
        ),
        site!(
            "src/splash/renderer.rs",
            724,
            "div",
            "subtitle_usable_height / GLYPH_ROWS"
        ),
        site!(
            "src/splash/renderer.rs",
            729,
            "div",
            "sh . saturating_mul (3) / 32"
        ),
        site!(
            "src/splash/renderer.rs",
            749,
            "div",
            "sh . saturating_mul (2) / 32"
        ),
        site!(
            "src/splash/renderer.rs",
            769,
            "div",
            "(v as u32) . saturating_mul (153) / 255"
        ),
        site!("src/splash/renderer.rs", 896, "div", "elapsed / 0.8"),
        site!("src/splash/renderer.rs", 901, "div", "fo_elapsed / 0.5"),
        site!(
            "src/splash/renderer.rs",
            904,
            "div",
            "(10.0 - elapsed) / 0.5"
        ),
        site!(
            "src/ui/main_window.rs",
            183,
            "rem",
            "(current_frame . saturating_add (1)) % frame_count"
        ),
        site!(
            "src/ui/main_window.rs",
            1352,
            "div",
            "desired_query_height as f64 / right_height as f64"
        ),
        site!(
            "src/ui/main_window.rs",
            1377,
            "div",
            "query_height as f64 / right_height as f64"
        ),
        site!(
            "src/ui/main_window.rs",
            3898,
            "div",
            "60.0 / CHANNEL_POLL_IDLE_INTERVAL_SECONDS"
        ),
        site!("src/ui/menu.rs", 46, "div", "(width - BUTTON_WIDTH) / 2"),
        site!("src/ui/mod.rs", 55, "div", "(mw - window . width ()) / 2"),
        site!("src/ui/mod.rs", 56, "div", "(mh - window . height ()) / 2"),
        site!(
            "src/ui/mod.rs",
            61,
            "div",
            "((sw as i32) - window . width ()) / 2"
        ),
        site!(
            "src/ui/mod.rs",
            62,
            "div",
            "((sh as i32) - window . height ()) / 2"
        ),
        site!(
            "src/ui/result_table.rs",
            1175,
            "div",
            "((font_size as i32 * 62) + 99) / 100"
        ),
        site!(
            "src/ui/result_table.rs",
            1185,
            "div",
            "((font_size as i32 * 62) + 99) / 100"
        ),
        site!(
            "src/ui/result_table.rs",
            2950,
            "div",
            "(visible_height . saturating_add (row_h) . saturating_sub (1)) / row_h"
        ),
        site!("src/ui/result_table.rs", 2972, "div", "hidden_px / item_extent"),
        site!("src/ui/result_table.rs", 2988, "div", "delta / row_h"),
        site!("src/ui/result_table.rs", 3028, "div", "delta / row_h"),
        site!("src/ui/result_table.rs", 3061, "div", "delta / start_w"),
        site!("src/ui/result_table.rs", 3452, "div", "gap_px / cw"),
        site!("src/ui/result_table.rs", 3531, "div", "hidden_px / cw"),
        site!(
            "src/ui/sql_editor/dba_tools.rs",
            858,
            "div_ceil",
            "SQL_MONITOR_AUTO_REFRESH_INTERVAL_MS . div_ceil (SQL_MONITOR_AUTO_REFRESH_POLL_MS)"
        ),
        site!("src/ui/sql_editor/execution.rs", 4870, "rem", "col % tab_stop"),
        site!("src/ui/sql_editor/execution.rs", 5919, "div", "size / 80"),
        site!(
            "src/ui/sql_editor/formatter.rs",
            1524,
            "checked_div",
            "value . checked_div (Self :: normalized_indent_tab_width ())"
        ),
        site!(
            "src/ui/sql_editor/formatter.rs",
            1530,
            "checked_rem",
            "value . checked_rem (Self :: normalized_indent_tab_width ())"
        ),
        site!(
            "src/ui/sql_editor/formatter.rs",
            1615,
            "div",
            "columns / indent_tab_width"
        ),
        site!(
            "src/ui/sql_editor/formatter.rs",
            1874,
            "checked_div",
            "(source_pos as u128) . saturating_mul (formatted . len () as u128) . checked_div (source . len () as u128)"
        ),
        site!(
            "src/ui/sql_editor/intellisense/mod.rs",
            101,
            "checked_rem",
            "next . checked_rem (worker_count)"
        ),
        site!(
            "src/ui/sql_editor/intellisense/popup.rs",
            44,
            "div",
            "(600 - BUTTON_WIDTH) / 2"
        ),
        site!(
            "src/ui/sql_editor/intellisense/popup.rs",
            96,
            "div",
            "(760 - BUTTON_WIDTH) / 2"
        ),
    ])
}

#[test]
fn reviewed_division_and_modulo_sites_are_tracked() {
    let actual = collect_arithmetic_sites();
    let expected = expected_arithmetic_sites();
    assert_eq!(
        actual, expected,
        "reviewed divide/modulo sites changed; inspect new arithmetic operations for zero-divisor safety and update this allowlist intentionally"
    );
}
