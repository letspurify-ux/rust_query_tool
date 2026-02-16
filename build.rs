use std::env;
use std::path::Path;
use std::process::Command;

fn has_system_lib_via_pkg_config(name: &str) -> bool {
    Command::new("pkg-config")
        .args(["--exists", name])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn build_empty_stub(out_dir: &Path, lib_name: &str) {
    let src = out_dir.join(format!("{}_stub.c", lib_name));
    std::fs::write(&src, "void space_query_x11_stub(void) {}\n").expect("write stub source");

    let mut build = cc::Build::new();
    build.file(&src);
    build.warnings(false);
    build.compile(lib_name);
}

fn main() {
    if env::var("CARGO_CFG_TARGET_OS").ok().as_deref() != Some("linux") {
        return;
    }

    let out_dir = env::var("OUT_DIR").expect("OUT_DIR");
    let out_path = Path::new(&out_dir);

    let stub_libs = [
        ("xinerama", "Xinerama"),
        ("xcursor", "Xcursor"),
        ("xfixes", "Xfixes"),
        ("xft", "Xft"),
        ("fontconfig", "fontconfig"),
    ];

    let mut any_missing = false;
    for (pkg_name, lib_name) in &stub_libs {
        if !has_system_lib_via_pkg_config(pkg_name) {
            build_empty_stub(out_path, lib_name);
            any_missing = true;
        }
    }

    if any_missing {
        println!("cargo:warning=Missing X11 dev libs detected; linking local stubs for test/build in this environment.");
        println!("cargo:rustc-link-search=native={}", out_path.display());
    }
}
