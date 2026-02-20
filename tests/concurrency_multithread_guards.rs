use std::fs;
use std::path::{Path, PathBuf};

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

    files
}

#[test]
fn thread_spawn_files_do_not_use_rc_or_refcell() {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();

    for file in collect_rust_files(&src_root) {
        let content = fs::read_to_string(&file)
            .unwrap_or_else(|err| panic!("failed to read source file {}: {err}", file.display()));

        if !content.contains("thread::spawn") {
            continue;
        }

        if content.contains("Rc<")
            || content.contains("std::rc::Rc")
            || content.contains("RefCell")
            || content.contains("std::cell::RefCell")
        {
            offenders.push(file);
        }
    }

    assert!(
        offenders.is_empty(),
        "thread::spawn files must not use Rc/RefCell: {:?}",
        offenders
    );
}

#[test]
fn shared_connection_is_arc_mutex() {
    let file = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/connection.rs");
    let content = fs::read_to_string(&file)
        .unwrap_or_else(|err| panic!("failed to read source file {}: {err}", file.display()));

    assert!(
        content.contains("pub type SharedConnection = Arc<Mutex<DatabaseConnection>>;"),
        "SharedConnection type alias must remain Arc<Mutex<DatabaseConnection>>"
    );
}
