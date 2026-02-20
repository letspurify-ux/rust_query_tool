use std::fs;
use std::path::{Path, PathBuf};

fn collect_rust_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) => panic!("failed to read directory {}: {err}", dir.display()),
        };

        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => panic!("failed to read directory entry in {}: {err}", dir.display()),
            };
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

fn strip_test_blocks(content: &str) -> String {
    let mut output = String::with_capacity(content.len());
    let mut i = 0;

    while i < content.len() {
        let rest = &content[i..];

        if rest.starts_with("#[cfg(test)]") {
            i += "#[cfg(test)]".len();
            while i < content.len() {
                let c = content.as_bytes()[i] as char;
                if c.is_whitespace() {
                    i += 1;
                    continue;
                }
                break;
            }

            if content[i..].starts_with("mod") {
                if let Some(open_brace_rel) = content[i..].find('{') {
                    let open_brace = i + open_brace_rel;
                    if let Some(after_block) = find_matching_brace_end(content, open_brace) {
                        i = after_block;
                        continue;
                    }
                }
            }

            continue;
        }

        if rest.starts_with("#[test]") {
            i += "#[test]".len();
            while i < content.len() {
                let c = content.as_bytes()[i] as char;
                if c.is_whitespace() {
                    i += 1;
                    continue;
                }
                break;
            }

            if content[i..].starts_with("fn") {
                if let Some(open_brace_rel) = content[i..].find('{') {
                    let open_brace = i + open_brace_rel;
                    if let Some(after_block) = find_matching_brace_end(content, open_brace) {
                        i = after_block;
                        continue;
                    }
                }
            }

            continue;
        }

        if let Some(ch) = rest.chars().next() {
            output.push(ch);
            i += ch.len_utf8();
        } else {
            break;
        }
    }

    output
}

fn find_matching_brace_end(content: &str, open_index: usize) -> Option<usize> {
    if content.as_bytes().get(open_index).copied() != Some(b'{') {
        return None;
    }

    let mut depth = 0usize;
    for (offset, byte) in content[open_index..].bytes().enumerate() {
        match byte {
            b'{' => depth += 1,
            b'}' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(open_index + offset + 1);
                }
            }
            _ => {}
        }
    }

    None
}

#[test]
fn non_test_source_does_not_use_panic_unwrap_expect() {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();

    for file in collect_rust_files(&src_root) {
        let path_str = file.to_string_lossy();
        if path_str.ends_with("/tests.rs") || path_str.contains("/tests/") || path_str.ends_with("_tests.rs") {
            continue;
        }

        let content = match fs::read_to_string(&file) {
            Ok(content) => content,
            Err(err) => panic!("failed to read source file {}: {err}", file.display()),
        };

        let non_test_code = strip_test_blocks(&content);

        if non_test_code.contains("panic!(")
            || non_test_code.contains(".unwrap(")
            || non_test_code.contains(".expect(")
        {
            offenders.push(file);
        }
    }

    assert!(
        offenders.is_empty(),
        "found panic-prone calls in non-test source files: {:?}",
        offenders
    );
}
