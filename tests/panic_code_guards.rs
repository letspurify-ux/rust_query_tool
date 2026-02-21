use std::ffi::OsStr;
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

fn find_banned_patterns(content: &str) -> Vec<&'static str> {
    const BANNED_PATTERNS: [&str; 8] = [
        "panic!(",
        ".unwrap(",
        ".expect(",
        ".unwrap_err(",
        ".unwrap_unchecked(",
        "Option::unwrap",
        "Result::unwrap",
        "Result::expect",
    ];

    BANNED_PATTERNS
        .iter()
        .filter(|pattern| content.contains(**pattern))
        .copied()
        .collect()
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

#[test]
fn non_test_source_does_not_use_panic_prone_calls() {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();

    for file in collect_rust_files(&src_root) {
        if is_test_source_file(&file) {
            continue;
        }

        let content = match fs::read_to_string(&file) {
            Ok(content) => content,
            Err(err) => panic!("failed to read source file {}: {err}", file.display()),
        };

        let non_test_code = strip_test_blocks(&content);
        let matched_patterns = find_banned_patterns(&non_test_code);

        if !matched_patterns.is_empty() {
            offenders.push((file, matched_patterns));
        }
    }

    assert!(
        offenders.is_empty(),
        "found panic-prone calls in non-test source files: {:?}",
        offenders
    );
}
