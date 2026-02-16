use std::fs;
use std::io::Write;
use std::panic;
use std::path::PathBuf;

/// Install a global panic hook that:
/// 1. Logs the panic via `tracing::error!`
/// 2. Writes a crash report file to `~/.local/share/space_query/crashes/`
/// 3. Delegates to the default panic hook for stderr output
///
/// Must be called once, early in `main()`, **after** logging is initialized.
pub fn install() {
    let default_hook = panic::take_hook();

    panic::set_hook(Box::new(move |panic_info| {
        // Log via tracing (will be captured by the file appender)
        tracing::error!("{}", panic_info);

        // Write a standalone crash report file
        match write_crash_report(panic_info) {
            Some(path) => {
                eprintln!(
                    "SPACE Query crashed unexpectedly. Report saved to: {}",
                    path.display()
                );
            }
            None => {
                eprintln!("SPACE Query crashed unexpectedly. Could not write crash report.");
            }
        }

        // Run the default hook (prints to stderr)
        default_hook(panic_info);
    }));
}

fn crash_directory() -> Option<PathBuf> {
    let mut path = dirs::data_dir()?;
    path.push("space_query");
    path.push("crashes");
    fs::create_dir_all(&path).ok()?;
    Some(path)
}

fn write_crash_report(info: &panic::PanicHookInfo) -> Option<PathBuf> {
    let mut path = crash_directory()?;

    let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
    path.push(format!("crash_{timestamp}.txt"));

    let mut file = fs::File::create(&path).ok()?;

    writeln!(file, "SPACE Query – Crash Report").ok()?;
    writeln!(file, "==========================").ok()?;
    writeln!(file, "Time    : {}", chrono::Local::now()).ok()?;
    writeln!(file, "Version : {}", env!("CARGO_PKG_VERSION")).ok()?;
    writeln!(file, "OS      : {} ({})", std::env::consts::OS, std::env::consts::ARCH).ok()?;
    writeln!(file).ok()?;

    if let Some(location) = info.location() {
        writeln!(file, "Location: {}:{}", location.file(), location.line()).ok()?;
        writeln!(file).ok()?;
    }

    writeln!(file, "Panic payload:").ok()?;
    if let Some(msg) = info.payload().downcast_ref::<&str>() {
        writeln!(file, "  {msg}").ok()?;
    } else if let Some(msg) = info.payload().downcast_ref::<String>() {
        writeln!(file, "  {msg}").ok()?;
    } else {
        writeln!(file, "  (non-string payload)").ok()?;
    }

    writeln!(file).ok()?;
    writeln!(file, "Full info:").ok()?;
    writeln!(file, "  {info}").ok()?;

    Some(path)
}
