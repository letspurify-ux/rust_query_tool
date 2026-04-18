use fltk::{app, input::IntInput, prelude::*, window::Window};
use space_query::db::connection::DatabaseType;
use space_query::db::{create_shared_connection, lock_connection, ConnectionInfo, QueryExecutor};
use space_query::ui::sql_editor::{QueryProgress, SqlEditorWidget};
use std::collections::VecDeque;
use std::env;
use std::process;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

#[derive(Default)]
struct ScriptProgressSummary {
    saw_batch_start: bool,
    statement_count: usize,
    recent_output: VecDeque<String>,
    failures: Vec<String>,
}

impl ScriptProgressSummary {
    fn push_output_lines(&mut self, lines: Vec<String>) {
        for line in lines {
            if self.recent_output.len() == 20 {
                self.recent_output.pop_front();
            }
            self.recent_output.push_back(line);
        }
    }

    fn push_failure(&mut self, details: String) {
        self.failures.push(details);
    }
}

fn test_connection_info() -> ConnectionInfo {
    ConnectionInfo {
        name: "oracle-oci-script-probe".to_string(),
        username: env::var("ORACLE_TEST_USER").unwrap_or_else(|_| "system".to_string()),
        password: env::var("ORACLE_TEST_PASSWORD").unwrap_or_else(|_| "password".to_string()),
        host: env::var("ORACLE_TEST_HOST").unwrap_or_else(|_| "localhost".to_string()),
        port: env::var("ORACLE_TEST_PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1521),
        service_name: env::var("ORACLE_TEST_SERVICE").unwrap_or_else(|_| "FREE".to_string()),
        db_type: DatabaseType::Oracle,
    }
}

fn cleanup_stale_local_oracle_test_sessions() -> Result<(), String> {
    let shared_connection = create_shared_connection();
    let conn = {
        let mut guard = lock_connection(&shared_connection);
        guard.connect(test_connection_info())?;
        guard.require_live_connection()?
    };

    let sessions = QueryExecutor::execute(
        conn.as_ref(),
        r#"
        SELECT sid, serial#
        FROM sys.v_$session
        WHERE username = USER
          AND sid <> TO_NUMBER(SYS_CONTEXT('USERENV', 'SID'))
          AND machine = SYS_CONTEXT('USERENV', 'HOST')
        "#,
    )
    .map_err(|err| err.to_string())?;

    for row in sessions.rows {
        if row.len() < 2 {
            continue;
        }
        let sid = row[0].trim();
        let serial = row[1].trim();
        if sid.is_empty() || serial.is_empty() {
            continue;
        }
        let _ = QueryExecutor::execute(
            conn.as_ref(),
            &format!("ALTER SYSTEM KILL SESSION '{sid},{serial}' IMMEDIATE"),
        );
    }

    let mut guard = lock_connection(&shared_connection);
    guard.disconnect();
    Ok(())
}

fn wait_for_batch_finish(finished: &AtomicBool, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !finished.load(Ordering::SeqCst) && Instant::now() < deadline {
        let _ = app::wait();
    }
}

fn main() {
    let _app = app::App::default();

    if let Err(err) = cleanup_stale_local_oracle_test_sessions() {
        eprintln!("failed to clean stale Oracle OCI probe sessions: {err}");
        process::exit(1);
    }

    let shared_connection = create_shared_connection();
    {
        let mut guard = lock_connection(&shared_connection);
        if let Err(err) = guard.connect(test_connection_info()) {
            eprintln!("failed to connect Oracle OCI probe session: {err}");
            process::exit(1);
        }
    }

    let mut window = Window::default().with_size(1, 1);
    let mut timeout_input = IntInput::default();
    timeout_input.set_value("300");
    let mut widget = SqlEditorWidget::new(shared_connection.clone(), timeout_input);
    widget.set_text(include_str!("../../test/test_all.sql"));
    window.end();
    window.show();

    let summary = Arc::new(Mutex::new(ScriptProgressSummary::default()));
    let finished = Arc::new(AtomicBool::new(false));
    widget.set_progress_callback({
        let summary = Arc::clone(&summary);
        let finished = Arc::clone(&finished);
        move |progress| {
            let mut guard = summary
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match progress {
                QueryProgress::BatchStart => guard.saw_batch_start = true,
                QueryProgress::ScriptOutput { lines } => guard.push_output_lines(lines),
                QueryProgress::StatementFinished {
                    index,
                    result,
                    timed_out,
                    ..
                } => {
                    guard.statement_count += 1;
                    if timed_out || !result.success {
                        let mut details = format!(
                            "statement #{index} failed\nSQL:\n{}\nMessage:\n{}",
                            result.sql, result.message
                        );
                        if timed_out {
                            details.push_str("\nTimed out: true");
                        }
                        if !guard.recent_output.is_empty() {
                            details.push_str("\nRecent script output:");
                            for line in &guard.recent_output {
                                details.push('\n');
                                details.push_str(line);
                            }
                        }
                        guard.push_failure(details);
                    }
                }
                QueryProgress::WorkerPanicked { message } => {
                    guard.push_failure(format!("worker panicked: {message}"));
                }
                QueryProgress::BatchFinished => {
                    finished.store(true, Ordering::SeqCst);
                }
                _ => {}
            }
        }
    });

    widget.execute_current();
    wait_for_batch_finish(finished.as_ref(), Duration::from_secs(300));

    {
        let mut guard = lock_connection(&shared_connection);
        guard.disconnect();
    }
    window.hide();

    let summary = summary
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if !finished.load(Ordering::SeqCst) {
        eprintln!(
            "timed out waiting for Oracle OCI script execution to finish after observing {} statement result(s)",
            summary.statement_count
        );
        process::exit(1);
    }
    if !summary.saw_batch_start {
        eprintln!("batch execution did not start");
        process::exit(1);
    }
    if summary.statement_count == 0 {
        eprintln!("script produced no statement results");
        process::exit(1);
    }
    if !summary.failures.is_empty() {
        eprintln!(
            "Oracle OCI failed to execute test/test_all.sql cleanly.\n\n{}",
            summary.failures.join("\n\n--------------------\n\n")
        );
        process::exit(1);
    }

    println!(
        "Oracle OCI executed test/test_all.sql successfully with {} statement result(s).",
        summary.statement_count
    );
}
