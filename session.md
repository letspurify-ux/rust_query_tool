# Session Management Rules

This document records the intended session ownership and cleanup rules for query execution.

## Goals

- A query tab must not leak database locks into unrelated tabs.
- A pooled session should remain attached to a tab only when it is needed to preserve an open transaction, explicit session lock, or session-bound state required by the current script.
- If a pooled session is retained, it must be visible in Session Activity.
- Oracle, MySQL, and MariaDB must follow the same high-level lifecycle rules. MariaDB uses the MySQL execution/session path in this codebase.

## Ownership

- Each query tab owns its own `pooled_db_session` lease.
- A tab must not directly reuse another tab's pooled session lease.
- A pooled lease is valid only for the same connection generation and database type.
- If a query tab is closed, its pooled session lease must be cleared.

## Return vs Retain

The SQL range selected by the user is one execution unit. Statements inside that range must run on the same database session when the backend has session-scoped state. After the selected range finishes, the app decides whether the pooled session still needs to be retained.

After selected-range execution finishes:

- If no open transaction, explicit session lock, or required session-bound state needs preservation, return the pooled session immediately.
- If an open transaction, explicit session lock, or required session-bound state may still exist, retain the pooled session on the same query tab.
- Retained sessions must be released by an explicit `COMMIT`, `ROLLBACK`, lock release, autocommit reset, or by closing the tab.

For MySQL/MariaDB:

- The selected range is held on one pooled session between statements so temporary tables, user variables, prepared statements, transaction directives, and stored procedure effects are visible to later statements in the same range.
- At the end of the selected range, session-bound state that was only needed inside the range is discarded unless a transaction or explicit lock is still open.
- Read-only statements run with `autocommit=0` may open read transactions.
- If a read-only statement had no active transaction before execution, the app should `ROLLBACK` after the statement to close the read transaction.
- `LOCK TABLES` and `GET_LOCK` are treated as explicit session lock preservation cases. `UNLOCK TABLES`, `RELEASE_LOCK`, and `RELEASE_ALL_LOCKS` clear that preservation requirement for the selected range.
- With `autocommit=0`, DML, `CALL`, transaction control with `AND CHAIN`, `SAVEPOINT`, `SET`, `PREPARE`/`EXECUTE`/`DEALLOCATE`, temporary-table DDL, and `SELECT ... INTO @var` are treated as session-retaining statements.
- `CALL` can perform DML without returning a reliable transaction signal on every server/version, so it must retain the session while `autocommit=0`.
- Plain read-only statements that do not create session-bound state should be rolled back and returned immediately when no transaction was already active.

For Oracle:

- The selected range already runs on one Oracle connection in the execution worker.
- A pooled session is retained only when Oracle reports an active local transaction.
- Read-only transaction cleanup should rollback the read-only transaction before considering the session idle.

## Cross-Tab Protection

Before starting a query in one tab:

- If another tab has a retained pooled session with an open transaction/session lock or required session-bound state, block the new execution.
- The user must commit, rollback, release the lock, or close the owning tab before running from another tab.
- Background idle pooled sessions that do not require preservation should be released automatically before the new execution.

## Session Activity

Session Activity must show retained pooled sessions.

- `Pooled session (session state retained)` means the tab owns a retained session that can affect other tabs.
- Plain idle pooled sessions should normally not remain after execution. If shown, they must be safe to release.
- Active query progress rows and retained pooled session rows are both part of the session activity view.

## Timeout Rules

For MySQL/MariaDB:

- Query timeout should apply to statement execution where supported.
- Lock waits must have a bounded timeout even when the UI timeout field is empty.
- The current default lock wait timeout is 60 seconds.

For Oracle:

- Oracle call timeout is applied around execution and reset during cleanup.

## Failure Handling

- On cancel, timeout, panic, or failed cleanup, do not retain a pooled session unless reuse is known to be safe.
- If session state cannot be inspected reliably, bias toward preventing hidden locks from remaining.
- If a retained session is visible in Session Activity, the owning tab is responsible for resolving it.
