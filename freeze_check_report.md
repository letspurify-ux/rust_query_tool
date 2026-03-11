# Freeze / Hang Static Review Report (Windows + FLTK + OpenGL)

## 1. Executive summary

### Most likely freeze causes (Top 3)
1. **Blocking `recv()` after GPU splash fallback path** in startup splash flow could block UI thread indefinitely when GPU splash is skipped and bootstrap thread stalls/disconnects.
2. **Modal dialog loops using `while dialog.shown() { app::wait(); ... }` at many sites** can appear unresponsive if callbacks do heavy work synchronously or if close signaling is lost.
3. **High-frequency redraw/timer pressure around splash animation + OpenGL draw path** (30 FPS timer + overlay redraw + GL redraw), which can amplify Windows message-loop starvation if combined with other expensive UI work.

### Critical items requiring immediate fix
- **Fixed:** replaced potentially unbounded blocking `result_receiver.recv()` with timeout-aware `recv_timeout()` in splash skip path, preserving timeout fallback behavior.

### Lower-probability, Windows-biased items
- `wait_for()` use in splash fade/destroy helper can be risky if reused in `while wait_for()` pattern later (Windows semantics caveat). Current use does not use return value as loop condition, so risk is low.
- GL initialization timing (`make_current` + first draw init) is acceptable but should remain strictly in `GlWindow::draw` path.

---

## 2. Findings

### Finding 1 — Potential UI-thread hard block after GPU-splash fallback
- **Severity:** critical
- **Location:** `src/splash/mod.rs`, `run_with_splash()` skip branch
- **Pattern:** when `skip_splash == true`, code destroyed splash window and called blocking `result_receiver.recv()`.
- **Why it can freeze/hang:** if bootstrap worker hangs (e.g., blocked I/O) or channel state is abnormal, the UI thread blocks permanently and app appears frozen at startup.
- **Repro estimate:** higher on Windows when GPU visual setup fails and splash is skipped, then bootstrap path is delayed/stuck.
- **Fix:** switched to `recv_timeout(remaining)` where `remaining = bootstrap_timeout - elapsed`, and marked timeout to execute existing timeout fallback path.
- **Patch status:** applied.

### Finding 2 — Windows busy-loop risk pattern audit for `wait_for()`
- **Severity:** medium
- **Location:** `src/splash/mod.rs`, `fade_out_and_destroy()` / `destroy_window()`
- **Pattern:** uses `app::wait_for(...)` in short lifecycle loops.
- **Why risky:** on Windows, `wait_for()` can be misleading if used as loop condition (`while wait_for()`) and may create CPU spin loops.
- **Current status:** current code **does not** use `wait_for()` as loop condition; it uses elapsed-time guard, so direct busy-loop risk is constrained.
- **Fix guidance:** keep `wait_for()` return value ignored unless needed; avoid conditional `while wait_for()` forever loops.

### Finding 3 — Multiple modal wait loops across UI dialogs
- **Severity:** medium
- **Location:** e.g. `connection_dialog`, `query_history`, `find_replace`, `log_viewer`, `object_browser`, `sql_editor/*`
- **Pattern:** repeated `while dialog.shown() { app::wait(); while let Ok(msg)=receiver.try_recv() { ... } }`.
- **Why risky:** not inherently wrong, but can become pseudo-freeze when handlers run heavy sync work in the same loop or message pump starves due to redraw storms.
- **Repro estimate:** intermittent “Not Responding” feel under heavy DB operations + UI churn.
- **Fix guidance:** keep heavy work in worker threads (already mostly done), ensure loop handlers remain lightweight, add watchdog logs for long handler durations.

### Finding 4 — OpenGL draw callback keeps lock around renderer init/render
- **Severity:** medium
- **Location:** `src/splash/mod.rs`, `install_gpu_draw()` draw closure
- **Pattern:** draw callback takes `renderer_slot` mutex, may initialize renderer and render in same closure.
- **Why risky:** if render path grows expensive, lock scope could increase frame latency and event processing jitter.
- **Current status:** lock is local and single-widget; no cross-thread UI mutation detected.
- **Fix guidance:** keep renderer-slot lock narrow, avoid any blocking operation while holding it.

### Finding 5 — Timer-driven redraw on both overlay and GL background
- **Severity:** low
- **Location:** `src/splash/mod.rs`, `start_animation_timer()`
- **Pattern:** each tick redraws overlay + optional GL background (30 FPS).
- **Why risky:** duplicated redraw scheduling on same window can increase CPU/GPU pressure.
- **Current status:** acceptable for splash duration, bounded by `running` flag.
- **Fix guidance:** if startup load is high, consider dirty-flag redraw or frame cap reduction during bootstrap spikes.

---

## 3. Architecture recommendations

### UI thread / worker boundary
- Keep **all FLTK widget/window lifecycle calls** on UI thread only.
- Continue worker -> UI communication via channel + `app::awake()` (current pattern is mostly correct).
- Avoid any future worker closure capturing UI widgets, even cloned handles.

### Redraw scheduler unification
- For each visual surface (especially GL), prefer one scheduler path.
- Avoid combining timer + idle + channel-triggered redraw without coalescing.
- Add a `pending_redraw`/`frame_requested` flag to merge redundant redraws.

### Animation/frame pump
- Preserve fixed frame cap (already 30 FPS).
- Optionally skip redraw when splash hidden/minimized or when no visual state changed.

### Lock strategy
- No blocking/long computation under mutex in draw/handle/timeout callbacks.
- In draw callbacks, copy minimal state out of lock and render afterwards.

### GL resource lifecycle
- Keep context-dependent init strictly inside draw after `make_current()`.
- On resize, continue relying on per-frame viewport setup (already present in renderer).
- Maintain renderer teardown on drop; avoid cross-thread GL resource access.

---

## 4. Minimal safe patch plan

1. **(Done)** Remove unbounded blocking wait in splash fallback path (`recv` -> timeout-aware `recv_timeout`).
2. Add lightweight latency logging around long modal-loop handlers (>100ms) to identify event-loop stalls.
3. Add redraw coalescing flag for splash (or future GL widgets) to prevent duplicate frame requests from multiple trigger paths.

---

## Windows OpenGL freeze triage

- Check whether startup took GPU-splash skip path.
- Confirm bootstrap timeout fallback activated vs indefinite wait.
- Record first `GlWindow::draw` timing and renderer init success/failure.
- Track frame cadence (requested vs rendered) and any handler overrun (>16ms, >33ms).

---

## Suspected loop/callback hotspots (for targeted debugging)

- `src/splash/mod.rs`: startup loop, fade-out loop, animation timeout callback.
- `src/ui/sql_editor/dba_tools.rs`: multiple modal dialog loops + auto-refresh thread coordination.
- `src/ui/main_window.rs`: channel poll scheduler (`add_timeout3`) and repeated rescheduling.

---

## Suggested log points for real freeze root-cause tracing

1. Splash lifecycle checkpoints: `show -> first wait -> first draw -> bootstrap result -> destroy`.
2. GPU draw callback duration histogram (ms) and renderer init result.
3. Modal dialog event-loop iteration timing with queue depth.
4. Worker->UI message lag (send timestamp vs consume timestamp).
5. Timeout scheduler cadence drift (`planned_at` vs `fired_at`).

