# Freeze Debug Checklist (Windows + FLTK + OpenGL)

## Quick triage
- [ ] Is freeze during splash only, or after main window shown?
- [ ] Did GPU splash initialize or fallback to non-GPU path?
- [ ] Is CPU usage near 100% (busy loop) or near 0% (blocking wait/deadlock)?

## UI thread safety
- [ ] No widget/window show/hide/resize/redraw from worker threads.
- [ ] Worker communicates via channel + `app::awake()` only.

## Event loop / timers
- [ ] No `while app::wait_for(...)` style loop used as condition.
- [ ] Timer callbacks do not immediately requeue redundant redraws.
- [ ] No expensive work inside callback/handle/draw paths.

## OpenGL correctness
- [ ] GL init after context current (`make_current`) in draw path.
- [ ] No GL calls from non-GL callback threads.
- [ ] Resize path updates viewport/projection consistently.

## Lock contention
- [ ] `draw()` does not wait on contended mutexes for long durations.
- [ ] No UI-thread blocking `recv()/join()` without timeout.
- [ ] No lock held while sending UI events or redraw requests.

## Logging points
- [ ] Splash lifecycle timestamps.
- [ ] Draw callback duration and frequency.
- [ ] Channel latency (worker send -> UI consume).
- [ ] Timeout callback drift and backlog size.
