use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

const AUTO_DISMISS_AFTER_READY: Duration = Duration::from_millis(1500);

/// Small UI event payload sent from the background bootstrap thread to the
/// splash window on the FLTK UI thread.
#[derive(Clone, Debug)]
pub enum SplashEvent {
    Loading(LoadingSnapshot),
    BootstrapFinished,
}

/// Immutable progress snapshot so the worker thread never shares mutable state
/// with the UI thread directly.
#[cfg_attr(not(feature = "gpu-splash"), allow(dead_code))]
#[derive(Clone, Debug)]
pub struct LoadingSnapshot {
    pub stage: String,
    pub detail: String,
    pub progress: f32,
}

impl LoadingSnapshot {
    pub fn new(stage: String, detail: String, progress: f32) -> Self {
        Self {
            stage,
            detail,
            progress: progress.clamp(0.0, 1.0),
        }
    }
}

/// Thread-safe handle passed into application bootstrap work.
#[derive(Clone)]
pub struct LoadingHandle {
    sender: Sender<SplashEvent>,
}

impl LoadingHandle {
    pub(crate) fn new(sender: Sender<SplashEvent>) -> Self {
        Self { sender }
    }

    /// Update the visible loading stage and progress.
    pub fn update<S1, S2>(&self, stage: S1, detail: S2, progress: f32)
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        let _ = self.sender.send(SplashEvent::Loading(LoadingSnapshot::new(
            stage.into(),
            detail.into(),
            progress,
        )));
    }

    pub(crate) fn finish(&self) {
        let _ = self.sender.send(SplashEvent::BootstrapFinished);
    }
}

/// UI-owned loading model. The splash keeps this state on the main thread and
/// mutates it only in response to events.
#[cfg_attr(not(feature = "gpu-splash"), allow(dead_code))]
#[derive(Debug)]
pub struct LoadingState {
    snapshot: LoadingSnapshot,
    started_at: Instant,
    minimum_display: Duration,
    bootstrap_finished_at: Option<Instant>,
    dismiss_requested: bool,
}

#[cfg_attr(not(feature = "gpu-splash"), allow(dead_code))]
impl LoadingState {
    pub fn new(
        minimum_display: Duration,
        initial_stage: impl Into<String>,
        initial_detail: impl Into<String>,
    ) -> Self {
        Self {
            snapshot: LoadingSnapshot::new(initial_stage.into(), initial_detail.into(), 0.08),
            started_at: Instant::now(),
            minimum_display,
            bootstrap_finished_at: None,
            dismiss_requested: false,
        }
    }

    pub fn apply_snapshot(&mut self, snapshot: LoadingSnapshot) {
        self.snapshot = snapshot;
    }

    pub fn mark_bootstrap_finished(&mut self) {
        if self.bootstrap_finished_at.is_none() {
            self.bootstrap_finished_at = Some(Instant::now());
            self.snapshot.progress = self.snapshot.progress.max(1.0);
        }
    }

    pub fn request_close(&mut self) {
        self.dismiss_requested = true;
    }

    pub fn stage_label(&self) -> String {
        if self.bootstrap_finished_at.is_some() && !self.should_close() {
            "WORKSPACE READY".to_string()
        } else {
            self.snapshot.stage.clone()
        }
    }

    pub fn detail_label(&self, dot_count: usize) -> String {
        let suffix = ".".repeat(dot_count.max(1));
        if self.bootstrap_finished_at.is_some() && !self.should_close() {
            "Click anywhere to continue".to_string()
        } else {
            format!("{}{suffix}", self.snapshot.detail)
        }
    }

    /// Keep the progress line moving gently to the right while the mandatory
    /// minimum splash duration is still in effect.
    pub fn display_progress(&self) -> f32 {
        let elapsed_ratio = if self.minimum_display.is_zero() {
            1.0
        } else {
            (self.started_at.elapsed().as_secs_f32() / self.minimum_display.as_secs_f32())
                .clamp(0.0, 1.0)
        };
        if self.bootstrap_finished_at.is_some() {
            self.snapshot.progress.max(elapsed_ratio)
        } else {
            self.snapshot.progress.min(0.94)
        }
    }

    pub fn should_close(&self) -> bool {
        let minimum_display_elapsed = self.started_at.elapsed() >= self.minimum_display;
        let auto_dismiss_elapsed = self
            .bootstrap_finished_at
            .map(|finished_at| finished_at.elapsed() >= AUTO_DISMISS_AFTER_READY)
            .unwrap_or(false);

        self.bootstrap_finished_at.is_some()
            && minimum_display_elapsed
            && (self.dismiss_requested || auto_dismiss_elapsed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_close_after_bootstrap_when_auto_dismiss_elapsed() {
        let mut state = LoadingState::new(
            Duration::from_millis(5),
            "BOOTSTRAPPING WORKSPACE",
            "Preparing launch surface",
        );

        std::thread::sleep(Duration::from_millis(6));
        state.mark_bootstrap_finished();
        std::thread::sleep(AUTO_DISMISS_AFTER_READY + Duration::from_millis(20));

        assert!(state.should_close());
    }

    #[test]
    fn should_close_after_bootstrap_when_user_requests_close() {
        let mut state = LoadingState::new(
            Duration::from_millis(5),
            "BOOTSTRAPPING WORKSPACE",
            "Preparing launch surface",
        );

        std::thread::sleep(Duration::from_millis(6));
        state.mark_bootstrap_finished();
        state.request_close();

        assert!(state.should_close());
    }
}
