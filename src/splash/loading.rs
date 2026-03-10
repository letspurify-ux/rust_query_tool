use fltk::app;
use std::time::{Duration, Instant};

/// Small UI event payload sent from the background bootstrap thread to the
/// splash window on the FLTK UI thread.
#[derive(Clone, Debug)]
pub enum SplashEvent {
    Loading(LoadingSnapshot),
    BootstrapFinished,
    GpuUnavailable(String),
}

/// Immutable progress snapshot so the worker thread never shares mutable state
/// with the UI thread directly.
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
    sender: app::Sender<SplashEvent>,
}

impl LoadingHandle {
    pub(crate) fn new(sender: app::Sender<SplashEvent>) -> Self {
        Self { sender }
    }

    /// Update the visible loading stage and progress.
    pub fn update<S1, S2>(&self, stage: S1, detail: S2, progress: f32)
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        self.sender.send(SplashEvent::Loading(LoadingSnapshot::new(
            stage.into(),
            detail.into(),
            progress,
        )));
    }

    pub(crate) fn finish(&self) {
        self.sender.send(SplashEvent::BootstrapFinished);
    }
}

/// UI-owned loading model. The splash keeps this state on the main thread and
/// mutates it only in response to events.
#[derive(Debug)]
pub struct LoadingState {
    snapshot: LoadingSnapshot,
    started_at: Instant,
    minimum_display: Duration,
    bootstrap_finished_at: Option<Instant>,
}

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

    pub fn stage_label(&self) -> String {
        if self.bootstrap_finished_at.is_some() && !self.should_close() {
            "FINALIZING INTERFACE".to_string()
        } else {
            self.snapshot.stage.clone()
        }
    }

    pub fn detail_label(&self, dot_count: usize) -> String {
        let suffix = ".".repeat(dot_count.max(1));
        if self.bootstrap_finished_at.is_some() && !self.should_close() {
            format!("Bringing workspace online{suffix}")
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
        self.bootstrap_finished_at.is_some() && self.started_at.elapsed() >= self.minimum_display
    }
}
