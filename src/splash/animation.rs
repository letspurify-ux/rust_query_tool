use std::time::Instant;

/// Lightweight animation state shared by the splash background and overlay.
///
/// The splash intentionally avoids a full scene graph. A small deterministic
/// state vector is enough for subtle camera drift, progress shimmer, and the
/// loading text pulse.
#[cfg_attr(not(feature = "gpu-splash"), allow(dead_code))]
#[derive(Debug)]
pub struct AnimationState {
    #[cfg(feature = "gpu-splash")]
    started_at: Instant,
    last_tick: Instant,
    time_seconds: f32,
}

#[cfg_attr(not(feature = "gpu-splash"), allow(dead_code))]
impl AnimationState {
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            #[cfg(feature = "gpu-splash")]
            started_at: now,
            last_tick: now,
            time_seconds: 0.0,
        }
    }

    /// Advance the animation clock using a clamped delta so window stalls do not
    /// create huge jumps in the shader or progress shimmer.
    pub fn tick(&mut self) {
        let now = Instant::now();
        let dt = now
            .duration_since(self.last_tick)
            .as_secs_f32()
            .clamp(0.0, 0.050);
        self.last_tick = now;
        self.time_seconds += dt;
    }

    #[cfg(feature = "gpu-splash")]
    pub fn elapsed_seconds(&self) -> f32 {
        self.started_at.elapsed().as_secs_f32()
    }

    /// A very small camera offset keeps the scene from feeling static while
    /// remaining restrained enough for a professional desktop tool.
    #[cfg(feature = "gpu-splash")]
    pub fn camera_offset(&self) -> [f32; 2] {
        let t = self.time_seconds;
        [
            (t * 0.16).sin() * 0.055 + (t * 0.04).cos() * 0.020,
            (t * 0.11).cos() * 0.040 + (t * 0.07).sin() * 0.015,
        ]
    }

    /// Used by the overlay progress line to move a gentle highlight across the
    /// filled segment instead of flashing.
    pub fn shimmer_phase(&self) -> f32 {
        ((self.time_seconds * 0.55).sin() * 0.5) + 0.5
    }

    /// The loading text uses a small dotted cadence rather than a spinner so the
    /// launch experience stays calm and tool-like.
    pub fn loading_dots(&self) -> usize {
        ((self.time_seconds * 1.7) as usize) % 3 + 1
    }
}

impl Default for AnimationState {
    fn default() -> Self {
        Self::new()
    }
}
