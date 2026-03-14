mod renderer;
mod shaders;

/// Runs the OpenGL space-themed splash screen.
/// Blocks until dismissed (10 second timeout or mouse click / Escape key).
/// Must be called BEFORE any FLTK initialization.
pub fn run_splash() {
    // In headless environments (CI, SSH), skip the splash gracefully.
    #[cfg(target_os = "linux")]
    {
        if std::env::var("DISPLAY").is_err() && std::env::var("WAYLAND_DISPLAY").is_err() {
            return;
        }
    }

    let conf = miniquad::conf::Conf {
        window_title: "SPACE Query".to_string(),
        window_width: 720,
        window_height: 450,
        window_resizable: false,
        high_dpi: true,
        ..Default::default()
    };

    miniquad::start(conf, move || Box::new(renderer::SplashStage::new()));
}
