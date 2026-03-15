mod renderer;
mod shaders;

/// Runs the OpenGL space-themed splash screen.
/// Blocks until dismissed (10 second timeout or mouse click / Escape key).
/// Must be called BEFORE any FLTK initialization.
///
/// If the GPU or OpenGL context is unavailable, the splash is silently
/// skipped and control returns immediately so the main window can start.
pub fn run_splash() {
    // In headless environments (CI, SSH), skip the splash gracefully.
    #[cfg(target_os = "linux")]
    {
        if std::env::var("DISPLAY").is_err() && std::env::var("WAYLAND_DISPLAY").is_err() {
            return;
        }
    }

    // On Linux, verify that a usable OpenGL library is loadable before
    // attempting to create a window.  This avoids a hard panic inside
    // miniquad when no GPU driver is installed.
    #[cfg(target_os = "linux")]
    {
        if !has_opengl_library() {
            return;
        }
    }

    // Catch any panic from miniquad (e.g. GL context creation failure on
    // systems without a GPU) so the main application can still start.
    let result = std::panic::catch_unwind(|| {
        let conf = miniquad::conf::Conf {
            window_title: "SPACE Query".to_string(),
            window_width: 504,
            window_height: 315,
            window_resizable: false,
            borderless: true,
            high_dpi: true,
            ..Default::default()
        };

        miniquad::start(conf, move || Box::new(renderer::SplashStage::new()));
    });

    if let Err(e) = result {
        let msg = if let Some(s) = e.downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = e.downcast_ref::<String>() {
            s.clone()
        } else {
            "unknown error".to_string()
        };
        eprintln!("Splash screen skipped (no GPU or OpenGL unavailable): {msg}");
    }
}

/// Checks whether a usable OpenGL shared library can be loaded.
#[cfg(target_os = "linux")]
fn has_opengl_library() -> bool {
    use std::ffi::CStr;

    const RTLD_LAZY: libc::c_int = 0x0001;

    // Common OpenGL library names on Linux
    const LIB_NAMES: &[&CStr] = unsafe {
        &[
            CStr::from_bytes_with_nul_unchecked(b"libGL.so.1\0"),
            CStr::from_bytes_with_nul_unchecked(b"libGL.so\0"),
            CStr::from_bytes_with_nul_unchecked(b"libEGL.so.1\0"),
            CStr::from_bytes_with_nul_unchecked(b"libEGL.so\0"),
        ]
    };

    for name in LIB_NAMES {
        // SAFETY: dlopen with RTLD_LAZY is safe to probe; we close the handle
        // immediately if the library loads successfully.
        let handle = unsafe { libc::dlopen(name.as_ptr(), RTLD_LAZY) };
        if !handle.is_null() {
            unsafe { libc::dlclose(handle) };
            return true;
        }
    }

    false
}
