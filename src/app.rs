use crate::splash::{self, LoadingHandle, SplashOptions};
use crate::ui::{theme, MainWindow};
use crate::utils::{self, AppConfig};
use fltk::app;
#[cfg(feature = "gpu-splash")]
use fltk::enums::Mode;

pub struct StartupContext {
    pub config: AppConfig,
    pub crash_report: Option<String>,
}

pub struct App;

impl App {
    pub fn new() -> Self {
        Self
    }

    fn bootstrap(loading: LoadingHandle) -> StartupContext {
        loading.update("BOOTSTRAPPING WORKSPACE", "Loading user settings", 0.18);
        let config = AppConfig::load();

        loading.update(
            "BOOTSTRAPPING WORKSPACE",
            "Inspecting previous session diagnostics",
            0.64,
        );
        let crash_report = utils::logging::take_crash_log();

        loading.update("BOOTSTRAPPING WORKSPACE", "Preparing main workspace", 0.92);

        StartupContext {
            config,
            crash_report,
        }
    }

    fn bootstrap_without_splash() -> StartupContext {
        let config = AppConfig::load();
        let crash_report = utils::logging::take_crash_log();

        StartupContext {
            config,
            crash_report,
        }
    }

    pub fn run(&self) {
        let app = app::App::default()
            .with_scheme(app::Scheme::Gtk)
            .load_system_fonts();

        let prefer_gpu = configure_gpu_visual();
        let startup = if prefer_gpu {
            splash::run_with_splash(SplashOptions::space_query(), Self::bootstrap)
        } else {
            Self::bootstrap_without_splash()
        };
        configure_fltk_globals(&startup.config);

        let current_group = fltk::group::Group::try_current();
        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let mut main_window = MainWindow::new_with_config(startup.config);
        main_window.setup_callbacks();
        main_window.show();

        if let Some(crash_report) = startup.crash_report.as_deref() {
            MainWindow::show_previous_crash_report(crash_report);
        }

        match app.run() {
            Ok(()) => {}
            Err(err) => {
                utils::logging::log_error("app", &format!("App run error: {err}"));
                eprintln!("Failed to run app: {err}");
            }
        }

        if let Some(ref group) = current_group {
            fltk::group::Group::set_current(Some(group));
        }
    }
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn configure_fltk_globals(config: &AppConfig) {
    let ui_size = config.ui_font_size.clamp(8, 24) as i32;
    app::set_font_size(ui_size);
    fltk::misc::Tooltip::set_font_size(ui_size);

    let (bg_r, bg_g, bg_b) = theme::app_background().to_rgb();
    app::background(bg_r, bg_g, bg_b);
    let (fg_r, fg_g, fg_b) = theme::app_foreground().to_rgb();
    app::foreground(fg_r, fg_g, fg_b);
}

fn configure_gpu_visual() -> bool {
    #[cfg(target_os = "windows")]
    {
        return false;
    }

    #[cfg(feature = "gpu-splash")]
    {
        let mode = Mode::Rgb8
            | Mode::Double
            | Mode::Depth
            | Mode::Alpha
            | Mode::MultiSample
            | Mode::Opengl3;
        match app::set_gl_visual(mode) {
            Ok(()) => true,
            Err(err) => {
                utils::logging::log_warning(
                    "splash",
                    &format!("OpenGL splash visual unavailable, skipping splash: {err}"),
                );
                false
            }
        }
    }

    #[cfg(not(feature = "gpu-splash"))]
    {
        false
    }
}
