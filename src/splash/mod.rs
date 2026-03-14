mod animation;
mod loading;
#[cfg(feature = "gpu-splash")]
mod overlay;
#[cfg(feature = "gpu-splash")]
mod renderer;
#[cfg(feature = "gpu-splash")]
mod standalone;

use self::animation::AnimationState;
use self::loading::{LoadingState, SplashEvent};
#[cfg(feature = "gpu-splash")]
use self::standalone::StandaloneSplash;

use std::sync::mpsc::{self, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

pub use self::loading::LoadingHandle;

const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(8);

pub struct SplashOptions {
    pub app_name: &'static str,
    pub subtitle: &'static str,
    pub initial_stage: &'static str,
    pub initial_detail: &'static str,
    pub minimum_display: Duration,
    pub bootstrap_timeout: Duration,
    pub default_size: (i32, i32),
}

impl SplashOptions {
    pub fn space_query() -> Self {
        Self {
            app_name: "SPACE Query",
            subtitle: "Database engineering workspace",
            initial_stage: "BOOTSTRAPPING WORKSPACE",
            initial_detail: "Preparing launch surface",
            minimum_display: Duration::from_secs(10),
            bootstrap_timeout: Duration::from_secs(10),
            default_size: (628, 358),
        }
    }
}

enum BootstrapResult<T> {
    Ready(T),
    Failed(String),
}

pub fn gpu_splash_enabled() -> bool {
    cfg!(feature = "gpu-splash")
}

/// Run arbitrary startup work while a dedicated OpenGL splash window stays
/// responsive. FLTK is not initialized until this function returns.
pub fn run_with_splash<T, F, G>(options: SplashOptions, bootstrap: F, timeout_fallback: G) -> T
where
    T: Send + 'static,
    F: FnOnce(LoadingHandle) -> T + Send + 'static,
    G: FnOnce() -> T,
{
    let mut loading_state = LoadingState::new(
        options.minimum_display,
        options.initial_stage,
        options.initial_detail,
    );
    let mut animation_state = AnimationState::new();
    let (event_sender, event_receiver) = mpsc::channel::<SplashEvent>();
    let (result_sender, result_receiver) = mpsc::sync_channel::<BootstrapResult<T>>(1);

    let worker_sender = event_sender;
    thread::spawn(move || {
        let handle = LoadingHandle::new(worker_sender);
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| bootstrap(handle.clone())));
        match result {
            Ok(value) => {
                let _ = result_sender.send(BootstrapResult::Ready(value));
                handle.finish();
            }
            Err(_) => {
                let _ = result_sender.send(BootstrapResult::Failed(
                    "Startup initialization terminated unexpectedly.".to_string(),
                ));
                handle.finish();
            }
        }
    });

    #[cfg(feature = "gpu-splash")]
    let mut splash = match StandaloneSplash::new(&options) {
        Ok(splash) => Some(splash),
        Err(error) => {
            crate::utils::logging::log_warning(
                "splash",
                &format!(
                    "Standalone splash initialization failed, continuing without splash: {error}"
                ),
            );
            None
        }
    };
    #[cfg(not(feature = "gpu-splash"))]
    let mut splash = ();

    let splash_started_at = Instant::now();
    let mut boot_result: Option<T> = None;
    let mut fatal_error: Option<String> = None;
    let mut timeout_fallback = Some(timeout_fallback);
    let mut timed_out = false;

    loop {
        while let Ok(event) = event_receiver.try_recv() {
            match event {
                SplashEvent::Loading(snapshot) => loading_state.apply_snapshot(snapshot),
                SplashEvent::BootstrapFinished => loading_state.mark_bootstrap_finished(),
            }
        }

        if boot_result.is_none() && fatal_error.is_none() {
            match result_receiver.try_recv() {
                Ok(BootstrapResult::Ready(result)) => {
                    loading_state.mark_bootstrap_finished();
                    boot_result = Some(result);
                }
                Ok(BootstrapResult::Failed(error)) => {
                    fatal_error = Some(error);
                }
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => {
                    fatal_error = Some(
                        "Startup bootstrap channel disconnected before completion.".to_string(),
                    );
                }
            }
        }

        if fatal_error.is_some() {
            break;
        }

        let dismiss_requested =
            pump_splash_backend(&mut splash, &loading_state, &mut animation_state);
        if dismiss_requested {
            loading_state.request_close();
        }

        let has_visible_splash = splash_backend_exists(&splash);
        if boot_result.is_some() {
            if !has_visible_splash || loading_state.should_close() {
                break;
            }
        }

        if boot_result.is_none()
            && fatal_error.is_none()
            && splash_started_at.elapsed() >= options.bootstrap_timeout
        {
            timed_out = true;
            crate::utils::logging::log_warning(
                "splash",
                &format!(
                    "Startup bootstrap exceeded {:?}; continuing with timeout fallback.",
                    options.bootstrap_timeout
                ),
            );
            break;
        }
    }

    drop_splash_backend(&mut splash);

    if timed_out && boot_result.is_none() {
        if let Some(fallback) = timeout_fallback.take() {
            return fallback();
        }
        crate::utils::logging::log_error(
            "splash",
            "Startup bootstrap timeout fallback was not available.",
        );
        eprintln!(
            "SPACE Query failed to start.\n\nBootstrap timed out and fallback was unavailable."
        );
        std::process::exit(1);
    }

    if let Some(error) = fatal_error {
        crate::utils::logging::log_error("splash", &error);
        eprintln!(
            "SPACE Query failed to start.\n\n{error}\n\nCheck the application log for details."
        );
        std::process::exit(1);
    }

    match boot_result {
        Some(result) => result,
        None => {
            crate::utils::logging::log_error(
                "splash",
                "Startup bootstrap finished without returning a result.",
            );
            eprintln!("SPACE Query failed to start.\n\nStartup returned no result.");
            std::process::exit(1);
        }
    }
}

#[cfg(feature = "gpu-splash")]
fn splash_backend_exists(splash: &Option<StandaloneSplash>) -> bool {
    splash.is_some()
}

#[cfg(not(feature = "gpu-splash"))]
fn splash_backend_exists(_: &()) -> bool {
    false
}

#[cfg(feature = "gpu-splash")]
fn drop_splash_backend(splash: &mut Option<StandaloneSplash>) {
    *splash = None;
}

#[cfg(not(feature = "gpu-splash"))]
fn drop_splash_backend(_: &mut ()) {}

#[cfg(feature = "gpu-splash")]
fn pump_splash_backend(
    splash: &mut Option<StandaloneSplash>,
    loading_state: &LoadingState,
    animation_state: &mut AnimationState,
) -> bool {
    let pump_result = if let Some(backend) = splash.as_mut() {
        Some(backend.pump(loading_state, animation_state))
    } else {
        None
    };

    if let Some(result) = pump_result {
        match result {
            Ok(dismiss_requested) => dismiss_requested,
            Err(error) => {
                crate::utils::logging::log_warning(
                    "splash",
                    &format!(
                        "Standalone splash render loop failed, continuing without splash: {error}"
                    ),
                );
                *splash = None;
                thread::sleep(IDLE_POLL_INTERVAL);
                false
            }
        }
    } else {
        thread::sleep(IDLE_POLL_INTERVAL);
        false
    }
}

#[cfg(not(feature = "gpu-splash"))]
fn pump_splash_backend(
    _splash: &mut (),
    _loading_state: &LoadingState,
    _animation_state: &mut AnimationState,
) -> bool {
    thread::sleep(IDLE_POLL_INTERVAL);
    false
}
