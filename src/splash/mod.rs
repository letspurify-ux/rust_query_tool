mod animation;
mod loading;
#[cfg(feature = "gpu-splash")]
mod renderer;

use self::animation::AnimationState;
use self::loading::{LoadingState, SplashEvent};
#[cfg(feature = "gpu-splash")]
use self::renderer::GpuRenderer;

use fltk::{
    app, draw,
    enums::{Align, Color, Event, Font, FrameType},
    frame::Frame,
    group::{Flex, FlexType, Group},
    image::RgbImage,
    prelude::*,
    surface::ImageSurface,
    window::Window,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::{self, TryRecvError},
    Arc, Mutex,
};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(feature = "gpu-splash")]
use fltk::{enums::Mode, window::GlWindow};

const FRAME_INTERVAL_SECONDS: f64 = 1.0 / 30.0;
const FADE_OUT_DURATION: Duration = Duration::from_millis(220);

pub use self::loading::LoadingHandle;

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
            minimum_display: Duration::from_millis(5000),
            bootstrap_timeout: Duration::from_secs(10),
            default_size: (628, 358),
        }
    }
}

struct SplashVisuals {
    window: Window,
    overlay_panel: Frame,
    #[cfg(feature = "gpu-splash")]
    gpu_background: Option<GlWindow>,
}

#[cfg(feature = "gpu-splash")]
enum RendererSlot {
    Uninitialized,
    Ready(GpuRenderer),
    Failed,
}

enum BootstrapResult<T> {
    Ready(T),
    Failed(String),
}

/// Run arbitrary startup work while a dedicated splash window stays responsive.
///
/// The background initialization closure runs on a worker thread. The splash
/// stays in the FLTK UI thread so it never fights the main window event loop.
pub fn run_with_splash<T, F, G>(options: SplashOptions, bootstrap: F, timeout_fallback: G) -> T
where
    T: Send + 'static,
    F: FnOnce(LoadingHandle) -> T + Send + 'static,
    G: FnOnce() -> T,
{
    let loading_state = Arc::new(Mutex::new(LoadingState::new(
        options.minimum_display,
        options.initial_stage,
        options.initial_detail,
    )));
    let animation_state = Arc::new(Mutex::new(AnimationState::new()));
    let running = Arc::new(AtomicBool::new(true));
    let (event_sender, event_receiver) = app::channel::<SplashEvent>();
    let (result_sender, result_receiver) = mpsc::sync_channel::<BootstrapResult<T>>(1);

    let mut visuals =
        build_splash_window(&options, &loading_state, &animation_state, &event_sender);
    visuals.window.show();
    app::flush();
    apply_window_shape(&mut visuals.window);
    let _ = app::wait();

    let mut overlay_panel_for_timer = visuals.overlay_panel.clone();
    #[cfg(feature = "gpu-splash")]
    start_animation_timer(
        &animation_state,
        &running,
        &mut overlay_panel_for_timer,
        visuals.gpu_background.as_ref().cloned(),
    );

    #[cfg(not(feature = "gpu-splash"))]
    start_animation_timer(&animation_state, &running, &mut overlay_panel_for_timer);

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

    let mut boot_result: Option<T> = None;
    let mut fatal_error: Option<String> = None;
    let mut timeout_fallback = Some(timeout_fallback);
    let splash_started_at = Instant::now();
    let mut timed_out = false;
    #[cfg_attr(not(feature = "gpu-splash"), allow(unused_mut))]
    let mut skip_splash = false;

    loop {
        let _ = app::wait();

        while let Some(event) = event_receiver.recv() {
            match event {
                SplashEvent::Loading(snapshot) => {
                    let lock_result = loading_state.lock();
                    match lock_result {
                        Ok(mut guard) => guard.apply_snapshot(snapshot),
                        Err(poisoned) => {
                            let mut guard = poisoned.into_inner();
                            guard.apply_snapshot(snapshot);
                        }
                    }
                    visuals.overlay_panel.redraw();
                }
                SplashEvent::BootstrapFinished => {
                    let lock_result = loading_state.lock();
                    match lock_result {
                        Ok(mut guard) => guard.mark_bootstrap_finished(),
                        Err(poisoned) => {
                            let mut guard = poisoned.into_inner();
                            guard.mark_bootstrap_finished();
                        }
                    }
                    visuals.overlay_panel.redraw();
                }
                #[cfg(feature = "gpu-splash")]
                SplashEvent::GpuUnavailable(reason) => {
                    crate::utils::logging::log_warning(
                        "splash",
                        &format!("GPU splash initialization failed, skipping splash: {reason}"),
                    );
                    skip_splash = true;
                    break;
                }
            }
        }

        if boot_result.is_none() && fatal_error.is_none() {
            match result_receiver.try_recv() {
                Ok(BootstrapResult::Ready(result)) => {
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

        if skip_splash {
            break;
        }

        if boot_result.is_some() {
            let should_close = match loading_state.lock() {
                Ok(guard) => guard.should_close(),
                Err(poisoned) => poisoned.into_inner().should_close(),
            };
            if should_close {
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

    running.store(false, Ordering::Relaxed);
    if skip_splash {
        destroy_window(&mut visuals.window);
        if boot_result.is_none() && fatal_error.is_none() {
            match result_receiver.recv() {
                Ok(BootstrapResult::Ready(result)) => {
                    boot_result = Some(result);
                }
                Ok(BootstrapResult::Failed(error)) => {
                    fatal_error = Some(error);
                }
                Err(err) => {
                    fatal_error = Some(format!(
                        "Startup bootstrap channel disconnected before completion: {err}"
                    ));
                }
            }
        }
    } else {
        fade_out_and_destroy(&mut visuals.window);
    }

    if timed_out && boot_result.is_none() {
        if let Some(fallback) = timeout_fallback.take() {
            return fallback();
        }
        crate::utils::logging::log_error(
            "splash",
            "Startup bootstrap timeout fallback was not available.",
        );
        fltk::dialog::alert_default(
            "SPACE Query failed to start.\n\nBootstrap timed out and fallback was unavailable.",
        );
        std::process::exit(1);
    }

    if let Some(error) = fatal_error {
        crate::utils::logging::log_error("splash", &error);
        fltk::dialog::alert_default(&format!(
            "SPACE Query failed to start.\n\n{error}\n\nCheck the application log for details."
        ));
        std::process::exit(1);
    }

    match boot_result {
        Some(result) => result,
        None => {
            crate::utils::logging::log_error(
                "splash",
                "Startup bootstrap finished without returning a result.",
            );
            fltk::dialog::alert_default(
                "SPACE Query failed to start.\n\nStartup returned no result.",
            );
            std::process::exit(1);
        }
    }
}

fn build_splash_window(
    options: &SplashOptions,
    loading_state: &Arc<Mutex<LoadingState>>,
    animation_state: &Arc<Mutex<AnimationState>>,
    _event_sender: &app::Sender<SplashEvent>,
) -> SplashVisuals {
    let current_group = Group::try_current();
    Group::set_current(None::<&Group>);

    let (window_width, window_height) = resolve_window_size(options.default_size);
    let mut window = Window::default()
        .with_size(window_width, window_height)
        .with_label(options.app_name)
        .center_screen();
    window.set_border(false);
    window.set_frame(FrameType::FlatBox);
    window.set_color(Color::from_rgb(5, 8, 18));

    window.begin();
    let root = Group::default_fill();
    root.begin();

    #[cfg(feature = "gpu-splash")]
    let mut gpu_background = {
        let mut gl_window = GlWindow::default_fill();
        gl_window.set_frame(FrameType::FlatBox);
        gl_window.set_mode(
            Mode::Rgb8
                | Mode::Double
                | Mode::Depth
                | Mode::Alpha
                | Mode::MultiSample
                | Mode::Opengl3,
        );
        install_gpu_draw(
            &mut gl_window,
            options.app_name,
            loading_state,
            animation_state,
            _event_sender,
        );
        Some(gl_window)
    };

    let mut overlay_layout = Flex::default_fill();
    overlay_layout.set_type(FlexType::Column);
    overlay_layout.set_frame(FrameType::NoBox);
    overlay_layout.set_margin(30);
    overlay_layout.set_spacing(0);

    let mut _top_spacer = Frame::default();
    _top_spacer.set_frame(FrameType::NoBox);

    let mut bottom_row = Flex::default();
    bottom_row.set_type(FlexType::Row);
    bottom_row.set_frame(FrameType::NoBox);
    bottom_row.set_spacing(0);

    let mut overlay_panel = Frame::default().with_size(404, 164);
    overlay_panel.set_frame(FrameType::NoBox);
    install_overlay_panel(
        &mut overlay_panel,
        options.app_name,
        options.subtitle,
        loading_state,
        animation_state,
    );
    bottom_row.fixed(&overlay_panel, 404);

    let mut _right_spacer = Frame::default();
    _right_spacer.set_frame(FrameType::NoBox);
    bottom_row.end();

    overlay_layout.fixed(&bottom_row, 176);
    overlay_layout.end();

    root.end();
    window.end();

    let redraw_window = window.clone();
    let redraw_panel = overlay_panel.clone();
    install_dismiss_handler(&mut window, loading_state, &redraw_window, &redraw_panel);
    let redraw_window = window.clone();
    let redraw_panel = overlay_panel.clone();
    install_dismiss_handler(
        &mut overlay_panel,
        loading_state,
        &redraw_window,
        &redraw_panel,
    );
    #[cfg(feature = "gpu-splash")]
    if let Some(ref mut gl_window) = gpu_background {
        let redraw_window = window.clone();
        let redraw_panel = overlay_panel.clone();
        install_dismiss_handler(gl_window, loading_state, &redraw_window, &redraw_panel);
    }

    if let Some(ref group) = current_group {
        Group::set_current(Some(group));
    }

    SplashVisuals {
        window,
        overlay_panel,
        #[cfg(feature = "gpu-splash")]
        gpu_background,
    }
}

fn install_dismiss_handler<W: WidgetExt + WidgetBase>(
    widget: &mut W,
    loading_state: &Arc<Mutex<LoadingState>>,
    redraw_window: &Window,
    redraw_panel: &Frame,
) {
    let loading_state = loading_state.clone();
    let mut redraw_window = redraw_window.clone();
    let mut redraw_panel = redraw_panel.clone();
    widget.handle(move |_widget, event| match event {
        Event::Push | Event::Released | Event::KeyDown => {
            match loading_state.lock() {
                Ok(mut guard) => guard.request_close(),
                Err(poisoned) => poisoned.into_inner().request_close(),
            }
            redraw_window.redraw();
            redraw_panel.redraw();
            true
        }
        _ => false,
    });
}

fn install_overlay_panel(
    panel: &mut Frame,
    app_name: &'static str,
    subtitle: &'static str,
    loading_state: &Arc<Mutex<LoadingState>>,
    animation_state: &Arc<Mutex<AnimationState>>,
) {
    let loading_state = loading_state.clone();
    let animation_state = animation_state.clone();

    panel.draw(move |frame| {
        let (stage, detail, progress) = match loading_state.lock() {
            Ok(guard) => (
                guard.stage_label(),
                guard.detail_label(
                    animation_state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .loading_dots(),
                ),
                guard.display_progress(),
            ),
            Err(poisoned) => {
                let guard = poisoned.into_inner();
                (
                    guard.stage_label(),
                    guard.detail_label(
                        animation_state
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .loading_dots(),
                    ),
                    guard.display_progress(),
                )
            }
        };

        let shimmer = match animation_state.lock() {
            Ok(guard) => guard.shimmer_phase(),
            Err(poisoned) => poisoned.into_inner().shimmer_phase(),
        };

        let x = frame.x();
        let y = frame.y();
        let w = frame.w();
        let h = frame.h();
        let panel_color = Color::from_rgb(10, 14, 24);
        let border_color = Color::from_rgb(24, 32, 50);
        let accent_color = Color::from_rgb(63, 104, 181);

        draw::draw_box(FrameType::RFlatBox, x, y, w, h, panel_color);
        draw::set_draw_color(border_color);
        draw::draw_box(FrameType::RoundedFrame, x, y, w, h, border_color);

        draw::set_draw_color(Color::from_rgb(32, 46, 78));
        draw::draw_rectf(x + 24, y + 18, w - 48, 2);
        draw::set_draw_color(accent_color);
        draw::draw_rectf(
            x + 24,
            y + 18,
            ((w - 48) as f32 * (0.22 + shimmer * 0.18)) as i32,
            2,
        );

        draw_brand_mark(x + 28, y + 34, 54);

        draw::set_draw_color(Color::from_rgb(232, 238, 248));
        draw::set_font(Font::HelveticaBold, 28);
        draw::draw_text2(
            app_name,
            x + 102,
            y + 32,
            w - 122,
            34,
            Align::Left | Align::Inside,
        );

        draw::set_draw_color(Color::from_rgb(150, 164, 186));
        draw::set_font(Font::Helvetica, 12);
        draw::draw_text2(
            subtitle,
            x + 102,
            y + 64,
            w - 122,
            20,
            Align::Left | Align::Inside,
        );

        draw::set_draw_color(Color::from_rgb(120, 140, 170));
        draw::set_font(Font::Courier, 11);
        draw::draw_text2(
            &stage,
            x + 28,
            y + 96,
            w - 56,
            18,
            Align::Left | Align::Inside,
        );

        draw::set_draw_color(Color::from_rgb(208, 214, 224));
        draw::set_font(Font::Helvetica, 13);
        draw::draw_text2(
            &detail,
            x + 28,
            y + 116,
            w - 56,
            20,
            Align::Left | Align::Inside,
        );

        let bar_x = x + 28;
        let bar_y = y + h - 28;
        let bar_width = w - 56;
        let filled = ((bar_width as f32) * progress.clamp(0.0, 1.0)) as i32;
        draw::set_draw_color(Color::from_rgb(24, 28, 40));
        draw::draw_rectf(bar_x, bar_y, bar_width, 3);
        draw::set_draw_color(Color::from_rgb(70, 112, 186));
        draw::draw_rectf(bar_x, bar_y, filled, 3);

        if filled > 10 {
            let glint_width = 26.min(filled);
            let glint_offset = ((filled - glint_width) as f32 * shimmer) as i32;
            draw::set_draw_color(Color::from_rgb(140, 176, 228));
            draw::draw_rectf(bar_x + glint_offset, bar_y, glint_width, 3);
        }
    });
}

fn apply_window_shape(window: &mut Window) {
    if let Some(shape) = create_window_shape(window.w(), window.h(), 26) {
        window.set_shape(Some(shape));
    }
}

fn create_window_shape(width: i32, height: i32, radius: i32) -> Option<RgbImage> {
    if width <= 0 || height <= 0 {
        return None;
    }

    let surface = ImageSurface::new(width, height, false);
    ImageSurface::push_current(&surface);

    draw::set_draw_color(Color::Black);
    draw::draw_rectf(0, 0, width, height);

    draw::set_draw_color(Color::White);
    draw::draw_rounded_rectf(0, 0, width, height, radius.max(0));

    let image = surface.image();
    ImageSurface::pop_current();
    image
}

fn draw_brand_mark(x: i32, y: i32, size: i32) {
    draw::set_draw_color(Color::from_rgb(92, 136, 214));
    draw::set_line_style(draw::LineStyle::Solid, 2);
    draw::draw_arc(x, y, size, size, 18.0, 336.0);
    draw::set_draw_color(Color::from_rgb(44, 74, 128));
    draw::draw_arc(x + 9, y + 9, size - 18, size - 18, 198.0, 18.0);
    draw::set_draw_color(Color::from_rgb(205, 224, 255));
    draw::draw_circle_fill(x + size - 13, y + 16, 8, Color::from_rgb(205, 224, 255));
    draw::set_draw_color(Color::from_rgb(28, 40, 72));
    draw::draw_circle_fill(
        x + size / 2,
        y + size / 2,
        size / 2 - 12,
        Color::from_rgb(12, 18, 32),
    );
    draw::set_draw_color(Color::from_rgb(114, 150, 214));
    draw::set_font(Font::HelveticaBold, 16);
    draw::draw_text2(
        "SQ",
        x + 2,
        y + 13,
        size,
        size,
        Align::Center | Align::Inside,
    );
    draw::set_line_style(draw::LineStyle::Solid, 0);
}

fn resolve_window_size(default_size: (i32, i32)) -> (i32, i32) {
    let (screen_width, screen_height) = app::screen_size();
    let max_width = (screen_width as i32).saturating_sub(96);
    let max_height = (screen_height as i32).saturating_sub(96);
    let width = default_size.0.min(max_width).max(720);
    let height = default_size.1.min(max_height).max(440);
    (width, height)
}

#[cfg(feature = "gpu-splash")]
fn start_animation_timer(
    animation_state: &Arc<Mutex<AnimationState>>,
    running: &Arc<AtomicBool>,
    overlay_panel: &mut Frame,
    gpu_background: Option<GlWindow>,
) {
    let animation_state = animation_state.clone();
    let running = running.clone();
    let mut overlay_panel = overlay_panel.clone();
    #[cfg(feature = "gpu-splash")]
    let mut gpu_background = gpu_background;

    app::add_timeout3(FRAME_INTERVAL_SECONDS, move |handle| {
        if !running.load(Ordering::Relaxed) {
            return;
        }

        {
            let lock_result = animation_state.lock();
            match lock_result {
                Ok(mut guard) => guard.tick(),
                Err(poisoned) => {
                    let mut guard = poisoned.into_inner();
                    guard.tick();
                }
            }
        }

        if !overlay_panel.was_deleted() {
            overlay_panel.redraw();
        }

        #[cfg(feature = "gpu-splash")]
        {
            if let Some(background) = gpu_background.as_mut() {
                if !background.was_deleted() {
                    background.redraw();
                }
            }
        }

        app::repeat_timeout3(FRAME_INTERVAL_SECONDS, handle);
    });
}

#[cfg(not(feature = "gpu-splash"))]
fn start_animation_timer(
    animation_state: &Arc<Mutex<AnimationState>>,
    running: &Arc<AtomicBool>,
    overlay_panel: &mut Frame,
) {
    let animation_state = animation_state.clone();
    let running = running.clone();
    let mut overlay_panel = overlay_panel.clone();

    app::add_timeout3(FRAME_INTERVAL_SECONDS, move |handle| {
        if !running.load(Ordering::Relaxed) {
            return;
        }

        {
            let lock_result = animation_state.lock();
            match lock_result {
                Ok(mut guard) => guard.tick(),
                Err(poisoned) => {
                    let mut guard = poisoned.into_inner();
                    guard.tick();
                }
            }
        }

        if !overlay_panel.was_deleted() {
            overlay_panel.redraw();
        }

        app::repeat_timeout3(FRAME_INTERVAL_SECONDS, handle);
    });
}

fn fade_out_and_destroy(window: &mut Window) {
    let started_at = Instant::now();
    while started_at.elapsed() < FADE_OUT_DURATION {
        let fade = 1.0
            - (started_at.elapsed().as_secs_f64() / FADE_OUT_DURATION.as_secs_f64())
                .clamp(0.0, 1.0);
        if !window.was_deleted() {
            window.set_opacity(fade);
            window.redraw();
        }
        let _ = app::wait_for(1.0 / 120.0);
    }

    if !window.was_deleted() {
        window.hide();
    }
    let _ = app::wait_for(0.01);
    if !window.was_deleted() {
        Window::delete(window.clone());
    }
}

fn destroy_window(window: &mut Window) {
    if !window.was_deleted() {
        window.hide();
    }
    let _ = app::wait_for(0.01);
    if !window.was_deleted() {
        Window::delete(window.clone());
    }
}

#[cfg(feature = "gpu-splash")]
fn install_gpu_draw(
    gl_window: &mut GlWindow,
    app_name: &'static str,
    loading_state: &Arc<Mutex<LoadingState>>,
    animation_state: &Arc<Mutex<AnimationState>>,
    event_sender: &app::Sender<SplashEvent>,
) {
    let loading_state = loading_state.clone();
    let animation_state = animation_state.clone();
    let event_sender = *event_sender;
    let renderer_slot = Arc::new(Mutex::new(RendererSlot::Uninitialized));
    let renderer_slot_for_draw = renderer_slot.clone();

    gl_window.draw(move |window| {
        window.make_current();

        let mut slot = renderer_slot_for_draw
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if matches!(*slot, RendererSlot::Uninitialized) {
            let init_result =
                unsafe { GpuRenderer::new(|name| window.get_proc_address(name), app_name) };
            match init_result {
                Ok(renderer) => {
                    *slot = RendererSlot::Ready(renderer);
                }
                Err(error) => {
                    *slot = RendererSlot::Failed;
                    event_sender.send(SplashEvent::GpuUnavailable(error));
                    return;
                }
            }
        }

        let progress = match loading_state.lock() {
            Ok(guard) => guard.display_progress(),
            Err(poisoned) => poisoned.into_inner().display_progress(),
        };

        let animation_guard = animation_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if let RendererSlot::Ready(renderer) = &mut *slot {
            unsafe {
                renderer.render(
                    &animation_guard,
                    progress,
                    window.pixel_w(),
                    window.pixel_h(),
                );
            }
        }
    });
}
