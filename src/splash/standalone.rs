#[cfg(feature = "gpu-splash")]
use crate::splash::animation::AnimationState;
#[cfg(feature = "gpu-splash")]
use crate::splash::loading::LoadingState;
#[cfg(feature = "gpu-splash")]
use crate::splash::overlay::{OverlayComposer, PANEL_HEIGHT, PANEL_WIDTH};
#[cfg(feature = "gpu-splash")]
use crate::splash::renderer::GpuRenderer;
#[cfg(feature = "gpu-splash")]
use crate::splash::SplashOptions;

#[cfg(feature = "gpu-splash")]
use glfw::{
    Action, Context, GlfwReceiver, MouseButton, OpenGlProfileHint, PWindow, WindowEvent,
    WindowHint, WindowMode,
};

#[cfg(feature = "gpu-splash")]
use std::ffi::c_void;

#[cfg(feature = "gpu-splash")]
pub struct StandaloneSplash {
    glfw: glfw::Glfw,
    window: PWindow,
    events: GlfwReceiver<(f64, WindowEvent)>,
    renderer: GpuRenderer,
    overlay: OverlayComposer,
}

#[cfg(feature = "gpu-splash")]
impl StandaloneSplash {
    pub fn new(options: &SplashOptions) -> Result<Self, String> {
        let mut glfw = glfw::init(glfw::fail_on_errors)
            .map_err(|error| format!("GLFW init failed: {error:?}"))?;

        glfw.window_hint(WindowHint::ContextVersion(3, 3));
        glfw.window_hint(WindowHint::OpenGlProfile(OpenGlProfileHint::Core));
        glfw.window_hint(WindowHint::Decorated(false));
        glfw.window_hint(WindowHint::Resizable(false));
        glfw.window_hint(WindowHint::Focused(true));
        #[cfg(target_os = "macos")]
        glfw.window_hint(WindowHint::OpenGlForwardCompat(true));

        let (window_width, window_height) = resolve_window_size(options.default_size);
        let (mut window, events) = glfw
            .create_window(
                window_width.max(1) as u32,
                window_height.max(1) as u32,
                options.app_name,
                WindowMode::Windowed,
            )
            .ok_or_else(|| "GLFW could not create the splash window.".to_string())?;

        center_window(&mut glfw, &mut window, window_width, window_height);

        window.set_key_polling(true);
        window.set_mouse_button_polling(true);
        window.set_close_polling(true);
        window.make_current();
        glfw.set_swap_interval(glfw::SwapInterval::Sync(1));

        let renderer = unsafe {
            GpuRenderer::new(|symbol| match window.get_proc_address(symbol) {
                Some(function) => function as *const c_void,
                None => std::ptr::null(),
            })
        }?;

        Ok(Self {
            glfw,
            window,
            events,
            renderer,
            overlay: OverlayComposer::new(options.app_name, options.subtitle),
        })
    }

    pub fn pump(
        &mut self,
        loading_state: &LoadingState,
        animation_state: &mut AnimationState,
    ) -> Result<bool, String> {
        let mut dismiss_requested = false;

        self.glfw.poll_events();
        for (_, event) in glfw::flush_messages(&self.events) {
            match event {
                WindowEvent::Close => {
                    self.window.set_should_close(false);
                    dismiss_requested = true;
                }
                WindowEvent::MouseButton(button, Action::Press, _) => {
                    if matches!(
                        button,
                        MouseButton::Button1 | MouseButton::Button2 | MouseButton::Button3
                    ) {
                        dismiss_requested = true;
                    }
                }
                WindowEvent::Key(_, _, Action::Press | Action::Repeat, _) => {
                    dismiss_requested = true;
                }
                _ => {}
            }
        }

        animation_state.tick();

        let (framebuffer_width, framebuffer_height) = self.window.get_framebuffer_size();
        let (window_width, window_height) = self.window.get_size();
        if framebuffer_width <= 0 || framebuffer_height <= 0 {
            return Ok(dismiss_requested);
        }

        let scale_x = framebuffer_width as f32 / window_width.max(1) as f32;
        let scale_y = framebuffer_height as f32 / window_height.max(1) as f32;
        let margin_x = (30.0 * scale_x).round() as i32;
        let margin_y = (30.0 * scale_y).round() as i32;
        let panel_width = ((PANEL_WIDTH as f32) * scale_x).round() as i32;
        let panel_height = ((PANEL_HEIGHT as f32) * scale_y).round() as i32;
        let panel_origin = (
            margin_x.max(0),
            framebuffer_height
                .saturating_sub(panel_height)
                .saturating_sub(margin_y)
                .max(0),
        );

        let overlay = self.overlay.compose(loading_state, animation_state);
        unsafe {
            self.renderer.render(
                animation_state,
                loading_state.display_progress(),
                framebuffer_width,
                framebuffer_height,
                panel_origin,
                (panel_width.max(1), panel_height.max(1)),
                overlay.pixels,
                overlay.width,
                overlay.height,
            )?;
        }

        self.window.swap_buffers();
        Ok(dismiss_requested)
    }
}

#[cfg(feature = "gpu-splash")]
fn resolve_window_size(default_size: (i32, i32)) -> (i32, i32) {
    (default_size.0.max(720), default_size.1.max(440))
}

#[cfg(feature = "gpu-splash")]
fn center_window(glfw: &mut glfw::Glfw, window: &mut PWindow, width: i32, height: i32) {
    let position = glfw.with_primary_monitor(|_, monitor| {
        monitor.and_then(|primary| {
            primary.get_video_mode().map(|mode| {
                let x = ((mode.width as i32) - width).saturating_div(2).max(0);
                let y = ((mode.height as i32) - height).saturating_div(2).max(0);
                (x, y)
            })
        })
    });

    if let Some((x, y)) = position {
        window.set_pos(x, y);
    }
}
