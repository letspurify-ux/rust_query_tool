use miniquad::*;
use std::time::Instant;

use super::shaders;

#[repr(C)]
struct Uniforms {
    u_time: f32,
    u_resolution: [f32; 2],
    u_alpha: f32,
}

#[repr(C)]
struct Vertex {
    position: [f32; 2],
}

pub struct SplashStage {
    ctx: Box<dyn RenderingBackend>,
    pipeline: Option<Pipeline>,
    bindings: Option<Bindings>,
    start_time: Instant,
    fade_out_start: Option<f32>,
}

impl SplashStage {
    pub fn new() -> Self {
        let mut ctx = window::new_rendering_backend();

        // Fullscreen quad: two triangles covering [-1, 1] in both axes
        let vertices: [Vertex; 4] = [
            Vertex { position: [-1.0, -1.0] },
            Vertex { position: [ 1.0, -1.0] },
            Vertex { position: [ 1.0,  1.0] },
            Vertex { position: [-1.0,  1.0] },
        ];
        let indices: [u16; 6] = [0, 1, 2, 0, 2, 3];

        let vertex_buffer = ctx.new_buffer(
            BufferType::VertexBuffer,
            BufferUsage::Immutable,
            BufferSource::slice(&vertices),
        );
        let index_buffer = ctx.new_buffer(
            BufferType::IndexBuffer,
            BufferUsage::Immutable,
            BufferSource::slice(&indices),
        );

        let bindings = Bindings {
            vertex_buffers: vec![vertex_buffer],
            index_buffer,
            images: vec![],
        };

        let shader = match ctx.new_shader(
            ShaderSource::Glsl {
                vertex: shaders::VERTEX,
                fragment: shaders::FRAGMENT,
            },
            shaders::meta(),
        ) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Failed to create splash shader: {e}");
                return Self {
                    ctx,
                    pipeline: None,
                    bindings: None,
                    start_time: Instant::now(),
                    fade_out_start: Some(0.0),
                };
            }
        };

        let pipeline = ctx.new_pipeline(
            &[BufferLayout::default()],
            &[VertexAttribute::new("position", VertexFormat::Float2)],
            shader,
            PipelineParams::default(),
        );

        Self {
            ctx,
            pipeline: Some(pipeline),
            bindings: Some(bindings),
            start_time: Instant::now(),
            fade_out_start: None,
        }
    }

    fn elapsed(&self) -> f32 {
        self.start_time.elapsed().as_secs_f32()
    }

    fn compute_alpha(&self) -> f32 {
        let elapsed = self.elapsed();

        // Fade in over 0.8 seconds
        let fade_in = (elapsed / 0.8).min(1.0);

        // Fade out
        let fade_out = if let Some(fo_start) = self.fade_out_start {
            let fo_elapsed = elapsed - fo_start;
            (1.0 - fo_elapsed / 0.5).max(0.0)
        } else if elapsed > 9.5 {
            // Auto fade-out in last 0.5s
            ((10.0 - elapsed) / 0.5).max(0.0)
        } else {
            1.0
        };

        fade_in * fade_out
    }
}

impl EventHandler for SplashStage {
    fn update(&mut self) {
        let elapsed = self.elapsed();

        // Quit after 10 seconds
        if elapsed >= 10.0 {
            window::quit();
            return;
        }

        // Quit after fade-out completes
        if let Some(fo_start) = self.fade_out_start {
            if elapsed - fo_start >= 0.5 {
                window::quit();
            }
        }
    }

    fn draw(&mut self) {
        let (pipeline, bindings) = match (self.pipeline.as_ref(), self.bindings.as_ref()) {
            (Some(p), Some(b)) => (p, b),
            _ => {
                window::quit();
                return;
            }
        };

        let (width, height) = window::screen_size();
        let time = self.elapsed();
        let alpha = self.compute_alpha();

        self.ctx.begin_default_pass(PassAction::Clear {
            color: Some((0.0, 0.0, 0.0, 1.0)),
            depth: None,
            stencil: None,
        });

        self.ctx.apply_pipeline(pipeline);
        self.ctx.apply_bindings(bindings);

        let uniforms = Uniforms {
            u_time: time,
            u_resolution: [width, height],
            u_alpha: alpha,
        };
        self.ctx.apply_uniforms(UniformsSource::table(&uniforms));

        self.ctx.draw(0, 6, 1);
        self.ctx.end_render_pass();
        self.ctx.commit_frame();
    }

    fn mouse_button_down_event(&mut self, _button: MouseButton, _x: f32, _y: f32) {
        if self.fade_out_start.is_none() {
            self.fade_out_start = Some(self.elapsed());
        }
    }

    fn key_down_event(&mut self, keycode: KeyCode, _keymods: KeyMods, _repeat: bool) {
        if keycode == KeyCode::Escape && self.fade_out_start.is_none() {
            self.fade_out_start = Some(self.elapsed());
        }
    }
}
