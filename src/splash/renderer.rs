use miniquad::*;
use std::time::Instant;

use super::shaders;

const TITLE_TEXTURE_WIDTH: u16 = 640;
const TITLE_TEXTURE_HEIGHT: u16 = 128;
const TITLE_TEXT: &str = "SPACE QUERY";
const SUBTITLE_TEXT: &str = "BUILT WITH RUST";

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

fn glyph_rows(ch: char) -> [u8; 7] {
    match ch {
        'A' => [
            0b01110, 0b10001, 0b10001, 0b11111, 0b10001, 0b10001, 0b10001,
        ],
        'B' => [
            0b11110, 0b10001, 0b10001, 0b11110, 0b10001, 0b10001, 0b11110,
        ],
        'C' => [
            0b01110, 0b10001, 0b10000, 0b10000, 0b10000, 0b10001, 0b01110,
        ],
        'E' => [
            0b11111, 0b10000, 0b10000, 0b11110, 0b10000, 0b10000, 0b11111,
        ],
        'H' => [
            0b10001, 0b10001, 0b10001, 0b11111, 0b10001, 0b10001, 0b10001,
        ],
        'I' => [
            0b11111, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100, 0b11111,
        ],
        'L' => [
            0b10000, 0b10000, 0b10000, 0b10000, 0b10000, 0b10000, 0b11111,
        ],
        'P' => [
            0b11110, 0b10001, 0b10001, 0b11110, 0b10000, 0b10000, 0b10000,
        ],
        'Q' => [
            0b01110, 0b10001, 0b10001, 0b10001, 0b10101, 0b10010, 0b01101,
        ],
        'R' => [
            0b11110, 0b10001, 0b10001, 0b11110, 0b10100, 0b10010, 0b10001,
        ],
        'S' => [
            0b01111, 0b10000, 0b10000, 0b01110, 0b00001, 0b00001, 0b11110,
        ],
        'T' => [
            0b11111, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100,
        ],
        'U' => [
            0b10001, 0b10001, 0b10001, 0b10001, 0b10001, 0b10001, 0b01110,
        ],
        'W' => [
            0b10001, 0b10001, 0b10001, 0b10101, 0b10101, 0b10101, 0b01010,
        ],
        'Y' => [
            0b10001, 0b10001, 0b01010, 0b00100, 0b00100, 0b00100, 0b00100,
        ],
        ' ' => [0; 7],
        _ => [0; 7],
    }
}

fn blend_pixel(pixels: &mut [u8], width: usize, height: usize, x: usize, y: usize, alpha: u8) {
    if x >= width || y >= height {
        return;
    }

    let idx = (y * width + x) * 4;
    let current = pixels.get(idx + 3).copied().unwrap_or(0);
    let mixed = current.max(alpha);

    if let Some(channel) = pixels.get_mut(idx) {
        *channel = 255;
    }
    if let Some(channel) = pixels.get_mut(idx + 1) {
        *channel = 255;
    }
    if let Some(channel) = pixels.get_mut(idx + 2) {
        *channel = 255;
    }
    if let Some(channel) = pixels.get_mut(idx + 3) {
        *channel = mixed;
    }
}

fn blur_alpha(mask: &[u8], width: usize, height: usize) -> Vec<u8> {
    let mut blurred = vec![0u8; width * height];

    for y in 0..height {
        for x in 0..width {
            let y_start = y.saturating_sub(2);
            let y_end = (y + 2).min(height.saturating_sub(1));
            let x_start = x.saturating_sub(2);
            let x_end = (x + 2).min(width.saturating_sub(1));

            let mut sum = 0u32;
            let mut count = 0u32;

            for ny in y_start..=y_end {
                for nx in x_start..=x_end {
                    let idx = ny * width + nx;
                    sum = sum.saturating_add(mask.get(idx).copied().unwrap_or(0) as u32);
                    count = count.saturating_add(1);
                }
            }

            let idx = y * width + x;
            let avg = if count == 0 { 0 } else { (sum / count) as u8 };
            blurred[idx] = avg;
        }
    }

    blurred
}

fn text_columns(text: &str) -> usize {
    text.chars().count().saturating_mul(6).saturating_sub(1)
}

fn draw_text_mask(
    mask: &mut [u8],
    width: usize,
    height: usize,
    text: &str,
    scale: usize,
    origin_x: usize,
    origin_y: usize,
) {
    let letter_advance = 6usize.saturating_mul(scale);

    for (glyph_index, ch) in text.chars().enumerate() {
        let rows = glyph_rows(ch);
        let glyph_origin_x = origin_x.saturating_add(glyph_index.saturating_mul(letter_advance));

        for (row_index, row_bits) in rows.iter().enumerate() {
            for col_index in 0..5usize {
                let mask_bit = 1u8 << (4usize.saturating_sub(col_index));
                if row_bits & mask_bit == 0 {
                    continue;
                }

                let x = glyph_origin_x.saturating_add(col_index.saturating_mul(scale));
                let y = origin_y.saturating_add(row_index.saturating_mul(scale));
                let max_y = y.saturating_add(scale).min(height);
                let max_x = x.saturating_add(scale).min(width);

                for py in y..max_y {
                    for px in x..max_x {
                        let idx = py * width + px;
                        if let Some(cell) = mask.get_mut(idx) {
                            *cell = 255;
                        }
                    }
                }
            }
        }
    }
}

fn build_title_texture() -> Vec<u8> {
    let width = usize::from(TITLE_TEXTURE_WIDTH);
    let height = usize::from(TITLE_TEXTURE_HEIGHT);
    let mut mask = vec![0u8; width * height];

    let title_columns = text_columns(TITLE_TEXT);
    let title_usable_width = width.saturating_mul(9) / 10;
    let title_usable_height = height.saturating_mul(13) / 32;
    let title_scale_x = title_usable_width / title_columns.max(1);
    let title_scale_y = title_usable_height / 7usize;
    let title_scale = title_scale_x.min(title_scale_y).max(1);
    let title_width = title_columns.saturating_mul(title_scale);
    let title_height = 7usize.saturating_mul(title_scale);
    let title_origin_x = width.saturating_sub(title_width) / 2;
    let title_origin_y = height.saturating_mul(7) / 32;
    draw_text_mask(
        &mut mask,
        width,
        height,
        TITLE_TEXT,
        title_scale,
        title_origin_x,
        title_origin_y,
    );

    let subtitle_columns = text_columns(SUBTITLE_TEXT);
    let subtitle_usable_width = width.saturating_mul(7) / 10;
    let subtitle_usable_height = height.saturating_mul(5) / 32;
    let subtitle_scale_x = subtitle_usable_width / subtitle_columns.max(1);
    let subtitle_scale_y = subtitle_usable_height / 7usize;
    let subtitle_scale = subtitle_scale_x.min(subtitle_scale_y).max(1);
    let subtitle_width = subtitle_columns.saturating_mul(subtitle_scale);
    let title_right = title_origin_x.saturating_add(title_width);
    let subtitle_origin_x = title_right.saturating_sub(subtitle_width);
    let subtitle_gap = height.saturating_mul(3) / 32;
    let subtitle_origin_y = title_origin_y
        .saturating_add(title_height)
        .saturating_add(subtitle_gap);
    draw_text_mask(
        &mut mask,
        width,
        height,
        SUBTITLE_TEXT,
        subtitle_scale,
        subtitle_origin_x,
        subtitle_origin_y,
    );

    let glow = blur_alpha(&mask, width, height);
    let mut pixels = vec![0u8; width * height * 4];

    for y in 0..height {
        for x in 0..width {
            let idx = y * width + x;
            let base = mask.get(idx).copied().unwrap_or(0);
            let glow_alpha = ((glow.get(idx).copied().unwrap_or(0) as f32) * 0.42) as u8;
            let alpha = base.max(glow_alpha);
            blend_pixel(&mut pixels, width, height, x, y, alpha);
        }
    }

    pixels
}

pub struct SplashStage {
    // Keep a concrete GL backend instead of a dyn RenderingBackend fat pointer.
    // The crash report points at the first backend virtual dispatch in draw(),
    // so removing that indirection avoids the invalid vtable load on macOS.
    ctx: GlContext,
    pipeline: Option<Pipeline>,
    bindings: Option<Bindings>,
    start_time: Instant,
    fade_out_start: Option<f32>,
    init_failed: bool,
}

impl SplashStage {
    pub fn new() -> Self {
        let mut ctx = GlContext::new();
        let title_texture = ctx.new_texture_from_rgba8(
            TITLE_TEXTURE_WIDTH,
            TITLE_TEXTURE_HEIGHT,
            &build_title_texture(),
        );

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
            images: vec![title_texture],
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
                    init_failed: true,
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
            init_failed: false,
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
        if self.init_failed {
            window::quit();
            return;
        }

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
            _ => return,
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
