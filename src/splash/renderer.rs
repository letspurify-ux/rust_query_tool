#[cfg(feature = "gpu-splash")]
use crate::splash::animation::AnimationState;

#[cfg(feature = "gpu-splash")]
use fltk::{
    draw,
    enums::{Align, Color, Font},
    prelude::{ImageExt, SurfaceDevice},
    surface::ImageSurface,
};

#[cfg(feature = "gpu-splash")]
use glow::HasContext;

#[cfg(feature = "gpu-splash")]
pub struct GpuRenderer {
    gl: glow::Context,
    program: glow::Program,
    vao: glow::VertexArray,
    resolution_uniform: Option<glow::UniformLocation>,
    time_uniform: Option<glow::UniformLocation>,
    camera_uniform: Option<glow::UniformLocation>,
    progress_uniform: Option<glow::UniformLocation>,
    title_sampler_uniform: Option<glow::UniformLocation>,
    title_origin_uniform: Option<glow::UniformLocation>,
    title_size_uniform: Option<glow::UniformLocation>,
    title_texture: Option<TitleTexture>,
    title_texture_failed: bool,
    title_text: &'static str,
}

#[cfg(feature = "gpu-splash")]
struct TitleTexture {
    texture: glow::Texture,
    width: i32,
    height: i32,
    screen_width: i32,
    screen_height: i32,
}

#[cfg(feature = "gpu-splash")]
struct TitleMaskData {
    pixels: Vec<u8>,
    width: i32,
    height: i32,
}

#[cfg(feature = "gpu-splash")]
impl GpuRenderer {
    /// Build a tiny shader-driven renderer that draws the entire background in a
    /// single full-screen pass. This keeps CPU cost low while still producing a
    /// richer scene than manual 2D painting.
    pub unsafe fn new<F>(loader: F, title_text: &'static str) -> Result<Self, String>
    where
        F: FnMut(&str) -> *const std::os::raw::c_void,
    {
        let gl = glow::Context::from_loader_function(loader);
        let program = compile_program(&gl)?;
        let vao = gl
            .create_vertex_array()
            .map_err(|err| format!("Failed to create splash VAO: {err}"))?;

        gl.bind_vertex_array(Some(vao));
        gl.bind_vertex_array(None);

        Ok(Self {
            resolution_uniform: gl.get_uniform_location(program, "u_resolution"),
            time_uniform: gl.get_uniform_location(program, "u_time"),
            camera_uniform: gl.get_uniform_location(program, "u_camera"),
            progress_uniform: gl.get_uniform_location(program, "u_progress"),
            title_sampler_uniform: gl.get_uniform_location(program, "u_title_tex"),
            title_origin_uniform: gl.get_uniform_location(program, "u_title_origin"),
            title_size_uniform: gl.get_uniform_location(program, "u_title_size"),
            title_texture: None,
            title_texture_failed: false,
            title_text,
            gl,
            program,
            vao,
        })
    }

    pub unsafe fn render(
        &mut self,
        animation: &AnimationState,
        progress: f32,
        width: i32,
        height: i32,
    ) {
        if let Err(error) = self.ensure_title_texture(width, height) {
            if !self.title_texture_failed {
                crate::utils::logging::log_warning(
                    "splash",
                    &format!("Unable to build splash title texture: {error}"),
                );
                self.title_texture_failed = true;
            }
        }

        self.gl.viewport(0, 0, width.max(1), height.max(1));
        self.gl.disable(glow::DEPTH_TEST);
        self.gl.disable(glow::BLEND);
        self.gl.clear_color(0.01, 0.01, 0.02, 1.0);
        self.gl.clear(glow::COLOR_BUFFER_BIT);
        self.gl.use_program(Some(self.program));
        self.gl.bind_vertex_array(Some(self.vao));

        if let Some(uniform) = &self.resolution_uniform {
            self.gl
                .uniform_2_f32(Some(uniform), width as f32, height as f32);
        }
        if let Some(uniform) = &self.time_uniform {
            self.gl
                .uniform_1_f32(Some(uniform), animation.elapsed_seconds());
        }
        if let Some(uniform) = &self.camera_uniform {
            let [x, y] = animation.camera_offset();
            self.gl.uniform_2_f32(Some(uniform), x, y);
        }
        if let Some(uniform) = &self.progress_uniform {
            self.gl
                .uniform_1_f32(Some(uniform), progress.clamp(0.0, 1.0));
        }
        if let Some(uniform) = &self.title_sampler_uniform {
            self.gl.uniform_1_i32(Some(uniform), 0);
        }

        let mut title_origin = (0.0_f32, 0.0_f32);
        let mut title_size = (0.0_f32, 0.0_f32);
        self.gl.active_texture(glow::TEXTURE0);
        if let Some(title_texture) = &self.title_texture {
            self.gl
                .bind_texture(glow::TEXTURE_2D, Some(title_texture.texture));
            title_size = (
                title_texture.width as f32 / width.max(1) as f32,
                title_texture.height as f32 / height.max(1) as f32,
            );
            title_origin = (
                (1.0 - title_size.0) * 0.5,
                ((1.0 - title_size.1) * 0.5 + 0.10).min(0.85 - title_size.1),
            );
        } else {
            self.gl.bind_texture(glow::TEXTURE_2D, None);
        }
        if let Some(uniform) = &self.title_origin_uniform {
            self.gl
                .uniform_2_f32(Some(uniform), title_origin.0, title_origin.1);
        }
        if let Some(uniform) = &self.title_size_uniform {
            self.gl
                .uniform_2_f32(Some(uniform), title_size.0, title_size.1);
        }

        self.gl.draw_arrays(glow::TRIANGLES, 0, 3);
        self.gl.bind_texture(glow::TEXTURE_2D, None);
        self.gl.bind_vertex_array(None);
        self.gl.use_program(None);
    }

    unsafe fn ensure_title_texture(&mut self, width: i32, height: i32) -> Result<(), String> {
        let width = width.max(1);
        let height = height.max(1);

        if let Some(title_texture) = &self.title_texture {
            if title_texture.screen_width == width && title_texture.screen_height == height {
                return Ok(());
            }
        }

        let title_mask = create_title_mask(self.title_text, width, height)?;
        let texture = self
            .gl
            .create_texture()
            .map_err(|err| format!("Failed to create splash title texture: {err}"))?;

        self.gl.active_texture(glow::TEXTURE0);
        self.gl.bind_texture(glow::TEXTURE_2D, Some(texture));
        self.gl.pixel_store_i32(glow::UNPACK_ALIGNMENT, 1);
        self.gl.tex_parameter_i32(
            glow::TEXTURE_2D,
            glow::TEXTURE_MIN_FILTER,
            glow::LINEAR as i32,
        );
        self.gl.tex_parameter_i32(
            glow::TEXTURE_2D,
            glow::TEXTURE_MAG_FILTER,
            glow::LINEAR as i32,
        );
        self.gl.tex_parameter_i32(
            glow::TEXTURE_2D,
            glow::TEXTURE_WRAP_S,
            glow::CLAMP_TO_EDGE as i32,
        );
        self.gl.tex_parameter_i32(
            glow::TEXTURE_2D,
            glow::TEXTURE_WRAP_T,
            glow::CLAMP_TO_EDGE as i32,
        );
        self.gl.tex_image_2d(
            glow::TEXTURE_2D,
            0,
            glow::R8 as i32,
            title_mask.width,
            title_mask.height,
            0,
            glow::RED,
            glow::UNSIGNED_BYTE,
            glow::PixelUnpackData::Slice(Some(title_mask.pixels.as_slice())),
        );
        self.gl.bind_texture(glow::TEXTURE_2D, None);

        if let Some(previous_texture) = self.title_texture.take() {
            self.gl.delete_texture(previous_texture.texture);
        }

        self.title_texture = Some(TitleTexture {
            texture,
            width: title_mask.width,
            height: title_mask.height,
            screen_width: width,
            screen_height: height,
        });
        self.title_texture_failed = false;

        Ok(())
    }
}

#[cfg(feature = "gpu-splash")]
impl Drop for GpuRenderer {
    fn drop(&mut self) {
        unsafe {
            if let Some(title_texture) = self.title_texture.take() {
                self.gl.delete_texture(title_texture.texture);
            }
            self.gl.delete_program(self.program);
            self.gl.delete_vertex_array(self.vao);
        }
    }
}

#[cfg(feature = "gpu-splash")]
fn create_title_mask(
    title_text: &str,
    screen_width: i32,
    screen_height: i32,
) -> Result<TitleMaskData, String> {
    let subtitle_text = "Built with Rust";
    // Reserve extra texture margin so the GPU halo can expand beyond the glyph edges
    // without being clipped by the title texture bounds.
    let target_width = ((screen_width as f32) * 0.42).round() as i32;
    let mut font_size = ((screen_height as f32) * 0.17).round() as i32;
    font_size = font_size.clamp(44, 92);

    loop {
        draw::set_font(Font::HelveticaBold, font_size);
        let (measured_width, _) = draw::measure(title_text, false);
        if measured_width <= target_width || font_size <= 36 {
            break;
        }
        font_size -= 1;
    }

    draw::set_font(Font::HelveticaBold, font_size);
    let (measured_width, measured_height) = draw::measure(title_text, false);
    let subtitle_font_size = ((font_size as f32) * 0.24).round() as i32;
    let subtitle_font_size = subtitle_font_size.clamp(12, 22);
    draw::set_font(Font::Helvetica, subtitle_font_size);
    let (subtitle_width, subtitle_height) = draw::measure(subtitle_text, false);
    let line_gap = ((font_size as f32) * 0.18).round() as i32;
    let padding_x = (font_size as f32 * 1.20).round() as i32;
    let padding_y = (font_size as f32 * 0.88).round() as i32;
    let content_width = measured_width.max(subtitle_width);
    let texture_width = (content_width + padding_x * 2).max(64);
    let texture_height = (measured_height + line_gap + subtitle_height + padding_y * 2).max(64);

    let surface = ImageSurface::new(texture_width, texture_height, false);
    ImageSurface::push_current(&surface);
    draw::set_draw_color(Color::Black);
    draw::draw_rectf(0, 0, texture_width, texture_height);

    draw::set_font(Font::HelveticaBold, font_size);
    draw::set_draw_color(Color::White);
    draw::draw_text2(
        title_text,
        padding_x,
        padding_y - ((font_size as f32) * 0.08).round() as i32,
        texture_width - padding_x * 2,
        measured_height + font_size / 4,
        Align::Center | Align::Inside,
    );

    draw::set_font(Font::Helvetica, subtitle_font_size);
    draw::set_draw_color(Color::from_rgb(170, 176, 188));
    draw::draw_text2(
        subtitle_text,
        padding_x,
        padding_y + measured_height + line_gap,
        texture_width - padding_x * 2,
        subtitle_height + subtitle_font_size / 2,
        Align::Right | Align::Inside,
    );
    ImageSurface::pop_current();

    let image = surface
        .image()
        .ok_or_else(|| "Image surface did not produce a title image".to_string())?;
    let data = image.to_rgb_data();
    let width = image.data_w();
    let height = image.data_h();
    let pixel_count = width.max(0).saturating_mul(height.max(0)) as usize;
    let mut pixels = Vec::with_capacity(pixel_count);

    for chunk in data.chunks_exact(3) {
        let alpha = chunk[0].max(chunk[1]).max(chunk[2]);
        pixels.push(alpha);
    }

    Ok(TitleMaskData {
        pixels,
        width,
        height,
    })
}

#[cfg(feature = "gpu-splash")]
unsafe fn compile_program(gl: &glow::Context) -> Result<glow::Program, String> {
    let variants = [
        ShaderVariant {
            label: "410 core",
            directive: "#version 410 core",
        },
        ShaderVariant {
            label: "330 core",
            directive: "#version 330 core",
        },
        ShaderVariant {
            label: "150",
            directive: "#version 150",
        },
    ];
    let version_string = gl.get_parameter_string(glow::VERSION);
    let shading_language = gl.get_parameter_string(glow::SHADING_LANGUAGE_VERSION);
    let mut errors: Vec<String> = Vec::new();

    for variant in variants {
        let vertex_source = vertex_shader_source(variant.directive);
        let fragment_source = fragment_shader_source(variant.directive);

        match build_program(gl, &vertex_source, &fragment_source) {
            Ok(program) => return Ok(program),
            Err(err) => {
                errors.push(format!("GLSL {} compile failed: {err}", variant.label));
            }
        }
    }

    Err(format!(
        "Unable to initialize splash renderer. OpenGL version: {version_string}. Shading language: {shading_language}. {}",
        errors.join(" | ")
    ))
}

#[cfg(feature = "gpu-splash")]
struct ShaderVariant {
    label: &'static str,
    directive: &'static str,
}

#[cfg(feature = "gpu-splash")]
unsafe fn build_program(
    gl: &glow::Context,
    vertex_source: &str,
    fragment_source: &str,
) -> Result<glow::Program, String> {
    let program = gl
        .create_program()
        .map_err(|err| format!("Failed to create GL program: {err}"))?;

    let vertex = gl
        .create_shader(glow::VERTEX_SHADER)
        .map_err(|err| format!("Failed to create vertex shader: {err}"))?;
    gl.shader_source(vertex, vertex_source);
    gl.compile_shader(vertex);
    if !gl.get_shader_compile_status(vertex) {
        let info = gl.get_shader_info_log(vertex);
        gl.delete_shader(vertex);
        gl.delete_program(program);
        return Err(info);
    }

    let fragment = gl
        .create_shader(glow::FRAGMENT_SHADER)
        .map_err(|err| format!("Failed to create fragment shader: {err}"))?;
    gl.shader_source(fragment, fragment_source);
    gl.compile_shader(fragment);
    if !gl.get_shader_compile_status(fragment) {
        let info = gl.get_shader_info_log(fragment);
        gl.delete_shader(vertex);
        gl.delete_shader(fragment);
        gl.delete_program(program);
        return Err(info);
    }

    gl.attach_shader(program, vertex);
    gl.attach_shader(program, fragment);
    gl.link_program(program);
    gl.detach_shader(program, vertex);
    gl.detach_shader(program, fragment);
    gl.delete_shader(vertex);
    gl.delete_shader(fragment);

    if !gl.get_program_link_status(program) {
        let info = gl.get_program_info_log(program);
        gl.delete_program(program);
        return Err(info);
    }

    Ok(program)
}

#[cfg(feature = "gpu-splash")]
fn vertex_shader_source(version_directive: &str) -> String {
    format!(
        r#"{version_directive}
out vec2 v_uv;

void main() {{
    vec2 positions[3] = vec2[](
        vec2(-1.0, -1.0),
        vec2(3.0, -1.0),
        vec2(-1.0, 3.0)
    );
    vec2 position = positions[gl_VertexID];
    v_uv = position * 0.5 + 0.5;
    gl_Position = vec4(position, 0.0, 1.0);
}}
"#
    )
}

#[cfg(feature = "gpu-splash")]
fn fragment_shader_source(version_directive: &str) -> String {
    format!(
        r#"{version_directive}
in vec2 v_uv;
out vec4 frag_color;

uniform vec2 u_resolution;
uniform float u_time;
uniform vec2 u_camera;
uniform float u_progress;
uniform sampler2D u_title_tex;
uniform vec2 u_title_origin;
uniform vec2 u_title_size;

float hash21(vec2 p) {{
    p = fract(p * vec2(123.34, 456.21));
    p += dot(p, p + 45.32);
    return fract(p.x * p.y);
}}

float noise(vec2 p) {{
    vec2 i = floor(p);
    vec2 f = fract(p);
    float a = hash21(i);
    float b = hash21(i + vec2(1.0, 0.0));
    float c = hash21(i + vec2(0.0, 1.0));
    float d = hash21(i + vec2(1.0, 1.0));
    vec2 u = f * f * (3.0 - 2.0 * f);
    return mix(a, b, u.x) + (c - a) * u.y * (1.0 - u.x) + (d - b) * u.x * u.y;
}}

float fbm(vec2 p) {{
    float value = 0.0;
    float amplitude = 0.55;
    mat2 rotation = mat2(1.6, 1.2, -1.2, 1.6);
    for (int i = 0; i < 5; i++) {{
        value += amplitude * noise(p);
        p = rotation * p;
        amplitude *= 0.52;
    }}
    return value;
}}

float star_layer(vec2 uv, float scale, float seed, float speed, float depth) {{
    vec2 p = uv * scale;
    p += vec2(u_time * speed, -u_time * speed * 0.35);
    p += u_camera * depth * scale;
    p += vec2(seed, seed * 1.37);

    vec2 cell = floor(p);
    vec2 local = fract(p) - 0.5;
    float selector = hash21(cell + seed);
    vec2 offset = vec2(
        hash21(cell + seed * 1.9),
        hash21(cell + seed * 3.1)
    ) - 0.5;
    float star_mask = step(0.9925, selector);
    float distance_to_star = length(local - offset * 0.65);
    float size = mix(0.02, 0.11, pow(selector, 16.0));
    float core = smoothstep(size, 0.0, distance_to_star);
    float halo = smoothstep(size * 3.8, 0.0, distance_to_star);
    float twinkle = 0.86 + 0.14 * sin(u_time * (0.7 + selector * 1.8) + selector * 22.0);
    return (core + halo * 0.33) * star_mask * twinkle;
}}

vec3 render_planet(vec2 uv, float aspect, out float alpha) {{
    vec2 center = vec2(0.77, 0.31) + u_camera * vec2(0.015, 0.010);
    vec2 p = vec2((uv.x - center.x) * aspect, uv.y - center.y);
    float radius = 0.235;
    float dist = length(p);
    float planet = smoothstep(radius, radius - 0.008, dist);

    if (planet <= 0.0) {{
        alpha = 0.0;
        return vec3(0.0);
    }}

    float z = sqrt(max(radius * radius - dist * dist, 0.0));
    vec3 normal = normalize(vec3(p, z));
    vec3 light_dir = normalize(vec3(-0.42, 0.18, 0.88));
    float diffuse = clamp(dot(normal, light_dir), 0.0, 1.0);
    float rim = pow(1.0 - max(normal.z, 0.0), 3.4);
    float bands = fbm(normal.xy * 4.8 + vec2(u_time * 0.035, -u_time * 0.018));

    vec3 deep = vec3(0.028, 0.062, 0.120);
    vec3 mid = vec3(0.082, 0.176, 0.320);
    vec3 base = mix(deep, mid, bands * 0.74 + 0.12);
    vec3 color = base * (0.24 + diffuse * 0.76);
    color += vec3(0.05, 0.10, 0.18) * rim * 0.55;

    float atmosphere = smoothstep(radius + 0.030, radius - 0.005, dist) - planet;
    color += vec3(0.11, 0.28, 0.54) * max(atmosphere, 0.0) * 0.85;

    alpha = clamp(planet + atmosphere, 0.0, 1.0);
    return color;
}}

vec3 render_planet_corona(vec2 uv, float aspect) {{
    vec2 center = vec2(0.77, 0.31) + u_camera * vec2(0.015, 0.010);
    vec2 p = vec2((uv.x - center.x) * aspect, uv.y - center.y);
    float radius = 0.235;
    float dist = length(p);
    float outer_band = smoothstep(radius + 0.135, radius + 0.010, dist);
    float inner_cut = smoothstep(radius + 0.018, radius - 0.004, dist);
    float corona_band = max(outer_band - inner_cut, 0.0);

    if (corona_band <= 0.0) {{
        return vec3(0.0);
    }}

    vec2 dir = normalize(p + vec2(0.0001, 0.0001));
    vec2 light_2d = normalize(vec2(-0.86, 0.34));
    float directional = pow(clamp(dot(dir, -light_2d) * 0.5 + 0.5, 0.0, 1.0), 1.9);
    float angle = atan(p.y, p.x);
    float flicker = 0.82 + 0.18 * sin(u_time * 1.35 + angle * 7.0 + dist * 24.0);
    float turbulence = fbm(vec2(angle * 2.8, u_time * 0.28 - dist * 9.5));
    float arc = 0.55 + 0.45 * sin(angle * 10.0 - u_time * 0.95 + turbulence * 2.0);
    float corona = corona_band * (0.55 + turbulence * 0.65) * flicker * (0.60 + arc * 0.40);
    vec3 corona_base = mix(vec3(0.10, 0.18, 0.40), vec3(0.56, 0.74, 1.00), directional);
    vec3 corona_hot = vec3(0.90, 0.95, 1.00) * pow(directional, 3.4);
    return corona_base * corona * 0.55 + corona_hot * corona * 0.22;
}}

float sd_segment(vec2 p, vec2 a, vec2 b) {{
    vec2 pa = p - a;
    vec2 ba = b - a;
    float h = clamp(dot(pa, ba) / max(dot(ba, ba), 0.0001), 0.0, 1.0);
    return length(pa - ba * h);
}}

float glyph_distance(int glyph, vec2 p) {{
    float d = 1000.0;
    float l = -0.42;
    float r = 0.42;
    float t = 0.60;
    float b = -0.60;
    float m = 0.0;

    if (glyph == 0) {{
        d = min(d, sd_segment(p, vec2(l, t), vec2(r, t)));
        d = min(d, sd_segment(p, vec2(l, m), vec2(r, m)));
        d = min(d, sd_segment(p, vec2(l, b), vec2(r, b)));
        d = min(d, sd_segment(p, vec2(l, m), vec2(l, t)));
        d = min(d, sd_segment(p, vec2(r, b), vec2(r, m)));
    }} else if (glyph == 1) {{
        d = min(d, sd_segment(p, vec2(l, b), vec2(l, t)));
        d = min(d, sd_segment(p, vec2(l, t), vec2(r, t)));
        d = min(d, sd_segment(p, vec2(l, m), vec2(r, m)));
        d = min(d, sd_segment(p, vec2(r, m), vec2(r, t)));
    }} else if (glyph == 2) {{
        d = min(d, sd_segment(p, vec2(l, b), vec2(0.0, t)));
        d = min(d, sd_segment(p, vec2(r, b), vec2(0.0, t)));
        d = min(d, sd_segment(p, vec2(l * 0.52, m), vec2(r * 0.52, m)));
    }} else if (glyph == 3) {{
        d = min(d, sd_segment(p, vec2(l, t), vec2(r * 0.82, t)));
        d = min(d, sd_segment(p, vec2(l, b), vec2(r * 0.82, b)));
        d = min(d, sd_segment(p, vec2(l, b + 0.06), vec2(l, t - 0.06)));
    }} else if (glyph == 4) {{
        d = min(d, sd_segment(p, vec2(l, b), vec2(l, t)));
        d = min(d, sd_segment(p, vec2(l, t), vec2(r, t)));
        d = min(d, sd_segment(p, vec2(l, m), vec2(r * 0.92, m)));
        d = min(d, sd_segment(p, vec2(l, b), vec2(r, b)));
    }} else if (glyph == 5) {{
        d = min(d, sd_segment(p, vec2(l, t), vec2(r, t)));
        d = min(d, sd_segment(p, vec2(r, t), vec2(r, b)));
        d = min(d, sd_segment(p, vec2(l, b), vec2(r, b)));
        d = min(d, sd_segment(p, vec2(l, t), vec2(l, b)));
        d = min(d, sd_segment(p, vec2(0.06, -0.02), vec2(r + 0.06, b - 0.12)));
    }} else if (glyph == 6) {{
        d = min(d, sd_segment(p, vec2(l, t), vec2(l, b)));
        d = min(d, sd_segment(p, vec2(r, t), vec2(r, b)));
        d = min(d, sd_segment(p, vec2(l, b), vec2(r, b)));
    }} else if (glyph == 7) {{
        d = min(d, sd_segment(p, vec2(l, b), vec2(l, t)));
        d = min(d, sd_segment(p, vec2(l, t), vec2(r, t)));
        d = min(d, sd_segment(p, vec2(l, m), vec2(r * 0.92, m)));
        d = min(d, sd_segment(p, vec2(r, m), vec2(r, t)));
        d = min(d, sd_segment(p, vec2(-0.02, -0.04), vec2(r, b)));
    }} else if (glyph == 8) {{
        d = min(d, sd_segment(p, vec2(l, t), vec2(0.0, 0.02)));
        d = min(d, sd_segment(p, vec2(r, t), vec2(0.0, 0.02)));
        d = min(d, sd_segment(p, vec2(0.0, 0.02), vec2(0.0, b)));
    }}

    return d;
}}

float title_distance(vec2 p) {{
    float d = 1000.0;
    d = min(d, glyph_distance(0, p - vec2(-5.95, 0.0)));
    d = min(d, glyph_distance(1, p - vec2(-4.58, 0.0)));
    d = min(d, glyph_distance(2, p - vec2(-3.21, 0.0)));
    d = min(d, glyph_distance(3, p - vec2(-1.84, 0.0)));
    d = min(d, glyph_distance(4, p - vec2(-0.47, 0.0)));
    d = min(d, glyph_distance(5, p - vec2(1.52, 0.0)));
    d = min(d, glyph_distance(6, p - vec2(2.89, 0.0)));
    d = min(d, glyph_distance(4, p - vec2(4.26, 0.0)));
    d = min(d, glyph_distance(7, p - vec2(5.63, 0.0)));
    d = min(d, glyph_distance(8, p - vec2(7.00, 0.0)));
    return d;
}}

void main() {{
    vec2 uv = v_uv;
    float aspect = u_resolution.x / max(u_resolution.y, 1.0);
    vec2 centered = uv - 0.5;
    centered.x *= aspect;

    vec3 color = mix(
        vec3(0.012, 0.016, 0.028),
        vec3(0.020, 0.040, 0.092),
        clamp(uv.y * 1.08 + 0.10, 0.0, 1.0)
    );

    float focal_glow = exp(-length(centered * vec2(1.0, 0.82)) * 1.65);
    color += vec3(0.010, 0.022, 0.056) * focal_glow * 0.58;

    float nebula_a = fbm(centered * 3.2 + u_camera * 0.45 + vec2(u_time * 0.014, -u_time * 0.010));
    float nebula_b = fbm(centered * 4.9 - u_camera * 0.30 + vec2(-u_time * 0.010, u_time * 0.012) + 12.7);
    float nebula_c = fbm(centered * 6.5 + vec2(5.2, -8.3));

    color += mix(vec3(0.02, 0.06, 0.14), vec3(0.10, 0.18, 0.38), nebula_a)
        * smoothstep(0.46, 0.86, nebula_a) * 0.24;
    color += mix(vec3(0.03, 0.04, 0.10), vec3(0.18, 0.09, 0.28), nebula_b)
        * smoothstep(0.54, 0.92, nebula_b) * 0.18;
    color += vec3(0.05, 0.07, 0.16) * smoothstep(0.62, 0.95, nebula_c) * 0.08;

    float stars =
        star_layer(uv, 38.0, 1.7, 0.004, 0.12) +
        star_layer(uv, 72.0, 8.3, 0.008, 0.25) +
        star_layer(uv, 126.0, 13.7, 0.012, 0.42);
    color += vec3(stars) * 0.92;

    float planet_alpha = 0.0;
    vec3 planet = render_planet(uv, aspect, planet_alpha);
    color = mix(color, planet, planet_alpha);
    color += render_planet_corona(uv, aspect);

    float accent_arc = smoothstep(0.0, 0.012, abs(uv.y - 0.86 + sin(uv.x * 9.0 + u_time * 0.25) * 0.006));
    color += vec3(0.00, 0.02, 0.05) * (1.0 - accent_arc) * (0.08 + u_progress * 0.04);

    if (u_title_size.x > 0.0 && u_title_size.y > 0.0) {{
        vec2 title_uv = (uv - u_title_origin) / u_title_size;
        if (title_uv.x >= 0.0 && title_uv.x <= 1.0 && title_uv.y >= 0.0 && title_uv.y <= 1.0) {{
            vec2 sample_uv = vec2(title_uv.x, 1.0 - title_uv.y);
            float title_mask = texture(u_title_tex, sample_uv).r;
            float title_shadow = texture(u_title_tex, clamp(sample_uv + vec2(-0.004, 0.010), 0.0, 1.0)).r;
            vec2 title_texel = vec2(
                1.0 / max(u_title_size.x * u_resolution.x, 1.0),
                1.0 / max(u_title_size.y * u_resolution.y, 1.0)
            );
            float inner_halo = 0.0;
            inner_halo += texture(u_title_tex, clamp(sample_uv + title_texel * vec2(3.0, 0.0), 0.0, 1.0)).r;
            inner_halo += texture(u_title_tex, clamp(sample_uv + title_texel * vec2(-3.0, 0.0), 0.0, 1.0)).r;
            inner_halo += texture(u_title_tex, clamp(sample_uv + title_texel * vec2(0.0, 3.0), 0.0, 1.0)).r;
            inner_halo += texture(u_title_tex, clamp(sample_uv + title_texel * vec2(0.0, -3.0), 0.0, 1.0)).r;
            inner_halo *= 0.25;

            float outer_halo = 0.0;
            outer_halo += texture(u_title_tex, clamp(sample_uv + title_texel * vec2(10.0, 0.0), 0.0, 1.0)).r;
            outer_halo += texture(u_title_tex, clamp(sample_uv + title_texel * vec2(-10.0, 0.0), 0.0, 1.0)).r;
            outer_halo += texture(u_title_tex, clamp(sample_uv + title_texel * vec2(0.0, 10.0), 0.0, 1.0)).r;
            outer_halo += texture(u_title_tex, clamp(sample_uv + title_texel * vec2(0.0, -10.0), 0.0, 1.0)).r;
            outer_halo += texture(u_title_tex, clamp(sample_uv + title_texel * vec2(8.0, 8.0), 0.0, 1.0)).r;
            outer_halo += texture(u_title_tex, clamp(sample_uv + title_texel * vec2(-8.0, 8.0), 0.0, 1.0)).r;
            outer_halo += texture(u_title_tex, clamp(sample_uv + title_texel * vec2(8.0, -8.0), 0.0, 1.0)).r;
            outer_halo += texture(u_title_tex, clamp(sample_uv + title_texel * vec2(-8.0, -8.0), 0.0, 1.0)).r;
            outer_halo += texture(u_title_tex, clamp(sample_uv + title_texel * vec2(16.0, 0.0), 0.0, 1.0)).r;
            outer_halo += texture(u_title_tex, clamp(sample_uv + title_texel * vec2(-16.0, 0.0), 0.0, 1.0)).r;
            outer_halo += texture(u_title_tex, clamp(sample_uv + title_texel * vec2(0.0, 16.0), 0.0, 1.0)).r;
            outer_halo += texture(u_title_tex, clamp(sample_uv + title_texel * vec2(0.0, -16.0), 0.0, 1.0)).r;
            outer_halo += texture(u_title_tex, clamp(sample_uv + title_texel * vec2(18.0, 18.0), 0.0, 1.0)).r;
            outer_halo += texture(u_title_tex, clamp(sample_uv + title_texel * vec2(-18.0, 18.0), 0.0, 1.0)).r;
            outer_halo += texture(u_title_tex, clamp(sample_uv + title_texel * vec2(18.0, -18.0), 0.0, 1.0)).r;
            outer_halo += texture(u_title_tex, clamp(sample_uv + title_texel * vec2(-18.0, -18.0), 0.0, 1.0)).r;
            outer_halo *= (1.0 / 16.0);

            float title_heat = 0.86
                + 0.14 * sin(u_time * 1.25 + sample_uv.x * 9.0 + sample_uv.y * 4.0);
            float title_glow = max(inner_halo - title_mask * 0.78, 0.0) * title_heat;
            float title_aura = max(outer_halo - title_mask * 0.18, 0.0) * (0.98 + title_heat * 0.24);
            float title_core = title_mask * (0.97 + 0.03 * sin(u_time * 1.10 + sample_uv.x * 8.0));
            color = mix(color, vec3(0.01, 0.02, 0.05), title_shadow * 0.28);
            color += vec3(0.05, 0.10, 0.26) * title_aura * 0.84;
            color += vec3(0.24, 0.38, 0.80) * pow(title_aura, 1.02) * 0.66;
            color += vec3(0.84, 0.92, 1.00) * pow(title_aura, 1.32) * 0.12;
            color += vec3(0.18, 0.28, 0.58) * title_glow * 0.50;
            color += vec3(0.92, 0.96, 1.00) * pow(title_glow, 1.50) * 0.12;
            color = mix(color, vec3(0.97, 0.98, 1.0), title_core);
        }}
    }}

    float vignette = smoothstep(1.34, 0.24, length(centered * vec2(1.04, 0.92)));
    color *= mix(0.50, 1.0, vignette);
    color += vec3(0.04, 0.05, 0.08) * (1.0 - vignette) * 0.16;

    frag_color = vec4(pow(color, vec3(0.96)), 1.0);
}}
"#
    )
}
