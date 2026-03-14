#[cfg(feature = "gpu-splash")]
use crate::splash::animation::AnimationState;

#[cfg(feature = "gpu-splash")]
use glow::HasContext;

#[cfg(feature = "gpu-splash")]
pub struct GpuRenderer {
    gl: glow::Context,
    background_program: glow::Program,
    overlay_program: glow::Program,
    vao: glow::VertexArray,
    resolution_uniform: Option<glow::UniformLocation>,
    time_uniform: Option<glow::UniformLocation>,
    camera_uniform: Option<glow::UniformLocation>,
    progress_uniform: Option<glow::UniformLocation>,
    overlay_viewport_uniform: Option<glow::UniformLocation>,
    overlay_origin_uniform: Option<glow::UniformLocation>,
    overlay_size_uniform: Option<glow::UniformLocation>,
    overlay_sampler_uniform: Option<glow::UniformLocation>,
    overlay_texture: Option<OverlayTexture>,
}

#[cfg(feature = "gpu-splash")]
struct OverlayTexture {
    texture: glow::Texture,
}

#[cfg(feature = "gpu-splash")]
impl GpuRenderer {
    pub unsafe fn new<F>(loader: F) -> Result<Self, String>
    where
        F: FnMut(&str) -> *const std::os::raw::c_void,
    {
        let gl = glow::Context::from_loader_function(loader);
        let background_program = compile_program(
            &gl,
            background_vertex_shader_source,
            background_fragment_shader_source,
        )?;
        let overlay_program = compile_program(
            &gl,
            overlay_vertex_shader_source,
            overlay_fragment_shader_source,
        )?;
        let vao = gl
            .create_vertex_array()
            .map_err(|err| format!("Failed to create splash VAO: {err}"))?;

        gl.bind_vertex_array(Some(vao));
        gl.bind_vertex_array(None);

        Ok(Self {
            resolution_uniform: gl.get_uniform_location(background_program, "u_resolution"),
            time_uniform: gl.get_uniform_location(background_program, "u_time"),
            camera_uniform: gl.get_uniform_location(background_program, "u_camera"),
            progress_uniform: gl.get_uniform_location(background_program, "u_progress"),
            overlay_viewport_uniform: gl.get_uniform_location(overlay_program, "u_viewport"),
            overlay_origin_uniform: gl.get_uniform_location(overlay_program, "u_origin"),
            overlay_size_uniform: gl.get_uniform_location(overlay_program, "u_size"),
            overlay_sampler_uniform: gl.get_uniform_location(overlay_program, "u_overlay_tex"),
            overlay_texture: None,
            gl,
            background_program,
            overlay_program,
            vao,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub unsafe fn render(
        &mut self,
        animation: &AnimationState,
        progress: f32,
        viewport_width: i32,
        viewport_height: i32,
        overlay_origin: (i32, i32),
        overlay_size: (i32, i32),
        overlay_pixels: &[u8],
        overlay_width: i32,
        overlay_height: i32,
    ) -> Result<(), String> {
        self.upload_overlay_texture(overlay_pixels, overlay_width, overlay_height)?;

        self.gl
            .viewport(0, 0, viewport_width.max(1), viewport_height.max(1));
        self.gl.disable(glow::DEPTH_TEST);
        self.gl.disable(glow::BLEND);
        self.gl.clear_color(0.01, 0.01, 0.02, 1.0);
        self.gl.clear(glow::COLOR_BUFFER_BIT);
        self.gl.bind_vertex_array(Some(self.vao));

        self.gl.use_program(Some(self.background_program));
        if let Some(uniform) = &self.resolution_uniform {
            self.gl.uniform_2_f32(
                Some(uniform),
                viewport_width.max(1) as f32,
                viewport_height.max(1) as f32,
            );
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
        self.gl.draw_arrays(glow::TRIANGLES, 0, 3);

        if let Some(texture) = &self.overlay_texture {
            self.gl.enable(glow::BLEND);
            self.gl
                .blend_func(glow::SRC_ALPHA, glow::ONE_MINUS_SRC_ALPHA);
            self.gl.use_program(Some(self.overlay_program));
            if let Some(uniform) = &self.overlay_viewport_uniform {
                self.gl.uniform_2_f32(
                    Some(uniform),
                    viewport_width.max(1) as f32,
                    viewport_height.max(1) as f32,
                );
            }
            if let Some(uniform) = &self.overlay_origin_uniform {
                self.gl.uniform_2_f32(
                    Some(uniform),
                    overlay_origin.0.max(0) as f32,
                    overlay_origin.1.max(0) as f32,
                );
            }
            if let Some(uniform) = &self.overlay_size_uniform {
                self.gl.uniform_2_f32(
                    Some(uniform),
                    overlay_size.0.max(1) as f32,
                    overlay_size.1.max(1) as f32,
                );
            }
            if let Some(uniform) = &self.overlay_sampler_uniform {
                self.gl.uniform_1_i32(Some(uniform), 0);
            }
            self.gl.active_texture(glow::TEXTURE0);
            self.gl
                .bind_texture(glow::TEXTURE_2D, Some(texture.texture));
            self.gl.draw_arrays(glow::TRIANGLE_STRIP, 0, 4);
            self.gl.bind_texture(glow::TEXTURE_2D, None);
            self.gl.disable(glow::BLEND);
        }

        self.gl.use_program(None);
        self.gl.bind_vertex_array(None);
        Ok(())
    }

    unsafe fn upload_overlay_texture(
        &mut self,
        overlay_pixels: &[u8],
        overlay_width: i32,
        overlay_height: i32,
    ) -> Result<(), String> {
        if overlay_width <= 0 || overlay_height <= 0 {
            return Ok(());
        }

        if self.overlay_texture.is_none() {
            let texture = self
                .gl
                .create_texture()
                .map_err(|err| format!("Failed to create splash overlay texture: {err}"))?;
            self.gl.bind_texture(glow::TEXTURE_2D, Some(texture));
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
            self.overlay_texture = Some(OverlayTexture { texture });
        }

        if let Some(texture) = &self.overlay_texture {
            self.gl.active_texture(glow::TEXTURE0);
            self.gl
                .bind_texture(glow::TEXTURE_2D, Some(texture.texture));
            self.gl.pixel_store_i32(glow::UNPACK_ALIGNMENT, 1);
            self.gl.tex_image_2d(
                glow::TEXTURE_2D,
                0,
                glow::RGBA as i32,
                overlay_width,
                overlay_height,
                0,
                glow::RGBA,
                glow::UNSIGNED_BYTE,
                glow::PixelUnpackData::Slice(Some(overlay_pixels)),
            );
            self.gl.bind_texture(glow::TEXTURE_2D, None);
        }

        Ok(())
    }
}

#[cfg(feature = "gpu-splash")]
impl Drop for GpuRenderer {
    fn drop(&mut self) {
        unsafe {
            if let Some(texture) = self.overlay_texture.take() {
                self.gl.delete_texture(texture.texture);
            }
            self.gl.delete_program(self.background_program);
            self.gl.delete_program(self.overlay_program);
            self.gl.delete_vertex_array(self.vao);
        }
    }
}

#[cfg(feature = "gpu-splash")]
type ShaderSourceFactory = fn(&str) -> String;

#[cfg(feature = "gpu-splash")]
unsafe fn compile_program(
    gl: &glow::Context,
    vertex_source: ShaderSourceFactory,
    fragment_source: ShaderSourceFactory,
) -> Result<glow::Program, String> {
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
        let vertex = vertex_source(variant.directive);
        let fragment = fragment_source(variant.directive);

        match build_program(gl, &vertex, &fragment) {
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
fn background_vertex_shader_source(version_directive: &str) -> String {
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
fn background_fragment_shader_source(version_directive: &str) -> String {
    format!(
        r#"{version_directive}
in vec2 v_uv;
out vec4 frag_color;

uniform vec2 u_resolution;
uniform float u_time;
uniform vec2 u_camera;
uniform float u_progress;

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

    float vignette = smoothstep(1.34, 0.24, length(centered * vec2(1.04, 0.92)));
    color *= mix(0.50, 1.0, vignette);
    color += vec3(0.04, 0.05, 0.08) * (1.0 - vignette) * 0.16;

    frag_color = vec4(pow(color, vec3(0.96)), 1.0);
}}
"#
    )
}

#[cfg(feature = "gpu-splash")]
fn overlay_vertex_shader_source(version_directive: &str) -> String {
    format!(
        r#"{version_directive}
out vec2 v_uv;

uniform vec2 u_viewport;
uniform vec2 u_origin;
uniform vec2 u_size;

void main() {{
    vec2 corners[4] = vec2[](
        vec2(0.0, 0.0),
        vec2(1.0, 0.0),
        vec2(0.0, 1.0),
        vec2(1.0, 1.0)
    );
    vec2 uv = corners[gl_VertexID];
    vec2 pixel = u_origin + uv * u_size;
    vec2 ndc = vec2(
        (pixel.x / max(u_viewport.x, 1.0)) * 2.0 - 1.0,
        1.0 - (pixel.y / max(u_viewport.y, 1.0)) * 2.0
    );
    gl_Position = vec4(ndc, 0.0, 1.0);
    v_uv = uv;
}}
"#
    )
}

#[cfg(feature = "gpu-splash")]
fn overlay_fragment_shader_source(version_directive: &str) -> String {
    format!(
        r#"{version_directive}
in vec2 v_uv;
out vec4 frag_color;

uniform sampler2D u_overlay_tex;

void main() {{
    frag_color = texture(u_overlay_tex, v_uv);
}}
"#
    )
}
