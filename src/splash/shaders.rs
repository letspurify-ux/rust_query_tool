pub const VERTEX: &str = r#"#version 100
attribute vec2 position;
varying highp vec2 uv;

void main() {
    uv = position * 0.5 + 0.5;
    gl_Position = vec4(position, 0.0, 1.0);
}
"#;

pub const FRAGMENT: &str = r#"#version 100
precision highp float;

varying highp vec2 uv;

uniform float u_time;
uniform vec2 u_resolution;
uniform float u_alpha;

// --- Noise utilities ---

float hash21(vec2 p) {
    p = fract(p * vec2(123.34, 456.21));
    p += dot(p, p + 45.32);
    return fract(p.x * p.y);
}

float hash11(float p) {
    p = fract(p * 0.1031);
    p *= p + 33.33;
    p *= p + p;
    return fract(p);
}

float value_noise(vec2 p) {
    vec2 i = floor(p);
    vec2 f = fract(p);
    f = f * f * (3.0 - 2.0 * f);

    float a = hash21(i);
    float b = hash21(i + vec2(1.0, 0.0));
    float c = hash21(i + vec2(0.0, 1.0));
    float d = hash21(i + vec2(1.0, 1.0));

    return mix(mix(a, b, f.x), mix(c, d, f.x), f.y);
}

float fbm(vec2 p) {
    float val = 0.0;
    float amp = 0.5;
    float freq = 1.0;
    for (int i = 0; i < 5; i++) {
        val += amp * value_noise(p * freq);
        freq *= 2.0;
        amp *= 0.5;
    }
    return val;
}

// --- Star field ---

float star_layer(vec2 uv_in, float scale, float time_offset) {
    vec2 grid_uv = uv_in * scale;
    vec2 grid_id = floor(grid_uv);
    vec2 grid_frac = fract(grid_uv) - 0.5;

    float brightness = 0.0;
    // Check 3x3 neighborhood to avoid edge artifacts
    for (int y = -1; y <= 1; y++) {
        for (int x = -1; x <= 1; x++) {
            vec2 offset = vec2(float(x), float(y));
            vec2 cell_id = grid_id + offset;
            float h = hash21(cell_id);

            // Only ~30% of cells have stars
            if (h > 0.3) continue;

            vec2 star_pos = offset + vec2(hash21(cell_id + 100.0), hash21(cell_id + 200.0)) - 0.5 - grid_frac;
            float dist = length(star_pos);

            // Star twinkle
            float twinkle = sin(u_time * (1.5 + h * 3.0) + h * 6.28 + time_offset) * 0.5 + 0.5;
            twinkle = mix(0.3, 1.0, twinkle);

            // Star size varies
            float star_size = mix(0.01, 0.035, h * h);
            float star = smoothstep(star_size, 0.0, dist) * twinkle;

            // Color temperature variation
            brightness += star * mix(0.6, 1.0, hash21(cell_id + 300.0));
        }
    }
    return brightness;
}

// --- Nebula ---

vec3 nebula(vec2 p, float time) {
    // Slow rotation
    float angle = time * 0.03;
    float ca = cos(angle);
    float sa = sin(angle);
    vec2 rp = vec2(ca * p.x - sa * p.y, sa * p.x + ca * p.y);

    float n1 = fbm(rp * 2.5 + vec2(time * 0.02, 0.0));
    float n2 = fbm(rp * 3.5 + vec2(0.0, time * 0.015) + 50.0);
    float n3 = fbm(rp * 1.8 - vec2(time * 0.01, time * 0.008) + 100.0);

    // Radial falloff from center
    float dist = length(p);
    float falloff = smoothstep(0.7, 0.1, dist);

    // Color channels
    vec3 col = vec3(0.0);
    col += vec3(0.05, 0.15, 0.45) * n1 * falloff * 1.5;     // Deep blue
    col += vec3(0.0, 0.35, 0.65) * n2 * falloff * 0.8;      // Accent blue
    col += vec3(0.25, 0.12, 0.45) * n3 * falloff * 0.6;     // Purple tint
    col += vec3(0.4, 0.6, 1.0) * pow(falloff, 3.0) * 0.15;  // Core glow

    return col;
}

// --- SDF text rendering ---

// Line segment SDF
float sd_segment(vec2 p, vec2 a, vec2 b) {
    vec2 pa = p - a;
    vec2 ba = b - a;
    float h = clamp(dot(pa, ba) / dot(ba, ba), 0.0, 1.0);
    return length(pa - ba * h);
}

// Box SDF
float sd_box(vec2 p, vec2 center, vec2 half_size) {
    vec2 d = abs(p - center) - half_size;
    return length(max(d, 0.0)) + min(max(d.x, d.y), 0.0);
}

// Each letter defined as line segments
// Letters are drawn in a normalized coordinate system
// where each char is roughly 0.0 to 1.0 wide and 0.0 to 1.5 tall
float letter_S(vec2 p) {
    float d = 1e10;
    d = min(d, sd_segment(p, vec2(0.8, 1.5), vec2(0.2, 1.5)));   // top bar
    d = min(d, sd_segment(p, vec2(0.2, 1.5), vec2(0.2, 0.9)));   // top-left vertical
    d = min(d, sd_segment(p, vec2(0.2, 0.9), vec2(0.8, 0.75)));  // middle diagonal
    d = min(d, sd_segment(p, vec2(0.8, 0.75), vec2(0.8, 0.0)));  // bottom-right vertical
    d = min(d, sd_segment(p, vec2(0.8, 0.0), vec2(0.2, 0.0)));   // bottom bar
    return d;
}

float letter_P(vec2 p) {
    float d = 1e10;
    d = min(d, sd_segment(p, vec2(0.2, 0.0), vec2(0.2, 1.5)));   // left vertical
    d = min(d, sd_segment(p, vec2(0.2, 1.5), vec2(0.7, 1.5)));   // top bar
    d = min(d, sd_segment(p, vec2(0.7, 1.5), vec2(0.8, 1.35)));  // top-right curve
    d = min(d, sd_segment(p, vec2(0.8, 1.35), vec2(0.8, 0.9)));  // right vertical
    d = min(d, sd_segment(p, vec2(0.8, 0.9), vec2(0.7, 0.75)));  // bottom-right curve
    d = min(d, sd_segment(p, vec2(0.7, 0.75), vec2(0.2, 0.75))); // middle bar
    return d;
}

float letter_A(vec2 p) {
    float d = 1e10;
    d = min(d, sd_segment(p, vec2(0.1, 0.0), vec2(0.5, 1.5)));   // left stroke
    d = min(d, sd_segment(p, vec2(0.5, 1.5), vec2(0.9, 0.0)));   // right stroke
    d = min(d, sd_segment(p, vec2(0.25, 0.6), vec2(0.75, 0.6))); // crossbar
    return d;
}

float letter_C(vec2 p) {
    float d = 1e10;
    d = min(d, sd_segment(p, vec2(0.8, 1.35), vec2(0.6, 1.5)));  // top-right approach
    d = min(d, sd_segment(p, vec2(0.6, 1.5), vec2(0.3, 1.5)));   // top bar
    d = min(d, sd_segment(p, vec2(0.3, 1.5), vec2(0.2, 1.35)));  // top-left curve
    d = min(d, sd_segment(p, vec2(0.2, 1.35), vec2(0.2, 0.15))); // left vertical
    d = min(d, sd_segment(p, vec2(0.2, 0.15), vec2(0.3, 0.0)));  // bottom-left curve
    d = min(d, sd_segment(p, vec2(0.3, 0.0), vec2(0.6, 0.0)));   // bottom bar
    d = min(d, sd_segment(p, vec2(0.6, 0.0), vec2(0.8, 0.15)));  // bottom-right approach
    return d;
}

float letter_E(vec2 p) {
    float d = 1e10;
    d = min(d, sd_segment(p, vec2(0.2, 0.0), vec2(0.2, 1.5)));   // left vertical
    d = min(d, sd_segment(p, vec2(0.2, 1.5), vec2(0.8, 1.5)));   // top bar
    d = min(d, sd_segment(p, vec2(0.2, 0.75), vec2(0.7, 0.75))); // middle bar
    d = min(d, sd_segment(p, vec2(0.2, 0.0), vec2(0.8, 0.0)));   // bottom bar
    return d;
}

float letter_Q(vec2 p) {
    float d = 1e10;
    // O shape
    d = min(d, sd_segment(p, vec2(0.3, 1.5), vec2(0.7, 1.5)));   // top
    d = min(d, sd_segment(p, vec2(0.7, 1.5), vec2(0.8, 1.35)));  // top-right
    d = min(d, sd_segment(p, vec2(0.8, 1.35), vec2(0.8, 0.15))); // right
    d = min(d, sd_segment(p, vec2(0.8, 0.15), vec2(0.7, 0.0)));  // bottom-right
    d = min(d, sd_segment(p, vec2(0.7, 0.0), vec2(0.3, 0.0)));   // bottom
    d = min(d, sd_segment(p, vec2(0.3, 0.0), vec2(0.2, 0.15)));  // bottom-left
    d = min(d, sd_segment(p, vec2(0.2, 0.15), vec2(0.2, 1.35))); // left
    d = min(d, sd_segment(p, vec2(0.2, 1.35), vec2(0.3, 1.5)));  // top-left
    // Q tail
    d = min(d, sd_segment(p, vec2(0.55, 0.3), vec2(0.9, -0.15)));
    return d;
}

float letter_u_lower(vec2 p) {
    float d = 1e10;
    d = min(d, sd_segment(p, vec2(0.2, 1.0), vec2(0.2, 0.15)));  // left down
    d = min(d, sd_segment(p, vec2(0.2, 0.15), vec2(0.3, 0.0)));  // bottom-left curve
    d = min(d, sd_segment(p, vec2(0.3, 0.0), vec2(0.6, 0.0)));   // bottom
    d = min(d, sd_segment(p, vec2(0.6, 0.0), vec2(0.7, 0.15)));  // bottom-right curve
    d = min(d, sd_segment(p, vec2(0.7, 0.15), vec2(0.7, 1.0)));  // right up
    return d;
}

float letter_e_lower(vec2 p) {
    float d = 1e10;
    d = min(d, sd_segment(p, vec2(0.2, 0.5), vec2(0.8, 0.5)));   // middle bar
    d = min(d, sd_segment(p, vec2(0.8, 0.5), vec2(0.8, 0.75)));  // right-up
    d = min(d, sd_segment(p, vec2(0.8, 0.75), vec2(0.6, 1.0)));  // top-right
    d = min(d, sd_segment(p, vec2(0.6, 1.0), vec2(0.3, 1.0)));   // top
    d = min(d, sd_segment(p, vec2(0.3, 1.0), vec2(0.2, 0.85)));  // top-left
    d = min(d, sd_segment(p, vec2(0.2, 0.85), vec2(0.2, 0.15))); // left
    d = min(d, sd_segment(p, vec2(0.2, 0.15), vec2(0.4, 0.0)));  // bottom-left
    d = min(d, sd_segment(p, vec2(0.4, 0.0), vec2(0.8, 0.1)));   // bottom
    return d;
}

float letter_r_lower(vec2 p) {
    float d = 1e10;
    d = min(d, sd_segment(p, vec2(0.2, 0.0), vec2(0.2, 1.0)));   // vertical
    d = min(d, sd_segment(p, vec2(0.2, 0.85), vec2(0.4, 1.0)));  // curve start
    d = min(d, sd_segment(p, vec2(0.4, 1.0), vec2(0.7, 1.0)));   // top
    return d;
}

float letter_y_lower(vec2 p) {
    float d = 1e10;
    d = min(d, sd_segment(p, vec2(0.2, 1.0), vec2(0.5, 0.35)));  // left stroke down
    d = min(d, sd_segment(p, vec2(0.8, 1.0), vec2(0.5, 0.35)));  // right stroke down
    d = min(d, sd_segment(p, vec2(0.5, 0.35), vec2(0.3, -0.3))); // tail
    return d;
}

// Render "SPACE" (uppercase) and "Query" (mixed case)
float render_text(vec2 p) {
    float d = 1e10;
    float char_w = 1.15;   // spacing between characters

    // "SPACE" - uppercase letters, shifted to center
    // Total width: 5 chars * 1.15 + gap + 5 chars * 1.15 ≈ 12
    float total_w = 12.0;
    float x_offset = -total_w * 0.5;

    // S
    d = min(d, letter_S(p - vec2(x_offset, 0.0)));
    // P
    d = min(d, letter_P(p - vec2(x_offset + char_w, 0.0)));
    // A
    d = min(d, letter_A(p - vec2(x_offset + char_w * 2.0, 0.0)));
    // C
    d = min(d, letter_C(p - vec2(x_offset + char_w * 3.0, 0.0)));
    // E
    d = min(d, letter_E(p - vec2(x_offset + char_w * 4.0, 0.0)));

    // Gap between words
    float gap = 0.7;

    // "Query" - Q uppercase, rest lowercase (shorter height)
    float query_x = x_offset + char_w * 5.0 + gap;

    // Q (uppercase)
    d = min(d, letter_Q(p - vec2(query_x, 0.0)));

    // u (lowercase - offset y so baseline aligns)
    float lower_x = query_x + char_w;
    d = min(d, letter_u_lower(p - vec2(lower_x, 0.0)));

    // e
    d = min(d, letter_e_lower(p - vec2(lower_x + char_w * 0.85, 0.0)));

    // r
    d = min(d, letter_r_lower(p - vec2(lower_x + char_w * 1.7, 0.0)));

    // y
    d = min(d, letter_y_lower(p - vec2(lower_x + char_w * 2.4, 0.0)));

    return d;
}

// --- Progress bar ---

float progress_bar(vec2 uv_in, float progress) {
    float bar_y = 0.06;
    float bar_h = 0.003;
    float bar_margin = 0.2;

    float in_bar = step(bar_margin, uv_in.x) * step(uv_in.x, 1.0 - bar_margin);
    float in_y = smoothstep(bar_y - bar_h * 2.0, bar_y - bar_h, uv_in.y)
               * smoothstep(bar_y + bar_h * 2.0, bar_y + bar_h, uv_in.y);

    // Background track
    float track = in_bar * in_y * 0.15;

    // Filled portion
    float fill_x = mix(bar_margin, 1.0 - bar_margin, progress);
    float filled = step(uv_in.x, fill_x) * in_bar * in_y;

    return track + filled;
}

// --- Shooting stars ---

float shooting_star(vec2 uv_in, float seed, float time) {
    float t = fract(time * 0.08 + seed);
    float active = step(0.0, t) * step(t, 0.3); // active 30% of cycle

    if (active < 0.5) return 0.0;

    float anim_t = t / 0.3;

    // Start and direction from seed
    float angle = hash11(seed * 17.3) * 0.5 + 0.3;
    vec2 start = vec2(hash11(seed * 7.1) * 0.6 + 0.2, hash11(seed * 13.7) * 0.4 + 0.5);
    vec2 dir = vec2(cos(angle), -sin(angle));

    float speed = 1.2;
    float tail_len = 0.15;

    vec2 head = start + dir * speed * anim_t;
    vec2 tail = head - dir * tail_len;

    float d = sd_segment(uv_in, tail, head);
    float brightness = smoothstep(0.004, 0.0, d);

    // Fade at edges of animation
    float fade = smoothstep(0.0, 0.1, anim_t) * smoothstep(0.3, 0.2, anim_t);

    return brightness * fade * 0.7;
}

void main() {
    vec2 aspect = vec2(u_resolution.x / u_resolution.y, 1.0);
    vec2 centered = (uv - 0.5) * aspect;

    // --- Background ---
    float vignette = 1.0 - length(centered) * 0.7;
    vignette = clamp(vignette, 0.0, 1.0);
    vec3 bg = mix(vec3(0.01, 0.01, 0.04), vec3(0.02, 0.02, 0.08), vignette);

    // Subtle vertical gradient
    bg += vec3(0.005, 0.005, 0.02) * uv.y;

    // --- Nebula ---
    vec3 neb = nebula(centered * 1.2, u_time);
    bg += neb;

    // --- Stars (3 layers) ---
    float stars = 0.0;
    stars += star_layer(uv, 80.0, 0.0) * 0.6;
    stars += star_layer(uv, 160.0, 2.0) * 0.4;
    stars += star_layer(uv, 320.0, 4.0) * 0.25;

    // Star color: mix white and light blue
    vec3 star_color = mix(vec3(0.8, 0.85, 1.0), vec3(1.0, 1.0, 1.0), 0.5);
    bg += star_color * stars;

    // --- Shooting stars ---
    float shooters = 0.0;
    shooters += shooting_star(uv, 1.0, u_time);
    shooters += shooting_star(uv, 2.7, u_time + 3.5);
    shooters += shooting_star(uv, 4.2, u_time + 7.0);
    bg += vec3(0.8, 0.9, 1.0) * shooters;

    // --- Text "SPACE Query" ---
    // Scale and position text
    float text_scale = 0.07;
    vec2 text_p = centered / text_scale;
    text_p.y += 0.5 / text_scale; // offset slightly above center

    float text_d = render_text(text_p);
    text_d *= text_scale; // scale distance back

    // Sharp text fill
    float text_fill = smoothstep(0.004, 0.001, text_d);

    // Glow around text
    float text_glow = smoothstep(0.06, 0.0, text_d) * 0.5;
    float text_glow_outer = smoothstep(0.12, 0.0, text_d) * 0.15;

    // Text color: bright white with blue glow
    bg += vec3(1.0, 1.0, 1.0) * text_fill;
    bg += vec3(0.3, 0.5, 1.0) * text_glow;
    bg += vec3(0.1, 0.3, 0.8) * text_glow_outer;

    // --- Subtitle "Oracle Database Query Tool" ---
    // Simple thin line as a decorative separator
    float sep_y = -0.5 / text_scale;
    float sep_d = abs(text_p.y - sep_y) * text_scale;
    float sep_x_range = step(-3.0, text_p.x) * step(text_p.x, 3.0);
    float separator = smoothstep(0.002, 0.0005, sep_d) * sep_x_range * 0.3;
    bg += vec3(0.3, 0.5, 1.0) * separator;

    // --- Progress bar ---
    float progress = clamp(u_time / 10.0, 0.0, 1.0);
    float bar = progress_bar(uv, progress);
    bg += vec3(0.0, 0.47, 0.83) * bar;

    // --- Final composite ---
    bg = clamp(bg, 0.0, 1.0);
    bg *= u_alpha;

    gl_FragColor = vec4(bg, 1.0);
}
"#;

pub fn meta() -> miniquad::ShaderMeta {
    miniquad::ShaderMeta {
        images: vec![],
        uniforms: miniquad::UniformBlockLayout {
            uniforms: vec![
                miniquad::UniformDesc::new("u_time", miniquad::UniformType::Float1),
                miniquad::UniformDesc::new("u_resolution", miniquad::UniformType::Float2),
                miniquad::UniformDesc::new("u_alpha", miniquad::UniformType::Float1),
            ],
        },
    }
}
