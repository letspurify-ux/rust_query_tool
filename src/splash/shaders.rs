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

// ========================================================
//  Utility
// ========================================================

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

vec2 hash22(vec2 p) {
    return vec2(hash21(p), hash21(p + 127.1));
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
    float v = 0.0, a = 0.5;
    mat2 rot = mat2(0.8, 0.6, -0.6, 0.8);  // domain rotation for richer detail
    for (int i = 0; i < 6; i++) {
        v += a * value_noise(p);
        p = rot * p * 2.0 + 0.5;
        a *= 0.5;
    }
    return v;
}

float sd_segment(vec2 p, vec2 a, vec2 b) {
    vec2 pa = p - a, ba = b - a;
    float h = clamp(dot(pa, ba) / dot(ba, ba), 0.0, 1.0);
    return length(pa - ba * h);
}

// ========================================================
//  Camera drift  — subtle parallax movement
// ========================================================

vec2 camera_offset() {
    return vec2(sin(u_time * 0.15) * 0.008, cos(u_time * 0.12) * 0.006);
}

// ========================================================
//  Deep-space background
// ========================================================

vec3 background(vec2 uv_c) {
    float d = length(uv_c);
    // Rich multi-tone gradient
    vec3 deep   = vec3(0.01, 0.005, 0.03);
    vec3 mid    = vec3(0.02, 0.01,  0.06);
    vec3 bright = vec3(0.03, 0.02,  0.09);
    vec3 col = mix(bright, mid, smoothstep(0.0, 0.5, d));
    col = mix(col, deep, smoothstep(0.5, 1.0, d));
    // Very faint warm corner
    col += vec3(0.015, 0.005, 0.0) * (1.0 - smoothstep(0.2, 0.8, length(uv_c - vec2(0.5, 0.3))));
    return col;
}

// ========================================================
//  Cosmic dust — layered wispy clouds
// ========================================================

vec3 cosmic_dust(vec2 p, float time) {
    vec3 col = vec3(0.0);
    // Layer 1: large blue dust
    float n1 = fbm(p * 1.5 + vec2(time * 0.005, time * 0.003));
    float n2 = fbm(p * 2.5 + vec2(-time * 0.008, time * 0.004) + 50.0);
    float mask1 = smoothstep(0.35, 0.65, n1) * (1.0 - smoothstep(0.3, 0.9, length(p)));
    float mask2 = smoothstep(0.4, 0.7, n2) * (1.0 - smoothstep(0.2, 1.0, length(p + vec2(0.15, -0.1))));
    col += vec3(0.04, 0.08, 0.2) * mask1 * 1.2;
    col += vec3(0.12, 0.04, 0.18) * mask2 * 0.8;
    return col;
}

// ========================================================
//  Nebula  — vibrant spiral structure
// ========================================================

vec3 nebula(vec2 p, float time) {
    // Slow spiral rotation
    float angle = time * 0.025 + length(p) * 1.5;
    float ca = cos(angle), sa = sin(angle);
    vec2 rp = vec2(ca * p.x - sa * p.y, sa * p.x + ca * p.y);

    float n1 = fbm(rp * 3.0 + vec2(time * 0.015, 0.0));
    float n2 = fbm(rp * 4.0 + vec2(0.0, time * 0.012) + 50.0);
    float n3 = fbm(rp * 2.0 - vec2(time * 0.01, time * 0.008) + 100.0);
    float n4 = fbm(p * 5.0 + vec2(time * 0.02, -time * 0.01) + 200.0);

    float dist = length(p);
    float falloff = 1.0 - smoothstep(0.08, 0.65, dist);

    vec3 col = vec3(0.0);
    col += vec3(0.06, 0.18, 0.55) * n1 * 1.8;     // Deep blue core
    col += vec3(0.0, 0.4, 0.75) * n2 * 1.0;        // Bright accent blue
    col += vec3(0.3, 0.1, 0.5) * n3 * 0.9;         // Purple wisps
    col += vec3(0.6, 0.2, 0.4) * n4 * 0.3;         // Pink highlights
    col += vec3(0.5, 0.7, 1.0) * pow(falloff, 4.0) * 0.2;  // Hot core
    col *= falloff;

    return col;
}

// ========================================================
//  Star field  — multi-color with diffraction spikes
// ========================================================

vec3 star_layer(vec2 uv_in, float scale, float time_offset, float drift) {
    vec2 drifted = uv_in + camera_offset() * drift;
    vec2 grid_uv = drifted * scale;
    vec2 grid_id = floor(grid_uv);
    vec2 grid_frac = fract(grid_uv) - 0.5;

    vec3 col = vec3(0.0);

    for (int y = -1; y <= 1; y++) {
        for (int x = -1; x <= 1; x++) {
            vec2 offset = vec2(float(x), float(y));
            vec2 cell_id = grid_id + offset;
            float h = hash21(cell_id);

            if (h > 0.35) continue;  // ~35% fill

            vec2 star_pos = offset + hash22(cell_id + 100.0) - 0.5 - grid_frac;
            float dist = length(star_pos);

            // Twinkle
            float twinkle = sin(u_time * (1.2 + h * 4.0) + h * 6.28 + time_offset) * 0.5 + 0.5;
            twinkle = mix(0.2, 1.0, twinkle);

            // Size
            float brightness_h = hash21(cell_id + 300.0);
            float star_size = mix(0.008, 0.04, brightness_h * brightness_h);
            float star = (1.0 - smoothstep(0.0, star_size, dist)) * twinkle;

            // Color temperature
            float temp = hash21(cell_id + 500.0);
            vec3 star_col;
            if (temp < 0.3) {
                star_col = vec3(0.7, 0.8, 1.0);      // Hot blue
            } else if (temp < 0.7) {
                star_col = vec3(1.0, 1.0, 0.98);     // White
            } else if (temp < 0.9) {
                star_col = vec3(1.0, 0.95, 0.8);     // Warm yellow
            } else {
                star_col = vec3(1.0, 0.8, 0.6);      // Orange giant
            }

            // Diffraction spikes on bright stars
            if (brightness_h > 0.7 && dist < 0.15) {
                float spike_h = (1.0 - smoothstep(0.0, 0.003, abs(star_pos.x))) * (1.0 - smoothstep(0.0, 0.1, abs(star_pos.y)));
                float spike_v = (1.0 - smoothstep(0.0, 0.003, abs(star_pos.y))) * (1.0 - smoothstep(0.0, 0.1, abs(star_pos.x)));
                // Diagonal spikes
                float d45a = abs(star_pos.x - star_pos.y) * 0.707;
                float d45b = abs(star_pos.x + star_pos.y) * 0.707;
                float spike_d1 = (1.0 - smoothstep(0.0, 0.002, d45a)) * (1.0 - smoothstep(0.0, 0.07, d45b + d45a));
                float spike_d2 = (1.0 - smoothstep(0.0, 0.002, d45b)) * (1.0 - smoothstep(0.0, 0.07, d45a + d45b));
                float spikes = (spike_h + spike_v + spike_d1 * 0.5 + spike_d2 * 0.5) * twinkle * 0.4;
                col += star_col * spikes;
            }

            col += star_col * star * mix(0.7, 1.0, brightness_h);
        }
    }
    return col;
}

// ========================================================
//  Planet  — glowing sphere with atmosphere
// ========================================================

vec3 planet(vec2 p) {
    vec2 center = vec2(0.32, -0.05);
    float radius = 0.12;
    vec2 d = p - center;
    float dist = length(d);

    vec3 col = vec3(0.0);

    // Atmosphere outer glow
    float atmo_outer = 1.0 - smoothstep(radius + 0.01, radius + 0.08, dist);
    col += vec3(0.1, 0.3, 0.8) * atmo_outer * 0.3;

    // Atmosphere rim
    float atmo = 1.0 - smoothstep(radius, radius + 0.025, dist);
    col += vec3(0.2, 0.5, 1.0) * atmo * 0.5;

    if (dist < radius) {
        // Surface
        vec2 uv_sphere = d / radius;
        float z = sqrt(max(0.0, 1.0 - dot(uv_sphere, uv_sphere)));

        // Lighting from top-right
        vec3 normal = vec3(uv_sphere, z);
        vec3 light_dir = normalize(vec3(0.5, 0.4, 0.7));
        float diffuse = max(0.0, dot(normal, light_dir));
        float fresnel = pow(1.0 - z, 3.0);

        // Surface detail
        float surf_n = fbm(uv_sphere * 4.0 + 30.0);
        vec3 base_col = mix(vec3(0.02, 0.05, 0.15), vec3(0.05, 0.12, 0.3), surf_n);

        // Bands
        float bands = sin(uv_sphere.y * 12.0 + surf_n * 3.0) * 0.5 + 0.5;
        base_col = mix(base_col, vec3(0.08, 0.15, 0.35), bands * 0.3);

        col = base_col * (diffuse * 0.8 + 0.15);
        col += vec3(0.2, 0.5, 1.0) * fresnel * 0.6;   // Rim light
        col += vec3(0.4, 0.6, 1.0) * pow(fresnel, 6.0) * 0.8;  // Strong rim
    }

    return col;
}

// ========================================================
//  Lens flare  — anamorphic streak + ghost circles
// ========================================================

vec3 lens_flare(vec2 p) {
    vec2 light_pos = vec2(0.32, -0.05) + vec2(0.1, 0.08);  // Just above planet
    vec2 d = p - light_pos;
    float dist = length(d);

    vec3 col = vec3(0.0);

    // Central glow
    float glow = 0.015 / (dist * dist + 0.015);
    col += vec3(0.4, 0.6, 1.0) * glow * 0.15;

    // Anamorphic horizontal streak
    float streak = 0.001 / (d.y * d.y + 0.001) * (1.0 - smoothstep(0.0, 0.5, abs(d.x)));
    col += vec3(0.3, 0.5, 1.0) * streak * 0.03;

    // Ghost circles
    for (int i = 1; i <= 3; i++) {
        float fi = float(i);
        vec2 ghost_p = p + d * fi * 0.4;
        float ghost_dist = length(ghost_p - light_pos);
        float ring = abs(ghost_dist - 0.05 * fi);
        float ghost = (1.0 - smoothstep(0.0, 0.008, ring)) * 0.1 / fi;
        col += vec3(0.2, 0.4, 0.9) * ghost;
    }

    return col;
}

// ========================================================
//  Aurora ribbons  — flowing energy bands
// ========================================================

vec3 aurora(vec2 p, float time) {
    vec3 col = vec3(0.0);
    for (int i = 0; i < 3; i++) {
        float fi = float(i);
        float y_base = 0.15 - fi * 0.12;
        float wave = sin(p.x * (3.0 + fi) + time * (0.3 + fi * 0.1) + fi * 2.0) * 0.04;
        wave += sin(p.x * (7.0 + fi * 2.0) - time * 0.2 + fi) * 0.015;
        float y_dist = abs(p.y - y_base - wave);
        float ribbon = 1.0 - smoothstep(0.0, 0.025, y_dist);
        ribbon *= 1.0 - smoothstep(0.3, 0.8, abs(p.x));  // Fade at edges

        vec3 ribbon_col;
        if (i == 0) ribbon_col = vec3(0.1, 0.4, 0.9);   // Blue
        else if (i == 1) ribbon_col = vec3(0.05, 0.6, 0.7);  // Cyan
        else ribbon_col = vec3(0.3, 0.15, 0.6);               // Purple

        col += ribbon_col * ribbon * (0.15 - fi * 0.03);
    }
    return col;
}

// ========================================================
//  Shooting stars  — bright streaks
// ========================================================

vec3 shooting_star(vec2 uv_in, float seed, float time) {
    float cycle = 8.0 + seed * 4.0;
    float t = mod(time + seed * 3.0, cycle) / cycle;
    float active_window = 0.15;

    if (t > active_window) return vec3(0.0);

    float anim_t = t / active_window;

    float angle = hash11(seed * 17.3) * 0.4 + 0.2;
    vec2 start = vec2(hash11(seed * 7.1) * 0.5 + 0.25, hash11(seed * 13.7) * 0.3 + 0.55);
    vec2 dir = vec2(cos(angle), -sin(angle));

    float speed = 1.5;
    float tail_len = 0.2;

    vec2 head = start + dir * speed * anim_t;
    vec2 tail = head - dir * tail_len * min(anim_t * 4.0, 1.0);

    float d = sd_segment(uv_in, tail, head);
    float core = 1.0 - smoothstep(0.0, 0.003, d);
    float glow = (1.0 - smoothstep(0.0, 0.015, d)) * 0.5;

    float fade = smoothstep(0.0, 0.15, anim_t) * (1.0 - smoothstep(0.7, 1.0, anim_t));

    // Gradient from white head to blue tail
    float head_dist = length(uv_in - head);
    float tail_dist = length(uv_in - tail);
    float gradient = clamp(head_dist / (head_dist + tail_dist + 0.001), 0.0, 1.0);
    vec3 col = mix(vec3(1.0, 1.0, 1.0), vec3(0.3, 0.5, 1.0), gradient);

    return col * (core + glow) * fade;
}

// ========================================================
//  SDF Text — "SPACE Query"
// ========================================================

float letter_S(vec2 p) {
    float d = 1e10;
    d = min(d, sd_segment(p, vec2(0.8, 1.5), vec2(0.2, 1.5)));
    d = min(d, sd_segment(p, vec2(0.2, 1.5), vec2(0.2, 0.9)));
    d = min(d, sd_segment(p, vec2(0.2, 0.9), vec2(0.8, 0.75)));
    d = min(d, sd_segment(p, vec2(0.8, 0.75), vec2(0.8, 0.0)));
    d = min(d, sd_segment(p, vec2(0.8, 0.0), vec2(0.2, 0.0)));
    return d;
}

float letter_P(vec2 p) {
    float d = 1e10;
    d = min(d, sd_segment(p, vec2(0.2, 0.0), vec2(0.2, 1.5)));
    d = min(d, sd_segment(p, vec2(0.2, 1.5), vec2(0.7, 1.5)));
    d = min(d, sd_segment(p, vec2(0.7, 1.5), vec2(0.8, 1.35)));
    d = min(d, sd_segment(p, vec2(0.8, 1.35), vec2(0.8, 0.9)));
    d = min(d, sd_segment(p, vec2(0.8, 0.9), vec2(0.7, 0.75)));
    d = min(d, sd_segment(p, vec2(0.7, 0.75), vec2(0.2, 0.75)));
    return d;
}

float letter_A(vec2 p) {
    float d = 1e10;
    d = min(d, sd_segment(p, vec2(0.1, 0.0), vec2(0.5, 1.5)));
    d = min(d, sd_segment(p, vec2(0.5, 1.5), vec2(0.9, 0.0)));
    d = min(d, sd_segment(p, vec2(0.25, 0.6), vec2(0.75, 0.6)));
    return d;
}

float letter_C(vec2 p) {
    float d = 1e10;
    d = min(d, sd_segment(p, vec2(0.8, 1.35), vec2(0.6, 1.5)));
    d = min(d, sd_segment(p, vec2(0.6, 1.5), vec2(0.3, 1.5)));
    d = min(d, sd_segment(p, vec2(0.3, 1.5), vec2(0.2, 1.35)));
    d = min(d, sd_segment(p, vec2(0.2, 1.35), vec2(0.2, 0.15)));
    d = min(d, sd_segment(p, vec2(0.2, 0.15), vec2(0.3, 0.0)));
    d = min(d, sd_segment(p, vec2(0.3, 0.0), vec2(0.6, 0.0)));
    d = min(d, sd_segment(p, vec2(0.6, 0.0), vec2(0.8, 0.15)));
    return d;
}

float letter_E(vec2 p) {
    float d = 1e10;
    d = min(d, sd_segment(p, vec2(0.2, 0.0), vec2(0.2, 1.5)));
    d = min(d, sd_segment(p, vec2(0.2, 1.5), vec2(0.8, 1.5)));
    d = min(d, sd_segment(p, vec2(0.2, 0.75), vec2(0.7, 0.75)));
    d = min(d, sd_segment(p, vec2(0.2, 0.0), vec2(0.8, 0.0)));
    return d;
}

float letter_Q(vec2 p) {
    float d = 1e10;
    d = min(d, sd_segment(p, vec2(0.3, 1.5), vec2(0.7, 1.5)));
    d = min(d, sd_segment(p, vec2(0.7, 1.5), vec2(0.8, 1.35)));
    d = min(d, sd_segment(p, vec2(0.8, 1.35), vec2(0.8, 0.15)));
    d = min(d, sd_segment(p, vec2(0.8, 0.15), vec2(0.7, 0.0)));
    d = min(d, sd_segment(p, vec2(0.7, 0.0), vec2(0.3, 0.0)));
    d = min(d, sd_segment(p, vec2(0.3, 0.0), vec2(0.2, 0.15)));
    d = min(d, sd_segment(p, vec2(0.2, 0.15), vec2(0.2, 1.35)));
    d = min(d, sd_segment(p, vec2(0.2, 1.35), vec2(0.3, 1.5)));
    d = min(d, sd_segment(p, vec2(0.55, 0.3), vec2(0.9, -0.15)));
    return d;
}

float letter_u_lower(vec2 p) {
    float d = 1e10;
    d = min(d, sd_segment(p, vec2(0.2, 1.0), vec2(0.2, 0.15)));
    d = min(d, sd_segment(p, vec2(0.2, 0.15), vec2(0.3, 0.0)));
    d = min(d, sd_segment(p, vec2(0.3, 0.0), vec2(0.6, 0.0)));
    d = min(d, sd_segment(p, vec2(0.6, 0.0), vec2(0.7, 0.15)));
    d = min(d, sd_segment(p, vec2(0.7, 0.15), vec2(0.7, 1.0)));
    return d;
}

float letter_e_lower(vec2 p) {
    float d = 1e10;
    d = min(d, sd_segment(p, vec2(0.2, 0.5), vec2(0.8, 0.5)));
    d = min(d, sd_segment(p, vec2(0.8, 0.5), vec2(0.8, 0.75)));
    d = min(d, sd_segment(p, vec2(0.8, 0.75), vec2(0.6, 1.0)));
    d = min(d, sd_segment(p, vec2(0.6, 1.0), vec2(0.3, 1.0)));
    d = min(d, sd_segment(p, vec2(0.3, 1.0), vec2(0.2, 0.85)));
    d = min(d, sd_segment(p, vec2(0.2, 0.85), vec2(0.2, 0.15)));
    d = min(d, sd_segment(p, vec2(0.2, 0.15), vec2(0.4, 0.0)));
    d = min(d, sd_segment(p, vec2(0.4, 0.0), vec2(0.8, 0.1)));
    return d;
}

float letter_r_lower(vec2 p) {
    float d = 1e10;
    d = min(d, sd_segment(p, vec2(0.2, 0.0), vec2(0.2, 1.0)));
    d = min(d, sd_segment(p, vec2(0.2, 0.85), vec2(0.4, 1.0)));
    d = min(d, sd_segment(p, vec2(0.4, 1.0), vec2(0.7, 1.0)));
    return d;
}

float letter_y_lower(vec2 p) {
    float d = 1e10;
    d = min(d, sd_segment(p, vec2(0.2, 1.0), vec2(0.5, 0.35)));
    d = min(d, sd_segment(p, vec2(0.8, 1.0), vec2(0.5, 0.35)));
    d = min(d, sd_segment(p, vec2(0.5, 0.35), vec2(0.3, -0.3)));
    return d;
}

// Per-character distance with x-position for sequential reveal
float render_text_with_pos(vec2 p, out float char_x_pos) {
    float d = 1e10;
    char_x_pos = 0.0;
    float char_w = 1.15;
    float total_w = 12.0;
    float x_off = -total_w * 0.5;

    // S P A C E
    float ds = letter_S(p - vec2(x_off, 0.0));
    float dp = letter_P(p - vec2(x_off + char_w, 0.0));
    float da = letter_A(p - vec2(x_off + char_w * 2.0, 0.0));
    float dc = letter_C(p - vec2(x_off + char_w * 3.0, 0.0));
    float de = letter_E(p - vec2(x_off + char_w * 4.0, 0.0));

    if (ds < d) { d = ds; char_x_pos = 0.0; }
    if (dp < d) { d = dp; char_x_pos = 1.0; }
    if (da < d) { d = da; char_x_pos = 2.0; }
    if (dc < d) { d = dc; char_x_pos = 3.0; }
    if (de < d) { d = de; char_x_pos = 4.0; }

    float gap = 0.7;
    float qx = x_off + char_w * 5.0 + gap;
    float lx = qx + char_w;

    float dq  = letter_Q(p - vec2(qx, 0.0));
    float du  = letter_u_lower(p - vec2(lx, 0.0));
    float de2 = letter_e_lower(p - vec2(lx + char_w * 0.85, 0.0));
    float dr  = letter_r_lower(p - vec2(lx + char_w * 1.7, 0.0));
    float dy  = letter_y_lower(p - vec2(lx + char_w * 2.4, 0.0));

    if (dq  < d) { d = dq;  char_x_pos = 5.0; }
    if (du  < d) { d = du;  char_x_pos = 6.0; }
    if (de2 < d) { d = de2; char_x_pos = 7.0; }
    if (dr  < d) { d = dr;  char_x_pos = 8.0; }
    if (dy  < d) { d = dy;  char_x_pos = 9.0; }

    return d;
}

// ========================================================
//  Progress bar — glowing with particle at leading edge
// ========================================================

vec3 progress_bar(vec2 uv_in, float progress) {
    float bar_y = 0.055;
    float bar_h = 0.0025;
    float margin = 0.22;

    float in_bar = step(margin, uv_in.x) * step(uv_in.x, 1.0 - margin);
    float in_y = smoothstep(bar_y - bar_h * 3.0, bar_y - bar_h, uv_in.y)
               * (1.0 - smoothstep(bar_y + bar_h, bar_y + bar_h * 3.0, uv_in.y));

    // Track
    vec3 col = vec3(0.15, 0.2, 0.35) * in_bar * in_y * 0.3;

    // Fill
    float fill_x = mix(margin, 1.0 - margin, progress);
    float filled = step(uv_in.x, fill_x) * in_bar * in_y;

    // Gradient fill color
    float t = (uv_in.x - margin) / (1.0 - 2.0 * margin);
    vec3 fill_col = mix(vec3(0.0, 0.3, 0.8), vec3(0.2, 0.6, 1.0), t);
    col += fill_col * filled;

    // Glow at leading edge
    float edge_dist = abs(uv_in.x - fill_x);
    float edge_glow = (1.0 - smoothstep(0.0, 0.03, edge_dist)) * (1.0 - smoothstep(0.0, 0.02, abs(uv_in.y - bar_y)));
    col += vec3(0.4, 0.7, 1.0) * edge_glow * 0.6 * step(margin, fill_x);

    // Particle sparkle at tip
    float sparkle = 1.0 - smoothstep(0.0, 0.008, length(vec2(uv_in.x - fill_x, uv_in.y - bar_y)));
    sparkle *= (sin(u_time * 8.0) * 0.3 + 0.7);
    col += vec3(0.8, 0.9, 1.0) * sparkle * step(margin, fill_x);

    return col;
}

// ========================================================
//  Orbital ring  — subtle ring around the title area
// ========================================================

vec3 orbital_ring(vec2 p, float time) {
    float angle = time * 0.15;
    // Elliptical ring tilted in perspective
    vec2 rp = vec2(
        p.x * cos(angle * 0.3) + p.y * sin(angle * 0.3),
        (-p.x * sin(angle * 0.3) + p.y * cos(angle * 0.3)) * 3.0
    );
    float ring_dist = abs(length(rp) - 0.25);
    float ring = (1.0 - smoothstep(0.0, 0.003, ring_dist)) * 0.12;
    // Dotted/moving pattern
    float dots = sin(atan(rp.y, rp.x) * 20.0 + time * 2.0) * 0.5 + 0.5;
    ring *= dots;
    return vec3(0.2, 0.5, 1.0) * ring;
}

// ========================================================
//  Main composition
// ========================================================

void main() {
    vec2 aspect = vec2(u_resolution.x / u_resolution.y, 1.0);
    vec2 centered = (uv - 0.5) * aspect + camera_offset();

    vec3 col = vec3(0.0);

    // Background
    col += background(centered);

    // Cosmic dust
    col += cosmic_dust(centered, u_time);

    // Nebula
    col += nebula(centered * 1.1, u_time);

    // Aurora ribbons
    col += aurora(centered, u_time);

    // Stars (4 layers with parallax drift)
    col += star_layer(uv, 60.0, 0.0, 1.0);
    col += star_layer(uv, 120.0, 2.0, 1.5);
    col += star_layer(uv, 240.0, 4.0, 2.0);
    col += star_layer(uv, 400.0, 6.0, 3.0);

    // Planet
    col += planet(centered);

    // Lens flare
    col += lens_flare(centered);

    // Orbital ring
    col += orbital_ring(centered, u_time);

    // Shooting stars
    col += shooting_star(uv, 1.0, u_time);
    col += shooting_star(uv, 2.7, u_time);
    col += shooting_star(uv, 4.2, u_time);
    col += shooting_star(uv, 5.9, u_time);
    col += shooting_star(uv, 7.3, u_time);

    // --- Text "SPACE Query" with sequential reveal ---
    float text_scale = 0.065;
    vec2 text_p = centered / text_scale;
    text_p.y += 0.3 / text_scale;

    float char_x_pos;
    float text_d = render_text_with_pos(text_p, char_x_pos);
    text_d *= text_scale;

    // Sequential reveal: each character appears 0.12s apart starting at t=0.5
    float reveal_time = 0.5 + char_x_pos * 0.12;
    float char_reveal = smoothstep(reveal_time, reveal_time + 0.3, u_time);

    // Pulsing glow
    float pulse = sin(u_time * 1.5 - char_x_pos * 0.3) * 0.15 + 0.85;

    // Text rendering
    float text_fill = (1.0 - smoothstep(0.001, 0.004, text_d)) * char_reveal;
    float text_glow = (1.0 - smoothstep(0.0, 0.05, text_d)) * 0.6 * char_reveal * pulse;
    float text_glow_outer = (1.0 - smoothstep(0.0, 0.12, text_d)) * 0.2 * char_reveal;
    float text_glow_far = (1.0 - smoothstep(0.0, 0.2, text_d)) * 0.05 * char_reveal;

    // Arrival flash per character
    float flash_t = u_time - reveal_time;
    float arrival_flash = smoothstep(0.0, 0.05, flash_t) * (1.0 - smoothstep(0.1, 0.4, flash_t));
    float flash_glow = (1.0 - smoothstep(0.0, 0.08, text_d)) * arrival_flash * 0.8;

    col += vec3(1.0, 1.0, 1.0) * text_fill;
    col += vec3(0.3, 0.55, 1.0) * text_glow;
    col += vec3(0.15, 0.35, 0.85) * text_glow_outer;
    col += vec3(0.1, 0.2, 0.6) * text_glow_far;
    col += vec3(0.6, 0.8, 1.0) * flash_glow;

    // Decorative separator line with fade-in
    float sep_reveal = smoothstep(2.0, 3.0, u_time);
    float sep_y = -0.6 / text_scale;
    float sep_d = abs(text_p.y - sep_y) * text_scale;
    float sep_x_range = smoothstep(-4.0, -2.5, text_p.x) * (1.0 - smoothstep(2.5, 4.0, text_p.x));
    float separator = (1.0 - smoothstep(0.0004, 0.002, sep_d)) * sep_x_range * 0.35 * sep_reveal;
    col += vec3(0.3, 0.55, 1.0) * separator;

    // --- Progress bar ---
    float progress = clamp(u_time / 10.0, 0.0, 1.0);
    col += progress_bar(uv, progress);

    // --- Vignette ---
    float vig = 1.0 - smoothstep(0.4, 1.1, length(centered));
    col *= mix(0.6, 1.0, vig);

    // --- Final ---
    // Subtle film grain for cinematic feel
    float grain = (hash21(uv * u_resolution + u_time * 100.0) - 0.5) * 0.015;
    col += grain;

    col = clamp(col, 0.0, 1.0);
    col *= u_alpha;

    gl_FragColor = vec4(col, 1.0);
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
