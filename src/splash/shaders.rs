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

    // Milky Way band — diagonal luminous streak with noise structure
    float mw_angle = 0.35;
    float mw_ca = cos(mw_angle), mw_sa = sin(mw_angle);
    vec2 mw_uv = vec2(mw_ca * uv_c.x + mw_sa * uv_c.y,
                      -mw_sa * uv_c.x + mw_ca * uv_c.y);
    float mw_dist = abs(mw_uv.y + 0.05);
    float mw_band = 1.0 - smoothstep(0.0, 0.25, mw_dist);
    mw_band *= mw_band;
    // Add noise structure within the band
    float mw_noise = fbm(vec2(mw_uv.x * 4.0 + 10.0, mw_uv.y * 8.0 + 20.0));
    float mw_detail = fbm(vec2(mw_uv.x * 12.0 + 30.0, mw_uv.y * 6.0 + 40.0));
    // Dark rift — central dust lane
    float dark_rift = smoothstep(0.45, 0.55, mw_detail) * smoothstep(0.0, 0.1, mw_band);
    col += vec3(0.025, 0.02, 0.04) * mw_band * mw_noise * 1.5;
    col += vec3(0.01, 0.015, 0.03) * mw_band * 0.5;
    col *= 1.0 - dark_rift * 0.3;

    return col;
}

// ========================================================
//  Distant galaxies — tiny fuzzy blobs for depth
// ========================================================

vec3 distant_galaxies(vec2 uv_in) {
    vec3 col = vec3(0.0);
    // Grid-based placement of faint distant galaxies
    vec2 grid = uv_in * 18.0;
    vec2 gid = floor(grid);
    vec2 gf = fract(grid) - 0.5;

    float h = hash21(gid + 700.0);
    if (h < 0.06) {  // ~6% of cells get a galaxy
        vec2 pos = hash22(gid + 800.0) - 0.5;
        vec2 dp = gf - pos;

        // Elliptical shape — random orientation and ellipticity
        float ga = hash21(gid + 900.0) * 3.14159;
        float ellip = 0.3 + hash21(gid + 1000.0) * 0.5;
        float gca = cos(ga), gsa = sin(ga);
        vec2 rotated = vec2(gca * dp.x + gsa * dp.y, (-gsa * dp.x + gca * dp.y) * (1.0 / ellip));
        float gdist = length(rotated);

        // Core + exponential halo
        float galaxy = (1.0 - smoothstep(0.0, 0.06, gdist)) * 0.4;
        galaxy += (1.0 - smoothstep(0.0, 0.02, gdist)) * 0.6;

        // Subtle spiral hint via noise
        float spiral = fbm(rotated * 15.0 + hash21(gid + 1100.0) * 50.0);
        galaxy *= 0.6 + spiral * 0.4;

        // Color varies: some yellowish, some bluish
        float color_h = hash21(gid + 1200.0);
        vec3 gal_col;
        if (color_h < 0.4) {
            gal_col = vec3(0.9, 0.85, 0.6);    // Elliptical — warm
        } else {
            gal_col = vec3(0.5, 0.6, 0.9);      // Spiral — blue
        }
        col += gal_col * galaxy * 0.04;
    }
    return col;
}

// ========================================================
//  Cosmic dust — layered wispy clouds
// ========================================================

vec3 cosmic_dust(vec2 p, float time, out float extinction) {
    vec3 col = vec3(0.0);
    extinction = 0.0;

    // Layer 1: large blue emission nebulosity
    float n1 = fbm(p * 1.5 + vec2(time * 0.005, time * 0.003));
    float n2 = fbm(p * 2.5 + vec2(-time * 0.008, time * 0.004) + 50.0);
    float mask1 = smoothstep(0.35, 0.65, n1) * (1.0 - smoothstep(0.3, 0.9, length(p)));
    float mask2 = smoothstep(0.4, 0.7, n2) * (1.0 - smoothstep(0.2, 1.0, length(p + vec2(0.15, -0.1))));
    col += vec3(0.04, 0.08, 0.2) * mask1 * 1.2;
    col += vec3(0.12, 0.04, 0.18) * mask2 * 0.8;

    // Layer 2: fine filaments — reflection nebula
    float n3 = fbm(p * 6.0 + vec2(time * 0.003, -time * 0.002) + 150.0);
    float filament = smoothstep(0.42, 0.58, n3) * (1.0 - smoothstep(0.25, 0.7, length(p - vec2(-0.1, 0.05))));
    col += vec3(0.06, 0.1, 0.25) * filament * 0.6;

    // Dark nebula — opaque dust that absorbs background light
    float dark_n = fbm(p * 3.0 + vec2(-time * 0.004, 0.0) + 250.0);
    float dark_mask = smoothstep(0.5, 0.65, dark_n) * (1.0 - smoothstep(0.4, 0.85, length(p + vec2(0.2, 0.15))));
    extinction = dark_mask * 0.5;

    // Forward scattering — dust lit from behind by stars/nebula
    vec2 light_dir_2d = normalize(vec2(0.42, 0.03) - p);
    float p_len = length(p);
    vec2 p_safe = (p_len > 0.001) ? p / p_len : vec2(0.0, 1.0);
    float scatter_angle = dot(p_safe, light_dir_2d) * 0.5 + 0.5;
    float forward_scatter = pow(scatter_angle, 4.0) * dark_mask;
    col += vec3(0.08, 0.12, 0.3) * forward_scatter * 0.4;

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

    // Domain warping for organic filament structure
    float warp_n = fbm(p * 2.0 + vec2(time * 0.008, -time * 0.006));
    vec2 warped = rp + vec2(warp_n - 0.5, fbm(p * 2.5 + 40.0) - 0.5) * 0.15;

    float n1 = fbm(warped * 3.0 + vec2(time * 0.015, 0.0));
    float n2 = fbm(warped * 4.0 + vec2(0.0, time * 0.012) + 50.0);
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

    // Emission edges — bright rims where gas density changes sharply
    float edge_n = fbm(warped * 6.0 + 300.0);
    float edge = abs(edge_n - 0.5);
    float emission = (1.0 - smoothstep(0.0, 0.06, edge)) * falloff;
    col += vec3(0.25, 0.5, 1.0) * emission * 0.2;

    // Dark dust absorption lanes
    float dust = fbm(rp * 3.5 + vec2(time * 0.005) + 500.0);
    float absorption = smoothstep(0.45, 0.55, dust) * falloff * 0.6;
    col *= 1.0 - absorption;

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

            // Soft halo glow — Airy disk-like falloff for bright stars
            if (brightness_h > 0.4) {
                float halo_radius = star_size * 3.0;
                float halo = (1.0 - smoothstep(0.0, halo_radius, dist));
                halo = halo * halo;  // Quadratic falloff
                col += star_col * halo * (brightness_h - 0.4) * twinkle * 0.3;
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

                // Chromatic fringe on brightest stars — subtle color bleed
                float fringe = (1.0 - smoothstep(0.0, star_size * 2.5, dist));
                col += vec3(0.1, 0.0, 0.2) * fringe * twinkle * 0.1;
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

    // Atmosphere outer glow — Rayleigh-like blue scatter
    float atmo_outer = 1.0 - smoothstep(radius + 0.01, radius + 0.10, dist);
    col += vec3(0.08, 0.22, 0.65) * atmo_outer * 0.25;
    // Secondary warm scatter on the lit side
    vec2 light_side = normalize(vec2(0.5, 0.4));
    float lit_factor = (dist > 0.001) ? dot(d / dist, light_side) * 0.5 + 0.5 : 0.5;
    col += vec3(0.15, 0.25, 0.6) * atmo_outer * lit_factor * 0.15;

    // Atmosphere rim
    float atmo = 1.0 - smoothstep(radius, radius + 0.03, dist);
    col += vec3(0.2, 0.5, 1.0) * atmo * 0.45;

    if (dist < radius) {
        // Surface
        vec2 uv_sphere = d / radius;
        float z = sqrt(max(0.0, 1.0 - dot(uv_sphere, uv_sphere)));

        // Lighting from top-right
        vec3 normal = vec3(uv_sphere, z);
        vec3 light_dir = normalize(vec3(0.5, 0.4, 0.7));
        float diffuse = max(0.0, dot(normal, light_dir));
        float fresnel = pow(1.0 - z, 3.0);

        // Terminator — soft day/night transition
        float terminator = smoothstep(-0.05, 0.2, diffuse);

        // Surface detail — multi-octave for realism
        float surf_n = fbm(uv_sphere * 4.0 + 30.0);
        float surf_detail = fbm(uv_sphere * 8.0 + 60.0);
        vec3 base_col = mix(vec3(0.02, 0.05, 0.15), vec3(0.05, 0.12, 0.3), surf_n);

        // Bands with turbulence
        float bands = sin(uv_sphere.y * 12.0 + surf_n * 3.0 + surf_detail * 1.5) * 0.5 + 0.5;
        base_col = mix(base_col, vec3(0.08, 0.15, 0.35), bands * 0.3);

        // Storm spots
        float storm = 1.0 - smoothstep(0.0, 0.08, length(uv_sphere - vec2(-0.3, 0.2)));
        base_col = mix(base_col, vec3(0.12, 0.18, 0.4), storm * 0.5);

        // Apply lighting with terminator
        col = base_col * (diffuse * 0.8 + 0.1) * terminator;
        // Night-side faint ambient
        col += base_col * 0.02 * (1.0 - terminator);

        // Cloud layer — semi-transparent wisps that rotate slowly
        float cloud_angle = u_time * 0.008;
        vec2 cloud_uv = vec2(
            uv_sphere.x * cos(cloud_angle) - uv_sphere.y * sin(cloud_angle),
            uv_sphere.x * sin(cloud_angle) + uv_sphere.y * cos(cloud_angle)
        );
        float clouds = fbm(cloud_uv * 5.0 + 15.0);
        clouds = smoothstep(0.4, 0.7, clouds);
        vec3 cloud_col = vec3(0.15, 0.22, 0.4) * (diffuse * 0.9 + 0.15) * terminator;
        col = mix(col, cloud_col, clouds * 0.5);

        // Specular highlight — sharp sun reflection on cloud/surface
        vec3 view_dir = vec3(0.0, 0.0, 1.0);
        vec3 half_dir = normalize(light_dir + view_dir);
        float spec = pow(max(0.0, dot(normal, half_dir)), 60.0);
        col += vec3(0.7, 0.85, 1.0) * spec * 0.5 * terminator;

        // Rim / atmosphere scatter on the lit limb
        col += vec3(0.2, 0.5, 1.0) * fresnel * 0.5 * terminator;
        col += vec3(0.4, 0.6, 1.0) * pow(fresnel, 6.0) * 0.7;
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

    // Central glow — warm core fading to blue
    float glow = 0.015 / (dist * dist + 0.015);
    col += mix(vec3(0.6, 0.7, 1.0), vec3(0.3, 0.5, 1.0), smoothstep(0.0, 0.15, dist)) * glow * 0.15;

    // Anamorphic horizontal streak with chromatic separation
    float streak_r = 0.001 / (d.y * d.y + 0.001) * (1.0 - smoothstep(0.0, 0.55, abs(d.x + 0.005)));
    float streak_g = 0.001 / (d.y * d.y + 0.001) * (1.0 - smoothstep(0.0, 0.50, abs(d.x)));
    float streak_b = 0.001 / (d.y * d.y + 0.001) * (1.0 - smoothstep(0.0, 0.45, abs(d.x - 0.005)));
    col += vec3(streak_r * 0.4, streak_g * 0.5, streak_b * 1.0) * 0.03;

    // Ghost circles with chromatic aberration — each color channel offset
    for (int i = 1; i <= 4; i++) {
        float fi = float(i);
        float scale = fi * 0.35 + 0.1;
        vec2 ghost_center = light_pos - d * scale;
        float ghost_radius = 0.04 * fi;

        // Separate R/G/B radii for chromatic fringing
        float ring_r = abs(length(p - ghost_center) - ghost_radius * 1.03);
        float ring_g = abs(length(p - ghost_center) - ghost_radius);
        float ring_b = abs(length(p - ghost_center) - ghost_radius * 0.97);
        float gr = (1.0 - smoothstep(0.0, 0.006, ring_r)) * 0.08 / fi;
        float gg = (1.0 - smoothstep(0.0, 0.006, ring_g)) * 0.08 / fi;
        float gb = (1.0 - smoothstep(0.0, 0.006, ring_b)) * 0.08 / fi;

        // Filled disc behind the ring (faint)
        float disc = (1.0 - smoothstep(0.0, ghost_radius, length(p - ghost_center)));
        col += vec3(0.1, 0.15, 0.3) * disc * 0.02 / fi;
        col += vec3(gr, gg, gb);
    }

    // Iris starburst — hexagonal blade pattern
    float flare_angle = atan(d.y, d.x);
    float blades = abs(sin(flare_angle * 3.0));  // 6-blade pattern
    float iris = blades * (1.0 - smoothstep(0.0, 0.12, dist)) * 0.08;
    col += vec3(0.2, 0.35, 0.8) * iris;

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

        // Curtain width varies with noise — thicker and thinner patches
        float curtain_width = 0.025 + fbm(vec2(p.x * 3.0 + fi * 10.0, time * 0.1)) * 0.02;
        float y_dist = abs(p.y - y_base - wave);
        float ribbon = 1.0 - smoothstep(0.0, curtain_width, y_dist);
        ribbon *= 1.0 - smoothstep(0.3, 0.8, abs(p.x));  // Fade at edges

        // Vertical ray structure — shimmer columns
        float ray_noise = fbm(vec2(p.x * 25.0 + fi * 7.0, time * 0.5 + fi));
        float rays = smoothstep(0.3, 0.6, ray_noise);
        // Fast shimmer within rays
        float shimmer = sin(p.x * 80.0 + time * (3.0 + fi) + fi * 5.0) * 0.3 + 0.7;
        ribbon *= mix(0.4, 1.0, rays * shimmer);

        // Height-dependent fade — brighter at top, fading downward
        float height_fade = smoothstep(y_base - curtain_width, y_base + curtain_width * 0.5, p.y);
        ribbon *= height_fade;

        vec3 ribbon_col;
        if (i == 0) ribbon_col = vec3(0.1, 0.4, 0.9);        // Blue
        else if (i == 1) ribbon_col = vec3(0.05, 0.6, 0.7);   // Cyan
        else ribbon_col = vec3(0.3, 0.15, 0.6);                // Purple

        // Color shift along height — green tint at lower altitude
        vec3 low_col = vec3(0.1, 0.5, 0.2);
        float altitude_mix = smoothstep(y_base - 0.03, y_base + 0.02, p.y);
        ribbon_col = mix(low_col, ribbon_col, altitude_mix);

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
    float core = 1.0 - smoothstep(0.0, 0.002, d);
    float glow = (1.0 - smoothstep(0.0, 0.012, d)) * 0.5;
    // Wider outer glow for atmospheric ionization
    float outer_glow = (1.0 - smoothstep(0.0, 0.035, d)) * 0.15;

    float fade = smoothstep(0.0, 0.15, anim_t) * (1.0 - smoothstep(0.7, 1.0, anim_t));

    // Ionization color gradient: white head → green → blue tail
    float head_dist = length(uv_in - head);
    float tail_dist = length(uv_in - tail);
    float gradient = clamp(head_dist / (head_dist + tail_dist + 0.001), 0.0, 1.0);
    vec3 head_col = vec3(1.0, 1.0, 0.95);           // Hot white
    vec3 mid_col  = vec3(0.4, 0.9, 0.5);             // Ionized green (Mg/Fe)
    vec3 tail_col = vec3(0.2, 0.4, 0.9);             // Cooled blue
    vec3 col = mix(head_col, mid_col, smoothstep(0.0, 0.4, gradient));
    col = mix(col, tail_col, smoothstep(0.4, 1.0, gradient));

    vec3 result = col * (core + glow) * fade;
    result += vec3(0.15, 0.3, 0.6) * outer_glow * fade;

    // Fragment sparks — small particles shed behind
    for (int i = 0; i < 3; i++) {
        float fi = float(i);
        float spark_t = anim_t - fi * 0.06 - 0.05;
        if (spark_t < 0.0 || spark_t > 1.0) continue;
        vec2 spark_pos = start + dir * speed * spark_t;
        // Offset perpendicular to travel direction
        vec2 perp = vec2(-dir.y, dir.x);
        float offset_h = hash11(seed * 31.7 + fi * 17.0) - 0.5;
        spark_pos += perp * offset_h * 0.015;
        float spark_d = length(uv_in - spark_pos);
        float spark = 1.0 - smoothstep(0.0, 0.003, spark_d);
        float spark_fade = (1.0 - smoothstep(0.0, 0.3, anim_t - spark_t));
        result += vec3(1.0, 0.8, 0.5) * spark * spark_fade * fade * 0.4;
    }

    return result;
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

    // Distant galaxies — behind everything else
    col += distant_galaxies(uv);

    // Cosmic dust (returns extinction for dimming stars behind dark nebulae)
    float dust_extinction;
    col += cosmic_dust(centered, u_time, dust_extinction);

    // Nebula
    col += nebula(centered * 1.1, u_time);

    // Aurora ribbons
    col += aurora(centered, u_time);

    // Stars (4 layers with parallax drift) — dimmed by foreground dust
    float star_dim = 1.0 - dust_extinction;
    col += star_layer(uv, 60.0, 0.0, 1.0) * star_dim;
    col += star_layer(uv, 120.0, 2.0, 1.5) * star_dim;
    col += star_layer(uv, 240.0, 4.0, 2.0) * mix(star_dim, 1.0, 0.3);
    col += star_layer(uv, 400.0, 6.0, 3.0) * mix(star_dim, 1.0, 0.5);

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

    // --- Vignette with chromatic aberration ---
    float vig_dist = length(centered);
    float vig = 1.0 - smoothstep(0.4, 1.1, vig_dist);
    col *= mix(0.6, 1.0, vig);
    // Subtle chromatic shift at edges — red shifts outward, blue inward
    float ca_amount = smoothstep(0.3, 0.9, vig_dist) * 0.008;
    float ca_r = length((uv - 0.5) * (1.0 + ca_amount));
    float ca_b = length((uv - 0.5) * (1.0 - ca_amount));
    col.r *= 1.0 + smoothstep(0.3, 0.8, ca_r) * 0.04;
    col.b *= 1.0 + smoothstep(0.3, 0.8, ca_b) * 0.03;

    // --- Final ---
    // ACES-like tone mapping for natural highlight rolloff
    col = col * (2.51 * col + 0.03) / (col * (2.43 * col + 0.59) + 0.14);

    // Subtle film grain for cinematic feel
    float grain = (hash21(uv * u_resolution + u_time * 100.0) - 0.5) * 0.012;
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
