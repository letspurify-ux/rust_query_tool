#[cfg(feature = "gpu-splash")]
use crate::splash::animation::AnimationState;
#[cfg(feature = "gpu-splash")]
use crate::splash::loading::LoadingState;

#[cfg(feature = "gpu-splash")]
use font8x8::{UnicodeFonts, BASIC_FONTS};

#[cfg(feature = "gpu-splash")]
pub const PANEL_WIDTH: i32 = 404;
#[cfg(feature = "gpu-splash")]
pub const PANEL_HEIGHT: i32 = 164;

#[cfg(feature = "gpu-splash")]
const PANEL_RADIUS: i32 = 18;

#[cfg(feature = "gpu-splash")]
pub struct OverlayComposer {
    app_name: &'static str,
    subtitle: &'static str,
    pixels: Vec<u8>,
}

#[cfg(feature = "gpu-splash")]
pub struct OverlayFrame<'a> {
    pub pixels: &'a [u8],
    pub width: i32,
    pub height: i32,
}

#[cfg(feature = "gpu-splash")]
impl OverlayComposer {
    pub fn new(app_name: &'static str, subtitle: &'static str) -> Self {
        let pixel_count = PANEL_WIDTH
            .max(0)
            .saturating_mul(PANEL_HEIGHT.max(0))
            .saturating_mul(4) as usize;
        Self {
            app_name,
            subtitle,
            pixels: vec![0; pixel_count],
        }
    }

    pub fn compose(
        &mut self,
        loading_state: &LoadingState,
        animation_state: &AnimationState,
    ) -> OverlayFrame<'_> {
        self.pixels.fill(0);

        fill_rounded_rect(
            &mut self.pixels,
            PANEL_WIDTH,
            PANEL_HEIGHT,
            0,
            0,
            PANEL_WIDTH,
            PANEL_HEIGHT,
            PANEL_RADIUS,
            [10, 14, 24, 232],
        );
        stroke_rounded_rect(
            &mut self.pixels,
            PANEL_WIDTH,
            PANEL_HEIGHT,
            0,
            0,
            PANEL_WIDTH,
            PANEL_HEIGHT,
            PANEL_RADIUS,
            1,
            [24, 32, 50, 255],
        );

        fill_rect(
            &mut self.pixels,
            PANEL_WIDTH,
            PANEL_HEIGHT,
            24,
            18,
            PANEL_WIDTH - 48,
            2,
            [32, 46, 78, 255],
        );
        let shimmer = animation_state.shimmer_phase();
        let accent_width = (((PANEL_WIDTH - 48) as f32) * (0.22 + shimmer * 0.18)).round() as i32;
        fill_rect(
            &mut self.pixels,
            PANEL_WIDTH,
            PANEL_HEIGHT,
            24,
            18,
            accent_width.clamp(0, PANEL_WIDTH - 48),
            2,
            [63, 104, 181, 255],
        );

        draw_brand_mark(&mut self.pixels, PANEL_WIDTH, PANEL_HEIGHT, 28, 34, 54);

        let stage = fit_text(&loading_state.stage_label(), PANEL_WIDTH - 56, 1);
        let detail = fit_text(
            &loading_state.detail_label(animation_state.loading_dots()),
            PANEL_WIDTH - 56,
            1,
        );

        draw_text(
            &mut self.pixels,
            PANEL_WIDTH,
            PANEL_HEIGHT,
            102,
            32,
            self.app_name,
            3,
            [232, 238, 248, 255],
        );
        draw_text(
            &mut self.pixels,
            PANEL_WIDTH,
            PANEL_HEIGHT,
            102,
            64,
            self.subtitle,
            1,
            [150, 164, 186, 255],
        );
        draw_text(
            &mut self.pixels,
            PANEL_WIDTH,
            PANEL_HEIGHT,
            28,
            98,
            &stage,
            1,
            [120, 140, 170, 255],
        );
        draw_text(
            &mut self.pixels,
            PANEL_WIDTH,
            PANEL_HEIGHT,
            28,
            118,
            &detail,
            1,
            [208, 214, 224, 255],
        );

        let bar_x = 28;
        let bar_y = PANEL_HEIGHT - 28;
        let bar_width = PANEL_WIDTH - 56;
        let filled =
            ((bar_width as f32) * loading_state.display_progress().clamp(0.0, 1.0)).round() as i32;
        fill_rect(
            &mut self.pixels,
            PANEL_WIDTH,
            PANEL_HEIGHT,
            bar_x,
            bar_y,
            bar_width,
            3,
            [24, 28, 40, 255],
        );
        fill_rect(
            &mut self.pixels,
            PANEL_WIDTH,
            PANEL_HEIGHT,
            bar_x,
            bar_y,
            filled.clamp(0, bar_width),
            3,
            [70, 112, 186, 255],
        );
        if filled > 10 {
            let glint_width = 26.min(filled);
            let glint_offset = (((filled - glint_width) as f32) * shimmer).round() as i32;
            fill_rect(
                &mut self.pixels,
                PANEL_WIDTH,
                PANEL_HEIGHT,
                bar_x + glint_offset,
                bar_y,
                glint_width,
                3,
                [140, 176, 228, 255],
            );
        }

        OverlayFrame {
            pixels: &self.pixels,
            width: PANEL_WIDTH,
            height: PANEL_HEIGHT,
        }
    }
}

#[cfg(feature = "gpu-splash")]
fn fit_text(text: &str, max_width: i32, scale: i32) -> String {
    if measure_text(text, scale) <= max_width {
        return text.to_string();
    }

    let ellipsis = "...";
    let mut fitted = String::new();
    for ch in text.chars() {
        let mut candidate = fitted.clone();
        candidate.push(ch);
        candidate.push_str(ellipsis);
        if measure_text(&candidate, scale) > max_width {
            break;
        }
        fitted.push(ch);
    }

    if fitted.is_empty() {
        ellipsis.to_string()
    } else {
        fitted.push_str(ellipsis);
        fitted
    }
}

#[cfg(feature = "gpu-splash")]
fn measure_text(text: &str, scale: i32) -> i32 {
    let advance = glyph_advance(scale);
    let glyph_count = text.chars().count() as i32;
    if glyph_count <= 0 {
        0
    } else {
        glyph_count
            .saturating_mul(advance)
            .saturating_sub(scale.max(1))
    }
}

#[cfg(feature = "gpu-splash")]
fn glyph_advance(scale: i32) -> i32 {
    8_i32
        .saturating_mul(scale.max(1))
        .saturating_add(scale.max(1))
}

#[cfg(feature = "gpu-splash")]
fn draw_text(
    pixels: &mut [u8],
    width: i32,
    height: i32,
    x: i32,
    y: i32,
    text: &str,
    scale: i32,
    color: [u8; 4],
) {
    let mut cursor_x = x;
    let advance = glyph_advance(scale);
    for ch in text.chars() {
        draw_glyph(pixels, width, height, cursor_x, y, ch, scale, color);
        cursor_x = cursor_x.saturating_add(advance);
    }
}

#[cfg(feature = "gpu-splash")]
fn draw_glyph(
    pixels: &mut [u8],
    width: i32,
    height: i32,
    x: i32,
    y: i32,
    ch: char,
    scale: i32,
    color: [u8; 4],
) {
    let glyph = BASIC_FONTS.get(ch).or_else(|| BASIC_FONTS.get('?'));

    if let Some(rows) = glyph {
        for (row_index, bits) in rows.iter().enumerate() {
            for column in 0..8 {
                if bits & (1 << column) == 0 {
                    continue;
                }

                fill_rect(
                    pixels,
                    width,
                    height,
                    x.saturating_add(column * scale.max(1)),
                    y.saturating_add(row_index as i32 * scale.max(1)),
                    scale.max(1),
                    scale.max(1),
                    color,
                );
            }
        }
    }
}

#[cfg(feature = "gpu-splash")]
fn draw_brand_mark(pixels: &mut [u8], width: i32, height: i32, x: i32, y: i32, size: i32) {
    let center_x = x + size / 2;
    let center_y = y + size / 2;
    draw_circle_stroke(
        pixels,
        width,
        height,
        center_x,
        center_y,
        size / 2 - 1,
        2,
        [92, 136, 214, 255],
    );
    draw_circle_stroke(
        pixels,
        width,
        height,
        center_x,
        center_y,
        size / 2 - 10,
        2,
        [44, 74, 128, 200],
    );
    fill_circle(
        pixels,
        width,
        height,
        center_x,
        center_y,
        size / 2 - 12,
        [12, 18, 32, 255],
    );
    fill_circle(
        pixels,
        width,
        height,
        x + size - 13,
        y + 16,
        6,
        [205, 224, 255, 255],
    );
    draw_text(
        pixels,
        width,
        height,
        x + 12,
        y + 18,
        "SQ",
        1,
        [114, 150, 214, 255],
    );
}

#[cfg(feature = "gpu-splash")]
fn fill_rect(
    pixels: &mut [u8],
    width: i32,
    height: i32,
    x: i32,
    y: i32,
    rect_width: i32,
    rect_height: i32,
    color: [u8; 4],
) {
    if rect_width <= 0 || rect_height <= 0 {
        return;
    }

    let start_x = x.max(0);
    let start_y = y.max(0);
    let end_x = x.saturating_add(rect_width).min(width);
    let end_y = y.saturating_add(rect_height).min(height);
    for py in start_y..end_y {
        for px in start_x..end_x {
            blend_pixel(pixels, width, height, px, py, color);
        }
    }
}

#[cfg(feature = "gpu-splash")]
fn fill_circle(
    pixels: &mut [u8],
    width: i32,
    height: i32,
    center_x: i32,
    center_y: i32,
    radius: i32,
    color: [u8; 4],
) {
    if radius <= 0 {
        return;
    }

    let radius_squared = radius.saturating_mul(radius);
    for py in (center_y - radius).max(0)..=(center_y + radius).min(height - 1) {
        for px in (center_x - radius).max(0)..=(center_x + radius).min(width - 1) {
            let dx = px - center_x;
            let dy = py - center_y;
            if dx.saturating_mul(dx).saturating_add(dy.saturating_mul(dy)) <= radius_squared {
                blend_pixel(pixels, width, height, px, py, color);
            }
        }
    }
}

#[cfg(feature = "gpu-splash")]
fn draw_circle_stroke(
    pixels: &mut [u8],
    width: i32,
    height: i32,
    center_x: i32,
    center_y: i32,
    radius: i32,
    thickness: i32,
    color: [u8; 4],
) {
    if radius <= 0 || thickness <= 0 {
        return;
    }

    let outer = radius.saturating_mul(radius);
    let inner_radius = radius.saturating_sub(thickness);
    let inner = inner_radius.saturating_mul(inner_radius);
    for py in (center_y - radius).max(0)..=(center_y + radius).min(height - 1) {
        for px in (center_x - radius).max(0)..=(center_x + radius).min(width - 1) {
            let dx = px - center_x;
            let dy = py - center_y;
            let distance = dx.saturating_mul(dx).saturating_add(dy.saturating_mul(dy));
            if distance <= outer && distance >= inner {
                blend_pixel(pixels, width, height, px, py, color);
            }
        }
    }
}

#[cfg(feature = "gpu-splash")]
fn fill_rounded_rect(
    pixels: &mut [u8],
    width: i32,
    height: i32,
    x: i32,
    y: i32,
    rect_width: i32,
    rect_height: i32,
    radius: i32,
    color: [u8; 4],
) {
    if rect_width <= 0 || rect_height <= 0 {
        return;
    }

    for py in y.max(0)..(y + rect_height).min(height) {
        for px in x.max(0)..(x + rect_width).min(width) {
            if point_in_rounded_rect(px, py, x, y, rect_width, rect_height, radius) {
                blend_pixel(pixels, width, height, px, py, color);
            }
        }
    }
}

#[cfg(feature = "gpu-splash")]
fn stroke_rounded_rect(
    pixels: &mut [u8],
    width: i32,
    height: i32,
    x: i32,
    y: i32,
    rect_width: i32,
    rect_height: i32,
    radius: i32,
    thickness: i32,
    color: [u8; 4],
) {
    if rect_width <= 0 || rect_height <= 0 || thickness <= 0 {
        return;
    }

    let inner_x = x.saturating_add(thickness);
    let inner_y = y.saturating_add(thickness);
    let inner_width = rect_width.saturating_sub(thickness.saturating_mul(2));
    let inner_height = rect_height.saturating_sub(thickness.saturating_mul(2));
    let inner_radius = radius.saturating_sub(thickness);

    for py in y.max(0)..(y + rect_height).min(height) {
        for px in x.max(0)..(x + rect_width).min(width) {
            let in_outer = point_in_rounded_rect(px, py, x, y, rect_width, rect_height, radius);
            let in_inner = if inner_width > 0 && inner_height > 0 {
                point_in_rounded_rect(
                    px,
                    py,
                    inner_x,
                    inner_y,
                    inner_width,
                    inner_height,
                    inner_radius,
                )
            } else {
                false
            };

            if in_outer && !in_inner {
                blend_pixel(pixels, width, height, px, py, color);
            }
        }
    }
}

#[cfg(feature = "gpu-splash")]
fn point_in_rounded_rect(
    px: i32,
    py: i32,
    x: i32,
    y: i32,
    rect_width: i32,
    rect_height: i32,
    radius: i32,
) -> bool {
    if rect_width <= 0 || rect_height <= 0 {
        return false;
    }

    let local_x = px - x;
    let local_y = py - y;
    if local_x < 0 || local_y < 0 || local_x >= rect_width || local_y >= rect_height {
        return false;
    }

    let bounded_radius = radius.max(0).min(rect_width / 2).min(rect_height / 2);
    if bounded_radius == 0 {
        return true;
    }

    let nearest_x = local_x.clamp(bounded_radius, rect_width - bounded_radius - 1);
    let nearest_y = local_y.clamp(bounded_radius, rect_height - bounded_radius - 1);
    let dx = local_x - nearest_x;
    let dy = local_y - nearest_y;
    dx.saturating_mul(dx).saturating_add(dy.saturating_mul(dy))
        <= bounded_radius.saturating_mul(bounded_radius)
}

#[cfg(feature = "gpu-splash")]
fn blend_pixel(pixels: &mut [u8], width: i32, height: i32, x: i32, y: i32, color: [u8; 4]) {
    if x < 0 || y < 0 || x >= width || y >= height {
        return;
    }

    let index = ((y as usize)
        .saturating_mul(width.max(0) as usize)
        .saturating_add(x as usize))
    .saturating_mul(4);
    if index.saturating_add(3) >= pixels.len() {
        return;
    }

    let src_alpha = color[3] as u32;
    let inv_src_alpha = 255_u32.saturating_sub(src_alpha);

    let dst_r = pixels[index] as u32;
    let dst_g = pixels[index + 1] as u32;
    let dst_b = pixels[index + 2] as u32;
    let dst_a = pixels[index + 3] as u32;

    pixels[index] = ((color[0] as u32)
        .saturating_mul(src_alpha)
        .saturating_add(dst_r.saturating_mul(inv_src_alpha))
        / 255) as u8;
    pixels[index + 1] = ((color[1] as u32)
        .saturating_mul(src_alpha)
        .saturating_add(dst_g.saturating_mul(inv_src_alpha))
        / 255) as u8;
    pixels[index + 2] = ((color[2] as u32)
        .saturating_mul(src_alpha)
        .saturating_add(dst_b.saturating_mul(inv_src_alpha))
        / 255) as u8;
    pixels[index + 3] = src_alpha
        .saturating_add(dst_a.saturating_mul(inv_src_alpha) / 255)
        .min(255) as u8;
}
