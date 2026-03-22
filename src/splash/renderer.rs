use miniquad::*;
use std::time::Instant;

use super::shaders;

const TITLE_TEXTURE_WIDTH: u16 = 1280;
const TITLE_TEXTURE_HEIGHT: u16 = 256;
const TITLE_TEXT: &str = "SPACE QUERY";
const SUBTITLE_TEXT: &str = "BUILT WITH RUST";
const VERSION_TEXT: &str = concat!("V", env!("CARGO_PKG_VERSION"));

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

fn glyph_rows(ch: char) -> [u16; 9] {
    match ch {
        'A' => [
            0b0011100,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0111110,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0000000,
        ],
        'B' => [
            0b0111100,
            0b0100010,
            0b0100010,
            0b0111100,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0111100,
            0b0000000,
        ],
        'C' => [
            0b0011100,
            0b0100010,
            0b0100000,
            0b0100000,
            0b0100000,
            0b0100000,
            0b0100010,
            0b0011100,
            0b0000000,
        ],
        'D' => [
            0b0111000,
            0b0100100,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0100100,
            0b0111000,
            0b0000000,
        ],
        'E' => [
            0b0111110,
            0b0100000,
            0b0100000,
            0b0111100,
            0b0100000,
            0b0100000,
            0b0100000,
            0b0111110,
            0b0000000,
        ],
        'F' => [
            0b0111110,
            0b0100000,
            0b0100000,
            0b0111100,
            0b0100000,
            0b0100000,
            0b0100000,
            0b0100000,
            0b0000000,
        ],
        'G' => [
            0b0011100,
            0b0100010,
            0b0100000,
            0b0100000,
            0b0100110,
            0b0100010,
            0b0100010,
            0b0011100,
            0b0000000,
        ],
        'H' => [
            0b0100010,
            0b0100010,
            0b0100010,
            0b0111110,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0000000,
        ],
        'I' => [
            0b0111110,
            0b0001000,
            0b0001000,
            0b0001000,
            0b0001000,
            0b0001000,
            0b0001000,
            0b0111110,
            0b0000000,
        ],
        'L' => [
            0b0100000,
            0b0100000,
            0b0100000,
            0b0100000,
            0b0100000,
            0b0100000,
            0b0100000,
            0b0111110,
            0b0000000,
        ],
        'M' => [
            0b0100010,
            0b0110110,
            0b0101010,
            0b0101010,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0000000,
        ],
        'N' => [
            0b0100010,
            0b0110010,
            0b0101010,
            0b0101010,
            0b0100110,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0000000,
        ],
        'O' => [
            0b0011100,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0011100,
            0b0000000,
        ],
        'P' => [
            0b0111100,
            0b0100010,
            0b0100010,
            0b0111100,
            0b0100000,
            0b0100000,
            0b0100000,
            0b0100000,
            0b0000000,
        ],
        'Q' => [
            0b0011100,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0101010,
            0b0100100,
            0b0011010,
            0b0000000,
        ],
        'R' => [
            0b0111100,
            0b0100010,
            0b0100010,
            0b0111100,
            0b0101000,
            0b0100100,
            0b0100010,
            0b0100010,
            0b0000000,
        ],
        'S' => [
            0b0011110,
            0b0100000,
            0b0100000,
            0b0011100,
            0b0000010,
            0b0000010,
            0b0000010,
            0b0111100,
            0b0000000,
        ],
        'T' => [
            0b0111110,
            0b0001000,
            0b0001000,
            0b0001000,
            0b0001000,
            0b0001000,
            0b0001000,
            0b0001000,
            0b0000000,
        ],
        'U' => [
            0b0100010,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0011100,
            0b0000000,
        ],
        'V' => [
            0b0100010,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0010100,
            0b0010100,
            0b0001000,
            0b0000000,
        ],
        'W' => [
            0b0100010,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0101010,
            0b0101010,
            0b0110110,
            0b0100010,
            0b0000000,
        ],
        'X' => [
            0b0100010,
            0b0100010,
            0b0010100,
            0b0001000,
            0b0001000,
            0b0010100,
            0b0100010,
            0b0100010,
            0b0000000,
        ],
        'Y' => [
            0b0100010,
            0b0100010,
            0b0010100,
            0b0001000,
            0b0001000,
            0b0001000,
            0b0001000,
            0b0001000,
            0b0000000,
        ],
        '0' => [
            0b0011100,
            0b0100010,
            0b0100110,
            0b0101010,
            0b0110010,
            0b0100010,
            0b0100010,
            0b0011100,
            0b0000000,
        ],
        '1' => [
            0b0001000,
            0b0011000,
            0b0001000,
            0b0001000,
            0b0001000,
            0b0001000,
            0b0001000,
            0b0011100,
            0b0000000,
        ],
        '2' => [
            0b0011100,
            0b0100010,
            0b0000010,
            0b0000100,
            0b0001000,
            0b0010000,
            0b0100000,
            0b0111110,
            0b0000000,
        ],
        '3' => [
            0b0011100,
            0b0100010,
            0b0000010,
            0b0001100,
            0b0000010,
            0b0000010,
            0b0100010,
            0b0011100,
            0b0000000,
        ],
        '4' => [
            0b0000100,
            0b0001100,
            0b0010100,
            0b0100100,
            0b0111110,
            0b0000100,
            0b0000100,
            0b0000100,
            0b0000000,
        ],
        '5' => [
            0b0111110,
            0b0100000,
            0b0100000,
            0b0111100,
            0b0000010,
            0b0000010,
            0b0100010,
            0b0011100,
            0b0000000,
        ],
        '6' => [
            0b0011100,
            0b0100000,
            0b0100000,
            0b0111100,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0011100,
            0b0000000,
        ],
        '7' => [
            0b0111110,
            0b0000010,
            0b0000100,
            0b0001000,
            0b0001000,
            0b0010000,
            0b0010000,
            0b0010000,
            0b0000000,
        ],
        '8' => [
            0b0011100,
            0b0100010,
            0b0100010,
            0b0011100,
            0b0100010,
            0b0100010,
            0b0100010,
            0b0011100,
            0b0000000,
        ],
        '9' => [
            0b0011100,
            0b0100010,
            0b0100010,
            0b0011110,
            0b0000010,
            0b0000010,
            0b0000010,
            0b0011100,
            0b0000000,
        ],
        '.' => [
            0b0000000,
            0b0000000,
            0b0000000,
            0b0000000,
            0b0000000,
            0b0000000,
            0b0011000,
            0b0011000,
            0b0000000,
        ],
        ' ' => [0; 9],
        _ => [0; 9],
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
    // Two-pass separable Gaussian blur (radius 4) for smoother glow
    const RADIUS: usize = 4;
    const KERNEL: [f32; 9] = [
        0.028, 0.060, 0.102, 0.145, 0.165, 0.145, 0.102, 0.060, 0.028,
    ];

    // Horizontal pass
    let mut temp = vec![0u8; width * height];
    for y in 0..height {
        for x in 0..width {
            let mut sum = 0.0f32;
            for k in 0..KERNEL.len() {
                let sx = (x as isize + k as isize - RADIUS as isize)
                    .max(0)
                    .min(width as isize - 1) as usize;
                let idx = y * width + sx;
                sum += mask.get(idx).copied().unwrap_or(0) as f32 * KERNEL[k];
            }
            temp[y * width + x] = (sum.round() as u32).min(255) as u8;
        }
    }

    // Vertical pass
    let mut blurred = vec![0u8; width * height];
    for y in 0..height {
        for x in 0..width {
            let mut sum = 0.0f32;
            for k in 0..KERNEL.len() {
                let sy = (y as isize + k as isize - RADIUS as isize)
                    .max(0)
                    .min(height as isize - 1) as usize;
                let idx = sy * width + x;
                sum += temp.get(idx).copied().unwrap_or(0) as f32 * KERNEL[k];
            }
            blurred[y * width + x] = (sum.round() as u32).min(255) as u8;
        }
    }

    blurred
}

fn text_columns(text: &str) -> usize {
    text.chars().count().saturating_mul(8).saturating_sub(1)
}

const GLYPH_ROWS: usize = 9;
const GLYPH_COLS: usize = 7;
const PADDED_GLYPH_ROWS: usize = GLYPH_ROWS + 2;
const PADDED_GLYPH_COLS: usize = GLYPH_COLS + 2;

#[derive(Clone, Copy)]
enum TriangleCutCorner {
    TopLeft,
    TopRight,
    BottomLeft,
    BottomRight,
}

fn should_skip_glyph_triangle_fill(
    ch: char,
    row: usize,
    col: usize,
    cut_corner: TriangleCutCorner,
) -> bool {
    matches!(
        (ch, row, col, cut_corner),
        ('R', 5, 3, TriangleCutCorner::BottomLeft) | ('R', 6, 4, TriangleCutCorner::BottomLeft)
    )
}

fn fill_mask_pixel(mask: &mut [u8], width: usize, height: usize, x: usize, y: usize, alpha: u8) {
    if x >= width || y >= height {
        return;
    }

    let idx = y.saturating_mul(width).saturating_add(x);
    if let Some(cell) = mask.get_mut(idx) {
        *cell = (*cell).max(alpha);
    }
}

fn fill_mask_rect(
    mask: &mut [u8],
    width: usize,
    height: usize,
    origin_x: usize,
    origin_y: usize,
    rect_width: usize,
    rect_height: usize,
    alpha: u8,
) {
    let max_y = origin_y.saturating_add(rect_height).min(height);
    let max_x = origin_x.saturating_add(rect_width).min(width);

    for py in origin_y..max_y {
        for px in origin_x..max_x {
            fill_mask_pixel(mask, width, height, px, py, alpha);
        }
    }
}

fn triangle_edge(ax: f32, ay: f32, bx: f32, by: f32, px: f32, py: f32) -> f32 {
    (px - ax) * (by - ay) - (py - ay) * (bx - ax)
}

fn fill_mask_triangle(
    mask: &mut [u8],
    width: usize,
    height: usize,
    vertices: [(f32, f32); 3],
    alpha: u8,
) {
    let min_x = vertices
        .iter()
        .map(|(x, _)| *x)
        .fold(f32::INFINITY, f32::min)
        .floor()
        .max(0.0) as usize;
    let max_x = vertices
        .iter()
        .map(|(x, _)| *x)
        .fold(f32::NEG_INFINITY, f32::max)
        .ceil()
        .min(width as f32) as usize;
    let min_y = vertices
        .iter()
        .map(|(_, y)| *y)
        .fold(f32::INFINITY, f32::min)
        .floor()
        .max(0.0) as usize;
    let max_y = vertices
        .iter()
        .map(|(_, y)| *y)
        .fold(f32::NEG_INFINITY, f32::max)
        .ceil()
        .min(height as f32) as usize;

    if min_x >= max_x || min_y >= max_y {
        return;
    }

    let area = triangle_edge(
        vertices[0].0,
        vertices[0].1,
        vertices[1].0,
        vertices[1].1,
        vertices[2].0,
        vertices[2].1,
    );
    if area.abs() <= f32::EPSILON {
        return;
    }

    for py in min_y..max_y {
        for px in min_x..max_x {
            let sample_x = px as f32 + 0.5;
            let sample_y = py as f32 + 0.5;

            let w0 = triangle_edge(
                vertices[1].0,
                vertices[1].1,
                vertices[2].0,
                vertices[2].1,
                sample_x,
                sample_y,
            );
            let w1 = triangle_edge(
                vertices[2].0,
                vertices[2].1,
                vertices[0].0,
                vertices[0].1,
                sample_x,
                sample_y,
            );
            let w2 = triangle_edge(
                vertices[0].0,
                vertices[0].1,
                vertices[1].0,
                vertices[1].1,
                sample_x,
                sample_y,
            );

            let inside = if area > 0.0 {
                w0 >= 0.0 && w1 >= 0.0 && w2 >= 0.0
            } else {
                w0 <= 0.0 && w1 <= 0.0 && w2 <= 0.0
            };

            if inside {
                fill_mask_pixel(mask, width, height, px, py, alpha);
            }
        }
    }
}

fn draw_cell_corner_triangle(
    mask: &mut [u8],
    width: usize,
    height: usize,
    origin_x: usize,
    origin_y: usize,
    scale: usize,
    cut_corner: TriangleCutCorner,
    alpha: u8,
) {
    let x0 = origin_x as f32;
    let y0 = origin_y as f32;
    let x1 = origin_x.saturating_add(scale) as f32;
    let y1 = origin_y.saturating_add(scale) as f32;

    let vertices = match cut_corner {
        TriangleCutCorner::TopLeft => [(x1, y0), (x0, y1), (x1, y1)],
        TriangleCutCorner::TopRight => [(x0, y0), (x0, y1), (x1, y1)],
        TriangleCutCorner::BottomLeft => [(x0, y0), (x1, y0), (x1, y1)],
        TriangleCutCorner::BottomRight => [(x0, y0), (x1, y0), (x0, y1)],
    };

    fill_mask_triangle(mask, width, height, vertices, alpha);
}

fn glyph_cell_filled(rows: &[u16; 9], row: usize, col: usize) -> bool {
    let Some(row_bits) = rows.get(row).copied() else {
        return false;
    };
    if col >= GLYPH_COLS {
        return false;
    }

    let mask_bit = 1u16 << (6usize.saturating_sub(col));
    row_bits & mask_bit != 0
}

fn padded_glyph_cell_filled(rows: &[u16; 9], padded_row: usize, padded_col: usize) -> bool {
    if padded_row == 0 || padded_col == 0 || padded_row > GLYPH_ROWS || padded_col > GLYPH_COLS {
        return false;
    }

    glyph_cell_filled(rows, padded_row - 1, padded_col - 1)
}

fn build_glyph_exterior_map(rows: &[u16; 9]) -> [bool; GLYPH_ROWS * GLYPH_COLS] {
    let mut exterior = [false; GLYPH_ROWS * GLYPH_COLS];
    let mut visited = [false; PADDED_GLYPH_ROWS * PADDED_GLYPH_COLS];
    let mut stack = vec![(0usize, 0usize)];

    while let Some((row, col)) = stack.pop() {
        if row >= PADDED_GLYPH_ROWS || col >= PADDED_GLYPH_COLS {
            continue;
        }

        let visited_idx = row.saturating_mul(PADDED_GLYPH_COLS).saturating_add(col);
        if visited.get(visited_idx).copied().unwrap_or(false) {
            continue;
        }
        if padded_glyph_cell_filled(rows, row, col) {
            continue;
        }

        if let Some(cell) = visited.get_mut(visited_idx) {
            *cell = true;
        }

        if (1..=GLYPH_ROWS).contains(&row) && (1..=GLYPH_COLS).contains(&col) {
            let glyph_idx = (row - 1).saturating_mul(GLYPH_COLS).saturating_add(col - 1);
            if let Some(cell) = exterior.get_mut(glyph_idx) {
                *cell = true;
            }
        }

        if row > 0 {
            stack.push((row - 1, col));
        }
        if row + 1 < PADDED_GLYPH_ROWS {
            stack.push((row + 1, col));
        }
        if col > 0 {
            stack.push((row, col - 1));
        }
        if col + 1 < PADDED_GLYPH_COLS {
            stack.push((row, col + 1));
        }
    }

    exterior
}

fn glyph_cell_exterior_empty(
    rows: &[u16; 9],
    exterior: &[bool; GLYPH_ROWS * GLYPH_COLS],
    row: usize,
    col: usize,
) -> bool {
    if glyph_cell_filled(rows, row, col) {
        return false;
    }

    let idx = row.saturating_mul(GLYPH_COLS).saturating_add(col);
    exterior.get(idx).copied().unwrap_or(false)
}

fn glyph_cell_exterior_score(
    rows: &[u16; 9],
    exterior: &[bool; GLYPH_ROWS * GLYPH_COLS],
    row: usize,
    col: usize,
) -> usize {
    let mut score = 0usize;

    for row_offset in -1isize..=1 {
        for col_offset in -1isize..=1 {
            if row_offset == 0 && col_offset == 0 {
                continue;
            }

            let next_row = row as isize + row_offset;
            let next_col = col as isize + col_offset;
            if next_row < 0
                || next_col < 0
                || next_row >= GLYPH_ROWS as isize
                || next_col >= GLYPH_COLS as isize
            {
                score = score.saturating_add(1);
                continue;
            }

            let neighbor_row = next_row as usize;
            let neighbor_col = next_col as usize;
            if glyph_cell_exterior_empty(rows, exterior, neighbor_row, neighbor_col) {
                score = score.saturating_add(1);
            }
        }
    }

    score
}

fn draw_glyph_triangle_fills(
    mask: &mut [u8],
    width: usize,
    height: usize,
    ch: char,
    rows: &[u16; 9],
    scale: usize,
    origin_x: usize,
    origin_y: usize,
) {
    let exterior = build_glyph_exterior_map(rows);

    for row in 0..(GLYPH_ROWS - 1) {
        for col in 0..(GLYPH_COLS - 1) {
            let tl = glyph_cell_filled(rows, row, col);
            let tr = glyph_cell_filled(rows, row, col + 1);
            let bl = glyph_cell_filled(rows, row + 1, col);
            let br = glyph_cell_filled(rows, row + 1, col + 1);

            let base_x = origin_x.saturating_add(col.saturating_mul(scale));
            let base_y = origin_y.saturating_add(row.saturating_mul(scale));

            if tl && br && !tr && !bl {
                let tr_score = glyph_cell_exterior_score(rows, &exterior, row, col + 1);
                let bl_score = glyph_cell_exterior_score(rows, &exterior, row + 1, col);
                let tr_exterior = glyph_cell_exterior_empty(rows, &exterior, row, col + 1);
                let bl_exterior = glyph_cell_exterior_empty(rows, &exterior, row + 1, col);

                if tr_exterior && (!bl_exterior || tr_score > bl_score) {
                    if !should_skip_glyph_triangle_fill(ch, row, col, TriangleCutCorner::TopRight) {
                        draw_cell_corner_triangle(
                            mask,
                            width,
                            height,
                            base_x.saturating_add(scale),
                            base_y,
                            scale,
                            TriangleCutCorner::TopRight,
                            255,
                        );
                    }
                } else if bl_exterior && (!tr_exterior || bl_score > tr_score) {
                    if !should_skip_glyph_triangle_fill(ch, row, col, TriangleCutCorner::BottomLeft)
                    {
                        draw_cell_corner_triangle(
                            mask,
                            width,
                            height,
                            base_x,
                            base_y.saturating_add(scale),
                            scale,
                            TriangleCutCorner::BottomLeft,
                            255,
                        );
                    }
                }
            } else if tr && bl && !tl && !br {
                let tl_score = glyph_cell_exterior_score(rows, &exterior, row, col);
                let br_score = glyph_cell_exterior_score(rows, &exterior, row + 1, col + 1);
                let tl_exterior = glyph_cell_exterior_empty(rows, &exterior, row, col);
                let br_exterior = glyph_cell_exterior_empty(rows, &exterior, row + 1, col + 1);

                if tl_exterior && (!br_exterior || tl_score > br_score) {
                    if !should_skip_glyph_triangle_fill(ch, row, col, TriangleCutCorner::TopLeft) {
                        draw_cell_corner_triangle(
                            mask,
                            width,
                            height,
                            base_x,
                            base_y,
                            scale,
                            TriangleCutCorner::TopLeft,
                            255,
                        );
                    }
                } else if br_exterior && (!tl_exterior || br_score > tl_score) {
                    if !should_skip_glyph_triangle_fill(
                        ch,
                        row,
                        col,
                        TriangleCutCorner::BottomRight,
                    ) {
                        draw_cell_corner_triangle(
                            mask,
                            width,
                            height,
                            base_x.saturating_add(scale),
                            base_y.saturating_add(scale),
                            scale,
                            TriangleCutCorner::BottomRight,
                            255,
                        );
                    }
                }
            }
        }
    }
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
    let letter_advance = 8usize.saturating_mul(scale);

    for (glyph_index, ch) in text.chars().enumerate() {
        let rows = glyph_rows(ch);
        let glyph_origin_x = origin_x.saturating_add(glyph_index.saturating_mul(letter_advance));

        for (row_index, row_bits) in rows.iter().enumerate() {
            for col_index in 0..GLYPH_COLS {
                let mask_bit = 1u16 << (6usize.saturating_sub(col_index));
                if row_bits & mask_bit == 0 {
                    continue;
                }

                let x = glyph_origin_x.saturating_add(col_index.saturating_mul(scale));
                let y = origin_y.saturating_add(row_index.saturating_mul(scale));
                fill_mask_rect(mask, width, height, x, y, scale, scale, 255);
            }
        }

        draw_glyph_triangle_fills(
            mask,
            width,
            height,
            ch,
            &rows,
            scale,
            glyph_origin_x,
            origin_y,
        );
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
    let title_scale_y = title_usable_height / GLYPH_ROWS;
    let title_scale = title_scale_x.min(title_scale_y).max(1);
    let title_width = title_columns.saturating_mul(title_scale);
    let title_height = GLYPH_ROWS.saturating_mul(title_scale);
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
    let subtitle_scale_y = subtitle_usable_height / GLYPH_ROWS;
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

    // Version text — small, below subtitle, right-aligned
    let subtitle_height = GLYPH_ROWS.saturating_mul(subtitle_scale);
    let version_columns = text_columns(VERSION_TEXT);
    let version_scale = subtitle_scale.saturating_sub(1).max(1);
    let version_width = version_columns.saturating_mul(version_scale);
    let version_origin_x = title_right.saturating_sub(version_width);
    let version_gap = height.saturating_mul(2) / 32;
    let version_origin_y = subtitle_origin_y
        .saturating_add(subtitle_height)
        .saturating_add(version_gap);

    // Draw version to a separate mask so we can apply reduced alpha
    let mut version_mask = vec![0u8; width * height];
    draw_text_mask(
        &mut version_mask,
        width,
        height,
        VERSION_TEXT,
        version_scale,
        version_origin_x,
        version_origin_y,
    );
    // Merge version mask at 60% opacity
    for i in 0..mask.len() {
        let v = version_mask.get(i).copied().unwrap_or(0);
        if v > 0 {
            let reduced = ((v as u32).saturating_mul(153) / 255) as u8; // 0.6 * 255 ≈ 153
            if let Some(cell) = mask.get_mut(i) {
                *cell = (*cell).max(reduced);
            }
        }
    }

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
            Vertex {
                position: [-1.0, -1.0],
            },
            Vertex {
                position: [1.0, -1.0],
            },
            Vertex {
                position: [1.0, 1.0],
            },
            Vertex {
                position: [-1.0, 1.0],
            },
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

#[cfg(test)]
mod tests {
    use super::{
        draw_glyph_triangle_fills, glyph_rows, should_skip_glyph_triangle_fill, TriangleCutCorner,
    };

    fn mask_alpha(mask: &[u8], width: usize, x: usize, y: usize) -> u8 {
        let idx = y.saturating_mul(width).saturating_add(x);
        mask.get(idx).copied().unwrap_or(0)
    }

    #[test]
    fn r_lower_diagonal_triangle_fill_is_skipped() {
        let scale = 8usize;
        // 7x9 glyph: width = 7*8 = 56, height = 9*8 = 72
        let width = 64usize;
        let height = 80usize;
        let mut mask = vec![0u8; width * height];

        draw_glyph_triangle_fills(&mut mask, width, height, 'R', &glyph_rows('R'), scale, 0, 0);

        // In 7x9 R, the skip coordinates are (row=5, col=3) and (row=6, col=4).
        // The BottomLeft triangle at (row=5, col=3) would be at pixel (3*8, (5+1)*8) = (24, 48).
        // Check that a pixel in that triangle region is 0.
        assert_eq!(mask_alpha(&mask, width, 25, 49), 0);
    }

    #[test]
    fn r_upper_bowl_triangle_fill_remains() {
        let scale = 8usize;
        let width = 64usize;
        let height = 80usize;
        let mut mask = vec![0u8; width * height];

        draw_glyph_triangle_fills(&mut mask, width, height, 'R', &glyph_rows('R'), scale, 0, 0);

        // In 7x9 R, the bowl area is around row 1-2, col 4-5.
        // Check a triangle fill pixel exists in the upper-right bowl area.
        // The TopRight triangle at (row=0, col=4) would fill at pixel (5*8, 0) area.
        assert_eq!(mask_alpha(&mask, width, 41, 7), 255);
    }

    #[test]
    fn only_r_lower_diagonal_override_is_enabled() {
        assert!(should_skip_glyph_triangle_fill(
            'R',
            5,
            3,
            TriangleCutCorner::BottomLeft
        ));
        assert!(!should_skip_glyph_triangle_fill(
            'R',
            0,
            4,
            TriangleCutCorner::TopRight
        ));
        assert!(!should_skip_glyph_triangle_fill(
            'Q',
            5,
            3,
            TriangleCutCorner::BottomLeft
        ));
    }
}
