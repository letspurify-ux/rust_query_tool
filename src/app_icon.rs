use fltk::{enums::ColorDepth, image::RgbImage, prelude::WindowExt};
use miniquad::conf::Icon;

include!("icon_fill.rs");

const BIG_ICON_SIZE: usize = 64;

pub fn miniquad_icon() -> Icon {
    let mut small = [0u8; 16 * 16 * 4];
    let mut medium = [0u8; 32 * 32 * 4];
    let mut big = [0u8; 64 * 64 * 4];

    fill_icon(&mut small, 16);
    fill_icon(&mut medium, 32);
    fill_icon(&mut big, 64);

    Icon { small, medium, big }
}

pub fn apply_window_icon<W: WindowExt>(window: &mut W) {
    if let Some(image) = fltk_icon_image() {
        window.set_icon(Some(image));
    }
}

fn fltk_icon_image() -> Option<RgbImage> {
    let mut pixels = [0u8; BIG_ICON_SIZE * BIG_ICON_SIZE * 4];
    fill_icon(&mut pixels, BIG_ICON_SIZE);
    let image = RgbImage::new(
        &pixels,
        BIG_ICON_SIZE as i32,
        BIG_ICON_SIZE as i32,
        ColorDepth::Rgba8,
    )
    .ok()?;
    Some(image)
}
