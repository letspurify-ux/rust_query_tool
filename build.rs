use std::env;
use std::path::Path;
use std::process::Command;

fn has_system_lib_via_pkg_config(name: &str) -> bool {
    Command::new("pkg-config")
        .args(["--exists", name])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn build_empty_stub(out_dir: &Path, lib_name: &str) -> std::io::Result<()> {
    let src = out_dir.join(format!("{}_stub.c", lib_name));
    std::fs::write(&src, "void space_query_x11_stub(void) {}\n")?;

    let mut build = cc::Build::new();
    build.file(&src);
    build.warnings(false);
    build.compile(lib_name);
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();

    if target_os == "linux" {
        let out_dir = env::var("OUT_DIR")?;
        let out_path = Path::new(&out_dir).to_path_buf();

        let missing_xinerama = !has_system_lib_via_pkg_config("xinerama");
        let missing_xcursor = !has_system_lib_via_pkg_config("xcursor");
        let missing_xfixes = !has_system_lib_via_pkg_config("xfixes");
        let missing_xft = !has_system_lib_via_pkg_config("xft");

        if missing_xinerama {
            build_empty_stub(&out_path, "Xinerama")?;
        }
        if missing_xcursor {
            build_empty_stub(&out_path, "Xcursor")?;
        }
        if missing_xfixes {
            build_empty_stub(&out_path, "Xfixes")?;
        }
        if missing_xft {
            build_empty_stub(&out_path, "Xft")?;
        }

        if missing_xinerama || missing_xcursor || missing_xfixes || missing_xft {
            println!("cargo:warning=Missing X11 dev libs detected; linking local stubs for test/build in this environment.");
            println!("cargo:rustc-link-search=native={}", out_path.display());
        }
    }

    if target_os == "windows" {
        if let Err(e) = embed_windows_icon() {
            println!("cargo:warning=Failed to embed Windows icon resource: {}", e);
        }
    }

    Ok(())
}

// ── Windows icon embedding ────────────────────────────────────────────────

fn embed_windows_icon() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = env::var("OUT_DIR")?;
    let ico_path = Path::new(&out_dir).join("space_query.ico");
    write_ico_file(&ico_path)?;

    let mut res = winres::WindowsResource::new();
    res.set_icon(ico_path.to_str().ok_or("icon path is not valid UTF-8")?);
    res.compile()?;
    Ok(())
}

/// Write a multi-size ICO file to `path` using programmatically generated pixels.
fn write_ico_file(path: &Path) -> std::io::Result<()> {
    use std::io::Write;

    const SIZES: &[usize] = &[16, 32, 48, 256];

    // Generate RGBA pixel data for each size.
    let images: Vec<Vec<u8>> = SIZES
        .iter()
        .map(|&sz| {
            let mut buf = vec![0u8; sz * sz * 4];
            fill_icon_bld(&mut buf, sz);
            buf
        })
        .collect();

    // Encode each image as a BMP DIB (used inside ICO entries).
    let dibs: Vec<Vec<u8>> = images
        .iter()
        .zip(SIZES.iter())
        .map(|(rgba, &sz)| encode_bmp_dib(rgba, sz))
        .collect();

    // Compute image offsets within the ICO file.
    let header_size = 6 + SIZES.len() * 16;
    let mut offset = header_size as u32;
    let mut offsets = Vec::with_capacity(SIZES.len());
    for dib in &dibs {
        offsets.push(offset);
        offset += dib.len() as u32;
    }

    let mut f = std::fs::File::create(path)?;

    // ICO header: reserved=0, type=1 (icon), count=N
    f.write_all(&0u16.to_le_bytes())?;
    f.write_all(&1u16.to_le_bytes())?;
    f.write_all(&(SIZES.len() as u16).to_le_bytes())?;

    // ICONDIRENTRY × N
    for (i, &sz) in SIZES.iter().enumerate() {
        let w = if sz == 256 { 0u8 } else { sz as u8 };
        let h = if sz == 256 { 0u8 } else { sz as u8 };
        f.write_all(&[w, h, 0u8, 0u8])?; // width, height, colorCount, reserved
        f.write_all(&1u16.to_le_bytes())?; // planes
        f.write_all(&32u16.to_le_bytes())?; // bitCount
        f.write_all(&(dibs[i].len() as u32).to_le_bytes())?; // bytesInRes
        f.write_all(&offsets[i].to_le_bytes())?; // imageOffset
    }

    // Image data
    for dib in &dibs {
        f.write_all(dib)?;
    }

    Ok(())
}

/// Encode RGBA pixel data as a 32-bit BMP DIB suitable for embedding in an ICO.
/// Rows are written bottom-to-top (BMP convention) and channels are reordered to BGRA.
fn encode_bmp_dib(rgba: &[u8], size: usize) -> Vec<u8> {
    // AND mask: 1-bit per pixel, padded to DWORD rows.  All zeros = fully opaque
    // at the legacy mask level; real transparency is carried by the alpha channel.
    let and_row_stride = ((size + 31) / 32) * 4;
    let and_size = and_row_stride * size;
    let total = 40 + size * size * 4 + and_size;
    let mut out = Vec::with_capacity(total);

    // BITMAPINFOHEADER (40 bytes)
    out.extend_from_slice(&40u32.to_le_bytes()); // biSize
    out.extend_from_slice(&(size as i32).to_le_bytes()); // biWidth
    out.extend_from_slice(&((size * 2) as i32).to_le_bytes()); // biHeight (*2 = BMP-in-ICO)
    out.extend_from_slice(&1u16.to_le_bytes()); // biPlanes
    out.extend_from_slice(&32u16.to_le_bytes()); // biBitCount
    out.extend_from_slice(&0u32.to_le_bytes()); // biCompression (BI_RGB, BGRA in practice)
    out.extend_from_slice(&0u32.to_le_bytes()); // biSizeImage
    out.extend_from_slice(&0i32.to_le_bytes()); // biXPelsPerMeter
    out.extend_from_slice(&0i32.to_le_bytes()); // biYPelsPerMeter
    out.extend_from_slice(&0u32.to_le_bytes()); // biClrUsed
    out.extend_from_slice(&0u32.to_le_bytes()); // biClrImportant

    // Pixel data: BGRA, bottom-to-top row order
    for row in (0..size).rev() {
        for col in 0..size {
            let idx = (row * size + col) * 4;
            out.push(rgba[idx + 2]); // B
            out.push(rgba[idx + 1]); // G
            out.push(rgba[idx]); // R
            out.push(rgba[idx + 3]); // A
        }
    }

    // AND mask (all zeros)
    out.extend(std::iter::repeat(0u8).take(and_size));

    out
}

// ── Icon pixel generation (shared with src/app_icon.rs via icon_fill.rs) ──
// Re-named to avoid collision with the crate's fill_icon in the same binary.

include!("src/icon_fill.rs");

// build.rs sees `fill_icon` from the include above, but we call it through
// a thin wrapper so the name is unambiguous within this file.
fn fill_icon_bld(buf: &mut [u8], size: usize) {
    fill_icon(buf, size);
}
