//! Compiles translations and embeds the icon and version information in
//! Windows executables.

fn main() {
    fastframe_i18n::build::compile_catalogs("assets/i18n");
    translation_helper();

    #[cfg(windows)]
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        println!("cargo:rerun-if-changed=packaging/windows/zapfast.ico");
        let mut resource = winresource::WindowsResource::new();
        resource
            .set_icon("packaging/windows/zapfast.ico")
            .set("ProductName", "ZapFast")
            .set("FileDescription", "ZapFast");
        if let Err(error) = resource.compile() {
            println!("cargo:warning=Windows resources not embedded: {error}");
        }
    }
}

// Embed the helper so portable binaries need no separate installation. Older
// SDKs can still build ZapFast, with chat translation explicitly unavailable.
fn translation_helper() {
    println!("cargo:rerun-if-changed=native/Translation.swift");
    let output =
        std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap()).join("zapfast-translate");
    std::fs::write(&output, []).unwrap();
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos") {
        return;
    }
    let arch = match std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() {
        Ok("aarch64") => "arm64",
        Ok("x86_64") => "x86_64",
        _ => return,
    };
    let result = std::process::Command::new("xcrun")
        .args(["swiftc", "-parse-as-library", "-O", "-target"])
        .arg(format!("{arch}-apple-macos11.0"))
        .arg("native/Translation.swift")
        .arg("-o")
        .arg(&output)
        .output();
    if !result.is_ok_and(|result| result.status.success()) {
        std::fs::write(output, []).unwrap();
        println!("cargo:warning=On-device translation unavailable: build with Xcode 26 or newer.");
    }
}
