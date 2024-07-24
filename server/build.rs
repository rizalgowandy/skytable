fn main() -> std::io::Result<()> {
    libsky::build_scripts::format_all_help_txt("skyd", "help_text", Default::default())?;
    if std::env::var("CARGO_CFG_MIRI").is_ok() {
        println!("cargo:rustc-cfg=feature=\"miri\"");
    }
    Ok(())
}
