fn main() -> std::io::Result<()> {
    libsky::build_scripts::format_all_help_txt("skyd", "help_text", Default::default())
}
