use std::{
    collections::HashMap,
    env,
    fs::{self, File},
    io::{self, Write},
    path::Path,
};

pub fn format_help_txt(
    binary_name: &str,
    help_text_path: &str,
    arguments: HashMap<&'static str, &'static str>,
) -> io::Result<()> {
    let help_msg = fs::read_to_string(help_text_path)?;
    let content = super::utils::format(&help_msg, arguments, true);
    // write
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join(binary_name);
    let mut f = File::create(&dest_path)?;
    f.write_all(content.as_bytes())?;
    Ok(())
}
