use std::env;

fn main() {
    let target_dir = "target";
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(format!("{}/ipmq.h", target_dir));
}