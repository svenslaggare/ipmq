#[cfg(feature="c_wrapper")]
fn main() {
    use std::env;

    let target_dir = "target";
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(format!("{}/ipmq.h", target_dir));
}

#[cfg(not(feature="c_wrapper"))]
fn main() {

}