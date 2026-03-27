use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // Use the local proto directory inside crates/proto during package build
    let proto_dir = manifest_dir.join("proto");
    let proto_file = proto_dir.join("hermes/broker/v1/broker.proto");

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let descriptor_path = out_dir.join("hermes_descriptor.bin");

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(&descriptor_path)
        .compile_protos(&[proto_file], &[proto_dir])?;

    Ok(())
}
