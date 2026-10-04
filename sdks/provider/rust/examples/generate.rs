//! Development-only deterministic protobuf regeneration. Consumers do not run this.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: generate <mppi/proto> <output-directory>".into());
    }
    let proto_root = std::path::PathBuf::from(&args[0]);
    let output = std::path::PathBuf::from(&args[1]);
    std::fs::create_dir_all(&output)?;
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        // Keep each streamed payload variant small; the final record is larger.
        // This changes Rust representation only, never protobuf encoding.
        .boxed(".mppi.v1.ChatChunk.kind.terminal")
        .out_dir(output)
        .compile_protos(&[proto_root.join("mppi/v1/mppi.proto")], &[proto_root])?;
    Ok(())
}
