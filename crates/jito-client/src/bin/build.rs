
fn main() {
    let proto_dir = "vendor/carbon-jito-protos/protos";

    let files = &[
        format!("{proto_dir}/auth.proto"),
        format!("{proto_dir}/bundle.proto"),
        format!("{proto_dir}/packet.proto"),
        format!("{proto_dir}/shared.proto"),
        format!("{proto_dir}/searcher.proto"),
        format!("{proto_dir}/block_engine.proto"),
        format!("{proto_dir}/relayer.proto"),
    ];

    tonic_build::configure()
        .build_transport(true)
        .compile(&files, &[proto_dir])
        .expect("compile jito protos");
}