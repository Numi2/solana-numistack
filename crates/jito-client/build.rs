fn main() {
    let protos = vec![
        "proto/auth.proto",
        "proto/bundle.proto",
        "proto/packet.proto",
        "proto/shared.proto",
        "proto/searcher.proto",
        "proto/block_engine.proto",
        "proto/relayer.proto",
    ];
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .compile(&protos, &["proto"]).expect("compile protos");
}


