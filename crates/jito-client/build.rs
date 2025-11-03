fn main() {
    let protos = vec![
        // "proto/auth.proto",  // Empty
        "proto/bundle.proto",
        "proto/packet.proto",
        "proto/shared.proto",
        "proto/searcher.proto",
        // "proto/block_engine.proto",  // Empty
        // "proto/relayer.proto",  // Empty
    ];
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_protos(&protos, &["proto"]).expect("compile protos");
}


