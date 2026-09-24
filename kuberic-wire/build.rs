fn main() {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["proto/kuberic.proto", "proto/replication.proto"],
            &["proto"],
        )
        .expect("failed to compile level-triggered Kuberic protobuf schema");
}
