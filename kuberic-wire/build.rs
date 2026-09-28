fn main() {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .boxed(".kuberic.level.v1.ExecuteCommandRequest.command.ensure_configuration")
        .boxed(".kuberic.level.v1.ExecuteCommandRequest.command.prepare_secondary_removal")
        .boxed(".kuberic.level.v1.ExecuteCommandRequest.command.retire_replica")
        .boxed(".kuberic.level.v1.ExecuteCommandRequest.command.accept_secondary_removal_commit")
        .boxed(".kuberic.level.v1.ScaleUpConfigurationEvidence.evidence.admission")
        .boxed(".kuberic.level.v1.ScaleUpConfigurationEvidence.evidence.failover")
        .compile_protos(
            &["proto/kuberic.proto", "proto/replication.proto"],
            &["proto"],
        )
        .expect("failed to compile level-triggered Kuberic protobuf schema");
}
