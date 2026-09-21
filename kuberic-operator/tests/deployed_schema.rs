use kube::CustomResourceExt;
use kuberic_operator::crd::KubericSet;

#[test]
fn deployed_kubericset_crd_matches_rust_api() {
    let documents: Vec<serde_json::Value> =
        serde_saphyr::from_multiple(include_str!("../deploy/deployment.yaml")).unwrap();
    let crds: Vec<_> = documents
        .iter()
        .filter(|document| {
            document["kind"] == "CustomResourceDefinition"
                && document["metadata"]["name"] == "kubericsets.kuberic.io"
        })
        .collect();
    assert_eq!(crds.len(), 1);
    let mut generated = serde_json::to_value(KubericSet::crd()).unwrap();
    generated["spec"]["names"]["listKind"] = serde_json::json!("KubericSetList");
    assert_eq!(
        crds[0], &generated,
        "run cargo run -p kuberic-operator --example generate-crd"
    );
}

#[test]
fn primary_balancing_documentation_examples_match_the_api() {
    let document = include_str!("../../docs/features/primary-balancing.md").replace("\r\n", "\n");
    let mut topology_keys = std::collections::BTreeSet::new();
    for block in document.split("```yaml\n").skip(1) {
        let yaml = block.split("```").next().unwrap();
        let set: KubericSet = serde_saphyr::from_str(yaml).unwrap();
        set.spec.scheduling.as_ref().unwrap().validate().unwrap();
        let policy = set.spec.primary_balancing.as_ref().unwrap();
        policy.validate().unwrap();
        topology_keys.insert(policy.topology_key.clone());
    }
    assert!(topology_keys.contains("kubernetes.io/hostname"));
    assert!(topology_keys.contains("topology.kubernetes.io/zone"));
}
