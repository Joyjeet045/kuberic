use std::collections::BTreeMap;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RbacDocument {
    api_version: String,
    kind: String,
    metadata: RbacMetadata,
    #[serde(default)]
    rules: Vec<RbacRule>,
    role_ref: Option<RbacRoleRef>,
    #[serde(default)]
    subjects: Vec<RbacSubject>,
}

#[derive(Debug, Deserialize)]
struct RbacMetadata {
    name: String,
    namespace: String,
    #[serde(default)]
    annotations: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RbacRule {
    api_groups: Vec<String>,
    resources: Vec<String>,
    verbs: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RbacRoleRef {
    api_group: String,
    kind: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct RbacSubject {
    kind: String,
    name: String,
    namespace: String,
}

fn parse_rbac_documents(source: &str) -> Vec<RbacDocument> {
    serde_yaml_ng::Deserializer::from_str(source)
        .map(|document| RbacDocument::deserialize(document).expect("valid RBAC YAML document"))
        .collect()
}

fn assert_least_privilege_rbac(
    source: &str,
    expected_name: &str,
    expected_lifecycle: &str,
    expected_verbs: &[&str],
) {
    let documents = parse_rbac_documents(source);
    assert_eq!(documents.len(), 3);
    assert_eq!(
        documents
            .iter()
            .map(|document| document.kind.as_str())
            .collect::<Vec<_>>(),
        ["ServiceAccount", "Role", "RoleBinding"]
    );

    for document in &documents {
        assert_eq!(document.metadata.name, expected_name);
        assert_eq!(document.metadata.namespace, "kuberic-checkpoints");
        assert_eq!(
            document
                .metadata
                .annotations
                .get("kuberic.io/checkpoint-lifecycle")
                .map(String::as_str),
            Some(expected_lifecycle)
        );
        assert!(!document.api_version.contains('*'));
    }

    assert_eq!(documents[0].api_version, "v1");
    assert!(documents[0].rules.is_empty());
    assert!(documents[0].role_ref.is_none());
    assert!(documents[0].subjects.is_empty());

    let role = &documents[1];
    assert_eq!(role.api_version, "rbac.authorization.k8s.io/v1");
    assert_eq!(role.rules.len(), 1);
    assert_eq!(role.rules[0].api_groups, [""]);
    assert_eq!(role.rules[0].resources, ["configmaps"]);
    assert_eq!(
        role.rules[0].verbs,
        expected_verbs
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    );
    assert!(
        role.rules[0]
            .api_groups
            .iter()
            .chain(&role.rules[0].resources)
            .chain(&role.rules[0].verbs)
            .all(|entry| entry != "*")
    );

    let binding = &documents[2];
    assert_eq!(binding.api_version, "rbac.authorization.k8s.io/v1");
    assert!(binding.rules.is_empty());
    let role_ref = binding.role_ref.as_ref().expect("RoleBinding roleRef");
    assert_eq!(role_ref.api_group, "rbac.authorization.k8s.io");
    assert_eq!(role_ref.kind, "Role");
    assert_eq!(role_ref.name, expected_name);
    assert_eq!(binding.subjects.len(), 1);
    assert_eq!(binding.subjects[0].kind, "ServiceAccount");
    assert_eq!(binding.subjects[0].name, expected_name);
    assert_eq!(binding.subjects[0].namespace, "kuberic-checkpoints");
}

#[test]
fn checkpoint_rbac_is_structural_and_lifecycle_specific() {
    assert_least_privilege_rbac(
        include_str!("../deploy/checkpoint-writer-rbac.yaml"),
        "kuberic-checkpoint-writer",
        "retained-writer",
        &["get", "create", "update"],
    );
    assert_least_privilege_rbac(
        include_str!("../deploy/checkpoint-cleanup-rbac.yaml"),
        "kuberic-checkpoint-cleanup",
        "explicit-orphan-cleanup",
        &["list", "delete"],
    );
}
