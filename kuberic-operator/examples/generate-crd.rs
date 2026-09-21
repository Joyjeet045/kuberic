use kube::CustomResourceExt;
use kuberic_operator::crd::KubericSet;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("deploy")
        .join("deployment.yaml");
    let input = std::fs::read_to_string(&path)?.replace("\r\n", "\n");
    let mut generated = serde_json::to_value(KubericSet::crd())?;
    // Preserve the explicit list kind otherwise defaulted by the API server.
    generated["spec"]["names"]["listKind"] = serde_json::json!("KubericSetList");
    let mut replacements = 0;
    let mut documents = Vec::new();
    for document in input.split("\n---\n") {
        let value: serde_json::Value = serde_saphyr::from_str(document)?;
        if value["kind"] == "CustomResourceDefinition"
            && value["metadata"]["name"] == "kubericsets.kuberic.io"
        {
            documents.push(
                serde_saphyr::to_string(&generated)?
                    .lines()
                    .map(str::trim_end)
                    .collect::<Vec<_>>()
                    .join("\n"),
            );
            replacements += 1;
        } else {
            documents.push(document.to_string());
        }
    }
    if replacements != 1 {
        return Err(format!("expected one KubericSet CRD, found {replacements}").into());
    }
    std::fs::write(&path, documents.join("\n---\n"))?;
    println!("Regenerated {}", path.display());
    Ok(())
}
