use kube::CustomResourceExt;
use kuberic_controller::crd::KubericSet;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", serde_json::to_string_pretty(&KubericSet::crd())?);
    Ok(())
}
