#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    kuberic_controller::default_main().await
}
