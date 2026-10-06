use kuberic_controller::evaluator::{EvaluationConfig, evaluate};
use kuberic_controller::plan::Plan;
use kuberic_runtime::protocol;

#[path = "secondary_scale_down.rs"]
pub mod fixture;

#[path = "../../../kuberic-runtime/tests/protocol_support/scale_down_model.rs"]
mod shared;

pub use shared::*;
