use crate::protocol;
pub(super) use crate::removal_fixture as fixture;
use crate::test_controller::{EvaluationConfig, Plan, evaluate};

#[path = "../../../../tests/protocol_support/scale_down_model.rs"]
mod shared;

pub use shared::*;
