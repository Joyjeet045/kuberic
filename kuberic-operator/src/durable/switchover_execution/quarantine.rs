use kuberic_durable_execution::ExactBytes;

use crate::durable::OperationObservations;

use super::{
    model::DirectSwitchoverDefinition,
    prepare::{DirectActivity, DirectEvaluation},
};

pub enum DirectQuarantineOutcome {
    Observe(ExactBytes),
    AwaitEvidence,
}

pub fn resolve_direct_quarantine(
    activity: &DirectActivity,
    definition: &DirectSwitchoverDefinition,
    observations: &OperationObservations,
    now: i64,
) -> Result<DirectQuarantineOutcome, String> {
    match activity.evaluate_quarantine(definition, observations, now)? {
        DirectEvaluation::Observe(result) => Ok(DirectQuarantineOutcome::Observe(result)),
        DirectEvaluation::AwaitEvidence => Ok(DirectQuarantineOutcome::AwaitEvidence),
        DirectEvaluation::DispatchReplica { .. } | DirectEvaluation::DispatchLabel => {
            Err("quarantined direct activity attempted a second dispatch".to_string())
        }
    }
}
