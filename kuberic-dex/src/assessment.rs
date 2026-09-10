/// Bounded feasibility classifications required by FR-014.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeasibilityClassification {
    Feasible,
    ConditionallyFeasible,
    Infeasible,
}

/// Mechanical inputs to the bounded FR-014 classifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FeasibilityInputs {
    pub safety_and_determinism_pass: bool,
    pub all_conformance_pass: bool,
    pub authoring_simplicity_pass: bool,
    pub has_in_scope_limitation: bool,
}

/// Classify only the synthetic model represented by the supplied evidence.
pub const fn classify_feasibility(inputs: FeasibilityInputs) -> FeasibilityClassification {
    if !inputs.safety_and_determinism_pass {
        FeasibilityClassification::Infeasible
    } else if !inputs.all_conformance_pass
        || !inputs.authoring_simplicity_pass
        || inputs.has_in_scope_limitation
    {
        FeasibilityClassification::ConditionallyFeasible
    } else {
        FeasibilityClassification::Feasible
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifier_matches_the_explicit_fr_014_truth_table() {
        use FeasibilityClassification::{
            ConditionallyFeasible as Conditional, Feasible, Infeasible,
        };

        let rows = [
            (inputs(false, false, false, false), Infeasible),
            (inputs(true, false, false, false), Conditional),
            (inputs(false, true, false, false), Infeasible),
            (inputs(true, true, false, false), Conditional),
            (inputs(false, false, true, false), Infeasible),
            (inputs(true, false, true, false), Conditional),
            (inputs(false, true, true, false), Infeasible),
            (inputs(true, true, true, false), Feasible),
            (inputs(false, false, false, true), Infeasible),
            (inputs(true, false, false, true), Conditional),
            (inputs(false, true, false, true), Infeasible),
            (inputs(true, true, false, true), Conditional),
            (inputs(false, false, true, true), Infeasible),
            (inputs(true, false, true, true), Conditional),
            (inputs(false, true, true, true), Infeasible),
            (inputs(true, true, true, true), Conditional),
        ];

        for (row, (input, expected)) in rows.into_iter().enumerate() {
            assert_eq!(
                classify_feasibility(input),
                expected,
                "truth-table row {row}"
            );
        }
    }

    const fn inputs(
        safety_and_determinism_pass: bool,
        all_conformance_pass: bool,
        authoring_simplicity_pass: bool,
        has_in_scope_limitation: bool,
    ) -> FeasibilityInputs {
        FeasibilityInputs {
            safety_and_determinism_pass,
            all_conformance_pass,
            authoring_simplicity_pass,
            has_in_scope_limitation,
        }
    }
}
