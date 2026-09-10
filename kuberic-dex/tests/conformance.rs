mod support;

use futures::executor::block_on;

use support::scenarios::{ScenarioId, run_conformance_matrix};

#[test]
fn fr_013_conformance_matrix() {
    let evidence = block_on(run_conformance_matrix());
    assert_eq!(evidence.len(), ScenarioId::ALL.len());

    for scenario in &evidence {
        println!(
            "{} {:?}: {}",
            scenario.id.stable_id(),
            scenario.id,
            if scenario.passed() { "PASS" } else { "FAIL" }
        );
        println!("  setup: {}", scenario.setup);
        for assertion in &scenario.assertions {
            println!(
                "  [{}] {}",
                if assertion.passed { "PASS" } else { "FAIL" },
                assertion.assertion
            );
        }
    }

    let failed: Vec<_> = evidence
        .iter()
        .filter(|scenario| !scenario.passed())
        .map(|scenario| scenario.id.stable_id())
        .collect();
    assert!(failed.is_empty(), "failed scenarios: {failed:?}");
}
