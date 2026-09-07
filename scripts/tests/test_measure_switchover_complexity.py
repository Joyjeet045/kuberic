from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "measure-switchover-complexity.py"
SPEC = importlib.util.spec_from_file_location("measure_complexity", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
measure_complexity = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = measure_complexity
SPEC.loader.exec_module(measure_complexity)

Segment = measure_complexity.Segment


class ComplexityMeasurementTests(unittest.TestCase):
    def test_extract_rejects_missing_boundary(self) -> None:
        with self.assertRaisesRegex(SystemExit, "missing complexity boundary"):
            measure_complexity.extract_segment_lines(
                ["fn main() {}"], Segment("fixture.rs", "missing")
            )

    def test_extract_rejects_reversed_boundary(self) -> None:
        lines = [
            "// COMPLEXITY-BOUNDARY: fixture:end",
            "fn body() {}",
            "// COMPLEXITY-BOUNDARY: fixture:start",
        ]
        with self.assertRaisesRegex(SystemExit, "invalid complexity boundary"):
            measure_complexity.extract_segment_lines(
                lines, Segment("fixture.rs", "fixture")
            )

    def test_registry_rejects_duplicate_label(self) -> None:
        measurements = [
            ("same", [Segment("a.rs", "one")]),
            ("same", [Segment("b.rs", "two")]),
        ]
        with self.assertRaisesRegex(SystemExit, "duplicate complexity label"):
            measure_complexity.validate_measurement_registry(measurements)

    def test_registry_rejects_duplicate_segment(self) -> None:
        segment = Segment("a.rs", "one")
        measurements = [("one", [segment]), ("two", [segment])]
        with self.assertRaisesRegex(SystemExit, "duplicate complexity segment"):
            measure_complexity.validate_measurement_registry(measurements)

    def test_registry_rejects_whole_file_with_bounded_duplicate(self) -> None:
        measurements = [
            ("whole", [Segment("a.rs")]),
            ("bounded", [Segment("a.rs", "one")]),
        ]
        with self.assertRaisesRegex(
            SystemExit, "duplicate whole-file complexity source"
        ):
            measure_complexity.validate_measurement_registry(measurements)

    def test_overlap_rejected(self) -> None:
        measurements = [
            ("one", [Segment("a.rs", "one")]),
            ("two", [Segment("a.rs", "two")]),
        ]

        def same_locations(_segment: Segment) -> set[tuple[str, int]]:
            return {("a.rs", 7)}

        with self.assertRaisesRegex(SystemExit, "complexity scopes overlap"):
            measure_complexity.validate_nonoverlapping(
                ["one", "two"], measurements, same_locations
            )

    def test_frozen_shared_baseline(self) -> None:
        self.assertEqual(measure_complexity.SHARED_BEFORE, (1208, 110))
        self.assertEqual(
            measure_complexity.BASELINE_REVISION,
            "8d773ef2b32fd3073e11849a131fe2c2f5e6b97b",
        )

    def test_shared_growth_is_measured_against_frozen_baseline(self) -> None:
        self.assertEqual(
            measure_complexity.subtract((1220, 113), measure_complexity.SHARED_BEFORE),
            (12, 3),
        )

    def test_ratio_calculation_is_dimension_specific(self) -> None:
        self.assertEqual(
            measure_complexity.ratio((600, 25), (1200, 100)),
            (0.5, 0.25),
        )

    def test_amortization_classifications(self) -> None:
        self.assertEqual(
            measure_complexity.classify_amortization(
                (700, 70), (1000, 100), (100, 10), (1000, 100)
            ),
            "positive",
        )
        self.assertEqual(
            measure_complexity.classify_amortization(
                (1000, 70), (1000, 100), (100, 10), (1000, 100)
            ),
            "negative",
        )
        self.assertEqual(
            measure_complexity.classify_amortization(
                (700, 70), (1000, 100), (300, 30), (1000, 100)
            ),
            "inconclusive/mixed",
        )

    def test_dimension_classification_uses_spec_thresholds(self) -> None:
        self.assertEqual(
            measure_complexity.classify_amortization_dimension(0.99, 0.25),
            "positive",
        )
        self.assertEqual(
            measure_complexity.classify_amortization_dimension(1.0, 0.0),
            "negative",
        )
        self.assertEqual(
            measure_complexity.classify_amortization_dimension(0.75, 0.50),
            "inconclusive/mixed",
        )
        self.assertEqual(
            measure_complexity.classify_amortization_dimension(0.75, 0.500001),
            "negative",
        )

    def test_remove_measurement_scopes_are_declared(self) -> None:
        labels = {label for label, _segments in measure_complexity.MEASUREMENTS}
        self.assertTrue(
            {
                "legacy_remove",
                "remove_module",
                "remove_comparable_workflow_scope",
                "remove_workflow_body_only",
                "remove_effect_integration",
                "remove_crd_integration",
                "remove_routing_integration",
                "remove_reconcile_integration",
                "remove_status_integration",
            }.issubset(labels)
        )


if __name__ == "__main__":
    unittest.main()
