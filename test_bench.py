#!/usr/bin/env python3
"""Standard-library tests for the statistical summary in bench.py.

    python3 -m unittest test_bench -v

No dependencies: the assertions are hand-computed and cross-checked against
statistics.quantiles(method='inclusive'), which is the same R-7 convention.
"""

import json
import statistics
import unittest

import bench


class PercentileTests(unittest.TestCase):
    def test_empty_is_none(self):
        self.assertIsNone(bench.percentile([], 50))

    def test_single_value_for_any_quantile(self):
        for q in (0, 25, 50, 90, 100):
            self.assertEqual(bench.percentile([42.0], q), 42.0)

    def test_odd_count_p50_is_exact_middle(self):
        # [10,20,30,40,50]: pos = 0.5*4 = 2.0 -> lands exactly on 30
        self.assertAlmostEqual(bench.percentile([10, 20, 30, 40, 50], 50), 30.0)

    def test_even_count_p50_interpolates(self):
        # [10,20,30,40]: pos = 0.5*3 = 1.5 -> halfway between 20 and 30
        self.assertAlmostEqual(bench.percentile([10, 20, 30, 40], 50), 25.0)

    def test_p90_interpolation_even(self):
        # pos = 0.9*3 = 2.7 -> 30 + 0.7*(40-30) = 37
        self.assertAlmostEqual(bench.percentile([10, 20, 30, 40], 90), 37.0)

    def test_p90_interpolation_odd(self):
        # pos = 0.9*4 = 3.6 -> 40 + 0.6*(50-40) = 46
        self.assertAlmostEqual(bench.percentile([10, 20, 30, 40, 50], 90), 46.0)

    def test_matches_stdlib_inclusive(self):
        # Guards against silent drift from the documented R-7 method. Includes
        # the real anthropic tail sample and a set with a duplicate value.
        for data in ([662.3, 669.1, 678.0, 1226.2], [1, 2, 3, 4, 5, 6, 7], [5.0, 9.0, 9.0, 12.0]):
            s = sorted(data)
            for p in (25, 50, 75, 90):
                expected = statistics.quantiles(s, n=100, method="inclusive")[p - 1]
                self.assertAlmostEqual(bench.percentile(s, p), expected, places=9)


class MetricStatsTests(unittest.TestCase):
    def test_reports_n_p50_p90_iqr(self):
        runs = [{"ttft": v} for v in (10, 20, 30, 40, 50)]
        stats = bench.metric_stats(runs, "ttft")
        self.assertEqual(stats["n"], 5)
        self.assertAlmostEqual(stats["p50"], 30.0)
        self.assertAlmostEqual(stats["p90"], 46.0)
        self.assertAlmostEqual(stats["iqr"], 20.0)  # p75(40) - p25(20)

    def test_empty_runs_is_none(self):
        self.assertIsNone(bench.metric_stats([], "ttft"))

    def test_partial_failures_count_successes_only(self):
        # Two attempts produced no ttft (None / missing key): n must be 3, not 5,
        # and the statistics must be computed from the survivors alone.
        runs = [
            {"ttft": 100.0},
            {"ttft": None},
            {"ttft": 200.0},
            {},  # key absent
            {"ttft": 300.0},
        ]
        stats = bench.metric_stats(runs, "ttft")
        self.assertEqual(stats["n"], 3)
        self.assertAlmostEqual(stats["p50"], 200.0)

    def test_unsorted_input_is_ordered_first(self):
        runs = [{"ttft": v} for v in (50, 10, 40, 20, 30)]
        self.assertAlmostEqual(bench.metric_stats(runs, "ttft")["p50"], 30.0)

    def test_summarize_shapes_each_metric(self):
        runs = [{"dns": 1.0, "tcp": 2.0, "tls": 3.0, "ttfb": 4.0, "ttft": 5.0, "e2e": 6.0}]
        summary = bench.summarize(runs, bench.COLD_METRICS)
        self.assertEqual(set(summary["metrics_ms"]), set(bench.COLD_METRICS))
        self.assertEqual(summary["metrics_ms"]["ttft"]["n"], 1)


class OutputContractTests(unittest.TestCase):
    """Locks the serialized top-level shape so an incompatible change is caught
    (and forces a conscious schema_version bump)."""

    def _results(self):
        return {
            "g1": {
                "cold": [{"dns": 1.0, "tcp": 2.0, "tls": 3.0, "ttfb": 4.0,
                          "ttft": 5.0, "e2e": 6.0, "receipts": {}}],
                "warm": [{"ttfb": 4.0, "ttft": 5.0, "conn": {}, "receipts": {}}],
                "errors": [],
            }
        }

    def test_top_level_shape_and_version(self):
        out = bench.build_output([{"name": "g1"}], 1, 1, 16, self._results())
        self.assertEqual(set(out), {"version", "configuration", "gateways"})
        self.assertEqual(out["version"], bench.VERSION)
        cfg = out["configuration"]
        self.assertEqual(cfg["units"], "ms")
        self.assertEqual(cfg["statistics"]["percentile_method"], "R-7 linear interpolation")

    def test_raw_prompt_is_not_persisted(self):
        out = bench.build_output([{"name": "g1"}], 1, 1, 16, self._results())
        self.assertNotIn("prompt", out["configuration"])
        self.assertNotIn("prompt", json.dumps(out))

    def test_gateway_entry_shape(self):
        out = bench.build_output([{"name": "g1"}], 1, 1, 16, self._results())
        self.assertEqual(set(out["gateways"]), {"g1"})
        g = out["gateways"]["g1"]
        self.assertEqual(set(g), {"summary", "cold", "warm", "errors"})
        self.assertEqual(set(g["summary"]["cold"]["metrics_ms"]), set(bench.COLD_METRICS))
        self.assertEqual(set(g["summary"]["warm"]["metrics_ms"]), set(bench.WARM_METRICS))

    def test_output_is_json_serializable(self):
        out = bench.build_output([{"name": "g1"}], 1, 1, 16, self._results())
        self.assertEqual(json.loads(json.dumps(out))["version"], bench.VERSION)


if __name__ == "__main__":
    unittest.main()
