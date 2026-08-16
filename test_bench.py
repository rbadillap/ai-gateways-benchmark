#!/usr/bin/env python3
"""Standard-library tests for bench.py: percentile math, reliability accounting,
failure classification, request construction, and the serialized output shape.

    python3 -m unittest test_bench -v

No dependencies. Percentiles are cross-checked against
statistics.quantiles(method='inclusive') (the same R-7 convention); network
calls are monkeypatched so no socket is ever touched.
"""

import json
import os
import socket
import ssl
import statistics
import unittest

import bench

GW = {"name": "g", "host": "h", "path": "/", "model": "m", "auth_value": "k"}
CFG = {"prompt": "hi", "max_tokens": 16}


def _ok(**timings):
    return {"run": 1, "outcome": "success", "http_status": 200,
            "timings_ms": timings, "receipts": {}, "error": None}


def _err(category, **timings):
    return {"run": 1, "outcome": "error", "http_status": None,
            "timings_ms": timings, "receipts": {},
            "error": {"category": category, "stage": "response", "message": "x"}}


class _FakeSock:
    def close(self):
        pass

    def settimeout(self, *a):
        pass


def _raise(exc):
    def f(*a, **k):
        raise exc
    return f


class PercentileTests(unittest.TestCase):
    def test_empty_is_none(self):
        self.assertIsNone(bench.percentile([], 50))

    def test_single_value_for_any_quantile(self):
        for q in (0, 25, 50, 90, 100):
            self.assertEqual(bench.percentile([42.0], q), 42.0)

    def test_odd_count_p50_is_exact_middle(self):
        self.assertAlmostEqual(bench.percentile([10, 20, 30, 40, 50], 50), 30.0)

    def test_even_count_p50_interpolates(self):
        self.assertAlmostEqual(bench.percentile([10, 20, 30, 40], 50), 25.0)

    def test_p90_interpolation_even(self):
        self.assertAlmostEqual(bench.percentile([10, 20, 30, 40], 90), 37.0)

    def test_p90_interpolation_odd(self):
        self.assertAlmostEqual(bench.percentile([10, 20, 30, 40, 50], 90), 46.0)

    def test_matches_stdlib_inclusive(self):
        for data in ([662.3, 669.1, 678.0, 1226.2], [1, 2, 3, 4, 5, 6, 7], [5.0, 9.0, 9.0, 12.0]):
            s = sorted(data)
            for p in (25, 50, 75, 90):
                expected = statistics.quantiles(s, n=100, method="inclusive")[p - 1]
                self.assertAlmostEqual(bench.percentile(s, p), expected, places=9)


class MetricStatsTests(unittest.TestCase):
    def test_reports_n_p50_p90_iqr(self):
        runs = [_ok(ttft=v) for v in (10, 20, 30, 40, 50)]
        stats = bench.metric_stats(runs, "ttft")
        self.assertEqual(stats["n"], 5)
        self.assertAlmostEqual(stats["p50"], 30.0)
        self.assertAlmostEqual(stats["p90"], 46.0)
        self.assertAlmostEqual(stats["iqr"], 20.0)  # p75(40) - p25(20)

    def test_empty_runs_is_none(self):
        self.assertIsNone(bench.metric_stats([], "ttft"))

    def test_failed_attempt_partial_timing_is_excluded(self):
        # A fast 429 captured ttfb but the attempt failed: it must not enter the
        # ttfb statistics, or a failure would make the gateway look faster.
        runs = [_ok(ttfb=200.0), _ok(ttfb=210.0), _err("http", ttfb=15.0)]
        stats = bench.metric_stats(runs, "ttfb")
        self.assertEqual(stats["n"], 2)  # only the two successes
        self.assertAlmostEqual(stats["p50"], 205.0)

    def test_unsorted_input_is_ordered_first(self):
        runs = [_ok(ttft=v) for v in (50, 10, 40, 20, 30)]
        self.assertAlmostEqual(bench.metric_stats(runs, "ttft")["p50"], 30.0)

    def test_summarize_shapes_each_metric(self):
        runs = [_ok(dns=1.0, tcp=2.0, tls=3.0, ttfb=4.0, ttft=5.0, e2e=6.0)]
        summary = bench.summarize(runs, bench.COLD_METRICS)
        self.assertEqual(set(summary["metrics_ms"]), set(bench.COLD_METRICS))
        self.assertEqual(summary["metrics_ms"]["ttft"]["n"], 1)


class ReliabilityTests(unittest.TestCase):
    def test_counts_and_success_rate(self):
        runs = [_ok(ttft=1.0), _ok(ttft=2.0), _err("http"), _err("timeout"), _err("http")]
        r = bench.reliability(runs)
        self.assertEqual((r["attempted"], r["succeeded"], r["failed"]), (5, 2, 3))
        self.assertAlmostEqual(r["success_rate"], 0.4)
        self.assertEqual(r["errors_by_category"], {"http": 2, "timeout": 1})

    def test_all_success(self):
        r = bench.reliability([_ok(ttft=1.0)])
        self.assertEqual((r["succeeded"], r["failed"]), (1, 0))
        self.assertEqual(r["success_rate"], 1.0)
        self.assertEqual(r["errors_by_category"], {})

    def test_summary_merges_reliability_and_metrics(self):
        s = bench.summarize([_ok(ttft=1.0), _err("http")], bench.WARM_METRICS)
        self.assertEqual((s["attempted"], s["succeeded"]), (2, 1))
        self.assertEqual(s["metrics_ms"]["ttft"]["n"], 1)


class ClassifyTests(unittest.TestCase):
    def test_success(self):
        self.assertEqual(bench.classify(200, 5.0, False), "success")

    def test_http_error_beats_timeout(self):
        self.assertEqual(bench.classify(429, None, False), "http")
        self.assertEqual(bench.classify(500, None, True), "http")

    def test_timeout(self):
        self.assertEqual(bench.classify(200, None, True), "timeout")
        self.assertEqual(bench.classify(None, None, True), "timeout")

    def test_protocol(self):
        # 200 but the stream carried no content token, and it did not time out
        self.assertEqual(bench.classify(200, None, False), "protocol")


class RunRecordTests(unittest.TestCase):
    """run_cold / run_warm always return a record and classify failures by stage.
    resolve / open_conn / timed_request / _drain are monkeypatched."""

    def setUp(self):
        self._orig = (bench.resolve, bench.open_conn, bench.timed_request, bench._drain)
        bench.resolve = lambda host: ("192.0.2.1", 3.0)
        bench.open_conn = lambda ip, host: (_FakeSock(), 8.0, 11.0)
        bench._drain = lambda sock, quiet=0.4: None

    def tearDown(self):
        bench.resolve, bench.open_conn, bench.timed_request, bench._drain = self._orig

    def test_cold_success(self):
        bench.timed_request = lambda s, r: (200, {"cf-ray": "x"}, 640.0, 642.0, "", False)
        rec = bench.run_cold(GW, CFG, 7)
        self.assertEqual(rec["outcome"], "success")
        self.assertEqual((rec["run"], rec["http_status"], rec["error"]), (7, 200, None))
        self.assertIn("e2e", rec["timings_ms"])
        self.assertEqual(rec["receipts"], {"cf-ray": "x"})

    def test_cold_http_error_keeps_setup_timings(self):
        bench.timed_request = lambda s, r: (429, {}, 15.0, None, "rate limited", False)
        rec = bench.run_cold(GW, CFG, 1)
        self.assertEqual(rec["outcome"], "error")
        self.assertEqual(rec["error"]["category"], "http")
        self.assertEqual(rec["http_status"], 429)
        self.assertIn("tls", rec["timings_ms"])       # setup timings kept
        self.assertNotIn("ttft", rec["timings_ms"])   # no success timing

    def test_cold_timeout(self):
        bench.timed_request = lambda s, r: (None, {}, None, None, "", True)
        self.assertEqual(bench.run_cold(GW, CFG, 1)["error"]["category"], "timeout")

    def test_cold_dns_failure(self):
        bench.resolve = _raise(socket.gaierror("no such host"))
        rec = bench.run_cold(GW, CFG, 1)
        self.assertEqual(rec["error"]["category"], "dns")
        self.assertEqual(rec["timings_ms"], {})

    def test_cold_tls_failure(self):
        bench.open_conn = _raise(ssl.SSLError("handshake failed"))
        self.assertEqual(bench.run_cold(GW, CFG, 1)["error"]["category"], "tls")

    def test_warm_connection_reuse_failure(self):
        calls = {"n": 0}

        def flaky(s, r):
            calls["n"] += 1
            if calls["n"] == 1:
                return (200, {}, 600.0, 602.0, "", False)  # warmup succeeds
            raise ConnectionResetError("peer closed")

        bench.timed_request = flaky
        rec = bench.run_warm(GW, CFG, 1)
        self.assertEqual(rec["error"]["category"], "connection_reuse")


class OutputContractTests(unittest.TestCase):
    """Locks the serialized top-level shape so an incompatible change is caught
    (and forces a conscious version bump)."""

    def _results(self):
        return {"g1": {
            "cold": [_ok(dns=1.0, tcp=2.0, tls=3.0, ttfb=4.0, ttft=5.0, e2e=6.0)],
            "warm": [_ok(ttfb=4.0, ttft=5.0)],
        }}

    def _gw(self):
        return {"name": "g1", "host": "h", "path": "/", "model": "m"}

    def test_top_level_shape_and_version(self):
        out = bench.build_output([self._gw()], 1, 1, 16, self._results())
        self.assertEqual(set(out), {"version", "configuration", "gateways"})
        self.assertEqual(out["version"], bench.VERSION)
        self.assertEqual(out["configuration"]["units"], "ms")
        self.assertEqual(out["configuration"]["statistics"]["percentile_method"],
                         "R-7 linear interpolation")

    def test_raw_prompt_is_not_persisted(self):
        out = bench.build_output([self._gw()], 1, 1, 16, self._results())
        self.assertNotIn("prompt", out["configuration"])
        self.assertNotIn("prompt", json.dumps(out))

    def test_gateway_entry_shape(self):
        out = bench.build_output([self._gw()], 1, 1, 16, self._results())
        self.assertEqual(set(out["gateways"]), {"g1"})
        g = out["gateways"]["g1"]
        self.assertEqual(set(g), {"target", "summary", "cold", "warm"})
        self.assertEqual(set(g["summary"]["cold"]) >= {"attempted", "succeeded",
                         "failed", "success_rate", "errors_by_category", "metrics_ms"}, True)
        self.assertEqual(set(g["summary"]["cold"]["metrics_ms"]), set(bench.COLD_METRICS))

    def test_output_is_json_serializable(self):
        out = bench.build_output([self._gw()], 1, 1, 16, self._results())
        self.assertEqual(json.loads(json.dumps(out))["version"], bench.VERSION)


class RequestBodyTests(unittest.TestCase):
    def _gw(self, **kw):
        gw = dict(GW)
        gw.update(kw)
        return gw

    def test_base_body_without_extra(self):
        b = bench.request_body(self._gw(), CFG)
        self.assertEqual(b["model"], "m")
        self.assertTrue(b["stream"])
        self.assertEqual(b["messages"], [{"role": "user", "content": "hi"}])

    def test_extra_body_is_merged(self):
        gw = self._gw(extra_body={"provider": {"only": ["anthropic"], "allow_fallbacks": False}})
        self.assertEqual(bench.request_body(gw, CFG)["provider"],
                         {"only": ["anthropic"], "allow_fallbacks": False})

    def test_extra_body_env_expansion_is_recursive(self):
        os.environ["BENCH_TEST_ACCT"] = "acct-123"
        try:
            gw = self._gw(extra_body={"routing": {"account": "$BENCH_TEST_ACCT"}})
            self.assertEqual(bench.request_body(gw, CFG)["routing"]["account"], "acct-123")
        finally:
            del os.environ["BENCH_TEST_ACCT"]

    def test_protected_fields_cannot_be_overridden(self):
        for field in ("model", "messages", "max_tokens", "stream"):
            with self.assertRaises(ValueError):
                bench.request_body(self._gw(extra_body={field: "x"}), CFG)


class TargetTests(unittest.TestCase):
    def _out(self, gw):
        return bench.build_output([gw], 1, 1, 16, {gw["name"]: {"cold": [], "warm": []}})

    def test_path_stays_templated(self):
        gw = {"name": "g", "host": "h", "path": "/v1/$ACCT/x", "model": "m",
              "routing": {"mode": "pinned", "provider": "anthropic",
                          "region": None, "fallbacks": False}}
        t = self._out(gw)["gateways"]["g"]["target"]
        self.assertEqual(t["path"], "/v1/$ACCT/x")  # not expanded: no ID leak
        self.assertEqual(t["model"], "m")
        self.assertEqual(t["routing"]["provider"], "anthropic")

    def test_missing_routing_defaults_to_dynamic(self):
        gw = {"name": "g", "host": "h", "path": "/", "model": "m"}
        self.assertEqual(self._out(gw)["gateways"]["g"]["target"]["routing"]["mode"], "dynamic")


if __name__ == "__main__":
    unittest.main()
