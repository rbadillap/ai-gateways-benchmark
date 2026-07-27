#!/usr/bin/env python3
"""ai-gateways-benchmark: TTFB/TTFT comparison across AI gateways, phase by phase.

Measures, per gateway, with cold (fresh) and warm (already-open) connections:

  dns   - getaddrinfo
  tcp   - socket connect
  tls   - TLS handshake (fresh SSLContext per connection: no ticket resumption,
          so every cold run pays the full handshake like a real cold start)
  ttfb  - request fully sent -> first response byte
  ttft  - request fully sent -> first visible content token in the SSE stream
  e2e   - dns + tcp + tls + ttft: what a short-lived process pays end to end

Warm runs open a connection, run one throwaway request to completion, then
measure a second request on the same socket (the connection-pool case).

Runs are interleaved round-robin across gateways so no gateway benefits from
time-of-day drift. Captures request-id headers (x-vercel-id, cf-ray, ...) as
receipts. Raw results are dumped to a timestamped JSON next to the report.

Usage: python3 bench.py config.json
"""

import json
import os
import re
import socket
import ssl
import sys
import time

VERSION = "0.2.0"  # bump on every release; the git tag matches (v0.2.0)
TIMEOUT = 20
CONTENT_RE = re.compile(rb'"(?:content|text)"\s*:\s*"[^"]')
END_MARKERS = (b"data: [DONE]", b'"type":"message_stop"', b"\r\n0\r\n\r\n")
RECEIPT_HEADERS = ("x-vercel-id", "cf-ray", "x-request-id", "request-id", "x-amzn-requestid")
PROTECTED_BODY_FIELDS = (
    "model",
    "messages",
    "max_tokens",
    "max_completion_tokens",
    "stream",
    "include_timings",
)

now = time.perf_counter


def token_limit(cfg):
    for field in ("max_completion_tokens", "max_tokens"):
        if field in cfg:
            return field, cfg[field]
    raise KeyError("config must set max_completion_tokens or max_tokens")


def resolve(host):
    t0 = now()
    infos = socket.getaddrinfo(host, 443, socket.AF_INET, socket.SOCK_STREAM)
    return infos[0][4][0], (now() - t0) * 1000


def open_conn(ip, host):
    t0 = now()
    raw = socket.create_connection((ip, 443), timeout=TIMEOUT)
    tcp_ms = (now() - t0) * 1000
    ctx = ssl.create_default_context()  # fresh context: no session resumption
    t1 = now()
    tls_sock = ctx.wrap_socket(raw, server_hostname=host)
    tls_ms = (now() - t1) * 1000
    tls_sock.settimeout(TIMEOUT)
    return tls_sock, tcp_ms, tls_ms


def _expandvars(obj):
    """Recursively expand $VARS in every string inside a JSON-ish structure."""
    if isinstance(obj, str):
        return os.path.expandvars(obj)
    if isinstance(obj, dict):
        return {k: _expandvars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expandvars(v) for v in obj]
    return obj


def request_body(gw, cfg):
    """The JSON request body: the shared, controlled fields plus a gateway's
    optional extra_body (provider-routing options). extra_body may not override
    a controlled field, and its $VARS are expanded from the environment."""
    token_field, token_value = token_limit(cfg)
    body = {
        "model": gw["model"],
        "messages": [{"role": "user", "content": cfg["prompt"]}],
        token_field: token_value,
        "stream": True,
    }
    if gw.get("include_timings", cfg.get("include_timings", False)):
        body["include_timings"] = True
    extra = gw.get("extra_body", {})
    clashes = sorted(k for k in extra if k in PROTECTED_BODY_FIELDS)
    if clashes:
        raise ValueError(
            f"{gw['name']}: extra_body may not override {', '.join(clashes)}")
    body.update(_expandvars(extra))
    return body


def build_request(gw, cfg):
    body = json.dumps(request_body(gw, cfg)).encode()
    headers = {
        "Host": gw["host"],
        gw.get("auth_header", "Authorization"): os.path.expandvars(gw["auth_value"]),
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
        "Connection": "keep-alive",
        "User-Agent": "ai-gateways-benchmark/1.0",
        "Content-Length": str(len(body)),
    }
    for k, v in gw.get("extra_headers", {}).items():
        headers[k] = os.path.expandvars(v)
    path = os.path.expandvars(gw["path"])
    head = f"POST {path} HTTP/1.1\r\n" + "".join(
        f"{k}: {v}\r\n" for k, v in headers.items()) + "\r\n"
    return head.encode() + body


def timed_request(sock, request):
    """Send one request and return status, headers, latency, and router timings."""
    t0 = now()
    sock.sendall(request)
    buf = b""
    ttfb = ttft = None
    status = None
    resp_headers = {}
    header_end = -1
    while True:
        try:
            chunk = sock.recv(65536)
        except socket.timeout:
            break
        if not chunk:
            break
        if ttfb is None:
            ttfb = (now() - t0) * 1000
        buf += chunk
        if header_end < 0:
            header_end = buf.find(b"\r\n\r\n")
            if header_end >= 0:
                head = buf[:header_end].decode("latin1", "replace")
                lines = head.split("\r\n")
                status = int(lines[0].split()[1])
                for line in lines[1:]:
                    if ":" in line:
                        k, v = line.split(":", 1)
                        resp_headers[k.strip().lower()] = v.strip()
        if ttft is None and header_end >= 0 and CONTENT_RE.search(buf, header_end):
            ttft = (now() - t0) * 1000
        if status is not None and status != 200 and header_end >= 0 and len(buf) > header_end + 4:
            break  # error body arrived; no stream to wait for
        if any(m in buf for m in END_MARKERS):
            break
    body_preview = buf[header_end + 4:header_end + 300].decode("utf8", "replace") if header_end >= 0 else ""
    timings = extract_timings(buf, header_end)
    return status, resp_headers, ttfb, ttft, body_preview, timings


def extract_timings(buf, header_end):
    """Read the last timing object from a Chat or Responses SSE payload."""
    if header_end < 0:
        return None
    found = None
    for line in buf[header_end + 4:].splitlines():
        marker = line.find(b"data: ")
        if marker < 0:
            continue
        raw = line[marker + len(b"data: "):].strip()
        if not raw.startswith(b"{"):
            continue
        try:
            event = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        candidate = event.get("timings") if isinstance(event, dict) else None
        response = event.get("response") if isinstance(event, dict) else None
        if candidate is None and isinstance(response, dict):
            candidate = response.get("timings")
        if isinstance(candidate, dict):
            found = candidate
    return found


def run_cold(gw, cfg):
    ip, dns_ms = resolve(gw["host"])
    sock, tcp_ms, tls_ms = open_conn(ip, gw["host"])
    try:
        status, headers, ttfb, ttft, preview, timings = timed_request(
            sock, build_request(gw, cfg)
        )
    finally:
        sock.close()
    if status != 200 or ttft is None:
        raise RuntimeError(f"HTTP {status}: {preview[:200]}")
    result = {
        "ip": ip, "dns": dns_ms, "tcp": tcp_ms, "tls": tls_ms,
        "ttfb": ttfb, "ttft": ttft,
        "e2e": dns_ms + tcp_ms + tls_ms + ttft,
        "timings": timings,
        "receipts": {h: headers[h] for h in RECEIPT_HEADERS if h in headers},
    }
    add_client_timing_delta(result)
    return result


def _drain(sock, quiet=0.4):
    """Consume trailing bytes (chunked terminator after [DONE]) before reuse."""
    sock.settimeout(quiet)
    try:
        while sock.recv(65536):
            pass
    except socket.timeout:
        pass
    sock.settimeout(TIMEOUT)


def run_warm(gw, cfg):
    ip, _ = resolve(gw["host"])
    sock, _, _ = open_conn(ip, gw["host"])
    try:
        request = build_request(gw, cfg)
        status, _, _, _, preview, _ = timed_request(
            sock, request
        )  # warmup, full read
        if status != 200:
            raise RuntimeError(f"warmup HTTP {status}: {preview[:200]}")
        _drain(sock)
        try:
            status, headers, ttfb, ttft, preview, timings = timed_request(
                sock, request
            )
        except (BrokenPipeError, ConnectionResetError) as e:
            raise RuntimeError(
                f"server closed reused connection ({type(e).__name__})") from e
    finally:
        sock.close()
    if ttfb is None:
        raise RuntimeError("server closed reused connection (no bytes on second request)")
    if status != 200 or ttft is None:
        raise RuntimeError(f"HTTP {status}: {preview[:200]}")
    result = {
        "ttfb": ttfb,
        "ttft": ttft,
        "timings": timings,
        "conn": {h: headers[h] for h in ("connection", "keep-alive") if h in headers},
        "receipts": {h: headers[h] for h in RECEIPT_HEADERS if h in headers},
    }
    add_client_timing_delta(result)
    return result


def add_client_timing_delta(result):
    timings = result.get("timings")
    router_ttft = timings.get("ttft_ms") if isinstance(timings, dict) else None
    if isinstance(router_ttft, (int, float)) and result.get("ttft") is not None:
        result["client_minus_router_ttft"] = result["ttft"] - router_ttft


def percentile(vals, q):
    """R-7 (linear interpolation) percentile of an ascending list, q in [0, 100].

    Deterministic and identical to the default used by R, NumPy, pandas, and
    Excel PERCENTILE.INC, i.e. statistics.quantiles(method='inclusive'). Named
    explicitly in the output so anyone can reproduce a number in their own tool.
    """
    if not vals:
        return None
    if len(vals) == 1:
        return vals[0]
    pos = (q / 100) * (len(vals) - 1)
    lo = int(pos)
    if lo + 1 >= len(vals):
        return vals[-1]
    return vals[lo] + (pos - lo) * (vals[lo + 1] - vals[lo])


def metric_stats(runs, key):
    """n, p50, p90, and IQR (p75 - p25) over successful runs only (None dropped)."""
    return value_stats(r[key] for r in runs if r.get(key) is not None)


def value_stats(values):
    vals = sorted(values)
    if not vals:
        return None
    return {
        "n": len(vals),
        "p50": round(percentile(vals, 50), 2),
        "p90": round(percentile(vals, 90), 2),
        "iqr": round(percentile(vals, 75) - percentile(vals, 25), 2),
    }


COLD_METRICS = ("dns", "tcp", "tls", "ttfb", "ttft", "e2e")
WARM_METRICS = ("ttfb", "ttft")


def summarize(runs, metrics):
    summary = {"metrics_ms": {k: metric_stats(runs, k) for k in metrics}}
    router_timings = router_timing_stats(runs)
    if router_timings:
        summary["router_timings_ms"] = router_timings
    return summary


ROUTER_TIMING_PATHS = {
    "ttft": "ttft_ms",
    "before_upstream": "router_before_upstream_ms",
    "upstream_headers": "upstream.response_headers_ms",
    "upstream_ttft": "upstream.ttft_ms",
    "router_overhead": "router_overhead_ms",
    "database": "database_ms",
    "kv": "kv_ms",
}


def nested_metric_stats(runs, path):
    vals = []
    for run in runs:
        value = run.get("timings")
        for part in path.split("."):
            value = value.get(part) if isinstance(value, dict) else None
        if isinstance(value, (int, float)):
            vals.append(value)
    return value_stats(vals)


def router_timing_stats(runs):
    if not any(isinstance(run.get("timings"), dict) for run in runs):
        return None
    stats = {
        name: nested_metric_stats(runs, path)
        for name, path in ROUTER_TIMING_PATHS.items()
    }
    stats["client_minus_router_ttft"] = metric_stats(
        runs, "client_minus_router_ttft"
    )
    return stats


def fmt(v):
    return f"{v:7.1f}" if v is not None else "      —"


def _target(gw):
    """What the run aimed at, for the result file. The path stays templated (its
    $VARS are not expanded) so account and gateway IDs never land in a result.
    A gateway that declares no routing is recorded as `dynamic`, not as a pin."""
    return {
        "host": gw["host"],
        "path": gw["path"],
        "model": gw["model"],
        "routing": gw.get("routing", {"mode": "dynamic", "provider": None,
                                      "region": None, "fallbacks": None}),
    }


def build_output(
    gateways,
    runs_cold,
    runs_warm,
    token_limit_value,
    results,
    token_limit_field="max_tokens",
):
    """Assemble the serialized result document. Pure (no I/O) so the top-level
    contract can be tested directly. `version` matches the release (and the git
    tag `vX.Y.Z`); call out any format-breaking change in the release notes.

    The raw prompt is deliberately not persisted: the README encourages sharing
    the result file, and a prompt can carry private text. Redaction rules for
    any embedded configuration are deferred to the observer-metadata work.
    """
    return {
        "version": VERSION,
        "configuration": {
            "runs_cold": runs_cold,
            "runs_warm": runs_warm,
            token_limit_field: token_limit_value,
            "timeout_seconds": TIMEOUT,
            "units": "ms",
            "statistics": {
                "percentiles": [50, 90],
                "percentile_method": "R-7 linear interpolation",
                "include_iqr": True,
            },
        },
        "gateways": {
            gw["name"]: {
                "target": _target(gw),
                "summary": {
                    "cold": summarize(results[gw["name"]]["cold"], COLD_METRICS),
                    "warm": summarize(results[gw["name"]]["warm"], WARM_METRICS),
                },
                "cold": results[gw["name"]]["cold"],
                "warm": results[gw["name"]]["warm"],
                "errors": results[gw["name"]]["errors"],
            }
            for gw in gateways
        },
    }


def timing_suffix(run):
    timings = run.get("timings")
    if not isinstance(timings, dict):
        return ""
    upstream = timings.get("upstream")
    if not isinstance(upstream, dict):
        upstream = {}
    router_overhead = numeric_value(timings.get("router_overhead_ms"))
    upstream_ttft = numeric_value(upstream.get("ttft_ms"))
    database = numeric_value(timings.get("database_ms"))
    kv = numeric_value(timings.get("kv_ms"))
    return (
        f" router={router_overhead:6.1f}ms"
        f" upstream={upstream_ttft:7.1f}ms"
        f" db={database:5.1f}ms"
        f" kv={kv:5.1f}ms"
    )


def numeric_value(value):
    return value if isinstance(value, (int, float)) else 0


def main():
    cfg = json.load(open(sys.argv[1] if len(sys.argv) > 1 else "config.json"))
    gateways = cfg["gateways"]
    try:
        for gw in gateways:
            request_body(gw, cfg)  # fail fast on a bad extra_body before any run
    except ValueError as e:
        sys.exit(f"config error: {e}")
    runs_cold = cfg.get("runs_cold", 50)
    runs_warm = cfg.get("runs_warm", 50)
    width = max(len(gw["name"]) for gw in gateways)
    results = {gw["name"]: {"cold": [], "warm": [], "errors": []} for gw in gateways}

    for i in range(runs_cold):
        for gw in gateways:  # round-robin: fair across time
            try:
                r = run_cold(gw, cfg)
                results[gw["name"]]["cold"].append(r)
                print(
                    f"cold {i+1} {gw['name']:<{width}} tls={r['tls']:6.1f}ms"
                    f" ttft={r['ttft']:7.1f}ms e2e={r['e2e']:7.1f}ms"
                    f"{timing_suffix(r)}"
                )
            except Exception as e:
                results[gw["name"]]["errors"].append(f"cold {i+1}: {e}")
                print(f"cold {i+1} {gw['name']:<{width}} ERROR: {e}")

    for i in range(runs_warm):
        for gw in gateways:
            try:
                r = run_warm(gw, cfg)
                results[gw["name"]]["warm"].append(r)
                print(
                    f"warm {i+1} {gw['name']:<{width}} ttfb={r['ttfb']:7.1f}ms"
                    f" ttft={r['ttft']:7.1f}ms{timing_suffix(r)}"
                )
            except Exception as e:
                results[gw["name"]]["errors"].append(f"warm {i+1}: {e}")
                print(f"warm {i+1} {gw['name']:<{width}} ERROR: {e}")

    token_field, token_value = token_limit(cfg)
    output = build_output(
        gateways,
        runs_cold,
        runs_warm,
        token_value,
        results,
        token_field,
    )

    stamp = time.strftime("%Y%m%d-%H%M%S")
    out = os.path.join(os.path.dirname(os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else "config.json")),
                       f"results-{stamp}.json")
    with open(out, "w") as f:
        json.dump(output, f, indent=2)

    def p50(stats):
        return stats["p50"] if stats else None

    print(f"\np50 (R-7) in ms, {runs_cold} cold + {runs_warm} warm runs, "
          f"model per gateway as configured, {token_field}={token_value}. "
          f"p90 and IQR are in the raw JSON.\n")
    print("| Gateway | DNS | TCP | TLS | TTFB | TTFT | Cold e2e TTFT | Warm TTFB | Warm TTFT |")
    print("|---|---|---|---|---|---|---|---|---|")
    for gw in gateways:
        c = output["gateways"][gw["name"]]["summary"]["cold"]["metrics_ms"]
        w = output["gateways"][gw["name"]]["summary"]["warm"]["metrics_ms"]
        print(f"| {gw['name']} |{fmt(p50(c['dns']))} |{fmt(p50(c['tcp']))} |{fmt(p50(c['tls']))} "
              f"|{fmt(p50(c['ttfb']))} |{fmt(p50(c['ttft']))} |{fmt(p50(c['e2e']))} "
              f"|{fmt(p50(w['ttfb']))} |{fmt(p50(w['ttft']))} |")

    timed_gateways = [
        gw for gw in gateways
        if any(
            isinstance(run.get("timings"), dict)
            for phase in ("cold", "warm")
            for run in results[gw["name"]][phase]
        )
    ]
    if timed_gateways:
        print("\nRouter-reported timing p50 (R-7) in ms:")
        print("| Gateway | Mode | Router TTFT | Before upstream | Upstream headers | Upstream TTFT | Router overhead | DB | KV | Client minus router TTFT |")
        print("|---|---|---|---|---|---|---|---|---|---|")
        for gw in timed_gateways:
            for phase in ("cold", "warm"):
                timing_stats = output["gateways"][gw["name"]]["summary"][
                    phase
                ].get("router_timings_ms")
                if not timing_stats:
                    continue
                print(
                    f"| {gw['name']} | {phase} "
                    f"|{fmt(p50(timing_stats['ttft']))} "
                    f"|{fmt(p50(timing_stats['before_upstream']))} "
                    f"|{fmt(p50(timing_stats['upstream_headers']))} "
                    f"|{fmt(p50(timing_stats['upstream_ttft']))} "
                    f"|{fmt(p50(timing_stats['router_overhead']))} "
                    f"|{fmt(p50(timing_stats['database']))} "
                    f"|{fmt(p50(timing_stats['kv']))} "
                    f"|{fmt(p50(timing_stats['client_minus_router_ttft']))} |"
                )
    print("\nReceipts (one per gateway):")
    for gw in gateways:
        runs = results[gw["name"]]["cold"]
        if runs and runs[0]["receipts"]:
            print(f"  {gw['name']}: {runs[0]['receipts']}")
    for gw in gateways:
        errs = results[gw["name"]]["errors"]
        if errs:
            print(f"\n{gw['name']} errors: {errs}")
    print(f"\nRaw results: {out}")


if __name__ == "__main__":
    main()
