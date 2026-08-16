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
time-of-day drift. Every attempt (success or failure) is recorded with its
outcome and a classified error, so reliability is a first-class result and a
failure never lowers a latency number. Captures request-id headers (x-vercel-id,
cf-ray, ...) as receipts. Raw results are dumped to a timestamped JSON next to
the report.

Usage: python3 bench.py config.json
"""

import json
import os
import re
import socket
import ssl
import sys
import time

VERSION = "0.3.0"  # bump on every release; the git tag matches (v0.3.0)
TIMEOUT = 20
CONTENT_RE = re.compile(rb'"(?:content|text)"\s*:\s*"[^"]')
END_MARKERS = (b"data: [DONE]", b'"type":"message_stop"', b"\r\n0\r\n\r\n")
RECEIPT_HEADERS = ("x-vercel-id", "cf-ray", "x-request-id", "request-id", "x-amzn-requestid")
PROTECTED_BODY_FIELDS = ("model", "messages", "max_tokens", "stream")

now = time.perf_counter


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
    body = {
        "model": gw["model"],
        "messages": [{"role": "user", "content": cfg["prompt"]}],
        "max_tokens": cfg["max_tokens"],
        "stream": True,
    }
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
    """Send one request on an open socket.
    Returns (status, headers, ttfb, ttft, body_preview, timed_out)."""
    t0 = now()
    sock.sendall(request)
    buf = b""
    ttfb = ttft = None
    status = None
    resp_headers = {}
    header_end = -1
    timed_out = False
    while True:
        try:
            chunk = sock.recv(65536)
        except socket.timeout:
            timed_out = True
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
    return status, resp_headers, ttfb, ttft, body_preview, timed_out


def _errored(rec, category, stage, message):
    """Mark an attempt record as failed, preserving the original message."""
    rec["outcome"] = "error"
    rec["error"] = {"category": category, "stage": stage, "message": str(message)[:300]}
    return rec


def classify(status, ttft, timed_out):
    """Map a completed (bytes-received) response to 'success' or a failure
    category: http (non-200), timeout (no complete first token in time), or
    protocol (200 but the stream carried no content token)."""
    if status == 200 and ttft is not None:
        return "success"
    if status is not None and status != 200:
        return "http"
    if timed_out:
        return "timeout"
    return "protocol"


def run_cold(gw, cfg, run):
    """One cold attempt. Always returns a record (never raises): on failure it
    keeps the timings captured so far and classifies the stage that broke."""
    rec = {"run": run, "outcome": "success", "http_status": None, "ip": None,
           "timings_ms": {}, "receipts": {}, "error": None}
    t = rec["timings_ms"]
    try:
        rec["ip"], t["dns"] = resolve(gw["host"])
    except Exception as e:
        return _errored(rec, "dns", "resolve", e)
    try:
        sock, t["tcp"], t["tls"] = open_conn(rec["ip"], gw["host"])
    except ssl.SSLError as e:
        return _errored(rec, "tls", "handshake", e)
    except Exception as e:
        return _errored(rec, "tcp", "connect", e)
    try:
        status, headers, ttfb, ttft, preview, timed_out = timed_request(sock, build_request(gw, cfg))
    except Exception as e:
        return _errored(rec, "unknown", "response", e)
    finally:
        sock.close()
    rec["http_status"] = status
    rec["receipts"] = {h: headers[h] for h in RECEIPT_HEADERS if h in headers}
    if ttfb is not None:
        t["ttfb"] = ttfb
    outcome = classify(status, ttft, timed_out)
    if outcome != "success":
        return _errored(rec, outcome, "response", f"HTTP {status}: {preview[:200]}")
    t["ttft"] = ttft
    t["e2e"] = t["dns"] + t["tcp"] + t["tls"] + ttft
    return rec


def _drain(sock, quiet=0.4):
    """Consume trailing bytes (chunked terminator after [DONE]) before reuse."""
    sock.settimeout(quiet)
    try:
        while sock.recv(65536):
            pass
    except socket.timeout:
        pass
    sock.settimeout(TIMEOUT)


def run_warm(gw, cfg, run):
    """One warm attempt: a throwaway request, then a measured one on the same
    socket. Always returns a record; a server that drops the reused connection
    is recorded as a connection_reuse failure, which is data about the endpoint."""
    rec = {"run": run, "outcome": "success", "http_status": None,
           "timings_ms": {}, "connection": {}, "receipts": {}, "error": None}
    t = rec["timings_ms"]
    try:
        ip, _ = resolve(gw["host"])
    except Exception as e:
        return _errored(rec, "dns", "resolve", e)
    try:
        sock, _, _ = open_conn(ip, gw["host"])
    except ssl.SSLError as e:
        return _errored(rec, "tls", "handshake", e)
    except Exception as e:
        return _errored(rec, "tcp", "connect", e)
    try:
        request = build_request(gw, cfg)
        status, _, _, _, preview, _ = timed_request(sock, request)  # warmup, full read
        if status != 200:
            return _errored(rec, "http", "warmup", f"warmup HTTP {status}: {preview[:200]}")
        _drain(sock)
        status, headers, ttfb, ttft, preview, timed_out = timed_request(sock, request)
    except (BrokenPipeError, ConnectionResetError) as e:
        return _errored(rec, "connection_reuse", "reuse",
                        f"server closed reused connection ({type(e).__name__})")
    except Exception as e:
        return _errored(rec, "unknown", "response", e)
    finally:
        sock.close()
    rec["http_status"] = status
    rec["receipts"] = {h: headers[h] for h in RECEIPT_HEADERS if h in headers}
    rec["connection"] = {h: headers[h] for h in ("connection", "keep-alive") if h in headers}
    if ttfb is None:
        return _errored(rec, "connection_reuse", "reuse", "no bytes on reused connection")
    t["ttfb"] = ttfb
    outcome = classify(status, ttft, timed_out)
    if outcome != "success":
        return _errored(rec, outcome, "response", f"HTTP {status}: {preview[:200]}")
    t["ttft"] = ttft
    return rec


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


def metric_stats(records, key):
    """n, p50, p90, and IQR (p75 - p25) over successful attempts only, read from
    each record's timings_ms. A failed attempt is excluded even if it captured a
    partial timing (e.g. a fast 429's ttfb), so a failure cannot lower a number."""
    vals = sorted(r["timings_ms"][key] for r in records
                  if r["outcome"] == "success" and key in r["timings_ms"])
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


def reliability(records):
    """attempted / succeeded / failed / success_rate and a per-category error
    count over every attempt (success and failure alike)."""
    succeeded = sum(1 for r in records if r["outcome"] == "success")
    by_category = {}
    for r in records:
        if r["outcome"] == "error":
            c = r["error"]["category"]
            by_category[c] = by_category.get(c, 0) + 1
    attempted = len(records)
    return {
        "attempted": attempted,
        "succeeded": succeeded,
        "failed": attempted - succeeded,
        "success_rate": round(succeeded / attempted, 3) if attempted else None,
        "errors_by_category": by_category,
    }


def summarize(records, metrics):
    return {**reliability(records), "metrics_ms": {k: metric_stats(records, k) for k in metrics}}


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


def build_output(gateways, runs_cold, runs_warm, max_tokens, results):
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
            "max_tokens": max_tokens,
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
            }
            for gw in gateways
        },
    }


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
    results = {gw["name"]: {"cold": [], "warm": []} for gw in gateways}

    for i in range(runs_cold):
        for gw in gateways:  # round-robin: fair across time
            r = run_cold(gw, cfg, i + 1)
            results[gw["name"]]["cold"].append(r)
            t = r["timings_ms"]
            if r["outcome"] == "success":
                print(f"cold {i+1} {gw['name']:<{width}} tls={t['tls']:6.1f}ms ttft={t['ttft']:7.1f}ms e2e={t['e2e']:7.1f}ms")
            else:
                print(f"cold {i+1} {gw['name']:<{width}} ERROR [{r['error']['category']}] {r['error']['message'][:70]}")

    for i in range(runs_warm):
        for gw in gateways:
            r = run_warm(gw, cfg, i + 1)
            results[gw["name"]]["warm"].append(r)
            t = r["timings_ms"]
            if r["outcome"] == "success":
                print(f"warm {i+1} {gw['name']:<{width}} ttfb={t['ttfb']:7.1f}ms ttft={t['ttft']:7.1f}ms")
            else:
                print(f"warm {i+1} {gw['name']:<{width}} ERROR [{r['error']['category']}] {r['error']['message'][:70]}")

    output = build_output(gateways, runs_cold, runs_warm, cfg["max_tokens"], results)

    stamp = time.strftime("%Y%m%d-%H%M%S")
    out = os.path.join(os.path.dirname(os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else "config.json")),
                       f"results-{stamp}.json")
    with open(out, "w") as f:
        json.dump(output, f, indent=2)

    def p50(stats):
        return stats["p50"] if stats else None

    print(f"\np50 (R-7) in ms, {runs_cold} cold + {runs_warm} warm runs, "
          f"model per gateway as configured, max_tokens={cfg['max_tokens']}. "
          f"'ok' is succeeded/attempted; p90 and IQR are in the raw JSON.\n")
    print("| Gateway | cold ok | warm ok | DNS | TCP | TLS | TTFB | TTFT | Cold e2e TTFT | Warm TTFB | Warm TTFT |")
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for gw in gateways:
        s = output["gateways"][gw["name"]]["summary"]
        c, w = s["cold"]["metrics_ms"], s["warm"]["metrics_ms"]
        c_ok = f"{s['cold']['succeeded']}/{s['cold']['attempted']}"
        w_ok = f"{s['warm']['succeeded']}/{s['warm']['attempted']}"
        print(f"| {gw['name']} | {c_ok} | {w_ok} |{fmt(p50(c['dns']))} |{fmt(p50(c['tcp']))} |{fmt(p50(c['tls']))} "
              f"|{fmt(p50(c['ttfb']))} |{fmt(p50(c['ttft']))} |{fmt(p50(c['e2e']))} "
              f"|{fmt(p50(w['ttfb']))} |{fmt(p50(w['ttft']))} |")
    print("\nReceipts (one per gateway):")
    for gw in gateways:
        for r in results[gw["name"]]["cold"]:
            if r["receipts"]:
                print(f"  {gw['name']}: {r['receipts']}")
                break
    for gw in gateways:
        s = output["gateways"][gw["name"]]["summary"]
        fails = {ph: s[ph]["errors_by_category"] for ph in ("cold", "warm") if s[ph]["failed"]}
        if fails:
            print(f"\n{gw['name']} failures: {fails}")
    print(f"\nRaw results: {out}")


if __name__ == "__main__":
    main()
