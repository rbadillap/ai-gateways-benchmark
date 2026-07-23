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

VERSION = "0.1.0"  # bump on every release; the git tag matches (v0.1.0)
TIMEOUT = 20
CONTENT_RE = re.compile(rb'"(?:content|text)"\s*:\s*"[^"]')
END_MARKERS = (b"data: [DONE]", b'"type":"message_stop"', b"\r\n0\r\n\r\n")
RECEIPT_HEADERS = ("x-vercel-id", "cf-ray", "x-request-id", "request-id", "x-amzn-requestid")

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


def build_request(gw, cfg):
    body = json.dumps({
        "model": gw["model"],
        "messages": [{"role": "user", "content": cfg["prompt"]}],
        "max_tokens": cfg["max_tokens"],
        "stream": True,
    }).encode()
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
    """Send one request on an open socket. Returns (status, headers, ttfb, ttft)."""
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
    return status, resp_headers, ttfb, ttft, body_preview


def run_cold(gw, cfg):
    ip, dns_ms = resolve(gw["host"])
    sock, tcp_ms, tls_ms = open_conn(ip, gw["host"])
    try:
        status, headers, ttfb, ttft, preview = timed_request(sock, build_request(gw, cfg))
    finally:
        sock.close()
    if status != 200 or ttft is None:
        raise RuntimeError(f"HTTP {status}: {preview[:200]}")
    return {
        "ip": ip, "dns": dns_ms, "tcp": tcp_ms, "tls": tls_ms,
        "ttfb": ttfb, "ttft": ttft,
        "e2e": dns_ms + tcp_ms + tls_ms + ttft,
        "receipts": {h: headers[h] for h in RECEIPT_HEADERS if h in headers},
    }


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
        status, _, _, _, preview = timed_request(sock, request)  # warmup, full read
        if status != 200:
            raise RuntimeError(f"warmup HTTP {status}: {preview[:200]}")
        _drain(sock)
        try:
            status, headers, ttfb, ttft, preview = timed_request(sock, request)
        except (BrokenPipeError, ConnectionResetError) as e:
            raise RuntimeError(
                f"server closed reused connection ({type(e).__name__})") from e
    finally:
        sock.close()
    if ttfb is None:
        raise RuntimeError("server closed reused connection (no bytes on second request)")
    if status != 200 or ttft is None:
        raise RuntimeError(f"HTTP {status}: {preview[:200]}")
    return {"ttfb": ttfb, "ttft": ttft,
            "conn": {h: headers[h] for h in ("connection", "keep-alive") if h in headers},
            "receipts": {h: headers[h] for h in RECEIPT_HEADERS if h in headers}}


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
    vals = sorted(r[key] for r in runs if r.get(key) is not None)
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
    return {"metrics_ms": {k: metric_stats(runs, k) for k in metrics}}


def fmt(v):
    return f"{v:7.1f}" if v is not None else "      —"


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


def main():
    cfg = json.load(open(sys.argv[1] if len(sys.argv) > 1 else "config.json"))
    gateways = cfg["gateways"]
    runs_cold = cfg.get("runs_cold", 50)
    runs_warm = cfg.get("runs_warm", 50)
    width = max(len(gw["name"]) for gw in gateways)
    results = {gw["name"]: {"cold": [], "warm": [], "errors": []} for gw in gateways}

    for i in range(runs_cold):
        for gw in gateways:  # round-robin: fair across time
            try:
                r = run_cold(gw, cfg)
                results[gw["name"]]["cold"].append(r)
                print(f"cold {i+1} {gw['name']:<{width}} tls={r['tls']:6.1f}ms ttft={r['ttft']:7.1f}ms e2e={r['e2e']:7.1f}ms")
            except Exception as e:
                results[gw["name"]]["errors"].append(f"cold {i+1}: {e}")
                print(f"cold {i+1} {gw['name']:<{width}} ERROR: {e}")

    for i in range(runs_warm):
        for gw in gateways:
            try:
                r = run_warm(gw, cfg)
                results[gw["name"]]["warm"].append(r)
                print(f"warm {i+1} {gw['name']:<{width}} ttfb={r['ttfb']:7.1f}ms ttft={r['ttft']:7.1f}ms")
            except Exception as e:
                results[gw["name"]]["errors"].append(f"warm {i+1}: {e}")
                print(f"warm {i+1} {gw['name']:<{width}} ERROR: {e}")

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
          f"p90 and IQR are in the raw JSON.\n")
    print("| Gateway | DNS | TCP | TLS | TTFB | TTFT | Cold e2e TTFT | Warm TTFB | Warm TTFT |")
    print("|---|---|---|---|---|---|---|---|---|")
    for gw in gateways:
        c = output["gateways"][gw["name"]]["summary"]["cold"]["metrics_ms"]
        w = output["gateways"][gw["name"]]["summary"]["warm"]["metrics_ms"]
        print(f"| {gw['name']} |{fmt(p50(c['dns']))} |{fmt(p50(c['tcp']))} |{fmt(p50(c['tls']))} "
              f"|{fmt(p50(c['ttfb']))} |{fmt(p50(c['ttft']))} |{fmt(p50(c['e2e']))} "
              f"|{fmt(p50(w['ttfb']))} |{fmt(p50(w['ttft']))} |")
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
