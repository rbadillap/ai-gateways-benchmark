# ai-gateways-benchmark

Phase-by-phase latency benchmark for AI gateways, measured from your own
machine. Raw sockets, zero dependencies, Python 3 stdlib only.

## Quickstart

```sh
cp config.example.json config.json   # endpoints and models
cp .env.example .env                 # your API keys
set -a; source .env; set +a
python3 bench.py config.json
```

It prints per-run lines while it works, then a medians table ready to
paste, receipt headers per gateway, and dumps raw per-run results to
`results-<timestamp>.json`:

```
| Gateway         | DNS | TCP | TLS  | TTFB   | TTFT   | Cold e2e TTFT | Warm TTFB | Warm TTFT |
|-----------------|-----|-----|------|--------|--------|---------------|-----------|-----------|
| provider-direct | 4.2 | 8.0 | 11.3 |  672.4 |  673.5 |  704.0        |  602.5    |  602.6    |
| gateway-a       | 3.1 | 9.0 | 17.6 |  800.2 |  800.6 |  848.8        |  580.3    |  580.7    |
| gateway-b       | 4.1 | 7.0 | 12.0 | 1240.0 | 1240.1 | 1277.3        | 1302.4    | 1303.8    |
```

## What it measures

```mermaid
flowchart LR
    subgraph setup [connection setup, paid on every cold start]
        DNS --> TCP --> TLS
    end
    subgraph request [request phase, cold and warm]
        SENT[request sent] --> TTFB[first byte] --> TTFT[first token]
    end
    TLS --> SENT
```

| Metric | What it measures |
|---|---|
| `dns` | Hostname resolution |
| `tcp` | Socket connect |
| `tls` | Full TLS handshake (fresh context per connection, no session resumption) |
| `ttfb` | Request fully sent → first response byte |
| `ttft` | Request fully sent → first content token in the SSE stream |
| `cold e2e ttft` | `dns + tcp + tls + ttft`, what a short-lived process pays end to end |
| `warm ttfb / ttft` | Second request on an already-open connection (the connection-pool case) |

Runs interleave round-robin across gateways to cancel time-of-day drift,
and request-id headers (`x-vercel-id`, `cf-ray`, …) are captured as
receipts.

## Configuration

`config.json` accepts any OpenAI-compatible chat-completions endpoint, plus
per-gateway overrides for the auth header and extra headers. `$VARS` in
`path`, `auth_value`, and `extra_headers` are expanded from the
environment, so account and gateway IDs can live in `.env` rather than the
config (e.g. `/v1/$CLOUDFLARE_ACCOUNT_ID/$CLOUDFLARE_GATEWAY_ID/...`).

The example config ships a provider-direct baseline row and both Cloudflare
shapes: `cloudflare` uses the OpenAI-compatible `compat` endpoint, which
requires provider keys stored in the gateway (BYOK), while
`cloudflare-anthropic` passes the provider key per request instead, so it
works without stored keys.

## Read this before publishing numbers

> [!IMPORTANT]
> Results are a property of your vantage point (region, ISP, transit) and
> of the moment you measured. They are not a global ranking. The same
> config from another country, or another day, can invert the table.

- `cold` means a new connection, not a provider-side cold start.
- A config that proxies one gateway through another measures the whole
  chain, never the outer gateway alone.
- Medians of small runs are indicative, not definitive. The raw JSON has
  the spread.

The full measurement doctrine, including baselines, topology naming, and
how to present results honestly: **[METHODOLOGY.md](METHODOLOGY.md)**.

## License

MIT
