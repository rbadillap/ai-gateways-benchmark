# ai-gateways-benchmark

Phase-by-phase latency benchmark for AI gateways, measured from your own
machine. Raw sockets, zero dependencies — Python 3 stdlib only.

Gateway latency discussions tend to conflate metrics that behave very
differently. This tool separates them:

| Metric | What it measures |
|---|---|
| `dns` | Hostname resolution |
| `tcp` | Socket connect |
| `tls` | Full TLS handshake (fresh context per connection — no session resumption) |
| `ttfb` | Request fully sent → first response byte |
| `ttft` | Request fully sent → first content token in the SSE stream |
| `cold e2e ttft` | `dns + tcp + tls + ttft` — what a short-lived process pays end to end |
| `warm ttfb / ttft` | Second request on an already-open connection (the connection-pool case) |

## Method

- Same model, same prompt, same `max_tokens`, authenticated streaming POSTs
  on every gateway — no unauthenticated edge responses counted as data.
- Cold runs use a fresh TLS context per connection, so every run pays the
  full handshake.
- Warm runs complete one throwaway request, then measure a second request on
  the same socket.
- Runs interleave round-robin across gateways to cancel time-of-day drift.
- Request-id headers (`x-vercel-id`, `cf-ray`, …) are captured as receipts.

## Setup

```sh
cp config.example.json config.json   # then fill in your endpoints/models
cp .env.example .env                 # then add your API keys
```

`config.json` accepts any OpenAI-compatible chat-completions endpoint, plus
per-gateway overrides for auth header and extra headers (see the Cloudflare
example, which shows a path-based gateway with a secondary auth header).
`$VARS` in `auth_value` and `extra_headers` are expanded from the
environment.

## Run

```sh
set -a; source .env; set +a
python3 bench.py config.json
```

Prints per-run lines while it works, then a medians table in markdown,
receipts, and any per-run errors. Raw per-run results are dumped to
`results-<timestamp>.json` next to the config.

## Reading the results honestly

- Medians of small runs are indicative, not definitive. Increase
  `runs_cold` / `runs_warm` for tighter numbers, and look at the raw JSON
  for spread — connection-phase variance is often more informative than the
  median.
- Results are a property of *your* vantage point (region, ISP, transit),
  not a global ranking. The same config from another country can invert the
  table.
- Some gateways route a given model across multiple upstream providers;
  TTFT then reflects whichever upstream served the run.
- A gateway configured to proxy through another gateway (double hop)
  overstates the outer gateway's own overhead. Compare like with like.
- `cold` here means a new connection, not a provider-side cold start.

## License

MIT
