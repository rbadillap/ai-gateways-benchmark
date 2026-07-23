# Methodology

Imagine you want to measure how fast a courier delivers packages. In your
test, the courier does not deliver directly. He hands the package to a
second courier, who runs the rest of the route. The time you record is
both couriers combined. If you publish that number as the first courier's
speed, you are blaming him for a route he never ran.

That is what a chained configuration does: a gateway proxying another
gateway measures the whole chain, not the outer gateway's own overhead.
Time each courier delivering directly before you compare them. When you
publish a chained number, label it as the chain it is.

Everything in this document exists to prevent that kind of honest number
from telling a dishonest story.

## How the tool measures

- Same model, same prompt, same `max_tokens`, authenticated streaming POSTs
  on every gateway. No unauthenticated edge responses counted as data.
- Cold runs use a fresh TLS context per connection, so every run pays the
  full handshake.
- Warm runs complete one throwaway request, then measure a second request
  on the same socket. A server that closes reused connections is reported
  as exactly that: it is data about the endpoint, not a benchmark error.
- Runs interleave round-robin across gateways to cancel time-of-day drift.
- Request-id headers are captured per run as receipts, so any published
  number can be traced to the requests that produced it.

## Baselines and topology

- **Include a provider-direct row** (no gateway at all) whenever you can.
  With a baseline in the table, every gateway's numbers can be read as
  overhead relative to going direct, which removes vendor-vs-vendor
  framing entirely: each gateway competes against the network, not against
  the row below it.[^1]
- **Name configs after their full path.** A gateway proxying another
  gateway (`cloudflare-openrouter`) is a different topology than the same
  gateway fronting a provider directly (`cloudflare-anthropic`), and the
  results table should say which one was measured. Prefer each gateway's
  shortest production configuration; add chained rows only deliberately,
  clearly labeled.
- **Bare vendor name = the vendor's canonical configuration.** When a
  vendor states which configuration they consider representative of their
  gateway, that config carries the bare name (`cloudflare`); every other
  topology gets an explicit suffix (`cloudflare-anthropic`). Any vendor is
  welcome to declare theirs.

## Reading results honestly

- Medians of small runs are indicative, not definitive. Increase
  `runs_cold` / `runs_warm` for tighter numbers, and look at the raw JSON
  for spread. Connection-phase variance is often more informative than the
  median.
- Results are a property of your vantage point: region, ISP, transit. The
  same config from another country can invert the table.
- Results are also a property of the moment. The same gateway can shift
  its TTFT by double-digit percentages between sessions, so a single run
  is a snapshot, not a truth.
- Some gateways route a given model across multiple upstream providers.
  TTFT then reflects whichever upstream served that run.
- When you share results, include the vantage point (country or region),
  the date, the config used, and the raw results file. Numbers without a
  "from where" and a "with what" are not results.

[^1]: Provider APIs commonly sit behind CDNs themselves, so the direct row
    measures the shortest available path, not a mythical raw origin.
