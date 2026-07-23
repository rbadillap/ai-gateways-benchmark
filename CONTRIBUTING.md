# Contributing

Contributions are welcome, including from the vendors being measured. This
document is the standard every change is held to.

## The one rule

Every change must apply the same method to every gateway. Before opening a
PR, ask: would this change be accepted if it had the opposite effect on the
vendors involved? If the answer depends on who wins, it fails the standard.

## Vendor affiliation

Working for, or with, a measured vendor does not disqualify you. Hiding it
does. State your affiliation in the PR description.

PRs are judged on measurement correctness, never on outcome. A change
argued from results ("this makes vendor X look wrong") will be closed. The
same change argued from method is welcome.

## What makes a good contribution

- Bug fixes with a reproduction.
- New gateway or provider examples, in their shortest production
  configuration (see [METHODOLOGY.md](METHODOLOGY.md)). Chained
  topologies must be named after their full path.
- Accuracy improvements: timing precision, protocol correctness, better
  error labeling.
- Documentation that helps people measure honestly.

## Methodology changes need an issue first

Changes that affect what the numbers mean (HTTP version, connection
handling, timing anchors, run scheduling) break comparability with results
measured before them. Open an issue before writing code. If adopted, the
change lands with release notes so any published result can cite the
version it was measured with.

## Code constraints

- Python standard library only. Zero dependencies is a feature: anyone can
  audit the entire tool in one sitting.
- No telemetry, and no network calls other than the measured requests.
- The tool reports numbers, receipts, and errors. It draws no conclusions.
  PRs adding rankings, verdicts, or promotional language to the output or
  the docs will be closed.

## Sharing results

When you post results in an issue or PR, include the vantage point (country
or region), the date, the config used, and the raw results file. Numbers
without a "from where" and a "with what" are not results.

Where practical, measure with accounts that are not publicly associated
with benchmarking, so no gateway can special-case known accounts. This
applies to every gateway equally.
