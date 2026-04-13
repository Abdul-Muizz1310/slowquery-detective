# Why slowquery-detective?

## The obvious version

The obvious version of this tool is an APM vendor — ship logs to Datadog or New Relic, stare at graphs, click through flame charts. That model makes you leave your editor to find answers that are locally knowable. Everything a Postgres DBA would ask about a slow query — "is this a seq scan? is there an index on the WHERE column? what's the p95 for this query pattern over the last minute?" — is answerable with data already in the process. There is no reason it needs to travel through a vendor first.

## Why I built it differently

Three deliberate design calls separate this from an APM. First, **fingerprint, not log**: `WHERE id=1` and `WHERE id=2` collapse into one actionable row instead of 10,000, because you fix queries by pattern, not by instance. Second, **rules first, LLM as fallback**: real performance wins are boring and deterministic — a seq scan on a WHERE column is the same fix whether you're on Postgres 12 or 16, so a six-rule engine handles the common cases without touching a model. The LLM is there for the long tail — plans where `Sort` nodes, CTE materializations, and `WHERE` clauses interact in ways a rule engine can't catch. When the rules engine has an answer, the LLM is never consulted; when it doesn't, the LLM returns a plain-English diagnosis plus a concrete DDL suggestion, or abstains. Third, **`EXPLAIN` off the request path**: running `EXPLAIN ANALYZE` on a slow query doubles the latency if you do it inline, so it has to be async and rate-limited per fingerprint. The goal is a tool that tells you *what to do next*, not one that asks you to interpret a flame chart.

## What I'd change if I did it again

I'd add a flame chart visualization of EXPLAIN plans — ironic given the APM critique, but the difference is that the data stays local and the visualization is read-only, not a gateway to a billing page. A tree view of nodes with width proportional to cost would make complex plans legible to developers who've never read raw EXPLAIN output. I'd also add MySQL support alongside Postgres: the fingerprinting and rules engine are dialect-agnostic in principle, but the EXPLAIN parser and DDL suggestions are Postgres-specific today, and broadening that would double the tool's audience.
