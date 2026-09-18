# Volmageddon — Combined Blueprint & Feature Registry

Single source of truth for what this platform is, what it needs, and where each piece stands.
Merged from the two planning documents:

| Ref | Document | Character |
| --- | --- | --- |
| **A** | *Vol Desk Blueprint* (`vol-desk-blueprint.html`, Sep 2026) | Lean build plan. Moontower-parity framing, 5 phases, ETF-first universe, monetization paths. |
| **B** | *Volatility Intelligence Platform & Retail Hedge Fund Blueprint* (`.docx`, v1.0) | Institutional spec. 9 phases, Bronze/Silver/Gold, SPX-first, strategy books, risk governance, promotion gates. |

Where the two disagree, the disagreement is recorded in [§8 Open decisions](#8-open-decisions) rather
than silently resolved. B is the more rigorous document and is treated as the spine; A contributes
the commercial framing and the near-term sequencing that B's phase 0 leaves open.

---

## 1. How to track work in this document

Every capability has a stable ID (`DATA-04`, `SURF-08`, …). IDs never change or get reused, so they
can be referenced from commit messages, branches, and the research ledger. Add new features at the
end of their group; never renumber.

| Status | Meaning |
| --- | --- |
| `done` | Implemented **and** validated against live or historical data. Evidence noted. |
| `partial` | Works for the current narrow case; named gap remains. |
| `todo` | Accepted as in scope, not started. |
| `deferred` | Deliberately postponed — reason recorded, usually a dependency or model-governance concern. |
| `blocked` | Cannot proceed; external constraint (subscription tier, missing feed). |

Current totals across 93 tracked features: **15 done · 19 partial · 53 todo · 5 deferred · 1 blocked**.
That is 16% complete by count. Collection plus the SPX drilldown (`SURF-09`) are the working
surface; `SIG` strategy scanners, `STRAT`/`PORT`/`EXEC`, and a fitted SVI remain untouched.

> **Status discipline.** `done` requires evidence, not just code. Every `done` row below either has a
> measured validation in §7 or is directly exercised by a script in `scripts/`. If a change can't
> reproduce the sanity checks in [§7.2](#72-standing-sanity-checks), it broke something.

---

## 2. Reconciled mandate

Both documents describe the same system with different emphasis. Combined statement of intent:

> Build a volatility operating system — data, analytics, research, portfolio, and execution tools —
> that answers where volatility is mispriced relative to a defensible forecast, which listed
> instrument expresses that view most efficiently, and whether the apparent edge survives realistic
> costs and risk constraints.

The workflow it must serve (B): **observe → forecast → structure → execute → size → monitor →
attribute → learn.** A tool earns its place only by improving a repeatable decision in that chain.

Two framings worth keeping side by side:

- **A's honesty clause — infrastructure isn't edge.** Everything this platform surfaces comes from
  public options data. The build reaches parity with what's purchasable, not past it. Edge, if any,
  comes from proprietary combination logic, cross-book signals a US-only vendor can't build, and
  disciplined execution — all validated statistically first.
- **B's no-guarantee clause.** No platform guarantees profitable strategies. The goal is to raise
  the probability of finding genuine edge, reject false discoveries early, and stop a valid signal
  from being destroyed by sizing, execution, or hidden tail risk.

### Definition of success (B, adopted verbatim in substance)

1. Data can reconstruct what was knowable at every decision timestamp — no look-ahead, no silent repairs.
2. Every signal has an economic reason, an explicit failure mode, and an out-of-sample result.
3. Returns stay inside agreed drawdown, liquidity, and tail-loss tolerances **after** realistic costs.
4. P&L attribution explains whether returns came from delta, gamma, theta, vega, skew, carry, timing, or residual.

### Monetization paths (A) — and what each still needs

| Path | Mechanism | Still required beyond this build |
| --- | --- | --- |
| Trade own book | Size and time positions off VRP, skew, dealer-positioning signals | A validated, cost-adjusted edge. The pipeline has zero expectancy until a signal is backtested net of slippage and fees and sized with real risk rules. |
| License signals | Sell scanner output or a GEX feed | A product: uptime, paying audience, and redistribution terms cleared with IBKR/ThetaData. A separate business. |
| Fund infrastructure | Tech backbone for a research desk / capital raise | Registration, compliance, audited track record, prime brokerage. Necessary groundwork, nowhere near sufficient. |

*Not financial or legal advice. Fund formation involves securities registration work outside this scope.*

---

## 3. Universe

B centres on cash-settled index options; A centres on the liquid ETFs already traded. Merged, with
B's priority ordering and A's breadth (see [D-01](#d-01--universe-priority)):

| Tier | Instruments | Role | Implemented |
| --- | --- | --- | --- |
| Primary | SPX, **SPXW** | Core surface, VIX linkage, deepest index vol | yes |
| Primary | NDX | Tech-heavy comparison, cross-index RV | yes |
| Secondary | SPY, QQQ | ETF comparison, liquidity, execution | yes |
| Secondary | IWM, XBI, SOXX | Cross-sectional breadth (A's existing book) | yes |
| Volatility | VIX options | Tail convexity, vol-of-vol | yes |
| Volatility | VIX spot, VXN, VOLQ | Regime + cross-market confirmation | VIX spot via IBKR only |
| Futures | ES, NQ, VX | Forward construction, hedging, VIX term structure | **no** — see `DATA-09` |
| Carry | Treasury/OIS curve, dividends, borrow | Arbitrage-consistent forwards | implied via parity — see `DATA-11` |

**SPXW is a separate OPRA root, not a detail.** ThetaData keys options by root: `SPXW` carries ~15.5k
contracts vs ~2.9k for `SPX`, so most SPX volume lives there. IBKR models the same contracts as a
trading class *under* `SPX`, making `SPXW` an invalid IBKR symbol — hence `thetadata_extra_roots` in
`config/universe.yaml`, read only by the ThetaData poller.

---

## 4. Data reality — what the subscriptions actually permit

**This section is load-bearing.** Both blueprints assumed capabilities the account does not have.
Every entry below was established by probing the local Terminal, not read from documentation.

### 4.1 ThetaData (options STANDARD · stock FREE · index FREE)

| Endpoint | Result |
| --- | --- |
| `/v3/option/snapshot/quote` | 200 — full-chain NBBO |
| `/v3/option/snapshot/ohlc` | 200 — session OHLC, volume, trade count |
| `/v3/option/snapshot/open_interest` | 200 — previous-session OI |
| `/v3/option/history/eod` | 200 — **from 2016-01-01 only** |
| `/v3/option/history/open_interest` | 200 — same window |
| `/v3/option/snapshot/greeks/*` | **403** — needs PROFESSIONAL |
| `/v3/option/history/greeks`, `/implied_volatility` | **404** — not in v3 |
| `/v3/stock/snapshot/quote` | **403** — needs VALUE |
| `/v3/stock/history/eod` | 200 — ~2.5 years only, max 365 days/request |
| `/v3/index/snapshot/price` | **403** — needs STANDARD |
| `/v3/index/history/eod` | **403** — beyond the last few weeks |
| `/v2/*` | **410 Gone** — current Terminal is v3-only, on port 25503 |

### 4.2 Three consequences that shape the architecture

1. **Greeks and IV are never vendor-supplied, at any tier held, for realtime or history.** They are
   always computed locally. B's requirement to "compare vendor Greeks to internal calculations"
   therefore has only one possible counterparty: IBKR's `modelGreeks` (`QUAL-05`).
2. **There is no usable underlying price series** — not spot, not index level, not deep stock
   history. The forward is recovered from the option chain by put-call parity instead. This is
   *better* than a spot feed rather than a workaround: the parity forward already contains the
   market's dividend and borrow assumptions, and for VIX it is the only correct input, since VIX
   options price off the VIX future for their expiry rather than spot VIX. It also directly satisfies
   B §5.1's "infer forward and discount factors using robust put-call parity".
3. **Options history starts 2016-01-01, not 2012** (A's assumption). A hard calendar cutoff, not a
   rolling window — 2015-12-31 is 403, 2016-01-01 succeeds — so it will not drift forward.

### 4.3 Regime coverage available from 2016

Relevant because B warns that six months of self-collected data "will not provide enough independent
market regimes for final confidence."

| Included | Missing |
| --- | --- |
| Feb 2018 VIXplosion · Q4 2018 · **Mar 2020 COVID** · 2022 bear/high-rate · Aug 2024 yen-carry unwind · 2023–26 low-vol carry | 2008 GFC · 2010 flash crash · 2011 US downgrade · 2015 August shock |

Adequate for walk-forward across several distinct regimes; **not** adequate for a true credit-crisis
stress. Tail work must lean on scenario shocks (`PORT-03`), not just historical replay.

---

## 5. Architecture

B's four zones separate immutable evidence from corrected data and derived intelligence, so every
model can be re-run after a bug fix without losing original observations.

| Zone | Purpose | Current state |
| --- | --- | --- |
| **Bronze** | Immutable provider-native capture | **Not separated** — see [D-03](#d-03--bronzesilvergold-separation) |
| **Silver** | Verified canonical market data | Partial: normalized, deduped, pinned schema; no verifier or quarantine |
| **Gold** | Features and research datasets | Surface (IV + greeks) computed inline, not versioned |
| **Operational** | Collector health, alerts, run state | Console logs only |

Today's pipeline writes a single 44-column row per contract that mixes Bronze-grade observations
(bid, ask, sizes, volume, OI) with Gold-grade derivations (forward, IV, greeks). Convenient for
research, wrong for lineage. Resolving this is `DATA-15`.

### 5.1 Current storage layout

```
data/thetadata/{root}/dt={date}/{HHMMSS}.parquet    # realtime, one file per poll
data/thetadata/{root}/dt={date}/compacted.parquet   # after run_compaction.py
data/thetadata_eod/{root}/dt={date}/eod.parquet     # historical EOD
data/thetadata_eod/{root}/_complete/{YYYY-MM}       # backfill resume markers
data/ibkr/{root}/dt={date}/{HHMMSS}.parquet         # realtime, narrowed chain
```

Realtime and historical ThetaData rows share one identical 44-column schema in the *code*, so a
scanner reads across both without knowing the origin. Provider directories stay separate until a
verifier establishes a normalized view (B §1.3).

> **On disk this is not yet true.** `data/thetadata/` currently holds 42-column files written by
> pre-refactor code, missing `session` and `last_trade_timestamp`, and its first poll per root wrote
> the three vendor timestamps as `String` where every later file used `Datetime(ms)` — so
> `read_parquet` over that directory raises `SchemaError` and only `compacted.parquet` is readable.
> It must be cleared and re-collected. `data/thetadata_eod/` is correct at 44 columns.

### 5.2 Canonical greeks convention

Black-76 with respect to the **forward**, since the forward is what we observe. Driftless, which is
what surfaces/VRP/skew want. `forward` and `discount` ride along per row so spot greeks are one step
away: `dF/dS = F/S`, so spot delta `= delta · F/S`, spot gamma `= gamma · (F/S)²` — ~1.006 for SPY,
immaterial for GEX aggregation but exact once IBKR spot is joined.

Units: `vega` per 1.00 of vol; `theta` and `charm` per calendar day; rest raw. Realtime `tau` runs to
16:00 ET on the expiration date so 0DTE gets a real fraction of a day; EOD history is stamped at the
close, so `tau` is whole days and same-day expiries are zero (already settled — no IV, correct).
AM-settled expiries (SPX monthlies, VIX) are overstated by a few hours; uncorrected.

---

## 6. Unified phase roadmap

A's 5 phases and B's 9 map onto one sequence. B's ordering wins because the surface is upstream of
skew, term structure, VRP, structure ranking, and anomaly detection — a weak contract master or
forward estimate contaminates every downstream feature.

| Phase | Deliverable | Exit criterion | A | B | Status |
| --- | --- | --- | --- | --- | --- |
| **0** | Decisions: universe, sampling, schema, risk principles | Blueprint reviewed, decisions recorded | 0 | 0 | **this document** |
| **1** | Collector hardening: contract master, IBKR/Theta capture, UTC model, manifests, heartbeats | Reliable daily capture with measured coverage and latency | 1 | 1 | `partial` |
| **2** | Verifier & Silver: dedupe, staleness, quote bounds, cross-provider checks, quarantine | Replayable canonical chain snapshots | — | 2 | `todo` |
| **3** | Historical backfill: ThetaData EOD 2016+, IBKR underlying bars | Point-in-time history in the same schema | 2 | (1) | `partial` |
| **4** | SPX drilldown: forward, IV solver, surface, term, skew, liquidity & quality panels | Independent parity/IV checks, stable daily output | 3 | 3 | `partial` |
| **5** | RV & VRP: realized-vol suite, forecasts, percentiles, event/session decomposition | Point-in-time features with validation report | 3 | 4 | `todo` |
| **6** | Strategy lab: structure ranker, scenarios, costs, backtester, attribution | First hypotheses accepted/rejected out of sample | 4 | 5 | `todo` |
| **7** | Portfolio cockpit: books, aggregate greeks, stresses, limits, research ledger | Paper portfolio under governance | — | 6 | `todo` |
| **8** | Execution pilot: IBKR combos, order controls, reconciliation, small capital | Live results reconcile, stay within limits | — | 7 | `todo` |
| **9** | Expansion: NDX/QQQ, VIX complex, collars, events; later dealer/dispersion | Each module passes the same gates | — | 8 | `deferred` |

**B's proposed approval: phases 0–4 only.** Harden collection, build verification/Silver, deliver the
SPX drilldown with a validated surface. Review evidence before implementing strategy automation.

Note phase 3 is further along than phase 1 or 2 — history was pulled before the collector was
hardened or a verifier existed. That inversion is deliberate (the tier's 2016 floor and the data's
value made it worth grabbing early) but it means backfilled data has *not* passed a verifier.

Phase 4's SPX drilldown (`scripts/run_surf09.py`) is now the working UI: liquidity-filtered term,
skew, forwards, forward variance, IV rank, and quality on EOD + live last-snapshot. It is still
`partial` because there is no fitted surface (`SURF-04`) and no executable bid/ask IV (`SURF-13`).

---

## 7. Feature registry

### 7.1 Groups

`DATA` sourcing & storage · `QUAL` quality & verification · `SURF` surface & market intelligence ·
`RV` realized volatility · `SIG` signals & scanners · `STRAT` strategy research · `PORT` portfolio &
risk · `EXEC` execution & operations · `GOV` governance · `UI` dashboards

### DATA — sourcing & storage

| ID | Feature | Src | Ph | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| DATA-01 | ThetaData realtime full-chain snapshot (quote + ohlc + OI) | A B | 1 | `done` | 3 bulk calls/root; 39,920 rows across 9 roots in 13.8s |
| DATA-02 | IBKR realtime narrowed chain snapshot + model greeks | A B | 1 | `done` | Nearest N expiries × M strikes; per-expiry `reqContractDetails` avoids invalid combos. Output schema now pinned, and IBKR's NaN/-1 "no data" sentinels are nulled per field — greeks keep legitimate −1 values, prices and volumes do not |
| DATA-03 | Partitioned Parquet lake with pinned schema | A B | 1 | `done` | `_OUTPUT_SCHEMA` written every poll regardless of which endpoints answered |
| DATA-04 | ThetaData EOD option history backfill, 2016+ | A B | 3 | `done` | Month-chunked, resumable via `_complete/` markers; `expiration=*` makes it tractable. **Lake currently starts 2024-01-02** (2016 subscription floor unused by choice). SPX Sep EOD still missing 09-01/02/03/08/09; SPXW missing 08/09 |
| DATA-05 | Compaction + change-based dedup | A | 1 | `done` | ~390 files/root/session → 1; row saving scales with illiquidity (XBI 68%, SPY 97%). **Gotcha:** a deduped file is a *change log*, not a snapshot series — to get state at time *t*, take the last row per contract at or before *t*. A scanner that naively groups by `poll_timestamp` will under-count contracts |
| DATA-06 | Trading-session gating | A B | 1 | `done` | NYSE calendar via `pandas_market_calendars`, so holidays and 13:00 ET half-days both gate correctly. Replaced a weekday + 9:30–16:00 test that treated Labor Day as a normal session |
| DATA-07 | Exact contract master, versioned, provider IDs | B | 1 | `todo` | B §3.3: never build chains from expiry × strike Cartesian product. Store exact contracts from definition endpoints, keyed by provider ID + canonical key |
| DATA-08 | IBKR historical bars for underlyings | A B | 3 | `done` | `scripts/run_ibkr_backfill.py`, `data/ibkr_eod/{sym}/daily.parquet`. Cash/index 2684 NYSE days 2016-01-04 → 2026-09-08 (09-09 not pulled yet). ThetaData stock/index history still gated |
| DATA-09 | Futures capture (ES, NQ, VX) + curve | B | 9 | `partial` | Daily bars only, front-month stitch of dated contracts + ContFuture. **IBKR will not resolve 2016 expiries** (Error 200). ES 802 bars from 2023-06-20; NQ 622 from 2024-03-18; VX 401 all-hours TRADES from 2025-02-05 (RTH was 16 bars). No VX term *curve* of back months. Closes shown on `SURF-09` with those start dates labelled |
| DATA-10 | Vol indices (VIX, VXN, VOLQ) + term measures | B | 9 | `partial` | VIX options via ThetaData; VIX spot via IBKR; VXN/VOLQ absent |
| DATA-11 | Rates / dividend / borrow inputs | B | 4 | `partial` | Parity forward implies dividends + borrow; discount uses one flat `THETADATA_RATE` |
| DATA-12 | Versioned event calendar (CPI, FOMC, payrolls, earnings, expiries) | B | 5 | `todo` | Exchange holidays are now handled by `DATA-06`; this is the macro/earnings calendar needed by `RV-05` and `SIG-04` |
| DATA-13 | Underlying intraday ticks / 1s / 1m bars | B | 5 | `todo` | Needed for realized vol, hedging, event alignment, replay |
| DATA-14 | Collector manifests, heartbeats, run state | B | 1 | `todo` | Console logs only today |
| DATA-15 | Bronze / Silver / Gold zone separation | B | 2 | `todo` | Raw observations and derived surface currently share one row — see [D-03](#d-03--bronzesilvergold-separation) |
| DATA-16 | Canonical UTC time model | B | 1 | `partial` | Have provider ts (naive ET) + UTC poll ts + session date. Missing explicit `event`/`received`/`stored` split and drift monitoring |
| DATA-17 | ATM / 25Δ / 10Δ high-frequency watchlist streams | B | 4 | `todo` | B §4.2: selected points more frequent than the full-chain snapshot |
| DATA-18 | Provider-separated storage until verified | B | 1 | `done` | Separate lake roots per source |
| DATA-19 | Full-chain **synchronized** snapshot cadence | B | 1 | `partial` | 60s, at the fast end of B's 1–5 min, ~1.7 GB/day — see [D-06](#d-06--sampling-cadence-and-storage). **Not synchronized:** roots are polled sequentially, so one cycle staggers them over ~14s. B §4.2 wants timestamp alignment across the chain for surface research; cross-index RV (`SIG-02`) is where this bites |
| DATA-20 | Retain expired / delisted contracts | B | 3 | `done` | Backfill keeps every expiry as of each session; nothing is pruned |

### QUAL — quality & verification

| ID | Feature | Src | Ph | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| QUAL-01 | Per-record quality fields (source, versions, staleness, crossed flags, reason codes) | B | 2 | `partial` | Have source dir, timestamps, sizes, conditions. Missing collector/schema version, staleness, validation status, reason codes. Also: on the EOD path `ohlc_timestamp` is aliased from `quote_timestamp` and `poll_timestamp` records when the *backfill ran*, not an observation time — both are synthesized values presented in observation columns |
| QUAL-02 | Canonical key uniqueness | B | 2 | `partial` | Dedup on join keys at ingest and in compaction; not asserted as an invariant |
| QUAL-03 | Crossed / locked / stale quote quarantine | B | 2 | `todo` | `SURF-09` *counts* crossed quotes; they still are not isolated into a quarantine table. Currently crossed quotes also fail to produce IV |
| QUAL-04 | No-arbitrage price-bound validation before IV solve | B | 4 | `partial` | Solver rejects prices outside the model's own range; no explicit pre-solve bound check or reporting |
| QUAL-05 | Cross-provider reconciliation (IBKR vs internal IV/greeks) | B | 2 | `partial` | **Unblocked.** The join used to return zero rows because the two feeds formatted `expiration` differently; both now emit a `Date` holding the last trading day. 44–144 contracts per root now reconcile — see §7.2. Still needs to run as a standing report rather than on demand |
| QUAL-06 | Put-call parity residual monitoring | B | 2 | `partial` | Measured (see §7.2). `SURF-09` shows ATM |call IV − put IV| and Fwd − SPX per session. Not yet a standing report with tolerances |
| QUAL-07 | Coverage / freshness / integrity reports + alerts | B | 2 | `todo` | B §13 acceptance measures |
| QUAL-08 | Reproducibility: same Bronze + version → same Gold | B | 2 | `blocked` | Needs `DATA-15` first; can't reproduce a zone that doesn't exist separately |
| QUAL-09 | Schema drift detection / alignment | B | 1 | `partial` | Both sources pin schema on write; compaction and `rewrite_day_to_schema` align. Live **09-08** rewritten to 44-col Date (0 corrupt). Live **09-09** still mixed 42/44 (~half); `SURF-09` aligns the last snapshot. Live **09-07** Labor Day left as 42-col and excluded from the drilldown. No automatic detector |
| QUAL-10 | Null-as-information discipline | B | 1 | `done` | Unsolvable IV → null, never 0. NaN converted to null on write |
| QUAL-11 | Fatal vs transient error separation | — | 1 | `done` | `fatal = True` stops the loop; transient logs and continues. Added after 403s spammed every symbol every interval |

### SURF — surface & market intelligence

| ID | Feature | Src | Ph | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| SURF-01 | Forward & discount inference | A B | 4 | `partial` | Spread-weighted put-call parity per (session, expiry). `SURF-09` plots the forward vs DTE and Fwd − SPX. Futures-based ES/SPX basis is a labelled close overlay, not a contract-matched hedge |
| SURF-02 | IV solver | A B | 4 | `done` | Vectorized bisection on Black-76, 64 iterations; can't diverge on wide quotes |
| SURF-03 | Greeks, 1st through 3rd order | A B | 4 | `done` | delta, gamma, vega, theta, rho, vanna, charm, speed, zomma. Drilldown shows Δ/Γ/vega/θ plus Σ γ·OI·100 (descriptive, not a dealer-GEX model) |
| SURF-04 | Arbitrage-aware surface fit + residuals | B | 4 | `todo` | Fit in delta or forward-moneyness space; expose residuals, weights, exclusions, stability. Raw observations preserved beside fitted values. **Not started** — drilldown is raw filtered IV |
| SURF-05 | Calendar & butterfly arbitrage diagnostics | B | 4 | `partial` | Diagnose only, on `SURF-09`: count of σ²T calendar inversions between consecutive expiries, and count of negative 25Δ butterflies. Not enforced; no three-strike price convexity check |
| SURF-06 | Liquidity filters (spread caps, size, staleness) | A B | 4 | `partial` | `SURF-09` default: bid>0, ask>bid, spread/mid ≤ 8%, size ≥ 1, DTE ≥ 1. Applied before term/skew/smile. **Gap:** no quote-staleness filter. 2026-08-31 EOD: 28,648 → 21,664 kept, 0 crossed |
| SURF-07 | ATM term structure + forward variance | A B | 4 | `done` | `SURF-09`: ATM mid IV vs DTE; strips `[σ²(T₂)T₂ − σ²(T₁)T₁] / (T₂ − T₁)`. 2026-08-31: 0 calendar inversions |
| SURF-08 | Skew scanner (10Δ/25Δ, RR, BF, z-scores) | A B | 4 | `partial` | SPX drilldown, not a universe scanner. RR = IV(put Δ) − IV(call Δ) — puts richer is positive, opposite the call−put wording in §9 (labelled on the page). BF = ½(call+put) − ATM. RR25 z-score vs 2024+ EOD. 2026-08-31 ~30d RR25 **+3.82 vol pts**, z **−0.43σ** |
| SURF-09 | Single-ticker drilldown, SPX first | B | 4 | `done` | `python scripts/run_surf09.py` → http://127.0.0.1:8765/ . SPX+SPXW EOD and live last-snapshot. NYSE sessions only; Labor Day 09-07 live excluded. Validated 2026-08-31 EOD (90% IV solve, ATM 11.65%, PC gap 0.05%) and 2026-09-08 live. Gaps live as other IDs: no SVI (`SURF-04`), mid IV only (`SURF-13`) |
| SURF-10 | IV rank / historical percentiles | A B | 5 | `partial` | 30d ATM percentile vs EOD 2024-01-02 through the selected session (666 cache points). Option lake does not go back to 2016. 2026-08-31 ATM at **24th** percentile |
| SURF-11 | Surface model versioning | B | 4 | `todo` | B §5.1. OptionMatrix may be evaluated as reference, but diagnostics matter more than adopting a library early |
| SURF-12 | Market cockpit (cross-asset regime) | A B | 5 | `todo` | Curves, events, breadth, quality status |
| SURF-13 | Bid / mid / ask surfaces — executable IV | B | 4 | `partial` | Mid only today. Lake does not store bid/ask IV; solver would have to be re-run on each side. B §1.3: "mid-price alone is not a strategy" — see [D-07](#d-07--executable-iv-vs-mid-price) |
| SURF-14 | Cross-sectional vol screening | A | 5 | `todo` | A's Moontower-parity item: rank the universe on one surface metric |

### RV — realized volatility

| ID | Feature | Src | Ph | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| RV-01 | Close-to-close realized vol | A B | 5 | `partial` | `SURF-09` shows 20d (and uses 60d internally) close-to-close SPX RV from `DATA-08`, 252-scaled. SPX only; not Parkinson/GK; not a forecast |
| RV-02 | Parkinson / Garman–Klass estimators | B | 5 | `todo` | High-low and open-close information |
| RV-03 | Intraday vs overnight decomposition | B | 5 | `todo` | Required where trading logic is session-dependent |
| RV-04 | RV forecast with uncertainty | B | 5 | `todo` | B insists on a forecast *distribution*, not a point estimate |
| RV-05 | Event vs non-event variance decomposition | B | 5 | `todo` | Needs `DATA-12` |

### SIG — signals & scanners

| ID | Feature | Src | Ph | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| SIG-01 | VRP scanner | A B | 5 | `todo` | Research label: implied variance − subsequent realized. `SURF-09` shows ATM² − RV20² as a **labelled variance gap**, not this scanner and not a forecast |
| SIG-02 | Relative-value lab (SPX–NDX, SPY–QQQ, VIX–VXN–VOLQ) | A B | 6 | `todo` | Surface spreads, hedge ratios, residual z-scores, convergence history |
| SIG-03 | Tail hedge selector / crisis convexity ratio | B | 6 | `todo` | Scenario package value ÷ executable premium, with scenario definition, elapsed time, surface response, probability |
| SIG-04 | Event vol module (expected move, crush, analogs) | A B | 9 | `todo` | A's "earnings vol suite" folds in here |
| SIG-05 | Signal stability across sampling times & parameters | B | 6 | `todo` | A platform KPI, not an afterthought |
| SIG-06 | Dealer positioning proxies (GEX / vanna / charm) | A B | 9 | `deferred` | **Direct conflict** — A core, B deferred. See [D-02](#d-02--dealer-positioning) |
| SIG-07 | Dispersion / correlation (index vs constituents) | B | 9 | `deferred` | Needs single-name surfaces + weight history |
| SIG-08 | Collar / financing anomaly detection | B | 9 | `deferred` | B explicitly defers the MU collar anomaly; needs dividends + borrow |
| SIG-09 | Income module (covered calls, cash-secured puts) | B | 9 | `deferred` | Assignment-aware return decomposition |
| SIG-10 | Defense module (protective puts, verticals, rolling rules) | B | 9 | `deferred` | Drawdown-budget comparison |

### STRAT — strategy research

| ID | Feature | Src | Ph | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| STRAT-01 | Research funnel enforced as a template | B | 6 | `todo` | 8 stages: hypothesis → signal → forecast → expression → historical test → forward test → small capital → scale/reject |
| STRAT-02 | Strategy spec with mandatory falsification rule | B | 6 | `todo` | Must state what observation disproves it. "The trade will recover" is not a risk rule |
| STRAT-03 | Trade structure ranker | B | 6 | `todo` | Compare expressions of the same view: scenario P&L, carry, greeks, capital, liquidity, payoff efficiency |
| STRAT-04 | Scenario & payoff studio | B | 6 | `todo` | Spot/vol/time surfaces, breakevens, gap scenarios, path-dependent hedge results |
| STRAT-05 | Backtest / replay engine, point-in-time and bid/ask aware | A B | 6 | `todo` | Must state quote side, delay, fill model, fees, spread sensitivity |
| STRAT-06 | Walk-forward + regime-partitioned reporting | B | 6 | `todo` | Calm, selloff, rebound, high-rate, low-rate, event, liquidity regimes |
| STRAT-07 | P&L attribution | A B | 6 | `todo` | delta + gamma/theta + vega + skew/term + execution + residual |
| STRAT-08 | Candidate strategy library | A B | 6 | `todo` | 8 candidates in §7.3 |
| STRAT-09 | Multiple-testing log & confidence adjustment | B | 6 | `todo` | Log every experiment; prefer economically motivated hypotheses |
| STRAT-10 | Bias controls | B | 6 | `todo` | Look-ahead, survivorship, mid-price fantasy, surface leakage, overfitting, regime concentration, hedge idealization |

### PORT — portfolio & risk

| ID | Feature | Src | Ph | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| PORT-01 | Strategy books with risk budgets | B | 7 | `todo` | 6 books in §7.4 |
| PORT-02 | Aggregated greeks across books | A B | 7 | `todo` | Net and gross |
| PORT-03 | Stress scenario engine | B | 7 | `todo` | Scenarios in §7.5 |
| PORT-04 | Layered limits | B | 7 | `todo` | Trade / book / portfolio / operational / governance |
| PORT-05 | Drawdown, liquidity, margin buffers | B | 7 | `todo` | |
| PORT-06 | Short-vol jump-loss estimation | B | 7 | `todo` | Never infer safety from a high win rate |
| PORT-07 | Correlated tail exposure | B | 7 | `todo` | Combine differentiated edges so carry finances protection |

### EXEC — execution & operations

| ID | Feature | Src | Ph | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| EXEC-01 | Execution workbench | B | 8 | `todo` | Limit ladder, combo orders, slippage budget, acknowledgements |
| EXEC-02 | Slippage / fill model | A B | 6 | `todo` | Needed by `STRAT-05` before any live work |
| EXEC-03 | Reconciliation & audit trail | B | 8 | `todo` | |
| EXEC-04 | Kill switches / emergency stop | B | 8 | `todo` | Must be hard-coded before any paper or live order workflow |
| EXEC-05 | Human-approved order staging | B | 8 | `todo` | Research before automation; alerts and proposed trades precede unattended execution |
| EXEC-06 | Systematic entries via IBKR API | A | 8 | `todo` | A's phase 4 signal & execution layer |

### GOV — governance

| ID | Feature | Src | Ph | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| GOV-01 | Research ledger | B | 7 | `todo` | Hypothesis version, parameters, approvals, experiments, post-trade review. Attribute P&L without rewriting the historical signal |
| GOV-02 | Promotion gates | B | 7 | `todo` | Research complete → paper → live pilot → scale → retire |
| GOV-03 | Acceptance scorecard + platform KPIs | B | 2 | `todo` | B §13: coverage, freshness, integrity, time, reproducibility, pricing, execution realism, risk, governance |
| GOV-04 | Immutable strategy versioning | B | 7 | `todo` | Every live strategy maps to an approved hypothesis and immutable version |
| GOV-05 | Mandatory performance report | B | 6 | `todo` | B §9.1, with appropriate caution for non-normal option returns |

### UI — dashboards

Ordered as B §11.1 requires — system health first, opportunity set fourth. A dashboard that leads
with trade ideas hides the data quality they depend on.

| ID | Feature | Src | Ph | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| UI-01 | System health panel | B | 2 | `todo` | Sources, timestamp lag, chain coverage, invalid/crossed quotes, delayed/live status |
| UI-02 | Portfolio risk panel | B | 7 | `todo` | Stress P&L, greeks, drawdown, liquidity, upcoming event exposure |
| UI-03 | Market regime panel | B | 5 | `partial` | Slice lives on `SURF-09`: IV rank, 20d RV, RR z-score, SPX path, labelled ES/NQ/VX closes. No events, no cross-index breadth |
| UI-04 | Opportunity set | B | 6 | `todo` | Ranked hypotheses with confidence, capacity, expected edge, failure conditions |
| UI-05 | Trade detail + approval checklist | B | 6 | `todo` | Scenario cube, executable package, costs, sizing |

**Four prohibitions (B §11.2)** — the dashboard must not: hide data-quality uncertainty behind a
single score; rank trades solely by theoretical return or cheap nominal premium; show vendor greeks
without model inputs, timestamps, and comparison to internal calculations; auto-execute on a
threshold before forward testing and operational controls exist.

### 7.2 Standing sanity checks

Reproduce these after any change to the surface code. All measured against live/historical data.

| Check | Expected | Last measured |
| --- | --- | --- |
| Put/call IV agreement at same strike, live NBBO | ≲0.001 vol | **0.0005** (767C 0.07246 vs 767P 0.07200) |
| Put/call IV agreement, EOD, tight-spread near-the-money | ≲0.005 vol | **0.003** (crash) / **0.004** (calm) |
| Put/call IV agreement, EOD, all pairs | regime-independent | **0.0249** Mar-2020 vs **0.0225** Jun-2024 — stable across regimes, so the tail is stale wing quotes, not a model error |
| SPY parity forward vs actual close, Mar 2020 | within ~1pt | 273.98/274 (9th), 240.79/239 (16th), 229.20/228 (20th) |
| SPY 30d ATM IV peak, Mar 2020 | ≈VIX 82.69 | **77%** on 2020-03-16 |
| Aug 2024 yen-carry spike visible | yes | XBI ATM IV 0.379 on 2024-08-05 |
| Forward monotonic in tenor | yes | 770.07 → 774.62 across the curve |
| ATM term structure smooth | yes | 5.7% → 12.9% contango |
| 25Δ risk reversal sign | positive, steepening | +0.96 → +3.65 vol pts |
| VIX forward is the *future*, not spot | yes | 18.03 |
| Chain IV solve rate | 85–92% index, lower on ETF wings | 93–95% SPX/SPXW/NDX, 88% SPY, 67–78% SOXX/XBI/QQQ/IWM (deep-ITM and unquoted-wing nulls are correct) |
| Internal IV vs IBKR `modelGreeks`, index, dte≥5 | ≲0.005 vol | **SPX mean +0.0000, std 0.0004** · NDX mean −0.0014, std 0.0014 |
| Internal IV vs IBKR `modelGreeks`, ETF, dte≥5 | ≲0.03 vol | median 0.011–0.022 across SPY/QQQ/IWM/XBI/SOXX; VIX −0.0057 |
| Internal IV vs IBKR, **0DTE** | expect disagreement | median 1.2–1.7 vol. Not a defect: at dte 0 almost no time value remains, so IV is hypersensitive to the tau convention and ours runs to 16:00 ET while IBKR's differs |
| Rolled-back Saturday expiry matches the same contract | bid/ask agree to cents | $0.38 max on dte 0, versus $0.84–1.01 on dte 5–7 — same order, so contract identity is confirmed |

### 7.3 Candidate strategy library (`STRAT-08`)

| Strategy | Signal / forecast | Structure | Essential tools |
| --- | --- | --- | --- |
| Conditional VRP | IV exceeds forecast RV by enough to cover jumps and costs | Defined-risk credit spread; researched delta-hedged straddle | `RV-04` `SURF-04` `SIG-01` `PORT-03` |
| Long gamma | Forecast RV exceeds executable IV | Straddle/strangle with hedge rule | `DATA-13` `EXEC-02` `STRAT-07` |
| Downside skew premium | Put skew rich vs history and forecast tail | Put spread / fly, bounded loss | `SURF-08` `SIG-03` `STRAT-04` |
| Term structure | Forward variance rich/cheap around a calendar | Calendar or diagonal | `SURF-07` `DATA-12` |
| Macro event vol | Implied event move differs from forecast | Event straddle/calendar, strict limits | `SIG-04` `RV-05` |
| Tail convexity | Crisis payoff undervalued vs bleed | SPX put/put spread or VIX call spread | `SIG-03` `DATA-09` |
| Cross-index RV | SPX/NDX vol residual unusually wide | Hedged option packages | `SIG-02` |
| Intraday/overnight VRP | Premium differs from realized session component | Session-specific gamma | `DATA-13` `RV-03` |

**Sources of plausible edge (B §7.1):** risk transfer (investors structurally pay for downside
insurance) · forecasting (RV estimable better than the implied benchmark in selected regimes) ·
relative value (inconsistent pricing across strike/maturity/underlying mean-reverting after hedge and
cost) · event decomposition (variance misallocated between event and non-event days) · execution
(patient package execution and better liquidity filters preserve a small edge).

### 7.4 Strategy books (`PORT-01`)

| Book | Objective | Expressions | Primary danger |
| --- | --- | --- | --- |
| Core VRP carry | Harvest persistent IV over subsequent RV | Defined-risk SPX spreads | Crash/jump losses, clustered drawdowns |
| Skew & term RV | Trade relative richness, not direction | Verticals, calendars, diagonals, flies | Surface marks may not be executable |
| Tactical volatility | Buy/sell gamma when forecast ≠ implied | Straddles, strangles, calendars | Timing error, theta bleed |
| Tail convexity | Cap portfolio loss, monetize shocks | SPX/NDX puts, put spreads, VIX call spreads | Premium bleed, wrong maturity |
| Event volatility | Event premium vs realized move | Event straddles/calendars | Binary gaps, regime dependence |
| Cross-market RV | Compare related vol markets | SPX–NDX, SPY–QQQ, VIX–VXN–VOLQ | Basis, hedge-ratio, liquidity risk |

**Portfolio objective:** do not maximise any single book. Combine differentiated edges so carry
finances research and protection, while convexity stops a short-vol book becoming a disguised ruin trade.

### 7.5 Required stress scenarios (`PORT-03`)

SPX/NDX spot shocks ±2%, −5%, −10%, −20% including overnight gaps · parallel and skewed IV shocks ·
term-structure inversion · VIX/VXN basis dislocation · vol crush after an event with little
underlying movement · liquidity shock (wider spreads, reduced size, delayed/cancelled fills, hedge
slippage) · rate/dividend/forward input errors · early exercise and assignment · provider outage,
delayed-data fallback, timestamp drift, partial-chain capture.

### 7.6 Feature-to-data dependency matrix

B Appendix A, extended with an availability column reflecting §4.

| Feature | Chain | Underlying | Rates/divs | Futures/vol idx | Events/history | Available now? |
| --- | --- | --- | --- | --- | --- | --- |
| Surface / skew | required | required | required | helpful | percentiles | **yes** — `SURF-09` on SPX+SPXW |
| Term / forwards | required | required | required | helpful | curves | **yes** |
| Realized vol | no | required | no | helpful | required | **partial** — close-to-close SPX via `DATA-08`; no `DATA-13` |
| VRP scanner | required | required | required | helpful | required | no — labelled ATM²−RV² only, not `SIG-01` |
| Tail selector | required | required | required | **required** | crisis history | no — needs `DATA-09` curve + GFC; 2016 floor excludes GFC |
| Event vol | required | required | required | helpful | calendar/analogs | no — needs `DATA-12` (no calendar file in repo) |
| Cross-index RV | both | both | required | required | required | partial — chains yes; ES/NQ/VX dailies short and labelled |
| Collar anomaly | required | required | + divs/borrow | helpful | corporate actions | no |
| Dealer proxies | required + OI | required | required | helpful | historical OI | **chain data yes** — OI captured; `SURF-09` shows Σγ·OI·100; deferred as a dealer model on governance |
| Dispersion | index + constituents | all constituents | required | helpful | weights/actions | no |

---

## 8. Open decisions

Each needs an explicit call before the dependent work starts. Recommended default given first.

#### D-01 — Universe priority
A builds ETF-first (SPY/QQQ/IWM/XBI/SOXX); B builds SPX/SPXW-first with ETFs as comparison.
**Recommend B's ordering.** Cash-settled index options are the deepest surface and the VIX linkage,
and B's phase 4 (SPX drilldown) is explicitly the forcing function for every upstream component.
Both are already collected, so this is a question of where analytics effort goes first, not data.

#### D-02 — Dealer positioning
A treats GEX/vanna/charm as core phase-3 analytics (Moontower parity); B explicitly defers it,
citing extra data needs and model governance. **Recommend B's caution with a caveat:** we already
have OI and internally computed gamma/vanna/charm, so a *diagnostic* aggregate is nearly free. Build
it labelled as a proxy with assumptions and sensitivity ranges exposed — never as a signal — until it
passes the same gates. Currently `deferred`.

#### D-03 — Bronze/Silver/Gold separation
Today one 44-column row mixes raw observations with derived surface values. B wants immutable
provider-native Bronze separate from derived Gold, so models can be re-run after a bug fix without
losing observations. **Recommend splitting before phase 5**, since it blocks `QUAL-08`
(reproducibility). Cost: a second write path and a join in research code. Doing it later means
rewriting the backfill output.

#### D-04 — History depth
A assumed 2012; reality is 2016-01-01 (§4.3). **Accept 2016 and rely on scenario stress for
crisis tails.** Alternative is a PROFESSIONAL upgrade — worth pricing only if tail work becomes
central, since it's also the only route to vendor greeks and deep index history.

#### D-05 — Greeks cross-check counterparty
B requires vendor greeks be compared to internal calculations. ThetaData greeks are 403 at every
held tier, so **IBKR `modelGreeks` is the only available counterparty**, and it's a narrowed chain
(nearest N expiries × M strikes). Accept partial coverage for `QUAL-05`, or widen the IBKR pull and
pay the pacing cost.

#### D-06 — Sampling cadence and storage
60s chosen, at the fast end of B's 1–5 min. Measured cost: ~40k rows and ~4.5 MB per cycle → **~1.7
GB/day**. Decision taken: keep 60s and prioritise compaction. Revisit if the lake outgrows the disk;
`THETADATA_MAX_DTE` / `THETADATA_STRIKE_RANGE` are the other levers.

#### D-07 — Executable IV vs mid-price
`SURF-13` currently computes IV from mid only, which B calls out directly: "mid-price alone is not a
strategy." **Recommend adding bid and ask surfaces alongside mid** as part of `SURF-06`, so every
downstream signal can be evaluated on the side it would actually trade.

#### D-08 — 0DTE scope
B defers 0DTE automation until timestamp quality, intraday replay, and execution-cost models are
proven. Our `tau` already handles 0DTE correctly (fraction of day to 16:00 ET), so **0DTE is in scope
for research and out of scope for automation** — consistent with both documents.

#### D-09 — Commercial intent
A frames three monetization paths; B is purely a fund model. These aren't exclusive, but licensing
signals would require redistribution terms cleared with IBKR/ThetaData, which the current agreements
almost certainly don't grant. **Recommend deferring any external-facing use** until the terms are checked.

---

## 9. Definitions

**Variance risk premium** — implied variance for a horizon minus subsequent realized variance over
the matched horizon (research label). For trading, replace subsequent realized with a point-in-time
forecast distribution and include jump and execution uncertainty.

**Forward variance** between T₁ and T₂ — `[σ²(T₂)·T₂ − σ²(T₁)·T₁] / (T₂ − T₁)`, with consistently
defined maturities and annualization.

**25-delta risk reversal** — `IV(25Δ call) − IV(25Δ put)`. Display the sign convention, since equity
practitioners often quote downside skew the other way.

**Crisis convexity ratio** — scenario package value ÷ executable premium paid, stated with the
scenario definition, time elapsed, surface response, and probability estimate.

**Realized range estimators** — close-to-close for standard return variance; Parkinson and
Garman–Klass for high-low/open-close information; intraday and overnight components separately where
trading logic is session-dependent.

**Point-in-time** — only information actually available as of the decision timestamp.

**Executable IV** — IV from a tradable quote side or documented fill model, not a stale midpoint.

**Surface residual** — observed IV minus fitted IV, interpreted only after liquidity and arbitrage checks.

**Regime** — a pre-defined state based on observable variables; never a retrospective narrative label.

**Anomaly** — a statistically and economically unusual price after accounting for carry, conventions,
liquidity, and costs.

**Edge** — expected net benefit supported by mechanism and evidence, with uncertainty and capacity limits.

**Book** — positions governed by a common objective and risk budget.

---

## 10. Change log

| Date | Change |
| --- | --- |
| 2026-09-05 | Combined A and B into this document. Established the 93-feature registry, recorded measured data-availability constraints (§4), and opened decisions D-01…D-09. Status reflects a working ThetaData realtime + 2016-onward EOD pipeline with locally solved surface. |
| 2026-09-07 | Replaced the weekday-plus-fixed-hours session test with the NYSE exchange calendar, closing `DATA-06`. Caught because the old test reported Labor Day as open, which would have written a full day of stale quotes to `dt=2026-09-07`. Half-day closes are now handled too. |
| 2026-09-08 | Audited `data/`. Canonicalized `expiration` to a typed `Date` in both sources and rolled IBKR's legacy Saturday expiry back to the last trading day, which unblocked `QUAL-05` — the cross-provider join previously matched zero contracts. Pinned the IBKR output schema and nulled its NaN/-1 sentinels. Verified the Date change is behavior-preserving across all 113,806 stored EOD rows. Downgraded `DATA-19` (snapshots are not synchronized). Recorded that on-disk realtime data is stale and must be re-collected. |
| 2026-09-18 | Landed `SURF-09` (`scripts/run_surf09.py`). Closed `DATA-08` and `SURF-07`; moved `DATA-09`, `SURF-05/06/08/10`, `RV-01`, `UI-03` to `partial`. Option EOD lake still starts 2024-01-02. IBKR expired futures before ~2024/2025 Error 200. Live 09-07 excluded; 09-08 normalized; 09-09 mixed schema aligned at last snapshot. Totals 15 done · 19 partial · 53 todo · 5 deferred · 1 blocked. |
