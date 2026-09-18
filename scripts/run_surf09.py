#!/usr/bin/env python
"""SURF-09: SPX (+ SPXW) single-ticker drilldown on the local Parquet lake.

    python scripts/run_surf09.py
    python scripts/run_surf09.py --port 8765

Phase 4 panels the lake can actually support: liquidity filter, forwards, ATM
term, forward variance, 10Δ/25Δ RR and butterflies, smile, IV rank vs 2024+
EOD, close-to-close RV vs ATM (labelled, not a forecast), calendar-variance
diagnostics, greeks on the chain, and ES/NQ/VX closes with their real start
dates.

Not on this page (data or scope missing): fitted SVI (SURF-04), bid/ask IV
(lake stores mid IV only), event calendar, VRP scanner, portfolio.
Labor Day 2026-09-07 live tape is excluded. Live 2026-09-09 uses the last
snapshot aligned onto the current schema.
"""
from __future__ import annotations

import argparse
import html
import math
import sys
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from volmagaddon.config import get_settings
from volmagaddon.sources.thetadata import _OUTPUT_SCHEMA
from volmagaddon.storage import _align

NY = ZoneInfo("America/New_York")
ROOTS = ("SPX", "SPXW")
EXCLUDE_LIVE = {date(2026, 9, 7)}
DEFAULT_SPREAD = 0.08
DEFAULT_MIN_SIZE = 1
DEFAULT_DTE_MIN = 1
DELTA_TOL = 0.08
YEAR = 365.25
SPX_MULTIPLIER = 100
SLIM_COLS = [
    "symbol", "session", "expiration", "dte", "tau", "right", "strike", "delta", "iv",
    "spread", "mid", "bid", "ask", "bid_size", "ask_size", "forward",
    "discount", "gamma", "vega", "theta", "vanna", "charm", "open_interest",
    "poll_timestamp",
]
_NYSE_SESSIONS: set[date] | None = None
_ATM_HISTORY: list[dict] | None = None
_DAILY: dict[str, pl.DataFrame] = {}


def _nyse_sessions() -> set[date]:
    global _NYSE_SESSIONS
    if _NYSE_SESSIONS is None:
        import pandas_market_calendars as mcal
        cal = mcal.get_calendar("NYSE")
        sched = cal.schedule(start_date="2024-01-01", end_date=date.today().isoformat())
        idx = sched.index
        if getattr(idx, "tz", None) is not None:
            idx = idx.tz_localize(None)
        _NYSE_SESSIONS = {ts.date() for ts in idx}
    return _NYSE_SESSIONS


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _iso(d: date | None) -> str:
    return "" if d is None else d.isoformat()


def _fmt(value, digits=2, pct=False) -> str:
    if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
        return "—"
    if pct:
        return f"{value * 100:.{digits}f}%"
    return f"{value:,.{digits}f}"


def _list_eod_days(data_dir: Path) -> list[date]:
    found: dict[date, set[str]] = {}
    for root in ROOTS:
        for part in (data_dir / "thetadata_eod" / root).glob("dt=*"):
            try:
                day = date.fromisoformat(part.name.removeprefix("dt="))
            except ValueError:
                continue
            if (part / "eod.parquet").exists():
                found.setdefault(day, set()).add(root)
    return sorted(d for d, roots in found.items() if roots and d in _nyse_sessions())


def _list_live_days(data_dir: Path) -> list[date]:
    found: set[date] = set()
    for root in ROOTS:
        for part in (data_dir / "thetadata" / root).glob("dt=*"):
            try:
                day = date.fromisoformat(part.name.removeprefix("dt="))
            except ValueError:
                continue
            if day in EXCLUDE_LIVE or day not in _nyse_sessions():
                continue
            if any(p.suffix == ".parquet" for p in part.glob("*.parquet")):
                found.add(day)
    return sorted(found)


def _read_parquet_aligned(path: Path) -> pl.DataFrame:
    available = set(pl.read_parquet_schema(path))
    cols = [c for c in SLIM_COLS if c in available] or None
    return _align(pl.read_parquet(path, columns=cols), _OUTPUT_SCHEMA)


def _read_eod(data_dir: Path, day: date) -> pl.DataFrame:
    frames = []
    for root in ROOTS:
        path = data_dir / "thetadata_eod" / root / f"dt={day.isoformat()}" / "eod.parquet"
        if path.exists():
            frames.append(_read_parquet_aligned(path))
    if not frames:
        return pl.DataFrame(schema=_OUTPUT_SCHEMA)
    return pl.concat(frames, how="vertical")


def _last_live_file(day_dir: Path) -> Path | None:
    parts = sorted(p for p in day_dir.glob("*.parquet") if p.name != "compacted.parquet")
    for path in reversed(parts):
        try:
            pl.read_parquet_schema(path)
            return path
        except Exception:
            continue
    return None


def _read_live(data_dir: Path, day: date) -> tuple[pl.DataFrame, datetime | None]:
    frames = []
    asof = None
    for root in ROOTS:
        path = _last_live_file(data_dir / "thetadata" / root / f"dt={day.isoformat()}")
        if path is None:
            continue
        df = _read_parquet_aligned(path)
        if df.schema.get("session") != pl.Date and "poll_timestamp" in df.columns:
            ts = pl.col("poll_timestamp")
            if df.schema["poll_timestamp"].time_zone is None:
                ts = ts.dt.replace_time_zone("UTC")
            df = df.with_columns(
                ts.dt.convert_time_zone("America/New_York").dt.date().alias("session")
            )
            df = _align(df, _OUTPUT_SCHEMA)
        frames.append(df)
        if "poll_timestamp" in df.columns and df["poll_timestamp"].null_count() < df.height:
            stamp = df["poll_timestamp"].max()
            if asof is None or stamp > asof:
                asof = stamp
    if not frames:
        return pl.DataFrame(schema=_OUTPUT_SCHEMA), None
    return pl.concat(frames, how="vertical"), asof


def _load_daily(data_dir: Path, symbol: str) -> pl.DataFrame | None:
    if symbol not in _DAILY:
        path = data_dir / "ibkr_eod" / symbol / "daily.parquet"
        _DAILY[symbol] = pl.read_parquet(path) if path.exists() else pl.DataFrame()
    df = _DAILY[symbol]
    return None if df is None or df.is_empty() else df


def _spot(data_dir: Path, day: date, symbol: str = "SPX") -> float | None:
    bars = _load_daily(data_dir, symbol)
    if bars is None:
        return None
    hit = bars.filter(pl.col("session") <= day).sort("session")
    if hit.is_empty() or hit["close"][-1] is None:
        return None
    return float(hit["close"][-1])


def _bar_asof(data_dir: Path, day: date, symbol: str) -> dict | None:
    bars = _load_daily(data_dir, symbol)
    if bars is None:
        return None
    hit = bars.filter(pl.col("session") <= day).sort("session")
    if hit.is_empty():
        return None
    row = hit.row(-1, named=True)
    return {
        "close": float(row["close"]) if row["close"] is not None else None,
        "session": row["session"],
        "start": bars["session"].min(),
        "end": bars["session"].max(),
        "n": bars.height,
    }


def _filter(df: pl.DataFrame, *, max_spread: float, min_size: int, dte_min: int) -> pl.DataFrame:
    if df.is_empty():
        return df
    mid = pl.col("mid")
    return df.filter(
        pl.col("bid").is_not_null()
        & pl.col("ask").is_not_null()
        & (pl.col("bid") > 0)
        & (pl.col("ask") > pl.col("bid"))
        & mid.is_not_null()
        & (mid > 0.05)
        & (pl.col("spread") / mid <= max_spread)
        & (pl.col("bid_size").fill_null(0) >= min_size)
        & (pl.col("ask_size").fill_null(0) >= min_size)
        & (pl.col("dte") >= dte_min)
        & pl.col("iv").is_not_null()
        & pl.col("forward").is_not_null()
        & pl.col("delta").is_not_null()
    )


def _nearest(df: pl.DataFrame, target: float, col: str = "delta") -> dict | None:
    if df.is_empty():
        return None
    order = df.with_columns((pl.col(col) - target).abs().alias("_err")).sort("_err")
    row = order.row(0, named=True)
    if row["_err"] > DELTA_TOL:
        return None
    return row


def _term_rows(liq: pl.DataFrame) -> list[dict]:
    rows = []
    if liq.is_empty():
        return rows
    for (expiration,), grp in liq.group_by(["expiration"], maintain_order=True):
        forward = grp["forward"].drop_nulls()
        if forward.is_empty():
            continue
        f = float(forward[0])
        dte = int(grp["dte"][0])
        tau = dte / YEAR
        calls = grp.filter(pl.col("right") == "C")
        puts = grp.filter(pl.col("right") == "P")
        atm_c = _nearest(calls, 0.50)
        atm_p = _nearest(puts, -0.50)
        ivs = [x["iv"] for x in (atm_c, atm_p) if x is not None]
        if not ivs:
            continue
        atm = sum(ivs) / len(ivs)
        c25, p25 = _nearest(calls, 0.25), _nearest(puts, -0.25)
        c10, p10 = _nearest(calls, 0.10), _nearest(puts, -0.10)
        rr25 = None if c25 is None or p25 is None else float(p25["iv"]) - float(c25["iv"])
        rr10 = None if c10 is None or p10 is None else float(p10["iv"]) - float(c10["iv"])
        bf25 = None if c25 is None or p25 is None else 0.5 * (float(c25["iv"]) + float(p25["iv"])) - atm
        bf10 = None if c10 is None or p10 is None else 0.5 * (float(c10["iv"]) + float(p10["iv"])) - atm
        disc = grp["discount"].drop_nulls()
        discount = float(disc[0]) if disc.len() else None
        rows.append({
            "expiration": expiration,
            "dte": dte,
            "tau": tau,
            "forward": f,
            "atm": atm,
            "atm_call": None if atm_c is None else float(atm_c["iv"]),
            "atm_put": None if atm_p is None else float(atm_p["iv"]),
            "rr25": rr25,
            "rr10": rr10,
            "bf25": bf25,
            "bf10": bf10,
            "total_var": atm * atm * tau,
            "discount": discount,
            "n": grp.height,
            "root": ",".join(sorted(set(grp["symbol"].to_list()))),
        })
    rows.sort(key=lambda r: r["dte"])
    return rows


def _fwd_var(term: list[dict]) -> list[dict]:
    out = []
    for a, b in zip(term, term[1:]):
        t1, t2 = a["tau"], b["tau"]
        if t2 <= t1:
            continue
        v1, v2 = a["total_var"], b["total_var"]
        fv = (v2 - v1) / (t2 - t1)
        out.append({
            "from": a["expiration"],
            "to": b["expiration"],
            "dte1": a["dte"],
            "dte2": b["dte"],
            "fwd_var": fv,
            "fwd_vol": math.sqrt(fv) if fv > 0 else None,
            "inverted": v2 + 1e-12 < v1,
        })
    return out


def _point_near_dte(term: list[dict], target: int = 30) -> dict | None:
    band = [r for r in term if abs(r["dte"] - target) <= 15]
    pool = band or term
    if not pool:
        return None
    return min(pool, key=lambda r: abs(r["dte"] - target))


def _history_point(data_dir: Path, day: date) -> dict | None:
    raw = _read_eod(data_dir, day)
    if not raw.is_empty():
        raw = raw.filter(pl.col("dte").is_between(15, 50))
    liq = _filter(raw, max_spread=DEFAULT_SPREAD, min_size=DEFAULT_MIN_SIZE, dte_min=DEFAULT_DTE_MIN)
    term = _term_rows(liq)
    pt = _point_near_dte(term, 30)
    if pt is None:
        return None
    return {
        "session": day,
        "dte": pt["dte"],
        "atm": pt["atm"],
        "rr25": pt["rr25"],
        "rr10": pt["rr10"],
        "bf25": pt["bf25"],
    }


def _ensure_atm_history(data_dir: Path) -> list[dict]:
    global _ATM_HISTORY
    if _ATM_HISTORY is not None:
        return _ATM_HISTORY
    days = _list_eod_days(data_dir)
    out = []
    t0 = datetime.now()
    for i, day in enumerate(days, 1):
        pt = _history_point(data_dir, day)
        if pt is not None:
            out.append(pt)
        if i % 100 == 0:
            print(f"[surf09] IV-rank cache {i}/{len(days)}", flush=True)
    _ATM_HISTORY = out
    print(f"[surf09] IV-rank cache {len(out)} sessions in "
          f"{(datetime.now() - t0).total_seconds():.1f}s", flush=True)
    return out


def _percentile(values: list[float], current: float) -> float | None:
    if not values:
        return None
    return sum(1 for v in values if v <= current) / len(values)


def _zscore(values: list[float], current: float) -> float | None:
    if len(values) < 5:
        return None
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    if var <= 0:
        return None
    return (current - mean) / math.sqrt(var)


def _rv(data_dir: Path, day: date, window: int) -> float | None:
    bars = _load_daily(data_dir, "SPX")
    if bars is None:
        return None
    hit = bars.filter(pl.col("session") <= day).sort("session")
    if hit.height < window + 1:
        return None
    closes = hit["close"].tail(window + 1).to_list()
    rets = []
    for a, b in zip(closes, closes[1:]):
        if a and b and a > 0 and b > 0:
            rets.append(math.log(b / a) ** 2)
    if len(rets) < window:
        return None
    return math.sqrt(252.0 * sum(rets) / len(rets))


def _svg_lines(series: list[tuple[str, list[float], list[float]]], *,
               title: str, xlabel: str, ylabel: str,
               width: int = 640, height: int = 280,
               y_pct: bool = False) -> str:
    usable = [(name, xs, ys) for name, xs, ys in series if xs and ys and len(xs) == len(ys)]
    if not usable:
        return ""
    pad_l, pad_r, pad_t, pad_b = 56, 16, 28, 40
    all_x = [x for _, xs, _ in usable for x in xs]
    all_y = [y for _, _, ys in usable for y in ys]
    xmin, xmax = min(all_x), max(all_x)
    ymin, ymax = min(all_y), max(all_y)
    if xmin == xmax:
        xmax = xmin + 1
    if ymin == ymax:
        ymax = ymin + 0.01
    ypad = (ymax - ymin) * 0.08
    ymin -= ypad
    ymax += ypad
    inner_w = width - pad_l - pad_r
    inner_h = height - pad_t - pad_b

    def sx(x: float) -> float:
        return pad_l + (x - xmin) / (xmax - xmin) * inner_w

    def sy(y: float) -> float:
        return pad_t + (1 - (y - ymin) / (ymax - ymin)) * inner_h

    def ylab(y: float) -> str:
        return f"{y*100:.1f}%" if y_pct else f"{y:.2f}"

    colors = ("#d4a017", "#6ea8fe", "#9d7cd8", "#7dcea0")
    parts = [
        f'<svg viewBox="0 0 {width} {height}" class="chart" role="img" aria-label="{html.escape(title)}">',
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="#141414"/>',
        f'<text x="{pad_l}" y="18" fill="#e8e4d9" font-size="12">{html.escape(title)}</text>',
        f'<text x="{width/2}" y="{height - 8}" fill="#8a8680" font-size="10" text-anchor="middle">{html.escape(xlabel)}</text>',
        f'<text x="12" y="{height/2}" fill="#8a8680" font-size="10" transform="rotate(-90 12 {height/2})" text-anchor="middle">{html.escape(ylabel)}</text>',
    ]
    for i in range(5):
        y = ymin + (ymax - ymin) * i / 4
        py = sy(y)
        parts.append(f'<line x1="{pad_l}" y1="{py:.1f}" x2="{width-pad_r}" y2="{py:.1f}" stroke="#2a2a2a"/>')
        parts.append(f'<text x="{pad_l-6}" y="{py+3:.1f}" fill="#8a8680" font-size="9" text-anchor="end">{ylab(y)}</text>')
    for i in range(5):
        x = xmin + (xmax - xmin) * i / 4
        px = sx(x)
        parts.append(f'<text x="{px:.1f}" y="{height-pad_b+14}" fill="#8a8680" font-size="9" text-anchor="middle">{x:.0f}</text>')
    for i, (name, xs, ys) in enumerate(usable):
        color = colors[i % len(colors)]
        pts = " ".join(f"{sx(x):.1f},{sy(y):.1f}" for x, y in zip(xs, ys))
        parts.append(f'<polyline fill="none" stroke="{color}" stroke-width="1.6" points="{pts}"/>')
        if len(xs) <= 80:
            for x, y in zip(xs, ys):
                parts.append(f'<circle cx="{sx(x):.1f}" cy="{sy(y):.1f}" r="2.2" fill="{color}"/>')
        parts.append(
            f'<text x="{width-pad_r-4}" y="{24 + i*14}" fill="{color}" font-size="10" text-anchor="end">{html.escape(name)}</text>'
        )
    parts.append("</svg>")
    return "\n".join(parts)


def _options(values: list[tuple[str, str]], selected: str) -> str:
    out = []
    for value, label in values:
        sel = " selected" if value == selected else ""
        out.append(f'<option value="{html.escape(value)}"{sel}>{html.escape(label)}</option>')
    return "\n".join(out)


def _page(ctx: dict) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>SURF-09 SPX drilldown</title>
<style>
  :root {{ color-scheme: dark; }}
  body {{ margin: 0; background: #0e0e0e; color: #e8e4d9; font: 14px/1.45 ui-sans-serif, system-ui, sans-serif; }}
  header {{ padding: 16px 24px 8px; border-bottom: 1px solid #2a2a2a; }}
  h1 {{ margin: 0 0 4px; font-size: 18px; font-weight: 600; }}
  .sub {{ color: #8a8680; font-size: 12px; }}
  form {{ display: flex; flex-wrap: wrap; gap: 12px 16px; padding: 14px 24px; align-items: end; }}
  label {{ display: flex; flex-direction: column; gap: 4px; font-size: 11px; color: #8a8680; text-transform: uppercase; letter-spacing: .04em; }}
  select, input {{ background: #1a1a1a; color: #e8e4d9; border: 1px solid #333; padding: 6px 8px; font: 13px ui-monospace, monospace; }}
  button {{ background: #d4a017; color: #111; border: 0; padding: 8px 14px; font-weight: 600; cursor: pointer; }}
  main {{ padding: 8px 24px 32px; }}
  .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 10px; margin-bottom: 16px; }}
  .card {{ background: #161616; border: 1px solid #2a2a2a; padding: 10px 12px; }}
  .card b {{ display: block; font-size: 18px; font-variant-numeric: tabular-nums; }}
  .card span {{ color: #8a8680; font-size: 11px; }}
  .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
  .chart {{ width: 100%; height: auto; border: 1px solid #2a2a2a; }}
  table {{ width: 100%; border-collapse: collapse; font: 12px/1.4 ui-monospace, monospace; margin-top: 8px; }}
  th, td {{ text-align: right; padding: 4px 8px; border-bottom: 1px solid #222; }}
  th:first-child, td:first-child {{ text-align: left; }}
  th {{ color: #8a8680; font-weight: 500; }}
  .note {{ color: #8a8680; font-size: 12px; margin: 8px 0 16px; }}
  .warn {{ color: #d4a017; }}
  .bad {{ color: #e07a5f; }}
  section {{ margin-top: 20px; }}
  h2 {{ font-size: 13px; font-weight: 600; margin: 0 0 8px; letter-spacing: .04em; text-transform: uppercase; color: #8a8680; }}
  @media (max-width: 900px) {{ .grid {{ grid-template-columns: 1fr; }} }}
</style>
</head>
<body>
<header>
  <h1>SURF-09 · SPX + SPXW</h1>
  <div class="sub">{html.escape(ctx["caption"])}</div>
</header>
<form method="get">
  <label>Session
    <select name="session">{ctx["session_opts"]}</select>
  </label>
  <label>Source
    <select name="source">{ctx["source_opts"]}</select>
  </label>
  <label>Expiry
    <select name="expiry">{ctx["expiry_opts"]}</select>
  </label>
  <label>Max spread / mid
    <input name="spread" type="number" step="0.01" min="0.01" max="1" value="{ctx["spread"]}"/>
  </label>
  <label>Min size
    <input name="minsize" type="number" min="0" value="{ctx["minsize"]}"/>
  </label>
  <label>Min DTE
    <input name="dte_min" type="number" min="0" value="{ctx["dte_min"]}"/>
  </label>
  <button type="submit">Update</button>
</form>
<main>
  {ctx["quality"]}
  {ctx["overlay"]}
  <p class="note">{ctx["filter_note"]}</p>
  <div class="grid">
    <div>{ctx["term_svg"]}</div>
    <div>{ctx["fwd_svg"]}</div>
    <div>{ctx["rr_svg"]}</div>
    <div>{ctx["bf_svg"]}</div>
    <div>{ctx["rank_svg"]}</div>
    <div>{ctx["rv_svg"]}</div>
  </div>
  <section>
    {ctx["smile_svg"]}
    <p class="note">Smile is liquidity-filtered mid IV. RR = IV(put Δ) − IV(call Δ). BF = ½(IV call + IV put) − ATM. Positive RR means puts are richer. Mid IV is not executable (SURF-13) — bid/ask IV is not stored in the lake.</p>
  </section>
  <section>
    <h2>ATM term + forward variance</h2>
    <p class="note">Forward variance between T1 and T2 is [σ²(T2)T2 − σ²(T1)T1] / (T2 − T1), T in years. A drop in total variance σ²T is a calendar inversion (flagged).</p>
    {ctx["term_table"]}
  </section>
  <section>
    <h2>Forward variance strips</h2>
    {ctx["fv_table"]}
  </section>
  <section>
    <h2>Selected expiry chain</h2>
    {ctx["chain_table"]}
  </section>
</main>
</body>
</html>
"""


def _cards(items: list[tuple[str, str]]) -> str:
    bits = []
    for label, value in items:
        bits.append(f'<div class="card"><b>{html.escape(value)}</b><span>{html.escape(label)}</span></div>')
    return '<div class="cards">' + "".join(bits) + "</div>"


def _table(headers: list[str], rows: list[list[str]]) -> str:
    if not rows:
        return '<p class="note warn">No rows.</p>'
    head = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body = []
    for row in rows:
        body.append("<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def build(data_dir: Path, params: dict[str, str]) -> str:
    eod_days = _list_eod_days(data_dir)
    live_days = _list_live_days(data_dir)
    source = params.get("source", "eod")
    if source not in ("eod", "live"):
        source = "eod"
    days = live_days if source == "live" else eod_days
    session = _parse_date(params.get("session"))
    if session not in days:
        session = days[-1] if days else None
    spread = float(params.get("spread") or DEFAULT_SPREAD)
    min_size = int(params.get("minsize") or DEFAULT_MIN_SIZE)
    dte_min = int(params.get("dte_min") or DEFAULT_DTE_MIN)

    asof = None
    raw = pl.DataFrame(schema=_OUTPUT_SCHEMA)
    if session is not None:
        if source == "live":
            raw, asof = _read_live(data_dir, session)
        else:
            raw = _read_eod(data_dir, session)

    crossed = 0
    if not raw.is_empty():
        crossed = int(
            raw.filter(
                pl.col("bid").is_not_null() & pl.col("ask").is_not_null() & (pl.col("bid") > pl.col("ask"))
            ).height
        )
    solved = 0 if raw.is_empty() else int(raw["iv"].is_not_null().sum())
    liq = _filter(raw, max_spread=spread, min_size=min_size, dte_min=dte_min)
    term = _term_rows(liq)
    fv = _fwd_var(term)
    n30 = _point_near_dte(term, 30)
    spot = _spot(data_dir, session) if session else None

    pc_gap = None
    if n30 and n30["atm_call"] is not None and n30["atm_put"] is not None:
        pc_gap = abs(n30["atm_call"] - n30["atm_put"])
    fwd_spot = None
    if n30 and spot:
        fwd_spot = n30["forward"] - spot
    inversions = sum(1 for x in fv if x["inverted"])
    neg_bf = sum(1 for r in term if r["bf25"] is not None and r["bf25"] < -1e-4)

    gamma_oi = None
    if not liq.is_empty() and "gamma" in liq.columns and "open_interest" in liq.columns:
        g = liq.filter(pl.col("gamma").is_not_null() & pl.col("open_interest").is_not_null())
        if g.height:
            gamma_oi = float((g["gamma"] * g["open_interest"] * SPX_MULTIPLIER).sum())

    hist = _ensure_atm_history(data_dir)
    hist_upto = [h for h in hist if session is None or h["session"] <= session]
    atms = [h["atm"] for h in hist_upto]
    rr25s = [h["rr25"] for h in hist_upto if h["rr25"] is not None]
    iv_rank = _percentile(atms, n30["atm"]) if n30 and atms else None
    rr_z = _zscore(rr25s, n30["rr25"]) if n30 and n30["rr25"] is not None else None
    rv20 = _rv(data_dir, session, 20) if session else None
    rv60 = _rv(data_dir, session, 60) if session else None
    vrp = None
    if n30 and rv20 is not None:
        vrp = n30["atm"] ** 2 - rv20 ** 2

    expiries = [(_iso(r["expiration"]), f"{r['expiration']}  dte {r['dte']}") for r in term]
    expiry = _parse_date(params.get("expiry"))
    if expiry not in {r["expiration"] for r in term} and term:
        expiry = min(term, key=lambda r: abs(r["dte"] - 30))["expiration"]

    smile_calls: list[tuple[float, float]] = []
    smile_puts: list[tuple[float, float]] = []
    chain_rows = []
    if expiry is not None and not liq.is_empty():
        slice_ = liq.filter(pl.col("expiration") == expiry).sort("strike")
        fwd = float(slice_["forward"][0]) if slice_.height else None
        if fwd:
            smile_calls = [
                (float(k) / fwd, float(iv))
                for k, iv, right in zip(slice_["strike"], slice_["iv"], slice_["right"])
                if right == "C" and 0.85 <= k / fwd <= 1.15
            ]
            smile_puts = [
                (float(k) / fwd, float(iv))
                for k, iv, right in zip(slice_["strike"], slice_["iv"], slice_["right"])
                if right == "P" and 0.85 <= k / fwd <= 1.15
            ]
        shown = slice_.filter((pl.col("strike") / fwd).is_between(0.85, 1.15)) if fwd else slice_
        for row in shown.head(120).iter_rows(named=True):
            chain_rows.append([
                html.escape(str(row["symbol"])),
                html.escape(row["right"]),
                _fmt(row["strike"], 0),
                _fmt(row["bid"], 2),
                _fmt(row["ask"], 2),
                _fmt(row["iv"], 2, pct=True),
                _fmt(row["delta"], 3),
                _fmt(row.get("gamma"), 5),
                _fmt(row.get("vega"), 2),
                _fmt(row.get("theta"), 2),
                str(row.get("open_interest") or "—"),
                _fmt(row["spread"] / row["mid"] if row["mid"] else None, 3),
            ])

    session_opts = _options(
        [(_iso(d), f"{_iso(d)}{'  (live)' if source=='live' else ''}") for d in days],
        _iso(session),
    )
    source_opts = _options([("eod", "EOD lake"), ("live", "Live last snapshot")], source)
    expiry_opts = _options(expiries or [("", "none")], _iso(expiry))

    asof_txt = ""
    if asof is not None:
        local = asof.astimezone(NY) if asof.tzinfo else asof.replace(tzinfo=NY)
        asof_txt = f" · as of {local:%Y-%m-%d %H:%M ET}"
    roots_present = [] if raw.is_empty() else sorted(set(raw["symbol"].to_list()))
    caption = (
        f"{'Live last snapshot' if source=='live' else 'ThetaData EOD'} · "
        f"{_iso(session) or 'no session'}{asof_txt} · roots {', '.join(roots_present) or '—'} · "
        f"IBKR SPX close {_fmt(spot, 2)}"
    )
    if source == "live" and session == date(2026, 9, 9):
        caption += " · 09-09 last file aligned from mixed 42/44 schema"

    quality = _cards([
        ("Contracts", f"{raw.height:,}"),
        ("IV solved", f"{(solved / raw.height if raw.height else 0):.0%}"),
        ("Liquidity kept", f"{liq.height:,}"),
        ("Crossed quotes", str(crossed)),
        ("~30d ATM IV", _fmt(None if n30 is None else n30["atm"], 2, pct=True)),
        ("IV rank 2024+", "—" if iv_rank is None else f"{iv_rank:.0%}"),
        ("~30d RR25", "—" if n30 is None or n30["rr25"] is None else f"{n30['rr25']*100:.2f} vol pts"),
        ("RR25 z (2024+)", "—" if rr_z is None else f"{rr_z:+.2f}σ"),
        ("ATM PC |C−P|", _fmt(pc_gap, 2, pct=True)),
        ("Fwd − SPX", _fmt(fwd_spot, 2)),
        ("Calendar inversions", str(inversions)),
        ("Neg 25Δ BF", str(neg_bf)),
        ("20d close RV", _fmt(rv20, 2, pct=True)),
        ("ATM² − RV20²", _fmt(vrp, 4)),
        ("Σ γ·OI·100", _fmt(gamma_oi, 0)),
        ("SPX close", _fmt(spot, 2)),
    ])

    overlays = []
    for sym, label in (("ES", "ES (from 2023-06-20)"), ("NQ", "NQ (from 2024-03-18)"),
                       ("VX", "VX all-hours (from 2025-02-05)")):
        info = _bar_asof(data_dir, session, sym) if session else None
        if info and info["close"] is not None:
            lag = "" if info["session"] == session else f" as of {info['session']}"
            overlays.append((label, f"{info['close']:.2f}{lag}"))
        else:
            overlays.append((label, "—"))
    overlay = _cards(overlays)

    filter_note = (
        f"Liquidity filter: bid&gt;0, ask&gt;bid, spread/mid ≤ {spread:.0%}, "
        f"size ≥ {min_size}, DTE ≥ {dte_min}. "
        "IV rank / RR z-score use EOD ATM~30d from 2024-01-02 through this session. "
        "RV is close-to-close SPX, 252-scaled — not a forecast, not event-stripped. "
        "ATM²−RV² is a labelled variance gap, not a tradable VRP. "
        "No FOMC/CPI calendar in the repo. No SVI fit. Live 09-07 is not listed."
    )

    term_svg = _svg_lines(
        [("ATM mid IV", [r["dte"] for r in term], [r["atm"] for r in term])],
        title="ATM term structure (delta 50, liquidity-filtered)",
        xlabel="DTE (calendar days)", ylabel="IV", y_pct=True,
    )
    fwd_svg = _svg_lines(
        [("Parity forward", [r["dte"] for r in term], [r["forward"] for r in term])]
        + ([("SPX close", [term[0]["dte"], term[-1]["dte"]], [spot, spot])] if spot and term else []),
        title="Put-call parity forward vs DTE",
        xlabel="DTE (calendar days)", ylabel="SPX points",
    )
    rr_svg = _svg_lines(
        [
            ("RR25 put−call", [r["dte"] for r in term if r["rr25"] is not None],
             [r["rr25"] for r in term if r["rr25"] is not None]),
            ("RR10 put−call", [r["dte"] for r in term if r["rr10"] is not None],
             [r["rr10"] for r in term if r["rr10"] is not None]),
        ],
        title="Risk reversal (put IV − call IV)",
        xlabel="DTE (calendar days)", ylabel="vol (decimal)",
    )
    bf_svg = _svg_lines(
        [
            ("BF25", [r["dte"] for r in term if r["bf25"] is not None],
             [r["bf25"] for r in term if r["bf25"] is not None]),
            ("BF10", [r["dte"] for r in term if r["bf10"] is not None],
             [r["bf10"] for r in term if r["bf10"] is not None]),
        ],
        title="Butterfly  ½(call+put) − ATM",
        xlabel="DTE (calendar days)", ylabel="vol (decimal)",
    )
    rank_svg = _svg_lines(
        [("30d ATM EOD", [h["session"].toordinal() for h in hist_upto[-252:]],
          [h["atm"] for h in hist_upto[-252:]])]
        + ([("This session", [session.toordinal()], [n30["atm"]])] if session and n30 else []),
        title="30d ATM IV history (EOD, last 252 sessions)",
        xlabel="Session (ordinal date)", ylabel="IV", y_pct=True,
    )
    spx_bars = _load_daily(data_dir, "SPX")
    rv_svg = ""
    if spx_bars is not None and session is not None:
        window = spx_bars.filter(
            (pl.col("session") > session - timedelta(days=400)) & (pl.col("session") <= session)
        ).sort("session")
        if window.height > 2:
            xs = [d.toordinal() for d in window["session"].to_list()]
            ys = [float(c) for c in window["close"].to_list()]
            rv_svg = _svg_lines(
                [("SPX close", xs, ys)],
                title="SPX daily close (IBKR, ~1y to session)",
                xlabel="Session (ordinal date)", ylabel="SPX",
            )

    smile_svg = _svg_lines(
        [
            ("Call IV", [x for x, _ in smile_calls], [y for _, y in smile_calls]),
            ("Put IV", [x for x, _ in smile_puts], [y for _, y in smile_puts]),
        ],
        title=f"Smile  {_iso(expiry)}  IV vs K/F  (band 0.85–1.15)",
        xlabel="Strike / forward", ylabel="IV", y_pct=True,
    )

    term_table = _table(
        ["Expiry", "DTE", "Forward", "ATM", "Call", "Put", "RR25", "RR10", "BF25", "σ²T", "N"],
        [[
            html.escape(_iso(r["expiration"])),
            str(r["dte"]),
            _fmt(r["forward"], 2),
            _fmt(r["atm"], 2, pct=True),
            _fmt(r["atm_call"], 2, pct=True),
            _fmt(r["atm_put"], 2, pct=True),
            "—" if r["rr25"] is None else f"{r['rr25']*100:.2f}",
            "—" if r["rr10"] is None else f"{r['rr10']*100:.2f}",
            "—" if r["bf25"] is None else f"{r['bf25']*100:.2f}",
            _fmt(r["total_var"], 4),
            str(r["n"]),
        ] for r in term],
    )
    fv_rows = []
    for x in fv:
        vol = "—" if x["fwd_vol"] is None else f"{x['fwd_vol']*100:.2f}%"
        flag = '<span class="bad">inversion</span>' if x["inverted"] else ""
        fv_rows.append([
            html.escape(f"{_iso(x['from'])} → {_iso(x['to'])}"),
            f"{x['dte1']}→{x['dte2']}",
            _fmt(x["fwd_var"], 4),
            vol,
            flag,
        ])
    fv_table = _table(["Window", "DTE", "Fwd var", "Fwd vol", ""], fv_rows)
    chain_table = _table(
        ["Root", "Right", "Strike", "Bid", "Ask", "IV", "Δ", "Γ", "Vega", "Θ", "OI", "Sprd/mid"],
        chain_rows,
    )
    if not chain_rows:
        chain_table = '<p class="note warn">No liquidity-filtered rows for this expiry.</p>'
    if not term_svg:
        term_svg = '<p class="note warn">No ATM points after the liquidity filter.</p>'

    return _page({
        "caption": caption,
        "session_opts": session_opts,
        "source_opts": source_opts,
        "expiry_opts": expiry_opts,
        "spread": f"{spread:.2f}",
        "minsize": str(min_size),
        "dte_min": str(dte_min),
        "quality": quality,
        "overlay": overlay,
        "filter_note": filter_note,
        "term_svg": term_svg,
        "fwd_svg": fwd_svg,
        "rr_svg": rr_svg,
        "bf_svg": bf_svg,
        "rank_svg": rank_svg,
        "rv_svg": rv_svg,
        "smile_svg": smile_svg or "",
        "term_table": term_table,
        "fv_table": fv_table,
        "chain_table": chain_table,
    })


class Handler(BaseHTTPRequestHandler):
    data_dir: Path

    def log_message(self, fmt: str, *args) -> None:
        sys.stderr.write(f"[surf09] {self.address_string()} {fmt % args}\n")

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path not in ("/", "/index.html"):
            self.send_error(404)
            return
        params = {k: v[-1] for k, v in parse_qs(parsed.query).items()}
        try:
            body = build(self.data_dir, params).encode("utf-8")
        except Exception as exc:  # noqa: BLE001 — show the fault on the page
            body = f"<pre>{html.escape(repr(exc))}</pre>".encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    settings = get_settings()
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--host", default="127.0.0.1")
    args = ap.parse_args()
    Handler.data_dir = settings.data_dir
    print("[surf09] building 30d ATM history cache (first load)…", flush=True)
    _ensure_atm_history(settings.data_dir)
    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    eod_n = len(_list_eod_days(settings.data_dir))
    live_n = len(_list_live_days(settings.data_dir))
    print(f"[surf09] http://{args.host}:{args.port}/  eod_days={eod_n} live_days={live_n}  "
          f"data_dir={settings.data_dir}", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("[surf09] stopped", flush=True)


if __name__ == "__main__":
    main()
