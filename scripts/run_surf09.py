#!/usr/bin/env python
"""SURF-09: SPX (+ SPXW) drilldown on the local Parquet lake.

    python scripts/run_surf09.py
    python scripts/run_surf09.py --port 8765

JSON API (same process, no extra service):

    GET /api/meta?source=eod|live&session=YYYY-MM-DD
    GET /api/chain?session=&source=&stamp=&expiry=&dte=30&band=0
    GET /api/gex?session=&source=&stamp=&expiry=&dte=30
    GET /api/tools?session=&source=&stamp=&expiry=&dte=30&band=0
    GET /api/refresh          rebuild IV-rank / VRP / calendar caches from disk

    stamp=latest or HHMMSS (live UTC filename). expiry omitted → nearest dte.
    band=0.15 keeps strikes in [0.85, 1.15] × forward. Defaults: source=eod.

Unblocked registry items that the current lake can support, on one page:
liquidity filter + live staleness, bid/ask IV (re-solved), quad-k fit,
calendar + three-strike butterfly diagnostics, IV rank, close/Parkinson/GK
and overnight/intraday RV, research VRP (ATM² − subsequent RV²), cross-root
ATM, health/coverage, crossed-quote quarantine, paired chain + GEX/OI/max pain
on EOD or a chosen live snapshot (point-in-time).

CPI reference months and FOMC statement dates are read from the Macro MySQL
database (FRED observation dates, not the BLS release clock). SPY regular-hours
1-minute bars are read from the FMP CSV (not SPX, not ticks, not extended hours).
svi-raw v1 sits beside quad-k v1. The research panel is an EWMA variance forecast
with an error band, FOMC-day versus other-day variance, and a 10Δ payoff ratio.
Not on this page: bronze/silver/gold, dealer inventory, dispersion, or any order
staging. Labor Day 2026-09-07 live tape is excluded.
"""
from __future__ import annotations

import argparse
import html
import importlib.util
import json
import math
import os
import sys
import threading
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from volmagaddon.config import get_settings
from volmagaddon.sources.thetadata import _OUTPUT_SCHEMA, _implied_vol
from volmagaddon.storage import _align

NY = ZoneInfo("America/New_York")
ROOTS = ("SPX", "SPXW")
EXCLUDE_LIVE = {date(2026, 9, 7)}
DEFAULT_SPREAD = 0.08
DEFAULT_MIN_SIZE = 1
DEFAULT_DTE_MIN = 1
DEFAULT_STALE_SEC = 120
DELTA_TOL = 0.08
YEAR = 365.25
SPX_MULTIPLIER = 100
SLIM_COLS = [
    "symbol", "session", "expiration", "dte", "tau", "right", "strike", "delta", "iv",
    "spread", "mid", "bid", "ask", "bid_size", "ask_size", "forward",
    "discount", "gamma", "vega", "theta", "vanna", "charm", "open_interest",
    "volume", "poll_timestamp", "quote_timestamp",
]
_NYSE_SESSIONS: set[date] | None = None
_ATM_HISTORY: list[dict] | None = None
_DAILY: dict[str, pl.DataFrame] = {}
_CROSS_ATM: dict[date, list[dict]] = {}
_VRP_CACHE: list[tuple[date, float]] | None = None
_CACHE_LOCK = threading.RLock()
_SPY1: pl.DataFrame | None = None
_SPY1_KEY: tuple | None = None


def _nyse_sessions() -> set[date]:
    global _NYSE_SESSIONS
    with _CACHE_LOCK:
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


def _live_files(day_dir: Path) -> list[Path]:
    return sorted(p for p in day_dir.glob("*.parquet") if p.name != "compacted.parquet")


def _readable_live_files(day_dir: Path) -> list[Path]:
    out = []
    for path in _live_files(day_dir):
        try:
            pl.read_parquet_schema(path)
            out.append(path)
        except Exception:
            continue
    return out


def _nearest_live_file(day_dir: Path, stamp: str | None) -> Path | None:
    parts = _readable_live_files(day_dir)
    if not parts:
        return None
    if not stamp or stamp == "latest" or not isinstance(stamp, str):
        return parts[-1]
    try:
        want = int(stamp)
    except ValueError:
        return parts[-1]

    def _dist(path: Path) -> int:
        try:
            return abs(int(path.stem) - want)
        except ValueError:
            return 10**9

    return min(parts, key=_dist)


def _list_live_stamps(data_dir: Path, day: date) -> list[str]:
    stamps: set[str] = set()
    for root in ROOTS:
        for path in _readable_live_files(data_dir / "thetadata" / root / f"dt={day.isoformat()}"):
            if path.stem.isdigit():
                stamps.add(path.stem)
    return sorted(stamps)


def _stamp_label(stem: str) -> str:
    if len(stem) == 6 and stem.isdigit():
        return f"{stem[0:2]}:{stem[2:4]}:{stem[4:6]} UTC"
    return stem


def _last_live_file(day_dir: Path) -> Path | None:
    return _nearest_live_file(day_dir, None)


def _read_live(data_dir: Path, day: date, stamp: str | None = None) -> tuple[pl.DataFrame, datetime | None]:
    frames = []
    asof = None
    for root in ROOTS:
        path = _nearest_live_file(data_dir / "thetadata" / root / f"dt={day.isoformat()}", stamp)
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
            poll_asof = df["poll_timestamp"].max()
            if asof is None or poll_asof > asof:
                asof = poll_asof
    if not frames:
        return pl.DataFrame(schema=_OUTPUT_SCHEMA), None
    return pl.concat(frames, how="vertical"), asof


def _num(row: dict, key: str) -> float | None:
    value = row.get(key)
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _pair_expiry(df: pl.DataFrame, expiry: date | None) -> list[dict]:
    """One call + one put per strike (highest OI wins if SPX and SPXW overlap)."""
    if expiry is None or df.is_empty() or "expiration" not in df.columns:
        return []
    sl = df.filter(pl.col("expiration") == expiry)
    if sl.is_empty():
        return []
    sl = sl.with_columns(pl.col("open_interest").fill_null(0).alias("_oi"))
    sl = sl.sort(["strike", "right", "_oi"], descending=[False, False, True]).unique(
        ["strike", "right"], keep="first"
    )
    calls = {float(r["strike"]): r for r in sl.filter(pl.col("right") == "C").iter_rows(named=True)}
    puts = {float(r["strike"]): r for r in sl.filter(pl.col("right") == "P").iter_rows(named=True)}
    pairs = []
    for strike in sorted(set(calls) | set(puts)):
        c, p = calls.get(strike), puts.get(strike)
        ce_oi = 0.0 if c is None else (_num(c, "open_interest") or 0.0)
        pe_oi = 0.0 if p is None else (_num(p, "open_interest") or 0.0)
        ce_g = 0.0 if c is None else (_num(c, "gamma") or 0.0)
        pe_g = 0.0 if p is None else (_num(p, "gamma") or 0.0)
        ce_gex = ce_g * ce_oi * SPX_MULTIPLIER
        pe_gex = pe_g * pe_oi * SPX_MULTIPLIER
        fwd = None
        if c is not None:
            fwd = _num(c, "forward")
        if fwd is None and p is not None:
            fwd = _num(p, "forward")
        pairs.append({
            "strike": strike,
            "forward": fwd,
            "c": c,
            "p": p,
            "ce_oi": ce_oi,
            "pe_oi": pe_oi,
            "ce_gamma": ce_g,
            "pe_gamma": pe_g,
            "ce_gex": ce_gex,
            "pe_gex": pe_gex,
            "net_gex": ce_gex - pe_gex,
        })
    return pairs


def _max_pain(pairs: list[dict]) -> tuple[float | None, float | None]:
    if not pairs:
        return None, None
    best_k = best_pain = None
    for settle in (p["strike"] for p in pairs):
        pain = 0.0
        for row in pairs:
            pain += max(settle - row["strike"], 0.0) * row["ce_oi"]
            pain += max(row["strike"] - settle, 0.0) * row["pe_oi"]
        if best_pain is None or pain < best_pain:
            best_pain, best_k = pain, settle
    return best_k, best_pain


def _gex_flip(pairs: list[dict], atm: float | None) -> float | None:
    """Strike nearest ATM where net GEX changes sign (call−put)."""
    if len(pairs) < 2:
        return None
    flips = []
    prev = pairs[0]
    for cur in pairs[1:]:
        a, b = prev["net_gex"], cur["net_gex"]
        if a == 0:
            flips.append(prev["strike"])
        elif a * b < 0:
            flips.append(cur["strike"] if abs(b) < abs(a) else prev["strike"])
        prev = cur
    if not flips:
        return None
    if atm is None:
        return flips[len(flips) // 2]
    return min(flips, key=lambda k: abs(k - atm))


def _chart_crosshair(*, pad_l: float, pad_t: float, plot_r: float, plot_b: float) -> str:
    """Hover overlay. The page script snaps using data-pts on the svg."""
    return (
        f'<g class="xh" visibility="hidden" pointer-events="none">'
        f'<line class="xh-v" y1="{pad_t}" y2="{plot_b}" stroke="#e8e4d9" stroke-width="0.7" stroke-dasharray="3 3"/>'
        f'<line class="xh-h" x1="{pad_l}" x2="{plot_r}" stroke="#e8e4d9" stroke-width="0.7" stroke-dasharray="3 3"/>'
        f'<circle class="xh-dot" r="3.2" fill="#e8e4d9" stroke="#141414" stroke-width="1"/>'
        f'<text class="xh-lab" fill="#e8e4d9" font-size="11" stroke="#141414" stroke-width="3" paint-order="stroke"></text>'
        f'</g>'
        f'<rect class="xh-hit" x="{pad_l}" y="{pad_t}" width="{plot_r - pad_l}" height="{plot_b - pad_t}" fill="transparent"/>'
    )


def _svg_open(width: int, height: int, title: str, pts: list[dict], snap: str) -> str:
    payload = html.escape(json.dumps(pts), quote=True)
    return (
        f'<svg viewBox="0 0 {width} {height}" class="chart" data-crosshair="1" data-snap="{snap}" '
        f'data-pts="{payload}" role="img" aria-label="{html.escape(title)}">'
    )


def _svg_bars(items: list[tuple[float, float]], *, title: str, xlabel: str, ylabel: str,
              width: int = 640, height: int = 280) -> str:
    usable = [(x, y) for x, y in items if y is not None and math.isfinite(y)]
    if not usable:
        return ""
    pad_l, pad_r, pad_t, pad_b = 64, 16, 28, 40
    xs = [x for x, _ in usable]
    ys = [y for _, y in usable]
    xmin, xmax = min(xs), max(xs)
    ymin, ymax = min(ys + [0.0]), max(ys + [0.0])
    if xmin == xmax:
        xmax = xmin + 1
    if ymin == ymax:
        ymax = ymin + 1
    ypad = (ymax - ymin) * 0.08
    ymin -= ypad
    ymax += ypad
    inner_w = width - pad_l - pad_r
    inner_h = height - pad_t - pad_b
    bar_w = max(inner_w / max(len(usable), 1) * 0.72, 1.2)

    def sx(x: float) -> float:
        return pad_l + (x - xmin) / (xmax - xmin) * inner_w

    def sy(y: float) -> float:
        return pad_t + (1 - (y - ymin) / (ymax - ymin)) * inner_h

    zero = sy(0.0)
    plot_r = width - pad_r
    plot_b = height - pad_b
    pts = []
    for x, y in usable:
        shown = 0.0 if abs(y) < 0.5 else y
        pts.append({
            "sx": round(sx(x), 1),
            "sy": round(sy(y), 1),
            "t": f"{x:,.0f}  ·  {shown:,.0f}",
        })
    parts = [
        _svg_open(width, height, title, pts, "x"),
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="#141414"/>',
        f'<text x="{pad_l}" y="18" fill="#e8e4d9" font-size="12">{html.escape(title)}</text>',
        f'<text x="{width/2}" y="{height - 8}" fill="#8a8680" font-size="10" text-anchor="middle">{html.escape(xlabel)}</text>',
        f'<text x="12" y="{height/2}" fill="#8a8680" font-size="10" transform="rotate(-90 12 {height/2})" text-anchor="middle">{html.escape(ylabel)}</text>',
        f'<line x1="{pad_l}" y1="{zero:.1f}" x2="{width-pad_r}" y2="{zero:.1f}" stroke="#444"/>',
    ]
    for i in range(5):
        y = ymin + (ymax - ymin) * i / 4
        py = sy(y)
        parts.append(f'<line x1="{pad_l}" y1="{py:.1f}" x2="{width-pad_r}" y2="{py:.1f}" stroke="#2a2a2a"/>')
        parts.append(f'<text x="{pad_l-6}" y="{py+3:.1f}" fill="#8a8680" font-size="9" text-anchor="end">{y:,.0f}</text>')
    for i in range(5):
        x = xmin + (xmax - xmin) * i / 4
        parts.append(f'<text x="{sx(x):.1f}" y="{height-pad_b+14}" fill="#8a8680" font-size="9" text-anchor="middle">{x:.0f}</text>')
    for x, y in usable:
        cx = sx(x)
        top = sy(max(y, 0.0))
        bot = sy(min(y, 0.0))
        color = "#d4a017" if y >= 0 else "#6ea8fe"
        parts.append(
            f'<rect x="{cx - bar_w/2:.1f}" y="{top:.1f}" width="{bar_w:.1f}" '
            f'height="{max(bot - top, 0.5):.1f}" fill="{color}" opacity="0.85"/>'
        )
    parts.append(_chart_crosshair(pad_l=pad_l, pad_t=pad_t, plot_r=plot_r, plot_b=plot_b))
    parts.append("</svg>")
    return "\n".join(parts)


def _json_num(value):
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _leg_json(row: dict | None) -> dict | None:
    if row is None:
        return None
    return {
        "root": row.get("symbol"),
        "bid": _json_num(row.get("bid")),
        "ask": _json_num(row.get("ask")),
        "mid": _json_num(row.get("mid")),
        "iv": _json_num(row.get("iv")),
        "delta": _json_num(row.get("delta")),
        "gamma": _json_num(row.get("gamma")),
        "vega": _json_num(row.get("vega")),
        "theta": _json_num(row.get("theta")),
        "open_interest": _json_num(row.get("open_interest")),
        "volume": _json_num(row.get("volume")),
        "bid_size": _json_num(row.get("bid_size")),
        "ask_size": _json_num(row.get("ask_size")),
        "spread": _json_num(row.get("spread")),
    }


def _raw_expiries(raw: pl.DataFrame) -> list[dict]:
    if raw.is_empty() or "expiration" not in raw.columns:
        return []
    out = []
    for exp in sorted(raw["expiration"].drop_nulls().unique().to_list()):
        sl = raw.filter(pl.col("expiration") == exp)
        dte = sl["dte"][0] if "dte" in sl.columns and sl.height else None
        try:
            dte_i = int(dte) if dte is not None and not isinstance(dte, datetime) else None
        except (TypeError, ValueError):
            dte_i = None
        out.append({"expiry": exp.isoformat() if hasattr(exp, "isoformat") else str(exp), "dte": dte_i})
    return out


def _pick_expiry(raw: pl.DataFrame, expiry: date | None, dte_target: int) -> date | None:
    catalog = _raw_expiries(raw)
    parsed = {c["expiry"]: c["dte"] for c in catalog}
    if expiry is not None and expiry.isoformat() in parsed:
        return expiry
    if not catalog:
        return None
    with_dte = [c for c in catalog if c["dte"] is not None]
    if with_dte:
        best = min(with_dte, key=lambda c: abs((c["dte"] or 0) - dte_target))
        return _parse_date(best["expiry"])
    return _parse_date(catalog[0]["expiry"])


def _load_snapshot(data_dir: Path, params: dict[str, str]) -> dict:
    source = params.get("source", "eod")
    if source not in ("eod", "live"):
        source = "eod"
    days = _list_live_days(data_dir) if source == "live" else _list_eod_days(data_dir)
    session = _parse_date(params.get("session"))
    if session not in days:
        session = days[-1] if days else None
    stamp = params.get("stamp") or "latest"
    live_stamps = _list_live_stamps(data_dir, session) if source == "live" and session else []
    if stamp != "latest" and stamp not in live_stamps:
        stamp = "latest"
    asof = None
    raw = pl.DataFrame(schema=_OUTPUT_SCHEMA)
    if session is not None:
        if source == "live":
            raw, asof = _read_live(data_dir, session, None if stamp == "latest" else stamp)
        else:
            raw = _read_eod(data_dir, session)
    dte_target = int(params.get("dte") or 30)
    expiry = _pick_expiry(raw, _parse_date(params.get("expiry")), dte_target)
    return {
        "source": source,
        "session": session,
        "stamp": stamp,
        "live_stamps": live_stamps,
        "asof": asof,
        "raw": raw,
        "expiry": expiry,
        "expiries": _raw_expiries(raw),
        "dte_target": dte_target,
    }


def _summarize_tools(pairs: list[dict], *, expiry: date | None, spot: float | None,
                     band: float = 0.0) -> dict:
    pair_fwd = next((p["forward"] for p in pairs if p["forward"] is not None), None)
    atm_k = None
    if pairs and pair_fwd:
        atm_k = min(pairs, key=lambda r: abs(r["strike"] - pair_fwd))["strike"]
    pain_k, pain_val = _max_pain(pairs)
    flip_k = _gex_flip(pairs, pair_fwd or atm_k)
    tot_ce_oi = sum(p["ce_oi"] for p in pairs)
    tot_pe_oi = sum(p["pe_oi"] for p in pairs)
    tot_ce_gex = sum(p["ce_gex"] for p in pairs)
    tot_pe_gex = sum(p["pe_gex"] for p in pairs)
    tot_net_gex = tot_ce_gex - tot_pe_gex
    pcr = (tot_pe_oi / tot_ce_oi) if tot_ce_oi else None
    ref_spot = pair_fwd or spot
    shown = pairs
    if band and pair_fwd:
        lo, hi = pair_fwd * (1 - band), pair_fwd * (1 + band)
        trimmed = [p for p in pairs if lo <= p["strike"] <= hi]
        if trimmed:
            shown = trimmed
    chain = []
    for p in shown:
        chain.append({
            "strike": p["strike"],
            "atm": atm_k is not None and abs(p["strike"] - atm_k) < 1e-6,
            "call": _leg_json(p["c"]),
            "put": _leg_json(p["p"]),
            "call_oi": p["ce_oi"],
            "put_oi": p["pe_oi"],
            "call_gex": p["ce_gex"],
            "put_gex": p["pe_gex"],
            "net_gex": p["net_gex"],
        })
    return {
        "expiry": _iso(expiry) or None,
        "forward": pair_fwd,
        "spot": spot,
        "atm_strike": atm_k,
        "paired_strikes": len(pairs),
        "returned_strikes": len(shown),
        "call_oi": tot_ce_oi,
        "put_oi": tot_pe_oi,
        "pcr_oi": pcr,
        "call_gex": tot_ce_gex,
        "put_gex": tot_pe_gex,
        "net_gex": tot_net_gex,
        "net_gex_times_forward": tot_net_gex * ref_spot if ref_spot else None,
        "call_oi_wall": max(pairs, key=lambda r: r["ce_oi"])["strike"] if pairs else None,
        "put_oi_wall": max(pairs, key=lambda r: r["pe_oi"])["strike"] if pairs else None,
        "gex_flip": flip_k,
        "max_pain": pain_k,
        "max_pain_contract_points": pain_val,
        "multiplier": SPX_MULTIPLIER,
        "gex_formula": "gamma * open_interest * 100; net = call - put",
        "gex_label": "descriptive listed-OI proxy, not a dealer inventory model",
        "chain": chain,
    }


def api_meta(data_dir: Path, params: dict[str, str]) -> dict:
    source = params.get("source", "eod")
    if source not in ("eod", "live"):
        source = "eod"
    eod_days = _list_eod_days(data_dir)
    live_days = _list_live_days(data_dir)
    days = live_days if source == "live" else eod_days
    session = _parse_date(params.get("session"))
    if session not in days:
        session = days[-1] if days else None
    stamps = _list_live_stamps(data_dir, session) if source == "live" and session else []
    snap = _load_snapshot(data_dir, {**params, "source": source, "session": _iso(session) or ""})
    return {
        "source": source,
        "session": _iso(session) or None,
        "eod_sessions": [d.isoformat() for d in eod_days],
        "live_sessions": [d.isoformat() for d in live_days],
        "stamps": stamps,
        "expiries": snap["expiries"],
        "default_expiry": _iso(snap["expiry"]) or None,
    }


def api_tools(data_dir: Path, params: dict[str, str]) -> dict:
    snap = _load_snapshot(data_dir, params)
    if snap["session"] is None:
        raise ValueError("no session available for this source")
    band = float(params.get("band") or 0)
    spot = _spot(data_dir, snap["session"])
    pairs = _pair_expiry(snap["raw"], snap["expiry"])
    asof = snap["asof"]
    asof_txt = None
    if asof is not None:
        local = asof.astimezone(NY) if getattr(asof, "tzinfo", None) else asof
        asof_txt = local.isoformat()
    payload = _summarize_tools(pairs, expiry=snap["expiry"], spot=spot, band=band)
    payload.update({
        "ok": True,
        "source": snap["source"],
        "session": _iso(snap["session"]),
        "stamp": snap["stamp"],
        "asof": asof_txt,
        "expiries": snap["expiries"],
        "contracts": snap["raw"].height,
        "roots": sorted(set(snap["raw"]["symbol"].to_list())) if not snap["raw"].is_empty() else [],
    })
    return payload


def _load_daily(data_dir: Path, symbol: str) -> pl.DataFrame | None:
    with _CACHE_LOCK:
        if symbol not in _DAILY:
            path = data_dir / "ibkr_eod" / symbol / "daily.parquet"
            _DAILY[symbol] = pl.read_parquet(path) if path.exists() else pl.DataFrame()
        df = _DAILY[symbol]
    return None if df is None or df.is_empty() else df


def _spy_1min_path() -> Path:
    return Path(os.getenv(
        "SPY_1MIN_CSV",
        r"W:\Workspace\LiveTrading\USStocks\SPY_1min_OHLCV_FMP.csv",
    ))


def _spy_1min() -> pl.DataFrame | None:
    """FMP SPY 1-minute RTH bars. Cached until the file size or mtime changes."""
    global _SPY1, _SPY1_KEY
    path = _spy_1min_path()
    if not path.is_file():
        return None
    st = path.stat()
    key = (str(path), st.st_mtime_ns, st.st_size)
    with _CACHE_LOCK:
        if _SPY1 is not None and _SPY1_KEY == key:
            return _SPY1
        frame = pl.read_csv(
            path,
            schema_overrides={
                "dt": pl.Utf8,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
            },
        )
        frame = (
            frame.with_columns(
                pl.col("dt").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
            )
            .drop_nulls("dt")
            .with_columns(pl.col("dt").dt.date().alias("session"))
        )
        _SPY1 = frame
        _SPY1_KEY = key
        return _SPY1


def _spy1_panel(session: date | None) -> str:
    path = _spy_1min_path()
    if not path.is_file():
        return f'<p class="note warn">SPY 1-minute file not found ({html.escape(str(path))}).</p>'
    try:
        frame = _spy_1min()
    except Exception as exc:
        return (
            '<p class="note warn">SPY 1-minute file could not be read ('
            f'{html.escape(type(exc).__name__)}).</p>'
        )
    if frame is None or frame.is_empty():
        return '<p class="note warn">SPY 1-minute file is empty.</p>'
    first = frame["session"].min()
    last = frame["session"].max()
    sessions = frame["session"].n_unique()
    note = (
        "SPY regular-hours 1-minute bars from FMP, clock time America/New_York, "
        f"09:30–15:59. {frame.height:,} bars across {sessions:,} sessions, "
        f"{first.isoformat()} through {last.isoformat()}. "
        "This is SPY, not SPX. No extended hours and no 1-second bars. "
        "1-min RV is the session's minute variance rate, annualized with 252 × 390."
    )
    if session is None:
        return f'<p class="note">{note}</p>'
    day = frame.filter(pl.col("session") == session).sort("dt")
    if day.is_empty():
        return (
            f'<p class="note">{note}</p>'
            '<p class="note warn">No SPY 1-minute bars for this session.</p>'
        )
    opens = day["open"].to_list()
    highs = day["high"].to_list()
    lows = day["low"].to_list()
    closes = day["close"].to_list()
    volumes = day["volume"].to_list()
    stamps = day["dt"].to_list()
    o, c = float(opens[0]), float(closes[-1])
    h, lo = float(max(highs)), float(min(lows))
    vol = float(sum(v for v in volumes if v is not None))
    rets = [
        math.log(b / a) ** 2
        for a, b in zip(closes, closes[1:])
        if a and b and a > 0 and b > 0
    ]
    rv = math.sqrt(sum(rets) / len(rets) * 390 * 252) if rets else None
    start = stamps[0].strftime("%H:%M")
    end = stamps[-1].strftime("%H:%M")
    xs = [stamp.replace(tzinfo=NY).timestamp() for stamp in stamps]
    chart = _svg_lines(
        [("SPY close", xs, [float(v) for v in closes])],
        title=f"SPY 1-minute close  {session.isoformat()}  {start}–{end}",
        xlabel="America/New_York", ylabel="SPY", x_scale="datetime",
    )
    cards = _cards([
        ("Open", _fmt(o, 2)),
        ("High", _fmt(h, 2)),
        ("Low", _fmt(lo, 2)),
        ("Close", _fmt(c, 2)),
        ("Change", _fmt((c / o - 1) if o else None, 2, pct=True)),
        ("Range", _fmt(((h - lo) / o) if o else None, 2, pct=True)),
        ("Volume", _fmt(vol, 0)),
        ("Bars", f"{day.height:,} · {start}–{end}"),
        ("1-min RV", _fmt(rv, 2, pct=True)),
    ])
    return f'<p class="note">{note}</p>{cards}{chart}'


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


def _mid(row: dict | None) -> float | None:
    if not row or row.get("mid") is None:
        return None
    try:
        value = float(row["mid"])
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    return value


def _package(*legs: tuple[dict | None, float]) -> float | None:
    """Net mid of a signed package, in index points. A positive number is a debit."""
    total = 0.0
    for row, qty in legs:
        mid = _mid(row)
        if mid is None:
            return None
        total += qty * mid
    return total


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
        # Premium of the listed package. Long fly = buy the wing strangle, sell the
        # ATM straddle. Long RR = buy the put, sell the call. Points, ×100 = USD.
        straddle_px = _package((atm_c, 1.0), (atm_p, 1.0))
        bf25_px = _package((c25, 1.0), (p25, 1.0), (atm_c, -1.0), (atm_p, -1.0))
        bf10_px = _package((c10, 1.0), (p10, 1.0), (atm_c, -1.0), (atm_p, -1.0))
        rr25_px = _package((p25, 1.0), (c25, -1.0))
        rr10_px = _package((p10, 1.0), (c10, -1.0))
        put10_px = _mid(p10)
        put10_k = None if p10 is None else float(p10["strike"])
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
            "straddle_px": straddle_px,
            "bf25_px": bf25_px,
            "bf10_px": bf10_px,
            "rr25_px": rr25_px,
            "rr10_px": rr10_px,
            "put10_px": put10_px,
            "put10_k": put10_k,
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
        "bf25_px": pt.get("bf25_px"),
        "bf10_px": pt.get("bf10_px"),
        "rr25_px": pt.get("rr25_px"),
        "rr10_px": pt.get("rr10_px"),
    }


def _ensure_atm_history(data_dir: Path) -> list[dict]:
    global _ATM_HISTORY
    with _CACHE_LOCK:
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


def refresh_caches(data_dir: Path) -> dict:
    """Drop in-memory history and rebuild it from the Parquet lake."""
    global _NYSE_SESSIONS, _ATM_HISTORY, _VRP_CACHE
    with _CACHE_LOCK:
        _NYSE_SESSIONS = None
        _ATM_HISTORY = None
        _VRP_CACHE = None
        _DAILY.clear()
        _CROSS_ATM.clear()
        print("[surf09] refreshing caches from disk…", flush=True)
        hist = _ensure_atm_history(data_dir)
        days = _list_eod_days(data_dir)
    last = days[-1].isoformat() if days else None
    print(f"[surf09] refresh done  iv_rank={len(hist)} eod_days={len(days)} last={last}",
          flush=True)
    return {
        "ok": True,
        "iv_rank_sessions": len(hist),
        "eod_days": len(days),
        "last_session": last,
    }


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


_STRUCTURES = (
    ("bf25_px", "25Δ fly", "Buy the fly", "Sell the fly"),
    ("bf10_px", "10Δ fly", "Buy the fly", "Sell the fly"),
    ("rr25_px", "25Δ RR", "Buy put / sell call", "Sell put / buy call"),
    ("rr10_px", "10Δ RR", "Buy put / sell call", "Sell put / buy call"),
)


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round(q * (len(ordered) - 1)))))
    return ordered[idx]


def _structure_panel(hist: list[dict]) -> str:
    """Rank ~30 DTE fly and RR package prices against their own history through each day."""
    note = (
        "Baseline is the ~30 DTE SPX+SPXW package on each session, same liquidity filter as the "
        "term structure. A fly price is the wing strangle minus the ATM straddle, so a credit is "
        "negative and a more negative number is cheaper to buy. An RR price is the put mid minus "
        "the call mid. The percentile uses only sessions up through that day. After 60 sessions, "
        "a cost at or below the 10th percentile is cheap to put on, and a cost at or above the "
        "90th is rich enough to sell. ×100 = USD per SPX contract. This is a price rank against "
        "the baseline, not a forecast of P&amp;L and not an order."
    )
    usable = [h for h in hist if any(h.get(key) is not None for key, *_ in _STRUCTURES)]
    if len(usable) < 5:
        return (
            f'<p class="note">{note}</p>'
            '<p class="note warn">Structure prices are not in the history cache yet. '
            "Use Refresh cache and reload.</p>"
        )
    cards: list[tuple[str, str]] = []
    charts = []
    alerts: list[tuple[date, int, str, float, float, str]] = []
    for key, label, buy, sell in _STRUCTURES:
        series = [(h["session"], h["dte"], h[key]) for h in usable if h.get(key) is not None]
        values = [v for _, _, v in series]
        ranks = []
        for i, value in enumerate(values):
            rank = sum(1 for v in values[:i + 1] if v <= value) / (i + 1)
            ranks.append(rank)
            if i + 1 < 60:
                continue
            if rank <= 0.10:
                alerts.append((series[i][0], series[i][1], label, value, rank, buy))
            elif rank >= 0.90:
                alerts.append((series[i][0], series[i][1], label, value, rank, sell))
        last_session, last_dte, last_px = series[-1]
        last_rank = ranks[-1]
        if len(values) < 60:
            action = "baseline still building"
        elif last_rank <= 0.10:
            action = buy
        elif last_rank >= 0.90:
            action = sell
        else:
            action = "inside the band"
        p10, p50, p90 = _quantile(values, 0.10), _quantile(values, 0.50), _quantile(values, 0.90)
        cards.append((
            f"{label} · {last_dte}d",
            f"{last_px:.2f} · {last_rank:.0%} · {action}",
        ))
        cards.append((
            f"{label} baseline",
            f"p10 {p10:.2f} · p50 {p50:.2f} · p90 {p90:.2f}",
        ))
        window = series[-252:]
        charts.append(_svg_lines(
            [(label, [d.toordinal() for d, _, _ in window], [v for _, _, v in window])],
            title=f"{label} cost · {last_session.isoformat()} is {last_px:.2f} ({last_rank:.0%})",
            xlabel="Session", ylabel="index points", x_scale="date",
        ))
    recent_cut = usable[-1]["session"] - timedelta(days=400)
    recent = [row for row in alerts if row[0] >= recent_cut]
    recent.sort(key=lambda row: (row[0], row[2]), reverse=True)
    rows = [[
        row[0].isoformat(),
        str(row[1]),
        html.escape(row[2]),
        f"{row[3]:.2f}",
        f"{row[4]:.0%}",
        html.escape(row[5]),
    ] for row in recent[:40]]
    today = usable[-1]["session"]
    classes = ["atm" if row[0] == today else "" for row in recent[:40]]
    summary = (
        f"{len(recent)} alerts in the last year of sessions "
        f"({sum(1 for row in recent if row[5].startswith('Buy'))} cheap to buy, "
        f"{sum(1 for row in recent if row[5].startswith('Sell'))} rich to sell). "
        f"The highlighted row, when present, is {today.isoformat()}."
    )
    return (
        f'<p class="note">{note}</p>'
        + _cards(cards)
        + f'<p class="note">{summary}</p>'
        + '<div class="grid">' + "".join(f"<div>{svg}</div>" for svg in charts) + "</div>"
        + _table(
            ["Session", "DTE", "Structure", "Cost", "Percentile", "Screen"],
            rows, classes,
        )
    )


def _rv(data_dir: Path, day: date, window: int, symbol: str = "SPX") -> float | None:
    bars = _load_daily(data_dir, symbol)
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


def _spx_returns(data_dir: Path, day: date) -> tuple[list[date], list[float]]:
    bars = _load_daily(data_dir, "SPX")
    if bars is None:
        return [], []
    hit = bars.filter(pl.col("session") <= day).sort("session")
    sessions = hit["session"].to_list()
    closes = hit["close"].to_list()
    days: list[date] = []
    rets: list[float] = []
    for i in range(1, len(closes)):
        a, b = closes[i - 1], closes[i]
        if a and b and a > 0 and b > 0:
            days.append(sessions[i])
            rets.append(math.log(float(b) / float(a)))
    return days, rets


_FOMC_DATES: set[date] | None = None


def _fomc_dates() -> set[date]:
    """Statement days from Macro.fomc_meetings. Empty when the database is unreachable."""
    global _FOMC_DATES
    if _FOMC_DATES is not None:
        return _FOMC_DATES
    auth = _macro_auth()
    if auth is None:
        return set()
    host, user, password, database = auth
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database,
            connection_timeout=3,
        )
        cur = conn.cursor()
        cur.execute("SELECT meeting_date FROM fomc_meetings")
        found: set[date] = set()
        for (day,) in cur.fetchall():
            if isinstance(day, datetime):
                day = day.date()
            if isinstance(day, date):
                found.add(day)
        conn.close()
    except Exception as exc:
        sys.stderr.write(f"[surf09] fomc dates unavailable: {type(exc).__name__}: {exc}\n")
        return set()
    if found:
        _FOMC_DATES = found
    return found


def _rv_forecast(data_dir: Path, day: date) -> dict | None:
    """EWMA variance forecast plus the historical error distribution. RV-04, research only."""
    days, rets = _spx_returns(data_dir, day)
    if len(rets) < 80:
        return None
    lam = 0.94
    var = rets[0] ** 2
    after: list[float] = []
    for ret in rets:
        var = lam * var + (1.0 - lam) * ret * ret
        after.append(var)
    point = math.sqrt(after[-1] * 252.0)
    errors: list[float] = []
    xs: list[date] = []
    fc: list[float] = []
    realized: list[float] = []
    for i in range(len(rets) - 20):
        window = rets[i + 1:i + 21]
        real = math.sqrt(252.0 * sum(r * r for r in window) / len(window))
        forecast = math.sqrt(after[i] * 252.0)
        errors.append(real - forecast)
        if days[i] >= day - timedelta(days=500):
            xs.append(days[i])
            fc.append(forecast)
            realized.append(real)
    if len(errors) < 40:
        return None
    err = np.asarray(errors, dtype=float)
    p10, p50, p90 = (float(v) for v in np.quantile(err, [0.1, 0.5, 0.9]))
    return {
        "point": point,
        "err_p10": p10,
        "err_p50": p50,
        "err_p90": p90,
        "lo": point + p10,
        "hi": point + p90,
        "n": len(errors),
        "xs": xs[-252:],
        "fc": fc[-252:],
        "rv": realized[-252:],
    }


def _event_variance(data_dir: Path, day: date) -> dict | None:
    """Close-to-close variance on FOMC statement days versus every other session. RV-05."""
    fomc = _fomc_dates()
    days, rets = _spx_returns(data_dir, day)
    if len(rets) < 80 or not fomc:
        return None
    event = [r * r for d, r in zip(days, rets) if d in fomc]
    other = [r * r for d, r in zip(days, rets) if d not in fomc]
    if len(event) < 8 or len(other) < 20:
        return None

    def _ann(vals: list[float]) -> float:
        return math.sqrt(252.0 * sum(vals) / len(vals))

    return {
        "event": _ann(event),
        "other": _ann(other),
        "n_event": len(event),
        "n_other": len(other),
    }


def _research_section(
    session: date | None,
    term: list[dict],
    n30: dict | None,
    fit: dict | None,
    svi: dict | None,
    forecast: dict | None,
    event: dict | None,
    atms: list[float],
    rr25s: list[float],
    iv_rank: float | None,
    rr_z: float | None,
) -> str:
    """Research readings that the chain, SPX closes, and FOMC dates can support."""
    bits = [
        "<p class=\"note\">Research labels, not orders. svi-raw v1 is Gatheral raw SVI on "
        "OTM total variance for the selected expiry, id stamped beside quad-k v1. "
        "The RV forecast is an EWMA (λ 0.94) of SPX daily variance, and the band is that "
        "point plus the 10th and 90th historical errors against the next 20 sessions. "
        "Event variance uses FOMC statement days only. CPI reference months are not release days, "
        "so they are not in this split. Nothing here is sent to a broker.</p>"
    ]
    version = []
    if svi:
        version.append((
            "svi-raw v1",
            f"a={svi['a']:.4f} b={svi['b']:.4f} ρ={svi['rho']:.3f} m={svi['m']:.3f} σ={svi['sigma']:.3f}"
            + (" · ρ on bound" if abs(svi["rho"]) >= 0.99 else ""),
        ))
        version.append(("SVI RMSE", f"{svi['rmse']*100:.2f} vol pts · n={svi['n']}"))
        version.append(("g(k)<0", f"{svi['g_breaks']} / 41 grid points"))
    if fit:
        version.append(("quad-k v1 RMSE", f"{fit['rmse']*100:.2f} vol pts · n={fit['n']}"))
    if session and (fit or svi):
        version.append(("Fit as of", f"{session.isoformat()} · selected expiry"))
    if version:
        bits.append(_cards(version))
    if forecast:
        bits.append(_cards([
            ("EWMA forecast", _fmt(forecast["point"], 2, pct=True)),
            ("10th error", _fmt(forecast["err_p10"], 2, pct=True)),
            ("Median error", _fmt(forecast["err_p50"], 2, pct=True)),
            ("90th error", _fmt(forecast["err_p90"], 2, pct=True)),
            ("Forecast band", f"{_fmt(forecast['lo'], 2, pct=True)} – {_fmt(forecast['hi'], 2, pct=True)}"),
            ("Error sample", f"{forecast['n']} windows"),
        ]))
        if forecast["xs"]:
            bits.append(_svg_lines(
                [
                    ("EWMA forecast", [d.toordinal() for d in forecast["xs"]], forecast["fc"]),
                    ("Next 20d RV", [d.toordinal() for d in forecast["xs"]], forecast["rv"]),
                ],
                title="SPX variance forecast vs the next 20 sessions",
                xlabel="Session", ylabel="annualized vol", y_pct=True, x_scale="date",
            ))
    if event:
        ratio = event["event"] / event["other"] if event["other"] else None
        bits.append(_cards([
            ("FOMC-day RV", _fmt(event["event"], 2, pct=True)),
            ("Other-day RV", _fmt(event["other"], 2, pct=True)),
            ("FOMC / other", "—" if ratio is None else f"{ratio:.2f}×"),
            ("FOMC days", str(event["n_event"])),
            ("Other days", f"{event['n_other']:,}"),
        ]))
    fomc = _fomc_dates()
    upcoming = sorted(d for d in fomc if session is None or d >= session)
    if upcoming and len(term) >= 2:
        nxt = upcoming[0]
        before = [r for r in term if r["expiration"] < nxt]
        after = [r for r in term if r["expiration"] >= nxt]
        if before and after:
            left, right = before[-1], after[0]
            gap = right["total_var"] - left["total_var"]
            move = math.sqrt(gap) if gap > 0 else None
            bits.append(_cards([
                ("Next FOMC", nxt.isoformat()),
                ("Bracket", f"{left['dte']}d → {right['dte']}d"),
                ("Gap move", _fmt(move, 2, pct=True)),
            ]))
            bits.append(
                '<p class="note">Gap move is √(σ²T after − σ²T before) for the expiries that '
                "bracket the next FOMC statement day. It includes the ordinary days in that gap, "
                "not a pure event extract.</p>"
            )
    if n30 and n30.get("put10_px") and n30.get("put10_k") and n30.get("forward"):
        payoff = max(n30["put10_k"] - 0.9 * n30["forward"], 0.0)
        premium = n30["put10_px"]
        ratio = payoff / premium if premium else None
        bits.append(_cards([
            ("30d 10Δ put", _fmt(premium, 2)),
            ("Strike", _fmt(n30["put10_k"], 2)),
            ("−10% intrinsic", _fmt(payoff, 2)),
            ("Payoff / premium", "—" if ratio is None else f"{ratio:.2f}×"),
        ]))
        bits.append(
            '<p class="note">Tail ratio is the intrinsic of the listed 10Δ put if the forward '
            "is 10% lower at expiry, divided by its mid. No probability and no hedge.</p>"
        )
    rank_252 = _percentile(atms[-252:], n30["atm"]) if n30 and len(atms) >= 40 else None
    z_drop = _zscore(rr25s[:-21], n30["rr25"]) if n30 and n30.get("rr25") is not None and len(rr25s) > 40 else None
    bits.append(_cards([
        ("IV rank, full", "—" if iv_rank is None else f"{iv_rank:.0%}"),
        ("IV rank, 252d", "—" if rank_252 is None else f"{rank_252:.0%}"),
        ("RR25 z, full", "—" if rr_z is None else f"{rr_z:.2f}"),
        ("RR25 z, drop 21d", "—" if z_drop is None else f"{z_drop:.2f}"),
    ]))
    bits.append(
        "<p class=\"note\">Not started: bronze/silver/gold storage (a lake rewrite), "
        "dealer GEX as an inventory signal (the strike chart stays descriptive), "
        "dispersion, collars, income, and defense (no single-name surfaces or borrow), "
        "and the strategy, portfolio, execution, and governance phases.</p>"
    )
    return "".join(bits)


def _range_rv(data_dir: Path, day: date, window: int) -> tuple[float | None, float | None]:
    """Parkinson and Garman–Klass from SPX OHLC. Returns (park, gk)."""
    bars = _load_daily(data_dir, "SPX")
    if bars is None:
        return None, None
    if not {"open", "high", "low", "close"}.issubset(set(bars.columns)):
        return None, None
    hit = bars.filter(pl.col("session") <= day).sort("session").tail(window)
    if hit.height < window:
        return None, None
    park_acc = gk_acc = 0.0
    n = 0
    for row in hit.iter_rows(named=True):
        o, h, low, c = row["open"], row["high"], row["low"], row["close"]
        if not all(v and v > 0 for v in (o, h, low, c)):
            continue
        hl = math.log(h / low) ** 2
        co = math.log(c / o) ** 2
        park_acc += hl
        gk_acc += 0.5 * hl - (2.0 * math.log(2.0) - 1.0) * co
        n += 1
    if n < max(5, window // 2):
        return None, None
    park = math.sqrt(252.0 * park_acc / n / (4.0 * math.log(2.0)))
    gk = math.sqrt(max(252.0 * gk_acc / n, 0.0))
    return park, gk


def _add_side_iv(df: pl.DataFrame) -> pl.DataFrame:
    """Black-76 IV on bid and ask using the stored forward/discount (SURF-13)."""
    empty = df.with_columns(
        pl.lit(None, pl.Float64).alias("iv_bid"),
        pl.lit(None, pl.Float64).alias("iv_ask"),
    )
    if df.is_empty() or "tau" not in df.columns:
        return empty
    tau = df["tau"].to_numpy().astype(float)
    dte = df["dte"].to_numpy().astype(float)
    tau = np.where(np.isfinite(tau) & (tau > 1e-8), tau, dte / YEAR)
    tau = np.where(tau > 1e-8, tau, np.nan)
    disc = df["discount"].to_numpy().astype(float)
    disc = np.where(np.isfinite(disc) & (disc > 0), disc, 1.0)
    fwd = df["forward"].to_numpy().astype(float)
    k = df["strike"].to_numpy().astype(float)
    is_call = (df["right"] == "C").to_numpy()
    bid_iv = _implied_vol(df["bid"].to_numpy().astype(float), fwd, k, tau, disc, is_call)
    ask_iv = _implied_vol(df["ask"].to_numpy().astype(float), fwd, k, tau, disc, is_call)
    return df.with_columns(
        pl.Series("iv_bid", bid_iv),
        pl.Series("iv_ask", ask_iv),
    )


def _fit_quad_k(slice_: pl.DataFrame) -> dict | None:
    """Weighted quadratic total-variance fit in log-moneyness. Model id: quad-k v1."""
    if slice_.height < 8:
        return None
    otm = slice_.filter(
        ((pl.col("right") == "C") & (pl.col("strike") >= pl.col("forward")))
        | ((pl.col("right") == "P") & (pl.col("strike") <= pl.col("forward")))
    )
    if otm.height < 8:
        otm = slice_
    tau = float(otm["tau"].drop_nulls()[0] if otm["tau"].null_count() < otm.height else otm["dte"][0] / YEAR)
    if tau <= 1e-8:
        return None
    k = np.log(otm["strike"].to_numpy() / otm["forward"].to_numpy())
    iv = otm["iv"].to_numpy().astype(float)
    wgt = 1.0 / np.maximum(otm["spread"].to_numpy().astype(float), 0.05)
    ok = np.isfinite(k) & np.isfinite(iv) & (iv > 0)
    k, iv, wgt = k[ok], iv[ok], wgt[ok]
    if k.size < 8:
        return None
    tot = iv * iv * tau
    x = np.column_stack([np.ones(k.size), k, k * k]) * wgt[:, None]
    beta, *_ = np.linalg.lstsq(x, tot * wgt, rcond=None)
    fitted_w = np.column_stack([np.ones(k.size), k, k * k]) @ beta
    fitted_iv = np.sqrt(np.maximum(fitted_w / tau, 0.0))
    resid = iv - fitted_iv
    rmse = float(np.sqrt(np.mean(resid * resid)))
    return {
        "model": "quad-k v1",
        "a": float(beta[0]), "b": float(beta[1]), "c": float(beta[2]),
        "rmse": rmse,
        "n": int(k.size),
        "tau": tau,
        "k": k.tolist(),
        "iv": iv.tolist(),
        "fit": fitted_iv.tolist(),
        "resid": resid.tolist(),
    }


def _svi_total_var(k: np.ndarray, a: float, b: float, rho: float, m: float, sigma: float) -> np.ndarray:
    """Gatheral raw SVI total variance. Model id: svi-raw v1."""
    dk = k - m
    return a + b * (rho * dk + np.sqrt(dk * dk + sigma * sigma))


def _svi_g_breaks(a: float, b: float, rho: float, m: float, sigma: float) -> int:
    """How many points on a log-moneyness grid fail Gatheral's g(k) ≥ 0 density check."""
    ks = np.linspace(-0.5, 0.5, 41)
    dk = ks - m
    disc = np.sqrt(dk * dk + sigma * sigma)
    w = np.maximum(_svi_total_var(ks, a, b, rho, m, sigma), 1e-8)
    wp = b * (rho + dk / disc)
    wpp = b * (sigma * sigma) / disc ** 3
    g = (1.0 - ks * wp / (2.0 * w)) ** 2 - (wp * wp) / 4.0 * (1.0 / w + 0.25) + wpp / 2.0
    return int(np.sum(~np.isfinite(g) | (g < 0)))


def _fit_svi(slice_: pl.DataFrame) -> dict | None:
    """Raw SVI on the same OTM total-variance points as quad-k. Not calendar-arbitrage free."""
    if slice_.height < 8:
        return None
    otm = slice_.filter(
        ((pl.col("right") == "C") & (pl.col("strike") >= pl.col("forward")))
        | ((pl.col("right") == "P") & (pl.col("strike") <= pl.col("forward")))
    )
    if otm.height < 8:
        otm = slice_
    tau = float(otm["tau"].drop_nulls()[0] if otm["tau"].null_count() < otm.height else otm["dte"][0] / YEAR)
    if tau <= 1e-8:
        return None
    k = np.log(otm["strike"].to_numpy() / otm["forward"].to_numpy())
    iv = otm["iv"].to_numpy().astype(float)
    wgt = 1.0 / np.maximum(otm["spread"].to_numpy().astype(float), 0.05)
    ok = np.isfinite(k) & np.isfinite(iv) & (iv > 0) & np.isfinite(wgt)
    k, iv, wgt = k[ok], iv[ok], wgt[ok]
    if k.size < 8:
        return None
    tot = iv * iv * tau
    try:
        from scipy.optimize import least_squares
    except ImportError:
        return None

    def residual(theta: np.ndarray) -> np.ndarray:
        a, b, rho, m, sigma = (float(v) for v in theta)
        return (_svi_total_var(k, a, b, rho, m, sigma) - tot) * np.sqrt(wgt)

    a0 = float(max(np.median(tot) * 0.5, 1e-4))
    start = np.array([a0, 0.1, -0.4, 0.0, 0.2])
    try:
        solved = least_squares(
            residual, start,
            bounds=([1e-6, 1e-6, -0.999, -1.5, 1e-3], [5.0, 5.0, 0.999, 1.5, 2.0]),
            max_nfev=200,
        )
    except Exception:
        return None
    if not np.all(np.isfinite(solved.x)):
        return None
    a, b, rho, m, sigma = (float(v) for v in solved.x)
    fitted_w = _svi_total_var(k, a, b, rho, m, sigma)
    if np.any(fitted_w <= 0):
        return None
    fitted_iv = np.sqrt(fitted_w / tau)
    resid = iv - fitted_iv
    return {
        "model": "svi-raw v1",
        "a": a, "b": b, "rho": rho, "m": m, "sigma": sigma,
        "rmse": float(np.sqrt(np.mean(resid * resid))),
        "n": int(k.size),
        "tau": tau,
        "g_breaks": _svi_g_breaks(a, b, rho, m, sigma),
        "iv": iv.tolist(),
        "fit": fitted_iv.tolist(),
        "resid": resid.tolist(),
    }


def _butterfly_violations(slice_: pl.DataFrame) -> int:
    """Count adjacent-strike mid-price convexity breaks on calls (SURF-05)."""
    calls = slice_.filter(pl.col("right") == "C").sort("strike")
    if calls.height < 3:
        return 0
    strikes = calls["strike"].to_list()
    mids = calls["mid"].to_list()
    bad = 0
    for i in range(1, len(strikes) - 1):
        w = (strikes[i] - strikes[i - 1]) / (strikes[i + 1] - strikes[i - 1])
        floor = (1 - w) * mids[i - 1] + w * mids[i + 1]
        if mids[i] > floor + 0.05:
            bad += 1
    return bad


def _stale_mask(df: pl.DataFrame, max_age_sec: float) -> pl.DataFrame:
    if df.is_empty() or "quote_timestamp" not in df.columns or "poll_timestamp" not in df.columns:
        return df
    if df["quote_timestamp"].null_count() == df.height:
        return df
    q = df["quote_timestamp"]
    p = df["poll_timestamp"]
    # quote is naive ET; poll is UTC
    age = (
        p.dt.convert_time_zone("America/New_York").dt.replace_time_zone(None)
        - q
    ).dt.total_seconds()
    return df.with_columns(age.alias("_age")).filter(
        pl.col("_age").is_null() | (pl.col("_age") <= max_age_sec)
    ).drop("_age")


def _cross_atm(data_dir: Path, day: date) -> list[dict]:
    """30d ATM IV for other EOD roots on the same session (SURF-14)."""
    with _CACHE_LOCK:
        if day in _CROSS_ATM:
            return _CROSS_ATM[day]
        rows = []
        for root in ("SPX", "SPXW", "NDX", "SPY", "QQQ", "IWM", "VIX"):
            path = data_dir / "thetadata_eod" / root / f"dt={day.isoformat()}" / "eod.parquet"
            if not path.exists():
                continue
            raw = _read_parquet_aligned(path)
            liq = _filter(raw, max_spread=DEFAULT_SPREAD, min_size=DEFAULT_MIN_SIZE, dte_min=DEFAULT_DTE_MIN)
            pt = _point_near_dte(_term_rows(liq), 30)
            if pt:
                rows.append({"root": root, "atm": pt["atm"], "dte": pt["dte"], "rr25": pt["rr25"]})
        rows.sort(key=lambda r: r["atm"])
        _CROSS_ATM[day] = rows
        return rows


def _session_rv(data_dir: Path, day: date, window: int) -> tuple[float | None, float | None]:
    """Overnight vs RTH close-to-open / open-to-close from daily OHLC (RV-03)."""
    bars = _load_daily(data_dir, "SPX")
    if bars is None:
        return None, None
    need = {"open", "high", "low", "close", "session"}
    if not need.issubset(set(bars.columns)):
        return None, None
    hit = bars.filter(pl.col("session") <= day).sort("session")
    if hit.height < window + 1:
        return None, None
    rows = hit.tail(window + 1).iter_rows(named=True)
    overnight: list[float] = []
    rth: list[float] = []
    prev = None
    for row in rows:
        if prev is not None:
            pc, o, c = prev["close"], row["open"], row["close"]
            if pc and o and pc > 0 and o > 0:
                overnight.append(math.log(o / pc) ** 2)
            if o and c and o > 0 and c > 0:
                rth.append(math.log(c / o) ** 2)
        prev = row
    on_v = math.sqrt(252.0 * sum(overnight) / len(overnight)) if len(overnight) >= window // 2 else None
    in_v = math.sqrt(252.0 * sum(rth) / len(rth)) if len(rth) >= window // 2 else None
    return on_v, in_v


def _dup_count(df: pl.DataFrame) -> int:
    if df.is_empty():
        return 0
    keys = [c for c in ("symbol", "expiration", "strike", "right") if c in df.columns]
    if len(keys) < 4:
        return 0
    return int(df.group_by(keys).len().filter(pl.col("len") > 1).height)


def _price_bound_count(df: pl.DataFrame) -> int:
    """Mids outside Black-76 intrinsic/upper bounds (QUAL-04)."""
    if df.is_empty():
        return 0
    need = {"mid", "forward", "strike", "discount", "right"}
    if not need.issubset(set(df.columns)):
        return 0
    mid = df["mid"].to_numpy().astype(float)
    fwd = df["forward"].to_numpy().astype(float)
    k = df["strike"].to_numpy().astype(float)
    disc = df["discount"].to_numpy().astype(float)
    disc = np.where(np.isfinite(disc) & (disc > 0), disc, 1.0)
    is_call = (df["right"] == "C").to_numpy()
    intrinsic = np.where(is_call, disc * np.maximum(fwd - k, 0.0), disc * np.maximum(k - fwd, 0.0))
    upper = np.where(is_call, disc * fwd, disc * k)
    ok = np.isfinite(mid) & np.isfinite(fwd) & np.isfinite(k) & (mid > 0)
    low = ok & (mid < intrinsic - 0.05)
    high = ok & np.isfinite(upper) & (mid > upper + 0.05)
    return int(np.sum(low | high))


def _butterfly_all(liq: pl.DataFrame) -> int:
    if liq.is_empty():
        return 0
    total = 0
    keys = [c for c in ("symbol", "expiration") if c in liq.columns]
    for _, grp in liq.group_by(keys, maintain_order=True):
        total += _butterfly_violations(grp)
    return total


def _missing_eod(data_dir: Path, eod_days: list[date], session: date | None) -> list[date]:
    if session is None:
        return []
    have = set(eod_days)
    recent = sorted(d for d in _nyse_sessions() if d <= session)
    return [d for d in recent[-20:] if d not in have]


def _schema_note(data_dir: Path, day: date, source: str) -> str:
    if source != "live":
        return "EOD 44-col"
    widths: set[int] = set()
    for root in ROOTS:
        day_dir = data_dir / "thetadata" / root / f"dt={day.isoformat()}"
        if not day_dir.exists():
            continue
        for path in sorted(day_dir.glob("*.parquet"))[-12:]:
            if path.name == "compacted.parquet":
                continue
            try:
                widths.add(len(pl.read_parquet_schema(path)))
            except Exception:
                widths.add(-1)
    if not widths:
        return "no live files"
    if widths == {44}:
        return "live 44-col"
    if 42 in widths and 44 in widths:
        return f"live mixed {sorted(w for w in widths if w > 0)}-col (last snapshot aligned)"
    return f"live cols={sorted(widths)}"


def _health_items(
    data_dir: Path, *, eod_days: list[date], live_days: list[date],
    session: date | None, source: str, raw: pl.DataFrame, crossed: int,
    dups: int, bounds: int, stale_dropped: int, missing: list[date],
) -> list[tuple[str, str]]:
    eod_last = eod_days[-1].isoformat() if eod_days else "—"
    live_last = live_days[-1].isoformat() if live_days else "—"
    miss = ", ".join(d.isoformat()[-5:] for d in missing[-6:]) if missing else "none"
    lag = "—"
    if session is not None and eod_days:
        lag = str((session - eod_days[-1]).days) if source == "live" else "0"
        if source == "eod":
            lag = str((eod_days[-1] - session).days) if eod_days[-1] != session else "0"
    return [
        ("EOD sessions (SPX+SPXW)", f"{len(eod_days):,} · last {eod_last}"),
        ("Live sessions listed", f"{len(live_days):,} · last {live_last}"),
        ("Schema (this session)", _schema_note(data_dir, session, source) if session else "—"),
        ("Missing EOD (last 20 NYSE)", miss),
        ("Duplicate contract keys", str(dups)),
        ("Crossed quotes isolated", str(crossed)),
        ("Mid outside price bounds", str(bounds)),
        ("Stale quotes dropped", str(stale_dropped)),
        ("Rows this snapshot", f"{raw.height:,}"),
        ("EOD lag vs picker (days)", lag),
    ]


def _vrp_series(hist: list[dict], data_dir: Path) -> list[tuple[date, float]]:
    """Research VRP: ATM² minus *subsequent* 20-session close-to-close variance."""
    global _VRP_CACHE
    with _CACHE_LOCK:
        if _VRP_CACHE is None:
            bars = _load_daily(data_dir, "SPX")
            src = _ATM_HISTORY or hist
            if bars is None or not src:
                _VRP_CACHE = []
            else:
                ordered = bars.sort("session")
                sess = ordered["session"].to_list()
                closes = ordered["close"].to_list()
                j = 0
                out: list[tuple[date, float]] = []
                for h in src:
                    while j < len(sess) and sess[j] <= h["session"]:
                        j += 1
                    window = closes[j:j + 21]
                    if len(window) < 21:
                        continue
                    rets = []
                    for a, b in zip(window, window[1:]):
                        if a and b and a > 0 and b > 0:
                            rets.append(math.log(b / a) ** 2)
                    if len(rets) < 20:
                        continue
                    rv = math.sqrt(252.0 * sum(rets) / len(rets))
                    out.append((h["session"], h["atm"] ** 2 - rv ** 2))
                _VRP_CACHE = out
        last = hist[-1]["session"] if hist else None
        return [p for p in _VRP_CACHE if last is None or p[0] <= last]


def _axis_x_label(x: float, x_scale: str, span: float) -> str:
    """Tick text for a chart x value. Dates are YYYY-MM-DD; datetimes include the clock."""
    if x_scale == "date":
        return date.fromordinal(int(round(x))).isoformat()
    if x_scale == "datetime":
        return datetime.fromtimestamp(x, tz=NY).strftime("%Y-%m-%d %H:%M")
    return f"{x:.2f}" if span < 5 else f"{x:.0f}"


def _svg_lines(series: list[tuple[str, list[float], list[float]]], *,
               title: str, xlabel: str, ylabel: str,
               width: int = 640, height: int = 280,
               y_pct: bool = False, x_scale: str = "number") -> str:
    usable = [(name, xs, ys) for name, xs, ys in series if xs and ys and len(xs) == len(ys)]
    if not usable:
        return ""
    dated = x_scale in ("date", "datetime")
    pad_l, pad_r, pad_t, pad_b = 56, 16, 28, (64 if dated else 40)
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

    span = xmax - xmin
    multi = len(usable) > 1
    hover = []
    for name, xs, ys in usable:
        for x, y in zip(xs, ys):
            if not (math.isfinite(x) and math.isfinite(y)):
                continue
            xlab = _axis_x_label(x, x_scale, span)
            text = f"{name}  ·  {xlab}  ·  {ylab(y)}" if multi else f"{xlab}  ·  {ylab(y)}"
            hover.append({"sx": round(sx(x), 1), "sy": round(sy(y), 1), "t": text})
    colors = ("#d4a017", "#6ea8fe", "#9d7cd8", "#7dcea0", "#e07a5f", "#c9ada7")
    plot_r = width - pad_r
    plot_b = height - pad_b
    parts = [
        _svg_open(width, height, title, hover, "xy"),
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
        xlab = html.escape(_axis_x_label(x, x_scale, xmax - xmin))
        if dated:
            ly = height - pad_b + 16
            parts.append(
                f'<text x="{px:.1f}" y="{ly}" fill="#8a8680" font-size="9" '
                f'text-anchor="end" transform="rotate(-40 {px:.1f} {ly})">{xlab}</text>'
            )
        else:
            parts.append(
                f'<text x="{px:.1f}" y="{height-pad_b+14}" fill="#8a8680" font-size="9" '
                f'text-anchor="middle">{xlab}</text>'
            )
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
    parts.append(_chart_crosshair(pad_l=pad_l, pad_t=pad_t, plot_r=plot_r, plot_b=plot_b))
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
  .chart .xh-hit {{ cursor: crosshair; }}
  table {{ width: 100%; border-collapse: collapse; font: 12px/1.4 ui-monospace, monospace; margin-top: 8px; }}
  th, td {{ text-align: right; padding: 4px 8px; border-bottom: 1px solid #222; }}
  th:first-child, td:first-child {{ text-align: left; }}
  th {{ color: #8a8680; font-weight: 500; }}
  .note {{ color: #8a8680; font-size: 12px; margin: 8px 0 16px; }}
  .warn {{ color: #d4a017; }}
  .bad {{ color: #e07a5f; }}
  .scroll {{ overflow-x: auto; }}
  tr.atm td {{ background: #241c08; }}
  .toolbar {{ display: flex; gap: 8px; padding: 0 24px 8px; }}
  .toolbar button {{ background: #2a2a2a; color: #e8e4d9; }}
  details.bar {{ border: 1px solid #2a2a2a; background: #141414; margin: 8px 0; }}
  details.bar > summary {{
    list-style: none; cursor: pointer; padding: 10px 14px;
    font-size: 12px; font-weight: 600; letter-spacing: .04em;
    text-transform: uppercase; color: #d4a017; user-select: none;
  }}
  details.bar > summary::-webkit-details-marker {{ display: none; }}
  details.bar > summary::before {{ content: "▸ "; color: #8a8680; }}
  details.bar[open] > summary::before {{ content: "▾ "; }}
  details.bar > .body {{ padding: 4px 14px 14px; }}
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
  <label>Snapshot (PIT)
    <select name="stamp">{ctx["stamp_opts"]}</select>
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
  <label>Stale sec (live)
    <input name="stale" type="number" min="0" value="{ctx["stale"]}"/>
  </label>
  <button type="submit">Update</button>
</form>
<div class="toolbar">
  <button type="button" id="refresh-cache">Refresh cache</button>
  <button type="button" id="open-all">Expand all</button>
  <button type="button" id="close-all">Collapse all</button>
  <span id="refresh-status" class="sub"></span>
</div>
<main>
  <details class="bar" data-id="health"><summary>System health</summary><div class="body">{ctx["health"]}</div></details>
  <details class="bar" data-id="surface"><summary>Session surface</summary><div class="body">{ctx["quality"]}<p class="note">{ctx["filter_note"]}</p></div></details>
  <details class="bar" data-id="overlay"><summary>Futures / VIX overlay</summary><div class="body">{ctx["overlay"]}</div></details>
  <details class="bar" data-id="macro"><summary>CPI / FOMC</summary><div class="body">{ctx["macro"]}</div></details>
  <details class="bar" data-id="chain"><summary>Option chain · IV / Greeks / OI</summary><div class="body"><p class="note">{ctx["chain_note"]}</p>{ctx["chain_cards"]}<div class="scroll">{ctx["pair_table"]}</div></div></details>
  <details class="bar" data-id="gex"><summary>GEX · OI walls · max pain</summary><div class="body"><p class="note">{ctx["gex_note"]}</p>{ctx["gex_cards"]}<div class="grid"><div>{ctx["gex_svg"]}</div><div>{ctx["oi_svg"]}</div></div>{ctx["gex_table"]}</div></details>
  <details class="bar" data-id="term"><summary>ATM term structure</summary><div class="body">{ctx["term_svg"]}</div></details>
  <details class="bar" data-id="fwd"><summary>Parity forward vs DTE</summary><div class="body">{ctx["fwd_svg"]}</div></details>
  <details class="bar" data-id="rr"><summary>Risk reversal 10Δ / 25Δ · cost</summary><div class="body">{ctx["rr_svg"]}</div></details>
  <details class="bar" data-id="bf"><summary>Butterflies 10Δ / 25Δ · cost</summary><div class="body">{ctx["bf_svg"]}</div></details>
  <details class="bar" data-id="struct"><summary>Structure screen · 30d fly / RR</summary><div class="body">{ctx["struct"]}</div></details>
  <details class="bar" data-id="rank"><summary>30d ATM IV rank</summary><div class="body">{ctx["rank_svg"]}</div></details>
  <details class="bar" data-id="rv"><summary>SPX path / realized vol</summary><div class="body">{ctx["rv_svg"]}</div></details>
  <details class="bar" data-id="spy1"><summary>SPY 1-minute</summary><div class="body">{ctx["spy1"]}</div></details>
  <details class="bar" data-id="vrp"><summary>Research VRP</summary><div class="body">{ctx["vrp_svg"]}</div></details>
  <details class="bar" data-id="xs"><summary>Cross-root 30d ATM</summary><div class="body">{ctx["xs_svg"]}{ctx["xs_table"]}</div></details>
  <details class="bar" data-id="smile"><summary>Smile · bid/ask IV · quad-k / SVI</summary><div class="body">{ctx["smile_svg"]}{ctx["fit_svg"]}<p class="note">{ctx["smile_note"]}</p></div></details>
  <details class="bar" data-id="research"><summary>Research · SVI, RV forecast, events</summary><div class="body">{ctx["research"]}</div></details>
  <details class="bar" data-id="watch"><summary>ATM / 10Δ / 25Δ watchlist</summary><div class="body">{ctx["watch_table"]}</div></details>
  <details class="bar" data-id="termtable"><summary>ATM term + forward variance table</summary><div class="body"><p class="note">Forward variance between T1 and T2 is [σ²(T2)T2 − σ²(T1)T1] / (T2 − T1), T in years. A drop in total variance σ²T is a calendar inversion (flagged).</p>{ctx["term_table"]}</div></details>
  <details class="bar" data-id="fv"><summary>Forward variance strips</summary><div class="body">{ctx["fv_table"]}</div></details>
  <details class="bar" data-id="crossed"><summary>Crossed quotes (quarantine)</summary><div class="body">{ctx["crossed_table"]}</div></details>
  <details class="bar" data-id="flat"><summary>Selected expiry chain (flat list)</summary><div class="body">{ctx["chain_table"]}</div></details>
  <details class="bar" data-id="registry"><summary>Registry — covered vs not</summary><div class="body"><p class="note">{ctx["registry_note"]}</p></div></details>
</main>
<script>
(function () {{
  const key = function (id) {{ return "surf09-open-" + id; }};
  const bars = document.querySelectorAll("details.bar");
  bars.forEach(function (d) {{
    const saved = sessionStorage.getItem(key(d.dataset.id));
    if (saved === "1") d.open = true;
    if (saved === "0") d.open = false;
    d.addEventListener("toggle", function () {{
      sessionStorage.setItem(key(d.dataset.id), d.open ? "1" : "0");
    }});
  }});
  function setAll(open) {{
    bars.forEach(function (d) {{
      d.open = open;
      sessionStorage.setItem(key(d.dataset.id), open ? "1" : "0");
    }});
  }}
  document.getElementById("open-all").onclick = function () {{ setAll(true); }};
  document.getElementById("close-all").onclick = function () {{ setAll(false); }};
  document.querySelectorAll("svg.chart[data-crosshair]").forEach(function (svg) {{
    let pts = [];
    try {{ pts = JSON.parse(svg.dataset.pts || "[]"); }} catch (e) {{ return; }}
    const layer = svg.querySelector(".xh");
    const vline = svg.querySelector(".xh-v");
    const hline = svg.querySelector(".xh-h");
    const dot = svg.querySelector(".xh-dot");
    const lab = svg.querySelector(".xh-lab");
    const hit = svg.querySelector(".xh-hit");
    if (!pts.length || !layer || !hit) return;
    const xy = svg.dataset.snap === "xy";
    function nearest(x, y) {{
      let best = pts[0];
      let bestD = xy
        ? (pts[0].sx - x) * (pts[0].sx - x) + (pts[0].sy - y) * (pts[0].sy - y)
        : Math.abs(pts[0].sx - x);
      for (let i = 1; i < pts.length; i++) {{
        const d = xy
          ? (pts[i].sx - x) * (pts[i].sx - x) + (pts[i].sy - y) * (pts[i].sy - y)
          : Math.abs(pts[i].sx - x);
        if (d < bestD) {{ best = pts[i]; bestD = d; }}
      }}
      return best;
    }}
    hit.addEventListener("mousemove", function (ev) {{
      const ctm = svg.getScreenCTM();
      if (!ctm) return;
      const p = svg.createSVGPoint();
      p.x = ev.clientX;
      p.y = ev.clientY;
      const loc = p.matrixTransform(ctm.inverse());
      const bar = nearest(loc.x, loc.y);
      vline.setAttribute("x1", bar.sx);
      vline.setAttribute("x2", bar.sx);
      hline.setAttribute("y1", bar.sy);
      hline.setAttribute("y2", bar.sy);
      dot.setAttribute("cx", bar.sx);
      dot.setAttribute("cy", bar.sy);
      const right = hit.x.baseVal.value + hit.width.baseVal.value;
      const top = hit.y.baseVal.value;
      let lx = bar.sx + 8;
      let ly = bar.sy - 8;
      if (lx + 8 > right) {{
        lab.setAttribute("text-anchor", "end");
        lx = bar.sx - 8;
      }} else {{
        lab.setAttribute("text-anchor", "start");
      }}
      if (ly < top + 12) ly = bar.sy + 14;
      lab.setAttribute("x", lx);
      lab.setAttribute("y", ly);
      lab.textContent = bar.t || "";
      layer.setAttribute("visibility", "visible");
    }});
    hit.addEventListener("mouseleave", function () {{
      layer.setAttribute("visibility", "hidden");
    }});
  }});
  document.getElementById("refresh-cache").onclick = function () {{
    const btn = this;
    const status = document.getElementById("refresh-status");
    btn.disabled = true;
    status.textContent = "Rebuilding cache from disk… about a minute.";
    fetch("/api/refresh").then(function (r) {{ return r.json(); }}).then(function (body) {{
      if (!body.ok) throw new Error(body.error || "refresh failed");
      location.reload();
    }}).catch(function (err) {{
      status.textContent = String(err);
      btn.disabled = false;
    }});
  }};
}})();
</script>
</body>
</html>
"""


_MACRO_AUTH: tuple[str, str, str, str] | None = None
_MACRO_AUTH_READY = False


def _macro_auth() -> tuple[str, str, str, str] | None:
    """MySQL login for the Macro database. Env wins; otherwise LiveTrading/settings.py."""
    global _MACRO_AUTH, _MACRO_AUTH_READY
    if _MACRO_AUTH_READY:
        return _MACRO_AUTH
    _MACRO_AUTH_READY = True
    host = os.getenv("MACRO_DB_HOST", "")
    user = os.getenv("MACRO_DB_USER", "")
    password = os.getenv("MACRO_DB_PASSWORD", "")
    database = os.getenv("MACRO_DB_NAME", "Macro")
    if not (host and user and password):
        path = Path(os.getenv("MACRO_SETTINGS", r"W:\Workspace\LiveTrading\settings.py"))
        if path.is_file():
            parent = str(path.parent)
            inserted = parent not in sys.path
            if inserted:
                sys.path.insert(0, parent)
            try:
                spec = importlib.util.spec_from_file_location("_livetrading_settings", path)
                if spec is not None and spec.loader is not None:
                    mod = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(mod)
                    host = host or str(getattr(mod, "MYSQL_HOST", "") or "")
                    user = user or str(getattr(mod, "MYSQL_USER", "") or "")
                    password = password or str(getattr(mod, "MYSQL_PASSWORD", "") or "")
            except Exception:
                host = user = password = ""
            finally:
                if inserted and parent in sys.path:
                    sys.path.remove(parent)
    if host and user and password:
        _MACRO_AUTH = (host, user, password, database)
    return _MACRO_AUTH


def _macro_flags(unscheduled, forecast, confcall) -> str:
    bits = []
    if int(forecast or 0):
        bits.append("SEP")
    if int(unscheduled or 0):
        bits.append("unscheduled")
    if int(confcall or 0):
        bits.append("conf call")
    return ", ".join(bits) if bits else "—"


def _macro_panel(session: date | None) -> str:
    """FOMC statement days and CPI reference months from Macro, as of this session."""
    global _FOMC_DATES
    asof = session or date.today()
    auth = _macro_auth()
    if auth is None:
        return '<p class="note warn">Macro database is not configured.</p>'
    host, user, password, database = auth
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database,
            connection_timeout=3,
        )
    except Exception as exc:
        return (
            '<p class="note warn">Macro database unavailable ('
            f'{html.escape(type(exc).__name__)}).</p>'
        )
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT meeting_date, unscheduled, forecast, confcall FROM fomc_meetings "
            "WHERE meeting_date BETWEEN %s AND %s ORDER BY meeting_date",
            (asof - timedelta(days=200), asof + timedelta(days=420)),
        )
        meetings = [(row[0], row[1], row[2], row[3]) for row in cur.fetchall()]
        cur.execute(
            "SELECT series_id, date, value FROM observations "
            "WHERE series_id IN ('CPIAUCNS', 'CPALTT01USM659N', 'FEDFUNDS') "
            "AND date <= %s AND date >= %s ORDER BY date",
            (asof, asof - timedelta(days=800)),
        )
        obs = [(row[0], row[1], float(row[2])) for row in cur.fetchall() if row[2] is not None]
        try:
            cur.execute("SELECT meeting_date FROM fomc_meetings")
            found: set[date] = set()
            for (day,) in cur.fetchall():
                if isinstance(day, datetime):
                    day = day.date()
                if isinstance(day, date):
                    found.add(day)
            if found:
                _FOMC_DATES = found
        except Exception:
            pass
    except Exception as exc:
        return (
            '<p class="note warn">Macro query failed ('
            f'{html.escape(type(exc).__name__)}).</p>'
        )
    finally:
        conn.close()

    def latest(series_id: str):
        rows = [item for item in obs if item[0] == series_id]
        return rows[-1] if rows else None

    prev = [row for row in meetings if row[0] < asof]
    nxt = [row for row in meetings if row[0] >= asof]
    last_m = prev[-1] if prev else None
    next_m = nxt[0] if nxt else None

    def meeting_card(row, label: str) -> tuple[str, str]:
        if row is None:
            return label, "—"
        day, uns, forecast, call = row
        delta = (day - asof).days
        when = "today" if delta == 0 else (f"in {delta}d" if delta > 0 else f"{-delta}d ago")
        return label, f"{day.isoformat()} · {when} · {_macro_flags(uns, forecast, call)}"

    cpi = latest("CPIAUCNS")
    yoy = latest("CPALTT01USM659N")
    funds = latest("FEDFUNDS")
    cards = [
        meeting_card(next_m, "Next FOMC"),
        meeting_card(last_m, "Last FOMC"),
        (
            "CPI index (NSA)",
            "—" if cpi is None else f"{cpi[2]:,.3f} · {cpi[1].strftime('%Y-%m')}",
        ),
        (
            "CPI YoY",
            "—" if yoy is None else f"{yoy[2]:.2f}% · {yoy[1].strftime('%Y-%m')}",
        ),
        (
            "Fed funds",
            "—" if funds is None else f"{funds[2]:.2f}% · {funds[1].strftime('%Y-%m')}",
        ),
    ]
    meet_rows = []
    meet_cls = []
    for day, uns, forecast, call in meetings:
        delta = (day - asof).days
        rel = "0" if delta == 0 else (f"+{delta}" if delta > 0 else str(delta))
        meet_rows.append([
            day.isoformat(),
            rel,
            _macro_flags(uns, forecast, call),
        ])
        meet_cls.append("atm" if next_m is not None and day == next_m[0] else "")
    yoy_by_month = {item[1]: item[2] for item in obs if item[0] == "CPALTT01USM659N"}
    cpi_rows = []
    for _sid, day, value in obs:
        if _sid != "CPIAUCNS":
            continue
        y = yoy_by_month.get(day)
        cpi_rows.append([
            day.strftime("%Y-%m"),
            f"{value:,.3f}",
            "—" if y is None else f"{y:.2f}%",
        ])
    cpi_rows = cpi_rows[-12:]
    stale = ""
    if cpi is not None and yoy is not None and (cpi[1].year, cpi[1].month) != (yoy[1].year, yoy[1].month):
        stale = (
            f" CPI YoY in Macro stops at {yoy[1].strftime('%Y-%m')} while the index "
            f"continues through {cpi[1].strftime('%Y-%m')}."
        )
    note = (
        "Read from the Macro database. FOMC dates are the statement day "
        "(the second day when the meeting spans two). SEP marks a projections meeting. "
        "CPI and fed funds dates are the FRED reference month, the 1st, not the morning "
        "the number is released. Values are the latest observation on or before this session."
        + stale
    )
    return (
        f'<p class="note">{note}</p>'
        + _cards(cards)
        + '<div class="grid"><div><h2>FOMC</h2>'
        + _table(["Statement day", "Days vs session", "Flags"], meet_rows, meet_cls)
        + '</div><div><h2>CPI</h2>'
        + _table(["Reference month", "Index (NSA)", "YoY"], cpi_rows)
        + "</div></div>"
    )


def _cards(items: list[tuple[str, str]]) -> str:
    bits = []
    for label, value in items:
        bits.append(f'<div class="card"><b>{html.escape(value)}</b><span>{html.escape(label)}</span></div>')
    return '<div class="cards">' + "".join(bits) + "</div>"


def _table(headers: list[str], rows: list[list[str]], classes: list[str] | None = None) -> str:
    if not rows:
        return '<p class="note warn">No rows.</p>'
    head = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body = []
    for i, row in enumerate(rows):
        cls = ""
        if classes and i < len(classes) and classes[i]:
            cls = f' class="{html.escape(classes[i])}"'
        body.append(f"<tr{cls}>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>")
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
    stale_sec = float(params.get("stale") or DEFAULT_STALE_SEC)
    stamp = params.get("stamp") or "latest"
    live_stamps = _list_live_stamps(data_dir, session) if source == "live" and session else []
    if stamp != "latest" and stamp not in live_stamps:
        stamp = "latest"

    asof = None
    raw = pl.DataFrame(schema=_OUTPUT_SCHEMA)
    if session is not None:
        if source == "live":
            raw, asof = _read_live(data_dir, session, None if stamp == "latest" else stamp)
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
    dups = _dup_count(raw)
    bounds = _price_bound_count(raw)
    crossed_rows: list[list[str]] = []
    if not raw.is_empty() and crossed:
        cq = raw.filter(
            pl.col("bid").is_not_null() & pl.col("ask").is_not_null() & (pl.col("bid") > pl.col("ask"))
        ).head(40)
        for row in cq.iter_rows(named=True):
            crossed_rows.append([
                html.escape(str(row["symbol"])),
                html.escape(_iso(row["expiration"]) if hasattr(row["expiration"], "isoformat") else str(row["expiration"])),
                html.escape(str(row["right"])),
                _fmt(row["strike"], 0),
                _fmt(row["bid"], 2),
                _fmt(row["ask"], 2),
                _fmt(row.get("bid_size"), 0),
                _fmt(row.get("ask_size"), 0),
            ])
    liq = _filter(raw, max_spread=spread, min_size=min_size, dte_min=dte_min)
    stale_dropped = 0
    if source == "live" and stale_sec > 0:
        before = liq.height
        liq = _stale_mask(liq, stale_sec)
        stale_dropped = before - liq.height
    convexity_breaks = _butterfly_all(liq)
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
    ndx_rv = _rv(data_dir, session, 20, "NDX") if session else None
    park20, gk20 = _range_rv(data_dir, session, 20) if session else (None, None)
    on20, rth20 = _session_rv(data_dir, session, 20) if session else (None, None)
    vrp = None
    if n30 and rv20 is not None:
        vrp = n30["atm"] ** 2 - rv20 ** 2
    vrp_hist = _vrp_series(hist_upto, data_dir) if hist_upto else []

    expiries = [(_iso(r["expiration"]), f"{r['expiration']}  dte {r['dte']}") for r in term]
    if not expiries and not raw.is_empty() and "expiration" in raw.columns:
        for exp in sorted(raw["expiration"].drop_nulls().unique().to_list()):
            dte = int(raw.filter(pl.col("expiration") == exp)["dte"][0]) if "dte" in raw.columns else 0
            expiries.append((_iso(exp), f"{exp}  dte {dte}"))
    expiry = _parse_date(params.get("expiry"))
    expiry_set = {r["expiration"] for r in term} or {_parse_date(e) for e, _ in expiries}
    if expiry not in expiry_set and expiries:
        if term:
            expiry = min(term, key=lambda r: abs(r["dte"] - 30))["expiration"]
        else:
            expiry = _parse_date(expiries[0][0])

    smile_calls: list[tuple[float, float]] = []
    smile_puts: list[tuple[float, float]] = []
    smile_bid: list[tuple[float, float]] = []
    smile_ask: list[tuple[float, float]] = []
    chain_rows = []
    fit = None
    svi = None
    slice_bfly = 0
    if expiry is not None and not liq.is_empty():
        slice_ = _add_side_iv(liq.filter(pl.col("expiration") == expiry).sort("strike"))
        slice_bfly = _butterfly_all(slice_)
        try:
            fit = _fit_quad_k(slice_)
        except Exception:
            fit = None
        try:
            svi = _fit_svi(slice_)
        except Exception:
            svi = None
        fwd = float(slice_["forward"][0]) if slice_.height else None
        if fwd:
            for row in slice_.iter_rows(named=True):
                kf = float(row["strike"]) / fwd
                if not (0.85 <= kf <= 1.15):
                    continue
                if row["right"] == "C" and row["iv"] is not None:
                    smile_calls.append((kf, float(row["iv"])))
                if row["right"] == "P" and row["iv"] is not None:
                    smile_puts.append((kf, float(row["iv"])))
                if row.get("iv_bid") is not None:
                    smile_bid.append((kf, float(row["iv_bid"])))
                if row.get("iv_ask") is not None:
                    smile_ask.append((kf, float(row["iv_ask"])))
        shown = slice_.filter((pl.col("strike") / fwd).is_between(0.85, 1.15)) if fwd else slice_
        for row in shown.head(120).iter_rows(named=True):
            chain_rows.append([
                html.escape(str(row["symbol"])),
                html.escape(row["right"]),
                _fmt(row["strike"], 0),
                _fmt(row["bid"], 2),
                _fmt(row["ask"], 2),
                _fmt(row["iv"], 2, pct=True),
                _fmt(row.get("iv_bid"), 2, pct=True),
                _fmt(row.get("iv_ask"), 2, pct=True),
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
    source_opts = _options([("eod", "EOD lake"), ("live", "Live snapshot (PIT)")], source)
    expiry_opts = _options(expiries or [("", "none")], _iso(expiry))
    if source == "live" and live_stamps:
        stamp_opts = _options(
            [("latest", f"latest ({_stamp_label(live_stamps[-1])})")]
            + [(s, _stamp_label(s)) for s in live_stamps],
            stamp,
        )
    else:
        stamp_opts = _options([("latest", "EOD close")], "latest")

    pairs = _pair_expiry(raw, expiry)
    tools = _summarize_tools(pairs, expiry=expiry, spot=spot, band=0)
    pair_fwd = tools["forward"]
    atm_k = tools["atm_strike"] or (n30.get("forward") if n30 else None)
    pain_k, pain_val = tools["max_pain"], tools["max_pain_contract_points"]
    flip_k = tools["gex_flip"]
    tot_ce_oi, tot_pe_oi = tools["call_oi"], tools["put_oi"]
    tot_ce_gex, tot_pe_gex = tools["call_gex"], tools["put_gex"]
    tot_net_gex = tools["net_gex"]
    pcr = tools["pcr_oi"]
    tot_net_gex_usd = tools["net_gex_times_forward"]
    call_wall, put_wall = tools["call_oi_wall"], tools["put_oi_wall"]

    pair_rows = []
    pair_cls = []
    for p in pairs:
        c, u = p["c"], p["p"]
        pair_rows.append([
            _fmt(p["strike"], 0),
            _fmt(None if c is None else c.get("bid"), 2),
            _fmt(None if c is None else c.get("ask"), 2),
            _fmt(None if c is None else c.get("iv"), 2, pct=True),
            _fmt(None if c is None else c.get("delta"), 3),
            _fmt(None if c is None else c.get("gamma"), 5),
            _fmt(None if c is None else c.get("vega"), 2),
            _fmt(None if c is None else c.get("theta"), 2),
            _fmt(p["ce_oi"], 0),
            _fmt(None if c is None else c.get("volume"), 0),
            _fmt(p["ce_gex"], 0),
            _fmt(None if u is None else u.get("bid"), 2),
            _fmt(None if u is None else u.get("ask"), 2),
            _fmt(None if u is None else u.get("iv"), 2, pct=True),
            _fmt(None if u is None else u.get("delta"), 3),
            _fmt(None if u is None else u.get("gamma"), 5),
            _fmt(None if u is None else u.get("vega"), 2),
            _fmt(None if u is None else u.get("theta"), 2),
            _fmt(p["pe_oi"], 0),
            _fmt(None if u is None else u.get("volume"), 0),
            _fmt(p["pe_gex"], 0),
            _fmt(p["net_gex"], 0),
        ])
        pair_cls.append("atm" if atm_k is not None and abs(p["strike"] - atm_k) < 1e-6 else "")
    pair_table = _table(
        ["K", "C bid", "C ask", "C IV", "C Δ", "C Γ", "C vega", "C θ", "C OI", "C vol", "C GEX",
         "P bid", "P ask", "P IV", "P Δ", "P Γ", "P vega", "P θ", "P OI", "P vol", "P GEX", "Net GEX"],
        pair_rows, pair_cls,
    )
    if not pair_rows:
        pair_table = '<p class="note warn">No contracts for this expiry in the selected snapshot.</p>'

    band = [(p["strike"], p["net_gex"]) for p in pairs]
    if pair_fwd:
        band = [(k, v) for k, v in band if pair_fwd * 0.85 <= k <= pair_fwd * 1.15] or band
    gex_svg = _svg_bars(
        band,
        title=f"Net GEX by strike  {_iso(expiry)}  (call − put)",
        xlabel="Strike", ylabel="γ × OI × 100",
    ) or '<p class="note">No GEX — need gamma and OI on this expiry.</p>'
    oi_svg = _svg_bars(
        [(p["strike"], p["ce_oi"] - p["pe_oi"]) for p in pairs if pair_fwd is None or pair_fwd * 0.85 <= p["strike"] <= pair_fwd * 1.15]
        or [(p["strike"], p["ce_oi"] - p["pe_oi"]) for p in pairs],
        title=f"OI imbalance  call − put  {_iso(expiry)}",
        xlabel="Strike", ylabel="contracts",
    ) or ""
    walls = sorted(pairs, key=lambda r: r["ce_oi"] + r["pe_oi"], reverse=True)[:8]
    gex_table = _table(
        ["Strike", "Call OI", "Put OI", "Call GEX", "Put GEX", "Net GEX"],
        [[
            _fmt(p["strike"], 0),
            _fmt(p["ce_oi"], 0),
            _fmt(p["pe_oi"], 0),
            _fmt(p["ce_gex"], 0),
            _fmt(p["pe_gex"], 0),
            _fmt(p["net_gex"], 0),
        ] for p in walls],
    )
    chain_cards = _cards([
        ("Paired strikes", f"{len(pairs):,}"),
        ("ATM / forward K", _fmt(atm_k, 0)),
        ("Parity forward", _fmt(pair_fwd, 2)),
        ("Call OI", _fmt(tot_ce_oi, 0)),
        ("Put OI", _fmt(tot_pe_oi, 0)),
        ("PCR (OI)", "—" if pcr is None else f"{pcr:.2f}"),
    ])
    gex_cards = _cards([
        ("Call GEX", _fmt(tot_ce_gex, 0)),
        ("Put GEX", _fmt(tot_pe_gex, 0)),
        ("Net GEX (C−P)", _fmt(tot_net_gex, 0)),
        ("Net GEX × F", _fmt(tot_net_gex_usd, 0)),
        ("Call OI wall", _fmt(call_wall, 0)),
        ("Put OI wall", _fmt(put_wall, 0)),
        ("GEX flip", _fmt(flip_k, 0)),
        ("Max pain", _fmt(pain_k, 0)),
    ])
    pit = "EOD close" if source == "eod" else (
        f"live {_stamp_label(live_stamps[-1]) if live_stamps else 'latest'}"
        if stamp == "latest" else f"live {_stamp_label(stamp)}"
    )
    chain_note = (
        f"Paired CE/PE grid on the selected expiry from this {pit} snapshot. "
        "IV and greeks are the lake Black-76 values (mid). Highest OI wins when SPX and SPXW share a strike. "
        "Highlighted row is the strike nearest the parity forward."
    )
    gex_note = (
        "GEX = γ × OI × 100 (SPX multiplier), same contract units as OpenAlgo. "
        "Net GEX = call GEX − put GEX. Net GEX × F is a $1-spot-move scale, not a P&amp;L. "
        "This is a descriptive positioning proxy from listed OI — not a dealer inventory model (SIG-06). "
        f"Max pain minimizes Σ intrinsic×OI if settlement prints at that strike"
        + (f" ({_fmt(pain_val, 0)} contract-points)." if pain_val is not None else ".")
    )

    asof_txt = ""
    if asof is not None:
        local = asof.astimezone(NY) if asof.tzinfo else asof.replace(tzinfo=NY)
        asof_txt = f" · as of {local:%Y-%m-%d %H:%M ET}"
    roots_present = [] if raw.is_empty() else sorted(set(raw["symbol"].to_list()))
    caption = (
        f"{'Live snapshot' if source=='live' else 'ThetaData EOD'} · "
        f"{_iso(session) or 'no session'}{asof_txt}"
        f"{'' if source != 'live' or stamp == 'latest' else ' · PIT ' + _stamp_label(stamp)} · "
        f"roots {', '.join(roots_present) or '—'} · "
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
        ("Call convexity breaks", str(convexity_breaks)),
        ("20d close RV", _fmt(rv20, 2, pct=True)),
        ("20d Parkinson", _fmt(park20, 2, pct=True)),
        ("20d Garman–Klass", _fmt(gk20, 2, pct=True)),
        ("20d overnight RV", _fmt(on20, 2, pct=True)),
        ("20d RTH RV", _fmt(rth20, 2, pct=True)),
        ("ATM² − RV20² (now)", _fmt(vrp, 4)),
        ("Σ γ·OI·100", _fmt(gamma_oi, 0)),
        ("SPX close", _fmt(spot, 2)),
        ("60d close RV", _fmt(rv60, 2, pct=True)),
    ])

    overlays = []
    for sym, label in (("ES", "ES (from 2023-06-20)"), ("NQ", "NQ (from 2024-03-18)"),
                       ("VX", "VX all-hours (from 2025-02-05)"), ("VIX", "VIX spot (IBKR)")):
        info = _bar_asof(data_dir, session, sym) if session else None
        if info and info["close"] is not None:
            lag = "" if info["session"] == session else f" as of {info['session']}"
            overlays.append((label, f"{info['close']:.2f}{lag}"))
        else:
            overlays.append((label, "—"))
    overlay = _cards(overlays)
    macro = _macro_panel(session)

    missing = _missing_eod(data_dir, eod_days, session)
    health = _cards(_health_items(
        data_dir, eod_days=eod_days, live_days=live_days, session=session,
        source=source, raw=raw, crossed=crossed, dups=dups, bounds=bounds,
        stale_dropped=stale_dropped, missing=missing,
    ))

    filter_note = (
        f"Liquidity filter: bid&gt;0, ask&gt;bid, spread/mid ≤ {spread:.0%}, "
        f"size ≥ {min_size}, DTE ≥ {dte_min}"
        f"{f', quote age ≤ {stale_sec:.0f}s (live)' if source == 'live' else ''}."
        " IV rank / RR z-score use EOD ATM~30d from 2024-01-02 through this session. "
        "Parkinson/GK and overnight/RTH use IBKR SPX OHLC, 252-scaled. "
        "ATM²−RV20² (now) uses trailing RV; the VRP chart uses subsequent 20 sessions "
        "(research label, not a forecast). Bid/ask IV is re-solved here, not stored in the lake. "
        "quad-k v1 is a weighted quadratic in log-moneyness. svi-raw v1 is Gatheral raw SVI "
        "on the same OTM points, with a g(k) density check and no calendar-arbitrage constraint. "
        "CPI / FOMC come from the Macro database, as of this session. Live 09-07 is not listed."
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
    def _px_series(key: str, max_dte: int = 120) -> tuple[list[float], list[float]]:
        pts = [(r["dte"], r[key]) for r in term
               if r.get(key) is not None and r["dte"] <= max_dte]
        return [d for d, _ in pts], [v for _, v in pts]

    rr25_x, rr25_y = _px_series("rr25_px")
    rr10_x, rr10_y = _px_series("rr10_px")
    bf25_x, bf25_y = _px_series("bf25_px")
    bf10_x, bf10_y = _px_series("bf10_px")
    str_x, str_y = _px_series("straddle_px")
    rr_svg = _svg_lines(
        [("25Δ buy put / sell call", rr25_x, rr25_y), ("10Δ buy put / sell call", rr10_x, rr10_y)],
        title="Risk reversal cost (put mid − call mid)",
        xlabel="DTE (calendar days)", ylabel="index points",
    )
    rr_svg += (
        '<p class="note">Index points of buying the put and selling the call at the same delta. '
        "Positive means the put is the more expensive wing. ×100 = USD per SPX contract. "
        "Shown through 120 DTE. Vol points and longer expiries stay in the term table.</p>"
    )
    bf_svg = _svg_lines(
        [("25Δ fly", bf25_x, bf25_y), ("10Δ fly", bf10_x, bf10_y)],
        title="Butterfly cost (buy wings, sell ATM straddle)",
        xlabel="DTE (calendar days)", ylabel="index points",
    )
    bf_svg += _svg_lines(
        [("ATM straddle", str_x, str_y)],
        title="ATM straddle cost (50Δ call mid + 50Δ put mid)",
        xlabel="DTE (calendar days)", ylabel="index points",
    )
    bf_svg += (
        '<p class="note">Fly price is the wing strangle minus the ATM straddle, in index points. '
        "It is usually a credit (negative): you are paid to own the wings against the body. "
        "A less negative fly is richer — the wings cost more relative to the straddle. "
        "The straddle chart is the body on its own, so a low number is a cheap straddle in premium, "
        "which is not the same thing as a low IV. ×100 = USD per SPX contract. "
        "Shown through 120 DTE so a two-year straddle does not flatten the front. "
        "Vol butterflies and every expiry's point cost stay in the term table.</p>"
    )
    struct = _structure_panel(hist_upto)
    rank_svg = _svg_lines(
        [("30d ATM EOD", [h["session"].toordinal() for h in hist_upto[-252:]],
          [h["atm"] for h in hist_upto[-252:]])]
        + ([("This session", [session.toordinal()], [n30["atm"]])] if session and n30 else []),
        title="30d ATM IV history (EOD, last 252 sessions)",
        xlabel="Session", ylabel="IV", y_pct=True, x_scale="date",
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
                xlabel="Session", ylabel="SPX", x_scale="date",
            )
    spy1 = _spy1_panel(session)

    smile_svg = _svg_lines(
        [
            ("Call mid IV", [x for x, _ in smile_calls], [y for _, y in smile_calls]),
            ("Put mid IV", [x for x, _ in smile_puts], [y for _, y in smile_puts]),
            ("Bid IV", [x for x, _ in smile_bid], [y for _, y in smile_bid]),
            ("Ask IV", [x for x, _ in smile_ask], [y for _, y in smile_ask]),
        ],
        title=f"Smile  {_iso(expiry)}  bid / mid / ask IV vs K/F",
        xlabel="Strike / forward", ylabel="IV", y_pct=True,
    )
    fit_svg = ""
    smile_note = (
        "Smile is liquidity-filtered. RR = IV(put Δ) − IV(call Δ). "
        "BF = ½(IV call + IV put) − ATM. Positive RR means puts are richer. "
        f"Three-strike call convexity breaks on this expiry: {slice_bfly}; "
        f"across all kept expiries: {convexity_breaks}."
    )
    if fit:
        smile_note += (
            f" quad-k v1 a={fit['a']:.5f} b={fit['b']:.5f} c={fit['c']:.5f}, "
            f"RMSE {fit['rmse']*100:.2f} vol pts on {fit['n']} OTM points."
        )
    if svi:
        smile_note += (
            f" svi-raw v1 a={svi['a']:.4f} b={svi['b']:.4f} ρ={svi['rho']:.3f} "
            f"m={svi['m']:.3f} σ={svi['sigma']:.3f}, RMSE {svi['rmse']*100:.2f} vol pts, "
            f"g(k)<0 on {svi['g_breaks']}/41. Calendar arbitrage is not constrained."
        )
    base = fit or svi
    if base:
        series = [("OTM IV", list(range(len(base["iv"]))), base["iv"])]
        if fit:
            series.append(("quad-k v1", list(range(len(fit["fit"]))), fit["fit"]))
        if svi and (fit is None or len(svi["fit"]) == len(fit["fit"])):
            series.append(("svi-raw v1", list(range(len(svi["fit"]))), svi["fit"]))
        qrmse = "—" if fit is None else f"{fit['rmse']*100:.2f}"
        srmse = "—" if svi is None else f"{svi['rmse']*100:.2f}"
        fit_svg = _svg_lines(
            series,
            title=f"Surface fit  quad-k RMSE {qrmse}  ·  svi-raw v1 RMSE {srmse} vol pts",
            xlabel="OTM point (sorted by k)", ylabel="IV", y_pct=True,
        )
        resid = []
        if fit:
            resid.append(("IV − quad-k", list(range(len(fit["resid"]))), fit["resid"]))
        if svi and (fit is None or len(svi["resid"]) == len(fit["resid"])):
            resid.append(("IV − svi", list(range(len(svi["resid"]))), svi["resid"]))
        fit_svg += _svg_lines(
            resid,
            title="Surface residual (observed − fitted)",
            xlabel="OTM point", ylabel="vol (decimal)",
        )

    vrp_svg = ""
    if vrp_hist:
        vrp_svg = _svg_lines(
            [("ATM² − next 20d RV²", [d.toordinal() for d, _ in vrp_hist[-252:]],
              [v for _, v in vrp_hist[-252:]])],
            title="Research VRP (subsequent realized, not a forecast)",
            xlabel="Session", ylabel="variance gap", x_scale="date",
        )
    elif vrp is not None:
        vrp_svg = '<p class="note">Need 20 sessions after this date for subsequent VRP. Trailing ATM²−RV20² is in the cards.</p>'

    xs = _cross_atm(data_dir, session) if session else []
    xs_svg = _svg_lines(
        [("30d ATM", list(range(len(xs))), [r["atm"] for r in xs])],
        title="Cross-root 30d ATM (EOD, same session)",
        xlabel="Root rank (cheap → rich)", ylabel="IV", y_pct=True,
    ) if xs else '<p class="note">No other-root EOD files for this session.</p>'
    xs_table = _table(
        ["Root", "DTE", "30d ATM", "RR25 vol pts"],
        [[
            html.escape(r["root"]),
            str(r["dte"]),
            _fmt(r["atm"], 2, pct=True),
            "—" if r["rr25"] is None else f"{r['rr25']*100:.2f}",
        ] for r in xs],
    )
    spx_atm = next((r["atm"] for r in xs if r["root"] in ("SPX", "SPXW")), None)
    ndx_atm = next((r["atm"] for r in xs if r["root"] == "NDX"), None)
    spy_atm = next((r["atm"] for r in xs if r["root"] == "SPY"), None)
    qqq_atm = next((r["atm"] for r in xs if r["root"] == "QQQ"), None)
    rv_spread = None
    if rv20 is not None and ndx_rv is not None:
        rv_spread = ndx_rv - rv20
    iv_spread = (ndx_atm - spx_atm) if (ndx_atm is not None and spx_atm is not None) else None
    etf_spread = (qqq_atm - spy_atm) if (qqq_atm is not None and spy_atm is not None) else None
    watch_table = _table(
        ["Metric", "Value", "Note"],
        [
            ["30d ATM mid", _fmt(None if n30 is None else n30["atm"], 2, pct=True), "SPX+SPXW, Δ50"],
            ["30d RR25", "—" if n30 is None or n30["rr25"] is None else f"{n30['rr25']*100:.2f} vol pts", "put IV − call IV"],
            ["30d RR10", "—" if n30 is None or n30["rr10"] is None else f"{n30['rr10']*100:.2f} vol pts", "put IV − call IV"],
            ["30d BF25", "—" if n30 is None or n30["bf25"] is None else f"{n30['bf25']*100:.2f} vol pts", "½(C+P)−ATM, vol"],
            ["30d straddle", _fmt(None if n30 is None else n30.get("straddle_px"), 2), "points, ×100 = USD"],
            ["30d BF25 cost", _fmt(None if n30 is None else n30.get("bf25_px"), 2), "wings − straddle, points"],
            ["30d BF10 cost", _fmt(None if n30 is None else n30.get("bf10_px"), 2), "wings − straddle, points"],
            ["30d RR25 cost", _fmt(None if n30 is None else n30.get("rr25_px"), 2), "put − call, points"],
            ["30d RR10 cost", _fmt(None if n30 is None else n30.get("rr10_px"), 2), "put − call, points"],
            ["IV rank", "—" if iv_rank is None else f"{iv_rank:.0%}", "2024+ EOD"],
            ["NDX − SPX ATM", _fmt(iv_spread, 2, pct=True), "same session EOD"],
            ["QQQ − SPY ATM", _fmt(etf_spread, 2, pct=True), "same session EOD"],
            ["NDX − SPX 20d RV", _fmt(rv_spread, 2, pct=True), "close-to-close"],
            ["Fit RMSE", "—" if fit is None else f"{fit['rmse']*100:.2f} vol pts", "quad-k v1"],
            ["SVI RMSE", "—" if svi is None else f"{svi['rmse']*100:.2f} vol pts", "svi-raw v1"],
        ],
    )

    term_table = _table(
        ["Expiry", "DTE", "Forward", "ATM", "Straddle", "RR25 vol", "BF25 vol", "RR25 px", "BF25 px", "σ²T", "N"],
        [[
            html.escape(_iso(r["expiration"])),
            str(r["dte"]),
            _fmt(r["forward"], 2),
            _fmt(r["atm"], 2, pct=True),
            _fmt(r.get("straddle_px"), 2),
            "—" if r["rr25"] is None else f"{r['rr25']*100:.2f}",
            "—" if r["bf25"] is None else f"{r['bf25']*100:.2f}",
            _fmt(r.get("rr25_px"), 2),
            _fmt(r.get("bf25_px"), 2),
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
        ["Root", "Right", "Strike", "Bid", "Ask", "IV", "IV bid", "IV ask", "Δ", "Γ", "Vega", "Θ", "OI", "Sprd/mid"],
        chain_rows,
    )
    if not chain_rows:
        chain_table = '<p class="note warn">No liquidity-filtered rows for this expiry.</p>'
    if not term_svg:
        term_svg = '<p class="note warn">No ATM points after the liquidity filter.</p>'
    crossed_table = _table(
        ["Root", "Expiry", "Right", "Strike", "Bid", "Ask", "Bid sz", "Ask sz"],
        crossed_rows,
    )
    if not crossed_rows:
        crossed_table = '<p class="note">No crossed quotes in this snapshot.</p>'
    forecast = _rv_forecast(data_dir, session) if session else None
    event_rv = _event_variance(data_dir, session) if session else None
    research = _research_section(
        session, term, n30, fit, svi, forecast, event_rv,
        atms, rr25s, iv_rank, rr_z,
    )
    registry_note = (
        "On this page: DATA-08/09/10 (closes), QUAL-01/02/03/04/06/07/09 (flags, dups, "
        "bounds, crossed quarantine, coverage), SURF-01/02/03/04-quad-k+svi-raw-v1/05/06/07/08/09/10/11/12/13/14, "
        "RV-01/02/03/04, RV-05 FOMC vs other days, SIG-01 research VRP + SIG-02 ATM/RV spreads, "
        "SIG-03 10Δ payoff ratio, SIG-04 FOMC gap move, SIG-05 rank/z window check, UI-01/03, "
        "paired chain / GEX / OI / max pain (descriptive). "
        "CPI reference months and FOMC statement dates are read from Macro. "
        "SPY 1-minute regular-hours bars are read from the FMP CSV. "
        "Not started: DATA-12 payrolls/earnings/release clock, DATA-13 SPX ticks and 1-second bars, "
        "DATA-15 bronze/silver (blocks QUAL-08), dealer-GEX as a signal (SIG-06), "
        "SIG-07..10, STRAT, PORT, EXEC, GOV."
    )

    return _page({
        "caption": caption,
        "session_opts": session_opts,
        "source_opts": source_opts,
        "expiry_opts": expiry_opts,
        "stamp_opts": stamp_opts,
        "chain_note": chain_note,
        "chain_cards": chain_cards,
        "pair_table": pair_table,
        "gex_note": gex_note,
        "gex_cards": gex_cards,
        "gex_svg": gex_svg,
        "oi_svg": oi_svg,
        "gex_table": gex_table,
        "spread": f"{spread:.2f}",
        "minsize": str(min_size),
        "dte_min": str(dte_min),
        "stale": f"{stale_sec:.0f}",
        "health": health,
        "quality": quality,
        "overlay": overlay,
        "macro": macro,
        "filter_note": filter_note,
        "term_svg": term_svg,
        "fwd_svg": fwd_svg,
        "rr_svg": rr_svg,
        "bf_svg": bf_svg,
        "struct": struct,
        "rank_svg": rank_svg,
        "rv_svg": rv_svg,
        "spy1": spy1,
        "vrp_svg": vrp_svg,
        "xs_svg": xs_svg,
        "smile_svg": smile_svg or "",
        "fit_svg": fit_svg,
        "smile_note": smile_note,
        "research": research,
        "watch_table": watch_table,
        "xs_table": xs_table,
        "term_table": term_table,
        "fv_table": fv_table,
        "crossed_table": crossed_table,
        "chain_table": chain_table,
        "registry_note": registry_note,
    })


class Handler(BaseHTTPRequestHandler):
    data_dir: Path

    def log_message(self, fmt: str, *args) -> None:
        sys.stderr.write(f"[surf09] {self.address_string()} {fmt % args}\n")

    def _cors(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self._cors()
        self.end_headers()

    def _send_json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload, default=str, allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self._cors()
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        params = {k: v[-1] for k, v in parse_qs(parsed.query).items()}
        path = parsed.path.rstrip("/") or "/"
        if path in ("/api", "/api/help"):
            self._send_json({
                "ok": True,
                "endpoints": {
                    "/api/meta": "sessions, live stamps, expiries",
                    "/api/chain": "paired CE/PE with IV, greeks, OI (same params)",
                    "/api/gex": "GEX totals, walls, flip, max pain + per-strike net",
                    "/api/tools": "chain + gex in one payload",
                    "/api/refresh": "rebuild IV-rank, VRP, and calendar caches from disk",
                },
                "params": {
                    "session": "YYYY-MM-DD (default: latest for source)",
                    "source": "eod | live (default eod)",
                    "stamp": "latest | HHMMSS UTC live filename",
                    "expiry": "YYYY-MM-DD (default: nearest dte)",
                    "dte": "target DTE when expiry omitted (default 30)",
                    "band": "keep strikes within ±band of forward (0 = all)",
                },
            })
            return
        if path.startswith("/api/"):
            try:
                if path == "/api/refresh":
                    self._send_json(refresh_caches(self.data_dir))
                elif path == "/api/meta":
                    self._send_json(api_meta(self.data_dir, params))
                elif path == "/api/tools":
                    self._send_json(api_tools(self.data_dir, params))
                elif path == "/api/chain":
                    payload = api_tools(self.data_dir, params)
                    self._send_json({
                        "ok": True,
                        "source": payload["source"],
                        "session": payload["session"],
                        "stamp": payload["stamp"],
                        "asof": payload["asof"],
                        "expiry": payload["expiry"],
                        "forward": payload["forward"],
                        "atm_strike": payload["atm_strike"],
                        "expiries": payload["expiries"],
                        "chain": payload["chain"],
                    })
                elif path == "/api/gex":
                    payload = api_tools(self.data_dir, params)
                    self._send_json({
                        "ok": True,
                        "source": payload["source"],
                        "session": payload["session"],
                        "stamp": payload["stamp"],
                        "asof": payload["asof"],
                        "expiry": payload["expiry"],
                        "forward": payload["forward"],
                        "call_gex": payload["call_gex"],
                        "put_gex": payload["put_gex"],
                        "net_gex": payload["net_gex"],
                        "net_gex_times_forward": payload["net_gex_times_forward"],
                        "call_oi": payload["call_oi"],
                        "put_oi": payload["put_oi"],
                        "pcr_oi": payload["pcr_oi"],
                        "call_oi_wall": payload["call_oi_wall"],
                        "put_oi_wall": payload["put_oi_wall"],
                        "gex_flip": payload["gex_flip"],
                        "max_pain": payload["max_pain"],
                        "max_pain_contract_points": payload["max_pain_contract_points"],
                        "multiplier": payload["multiplier"],
                        "gex_formula": payload["gex_formula"],
                        "gex_label": payload["gex_label"],
                        "strikes": [
                            {"strike": r["strike"], "call_gex": r["call_gex"],
                             "put_gex": r["put_gex"], "net_gex": r["net_gex"],
                             "call_oi": r["call_oi"], "put_oi": r["put_oi"]}
                            for r in payload["chain"]
                        ],
                    })
                else:
                    self._send_json({"ok": False, "error": f"unknown endpoint {path}"}, 404)
            except Exception as exc:  # noqa: BLE001
                self._send_json({"ok": False, "error": repr(exc)}, 500)
            return
        if path not in ("/", "/index.html"):
            self.send_error(404)
            return
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
    print(f"[surf09] api  http://{args.host}:{args.port}/api  "
          f"/api/meta /api/chain /api/gex /api/tools /api/refresh", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("[surf09] stopped", flush=True)


if __name__ == "__main__":
    main()
