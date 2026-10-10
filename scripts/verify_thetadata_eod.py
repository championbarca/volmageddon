#!/usr/bin/env python
"""Check ThetaData EOD option files against the NYSE calendar.

A month marker under ``_complete/`` is not proof the month is complete. This
walks ``data/thetadata_eod/{root}/dt=YYYY-MM-DD/eod.parquet`` and reports:

- NYSE sessions in the window with no file
- files that are empty, thin versus that root's own median, or unreadable
- a session column that does not match the directory date
- duplicate (expiration, strike, right) keys
- IV or open-interest mostly null
- a month marker that exists while an NYSE session in that month is missing
- a session whose file is present but too sparse to plot the front of the
  SPX curve: weeklies live on SPXW, so an SPX-only day leaves one expiry
  inside 45 days and no tenor near 30 days

Exit status is 0 when nothing is wrong, 1 when any check fails.

    python scripts/verify_thetadata_eod.py
    python scripts/verify_thetadata_eod.py --symbols SPX SPXW --start 2026-09-01 --end 2026-10-09

``--end`` defaults to the latest NYSE session on or before today's New York
date, not the PC clock. ``--start`` defaults to the earliest file found.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import polars as pl

from volmagaddon.config import get_settings

NY = ZoneInfo("America/New_York")
SOURCE = "thetadata_eod"
_KEYS = ["expiration", "strike", "right"]
# The desk chart draws SPX and SPXW together. A normal session has about 30
# expiries inside 45 days and one of them within a week of 30 days. An SPX
# file alone is a full monthly chain and still fails both of these.
DESK_ROOTS = ("SPX", "SPXW")
MIN_FRONT_TENORS = 10
NEAR_30_DAYS = 7


def _log(message: str) -> None:
    print(message, flush=True)


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _ny_today() -> date:
    return datetime.now(NY).date()


def _nyse_days(start: date, end: date) -> list[date]:
    import pandas_market_calendars as mcal

    cal = mcal.get_calendar("NYSE")
    sched = cal.schedule(start_date=start.isoformat(), end_date=end.isoformat())
    idx = sched.index
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_localize(None)
    return [ts.date() for ts in idx]


def _last_closed_session(today: date) -> date:
    """Latest NYSE session on or before today. Looks back across a long weekend."""
    days = _nyse_days(today.replace(year=today.year - 1), today)
    return days[-1] if days else today


def _ranges(days: list[date]) -> str:
    if not days:
        return "—"
    ordered = sorted(days)
    chunks: list[str] = []
    begin = prev = ordered[0]
    for day in ordered[1:]:
        if (day - prev).days == 1:
            prev = day
            continue
        chunks.append(begin.isoformat() if begin == prev else f"{begin.isoformat()}..{prev.isoformat()}")
        begin = prev = day
    chunks.append(begin.isoformat() if begin == prev else f"{begin.isoformat()}..{prev.isoformat()}")
    return ", ".join(chunks)


def _file_days(root_dir: Path) -> dict[date, Path]:
    found: dict[date, Path] = {}
    if not root_dir.is_dir():
        return found
    for part in root_dir.glob("dt=*/eod.parquet"):
        try:
            day = date.fromisoformat(part.parent.name.removeprefix("dt="))
        except ValueError:
            continue
        found[day] = part
    return found


def _markers(root_dir: Path) -> set[str]:
    marker_dir = root_dir / "_complete"
    if not marker_dir.is_dir():
        return set()
    return {path.name for path in marker_dir.iterdir() if path.is_file()}


def _inspect(path: Path) -> dict:
    frame = pl.read_parquet(path, columns=["session", "dte", *_KEYS, "iv", "open_interest"])
    rows = frame.height
    sessions = frame["session"].drop_nulls().unique().to_list() if rows else []
    dupes = 0
    front: set[int] = set()
    if rows:
        dupes = int(
            frame.group_by(_KEYS).len().filter(pl.col("len") > 1).height
        )
        for dte in frame.filter(pl.col("dte").is_between(0, 45))["dte"].unique().to_list():
            front.add(int(dte))
    iv_ok = int(frame["iv"].is_not_null().sum()) if rows else 0
    oi_ok = int(frame["open_interest"].is_not_null().sum()) if rows else 0
    return {
        "rows": rows,
        "sessions": sessions,
        "dupes": dupes,
        "front": front,
        "iv_rate": (iv_ok / rows) if rows else 0.0,
        "oi_rate": (oi_ok / rows) if rows else 0.0,
    }


def _desk_gaps(coverage: dict[str, dict[date, set[int]]], expected: list[date]) -> list[str]:
    """Sessions that have a file but not enough expiries to draw the front curve."""
    lines = []
    for day in expected:
        present = [root for root in DESK_ROOTS if day in coverage.get(root, {})]
        if not present:
            continue
        tenors: set[int] = set()
        for root in present:
            tenors |= coverage[root][day]
        nearest = min(tenors, key=lambda dte: (abs(dte - 30), dte)) if tenors else None
        sparse = len(tenors) < MIN_FRONT_TENORS or nearest is None or abs(nearest - 30) > NEAR_30_DAYS
        if not sparse:
            continue
        absent = [root for root in DESK_ROOTS if root not in present]
        near = "none" if nearest is None else f"{nearest}d"
        lines.append(
            f"  INSUFFICIENT {day}  have {'+'.join(present)}  "
            f"missing {'+'.join(absent) if absent else '—'}  "
            f"{len(tenors)} {'tenor' if len(tenors) == 1 else 'tenors'} inside 45d  nearest to 30d is {near}"
        )
    return lines


def main() -> int:
    settings = get_settings()
    universe = [item.symbol for item in settings.thetadata_instruments]
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--symbols", nargs="*", default=None, help="roots (default: the ThetaData universe)")
    ap.add_argument("--start", type=_parse_date, default=None, help="first session (default: earliest file)")
    ap.add_argument("--end", type=_parse_date, default=None, help="last session (default: latest closed NYSE day)")
    ap.add_argument("--min-iv", type=float, default=0.50, help="flag a file below this IV solve rate (default 0.50)")
    ap.add_argument("--min-oi", type=float, default=0.50, help="flag a file below this open-interest fill rate (default 0.50)")
    args = ap.parse_args()

    if args.symbols:
        missing = [sym for sym in args.symbols if sym.upper() not in universe]
        if missing:
            ap.error(f"not in universe.yaml: {', '.join(missing)}")
        roots = [sym.upper() for sym in args.symbols]
    else:
        roots = universe

    lake = settings.data_dir / SOURCE
    end = args.end or _last_closed_session(_ny_today())
    discovered: dict[str, dict[date, Path]] = {root: _file_days(lake / root) for root in roots}
    if args.start is not None:
        start = args.start
    else:
        earliest = [min(days) for days in discovered.values() if days]
        start = min(earliest) if earliest else end
    if start > end:
        ap.error(f"start {start} is after end {end}")

    expected = _nyse_days(start, end)
    _log(f"window {start} .. {end}   NYSE sessions {len(expected)}   roots {', '.join(roots)}")
    failures = 0
    coverage: dict[str, dict[date, set[int]]] = {root: {} for root in roots}

    for root in roots:
        files = discovered[root]
        root_dir = lake / root
        in_window = {day: path for day, path in files.items() if start <= day <= end}
        missing_days = [day for day in expected if day not in in_window]
        extra = sorted(day for day in in_window if day not in set(expected))
        markers = _markers(root_dir)
        false_markers: list[str] = []
        for day in missing_days:
            stamp = f"{day:%Y-%m}"
            if stamp in markers and stamp not in false_markers:
                false_markers.append(stamp)

        expected_set = set(expected)
        stats: list[tuple[date, dict]] = []
        bad_files: list[str] = []
        for day in sorted(in_window):
            if day not in expected_set:
                continue
            path = in_window[day]
            try:
                info = _inspect(path)
            except Exception as exc:
                bad_files.append(f"  UNREADABLE {day}  {type(exc).__name__}: {exc}")
                failures += 1
                continue
            info_sessions = []
            for value in info["sessions"]:
                if isinstance(value, datetime):
                    value = value.date()
                info_sessions.append(value)
            if info["rows"] == 0:
                bad_files.append(f"  EMPTY      {day}")
                failures += 1
            elif info_sessions != [day]:
                shown = ", ".join(str(v) for v in info_sessions[:4]) or "none"
                bad_files.append(f"  SESSION    {day}  file contains {shown}")
                failures += 1
            if info["dupes"]:
                bad_files.append(f"  DUPES      {day}  {info['dupes']} keys")
                failures += 1
            if info["rows"] > 0:
                coverage[root][day] = info["front"]
            stats.append((day, info))

        heights = [info["rows"] for _, info in stats if info["rows"] > 0]
        median = sorted(heights)[len(heights) // 2] if heights else 0
        thin_floor = max(200, int(median * 0.25)) if median else 0
        iv_rates = []
        oi_rates = []
        for day, info in stats:
            if info["rows"] <= 0:
                continue
            iv_rates.append(info["iv_rate"])
            oi_rates.append(info["oi_rate"])
            if info["rows"] < thin_floor:
                bad_files.append(f"  THIN       {day}  {info['rows']} rows  (median {median})")
                failures += 1
            if info["iv_rate"] < args.min_iv:
                bad_files.append(f"  LOW_IV     {day}  {info['iv_rate']:.1%} solved")
                failures += 1
            if info["oi_rate"] < args.min_oi:
                bad_files.append(f"  LOW_OI     {day}  {info['oi_rate']:.1%} filled")
                failures += 1

        if missing_days:
            failures += len(missing_days)
        if false_markers:
            failures += len(false_markers)

        iv_med = sorted(iv_rates)[len(iv_rates) // 2] if iv_rates else 0.0
        oi_med = sorted(oi_rates)[len(oi_rates) // 2] if oi_rates else 0.0
        _log(
            f"{root:6}  files {len(in_window):4}  missing {len(missing_days):4}  "
            f"median_rows {median:7}  iv {iv_med:6.1%}  oi {oi_med:6.1%}"
        )
        if missing_days:
            _log(f"  MISSING    {_ranges(missing_days)}")
        if false_markers:
            _log(f"  MARKER     marked complete but missing sessions: {', '.join(false_markers)}")
        if extra:
            _log(f"  OFF_CAL    {_ranges(extra)}")
        for line in bad_files:
            _log(line)

    if set(DESK_ROOTS) <= set(roots):
        gaps = _desk_gaps(coverage, expected)
        _log(f"DESK SPX+SPXW  chart gaps {len(gaps)}")
        for line in gaps:
            _log(line)
        failures += len(gaps)

    if failures:
        _log(f"FAIL  {failures} problems")
        return 1
    _log("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
