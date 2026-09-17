#!/usr/bin/env python
"""Entry point: compact the realtime pollers' per-poll files into one file per day.

The pollers write one small Parquet per underlying per poll, which is the right
shape while a session is live and the wrong shape afterwards. This rolls each
completed day into a single file and drops snapshots where nothing about a
contract's quote or size actually changed.

The file-count reduction is the main win (~390 files per root per session down to
one). The row saving is real but modest and scales with illiquidity: measured over
one session, XBI kept 68% of its rows and SPY 97%, because `volume` and
`trade_count` genuinely change on every print for an active contract.

    python scripts/run_compaction.py                     # every finished day
    python scripts/run_compaction.py --day 2026-09-04
    python scripts/run_compaction.py --delete            # remove inputs once written
    python scripts/run_compaction.py --no-dedupe         # concatenate only

Today is skipped unless named explicitly with --day, since compacting a session
that's still being written would strand the later polls.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from volmagaddon.config import get_settings
from volmagaddon.sources.thetadata import _OUTPUT_SCHEMA
from volmagaddon.storage import COMPACTED_NAME, compact_day, rewrite_day_to_schema


def _log(message: str) -> None:
    print(f"[compact] {message}", flush=True)


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> None:
    settings = get_settings()
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", default="thetadata", help="lake subdirectory (default thetadata)")
    ap.add_argument("--day", type=_parse_date, default=None, help="single session to compact")
    ap.add_argument("--delete", action="store_true",
                    help="delete the per-poll files after the compacted file is written")
    ap.add_argument("--no-dedupe", dest="dedupe", action="store_false",
                    help="keep every snapshot, just concatenate")
    ap.add_argument("--normalize", action="store_true",
                    help="rewrite poll files onto the current schema and drop corrupt parquets")
    args = ap.parse_args()

    root = settings.data_dir / args.source
    if not root.exists():
        ap.error(f"nothing at {root}")

    today = date.today()
    jobs: list[tuple[str, date]] = []
    for underlying_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        for day_dir in sorted(underlying_dir.glob("dt=*")):
            try:
                day = _parse_date(day_dir.name.removeprefix("dt="))
            except ValueError:
                continue
            if args.day is not None and day != args.day:
                continue
            if args.day is None and day >= today:
                continue  # still being written
            if any(p.name != COMPACTED_NAME for p in day_dir.glob("*.parquet")):
                jobs.append((underlying_dir.name, day))

    if not jobs:
        _log("nothing to compact")
        return

    if args.normalize:
        _log(f"{len(jobs)} day(s) to normalize in {root}")
        for underlying, day in jobs:
            day_dir = settings.data_dir / args.source / underlying / f"dt={day.isoformat()}"
            rewritten, deleted, skipped = rewrite_day_to_schema(day_dir, _OUTPUT_SCHEMA)
            _log(f"{underlying:<6} {day}  rewritten={rewritten} corrupt_deleted={deleted} "
                 f"already_ok={skipped}")
        return

    _log(f"{len(jobs)} day(s) to compact in {root}  dedupe={args.dedupe} delete={args.delete}")
    rows_before = rows_after = 0
    for underlying, day in jobs:
        result = compact_day(settings.data_dir, args.source, underlying, day,
                             dedupe=args.dedupe, remove_source=args.delete)
        if result is None:
            continue
        path, before, after = result
        rows_before += before
        rows_after += after
        kept = after / before if before else 1.0
        _log(f"{underlying:<6} {day}  {before:>8} -> {after:>8} rows ({kept:5.1%})  "
             f"{path.stat().st_size / 1e6:6.1f}MB")

    if rows_before:
        _log(f"total {rows_before} -> {rows_after} rows ({rows_after / rows_before:.1%} kept)")


if __name__ == "__main__":
    main()
