#!/usr/bin/env python
"""Entry point: backfill ThetaData EOD option history into the Parquet lake.

Resumable and safe to re-run: a month is skipped once its marker exists, so an
interrupted run picks up where it stopped. Delete a marker to force a refetch.

    python scripts/run_thetadata_backfill.py                       # everything, 2016-01-01 on
    python scripts/run_thetadata_backfill.py --start 2024-01-01
    python scripts/run_thetadata_backfill.py --symbols SPY SPXW
    python scripts/run_thetadata_backfill.py --force              # ignore markers

This is a long job. A full chain is ~12k contracts per session and the Terminal
serves ~4k rows/s, so expect hours per root for the full window; run it against
a session that's already closed.
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from volmagaddon.config import get_settings
from volmagaddon.sources.thetadata import (HISTORY_START, ThetaDataError, get_eod_history,
                                            month_chunks)

SOURCE = "thetadata_eod"


def _log(message: str) -> None:
    print(f"[backfill] {message}", flush=True)


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> None:
    settings = get_settings()
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", type=_parse_date, default=HISTORY_START,
                    help=f"first session (default {HISTORY_START}, the subscription floor)")
    ap.add_argument("--end", type=_parse_date, default=date.today(),
                    help="last session (default today)")
    ap.add_argument("--symbols", nargs="*", default=None,
                    help="roots to pull (default: the whole ThetaData universe)")
    ap.add_argument("--force", action="store_true", help="refetch months already marked done")
    args = ap.parse_args()

    universe = {i.symbol: i for i in settings.thetadata_instruments}
    if args.symbols:
        missing = [s for s in args.symbols if s.upper() not in universe]
        if missing:
            ap.error(f"not in universe.yaml: {', '.join(missing)}")
        targets = [universe[s.upper()] for s in args.symbols]
    else:
        targets = list(universe.values())

    if args.start < HISTORY_START:
        _log(f"clamping start to {HISTORY_START} — earlier dates 403 on this subscription")

    chunks = list(month_chunks(args.start, args.end))
    _log(f"roots={[i.symbol for i in targets]} months={len(chunks)} "
         f"range={max(args.start, HISTORY_START)}..{args.end}")

    started = time.time()
    rows_total = 0
    for instrument in targets:
        marker_dir = settings.data_dir / SOURCE / instrument.symbol / "_complete"
        marker_dir.mkdir(parents=True, exist_ok=True)

        for chunk_start, chunk_end in chunks:
            marker = marker_dir / f"{chunk_start:%Y-%m}"
            if marker.exists() and not args.force:
                continue

            t0 = time.time()
            try:
                df = get_eod_history(instrument, chunk_start, chunk_end, settings)
            except ThetaDataError as exc:
                # One bad month shouldn't abandon the other 100. No marker is
                # written, so a rerun retries exactly this chunk.
                _log(f"{instrument.symbol} {chunk_start:%Y-%m}: ERROR {exc}")
                continue

            if df.is_empty():
                _log(f"{instrument.symbol} {chunk_start:%Y-%m}: no data")
                marker.touch()
                continue

            # Day-level partitions to match the realtime lake, so a query can
            # prune to a session without opening a month.
            for (session,), day_df in df.group_by(["session"], maintain_order=True):
                day_dir = (settings.data_dir / SOURCE / instrument.symbol
                           / f"dt={session.isoformat()}")
                day_dir.mkdir(parents=True, exist_ok=True)
                day_df.write_parquet(day_dir / "eod.parquet")

            marker.touch()
            rows_total += df.height
            sessions = df["session"].n_unique()
            solved = df["iv"].is_not_null().sum()
            _log(f"{instrument.symbol} {chunk_start:%Y-%m}: {df.height:>7} rows "
                 f"{sessions:>2} sessions  iv={solved / df.height:5.1%}  {time.time() - t0:6.1f}s")

    _log(f"done — {rows_total} rows in {(time.time() - started) / 60:.1f} min")


if __name__ == "__main__":
    main()
