#!/usr/bin/env python
"""Entry point: backfill IBKR underlying bars (not option chains).

ThetaData option EOD is `run_thetadata_backfill.py`. This script is the other
half of Phase 3: a long underlying price series, because ThetaData stock/index
history is gated on this account.

TWS or IB Gateway must already be logged in. Use a different client id from
the live poller (default 18 vs the poller's 17) so they can coexist.

    python scripts/run_ibkr_backfill.py
    python scripts/run_ibkr_backfill.py --start 2016-01-01 --end 2026-09-08
    python scripts/run_ibkr_backfill.py --symbols SPY QQQ SPX
    python scripts/run_ibkr_backfill.py --bar-size "1 min" --start 2026-08-01
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from volmagaddon.config import get_settings
from volmagaddon.sources.ibkr import connect, get_dated_future_history, get_underlying_history

SOURCE = "ibkr_eod"


def _log(message: str) -> None:
    print(f"[ibkr-backfill] {message}", flush=True)


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _year_ends(start: date, end: date) -> list[date]:
    """Inclusive year-end (or final `end`) dates covering [start, end]."""
    out = []
    year = start.year
    while year < end.year:
        out.append(date(year, 12, 31))
        year += 1
    out.append(end)
    return out


def main() -> None:
    settings = get_settings()
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", type=_parse_date, default=date(2016, 1, 1))
    ap.add_argument("--end", type=_parse_date, default=date.today())
    ap.add_argument("--symbols", nargs="*", default=None)
    ap.add_argument("--bar-size", default="1 day",
                    help='IBKR bar size, e.g. "1 day" or "1 min"')
    ap.add_argument("--client-id", type=int, default=18,
                    help="must differ from the live poller's IBKR_CLIENT_ID")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    universe = {i.symbol: i for i in settings.bar_instruments}
    if args.symbols:
        missing = [s for s in args.symbols if s.upper() not in universe]
        if missing:
            ap.error(f"not in universe.yaml (underlyings/indices/futures): {', '.join(missing)}")
        targets = [universe[s.upper()] for s in args.symbols]
    else:
        targets = list(universe.values())

    chunks = _year_ends(args.start, args.end)
    _log(f"roots={[i.symbol for i in targets]} years={len(chunks)} "
         f"range={args.start}..{args.end} bar={args.bar_size!r} client_id={args.client_id}")

    settings.ibkr_client_id = args.client_id
    ib = connect(settings)
    started = time.time()
    rows_total = 0

    def _ensure() -> None:
        if ib.isConnected():
            return
        _log("socket dropped — reconnecting")
        try:
            ib.disconnect()
        except Exception:
            pass
        time.sleep(2)
        ib.connect(
            settings.ibkr_host, settings.ibkr_port,
            clientId=settings.ibkr_client_id,
        )

    try:
        for instrument in targets:
            marker_dir = settings.data_dir / SOURCE / instrument.symbol / "_complete"
            marker_dir.mkdir(parents=True, exist_ok=True)
            frames = []
            if instrument.kind == "future":
                # ContFuture is truncated (ES ~2023, NQ ~2024, VX almost empty).
                # Dated includeExpired only resolves ~1y of listed/recently-expired
                # contracts on this account — 2016 expiries 200. Merge both:
                # dated wins on overlap (real contract), ContFuture fills the earlier
                # hole. Never replace a longer existing file with a shorter stitch.
                out = settings.data_dir / SOURCE / instrument.symbol / "daily.parquet"
                if args.bar_size != "1 day":
                    out = settings.data_dir / SOURCE / instrument.symbol / f"{args.bar_size.replace(' ', '_')}.parquet"
                label = f"{args.start:%Y}-{args.end:%Y}-dated"
                marker = marker_dir / f"{label}-{args.bar_size.replace(' ', '')}"
                t0 = time.time()
                dated = None
                cont = None
                try:
                    _ensure()
                    dated = get_dated_future_history(
                        ib, instrument, start=args.start, end=args.end,
                        bar_size=args.bar_size, log=_log, ensure=_ensure)
                except Exception as exc:  # noqa: BLE001
                    _log(f"{instrument.symbol} dated: ERROR {exc!r}")
                try:
                    _ensure()
                    years = max(1, args.end.year - args.start.year + 1)
                    cont = get_underlying_history(
                        ib, instrument, end=None, duration=f"{years} Y",
                        bar_size=args.bar_size)
                except Exception as exc:  # noqa: BLE001
                    _log(f"{instrument.symbol} contfuture: ERROR {exc!r}")
                pieces = []
                if out.exists():
                    pieces.append(pl.read_parquet(out))
                if cont is not None and not cont.is_empty():
                    pieces.append(cont)
                    _log(f"{instrument.symbol} contfuture: {cont.height:>6} bars  "
                         f"{cont['session'].min()}..{cont['session'].max()}")
                if dated is not None and not dated.is_empty():
                    pieces.append(dated)
                    _log(f"{instrument.symbol} dated: {dated.height:>6} bars  "
                         f"{dated['session'].min()}..{dated['session'].max()}")
                if not pieces:
                    _log(f"{instrument.symbol} {label}: no data (not marking complete)")
                    continue
                combined = pl.concat(pieces, how="vertical").unique(
                    subset=["session"], keep="last").sort("session")
                combined.write_parquet(out)
                marker.touch()
                rows_total += combined.height
                _log(f"{instrument.symbol}: wrote {combined.height} rows "
                     f"{combined['session'].min()}..{combined['session'].max()}  "
                     f"-> {out}  {time.time() - t0:5.1f}s")
                continue
            else:
                for chunk_end in chunks:
                    label = f"{chunk_end:%Y}"
                    marker = marker_dir / f"{label}-{args.bar_size.replace(' ', '')}"
                    if marker.exists() and not args.force:
                        continue
                    t0 = time.time()
                    try:
                        _ensure()
                        df = get_underlying_history(
                            ib, instrument, end=chunk_end, duration="1 Y",
                            bar_size=args.bar_size)
                    except Exception as exc:  # noqa: BLE001 — one chunk must not kill the rest
                        _log(f"{instrument.symbol} {label}: ERROR {exc!r}")
                        time.sleep(2)
                        continue
                    if df.is_empty():
                        _log(f"{instrument.symbol} {label}: no data (not marking complete)")
                        time.sleep(2)
                        continue
                    frames.append(df)
                    marker.touch()
                    rows_total += df.height
                    _log(f"{instrument.symbol} {label}: {df.height:>6} bars  "
                         f"{df['session'].min()}..{df['session'].max()}  {time.time() - t0:5.1f}s")
                    time.sleep(2)

            if not frames:
                continue
            combined = pl.concat(frames, how="vertical").unique(
                subset=["session"], keep="last").sort("session")
            out = settings.data_dir / SOURCE / instrument.symbol / "daily.parquet"
            if args.bar_size != "1 day":
                out = settings.data_dir / SOURCE / instrument.symbol / f"{args.bar_size.replace(' ', '_')}.parquet"
            combined.write_parquet(out)
            _log(f"{instrument.symbol}: wrote {combined.height} rows -> {out}")
    finally:
        ib.disconnect()
    _log(f"done — {rows_total} bars in {(time.time() - started) / 60:.1f} min")


if __name__ == "__main__":
    main()
