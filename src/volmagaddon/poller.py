"""Source-agnostic polling loop shared by both scripts/run_*_poller.py entry points."""
from __future__ import annotations

import time
from datetime import date, datetime, timezone
from typing import Callable
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal
import polars as pl

from .config import Instrument
from .storage import write_snapshot

NY = ZoneInfo("America/New_York")

# NYSE rather than CBOE_Equity_Options: the two agree on every trading day, but
# the CBOE calendar in this library misses the 13:00 ET Christmas Eve close,
# which would have us polling a shut market for three hours. Where they differ,
# the earlier close is the safe direction.
_CALENDAR = "NYSE"

# Sessions are looked up per year and cached — the poll loop asks every 60s and
# building a year's schedule is far too slow to repeat.
_sessions: dict[date, tuple[datetime, datetime]] = {}
_sessions_year: int | None = None

PollFn = Callable[[Instrument, object], pl.DataFrame]  # (instrument, settings) -> DataFrame


def _log(message: str) -> None:
    # Unbuffered: these run for a whole session, usually with stdout redirected
    # to a file, and Python only line-buffers when it's a tty — without the
    # flush the log stays empty for hours.
    print(message, flush=True)


def session_window(day: date) -> tuple[datetime, datetime] | None:
    """UTC open/close for a trading day, or None if the exchange was shut.

    Returning None for holidays is the whole point: a weekday test alone had us
    treating Labor Day as a normal session, which would have written a full
    day's worth of stale quotes into a partition that shouldn't exist.
    """
    global _sessions, _sessions_year
    if _sessions_year != day.year:
        schedule = mcal.get_calendar(_CALENDAR).schedule(
            start_date=f"{day.year}-01-01", end_date=f"{day.year}-12-31")
        _sessions = {idx.date(): (row.market_open.to_pydatetime(),
                                   row.market_close.to_pydatetime())
                      for idx, row in schedule.iterrows()}
        _sessions_year = day.year
    return _sessions.get(day)


def is_market_open(now: datetime | None = None) -> bool:
    """True only inside a real session, half-days included."""
    now = now or datetime.now(NY)
    window = session_window(now.date())
    if window is None:
        return False
    opened, closed = window
    return opened <= now.astimezone(timezone.utc) <= closed


def run_loop(poll_fn: PollFn, settings, source_name: str,
              instruments: list[Instrument] | None = None) -> None:
    instruments = instruments if instruments is not None else settings.instruments
    symbols = [i.symbol for i in instruments]
    _log(f"[{source_name}] starting — instruments={symbols} "
         f"interval={settings.poll_interval_seconds}s data_dir={settings.data_dir}")
    while True:
        now_ny = datetime.now(NY)
        if not is_market_open(now_ny):
            reason = "no session" if session_window(now_ny.date()) is None else "outside hours"
            _log(f"[{source_name}] market closed ({now_ny:%Y-%m-%d %H:%M %Z}, {reason}) — sleeping")
            time.sleep(settings.poll_interval_seconds)
            continue

        for instrument in instruments:
            try:
                df = poll_fn(instrument, settings)
                path = write_snapshot(df, data_dir=settings.data_dir, source=source_name,
                                       underlying=instrument.symbol)
                if path is not None:
                    _log(f"[{source_name}] {instrument.symbol} ({instrument.kind}): "
                         f"{df.height} rows -> {path}")
                else:
                    _log(f"[{source_name}] {instrument.symbol} ({instrument.kind}): no data this poll")
            except Exception as exc:  # noqa: BLE001 — v1: log and keep polling other symbols
                # Some failures are structural (subscription tier, bad credentials):
                # every symbol will fail identically on every future poll, so the
                # loop stops instead of filling the log with the same error.
                if getattr(exc, "fatal", False):
                    _log(f"[{source_name}] FATAL — {exc}")
                    _log(f"[{source_name}] not retryable, stopping.")
                    raise
                _log(f"[{source_name}] {instrument.symbol} ({instrument.kind}): ERROR {exc!r}")

        time.sleep(settings.poll_interval_seconds)
