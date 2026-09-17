"""Partitioned Parquet writer and compaction, shared by both pollers.

Layout: {data_dir}/{source}/{underlying}/dt={YYYY-MM-DD}/{HHMMSS}.parquet

One small file per poll per underlying, deliberately — see the README's
"Design notes" section for why. `compact_day` rolls each completed day into a
single file per underlying once the session is done.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

COMPACTED_NAME = "compacted.parquet"

# Fields that make a snapshot row worth keeping. At a 60s cadence most contracts
# in a full chain never trade and never requote, so consecutive snapshots are
# byte-identical on everything that matters; the only things that always differ
# are the poll clock and the derived surface. Deduping on state rather than on
# the whole row is what makes the compaction worthwhile.
_STATE_COLS = (
    "bid", "ask", "bid_size", "ask_size", "bid_exchange", "ask_exchange",
    "bid_condition", "ask_condition", "day_open", "day_high", "day_low",
    "day_close", "volume", "trade_count", "open_interest",
)
_CONTRACT_KEYS = ("symbol", "expiration", "strike", "right")


def write_snapshot(df: pl.DataFrame, *, data_dir: Path, source: str, underlying: str,
                    ts: datetime | None = None) -> Path | None:
    """Write one polled snapshot to its partitioned path. Returns the path written,
    or None if the frame was empty (nothing to write, not an error — e.g. market closed).
    """
    if df.is_empty():
        return None

    ts = ts or datetime.now(timezone.utc)
    day_dir = data_dir / source / underlying / f"dt={ts.date().isoformat()}"
    day_dir.mkdir(parents=True, exist_ok=True)

    out_path = day_dir / f"{ts.strftime('%H%M%S')}.parquet"
    df.write_parquet(out_path)
    return out_path


def _align(df: pl.DataFrame, reference: dict[str, pl.DataType]) -> pl.DataFrame:
    """Coerce one frame onto a reference schema.

    Files written across a code change can disagree on both columns and dtypes,
    and `concat` refuses on either. Absent columns become typed nulls; a column
    stored as text where the reference wants a timestamp is parsed rather than
    cast, since casting String to Datetime doesn't work.
    """
    projection = []
    for name, dtype in reference.items():
        if name not in df.columns:
            projection.append(pl.lit(None, dtype).alias(name))
        elif df.schema[name] == dtype:
            projection.append(pl.col(name))
        elif df.schema[name] == pl.String and dtype in (pl.Date, pl.Datetime):
            parsed = pl.col(name).str.to_datetime(strict=False)
            projection.append((parsed.dt.date() if dtype == pl.Date else parsed)
                              .cast(dtype).alias(name))
        else:
            projection.append(pl.col(name).cast(dtype, strict=False))
    return df.select(projection)


def dedupe_unchanged(df: pl.DataFrame) -> pl.DataFrame:
    """Keep only the snapshots where a contract's quote or size state changed.

    The first observation of each contract is always kept, so the result is a
    change-log: to recover the state at any time, take the last row per contract
    at or before it.
    """
    state = [c for c in _STATE_COLS if c in df.columns]
    keys = [c for c in _CONTRACT_KEYS if c in df.columns]
    if not state or not keys or "poll_timestamp" not in df.columns:
        return df

    df = df.sort("poll_timestamp")
    changed = pl.any_horizontal(
        [pl.col(c).ne_missing(pl.col(c).shift(1).over(keys)) for c in state]
    )
    first = pl.int_range(pl.len()).over(keys) == 0
    return df.filter(first | changed)


def rewrite_day_to_schema(day_dir: Path, schema: dict[str, pl.DataType]) -> tuple[int, int, int]:
    """Rewrite every poll file in `day_dir` onto `schema`. Deletes unreadable files.

    Live files written before `session`/`last_trade_timestamp` existed, or with
    `expiration` stored as text, are projected forward. Nothing is synthesized:
    missing columns become typed nulls, and a parquet that will not parse is
    removed rather than patched. Returns (rewritten, deleted_corrupt, already_ok).
    """
    parts = sorted(p for p in day_dir.glob("*.parquet") if p.name != COMPACTED_NAME)
    rewritten = deleted = skipped = 0
    for path in parts:
        try:
            file_schema = pl.read_parquet_schema(path)
        except Exception:
            path.unlink()
            deleted += 1
            continue
        if dict(file_schema) == schema:
            skipped += 1
            continue
        try:
            df = pl.read_parquet(path)
        except Exception:
            path.unlink()
            deleted += 1
            continue
        if "session" not in df.columns and "poll_timestamp" in df.columns:
            ts = pl.col("poll_timestamp")
            if df.schema["poll_timestamp"].time_zone is None:
                ts = ts.dt.replace_time_zone("UTC")
            df = df.with_columns(
                ts.dt.convert_time_zone("America/New_York").dt.date().alias("session")
            )
        df = _align(df, schema)
        tmp = path.with_suffix(".parquet.tmp")
        df.write_parquet(tmp)
        tmp.replace(path)
        rewritten += 1
    return rewritten, deleted, skipped


def compact_day(data_dir: Path, source: str, underlying: str, day: date, *,
                 dedupe: bool = True, remove_source: bool = False) -> tuple[Path, int, int] | None:
    """Roll one day's per-poll files into a single Parquet. Returns (path, before, after).

    Source files are only deleted when `remove_source` is set, and only after the
    compacted file is written and read back — a half-compacted day that still has
    its inputs is recoverable, one that doesn't isn't.
    """
    day_dir = data_dir / source / underlying / f"dt={day.isoformat()}"
    parts = sorted(p for p in day_dir.glob("*.parquet") if p.name != COMPACTED_NAME)
    if not parts:
        return None

    frames = []
    for p in parts:
        try:
            df = pl.read_parquet(p)
        except Exception:
            continue
        frames.append(df)
    if not frames:
        return None

    # The newest readable file is the reference: if the code changed mid-session,
    # its schema is the current one. Unreadable files are skipped, not patched.
    reference = frames[-1].schema
    frames = [_align(df, reference) for df in frames]

    combined = pl.concat(frames, how="vertical")
    before = combined.height
    if dedupe:
        combined = dedupe_unchanged(combined)

    out_path = day_dir / COMPACTED_NAME
    combined.write_parquet(out_path)

    # Verify by reading back before touching the inputs. A half-compacted day
    # that still has its source files is recoverable; one that doesn't isn't.
    if remove_source:
        if pl.scan_parquet(out_path).select(pl.len()).collect().item() != combined.height:
            raise OSError(f"{out_path}: readback mismatch, keeping source files")
        for p in parts:
            p.unlink()

    return out_path, before, combined.height
