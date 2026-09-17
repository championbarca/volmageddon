"""ThetaData Terminal v3 client — realtime chain snapshots and EOD history.

## What this account can actually reach

The endpoint set here is dictated by subscription tier, which the Terminal only
reports at request time. Probing the local Terminal gives:

    /v3/option/snapshot/quote            200  full-chain NBBO
    /v3/option/snapshot/ohlc             200  session OHLC + volume + trade count
    /v3/option/snapshot/open_interest    200  previous-session OI
    /v3/option/snapshot/greeks/*         403  needs PROFESSIONAL, account is STANDARD
    /v3/option/history/eod               200  but only from 2016-01-01 (403 before)
    /v3/option/history/open_interest     200  same window
    /v3/stock/snapshot/quote             403  stock tier is FREE, needs VALUE
    /v3/stock/history/eod                200  only ~2.5 years back, 365 days per request
    /v3/index/snapshot/price             403  index tier is FREE, needs STANDARD
    /v3/index/history/eod                403  beyond the last few weeks

Consequences that shape this module:

1. Greeks and IV are never available from the API, at any tier we hold, for
   either realtime or history. They are always computed locally here.
2. There is no usable underlying price series — not spot, not index level, not
   deep stock history. The forward is recovered from the option chain instead.
3. Options history starts 2016-01-01, not 2012 as the blueprint assumed. That
   is a hard calendar floor, not a rolling window.

## Getting an underlying without an underlying feed

The forward is recovered from the chain itself via put-call parity,
`C - P = D * (F - K)`, fitted across paired strikes per (session, expiry).

This is better than a spot feed rather than a substitute for one:

- The parity forward already contains the market's dividend and borrow
  assumptions, so no dividend calendar or borrow curve is needed.
- For VIX (and any option on a future) it is the *only* correct input — VIX
  options price off the VIX future for their expiry, not off spot VIX.

Only the discount factor still needs an assumption (`THETADATA_RATE`), and it
barely matters: the parity slope is far too noisy to fit a rate from
short-dated mids, while a 100bp error in `r` moves a 30-day IV by well under a
vol point.

## Greeks convention

Greeks are Black-76 with respect to the *forward*, since the forward is what we
observe. They are driftless, which is what vol analytics (surfaces, VRP, skew)
wants anyway. `forward` and `discount` are emitted per row so the conversion to
spot greeks stays available: dF/dS = F/S, so spot delta = delta * F/S and spot
gamma = gamma * (F/S)^2 — a ~1.006 factor for SPY, immaterial for GEX
aggregation but exact once IBKR spot is joined in.

Units: `vega` is per 1.00 of vol (divide by 100 for per-vol-point), `theta` and
`charm` are per calendar day, everything else is raw.

Realtime `tau` runs to 16:00 ET on the expiration date, so 0DTE contracts get a
real fraction of a day rather than zero. EOD history is stamped at the close, so
there `tau` is whole days and same-day expiries land at zero (already settled,
so no IV — correct). AM-settled expiries (SPX monthlies, VIX) are overstated by
a few hours; not corrected here.
"""
from __future__ import annotations

import io
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterator
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl
import requests
from scipy.special import ndtr

from ..config import Instrument

NY = ZoneInfo("America/New_York")

# A contract is identified by these four; history adds the session, since one
# response spans many trading days.
_JOIN_KEYS = ["symbol", "expiration", "strike", "right"]
_HISTORY_KEYS = [*_JOIN_KEYS, "session"]
_SECONDS_PER_YEAR = 365.25 * 24 * 3600

# Hard floor on this subscription: earlier dates return 403, not empty.
HISTORY_START = date(2016, 1, 1)

# ThetaData's "your request was fine, there just isn't data" status. Common and
# expected (illiquid root, market holiday, nothing printed yet), so it maps to an
# empty frame rather than an exception.
_NO_DATA = 472

_IV_BOUNDS = (1e-4, 5.0)
_IV_ITERATIONS = 64

# Parity fits use strikes near the money, where both legs are liquid enough for
# the mid to mean something. Deep ITM/OTM quotes are wide and stale and drag the
# fit badly — an unfiltered least-squares fit on the full SPY chain implies a
# discount factor above 1.0, i.e. negative rates.
_ATM_BAND = 0.06
_ATM_BAND_WIDE = 0.15
_MIN_PARITY_PAIRS = 3

# Written on every poll regardless of which endpoints answered, so realtime and
# historical files stay readable as one dataset. Without this, a root whose ohlc
# call comes back empty silently drops those columns and a later scan over the
# directory fails on mismatched schemas.
_OUTPUT_SCHEMA: dict[str, Any] = {
    "symbol": pl.String,
    "asset_class": pl.String,
    "session": pl.Date,
    # Typed rather than text: expiry is half the contract key, and a provider
    # that formats it differently (IBKR emits YYYYMMDD) silently joins to
    # nothing when both sides are strings.
    "expiration": pl.Date,
    "strike": pl.Float64,
    "right": pl.String,
    "dte": pl.Int64,
    "tau": pl.Float64,
    "bid": pl.Float64,
    "ask": pl.Float64,
    "mid": pl.Float64,
    "spread": pl.Float64,
    "bid_size": pl.Int64,
    "ask_size": pl.Int64,
    "bid_exchange": pl.Int64,
    "ask_exchange": pl.Int64,
    "bid_condition": pl.Int64,
    "ask_condition": pl.Int64,
    "day_open": pl.Float64,
    "day_high": pl.Float64,
    "day_low": pl.Float64,
    "day_close": pl.Float64,
    "volume": pl.Int64,
    "trade_count": pl.Int64,
    "open_interest": pl.Int64,
    "forward": pl.Float64,
    "discount": pl.Float64,
    "parity_pairs": pl.Int64,
    "iv": pl.Float64,
    "model_price": pl.Float64,
    "delta": pl.Float64,
    "gamma": pl.Float64,
    "vega": pl.Float64,
    "theta": pl.Float64,
    "rho": pl.Float64,
    "vanna": pl.Float64,
    "charm": pl.Float64,
    "speed": pl.Float64,
    "zomma": pl.Float64,
    # Terminal-supplied, naive US/Eastern. OI is stamped pre-open because it's
    # the previous session's figure.
    "quote_timestamp": pl.Datetime("ms"),
    "ohlc_timestamp": pl.Datetime("ms"),
    "oi_timestamp": pl.Datetime("ms"),
    "last_trade_timestamp": pl.Datetime("ms"),
    "poll_timestamp": pl.Datetime("us", "UTC"),
}
_TIMESTAMP_COLS = ("quote_timestamp", "ohlc_timestamp", "oi_timestamp",
                   "last_trade_timestamp")


class ThetaDataError(RuntimeError):
    """Terminal reachable but the request failed."""


class ThetaPermissionError(ThetaDataError):
    """403 — subscription tier doesn't cover this endpoint or date range.

    Flagged fatal because retrying is pointless: without a plan change the next
    attempt fails identically. The poller checks this and stops rather than
    reprinting the same 403 for every symbol every interval.
    """

    fatal = True


def _get_csv(base_url: str, path: str, params: dict[str, Any], timeout: int) -> pl.DataFrame:
    url = f"{base_url}{path}"
    try:
        resp = requests.get(url, params={**params, "format": "csv"}, timeout=timeout)
    except requests.RequestException as exc:
        raise ThetaDataError(f"{path}: {exc}") from exc

    if resp.status_code == 403:
        raise ThetaPermissionError(f"{path}: {resp.text.strip()}")
    if resp.status_code == _NO_DATA:
        return pl.DataFrame()
    if resp.status_code != 200:
        raise ThetaDataError(f"{path}: HTTP {resp.status_code} {resp.text.strip()[:200]}")

    body = resp.text
    if not body.strip():
        return pl.DataFrame()
    return pl.read_csv(io.StringIO(body))


def _as_date(dtype: Any, name: str) -> pl.Expr:
    """Coerce a provider column to Date whether it arrived parsed or as text.

    `read_csv` leaves dates as strings unless asked otherwise, but that is an
    inference default rather than a guarantee, so both cases are handled.
    """
    col = pl.col(name)
    if dtype == pl.Date:
        return col
    if isinstance(dtype, pl.Datetime) or dtype == pl.Datetime:
        return col.dt.date()
    return col.cast(pl.String).str.to_date(strict=False)


def _normalize(df: pl.DataFrame, *, ts_col: str, ts_name: str, keep: dict[str, str],
                sessioned: bool) -> pl.DataFrame:
    """Rename to the output schema and drop rows that can't be keyed or joined."""
    if df.is_empty() or ts_col not in df.columns:
        return pl.DataFrame()

    df = df.filter(
        pl.col("right").is_in(["CALL", "PUT"])
        & pl.col("strike").is_not_null()
        & pl.col("expiration").is_not_null()
        & pl.col(ts_col).is_not_null()
    ).with_columns(
        pl.when(pl.col("right") == "CALL").then(pl.lit("C")).otherwise(pl.lit("P")).alias("right"),
        pl.col(ts_col).str.to_datetime(time_unit="ms", strict=False).alias(ts_name),
        _as_date(df.schema["expiration"], "expiration").alias("expiration"),
    ).filter(pl.col("expiration").is_not_null())

    keys = list(_JOIN_KEYS)
    if sessioned:
        # One history response spans many trading days, so the session is part of
        # the key. Taken from the record's own timestamp rather than the requested
        # range, so holidays and gaps take care of themselves.
        df = df.with_columns(pl.col(ts_name).dt.date().alias("session"))
        keys.append("session")

    selected = [*keys, ts_name]
    selected += [pl.col(src).alias(dst) for src, dst in keep.items() if src in df.columns]
    # A duplicate on the key would fan out the join.
    return df.select(selected).unique(subset=keys, keep="first")


def _conform(df: pl.DataFrame) -> pl.DataFrame:
    """Project onto _OUTPUT_SCHEMA, filling absent columns with typed nulls."""
    present = set(df.columns)
    return df.with_columns(
        [pl.lit(None, dtype).alias(name)
         for name, dtype in _OUTPUT_SCHEMA.items() if name not in present]
    ).select(
        [pl.col(name).cast(dtype) for name, dtype in _OUTPUT_SCHEMA.items()]
    )


def _forward_curve(pairs: pl.DataFrame, rate: float) -> pl.DataFrame:
    """Spread-weighted put-call-parity forward per (session, expiry).

    `pairs` needs one row per (session, expiration, strike) carrying both legs'
    mids and spreads, plus `tau`. Groups that can't be fitted are omitted, which
    leaves their IV and greeks null downstream rather than guessing a forward.
    """
    rows = []
    for (session, expiration), grp in pairs.group_by(["session", "expiration"],
                                                      maintain_order=True):
        tau = float(grp["tau"][0])
        if tau <= 0 or grp.height < _MIN_PARITY_PAIRS:
            continue

        strikes = grp["strike"].to_numpy().astype(float)
        basis = (grp["call_mid"] - grp["put_mid"]).to_numpy().astype(float)
        discount = float(np.exp(-rate * tau))

        # Rough forward first, purely to locate the money so the real fit can be
        # restricted to it. Least squares on everything is too noisy to trust for
        # the forward itself, but it's fine as a starting point.
        design = np.vstack([strikes, np.ones(strikes.size)]).T
        slope, intercept = np.linalg.lstsq(design, basis, rcond=None)[0]
        seed = intercept / -slope if slope < 0 else float(strikes[np.argmin(np.abs(basis))])

        band = np.abs(strikes - seed) < _ATM_BAND * seed
        if band.sum() < _MIN_PARITY_PAIRS:
            band = np.abs(strikes - seed) < _ATM_BAND_WIDE * seed
        if band.sum() < _MIN_PARITY_PAIRS:
            band = np.ones(strikes.size, dtype=bool)

        spread = (grp["call_spread"] + grp["put_spread"]).to_numpy().astype(float)
        weight = 1.0 / np.maximum(spread[band], 0.01)
        forward = float(np.sum(weight * (strikes[band] + basis[band] / discount)) / np.sum(weight))
        if not np.isfinite(forward) or forward <= 0:
            continue

        rows.append({
            "session": session,
            "expiration": expiration,
            "forward": forward,
            "discount": discount,
            "parity_pairs": int(band.sum()),
        })

    if not rows:
        return pl.DataFrame(schema={"session": pl.Date, "expiration": pl.Date,
                                     "forward": pl.Float64, "discount": pl.Float64,
                                     "parity_pairs": pl.Int64})
    return pl.DataFrame(rows)


def _black76(forward, strike, tau, vol, discount, is_call):
    """Vectorized Black-76. Puts come off parity so both wings stay consistent."""
    sd = vol * np.sqrt(tau)
    d1 = (np.log(forward / strike) + 0.5 * sd * sd) / sd
    d2 = d1 - sd
    call = discount * (forward * ndtr(d1) - strike * ndtr(d2))
    return np.where(is_call, call, call - discount * (forward - strike))


def _implied_vol(price, forward, strike, tau, discount, is_call):
    """Bisection on vol. Vectorized, so the iteration count is nearly free.

    Bisection over Newton deliberately: it can't diverge on the wide, barely-
    quoted contracts that make up most of a full chain, and 64 halvings of
    [1e-4, 5.0] is well past double precision anyway.
    """
    lo = np.full(price.shape, _IV_BOUNDS[0])
    hi = np.full(price.shape, _IV_BOUNDS[1])

    # Only prices strictly inside the model's own range are invertible; anything
    # at or outside the bounds has no finite IV (crossed/stale quote, or a mid
    # through intrinsic).
    solvable = (
        (price > _black76(forward, strike, tau, lo, discount, is_call))
        & (price < _black76(forward, strike, tau, hi, discount, is_call))
    )

    for _ in range(_IV_ITERATIONS):
        mid = 0.5 * (lo + hi)
        undervalued = _black76(forward, strike, tau, mid, discount, is_call) < price
        lo = np.where(undervalued, mid, lo)
        hi = np.where(undervalued, hi, mid)

    return np.where(solvable, 0.5 * (lo + hi), np.nan)


def _greeks(forward, strike, tau, vol, discount, is_call, rate):
    """Black-76 forward greeks, first through third order."""
    sqrt_t = np.sqrt(tau)
    sd = vol * sqrt_t
    d1 = (np.log(forward / strike) + 0.5 * sd * sd) / sd
    d2 = d1 - sd
    pdf = np.exp(-0.5 * d1 * d1) / np.sqrt(2.0 * np.pi)

    price = _black76(forward, strike, tau, vol, discount, is_call)
    delta = discount * np.where(is_call, ndtr(d1), ndtr(d1) - 1.0)
    gamma = discount * pdf / (forward * sd)
    vega = discount * forward * pdf * sqrt_t

    # d(price)/d(T) = -r*price + D*F*pdf*vol/(2*sqrt(T)); theta is its negative.
    theta = rate * price - discount * forward * pdf * vol / (2.0 * sqrt_t)
    d1_dt = -np.log(forward / strike) / (2.0 * vol * tau * sqrt_t) + vol / (4.0 * sqrt_t)

    return {
        "model_price": price,
        "delta": delta,
        "gamma": gamma,
        "vega": vega,
        "theta": theta / 365.25,
        "rho": -tau * price,
        "vanna": -discount * pdf * d2 / vol,
        "charm": (rate * delta - discount * pdf * d1_dt) / 365.25,
        "speed": -gamma / forward * (1.0 + d1 / sd),
        "zomma": gamma * (d1 * d2 - 1.0) / vol,
    }


def _enrich(df: pl.DataFrame, asof: pl.Expr) -> pl.DataFrame:
    """Time to expiry and tradeable mid — needed whether or not greeks are on.

    `asof` is an expression so realtime can pass the wall clock while history
    passes each row's own session close.
    """
    expiry_close = pl.col("expiration").cast(pl.Datetime("ms")) + timedelta(hours=16)
    df = df.with_columns(
        ((expiry_close - asof).dt.total_seconds() / _SECONDS_PER_YEAR).alias("tau"),
        (pl.col("expiration") - pl.col("session")).dt.total_days().alias("dte"),
    )

    # Snapshots still carry the previous session's expiry, and history can carry
    # an expiry that already settled. Both are dead: no forward, no IV, and they
    # would double-count in any OI or gamma aggregation. `max_dte` bounds only
    # the far end, so this is the near guard.
    df = df.filter(pl.col("dte") >= 0)

    quotable = (
        pl.col("bid").is_not_null() & pl.col("ask").is_not_null()
        & (pl.col("bid") > 0) & (pl.col("ask") >= pl.col("bid"))
    )
    return df.with_columns(
        pl.when(quotable).then((pl.col("bid") + pl.col("ask")) / 2).alias("mid"),
        pl.when(quotable).then(pl.col("ask") - pl.col("bid")).alias("spread"),
    )


def add_vol_surface(df: pl.DataFrame, *, rate: float) -> pl.DataFrame:
    """Attach forward, discount, IV and greeks. Unsolvable rows get nulls.

    Expects `_enrich` to have run, and a `session` column. Deep-ITM contracts
    routinely come back null: their mid sits at or inside intrinsic, so no vol
    reproduces it. That loses nothing — the same-strike OTM leg is quoted
    directly and does solve, and put-call parity makes the two equivalent.
    """
    legs = df.filter(pl.col("mid").is_not_null() & (pl.col("tau") > 0))
    pairs = (
        legs.filter(pl.col("right") == "C")
        .select("session", "expiration", "strike", "tau",
                call_mid="mid", call_spread="spread")
        .join(
            legs.filter(pl.col("right") == "P")
            .select("session", "expiration", "strike", put_mid="mid", put_spread="spread"),
            on=["session", "expiration", "strike"], how="inner",
        )
    )
    df = df.join(_forward_curve(pairs, rate), on=["session", "expiration"], how="left")

    surface_cols = ["iv", "model_price", "delta", "gamma", "vega", "theta", "rho",
                    "vanna", "charm", "speed", "zomma"]
    target = df.filter(
        pl.col("mid").is_not_null() & pl.col("forward").is_not_null()
        & (pl.col("tau") > 0) & (pl.col("strike") > 0)
    )
    if target.is_empty():
        return df.with_columns([pl.lit(None, pl.Float64).alias(c) for c in surface_cols])

    forward = target["forward"].to_numpy().astype(float)
    strike = target["strike"].to_numpy().astype(float)
    tau = target["tau"].to_numpy().astype(float)
    discount = target["discount"].to_numpy().astype(float)
    price = target["mid"].to_numpy().astype(float)
    is_call = (target["right"] == "C").to_numpy()

    iv = _implied_vol(price, forward, strike, tau, discount, is_call)
    surface = {"iv": iv}
    # Greeks are only meaningful where the inversion succeeded, and feeding NaN
    # vol into the formulas would raise divide-by-zero warnings.
    priced = np.isfinite(iv)
    safe_iv = np.where(priced, iv, 1.0)
    for name, values in _greeks(forward, strike, tau, safe_iv, discount, is_call, rate).items():
        surface[name] = np.where(priced, values, np.nan)

    # NaN -> null so unsolved rows read as missing rather than as a float that
    # quietly poisons every downstream mean.
    solved = target.select(_HISTORY_KEYS).with_columns(
        [pl.Series(name, values, dtype=pl.Float64).fill_nan(None)
         for name, values in surface.items()]
    )
    return df.join(solved, on=_HISTORY_KEYS, how="left")


# --------------------------------------------------------------------------
# Realtime snapshots
# --------------------------------------------------------------------------

def _snapshot(endpoint: str, symbol: str, settings, *, with_strike: bool) -> pl.DataFrame:
    params: dict[str, Any] = {"symbol": symbol, "expiration": "*", "right": "both"}
    if with_strike:
        # The open_interest endpoint rejects a strike filter; the others need it.
        params["strike"] = "*"
    if settings.thetadata_max_dte is not None:
        params["max_dte"] = settings.thetadata_max_dte
    if settings.thetadata_min_dte is not None:
        params["min_dte"] = settings.thetadata_min_dte
    if settings.thetadata_strike_range is not None:
        params["strike_range"] = settings.thetadata_strike_range
    return _get_csv(settings.thetadata_base_url, f"/v3/option/snapshot/{endpoint}",
                    params, settings.thetadata_timeout)


def get_chain_snapshot(instrument: Instrument, settings) -> pl.DataFrame:
    """poll_fn for volmagaddon.poller.run_loop: (instrument, settings) -> DataFrame.

    Three bulk calls (one per permitted endpoint) cover the whole chain for a
    root, so cost is per-underlying rather than per-contract. An empty frame
    means "nothing to store this poll", not an error.
    """
    symbol = instrument.symbol

    quotes = _normalize(
        _snapshot("quote", symbol, settings, with_strike=True),
        ts_col="timestamp", ts_name="quote_timestamp", sessioned=False,
        keep={"bid": "bid", "ask": "ask", "bid_size": "bid_size", "ask_size": "ask_size",
              "bid_exchange": "bid_exchange", "ask_exchange": "ask_exchange",
              "bid_condition": "bid_condition", "ask_condition": "ask_condition"},
    )
    if quotes.is_empty():
        return pl.DataFrame()

    ohlc = _normalize(
        _snapshot("ohlc", symbol, settings, with_strike=True),
        ts_col="timestamp", ts_name="ohlc_timestamp", sessioned=False,
        keep={"open": "day_open", "high": "day_high", "low": "day_low",
              "close": "day_close", "volume": "volume", "count": "trade_count"},
    )
    open_interest = _normalize(
        _snapshot("open_interest", symbol, settings, with_strike=False),
        ts_col="timestamp", ts_name="oi_timestamp", sessioned=False,
        keep={"open_interest": "open_interest"},
    )

    # Quotes are the spine: every listed contract has an NBBO, but only those
    # that printed today appear in ohlc, and OI lags by a session.
    df = quotes
    for side in (ohlc, open_interest):
        if not side.is_empty():
            df = df.join(side, on=_JOIN_KEYS, how="left")

    asof = datetime.now(NY).replace(tzinfo=None)  # Terminal timestamps are naive ET
    df = _enrich(df.with_columns(pl.lit(asof.date()).alias("session")), pl.lit(asof))
    if settings.thetadata_compute_greeks:
        df = add_vol_surface(df, rate=settings.thetadata_rate)

    return _conform(df.with_columns(
        pl.lit(instrument.kind).alias("asset_class"),
        pl.lit(datetime.now(timezone.utc)).alias("poll_timestamp"),
    ))


# --------------------------------------------------------------------------
# EOD history
# --------------------------------------------------------------------------

def month_chunks(start: date, end: date) -> Iterator[tuple[date, date]]:
    """Calendar-month ranges covering [start, end].

    A month is the unit of work for the backfill: big enough that per-request
    overhead disappears (throughput is ~4k rows/s either way), small enough that
    a failure costs one retry rather than a year, and it lines up with a natural
    resume marker.
    """
    cursor = max(start, HISTORY_START).replace(day=1)
    while cursor <= end:
        nxt = (cursor.replace(day=28) + timedelta(days=4)).replace(day=1)
        yield max(cursor, start), min(nxt - timedelta(days=1), end)
        cursor = nxt


def _history(endpoint: str, symbol: str, start: date, end: date, settings, *,
              with_strike: bool) -> pl.DataFrame:
    params: dict[str, Any] = {
        "symbol": symbol, "expiration": "*", "right": "both",
        "start_date": start.isoformat(), "end_date": end.isoformat(),
    }
    if with_strike:
        params["strike"] = "*"
    if settings.thetadata_history_max_dte is not None:
        params["max_dte"] = settings.thetadata_history_max_dte
    return _get_csv(settings.thetadata_base_url, f"/v3/option/history/{endpoint}",
                    params, settings.thetadata_history_timeout)


def get_eod_history(instrument: Instrument, start: date, end: date, settings) -> pl.DataFrame:
    """Closing NBBO + OHLCV + OI for every contract on every session in range.

    Two bulk calls cover the whole range for a root — `expiration=*` is honoured
    by the history endpoints, so a month of a full chain is one request. The vol
    surface is solved per session, so each day gets its own parity forward.
    """
    if end < HISTORY_START:
        return pl.DataFrame()
    start = max(start, HISTORY_START)

    eod = _normalize(
        _history("eod", instrument.symbol, start, end, settings, with_strike=True),
        ts_col="created", ts_name="quote_timestamp", sessioned=True,
        keep={"bid": "bid", "ask": "ask", "bid_size": "bid_size", "ask_size": "ask_size",
              "bid_exchange": "bid_exchange", "ask_exchange": "ask_exchange",
              "bid_condition": "bid_condition", "ask_condition": "ask_condition",
              "open": "day_open", "high": "day_high", "low": "day_low",
              "close": "day_close", "volume": "volume", "count": "trade_count",
              "last_trade": "last_trade_timestamp"},
    )
    if eod.is_empty():
        return pl.DataFrame()

    open_interest = _normalize(
        _history("open_interest", instrument.symbol, start, end, settings, with_strike=False),
        ts_col="timestamp", ts_name="oi_timestamp", sessioned=True,
        keep={"open_interest": "open_interest"},
    )
    df = eod if open_interest.is_empty() else eod.join(open_interest, on=_HISTORY_KEYS,
                                                        how="left")

    # last_trade arrives as a string in the `keep` passthrough, unlike the
    # timestamp columns _normalize parses.
    if df.schema.get("last_trade_timestamp") == pl.String:
        df = df.with_columns(
            pl.col("last_trade_timestamp").str.to_datetime(time_unit="ms", strict=False)
        )

    # At EOD the observation time *is* the close, so tau is whole days and a
    # same-day expiry lands at zero — already settled, hence no IV.
    session_close = pl.col("session").cast(pl.Datetime("ms")) + timedelta(hours=16)
    df = _enrich(df, session_close)
    if settings.thetadata_compute_greeks:
        df = add_vol_surface(df, rate=settings.thetadata_rate)

    return _conform(df.with_columns(
        pl.lit(instrument.kind).alias("asset_class"),
        pl.col("quote_timestamp").alias("ohlc_timestamp"),
        pl.lit(datetime.now(timezone.utc)).alias("poll_timestamp"),
    ))
