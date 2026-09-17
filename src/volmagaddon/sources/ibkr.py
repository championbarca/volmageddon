"""IBKR chain snapshot via ib_async — TWS/IB Gateway must already be running.

IBKR is the secondary/cross-check source here, not the primary chain feed:
ThetaData's single-call full-chain endpoint is cheaper and faster to poll at
intraday cadence than IBKR's per-contract market data lines. This module
deliberately narrows the request to the nearest `ibkr_max_expiries`
expirations and `ibkr_strikes_per_side` strikes either side of spot to stay
well clear of IBKR's pacing/market-data-line limits — widen those two knobs
in .env once you've confirmed your account's limits handle it.

Index underlyings (SPX, NDX, VIX) need a different IBKR contract type (Index,
not Stock) and their options aren't SMART-routed the way equity options are —
they're single-listed, mostly at CBOE. `_INDEX_QUOTE_EXCHANGE` below is a
best-effort default for resolving the index's own quote; the *option* chain's
exchange is always taken from whatever reqSecDefOptParams actually returns
(chain.exchange) rather than hardcoded, since guessing that wrong is exactly
what caused the SPY/IWM "no security definition" spam this module used to hit.
If a specific index errors on the quote step, the fix is almost always this
exchange mapping — check what your IBKR market data permissions actually list
it under and adjust.

## Making rows joinable to ThetaData

This source only earns its keep as a cross-check, which means every row has to
join to the ThetaData row for the same contract. Two things blocked that:

- IBKR formats the expiry as `YYYYMMDD` where ThetaData uses a real date, and
  for some ETF options it reports the *Saturday* after the final session — the
  pre-2015 OCC convention. Both feeds are normalized to a `Date` holding the
  last trading day, with IBKR's provider strings kept alongside.
- IBKR signals "no data" as either NaN or -1 depending on field and feed, and
  a column that happened to be entirely absent for one poll was written as
  Null dtype, which then refuses to concat with the Float64 written by the
  next. Hence the pinned output schema below.
"""
from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

import polars as pl
from ib_async import IB, ContFuture, Future, Index, Option, Stock

from ..config import Instrument

NY = ZoneInfo("America/New_York")

_INDEX_QUOTE_EXCHANGE = {
    "SPX": "CBOE",
    "VIX": "CBOE",
    "NDX": "NASDAQ",
}

_FUTURE_EXCHANGE = {
    "ES": "CME",
    "NQ": "CME",
    "VX": "CFE",
}

# IBKR's continuous-future root is not always the OPRA/ticker we store.
# VX futures are listed as symbol VIX on CFE (trading class VX).
_FUTURE_IB_SYMBOL = {
    "VX": "VIX",
}

_FUTURE_MULTIPLIER = {
    "ES": "50",
    "NQ": "20",
    "VX": "1000",
}

# Quarterly CME months. VX is monthly.
_QUARTERLY_MONTHS = {3, 6, 9, 12}

# Written on every poll, so a missing field becomes a typed null instead of
# changing the file's schema. Mirrors thetadata's `symbol/session/expiration/
# strike/right` key exactly so the two sources join without translation.
_OUTPUT_SCHEMA: dict[str, Any] = {
    "symbol": pl.String,
    "asset_class": pl.String,
    "session": pl.Date,
    "expiration": pl.Date,
    # Provider-native values, retained so the normalization above stays
    # auditable and reversible rather than being a silent rewrite.
    "expiration_raw": pl.String,
    "real_expiration": pl.Date,
    "strike": pl.Float64,
    "right": pl.String,
    "bid": pl.Float64,
    "ask": pl.Float64,
    "last": pl.Float64,
    "volume": pl.Float64,
    "underlying_price": pl.Float64,
    "delta": pl.Float64,
    "gamma": pl.Float64,
    "vega": pl.Float64,
    "theta": pl.Float64,
    "implied_vol": pl.Float64,
    "opt_price": pl.Float64,
    "poll_timestamp": pl.Datetime("us", "UTC"),
}


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema=_OUTPUT_SCHEMA)


def _num(value) -> float | None:
    """NaN -> null. For fields where a negative value is legitimate."""
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return None if v != v else v


def _price(value) -> float | None:
    """NaN and negatives -> null.

    For prices, sizes and volumes, where negative is impossible and IBKR's -1
    "no data" sentinel would otherwise be indistinguishable from a real number
    once it is sitting in a Float64 column.
    """
    v = _num(value)
    return None if v is None or v < 0 else v


def _parse_ib_date(raw: str | None) -> date | None:
    """IBKR dates are YYYYMMDD, occasionally with a time appended."""
    if not raw:
        return None
    try:
        return datetime.strptime(raw.strip()[:8], "%Y%m%d").date()
    except ValueError:
        return None


def _last_trading_day(d: date | None) -> date | None:
    """Roll a weekend expiry back to the Friday session that actually traded.

    A Saturday expiration is the legacy OCC convention for a contract whose
    final session was the preceding Friday, which is how OPRA and ThetaData key
    it. Rolling back is what makes the two sides comparable.
    """
    if d is None or d.weekday() < 5:
        return d
    return d - timedelta(days=d.weekday() - 4)


def connect(settings) -> IB:
    ib = IB()
    ib.connect(settings.ibkr_host, settings.ibkr_port, clientId=settings.ibkr_client_id)
    return ib


def _nearest_strikes(strikes: list[float], spot: float, n_per_side: int) -> list[float]:
    ordered = sorted(strikes, key=lambda k: abs(k - spot))
    return sorted(ordered[: n_per_side * 2])


def _underlying_contract(instrument: Instrument):
    if instrument.kind == "index":
        exchange = _INDEX_QUOTE_EXCHANGE.get(instrument.symbol, "CBOE")
        return Index(instrument.symbol, exchange, "USD")
    if instrument.kind == "future":
        exchange = _FUTURE_EXCHANGE.get(instrument.symbol, "CME")
        ib_symbol = _FUTURE_IB_SYMBOL.get(instrument.symbol, instrument.symbol)
        # ContFuture(symbol, exchange, localSymbol, ...) — currency is keyword-only
        # in the 3rd positional slot. Passing "USD" there made localSymbol=USD
        # and IBKR returned Error 200 for VX.
        return ContFuture(ib_symbol, exchange, currency="USD")
    return Stock(instrument.symbol, "SMART", "USD")


_BAR_SCHEMA: dict[str, Any] = {
    "symbol": pl.String,
    "asset_class": pl.String,
    "session": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "average": pl.Float64,
    "bar_count": pl.Int64,
    "bar_size": pl.String,
}


def get_underlying_history(ib: IB, instrument: Instrument, *,
                            end: datetime | date | str | None = None,
                            duration: str = "1 Y",
                            bar_size: str = "1 day") -> pl.DataFrame:
    """Daily (or other) bars for the underlying itself — not the option chain.

    ThetaData stock/index history is gated on this account, so IBKR is the
    only route to a long underlying series (`DATA-08`). One request per call;
    the backfill script chunks by year to stay inside IBKR duration limits.
    """
    contract = _underlying_contract(instrument)
    ib.qualifyContracts(contract)

    # Error 10339: IBKR forbids endDateTime on ContFuture and then drops the
    # socket — the backfill looked "not connected" after the first ES year.
    if instrument.kind == "future" or end is None:
        end_str = ""
    elif isinstance(end, datetime):
        end_str = end.strftime("%Y%m%d %H:%M:%S")
    elif isinstance(end, date):
        end_str = end.strftime("%Y%m%d 16:00:00")
    else:
        end_str = end

    bars = ib.reqHistoricalData(
        contract,
        endDateTime=end_str,
        durationStr=duration,
        barSizeSetting=bar_size,
        whatToShow="TRADES",
        useRTH=False if instrument.kind == "future" and instrument.symbol == "VX" else True,
        formatDate=1,
    )
    if not bars:
        return pl.DataFrame(schema=_BAR_SCHEMA)

    rows = []
    for b in bars:
        raw = b.date
        if isinstance(raw, datetime):
            session = raw.date()
        elif isinstance(raw, date):
            session = raw
        else:
            session = _parse_ib_date(str(raw)[:8])
        rows.append({
            "symbol": instrument.symbol,
            "asset_class": instrument.kind,
            "session": session,
            "open": _price(b.open),
            "high": _price(b.high),
            "low": _price(b.low),
            "close": _price(b.close),
            "volume": _price(b.volume),
            "average": _price(getattr(b, "average", None)),
            "bar_count": int(b.barCount) if getattr(b, "barCount", None) not in (None, -1) else None,
            "bar_size": bar_size,
        })
    return pl.DataFrame(rows, schema=_BAR_SCHEMA)


def _bars_to_frame(instrument: Instrument, bars, bar_size: str) -> pl.DataFrame:
    if not bars:
        return pl.DataFrame(schema=_BAR_SCHEMA)
    rows = []
    for b in bars:
        raw = b.date
        if isinstance(raw, datetime):
            session = raw.date()
        elif isinstance(raw, date):
            session = raw
        else:
            session = _parse_ib_date(str(raw)[:8])
        rows.append({
            "symbol": instrument.symbol,
            "asset_class": instrument.kind,
            "session": session,
            "open": _price(b.open),
            "high": _price(b.high),
            "low": _price(b.low),
            "close": _price(b.close),
            "volume": _price(b.volume),
            "average": _price(getattr(b, "average", None)),
            "bar_count": int(b.barCount) if getattr(b, "barCount", None) not in (None, -1) else None,
            "bar_size": bar_size,
        })
    return pl.DataFrame(rows, schema=_BAR_SCHEMA)


def _third_friday(year: int, month: int) -> date:
    d = date(year, month, 1)
    return d + timedelta(days=(4 - d.weekday()) % 7 + 14)


def _vx_last_trade(year: int, month: int) -> date:
    """VIX futures expire Wednesday, 30 days before the next month's 3rd Friday."""
    if month == 12:
        return _third_friday(year + 1, 1) - timedelta(days=30)
    return _third_friday(year, month + 1) - timedelta(days=30)


def _candidate_future_expiries(symbol: str, start: date, end: date) -> list[date]:
    """Last-trade dates that could have been front-month in [start, end]."""
    y, m = start.year, start.month - 4
    while m <= 0:
        y -= 1
        m += 12
    end_y, end_m = end.year, end.month + 3
    while end_m > 12:
        end_y += 1
        end_m -= 12
    quarterly = symbol in ("ES", "NQ")
    out: list[date] = []
    cy, cm = y, m
    while (cy, cm) <= (end_y, end_m):
        if not quarterly or cm in _QUARTERLY_MONTHS:
            out.append(_vx_last_trade(cy, cm) if symbol == "VX" else _third_friday(cy, cm))
        cm += 1
        if cm > 12:
            cm = 1
            cy += 1
    return out


def _resolve_dated_future(ib: IB, ib_symbol: str, expiry: date, exchanges: list[str],
                           *, multiplier: str = "", trading_class: str = ""):
    """Ask IBKR for one expired/listed future. Empty details = not on this account."""
    for exchange in exchanges:
        for delta in (0, 1):
            day = expiry - timedelta(days=delta)
            fut = Future(
                ib_symbol, day.strftime("%Y%m%d"), exchange,
                currency="USD", includeExpired=True,
            )
            if multiplier:
                fut.multiplier = multiplier
            if trading_class:
                fut.tradingClass = trading_class
            details = ib.reqContractDetails(fut)
            if details:
                return details[0].contract
    return None


def _list_dated_futures(ib: IB, instrument: Instrument, start: date, end: date,
                         log: Callable[[str], None]) -> list:
    """Listed (including expired) dated futures IBKR will actually resolve.

    ContFuture is truncated on this account (ES from 2023, NQ from 2024, VX
    almost empty). Dated contracts with includeExpired are the real series.
    Months IBKR does not list are skipped — nothing is invented.
    """
    ib_symbol = _FUTURE_IB_SYMBOL.get(instrument.symbol, instrument.symbol)
    primary = _FUTURE_EXCHANGE.get(instrument.symbol, "CME")
    exchanges = [primary]
    if instrument.symbol in ("ES", "NQ") and "GLOBEX" not in exchanges:
        exchanges.append("GLOBEX")

    probe_start = max(start, end - timedelta(days=550))
    expiries = [e for e in _candidate_future_expiries(instrument.symbol, start, end)
                if e >= probe_start]
    log(f"{instrument.symbol}: probing {len(expiries)} dated expiries "
        f"(from {probe_start}) on {exchanges}")
    probed = []
    seen: set[int] = set()
    missing = 0
    for expiry in expiries:
        try:
            contract = _resolve_dated_future(
                ib, ib_symbol, expiry, exchanges,
                multiplier=_FUTURE_MULTIPLIER.get(instrument.symbol, ""),
                trading_class="VX" if instrument.symbol == "VX" else instrument.symbol,
            )
        except Exception as exc:  # noqa: BLE001 — missing month is expected
            log(f"{instrument.symbol} {expiry}: skip {exc!r}")
            missing += 1
            continue
        if contract is None or not contract.conId or contract.conId in seen:
            missing += 1
            time.sleep(0.15)
            continue
        seen.add(contract.conId)
        probed.append(contract)
        time.sleep(0.15)
    probed.sort(key=lambda c: c.lastTradeDateOrContractMonth or "")
    log(f"{instrument.symbol}: resolved {len(probed)} dated contracts, {missing} not listed")
    return probed


def _req_future_bars(ib: IB, contract, duration: str, bar_size: str, end_str: str,
                     *, use_rth: bool):
    kwargs = dict(
        endDateTime=end_str,
        durationStr=duration,
        barSizeSetting=bar_size,
        whatToShow="TRADES",
        useRTH=use_rth,
        formatDate=1,
    )
    try:
        return ib.reqHistoricalData(contract, **kwargs)
    except Exception as exc:
        text = str(exc).lower()
        if "162" in str(exc) or "pacing" in text:
            time.sleep(15)
            return ib.reqHistoricalData(contract, **kwargs)
        raise


def get_dated_future_history(ib: IB, instrument: Instrument, *,
                              start: date, end: date,
                              bar_size: str = "1 day",
                              log: Callable[[str], None] | None = None,
                              ensure: Callable[[], None] | None = None) -> pl.DataFrame:
    """Front-month stitch of dated futures. Prices are IBKR prints, not a continuous formula.

    For each session the nearest contract whose last trade date is on or after
    that session is kept. Sessions IBKR has no contract for are omitted.
    """
    log = log or (lambda _m: None)
    if ensure:
        ensure()
    contracts = _list_dated_futures(ib, instrument, start, end, log)
    if not contracts:
        return pl.DataFrame(schema=_BAR_SCHEMA)

    frames = []
    for contract in contracts:
        expiry = _parse_ib_date(contract.lastTradeDateOrContractMonth)
        label = contract.lastTradeDateOrContractMonth or "?"
        if expiry is None:
            continue
        end_str = expiry.strftime("%Y%m%d 16:00:00")
        if ensure:
            ensure()
        elif not ib.isConnected():
            raise ConnectionError("IBKR socket dropped mid-futures backfill")
        try:
            bars = _req_future_bars(
                ib, contract, "1 Y", bar_size, end_str,
                use_rth=instrument.symbol != "VX",
            )
        except Exception as exc:  # noqa: BLE001 — one expiry must not kill the rest
            log(f"{instrument.symbol} {label}: ERROR {exc!r}")
            time.sleep(2)
            continue
        df = _bars_to_frame(instrument, bars, bar_size)
        if df.is_empty():
            log(f"{instrument.symbol} {label}: no data")
            time.sleep(2)
            continue
        df = df.filter(
            (pl.col("session") >= start) & (pl.col("session") <= end) & (pl.col("session") <= expiry)
        )
        if df.is_empty():
            log(f"{instrument.symbol} {label}: bars outside window")
            time.sleep(2)
            continue
        frames.append(df.with_columns(pl.lit(expiry).alias("_expiry")))
        log(f"{instrument.symbol} {label}: {df.height:>5} bars  "
            f"{df['session'].min()}..{df['session'].max()}")
        time.sleep(2.5)

    if not frames:
        return pl.DataFrame(schema=_BAR_SCHEMA)

    stacked = pl.concat(frames, how="vertical")
    front = (
        stacked.filter(pl.col("_expiry") >= pl.col("session"))
        .sort(["session", "_expiry"])
        .unique(subset=["session"], keep="first")
        .drop("_expiry")
        .sort("session")
    )
    log(f"{instrument.symbol}: stitched {front.height} front-month sessions "
        f"{front['session'].min()}..{front['session'].max()}")
    return front.select(list(_BAR_SCHEMA))


def _select_chain(chains, instrument: Instrument):
    symbol = instrument.symbol
    if instrument.kind == "equity":
        chain = next((c for c in chains if c.exchange == "SMART" and c.tradingClass == symbol), None)
        return chain or next((c for c in chains if c.exchange == "SMART"), None)
    # Index options aren't SMART-routed — take whichever entry's trading class
    # matches the index itself, regardless of which exchange it's listed under.
    chain = next((c for c in chains if c.tradingClass == symbol), None)
    return chain or (chains[0] if chains else None)


def get_chain_snapshot(ib: IB, instrument: Instrument, settings) -> pl.DataFrame:
    symbol = instrument.symbol
    underlying = _underlying_contract(instrument)
    ib.qualifyContracts(underlying)

    [spot_ticker] = ib.reqTickers(underlying)
    spot = spot_ticker.marketPrice()
    if spot != spot and spot_ticker.close:  # marketPrice() is NaN outside RTH sometimes
        spot = spot_ticker.close
    if not spot or spot != spot:
        return _empty()

    chains = ib.reqSecDefOptParams(underlying.symbol, "", underlying.secType, underlying.conId)
    chain = _select_chain(chains, instrument)
    if chain is None:
        return _empty()

    expiries = sorted(chain.expirations)[: settings.ibkr_max_expiries]

    # reqSecDefOptParams' strike/expiration lists are each the UNION across the other
    # dimension — they don't promise every strike is listed for every expiry (weeklies
    # vs monthlies especially often differ). Building a blind strike x expiry x right
    # cross-product from those two lists floods IB's error log with "no security
    # definition" for combos that were never listed. Ask per-expiry instead: this
    # returns only contracts that actually exist, already fully qualified (conId set).
    qualified = []
    # IBKR's own view of when each contract really expires, keyed by conId.
    # Only ContractDetails carries it; the Contract on the ticker does not.
    real_expiry: dict[int, str] = {}
    for exp in expiries:
        probe = Option(symbol, exp, exchange=chain.exchange, currency="USD",
                        tradingClass=chain.tradingClass)
        details = ib.reqContractDetails(probe)
        if not details:
            continue
        available_strikes = sorted({d.contract.strike for d in details})
        near_strikes = _nearest_strikes(available_strikes, spot, settings.ibkr_strikes_per_side)
        by_key = {(d.contract.strike, d.contract.right): d.contract for d in details}
        real_expiry.update({d.contract.conId: getattr(d, "realExpirationDate", "")
                            for d in details})
        for strike in near_strikes:
            for right in ("C", "P"):
                c = by_key.get((strike, right))
                if c is not None:
                    qualified.append(c)

    if not qualified:
        return _empty()

    tickers = ib.reqTickers(*qualified)
    poll_ts = datetime.now(timezone.utc)
    session = datetime.now(NY).date()

    rows = []
    for t in tickers:
        c = t.contract
        mg = t.modelGreeks
        raw_expiry = c.lastTradeDateOrContractMonth
        rows.append({
            "symbol": symbol,
            "asset_class": instrument.kind,
            "session": session,
            "expiration": _last_trading_day(_parse_ib_date(raw_expiry)),
            "expiration_raw": raw_expiry or None,
            "real_expiration": _parse_ib_date(real_expiry.get(c.conId)),
            "strike": _price(c.strike),
            "right": (c.right or "")[:1].upper() or None,
            "bid": _price(t.bid),
            "ask": _price(t.ask),
            "last": _price(t.last),
            "volume": _price(t.volume),
            "underlying_price": _price(mg.undPrice if mg else spot),
            "delta": _num(mg.delta if mg else None),
            "gamma": _num(mg.gamma if mg else None),
            "vega": _num(mg.vega if mg else None),
            "theta": _num(mg.theta if mg else None),
            "implied_vol": _num(mg.impliedVol if mg else None),
            "opt_price": _price(mg.optPrice if mg else None),
            "poll_timestamp": poll_ts,
        })

    return pl.DataFrame(rows, schema=_OUTPUT_SCHEMA) if rows else _empty()
