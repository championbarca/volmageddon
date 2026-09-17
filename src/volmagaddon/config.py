"""Loads .env + config/universe.yaml into a single Settings object.

Nothing fancy on purpose — Phase 1 is meant to be easy to read and change, not
a framework. Reach for pydantic-settings later if this file starts feeling cramped.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()


def _int(name: str, default: int) -> int:
    val = os.getenv(name)
    return int(val) if val not in (None, "") else default


def _opt_int(name: str) -> int | None:
    val = os.getenv(name)
    return int(val) if val not in (None, "") else None


def _float(name: str, default: float) -> float:
    val = os.getenv(name)
    return float(val) if val not in (None, "") else default


def _bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val in (None, ""):
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Instrument:
    symbol: str
    kind: str  # "equity", "index", or "future" — drives IBKR contract type + routing


@dataclass
class Settings:
    # IBKR
    ibkr_host: str = os.getenv("IBKR_HOST", "127.0.0.1")
    ibkr_port: int = _int("IBKR_PORT", 7497)
    ibkr_client_id: int = _int("IBKR_CLIENT_ID", 17)
    ibkr_max_expiries: int = _int("IBKR_MAX_EXPIRIES", 4)
    ibkr_strikes_per_side: int = _int("IBKR_STRIKES_PER_SIDE", 10)

    # ThetaData
    thetadata_base_url: str = os.getenv("THETADATA_BASE_URL", "http://127.0.0.1:25503")
    thetadata_max_dte: int | None = _opt_int("THETADATA_MAX_DTE")
    thetadata_min_dte: int | None = _opt_int("THETADATA_MIN_DTE")
    thetadata_strike_range: int | None = _opt_int("THETADATA_STRIKE_RANGE")
    thetadata_timeout: int = _int("THETADATA_TIMEOUT", 60)
    # A month of a full chain is ~320k rows at ~4k rows/s, so history needs a far
    # longer ceiling than a snapshot.
    thetadata_history_timeout: int = _int("THETADATA_HISTORY_TIMEOUT", 900)
    # Unset pulls every listed expiry, which is what the surface history wants;
    # set it to trade completeness for speed and disk.
    thetadata_history_max_dte: int | None = _opt_int("THETADATA_HISTORY_MAX_DTE")
    # Only used for the discount factor — the forward comes from put-call parity,
    # so dividends and borrow are already baked in. See sources/thetadata.py.
    thetadata_rate: float = _float("THETADATA_RATE", 0.043)
    thetadata_compute_greeks: bool = _bool("THETADATA_COMPUTE_GREEKS", True)

    # Shared
    data_dir: Path = Path(os.getenv("DATA_DIR", "./data"))
    universe_file: Path = Path(os.getenv("UNIVERSE_FILE", "./config/universe.yaml"))
    poll_interval_seconds: int = _int("POLL_INTERVAL_SECONDS", 60)

    equities: list[str] = field(default_factory=list)
    indices: list[str] = field(default_factory=list)
    theta_extra_roots: list[str] = field(default_factory=list)
    futures: list[str] = field(default_factory=list)
    instruments: list[Instrument] = field(default_factory=list, init=False)
    thetadata_instruments: list[Instrument] = field(default_factory=list, init=False)
    bar_instruments: list[Instrument] = field(default_factory=list, init=False)

    def __post_init__(self) -> None:
        if not self.equities and not self.indices:
            loaded = load_universe(self.universe_file)
            self.equities = loaded["equities"]
            self.indices = loaded["indices"]
            self.theta_extra_roots = loaded["theta_extra_roots"]
            self.futures = loaded["futures"]
        self.instruments = (
            [Instrument(s, "equity") for s in self.equities]
            + [Instrument(s, "index") for s in self.indices]
        )
        # ThetaData keys options by OPRA root, which isn't always the underlying's
        # symbol — SPX weeklies list under SPXW and hold most of the SPX volume.
        # IBKR treats those as a trading class under SPX, so they'd be invalid as
        # an IBKR symbol; they only go to the ThetaData poller.
        self.thetadata_instruments = self.instruments + [
            Instrument(s, "index") for s in self.theta_extra_roots
        ]
        # Daily/intraday bars: cash underlyings plus listed futures. Not used by
        # the option pollers.
        self.bar_instruments = self.instruments + [
            Instrument(s, "future") for s in self.futures
        ]
        self.data_dir.mkdir(parents=True, exist_ok=True)


def load_universe(path: Path) -> dict[str, list[str]]:
    with open(path) as f:
        cfg = yaml.safe_load(f)
    return {
        "equities": [s.upper() for s in cfg.get("underlyings", [])],
        "indices": [s.upper() for s in cfg.get("indices", [])],
        "theta_extra_roots": [s.upper() for s in cfg.get("thetadata_extra_roots") or []],
        "futures": [s.upper() for s in cfg.get("futures") or []],
    }


def get_settings() -> Settings:
    return Settings()
