#!/usr/bin/env python
"""Entry point: poll IBKR's (narrowed) chain snapshot for the whole universe on a loop.

Run this in its own terminal/process, alongside run_thetadata_poller.py.
Requires TWS or IB Gateway already running and logged in with the API enabled.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from volmagaddon.config import get_settings
from volmagaddon.poller import run_loop
from volmagaddon.sources.ibkr import connect, get_chain_snapshot


def main() -> None:
    settings = get_settings()
    ib = connect(settings)
    try:
        run_loop(lambda instrument, s: get_chain_snapshot(ib, instrument, s), settings, source_name="ibkr")
    finally:
        ib.disconnect()


if __name__ == "__main__":
    main()
