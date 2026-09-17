#!/usr/bin/env python
"""Entry point: poll ThetaData's full chain snapshot for the whole universe on a loop.

Run this in its own terminal/process, alongside run_ibkr_poller.py.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from volmagaddon.config import get_settings
from volmagaddon.poller import run_loop
from volmagaddon.sources.thetadata import get_chain_snapshot


def main() -> None:
    settings = get_settings()
    run_loop(get_chain_snapshot, settings, source_name="thetadata",
             instruments=settings.thetadata_instruments)


if __name__ == "__main__":
    main()
