#!/usr/bin/env python
"""Verify SURF-09 chain / GEX / OI output against formulas and the EOD lake.

    python scripts/test_surf09_tools.py

Synthetic tests always run. Lake tests skip if 2026-08-31 EOD is missing.
"""
from __future__ import annotations

import importlib.util
import json
import math
import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

_SPEC = importlib.util.spec_from_file_location("run_surf09", Path(__file__).with_name("run_surf09.py"))
surf = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(surf)

from volmagaddon.config import get_settings

EOD_DAY = date(2026, 8, 31)
EOD_EXPIRY = date(2026, 9, 30)


def _pair(*, strike, ce_oi=0.0, pe_oi=0.0, ce_g=0.0, pe_g=0.0, forward=None, call=None, put=None):
    ce_gex = ce_g * ce_oi * surf.SPX_MULTIPLIER
    pe_gex = pe_g * pe_oi * surf.SPX_MULTIPLIER
    return {
        "strike": strike,
        "forward": forward,
        "c": call,
        "p": put,
        "ce_oi": ce_oi,
        "pe_oi": pe_oi,
        "ce_gamma": ce_g,
        "pe_gamma": pe_g,
        "ce_gex": ce_gex,
        "pe_gex": pe_gex,
        "net_gex": ce_gex - pe_gex,
    }


class FormulaTests(unittest.TestCase):
    def test_gex_is_gamma_times_oi_times_100(self):
        row = _pair(strike=5000, ce_oi=10, pe_oi=4, ce_g=0.002, pe_g=0.001)
        self.assertAlmostEqual(row["ce_gex"], 0.002 * 10 * 100)
        self.assertAlmostEqual(row["pe_gex"], 0.001 * 4 * 100)
        self.assertAlmostEqual(row["net_gex"], row["ce_gex"] - row["pe_gex"])

    def test_max_pain_prefers_the_strike_that_minimizes_intrinsic_times_oi(self):
        pairs = [
            _pair(strike=10, ce_oi=1, pe_oi=0),
            _pair(strike=20, ce_oi=0, pe_oi=100),
        ]
        strike, pain = surf._max_pain(pairs)
        self.assertEqual(strike, 20)
        self.assertEqual(pain, 10.0)

    def test_gex_flip_is_the_sign_change_nearest_atm(self):
        pairs = [
            _pair(strike=90, ce_g=0.02, ce_oi=10),
            _pair(strike=100, pe_g=0.001, pe_oi=10),
            _pair(strike=110, pe_g=0.02, pe_oi=10),
        ]
        self.assertGreater(pairs[0]["net_gex"], 0)
        self.assertLess(pairs[1]["net_gex"], 0)
        self.assertEqual(surf._gex_flip(pairs, 100), 100)

    def test_summarize_pcr_and_json_roundtrip(self):
        pairs = [
            _pair(strike=100, ce_oi=50, pe_oi=75, ce_g=0.01, pe_g=0.01, forward=100,
                  call={"symbol": "SPX", "iv": 0.12, "gamma": 0.01, "open_interest": 50, "bid": 1, "ask": 1.1}),
            _pair(strike=110, ce_oi=10, pe_oi=20, ce_g=0.005, pe_g=0.006, forward=100,
                  put={"symbol": "SPXW", "iv": 0.14, "gamma": 0.006, "open_interest": 20, "bid": 2, "ask": 2.2}),
        ]
        out = surf._summarize_tools(pairs, expiry=date(2026, 9, 30), spot=99.0, band=0)
        self.assertAlmostEqual(out["pcr_oi"], 95 / 60)
        self.assertAlmostEqual(out["net_gex"], out["call_gex"] - out["put_gex"])
        self.assertAlmostEqual(out["net_gex_times_forward"], out["net_gex"] * 100)
        self.assertEqual(out["atm_strike"], 100)
        self.assertEqual(out["paired_strikes"], 2)
        blob = json.dumps(out, default=str, allow_nan=False)
        back = json.loads(blob)
        self.assertEqual(back["expiry"], "2026-09-30")
        self.assertEqual(len(back["chain"]), 2)
        self.assertEqual(back["chain"][0]["call"]["iv"], 0.12)

    def test_band_keeps_strikes_near_forward(self):
        pairs = [
            _pair(strike=50, forward=100, ce_oi=1),
            _pair(strike=100, forward=100, ce_oi=1),
            _pair(strike=200, forward=100, ce_oi=1),
        ]
        out = surf._summarize_tools(pairs, expiry=date(2026, 9, 30), spot=100, band=0.15)
        self.assertEqual(out["paired_strikes"], 3)
        self.assertEqual(out["returned_strikes"], 1)
        self.assertEqual(out["chain"][0]["strike"], 100)


@unittest.skipUnless(
    (get_settings().data_dir / "thetadata_eod" / "SPX" / f"dt={EOD_DAY.isoformat()}" / "eod.parquet").exists()
    and (get_settings().data_dir / "thetadata_eod" / "SPXW" / f"dt={EOD_DAY.isoformat()}" / "eod.parquet").exists(),
    "2026-08-31 EOD lake not present",
)
class LakeOutputTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data_dir = get_settings().data_dir
        cls.payload = surf.api_tools(cls.data_dir, {
            "session": EOD_DAY.isoformat(),
            "source": "eod",
            "dte": "30",
        })

    def test_session_and_expiry_match_the_request(self):
        self.assertTrue(self.payload["ok"])
        self.assertEqual(self.payload["session"], EOD_DAY.isoformat())
        self.assertEqual(self.payload["source"], "eod")
        self.assertEqual(self.payload["expiry"], EOD_EXPIRY.isoformat())

    def test_golden_totals_from_2026_08_31(self):
        """Pinned to the first validated SURF-09 tools run on this lake."""
        self.assertEqual(self.payload["paired_strikes"], 599)
        self.assertEqual(self.payload["max_pain"], 7575)
        self.assertAlmostEqual(self.payload["net_gex"], 718.8441138542912, places=4)
        self.assertAlmostEqual(self.payload["pcr_oi"], 1.1538163012969198, places=6)
        self.assertGreater(self.payload["call_oi"], 0)
        self.assertGreater(self.payload["put_oi"], 0)

    def test_totals_are_sums_of_the_chain(self):
        chain = self.payload["chain"]
        self.assertEqual(len(chain), self.payload["returned_strikes"])
        self.assertAlmostEqual(sum(r["call_gex"] for r in chain), self.payload["call_gex"], places=6)
        self.assertAlmostEqual(sum(r["put_gex"] for r in chain), self.payload["put_gex"], places=6)
        self.assertAlmostEqual(self.payload["net_gex"], self.payload["call_gex"] - self.payload["put_gex"], places=9)
        self.assertAlmostEqual(self.payload["pcr_oi"], self.payload["put_oi"] / self.payload["call_oi"], places=9)
        if self.payload["forward"]:
            self.assertAlmostEqual(
                self.payload["net_gex_times_forward"],
                self.payload["net_gex"] * self.payload["forward"],
                places=4,
            )

    def test_each_strike_gex_matches_lake_gamma_and_oi(self):
        raw = surf._read_eod(self.data_dir, EOD_DAY)
        pairs = surf._pair_expiry(raw, EOD_EXPIRY)
        by_k = {p["strike"]: p for p in pairs}
        checked = 0
        for row in self.payload["chain"]:
            src = by_k[row["strike"]]
            self.assertAlmostEqual(row["call_gex"], src["ce_gamma"] * src["ce_oi"] * 100, places=8)
            self.assertAlmostEqual(row["put_gex"], src["pe_gamma"] * src["pe_oi"] * 100, places=8)
            self.assertAlmostEqual(row["net_gex"], row["call_gex"] - row["put_gex"], places=8)
            checked += 1
        self.assertEqual(checked, 599)

    def test_atm_row_is_nearest_forward_and_has_iv(self):
        atm = [r for r in self.payload["chain"] if r["atm"]]
        self.assertEqual(len(atm), 1)
        fwd = self.payload["forward"]
        self.assertIsNotNone(fwd)
        nearest = min(self.payload["chain"], key=lambda r: abs(r["strike"] - fwd))
        self.assertEqual(atm[0]["strike"], nearest["strike"])
        call = atm[0]["call"]
        put = atm[0]["put"]
        self.assertIsNotNone(call)
        self.assertIsNotNone(put)
        self.assertGreater(call["iv"], 0)
        self.assertGreater(put["iv"], 0)
        self.assertLess(abs(call["iv"] - put["iv"]), 0.01)

    def test_max_pain_is_a_listed_strike(self):
        strikes = {r["strike"] for r in self.payload["chain"]}
        self.assertIn(self.payload["max_pain"], strikes)

    def test_payload_is_strict_json(self):
        blob = json.dumps(self.payload, default=str, allow_nan=False)
        self.assertGreater(len(blob), 1000)
        self.assertNotIn("NaN", blob)

    def test_meta_lists_the_session_and_default_expiry(self):
        meta = surf.api_meta(self.data_dir, {"source": "eod", "session": EOD_DAY.isoformat()})
        self.assertIn(EOD_DAY.isoformat(), meta["eod_sessions"])
        self.assertEqual(meta["default_expiry"], EOD_EXPIRY.isoformat())
        self.assertTrue(any(x["expiry"] == EOD_EXPIRY.isoformat() for x in meta["expiries"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
