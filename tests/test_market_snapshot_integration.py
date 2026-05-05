import os
import tempfile
import time
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

_TMP_DIR = tempfile.TemporaryDirectory()
os.environ["TRADE_DASHBOARD_DB_PATH"] = os.path.join(_TMP_DIR.name, "test_trade_dashboard.db")

from app.kite_engine import MarketEngine
import app.main as main_module


class FakeKite:
    def __init__(self, snapshots):
        self.snapshots = list(snapshots)
        self.calls = 0

    def quote(self, symbols):
        snapshot = self.snapshots[min(self.calls, len(self.snapshots) - 1)]
        self.calls += 1
        return {
            f"NSE:{sector}": {
                "ohlc": {"close": close},
                "last_price": price,
            }
            for sector, (price, close) in snapshot.items()
        }


def make_snapshot(sector_price):
    return {
        "gainers": [
            {
                "symbol": "ABC",
                "name": "ABC",
                "price": 101.25,
                "change": 1.25,
                "is_fno": False,
                "sectors": [],
            }
        ],
        "losers": [
            {
                "symbol": "XYZ",
                "name": "XYZ",
                "price": 98.75,
                "change": -0.85,
                "is_fno": False,
                "sectors": [],
            }
        ],
        "sectors": [
            {"sector": "NIFTY IT", "price": sector_price, "change": 1.31},
            {"sector": "NIFTY PSU BANK", "price": 8441.5, "change": -0.17},
        ],
        "sector_gainers": [
            {"sector": "NIFTY IT", "price": sector_price, "change": 1.31}
        ],
        "sector_losers": [
            {"sector": "NIFTY PSU BANK", "price": 8441.5, "change": -0.17}
        ],
        "updated_at": "2026-05-05T09:54:00+05:30",
        "connected": True,
        "error": None,
        "market_open": True,
        "snapshot_source": "api_sector",
    }


class MarketEngineSectorRefreshTests(unittest.TestCase):
    def test_sector_snapshot_refreshes_on_repeated_live_snapshot_requests(self):
        fake_kite = FakeKite(
            [
                {
                    "NIFTY IT": (8633.4, 8521.8),
                    "NIFTY PSU BANK": (8441.5, 8455.9),
                },
                {
                    "NIFTY IT": (8640.45, 8521.8),
                    "NIFTY PSU BANK": (8434.2, 8455.9),
                },
            ]
        )
        engine = MarketEngine(redis_client=None)
        engine.kite = fake_kite
        engine.connected = True
        engine.latest = {
            "ABC": {
                "symbol": "ABC",
                "name": "ABC",
                "price": 101.25,
                "change": 1.25,
                "is_fno": False,
                "sectors": [],
            }
        }
        engine.sector_tokens = {"NIFTY IT": 1, "NIFTY PSU BANK": 2}
        engine._is_market_open = lambda: True
        engine._is_live_feed_stale = lambda: False
        engine._cached_snapshot = lambda: None
        engine._cached_closed_snapshot = lambda: None
        engine._save_snapshot = lambda snapshot: None
        engine._save_closed_snapshot = lambda snapshot: None
        engine._ensure_background_refresh = lambda *args, **kwargs: False

        engine._refresh_sector_snapshot(force=True)
        first_snapshot = engine.get_snapshot()

        engine.last_sector_quote_ts = time.time() - 6
        second_snapshot = engine.get_snapshot()

        self.assertEqual(first_snapshot["sector_gainers"][0]["price"], 8633.4)
        self.assertEqual(second_snapshot["sector_gainers"][0]["price"], 8640.45)
        self.assertEqual(second_snapshot["sector_losers"][0]["price"], 8434.2)
        self.assertEqual(second_snapshot["snapshot_source"], "api_sector")


class MarketSnapshotApiIntegrationTests(unittest.TestCase):
    def test_market_snapshot_endpoint_returns_updated_sector_payloads_between_polls(self):
        with patch.object(
            main_module.engine,
            "get_snapshot",
            side_effect=[make_snapshot(8633.4), make_snapshot(8640.45)],
        ):
            with TestClient(main_module.app) as client:
                first_response = client.get("/api/market-snapshot")
                second_response = client.get("/api/market-snapshot")

        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(second_response.status_code, 200)
        self.assertEqual(
            first_response.headers.get("Cache-Control"),
            "no-store, no-cache, must-revalidate, max-age=0",
        )
        self.assertEqual(
            second_response.json()["sector_gainers"][0]["price"],
            8640.45,
        )
        self.assertNotEqual(
            first_response.json()["sector_gainers"][0]["price"],
            second_response.json()["sector_gainers"][0]["price"],
        )


if __name__ == "__main__":
    unittest.main()
