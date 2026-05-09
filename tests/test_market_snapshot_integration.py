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
    def test_stock_row_from_candles_includes_day_volume(self):
        engine = MarketEngine(redis_client=None)
        row, latest_dt = engine._build_stock_row_from_candles(
            "INFY",
            [
                {"close": 1480.0, "volume": 123456, "date": "2026-05-08"},
                {"close": 1500.0, "volume": 234567, "date": "2026-05-09"},
            ],
        )

        self.assertEqual(row["symbol"], "INFY")
        self.assertEqual(row["volume"], 234567)
        self.assertEqual(row["price"], 1500.0)
        self.assertEqual(row["change"], 1.35)
        self.assertEqual(latest_dt, "2026-05-09")

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

    def test_live_snapshot_schedules_background_refresh_when_cache_is_empty(self):
        engine = MarketEngine(redis_client=None)
        engine.kite = object()
        engine.connected = False
        engine.latest = {}
        engine.sector_latest = {}
        engine._is_market_open = lambda: True
        engine._is_live_feed_stale = lambda: False
        engine._cached_snapshot = lambda: None
        engine._cached_closed_snapshot = lambda: None
        engine._save_snapshot = lambda snapshot: None
        engine._save_closed_snapshot = lambda snapshot: None

        scheduled = []
        engine._ensure_background_refresh = lambda market_open, reason="initial": scheduled.append((market_open, reason)) or True
        engine._refresh_sector_snapshot = lambda *args, **kwargs: self.fail("get_snapshot should not block on sector refresh")
        engine._refresh_rest_snapshot = lambda *args, **kwargs: self.fail("get_snapshot should not block on rest refresh")

        snapshot = engine.get_snapshot()

        self.assertEqual(scheduled, [(True, "initial")])
        self.assertEqual(snapshot["gainers"], [])
        self.assertEqual(snapshot["sector_gainers"], [])

    def test_closed_market_sector_breakdown_uses_cached_rows_and_memberships(self):
        engine = MarketEngine(redis_client=None)
        engine.kite = None
        engine.latest = {}
        engine._is_market_open = lambda: False
        engine._cached_sector_memberships = lambda: {
            "sector_members": {"NIFTY IT": ["INFY", "TCS"]},
            "symbol_to_sectors": {
                "INFY": ["NIFTY IT"],
                "TCS": ["NIFTY IT"],
            },
        }
        engine._cached_latest_rows = lambda: {
            "rows": {
                "INFY": {
                    "symbol": "INFY",
                    "name": "Infosys",
                    "price": 1500.0,
                    "change": 1.25,
                    "is_fno": True,
                    "sectors": ["NIFTY IT"],
                },
                "TCS": {
                    "symbol": "TCS",
                    "name": "TCS",
                    "price": 4200.0,
                    "change": -0.5,
                    "is_fno": True,
                    "sectors": ["NIFTY IT"],
                },
            },
            "updated_at": "2026-05-08T15:30:00+05:30",
            "snapshot_source": "historical_eod",
        }

        payload = engine.get_sector_breakdown("NIFTY IT")

        self.assertEqual(payload["sector"], "NIFTY IT")
        self.assertEqual(payload["constituent_count"], 2)
        self.assertEqual(payload["stocks"][0]["symbol"], "INFY")
        self.assertEqual(payload["stocks"][1]["symbol"], "TCS")

    def test_closed_market_sector_breakdown_prefers_cached_sector_payload(self):
        engine = MarketEngine(redis_client=None)
        engine._is_market_open = lambda: False
        engine._cached_sector_breakdowns = lambda: {
            "NIFTY IT": {
                "sector": "NIFTY IT",
                "stocks": [
                    {
                        "rank": 1,
                        "symbol": "INFY",
                        "name": "Infosys",
                        "price": 1500.0,
                        "change": 1.25,
                        "is_fno": True,
                        "sectors": ["NIFTY IT"],
                    }
                ],
                "updated_at": "2026-05-12T15:30:00+05:30",
                "market_open": False,
                "snapshot_source": "historical_eod",
                "constituent_count": 1,
            }
        }
        engine._refresh_sector_memberships = lambda *args, **kwargs: self.fail("closed cache should avoid membership refresh")

        payload = engine.get_sector_breakdown("NIFTY IT")

        self.assertEqual(payload["constituent_count"], 1)
        self.assertEqual(payload["stocks"][0]["symbol"], "INFY")


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
