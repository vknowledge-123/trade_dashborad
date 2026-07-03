import os
import struct
import tempfile
import time
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from fastapi.testclient import TestClient

_TMP_DIR = tempfile.TemporaryDirectory()
os.environ["TRADE_DASHBOARD_DB_PATH"] = os.path.join(_TMP_DIR.name, "test_trade_dashboard.db")

from app.kite_engine import IST, MarketEngine, SWING_SCANNER_CACHE_VERSION
import app.main as main_module

main_module.init_db()


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


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code
        self.text = str(payload)

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"{self.status_code} error")

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.headers = {}
        self.last_payload = None

    def post(self, url, json, timeout):
        self.last_payload = json
        return self.response


class RateLimitedDhan:
    def marketfeed_quote(self, securities):
        from app.kite_engine import DhanRateLimitError
        raise DhanRateLimitError("Dhan API /marketfeed/quote rate limited (429)")


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
    def test_latest_completed_session_date_uses_previous_trading_day_on_weekend(self):
        engine = MarketEngine(redis_client=None)

        completed = engine._latest_completed_session_date(datetime(2026, 5, 10, 16, 0, tzinfo=None))

        self.assertEqual(completed.isoformat(), "2026-05-08")

    def test_previous_day_badges_use_last_completed_session_candles(self):
        engine = MarketEngine(redis_client=None)
        engine.kite = object()
        engine.symbol_to_token = {"INFY": 123}
        engine._latest_completed_session_date = lambda now=None: datetime(2026, 5, 8).date()
        engine._fetch_recent_day_candles = lambda token, _from_date, _to_date, limit=None: [
            {"date": "2026-05-07", "close": 1500.0, "volume": 100},
            {"date": "2026-05-08", "close": 1530.0, "volume": 120},
        ][-limit:] if limit else [
            {"date": "2026-05-07", "close": 1500.0, "volume": 100},
            {"date": "2026-05-08", "close": 1530.0, "volume": 120},
        ]

        change = engine._get_previous_day_change("INFY")

        self.assertEqual(change, 2.0)
        self.assertEqual(engine.previous_day_badges_cache["INFY"]["cache_marker"], "2026-05-08")

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

    def test_swing_staircase_pattern_requires_higher_low_structure(self):
        engine = MarketEngine(redis_client=None)
        candles = []
        price = 100.0
        for index in range(8):
            close = price + (0.4 if index % 2 == 0 else -0.1)
            candles.append(
                {
                    "date": f"2026-06-{index + 1:02d}",
                    "open": price,
                    "high": max(price, close) + 0.4,
                    "low": min(price, close) - 0.15 + index * 0.04,
                    "close": close,
                    "volume": 100000 + index * 1000,
                }
            )
            price = close

        pattern = engine._swing_staircase_pattern(candles)

        self.assertTrue(pattern["is_valid"])
        self.assertEqual(pattern["label"], "Staircase confirmed")

    def test_swing_price_volume_growth_requires_close_and_volume_expansion(self):
        engine = MarketEngine(redis_client=None)
        candles = [
            {
                "date": f"2026-06-{index + 1:02d}",
                "open": 100 + index,
                "high": 101 + index,
                "low": 99 + index,
                "close": 100 + index,
                "volume": 100000,
            }
            for index in range(6)
        ]
        candles[-2]["close"] = 105.0
        candles[-2]["volume"] = 100000
        candles[-1]["close"] = 106.0
        candles[-1]["volume"] = 135000

        growth = engine._swing_price_volume_growth(candles)

        self.assertTrue(growth["is_valid"])
        self.assertEqual(growth["label"], "Price-volume growth")
        self.assertGreater(growth["volume_ratio"], 1.05)

    def test_old_swing_cache_is_ignored_after_strategy_version_change(self):
        engine = MarketEngine(redis_client=None)
        engine._completed_session_cache_marker = lambda: "2026-06-26"
        engine._cached_swing_scanner_payload = lambda: {
            "cache_marker": "2026-06-26",
            "cache_version": SWING_SCANNER_CACHE_VERSION - 1,
            "rows": [{"symbol": "TITAN", "scan_state": "BULLISH_SCAN", "score": 90}],
        }
        engine._build_swing_scanner_payload = lambda: {
            "cache_marker": "2026-06-26",
            "rows": [
                {
                    "symbol": "TITAN",
                    "scan_state": "BULLISH_SCAN",
                    "score": 82,
                    "staircase_pattern": "Staircase confirmed",
                    "daily_volume_ratio": 1.2,
                }
            ],
        }

        payload = engine.get_swing_scanner(cached_only=False)

        self.assertEqual(payload["rows"][0]["score"], 82)
        self.assertEqual(payload["filtered_rows"][0]["staircase_pattern"], "Staircase confirmed")

    def test_swing_filter_downgrades_rows_without_structure_fields(self):
        engine = MarketEngine(redis_client=None)
        payload = {
            "rows": [
                {
                    "symbol": "TITAN",
                    "scan_state": "BULLISH_SCAN",
                    "score": 87,
                    "setup": "Bullish scan: PDL sweep reclaim",
                }
            ]
        }

        filtered = engine._filter_swing_rows(payload, min_score=0)

        self.assertEqual(filtered["filtered_rows"][0]["scan_state"], "WATCH_RECLAIM")
        self.assertLessEqual(filtered["filtered_rows"][0]["score"], 61)
        self.assertEqual(filtered["filtered_rows"][0]["setup"], "Bullish watch: structure confirmation pending")

    def test_swing_scanner_cache_only_does_not_fetch_history_when_cache_missing(self):
        engine = MarketEngine(redis_client=None)
        engine.kite = object()
        engine.symbol_to_token = {"INFY": 123}
        engine._completed_session_cache_marker = lambda: "2026-07-03"
        engine._cached_swing_scanner_payload = lambda: None
        engine._ensure_swing_scanner_background_refresh = lambda: True
        engine._build_swing_scanner_payload = lambda *args, **kwargs: self.fail("cache-only swing page should not fetch history inline")

        payload = engine.get_swing_scanner(cached_only=True)

        self.assertTrue(payload["cache_pending"])
        self.assertIn("daily cache is not ready", payload["error"])

    def test_swing_scanner_uses_nifty500_universe_when_available(self):
        engine = MarketEngine(redis_client=None)
        engine.kite = object()
        engine.nifty500_set = {"INFY", "HCLTECH", "COFORGE"}
        engine.symbol_to_token = {"INFY": 1, "HCLTECH": 2, "COFORGE": 3}
        seen = []
        engine._fetch_swing_candles = lambda symbol, sessions=180: seen.append(symbol) or []

        payload = engine._build_swing_scanner_payload()

        self.assertEqual(payload["tracked_count"], 3)
        self.assertEqual(set(payload["symbols"]), {"INFY", "HCLTECH", "COFORGE"})
        self.assertEqual(set(seen), {"INFY", "HCLTECH", "COFORGE"})

    def test_market_history_cache_builds_and_saves_swing_scanner(self):
        engine = MarketEngine(redis_client=None)
        engine.symbol_to_token = {"INFY": 123}
        engine.sector_tokens = {}
        engine._completed_session_cache_marker = lambda: "2026-07-03"
        engine._warm_market_open_stock_cache = lambda cache_marker, force=False: {
            "processed": 1,
            "previous_close_updated": 1,
            "badge_updated": 1,
            "volume_updated": 1,
        }
        engine._warm_relative_rotation_graph_cache = lambda cache_marker, force=False: {"items": [{"sector": "NIFTY IT"}]}
        saved = {}
        engine._save_swing_scanner_cache = lambda payload: saved.update(payload)
        engine._build_swing_scanner_payload = lambda: {
            "rows": [{"symbol": "INFY", "scan_state": "BULLISH_SCAN", "score": 80}],
            "tracked_count": 1,
            "error": None,
        }
        engine._save_previous_close_cache = lambda: None

        engine._run_daily_market_history_cache_job(force=True)

        self.assertEqual(saved["rows"][0]["symbol"], "INFY")
        self.assertIn("swing rows", engine.history_cache_status["message"])

    def test_rest_snapshot_uses_quote_volume_field(self):
        engine = MarketEngine(redis_client=None)
        engine.kite = object()
        engine.symbol_to_token = {"INFY": 123}
        engine._is_market_open = lambda: True
        engine._quote_symbols = lambda kite, symbols: {
            "NSE:INFY": {
                "last_price": 1501.0,
                "volume": 987654,
                "ohlc": {"close": 1490.0},
            }
        }
        engine._save_latest_rows_cache = lambda: None
        engine._save_sector_breakdowns_cache = lambda *args, **kwargs: None

        refreshed = engine._refresh_rest_snapshot(force=True)

        self.assertTrue(refreshed)
        self.assertEqual(engine.latest["INFY"]["volume"], 987654)

    def test_websocket_tick_preserves_existing_volume_when_tick_has_no_volume(self):
        engine = MarketEngine(redis_client=None)
        engine.token_to_symbol = {123: "INFY"}
        engine.rest_prev_close = {"INFY": 1490.0}
        engine.latest = {
            "INFY": {
                "symbol": "INFY",
                "name": "Infosys",
                "price": 1498.0,
                "change": 0.54,
                "volume": 543210,
                "is_fno": True,
                "sectors": [],
            }
        }

        engine._on_ticks(
            None,
            [{"instrument_token": 123, "last_price": 1500.0, "ohlc": {"close": 1490.0}}],
        )

        self.assertEqual(engine.latest["INFY"]["volume"], 543210)
        self.assertTrue(engine.connected)

    def test_dhan_websocket_tick_preserves_existing_open_high_low_flags(self):
        engine = MarketEngine(redis_client=None)
        engine.token_to_symbol = {123: "INFY"}
        engine.rest_prev_close = {"INFY": 1490.0}
        engine.latest = {
            "INFY": {
                "symbol": "INFY",
                "name": "Infosys",
                "price": 1498.0,
                "change": 0.54,
                "volume": 543210,
                "day_open": 1500.0,
                "day_low": 1500.0,
                "day_high": 1510.0,
                "open_equals_low": True,
                "open_equals_high": False,
                "ohlc_badges": ["OPEN=LOW"],
            }
        }
        packet = struct.pack("<B H B I f", 2, 12, 1, 123, 1502.0)

        engine._on_dhan_binary_message(packet)

        self.assertEqual(engine.latest["INFY"]["day_open"], 1500.0)
        self.assertEqual(engine.latest["INFY"]["day_low"], 1500.0)
        self.assertTrue(engine.latest["INFY"]["open_equals_low"])
        self.assertIn("OPEN=LOW", engine.latest["INFY"]["ohlc_badges"])

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
        engine._refresh_sector_snapshot(force=True)
        second_snapshot = engine.get_snapshot()

        self.assertEqual(first_snapshot["sector_gainers"][0]["price"], 8633.4)
        self.assertEqual(second_snapshot["sector_gainers"][0]["price"], 8640.45)
        self.assertEqual(second_snapshot["sector_losers"][0]["price"], 8434.2)
        self.assertEqual(second_snapshot["snapshot_source"], "api_sector")

    def test_decorate_snapshot_rows_adds_previous_day_badge_flags(self):
        engine = MarketEngine(redis_client=None)
        snapshot = {
            "gainers": [{"symbol": "INFY", "change": 1.2}],
            "losers": [{"symbol": "TCS", "change": -0.8}],
        }

        changes = {"INFY": 2.15, "TCS": -1.25}
        engine._get_previous_day_change = lambda symbol, allow_fetch=True: changes.get(symbol)

        engine._decorate_snapshot_rows(snapshot)

        self.assertTrue(snapshot["gainers"][0]["previous_day_positive"])
        self.assertEqual(snapshot["gainers"][0]["previous_day_change"], 2.15)
        self.assertFalse(snapshot["losers"][0]["previous_day_positive"])
        self.assertEqual(snapshot["losers"][0]["previous_day_change"], -1.25)

    def test_build_stock_row_includes_turnover_from_price_and_volume(self):
        engine = MarketEngine(redis_client=None)

        row = engine._build_stock_row("INFY", last_price=1500.5, close=1450.0, volume=123456)

        self.assertEqual(row["turnover"], 185245728.0)

    def test_decorate_snapshot_rows_backfills_cached_turnover(self):
        engine = MarketEngine(redis_client=None)
        snapshot = {
            "gainers": [{"symbol": "INFY", "price": 1500.0, "volume": 1000, "change": 1.2}],
            "losers": [{"symbol": "TCS", "price": 4200.0, "volume": 50, "change": -0.8}],
        }
        engine._get_previous_day_change = lambda symbol, allow_fetch=True: None

        engine._decorate_snapshot_rows(snapshot)

        self.assertEqual(snapshot["gainers"][0]["turnover"], 1500000.0)
        self.assertEqual(snapshot["losers"][0]["turnover"], 210000.0)

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

    def test_closed_market_does_not_return_stale_closed_snapshot_for_old_session(self):
        engine = MarketEngine(redis_client=None)
        engine.kite = object()
        engine.latest = {}
        engine.sector_latest = {}
        engine._is_market_open = lambda: False
        engine._completed_session_cache_marker = lambda: "2026-05-22"
        engine._cached_closed_snapshot = lambda: {
            "session_marker": "2026-05-21",
            "gainers": [{"symbol": "OLD", "price": 10, "change": 1}],
            "losers": [],
            "sector_gainers": [],
            "sector_losers": [],
            "updated_at": "2026-05-21T15:30:00+05:30",
        }
        engine._cached_snapshot = lambda: {
            "session_marker": "2026-05-21",
            "gainers": [{"symbol": "OLD", "price": 10, "change": 1}],
            "losers": [],
            "sector_gainers": [],
            "sector_losers": [],
            "updated_at": "2026-05-21T15:30:00+05:30",
        }
        engine._save_snapshot = lambda snapshot: None
        engine._save_closed_snapshot = lambda snapshot: None
        scheduled = []
        engine._ensure_background_refresh = lambda market_open, reason="initial": scheduled.append((market_open, reason)) or True

        snapshot = engine.get_snapshot()

        self.assertEqual(snapshot["gainers"], [])
        self.assertEqual(scheduled, [(False, "closed_market_bootstrap")])

    def test_closed_market_snapshot_builds_sector_movers_from_stock_rows(self):
        engine = MarketEngine(redis_client=None)
        engine.latest = {
            "INFY": {"symbol": "INFY", "price": 1500, "change": 2.0, "sectors": ["NIFTY IT"]},
            "TCS": {"symbol": "TCS", "price": 4200, "change": 1.0, "sectors": ["NIFTY IT"]},
            "HDFCBANK": {"symbol": "HDFCBANK", "price": 900, "change": -1.0, "sectors": ["NIFTY PVT BANK"]},
        }
        engine.sector_latest = {}
        engine.symbol_to_sectors = {
            "INFY": ["NIFTY IT"],
            "TCS": ["NIFTY IT"],
            "HDFCBANK": ["NIFTY PVT BANK"],
        }

        snapshot = engine._build_snapshot(market_open=False)

        self.assertEqual(snapshot["sector_gainers"][0]["sector"], "NIFTY IT")
        self.assertEqual(snapshot["sector_gainers"][0]["change"], 1.5)
        self.assertEqual(snapshot["sector_gainers"][0]["price"], "-")
        self.assertEqual(snapshot["sector_losers"][0]["sector"], "NIFTY PVT BANK")

    def test_sector_strength_uses_fallback_members_when_membership_cache_is_missing(self):
        engine = MarketEngine(redis_client=None)
        engine.latest = {
            "INFY": {"symbol": "INFY", "price": 1500, "change": 2.0},
            "TCS": {"symbol": "TCS", "price": 4200, "change": 1.0},
            "HDFCBANK": {"symbol": "HDFCBANK", "price": 900, "change": -1.0},
        }
        engine.sector_latest = {}
        engine.symbol_to_sectors = {}
        engine._restore_cached_sector_memberships = lambda: False

        snapshot = engine._build_snapshot(market_open=False)

        self.assertEqual(snapshot["sector_gainers"][0]["sector"], "NIFTY IT")
        self.assertEqual(snapshot["sector_gainers"][0]["change"], 1.5)
        self.assertEqual(snapshot["sector_losers"][0]["sector"], "NIFTY BANK")

    def test_closed_market_snapshot_uses_constituent_sector_moves_when_sector_quotes_are_zero(self):
        engine = MarketEngine(redis_client=None)
        engine.latest = {
            "INFY": {"symbol": "INFY", "price": 1500, "change": 2.0, "sectors": ["NIFTY IT"]},
            "TCS": {"symbol": "TCS", "price": 4200, "change": 1.0, "sectors": ["NIFTY IT"]},
            "HDFCBANK": {"symbol": "HDFCBANK", "price": 900, "change": -1.0, "sectors": ["NIFTY PVT BANK"]},
        }
        engine.sector_latest = {
            "NIFTY IT": {"sector": "NIFTY IT", "price": 27439.4, "change": 0.0},
            "NIFTY PVT BANK": {"sector": "NIFTY PVT BANK", "price": 28215.45, "change": 0.0},
        }

        snapshot = engine._build_snapshot(market_open=False)

        self.assertEqual(snapshot["sector_gainers"][0]["sector"], "NIFTY IT")
        self.assertEqual(snapshot["sector_gainers"][0]["change"], 1.5)
        self.assertEqual(snapshot["sector_gainers"][0]["price"], 27439.4)
        self.assertEqual(snapshot["sector_losers"][0]["sector"], "NIFTY PVT BANK")
        self.assertEqual(snapshot["sector_losers"][0]["change"], -1.0)

    def test_cached_sector_movers_are_resplit_by_change_sign(self):
        engine = MarketEngine(redis_client=None)
        snapshot = {
            "sector_gainers": [
                {"sector": "NIFTY IT", "price": "-", "change": 0.63},
                {"sector": "NIFTY PSU BANK", "price": "-", "change": -0.43},
            ],
            "sector_losers": [
                {"sector": "NIFTY ENERGY", "price": "-", "change": -0.29},
            ],
        }

        normalized = engine._normalize_sector_movers(snapshot)

        self.assertEqual([row["sector"] for row in normalized["sector_gainers"]], ["NIFTY IT"])
        self.assertEqual(
            [row["sector"] for row in normalized["sector_losers"]],
            ["NIFTY PSU BANK", "NIFTY ENERGY"],
        )

    def test_closed_market_snapshot_hydrates_from_previous_day_stock_cache(self):
        engine = MarketEngine(redis_client=None)
        engine.nifty500_set = {"INFY", "HDFCBANK"}
        engine.symbol_to_token = {"INFY": 1, "HDFCBANK": 2}
        engine.symbol_to_name = {"INFY": "Infosys", "HDFCBANK": "HDFC Bank"}
        engine.symbol_to_sectors = {"INFY": ["NIFTY IT"], "HDFCBANK": ["NIFTY PVT BANK"]}
        engine._completed_session_cache_marker = lambda: "2026-05-22"
        engine.previous_close_cache = {
            "symbols": {
                "INFY": {"cache_marker": "2026-05-22", "broker": "kite", "close": 1500.0},
                "HDFCBANK": {"cache_marker": "2026-05-22", "broker": "kite", "close": 900.0},
            }
        }
        engine.previous_day_badges_cache = {
            "INFY": {"cache_marker": "2026-05-22", "broker": "kite", "change": 2.0},
            "HDFCBANK": {"cache_marker": "2026-05-22", "broker": "kite", "change": -1.0},
        }
        engine._restore_previous_day_badges_cache = lambda: engine.previous_day_badges_cache
        engine._restore_previous_close_cache = lambda: engine.previous_close_cache
        engine._save_latest_rows_cache = lambda: None

        snapshot = engine._build_snapshot(market_open=False)

        self.assertEqual(snapshot["gainers"][0]["symbol"], "INFY")
        self.assertEqual(snapshot["gainers"][0]["change"], 2.0)
        self.assertEqual(snapshot["losers"][0]["symbol"], "HDFCBANK")
        self.assertEqual(snapshot["sector_gainers"][0]["sector"], "NIFTY IT")
        self.assertEqual(snapshot["sector_losers"][0]["sector"], "NIFTY PVT BANK")
        self.assertEqual(engine.last_snapshot_source, "previous_day_cache")

    def test_previous_day_cache_hydration_populates_shared_latest_rows(self):
        engine = MarketEngine(redis_client=None)
        engine.symbol_to_token = {"INFY": 1}
        engine.symbol_to_name = {"INFY": "Infosys"}
        engine.symbol_to_sectors = {"INFY": ["NIFTY IT"]}
        engine._completed_session_cache_marker = lambda: "2026-05-22"
        engine.previous_close_cache = {
            "symbols": {
                "INFY": {"cache_marker": "2026-05-22", "broker": "kite", "close": 1500.0},
            }
        }
        engine.previous_day_badges_cache = {
            "INFY": {"cache_marker": "2026-05-22", "broker": "kite", "change": 2.0},
        }
        engine._restore_previous_day_badges_cache = lambda: engine.previous_day_badges_cache
        engine._restore_previous_close_cache = lambda: engine.previous_close_cache
        engine._save_latest_rows_cache = lambda: None

        rows = engine._hydrate_latest_rows_from_previous_day_cache(["INFY"])

        self.assertEqual(rows[0]["symbol"], "INFY")
        self.assertEqual(engine.latest["INFY"]["price"], 1500.0)
        self.assertEqual(engine.latest["INFY"]["change"], 2.0)

    def test_closed_sector_quote_refresh_preserves_existing_nonzero_change(self):
        engine = MarketEngine(redis_client=None)
        engine.kite = object()
        engine.sector_tokens = {"NIFTY IT": 123}
        engine.sector_latest = {
            "NIFTY IT": {"sector": "NIFTY IT", "price": 27000.0, "change": 1.25}
        }
        engine._is_market_open = lambda: False
        engine._fetch_sector_quote = lambda kite, sectors: (
            {},
            {"NIFTY IT": {"sector": "NIFTY IT", "price": 27439.4, "change": 0.0}},
        )

        refreshed = engine._refresh_sector_snapshot(force=True)

        self.assertTrue(refreshed)
        self.assertEqual(engine.sector_latest["NIFTY IT"]["price"], 27439.4)
        self.assertEqual(engine.sector_latest["NIFTY IT"]["change"], 1.25)

    def test_pdh_pdl_scanner_uses_cached_rows_without_blocking_on_history(self):
        engine = MarketEngine(redis_client=None)
        engine.kite = object()
        engine.symbol_to_token = {"INFY": 123}
        engine._is_market_open = lambda: True
        engine._completed_session_cache_marker = lambda: "2026-05-22"
        engine.latest = {
            "INFY": {
                "symbol": "INFY",
                "name": "Infosys",
                "price": 100.0,
                "change": 0.2,
                "volume": 1000,
                "is_fno": True,
                "sectors": [],
            }
        }
        engine.previous_day_levels_cache = {
            "INFY": {
                "cache_marker": "2026-05-22",
                "high": 100.10,
                "low": 98.0,
                "close": 99.5,
                "date": "2026-05-22",
            }
        }
        engine._get_latest_rows_for_symbols = lambda symbols: self.fail("scanner API should not fetch quotes inline")
        engine._get_previous_day_levels = lambda symbol: self.fail("scanner API should not fetch history inline")
        engine._ensure_scanner_background_refresh = lambda: False

        payload = engine.get_pdh_pdl_scanner(level="pdh", side="below", max_pct=0.2)

        self.assertEqual(payload["filtered_rows"][0]["symbol"], "INFY")
        self.assertEqual(payload["filtered_rows"][0]["pdh_side"], "below")
        self.assertAlmostEqual(abs(payload["filtered_rows"][0]["pdh_distance_percent"]), 0.1, places=3)

    def test_dhan_private_bank_security_id_is_available_for_idx_segment(self):
        engine = MarketEngine(redis_client=None)
        engine.broker = "dhan"
        engine._dhan_scrip_rows = lambda: iter([])
        engine.kite = object()
        engine._refresh_sector_memberships = lambda *args, **kwargs: None
        engine._fetch_sector_quote = lambda *args, **kwargs: ({}, {})

        engine._build_dhan_universe(["NIFTY PVT BANK"])

        self.assertEqual(engine.sector_tokens["NIFTY PVT BANK"], 15)
        self.assertEqual(engine.dhan_security_to_segment[15], "IDX_I")

    def test_dhan_sector_security_id_fallbacks_cover_configured_indices(self):
        expected_ids = {
            "NIFTY AUTO": 14,
            "NIFTY IT": 29,
            "NIFTY METAL": 31,
            "NIFTY INFRA": 43,
            "NIFTY FINSEREXBNK": 495,
            "NIFTY MS FIN SERV": 819,
            "NIFTY HEALTHCARE": 447,
            "NIFTY MIDSML HLTH": 471,
            "NIFTY PSU BANK": 33,
            "NIFTY CONSR DURBL": 466,
            "NIFTY FMCG": 28,
            "NIFTY PVT BANK": 15,
            "NIFTY ENERGY": 42,
            "NIFTY CPSE": 45,
            "NIFTY BANK": 25,
            "NIFTY MS IT TELCM": 821,
            "NIFTY IND DEFENCE": 493,
            "NIFTY MEDIA": 30,
            "NIFTY IND DIGITAL": 473,
            "NIFTY PHARMA": 32,
            "NIFTY IND TOURISM": 815,
            "NIFTY CAPITAL MKT": 803,
            "NIFTY OIL AND GAS": 470,
            "NIFTY INDIA MFG": 474,
        }
        engine = MarketEngine(redis_client=None)
        engine.broker = "dhan"
        engine._dhan_scrip_rows = lambda: iter([])
        engine.kite = object()
        engine._refresh_sector_memberships = lambda *args, **kwargs: None
        engine._fetch_sector_quote = lambda *args, **kwargs: ({}, {})

        engine._build_dhan_universe(list(expected_ids))

        self.assertEqual(engine.sector_tokens, expected_ids)
        for security_id in expected_ids.values():
            self.assertEqual(engine.dhan_security_to_segment[security_id], "IDX_I")

    def test_dhan_universe_always_adds_rrg_benchmark_fallback(self):
        engine = MarketEngine(redis_client=None)
        engine._dhan_scrip_rows = lambda: []
        engine._refresh_sector_memberships = lambda *args, **kwargs: None
        engine._fetch_sector_quote = lambda *args, **kwargs: ({}, {})

        engine._build_dhan_universe(["NIFTY IT"])

        self.assertEqual(engine.index_tokens["NIFTY 50"], 13)
        self.assertEqual(engine.dhan_security_to_segment[13], "IDX_I")
        self.assertEqual(engine.dhan_security_to_instrument[13], "INDEX")

    def test_dhan_segment_normalizer_matches_working_exporter_inputs(self):
        engine = MarketEngine(redis_client=None)

        self.assertEqual(engine._normalize_dhan_segment_instrument("NSE", "", "EQUITY"), ("NSE_EQ", "EQUITY"))
        self.assertEqual(engine._normalize_dhan_segment_instrument("NSE_EQ", "", "EQ"), ("NSE_EQ", "EQUITY"))
        self.assertEqual(engine._normalize_dhan_segment_instrument("", "1", "EQUITY"), ("NSE_EQ", "EQUITY"))
        self.assertEqual(engine._normalize_dhan_segment_instrument("IDX", "", "INDEX"), ("IDX_I", "INDEX"))
        self.assertEqual(engine._normalize_dhan_segment_instrument("FNO", "", "FUTIDX"), ("NSE_FNO", "FUTIDX"))

    def test_dhan_universe_accepts_numeric_nse_equity_segment_code(self):
        engine = MarketEngine(redis_client=None)
        engine.nifty500_set = {"RELIANCE"}
        rows = [
            {
                "SEM_EXM_EXCH_ID": "",
                "SEM_SEGMENT": "1",
                "SEM_SERIES": "EQ",
                "SEM_INSTRUMENT_NAME": "EQUITY",
                "SEM_SMST_SECURITY_ID": "2885",
                "SEM_TRADING_SYMBOL": "RELIANCE",
                "SEM_CUSTOM_SYMBOL": "Reliance Industries",
            }
        ]
        engine._dhan_scrip_rows = lambda: rows
        engine._refresh_sector_memberships = lambda *args, **kwargs: None
        engine._fetch_sector_quote = lambda *args, **kwargs: ({}, {})

        engine._build_dhan_universe([])

        self.assertEqual(engine.symbol_to_token["RELIANCE"], 2885)
        self.assertEqual(engine.dhan_security_to_segment[2885], "NSE_EQ")
        self.assertEqual(engine.dhan_security_to_instrument[2885], "EQUITY")

    def test_acceleration_volume_baseline_looks_back_to_find_five_sessions(self):
        engine = MarketEngine(redis_client=None)
        engine.symbol_to_token = {"360ONE": 1}
        engine.nifty500_set = {"360ONE"}
        engine._completed_session_cache_marker = lambda: "2026-06-25"
        candles = [
            {"date": "2026-06-18", "close": 1120.0, "volume": 700000},
            {"date": "2026-06-19", "close": 1145.1, "volume": 1587197},
            {"date": "2026-06-22", "close": 1139.2, "volume": 496298},
            {"date": "2026-06-23", "close": 1116.9, "volume": 385961},
            {"date": "2026-06-24", "close": 1099.9, "volume": 561423},
        ]
        requested = []
        def fake_fetch(token, from_date, to_date, limit=None):
            requested.append(limit)
            return candles[-limit:] if limit else candles
        engine._fetch_recent_day_candles = fake_fetch

        summary = engine._warm_market_open_stock_cache("2026-06-25", force=True)

        self.assertEqual(summary["volume_updated"], 1)
        self.assertIn(10, requested)
        cached = engine.acceleration_volume_sma_cache["360ONE"]
        self.assertEqual(cached["sessions"], 5)
        self.assertEqual(cached["lookback_sessions"], 10)
        self.assertEqual(cached["session_minutes"], 5 * 375)
        self.assertEqual(cached["volume_sma"], round(sum(c["volume"] for c in candles) / (5 * 375), 2))

    def test_acceleration_scanner_keeps_intraday_hits_after_current_move_fades(self):
        engine = MarketEngine(redis_client=None)
        engine.broker = "kite"
        engine._restore_acceleration_hits_cache = lambda: None
        engine._save_acceleration_hits_cache = lambda: None
        engine.latest = {"INFY": {"symbol": "INFY", "price": 101.0, "change": 1.0, "volume": 1000}}
        now = datetime.now(IST)
        current_bucket = engine._bucket_start(now, 1).isoformat()
        previous_bucket = (engine._bucket_start(now, 1) - timedelta(minutes=1)).isoformat()
        engine.acceleration_closes["INFY"][(1, previous_bucket)] = {
            "close": 100.0,
            "updated_at": previous_bucket,
        }
        engine.acceleration_closes["INFY"][(1, current_bucket)] = {
            "close": 101.0,
            "updated_at": current_bucket,
            "candle_volume": 10000,
        }

        first_payload = engine.get_acceleration_scanner(timeframe=1, min_gain=0.5)
        self.assertEqual(len(first_payload["rows"]), 1)
        self.assertEqual(first_payload["rows"][0]["symbol"], "INFY")

        engine.acceleration_closes["INFY"][(1, current_bucket)]["close"] = 100.1
        faded_payload = engine.get_acceleration_scanner(timeframe=1, min_gain=0.5)

        self.assertEqual(len(faded_payload["rows"]), 1)
        self.assertEqual(faded_payload["rows"][0]["symbol"], "INFY")
        self.assertEqual(faded_payload["rows"][0]["move_percent"], 1.0)
        self.assertEqual(faded_payload["persisted_count"], 1)

    def test_acceleration_scanner_adds_sector_change_and_rank(self):
        engine = MarketEngine(redis_client=None)
        engine.broker = "kite"
        engine._restore_acceleration_hits_cache = lambda: None
        engine._save_acceleration_hits_cache = lambda: None
        engine.symbol_to_sectors = {"INFY": ["NIFTY IT"], "HDFCBANK": ["NIFTY PVT BANK"]}
        engine.sector_latest = {
            "NIFTY IT": {"sector": "NIFTY IT", "price": 1000, "change": 2.5},
            "NIFTY PVT BANK": {"sector": "NIFTY PVT BANK", "price": 1000, "change": 1.0},
        }
        engine.latest = {
            "INFY": {"symbol": "INFY", "price": 101.0, "change": 1.0, "volume": 1000, "sectors": ["NIFTY IT"]},
        }
        now = datetime.now(IST)
        current_bucket = engine._bucket_start(now, 1).isoformat()
        previous_bucket = (engine._bucket_start(now, 1) - timedelta(minutes=1)).isoformat()
        engine.acceleration_closes["INFY"][(1, previous_bucket)] = {"close": 100.0, "updated_at": previous_bucket}
        engine.acceleration_closes["INFY"][(1, current_bucket)] = {
            "close": 101.0,
            "updated_at": current_bucket,
            "candle_volume": 10000,
        }

        payload = engine.get_acceleration_scanner(timeframe=1, min_gain=0.5)

        self.assertEqual(payload["rows"][0]["sector_name"], "NIFTY IT")
        self.assertEqual(payload["rows"][0]["sector_change"], 2.5)
        self.assertEqual(payload["rows"][0]["sector_rank"], 1)
        self.assertEqual(payload["rows"][0]["sector_count"], 2)

    def test_open_extreme_scanner_ranks_only_open_low_and_open_high_rows(self):
        engine = MarketEngine(redis_client=None)
        engine.nifty500_set = set()
        engine.symbol_to_sectors = {
            "LOW1": ["NIFTY IT"],
            "LOW2": ["NIFTY IT"],
            "HIGH1": ["NIFTY MEDIA"],
            "HIGH2": ["NIFTY MEDIA"],
            "WRONGLOW": ["NIFTY IT"],
            "WRONGHIGH": ["NIFTY MEDIA"],
        }
        engine.sector_latest = {
            "NIFTY IT": {"sector": "NIFTY IT", "price": 1000, "change": 2.5},
            "NIFTY MEDIA": {"sector": "NIFTY MEDIA", "price": 1000, "change": -1.5},
        }
        engine.latest = {
            "AAA": {
                "symbol": "AAA",
                "price": 110,
                "change": 10.0,
                "open_equals_low": False,
                "open_equals_high": False,
            },
            "LOW1": {
                "symbol": "LOW1",
                "price": 104,
                "change": 4.0,
                "day_open": 100,
                "day_low": 100,
                "open_equals_low": True,
                "open_equals_high": False,
            },
            "LOW2": {
                "symbol": "LOW2",
                "price": 106,
                "change": 6.0,
                "day_open": 100,
                "day_low": 100,
                "open_equals_low": True,
                "open_equals_high": False,
            },
            "HIGH1": {
                "symbol": "HIGH1",
                "price": 96,
                "change": -4.0,
                "day_open": 100,
                "day_high": 100,
                "open_equals_low": False,
                "open_equals_high": True,
            },
            "HIGH2": {
                "symbol": "HIGH2",
                "price": 92,
                "change": -8.0,
                "day_open": 100,
                "day_high": 100,
                "open_equals_low": False,
                "open_equals_high": True,
            },
            "WRONGLOW": {
                "symbol": "WRONGLOW",
                "price": 98,
                "change": -2.0,
                "day_open": 100,
                "day_low": 100,
                "open_equals_low": True,
                "open_equals_high": False,
            },
            "WRONGHIGH": {
                "symbol": "WRONGHIGH",
                "price": 103,
                "change": 3.0,
                "day_open": 100,
                "day_high": 100,
                "open_equals_low": False,
                "open_equals_high": True,
            },
        }

        payload = engine.get_open_extreme_scanner()

        self.assertEqual([row["symbol"] for row in payload["open_low_gainers"]], ["LOW2", "LOW1"])
        self.assertEqual([row["symbol"] for row in payload["open_high_losers"]], ["HIGH2", "HIGH1"])
        self.assertNotIn("AAA", [row["symbol"] for row in payload["open_low_gainers"]])
        self.assertNotIn("WRONGLOW", [row["symbol"] for row in payload["open_low_gainers"]])
        self.assertNotIn("WRONGHIGH", [row["symbol"] for row in payload["open_high_losers"]])
        self.assertEqual(payload["open_low_gainers"][0]["sector_name"], "NIFTY IT")
        self.assertEqual(payload["open_low_gainers"][0]["sector_change"], 2.5)
        self.assertEqual(payload["open_high_losers"][0]["sector_name"], "NIFTY MEDIA")
        self.assertEqual(payload["open_high_losers"][0]["sector_change"], -1.5)

    def test_open_extreme_scanner_uses_cached_flags_when_live_row_lacks_ohlc(self):
        engine = MarketEngine(redis_client=None)
        engine.nifty500_set = set()
        engine._is_market_open = lambda: False
        engine.latest = {
            "ECLERX": {
                "symbol": "ECLERX",
                "price": 1485,
                "change": 6.73,
                "volume": 1310000,
            }
        }
        engine._cached_latest_rows = lambda: {
            "rows": {
                "ECLERX": {
                    "symbol": "ECLERX",
                    "price": 1485,
                    "change": 6.73,
                    "volume": 1310000,
                    "day_open": 1391.3,
                    "day_low": 1391.3,
                    "day_high": 1490,
                    "open_equals_low": True,
                    "ohlc_badges": ["OPEN=LOW"],
                }
            }
        }

        payload = engine.get_open_extreme_scanner()

        self.assertEqual(payload["open_low_gainers"][0]["symbol"], "ECLERX")
        self.assertTrue(payload["open_low_gainers"][0]["open_equals_low"])
        self.assertEqual(payload["open_low_gainers"][0]["day_open"], 1391.3)

    def test_closed_open_extreme_scanner_forces_today_quote_refresh(self):
        engine = MarketEngine(redis_client=None)
        engine.nifty500_set = set()
        engine.kite = object()
        engine.symbol_to_token = {"ECLERX": 1, "ZEEL": 2}
        engine.symbol_to_name = {"ECLERX": "Eclerx", "ZEEL": "Zee"}
        engine._is_market_open = lambda: False
        engine._cached_latest_rows = lambda: {}
        engine._save_latest_rows_cache = lambda: None

        refreshed = []

        def fake_refresh(force=False):
            refreshed.append(force)
            engine.latest = {
                "ECLERX": {
                    "symbol": "ECLERX",
                    "name": "Eclerx",
                    "price": 1485.0,
                    "change": 6.73,
                    "day_open": 1391.3,
                    "day_high": 1490.0,
                    "day_low": 1391.3,
                    "open_equals_low": True,
                    "open_equals_high": False,
                },
                "ZEEL": {
                    "symbol": "ZEEL",
                    "name": "Zee",
                    "price": 94.0,
                    "change": -3.2,
                    "day_open": 100.0,
                    "day_high": 100.0,
                    "day_low": 92.0,
                    "open_equals_low": False,
                    "open_equals_high": True,
                },
            }
            return True

        engine._refresh_rest_snapshot = fake_refresh

        payload = engine.get_open_extreme_scanner()

        self.assertEqual(refreshed, [True])
        self.assertEqual(payload["open_low_gainers"][0]["symbol"], "ECLERX")
        self.assertEqual(payload["open_low_gainers"][0]["day_open"], 1391.3)
        self.assertTrue(payload["open_low_gainers"][0]["open_equals_low"])
        self.assertEqual(payload["open_high_losers"][0]["symbol"], "ZEEL")
        self.assertTrue(payload["open_high_losers"][0]["open_equals_high"])

    def test_open_extreme_scanner_does_not_use_previous_day_ohlc_cache(self):
        engine = MarketEngine(redis_client=None)
        engine.nifty500_set = set()
        engine.symbol_to_token = {"ECLERX": 1}
        engine._is_market_open = lambda: False
        engine.kite = None
        engine._cached_latest_rows = lambda: {}
        engine._rows_for_symbols_from_cache = lambda symbols: []
        engine.previous_day_levels_cache = {
            "ECLERX": {
                "cache_marker": "2026-07-03",
                "broker": "kite",
                "open": 1391.3,
                "high": 1490.0,
                "low": 1391.3,
                "close": 1485.0,
            }
        }
        engine.previous_day_badges_cache = {
            "ECLERX": {"cache_marker": "2026-07-03", "broker": "kite", "change": 6.73}
        }

        payload = engine.get_open_extreme_scanner()

        self.assertEqual(payload["open_low_gainers"], [])
        self.assertEqual(payload["open_high_losers"], [])

    def test_acceleration_scanner_keeps_repeated_hits_for_same_stock(self):
        engine = MarketEngine(redis_client=None)
        engine.broker = "kite"
        engine._restore_acceleration_hits_cache = lambda: None
        engine._save_acceleration_hits_cache = lambda: None
        now = datetime.now(IST)
        first = {
            "symbol": "INFY",
            "name": "Infosys",
            "timeframe": 1,
            "current_bucket": "2026-06-30T09:16:00+05:30",
            "previous_bucket": "2026-06-30T09:15:00+05:30",
            "direction": "up",
            "move_percent": 0.7,
            "from_close": 100,
            "to_close": 100.7,
        }
        second = dict(first)
        second.update(
            {
                "current_bucket": "2026-06-30T09:18:00+05:30",
                "previous_bucket": "2026-06-30T09:17:00+05:30",
                "move_percent": 1.2,
                "to_close": 101.2,
            }
        )

        engine._remember_acceleration_hit(first, min_gain=0.5, now=now)
        engine._remember_acceleration_hit(second, min_gain=0.5, now=now + timedelta(minutes=2))
        rows = engine._acceleration_hits_for_day(timeframe=1, min_gain=0.5, now=now)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["move_percent"], 1.2)
        self.assertEqual(rows[0]["repeat_count"], 2)
        self.assertEqual(rows[1]["repeat_count"], 2)

    def test_acceleration_hit_expires_after_two_minutes_unless_kept(self):
        engine = MarketEngine(redis_client=None)
        engine._restore_acceleration_hits_cache = lambda: None
        engine._save_acceleration_hits_cache = lambda: None
        now = datetime.now(IST).replace(hour=10, minute=0, second=0, microsecond=0)
        row = {
            "symbol": "INFY",
            "name": "Infosys",
            "timeframe": 1,
            "current_bucket": now.isoformat(),
            "previous_bucket": (now - timedelta(minutes=1)).isoformat(),
            "direction": "up",
            "move_percent": 0.8,
            "from_close": 100,
            "to_close": 100.8,
        }

        engine._remember_acceleration_hit(row, min_gain=0.5, now=now)
        active_rows = engine._acceleration_hits_for_day(timeframe=1, min_gain=0.5, now=now + timedelta(seconds=119))
        expired_rows = engine._acceleration_hits_for_day(timeframe=1, min_gain=0.5, now=now + timedelta(seconds=121))

        self.assertEqual(len(active_rows), 1)
        self.assertEqual(active_rows[0]["ttl_seconds"], 120)
        self.assertEqual(expired_rows, [])

    def test_acceleration_hit_keep_and_delete_actions(self):
        engine = MarketEngine(redis_client=None)
        engine._restore_acceleration_hits_cache = lambda: None
        engine._save_acceleration_hits_cache = lambda: None
        now = datetime.now(IST).replace(hour=10, minute=0, second=0, microsecond=0)
        row = {
            "symbol": "INFY",
            "name": "Infosys",
            "timeframe": 1,
            "current_bucket": now.isoformat(),
            "previous_bucket": (now - timedelta(minutes=1)).isoformat(),
            "direction": "up",
            "move_percent": 0.8,
            "from_close": 100,
            "to_close": 100.8,
        }

        engine._remember_acceleration_hit(row, min_gain=0.5, now=now)
        event_id = engine.acceleration_hits[engine._acceleration_hit_day_key(now)][0]["event_id"]

        keep_result = engine.update_acceleration_hit(event_id, "keep")
        kept_rows = engine._acceleration_hits_for_day(timeframe=1, min_gain=0.5, now=now + timedelta(hours=6))

        self.assertTrue(keep_result["ok"])
        self.assertEqual(len(kept_rows), 1)
        self.assertTrue(kept_rows[0]["kept"])
        self.assertIsNone(kept_rows[0]["expires_at"])

        delete_result = engine.update_acceleration_hit(event_id, "delete")
        deleted_rows = engine._acceleration_hits_for_day(timeframe=1, min_gain=0.5, now=now + timedelta(hours=6))

        self.assertTrue(delete_result["ok"])
        self.assertEqual(deleted_rows, [])

    def test_dhan_historical_daily_payload_matches_sdk_dates_without_future_day(self):
        from app.kite_engine import DhanClient

        fake_session = FakeSession(
            FakeResponse(
                {
                    "open": [100],
                    "high": [101],
                    "low": [99],
                    "close": [100.5],
                    "volume": [1000],
                    "timestamp": [1779906600],
                }
            )
        )
        client = DhanClient("client", "token", http_session=fake_session)

        candles = client.historical_data(
            ("NSE_EQ", "1594", "EQUITY"),
            datetime(2026, 5, 27),
            datetime(2026, 5, 27, 23, 59),
            "day",
        )

        self.assertEqual(fake_session.last_payload["fromDate"], "2026-05-27")
        self.assertEqual(fake_session.last_payload["toDate"], "2026-05-27")
        self.assertEqual(fake_session.last_payload["securityId"], "1594")
        self.assertEqual(fake_session.last_payload["exchangeSegment"], "NSE_EQ")
        self.assertEqual(fake_session.last_payload["instrument"], "EQUITY")
        self.assertEqual(candles[0]["high"], 101)
        self.assertEqual(candles[0]["volume"], 1000)

    def test_dhan_market_order_payload_uses_intraday_market_order(self):
        from app.kite_engine import DhanClient

        fake_session = FakeSession(FakeResponse({"status": "success", "orderId": "OID123"}))
        client = DhanClient("client-1", "token", http_session=fake_session)

        response = client.place_market_order("1594", "BUY", 20, correlation_id="TESTORDER")

        self.assertEqual(response["orderId"], "OID123")
        self.assertEqual(fake_session.last_payload["transactionType"], "BUY")
        self.assertEqual(fake_session.last_payload["exchangeSegment"], "NSE_EQ")
        self.assertEqual(fake_session.last_payload["productType"], "INTRADAY")
        self.assertEqual(fake_session.last_payload["orderType"], "MARKET")
        self.assertEqual(fake_session.last_payload["validity"], "DAY")
        self.assertEqual(fake_session.last_payload["dhanClientId"], "client-1")
        self.assertEqual(fake_session.last_payload["securityId"], "1594")
        self.assertEqual(fake_session.last_payload["quantity"], 20)
        self.assertEqual(fake_session.last_payload["disclosedQuantity"], 0)
        self.assertEqual(fake_session.last_payload["price"], 0.0)
        self.assertEqual(fake_session.last_payload["triggerPrice"], 0.0)
        self.assertIs(fake_session.last_payload["afterMarketOrder"], False)
        self.assertIsNone(fake_session.last_payload["boProfitValue"])
        self.assertIsNone(fake_session.last_payload["boStopLossValue"])
        self.assertNotIn("amoTime", fake_session.last_payload)

    def test_dhan_limit_order_payload_uses_price(self):
        from app.kite_engine import DhanClient

        fake_session = FakeSession(FakeResponse({"status": "success", "orderId": "OID124"}))
        client = DhanClient("client-1", "token", http_session=fake_session)

        response = client.place_order("1594", "BUY", 20, order_type="LIMIT", price=505.0, correlation_id="TESTLIMIT")

        self.assertEqual(response["orderId"], "OID124")
        self.assertEqual(fake_session.last_payload["orderType"], "LIMIT")
        self.assertEqual(fake_session.last_payload["price"], 505.0)
        self.assertEqual(fake_session.last_payload["productType"], "INTRADAY")

    def test_broker_start_prioritizes_websocket_before_dashboard_warmup(self):
        class FakeKiteConnect:
            def __init__(self, api_key=None):
                self.api_key = api_key

            def set_access_token(self, access_token):
                self.access_token = access_token

            def set_session_expiry_hook(self, hook):
                self.hook = hook

        engine = MarketEngine(redis_client=None)
        events = []
        engine._is_market_open = lambda: True
        engine.build_universe = lambda *args, **kwargs: events.append(("build", kwargs.get("warm_dashboard")))
        engine._restore_previous_close_cache = lambda: events.append(("restore_prev_close", None))
        engine._create_ticker = lambda: events.append(("websocket", None)) or True
        engine._ensure_background_refresh = lambda **kwargs: events.append(("background", kwargs.get("reason"))) or True
        engine._refresh_rest_snapshot = lambda *args, **kwargs: self.fail("REST snapshot should run in background after websocket startup")
        engine._refresh_sector_snapshot = lambda *args, **kwargs: self.fail("Sector snapshot should run in background after websocket startup")

        with patch("app.kite_engine.KiteConnect", FakeKiteConnect):
            engine.start("api-key", "access-token", ["NIFTY IT"])

        self.assertEqual(
            events,
            [
                ("build", False),
                ("restore_prev_close", None),
                ("websocket", None),
                ("background", "startup_snapshot"),
            ],
        )

    def test_dhan_start_uses_cached_universe_before_background_rebuild(self):
        engine = MarketEngine(redis_client=None)
        events = []
        engine._restore_dhan_universe_cache = lambda sector_names=None: events.append(("restore_universe", None)) or True
        engine._restore_previous_close_cache = lambda: events.append(("restore_prev_close", None))
        engine._create_ticker = lambda: events.append(("websocket", None)) or True
        engine._refresh_dhan_universe_background = lambda sector_names: events.append(("background_universe", tuple(sector_names)))
        engine._ensure_background_refresh = lambda **kwargs: events.append(("background_snapshot", kwargs.get("reason"))) or True
        engine.build_universe = lambda *args, **kwargs: self.fail("Dhan startup should not block on full universe rebuild when cache exists")
        engine._is_market_open = lambda: True

        engine.start_dhan("client-1", "token-1", ["NIFTY IT"])

        self.assertEqual(
            events,
            [
                ("restore_universe", None),
                ("restore_prev_close", None),
                ("websocket", None),
                ("background_universe", ("NIFTY IT",)),
                ("background_snapshot", "startup_snapshot"),
            ],
        )

    @patch("app.kite_engine.load_market_cache")
    def test_dhan_universe_cache_restore_builds_fast_subscription_maps(self, load_cache):
        load_cache.return_value = {
            "symbol_to_token": {"INFY": 1594, "TCS": 11536},
            "token_to_symbol": {"1594": "INFY", "11536": "TCS"},
            "symbol_to_name": {"INFY": "Infosys", "TCS": "Tata Consultancy Services"},
            "dhan_security_to_segment": {"1594": "NSE_EQ", "11536": "NSE_EQ"},
            "dhan_security_to_instrument": {"1594": "EQUITY", "11536": "EQUITY"},
            "index_tokens": {"NIFTY 50": 13},
            "sector_tokens": {},
            "fno_symbols": ["INFY"],
        }
        engine = MarketEngine(redis_client=None)
        engine.nifty500_set = {"INFY"}

        restored = engine._restore_dhan_universe_cache(["NIFTY IT"])

        self.assertTrue(restored)
        self.assertEqual(engine.symbol_to_token["INFY"], 1594)
        self.assertEqual(engine.token_to_symbol[11536], "TCS")
        self.assertEqual(engine.equity_tokens, [1594, 11536])
        self.assertEqual(engine.sector_tokens["NIFTY IT"], 29)
        self.assertEqual(engine.index_tokens["NIFTY 50"], 13)
        self.assertEqual(engine.dhan_security_to_segment[29], "IDX_I")
        self.assertEqual(engine.dhan_security_to_instrument[1594], "EQUITY")

    def test_live_feed_stale_check_gives_new_dhan_socket_connect_grace(self):
        engine = MarketEngine(redis_client=None)
        engine.kite = object()
        engine.ticker = object()
        engine.last_ticker_start_ts = time.time()
        engine.last_connect_ts = time.time()
        engine.last_tick_ts = 0
        engine._is_market_open = lambda: True

        self.assertFalse(engine._is_live_feed_stale())

        engine.last_connect_ts = time.time() - 90

        self.assertTrue(engine._is_live_feed_stale())

    def test_snapshot_runtime_fields_expose_live_feed_diagnostics(self):
        engine = MarketEngine(redis_client=None)
        engine.connected = False
        engine.last_error = "Dhan WebSocket closed: 1006"
        engine.last_tick_ts = time.time() - 12
        engine.last_connect_ts = time.time() - 3
        engine.live_feed_subscription_count = 498
        engine.live_feed_last_close = "1006"

        snapshot = engine._with_runtime_fields({"gainers": [], "losers": []}, market_open=True, source="api")

        self.assertFalse(snapshot["connected"])
        self.assertEqual(snapshot["error"], "Dhan WebSocket closed: 1006")
        self.assertEqual(snapshot["live_feed_subscription_count"], 498)
        self.assertEqual(snapshot["live_feed_last_close"], "1006")
        self.assertGreaterEqual(snapshot["last_tick_age_seconds"], 11)
        self.assertLessEqual(snapshot["last_connect_age_seconds"], 4)

    def test_acceleration_order_computes_quantity_from_capital_and_ltp(self):
        from app.kite_engine import DhanClient

        fake_session = FakeSession(FakeResponse({"status": "success", "orderId": "OID456"}))
        engine = MarketEngine(redis_client=None)
        engine.broker = "dhan"
        engine.kite = DhanClient("client-1", "token", http_session=fake_session)
        engine.symbol_to_token = {"INFY": 1594}
        engine.dhan_security_to_segment = {1594: "NSE_EQ"}
        engine.dhan_security_to_instrument = {1594: "EQUITY"}
        engine.latest = {"INFY": {"price": 500.0}}

        result = engine.place_acceleration_market_order(
            "INFY",
            "BUY",
            per_trade_capital=10000,
            buy_limit_offset_pct=2,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["quantity"], 20)
        self.assertEqual(result["order_id"], "OID456")
        self.assertEqual(result["product_type"], "INTRADAY")
        self.assertEqual(result["order_type"], "LIMIT")
        self.assertEqual(result["limit_price"], 510.0)
        self.assertEqual(result["limit_offset_pct"], 2)
        self.assertEqual(fake_session.last_payload["quantity"], 20)
        self.assertEqual(fake_session.last_payload["orderType"], "LIMIT")
        self.assertEqual(fake_session.last_payload["price"], 510.0)

    def test_acceleration_order_supports_kite_mis_market_order(self):
        class FakeKiteOrder:
            def __init__(self):
                self.last_payload = None

            def place_order(self, **kwargs):
                self.last_payload = kwargs
                return "KITE123"

        fake_kite = FakeKiteOrder()
        engine = MarketEngine(redis_client=None)
        engine.broker = "kite"
        engine.kite = fake_kite
        engine.symbol_to_token = {"INFY": 1594}
        engine.latest = {"INFY": {"price": 500.0}}

        result = engine.place_acceleration_market_order(
            "INFY",
            "SELL",
            per_trade_capital=10000,
            sell_limit_offset_pct=2,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["quantity"], 20)
        self.assertEqual(result["order_id"], "KITE123")
        self.assertEqual(result["product_type"], "MIS")
        self.assertEqual(result["order_type"], "LIMIT")
        self.assertEqual(result["limit_price"], 490.0)
        self.assertEqual(result["limit_offset_pct"], 2)
        self.assertEqual(fake_kite.last_payload["tradingsymbol"], "INFY")
        self.assertEqual(fake_kite.last_payload["transaction_type"], "SELL")
        self.assertEqual(fake_kite.last_payload["product"], "MIS")
        self.assertEqual(fake_kite.last_payload["order_type"], "LIMIT")
        self.assertEqual(fake_kite.last_payload["price"], 490.0)

    def test_dhan_historical_parser_accepts_sdk_style_rows_with_volume(self):
        from app.kite_engine import DhanClient

        client = DhanClient("client", "token", http_session=FakeSession(FakeResponse({})))
        candles = client._candles_from_dhan_response(
            {
                "status": "success",
                "data": {
                    "data": [
                        {
                            "timestamp": 1778207400,
                            "open": 100,
                            "high": 105,
                            "low": 99,
                            "close": 103,
                            "Volume": "12345.0",
                        }
                    ]
                },
            }
        )

        self.assertEqual(len(candles), 1)
        self.assertEqual(candles[0]["volume"], "12345.0")

    def test_dhan_historical_parser_accepts_start_time_volume_arrays(self):
        from app.kite_engine import DhanClient

        client = DhanClient("client", "token", http_session=FakeSession(FakeResponse({})))
        candles = client._candles_from_dhan_response(
            {
                "open": [3750.0, 3757.85],
                "high": [3750.0, 3757.90],
                "low": [3746.1, 3746.10],
                "close": [3751.25, 3751.25],
                "volume": [166, 53629],
                "start_Time": [1328845020, 1328845500],
            }
        )

        self.assertEqual(len(candles), 2)
        self.assertEqual(candles[0]["volume"], 166)
        self.assertEqual(candles[1]["volume"], 53629)

    def test_dhan_quote_rate_limit_sets_cooldown(self):
        engine = MarketEngine(redis_client=None)
        engine.broker = "dhan"
        engine.kite = RateLimitedDhan()
        engine.symbol_to_token = {"INFY": 123}
        engine.token_to_symbol = {123: "INFY"}

        quoted = engine._quote_symbols(engine.kite, ["INFY"])

        self.assertEqual(quoted, {})
        self.assertTrue(engine._is_quote_rate_limited())

    def test_live_feed_stale_check_gives_new_ticker_connect_grace(self):
        engine = MarketEngine(redis_client=None)
        engine.kite = object()
        engine.ticker = object()
        engine._is_market_open = lambda: True
        engine.last_tick_ts = 0
        engine.last_connect_ts = 0
        engine.last_ticker_start_ts = time.time()

        self.assertFalse(engine._is_live_feed_stale())

        engine.last_ticker_start_ts = time.time() - 60
        self.assertTrue(engine._is_live_feed_stale())

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
                    "volume": 1000,
                    "is_fno": True,
                    "sectors": ["NIFTY IT"],
                    "previous_day_change": -9.0,
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
        self.assertEqual(payload["stocks"][0]["change"], 1.25)
        self.assertEqual(payload["stocks"][0]["turnover"], 1500000.0)
        self.assertEqual(payload["stocks"][1]["symbol"], "TCS")

    def test_nifty_it_breakdown_uses_fallback_members_when_csv_is_unavailable(self):
        engine = MarketEngine(redis_client=None)
        engine.kite = object()
        engine._is_market_open = lambda: True
        engine.symbol_to_token = {"INFY": 1, "TCS": 2, "WIPRO": 3}
        engine.symbol_to_name = {"INFY": "Infosys", "TCS": "TCS", "WIPRO": "Wipro"}
        engine._fetch_sector_constituent_url = lambda page_url: None
        engine._quote_symbols = lambda kite, symbols: {
            f"NSE:{symbol}": {
                "last_price": 100 + idx,
                "volume": 1000,
                "ohlc": {"close": 100},
            }
            for idx, symbol in enumerate(symbols)
        }
        engine._cached_latest_rows = lambda: {}
        engine._cached_previous_close = lambda bucket, key: 100
        engine._save_latest_rows_cache = lambda: None
        engine._save_previous_close_cache = lambda: None

        payload = engine.get_sector_breakdown("NIFTY IT")

        self.assertEqual(payload["sector"], "NIFTY IT")
        self.assertGreaterEqual(payload["constituent_count"], 3)
        self.assertIn("INFY", [row["symbol"] for row in payload["stocks"]])

    def test_nifty_metal_breakdown_uses_fallback_members_when_csv_is_unavailable(self):
        engine = MarketEngine(redis_client=None)
        engine.kite = object()
        engine._is_market_open = lambda: True
        engine.symbol_to_token = {"TATASTEEL": 1, "JSWSTEEL": 2, "HINDALCO": 3, "JSL": 4}
        engine.symbol_to_name = {
            "TATASTEEL": "Tata Steel",
            "JSWSTEEL": "JSW Steel",
            "HINDALCO": "Hindalco",
            "JSL": "Jindal Stainless",
        }
        engine._fetch_sector_constituent_url = lambda page_url: None
        engine._quote_symbols = lambda kite, symbols: {
            f"NSE:{symbol}": {
                "last_price": 100 + idx,
                "volume": 1000,
                "ohlc": {"close": 100},
            }
            for idx, symbol in enumerate(symbols)
        }
        engine._cached_latest_rows = lambda: {}
        engine._cached_previous_close = lambda bucket, key: 100
        engine._save_latest_rows_cache = lambda: None
        engine._save_previous_close_cache = lambda: None

        payload = engine.get_sector_breakdown("NIFTY METAL")

        self.assertEqual(payload["sector"], "NIFTY METAL")
        self.assertGreaterEqual(payload["constituent_count"], 4)
        self.assertIn("TATASTEEL", [row["symbol"] for row in payload["stocks"]])

    def test_closed_market_sector_breakdown_prefers_latest_rows_cache_over_stale_breakdown_cache(self):
        engine = MarketEngine(redis_client=None)
        engine.kite = object()
        engine.latest = {}
        engine._is_market_open = lambda: False
        engine._completed_session_cache_marker = lambda: "2026-05-22"
        engine.sector_members = {"NIFTY IT": ["INFY", "TCS"]}
        engine._refresh_sector_memberships = lambda *args, **kwargs: None
        engine._cached_latest_rows = lambda: {
            "rows": {
                "INFY": {
                    "symbol": "INFY",
                    "name": "Infosys",
                    "price": 1500.0,
                    "change": 1.25,
                    "volume": 123,
                    "is_fno": True,
                    "sectors": ["NIFTY IT"],
                },
                "TCS": {
                    "symbol": "TCS",
                    "name": "TCS",
                    "price": 4200.0,
                    "change": -0.5,
                    "volume": 456,
                    "is_fno": True,
                    "sectors": ["NIFTY IT"],
                },
            },
            "updated_at": "2026-05-22T15:30:00+05:30",
            "snapshot_source": "historical_eod",
        }
        engine._cached_sector_breakdowns = lambda: {
            "NIFTY IT": {
                "session_marker": "2026-05-22",
                "stocks": [
                    {"symbol": "INFY", "price": 1, "change": -20},
                    {"symbol": "TCS", "price": 1, "change": -30},
                ],
            }
        }
        engine._quote_symbols = lambda *args, **kwargs: self.fail("closed sector breakdown should use shared row cache")

        payload = engine.get_sector_breakdown("NIFTY IT")

        self.assertEqual([row["symbol"] for row in payload["stocks"]], ["INFY", "TCS"])
        self.assertEqual(payload["stocks"][0]["change"], 1.25)
        self.assertEqual(payload["stocks"][0]["turnover"], 184500.0)
        self.assertEqual(payload["session_marker"], "2026-05-22")

    def test_closed_market_sector_breakdown_uses_quote_ltp_with_cached_previous_close(self):
        engine = MarketEngine(redis_client=None)
        engine.kite = object()
        engine._is_market_open = lambda: False
        engine._completed_session_cache_marker = lambda: "2026-07-03"
        engine.sector_members = {"NIFTY IT": ["HCLTECH"]}
        engine.symbol_to_token = {"HCLTECH": 7229}
        engine.symbol_to_name = {"HCLTECH": "HCL Technologies"}
        engine.previous_close_cache = {
            "symbols": {
                "HCLTECH": {
                    "cache_marker": "2026-07-03",
                    "broker": "kite",
                    "close": 1084.76,
                }
            }
        }
        engine.latest = {
            "HCLTECH": {
                "symbol": "HCLTECH",
                "price": 1139.0,
                "change": 0.0,
                "volume": 12770000,
            }
        }
        engine._cached_latest_rows = lambda: {"rows": {}}
        engine._cached_sector_breakdowns = lambda: {}
        engine._quote_symbols = lambda kite, symbols: {
            "NSE:HCLTECH": {
                "last_price": 1139.0,
                "volume": 12770000,
                "ohlc": {},
            }
        }
        engine._save_latest_rows_cache = lambda: None
        engine._save_previous_close_cache = lambda: None

        payload = engine.get_sector_breakdown("NIFTY IT")

        self.assertEqual(payload["stocks"][0]["symbol"], "HCLTECH")
        self.assertAlmostEqual(payload["stocks"][0]["change"], 5.0, places=1)
        self.assertEqual(payload["stocks"][0]["turnover"], 14545030000.0)

    def test_closed_market_cache_prefers_persisted_latest_rows_over_zero_memory_rows(self):
        engine = MarketEngine(redis_client=None)
        engine._is_market_open = lambda: False
        engine.latest = {"HCLTECH": {"symbol": "HCLTECH", "price": 1139, "change": 0.0}}
        engine._cached_latest_rows = lambda: {
            "rows": {
                "HCLTECH": {
                    "symbol": "HCLTECH",
                    "price": 1139,
                    "change": 5.0,
                    "volume": 12770000,
                }
            },
            "updated_at": "2026-07-03T15:30:00+05:30",
            "snapshot_source": "api",
        }

        rows = engine._rows_for_symbols_from_cache(["HCLTECH"])

        self.assertEqual(rows[0]["change"], 5.0)

    def test_closed_market_sector_breakdown_can_use_previous_close_cache_without_latest_rows(self):
        engine = MarketEngine(redis_client=None)
        engine.kite = None
        engine.latest = {}
        engine._is_market_open = lambda: False
        engine._completed_session_cache_marker = lambda: "2026-05-22"
        engine.sector_members = {"NIFTY MEDIA": ["ZEEL", "PVRINOX"]}
        engine.symbol_to_name = {"ZEEL": "Zee Entertainment", "PVRINOX": "PVR Inox"}
        engine.symbol_to_sectors = {"ZEEL": ["NIFTY MEDIA"], "PVRINOX": ["NIFTY MEDIA"]}
        engine.previous_close_cache = {
            "symbols": {
                "ZEEL": {"cache_marker": "2026-05-22", "close": 91.46},
                "PVRINOX": {"cache_marker": "2026-05-22", "close": 957.8},
            }
        }
        engine.previous_day_badges_cache = {
            "ZEEL": {"cache_marker": "2026-05-22", "change": 3.72},
            "PVRINOX": {"cache_marker": "2026-05-22", "change": 0.78},
        }
        engine._cached_latest_rows = lambda: {"rows": {}}
        engine._cached_sector_breakdowns = lambda: {}

        payload = engine.get_sector_breakdown("NIFTY MEDIA")

        self.assertEqual(payload["constituent_count"], 2)
        self.assertEqual([row["symbol"] for row in payload["stocks"]], ["ZEEL", "PVRINOX"])
        self.assertEqual(payload["stocks"][0]["change"], 3.72)
        self.assertEqual(payload["stocks"][1]["price"], 957.8)

    def test_closed_market_sector_breakdown_prefers_cached_sector_payload(self):
        engine = MarketEngine(redis_client=None)
        engine._is_market_open = lambda: False
        engine._completed_session_cache_marker = lambda: "2026-05-12"
        engine._cached_sector_breakdowns = lambda: {
            "NIFTY IT": {
                "sector": "NIFTY IT",
                "session_marker": "2026-05-12",
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

    def test_live_sector_breakdown_uses_quote_volume_field(self):
        engine = MarketEngine(redis_client=None)
        engine.kite = object()
        engine._is_market_open = lambda: True
        engine.symbol_to_token = {"INFY": 123}
        engine.sector_members = {"NIFTY IT": ["INFY"]}
        engine._refresh_sector_memberships = lambda *args, **kwargs: None
        engine._decorate_rows_with_previous_day_badges = lambda rows, fetch_missing=True: rows
        engine._quote_symbols = lambda kite, symbols: {
            "NSE:INFY": {
                "last_price": 1502.0,
                "volume": 321000,
                "ohlc": {"close": 1490.0},
            }
        }
        engine._save_latest_rows_cache = lambda: None

        payload = engine.get_sector_breakdown("NIFTY IT")

        self.assertEqual(payload["stocks"][0]["symbol"], "INFY")
        self.assertEqual(payload["stocks"][0]["volume"], 321000)

    def test_build_rrg_payload_from_series_returns_quadrant_coordinates(self):
        engine = MarketEngine(redis_client=None)
        benchmark_series = [(f"2026-05-{day:02d}", 100 + day) for day in range(1, 25)]
        component_series = {
            "NIFTY IT": [(f"2026-05-{day:02d}", 100 + day * 1.4) for day in range(1, 25)],
            "NIFTY FMCG": [(f"2026-05-{day:02d}", 124 - day * 0.6) for day in range(1, 25)],
        }

        payload = engine._build_rrg_payload_from_series("NIFTY 50", benchmark_series, component_series)

        self.assertEqual(payload["benchmark"], "NIFTY 50")
        self.assertEqual(len(payload["items"]), 2)
        self.assertIn("trail", payload["items"][0])
        self.assertIn("quadrant", payload["items"][0])
        self.assertTrue(all("x" in point and "y" in point for point in payload["items"][0]["trail"]))

    def test_rrg_history_fetch_uses_wider_window_than_chart_lookback(self):
        engine = MarketEngine(redis_client=None)
        engine.kite = object()
        engine.index_tokens = {"NIFTY 50": 13}
        engine.sector_tokens = {"NIFTY IT": 29}
        requested_windows = []
        sessions = [datetime(2026, 5, day).date() for day in range(1, 31)]
        engine._trading_session_window = lambda end_date, sessions_count: sessions[-sessions_count:]
        engine._session_start_dt = lambda session_date: session_date
        engine._session_end_dt = lambda session_date: session_date

        def fake_series(token, from_date, to_date):
            requested_windows.append((from_date, to_date))
            return [(f"2026-05-{day:02d}", 100 + day + token / 1000) for day in range(1, 31)]

        engine._fetch_rrg_price_series = fake_series

        benchmark, components, error = engine._build_rrg_series_map("NIFTY 50", "2026-05-30")

        self.assertIsNone(error)
        self.assertEqual(len(benchmark), 30)
        self.assertIn("NIFTY IT", components)
        self.assertEqual(requested_windows[0][0], sessions[0])

    def test_relative_rotation_uses_cached_same_session_payload(self):
        engine = MarketEngine(redis_client=None)
        engine.kite = object()
        engine._is_market_open = lambda: False
        engine._completed_session_cache_marker = lambda: "2026-05-09"
        engine._cached_relative_rotation_graph = lambda: {
            "benchmark": "NIFTY 50",
            "cache_marker": "2026-05-09",
            "market_open": False,
            "items": [{"sector": "NIFTY IT", "trail": [{"x": 101, "y": 102}]}],
        }

        payload = engine.get_relative_rotation_graph()

        self.assertEqual(payload["benchmark"], "NIFTY 50")
        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["cache_marker"], "2026-05-09")

    def test_start_daily_market_history_cache_marks_already_cached_session(self):
        engine = MarketEngine(redis_client=None)
        engine._completed_session_cache_marker = lambda: "2026-05-09"
        engine._cached_relative_rotation_graph = lambda: {
            "cache_marker": "2026-05-09",
            "broker": "kite",
            "items": [{"sector": "NIFTY IT"}],
        }
        engine._cached_swing_scanner_payload = lambda: {
            "cache_marker": "2026-05-09",
            "broker": "kite",
            "cache_version": SWING_SCANNER_CACHE_VERSION,
            "rows": [{"symbol": "INFY"}],
            "error": None,
        }

        status = engine.start_daily_market_history_cache(force=False)

        self.assertEqual(status["status"], "completed")
        self.assertEqual(status["session_marker"], "2026-05-09")
        self.assertEqual(status["broker"], "kite")
        self.assertIn("already ready", status["message"])

    def test_dhan_history_cache_does_not_reuse_kite_ready_cache(self):
        engine = MarketEngine(redis_client=None)
        engine.broker = "dhan"
        engine._completed_session_cache_marker = lambda: "2026-05-09"
        engine._cached_relative_rotation_graph = lambda: {
            "cache_marker": "2026-05-09",
            "broker": "kite",
            "items": [{"sector": "NIFTY IT"}],
        }
        started = []

        class ImmediateThread:
            def __init__(self, target, args=(), daemon=None):
                self.target = target
                self.args = args
                self.daemon = daemon

            def start(self):
                started.append(True)

            def is_alive(self):
                return False

        with patch("app.kite_engine.threading.Thread", ImmediateThread):
            status = engine.start_daily_market_history_cache(force=False)

        self.assertEqual(status["status"], "running")
        self.assertEqual(status["broker"], "dhan")
        self.assertTrue(started)

    def test_dhan_throttled_history_uses_selected_broker_security_tuple(self):
        class FakeDhan:
            def __init__(self):
                self.calls = []

            def historical_data(self, token, from_date, to_date, interval):
                self.calls.append((token, from_date, to_date, interval))
                return [{"date": from_date, "close": 100}]

        engine = MarketEngine(redis_client=None)
        engine.broker = "dhan"
        engine.kite = FakeDhan()
        engine.dhan_security_to_segment = {123: "IDX_I"}
        engine.dhan_security_to_instrument = {123: "INDEX"}

        candles = engine._throttled_historical_day_data(
            123,
            datetime(2026, 5, 8),
            datetime(2026, 5, 9),
        )

        self.assertEqual(candles[0]["close"], 100)
        self.assertEqual(engine.kite.calls[0][0], ("IDX_I", "123", "INDEX"))


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

    def test_relative_rotation_endpoint_returns_payload(self):
        with patch.object(
            main_module.engine,
            "get_relative_rotation_graph",
            return_value={
                "benchmark": "NIFTY 50",
                "market_open": False,
                "updated_at": "2026-05-10T10:00:00+05:30",
                "items": [
                    {
                        "sector": "NIFTY IT",
                        "quadrant": "Leading",
                        "color": "#00e5a0",
                        "rs_ratio": 103.2,
                        "rs_momentum": 101.4,
                        "trail": [{"date": "2026-05-09", "x": 101.4, "y": 103.2}],
                        "relative_strength": 108.4,
                        "latest_price": 25433.5,
                    }
                ],
                "x_domain": [95, 105],
                "y_domain": [95, 105],
            },
        ):
            with TestClient(main_module.app) as client:
                response = client.get("/api/relative-rotation")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["benchmark"], "NIFTY 50")
        self.assertEqual(response.json()["items"][0]["sector"], "NIFTY IT")

    def test_admin_market_history_cache_endpoint_requires_admin(self):
        with TestClient(main_module.app) as client:
            response = client.post("/api/admin/market-history/cache")

        self.assertEqual(response.status_code, 403)
        self.assertFalse(response.json()["ok"])

    def test_admin_market_history_cache_endpoint_returns_status_for_admin(self):
        with patch.object(main_module, "require_admin", return_value={"id": 1}), patch.object(
            main_module.engine,
            "start_daily_market_history_cache",
            return_value={"status": "running", "session_marker": "2026-05-09"},
        ):
            with TestClient(main_module.app) as client:
                response = client.post("/api/admin/market-history/cache")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertEqual(response.json()["status"]["status"], "running")

    def test_admin_market_history_status_endpoint_returns_status_for_admin(self):
        with patch.object(main_module, "require_admin", return_value={"id": 1}), patch.object(
            main_module.engine,
            "get_history_cache_status",
            return_value={"status": "completed", "session_marker": "2026-05-09"},
        ):
            with TestClient(main_module.app) as client:
                response = client.get("/api/admin/market-history/status")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertEqual(response.json()["status"]["status"], "completed")


if __name__ == "__main__":
    unittest.main()
