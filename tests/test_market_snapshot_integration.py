import os
import tempfile
import time
import unittest
from datetime import datetime
from unittest.mock import patch

from fastapi.testclient import TestClient

_TMP_DIR = tempfile.TemporaryDirectory()
os.environ["TRADE_DASHBOARD_DB_PATH"] = os.path.join(_TMP_DIR.name, "test_trade_dashboard.db")

from app.kite_engine import MarketEngine
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
                "session_marker": "2026-05-21",
                "stocks": [{"symbol": "OLD", "price": 1, "change": 0}],
            }
        }
        engine._quote_symbols = lambda *args, **kwargs: self.fail("closed sector breakdown should use shared row cache")

        payload = engine.get_sector_breakdown("NIFTY IT")

        self.assertEqual([row["symbol"] for row in payload["stocks"]], ["INFY", "TCS"])
        self.assertEqual(payload["session_marker"], "2026-05-22")

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
