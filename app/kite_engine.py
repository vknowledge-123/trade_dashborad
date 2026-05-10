import csv
import io
import math
import re
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, time as dtime, timezone
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests
from kiteconnect import KiteConnect, KiteTicker

from app.config import KITE_ACCESS_TOKEN_KEY, KITE_TOKEN_UPDATED_KEY
from app.db import load_market_cache, save_market_cache

SNAPSHOT_CACHE_KEY = "latest_snapshot"
CLOSED_SNAPSHOT_CACHE_KEY = "latest_closed_snapshot"
LATEST_ROWS_CACHE_KEY = "latest_rows"
SECTOR_MEMBERS_CACHE_KEY = "sector_memberships"
SECTOR_BREAKDOWNS_CACHE_KEY = "sector_breakdowns"
PREVIOUS_DAY_BADGES_CACHE_KEY = "previous_day_badges"
RRG_CACHE_KEY = "relative_rotation_graph"
IST = ZoneInfo("Asia/Kolkata")
LIVE_FEED_STALE_AFTER_SECONDS = 15
LIVE_FEED_RECONNECT_COOLDOWN_SECONDS = 20
SECTOR_SNAPSHOT_REFRESH_SECONDS = 5
RRG_BENCHMARK_SYMBOL = "NIFTY 50"
RRG_LOOKBACK_SESSIONS = 15
RRG_TRAIL_POINTS = 14
RRG_NORMALIZATION_WINDOW = 14
HISTORICAL_DAY_REQUEST_DELAY_SECONDS = 0.35
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/135.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,text/csv,*/*;q=0.8",
}
SECTOR_INDEX_PAGES = {
    "NIFTY AUTO": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-auto",
    "NIFTY IT": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-it",
    "NIFTY METAL": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-metal",
    "NIFTY FINSEREXBNK": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-financial--services-ex-bank",
    "NIFTY MS FIN SERV": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-midsmall--financial-services",
    "NIFTY HEALTHCARE": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-healthcare-index",
    "NIFTY MIDSML HLTH": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-midsmallhealthcare",
    "NIFTY PSU BANK": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-psu-bank",
    "NIFTY CONSR DURBL": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-consumer-durables-index",
    "NIFTY FMCG": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-fmcg",
    "NIFTY PVT BANK": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-private-bank",
    "NIFTY ENERGY": "https://www.niftyindices.com/indices/equity/thematic-indices/nifty-energy",
    "NIFTY CPSE": "https://www.niftyindices.com/indices/equity/thematic-indices/nifty-cpse",
    "NIFTY MS IT TELCM": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-midsmall--it-telecom",
    "NIFTY IND DEFENCE": "https://www.niftyindices.com/indices/equity/thematic-indices/nifty-india-defence",
    "NIFTY MEDIA": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-media",
    "NIFTY IND DIGITAL": "https://www.niftyindices.com/indices/equity/thematic-indices/nifty-india-digital",
    "NIFTY IND TOURISM": "https://www.niftyindices.com/indices/equity/thematic-indices/nifty-india-tourism",
    "NIFTY CAPITAL MKT": "https://www.niftyindices.com/indices/equity/thematic-indices/nifty-capital-markets",
    "NIFTY OIL AND GAS": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-oil-and-gas-index",
    "NIFTY INDIA MFG": "https://www.niftyindices.com/indices/equity/thematic-indices/nifty-india-manufacturing",
}
NSE_TRADING_HOLIDAYS = {
    "2026-01-26",
    "2026-03-03",
    "2026-03-26",
    "2026-03-31",
    "2026-04-03",
    "2026-04-14",
    "2026-05-01",
    "2026-05-28",
    "2026-06-26",
    "2026-09-14",
    "2026-10-02",
    "2026-10-20",
    "2026-11-10",
    "2026-11-24",
    "2026-12-25",
}


class MarketEngine:
    def __init__(self, redis_client):
        self.redis = redis_client
        self.api_key = None
        self.access_token = None
        self.sector_names = []
        self.ticker = None
        self.thread = None
        self.kite = None
        self.lock = threading.Lock()
        self.token_to_symbol = {}
        self.symbol_to_token = {}
        self.symbol_to_name = {}
        self.symbol_to_sectors = {}
        self.fno_symbols = set()
        self.fno_override = set()
        self.nifty500_set = set()
        self.equity_tokens = []
        self.index_tokens = {}
        self.sector_tokens = {}
        self.sector_token_to_name = {}
        self.sector_members = {}
        self.sector_prev_close = {}
        self.rest_prev_close = {}
        self.latest = {}
        self.sector_latest = {}
        self.connected = False
        self.last_error = None
        self.last_update = None
        self.last_tick_ts = 0.0
        self.last_connect_ts = 0.0
        self.last_reconnect_attempt_ts = 0.0
        self.demo_mode = False
        self.demo_snapshot = None
        self.last_sector_quote_ts = 0
        self.last_rest_refresh_ts = 0
        self.last_closed_refresh_ts = 0
        self.last_membership_refresh_date = None
        self.last_snapshot_source = "empty"
        self.refresh_lock = threading.Lock()
        self.refresh_thread = None
        self.refresh_reason = None
        self.start_lock = threading.Lock()
        self.previous_day_badges_cache = {}
        self.historical_fetch_lock = threading.Lock()
        self.last_historical_fetch_ts = 0.0
        self.history_cache_lock = threading.Lock()
        self.history_cache_thread = None
        self.history_cache_status = {
            "status": "idle",
            "session_marker": None,
            "started_at": None,
            "finished_at": None,
            "processed": 0,
            "total": 0,
            "message": "No market history cache job has been started yet.",
            "error": None,
        }
        self.badge_warm_lock = threading.Lock()
        self.badge_warm_thread = None
        self.pending_badge_symbols = set()
        self.http = requests.Session()
        self.http.headers.update(HTTP_HEADERS)

    def _extract_underlying(self, tradingsymbol):
        match = re.match(r"^[A-Z]+", tradingsymbol)
        return match.group(0) if match else tradingsymbol

    def _chunked(self, items, size):
        for idx in range(0, len(items), size):
            yield items[idx:idx + size]

    def _utc_now(self):
        return datetime.now(IST).isoformat(timespec="seconds")

    def _is_trading_session_date(self, session_date):
        return (
            session_date.weekday() < 5
            and session_date.isoformat() not in NSE_TRADING_HOLIDAYS
        )

    def _previous_trading_session_date(self, session_date):
        probe = session_date - timedelta(days=1)
        while not self._is_trading_session_date(probe):
            probe -= timedelta(days=1)
        return probe

    def _latest_completed_session_date(self, now=None):
        moment = now or datetime.now(IST)
        session_date = moment.date()
        if self._is_trading_session_date(session_date) and moment.time() >= dtime(15, 30):
            return session_date
        return self._previous_trading_session_date(session_date)

    def _trading_session_window(self, end_session_date, sessions):
        if sessions <= 0:
            return []
        dates = []
        probe = end_session_date
        while len(dates) < sessions:
            if self._is_trading_session_date(probe):
                dates.append(probe)
            probe -= timedelta(days=1)
        dates.reverse()
        return dates

    def _session_start_dt(self, session_date):
        return datetime.combine(session_date, dtime.min, tzinfo=IST)

    def _session_end_dt(self, session_date):
        return datetime.combine(session_date, dtime.max, tzinfo=IST)

    def _completed_session_cache_marker(self):
        return self._latest_completed_session_date().isoformat()

    def _historical_date_arg(self, value):
        if hasattr(value, "date"):
            value = value.date()
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return value

    def _tracked_feed_activity_ts(self):
        return max(self.last_tick_ts, self.last_connect_ts)

    def _is_live_feed_stale(self):
        if not self.kite or not self._is_market_open():
            return False
        activity_ts = self._tracked_feed_activity_ts()
        if not activity_ts:
            return bool(self.ticker)
        return (time.time() - activity_ts) > LIVE_FEED_STALE_AFTER_SECONDS

    def _close_ticker(self):
        if not self.ticker:
            return
        try:
            self.ticker.close()
        except Exception:
            pass
        self.ticker = None

    def _subscribed_tokens(self):
        tokens = list(self.equity_tokens)
        sector_token_list = list(self.sector_tokens.values())
        all_tokens = tokens + sector_token_list
        if len(all_tokens) > 3000:
            max_eq = max(0, 3000 - len(sector_token_list))
            all_tokens = tokens[:max_eq] + sector_token_list
        return all_tokens, sector_token_list

    def _create_ticker(self):
        if not self.api_key or not self.access_token:
            return False
        all_tokens, sector_token_list = self._subscribed_tokens()
        print(f"[engine] equity_tokens={len(self.equity_tokens)} sector_tokens={len(sector_token_list)} subscribed={len(all_tokens)}")
        if sector_token_list:
            print(f"[engine] sector tokens: {sorted(self.sector_tokens.keys())}")
        else:
            print("[engine] WARNING: no sector tokens found for provided sector list")

        self.connected = False
        self.last_connect_ts = 0.0
        self.last_tick_ts = 0.0
        self.ticker = KiteTicker(
            self.api_key,
            self.access_token,
            reconnect=True,
            reconnect_max_tries=50,
            reconnect_max_delay=60,
            connect_timeout=30,
        )
        self.ticker.on_connect = lambda ws, resp: self._on_connect(ws, resp, all_tokens)
        self.ticker.on_ticks = self._on_ticks
        self.ticker.on_close = self._on_close
        self.ticker.on_error = self._on_error
        self.ticker.on_reconnect = self._on_reconnect
        self.ticker.on_noreconnect = self._on_noreconnect
        self.ticker.connect(threaded=True)
        return True

    def _restart_live_feed(self, reason="stale"):
        if not self.api_key or not self.access_token:
            return False
        now_ts = time.time()
        if now_ts - self.last_reconnect_attempt_ts < LIVE_FEED_RECONNECT_COOLDOWN_SECONDS:
            return False
        self.last_reconnect_attempt_ts = now_ts
        self.last_error = f"Live feed stalled, reconnecting ({reason})"
        self.connected = False
        self._close_ticker()
        return self._create_ticker()

    def _is_tracked_symbol(self, symbol):
        return not self.nifty500_set or symbol.upper() in self.nifty500_set

    def _coerce_volume(self, value):
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _extract_volume(self, payload, fallback=None):
        fallback_volume = self._coerce_volume(fallback)
        for key in ("volume_traded", "volume"):
            volume = self._coerce_volume((payload or {}).get(key))
            if volume is None:
                continue
            if volume == 0 and fallback_volume not in (None, 0):
                return fallback_volume
            return volume
        return fallback_volume

    def _build_stock_row(self, symbol, last_price, close, volume=None):
        if last_price in (None, 0) or close in (None, 0):
            return None
        change = (last_price - close) / close * 100
        return {
            "symbol": symbol,
            "name": self.symbol_to_name.get(symbol, symbol),
            "price": round(last_price, 2),
            "change": round(change, 2),
            "volume": int(volume) if volume not in (None, "") else None,
            "is_fno": symbol.upper() in self.fno_symbols or self.symbol_to_name.get(symbol, "").upper() in self.fno_symbols,
            "sectors": self.symbol_to_sectors.get(symbol, []),
        }

    def _cached_snapshot(self):
        return load_market_cache(SNAPSHOT_CACHE_KEY)

    def _cached_closed_snapshot(self):
        return load_market_cache(CLOSED_SNAPSHOT_CACHE_KEY)

    def _save_snapshot(self, snapshot):
        try:
            save_market_cache(SNAPSHOT_CACHE_KEY, snapshot)
        except Exception:
            return

    def _save_closed_snapshot(self, snapshot):
        try:
            save_market_cache(CLOSED_SNAPSHOT_CACHE_KEY, snapshot)
        except Exception:
            return

    def _save_latest_rows_cache(self):
        try:
            save_market_cache(
                LATEST_ROWS_CACHE_KEY,
                {
                    "rows": self.latest,
                    "updated_at": self.last_update,
                    "snapshot_source": self.last_snapshot_source,
                },
            )
        except Exception:
            return

    def _cached_latest_rows(self):
        return load_market_cache(LATEST_ROWS_CACHE_KEY)

    def _save_sector_memberships_cache(self):
        try:
            save_market_cache(
                SECTOR_MEMBERS_CACHE_KEY,
                {
                    "sector_members": self.sector_members,
                    "symbol_to_sectors": self.symbol_to_sectors,
                },
            )
        except Exception:
            return

    def _cached_sector_memberships(self):
        return load_market_cache(SECTOR_MEMBERS_CACHE_KEY)

    def _save_sector_breakdowns_cache(self, rows_by_symbol, market_open):
        if not rows_by_symbol or not self.symbol_to_sectors:
            return
        grouped = defaultdict(list)
        for symbol, row in rows_by_symbol.items():
            if not isinstance(row, dict):
                continue
            for sector in row.get("sectors") or self.symbol_to_sectors.get(symbol, []):
                grouped[sector].append(dict(row))

        payload = {}
        for sector, rows in grouped.items():
            ranked = sorted(rows, key=lambda item: item["change"], reverse=True)
            for index, row in enumerate(ranked, start=1):
                row["rank"] = index
            payload[sector] = {
                "sector": sector,
                "stocks": ranked,
                "updated_at": self.last_update,
                "market_open": market_open,
                "snapshot_source": self.last_snapshot_source,
                "constituent_count": len(ranked),
            }

        if not payload:
            return
        try:
            save_market_cache(SECTOR_BREAKDOWNS_CACHE_KEY, payload)
        except Exception:
            return

    def _cached_sector_breakdowns(self):
        return load_market_cache(SECTOR_BREAKDOWNS_CACHE_KEY)

    def _save_previous_day_badges_cache(self):
        try:
            save_market_cache(PREVIOUS_DAY_BADGES_CACHE_KEY, self.previous_day_badges_cache)
        except Exception:
            return

    def _restore_previous_day_badges_cache(self):
        if self.previous_day_badges_cache:
            return
        cached = load_market_cache(PREVIOUS_DAY_BADGES_CACHE_KEY)
        if isinstance(cached, dict):
            self.previous_day_badges_cache = cached

    def _cached_relative_rotation_graph(self):
        return load_market_cache(RRG_CACHE_KEY)

    def _save_relative_rotation_graph(self, payload):
        try:
            save_market_cache(RRG_CACHE_KEY, payload)
        except Exception:
            return

    def _empty_rrg_payload(self, benchmark_symbol, cache_marker, market_open, message=None):
        return {
            "benchmark": benchmark_symbol,
            "cache_marker": cache_marker,
            "market_open": market_open,
            "updated_at": self.last_update or self._utc_now(),
            "latest_session": None,
            "normalization_window": RRG_NORMALIZATION_WINDOW,
            "trail_points": RRG_TRAIL_POINTS,
            "items": [],
            "x_domain": [90, 110],
            "y_domain": [90, 110],
            "cache_pending": True,
            "cache_stale": False,
            "error": message or "Relative rotation cache is warming in the background.",
        }

    def _history_cache_status_payload(self):
        status = dict(self.history_cache_status)
        status["is_running"] = bool(self.history_cache_thread and self.history_cache_thread.is_alive())
        return status

    def get_history_cache_status(self):
        with self.history_cache_lock:
            return self._history_cache_status_payload()

    def _update_history_cache_status(self, **updates):
        with self.history_cache_lock:
            self.history_cache_status.update(updates)
            return self._history_cache_status_payload()

    def _cache_payload_matches_marker(self, payload, cache_marker):
        return bool(payload and payload.get("cache_marker") == cache_marker)

    def _throttled_historical_day_data(self, token, from_date, to_date):
        if not self.kite or not token:
            return []
        with self.historical_fetch_lock:
            elapsed = time.monotonic() - self.last_historical_fetch_ts
            if elapsed < HISTORICAL_DAY_REQUEST_DELAY_SECONDS:
                time.sleep(HISTORICAL_DAY_REQUEST_DELAY_SECONDS - elapsed)
            try:
                candles = self.kite.historical_data(
                    token,
                    self._historical_date_arg(from_date),
                    self._historical_date_arg(to_date),
                    "day",
                )
            except Exception as exc:
                self.last_error = str(exc)
                self.last_historical_fetch_ts = time.monotonic()
                return []
            self.last_historical_fetch_ts = time.monotonic()
            return candles

    def _restore_cached_sector_memberships(self):
        cached = self._cached_sector_memberships()
        if not cached:
            return False
        sector_members = cached.get("sector_members") or {}
        symbol_to_sectors = cached.get("symbol_to_sectors") or {}
        if not sector_members:
            return False
        self.sector_members = {
            str(sector): [str(symbol).upper() for symbol in members or []]
            for sector, members in sector_members.items()
            if members
        }
        self.symbol_to_sectors = {
            str(symbol).upper(): [str(sector) for sector in sectors or []]
            for symbol, sectors in symbol_to_sectors.items()
            if sectors
        }
        return bool(self.sector_members)

    def _rows_for_symbols_from_cache(self, symbols):
        requested = [symbol for symbol in symbols if symbol]
        if not requested:
            return []

        with self.lock:
            if self.latest:
                in_memory_rows = [dict(self.latest[symbol]) for symbol in requested if symbol in self.latest]
                if in_memory_rows:
                    return in_memory_rows

        cached = self._cached_latest_rows()
        rows = (cached or {}).get("rows") or {}
        if not rows:
            return []

        with self.lock:
            for symbol, row in rows.items():
                if isinstance(row, dict):
                    self.latest.setdefault(symbol, row)

        if cached.get("updated_at") and not self.last_update:
            self.last_update = cached["updated_at"]
        if cached.get("snapshot_source") and self.last_snapshot_source == "empty":
            self.last_snapshot_source = cached["snapshot_source"]

        return [dict(rows[symbol]) for symbol in requested if symbol in rows]

    def _stock_row_count(self, snapshot):
        return len(snapshot.get("gainers") or []) + len(snapshot.get("losers") or [])

    def _sector_row_count(self, snapshot):
        return len(snapshot.get("sector_gainers") or []) + len(snapshot.get("sector_losers") or [])

    def _with_runtime_fields(self, snapshot, market_open, source=None):
        runtime = dict(snapshot)
        runtime["connected"] = self.connected
        runtime["error"] = self.last_error
        runtime["market_open"] = market_open
        if source:
            runtime["snapshot_source"] = source
        return runtime

    def _merge_with_cached_snapshot(self, snapshot, cached):
        if not cached:
            return snapshot
        merged = dict(cached)
        merged.update(snapshot)
        if snapshot.get("gainers") or snapshot.get("losers"):
            merged["gainers"] = snapshot.get("gainers", [])
            merged["losers"] = snapshot.get("losers", [])
        if snapshot.get("sector_gainers") or snapshot.get("sector_losers"):
            merged["sectors"] = snapshot.get("sectors", [])
            merged["sector_gainers"] = snapshot.get("sector_gainers", [])
            merged["sector_losers"] = snapshot.get("sector_losers", [])
        merged["connected"] = snapshot.get("connected")
        merged["error"] = snapshot.get("error")
        merged["market_open"] = snapshot.get("market_open")
        merged["snapshot_source"] = snapshot.get("snapshot_source")
        merged["updated_at"] = snapshot.get("updated_at") or cached.get("updated_at")
        return merged

    def _candle_date(self, candle):
        candle_dt = candle.get("date")
        if hasattr(candle_dt, "date"):
            return candle_dt.date()
        return candle_dt

    def _format_candle_date(self, candle):
        candle_date = self._candle_date(candle)
        return candle_date.isoformat() if hasattr(candle_date, "isoformat") else str(candle_date)

    def _series_mean(self, values):
        return sum(values) / len(values) if values else 0.0

    def _series_std(self, values):
        if len(values) < 2:
            return 0.0
        avg = self._series_mean(values)
        variance = sum((value - avg) ** 2 for value in values) / len(values)
        return math.sqrt(variance)

    def _normalize_rrg_series(self, values, window=RRG_NORMALIZATION_WINDOW, scale=10.0):
        normalized = []
        for index, value in enumerate(values):
            subset = values[max(0, index - window + 1):index + 1]
            std = self._series_std(subset)
            if std == 0:
                normalized.append(100.0)
                continue
            avg = self._series_mean(subset)
            normalized.append(100.0 + ((value - avg) / std) * scale)
        return normalized

    def _rrg_quadrant(self, momentum, ratio):
        if ratio >= 100 and momentum >= 100:
            return "Leading"
        if ratio >= 100 and momentum < 100:
            return "Weakening"
        if ratio < 100 and momentum < 100:
            return "Lagging"
        return "Improving"

    def _rrg_color(self, quadrant):
        return {
            "Leading": "#00e5a0",
            "Weakening": "#ffb830",
            "Lagging": "#ff4d6d",
            "Improving": "#00d4ff",
        }.get(quadrant, "#8ca5c8")

    def _warm_previous_day_badge_for_symbol(self, symbol, cache_marker):
        symbol = (symbol or "").upper()
        if not symbol:
            return None
        token = self.symbol_to_token.get(symbol)
        if not token or not self.kite:
            return None
        completed_session = self._latest_completed_session_date()
        session_window = self._trading_session_window(completed_session, 2)
        if len(session_window) < 2:
            return None
        candles = self._fetch_recent_day_candles(
            token,
            self._session_start_dt(session_window[0]),
            self._session_end_dt(session_window[-1]),
            limit=2,
        )
        if len(candles) < 2:
            return None
        current_close = candles[-1].get("close")
        prior_close = candles[-2].get("close")
        if current_close in (None, 0) or prior_close in (None, 0):
            return None
        change = round(((current_close - prior_close) / prior_close) * 100, 2)
        self.previous_day_badges_cache[symbol] = {
            "cache_marker": cache_marker,
            "change": change,
        }
        return change

    def _run_badge_warm_job(self):
        cache_marker = self._completed_session_cache_marker()
        while True:
            with self.badge_warm_lock:
                symbols = sorted(self.pending_badge_symbols)
                self.pending_badge_symbols.clear()
            if not symbols:
                break
            self._restore_previous_day_badges_cache()
            updated = False
            for symbol in symbols:
                cached = self.previous_day_badges_cache.get(symbol)
                if cached and cached.get("cache_marker") == cache_marker:
                    continue
                if self._warm_previous_day_badge_for_symbol(symbol, cache_marker) is not None:
                    updated = True
            if updated:
                self._save_previous_day_badges_cache()
        with self.badge_warm_lock:
            self.badge_warm_thread = None

    def _ensure_previous_day_badges_background(self, symbols):
        filtered = {
            str(symbol).upper()
            for symbol in (symbols or [])
            if symbol and self.symbol_to_token.get(str(symbol).upper())
        }
        if not filtered or not self.kite:
            return False
        with self.badge_warm_lock:
            self.pending_badge_symbols.update(filtered)
            if self.badge_warm_thread and self.badge_warm_thread.is_alive():
                return False
            thread = threading.Thread(target=self._run_badge_warm_job, daemon=True)
            self.badge_warm_thread = thread
            thread.start()
            return True

    def _decorate_rows_with_previous_day_badges(self, rows, fetch_missing=False):
        if not rows:
            return rows
        missing_symbols = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            change = self._get_previous_day_change(row.get("symbol"), allow_fetch=fetch_missing)
            row["previous_day_change"] = round(change, 2) if change is not None else None
            row["previous_day_positive"] = bool(change is not None and change > 0)
            if change is None and row.get("symbol"):
                missing_symbols.append(row.get("symbol"))
        if missing_symbols and not fetch_missing:
            self._ensure_previous_day_badges_background(missing_symbols)
        return rows

    def _decorate_snapshot_rows(self, snapshot):
        if not snapshot:
            return snapshot
        self._decorate_rows_with_previous_day_badges(snapshot.get("gainers") or [], fetch_missing=False)
        self._decorate_rows_with_previous_day_badges(snapshot.get("losers") or [], fetch_missing=False)
        return snapshot

    def _get_previous_day_change(self, symbol, allow_fetch=True):
        symbol = (symbol or "").upper()
        if not symbol:
            return None
        self._restore_previous_day_badges_cache()
        cache_marker = self._completed_session_cache_marker()
        cached = self.previous_day_badges_cache.get(symbol)
        if cached and cached.get("cache_marker") == cache_marker:
            return cached.get("change")

        if not allow_fetch:
            return cached.get("change") if cached else None

        change = self._warm_previous_day_badge_for_symbol(symbol, cache_marker)
        if change is None:
            return cached.get("change") if cached else None
        self._save_previous_day_badges_cache()
        return change

    def _run_refresh_job(self, reason, market_open):
        try:
            if market_open:
                if reason in {"reconnect", "stale", "initial"}:
                    self._restart_live_feed(reason=reason)
                self._refresh_rest_snapshot(force=reason in {"initial", "reconnect", "stale"})
                self._refresh_sector_snapshot(force=True)
            else:
                rest_ok = self._refresh_rest_snapshot(force=True)
                self._refresh_sector_snapshot(force=True)
                if not rest_ok or not self.latest:
                    self._refresh_closed_market_snapshot(force=True)
        except Exception as exc:
            self.last_error = str(exc)
        finally:
            with self.refresh_lock:
                self.refresh_thread = None
                self.refresh_reason = None

    def _ensure_background_refresh(self, market_open, reason="initial"):
        with self.refresh_lock:
            if self.refresh_thread and self.refresh_thread.is_alive():
                return False
            thread = threading.Thread(
                target=self._run_refresh_job,
                args=(reason, market_open),
                daemon=True,
            )
            self.refresh_thread = thread
            self.refresh_reason = reason
            thread.start()
            return True

    def _fetch_sector_constituent_url(self, page_url):
        try:
            response = self.http.get(page_url, timeout=(10, 40))
            response.raise_for_status()
            match = re.search(r"IndexConstituent/[^\"'<>]+\.csv", response.text, re.IGNORECASE)
            if not match:
                return None
            return urljoin("https://www.niftyindices.com/", match.group(0).lstrip("/"))
        except Exception:
            return None

    def _fetch_sector_members(self, sector_name, page_url):
        csv_url = self._fetch_sector_constituent_url(page_url)
        if not csv_url:
            return []
        try:
            response = self.http.get(csv_url, timeout=(10, 60))
            response.raise_for_status()
            text = response.content.decode("utf-8-sig", errors="ignore")
            reader = csv.DictReader(io.StringIO(text))
            members = []
            seen = set()
            for row in reader:
                symbol = (row.get("Symbol") or row.get("SYMBOL") or "").strip().upper()
                if not symbol or symbol in seen or symbol not in self.symbol_to_token:
                    continue
                seen.add(symbol)
                members.append(symbol)
            return members
        except Exception as exc:
            self.last_error = f"Sector constituent load failed for {sector_name}: {exc}"
            return []

    def _refresh_sector_memberships(self, force=False):
        if not self.sector_members:
            self._restore_cached_sector_memberships()

        today = datetime.now(IST).date().isoformat()
        if not force and self.last_membership_refresh_date == today and self.sector_members:
            return

        sector_members = {}
        symbol_to_sectors = defaultdict(set)
        for sector_name, page_url in SECTOR_INDEX_PAGES.items():
            members = self._fetch_sector_members(sector_name, page_url)
            if not members:
                continue
            sector_members[sector_name] = members
            for symbol in members:
                symbol_to_sectors[symbol].add(sector_name)

        if sector_members:
            self.sector_members = sector_members
            self.symbol_to_sectors = {
                symbol: sorted(sectors)
                for symbol, sectors in symbol_to_sectors.items()
            }
            self.last_membership_refresh_date = today
            self._save_sector_memberships_cache()
        elif self.sector_members:
            self.last_membership_refresh_date = today

    def build_universe(self, kite: KiteConnect, sector_names):
        instruments = kite.instruments("NSE")
        nse_eq = [i for i in instruments if i.get("instrument_type") == "EQ"]

        nfo = kite.instruments("NFO")
        fno_set = set()
        for inst in nfo:
            name = inst.get("name")
            if name:
                fno_set.add(name.upper())
            ts = inst.get("tradingsymbol", "")
            if ts:
                fno_set.add(self._extract_underlying(ts).upper())

        token_to_symbol = {}
        symbol_to_token = {}
        symbol_to_name = {}
        equity_tokens = []
        for inst in nse_eq:
            symbol = inst.get("tradingsymbol")
            token = inst.get("instrument_token")
            name = inst.get("name")
            if symbol and token:
                token = int(token)
                token_to_symbol[token] = symbol
                symbol_to_token[symbol] = token
                symbol_to_name[symbol] = name or symbol
                if self._is_tracked_symbol(symbol):
                    equity_tokens.append(token)

        all_index_tokens = {}
        index_tokens = {}
        for inst in instruments:
            if inst.get("segment") != "INDICES":
                continue
            ts = inst.get("tradingsymbol")
            token = inst.get("instrument_token")
            if not ts or not token:
                continue
            all_index_tokens[ts] = int(token)
            if ts in sector_names:
                index_tokens[ts] = int(token)

        self.token_to_symbol = token_to_symbol
        self.symbol_to_token = symbol_to_token
        self.symbol_to_name = symbol_to_name
        self.fno_symbols = fno_set | {s.upper() for s in self.fno_override}
        self.index_tokens = all_index_tokens
        self.sector_tokens = index_tokens
        self.sector_token_to_name = {token: name for name, token in index_tokens.items()}
        self.equity_tokens = equity_tokens
        self._refresh_sector_memberships(force=True)

        prev_close, latest = self._fetch_sector_quote(kite, list(index_tokens.keys()))
        self.sector_prev_close = prev_close
        if latest:
            self.sector_latest.update(latest)

    def _fetch_sector_quote(self, kite: KiteConnect, sector_symbols):
        if not sector_symbols:
            return {}, {}
        try:
            symbols = [f"NSE:{s}" for s in sector_symbols]
            data = kite.quote(symbols)
            prev = {}
            latest = {}
            for sym, payload in data.items():
                ohlc = payload.get("ohlc") or {}
                close = ohlc.get("close")
                last_price = payload.get("last_price")
                name = sym.split(":", 1)[-1]
                if close not in (None, 0):
                    prev[name] = close
                if last_price not in (None, 0):
                    base_close = close if close not in (None, 0) else self.sector_prev_close.get(name)
                    if base_close in (None, 0):
                        change = 0.0
                    else:
                        change = (last_price - base_close) / base_close * 100
                    latest[name] = {
                        "sector": name,
                        "price": round(last_price, 2),
                        "change": round(change, 2),
                    }
            return prev, latest
        except Exception:
            return {}, {}

    def _fetch_prev_close_from_history(self, kite: KiteConnect, symbol):
        token = self.symbol_to_token.get(symbol)
        if not token:
            return None
        completed_session = self._latest_completed_session_date()
        session_window = self._trading_session_window(completed_session, 1)
        if not session_window:
            return None
        candles = self._fetch_recent_day_candles(
            token,
            self._session_start_dt(session_window[0]),
            self._session_end_dt(session_window[-1]),
            limit=1,
        )
        return candles[-1].get("close") if candles else None

    def _fetch_recent_day_candles(self, token, from_date, to_date, limit=None):
        candles = self._throttled_historical_day_data(token, from_date, to_date)
        valid = []
        for candle in candles:
            close = candle.get("close")
            if close in (None, 0):
                continue
            valid.append(candle)
        if limit:
            return valid[-limit:]
        return valid

    def _fetch_last_two_day_candles(self, token, from_date, to_date):
        return self._fetch_recent_day_candles(token, from_date, to_date, limit=2)

    def _build_stock_row_from_candles(self, symbol, candles):
        if len(candles) < 2:
            return None, None
        prev_close = candles[-2].get("close")
        latest_close = candles[-1].get("close")
        latest_volume = candles[-1].get("volume")
        row = self._build_stock_row(symbol, latest_close, prev_close, volume=latest_volume)
        latest_dt = candles[-1].get("date")
        return row, latest_dt

    def _build_sector_row_from_candles(self, sector_name, candles):
        if len(candles) < 2:
            return None, None
        prev_close = candles[-2].get("close")
        latest_close = candles[-1].get("close")
        if latest_close in (None, 0) or prev_close in (None, 0):
            return None, None
        change = (latest_close - prev_close) / prev_close * 100
        latest_dt = candles[-1].get("date")
        return {
            "sector": sector_name,
            "price": round(latest_close, 2),
            "change": round(change, 2),
        }, latest_dt

    def _quote_symbols(self, kite: KiteConnect, symbols):
        quoted = {}
        if not symbols:
            return quoted
        formatted = [f"NSE:{symbol}" for symbol in symbols]
        for chunk in self._chunked(formatted, 200):
            try:
                quoted.update(kite.quote(chunk))
            except Exception as exc:
                self.last_error = str(exc)
        return quoted

    def _refresh_rest_snapshot(self, force=False):
        if not self.kite or not self.symbol_to_token:
            return False

        market_open = self._is_market_open()
        min_interval = 20 if market_open else 300
        now_ts = time.time()
        if not force and now_ts - self.last_rest_refresh_ts < min_interval:
            return False

        tracked_symbols = [
            symbol for symbol in self.symbol_to_token.keys()
            if self._is_tracked_symbol(symbol)
        ]
        quoted = self._quote_symbols(self.kite, tracked_symbols)
        updated = {}
        missing_history = []

        for key, payload in quoted.items():
            symbol = key.split(":", 1)[-1]
            last_price = payload.get("last_price")
            volume = self._extract_volume(payload, fallback=(self.latest.get(symbol) or {}).get("volume"))
            ohlc = payload.get("ohlc") or {}
            close = ohlc.get("close")
            if close not in (None, 0):
                self.rest_prev_close[symbol] = close
            base_close = close if close not in (None, 0) else self.rest_prev_close.get(symbol)
            row = self._build_stock_row(symbol, last_price, base_close, volume=volume)
            if row:
                updated[symbol] = row
            elif last_price not in (None, 0):
                missing_history.append((symbol, last_price, volume))

        for symbol, last_price, volume in missing_history:
            close = self._fetch_prev_close_from_history(self.kite, symbol)
            if close in (None, 0):
                continue
            self.rest_prev_close[symbol] = close
            row = self._build_stock_row(symbol, last_price, close, volume=volume)
            if row:
                updated[symbol] = row

        if updated:
            with self.lock:
                self.latest.update(updated)
                self.last_update = self._utc_now()
            self.last_snapshot_source = "api"
            self._save_latest_rows_cache()
            self._save_sector_breakdowns_cache(updated, market_open)

        self.last_rest_refresh_ts = now_ts
        return bool(updated)

    def _refresh_sector_snapshot(self, force=False):
        if not self.kite or not self.sector_tokens:
            return False
        refresh_interval = SECTOR_SNAPSHOT_REFRESH_SECONDS if self._is_market_open() else 120
        now = time.time()
        if not force and now - self.last_sector_quote_ts < refresh_interval:
            return False
        prev, latest = self._fetch_sector_quote(self.kite, list(self.sector_tokens.keys()))
        with self.lock:
            if prev:
                self.sector_prev_close.update(prev)
            if latest:
                self.sector_latest.update(latest)
                self.last_update = self._utc_now()
                self.last_snapshot_source = "api_sector"
        self.last_sector_quote_ts = now
        return bool(latest)

    def _refresh_closed_market_snapshot(self, force=False):
        if not self.kite or not self.symbol_to_token:
            return False

        now_ts = time.time()
        if not force and now_ts - self.last_closed_refresh_ts < 3600 and self.latest and self.sector_latest:
            return True

        completed_session = self._latest_completed_session_date()
        session_window = self._trading_session_window(completed_session, 2)
        if len(session_window) < 2:
            return False
        from_date = self._session_start_dt(session_window[0])
        to_date = self._session_end_dt(session_window[-1])
        stock_rows = {}
        sector_rows = {}
        latest_dates = []

        tracked_symbols = [
            symbol for symbol in self.symbol_to_token.keys()
            if self._is_tracked_symbol(symbol)
        ]
        for symbol in tracked_symbols:
            candles = self._fetch_last_two_day_candles(self.symbol_to_token.get(symbol), from_date, to_date)
            row, latest_dt = self._build_stock_row_from_candles(symbol, candles)
            if not row:
                continue
            stock_rows[symbol] = row
            if latest_dt:
                latest_dates.append(latest_dt)

        for sector_name, token in self.sector_tokens.items():
            candles = self._fetch_last_two_day_candles(token, from_date, to_date)
            row, latest_dt = self._build_sector_row_from_candles(sector_name, candles)
            if not row:
                continue
            sector_rows[sector_name] = row
            if latest_dt:
                latest_dates.append(latest_dt)

        if stock_rows or sector_rows:
            if latest_dates:
                latest_dt = max(latest_dates)
                if hasattr(latest_dt, "isoformat"):
                    self.last_update = latest_dt.isoformat()
                else:
                    self.last_update = str(latest_dt)
            else:
                self.last_update = self._utc_now()
            with self.lock:
                if stock_rows:
                    self.latest = stock_rows
                if sector_rows:
                    self.sector_latest = sector_rows
            self.last_snapshot_source = "historical_eod"
            self.last_closed_refresh_ts = now_ts
            if stock_rows:
                self._save_latest_rows_cache()
                self._save_sector_breakdowns_cache(stock_rows, False)
            snapshot = self._build_snapshot(False)
            if self._stock_row_count(snapshot):
                self._save_snapshot(snapshot)
                self._save_closed_snapshot(snapshot)
            return True
        return False

    def start(self, api_key, access_token, sector_names):
        with self.start_lock:
            try:
                self.api_key = api_key
                self.access_token = access_token
                self.sector_names = list(sector_names or [])
                self._close_ticker()
                kite = KiteConnect(api_key=api_key)
                kite.set_access_token(access_token)
                kite.set_session_expiry_hook(self._on_session_expiry)
                self.kite = kite
                self.build_universe(kite, sector_names)
                if self._is_market_open():
                    self._refresh_rest_snapshot(force=True)
                else:
                    self._refresh_closed_market_snapshot(force=True)
                self._create_ticker()
                self.last_error = None
            except Exception as exc:
                self.last_error = str(exc)
                self.connected = False

    def _on_connect(self, ws, response, tokens):
        try:
            ws.subscribe(tokens)
            ws.set_mode(ws.MODE_FULL, tokens)
            self.connected = True
            self.last_connect_ts = time.time()
            self.last_error = None
        except Exception as exc:
            self.last_error = str(exc)
            self.connected = False

    def _on_close(self, ws, code, reason):
        self.connected = False
        self.last_error = f"WebSocket closed: {code} {reason}"

    def _on_error(self, ws, code, reason):
        self.connected = False
        self.last_error = f"WebSocket error: {code} {reason}"

    def _on_reconnect(self, ws, attempts_count):
        self.connected = False
        self.last_reconnect_attempt_ts = time.time()
        self.last_error = f"WebSocket reconnecting (attempt {attempts_count})"

    def _on_noreconnect(self, ws):
        self.connected = False
        self.last_error = "WebSocket stopped reconnecting. Refresh the Kite session from the admin panel."

    def _on_session_expiry(self):
        self.connected = False
        self.last_error = "Kite access token expired. Refresh the Kite session from the admin panel."
        self._close_ticker()

    def _on_ticks(self, ws, ticks):
        with self.lock:
            for tick in ticks:
                token = tick.get("instrument_token")
                last_price = tick.get("last_price")
                ohlc = tick.get("ohlc", {})
                close = ohlc.get("close")
                if not token or last_price is None:
                    continue

                if token in self.token_to_symbol:
                    symbol = self.token_to_symbol[token]
                    volume = self._extract_volume(tick, fallback=(self.latest.get(symbol) or {}).get("volume"))
                    base_close = close if close not in (None, 0) else self.rest_prev_close.get(symbol)
                    row = self._build_stock_row(symbol, last_price, base_close, volume=volume)
                    if row:
                        self.latest[symbol] = row
                else:
                    name = self.sector_token_to_name.get(token)
                    if not name:
                        continue
                    base_close = close
                    if base_close in (None, 0):
                        base_close = self.sector_prev_close.get(name)
                    if base_close in (None, 0):
                        change = 0.0
                    else:
                        change = (last_price - base_close) / base_close * 100
                    self.sector_latest[name] = {
                        "sector": name,
                        "price": round(last_price, 2),
                        "change": round(change, 2),
                    }
            self.last_update = self._utc_now()
            self.last_tick_ts = time.time()
            self.connected = True
        self.last_snapshot_source = "websocket"

    def _build_snapshot(self, market_open):
        with self.lock:
            movers = list(self.latest.values())
            if self.nifty500_set:
                movers = [m for m in movers if m["symbol"].upper() in self.nifty500_set]
            gainers = [dict(m) for m in sorted([m for m in movers if m["change"] > 0], key=lambda x: x["change"], reverse=True)[:20]]
            losers = [dict(m) for m in sorted([m for m in movers if m["change"] < 0], key=lambda x: x["change"])[:20]]
            sectors = list(self.sector_latest.values())
            sector_gainers = sorted([s for s in sectors if s["change"] > 0], key=lambda x: x["change"], reverse=True)[:10]
            sector_losers = sorted([s for s in sectors if s["change"] < 0], key=lambda x: x["change"])[:10]
            snapshot = {
                "gainers": gainers,
                "losers": losers,
                "sectors": sectors,
                "sector_gainers": sector_gainers,
                "sector_losers": sector_losers,
                "updated_at": self.last_update,
                "connected": self.connected,
                "error": self.last_error,
                "market_open": market_open,
                "snapshot_source": self.last_snapshot_source,
            }
        self._decorate_snapshot_rows(snapshot)
        return snapshot

    def _cache_is_fresh(self, cached_payload, max_age_seconds):
        if not cached_payload or not cached_payload.get("_cached_at"):
            return False
        try:
            cached_at = datetime.fromisoformat(cached_payload["_cached_at"])
        except Exception:
            return False
        return (datetime.now(timezone.utc).replace(tzinfo=None) - cached_at).total_seconds() <= max_age_seconds

    def _fetch_rrg_price_series(self, token, from_date, to_date):
        candles = self._fetch_recent_day_candles(token, from_date, to_date)
        series = []
        for candle in candles:
            candle_date = self._format_candle_date(candle)
            close = candle.get("close")
            if close in (None, 0):
                continue
            series.append((candle_date, float(close)))
        return series

    def _symbols_for_badge_warmup(self):
        return sorted(
            symbol for symbol, token in self.symbol_to_token.items()
            if token and self._is_tracked_symbol(symbol)
        )

    def _warm_previous_day_badges_cache(self, cache_marker, force=False):
        self._restore_previous_day_badges_cache()
        symbols = self._symbols_for_badge_warmup()
        total = len(symbols)
        if not total:
            return {"processed": 0, "total": 0, "updated": 0}

        processed = 0
        updated = 0
        for symbol in symbols:
            processed += 1
            self._update_history_cache_status(
                processed=processed,
                total=total + len(self.sector_tokens) + 1,
                message=f"Caching previous-day badge history for tracked stocks ({processed}/{total})",
            )
            cached = self.previous_day_badges_cache.get(symbol)
            if not force and cached and cached.get("cache_marker") == cache_marker:
                continue
            if self._warm_previous_day_badge_for_symbol(symbol, cache_marker) is not None:
                updated += 1

        if updated:
            self._save_previous_day_badges_cache()
        return {"processed": processed, "total": total, "updated": updated}

    def _build_rrg_series_map(self, benchmark_symbol, cache_marker):
        if not self.kite:
            return None, {}, "Kite Connect session is not available for historical index candles."
        benchmark_token = self.index_tokens.get(benchmark_symbol)
        if not benchmark_token:
            return None, {}, f"Benchmark token for {benchmark_symbol} is not available."

        session_window = self._trading_session_window(datetime.fromisoformat(cache_marker).date(), RRG_LOOKBACK_SESSIONS)
        if len(session_window) < RRG_LOOKBACK_SESSIONS:
            return None, {}, "Not enough completed trading sessions are available yet."

        from_date = self._session_start_dt(session_window[0])
        to_date = self._session_end_dt(session_window[-1])
        benchmark_series = self._fetch_rrg_price_series(benchmark_token, from_date, to_date)
        if len(benchmark_series) < RRG_LOOKBACK_SESSIONS:
            return None, {}, (
                f"{benchmark_symbol} returned only {len(benchmark_series)} daily candles for "
                f"{session_window[0].isoformat()} to {session_window[-1].isoformat()}."
            )
        component_series = {}
        sector_names = list(self.sector_tokens.keys()) or list(self.sector_names)

        total_rrg_requests = len(sector_names) + 1
        processed = 0
        self._update_history_cache_status(
            processed=0,
            total=(self.history_cache_status.get("total") or 0),
            message=f"Caching RRG history (0/{total_rrg_requests})",
        )

        for sector_name in sector_names:
            processed += 1
            self._update_history_cache_status(
                message=f"Caching RRG history ({processed}/{total_rrg_requests})",
            )
            token = self.sector_tokens.get(sector_name)
            if not token:
                continue
            series = self._fetch_rrg_price_series(token, from_date, to_date)
            if len(series) >= RRG_LOOKBACK_SESSIONS:
                component_series[sector_name] = series

        if not component_series:
            return None, {}, (
                f"No sector index returned at least {RRG_LOOKBACK_SESSIONS} daily candles for "
                f"{session_window[0].isoformat()} to {session_window[-1].isoformat()}."
            )
        return benchmark_series, component_series, None

    def _warm_relative_rotation_graph_cache(self, cache_marker, force=False):
        cached = self._cached_relative_rotation_graph()
        if not force and self._cache_payload_matches_marker(cached, cache_marker) and cached.get("items"):
            return cached

        benchmark_series, component_series, error = self._build_rrg_series_map(RRG_BENCHMARK_SYMBOL, cache_marker)
        if error:
            return {
                "benchmark": RRG_BENCHMARK_SYMBOL,
                "cache_marker": cache_marker,
                "market_open": self._is_market_open(),
                "updated_at": self.last_update or self._utc_now(),
                "latest_session": cache_marker,
                "normalization_window": RRG_NORMALIZATION_WINDOW,
                "trail_points": RRG_TRAIL_POINTS,
                "items": [],
                "error": error,
                "x_domain": [90, 110],
                "y_domain": [90, 110],
            }

        payload = self._build_rrg_payload_from_series(RRG_BENCHMARK_SYMBOL, benchmark_series, component_series)
        payload["cache_marker"] = cache_marker
        payload["market_open"] = self._is_market_open()
        payload["updated_at"] = self.last_update or self._utc_now()
        if not payload.get("items"):
            payload["error"] = (
                f"Historical index data was fetched, but 0 sectors aligned into a full "
                f"{RRG_LOOKBACK_SESSIONS}-session RRG window."
            )
        self._save_relative_rotation_graph(payload)
        return payload

    def _run_daily_market_history_cache_job(self, force=False):
        cache_marker = self._completed_session_cache_marker()
        started_at = self._utc_now()
        total = len(
            [
                symbol for symbol, token in self.symbol_to_token.items()
                if token and self._is_tracked_symbol(symbol)
            ]
        ) + len(self.sector_tokens) + 1
        self._update_history_cache_status(
            status="running",
            session_marker=cache_marker,
            started_at=started_at,
            finished_at=None,
            processed=0,
            total=total,
            message="Preparing historical market cache...",
            error=None,
        )
        try:
            badge_summary = self._warm_previous_day_badges_cache(cache_marker, force=force)
            rrg_payload = self._warm_relative_rotation_graph_cache(cache_marker, force=force)
            status = "completed" if rrg_payload.get("items") else "warning"
            self._update_history_cache_status(
                status=status,
                finished_at=self._utc_now(),
                processed=total,
                total=total,
                message=(
                    f"Cached {badge_summary['updated']} badge rows and "
                    f"{len(rrg_payload.get('items') or [])} RRG sectors for {cache_marker}."
                ),
                error=rrg_payload.get("error"),
            )
        except Exception as exc:
            self.last_error = str(exc)
            self._update_history_cache_status(
                status="failed",
                finished_at=self._utc_now(),
                message="Historical market cache job failed.",
                error=str(exc),
            )
        finally:
            with self.history_cache_lock:
                self.history_cache_thread = None

    def start_daily_market_history_cache(self, force=False):
        cache_marker = self._completed_session_cache_marker()
        cached_rrg = self._cached_relative_rotation_graph()
        if not force and self._cache_payload_matches_marker(cached_rrg, cache_marker) and cached_rrg.get("items"):
            return self._update_history_cache_status(
                status="completed",
                session_marker=cache_marker,
                finished_at=self._utc_now(),
                message=f"Historical market cache for {cache_marker} is already ready.",
                error=None,
            )
        with self.history_cache_lock:
            if self.history_cache_thread and self.history_cache_thread.is_alive():
                return self._history_cache_status_payload()
            self.history_cache_status.update(
                {
                    "status": "running",
                    "session_marker": cache_marker,
                    "started_at": self._utc_now(),
                    "finished_at": None,
                    "processed": 0,
                    "total": 0,
                    "message": "Preparing historical market cache...",
                    "error": None,
                }
            )
            thread = threading.Thread(
                target=self._run_daily_market_history_cache_job,
                args=(force,),
                daemon=True,
            )
            self.history_cache_thread = thread
            thread.start()
            return self._history_cache_status_payload()

    def _build_rrg_payload_from_series(self, benchmark_symbol, benchmark_series, component_series):
        benchmark_map = {date: close for date, close in benchmark_series if close not in (None, 0)}
        items = []
        all_x = [100.0]
        all_y = [100.0]
        latest_session = None

        for sector_name, sector_series in component_series.items():
            sector_map = {date: close for date, close in sector_series if close not in (None, 0)}
            common_dates = sorted(set(benchmark_map).intersection(sector_map))
            if len(common_dates) < RRG_LOOKBACK_SESSIONS:
                continue
            common_dates = common_dates[-RRG_LOOKBACK_SESSIONS:]
            rs_values = [
                (sector_map[date] / benchmark_map[date]) * 100.0
                for date in common_dates
                if benchmark_map[date] not in (None, 0)
            ]
            if len(rs_values) != len(common_dates):
                continue
            rs_ratio = self._normalize_rrg_series(rs_values)
            rs_delta = [0.0]
            rs_delta.extend(rs_ratio[idx] - rs_ratio[idx - 1] for idx in range(1, len(rs_ratio)))
            rs_momentum = self._normalize_rrg_series(rs_delta)
            trail_start = max(0, len(common_dates) - RRG_TRAIL_POINTS)
            trail = []
            for index in range(trail_start, len(common_dates)):
                point = {
                    "date": common_dates[index],
                    "x": round(rs_momentum[index], 2),
                    "y": round(rs_ratio[index], 2),
                }
                trail.append(point)
                all_x.append(point["x"])
                all_y.append(point["y"])
            if not trail:
                continue
            current = trail[-1]
            quadrant = self._rrg_quadrant(current["x"], current["y"])
            latest_session = max(latest_session or common_dates[-1], common_dates[-1])
            items.append(
                {
                    "sector": sector_name,
                    "quadrant": quadrant,
                    "color": self._rrg_color(quadrant),
                    "rs_ratio": current["y"],
                    "rs_momentum": current["x"],
                    "trail": trail,
                    "relative_strength": round(rs_values[-1], 2),
                    "latest_price": round(sector_map[common_dates[-1]], 2),
                }
            )

        items.sort(key=lambda item: (-item["rs_ratio"], -item["rs_momentum"], item["sector"]))
        padding = 3.0
        return {
            "benchmark": benchmark_symbol,
            "market_open": self._is_market_open(),
            "updated_at": self.last_update or self._utc_now(),
            "latest_session": latest_session,
            "normalization_window": RRG_NORMALIZATION_WINDOW,
            "trail_points": RRG_TRAIL_POINTS,
            "items": items,
            "x_domain": [round(min(all_x) - padding, 2), round(max(all_x) + padding, 2)],
            "y_domain": [round(min(all_y) - padding, 2), round(max(all_y) + padding, 2)],
        }

    def get_relative_rotation_graph(self, benchmark_symbol=RRG_BENCHMARK_SYMBOL, cached_only=False):
        market_open = self._is_market_open()
        cache_marker = self._completed_session_cache_marker()
        cached = self._cached_relative_rotation_graph()
        if self._cache_payload_matches_marker(cached, cache_marker) and cached.get("items"):
            cached["market_open"] = market_open
            cached["cache_pending"] = False
            cached["cache_stale"] = False
            return cached

        if cached_only:
            if cached and cached.get("items"):
                cached["market_open"] = market_open
                cached["cache_pending"] = True
                cached["cache_stale"] = True
                cached["error"] = (
                    f"Showing cached rotation data from {cached.get('cache_marker') or 'the prior session'} "
                    "while the latest session cache warms in the background."
                )
                self.start_daily_market_history_cache(force=False)
                return cached
            self.start_daily_market_history_cache(force=False)
            return self._empty_rrg_payload(
                benchmark_symbol,
                cache_marker,
                market_open,
                message="Relative rotation cache is warming in the background. Please wait a moment.",
            )

        if cached and cached.get("items"):
            cached["market_open"] = market_open
            cached["cache_pending"] = True
            cached["cache_stale"] = True
            cached["error"] = (
                f"Showing cached rotation data from {cached.get('cache_marker') or 'the prior session'} "
                "while the latest session cache warms in the background."
            )
            self.start_daily_market_history_cache(force=False)
            return cached

        status = self.start_daily_market_history_cache(force=False)
        return self._empty_rrg_payload(
            benchmark_symbol,
            cache_marker,
            market_open,
            message=status.get("message") or "Relative rotation cache is warming in the background.",
        )

    def get_snapshot(self):
        if self.demo_mode and self.demo_snapshot:
            snap = dict(self.demo_snapshot)
            snap["updated_at"] = self._utc_now()
            snap["connected"] = False
            snap["error"] = None
            return snap

        market_open = self._is_market_open()
        closed_cached = self._cached_closed_snapshot() if not market_open else None
        if not market_open and closed_cached and self._stock_row_count(closed_cached):
            if self.kite and (not self.latest or not self.sector_latest):
                self._ensure_background_refresh(market_open=False, reason="closed_market_bootstrap")
            return self._decorate_snapshot_rows(self._with_runtime_fields(closed_cached, False, "closed_cache"))

        if self.kite:
            if self._is_live_feed_stale():
                self.connected = False
                if not self.last_error or "stalled" not in self.last_error.lower():
                    self.last_error = "Live feed stalled, refreshing from API"
                self._ensure_background_refresh(market_open=True, reason="stale")
            if market_open:
                if not self.latest:
                    self._ensure_background_refresh(market_open=True, reason="initial")
                elif not self.connected:
                    self._ensure_background_refresh(market_open=True, reason="reconnect")
                elif not self.sector_latest:
                    self._ensure_background_refresh(market_open=True, reason="sector_bootstrap")
            else:
                if not self.latest or not self.sector_latest:
                    self._ensure_background_refresh(market_open=False, reason="closed_market_bootstrap")

        snapshot = self._build_snapshot(market_open)
        cached = self._cached_snapshot()
        has_stock_data = any(snapshot.get(key) for key in ("gainers", "losers"))
        has_sector_data = any(snapshot.get(key) for key in ("sector_gainers", "sector_losers"))
        if not market_open and closed_cached:
            if self._stock_row_count(closed_cached) > self._stock_row_count(snapshot):
                return self._decorate_snapshot_rows(self._with_runtime_fields(closed_cached, False, "closed_cache"))
        if has_stock_data:
            self._save_snapshot(snapshot)
            if not market_open:
                self._save_closed_snapshot(snapshot)
            return snapshot
        if has_sector_data and cached and any(cached.get(key) for key in ("gainers", "losers")):
            return self._decorate_snapshot_rows(self._merge_with_cached_snapshot(snapshot, cached))
        if has_sector_data and not cached:
            self._save_snapshot(snapshot)
            return snapshot
        if cached:
            cached["connected"] = self.connected
            cached["error"] = self.last_error
            cached["market_open"] = market_open
            cached["snapshot_source"] = "cache"
            return self._decorate_snapshot_rows(cached)
        return snapshot

    def _get_latest_rows_for_symbols(self, symbols):
        if not self.kite:
            return self._rows_for_symbols_from_cache(symbols)

        if not self._is_market_open():
            cached_rows = self._rows_for_symbols_from_cache(symbols)
            if cached_rows:
                self.last_snapshot_source = "closed_cache_rows"
                return cached_rows
            now = datetime.now(ZoneInfo("Asia/Kolkata"))
            from_date = now - timedelta(days=15)
            rows = []
            for symbol in symbols:
                token = self.symbol_to_token.get(symbol)
                candles = self._fetch_last_two_day_candles(token, from_date, now)
                row, latest_dt = self._build_stock_row_from_candles(symbol, candles)
                if row:
                    rows.append(row)
                    if latest_dt and hasattr(latest_dt, "isoformat"):
                        self.last_update = latest_dt.isoformat()
            if rows:
                with self.lock:
                    for row in rows:
                        self.latest[row["symbol"]] = row
                self.last_snapshot_source = "historical_eod"
                self._save_latest_rows_cache()
            return rows

        requested = [symbol for symbol in symbols if symbol in self.symbol_to_token]
        if not requested:
            return []

        quoted = self._quote_symbols(self.kite, requested)
        rows = []
        missing_history = []
        for key, payload in quoted.items():
            symbol = key.split(":", 1)[-1]
            last_price = payload.get("last_price")
            volume = self._extract_volume(payload, fallback=(self.latest.get(symbol) or {}).get("volume"))
            ohlc = payload.get("ohlc") or {}
            close = ohlc.get("close")
            if close not in (None, 0):
                self.rest_prev_close[symbol] = close
            base_close = close if close not in (None, 0) else self.rest_prev_close.get(symbol)
            row = self._build_stock_row(symbol, last_price, base_close, volume=volume)
            if row:
                rows.append(row)
            elif last_price not in (None, 0):
                missing_history.append((symbol, last_price, volume))

        for symbol, last_price, volume in missing_history:
            close = self._fetch_prev_close_from_history(self.kite, symbol)
            if close in (None, 0):
                continue
            self.rest_prev_close[symbol] = close
            row = self._build_stock_row(symbol, last_price, close, volume=volume)
            if row:
                rows.append(row)

        if rows:
            with self.lock:
                for row in rows:
                    self.latest[row["symbol"]] = row
                self.last_update = self._utc_now()
            self.last_snapshot_source = "api"
            self._save_latest_rows_cache()
        return rows

    def get_sector_breakdown(self, sector_name):
        sector = (sector_name or "").strip()
        if not sector:
            return {"sector": "", "stocks": [], "updated_at": self.last_update, "market_open": self._is_market_open()}

        market_open = self._is_market_open()
        if not market_open:
            cached_breakdowns = self._cached_sector_breakdowns() or {}
            cached_payload = cached_breakdowns.get(sector)
            if cached_payload and cached_payload.get("stocks"):
                self._decorate_rows_with_previous_day_badges(cached_payload["stocks"])
                return cached_payload

        self._refresh_sector_memberships(force=not bool(self.sector_members))
        symbols = self.sector_members.get(sector, [])
        rows = self._get_latest_rows_for_symbols(symbols)
        ranked = sorted(rows, key=lambda row: row["change"], reverse=True)
        self._decorate_rows_with_previous_day_badges(ranked)
        for index, row in enumerate(ranked, start=1):
            row["rank"] = index
        return {
            "sector": sector,
            "stocks": ranked,
            "updated_at": self.last_update,
            "market_open": market_open,
            "snapshot_source": self.last_snapshot_source,
            "constituent_count": len(ranked),
        }

    def _is_market_open(self):
        try:
            now = datetime.now(IST)
            if now.weekday() >= 5:
                return False
            if now.date().isoformat() in NSE_TRADING_HOLIDAYS:
                return False
            start = dtime(9, 15)
            end = dtime(15, 30)
            return start <= now.time() <= end
        except Exception:
            return True

    def token_from_redis(self):
        if not self.redis:
            return None
        try:
            token = self.redis.get(KITE_ACCESS_TOKEN_KEY)
            if token:
                return token.decode("utf-8") if isinstance(token, bytes) else token
        except Exception:
            return None
        return None

    def save_token(self, access_token):
        if not self.redis:
            return
        try:
            self.redis.set(KITE_ACCESS_TOKEN_KEY, access_token)
            self.redis.set(KITE_TOKEN_UPDATED_KEY, self._utc_now())
        except Exception:
            return
