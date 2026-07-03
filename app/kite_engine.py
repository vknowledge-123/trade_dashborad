import csv
import io
import json
import math
import re
import struct
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
PREVIOUS_CLOSE_CACHE_KEY = "previous_close_map"
PREVIOUS_DAY_BADGES_CACHE_KEY = "previous_day_badges"
PREVIOUS_DAY_LEVELS_CACHE_KEY = "previous_day_levels"
PDH_PDL_SCANNER_CACHE_KEY = "pdh_pdl_scanner"
RRG_CACHE_KEY = "relative_rotation_graph"
SWING_SCANNER_CACHE_KEY = "swing_scanner"
ACCELERATION_VOLUME_SMA_CACHE_KEY = "acceleration_volume_sma"
ACCELERATION_HITS_CACHE_KEY = "acceleration_hits"
DHAN_SCRIP_MASTER_CACHE_KEY = "dhan_scrip_master"
IST = ZoneInfo("Asia/Kolkata")
LIVE_FEED_STALE_AFTER_SECONDS = 15
LIVE_FEED_RECONNECT_COOLDOWN_SECONDS = 20
LIVE_FEED_CONNECT_GRACE_SECONDS = 45
SECTOR_SNAPSHOT_REFRESH_SECONDS = 5
RRG_BENCHMARK_SYMBOL = "NIFTY 50"
RRG_LOOKBACK_SESSIONS = 15
RRG_FETCH_SESSIONS = 30
RRG_TRAIL_POINTS = 14
RRG_NORMALIZATION_WINDOW = 14
HISTORICAL_DAY_REQUEST_DELAY_SECONDS = 0.35
SWING_SCANNER_CACHE_VERSION = 2
ACCELERATION_SCANNER_MIN_GAIN_PERCENT = 0.5
ACCELERATION_TIMEFRAMES = {1, 5, 15}
ACCELERATION_VOLUME_SMA_SESSIONS = 5
ACCELERATION_VOLUME_LOOKBACK_SESSIONS = 10
ACCELERATION_HIT_TTL_SECONDS = 120
DHAN_SCRIP_MASTER_MAX_CACHE_DAYS = 7
NSE_INTRADAY_SESSION_MINUTES = 375
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/135.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,text/csv,*/*;q=0.8",
}
SECTOR_INDEX_PAGES = {
    "NIFTY AUTO": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-auto",
    "NIFTY IT": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-it",
    "NIFTY METAL": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-metal",
    "NIFTY INFRA": "https://www.niftyindices.com/indices/equity/thematic-indices/nifty-infrastructure",
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
    "NIFTY BANK": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-bank",
    "NIFTY MS IT TELCM": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-midsmall--it-telecom",
    "NIFTY IND DEFENCE": "https://www.niftyindices.com/indices/equity/thematic-indices/nifty-india-defence",
    "NIFTY MEDIA": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-media",
    "NIFTY IND DIGITAL": "https://www.niftyindices.com/indices/equity/thematic-indices/nifty-india-digital",
    "NIFTY PHARMA": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-pharma",
    "NIFTY IND TOURISM": "https://www.niftyindices.com/indices/equity/thematic-indices/nifty-india-tourism",
    "NIFTY CAPITAL MKT": "https://www.niftyindices.com/indices/equity/thematic-indices/nifty-capital-markets",
    "NIFTY OIL AND GAS": "https://www.niftyindices.com/indices/equity/sectoral-indices/nifty-oil-and-gas-index",
    "NIFTY INDIA MFG": "https://www.niftyindices.com/indices/equity/thematic-indices/nifty-india-manufacturing",
}
FALLBACK_SECTOR_MEMBERS = {
    "NIFTY IT": [
        "COFORGE",
        "HCLTECH",
        "INFY",
        "LTIM",
        "MPHASIS",
        "OFSS",
        "PERSISTENT",
        "TCS",
        "TECHM",
        "WIPRO",
    ],
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

DHAN_SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
DHAN_API_BASE_URL = "https://api.dhan.co/v2"
DHAN_FEED_URL = "wss://api-feed.dhan.co?version=2&token={token}&clientId={client_id}&authType=2"
DHAN_EXCHANGE_SEGMENT_CODES = {
    0: "IDX_I",
    1: "NSE_EQ",
    2: "NSE_FNO",
    3: "NSE_CURRENCY",
    4: "BSE_EQ",
    5: "MCX_COMM",
    7: "BSE_CURRENCY",
    8: "BSE_FNO",
}
DHAN_SECTOR_SECURITY_IDS = {
    "NIFTY 50": "13",
    "NIFTY AUTO": "14",
    "NIFTY IT": "29",
    "NIFTY METAL": "31",
    "NIFTY INFRA": "43",
    "NIFTY FINSEREXBNK": "495",
    "NIFTY FINANCIAL SERVICES EX BANK": "495",
    "NIFTY MS FIN SERV": "819",
    "NIFTY MID SMALL FINANCIAL SERVICES": "819",
    "NIFTY HEALTHCARE": "447",
    "NIFTY MIDSML HLTH": "471",
    "NIFTY MIDSMALL HEALTHCARE": "471",
    "NIFTY PSU BANK": "33",
    "NIFTY CONSR DURBL": "466",
    "NIFTY CONSUMER DURABLE": "466",
    "NIFTY FMCG": "28",
    "NIFTY PVT BANK": "15",
    "NIFTY PRIVATE BANK": "15",
    "NIFTY ENERGY": "42",
    "NIFTY CPSE": "45",
    "NIFTY BANK": "25",
    "NIFTY MS IT TELCM": "821",
    "NIFTY MID SMALL IT TELECOM": "821",
    "NIFTY IND DEFENCE": "493",
    "NIFTY INDIA DEFENCE": "493",
    "NIFTY MEDIA": "30",
    "NIFTY IND DIGITAL": "473",
    "NIFTY INDIA DIGITAL": "473",
    "NIFTY PHARMA": "32",
    "NIFTY IND TOURISM": "815",
    "NIFTY INDIA TOURISM": "815",
    "NIFTY CAPITAL MKT": "803",
    "NIFTY CAPITAL MARKETS": "803",
    "NIFTY OIL AND GAS": "470",
    "NIFTY INDIA MFG": "474",
    "NIFTY INDIA MANUFACTURING": "474",
}
NIFTY_50_SCANNER_STOCKS = [
    "HDFCBANK", "ICICIBANK", "MAXHEALTH", "RELIANCE", "INFY",
    "BHARTIARTL", "ITC", "WIPRO", "M&M", "SBIN",
    "AXISBANK", "TCS", "ETERNAL", "LT", "SHRIRAMFIN",
    "MARUTI", "TRENT", "APOLLOHOSP", "COALINDIA", "NTPC",
    "ASIANPAINT", "BAJFINANCE", "TATASTEEL", "KOTAKBANK", "HINDALCO",
    "POWERGRID", "SUNPHARMA", "ULTRACEMCO", "BEL", "JIOFIN",
    "EICHERMOT", "ADANIENT", "GRASIM", "TITAN", "INDIGO",
    "ADANIPORTS", "ONGC", "TMPV", "HINDUNILVR", "DRREDDY",
    "HDFCLIFE", "HCLTECH", "BAJAJ-AUTO", "SBILIFE", "TATACONSUM",
    "NESTLEIND", "CIPLA", "TECHM", "BAJAJFINSV", "JSWSTEEL",
]


class DhanRateLimitError(RuntimeError):
    pass


class DhanClient:
    def __init__(self, client_id, access_token, http_session=None):
        self.client_id = client_id
        self.access_token = access_token
        self.http = http_session or requests.Session()
        self.http.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "access-token": access_token,
                "client-id": client_id,
            }
        )

    def _post(self, path, payload):
        response = self.http.post(
            f"{DHAN_API_BASE_URL}{path}",
            json=payload,
            timeout=(10, 40),
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            detail = response.text[:500] if response.text else str(exc)
            if response.status_code == 429:
                raise DhanRateLimitError(f"Dhan API {path} rate limited (429): {detail}") from exc
            raise RuntimeError(f"Dhan API {path} failed ({response.status_code}): {detail}") from exc
        data = response.json()
        if isinstance(data, dict) and data.get("status") not in (None, "success"):
            raise RuntimeError(data.get("remarks") or data.get("message") or str(data))
        return data

    def marketfeed_quote(self, securities):
        data = self._post("/marketfeed/quote", securities).get("data") or {}
        return data.get("data") if isinstance(data.get("data"), dict) else data

    def marketfeed_ohlc(self, securities):
        data = self._post("/marketfeed/ohlc", securities).get("data") or {}
        return data.get("data") if isinstance(data.get("data"), dict) else data

    def place_order(self, security_id, transaction_type, quantity, exchange_segment="NSE_EQ", product_type="INTRADAY", order_type="MARKET", price=0.0, correlation_id=""):
        transaction_type = str(transaction_type or "").upper()
        if transaction_type not in {"BUY", "SELL"}:
            raise ValueError("transaction_type must be BUY or SELL")
        quantity = int(quantity or 0)
        if quantity <= 0:
            raise ValueError("quantity must be greater than zero")
        order_type = str(order_type or "MARKET").upper()
        if order_type not in {"MARKET", "LIMIT"}:
            raise ValueError("order_type must be MARKET or LIMIT")
        price = float(price or 0)
        if order_type == "LIMIT" and price <= 0:
            raise ValueError("price must be greater than zero for LIMIT orders")
        payload = {
            "dhanClientId": str(self.client_id),
            "transactionType": transaction_type,
            "exchangeSegment": exchange_segment,
            "productType": product_type,
            "orderType": order_type,
            "validity": "DAY",
            "securityId": str(security_id),
            "quantity": quantity,
            "disclosedQuantity": 0,
            "price": round(price, 2) if order_type == "LIMIT" else 0.0,
            "triggerPrice": 0.0,
            "afterMarketOrder": False,
            "boProfitValue": None,
            "boStopLossValue": None,
        }
        if correlation_id:
            payload["correlationId"] = str(correlation_id or "")[:30]
        return self._post("/orders", payload)

    def place_market_order(self, security_id, transaction_type, quantity, exchange_segment="NSE_EQ", product_type="INTRADAY", correlation_id=""):
        return self.place_order(
            security_id=security_id,
            transaction_type=transaction_type,
            quantity=quantity,
            exchange_segment=exchange_segment,
            product_type=product_type,
            order_type="MARKET",
            price=0.0,
            correlation_id=correlation_id,
        )

    def historical_data(self, security_id, from_date, to_date, interval):
        from_value = from_date.date().isoformat() if hasattr(from_date, "date") else str(from_date)
        to_value = to_date.date().isoformat() if hasattr(to_date, "date") else str(to_date)
        segment = "NSE_EQ"
        instrument = "EQUITY"
        if isinstance(security_id, tuple):
            segment, security_id, instrument = security_id
        payload = {
            "securityId": str(security_id),
            "exchangeSegment": segment,
            "instrument": instrument,
            "expiryCode": 0,
            "oi": False,
            "fromDate": from_value,
            "toDate": to_value,
        }
        data = self._post("/charts/historical", payload)
        return self._candles_from_dhan_response(data)

    def _candles_from_dhan_response(self, data):
        if not isinstance(data, dict):
            return []
        payload = data.get("data", data)
        if isinstance(payload, dict) and isinstance(payload.get("data"), (dict, list)):
            payload = payload["data"]
        if isinstance(payload, list):
            return self._candles_from_dict_rows(payload)
        if isinstance(payload, dict):
            for key in ("candles", "CANDLES", "records", "rows"):
                if isinstance(payload.get(key), list):
                    return self._candles_from_dict_rows(payload[key])
            return self._candles_from_dhan_arrays(payload)
        return []

    def _candles_from_dict_rows(self, rows):
        candles = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            timestamp = (
                row.get("timestamp")
                or row.get("time")
                or row.get("date")
                or row.get("datetime")
            )
            candle_dt = timestamp
            if isinstance(timestamp, (int, float)):
                candle_dt = datetime.fromtimestamp(int(timestamp), tz=IST)
            candles.append(
                {
                    "date": candle_dt,
                    "open": row.get("open") or row.get("Open") or row.get("OPEN"),
                    "high": row.get("high") or row.get("High") or row.get("HIGH"),
                    "low": row.get("low") or row.get("Low") or row.get("LOW"),
                    "close": row.get("close") or row.get("Close") or row.get("CLOSE"),
                    "volume": self._first_present(
                        row,
                        "volume",
                        "volumes",
                        "Volume",
                        "VOLUME",
                        "vol",
                        "VOL",
                        "volume_traded",
                        "total_volume",
                    ),
                }
            )
        return candles

    def _candles_from_dhan_arrays(self, data):
        if not isinstance(data, dict):
            return []
        timestamps = (
            data.get("timestamp")
            or data.get("start_Time")
            or data.get("start_time")
            or data.get("startTime")
            or data.get("time")
            or []
        )
        opens = data.get("open") or []
        highs = data.get("high") or []
        lows = data.get("low") or []
        closes = data.get("close") or []
        volumes = (
            data.get("volume")
            or data.get("volumes")
            or data.get("Volume")
            or data.get("VOLUME")
            or data.get("vol")
            or data.get("VOL")
            or data.get("volume_traded")
            or data.get("total_volume")
            or []
        )
        candles = []
        total = min(len(timestamps), len(opens), len(highs), len(lows), len(closes))
        for idx in range(total):
            candle_dt = datetime.fromtimestamp(int(timestamps[idx]), tz=IST)
            candles.append(
                {
                    "date": candle_dt,
                    "open": opens[idx],
                    "high": highs[idx],
                    "low": lows[idx],
                    "close": closes[idx],
                    "volume": volumes[idx] if idx < len(volumes) else None,
                }
            )
        return candles

    def _first_present(self, payload, *keys):
        for key in keys:
            if isinstance(payload, dict) and key in payload and payload.get(key) not in (None, ""):
                return payload.get(key)
        return None


class MarketEngine:
    def __init__(self, redis_client):
        self.redis = redis_client
        self.api_key = None
        self.access_token = None
        self.client_id = None
        self.broker = "kite"
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
        self.dhan_security_to_symbol = {}
        self.dhan_symbol_to_security = {}
        self.dhan_security_to_segment = {}
        self.dhan_security_to_instrument = {}
        self.sector_members = {}
        self.sector_prev_close = {}
        self.rest_prev_close = {}
        self.previous_close_cache = {}
        self.latest = {}
        self.sector_latest = {}
        self.connected = False
        self.last_error = None
        self.last_update = None
        self.last_tick_ts = 0.0
        self.last_connect_ts = 0.0
        self.last_reconnect_attempt_ts = 0.0
        self.last_ticker_start_ts = 0.0
        self.websocket_generation = 0
        self.demo_mode = False
        self.demo_snapshot = None
        self.last_sector_quote_ts = 0
        self.last_rest_refresh_ts = 0
        self.last_closed_refresh_ts = 0
        self.quote_rate_limited_until = 0.0
        self.historical_rate_limited_until = 0.0
        self.last_membership_refresh_date = None
        self.last_snapshot_source = "empty"
        self.refresh_lock = threading.Lock()
        self.refresh_thread = None
        self.refresh_reason = None
        self.start_lock = threading.Lock()
        self.previous_day_badges_cache = {}
        self.previous_day_levels_cache = {}
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
            "broker": self._current_broker(),
            "message": "No market history cache job has been started yet.",
            "error": None,
        }
        self.badge_warm_lock = threading.Lock()
        self.badge_warm_thread = None
        self.pending_badge_symbols = set()
        self.scanner_lock = threading.Lock()
        self.scanner_thread = None
        self.scanner_status = {
            "status": "idle",
            "started_at": None,
            "finished_at": None,
            "message": "Scanner cache has not been warmed yet.",
            "error": None,
        }
        self.acceleration_lock = threading.Lock()
        self.acceleration_closes = defaultdict(dict)
        self.acceleration_hits = defaultdict(list)
        self.acceleration_volume_sma_cache = {}
        self.swing_scanner_lock = threading.Lock()
        self.swing_scanner_thread = None
        self.swing_scanner_status = {
            "status": "idle",
            "started_at": None,
            "finished_at": None,
            "message": "Swing scanner cache has not been warmed yet.",
            "error": None,
        }
        self.http = requests.Session()
        self.http.headers.update(HTTP_HEADERS)

    def _extract_underlying(self, tradingsymbol):
        match = re.match(r"^[A-Z]+", tradingsymbol)
        return match.group(0) if match else tradingsymbol

    def _normalize_symbol(self, value):
        return (value or "").strip().upper()

    def _dhan_scrip_rows(self, force=False):
        today = datetime.now(IST).date().isoformat()
        text = None
        if not force:
            cached = load_market_cache(DHAN_SCRIP_MASTER_CACHE_KEY)
            if isinstance(cached, dict):
                cache_date = cached.get("cache_date")
                try:
                    cache_age = (datetime.fromisoformat(today).date() - datetime.fromisoformat(cache_date).date()).days
                except (TypeError, ValueError):
                    cache_age = DHAN_SCRIP_MASTER_MAX_CACHE_DAYS + 1
                if 0 <= cache_age <= DHAN_SCRIP_MASTER_MAX_CACHE_DAYS:
                    text = cached.get("csv_text")
        if not text:
            response = self.http.get(DHAN_SCRIP_MASTER_URL, timeout=(10, 60))
            response.raise_for_status()
            text = response.content.decode("utf-8-sig", errors="ignore")
            save_market_cache(
                DHAN_SCRIP_MASTER_CACHE_KEY,
                {
                    "cache_date": today,
                    "broker": "dhan",
                    "cached_at": self._utc_now(),
                    "csv_text": text,
                },
            )
        return csv.DictReader(io.StringIO(text))

    def _row_value(self, row, *keys):
        for key in keys:
            value = row.get(key)
            if value not in (None, ""):
                return str(value).strip()
        return ""

    def _normalize_dhan_segment_instrument(self, exch=None, segment_code=None, instrument=None):
        exch = self._normalize_symbol(exch)
        segment_code = self._normalize_symbol(segment_code)
        instrument = self._normalize_symbol(instrument)
        if segment_code in {"IDX", "INDEX", "IDX_I", "I", "0"} or instrument == "INDEX":
            return "IDX_I", "INDEX"
        if exch in {"NSE", "NSE_EQ"} or segment_code in {"NSE", "NSE_EQ", "E", "1"}:
            if instrument in {"", "EQUITY", "EQ", "STOCK"}:
                return "NSE_EQ", "EQUITY"
        if exch in {"BSE", "BSE_EQ"} or segment_code in {"BSE", "BSE_EQ", "4"}:
            if instrument in {"", "EQUITY", "EQ", "STOCK"}:
                return "BSE_EQ", "EQUITY"
        if exch in {"FNO", "NSE_FNO"} or segment_code in {"FNO", "NSE_FNO", "D", "2"}:
            return "NSE_FNO", instrument or "FUTIDX"
        if exch in {"MCX", "MCX_COMM"} or segment_code in {"MCX", "MCX_COMM", "M", "5"}:
            return "MCX_COMM", instrument or "COMM"
        return None, None

    def _build_dhan_universe(self, sector_names, warm_dashboard=True):
        token_to_symbol = {}
        symbol_to_token = {}
        symbol_to_name = {}
        security_to_segment = {}
        security_to_instrument = {}
        sector_tokens = {}
        all_index_tokens = {}
        tracked = set(self.nifty500_set or []) | set(NIFTY_50_SCANNER_STOCKS)

        for row in self._dhan_scrip_rows():
            exch = self._row_value(row, "SEM_EXM_EXCH_ID", "EXCH_ID")
            segment_code = self._row_value(row, "SEM_SEGMENT", "SEGMENT")
            series = self._row_value(row, "SEM_SERIES", "SERIES")
            instrument = self._row_value(row, "SEM_INSTRUMENT_NAME", "INSTRUMENT")
            security_id = self._row_value(row, "SEM_SMST_SECURITY_ID", "SECURITY_ID", "SECURITY_ID")
            symbol = self._normalize_symbol(
                self._row_value(row, "SEM_TRADING_SYMBOL", "SYMBOL_NAME", "SM_SYMBOL_NAME")
            )
            display_name = self._row_value(row, "SEM_CUSTOM_SYMBOL", "DISPLAY_NAME", "SM_SYMBOL_NAME")
            if not security_id:
                continue

            api_segment, api_instrument = self._normalize_dhan_segment_instrument(exch, segment_code, instrument)
            if api_segment == "NSE_EQ" and api_instrument == "EQUITY" and (not series or series == "EQ"):
                if not symbol:
                    continue
                token = int(float(security_id))
                token_to_symbol[token] = symbol
                symbol_to_token[symbol] = token
                symbol_to_name[symbol] = display_name or symbol
                security_to_segment[token] = api_segment
                security_to_instrument[token] = api_instrument
                continue

            if api_segment == "IDX_I" or instrument == "INDEX":
                name = self._normalize_symbol(display_name or symbol)
                token = int(float(security_id))
                all_index_tokens[name] = token
                security_to_segment[token] = "IDX_I"
                security_to_instrument[token] = "INDEX"
                if name in sector_names:
                    sector_tokens[name] = token

        for name, security_id in DHAN_SECTOR_SECURITY_IDS.items():
            if name == RRG_BENCHMARK_SYMBOL:
                token = int(security_id)
                all_index_tokens[name] = token
                security_to_segment[token] = "IDX_I"
                security_to_instrument[token] = "INDEX"
                continue
            if name in sector_names and name not in sector_tokens:
                token = int(security_id)
                sector_tokens[name] = token
                all_index_tokens[name] = token
                security_to_segment[token] = "IDX_I"
                security_to_instrument[token] = "INDEX"

        self.token_to_symbol = token_to_symbol
        self.symbol_to_token = symbol_to_token
        self.symbol_to_name = symbol_to_name
        self.dhan_symbol_to_security = symbol_to_token
        self.dhan_security_to_symbol = token_to_symbol
        self.dhan_security_to_segment = security_to_segment
        self.dhan_security_to_instrument = security_to_instrument
        self.index_tokens = all_index_tokens
        self.sector_tokens = sector_tokens
        self.sector_token_to_name = {token: name for name, token in sector_tokens.items()}
        self.fno_symbols = {s.upper() for s in self.fno_override}
        self.equity_tokens = [
            token for symbol, token in symbol_to_token.items()
            if not tracked or symbol in tracked
        ]
        if warm_dashboard:
            self._refresh_sector_memberships(force=False)
            self._restore_previous_close_cache()

            prev, latest = self._fetch_sector_quote(self.kite, list(sector_tokens.keys()))
            if prev:
                self.sector_prev_close.update(prev)
            if latest:
                self.sector_latest.update(latest)

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

    def _current_broker(self):
        return self.broker if self.broker in {"kite", "dhan"} else "kite"

    def _broker_label(self):
        return "Dhan" if self._current_broker() == "dhan" else "Zerodha"

    def _payload_matches_broker(self, payload):
        if not isinstance(payload, dict):
            return False
        # Older cache rows did not store a broker; treat them as Zerodha/Kite rows.
        broker = (payload.get("broker") or "kite").lower()
        return broker == self._current_broker()

    def _historical_date_arg(self, value):
        if hasattr(value, "date"):
            value = value.date()
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return value

    def _tracked_feed_activity_ts(self):
        return max(self.last_tick_ts, self.last_connect_ts)

    def _is_quote_rate_limited(self):
        return time.time() < self.quote_rate_limited_until

    def _mark_quote_rate_limited(self, exc):
        self.quote_rate_limited_until = time.time() + 300
        self.last_error = str(exc)

    def _is_historical_rate_limited(self):
        return time.time() < self.historical_rate_limited_until

    def _mark_historical_rate_limited(self, exc):
        self.historical_rate_limited_until = time.time() + 900
        self.last_error = str(exc)

    def _is_live_feed_stale(self):
        if not self.kite or not self._is_market_open():
            return False
        activity_ts = self._tracked_feed_activity_ts()
        if not activity_ts:
            if not self.ticker:
                return False
            return (time.time() - self.last_ticker_start_ts) > LIVE_FEED_CONNECT_GRACE_SECONDS
        return (time.time() - activity_ts) > LIVE_FEED_STALE_AFTER_SECONDS

    def _close_ticker(self):
        self.websocket_generation += 1
        if not self.ticker:
            return
        try:
            if hasattr(self.ticker, "close"):
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
        self.last_ticker_start_ts = time.time()
        if self.broker == "dhan":
            return self._create_dhan_ticker()
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

    def _sector_rankings(self, include_fallback=True):
        with self.lock:
            sector_rows = [dict(row) for row in self.sector_latest.values() if isinstance(row, dict)]
            latest_rows = {symbol: dict(row) for symbol, row in self.latest.items()}
        if not sector_rows and include_fallback:
            sector_rows = self._sector_rows_from_stock_rows(latest_rows)
        ranked = sorted(
            [row for row in sector_rows if row.get("change") is not None],
            key=lambda item: float(item.get("change") or 0),
            reverse=True,
        )
        return {
            str(row.get("sector") or "").upper(): {
                "sector": row.get("sector"),
                "sector_change": round(float(row.get("change") or 0), 2),
                "sector_rank": idx,
                "sector_count": len(ranked),
            }
            for idx, row in enumerate(ranked, start=1)
            if row.get("sector")
        }

    def _sector_context_for_symbol(self, symbol, latest=None, rankings=None):
        symbol = str(symbol or "").upper()
        latest = latest or {}
        sectors = latest.get("sectors") or self.symbol_to_sectors.get(symbol, [])
        if not sectors:
            return {
                "sector": None,
                "sector_name": None,
                "sector_change": None,
                "sector_rank": None,
                "sector_count": None,
            }
        rankings = rankings if rankings is not None else self._sector_rankings()
        contexts = [rankings.get(str(sector).upper()) for sector in sectors]
        contexts = [ctx for ctx in contexts if ctx]
        if contexts:
            context = sorted(contexts, key=lambda item: item.get("sector_rank") or 9999)[0]
            return {
                "sector": context.get("sector"),
                "sector_name": context.get("sector"),
                "sector_change": context.get("sector_change"),
                "sector_rank": context.get("sector_rank"),
                "sector_count": context.get("sector_count"),
            }
        sector_name = sectors[0]
        return {
            "sector": sector_name,
            "sector_name": sector_name,
            "sector_change": None,
            "sector_rank": None,
            "sector_count": None,
        }

    def _sector_rows_from_stock_rows(self, rows_by_symbol=None):
        rows_by_symbol = rows_by_symbol or self.latest
        if not rows_by_symbol:
            return []
        if not self.symbol_to_sectors:
            self._restore_cached_sector_memberships()
        grouped = defaultdict(list)
        for symbol, row in rows_by_symbol.items():
            if not isinstance(row, dict):
                continue
            change = row.get("change")
            if change is None:
                continue
            sectors = row.get("sectors") or self.symbol_to_sectors.get(str(symbol).upper(), [])
            for sector in sectors:
                grouped[sector].append(float(change))
        sector_rows = []
        for sector, changes in grouped.items():
            if not changes:
                continue
            sector_rows.append(
                {
                    "sector": sector,
                    "price": "-",
                    "change": round(sum(changes) / len(changes), 2),
                    "rank_source": "constituent_average",
                    "constituent_count": len(changes),
                }
            )
        return sector_rows

    def _create_dhan_ticker(self):
        try:
            import websocket
        except Exception as exc:
            self.connected = False
            self.last_error = f"Dhan websocket-client is not installed: {exc}"
            return False

        instruments = []
        for token in self.equity_tokens:
            instruments.append({"ExchangeSegment": "NSE_EQ", "SecurityId": str(token)})
        for token in self.sector_tokens.values():
            instruments.append({"ExchangeSegment": "IDX_I", "SecurityId": str(token)})
        if not instruments:
            self.connected = False
            self.last_error = "No Dhan security IDs available for websocket subscription."
            return False

        url = DHAN_FEED_URL.format(token=self.access_token, client_id=self.client_id or self.api_key)
        generation = self.websocket_generation

        def on_open(ws):
            if generation != self.websocket_generation:
                return
            self.connected = True
            self.last_connect_ts = time.time()
            self.last_error = None
            for chunk in self._chunked(instruments, 100):
                ws.send(
                    json.dumps(
                        {
                            "RequestCode": 17,
                            "InstrumentCount": len(chunk),
                            "InstrumentList": chunk,
                        }
                    )
                )

        def on_message(ws, message):
            if generation != self.websocket_generation:
                return
            if isinstance(message, str):
                return
            self._on_dhan_binary_message(message)

        def on_error(ws, error):
            if generation != self.websocket_generation:
                return
            self.connected = False
            self.last_error = f"Dhan WebSocket error: {error}"

        def on_close(ws, code, reason):
            if generation != self.websocket_generation:
                return
            self.connected = False
            self.last_error = f"Dhan WebSocket closed: {code} {reason}"

        self.ticker = websocket.WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        def run():
            backoff = 2
            while generation == self.websocket_generation and self.broker == "dhan" and self.access_token:
                try:
                    self.last_ticker_start_ts = time.time()
                    self.ticker.run_forever(ping_interval=20, ping_timeout=10)
                except Exception as exc:
                    if generation != self.websocket_generation:
                        break
                    self.connected = False
                    self.last_error = f"Dhan WebSocket stopped: {exc}"
                if generation != self.websocket_generation or self.broker != "dhan":
                    break
                self.connected = False
                self.last_reconnect_attempt_ts = time.time()
                if self._is_market_open():
                    self.last_error = "Dhan WebSocket reconnecting"
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        self.thread = thread
        return True

    def _on_dhan_binary_message(self, message):
        if not message or len(message) < 8:
            return
        try:
            packet_code, _length, exchange_code, security_id = struct.unpack_from("<B H B I", message, 0)
            segment = DHAN_EXCHANGE_SEGMENT_CODES.get(exchange_code)
            if not segment:
                return
            token = int(security_id)
            with self.lock:
                if packet_code == 6 and len(message) >= 16:
                    prev_close = struct.unpack_from("<f", message, 8)[0]
                    if segment == "IDX_I":
                        name = self.sector_token_to_name.get(token)
                        if name and prev_close not in (None, 0):
                            self.sector_prev_close[name] = prev_close
                    elif prev_close not in (None, 0):
                        symbol = self.token_to_symbol.get(token)
                        if symbol:
                            self.rest_prev_close[symbol] = prev_close
                    return

                if packet_code not in {2, 4, 8} or len(message) < 12:
                    return
                last_price = struct.unpack_from("<f", message, 8)[0]
                volume = None
                day_close = None
                if packet_code == 4 and len(message) >= 50:
                    volume = struct.unpack_from("<I", message, 22)[0]
                    day_close = struct.unpack_from("<f", message, 38)[0]
                elif packet_code == 8 and len(message) >= 58:
                    volume = struct.unpack_from("<I", message, 22)[0]
                    day_close = struct.unpack_from("<f", message, 50)[0]

                if segment == "IDX_I":
                    name = self.sector_token_to_name.get(token)
                    if not name:
                        return
                    base_close = day_close if day_close not in (None, 0) else self.sector_prev_close.get(name)
                    change = 0.0 if base_close in (None, 0) else (last_price - base_close) / base_close * 100
                    self.sector_latest[name] = {
                        "sector": name,
                        "price": round(last_price, 2),
                        "change": round(change, 2),
                    }
                else:
                    symbol = self.token_to_symbol.get(token)
                    if not symbol:
                        return
                    base_close = day_close if day_close not in (None, 0) else self.rest_prev_close.get(symbol)
                    row = self._build_stock_row(
                        symbol,
                        last_price,
                        base_close,
                        volume=volume or (self.latest.get(symbol) or {}).get("volume"),
                    )
                    if row:
                        self.latest[symbol] = row
                        self._record_acceleration_price(symbol, last_price, cumulative_volume=volume)
                self.last_update = self._utc_now()
                self.last_tick_ts = time.time()
                self.connected = True
                self.last_snapshot_source = "dhan_websocket"
        except Exception as exc:
            self.last_error = f"Dhan tick parse failed: {exc}"

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
            try:
                return int(float(value))
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

    def _candle_volume(self, candle):
        if not isinstance(candle, dict):
            return None
        for key in ("volume", "volumes", "Volume", "VOLUME", "volume_traded", "total_volume"):
            volume = self._coerce_volume(candle.get(key))
            if volume not in (None, 0):
                return volume
        return None

    def _float_or_none(self, value):
        if value in (None, ""):
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if math.isfinite(number) else None

    def _ohlc_badges(self, day_open=None, day_high=None, day_low=None):
        day_open = self._float_or_none(day_open)
        day_high = self._float_or_none(day_high)
        day_low = self._float_or_none(day_low)
        badges = []
        tolerance = 0.005
        open_equals_low = (
            day_open is not None
            and day_low is not None
            and abs(day_open - day_low) <= tolerance
        )
        open_equals_high = (
            day_open is not None
            and day_high is not None
            and abs(day_open - day_high) <= tolerance
        )
        if open_equals_low:
            badges.append("OPEN=LOW")
        if open_equals_high:
            badges.append("OPEN=HIGH")
        return badges, open_equals_low, open_equals_high

    def _build_stock_row(self, symbol, last_price, close, volume=None, day_open=None, day_high=None, day_low=None):
        if last_price in (None, 0) or close in (None, 0):
            return None
        change = (last_price - close) / close * 100
        day_open_value = self._float_or_none(day_open)
        day_high_value = self._float_or_none(day_high)
        day_low_value = self._float_or_none(day_low)
        ohlc_badges, open_equals_low, open_equals_high = self._ohlc_badges(day_open, day_high, day_low)
        return {
            "symbol": symbol,
            "name": self.symbol_to_name.get(symbol, symbol),
            "price": round(last_price, 2),
            "change": round(change, 2),
            "volume": int(volume) if volume not in (None, "") else None,
            "is_fno": symbol.upper() in self.fno_symbols or self.symbol_to_name.get(symbol, "").upper() in self.fno_symbols,
            "sectors": self.symbol_to_sectors.get(symbol, []),
            "day_open": round(day_open_value, 2) if day_open_value is not None else None,
            "day_high": round(day_high_value, 2) if day_high_value is not None else None,
            "day_low": round(day_low_value, 2) if day_low_value is not None else None,
            "open_equals_low": open_equals_low,
            "open_equals_high": open_equals_high,
            "ohlc_badges": ohlc_badges,
        }

    def _bucket_start(self, moment, timeframe):
        timeframe = int(timeframe)
        minute = (moment.minute // timeframe) * timeframe
        return moment.replace(minute=minute, second=0, microsecond=0)

    def _record_acceleration_price(self, symbol, price, moment=None, cumulative_volume=None):
        symbol = (symbol or "").upper()
        price = self._float_or_none(price)
        if not symbol or price in (None, 0):
            return
        cumulative_volume = self._coerce_volume(cumulative_volume)
        moment = moment or datetime.now(IST)
        with self.acceleration_lock:
            symbol_buckets = self.acceleration_closes[symbol]
            for timeframe in ACCELERATION_TIMEFRAMES:
                bucket = self._bucket_start(moment, timeframe).isoformat()
                key = (timeframe, bucket)
                current = symbol_buckets.get(key) or {}
                first_volume = current.get("first_volume")
                if cumulative_volume is not None and first_volume is None:
                    first_volume = cumulative_volume
                current.update(
                    {
                        "close": round(price, 2),
                        "updated_at": moment.isoformat(),
                    }
                )
                if cumulative_volume is not None:
                    current["first_volume"] = first_volume
                    current["last_volume"] = cumulative_volume
                    if first_volume is not None:
                        current["candle_volume"] = max(0, cumulative_volume - first_volume)
                symbol_buckets[key] = current
            if len(symbol_buckets) > 90:
                keys = sorted(symbol_buckets.keys(), key=lambda item: item[1])
                for key in keys[:-75]:
                    symbol_buckets.pop(key, None)

    def _acceleration_volume_sma(self, symbol):
        symbol = (symbol or "").upper()
        if not symbol:
            return None
        if not self.acceleration_volume_sma_cache:
            self._restore_acceleration_volume_sma_cache()
        marker = self._completed_session_cache_marker()
        payload = self.acceleration_volume_sma_cache.get(symbol)
        if (
            not isinstance(payload, dict)
            or payload.get("cache_marker") != marker
            or not self._payload_matches_broker(payload)
        ):
            return None
        volume_sma = payload.get("volume_sma")
        return volume_sma if volume_sma not in (None, 0) else None

    def _acceleration_volume_sma_count(self):
        if not self.acceleration_volume_sma_cache:
            self._restore_acceleration_volume_sma_cache()
        marker = self._completed_session_cache_marker()
        count = 0
        for payload in self.acceleration_volume_sma_cache.values():
            if (
                isinstance(payload, dict)
                and payload.get("cache_marker") == marker
                and self._payload_matches_broker(payload)
                and payload.get("volume_sma") not in (None, 0)
            ):
                count += 1
        return count

    def _acceleration_hit_day_key(self, moment=None):
        moment = moment or datetime.now(IST)
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=IST)
        return moment.astimezone(IST).date().isoformat()

    def _acceleration_event_id(self, symbol, timeframe, current_bucket, direction):
        return f"{symbol}:{int(timeframe)}:{current_bucket}:{direction}"

    def _save_acceleration_hits_cache(self):
        try:
            day_key = self._acceleration_hit_day_key()
            save_market_cache(
                ACCELERATION_HITS_CACHE_KEY,
                {
                    "cache_marker": day_key,
                    "broker": self._current_broker(),
                    "hits": list(self.acceleration_hits.get(day_key, [])),
                },
            )
        except Exception:
            return

    def _restore_acceleration_hits_cache(self):
        if self.acceleration_hits:
            return
        cached = load_market_cache(ACCELERATION_HITS_CACHE_KEY)
        if not isinstance(cached, dict):
            return
        day_key = self._acceleration_hit_day_key()
        if cached.get("cache_marker") != day_key or not self._payload_matches_broker(cached):
            return
        hits = cached.get("hits")
        if isinstance(hits, list):
            self.acceleration_hits[day_key] = [dict(item) for item in hits if isinstance(item, dict)]

    def _remember_acceleration_hit(self, row, min_gain, now=None):
        now = now or datetime.now(IST)
        day_key = self._acceleration_hit_day_key(now)
        self._restore_acceleration_hits_cache()
        event_id = self._acceleration_event_id(
            row.get("symbol"),
            row.get("timeframe"),
            row.get("current_bucket"),
            row.get("direction"),
        )
        stored = dict(row)
        stored["event_id"] = event_id
        stored["appearance_time"] = now.isoformat()
        stored["appearance_time_display"] = now.strftime("%I:%M:%S %p")
        stored["expires_at"] = (now + timedelta(seconds=ACCELERATION_HIT_TTL_SECONDS)).isoformat()
        stored["ttl_seconds"] = ACCELERATION_HIT_TTL_SECONDS
        stored["kept"] = False
        stored["deleted"] = False
        stored["scan_date"] = day_key
        stored["threshold_percent"] = round(float(min_gain or 0), 3)
        stored["repeat_count"] = 1
        day_hits = self.acceleration_hits[day_key]
        if not any(item.get("event_id") == event_id for item in day_hits):
            day_hits.append(stored)
            self._save_acceleration_hits_cache()
        for old_key in list(self.acceleration_hits.keys()):
            if old_key != day_key:
                self.acceleration_hits.pop(old_key, None)
        if len(day_hits) > 1200:
            day_hits.sort(key=lambda item: item.get("appearance_time") or "")
            del day_hits[:-1000]
            self._save_acceleration_hits_cache()

    def _acceleration_hits_for_day(self, timeframe=None, min_gain=None, now=None):
        day_key = self._acceleration_hit_day_key(now)
        self._restore_acceleration_hits_cache()
        now = now or datetime.now(IST)
        try:
            timeframe = int(timeframe) if timeframe is not None else None
        except (TypeError, ValueError):
            timeframe = None
        try:
            min_gain = float(min_gain) if min_gain is not None else None
        except (TypeError, ValueError):
            min_gain = None
        rows = []
        for row in self.acceleration_hits.get(day_key, []):
            if row.get("deleted"):
                continue
            if not row.get("kept"):
                expires_at = row.get("expires_at")
                try:
                    expires_dt = datetime.fromisoformat(expires_at) if expires_at else None
                except (TypeError, ValueError):
                    expires_dt = None
                if expires_dt and expires_dt < now:
                    continue
            if timeframe and int(row.get("timeframe") or 0) != timeframe:
                continue
            if min_gain is not None and abs(float(row.get("move_percent") or 0)) < min_gain:
                continue
            rows.append(dict(row))
        rows.sort(
            key=lambda item: (
                abs(float(item.get("move_percent") or 0)),
                item.get("appearance_time") or "",
            ),
            reverse=True,
        )
        symbol_counts = defaultdict(int)
        for row in rows:
            symbol_counts[row.get("symbol")] += 1
        for row in rows:
            row["repeat_count"] = symbol_counts.get(row.get("symbol"), 1)
        return rows

    def update_acceleration_hit(self, event_id, action):
        event_id = str(event_id or "")
        action = str(action or "").lower()
        if action not in {"keep", "delete"}:
            return {"ok": False, "error": "Action must be keep or delete."}
        self._restore_acceleration_hits_cache()
        day_key = self._acceleration_hit_day_key()
        with self.acceleration_lock:
            for row in self.acceleration_hits.get(day_key, []):
                if row.get("event_id") != event_id:
                    continue
                if action == "keep":
                    row["kept"] = True
                    row["expires_at"] = None
                    row["deleted"] = False
                    message = f"{row.get('symbol', 'Stock')} kept for the day."
                else:
                    row["deleted"] = True
                    message = f"{row.get('symbol', 'Stock')} removed from scanner."
                self._save_acceleration_hits_cache()
                return {"ok": True, "event_id": event_id, "action": action, "message": message}
        return {"ok": False, "error": "Acceleration row was not found or already expired."}

    def get_acceleration_scanner(self, timeframe=1, min_gain=ACCELERATION_SCANNER_MIN_GAIN_PERCENT):
        try:
            timeframe = int(timeframe)
        except (TypeError, ValueError):
            timeframe = 1
        if timeframe not in ACCELERATION_TIMEFRAMES:
            timeframe = 1
        try:
            min_gain = float(min_gain)
        except (TypeError, ValueError):
            min_gain = ACCELERATION_SCANNER_MIN_GAIN_PERCENT
        now = datetime.now(IST)
        current_bucket = self._bucket_start(now, timeframe).isoformat()
        previous_bucket = (self._bucket_start(now, timeframe) - timedelta(minutes=timeframe)).isoformat()
        with self.lock:
            latest_rows = {symbol: dict(row) for symbol, row in self.latest.items()}
        sector_rankings = self._sector_rankings()
        rows = []
        live_rows = []
        current_bucket_count = 0
        previous_bucket_count = 0
        with self.acceleration_lock:
            for symbol, buckets in self.acceleration_closes.items():
                current = buckets.get((timeframe, current_bucket))
                previous = buckets.get((timeframe, previous_bucket))
                if current:
                    current_bucket_count += 1
                if previous:
                    previous_bucket_count += 1
                if not current or not previous:
                    continue
                previous_close = previous.get("close")
                current_close = current.get("close")
                if previous_close in (None, 0) or current_close in (None, 0):
                    continue
                change_percent = ((current_close - previous_close) / previous_close) * 100
                if abs(change_percent) < min_gain:
                    continue
                latest = latest_rows.get(symbol) or {}
                candle_volume = current.get("candle_volume")
                if candle_volume is None:
                    first_volume = current.get("first_volume")
                    last_volume = current.get("last_volume")
                    if first_volume is not None and last_volume is not None:
                        candle_volume = max(0, last_volume - first_volume)
                volume_sma = self._acceleration_volume_sma(symbol)
                volume_sma_multiplier = None
                if candle_volume is not None and volume_sma not in (None, 0):
                    volume_sma_multiplier = candle_volume / volume_sma
                turnover = None
                if candle_volume is not None and current_close not in (None, 0):
                    turnover = candle_volume * current_close
                sector_context = self._sector_context_for_symbol(symbol, latest, sector_rankings)
                rows.append(
                    {
                        "symbol": symbol,
                        "name": self.symbol_to_name.get(symbol, symbol),
                        "price": latest.get("price", current_close),
                        "change": latest.get("change"),
                        "timeframe": timeframe,
                        "from_close": round(previous_close, 2),
                        "to_close": round(current_close, 2),
                        "direction": "up" if change_percent >= 0 else "down",
                        "move_percent": round(change_percent, 3),
                        "gain_percent": round(change_percent, 3),
                        "volume": latest.get("volume"),
                        "candle_volume": int(candle_volume) if candle_volume is not None else None,
                        "volume_sma": round(volume_sma, 2) if volume_sma is not None else None,
                        "volume_sma_multiplier": round(volume_sma_multiplier, 2) if volume_sma_multiplier is not None else None,
                        "turnover": round(turnover, 2) if turnover is not None else None,
                        "is_fno": latest.get("is_fno", symbol in self.fno_symbols),
                        "updated_at": current.get("updated_at"),
                        "bucket_start": current_bucket,
                        "current_bucket": current_bucket,
                        "previous_bucket": previous_bucket,
                        **sector_context,
                    }
                )
            for row in rows:
                self._remember_acceleration_hit(row, min_gain, now=now)
            live_rows = list(rows)
            rows = self._acceleration_hits_for_day(timeframe=timeframe, min_gain=min_gain, now=now)
        market_open = self._is_market_open()
        volume_sma_count = self._acceleration_volume_sma_count()
        if rows:
            error = None
        elif not market_open:
            error = (
                "Market is closed. Cache Data prepares previous close and volume SMA baselines, "
                "but acceleration rows need live market buckets after 9:15."
            )
        elif not current_bucket_count or not previous_bucket_count:
            error = (
                f"Waiting for live {timeframe}-minute buckets. The scanner needs both previous and current "
                "bucket prices before rows can appear."
            )
        else:
            error = "No stocks have moved beyond the selected acceleration threshold yet."
        return {
            "rows": rows,
            "live_rows": live_rows,
            "timeframe": timeframe,
            "min_gain": min_gain,
            "persisted_count": len(rows),
            "tracked_count": len(latest_rows),
            "volume_sma_ready": bool(volume_sma_count),
            "volume_sma_count": volume_sma_count,
            "current_bucket_count": current_bucket_count,
            "previous_bucket_count": previous_bucket_count,
            "updated_at": self.last_update or self._utc_now(),
            "market_open": market_open,
            "current_bucket": current_bucket,
            "previous_bucket": previous_bucket,
            "error": error,
        }

    def place_acceleration_market_order(
        self,
        symbol,
        side,
        per_trade_capital=10000,
        client_price=None,
        buy_limit_offset_pct=1,
        sell_limit_offset_pct=1,
    ):
        symbol = (symbol or "").strip().upper()
        side = (side or "").strip().upper()
        if side not in {"BUY", "SELL"}:
            return {"ok": False, "error": "Order side must be BUY or SELL."}
        active_broker = self._current_broker()
        if active_broker not in {"dhan", "kite"} or not self.kite:
            return {"ok": False, "error": "Order placement requires an authenticated Dhan or Kite broker."}
        token = self.symbol_to_token.get(symbol)
        if not token:
            return {"ok": False, "error": f"{symbol} is not available in broker universe yet."}
        if active_broker == "dhan":
            segment = self.dhan_security_to_segment.get(int(token), "NSE_EQ")
            instrument = self.dhan_security_to_instrument.get(int(token), "EQUITY")
            if segment != "NSE_EQ" or instrument != "EQUITY":
                return {"ok": False, "error": f"{symbol} is not an NSE equity instrument."}
        try:
            capital = float(per_trade_capital or 0)
        except (TypeError, ValueError):
            capital = 0
        if capital <= 0:
            return {"ok": False, "error": "Per trade capital must be greater than zero."}
        with self.lock:
            latest = dict(self.latest.get(symbol) or {})
        price = self._float_or_none(latest.get("price"))
        if price in (None, 0):
            price = self._float_or_none(client_price)
        if price in (None, 0):
            return {"ok": False, "error": f"Live price is not available for {symbol}."}
        quantity = int(capital // price)
        if quantity <= 0:
            return {"ok": False, "error": f"Capital {capital:.2f} is lower than {symbol} price {price:.2f}."}
        try:
            buy_offset = max(0.0, float(buy_limit_offset_pct if buy_limit_offset_pct is not None else 1))
        except (TypeError, ValueError):
            buy_offset = 1.0
        try:
            sell_offset = max(0.0, float(sell_limit_offset_pct if sell_limit_offset_pct is not None else 1))
        except (TypeError, ValueError):
            sell_offset = 1.0
        limit_offset_pct = buy_offset if side == "BUY" else sell_offset
        limit_multiplier = 1 + (limit_offset_pct / 100) if side == "BUY" else 1 - (limit_offset_pct / 100)
        limit_price = round(price * limit_multiplier, 2)
        if limit_price <= 0:
            return {"ok": False, "error": "Limit price must be greater than zero."}
        correlation_id = f"ACC{side[:1]}{symbol}{int(time.time())}"[:30]
        try:
            if active_broker == "dhan":
                response = self.kite.place_order(
                    security_id=token,
                    transaction_type=side,
                    quantity=quantity,
                    exchange_segment="NSE_EQ",
                    product_type="INTRADAY",
                    order_type="LIMIT",
                    price=limit_price,
                    correlation_id=correlation_id,
                )
                broker_product = "INTRADAY"
                broker_order_type = "LIMIT"
            else:
                kite_side = "BUY" if side == "BUY" else "SELL"
                response = self.kite.place_order(
                    variety="regular",
                    exchange="NSE",
                    tradingsymbol=symbol,
                    transaction_type=kite_side,
                    quantity=quantity,
                    product="MIS",
                    order_type="LIMIT",
                    price=limit_price,
                )
                broker_product = "MIS"
                broker_order_type = "LIMIT"
        except Exception as exc:
            self.last_error = str(exc)
            return {"ok": False, "error": str(exc)}
        order_id = response if isinstance(response, str) else None
        if isinstance(response, dict):
            order_id = response.get("orderId") or response.get("order_id") or response.get("order_id")
            data = response.get("data")
            if isinstance(data, dict):
                order_id = order_id or data.get("orderId") or data.get("order_id")
        return {
            "ok": True,
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": round(price, 2),
            "limit_price": limit_price,
            "limit_offset_pct": round(limit_offset_pct, 3),
            "capital": round(capital, 2),
            "product_type": broker_product,
            "order_type": broker_order_type,
            "broker": active_broker,
            "order_id": order_id,
            "response": response,
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
            ranked = self._rank_sector_breakdown_rows(rows, market_open)
            payload[sector] = {
                "sector": sector,
                "stocks": ranked,
                "updated_at": self.last_update,
                "session_marker": self._completed_session_cache_marker() if not market_open else None,
                "market_open": market_open,
                "snapshot_source": self.last_snapshot_source,
                "constituent_count": len(ranked),
            }

        if not payload:
            return
        try:
            existing = self._cached_sector_breakdowns() or {}
            existing.update(payload)
            save_market_cache(SECTOR_BREAKDOWNS_CACHE_KEY, existing)
        except Exception:
            return

    def _cached_sector_breakdowns(self):
        return load_market_cache(SECTOR_BREAKDOWNS_CACHE_KEY)

    def _save_acceleration_volume_sma_cache(self):
        try:
            save_market_cache(ACCELERATION_VOLUME_SMA_CACHE_KEY, self.acceleration_volume_sma_cache)
        except Exception:
            return

    def _restore_acceleration_volume_sma_cache(self):
        cached = load_market_cache(ACCELERATION_VOLUME_SMA_CACHE_KEY)
        if isinstance(cached, dict):
            self.acceleration_volume_sma_cache = cached
        return self.acceleration_volume_sma_cache

    def _save_previous_close_cache(self):
        try:
            save_market_cache(PREVIOUS_CLOSE_CACHE_KEY, self.previous_close_cache)
        except Exception:
            return

    def _restore_previous_close_cache(self):
        cached = load_market_cache(PREVIOUS_CLOSE_CACHE_KEY)
        if not isinstance(cached, dict):
            return {}
        self.previous_close_cache = cached
        marker = self._completed_session_cache_marker()
        symbols = cached.get("symbols") or {}
        sectors = cached.get("sectors") or {}
        for symbol, payload in symbols.items():
            if (
                not isinstance(payload, dict)
                or payload.get("cache_marker") != marker
                or not self._payload_matches_broker(payload)
            ):
                continue
            close = payload.get("close")
            if close not in (None, 0):
                self.rest_prev_close[str(symbol).upper()] = close
        for sector, payload in sectors.items():
            if (
                not isinstance(payload, dict)
                or payload.get("cache_marker") != marker
                or not self._payload_matches_broker(payload)
            ):
                continue
            close = payload.get("close")
            if close not in (None, 0):
                self.sector_prev_close[str(sector).upper()] = close
        return cached

    def _remember_previous_close(self, bucket, key, close, cache_marker=None):
        if close in (None, 0) or not key:
            return False
        marker = cache_marker or self._completed_session_cache_marker()
        normalized_key = str(key).upper()
        cache_bucket = "sectors" if bucket == "sectors" else "symbols"
        self.previous_close_cache.setdefault(cache_bucket, {})
        self.previous_close_cache[cache_bucket][normalized_key] = {
            "cache_marker": marker,
            "broker": self._current_broker(),
            "close": round(float(close), 2),
            "updated_at": self._utc_now(),
        }
        if cache_bucket == "sectors":
            self.sector_prev_close[normalized_key] = close
        else:
            self.rest_prev_close[normalized_key] = close
        return True

    def _cached_previous_close(self, bucket, key):
        if not key:
            return None
        if not self.previous_close_cache:
            self._restore_previous_close_cache()
        marker = self._completed_session_cache_marker()
        cache_bucket = "sectors" if bucket == "sectors" else "symbols"
        payload = (self.previous_close_cache.get(cache_bucket) or {}).get(str(key).upper())
        if (
            not isinstance(payload, dict)
            or payload.get("cache_marker") != marker
            or not self._payload_matches_broker(payload)
        ):
            return None
        close = payload.get("close")
        return close if close not in (None, 0) else None

    def _save_previous_day_badges_cache(self):
        try:
            save_market_cache(PREVIOUS_DAY_BADGES_CACHE_KEY, self.previous_day_badges_cache)
        except Exception:
            return

    def _save_previous_day_levels_cache(self):
        try:
            save_market_cache(PREVIOUS_DAY_LEVELS_CACHE_KEY, self.previous_day_levels_cache)
        except Exception:
            return

    def _restore_previous_day_levels_cache(self):
        if self.previous_day_levels_cache:
            return
        cached = load_market_cache(PREVIOUS_DAY_LEVELS_CACHE_KEY)
        if isinstance(cached, dict):
            self.previous_day_levels_cache = cached

    def _save_scanner_cache(self, payload):
        try:
            save_market_cache(PDH_PDL_SCANNER_CACHE_KEY, payload)
        except Exception:
            return

    def _cached_scanner_payload(self):
        return load_market_cache(PDH_PDL_SCANNER_CACHE_KEY)

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
            "broker": self._current_broker(),
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
        return bool(
            payload
            and payload.get("cache_marker") == cache_marker
            and self._payload_matches_broker(payload)
        )

    def _throttled_historical_day_data(self, token, from_date, to_date):
        if not self.kite or not token:
            return []
        if self.broker == "dhan" and self._is_historical_rate_limited():
            return []
        with self.historical_fetch_lock:
            elapsed = time.monotonic() - self.last_historical_fetch_ts
            if elapsed < HISTORICAL_DAY_REQUEST_DELAY_SECONDS:
                time.sleep(HISTORICAL_DAY_REQUEST_DELAY_SECONDS - elapsed)
            try:
                request_token = token
                if self.broker == "dhan":
                    request_token = (
                        self.dhan_security_to_segment.get(int(token), "NSE_EQ"),
                        str(token),
                        self.dhan_security_to_instrument.get(int(token), "EQUITY"),
                    )
                candles = self.kite.historical_data(
                    request_token,
                    self._historical_date_arg(from_date),
                    self._historical_date_arg(to_date),
                    "day",
                )
            except DhanRateLimitError as exc:
                self._mark_historical_rate_limited(exc)
                self.last_historical_fetch_ts = time.monotonic()
                return []
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
            in_memory_by_symbol = {}
            if self.latest:
                in_memory_by_symbol = {
                    symbol: dict(self.latest[symbol])
                    for symbol in requested
                    if symbol in self.latest
                }
                if len(in_memory_by_symbol) == len(requested):
                    return [in_memory_by_symbol[symbol] for symbol in requested]

        cached = self._cached_latest_rows()
        rows = (cached or {}).get("rows") or {}
        if not rows:
            previous_close_rows = {
                row["symbol"]: row
                for row in self._rows_for_symbols_from_previous_close_cache(requested)
                if isinstance(row, dict) and row.get("symbol")
            }
            return [
                in_memory_by_symbol.get(symbol) or previous_close_rows.get(symbol)
                for symbol in requested
                if in_memory_by_symbol.get(symbol) or previous_close_rows.get(symbol)
            ]

        with self.lock:
            for symbol, row in rows.items():
                if isinstance(row, dict):
                    self.latest.setdefault(symbol, row)

        if cached.get("updated_at") and not self.last_update:
            self.last_update = cached["updated_at"]
        if cached.get("snapshot_source") and self.last_snapshot_source == "empty":
            self.last_snapshot_source = cached["snapshot_source"]

        previous_close_rows = {
            row["symbol"]: row
            for row in self._rows_for_symbols_from_previous_close_cache(requested)
            if isinstance(row, dict) and row.get("symbol")
        }
        merged = []
        for symbol in requested:
            if symbol in in_memory_by_symbol:
                merged.append(in_memory_by_symbol[symbol])
            elif symbol in rows:
                merged.append(dict(rows[symbol]))
            elif symbol in previous_close_rows:
                merged.append(previous_close_rows[symbol])
        return merged

    def _rows_for_symbols_from_previous_close_cache(self, symbols):
        requested = [symbol for symbol in symbols if symbol]
        if not requested:
            return []
        self._restore_previous_day_badges_cache()
        rows = []
        for symbol in requested:
            symbol = str(symbol).upper()
            close = self._cached_previous_close("symbols", symbol)
            change = self._get_previous_day_change(symbol, allow_fetch=False)
            if close in (None, 0) or change is None:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "name": self.symbol_to_name.get(symbol, symbol),
                    "price": round(float(close), 2),
                    "change": round(float(change), 2),
                    "volume": None,
                    "is_fno": symbol in self.fno_symbols or self.symbol_to_name.get(symbol, "").upper() in self.fno_symbols,
                    "sectors": self.symbol_to_sectors.get(symbol, []),
                    "previous_day_change": round(float(change), 2),
                    "previous_day_positive": float(change) > 0,
                }
            )
        if rows and not self.last_update:
            self.last_update = self._utc_now()
        return rows

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

    def _ensure_snapshot_sector_rows(self, snapshot):
        if not snapshot or snapshot.get("sector_gainers") or snapshot.get("sector_losers"):
            return snapshot
        rows = {}
        for row in (snapshot.get("gainers") or []) + (snapshot.get("losers") or []):
            if isinstance(row, dict) and row.get("symbol"):
                rows[row["symbol"]] = row
        if not rows:
            return snapshot
        sectors = self._sector_rows_from_stock_rows(rows)
        if not sectors:
            return snapshot
        enriched = dict(snapshot)
        enriched["sectors"] = sectors
        enriched["sector_gainers"] = sorted(
            [row for row in sectors if row.get("change", 0) > 0],
            key=lambda item: item.get("change", 0),
            reverse=True,
        )[:10]
        enriched["sector_losers"] = sorted(
            [row for row in sectors if row.get("change", 0) < 0],
            key=lambda item: item.get("change", 0),
        )[:10]
        return enriched

    def _snapshot_cache_marker(self, snapshot):
        if not snapshot:
            return None
        marker = snapshot.get("session_marker") or snapshot.get("cache_marker")
        if marker:
            return marker
        updated_at = snapshot.get("updated_at")
        if not updated_at:
            return None
        try:
            parsed = datetime.fromisoformat(str(updated_at).replace("Z", "+00:00"))
            return parsed.astimezone(IST).date().isoformat() if parsed.tzinfo else parsed.date().isoformat()
        except Exception:
            match = re.search(r"\d{4}-\d{2}-\d{2}", str(updated_at))
            return match.group(0) if match else None

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
        self._remember_previous_close("symbols", symbol, current_close, cache_marker=cache_marker)
        self.previous_day_badges_cache[symbol] = {
            "cache_marker": cache_marker,
            "broker": self._current_broker(),
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
                if (
                    cached
                    and cached.get("cache_marker") == cache_marker
                    and self._payload_matches_broker(cached)
                ):
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

    def _rank_sector_breakdown_rows(self, rows, market_open):
        prepared = [dict(row) for row in rows or [] if isinstance(row, dict)]
        if not market_open:
            self._decorate_rows_with_previous_day_badges(prepared, fetch_missing=False)
            for row in prepared:
                previous_change = row.get("previous_day_change")
                try:
                    if previous_change is not None:
                        row["change"] = round(float(previous_change), 2)
                except (TypeError, ValueError):
                    continue
        ranked = sorted(prepared, key=lambda item: item.get("change") or 0, reverse=True)
        for index, row in enumerate(ranked, start=1):
            row["rank"] = index
        return ranked

    def _get_previous_day_change(self, symbol, allow_fetch=True):
        symbol = (symbol or "").upper()
        if not symbol:
            return None
        self._restore_previous_day_badges_cache()
        cache_marker = self._completed_session_cache_marker()
        cached = self.previous_day_badges_cache.get(symbol)
        if cached and cached.get("cache_marker") == cache_marker and self._payload_matches_broker(cached):
            return cached.get("change")

        if not allow_fetch:
            return cached.get("change") if cached and self._payload_matches_broker(cached) else None

        change = self._warm_previous_day_badge_for_symbol(symbol, cache_marker)
        if change is None:
            return cached.get("change") if cached and self._payload_matches_broker(cached) else None
        self._save_previous_day_badges_cache()
        return change

    def _get_previous_day_levels(self, symbol):
        symbol = (symbol or "").upper()
        if not symbol:
            return None
        self._restore_previous_day_levels_cache()
        cache_marker = self._completed_session_cache_marker()
        cached = self.previous_day_levels_cache.get(symbol)
        if cached and cached.get("cache_marker") == cache_marker and self._payload_matches_broker(cached):
            return cached
        cached = cached if cached and self._payload_matches_broker(cached) else None
        token = self.symbol_to_token.get(symbol)
        if not token or not self.kite:
            return cached
        completed_session = self._latest_completed_session_date()
        lookup_start = self._trading_session_window(completed_session, 6)
        from_session = lookup_start[0] if lookup_start else completed_session
        candles = self._fetch_recent_day_candles(
            token,
            self._session_start_dt(from_session),
            self._session_end_dt(completed_session),
            limit=None,
        )
        if not candles:
            return cached
        candle = None
        for item in reversed(candles):
            candle_date = self._candle_date(item)
            if hasattr(candle_date, "isoformat") and candle_date <= completed_session:
                candle = item
                break
            if isinstance(candle_date, str) and candle_date <= completed_session.isoformat():
                candle = item
                break
        if not candle:
            candle = candles[-1]
        high = candle.get("high")
        low = candle.get("low")
        close = candle.get("close")
        if high in (None, 0) or low in (None, 0):
            return cached
        levels = {
            "cache_marker": cache_marker,
            "broker": self._current_broker(),
            "high": round(float(high), 2),
            "low": round(float(low), 2),
            "close": round(float(close), 2) if close not in (None, 0) else None,
            "date": self._format_candle_date(candle),
        }
        self.previous_day_levels_cache[symbol] = levels
        self._save_previous_day_levels_cache()
        return levels

    def _scanner_current_rows(self, symbols):
        requested = [symbol for symbol in symbols if symbol]
        rows = []
        with self.lock:
            for symbol in requested:
                row = self.latest.get(symbol)
                if row:
                    rows.append(dict(row))
        if rows:
            return rows
        return self._rows_for_symbols_from_cache(requested)

    def _scanner_status_payload(self):
        status = dict(self.scanner_status)
        status["is_running"] = bool(self.scanner_thread and self.scanner_thread.is_alive())
        return status

    def _run_scanner_refresh_job(self):
        with self.scanner_lock:
            self.scanner_status.update(
                {
                    "status": "running",
                    "started_at": self._utc_now(),
                    "finished_at": None,
                    "message": "Refreshing PDH/PDL scanner data...",
                    "error": None,
                }
            )
        try:
            symbols = [symbol for symbol in NIFTY_50_SCANNER_STOCKS if symbol in self.symbol_to_token]
            if self.kite and symbols:
                if self._is_market_open():
                    self._get_latest_rows_for_symbols(symbols)
                else:
                    rows = self._get_latest_rows_for_symbols(symbols)
                    if not rows:
                        completed_session = self._latest_completed_session_date()
                        from_date = self._session_start_dt(completed_session - timedelta(days=15))
                        to_date = self._session_end_dt(completed_session)
                        for symbol in symbols:
                            candles = self._fetch_last_two_day_candles(self.symbol_to_token.get(symbol), from_date, to_date)
                            row, latest_dt = self._build_stock_row_from_candles(symbol, candles)
                            if row:
                                with self.lock:
                                    self.latest[symbol] = row
                                if latest_dt and hasattr(latest_dt, "isoformat"):
                                    self.last_update = latest_dt.isoformat()

                for symbol in symbols:
                    self._get_previous_day_levels(symbol)

            payload = self._build_pdh_pdl_scanner_payload()
            self._save_scanner_cache(payload)
            with self.scanner_lock:
                self.scanner_status.update(
                    {
                        "status": "completed",
                        "finished_at": self._utc_now(),
                        "message": f"Scanner refreshed for {payload.get('tracked_count', 0)} Nifty 50 stocks.",
                        "error": payload.get("error"),
                    }
                )
        except Exception as exc:
            self.last_error = str(exc)
            with self.scanner_lock:
                self.scanner_status.update(
                    {
                        "status": "failed",
                        "finished_at": self._utc_now(),
                        "message": "PDH/PDL scanner refresh failed.",
                        "error": str(exc),
                    }
                )
        finally:
            with self.scanner_lock:
                self.scanner_thread = None

    def _ensure_scanner_background_refresh(self):
        with self.scanner_lock:
            if self.scanner_thread and self.scanner_thread.is_alive():
                return False
            thread = threading.Thread(target=self._run_scanner_refresh_job, daemon=True)
            self.scanner_thread = thread
            thread.start()
            return True

    def _build_pdh_pdl_scanner_payload(self):
        symbols = [symbol for symbol in NIFTY_50_SCANNER_STOCKS if symbol in self.symbol_to_token]
        market_open = self._is_market_open()
        if not symbols:
            return {
                "pdh_breaks": [],
                "pdl_breaks": [],
                "rows": [],
                "symbols": NIFTY_50_SCANNER_STOCKS,
                "tracked_count": 0,
                "updated_at": self.last_update,
                "market_open": market_open,
                "snapshot_source": self.last_snapshot_source,
                "error": self.last_error or "Broker universe is not ready yet.",
            }

        rows = self._scanner_current_rows(symbols)
        row_map = {row.get("symbol"): row for row in rows if isinstance(row, dict)}
        pdh_breaks = []
        pdl_breaks = []
        scanner_rows = []
        missing_levels = 0

        for symbol in symbols:
            row = row_map.get(symbol)
            if not row:
                continue
            self._restore_previous_day_levels_cache()
            cache_marker = self._completed_session_cache_marker()
            levels = self.previous_day_levels_cache.get(symbol)
            if levels and levels.get("cache_marker") != cache_marker:
                levels = None
            if not levels:
                missing_levels += 1
                continue
            price = row.get("price")
            if price in (None, 0):
                continue
            base = {
                "symbol": symbol,
                "name": row.get("name") or self.symbol_to_name.get(symbol, symbol),
                "price": price,
                "change": row.get("change"),
                "volume": row.get("volume"),
                "previous_high": levels.get("high"),
                "previous_low": levels.get("low"),
                "previous_close": levels.get("close"),
                "previous_date": levels.get("date"),
                "is_fno": row.get("is_fno", False),
            }
            high = levels.get("high")
            low = levels.get("low")
            if high not in (None, 0):
                base["pdh_distance_points"] = round(price - high, 2)
                base["pdh_distance_percent"] = round(((price - high) / high) * 100, 3)
                base["pdh_side"] = "above" if price >= high else "below"
            else:
                base["pdh_distance_points"] = None
                base["pdh_distance_percent"] = None
                base["pdh_side"] = None
            if low not in (None, 0):
                base["pdl_distance_points"] = round(price - low, 2)
                base["pdl_distance_percent"] = round(((price - low) / low) * 100, 3)
                base["pdl_side"] = "above" if price >= low else "below"
            else:
                base["pdl_distance_points"] = None
                base["pdl_distance_percent"] = None
                base["pdl_side"] = None
            scanner_rows.append(dict(base))
            if levels.get("high") not in (None, 0) and price > levels["high"]:
                item = dict(base)
                item["break_points"] = round(price - levels["high"], 2)
                item["break_percent"] = round(((price - levels["high"]) / levels["high"]) * 100, 2)
                pdh_breaks.append(item)
            if levels.get("low") not in (None, 0) and price < levels["low"]:
                item = dict(base)
                item["break_points"] = round(levels["low"] - price, 2)
                item["break_percent"] = round(((levels["low"] - price) / levels["low"]) * 100, 2)
                pdl_breaks.append(item)

        pdh_breaks.sort(key=lambda item: item["break_percent"], reverse=True)
        pdl_breaks.sort(key=lambda item: item["break_percent"], reverse=True)
        return {
            "pdh_breaks": pdh_breaks,
            "pdl_breaks": pdl_breaks,
            "rows": scanner_rows,
            "symbols": symbols,
            "tracked_count": len(symbols),
            "missing_levels": missing_levels,
            "updated_at": self.last_update or self._utc_now(),
            "market_open": market_open,
            "snapshot_source": self.last_snapshot_source,
            "warning": "Using cached rows while Dhan quote cooldown is active." if scanner_rows and self._is_quote_rate_limited() else None,
            "error": None if scanner_rows else self.last_error,
        }

    def _apply_scanner_filters(self, payload, level="all", side="all", min_pct=None, max_pct=None):
        filtered = dict(payload or {})
        rows = list(filtered.get("rows") or [])
        level = (level or "all").lower()
        side = (side or "all").lower()

        def passes(row):
            checks = []
            if level in {"all", "pdh"}:
                checks.append(("pdh", row.get("pdh_distance_percent"), row.get("pdh_side")))
            if level in {"all", "pdl"}:
                checks.append(("pdl", row.get("pdl_distance_percent"), row.get("pdl_side")))
            for level_name, distance, row_side in checks:
                if distance is None:
                    continue
                abs_distance = abs(float(distance))
                if side != "all" and row_side != side:
                    continue
                if min_pct is not None and abs_distance < min_pct:
                    continue
                if max_pct is not None and abs_distance > max_pct:
                    continue
                return True
            return False

        filtered["filtered_rows"] = [row for row in rows if passes(row)]
        filtered["filter"] = {
            "level": level,
            "side": side,
            "min_pct": min_pct,
            "max_pct": max_pct,
        }
        return filtered

    def get_pdh_pdl_scanner(self, level="all", side="all", min_pct=None, max_pct=None, cached_only=True):
        payload = self._build_pdh_pdl_scanner_payload()
        has_live_rows = bool(payload.get("rows"))
        missing_levels = int(payload.get("missing_levels") or 0)
        if (not has_live_rows or missing_levels) and self.kite:
            self._ensure_scanner_background_refresh()
        if cached_only and (not has_live_rows or missing_levels):
            cached = self._cached_scanner_payload()
            if cached and cached.get("rows"):
                payload = dict(cached)
                payload["cache_pending"] = True
                payload["cache_stale"] = True
                payload["market_open"] = self._is_market_open()
                payload["error"] = payload.get("error") or "Refreshing scanner data in the background."
            else:
                payload["cache_pending"] = True
                payload["error"] = payload.get("error") or "Scanner data is warming in the background."
        payload["status"] = self._scanner_status_payload()
        return self._apply_scanner_filters(payload, level=level, side=side, min_pct=min_pct, max_pct=max_pct)

    def _save_swing_scanner_cache(self, payload):
        if payload:
            payload = dict(payload)
            payload["cache_version"] = SWING_SCANNER_CACHE_VERSION
            save_market_cache(SWING_SCANNER_CACHE_KEY, payload)

    def _is_valid_swing_scanner_cache(self, payload):
        return bool(payload) and payload.get("cache_version") == SWING_SCANNER_CACHE_VERSION

    def _cached_swing_scanner_payload(self):
        payload = load_market_cache(SWING_SCANNER_CACHE_KEY)
        if not self._is_valid_swing_scanner_cache(payload):
            return None
        return payload

    def _swing_status_payload(self):
        status = dict(self.swing_scanner_status)
        status["is_running"] = bool(self.swing_scanner_thread and self.swing_scanner_thread.is_alive())
        return status

    def _clean_daily_candles(self, candles):
        cleaned = []
        for candle in candles or []:
            try:
                close = float(candle.get("close"))
                high = float(candle.get("high"))
                low = float(candle.get("low"))
                open_price = float(candle.get("open"))
            except (TypeError, ValueError):
                continue
            if close <= 0 or high <= 0 or low <= 0:
                continue
            cleaned.append(
                {
                    "date": self._format_candle_date(candle),
                    "open": open_price,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": float(candle.get("volume") or 0),
                }
            )
        cleaned.sort(key=lambda item: item.get("date") or "")
        return cleaned

    def _fetch_swing_candles(self, symbol, sessions=180):
        symbol = (symbol or "").strip().upper()
        token = self.symbol_to_token.get(symbol)
        if not self.kite:
            self.last_error = "Broker is not authenticated yet, so daily swing history cannot be fetched."
            return []
        if not token:
            self.last_error = f"{symbol} is not available in the current broker universe yet."
            return []
        completed_session = self._latest_completed_session_date()
        session_window = self._trading_session_window(completed_session, max(30, int(sessions)))
        if not session_window:
            return []
        candles = self._fetch_recent_day_candles(
            token,
            self._session_start_dt(session_window[0]),
            self._session_end_dt(session_window[-1]),
            limit=None,
        )
        return self._clean_daily_candles(candles)

    def _sma(self, values, period):
        if len(values) < period:
            return None
        return sum(values[-period:]) / period

    def _atr_from_candles(self, candles, period=14):
        if len(candles) < 2:
            return 0.0
        ranges = []
        for previous, current in zip(candles[-period - 1:], candles[-period:]):
            high = current["high"]
            low = current["low"]
            prev_close = previous["close"]
            ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        return sum(ranges) / len(ranges) if ranges else 0.0

    def _rsi_from_closes(self, closes, period=14):
        if len(closes) <= period:
            return None
        gains = []
        losses = []
        for previous, current in zip(closes[-period - 1:-1], closes[-period:]):
            change = current - previous
            gains.append(max(change, 0.0))
            losses.append(abs(min(change, 0.0)))
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def _swing_staircase_pattern(self, candles, lookback=8):
        recent = candles[-lookback:] if len(candles) >= lookback else candles[:]
        if len(recent) < 5:
            return {
                "is_valid": False,
                "score": 0.0,
                "label": "Staircase forming",
                "higher_low_count": 0,
                "higher_close_count": 0,
            }
        higher_lows = sum(1 for previous, current in zip(recent[:-1], recent[1:]) if current["low"] >= previous["low"])
        higher_closes = sum(1 for previous, current in zip(recent[:-1], recent[1:]) if current["close"] >= previous["close"])
        tight_pullbacks = 0
        for previous, current in zip(recent[:-1], recent[1:]):
            previous_range = max(previous["high"] - previous["low"], 0.01)
            pullback_depth = (previous["close"] - current["low"]) / previous_range
            if pullback_depth <= 0.75:
                tight_pullbacks += 1
        total_steps = max(len(recent) - 1, 1)
        structure_ratio = (higher_lows + higher_closes + tight_pullbacks) / (total_steps * 3)
        is_valid = higher_lows >= max(3, total_steps - 3) and higher_closes >= max(3, total_steps - 4)
        label = "Staircase confirmed" if is_valid else "Staircase forming"
        return {
            "is_valid": is_valid,
            "score": round(structure_ratio * 100, 1),
            "label": label,
            "higher_low_count": higher_lows,
            "higher_close_count": higher_closes,
        }

    def _swing_price_volume_growth(self, candles):
        if len(candles) < 6:
            return {
                "is_valid": False,
                "price_growth_pct": 0.0,
                "volume_growth_pct": 0.0,
                "volume_ratio": 0.0,
                "label": "Price-volume pending",
            }
        current = candles[-1]
        previous = candles[-2]
        prior_volumes = [item["volume"] for item in candles[-6:-1] if item.get("volume")]
        avg_volume_5 = sum(prior_volumes) / len(prior_volumes) if prior_volumes else 0.0
        price_growth_pct = ((current["close"] - previous["close"]) / previous["close"]) * 100 if previous["close"] else 0.0
        volume_growth_pct = ((current["volume"] - previous["volume"]) / previous["volume"]) * 100 if previous["volume"] else 0.0
        volume_ratio = (current["volume"] / avg_volume_5) if avg_volume_5 else 0.0
        is_valid = current["close"] > previous["close"] and current["volume"] > previous["volume"] and volume_ratio >= 1.05
        label = "Price-volume growth" if is_valid else "Price-volume not confirmed"
        return {
            "is_valid": is_valid,
            "price_growth_pct": round(price_growth_pct, 2),
            "volume_growth_pct": round(volume_growth_pct, 2),
            "volume_ratio": round(volume_ratio, 2) if volume_ratio else 0.0,
            "label": label,
        }

    def _swing_signal_for_candles(self, symbol, candles):
        candles = self._clean_daily_candles(candles)
        if len(candles) < 35:
            return None
        closes = [item["close"] for item in candles]
        volumes = [item["volume"] for item in candles]
        current = candles[-1]
        previous = candles[-2]
        atr = self._atr_from_candles(candles, 14)
        sma20 = self._sma(closes, 20)
        sma50 = self._sma(closes, 50) or self._sma(closes, min(30, len(closes)))
        rsi = self._rsi_from_closes(closes, 14)
        if rsi is None or rsi < 40 or rsi > 60:
            return None
        prior_window = candles[-21:-1] if len(candles) >= 21 else candles[:-1]
        recent_high = max(item["high"] for item in prior_window)
        recent_low = min(item["low"] for item in prior_window)
        prev_high = previous["high"]
        prev_low = previous["low"]
        prev_close = previous["close"]
        avg_volume = self._sma(volumes, min(20, len(volumes))) or 0
        volume_ratio = (current["volume"] / avg_volume) if avg_volume else 0
        staircase = self._swing_staircase_pattern(candles)
        price_volume = self._swing_price_volume_growth(candles)
        price = current["close"]
        score = 42.0
        scan_state = "WATCH_RECLAIM"
        setup = "Bullish liquidity watch"
        notes = [f"Daily RSI is {rsi:.1f}, inside the 40-60 swing accumulation band."]

        if sma20 and price >= sma20:
            score += 8
            notes.append("Close is holding above 20-DMA.")
        elif sma20:
            score -= 4
            notes.append("Close is still below 20-DMA, so reclaim needs confirmation.")
        if sma50 and price >= sma50:
            score += 5
            notes.append("Price is above 50-DMA support.")
        if volume_ratio >= 1.15:
            score += 6
            notes.append("Volume is expanding versus 20-day average.")
        if staircase["is_valid"]:
            score += 12
            notes.append("Daily staircase pattern is confirmed with higher lows and improving closes.")
        else:
            score -= 8
            notes.append("Daily staircase structure is still forming.")
        if price_volume["is_valid"]:
            score += 12
            notes.append("Daily candle confirms price and volume growth together.")
        else:
            score -= 10
            notes.append("Price-volume growth is not confirmed on the latest daily candle.")

        range_20 = max(recent_high - recent_low, 0.01)
        controlled_retracement = price >= recent_low + range_20 * 0.30 and price <= recent_high - range_20 * 0.05
        pdl_sweep_reclaim = current["low"] < prev_low and price > prev_low
        swing_low_sweep_reclaim = current["low"] <= recent_low + max(atr * 0.12, 0.01) and price > recent_low + max(atr * 0.15, 0.01)
        previous_close_reclaim = current["low"] <= prev_close <= current["high"] and price > prev_close and price > current["open"]
        resistance_reclaim = price > prev_high and price > current["open"]

        defended_level = min(current["low"], prev_low if pdl_sweep_reclaim else recent_low)
        reclaimed_level = None
        liquidity_pattern = "Waiting for bullish reclaim"
        if pdl_sweep_reclaim:
            reclaimed_level = prev_low
            liquidity_pattern = "PDL sweep and reclaim"
            setup = "Bullish scan: PDL sweep reclaim"
            scan_state = "BULLISH_SCAN"
            score += 24
            notes.append("Price swept prior-day low liquidity and closed back above it.")
        elif swing_low_sweep_reclaim:
            reclaimed_level = recent_low
            liquidity_pattern = "20-day swing-low sweep and reclaim"
            setup = "Bullish scan: swing-low reclaim"
            scan_state = "BULLISH_SCAN"
            score += 22
            notes.append("Price swept the recent swing-low zone and reclaimed it on the daily candle.")
        elif previous_close_reclaim:
            reclaimed_level = prev_close
            liquidity_pattern = "Previous close reclaim"
            setup = "Bullish scan: previous close reclaim"
            scan_state = "BULLISH_SCAN"
            score += 16
            notes.append("Previous close was revisited and reclaimed with a bullish daily close.")
        elif resistance_reclaim:
            reclaimed_level = prev_high
            liquidity_pattern = "PDH resistance reclaim"
            setup = "Bullish scan: resistance reclaim"
            scan_state = "BULLISH_SCAN"
            score += 14
            notes.append("Close reclaimed prior-day high resistance.")
        else:
            reclaimed_level = max(prev_close, recent_low)
            score += 8
            notes.append("Liquidity zone is mapped, but bullish reclaim is not complete yet.")

        if controlled_retracement:
            score += 8
            notes.append("Retracement is controlled inside the 20-day range.")
        resistance_zone_low = min(prev_high, recent_high)
        resistance_zone_high = max(prev_high, recent_high)
        support_zone_low = min(defended_level, reclaimed_level or defended_level)
        support_zone_high = max(defended_level, reclaimed_level or defended_level) + max(atr * 0.15, 0.01)
        if resistance_zone_high <= price:
            resistance_zone_high = price + max(atr, price * 0.015)
        room_to_resistance = ((resistance_zone_low - price) / price) * 100 if price else 0
        if scan_state == "BULLISH_SCAN" and resistance_zone_low > price and room_to_resistance < 2.0:
            score -= 6
            notes.append("Nearest resistance zone is close, so scan quality is capped.")

        if scan_state == "BULLISH_SCAN" and (not staircase["is_valid"] or not price_volume["is_valid"]):
            scan_state = "WATCH_RECLAIM"
            setup = "Bullish watch: structure confirmation pending"
            score = min(score, 61.0)
        rating = "Strong Bullish" if score >= 75 and scan_state == "BULLISH_SCAN" else "Bullish" if score >= 62 and scan_state == "BULLISH_SCAN" else "Watch"
        return {
            "symbol": symbol,
            "name": self.symbol_to_name.get(symbol, symbol),
            "date": current["date"],
            "price": round(price, 2),
            "scan_state": scan_state,
            "rating": rating,
            "setup": setup,
            "score": round(max(0.0, min(score, 100.0)), 1),
            "liquidity_pattern": liquidity_pattern,
            "reclaimed_level": round(reclaimed_level, 2) if reclaimed_level is not None else None,
            "sma20": round(sma20, 2) if sma20 else None,
            "sma50": round(sma50, 2) if sma50 else None,
            "rsi": round(rsi, 2),
            "atr": round(atr, 2),
            "volume_ratio": round(volume_ratio, 2) if volume_ratio else None,
            "staircase_pattern": staircase["label"],
            "staircase_score": staircase["score"],
            "higher_low_count": staircase["higher_low_count"],
            "higher_close_count": staircase["higher_close_count"],
            "price_growth_pct": price_volume["price_growth_pct"],
            "volume_growth_pct": price_volume["volume_growth_pct"],
            "daily_volume_ratio": price_volume["volume_ratio"],
            "price_volume_pattern": price_volume["label"],
            "support_zone": f"{support_zone_low:.2f} - {support_zone_high:.2f}",
            "resistance_zone": f"{resistance_zone_low:.2f} - {resistance_zone_high:.2f}",
            "support_zone_low": round(support_zone_low, 2),
            "support_zone_high": round(support_zone_high, 2),
            "resistance_zone_low": round(resistance_zone_low, 2),
            "resistance_zone_high": round(resistance_zone_high, 2),
            "pdh": round(prev_high, 2),
            "pdl": round(prev_low, 2),
            "notes": notes[:5],
        }

    def _build_swing_scanner_payload(self, symbols=None, sessions=180):
        requested = [str(symbol).upper() for symbol in (symbols or NIFTY_50_SCANNER_STOCKS) if symbol]
        tracked = [symbol for symbol in requested if symbol in self.symbol_to_token]
        if not self.kite:
            return {
                "rows": [],
                "tracked_count": 0,
                "missing_count": len(requested),
                "symbols": requested,
                "updated_at": self._utc_now(),
                "market_open": self._is_market_open(),
                "cache_marker": self._completed_session_cache_marker(),
                "cache_version": SWING_SCANNER_CACHE_VERSION,
                "error": "Broker is not authenticated yet. Add Kite/Dhan credentials, then refresh the swing scanner.",
            }
        if not tracked:
            return {
                "rows": [],
                "tracked_count": 0,
                "missing_count": len(requested),
                "symbols": requested,
                "updated_at": self._utc_now(),
                "market_open": self._is_market_open(),
                "cache_marker": self._completed_session_cache_marker(),
                "cache_version": SWING_SCANNER_CACHE_VERSION,
                "error": "Broker universe is still loading. Wait a minute after authentication, then refresh the swing scanner.",
            }
        rows = []
        missing = 0
        for symbol in tracked:
            candles = self._fetch_swing_candles(symbol, sessions=sessions)
            signal = self._swing_signal_for_candles(symbol, candles)
            if signal:
                rows.append(signal)
            else:
                missing += 1
        rows.sort(key=lambda item: (item.get("score") or 0, item.get("volume_ratio") or 0), reverse=True)
        return {
            "rows": rows,
            "tracked_count": len(tracked),
            "missing_count": missing,
            "symbols": tracked or requested,
            "updated_at": self._utc_now(),
            "market_open": self._is_market_open(),
            "cache_marker": self._completed_session_cache_marker(),
            "cache_version": SWING_SCANNER_CACHE_VERSION,
            "error": None if rows else (self.last_error or "Swing scanner data is warming or broker history is unavailable."),
        }

    def _run_swing_scanner_refresh_job(self):
        with self.swing_scanner_lock:
            self.swing_scanner_status.update(
                {
                    "status": "running",
                    "started_at": self._utc_now(),
                    "finished_at": None,
                    "message": "Refreshing daily swing scanner...",
                    "error": None,
                }
            )
        try:
            payload = self._build_swing_scanner_payload()
            self._save_swing_scanner_cache(payload)
            with self.swing_scanner_lock:
                self.swing_scanner_status.update(
                    {
                        "status": "completed",
                        "finished_at": self._utc_now(),
                        "message": f"Swing scanner refreshed for {payload.get('tracked_count', 0)} stocks.",
                        "error": payload.get("error"),
                    }
                )
        except Exception as exc:
            self.last_error = str(exc)
            with self.swing_scanner_lock:
                self.swing_scanner_status.update(
                    {
                        "status": "failed",
                        "finished_at": self._utc_now(),
                        "message": "Swing scanner refresh failed.",
                        "error": str(exc),
                    }
                )
        finally:
            with self.swing_scanner_lock:
                self.swing_scanner_thread = None

    def _ensure_swing_scanner_background_refresh(self):
        with self.swing_scanner_lock:
            if self.swing_scanner_thread and self.swing_scanner_thread.is_alive():
                return False
            thread = threading.Thread(target=self._run_swing_scanner_refresh_job, daemon=True)
            self.swing_scanner_thread = thread
            thread.start()
            return True

    def _filter_swing_rows(self, payload, side="long", min_score=0):
        filtered = dict(payload or {})
        side = "LONG"
        try:
            min_score = float(min_score or 0)
        except (TypeError, ValueError):
            min_score = 0
        rows = []
        for row in filtered.get("rows") or []:
            row = dict(row)
            has_structure_fields = bool(row.get("staircase_pattern")) and row.get("daily_volume_ratio") is not None
            if not has_structure_fields and (row.get("scan_state") or "").upper() == "BULLISH_SCAN":
                row["scan_state"] = "WATCH_RECLAIM"
                row["rating"] = "Watch"
                row["setup"] = "Bullish watch: structure confirmation pending"
                row["score"] = min(float(row.get("score") or 0), 61.0)
            if (row.get("score") or 0) < min_score:
                continue
            row_state = (row.get("scan_state") or "").upper()
            if row_state not in {"BULLISH_SCAN", "WATCH_RECLAIM"}:
                continue
            rows.append(row)
        filtered["filtered_rows"] = rows
        filtered["total_rows"] = len(filtered.get("rows") or [])
        if filtered.get("rows") and not rows:
            filtered["warning"] = "No bullish scan rows match the current filter. Lower Min Score."
        filtered["filter"] = {"side": "long", "min_score": min_score}
        return filtered

    def get_swing_scanner(self, side="long", min_score=0, cached_only=True):
        cached = self._cached_swing_scanner_payload()
        if cached and not self._is_valid_swing_scanner_cache(cached):
            cached = None
        cache_marker = self._completed_session_cache_marker()
        if cached and cached.get("cache_marker") == cache_marker:
            payload = dict(cached)
        else:
            payload = cached if cached else self._build_swing_scanner_payload()
            if self.kite:
                self._ensure_swing_scanner_background_refresh()
            if not cached_only and self.kite:
                payload = self._build_swing_scanner_payload()
                self._save_swing_scanner_cache(payload)
        payload["market_open"] = self._is_market_open()
        payload["status"] = self._swing_status_payload()
        if not payload.get("rows") and self.kite:
            payload["cache_pending"] = True
        return self._filter_swing_rows(payload, side="long", min_score=min_score)

    def backtest_swing_symbol(self, symbol, sessions=260, holding_days=20):
        symbol = (symbol or "").strip().upper()
        if not self.kite:
            return {
                "symbol": symbol,
                "trades": [],
                "summary": {"total": 0, "wins": 0, "losses": 0, "win_rate": 0, "avg_return": 0},
                "error": "Broker is not authenticated yet, so backtest history cannot be fetched.",
            }
        if symbol not in self.symbol_to_token:
            return {
                "symbol": symbol,
                "trades": [],
                "summary": {"total": 0, "wins": 0, "losses": 0, "win_rate": 0, "avg_return": 0},
                "error": f"{symbol} is not loaded in the broker universe yet. Wait after authentication or check the symbol.",
            }
        candles = self._fetch_swing_candles(symbol, sessions=max(80, min(int(sessions or 260), 420)))
        if len(candles) < 60:
            return {
                "symbol": symbol,
                "trades": [],
                "summary": {"total": 0, "wins": 0, "losses": 0, "win_rate": 0, "avg_return": 0},
                "error": f"Only {len(candles)} daily candles were available. Increase sessions, refresh broker token, or wait for historical cache warming.",
            }
        trades = []
        open_trade = None
        for index in range(45, len(candles) - 1):
            window = candles[:index + 1]
            signal = self._swing_signal_for_candles(symbol, window)
            current = candles[index]
            if open_trade:
                exit_reason = None
                if current["low"] <= open_trade["support_zone_low"]:
                    exit_price = open_trade["support_zone_low"]
                    exit_reason = "support_broken"
                elif current["high"] >= open_trade["resistance_zone_low"]:
                    exit_price = open_trade["resistance_zone_low"]
                    exit_reason = "resistance_reached"
                if not exit_reason and index - open_trade["entry_index"] >= holding_days:
                    exit_price = current["close"]
                    exit_reason = "time"
                if exit_reason:
                    pnl_pct = ((exit_price - open_trade["entry_price"]) / open_trade["entry_price"]) * 100
                    trade = dict(open_trade)
                    trade.update(
                        {
                            "exit_date": current["date"],
                            "exit_price": round(exit_price, 2),
                            "exit_reason": exit_reason,
                            "return_pct": round(pnl_pct, 2),
                        }
                    )
                    trade.pop("entry_index", None)
                    trades.append(trade)
                    open_trade = None
                continue
            if signal and signal.get("scan_state") == "BULLISH_SCAN" and (signal.get("score") or 0) >= 68:
                next_candle = candles[index + 1]
                open_trade = {
                    "symbol": symbol,
                    "scan_state": signal["scan_state"],
                    "setup": signal["setup"],
                    "score": signal["score"],
                    "entry_date": next_candle["date"],
                    "entry_price": round(next_candle["open"], 2),
                    "support_zone": signal["support_zone"],
                    "resistance_zone": signal["resistance_zone"],
                    "support_zone_low": signal["support_zone_low"],
                    "resistance_zone_low": signal["resistance_zone_low"],
                    "entry_index": index + 1,
                }
        returns = [trade["return_pct"] for trade in trades]
        wins = len([value for value in returns if value > 0])
        losses = len([value for value in returns if value <= 0])
        avg_return = sum(returns) / len(returns) if returns else 0
        return {
            "symbol": symbol,
            "name": self.symbol_to_name.get(symbol, symbol),
            "trades": trades[-80:],
            "summary": {
                "total": len(trades),
                "wins": wins,
                "losses": losses,
                "win_rate": round((wins / len(trades)) * 100, 2) if trades else 0,
                "avg_return": round(avg_return, 2),
            },
            "error": None,
        }

    def _run_refresh_job(self, reason, market_open):
        try:
            self._restore_previous_close_cache()
            self._refresh_sector_memberships(force=False)
            if market_open:
                if reason in {"reconnect", "stale", "initial"}:
                    self._restart_live_feed(reason=reason)
                self._refresh_rest_snapshot(force=reason in {"initial", "reconnect", "stale", "startup_snapshot"})
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
            return self._fallback_sector_members(sector_name)
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
            return members or self._fallback_sector_members(sector_name)
        except Exception as exc:
            self.last_error = f"Sector constituent load failed for {sector_name}: {exc}"
            return self._fallback_sector_members(sector_name)

    def _fallback_sector_members(self, sector_name):
        members = FALLBACK_SECTOR_MEMBERS.get(str(sector_name or "").strip().upper(), [])
        return [symbol for symbol in members if symbol in self.symbol_to_token]

    def _refresh_sector_memberships(self, force=False):
        restored_from_cache = False
        if not self.sector_members:
            restored_from_cache = self._restore_cached_sector_memberships()

        today = datetime.now(IST).date().isoformat()
        if restored_from_cache and not force and self.sector_members:
            self.last_membership_refresh_date = today
            return
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
        for sector_name, fallback_members in FALLBACK_SECTOR_MEMBERS.items():
            if sector_members.get(sector_name):
                continue
            members = [symbol for symbol in fallback_members if symbol in self.symbol_to_token]
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

    def build_universe(self, kite: KiteConnect, sector_names, warm_dashboard=True):
        if self.broker == "dhan":
            return self._build_dhan_universe(sector_names, warm_dashboard=warm_dashboard)

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
        if warm_dashboard:
            self._refresh_sector_memberships(force=False)
            self._restore_previous_close_cache()

            prev_close, latest = self._fetch_sector_quote(kite, list(index_tokens.keys()))
            if prev_close:
                self.sector_prev_close.update(prev_close)
            if latest:
                self.sector_latest.update(latest)

    def _fetch_sector_quote(self, kite: KiteConnect, sector_symbols):
        if not sector_symbols:
            return {}, {}
        try:
            if self.broker == "dhan":
                if self._is_quote_rate_limited():
                    return {}, {}
                ids = [
                    int(self.sector_tokens.get(symbol))
                    for symbol in sector_symbols
                    if self.sector_tokens.get(symbol)
                ]
                if not ids:
                    return {}, {}
                try:
                    data = kite.marketfeed_quote({"IDX_I": ids})
                except DhanRateLimitError as exc:
                    self._mark_quote_rate_limited(exc)
                    return {}, {}
                prev = {}
                latest = {}
                prev_cache_updated = False
                segment_data = data.get("IDX_I") or {}
                for security_id, payload in segment_data.items():
                    token = int(security_id)
                    name = self.sector_token_to_name.get(token)
                    if not name:
                        continue
                    ohlc = payload.get("ohlc") or {}
                    close = ohlc.get("close")
                    last_price = payload.get("last_price")
                    if close not in (None, 0):
                        prev[name] = close
                        prev_cache_updated = self._remember_previous_close("sectors", name, close) or prev_cache_updated
                    if last_price not in (None, 0):
                        base_close = close if close not in (None, 0) else self.sector_prev_close.get(name)
                        if base_close in (None, 0):
                            base_close = self._cached_previous_close("sectors", name)
                        change = 0.0 if base_close in (None, 0) else (last_price - base_close) / base_close * 100
                        latest[name] = {
                            "sector": name,
                            "price": round(last_price, 2),
                            "change": round(change, 2),
                        }
                if prev_cache_updated:
                    self._save_previous_close_cache()
                return prev, latest

            symbols = [f"NSE:{s}" for s in sector_symbols]
            data = kite.quote(symbols)
            prev = {}
            latest = {}
            prev_cache_updated = False
            for sym, payload in data.items():
                ohlc = payload.get("ohlc") or {}
                close = ohlc.get("close")
                last_price = payload.get("last_price")
                name = sym.split(":", 1)[-1]
                if close not in (None, 0):
                    prev[name] = close
                    prev_cache_updated = self._remember_previous_close("sectors", name, close) or prev_cache_updated
                if last_price not in (None, 0):
                    base_close = close if close not in (None, 0) else self.sector_prev_close.get(name)
                    if base_close in (None, 0):
                        base_close = self._cached_previous_close("sectors", name)
                    if base_close in (None, 0):
                        change = 0.0
                    else:
                        change = (last_price - base_close) / base_close * 100
                    latest[name] = {
                        "sector": name,
                        "price": round(last_price, 2),
                        "change": round(change, 2),
                    }
            if prev_cache_updated:
                self._save_previous_close_cache()
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
        close = candles[-1].get("close") if candles else None
        if close not in (None, 0):
            self._remember_previous_close("symbols", symbol, close)
            self._save_previous_close_cache()
        return close

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
        if latest_close not in (None, 0):
            self._remember_previous_close("symbols", symbol, latest_close)
        latest_candle = candles[-1]
        row = self._build_stock_row(
            symbol,
            latest_close,
            prev_close,
            volume=latest_volume,
            day_open=latest_candle.get("open"),
            day_high=latest_candle.get("high"),
            day_low=latest_candle.get("low"),
        )
        latest_dt = candles[-1].get("date")
        return row, latest_dt

    def _build_sector_row_from_candles(self, sector_name, candles):
        if len(candles) < 2:
            return None, None
        prev_close = candles[-2].get("close")
        latest_close = candles[-1].get("close")
        if latest_close in (None, 0) or prev_close in (None, 0):
            return None, None
        self._remember_previous_close("sectors", sector_name, latest_close)
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
        if self.broker == "dhan":
            if self._is_quote_rate_limited():
                return quoted
            ids = [
                int(self.symbol_to_token.get(symbol))
                for symbol in symbols
                if self.symbol_to_token.get(symbol)
            ]
            for chunk in self._chunked(ids, 1000):
                try:
                    data = kite.marketfeed_quote({"NSE_EQ": chunk})
                except DhanRateLimitError as exc:
                    self._mark_quote_rate_limited(exc)
                    break
                except Exception as exc:
                    self.last_error = str(exc)
                    continue
                for security_id, payload in (data.get("NSE_EQ") or {}).items():
                    symbol = self.token_to_symbol.get(int(security_id))
                    if symbol:
                        quoted[f"NSE:{symbol}"] = payload
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
        if self.broker == "dhan" and self._is_quote_rate_limited():
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
        prev_cache_updated = False

        for key, payload in quoted.items():
            symbol = key.split(":", 1)[-1]
            last_price = payload.get("last_price")
            volume = self._extract_volume(payload, fallback=(self.latest.get(symbol) or {}).get("volume"))
            ohlc = payload.get("ohlc") or {}
            close = ohlc.get("close")
            if close not in (None, 0):
                self.rest_prev_close[symbol] = close
                prev_cache_updated = self._remember_previous_close("symbols", symbol, close) or prev_cache_updated
            base_close = close if close not in (None, 0) else self.rest_prev_close.get(symbol)
            if base_close in (None, 0):
                base_close = self._cached_previous_close("symbols", symbol)
            row = self._build_stock_row(
                symbol,
                last_price,
                base_close,
                volume=volume,
                day_open=ohlc.get("open"),
                day_high=ohlc.get("high"),
                day_low=ohlc.get("low"),
            )
            if row:
                updated[symbol] = row
                self._record_acceleration_price(symbol, last_price, cumulative_volume=volume)
            elif not market_open and last_price not in (None, 0):
                missing_history.append((symbol, last_price, volume))

        if not market_open:
            for symbol, last_price, volume in missing_history:
                close = self._fetch_prev_close_from_history(self.kite, symbol)
                if close in (None, 0):
                    continue
                self.rest_prev_close[symbol] = close
                prev_cache_updated = self._remember_previous_close("symbols", symbol, close) or prev_cache_updated
                row = self._build_stock_row(symbol, last_price, close, volume=volume)
                if row:
                    updated[symbol] = row
                    self._record_acceleration_price(symbol, last_price, cumulative_volume=volume)

        if updated:
            with self.lock:
                self.latest.update(updated)
                self.last_update = self._utc_now()
            self.last_snapshot_source = "api"
            self._save_latest_rows_cache()
            self._save_sector_breakdowns_cache(updated, market_open)
        if prev_cache_updated:
            self._save_previous_close_cache()

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
            if stock_rows or sector_rows:
                self._save_previous_close_cache()
            snapshot = self._build_snapshot(False)
            if self._stock_row_count(snapshot):
                snapshot["session_marker"] = completed_session.isoformat()
                self._save_snapshot(snapshot)
                self._save_closed_snapshot(snapshot)
            return True
        return False

    def start(self, api_key, access_token, sector_names):
        with self.start_lock:
            try:
                self.broker = "kite"
                self.api_key = api_key
                self.access_token = access_token
                self.client_id = None
                self.sector_names = list(sector_names or [])
                self._close_ticker()
                kite = KiteConnect(api_key=api_key)
                kite.set_access_token(access_token)
                kite.set_session_expiry_hook(self._on_session_expiry)
                self.kite = kite
                self.build_universe(kite, sector_names, warm_dashboard=False)
                self._restore_previous_close_cache()
                self._create_ticker()
                self._ensure_background_refresh(
                    market_open=self._is_market_open(),
                    reason="startup_snapshot",
                )
                self.last_error = None
            except Exception as exc:
                self.last_error = str(exc)
                self.connected = False

    def start_dhan(self, client_id, access_token, sector_names):
        with self.start_lock:
            try:
                self.broker = "dhan"
                self.client_id = client_id
                self.api_key = client_id
                self.access_token = access_token
                self.sector_names = list(sector_names or [])
                self._close_ticker()
                self.kite = DhanClient(client_id, access_token)
                self.build_universe(self.kite, sector_names, warm_dashboard=False)
                self._restore_previous_close_cache()
                self._create_ticker()
                self._ensure_background_refresh(
                    market_open=self._is_market_open(),
                    reason="startup_snapshot",
                )
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
                    if close not in (None, 0):
                        self._remember_previous_close("symbols", symbol, close)
                    if base_close in (None, 0):
                        base_close = self._cached_previous_close("symbols", symbol)
                    row = self._build_stock_row(
                        symbol,
                        last_price,
                        base_close,
                        volume=volume,
                        day_open=ohlc.get("open"),
                        day_high=ohlc.get("high"),
                        day_low=ohlc.get("low"),
                    )
                    if row:
                        self.latest[symbol] = row
                        self._record_acceleration_price(symbol, last_price, cumulative_volume=volume)
                else:
                    name = self.sector_token_to_name.get(token)
                    if not name:
                        continue
                    base_close = close
                    if base_close in (None, 0):
                        base_close = self.sector_prev_close.get(name)
                    if close not in (None, 0):
                        self._remember_previous_close("sectors", name, close)
                    if base_close in (None, 0):
                        base_close = self._cached_previous_close("sectors", name)
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
        if not self.sector_latest and not self.symbol_to_sectors:
            self._restore_cached_sector_memberships()
        with self.lock:
            movers = list(self.latest.values())
            if self.nifty500_set:
                movers = [m for m in movers if m["symbol"].upper() in self.nifty500_set]
            gainers = [dict(m) for m in sorted([m for m in movers if m["change"] > 0], key=lambda x: x["change"], reverse=True)[:20]]
            losers = [dict(m) for m in sorted([m for m in movers if m["change"] < 0], key=lambda x: x["change"])[:20]]
            sectors = list(self.sector_latest.values())
            if not sectors:
                grouped = defaultdict(list)
                for row in movers:
                    for sector in row.get("sectors") or self.symbol_to_sectors.get(row.get("symbol", "").upper(), []):
                        if row.get("change") is not None:
                            grouped[sector].append(float(row.get("change") or 0))
                sectors = [
                    {
                        "sector": sector,
                        "price": "-",
                        "change": round(sum(changes) / len(changes), 2),
                        "rank_source": "constituent_average",
                        "constituent_count": len(changes),
                    }
                    for sector, changes in grouped.items()
                    if changes
                ]
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

    def _latest_volume_values(self, candles):
        volumes = [
            volume
            for volume in (self._candle_volume(candle) for candle in candles or [])
            if volume not in (None, 0)
        ]
        return volumes[-ACCELERATION_VOLUME_SMA_SESSIONS:]

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
            cached_close = self._cached_previous_close("symbols", symbol)
            if (
                not force
                and cached
                and cached.get("cache_marker") == cache_marker
                and self._payload_matches_broker(cached)
                and cached_close not in (None, 0)
            ):
                continue
            if self._warm_previous_day_badge_for_symbol(symbol, cache_marker) is not None:
                updated += 1

        if updated:
            self._save_previous_day_badges_cache()
            self._save_previous_close_cache()
        return {"processed": processed, "total": total, "updated": updated}

    def _warm_acceleration_volume_sma_cache(self, cache_marker, force=False):
        self._restore_acceleration_volume_sma_cache()
        symbols = self._symbols_for_badge_warmup()
        total = len(symbols)
        if not total:
            return {"processed": 0, "total": 0, "updated": 0}

        completed_session = datetime.fromisoformat(cache_marker).date()
        session_window = self._trading_session_window(completed_session, ACCELERATION_VOLUME_LOOKBACK_SESSIONS)
        if len(session_window) < ACCELERATION_VOLUME_SMA_SESSIONS:
            return {"processed": 0, "total": total, "updated": 0}

        from_date = self._session_start_dt(session_window[0])
        to_date = self._session_end_dt(session_window[-1])
        processed = 0
        updated = 0
        for symbol in symbols:
            processed += 1
            self._update_history_cache_status(
                processed=processed,
                total=total + len(self.sector_tokens) + 1,
                message=f"Caching acceleration volume baselines ({processed}/{total})",
            )
            cached = self.acceleration_volume_sma_cache.get(symbol)
            if (
                not force
                and cached
                and cached.get("cache_marker") == cache_marker
                and self._payload_matches_broker(cached)
                and cached.get("volume_sma") not in (None, 0)
            ):
                continue
            token = self.symbol_to_token.get(symbol)
            candles = self._fetch_recent_day_candles(token, from_date, to_date, limit=ACCELERATION_VOLUME_LOOKBACK_SESSIONS)
            volumes = self._latest_volume_values(candles)
            if len(volumes) < ACCELERATION_VOLUME_SMA_SESSIONS:
                continue
            volume_sum = sum(volumes)
            session_minutes = ACCELERATION_VOLUME_SMA_SESSIONS * NSE_INTRADAY_SESSION_MINUTES
            volume_sma = volume_sum / session_minutes
            self.acceleration_volume_sma_cache[symbol] = {
                "cache_marker": cache_marker,
                "broker": self._current_broker(),
                "sessions": ACCELERATION_VOLUME_SMA_SESSIONS,
                "lookback_sessions": ACCELERATION_VOLUME_LOOKBACK_SESSIONS,
                "session_minutes": session_minutes,
                "volume_sum": int(volume_sum),
                "volume_sma": round(volume_sma, 2),
                "updated_at": self._utc_now(),
            }
            updated += 1

        if updated:
            self._save_acceleration_volume_sma_cache()
        return {"processed": processed, "total": total, "updated": updated}

    def _warm_market_open_stock_cache(self, cache_marker, force=False):
        self._restore_previous_day_badges_cache()
        self._restore_acceleration_volume_sma_cache()
        symbols = self._symbols_for_badge_warmup()
        total = len(symbols)
        if not total:
            return {
                "processed": 0,
                "total": 0,
                "badge_updated": 0,
                "volume_updated": 0,
                "previous_close_updated": 0,
            }

        completed_session = datetime.fromisoformat(cache_marker).date()
        session_window = self._trading_session_window(completed_session, ACCELERATION_VOLUME_LOOKBACK_SESSIONS)
        if len(session_window) < ACCELERATION_VOLUME_SMA_SESSIONS:
            return {
                "processed": 0,
                "total": total,
                "badge_updated": 0,
                "volume_updated": 0,
                "previous_close_updated": 0,
            }

        from_date = self._session_start_dt(session_window[0])
        to_date = self._session_end_dt(session_window[-1])
        processed = 0
        badge_updated = 0
        volume_updated = 0
        previous_close_updated = 0
        prev_close_cache_dirty = False
        badges_dirty = False
        volume_dirty = False

        for symbol in symbols:
            processed += 1
            self._update_history_cache_status(
                processed=processed,
                total=total + len(self.sector_tokens) + 1,
                message=f"Caching market-open stock data ({processed}/{total})",
            )

            badge_cached = self.previous_day_badges_cache.get(symbol)
            volume_cached = self.acceleration_volume_sma_cache.get(symbol)
            previous_close_cached = self._cached_previous_close("symbols", symbol)
            stock_cache_ready = (
                not force
                and badge_cached
                and badge_cached.get("cache_marker") == cache_marker
                and self._payload_matches_broker(badge_cached)
                and volume_cached
                and volume_cached.get("cache_marker") == cache_marker
                and self._payload_matches_broker(volume_cached)
                and volume_cached.get("volume_sma") not in (None, 0)
                and previous_close_cached not in (None, 0)
            )
            if stock_cache_ready:
                continue

            token = self.symbol_to_token.get(symbol)
            candles = self._fetch_recent_day_candles(token, from_date, to_date, limit=ACCELERATION_VOLUME_LOOKBACK_SESSIONS)
            if not candles:
                continue

            latest_close = candles[-1].get("close")
            if latest_close not in (None, 0):
                if self._remember_previous_close("symbols", symbol, latest_close, cache_marker=cache_marker):
                    prev_close_cache_dirty = True
                    previous_close_updated += 1

            if len(candles) >= 2:
                current_close = candles[-1].get("close")
                prior_close = candles[-2].get("close")
                if current_close not in (None, 0) and prior_close not in (None, 0):
                    change = round(((current_close - prior_close) / prior_close) * 100, 2)
                    self.previous_day_badges_cache[symbol] = {
                        "cache_marker": cache_marker,
                        "broker": self._current_broker(),
                        "change": change,
                    }
                    badges_dirty = True
                    badge_updated += 1

            volumes = self._latest_volume_values(candles)
            if len(volumes) >= ACCELERATION_VOLUME_SMA_SESSIONS:
                volume_sum = sum(volumes)
                session_minutes = ACCELERATION_VOLUME_SMA_SESSIONS * NSE_INTRADAY_SESSION_MINUTES
                volume_sma = volume_sum / session_minutes
                self.acceleration_volume_sma_cache[symbol] = {
                    "cache_marker": cache_marker,
                    "broker": self._current_broker(),
                    "sessions": ACCELERATION_VOLUME_SMA_SESSIONS,
                    "lookback_sessions": ACCELERATION_VOLUME_LOOKBACK_SESSIONS,
                    "session_minutes": session_minutes,
                    "volume_sum": int(volume_sum),
                    "volume_sma": round(volume_sma, 2),
                    "updated_at": self._utc_now(),
                }
                volume_dirty = True
                volume_updated += 1

        if prev_close_cache_dirty:
            self._save_previous_close_cache()
        if badges_dirty:
            self._save_previous_day_badges_cache()
        if volume_dirty:
            self._save_acceleration_volume_sma_cache()
        return {
            "processed": processed,
            "total": total,
            "badge_updated": badge_updated,
            "volume_updated": volume_updated,
            "previous_close_updated": previous_close_updated,
        }

    def _market_open_stock_cache_ready(self, cache_marker):
        self._restore_previous_day_badges_cache()
        self._restore_acceleration_volume_sma_cache()
        symbols = self._symbols_for_badge_warmup()
        if not symbols:
            return True
        checked = 0
        ready = 0
        for symbol in symbols:
            checked += 1
            badge_cached = self.previous_day_badges_cache.get(symbol)
            volume_cached = self.acceleration_volume_sma_cache.get(symbol)
            previous_close_cached = self._cached_previous_close("symbols", symbol)
            if (
                badge_cached
                and badge_cached.get("cache_marker") == cache_marker
                and self._payload_matches_broker(badge_cached)
                and volume_cached
                and volume_cached.get("cache_marker") == cache_marker
                and self._payload_matches_broker(volume_cached)
                and volume_cached.get("volume_sma") not in (None, 0)
                and previous_close_cached not in (None, 0)
            ):
                ready += 1
        return checked > 0 and ready >= max(1, int(checked * 0.9))

    def _build_rrg_series_map(self, benchmark_symbol, cache_marker):
        if not self.kite:
            return None, {}, f"{self._broker_label()} session is not available for historical index candles."
        benchmark_token = self.index_tokens.get(benchmark_symbol)
        if not benchmark_token:
            return None, {}, f"Benchmark token for {benchmark_symbol} is not available."

        session_window = self._trading_session_window(datetime.fromisoformat(cache_marker).date(), RRG_FETCH_SESSIONS)
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
        if benchmark_series:
            self._remember_previous_close("sectors", benchmark_symbol, benchmark_series[-1][1], cache_marker)
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
                self._remember_previous_close("sectors", sector_name, series[-1][1], cache_marker)

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
                "broker": self._current_broker(),
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
        payload["broker"] = self._current_broker()
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
            message=f"Preparing {self._broker_label()} historical market cache...",
            broker=self._current_broker(),
            error=None,
            market_open_ready=False,
        )
        try:
            stock_summary = self._warm_market_open_stock_cache(cache_marker, force=force)
            rrg_payload = self._warm_relative_rotation_graph_cache(cache_marker, force=force)
            self._save_previous_close_cache()
            status = "completed" if rrg_payload.get("items") else "warning"
            self._update_history_cache_status(
                status=status,
                finished_at=self._utc_now(),
                processed=total,
                total=total,
                message=(
                    f"Premarket sync ready: cached {stock_summary['previous_close_updated']} previous closes, "
                    f"{stock_summary['badge_updated']} badge rows, {stock_summary['volume_updated']} volume baselines and "
                    f"{len(rrg_payload.get('items') or [])} RRG sectors for {cache_marker}."
                ),
                error=rrg_payload.get("error"),
                market_open_ready=True,
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
        stock_cache_ready = self._market_open_stock_cache_ready(cache_marker)
        if (
            not force
            and stock_cache_ready
            and self._cache_payload_matches_marker(cached_rrg, cache_marker)
            and cached_rrg.get("items")
        ):
            return self._update_history_cache_status(
                status="completed",
                session_marker=cache_marker,
                broker=self._current_broker(),
                finished_at=self._utc_now(),
                message=f"{self._broker_label()} premarket cache for {cache_marker} is already ready.",
                error=None,
                market_open_ready=True,
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
                    "message": f"Preparing {self._broker_label()} historical market cache...",
                    "broker": self._current_broker(),
                    "error": None,
                    "market_open_ready": False,
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
            "broker": self._current_broker(),
            "updated_at": self.last_update or self._utc_now(),
            "latest_session": latest_session,
            "normalization_window": RRG_NORMALIZATION_WINDOW,
            "trail_points": RRG_TRAIL_POINTS,
            "items": items,
            "x_domain": [round(min(all_x) - padding, 2), round(max(all_x) + padding, 2)],
            "y_domain": [round(min(all_y) - padding, 2), round(max(all_y) + padding, 2)],
        }

    def get_relative_rotation_graph(self, benchmark_symbol=RRG_BENCHMARK_SYMBOL, cached_only=False, auto_start=False):
        market_open = self._is_market_open()
        cache_marker = self._completed_session_cache_marker()
        cached = self._cached_relative_rotation_graph()
        if self._cache_payload_matches_marker(cached, cache_marker) and cached.get("items"):
            cached["market_open"] = market_open
            cached["cache_pending"] = False
            cached["cache_stale"] = False
            return cached

        if cached_only:
            if cached and cached.get("items") and self._payload_matches_broker(cached):
                cached["market_open"] = market_open
                cached["cache_pending"] = True
                cached["cache_stale"] = True
                cached["error"] = (
                    f"Showing cached {self._broker_label()} rotation data from {cached.get('cache_marker') or 'the prior session'} "
                    "until the shared premarket sync is refreshed."
                )
                return cached
            if auto_start:
                self.start_daily_market_history_cache(force=False)
            return self._empty_rrg_payload(
                benchmark_symbol,
                cache_marker,
                market_open,
                message=f"{self._broker_label()} RRG cache is not ready. Admin can run Premarket Sync.",
            )

        if cached and cached.get("items") and self._payload_matches_broker(cached):
            cached["market_open"] = market_open
            cached["cache_pending"] = True
            cached["cache_stale"] = True
            cached["error"] = (
                f"Showing cached {self._broker_label()} rotation data from {cached.get('cache_marker') or 'the prior session'} "
                "until the shared premarket sync is refreshed."
            )
            return cached

        status = self.start_daily_market_history_cache(force=False) if auto_start else {}
        return self._empty_rrg_payload(
            benchmark_symbol,
            cache_marker,
            market_open,
            message=status.get("message") or "RRG cache is not ready. Admin can run Premarket Sync.",
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
        completed_marker = self._completed_session_cache_marker() if not market_open else None
        closed_cache_fresh = (
            not market_open
            and closed_cached
            and self._stock_row_count(closed_cached)
            and self._snapshot_cache_marker(closed_cached) == completed_marker
        )
        if not market_open and closed_cache_fresh:
            if self.kite and (not self.latest or not self.sector_latest):
                self._ensure_background_refresh(market_open=False, reason="closed_market_bootstrap")
            return self._decorate_snapshot_rows(
                self._ensure_snapshot_sector_rows(self._with_runtime_fields(closed_cached, False, "closed_cache"))
            )

        if self.kite:
            if self._is_live_feed_stale():
                self.connected = False
                if not self.last_error or "stalled" not in self.last_error.lower():
                    self.last_error = "Live feed stalled, refreshing from API"
                self._ensure_background_refresh(market_open=True, reason="stale")
            if market_open:
                stale_closed_source = self.last_snapshot_source in {
                    "historical_eod",
                    "closed_cache",
                    "closed_cache_rows",
                    "cache",
                }
                if stale_closed_source and time.time() - self.last_rest_refresh_ts > 15:
                    self._ensure_background_refresh(market_open=True, reason="initial")
                elif not self.latest:
                    self._ensure_background_refresh(market_open=True, reason="initial")
                elif not self.connected:
                    self._ensure_background_refresh(market_open=True, reason="reconnect")
                elif not self.sector_latest:
                    self._ensure_background_refresh(market_open=True, reason="sector_bootstrap")
            else:
                if not self.latest or not self.sector_latest:
                    self._ensure_background_refresh(market_open=False, reason="closed_market_bootstrap")
                elif closed_cached and not closed_cache_fresh:
                    self._ensure_background_refresh(market_open=False, reason="closed_market_stale")

        snapshot = self._build_snapshot(market_open)
        cached = self._cached_snapshot()
        has_stock_data = any(snapshot.get(key) for key in ("gainers", "losers"))
        has_sector_data = any(snapshot.get(key) for key in ("sector_gainers", "sector_losers"))
        if not market_open and closed_cache_fresh:
            if self._stock_row_count(closed_cached) > self._stock_row_count(snapshot):
                return self._decorate_snapshot_rows(
                    self._ensure_snapshot_sector_rows(self._with_runtime_fields(closed_cached, False, "closed_cache"))
                )
        if has_stock_data:
            self._save_snapshot(snapshot)
            if not market_open:
                self._save_closed_snapshot(snapshot)
            return snapshot
        if not market_open and has_sector_data and cached and any(cached.get(key) for key in ("gainers", "losers")):
            return self._decorate_snapshot_rows(self._ensure_snapshot_sector_rows(self._merge_with_cached_snapshot(snapshot, cached)))
        if has_sector_data and not cached:
            self._save_snapshot(snapshot)
            return snapshot
        cached_fresh = (
            cached
            and not market_open
            and self._snapshot_cache_marker(cached) == completed_marker
        )
        if cached_fresh:
            cached["connected"] = self.connected
            cached["error"] = self.last_error
            cached["market_open"] = market_open
            cached["snapshot_source"] = "cache"
            return self._decorate_snapshot_rows(self._ensure_snapshot_sector_rows(cached))
        return snapshot

    def _get_latest_rows_for_symbols(self, symbols):
        if not self.kite:
            return self._rows_for_symbols_from_cache(symbols)

        market_open = self._is_market_open()
        if not market_open:
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
                self._save_previous_close_cache()
            return rows

        requested = [symbol for symbol in symbols if symbol in self.symbol_to_token]
        if not requested:
            return []

        quoted = self._quote_symbols(self.kite, requested)
        rows = []
        missing_history = []
        prev_cache_updated = False
        for key, payload in quoted.items():
            symbol = key.split(":", 1)[-1]
            last_price = payload.get("last_price")
            volume = self._extract_volume(payload, fallback=(self.latest.get(symbol) or {}).get("volume"))
            ohlc = payload.get("ohlc") or {}
            close = ohlc.get("close")
            if close not in (None, 0):
                self.rest_prev_close[symbol] = close
                prev_cache_updated = self._remember_previous_close("symbols", symbol, close) or prev_cache_updated
            base_close = close if close not in (None, 0) else self.rest_prev_close.get(symbol)
            if base_close in (None, 0):
                base_close = self._cached_previous_close("symbols", symbol)
            row = self._build_stock_row(
                symbol,
                last_price,
                base_close,
                volume=volume,
                day_open=ohlc.get("open"),
                day_high=ohlc.get("high"),
                day_low=ohlc.get("low"),
            )
            if row:
                rows.append(row)
                self._record_acceleration_price(symbol, last_price, cumulative_volume=volume)
            elif not market_open and last_price not in (None, 0):
                missing_history.append((symbol, last_price, volume))

        if not market_open:
            for symbol, last_price, volume in missing_history:
                close = self._fetch_prev_close_from_history(self.kite, symbol)
                if close in (None, 0):
                    continue
                self.rest_prev_close[symbol] = close
                prev_cache_updated = self._remember_previous_close("symbols", symbol, close) or prev_cache_updated
                row = self._build_stock_row(symbol, last_price, close, volume=volume)
                if row:
                    rows.append(row)
                    self._record_acceleration_price(symbol, last_price, cumulative_volume=volume)

        if rows:
            with self.lock:
                for row in rows:
                    self.latest[row["symbol"]] = row
                self.last_update = self._utc_now()
            self.last_snapshot_source = "api"
            self._save_latest_rows_cache()
        if prev_cache_updated:
            self._save_previous_close_cache()
        if not rows:
            cached_rows = self._rows_for_symbols_from_cache(requested)
            if cached_rows:
                self.last_snapshot_source = "live_cache_rows"
                return cached_rows
        return rows

    def get_sector_breakdown(self, sector_name):
        sector = (sector_name or "").strip()
        if not sector:
            return {"sector": "", "stocks": [], "updated_at": self.last_update, "market_open": self._is_market_open()}

        market_open = self._is_market_open()
        if not market_open:
            cached_breakdowns = self._cached_sector_breakdowns() or {}
            cached_payload = cached_breakdowns.get(sector)
            cache_marker = self._completed_session_cache_marker()
            if cached_payload and cached_payload.get("stocks"):
                marker = cached_payload.get("session_marker") or self._snapshot_cache_marker(cached_payload)
                if marker == cache_marker:
                    payload = dict(cached_payload)
                    payload["stocks"] = self._rank_sector_breakdown_rows(cached_payload["stocks"], market_open=False)
                    payload["constituent_count"] = len(payload["stocks"])
                    return payload

        self._refresh_sector_memberships(force=not bool(self.sector_members))
        symbols = self.sector_members.get(sector, [])
        if not symbols:
            symbols = self._fallback_sector_members(sector)
            if symbols:
                self.sector_members[sector] = symbols
                for symbol in symbols:
                    sectors = set(self.symbol_to_sectors.get(symbol, []))
                    sectors.add(sector)
                    self.symbol_to_sectors[symbol] = sorted(sectors)

        if not market_open:
            rows = self._rows_for_symbols_from_cache(symbols)
            if not rows:
                if cached_payload and cached_payload.get("stocks"):
                    marker = cached_payload.get("session_marker") or self._snapshot_cache_marker(cached_payload)
                    if marker == cache_marker:
                        payload = dict(cached_payload)
                        payload["stocks"] = self._rank_sector_breakdown_rows(cached_payload["stocks"], market_open=False)
                        payload["constituent_count"] = len(payload["stocks"])
                        return payload
        else:
            rows = self._get_latest_rows_for_symbols(symbols)

        ranked = self._rank_sector_breakdown_rows(rows, market_open)
        if market_open:
            self._decorate_rows_with_previous_day_badges(ranked)
        return {
            "sector": sector,
            "stocks": ranked,
            "updated_at": self.last_update,
            "session_marker": self._completed_session_cache_marker() if not market_open else None,
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
