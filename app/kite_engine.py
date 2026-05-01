import csv
import io
import re
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, time as dtime
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests
from kiteconnect import KiteConnect, KiteTicker

from app.config import KITE_ACCESS_TOKEN_KEY, KITE_TOKEN_UPDATED_KEY
from app.db import load_market_cache, save_market_cache

SNAPSHOT_CACHE_KEY = "latest_snapshot"
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
        self.http = requests.Session()
        self.http.headers.update(HTTP_HEADERS)

    def _extract_underlying(self, tradingsymbol):
        match = re.match(r"^[A-Z]+", tradingsymbol)
        return match.group(0) if match else tradingsymbol

    def _chunked(self, items, size):
        for idx in range(0, len(items), size):
            yield items[idx:idx + size]

    def _utc_now(self):
        return datetime.utcnow().isoformat(timespec="seconds")

    def _is_tracked_symbol(self, symbol):
        return not self.nifty500_set or symbol.upper() in self.nifty500_set

    def _build_stock_row(self, symbol, last_price, close):
        if last_price in (None, 0) or close in (None, 0):
            return None
        change = (last_price - close) / close * 100
        return {
            "symbol": symbol,
            "name": self.symbol_to_name.get(symbol, symbol),
            "price": round(last_price, 2),
            "change": round(change, 2),
            "is_fno": symbol.upper() in self.fno_symbols or self.symbol_to_name.get(symbol, "").upper() in self.fno_symbols,
            "sectors": self.symbol_to_sectors.get(symbol, []),
        }

    def _cached_snapshot(self):
        return load_market_cache(SNAPSHOT_CACHE_KEY)

    def _save_snapshot(self, snapshot):
        try:
            save_market_cache(SNAPSHOT_CACHE_KEY, snapshot)
        except Exception:
            return

    def _run_refresh_job(self, reason, market_open):
        try:
            if market_open:
                self._refresh_rest_snapshot(force=reason == "initial")
                self._refresh_sector_snapshot(force=True)
            else:
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
        today = datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat()
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

        index_tokens = {}
        for inst in instruments:
            if inst.get("segment") != "INDICES":
                continue
            ts = inst.get("tradingsymbol")
            if ts in sector_names:
                index_tokens[ts] = int(inst.get("instrument_token"))

        self.token_to_symbol = token_to_symbol
        self.symbol_to_token = symbol_to_token
        self.symbol_to_name = symbol_to_name
        self.fno_symbols = fno_set | {s.upper() for s in self.fno_override}
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
        try:
            now = datetime.now(ZoneInfo("Asia/Kolkata"))
            candles = kite.historical_data(
                token,
                now - timedelta(days=10),
                now,
                "day",
            )
            prior_close = None
            for candle in candles:
                candle_dt = candle.get("date")
                candle_date = candle_dt.date() if hasattr(candle_dt, "date") else candle_dt
                close = candle.get("close")
                if close in (None, 0):
                    continue
                if candle_date < now.date():
                    prior_close = close
            return prior_close
        except Exception:
            return None

    def _fetch_last_two_day_candles(self, token, from_date, to_date):
        if not self.kite or not token:
            return []
        try:
            candles = self.kite.historical_data(token, from_date, to_date, "day")
        except Exception as exc:
            self.last_error = str(exc)
            return []
        valid = []
        for candle in candles:
            close = candle.get("close")
            if close in (None, 0):
                continue
            valid.append(candle)
        return valid[-2:] if len(valid) >= 2 else valid

    def _build_stock_row_from_candles(self, symbol, candles):
        if len(candles) < 2:
            return None, None
        prev_close = candles[-2].get("close")
        latest_close = candles[-1].get("close")
        row = self._build_stock_row(symbol, latest_close, prev_close)
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
            ohlc = payload.get("ohlc") or {}
            close = ohlc.get("close")
            if close not in (None, 0):
                self.rest_prev_close[symbol] = close
            base_close = close if close not in (None, 0) else self.rest_prev_close.get(symbol)
            row = self._build_stock_row(symbol, last_price, base_close)
            if row:
                updated[symbol] = row
            elif last_price not in (None, 0):
                missing_history.append((symbol, last_price))

        for symbol, last_price in missing_history:
            close = self._fetch_prev_close_from_history(self.kite, symbol)
            if close in (None, 0):
                continue
            self.rest_prev_close[symbol] = close
            row = self._build_stock_row(symbol, last_price, close)
            if row:
                updated[symbol] = row

        if updated:
            with self.lock:
                self.latest.update(updated)
                self.last_update = self._utc_now()
            self.last_snapshot_source = "api"

        self.last_rest_refresh_ts = now_ts
        return bool(updated)

    def _refresh_sector_snapshot(self, force=False):
        if not self.kite or not self.sector_tokens:
            return
        refresh_interval = 10 if self._is_market_open() else 120
        now = time.time()
        if not force and now - self.last_sector_quote_ts < refresh_interval:
            return
        prev, latest = self._fetch_sector_quote(self.kite, list(self.sector_tokens.keys()))
        with self.lock:
            if prev:
                self.sector_prev_close.update(prev)
            if latest:
                self.sector_latest.update(latest)
        self.last_sector_quote_ts = now

    def _refresh_closed_market_snapshot(self, force=False):
        if not self.kite or not self.symbol_to_token:
            return False

        now = datetime.now(ZoneInfo("Asia/Kolkata"))
        now_ts = time.time()
        if not force and now_ts - self.last_closed_refresh_ts < 3600 and self.latest and self.sector_latest:
            return True

        from_date = now - timedelta(days=15)
        stock_rows = {}
        sector_rows = {}
        latest_dates = []

        tracked_symbols = [
            symbol for symbol in self.symbol_to_token.keys()
            if self._is_tracked_symbol(symbol)
        ]
        for symbol in tracked_symbols:
            candles = self._fetch_last_two_day_candles(self.symbol_to_token.get(symbol), from_date, now)
            row, latest_dt = self._build_stock_row_from_candles(symbol, candles)
            if not row:
                continue
            stock_rows[symbol] = row
            if latest_dt:
                latest_dates.append(latest_dt)

        for sector_name, token in self.sector_tokens.items():
            candles = self._fetch_last_two_day_candles(token, from_date, now)
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
            return True
        return False

    def start(self, api_key, access_token, sector_names):
        try:
            if self.ticker:
                try:
                    self.ticker.close()
                except Exception:
                    pass
                self.ticker = None
            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)
            self.kite = kite
            self.build_universe(kite, sector_names)
            if self._is_market_open():
                self._refresh_rest_snapshot(force=True)
            else:
                self._refresh_closed_market_snapshot(force=True)

            tokens = list(self.equity_tokens)
            sector_token_list = list(self.sector_tokens.values())
            all_tokens = tokens + sector_token_list
            if len(all_tokens) > 3000:
                max_eq = max(0, 3000 - len(sector_token_list))
                all_tokens = tokens[:max_eq] + sector_token_list
            print(f"[engine] equity_tokens={len(tokens)} sector_tokens={len(sector_token_list)} subscribed={len(all_tokens)}")
            if sector_token_list:
                print(f"[engine] sector tokens: {sorted(self.sector_tokens.keys())}")
            else:
                print("[engine] WARNING: no sector tokens found for provided sector list")

            self.ticker = KiteTicker(api_key, access_token)
            self.ticker.on_connect = lambda ws, resp: self._on_connect(ws, resp, all_tokens)
            self.ticker.on_ticks = self._on_ticks
            self.ticker.on_close = self._on_close
            self.ticker.on_error = self._on_error

            self.ticker.connect(threaded=True)
            self.connected = True
            self.last_error = None
        except Exception as exc:
            self.last_error = str(exc)
            self.connected = False

    def _on_connect(self, ws, response, tokens):
        try:
            ws.subscribe(tokens)
            ws.set_mode(ws.MODE_FULL, tokens)
        except Exception as exc:
            self.last_error = str(exc)

    def _on_close(self, ws, code, reason):
        self.connected = False
        self.last_error = f"WebSocket closed: {code} {reason}"

    def _on_error(self, ws, code, reason):
        self.connected = False
        self.last_error = f"WebSocket error: {code} {reason}"

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
                    base_close = close if close not in (None, 0) else self.rest_prev_close.get(symbol)
                    row = self._build_stock_row(symbol, last_price, base_close)
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
        self.last_snapshot_source = "websocket"

    def _build_snapshot(self, market_open):
        with self.lock:
            movers = list(self.latest.values())
            if self.nifty500_set:
                movers = [m for m in movers if m["symbol"].upper() in self.nifty500_set]
            gainers = sorted([m for m in movers if m["change"] > 0], key=lambda x: x["change"], reverse=True)[:20]
            losers = sorted([m for m in movers if m["change"] < 0], key=lambda x: x["change"])[:20]
            sectors = list(self.sector_latest.values())
            sector_gainers = sorted([s for s in sectors if s["change"] > 0], key=lambda x: x["change"], reverse=True)[:10]
            sector_losers = sorted([s for s in sectors if s["change"] < 0], key=lambda x: x["change"])[:10]
            return {
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

    def get_snapshot(self):
        if self.demo_mode and self.demo_snapshot:
            snap = dict(self.demo_snapshot)
            snap["updated_at"] = self._utc_now()
            snap["connected"] = False
            snap["error"] = None
            return snap

        market_open = self._is_market_open()
        if self.kite:
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
        has_data = any(snapshot.get(key) for key in ("gainers", "losers", "sector_gainers", "sector_losers"))
        if has_data:
            self._save_snapshot(snapshot)
            return snapshot

        cached = self._cached_snapshot()
        if cached:
            cached["connected"] = self.connected
            cached["error"] = self.last_error
            cached["market_open"] = market_open
            cached["snapshot_source"] = "cache"
            return cached
        return snapshot

    def _get_latest_rows_for_symbols(self, symbols):
        if not self.kite:
            return []

        if not self._is_market_open():
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
            ohlc = payload.get("ohlc") or {}
            close = ohlc.get("close")
            if close not in (None, 0):
                self.rest_prev_close[symbol] = close
            base_close = close if close not in (None, 0) else self.rest_prev_close.get(symbol)
            row = self._build_stock_row(symbol, last_price, base_close)
            if row:
                rows.append(row)
            elif last_price not in (None, 0):
                missing_history.append((symbol, last_price))

        for symbol, last_price in missing_history:
            close = self._fetch_prev_close_from_history(self.kite, symbol)
            if close in (None, 0):
                continue
            self.rest_prev_close[symbol] = close
            row = self._build_stock_row(symbol, last_price, close)
            if row:
                rows.append(row)

        if rows:
            with self.lock:
                for row in rows:
                    self.latest[row["symbol"]] = row
                self.last_update = self._utc_now()
            self.last_snapshot_source = "api"
        return rows

    def get_sector_breakdown(self, sector_name):
        sector = (sector_name or "").strip()
        if not sector:
            return {"sector": "", "stocks": [], "updated_at": self.last_update, "market_open": self._is_market_open()}

        self._refresh_sector_memberships(force=not bool(self.sector_members))
        symbols = self.sector_members.get(sector, [])
        rows = self._get_latest_rows_for_symbols(symbols)
        ranked = sorted(rows, key=lambda row: row["change"], reverse=True)
        for index, row in enumerate(ranked, start=1):
            row["rank"] = index
        return {
            "sector": sector,
            "stocks": ranked,
            "updated_at": self.last_update,
            "market_open": self._is_market_open(),
            "snapshot_source": self.last_snapshot_source,
            "constituent_count": len(ranked),
        }

    def _is_market_open(self):
        try:
            now = datetime.now(ZoneInfo("Asia/Kolkata"))
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
