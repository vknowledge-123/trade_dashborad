import threading
import time
import re
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

from kiteconnect import KiteConnect, KiteTicker

from app.config import KITE_ACCESS_TOKEN_KEY, KITE_TOKEN_UPDATED_KEY


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
        self.fno_symbols = set()
        self.fno_override = set()
        self.nifty500_set = set()
        self.equity_tokens = []
        self.sector_tokens = {}
        self.sector_prev_close = {}
        self.latest = {}
        self.sector_latest = {}
        self.connected = False
        self.last_error = None
        self.last_update = None
        self.demo_mode = False
        self.demo_snapshot = None
        self.last_sector_quote_ts = 0

    def _extract_underlying(self, tradingsymbol):
        match = re.match(r"^[A-Z]+", tradingsymbol)
        return match.group(0) if match else tradingsymbol

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
                if not self.nifty500_set or symbol.upper() in self.nifty500_set:
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
        self.equity_tokens = equity_tokens
        prev_close, latest = self._fetch_sector_quote(kite, list(index_tokens.keys()))
        self.sector_prev_close = prev_close
        # Seed sector_latest so UI shows immediately even before ticks
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
                if last_price not in (None, 0) and close not in (None, 0):
                    change = (last_price - close) / close * 100
                    latest[name] = {
                        "sector": name,
                        "price": round(last_price, 2),
                        "change": round(change, 2),
                    }
            return prev, latest
        except Exception:
            return {}, {}

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
                if not token or last_price is None or close in (None, 0):
                    continue

                if token in self.token_to_symbol:
                    symbol = self.token_to_symbol[token]
                    change = (last_price - close) / close * 100
                    self.latest[symbol] = {
                        "symbol": symbol,
                        "price": round(last_price, 2),
                        "change": round(change, 2),
                        "is_fno": symbol.upper() in self.fno_symbols or self.symbol_to_name.get(symbol, "").upper() in self.fno_symbols,
                    }
                else:
                    for name, sector_token in self.sector_tokens.items():
                        if token == sector_token:
                            # Indices sometimes miss OHLC close; still show price
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
            self.last_update = datetime.utcnow().isoformat(timespec="seconds")

    def get_snapshot(self):
        if self.demo_mode and self.demo_snapshot:
            snap = dict(self.demo_snapshot)
            snap["updated_at"] = datetime.utcnow().isoformat(timespec="seconds")
            snap["connected"] = False
            snap["error"] = None
            return snap
        # Refresh sector quotes every 10 seconds to keep sector panel live
        if self.kite and self.sector_tokens:
            now = time.time()
            if now - self.last_sector_quote_ts >= 10:
                prev, latest = self._fetch_sector_quote(self.kite, list(self.sector_tokens.keys()))
                with self.lock:
                    if prev:
                        self.sector_prev_close.update(prev)
                    if latest:
                        self.sector_latest.update(latest)
                self.last_sector_quote_ts = now
        with self.lock:
            movers = list(self.latest.values())
            if self.nifty500_set:
                movers = [m for m in movers if m["symbol"].upper() in self.nifty500_set]
            gainers = sorted([m for m in movers if m["change"] > 0], key=lambda x: x["change"], reverse=True)[:20]
            losers = sorted([m for m in movers if m["change"] < 0], key=lambda x: x["change"])[:20]
            sectors = list(self.sector_latest.values())
            sector_gainers = sorted([s for s in sectors if s["change"] > 0], key=lambda x: x["change"], reverse=True)[:10]
            sector_losers = sorted([s for s in sectors if s["change"] < 0], key=lambda x: x["change"])[:10]
            market_open = self._is_market_open()
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
            }

    def _is_market_open(self):
        try:
            now = datetime.now(ZoneInfo("Asia/Kolkata"))
            if now.weekday() >= 5:
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
            self.redis.set(KITE_TOKEN_UPDATED_KEY, datetime.utcnow().isoformat(timespec="seconds"))
        except Exception:
            return
