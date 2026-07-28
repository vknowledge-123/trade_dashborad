from datetime import datetime, timedelta, timezone
import io
import math
from pathlib import Path
import re
import secrets
import threading
from urllib.parse import parse_qs, urlparse

import pyotp
import qrcode
from PIL import Image, UnidentifiedImageError

from fastapi import Body, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
import redis
from kiteconnect import KiteConnect

from app.middleware import BlockLoggedInUserFromAdminMiddleware, SecurityHeadersMiddleware
from app.db import (
    init_db,
    create_user,
    get_user_by_email,
    get_user_by_id,
    get_admin_user,
    update_user_password_hash,
    record_user_login,
    get_recent_users,
    set_admin_totp,
    log_admin_login,
    get_admin_login_audit,
    get_inquiries,
    update_inquiry_status,
    save_kite_credentials,
    get_kite_credentials,
    save_dhan_credentials,
    get_dhan_credentials,
    set_active_broker,
    get_active_broker,
    create_inquiry,
    get_course_settings,
    update_course_settings,
    update_free_course_settings,
    update_course_payment_qr,
    add_free_course_class,
    delete_free_course_class,
    get_free_course_classes,
    add_academy_video,
    delete_academy_video,
    get_academy_videos,
    create_academy_license,
    get_recent_academy_licenses,
    get_active_license_for_user,
    activate_academy_license,
)
from app.config import (
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
    REDIS_PASSWORD,
    SESSION_SECRET_KEY,
    SESSION_HTTPS_ONLY,
    SESSION_SAMESITE,
    ADMIN_IP_ALLOWLIST,
    HCAPTCHA_SITE_KEY,
    HCAPTCHA_SECRET,
)
from app.kite_engine import IST, MarketEngine, NSE_TRADING_HOLIDAYS
from app.security import hash_password, verify_password, should_upgrade_password_hash

app = FastAPI()
# Serve local static assets.
app.mount("/static", StaticFiles(directory="static"), name="static")
# Middleware order matters: SessionMiddleware must wrap anything that reads sessions.
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(BlockLoggedInUserFromAdminMiddleware)
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET_KEY,
    max_age=60 * 60 * 24 * 30,
    https_only=SESSION_HTTPS_ONLY,
    same_site=SESSION_SAMESITE,
)

templates = Jinja2Templates(directory="templates")

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD)
premarket_scheduler_started = False
premarket_scheduler_lock = threading.Lock()
PREMARKET_CACHE_HOUR = 8
PREMARKET_CACHE_MINUTE = 45

SECTOR_INDICES = [
    "NIFTY METAL",
    "NIFTY INDIA MFG",
    "NIFTY FINSEREXBNK",
    "NIFTY INFRA",
    "NIFTY MS FIN SERV",
    "NIFTY HEALTHCARE",
    "NIFTY MIDSML HLTH",
    "NIFTY PSU BANK",
    "NIFTY CONSR DURBL",
    "NIFTY FMCG",
    "NIFTY PVT BANK",
    "NIFTY ENERGY",
    "NIFTY IT",
    "NIFTY CPSE",
    "NIFTY MS IT TELCM",
    "NIFTY IND DEFENCE",
    "NIFTY AUTO",
    "NIFTY BANK",
    "NIFTY MEDIA",
    "NIFTY IND DIGITAL",
    "NIFTY PHARMA",
    "NIFTY IND TOURISM",
    "NIFTY CAPITAL MKT",
    "NIFTY OIL AND GAS",
]


SIMULATION_FNO_STOCKS = {
    "360ONE", "ABB", "APLAPOLLO", "AUBANK", "ADANIENSOL", "ADANIENT", "ADANIGREEN",
    "ADANIPORTS", "ATGL", "ABCAPITAL", "ABFRL", "ALKEM", "AMBER", "AMBUJACEM",
    "ANGELONE", "APOLLOHOSP", "ASHOKLEY", "ASIANPAINT", "ASTRAL", "AUROPHARMA",
    "DMART", "AXISBANK", "BSE", "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV",
    "BANDHANBNK", "BANKBARODA", "BANKINDIA", "BDL", "BEL", "BHARATFORG", "BHEL",
    "BPCL", "BHARTIARTL", "BIOCON", "BLUESTARCO", "BOSCHLTD", "BRITANNIA", "CESC",
    "CGPOWER", "CANBK", "CDSL", "CHOLAFIN", "CIPLA", "COALINDIA", "COFORGE", "COLPAL",
    "CAMS", "CONCOR", "CROMPTON", "CUMMINSIND", "CYIENT", "DLF", "DABUR", "DALBHARAT",
    "DELHIVERY", "DIVISLAB", "DIXON", "DRREDDY", "ETERNAL", "EICHERMOT", "EXIDEIND",
    "NYKAA", "FORTIS", "GAIL", "GMRAIRPORT", "GLENMARK", "GODREJCP", "GODREJPROP",
    "GRANULES", "GRASIM", "HCLTECH", "HDFCAMC", "HDFCBANK", "HDFCLIFE", "HFCL",
    "HAVELLS", "HEROMOTOCO", "HINDALCO", "HAL", "HINDPETRO", "HINDUNILVR", "HINDZINC",
    "HUDCO", "ICICIBANK", "ICICIGI", "ICICIPRULI", "IDFCFIRSTB", "IIFL", "IRB", "ITC",
    "INDIANB", "IEX", "IOC", "IRCTC", "IRFC", "IREDA", "IGL", "INDUSTOWER", "INDUSINDBK",
    "NAUKRI", "INFY", "INOXWIND", "INDIGO", "JSWENERGY", "JSWSTEEL", "JSL", "JINDALSTEL",
    "JIOFIN", "JUBLFOOD", "KEI", "KPITTECH", "KALYANKJIL", "KAYNES", "KFINTECH", "KOTAKBANK",
    "LTF", "LICHSGFIN", "LTIM", "LT", "LAURUSLABS", "LICI", "LODHA", "LUPIN", "M&M",
    "MANAPPURAM", "MANKIND", "MARICO", "MARUTI", "MFSL", "MAXHEALTH", "MAZDOCK", "MPHASIS",
    "MCX", "MUTHOOTFIN", "NBCC", "NCC", "NHPC", "NMDC", "NTPC", "NATIONALUM", "NESTLEIND",
    "NUVAMA", "OBEROIRLTY", "ONGC", "OIL", "PAYTM", "OFSS", "POLICYBZR", "PGEL", "PIIND",
    "PNBHOUSING", "PAGEIND", "PATANJALI", "PERSISTENT", "PETRONET", "PIDILITIND", "PPLPHARMA",
    "POLYCAB", "POONAWALLA", "PFC", "POWERGRID", "PRESTIGE", "PNB", "RBLBANK", "RECLTD",
    "RVNL", "RELIANCE", "SBICARD", "SBILIFE", "SHREECEM", "SJVN", "SRF", "MOTHERSON",
    "SHRIRAMFIN", "SIEMENS", "SOLARINDS", "SONACOMS", "SBIN", "SAIL", "SUNPHARMA",
    "SUPREMEIND", "SUZLON", "SYNGENE", "TATACONSUM", "TITAGARH", "TVSMOTOR", "TATACHEM",
    "TCS", "TATAELXSI", "TATAMOTORS", "TATAPOWER", "TATASTEEL", "TATATECH", "TECHM",
    "FEDERALBNK", "INDHOTEL", "PHOENIXLTD", "TITAN", "TORNTPHARM", "TORNTPOWER", "TRENT",
    "TIINDIA", "UNOMINDA", "UPL", "ULTRACEMCO", "UNIONBANK", "UNITDSPR", "VBL", "VEDL",
    "IDEA", "VOLTAS", "WIPRO", "YESBANK", "ZYDUSLIFE"
}

NIFTY_500_STOCKS = {
    "HDFCBANK", "BSE", "ICICIBANK", "LT", "RELIANCE", "SBIN",
    "INDIGO", "GALLANTT", "INFY", "SHRIRAMFIN", "BHARTIARTL", "AXISBANK",
    "ASHOKLEY", "TITAN", "ADANIGREEN", "TMCV", "ETERNAL", "MARUTI", "M&M",
    "BAJFINANCE", "ONGC", "TCS", "ADANIPORTS", "VEDL", "ADANIPOWER", "MCX",
    "KOTAKBANK", "DIXON", "GROWW", "HINDPETRO", "COALINDIA", "ATGL",
    "ADANIENT", "BLS", "NATIONALUM", "DLF", "FORCEMOT", "OLAELEC", "BPCL",
    "HINDALCO", "BEL", "SUNPHARMA", "GRSE", "ULTRACEMCO", "HINDUNILVR",
    "OIL", "TATASTEEL", "CHOLAHLDNG", "TMPV", "NTPC", "IOC", "EICHERMOT",
    "ITC", "KAYNES", "CHOLAFIN", "JUBLFOOD", "SAIL", "CDSL", "PFC", "TRENT",
    "HCLTECH", "ASIANPAINT", "IDEA", "WAAREEENER", "TVSMOTOR", "HINDCOPPER",
    "POLYCAB", "COFORGE", "HAL", "ITCHOTELS", "MOTHERSON", "CUMMINSIND",
    "JIOFIN", "ADANIENSOL", "HDFCAMC", "PERSISTENT", "MAZDOCK", "AUBANK",
    "PAYTM", "KALYANKJIL", "BANKBARODA", "BAJAJ-AUTO", "MUTHOOTFIN", "BHEL",
    "DMART", "LUPIN", "SUZLON", "SWIGGY", "WIPRO", "ABB", "NETWEB", "PGEL",
    "ANGELONE", "CANBK", "HEROMOTOCO", "BANDHANBNK", "LODHA", "AMBUJACEM",
    "ABREL", "LICI", "BOSCHLTD", "POWERINDIA", "TECHM", "BAJAJFINSV",
    "HINDZINC", "PRESTIGE", "UNIONBANK", "SAMMAANCAP", "AUROPHARMA",
    "ICICIAMC", "KEI", "BHARATFORG", "GODREJCP", "AMBER", "GAIL",
    "MAXHEALTH", "POWERGRID", "360ONE", "HDFCLIFE", "PINELABS", "TATAPOWER",
    "FIRSTCRY", "VBL", "PNB", "APOLLOHOSP", "IDFCFIRSTB", "INDUSINDBK",
    "BLUESTARCO", "CHENNPETRO", "CIPLA", "ZEEL", "BRITANNIA", "IDBI",
    "SOLARINDS", "BANKINDIA", "PIDILITIND", "GVT&D", "LTF", "PHOENIXLTD",
    "PETRONET", "MFSL", "TITAGARH", "RECLTD", "GODREJPROP", "NAM-INDIA",
    "DELHIVERY", "COCHINSHIP", "ABCAPITAL", "YESBANK", "GRASIM", "PATANJALI",
    "INDHOTEL", "RPOWER", "BIOCON", "INDIANB", "JSWSTEEL", "MAHABANK",
    "HYUNDAI", "KARURVYSYA", "NATCOPHARM", "M&MFIN", "FEDERALBNK", "BDL",
    "SBILIFE", "IRFC", "MARICO", "VMM", "CGPOWER", "SRF", "UPL", "LTM",
    "JINDALSTEL", "RVNL", "KFINTECH", "RBLBANK", "VOLTAS", "GODFRYPHLP",
    "NMDC", "NESTLEIND", "MPHASIS", "LAURUSLABS", "TATACONSUM", "ATHERENERG",
    "KPITTECH", "TEJASNET", "DRREDDY", "NAUKRI", "IIFL", "DIVISLAB", "HFCL",
    "MRF", "PREMIERENE", "MANAPPURAM", "FORTIS", "POLICYBZR", "JSWINFRA",
    "TBOTEK", "JKTYRE", "FIVESTAR", "TORNTPHARM", "DATAPATTNS", "AWL",
    "TARIL", "OBEROIRLTY", "SONACOMS", "MANKIND", "APLAPOLLO", "INOXWIND",
    "HSCL", "CREDITACC", "JPPOWER", "IREDA", "GPIL", "HBLENGINE", "WELCORP",
    "MRPL", "TATACAP", "LLOYDSME", "UNOMINDA", "GMDCLTD", "LENSKART",
    "MOTILALOFS", "GESHIP", "OLECTRA", "GLENMARK", "SIEMENS", "INDUSTOWER",
    "APOLLOTYRE", "DALBHARAT", "OFSS", "SCI", "GMRAIRPORT", "LGEINDIA",
    "NBCC", "JSWENERGY", "BELRISE", "RADICO", "JBCHEPHARM", "REDINGTON",
    "ACUTAAS", "CRAFTSMAN", "ICICIGI", "ANANDRATHI", "ANANTRAJ", "ZYDUSWELL",
    "ENRIN", "WOCKPHARMA", "SAGILITY", "CROMPTON", "SUPREMEIND", "THERMAX",
    "SBICARD", "IRCTC", "NTPCGREEN", "HEG", "HUDCO", "UTIAMC", "NHPC",
    "ASTRAL", "LICHSGFIN", "CUB", "ENGINERSIN", "NUVAMA", "PCBL",
    "PNBHOUSING", "TATAELXSI", "KIRLOSENG", "POONAWALLA", "PIIND", "HDBFS",
    "NAVINFLUOR", "BAJAJHLDNG", "COLPAL", "UNITDSPR", "NYKAA", "NEULANDLAB",
    "CPPLUS", "IRCON", "HAVELLS", "CAMS", "BAJAJHFL", "GRAPHITE", "EXIDEIND",
    "EMMVEE", "FACT", "SYRMA", "ALKEM", "APTUS", "LATENTVIEW", "APARINDS",
    "BRIGADE", "TIINDIA", "KEC", "MGL", "IFCI", "SHREECEM", "JWL",
    "LALPATHLAB", "COROMANDEL", "SYNGENE", "BALKRISIND", "DABUR", "CONCOR",
    "IEX", "J&KBANK", "PARADEEP", "GODIGIT", "DEEPAKFERT", "JBMA", "ZYDUSLIFE",
    "ANTHEM", "CARTRADE", "CHOICEIN", "JINDALSAW", "PIRAMALFIN", "PAGEIND",
    "PVRINOX", "GRANULES", "AEGISLOG", "KAJARIACER", "LEMONTREE", "TECHNOE",
    "BHARTIHEXA", "LTFOODS", "JMFINANCIL", "MEESHO", "CCL", "FSL",
    "SCHAEFFLER", "CEMPRO", "3MINDIA", "TATACHEM", "ENDURANCE", "NH",
    "TORNTPOWER", "ESCORTS", "GRAVITA", "KPIL", "LINDEINDIA", "WHIRLPOOL",
    "CHAMBLFERT", "ABFRL", "DEVYANI", "ZFCVINDIA", "NEWGEN", "ACC",
    "RAMCOCEM", "IGL", "IPCALAB", "SHYAMMETL", "ZENTEC", "RAILTEL",
    "TATAINVEST", "HEXT", "JAINREC", "ABSLAMC", "BALRAMCHIN", "BSOFT", "BEML",
    "HONAUT", "NCC", "SJVN", "ARE&M", "CEATLTD", "JSWCEMENT", "URBANCO",
    "JSL", "RRKABEL", "NLCINDIA", "ICICIPRULI", "PPLPHARMA", "TATATECH",
    "SAILIFE", "SUNDARMFIN", "ZENSARTECH", "CGCL", "SWANCORP", "LTTS",
    "RAINBOW", "BBTC", "ONESOURCE", "CASTROLIND", "MSUMI", "JYOTICNC",
    "ACMESOLAR", "UCOBANK", "CRISIL", "PWL", "TTML", "CESC", "AEGISVOPAK",
    "CENTRALBK", "SIGNATURE", "AFFLE", "THELEELA", "IKS", "CANHLIFE", "IGIL",
    "GSPL", "AIAENG", "IOB", "TRITURBINE", "ELECON", "CHALET", "HOMEFIRST",
    "CYIENT", "CAPLIPOINT", "EIHOTEL", "TENNIND", "AAVAS", "AARTIIND",
    "CLEAN", "PTCIL", "COHANCE", "FINCABLES", "TATACOMM", "BERGEPAINT",
    "KIMS", "DEEPAKNTR", "INDIAMART", "HONASA", "ITI", "IRB", "AJANTPHARM",
    "SOBHA", "INTELLECT", "SAREGAMA", "EMAMILTD", "AIIL", "POLYMED",
    "NSLNISP", "SAPPHIRE", "TIMKEN", "CONCORDBIO", "EMCURE", "GICRE", "ABDL",
    "SCHNEIDER", "JKCEMENT", "ASTERDM", "ECLERX", "CARBORUNIV", "ABBOTINDIA",
    "GILLETTE", "ACE", "GABRIEL", "SARDAEN", "VTL", "WELSPUNLIV",
    "JUBLPHARMA", "EIDPARRY", "RKFORGE", "NAVA", "TRIDENT", "UBL", "BLUEJET",
    "MEDANTA", "GODREJIND", "ANURAS", "AADHARHFC", "KPRMILL", "SONATSOFTW",
    "MMTC", "RITES", "VIJAYA", "TEGA", "USHAMART", "ELGIEQUIP", "BATAINDIA",
    "MINDACORP", "GLAXO", "BAYERCROP", "INDIACEM", "MAPMYINDIA", "SUMICHEM",
    "SBFC", "INDGN", "SUNTV", "NUVOCO", "ASAHIINDIA", "NIACL", "FLUOROCHEM",
    "JUBLINGREA", "STARHEALTH", "CANFINHOME", "BLUEDART", "SPLPETRO", "DOMS",
    "TRAVELFOOD", "BIKAJI", "PFIZER", "RHIM", "ABLBL", "ERIS", "DCMSHRIRAM",
    "ATUL", "GLAND", "AFCONS", "AKZOINDIA", "NIVABUPA"
}


engine = MarketEngine(redis_client)
engine.fno_override = SIMULATION_FNO_STOCKS
engine.nifty500_set = {s.upper() for s in NIFTY_500_STOCKS}
engine.demo_mode = False
GUEST_REGISTER_PROMPT_HOURS = 24
REGISTERED_TRIAL_TRADING_DAYS = 3


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def format_compact_volume(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"

    abs_number = abs(number)
    if abs_number >= 1_000_000_000:
        return f"{number / 1_000_000_000:.2f}B"
    if abs_number >= 1_000_000:
        return f"{number / 1_000_000:.2f}M"
    if abs_number >= 1_000:
        return f"{number / 1_000:.1f}K"
    return str(int(number))


def format_crores(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{number / 10_000_000:.2f} Cr"


def csrf_token(request: Request):
    token = request.session.get("csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        request.session["csrf_token"] = token
    return token


def require_csrf(request: Request, submitted_token: str):
    expected = request.session.get("csrf_token")
    if not expected or not submitted_token or not secrets.compare_digest(expected, submitted_token):
        raise HTTPException(status_code=403, detail="Invalid CSRF token")


def youtube_embed_url(url):
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
    except Exception:
        return raw
    host = parsed.netloc.lower()
    video_id = ""
    if host in {"youtu.be", "www.youtu.be"}:
        video_id = parsed.path.strip("/").split("/")[0]
    elif host in {"youtube.com", "www.youtube.com", "m.youtube.com"}:
        if parsed.path.startswith("/embed/"):
            video_id = parsed.path.strip("/").split("/")[1] if len(parsed.path.strip("/").split("/")) > 1 else ""
        else:
            video_id = parse_qs(parsed.query).get("v", [""])[0]
            if not video_id and parsed.path.startswith("/shorts/"):
                video_id = parsed.path.strip("/").split("/")[1]
    if video_id and re.fullmatch(r"[A-Za-z0-9_-]{6,20}", video_id):
        return f"https://www.youtube.com/embed/{video_id}"
    return ""


def ione_power_rating(row):
    try:
        turnover_crore = float(row.get("opening_turnover") or 0) / 10_000_000
    except (TypeError, ValueError):
        turnover_crore = 0
    try:
        multiplier = float(row.get("opening_volume_sma_multiplier") or 0)
    except (TypeError, ValueError):
        multiplier = 0
    try:
        sector_rank = int(row.get("sector_rank") or 0)
    except (TypeError, ValueError):
        sector_rank = 0

    leading_sector = sector_rank in {1, 2}
    if turnover_crore > 10 and multiplier > 2.5:
        return 5
    if turnover_crore >= 6 and turnover_crore <= 10 and multiplier >= 2.5:
        return 4
    if turnover_crore > 3 and turnover_crore < 4 and multiplier >= 3 and sector_rank in {1, 2}:
        return 4
    if turnover_crore > 2 and turnover_crore < 3 and multiplier > 2:
        return 3
    if leading_sector and turnover_crore > 0.5:
        return 3
    if turnover_crore > 1 and turnover_crore <= 2:
        return 2
    return 1


def ione_power_leading_sector(row):
    try:
        sector_rank = int(row.get("sector_rank") or 0)
    except (TypeError, ValueError):
        sector_rank = 0
    try:
        turnover = float(row.get("opening_turnover") or 0)
    except (TypeError, ValueError):
        turnover = 0
    return sector_rank in {1, 2} and turnover > 5_000_000


def ione_power_break_alert(row, side):
    symbol = str(row.get("symbol") or "").upper()
    if not symbol:
        return {}
    try:
        price = float(row.get("price") or 0)
    except (TypeError, ValueError):
        price = 0
    if price <= 0:
        return {}

    def level_broken(level, comparator):
        try:
            numeric_level = float(level)
        except (TypeError, ValueError):
            return False
        if numeric_level <= 0:
            return False
        return comparator(price, numeric_level)

    if side == "long":
        first_candle_level = row.get("opening_candle_high")
        day_level = row.get("day_high")
        label = "high"
        direction = "breakout"
        broken = any(
            level_broken(level, lambda price_value, level_value: price_value >= level_value)
            for level in (first_candle_level, day_level)
        )
    else:
        first_candle_level = row.get("opening_candle_low")
        day_level = row.get("day_low")
        label = "low"
        direction = "breakdown"
        broken = any(
            level_broken(level, lambda price_value, level_value: price_value <= level_value)
            for level in (first_candle_level, day_level)
        )
    if not broken:
        return {}
    hit_at = row.get("first_hit_at") or row.get("updated_at") or ""
    return {
        "alert_key": f"{symbol}:{side}:{direction}:{hit_at}",
        "alert_text": f"{symbol} {direction} of first candle/day {label}",
    }


def format_ione_hit_time(value):
    if not value:
        return "-"
    try:
        parsed = datetime.fromisoformat(str(value))
        if parsed.tzinfo:
            parsed = parsed.astimezone(IST)
        return parsed.strftime("%I:%M %p")
    except Exception:
        return str(value)


def ione_power_row(row, side="long"):
    symbol = str(row.get("symbol") or "").upper()
    try:
        sort_turnover = float(row.get("opening_turnover") or 0)
    except (TypeError, ValueError):
        sort_turnover = 0
    try:
        sort_multiplier = float(row.get("opening_volume_sma_multiplier") or 0)
    except (TypeError, ValueError):
        sort_multiplier = 0
    try:
        sort_change = abs(float(row.get("change") or 0))
    except (TypeError, ValueError):
        sort_change = 0
    raw_hit_time = str(row.get("first_hit_at") or row.get("updated_at") or "")
    payload = {
        "symbol": symbol,
        "name": row.get("name") or symbol,
        "is_fno": bool(row.get("is_fno")),
        "leading_sector": ione_power_leading_sector(row),
        "leading_sector_label": "Leading Sector" if side == "long" else "Most Fall Sector",
        "change": row.get("change"),
        "sector_rank": row.get("sector_rank"),
        "sector_side": row.get("sector_side"),
        "sector_name": row.get("sector_name") or row.get("sector"),
        "first_hit_time": format_ione_hit_time(raw_hit_time),
        "rating": ione_power_rating(row),
        "chart_url": f"https://www.tradingview.com/chart/?symbol=NSE%3A{symbol}",
        "_sort_turnover": sort_turnover,
        "_sort_multiplier": sort_multiplier,
        "_sort_change": sort_change,
        "_sort_hit_time": raw_hit_time,
    }
    payload.update(ione_power_break_alert(row, side))
    return payload


def ione_power_sort_key(row):
    return (
        row.get("rating") or 0,
        row.get("_sort_turnover") or 0,
        row.get("_sort_multiplier") or 0,
        row.get("_sort_change") or 0,
        row.get("_sort_hit_time") or "",
    )


def strip_ione_power_sort_fields(rows):
    for row in rows:
        for key in ("_sort_turnover", "_sort_multiplier", "_sort_change", "_sort_hit_time"):
            row.pop(key, None)
    return rows


def ione_power_payload(scanner):
    long_rows = [ione_power_row(row, "long") for row in scanner.get("open_low_gainers") or []]
    short_rows = [ione_power_row(row, "short") for row in scanner.get("open_high_losers") or []]
    long_rows.sort(key=ione_power_sort_key, reverse=True)
    short_rows.sort(key=ione_power_sort_key, reverse=True)
    return {
        "ione_power_long": strip_ione_power_sort_fields(long_rows),
        "ione_power_short": strip_ione_power_sort_fields(short_rows),
        "updated_at": scanner.get("updated_at"),
        "market_open": scanner.get("market_open"),
        "error": scanner.get("error"),
    }


def blaster_hit_time(row):
    return format_ione_hit_time(row.get("appearance_time") or row.get("updated_at"))


def blaster_sector_rank_label(row):
    try:
        sector_rank = int(row.get("sector_rank") or 0)
    except (TypeError, ValueError):
        sector_rank = 0
    if sector_rank not in {1, 2}:
        return None
    try:
        move_percent = float(row.get("move_percent") or 0)
    except (TypeError, ValueError):
        move_percent = 0
    prefix = "Leading Sector Rank" if move_percent >= 0 else "Sector Fall Rank"
    return f"{prefix} {sector_rank}"


def blaster_intraday_row(row):
    symbol = str(row.get("symbol") or "").upper()
    try:
        move_percent = float(row.get("move_percent"))
    except (TypeError, ValueError):
        move_percent = None
    return {
        "symbol": symbol,
        "name": row.get("name") or symbol,
        "status": "Blast",
        "move_percent": round(move_percent, 2) if move_percent is not None else None,
        "sector_rank_label": blaster_sector_rank_label(row),
        "hit_time": blaster_hit_time(row),
        "chart_url": f"https://www.tradingview.com/chart/?symbol=NSE%3A{symbol}",
    }


def blaster_intraday_payload(scanner):
    rows = []
    for row in scanner.get("rows") or []:
        try:
            turnover = float(row.get("turnover") or 0)
        except (TypeError, ValueError):
            turnover = 0
        if turnover >= 50_000_000:
            rows.append(row)
    rows.sort(key=lambda item: item.get("appearance_time") or "", reverse=True)
    return {
        "rows": [blaster_intraday_row(row) for row in rows],
        "timeframe": scanner.get("timeframe"),
        "updated_at": scanner.get("updated_at"),
        "market_open": scanner.get("market_open"),
        "error": scanner.get("error"),
    }


def _scanner_row_float(row, key):
    try:
        return float(row.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def admin_blaster_payload(scanner):
    payload = dict(scanner or {})
    rows = []
    for row in payload.get("rows") or []:
        multiplier = _scanner_row_float(row, "volume_sma_multiplier")
        turnover = _scanner_row_float(row, "turnover")
        if multiplier > 10 and turnover > 10_000_000:
            rows.append(row)
    rows.sort(key=lambda item: item.get("appearance_time") or "", reverse=True)
    payload["rows"] = rows
    if not rows and not payload.get("error"):
        payload["error"] = "No strong Blaster matches yet."
    return payload


templates.env.globals["format_compact_volume"] = format_compact_volume
templates.env.globals["format_crores"] = format_crores
templates.env.globals["youtube_embed_url"] = youtube_embed_url
templates.env.globals["csrf_token"] = csrf_token
templates.env.globals["ione_power_rating"] = ione_power_rating
templates.env.globals["format_ione_hit_time"] = format_ione_hit_time


def start_premarket_cache_scheduler():
    global premarket_scheduler_started
    with premarket_scheduler_lock:
        if premarket_scheduler_started:
            return
        premarket_scheduler_started = True

    def run():
        last_ready_date = None
        while True:
            now = datetime.now(IST)
            scheduled_at = now.replace(
                hour=PREMARKET_CACHE_HOUR,
                minute=PREMARKET_CACHE_MINUTE,
                second=0,
                microsecond=0,
            )
            if now >= scheduled_at and last_ready_date != now.date() and engine.kite:
                current_status = engine.get_history_cache_status()
                next_retry_at = current_status.get("next_retry_at")
                retry_ready = True
                if next_retry_at:
                    try:
                        retry_ready = datetime.fromisoformat(next_retry_at) <= now
                    except (TypeError, ValueError):
                        retry_ready = True
                if not current_status.get("is_running") and retry_ready:
                    status = engine.start_daily_market_history_cache(force=False)
                    if status.get("market_open_ready"):
                        last_ready_date = now.date()
            threading.Event().wait(60)

    threading.Thread(target=run, daemon=True).start()


@app.on_event("startup")
def on_startup():
    init_db()
    start_premarket_cache_scheduler()
    active_broker = get_active_broker()
    if active_broker == "dhan":
        dhan_creds = get_dhan_credentials()
        if dhan_creds:
            threading.Thread(
                target=engine.start_dhan,
                args=(dhan_creds["client_id"], dhan_creds["access_token"], SECTOR_INDICES),
                daemon=True,
            ).start()
        return

    creds = get_kite_credentials()
    token = engine.token_from_redis()
    if creds and token:
        threading.Thread(
            target=engine.start,
            args=(creds["api_key"], token, SECTOR_INDICES),
            daemon=True,
        ).start()


# --- Helpers ---

def maybe_upgrade_password_hash(user_row, password: str, verify_result=None):
    if not user_row:
        return
    stored = user_row["password_hash"]
    result = verify_result or verify_password(password, stored)
    if should_upgrade_password_hash(stored, result):
        update_user_password_hash(user_row["id"], hash_password(password))


def password_policy_error(password: str):
    value = password or ""
    if len(value) < 8:
        return "Password must be at least 8 characters."
    if not re.search(r"[A-Za-z]", value) or not re.search(r"\d", value):
        return "Password must include at least one letter and one number."
    return None


def _login_keys(prefix: str, email: str, ip: str):
    safe_email = (email or "").lower()
    safe_ip = ip or "unknown"
    return (
        f"{prefix}:login:fail:{safe_email}:{safe_ip}",
        f"{prefix}:login:lock:{safe_email}:{safe_ip}",
    )


def login_locked(prefix: str, email: str, ip: str):
    try:
        _, lock_key = _login_keys(prefix, email, ip)
        ttl = redis_client.ttl(lock_key)
        return ttl if ttl and ttl > 0 else 0
    except Exception:
        return 0


def login_fail(prefix: str, email: str, ip: str):
    try:
        fail_key, lock_key = _login_keys(prefix, email, ip)
        count = redis_client.incr(fail_key)
        if count == 1:
            redis_client.expire(fail_key, 600)
        if count >= 5:
            redis_client.setex(lock_key, 600, "1")
            redis_client.delete(fail_key)
    except Exception:
        pass


def login_success(prefix: str, email: str, ip: str):
    try:
        fail_key, lock_key = _login_keys(prefix, email, ip)
        redis_client.delete(fail_key)
        redis_client.delete(lock_key)
    except Exception:
        pass


def _admin_login_keys(email: str, ip: str):
    safe_email = (email or "").lower()
    safe_ip = ip or "unknown"
    return (
        f"admin:login:fail:{safe_email}:{safe_ip}",
        f"admin:login:lock:{safe_email}:{safe_ip}",
    )


def admin_login_locked(email: str, ip: str):
    return login_locked("admin", email, ip)


def admin_login_fail(email: str, ip: str):
    login_fail("admin", email, ip)


def admin_login_success(email: str, ip: str):
    login_success("admin", email, ip)


def get_client_ip(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


def is_ip_allowed(ip: str) -> bool:
    if not ADMIN_IP_ALLOWLIST:
        return True
    return ip in ADMIN_IP_ALLOWLIST


def current_user(request: Request):
    user_id = request.session.get("user_id")
    if not user_id:
        return None
    return get_user_by_id(user_id)


def current_admin(request: Request):
    admin_id = request.session.get("admin_id")
    if not admin_id:
        return None
    admin = get_user_by_id(admin_id)
    if not admin or not admin["is_admin"]:
        return None
    return admin


def require_login(request: Request):
    user = current_user(request)
    if not user:
        return None
    return user


def require_admin(request: Request):
    admin = current_admin(request)
    if not admin:
        return None
    return admin


def trial_status(user_row):
    start = datetime.fromisoformat(user_row["trial_start"])
    days = int(user_row["trial_days"])
    now = utcnow()
    if user_row["is_admin"]:
        end = start + timedelta(days=days)
        remaining_seconds = max(0, int((end - now).total_seconds()))
        remaining_days = math.ceil(remaining_seconds / 86400) if remaining_seconds else 0
        return {
            "active": now <= end,
            "end_date": end.strftime("%Y-%m-%d"),
            "remaining_days": remaining_days,
            "total_days": days,
        }

    trial_days = max(days, REGISTERED_TRIAL_TRADING_DAYS)
    start_date = ist_date_from_utc_naive(start)
    now_date = ist_date_from_utc_naive(now)
    end_date = add_trading_days(start_date, trial_days)
    return {
        "active": now_date <= end_date,
        "end_date": end_date.isoformat(),
        "remaining_days": count_trading_days(now_date, end_date) if now_date <= end_date else 0,
        "total_days": trial_days,
    }


def ist_date_from_utc_naive(moment):
    return moment.replace(tzinfo=timezone.utc).astimezone(IST).date()


def is_trial_trading_day(session_date):
    return session_date.weekday() < 5 and session_date.isoformat() not in NSE_TRADING_HOLIDAYS


def add_trading_days(start_date, trading_days):
    current = start_date
    counted = 0
    while True:
        if is_trial_trading_day(current):
            counted += 1
            if counted >= trading_days:
                return current
        current += timedelta(days=1)


def count_trading_days(start_date, end_date):
    current = start_date
    count = 0
    while current <= end_date:
        if is_trial_trading_day(current):
            count += 1
        current += timedelta(days=1)
    return count


def guest_dashboard_status(request: Request):
    now = utcnow()
    raw_started = request.session.get("guest_dashboard_started_at")
    started_at = None
    if raw_started:
        try:
            started_at = datetime.fromisoformat(raw_started)
        except Exception:
            started_at = None
    if not started_at:
        started_at = now
        request.session["guest_dashboard_started_at"] = started_at.isoformat(timespec="seconds")

    register_prompt_at = started_at + timedelta(hours=GUEST_REGISTER_PROMPT_HOURS)
    register_remaining_seconds = max(0, int((register_prompt_at - now).total_seconds()))
    hours_left = register_remaining_seconds // 3600
    minutes_left = max(1, register_remaining_seconds // 60) if register_remaining_seconds else 0

    if now >= register_prompt_at:
        stage = "register"
        remaining_text = "Registration required"
    elif hours_left >= 1:
        stage = "fresh"
        remaining_text = f"{hours_left}h before registration prompt"
    else:
        stage = "fresh"
        remaining_text = f"{minutes_left}m before registration prompt"

    return {
        "started_at": started_at.isoformat(timespec="seconds"),
        "register_prompt_at": register_prompt_at.isoformat(timespec="seconds"),
        "stage": stage,
        "remaining_text": remaining_text,
    }


def should_show_free_course_prompt(request: Request, user_row=None, admin_row=None):
    if admin_row:
        return False
    if user_row and get_active_license_for_user(user_row["id"]):
        return False
    today = utcnow().date().isoformat()
    return request.session.get("free_course_prompt_dismissed_on") != today


def start_engine_in_background(api_key: str, access_token: str):
    thread = threading.Thread(
        target=engine.start,
        args=(api_key, access_token, SECTOR_INDICES),
        daemon=True,
    )
    thread.start()


def start_dhan_engine_in_background(client_id: str, access_token: str):
    thread = threading.Thread(
        target=engine.start_dhan,
        args=(client_id, access_token, SECTOR_INDICES),
        daemon=True,
    )
    thread.start()


# --- Public Routes ---

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return RedirectResponse(url="/dashboard", status_code=302)


@app.get("/register", response_class=HTMLResponse)
def register_get(request: Request):
    return templates.TemplateResponse(
        request,
        "register.html",
        {
            "error": None,
            "user": None,
            "admin": None,
            "guest_expired": request.query_params.get("guest") == "expired",
        },
    )


@app.post("/register", response_class=HTMLResponse)
def register_post(
    request: Request,
    full_name: str = Form(...),
    email: str = Form(...),
    phone: str = Form(...),
    password: str = Form(...),
    csrf_token: str = Form(""),
):
    require_csrf(request, csrf_token)
    password_error = password_policy_error(password)
    if password_error:
        return templates.TemplateResponse(
            request,
            "register.html",
            {"error": password_error, "user": None, "admin": None, "guest_expired": False},
        )
    existing = get_user_by_email(email)
    if existing:
        return templates.TemplateResponse(
            request,
            "register.html",
            {"error": "Email already registered.", "user": None, "admin": None, "guest_expired": False},
        )

    password_hash = hash_password(password)
    user_id = create_user(full_name, email, phone, password_hash, trial_days=REGISTERED_TRIAL_TRADING_DAYS)
    request.session["user_id"] = user_id
    return RedirectResponse(url="/dashboard", status_code=302)


@app.get("/login", response_class=HTMLResponse)
def login_get(request: Request):
    return templates.TemplateResponse(request, "login.html", {"error": None, "user": None, "admin": None})


@app.post("/login", response_class=HTMLResponse)
def login_post(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    csrf_token: str = Form(""),
):
    require_csrf(request, csrf_token)
    ip = get_client_ip(request)
    locked_ttl = login_locked("user", email, ip)
    if locked_ttl:
        minutes = max(1, locked_ttl // 60)
        return templates.TemplateResponse(
            request,
            "login.html",
            {"error": f"Too many attempts. Try again in {minutes} minutes.", "user": None, "admin": None},
        )
    user = get_user_by_email(email)
    if not user:
        login_fail("user", email, ip)
        return templates.TemplateResponse(request, "login.html", {"error": "Invalid email or password.", "user": None, "admin": None})

    verify_result = verify_password(password, user["password_hash"])
    if not verify_result.ok:
        login_fail("user", email, ip)
        return templates.TemplateResponse(request, "login.html", {"error": "Invalid email or password.", "user": None, "admin": None})

    login_success("user", email, ip)
    maybe_upgrade_password_hash(user, password, verify_result)
    user_agent = request.headers.get("user-agent", "-")
    record_user_login(user["id"], ip, user_agent)
    request.session["user_id"] = user["id"]
    return RedirectResponse(url="/dashboard", status_code=302)


@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/dashboard", status_code=302)


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    user = current_user(request)
    admin = current_admin(request)
    guest_trial = None
    if not user and not admin:
        guest_trial = guest_dashboard_status(request)
        if guest_trial["stage"] == "register":
            return RedirectResponse(url="/register?guest=expired", status_code=302)
    snapshot = engine.get_snapshot()
    trial = trial_status(user) if user else None
    course_settings = get_course_settings()

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "user": user,
            "admin": admin,
            "trial": trial,
            "guest_trial": guest_trial,
            "snapshot": snapshot,
            "public_mode": True if not user and not admin else False,
            "course_settings": course_settings,
            "show_free_course_prompt": False,
        },
    )


@app.get("/free-course", response_class=HTMLResponse)
def free_course(request: Request):
    raise HTTPException(status_code=404, detail="Free course page removed")


@app.post("/free-course/dismiss")
def free_course_dismiss(request: Request):
    request.session["free_course_prompt_dismissed_on"] = utcnow().date().isoformat()
    return JSONResponse({"ok": True})


@app.get("/relative-rotation", response_class=HTMLResponse)
def relative_rotation(request: Request):
    user = current_user(request)
    admin = current_admin(request)
    history_cache_status = engine.get_history_cache_status() if admin else None
    return templates.TemplateResponse(
        request,
        "relative_rotation.html",
        {
            "title": "Relative Rotation Graph",
            "user": user,
            "admin": admin,
            "public_mode": True if not user and not admin else False,
            "rrg_payload": engine.get_relative_rotation_graph(cached_only=True, auto_start=False),
            "history_cache_status": history_cache_status,
        },
    )


@app.get("/pdh-pdl-scanner", response_class=HTMLResponse)
def pdh_pdl_scanner(request: Request):
    raise HTTPException(status_code=404, detail="PDH/PDL scanner removed")


@app.get("/swing-scanner", response_class=HTMLResponse)
def swing_scanner(request: Request):
    raise HTTPException(status_code=404, detail="Swing scanner removed")


@app.get("/acceleration-scanner", response_class=HTMLResponse)
def acceleration_scanner(request: Request):
    user = current_user(request)
    admin = current_admin(request)
    scanner = engine.get_acceleration_scanner(timeframe=1, min_gain=0.5)
    if admin:
        scanner = admin_blaster_payload(scanner)
    return templates.TemplateResponse(
        request,
        "acceleration_scanner.html",
        {
            "title": "Acceleration Scanner" if admin else "Blaster Intraday Scan",
            "user": user,
            "admin": admin,
            "public_mode": True if not user and not admin else False,
            "scanner": scanner,
            "blaster_scanner": blaster_intraday_payload(scanner),
        },
    )


@app.get("/gap-reversal-scanner", response_class=HTMLResponse)
def gap_reversal_scanner(request: Request):
    admin = current_admin(request)
    if not admin:
        raise HTTPException(status_code=404, detail="Scanner not found")
    scanner = engine.get_gap_reversal_scanner()
    return templates.TemplateResponse(
        request,
        "gap_reversal_scanner.html",
        {
            "title": "Gap Reversal",
            "user": None,
            "admin": admin,
            "public_mode": False,
            "scanner": scanner,
        },
    )


@app.get("/open-extreme-scanner", response_class=HTMLResponse)
def open_extreme_scanner(request: Request):
    user = current_user(request)
    admin = current_admin(request)
    scanner = engine.get_open_extreme_scanner()
    return templates.TemplateResponse(
        request,
        "open_extreme_scanner.html",
        {
            "title": "Power",
            "user": user,
            "admin": admin,
            "public_mode": True if not user and not admin else False,
            "scanner": scanner,
            "ione_scanner": ione_power_payload(scanner),
        },
    )


@app.get("/api/market-snapshot")
def market_snapshot(request: Request):
    return JSONResponse(
        engine.get_snapshot(),
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/api/relative-rotation")
def relative_rotation_data(request: Request):
    return JSONResponse(
        engine.get_relative_rotation_graph(),
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/api/pdh-pdl-scanner")
def pdh_pdl_scanner_data(
    request: Request,
    level: str = "all",
    side: str = "all",
    min_pct: float = None,
    max_pct: float = None,
):
    raise HTTPException(status_code=404, detail="PDH/PDL scanner removed")


@app.get("/api/swing-scanner")
def swing_scanner_data(
    request: Request,
    side: str = "long",
    min_score: float = 0,
    refresh: bool = False,
):
    raise HTTPException(status_code=404, detail="Swing scanner removed")


@app.get("/api/swing-backtest")
def swing_backtest_data(
    request: Request,
    symbol: str,
    sessions: int = 260,
    holding_days: int = 20,
):
    raise HTTPException(status_code=404, detail="Swing scanner removed")


@app.get("/api/acceleration-scanner")
def acceleration_scanner_data(
    request: Request,
    timeframe: int = 1,
    min_gain: float = 0.5,
):
    scanner = engine.get_acceleration_scanner(timeframe=timeframe, min_gain=min_gain)
    if not current_admin(request):
        return JSONResponse(
            blaster_intraday_payload(scanner),
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )
    return JSONResponse(
        admin_blaster_payload(scanner),
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/api/admin/gap-reversal-scanner")
def gap_reversal_scanner_data(request: Request):
    if not current_admin(request):
        return JSONResponse({"ok": False, "error": "Admin authentication required."}, status_code=403)
    return JSONResponse(
        engine.get_gap_reversal_scanner(),
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/api/open-extreme-scanner")
def open_extreme_scanner_data(request: Request):
    scanner = engine.get_open_extreme_scanner()
    if not current_admin(request):
        return JSONResponse(
            ione_power_payload(scanner),
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )
    return JSONResponse(
        scanner,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.post("/api/admin/acceleration-order")
def admin_acceleration_order(
    request: Request,
    payload: dict = Body(...),
):
    admin = require_admin(request)
    if not admin:
        return JSONResponse({"ok": False, "error": "Admin authentication required."}, status_code=403)
    try:
        result = engine.place_acceleration_market_order(
            symbol=payload.get("symbol"),
            side=payload.get("side"),
            per_trade_capital=payload.get("capital", 10000),
            client_price=payload.get("price"),
        )
    except Exception as exc:
        result = {"ok": False, "error": str(exc) or "Order placement failed."}
    return JSONResponse(
        result,
        status_code=200 if result.get("ok") else 400,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.post("/api/admin/ione-power-order")
def admin_ione_power_order(
    request: Request,
    payload: dict = Body(...),
):
    admin = require_admin(request)
    if not admin:
        return JSONResponse({"ok": False, "error": "Admin authentication required."}, status_code=403)
    try:
        order_kind = str(payload.get("order_kind") or "equity").lower()
        if order_kind == "option":
            result = engine.place_ione_power_option_order(
                symbol=payload.get("symbol"),
                option_side=payload.get("option_side"),
                client_price=payload.get("price"),
                option_ltp=payload.get("option_ltp"),
                retry_attempts=2,
            )
        else:
            result = engine.place_ione_power_equity_order(
                symbol=payload.get("symbol"),
                side=payload.get("side"),
                per_trade_risk=payload.get("per_trade_risk", 800),
                stop_loss_pct=payload.get("stop_loss_pct", 0.6),
                client_price=payload.get("price"),
                locked_quantity=payload.get("quantity"),
                retry_attempts=2,
            )
    except Exception as exc:
        result = {"ok": False, "error": str(exc) or "Order placement failed."}
    return JSONResponse(
        result,
        status_code=200 if result.get("ok") else 400,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.post("/api/acceleration-hit")
def acceleration_hit_action(
    request: Request,
    payload: dict = Body(...),
):
    result = engine.update_acceleration_hit(
        event_id=payload.get("event_id"),
        action=payload.get("action"),
    )
    return JSONResponse(
        result,
        status_code=200 if result.get("ok") else 400,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.post("/api/admin/market-history/cache")
def admin_market_history_cache(request: Request):
    admin = require_admin(request)
    if not admin:
        return JSONResponse({"ok": False, "error": "Admin authentication required."}, status_code=403)
    return JSONResponse(
        {
            "ok": True,
            "status": engine.start_daily_market_history_cache(force=True),
        },
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/api/admin/market-history/status")
def admin_market_history_status(request: Request):
    admin = require_admin(request)
    if not admin:
        return JSONResponse({"ok": False, "error": "Admin authentication required."}, status_code=403)
    return JSONResponse(
        {
            "ok": True,
            "status": engine.get_history_cache_status(),
        },
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/api/sector-breakdown")
def sector_breakdown(request: Request, sector: str):
    return JSONResponse(
        engine.get_sector_breakdown(sector),
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )

@app.get("/inquiry", response_class=HTMLResponse)
def inquiry_get(request: Request):
    user = require_login(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    return templates.TemplateResponse(
        request,
        "inquiry.html",
        {
            "user": user,
            "admin": None,
        },
    )


@app.post("/inquiry")
def inquiry(
    request: Request,
    subject: str = Form(...),
    message: str = Form(...),
    csrf_token: str = Form(""),
):
    require_csrf(request, csrf_token)
    user = require_login(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)

    create_inquiry(user["id"], subject, message)
    return RedirectResponse(url="/dashboard?inquiry=success", status_code=302)


@app.get("/services", response_class=HTMLResponse)
def services(request: Request):
    raise HTTPException(status_code=404, detail="Services page removed")


@app.get("/premium", response_class=HTMLResponse)
def premium(request: Request):
    user = current_user(request)
    admin = current_admin(request)
    course_settings = get_course_settings()
    videos = get_academy_videos()
    return templates.TemplateResponse(
        request,
        "premium.html",
        {
            "title": "Academy & Premium Subscription",
            "user": user,
            "admin": admin,
            "public_mode": True if not user and not admin else False,
            "course_settings": course_settings,
            "course_video_count": len(videos),
            "guest_expired": request.query_params.get("guest") == "expired",
        },
    )


@app.get("/academy", response_class=HTMLResponse)
def academy(request: Request):
    admin = current_admin(request)
    user = current_user(request)
    if not admin and not user:
        return RedirectResponse(url="/login", status_code=302)

    active_license = None
    academy_access = False
    if admin:
        academy_access = True
    elif user:
        active_license = get_active_license_for_user(user["id"])
        academy_access = active_license is not None

    return templates.TemplateResponse(
        request,
        "academy.html",
        {
            "title": "Academy",
            "user": user,
            "admin": admin,
            "public_mode": False,
            "course_settings": get_course_settings(),
            "academy_videos": get_academy_videos(include_unpublished=bool(admin)),
            "academy_access": academy_access,
            "active_license": active_license,
            "license_error": None,
            "license_success": request.query_params.get("activated") == "1",
        },
    )


@app.post("/academy/license", response_class=HTMLResponse)
def academy_activate_license(request: Request, license_key: str = Form(...), csrf_token: str = Form("")):
    require_csrf(request, csrf_token)
    user = require_login(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)

    result = activate_academy_license(user["id"], user["email"], license_key)
    if result["ok"]:
        return RedirectResponse(url="/academy?activated=1", status_code=302)

    active_license = get_active_license_for_user(user["id"])
    return templates.TemplateResponse(
        request,
        "academy.html",
        {
            "title": "Academy",
            "user": user,
            "admin": None,
            "public_mode": False,
            "course_settings": get_course_settings(),
            "academy_videos": get_academy_videos(),
            "academy_access": active_license is not None,
            "active_license": active_license,
            "license_error": result["error"],
            "license_success": False,
        },
    )


# --- Admin Routes ---

@app.get("/admin", response_class=HTMLResponse)
def admin_home(request: Request):
    admin = current_admin(request)
    if admin:
        ip = get_client_ip(request)
        if not is_ip_allowed(ip):
            return RedirectResponse(url="/admin/login", status_code=302)
        if not admin["totp_enabled"]:
            return RedirectResponse(url="/admin/2fa/setup", status_code=302)
        creds = get_kite_credentials()
        dhan_creds = get_dhan_credentials()
        active_broker = get_active_broker()
        audit_logs = get_admin_login_audit(12)
        inquiries = get_inquiries(20)
        recent_users = get_recent_users(20)
        course_settings = get_course_settings()
        free_course_classes = get_free_course_classes(include_unpublished=True)
        academy_videos = get_academy_videos(include_unpublished=True)
        academy_licenses = get_recent_academy_licenses(30)
        generated_license = request.session.pop("last_generated_license", None)
        users_activity = []
        for u in recent_users:
            trial = trial_status(u)
            users_activity.append(
                {
                    "id": u["id"],
                    "full_name": u["full_name"],
                    "email": u["email"],
                    "phone": u["phone"],
                    "created_at": u["created_at"],
                    "trial": trial,
                    "last_login_at": u["last_login_at"],
                    "login_count": u["login_count"],
                }
            )
        return templates.TemplateResponse(
            request,
            "admin_panel.html",
            {
                "admin": admin,
                "user": None,
                "creds": creds,
                "dhan_creds": dhan_creds,
                "active_broker": active_broker,
                "audit_logs": audit_logs,
                "inquiries": inquiries,
                "users_activity": users_activity,
                "course_settings": course_settings,
                "free_course_classes": free_course_classes,
                "academy_videos": academy_videos,
                "academy_licenses": academy_licenses,
                "generated_license": generated_license,
            },
        )

    if not get_admin_user():
        return RedirectResponse(url="/admin/setup", status_code=302)

    return RedirectResponse(url="/admin/login", status_code=302)


@app.get("/admin/setup", response_class=HTMLResponse)
def admin_setup_get(request: Request):
    ip = get_client_ip(request)
    if not is_ip_allowed(ip):
        return RedirectResponse(url="/admin/login", status_code=302)
    if get_admin_user():
        return RedirectResponse(url="/admin/login", status_code=302)
    return templates.TemplateResponse(request, "admin_setup.html", {"error": None, "admin": None, "user": None})


@app.post("/admin/setup", response_class=HTMLResponse)
def admin_setup_post(
    request: Request,
    full_name: str = Form(...),
    email: str = Form(...),
    phone: str = Form(...),
    password: str = Form(...),
    csrf_token: str = Form(""),
):
    require_csrf(request, csrf_token)
    ip = get_client_ip(request)
    if not is_ip_allowed(ip):
        return templates.TemplateResponse(
            request,
            "admin_setup.html",
            {"error": "Admin setup is not allowed from this IP.", "admin": None, "user": None},
        )
    if get_admin_user():
        return RedirectResponse(url="/admin/login", status_code=302)
    password_error = password_policy_error(password)
    if password_error:
        return templates.TemplateResponse(
            request,
            "admin_setup.html",
            {"error": password_error, "admin": None, "user": None},
        )
    existing = get_user_by_email(email)
    if existing:
        return templates.TemplateResponse(
            request,
            "admin_setup.html",
            {
                "error": "That email is already registered as a user. Use a different email or remove the old user account first.",
                "admin": None,
                "user": None,
            },
        )

    password_hash = hash_password(password)
    admin_id = create_user(full_name, email, phone, password_hash, trial_days=1, is_admin=1)
    request.session["admin_id"] = admin_id
    return RedirectResponse(url="/admin/2fa/setup", status_code=302)


@app.get("/admin/login", response_class=HTMLResponse)
def admin_login_get(request: Request):
    admin = current_admin(request)
    if admin:
        if admin["totp_enabled"]:
            return RedirectResponse(url="/admin", status_code=302)
        return RedirectResponse(url="/admin/2fa/setup", status_code=302)
    pending_id = request.session.get("admin_2fa_pending")
    if pending_id:
        return RedirectResponse(url="/admin/2fa", status_code=302)
    ip = get_client_ip(request)
    if not is_ip_allowed(ip):
        return templates.TemplateResponse(
            request,
            "admin_login.html",
            {
                "error": "Admin login is not allowed from this IP.",
                "admin": None,
                "user": None,
            },
        )
    return templates.TemplateResponse(
        request,
        "admin_login.html",
        {"error": None, "admin": None, "user": None},
    )


@app.post("/admin/login", response_class=HTMLResponse)
def admin_login_post(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    csrf_token: str = Form(""),
):
    require_csrf(request, csrf_token)
    ip = get_client_ip(request)
    user_agent = request.headers.get("user-agent", "-")
    if not is_ip_allowed(ip):
        log_admin_login(email, ip, user_agent, False, "ip_not_allowed")
        return templates.TemplateResponse(
            request,
            "admin_login.html",
            {
                "error": "Admin login is not allowed from this IP.",
                "admin": None,
                "user": None,
            },
        )

    locked_ttl = admin_login_locked(email, ip)
    if locked_ttl:
        minutes = max(1, locked_ttl // 60)
        log_admin_login(email, ip, user_agent, False, "rate_limited")
        return templates.TemplateResponse(
            request,
            "admin_login.html",
            {
                "error": f"Too many attempts. Try again in {minutes} minutes.",
                "admin": None,
                "user": None,
            },
        )

    admin = get_user_by_email(email)
    if not admin or not admin["is_admin"]:
        admin_login_fail(email, ip)
        log_admin_login(email, ip, user_agent, False, "invalid_credentials")
        return templates.TemplateResponse(
            request,
            "admin_login.html",
            {"error": "Invalid admin credentials.", "admin": None, "user": None},
        )

    verify_result = verify_password(password, admin["password_hash"])
    if not verify_result.ok:
        admin_login_fail(email, ip)
        log_admin_login(email, ip, user_agent, False, "invalid_credentials")
        return templates.TemplateResponse(
            request,
            "admin_login.html",
            {"error": "Invalid admin credentials.", "admin": None, "user": None},
        )

    admin_login_success(email, ip)
    maybe_upgrade_password_hash(admin, password, verify_result)
    log_admin_login(email, ip, user_agent, True, "success")
    if admin["totp_enabled"]:
        request.session["admin_2fa_pending"] = admin["id"]
        return RedirectResponse(url="/admin/2fa", status_code=302)

    request.session["admin_id"] = admin["id"]
    return RedirectResponse(url="/admin/2fa/setup", status_code=302)


@app.get("/admin/2fa", response_class=HTMLResponse)
def admin_2fa_get(request: Request):
    ip = get_client_ip(request)
    if not is_ip_allowed(ip):
        return RedirectResponse(url="/admin/login", status_code=302)
    pending_id = request.session.get("admin_2fa_pending")
    if not pending_id:
        return RedirectResponse(url="/admin/login", status_code=302)
    return templates.TemplateResponse(request, "admin_2fa.html", {"error": None, "admin": None, "user": None})


@app.post("/admin/2fa", response_class=HTMLResponse)
def admin_2fa_post(request: Request, code: str = Form(...), csrf_token: str = Form("")):
    require_csrf(request, csrf_token)
    ip = get_client_ip(request)
    if not is_ip_allowed(ip):
        return RedirectResponse(url="/admin/login", status_code=302)
    pending_id = request.session.get("admin_2fa_pending")
    if not pending_id:
        return RedirectResponse(url="/admin/login", status_code=302)
    admin = get_user_by_id(pending_id)
    if not admin or not admin["is_admin"] or not admin["totp_enabled"] or not admin["totp_secret"]:
        request.session.pop("admin_2fa_pending", None)
        return RedirectResponse(url="/admin/login", status_code=302)

    totp = pyotp.TOTP(admin["totp_secret"])
    if not totp.verify(code, valid_window=1):
        return templates.TemplateResponse(
            request,
            "admin_2fa.html",
            {"error": "Invalid authentication code.", "admin": None, "user": None},
        )

    request.session.pop("admin_2fa_pending", None)
    request.session["admin_id"] = admin["id"]
    return RedirectResponse(url="/admin", status_code=302)


@app.get("/admin/logout")
def admin_logout(request: Request):
    request.session.pop("admin_id", None)
    request.session.pop("admin_2fa_pending", None)
    request.session.pop("admin_2fa_setup_secret", None)
    return RedirectResponse(url="/admin/login", status_code=302)


@app.get("/admin/2fa/setup", response_class=HTMLResponse)
def admin_2fa_setup_get(request: Request):
    ip = get_client_ip(request)
    if not is_ip_allowed(ip):
        return RedirectResponse(url="/admin/login", status_code=302)
    admin = require_admin(request)
    if not admin:
        return RedirectResponse(url="/admin/login", status_code=302)

    if admin["totp_enabled"]:
        return RedirectResponse(url="/admin", status_code=302)

    secret = pyotp.random_base32()
    request.session["admin_2fa_setup_secret"] = secret
    issuer = "IoneAlgo Pro"
    label = admin["email"]
    otpauth = pyotp.totp.TOTP(secret).provisioning_uri(name=label, issuer_name=issuer)
    return templates.TemplateResponse(
        request,
        "admin_2fa_setup.html",
        {"secret": secret, "otpauth": otpauth, "error": None, "admin": admin, "user": None},
    )


@app.post("/admin/2fa/setup", response_class=HTMLResponse)
def admin_2fa_setup_post(request: Request, code: str = Form(...), csrf_token: str = Form("")):
    require_csrf(request, csrf_token)
    ip = get_client_ip(request)
    if not is_ip_allowed(ip):
        return RedirectResponse(url="/admin/login", status_code=302)
    admin = require_admin(request)
    if not admin:
        return RedirectResponse(url="/admin/login", status_code=302)

    if admin["totp_enabled"]:
        return RedirectResponse(url="/admin", status_code=302)

    secret = request.session.get("admin_2fa_setup_secret")
    if not secret:
        return RedirectResponse(url="/admin/2fa/setup", status_code=302)

    totp = pyotp.TOTP(secret)
    if not totp.verify(code, valid_window=1):
        issuer = "IoneAlgo Pro"
        label = admin["email"]
        otpauth = pyotp.totp.TOTP(secret).provisioning_uri(name=label, issuer_name=issuer)
        return templates.TemplateResponse(
            request,
            "admin_2fa_setup.html",
            {"secret": secret, "otpauth": otpauth, "error": "Invalid authentication code.", "admin": admin, "user": None},
        )

    set_admin_totp(admin["id"], secret, True)
    request.session.pop("admin_2fa_setup_secret", None)
    return RedirectResponse(url="/admin", status_code=302)


@app.get("/admin/2fa/qr")
def admin_2fa_qr(request: Request):
    admin = require_admin(request)
    if not admin:
        return RedirectResponse(url="/admin/login", status_code=302)

    secret = request.session.get("admin_2fa_setup_secret")
    if not secret:
        return RedirectResponse(url="/admin/2fa/setup", status_code=302)

    issuer = "IoneAlgo Pro"
    label = admin["email"]
    otpauth = pyotp.totp.TOTP(secret).provisioning_uri(name=label, issuer_name=issuer)
    img = qrcode.make(otpauth)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return StreamingResponse(buf, media_type="image/png")


@app.post("/admin/kite/credentials")
def admin_save_kite_credentials(
    request: Request,
    api_key: str = Form(...),
    api_secret: str = Form(...),
    csrf_token: str = Form(""),
):
    require_csrf(request, csrf_token)
    admin = require_admin(request)
    if not admin:
        return RedirectResponse(url="/admin/login", status_code=302)

    save_kite_credentials(api_key, api_secret)
    return RedirectResponse(url="/admin", status_code=302)


@app.post("/admin/dhan/credentials")
def admin_save_dhan_credentials(
    request: Request,
    client_id: str = Form(...),
    access_token: str = Form(...),
    csrf_token: str = Form(""),
):
    require_csrf(request, csrf_token)
    admin = require_admin(request)
    if not admin:
        return RedirectResponse(url="/admin/login", status_code=302)

    save_dhan_credentials(client_id, access_token)
    set_active_broker("dhan")
    start_dhan_engine_in_background(client_id, access_token)
    return RedirectResponse(url="/admin?dhan=connected", status_code=302)


@app.post("/admin/broker")
def admin_select_broker(
    request: Request,
    active_broker: str = Form(...),
    csrf_token: str = Form(""),
):
    require_csrf(request, csrf_token)
    admin = require_admin(request)
    if not admin:
        return RedirectResponse(url="/admin/login", status_code=302)

    broker = "dhan" if active_broker == "dhan" else "kite"
    set_active_broker(broker)
    if broker == "dhan":
        creds = get_dhan_credentials()
        if creds:
            start_dhan_engine_in_background(creds["client_id"], creds["access_token"])
    else:
        creds = get_kite_credentials()
        token = engine.token_from_redis()
        if creds and token:
            start_engine_in_background(creds["api_key"], token)
    return RedirectResponse(url="/admin", status_code=302)


@app.post("/admin/inquiry/status")
def admin_inquiry_status(
    request: Request,
    inquiry_id: int = Form(...),
    status: str = Form(...),
    csrf_token: str = Form(""),
):
    require_csrf(request, csrf_token)
    admin = require_admin(request)
    if not admin:
        return RedirectResponse(url="/admin/login", status_code=302)
    status = "closed" if status == "closed" else "open"
    update_inquiry_status(inquiry_id, status)
    return RedirectResponse(url="/admin", status_code=302)


@app.post("/admin/course/settings")
def admin_course_settings(
    request: Request,
    four_month_price: int = Form(...),
    one_year_price: int = Form(...),
    support_text: str = Form(...),
    csrf_token: str = Form(""),
):
    require_csrf(request, csrf_token)
    admin = require_admin(request)
    if not admin:
        return RedirectResponse(url="/admin/login", status_code=302)

    update_course_settings(four_month_price, one_year_price, support_text)
    return RedirectResponse(url="/admin", status_code=302)


@app.post("/admin/free-course/settings")
def admin_free_course_settings(
    request: Request,
    free_course_title: str = Form(...),
    free_course_description: str = Form(""),
    free_course_youtube_url: str = Form(""),
    csrf_token: str = Form(""),
):
    raise HTTPException(status_code=404, detail="Free course management removed")


@app.post("/admin/free-course/classes")
def admin_add_free_course_class(
    request: Request,
    title: str = Form(...),
    description: str = Form(""),
    youtube_url: str = Form(...),
    sort_order: int = Form(0),
    is_published: int = Form(1),
    csrf_token: str = Form(""),
):
    raise HTTPException(status_code=404, detail="Free course management removed")


@app.post("/admin/free-course/classes/delete")
def admin_delete_free_course_class(request: Request, class_id: int = Form(...), csrf_token: str = Form("")):
    raise HTTPException(status_code=404, detail="Free course management removed")


@app.post("/admin/course/payment-qr")
async def admin_course_payment_qr(
    request: Request,
    payment_qr: UploadFile = File(...),
    csrf_token: str = Form(""),
):
    require_csrf(request, csrf_token)
    admin = require_admin(request)
    if not admin:
        return RedirectResponse(url="/admin/login", status_code=302)

    content_type = (payment_qr.content_type or "").lower()
    allowed_types = {"image/png", "image/jpeg", "image/jpg", "image/webp"}
    if content_type not in allowed_types:
        return RedirectResponse(url="/admin?payment_qr=invalid", status_code=302)

    data = await payment_qr.read()
    if not data or len(data) > 3 * 1024 * 1024:
        return RedirectResponse(url="/admin?payment_qr=invalid", status_code=302)

    try:
        with Image.open(io.BytesIO(data)) as image:
            image.load()
            image = image.convert("RGB")
            if max(image.size) > 900:
                image.thumbnail((900, 900), Image.Resampling.NEAREST)
            output = io.BytesIO()
            image.save(output, format="PNG", optimize=True, compress_level=9)
            optimized = output.getvalue()
    except (UnidentifiedImageError, OSError, ValueError):
        return RedirectResponse(url="/admin?payment_qr=invalid", status_code=302)

    upload_dir = Path("static") / "uploads" / "payments"
    upload_dir.mkdir(parents=True, exist_ok=True)
    filename = f"payment-qr-{int(datetime.now(timezone.utc).timestamp())}.png"
    target = upload_dir / filename
    target.write_bytes(optimized)
    update_course_payment_qr(f"/static/uploads/payments/{filename}")
    return RedirectResponse(url="/admin?payment_qr=uploaded", status_code=302)


@app.post("/admin/academy/videos")
def admin_add_video(
    request: Request,
    title: str = Form(...),
    youtube_url: str = Form(...),
    sort_order: int = Form(0),
    is_published: int = Form(1),
    csrf_token: str = Form(""),
):
    require_csrf(request, csrf_token)
    admin = require_admin(request)
    if not admin:
        return RedirectResponse(url="/admin/login", status_code=302)
    if not youtube_embed_url(youtube_url):
        return RedirectResponse(url="/admin?academy=invalid_url", status_code=302)

    add_academy_video(title, youtube_url, sort_order, is_published)
    return RedirectResponse(url="/admin", status_code=302)


@app.post("/admin/academy/videos/delete")
def admin_delete_video(request: Request, video_id: int = Form(...), csrf_token: str = Form("")):
    require_csrf(request, csrf_token)
    admin = require_admin(request)
    if not admin:
        return RedirectResponse(url="/admin/login", status_code=302)

    delete_academy_video(video_id)
    return RedirectResponse(url="/admin", status_code=302)


@app.post("/admin/academy/licenses")
def admin_generate_license(
    request: Request,
    assigned_email: str = Form(...),
    plan_name: str = Form(...),
    duration_days: int = Form(...),
    notes: str = Form(""),
    csrf_token: str = Form(""),
):
    require_csrf(request, csrf_token)
    admin = require_admin(request)
    if not admin:
        return RedirectResponse(url="/admin/login", status_code=302)

    generated = create_academy_license(assigned_email, plan_name, duration_days, notes)
    request.session["last_generated_license"] = {
        "license_key": generated["license_key"],
        "assigned_email": assigned_email.strip().lower(),
        "plan_name": plan_name.strip(),
        "duration_days": int(duration_days),
    }
    return RedirectResponse(url="/admin", status_code=302)


@app.get("/admin/kite/login")
def admin_kite_login(request: Request):
    admin = require_admin(request)
    if not admin:
        return RedirectResponse(url="/admin/login", status_code=302)

    creds = get_kite_credentials()
    if not creds:
        return RedirectResponse(url="/admin", status_code=302)

    kite = KiteConnect(api_key=creds["api_key"])
    login_url = kite.login_url()
    return RedirectResponse(url=login_url, status_code=302)


@app.get("/zerodha/callback")
def kite_callback(request: Request, request_token: str = None, status: str = None):
    admin = require_admin(request)
    if not admin:
        return RedirectResponse(url="/admin/login", status_code=302)

    creds = get_kite_credentials()
    if not creds or status != "success" or not request_token:
        return RedirectResponse(url="/admin?kite=failed", status_code=302)

    kite = KiteConnect(api_key=creds["api_key"])
    data = kite.generate_session(request_token, api_secret=creds["api_secret"])
    access_token = data.get("access_token")
    if access_token:
        engine.save_token(access_token)
        set_active_broker("kite")
        start_engine_in_background(creds["api_key"], access_token)

    return RedirectResponse(url="/admin?kite=connected", status_code=302)
