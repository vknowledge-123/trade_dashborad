from datetime import datetime, timedelta
import io
import math

import pyotp
import qrcode

from fastapi import FastAPI, Form, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
import redis
from kiteconnect import KiteConnect

from app.middleware import BlockLoggedInUserFromAdminMiddleware
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
    create_inquiry,
    get_course_settings,
    update_course_settings,
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
from app.kite_engine import MarketEngine
from app.security import hash_password, verify_password, should_upgrade_password_hash

app = FastAPI()
# Serve local static assets (images used in Services page, etc.)
app.mount("/static", StaticFiles(directory="static"), name="static")
# Middleware order matters: SessionMiddleware must wrap anything that reads sessions.
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

SECTOR_INDICES = [
    "NIFTY AUTO",
    "NIFTY IT",
    "NIFTY METAL",
    "NIFTY FINSEREXBNK",
    "NIFTY MS FIN SERV",
    "NIFTY HEALTHCARE",
    "NIFTY MIDSML HLTH",
    "NIFTY PSU BANK",
    "NIFTY CONSR DURBL",
    "NIFTY FMCG",
    "NIFTY PVT BANK",
    "NIFTY ENERGY",
    "NIFTY CPSE",
    "NIFTY MS IT TELCM",
    "NIFTY IND DEFENCE",
    "NIFTY MEDIA",
    "NIFTY IND DIGITAL",
    "NIFTY IND TOURISM",
    "NIFTY CAPITAL MKT",
    "NIFTY OIL AND GAS",
    "NIFTY INDIA MFG",
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
GUEST_PREMIUM_PROMPT_DAYS = 7
USER_PREMIUM_PROMPT_DAYS = 7

@app.on_event("startup")
def on_startup():
    init_db()
    creds = get_kite_credentials()
    token = engine.token_from_redis()
    if creds and token:
        engine.start(creds["api_key"], token, SECTOR_INDICES)


# --- Helpers ---

def maybe_upgrade_password_hash(user_row, password: str, verify_result=None):
    if not user_row:
        return
    stored = user_row["password_hash"]
    result = verify_result or verify_password(password, stored)
    if should_upgrade_password_hash(stored, result):
        update_user_password_hash(user_row["id"], hash_password(password))


def _admin_login_keys(email: str, ip: str):
    safe_email = (email or "").lower()
    safe_ip = ip or "unknown"
    return (
        f"admin:login:fail:{safe_email}:{safe_ip}",
        f"admin:login:lock:{safe_email}:{safe_ip}",
    )


def admin_login_locked(email: str, ip: str):
    try:
        _, lock_key = _admin_login_keys(email, ip)
        ttl = redis_client.ttl(lock_key)
        return ttl if ttl and ttl > 0 else 0
    except Exception:
        return 0


def admin_login_fail(email: str, ip: str):
    try:
        fail_key, lock_key = _admin_login_keys(email, ip)
        count = redis_client.incr(fail_key)
        if count == 1:
            redis_client.expire(fail_key, 600)
        if count >= 5:
            redis_client.setex(lock_key, 600, "1")
            redis_client.delete(fail_key)
    except Exception:
        pass


def admin_login_success(email: str, ip: str):
    try:
        fail_key, lock_key = _admin_login_keys(email, ip)
        redis_client.delete(fail_key)
        redis_client.delete(lock_key)
    except Exception:
        pass


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
    if not user_row["is_admin"]:
        days = max(days, USER_PREMIUM_PROMPT_DAYS)
    end = start + timedelta(days=days)
    now = datetime.utcnow()
    remaining_seconds = max(0, int((end - now).total_seconds()))
    remaining_days = math.ceil(remaining_seconds / 86400) if remaining_seconds else 0
    return {
        "active": now <= end,
        "end_date": end.strftime("%Y-%m-%d"),
        "remaining_days": remaining_days,
        "total_days": days,
    }


def guest_dashboard_status(request: Request):
    now = datetime.utcnow()
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
    premium_prompt_at = started_at + timedelta(days=GUEST_PREMIUM_PROMPT_DAYS)
    register_remaining_seconds = max(0, int((register_prompt_at - now).total_seconds()))
    hours_left = register_remaining_seconds // 3600
    minutes_left = max(1, register_remaining_seconds // 60) if register_remaining_seconds else 0

    if now >= premium_prompt_at:
        stage = "premium"
        remaining_text = "Premium invitation active"
    elif now >= register_prompt_at:
        stage = "register"
        remaining_text = "Registration recommended"
    elif hours_left >= 1:
        stage = "fresh"
        remaining_text = f"{hours_left}h before registration prompt"
    else:
        stage = "fresh"
        remaining_text = f"{minutes_left}m before registration prompt"

    return {
        "started_at": started_at.isoformat(timespec="seconds"),
        "register_prompt_at": register_prompt_at.isoformat(timespec="seconds"),
        "premium_prompt_at": premium_prompt_at.isoformat(timespec="seconds"),
        "stage": stage,
        "remaining_text": remaining_text,
    }


# --- Public Routes ---

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return RedirectResponse(url="/dashboard", status_code=302)


@app.get("/register", response_class=HTMLResponse)
def register_get(request: Request):
    return templates.TemplateResponse(request, "register.html", {"error": None, "user": None, "admin": None})


@app.post("/register", response_class=HTMLResponse)
def register_post(
    request: Request,
    full_name: str = Form(...),
    email: str = Form(...),
    phone: str = Form(...),
    password: str = Form(...),
):
    existing = get_user_by_email(email)
    if existing:
        return templates.TemplateResponse(request, "register.html", {"error": "Email already registered.", "user": None, "admin": None})

    password_hash = hash_password(password)
    user_id = create_user(full_name, email, phone, password_hash, trial_days=USER_PREMIUM_PROMPT_DAYS)
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
):
    user = get_user_by_email(email)
    if not user:
        return templates.TemplateResponse(request, "login.html", {"error": "Invalid email or password.", "user": None, "admin": None})

    verify_result = verify_password(password, user["password_hash"])
    if not verify_result.ok:
        return templates.TemplateResponse(request, "login.html", {"error": "Invalid email or password.", "user": None, "admin": None})

    maybe_upgrade_password_hash(user, password, verify_result)
    ip = get_client_ip(request)
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
    snapshot = engine.get_snapshot()
    trial = trial_status(user) if user else None

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
        },
    )


@app.get("/api/market-snapshot")
def market_snapshot(request: Request):
    return JSONResponse(engine.get_snapshot())


@app.get("/api/sector-breakdown")
def sector_breakdown(request: Request, sector: str):
    return JSONResponse(engine.get_sector_breakdown(sector))

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
):
    user = require_login(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)

    create_inquiry(user["id"], subject, message)
    return RedirectResponse(url="/dashboard?inquiry=success", status_code=302)


@app.get("/services", response_class=HTMLResponse)
def services(request: Request):
    user = current_user(request)
    admin = current_admin(request)
    return templates.TemplateResponse(
        request,
        "services.html",
        {
            "title": "Services",
            "user": user,
            "admin": admin,
            "public_mode": True if not user and not admin else False,
        },
    )


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
def academy_activate_license(request: Request, license_key: str = Form(...)):
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
        audit_logs = get_admin_login_audit(12)
        inquiries = get_inquiries(20)
        recent_users = get_recent_users(20)
        course_settings = get_course_settings()
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
                "audit_logs": audit_logs,
                "inquiries": inquiries,
                "users_activity": users_activity,
                "course_settings": course_settings,
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
):
    ip = get_client_ip(request)
    if not is_ip_allowed(ip):
        return templates.TemplateResponse(
            request,
            "admin_setup.html",
            {"error": "Admin setup is not allowed from this IP.", "admin": None, "user": None},
        )
    if get_admin_user():
        return RedirectResponse(url="/admin/login", status_code=302)
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
):
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
def admin_2fa_post(request: Request, code: str = Form(...)):
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
def admin_2fa_setup_post(request: Request, code: str = Form(...)):
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
):
    admin = require_admin(request)
    if not admin:
        return RedirectResponse(url="/admin/login", status_code=302)

    save_kite_credentials(api_key, api_secret)
    return RedirectResponse(url="/admin", status_code=302)


@app.post("/admin/inquiry/status")
def admin_inquiry_status(
    request: Request,
    inquiry_id: int = Form(...),
    status: str = Form(...),
):
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
):
    admin = require_admin(request)
    if not admin:
        return RedirectResponse(url="/admin/login", status_code=302)

    update_course_settings(four_month_price, one_year_price, support_text)
    return RedirectResponse(url="/admin", status_code=302)


@app.post("/admin/academy/videos")
def admin_add_video(
    request: Request,
    title: str = Form(...),
    youtube_url: str = Form(...),
    sort_order: int = Form(0),
    is_published: int = Form(1),
):
    admin = require_admin(request)
    if not admin:
        return RedirectResponse(url="/admin/login", status_code=302)

    add_academy_video(title, youtube_url, sort_order, is_published)
    return RedirectResponse(url="/admin", status_code=302)


@app.post("/admin/academy/videos/delete")
def admin_delete_video(request: Request, video_id: int = Form(...)):
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
):
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
def kite_callback(request: Request, request_token: str = None):
    admin = require_admin(request)
    if not admin:
        return RedirectResponse(url="/admin/login", status_code=302)

    creds = get_kite_credentials()
    if not creds or not request_token:
        return RedirectResponse(url="/admin", status_code=302)

    kite = KiteConnect(api_key=creds["api_key"])
    data = kite.generate_session(request_token, api_secret=creds["api_secret"])
    access_token = data.get("access_token")
    if access_token:
        engine.save_token(access_token)
        engine.start(creds["api_key"], access_token, SECTOR_INDICES)

    return RedirectResponse(url="/admin", status_code=302)
