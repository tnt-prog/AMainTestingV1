#!/usr/bin/env python3
"""
OKX Futures Scanner — Streamlit Dashboard  v2
Performance improvements:
  A. Bulk ticker pre-filter  — 1 API call eliminates ~70% of coins before any candle fetch
  B. Parallel timeframe fetch — 4 timeframes fetched simultaneously per coin
  C. Staged candle fetch      — quick 50-candle RSI/resistance check before full fetch
  D. Symbol cache             — OKX instrument list cached for 6 h, not re-fetched every cycle
  + API semaphore             — hard cap of 8 concurrent HTTP requests (no exchange blocking)
"""

import json, os, pathlib, threading, time, uuid, traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

# ─────────────────────────────────────────────────────────────────────────────
# Network constants
# ─────────────────────────────────────────────────────────────────────────────
BASE          = "https://www.okx.com"
OKX_INTERVALS = {"30m": "30m", "3m": "3m", "5m": "5m", "15m": "15m", "1h": "1H"}

# ─────────────────────────────────────────────────────────────────────────────
# API rate-limiter  — hard cap: 8 concurrent HTTP requests at any time
# OKX public-market limit is 20 req / 2 s.  Staying well below keeps us safe.
# ─────────────────────────────────────────────────────────────────────────────
_api_sem = threading.Semaphore(8)

# ─────────────────────────────────────────────────────────────────────────────
# Symbol-cache TTL  (D)
# ─────────────────────────────────────────────────────────────────────────────
_SYMBOL_CACHE_TTL = 6 * 3600   # refresh OKX instrument list every 6 hours

# ─────────────────────────────────────────────────────────────────────────────
# Bulk pre-filter thresholds  (A)
# ─────────────────────────────────────────────────────────────────────────────
PRE_FILTER_MIN_VOL_USDT  =  100_000   # minimum 24 h USDT volume
PRE_FILTER_LOW_BUFFER    =    1.005   # price must be ≥ 0.5 % above 24 h low

# ─────────────────────────────────────────────────────────────────────────────
# Dubai Timezone (UTC+4, no DST)
# ─────────────────────────────────────────────────────────────────────────────
DUBAI_TZ = timezone(timedelta(hours=4))

def dubai_now() -> datetime:
    return datetime.now(DUBAI_TZ)

def to_dubai(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(DUBAI_TZ)

def fmt_dubai(iso_str: str, fmt: str = "%m/%d %H:%M") -> str:
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return to_dubai(dt).strftime(fmt)
    except Exception:
        return iso_str[:16] if iso_str else "—"

# ─────────────────────────────────────────────────────────────────────────────
# Default configuration
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_CONFIG: dict = {
    "tp_pct":               1.5,
    "sl_pct":               3.0,
    # ── per-filter enable/disable ──────────────────────────────────────────────
    "use_pre_filter":       True,   # Bulk ticker pre-filter (volume / change / low)
    "use_rsi_5m":           True,   # F4 — 5m RSI
    "rsi_5m_min":           30,
    "use_rsi_1h":           True,   # F7 — 1h RSI
    "rsi_1h_min":           30,
    "rsi_1h_max":           95,
    "loop_minutes":         5,
    "cooldown_minutes":     5,
    "use_ema_3m":           False,
    "ema_period_3m":      12,
    "use_ema_5m":         True,
    "ema_period_5m":      12,
    "use_ema_15m":        True,
    "ema_period_15m":     12,
    "use_macd":           True,
    "use_sar":            True,
    "use_vol_spike":      False,
    "vol_spike_mult":     2.0,
    "vol_spike_lookback": 20,
    "use_pdz":            True,   # F12 — Premium/Discount/Equilibrium zone (5m)
    "watchlist": [
        # ── Top-tier liquid OKX perpetuals ───────────────────────────────────
        "BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT","ADAUSDT",
        "DOGEUSDT","AVAXUSDT","DOTUSDT","LINKUSDT","LTCUSDT","BCHUSDT",
        "XLMUSDT","TRXUSDT","ATOMUSDT","UNIUSDT","ETCUSDT","NEARUSDT",
        "APTUSDT","SUIUSDT","INJUSDT","ARBUSDT","OPUSDT","STRKUSDT",
        "TONUSDT","AAVEUSDT","LDOUSDT","FILUSDT","IMXUSDT","STXUSDT",
        "ORDIUSDT","WLDUSDT","JUPUSDT","PENDLEUSDT","CRVUSDT","TIAUSDT",
        "SEIUSDT","TAOUSDT","RENDERUSDT","FETUSDT","HBARUSDT","MANTRAUSDT",
        "SANDUSDT","GALAUSDT","AXSUSDT","APEUSDT","GRTUSDT","ENAUSDT",
        "POLUSDT","ZKUSDT","DYDXUSDT","SNXUSDT","COMPUSDT","ARUSDT",
        "KASUSDT","VETUSDT","ICPUSDT","MANAUSDT","ALICEUSDT","ALGOUSDT",
        # ── Mid-cap OKX perpetuals ────────────────────────────────────────────
        "SOLUSDT","WIFUSDT","JUPUSDT","BONKUSDT","PYTHUSDT","JITOSOL",
        "MEMEUSDT","BOMEUSDT","1000PEPEUSDT","1000SHIBUSDT","1000BONKUSDT",
        "1000FLOKIUSDT","TURBOUSDT","NEIROUSDT","FARTCOINUSDT",
        "XMRUSDT","ZECUSDT","DASHUSDT","DUSKUSDT","POLYXUSDT",
        "LUNA2USDT","STGUSDT","CAKEUSDT","1INCHUSDT","MORPHOUSDT",
        "AEROUSDT","ONDOUSDT","EIGENUSDT","ENAUSDT","CHZUSDT",
        "TRUMPUSDT","KAVAUSDT","API3USDT","PEOPLEUSDT","JTOUSDT",
        "TRBUSDT","ENSUSDT","QNTUSDT","ZILUSDT","ANKRUSDT",
        "ROSEUSDT","FLOWUSDT","AXLUSDT","YGGUSDT","INJUSDT",
        "CYBERUSDT","AUCTIONUSDT","CFXUSDT","JASMYUSDT","CRVUSDT",
        "WLDUSDT","ZKUSDT","LINEAUSDT","TOKENUSDT","WUSDT",
        "MOVEUSDT","INITUSDT","ENJUSDT","DENTUSDT","SYNUSDT",
        "ZROUSDT","SEIUSDT","ALGOUSDT","NAORISUSDT","ONTUSDT",
        "ARKMUSDT","VIRTUALUSDT","AIXBTUSDT","GRTUSDT","AGLDUSDT",
        "AIAUSDT","SKYAIUSDT","DEEPUSDT","VVVUSDT","ATUSDT",
        "HYPEUSDT","GRASSUSDT","KAITOUSDT","OMUSDT","IPUSDT",
        "ZORAUSDT","SPXUSDT","AKTUSDT","PNUTUSDT","WAXPUSDT",
        "PENGUUSDT","TNSRUSDT","WLFIUSDT","MOODENGUSDT","ACXUSDT",
        "GALAUSDT","APTUSDT","MEUSDT","COLLECTUSDT","LDOUSDT",
        "GUNUSDT","SKYUSDT","SYRUPUSDT","ALLOUSDT","LITUSDT",
        "NEOUSDT","REZUSDT","ARCUSDT","KERNELUSDT","PLUMEUSDT",
    ],
}

# ─────────────────────────────────────────────────────────────────────────────
# Sector tags
# ─────────────────────────────────────────────────────────────────────────────
SECTORS: dict = {
    "FETUSDT":"AI","RENDERUSDT":"AI","AIXBTUSDT":"AI","GRTUSDT":"AI",
    "AGLDUSDT":"AI","AIAUSDT":"AI","AINUSDT":"AI","UAIUSDT":"AI",
    "ARKMUSDT":"AI","VIRTUALUSDT":"AI","SKYAIUSDT":"AI","DEEPUSDT":"AI",
    "ZECUSDT":"Privacy","DASHUSDT":"Privacy","XMRUSDT":"Privacy",
    "DUSKUSDT":"Privacy","POLYXUSDT":"Privacy",
    "BTCUSDT":"BTC","ORDIUSDT":"BTC",
    "ETHUSDT":"L1","SOLUSDT":"L1","AVAXUSDT":"L1","ADAUSDT":"L1",
    "DOTUSDT":"L1","NEARUSDT":"L1","APTUSDT":"L1","SUIUSDT":"L1",
    "TONUSDT":"L1","XLMUSDT":"L1","TRXUSDT":"L1","LTCUSDT":"L1",
    "BCHUSDT":"L1","XRPUSDT":"L1","BNBUSDT":"L1","ATOMUSDT":"L1",
    "ARBUSDT":"L2","OPUSDT":"L2","STRKUSDT":"L2","ZKUSDT":"L2",
    "LINEAUSDT":"L2","POLUSDT":"L2","IMXUSDT":"L2",
    "AAVEUSDT":"DeFi","UNIUSDT":"DeFi","CRVUSDT":"DeFi","COMPUSDT":"DeFi",
    "SNXUSDT":"DeFi","DYDXUSDT":"DeFi","PENDLEUSDT":"DeFi","AEROUSDT":"DeFi",
    "MORPHOUSDT":"DeFi","1INCHUSDT":"DeFi","CAKEUSDT":"DeFi","LDOUSDT":"DeFi",
    "DOGEUSDT":"Meme","1000PEPEUSDT":"Meme","1000SHIBUSDT":"Meme",
    "1000BONKUSDT":"Meme","1000FLOKIUSDT":"Meme","FARTCOINUSDT":"Meme",
    "MEMEUSDT":"Meme","BOMEUSDT":"Meme","TURBOUSDT":"Meme","NEIROUSDT":"Meme",
    "SANDUSDT":"Gaming","MANAUSDT":"Gaming","GALAUSDT":"Gaming",
    "AXSUSDT":"Gaming","ALICEUSDT":"Gaming","APEUSDT":"Gaming",
}

# ─────────────────────────────────────────────────────────────────────────────
# Max leverage tiers
# ─────────────────────────────────────────────────────────────────────────────
MAX_LEVERAGE: dict = {
    "BTCUSDT":125,"ETHUSDT":100,
    "SOLUSDT":75,"BNBUSDT":75,"XRPUSDT":75,"DOGEUSDT":75,
    "ADAUSDT":75,"LTCUSDT":75,"BCHUSDT":75,"TRXUSDT":75,
    "XLMUSDT":75,"DOTUSDT":75,"AVAXUSDT":75,"LINKUSDT":75,
    "UNIUSDT":75,"ATOMUSDT":75,"ETCUSDT":75,
    "NEARUSDT":50,"APTUSDT":50,"SUIUSDT":50,"ARBUSDT":50,
    "OPUSDT":50,"INJUSDT":50,"TONUSDT":50,"AAVEUSDT":50,
    "LDOUSDT":50,"FILUSDT":50,"IMXUSDT":50,"STXUSDT":50,
    "ORDIUSDT":50,"WLDUSDT":50,"JUPUSDT":50,"PENDLEUSDT":50,
    "CRVUSDT":50,"TIAUSDT":50,"SEIUSDT":50,"TAOUSDT":50,
    "RENDERUSDT":50,"FETUSDT":50,"HBARUSDT":50,"MANTRAUSDT":50,
    "SANDUSDT":50,"GALAUSDT":50,"AXSUSDT":50,"APEUSDT":50,
    "GRTUSDT":50,"ENAUSDT":50,"POLUSDT":50,"STRKUSDT":50,
    "ZKUSDT":50,"DYDXUSDT":50,"SNXUSDT":50,"COMPUSDT":50,
    "ARUSDT":50,"KASUSDT":50,"VETUSDT":50,"ICPUSDT":50,
}

def get_max_leverage(sym: str) -> int:
    return MAX_LEVERAGE.get(sym, 20)

# ─────────────────────────────────────────────────────────────────────────────
# SL reason analyzer
# ─────────────────────────────────────────────────────────────────────────────
def analyze_sl_reason(sig: dict) -> str:
    criteria = sig.get("criteria", {})
    if not criteria:
        return "No criteria data captured"
    reasons = []
    rsi_5m  = criteria.get("rsi_5m", 50)
    rsi_1h  = criteria.get("rsi_1h", 50)
    if rsi_5m < 35:
        reasons.append(f"RSI 5m very low at entry ({rsi_5m}) — momentum already fading")
    elif rsi_5m < 42:
        reasons.append(f"RSI 5m borderline ({rsi_5m}) — weak short-term momentum")
    if rsi_1h > 82:
        reasons.append(f"RSI 1h near overbought ({rsi_1h}) — higher-TF exhaustion risk")
    elif rsi_1h < 40:
        reasons.append(f"RSI 1h weak ({rsi_1h}) — no hourly uptrend support")
    if not reasons:
        reasons.append("Unexpected market reversal / macro event — filters were healthy at entry")
    return "\n".join(f"• {r}" for r in reasons)

# ─────────────────────────────────────────────────────────────────────────────
# Config persistence
# ─────────────────────────────────────────────────────────────────────────────
try:
    _SCRIPT_DIR = pathlib.Path(__file__).parent.absolute()
except Exception:
    _SCRIPT_DIR = pathlib.Path.cwd()

_probe = _SCRIPT_DIR / ".write_probe"
try:
    _probe.touch(); _probe.unlink()
except OSError:
    import tempfile
    _SCRIPT_DIR = pathlib.Path(tempfile.gettempdir()) / "okx_scanner"

_SCRIPT_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_FILE  = _SCRIPT_DIR / "scanner_config.json"
LOG_FILE     = _SCRIPT_DIR / "scanner_log.json"
_config_lock = threading.Lock()

def load_config() -> dict:
    if CONFIG_FILE.exists():
        try:
            saved = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
            cfg   = dict(DEFAULT_CONFIG)
            for k in DEFAULT_CONFIG:
                if k in saved:
                    cfg[k] = saved[k]
            return cfg
        except Exception:
            pass
    return dict(DEFAULT_CONFIG)

def save_config(cfg: dict):
    with _config_lock:
        CONFIG_FILE.write_text(json.dumps(cfg, indent=2), encoding="utf-8")

def _migrate_criteria(crit: dict) -> dict:
    """
    Convert old-format criteria that stored "✅"/"—" into the new per-timeframe
    numeric keys.  Values that were "✅" become None (no data available) so the
    UI shows "—" instead of a stale tick mark.
    """
    if not crit:
        return crit
    # Migrate flat MACD/SAR/Vol keys → per-timeframe keys
    if "macd" in crit and "macd_3m" not in crit:
        v = crit.pop("macd")
        crit["macd_3m"]  = None if v == "✅" else v
        crit["macd_5m"]  = None
        crit["macd_15m"] = None
    if "sar" in crit and "sar_3m" not in crit:
        v = crit.pop("sar")
        crit["sar_3m"]  = None if v == "✅" else v
        crit["sar_5m"]  = None
        crit["sar_15m"] = None
    if "vol" in crit and "vol_ratio" not in crit:
        v = crit.pop("vol")
        crit["vol_ratio"] = None if v == "✅" else v
    # EMA: if stored as "✅" we have no actual value — set to None
    for key in ("ema_3m", "ema_5m", "ema_15m"):
        if crit.get(key) == "✅":
            crit[key] = None
    return crit

def load_log():
    if LOG_FILE.exists():
        try:
            data = json.loads(LOG_FILE.read_text(encoding="utf-8"))
            # Migrate any pre-update signals transparently
            for sig in data.get("signals", []):
                if "criteria" in sig:
                    sig["criteria"] = _migrate_criteria(sig["criteria"])
            return data
        except Exception:
            pass
    return {"health": {"total_cycles": 0, "last_scan_at": None,
                        "last_scan_duration_s": 0.0, "total_api_errors": 0,
                        "watchlist_size": 0, "pre_filtered_out": 0,
                        "deep_scanned": 0},
            "signals": []}

def save_log(log):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOG_FILE.write_text(json.dumps(log, indent=2), encoding="utf-8")

# ─────────────────────────────────────────────────────────────────────────────
# Module-level shared state
# ─────────────────────────────────────────────────────────────────────────────
if "_scanner_initialised" not in st.session_state:
    import builtins
    if not getattr(builtins, "_binance_scanner_globals_set", False):
        import builtins as _b
        _b._binance_scanner_globals_set = True
        _b._bsc_cfg           = load_config()
        _b._bsc_log           = load_log()
        _b._bsc_log_lock      = threading.Lock()
        _b._bsc_running       = threading.Event()
        _b._bsc_running.set()
        _b._bsc_thread        = None
        _b._bsc_filter_counts = {}
        _b._bsc_filter_lock   = threading.Lock()
        _b._bsc_last_error    = ""
        # D — symbol cache
        _b._bsc_symbol_cache  = {"symbols": [], "fetched_at": 0, "wl_key": ""}
    st.session_state["_scanner_initialised"] = True

import builtins as _b
_cfg             = _b._bsc_cfg
_log             = _b._bsc_log
_log_lock        = _b._bsc_log_lock
_scanner_running = _b._bsc_running
_filter_lock     = _b._bsc_filter_lock
_filter_counts   = _b._bsc_filter_counts

# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers  (+semaphore rate limiter)
# ─────────────────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept":          "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}
_local = threading.local()

def get_session():
    if not hasattr(_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        _local.session = s
    return _local.session

def safe_get(url, params=None, _retries=4):
    for attempt in range(_retries):
        try:
            with _api_sem:                           # ← rate-limiter (B/D)
                r = get_session().get(url, params=params, timeout=20)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 30))
                time.sleep(wait); continue
            if r.status_code in (418, 403, 451):
                raise RuntimeError(
                    f"HTTP {r.status_code}: OKX is blocking this server's IP. "
                    "Try Railway (railway.app) — uses residential IPs.")
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "code" in data and data["code"] != "0":
                raise RuntimeError(f"OKX API error {data['code']}: {data.get('msg','')}")
            return data
        except requests.exceptions.ConnectionError:
            if attempt < _retries - 1: time.sleep(3); continue
            raise
    raise RuntimeError(f"Failed after {_retries} retries: {url}")

# ─────────────────────────────────────────────────────────────────────────────
# OKX symbol helpers
# ─────────────────────────────────────────────────────────────────────────────
def _to_okx(sym: str) -> str:
    return f"{sym[:-4]}-USDT-SWAP" if sym.endswith("USDT") else sym

def _from_okx(inst_id: str) -> str:
    return inst_id.replace("-USDT-SWAP", "USDT") if inst_id.endswith("-USDT-SWAP") else inst_id

def get_symbols(watchlist: list) -> list:
    """Return watchlist symbols that are live USDT-SWAP perps on OKX."""
    active = set()
    data   = safe_get(f"{BASE}/api/v5/public/instruments", {"instType": "SWAP"})
    for s in data.get("data", []):
        inst_id = s.get("instId", "")
        if inst_id.endswith("-USDT-SWAP") and s.get("state") == "live":
            active.add(_from_okx(inst_id))
    skipped = [s for s in watchlist if s not in active]
    if skipped:
        print(f"  [Symbol cache] {len(skipped)} watchlist coins not on OKX: {skipped[:5]}...")
    return [s for s in watchlist if s in active]

# ─────────────────────────────────────────────────────────────────────────────
# D — Cached symbol lookup (refresh every 6 h or on watchlist change)
# ─────────────────────────────────────────────────────────────────────────────
def get_symbols_cached(watchlist: list) -> list:
    now    = time.time()
    cache  = _b._bsc_symbol_cache
    wl_key = ",".join(sorted(watchlist))
    if (not cache["symbols"] or
            now - cache["fetched_at"] > _SYMBOL_CACHE_TTL or
            cache["wl_key"] != wl_key):
        print("[Scanner] Refreshing OKX symbol cache…")
        cache["symbols"]    = get_symbols(watchlist)
        cache["fetched_at"] = now
        cache["wl_key"]     = wl_key
    return list(cache["symbols"])

# ─────────────────────────────────────────────────────────────────────────────
# A — Bulk ticker fetch + pre-filter (single API call)
# ─────────────────────────────────────────────────────────────────────────────
def get_bulk_tickers() -> dict:
    """
    ONE API call → dict {BTCUSDT: {last, open24h, high24h, low24h, volCcy24h}}
    for every USDT-SWAP pair on OKX.
    """
    data   = safe_get(f"{BASE}/api/v5/market/tickers", {"instType": "SWAP"})
    result = {}
    for t in data.get("data", []):
        inst_id = t.get("instId", "")
        if not inst_id.endswith("-USDT-SWAP"):
            continue
        sym = _from_okx(inst_id)
        try:
            result[sym] = {
                "last":      float(t.get("last",      0) or 0),
                "open24h":   float(t.get("open24h",   0) or 0),
                "high24h":   float(t.get("high24h",   0) or 0),
                "low24h":    float(t.get("low24h",    0) or 0),
                "volCcy24h": float(t.get("volCcy24h", 0) or 0),
            }
        except Exception:
            pass
    return result

def pre_filter_by_ticker(symbols: list, tickers: dict) -> list:
    """
    Zero extra API calls — uses data already in the bulk ticker snapshot.

    Keeps a coin only when:
      1. 24 h USDT volume ≥ PRE_FILTER_MIN_VOL_USDT  (liquid market)
      2. Last price ≥ 24 h low × PRE_FILTER_LOW_BUFFER (off the lows)
    """
    kept = []
    for sym in symbols:
        t = tickers.get(sym)
        if not t:
            continue
        last     = t["last"]
        low24h   = t["low24h"]
        vol_usdt = t["volCcy24h"]

        if vol_usdt < PRE_FILTER_MIN_VOL_USDT:
            continue

        if low24h > 0 and last < low24h * PRE_FILTER_LOW_BUFFER:
            continue

        kept.append(sym)
    return kept

# ─────────────────────────────────────────────────────────────────────────────
# Candle fetch
# ─────────────────────────────────────────────────────────────────────────────
def get_klines(sym: str, interval: str, limit: int) -> list:
    okx_iv  = OKX_INTERVALS.get(interval, interval)
    inst_id = _to_okx(sym)
    all_bars: list = []
    after = None
    while len(all_bars) < limit:
        batch  = min(300, limit - len(all_bars))
        params = {"instId": inst_id, "bar": okx_iv, "limit": batch}
        if after:
            params["after"] = after
        data = safe_get(f"{BASE}/api/v5/market/candles", params)
        bars = data.get("data", [])
        if not bars: break
        all_bars.extend(bars)
        after = bars[-1][0]
        if len(bars) < batch: break
    all_bars.reverse()
    return [{"time": int(b[0]), "open": float(b[1]), "high": float(b[2]),
             "low":  float(b[3]), "close": float(b[4]), "volume": float(b[5])}
            for b in all_bars]

# ─────────────────────────────────────────────────────────────────────────────
# Technical indicators
# ─────────────────────────────────────────────────────────────────────────────
def calc_rsi_series(closes, period=14):
    if len(closes) < period + 2: return []
    deltas = [closes[i]-closes[i-1] for i in range(1, len(closes))]
    gains  = [max(d, 0.) for d in deltas]
    losses = [max(-d, 0.) for d in deltas]
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    rsi = [100. if al == 0 else 100 - 100 / (1 + ag / al)]
    for i in range(period, len(deltas)):
        ag = (ag*(period-1)+gains[i])/period
        al = (al*(period-1)+losses[i])/period
        rsi.append(100. if al == 0 else 100 - 100/(1+ag/al))
    return rsi

def calc_ema(values: list, period: int) -> list:
    if len(values) < period: return []
    k      = 2.0 / (period + 1)
    result = [sum(values[:period]) / period]
    for v in values[period:]:
        result.append(v * k + result[-1] * (1 - k))
    return result

def calc_macd(closes: list, fast=12, slow=26, signal_period=9):
    if len(closes) < slow + signal_period: return [], [], []
    ema_f = calc_ema(closes, fast)
    ema_s = calc_ema(closes, slow)
    trim  = len(ema_f) - len(ema_s)
    ema_f = ema_f[trim:]
    macd_line = [f-s for f,s in zip(ema_f, ema_s)]
    if len(macd_line) < signal_period: return [], [], []
    sig_line     = calc_ema(macd_line, signal_period)
    trim2        = len(macd_line) - len(sig_line)
    macd_aligned = macd_line[trim2:]
    histogram    = [m-s for m,s in zip(macd_aligned, sig_line)]
    return macd_aligned, sig_line, histogram

def macd_bullish(closes: list, crossover_lookback: int = 12) -> bool:
    """
    True when (all on given closes):
      1. MACD line > 0
      2. Signal line > 0
      3. Histogram > 0 AND increasing  ← dark green only
      4. Bullish crossover within last crossover_lookback candles
    """
    macd_line, sig_line, histogram = calc_macd(closes)
    if not histogram or len(histogram) < 2: return False
    if macd_line[-1] <= 0 or sig_line[-1] <= 0: return False
    if histogram[-1] <= 0 or histogram[-1] <= histogram[-2]: return False
    n = min(crossover_lookback + 1, len(macd_line))
    for i in range(1, n):
        prev, curr = -(i+1), -i
        if (len(macd_line)+prev >= 0 and
                macd_line[prev] <= sig_line[prev] and
                macd_line[curr] >  sig_line[curr]):
            return True
    return False

def calc_parabolic_sar(candles: list, af_start=0.02, af_step=0.02, af_max=0.20):
    if not candles: return []
    if len(candles) < 2: return [(candles[0]["close"], True)]
    highs  = [c["high"]  for c in candles]
    lows   = [c["low"]   for c in candles]
    closes = [c["close"] for c in candles]
    bullish = closes[1] >= closes[0]
    ep, sar, af = (highs[0], lows[0], af_start) if bullish else (lows[0], highs[0], af_start)
    result = [(sar, bullish)]
    for i in range(1, len(candles)):
        new_sar = sar + af*(ep-sar)
        if bullish:
            new_sar = min(new_sar, lows[i-1])
            if i >= 2: new_sar = min(new_sar, lows[i-2])
            if lows[i] < new_sar:
                bullish, new_sar, ep, af = False, ep, lows[i], af_start
            else:
                if highs[i] > ep: ep = highs[i]; af = min(af+af_step, af_max)
        else:
            new_sar = max(new_sar, highs[i-1])
            if i >= 2: new_sar = max(new_sar, highs[i-2])
            if highs[i] > new_sar:
                bullish, new_sar, ep, af = True, ep, highs[i], af_start
            else:
                if lows[i] < ep: ep = lows[i]; af = min(af+af_step, af_max)
        sar = new_sar
        result.append((sar, bullish))
    return result

# ─────────────────────────────────────────────────────────────────────────────
# F12 — Premium / Discount / Equilibrium zone  (DZSAFM Pine Script logic)
# ─────────────────────────────────────────────────────────────────────────────
def calc_pdz_zone(candles: list, price: float) -> tuple:
    """
    Compute Smart Money Premium/Discount/Equilibrium zones from up to the last
    50 candles on a given timeframe (mirrors the DZSAFM TradingView indicator).

    Zone boundaries  (H = swing high, L = swing low over last 50 candles):
      Premium zone    : price >= 0.95·H + 0.05·L     ← top 5% of range
      Equilibrium zone: 0.475·H+0.525·L ≤ price ≤ 0.525·H+0.475·L
      Discount zone   : price ≤ 0.05·H + 0.95·L      ← bottom 5% of range

    Qualification logic (LONG trades only):
      • Discount zone                         → QUALIFIES  (greatest room to pump)
      • Equilibrium zone                      → REJECTED   (indecision, risky)
      • Premium zone                          → REJECTED   (no upward room)
      • Band A (equil_top < price < prem_bot) → QUALIFIES if room ≥ 3 %
      • Band B (disc_top  < price < equil_bot)→ QUALIFIES if room ≥ 3 %

    Returns (qualifies: bool, zone_label: str)
    """
    if not candles or len(candles) < 2:
        return True, "unknown"

    lookback = candles[-50:]
    H = max(c["high"] for c in lookback)
    L = min(c["low"]  for c in lookback)

    if H <= L:
        return True, "unknown"

    # Boundary levels  (exact Pine Script maths from DZSAFM)
    premium_bottom = 0.95 * H + 0.05 * L    # bottom edge of Premium zone
    equil_top      = 0.525 * H + 0.475 * L  # top edge of Equilibrium zone
    equil_bottom   = 0.475 * H + 0.525 * L  # bottom edge of Equilibrium zone
    discount_top   = 0.05  * H + 0.95  * L  # top edge of Discount zone

    if price <= discount_top:
        # Fully inside Discount zone — best long setup
        return True, "Discount"
    elif price >= premium_bottom:
        # Fully inside Premium zone — no upward room
        return False, "Premium"
    elif equil_bottom <= price <= equil_top:
        # Equilibrium band — can go either way, skip
        return False, "Equilibrium"
    elif equil_top < price < premium_bottom:
        # Band A: between Equilibrium top and Premium bottom
        room_pct = (premium_bottom - price) / price * 100
        label    = f"BandA(room:{room_pct:.1f}%)"
        return (room_pct >= 3.0), label
    else:
        # Band B: between Discount top and Equilibrium bottom
        room_pct = (equil_bottom - price) / price * 100
        label    = f"BandB(room:{room_pct:.1f}%)"
        return (room_pct >= 3.0), label


# ─────────────────────────────────────────────────────────────────────────────
# Filter funnel counter
# ─────────────────────────────────────────────────────────────────────────────
def _reset_filter_counts():
    counts = {
        "total_watchlist":  0,
        "pre_filtered_out": 0,   # removed by bulk ticker pre-filter (A)
        "checked":          0,   # entered deep-scan
        "f4_rsi5m":         0,
        "f7_rsi1h":         0,
        "f8_ema":           0,
        "f9_macd":          0,
        "f10_sar":          0,
        "f11_vol":          0,
        "f12_pdz":          0,
        "passed":           0,
        "errors":           0,
    }
    with _filter_lock:
        _filter_counts.clear()
        _filter_counts.update(counts)
    _b._bsc_filter_counts = _filter_counts

# ─────────────────────────────────────────────────────────────────────────────
# Per-coin processing  (B + C applied here)
# ─────────────────────────────────────────────────────────────────────────────
def process(sym, cfg: dict):
    try:
        with _filter_lock:
            _filter_counts["checked"] = _filter_counts.get("checked", 0) + 1

        # ── C — Stage 1: Quick RSI + Resistance check (50 candles, 1 API call)
        m5_quick    = get_klines(sym, "5m", 50)[:-1]
        closes_5m_q = [c["close"] for c in m5_quick]
        entry_q     = round(m5_quick[-1]["close"], 6)

        rsi5_q = (calc_rsi_series(closes_5m_q) or [0])[-1]
        if cfg.get("use_rsi_5m", True) and rsi5_q < cfg["rsi_5m_min"]:
            with _filter_lock: _filter_counts["f4_rsi5m"] = _filter_counts.get("f4_rsi5m",0)+1
            return None

        # ── B — Stage 2: Fetch all 4 timeframes IN PARALLEL (3 API calls saved)
        # 5m full needed for EMA (configurable period); 1h needs only 19 candles
        candle_limit_5m  = 210 if (cfg.get("use_ema_5m") or cfg.get("use_macd") or cfg.get("use_sar")) else 50
        candle_limit_15m = 210 if (cfg.get("use_ema_15m")or cfg.get("use_macd") or cfg.get("use_sar")) else 50
        candle_limit_3m  =  80 if (cfg.get("use_ema_3m") or cfg.get("use_macd") or cfg.get("use_sar")) else 30

        with ThreadPoolExecutor(max_workers=4) as pool:
            f_5m  = pool.submit(get_klines, sym, "5m",  candle_limit_5m)
            f_15m = pool.submit(get_klines, sym, "15m", candle_limit_15m)
            f_1h  = pool.submit(get_klines, sym, "1h",  19)
            f_3m  = pool.submit(get_klines, sym, "3m",  candle_limit_3m)
            m5          = f_5m.result()[:-1]
            m15         = f_15m.result()[:-1]
            m1h_candles = f_1h.result()[:-1]
            m3_candles  = f_3m.result()[:-1]

        closes_5m  = [c["close"] for c in m5]
        closes_15m = [c["close"] for c in m15]
        closes_3m  = [c["close"] for c in m3_candles]
        closes_1h  = [c["close"] for c in m1h_candles]
        entry      = round(m5[-1]["close"], 6)

        # ── F7: 1h RSI ────────────────────────────────────────────────────────
        rsi1h = (calc_rsi_series(closes_1h) or [0])[-1]
        if cfg.get("use_rsi_1h", True) and \
                not (cfg["rsi_1h_min"] <= rsi1h <= cfg["rsi_1h_max"]):
            with _filter_lock: _filter_counts["f7_rsi1h"] = _filter_counts.get("f7_rsi1h",0)+1
            return None

        # ── F8: EMA_Selection — capture actual EMA value at entry ─────────────
        ema_3m_val = ema_5m_val = ema_15m_val = None
        if cfg.get("use_ema_3m"):
            ema = calc_ema(closes_3m, max(2, int(cfg.get("ema_period_3m", 12))))
            if not ema or entry < ema[-1]:
                with _filter_lock: _filter_counts["f8_ema"] = _filter_counts.get("f8_ema",0)+1
                return None
            ema_3m_val = round(ema[-1], 6)
        if cfg.get("use_ema_5m"):
            ema = calc_ema(closes_5m, max(2, int(cfg.get("ema_period_5m", 12))))
            if not ema or entry < ema[-1]:
                with _filter_lock: _filter_counts["f8_ema"] = _filter_counts.get("f8_ema",0)+1
                return None
            ema_5m_val = round(ema[-1], 6)
        if cfg.get("use_ema_15m"):
            ema = calc_ema(closes_15m, max(2, int(cfg.get("ema_period_15m", 12))))
            if not ema or entry < ema[-1]:
                with _filter_lock: _filter_counts["f8_ema"] = _filter_counts.get("f8_ema",0)+1
                return None
            ema_15m_val = round(ema[-1], 6)

        # ── F9: MACD dark-green — capture MACD line values ───────────────────
        macd_3m_val = macd_5m_val = macd_15m_val = None
        if cfg.get("use_macd"):
            if not (macd_bullish(closes_3m) and
                    macd_bullish(closes_5m)  and
                    macd_bullish(closes_15m)):
                with _filter_lock: _filter_counts["f9_macd"] = _filter_counts.get("f9_macd",0)+1
                return None
            ml3,  _, _ = calc_macd(closes_3m)
            ml5,  _, _ = calc_macd(closes_5m)
            ml15, _, _ = calc_macd(closes_15m)
            if ml3:  macd_3m_val  = round(ml3[-1],  8)
            if ml5:  macd_5m_val  = round(ml5[-1],  8)
            if ml15: macd_15m_val = round(ml15[-1], 8)

        # ── F10: Parabolic SAR — capture SAR value at entry ──────────────────
        sar_3m_val = sar_5m_val = sar_15m_val = None
        if cfg.get("use_sar"):
            sar_3m  = calc_parabolic_sar(m3_candles)
            sar_5m  = calc_parabolic_sar(m5)
            sar_15m = calc_parabolic_sar(m15)
            if not (sar_3m  and sar_3m[-1][1]  and
                    sar_5m  and sar_5m[-1][1]  and
                    sar_15m and sar_15m[-1][1]):
                with _filter_lock: _filter_counts["f10_sar"] = _filter_counts.get("f10_sar",0)+1
                return None
            sar_3m_val  = round(sar_3m[-1][0],  6)
            sar_5m_val  = round(sar_5m[-1][0],  6)
            sar_15m_val = round(sar_15m[-1][0], 6)

        # ── F11: Volume spike — capture vol ratio ─────────────────────────────
        vol_ratio = None
        if cfg.get("use_vol_spike"):
            lookback = max(2, int(cfg.get("vol_spike_lookback", 20)))
            mult     = float(cfg.get("vol_spike_mult", 2.0))
            vols_15m = [c["volume"] for c in m15]
            if len(vols_15m) >= lookback + 1:
                window   = vols_15m[-(lookback+1):-1]
                avg_vol  = sum(window)/len(window)
                if avg_vol <= 0 or vols_15m[-1] < mult * avg_vol:
                    with _filter_lock: _filter_counts["f11_vol"] = _filter_counts.get("f11_vol",0)+1
                    return None
                vol_ratio = round(vols_15m[-1] / avg_vol, 2) if avg_vol > 0 else None

        # ── F12: PDZ — Premium / Discount / Equilibrium zone (5m) ───────────
        pdz_zone = "—"
        if cfg.get("use_pdz", True):
            pdz_pass, pdz_zone = calc_pdz_zone(m5, entry)
            if not pdz_pass:
                with _filter_lock:
                    _filter_counts["f12_pdz"] = _filter_counts.get("f12_pdz", 0) + 1
                return None

        tp      = round(entry * (1 + cfg["tp_pct"]/100), 6)
        sl      = round(entry * (1 - cfg["sl_pct"]/100), 6)
        sec     = SECTORS.get(sym, "Other")
        max_lev = get_max_leverage(sym)

        with _filter_lock:
            _filter_counts["passed"] = _filter_counts.get("passed",0)+1

        rsi5 = (calc_rsi_series(closes_5m) or [rsi5_q])[-1]   # use full-set RSI if available
        criteria = {
            "rsi_5m":    round(rsi5,  1),
            "rsi_1h":    round(rsi1h, 1),
            "ema_3m":    ema_3m_val  if cfg.get("use_ema_3m")    else "—",
            "ema_5m":    ema_5m_val  if cfg.get("use_ema_5m")    else "—",
            "ema_15m":   ema_15m_val if cfg.get("use_ema_15m")   else "—",
            "macd_3m":   macd_3m_val  if cfg.get("use_macd")     else "—",
            "macd_5m":   macd_5m_val  if cfg.get("use_macd")     else "—",
            "macd_15m":  macd_15m_val if cfg.get("use_macd")     else "—",
            "sar_3m":    sar_3m_val   if cfg.get("use_sar")      else "—",
            "sar_5m":    sar_5m_val   if cfg.get("use_sar")      else "—",
            "sar_15m":   sar_15m_val  if cfg.get("use_sar")      else "—",
            "vol_ratio": vol_ratio    if cfg.get("use_vol_spike") else "—",
            "pdz_zone":  pdz_zone     if cfg.get("use_pdz", True) else "—",
        }

        return {
            "id":          str(uuid.uuid4())[:8],
            "timestamp":   dubai_now().isoformat(),
            "symbol":      sym,
            "entry":       entry,
            "tp":          tp,
            "sl":          sl,
            "sector":      sec,
            "status":      "open",
            "close_price": None,
            "close_time":  None,
            "max_lev":     max_lev,
            "criteria":    criteria,
        }

    except Exception:
        with _filter_lock:
            _filter_counts["errors"] = _filter_counts.get("errors",0)+1
        return "error"

# ─────────────────────────────────────────────────────────────────────────────
# Scan orchestrator  (A + D wired here)
# ─────────────────────────────────────────────────────────────────────────────
def scan(cfg: dict):
    _reset_filter_counts()

    # D — cached symbol list (one instrument API call max every 6 h)
    symbols = get_symbols_cached(cfg["watchlist"])
    with _filter_lock:
        _filter_counts["total_watchlist"] = len(symbols)

    # A — bulk ticker pre-filter (1 API call → eliminates ~70 % of coins)
    tickers = get_bulk_tickers()
    if cfg.get("use_pre_filter", True):
        pre_filtered = pre_filter_by_ticker(symbols, tickers)
    else:
        pre_filtered = list(symbols)   # pre-filter disabled — deep-scan all
    with _filter_lock:
        _filter_counts["pre_filtered_out"] = len(symbols) - len(pre_filtered)

    print(f"[Scan] {len(symbols)} valid symbols → "
          f"{len(pre_filtered)} after bulk pre-filter "
          f"({'disabled' if not cfg.get('use_pre_filter', True) else _filter_counts['pre_filtered_out']} removed)")

    results = []
    # 6 outer workers (reduced from 8 to stay safely under rate limit with inner parallel fetches)
    with ThreadPoolExecutor(max_workers=6) as exe:
        futs = [exe.submit(process, s, cfg) for s in pre_filtered]
        for f in as_completed(futs):
            r = f.result()
            if r and r != "error":
                results.append(r)

    return sorted(results, key=lambda x: x["symbol"]), _filter_counts.get("errors", 0)

# ─────────────────────────────────────────────────────────────────────────────
# Open signal tracker
# ─────────────────────────────────────────────────────────────────────────────
def update_open_signals(signals):
    for sig in signals:
        if sig["status"] != "open": continue
        try:
            sig_ts_ms = int(datetime.fromisoformat(sig["timestamp"]).timestamp() * 1000)
            candles   = get_klines(sig["symbol"], "5m", 200)
            post      = [c for c in candles if c["time"] >= sig_ts_ms]
            tp_time = sl_time = None
            for c in post:
                if tp_time is None and c["high"] >= sig["tp"]: tp_time = c["time"]
                if sl_time is None and c["low"]  <= sig["sl"]: sl_time = c["time"]
            if tp_time is not None or sl_time is not None:
                if tp_time is not None and (sl_time is None or tp_time <= sl_time):
                    sig.update(status="tp_hit", close_price=sig["tp"],
                               close_time=to_dubai(datetime.fromtimestamp(
                                   tp_time/1000, tz=timezone.utc)).isoformat())
                else:
                    sig.update(status="sl_hit", close_price=sig["sl"],
                               close_time=to_dubai(datetime.fromtimestamp(
                                   sl_time/1000, tz=timezone.utc)).isoformat())
        except Exception:
            pass
    return signals

# ─────────────────────────────────────────────────────────────────────────────
# Background scanner thread
# ─────────────────────────────────────────────────────────────────────────────
def _bg_loop():
    while True:
        if not _scanner_running.is_set():
            time.sleep(2); continue
        with _config_lock:
            cfg = dict(_b._bsc_cfg)
        t0 = time.time()
        try:
            with _log_lock:
                _b._bsc_log["signals"] = update_open_signals(_b._bsc_log["signals"])
            new_sigs, errors = scan(cfg)
            cutoff = dubai_now() - timedelta(minutes=cfg["cooldown_minutes"])
            with _log_lock:
                cooled = {s["symbol"] for s in _b._bsc_log["signals"]
                          if datetime.fromisoformat(s["timestamp"].replace("Z","+00:00")) >= cutoff}
                active = {s["symbol"] for s in _b._bsc_log["signals"] if s["status"]=="open"}
                skip   = cooled | active
                for sig in new_sigs:
                    if sig["symbol"] not in skip:
                        _b._bsc_log["signals"].append(sig); skip.add(sig["symbol"])
                elapsed = time.time() - t0
                _b._bsc_log["health"].update(
                    total_cycles         = _b._bsc_log["health"].get("total_cycles",0)+1,
                    last_scan_at         = dubai_now().isoformat(),
                    last_scan_duration_s = round(elapsed, 1),
                    total_api_errors     = _b._bsc_log["health"].get("total_api_errors",0)+errors,
                    watchlist_size       = len(cfg["watchlist"]),
                    pre_filtered_out     = _filter_counts.get("pre_filtered_out",0),
                    deep_scanned         = _filter_counts.get("checked",0),
                )
                save_log(_b._bsc_log)
            _b._bsc_last_error = ""
        except Exception as e:
            _b._bsc_last_error = str(e)
        elapsed = time.time() - t0
        time.sleep(max(0, cfg["loop_minutes"]*60 - elapsed))

def _ensure_scanner():
    if _b._bsc_thread is None or not _b._bsc_thread.is_alive():
        t = threading.Thread(target=_bg_loop, daemon=True, name="okx-scanner")
        t.start(); _b._bsc_thread = t

# ─────────────────────────────────────────────────────────────────────────────
# STREAMLIT UI
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="OKX Futures Scanner v2", page_icon="🔍",
                   layout="wide", initial_sidebar_state="expanded")
_ensure_scanner()

with _log_lock:    _snap_log = json.loads(json.dumps(_b._bsc_log))
with _config_lock: _snap_cfg = dict(_b._bsc_cfg)

health  = _snap_log.get("health", {})
signals = _snap_log.get("signals", [])

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Configuration")
    running   = _scanner_running.is_set()
    btn_label = "⏹ Stop Scanner" if running else "▶️ Start Scanner"
    if st.button(btn_label, use_container_width=True, type="primary" if running else "secondary"):
        if running: _scanner_running.clear()
        else:       _scanner_running.set()
        st.rerun()
    st.caption(f"{'🟢' if running else '🔴'}  Scanner {'running' if running else 'stopped'}")
    st.divider()

    st.markdown("**📊 Trade Settings**")
    c1, c2 = st.columns(2)
    new_tp = c1.number_input("TP %", min_value=0.1, max_value=20.0, step=0.1, value=float(_snap_cfg["tp_pct"]),    key="cfg_tp")
    new_sl = c2.number_input("SL %", min_value=0.1, max_value=20.0, step=0.1, value=float(_snap_cfg["sl_pct"]),    key="cfg_sl")
    st.divider()

    # ── Pre-filter ──────────────────────────────────────────────────────────────
    new_use_pre_filter = st.checkbox(
        "⚡ Enable Bulk Pre-filter",
        value=bool(_snap_cfg.get("use_pre_filter", True)), key="cfg_use_pre_filter",
        help="One API call eliminates coins with low volume, big red candle, or near 24h low. "
             "Disable only to deep-scan ALL watchlist coins (much slower).")
    if new_use_pre_filter:
        st.caption("✅ Vol ≥ 100k USDT · Price ≥ 24h Low × 1.005")
    st.divider()

    # ── F4: 5m RSI ─────────────────────────────────────────────────────────────
    new_use_rsi_5m = st.checkbox("📈 Enable F4 — 5m RSI",
                                  value=bool(_snap_cfg.get("use_rsi_5m", True)), key="cfg_use_rsi_5m")
    new_rsi5_min = st.number_input("5m RSI min", min_value=0, max_value=100, step=1,
                                    value=int(_snap_cfg["rsi_5m_min"]), key="cfg_rsi5",
                                    disabled=not new_use_rsi_5m)
    st.divider()

    # ── F7: 1h RSI ─────────────────────────────────────────────────────────────
    new_use_rsi_1h = st.checkbox("📈 Enable F7 — 1h RSI",
                                  value=bool(_snap_cfg.get("use_rsi_1h", True)), key="cfg_use_rsi_1h")
    c3, c4 = st.columns(2)
    new_rsi1h_min = c3.number_input("1h min", min_value=0, max_value=100, step=1,
                                     value=int(_snap_cfg["rsi_1h_min"]), key="cfg_rsi1h_min",
                                     disabled=not new_use_rsi_1h)
    new_rsi1h_max = c4.number_input("1h max", min_value=0, max_value=100, step=1,
                                     value=int(_snap_cfg["rsi_1h_max"]), key="cfg_rsi1h_max",
                                     disabled=not new_use_rsi_1h)
    st.divider()

    # ── EMA_Selection ──────────────────────────────────────────────────────────
    st.markdown("**📉 EMA_Selection** (price must be above EMA)")
    ea1, ea2 = st.columns([1,2])
    new_use_ema_3m    = ea1.checkbox("3m EMA", value=bool(_snap_cfg.get("use_ema_3m",False)), key="cfg_use_ema_3m")
    new_ema_period_3m = ea2.number_input("P##3m", min_value=2, max_value=500, step=1, value=int(_snap_cfg.get("ema_period_3m",12)),
                                         key="cfg_ema_period_3m", disabled=not new_use_ema_3m,
                                         label_visibility="collapsed")
    eb1, eb2 = st.columns([1,2])
    new_use_ema_5m    = eb1.checkbox("5m EMA", value=bool(_snap_cfg.get("use_ema_5m",True)),  key="cfg_use_ema_5m")
    new_ema_period_5m = eb2.number_input("P##5m", min_value=2, max_value=500, step=1, value=int(_snap_cfg.get("ema_period_5m",12)),
                                         key="cfg_ema_period_5m", disabled=not new_use_ema_5m,
                                         label_visibility="collapsed")
    ec1, ec2 = st.columns([1,2])
    new_use_ema_15m    = ec1.checkbox("15m EMA", value=bool(_snap_cfg.get("use_ema_15m",True)), key="cfg_use_ema_15m")
    new_ema_period_15m = ec2.number_input("P##15m", min_value=2, max_value=500, step=1, value=int(_snap_cfg.get("ema_period_15m",12)),
                                          key="cfg_ema_period_15m", disabled=not new_use_ema_15m,
                                          label_visibility="collapsed")
    st.divider()

    # ── F9: MACD ───────────────────────────────────────────────────────────────
    st.markdown("**📊 F9 — MACD** (dark 🟢 histogram, 3m·5m·15m)")
    new_use_macd = st.checkbox("Enable MACD filter", value=bool(_snap_cfg.get("use_macd",True)), key="cfg_use_macd",
        help="MACD>0, Signal>0, Histogram>0 AND increasing (dark green only), crossover within 12 candles — on 3m·5m·15m")
    if new_use_macd: st.caption("✅ MACD>0 · Signal>0 · Histogram 🟢↑ · Crossover ↑")
    st.divider()

    # ── F10: SAR ───────────────────────────────────────────────────────────────
    st.markdown("**🪂 F10 — Parabolic SAR** (3m·5m·15m)")
    new_use_sar = st.checkbox("Enable SAR filter", value=bool(_snap_cfg.get("use_sar",True)), key="cfg_use_sar")
    if new_use_sar: st.caption("✅ SAR below price on 3m · 5m · 15m")
    st.divider()

    # ── F11: Volume Spike ──────────────────────────────────────────────────────
    st.markdown("**📦 F11 — Volume Spike** (15m)")
    new_use_vol_spike = st.checkbox("Enable vol spike", value=bool(_snap_cfg.get("use_vol_spike",False)), key="cfg_use_vol_spike")
    vx1, vx2 = st.columns(2)
    new_vol_mult     = vx1.number_input("Mult (X×)", min_value=1.0, max_value=20.0, step=0.5,
                                         value=float(_snap_cfg.get("vol_spike_mult",2.0)), key="cfg_vol_mult",
                                         disabled=not new_use_vol_spike)
    new_vol_lookback = vx2.number_input("Lookback (N)", min_value=2, max_value=100, step=1,
                                         value=int(_snap_cfg.get("vol_spike_lookback",20)), key="cfg_vol_lookback",
                                         disabled=not new_use_vol_spike)
    st.divider()

    # ── F12: PDZ — Premium / Discount / Equilibrium zones ──────────────────────
    st.markdown("**🎯 F12 — PDZ — Premium / Discount Zones** (5m)")
    new_use_pdz = st.checkbox(
        "Enable PDZ filter",
        value=bool(_snap_cfg.get("use_pdz", True)), key="cfg_use_pdz",
        help=(
            "Uses DZSAFM Smart Money zone logic on 5m candles.\n\n"
            "✅ Qualifies: Discount zone · BandA ≥3% room · BandB ≥3% room\n"
            "❌ Rejected:  Equilibrium zone · Premium zone"
        ))
    if new_use_pdz:
        st.caption("✅ Discount · BandA/B ≥3% room | ❌ Premium · Equilibrium")
    st.divider()

    st.markdown("**⏱ Execution**")
    c5, c6 = st.columns(2)
    new_loop = c5.number_input("Loop (min)", min_value=1, max_value=60, step=1, value=int(_snap_cfg["loop_minutes"]),    key="cfg_loop")
    new_cool = c6.number_input("Cooldown (min)", min_value=1, max_value=120, step=1, value=int(_snap_cfg["cooldown_minutes"]), key="cfg_cool")
    st.divider()

    st.markdown("**📋 Watchlist** (one symbol per line)")
    wl_text = st.text_area("wl", value="\n".join(_snap_cfg["watchlist"]),
                            height=180, label_visibility="collapsed", key="cfg_wl")
    st.divider()

    st.markdown("**🗑 Clear History**")
    if st.button("⚡ Flush All", use_container_width=True, type="secondary"):
        with _log_lock:
            _b._bsc_log["signals"] = []
            _b._bsc_log["health"]  = {"total_cycles":0,"last_scan_at":None,
                                       "last_scan_duration_s":0.0,"total_api_errors":0,
                                       "watchlist_size":0,"pre_filtered_out":0,"deep_scanned":0}
            save_log(_b._bsc_log)
        st.success("✅ Flushed"); st.rerun()

    cd1, cd2 = st.columns(2)
    if cd1.button("📅 Clear 24h", use_container_width=True):
        cutoff = dubai_now() - timedelta(hours=24)
        with _log_lock:
            before = len(_b._bsc_log["signals"])
            _b._bsc_log["signals"] = [s for s in _b._bsc_log["signals"]
                if datetime.fromisoformat(s["timestamp"].replace("Z","+00:00")) < cutoff]
            save_log(_b._bsc_log)
        st.success(f"✅ Removed {before-len(_b._bsc_log['signals'])}"); st.rerun()
    if cd2.button("📆 Clear 7d", use_container_width=True):
        cutoff = dubai_now() - timedelta(days=7)
        with _log_lock:
            before = len(_b._bsc_log["signals"])
            _b._bsc_log["signals"] = [s for s in _b._bsc_log["signals"]
                if datetime.fromisoformat(s["timestamp"].replace("Z","+00:00")) < cutoff]
            save_log(_b._bsc_log)
        st.success(f"✅ Removed {before-len(_b._bsc_log['signals'])}"); st.rerun()
    st.divider()

    if st.button("↩️ Reset Defaults", use_container_width=True, type="secondary"):
        d = dict(DEFAULT_CONFIG)
        with _config_lock: _b._bsc_cfg.clear(); _b._bsc_cfg.update(d)
        save_config(d)
        # Also invalidate symbol cache so it re-verifies on next scan
        _b._bsc_symbol_cache["fetched_at"] = 0
        st.success("✅ Reset"); st.rerun()

    if st.button("💾 Save & Apply", use_container_width=True, type="primary"):
        new_wl = [s.strip().upper() for s in wl_text.splitlines() if s.strip()]
        new_cfg = {
            "tp_pct": new_tp, "sl_pct": new_sl,
            # enable/disable flags
            "use_pre_filter":      bool(new_use_pre_filter),
            "use_rsi_5m":          bool(new_use_rsi_5m),
            "use_rsi_1h":          bool(new_use_rsi_1h),
            # filter parameters
            "rsi_5m_min": int(new_rsi5_min),
            "rsi_1h_min": int(new_rsi1h_min), "rsi_1h_max": int(new_rsi1h_max),
            "loop_minutes": int(new_loop), "cooldown_minutes": int(new_cool),
            "use_ema_3m": bool(new_use_ema_3m), "ema_period_3m": int(new_ema_period_3m),
            "use_ema_5m": bool(new_use_ema_5m), "ema_period_5m": int(new_ema_period_5m),
            "use_ema_15m": bool(new_use_ema_15m), "ema_period_15m": int(new_ema_period_15m),
            "use_macd": bool(new_use_macd), "use_sar": bool(new_use_sar),
            "use_vol_spike": bool(new_use_vol_spike),
            "vol_spike_mult": float(new_vol_mult), "vol_spike_lookback": int(new_vol_lookback),
            "use_pdz": bool(new_use_pdz),
            "watchlist": new_wl,
        }
        with _config_lock: _b._bsc_cfg.clear(); _b._bsc_cfg.update(new_cfg)
        save_config(new_cfg)
        # Invalidate symbol cache if watchlist changed
        if new_wl != _snap_cfg.get("watchlist", []):
            _b._bsc_symbol_cache["fetched_at"] = 0
        st.success(f"✅ Saved — {len(new_wl)} coins"); st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# MAIN AREA
# ─────────────────────────────────────────────────────────────────────────────
st.title("🔍 OKX Futures Scanner  v2")

last_scan = health.get("last_scan_at", "never")
if last_scan and last_scan != "never":
    try:
        ts       = datetime.fromisoformat(last_scan.replace("Z","+00:00"))
        ts_dubai = to_dubai(ts)
        ago      = int((dubai_now()-ts_dubai).total_seconds()/60)
        last_scan = f"{ago}m ago  ({ts_dubai.strftime('%H:%M')} GST)"
    except Exception: pass

col_h1, col_h2 = st.columns([3,1])
col_h1.caption(f"Last scan: {last_scan}   |   Auto-refresh every 30 s   |   🕐 Dubai / GST (UTC+4)")
if col_h2.button("🔄 Refresh", key="manual_refresh"): st.rerun()

# ── Health metrics ─────────────────────────────────────────────────────────────
open_count = sum(1 for s in signals if s["status"]=="open")
tp_count   = sum(1 for s in signals if s["status"]=="tp_hit")
sl_count   = sum(1 for s in signals if s["status"]=="sl_hit")
pre_out    = health.get("pre_filtered_out", 0)
deep_sc    = health.get("deep_scanned",     0)

m1,m2,m3,m4,m5c,m6,m7,m8 = st.columns(8)
m1.metric("Cycles",        health.get("total_cycles",0))
m2.metric("Scan Time",     f"{health.get('last_scan_duration_s',0)}s")
m3.metric("API Errors",    health.get("total_api_errors",0))
m4.metric("Pre-filtered ⚡", pre_out, help="Coins removed by bulk ticker pre-filter (saves API calls)")
m5c.metric("Deep Scanned", deep_sc, help="Coins that passed pre-filter and received full candle analysis")
m6.metric("Open",          open_count)
m7.metric("TP Hit ✅",     tp_count)
m8.metric("SL Hit ❌",     sl_count)

if getattr(_b, "_bsc_last_error", ""):
    st.warning(f"⚠️ {_b._bsc_last_error}")
st.divider()

# ── Active filter badges ───────────────────────────────────────────────────────
badges = []
if _snap_cfg.get("use_ema_3m"):    badges.append(f"📉 EMA{_snap_cfg.get('ema_period_3m',12)} 3m")
if _snap_cfg.get("use_ema_5m"):    badges.append(f"📉 EMA{_snap_cfg.get('ema_period_5m',12)} 5m")
if _snap_cfg.get("use_ema_15m"):   badges.append(f"📉 EMA{_snap_cfg.get('ema_period_15m',12)} 15m")
if _snap_cfg.get("use_macd"):      badges.append("📊 MACD 🟢↑ 3m·5m·15m")
if _snap_cfg.get("use_sar"):       badges.append("🪂 SAR 3m·5m·15m")
if _snap_cfg.get("use_vol_spike"): badges.append(
    f"📦 Vol ≥{_snap_cfg.get('vol_spike_mult',2.0)}× / {_snap_cfg.get('vol_spike_lookback',20)} 15m")
if _snap_cfg.get("use_pdz", True):  badges.append("🎯 PDZ Zones (5m)")
st.markdown("**Active Filters:**")
st.caption("  |  ".join(badges) if badges else "No advanced filters enabled")
st.divider()

# ── Sector filter ──────────────────────────────────────────────────────────────
all_sectors = ["All","BTC","L1","L2","DeFi","AI","Privacy","Meme","Gaming","Other"]
if "sector_filter" not in st.session_state: st.session_state["sector_filter"] = "All"
scols = st.columns(len(all_sectors))
for i, sec in enumerate(all_sectors):
    active = st.session_state["sector_filter"] == sec
    if scols[i].button(sec, key=f"sec_{sec}", type="primary" if active else "secondary",
                        use_container_width=True):
        st.session_state["sector_filter"] = sec; st.rerun()
selected_sector = st.session_state["sector_filter"]

# ── Signals table ──────────────────────────────────────────────────────────────
filtered        = signals if selected_sector=="All" else \
                  [s for s in signals if s.get("sector")==selected_sector]
filtered_sorted = sorted(filtered, key=lambda x: x.get("timestamp",""), reverse=True)
st.markdown(f"### Signals ({len(filtered_sorted)} shown)")

if filtered_sorted:
    rows = []
    for s in filtered_sorted:
        status      = s.get("status","open")
        status_icon = {"open":"🔵 Open","tp_hit":"✅ TP Hit","sl_hit":"❌ SL Hit"}.get(status,status)
        ts_str      = fmt_dubai(s.get("timestamp",""))
        close_str   = fmt_dubai(s["close_time"]) if s.get("close_time") else "—"
        crit        = s.get("criteria",{})
        def _cv(v):
            """Format a criteria value: show number if numeric, else show as-is."""
            if v is None or v == "—" or v == "✅": return "—"
            try: return f"{float(v):.6f}".rstrip("0").rstrip(".")
            except (TypeError, ValueError): return str(v)
        pdz_val  = crit.get("pdz_zone", "—") or "—"
        crit_str    = (
            f"• RSI 5m    : {crit.get('rsi_5m','—')}\n"
            f"• RSI 1h    : {crit.get('rsi_1h','—')}\n"
            f"• EMA 3m    : {_cv(crit.get('ema_3m','—'))}\n"
            f"• EMA 5m    : {_cv(crit.get('ema_5m','—'))}\n"
            f"• EMA 15m   : {_cv(crit.get('ema_15m','—'))}\n"
            f"• MACD 3m   : {_cv(crit.get('macd_3m'))}\n"
            f"• MACD 5m   : {_cv(crit.get('macd_5m'))}\n"
            f"• MACD 15m  : {_cv(crit.get('macd_15m'))}\n"
            f"• SAR 3m    : {_cv(crit.get('sar_3m'))}\n"
            f"• SAR 5m    : {_cv(crit.get('sar_5m'))}\n"
            f"• SAR 15m   : {_cv(crit.get('sar_15m'))}\n"
            f"• Vol ×avg  : {_cv(crit.get('vol_ratio'))}\n"
            f"• PDZ Zone  : {pdz_val}"
        ) if crit else "—"
        max_lev   = s.get("max_lev", get_max_leverage(s.get("symbol","")))
        sl_reason = analyze_sl_reason(s) if status=="sl_hit" else "—"
        rows.append({
            "Time (GST)":     ts_str,
            "Symbol":         s.get("symbol",""),
            "Sector":         s.get("sector","Other"),
            "Entry":          s.get("entry",""),
            "TP":             s.get("tp",""),
            "SL":             s.get("sl",""),
            "Status":         status_icon,
            "Close Time":     close_str,
            "Close $":        s.get("close_price") or "—",
            "Max Lev":        f"{max_lev}×",
            "Entry Criteria": crit_str,
            "⚠️ SL Reason":  sl_reason,
        })
    st.dataframe(rows, use_container_width=True, hide_index=True,
                 column_config={
                     "Entry":          st.column_config.NumberColumn(format="%.6f"),
                     "TP":             st.column_config.NumberColumn(format="%.6f"),
                     "SL":             st.column_config.NumberColumn(format="%.6f"),
                     "Entry Criteria": st.column_config.TextColumn(width="medium"),
                     "⚠️ SL Reason":  st.column_config.TextColumn(width="medium"),
                     "Max Lev":        st.column_config.TextColumn(width="small"),
                     "Close Time":     st.column_config.TextColumn(width="small"),
                 })
else:
    st.info("No signals yet — scanner runs every few minutes.")

# ── Charts ─────────────────────────────────────────────────────────────────────
if signals:
    st.divider()
    ch1, ch2 = st.columns(2)
    sec_counts: dict = {}
    for s in signals:
        k = s.get("sector","Other"); sec_counts[k] = sec_counts.get(k,0)+1
    ch1.plotly_chart(go.Figure(go.Pie(
        labels=list(sec_counts.keys()), values=list(sec_counts.values()),
        hole=0.4, marker=dict(colors=px.colors.qualitative.Dark24)
    )).update_layout(title="Signals by Sector", paper_bgcolor="rgba(0,0,0,0)",
                     plot_bgcolor="rgba(0,0,0,0)", font=dict(color="#e6edf3"),
                     margin=dict(t=40,b=10,l=10,r=10)),
                     use_container_width=True)
    outcome = {"Open":open_count,"TP Hit":tp_count,"SL Hit":sl_count}
    ch2.plotly_chart(go.Figure(go.Bar(
        x=list(outcome.keys()), y=list(outcome.values()),
        marker_color=["#58a6ff","#3fb950","#f85149"],
        text=list(outcome.values()), textposition="outside"
    )).update_layout(title="Signal Outcomes", paper_bgcolor="rgba(0,0,0,0)",
                     plot_bgcolor="rgba(0,0,0,0)", font=dict(color="#e6edf3"),
                     yaxis=dict(gridcolor="#21262d"), margin=dict(t=40,b=10,l=10,r=10)),
                     use_container_width=True)

    if len(signals) > 1:
        from collections import Counter
        dc: Counter = Counter()
        for s in signals:
            try:
                d = to_dubai(datetime.fromisoformat(s["timestamp"].replace("Z","+00:00"))).strftime("%m/%d")
                dc[d] += 1
            except Exception: pass
        if dc:
            days = sorted(dc.keys())
            st.plotly_chart(go.Figure(go.Bar(
                x=days, y=[dc[d] for d in days],
                marker_color="#d29922", text=[dc[d] for d in days], textposition="outside"
            )).update_layout(title="Signals Per Day (Dubai/GST)", paper_bgcolor="rgba(0,0,0,0)",
                             plot_bgcolor="rgba(0,0,0,0)", font=dict(color="#e6edf3"),
                             yaxis=dict(gridcolor="#21262d"), margin=dict(t=40,b=10,l=10,r=10)),
                             use_container_width=True)

# ── Filter funnel ──────────────────────────────────────────────────────────────
fc = dict(_filter_counts)
if fc.get("total_watchlist", 0) > 0:
    with st.expander("🔬 Last scan filter funnel"):
        total     = fc.get("total_watchlist", 0)
        pre_out_n = fc.get("pre_filtered_out", 0)
        after_pre = total - pre_out_n
        checked   = fc.get("checked", after_pre)
        after_f4  = checked   - fc.get("f4_rsi5m",0)
        after_f7  = after_f4  - fc.get("f7_rsi1h",0)
        after_f8  = after_f7  - fc.get("f8_ema",0)
        after_f9  = after_f8  - fc.get("f9_macd",0)
        after_f10 = after_f9  - fc.get("f10_sar",0)
        after_f11 = after_f10 - fc.get("f11_vol",0)
        after_f12 = after_f11 - fc.get("f12_pdz",0)

        def _lbl(enabled, on_str, off_str="off"):
            return on_str if enabled else f"{on_str.split('—')[0].strip()} ({off_str})"

        pre_lbl  = "⚡ After Bulk Pre-filter" if _snap_cfg.get("use_pre_filter", True) else "⚡ Pre-filter (disabled)"
        f4_lbl   = f"F4 — 5m RSI ≥{_snap_cfg.get('rsi_5m_min',30)}" if _snap_cfg.get("use_rsi_5m", True) else "F4 — 5m RSI (off)"
        f7_lbl   = (f"F7 — 1h RSI {_snap_cfg.get('rsi_1h_min',30)}–{_snap_cfg.get('rsi_1h_max',95)}"
                    if _snap_cfg.get("use_rsi_1h", True) else "F7 — 1h RSI (off)")
        ema_parts = []
        if _snap_cfg.get("use_ema_3m"):  ema_parts.append(f"3m EMA{_snap_cfg.get('ema_period_3m',12)}")
        if _snap_cfg.get("use_ema_5m"):  ema_parts.append(f"5m EMA{_snap_cfg.get('ema_period_5m',12)}")
        if _snap_cfg.get("use_ema_15m"): ema_parts.append(f"15m EMA{_snap_cfg.get('ema_period_15m',12)}")
        ema_lbl  = ("EMA_Selection ("+(" · ".join(ema_parts))+")") if ema_parts else "EMA_Selection (off)"
        macd_lbl = "F9 MACD 🟢↑ 3m·5m·15m"  if _snap_cfg.get("use_macd") else "F9 MACD (off)"
        sar_lbl  = "F10 SAR 3m·5m·15m"       if _snap_cfg.get("use_sar")  else "F10 SAR (off)"
        vol_lbl  = (f"F11 Vol ≥{_snap_cfg.get('vol_spike_mult',2.0)}× / {_snap_cfg.get('vol_spike_lookback',20)} 15m"
                    if _snap_cfg.get("use_vol_spike") else "F11 Vol (off)")
        pdz_lbl  = "F12 PDZ Zones (5m)" if _snap_cfg.get("use_pdz", True) else "F12 PDZ (off)"

        funnel_data = [
            (f"Watchlist ({total})",  total),
            (pre_lbl,                 after_pre),
            ("Entered Deep Scan",     checked),
            (f"After {f4_lbl}",       after_f4),
            (f"After {f7_lbl}",       after_f7),
            (f"After {ema_lbl}",      after_f8),
            (f"After {macd_lbl}",     after_f9),
            (f"After {sar_lbl}",      after_f10),
            (f"After {vol_lbl}",      after_f11),
            (f"After {pdz_lbl}",      fc.get("passed",0)),
        ]
        fig_funnel = go.Figure(go.Funnel(
            y=[d[0] for d in funnel_data],
            x=[d[1] for d in funnel_data],
            marker=dict(color=["#1f6feb","#388bfd","#58a6ff","#79c0ff","#a5d6ff",
                                "#3fb950","#56d364","#d29922","#e3b341","#f85149"]),
            textinfo="value+percent initial",
        ))
        fig_funnel.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e6edf3"), margin=dict(t=10,b=10,l=10,r=10), height=500)
        st.plotly_chart(fig_funnel, use_container_width=True)
        col_a, col_b, col_c = st.columns(3)
        col_a.metric("Pre-filtered out ⚡", pre_out_n, help="Removed cheaply — no candle API calls used")
        col_b.metric("Deep scanned 🔬",     checked,   help="Received full multi-timeframe candle analysis")
        col_c.metric("Errors",              fc.get("errors",0))

# ─────────────────────────────────────────────────────────────────────────────
# Auto-refresh
# ─────────────────────────────────────────────────────────────────────────────
time.sleep(30)
st.rerun()
