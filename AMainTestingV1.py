#!/usr/bin/env python3
"""
Binance Futures Scanner — Streamlit Dashboard
Deploy free at share.streamlit.io  (connect GitHub repo, select this file)
"""

import json, os, pathlib, threading, time, uuid, traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

# ─────────────────────────────────────────────────────────────────────────────
# Network constants  (OKX — cloud-friendly, no datacenter IP blocks)
# ─────────────────────────────────────────────────────────────────────────────
BASE         = "https://www.okx.com"
LOOKBACK_30M = 300    # OKX max 300 per request; 300 × 30m = 6.25 days, plenty for avg vol

# OKX interval strings
OKX_INTERVALS = {"30m": "30m", "3m": "3m", "5m": "5m", "15m": "15m", "1h": "1H"}

# ─────────────────────────────────────────────────────────────────────────────
# Default configuration
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_CONFIG: dict = {
    "tp_pct":             2.0,
    "sl_pct":             1.0,
    "rsi_5m_min":         45,
    "resistance_tol_pct": 1.5,
    "rsi_1h_min":         40,
    "rsi_1h_max":         72,
    "loop_minutes":       3,
    "cooldown_minutes":   30,
    # ── EMA per-timeframe ────────────────────────────────────────────────────
    "use_ema_3m":         False,
    "ema_period_3m":      200,
    "use_ema_5m":         True,
    "ema_period_5m":      200,
    "use_ema_15m":        True,
    "ema_period_15m":     200,
    # ── MACD ─────────────────────────────────────────────────────────────────
    "use_macd":           True,
    # ── Parabolic SAR ────────────────────────────────────────────────────────
    "use_sar":            True,
    # ── Volume spike ─────────────────────────────────────────────────────────
    "use_vol_spike":      False,
    "vol_spike_mult":     2.0,
    "vol_spike_lookback": 20,
    "watchlist": [
        "PTBUSDT","SANTOSUSDT","XRPUSDT","HEMIUSDT","OGUSDT","SIRENUSDT",
        "BANUSDT","BASUSDT","4USDT","MAGMAUSDT","XANUSDT","TRIAUSDT",
        "JELLYJELLYUSDT","STABLEUSDT","ROBOUSDT","POWERUSDT","XNYUSDT",
        "1000RATSUSDT","BEATUSDT","QUSDT","LYNUSDT","RIVERUSDT","RAVEUSDT",
        "FARTCOINUSDT","DEGOUSDT","BULLAUSDT","CFGUSDT","COMPUSDT","CCUSDT",
        "ELSAUSDT","MYXUSDT","FORMUSDT","CYSUSDT","BTRUSDT","FHEUSDT",
        "NIGHTUSDT","THEUSDT","UAIUSDT","GRASSUSDT","HUMAUSDT","AINUSDT",
        "ATHUSDT","FOLKSUSDT","BABYUSDT","AIAUSDT","SUPERUSDT","KATUSDT",
        "HUSDT","SIGNUSDT","BARDUSDT","ANIMEUSDT","BCHUSDT","PIXELUSDT",
        "ZENUSDT","DASHUSDT","DEXEUSDT","BREVUSDT","FOGOUSDT","RESOLVUSDT",
        "POLYXUSDT","FIGHTUSDT","ORCAUSDT","SKRUSDT","ZECUSDT","TRADOORUSDT",
        "KAITOUSDT","LPTUSDT","ETHFIUSDT","RPLUSDT","BTCDOMUSDT","DYDXUSDT",
        "FRAXUSDT","OGNUSDT","PARTIUSDT","ONDOUSDT","AIOUSDT","KASUSDT",
        "ARUSDT","EIGENUSDT","CHZUSDT","BIOUSDT","TRUMPUSDT","KAVAUSDT",
        "SNXUSDT","API3USDT","AVNTUSDT","ENAUSDT","BIRBUSDT","ZKCUSDT",
        "GIGGLEUSDT","KITEUSDT","PEOPLEUSDT","ATOMUSDT","PLAYUSDT","BNBUSDT",
        "SENTUSDT","HYPEUSDT","HOLOUSDT","C98USDT","CLANKERUSDT","GPSUSDT",
        "KNCUSDT","BERAUSDT","ICPUSDT","SAHARAUSDT","TRBUSDT","MONUSDT",
        "PLUMEUSDT","ENSOUSDT","JTOUSDT","AEROUSDT","LIGHTUSDT","FFUSDT",
        "XTZUSDT","SOMIUSDT","1000LUNCUSDT","COSUSDT","TAUSDT","BTCUSDT",
        "STGUSDT","VANAUSDT","MERLUSDT","JCTUSDT","OPUSDT","PIEVERSEUSDT",
        "FLOWUSDT","AXLUSDT","FUSDT","TRXUSDT","YGGUSDT","AZTECUSDT",
        "AWEUSDT","ESPUSDT","STXUSDT","LTCUSDT","DOGEUSDT","XMRUSDT",
        "VANRYUSDT","IMXUSDT","BANANAS31USDT","PROVEUSDT","ASTERUSDT",
        "OPNUSDT","ORDIUSDT","WLDUSDT","TONUSDT","INJUSDT","ETCUSDT",
        "ZKUSDT","CYBERUSDT","GUSDT","OPENUSDT","AUCTIONUSDT","CAKEUSDT",
        "AIXBTUSDT","CFXUSDT","LINEAUSDT","ZKPUSDT","JASMYUSDT","QNTUSDT",
        "MIRAUSDT","LAUSDT","MORPHOUSDT","LUNA2USDT","1000PEPEUSDT","NEARUSDT",
        "ONUSDT","1000FLOKIUSDT","MEMEUSDT","LAYERUSDT","AAVEUSDT","STRKUSDT",
        "FILUSDT","STEEMUSDT","1000BONKUSDT","SPACEUSDT","PHAUSDT","ETHUSDT",
        "TURBOUSDT","ICNTUSDT","XAUTUSDT","DOTUSDT","SOLUSDT","JUPUSDT",
        "PAXGUSDT","PENGUUSDT","ANKRUSDT","MANTRAUSDT","TNSRUSDT","WLFIUSDT",
        "PUMPUSDT","PYTHUSDT","AXSUSDT","GRTUSDT","ENSUSDT","1000SHIBUSDT",
        "PENDLEUSDT","CRVUSDT","ZILUSDT","MOODENGUSDT","TIAUSDT","ACXUSDT",
        "1INCHUSDT","ARBUSDT","ZORAUSDT","IPUSDT","HBARUSDT","ROSEUSDT",
        "GALAUSDT","COLLECTUSDT","MEUSDT","APTUSDT","SPXUSDT","AKTUSDT",
        "INXUSDT","USELESSUSDT","VIRTUALUSDT","WAXPUSDT","BOMEUSDT","SKYAIUSDT",
        "NEIROUSDT","LDOUSDT","METUSDT","EDGEUSDT","WETUSDT","VETUSDT",
        "XLMUSDT","0GUSDT","LINKUSDT","DUSKUSDT","UNIUSDT","SUIUSDT",
        "ACUUSDT","GUNUSDT","SKYUSDT","SYRUPUSDT","SANDUSDT","ALLOUSDT",
        "ZAMAUSDT","PNUTUSDT","GASUSDT","LITUSDT","ADAUSDT","AVAXUSDT",
        "NEOUSDT","POLUSDT","RENDERUSDT","WIFUSDT","FETUSDT","WUSDT",
        "REZUSDT","HANAUSDT","MOVEUSDT","MANAUSDT","ARCUSDT","MUSDT",
        "INITUSDT","ENJUSDT","DENTUSDT","ALICEUSDT","TAOUSDT","APEUSDT",
        "AGLDUSDT","ATUSDT","VVVUSDT","DEEPUSDT","ARKMUSDT","SYNUSDT",
        "BSBUSDT","TAKEUSDT","ZROUSDT","EULUSDT","SEIUSDT","BLUAIUSDT",
        "APRUSDT","BRUSDT","ALGOUSDT","SUSDT","NAORISUSDT","XPLUSDT",
        "KERNELUSDT","CUSDT","GUAUSDT","PIPPINUSDT","GWEIUSDT","CLOUSDT",
        "ARIAUSDT","NOMUSDT","ONTUSDT","STOUSDT",
    ],
}

# ─────────────────────────────────────────────────────────────────────────────
# Sector tags
# ─────────────────────────────────────────────────────────────────────────────
SECTORS: dict = {
    "FETUSDT":"AI","RENDERUSDT":"AI","AIXBTUSDT":"AI","GRTUSDT":"AI",
    "AGLDUSDT":"AI","AIAUSDT":"AI","AINUSDT":"AI","UAIUSDT":"AI",
    "ARKMUSDT":"AI","VIRTUALUSDT":"AI","SKYAIUSDT":"AI",
    "ZECUSDT":"Privacy","DASHUSDT":"Privacy","XMRUSDT":"Privacy",
    "DUSKUSDT":"Privacy","PHAUSDT":"Privacy","POLYXUSDT":"Privacy",
    "BTCUSDT":"BTC","BTCDOMUSDT":"BTC","ORDIUSDT":"BTC",
    "ETHUSDT":"L1","SOLUSDT":"L1","AVAXUSDT":"L1","ADAUSDT":"L1",
    "DOTUSDT":"L1","NEARUSDT":"L1","APTUSDT":"L1","SUIUSDT":"L1",
    "TONUSDT":"L1","XLMUSDT":"L1","TRXUSDT":"L1","LTCUSDT":"L1",
    "BCHUSDT":"L1","XRPUSDT":"L1","BNBUSDT":"L1","ATOMUSDT":"L1",
    "ARBUSDT":"L2","OPUSDT":"L2","STRKUSDT":"L2","ZKUSDT":"L2",
    "LINEAUSDT":"L2","ZKPUSDT":"L2","POLUSDT":"L2","IMXUSDT":"L2",
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
    _SCRIPT_DIR = pathlib.Path(tempfile.gettempdir()) / "binance_scanner"

_SCRIPT_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_FILE = _SCRIPT_DIR / "scanner_config.json"
LOG_FILE    = _SCRIPT_DIR / "scanner_log.json"

_config_lock = threading.Lock()

def load_config() -> dict:
    if CONFIG_FILE.exists():
        try:
            saved = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
            cfg = dict(DEFAULT_CONFIG)
            # Only copy keys that still exist in DEFAULT_CONFIG (drops stale keys)
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

# ─────────────────────────────────────────────────────────────────────────────
# Log persistence
# ─────────────────────────────────────────────────────────────────────────────
def load_log():
    if LOG_FILE.exists():
        try: return json.loads(LOG_FILE.read_text(encoding="utf-8"))
        except Exception: pass
    return {"health": {"total_cycles": 0, "last_scan_at": None,
                        "last_scan_duration_s": 0.0, "total_api_errors": 0,
                        "watchlist_size": 0},
            "signals": []}

def save_log(log):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOG_FILE.write_text(json.dumps(log, indent=2), encoding="utf-8")

# ─────────────────────────────────────────────────────────────────────────────
# Module-level shared state  (persists across Streamlit reruns in same process)
# ─────────────────────────────────────────────────────────────────────────────
if "_scanner_initialised" not in st.session_state:
    import builtins
    if not getattr(builtins, "_binance_scanner_globals_set", False):
        import builtins as _b
        _b._binance_scanner_globals_set = True
        _b._bsc_cfg            = load_config()
        _b._bsc_log            = load_log()
        _b._bsc_log_lock       = threading.Lock()
        _b._bsc_running        = threading.Event()
        _b._bsc_running.set()
        _b._bsc_thread         = None
        _b._bsc_filter_counts  = {}
        _b._bsc_filter_lock    = threading.Lock()
        _b._bsc_last_error     = ""
    st.session_state["_scanner_initialised"] = True

import builtins as _b
_cfg           = _b._bsc_cfg
_log           = _b._bsc_log
_log_lock      = _b._bsc_log_lock
_scanner_running = _b._bsc_running
_filter_lock   = _b._bsc_filter_lock
_filter_counts = _b._bsc_filter_counts

# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers
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
            r = get_session().get(url, params=params, timeout=20)
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", 60))); continue
            if r.status_code in (418, 403, 451):
                raise RuntimeError(
                    f"HTTP {r.status_code}: Exchange is blocking this server's IP. "
                    "Deploy on Railway (railway.app) instead — uses non-datacenter IPs."
                )
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "code" in data and data["code"] != "0":
                raise RuntimeError(f"OKX API error {data['code']}: {data.get('msg', '')}")
            return data
        except requests.exceptions.ConnectionError:
            if attempt < _retries - 1: time.sleep(5); continue
            raise
    raise RuntimeError(f"Failed after {_retries} retries: {url}")

# ─────────────────────────────────────────────────────────────────────────────
# OKX data helpers
# ─────────────────────────────────────────────────────────────────────────────
def _to_okx(sym: str) -> str:
    """BTCUSDT  →  BTC-USDT-SWAP"""
    return f"{sym[:-4]}-USDT-SWAP" if sym.endswith("USDT") else sym

def _from_okx(inst_id: str) -> str:
    """BTC-USDT-SWAP  →  BTCUSDT"""
    return inst_id.replace("-USDT-SWAP", "USDT") if inst_id.endswith("-USDT-SWAP") else inst_id

def get_symbols(watchlist: list) -> list:
    """Return watchlist symbols that are live USDT linear perps on OKX."""
    active = set()
    data   = safe_get(f"{BASE}/api/v5/public/instruments", {"instType": "SWAP"})
    for s in data.get("data", []):
        inst_id = s.get("instId", "")
        if inst_id.endswith("-USDT-SWAP") and s.get("state") == "live":
            active.add(_from_okx(inst_id))
    skipped = len([s for s in watchlist if s not in active])
    if skipped:
        print(f"  [{skipped} watchlist symbol(s) not trading on OKX — skipped]")
    return [s for s in watchlist if s in active]

def get_klines(sym: str, interval: str, limit: int) -> list:
    """Fetch OHLCV candles from OKX in ascending time order.
    OKX max = 300 per request, so we paginate when limit > 300.
    """
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
        if not bars:
            break
        all_bars.extend(bars)
        after = bars[-1][0]
        if len(bars) < batch:
            break

    # OKX returns newest-first; reverse to oldest-first
    all_bars.reverse()
    return [{"time":   int(b[0]),
             "open":   float(b[1]),
             "high":   float(b[2]),
             "low":    float(b[3]),
             "close":  float(b[4]),
             "volume": float(b[5])}
            for b in all_bars]

# ─────────────────────────────────────────────────────────────────────────────
# Technical helpers
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
        ag = (ag * (period-1) + gains[i]) / period
        al = (al * (period-1) + losses[i]) / period
        rsi.append(100. if al == 0 else 100 - 100 / (1 + ag / al))
    return rsi

def find_swing_highs(candles, neighbors=2):
    highs = [c["high"] for c in candles]
    peaks = []
    for i in range(neighbors, len(highs) - neighbors):
        if (all(highs[i] > highs[i-j] for j in range(1, neighbors+1)) and
                all(highs[i] > highs[i+j] for j in range(1, neighbors+1))):
            peaks.append(highs[i])
    return peaks

def is_near_resistance(entry, peaks, tolerance):
    return any(entry <= p <= entry * (1 + tolerance) for p in peaks)

# ─────────────────────────────────────────────────────────────────────────────
# EMA / MACD helpers
# ─────────────────────────────────────────────────────────────────────────────
def calc_ema(values: list, period: int) -> list:
    """Exponential Moving Average. Returns len(values)-period+1 values (seed = SMA)."""
    if len(values) < period:
        return []
    k = 2.0 / (period + 1)
    result = [sum(values[:period]) / period]
    for v in values[period:]:
        result.append(v * k + result[-1] * (1 - k))
    return result

def calc_macd(closes: list, fast: int = 12, slow: int = 26,
              signal_period: int = 9):
    """
    Returns (macd_line, signal_line, histogram) — all same length.
    Returns ([], [], []) when there is insufficient data.
    """
    if len(closes) < slow + signal_period:
        return [], [], []
    ema_f = calc_ema(closes, fast)
    ema_s = calc_ema(closes, slow)
    trim = len(ema_f) - len(ema_s)
    ema_f = ema_f[trim:]
    macd_line = [f - s for f, s in zip(ema_f, ema_s)]
    if len(macd_line) < signal_period:
        return [], [], []
    sig_line = calc_ema(macd_line, signal_period)
    trim2 = len(macd_line) - len(sig_line)
    macd_aligned = macd_line[trim2:]
    histogram = [m - s for m, s in zip(macd_aligned, sig_line)]
    return macd_aligned, sig_line, histogram

def macd_bullish(closes: list, crossover_lookback: int = 12) -> bool:
    """
    Returns True when ALL of the following hold on the given closes:
      1. MACD line  > 0  (above zero — bullish territory)
      2. Signal line > 0  (signal in positive zone)
      3. Histogram  > 0  (MACD above signal — momentum building)
      4. A bullish crossover (MACD crossed above signal) within last
         `crossover_lookback` candles.
    """
    macd_line, sig_line, histogram = calc_macd(closes)
    if not histogram:
        return False
    # Conditions 1-3: all three components must be positive
    if macd_line[-1] <= 0 or sig_line[-1] <= 0 or histogram[-1] <= 0:
        return False
    # Condition 4: scan for a recent bullish crossover
    n = min(crossover_lookback + 1, len(macd_line))
    for i in range(1, n):
        prev = -(i + 1)
        curr = -i
        if (len(macd_line) + prev >= 0 and
                macd_line[prev] <= sig_line[prev] and
                macd_line[curr] >  sig_line[curr]):
            return True
    return False

# ─────────────────────────────────────────────────────────────────────────────
# Parabolic SAR helper
# ─────────────────────────────────────────────────────────────────────────────
def calc_parabolic_sar(candles: list, af_start: float = 0.02,
                       af_step: float = 0.02, af_max: float = 0.20) -> list:
    """
    Computes Parabolic SAR for every candle.
    Returns a list of (sar_value, is_bullish) tuples, same length as candles.
    is_bullish=True  →  SAR is *below* price (uptrend / bullish momentum).
    """
    if not candles:
        return []
    if len(candles) < 2:
        return [(candles[0]["close"], True)]

    highs  = [c["high"]  for c in candles]
    lows   = [c["low"]   for c in candles]
    closes = [c["close"] for c in candles]

    bullish = closes[1] >= closes[0]
    ep      = highs[0] if bullish else lows[0]
    sar     = lows[0]  if bullish else highs[0]
    af      = af_start
    result  = [(sar, bullish)]

    for i in range(1, len(candles)):
        new_sar = sar + af * (ep - sar)

        if bullish:
            new_sar = min(new_sar, lows[i - 1])
            if i >= 2:
                new_sar = min(new_sar, lows[i - 2])
            if lows[i] < new_sar:
                bullish = False
                new_sar = ep
                ep      = lows[i]
                af      = af_start
            else:
                if highs[i] > ep:
                    ep = highs[i]
                    af = min(af + af_step, af_max)
        else:
            new_sar = max(new_sar, highs[i - 1])
            if i >= 2:
                new_sar = max(new_sar, highs[i - 2])
            if highs[i] > new_sar:
                bullish = True
                new_sar = ep
                ep      = highs[i]
                af      = af_start
            else:
                if lows[i] < ep:
                    ep = lows[i]
                    af = min(af + af_step, af_max)

        sar = new_sar
        result.append((sar, bullish))

    return result

# ─────────────────────────────────────────────────────────────────────────────
# Core filter logic
# ─────────────────────────────────────────────────────────────────────────────
def _reset_filter_counts():
    global _filter_counts
    counts = {
        "checked":    0,
        "f4_rsi5m":   0,
        "f5_res5m":   0,
        "f6_res15m":  0,
        "f7_rsi1h":   0,
        "f8_ema":     0,
        "f9_macd":    0,
        "f10_sar":    0,
        "f11_vol":    0,
        "passed":     0,
        "errors":     0,
    }
    with _filter_lock:
        _filter_counts.clear()
        _filter_counts.update(counts)
    _b._bsc_filter_counts = _filter_counts

def process(sym, cfg: dict):
    try:
        with _filter_lock: _filter_counts["checked"] = _filter_counts.get("checked", 0) + 1

        # ── Fetch 5m candles (210 for EMA, MACD, SAR) ────────────────────────
        m5            = get_klines(sym, "5m", 210)[:-1]
        current_price = m5[-1]["close"]
        closes_5m     = [c["close"] for c in m5]

        rsi5 = (calc_rsi_series(closes_5m) or [0])[-1]
        if rsi5 < cfg["rsi_5m_min"]:
            with _filter_lock: _filter_counts["f4_rsi5m"] = _filter_counts.get("f4_rsi5m", 0) + 1
            return None

        entry = round(current_price, 6)
        tol   = cfg["resistance_tol_pct"] / 100

        peaks_5m = find_swing_highs(m5[-25:], neighbors=2)
        if is_near_resistance(entry, peaks_5m, tol):
            with _filter_lock: _filter_counts["f5_res5m"] = _filter_counts.get("f5_res5m", 0) + 1
            return None

        # ── Fetch 15m candles ────────────────────────────────────────────────
        m15        = get_klines(sym, "15m", 210)[:-1]
        closes_15m = [c["close"] for c in m15]

        peaks_15m = find_swing_highs(m15[-25:], neighbors=2)
        if is_near_resistance(entry, peaks_15m, tol):
            with _filter_lock: _filter_counts["f6_res15m"] = _filter_counts.get("f6_res15m", 0) + 1
            return None

        rsi1h = (calc_rsi_series([c["close"] for c in get_klines(sym, "1h", 19)[:-1]]) or [0])[-1]
        if not (cfg["rsi_1h_min"] <= rsi1h <= cfg["rsi_1h_max"]):
            with _filter_lock: _filter_counts["f7_rsi1h"] = _filter_counts.get("f7_rsi1h", 0) + 1
            return None

        # ── Fetch 3m candles (needed for EMA/MACD/SAR on 3m) ─────────────────
        m3_candles = get_klines(sym, "3m", 80)[:-1]
        closes_3m  = [c["close"] for c in m3_candles]

        # ── F8 — EMA filter (per-timeframe, each individually configurable) ───
        # 3m EMA
        if cfg.get("use_ema_3m", False):
            ema_p_3m = max(2, int(cfg.get("ema_period_3m", 200)))
            ema_3m   = calc_ema(closes_3m, ema_p_3m)
            if not ema_3m or entry < ema_3m[-1]:
                with _filter_lock: _filter_counts["f8_ema"] = _filter_counts.get("f8_ema", 0) + 1
                return None
        # 5m EMA
        if cfg.get("use_ema_5m", True):
            ema_p_5m = max(2, int(cfg.get("ema_period_5m", 200)))
            ema_5m   = calc_ema(closes_5m, ema_p_5m)
            if not ema_5m or entry < ema_5m[-1]:
                with _filter_lock: _filter_counts["f8_ema"] = _filter_counts.get("f8_ema", 0) + 1
                return None
        # 15m EMA
        if cfg.get("use_ema_15m", True):
            ema_p_15m = max(2, int(cfg.get("ema_period_15m", 200)))
            ema_15m   = calc_ema(closes_15m, ema_p_15m)
            if not ema_15m or entry < ema_15m[-1]:
                with _filter_lock: _filter_counts["f8_ema"] = _filter_counts.get("f8_ema", 0) + 1
                return None

        # ── F9 — MACD bullish filter (3m, 5m, 15m) ───────────────────────────
        # Per-timeframe conditions checked:
        #   • MACD line > 0  (above zero — bullish territory)
        #   • Signal line > 0
        #   • Histogram > 0  (momentum building — MACD above signal)
        #   • Bullish crossover (MACD crossed above signal) within last 12 candles
        if cfg.get("use_macd", True):
            if (not macd_bullish(closes_3m) or
                    not macd_bullish(closes_5m) or
                    not macd_bullish(closes_15m)):
                with _filter_lock: _filter_counts["f9_macd"] = _filter_counts.get("f9_macd", 0) + 1
                return None

        # ── F10 — Parabolic SAR bullish (3m, 5m AND 15m) ─────────────────────
        # SAR must be BELOW price on all three timeframes
        if cfg.get("use_sar", True):
            sar_3m  = calc_parabolic_sar(m3_candles)
            sar_5m  = calc_parabolic_sar(m5)
            sar_15m = calc_parabolic_sar(m15)
            if (not sar_3m  or not sar_3m[-1][1] or
                    not sar_5m  or not sar_5m[-1][1] or
                    not sar_15m or not sar_15m[-1][1]):
                with _filter_lock: _filter_counts["f10_sar"] = _filter_counts.get("f10_sar", 0) + 1
                return None

        # ── F11 — Volume spike filter (15m last candle vs lookback avg) ───────
        # The last complete 15m candle must have volume >= mult × avg(lookback candles)
        if cfg.get("use_vol_spike", False):
            lookback = max(2, int(cfg.get("vol_spike_lookback", 20)))
            mult     = float(cfg.get("vol_spike_mult", 2.0))
            vols_15m = [c["volume"] for c in m15]
            if len(vols_15m) >= lookback + 1:
                # average of the `lookback` candles immediately before the last one
                window  = vols_15m[-(lookback + 1):-1]
                avg_vol = sum(window) / len(window)
                last_vol = vols_15m[-1]
                if avg_vol <= 0 or last_vol < mult * avg_vol:
                    with _filter_lock: _filter_counts["f11_vol"] = _filter_counts.get("f11_vol", 0) + 1
                    return None

        tp  = round(entry * (1 + cfg["tp_pct"] / 100), 6)
        sl  = round(entry * (1 - cfg["sl_pct"] / 100), 6)
        sec = SECTORS.get(sym, "Other")
        with _filter_lock: _filter_counts["passed"] = _filter_counts.get("passed", 0) + 1

        return {
            "id":          str(uuid.uuid4())[:8],
            "timestamp":   datetime.now(timezone.utc).isoformat(),
            "symbol":      sym,
            "entry":       entry,
            "tp":          tp,
            "sl":          sl,
            "sector":      sec,
            "status":      "open",
            "close_price": None,
            "close_time":  None,
        }
    except Exception:
        with _filter_lock: _filter_counts["errors"] = _filter_counts.get("errors", 0) + 1
        return "error"

def scan(cfg: dict):
    _reset_filter_counts()
    symbols = get_symbols(cfg["watchlist"])
    results = []
    with ThreadPoolExecutor(max_workers=8) as exe:
        futs = [exe.submit(process, s, cfg) for s in symbols]
        for f in as_completed(futs):
            r = f.result()
            if r and r != "error":
                results.append(r)
    return sorted(results, key=lambda x: x["symbol"]), _filter_counts.get("errors", 0)

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
                               close_time=datetime.fromtimestamp(tp_time / 1000, tz=timezone.utc).isoformat())
                else:
                    sig.update(status="sl_hit", close_price=sig["sl"],
                               close_time=datetime.fromtimestamp(sl_time / 1000, tz=timezone.utc).isoformat())
        except Exception:
            pass
    return signals

# ─────────────────────────────────────────────────────────────────────────────
# Background scanner thread
# ─────────────────────────────────────────────────────────────────────────────
def _bg_loop():
    while True:
        if not _scanner_running.is_set():
            time.sleep(2)
            continue
        with _config_lock:
            cfg = dict(_b._bsc_cfg)
        t0 = time.time()
        try:
            with _log_lock:
                _b._bsc_log["signals"] = update_open_signals(_b._bsc_log["signals"])
            new_sigs, errors = scan(cfg)
            cutoff      = datetime.now(timezone.utc) - timedelta(minutes=cfg["cooldown_minutes"])
            with _log_lock:
                cooled  = {s["symbol"] for s in _b._bsc_log["signals"]
                           if datetime.fromisoformat(s["timestamp"].replace("Z", "+00:00")) >= cutoff}
                active  = {s["symbol"] for s in _b._bsc_log["signals"] if s["status"] == "open"}
                skip    = cooled | active
                for sig in new_sigs:
                    if sig["symbol"] not in skip:
                        _b._bsc_log["signals"].append(sig)
                        skip.add(sig["symbol"])
                elapsed = time.time() - t0
                _b._bsc_log["health"].update(
                    total_cycles         = _b._bsc_log["health"].get("total_cycles", 0) + 1,
                    last_scan_at         = datetime.now(timezone.utc).isoformat(),
                    last_scan_duration_s = round(elapsed, 1),
                    total_api_errors     = _b._bsc_log["health"].get("total_api_errors", 0) + errors,
                    watchlist_size       = len(cfg["watchlist"]),
                )
                save_log(_b._bsc_log)
            _b._bsc_last_error = ""
        except Exception as e:
            _b._bsc_last_error = str(e)
        elapsed = time.time() - t0
        time.sleep(max(0, cfg["loop_minutes"] * 60 - elapsed))

def _ensure_scanner():
    if _b._bsc_thread is None or not _b._bsc_thread.is_alive():
        t = threading.Thread(target=_bg_loop, daemon=True, name="binance-scanner")
        t.start()
        _b._bsc_thread = t

# ─────────────────────────────────────────────────────────────────────────────
# STREAMLIT UI
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="OKX Futures Scanner",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

_ensure_scanner()

# ── Snapshot shared state for this render ────────────────────────────────────
with _log_lock:
    _snap_log = json.loads(json.dumps(_b._bsc_log))
with _config_lock:
    _snap_cfg = dict(_b._bsc_cfg)

health  = _snap_log.get("health", {})
signals = _snap_log.get("signals", [])

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR — Configuration
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Configuration")

    # ── Scanner control ───────────────────────────────────────────────────────
    running = _scanner_running.is_set()
    btn_label = "⏹ Stop Scanner" if running else "▶️ Start Scanner"
    btn_color = "primary" if running else "secondary"
    if st.button(btn_label, use_container_width=True, type=btn_color):
        if running:
            _scanner_running.clear()
        else:
            _scanner_running.set()
        st.rerun()

    status_color = "🟢" if running else "🔴"
    st.caption(f"{status_color}  Scanner is {'running' if running else 'stopped'}")

    st.divider()

    # ── Trade management ──────────────────────────────────────────────────────
    st.markdown("**📊 Trade Settings**")
    c1, c2 = st.columns(2)
    new_tp = c1.number_input("TP %", min_value=0.1, max_value=20.0, step=0.1,
                              value=float(_snap_cfg["tp_pct"]), key="cfg_tp")
    new_sl = c2.number_input("SL %", min_value=0.1, max_value=20.0, step=0.1,
                              value=float(_snap_cfg["sl_pct"]), key="cfg_sl")

    st.divider()

    # ── RSI ───────────────────────────────────────────────────────────────────
    st.markdown("**📈 F4/F7 — RSI**")
    new_rsi5_min = st.number_input("5m RSI min", 0, 100, step=1,
                                    value=int(_snap_cfg["rsi_5m_min"]), key="cfg_rsi5")
    c3, c4 = st.columns(2)
    new_rsi1h_min = c3.number_input("1h RSI min", 0, 100, step=1,
                                     value=int(_snap_cfg["rsi_1h_min"]), key="cfg_rsi1h_min")
    new_rsi1h_max = c4.number_input("1h RSI max", 0, 100, step=1,
                                     value=int(_snap_cfg["rsi_1h_max"]), key="cfg_rsi1h_max")

    st.divider()

    # ── Resistance ────────────────────────────────────────────────────────────
    st.markdown("**🚧 F5/F6 — Resistance**")
    new_res_tol = st.number_input("Tolerance % above entry", 0.1, 10.0, step=0.1,
                                   value=float(_snap_cfg["resistance_tol_pct"]), key="cfg_res")

    st.divider()

    # ── EMA Filter — per timeframe ────────────────────────────────────────────
    st.markdown("**📉 F8 — EMA Filter** (price above EMA per timeframe)")

    # 3m EMA
    ea1, ea2 = st.columns([1, 2])
    new_use_ema_3m = ea1.checkbox(
        "3m EMA",
        value=bool(_snap_cfg.get("use_ema_3m", False)),
        key="cfg_use_ema_3m",
        help="Price must be above EMA on the 3-minute chart.",
    )
    new_ema_period_3m = ea2.number_input(
        "Period##3m", min_value=2, max_value=500, step=1,
        value=int(_snap_cfg.get("ema_period_3m", 200)),
        key="cfg_ema_period_3m",
        disabled=not new_use_ema_3m,
        label_visibility="collapsed",
    )
    if new_use_ema_3m:
        ea2.caption(f"3m EMA {new_ema_period_3m}")

    # 5m EMA
    eb1, eb2 = st.columns([1, 2])
    new_use_ema_5m = eb1.checkbox(
        "5m EMA",
        value=bool(_snap_cfg.get("use_ema_5m", True)),
        key="cfg_use_ema_5m",
        help="Price must be above EMA on the 5-minute chart.",
    )
    new_ema_period_5m = eb2.number_input(
        "Period##5m", min_value=2, max_value=500, step=1,
        value=int(_snap_cfg.get("ema_period_5m", 200)),
        key="cfg_ema_period_5m",
        disabled=not new_use_ema_5m,
        label_visibility="collapsed",
    )
    if new_use_ema_5m:
        eb2.caption(f"5m EMA {new_ema_period_5m}")

    # 15m EMA
    ec1, ec2 = st.columns([1, 2])
    new_use_ema_15m = ec1.checkbox(
        "15m EMA",
        value=bool(_snap_cfg.get("use_ema_15m", True)),
        key="cfg_use_ema_15m",
        help="Price must be above EMA on the 15-minute chart.",
    )
    new_ema_period_15m = ec2.number_input(
        "Period##15m", min_value=2, max_value=500, step=1,
        value=int(_snap_cfg.get("ema_period_15m", 200)),
        key="cfg_ema_period_15m",
        disabled=not new_use_ema_15m,
        label_visibility="collapsed",
    )
    if new_use_ema_15m:
        ec2.caption(f"15m EMA {new_ema_period_15m}")

    st.divider()

    # ── MACD Filter ───────────────────────────────────────────────────────────
    st.markdown("**📊 F9 — MACD Filter** (3m, 5m & 15m)")
    new_use_macd = st.checkbox(
        "Enable MACD filter",
        value=bool(_snap_cfg.get("use_macd", True)),
        key="cfg_use_macd",
        help=(
            "All 4 conditions must be true on each of 3m, 5m & 15m:\n"
            "① MACD line > 0  ② Signal line > 0  "
            "③ Histogram > 0  ④ Bullish crossover within last 12 candles"
        ),
    )
    if new_use_macd:
        st.caption("✅ MACD line > 0 · Signal > 0 · Histogram > 0 · Crossover ↑")

    st.divider()

    # ── Parabolic SAR Filter ──────────────────────────────────────────────────
    st.markdown("**🪂 F10 — Parabolic SAR** (3m, 5m & 15m)")
    new_use_sar = st.checkbox(
        "Enable Parabolic SAR filter",
        value=bool(_snap_cfg.get("use_sar", True)),
        key="cfg_use_sar",
        help="SAR must be below price (bullish trend) on the 3m, 5m AND 15m charts.",
    )
    if new_use_sar:
        st.caption("✅ SAR below price on 3m · 5m · 15m")

    st.divider()

    # ── Volume Spike Filter ───────────────────────────────────────────────────
    st.markdown("**📦 F11 — Volume Spike** (15m)")
    new_use_vol_spike = st.checkbox(
        "Enable volume spike filter",
        value=bool(_snap_cfg.get("use_vol_spike", False)),
        key="cfg_use_vol_spike",
        help=(
            "Last complete 15m candle volume must be ≥ X × the average volume "
            "of the previous N candles."
        ),
    )
    vx1, vx2 = st.columns(2)
    new_vol_mult = vx1.number_input(
        "Multiplier (X×)",
        min_value=1.0, max_value=20.0, step=0.5,
        value=float(_snap_cfg.get("vol_spike_mult", 2.0)),
        key="cfg_vol_mult",
        disabled=not new_use_vol_spike,
        help="e.g. 2.0 = last candle must have 2× the average volume.",
    )
    new_vol_lookback = vx2.number_input(
        "Lookback (N candles)",
        min_value=2, max_value=100, step=1,
        value=int(_snap_cfg.get("vol_spike_lookback", 20)),
        key="cfg_vol_lookback",
        disabled=not new_use_vol_spike,
        help="Number of prior 15m candles used to compute the average volume.",
    )
    if new_use_vol_spike:
        st.caption(f"✅ Last 15m candle volume ≥ {new_vol_mult}× avg of last {new_vol_lookback} candles")

    st.divider()

    # ── Execution ─────────────────────────────────────────────────────────────
    st.markdown("**⏱ Execution**")
    c5, c6 = st.columns(2)
    new_loop = c5.number_input("Loop (min)", 1, 60, step=1,
                                value=int(_snap_cfg["loop_minutes"]), key="cfg_loop")
    new_cool = c6.number_input("Cooldown (min)", 1, 120, step=1,
                                value=int(_snap_cfg["cooldown_minutes"]), key="cfg_cool")

    st.divider()

    # ── Watchlist ─────────────────────────────────────────────────────────────
    st.markdown("**📋 Watchlist** (one symbol per line)")
    wl_text = st.text_area(
        label="watchlist",
        value="\n".join(_snap_cfg["watchlist"]),
        height=180,
        label_visibility="collapsed",
        key="cfg_wl",
    )

    st.divider()

    # ── Data management ───────────────────────────────────────────────────────
    st.markdown("**🗑 Clear Signal History**")

    if st.button("⚡ Flush All Data", use_container_width=True, type="secondary",
                 help="Remove every signal from history and reset health counters"):
        with _log_lock:
            _b._bsc_log["signals"] = []
            _b._bsc_log["health"] = {
                "total_cycles": 0, "last_scan_at": None,
                "last_scan_duration_s": 0.0, "total_api_errors": 0,
                "watchlist_size": 0,
            }
            save_log(_b._bsc_log)
        st.success("✅ All data flushed")
        st.rerun()

    c_day, c_week = st.columns(2)
    if c_day.button("📅 Clear 24h", use_container_width=True,
                    help="Remove signals from the last 24 hours"):
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        with _log_lock:
            before = len(_b._bsc_log["signals"])
            _b._bsc_log["signals"] = [
                s for s in _b._bsc_log["signals"]
                if datetime.fromisoformat(
                    s["timestamp"].replace("Z", "+00:00")) < cutoff
            ]
            removed = before - len(_b._bsc_log["signals"])
            save_log(_b._bsc_log)
        st.success(f"✅ Removed {removed} signal(s) from last 24 h")
        st.rerun()

    if c_week.button("📆 Clear 7d", use_container_width=True,
                     help="Remove signals from the last 7 days"):
        cutoff = datetime.now(timezone.utc) - timedelta(days=7)
        with _log_lock:
            before = len(_b._bsc_log["signals"])
            _b._bsc_log["signals"] = [
                s for s in _b._bsc_log["signals"]
                if datetime.fromisoformat(
                    s["timestamp"].replace("Z", "+00:00")) < cutoff
            ]
            removed = before - len(_b._bsc_log["signals"])
            save_log(_b._bsc_log)
        st.success(f"✅ Removed {removed} signal(s) from last 7 days")
        st.rerun()

    st.divider()

    # ── Reset to Defaults ─────────────────────────────────────────────────────
    if st.button("↩️ Reset to Defaults", use_container_width=True, type="secondary",
                 help="Restore every setting to the built-in default values."):
        default_cfg = dict(DEFAULT_CONFIG)
        with _config_lock:
            _b._bsc_cfg.clear()
            _b._bsc_cfg.update(default_cfg)
        save_config(default_cfg)
        st.success("✅ All settings reset to defaults")
        st.rerun()

    # ── Save button ───────────────────────────────────────────────────────────
    if st.button("💾 Save & Apply", use_container_width=True, type="primary"):
        new_wl = [s.strip().upper() for s in wl_text.splitlines() if s.strip()]
        new_cfg = {
            "tp_pct":             new_tp,
            "sl_pct":             new_sl,
            "rsi_5m_min":         int(new_rsi5_min),
            "resistance_tol_pct": new_res_tol,
            "rsi_1h_min":         int(new_rsi1h_min),
            "rsi_1h_max":         int(new_rsi1h_max),
            "loop_minutes":       int(new_loop),
            "cooldown_minutes":   int(new_cool),
            # EMA per timeframe
            "use_ema_3m":         bool(new_use_ema_3m),
            "ema_period_3m":      int(new_ema_period_3m),
            "use_ema_5m":         bool(new_use_ema_5m),
            "ema_period_5m":      int(new_ema_period_5m),
            "use_ema_15m":        bool(new_use_ema_15m),
            "ema_period_15m":     int(new_ema_period_15m),
            # MACD
            "use_macd":           bool(new_use_macd),
            # SAR
            "use_sar":            bool(new_use_sar),
            # Volume spike
            "use_vol_spike":      bool(new_use_vol_spike),
            "vol_spike_mult":     float(new_vol_mult),
            "vol_spike_lookback": int(new_vol_lookback),
            "watchlist":          new_wl,
        }
        with _config_lock:
            _b._bsc_cfg.clear()
            _b._bsc_cfg.update(new_cfg)
        save_config(new_cfg)
        st.success(f"✅ Config saved — {len(new_wl)} coins in watchlist")
        st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# MAIN AREA
# ─────────────────────────────────────────────────────────────────────────────

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🔍 OKX Futures Scanner")

last_scan = health.get("last_scan_at", "never")
if last_scan and last_scan != "never":
    try:
        ts  = datetime.fromisoformat(last_scan.replace("Z", "+00:00"))
        ago = int((datetime.now(timezone.utc) - ts).total_seconds() / 60)
        last_scan = f"{ago}m ago"
    except Exception:
        pass

col_h1, col_h2 = st.columns([3, 1])
col_h1.caption(f"Last scan: {last_scan}   |   Auto-refreshes every 30 s")
if col_h2.button("🔄 Refresh now", key="manual_refresh"):
    st.rerun()

# ── Health metrics ────────────────────────────────────────────────────────────
open_count = sum(1 for s in signals if s["status"] == "open")
tp_count   = sum(1 for s in signals if s["status"] == "tp_hit")
sl_count   = sum(1 for s in signals if s["status"] == "sl_hit")

m1, m2, m3, m4, m5_col, m6 = st.columns(6)
m1.metric("Total Cycles",    health.get("total_cycles", 0))
m2.metric("Last Scan",       f"{health.get('last_scan_duration_s', 0)}s")
m3.metric("API Errors",      health.get("total_api_errors", 0))
m4.metric("Open Trades",     open_count)
m5_col.metric("TP Hit",      tp_count)
m6.metric("SL Hit",          sl_count)

# ── Last scanner error (if any) ───────────────────────────────────────────────
if getattr(_b, "_bsc_last_error", ""):
    st.warning(f"⚠️ Last scanner error: {_b._bsc_last_error}")

st.divider()

# ── Active filters badge row ──────────────────────────────────────────────────
st.markdown("**Active Filters:**")
badges = []
if _snap_cfg.get("use_ema_3m"):    badges.append(f"📉 EMA{_snap_cfg.get('ema_period_3m',200)} 3m")
if _snap_cfg.get("use_ema_5m"):    badges.append(f"📉 EMA{_snap_cfg.get('ema_period_5m',200)} 5m")
if _snap_cfg.get("use_ema_15m"):   badges.append(f"📉 EMA{_snap_cfg.get('ema_period_15m',200)} 15m")
if _snap_cfg.get("use_macd"):      badges.append("📊 MACD 3m·5m·15m")
if _snap_cfg.get("use_sar"):       badges.append("🪂 SAR 3m·5m·15m")
if _snap_cfg.get("use_vol_spike"): badges.append(
    f"📦 Vol ≥{_snap_cfg.get('vol_spike_mult',2.0)}× / {_snap_cfg.get('vol_spike_lookback',20)} 15m candles")
if badges:
    st.caption("  |  ".join(badges))
else:
    st.caption("No advanced filters enabled")

st.divider()

# ── Sector filter ─────────────────────────────────────────────────────────────
all_sectors = ["All", "BTC", "L1", "L2", "DeFi", "AI", "Privacy", "Meme", "Gaming", "Other"]
if "sector_filter" not in st.session_state:
    st.session_state["sector_filter"] = "All"

sector_cols = st.columns(len(all_sectors))
for i, sec in enumerate(all_sectors):
    active = st.session_state["sector_filter"] == sec
    btn_type = "primary" if active else "secondary"
    if sector_cols[i].button(sec, key=f"sec_{sec}", type=btn_type, use_container_width=True):
        st.session_state["sector_filter"] = sec
        st.rerun()

selected_sector = st.session_state["sector_filter"]

# ── Signals table ─────────────────────────────────────────────────────────────
filtered = signals if selected_sector == "All" else [s for s in signals if s.get("sector") == selected_sector]
filtered_sorted = sorted(filtered, key=lambda x: x.get("timestamp", ""), reverse=True)

st.markdown(f"### Signals ({len(filtered_sorted)} shown)")

if filtered_sorted:
    rows = []
    for s in filtered_sorted:
        status = s.get("status", "open")
        status_icon = {"open": "🔵 Open", "tp_hit": "✅ TP Hit", "sl_hit": "❌ SL Hit"}.get(status, status)
        ts = s.get("timestamp", "")
        try:
            ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            ts_str = ts_dt.strftime("%m/%d %H:%M")
        except Exception:
            ts_str = ts[:16] if ts else "—"
        rows.append({
            "Time":    ts_str,
            "Symbol":  s.get("symbol", ""),
            "Sector":  s.get("sector", "Other"),
            "Entry":   s.get("entry", ""),
            "TP":      s.get("tp", ""),
            "SL":      s.get("sl", ""),
            "Status":  status_icon,
            "Close $": s.get("close_price") or "—",
        })
    st.dataframe(rows, use_container_width=True, hide_index=True,
                 column_config={
                     "TP":    st.column_config.NumberColumn(format="%.6f"),
                     "SL":    st.column_config.NumberColumn(format="%.6f"),
                     "Entry": st.column_config.NumberColumn(format="%.6f"),
                 })
else:
    st.info("No signals yet. The scanner runs every few minutes — check back soon.")

# ── Charts ────────────────────────────────────────────────────────────────────
if signals:
    st.divider()
    ch1, ch2 = st.columns(2)

    # Pie: sector distribution
    sec_counts: dict = {}
    for s in signals:
        sec = s.get("sector", "Other")
        sec_counts[sec] = sec_counts.get(sec, 0) + 1
    fig_pie = go.Figure(go.Pie(
        labels=list(sec_counts.keys()),
        values=list(sec_counts.values()),
        hole=0.4,
        marker=dict(colors=px.colors.qualitative.Dark24),
    ))
    fig_pie.update_layout(
        title="Signals by Sector",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e6edf3"),
        margin=dict(t=40, b=10, l=10, r=10),
        legend=dict(font=dict(size=11)),
    )
    ch1.plotly_chart(fig_pie, use_container_width=True)

    # Bar: outcome breakdown
    outcome_data = {
        "Open":   sum(1 for s in signals if s["status"] == "open"),
        "TP Hit": sum(1 for s in signals if s["status"] == "tp_hit"),
        "SL Hit": sum(1 for s in signals if s["status"] == "sl_hit"),
    }
    fig_bar = go.Figure(go.Bar(
        x=list(outcome_data.keys()),
        y=list(outcome_data.values()),
        marker_color=["#58a6ff", "#3fb950", "#f85149"],
        text=list(outcome_data.values()),
        textposition="outside",
    ))
    fig_bar.update_layout(
        title="Signal Outcomes",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e6edf3"),
        yaxis=dict(gridcolor="#21262d"),
        margin=dict(t=40, b=10, l=10, r=10),
    )
    ch2.plotly_chart(fig_bar, use_container_width=True)

    # Timeline: signals per day
    if len(signals) > 1:
        from collections import Counter
        day_counts: Counter = Counter()
        for s in signals:
            try:
                d = datetime.fromisoformat(s["timestamp"].replace("Z", "+00:00")).strftime("%m/%d")
                day_counts[d] += 1
            except Exception:
                pass
        if day_counts:
            days   = sorted(day_counts.keys())
            counts = [day_counts[d] for d in days]
            fig_line = go.Figure(go.Bar(
                x=days, y=counts,
                marker_color="#d29922",
                text=counts, textposition="outside",
            ))
            fig_line.update_layout(
                title="Signals Per Day",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e6edf3"),
                yaxis=dict(gridcolor="#21262d"),
                margin=dict(t=40, b=10, l=10, r=10),
            )
            st.plotly_chart(fig_line, use_container_width=True)

# ── Filter funnel (collapsible) ───────────────────────────────────────────────
fc = dict(_filter_counts)
if fc.get("checked", 0) > 0:
    with st.expander("🔬 Last scan filter funnel"):
        checked = fc.get("checked", 0)
        f4  = fc.get("f4_rsi5m",  0)
        f5  = fc.get("f5_res5m",  0)
        f6  = fc.get("f6_res15m", 0)
        f7  = fc.get("f7_rsi1h",  0)
        f8  = fc.get("f8_ema",    0)
        f9  = fc.get("f9_macd",   0)
        f10 = fc.get("f10_sar",   0)
        f11 = fc.get("f11_vol",   0)

        after_f4  = checked   - f4
        after_f5  = after_f4  - f5
        after_f6  = after_f5  - f6
        after_f7  = after_f6  - f7
        after_f8  = after_f7  - f8
        after_f9  = after_f8  - f9
        after_f10 = after_f9  - f10

        # Build labels to reflect which filters are active
        ema_parts = []
        if _snap_cfg.get("use_ema_3m"):  ema_parts.append(f"3m EMA{_snap_cfg.get('ema_period_3m',200)}")
        if _snap_cfg.get("use_ema_5m"):  ema_parts.append(f"5m EMA{_snap_cfg.get('ema_period_5m',200)}")
        if _snap_cfg.get("use_ema_15m"): ema_parts.append(f"15m EMA{_snap_cfg.get('ema_period_15m',200)}")
        ema_lbl = ("After F8 EMA (" + " · ".join(ema_parts) + ")") if ema_parts else "F8 EMA (off)"

        macd_lbl = "After F9 MACD 3m·5m·15m" if _snap_cfg.get("use_macd") else "F9 MACD (off)"
        sar_lbl  = "After F10 SAR 3m·5m·15m"  if _snap_cfg.get("use_sar")  else "F10 SAR (off)"
        vol_lbl  = (
            f"After F11 Vol ≥{_snap_cfg.get('vol_spike_mult',2.0)}× "
            f"/ {_snap_cfg.get('vol_spike_lookback',20)} 15m"
            if _snap_cfg.get("use_vol_spike") else "F11 Vol Spike (off)"
        )

        funnel_data = [
            ("Checked",           checked),
            ("After F4 5m RSI",   after_f4),
            ("After F5 5m Res.",  after_f5),
            ("After F6 15m Res.", after_f6),
            ("After F7 1h RSI",   after_f7),
            (ema_lbl,             after_f8),
            (macd_lbl,            after_f9),
            (sar_lbl,             after_f10),
            (vol_lbl,             fc.get("passed", 0)),
        ]
        fig_funnel = go.Figure(go.Funnel(
            y=[d[0] for d in funnel_data],
            x=[d[1] for d in funnel_data],
            marker=dict(color=[
                "#58a6ff","#79c0ff","#a5d6ff",
                "#3fb950","#56d364","#d29922","#e3b341","#f0883e","#f85149",
            ]),
            textinfo="value+percent initial",
        ))
        fig_funnel.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e6edf3"),
            margin=dict(t=10, b=10, l=10, r=10),
            height=450,
        )
        st.plotly_chart(fig_funnel, use_container_width=True)
        st.caption(f"Errors this cycle: {fc.get('errors', 0)}")

# ─────────────────────────────────────────────────────────────────────────────
# Auto-refresh every 30 seconds
# ─────────────────────────────────────────────────────────────────────────────
time.sleep(30)
st.rerun()
