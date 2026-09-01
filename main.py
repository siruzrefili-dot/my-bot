import os
import logging
import requests
import pandas as pd
import numpy as np
import time
import asyncio
from flask import Flask
from threading import Thread
from datetime import datetime, timezone
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
app_flask = Flask(__name__)

@app_flask.route("/")
def home():
    return "Professional SMC AI Bot is running!"

def run_flask():
    port = int(os.getenv("PORT", "10000"))
    app_flask.run(host="0.0.0.0", port=port)

def keep_alive():
    Thread(target=run_flask, daemon=True).start()

def env_bool(name, default=True):
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "beli", "bəli", "on")

TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "1121794078")

FALLBACK_COINS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "LINKUSDT", "SUIUSDT"]
SCAN_TOP_N_COINS = int(os.getenv("SCAN_TOP_N_COINS", "40"))
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))
NOTIFY_COOLDOWN_SECONDS = int(os.getenv("NOTIFY_COOLDOWN_SECONDS", "7200"))
LEVERAGE = int(os.getenv("LEVERAGE", "10"))

SWING_LOOKBACK = int(os.getenv("SWING_LOOKBACK", "2"))
KLINES_LIMIT = int(os.getenv("KLINES_LIMIT", "200"))
DAILY_KLINES_LIMIT = int(os.getenv("DAILY_KLINES_LIMIT", "150"))
ENTRY_KLINES_LIMIT = int(os.getenv("ENTRY_KLINES_LIMIT", "200"))
MIN_RR_RATIO = float(os.getenv("MIN_RR_RATIO", "2.0"))
MAX_EVENT_AGE_BARS = int(os.getenv("MAX_EVENT_AGE_BARS", "25"))
ACCOUNT_BALANCE = float(os.getenv("ACCOUNT_BALANCE", "1000"))
RISK_PERCENT = float(os.getenv("RISK_PERCENT", "1"))
MIN_SIGNAL_SCORE = float(os.getenv("MIN_SIGNAL_SCORE", "65"))

REQUIRE_TREND_ALIGN = env_bool("REQUIRE_TREND_ALIGN", True)
REQUIRE_LIQUIDITY_SWEEP = env_bool("REQUIRE_LIQUIDITY_SWEEP", True)
REQUIRE_FVG = env_bool("REQUIRE_FVG", True)
REQUIRE_SESSION_FILTER = env_bool("REQUIRE_SESSION_FILTER", True)
REQUIRE_OB_UNMITIGATED = env_bool("REQUIRE_OB_UNMITIGATED", True)
REQUIRE_CHOCH_ONLY = env_bool("REQUIRE_CHOCH_ONLY", False)
REQUIRE_EQUAL_LEVEL_SWEEP = env_bool("REQUIRE_EQUAL_LEVEL_SWEEP", False)
REQUIRE_VOLUME = env_bool("REQUIRE_VOLUME", True)
REQUIRE_DISPLACEMENT = env_bool("REQUIRE_DISPLACEMENT", True)
REQUIRE_15M_CONFIRMATION = env_bool("REQUIRE_15M_CONFIRMATION", True)
REQUIRE_BTC_FILTER = env_bool("REQUIRE_BTC_FILTER", True)
REQUIRE_FUNDING_FILTER = env_bool("REQUIRE_FUNDING_FILTER", False)
REQUIRE_SESSION_FILTER = env_bool("REQUIRE_SESSION_FILTER", True)

session_http = requests.Session()
_last_notified = {}

def safe_get(url, params=None, timeout=8, retries=2):
    for attempt in range(retries + 1):
        try:
            response = session_http.get(url, params=params, timeout=timeout)
            if response.status_code == 200:
                return response
            logging.warning(f"HTTP {response.status_code}: {url}")
        except Exception as e:
            logging.warning(f"Request error attempt {attempt + 1}: {e}")
        time.sleep(1 + attempt)
    return None

def fetch_top_liquid_coins(limit=SCAN_TOP_N_COINS):
    url = "https://api.bybit.com/v5/market/tickers"
    response = safe_get(url, {"category": "linear"}, timeout=10)
    try:
        if response:
            payload = response.json()
            if payload.get("retCode") == 0:
                rows = payload.get("result", {}).get("list", [])
                rows = [r for r in rows if r.get("symbol", "").endswith("USDT") and float(r.get("turnover24h") or 0) > 0]
                rows.sort(key=lambda r: float(r.get("turnover24h") or 0), reverse=True)
                symbols = [r["symbol"] for r in rows[:limit]]
                if symbols:
                    logging.info(f"{len(symbols)} likvid coin taranır.")
                    return symbols
    except Exception as e:
        logging.error(f"Likvid coin xətası: {e}")
    return FALLBACK_COINS

def fetch_klines(symbol, interval="60", limit=200):
    url = "https://api.bybit.com/v5/market/kline"
    params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
    response = safe_get(url, params, timeout=8)
    try:
        if response:
            payload = response.json()
            if payload.get("retCode") == 0:
                rows = payload.get("result", {}).get("list", [])
                if len(rows) > 30:
                    df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"])
                    df = df.iloc[::-1].reset_index(drop=True)
                    for col in ["open", "high", "low", "close", "volume", "turnover"]:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
                    df = df.dropna().reset_index(drop=True)
                    if len(df) > 2:
                        return df.iloc[:-1].reset_index(drop=True)
            else:
                logging.warning(f"{symbol} API: {payload.get('retMsg')}")
    except Exception as e:
        logging.error(f"{symbol} kline xətası: {e}")
    return None

def compute_atr(df, period=14):
    prev_close = df["close"].shift(1)
    tr = pd.concat([(df["high"] - df["low"]), (df["high"] - prev_close).abs(), (df["low"] - prev_close).abs()], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=1).mean()

def compute_volume_ratio(df, period=20):
    avg_volume = df["volume"].rolling(period, min_periods=5).mean()
    return df["volume"] / avg_volume.replace(0, np.nan)

def find_swing_points(df, lookback=2):
    highs = df["high"].values
    lows = df["low"].values
    swing_highs = []
    swing_lows = []
    for i in range(lookback, len(df) - lookback):
        if highs[i] > np.max(highs[i - lookback:i]) and highs[i] > np.max(highs[i + 1:i + lookback + 1]):
            swing_highs.append((i, float(highs[i])))
        if lows[i] < np.min(lows[i - lookback:i]) and lows[i] < np.min(lows[i + 1:i + lookback + 1]):
            swing_lows.append((i, float(lows[i])))
    return swing_highs, swing_lows

def determine_trend_bias(swing_highs, swing_lows):
    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return None
    hh = swing_highs[-1][1] > swing_highs[-2][1]
    hl = swing_lows[-1][1] > swing_lows[-2][1]
    lh = swing_highs[-1][1] < swing_highs[-2][1]
    ll = swing_lows[-1][1] < swing_lows[-2][1]
    if hh and hl:
        return "bullish"
    if lh and ll:
        return "bearish"
    return "ranging"

def compute_structure_events(df, swing_highs, swing_lows):
    events = []
    close = df["close"].values
    trend_bias = None
    pivots = sorted([(i, p, "high") for i, p in swing_highs] + [(i, p, "low") for i, p in swing_lows])
    active_high = None
    active_low = None
    high_crossed = True
    low_crossed = True
    pivot_ptr = 0
    for i in range(len(df)):
        while pivot_ptr < len(pivots) and pivots[pivot_ptr][0] == i:
            idx, price, pivot_type = pivots[pivot_ptr]
            if pivot_type == "high":
                active_high = (idx, price)
                high_crossed = False
            else:
                active_low = (idx, price)
                low_crossed = False
            pivot_ptr += 1
        if i == 0:
            continue
        if active_high and not high_crossed and close[i - 1] <= active_high[1] < close[i]:
            kind = "CHoCH" if trend_bias == "bearish" else "BOS"
            trend_bias = "bullish"
            high_crossed = True
            events.append({"index": i, "bias": "bullish", "kind": kind, "level": active_high[1]})
        if active_low and not low_crossed and close[i - 1] >= active_low[1] > close[i]:
            kind = "CHoCH" if trend_bias == "bullish" else "BOS"
            trend_bias = "bearish"
            low_crossed = True
            events.append({"index": i, "bias": "bearish", "kind": kind, "level": active_low[1]})
    return events

def detect_equal_levels(swing_highs, swing_lows, atr_series, threshold=0.15):
    equal_highs = []
    equal_lows = []
    for i in range(1, len(swing_highs)):
        i1, p1 = swing_highs[i - 1]
        i2, p2 = swing_highs[i]
        atr = float(atr_series.iloc[min(i2, len(atr_series) - 1)])
        if atr > 0 and abs(p1 - p2) <= atr * threshold:
            equal_highs.append((i1, i2, (p1 + p2) / 2))
    for i in range(1, len(swing_lows)):
        i1, p1 = swing_lows[i - 1]
        i2, p2 = swing_lows[i]
        atr = float(atr_series.iloc[min(i2, len(atr_series) - 1)])
        if atr > 0 and abs(p1 - p2) <= atr * threshold:
            equal_lows.append((i1, i2, (p1 + p2) / 2))
    return equal_highs, equal_lows

def detect_liquidity_sweep(df, direction, break_idx, lookback_window=20):
    start = max(0, break_idx - lookback_window)
    segment = df.iloc[start:break_idx + 1].reset_index(drop=True)
    if len(segment) < 4:
        return False
    if direction == "bullish":
        for i in range(2, len(segment)):
            prior_low = segment["low"].iloc[:i].min()
            if segment["low"].iloc[i] < prior_low and segment["close"].iloc[i] > prior_low:
                return True
    else:
        for i in range(2, len(segment)):
            prior_high = segment["high"].iloc[:i].max()
            if segment["high"].iloc[i] > prior_high and segment["close"].iloc[i] < prior_high:
                return True
    return False

def find_fvg(df, direction, break_idx, lookback_window=20):
    start = max(0, break_idx - lookback_window)
    segment = df.iloc[start:break_idx + 1]
    found = []
    for i in range(1, len(segment) - 1):
        prev_candle = segment.iloc[i - 1]
        next_candle = segment.iloc[i + 1]
        if direction == "bullish" and float(prev_candle["high"]) < float(next_candle["low"]):
            found.append({"low": float(prev_candle["high"]), "high": float(next_candle["low"]), "mid": (float(prev_candle["high"]) + float(next_candle["low"])) / 2})
        if direction == "bearish" and float(prev_candle["low"]) > float(next_candle["high"]):
            found.append({"low": float(next_candle["high"]), "high": float(prev_candle["low"]), "mid": (float(next_candle["high"]) + float(prev_candle["low"])) / 2})
    return found[-1] if found else None

def detect_displacement(df, break_idx, atr_series):
    if break_idx <= 0 or break_idx >= len(df):
        return False, 0.0
    candle = df.iloc[break_idx]
    body = abs(float(candle["close"]) - float(candle["open"]))
    atr = float(atr_series.iloc[break_idx])
    if atr <= 0:
        return False, 0.0
    ratio = body / atr
    return ratio >= 0.8, ratio

def find_order_block(df, direction, break_idx, lookback=30):
    start = max(0, break_idx - lookback)
    if direction == "bullish":
        candidates = [i for i in range(break_idx - 1, start - 1, -1) if df["close"].iloc[i] < df["open"].iloc[i]]
    else:
        candidates = [i for i in range(break_idx - 1, start - 1, -1) if df["close"].iloc[i] > df["open"].iloc[i]]
    if not candidates:
        return None
    ob_idx = candidates[0]
    ob_high = float(df["high"].iloc[ob_idx])
    ob_low = float(df["low"].iloc[ob_idx])
    mitigated = False
    for j in range(break_idx + 1, len(df)):
        if direction == "bullish" and float(df["close"].iloc[j]) < ob_low:
            mitigated = True
            break
        if direction == "bearish" and float(df["close"].iloc[j]) > ob_high:
            mitigated = True
            break
    return {"index": ob_idx, "high": ob_high, "low": ob_low, "mid": (ob_high + ob_low) / 2, "mitigated": mitigated}

def find_next_liquidity(direction, current_price, swing_highs, swing_lows):
    if direction == "bullish":
        targets = [p for _, p in swing_highs if p > current_price]
        return min(targets) if targets else None
    targets = [p for _, p in swing_lows if p < current_price]
    return max(targets) if targets else None

def get_trading_session():
    hour = datetime.now(timezone.utc).hour
    london = 7 <= hour < 16
    new_york = 13 <= hour < 22
    active = london or new_york
    labels = []
    if london:
        labels.append("London")
    if new_york:
        labels.append("New York")
    return active, " + ".join(labels) if labels else "Asia"

def get_daily_trend_bias(symbol):
    df = fetch_klines(symbol, "D", DAILY_KLINES_LIMIT)
    if df is None or len(df) < 40:
        return None
    sh, sl = find_swing_points(df, SWING_LOOKBACK)
    return determine_trend_bias(sh, sl)

def get_btc_market_bias():
    df = fetch_klines("BTCUSDT", "240", 120)
    if df is None or len(df) < 50:
        return None
    close = df["close"]
    ema20 = close.ewm(span=20, adjust=False).mean().iloc[-1]
    ema50 = close.ewm(span=50, adjust=False).mean().iloc[-1]
    price = close.iloc[-1]
    if price > ema20 > ema50:
        return "bullish"
    if price < ema20 < ema50:
        return "bearish"
    return "neutral"

def get_fear_greed():
    try:
        response = safe_get("https://api.alternative.me/fng/", {"limit": 1}, timeout=6, retries=1)
        if response:
            data = response.json().get("data", [])
            if data:
                value = int(data[0].get("value", 50))
                classification = data[0].get("value_classification", "Unknown")
                return value, classification
    except Exception as e:
        logging.warning(f"Fear & Greed xətası: {e}")
    return None, "Unknown"

def get_funding_rate(symbol):
    url = "https://api.bybit.com/v5/market/funding/history"
    params = {"category": "linear", "symbol": symbol, "limit": 1}
    try:
        response = safe_get(url, params, timeout=6, retries=1)
        if response:
            rows = response.json().get("result", {}).get("list", [])
            if rows:
                return float(rows[0].get("fundingRate", 0))
    except Exception as e:
        logging.warning(f"Funding xətası {symbol}: {e}")
    return None

def get_open_interest_trend(symbol):
    url = "https://api.bybit.com/v5/market/open-interest"
    params = {"category": "linear", "symbol": symbol, "intervalTime": "1h", "limit": 10}
    try:
        response = safe_get(url, params, timeout=6, retries=1)
        if response:
            rows = response.json().get("result", {}).get("list", [])
            if len(rows) >= 2:
                rows = rows[::-1]
                first = float(rows[0].get("openInterest", 0))
                last = float(rows[-1].get("openInterest", 0))
                if first > 0:
                    change = ((last - first) / first) * 100
                    return change
    except Exception as e:
        logging.warning(f"OI xətası {symbol}: {e}")
    return None

def check_15m_confirmation(symbol, direction):
    df = fetch_klines(symbol, "15", ENTRY_KLINES_LIMIT)
    if df is None or len(df) < 50:
        return False, {"reason": "15M data yoxdur"}
    sh, sl = find_swing_points(df, 2)
    events = compute_structure_events(df, sh, sl)
    if not events:
        return False, {"reason": "15M structure event yoxdur"}
    last = events[-1]
    age = len(df) - 1 - last["index"]
    atr = compute_atr(df, 14)
    displacement, displacement_ratio = detect_displacement(df, last["index"], atr)
    volume_ratio = compute_volume_ratio(df, 20).iloc[-1]
    direction_ok = last["bias"] == direction
    fresh = age <= 12
    volume_ok = not pd.isna(volume_ratio) and volume_ratio >= 0.8
    displacement_ok = displacement_ratio >= 0.5
    confirmed = direction_ok and fresh and displacement_ok and volume_ok
    return confirmed, {"event": last["kind"], "direction_ok": direction_ok, "age": age, "fresh": fresh, "displacement": round(displacement_ratio, 2), "volume_ratio": round(float(volume_ratio), 2) if not pd.isna(volume_ratio) else 0.0}

def fundamental_score(symbol, direction):
    score = 0
    data = {}
    btc_bias = get_btc_market_bias()
    data["btc_bias"] = btc_bias
    if btc_bias == direction:
        score += 10
        data["btc_alignment"] = True
    elif btc_bias == "neutral":
        score += 2
        data["btc_alignment"] = None
    else:
        score -= 8
        data["btc_alignment"] = False
    fg_value, fg_class = get_fear_greed()
    data["fear_greed"] = fg_value
    data["fear_greed_class"] = fg_class
    if fg_value is not None:
        if direction == "bullish" and fg_value < 85:
            score += 5
        elif direction == "bearish" and fg_value > 15:
            score += 5
        if fg_value <= 15 or fg_value >= 90:
            score -= 3
    funding = get_funding_rate(symbol)
    data["funding"] = funding
    if funding is not None:
        if direction == "bullish":
            if funding < 0:
                score += 5
            elif funding > 0.001:
                score -= 5
        else:
            if funding > 0:
                score += 5
            elif funding < -0.001:
                score -= 5
    oi_change = get_open_interest_trend(symbol)
    data["oi_change"] = oi_change
    if oi_change is not None:
        if oi_change > 0:
            score += 3
        elif oi_change < -5:
            score -= 3
    return score, data

def calculate_signal_score(event_kind, rr_ratio, sweep, fvg, displacement_ratio, volume_ratio, entry_confirmed, fundamental):
    score = 0
    score += 20 if event_kind == "CHoCH" else 12
    if sweep:
        score += 12
    if fvg:
        score += 8
    if rr_ratio >= 4:
        score += 20
    elif rr_ratio >= 3:
        score += 15
    elif rr_ratio >= 2:
        score += 10
    if displacement_ratio >= 1.5:
        score += 10
    elif displacement_ratio >= 0.8:
        score += 7
    elif displacement_ratio >= 0.5:
        score += 3
    if volume_ratio >= 1.5:
        score += 8
    elif volume_ratio >= 1.1:
        score += 5
    elif volume_ratio >= 0.8:
        score += 2
    if entry_confirmed:
        score += 15
    score += fundamental
    return round(max(0, min(score, 100)), 1)

def analyze_smc_pro(symbol, session_active, session_name):
    conditions = {}
    daily_bias = get_daily_trend_bias(symbol)
    cond_daily = daily_bias in ("bullish", "bearish")
    conditions[f"Daily trend: {daily_bias or 'unknown'}"] = cond_daily
    if not cond_daily:
        return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}
    df = fetch_klines(symbol, "60", KLINES_LIMIT)
    if df is None or len(df) < 70:
        return {"symbol": symbol, "passed": False, "error": "1H data alınmadı", "conditions": conditions}
    swing_highs, swing_lows = find_swing_points(df, SWING_LOOKBACK)
    structure_ok = len(swing_highs) >= 2 and len(swing_lows) >= 2
    conditions["1H market structure"] = structure_ok
    if not structure_ok:
        return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}
    events = compute_structure_events(df, swing_highs, swing_lows)
    conditions["BOS/CHoCH event"] = len(events) > 0
    if not events:
        return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}
    last_event = events[-1]
    direction = last_event["bias"]
    event_kind = last_event["kind"]
    break_idx = last_event["index"]
    event_age = len(df) - 1 - break_idx
    fresh_event = event_age <= MAX_EVENT_AGE_BARS
    conditions[f"Fresh structure event <= {MAX_EVENT_AGE_BARS} bars"] = fresh_event
    if not fresh_event:
        return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}
    if REQUIRE_CHOCH_ONLY:
        choch_ok = event_kind == "CHoCH"
        conditions["CHoCH only"] = choch_ok
        if not choch_ok:
            return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}
    if REQUIRE_TREND_ALIGN:
        trend_ok = direction == daily_bias
        conditions["Daily and 1H trend aligned"] = trend_ok
        if not trend_ok:
            return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}
    atr = compute_atr(df, 14)
    equal_highs, equal_lows = detect_equal_levels(swing_highs, swing_lows, atr)
    sweep = detect_liquidity_sweep(df, direction, break_idx)
    if REQUIRE_LIQUIDITY_SWEEP:
        conditions["Liquidity sweep"] = sweep
        if not sweep:
            return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}
    if REQUIRE_EQUAL_LEVEL_SWEEP:
        relevant = equal_lows if direction == "bullish" else equal_highs
        eq_ok = len(relevant) > 0
        conditions["Equal liquidity pool"] = eq_ok
        if not eq_ok:
            return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}
    displacement_ok, displacement_ratio = detect_displacement(df, break_idx, atr)
    if REQUIRE_DISPLACEMENT:
        conditions[f"Displacement ATR ratio {displacement_ratio:.2f}"] = displacement_ok
        if not displacement_ok:
            return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}
    volume_ratio_series = compute_volume_ratio(df, 20)
    break_volume_ratio = float(volume_ratio_series.iloc[break_idx]) if break_idx < len(volume_ratio_series) and not pd.isna(volume_ratio_series.iloc[break_idx]) else 0.0
    volume_ok = break_volume_ratio >= 0.8
    if REQUIRE_VOLUME:
        conditions[f"Volume confirmation ratio {break_volume_ratio:.2f}"] = volume_ok
        if not volume_ok:
            return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}
    ob = find_order_block(df, direction, break_idx)
    ob_ok = ob is not None
    conditions["Valid Order Block"] = ob_ok
    if not ob_ok:
        return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}
    if REQUIRE_OB_UNMITIGATED:
        unmitigated = not ob["mitigated"]
        conditions["Order Block unmitigated"] = unmitigated
        if not unmitigated:
            return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}
    fvg = find_fvg(df, direction, break_idx)
    fvg_ok = fvg is not None
    if REQUIRE_FVG:
        conditions["Fair Value Gap"] = fvg_ok
        if not fvg_ok:
            return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}
    current_price = float(df["close"].iloc[-1])
    ob_buffer = (ob["high"] - ob["low"]) * 0.15
    in_ob_zone = (ob["low"] - ob_buffer) <= current_price <= (ob["high"] + ob_buffer)
    fvg_zone = False
    if fvg:
        fvg_buffer = (fvg["high"] - fvg["low"]) * 0.15
        fvg_zone = (fvg["low"] - fvg_buffer) <= current_price <= (fvg["high"] + fvg_buffer)
    zone_ok = in_ob_zone or fvg_zone
    conditions["Price inside OB or FVG zone"] = zone_ok
    if not zone_ok:
        return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}
    liquidity_target = find_next_liquidity(direction, current_price, swing_highs, swing_lows)
    target_ok = liquidity_target is not None
    conditions["Liquidity TP target"] = target_ok
    if not target_ok:
        return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}
    entry = current_price
    sl_atr_buffer = float(atr.iloc[-1]) * 0.15
    if direction == "bullish":
        sl = ob["low"] - sl_atr_buffer
        tp = liquidity_target
        bias = f"🟢 LONG ({event_kind})"
    else:
        sl = ob["high"] + sl_atr_buffer
        tp = liquidity_target
        bias = f"🔴 SHORT ({event_kind})"
    risk = abs(entry - sl)
    reward = abs(tp - entry)
    rr_ratio = reward / risk if risk > 0 else 0
    rr_ok = rr_ratio >= MIN_RR_RATIO
    conditions[f"RR >= 1:{MIN_RR_RATIO} actual 1:{rr_ratio:.2f}"] = rr_ok
    if not rr_ok:
        return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}
    if REQUIRE_SESSION_FILTER:
        conditions[f"Active trading session: {session_name}"] = session_active
        if not session_active:
            return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}
    entry_confirmed, entry_data = check_15m_confirmation(symbol, direction)
    if REQUIRE_15M_CONFIRMATION:
        conditions["15M entry confirmation"] = entry_confirmed
        if not entry_confirmed:
            return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}
    fundamental, fundamental_data = fundamental_score(symbol, direction)
    btc_alignment = fundamental_data.get("btc_alignment")
    if REQUIRE_BTC_FILTER:
        btc_ok = btc_alignment is not False
        conditions["BTC market alignment"] = btc_ok
        if not btc_ok:
            return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}
    funding = fundamental_data.get("funding")
    if REQUIRE_FUNDING_FILTER and funding is not None:
        if direction == "bullish":
            funding_ok = funding < 0.002
        else:
            funding_ok = funding > -0.002
        conditions["Funding rate acceptable"] = funding_ok
        if not funding_ok:
            return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}
    score = calculate_signal_score(event_kind, rr_ratio, sweep, fvg_ok, displacement_ratio, break_volume_ratio, entry_confirmed, fundamental)
    score_ok = score >= MIN_SIGNAL_SCORE
    conditions[f"Signal score >= {MIN_SIGNAL_SCORE} actual {score}"] = score_ok
    if not score_ok:
        return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions, "score": score}
    risk_amount = ACCOUNT_BALANCE * RISK_PERCENT / 100
    position_size = risk_amount / risk if risk > 0 else 0
    notional_value = position_size * entry
    margin_required = notional_value / LEVERAGE if LEVERAGE > 0 else notional_value
    signal_id = f"{symbol}_{direction}_{event_kind}_{int(df['timestamp'].iloc[break_idx])}"
    return {"symbol": symbol, "passed": True, "error": None, "conditions": conditions, "bias": bias, "direction": direction, "event_kind": event_kind, "entry": round(entry, 6), "sl": round(sl, 6), "tp": round(tp, 6), "rr_ratio": round(rr_ratio, 2), "leverage": LEVERAGE, "risk_amount": round(risk_amount, 2), "position_size": round(position_size, 6), "notional_value": round(notional_value, 2), "margin_required": round(margin_required, 2), "daily_bias": daily_bias, "session": session_name, "score": score, "event_age": event_age, "signal_id": signal_id, "fundamental": fundamental_data, "entry_confirmation": entry_data, "fvg": fvg, "ob": ob}

def get_best_smc_signal():
    session_active, session_name = get_trading_session()
    coins = fetch_top_liquid_coins()
    all_results = []
    for symbol in coins:
        try:
            result = analyze_smc_pro(symbol, session_active, session_name)
            all_results.append(result)
        except Exception as e:
            logging.error(f"{symbol} analiz xətası: {e}")
            all_results.append({"symbol": symbol, "passed": False, "error": str(e), "conditions": {}})
        time.sleep(0.1)
    valid = [r for r in all_results if r.get("passed")]
    valid.sort(key=lambda x: (x.get("score", 0), x.get("rr_ratio", 0)), reverse=True)
    return valid[0] if valid else None, all_results

def format_diagnostics(all_results, max_detail=10):
    total = len(all_results)
    reasons = {}
    errors = 0
    for result in all_results:
        if result.get("error"):
            errors += 1
            continue
        failed = [name for name, ok in result.get("conditions", {}).items() if not ok]
        if failed:
            key = failed[0].split(" actual")[0].split(" (")[0]
            reasons[key] = reasons.get(key, 0) + 1
    lines = [f"📋 *Xülasə:* `{total}` coin yoxlanıldı.", "*Ən çox dayandıran şərtlər:*"]
    for reason, count in sorted(reasons.items(), key=lambda x: x[1], reverse=True)[:10]:
        lines.append(f"• {reason}: `{count}`")
    if errors:
        lines.append(f"• API/Analiz xətası: `{errors}`")
    lines.append(f"\n*İlk {min(max_detail, total)} coin:*")
    for result in all_results[:max_detail]:
        symbol = result.get("symbol", "?")
        if result.get("error"):
            lines.append(f"• `{symbol}` ❌ {str(result['error'])[:70]}")
        else:
            failed = [name for name, ok in result.get("conditions", {}).items() if not ok]
            if failed:
                lines.append(f"• `{symbol}` ❌ {failed[0]}")
            else:
                lines.append(f"• `{symbol}` ✅ PASS")
    return "\n".join(lines)
def format_signal_message(res, title="📊 *PROFESSIONAL SMC AI SİQNALI*"):
    fundamental = res.get("fundamental", {})
    fg = fundamental.get("fear_greed")
    funding = fundamental.get("funding")
    oi = fundamental.get("oi_change")
    return f"""{title}

🪙 *Coin:* `{res['symbol']}`
⭐ *Signal Score:* `{res['score']}/100`
⚙️ *Leverage:* `{res['leverage']}x`
🎯 *Setup:* {res['bias']}
📈 *Daily Trend:* `{res['daily_bias']}`
🕒 *Session:* `{res['session']}`
⚖️ *Risk/Reward:* `1:{res['rr_ratio']}`

📍 *ENTRY:* `${res['entry']}`
🛑 *STOP LOSS:* `${res['sl']}`
🎯 *TAKE PROFIT:* `${res['tp']}`

📦 *Position Size:* `{res['position_size']}`
💰 *Position Value:* `${res['notional_value']}`
💳 *Estimated Margin:* `${res['margin_required']}`
⚠️ *Risk:* `${res['risk_amount']}`

🌍 *FUNDAMENTAL ANALYSIS*
₿ BTC Bias: `{fundamental.get('btc_bias')}`
😨 Fear & Greed: `{fg}`
📊 Funding: `{funding}`
📈 Open Interest: `{oi}%`

🔎 *15M Entry Confirmation:* `{res['entry_confirmation']}`
🆔 *Signal ID:* `{res['signal_id']}`"""

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"""📊 *Professional SMC AI Bot* aktivdir!

Canlı analiz üçün:
/analiz

Bot hər `{CHECK_INTERVAL_SECONDS // 60}` dəqiqədən bir bazarı avtomatik skan edir.

Sistem:
Daily Trend → 1H SMC → Sweep → BOS/CHoCH → Displacement → Volume → OB → FVG → 15M Confirmation → Fundamental Filter → Score → Best Signal""", parse_mode="Markdown")

async def analiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"🔍 Ən likvid `{SCAN_TOP_N_COINS}` coin taranır. Daily + 1H SMC + 15M Entry + Fundamental Analysis yoxlanılır...", parse_mode="Markdown")
    res, all_results = await asyncio.to_thread(get_best_smc_signal)
    if res:
        await update.message.reply_text(format_signal_message(res), parse_mode="Markdown")
    else:
        diag = format_diagnostics(all_results)
        await update.message.reply_text("❌ Hazırda bütün sərt şərtləri keçən yüksək keyfiyyətli setup yoxdur.\n\n" + diag, parse_mode="Markdown")

async def send_auto_signal(application, res):
    try:
        await application.bot.send_message(chat_id=CHAT_ID, text=format_signal_message(res, "🚨 *AVTOMATİK PROFESSIONAL SİQNAL* 🚨"), parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Telegram göndərmə xətası: {e}")

async def auto_signal_loop(application):
    await asyncio.sleep(10)
    while True:
        try:
            res, _ = await asyncio.to_thread(get_best_smc_signal)
            if res:
                signal_id = res["signal_id"]
                now = time.time()
                last_time = _last_notified.get(signal_id, 0)
                if now - last_time >= NOTIFY_COOLDOWN_SECONDS:
                    await send_auto_signal(application, res)
                    _last_notified[signal_id] = now
                    logging.info(f"Signal sent: {signal_id}")
                else:
                    logging.info(f"Cooldown active: {signal_id}")
        except Exception as e:
            logging.error(f"Auto scan error: {e}")
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)

async def post_init(application):
    application.create_task(auto_signal_loop(application))
    logging.info("Automatic signal scanner started.")

def main():
    if not TOKEN:
        logging.error("BOT_TOKEN tapılmadı. Environment Variables yoxlayın.")
        return
    keep_alive()
    application = ApplicationBuilder().token(TOKEN).post_init(post_init).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("analiz", analiz))
    logging.info("Professional SMC AI Bot started.")
    application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
