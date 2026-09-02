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

@app_flask.route("/health")
def health():
    return {"status": "ok", "bot": "running"}

def run_flask():
    port = int(os.getenv("PORT", "10000"))
    app_flask.run(host="0.0.0.0", port=port, use_reloader=False)

def keep_alive():
    Thread(target=run_flask, daemon=True).start()

def env_bool(name, default=True):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on", "beli", "bəli")

TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "1121794078")

FALLBACK_COINS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "LINKUSDT", "SUIUSDT", "DOGEUSDT", "LTCUSDT", "DOTUSDT", "ATOMUSDT", "NEARUSDT"]

SCAN_TOP_N_COINS = int(os.getenv("SCAN_TOP_N_COINS", "40"))
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))
NOTIFY_COOLDOWN_SECONDS = int(os.getenv("NOTIFY_COOLDOWN_SECONDS", "7200"))

LEVERAGE = int(os.getenv("LEVERAGE", "10"))
ACCOUNT_BALANCE = float(os.getenv("ACCOUNT_BALANCE", "1000"))
RISK_PERCENT = float(os.getenv("RISK_PERCENT", "1"))

SWING_LOOKBACK = int(os.getenv("SWING_LOOKBACK", "2"))
KLINES_LIMIT = int(os.getenv("KLINES_LIMIT", "200"))
DAILY_KLINES_LIMIT = int(os.getenv("DAILY_KLINES_LIMIT", "150"))
ENTRY_KLINES_LIMIT = int(os.getenv("ENTRY_KLINES_LIMIT", "200"))

MIN_RR_RATIO = float(os.getenv("MIN_RR_RATIO", "2.0"))
MIN_SIGNAL_SCORE = float(os.getenv("MIN_SIGNAL_SCORE", "65"))
MAX_EVENT_AGE_BARS = int(os.getenv("MAX_EVENT_AGE_BARS", "25"))

REQUIRE_TREND_ALIGN = env_bool("REQUIRE_TREND_ALIGN", True)
REQUIRE_SESSION_FILTER = env_bool("REQUIRE_SESSION_FILTER", False)
REQUIRE_15M_CONFIRMATION = env_bool("REQUIRE_15M_CONFIRMATION", False)
REQUIRE_BTC_FILTER = env_bool("REQUIRE_BTC_FILTER", False)

session_http = requests.Session()

_last_notified = {}
_cache = {}
_bot_stats = {
    "last_scan": None,
    "last_signal": None,
    "coins_scanned": 0,
    "valid_signals": 0,
    "last_error": None
}

def safe_get(url, params=None, timeout=8, retries=2):
    for attempt in range(retries + 1):
        try:
            response = session_http.get(url, params=params, timeout=timeout)
            if response.status_code == 200:
                return response
            logging.warning(f"HTTP {response.status_code}: {url}")
        except Exception as error:
            logging.warning(f"Request error {attempt + 1}: {error}")
        time.sleep(1 + attempt)
    return None

def cache_get(key, ttl):
    item = _cache.get(key)
    if not item:
        return None
    value, timestamp = item
    if time.time() - timestamp < ttl:
        return value
    return None

def cache_set(key, value):
    _cache[key] = (value, time.time())
    return value

def fetch_top_liquid_coins(limit=40):
    cache_key = f"coins_{limit}"
    cached = cache_get(cache_key, 300)
    if cached:
        return cached
    url = "https://api.bybit.com/v5/market/tickers"
    response = safe_get(url, {"category": "linear"}, timeout=10)
    try:
        if response:
            payload = response.json()
            if payload.get("retCode") == 0:
                rows = payload.get("result", {}).get("list", [])
                valid = []
                for row in rows:
                    symbol = row.get("symbol", "")
                    turnover = float(row.get("turnover24h") or 0)
                    if symbol.endswith("USDT") and turnover > 0:
                        valid.append(row)
                valid.sort(key=lambda x: float(x.get("turnover24h") or 0), reverse=True)
                symbols = [x["symbol"] for x in valid[:limit]]
                if symbols:
                    logging.info(f"{len(symbols)} likvid coin tapıldı.")
                    return cache_set(cache_key, symbols)
    except Exception as error:
        logging.error(f"Likvid coin xətası: {error}")
    return FALLBACK_COINS[:limit]

def fetch_klines(symbol, interval="60", limit=200):
    url = "https://api.bybit.com/v5/market/kline"
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    response = safe_get(url, params, timeout=10)
    try:
        if not response:
            return None
        payload = response.json()
        if payload.get("retCode") != 0:
            logging.warning(f"{symbol} API error: {payload.get('retMsg')}")
            return None
        rows = payload.get("result", {}).get("list", [])
        if len(rows) < 30:
            return None
        df = pd.DataFrame(rows, columns=[
            "timestamp", "open", "high", "low",
            "close", "volume", "turnover"
        ])
        df = df.iloc[::-1].reset_index(drop=True)
        for column in ["open", "high", "low", "close", "volume", "turnover"]:
            df[column] = pd.to_numeric(df[column], errors="coerce")
        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
        df = df.dropna().reset_index(drop=True)
        if len(df) < 30:
            return None
        return df.iloc[:-1].reset_index(drop=True)
    except Exception as error:
        logging.error(f"{symbol} kline xətası: {error}")
        return None

def compute_atr(df, period=14):
    previous_close = df["close"].shift(1)
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - previous_close).abs()
    tr3 = (df["low"] - previous_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=1).mean()

def compute_volume_ratio(df, period=20):
    average = df["volume"].rolling(period, min_periods=5).mean()
    return df["volume"] / average.replace(0, np.nan)

def find_swing_points(df, lookback=2):
    highs = df["high"].values
    lows = df["low"].values
    swing_highs = []
    swing_lows = []
    for index in range(lookback, len(df) - lookback):
        left_high = highs[index - lookback:index]
        right_high = highs[index + 1:index + lookback + 1]
        left_low = lows[index - lookback:index]
        right_low = lows[index + 1:index + lookback + 1]
        if highs[index] > np.max(left_high) and highs[index] > np.max(right_high):
            swing_highs.append((index, float(highs[index])))
        if lows[index] < np.min(left_low) and lows[index] < np.min(right_low):
            swing_lows.append((index, float(lows[index])))
    return swing_highs, swing_lows

def determine_trend_bias(swing_highs, swing_lows):
    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return "unknown"
    last_high = swing_highs[-1][1]
    previous_high = swing_highs[-2][1]
    last_low = swing_lows[-1][1]
    previous_low = swing_lows[-2][1]
    if last_high > previous_high and last_low > previous_low:
        return "bullish"
    if last_high < previous_high and last_low < previous_low:
        return "bearish"
    return "ranging"

def compute_structure_events(df, swing_highs, swing_lows):
    events = []
    close = df["close"].values
    trend_bias = None
    pivots = sorted(
        [(i, p, "high") for i, p in swing_highs] +
        [(i, p, "low") for i, p in swing_lows]
    )
    active_high = None
    active_low = None
    high_crossed = True
    low_crossed = True
    pointer = 0
    for index in range(len(df)):
        while pointer < len(pivots) and pivots[pointer][0] == index:
            pivot_index, price, pivot_type = pivots[pointer]
            if pivot_type == "high":
                active_high = (pivot_index, price)
                high_crossed = False
            else:
                active_low = (pivot_index, price)
                low_crossed = False
            pointer += 1
        if index == 0:
            continue
        if active_high and not high_crossed:
            if close[index - 1] <= active_high[1] < close[index]:
                kind = "CHoCH" if trend_bias == "bearish" else "BOS"
                trend_bias = "bullish"
                high_crossed = True
                events.append({
                    "index": index,
                    "bias": "bullish",
                    "kind": kind,
                    "level": active_high[1]
                })
        if active_low and not low_crossed:
            if close[index - 1] >= active_low[1] > close[index]:
                kind = "CHoCH" if trend_bias == "bullish" else "BOS"
                trend_bias = "bearish"
                low_crossed = True
                events.append({
                    "index": index,
                    "bias": "bearish",
                    "kind": kind,
                    "level": active_low[1]
                })
    return events

def detect_equal_levels(swing_highs, swing_lows, atr_series, threshold=0.20):
    equal_highs = []
    equal_lows = []
    for index in range(1, len(swing_highs)):
        i1, p1 = swing_highs[index - 1]
        i2, p2 = swing_highs[index]
        atr = float(atr_series.iloc[min(i2, len(atr_series) - 1)])
        if atr > 0 and abs(p1 - p2) <= atr * threshold:
            equal_highs.append((i1, i2, (p1 + p2) / 2))
    for index in range(1, len(swing_lows)):
        i1, p1 = swing_lows[index - 1]
        i2, p2 = swing_lows[index]
        atr = float(atr_series.iloc[min(i2, len(atr_series) - 1)])
        if atr > 0 and abs(p1 - p2) <= atr * threshold:
            equal_lows.append((i1, i2, (p1 + p2) / 2))
    return equal_highs, equal_lows

def detect_liquidity_sweep(df, direction, break_index, lookback_window=20):
    start = max(0, break_index - lookback_window)
    segment = df.iloc[start:break_index + 1].reset_index(drop=True)
    if len(segment) < 4:
        return False
    if direction == "bullish":
        for index in range(2, len(segment)):
            previous_low = segment["low"].iloc[:index].min()
            if segment["low"].iloc[index] < previous_low:
                if segment["close"].iloc[index] > previous_low:
                    return True
    else:
        for index in range(2, len(segment)):
            previous_high = segment["high"].iloc[:index].max()
            if segment["high"].iloc[index] > previous_high:
                if segment["close"].iloc[index] < previous_high:
                    return True
    return False

def find_fvg(df, direction, break_index, lookback_window=30):
    start = max(0, break_index - lookback_window)
    segment = df.iloc[start:break_index + 1].reset_index(drop=True)
    found = []
    for index in range(1, len(segment) - 1):
        previous_candle = segment.iloc[index - 1]
        next_candle = segment.iloc[index + 1]
        if direction == "bullish":
            if float(previous_candle["high"]) < float(next_candle["low"]):
                low = float(previous_candle["high"])
                high = float(next_candle["low"])
                found.append({"low": low, "high": high, "mid": (low + high) / 2})
        else:
            if float(previous_candle["low"]) > float(next_candle["high"]):
                low = float(next_candle["high"])
                high = float(previous_candle["low"])
                found.append({"low": low, "high": high, "mid": (low + high) / 2})
    return found[-1] if found else None

def detect_displacement(df, break_index, atr_series):
    if break_index <= 0 or break_index >= len(df):
        return False, 0.0
    candle = df.iloc[break_index]
    body = abs(float(candle["close"]) - float(candle["open"]))
    atr = float(atr_series.iloc[break_index])
    if atr <= 0:
        return False, 0.0
    ratio = body / atr
    return ratio >= 0.8, ratio

def find_order_block(df, direction, break_index, lookback=30):
    start = max(0, break_index - lookback)
    if direction == "bullish":
        candidates = [
            i for i in range(break_index - 1, start - 1, -1)
            if df["close"].iloc[i] < df["open"].iloc[i]
        ]
    else:
        candidates = [
            i for i in range(break_index - 1, start - 1, -1)
            if df["close"].iloc[i] > df["open"].iloc[i]
        ]
    if not candidates:
        return None
    ob_index = candidates[0]
    ob_high = float(df["high"].iloc[ob_index])
    ob_low = float(df["low"].iloc[ob_index])
    return {
        "index": ob_index,
        "high": ob_high,
        "low": ob_low,
        "mid": (ob_high + ob_low) / 2
    }

def find_next_liquidity(direction, current_price, swing_highs, swing_lows):
    if direction == "bullish":
        targets = [price for _, price in swing_highs if price > current_price]
        return min(targets) if targets else None
    targets = [price for _, price in swing_lows if price < current_price]
    return max(targets) if targets else None

def get_premium_discount(df, current_price, swing_highs, swing_lows):
    if not swing_highs or not swing_lows:
        return "unknown", None
    recent_high = swing_highs[-1][1]
    recent_low = swing_lows[-1][1]
    dealing_range = abs(recent_high - recent_low)
    if dealing_range <= 0:
        return "unknown", None
    equilibrium = (recent_high + recent_low) / 2
    if current_price < equilibrium:
        return "discount", equilibrium
    if current_price > equilibrium:
        return "premium", equilibrium
    return "equilibrium", equilibrium

def get_trading_session():
    hour = datetime.now(timezone.utc).hour
    london = 7 <= hour < 16
    new_york = 13 <= hour < 22
    active = london or new_york
    sessions = []
    if london:
        sessions.append("London")
    if new_york:
        sessions.append("New York")
    if not sessions:
        sessions.append("Asia")
    return active, " + ".join(sessions)

def get_daily_trend_bias(symbol):
    df = fetch_klines(symbol, "D", DAILY_KLINES_LIMIT)
    if df is None or len(df) < 40:
        return "unknown"
    highs, lows = find_swing_points(df, SWING_LOOKBACK)
    return determine_trend_bias(highs, lows)

def get_btc_market_bias():
    cached = cache_get("btc_bias", 60)
    if cached:
        return cached
    df = fetch_klines("BTCUSDT", "240", 120)
    if df is None or len(df) < 50:
        return "neutral"
    close = df["close"]
    ema20 = close.ewm(span=20, adjust=False).mean().iloc[-1]
    ema50 = close.ewm(span=50, adjust=False).mean().iloc[-1]
    price = close.iloc[-1]
    if price > ema20 > ema50:
        result = "bullish"
    elif price < ema20 < ema50:
        result = "bearish"
    else:
        result = "neutral"
    return cache_set("btc_bias", result)

def get_fear_greed():
    cached = cache_get("fear_greed", 300)
    if cached:
        return cached
    try:
        response = safe_get(
            "https://api.alternative.me/fng/",
            {"limit": 1},
            timeout=8,
            retries=1
        )
        if response:
            data = response.json().get("data", [])
            if data:
                value = int(data[0].get("value", 50))
                classification = data[0].get("value_classification", "Unknown")
                return cache_set("fear_greed", (value, classification))
    except Exception as error:
        logging.warning(f"Fear and Greed xətası: {error}")
    return 50, "Unknown"

def get_funding_rate(symbol):
    url = "https://api.bybit.com/v5/market/funding/history"
    params = {
        "category": "linear",
        "symbol": symbol,
        "limit": 1
    }
    try:
        response = safe_get(url, params, timeout=8, retries=1)
        if response:
            rows = response.json().get("result", {}).get("list", [])
            if rows:
                return float(rows[0].get("fundingRate", 0))
    except Exception as error:
        logging.warning(f"Funding xətası {symbol}: {error}")
    return None

def get_open_interest_trend(symbol):
    url = "https://api.bybit.com/v5/market/open-interest"
    params = {
        "category": "linear",
        "symbol": symbol,
        "intervalTime": "1h",
        "limit": 10
    }
    try:
        response = safe_get(url, params, timeout=8, retries=1)
        if response:
            rows = response.json().get("result", {}).get("list", [])
            if len(rows) >= 2:
                rows = rows[::-1]
                first = float(rows[0].get("openInterest", 0))
                last = float(rows[-1].get("openInterest", 0))
                if first > 0:
                    return ((last - first) / first) * 100
    except Exception as error:
        logging.warning(f"OI xətası {symbol}: {error}")
    return None

def check_15m_confirmation(symbol, direction):
    df = fetch_klines(symbol, "15", ENTRY_KLINES_LIMIT)
    if df is None or len(df) < 50:
        return False, {"reason": "15M data yoxdur"}
    highs, lows = find_swing_points(df, 2)
    events = compute_structure_events(df, highs, lows)
    if not events:
        return False, {"reason": "15M structure yoxdur"}
    event = events[-1]
    age = len(df) - 1 - event["index"]
    atr = compute_atr(df, 14)
    _, displacement_ratio = detect_displacement(df, event["index"], atr)
    volume_series = compute_volume_ratio(df, 20)
    volume_ratio = volume_series.iloc[-1]
    direction_ok = event["bias"] == direction
    fresh = age <= 12
    displacement_ok = displacement_ratio >= 0.5
    volume_ok = not pd.isna(volume_ratio) and volume_ratio >= 0.8
    confirmed = direction_ok and fresh and displacement_ok and volume_ok
    data = {
        "event": event["kind"],
        "age": age,
        "direction_ok": direction_ok,
        "fresh": fresh,
        "displacement": round(displacement_ratio, 2),
        "volume_ratio": round(float(volume_ratio), 2) if not pd.isna(volume_ratio) else 0
    }
    return confirmed, data

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

    fear_greed, classification = get_fear_greed()
    data["fear_greed"] = fear_greed
    data["fear_greed_class"] = classification

    if direction == "bullish":
        if 20 <= fear_greed <= 80:
            score += 5
    else:
        if 20 <= fear_greed <= 85:
            score += 5

    if fear_greed <= 10 or fear_greed >= 95:
        score -= 5

    funding = get_funding_rate(symbol)
    data["funding"] = funding

    if funding is not None:
        if direction == "bullish":
            if funding < 0:
                score += 5
            elif funding > 0.001:
                score -= 4
        else:
            if funding > 0:
                score += 5
            elif funding < -0.001:
                score -= 4

    oi_change = get_open_interest_trend(symbol)
    data["oi_change"] = oi_change

    if oi_change is not None:
        if oi_change > 0:
            score += 3
        elif oi_change < -5:
            score -= 3

    return score, data

def calculate_signal_score(data):
    score = 0

    if data.get("daily_trend_valid"):
        score += 10

    if data.get("trend_aligned"):
        score += 10

    if data.get("event_kind") == "CHoCH":
        score += 12
    elif data.get("event_kind") == "BOS":
        score += 8

    if data.get("sweep"):
        score += 10

    if data.get("equal_liquidity"):
        score += 4

    if data.get("fvg"):
        score += 8

    if data.get("order_block"):
        score += 8

    if data.get("zone_ok"):
        score += 8

    if data.get("premium_discount_ok"):
        score += 5

    displacement = data.get("displacement_ratio", 0)

    if displacement >= 1.5:
        score += 8
    elif displacement >= 1.0:
        score += 6
    elif displacement >= 0.5:
        score += 3

    volume = data.get("volume_ratio", 0)

    if volume >= 1.5:
        score += 7
    elif volume >= 1.1:
        score += 5
    elif volume >= 0.8:
        score += 2

    rr_ratio = data.get("rr_ratio", 0)

    if rr_ratio >= 4:
        score += 15
    elif rr_ratio >= 3:
        score += 12
    elif rr_ratio >= 2:
        score += 8

    if data.get("entry_confirmed"):
        score += 10

    score += data.get("fundamental_score", 0)

    return round(max(0, min(score, 100)), 1)

def analyze_smc_pro(symbol, session_active, session_name):
    conditions = {}

    daily_bias = get_daily_trend_bias(symbol)

    daily_valid = daily_bias in ("bullish", "bearish")

    conditions["Daily trend valid"] = daily_valid

    if not daily_valid:
        return {
            "symbol": symbol,
            "passed": False,
            "conditions": conditions,
            "score": 0,
            "error": None
        }

    df = fetch_klines(symbol, "60", KLINES_LIMIT)

    if df is None or len(df) < 70:
        return {
            "symbol": symbol,
            "passed": False,
            "conditions": conditions,
            "score": 0,
            "error": "1H data alınmadı"
        }

    swing_highs, swing_lows = find_swing_points(df, SWING_LOOKBACK)

    structure_ok = len(swing_highs) >= 2 and len(swing_lows) >= 2

    conditions["1H market structure"] = structure_ok

    if not structure_ok:
        return {
            "symbol": symbol,
            "passed": False,
            "conditions": conditions,
            "score": 0,
            "error": None
        }

    events = compute_structure_events(df, swing_highs, swing_lows)

    event_exists = len(events) > 0

    conditions["BOS/CHoCH event"] = event_exists

    if not event_exists:
        return {
            "symbol": symbol,
            "passed": False,
            "conditions": conditions,
            "score": 0,
            "error": None
        }

    last_event = events[-1]

    direction = last_event["bias"]
    event_kind = last_event["kind"]
    break_index = last_event["index"]

    event_age = len(df) - 1 - break_index

    fresh_event = event_age <= MAX_EVENT_AGE_BARS

    conditions["Fresh structure event"] = fresh_event

    atr = compute_atr(df, 14)

    trend_aligned = direction == daily_bias

    conditions["Daily and 1H trend aligned"] = trend_aligned

    equal_highs, equal_lows = detect_equal_levels(
        swing_highs,
        swing_lows,
        atr
    )

    equal_liquidity = (
        len(equal_lows) > 0
        if direction == "bullish"
        else len(equal_highs) > 0
    )

    conditions["Equal liquidity"] = equal_liquidity

    sweep = detect_liquidity_sweep(
        df,
        direction,
        break_index
    )

    conditions["Liquidity sweep"] = sweep

    displacement_ok, displacement_ratio = detect_displacement(
        df,
        break_index,
        atr
    )

    conditions["Displacement"] = displacement_ok

    volume_series = compute_volume_ratio(df, 20)

    if break_index < len(volume_series):
        volume_ratio = volume_series.iloc[break_index]
    else:
        volume_ratio = 0

    if pd.isna(volume_ratio):
        volume_ratio = 0

    volume_ratio = float(volume_ratio)

    volume_ok = volume_ratio >= 0.8

    conditions["Volume confirmation"] = volume_ok

    order_block = find_order_block(
        df,
        direction,
        break_index
    )

    ob_ok = order_block is not None

    conditions["Order Block"] = ob_ok

    fvg = find_fvg(
        df,
        direction,
        break_index
    )

    fvg_ok = fvg is not None

    conditions["Fair Value Gap"] = fvg_ok

    current_price = float(df["close"].iloc[-1])

    in_ob_zone = False

    if order_block:
        ob_buffer = (
            order_block["high"] -
            order_block["low"]
        ) * 0.25

        in_ob_zone = (
            order_block["low"] - ob_buffer
            <= current_price
            <= order_block["high"] + ob_buffer
        )

    in_fvg_zone = False

    if fvg:
        fvg_buffer = (
            fvg["high"] -
            fvg["low"]
        ) * 0.25

        in_fvg_zone = (
            fvg["low"] - fvg_buffer
            <= current_price
            <= fvg["high"] + fvg_buffer
        )

    zone_ok = in_ob_zone or in_fvg_zone

    conditions["Price in POI zone"] = zone_ok

    premium_discount, equilibrium = get_premium_discount(
        df,
        current_price,
        swing_highs,
        swing_lows
    )

    if direction == "bullish":
        premium_discount_ok = premium_discount in (
            "discount",
            "equilibrium"
        )
    else:
        premium_discount_ok = premium_discount in (
            "premium",
            "equilibrium"
        )

    conditions["Premium/Discount zone"] = premium_discount_ok

    liquidity_target = find_next_liquidity(
        direction,
        current_price,
        swing_highs,
        swing_lows
    )

    target_ok = liquidity_target is not None

    conditions["Liquidity TP target"] = target_ok

    if not target_ok:
        return {
            "symbol": symbol,
            "passed": False,
            "conditions": conditions,
            "score": 0,
            "error": None
        }

    entry = current_price

    atr_buffer = float(atr.iloc[-1]) * 0.20

    if direction == "bullish":
        if order_block:
            sl = order_block["low"] - atr_buffer
        else:
            sl = min(df["low"].iloc[-10:]) - atr_buffer

        tp = liquidity_target
        bias = f"🟢 LONG ({event_kind})"

    else:
        if order_block:
            sl = order_block["high"] + atr_buffer
        else:
            sl = max(df["high"].iloc[-10:]) + atr_buffer

        tp = liquidity_target
        bias = f"🔴 SHORT ({event_kind})"

    risk = abs(entry - sl)
    reward = abs(tp - entry)

    rr_ratio = (
        reward / risk
        if risk > 0
        else 0
    )

    rr_ok = rr_ratio >= MIN_RR_RATIO

    conditions["Minimum Risk Reward"] = rr_ok

    entry_confirmed, entry_data = check_15m_confirmation(
        symbol,
        direction
    )

    conditions["15M confirmation"] = entry_confirmed

    fundamental, fundamental_data = fundamental_score(
        symbol,
        direction
    )

    btc_alignment = fundamental_data.get("btc_alignment")

    btc_ok = btc_alignment is not False

    conditions["BTC market alignment"] = btc_ok

    session_ok = session_active

    conditions["Trading session"] = session_ok

    score_data = {
        "daily_trend_valid": daily_valid,
        "trend_aligned": trend_aligned,
        "event_kind": event_kind,
        "sweep": sweep,
        "equal_liquidity": equal_liquidity,
        "fvg": fvg_ok,
        "order_block": ob_ok,
        "zone_ok": zone_ok,
        "premium_discount_ok": premium_discount_ok,
        "displacement_ratio": displacement_ratio,
        "volume_ratio": volume_ratio,
        "rr_ratio": rr_ratio,
        "entry_confirmed": entry_confirmed,
        "fundamental_score": fundamental
    }

    score = calculate_signal_score(score_data)

    score_ok = score >= MIN_SIGNAL_SCORE

    conditions["Signal score"] = score_ok

    hard_conditions = [
        daily_valid,
        structure_ok,
        fresh_event,
        rr_ok
    ]

    if REQUIRE_TREND_ALIGN:
        hard_conditions.append(trend_aligned)

    if REQUIRE_SESSION_FILTER:
        hard_conditions.append(session_ok)

    if REQUIRE_15M_CONFIRMATION:
        hard_conditions.append(entry_confirmed)

    if REQUIRE_BTC_FILTER:
        hard_conditions.append(btc_ok)

    hard_passed = all(hard_conditions)

    passed = hard_passed and score_ok

    risk_amount = (
        ACCOUNT_BALANCE *
        RISK_PERCENT /
        100
    )

    position_size = (
        risk_amount / risk
        if risk > 0
        else 0
    )

    notional_value = (
        position_size *
        entry
    )

    margin_required = (
        notional_value /
        LEVERAGE
        if LEVERAGE > 0
        else notional_value
    )

    signal_id = (
        f"{symbol}_"
        f"{direction}_"
        f"{event_kind}_"
        f"{int(df['timestamp'].iloc[break_index])}"
    )

    return {
        "symbol": symbol,
        "passed": passed,
        "error": None,
        "conditions": conditions,
        "score": score,
        "bias": bias,
        "direction": direction,
        "event_kind": event_kind,
        "entry": round(entry, 6),
        "sl": round(sl, 6),
        "tp": round(tp, 6),
        "rr_ratio": round(rr_ratio, 2),
        "leverage": LEVERAGE,
        "risk_amount": round(risk_amount, 2),
        "position_size": round(position_size, 6),
        "notional_value": round(notional_value, 2),
        "margin_required": round(margin_required, 2),
        "daily_bias": daily_bias,
        "session": session_name,
        "event_age": event_age,
        "signal_id": signal_id,
        "fundamental": fundamental_data,
        "entry_confirmation": entry_data,
        "fvg": fvg,
        "order_block": order_block,
        "premium_discount": premium_discount,
        "equilibrium": equilibrium,
        "hard_passed": hard_passed
    }

def get_best_smc_signal():
    session_active, session_name = get_trading_session()

    coins = fetch_top_liquid_coins(
        SCAN_TOP_N_COINS
    )

    all_results = []

    for symbol in coins:
        try:
            result = analyze_smc_pro(
                symbol,
                session_active,
                session_name
            )

            all_results.append(result)

        except Exception as error:

            logging.error(
                f"{symbol} analiz xətası: {error}"
            )

            all_results.append({
                "symbol": symbol,
                "passed": False,
                "error": str(error),
                "conditions": {},
                "score": 0
            })

        time.sleep(0.05)

    _bot_stats["last_scan"] = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )

    _bot_stats["coins_scanned"] = len(coins)

    valid = [
        result
        for result in all_results
        if result.get("passed")
    ]

    valid.sort(
        key=lambda item: (
            item.get("score", 0),
            item.get("rr_ratio", 0)
        ),
        reverse=True
    )

    _bot_stats["valid_signals"] = len(valid)

    return (
        valid[0]
        if valid
        else None,
        all_results
    )

def format_diagnostics(all_results, max_detail=10):
    total = len(all_results)

    reasons = {}

    errors = 0

    for result in all_results:

        if result.get("error"):
            errors += 1
            continue

        failed = [
            name
            for name, passed in result.get(
                "conditions",
                {}
            ).items()
            if not passed
        ]

        if failed:
            reason = failed[0]
            reasons[reason] = (
                reasons.get(reason, 0)
                + 1
            )

    lines = [
        f"📋 Xülasə: {total} coin yoxlanıldı.",
        "",
        "Ən çox dayandıran şərtlər:"
    ]

    sorted_reasons = sorted(
        reasons.items(),
        key=lambda item: item[1],
        reverse=True
    )

    for reason, count in sorted_reasons[:10]:
        lines.append(
            f"• {reason}: {count}"
        )

    if errors:
        lines.append(
            f"• API/Analiz xətası: {errors}"
        )

    lines.append("")
    lines.append(
        f"İlk {min(max_detail, total)} coin:"
    )

    for result in all_results[:max_detail]:

        symbol = result.get(
            "symbol",
            "?"
        )

        score = result.get(
            "score",
            0
        )

        if result.get("error"):

            lines.append(
                f"• {symbol} ❌ {str(result['error'])[:60]}"
            )

            continue

        failed = [
            name
            for name, passed in result.get(
                "conditions",
                {}
            ).items()
            if not passed
        ]

        if failed:

            lines.append(
                f"• {symbol} ❌ {failed[0]} | Score {score}"
            )

        else:

            lines.append(
                f"• {symbol} ✅ PASS | Score {score}"
            )

    return "\n".join(lines)

def format_signal_message(
    result,
    title="📊 PROFESSIONAL SMC AI SİQNALI"
):

    fundamental = result.get(
        "fundamental",
        {}
    )

    entry_confirmation = result.get(
        "entry_confirmation",
        {}
    )

    fear_greed = fundamental.get(
        "fear_greed",
        "N/A"
    )

    funding = fundamental.get(
        "funding",
        "N/A"
    )

    oi_change = fundamental.get(
        "oi_change",
        "N/A"
    )

    return f"""
{title}

🪙 Coin: {result['symbol']}
⭐ Signal Score: {result['score']}/100
⚙️ Leverage: {result['leverage']}x

🎯 Setup: {result['bias']}
📈 Daily Trend: {result['daily_bias']}
🕒 Session: {result['session']}
📊 Premium/Discount: {result['premium_discount']}
⚖️ Risk/Reward: 1:{result['rr_ratio']}

📍 ENTRY: ${result['entry']}
🛑 STOP LOSS: ${result['sl']}
🎯 TAKE PROFIT: ${result['tp']}

📦 Position Size: {result['position_size']}
💰 Position Value: ${result['notional_value']}
💳 Estimated Margin: ${result['margin_required']}
⚠️ Maximum Risk: ${result['risk_amount']}

🌍 FUNDAMENTAL ANALYSIS

₿ BTC Bias: {fundamental.get('btc_bias')}
😨 Fear & Greed: {fear_greed}
💸 Funding Rate: {funding}
📈 Open Interest: {oi_change}%

🔎 15M Confirmation:
Event: {entry_confirmation.get('event', 'N/A')}
Fresh: {entry_confirmation.get('fresh', False)}
Displacement: {entry_confirmation.get('displacement', 0)}
Volume Ratio: {entry_confirmation.get('volume_ratio', 0)}

🆔 Signal ID:
{result['signal_id']}
"""

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    message = f"""
📊 Professional SMC AI Bot aktivdir!

Canlı analiz:
/analiz

Bot hər {CHECK_INTERVAL_SECONDS // 60} dəqiqədən bir bazarı avtomatik skan edir.

Status:
/status

Sistem:

Daily Trend
↓
1H Market Structure
↓
BOS / CHoCH
↓
Liquidity Sweep
↓
Equal High / Low
↓
Displacement
↓
Volume
↓
Order Block
↓
Fair Value Gap
↓
Premium / Discount
↓
15M Confirmation
↓
BTC Filter
↓
Funding
↓
Open Interest
↓
Fear & Greed
↓
Risk / Reward
↓
Signal Score
↓
Best Signal
"""

    await update.message.reply_text(
        message
    )

async def analiz(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(
        f"🔍 Ən likvid {SCAN_TOP_N_COINS} coin taranır...\n\n"
        "Daily + 1H SMC + 15M Entry + Fundamental Analysis yoxlanılır."
    )

    result, all_results = await asyncio.to_thread(
        get_best_smc_signal
    )

    if result:

        await update.message.reply_text(
            format_signal_message(result)
        )

    else:

        diagnostics = format_diagnostics(
            all_results
        )

        message = (
            "❌ Hazırda minimum keyfiyyət "
            "şərtlərini keçən setup yoxdur.\n\n"
            + diagnostics
        )

        await update.message.reply_text(
            message
        )

async def status(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    message = f"""
🤖 BOT STATUS

🟢 Bot: Aktiv
🪙 Scan coin sayı: {_bot_stats['coins_scanned']}
📊 Son scan: {_bot_stats['last_scan']}
🎯 Keçən setup: {_bot_stats['valid_signals']}
⏱ Scan interval: {CHECK_INTERVAL_SECONDS} saniyə
⭐ Minimum score: {MIN_SIGNAL_SCORE}
⚖️ Minimum RR: 1:{MIN_RR_RATIO}
⚙️ Leverage: {LEVERAGE}x
"""

    await update.message.reply_text(
        message
    )

async def send_auto_signal(
    application,
    result
):

    try:

        await application.bot.send_message(
            chat_id=CHAT_ID,
            text=format_signal_message(
                result,
                "🚨 AVTOMATİK PROFESSIONAL SİQNAL 🚨"
            )
        )

        _bot_stats["last_signal"] = (
            result["signal_id"]
        )

    except Exception as error:

        logging.error(
            f"Telegram göndərmə xətası: {error}"
        )

async def auto_signal_loop(application):

    await asyncio.sleep(15)

    while True:

        try:

            result, _ = await asyncio.to_thread(
                get_best_smc_signal
            )

            if result:

                signal_id = result[
                    "signal_id"
                ]

                current_time = time.time()

                last_time = _last_notified.get(
                    signal_id,
                    0
                )

                if (
                    current_time -
                    last_time
                    >=
                    NOTIFY_COOLDOWN_SECONDS
                ):

                    await send_auto_signal(
                        application,
                        result
                    )

                    _last_notified[
                        signal_id
                    ] = current_time

                    logging.info(
                        f"Signal sent: {signal_id}"
                    )

                else:

                    logging.info(
                        f"Cooldown active: {signal_id}"
                    )

        except Exception as error:

            _bot_stats["last_error"] = str(
                error
            )

            logging.error(
                f"Auto scan error: {error}"
            )

        await asyncio.sleep(
            CHECK_INTERVAL_SECONDS
        )

async def post_init(application):

    application.create_task(
        auto_signal_loop(application)
    )

    logging.info(
        "Automatic scanner started."
    )

def main():

    if not TOKEN:

        logging.error(
            "BOT_TOKEN tapılmadı."
        )

        return

    loop = asyncio.new_event_loop()

    asyncio.set_event_loop(loop)

    keep_alive()

    application = (
        ApplicationBuilder()
        .token(TOKEN)
        .post_init(post_init)
        .build()
    )

    application.add_handler(
        CommandHandler(
            "start",
            start
        )
    )

    application.add_handler(
        CommandHandler(
            "analiz",
            analiz
        )
    )

    application.add_handler(
        CommandHandler(
            "status",
            status
        )
    )

    logging.info(
        "Professional SMC AI Bot started."
    )

    application.run_polling(
        drop_pending_updates=True
    )

if __name__ == "__main__":
    main()
