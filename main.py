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

# ================= CONFIG =================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def env_bool(name, default=True):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")

TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "1121794078")

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

ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))
VOLUME_PERIOD = int(os.getenv("VOLUME_PERIOD", "20"))

MIN_RR_RATIO = float(os.getenv("MIN_RR_RATIO", "2.0"))
MIN_SIGNAL_SCORE = float(os.getenv("MIN_SIGNAL_SCORE", "70"))
MAX_EVENT_AGE_BARS = int(os.getenv("MAX_EVENT_AGE_BARS", "20"))

# ================= FILTERS =================

REQUIRE_TREND_ALIGN = env_bool("REQUIRE_TREND_ALIGN", True)
REQUIRE_LIQUIDITY_SWEEP = env_bool("REQUIRE_LIQUIDITY_SWEEP", True)
REQUIRE_FVG = env_bool("REQUIRE_FVG", True)
REQUIRE_VOLUME = env_bool("REQUIRE_VOLUME", True)
REQUIRE_DISPLACEMENT = env_bool("REQUIRE_DISPLACEMENT", True)
REQUIRE_POI = env_bool("REQUIRE_POI", True)
REQUIRE_15M_CONFIRMATION = env_bool("REQUIRE_15M_CONFIRMATION", True)
REQUIRE_BTC_FILTER = env_bool("REQUIRE_BTC_FILTER", True)

# ================= COINS =================

FALLBACK_COINS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "AVAXUSDT",
    "DOGEUSDT",
    "LINKUSDT",
    "SUIUSDT",
    "ARBUSDT",
    "OPUSDT"
]

# ================= HTTP =================

session_http = requests.Session()
_last_notified = {}

def safe_get(url, params=None, timeout=8, retries=2):
    for attempt in range(retries + 1):
        try:
            response = session_http.get(
                url,
                params=params,
                timeout=timeout
            )

            if response.status_code == 200:
                return response

            logging.warning(
                f"HTTP {response.status_code}: {url}"
            )

        except Exception as e:
            logging.warning(
                f"Request error {attempt + 1}: {e}"
            )

        time.sleep(1 + attempt)

    return None

# ================= BYBIT COINS =================

def fetch_top_liquid_coins(limit=SCAN_TOP_N_COINS):

    url = "https://api.bybit.com/v5/market/tickers"

    response = safe_get(
        url,
        {"category": "linear"},
        timeout=10
    )

    try:

        if response:

            payload = response.json()

            if payload.get("retCode") == 0:

                rows = payload.get(
                    "result",
                    {}
                ).get(
                    "list",
                    []
                )

                rows = [
                    r for r in rows
                    if r.get(
                        "symbol",
                        ""
                    ).endswith("USDT")
                    and float(
                        r.get(
                            "turnover24h",
                            0
                        ) or 0
                    ) > 0
                ]

                rows.sort(
                    key=lambda r: float(
                        r.get(
                            "turnover24h",
                            0
                        ) or 0
                    ),
                    reverse=True
                )

                symbols = [
                    r["symbol"]
                    for r in rows[:limit]
                ]

                if symbols:

                    logging.info(
                        f"{len(symbols)} likvid coin taranır"
                    )

                    return symbols

    except Exception as e:

        logging.error(
            f"Coin siyahısı xətası: {e}"
        )

    return FALLBACK_COINS

# ================= KLINES =================

def fetch_klines(symbol, interval="60", limit=200):

    url = "https://api.bybit.com/v5/market/kline"

    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }

    response = safe_get(
        url,
        params,
        timeout=8
    )

    try:

        if response:

            payload = response.json()

            if payload.get("retCode") == 0:

                rows = payload.get(
                    "result",
                    {}
                ).get(
                    "list",
                    []
                )

                if len(rows) > 30:

                    df = pd.DataFrame(
                        rows,
                        columns=[
                            "timestamp",
                            "open",
                            "high",
                            "low",
                            "close",
                            "volume",
                            "turnover"
                        ]
                    )

                    df = df.iloc[::-1].reset_index(
                        drop=True
                    )

                    for col in [
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "turnover"
                    ]:

                        df[col] = pd.to_numeric(
                            df[col],
                            errors="coerce"
                        )

                    df["timestamp"] = pd.to_numeric(
                        df["timestamp"],
                        errors="coerce"
                    )

                    df = df.dropna().reset_index(
                        drop=True
                    )

                    if len(df) > 5:

                        # Açıq və hələ bağlanmamış
                        # son şamı analizdən çıxarırıq
                        return df.iloc[:-1].reset_index(
                            drop=True
                        )

            else:

                logging.warning(
                    f"{symbol} API error: "
                    f"{payload.get('retMsg')}"
                )

    except Exception as e:

        logging.error(
            f"{symbol} kline error: {e}"
        )

    return None

# ================= ATR =================

def compute_atr(df, period=ATR_PERIOD):

    previous_close = df["close"].shift(1)

    tr1 = df["high"] - df["low"]

    tr2 = (
        df["high"] - previous_close
    ).abs()

    tr3 = (
        df["low"] - previous_close
    ).abs()

    true_range = pd.concat(
        [
            tr1,
            tr2,
            tr3
        ],
        axis=1
    ).max(axis=1)

    atr = true_range.rolling(
        period,
        min_periods=1
    ).mean()

    return atr

# ================= VOLUME =================

def compute_volume_ratio(
    df,
    period=VOLUME_PERIOD
):

    average_volume = df[
        "volume"
    ].rolling(
        period,
        min_periods=5
    ).mean()

    ratio = df["volume"] / average_volume.replace(
        0,
        np.nan
    )

    return ratio

# ================= EMA =================

def compute_ema(
    series,
    period
):

    return series.ewm(
        span=period,
        adjust=False
    ).mean()

# ================= SWING POINTS =================

def find_swing_points(
    df,
    lookback=SWING_LOOKBACK
):

    highs = df["high"].values
    lows = df["low"].values

    swing_highs = []
    swing_lows = []

    for i in range(
        lookback,
        len(df) - lookback
    ):

        left_high = highs[
            i - lookback:i
        ]

        right_high = highs[
            i + 1:i + lookback + 1
        ]

        if (
            highs[i] > np.max(left_high)
            and highs[i] > np.max(right_high)
        ):

            swing_highs.append(
                (
                    i,
                    float(highs[i])
                )
            )

        left_low = lows[
            i - lookback:i
        ]

        right_low = lows[
            i + 1:i + lookback + 1
        ]

        if (
            lows[i] < np.min(left_low)
            and lows[i] < np.min(right_low)
        ):

            swing_lows.append(
                (
                    i,
                    float(lows[i])
                )
            )

    return swing_highs, swing_lows

# ================= DAILY TREND =================

def determine_trend_bias(
    swing_highs,
    swing_lows
):

    if (
        len(swing_highs) < 2
        or len(swing_lows) < 2
    ):

        return None

    last_high = swing_highs[-1][1]
    previous_high = swing_highs[-2][1]

    last_low = swing_lows[-1][1]
    previous_low = swing_lows[-2][1]

    higher_high = (
        last_high > previous_high
    )

    higher_low = (
        last_low > previous_low
    )

    lower_high = (
        last_high < previous_high
    )

    lower_low = (
        last_low < previous_low
    )

    if higher_high and higher_low:

        return "bullish"

    if lower_high and lower_low:

        return "bearish"

    return "ranging"

def get_daily_trend_bias(symbol):

    df = fetch_klines(
        symbol,
        "D",
        DAILY_KLINES_LIMIT
    )

    if df is None or len(df) < 40:

        return None

    swing_highs, swing_lows = find_swing_points(
        df,
        SWING_LOOKBACK
    )

    return determine_trend_bias(
        swing_highs,
        swing_lows
    )

# ================= BTC MARKET FILTER =================

def get_btc_market_bias():

    df = fetch_klines(
        "BTCUSDT",
        "240",
        120
    )

    if df is None or len(df) < 60:

        return None

    close = df["close"]

    ema20 = compute_ema(
        close,
        20
    ).iloc[-1]

    ema50 = compute_ema(
        close,
        50
    ).iloc[-1]

    price = close.iloc[-1]

    if (
        price > ema20
        and ema20 > ema50
    ):

        return "bullish"

    if (
        price < ema20
        and ema20 < ema50
    ):

        return "bearish"

    return "neutral"

# ================= TRADING SESSION =================

def get_trading_session():

    hour = datetime.now(
        timezone.utc
    ).hour

    london = (
        7 <= hour < 16
    )

    new_york = (
        13 <= hour < 22
    )

    active = london or new_york

    sessions = []

    if london:
        sessions.append("London")

    if new_york:
        sessions.append("New York")

    session_name = (
        " + ".join(sessions)
        if sessions
        else "Asia"
    )

    return active, session_name

# ================= FLASK KEEP ALIVE =================

app_flask = Flask(__name__)

@app_flask.route("/")
def home():

    return (
        "Professional SMC AI Bot "
        "is running!"
    )

def run_flask():

    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )

    app_flask.run(
        host="0.0.0.0",
        port=port
    )

def keep_alive():

    Thread(
        target=run_flask,
        daemon=True
    ).start()
    # ================= MARKET STRUCTURE =================

def compute_structure_events(df, swing_highs, swing_lows):
    events = []
    close = df["close"].values
    trend_bias = None
    pivots = sorted(
        [(i, p, "high") for i, p in swing_highs] +
        [(i, p, "low") for i, p in swing_lows],
        key=lambda x: x[0]
    )
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
        if active_high and not high_crossed:
            if close[i - 1] <= active_high[1] and close[i] > active_high[1]:
                kind = "CHoCH" if trend_bias == "bearish" else "BOS"
                trend_bias = "bullish"
                high_crossed = True
                events.append({
                    "index": i,
                    "bias": "bullish",
                    "kind": kind,
                    "level": active_high[1]
                })
        if active_low and not low_crossed:
            if close[i - 1] >= active_low[1] and close[i] < active_low[1]:
                kind = "CHoCH" if trend_bias == "bullish" else "BOS"
                trend_bias = "bearish"
                low_crossed = True
                events.append({
                    "index": i,
                    "bias": "bearish",
                    "kind": kind,
                    "level": active_low[1]
                })
    return events

# ================= LIQUIDITY SWEEP =================

def detect_liquidity_sweep(df, direction, break_idx, lookback_window=20):
    start = max(0, break_idx - lookback_window)
    segment = df.iloc[start:break_idx + 1].reset_index(drop=True)
    if len(segment) < 4:
        return False
    if direction == "bullish":
        for i in range(2, len(segment)):
            prior_low = segment["low"].iloc[:i].min()
            current_low = float(segment["low"].iloc[i])
            current_close = float(segment["close"].iloc[i])
            if current_low < prior_low and current_close > prior_low:
                return True
    elif direction == "bearish":
        for i in range(2, len(segment)):
            prior_high = segment["high"].iloc[:i].max()
            current_high = float(segment["high"].iloc[i])
            current_close = float(segment["close"].iloc[i])
            if current_high > prior_high and current_close < prior_high:
                return True
    return False

# ================= EQUAL HIGHS / LOWS =================

def detect_equal_levels(swing_highs, swing_lows, atr_series, threshold=0.15):
    equal_highs = []
    equal_lows = []
    for i in range(1, len(swing_highs)):
        idx1, price1 = swing_highs[i - 1]
        idx2, price2 = swing_highs[i]
        atr = float(atr_series.iloc[min(idx2, len(atr_series) - 1)])
        if atr > 0 and abs(price1 - price2) <= atr * threshold:
            equal_highs.append({
                "index1": idx1,
                "index2": idx2,
                "price": (price1 + price2) / 2
            })
    for i in range(1, len(swing_lows)):
        idx1, price1 = swing_lows[i - 1]
        idx2, price2 = swing_lows[i]
        atr = float(atr_series.iloc[min(idx2, len(atr_series) - 1)])
        if atr > 0 and abs(price1 - price2) <= atr * threshold:
            equal_lows.append({
                "index1": idx1,
                "index2": idx2,
                "price": (price1 + price2) / 2
            })
    return equal_highs, equal_lows

# ================= FAIR VALUE GAP =================

def find_fvg(df, direction, break_idx, lookback_window=25):
    start = max(0, break_idx - lookback_window)
    end = min(len(df), break_idx + 1)
    segment = df.iloc[start:end].reset_index(drop=True)
    found = []
    for i in range(1, len(segment) - 1):
        first = segment.iloc[i - 1]
        third = segment.iloc[i + 1]
        if direction == "bullish":
            if float(first["high"]) < float(third["low"]):
                low = float(first["high"])
                high = float(third["low"])
                found.append({
                    "low": low,
                    "high": high,
                    "mid": (low + high) / 2,
                    "direction": "bullish"
                })
        elif direction == "bearish":
            if float(first["low"]) > float(third["high"]):
                low = float(third["high"])
                high = float(first["low"])
                found.append({
                    "low": low,
                    "high": high,
                    "mid": (low + high) / 2,
                    "direction": "bearish"
                })
    return found[-1] if found else None

# ================= DISPLACEMENT =================

def detect_displacement(df, break_idx, atr_series):
    if break_idx <= 0 or break_idx >= len(df):
        return False, 0.0
    candle = df.iloc[break_idx]
    body = abs(float(candle["close"]) - float(candle["open"]))
    atr = float(atr_series.iloc[break_idx])
    if atr <= 0:
        return False, 0.0
    ratio = body / atr
    strong = ratio >= 0.6
    return strong, ratio

# ================= ORDER BLOCK =================

def find_order_block(df, direction, break_idx, lookback=30):
    start = max(0, break_idx - lookback)
    if direction == "bullish":
        candidates = [
            i for i in range(break_idx - 1, start - 1, -1)
            if float(df["close"].iloc[i]) < float(df["open"].iloc[i])
        ]
    else:
        candidates = [
            i for i in range(break_idx - 1, start - 1, -1)
            if float(df["close"].iloc[i]) > float(df["open"].iloc[i])
        ]
    if not candidates:
        return None
    ob_idx = candidates[0]
    ob_high = float(df["high"].iloc[ob_idx])
    ob_low = float(df["low"].iloc[ob_idx])
    mitigated = False
    for j in range(break_idx + 1, len(df)):
        high = float(df["high"].iloc[j])
        low = float(df["low"].iloc[j])
        if direction == "bullish":
            if low <= ob_low:
                mitigated = True
                break
        else:
            if high >= ob_high:
                mitigated = True
                break
    return {
        "index": ob_idx,
        "high": ob_high,
        "low": ob_low,
        "mid": (ob_high + ob_low) / 2,
        "mitigated": mitigated,
        "direction": direction
    }

# ================= POI ZONE =================

def price_in_zone(price, zone, buffer_percent=0.15):
    if zone is None:
        return False
    low = float(zone["low"])
    high = float(zone["high"])
    size = high - low
    if size <= 0:
        return low <= price <= high
    buffer = size * buffer_percent
    return (low - buffer) <= price <= (high + buffer)

def get_poi_status(current_price, ob, fvg):
    in_ob = price_in_zone(current_price, ob)
    in_fvg = price_in_zone(current_price, fvg)
    return in_ob or in_fvg, in_ob, in_fvg

# ================= LIQUIDITY TARGET =================

def find_next_liquidity(direction, current_price, swing_highs, swing_lows):
    if direction == "bullish":
        targets = [price for _, price in swing_highs if price > current_price]
        return min(targets) if targets else None
    targets = [price for _, price in swing_lows if price < current_price]
    return max(targets) if targets else None

# ================= RISK / REWARD =================

def calculate_trade_levels(direction, entry, ob, liquidity_target, atr_value):
    if ob is None or liquidity_target is None:
        return None
    atr_buffer = atr_value * 0.15
    if direction == "bullish":
        sl = float(ob["low"]) - atr_buffer
        tp = float(liquidity_target)
    else:
        sl = float(ob["high"]) + atr_buffer
        tp = float(liquidity_target)
    risk = abs(entry - sl)
    reward = abs(tp - entry)
    if risk <= 0:
        return None
    rr_ratio = reward / risk
    return {
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "risk": risk,
        "reward": reward,
        "rr_ratio": rr_ratio
    }

# ================= POSITION SIZE =================

def calculate_position_size(entry, sl):
    risk_amount = ACCOUNT_BALANCE * RISK_PERCENT / 100
    stop_distance = abs(entry - sl)
    if stop_distance <= 0:
        return {
            "risk_amount": risk_amount,
            "position_size": 0,
            "notional_value": 0,
            "margin_required": 0
        }
    position_size = risk_amount / stop_distance
    notional_value = position_size * entry
    margin_required = notional_value / LEVERAGE if LEVERAGE > 0 else notional_value
    return {
        "risk_amount": risk_amount,
        "position_size": position_size,
        "notional_value": notional_value,
        "margin_required": margin_required
    }

# ================= SMC DATA =================

def analyze_1h_smc(symbol):
    df = fetch_klines(symbol, "60", KLINES_LIMIT)
    if df is None or len(df) < 70:
        return None, "1H data alınmadı"
    swing_highs, swing_lows = find_swing_points(df, SWING_LOOKBACK)
    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return None, "Kifayət qədər swing yoxdur"
    events = compute_structure_events(df, swing_highs, swing_lows)
    if not events:
        return None, "BOS/CHoCH yoxdur"
    last_event = events[-1]
    break_idx = last_event["index"]
    event_age = len(df) - 1 - break_idx
    if event_age > MAX_EVENT_AGE_BARS:
        return None, "Structure event köhnədir"
    atr = compute_atr(df, ATR_PERIOD)
    volume_series = compute_volume_ratio(df, VOLUME_PERIOD)
    displacement_ok, displacement_ratio = detect_displacement(
        df,
        break_idx,
        atr
    )
    volume_ratio = float(volume_series.iloc[break_idx]) if not pd.isna(volume_series.iloc[break_idx]) else 0.0
    sweep = detect_liquidity_sweep(
        df,
        last_event["bias"],
        break_idx
    )
    fvg = find_fvg(
        df,
        last_event["bias"],
        break_idx
    )
    ob = find_order_block(
        df,
        last_event["bias"],
        break_idx
    )
    current_price = float(df["close"].iloc[-1])
    poi_ok, in_ob, in_fvg = get_poi_status(
        current_price,
        ob,
        fvg
    )
    equal_highs, equal_lows = detect_equal_levels(
        swing_highs,
        swing_lows,
        atr
    )
    target = find_next_liquidity(
        last_event["bias"],
        current_price,
        swing_highs,
        swing_lows
    )
    return {
        "df": df,
        "direction": last_event["bias"],
        "event_kind": last_event["kind"],
        "break_idx": break_idx,
        "event_age": event_age,
        "swing_highs": swing_highs,
        "swing_lows": swing_lows,
        "equal_highs": equal_highs,
        "equal_lows": equal_lows,
        "atr": atr,
        "atr_value": float(atr.iloc[-1]),
        "volume_ratio": volume_ratio,
        "displacement_ok": displacement_ok,
        "displacement_ratio": displacement_ratio,
        "sweep": sweep,
        "fvg": fvg,
        "ob": ob,
        "current_price": current_price,
        "poi_ok": poi_ok,
        "in_ob": in_ob,
        "in_fvg": in_fvg,
        "target": target
    }, None
    # ================= FUNDAMENTAL ANALYSIS =================

def get_fear_greed():
    try:
        response = safe_get(
            "https://api.alternative.me/fng/",
            {"limit": 1},
            timeout=6,
            retries=1
        )
        if response:
            data = response.json().get("data", [])
            if data:
                value = int(data[0].get("value", 50))
                classification = data[0].get(
                    "value_classification",
                    "Unknown"
                )
                return value, classification
    except Exception as e:
        logging.warning(f"Fear and Greed error: {e}")
    return None, "Unknown"

def get_funding_rate(symbol):
    url = "https://api.bybit.com/v5/market/funding/history"
    params = {
        "category": "linear",
        "symbol": symbol,
        "limit": 1
    }
    try:
        response = safe_get(
            url,
            params,
            timeout=6,
            retries=1
        )
        if response:
            rows = response.json().get(
                "result",
                {}
            ).get(
                "list",
                []
            )
            if rows:
                return float(
                    rows[0].get(
                        "fundingRate",
                        0
                    )
                )
    except Exception as e:
        logging.warning(
            f"Funding error {symbol}: {e}"
        )
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
        response = safe_get(
            url,
            params,
            timeout=6,
            retries=1
        )
        if response:
            rows = response.json().get(
                "result",
                {}
            ).get(
                "list",
                []
            )
            if len(rows) >= 2:
                rows = rows[::-1]
                first = float(
                    rows[0].get(
                        "openInterest",
                        0
                    )
                )
                last = float(
                    rows[-1].get(
                        "openInterest",
                        0
                    )
                )
                if first > 0:
                    return (
                        (last - first)
                        / first
                    ) * 100
    except Exception as e:
        logging.warning(
            f"Open Interest error {symbol}: {e}"
        )
    return None

def fundamental_score(symbol, direction):
    score = 0
    data = {}

    btc_bias = get_btc_market_bias()
    data["btc_bias"] = btc_bias

    if btc_bias == direction:
        score += 10
        data["btc_alignment"] = True
    elif btc_bias == "neutral":
        score += 3
        data["btc_alignment"] = None
    else:
        score -= 8
        data["btc_alignment"] = False

    fear_greed, fg_class = get_fear_greed()

    data["fear_greed"] = fear_greed
    data["fear_greed_class"] = fg_class

    if fear_greed is not None:
        if direction == "bullish":
            if fear_greed < 80:
                score += 5
            if fear_greed >= 90:
                score -= 5
        else:
            if fear_greed > 20:
                score += 5
            if fear_greed <= 10:
                score -= 5

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

# ================= 15M CONFIRMATION =================

def check_15m_confirmation(symbol, direction):
    df = fetch_klines(
        symbol,
        "15",
        ENTRY_KLINES_LIMIT
    )

    if df is None or len(df) < 50:
        return False, {
            "reason": "15M data yoxdur"
        }

    swing_highs, swing_lows = find_swing_points(
        df,
        SWING_LOOKBACK
    )

    events = compute_structure_events(
        df,
        swing_highs,
        swing_lows
    )

    if not events:
        return False, {
            "reason": "15M structure yoxdur"
        }

    last = events[-1]

    age = (
        len(df)
        - 1
        - last["index"]
    )

    atr = compute_atr(
        df,
        ATR_PERIOD
    )

    volume_series = compute_volume_ratio(
        df,
        VOLUME_PERIOD
    )

    _, displacement_ratio = detect_displacement(
        df,
        last["index"],
        atr
    )

    volume_ratio = volume_series.iloc[-1]

    direction_ok = (
        last["bias"] == direction
    )

    fresh = age <= 12

    displacement_ok = (
        displacement_ratio >= 0.5
    )

    volume_ok = (
        not pd.isna(volume_ratio)
        and float(volume_ratio) >= 0.7
    )

    confirmed = (
        direction_ok
        and fresh
        and displacement_ok
        and volume_ok
    )

    return confirmed, {
        "event": last["kind"],
        "direction_ok": direction_ok,
        "age": age,
        "fresh": fresh,
        "displacement": round(
            displacement_ratio,
            2
        ),
        "volume_ratio": round(
            float(volume_ratio),
            2
        )
        if not pd.isna(volume_ratio)
        else 0.0
    }

# ================= SIGNAL SCORE =================

def calculate_signal_score(
    event_kind,
    rr_ratio,
    sweep,
    fvg,
    poi_ok,
    displacement_ratio,
    volume_ratio,
    entry_confirmed,
    fundamental
):
    score = 0

    if event_kind == "CHoCH":
        score += 15
    else:
        score += 12

    if sweep:
        score += 12

    if fvg:
        score += 8

    if poi_ok:
        score += 10

    if rr_ratio >= 4:
        score += 18
    elif rr_ratio >= 3:
        score += 14
    elif rr_ratio >= 2:
        score += 10

    if displacement_ratio >= 1.5:
        score += 10
    elif displacement_ratio >= 0.8:
        score += 7
    elif displacement_ratio >= 0.5:
        score += 4

    if volume_ratio >= 1.5:
        score += 8
    elif volume_ratio >= 1.0:
        score += 5
    elif volume_ratio >= 0.7:
        score += 2

    if entry_confirmed:
        score += 15

    score += fundamental

    return round(
        max(
            0,
            min(score, 100)
        ),
        1
    )

# ================= MAIN SMC ANALYSIS =================

def analyze_smc_pro(
    symbol,
    session_active,
    session_name
):
    conditions = {}

    daily_bias = get_daily_trend_bias(
        symbol
    )

    daily_valid = (
        daily_bias
        in (
            "bullish",
            "bearish"
        )
    )

    conditions["Daily trend valid"] = daily_valid

    if not daily_valid:
        return {
            "symbol": symbol,
            "passed": False,
            "conditions": conditions,
            "score": 0
        }

    smc, error = analyze_1h_smc(symbol)

    if smc is None:
        return {
            "symbol": symbol,
            "passed": False,
            "error": error,
            "conditions": conditions,
            "score": 0
        }

    direction = smc["direction"]

    trend_aligned = (
        direction == daily_bias
    )

    conditions[
        "Daily and 1H trend aligned"
    ] = trend_aligned

    if REQUIRE_TREND_ALIGN:
        if not trend_aligned:
            return {
                "symbol": symbol,
                "passed": False,
                "conditions": conditions,
                "score": 0
            }

    sweep = smc["sweep"]

    conditions[
        "Liquidity sweep"
    ] = sweep

    if REQUIRE_LIQUIDITY_SWEEP:
        if not sweep:
            return {
                "symbol": symbol,
                "passed": False,
                "conditions": conditions,
                "score": 0
            }

    displacement_ok = smc[
        "displacement_ok"
    ]

    displacement_ratio = smc[
        "displacement_ratio"
    ]

    conditions[
        "Displacement"
    ] = displacement_ok

    if REQUIRE_DISPLACEMENT:
        if not displacement_ok:
            return {
                "symbol": symbol,
                "passed": False,
                "conditions": conditions,
                "score": 0
            }

    volume_ratio = smc[
        "volume_ratio"
    ]

    volume_ok = (
        volume_ratio >= 0.7
    )

    conditions[
        "Volume confirmation"
    ] = volume_ok

    if REQUIRE_VOLUME:
        if not volume_ok:
            return {
                "symbol": symbol,
                "passed": False,
                "conditions": conditions,
                "score": 0
            }

    ob = smc["ob"]

    ob_ok = (
        ob is not None
    )

    conditions[
        "Order Block found"
    ] = ob_ok

    if not ob_ok:
        return {
            "symbol": symbol,
            "passed": False,
            "conditions": conditions,
            "score": 0
        }

    fvg = smc["fvg"]

    fvg_ok = (
        fvg is not None
    )

    conditions[
        "Fair Value Gap"
    ] = fvg_ok

    if REQUIRE_FVG:
        if not fvg_ok:
            return {
                "symbol": symbol,
                "passed": False,
                "conditions": conditions,
                "score": 0
            }

    poi_ok = smc[
        "poi_ok"
    ]

    conditions[
        "Price in POI zone"
    ] = poi_ok

    if REQUIRE_POI:
        if not poi_ok:
            return {
                "symbol": symbol,
                "passed": False,
                "conditions": conditions,
                "score": 0
            }

    target = smc["target"]

    target_ok = (
        target is not None
    )

    conditions[
        "Liquidity target"
    ] = target_ok

    if not target_ok:
        return {
            "symbol": symbol,
            "passed": False,
            "conditions": conditions,
            "score": 0
        }

    entry = smc[
        "current_price"
    ]

    levels = calculate_trade_levels(
        direction,
        entry,
        ob,
        target,
        smc["atr_value"]
    )

    if levels is None:
        return {
            "symbol": symbol,
            "passed": False,
            "conditions": conditions,
            "score": 0
        }

    rr_ratio = levels[
        "rr_ratio"
    ]

    rr_ok = (
        rr_ratio >= MIN_RR_RATIO
    )

    conditions[
        f"Risk Reward >= 1:{MIN_RR_RATIO}"
    ] = rr_ok

    if not rr_ok:
        return {
            "symbol": symbol,
            "passed": False,
            "conditions": conditions,
            "score": 0
        }

    conditions[
        "Active trading session"
    ] = session_active

    entry_confirmed, entry_data = (
        check_15m_confirmation(
            symbol,
            direction
        )
    )

    conditions[
        "15M confirmation"
    ] = entry_confirmed

    if REQUIRE_15M_CONFIRMATION:
        if not entry_confirmed:
            return {
                "symbol": symbol,
                "passed": False,
                "conditions": conditions,
                "score": 0
            }

    fundamental, fundamental_data = (
        fundamental_score(
            symbol,
            direction
        )
    )

    btc_alignment = fundamental_data.get(
        "btc_alignment"
    )

    btc_ok = (
        btc_alignment is not False
    )

    conditions[
        "BTC market alignment"
    ] = btc_ok

    if REQUIRE_BTC_FILTER:
        if not btc_ok:
            return {
                "symbol": symbol,
                "passed": False,
                "conditions": conditions,
                "score": 0
            }

    score = calculate_signal_score(
        smc["event_kind"],
        rr_ratio,
        sweep,
        fvg_ok,
        poi_ok,
        displacement_ratio,
        volume_ratio,
        entry_confirmed,
        fundamental
    )

    score_ok = (
        score >= MIN_SIGNAL_SCORE
    )

    conditions[
        "Minimum signal score"
    ] = score_ok

    if not score_ok:
        return {
            "symbol": symbol,
            "passed": False,
            "conditions": conditions,
            "score": score
        }

    position_data = calculate_position_size(
        levels["entry"],
        levels["sl"]
    )

    bias = (
        "🟢 LONG"
        if direction == "bullish"
        else "🔴 SHORT"
    )

    signal_id = (
        f"{symbol}_"
        f"{direction}_"
        f"{smc['event_kind']}_"
        f"{int(time.time() // 900)}"
    )

    return {
        "symbol": symbol,
        "passed": True,
        "conditions": conditions,
        "score": score,
        "bias": bias,
        "direction": direction,
        "event_kind": smc["event_kind"],
        "entry": round(
            levels["entry"],
            8
        ),
        "sl": round(
            levels["sl"],
            8
        ),
        "tp": round(
            levels["tp"],
            8
        ),
        "rr_ratio": round(
            rr_ratio,
            2
        ),
        "leverage": LEVERAGE,
        "daily_bias": daily_bias,
        "session": session_name,
        "risk_amount": round(
            position_data[
                "risk_amount"
            ],
            2
        ),
        "position_size": round(
            position_data[
                "position_size"
            ],
            6
        ),
        "notional_value": round(
            position_data[
                "notional_value"
            ],
            2
        ),
        "margin_required": round(
            position_data[
                "margin_required"
            ],
            2
        ),
        "sweep": sweep,
        "poi_ok": poi_ok,
        "fvg_ok": fvg_ok,
        "displacement_ratio": round(
            displacement_ratio,
            2
        ),
        "volume_ratio": round(
            volume_ratio,
            2
        ),
        "entry_confirmation": entry_data,
        "fundamental": fundamental_data,
        "signal_id": signal_id
    }

# ================= SCAN MARKET =================

def get_best_smc_signal():

    session_active, session_name = (
        get_trading_session()
    )

    coins = fetch_top_liquid_coins()

    all_results = []

    for symbol in coins:

        try:

            result = analyze_smc_pro(
                symbol,
                session_active,
                session_name
            )

            all_results.append(
                result
            )

        except Exception as e:

            logging.error(
                f"{symbol} analiz error: {e}"
            )

            all_results.append({
                "symbol": symbol,
                "passed": False,
                "error": str(e),
                "conditions": {},
                "score": 0
            })

        time.sleep(0.15)

    valid = [
        result
        for result in all_results
        if result.get("passed")
    ]

    valid.sort(
        key=lambda x: (
            x.get("score", 0),
            x.get("rr_ratio", 0)
        ),
        reverse=True
    )

    return (
        valid[0]
        if valid
        else None,
        all_results
    )

# ================= DIAGNOSTICS =================

def format_diagnostics(
    all_results,
    max_detail=10
):

    total = len(all_results)

    reasons = {}

    for result in all_results:

        failed = [
            name
            for name, ok in result.get(
                "conditions",
                {}
            ).items()
            if not ok
        ]

        if failed:

            reason = failed[0]

            reasons[reason] = (
                reasons.get(
                    reason,
                    0
                )
                + 1
            )

    lines = [
        "📋 *Xülasə:* "
        f"`{total}` coin yoxlanıldı.",
        "",
        "*Ən çox dayandıran şərtlər:*"
    ]

    for reason, count in sorted(
        reasons.items(),
        key=lambda x: x[1],
        reverse=True
    )[:10]:

        lines.append(
            f"• {reason}: `{count}`"
        )

    lines.append(
        ""
    )

    lines.append(
        f"*İlk {min(max_detail, total)} coin:*"
    )

    for result in all_results[
        :max_detail
    ]:

        symbol = result.get(
            "symbol",
            "?"
        )

        score = result.get(
            "score",
            0
        )

        if result.get("passed"):

            lines.append(
                f"• `{symbol}` "
                f"✅ PASS "
                f"| Score `{score}`"
            )

        elif result.get("error"):

            lines.append(
                f"• `{symbol}` ❌ "
                f"{str(result['error'])[:60]}"
            )

        else:

            failed = [
                name
                for name, ok
                in result.get(
                    "conditions",
                    {}
                ).items()
                if not ok
            ]

            reason = (
                failed[0]
                if failed
                else "No setup"
            )

            lines.append(
                f"• `{symbol}` ❌ "
                f"{reason} "
                f"| Score `{score}`"
            )

    return "\n".join(
        lines
    )

# ================= SIGNAL MESSAGE =================

def format_signal_message(
    res,
    title="🚨 *AUTOMATIC SMC SIGNAL* 🚨"
):

    fundamental = res.get(
        "fundamental",
        {}
    )

    fg = fundamental.get(
        "fear_greed"
    )

    funding = fundamental.get(
        "funding"
    )

    oi = fundamental.get(
        "oi_change"
    )

    entry_confirmation = res.get(
        "entry_confirmation",
        {}
    )

    strength = (
        "🔥 VERY STRONG"
        if res["score"] >= 90
        else "🟢 STRONG"
    )

    return f"""{title}

{strength}

🪙 *Coin:* `{res['symbol']}`
⭐ *Score:* `{res['score']}/100`
🎯 *Setup:* {res['bias']} ({res['event_kind']})

📈 *Daily Trend:* `{res['daily_bias']}`
🕒 *Session:* `{res['session']}`
⚖️ *Risk Reward:* `1:{res['rr_ratio']}`

📍 *ENTRY:* `{res['entry']}`
🛑 *STOP LOSS:* `{res['sl']}`
🎯 *TAKE PROFIT:* `{res['tp']}`

⚙️ *Leverage:* `{res['leverage']}x`
💰 *Position Value:* `${res['notional_value']}`
💳 *Estimated Margin:* `${res['margin_required']}`
⚠️ *Risk:* `${res['risk_amount']}`

🔎 *SMC ANALYSIS*
💧 Liquidity Sweep: `{res['sweep']}`
📦 POI Zone: `{res['poi_ok']}`
📊 FVG: `{res['fvg_ok']}`
⚡ Displacement: `{res['displacement_ratio']}`
📈 Volume Ratio: `{res['volume_ratio']}`

⏱ *15M Confirmation:*
`{entry_confirmation}`

🌍 *FUNDAMENTAL*
₿ BTC Bias: `{fundamental.get('btc_bias')}`
😨 Fear and Greed: `{fg}`
💰 Funding: `{funding}`
📊 Open Interest: `{oi}`

🆔 *Signal ID:*
`{res['signal_id']}`"""

# ================= TELEGRAM COMMANDS =================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    message = f"""📊 *Professional SMC AI Bot aktivdir!*

Canlı analiz üçün:

/analiz

Bot avtomatik olaraq hər
`{CHECK_INTERVAL_SECONDS // 60}` dəqiqədən bir
bazarı skan edir.

Sistem:

Daily Trend
⬇️
1H Market Structure
⬇️
BOS / CHoCH
⬇️
Liquidity Sweep
⬇️
Displacement
⬇️
Volume
⬇️
Order Block
⬇️
FVG
⬇️
POI Zone
⬇️
15M Confirmation
⬇️
Fundamental Analysis
⬇️
Signal Score
⬇️
Best Signal 🚨"""

    await update.message.reply_text(
        message,
        parse_mode="Markdown"
    )

async def analiz(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(
        f"🔍 Ən likvid "
        f"`{SCAN_TOP_N_COINS}` coin "
        f"taranır...",
        parse_mode="Markdown"
    )

    result, all_results = (
        await asyncio.to_thread(
            get_best_smc_signal
        )
    )

    if result:

        await update.message.reply_text(
            format_signal_message(
                result,
                "📊 *LIVE SMC ANALYSIS*"
            ),
            parse_mode="Markdown"
        )

    else:

        diagnostics = format_diagnostics(
            all_results
        )

        message = (
            "❌ Hazırda minimum "
            "keyfiyyət şərtlərini keçən "
            "setup yoxdur.\n\n"
            + diagnostics
        )

        await update.message.reply_text(
            message,
            parse_mode="Markdown"
        )

# ================= SEND SIGNAL =================

async def send_auto_signal(
    application,
    result
):

    try:

        await application.bot.send_message(
            chat_id=CHAT_ID,
            text=format_signal_message(
                result
            ),
            parse_mode="Markdown"
        )

        logging.info(
            f"Signal sent: "
            f"{result['signal_id']}"
        )

    except Exception as e:

        logging.error(
            f"Telegram send error: {e}"
        )

# ================= AUTO SCANNER =================

async def auto_signal_loop(
    application
):

    await asyncio.sleep(15)

    while True:

        try:

            logging.info(
                "Automatic market scan started..."
            )

            result, _ = (
                await asyncio.to_thread(
                    get_best_smc_signal
                )
            )

            if result:

                signal_id = result[
                    "signal_id"
                ]

                now = time.time()

                last_time = (
                    _last_notified.get(
                        signal_id,
                        0
                    )
                )

                if (
                    now - last_time
                    >= NOTIFY_COOLDOWN_SECONDS
                ):

                    await send_auto_signal(
                        application,
                        result
                    )

                    _last_notified[
                        signal_id
                    ] = now

                else:

                    logging.info(
                        "Signal cooldown active"
                    )

            else:

                logging.info(
                    "No valid setup found"
                )

        except Exception as e:

            logging.error(
                f"Auto scanner error: {e}"
            )

        await asyncio.sleep(
            CHECK_INTERVAL_SECONDS
        )

# ================= BOT START =================

async def post_init(application):

    application.create_task(
        auto_signal_loop(
            application
        )
    )

    logging.info(
        "Automatic scanner started"
    )

# ================= MAIN =================

def main():

    if not TOKEN:

        logging.error(
            "BOT_TOKEN tapılmadı. "
            "Environment Variables yoxlayın."
        )

        return

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

    logging.info(
        "Professional SMC AI Bot started!"
    )

    application.run_polling(
        drop_pending_updates=True
    )

if __name__ == "__main__":
    main()
 
