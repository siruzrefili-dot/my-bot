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
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on", "beli", "bəli")

TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "1121794078")

FALLBACK_COINS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "LINKUSDT", "SUIUSDT"
]

SCAN_TOP_N_COINS = int(os.getenv("SCAN_TOP_N_COINS", "40"))
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))
NOTIFY_COOLDOWN_SECONDS = int(os.getenv("NOTIFY_COOLDOWN_SECONDS", "7200"))
LEVERAGE = int(os.getenv("LEVERAGE", "10"))

SWING_LOOKBACK = int(os.getenv("SWING_LOOKBACK", "2"))
KLINES_LIMIT = int(os.getenv("KLINES_LIMIT", "200"))
DAILY_KLINES_LIMIT = int(os.getenv("DAILY_KLINES_LIMIT", "150"))
ENTRY_KLINES_LIMIT = int(os.getenv("ENTRY_KLINES_LIMIT", "200"))

MIN_RR_RATIO = float(os.getenv("MIN_RR_RATIO", "2.0"))
MAX_EVENT_AGE_BARS = int(os.getenv("MAX_EVENT_AGE_BARS", "30"))

ACCOUNT_BALANCE = float(os.getenv("ACCOUNT_BALANCE", "1000"))
RISK_PERCENT = float(os.getenv("RISK_PERCENT", "1"))

MIN_SIGNAL_SCORE = float(os.getenv("MIN_SIGNAL_SCORE", "70"))
MIN_AUTO_SIGNAL_SCORE = float(os.getenv("MIN_AUTO_SIGNAL_SCORE", "75"))

REQUIRE_SESSION_FILTER = env_bool("REQUIRE_SESSION_FILTER", False)
REQUIRE_TREND_ALIGN = env_bool("REQUIRE_TREND_ALIGN", True)

session_http = requests.Session()
_last_notified = {}

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

def fetch_top_liquid_coins(limit=40):
    url = "https://api.bybit.com/v5/market/tickers"
    response = safe_get(url, {"category": "linear"}, timeout=10)

    try:
        if response:
            payload = response.json()

            if payload.get("retCode") == 0:
                rows = payload.get("result", {}).get("list", [])

                rows = [
                    row for row in rows
                    if row.get("symbol", "").endswith("USDT")
                    and float(row.get("turnover24h") or 0) > 0
                ]

                rows.sort(
                    key=lambda row: float(row.get("turnover24h") or 0),
                    reverse=True
                )

                symbols = [row["symbol"] for row in rows[:limit]]

                if symbols:
                    logging.info(f"{len(symbols)} coin skan edilir.")
                    return symbols

    except Exception as error:
        logging.error(f"Coin siyahısı xətası: {error}")

    return FALLBACK_COINS

def fetch_klines(symbol, interval="60", limit=200):
    url = "https://api.bybit.com/v5/market/kline"

    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }

    response = safe_get(url, params, timeout=8)

    try:
        if response:
            payload = response.json()

            if payload.get("retCode") == 0:
                rows = payload.get("result", {}).get("list", [])

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

                    df = df.iloc[::-1].reset_index(drop=True)

                    numeric_columns = [
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "turnover",
                        "timestamp"
                    ]

                    for column in numeric_columns:
                        df[column] = pd.to_numeric(
                            df[column],
                            errors="coerce"
                        )

                    df = df.dropna().reset_index(drop=True)

                    if len(df) > 10:
                        return df.iloc[:-1].reset_index(drop=True)

    except Exception as error:
        logging.error(f"{symbol} kline xətası: {error}")

    return None

def compute_atr(df, period=14):
    previous_close = df["close"].shift(1)

    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - previous_close).abs(),
            (df["low"] - previous_close).abs()
        ],
        axis=1
    ).max(axis=1)

    return tr.rolling(period, min_periods=1).mean()

def compute_volume_ratio(df, period=20):
    average_volume = df["volume"].rolling(
        period,
        min_periods=5
    ).mean()

    return df["volume"] / average_volume.replace(0, np.nan)

def find_swing_points(df, lookback=2):
    highs = df["high"].values
    lows = df["low"].values

    swing_highs = []
    swing_lows = []

    for index in range(
        lookback,
        len(df) - lookback
    ):

        current_high = highs[index]
        current_low = lows[index]

        left_high = highs[index - lookback:index]
        right_high = highs[index + 1:index + lookback + 1]

        left_low = lows[index - lookback:index]
        right_low = lows[index + 1:index + lookback + 1]

        if (
            current_high > np.max(left_high)
            and current_high > np.max(right_high)
        ):
            swing_highs.append(
                (index, float(current_high))
            )

        if (
            current_low < np.min(left_low)
            and current_low < np.min(right_low)
        ):
            swing_lows.append(
                (index, float(current_low))
            )

    return swing_highs, swing_lows

def determine_trend_bias(swing_highs, swing_lows):

    if len(swing_highs) < 2:
        return None

    if len(swing_lows) < 2:
        return None

    last_high = swing_highs[-1][1]
    previous_high = swing_highs[-2][1]

    last_low = swing_lows[-1][1]
    previous_low = swing_lows[-2][1]

    if last_high > previous_high and last_low > previous_low:
        return "bullish"

    if last_high < previous_high and last_low < previous_low:
        return "bearish"

    return "ranging"

def compute_structure_events(
    df,
    swing_highs,
    swing_lows
):

    events = []

    close = df["close"].values

    pivots = sorted(
        [(index, price, "high") for index, price in swing_highs]
        +
        [(index, price, "low") for index, price in swing_lows]
    )

    active_high = None
    active_low = None

    high_crossed = True
    low_crossed = True

    trend_bias = None
    pointer = 0

    for index in range(len(df)):

        while (
            pointer < len(pivots)
            and pivots[pointer][0] == index
        ):

            pivot_index, price, pivot_type = pivots[pointer]

            if pivot_type == "high":
                active_high = (
                    pivot_index,
                    price
                )
                high_crossed = False

            else:
                active_low = (
                    pivot_index,
                    price
                )
                low_crossed = False

            pointer += 1

        if index == 0:
            continue

        if (
            active_high
            and not high_crossed
            and close[index - 1] <= active_high[1]
            and close[index] > active_high[1]
        ):

            event_kind = (
                "CHoCH"
                if trend_bias == "bearish"
                else "BOS"
            )

            trend_bias = "bullish"
            high_crossed = True

            events.append(
                {
                    "index": index,
                    "bias": "bullish",
                    "kind": event_kind,
                    "level": active_high[1]
                }
            )

        if (
            active_low
            and not low_crossed
            and close[index - 1] >= active_low[1]
            and close[index] < active_low[1]
        ):

            event_kind = (
                "CHoCH"
                if trend_bias == "bullish"
                else "BOS"
            )

            trend_bias = "bearish"
            low_crossed = True

            events.append(
                {
                    "index": index,
                    "bias": "bearish",
                    "kind": event_kind,
                    "level": active_low[1]
                }
            )

    return events

def detect_liquidity_sweep(
    df,
    direction,
    break_index,
    lookback_window=20
):

    start = max(
        0,
        break_index - lookback_window
    )

    segment = df.iloc[
        start:break_index + 1
    ].reset_index(drop=True)

    if len(segment) < 4:
        return False

    if direction == "bullish":

        for index in range(2, len(segment)):

            previous_low = (
                segment["low"]
                .iloc[:index]
                .min()
            )

            if (
                segment["low"].iloc[index]
                < previous_low
                and segment["close"].iloc[index]
                > previous_low
            ):
                return True

    else:

        for index in range(2, len(segment)):

            previous_high = (
                segment["high"]
                .iloc[:index]
                .max()
            )

            if (
                segment["high"].iloc[index]
                > previous_high
                and segment["close"].iloc[index]
                < previous_high
            ):
                return True

    return False

def find_fvg(
    df,
    direction,
    break_index,
    lookback_window=25
):

    start = max(
        0,
        break_index - lookback_window
    )

    segment = df.iloc[
        start:break_index + 1
    ]

    found = []

    for index in range(
        1,
        len(segment) - 1
    ):

        previous_candle = segment.iloc[index - 1]
        next_candle = segment.iloc[index + 1]

        if direction == "bullish":

            if (
                float(previous_candle["high"])
                <
                float(next_candle["low"])
            ):

                low = float(
                    previous_candle["high"]
                )

                high = float(
                    next_candle["low"]
                )

                found.append(
                    {
                        "low": low,
                        "high": high,
                        "mid": (low + high) / 2
                    }
                )

        else:

            if (
                float(previous_candle["low"])
                >
                float(next_candle["high"])
            ):

                low = float(
                    next_candle["high"]
                )

                high = float(
                    previous_candle["low"]
                )

                found.append(
                    {
                        "low": low,
                        "high": high,
                        "mid": (low + high) / 2
                    }
                )

    return found[-1] if found else None

def detect_displacement(
    df,
    break_index,
    atr_series
):

    if break_index <= 0:
        return False, 0.0

    if break_index >= len(df):
        return False, 0.0

    candle = df.iloc[break_index]

    body = abs(
        float(candle["close"])
        -
        float(candle["open"])
    )

    atr = float(
        atr_series.iloc[break_index]
    )

    if atr <= 0:
        return False, 0.0

    ratio = body / atr

    return ratio >= 0.8, ratio

def find_order_block(
    df,
    direction,
    break_index,
    lookback=30
):

    start = max(
        0,
        break_index - lookback
    )

    if direction == "bullish":

        candidates = [
            index
            for index in range(
                break_index - 1,
                start - 1,
                -1
            )
            if df["close"].iloc[index]
            <
            df["open"].iloc[index]
        ]

    else:

        candidates = [
            index
            for index in range(
                break_index - 1,
                start - 1,
                -1
            )
            if df["close"].iloc[index]
            >
            df["open"].iloc[index]
        ]

    if not candidates:
        return None

    ob_index = candidates[0]

    ob_high = float(
        df["high"].iloc[ob_index]
    )

    ob_low = float(
        df["low"].iloc[ob_index]
    )

    mitigated = False

    for index in range(
        break_index + 1,
        len(df)
    ):

        if (
            direction == "bullish"
            and float(df["close"].iloc[index])
            < ob_low
        ):
            mitigated = True
            break

        if (
            direction == "bearish"
            and float(df["close"].iloc[index])
            > ob_high
        ):
            mitigated = True
            break

    return {
        "index": ob_index,
        "high": ob_high,
        "low": ob_low,
        "mid": (ob_high + ob_low) / 2,
        "mitigated": mitigated
    }

def find_next_liquidity(
    direction,
    current_price,
    swing_highs,
    swing_lows
):

    if direction == "bullish":

        targets = [
            price
            for _, price in swing_highs
            if price > current_price
        ]

        return (
            min(targets)
            if targets
            else None
        )

    targets = [
        price
        for _, price in swing_lows
        if price < current_price
    ]

    return (
        max(targets)
        if targets
        else None
    )

def get_trading_session():

    hour = datetime.now(
        timezone.utc
    ).hour

    london = 7 <= hour < 16
    new_york = 13 <= hour < 22

    active = london or new_york

    sessions = []

    if london:
        sessions.append("London")

    if new_york:
        sessions.append("New York")

    if sessions:
        name = " + ".join(sessions)
    else:
        name = "Asia"

    return active, name

def get_daily_trend_bias(symbol):

    df = fetch_klines(
        symbol,
        "D",
        DAILY_KLINES_LIMIT
    )

    if df is None:
        return None

    if len(df) < 40:
        return None

    swing_highs, swing_lows = find_swing_points(
        df,
        SWING_LOOKBACK
    )

    return determine_trend_bias(
        swing_highs,
        swing_lows
    )

def get_btc_market_bias():

    df = fetch_klines(
        "BTCUSDT",
        "240",
        120
    )

    if df is None:
        return None

    if len(df) < 50:
        return None

    close = df["close"]

    ema20 = close.ewm(
        span=20,
        adjust=False
    ).mean().iloc[-1]

    ema50 = close.ewm(
        span=50,
        adjust=False
    ).mean().iloc[-1]

    price = close.iloc[-1]

    if price > ema20 > ema50:
        return "bullish"

    if price < ema20 < ema50:
        return "bearish"

    return "neutral"

def get_fear_greed():

    try:

        response = safe_get(
            "https://api.alternative.me/fng/",
            {"limit": 1},
            timeout=6,
            retries=1
        )

        if response:

            data = response.json().get(
                "data",
                []
            )

            if data:

                value = int(
                    data[0].get(
                        "value",
                        50
                    )
                )

                classification = data[0].get(
                    "value_classification",
                    "Unknown"
                )

                return value, classification

    except Exception as error:

        logging.warning(
            f"Fear Greed xətası: {error}"
        )

    return None, "Unknown"

def get_funding_rate(symbol):

    url = (
        "https://api.bybit.com/"
        "v5/market/funding/history"
    )

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

            rows = (
                response.json()
                .get("result", {})
                .get("list", [])
            )

            if rows:

                return float(
                    rows[0].get(
                        "fundingRate",
                        0
                    )
                )

    except Exception as error:

        logging.warning(
            f"Funding xətası {symbol}: {error}"
        )

    return None

def get_open_interest_trend(symbol):

    url = (
        "https://api.bybit.com/"
        "v5/market/open-interest"
    )

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

            rows = (
                response.json()
                .get("result", {})
                .get("list", [])
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
                        /
                        first
                        *
                        100
                    )

    except Exception as error:

        logging.warning(
            f"OI xətası {symbol}: {error}"
        )

    return None

def check_15m_confirmation(
    symbol,
    direction
):

    df = fetch_klines(
        symbol,
        "15",
        ENTRY_KLINES_LIMIT
    )

    if df is None:
        return False, {}

    if len(df) < 50:
        return False, {}

    swing_highs, swing_lows = find_swing_points(
        df,
        2
    )

    events = compute_structure_events(
        df,
        swing_highs,
        swing_lows
    )

    if not events:

        return False, {
            "reason":
            "No structure event"
        }

    last_event = events[-1]

    age = (
        len(df)
        -
        1
        -
        last_event["index"]
    )

    atr = compute_atr(
        df,
        14
    )

    _, displacement_ratio = detect_displacement(
        df,
        last_event["index"],
        atr
    )

    volume_ratio = compute_volume_ratio(
        df,
        20
    ).iloc[-1]

    direction_ok = (
        last_event["bias"]
        ==
        direction
    )

    fresh = age <= 12

    displacement_ok = (
        displacement_ratio >= 0.5
    )

    volume_ok = (
        not pd.isna(volume_ratio)
        and volume_ratio >= 0.8
    )

    confirmed = (
        direction_ok
        and fresh
        and displacement_ok
        and volume_ok
    )

    return confirmed, {
        "event": last_event["kind"],
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

def fundamental_score(
    symbol,
    direction
):

    score = 0
    data = {}

    btc_bias = get_btc_market_bias()

    data["btc_bias"] = btc_bias

    if btc_bias == direction:

        score += 8
        data["btc_alignment"] = True

    elif btc_bias == "neutral":

        score += 2
        data["btc_alignment"] = None

    else:

        score -= 5
        data["btc_alignment"] = False

    fear_greed, classification = get_fear_greed()

    data["fear_greed"] = fear_greed
    data["fear_greed_class"] = classification

    if fear_greed is not None:

        if direction == "bullish":

            if fear_greed < 85:
                score += 4

        else:

            if fear_greed > 15:
                score += 4

        if (
            fear_greed <= 10
            or fear_greed >= 95
        ):
            score -= 3

    funding = get_funding_rate(
        symbol
    )

    data["funding"] = funding

    if funding is not None:

        if direction == "bullish":

            if funding < 0:
                score += 4

            elif funding > 0.001:
                score -= 3

        else:

            if funding > 0:
                score += 4

            elif funding < -0.001:
                score -= 3

    oi_change = get_open_interest_trend(
        symbol
    )

    data["oi_change"] = oi_change

    if oi_change is not None:

        if oi_change > 0:
            score += 3

        elif oi_change < -5:
            score -= 2

    return score, data

def calculate_signal_score(
    event_kind,
    trend_aligned,
    sweep,
    fvg,
    poi,
    displacement_ratio,
    volume_ratio,
    rr_ratio,
    entry_confirmed,
    session_active,
    fundamental
):

    score = 0

    score += 20

    if event_kind == "CHoCH":
        score += 8
    else:
        score += 5

    if trend_aligned:
        score += 15

    if sweep:
        score += 8

    if fvg:
        score += 5

    if poi:
        score += 10

    if displacement_ratio >= 1.5:
        score += 8

    elif displacement_ratio >= 0.8:
        score += 6

    elif displacement_ratio >= 0.5:
        score += 3

    if volume_ratio >= 1.5:
        score += 7

    elif volume_ratio >= 1.1:
        score += 5

    elif volume_ratio >= 0.8:
        score += 2

    if rr_ratio >= 4:
        score += 15

    elif rr_ratio >= 3:
        score += 12

    elif rr_ratio >= 2:
        score += 8

    if entry_confirmed:
        score += 8

    if session_active:
        score += 4

    score += fundamental

    return round(
        max(0, min(score, 100)),
        1
    )

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

    conditions[
        "Daily trend valid"
    ] = daily_valid

    if not daily_valid:

        return {
            "symbol": symbol,
            "passed": False,
            "error": None,
            "conditions": conditions,
            "score": 0
        }

    df = fetch_klines(
        symbol,
        "60",
        KLINES_LIMIT
    )

    if df is None:

        return {
            "symbol": symbol,
            "passed": False,
            "error": "1H data alınmadı",
            "conditions": conditions,
            "score": 0
        }

    if len(df) < 70:

        return {
            "symbol": symbol,
            "passed": False,
            "error": "1H data azdır",
            "conditions": conditions,
            "score": 0
        }

    swing_highs, swing_lows = (
        find_swing_points(
            df,
            SWING_LOOKBACK
        )
    )

    structure_ok = (
        len(swing_highs) >= 2
        and
        len(swing_lows) >= 2
    )

    conditions[
        "1H market structure"
    ] = structure_ok

    if not structure_ok:

        return {
            "symbol": symbol,
            "passed": False,
            "error": None,
            "conditions": conditions,
            "score": 0
        }

    events = compute_structure_events(
        df,
        swing_highs,
        swing_lows
    )

    event_ok = len(events) > 0

    conditions[
        "BOS or CHoCH event"
    ] = event_ok

    if not event_ok:

        return {
            "symbol": symbol,
            "passed": False,
            "error": None,
            "conditions": conditions,
            "score": 0
        }

    last_event = events[-1]

    direction = last_event["bias"]
    event_kind = last_event["kind"]
    break_index = last_event["index"]

    event_age = (
        len(df)
        -
        1
        -
        break_index
    )

    fresh_event = (
        event_age
        <=
        MAX_EVENT_AGE_BARS
    )

    conditions[
        "Fresh structure event"
    ] = fresh_event

    if not fresh_event:

        return {
            "symbol": symbol,
            "passed": False,
            "error": None,
            "conditions": conditions,
            "score": 0
        }

    trend_aligned = (
        direction
        ==
        daily_bias
    )

    conditions[
        "Daily and 1H trend aligned"
    ] = trend_aligned

    if (
        REQUIRE_TREND_ALIGN
        and not trend_aligned
    ):

        return {
            "symbol": symbol,
            "passed": False,
            "error": None,
            "conditions": conditions,
            "score": 0
        }

    atr = compute_atr(
        df,
        14
    )

    sweep = detect_liquidity_sweep(
        df,
        direction,
        break_index
    )

    conditions[
        "Liquidity sweep"
    ] = sweep

    _, displacement_ratio = (
        detect_displacement(
            df,
            break_index,
            atr
        )
    )

    displacement_ok = (
        displacement_ratio
        >=
        0.5
    )

    conditions[
        "Displacement"
    ] = displacement_ok

    volume_series = compute_volume_ratio(
        df,
        20
    )

    volume_ratio = float(
        volume_series.iloc[break_index]
    )

    if pd.isna(volume_ratio):
        volume_ratio = 0.0

    volume_ok = (
        volume_ratio
        >=
        0.8
    )

    conditions[
        "Volume confirmation"
    ] = volume_ok

    order_block = find_order_block(
        df,
        direction,
        break_index
    )

    ob_valid = (
        order_block is not None
    )

    conditions[
        "Order Block found"
    ] = ob_valid

    fvg = find_fvg(
        df,
        direction,
        break_index
    )

    fvg_ok = (
        fvg is not None
    )

    conditions[
        "Fair Value Gap"
    ] = fvg_ok

    current_price = float(
        df["close"].iloc[-1]
    )

    poi = False

    if order_block:

        ob_buffer = (
            order_block["high"]
            -
            order_block["low"]
        ) * 0.20

        in_ob = (
            order_block["low"]
            -
            ob_buffer
            <=
            current_price
            <=
            order_block["high"]
            +
            ob_buffer
        )

        poi = in_ob

    if fvg:

        fvg_buffer = (
            fvg["high"]
            -
            fvg["low"]
        ) * 0.20

        in_fvg = (
            fvg["low"]
            -
            fvg_buffer
            <=
            current_price
            <=
            fvg["high"]
            +
            fvg_buffer
        )

        poi = poi or in_fvg

    conditions[
        "Price in POI zone"
    ] = poi

    liquidity_target = (
        find_next_liquidity(
            direction,
            current_price,
            swing_highs,
            swing_lows
        )
    )

    if liquidity_target is None:

        return {
            "symbol": symbol,
            "passed": False,
            "error": None,
            "conditions": conditions,
            "score": 0
        }

    if order_block is None:

        return {
            "symbol": symbol,
            "passed": False,
            "error": None,
            "conditions": conditions,
            "score": 0
        }

    sl_buffer = (
        float(atr.iloc[-1])
        *
        0.15
    )

    entry = current_price

    if direction == "bullish":

        sl = (
            order_block["low"]
            -
            sl_buffer
        )

        tp = liquidity_target

        bias = (
            f"🟢 LONG ({event_kind})"
        )

    else:

        sl = (
            order_block["high"]
            +
            sl_buffer
        )

        tp = liquidity_target

        bias = (
            f"🔴 SHORT ({event_kind})"
        )

    risk = abs(
        entry - sl
    )

    reward = abs(
        tp - entry
    )

    rr_ratio = (
        reward / risk
        if risk > 0
        else 0
    )

    rr_ok = (
        rr_ratio
        >=
        MIN_RR_RATIO
    )

    conditions[
        "Risk Reward minimum"
    ] = rr_ok

    if not rr_ok:

        return {
            "symbol": symbol,
            "passed": False,
            "error": None,
            "conditions": conditions,
            "score": 0
        }

    entry_confirmed, entry_data = (
        check_15m_confirmation(
            symbol,
            direction
        )
    )

    conditions[
        "15M entry confirmation"
    ] = entry_confirmed

    fundamental, fundamental_data = (
        fundamental_score(
            symbol,
            direction
        )
    )

    conditions[
        "BTC alignment"
    ] = (
        fundamental_data.get(
            "btc_alignment"
        )
        is not False
    )

    session_ok = (
        session_active
        or
        not REQUIRE_SESSION_FILTER
    )

    conditions[
        "Trading session"
    ] = session_ok

    if not session_ok:

        return {
            "symbol": symbol,
            "passed": False,
            "error": None,
            "conditions": conditions,
            "score": 0
        }

    score = calculate_signal_score(
        event_kind,
        trend_aligned,
        sweep,
        fvg_ok,
        poi,
        displacement_ratio,
        volume_ratio,
        rr_ratio,
        entry_confirmed,
        session_active,
        fundamental
    )

    score_ok = (
        score
        >=
        MIN_SIGNAL_SCORE
    )

    conditions[
        "Minimum signal score"
    ] = score_ok

    risk_amount = (
        ACCOUNT_BALANCE
        *
        RISK_PERCENT
        /
        100
    )

    position_size = (
        risk_amount / risk
        if risk > 0
        else 0
    )

    notional_value = (
        position_size
        *
        entry
    )

    margin_required = (
        notional_value
        /
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
        "passed": score_ok,
        "error": None,
        "conditions": conditions,
        "bias": bias,
        "direction": direction,
        "event_kind": event_kind,
        "entry": round(entry, 6),
        "sl": round(sl, 6),
        "tp": round(tp, 6),
        "rr_ratio": round(rr_ratio, 2),
        "leverage": LEVERAGE,
        "risk_amount": round(
            risk_amount,
            2
        ),
        "position_size": round(
            position_size,
            6
        ),
        "notional_value": round(
            notional_value,
            2
        ),
        "margin_required": round(
            margin_required,
            2
        ),
        "daily_bias": daily_bias,
        "session": session_name,
        "score": score,
        "event_age": event_age,
        "signal_id": signal_id,
        "fundamental": fundamental_data,
        "entry_confirmation": entry_data,
        "sweep": sweep,
        "fvg": fvg_ok,
        "poi": poi,
        "displacement": round(
            displacement_ratio,
            2
        ),
        "volume_ratio": round(
            volume_ratio,
            2
        )
    }

def get_best_smc_signal():

    session_active, session_name = (
        get_trading_session()
    )

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

            all_results.append(
                result
            )

        except Exception as error:

            logging.error(
                f"{symbol} analiz xətası: {error}"
            )

            all_results.append(
                {
                    "symbol": symbol,
                    "passed": False,
                    "error": str(error),
                    "conditions": {},
                    "score": 0
                }
            )

        time.sleep(0.1)

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

    best = (
        valid[0]
        if valid
        else None
    )

    return best, all_results

def format_diagnostics(
    all_results,
    max_detail=10
):

    total = len(all_results)

    reasons = {}

    for result in all_results:

        if result.get("error"):
            reasons["API or analysis error"] = (
                reasons.get(
                    "API or analysis error",
                    0
                )
                +
                1
            )
            continue

        failed = [
            name
            for name, ok
            in result.get(
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
                +
                1
            )

    lines = []

    lines.append(
        f"📋 Xülasə: {total} coin yoxlanıldı."
    )

    lines.append("")
    lines.append(
        "Ən çox zəif olan şərtlər:"
    )

    sorted_reasons = sorted(
        reasons.items(),
        key=lambda item: item[1],
        reverse=True
    )

    for reason, count in sorted_reasons[:10]:

        lines.append(
            f"• {reason}: {count}"
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
                f"• {symbol} ❌ Error"
            )

        elif result.get("passed"):

            lines.append(
                f"• {symbol} ✅ PASS | Score {score}"
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
                else "Minimum quality"
            )

            lines.append(
                f"• {symbol} ❌ {reason} | Score {score}"
            )

    return "\n".join(lines)

def signal_quality(score):

    if score >= 85:
        return "🔥 PREMIUM"

    if score >= 75:
        return "🟢 STRONG"

    if score >= 65:
        return "🟡 WATCHLIST"

    return "🔴 LOW"

def format_signal_message(
    result,
    title="📊 PROFESSIONAL SMC AI SIGNAL"
):

    fundamental = result.get(
        "fundamental",
        {}
    )

    quality = signal_quality(
        result["score"]
    )

    return f"""{title}

{quality}

🪙 Coin: {result["symbol"]}
⭐ Score: {result["score"]}/100
🎯 Setup: {result["bias"]}

📈 Daily Trend: {result["daily_bias"]}
🕒 Session: {result["session"]}
⚖️ Risk Reward: 1:{result["rr_ratio"]}

📍 ENTRY: {result["entry"]}
🛑 STOP LOSS: {result["sl"]}
🎯 TAKE PROFIT: {result["tp"]}

⚙️ Leverage: {result["leverage"]}x
💰 Position Value: ${result["notional_value"]}
💳 Estimated Margin: ${result["margin_required"]}
⚠️ Risk: ${result["risk_amount"]}

🔎 SMC ANALYSIS
💧 Liquidity Sweep: {result["sweep"]}
📦 POI Zone: {result["poi"]}
📊 FVG: {result["fvg"]}
⚡ Displacement: {result["displacement"]}
📈 Volume Ratio: {result["volume_ratio"]}

⏱ 15M Confirmation:
{result["entry_confirmation"]}

🌍 FUNDAMENTAL
₿ BTC Bias: {fundamental.get("btc_bias")}
😨 Fear and Greed: {fundamental.get("fear_greed")}
💰 Funding: {fundamental.get("funding")}
📊 Open Interest: {fundamental.get("oi_change")}

🆔 Signal ID:
{result["signal_id"]}
"""

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    message = f"""📊 Professional SMC AI Bot aktivdir!

Canlı analiz üçün:
/analiz

Bot hər {CHECK_INTERVAL_SECONDS // 60} dəqiqədən bir bazarı avtomatik skan edir.

Sistem:

Daily Trend
↓
1H Market Structure
↓
BOS / CHoCH
↓
Liquidity
↓
Displacement
↓
Volume
↓
Order Block / FVG
↓
POI
↓
15M Confirmation
↓
Fundamental Analysis
↓
Quality Score
↓
Best Signal

Vacib fərq:

Sweep, FVG, POI, Displacement və Volume artıq setup-u dərhal rədd etmir.

Onlar Signal Score sisteminə təsir edir."""

    await update.message.reply_text(
        message
    )

async def analiz(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(
        f"🔍 {SCAN_TOP_N_COINS} likvid coin analiz edilir. Gözləyin..."
    )

    result, all_results = (
        await asyncio.to_thread(
            get_best_smc_signal
        )
    )

    if result:

        await update.message.reply_text(
            format_signal_message(
                result
            )
        )

    else:

        diagnostics = (
            format_diagnostics(
                all_results
            )
        )

        message = (
            "❌ Hazırda minimum keyfiyyət "
            "şərtlərini keçən setup yoxdur.\n\n"
            +
            diagnostics
        )

        await update.message.reply_text(
            message
        )

async def send_auto_signal(
    application,
    result
):

    if not CHAT_ID:
        logging.warning(
            "CHAT_ID yoxdur."
        )
        return

    try:

        await application.bot.send_message(
            chat_id=CHAT_ID,
            text=format_signal_message(
                result,
                "🚨 AUTOMATIC SMC SIGNAL 🚨"
            )
        )

    except Exception as error:

        logging.error(
            f"Telegram göndərmə xətası: {error}"
        )

async def auto_signal_loop(
    application
):

    await asyncio.sleep(10)

    while True:

        try:

            result, _ = (
                await asyncio.to_thread(
                    get_best_smc_signal
                )
            )

            if result:

                score = result.get(
                    "score",
                    0
                )

                if score >= MIN_AUTO_SIGNAL_SCORE:

                    signal_id = (
                        result["signal_id"]
                    )

                    now = time.time()

                    last_time = (
                        _last_notified.get(
                            signal_id,
                            0
                        )
                    )

                    if (
                        now - last_time
                        >=
                        NOTIFY_COOLDOWN_SECONDS
                    ):

                        await send_auto_signal(
                            application,
                            result
                        )

                        _last_notified[
                            signal_id
                        ] = now

                        logging.info(
                            f"Signal sent: {signal_id}"
                        )

        except Exception as error:

            logging.error(
                f"Auto scan error: {error}"
            )

        await asyncio.sleep(
            CHECK_INTERVAL_SECONDS
        )

async def post_init(
    application
):

    application.create_task(
        auto_signal_loop(
            application
        )
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
        "Professional SMC AI Bot started."
    )

    application.run_polling(
        drop_pending_updates=True
    )

if __name__ == "__main__":
    main()
                
