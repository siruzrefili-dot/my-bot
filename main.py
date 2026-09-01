import os
import asyncio
import logging
import time
from datetime import datetime, timezone

import requests
import pandas as pd
from flask import Flask
from threading import Thread
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

app_flask = Flask(__name__)

@app_flask.route("/")
def home():
    return "Professional SMC Bot is running!"

def run_flask():
    port = int(os.getenv("PORT", "10000"))
    app_flask.run(host="0.0.0.0", port=port)

def keep_alive():
    Thread(target=run_flask, daemon=True).start()

def env_bool(name, default=True):
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in (
        "1", "true", "yes", "beli", "bəli", "on"
    )

TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "1121794078")

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
    "SUIUSDT"
]

SCAN_TOP_N_COINS = int(
    os.getenv("SCAN_TOP_N_COINS", "40")
)

CHECK_INTERVAL_SECONDS = int(
    os.getenv("CHECK_INTERVAL_SECONDS", "300")
)

NOTIFY_COOLDOWN_SECONDS = int(
    os.getenv("NOTIFY_COOLDOWN_SECONDS", "7200")
)

LEVERAGE = int(
    os.getenv("LEVERAGE", "10")
)

SWING_LOOKBACK = int(
    os.getenv("SWING_LOOKBACK", "2")
)

MIN_RR_RATIO = float(
    os.getenv("MIN_RR_RATIO", "2.0")
)

KLINES_LIMIT = int(
    os.getenv("KLINES_LIMIT", "150")
)

DAILY_KLINES_LIMIT = int(
    os.getenv("DAILY_KLINES_LIMIT", "120")
)

REQUIRE_TREND_ALIGN = env_bool(
    "REQUIRE_TREND_ALIGN", True
)

REQUIRE_LIQUIDITY_SWEEP = env_bool(
    "REQUIRE_LIQUIDITY_SWEEP", True
)

REQUIRE_FVG = env_bool(
    "REQUIRE_FVG", True
)

REQUIRE_SESSION_FILTER = env_bool(
    "REQUIRE_SESSION_FILTER", True
)

REQUIRE_OB_UNMITIGATED = env_bool(
    "REQUIRE_OB_UNMITIGATED", True
)

REQUIRE_CHOCH_ONLY = env_bool(
    "REQUIRE_CHOCH_ONLY", False
)

REQUIRE_EQUAL_LEVEL_SWEEP = env_bool(
    "REQUIRE_EQUAL_LEVEL_SWEEP", False
)

ACCOUNT_BALANCE = float(
    os.getenv("ACCOUNT_BALANCE", "1000")
)

RISK_PERCENT = float(
    os.getenv("RISK_PERCENT", "1")
)

REQUEST_TIMEOUT = int(
    os.getenv("REQUEST_TIMEOUT", "8")
)

TOP_COINS_CACHE_SECONDS = int(
    os.getenv("TOP_COINS_CACHE_SECONDS", "600")
)

_last_notified = {}
_analysis_lock = asyncio.Lock()

_coins_cache = []
_coins_cache_time = 0


# ============================================================
# DATA
# ============================================================

def api_get(url, retries=3):
    for attempt in range(retries):
        try:
            response = requests.get(
                url,
                timeout=REQUEST_TIMEOUT
            )

            if response.status_code == 200:
                return response.json()

            logging.warning(
                f"API status={response.status_code}"
            )

        except Exception as e:
            logging.warning(
                f"API attempt {attempt + 1} failed: {e}"
            )

        time.sleep(1 + attempt)

    return None


def fetch_top_liquid_coins(limit=SCAN_TOP_N_COINS):
    global _coins_cache
    global _coins_cache_time

    now = time.time()

    if (
        _coins_cache and
        now - _coins_cache_time < TOP_COINS_CACHE_SECONDS
    ):
        return _coins_cache

    url = (
        "https://api.bybit.com/v5/market/tickers"
        "?category=linear"
    )

    payload = api_get(url)

    try:
        if payload and payload.get("retCode") == 0:

            rows = payload.get(
                "result", {}
            ).get("list", [])

            pairs = []

            for row in rows:
                symbol = row.get("symbol", "")
                turnover = float(
                    row.get("turnover24h") or 0
                )

                if (
                    symbol.endswith("USDT") and
                    turnover > 0
                ):
                    pairs.append(
                        (symbol, turnover)
                    )

            pairs.sort(
                key=lambda x: x[1],
                reverse=True
            )

            symbols = [
                x[0] for x in pairs[:limit]
            ]

            if symbols:
                _coins_cache = symbols
                _coins_cache_time = now

                logging.info(
                    f"{len(symbols)} likvid coin tapıldı"
                )

                return symbols

    except Exception as e:
        logging.error(
            f"Coin siyahısı xətası: {e}"
        )

    logging.warning(
        "Fallback coin siyahısı istifadə edilir"
    )

    return FALLBACK_COINS


def fetch_klines(
    symbol,
    interval="60",
    limit=KLINES_LIMIT
):
    url = (
        "https://api.bybit.com/v5/market/kline"
        f"?category=linear"
        f"&symbol={symbol}"
        f"&interval={interval}"
        f"&limit={limit}"
    )

    payload = api_get(url)

    try:
        if not payload:
            return None

        if payload.get("retCode") != 0:
            logging.warning(
                f"{symbol}: "
                f"{payload.get('retMsg')}"
            )
            return None

        rows = payload.get(
            "result", {}
        ).get("list", [])

        if len(rows) < 30:
            return None

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

        df = df.dropna().reset_index(
            drop=True
        )

        if len(df) < 30:
            return None

        # Son natamam şamı sil
        df = df.iloc[:-1].reset_index(
            drop=True
        )

        return df

    except Exception as e:
        logging.error(
            f"{symbol} data parsing error: {e}"
        )

    return None


# ============================================================
# SWING
# ============================================================

def find_swing_points(
    df,
    lookback=SWING_LOOKBACK
):
    highs = df["high"].values
    lows = df["low"].values

    swing_highs = []
    swing_lows = []

    n = len(df)

    for i in range(
        lookback,
        n - lookback
    ):

        left_high = highs[
            i - lookback:i
        ]

        right_high = highs[
            i + 1:i + lookback + 1
        ]

        if (
            highs[i] > left_high.max() and
            highs[i] > right_high.max()
        ):
            swing_highs.append(
                (i, highs[i])
            )

        left_low = lows[
            i - lookback:i
        ]

        right_low = lows[
            i + 1:i + lookback + 1
        ]

        if (
            lows[i] < left_low.min() and
            lows[i] < right_low.min()
        ):
            swing_lows.append(
                (i, lows[i])
            )

    return swing_highs, swing_lows


def determine_trend_bias(
    swing_highs,
    swing_lows
):
    if (
        len(swing_highs) < 2 or
        len(swing_lows) < 2
    ):
        return None

    last_high = swing_highs[-1][1]
    prev_high = swing_highs[-2][1]

    last_low = swing_lows[-1][1]
    prev_low = swing_lows[-2][1]

    if (
        last_high > prev_high and
        last_low > prev_low
    ):
        return "bullish"

    if (
        last_high < prev_high and
        last_low < prev_low
    ):
        return "bearish"

    return "ranging"


# ============================================================
# ATR
# ============================================================

def compute_atr(
    df,
    period=14
):
    high = df["high"]
    low = df["low"]
    close = df["close"]

    prev_close = close.shift(1)

    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs()
        ],
        axis=1
    ).max(axis=1)

    return tr.rolling(
        period,
        min_periods=1
    ).mean()


# ============================================================
# STRUCTURE
# ============================================================

def compute_structure_events(
    df,
    swing_highs,
    swing_lows
):
    close = df["close"].values

    events = []
    trend_bias = None

    pivots = sorted(
        [
            (i, p, "high")
            for i, p in swing_highs
        ] +
        [
            (i, p, "low")
            for i, p in swing_lows
        ]
    )

    pivot_ptr = 0

    active_high = None
    active_low = None

    high_crossed = True
    low_crossed = True

    for i in range(len(df)):

        while (
            pivot_ptr < len(pivots) and
            pivots[pivot_ptr][0] == i
        ):

            idx, price, pivot_type = pivots[
                pivot_ptr
            ]

            if pivot_type == "high":
                active_high = (
                    idx,
                    price
                )
                high_crossed = False

            else:
                active_low = (
                    idx,
                    price
                )
                low_crossed = False

            pivot_ptr += 1

        if (
            active_high is not None and
            not high_crossed and
            i > 0
        ):

            if (
                close[i - 1] <= active_high[1] and
                close[i] > active_high[1]
            ):

                kind = (
                    "CHoCH"
                    if trend_bias == "bearish"
                    else "BOS"
                )

                trend_bias = "bullish"
                high_crossed = True

                events.append({
                    "index": i,
                    "bias": "bullish",
                    "kind": kind,
                    "level": active_high[1]
                })

        if (
            active_low is not None and
            not low_crossed and
            i > 0
        ):

            if (
                close[i - 1] >= active_low[1] and
                close[i] < active_low[1]
            ):

                kind = (
                    "CHoCH"
                    if trend_bias == "bullish"
                    else "BOS"
                )

                trend_bias = "bearish"
                low_crossed = True

                events.append({
                    "index": i,
                    "bias": "bearish",
                    "kind": kind,
                    "level": active_low[1]
                })

    return events


# ============================================================
# EQUAL HIGH / LOW
# ============================================================

def detect_equal_levels(
    df,
    swing_highs,
    swing_lows,
    atr_series,
    threshold=0.1
):
    equal_highs = []
    equal_lows = []

    for i in range(
        1,
        len(swing_highs)
    ):

        idx1, p1 = swing_highs[i - 1]
        idx2, p2 = swing_highs[i]

        atr = atr_series.iloc[
            min(idx2, len(atr_series) - 1)
        ]

        if (
            atr > 0 and
            abs(p1 - p2) <= atr * threshold
        ):
            equal_highs.append(
                (
                    idx1,
                    idx2,
                    (p1 + p2) / 2
                )
            )

    for i in range(
        1,
        len(swing_lows)
    ):

        idx1, p1 = swing_lows[i - 1]
        idx2, p2 = swing_lows[i]

        atr = atr_series.iloc[
            min(idx2, len(atr_series) - 1)
        ]

        if (
            atr > 0 and
            abs(p1 - p2) <= atr * threshold
        ):
            equal_lows.append(
                (
                    idx1,
                    idx2,
                    (p1 + p2) / 2
                )
            )

    return equal_highs, equal_lows


# ============================================================
# ORDER BLOCK
# ============================================================

def find_order_block_advanced(
    df,
    direction,
    break_idx,
    lookback=50
):
    start = max(
        0,
        break_idx - lookback
    )

    window = df.iloc[
        start:break_idx + 1
    ]

    if window.empty:
        return None

    if direction == "bullish":

        relative_idx = (
            window["low"].values.argmin()
        )

        ob_idx = start + relative_idx

    else:

        relative_idx = (
            window["high"].values.argmax()
        )

        ob_idx = start + relative_idx

    ob_high = float(
        df["high"].iloc[ob_idx]
    )

    ob_low = float(
        df["low"].iloc[ob_idx]
    )

    mitigated = False

    # Yalnız struktur break-dən sonrakı
    # şamları yoxla
    for j in range(
        break_idx + 1,
        len(df)
    ):

        if (
            direction == "bullish" and
            df["close"].iloc[j] < ob_low
        ):
            mitigated = True
            break

        if (
            direction == "bearish" and
            df["close"].iloc[j] > ob_high
        ):
            mitigated = True
            break

    return {
        "high": ob_high,
        "low": ob_low,
        "index": ob_idx,
        "mitigated": mitigated
    }


# ============================================================
# LIQUIDITY SWEEP
# ============================================================

def detect_liquidity_sweep(
    df,
    direction,
    break_idx,
    lookback_window=15
):
    start = max(
        0,
        break_idx - lookback_window
    )

    segment = df.iloc[
        start:break_idx + 1
    ].reset_index(drop=True)

    if len(segment) < 3:
        return False

    if direction == "bullish":

        for i in range(
            2,
            len(segment)
        ):

            prior_low = segment[
                "low"
            ].iloc[:i].min()

            current_low = segment[
                "low"
            ].iloc[i]

            current_close = segment[
                "close"
            ].iloc[i]

            if (
                current_low < prior_low and
                current_close > prior_low
            ):
                return True

    else:

        for i in range(
            2,
            len(segment)
        ):

            prior_high = segment[
                "high"
            ].iloc[:i].max()

            current_high = segment[
                "high"
            ].iloc[i]

            current_close = segment[
                "close"
            ].iloc[i]

            if (
                current_high > prior_high and
                current_close < prior_high
            ):
                return True

    return False


# ============================================================
# FVG
# ============================================================

def detect_fvg(
    df,
    direction,
    break_idx,
    lookback_window=15
):
    start = max(
        0,
        break_idx - lookback_window
    )

    segment = df.iloc[
        start:break_idx + 1
    ].reset_index(drop=True)

    if len(segment) < 3:
        return False

    for i in range(
        1,
        len(segment) - 1
    ):

        prev_candle = segment.iloc[
            i - 1
        ]

        next_candle = segment.iloc[
            i + 1
        ]

        if direction == "bullish":

            if (
                prev_candle["high"] <
                next_candle["low"]
            ):
                return True

        else:

            if (
                prev_candle["low"] >
                next_candle["high"]
            ):
                return True

    return False


# ============================================================
# LIQUIDITY TARGET
# ============================================================

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

        return min(targets) if targets else None

    targets = [
        price
        for _, price in swing_lows
        if price < current_price
    ]

    return max(targets) if targets else None


# ============================================================
# SESSION
# ============================================================

def get_trading_session():
    hour = datetime.now(
        timezone.utc
    ).hour

    in_london = (
        7 <= hour < 16
    )

    in_ny = (
        13 <= hour < 22
    )

    active = (
        in_london or
        in_ny
    )

    sessions = []

    if in_london:
        sessions.append("London")

    if in_ny:
        sessions.append("New York")

    if sessions:
        name = " + ".join(sessions)
    else:
        name = "Asiya"

    return active, name


# ============================================================
# DAILY TREND
# ============================================================

def get_daily_trend_bias(symbol):

    df = fetch_klines(
        symbol,
        interval="D",
        limit=DAILY_KLINES_LIMIT
    )

    if df is None or len(df) < 30:
        return None

    swing_highs, swing_lows = (
        find_swing_points(
            df,
            lookback=2
        )
    )

    return determine_trend_bias(
        swing_highs,
        swing_lows
    )


# ============================================================
# MAIN SMC ANALYSIS
# ============================================================

def analyze_smc_pro(
    symbol,
    session_active,
    session_name
):

    conditions = {}

    # 1 DAILY TREND

    daily_bias = get_daily_trend_bias(
        symbol
    )

    cond_daily = daily_bias in (
        "bullish",
        "bearish"
    )

    conditions[
        f"Günlük trend aydındır "
        f"({daily_bias or 'naməlum'})"
    ] = cond_daily

    if not cond_daily:

        return {
            "symbol": symbol,
            "passed": False,
            "error": None,
            "conditions": conditions
        }

    # 2 1H DATA

    df = fetch_klines(
        symbol,
        interval="60",
        limit=KLINES_LIMIT
    )

    if df is None or len(df) < 60:

        return {
            "symbol": symbol,
            "passed": False,
            "error": "1H data alınmadı",
            "conditions": conditions
        }

    # 3 SWING

    swing_highs, swing_lows = (
        find_swing_points(df)
    )

    cond_structure = (
        len(swing_highs) >= 2 and
        len(swing_lows) >= 2
    )

    conditions[
        "1H bazar strukturu kifayətdir"
    ] = cond_structure

    if not cond_structure:

        return {
            "symbol": symbol,
            "passed": False,
            "error": None,
            "conditions": conditions
        }

    # 4 BOS CHOCH

    events = compute_structure_events(
        df,
        swing_highs,
        swing_lows
    )

    if not events:

        conditions[
            "Struktur hadisəsi BOS/CHoCH"
        ] = False

        return {
            "symbol": symbol,
            "passed": False,
            "error": None,
            "conditions": conditions
        }

    last_event = events[-1]

    direction = last_event["bias"]

    break_idx = last_event["index"]

    event_kind = last_event["kind"]

    conditions[
        f"Struktur hadisəsi {event_kind}"
    ] = True

    # Son hadisə çox köhnədirsə
    bars_since_break = (
        len(df) - 1 - break_idx
    )

    cond_fresh_event = (
        bars_since_break <= 30
    )

    conditions[
        f"Struktur break təzədir "
        f"({bars_since_break} şam)"
    ] = cond_fresh_event

    if not cond_fresh_event:

        return {
            "symbol": symbol,
            "passed": False,
            "error": None,
            "conditions": conditions
        }

    # CHOCH ONLY

    if REQUIRE_CHOCH_ONLY:

        cond_choch = (
            event_kind == "CHoCH"
        )

        conditions[
            "Son hadisə CHoCH-dur"
        ] = cond_choch

        if not cond_choch:

            return {
                "symbol": symbol,
                "passed": False,
                "error": None,
                "conditions": conditions
            }

    # TREND ALIGN

    if REQUIRE_TREND_ALIGN:

        cond_trend = (
            direction == daily_bias
        )

        conditions[
            f"1H istiqaməti Daily ilə uyğundur "
            f"({direction})"
        ] = cond_trend

        if not cond_trend:

            return {
                "symbol": symbol,
                "passed": False,
                "error": None,
                "conditions": conditions
            }

    # ATR

    atr_series = compute_atr(df)

    equal_highs, equal_lows = (
        detect_equal_levels(
            df,
            swing_highs,
            swing_lows,
            atr_series
        )
    )

    # LIQUIDITY SWEEP

    if REQUIRE_LIQUIDITY_SWEEP:

        cond_sweep = (
            detect_liquidity_sweep(
                df,
                direction,
                break_idx
            )
        )

        conditions[
            "Liquidity Sweep aşkarlanıb"
        ] = cond_sweep

        if not cond_sweep:

            return {
                "symbol": symbol,
                "passed": False,
                "error": None,
                "conditions": conditions
            }

    # EQUAL LEVEL

    if REQUIRE_EQUAL_LEVEL_SWEEP:

        if direction == "bullish":

            cond_equal = (
                len(equal_lows) > 0
            )

        else:

            cond_equal = (
                len(equal_highs) > 0
            )

        conditions[
            "Equal High/Low liquidity mövcuddur"
        ] = cond_equal

        if not cond_equal:

            return {
                "symbol": symbol,
                "passed": False,
                "error": None,
                "conditions": conditions
            }

    # ORDER BLOCK

    ob = find_order_block_advanced(
        df,
        direction,
        break_idx
    )

    cond_ob = ob is not None

    conditions[
        "Order Block tapıldı"
    ] = cond_ob

    if not cond_ob:

        return {
            "symbol": symbol,
            "passed": False,
            "error": None,
            "conditions": conditions
        }

    # UNMITIGATED

    if REQUIRE_OB_UNMITIGATED:

        cond_unmitigated = (
            not ob["mitigated"]
        )

        conditions[
            "Order Block mitigate olunmayıb"
        ] = cond_unmitigated

        if not cond_unmitigated:

            return {
                "symbol": symbol,
                "passed": False,
                "error": None,
                "conditions": conditions
            }

    # FVG

    if REQUIRE_FVG:

        cond_fvg = detect_fvg(
            df,
            direction,
            break_idx
        )

        conditions[
            "Fair Value Gap mövcuddur"
        ] = cond_fvg

        if not cond_fvg:

            return {
                "symbol": symbol,
                "passed": False,
                "error": None,
                "conditions": conditions
            }

    # CURRENT PRICE

    current_price = float(
        df["close"].iloc[-1]
    )

    # OB RETEST

    ob_range = (
        ob["high"] -
        ob["low"]
    )

    buffer = ob_range * 0.10

    in_zone = (
        ob["low"] - buffer <=
        current_price <=
        ob["high"] + buffer
    )

    conditions[
        "Qiymət Order Block zonasındadır"
    ] = in_zone

    if not in_zone:

        return {
            "symbol": symbol,
            "passed": False,
            "error": None,
            "conditions": conditions
        }

    # LIQUIDITY TARGET

    liquidity_target = (
        find_next_liquidity(
            direction,
            current_price,
            swing_highs,
            swing_lows
        )
    )

    cond_target = (
        liquidity_target is not None
    )

    conditions[
        "Take Profit üçün likvidlik hədəfi var"
    ] = cond_target

    if not cond_target:

        return {
            "symbol": symbol,
            "passed": False,
            "error": None,
            "conditions": conditions
        }

    # ENTRY SL TP

    entry = current_price

    if direction == "bullish":

        sl = (
            ob["low"] * 0.997
        )

        tp = liquidity_target

        bias = (
            f"🟢 LONG "
            f"({event_kind} + Bullish OB Retest)"
        )

    else:

        sl = (
            ob["high"] * 1.003
        )

        tp = liquidity_target

        bias = (
            f"🔴 SHORT "
            f"({event_kind} + Bearish OB Retest)"
        )

    # RR

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

    cond_rr = (
        rr_ratio >= MIN_RR_RATIO
    )

    conditions[
        f"RR >= 1:{MIN_RR_RATIO} "
        f"(faktiki 1:{rr_ratio:.2f})"
    ] = cond_rr

    if not cond_rr:

        return {
            "symbol": symbol,
            "passed": False,
            "error": None,
            "conditions": conditions
        }

    # SESSION

    if REQUIRE_SESSION_FILTER:

        conditions[
            f"Aktiv treyding seansı "
            f"({session_name})"
        ] = session_active

        if not session_active:

            return {
                "symbol": symbol,
                "passed": False,
                "error": None,
                "conditions": conditions
            }

    # RISK MANAGEMENT

    risk_amount = (
        ACCOUNT_BALANCE *
        RISK_PERCENT / 100
    )

    position_size = (
        risk_amount / risk
        if risk > 0
        else 0
    )

    # SIQNAL SCORE

    score = 0

    if event_kind == "CHoCH":
        score += 15
    else:
        score += 10

    score += min(
        rr_ratio * 10,
        35
    )

    if REQUIRE_LIQUIDITY_SWEEP:
        score += 15

    if REQUIRE_FVG:
        score += 10

    if REQUIRE_TREND_ALIGN:
        score += 15

    score = min(
        round(score, 1),
        100
    )

    return {
        "symbol": symbol,
        "passed": True,
        "error": None,
        "conditions": conditions,
        "bias": bias,
        "event_kind": event_kind,
        "entry": round(entry, 8),
        "sl": round(sl, 8),
        "tp": round(tp, 8),
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
        "daily_bias": daily_bias,
        "session": session_name,
        "score": score
  }
# ============================================================
# SCAN
# ============================================================

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

            all_results.append(result)

        except Exception as e:

            logging.error(
                f"{symbol} analiz xətası: {e}"
            )

            all_results.append({
                "symbol": symbol,
                "passed": False,
                "error": str(e),
                "conditions": {}
            })

        time.sleep(0.15)

    passed = [
        x for x in all_results
        if x.get("passed")
    ]

    if not passed:

        return None, all_results

    # Ən yaxşı siqnal:
    # əvvəl score, sonra RR

    best = max(
        passed,
        key=lambda x: (
            x.get("score", 0),
            x.get("rr_ratio", 0)
        )
    )

    return best, all_results


# ============================================================
# DIAGNOSTICS
# ============================================================

def format_diagnostics(
    all_results,
    max_detail=8
):

    total = len(all_results)

    reason_counts = {}

    error_count = 0

    for res in all_results:

        if res.get("error"):

            error_count += 1
            continue

        failed = [
            name
            for name, ok
            in res.get(
                "conditions",
                {}
            ).items()
            if not ok
        ]

        if failed:

            key = failed[0]

            reason_counts[key] = (
                reason_counts.get(
                    key,
                    0
                ) + 1
            )

    lines = [
        f"📋 *Xülasə:* "
        f"{total} coin yoxlanıldı."
    ]

    lines.append(
        "\n*Səbəblərin bölgüsü:*"
    )

    sorted_reasons = sorted(
        reason_counts.items(),
        key=lambda x: x[1],
        reverse=True
    )

    for reason, count in sorted_reasons[:10]:

        lines.append(
            f"• {reason}: `{count}` coin"
        )

    if error_count:

        lines.append(
            f"• API/xəta: `{error_count}` coin"
        )

    lines.append(
        f"\n*İlk {max_detail} coin:*"
    )

    for res in all_results[:max_detail]:

        symbol = res["symbol"]

        if res.get("error"):

            lines.append(
                f"• `{symbol}`: ❌ API/xəta"
            )

            continue

        failed = [
            name
            for name, ok
            in res.get(
                "conditions",
                {}
            ).items()
            if not ok
        ]

        if failed:

            lines.append(
                f"• `{symbol}`: ❌ "
                f"{failed[0]}"
            )

        else:

            lines.append(
                f"• `{symbol}`: ✅ Keçdi"
            )

    return "\n".join(lines)


# ============================================================
# SIGNAL MESSAGE
# ============================================================

def format_signal_message(
    res,
    title="📊 *Professional SMC Siqnalı*"
):

    coin = res["symbol"].replace(
        "USDT",
        ""
    )

    return (
        f"{title}\n\n"
        f"🪙 *Coin:* `{res['symbol']}`\n"
        f"⭐ *Signal Score:* `{res['score']}/100`\n"
        f"⚙️ *Leverage:* `{res['leverage']}x`\n"
        f"🎯 *Siqnal:* {res['bias']}\n"
        f"📈 *Daily Trend:* `{res['daily_bias']}`\n"
        f"🕒 *Seans:* `{res['session']}`\n"
        f"⚖️ *Risk/Mükafat:* "
        f"`1:{res['rr_ratio']}`\n\n"
        f"📍 *ENTRY:* "
        f"`{res['entry']}`\n"
        f"🛑 *STOP LOSS:* "
        f"`{res['sl']}`\n"
        f"🎯 *TAKE PROFIT:* "
        f"`{res['tp']}`\n\n"
        f"💰 *Risk Management:*\n"
        f"Balans: `${ACCOUNT_BALANCE}`\n"
        f"Risk: `{RISK_PERCENT}%`\n"
        f"Risk məbləği: "
        f"`${res['risk_amount']}`\n"
        f"Pozisiya: "
        f"`{res['position_size']} {coin}`"
    )


# ============================================================
# AUTOMATIC SIGNAL LOOP
# ============================================================

async def auto_signal_loop(application):

    logging.info(
        "Automatic signal scanner başladı"
    )

    await asyncio.sleep(10)

    while True:

        try:

            logging.info(
                "Automatic market scan başladı"
            )

            async with _analysis_lock:

                result, _ = (
                    await asyncio.to_thread(
                        get_best_smc_signal
                    )
                )

            if result:

                symbol = result["symbol"]

                now = time.time()

                last_time = (
                    _last_notified.get(
                        symbol,
                        0
                    )
                )

                elapsed = (
                    now - last_time
                )

                if (
                    elapsed >=
                    NOTIFY_COOLDOWN_SECONDS
                ):

                    message = (
                        format_signal_message(
                            result,
                            title=(
                                "🚨 *AVTOMATİK "
                                "SMC SİQNALI* 🚨"
                            )
                        )
                    )

                    await application.bot.send_message(
                        chat_id=CHAT_ID,
                        text=message,
                        parse_mode="Markdown"
                    )

                    _last_notified[
                        symbol
                    ] = now

                    logging.info(
                        f"{symbol}: "
                        f"avtomatik siqnal göndərildi"
                    )

                else:

                    logging.info(
                        f"{symbol}: "
                        f"cooldown aktivdir"
                    )

            else:

                logging.info(
                    "Uyğun siqnal tapılmadı"
                )

        except Exception as e:

            logging.exception(
                f"Automatic scanner error: {e}"
            )

        await asyncio.sleep(
            CHECK_INTERVAL_SECONDS
          )
# ============================================================
# TELEGRAM COMMANDS
# ============================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(
        "📊 *Professional SMC Bot aktivdir!*\n\n"
        "🔍 Canlı analiz: `/analiz`\n\n"
        f"🔔 Avtomatik scan: "
        f"hər `{CHECK_INTERVAL_SECONDS // 60}` dəqiqə\n"
        f"📊 Scan olunan coin sayı: "
        f"`{SCAN_TOP_N_COINS}`\n"
        f"⚖️ Minimum RR: "
        f"`1:{MIN_RR_RATIO}`\n\n"
        "Strategiya:\n"
        "Daily Trend → 1H BOS/CHoCH → "
        "Liquidity Sweep → OB → FVG → "
        "OB Retest → Liquidity TP → RR",
        parse_mode="Markdown"
    )


async def analiz(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(
        f"🔍 {SCAN_TOP_N_COINS} likvid coin "
        f"analiz edilir...\n\n"
        "⏳ Daily + 1H SMC yoxlanılır."
    )

    try:

        async with _analysis_lock:

            result, all_results = (
                await asyncio.to_thread(
                    get_best_smc_signal
                )
            )

        if result:

            message = (
                format_signal_message(
                    result
                )
            )

            await update.message.reply_text(
                message,
                parse_mode="Markdown"
            )

        else:

            diagnostics = (
                format_diagnostics(
                    all_results
                )
            )

            await update.message.reply_text(
                "❌ Hazırda bütün sərt "
                "SMC şərtlərini ödəyən "
                "siqnal tapılmadı.\n\n" +
                diagnostics,
                parse_mode="Markdown"
            )

    except Exception as e:

        logging.exception(
            f"/analiz xətası: {e}"
        )

        await update.message.reply_text(
            "❌ Analiz zamanı xəta baş verdi."
        )


# ============================================================
# POST INIT
# ============================================================

async def post_init(application):

    application.create_task(
        auto_signal_loop(application)
    )

    logging.info(
        "Auto signal task yaradıldı"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    if not TOKEN:

        logging.error(
            "BOT_TOKEN tapılmadı!"
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
        "Professional SMC Bot işə düşdü"
    )

    application.run_polling(
        drop_pending_updates=True
    )


if __name__ == "__main__":
    main()
