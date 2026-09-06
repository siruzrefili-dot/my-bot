# ============================================
# SIMPLE TRADING BOT V1.1
# PART 1/8 - IMPORT + CONFIG
# ============================================

import os
import json
import time
import logging
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
import numpy as np
from flask import Flask, jsonify


BOT_VERSION = "SIMPLE V1.1"

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "1121794078")

BYBIT_URL = "https://api.bybit.com"
CATEGORY = "linear"

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "SUIUSDT"
]

TREND_TF = "240"
SETUP_TF = "60"
ENTRY_TF = "15"
MONITOR_TF = "1"

EMA_FAST = 50
EMA_SLOW = 200
RSI_PERIOD = 14
ATR_PERIOD = 14
VOLUME_PERIOD = 20

RSI_LEVEL = 50.0

PULLBACK_ATR = 0.80
STRONG_CANDLE_ATR = 0.50
MIN_VOLUME_RATIO = 1.10

MIN_ATR_PERCENT = 0.10
MAX_ATR_PERCENT = 6.00

ACCOUNT_BALANCE = 1000.0
RISK_PERCENT = 1.0

MIN_RR = 2.0

MAX_ACTIVE_SIGNALS = 3
MAX_DAILY_SIGNALS = 5

SYMBOL_COOLDOWN_MINUTES = 120

SCAN_INTERVAL = 300
MONITOR_INTERVAL = 30

REQUEST_TIMEOUT = 15

DATA_DIR = "simple_bot_data"
SIGNALS_FILE = os.path.join(DATA_DIR, "signals.json")
STATS_FILE = os.path.join(DATA_DIR, "stats.json")

FLASK_PORT = 10000

os.makedirs(DATA_DIR, exist_ok=True)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("SimpleBot")

print("=" * 55)
print(" SIMPLE TRADING BOT V1.1")
print("=" * 55)
print("4H Trend")
print("1H Pullback")
print("15M RSI + Strong Candle")
print("Volume + ATR")
print("SL + TP 1:2")
print("Telegram Commands Enabled")
print("=" * 55)
# ============================================
# PART 2/8 - STORAGE + BYBIT API
# ============================================

file_lock = threading.RLock()


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def load_json(filename, default):
    with file_lock:
        try:
            if not os.path.exists(filename):
                return default

            with open(filename, "r", encoding="utf-8") as f:
                return json.load(f)

        except Exception as e:
            logger.error("Load error: %s", e)
            return default


def save_json(filename, data):
    with file_lock:
        try:
            temp_file = filename + ".tmp"

            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(
                    data,
                    f,
                    indent=2,
                    ensure_ascii=False
                )

            os.replace(temp_file, filename)

        except Exception as e:
            logger.error("Save error: %s", e)


class BybitClient:

    def __init__(self):
        self.local = threading.local()

    def get_session(self):
        if not hasattr(self.local, "session"):
            self.local.session = requests.Session()

            self.local.session.headers.update({
                "User-Agent": "SimpleTradingBot/1.1"
            })

        return self.local.session

    def get(self, endpoint, params=None):
        url = BYBIT_URL + endpoint
        last_error = None

        for attempt in range(3):
            try:
                response = self.get_session().get(
                    url,
                    params=params or {},
                    timeout=REQUEST_TIMEOUT
                )

                response.raise_for_status()

                data = response.json()

                if data.get("retCode") != 0:
                    raise RuntimeError(
                        data.get(
                            "retMsg",
                            "Bybit API error"
                        )
                    )

                return data

            except Exception as e:
                last_error = e

                if attempt < 2:
                    time.sleep(1 + attempt)

        raise last_error

    def klines(self, symbol, interval, limit=300):

        data = self.get(
            "/v5/market/kline",
            {
                "category": CATEGORY,
                "symbol": symbol,
                "interval": interval,
                "limit": limit
            }
        )

        rows = data["result"]["list"]

        if not rows:
            return pd.DataFrame()

        rows = list(reversed(rows))

        result = []

        for row in rows:
            result.append({
                "timestamp": int(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5])
            })

        df = pd.DataFrame(result)

        if df.empty:
            return df

        df["datetime"] = pd.to_datetime(
            df["timestamp"],
            unit="ms",
            utc=True
        )

        now_ms = int(time.time() * 1000)

        interval_ms = (
            int(interval) * 60 * 1000
        )

        last_timestamp = int(
            df.iloc[-1]["timestamp"]
        )

        if now_ms < last_timestamp + interval_ms:
            df = df.iloc[:-1].copy()

        return df.reset_index(drop=True)
# ============================================
# PART 3/8 - INDICATORS
# ============================================

def calculate_ema(series, period):
    return series.ewm(
        span=period,
        adjust=False
    ).mean()


def calculate_rsi(series, period=14):

    delta = series.diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()

    avg_loss = loss.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()

    rs = avg_gain / avg_loss.replace(
        0,
        np.nan
    )

    result = 100 - (
        100 / (1 + rs)
    )

    return result.fillna(50)


def calculate_atr(df, period=14):

    previous_close = df["close"].shift(1)

    tr1 = (
        df["high"] -
        df["low"]
    )

    tr2 = (
        df["high"] -
        previous_close
    ).abs()

    tr3 = (
        df["low"] -
        previous_close
    ).abs()

    true_range = pd.concat(
        [tr1, tr2, tr3],
        axis=1
    ).max(axis=1)

    return true_range.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()


def add_indicators(df):

    df = df.copy()

    df["ema50"] = calculate_ema(
        df["close"],
        EMA_FAST
    )

    df["ema200"] = calculate_ema(
        df["close"],
        EMA_SLOW
    )

    df["rsi"] = calculate_rsi(
        df["close"],
        RSI_PERIOD
    )

    df["atr"] = calculate_atr(
        df,
        ATR_PERIOD
    )

    df["volume_ma"] = (
        df["volume"]
        .rolling(VOLUME_PERIOD)
        .mean()
    )

    df["body"] = (
        df["close"] -
        df["open"]
    ).abs()

    return df


def has_enough_data(df, minimum):

    if df is None:
        return False

    if df.empty:
        return False

    return len(df) >= minimum
# ============================================
# PART 4/8 - STRATEGY
# ============================================

def get_4h_trend(df):

    if not has_enough_data(df, 220):
        return "NONE"

    last = df.iloc[-1]

    price = float(last["close"])
    ema50 = float(last["ema50"])
    ema200 = float(last["ema200"])

    if (
        price > ema200
        and ema50 > ema200
    ):
        return "LONG"

    if (
        price < ema200
        and ema50 < ema200
    ):
        return "SHORT"

    return "NONE"


def check_1h_pullback(df, direction):

    if len(df) < 10:
        return False

    recent = df.iloc[-6:]

    for _, candle in recent.iterrows():

        ema50 = float(candle["ema50"])
        atr_value = float(candle["atr"])

        if atr_value <= 0:
            continue

        if direction == "LONG":

            distance = abs(
                float(candle["low"]) -
                ema50
            )

            if distance <= (
                atr_value *
                PULLBACK_ATR
            ):
                return True

        elif direction == "SHORT":

            distance = abs(
                float(candle["high"]) -
                ema50
            )

            if distance <= (
                atr_value *
                PULLBACK_ATR
            ):
                return True

    return False


def check_15m_rsi(df, direction):

    if len(df) < 3:
        return False

    previous = df.iloc[-2]
    current = df.iloc[-1]

    previous_rsi = float(previous["rsi"])
    current_rsi = float(current["rsi"])

    if direction == "LONG":

        return (
            previous_rsi < RSI_LEVEL
            and
            current_rsi > RSI_LEVEL
            and
            current_rsi > previous_rsi
        )

    if direction == "SHORT":

        return (
            previous_rsi > RSI_LEVEL
            and
            current_rsi < RSI_LEVEL
            and
            current_rsi < previous_rsi
        )

    return False


def check_strong_candle(df, direction):

    if len(df) < 2:
        return False

    current = df.iloc[-1]
    previous = df.iloc[-2]

    body = float(current["body"])
    atr_value = float(current["atr"])

    if atr_value <= 0:
        return False

    if body < (
        atr_value *
        STRONG_CANDLE_ATR
    ):
        return False

    if direction == "LONG":

        if current["close"] <= current["open"]:
            return False

        if current["close"] <= previous["close"]:
            return False

        return True

    if direction == "SHORT":

        if current["close"] >= current["open"]:
            return False

        if current["close"] >= previous["close"]:
            return False

        return True

    return False


def check_volume(df):

    if len(df) < VOLUME_PERIOD + 2:
        return False, 0.0

    current = df.iloc[-1]

    volume_ma = float(
        current["volume_ma"]
    )

    if volume_ma <= 0 or np.isnan(volume_ma):
        return False, 0.0

    ratio = (
        float(current["volume"])
        / volume_ma
    )

    return (
        ratio >= MIN_VOLUME_RATIO,
        ratio
    )


def check_atr(df):

    if df.empty:
        return False, 0.0

    current = df.iloc[-1]

    price = float(current["close"])
    atr_value = float(current["atr"])

    if price <= 0 or atr_value <= 0:
        return False, 0.0

    atr_percent = (
        atr_value /
        price
    ) * 100

    valid = (
        atr_percent >= MIN_ATR_PERCENT
        and
        atr_percent <= MAX_ATR_PERCENT
    )

    return valid, atr_percent
# ============================================
# PART 5/8 - TRADE + SIGNAL
# ============================================

def get_swing_low(df, lookback=20):

    if df.empty:
        return None

    data = df.iloc[-lookback:]

    return float(
        data["low"].min()
    )


def get_swing_high(df, lookback=20):

    if df.empty:
        return None

    data = df.iloc[-lookback:]

    return float(
        data["high"].max()
    )


def calculate_trade(
    symbol,
    direction,
    entry,
    swing_low,
    swing_high,
    atr_value
):

    if entry <= 0 or atr_value <= 0:
        return None

    buffer_value = atr_value * 0.20

    if direction == "LONG":

        if swing_low is None:
            return None

        if swing_low >= entry:
            return None

        stop = swing_low - buffer_value

        risk_distance = entry - stop

        if risk_distance <= 0:
            return None

        target = (
            entry +
            risk_distance * MIN_RR
        )

    elif direction == "SHORT":

        if swing_high is None:
            return None

        if swing_high <= entry:
            return None

        stop = swing_high + buffer_value

        risk_distance = stop - entry

        if risk_distance <= 0:
            return None

        target = (
            entry -
            risk_distance * MIN_RR
        )

    else:
        return None

    rr = (
        abs(target - entry)
        / risk_distance
    )

    if rr < MIN_RR:
        return None

    risk_amount = (
        ACCOUNT_BALANCE *
        RISK_PERCENT /
        100
    )

    quantity = (
        risk_amount /
        risk_distance
    )

    notional = quantity * entry

    return {
        "symbol": symbol,
        "direction": direction,
        "entry": round(entry, 8),
        "stop": round(stop, 8),
        "target": round(target, 8),
        "risk_distance": round(
            risk_distance,
            8
        ),
        "rr": round(rr, 2),
        "risk_amount": round(
            risk_amount,
            4
        ),
        "quantity": round(
            quantity,
            8
        ),
        "notional": round(
            notional,
            4
        )
    }


def create_signal(
    trade,
    df15,
    volume_ratio,
    atr_percent
):

    current = df15.iloc[-1]

    return {
        "id": (
            trade["symbol"]
            + "_"
            + trade["direction"]
            + "_"
            + str(int(time.time()))
        ),

        "symbol": trade["symbol"],
        "direction": trade["direction"],

        "entry": trade["entry"],
        "stop": trade["stop"],
        "target": trade["target"],

        "rr": trade["rr"],
        "risk_distance": trade["risk_distance"],
        "risk_amount": trade["risk_amount"],
        "quantity": trade["quantity"],
        "notional": trade["notional"],

        "rsi": round(
            float(current["rsi"]),
            2
        ),

        "volume_ratio": round(
            volume_ratio,
            2
        ),

        "atr_percent": round(
            atr_percent,
            3
        ),

        "entry_candle_timestamp": int(
            current["timestamp"]
        ),

        "created_timestamp": time.time(),
        "created_at": utc_now(),

        "status": "ACTIVE",
        "last_checked_candle": 0
        
    }
# ============================================
# PART 6/8 - TRACKER + ANALYSIS
# ============================================

class SignalTracker:

    def __init__(self):

        self.lock = threading.RLock()

        self.signals = load_json(
            SIGNALS_FILE,
            []
        )

        self.stats = load_json(
            STATS_FILE,
            {
                "wins": 0,
                "losses": 0,
                "total": 0,
                "profit_r": 0.0,
                "daily_count": 0,
                "daily_date": ""
            }
        )

        self.reset_daily()

    def reset_daily(self):

        today = datetime.now(
            timezone.utc
        ).strftime("%Y-%m-%d")

        with self.lock:

            if self.stats.get(
                "daily_date"
            ) != today:

                self.stats["daily_date"] = today
                self.stats["daily_count"] = 0

                self.save()

    def save(self):

        save_json(
            SIGNALS_FILE,
            self.signals
        )

        save_json(
            STATS_FILE,
            self.stats
        )

    def active(self):

        with self.lock:

            return [
                signal
                for signal in self.signals
                if signal.get("status") == "ACTIVE"
            ]

    def can_create(self, symbol):

        with self.lock:

            self.reset_daily()

            if len(self.active()) >= MAX_ACTIVE_SIGNALS:
                return False

            if self.stats.get(
                "daily_count",
                0
            ) >= MAX_DAILY_SIGNALS:
                return False

            now = time.time()

            cooldown = (
                SYMBOL_COOLDOWN_MINUTES * 60
            )

            for signal in self.signals:

                if signal.get("symbol") != symbol:
                    continue

                created = signal.get(
                    "created_timestamp",
                    0
                )

                if now - created < cooldown:
                    return False

            return True

    def add(self, signal):

        with self.lock:

            if not self.can_create(
                signal["symbol"]
            ):
                return False

            self.signals.append(signal)

            self.stats["daily_count"] = (
                self.stats.get(
                    "daily_count",
                    0
                ) + 1
            )

            self.signals = self.signals[-300:]

            self.save()

            return True

    def update(self, signal):

        with self.lock:

            for index, item in enumerate(
                self.signals
            ):

                if item.get("id") == signal["id"]:
                    self.signals[index] = signal
                    break

            self.save()

    def close(
        self,
        signal,
        result_r,
        result
    ):

        with self.lock:

            signal["status"] = "CLOSED"
            signal["result_r"] = round(
                result_r,
                3
            )
            signal["result"] = result
            signal["closed_at"] = utc_now()

            self.stats["total"] += 1
            self.stats["profit_r"] += result_r

            if result == "WIN":
                self.stats["wins"] += 1
            else:
                self.stats["losses"] += 1

            self.update(signal)

    def summary(self):

        with self.lock:

            total = self.stats.get(
                "total",
                0
            )

            wins = self.stats.get(
                "wins",
                0
            )

            win_rate = (
                wins / total * 100
                if total > 0
                else 0
            )

            return {
                "total": total,
                "wins": wins,
                "losses": self.stats.get(
                    "losses",
                    0
                ),
                "win_rate": round(
                    win_rate,
                    2
                ),
                "profit_r": round(
                    self.stats.get(
                        "profit_r",
                        0.0
                    ),
                    3
                ),
                "daily_signals": self.stats.get(
                    "daily_count",
                    0
                ),
                "active": len(
                    self.active()
                )
            }


BYBIT = BybitClient()
TRACKER = SignalTracker()


def analyze_symbol(symbol):

    try:

        df4h = BYBIT.klines(
            symbol,
            TREND_TF,
            300
        )

        if not has_enough_data(df4h, 220):
            return None

        df4h = add_indicators(df4h)

        trend = get_4h_trend(df4h)

        if trend == "NONE":
            return None

        df1h = BYBIT.klines(
            symbol,
            SETUP_TF,
            250
        )

        if not has_enough_data(df1h, 50):
            return None

        df1h = add_indicators(df1h)

        if not check_1h_pullback(
            df1h,
            trend
        ):
            return None

        df15 = BYBIT.klines(
            symbol,
            ENTRY_TF,
            200
        )

        if not has_enough_data(df15, 50):
            return None

        df15 = add_indicators(df15)

        if not check_15m_rsi(
            df15,
            trend
        ):
            return None

        if not check_strong_candle(
            df15,
            trend
        ):
            return None

        volume_ok, volume_ratio = check_volume(df15)

        if not volume_ok:
            return None

        atr_ok, atr_percent = check_atr(df15)

        if not atr_ok:
            return None

        current = df15.iloc[-1]

        entry = float(current["close"])
        atr_value = float(current["atr"])

        swing_low = get_swing_low(
            df1h,
            20
        )

        swing_high = get_swing_high(
            df1h,
            20
        )

        trade = calculate_trade(
            symbol,
            trend,
            entry,
            swing_low,
            swing_high,
            atr_value
        )

        if trade is None:
            return None

        return create_signal(
            trade,
            df15,
            volume_ratio,
            atr_percent
        )

    except Exception as e:

        logger.error(
            "%s analysis error: %s",
            symbol,
            e
        )

        return None
# ============================================
# PART 7/8 - SCANNER + TELEGRAM COMMANDS
# ============================================

class TelegramBot:

    def __init__(self):

        self.offset = 0
        self.command_running = False
        self.lock = threading.Lock()

    def send(self, text):

        if not BOT_TOKEN:
            logger.warning(
                "BOT_TOKEN is empty"
            )
            return False

        url = (
            "https://api.telegram.org/bot"
            + BOT_TOKEN
            + "/sendMessage"
        )

        try:

            response = requests.post(
                url,
                json={
                    "chat_id": CHAT_ID,
                    "text": text
                },
                timeout=REQUEST_TIMEOUT
            )

            response.raise_for_status()

            data = response.json()

            return bool(data.get("ok"))

        except Exception as e:

            logger.error(
                "Telegram send error: %s",
                e
            )

            return False

    def send_signal(self, signal):

        if signal["direction"] == "LONG":
            icon = "🟢"
        else:
            icon = "🔴"

        text = (
            f"{icon} SIMPLE SIGNAL\n\n"
            f"Coin: {signal['symbol']}\n"
            f"Direction: {signal['direction']}\n\n"
            f"Entry: {signal['entry']}\n"
            f"SL: {signal['stop']}\n"
            f"TP: {signal['target']}\n"
            f"RR: 1:{signal['rr']}\n\n"
            f"RSI: {signal['rsi']}\n"
            f"Volume: {signal['volume_ratio']}x\n"
            f"ATR: {signal['atr_percent']}%\n\n"
            f"Risk: {RISK_PERCENT}%"
        )

        return self.send(text)

    def process_updates(self):

        if not BOT_TOKEN:
            return

        url = (
            "https://api.telegram.org/bot"
            + BOT_TOKEN
            + "/getUpdates"
        )

        try:

            response = requests.get(
                url,
                params={
                    "offset": self.offset,
                    "timeout": 20
                },
                timeout=25
            )

            response.raise_for_status()

            data = response.json()

            if not data.get("ok"):
                return

            updates = data.get("result", [])

            for update in updates:

                self.offset = (
                    int(update["update_id"]) + 1
                )

                message = update.get("message")

                if not message:
                    continue

                chat = message.get("chat", {})
                chat_id = str(
                    chat.get("id", "")
                )

                if chat_id != str(CHAT_ID):
                    continue

                text = str(
                    message.get("text", "")
                ).strip()

                if not text:
                    continue

                self.handle_command(text)

        except Exception as e:

            logger.error(
                "Telegram update error: %s",
                e
            )

    def handle_command(self, text):

        command = text.split()[0].lower()

        if "@" in command:
            command = command.split("@")[0]

        if command == "/start":

            self.send(
                "🟢 SIMPLE BOT AKTİVDİR\n\n"
                "Telegram əmrləri:\n"
                "/help - əmrlər\n"
                "/status - bot vəziyyəti\n"
                "/stats - statistika\n"
                "/active - aktiv siqnallar\n"
                "/scan - dərhal analiz"
            )

        elif command == "/help":

            self.send(
                "📋 TELEGRAM ƏMRLƏRİ\n\n"
                "/start\n"
                "Bot haqqında məlumat.\n\n"
                "/status\n"
                "Botun cari vəziyyəti.\n\n"
                "/stats\n"
                "WIN, LOSS, win rate və R statistikası.\n\n"
                "/active\n"
                "Hazırda aktiv siqnallar.\n\n"
                "/scan\n"
                "10 coin-i dərhal analiz edir."
            )

        elif command == "/status":

            summary = TRACKER.summary()

            self.send(
                "🟢 BOT STATUS\n\n"
                f"Version: {BOT_VERSION}\n"
                f"Coins: {len(SYMBOLS)}\n"
                f"Risk: {RISK_PERCENT}%\n"
                f"RR: 1:{MIN_RR}\n"
                f"Active: {summary['active']}/"
                f"{MAX_ACTIVE_SIGNALS}\n"
                f"Today: {summary['daily_signals']}/"
                f"{MAX_DAILY_SIGNALS}"
            )

        elif command == "/stats":

            summary = TRACKER.summary()

            self.send(
                "📊 STATISTICS\n\n"
                f"Total: {summary['total']}\n"
                f"Wins: {summary['wins']}\n"
                f"Losses: {summary['losses']}\n"
                f"Win rate: {summary['win_rate']}%\n"
                f"Profit: {summary['profit_r']}R\n"
                f"Today signals: "
                f"{summary['daily_signals']}\n"
                f"Active: {summary['active']}"
            )

        elif command == "/active":

            active = TRACKER.active()

            if not active:

                self.send(
                    "ℹ️ Hazırda aktiv siqnal yoxdur."
                )

                return

            lines = [
                "📌 AKTİV SİQNALLAR\n"
            ]

            for signal in active:

                lines.append(
                    f"{signal['symbol']} "
                    f"{signal['direction']}\n"
                    f"Entry: {signal['entry']}\n"
                    f"SL: {signal['stop']}\n"
                    f"TP: {signal['target']}\n"
                    f"RR: 1:{signal['rr']}\n"
                )

            self.send("\n".join(lines))

        elif command == "/scan":

            with self.lock:

                if self.command_running:

                    self.send(
                        "⏳ Hazırda scan gedir."
                    )

                    return

                self.command_running = True

            self.send(
                "🔎 10 coin analiz edilir..."
            )

            threading.Thread(
                target=self.manual_scan,
                daemon=True
            ).start()

    def manual_scan(self):

        try:

            signals = scan()

            count = 0

            for signal in signals:

                if TRACKER.add(signal):

                    count += 1

                    self.send_signal(signal)

            if count == 0:

                self.send(
                    "ℹ️ Hazırda uyğun setup tapılmadı."
                )

            else:

                self.send(
                    f"✅ Scan tamamlandı.\n"
                    f"Yeni siqnal: {count}"
                )

        except Exception as e:

            logger.error(
                "Manual scan error: %s",
                e
            )

            self.send(
                "❌ Scan zamanı xəta baş verdi."
            )

        finally:

            with self.lock:
                self.command_running = False


TELEGRAM = TelegramBot()


def scan():

    logger.info(
        "Scanning %d coins...",
        len(SYMBOLS)
    )

    candidates = []

    with ThreadPoolExecutor(
        max_workers=5
    ) as executor:

        futures = {
            executor.submit(
                analyze_symbol,
                symbol
            ): symbol
            for symbol in SYMBOLS
        }

        for future in as_completed(futures):

            symbol = futures[future]

            try:

                signal = future.result()

                if signal:
                    candidates.append(signal)

            except Exception as e:

                logger.error(
                    "%s scan error: %s",
                    symbol,
                    e
                )

    candidates.sort(
        key=lambda x: x["volume_ratio"],
        reverse=True
    )

    selected = []

    for signal in candidates:

        if len(selected) >= MAX_ACTIVE_SIGNALS:
            break

        if TRACKER.can_create(
            signal["symbol"]
        ):
            selected.append(signal)

    logger.info(
        "Valid setups: %d",
        len(selected)
    )

    return selected


def scanner_loop():

    logger.info(
        "Scanner loop started"
    )

    while True:

        start_time = time.time()

        try:

            signals = scan()

            for signal in signals:

                if not TRACKER.add(signal):
                    continue

                logger.info(
                    "NEW SIGNAL | %s | %s | Entry=%s | SL=%s | TP=%s",
                    signal["symbol"],
                    signal["direction"],
                    signal["entry"],
                    signal["stop"],
                    signal["target"]
                )

                TELEGRAM.send_signal(signal)

        except Exception as e:

            logger.error(
                "Scanner loop error: %s",
                e
            )

        elapsed = time.time() - start_time

        sleep_time = max(
            10,
            SCAN_INTERVAL - elapsed
        )

        time.sleep(sleep_time)


def telegram_loop():

    logger.info(
        "Telegram command loop started"
    )

    while True:

        try:
            TELEGRAM.process_updates()

        except Exception as e:

            logger.error(
                "Telegram loop error: %s",
                e
            )

        time.sleep(1)
# ============================================
# PART 8/8 - MONITOR + FLASK + MAIN
# ============================================

def monitor_signal(signal):

    symbol = signal["symbol"]

    try:

        df = BYBIT.klines(
            symbol,
            MONITOR_TF,
            100
        )

        if df.empty:
            return

        entry = float(signal["entry"])
        stop = float(signal["stop"])
        target = float(signal["target"])

        risk_distance = abs(
            entry - stop
        )

        if risk_distance <= 0:
            return

        entry_candle_time = int(
            signal.get(
                "entry_candle_timestamp",
                0
            )
        )

        entry_close_time = (
            entry_candle_time
            + 15 * 60 * 1000
        )

        last_checked = int(
            signal.get(
                "last_checked_candle",
                0
            )
        )

        for _, candle in df.iterrows():

            candle_time = int(
                candle["timestamp"]
            )

            if candle_time < entry_close_time:
                continue

            if candle_time <= last_checked:
                continue

            high = float(candle["high"])
            low = float(candle["low"])

            if signal["direction"] == "LONG":

                if low <= stop:

                    result_r = (
                        stop - entry
                    ) / risk_distance

                    TRACKER.close(
                        signal,
                        result_r,
                        "LOSS"
                    )

                    TELEGRAM.send(
                        "🔴 LOSS\n"
                        f"{symbol}\n"
                        f"Result: {result_r:.2f}R"
                    )

                    return

                if high >= target:

                    result_r = (
                        target - entry
                    ) / risk_distance

                    TRACKER.close(
                        signal,
                        result_r,
                        "WIN"
                    )

                    TELEGRAM.send(
                        "🟢 WIN\n"
                        f"{symbol}\n"
                        f"Result: +{result_r:.2f}R"
                    )

                    return

            elif signal["direction"] == "SHORT":

                if high >= stop:

                    result_r = (
                        entry - stop
                    ) / risk_distance

                    TRACKER.close(
                        signal,
                        result_r,
                        "LOSS"
                    )

                    TELEGRAM.send(
                        "🔴 LOSS\n"
                        f"{symbol}\n"
                        f"Result: {result_r:.2f}R"
                    )

                    return

                if low <= target:

                    result_r = (
                        entry - target
                    ) / risk_distance

                    TRACKER.close(
                        signal,
                        result_r,
                        "WIN"
                    )

                    TELEGRAM.send(
                        "🟢 WIN\n"
                        f"{symbol}\n"
                        f"Result: +{result_r:.2f}R"
                    )

                    return

            signal["last_checked_candle"] = candle_time

        signal["last_checked"] = utc_now()

        TRACKER.update(signal)

    except Exception as e:

        logger.error(
            "Monitor error %s: %s",
            symbol,
            e
        )


def monitor_loop():

    logger.info(
        "Monitor loop started"
    )

    while True:

        try:

            active = TRACKER.active()

            for signal in active:
                monitor_signal(signal)

        except Exception as e:

            logger.error(
                "Monitor loop error: %s",
                e
            )

        time.sleep(MONITOR_INTERVAL)


app = Flask(__name__)


@app.route("/")
def home():

    return jsonify({
        "bot": "Simple Trading Bot",
        "version": BOT_VERSION,
        "status": "running",
        "telegram_commands": [
            "/start",
            "/help",
            "/status",
            "/stats",
            "/active",
            "/scan"
        ],
        "strategy": (
            "4H Trend + 1H Pullback + "
            "15M RSI + Strong Candle + "
            "Volume + ATR"
        )
    })


@app.route("/health")
def health():

    return jsonify({
        "status": "ok",
        "time": utc_now()
    })


@app.route("/stats")
def stats():

    return jsonify(
        TRACKER.summary()
    )


@app.route("/active")
def active():

    return jsonify(
        TRACKER.active()
    )


def flask_loop():

    try:

        app.run(
            host="0.0.0.0",
            port=FLASK_PORT,
            debug=False,
            use_reloader=False
        )

    except Exception as e:

        logger.error(
            "Flask error: %s",
            e
        )


def main():

    logger.info(
        "=========================================="
    )

    logger.info(
        "SIMPLE BOT V1.1 STARTED"
    )

    logger.info(
        "Coins: %d",
        len(SYMBOLS)
    )

    logger.info(
        "Risk: %.2f%%",
        RISK_PERCENT
    )

    logger.info(
        "Minimum RR: %.1f",
        MIN_RR
    )

    logger.info(
        "Telegram commands enabled"
    )

    logger.info(
        "=========================================="
    )

    if not BOT_TOKEN:

        logger.warning(
            "BOT_TOKEN not set. "
            "Telegram disabled."
        )

    threading.Thread(
        target=flask_loop,
        daemon=True
    ).start()

    threading.Thread(
        target=scanner_loop,
        daemon=True
    ).start()

    threading.Thread(
        target=monitor_loop,
        daemon=True
    ).start()

    threading.Thread(
        target=telegram_loop,
        daemon=True
    ).start()

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
