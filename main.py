import os
import json
import time
import math
import logging
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

import numpy as np
import pandas as pd
import requests
from flask import Flask, jsonify


# ============================================================
# SWING AI BOT V9.3 PROFESSIONAL (TAM DÜZƏLDİLMİŞ)
# ============================================================

BOT_VERSION = "9.3 PROFESSIONAL"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("SWING_AI")


class Config:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    CHAT_ID: str = os.getenv("CHAT_ID", "1121794078")

    BASE_URL = "https://api.bybit.com"
    CATEGORY = "linear"

    SCAN_TOP_N = 40
    MAX_SIGNALS_TO_SEND = 3
    CHECK_INTERVAL = 300
    MONITOR_INTERVAL = 30
    PARALLEL_WORKERS = 6

    TREND_TF = "240"
    SETUP_TF = "60"
    ENTRY_TF = "15"

    EMA_FAST = 50
    EMA_SLOW = 200
    RSI_PERIOD = 14
    ATR_PERIOD = 14

    SWING_LOOKBACK = 5

    MIN_SCORE = 65
    MIN_RR = 2.0
    MAX_RR = 3.0

    ACCOUNT_BALANCE = 1000.0
    RISK_PERCENT = 1.0
    MAX_ACTIVE_SIGNALS = 3
    MAX_DAILY_SIGNALS = 5

    COMMISSION_PERCENT = 0.04
    SLIPPAGE_PERCENT = 0.02

    MIN_ATR_PCT = 0.15
    MAX_ATR_PCT = 8.0

    VOLUME_LOOKBACK = 20
    MIN_VOLUME_RATIO = 1.10

    MIN_EMA_DISTANCE_PCT = 0.15

    FUNDING_FILTER_ENABLED = True
    MAX_ABS_FUNDING = 0.0015

    OI_FILTER_ENABLED = True
    OI_LOOKBACK = 5
    MIN_OI_CHANGE_PCT = -5.0

    BTC_FILTER_ENABLED = True

    CORRELATION_FILTER_ENABLED = True
    MAX_CORRELATED_ACTIVE = 2
    CORRELATION_THRESHOLD = 0.80

    NEWS_FILTER_ENABLED = False

    REQUIRE_RETEST = True
    RETEST_MAX_BARS = 4
    RETEST_ATR_DISTANCE = 0.60

    PARTIAL_TP_PERCENT = 50.0   # entry->target məsafəsinin neçə %-də "qismən" işarələnsin
    BREAKEVEN_AFTER_R = 1.0
    TRAILING_AFTER_R = 1.5
    TRAILING_ATR_MULT = 1.2

    DEFAULT_LEVERAGE = 10

    DATA_DIR = "swing_bot_data"
    SIGNAL_FILE = os.path.join(DATA_DIR, "signals.json")
    STATS_FILE = os.path.join(DATA_DIR, "stats.json")
    STATE_FILE = os.path.join(DATA_DIR, "state.json")

    FLASK_PORT = int(os.getenv("PORT", "10000"))

    REQUEST_TIMEOUT = 15
    CACHE_TTL = 25
    INSTRUMENT_CACHE_TTL = 3600

    MAX_CLOSED_SIGNALS_KEPT = 200
    MAX_SIGNAL_LOG_KEPT = 1000

    FALLBACK_COINS: List[str] = [
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
        "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "LINKUSDT", "SUIUSDT",
        "ARBUSDT", "OPUSDT"
    ]

    @classmethod
    def validate(cls):
        errors = []
        if not cls.BOT_TOKEN:
            errors.append("BOT_TOKEN boş ola bilməz")
        if cls.MIN_RR < 0:
            errors.append("MIN_RR mənfi ola bilməz")
        if cls.MAX_RR <= cls.MIN_RR:
            errors.append("MAX_RR > MIN_RR olmalıdır")
        if cls.MAX_ACTIVE_SIGNALS < 1:
            errors.append("MAX_ACTIVE_SIGNALS >= 1 olmalıdır")
        if cls.MAX_DAILY_SIGNALS < 1:
            errors.append("MAX_DAILY_SIGNALS >= 1 olmalıdır")
        if cls.PARALLEL_WORKERS < 1:
            errors.append("PARALLEL_WORKERS >= 1 olmalıdır")
        if not (0 <= cls.MIN_SCORE <= 100):
            errors.append("MIN_SCORE 0-100 aralığında olmalıdır")
        if cls.EMA_FAST <= 0 or cls.EMA_SLOW <= 0:
            errors.append("EMA periodları müsbət olmalıdır")
        if cls.RSI_PERIOD <= 0:
            errors.append("RSI_PERIOD müsbət olmalıdır")
        if cls.ATR_PERIOD <= 0:
            errors.append("ATR_PERIOD müsbət olmalıdır")
        if not (0 < cls.RISK_PERCENT <= 100):
            errors.append("RISK_PERCENT 0-100 aralığında olmalıdır")
        if cls.COMMISSION_PERCENT < 0:
            errors.append("COMMISSION_PERCENT mənfi ola bilməz")
        if cls.SLIPPAGE_PERCENT < 0:
            errors.append("SLIPPAGE_PERCENT mənfi ola bilməz")
        if cls.ACCOUNT_BALANCE <= 0:
            errors.append("ACCOUNT_BALANCE müsbət olmalıdır")
        if not (0 <= cls.CORRELATION_THRESHOLD <= 1):
            errors.append("CORRELATION_THRESHOLD 0-1 aralığında olmalıdır")
        if cls.MAX_CORRELATED_ACTIVE < 1:
            errors.append("MAX_CORRELATED_ACTIVE >= 1 olmalıdır")
        if errors:
            raise ValueError("Config xətaları: " + "; ".join(errors))


os.makedirs(Config.DATA_DIR, exist_ok=True)


def load_json(path: str, default):
    try:
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error("JSON load error (%s): %s", path, e)
        return default


def save_json(path: str, data):
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception as e:
        logger.error("JSON save error (%s): %s", path, e)


class APICache:
    def __init__(self):
        self.data = {}
        self.lock = threading.RLock()

    def get(self, key):
        with self.lock:
            item = self.data.get(key)
            if not item:
                return None
            value, timestamp = item
            if time.time() - timestamp > Config.CACHE_TTL:
                del self.data[key]
                return None
            return value

    def set(self, key, value):
        with self.lock:
            self.data[key] = (value, time.time())


class InstrumentCache:
    def __init__(self):
        self.data = {}
        self.lock = threading.RLock()

    def get(self, symbol):
        with self.lock:
            item = self.data.get(symbol)
            if not item:
                return None
            value, timestamp = item
            if time.time() - timestamp > Config.INSTRUMENT_CACHE_TTL:
                del self.data[symbol]
                return None
            return value

    def set(self, symbol, value):
        with self.lock:
            self.data[symbol] = (value, time.time())


CACHE = APICache()
INSTRUMENT_CACHE = InstrumentCache()
class BybitClient:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": f"SwingAIProfessional/{BOT_VERSION}"
        })

    def get(self, endpoint, params=None, use_cache=True, retries=2):
        key = endpoint + "|" + json.dumps(params or {}, sort_keys=True)

        if use_cache:
            cached = CACHE.get(key)
            if cached is not None:
                return cached

        url = Config.BASE_URL + endpoint
        last_err = None

        for attempt in range(retries + 1):
            try:
                r = self.session.get(url, params=params, timeout=Config.REQUEST_TIMEOUT)
                r.raise_for_status()
                data = r.json()
                if data.get("retCode", 0) != 0:
                    raise RuntimeError(str(data.get("retMsg", "Bybit error")))
                if use_cache:
                    CACHE.set(key, data)
                return data
            except Exception as e:
                last_err = e
                if attempt < retries:
                    time.sleep(0.5 * (attempt + 1))
                    continue

        logger.warning("Bybit request failed %s: %s", endpoint, last_err)
        raise last_err

    def fetch_klines(self, symbol: str, interval: str, limit: int = 500) -> pd.DataFrame:
        data = self.get("/v5/market/kline", {
            "category": Config.CATEGORY,
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        })
        rows = data["result"]["list"]
        rows = list(reversed(rows))
        df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"])
        for c in ["open", "high", "low", "close", "volume", "turnover"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df["timestamp"] = pd.to_datetime(pd.to_numeric(df["timestamp"]), unit="ms", utc=True)
        df = df.dropna().reset_index(drop=True)

        if len(df) > 2:
            now = pd.Timestamp.now(tz="UTC")
            last_time = df.iloc[-1]["timestamp"]
            if interval == "D":
                candle_seconds = 86400
            else:
                candle_seconds = int(interval) * 60
            age = (now.to_pydatetime() - last_time.to_pydatetime()).total_seconds()
            if age < candle_seconds:
                df = df.iloc[:-1].copy()

        return df.reset_index(drop=True)

    def fetch_tickers(self):
        data = self.get("/v5/market/tickers", {"category": Config.CATEGORY}, use_cache=False)
        return data["result"]["list"]

    def fetch_funding(self, symbol):
        data = self.get("/v5/market/funding/history",
                         {"category": Config.CATEGORY, "symbol": symbol, "limit": 1}, use_cache=False)
        rows = data["result"]["list"]
        if not rows:
            return 0.0
        return float(rows[0].get("fundingRate", 0))

    def fetch_open_interest(self, symbol):
        data = self.get("/v5/market/open-interest",
                         {"category": Config.CATEGORY, "symbol": symbol,
                          "intervalTime": "1h", "limit": Config.OI_LOOKBACK}, use_cache=False)
        rows = data["result"]["list"]
        if len(rows) < 2:
            return 0.0
        values = [float(x["openInterest"]) for x in rows]
        old = values[-1]
        new = values[0]
        if old == 0:
            return 0.0
        return (new - old) / old * 100.0

    def fetch_orderbook(self, symbol):
        return self.get("/v5/market/orderbook",
                         {"category": Config.CATEGORY, "symbol": symbol, "limit": 5}, use_cache=False)

    def fetch_instrument(self, symbol):
        cached = INSTRUMENT_CACHE.get(symbol)
        if cached is not None:
            return cached
        data = self.get("/v5/market/instruments-info",
                         {"category": Config.CATEGORY, "symbol": symbol})
        rows = data["result"]["list"]
        result = rows[0] if rows else {}
        INSTRUMENT_CACHE.set(symbol, result)
        return result

    def fetch_top_symbols(self):
        try:
            tickers = self.fetch_tickers()
            clean = []
            for t in tickers:
                symbol = t.get("symbol", "")
                if not symbol.endswith("USDT"):
                    continue
                if t.get("status") not in [None, "Trading"]:
                    continue
                try:
                    turnover = float(t.get("turnover24h", 0))
                except Exception:
                    turnover = 0
                if turnover <= 0:
                    continue
                clean.append((symbol, turnover))
            clean.sort(key=lambda x: x[1], reverse=True)
            symbols = [x[0] for x in clean[:Config.SCAN_TOP_N]]
            if symbols:
                return symbols
        except Exception as e:
            logger.warning("Top symbol scan failed: %s", e)
        return Config.FALLBACK_COINS.copy()


BYBIT = BybitClient()
# ============================================================
# INDICATORS
# ============================================================

class Indicators:

    @staticmethod
    def ema(series, period):
        return series.ewm(span=period, adjust=False).mean()

    @staticmethod
    def rsi(series, period=14):
        delta = series.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        return rsi.fillna(50)

    @staticmethod
    def atr(df, period=14):
        prev_close = df["close"].shift(1)
        tr1 = df["high"] - df["low"]
        tr2 = (df["high"] - prev_close).abs()
        tr3 = (df["low"] - prev_close).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        return tr.ewm(alpha=1 / period, adjust=False).mean()

    @staticmethod
    def add_all(df):
        df = df.copy()
        df["ema50"] = Indicators.ema(df["close"], Config.EMA_FAST)
        df["ema200"] = Indicators.ema(df["close"], Config.EMA_SLOW)
        df["rsi"] = Indicators.rsi(df["close"], Config.RSI_PERIOD)
        df["atr"] = Indicators.atr(df, Config.ATR_PERIOD)
        df["vol_ma"] = df["volume"].rolling(Config.VOLUME_LOOKBACK).mean()
        return df


# ============================================================
# SWING STRUCTURE
# DÜZƏLİŞ B: bütün metodlar indi əvvəlcədən hesablanmış `swings`
# (highs, lows) qəbul edə bilir - eyni df üçün 4 dəfə təkrar
# axtarış aparılmır (setup_direction/premium_discount/
# nearest_stop/nearest_target artıq bunu paylaşır).
# ============================================================

class Structure:

    @staticmethod
    def swing_points(df):
        n = Config.SWING_LOOKBACK
        highs = []
        lows = []
        if len(df) < n * 2 + 5:
            return highs, lows
        for i in range(n, len(df) - n):
            h = df["high"].iloc[i]
            l = df["low"].iloc[i]
            left_high = df["high"].iloc[i - n:i]
            right_high = df["high"].iloc[i + 1:i + n + 1]
            left_low = df["low"].iloc[i - n:i]
            right_low = df["low"].iloc[i + 1:i + n + 1]
            if h > left_high.max() and h > right_high.max():
                highs.append((i, float(h)))
            if l < left_low.min() and l < right_low.min():
                lows.append((i, float(l)))
        return highs, lows

    @staticmethod
    def trend_structure(df, swings=None):
        highs, lows = swings if swings is not None else Structure.swing_points(df)
        if len(highs) < 2 or len(lows) < 2:
            return "neutral"
        h1, h2 = highs[-2][1], highs[-1][1]
        l1, l2 = lows[-2][1], lows[-1][1]
        if h2 > h1 and l2 > l1:
            return "bullish"
        if h2 < h1 and l2 < l1:
            return "bearish"
        return "neutral"

    @staticmethod
    def latest_high(df, swings=None):
        highs, _ = swings if swings is not None else Structure.swing_points(df)
        return highs[-1] if highs else None

    @staticmethod
    def latest_low(df, swings=None):
        _, lows = swings if swings is not None else Structure.swing_points(df)
        return lows[-1] if lows else None


# ============================================================
# REGIME FILTER
# ============================================================

class RegimeFilter:

    @staticmethod
    def analyze(df):
        if len(df) < 220:
            return {"bull": False, "bear": False, "quality": 0, "distance": 0.0}
        last = df.iloc[-1]
        ema50 = float(last["ema50"])
        ema200 = float(last["ema200"])
        price = float(last["close"])
        if ema200 == 0:
            return {"bull": False, "bear": False, "quality": 0, "distance": 0.0}
        distance = abs(ema50 - ema200) / ema200 * 100
        structure = Structure.trend_structure(df)
        bull = (price > ema200 and ema50 > ema200 and structure == "bullish")
        bear = (price < ema200 and ema50 < ema200 and structure == "bearish")
        quality = min(100, int(distance * 25))
        return {"bull": bull, "bear": bear, "quality": quality, "distance": distance}


# ============================================================
# VOLUME FILTER
# ============================================================

class VolumeFilter:

    @staticmethod
    def check(df):
        if len(df) < Config.VOLUME_LOOKBACK + 5:
            return False, 0.0
        last = df.iloc[-1]
        ma = float(last["vol_ma"])
        if ma <= 0:
            return False, 0.0
        ratio = float(last["volume"]) / ma
        return ratio >= Config.MIN_VOLUME_RATIO, ratio


# ============================================================
# ATR FILTER
# ============================================================

class ATRFilter:

    @staticmethod
    def check(df):
        last = df.iloc[-1]
        price = float(last["close"])
        atr = float(last["atr"])
        if price <= 0 or atr <= 0:
            return False, 0.0
        atr_pct = atr / price * 100
        valid = Config.MIN_ATR_PCT <= atr_pct <= Config.MAX_ATR_PCT
        return valid, atr_pct


# ============================================================
# FRESHNESS FILTER
# ============================================================

class FreshnessFilter:

    @staticmethod
    def check(df):
        if len(df) < 2:
            return False
        last_time = df["timestamp"].iloc[-1]
        now = pd.Timestamp.now(tz="UTC")
        age_minutes = (now - last_time).total_seconds() / 60
        return age_minutes <= 30
# ============================================================
# STRATEGY ENGINE
# DÜZƏLİŞ A+B: trend_4h artıq hazır `regime` qəbul edə bilir;
# setup_direction/premium_discount/nearest_stop/nearest_target
# artıq hazır `swings` qəbul edə bilir.
# ============================================================

class StrategyEngine:

    @staticmethod
    def trend_4h(df, regime=None):
        if len(df) < 220:
            return "neutral"
        if regime is None:
            regime = RegimeFilter.analyze(df)
        if regime["bull"]:
            return "long"
        if regime["bear"]:
            return "short"
        return "neutral"

    @staticmethod
    def setup_direction(df, swings=None):
        direction = Structure.trend_structure(df, swings=swings)
        if direction == "bullish":
            return "long"
        if direction == "bearish":
            return "short"
        return "neutral"

    @staticmethod
    def pullback(df, direction):
        if len(df) < 10:
            return False
        recent = df.iloc[-4:]
        if direction == "long":
            for _, row in recent.iterrows():
                if row["low"] <= row["ema50"] + row["atr"] * Config.RETEST_ATR_DISTANCE:
                    return True
            return False
        for _, row in recent.iterrows():
            if row["high"] >= row["ema50"] - row["atr"] * Config.RETEST_ATR_DISTANCE:
                return True
        return False

    @staticmethod
    def rsi_reversal(df, direction):
        if len(df) < 5:
            return False
        r = df["rsi"].iloc[-5:]
        if direction == "long":
            return (r.iloc[-5] < 50 and r.iloc[-2] <= 50 and r.iloc[-1] > 50 and r.iloc[-1] > r.iloc[-2])
        return (r.iloc[-5] > 50 and r.iloc[-2] >= 50 and r.iloc[-1] < 50 and r.iloc[-1] < r.iloc[-2])

    @staticmethod
    def fresh_bos(df, direction):
        if len(df) < 20:
            return False, None
        highs, lows = Structure.swing_points(df)
        if direction == "long":
            if not highs:
                return False, None
            _, level = highs[-1]
            prev_close = float(df["close"].iloc[-2])
            last_close = float(df["close"].iloc[-1])
            return (prev_close <= level and last_close > level), level
        if not lows:
            return False, None
        _, level = lows[-1]
        prev_close = float(df["close"].iloc[-2])
        last_close = float(df["close"].iloc[-1])
        return (prev_close >= level and last_close < level), level

    @staticmethod
    def strong_candle(df, direction):
        if len(df) < 3:
            return False
        row = df.iloc[-1]
        body = abs(float(row["close"]) - float(row["open"]))
        atr = float(row["atr"])
        if atr <= 0:
            return False
        body_ok = body >= atr * 0.50
        if direction == "long":
            return (body_ok and row["close"] > row["open"] and row["close"] > df["close"].iloc[-2])
        return (body_ok and row["close"] < row["open"] and row["close"] < df["close"].iloc[-2])

    @staticmethod
    def premium_discount(df, direction, swings=None):
        highs, lows = swings if swings is not None else Structure.swing_points(df)
        if not highs or not lows:
            return False
        high, low = highs[-1][1], lows[-1][1]
        if high <= low:
            return False
        price = float(df["close"].iloc[-1])
        midpoint = (high + low) / 2
        if direction == "long":
            return price <= midpoint
        return price >= midpoint

    @staticmethod
    def nearest_stop(df, direction, swings=None):
        atr = float(df["atr"].iloc[-1])
        highs, lows = swings if swings is not None else Structure.swing_points(df)
        if direction == "long":
            if not lows:
                return None
            return float(lows[-1][1]) - atr * 0.20
        if not highs:
            return None
        return float(highs[-1][1]) + atr * 0.20

    @staticmethod
    def nearest_target(df, direction, entry, swings=None):
        highs, lows = swings if swings is not None else Structure.swing_points(df)
        if direction == "long":
            targets = [price for _, price in highs if price > entry]
            return min(targets) if targets else None
        targets = [price for _, price in lows if price < entry]
        return max(targets) if targets else None


# ============================================================
# RETEST CONFIRMATION
# ============================================================

class RetestFilter:

    @staticmethod
    def check(df, direction, bos_level):
        if not Config.REQUIRE_RETEST:
            return True
        if bos_level is None:
            return False
        atr = float(df["atr"].iloc[-1])
        recent = df.iloc[-Config.RETEST_MAX_BARS:]
        for _, row in recent.iterrows():
            if direction == "long":
                touched = row["low"] <= bos_level + atr * Config.RETEST_ATR_DISTANCE
                held = row["close"] >= bos_level
                if touched and held:
                    return True
            else:
                touched = row["high"] >= bos_level - atr * Config.RETEST_ATR_DISTANCE
                held = row["close"] <= bos_level
                if touched and held:
                    return True
        return False


# ============================================================
# BTC MARKET FILTER
# ============================================================

class BTCFilter:

    @staticmethod
    def get_bias():
        if not Config.BTC_FILTER_ENABLED:
            return "neutral"
        try:
            df = BYBIT.fetch_klines("BTCUSDT", Config.TREND_TF, 250)
            df = Indicators.add_all(df)
            if len(df) < 220:
                return "neutral"
            last = df.iloc[-1]
            if last["close"] > last["ema200"] and last["ema50"] > last["ema200"]:
                return "bull"
            if last["close"] < last["ema200"] and last["ema50"] < last["ema200"]:
                return "bear"
            return "neutral"
        except Exception:
            return "neutral"

    @staticmethod
    def allows(direction):
        if not Config.BTC_FILTER_ENABLED:
            return True
        bias = BTCFilter.get_bias()
        if direction == "long":
            return bias != "bear"
        return bias != "bull"


# ============================================================
# FUNDING FILTER
# ============================================================

class FundingFilter:

    @staticmethod
    def check(symbol, direction):
        if not Config.FUNDING_FILTER_ENABLED:
            return True, 0.0
        try:
            funding = BYBIT.fetch_funding(symbol)
            if abs(funding) > Config.MAX_ABS_FUNDING:
                return False, funding
            if direction == "long" and funding > Config.MAX_ABS_FUNDING * 0.7:
                return False, funding
            if direction == "short" and funding < -Config.MAX_ABS_FUNDING * 0.7:
                return False, funding
            return True, funding
        except Exception:
            return True, 0.0


# ============================================================
# OPEN INTEREST FILTER
# ============================================================

class OIFilter:

    @staticmethod
    def check(symbol, direction):
        if not Config.OI_FILTER_ENABLED:
            return True, 0.0
        try:
            change = BYBIT.fetch_open_interest(symbol)
            if change < Config.MIN_OI_CHANGE_PCT:
                return False, change
            return True, change
        except Exception:
            return True, 0.0


# ============================================================
# SPREAD FILTER
# ============================================================

class SpreadFilter:

    @staticmethod
    def check(symbol):
        try:
            data = BYBIT.fetch_orderbook(symbol)
            result = data["result"]
            bids = result.get("b", [])
            asks = result.get("a", [])
            if not bids or not asks:
                return False, 999.0
            bid = float(bids[0][0])
            ask = float(asks[0][0])
            if bid <= 0 or ask <= 0:
                return False, 999.0
            mid = (bid + ask) / 2
            spread = (ask - bid) / mid * 100
            return spread <= 0.15, spread
        except Exception:
            return False, 999.0


# ============================================================
# CORRELATION FILTER
# ============================================================

class CorrelationFilter:

    @staticmethod
    def _corr_between(sym1, sym2):
        try:
            df1 = BYBIT.fetch_klines(sym1, Config.SETUP_TF, 100)
            df2 = BYBIT.fetch_klines(sym2, Config.SETUP_TF, 100)
            if len(df1) < 50 or len(df2) < 50:
                return 0.0
            ret1 = df1["close"].pct_change().dropna()
            ret2 = df2["close"].pct_change().dropna()
            n = min(len(ret1), len(ret2))
            if n < 30:
                return 0.0
            corr = float(ret1.iloc[-n:].corr(ret2.iloc[-n:]))
            return abs(corr) if not math.isnan(corr) else 0.0
        except Exception:
            return 0.0

    @staticmethod
    def allows(symbol, active_signals):
        if not Config.CORRELATION_FILTER_ENABLED:
            return True, 0.0
        correlated_count = 0
        max_corr = 0.0
        for signal in active_signals:
            other_symbol = signal.get("symbol")
            if not other_symbol or other_symbol == symbol:
                continue
            corr = CorrelationFilter._corr_between(symbol, other_symbol)
            max_corr = max(max_corr, corr)
            if corr >= Config.CORRELATION_THRESHOLD:
                correlated_count += 1
        if correlated_count >= Config.MAX_CORRELATED_ACTIVE:
            return False, max_corr
        return True, max_corr


# ============================================================
# NEWS FILTER
# ============================================================

class NewsFilter:

    @staticmethod
    def check(symbol):
        if not Config.NEWS_FILTER_ENABLED:
            return True, "disabled"
        return True, "not_configured"
# ============================================================
# PRECISION
# ============================================================

class Precision:

    @staticmethod
    def decimals(step):
        try:
            text = format(float(step), "f").rstrip("0")
            if "." not in text:
                return 0
            return len(text.split(".")[1])
        except Exception:
            return 8

    @staticmethod
    def round_step(value, step):
        if not step or step <= 0:
            return value
        decimals = Precision.decimals(step)
        result = math.floor(value / step) * step
        return round(result, decimals)

    @staticmethod
    def price(symbol, value):
        try:
            info = BYBIT.fetch_instrument(symbol)
            tick = float(info["priceFilter"]["tickSize"])
            return Precision.round_step(value, tick)
        except Exception:
            return float(value)

    @staticmethod
    def quantity(symbol, qty):
        try:
            info = BYBIT.fetch_instrument(symbol)
            step = float(info["lotSizeFilter"]["qtyStep"])
            min_qty = float(info["lotSizeFilter"].get("minOrderQty", 0))
            qty = Precision.round_step(qty, step)
            if qty < min_qty:
                return 0.0
            return qty
        except Exception:
            return round(qty, 6)


# ============================================================
# RISK ENGINE
# ============================================================

class RiskEngine:

    @staticmethod
    def position_size(symbol, entry, stop):
        risk_amount = Config.ACCOUNT_BALANCE * Config.RISK_PERCENT / 100
        distance = abs(entry - stop)
        if distance <= 0:
            return 0.0
        qty = risk_amount / distance
        return Precision.quantity(symbol, qty)

    @staticmethod
    def rr(entry, stop, target):
        risk = abs(entry - stop)
        reward = abs(target - entry)
        if risk <= 0:
            return 0.0
        return reward / risk

    @staticmethod
    def liquidation_estimate(entry, direction, leverage=None):
        leverage = leverage or Config.DEFAULT_LEVERAGE
        if leverage <= 0:
            return None
        if direction == "long":
            return entry * (1 - 0.9 / leverage)
        return entry * (1 + 0.9 / leverage)


# ============================================================
# SCORE ENGINE — yalnız keyfiyyət göstəriciləri (hard-filter deyil)
# ============================================================

class ScoreEngine:

    @staticmethod
    def calculate(data):
        score = 0.0

        rr = data.get("rr", 0.0)
        rr_range = max(0.0001, Config.MAX_RR - Config.MIN_RR)
        score += max(0.0, min(1.0, (rr - Config.MIN_RR) / rr_range)) * 30

        ratio = data.get("volume_ratio", 0.0)
        score += max(0.0, min(1.0, (ratio - Config.MIN_VOLUME_RATIO) / 1.0)) * 20

        regime_quality = data.get("regime_quality", 0)
        score += (min(100, regime_quality) / 100) * 20

        funding = abs(data.get("funding", 0.0))
        score += max(0.0, 1 - (funding / max(Config.MAX_ABS_FUNDING, 1e-9))) * 15

        oi_change = data.get("oi_change", 0.0)
        if oi_change >= 0:
            score += 10.0
        else:
            oi_range = max(0.0001, abs(Config.MIN_OI_CHANGE_PCT))
            score += max(0.0, 1 - abs(oi_change) / oi_range) * 10

        spread = data.get("spread_pct", 999.0)
        score += max(0.0, 1 - spread / 0.15) * 5

        return round(min(100.0, max(0.0, score)), 1)


# ============================================================
# SIGNAL SCANNER
# DÜZƏLİŞ A: regime bir dəfə hesablanır (trend + score üçün paylaşılır)
# DÜZƏLİŞ B: swing_points(df1) bir dəfə hesablanır, 4 funksiyaya ötürülür
# DÜZƏLİŞ C: original_risk artıq Precision-dan SONRAKI entry/stop-dan hesablanır
# ============================================================

class SignalScanner:

    def analyze_symbol(self, symbol, manual=False):
        reasons = []
        try:
            df4 = BYBIT.fetch_klines(symbol, Config.TREND_TF, 300)
            df1 = BYBIT.fetch_klines(symbol, Config.SETUP_TF, 300)
            df15 = BYBIT.fetch_klines(symbol, Config.ENTRY_TF, 200)
            if len(df4) < 220 or len(df1) < 80 or len(df15) < 50:
                return None

            df4 = Indicators.add_all(df4)
            df1 = Indicators.add_all(df1)
            df15 = Indicators.add_all(df15)

            fresh_ok = FreshnessFilter.check(df15)
            if not fresh_ok:
                reasons.append("stale_market_data")

            # DÜZƏLİŞ A: regime bir dəfə hesablanır
            regime = RegimeFilter.analyze(df4)
            trend = StrategyEngine.trend_4h(df4, regime=regime)
            if trend == "neutral":
                return None
            direction = trend

            # DÜZƏLİŞ B: swing_points(df1) bir dəfə hesablanır
            swings_1h = Structure.swing_points(df1)

            setup_direction = StrategyEngine.setup_direction(df1, swings=swings_1h)
            if setup_direction != direction:
                return None

            pullback_ok = StrategyEngine.pullback(df1, direction)
            if not pullback_ok:
                reasons.append("no_pullback")

            rsi_ok = StrategyEngine.rsi_reversal(df1, direction)
            if not rsi_ok:
                reasons.append("no_rsi_reversal")

            bos_ok, bos_level = StrategyEngine.fresh_bos(df15, direction)
            if not bos_ok:
                reasons.append("no_fresh_bos")

            candle_ok = StrategyEngine.strong_candle(df15, direction)
            if not candle_ok:
                reasons.append("weak_candle")

            volume_ok, volume_ratio = VolumeFilter.check(df15)
            if not volume_ok:
                reasons.append("low_volume")

            atr_ok, atr_pct = ATRFilter.check(df15)
            if not atr_ok:
                reasons.append("bad_atr_regime")

            btc_ok = BTCFilter.allows(direction)
            if not btc_ok:
                reasons.append("btc_filter")

            funding_ok, funding = FundingFilter.check(symbol, direction)
            if not funding_ok:
                reasons.append("funding_extreme")

            oi_ok, oi_change = OIFilter.check(symbol, direction)
            if not oi_ok:
                reasons.append("open_interest_filter")

            spread_ok, spread = SpreadFilter.check(symbol)
            if not spread_ok:
                reasons.append("spread_too_wide")

            news_ok, _ = NewsFilter.check(symbol)

            retest_ok = RetestFilter.check(df15, direction, bos_level)
            if not retest_ok:
                reasons.append("retest_not_confirmed")

            premium_ok = StrategyEngine.premium_discount(df1, direction, swings=swings_1h)
            if not premium_ok:
                reasons.append("wrong_premium_discount")

            last = df15.iloc[-1]
            entry_raw = float(last["close"])

            if bos_ok and bos_level is not None:
                atr15 = float(df15["atr"].iloc[-1])
                distance = abs(entry_raw - bos_level)
                if distance > 0.5 * atr15:
                    logger.debug("%s rədd edildi: entry_too_far_from_breakout", symbol)
                    return None

            stop_raw = StrategyEngine.nearest_stop(df1, direction, swings=swings_1h)
            if stop_raw is None:
                return None

            target_raw = StrategyEngine.nearest_target(df1, direction, entry_raw, swings=swings_1h)
            if target_raw is None:
                return None

            rr = RiskEngine.rr(entry_raw, stop_raw, target_raw)
            if rr > Config.MAX_RR:
                risk = abs(entry_raw - stop_raw)
                if direction == "long":
                    target_raw = entry_raw + risk * Config.MAX_RR
                else:
                    target_raw = entry_raw - risk * Config.MAX_RR
                rr = Config.MAX_RR

            hard_checks = {
                "fresh": fresh_ok, "pullback": pullback_ok, "rsi": rsi_ok, "bos": bos_ok,
                "candle": candle_ok, "volume": volume_ok, "atr": atr_ok, "btc": btc_ok,
                "funding": funding_ok, "oi": oi_ok, "spread": spread_ok, "news": news_ok,
                "retest": retest_ok, "premium_discount": premium_ok, "rr": rr >= Config.MIN_RR,
            }
            failed = [k for k, v in hard_checks.items() if not v]
            if failed:
                logger.debug("%s rədd edildi (hard-filter): %s | reasons=%s", symbol, failed, reasons)
                return None

            quality_data = {
                "rr": rr, "volume_ratio": volume_ratio,
                "regime_quality": regime.get("quality", 0),
                "funding": funding, "oi_change": oi_change, "spread_pct": spread,
            }
            score = ScoreEngine.calculate(quality_data)
            if score < Config.MIN_SCORE:
                logger.debug("%s rədd edildi: score=%.1f < MIN_SCORE=%s", symbol, score, Config.MIN_SCORE)
                return None

            active = TRACKER.active_signals()
            correlation_ok, correlation = CorrelationFilter.allows(symbol, active)
            if not correlation_ok:
                return None

            # DÜZƏLİŞ C: əvvəlcə tick-size-a yuvarlaqlaşdır, sonra RİSK bunlardan hesablanır
            entry = Precision.price(symbol, entry_raw)
            stop = Precision.price(symbol, stop_raw)
            target = Precision.price(symbol, target_raw)
            original_risk = abs(entry - stop)
            if original_risk <= 0:
                return None

            qty = RiskEngine.position_size(symbol, entry, stop)
            if qty <= 0:
                return None

            liquidation = RiskEngine.liquidation_estimate(entry, direction)

            signal = {
                "id": f"{symbol}_{int(time.time())}",
                "symbol": symbol,
                "direction": direction.upper(),
                "entry": entry,
                "stop": stop,
                "target": target,
                "rr": round(rr, 2),
                "score": score,
                "quantity": qty,
                "funding": funding,
                "oi_change": oi_change,
                "spread_pct": spread,
                "volume_ratio": volume_ratio,
                "atr_pct": atr_pct,
                "correlation": correlation,
                "btc_bias": BTCFilter.get_bias(),
                "bos_level": bos_level,
                "liquidation_estimate": liquidation,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "status": "ACTIVE",
                "partial_taken": False,
                "breakeven": False,
                "trailing": False,
                "manual": manual,
                "last_checked_at": None,
                "original_risk": original_risk,
            }
            return signal
        except Exception as e:
            logger.error("%s analysis error: %s", symbol, e)
            return None

    def scan(self):
        symbols = BYBIT.fetch_top_symbols()
        results = []
        with ThreadPoolExecutor(max_workers=Config.PARALLEL_WORKERS) as executor:
            futures = {executor.submit(self.analyze_symbol, symbol): symbol for symbol in symbols}
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    logger.warning("Scanner worker error: %s", e)
        results.sort(key=lambda x: (x["score"], x["rr"]), reverse=True)
        return results[:Config.MAX_SIGNALS_TO_SEND]
# ============================================================
# PERFORMANCE TRACKER
# ============================================================

class PerformanceTracker:

    def __init__(self):
        self.lock = threading.RLock()
        self.state = load_json(Config.STATE_FILE, {"signals": [], "daily_date": "", "daily_count": 0})
        self.stats = load_json(Config.STATS_FILE, {
            "wins": 0, "losses": 0, "breakevens": 0, "total": 0,
            "profit_r": 0.0, "profit_usdt": 0.0,
            "total_profit_r": 0.0, "total_loss_r": 0.0,
            "expectancy": 0.0,
            "max_consecutive_losses": 0,
            "score_groups": {},
            "direction_stats": {}
        })

    def save(self):
        active = [s for s in self.state["signals"] if s.get("status") == "ACTIVE"]
        closed = [s for s in self.state["signals"] if s.get("status") != "ACTIVE"]
        closed = closed[-Config.MAX_CLOSED_SIGNALS_KEPT:]
        self.state["signals"] = active + closed
        save_json(Config.STATE_FILE, self.state)
        save_json(Config.STATS_FILE, self.stats)

    def log_signal_created(self, signal):
        log = load_json(Config.SIGNAL_FILE, [])
        log.append({
            "id": signal["id"], "symbol": signal["symbol"], "direction": signal["direction"],
            "entry": signal["entry"], "stop": signal["stop"], "target": signal["target"],
            "rr": signal["rr"], "score": signal["score"], "created_at": signal["created_at"]
        })
        log = log[-Config.MAX_SIGNAL_LOG_KEPT:]
        save_json(Config.SIGNAL_FILE, log)

    def reset_day_if_needed(self):
        today = datetime.now(timezone.utc).date().isoformat()
        if self.state.get("daily_date") != today:
            self.state["daily_date"] = today
            self.state["daily_count"] = 0
            self.save()

    def can_create_signal(self):
        with self.lock:
            self.reset_day_if_needed()
            active = [x for x in self.state["signals"] if x.get("status") == "ACTIVE"]
            if len(active) >= Config.MAX_ACTIVE_SIGNALS:
                return False
            if self.state["daily_count"] >= Config.MAX_DAILY_SIGNALS:
                return False
            return True

    def add_signal(self, signal):
        with self.lock:
            self.reset_day_if_needed()
            active = [x for x in self.state["signals"] if x.get("status") == "ACTIVE"]
            if len(active) >= Config.MAX_ACTIVE_SIGNALS:
                return False
            if self.state["daily_count"] >= Config.MAX_DAILY_SIGNALS:
                return False
            self.state["signals"].append(signal)
            self.state["daily_count"] += 1
            self.save()
            self.log_signal_created(signal)
            return True

    def active_signals(self):
        with self.lock:
            return [x for x in self.state["signals"] if x.get("status") == "ACTIVE"]

    def update_signal(self, signal):
        with self.lock:
            for i, item in enumerate(self.state["signals"]):
                if item.get("id") == signal.get("id"):
                    self.state["signals"][i] = signal
                    break
            self.save()

    def close_signal(self, signal, result, r_multiple):
        with self.lock:
            signal["status"] = result
            signal["closed_at"] = datetime.now(timezone.utc).isoformat()
            signal["result_r"] = r_multiple

            self.stats["total"] += 1
            if result == "WIN":
                self.stats["wins"] += 1
            elif result == "LOSS":
                self.stats["losses"] += 1
            elif result == "BREAKEVEN":
                self.stats["breakevens"] += 1

            self.stats["profit_r"] += r_multiple
            self.stats["profit_usdt"] += r_multiple * Config.ACCOUNT_BALANCE * Config.RISK_PERCENT / 100

            self._update_extended_stats(signal, result, r_multiple)
            self.update_signal(signal)

    def _update_extended_stats(self, signal, result, r_multiple):
        if r_multiple > 0:
            self.stats["total_profit_r"] = self.stats.get("total_profit_r", 0.0) + r_multiple
        elif r_multiple < 0:
            self.stats["total_loss_r"] = self.stats.get("total_loss_r", 0.0) + abs(r_multiple)

        total_trades = self.stats["total"]
        if total_trades > 0:
            self.stats["expectancy"] = self.stats["profit_r"] / total_trades

        if result == "LOSS":
            max_streak = 0
            current = 0
            for s in self.state["signals"]:
                if s.get("status") == "LOSS":
                    current += 1
                    max_streak = max(max_streak, current)
                else:
                    current = 0
            self.stats["max_consecutive_losses"] = max_streak

        score = signal.get("score", 0)
        if 85 <= score < 95:
            key = "85-95"
        elif score >= 95:
            key = "95+"
        elif 65 <= score < 85:
            key = "65-85"
        else:
            key = None
        if key:
            self.stats.setdefault("score_groups", {})
            group = self.stats["score_groups"].setdefault(key, {"total": 0, "wins": 0, "pnl_r": 0.0})
            group["total"] += 1
            if result == "WIN":
                group["wins"] += 1
            group["pnl_r"] += r_multiple

        direction_key = "LONG" if signal.get("direction") == "LONG" else "SHORT"
        self.stats.setdefault("direction_stats", {})
        d = self.stats["direction_stats"].setdefault(direction_key, {"total": 0, "wins": 0, "pnl_r": 0.0})
        d["total"] += 1
        if result == "WIN":
            d["wins"] += 1
        d["pnl_r"] += r_multiple

    def report(self):
        with self.lock:
            total = self.stats["total"]
            winrate = (self.stats["wins"] / total * 100) if total > 0 else 0.0

            total_profit_r = self.stats.get("total_profit_r", 0.0)
            total_loss_r = self.stats.get("total_loss_r", 0.0)

            if total_loss_r > 0:
                profit_factor_display = round(total_profit_r / total_loss_r, 2)
            elif total_profit_r > 0:
                profit_factor_display = "∞"
            else:
                profit_factor_display = 0.0

            return {
                "total": total,
                "wins": self.stats["wins"],
                "losses": self.stats["losses"],
                "breakevens": self.stats["breakevens"],
                "winrate": round(winrate, 2),
                "profit_r": round(self.stats["profit_r"], 2),
                "profit_usdt": round(self.stats["profit_usdt"], 2),
                "profit_factor": profit_factor_display,
                "expectancy": round(self.stats.get("expectancy", 0.0), 4),
                "max_consecutive_losses": self.stats.get("max_consecutive_losses", 0),
                "score_groups": self.stats.get("score_groups", {}),
                "direction_stats": self.stats.get("direction_stats", {})
            }


# ============================================================
# POSITION MANAGER
# DÜZƏLİŞ: WIN/LOSS/BREAKEVEN r_mult işarəsinə görə təyin olunur.
# YENİ: PARTIAL_TP_PERCENT indi real işləyir (informativ - broker
# icrası olmadığından mövqe bağlanmır, sadəcə qeyd olunur/loglanır).
# ============================================================

class PositionManager:

    RESULT_EPSILON = 0.05

    def __init__(self, tracker):
        self.tracker = tracker

    @staticmethod
    def _classify(r_mult, epsilon):
        if r_mult > epsilon:
            return "WIN"
        if r_mult < -epsilon:
            return "LOSS"
        return "BREAKEVEN"

    def _check_partial(self, signal, direction, entry, target, current):
        if signal.get("partial_taken"):
            return
        pct = Config.PARTIAL_TP_PERCENT / 100
        if direction == "long":
            partial_level = entry + (target - entry) * pct
            reached = current >= partial_level
        else:
            partial_level = entry - (entry - target) * pct
            reached = current <= partial_level
        if reached:
            signal["partial_taken"] = True
            logger.info(
                "%s qismən-TP səviyyəsinə çatdı (%.0f%%) — informativdir, broker icrası yoxdur",
                signal["symbol"], Config.PARTIAL_TP_PERCENT
            )

    def update_one(self, signal):
        try:
            df = BYBIT.fetch_klines(signal["symbol"], "1", 100)
            if len(df) < 5:
                return

            direction = signal["direction"].lower()
            entry = float(signal["entry"])

            original_risk = signal.get("original_risk")
            if not original_risk or original_risk <= 0:
                original_risk = abs(entry - float(signal["stop"]))
                signal["original_risk"] = original_risk
            if original_risk <= 0:
                return

            last_checked = signal.get("last_checked_at")
            if last_checked:
                last_ts = pd.Timestamp(last_checked)
                bars = df[df["timestamp"] > last_ts]
            else:
                bars = df.iloc[-1:]

            if bars.empty:
                return

            target = float(signal["target"])
            stop = float(signal["stop"])

            for _, row in bars.iterrows():
                high = float(row["high"])
                low = float(row["low"])
                current = float(row["close"])

                if direction == "long":
                    if low <= stop:
                        r_mult = (stop - entry) / original_risk
                        result = self._classify(r_mult, self.RESULT_EPSILON)
                        self.tracker.close_signal(signal, result, round(r_mult, 3))
                        return
                    if high >= target:
                        self.tracker.close_signal(signal, "WIN", signal["rr"])
                        return

                    self._check_partial(signal, direction, entry, target, current)

                    r_now = (current - entry) / original_risk
                    if r_now >= Config.BREAKEVEN_AFTER_R and not signal.get("breakeven"):
                        stop = entry
                        signal["stop"] = entry
                        signal["breakeven"] = True
                    if r_now >= Config.TRAILING_AFTER_R:
                        atr = float(Indicators.atr(df, Config.ATR_PERIOD).iloc[-1])
                        new_stop = current - atr * Config.TRAILING_ATR_MULT
                        if new_stop > stop:
                            stop = Precision.price(signal["symbol"], new_stop)
                            signal["stop"] = stop
                            signal["trailing"] = True
                else:
                    if high >= stop:
                        r_mult = (entry - stop) / original_risk
                        result = self._classify(r_mult, self.RESULT_EPSILON)
                        self.tracker.close_signal(signal, result, round(r_mult, 3))
                        return
                    if low <= target:
                        self.tracker.close_signal(signal, "WIN", signal["rr"])
                        return

                    self._check_partial(signal, direction, entry, target, current)

                    r_now = (entry - current) / original_risk
                    if r_now >= Config.BREAKEVEN_AFTER_R and not signal.get("breakeven"):
                        stop = entry
                        signal["stop"] = entry
                        signal["breakeven"] = True
                    if r_now >= Config.TRAILING_AFTER_R:
                        atr = float(Indicators.atr(df, Config.ATR_PERIOD).iloc[-1])
                        new_stop = current + atr * Config.TRAILING_ATR_MULT
                        if new_stop < stop:
                            stop = Precision.price(signal["symbol"], new_stop)
                            signal["stop"] = stop
                            signal["trailing"] = True

            signal["last_checked_at"] = df["timestamp"].iloc[-1].isoformat()
            self.tracker.update_signal(signal)
        except Exception as e:
            logger.warning("Position monitor error %s: %s", signal.get("symbol"), e)

    def monitor(self):
        active = self.tracker.active_signals()
        if not active:
            return
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(self.update_one, signal) for signal in active]
            for future in futures:
                try:
                    future.result()
                except Exception:
                    pass
# ============================================================
# TELEGRAM
# ============================================================

class TelegramBot:

    def __init__(self):
        self.token = Config.BOT_TOKEN
        self.chat_id = Config.CHAT_ID
        self.last_sent = {}
        self.retry_queue = []
        self.retry_lock = threading.RLock()

    def enabled(self):
        return bool(self.token and self.chat_id)

    def send(self, text):
        if not self.enabled():
            logger.info("Telegram disabled")
            return False
        url = "https://api.telegram.org/bot" + self.token + "/sendMessage"
        try:
            response = requests.post(url, json={"chat_id": self.chat_id, "text": text}, timeout=15)
            return response.ok
        except Exception as e:
            logger.warning("Telegram error: %s", e)
            return False

    def _queue_retry(self, text):
        with self.retry_lock:
            self.retry_queue.append({"text": text, "attempts": 0})

    def flush_retry_queue(self):
        if not self.enabled():
            return
        with self.retry_lock:
            remaining = []
            for item in self.retry_queue:
                if item["attempts"] >= 5:
                    logger.warning("Mesaj 5 cəhddən sonra ləğv edildi")
                    continue
                if self.send(item["text"]):
                    continue
                item["attempts"] += 1
                remaining.append(item)
            self.retry_queue = remaining

    def format_signal(self, s):
        direction = s["direction"]
        emoji = "🟢" if direction == "LONG" else "🔴"
        return (
            f"{emoji} SWING AI V9\n\n"
            f"Coin: {s['symbol']}\n"
            f"Direction: {direction}\n\n"
            f"Entry: {s['entry']}\n"
            f"SL: {s['stop']}\n"
            f"TP: {s['target']}\n\n"
            f"RR: 1:{s['rr']}\n"
            f"Score: {s['score']}/100\n"
            f"Volume: {s['volume_ratio']:.2f}x\n"
            f"ATR: {s['atr_pct']:.2f}%\n"
            f"Funding: {s['funding']:.6f}\n"
            f"OI: {s['oi_change']:.2f}%\n"
            f"Spread: {s['spread_pct']:.3f}%\n"
            f"BTC: {s['btc_bias']}\n\n"
            f"Risk: {Config.RISK_PERCENT}%\n"
            f"Qty: {s['quantity']}\n\n"
            f"Status: ACTIVE"
        )

    def send_signals(self, signals):
        sent = []
        for signal in signals:
            if not TRACKER.can_create_signal():
                break
            key = signal["symbol"]
            last = self.last_sent.get(key, 0)
            if time.time() - last < 7200:
                continue
            if not TRACKER.add_signal(signal):
                continue
            text = self.format_signal(signal)
            if self.send(text):
                self.last_sent[key] = time.time()
                sent.append(signal)
            else:
                self._queue_retry(text)
        return sent

    def send_stats(self):
        stats = TRACKER.report()
        text = (
            "📊 SWING AI V9 STATS\n\n"
            f"Trades: {stats['total']}\n"
            f"Wins: {stats['wins']}\n"
            f"Losses: {stats['losses']}\n"
            f"BE: {stats['breakevens']}\n"
            f"Winrate: {stats['winrate']}%\n"
            f"Profit R: {stats['profit_r']}\n"
            f"Profit USDT: {stats['profit_usdt']}\n"
            f"Profit Factor: {stats['profit_factor']}\n"
            f"Expectancy: {stats['expectancy']}\n"
            f"Max Consecutive Losses: {stats['max_consecutive_losses']}\n\n"
            f"Score Groups:\n"
        )
        for key, val in stats.get('score_groups', {}).items():
            text += f"{key}: {val['wins']}/{val['total']} (R: {round(val['pnl_r'], 2)})\n"
        text += "\nDirection Stats:\n"
        for key, val in stats.get('direction_stats', {}).items():
            text += f"{key}: {val['wins']}/{val['total']} (R: {round(val['pnl_r'], 2)})\n"
        self.send(text)

    def send_active(self):
        active = TRACKER.active_signals()
        if not active:
            self.send("Aktiv signal yoxdur.")
            return
        lines = ["📌 AKTİV SİQNALLAR\n"]
        for s in active:
            lines.append(f"{s['symbol']} {s['direction']} | Entry {s['entry']} | SL {s['stop']} | TP {s['target']}")
        self.send("\n".join(lines))


TELEGRAM = None  # aşağıda instansiya olunur


class TelegramCommandWorker:

    def __init__(self):
        self.offset = 0
        self.running = True

    def get_updates(self):
        if not TELEGRAM.enabled():
            return []
        url = "https://api.telegram.org/bot" + Config.BOT_TOKEN + "/getUpdates"
        try:
            r = requests.get(url, params={"offset": self.offset, "timeout": 20}, timeout=25)
            data = r.json()
            if not data.get("ok"):
                return []
            return data.get("result", [])
        except Exception:
            return []

    def handle(self, update):
        try:
            message = update.get("message", {})
            text = message.get("text", "").strip().lower()
            chat_id = str(message.get("chat", {}).get("id", ""))
            if chat_id != str(Config.CHAT_ID):
                return
            if text == "/start":
                TELEGRAM.send("SWING AI V9 Professional aktivdir.")
            elif text == "/stats":
                TELEGRAM.send_stats()
            elif text == "/signals":
                TELEGRAM.send_active()
            elif text == "/analiz":
                TELEGRAM.send("🔎 Professional scan başlayır...")
                signals = SCANNER.scan()
                if not signals:
                    TELEGRAM.send("❌ Hazırda bütün filtrləri keçən setup yoxdur.")
                    return
                for s in signals:
                    TELEGRAM.send(TELEGRAM.format_signal(s))
            elif text == "/help":
                TELEGRAM.send("/start\n/stats\n/signals\n/analiz\n/help")
        except Exception as e:
            logger.warning("Command handle error: %s", e)

    def run(self):
        if not TELEGRAM.enabled():
            return
        while self.running:
            updates = self.get_updates()
            for update in updates:
                self.offset = max(self.offset, update.get("update_id", 0) + 1)
                self.handle(update)
            time.sleep(1)


class RetryWorker:

    def __init__(self, telegram_bot):
        self.telegram = telegram_bot
        self.running = True

    def run(self):
        while self.running:
            try:
                self.telegram.flush_retry_queue()
            except Exception as e:
                logger.warning("Retry worker error: %s", e)
            time.sleep(60)


# ============================================================
# QLOBAL OBYEKTLƏR
# ============================================================

TRACKER = PerformanceTracker()
SCANNER = SignalScanner()
POSITION_MANAGER = PositionManager(TRACKER)
TELEGRAM = TelegramBot()
RETRY_WORKER = RetryWorker(TELEGRAM)


# ============================================================
# FLASK HEALTH SERVER
# ============================================================

app = Flask(__name__)


@app.route("/")
def home():
    return jsonify({"bot": "SWING AI", "version": BOT_VERSION, "status": "running"})


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "time": datetime.now(timezone.utc).isoformat(),
        "active_signals": len(TRACKER.active_signals())
    })


@app.route("/stats")
def stats():
    return jsonify(TRACKER.report())


@app.route("/active")
def active():
    return jsonify(TRACKER.active_signals())


def start_flask():
    app.run(host="0.0.0.0", port=Config.FLASK_PORT, debug=False, use_reloader=False)


# ============================================================
# WORKERS
# ============================================================

def scanner_loop():
    while True:
        try:
            logger.info("Professional market scan started...")
            signals = SCANNER.scan()
            if signals:
                logger.info("Found %s professional setup(s)", len(signals))
                sent = TELEGRAM.send_signals(signals)
                for s in sent:
                    logger.info("SIGNAL: %s %s Score=%s RR=%s", s["symbol"], s["direction"], s["score"], s["rr"])
            else:
                logger.info("No setup passed all professional filters.")
        except Exception as e:
            logger.error("Scanner loop error: %s", e)
        time.sleep(Config.CHECK_INTERVAL)


def monitor_loop():
    while True:
        try:
            POSITION_MANAGER.monitor()
        except Exception as e:
            logger.warning("Monitor loop error: %s", e)
        time.sleep(Config.MONITOR_INTERVAL)


def startup_message():
    logger.info("=" * 60)
    logger.info("SWING AI BOT %s", BOT_VERSION)
    logger.info("=" * 60)
    logger.info("Strategy:")
    logger.info("4H Trend -> 1H Structure -> Pullback -> RSI Reversal")
    logger.info("15M Fresh BOS -> Retest -> Strong Candle")
    logger.info("Volume + ATR + BTC + Funding + OI + Spread")
    logger.info("Premium/Discount + Correlation + RR")
    logger.info("Risk=%s%% | MinScore=%s | RR=1:%s-1:%s",
                Config.RISK_PERCENT, Config.MIN_SCORE, Config.MIN_RR, Config.MAX_RR)
    logger.info("Max Active=%s | Max Daily=%s", Config.MAX_ACTIVE_SIGNALS, Config.MAX_DAILY_SIGNALS)
    logger.info("Telegram=%s", "ON" if TELEGRAM.enabled() else "OFF")
    logger.info("=" * 60)


def main():
    try:
        Config.validate()
    except ValueError as e:
        logger.error("Konfiqurasiya xətası: %s", e)
        return

    startup_message()

    threading.Thread(target=start_flask, daemon=True).start()
    threading.Thread(target=scanner_loop, daemon=True).start()
    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=TelegramCommandWorker().run, daemon=True).start()
    threading.Thread(target=RETRY_WORKER.run, daemon=True).start()

    logger.info("SWING AI BOT %s STARTED", BOT_VERSION)

    while True:
        try:
            time.sleep(60)
        except KeyboardInterrupt:
            logger.info("Bot stopped by user.")
            break
        except Exception:
            time.sleep(5)


if __name__ == "__main__":
    main()
