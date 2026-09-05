"""
PROFESSIONAL SMC AI BOT - V5.5 FINAL
Bütün problemlər düzəldi:
- Bütün liquidity-lər içindən ilk untouched target seçilir
- Yalnız son tamamlanmış session liquidity istifadə olunur
- 100 ballıq score məntiqli bölünüb (entry_conf çıxarıldı)
"""

import asyncio
import json
import logging
import math
import os
import time
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from threading import Thread, RLock, local
from typing import Any, Dict, List, Optional, Tuple
import concurrent.futures
import tempfile
import shutil

import numpy as np
import pandas as pd
import requests
from flask import Flask
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# ============================================================================
# KONFİQURASİYA
# ============================================================================

@dataclass
class Config:
    bot_token: str = field(default_factory=lambda: os.getenv("BOT_TOKEN", ""))
    chat_id: str = field(default_factory=lambda: os.getenv("CHAT_ID", "1121794078"))
    scan_top_n_coins: int = int(os.getenv("SCAN_TOP_N_COINS", "40"))
    check_interval_seconds: int = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))
    monitor_interval_seconds: int = int(os.getenv("MONITOR_INTERVAL_SECONDS", "30"))
    notify_cooldown_seconds: int = int(os.getenv("NOTIFY_COOLDOWN_SECONDS", "7200"))
    leverage: int = int(os.getenv("LEVERAGE", "10"))
    account_balance: float = float(os.getenv("ACCOUNT_BALANCE", "1000"))
    risk_percent: float = float(os.getenv("RISK_PERCENT", "1"))
    commission_percent: float = float(os.getenv("COMMISSION_PERCENT", "0.04"))
    slippage_percent: float = float(os.getenv("SLIPPAGE_PERCENT", "0.02"))
    swing_lookback: int = int(os.getenv("SWING_LOOKBACK", "2"))
    klines_limit: int = int(os.getenv("KLINES_LIMIT", "200"))
    daily_klines_limit: int = int(os.getenv("DAILY_KLINES_LIMIT", "150"))
    entry_klines_limit: int = int(os.getenv("ENTRY_KLINES_LIMIT", "200"))
    atr_period: int = int(os.getenv("ATR_PERIOD", "14"))
    volume_period: int = int(os.getenv("VOLUME_PERIOD", "20"))
    min_rr_ratio: float = float(os.getenv("MIN_RR_RATIO", "2.0"))
    min_signal_score: float = float(os.getenv("MIN_SIGNAL_SCORE", "70"))
    max_event_age_bars: int = int(os.getenv("MAX_EVENT_AGE_BARS", "20"))
    ote_fib_low: float = float(os.getenv("OTE_FIB_LOW", "0.618"))
    ote_fib_high: float = float(os.getenv("OTE_FIB_HIGH", "0.786"))
    flask_port: int = int(os.getenv("PORT", "10000"))
    parallel_workers: int = int(os.getenv("PARALLEL_WORKERS", "6"))
    fallback_coins: List[str] = field(default_factory=lambda: [
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
        "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "LINKUSDT", "SUIUSDT",
        "ARBUSDT", "OPUSDT"
    ])

    require_daily_trend: bool = True
    require_4h_trend: bool = True
    require_triple_alignment: bool = True
    require_15m_confirmation: bool = True

    use_sweep_scoring: bool = True
    use_fvg_scoring: bool = True
    use_poi_scoring: bool = True
    use_displacement_scoring: bool = True
    use_volume_scoring: bool = True
    use_ote_scoring: bool = True
    use_cvd_scoring: bool = True
    use_btc_scoring: bool = True
    use_session_scoring: bool = True
    use_funding_scoring: bool = True
    use_fear_greed_scoring: bool = True
    use_oi_scoring: bool = True
    use_unmitigated_ob_scoring: bool = True

    def __post_init__(self) -> None:
        if not self.bot_token:
            raise ValueError("BOT_TOKEN required")
        if self.leverage <= 0:
            raise ValueError("LEVERAGE > 0 required")
        if self.account_balance <= 0:
            raise ValueError("ACCOUNT_BALANCE > 0 required")
        if self.risk_percent <= 0 or self.risk_percent > 100:
            raise ValueError("RISK_PERCENT 0-100")
        if self.min_rr_ratio < 0:
            raise ValueError("MIN_RR_RATIO >= 0")
        if self.min_signal_score < 0 or self.min_signal_score > 100:
            raise ValueError("MIN_SIGNAL_SCORE 0-100")

# ============================================================================
# LOGGING
# ============================================================================

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)

# ============================================================================
# API CACHE
# ============================================================================

class APICache:
    def __init__(self, default_ttl: int = 300):
        self._cache = {}
        self._default_ttl = default_ttl
        self._lock = RLock()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key in self._cache:
                data, timestamp, ttl = self._cache[key]
                if time.time() - timestamp < ttl:
                    return data
                del self._cache[key]
            return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        with self._lock:
            self._cache[key] = (value, time.time(), ttl or self._default_ttl)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

# ============================================================================
# BYBIT CLIENT
# ============================================================================

class BybitClient:
    BASE_URL = "https://api.bybit.com/v5"
    _thread_local = local()

    def __init__(self, config: Config, timeout: int = 8, retries: int = 3) -> None:
        self.config = config
        self.timeout = timeout
        self.retries = retries
        self._cache = APICache(default_ttl=300)
        self._klines_cache_ttl = {
            "D": 3600, "240": 1200, "60": 300, "15": 120, "1": 30
        }

    @property
    def session(self) -> requests.Session:
        if not hasattr(self._thread_local, 'session'):
            self._thread_local.session = requests.Session()
        return self._thread_local.session

    def _safe_get(self, endpoint: str, params: Optional[Dict] = None, use_cache: bool = True, cache_ttl: Optional[int] = None) -> Optional[requests.Response]:
        cache_key = f"{endpoint}:{json.dumps(params, sort_keys=True)}" if params else endpoint
        if use_cache:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached
        for attempt in range(self.retries + 1):
            try:
                resp = self.session.get(f"{self.BASE_URL}/{endpoint}", params=params, timeout=self.timeout)
                if resp.status_code == 200:
                    try:
                        data = resp.json()
                        if data.get("retCode") != 0:
                            logger.warning(f"Bybit retCode {data.get('retCode')}: {data.get('retMsg')}")
                            if attempt < self.retries:
                                time.sleep(2 ** attempt)
                            continue
                    except:
                        pass
                    if use_cache:
                        self._cache.set(cache_key, resp, cache_ttl)
                    return resp
                elif resp.status_code == 429:
                    wait = 2 ** attempt
                    logger.warning(f"Rate limit, waiting {wait}s")
                    time.sleep(wait)
                else:
                    logger.warning(f"HTTP {resp.status_code}: {endpoint} (attempt {attempt+1})")
                    if attempt < self.retries:
                        time.sleep(1 + attempt * 0.5)
            except Exception as e:
                logger.warning(f"Request error {attempt+1}: {e}")
                if attempt < self.retries:
                    time.sleep(1 + attempt * 0.5)
        return None

    def fetch_tickers(self, category: str = "linear") -> List[Dict]:
        resp = self._safe_get("market/tickers", {"category": category})
        if not resp:
            return []
        try:
            data = resp.json()
            if data.get("retCode") == 0:
                rows = data.get("result", {}).get("list", [])
                return [r for r in rows if r.get("symbol", "").endswith("USDT") and float(r.get("turnover24h", 0) or 0) > 0]
        except Exception:
            pass
        return []

    def fetch_klines(self, symbol: str, interval: str = "60", limit: int = 200) -> Optional[pd.DataFrame]:
        cache_key = f"{symbol}_{interval}_{limit}"
        ttl = self._klines_cache_ttl.get(interval, 120)
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
        resp = self._safe_get("market/kline", params, use_cache=False)
        if not resp:
            return None
        try:
            data = resp.json()
            if data.get("retCode") != 0:
                return None
            rows = data.get("result", {}).get("list", [])
            if len(rows) < 10:
                return None
            df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"])
            df = df.iloc[::-1].reset_index(drop=True)
            for col in ["open", "high", "low", "close", "volume", "turnover"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
            df = df.dropna().reset_index(drop=True)
            if len(df) > 5:
                result = df.iloc[:-1].reset_index(drop=True)
                self._cache.set(cache_key, result, ttl)
                return result
        except Exception as e:
            logger.error(f"{symbol} kline error: {e}")
        return None

    def fetch_1m_high_low_since(self, symbol: str, since_timestamp: int) -> List[Tuple[float, float, float, int]]:
        current_time = int(time.time() * 1000)
        params = {"category": "linear", "symbol": symbol, "interval": "1", "limit": 200}
        resp = self._safe_get("market/kline", params, use_cache=False)
        if not resp:
            return []
        try:
            data = resp.json()
            if data.get("retCode") != 0:
                return []
            rows = data.get("result", {}).get("list", [])
            result = []
            for row in rows:
                ts = int(row[0])
                if ts + 60000 <= current_time and ts > since_timestamp:
                    result.append((float(row[2]), float(row[3]), float(row[4]), ts))
            return sorted(result, key=lambda x: x[3])
        except Exception as e:
            logger.warning(f"1M since error {symbol}: {e}")
        return []

    def fetch_current_price(self, symbol: str) -> Optional[float]:
        resp = self._safe_get("market/tickers", {"category": "linear", "symbol": symbol}, use_cache=False)
        if not resp:
            return None
        try:
            data = resp.json()
            if data.get("retCode") == 0:
                rows = data.get("result", {}).get("list", [])
                if rows:
                    return float(rows[0].get("lastPrice", 0))
        except Exception as e:
            logger.warning(f"Current price error {symbol}: {e}")
        return None

    def fetch_funding_rate(self, symbol: str) -> Optional[float]:
        cache_key = f"funding_{symbol}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached
        resp = self._safe_get("market/funding/history", {"category": "linear", "symbol": symbol, "limit": 1})
        if not resp:
            return None
        try:
            rows = resp.json().get("result", {}).get("list", [])
            if rows:
                result = float(rows[0].get("fundingRate", 0))
                self._cache.set(cache_key, result, 600)
                return result
        except Exception:
            pass
        return None

    def fetch_open_interest_trend(self, symbol: str) -> Optional[float]:
        cache_key = f"oi_{symbol}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached
        resp = self._safe_get("market/open-interest", {
            "category": "linear", "symbol": symbol, "intervalTime": "1h", "limit": 10
        })
        if not resp:
            return None
        try:
            rows = resp.json().get("result", {}).get("list", [])
            if len(rows) >= 2:
                rows = rows[::-1]
                first = float(rows[0].get("openInterest", 0))
                last = float(rows[-1].get("openInterest", 0))
                if first > 0:
                    result = ((last - first) / first) * 100
                    self._cache.set(cache_key, result, 600)
                    return result
        except Exception:
            pass
        return None

    def clear_cache(self) -> None:
        self._cache.clear()
# ============================================================================
# SMC KÖMƏKÇİ ALƏTLƏR
# ============================================================================

class SMCHelpers:
    @staticmethod
    def calculate_ote_structural(df: pd.DataFrame, direction: str, break_idx: int, swing_highs: List, swing_lows: List) -> Optional[Tuple[float, float]]:
        if len(df) < 10 or break_idx < 5:
            return None

        if direction == "bullish":
            valid_lows = [(i, p) for i, p in swing_lows if i < break_idx]
            if not valid_lows:
                return None
            swing_low_idx, swing_low = max(valid_lows, key=lambda x: x[0])

            valid_highs = [(i, p) for i, p in swing_highs if swing_low_idx < i < break_idx]
            if not valid_highs:
                return None
            _, swing_high = max(valid_highs, key=lambda x: (x[1], x[0]))

            diff = swing_high - swing_low
            if diff <= 0:
                return None
            level_618 = swing_high - diff * 0.618
            level_786 = swing_high - diff * 0.786
        else:
            valid_highs = [(i, p) for i, p in swing_highs if i < break_idx]
            if not valid_highs:
                return None
            swing_high_idx, swing_high = max(valid_highs, key=lambda x: (x[1], x[0]))

            valid_lows = [(i, p) for i, p in swing_lows if swing_high_idx < i < break_idx]
            if not valid_lows:
                return None
            _, swing_low = max(valid_lows, key=lambda x: (x[0] - x[1], x[0]))

            diff = swing_high - swing_low
            if diff <= 0:
                return None
            level_618 = swing_low + diff * 0.618
            level_786 = swing_low + diff * 0.786

        zone_low = min(level_618, level_786)
        zone_high = max(level_618, level_786)
        return zone_low, zone_high

    @staticmethod
    def compute_cvd(df: pd.DataFrame) -> pd.Series:
        delta = []
        for i in range(len(df)):
            if float(df["close"].iloc[i]) > float(df["open"].iloc[i]):
                delta.append(float(df["volume"].iloc[i]))
            elif float(df["close"].iloc[i]) < float(df["open"].iloc[i]):
                delta.append(-float(df["volume"].iloc[i]))
            else:
                delta.append(0)
        return pd.Series(delta).cumsum()

    @staticmethod
    def is_level_touched(df: pd.DataFrame, level: float, from_index: int, direction: str) -> bool:
        if from_index >= len(df):
            return True
        sub_df = df.iloc[from_index + 1:]
        if direction == "bullish":
            return any(float(row["low"]) <= level for _, row in sub_df.iterrows())
        else:
            return any(float(row["high"]) >= level for _, row in sub_df.iterrows())

    @staticmethod
    def get_completed_session_levels(df: pd.DataFrame, lookback_days: int = 5, interval: str = "60") -> List[Dict]:
        """
        Yalnız son tamamlanmış sessiyaların high/low səviyyələrini qaytarır.
        Asia: 0-8 UTC (tamamlanma 8:00)
        London: 8-16 UTC (tamamlanma 16:00)
        NY: 16-24 UTC (tamamlanma 24:00)
        """
        if len(df) < 1:
            return []

        # Cari vaxt
        now = datetime.now(timezone.utc)
        current_hour = now.hour

        # Hansı sessiyalar tamamlanıb?
        completed_sessions = []
        if current_hour >= 8:
            completed_sessions.append("Asia")
        if current_hour >= 16:
            completed_sessions.append("London")
        if current_hour >= 24 or current_hour < 16:
            completed_sessions.append("NY")

        # Heç biri tamamlanmayıbsa (0-8 arası) - Asia hələ davam edir
        if not completed_sessions:
            completed_sessions = []  # Boş qaytar

        interval_seconds = {
            "1": 60, "3": 180, "5": 300, "15": 900, "30": 1800,
            "60": 3600, "120": 7200, "240": 14400, "D": 86400, "W": 604800
        }
        secs = interval_seconds.get(interval, 3600)
        bars_per_day = int(86400 / secs) if secs > 0 else 24
        start_idx = max(0, len(df) - lookback_days * bars_per_day)

        # Son 5 gün ərzində tamamlanmış sessiyaların high/low-ları
        sessions = {
            "Asia": {"high": -np.inf, "low": np.inf, "high_idx": -1, "low_idx": -1},
            "London": {"high": -np.inf, "low": np.inf, "high_idx": -1, "low_idx": -1},
            "NY": {"high": -np.inf, "low": np.inf, "high_idx": -1, "low_idx": -1}
        }

        for idx, row in df.iloc[start_idx:].iterrows():
            ts = datetime.fromtimestamp(row["timestamp"] / 1000, tz=timezone.utc)
            hour = ts.hour
            if 0 <= hour < 8:
                sess = "Asia"
            elif 8 <= hour < 16:
                sess = "London"
            else:
                sess = "NY"
            high = float(row["high"])
            low = float(row["low"])
            if high > sessions[sess]["high"]:
                sessions[sess]["high"] = high
                sessions[sess]["high_idx"] = idx
            if low < sessions[sess]["low"]:
                sessions[sess]["low"] = low
                sessions[sess]["low_idx"] = idx

        result = []
        for sess in completed_sessions:
            data = sessions.get(sess, {})
            if data.get("high") != -np.inf:
                result.append({"level": data["high"], "type": f"{sess}_high", "idx": data["high_idx"]})
            if data.get("low") != np.inf:
                result.append({"level": data["low"], "type": f"{sess}_low", "idx": data["low_idx"]})
        return result

    @staticmethod
    def get_session_levels(df: pd.DataFrame, lookback_days: int = 5, interval: str = "60") -> List[float]:
        levels = SMCHelpers.get_completed_session_levels(df, lookback_days, interval)
        return [l["level"] for l in levels]

    @staticmethod
    def detect_latest_fvg(df: pd.DataFrame, direction: str, break_idx: int, lookback_window: int = 50) -> Optional[Dict]:
        start = max(0, break_idx - lookback_window)
        segment = df.iloc[start:break_idx + 1].reset_index(drop=True)
        fvg_list = []
        for i in range(1, len(segment) - 1):
            first = segment.iloc[i - 1]
            third = segment.iloc[i + 1]
            if direction == "bullish" and float(first["high"]) < float(third["low"]):
                low = float(first["high"])
                high = float(third["low"])
                mitigated = False
                for j in range(i + 1, len(segment)):
                    if float(segment["low"].iloc[j]) <= low:
                        mitigated = True
                        break
                if not mitigated:
                    fvg_list.append({
                        "low": low, "high": high, "mid": (low + high) / 2,
                        "direction": direction, "type": "standard",
                        "mitigated": False, "fvg_index": i
                    })
            if direction == "bearish" and float(first["low"]) > float(third["high"]):
                low = float(third["high"])
                high = float(first["low"])
                mitigated = False
                for j in range(i + 1, len(segment)):
                    if float(segment["high"].iloc[j]) >= high:
                        mitigated = True
                        break
                if not mitigated:
                    fvg_list.append({
                        "low": low, "high": high, "mid": (low + high) / 2,
                        "direction": direction, "type": "standard",
                        "mitigated": False, "fvg_index": i
                    })
        if fvg_list:
            return max(fvg_list, key=lambda x: x["fvg_index"])
        return None

    @staticmethod
    def detect_latest_ob(df: pd.DataFrame, direction: str, break_idx: int, lookback: int = 50) -> Optional[Dict]:
        start = max(0, break_idx - lookback)
        if direction == "bullish":
            candidates = [i for i in range(break_idx - 1, start - 1, -1)
                          if float(df["close"].iloc[i]) < float(df["open"].iloc[i])]
        else:
            candidates = [i for i in range(break_idx - 1, start - 1, -1)
                          if float(df["close"].iloc[i]) > float(df["open"].iloc[i])]
        for ob_idx in candidates:
            ob_high = float(df["high"].iloc[ob_idx])
            ob_low = float(df["low"].iloc[ob_idx])
            mitigated = False
            for j in range(ob_idx + 1, len(df)):
                if direction == "bullish" and float(df["low"].iloc[j]) <= ob_low:
                    mitigated = True
                    break
                if direction == "bearish" and float(df["high"].iloc[j]) >= ob_high:
                    mitigated = True
                    break
            if not mitigated:
                return {
                    "index": ob_idx, "high": ob_high, "low": ob_low,
                    "mid": (ob_high + ob_low) / 2, "mitigated": False,
                    "direction": direction, "ob_index": ob_idx
                }
        return None
# ============================================================================
# SMC ANALİZER (HİSSƏ 1)
# ============================================================================

class SMCAnalyzer:
    def __init__(self, config: Config, client: BybitClient) -> None:
        self.config = config
        self.client = client
        self.helpers = SMCHelpers()
        self._btc_bias_cache = None
        self._btc_bias_timestamp = 0
        self._btc_bias_lock = RLock()
        self._fear_greed_cache = None
        self._fear_greed_timestamp = 0
        self._fear_greed_lock = RLock()

    def _get_btc_bias(self) -> Optional[str]:
        now = time.time()
        with self._btc_bias_lock:
            if self._btc_bias_cache is not None and (now - self._btc_bias_timestamp) < 300:
                return self._btc_bias_cache
        try:
            df = self.client.fetch_klines("BTCUSDT", "240", 120)
            if df is None or len(df) < 60:
                with self._btc_bias_lock:
                    return self._btc_bias_cache
            close = df["close"]
            ema20 = self.compute_ema(close, 20).iloc[-1]
            ema50 = self.compute_ema(close, 50).iloc[-1]
            price = close.iloc[-1]
            if price > ema20 and ema20 > ema50:
                result = "bullish"
            elif price < ema20 and ema20 < ema50:
                result = "bearish"
            else:
                result = "neutral"
            with self._btc_bias_lock:
                self._btc_bias_cache = result
                self._btc_bias_timestamp = now
            return result
        except Exception as e:
            logger.warning(f"BTC bias fetch error: {e}")
            with self._btc_bias_lock:
                return self._btc_bias_cache

    @staticmethod
    def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        prev_close = df["close"].shift(1)
        tr1 = df["high"] - df["low"]
        tr2 = (df["high"] - prev_close).abs()
        tr3 = (df["low"] - prev_close).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        return tr.rolling(period, min_periods=1).mean()

    @staticmethod
    def compute_volume_ratio(df: pd.DataFrame, period: int = 20) -> pd.Series:
        avg = df["volume"].rolling(period, min_periods=5).mean()
        return df["volume"] / avg.replace(0, np.nan)

    @staticmethod
    def compute_ema(series: pd.Series, period: int) -> pd.Series:
        return series.ewm(span=period, adjust=False).mean()

    @staticmethod
    def find_swing_points(df: pd.DataFrame, lookback: int = 2) -> Tuple[List[Tuple[int, float]], List[Tuple[int, float]]]:
        highs = df["high"].values
        lows = df["low"].values
        swing_highs, swing_lows = [], []
        for i in range(lookback, len(df) - lookback):
            if highs[i] > np.max(highs[i - lookback:i]) and highs[i] > np.max(highs[i + 1:i + lookback + 1]):
                swing_highs.append((i, float(highs[i])))
            if lows[i] < np.min(lows[i - lookback:i]) and lows[i] < np.min(lows[i + 1:i + lookback + 1]):
                swing_lows.append((i, float(lows[i])))
        return swing_highs, swing_lows

    @staticmethod
    def determine_trend_bias(sh: List, sl: List) -> Optional[str]:
        if len(sh) < 2 or len(sl) < 2:
            return None
        hh = sh[-1][1] > sh[-2][1]
        hl = sl[-1][1] > sl[-2][1]
        lh = sh[-1][1] < sh[-2][1]
        ll = sl[-1][1] < sl[-2][1]
        if hh and hl:
            return "bullish"
        if lh and ll:
            return "bearish"
        return "ranging"

    def compute_structure_events(self, df: pd.DataFrame, sh: List, sl: List) -> List[Dict]:
        events = []
        close = df["close"].values
        trend_bias = None
        pivots = sorted([(i, p, "high") for i, p in sh] + [(i, p, "low") for i, p in sl], key=lambda x: x[0])
        active_high, active_low = None, None
        high_crossed, low_crossed = True, True
        ptr = 0
        for i in range(1, len(df)):
            while ptr < len(pivots) and pivots[ptr][0] == i:
                idx, price, typ = pivots[ptr]
                if typ == "high":
                    active_high = (idx, price)
                    high_crossed = False
                else:
                    active_low = (idx, price)
                    low_crossed = False
                ptr += 1
            if active_high and not high_crossed:
                if close[i - 1] <= active_high[1] and close[i] > active_high[1]:
                    kind = "CHoCH" if trend_bias == "bearish" else "BOS"
                    trend_bias = "bullish"
                    high_crossed = True
                    events.append({"index": i, "bias": "bullish", "kind": kind, "level": active_high[1], "time": df["timestamp"].iloc[i]})
            if active_low and not low_crossed:
                if close[i - 1] >= active_low[1] and close[i] < active_low[1]:
                    kind = "CHoCH" if trend_bias == "bullish" else "BOS"
                    trend_bias = "bearish"
                    low_crossed = True
                    events.append({"index": i, "bias": "bearish", "kind": kind, "level": active_low[1], "time": df["timestamp"].iloc[i]})
        return events

    def detect_liquidity_sweep(self, df: pd.DataFrame, direction: str, break_idx: int, window: int = 20) -> bool:
        try:
            start = max(0, break_idx - window)
            seg = df.iloc[start:break_idx + 1].reset_index(drop=True)
            if len(seg) < 4:
                return False
            if direction == "bullish":
                for i in range(2, len(seg)):
                    prior_low = seg["low"].iloc[:i].min()
                    cur_low = float(seg["low"].iloc[i])
                    cur_close = float(seg["close"].iloc[i])
                    if cur_low < prior_low and cur_close > prior_low:
                        return True
            else:
                for i in range(2, len(seg)):
                    prior_high = seg["high"].iloc[:i].max()
                    cur_high = float(seg["high"].iloc[i])
                    cur_close = float(seg["close"].iloc[i])
                    if cur_high > prior_high and cur_close < prior_high:
                        return True
        except Exception as e:
            logger.warning(f"Sweep detection error: {e}")
        return False

    def find_order_block(self, df: pd.DataFrame, direction: str, break_idx: int, lookback: int = 30) -> Optional[Dict]:
        return self.helpers.detect_latest_ob(df, direction, break_idx, lookback)

    # 🔥 YENİ: Bütün liquidity-lər içindən ilk untouched target seçimi
    def find_untouched_liquidity(self, direction: str, entry: float, df: pd.DataFrame, swing_highs: List, swing_lows: List, session_levels_with_time: List[Dict]) -> Optional[float]:
        try:
            candidates = []

            # Swing high/lows
            if direction == "bullish":
                for idx, level in swing_highs:
                    if level > entry:
                        if not self.helpers.is_level_touched(df, level, idx, "bullish"):
                            candidates.append(("swing", level, idx))
            else:
                for idx, level in swing_lows:
                    if level < entry:
                        if not self.helpers.is_level_touched(df, level, idx, "bearish"):
                            candidates.append(("swing", level, idx))

            # Session levels
            for lvl_data in session_levels_with_time:
                level = lvl_data["level"]
                if direction == "bullish" and level > entry:
                    if not self.helpers.is_level_touched(df, level, lvl_data["idx"], "bullish"):
                        candidates.append(("session", level, lvl_data["idx"]))
                elif direction == "bearish" and level < entry:
                    if not self.helpers.is_level_touched(df, level, lvl_data["idx"], "bearish"):
                        candidates.append(("session", level, lvl_data["idx"]))

            if not candidates:
                return None

            # Qiymətə ən yaxın seç
            candidates.sort(key=lambda x: abs(x[1] - entry))
            return candidates[0][1]

        except Exception as e:
            logger.warning(f"Untouched liquidity search error: {e}")
            return None
# ============================================================================
# SMC ANALİZER (HİSSƏ 2)
# ============================================================================

    def check_ote(self, df: pd.DataFrame, direction: str, break_idx: int, current_price: float, swing_highs: List, swing_lows: List) -> Tuple[bool, Optional[Tuple[float, float]]]:
        try:
            ote = self.helpers.calculate_ote_structural(df, direction, break_idx, swing_highs, swing_lows)
            if not ote:
                return False, None
            low, high = ote
            return low <= current_price <= high, ote
        except Exception:
            return False, None

    def check_cvd_trend(self, df: pd.DataFrame, direction: str) -> Optional[bool]:
        try:
            cvd = self.helpers.compute_cvd(df)
            if len(cvd) < 10:
                return None
            slope = np.polyfit(range(10), cvd.iloc[-10:], 1)[0]
            if direction == "bullish":
                return slope > 0
            else:
                return slope < 0
        except Exception:
            return None

    def get_session_liquidity_targets_with_time(self, df: pd.DataFrame, interval: str = "60") -> List[Dict]:
        return self.helpers.get_completed_session_levels(df, lookback_days=5, interval=interval)

    def get_session_liquidity_targets(self, df: pd.DataFrame, interval: str = "60") -> List[float]:
        return self.helpers.get_session_levels(df, lookback_days=5, interval=interval)

    def detect_displacement(self, df: pd.DataFrame, break_idx: int, atr_series: pd.Series) -> Tuple[bool, float]:
        try:
            if break_idx <= 0 or break_idx >= len(df):
                return False, 0.0
            candle = df.iloc[break_idx]
            body = abs(float(candle["close"]) - float(candle["open"]))
            atr = float(atr_series.iloc[break_idx])
            if atr <= 0:
                return False, 0.0
            ratio = body / atr
            return ratio >= 0.6, ratio
        except Exception:
            return False, 0.0

    @staticmethod
    def price_in_zone(price: float, zone: Optional[Dict], buffer: float = 0.15) -> bool:
        if zone is None:
            return False
        try:
            low = float(zone["low"])
            high = float(zone["high"])
            size = high - low
            if size <= 0:
                return low <= price <= high
            buff = size * buffer
            return (low - buff) <= price <= (high + buff)
        except Exception:
            return False

    def get_poi_status(self, price: float, ob: Optional[Dict], fvg: Optional[Dict]) -> Tuple[bool, bool, bool]:
        in_ob = self.price_in_zone(price, ob)
        in_fvg = self.price_in_zone(price, fvg)
        return in_ob or in_fvg, in_ob, in_fvg

    def get_daily_trend_bias(self, symbol: str) -> Optional[str]:
        try:
            df = self.client.fetch_klines(symbol, "D", self.config.daily_klines_limit)
            if df is None or len(df) < 40:
                return None
            sh, sl = self.find_swing_points(df, self.config.swing_lookback)
            return self.determine_trend_bias(sh, sl)
        except Exception as e:
            logger.warning(f"Daily trend error {symbol}: {e}")
        return None

    def get_4h_trend_bias(self, symbol: str) -> Optional[str]:
        try:
            df = self.client.fetch_klines(symbol, "240", self.config.klines_limit)
            if df is None or len(df) < 40:
                return None
            sh, sl = self.find_swing_points(df, self.config.swing_lookback)
            return self.determine_trend_bias(sh, sl)
        except Exception as e:
            logger.warning(f"4H trend error {symbol}: {e}")
        return None

    def get_btc_market_bias(self) -> Optional[str]:
        return self._get_btc_bias()

    @staticmethod
    def get_trading_session() -> Tuple[bool, str]:
        try:
            hour = datetime.now(timezone.utc).hour
            london = 7 <= hour < 16
            new_york = 13 <= hour < 22
            active = london or new_york
            sessions = []
            if london:
                sessions.append("London")
            if new_york:
                sessions.append("New York")
            return active, " + ".join(sessions) if sessions else "Asia"
        except Exception:
            return False, "Unknown"

    def get_fear_greed(self) -> Tuple[Optional[int], str]:
        now = time.time()
        with self._fear_greed_lock:
            if self._fear_greed_cache is not None and (now - self._fear_greed_timestamp) < 3600:
                return self._fear_greed_cache
        try:
            resp = requests.get("https://api.alternative.me/fng/", params={"limit": 1}, timeout=6)
            if resp:
                data = resp.json().get("data", [])
                if data:
                    result = (int(data[0].get("value", 50)), data[0].get("value_classification", "Unknown"))
                    with self._fear_greed_lock:
                        self._fear_greed_cache = result
                        self._fear_greed_timestamp = now
                    return result
        except Exception as e:
            logger.warning(f"Fear&Greed error: {e}")
        with self._fear_greed_lock:
            self._fear_greed_cache = (None, "Unknown")
            self._fear_greed_timestamp = now
        return self._fear_greed_cache

    def fundamental_data(self, symbol: str) -> Dict:
        data = {}
        try:
            data["btc_bias"] = self.get_btc_market_bias()
            fg, fg_class = self.get_fear_greed()
            data["fear_greed"] = fg
            data["fear_greed_class"] = fg_class
            data["funding"] = self.client.fetch_funding_rate(symbol)
            data["oi_change"] = self.client.fetch_open_interest_trend(symbol)
        except Exception as e:
            logger.warning(f"Fundamental data error {symbol}: {e}")
        return data

    def check_15m_confirmation(self, symbol: str, direction: str, poi_zone: Optional[Dict]) -> Tuple[bool, Dict]:
        try:
            df = self.client.fetch_klines(symbol, "15", self.config.entry_klines_limit)
            if df is None or len(df) < 50:
                return False, {"reason": "15M data yoxdur"}

            sh, sl = self.find_swing_points(df, self.config.swing_lookback)
            events = self.compute_structure_events(df, sh, sl)
            if not events:
                return False, {"reason": "15M structure yoxdur"}

            last = events[-1]
            break_idx = last["index"]
            age = len(df) - 1 - break_idx
            atr = self.compute_atr(df, self.config.atr_period)
            vol_series = self.compute_volume_ratio(df, self.config.volume_period)
            _, disp_ratio = self.detect_displacement(df, break_idx, atr)
            vol_ratio = float(vol_series.iloc[-1]) if not pd.isna(vol_series.iloc[-1]) else 0.0

            confirmed = (last["bias"] == direction and age <= 12 and disp_ratio >= 0.5 and vol_ratio >= 0.7)
            poi_retest = False

            if confirmed and poi_zone:
                poi_low = poi_zone["low"]
                poi_high = poi_zone["high"]
                poi_mid = (poi_low + poi_high) / 2

                post_bos = df.iloc[break_idx + 1:break_idx + 13]
                if len(post_bos) < 3:
                    confirmed = False
                else:
                    retest_found = False
                    trigger_found = False

                    for _, row in post_bos.iterrows():
                        candle_low = float(row["low"])
                        candle_high = float(row["high"])
                        candle_close = float(row["close"])
                        candle_open = float(row["open"])
                        body = abs(candle_close - candle_open)

                        if candle_low <= poi_high and candle_high >= poi_low:
                            retest_found = True
                            poi_retest = True
                            if direction == "bullish":
                                wick_bottom = min(candle_open, candle_close) - candle_low
                                if wick_bottom >= body * 0.5 and candle_close > poi_mid and candle_close > candle_open:
                                    trigger_found = True
                            else:
                                wick_top = candle_high - max(candle_open, candle_close)
                                if wick_top >= body * 0.5 and candle_close < poi_mid and candle_close < candle_open:
                                    trigger_found = True
                            if trigger_found:
                                break

                    if not retest_found:
                        confirmed = False
                    if retest_found and not trigger_found:
                        confirmed = False

            return confirmed, {
                "event": last["kind"],
                "direction_ok": last["bias"] == direction,
                "age": age,
                "fresh": age <= 12,
                "displacement": round(disp_ratio, 2),
                "volume_ratio": round(vol_ratio, 2),
                "trigger": confirmed,
                "poi_retest": poi_retest
            }
        except Exception as e:
            logger.warning(f"15M confirmation error {symbol}: {e}")
        return False, {"reason": "Exception"}

    def calculate_trade_levels(self, direction: str, df: pd.DataFrame, entry: float, ob: Optional[Dict], liquidity_target: Optional[float], atr_value: float, session_targets_with_time: List[Dict], sh: List, sl: List) -> Optional[Dict]:
        """
        🔥 YENİ: liquidity_target artıq untouched funksiyasından gəlir
        """
        try:
            if ob is None or liquidity_target is None:
                return None

            if direction == "bullish":
                if sl and sl[-1][1] < entry:
                    sl_price = sl[-1][1] * 0.999
                else:
                    sl_price = ob["low"] - atr_value * 0.5

                rr_target = (liquidity_target - entry) / (entry - sl_price) if (entry - sl_price) > 0 else 0
                if rr_target >= self.config.min_rr_ratio:
                    tp = liquidity_target
                else:
                    return None

            else:
                if sh and sh[-1][1] > entry:
                    sl_price = sh[-1][1] * 1.001
                else:
                    sl_price = ob["high"] + atr_value * 0.5

                rr_target = (entry - liquidity_target) / (sl_price - entry) if (sl_price - entry) > 0 else 0
                if rr_target >= self.config.min_rr_ratio:
                    tp = liquidity_target
                else:
                    return None

            if direction == "bullish" and (sl_price >= entry or tp <= entry):
                return None
            if direction == "bearish" and (sl_price <= entry or tp >= entry):
                return None

            commission = self.config.commission_percent / 100
            slippage = self.config.slippage_percent / 100

            if direction == "bullish":
                entry_adj = entry * (1 + commission + slippage)
                sl_adj = sl_price * (1 - commission)
                tp_adj = tp * (1 - commission - slippage)
            else:
                entry_adj = entry * (1 - commission - slippage)
                sl_adj = sl_price * (1 + commission)
                tp_adj = tp * (1 + commission + slippage)

            risk = abs(entry_adj - sl_adj)
            reward = abs(tp_adj - entry_adj)
            if risk <= 0:
                return None
            return {"entry": entry, "entry_adjusted": entry_adj, "sl": sl_price, "sl_adjusted": sl_adj, "tp": tp, "tp_adjusted": tp_adj, "risk": risk, "reward": reward, "rr_ratio": reward / risk}
        except Exception as e:
            logger.warning(f"Trade levels error: {e}")
        return None
# ============================================================================
# SMC ANALİZER (HİSSƏ 3 - QALAN METODLAR)
# ============================================================================

    def calculate_position_size(self, entry: float, sl: float, direction: str) -> Dict:
        try:
            risk_amount = self.config.account_balance * self.config.risk_percent / 100
            stop_distance = abs(entry - sl)
            if stop_distance <= 0:
                return {"risk_amount": risk_amount, "position_size": 0, "notional_value": 0, "margin_required": 0, "liquidation_price": 0}
            pos_size = risk_amount / stop_distance
            notional = pos_size * entry
            margin_required = notional / self.config.leverage if self.config.leverage > 0 else notional
            if margin_required > self.config.account_balance:
                max_notional = self.config.account_balance * self.config.leverage
                pos_size = max_notional / entry
                margin_required = max_notional / self.config.leverage
                notional = max_notional
            if direction == "bullish":
                liq_price = entry - (entry / self.config.leverage)
            else:
                liq_price = entry + (entry / self.config.leverage)
            return {
                "risk_amount": risk_amount,
                "position_size": pos_size,
                "notional_value": notional,
                "margin_required": margin_required,
                "liquidation_price": liq_price
            }
        except Exception as e:
            logger.warning(f"Position size error: {e}")
        return {"risk_amount": 0, "position_size": 0, "notional_value": 0, "margin_required": 0, "liquidation_price": 0}

    def analyze_1h_smc(self, symbol: str) -> Tuple[Optional[Dict], Optional[str]]:
        try:
            df = self.client.fetch_klines(symbol, "60", self.config.klines_limit)
            if df is None or len(df) < 70:
                return None, "1H data alınmadı"
            sh, sl = self.find_swing_points(df, self.config.swing_lookback)
            if len(sh) < 2 or len(sl) < 2:
                return None, "Kifayət qədər swing yoxdur"
            events = self.compute_structure_events(df, sh, sl)
            if not events:
                return None, "BOS/CHoCH yoxdur"
            last = events[-1]
            break_idx = last["index"]
            if len(df) - 1 - break_idx > self.config.max_event_age_bars:
                return None, "Structure event köhnədir"
            atr = self.compute_atr(df, self.config.atr_period)
            vol_series = self.compute_volume_ratio(df, self.config.volume_period)
            disp_ok, disp_ratio = self.detect_displacement(df, break_idx, atr)
            vol_ratio = float(vol_series.iloc[break_idx]) if not pd.isna(vol_series.iloc[break_idx]) else 0.0
            sweep = self.detect_liquidity_sweep(df, last["bias"], break_idx)
            fvg = self.helpers.detect_latest_fvg(df, last["bias"], break_idx)
            ob = self.helpers.detect_latest_ob(df, last["bias"], break_idx)
            current_price = self.client.fetch_current_price(symbol)
            if current_price is None:
                current_price = float(df["close"].iloc[-1])
            poi_ok, in_ob, in_fvg = self.get_poi_status(current_price, ob, fvg)

            # Session target-lar (yalnız tamamlanmış)
            session_targets_with_time = self.get_session_liquidity_targets_with_time(df, interval="60")
            session_targets = [l["level"] for l in session_targets_with_time]

            # 🔥 YENİ: untouched liquidity seçimi
            target = self.find_untouched_liquidity(
                last["bias"], current_price, df, sh, sl, session_targets_with_time
            )

            ote_ok, ote_zone = self.check_ote(df, last["bias"], break_idx, current_price, sh, sl)
            cvd_ok = self.check_cvd_trend(df, last["bias"])
            poi_zone = ob if ob else fvg
            now_ms = int(time.time() * 1000)
            last_1m_ts = (now_ms // 60000) * 60000 - 60000
            return {
                "df": df,
                "direction": last["bias"],
                "event_kind": last["kind"],
                "break_idx": break_idx,
                "event_time": last["time"],
                "atr_value": float(atr.iloc[-1]),
                "volume_ratio": vol_ratio,
                "displacement_ok": disp_ok,
                "displacement_ratio": disp_ratio,
                "sweep": sweep,
                "fvg": fvg,
                "ob": ob,
                "current_price": current_price,
                "poi_ok": poi_ok,
                "in_ob": in_ob,
                "in_fvg": in_fvg,
                "target": target,
                "ote_ok": ote_ok,
                "ote_zone": ote_zone,
                "cvd_ok": cvd_ok,
                "session_targets": session_targets,
                "session_targets_with_time": session_targets_with_time,
                "swing_highs": sh,
                "swing_lows": sl,
                "poi_zone": poi_zone,
                "last_1m_ts": last_1m_ts
            }, None
        except Exception as e:
            error_msg = str(e)
            logger.warning(f"1H SMC error {symbol}: {error_msg}")
            return None, f"Exception: {error_msg}"

    def analyze_smc_pro(self, symbol: str, session_active: bool, session_name: str) -> Dict:
        conditions = {}
        scores = {}
        total_score = 0
        fund_data = {}

        try:
            daily = self.get_daily_trend_bias(symbol)
            daily_ok = daily in ("bullish", "bearish")
            conditions["Daily trend"] = daily_ok
            if self.config.require_daily_trend and not daily_ok:
                return {"symbol": symbol, "passed": False, "conditions": conditions, "score": 0, "reason": "Daily trend yoxdur"}

            h4 = self.get_4h_trend_bias(symbol)
            h4_ok = h4 in ("bullish", "bearish")
            conditions["4H trend"] = h4_ok
            if self.config.require_4h_trend and not h4_ok:
                return {"symbol": symbol, "passed": False, "conditions": conditions, "score": 0, "reason": "4H trend yoxdur"}

            smc, err = self.analyze_1h_smc(symbol)
            if smc is None:
                return {"symbol": symbol, "passed": False, "error": err, "conditions": conditions, "score": 0, "reason": err}

            direction = smc["direction"]
            triple_ok = direction == daily and direction == h4
            conditions["Triple alignment"] = triple_ok
            if self.config.require_triple_alignment and not triple_ok:
                return {"symbol": symbol, "passed": False, "conditions": conditions, "score": 0, "reason": "Triple alignment yoxdur"}

            entry_conf, entry_data = self.check_15m_confirmation(symbol, direction, smc.get("poi_zone"))
            conditions["15M confirmation"] = entry_conf
            if self.config.require_15m_confirmation and not entry_conf:
                return {"symbol": symbol, "passed": False, "conditions": conditions, "score": 0, "reason": "15M confirmation yoxdur"}

            target = smc["target"]
            if target is None:
                return {"symbol": symbol, "passed": False, "conditions": conditions, "score": 0, "reason": "Liquidity target yoxdur"}

            entry = smc["current_price"]
            levels = self.calculate_trade_levels(
                direction, smc["df"], entry, smc["ob"], target,
                smc["atr_value"], smc["session_targets_with_time"],
                smc["swing_highs"], smc["swing_lows"]
            )
            if levels is None:
                return {"symbol": symbol, "passed": False, "conditions": conditions, "score": 0, "reason": "Liquidity target RR keçmədi"}

            rr = levels["rr_ratio"]
            rr_ok = rr >= self.config.min_rr_ratio
            conditions[f"RR >= 1:{self.config.min_rr_ratio}"] = rr_ok
            if not rr_ok:
                return {"symbol": symbol, "passed": False, "conditions": conditions, "score": 0, "reason": f"RR {rr} çox aşağıdır"}

            fund_data = self.fundamental_data(symbol)

            # 🔥 YENİ SCORE: 100-lük, entry_conf çıxarıldı
            # OB(11) + Sweep(10) + FVG(7) + POI(8) + Disp(8) + Vol(6) + OTE(10) + CVD(7) + BTC(8) + Sess(6) + FG(5) + Funding(5) + OI(3) + Event(6) = 100
            if self.config.use_unmitigated_ob_scoring:
                ob = smc["ob"]
                scores["unmitigated_ob"] = 11 if (ob is not None and not ob["mitigated"]) else 0
                conditions["Unmitigated OB"] = ob is not None and not ob["mitigated"]

            if self.config.use_sweep_scoring:
                scores["sweep"] = 10 if smc["sweep"] else 0
                conditions["Sweep"] = smc["sweep"]

            if self.config.use_fvg_scoring:
                fvg = smc["fvg"]
                if fvg is not None:
                    fvg_ok = not fvg.get("mitigated", False)
                    scores["fvg"] = 7 if fvg_ok else 3
                    conditions["FVG"] = fvg_ok
                else:
                    scores["fvg"] = 0
                    conditions["FVG"] = False

            if self.config.use_poi_scoring:
                poi_ok = smc["poi_ok"]
                scores["poi"] = 8 if poi_ok else 0
                conditions["POI"] = poi_ok

            if self.config.use_displacement_scoring:
                disp_ratio = smc["displacement_ratio"]
                if disp_ratio >= 1.5:
                    scores["displacement"] = 8
                elif disp_ratio >= 0.8:
                    scores["displacement"] = 5
                elif disp_ratio >= 0.5:
                    scores["displacement"] = 3
                else:
                    scores["displacement"] = 0
                conditions["Displacement"] = disp_ratio

            if self.config.use_volume_scoring:
                vol_ratio = smc["volume_ratio"]
                if vol_ratio >= 1.5:
                    scores["volume"] = 6
                elif vol_ratio >= 1.0:
                    scores["volume"] = 4
                elif vol_ratio >= 0.7:
                    scores["volume"] = 2
                else:
                    scores["volume"] = 0
                conditions["Volume"] = vol_ratio

            if self.config.use_ote_scoring:
                ote_ok = smc["ote_ok"]
                scores["ote"] = 10 if ote_ok else 0
                conditions["OTE"] = ote_ok

            if self.config.use_cvd_scoring:
                cvd_ok = smc["cvd_ok"] if smc["cvd_ok"] is not None else False
                scores["cvd"] = 7 if cvd_ok else 0
                conditions["CVD (Delta Proxy)"] = cvd_ok

            if self.config.use_btc_scoring:
                btc_bias = fund_data.get("btc_bias")
                if btc_bias == direction:
                    scores["btc"] = 8
                elif btc_bias == "neutral":
                    scores["btc"] = 3
                else:
                    scores["btc"] = 0
                conditions["BTC"] = btc_bias

            if self.config.use_session_scoring:
                scores["session"] = 6 if session_active else 0
                conditions["Session"] = session_active

            if self.config.use_fear_greed_scoring:
                fg = fund_data.get("fear_greed")
                if fg is not None:
                    if direction == "bullish" and fg < 80:
                        scores["fear_greed"] = 5
                    elif direction == "bearish" and fg > 20:
                        scores["fear_greed"] = 5
                    else:
                        scores["fear_greed"] = 0
                else:
                    scores["fear_greed"] = 0

            if self.config.use_funding_scoring:
                funding = fund_data.get("funding")
                if funding is not None:
                    if direction == "bullish" and funding < 0:
                        scores["funding"] = 5
                    elif direction == "bearish" and funding > 0:
                        scores["funding"] = 5
                    else:
                        scores["funding"] = 0
                else:
                    scores["funding"] = 0

            if self.config.use_oi_scoring:
                oi = fund_data.get("oi_change")
                if oi is not None:
                    scores["oi"] = 3 if oi > 0 else 0
                else:
                    scores["oi"] = 0

            event_kind = smc["event_kind"]
            scores["event"] = 6 if event_kind == "CHoCH" else 4

            max_possible = 100  # Sabit
            raw_score = sum(scores.values())
            total_score = raw_score
            score_ok = total_score >= self.config.min_signal_score
            conditions["Total Score"] = round(total_score, 1)
            conditions["Min score threshold"] = score_ok

            if not score_ok:
                return {"symbol": symbol, "passed": False, "conditions": conditions, "score": round(total_score, 1), "reason": f"Score {round(total_score,1)} aşağıdır"}

            pos = self.calculate_position_size(levels["entry_adjusted"], levels["sl_adjusted"], direction)
            if pos["margin_required"] > self.config.account_balance:
                return {"symbol": symbol, "passed": False, "conditions": conditions, "score": round(total_score, 1), "reason": "Margin balansdan böyükdür"}

            poi_zone = smc.get("poi_zone")
            if poi_zone:
                poi_price = int(poi_zone.get("mid", smc["current_price"]) * 1000)
            else:
                poi_price = int(smc["current_price"] * 1000)
            signal_id = generate_signal_id(symbol, direction, smc["event_time"], poi_price)

            return {
                "symbol": symbol,
                "passed": True,
                "conditions": conditions,
                "scores": scores,
                "score": round(total_score, 1),
                "bias": "🟢 LONG" if direction == "bullish" else "🔴 SHORT",
                "direction": direction,
                "event_kind": event_kind,
                "entry": round(levels["entry"], 8),
                "entry_adj": round(levels["entry_adjusted"], 8),
                "sl": round(levels["sl"], 8),
                "sl_adj": round(levels["sl_adjusted"], 8),
                "tp": round(levels["tp"], 8),
                "tp_adj": round(levels["tp_adjusted"], 8),
                "rr_ratio": round(rr, 2),
                "leverage": self.config.leverage,
                "daily_bias": daily,
                "h4_bias": h4,
                "session": session_name,
                "risk_amount": round(pos["risk_amount"], 2),
                "position_size": round(pos["position_size"], 6),
                "notional_value": round(pos["notional_value"], 2),
                "margin_required": round(pos["margin_required"], 2),
                "liquidation_price": round(pos.get("liquidation_price", 0), 8),
                "sweep": smc["sweep"],
                "poi_ok": smc["poi_ok"],
                "fvg_ok": smc["fvg"] is not None and not smc["fvg"].get("mitigated", False),
                "fvg_type": "latest" if smc["fvg"] else None,
                "ote_ok": smc["ote_ok"],
                "cvd_ok": smc["cvd_ok"] if smc["cvd_ok"] is not None else False,
                "displacement_ratio": round(smc["displacement_ratio"], 2),
                "volume_ratio": round(smc["volume_ratio"], 2),
                "entry_confirmation": entry_data,
                "fundamental": fund_data,
                "signal_id": signal_id,
                "last_1m_ts": smc["last_1m_ts"]
            }
        except Exception as e:
            logger.error(f"analyze_smc_pro error {symbol}: {e}")
            return {"symbol": symbol, "passed": False, "error": str(e), "conditions": conditions, "score": 0, "reason": f"Exception: {e}"}

# ============================================================================
# YARDIMÇI FUNKSİYALAR
# ============================================================================

def generate_signal_id(symbol: str, direction: str, event_time: int, poi_price: int) -> str:
    return f"{symbol}_{direction}_{event_time}_{poi_price}"
# ============================================================================
# PERFORMANCE TRACKER
# ============================================================================

class PerformanceTracker:
    HISTORY_FILE = "signals_history.json"
    ACTIVE_SIGNALS_FILE = "active_signals.json"
    EQUITY_FILE = "equity_curve.json"
    NOTIFICATION_FILE = "notifications.json"
    RETRY_FILE = "retry_pending.json"
    _file_lock = RLock()

    @classmethod
    def _atomic_write(cls, filepath: str, data: Any) -> None:
        fd, temp_path = tempfile.mkstemp(dir=os.path.dirname(filepath) or '.', suffix='.tmp')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            shutil.move(temp_path, filepath)
        except Exception:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    @classmethod
    def _load_active(cls) -> List[Dict]:
        try:
            with open(cls.ACTIVE_SIGNALS_FILE, "r") as f:
                return json.load(f)
        except:
            return []

    @classmethod
    def _load_history(cls) -> List[Dict]:
        try:
            with open(cls.HISTORY_FILE, "r") as f:
                return json.load(f)
        except:
            return []

    @classmethod
    def save_signal(cls, signal: Dict) -> bool:
        with cls._file_lock:
            try:
                active = cls._load_active()
                for sig in active:
                    if sig["signal_id"] == signal["signal_id"]:
                        return False
                history = cls._load_history()
                for sig in history:
                    if sig["signal_id"] == signal["signal_id"]:
                        return False
                active.append({
                    "signal_id": signal["signal_id"],
                    "symbol": signal["symbol"],
                    "direction": signal["direction"],
                    "entry": signal["entry"],
                    "sl": signal["sl"],
                    "tp": signal["tp"],
                    "rr_ratio": signal["rr_ratio"],
                    "entry_time": datetime.now(timezone.utc).isoformat(),
                    "status": "active",
                    "leverage": signal.get("leverage", 10),
                    "position_size": signal.get("position_size", 0),
                    "notional_value": signal.get("notional_value", 0),
                    "margin_required": signal.get("margin_required", 0),
                    "notified": False,
                    "last_1m_ts": signal.get("last_1m_ts", int(time.time() * 1000) - 60000)
                })
                cls._atomic_write(cls.ACTIVE_SIGNALS_FILE, active)
                return True
            except Exception as e:
                logger.warning(f"Could not save active signal: {e}")
                return False

    @classmethod
    def update_signal(cls, signal_id: str, result: str, exit_price: float, exit_time: str, config: Config) -> None:
        with cls._file_lock:
            try:
                active = cls._load_active()
                history = cls._load_history()
                for sig in active:
                    if sig["signal_id"] == signal_id:
                        sig["status"] = "closed"
                        sig["result"] = result
                        sig["exit_price"] = exit_price
                        sig["exit_time"] = exit_time

                        entry = sig["entry"]
                        size = sig.get("position_size", 0)
                        lev = sig.get("leverage", 1)
                        taker_fee = config.commission_percent / 100
                        slippage = config.slippage_percent / 100

                        if size <= 0:
                            sig["pnl_usd"] = 0
                            sig["pnl_percent"] = 0
                        else:
                            if sig["direction"] == "bullish":
                                fill_entry = entry * (1 + slippage)
                                fill_exit = exit_price * (1 - slippage)
                                raw_pnl = (fill_exit - fill_entry) * size
                                fee = (fill_entry * size * taker_fee) + (fill_exit * size * taker_fee)
                                pnl_usd = raw_pnl - fee
                                pnl_pct = (pnl_usd / (fill_entry * size / lev)) * 100 if entry > 0 else 0
                            else:
                                fill_entry = entry * (1 - slippage)
                                fill_exit = exit_price * (1 + slippage)
                                raw_pnl = (fill_entry - fill_exit) * size
                                fee = (fill_entry * size * taker_fee) + (fill_exit * size * taker_fee)
                                pnl_usd = raw_pnl - fee
                                pnl_pct = (pnl_usd / (fill_entry * size / lev)) * 100 if entry > 0 else 0

                            sig["pnl_usd"] = round(pnl_usd, 2)
                            sig["pnl_percent"] = round(pnl_pct, 2)

                        history.append(sig)
                        break

                active = [s for s in active if s["status"] != "closed"]
                cls._atomic_write(cls.ACTIVE_SIGNALS_FILE, active)
                cls._atomic_write(cls.HISTORY_FILE, history)
                cls._update_equity_curve(config)
            except Exception as e:
                logger.warning(f"Could not update signal: {e}")

    @classmethod
    def _update_equity_curve(cls, config: Config) -> None:
        try:
            history = cls._load_history()
            initial_equity = config.account_balance
            equity = initial_equity
            curve = []
            for trade in history:
                equity += trade.get("pnl_usd", 0)
                curve.append({
                    "time": trade.get("exit_time", datetime.now(timezone.utc).isoformat()),
                    "equity": round(equity, 2)
                })
            cls._atomic_write(cls.EQUITY_FILE, curve)
        except Exception as e:
            logger.warning(f"Equity curve update error: {e}")

    @classmethod
    def process_candle(cls, symbol: str, high: float, low: float, close: float, ts: int, config: Config) -> None:
        with cls._file_lock:
            try:
                active = cls._load_active()
                history = cls._load_history()
                now = datetime.now(timezone.utc).isoformat()
                updated = False

                for sig in active:
                    if sig["symbol"] != symbol or sig["status"] != "active":
                        continue
                    if ts <= sig.get("last_1m_ts", 0):
                        continue

                    entry = sig.get("entry", 0)
                    sl = sig.get("sl", 0)
                    tp = sig.get("tp", 0)
                    result = None
                    exit_price = None

                    if sig["direction"] == "bullish":
                        if high >= tp and low <= sl:
                            result = "LOSS"
                            exit_price = sl
                        elif high >= tp:
                            result = "WIN"
                            exit_price = tp
                        elif low <= sl:
                            result = "LOSS"
                            exit_price = sl
                    else:
                        if high >= sl and low <= tp:
                            result = "LOSS"
                            exit_price = sl
                        elif low <= tp:
                            result = "WIN"
                            exit_price = tp
                        elif high >= sl:
                            result = "LOSS"
                            exit_price = sl

                    if result:
                        sig["status"] = "closed"
                        sig["result"] = result
                        sig["exit_price"] = exit_price
                        sig["exit_time"] = now

                        size = sig.get("position_size", 0)
                        lev = sig.get("leverage", 1)
                        taker_fee = config.commission_percent / 100
                        slippage = config.slippage_percent / 100

                        if size <= 0:
                            sig["pnl_usd"] = 0
                            sig["pnl_percent"] = 0
                        else:
                            if sig["direction"] == "bullish":
                                fill_entry = entry * (1 + slippage)
                                fill_exit = exit_price * (1 - slippage)
                                raw_pnl = (fill_exit - fill_entry) * size
                                fee = (fill_entry * size * taker_fee) + (fill_exit * size * taker_fee)
                                pnl_usd = raw_pnl - fee
                                pnl_pct = (pnl_usd / (fill_entry * size / lev)) * 100 if entry > 0 else 0
                            else:
                                fill_entry = entry * (1 - slippage)
                                fill_exit = exit_price * (1 + slippage)
                                raw_pnl = (fill_entry - fill_exit) * size
                                fee = (fill_entry * size * taker_fee) + (fill_exit * size * taker_fee)
                                pnl_usd = raw_pnl - fee
                                pnl_pct = (pnl_usd / (fill_entry * size / lev)) * 100 if entry > 0 else 0

                            sig["pnl_usd"] = round(pnl_usd, 2)
                            sig["pnl_percent"] = round(pnl_pct, 2)

                        history.append(sig)
                        updated = True
                    else:
                        sig["last_1m_ts"] = ts
                        updated = True

                if updated:
                    active = [s for s in active if s["status"] != "closed"]
                    cls._atomic_write(cls.ACTIVE_SIGNALS_FILE, active)
                    cls._atomic_write(cls.HISTORY_FILE, history)
                    cls._update_equity_curve(config)
            except Exception as e:
                logger.warning(f"Process candle error {symbol}: {e}")

    @classmethod
    def calculate_stats(cls, config: Config) -> Dict:
        with cls._file_lock:
            try:
                history = cls._load_history()
                if not history:
                    return {"total": 0, "win_rate": 0, "avg_rr": 0, "sharpe": 0, "drawdown": 0, "active": 0, "total_pnl": 0}

                total = len(history)
                wins = sum(1 for d in history if d.get("result") == "WIN")
                win_rate = (wins / total * 100) if total > 0 else 0

                returns = []
                for d in history:
                    pnl = d.get("pnl_usd", 0)
                    margin = d.get("margin_required", 1)
                    if margin > 0:
                        returns.append(pnl / margin)

                if len(returns) > 1:
                    mean_return = np.mean(returns)
                    std_return = np.std(returns) if len(returns) > 1 else 1
                    sharpe = (mean_return / std_return) * math.sqrt(len(returns)) if std_return > 0 else 0
                else:
                    sharpe = 0

                try:
                    with open(cls.EQUITY_FILE, "r") as f:
                        curve = json.load(f)
                    if len(curve) > 1:
                        equities = [c["equity"] for c in curve]
                        max_equity = 0
                        max_dd_pct = 0
                        for eq in equities:
                            if eq > max_equity:
                                max_equity = eq
                            dd_pct = (max_equity - eq) / max_equity * 100 if max_equity > 0 else 0
                            if dd_pct > max_dd_pct:
                                max_dd_pct = dd_pct
                        drawdown = round(max_dd_pct, 2)
                    else:
                        drawdown = 0
                except:
                    drawdown = 0

                rr_list = [d.get("rr_ratio", 0) for d in history if d.get("rr_ratio", 0) > 0]
                avg_rr = np.mean(rr_list) if rr_list else 0
                total_pnl = sum(d.get("pnl_usd", 0) for d in history)

                return {
                    "total": total,
                    "win_rate": round(win_rate, 2),
                    "avg_rr": round(avg_rr, 2),
                    "sharpe": round(sharpe, 2),
                    "drawdown": drawdown,
                    "active": len([s for s in cls._load_active() if s.get("status") == "active"]),
                    "total_pnl": round(total_pnl, 2)
                }
            except Exception as e:
                logger.warning(f"Stats calculation error: {e}")
                return {"total": 0, "win_rate": 0, "avg_rr": 0, "sharpe": 0, "drawdown": 0, "active": 0, "total_pnl": 0}

    @classmethod
    def get_last_notified(cls, signal_id: str) -> float:
        try:
            with open(cls.NOTIFICATION_FILE, "r") as f:
                data = json.load(f)
                return data.get(signal_id, 0)
        except:
            return 0

    @classmethod
    def set_last_notified(cls, signal_id: str, timestamp: float) -> None:
        with cls._file_lock:
            try:
                with open(cls.NOTIFICATION_FILE, "r") as f:
                    data = json.load(f)
            except:
                data = {}
            data[signal_id] = timestamp
            cls._atomic_write(cls.NOTIFICATION_FILE, data)

    @classmethod
    def get_pending_retry(cls) -> Dict[str, int]:
        try:
            with open(cls.RETRY_FILE, "r") as f:
                return json.load(f)
        except:
            return {}

    @classmethod
    def set_pending_retry(cls, retry_dict: Dict[str, int]) -> None:
        with cls._file_lock:
            cls._atomic_write(cls.RETRY_FILE, retry_dict)

# ============================================================================
# SKANER + MONITOR LOOP
# ============================================================================

class SignalScanner:
    def __init__(self, config: Config, client: BybitClient, analyzer: SMCAnalyzer) -> None:
        self.config = config
        self.client = client
        self.analyzer = analyzer

    def fetch_top_liquid_coins(self, limit: int) -> List[str]:
        try:
            tickers = self.client.fetch_tickers()
            if not tickers:
                return self.config.fallback_coins
            tickers.sort(key=lambda x: float(x.get("turnover24h", 0) or 0), reverse=True)
            symbols = [r["symbol"] for r in tickers[:limit]]
            if symbols:
                logger.info(f"{len(symbols)} likvid coin taranır")
                return symbols
            return self.config.fallback_coins
        except Exception as e:
            logger.warning(f"Coin fetch error: {e}")
            return self.config.fallback_coins

    def scan(self) -> Tuple[Optional[Dict], List[Dict]]:
        try:
            session_active, session_name = self.analyzer.get_trading_session()
            coins = self.fetch_top_liquid_coins(self.config.scan_top_n_coins)

            results = []
            workers = min(self.config.parallel_workers, len(coins), 6)
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_symbol = {
                    executor.submit(self.analyzer.analyze_smc_pro, symbol, session_active, session_name): symbol
                    for symbol in coins
                }
                for future in concurrent.futures.as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    try:
                        res = future.result(timeout=30)
                        results.append(res)
                    except Exception as e:
                        logger.error(f"{symbol} error: {e}")
                        results.append({"symbol": symbol, "passed": False, "error": str(e), "conditions": {}, "score": 0, "reason": str(e)})

            valid = [r for r in results if r.get("passed")]
            valid.sort(key=lambda x: (x.get("score", 0), x.get("rr_ratio", 0)), reverse=True)
            return valid[0] if valid else None, results
        except Exception as e:
            logger.error(f"Scan error: {e}")
            return None, []

    async def monitor_loop(self, application) -> None:
        await asyncio.sleep(5)
        logger.info("1M TP/SL monitor loop started (30s interval)")

        while True:
            try:
                active = PerformanceTracker.load_active()
                if not active:
                    await asyncio.sleep(self.config.monitor_interval_seconds)
                    continue

                symbol_ts = {}
                for sig in active:
                    if sig.get("status") != "active":
                        continue
                    sym = sig.get("symbol")
                    ts = sig.get("last_1m_ts", 0)
                    if sym not in symbol_ts or ts < symbol_ts[sym]:
                        symbol_ts[sym] = ts

                for symbol, since_ts in symbol_ts.items():
                    try:
                        candles = self.client.fetch_1m_high_low_since(symbol, since_ts)
                        if candles:
                            for high, low, close, ts in candles:
                                PerformanceTracker.process_candle(symbol, high, low, close, ts, self.config)
                    except Exception as e:
                        logger.warning(f"Monitor error {symbol}: {e}")

            except Exception as e:
                logger.error(f"Monitor loop error: {e}")

            await asyncio.sleep(self.config.monitor_interval_seconds)
# ============================================================================
# TELEGRAM BOT
# ============================================================================

class TelegramBot:
    def __init__(self, config: Config, scanner: SignalScanner) -> None:
        self.config = config
        self.scanner = scanner

    def format_signal(self, res: Dict, title: str = "🚨 *PRO SMC SIGNAL* 🚨") -> str:
        f = res.get("fundamental", {})
        ec = res.get("entry_confirmation", {})
        strength = "🔥 VERY STRONG" if res["score"] >= 90 else "🟢 STRONG" if res["score"] >= 75 else "🔵 GOOD"
        scores = res.get("scores", {})
        trigger = ec.get("trigger", False)
        trigger_text = "✅" if trigger else "❌"
        liq = res.get('liquidation_price', 0)
        fvg_info = "Latest" if res.get('fvg_ok') else "❌"
        return f"""{title}

{strength} | Score: `{res['score']}/100`

🪙 *{res['symbol']}* | {res['bias']} ({res['event_kind']})

📈 Daily: `{res['daily_bias']}` | 4H: `{res['h4_bias']}`
🕒 Session: `{res['session']}`
⚖️ RR: `1:{res['rr_ratio']}`

📍 ENTRY: `{res['entry']}` (Adj: `{res['entry_adj']}`)
🛑 SL: `{res['sl']}` (Adj: `{res['sl_adj']}`)
🎯 TP: `{res['tp']}` (Adj: `{res['tp_adj']}`)
💀 Est.Liq: `{liq}`

⚙️ Lev: `{res['leverage']}x` | 💰 Margin: `${res['margin_required']}`
📦 Size: `{res['position_size']}` | Risk: `${res['risk_amount']}`

🔎 *SMC Filters*
💧 Sweep: `{res['sweep']}` | 📦 POI: `{res['poi_ok']}`
📊 FVG: `{res['fvg_ok']}` ({fvg_info})
🎯 OTE: `{res['ote_ok']}` | 📈 CVD: `{res['cvd_ok']}`
⚡ Disp: `{res['displacement_ratio']}` | Vol: `{res['volume_ratio']}`

📊 *Score Breakdown*
OB: `{scores.get('unmitigated_ob',0)}` | Sweep: `{scores.get('sweep',0)}`
FVG: `{scores.get('fvg',0)}` | POI: `{scores.get('poi',0)}`
Disp: `{scores.get('displacement',0)}` | Vol: `{scores.get('volume',0)}`
OTE: `{scores.get('ote',0)}` | CVD: `{scores.get('cvd',0)}`
BTC: `{scores.get('btc',0)}` | Session: `{scores.get('session',0)}`
FG: `{scores.get('fear_greed',0)}` | Funding: `{scores.get('funding',0)}`
OI: `{scores.get('oi',0)}` | Event: `{scores.get('event',0)}`

⏱ 15M: `{ec.get('event')}` (Age: `{ec.get('age')}`) | Trigger: {trigger_text}
🌍 BTC: `{f.get('btc_bias')}` | FG: `{f.get('fear_greed')}`
🆔 `{res['signal_id']}`

*Score 0-100 arası SMC uyğunluq göstəricisidir, qazanma ehtimalı deyil."""

    def format_diagnostics(self, all_results: List[Dict], max_detail: int = 10) -> str:
        total = len(all_results)
        reasons = {}
        for r in all_results:
            reason = r.get("reason")
            if reason:
                reasons[reason] = reasons.get(reason, 0) + 1
            else:
                failed = [name for name, ok in r.get("conditions", {}).items() if not ok]
                if failed:
                    reason = failed[0]
                    reasons[reason] = reasons.get(reason, 0) + 1
        lines = [f"📋 *Xülasə:* `{total}` coin.", "", "*Ən çox dayanan:*"]
        for reason, count in sorted(reasons.items(), key=lambda x: x[1], reverse=True)[:10]:
            lines.append(f"• {reason}: `{count}`")
        lines.append("")
        for r in all_results[:max_detail]:
            sym = r.get("symbol", "?")
            sc = r.get("score", 0)
            if r.get("passed"):
                lines.append(f"• `{sym}` ✅ PASS | Score `{sc}`")
            else:
                reason = r.get("reason") or "Unknown"
                lines.append(f"• `{sym}` ❌ {reason} | Score `{sc}`")
        return "\n".join(lines)

    async def send_signal(self, application, result: Dict) -> None:
        try:
            if not PerformanceTracker.save_signal(result):
                logger.info(f"Signal {result['signal_id']} already exists, skipping send")
                return

            last_notified = PerformanceTracker.get_last_notified(result["signal_id"])
            if time.time() - last_notified < self.config.notify_cooldown_seconds:
                logger.info(f"Cooldown active for {result['signal_id']}")
                return

            try:
                await application.bot.send_message(
                    chat_id=self.config.chat_id,
                    text=self.format_signal(result),
                    parse_mode="Markdown"
                )
                logger.info(f"Signal sent: {result['signal_id']}")
                PerformanceTracker.set_last_notified(result["signal_id"], time.time())
                with PerformanceTracker._file_lock:
                    active = PerformanceTracker._load_active()
                    for sig in active:
                        if sig["signal_id"] == result["signal_id"]:
                            sig["notified"] = True
                            break
                    PerformanceTracker._atomic_write(PerformanceTracker.ACTIVE_SIGNALS_FILE, active)

            except Exception as e:
                logger.error(f"Telegram send error: {e}")
                retry = PerformanceTracker.get_pending_retry()
                retry[result["signal_id"]] = 3
                PerformanceTracker.set_pending_retry(retry)

        except Exception as e:
            logger.error(f"Send error: {e}")

    async def retry_pending_signals(self, application) -> None:
        while True:
            try:
                retry = PerformanceTracker.get_pending_retry()
                if retry:
                    to_remove = []
                    for sid, retries in retry.items():
                        active = PerformanceTracker.load_active()
                        signal = next((s for s in active if s["signal_id"] == sid), None)
                        if signal and not signal.get("notified", False):
                            try:
                                await application.bot.send_message(
                                    chat_id=self.config.chat_id,
                                    text=self.format_signal(signal),
                                    parse_mode="Markdown"
                                )
                                logger.info(f"Retry signal sent: {sid}")
                                PerformanceTracker.set_last_notified(sid, time.time())
                                with PerformanceTracker._file_lock:
                                    active2 = PerformanceTracker._load_active()
                                    for s in active2:
                                        if s["signal_id"] == sid:
                                            s["notified"] = True
                                            break
                                    PerformanceTracker._atomic_write(PerformanceTracker.ACTIVE_SIGNALS_FILE, active2)
                                to_remove.append(sid)
                            except Exception as e:
                                logger.warning(f"Retry failed for {sid}: {e}")
                                if retries > 1:
                                    retry[sid] = retries - 1
                                else:
                                    to_remove.append(sid)
                        else:
                            to_remove.append(sid)

                    for sid in to_remove:
                        if sid in retry:
                            del retry[sid]
                    PerformanceTracker.set_pending_retry(retry)

            except Exception as e:
                logger.error(f"Retry loop error: {e}")

            await asyncio.sleep(60)

    async def auto_scan_loop(self, application) -> None:
        await asyncio.sleep(15)
        while True:
            try:
                logger.info("Auto scan started...")
                result, all_res = await asyncio.to_thread(self.scanner.scan)
                if result:
                    await self.send_signal(application, result)
                else:
                    logger.info("No valid setup")
            except Exception as e:
                logger.error(f"Auto scan error: {e}")
            await asyncio.sleep(self.config.check_interval_seconds)

    @staticmethod
    async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        msg = """📊 *Professional SMC AI Bot V5.5 FINAL*

✅ Hard Filter:
• Daily + 4H + 1H Triple Alignment
• 15M Confirmation (BOS → POI retest → wick rejection)
• RR >= 1:2
• Liquidity target untouched + RR (REJECT əgər keçmirsə)

✅ Soft Filter (100-lük çəkilərlə):
• OB(11), Sweep(10), FVG(7), POI(8), Displacement(8), Volume(6)
• OTE(10), CVD(7), BTC(8), Session(6), Fear&Greed(5)
• Funding(5), OI(3), Event(6)

✅ Real Performance:
• Atomic 1M candle processor (lock altında)
• Hər signal üçün persistent last_1m_ts
• Dəqiq 100-lük scoring (double-counting yoxdur)
• Real POI intersection + wick rejection
• Yalnız son tamamlanmış session liquidity
• Bütün liquidity-lər içindən ilk untouched target
• Persistent retry + notification (restart-safe)
• Atomic JSON + RLock
• P&L, Equity, Drawdown, Sharpe

Komandalar:
/analiz - Canlı analiz
/stats - Performans statistikası"""
        await update.message.reply_text(msg, parse_mode="Markdown")

    @staticmethod
    async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        config = context.bot_data.get("config")
        stats = PerformanceTracker.calculate_stats(config)
        msg = f"""📈 *PERFORMANCE STATISTICS*

📊 Total Signals: `{stats['total']}`
🏆 Win Rate: `{stats['win_rate']}%`
⚖️ Avg RR: `1:{stats['avg_rr']}`
📉 Sharpe Ratio: `{stats['sharpe']}`
📉 Max Drawdown: `{stats['drawdown']}%`
🟢 Active Signals: `{stats['active']}`
💰 Total P&L: `${stats['total_pnl']}`

*Statistikalar real TP/SL bağlanmalarına əsaslanır.*"""
        await update.message.reply_text(msg, parse_mode="Markdown")

    async def analiz_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await update.message.reply_text(f"🔍 Skan edilir...", parse_mode="Markdown")
        result, all_res = await asyncio.to_thread(self.scanner.scan)
        if result:
            await update.message.reply_text(self.format_signal(result, "📊 *LIVE SMC ANALYSIS*"), parse_mode="Markdown")
        else:
            msg = "❌ Heç bir setup keçmədi.\n\n" + self.format_diagnostics(all_res)
            await update.message.reply_text(msg, parse_mode="Markdown")

# ============================================================================
# FLASK KEEP-ALIVE
# ============================================================================

class KeepAliveServer:
    def __init__(self, port: int) -> None:
        self.port = port
        self.app = Flask(__name__)
        @self.app.route("/")
        def home():
            return "PRO SMC AI BOT V5.5 FINAL RUNNING"
    def run(self) -> None:
        Thread(target=self._run, daemon=True).start()
    def _run(self) -> None:
        self.app.run(host="0.0.0.0", port=self.port)

# ============================================================================
# MAIN
# ============================================================================

config = None

async def post_init(application):
    bot: TelegramBot = application.bot_data["bot_instance"]
    scanner: SignalScanner = application.bot_data["scanner"]

    application.create_task(bot.auto_scan_loop(application))
    application.create_task(scanner.monitor_loop(application))
    application.create_task(bot.retry_pending_signals(application))

    logger.info("All loops started (scan, monitor, retry)")

def main() -> None:
    global config
    try:
        config = Config()
    except ValueError as e:
        logger.error(e)
        return

    KeepAliveServer(config.flask_port).run()

    client = BybitClient(config)
    analyzer = SMCAnalyzer(config, client)
    scanner = SignalScanner(config, client, analyzer)
    bot = TelegramBot(config, scanner)

    app = ApplicationBuilder().token(config.bot_token).post_init(post_init).build()
    app.bot_data["bot_instance"] = bot
    app.bot_data["scanner"] = scanner
    app.bot_data["config"] = config

    app.add_handler(CommandHandler("start", TelegramBot.start_command))
    app.add_handler(CommandHandler("analiz", bot.analiz_command))
    app.add_handler(CommandHandler("stats", TelegramBot.stats_command))

    logger.info("PROFESSIONAL SMC BOT V5.5 FINAL STARTED!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
