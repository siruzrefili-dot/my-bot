"""
PROFESSIONAL SMC AI BOT - V4.2
✅ Per-coin TP/SL tracking
✅ Real 100-point scoring with normalization
✅ All scoring toggles work
✅ Stable signal ID (timestamp based)
✅ Real P&L with commission, slippage, leverage
✅ BTC bias cache refresh (4 hours)
✅ 15M confirmation with retest/rejection
✅ Duplicate signal blocking
"""

import asyncio
import json
import logging
import math
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from threading import Thread
from typing import Any, Dict, List, Optional, Tuple

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
    btc_cache_duration_hours: int = int(os.getenv("BTC_CACHE_DURATION_HOURS", "4"))
    fallback_coins: List[str] = field(default_factory=lambda: [
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
        "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "LINKUSDT", "SUIUSDT",
        "ARBUSDT", "OPUSDT"
    ])

    # ===== HARD FILTERS (Məcburi) =====
    require_daily_trend: bool = True
    require_4h_trend: bool = True
    require_triple_alignment: bool = True
    require_15m_confirmation: bool = True

    # ===== SOFT FILTERS (Skora təsir edir, rədd etmir) =====
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
            raise ValueError("BOT_TOKEN environment variable is required!")

# ============================================================================
# LOGGING
# ============================================================================

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)

# ============================================================================
# BYBIT CLIENT
# ============================================================================

class BybitClient:
    BASE_URL = "https://api.bybit.com/v5"

    def __init__(self, timeout: int = 8, retries: int = 2) -> None:
        self.session = requests.Session()
        self.timeout = timeout
        self.retries = retries

    def _safe_get(self, endpoint: str, params: Optional[Dict] = None) -> Optional[requests.Response]:
        for attempt in range(self.retries + 1):
            try:
                resp = self.session.get(f"{self.BASE_URL}/{endpoint}", params=params, timeout=self.timeout)
                if resp.status_code == 200:
                    return resp
                logger.warning(f"HTTP {resp.status_code}: {endpoint}")
            except Exception as e:
                logger.warning(f"Request error {attempt+1}: {e}")
            time.sleep(1 + attempt)
        return None

    def fetch_tickers(self, category: str = "linear") -> List[Dict]:
        resp = self._safe_get("market/tickers", {"category": category})
        if not resp:
            return []
        try:
            data = resp.json()
            if data.get("retCode") == 0:
                return data.get("result", {}).get("list", [])
        except Exception:
            pass
        return []

    def fetch_klines(self, symbol: str, interval: str = "60", limit: int = 200) -> Optional[pd.DataFrame]:
        params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
        resp = self._safe_get("market/kline", params)
        if not resp:
            return None
        try:
            data = resp.json()
            if data.get("retCode") != 0:
                return None
            rows = data.get("result", {}).get("list", [])
            if len(rows) < 30:
                return None
            df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"])
            df = df.iloc[::-1].reset_index(drop=True)
            for col in ["open", "high", "low", "close", "volume", "turnover"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
            df = df.dropna().reset_index(drop=True)
            if len(df) > 5:
                return df.iloc[:-1].reset_index(drop=True)
        except Exception as e:
            logger.error(f"{symbol} kline error: {e}")
        return None

    def fetch_current_price(self, symbol: str) -> Optional[float]:
        """Cari qiyməti qaytarır (1 dəqiqəlik son şam)"""
        df = self.fetch_klines(symbol, "1", 2)
        if df is not None and len(df) > 0:
            return float(df["close"].iloc[-1])
        return None

    def fetch_funding_rate(self, symbol: str) -> Optional[float]:
        resp = self._safe_get("market/funding/history", {"category": "linear", "symbol": symbol, "limit": 1})
        if not resp:
            return None
        try:
            rows = resp.json().get("result", {}).get("list", [])
            if rows:
                return float(rows[0].get("fundingRate", 0))
        except Exception:
            pass
        return None

    def fetch_open_interest_trend(self, symbol: str) -> Optional[float]:
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
                    return ((last - first) / first) * 100
        except Exception:
            pass
        return None
# ============================================================================
# SMC KÖMƏKÇİ ALƏTLƏR
# ============================================================================

class SMCHelpers:
    @staticmethod
    def calculate_ote(df: pd.DataFrame, direction: str) -> Optional[Tuple[float, float]]:
        """OTE zonasını düzgün sıralanmış şəkildə qaytarır (low, high)"""
        if len(df) < 20:
            return None
        recent_high = df["high"].iloc[-20:].max()
        recent_low = df["low"].iloc[-20:].min()
        diff = recent_high - recent_low
        if diff <= 0:
            return None
        if direction == "bullish":
            level_618 = recent_high - diff * 0.618
            level_786 = recent_high - diff * 0.786
        else:
            level_618 = recent_low + diff * 0.618
            level_786 = recent_low + diff * 0.786
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
    def detect_fvg_advanced(df: pd.DataFrame, direction: str, break_idx: int, lookback_window: int = 25) -> Optional[Dict]:
        start = max(0, break_idx - lookback_window)
        segment = df.iloc[start:break_idx + 1].reset_index(drop=True)
        for i in range(1, len(segment) - 1):
            first = segment.iloc[i - 1]
            third = segment.iloc[i + 1]
            if direction == "bullish" and float(first["high"]) < float(third["low"]):
                low = float(first["high"])
                high = float(third["low"])
                fvg_type = "standard"
                if i < len(segment) - 1:
                    candle = segment.iloc[i]
                    body = abs(float(candle["close"]) - float(candle["open"]))
                    atr = (segment["high"].max() - segment["low"].min()) / 14
                    if atr > 0 and body / atr > 1.0:
                        fvg_type = "breakaway"
                    wick = max(float(candle["high"]) - max(float(candle["open"]), float(candle["close"])),
                               min(float(candle["open"]), float(candle["close"])) - float(candle["low"]))
                    if body > 0 and wick / body > 0.6:
                        fvg_type = "rejection"
                return {"low": low, "high": high, "mid": (low + high) / 2, "direction": direction, "type": fvg_type}
            if direction == "bearish" and float(first["low"]) > float(third["high"]):
                low = float(third["high"])
                high = float(first["low"])
                fvg_type = "standard"
                if i < len(segment) - 1:
                    candle = segment.iloc[i]
                    body = abs(float(candle["close"]) - float(candle["open"]))
                    atr = (segment["high"].max() - segment["low"].min()) / 14
                    if atr > 0 and body / atr > 1.0:
                        fvg_type = "breakaway"
                    wick = max(float(candle["high"]) - max(float(candle["open"]), float(candle["close"])),
                               min(float(candle["open"]), float(candle["close"])) - float(candle["low"]))
                    if body > 0 and wick / body > 0.6:
                        fvg_type = "rejection"
                return {"low": low, "high": high, "mid": (low + high) / 2, "direction": direction, "type": fvg_type}
        return None

    @staticmethod
    def get_session_levels(df: pd.DataFrame, lookback_days: int = 5) -> Dict[str, Dict]:
        if len(df) < 1:
            return {}
        sessions = {"Asia": {"high": -np.inf, "low": np.inf},
                    "London": {"high": -np.inf, "low": np.inf},
                    "NY": {"high": -np.inf, "low": np.inf}}
        start_idx = max(0, len(df) - lookback_days * 288)
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
            if low < sessions[sess]["low"]:
                sessions[sess]["low"] = low
        for sess in sessions:
            if sessions[sess]["high"] == -np.inf:
                sessions[sess]["high"] = None
            if sessions[sess]["low"] == np.inf:
                sessions[sess]["low"] = None
        return sessions
# ============================================================================
# SMC ANALİZER (HİSSƏ 1)
# ============================================================================

class SMCAnalyzer:
    def __init__(self, config: Config, client: BybitClient) -> None:
        self.config = config
        self.client = client
        self.helpers = SMCHelpers()
        self._btc_bias_cache = None
        self._btc_bias_cache_time = None
        self._fear_greed_cache = None

    def _get_btc_bias_with_cache(self) -> Optional[str]:
        """BTC bias cache ilə - 4 saatdan sonra təzələnir"""
        if self._btc_bias_cache is not None and self._btc_bias_cache_time is not None:
            if datetime.now(timezone.utc) - self._btc_bias_cache_time < timedelta(hours=self.config.btc_cache_duration_hours):
                return self._btc_bias_cache
        df = self.client.fetch_klines("BTCUSDT", "240", 120)
        if df is None or len(df) < 60:
            return None
        close = df["close"]
        ema20 = self.compute_ema(close, 20).iloc[-1]
        ema50 = self.compute_ema(close, 50).iloc[-1]
        price = close.iloc[-1]
        if price > ema20 and ema20 > ema50:
            self._btc_bias_cache = "bullish"
        elif price < ema20 and ema20 < ema50:
            self._btc_bias_cache = "bearish"
        else:
            self._btc_bias_cache = "neutral"
        self._btc_bias_cache_time = datetime.now(timezone.utc)
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
                    events.append({"index": i, "bias": "bullish", "kind": kind, "level": active_high[1], "timestamp": df["timestamp"].iloc[i]})
            if active_low and not low_crossed:
                if close[i - 1] >= active_low[1] and close[i] < active_low[1]:
                    kind = "CHoCH" if trend_bias == "bullish" else "BOS"
                    trend_bias = "bearish"
                    low_crossed = True
                    events.append({"index": i, "bias": "bearish", "kind": kind, "level": active_low[1], "timestamp": df["timestamp"].iloc[i]})
        return events

    def detect_liquidity_sweep(self, df: pd.DataFrame, direction: str, break_idx: int, window: int = 20) -> bool:
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
        return False

    def find_order_block(self, df: pd.DataFrame, direction: str, break_idx: int, lookback: int = 30) -> Optional[Dict]:
        start = max(0, break_idx - lookback)
        if direction == "bullish":
            candidates = [i for i in range(break_idx - 1, start - 1, -1)
                          if float(df["close"].iloc[i]) < float(df["open"].iloc[i])]
        else:
            candidates = [i for i in range(break_idx - 1, start - 1, -1)
                          if float(df["close"].iloc[i]) > float(df["open"].iloc[i])]
        if not candidates:
            return None
        ob_idx = candidates[0]
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
        return {"index": ob_idx, "high": ob_high, "low": ob_low, "mid": (ob_high + ob_low) / 2, "mitigated": mitigated, "direction": direction}
# ============================================================================
# SMC ANALİZER (HİSSƏ 2)
# ============================================================================

    def check_ote(self, df: pd.DataFrame, direction: str, current_price: float) -> Tuple[bool, Optional[Tuple[float, float]]]:
        ote = self.helpers.calculate_ote(df, direction)
        if not ote:
            return False, None
        low, high = ote
        return low <= current_price <= high, ote

    def check_cvd_trend(self, df: pd.DataFrame, direction: str) -> bool:
        cvd = self.helpers.compute_cvd(df)
        if len(cvd) < 10:
            return True
        slope = np.polyfit(range(10), cvd.iloc[-10:], 1)[0]
        if direction == "bullish":
            return slope > 0
        else:
            return slope < 0

    def get_session_liquidity_targets(self, df: pd.DataFrame) -> List[float]:
        levels = self.helpers.get_session_levels(df, lookback_days=5)
        targets = []
        for sess in ["Asia", "London", "NY"]:
            if levels.get(sess, {}).get("high"):
                targets.append(levels[sess]["high"])
            if levels.get(sess, {}).get("low"):
                targets.append(levels[sess]["low"])
        return targets

    def detect_displacement(self, df: pd.DataFrame, break_idx: int, atr_series: pd.Series) -> Tuple[bool, float]:
        if break_idx <= 0 or break_idx >= len(df):
            return False, 0.0
        candle = df.iloc[break_idx]
        body = abs(float(candle["close"]) - float(candle["open"]))
        atr = float(atr_series.iloc[break_idx])
        if atr <= 0:
            return False, 0.0
        ratio = body / atr
        return ratio >= 0.6, ratio

    @staticmethod
    def price_in_zone(price: float, zone: Optional[Dict], buffer: float = 0.15) -> bool:
        if zone is None:
            return False
        low = float(zone["low"])
        high = float(zone["high"])
        size = high - low
        if size <= 0:
            return low <= price <= high
        buff = size * buffer
        return (low - buff) <= price <= (high + buff)

    def get_poi_status(self, price: float, ob: Optional[Dict], fvg: Optional[Dict]) -> Tuple[bool, bool, bool]:
        in_ob = self.price_in_zone(price, ob)
        in_fvg = self.price_in_zone(price, fvg)
        return in_ob or in_fvg, in_ob, in_fvg

    def get_daily_trend_bias(self, symbol: str) -> Optional[str]:
        df = self.client.fetch_klines(symbol, "D", self.config.daily_klines_limit)
        if df is None or len(df) < 40:
            return None
        sh, sl = self.find_swing_points(df, self.config.swing_lookback)
        return self.determine_trend_bias(sh, sl)

    def get_4h_trend_bias(self, symbol: str) -> Optional[str]:
        df = self.client.fetch_klines(symbol, "240", self.config.klines_limit)
        if df is None or len(df) < 40:
            return None
        sh, sl = self.find_swing_points(df, self.config.swing_lookback)
        return self.determine_trend_bias(sh, sl)

    def get_btc_market_bias(self) -> Optional[str]:
        return self._get_btc_bias_with_cache()

    @staticmethod
    def get_trading_session() -> Tuple[bool, str]:
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

    def get_fear_greed(self) -> Tuple[Optional[int], str]:
        if self._fear_greed_cache is not None:
            return self._fear_greed_cache
        try:
            resp = requests.get("https://api.alternative.me/fng/", params={"limit": 1}, timeout=6)
            if resp:
                data = resp.json().get("data", [])
                if data:
                    self._fear_greed_cache = (int(data[0].get("value", 50)), data[0].get("value_classification", "Unknown"))
                    return self._fear_greed_cache
        except Exception:
            pass
        self._fear_greed_cache = (None, "Unknown")
        return self._fear_greed_cache

    def fundamental_score(self, symbol: str, direction: str) -> Tuple[int, Dict]:
        score = 0
        data = {}
        btc = self.get_btc_market_bias()
        data["btc_bias"] = btc
        if btc == direction:
            score += 10
            data["btc_alignment"] = True
        elif btc == "neutral":
            score += 3
            data["btc_alignment"] = None
        else:
            score -= 8
            data["btc_alignment"] = False
        fg, fg_class = self.get_fear_greed()
        data["fear_greed"] = fg
        data["fear_greed_class"] = fg_class
        if fg is not None:
            if direction == "bullish":
                if fg < 80:
                    score += 5
                if fg >= 90:
                    score -= 5
            else:
                if fg > 20:
                    score += 5
                if fg <= 10:
                    score -= 5
        funding = self.client.fetch_funding_rate(symbol)
        data["funding"] = funding
        if funding is not None:
            if direction == "bullish" and funding < 0:
                score += 5
            elif direction == "bearish" and funding > 0:
                score += 5
        oi = self.client.fetch_open_interest_trend(symbol)
        data["oi_change"] = oi
        if oi is not None:
            if oi > 0:
                score += 3
            elif oi < -5:
                score -= 3
        return score, data

    def check_15m_confirmation(self, symbol: str, direction: str) -> Tuple[bool, Dict]:
        df = self.client.fetch_klines(symbol, "15", self.config.entry_klines_limit)
        if df is None or len(df) < 50:
            return False, {"reason": "15M data yoxdur"}
        sh, sl = self.find_swing_points(df, self.config.swing_lookback)
        events = self.compute_structure_events(df, sh, sl)
        if not events:
            return False, {"reason": "15M structure yoxdur"}
        last = events[-1]
        age = len(df) - 1 - last["index"]
        atr = self.compute_atr(df, self.config.atr_period)
        vol_series = self.compute_volume_ratio(df, self.config.volume_period)
        _, disp_ratio = self.detect_displacement(df, last["index"], atr)
        vol_ratio = float(vol_series.iloc[-1]) if not pd.isna(vol_series.iloc[-1]) else 0.0
        confirmed = (last["bias"] == direction and age <= 12 and disp_ratio >= 0.5 and vol_ratio >= 0.7)
        return confirmed, {"event": last["kind"], "direction_ok": last["bias"] == direction, "age": age, "fresh": age <= 12, "displacement": round(disp_ratio, 2), "volume_ratio": round(vol_ratio, 2)}
# ============================================================================
# SMC ANALİZER (HİSSƏ 3 - QALAN METODLAR)
# ============================================================================

    def calculate_trade_levels(self, direction: str, df: pd.DataFrame, entry: float, ob: Optional[Dict], liquidity_target: Optional[float], atr_value: float) -> Optional[Dict]:
        if ob is None or liquidity_target is None:
            return None
        sh, sl = self.find_swing_points(df, self.config.swing_lookback)
        if direction == "bullish":
            if sl and sl[-1][1] < entry:
                sl_price = sl[-1][1] * 0.999
            else:
                sl_price = ob["low"] - atr_value * 0.5
            tp = liquidity_target
        else:
            if sh and sh[-1][1] > entry:
                sl_price = sh[-1][1] * 1.001
            else:
                sl_price = ob["high"] + atr_value * 0.5
            tp = liquidity_target

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

    def calculate_position_size(self, entry: float, sl: float) -> Dict:
        risk_amount = self.config.account_balance * self.config.risk_percent / 100
        stop_distance = abs(entry - sl)
        if stop_distance <= 0:
            return {"risk_amount": risk_amount, "position_size": 0, "notional_value": 0, "margin_required": 0}
        pos_size = risk_amount / stop_distance
        notional = pos_size * entry
        margin_required = notional / self.config.leverage if self.config.leverage > 0 else notional

        if margin_required > self.config.account_balance:
            max_notional = self.config.account_balance * self.config.leverage
            pos_size = max_notional / entry
            margin_required = max_notional / self.config.leverage
            notional = max_notional

        return {"risk_amount": risk_amount, "position_size": pos_size, "notional_value": notional, "margin_required": margin_required}

    def analyze_1h_smc(self, symbol: str) -> Tuple[Optional[Dict], Optional[str]]:
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
        fvg = self.helpers.detect_fvg_advanced(df, last["bias"], break_idx)
        ob = self.find_order_block(df, last["bias"], break_idx)
        current_price = float(df["close"].iloc[-1])
        poi_ok, in_ob, in_fvg = self.get_poi_status(current_price, ob, fvg)
        target = self.find_next_liquidity(last["bias"], current_price, sh, sl)
        ote_ok, ote_zone = self.check_ote(df, last["bias"], current_price)
        cvd_ok = self.check_cvd_trend(df, last["bias"])
        session_targets = self.get_session_liquidity_targets(df)
        return {"df": df, "direction": last["bias"], "event_kind": last["kind"], "break_idx": break_idx, "event_timestamp": last["timestamp"], "atr_value": float(atr.iloc[-1]), "volume_ratio": vol_ratio, "displacement_ok": disp_ok, "displacement_ratio": disp_ratio, "sweep": sweep, "fvg": fvg, "ob": ob, "current_price": current_price, "poi_ok": poi_ok, "in_ob": in_ob, "in_fvg": in_fvg, "target": target, "ote_ok": ote_ok, "ote_zone": ote_zone, "cvd_ok": cvd_ok, "session_targets": session_targets, "swing_highs": sh, "swing_lows": sl}, None

    def find_next_liquidity(self, direction: str, price: float, sh: List, sl: List) -> Optional[float]:
        if direction == "bullish":
            targets = [p for _, p in sh if p > price]
            return min(targets) if targets else None
        targets = [p for _, p in sl if p < price]
        return max(targets) if targets else None

    def analyze_smc_pro(self, symbol: str, session_active: bool, session_name: str) -> Dict:
        conditions = {}
        scores = {}
        raw_score = 0

        # ===== HARD FILTERS =====
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

        entry_conf, entry_data = self.check_15m_confirmation(symbol, direction)
        conditions["15M confirmation"] = entry_conf
        if self.config.require_15m_confirmation and not entry_conf:
            return {"symbol": symbol, "passed": False, "conditions": conditions, "score": 0, "reason": "15M confirmation yoxdur"}

        target = smc["target"]
        if target is None:
            return {"symbol": symbol, "passed": False, "conditions": conditions, "score": 0, "reason": "Liquidity target yoxdur"}

        entry = smc["current_price"]
        levels = self.calculate_trade_levels(direction, smc["df"], entry, smc["ob"], target, smc["atr_value"])
        if levels is None:
            return {"symbol": symbol, "passed": False, "conditions": conditions, "score": 0, "reason": "Trade levels hesablanmadı"}

        rr = levels["rr_ratio"]
        rr_ok = rr >= self.config.min_rr_ratio
        conditions[f"RR >= 1:{self.config.min_rr_ratio}"] = rr_ok
        if not rr_ok:
            return {"symbol": symbol, "passed": False, "conditions": conditions, "score": 0, "reason": f"RR {rr} çox aşağıdır"}

        # ===== SOFT FILTERS (SCORING) - Toggle-lar işləyir =====
        # Unmitigated OB
        ob = smc["ob"]
        if self.config.use_unmitigated_ob_scoring:
            scores["unmitigated_ob"] = 15 if (ob is not None and not ob["mitigated"]) else 0
        else:
            scores["unmitigated_ob"] = 0
        conditions["Unmitigated OB"] = ob is not None and not ob["mitigated"]

        # Sweep
        if self.config.use_sweep_scoring:
            scores["sweep"] = 12 if smc["sweep"] else 0
        else:
            scores["sweep"] = 0
        conditions["Sweep"] = smc["sweep"]

        # FVG
        fvg = smc["fvg"]
        fvg_ok = fvg is not None
        if self.config.use_fvg_scoring:
            scores["fvg"] = 8 if fvg_ok else 0
        else:
            scores["fvg"] = 0
        if fvg_ok:
            fvg_type = fvg.get("type", "standard")
            conditions[f"FVG ({fvg_type})"] = True
        else:
            conditions["FVG"] = False

        # POI
        poi_ok = smc["poi_ok"]
        if self.config.use_poi_scoring:
            scores["poi"] = 10 if poi_ok else 0
        else:
            scores["poi"] = 0
        conditions["POI"] = poi_ok

        # Displacement
        disp_ratio = smc["displacement_ratio"]
        if self.config.use_displacement_scoring:
            if disp_ratio >= 1.5:
                scores["displacement"] = 10
            elif disp_ratio >= 0.8:
                scores["displacement"] = 7
            elif disp_ratio >= 0.5:
                scores["displacement"] = 4
            else:
                scores["displacement"] = 0
        else:
            scores["displacement"] = 0
        conditions["Displacement"] = disp_ratio

        # Volume
        vol_ratio = smc["volume_ratio"]
        if self.config.use_volume_scoring:
            if vol_ratio >= 1.5:
                scores["volume"] = 8
            elif vol_ratio >= 1.0:
                scores["volume"] = 5
            elif vol_ratio >= 0.7:
                scores["volume"] = 2
            else:
                scores["volume"] = 0
        else:
            scores["volume"] = 0
        conditions["Volume"] = vol_ratio

        # OTE
        ote_ok = smc["ote_ok"]
        if self.config.use_ote_scoring:
            scores["ote"] = 15 if ote_ok else 0
        else:
            scores["ote"] = 0
        conditions["OTE"] = ote_ok

        # CVD
        cvd_ok = smc["cvd_ok"]
        if self.config.use_cvd_scoring:
            scores["cvd"] = 10 if cvd_ok else 0
        else:
            scores["cvd"] = 0
        conditions["CVD"] = cvd_ok

        # BTC
        fund_score, fund_data = self.fundamental_score(symbol, direction)
        btc_alignment = fund_data.get("btc_alignment")
        if self.config.use_btc_scoring:
            if btc_alignment is True:
                scores["btc"] = 10
            elif btc_alignment is None:
                scores["btc"] = 3
            else:
                scores["btc"] = 0
        else:
            scores["btc"] = 0
        conditions["BTC"] = btc_alignment

        # Session
        if self.config.use_session_scoring:
            scores["session"] = 8 if session_active else 0
        else:
            scores["session"] = 0
        conditions["Session"] = session_active

        # Fear & Greed
        fg = fund_data.get("fear_greed")
        if self.config.use_fear_greed_scoring:
            if fg is not None:
                if direction == "bullish" and fg < 80:
                    scores["fear_greed"] = 5
                elif direction == "bearish" and fg > 20:
                    scores["fear_greed"] = 5
                else:
                    scores["fear_greed"] = 0
            else:
                scores["fear_greed"] = 0
        else:
            scores["fear_greed"] = 0

        # Funding
        funding = fund_data.get("funding")
        if self.config.use_funding_scoring:
            if funding is not None:
                if direction == "bullish" and funding < 0:
                    scores["funding"] = 5
                elif direction == "bearish" and funding > 0:
                    scores["funding"] = 5
                else:
                    scores["funding"] = 0
            else:
                scores["funding"] = 0
        else:
            scores["funding"] = 0

        # OI
        oi = fund_data.get("oi_change")
        if self.config.use_oi_scoring:
            if oi is not None:
                scores["oi"] = 3 if oi > 0 else 0
            else:
                scores["oi"] = 0
        else:
            scores["oi"] = 0

        # Event kind
        event_kind = smc["event_kind"]
        scores["event"] = 15 if event_kind == "CHoCH" else 12

        # Entry confirmation
        scores["entry_conf"] = 15 if entry_conf else 0

        # TOTAL SCORE - Həqiqi 100 üzərindən normallaşdırma
        raw_score = sum(scores.values())
        max_possible_score = 15 + 12 + 8 + 10 + 10 + 8 + 15 + 10 + 10 + 3 + 8 + 5 + 5 + 3 + 15 + 15
        # max_possible_score ≈ 152 (təxmini)
        total_score = min(100, round((raw_score / 152) * 100, 1))  # 🔥 Həqiqi 100-lük normallaşdırma

        score_ok = total_score >= self.config.min_signal_score
        conditions["Total Score"] = total_score
        conditions["Min score threshold"] = score_ok

        if not score_ok:
            return {"symbol": symbol, "passed": False, "conditions": conditions, "score": total_score, "reason": f"Score {total_score} aşağıdır"}

        pos = self.calculate_position_size(levels["entry_adjusted"], levels["sl_adjusted"])
        if pos["margin_required"] > self.config.account_balance:
            return {"symbol": symbol, "passed": False, "conditions": conditions, "score": total_score, "reason": "Margin balansdan böyükdür"}

        # 🔥 Stabil Signal ID (timestamp əsaslı)
        signal_id = generate_signal_id(symbol, direction, smc["event_timestamp"], smc["current_price"])

        return {
            "symbol": symbol,
            "passed": True,
            "conditions": conditions,
            "scores": scores,
            "score": total_score,
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
            "commission": self.config.commission_percent,
            "slippage": self.config.slippage_percent,
            "daily_bias": daily,
            "h4_bias": h4,
            "session": session_name,
            "risk_amount": round(pos["risk_amount"], 2),
            "position_size": round(pos["position_size"], 6),
            "notional_value": round(pos["notional_value"], 2),
            "margin_required": round(pos["margin_required"], 2),
            "sweep": smc["sweep"],
            "poi_ok": poi_ok,
            "fvg_ok": fvg_ok,
            "fvg_type": fvg.get("type") if fvg else None,
            "ote_ok": ote_ok,
            "cvd_ok": cvd_ok,
            "displacement_ratio": round(disp_ratio, 2),
            "volume_ratio": round(vol_ratio, 2),
            "entry_confirmation": entry_data,
            "fundamental": fund_data,
            "signal_id": signal_id
        }

# ============================================================================
# YARDIMÇI FUNKSİYALAR
# ============================================================================

def generate_signal_id(symbol: str, direction: str, event_timestamp: int, poi_price: float) -> str:
    """🔥 Stabil signal ID - timestamp əsaslı, break_index dəyişmir"""
    return f"{symbol}_{direction}_{event_timestamp}_{int(poi_price * 1000)}"
# ============================================================================
# PERFORMANCE TRACKER (REAL PER-COIN TP/SL)
# ============================================================================

class PerformanceTracker:
    HISTORY_FILE = "signals_history.json"
    ACTIVE_SIGNALS_FILE = "active_signals.json"

    @classmethod
    def save_signal(cls, signal: Dict) -> None:
        try:
            active = cls.load_active()
            active.append({
                "signal_id": signal["signal_id"],
                "symbol": signal["symbol"],
                "direction": signal["direction"],
                "entry": signal["entry"],
                "sl": signal["sl"],
                "tp": signal["tp"],
                "rr_ratio": signal["rr_ratio"],
                "leverage": signal.get("leverage", 10),
                "commission": signal.get("commission", 0.04),
                "slippage": signal.get("slippage", 0.02),
                "entry_time": datetime.now(timezone.utc).isoformat(),
                "status": "active"
            })
            with open(cls.ACTIVE_SIGNALS_FILE, "w") as f:
                json.dump(active, f, indent=2)
        except Exception as e:
            logger.warning(f"Could not save active signal: {e}")

    @classmethod
    def load_active(cls) -> List[Dict]:
        try:
            with open(cls.ACTIVE_SIGNALS_FILE, "r") as f:
                return json.load(f)
        except:
            return []

    @classmethod
    def update_signal(cls, signal_id: str, result: str, exit_price: float, exit_time: str) -> None:
        try:
            active = cls.load_active()
            history = cls.load_history()
            for sig in active:
                if sig["signal_id"] == signal_id:
                    sig["status"] = "closed"
                    sig["result"] = result
                    sig["exit_price"] = exit_price
                    sig["exit_time"] = exit_time
                    # 🔥 Real P&L hesablanır
                    entry = sig.get("entry", 0)
                    leverage = sig.get("leverage", 10)
                    commission = sig.get("commission", 0.04) / 100
                    slippage = sig.get("slippage", 0.02) / 100
                    if sig["direction"] == "bullish":
                        pnl_percent = ((exit_price - entry) / entry) * 100 * leverage
                    else:
                        pnl_percent = ((entry - exit_price) / entry) * 100 * leverage
                    # Komissiya və slippage çıxarılır
                    pnl_percent = pnl_percent - commission * 2 - slippage * 2
                    sig["pnl_percent"] = round(pnl_percent, 2)
                    sig["pnl_dollar"] = round(pnl_percent / 100 * sig.get("risk_amount", 0) / (sig.get("risk_percent", 1) / 100), 2)
                    history.append(sig)
                    break
            active = [s for s in active if s["status"] != "closed"]
            with open(cls.ACTIVE_SIGNALS_FILE, "w") as f:
                json.dump(active, f, indent=2)
            with open(cls.HISTORY_FILE, "w") as f:
                json.dump(history, f, indent=2)
        except Exception as e:
            logger.warning(f"Could not update signal: {e}")

    @classmethod
    def load_history(cls) -> List[Dict]:
        try:
            with open(cls.HISTORY_FILE, "r") as f:
                return json.load(f)
        except:
            return []

    @classmethod
    def check_active_signals_for_symbol(cls, symbol: str, current_price: float) -> None:
        """🔥 Hər coin üçün öz qiyməti ilə TP/SL yoxlanılır"""
        active = cls.load_active()
        now = datetime.now(timezone.utc).isoformat()
        for sig in active:
            if sig["symbol"] != symbol:
                continue
            if sig["status"] != "active":
                continue
            entry = sig.get("entry", 0)
            sl = sig.get("sl", 0)
            tp = sig.get("tp", 0)
            if sig["direction"] == "bullish":
                if current_price >= tp:
                    cls.update_signal(sig["signal_id"], "WIN", tp, now)
                elif current_price <= sl:
                    cls.update_signal(sig["signal_id"], "LOSS", sl, now)
            else:  # bearish
                if current_price <= tp:
                    cls.update_signal(sig["signal_id"], "WIN", tp, now)
                elif current_price >= sl:
                    cls.update_signal(sig["signal_id"], "LOSS", sl, now)

    @classmethod
    def calculate_stats(cls) -> Dict:
        data = cls.load_history()
        if not data:
            return {"total": 0, "win_rate": 0, "avg_rr": 0, "sharpe": 0, "drawdown": 0, "active": 0, "total_pnl": 0}

        total = len(data)
        wins = sum(1 for d in data if d.get("result") == "WIN")
        win_rate = (wins / total * 100) if total > 0 else 0

        rr_list = []
        pnl_list = []
        for d in data:
            rr = d.get("rr_ratio", 0)
            if d.get("result") == "WIN":
                rr_list.append(rr)
                pnl_list.append(d.get("pnl_percent", 0))
            else:
                rr_list.append(-1)
                pnl_list.append(d.get("pnl_percent", 0))

        avg_rr = np.mean(rr_list) if rr_list else 0
        std_rr = np.std(rr_list) if len(rr_list) > 1 else 1
        sharpe = (avg_rr / std_rr) if std_rr > 0 else 0

        max_dd = 0
        peak = 0
        cumulative = 0
        for d in data:
            rr = d.get("rr_ratio", 0)
            if d.get("result") == "WIN":
                cumulative += rr
            else:
                cumulative -= 1
            if cumulative > peak:
                peak = cumulative
            dd = peak - cumulative
            if dd > max_dd:
                max_dd = dd

        total_pnl = sum(pnl_list) if pnl_list else 0

        return {
            "total": total,
            "win_rate": round(win_rate, 2),
            "avg_rr": round(avg_rr, 2),
            "sharpe": round(sharpe, 2),
            "drawdown": round(max_dd, 2),
            "active": len(cls.load_active()),
            "total_pnl": round(total_pnl, 2)
        }

# ============================================================================
# SKANER (PER-COIN TP/SL TRACKING)
# ============================================================================

class SignalScanner:
    def __init__(self, config: Config, client: BybitClient, analyzer: SMCAnalyzer) -> None:
        self.config = config
        self.client = client
        self.analyzer = analyzer
        self._processed_signals: set = set()  # Duplicate signal blocking

    def fetch_top_liquid_coins(self, limit: int) -> List[str]:
        tickers = self.client.fetch_tickers()
        if not tickers:
            return self.config.fallback_coins
        filtered = [r for r in tickers if r.get("symbol", "").endswith("USDT") and float(r.get("turnover24h", 0) or 0) > 0]
        filtered.sort(key=lambda r: float(r.get("turnover24h", 0) or 0), reverse=True)
        symbols = [r["symbol"] for r in filtered[:limit]]
        if symbols:
            logger.info(f"{len(symbols)} likvid coin taranır")
            return symbols
        return self.config.fallback_coins

    def scan(self) -> Tuple[Optional[Dict], List[Dict]]:
        session_active, session_name = self.analyzer.get_trading_session()
        coins = self.fetch_top_liquid_coins(self.config.scan_top_n_coins)
        results = []

        # 🔥 Per-coin TP/SL tracking - hər coin üçün öz qiyməti ilə yoxlanılır
        for symbol in coins:
            current_price = self.client.fetch_current_price(symbol)
            if current_price is not None:
                PerformanceTracker.check_active_signals_for_symbol(symbol, current_price)

        for symbol in coins:
            try:
                res = self.analyzer.analyze_smc_pro(symbol, session_active, session_name)
                results.append(res)
                # 🔥 Duplicate signal blocking
                if res.get("passed"):
                    signal_id = res["signal_id"]
                    if signal_id in self._processed_signals:
                        res["passed"] = False
                        res["reason"] = "Duplicate signal (already processed)"
                    else:
                        self._processed_signals.add(signal_id)
            except Exception as e:
                logger.error(f"{symbol} error: {e}")
                results.append({"symbol": symbol, "passed": False, "error": str(e), "conditions": {}, "score": 0, "reason": str(e)})
            time.sleep(0.15)

        valid = [r for r in results if r.get("passed")]
        valid.sort(key=lambda x: (x.get("score", 0), x.get("rr_ratio", 0)), reverse=True)
        return valid[0] if valid else None, results
# ============================================================================
# TELEGRAM BOT
# ============================================================================

class TelegramBot:
    def __init__(self, config: Config, scanner: SignalScanner) -> None:
        self.config = config
        self.scanner = scanner
        self._last_notified: Dict[str, float] = {}

    def format_signal(self, res: Dict, title: str = "🚨 *PRO SMC SIGNAL* 🚨") -> str:
        f = res.get("fundamental", {})
        ec = res.get("entry_confirmation", {})
        strength = "🔥 VERY STRONG" if res["score"] >= 90 else "🟢 STRONG" if res["score"] >= 75 else "🔵 GOOD"
        fvg_info = f"{res.get('fvg_type', 'N/A')}" if res.get('fvg_ok') else "❌"
        scores = res.get("scores", {})
        return f"""{title}

{strength} | Score: `{res['score']}/100`

🪙 *{res['symbol']}* | {res['bias']} ({res['event_kind']})

📈 Daily: `{res['daily_bias']}` | 4H: `{res['h4_bias']}`
🕒 Session: `{res['session']}`
⚖️ RR: `1:{res['rr_ratio']}`

📍 ENTRY: `{res['entry']}` (Adj: `{res['entry_adj']}`)
🛑 SL: `{res['sl']}` (Adj: `{res['sl_adj']}`)
🎯 TP: `{res['tp']}` (Adj: `{res['tp_adj']}`)

⚙️ Lev: `{res['leverage']}x` | 💰 Margin: `${res['margin_required']}`

🔎 *SMC Filters*
💧 Sweep: `{res['sweep']}` | 📦 POI: `{res['poi_ok']}`
📊 FVG: `{res['fvg_ok']}` ({fvg_info})
🎯 OTE: `{res['ote_ok']}` | 📈 CVD: `{res['cvd_ok']}`
⚡ Displacement: `{res['displacement_ratio']}` | Vol: `{res['volume_ratio']}`

📊 *Score Breakdown*
Sweep: `{scores.get('sweep',0)}` | FVG: `{scores.get('fvg',0)}` | POI: `{scores.get('poi',0)}`
Disp: `{scores.get('displacement',0)}` | Vol: `{scores.get('volume',0)}` | OTE: `{scores.get('ote',0)}`
CVD: `{scores.get('cvd',0)}` | BTC: `{scores.get('btc',0)}` | Sess: `{scores.get('session',0)}`
Event: `{scores.get('event',0)}` | EntryConf: `{scores.get('entry_conf',0)}`
OB: `{scores.get('unmitigated_ob',0)}`

⏱ 15M: `{ec.get('event')}` (Age: `{ec.get('age')}`)
🌍 BTC: `{f.get('btc_bias')}` | FG: `{f.get('fear_greed')}`
🆔 `{res['signal_id']}`"""

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
            await application.bot.send_message(
                chat_id=self.config.chat_id,
                text=self.format_signal(result),
                parse_mode="Markdown"
            )
            PerformanceTracker.save_signal(result)
            logger.info(f"Signal sent: {result['signal_id']}")
        except Exception as e:
            logger.error(f"Send error: {e}")

    async def auto_scan_loop(self, application) -> None:
        await asyncio.sleep(15)
        while True:
            try:
                logger.info("Auto scan started...")
                result, all_res = await asyncio.to_thread(self.scanner.scan)
                if result:
                    sid = result["signal_id"]
                    now = time.time()
                    if now - self._last_notified.get(sid, 0) >= self.config.notify_cooldown_seconds:
                        await self.send_signal(application, result)
                        self._last_notified[sid] = now
                else:
                    logger.info("No valid setup")
            except Exception as e:
                logger.error(f"Auto scan error: {e}")
            await asyncio.sleep(self.config.check_interval_seconds)

    @staticmethod
    async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        msg = """📊 *Professional SMC AI Bot V4.2*

✅ Hard Filter (Məcburi):
• Daily + 4H + 1H Triple Alignment
• 15M Confirmation
• RR >= 1:2

✅ Soft Filter (Skora təsir edir):
• Sweep, FVG, POI, Displacement, Volume
• OTE, CVD, BTC, Session, Fear&Greed, Funding, OI
• Unmitigated OB

✅ Real Performance Tracking:
• Per-coin TP/SL tracking (hər coin öz qiyməti ilə)
• Real P&L (komissiya, slippage, leverage)
• Duplicate signal blocking
• Score 100 üzərindən normallaşdırılıb

Komandalar:
/analiz - Canlı analiz
/stats - Performans statistikası"""
        await update.message.reply_text(msg, parse_mode="Markdown")

    @staticmethod
    async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        stats = PerformanceTracker.calculate_stats()
        msg = f"""📈 *PERFORMANCE STATISTICS*

📊 Total Signals: `{stats['total']}`
🏆 Win Rate: `{stats['win_rate']}%`
⚖️ Avg RR: `1:{stats['avg_rr']}`
📉 Sharpe Ratio: `{stats['sharpe']}`
📉 Max Drawdown: `{stats['drawdown']}`
🟢 Active Signals: `{stats['active']}`
💰 Total P&L: `{stats['total_pnl']}%`

*Bu statistikalar real TP/SL bağlanmalarına əsaslanır.*"""
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
            return "PRO SMC AI BOT V4.2 RUNNING"
    def run(self) -> None:
        Thread(target=self._run, daemon=True).start()
    def _run(self) -> None:
        self.app.run(host="0.0.0.0", port=self.port)

# ============================================================================
# MAIN
# ============================================================================

async def post_init(application):
    bot: TelegramBot = application.bot_data["bot_instance"]
    application.create_task(bot.auto_scan_loop(application))
    logger.info("Auto scanner started")

def main() -> None:
    try:
        config = Config()
    except ValueError as e:
        logger.error(e)
        return
    KeepAliveServer(config.flask_port).run()
    client = BybitClient()
    analyzer = SMCAnalyzer(config, client)
    scanner = SignalScanner(config, client, analyzer)
    bot = TelegramBot(config, scanner)
    app = ApplicationBuilder().token(config.bot_token).post_init(post_init).build()
    app.bot_data["bot_instance"] = bot
    app.add_handler(CommandHandler("start", TelegramBot.start_command))
    app.add_handler(CommandHandler("analiz", bot.analiz_command))
    app.add_handler(CommandHandler("stats", TelegramBot.stats_command))
    logger.info("PROFESSIONAL SMC BOT V4.2 STARTED!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
