import os
import time
import html
import asyncio
import logging
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timezone

import aiohttp
from aiohttp import web
import pandas as pd
from dotenv import load_dotenv
from pybit.unified_trading import HTTP
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("BybitSMCBot")


def safe_float(val, default: float = 0.0) -> float:
    try:
        if val is None or val == "":
            return default
        return float(val)
    except (ValueError, TypeError):
        return default


class Config:
    BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "")
    BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")
    USE_DEMO = os.getenv("USE_DEMO", "true").lower() == "true"
    DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
    LIVE_CONFIRM = os.getenv("LIVE_CONFIRM", "")

    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
    TELEGRAM_CHAT_ID = str(os.getenv("TELEGRAM_CHAT_ID", ""))

    MIN_24H_VOLUME_USDT = 30_000_000
    MAX_SPREAD_PCT = 0.0015
    TOP_COINS_LIMIT = 40
    SCAN_INTERVAL_SECONDS = 900
    MONITOR_INTERVAL_SECONDS = 60
    LIMIT_ORDER_EXPIRY_SECONDS = 14400
    HTF_INTERVAL = "240"
    LTF_INTERVAL = "60"

    RISK_PER_TRADE_PCT = 0.01
    MAX_TOTAL_RISK_PCT = 0.03
    MAX_OPEN_POSITIONS = 3
    MAX_SAME_DIRECTION = 1
    DAILY_MAX_LOSS_PCT = 0.04
    MAX_CONSECUTIVE_LOSSES = 5
    MIN_SL_PCT = 0.006
    ATR_SL_MULT = 2.0
    BE_TRIGGER_R = 1.5
    TRAILING_ATR_MULT = 2.0
    EMERGENCY_TP_RR = 10.0
    MIN_TOTAL_SCORE = 75
    LEVERAGE = 5

    @classmethod
    def validate_live_mode(cls):
        if not cls.USE_DEMO and not cls.DRY_RUN and cls.LIVE_CONFIRM != "YES":
            raise RuntimeError("REAL REJİM BLOKLANDI: Render-də LIVE_CONFIRM=YES təyin edilməyib!")
class BybitClient:
    def __init__(self):
        self.session = HTTP(
            demo=Config.USE_DEMO,
            api_key=Config.BYBIT_API_KEY,
            api_secret=Config.BYBIT_API_SECRET,
        )
        self.instruments = {}

    def api_call_with_retry(self, func, *args, retries: int = 3, **kwargs):
        for attempt in range(retries):
            try:
                resp = func(*args, **kwargs)
                if isinstance(resp, dict) and resp.get("retCode") == 10006:
                    time.sleep(2 ** attempt)
                    continue
                return resp
            except Exception as e:
                if attempt == retries - 1:
                    raise e
                time.sleep(2 ** attempt)
        return {}

    def load_instruments(self):
        cursor = ""
        while True:
            params = {"category": "linear", "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            resp = self.api_call_with_retry(self.session.get_instruments_info, **params)
            if resp.get("retCode") != 0:
                break
            for item in resp["result"]["list"]:
                if item.get("status") != "Trading":
                    continue
                self.instruments[item["symbol"]] = {
                    "qtyStep": item["lotSizeFilter"]["qtyStep"],
                    "minOrderQty": safe_float(item["lotSizeFilter"]["minOrderQty"]),
                    "minNotional": safe_float(item["lotSizeFilter"].get("minNotionalValue"), 5.0),
                    "tickSize": item["priceFilter"]["tickSize"],
                    "maxLeverage": int(safe_float(item.get("leverageFilter", {}).get("maxLeverage"), 5)),
                }
            cursor = resp["result"].get("nextPageCursor", "")
            if not cursor:
                break
        logger.info(f"{len(self.instruments)} aktiv (Trading) fyuçers instrumenti yükləndi.")

    @staticmethod
    def format_step(value: float, step: str) -> str:
        return str(Decimal(str(value)).quantize(Decimal(step), rounding=ROUND_DOWN))

    def get_all_tickers_map(self) -> dict[str, dict]:
        resp = self.api_call_with_retry(self.session.get_tickers, category="linear")
        if resp.get("retCode") != 0:
            return {}
        return {t["symbol"]: t for t in resp["result"]["list"]}

    def get_top_liquid_futures(self) -> list[dict]:
        resp = self.api_call_with_retry(self.session.get_tickers, category="linear")
        if resp.get("retCode") != 0:
            return []
        tickers = []
        for t in resp["result"]["list"]:
            sym = t["symbol"]
            if not sym.endswith("USDT") or sym not in self.instruments:
                continue
            if safe_float(t.get("turnover24h")) < Config.MIN_24H_VOLUME_USDT:
                continue
            bid = safe_float(t.get("bid1Price"))
            ask = safe_float(t.get("ask1Price"))
            if bid > 0 and ask > 0 and ((ask - bid) / bid) > Config.MAX_SPREAD_PCT:
                continue
            tickers.append(t)
        tickers.sort(key=lambda x: safe_float(x.get("turnover24h")), reverse=True)
        return tickers[: Config.TOP_COINS_LIMIT]

    def get_klines(self, symbol: str, interval: str, limit: int = 100) -> pd.DataFrame:
        resp = self.api_call_with_retry(
            self.session.get_kline, category="linear", symbol=symbol, interval=interval, limit=limit
        )
        if resp.get("retCode") != 0 or not resp["result"]["list"]:
            return pd.DataFrame()
        df = pd.DataFrame(
            resp["result"]["list"],
            columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"],
        )
        df = df.iloc[::-1].reset_index(drop=True)
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        return df.iloc[:-1].copy()

    def get_usdt_balance(self) -> float:
        try:
            resp = self.api_call_with_retry(
                self.session.get_wallet_balance, accountType="UNIFIED", coin="USDT"
            )
            if resp.get("retCode") == 0:
                return safe_float(resp["result"]["list"][0].get("totalEquity"))
        except Exception as e:
            logger.error(f"Balans xətası: {e}")
        return 0.0

    def get_open_positions(self) -> list[dict]:
        try:
            resp = self.api_call_with_retry(
                self.session.get_positions, category="linear", settleCoin="USDT"
            )
            if resp.get("retCode") == 0:
                return [p for p in resp["result"]["list"] if safe_float(p.get("size")) > 0]
        except Exception as e:
            logger.error(f"Pozisiya oxuma xətası: {e}")
        return []

    def get_open_orders(self) -> list[dict]:
        try:
            resp = self.api_call_with_retry(
                self.session.get_open_orders, category="linear", settleCoin="USDT"
            )
            if resp.get("retCode") == 0:
                return [
                    o for o in resp["result"]["list"]
                    if o.get("orderType") == "Limit" and not o.get("reduceOnly")
                ]
        except Exception as e:
            logger.error(f"Açıq əmr oxuma xətası: {e}")
        return []

    def cancel_order(self, symbol: str, order_id: str) -> bool:
        try:
            resp = self.api_call_with_retry(
                self.session.cancel_order, category="linear", symbol=symbol, orderId=order_id
            )
            return resp.get("retCode") == 0
        except Exception as e:
            logger.error(f"Əmr ləğv xətası ({symbol}): {e}")
            return False

    def get_closed_pnl(self, limit: int = 5) -> list[dict]:
        try:
            resp = self.api_call_with_retry(
                self.session.get_closed_pnl, category="linear", limit=limit
            )
            if resp.get("retCode") == 0:
                return resp["result"]["list"]
        except Exception as e:
            logger.error(f"Closed PnL xətası: {e}")
        return []

    def set_trading_stop(self, symbol: str, sl: str = None, tp: str = None, position_idx: int = 0) -> bool:
        params = {
            "category": "linear",
            "symbol": symbol,
            "tpslMode": "Full",
            "slTriggerBy": "MarkPrice",
            "tpTriggerBy": "LastPrice",
            "positionIdx": int(position_idx),
        }
        if sl:
            params["stopLoss"] = sl
        if tp:
            params["takeProfit"] = tp
        try:
            resp = self.api_call_with_retry(self.session.set_trading_stop, **params)
            return resp.get("retCode") == 0
        except Exception as e:
            logger.error(f"set_trading_stop xətası ({symbol}): {e}")
            return False

    def close_all_positions_and_orders(self) -> tuple[int, int]:
        positions = self.get_open_positions()
        orders = self.get_open_orders()
        closed_pos, canceled_ord = 0, 0

        for o in orders:
            if self.cancel_order(o["symbol"], o["orderId"]):
                canceled_ord += 1

        for p in positions:
            side = "Sell" if p["side"] == "Buy" else "Buy"
            try:
                self.api_call_with_retry(
                    self.session.place_order,
                    category="linear",
                    symbol=p["symbol"],
                    side=side,
                    orderType="Market",
                    qty=p["size"],
                    reduceOnly=True,
                    positionIdx=int(p.get("positionIdx", 0)),
                )
                closed_pos += 1
            except Exception as e:
                logger.error(f"{p['symbol']} bağlanma xətası: {e}")
        return closed_pos, canceled_ord
class TechnicalSMCAgent:
    @staticmethod
    def calc_adx(df: pd.DataFrame, period: int = 14) -> float:
        high, low, close = df["high"], df["low"], df["close"]
        plus_dm = high.diff().clip(lower=0)
        minus_dm = (-low.diff()).clip(lower=0)
        plus_dm = plus_dm.where(plus_dm > minus_dm, 0.0)
        minus_dm = minus_dm.where(minus_dm > plus_dm, 0.0)

        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low - close.shift()).abs(),
        ], axis=1).max(axis=1)

        atr = tr.ewm(alpha=1 / period, adjust=False).mean()
        plus_di = 100 * (plus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr.replace(0, 1e-9))
        minus_di = 100 * (minus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr.replace(0, 1e-9))
        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, 1e-9)
        adx = dx.ewm(alpha=1 / period, adjust=False).mean().iloc[-1]
        return float(adx) if pd.notna(adx) else 0.0

    def analyze(self, df_htf: pd.DataFrame, df_ltf: pd.DataFrame) -> dict:
        if len(df_htf) < 60 or len(df_ltf) < 60:
            return {"score": 0, "signal": None}

        adx = self.calc_adx(df_ltf, 14)
        if adx < 20:
            return {"score": 0, "signal": None}

        df_htf["ema20"] = df_htf["close"].ewm(span=20).mean()
        df_htf["ema50"] = df_htf["close"].ewm(span=50).mean()
        htf_bull = df_htf["ema20"].iloc[-1] > df_htf["ema50"].iloc[-1]
        htf_bear = df_htf["ema20"].iloc[-1] < df_htf["ema50"].iloc[-1]

        recent_high = df_ltf["high"].iloc[-20:-3].max()
        recent_low = df_ltf["low"].iloc[-20:-3].min()
        prior_swing_low = df_ltf["low"].iloc[-25:-6].min()
        prior_swing_high = df_ltf["high"].iloc[-25:-6].max()
        last_close = df_ltf["close"].iloc[-1]

        c1, c2, c3 = df_ltf.iloc[-3], df_ltf.iloc[-2], df_ltf.iloc[-1]
        bull_fvg = c3["low"] > c1["high"] and c2["close"] > c2["open"]
        bear_fvg = c3["high"] < c1["low"] and c2["close"] < c2["open"]

        bull_sweep = df_ltf["low"].iloc[-6:-1].min() < prior_swing_low and last_close > prior_swing_low
        bear_sweep = df_ltf["high"].iloc[-6:-1].max() > prior_swing_high and last_close < prior_swing_high

        atr = (df_ltf["high"] - df_ltf["low"]).iloc[-15:-1].mean()
        if atr <= 0:
            return {"score": 0, "signal": None}

        overextended = abs(c3["close"] - c3["open"]) > (2.2 * atr)
        avg_vol = df_ltf["volume"].iloc[-21:-1].mean()
        rvol = c2["volume"] / avg_vol if avg_vol > 0 else 0

        score, signal, entry, sl, order_type = 0, None, last_close, 0.0, "Market"
        reasons = [f"ADX {adx:.0f}"]

        if htf_bull and last_close > recent_high:
            signal = "Buy"
            score += 20
            reasons.append("4H Bull + 1H BOS")
            struct_sl = min(c1["low"], recent_low) * 0.998
            if bull_sweep:
                score += 15
                reasons.append("Liquidity Sweep")
            if bull_fvg:
                score += 15
                reasons.append("Bull FVG")
                fvg_mid = (c1["high"] + c3["low"]) / 2
                if overextended and struct_sl < fvg_mid < last_close:
                    entry = fvg_mid
                    order_type = "Limit"
                    reasons.append("FVG 50% Limit")
            sl = min(struct_sl, entry - (atr * Config.ATR_SL_MULT))
            if rvol >= 1.8:
                score += 10
                reasons.append(f"RVOL {rvol:.1f}x")

        elif htf_bear and last_close < recent_low:
            signal = "Sell"
            score += 20
            reasons.append("4H Bear + 1H BOS")
            struct_sl = max(c1["high"], recent_high) * 1.002
            if bear_sweep:
                score += 15
                reasons.append("Liquidity Sweep")
            if bear_fvg:
                score += 15
                reasons.append("Bear FVG")
                fvg_mid = (c1["low"] + c3["high"]) / 2
                if overextended and last_close < fvg_mid < struct_sl:
                    entry = fvg_mid
                    order_type = "Limit"
                    reasons.append("FVG 50% Limit")
            sl = max(struct_sl, entry + (atr * Config.ATR_SL_MULT))
            if rvol >= 1.8:
                score += 10
                reasons.append(f"RVOL {rvol:.1f}x")

        return {
            "score": min(score, 55),
            "signal": signal,
            "entry": entry,
            "sl": sl,
            "atr": atr,
            "order_type": order_type,
            "reasons": reasons,
                    }
class DerivativesAgent:
    def analyze(self, ticker: dict, signal: str) -> dict:
        funding = safe_float(ticker.get("fundingRate"))
        if signal == "Buy":
            if funding > 0.0003:
                return {"score": -100, "reasons": [f"VETO: Yüksək Funding ({funding:.4%})"]}
            return {"score": 25 if funding <= 0.0001 else 15, "reasons": [f"Funding {funding:.4%}"]}
        elif signal == "Sell":
            if funding < -0.0003:
                return {"score": -100, "reasons": [f"VETO: Short Squeeze ({funding:.4%})"]}
            return {"score": 25 if funding >= -0.0001 else 15, "reasons": [f"Funding {funding:.4%}"]}
        return {"score": 0, "reasons": []}


class FundamentalAgent:
    async def get_fng(self) -> int:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get("https://api.alternative.me/fng/?limit=1", timeout=5) as r:
                    data = await r.json()
                    return int(data["data"][0]["value"])
        except Exception:
            return 50

    def analyze(self, btc_trend: str, signal: str, fng: int) -> dict:
        score = 0
        reasons = [f"F&G: {fng}"]
        if (btc_trend == "Bullish" and signal == "Buy") or (btc_trend == "Bearish" and signal == "Sell"):
            score += 15
            reasons.append(f"BTC {btc_trend}")
        else:
            score += 5
        if (signal == "Buy" and fng < 80) or (signal == "Sell" and fng > 20):
            score += 10
        return {"score": score, "reasons": reasons}
class RiskManagerAgent:
    def __init__(self, client: BybitClient):
        self.client = client

    def execute_trade(self, setup: dict) -> dict:
        symbol = setup["symbol"]
        side = setup["signal"]
        entry = setup["entry"]
        sl = setup["sl"]
        order_type = setup.get("order_type", "Market")

        risk_unit = abs(entry - sl)
        if risk_unit <= 0 or entry <= 0:
            return {"status": "rejected", "reason": "SL və ya qiymət məsafəsi 0-dır"}

        sl_pct = risk_unit / entry
        if sl_pct < Config.MIN_SL_PCT:
            return {"status": "rejected", "reason": f"SL çox yaxındır ({sl_pct:.2%} < {Config.MIN_SL_PCT:.2%})"}

        # Trailing Stop trendi tam tutsun deyə uzaq 10R tavan TP qoyulur
        tp_dist = risk_unit * Config.EMERGENCY_TP_RR
        tp = entry + tp_dist if side == "Buy" else max(entry - tp_dist, entry * 0.05)

        balance = self.client.get_usdt_balance()
        if balance <= 0:
            return {"status": "rejected", "reason": "Balans 0-dır"}

        inst = self.client.instruments.get(symbol)
        if not inst:
            return {"status": "rejected", "reason": "Instrument tapılmadı"}

        safe_lev = min(Config.LEVERAGE, inst["maxLeverage"])
        risk_usdt = balance * Config.RISK_PER_TRADE_PCT
        effective_risk = risk_unit + (entry * 0.0012)
        raw_qty = risk_usdt / effective_risk
        max_qty = (balance * safe_lev * 0.90) / entry
        final_qty = min(raw_qty, max_qty)

        qty_str = self.client.format_step(final_qty, inst["qtyStep"])
        qty_f = safe_float(qty_str)
        if qty_f < inst["minOrderQty"] or (qty_f * entry) < inst["minNotional"]:
            return {"status": "rejected", "reason": "Miqdar və ya dəyər birja minimumundan azdır"}

        entry_str = self.client.format_step(entry, inst["tickSize"])
        sl_str = self.client.format_step(sl, inst["tickSize"])
        tp_str = self.client.format_step(tp, inst["tickSize"])

        if Config.DRY_RUN:
            return {
                "status": "executed",
                "order_type": f"DRY_RUN ({order_type})",
                "entry": entry_str,
                "qty": qty_str,
                "sl": sl_str,
                "tp": tp_str,
            }

        try:
            self.client.session.switch_margin_mode(
                category="linear", symbol=symbol, tradeMode=1,
                buyLeverage=str(safe_lev), sellLeverage=str(safe_lev),
            )
        except Exception:
            pass
        try:
            self.client.session.set_leverage(
                category="linear", symbol=symbol,
                buyLeverage=str(safe_lev), sellLeverage=str(safe_lev),
            )
        except Exception:
            pass

        params = {
            "category": "linear",
            "symbol": symbol,
            "side": side,
            "orderType": order_type,
            "qty": qty_str,
            "stopLoss": sl_str,
            "takeProfit": tp_str,
            "tpslMode": "Full",
            "slTriggerBy": "MarkPrice",
            "tpTriggerBy": "LastPrice",
        }
        if order_type == "Limit":
            params["price"] = entry_str
            params["timeInForce"] = "GTC"

        try:
            order = self.client.api_call_with_retry(self.client.session.place_order, **params)
            if order.get("retCode") != 0:
                msg = str(order.get("retMsg", ""))
                return {
                    "status": "rejected",
                    "reason": f"Bybit rədd etdi: {msg}",
                    "skip_symbol": "110126" in msg or order.get("retCode") == 110126,
                }

            if order_type == "Market":
                time.sleep(1)
                pos_list = self.client.get_open_positions()
                pos = next((p for p in pos_list if p["symbol"] == symbol), None)
                pos_idx = int(pos.get("positionIdx", 0)) if pos else 0
                self.client.set_trading_stop(symbol, sl=sl_str, tp=tp_str, position_idx=pos_idx)

            return {
                "status": "executed",
                "order": order,
                "order_type": order_type,
                "entry": entry_str,
                "qty": qty_str,
                "sl": sl_str,
                "tp": tp_str,
            }
        except Exception as e:
            err_str = str(e)
            logger.error(f"Order xətası ({symbol}): {err_str}")
            return {
                "status": "rejected",
                "reason": f"Bybit API xətası: {err_str}",
                "skip_symbol": "110126" in err_str,
            }
class TradingOrchestrator:
    def __init__(self):
        self.client = BybitClient()
        self.tech_agent = TechnicalSMCAgent()
        self.deriv_agent = DerivativesAgent()
        self.fund_agent = FundamentalAgent()
        self.risk_agent = RiskManagerAgent(self.client)

        self.is_paused = False
        self.scan_lock = asyncio.Lock()
        self.tracked_positions: dict[str, dict] = {}
        self.blacklisted_symbols: set[str] = set()
        self.last_loss_pause_id: str = ""
        self.daily_start_balance = 0.0
        self.last_reset_day = datetime.now(timezone.utc).day
        self.tg_app: Application | None = None

    async def send_telegram(self, text: str):
        if self.tg_app and Config.TELEGRAM_CHAT_ID:
            try:
                await self.tg_app.bot.send_message(
                    chat_id=Config.TELEGRAM_CHAT_ID, text=text, parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Telegram göndərmə xətası: {e}")

    async def position_monitor_loop(self):
        """Hər 60 saniyədən bir: Limit ləğvi, TP/SL bildirişi, +1.5R Breakeven və 2x ATR Trailing Stop."""
        init_pos = await asyncio.to_thread(self.client.get_open_positions)
        for p in init_pos:
            entry = safe_float(p.get("avgPrice"))
            sl = safe_float(p.get("stopLoss"))
            side = p["side"]
            already_be = (side == "Buy" and sl >= entry) or (side == "Sell" and sl <= entry and sl > 0)
            self.tracked_positions[p["symbol"]] = {
                "side": side,
                "entry": entry,
                "sl": sl,
                "risk_unit": abs(entry - sl) if sl > 0 else 0.0,
                "be_done": already_be,
            }

        while True:
            await asyncio.sleep(Config.MONITOR_INTERVAL_SECONDS)
            try:
                open_orders = await asyncio.to_thread(self.client.get_open_orders)
                if open_orders:
                    ticker_map = await asyncio.to_thread(self.client.get_all_tickers_map)
                    now_ms = time.time() * 1000
                    for o in open_orders:
                        sym = o["symbol"]
                        created_ms = safe_float(o.get("createdTime"))
                        expired = created_ms > 0 and (now_ms - created_ms) >= (Config.LIMIT_ORDER_EXPIRY_SECONDS * 1000)

                        sl_p = safe_float(o.get("stopLoss"))
                        last_p = safe_float(ticker_map.get(sym, {}).get("lastPrice"))
                        invalidated = (
                            sl_p > 0 and last_p > 0 and (
                                (o["side"] == "Buy" and last_p <= sl_p) or
                                (o["side"] == "Sell" and last_p >= sl_p)
                            )
                        )
                        if expired or invalidated:
                            reason = "qiymət SL-i keçdiyi" if invalidated else "4 saat ərzində dolmadığı"
                            if await asyncio.to_thread(self.client.cancel_order, sym, o["orderId"]):
                                await self.send_telegram(
                                    f"⏳ <b>LİMİT ƏMRİ LƏĞV EDİLDİ ({sym})</b>\n"
                                    f"Setup {reason} üçün əmr avtomatik silindi."
                                )

                current_pos = await asyncio.to_thread(self.client.get_open_positions)
                current_symbols = {p["symbol"] for p in current_pos}

                closed_symbols = set(self.tracked_positions.keys()) - current_symbols
                if closed_symbols:
                    history = await asyncio.to_thread(self.client.get_closed_pnl, 10)
                    for sym in closed_symbols:
                        self.tracked_positions.pop(sym, None)
                        match = next((h for h in history if h["symbol"] == sym), None)
                        if match:
                            pnl = safe_float(match.get("closedPnl"))
                            tag = "✅ QAZANC (TRAILING/TP)" if pnl > 0 else "🛑 SL / ZƏRƏR"
                            await self.send_telegram(
                                f"<b>{tag} — MÖVQE BAĞLANDI</b>\n"
                                f"Koin: <b>{sym}</b>\n"
                                f"Giriş: {match.get('avgEntryPrice')} | Çıxış: {match.get('avgExitPrice')}\n"
                                f"Xalis PnL: <b>{pnl:+.2f} USDT</b>"
                            )

                for p in current_pos:
                    sym = p["symbol"]
                    side = p["side"]
                    entry = safe_float(p.get("avgPrice"))
                    sl = safe_float(p.get("stopLoss"))
                    mark = safe_float(p.get("markPrice"))
                    pos_idx = int(p.get("positionIdx", 0))

                    if sym not in self.tracked_positions:
                        already_be = (side == "Buy" and sl >= entry) or (side == "Sell" and sl <= entry and sl > 0)
                        self.tracked_positions[sym] = {
                            "side": side,
                            "entry": entry,
                            "sl": sl,
                            "risk_unit": abs(entry - sl) if sl > 0 else 0.0,
                            "be_done": already_be,
                        }

                    info = self.tracked_positions[sym]
                    if entry <= 0 or sl <= 0 or mark <= 0:
                        continue

                    inst = self.client.instruments.get(sym)
                    if not inst:
                        continue

                    # 1. +1.5R Breakeven aktivləşməsi
                    if not info["be_done"] and info["risk_unit"] > 0:
                        trigger_dist = info["risk_unit"] * Config.BE_TRIGGER_R
                        if side == "Buy" and sl < entry and mark >= entry + trigger_dist:
                            be_str = self.client.format_step(entry * 1.001, inst["tickSize"])
                            if await asyncio.to_thread(self.client.set_trading_stop, sym, sl=be_str, position_idx=pos_idx):
                                info["be_done"] = True
                                info["sl"] = safe_float(be_str)
                                await self.send_telegram(
                                    f"🛡️ <b>BREAKEVEN + TRAILING AKTİV ({sym})</b>\n"
                                    f"+1.5R çatdı, SL girişə çəkildi ({be_str}). Bundan sonra 2× ATR izləyəcək."
                                )
                        elif side == "Sell" and sl > entry and mark <= entry - trigger_dist:
                            be_str = self.client.format_step(entry * 0.999, inst["tickSize"])
                            if await asyncio.to_thread(self.client.set_trading_stop, sym, sl=be_str, position_idx=pos_idx):
                                info["be_done"] = True
                                info["sl"] = safe_float(be_str)
                                await self.send_telegram(
                                    f"🛡️ <b>BREAKEVEN + TRAILING AKTİV ({sym})</b>\n"
                                    f"+1.5R çatdı, SL girişə çəkildi ({be_str}). Bundan sonra 2× ATR izləyəcək."
                                )

                    # 2. Breakeven-dən sonra 2x ATR Trailing Stop (SL-i qazanca doğru sürüşdür)
                    elif info["be_done"]:
                        df_ltf = await asyncio.to_thread(self.client.get_klines, sym, Config.LTF_INTERVAL, 20)
                        if len(df_ltf) >= 15:
                            atr = (df_ltf["high"] - df_ltf["low"]).iloc[-15:-1].mean()
                            if atr > 0:
                                if side == "Buy":
                                    new_sl = mark - (atr * Config.TRAILING_ATR_MULT)
                                    if new_sl > sl * 1.003:
                                        sl_str = self.client.format_step(new_sl, inst["tickSize"])
                                        if await asyncio.to_thread(self.client.set_trading_stop, sym, sl=sl_str, position_idx=pos_idx):
                                            info["sl"] = safe_float(sl_str)
                                            logger.info(f"Trailing SL yeniləndi ({sym} Buy): {sl_str}")
                                elif side == "Sell":
                                    new_sl = mark + (atr * Config.TRAILING_ATR_MULT)
                                    if new_sl < sl * 0.997:
                                        sl_str = self.client.format_step(new_sl, inst["tickSize"])
                                        if await asyncio.to_thread(self.client.set_trading_stop, sym, sl=sl_str, position_idx=pos_idx):
                                            info["sl"] = safe_float(sl_str)
                                            logger.info(f"Trailing SL yeniləndi ({sym} Sell): {sl_str}")
            except Exception as e:
                logger.error(f"Monitor xətası: {e}")

    async def run_scan_cycle(self) -> str:
        if self.scan_lock.locked():
            return "⏳ Hazırda başqa bir skan davam edir, bir az sonra yenidən yoxlayın."

        async with self.scan_lock:
            today = datetime.now(timezone.utc).day
            balance = await asyncio.to_thread(self.client.get_usdt_balance)
            if self.daily_start_balance == 0 or today != self.last_reset_day:
                self.daily_start_balance = balance
                self.last_reset_day = today

            if self.daily_start_balance > 0:
                drawdown = (self.daily_start_balance - balance) / self.daily_start_balance
                if drawdown >= Config.DAILY_MAX_LOSS_PCT:
                    self.is_paused = True
                    msg = f"🛑 <b>KILL-SWITCH:</b> Gündəlik zərər {drawdown:.2%} həddinə çatdı. Bot dayandırıldı."
                    await self.send_telegram(msg)
                    return msg

            recent_pnl = await asyncio.to_thread(self.client.get_closed_pnl, Config.MAX_CONSECUTIVE_LOSSES)
            if len(recent_pnl) == Config.MAX_CONSECUTIVE_LOSSES:
                latest_id = str(recent_pnl[0].get("orderId", ""))
                if latest_id != self.last_loss_pause_id and all(safe_float(x.get("closedPnl")) < 0 for x in recent_pnl):
                    self.last_loss_pause_id = latest_id
                    self.is_paused = True
                    msg = f"🛑 <b>ARDICIL ZƏRƏR DAYANMASI:</b> Son {Config.MAX_CONSECUTIVE_LOSSES} əməliyyat SL oldu. Bot dayandırıldı (/resume ilə açın)."
                    await self.send_telegram(msg)
                    return msg

            open_pos = await asyncio.to_thread(self.client.get_open_positions)
            open_orders = await asyncio.to_thread(self.client.get_open_orders)
            total_active = len(open_pos) + len(open_orders)
            if total_active >= Config.MAX_OPEN_POSITIONS:
                return f"Maksimum aktiv əməliyyat limitinə ({total_active}/{Config.MAX_OPEN_POSITIONS}) çatılıb."

            if balance > 0:
                open_risk = sum(
                    abs(safe_float(p.get("avgPrice")) - safe_float(p.get("stopLoss"))) * safe_float(p.get("size"))
                    for p in open_pos if safe_float(p.get("stopLoss")) > 0
                )
                if (open_risk / balance) >= Config.MAX_TOTAL_RISK_PCT:
                    return f"Ümumi açıq risk limitinə ({open_risk / balance:.2%}) çatılıb."

            buy_count = sum(1 for p in open_pos if p.get("side") == "Buy") + sum(1 for o in open_orders if o.get("side") == "Buy")
            sell_count = sum(1 for p in open_pos if p.get("side") == "Sell") + sum(1 for o in open_orders if o.get("side") == "Sell")

            active_symbols = {p["symbol"] for p in open_pos} | {o["symbol"] for o in open_orders} | self.blacklisted_symbols

            btc_df = await asyncio.to_thread(self.client.get_klines, "BTCUSDT", Config.HTF_INTERVAL, 60)
            btc_trend = "Neutral"
            if len(btc_df) >= 50:
                ema20 = btc_df["close"].ewm(span=20).mean().iloc[-1]
                ema50 = btc_df["close"].ewm(span=50).mean().iloc[-1]
                btc_trend = "Bullish" if ema20 > ema50 else "Bearish"

            fng = await self.fund_agent.get_fng()
            tickers = await asyncio.to_thread(self.client.get_top_liquid_futures)
            candidates = []

            for t in tickers:
                sym = t["symbol"]
                if sym in active_symbols:
                    continue
                try:
                    df_htf = await asyncio.to_thread(self.client.get_klines, sym, Config.HTF_INTERVAL, 80)
                    df_ltf = await asyncio.to_thread(self.client.get_klines, sym, Config.LTF_INTERVAL, 80)
                    await asyncio.sleep(0.15)

                    tech = self.tech_agent.analyze(df_htf, df_ltf)
                    if not tech["signal"]:
                        continue

                    deriv = self.deriv_agent.analyze(t, tech["signal"])
                    fund = self.fund_agent.analyze(btc_trend, tech["signal"], fng)
                    total_score = min(tech["score"] + deriv["score"] + fund["score"], 100)

                    candidates.append({
                        "symbol": sym,
                        "signal": tech["signal"],
                        "entry": tech["entry"],
                        "sl": tech["sl"],
                        "atr": tech["atr"],
                        "order_type": tech["order_type"],
                        "score": total_score,
                        "veto": deriv["score"] < 0,
                        "reasons": tech["reasons"] + deriv["reasons"] + fund["reasons"],
                    })
                except Exception as e:
                    logger.warning(f"{sym} analiz xətası: {e}")

            candidates.sort(key=lambda x: x["score"], reverse=True)
            top3_lines = []
            for i, c in enumerate(candidates[:3], 1):
                status_tag = "⛔ VETO" if c["veto"] else f"{c['score']}/100"
                top3_lines.append(
                    f"{i}. <b>{c['symbol']}</b> ({c['signal']}) — <b>{status_tag}</b>\n   <i>{', '.join(c['reasons'])}</i>"
                )
            top3_text = "\n".join(top3_lines) if top3_lines else "Trend/ADX şərtinə uyğun koin tapılmadı."

            best_setup = next(
                (
                    c for c in candidates
                    if not c["veto"]
                    and c["score"] >= Config.MIN_TOTAL_SCORE
                    and (
                        (c["signal"] == "Buy" and buy_count < Config.MAX_SAME_DIRECTION)
                        or (c["signal"] == "Sell" and sell_count < Config.MAX_SAME_DIRECTION)
                    )
                ),
                None,
            )

            if best_setup:
                res = await asyncio.to_thread(self.risk_agent.execute_trade, best_setup)
                if res["status"] == "executed":
                    mode_tag = "DRY_RUN" if Config.DRY_RUN else ("DEMO" if Config.USE_DEMO else "REAL")
                    msg = (
                        f"🚀 <b>YENİ ƏMƏLİYYAT ({mode_tag})</b>\n"
                        f"Koin: <b>{best_setup['symbol']}</b> | {best_setup['signal']} ({res['order_type']})\n"
                        f"Agent Balı: <b>{best_setup['score']}/100</b>\n"
                        f"Giriş: {res['entry']} | Miqdar: {res['qty']}\n"
                        f"SL: {res['sl']} | Trailing: +1.5R sonra 2× ATR\n"
                        f"Səbəblər: {', '.join(best_setup['reasons'])}\n\n"
                        f"🏆 <b>Top 3 Namizəd:</b>\n{top3_text}"
                    )
                    await self.send_telegram(msg)
                    return msg

                if res.get("skip_symbol"):
                    self.blacklisted_symbols.add(best_setup["symbol"])
                safe_reason = html.escape(str(res.get("reason", "")))
                err_msg = f"⚠️ Namizəd ({best_setup['symbol']}) rədd edildi: {safe_reason}\n\n🏆 <b>Top 3:</b>\n{top3_text}"
                await self.send_telegram(err_msg)
                return err_msg

            return (
                f"🔍 <b>Skan tamamlandı</b> ({len(tickers)} koin | BTC: {btc_trend} | F&G: {fng})\n"
                f"Uyğun fürsət tapılmadı (və ya istiqamət limiti doludur).\n\n"
                f"🏆 <b>Ən Yüksək Ballı Top 3:</b>\n{top3_text}"
            )

    async def background_loop(self):
        await asyncio.to_thread(self.client.load_instruments)
        asyncio.create_task(self.position_monitor_loop())
        mode = "DRY_RUN" if Config.DRY_RUN else ("DEMO" if Config.USE_DEMO else "REAL")
        await self.send_telegram(
            f"✅ <b>Bybit SMC Multi-Agent Bot (v6 Pro — Trailing Stop) işə düşdü!</b>\n"
            f"Rejim: {mode} | 1H + 2× ATR SL + 2× ATR Trailing Stop (+1.5R)"
        )
        while True:
            if not self.is_paused:
                try:
                    summary = await self.run_scan_cycle()
                    logger.info(summary)
                except Exception as e:
                    logger.error(f"Skan dövrü xətası: {e}")
            await asyncio.sleep(Config.SCAN_INTERVAL_SECONDS)
orchestrator = TradingOrchestrator()

def is_authorized(update: Update) -> bool:
    return str(update.effective_chat.id) == Config.TELEGRAM_CHAT_ID

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    await update.message.reply_text(
        "🤖 <b>Bybit SMC Multi-Agent Bot (v6 Pro — Trailing Stop)</b>\n"
        "/status - Balans, açıq mövqelər və Limit əmrləri\n"
        "/scan - İndi manuel skan başlat (Top 3 hesabatı)\n"
        "/history - Ümumi statistika (+ / -) və son 5 əməliyyat\n"
        "/pause - Avtomatik skanı dayandır\n"
        "/resume - Avtomatik skanı davam etdir\n"
        "/close_all - Bütün mövqeləri bağla və Limit əmrlərini ləğv et",
        parse_mode="HTML",
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    bal = await asyncio.to_thread(orchestrator.client.get_usdt_balance)
    pos = await asyncio.to_thread(orchestrator.client.get_open_positions)
    orders = await asyncio.to_thread(orchestrator.client.get_open_orders)
    state = "⏸ Dayandırılıb" if orchestrator.is_paused else "🟢 Aktiv (7/24)"

    pos_text = "\n".join([
        f"- {p['symbol']} ({p['side']}): PnL {p.get('unrealisedPnl', '0')} USDT | SL: {p.get('stopLoss') or 'Yox'} | TP: {p.get('takeProfit') or 'Yox'}"
        for p in pos
    ]) or "Yoxdur"
    ord_text = "\n".join([f"- {o['symbol']} ({o['side']} Limit @ {o.get('price')})" for o in orders]) or "Yoxdur"

    await update.message.reply_text(
        f"📊 <b>Status:</b> {state}\n"
        f"💰 <b>Balans:</b> {bal:.2f} USDT\n"
        f"📂 <b>Açıq Mövqelər ({len(pos)}):</b>\n{pos_text}\n"
        f"⏳ <b>Gözləyən Limit Əmrləri ({len(orders)}):</b>\n{ord_text}",
        parse_mode="HTML",
    )

async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    history = await asyncio.to_thread(orchestrator.client.get_closed_pnl, 100)
    if not history:
        await update.message.reply_text("📜 Hələlik bağlanmış əməliyyat tarixçəsi yoxdur.")
        return

    wins = sum(1 for h in history if safe_float(h.get("closedPnl")) > 0)
    losses = sum(1 for h in history if safe_float(h.get("closedPnl")) < 0)
    total_pnl = sum(safe_float(h.get("closedPnl")) for h in history)
    win_rate = (wins / len(history)) * 100

    lines = []
    for h in history[:5]:
        pnl = safe_float(h.get("closedPnl"))
        icon = "✅" if pnl > 0 else "🛑"
        lines.append(f"{icon} <b>{h['symbol']}</b>: <b>{pnl:+.2f} USDT</b> (Çıxış: {h.get('avgExitPrice')})")

    msg = (
        f"📊 <b>Ümumi Statistika (Son {len(history)} əməliyyat):</b>\n"
        f"✅ Qazanc: <b>{wins}+</b> | 🛑 Zərər: <b>{losses}-</b>\n"
        f"🎯 Win Rate: <b>{win_rate:.1f}%</b>\n"
        f"💰 Ümumi PnL: <b>{total_pnl:+.2f} USDT</b>\n\n"
        f"📜 <b>Son 5 Bağlanan Əməliyyat:</b>\n" + "\n".join(lines)
    )
    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    await update.message.reply_text("🔍 Skan başladıldı, Top 3 hesabat hazırlanır...")
    res = await orchestrator.run_scan_cycle()
    await update.message.reply_text(res, parse_mode="HTML")

async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    orchestrator.is_paused = True
    await update.message.reply_text("⏸ Bot dayandırıldı.")

async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    orchestrator.is_paused = False
    await update.message.reply_text("▶️ Bot yenidən aktiv edildi.")

async def cmd_close_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    c_pos, c_ord = await asyncio.to_thread(orchestrator.client.close_all_positions_and_orders)
    await update.message.reply_text(
        f"🚨 {c_pos} açıq mövqe bağlandı, {c_ord} gözləyən Limit əmri ləğv edildi."
    )

async def health_check(request):
    return web.Response(text="Bybit SMC Bot v6 Pro is running 24/7")

async def main():
    Config.validate_live_mode()

    web_app = web.Application()
    web_app.router.add_get("/", health_check)
    runner = web.AppRunner(web_app)
    await runner.setup()
    port = int(os.getenv("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Health-check server port {port} üzərində aktivdir.")

    tg_app = Application.builder().token(Config.TELEGRAM_TOKEN).build()
    tg_app.add_handler(CommandHandler("start", cmd_start))
    tg_app.add_handler(CommandHandler("status", cmd_status))
    tg_app.add_handler(CommandHandler("history", cmd_history))
    tg_app.add_handler(CommandHandler("scan", cmd_scan))
    tg_app.add_handler(CommandHandler("pause", cmd_pause))
    tg_app.add_handler(CommandHandler("resume", cmd_resume))
    tg_app.add_handler(CommandHandler("close_all", cmd_close_all))

    orchestrator.tg_app = tg_app
    await tg_app.initialize()
    await tg_app.start()
    await tg_app.updater.start_polling()

    await orchestrator.background_loop()

if __name__ == "__main__":
    asyncio.run(main())
