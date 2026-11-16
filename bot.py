import os
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
    """Bybit boş string ('') və ya None qaytardıqda xətanın qarşısını alır."""
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

    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
    TELEGRAM_CHAT_ID = str(os.getenv("TELEGRAM_CHAT_ID", ""))

    MIN_24H_VOLUME_USDT = 30_000_000
    TOP_COINS_LIMIT = 40
    SCAN_INTERVAL_SECONDS = 900
    HTF_INTERVAL = "240"
    LTF_INTERVAL = "15"

    RISK_PER_TRADE_PCT = 0.01
    MAX_OPEN_POSITIONS = 3
    DAILY_MAX_LOSS_PCT = 0.04
    MIN_RR_RATIO = 2.5
    MIN_TOTAL_SCORE = 75
    LEVERAGE = 5
class BybitClient:
    def __init__(self):
        self.session = HTTP(
            demo=Config.USE_DEMO,
            api_key=Config.BYBIT_API_KEY,
            api_secret=Config.BYBIT_API_SECRET,
        )
        self.instruments = {}

    def load_instruments(self):
        resp = self.session.get_instruments_info(category="linear")
        if resp.get("retCode") == 0:
            for item in resp["result"]["list"]:
                self.instruments[item["symbol"]] = {
                    "qtyStep": item["lotSizeFilter"]["qtyStep"],
                    "minOrderQty": safe_float(item["lotSizeFilter"]["minOrderQty"]),
                    "tickSize": item["priceFilter"]["tickSize"],
                }
        logger.info(f"{len(self.instruments)} fyuçers instrumenti yükləndi.")

    @staticmethod
    def format_step(value: float, step: str) -> str:
        return str(Decimal(str(value)).quantize(Decimal(step), rounding=ROUND_DOWN))

    def get_top_liquid_futures(self) -> list[dict]:
        resp = self.session.get_tickers(category="linear")
        if resp.get("retCode") != 0:
            return []
        tickers = [
            t for t in resp["result"]["list"]
            if t["symbol"].endswith("USDT")
            and safe_float(t.get("turnover24h")) >= Config.MIN_24H_VOLUME_USDT
        ]
        tickers.sort(key=lambda x: safe_float(x.get("turnover24h")), reverse=True)
        return tickers[: Config.TOP_COINS_LIMIT]

    def get_klines(self, symbol: str, interval: str, limit: int = 100) -> pd.DataFrame:
        resp = self.session.get_kline(
            category="linear", symbol=symbol, interval=interval, limit=limit
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
            resp = self.session.get_wallet_balance(accountType="UNIFIED", coin="USDT")
            if resp.get("retCode") == 0:
                return safe_float(resp["result"]["list"][0].get("totalEquity"))
        except Exception as e:
            logger.error(f"Balans xətası: {e}")
        return 0.0

    def get_open_positions(self) -> list[dict]:
        try:
            resp = self.session.get_positions(category="linear", settleCoin="USDT")
            if resp.get("retCode") == 0:
                return [p for p in resp["result"]["list"] if safe_float(p.get("size")) > 0]
        except Exception as e:
            logger.error(f"Pozisiya oxuma xətası: {e}")
        return []

    def close_all_positions(self) -> int:
        positions = self.get_open_positions()
        closed = 0
        for p in positions:
            side = "Sell" if p["side"] == "Buy" else "Buy"
            try:
                self.session.place_order(
                    category="linear",
                    symbol=p["symbol"],
                    side=side,
                    orderType="Market",
                    qty=p["size"],
                    reduceOnly=True,
                )
                closed += 1
            except Exception as e:
                logger.error(f"{p['symbol']} bağlanma xətası: {e}")
        return closed
class TechnicalSMCAgent:
    def analyze(self, df_htf: pd.DataFrame, df_ltf: pd.DataFrame) -> dict:
        if len(df_htf) < 60 or len(df_ltf) < 60:
            return {"score": 0, "signal": None}

        df_htf["ema20"] = df_htf["close"].ewm(span=20).mean()
        df_htf["ema50"] = df_htf["close"].ewm(span=50).mean()
        htf_bull = df_htf["ema20"].iloc[-1] > df_htf["ema50"].iloc[-1]
        htf_bear = df_htf["ema20"].iloc[-1] < df_htf["ema50"].iloc[-1]

        recent_high = df_ltf["high"].iloc[-20:-3].max()
        recent_low = df_ltf["low"].iloc[-20:-3].min()
        last_close = df_ltf["close"].iloc[-1]

        c1, c2, c3 = df_ltf.iloc[-3], df_ltf.iloc[-2], df_ltf.iloc[-1]
        bull_fvg = c3["low"] > c1["high"] and c2["close"] > c2["open"]
        bear_fvg = c3["high"] < c1["low"] and c2["close"] < c2["open"]

        avg_vol = df_ltf["volume"].iloc[-21:-1].mean()
        rvol = c2["volume"] / avg_vol if avg_vol > 0 else 0

        score, signal, entry, sl = 0, None, last_close, 0.0
        reasons = []

        if htf_bull and last_close > recent_high:
            signal = "Buy"
            score += 20
            reasons.append("4H Bullish + 15M BOS")
            if bull_fvg:
                score += 15
                reasons.append("Bullish FVG")
            if rvol >= 1.8:
                score += 15
                reasons.append(f"RVOL {rvol:.1f}x")
            sl = min(c1["low"], recent_low) * 0.998

        elif htf_bear and last_close < recent_low:
            signal = "Sell"
            score += 20
            reasons.append("4H Bearish + 15M BOS")
            if bear_fvg:
                score += 15
                reasons.append("Bearish FVG")
            if rvol >= 1.8:
                score += 15
                reasons.append(f"RVOL {rvol:.1f}x")
            sl = max(c1["high"], recent_high) * 1.002

        return {"score": score, "signal": signal, "entry": entry, "sl": sl, "reasons": reasons}
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

    async def analyze(self, btc_trend: str, signal: str) -> dict:
        fng = await self.get_fng()
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
        symbol, side, entry, sl = setup["symbol"], setup["signal"], setup["entry"], setup["sl"]
        risk_unit = abs(entry - sl)
        if risk_unit <= 0 or entry <= 0:
            return {"status": "rejected", "reason": "SL və ya qiymət məsafəsi 0-dır"}

        tp = entry + (risk_unit * Config.MIN_RR_RATIO) if side == "Buy" else entry - (risk_unit * Config.MIN_RR_RATIO)
        balance = self.client.get_usdt_balance()
        if balance <= 0:
            return {"status": "rejected", "reason": "Balans 0-dır"}

        risk_usdt = balance * Config.RISK_PER_TRADE_PCT
        raw_qty = risk_usdt / risk_unit

        # Marja Aşımı Qoruması (Max mövqe = Balans * Leverage * 90%)
        max_qty = (balance * Config.LEVERAGE * 0.90) / entry
        final_qty = min(raw_qty, max_qty)

        inst = self.client.instruments.get(symbol)
        if not inst:
            return {"status": "rejected", "reason": "Instrument tapılmadı"}

        qty_str = self.client.format_step(final_qty, inst["qtyStep"])
        if safe_float(qty_str) < inst["minOrderQty"]:
            return {"status": "rejected", "reason": "Miqdar minimumdan azdır"}

        sl_str = self.client.format_step(sl, inst["tickSize"])
        tp_str = self.client.format_step(tp, inst["tickSize"])

        try:
            self.client.session.set_leverage(
                category="linear",
                symbol=symbol,
                buyLeverage=str(Config.LEVERAGE),
                sellLeverage=str(Config.LEVERAGE),
            )
        except Exception:
            pass

        order = self.client.session.place_order(
            category="linear",
            symbol=symbol,
            side=side,
            orderType="Market",
            qty=qty_str,
            stopLoss=sl_str,
            takeProfit=tp_str,
            slTriggerBy="MarkPrice",
            tpTriggerBy="LastPrice",
        )
        return {"status": "executed", "order": order, "qty": qty_str, "sl": sl_str, "tp": tp_str}
class TradingOrchestrator:
    def __init__(self):
        self.client = BybitClient()
        self.tech_agent = TechnicalSMCAgent()
        self.deriv_agent = DerivativesAgent()
        self.fund_agent = FundamentalAgent()
        self.risk_agent = RiskManagerAgent(self.client)

        self.is_paused = False
        self.scan_lock = asyncio.Lock()
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
                    msg = f"🛑 <b>KILL-SWITCH AKTİV:</b> Gündəlik zərər {drawdown:.2%} həddinə çatdı. Bot dayandırıldı."
                    await self.send_telegram(msg)
                    return msg

            open_pos = await asyncio.to_thread(self.client.get_open_positions)
            if len(open_pos) >= Config.MAX_OPEN_POSITIONS:
                return f"Maksimum açıq mövqe limitinə ({Config.MAX_OPEN_POSITIONS}) çatılıb."

            open_symbols = {p["symbol"] for p in open_pos}

            btc_df = await asyncio.to_thread(self.client.get_klines, "BTCUSDT", Config.HTF_INTERVAL, 60)
            btc_trend = "Neutral"
            if len(btc_df) >= 50:
                ema20 = btc_df["close"].ewm(span=20).mean().iloc[-1]
                ema50 = btc_df["close"].ewm(span=50).mean().iloc[-1]
                btc_trend = "Bullish" if ema20 > ema50 else "Bearish"

            tickers = await asyncio.to_thread(self.client.get_top_liquid_futures)
            candidates = []

            for t in tickers:
                sym = t["symbol"]
                if sym in open_symbols:
                    continue

                df_htf = await asyncio.to_thread(self.client.get_klines, sym, Config.HTF_INTERVAL, 80)
                df_ltf = await asyncio.to_thread(self.client.get_klines, sym, Config.LTF_INTERVAL, 80)
                await asyncio.sleep(0.15)

                tech = self.tech_agent.analyze(df_htf, df_ltf)
                if not tech["signal"]:
                    continue

                deriv = self.deriv_agent.analyze(t, tech["signal"])
                fund = await self.fund_agent.analyze(btc_trend, tech["signal"])
                total_score = tech["score"] + deriv["score"] + fund["score"]

                candidates.append({
                    "symbol": sym,
                    "signal": tech["signal"],
                    "entry": tech["entry"],
                    "sl": tech["sl"],
                    "score": total_score,
                    "veto": deriv["score"] < 0,
                    "reasons": tech["reasons"] + deriv["reasons"] + fund["reasons"],
                })

            candidates.sort(key=lambda x: x["score"], reverse=True)
            top3_lines = []
            for i, c in enumerate(candidates[:3], 1):
                status_tag = "⛔ VETO" if c["veto"] else f"{c['score']}/100"
                top3_lines.append(
                    f"{i}. <b>{c['symbol']}</b> ({c['signal']}) — <b>{status_tag}</b>\n   <i>{', '.join(c['reasons'])}</i>"
                )
            top3_text = "\n".join(top3_lines) if top3_lines else "BOS/Trend siqnalı verən koin tapılmadı."

            best_setup = next((c for c in candidates if not c["veto"] and c["score"] >= Config.MIN_TOTAL_SCORE), None)

            if best_setup:
                res = await asyncio.to_thread(self.risk_agent.execute_trade, best_setup)
                if res["status"] == "executed":
                    msg = (
                        f"🚀 <b>YENİ ƏMƏLİYYAT ({'DEMO' if Config.USE_DEMO else 'REAL'})</b>\n"
                        f"Koin: <b>{best_setup['symbol']}</b> | İstiqamət: <b>{best_setup['signal']}</b>\n"
                        f"Agent Balı: <b>{best_setup['score']}/100</b>\n"
                        f"Miqdar: {res['qty']} | SL: {res['sl']} | TP: {res['tp']}\n"
                        f"Səbəblər: {', '.join(best_setup['reasons'])}\n\n"
                        f"🏆 <b>Top 3 Namizəd:</b>\n{top3_text}"
                    )
                    await self.send_telegram(msg)
                    return msg
                return f"⚠️ Namizəd ({best_setup['symbol']}) rədd edildi: {res['reason']}\n\n🏆 <b>Top 3:</b>\n{top3_text}"

            return (
                f"🔍 <b>Skan tamamlandı</b> ({len(tickers)} koin | BTC: {btc_trend})\n"
                f"75+ bal yığan fürsət tapılmadı.\n\n"
                f"🏆 <b>Ən Yüksək Ballı Top 3:</b>\n{top3_text}"
            )

    async def background_loop(self):
        await asyncio.to_thread(self.client.load_instruments)
        await self.send_telegram(
            f"✅ <b>Bybit SMC Multi-Agent Bot (v2) işə düşdü!</b>\nRejim: {'DEMO' if Config.USE_DEMO else 'REAL'}"
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
        "🤖 <b>Bybit SMC Multi-Agent Bot (v2)</b>\n"
        "/status - Balans və açıq mövqelər\n"
        "/scan - İndi manuel skan başlat (Top 3 hesabatı ilə)\n"
        "/pause - Avtomatik skanı dayandır\n"
        "/resume - Avtomatik skanı davam etdir\n"
        "/close_all - Bütün açıq mövqeləri bağla",
        parse_mode="HTML",
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    bal = await asyncio.to_thread(orchestrator.client.get_usdt_balance)
    pos = await asyncio.to_thread(orchestrator.client.get_open_positions)
    state = "⏸ Dayandırılıb" if orchestrator.is_paused else "🟢 Aktiv (7/24)"
    pos_text = "\n".join([f"- {p['symbol']} ({p['side']}): PnL {p.get('unrealisedPnl', '0')} USDT" for p in pos]) or "Yoxdur"
    await update.message.reply_text(
        f"📊 <b>Status:</b> {state}\n"
        f"💰 <b>Balans:</b> {bal:.2f} USDT\n"
        f"📂 <b>Açıq Mövqelər ({len(pos)}):</b>\n{pos_text}",
        parse_mode="HTML",
    )

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
    count = await asyncio.to_thread(orchestrator.client.close_all_positions)
    await update.message.reply_text(f"🚨 {count} açıq mövqe bazar qiyməti ilə bağlandı.")

async def health_check(request):
    return web.Response(text="Bybit SMC Bot v2 is running 24/7")

async def main():
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
