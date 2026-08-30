import os
import logging
import requests
import pandas as pd
import numpy as np
from flask import Flask
from threading import Thread
import time
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

app_flask = Flask('')

@app_flask.route('/')
def home():
    return "Real-Data SMC Bot is running!"

def run_flask():
    app_flask.run(host='0.0.0.0', port=int(os.getenv("PORT", 10000)))

def keep_alive():
    t = Thread(target=run_flask)
    t.start()

TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = "1121794078"

FUTURES_COINS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", 
    "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "LINKUSDT", "SUIUSDT"
]
LEVERAGE = 10

def fetch_binance_futures_klines(symbol, interval="1h", limit=100):
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list) and len(data) > 30:
                df = pd.DataFrame(data, columns=[
                    'timestamp', 'open', 'high', 'low', 'close', 'volume',
                    'close_time', 'quote_asset_volume', 'number_of_trades',
                    'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
                ])
                df['open'] = df['open'].astype(float)
                df['high'] = df['high'].astype(float)
                df['low'] = df['low'].astype(float)
                df['close'] = df['close'].astype(float)
                df['volume'] = df['volume'].astype(float)
                return df
    except Exception as e:
        logging.error(f"API xətası ({symbol}): {e}")
    return None

def analyze_balanced_smc(symbol):
    df = fetch_binance_futures_klines(symbol)
    if df is None or len(df) < 30:
        return None

    current_price = df['close'].iloc[-1]
    recent_high = df['high'].iloc[-30:-1].max()
    recent_low = df['low'].iloc[-30:-1].min()
    mid_price = (recent_high + recent_low) / 2
    
    avg_volume = df['volume'].mean()
    current_volume = df['volume'].iloc[-1]
    is_volume_good = current_volume > (avg_volume * 0.7)

    if current_price < mid_price:
        bias = "🟢 LONG (SMC Discount Zone)"
        entry = round(current_price, 2)
        sl = round(recent_low * 0.993, 2)
        tp = round(entry + ((entry - sl) * 2.5), 2)
    else:
        bias = "🔴 SHORT (SMC Premium Zone)"
        entry = round(current_price, 2)
        sl = round(recent_high * 1.007, 2)
        tp = round(entry - ((sl - entry) * 2.5), 2)

    volume_status = "Aktiv ⚡" if is_volume_good else "Normal 📊"

    return {
        "symbol": symbol,
        "bias": bias,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "volume": volume_status,
        "leverage": LEVERAGE
    }

def get_best_smc_signal():
    for symbol in FUTURES_COINS:
        res = analyze_balanced_smc(symbol)
        if res:
            return res
    return None

def background_auto_signals():
    if not TOKEN:
        return
    while True:
        try:
            time.sleep(3600)
            res = get_best_smc_signal()
            if res:
                msg = (
                    f"🚨 *REAL BAZAR SİQNALİ* 🚨\n\n"
                    f"🪙 *Aktiv:* `{res['symbol']}`\n"
                    f"⚙️ *Leverage:* `{res['leverage']}x`\n"
                    f"🎯 *Strategiya:* *{res['bias']}*\n"
                    f"⚡ *Həcm:* `{res['volume']}`\n\n"
                    f"📍 *Giriş (Entry):* `${res['entry']}`\n"
                    f"🛑 *Stop Loss (SL):* `${res['sl']}`\n"
                    f"🎯 *Take Profit (TP):* `${res['tp']}`"
                )
                requests.post(
                    f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                    json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}
                )
        except Exception as e:
            logging.error(f"Avtomatik xəta: {e}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📊 *Real-Time SMC Bot* aktivdir!\n"
        "Canlı analiz üçün `/analiz` yazın.",
        parse_mode="Markdown"
    )

async def analiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 Binance Futures canlı bazarı skan edilir...")
    res = get_best_smc_signal()
    
    if res:
        msg = (
            f"📊 *Real SMC Ticarət Siqnalı*\n\n"
            f"🪙 *Aktiv:* `{res['symbol']}`\n"
            f"⚙️ *Leverage:* `{res['leverage']}x`\n"
            f"🎯 *Strategiya:* *{res['bias']}*\n"
            f"⚡ *Həcm:* `{res['volume']}`\n\n"
            f"📍 *Giriş (Entry):* `${res['entry']}`\n"
            f"🛑 *Stop Loss (SL):* `${res['sl']}`\n"
            f"🎯 *Take Profit (TP):* `${res['tp']}`"
        )
        await update.message.reply_text(msg, parse_mode="Markdown")
    else:
        await update.message.reply_text("Hazırda uyğun struktur tapılmadı.")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    keep_alive()
    
    t_auto = Thread(target=background_auto_signals, daemon=True)
    t_auto.start()
    
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("analiz", analiz))
    app.run_polling()
                
