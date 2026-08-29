import os
import logging
import requests
import pandas as pd
import numpy as np
from flask import Flask
from threading import Thread
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# Render port xətasını önləmək üçün sadə veb server
app_flask = Flask('')

@app_flask.route('/')
def home():
    return "Futures SMC Bot is running!"

def run_flask():
    app_flask.run(host='0.0.0.0', port=int(os.getenv("PORT", 10000)))

def keep_alive():
    t = Thread(target=run_flask)
    t.start()

TOKEN = os.getenv("BOT_TOKEN")
# Füçers bazarında ən çox həcmi olan əsas cütlüklər
FUTURES_COINS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT", "DOGEUSDT"]
LEVERAGE = 10  # Füçers üçün tövsiyə olunan standart leverej

def fetch_binance_futures_klines(symbol, interval="1h", limit=100):
    # Binance Futures API (dreqular Spot əvəzinə füçers klines endpointi)
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
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
        logging.error(f"Binance Futures API xətası ({symbol}): {e}")
    return None

def analyze_futures_smc(symbol):
    df = fetch_binance_futures_klines(symbol, interval="1h", limit=100)
    if df is None or len(df) < 50:
        return None

    # Təməl Analiz Filtri: Həcm aktivliyi
    avg_volume = df['volume'].mean()
    current_volume = df['volume'].iloc[-1]
    is_volume_strong = current_volume > (avg_volume * 1.1)

    # SMC - Order Block & Range Hesablamaları
    current_price = df['close'].iloc[-1]
    recent_high = df['high'].iloc[-20:-1].max()
    recent_low = df['low'].iloc[-20:-1].min()
    
    price_range = recent_high - recent_low
    fib_0618 = recent_high - (price_range * 0.618) # Discount Zone (Golden Pocket)

    # Füçers mövqeyi və risk hesablaması
    if current_price <= fib_0618 * 1.01:
        bias = "🟢 LONG (Füçers Alış / Discount Zone)"
        entry = round(current_price, 4)
        sl = round(recent_low * 0.99, 4)  # Likvidlik bölgəsinin altı
        tp = round(entry + ((entry - sl) * 2.5), 4) # 1:2.5 Risk/Reward
    elif current_price >= recent_high * 0.99:
        bias = "🔴 SHORT (Füçers Satış / Premium Zone)"
        entry = round(current_price, 4)
        sl = round(recent_high * 1.01, 4) # Zirvənin üstü
        tp = round(entry - ((sl - entry) * 2.5), 4)
    else:
        bias = "⚖️ RANGE (Gözləmə / Konsolidasiya)"
        entry = round(current_price, 4)
        sl = round(current_price * 0.985, 4)
        tp = round(current_price * 1.025, 4)

    fund_text = "Yüksək Həcm ⚡" if is_volume_strong else "Stabil Həcm 📊"

    return {
        "symbol": symbol,
        "bias": bias,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "fundamental": fund_text,
        "leverage": LEVERAGE
    }

def get_best_futures_signal():
    best_res = None
    for symbol in FUTURES_COINS:
        res = analyze_futures_smc(symbol)
        if res and ("LONG" in res["bias"] or "SHORT" in res["bias"]):
            best_res = res
            break
    
    if not best_res:
        best_res = analyze_futures_smc(FUTURES_COINS[0])
    
    return best_res

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🚀 *Füçers SMC & Təməl Analiz Botu* aktivdir!\n"
        "Leverage ilə işləyən dəqiq giriş nöqtələri üçün /analiz yazın.",
        parse_mode="Markdown"
    )

async def analiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⚡ Füçers bazarı (Binance FAPI) SMC strategiyası ilə skan edilir...")
    res = get_best_futures_signal()
    
    if res:
        msg = (
            f"📊 *Füçers Ticarət Siqnalı (SMC)*\n\n"
            f"🪙 *Coin:* `{res['symbol']}`\n"
            f"⚙️ *Tövsiyə olunan Leverage:* `{res['leverage']}x`\n"
            f"🎯 *Əməliyyat:* *{res['bias']}*\n"
            f"📈 *Həcm Statusu:* `{res['fundamental']}`\n\n"
            f"📍 *Giriş (Entry):* `${res['entry']}`\n"
            f"🛑 *Stop Loss (SL):* `${res['sl']}`\n"
            f"🎯 *Take Profit (TP):* `${res['tp']}`\n\n"
            f"⚠️ *Füçers Xəbərdarlığı:* Risk idarəetməsini və marjanı unutmayın!"
        )
        await update.message.reply_text(msg, parse_mode="Markdown")
    else:
        await update.message.reply_text("Hazırda füçers bazarında dəqiq siqnal şərti ödənmədi.")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    keep_alive()
    
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("analiz", analiz))
    app.run_polling()
    
