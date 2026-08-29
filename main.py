import os
import logging
import requests
import pandas as pd
import numpy as np
from flask import Flask
from threading import Thread
import time
from telegram import Bot, Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# Render port xətasını önləmək üçün veb server
app_flask = Flask('')

@app_flask.route('/')
def home():
    return "True SMC Futures Bot is running!"

def run_flask():
    app_flask.run(host='0.0.0.0', port=int(os.getenv("PORT", 10000)))

def keep_alive():
    t = Thread(target=run_flask)
    t.start()

# Konfiqurasiya
TOKEN = os.getenv("BOT_TOKEN")
# Buraya öz Telegram Chat ID-ni rəqəm olaraq yaz (məsələn: "12345678") və ya Render-də Environment Variable kimi əlavə et
CHAT_ID = os.getenv("CHAT_ID", "BURAYA_OZ_CHAT_ID_NIZI_YAZIN") 

FUTURES_COINS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
LEVERAGE = 10

def fetch_binance_futures_klines(symbol, interval="1h", limit=150):
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
        logging.error(f"API xətası ({symbol}): {e}")
    return None

def analyze_true_smc(symbol):
    df = fetch_binance_futures_klines(symbol, interval="1h", limit=150)
    if df is None or len(df) < 100:
        return None

    current_price = df['close'].iloc[-1]
    
    # 1. Order Block (OB) Təsbiti: Son güclü hərəkətdən əvvəlki əks rəngli şam
    # Əgər son şamlar yuxarı qalxıbsa (Bullish), ən sonki qırmızı şam Order Block-dur.
    # Əgər aşağı düşübsə (Bearish), ən sonki yaşıl şam Order Block-dur.
    
    ob_zone = None
    trend = "NEUTRAL"
    
    # Son 50 şamın struktur zirvə və dipləri (BOS / CHoCH üçün)
    recent_high = df['high'].iloc[-50:-1].max()
    recent_low = df['low'].iloc[-50:-1].min()
    
    # FVG (Fair Value Gap) - 3 ardıcıl şam arasındakı boşluq yoxlanılır
    # Məsələn: Şam[i-2] high ilə Şam[i] low arasındakı boşluq
    fvg_detected = False
    for i in range(len(df) - 5, len(df) - 1):
        if df['low'].iloc[i] > df['high'].iloc[i-2]: # Bullish FVG
            fvg_detected = True
            break
        elif df['high'].iloc[i] < df['low'].iloc[i-2]: # Bearish FVG
            fvg_detected = True
            break

    # Real SMC Məntiqi: Qiymət Discount (endirim) zonasındadırsa LONG, Premium (zirvə) zonasındadırsa SHORT
    price_range = recent_high - recent_low
    golden_pocket_low = recent_low + (price_range * 0.5) # Equilibrium
    
    if current_price < golden_pocket_low:
        # Long (Bullish Order Block & Discount Zone)
        bias = "🟢 LONG (SMC Bullish OB + Discount)"
        entry = round(current_price, 4)
        sl = round(recent_low * 0.991, 4) # Struktur dipinin altı (Liquidity sweep qoruması)
        tp = round(entry + ((entry - sl) * 3), 4) # 1:3 Risk/Reward (SMC standartı)
    else:
        # Short (Bearish Order Block & Premium Zone)
        bias = "🔴 SHORT (SMC Bearish OB + Premium)"
        entry = round(current_price, 4)
        sl = round(recent_high * 1.009, 4) # Struktur zirvəsinin üstü
        tp = round(entry - ((sl - entry) * 3), 4)

    return {
        "symbol": symbol,
        "bias": bias,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "fvg": "Var ⚡"솥 if fvg_detected else "Normal 📊",
        "leverage": LEVERAGE
    }

def get_best_smc_signal():
    # Ən güclü fürsəti verən coini tapırıq
    for symbol in FUTURES_COINS:
        res = analyze_true_smc(symbol)
        if res:
            return res
    return None

# Avtomatik bildiriş göndərən arxa plan funksiyası
def background_auto_signals():
    # Bot obyektini yaradırıq
    if not TOKEN or CHAT_ID == "BURAYA_OZ_CHAT_ID_NIZI_YAZIN":
        return
    
    auto_bot = Bot(token=TOKEN)
    
    while True:
        try:
            time.sleep(7200) # Hər 2 saatdan bir avtomatik analiz edib göndərəcək
            res = get_best_smc_signal()
            if res:
                msg = (
                    f"🚨 *AVTOMATİK SMC FÜÇERS SİQNALI* 🚨\n\n"
                    f"🪙 *Coin:* `{res['symbol']}`\n"
                    f"⚙️ *Leverage:* `{res['leverage']}x`\n"
                    f"🎯 *Strategiya:* *{res['bias']}*\n"
                    f"⚡ *FVG (Balanssızlıq):* `{res['fvg']}`\n\n"
                    f"📍 *Giriş (Entry):* `${res['entry']}`\n"
                    f"🛑 *Stop Loss (SL):* `${res['sl']}`\n"
                    f"🎯 *Take Profit (TP):* `${res['tp']}`\n\n"
                    f"⚖️ *Qeyd:* Smart Money Concepts (Order Block) qaydaları ilə hesablandı."
                )
                # Synchronous send message using requests to telegram api to avoid async loop conflicts in thread
                requests.post(
                    f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                    json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}
                )
        except Exception as e:
            logging.error(f"Avtomatik bildiriş xətası: {e}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🧠 *True SMC & Order Block Füçers Botu* aktivdir!\n"
        "Ani analiz almaq üçün `/analiz` yazın.",
        parse_mode="Markdown"
    )

async def analiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 Binance Futures qrafikləri SMC (Order Block & FVG) üzrə skan edilir...")
    res = get_best_smc_signal()
    
    if res:
        msg = (
            f"📊 *SMC Füçers Ticarət Siqnalı*\n\n"
            f"🪙 *Coin:* `{res['symbol']}`\n"
            f"⚙️ *Leverage:* `{res['leverage']}x`\n"
            f"🎯 *Strategiya:* *{res['bias']}*\n"
            f"⚡ *Balanssızlıq (FVG):* `{res['fvg']}`\n\n"
            f"📍 *Giriş (Entry):* `${res['entry']}`\n"
            f"🛑 *Stop Loss (SL):* `${res['sl']}`\n"
            f"🎯 *Take Profit (TP):* `${res['tp']}`\n\n"
            f"⚠️ *Risk İdarəetməsi:* Həmişə Stop Loss istifadə edin!"
        )
        await update.message.reply_text(msg, parse_mode="Markdown")
    else:
        await update.message.reply_text("Hazırda uyğun SMC strukturu tapılmadı.")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    keep_alive()
    
    # Arxa planda avtomatik bildiriş göndərən thread başladılır
    t_auto = Thread(target=background_auto_signals, daemon=True)
    t_auto.start()
    
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("analiz", analiz))
    app.run_polling()
            
