import os
import logging
import requests
from flask import Flask
from threading import Thread
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# Render üçün sadə veb server (Port xətasını aradan qaldırmaq üçün)
app_flask = Flask('')

@app_flask.route('/')
def home():
    return "Bot is running!"

def run_flask():
    app_flask.run(host='0.0.0.0', port=int(os.getenv("PORT", 10000)))

def keep_alive():
    t = Thread(target=run_flask)
    t.start()

# Telegram Bot Konfiqurasiyası
TOKEN = os.getenv("BOT_TOKEN")
COINS = ["bitcoin", "ethereum", "solana", "ripple", "binancecoin", "cardano", "avalanche-2", "dogecoin", "polkadot", "chainlink"]

def get_best_crypto_opportunity():
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "ids": ",".join(COINS),
            "order": "market_cap_desc",
            "per_page": 10,
            "page": 1,
            "sparkline": "false",
            "price_change_percentage": "24h"
        }
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            best_coin = None
            max_score = -9999
            
            for item in data:
                symbol = item['symbol'].upper() + "USDT"
                price = item['current_price']
                price_change = item.get('price_change_percentage_24h', 0) or 0
                volume = item['total_volume']
                
                if price_change < 0:
                    current_signal = "🟢 BUY (Dip fürsəti)"
                    current_score = abs(price_change) * (volume / 1000000)
                else:
                    current_signal = "🔴 SELL (Zirvə/Satış fürsəti)"
                    current_score = price_change * (volume / 1000000)

                if current_score > max_score:
                    max_score = current_score
                    best_coin = {
                        "symbol": symbol,
                        "price": price,
                        "change": round(price_change, 2),
                        "signal": current_signal
                    }

            if best_coin:
                return (
                    f"📊 *10 Coin İçindən Ən Yüksək Fürsət!*\n\n"
                    f"🪙 *Coin:* `{best_coin['symbol']}`\n"
                    f"💰 *Qiymət:* `${best_coin['price']}`\n"
                    f"📈 *24s Dəyişiklik:* `{best_coin['change']}%`\n"
                    f"🎯 *Tövsiyə:* *{best_coin['signal']}*"
                )
    except Exception as e:
        logging.error(f"API xətası: {e}")
    
    return "Hazırda bazar məlumatlarını oxumaq mümkün olmadı."

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Salam! Trading botum aktivdir və port సమస్య həll olundu.\n"
        "Analiz almaq üçün /analiz yazın! 🚀",
        parse_mode="Markdown"
    )

async def analiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 10 coin analiz edilir, zəhmət olmasa gözləyin...")
    result = get_best_crypto_opportunity()
    await update.message.reply_text(result, parse_mode="Markdown")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    keep_alive()  # Veb serveri işə salırıq ki, Render port xətası verməsin
    
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("analiz", analiz))
    app.run_polling()
        
