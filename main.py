import os
import logging
import requests
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# Tokeni Render-dəki Environment Variable-dan oxuyuruq
TOKEN = os.getenv("BOT_TOKEN")

# İzlənəcək 10 məşhur coin
COINS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT", "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "DOTUSDT", "LINKUSDT"]

def get_best_crypto_opportunity():
    try:
        url = "https://api.binance.com/api/v3/ticker/24hr"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            filtered = [item for item in data if item['symbol'] in COINS]
            
            best_coin = None
            max_score = -9999
            
            for item in filtered:
                symbol = item['symbol']
                price_change = float(item['priceChangePercent'])
                volume = float(item['quoteVolume'])
                
                # Fürsət balı hesablamaq (həcm və dəyişikliyə əsasən)
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
                        "price": item['lastPrice'],
                        "change": price_change,
                        "signal": current_signal
                    }

            if best_coin:
                return (
                    f"📊 *10 Coin İçindən Ən Yüksək Fürsət!*\n\n"
                    f"🪙 *Coin:* `{best_coin['symbol']}`\n"
                    f"💰 *Qiymət:* `{best_coin['price']}`\n"
                    f"📈 *24s Dəyişiklik:* `{best_coin['change']}%`\n"
                    f"🎯 *Tövsiyə:* *{best_coin['signal']}*"
                )
    except Exception as e:
        logging.error(f"API xətası: {e}")
    
    return "Hazırda bazar məlumatlarını oxumaq mümkün olmadı."

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Salam! Trading botum aktivdir. 10 əsas coini izləyirəm.\n"
        "Dərhal analiz almaq üçün /analiz yazın! 🚀",
        parse_mode="Markdown"
    )

async def analiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 10 coin analiz edilir, zəhmət olmasa gözləyin...")
    result = get_best_crypto_opportunity()
    await update.message.reply_text(result, parse_mode="Markdown")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app = ApplicationBuilder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("analiz", analiz))
    
    app.run_polling()
    
  
