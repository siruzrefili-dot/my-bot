import os
import logging
import requests
import pandas as pd
from flask import Flask
from threading import Thread
import time
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# Logging quraşdırması
logging.basicConfig(level=logging.INFO)

# Flask serveri (Render üçün keep-alive)
app_flask = Flask('')


@app_flask.route('/')
def home():
  return "Real-Data SMC Bot is running!"


def run_flask():
  port = int(os.getenv("PORT", 10000))
  app_flask.run(host='0.0.0.0', port=port)


def keep_alive():
  t = Thread(target=run_flask, daemon=True)
  t.start()


TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "1121794078")

FUTURES_COINS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "AVAXUSDT",
    "DOGEUSDT",
    "LINKUSDT",
    "SUIUSDT",
]
LEVERAGE = 10

# --- ŞƏRT PARAMETRLƏRİ (istəsəniz bunları dəyişə bilərsiniz) ---
MIN_ZONE_DISTANCE_PCT = 0.5   # Premium/Discount zonasının orta xətdən min. uzaqlığı (%)
MIN_VOLUME_RATIO = 0.7        # Cari həcmin orta həcmə nisbəti minimum həddi


def fetch_futures_klines(symbol, interval="60", limit=100):
  """
  Bybit Futures (v5, USDT Perpetual = 'linear' category) API-dən şam (kline) datası çəkir.
  interval: Bybit formatı - "60" = 1 saat (Binance-in "1h" formatına uyğun).
  """
  url = (
      f"https://api.bybit.com/v5/market/kline"
      f"?category=linear&symbol={symbol}&interval={interval}&limit={limit}"
  )
  try:
    response = requests.get(url, timeout=5)
    if response.status_code == 200:
      payload = response.json()
      if payload.get("retCode") == 0:
        rows = payload.get("result", {}).get("list", [])
        if len(rows) > 30:
          df = pd.DataFrame(
              rows,
              columns=[
                  "timestamp",
                  "open",
                  "high",
                  "low",
                  "close",
                  "volume",
                  "turnover",
              ],
          )
          # Bybit ən yeni şamı birinci qaytarır - xronoloji sıraya (köhnədən yeniyə) salırıq
          df = df.iloc[::-1].reset_index(drop=True)
          df["open"] = df["open"].astype(float)
          df["high"] = df["high"].astype(float)
          df["low"] = df["low"].astype(float)
          df["close"] = df["close"].astype(float)
          df["volume"] = df["volume"].astype(float)
          # Son sətir hələ bağlanmamış (davam edən) şamdır - onu çıxarırıq,
          # əks halda "cari həcm" süni şəkildə çox aşağı görünür.
          df = df.iloc[:-1].reset_index(drop=True)
          return df
        else:
          logging.warning(f"{symbol}: Bybit-dən qaytarılan data kifayət qədər deyil (len={len(rows)})")
      else:
        logging.error(f"{symbol}: Bybit API xətası - retCode={payload.get('retCode')}, msg={payload.get('retMsg')}")
    elif response.status_code == 403:
      logging.error(f"{symbol}: Bybit bu regionu bloklayır (403 - geo-restriction).")
    else:
      logging.error(f"{symbol}: Bybit API status {response.status_code} - {response.text[:200]}")
  except Exception as e:
    logging.error(f"API xətası ({symbol}): {e}")
  return None


def analyze_balanced_smc(symbol):
  """
  Hər coin üçün analiz aparır və bütün şərtlərin nəticəsini
  (keçdi/keçmədi) qaytarır ki, diaqnostika mümkün olsun.
  """
  df = fetch_futures_klines(symbol)

  if df is None or len(df) < 30:
    return {
        "symbol": symbol,
        "passed": False,
        "error": "Bybit API-dən data alınmadı (şəbəkə problemi ola bilər)",
        "conditions": {},
    }

  current_price = df["close"].iloc[-1]
  recent_high = df["high"].iloc[-30:-1].max()
  recent_low = df["low"].iloc[-30:-1].min()
  mid_price = (recent_high + recent_low) / 2

  avg_volume = df["volume"].mean()
  current_volume = df["volume"].iloc[-1]
  volume_ratio = (current_volume / avg_volume) if avg_volume > 0 else 0

  zone_distance_pct = abs(current_price - mid_price) / mid_price * 100

  # --- Şərtlər ---
  cond_zone_clear = zone_distance_pct >= MIN_ZONE_DISTANCE_PCT
  cond_volume_ok = volume_ratio >= MIN_VOLUME_RATIO

  conditions = {
      f"Aydın Premium/Discount zonası (>= {MIN_ZONE_DISTANCE_PCT}%, faktiki: {zone_distance_pct:.2f}%)": cond_zone_clear,
      f"Həcm aktivdir (>= {MIN_VOLUME_RATIO*100:.0f}% orta, faktiki: {volume_ratio*100:.0f}%)": cond_volume_ok,
  }

  passed = all(conditions.values())

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

  volume_status = "Aktiv ⚡" if cond_volume_ok else "Normal 📊"

  return {
      "symbol": symbol,
      "passed": passed,
      "error": None,
      "conditions": conditions,
      "bias": bias,
      "entry": entry,
      "sl": sl,
      "tp": tp,
      "volume": volume_status,
      "leverage": LEVERAGE,
  }


def get_best_smc_signal():
  """
  Bütün coinləri yoxlayır. Şərtləri ödəyən ilk siqnalı,
  və HƏR coin üçün diaqnostik nəticələri qaytarır.
  """
  all_results = []
  for symbol in FUTURES_COINS:
    res = analyze_balanced_smc(symbol)
    all_results.append(res)
    time.sleep(0.2)  # Rate-limit xətasının qarşısını almaq üçün kiçik fasilə

  for res in all_results:
    if res["passed"]:
      return res, all_results

  return None, all_results


def format_diagnostics(all_results):
  """Heç bir siqnal tapılmayanda, hər coin üçün nə baş verdiyini göstərir."""
  lines = ["📋 *Analiz Detalları (nəyə görə siqnal tapılmadı):*\n"]
  for res in all_results:
    symbol = res["symbol"]
    if res["error"]:
      lines.append(f"• `{symbol}`: ❌ {res['error']}")
    else:
      failed = [name for name, ok in res["conditions"].items() if not ok]
      if failed:
        lines.append(f"• `{symbol}`: ❌ Ödənilməyən şərt(lər):")
        for f in failed:
          lines.append(f"   - {f}")
      else:
        lines.append(f"• `{symbol}`: ✅ Bütün şərtlər ödənildi")
  return "\n".join(lines)


def background_auto_signals(application):
  if not TOKEN:
    return
  while True:
    time.sleep(3600)
    try:
      res, _ = get_best_smc_signal()
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
        application.bot.send_message(
            chat_id=CHAT_ID, text=msg, parse_mode="Markdown"
        )
    except Exception as e:
      logging.error(f"Avtomatik xəta: {e}")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
  await update.message.reply_text(
      "📊 *Real-Time SMC Bot* aktivdir!\n"
      "Canlı analiz üçün `/analiz` yazın.",
      parse_mode="Markdown",
  )


async def analiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
  await update.message.reply_text("🔍 Bybit Futures canlı bazarı skan edilir...")
  res, all_results = get_best_smc_signal()

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
    diag = format_diagnostics(all_results)
    await update.message.reply_text(
        "Hazırda uyğun struktur tapılmadı.\n\n" + diag,
        parse_mode="Markdown",
    )


def main():
  if not TOKEN:
    logging.error("BOT_TOKEN tapılmadı! Environment variables yoxlayın.")
    return

  keep_alive()

  application = ApplicationBuilder().token(TOKEN).build()

  application.add_handler(CommandHandler("start", start))
  application.add_handler(CommandHandler("analiz", analiz))

  t_auto = Thread(
      target=background_auto_signals, args=(application,), daemon=True
  )
  t_auto.start()

  logging.info("Bot işə düşdü...")
  application.run_polling()


if __name__ == "__main__":
  main()
          
