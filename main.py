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
  return "Real SMC Bot is running!"


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

# --- SMC PARAMETRLƏRİ ---
SWING_LOOKBACK = 2      # Swing high/low təyin etmək üçün hər tərəfdən neçə şama baxılsın
MIN_RR_RATIO = 1.5      # Minimum Risk/Mükafat nisbəti (1:1.5)
KLINES_LIMIT = 150      # Analiz üçün çəkiləcək şam sayı


def fetch_futures_klines(symbol, interval="60", limit=KLINES_LIMIT):
  """
  Bybit Futures (v5, USDT Perpetual = 'linear' category) API-dən şam (kline) datası çəkir.
  interval: Bybit formatı - "60" = 1 saat.
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
        if len(rows) > 60:
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
          # Son sətir hələ bağlanmamış (davam edən) şamdır - onu çıxarırıq
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


def find_swing_points(df, lookback=SWING_LOOKBACK):
  """
  Fraktal əsaslı swing high/low nöqtələrini tapır.
  Bir şam yalnız o halda 'swing high'dır ki, hər iki tərəfdəki
  'lookback' qədər şamdan da yüksək olsun (eyni qayda swing low üçün).
  """
  highs = df["high"].values
  lows = df["low"].values
  n = len(df)
  swing_highs = []  # [(index, qiymət), ...]
  swing_lows = []

  for i in range(lookback, n - lookback):
    left_high = highs[i - lookback:i]
    right_high = highs[i + 1:i + lookback + 1]
    if highs[i] > left_high.max() and highs[i] > right_high.max():
      swing_highs.append((i, highs[i]))

    left_low = lows[i - lookback:i]
    right_low = lows[i + 1:i + lookback + 1]
    if lows[i] < left_low.min() and lows[i] < right_low.min():
      swing_lows.append((i, lows[i]))

  return swing_highs, swing_lows


def detect_bos(df, swing_highs, swing_lows):
  """
  BOS (Break of Structure) aşkarlayır: cari (son bağlanmış) qiymət
  son swing high-ı keçibsə -> bullish BOS,
  son swing low-u keçibsə -> bearish BOS.
  """
  if not swing_highs or not swing_lows:
    return None, None

  last_close = df["close"].iloc[-1]
  last_sh_idx, last_sh_price = swing_highs[-1]
  last_sl_idx, last_sl_price = swing_lows[-1]

  if last_close > last_sh_price:
    return "bullish", last_sh_idx
  if last_close < last_sl_price:
    return "bearish", last_sl_idx
  return None, None


def find_order_block(df, direction, break_idx):
  """
  BOS-a səbəb olan impulsiv hərəkətdən əvvəlki son ƏKS istiqamətli
  şamı (Order Block) tapır.
  Bullish BOS -> son qırmızı (bearish) şam axtarılır.
  Bearish BOS -> son yaşıl (bullish) şam axtarılır.
  """
  segment = df.iloc[break_idx:-1]
  if segment.empty:
    return None

  if direction == "bullish":
    candidates = segment[segment["close"] < segment["open"]]
  else:
    candidates = segment[segment["close"] > segment["open"]]

  if candidates.empty:
    return None

  ob = candidates.iloc[-1]
  return {"high": float(ob["high"]), "low": float(ob["low"])}


def find_next_liquidity(direction, current_price, swing_highs, swing_lows):
  """TP hədəfi kimi növbəti (hələ toxunulmamış) likvidlik səviyyəsini tapır."""
  if direction == "bullish":
    targets = [p for _, p in swing_highs if p > current_price]
    return min(targets) if targets else None
  else:
    targets = [p for _, p in swing_lows if p < current_price]
    return max(targets) if targets else None


def analyze_smc(symbol):
  """
  Əsl Smart Money Concepts məntiqi ilə analiz:
  1. Swing nöqtələri (bazar strukturu)
  2. BOS (Break of Structure)
  3. Order Block (impulsu yaradan əks şam)
  4. Qiymətin OB zonasına geri çəkilməsi (retracement)
  5. Növbəti likvidlik səviyyəsi (TP)
  6. Risk/Mükafat yoxlanması
  """
  df = fetch_futures_klines(symbol)

  if df is None or len(df) < 60:
    return {
        "symbol": symbol,
        "passed": False,
        "error": "Bybit API-dən data alınmadı (şəbəkə problemi ola bilər)",
        "conditions": {},
    }

  conditions = {}

  swing_highs, swing_lows = find_swing_points(df)
  cond_structure = len(swing_highs) >= 2 and len(swing_lows) >= 2
  conditions["Kifayət qədər bazar strukturu (swing nöqtələri) mövcuddur"] = cond_structure
  if not cond_structure:
    return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

  direction, break_idx = detect_bos(df, swing_highs, swing_lows)
  cond_bos = direction is not None
  conditions["BOS baş verib (struktur qırılıb)"] = cond_bos
  if not cond_bos:
    return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

  ob = find_order_block(df, direction, break_idx)
  cond_ob = ob is not None
  conditions["Order Block (OB) tapıldı"] = cond_ob
  if not cond_ob:
    return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

  current_price = df["close"].iloc[-1]
  buffer = (ob["high"] - ob["low"]) * 0.1
  in_zone = (ob["low"] - buffer) <= current_price <= (ob["high"] + buffer)
  conditions[
      f"Qiymət OB zonasına geri çəkilib (OB: {ob['low']:.4f}-{ob['high']:.4f}, cari: {current_price:.4f})"
  ] = in_zone
  if not in_zone:
    return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

  liquidity_target = find_next_liquidity(direction, current_price, swing_highs, swing_lows)
  cond_liquidity = liquidity_target is not None
  conditions["Növbəti likvidlik səviyyəsi (TP hədəfi) mövcuddur"] = cond_liquidity
  if not cond_liquidity:
    return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

  entry = round(current_price, 4)
  if direction == "bullish":
    sl = round(ob["low"] * 0.997, 4)
    tp = round(liquidity_target, 4)
    bias = "🟢 LONG (Bullish Order Block Retest)"
  else:
    sl = round(ob["high"] * 1.003, 4)
    tp = round(liquidity_target, 4)
    bias = "🔴 SHORT (Bearish Order Block Retest)"

  risk = abs(entry - sl)
  reward = abs(tp - entry)
  rr_ratio = (reward / risk) if risk > 0 else 0
  cond_rr = rr_ratio >= MIN_RR_RATIO
  conditions[f"Risk/Mükafat nisbəti kifayətdir (>= 1:{MIN_RR_RATIO}, faktiki: 1:{rr_ratio:.2f})"] = cond_rr

  passed = cond_rr

  return {
      "symbol": symbol,
      "passed": passed,
      "error": None,
      "conditions": conditions,
      "bias": bias,
      "entry": entry,
      "sl": sl,
      "tp": tp,
      "rr_ratio": round(rr_ratio, 2),
      "leverage": LEVERAGE,
  }


def get_best_smc_signal():
  """
  Bütün coinləri yoxlayır. Şərtləri ödəyən ilk siqnalı,
  və HƏR coin üçün diaqnostik nəticələri qaytarır.
  """
  all_results = []
  for symbol in FUTURES_COINS:
    res = analyze_smc(symbol)
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
        lines.append(f"• `{symbol}`: ❌ Ödənilməyən şərt:")
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
            f"🚨 *REAL SMC SİQNALİ* 🚨\n\n"
            f"🪙 *Aktiv:* `{res['symbol']}`\n"
            f"⚙️ *Leverage:* `{res['leverage']}x`\n"
            f"🎯 *Strategiya:* *{res['bias']}*\n"
            f"⚖️ *Risk/Mükafat:* `1:{res['rr_ratio']}`\n\n"
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
      "📊 *Real SMC Bot* aktivdir!\n"
      "Canlı analiz üçün `/analiz` yazın.\n\n"
      "Metodologiya: Market Structure (BOS) + Order Block + Liquidity Target",
      parse_mode="Markdown",
  )


async def analiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
  await update.message.reply_text("🔍 Real SMC (BOS + Order Block) əsasında bazar skan edilir...")
  res, all_results = get_best_smc_signal()

  if res:
    msg = (
        f"📊 *Real SMC Ticarət Siqnalı*\n\n"
        f"🪙 *Aktiv:* `{res['symbol']}`\n"
        f"⚙️ *Leverage:* `{res['leverage']}x`\n"
        f"🎯 *Strategiya:* *{res['bias']}*\n"
        f"⚖️ *Risk/Mükafat:* `1:{res['rr_ratio']}`\n\n"
        f"📍 *Giriş (Entry):* `${res['entry']}`\n"
        f"🛑 *Stop Loss (SL):* `${res['sl']}`\n"
        f"🎯 *Take Profit (TP):* `${res['tp']}`"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")
  else:
    diag = format_diagnostics(all_results)
    await update.message.reply_text(
        "Hazırda uyğun SMC strukturu tapılmadı.\n\n" + diag,
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
  
