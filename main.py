import os
import logging
import requests
import pandas as pd
from flask import Flask
from threading import Thread
import time
from datetime import datetime, timezone
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# Logging quraşdırması
logging.basicConfig(level=logging.INFO)

# Flask serveri (Render üçün keep-alive)
app_flask = Flask('')


@app_flask.route('/')
def home():
  return "Professional SMC Bot is running!"


def run_flask():
  port = int(os.getenv("PORT", 10000))
  app_flask.run(host='0.0.0.0', port=port)


def keep_alive():
  t = Thread(target=run_flask, daemon=True)
  t.start()


def env_bool(name, default=True):
  val = os.getenv(name)
  if val is None:
    return default
  return val.strip().lower() in ("1", "true", "yes", "beli", "bəli")


TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "1121794078")

# Əgər dinamik siyahı çəkilə bilməzsə, ehtiyat (fallback) siyahı
FALLBACK_COINS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "LINKUSDT", "SUIUSDT",
]

SCAN_TOP_N_COINS = int(os.getenv("SCAN_TOP_N_COINS", "40"))  # Neçə ən likvid coin taransın
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))  # Neçə saniyədə bir avtomatik yoxlansın (defolt: 5 dəqiqə)
NOTIFY_COOLDOWN_SECONDS = int(os.getenv("NOTIFY_COOLDOWN_SECONDS", "7200"))  # Eyni coin üçün təkrar bildiriş arası minimum vaxt (defolt: 2 saat)
LEVERAGE = 10

# --- SMC PARAMETRLƏRİ ---
SWING_LOOKBACK = 2          # Swing high/low təyin etmək üçün hər tərəfdən neçə şama baxılsın
MIN_RR_RATIO = 2.0          # Minimum Risk/Mükafat nisbəti (peşəkar standart: 1:2)
KLINES_LIMIT = 150          # 1H analiz üçün çəkiləcək şam sayı
DAILY_KLINES_LIMIT = 120    # Günlük trend üçün çəkiləcək şam sayı

# --- ƏLAVƏ ŞƏRTLƏRİ AÇ/BAĞLA (Environment Variables ilə tənzimlənə bilər) ---
REQUIRE_TREND_ALIGN = env_bool("REQUIRE_TREND_ALIGN", True)        # 1H BOS günlük trendlə üst-üstə düşməlidir
REQUIRE_LIQUIDITY_SWEEP = env_bool("REQUIRE_LIQUIDITY_SWEEP", True)  # BOS-dan əvvəl liquidity sweep olmalıdır
REQUIRE_FVG = env_bool("REQUIRE_FVG", True)                        # Fair Value Gap mövcud olmalıdır
REQUIRE_SESSION_FILTER = env_bool("REQUIRE_SESSION_FILTER", True)  # Yalnız London/NY seansında siqnal

# --- RİSK İDARƏÇİLİYİ ---
ACCOUNT_BALANCE = float(os.getenv("ACCOUNT_BALANCE", "1000"))  # Fərz edilən balans (USDT)
RISK_PERCENT = float(os.getenv("RISK_PERCENT", "1"))           # Əməliyyat başına risk (%)


# ============================================================
#                       DATA ÇƏKİLMƏSİ
# ============================================================

def fetch_top_liquid_coins(limit=SCAN_TOP_N_COINS):
  """
  Bybit-dəki bütün USDT Perpetual coinləri 24 saatlıq dövriyyəyə (turnover)
  görə sıralayıb ən likvid olanları qaytarır. Bu, statik 10 coin siyahısından
  daha geniş bazar taraması təmin edir - eyni sərt SMC qaydaları ilə.
  """
  url = "https://api.bybit.com/v5/market/tickers?category=linear"
  try:
    response = requests.get(url, timeout=8)
    if response.status_code == 200:
      payload = response.json()
      if payload.get("retCode") == 0:
        rows = payload.get("result", {}).get("list", [])
        usdt_pairs = [r for r in rows if r.get("symbol", "").endswith("USDT")]
        usdt_pairs.sort(key=lambda r: float(r.get("turnover24h") or 0), reverse=True)
        symbols = [r["symbol"] for r in usdt_pairs[:limit]]
        if symbols:
          logging.info(f"Dinamik siyahı: {len(symbols)} ən likvid coin taranacaq.")
          return symbols
  except Exception as e:
    logging.error(f"Ən likvid coin siyahısı çəkilə bilmədi: {e}")
  logging.warning("Ehtiyat (fallback) coin siyahısı istifadə olunur.")
  return FALLBACK_COINS


def fetch_klines(symbol, interval="60", limit=KLINES_LIMIT):
  """
  Bybit Futures (v5, USDT Perpetual = 'linear' category) API-dən şam datası çəkir.
  interval: Bybit formatı - "60" = 1 saat, "D" = günlük.
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
              columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"],
          )
          df = df.iloc[::-1].reset_index(drop=True)  # köhnədən yeniyə sırala
          for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
          df = df.iloc[:-1].reset_index(drop=True)  # son (natamam) şamı çıxar
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


# ============================================================
#                    STRUKTUR / SMC FUNKSİYALARI
# ============================================================

def find_swing_points(df, lookback=SWING_LOOKBACK):
  """Fraktal əsaslı swing high/low nöqtələrini tapır."""
  highs = df["high"].values
  lows = df["low"].values
  n = len(df)
  swing_highs, swing_lows = [], []

  for i in range(lookback, n - lookback):
    left_h, right_h = highs[i - lookback:i], highs[i + 1:i + lookback + 1]
    if highs[i] > left_h.max() and highs[i] > right_h.max():
      swing_highs.append((i, highs[i]))
    left_l, right_l = lows[i - lookback:i], lows[i + 1:i + lookback + 1]
    if lows[i] < left_l.min() and lows[i] < right_l.min():
      swing_lows.append((i, lows[i]))

  return swing_highs, swing_lows


def determine_trend_bias(swing_highs, swing_lows):
  """HH/HL -> bullish, LH/LL -> bearish, əks halda ranging."""
  if len(swing_highs) < 2 or len(swing_lows) < 2:
    return None
  hh = swing_highs[-1][1] > swing_highs[-2][1]
  hl = swing_lows[-1][1] > swing_lows[-2][1]
  lh = swing_highs[-1][1] < swing_highs[-2][1]
  ll = swing_lows[-1][1] < swing_lows[-2][1]
  if hh and hl:
    return "bullish"
  if lh and ll:
    return "bearish"
  return "ranging"


def detect_bos(df, swing_highs, swing_lows):
  """Cari qiymət son swing-i keçibsə BOS aşkarlanır."""
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
  """BOS-a səbəb olan impulsdan əvvəlki son əks-istiqamətli şam (OB)."""
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


def detect_liquidity_sweep(df, direction, break_idx, lookback_window=15):
  """
  BOS-dan əvvəlki pəncərədə 'stop-hunt' (liquidity sweep) axtarır:
  qiymət əvvəlki bir dibi/zirvəni kəsib keçir, sonra geri qayıdıb bağlanır.
  """
  start = max(0, break_idx - lookback_window)
  segment = df.iloc[start:break_idx + 1].reset_index(drop=True)
  if len(segment) < 3:
    return False

  if direction == "bullish":
    for i in range(2, len(segment)):
      prior_min = segment["low"].iloc[:i].min()
      if segment["low"].iloc[i] < prior_min and segment["close"].iloc[i] > prior_min:
        return True
  else:
    for i in range(2, len(segment)):
      prior_max = segment["high"].iloc[:i].max()
      if segment["high"].iloc[i] > prior_max and segment["close"].iloc[i] < prior_max:
        return True
  return False


def detect_fvg(df, direction, break_idx, lookback_window=15):
  """
  Impuls seqmentində Fair Value Gap (3-şam boşluğu) axtarır.
  Bullish FVG: şam[i-1].high < şam[i+1].low
  Bearish FVG: şam[i-1].low > şam[i+1].high
  """
  start = max(0, break_idx - lookback_window)
  segment = df.iloc[start:break_idx + 1].reset_index(drop=True)
  if len(segment) < 3:
    return False

  for i in range(1, len(segment) - 1):
    if direction == "bullish":
      if segment["high"].iloc[i - 1] < segment["low"].iloc[i + 1]:
        return True
    else:
      if segment["low"].iloc[i - 1] > segment["high"].iloc[i + 1]:
        return True
  return False


def find_next_liquidity(direction, current_price, swing_highs, swing_lows):
  """TP hədəfi: növbəti (hələ toxunulmamış) likvidlik səviyyəsi."""
  if direction == "bullish":
    targets = [p for _, p in swing_highs if p > current_price]
    return min(targets) if targets else None
  else:
    targets = [p for _, p in swing_lows if p < current_price]
    return max(targets) if targets else None


def get_trading_session():
  """Hazırda London/New York (yüksək likvidlik) seansındayıqmı yoxlayır."""
  hour = datetime.now(timezone.utc).hour
  in_london = 7 <= hour < 16
  in_ny = 13 <= hour < 22
  active = in_london or in_ny
  labels = []
  if in_london:
    labels.append("London")
  if in_ny:
    labels.append("New York")
  session_name = " + ".join(labels) if labels else "Asiya seansı (aşağı likvidlik)"
  return active, session_name


# ============================================================
#                    ƏSAS ANALİZ FUNKSİYASI
# ============================================================

def get_daily_trend_bias(symbol):
  """Günlük (Daily) timeframe-də ümumi trend istiqamətini müəyyənləşdirir."""
  df_daily = fetch_klines(symbol, interval="D", limit=DAILY_KLINES_LIMIT)
  if df_daily is None or len(df_daily) < 30:
    return None
  sh, sl = find_swing_points(df_daily, lookback=2)
  return determine_trend_bias(sh, sl)


def analyze_smc_pro(symbol, session_active, session_name):
  """
  Peşəkar SMC analiz zənciri:
  1. Günlük trend bias
  2. 1H bazar strukturu + BOS
  3. BOS-un günlük trendlə uyğunluğu (Trend Filter)
  4. Liquidity Sweep
  5. Order Block
  6. Fair Value Gap (əlavə təsdiq)
  7. Qiymətin OB zonasına retracement-i
  8. Liquidity Target (TP)
  9. Risk/Mükafat nisbəti
  10. Aktiv treyding seansı (London/NY)
  """
  conditions = {}

  # --- 1. Günlük trend ---
  daily_bias = get_daily_trend_bias(symbol)
  cond_daily = daily_bias in ("bullish", "bearish")
  conditions[f"Günlük trend aydındır (nəticə: {daily_bias or 'naməlum'})"] = cond_daily
  if not cond_daily:
    return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

  # --- 2. 1H data və struktur ---
  df = fetch_klines(symbol, interval="60", limit=KLINES_LIMIT)
  if df is None or len(df) < 60:
    return {
        "symbol": symbol, "passed": False,
        "error": "Bybit API-dən 1H data alınmadı (şəbəkə problemi ola bilər)",
        "conditions": {},
    }

  swing_highs, swing_lows = find_swing_points(df)
  cond_structure = len(swing_highs) >= 2 and len(swing_lows) >= 2
  conditions["1H bazar strukturu (swing nöqtələri) kifayətdir"] = cond_structure
  if not cond_structure:
    return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

  direction, break_idx = detect_bos(df, swing_highs, swing_lows)
  cond_bos = direction is not None
  conditions["1H-da BOS baş verib (struktur qırılıb)"] = cond_bos
  if not cond_bos:
    return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

  # --- 3. Trend Filter ---
  if REQUIRE_TREND_ALIGN:
    cond_trend_align = (direction == daily_bias)
    conditions[f"1H BOS günlük trendlə üst-üstə düşür ({direction} vs {daily_bias})"] = cond_trend_align
    if not cond_trend_align:
      return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

  # --- 4. Liquidity Sweep ---
  if REQUIRE_LIQUIDITY_SWEEP:
    cond_sweep = detect_liquidity_sweep(df, direction, break_idx)
    conditions["Liquidity Sweep (stop-hunt) aşkarlanıb"] = cond_sweep
    if not cond_sweep:
      return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

  # --- 5. Order Block ---
  ob = find_order_block(df, direction, break_idx)
  cond_ob = ob is not None
  conditions["Order Block (OB) tapıldı"] = cond_ob
  if not cond_ob:
    return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

  # --- 6. Fair Value Gap ---
  if REQUIRE_FVG:
    cond_fvg = detect_fvg(df, direction, break_idx)
    conditions["Fair Value Gap (FVG) mövcuddur"] = cond_fvg
    if not cond_fvg:
      return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

  # --- 7. Retracement OB zonasına ---
  current_price = df["close"].iloc[-1]
  buffer = (ob["high"] - ob["low"]) * 0.1
  in_zone = (ob["low"] - buffer) <= current_price <= (ob["high"] + buffer)
  conditions[
      f"Qiymət OB zonasına geri çəkilib (OB: {ob['low']:.4f}-{ob['high']:.4f}, cari: {current_price:.4f})"
  ] = in_zone
  if not in_zone:
    return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

  # --- 8. Liquidity Target (TP) ---
  liquidity_target = find_next_liquidity(direction, current_price, swing_highs, swing_lows)
  cond_liquidity = liquidity_target is not None
  conditions["Növbəti likvidlik səviyyəsi (TP hədəfi) mövcuddur"] = cond_liquidity
  if not cond_liquidity:
    return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

  entry = round(current_price, 4)
  if direction == "bullish":
    sl = round(ob["low"] * 0.997, 4)
    tp = round(liquidity_target, 4)
    bias = "🟢 LONG (Bullish OB Retest + Trend Aligned)"
  else:
    sl = round(ob["high"] * 1.003, 4)
    tp = round(liquidity_target, 4)
    bias = "🔴 SHORT (Bearish OB Retest + Trend Aligned)"

  risk = abs(entry - sl)
  reward = abs(tp - entry)
  rr_ratio = (reward / risk) if risk > 0 else 0

  # --- 9. Risk/Mükafat ---
  cond_rr = rr_ratio >= MIN_RR_RATIO
  conditions[f"Risk/Mükafat nisbəti kifayətdir (>= 1:{MIN_RR_RATIO}, faktiki: 1:{rr_ratio:.2f})"] = cond_rr
  if not cond_rr:
    return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

  # --- 10. Seans filtri ---
  if REQUIRE_SESSION_FILTER:
    conditions[f"Aktiv treyding seansındayıq (hazırda: {session_name})"] = session_active
    if not session_active:
      return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

  # --- Risk idarəçiliyi: pozisiya ölçüsü ---
  risk_amount = ACCOUNT_BALANCE * (RISK_PERCENT / 100)
  position_size = (risk_amount / risk) if risk > 0 else 0

  return {
      "symbol": symbol,
      "passed": True,
      "error": None,
      "conditions": conditions,
      "bias": bias,
      "entry": entry,
      "sl": sl,
      "tp": tp,
      "rr_ratio": round(rr_ratio, 2),
      "leverage": LEVERAGE,
      "risk_amount": round(risk_amount, 2),
      "position_size": round(position_size, 4),
      "daily_bias": daily_bias,
      "session": session_name,
  }


def get_best_smc_signal():
  """Bütün coinləri (dinamik ən likvid siyahı) yoxlayır, ilk uyğun siqnalı və hər coin üçün diaqnostikanı qaytarır."""
  session_active, session_name = get_trading_session()
  coins_to_scan = fetch_top_liquid_coins()
  all_results = []
  for symbol in coins_to_scan:
    res = analyze_smc_pro(symbol, session_active, session_name)
    all_results.append(res)
    time.sleep(0.15)

  for res in all_results:
    if res["passed"]:
      return res, all_results

  return None, all_results


def format_diagnostics(all_results, max_detail=8):
  """
  Heç bir siqnal tapılmayanda, ümumi xülasə (hansı şərtdə neçə coin dayandı)
  + ilk bir neçə coin üçün ətraflı detal göstərir (mesaj çox uzun olmasın deyə).
  """
  total = len(all_results)
  reason_counts = {}
  error_count = 0

  for res in all_results:
    if res["error"]:
      error_count += 1
      continue
    failed = [name for name, ok in res["conditions"].items() if not ok]
    if failed:
      key = failed[0].split(" (")[0]
      reason_counts[key] = reason_counts.get(key, 0) + 1

  lines = [f"📋 *Xülasə:* {total} coin yoxlanıldı, heç biri bütün şərtləri ödəmədi.\n"]
  lines.append("*Səbəblərin bölgüsü:*")
  for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
    lines.append(f"• {reason}: `{count}` coin")
  if error_count:
    lines.append(f"• API xətası: `{error_count}` coin")

  lines.append(f"\n*Ətraflı (ilk {max_detail} coin):*")
  for res in all_results[:max_detail]:
    symbol = res["symbol"]
    if res["error"]:
      lines.append(f"• `{symbol}`: ❌ {res['error']}")
    else:
      failed = [name for name, ok in res["conditions"].items() if not ok]
      if failed:
        lines.append(f"• `{symbol}`: ❌ {failed[0]}")
      else:
        lines.append(f"• `{symbol}`: ✅ Bütün şərtlər ödənildi")

  return "\n".join(lines)


def format_signal_message(res, title="📊 *Professional SMC Ticarət Siqnalı*"):
  return (
      f"{title}\n\n"
      f"🪙 *Aktiv:* `{res['symbol']}`\n"
      f"⚙️ *Leverage:* `{res['leverage']}x`\n"
      f"🎯 *Strategiya:* *{res['bias']}*\n"
      f"📈 *Günlük Trend:* `{res['daily_bias']}`\n"
      f"🕒 *Seans:* `{res['session']}`\n"
      f"⚖️ *Risk/Mükafat:* `1:{res['rr_ratio']}`\n\n"
      f"📍 *Giriş (Entry):* `${res['entry']}`\n"
      f"🛑 *Stop Loss (SL):* `${res['sl']}`\n"
      f"🎯 *Take Profit (TP):* `${res['tp']}`\n\n"
      f"💰 *Risk İdarəçiliyi:*\n"
      f"   Balans: `${ACCOUNT_BALANCE}` | Risk: `{RISK_PERCENT}%` (`${res['risk_amount']}`)\n"
      f"   Tövsiyə olunan pozisiya ölçüsü: `{res['position_size']}` {res['symbol'].replace('USDT','')}"
  )


_last_notified = {}  # {symbol: son_bildiriş_vaxtı (unix timestamp)}


def background_auto_signals(application):
  if not TOKEN:
    return
  while True:
    time.sleep(CHECK_INTERVAL_SECONDS)
    try:
      res, _ = get_best_smc_signal()
      if res:
        symbol = res["symbol"]
        now = time.time()
        last_time = _last_notified.get(symbol, 0)
        if now - last_time >= NOTIFY_COOLDOWN_SECONDS:
          msg = format_signal_message(res, title="🚨 *AVTOMATİK PROFESSIONAL SİQNAL* 🚨")
          application.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="Markdown")
          _last_notified[symbol] = now
          logging.info(f"{symbol}: bildiriş göndərildi.")
        else:
          logging.info(f"{symbol}: siqnal var, amma cooldown aktivdir - bildiriş göndərilmədi.")
    except Exception as e:
      logging.error(f"Avtomatik xəta: {e}")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
  await update.message.reply_text(
      "📊 *Professional SMC Bot* aktivdir!\n"
      "Canlı analiz üçün `/analiz` yazın.\n\n"
      f"🔔 Bot arxa planda hər {CHECK_INTERVAL_SECONDS // 60} dəqiqədən bir avtomatik yoxlayır "
      "və uyğun siqnal tapılan kimi sizə bildiriş göndərəcək.\n\n"
      "Metodologiya: Daily Trend + 1H BOS + Liquidity Sweep + Order Block + FVG "
      "+ Liquidity Target + Risk İdarəçiliyi + Seans Filtri",
      parse_mode="Markdown",
  )


async def analiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
  await update.message.reply_text(
      f"🔍 Ən likvid {SCAN_TOP_N_COINS} coin üzərində Professional SMC analiz edilir "
      f"(Daily trend + 1H BOS + Sweep + OB + FVG)... Bu bir az vaxt ala bilər."
  )
  res, all_results = get_best_smc_signal()

  if res:
    msg = format_signal_message(res)
    await update.message.reply_text(msg, parse_mode="Markdown")
  else:
    diag = format_diagnostics(all_results)
    await update.message.reply_text(
        "Hazırda peşəkar SMC şərtlərinin hamısını ödəyən struktur tapılmadı.\n\n" + diag,
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

  t_auto = Thread(target=background_auto_signals, args=(application,), daemon=True)
  t_auto.start()

  logging.info("Bot işə düşdü...")
  application.run_polling()


if __name__ == "__main__":
  main()
    
