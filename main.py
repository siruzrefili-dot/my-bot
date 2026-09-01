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
REQUIRE_TREND_ALIGN = env_bool("REQUIRE_TREND_ALIGN", True)        # 1H struktur günlük trendlə üst-üstə düşməlidir
REQUIRE_LIQUIDITY_SWEEP = env_bool("REQUIRE_LIQUIDITY_SWEEP", True)  # BOS/CHoCH-dan əvvəl liquidity sweep olmalıdır
REQUIRE_FVG = env_bool("REQUIRE_FVG", True)                        # Fair Value Gap mövcud olmalıdır
REQUIRE_SESSION_FILTER = env_bool("REQUIRE_SESSION_FILTER", True)  # Yalnız London/NY seansında siqnal
REQUIRE_OB_UNMITIGATED = env_bool("REQUIRE_OB_UNMITIGATED", True)  # OB artıq 'sındırılmış' olmamalıdır
REQUIRE_CHOCH_ONLY = env_bool("REQUIRE_CHOCH_ONLY", False)         # Yalnız CHoCH (trend dönüşü) qəbul et, BOS yox
REQUIRE_EQUAL_LEVEL_SWEEP = env_bool("REQUIRE_EQUAL_LEVEL_SWEEP", False)  # Sweep məhz Equal High/Low-a yaxın olmalıdır

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


def compute_atr(df, period=100):
  """LuxAlgo-dakı ATR hesablama məntiqi - True Range-in sadə hərəkətli ortası."""
  high, low, close = df["high"], df["low"], df["close"]
  prev_close = close.shift(1)
  tr = pd.concat([
      (high - low),
      (high - prev_close).abs(),
      (low - prev_close).abs(),
  ], axis=1).max(axis=1)
  return tr.rolling(period, min_periods=1).mean()


def compute_structure_events(df, swing_highs, swing_lows):
  """
  LuxAlgo-nun CHoCH/BOS ayrımına uyğun: bar-bar keçərək aktiv (hələ
  qırılmamış) son swing high/low-u izləyir, trend bias-ı yadda saxlayır.
  - Trend davam edən qırılma -> BOS
  - Trend əks istiqamətə dönən İLK qırılma -> CHoCH (daha güclü siqnal)
  Nəticə: xronoloji sıra ilə hadisələr siyahısı, hər biri
  {"index", "bias", "kind" (BOS/CHoCH)}.
  """
  n = len(df)
  close = df["close"].values
  events = []
  trend_bias = None

  all_pivots = sorted(
      [(idx, price, "high") for idx, price in swing_highs] +
      [(idx, price, "low") for idx, price in swing_lows]
  )
  pivot_ptr = 0
  active_high, active_low = None, None
  high_crossed, low_crossed = True, True

  for i in range(n):
    while pivot_ptr < len(all_pivots) and all_pivots[pivot_ptr][0] == i:
      idx, price, kind = all_pivots[pivot_ptr]
      if kind == "high":
        active_high = (idx, price)
        high_crossed = False
      else:
        active_low = (idx, price)
        low_crossed = False
      pivot_ptr += 1

    if active_high is not None and not high_crossed and i > 0:
      if close[i - 1] <= active_high[1] < close[i]:
        kind = "CHoCH" if trend_bias == "bearish" else "BOS"
        trend_bias = "bullish"
        high_crossed = True
        events.append({"index": i, "bias": "bullish", "kind": kind, "level": active_high[1]})

    if active_low is not None and not low_crossed and i > 0:
      if close[i - 1] >= active_low[1] > close[i]:
        kind = "CHoCH" if trend_bias == "bullish" else "BOS"
        trend_bias = "bearish"
        low_crossed = True
        events.append({"index": i, "bias": "bearish", "kind": kind, "level": active_low[1]})

  return events


def detect_equal_levels(df, swing_highs, swing_lows, atr_series, threshold=0.1):
  """
  LuxAlgo-nun EQH/EQL (Equal High/Low) məntiqi: ardıcıl swing nöqtələri
  ATR-ə görə kifayət qədər yaxındırsa, bu, real 'liquidity pool' hesab olunur.
  """
  equal_highs, equal_lows = [], []
  for i in range(1, len(swing_highs)):
    idx1, p1 = swing_highs[i - 1]
    idx2, p2 = swing_highs[i]
    atr_val = atr_series.iloc[idx2] if idx2 < len(atr_series) else atr_series.iloc[-1]
    if atr_val and abs(p1 - p2) < threshold * atr_val:
      equal_highs.append((idx1, idx2, (p1 + p2) / 2))
  for i in range(1, len(swing_lows)):
    idx1, p1 = swing_lows[i - 1]
    idx2, p2 = swing_lows[i]
    atr_val = atr_series.iloc[idx2] if idx2 < len(atr_series) else atr_series.iloc[-1]
    if atr_val and abs(p1 - p2) < threshold * atr_val:
      equal_lows.append((idx1, idx2, (p1 + p2) / 2))
  return equal_highs, equal_lows


def find_order_block_advanced(df, direction, break_idx, lookback=50):
  """
  LuxAlgo metodu: break-dən əvvəlki pəncərədə (məs. 50 bar) EKSTREMUM
  qiymətli şam (bullish üçün ən aşağı low, bearish üçün ən yüksək high)
  Order Block kimi qəbul olunur - sadəcə 'son əks-rəngli şam' əvəzinə.
  Həmçinin bu OB-nin break-dən sonra artıq 'mitigate' (sındırılıb) olub-
  olmadığı yoxlanılır.
  """
  lookback_start = max(0, break_idx - lookback)
  window_high = df["high"].iloc[lookback_start:break_idx + 1]
  window_low = df["low"].iloc[lookback_start:break_idx + 1]
  if window_high.empty:
    return None

  if direction == "bullish":
    ob_idx = int(window_low.values.argmin()) + lookback_start
  else:
    ob_idx = int(window_high.values.argmax()) + lookback_start

  ob_high = float(df["high"].iloc[ob_idx])
  ob_low = float(df["low"].iloc[ob_idx])

  # Mitigation yoxlanması: OB-dən sonra qiymət artıq onu 'sındırıbmı'?
  mitigated = False
  close = df["close"].values
  for j in range(ob_idx + 1, len(df)):
    if direction == "bullish" and close[j] < ob_low:
      mitigated = True
      break
    if direction == "bearish" and close[j] > ob_high:
      mitigated = True
      break

  return {"high": ob_high, "low": ob_low, "index": ob_idx, "mitigated": mitigated}


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
  Peşəkar SMC analiz zənciri (LuxAlgo metodologiyasına uyğunlaşdırılıb):
  1. Günlük trend bias
  2. 1H bazar strukturu + BOS/CHoCH (trend-track edilmiş)
  3. Struktur hadisəsinin günlük trendlə uyğunluğu (Trend Filter)
  4. Liquidity Sweep (istəyə görə Equal Level ilə təsdiqlənmiş)
  5. Order Block (ekstremum-əsaslı, mitigation yoxlanılmış)
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

  # --- BOS/CHoCH (trend-track edilmiş hadisələr) ---
  structure_events = compute_structure_events(df, swing_highs, swing_lows)
  cond_event = len(structure_events) > 0
  conditions["Struktur hadisəsi (BOS/CHoCH) baş verib"] = cond_event
  if not cond_event:
    return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

  last_event = structure_events[-1]
  direction = last_event["bias"]
  break_idx = last_event["index"]
  event_kind = last_event["kind"]  # "BOS" və ya "CHoCH"

  if REQUIRE_CHOCH_ONLY:
    cond_choch = (event_kind == "CHoCH")
    conditions["Son struktur hadisəsi CHoCH-dur (trend dönüşü)"] = cond_choch
    if not cond_choch:
      return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

  # --- 3. Trend Filter ---
  if REQUIRE_TREND_ALIGN:
    cond_trend_align = (direction == daily_bias)
    conditions[f"1H {event_kind} günlük trendlə üst-üstə düşür ({direction} vs {daily_bias})"] = cond_trend_align
    if not cond_trend_align:
      return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

  # --- Equal High/Low (liquidity pool) ---
  atr_series = compute_atr(df)
  equal_highs, equal_lows = detect_equal_levels(df, swing_highs, swing_lows, atr_series)

  # --- 4. Liquidity Sweep ---
  if REQUIRE_LIQUIDITY_SWEEP:
    cond_sweep = detect_liquidity_sweep(df, direction, break_idx)
    conditions["Liquidity Sweep (stop-hunt) aşkarlanıb"] = cond_sweep
    if not cond_sweep:
      return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

    if REQUIRE_EQUAL_LEVEL_SWEEP:
      relevant_levels = equal_lows if direction == "bullish" else equal_highs
      cond_eq_sweep = len(relevant_levels) > 0
      conditions["Sweep real Equal High/Low (liquidity pool) yaxınlığındadır"] = cond_eq_sweep
      if not cond_eq_sweep:
        return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

  # --- 5. Order Block (ekstremum-əsaslı + mitigation) ---
  ob = find_order_block_advanced(df, direction, break_idx)
  cond_ob = ob is not None
  conditions["Order Block (OB) tapıldı"] = cond_ob
  if not cond_ob:
    return {"symbol": symbol, "passed": False, "error": None, "conditions": conditions}

  if REQUIRE_OB_UNMITIGATED:
    cond_unmitigated = not ob["mitigated"]
    conditions["Order Block hələ mitigate olunmayıb (etibarlıdır)"] = cond_unmitigated
    if not cond_unmitigated:
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
    bias = f"🟢 LONG ({event_kind}: Bullish OB Retest + Trend Aligned)"
  else:
    sl = round(ob["high"] * 1.003, 4)
    tp = round(liquidity_target, 4)
    bias = f"🔴 SHORT ({event_kind}: Bearish OB Retest + Trend Aligned)"

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
      "event_kind": event_kind,
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
