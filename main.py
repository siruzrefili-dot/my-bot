import os
import time
import json
import math
import signal
import logging
import threading
import traceback
import uuid
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify

BOT_VERSION = "17.2 SMC-ULTRA-FIXED"

class Config:
    BASE_URL = "https://bybit.com"
    CATEGORY = "linear"
    BOT_TOKEN = os.getenv("BOT_TOKEN", "")
    CHAT_ID = os.getenv("CHAT_ID", "1121794078")
    
    # Skan Parametrləri (Bütün Bybit coinləri üçün genişləndirilmiş struktur)
    SCAN_TOP_N = 300           
    CANDIDATE_LIMIT = 300
    CHECK_INTERVAL = 300       # Hər 5 dəqiqədən bir tam skan dövrü
    MONITOR_INTERVAL = 2       # Pozisiyaları hər 2 saniyədən bir yoxlayır
    PARALLEL_WORKERS = 10      # Sürətli skan üçün paralel işçi sayı
    
    # SMC Zaman İntervalları
    TREND_TF = "240"           # 4H Struktur (HTF Trend)
    LIQ_TF = "60"              # 1H Likvidlik (ITF Liquidity Sweep)
    ENTRY_TF = "15"            # 15M Dəqiq Giriş (LTF CHoCH/BOS + OB + FVG)
    
    # SMC Risk və Strategiya Hədəfləri
    MIN_SCORE = 70
    MIN_RR = 3.0               # Professional SMC üçün minimum R:R
    MAX_RR = 6.5
    ACCOUNT_BALANCE = 1132.54  # Cari uğurlu balans
    RISK_PERCENT = 1.          # Hər trade-də maksimum 1% risk
    
    MAX_ACTIVE_SIGNALS = 3     # Eyni anda maksimum 3 aktiv əməliyyat
    MAX_DAILY_SIGNALS = 5
    MAX_SIGNALS_TO_SEND = 2
    
    # Fundamental və Bazar Gücü Filtrləri
    MIN_ATR_PCT = .20          
    MAX_ATR_PCT = 12.
    MAX_ABS_FUNDING = .0020    
    MIN_OI_CHANGE_PCT = 6.0    # Open Interest həqiqi pul süzgəci
    MAX_SPREAD_PCT = .10       
    MAX_HOLD_HOURS = 48
    
    DATA_DIR = "swing_bot_data"
    FLASK_PORT = int(os.getenv("PORT", "10000"))
    REQUEST_TIMEOUT = 15
    
    CACHE_TTL = 8
    TICKER_CACHE_TTL = 15
    INSTRUMENT_CACHE_TTL = 3600
    SIGNAL_FILE = os.path.join(DATA_DIR, "signals.json")
    
    TAKER_FEE_PCT = .055
    SLIPPAGE_PCT = .02
    TRADFI = {"XAU", "XAG", "SPX", "NDX"}

os.makedirs(Config.DATA_DIR, exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("SMC_AI")
STOP_EVENT = threading.Event()
def safe_float(v, d=0.):
    try:
        x = float(v)
        return d if not math.isfinite(x) else x
    except: return d

def utc_now(): return datetime.now(timezone.utc)
def utc_iso(): return utc_now().isoformat()
def valid(df, n): return isinstance(df, pd.DataFrame) and len(df) >= n

def short_tb(tb, maxlen=200):
    try: return " | ".join([x.strip() for x in str(tb).strip().split("\n") if x.strip()][-2:])[:maxlen]
    except: return "?"

class Cache:
    def __init__(self, ttl):
        self.ttl = ttl
        self.data = {}
        self.lock = threading.RLock()
        
    def get(self, k):
        with self.lock:
            x = self.data.get(k)
            if x is None: return None
            if time.time() - x > self.ttl:
                self.data.pop(k, None)
                return None
            return x
            
    def set(self, k, v):
        with self.lock:
            self.data[k] = (v, time.time())
        return v
        
    def clear(self):
        with self.lock:
            self.data.clear()
class BybitClient:
    def __init__(self):
        self.base = Config.BASE_URL
        self.cache = Cache(Config.CACHE_TTL)
        self.tcache = Cache(Config.TICKER_CACHE_TTL)
        self.icache = Cache(Config.INSTRUMENT_CACHE_TTL)
        self.local = threading.local()
        self._lock = threading.Lock()
        self._last = 0.
        
    def session(self):
        if not hasattr(self.local, "session"):
            s = requests.Session()
            s.headers.update({"User-Agent": "SMCAI/17.2"})
            self.local.session = s
        return self.local.session
        
    def _rate(self):
        with self._lock:
            w = 0.02 - (time.time() - self._last)  
            if w > 0: time.sleep(w)
            self._last = time.time()
            
    def get(self, path, params=None, key=None, cache=None, retries=3):
        c = cache or self.cache
        if key:
            z = c.get(key)
            if z is not None: return z
        last = None
        for i in range(max(1, retries)):
            try:
                self._rate()
                r = self.session().get(self.base + path, params=params or {}, timeout=Config.REQUEST_TIMEOUT)
                if r.status_code == 429:
                    time.sleep(1.0 * (i + 1))
                    continue
                r.raise_for_status()
                z = r.json()
                if z.get("retCode") != 0: raise RuntimeError(z.get("retMsg", "API"))
                if key: c.set(key, z)
                return z
            except Exception as e:
                last = e
                if i < retries - 1: time.sleep(.5 * (i + 1))
        return None
        
    def klines(self, symbol, interval, limit=150):
        z = self.get("/v5/market/kline", {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}, f"k:{symbol}:{interval}:{limit}", self.cache)
        rows = z.get("result", {}).get("list", []) if z else []
        if not rows: return pd.DataFrame()
        rows = list(reversed(rows))
        cols = ["timestamp", "open", "high", "low", "close", "volume", "turnover"]
        df = pd.DataFrame(rows, columns=cols)
        for c in cols: df[c] = pd.to_numeric(df[c], errors="coerce")
        return df.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)
        
    def kline_1m_live(self, symbol):
        z = self.get("/v5/market/kline", {"category": "linear", "symbol": symbol, "interval": "1", "limit": 2})
        rows = z.get("result", {}).get("list", []) if z else []
        if not rows or len(rows) == 0: return None
        r = list(reversed(rows))[-1]
        return {
            "ts": int(safe_float(r)), "open": safe_float(r), "high": safe_float(r), 
            "low": safe_float(r), "close": safe_float(r), "volume": safe_float(r)
        }
        
    def ticker(self, symbol, fresh=False):
        c = None if fresh else self.tcache
        k = None if fresh else f"t:{symbol}"
        z = self.get("/v5/market/tickers", {"category": "linear", "symbol": symbol}, k, c)
        a = z.get("result", {}).get("list", []) if z else []
        if not a or len(a) == 0: return {}
        tick = a
        return {"symbol": tick.get("symbol", ""), "last_price": safe_float(tick.get("lastPrice")), "funding": safe_float(tick.get("fundingRate"), 0.), "turnover24h": safe_float(tick.get("turnover24h"))}
        
    def all_tickers(self):
        z = self.get("/v5/market/tickers", {"category": "linear"}, "ALL_T", self.tcache)
        return z.get("result", {}).get("list", []) if z else []
        
    def oi_change(self, symbol):
        z = self.get("/v5/market/open-interest", {"category": "linear", "symbol": symbol, "intervalTime": "1h", "limit": 5}, f"oi:{symbol}")
        rows = z.get("result", {}).get("list", []) if z else []
        if len(rows) < 2: return 0.
        try:
            current_oi = safe_float(rows.get("openInterest"))
            past_oi = safe_float(rows[-1].get("openInterest"))
            if past_oi > 0: return ((current_oi - past_oi) / past_oi) * 100
        except: pass
        return 0.
        
    def book(self, symbol):
        z = self.get("/v5/market/orderbook", {"category": "linear", "symbol": symbol, "limit": 5}, f"b:{symbol}")
        return z.get("result", {}) if z else {}
        
    def instrument(self, symbol):
        z = self.icache.get(symbol)
        if z is not None: return z
        x = self.get("/v5/market/instruments-info", {"category": "linear", "symbol": symbol}, f"i:{symbol}")
        a = x.get("result", {}).get("list", []) if x else []
        if not a or len(a) == 0: return {}
        inst = a; p = inst.get("priceFilter", {}); q = inst.get("lotSizeFilter", {})
        z = {"tick": safe_float(p.get("tickSize")), "step": safe_float(q.get("qtyStep")), "min": safe_float(q.get("minOrderQty")), "status": inst.get("status", ""), "symbol": inst.get("symbol", "")}
        self.icache.set(symbol, z); return z

BYBIT = BybitClient()
class SMC:
    @staticmethod
    def swings(df, left=4, right=4):
        h = df.high.to_numpy(); l = df.low.to_numpy(); n = len(df)
        sh, sl = [], []
        for i in range(left, n - right):
            if h[i] == np.max(h[i-left:i+right+1]) and h[i] > np.max(h[i-left:i]): sh.append((i, float(h[i])))
            if l[i] == np.min(l[i-left:i+right+1]) and l[i] < np.min(l[i-left:i]): sl.append((i, float(l[i])))
        return sh, sl

    @staticmethod
    def analyze_structure(df, sh, sl):
        if not sh or not sl: return {"type": "None", "direction": "neutral", "level": 0., "index": 0}
        lc = df.iloc[-1]; po = df.iloc[-2]
        latest_sh = sh[-1]  
        latest_sl = sl[-1]  
        if lc.close > latest_sh:
            t = "CHoCH" if po.close <= latest_sh else "BOS"
            return {"type": t, "direction": "long", "level": latest_sh, "index": len(df)-1}
        if lc.close < latest_sl:
            t = "CHoCH" if po.close >= latest_sl else "BOS"
            return {"type": t, "direction": "short", "level": latest_sl, "index": len(df)-1}
        return {"type": "None", "direction": "neutral", "level": 0., "index": 0}

    @staticmethod
    def detect_sweep(df, sh, sl, lookback=35):
        n = len(df); lc = df.iloc[-1]
        bull_s = bear_s = False; lvl = None
        for idx, val in reversed(sl):
            if n - idx <= lookback:
                if lc.low < val and lc.close > val: bull_s = True; lvl = val; break
        for idx, val in reversed(sh):
            if n - idx <= lookback:
                if lc.high > val and lc.close < val: bear_s = True; lvl = val; break
        return {"bullish": bull_s, "bearish": bear_s, "level": lvl}

    @staticmethod
    def find_poi(df, struct, lookback=40):
        if struct["type"] == "None": return None
        n = len(df); idx = struct.get("index", n-1); d = struct["direction"]
        start = max(0, idx - lookback)
        fvg = None
        for i in range(n-2, n-15, -1):
            if d == "long" and df.high.iloc[i-1] < df.low.iloc[i+1]:
                fvg = {"top": float(df.low.iloc[i+1]), "bottom": float(df.high.iloc[i-1])}; break
            if d == "short" and df.low.iloc[i-1] > df.high.iloc[i+1]:
                fvg = {"top": float(df.low.iloc[i-1]), "bottom": float(df.high.iloc[i+1])}; break
        ob = None
        for i in range(idx - 1, start, -1):
            if d == "long" and df.close.iloc[i] < df.open.iloc[i]:
                ob = {"top": float(df.high.iloc[i]), "bottom": float(df.low.iloc[i]), "mid": float((df.high.iloc[i]+df.low.iloc[i])/2)}; break
            if d == "short" and df.close.iloc[i] > df.open.iloc[i]:
                ob = {"top": float(df.high.iloc[i]), "bottom": float(df.low.iloc[i]), "mid": float((df.high.iloc[i]+df.low.iloc[i])/2)}; break
        return {"ob": ob, "fvg": fvg}

    @staticmethod
    def calculate_atr(df, period=14):
        high = df['high'].to_numpy(); low = df['low'].to_numpy(); close = df['close'].to_numpy()
        tr = np.zeros(len(df))
        for i in range(1, len(df)):
            tr[i] = max(high[i] - low[i], abs(high[i] - close[i-1]), abs(low[i] - close[i-1]))
        return float(np.mean(tr[-period:]))
class Store:
    def __init__(self):
        self.lock = threading.RLock()
        self.active = []
        self.closed = []
        self.stages = {}
        self.errors = []
        self.day = utc_now().date().isoformat()
        self.daily_count = 0
        self.balance = Config.ACCOUNT_BALANCE
        self.start_balance = Config.ACCOUNT_BALANCE
        
    def reset_day(self):
        d = utc_now().date().isoformat()
        with self.lock:
            if d != self.day: self.day = d; self.daily_count = 0
                
    def add_stage(self, name, ok):
        with self.lock:
            x = self.stages.setdefault(name, {"pass": 0, "fail": 0})
            x["pass" if ok else "fail"] += 1
            
    def stage_report(self):
        with self.lock: q = dict(self.stages)
        return "\n".join(f" 🔹 {k}: {v['pass']}/{v['pass']+v['fail']} keçdi" for k, v in q.items())
        
    def add_error(self, s, err, detail=""):
        with self.lock:
            self.errors = (self.errors + [{"symbol": s, "error": err, "detail": detail, "time": utc_iso()}])[-Config.MAX_ERRORS_KEPT:]
            
    def can_add(self):
        self.reset_day()
        with self.lock: return len(self.active) < Config.MAX_ACTIVE_SIGNALS and self.daily_count < Config.MAX_DAILY_SIGNALS
        
    def add(self, x):
        with self.lock:
            if not self.can_add(): return False
            x["id"] = str(uuid.uuid4())
            x["risk_amount"] = self.balance * Config.RISK_PERCENT / 100
            self.active.append(x)
            self.daily_count += 1
            self.save()
            return True
            
    def close_position(self, sid, result, close_p, rr_val):
        with self.lock:
            idx = next((i for i, a in enumerate(self.active) if a.get("id") == sid), None)
            if idx is None: return None
            a = self.active.pop(idx)
            y = dict(a)
            y.update({"closed_at": utc_iso(), "result": result, "exit_price": close_p, "result_r": rr_val})
            risk_amt = y["risk_amount"]
            net = rr_val * risk_amt
            y["pnl"] = round(net, 2)
            self.balance += net
            self.closed.append(y)
            self.save()
            return y
            
    def save(self):
        try:
            tmp = Config.SIGNAL_FILE + ".tmp"
            with self.lock:
                data = {
                    "active": list(self.active), "closed": list(self.closed),
                    "day": self.day, "daily_count": self.daily_count,
                    "balance": self.balance, "start_balance": self.start_balance
                }
            with open(tmp, "w", encoding="utf8") as f: json.dump(data, f, indent=2)
            os.replace(tmp, Config.SIGNAL_FILE)
        except Exception as e: log.warning("Fayl qeyd xətası: %s", e)
        
    def load(self):
        try:
            if os.path.exists(Config.SIGNAL_FILE):
                with open(Config.SIGNAL_FILE, encoding="utf8") as f: x = json.load(f)
                self.active = x.get("active", [])
                self.closed = x.get("closed", [])
                self.balance = x.get("balance", Config.ACCOUNT_BALANCE)
        except Exception as e: log.warning("Yükləmə xətası: %s", e)

STORE = Store(); STORE.load()
class Analyzer:
    def __init__(self, symbol): self.symbol = symbol
    def run(self):
        s = self.symbol
        d4 = BYBIT.klines(s, Config.TREND_TF, 120)
        d1 = BYBIT.klines(s, Config.LIQ_TF, 120)
        d15 = BYBIT.klines(s, Config.ENTRY_TF, 120)
        
        if not (valid(d4, 60) and valid(d1, 60) and valid(d15, 60)): return None
        
        # 1. 4H Zaman İntervalında Trend təyini (HTF BOS/CHoCH)
        sh4, sl4 = SMC.swings(d4)
        struct4 = SMC.analyze_structure(d4, sh4, sl4)
        STORE.add_stage("4H_MARKET_STRUCTURE", struct4["direction"] != "neutral")
        if struct4["direction"] == "neutral": return None
        direction = struct4["direction"]
        
        # 2. 1H Zaman intervalında Likvidlik Süpürülməsi (ITF Sweep)
        sh1, sl1 = SMC.swings(d1)
        sweep1 = SMC.detect_sweep(d1, sh1, sl1)
        if direction == "long" and not sweep1["bullish"]: return None
        if direction == "short" and not sweep1["bearish"]: return None
        STORE.add_stage("1H_LIQUIDITY_SWEEP", True)
        
        # 3. 15M Zaman intervalında LTF qırılma təsdiqi
        sh15, sl15 = SMC.swings(d15)
        struct15 = SMC.analyze_structure(d15, sh15, sl15)
        if struct15["type"] == "None" or struct15["direction"] != direction: return None
        STORE.add_stage("15M_LTF_CONFIRMATION", True)
        
        poi = SMC.find_poi(d15, struct15)
        if not poi or not poi["ob"]: return None
        ob = poi["ob"]
        
        # 4. Fundamental Verilənlər Kontrolu (OI pul axını)
        t = BYBIT.ticker(s, True)
        live_p = safe_float(t.get("last_price"))
        funding = safe_float(t.get("funding"))
        oi_inc = BYBIT.oi_change(s)
        
        if live_p <= 0 or abs(funding) > Config.MAX_ABS_FUNDING: return None
        if oi_inc < Config.MIN_OI_CHANGE_PCT: return None 
        STORE.add_stage("FUNDAMENTAL_OI_AND_FUNDING", True)
        
        # Giriş Order Block təpəsindən, Dar SL və Geniş SMC Hədəfi
        atr = SMC.calculate_atr(d15)
        if direction == "long":
            entry = ob["top"]
            sl = ob["bottom"] - (atr * 0.12)
            tp = entry + abs(entry - sl) * 3.5
        else:
            entry = ob["bottom"]
            sl = ob["top"] + (atr * 0.12)
            tp = entry - abs(sl - entry) * 3.5
            
        risk = abs(entry - sl)
        if risk <= 0: return None
        rr = abs(tp - entry) / risk
        
        if not (Config.MIN_RR <= rr <= Config.MAX_RR): return None
        STORE.add_stage("SMC_OPTIMAL_RR_RATIO", True)
        
        return {
            "symbol": s, "direction": direction, "score": 90, 
            "entry": entry, "sl": sl, "tp": tp, "rr": rr, 
            "risk": risk, "oi_growth": oi_inc, "funding": funding, "created_ts": time.time()
        }

def analyze_symbol(s):
    try: return Analyzer(s).run()
    except Exception as e:
        tb = traceback.format_exc(); STORE.add_error(s, str(e), short_tb(tb)); return None
class Scanner:
    def __init__(self): 
        self.lock = threading.Lock()
        self.running = False
    def scan(self, force=False):
        if not self.lock.acquire(False): return
        self.running = True
        try:
            if not STORE.can_add(): return
            tickers = BYBIT.all_tickers()
            symbols = [x["symbol"] for x in tickers if x.get("symbol", "").endswith("USDT") and x.get("symbol", "")[:-4] not in Config.TRADFI][:Config.SCAN_TOP_N]
            if not symbols: symbols = Config.FALLBACK_COINS
            
            found = []
            with ThreadPoolExecutor(max_workers=Config.PARALLEL_WORKERS) as ex:
                fs = {ex.submit(analyze_symbol, s): s for s in symbols}
                for f in as_completed(fs):
                    res = f.result()
                    if res: found.append(res)
            for x in found:
                if STORE.add(x):
                    Telegram.signal(x); break
        finally:
            self.running = False
            if self.lock.locked(): self.lock.release()

SCANNER = Scanner()

class Telegram:
    @staticmethod
    def send(text):
        if not Config.BOT_TOKEN: return False
        try: return requests.post(f"https://telegram.org{Config.BOT_TOKEN}/sendMessage", json={"chat_id": Config.CHAT_ID, "text": text}, timeout=10).ok
        except Exception as e: return False
        
    @staticmethod
    def signal(x):
        d = "🟢 INST_BUY (OB LONG)" if x["direction"] == "long" else "🔴 INST_SELL (OB SHORT)"
        body = (f"🚨 SMC SİQNAL #{STORE.daily_count}\n━━━━━━━━━━━━━━━━━━\n📌 Aktiv: {x['symbol']}  {d}\n━━━━━━━━━━━━━━━━━━\n🎯 Skan Hesabı: {x['score']}/100\n📊 Real OI Artımı: +{x['oi_growth']:.1f}%\n🪙 Funding Rate: {x['funding']:.5f}\n💰 Giriş (OB): {x['entry']:.5g}\n🛑 Dar SL: {x['sl']:.5g}\n✅ Hədəf TP: {x['tp']:.5g}\n⚖️ Risk/Mükafat: 1:{x['rr']:.2f}\n💵 Risk Miqdarı: {x['risk_amount']:.2f} USDT\n━━━━━━━━━━━━━━━━━━\n⚠️ Smart Money Flow")
        return Telegram.send(body)
        
    @staticmethod
    def closed(y):
        em = "✅ HƏDƏF TP VURULDU!" if y.get("result") == "TP" else "🔴 STOP LOSS VURULDU"
        return Telegram.send(f"{em}\n🏷 {y['symbol']} | {y['direction'].upper()}\nNet Mənfəət: {y['result_r']:.2f}R\nPnL: {y['pnl']:.2f} USDT\nCari Balans: {STORE.balance:.2f} USDT")
        
    @staticmethod
    def status():
        with STORE.lock: a = list(STORE.active)
        if not a: return "📭 Hazırda aktiv hər hansı SMC pozisiya yoxdur."
        return "📋 AKTİV SMC POZİSİYALARI:\n" + "\n".join([f"📌 {x['symbol']} {x['direction'].upper()} | Entry: {x['entry']} | SL: {x['sl']} | TP: {x['tp']}" for x in a])

class PositionManager:
    def check(self, x):
        k = BYBIT.kline_1m_live(x["symbol"])
        if not k: return
        hi, lo = k["high"], k["low"]
        sid = x["id"]; d = x["direction"]; sl = x["sl"]; tp = x["tp"]; rr = x["rr"]
        if d == "long":
            if lo <= sl:
                y = STORE.close_position(sid, "SL", sl, -1.0)
                if y: Telegram.closed(y)
            elif hi >= tp:
                y = STORE.close_position(sid, "TP", tp, rr)
                if y: Telegram.closed(y)
        else:
            if hi >= sl:
                y = STORE.close_position(sid, "SL", sl, -1.0)
                if y: Telegram.closed(y)
            elif lo <= tp:
                y = STORE.close_position(sid, "TP", tp, rr)
                if y: Telegram.closed(y)
                
    def run(self):
        while not STOP_EVENT.is_set():
            try:
                for x in list(STORE.active): self.check(x)
            except Exception: pass
            STOP_EVENT.wait(Config.MONITOR_INTERVAL)
class TelegramPoller:
    def run(self):
        if not Config.BOT_TOKEN: return
        url = f"https://telegram.org{Config.BOT_TOKEN}/getUpdates"
        offset = 0
        while not STOP_EVENT.is_set():
            try:
                r = requests.get(url, params={"timeout": 15, "offset": offset}, timeout=20).json()
                for u in r.get("result", []):
                    offset = u["update_id"] + 1
                    m = u.get("message", {})
                    if str(m.get("chat", {}).get("id", "")) != str(Config.CHAT_ID): continue
                    txt = m.get("text", "").strip().lower()
                    if txt == "/scan":
                        Telegram.send("🔎 BÜTÜN BYBİT COİNLƏRİ üçün İnstitusional SMC və OI skaneri başladııldı... (Bu proses fond dərinliyinə görə 10-15 saniyə çəkə bilər)")
                        threading.Thread(target=SCANNER.scan, kwargs={"force": True}, daemon=True).start()
                    elif txt in ["/status", "/signals"]: Telegram.send(Telegram.status())
                    elif txt == "/stats": Telegram.send(f"📊 SMC ULTIMATE BOT STATS\n💵 Cari Balans: {STORE.balance:.2f} USDT\n📈 Aktiv Pozisiyalar: {len(STORE.active)}\n📋 Müasir Skan Süzgəcləri:\n{STORE.stage_report()}")
            except: pass
            STOP_EVENT.wait(2)

app = Flask(__name__)
@app.get("/")
def home(): return jsonify({"status": "running", "bot": "SMC-ULTRA", "total_pairs_scanned": Config.SCAN_TOP_N, "balance": round(STORE.balance, 2)})

def main():
    Telegram.send(f"🤖 SMC-INSTITUTIONAL v{BOT_VERSION} Başladıldı!\n🔍 Texniki: 4H Market Structure -> 1H Liquidity Sweep -> 15M Order Block / FVG\n📊 Fundamental: Open Interest Flow & Funding Süzgəcləri ilə Bütün Bybit Perps Skanı Tamamilə Aktivdir.")
    threading.Thread(target=lambda: app.run(host="0.0.0.0", port=Config.FLASK_PORT, debug=False, use_reloader=False), daemon=True).start()
    threading.Thread(target=PositionManager().run, daemon=True).start()
    threading.Thread(target=TelegramPoller().run, daemon=True).start()
    while not STOP_EVENT.is_set():
        SCANNER.scan()
        STOP_EVENT.wait(Config.CHECK_INTERVAL)

if __name__ == "__main__":
    main()
