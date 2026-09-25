# ============================================
# SWING AI v14.0 — FIXED + IMPROVED
# HİSSƏ 1/8 — Imports + Config + Utils + Cache
# ============================================
import os,time,json,math,signal,logging,threading,traceback,uuid
import requests,numpy as np,pandas as pd
from datetime import datetime,timezone
from concurrent.futures import ThreadPoolExecutor,as_completed
from flask import Flask,jsonify

BOT_VERSION="14.0 FIXED + IMPROVED"

class Config:
 BOT_TOKEN=os.getenv("BOT_TOKEN","");CHAT_ID=os.getenv("CHAT_ID","1121794078")
 BASE_URL="https://api.bybit.com";CATEGORY="linear"
 SCAN_TOP_N=40;CANDIDATE_LIMIT=80;CHECK_INTERVAL=300;MONITOR_INTERVAL=5;PARALLEL_WORKERS=3
 TREND_TF="240";SETUP_TF="60";ENTRY_TF="15"
 EMA_FAST=50;EMA_SLOW=200;RSI_PERIOD=14;ATR_PERIOD=14;VOLUME_LOOKBACK=20
 MIN_SCORE=60;MIN_RR=2.2;MAX_RR=3.5
 ACCOUNT_BALANCE=1000.0;RISK_PERCENT=1.0
 # GÜNCƏLLƏMƏ: gündəlik tavan 2->3, amma OVERRIDE mexanizmi artıq lazım
 # deyil (tavan onsuz da 3-dür), söndürülüb.
 MAX_ACTIVE_SIGNALS=2;MAX_DAILY_SIGNALS=3;MAX_SIGNALS_TO_SEND=1
 # GÜNCƏLLƏMƏ: ardıcıl artan eşik — 1-ci ≥85, 2-ci ≥90, 3-cü ≥92 (90-dan da
 # yuxarı). Gün ərzində növbəti hər siqnal əvvəlkindən güclü olmalıdır.
 FIRST_SIGNAL_SCORE=85;SECOND_SIGNAL_SCORE=90;THIRD_SIGNAL_SCORE=92
 SIGNAL_SCORE_THRESHOLDS=[FIRST_SIGNAL_SCORE,SECOND_SIGNAL_SCORE,THIRD_SIGNAL_SCORE]
 OVERRIDE_SCORE=95;MAX_OVERRIDE_DAILY=0  # 0 = override sondurulub
 # FIX: TIER_B_SCORE=999 tier B-ni statistik olaraq mumkunsuz edirdi (hec vaxt
 # cixmirdi). Indi menali araliqda -- reportlarda B tier real gorunecek.
 # Qeyd: gonderilen siqnallar yene YALNIZ A tier-den secilir, bu deyer sirf
 # hesabat/stat meqsedlidir.
 TIER_A_SCORE=72;TIER_B_SCORE=66;MIN_RR_ELITE=2.4
 MIN_ATR_PCT=0.20;MAX_ATR_PCT=10.0
 MIN_VOLUME_RATIO=1.00;VOLUME_WINDOW=5
 MIN_EMA_DISTANCE_PCT=0.09
 LONG_RSI_MIN=42.0;LONG_RSI_MAX=68.0
 SHORT_RSI_MIN=32.0;SHORT_RSI_MAX=58.0
 MIN_SL_ATR=0.80;MAX_SL_ATR=2.50;SL_BUFFER_ATR=0.15
 BOS_BUFFER_ATR=0.05;ALLOW_RECLAIM_BOS=True;BOS_LOOKBACK=100
 RETEST_MAX_BARS=15;RETEST_ATR_DISTANCE=0.90
 CONFIRMATION_MAX_BARS_AFTER_RETEST=5;MAX_CONFIRM_AGE=10
 MAX_ENTRY_EXTENSION_ATR=3.5
 TARGET_LOOKBACK_15=80;TARGET_LOOKBACK_1H=80;TARGET_LOOKBACK_4H=80
 TARGET_BUFFER_ATR=0.10
 MAX_ABS_FUNDING=0.003;MIN_OI_CHANGE_PCT=-10.0;OI_LOOKBACK=5;MAX_SPREAD_PCT=0.20
 BTC_FILTER_ENABLED=True;CORRELATION_FILTER_ENABLED=False
 MAX_CORRELATED_ACTIVE=2;CORRELATION_THRESHOLD=0.85
 SCAN_HOUR_START=9;SCAN_HOUR_END=20;SCAN_HOURS_ENABLED=True
 DEBUG_SYMBOLS_ENABLED=False
 MAX_HOLD_HOURS=96;DATA_DIR="swing_bot_data"
 FLASK_PORT=int(os.getenv("PORT","10000"))
 REQUEST_TIMEOUT=15;CACHE_TTL=10;TICKER_CACHE_TTL=30;INSTRUMENT_CACHE_TTL=3600
 REGIME_CACHE_TTL=60
 MAX_CLOSED_SIGNALS_KEPT=500;MAX_ERRORS_KEPT=50
 SIGNAL_FILE=os.path.join(DATA_DIR,"signals.json")
 TAKER_FEE_PCT=0.055
 SLIPPAGE_PCT=0.02
 SCORE_W_REGIME=0.20;SCORE_W_SETUP=0.10;SCORE_W_PULLBACK=0.10
 SCORE_W_BOS=0.12;SCORE_W_RETEST=0.10;SCORE_W_CONFIRM=0.12
 SCORE_W_RSI=0.08;SCORE_W_VOLUME=0.06;SCORE_W_ZONE=0.05
 SCORE_W_BTC=0.03;SCORE_W_RR=0.04
 FALLBACK_COINS=["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT","ADAUSDT",
                 "AVAXUSDT","DOGEUSDT","LINKUSDT","SUIUSDT","ARBUSDT","OPUSDT"]
 TRADFI={"XAU","XAG","CL","USOIL","UKOIL","SPX","NDX","DJI","US30","NAS100","US500",
         "GER40","UK100","JP225","HK50","AAPL","AMZN","AMD","COIN","GOOG","GOOGL",
         "META","MSFT","MSTR","MU","NFLX","NVDA","ORCL","PLTR","QCOM","TSLA","TSM",
         "HOOD","SMCI","RKLB","OPENAI","NBIS","HPE","ANTHROPIC","CRWV","ADBE",
         "NOKIA","LITE","ASTS","AEHR","ALAB","COHR","CIEN","POET","BNC","AXTI",
         "FLNC","CRCL","BE","ONDS","CBRS","PURR","STXX","QNTX","SKHYNIX","SNDK","SOXL"}

os.makedirs(Config.DATA_DIR,exist_ok=True)
logging.basicConfig(level=logging.INFO,format="%(asctime)s | %(levelname)s | %(message)s")
log=logging.getLogger("SWING_AI");STOP_EVENT=threading.Event()

def safe_float(v,d=0.0):
 try:
  x=float(v)
  return d if not math.isfinite(x) else x
 except:return d

def utc_now():return datetime.now(timezone.utc)
def utc_iso():return utc_now().isoformat()
def valid(df,n):return isinstance(df,pd.DataFrame) and len(df)>=n

def short_tb(tb,maxlen=200):
 try:
  lines=[l.strip() for l in tb.strip().split("\n") if l.strip()]
  return " | ".join(lines[-2:])[:maxlen]
 except:return "?"

class Cache:
 def __init__(self,ttl):self.ttl=ttl;self.data={};self.lock=threading.RLock()
 def get(self,k):
  with self.lock:
   x=self.data.get(k)
   if not x:return None
   if time.time()-x[1]>self.ttl:self.data.pop(k,None);return None
   return x[0]
 def set(self,k,v):
  with self.lock:self.data[k]=(v,time.time())

# ============================================
# HİSSƏ 2/8 — BybitClient + validate_config
# ============================================
class BybitClient:
 def __init__(self):
  self.base=Config.BASE_URL
  self.cache=Cache(Config.CACHE_TTL)
  self.tcache=Cache(Config.TICKER_CACHE_TTL)
  self.icache=Cache(Config.INSTRUMENT_CACHE_TTL)
  # FIX: BTC 4H rejim datasi ucun ayrica, daha uzun omurlu (60s) kes.
  # Evvelki versiyada BTCFilter her simvol ucun umumi 10s-lik kesi
  # istifade edirdi, bu da bir scan ereine icinde BTC sorgusunun defelerle
  # tekrarlanmasina sebeb olurdu.
  self.regime_cache=Cache(Config.REGIME_CACHE_TTL)
  self.local=threading.local();self._lock=threading.Lock();self._last=0.0
 def session(self):
  if not hasattr(self.local,"session"):
   s=requests.Session();s.headers.update({"User-Agent":"SwingAI/14.0"})
   self.local.session=s
  return self.local.session
 def _rate(self):
  with self._lock:
   w=0.10-(time.time()-self._last)
   if w>0:time.sleep(w)
   self._last=time.time()
 def get(self,path,params=None,key=None,cache=None,retries=3):
  c=cache or self.cache
  if key:
   z=c.get(key)
   if z is not None:return z
  last=None
  for i in range(retries):
   try:
    self._rate()
    r=self.session().get(self.base+path,params=params or {},timeout=Config.REQUEST_TIMEOUT)
    if r.status_code==429:time.sleep(1.5*(i+1));continue
    r.raise_for_status();z=r.json()
    if z.get("retCode")!=0:raise RuntimeError(z.get("retMsg","API"))
    if key:c.set(key,z)
    return z
   except Exception as e:
    last=e
    if i<retries-1:time.sleep(0.8*(i+1))
  log.warning("API %s: %s",path,last);return None
 def klines(self,symbol,interval,limit=300,cache=None):
  z=self.get("/v5/market/kline",{"category":"linear","symbol":symbol,
    "interval":interval,"limit":limit},f"k:{symbol}:{interval}:{limit}",cache)
  rows=z.get("result",{}).get("list",[]) if z else []
  if not rows:return pd.DataFrame()
  rows=list(reversed(rows))
  cols=["timestamp","open","high","low","close","volume","turnover"]
  df=pd.DataFrame(rows,columns=cols)
  for c in cols:df[c]=pd.to_numeric(df[c],errors="coerce")
  df=df.dropna(subset=["open","high","low","close","volume"])
  return df.iloc[:-1].reset_index(drop=True) if len(df)>1 else df.reset_index(drop=True)
 def kline_1m_live(self,symbol):
  z=self.get("/v5/market/kline",{"category":"linear","symbol":symbol,
    "interval":"1","limit":2})
  rows=z.get("result",{}).get("list",[]) if z else []
  if not rows:return None
  rows=list(reversed(rows))
  if not rows:return None
  r=rows[-1]
  return {"ts":int(safe_float(r[0])),
          "open":safe_float(r[1]),"high":safe_float(r[2]),
          "low":safe_float(r[3]),"close":safe_float(r[4]),
          "volume":safe_float(r[5])}
 def ticker(self,symbol,fresh=False):
  c=None if fresh else self.tcache;k=None if fresh else f"t:{symbol}"
  z=self.get("/v5/market/tickers",{"category":"linear","symbol":symbol},k,c)
  a=z.get("result",{}).get("list",[]) if z else []
  if not a:return {}
  a=a[0]
  return {"symbol":a.get("symbol",""),"last_price":safe_float(a.get("lastPrice")),
    "change":safe_float(a.get("price24hPcnt"))*100,
    "funding":safe_float(a.get("fundingRate"),math.nan),
    "oi":safe_float(a.get("openInterest"),math.nan),
    "turnover24h":safe_float(a.get("turnover24h"))}
 def all_tickers(self):
  z=self.get("/v5/market/tickers",{"category":"linear"},"ALL_T",self.tcache)
  return z.get("result",{}).get("list",[]) if z else []
 def oi(self,symbol,limit=10):
  z=self.get("/v5/market/open-interest",{"category":"linear","symbol":symbol,
    "intervalTime":"1h","limit":limit},f"oi:{symbol}:{limit}")
  rows=z.get("result",{}).get("list",[]) if z else []
  if not rows:return pd.DataFrame()
  df=pd.DataFrame(rows)
  for c in ["openInterest","singleOpenInterest","timestamp"]:
   if c in df:df[c]=pd.to_numeric(df[c],errors="coerce")
  col="singleOpenInterest" if ("singleOpenInterest" in df and
    df["singleOpenInterest"].notna().sum()>=2) else "openInterest"
  if col not in df:return pd.DataFrame()
  return df.rename(columns={col:"oi_value"}).sort_values("timestamp").reset_index(drop=True)
 def book(self,symbol):
  z=self.get("/v5/market/orderbook",{"category":"linear","symbol":symbol,"limit":25},
    f"b:{symbol}")
  return z.get("result",{}) if z else {}
 def instrument(self,symbol):
  z=self.icache.get(symbol)
  if z is not None:return z
  x=self.get("/v5/market/instruments-info",{"category":"linear","symbol":symbol},
    f"i:{symbol}")
  a=x.get("result",{}).get("list",[]) if x else []
  if not a:return {}
  a=a[0];p=a.get("priceFilter",{});q=a.get("lotSizeFilter",{})
  z={"tick":safe_float(p.get("tickSize")),"step":safe_float(q.get("qtyStep")),
    "min":safe_float(q.get("minOrderQty")),"status":a.get("status",""),
    "contractType":a.get("contractType",""),"quoteCoin":a.get("quoteCoin",""),
    "settleCoin":a.get("settleCoin",""),"baseCoin":a.get("baseCoin",""),
    "symbol":a.get("symbol","")}
  self.icache.set(symbol,z);return z

BYBIT=BybitClient()

def validate_config():
 if not 0<=Config.MIN_SCORE<=100 or Config.MIN_RR<=0 or Config.MAX_RR<Config.MIN_RR:
  raise ValueError("score/rr")
 if Config.RISK_PERCENT<=0:raise ValueError("risk")
 if not (Config.LONG_RSI_MIN<Config.LONG_RSI_MAX and Config.SHORT_RSI_MIN<Config.SHORT_RSI_MAX):
  raise ValueError("rsi")
 if not (0<Config.MIN_SL_ATR<Config.MAX_SL_ATR):raise ValueError("sl_atr")
 # FIX: TIER_B_SCORE artiq menali oldugu ucun burda da dogrulanir
 if not (Config.MIN_SCORE<=Config.TIER_B_SCORE<Config.TIER_A_SCORE):raise ValueError("tier")
 if not (Config.FIRST_SIGNAL_SCORE>=Config.TIER_A_SCORE):raise ValueError("first")
 if not (Config.OVERRIDE_SCORE>=Config.FIRST_SIGNAL_SCORE):raise ValueError("override")
 # FIX: evvelki versiyada bu yoxlamalar heç yox idi -- yanlis config ile bot
 # sukutla sehv isleye bilerdi (mes. EMA_FAST>=EMA_SLOW, menfi interval ve s.)
 if Config.EMA_FAST>=Config.EMA_SLOW:raise ValueError("ema")
 if Config.MAX_ACTIVE_SIGNALS<=0 or Config.MAX_DAILY_SIGNALS<=0:raise ValueError("limits")
 if Config.MONITOR_INTERVAL<=0 or Config.CHECK_INTERVAL<=0:raise ValueError("intervals")
 if Config.MAX_SIGNALS_TO_SEND<=0:raise ValueError("send_limit")
 if not Config.SIGNAL_SCORE_THRESHOLDS:raise ValueError("thresholds")

# ============================================
# HİSSƏ 3/8 — Indicators + Structure
# ============================================
class Indicators:
 @staticmethod
 def ema(s,n):return s.ewm(span=n,adjust=False).mean()
 @staticmethod
 def rsi(s,n=14):
  d=s.diff();g=d.clip(lower=0);l=-d.clip(upper=0)
  ag=g.ewm(alpha=1/n,adjust=False).mean();al=l.ewm(alpha=1/n,adjust=False).mean()
  rs=ag/al.replace(0,np.nan);r=100-100/(1+rs)
  r=r.where(al!=0,100);r=r.where(~((ag==0)&(al==0)),50);return r
 @staticmethod
 def atr(df,n=14):
  pc=df.close.shift(1)
  tr=pd.concat([df.high-df.low,(df.high-pc).abs(),(df.low-pc).abs()],axis=1).max(axis=1)
  return tr.ewm(alpha=1/n,adjust=False).mean()
 @staticmethod
 def add(df):
  x=df.copy()
  x["ema_fast"]=Indicators.ema(x.close,Config.EMA_FAST)
  x["ema_slow"]=Indicators.ema(x.close,Config.EMA_SLOW)
  x["rsi"]=Indicators.rsi(x.close,Config.RSI_PERIOD)
  x["atr"]=Indicators.atr(x,Config.ATR_PERIOD)
  x["volume_ma"]=x.volume.rolling(Config.VOLUME_LOOKBACK).mean()
  x["volume_ratio"]=x.volume/x.volume_ma.replace(0,np.nan)
  x["ema_distance_pct"]=(x.ema_fast-x.ema_slow)/x.close*100
  x["atr_pct"]=x.atr/x.close*100
  return x

class Structure:
 # FIX: evvelki versiyada cache_key hec bir cagirisdan otururulmurdu, ona gore
 # kes hemise bos qalirdi ve swings() eyni df uzerinde bir nece defe (BOS,
 # structural_stop, target_candidates) tekrar-tekrar hesablanirdi. Indi kes
 # avtomatik olaraq (df-in identity-si + uzunlugu) uzerinden isleyir -- her
 # cagiris ucun ayrica cache_key otururmeye ehtiyac qalmir.
 _cache={};_lock=threading.RLock()
 @staticmethod
 def swings(df,l=5,r=5):
  key=(id(df),len(df))
  with Structure._lock:
   c=Structure._cache.get(key)
   if c is not None:return c
  h=df.high.to_numpy();lo=df.low.to_numpy();sh=[];sl=[];n=len(df)
  for i in range(l,n-r):
   if h[i]>=max(h[i-l:i]) and h[i]>max(h[i+1:i+r+1]):sh.append((i,float(h[i])))
   if lo[i]<=min(lo[i-l:i]) and lo[i]<min(lo[i+1:i+r+1]):sl.append((i,float(lo[i])))
  result=(sh,sl)
  with Structure._lock:
   Structure._cache[key]=result
   if len(Structure._cache)>200:
    for k in list(Structure._cache.keys())[:50]:Structure._cache.pop(k,None)
  return result
 @staticmethod
 def trend(df):
  sh,sl=Structure.swings(df)
  if len(sh)<2 or len(sl)<2:return "neutral"
  if sh[-1][1]>sh[-2][1] and sl[-1][1]>sl[-2][1]:return "bullish"
  if sh[-1][1]<sh[-2][1] and sl[-1][1]<sl[-2][1]:return "bearish"
  return "neutral"

# ============================================
# HİSSƏ 4/8 — Regime + Strategy
# (funksional bug tapilmadi -- oldugu kimi saxlanildi)
# ============================================
class Regime:
 @staticmethod
 def analyze(df):
  if len(df)<220:return {"direction":"neutral","quality":0,"structure":"neutral"}
  x=df.iloc[-1];st=Structure.trend(df)
  dist=abs(safe_float(x.ema_distance_pct));atr_pct=safe_float(x.atr_pct)
  eb=x.close>x.ema_slow and x.ema_fast>x.ema_slow
  es=x.close<x.ema_slow and x.ema_fast<x.ema_slow
  if not (eb or es):return {"direction":"neutral","quality":25,"structure":st}
  if dist<Config.MIN_EMA_DISTANCE_PCT:return {"direction":"neutral","quality":35,"structure":st}
  if eb:d="long";sm=(st=="bullish");so=(st=="bearish")
  else:d="short";sm=(st=="bearish");so=(st=="bullish")
  q=min(dist/1.5,1.0)*40+min(atr_pct/2.0,1.0)*20+(40 if sm else (0 if so else 25))
  return {"direction":d,"quality":min(100,max(0,q)),"structure":st}

class Strategy:
 @staticmethod
 def setup(df,d):
  if len(df)<100:return False
  x=df.iloc[-1]
  if d=="long":return bool(x.ema_fast>x.ema_slow or x.close>x.ema_slow)
  return bool(x.ema_fast<x.ema_slow or x.close<x.ema_slow)
 @staticmethod
 def pullback(df,d):
  if len(df)<8:return False
  x=df.iloc[-7:-1]
  ema=safe_float(df.ema_fast.iloc[-1]);atr=safe_float(df.atr.iloc[-1])
  if atr<=0:return False
  if d=="long":return bool((x.low<=ema+atr*1.5).any())
  return bool((x.high>=ema-atr*1.5).any())
 @staticmethod
 def rsi_allowed(df,d):
  r=safe_float(df.rsi.iloc[-1],math.nan)
  if not math.isfinite(r):return False
  if d=="long":return Config.LONG_RSI_MIN<=r<=Config.LONG_RSI_MAX
  return Config.SHORT_RSI_MIN<=r<=Config.SHORT_RSI_MAX
 @staticmethod
 def rsi_score(df,d):
  r=safe_float(df.rsi.iloc[-1],math.nan)
  if not math.isfinite(r):return 0
  if d=="long":
   if 48<=r<=58:return 100
   if Config.LONG_RSI_MIN<=r<48 or 58<r<=62:return 80
   return 65
  if 42<=r<=52:return 100
  if 38<=r<42 or 52<r<=55:return 80
  return 65
 @staticmethod
 def strong(df,i,d):
  if i<1 or i>=len(df):return False
  x=df.iloc[i];p=df.iloc[i-1];atr=safe_float(x.atr)
  if atr<=0 or abs(x.close-x.open)<atr*0.25:return False
  if d=="long":return bool(x.close>x.open and x.close>=p.close and x.close>=x.high-atr*0.30)
  return bool(x.close<x.open and x.close<=p.close and x.close<=x.low+atr*0.30)
 @staticmethod
 def bos(df,d):
  sh,sl=Structure.swings(df);n=len(df)
  sw=sh if d=="long" else sl;cut=max(0,n-Config.BOS_LOOKBACK-15);cands=[]
  for si,lv in sw:
   if si<cut or si>=n-3:continue
   for i in range(si+1,n):
    atr=safe_float(df.atr.iloc[i],math.nan)
    if not math.isfinite(atr) or atr<=0:continue
    x=df.iloc[i];buf=atr*Config.BOS_BUFFER_ATR
    if d=="long":
     cb=x.close>=lv+buf
     rc=(Config.ALLOW_RECLAIM_BOS and Strategy.strong(df,i,d) and x.close>=lv and x.high>=lv)
    else:
     cb=x.close<=lv-buf
     rc=(Config.ALLOW_RECLAIM_BOS and Strategy.strong(df,i,d) and x.close<=lv and x.low<=lv)
    if cb or rc:
     cands.append({"level":lv,"bar":i,"swing":si,"type":"CLOSE_BOS" if cb else "RECLAIM_BOS"});break
  if not cands:return None
  best=max(cands,key=lambda z:z["bar"]);age=n-1-best["bar"]
  max_age=Config.RETEST_MAX_BARS+Config.CONFIRMATION_MAX_BARS_AFTER_RETEST+5
  return best if age<=max_age else None
 @staticmethod
 def sequence(df,bos,d):
  if not bos:return None
  lv=bos["level"];start=bos["bar"]+1
  end=min(len(df),start+Config.RETEST_MAX_BARS+1)
  # FIX (RETEST_CONFIRM darboğazı): evvelki versiyada "toxunma" (touch) ve
  # "geri tutma" (hold) EYNI sam daxilinde bas vermeli idi. Real bazarda
  # retest cox vaxt bir nece sam erzinde formalasir (bir sam asagi enib
  # bagliyir, sonraki sam geri qayidir). Indi touch herhansi bir samda bas
  # verende yadda saxlanilir, hold ise HERHANSI SONRAKI samda (eyni ve ya
  # daha sonraki) yoxlanilir -- RETEST_ATR_DISTANCE ve RETEST_MAX_BARS
  # deyerlerine toxunulmayib, yalniz iki serti eyni sama baglayan qeyd goturulub.
  touch_idx=None
  for i in range(start,end):
   x=df.iloc[i];atr=safe_float(x.atr)
   if atr<=0:continue
   if d=="long":
    touched=x.low<=lv+atr*Config.RETEST_ATR_DISTANCE
    held=x.close>=lv
   else:
    touched=x.high>=lv-atr*Config.RETEST_ATR_DISTANCE
    held=x.close<=lv
   if touched:touch_idx=i
   if touch_idx is None or not held:continue
   for j in range(i+1,min(len(df),i+1+Config.CONFIRMATION_MAX_BARS_AFTER_RETEST+1)):
    if Strategy.strong(df,j,d):
     if d=="long" and df.close.iloc[j]>lv:
      return {"bos":bos["bar"],"retest":touch_idx,"confirm":j,"level":lv}
     if d=="short" and df.close.iloc[j]<lv:
      return {"bos":bos["bar"],"retest":touch_idx,"confirm":j,"level":lv}
  return None
 @staticmethod
 def zone_score(df,d):
  hi=df.high.iloc[-50:].max();lo=df.low.iloc[-50:].min()
  mid=(hi+lo)/2;p=df.close.iloc[-1];atr=df.atr.iloc[-1]
  if atr<=0:return 0
  if d=="long":
   if p<=mid:return 100
   if p<=mid+atr:return 70
   return 35
  if p>=mid:return 100
  if p>=mid-atr:return 70
  return 35

# ============================================
# HİSSƏ 5/8 — Filters + BTCFilter + Correlation + Risk
# ============================================
class Filters:
 @staticmethod
 def volatility(df):
  if not valid(df,30):return False
  a=safe_float(df.atr_pct.iloc[-1],math.nan)
  return math.isfinite(a) and Config.MIN_ATR_PCT<=a<=Config.MAX_ATR_PCT
 @staticmethod
 def volume(df,lookback=None):
  lb=lookback or Config.VOLUME_WINDOW
  if not valid(df,Config.VOLUME_LOOKBACK+2):return False
  v=safe_float(df.volume_ratio.iloc[-lb:].max(),math.nan)
  return math.isfinite(v) and v>=Config.MIN_VOLUME_RATIO
 @staticmethod
 def volume_score(df,confirm_idx=None):
  if not valid(df,Config.VOLUME_LOOKBACK+2):return 0
  if confirm_idx is not None and 0<=confirm_idx<len(df):
   vr=safe_float(df.volume_ratio.iloc[confirm_idx],math.nan)
  else:
   vr=safe_float(df.volume_ratio.iloc[-Config.VOLUME_WINDOW:].max(),math.nan)
  if not math.isfinite(vr):return 0
  return min(100,vr*60)
 @staticmethod
 def funding(t):
  if not t:return False
  f=safe_float(t.get("funding"),math.nan)
  return math.isfinite(f) and abs(f)<=Config.MAX_ABS_FUNDING
 @staticmethod
 def oi(df):
  if not valid(df,2) or "oi_value" not in df:return False
  a=safe_float(df.oi_value.iloc[-1],math.nan)
  b=safe_float(df.oi_value.iloc[-min(Config.OI_LOOKBACK,len(df))],math.nan)
  if not (math.isfinite(a) and math.isfinite(b)) or b<=0:return False
  return (a-b)/b*100>=Config.MIN_OI_CHANGE_PCT
 @staticmethod
 def spread(book):
  try:
   b=float(book["b"][0][0]);a=float(book["a"][0][0])
   return b>0 and a>=b and (a-b)/b*100<=Config.MAX_SPREAD_PCT
  except:return False

class BTCFilter:
 @staticmethod
 def evaluate(direction):
  try:
   if not Config.BTC_FILTER_ENABLED:
    return {"allowed":True,"score":50}
   # FIX: BTC 4H datasi artiq ayrica 60s TTL-li kesden oxunur (evvelki 10s
   # umumi kes yerine) -- her simvol ucun eyni BTC sorgusunun tekrarlanmasini azaldir.
   df=BYBIT.klines("BTCUSDT","240",250,cache=BYBIT.regime_cache)
   if not valid(df,220):
    return {"allowed":True,"score":50}
   df=Indicators.add(df)
   x=df.iloc[-1]
   btc_bull=x.ema_fast>x.ema_slow and x.close>x.ema_slow
   btc_bear=x.ema_fast<x.ema_slow and x.close<x.ema_slow
   st=Structure.trend(df)
   if direction=="long":
    if btc_bull and st=="bullish":return {"allowed":True,"score":100}
    if btc_bull:return {"allowed":True,"score":80}
    if not (btc_bull or btc_bear):return {"allowed":True,"score":55}
    if btc_bear and st=="bearish":return {"allowed":False,"score":0}
    return {"allowed":True,"score":40}
   else:
    if btc_bear and st=="bearish":return {"allowed":True,"score":100}
    if btc_bear:return {"allowed":True,"score":80}
    if not (btc_bull or btc_bear):return {"allowed":True,"score":55}
    if btc_bull and st=="bullish":return {"allowed":False,"score":0}
    return {"allowed":True,"score":40}
  except Exception as e:
   log.warning("BTC filter error: %s",e)
   return {"allowed":True,"score":50}

class Correlation:
 @staticmethod
 def allowed(symbol,active):
  if not Config.CORRELATION_FILTER_ENABLED:return True
  # FIX: evvelki versiyada namized simvolun ("a") 1H klineri her aktiv simvol
  # ucun loop icinde TEKRAR-TEKRAR sorgulanirdi. Indi bir defe cekilir.
  a=BYBIT.klines(symbol,"60",80)
  if not valid(a,40):return True
  ra=a.close.pct_change().tail(40)
  n=0
  for s in active:
   if s==symbol:continue
   try:
    b=BYBIT.klines(s,"60",80)
    if not valid(b,40):continue
    c=ra.corr(b.close.pct_change().tail(40))
    if math.isfinite(safe_float(c,math.nan)) and c>=Config.CORRELATION_THRESHOLD:n+=1
   except:continue
  return n<Config.MAX_CORRELATED_ACTIVE

class Risk:
 @staticmethod
 def structural_stop(df,e,d):
  sh,sl=Structure.swings(df);atr=safe_float(df.atr.iloc[-1]);buf=atr*Config.SL_BUFFER_ATR
  cut=max(0,len(df)-Config.TARGET_LOOKBACK_15)
  if d=="long":
   v=[x for i,x in sl if i>=cut and x<e]
   return max(v)-buf if v else e-atr*Config.MIN_SL_ATR
  v=[x for i,x in sh if i>=cut and x>e]
  return min(v)+buf if v else e+atr*Config.MIN_SL_ATR
 @staticmethod
 def target_candidates(df,d,e,lookback):
  sh,sl=Structure.swings(df);cut=max(0,len(df)-lookback)
  src=sh if d=="long" else sl
  v=[x for i,x in src if i>=cut and (x>e if d=="long" else x<e)]
  return sorted(set(v),reverse=(d=="short"))
 @staticmethod
 def levels(d15,d1,d4,d):
  e=safe_float(d15.close.iloc[-1]);atr=safe_float(d15.atr.iloc[-1])
  if e<=0 or atr<=0:return None
  raw=Risk.structural_stop(d15,e,d)
  minrisk=atr*Config.MIN_SL_ATR;maxrisk=atr*Config.MAX_SL_ATR
  sl=min(raw,e-minrisk) if d=="long" else max(raw,e+minrisk)
  risk=abs(e-sl)
  if risk<=0 or risk>maxrisk:return None
  cs=(Risk.target_candidates(d1,d,e,Config.TARGET_LOOKBACK_1H)+
      Risk.target_candidates(d15,d,e,Config.TARGET_LOOKBACK_15)+
      Risk.target_candidates(d4,d,e,Config.TARGET_LOOKBACK_4H))
  cs=sorted(set(cs),reverse=(d=="short"))
  if d=="long":lo=e+risk*Config.MIN_RR;hi=e+risk*Config.MAX_RR
  else:lo=e-risk*Config.MIN_RR;hi=e-risk*Config.MAX_RR
  for lv in cs:
   if d=="long":
    tp=lv-atr*Config.TARGET_BUFFER_ATR
    if not (lo<=tp<=hi):continue
   else:
    tp=lv+atr*Config.TARGET_BUFFER_ATR
    if not (hi<=tp<=lo):continue
   rr=abs(tp-e)/risk
   if Config.MIN_RR<=rr<=Config.MAX_RR:
    return {"entry":e,"sl":sl,"tp":tp,"risk":risk,"rr":rr}
  return None

# ============================================
# HİSSƏ 6/8 — Scoring + Analyzer + analyze_symbol
# ============================================
class Scoring:
 @staticmethod
 def calculate(rq,d15,d,seq,bos,btc_eval,confirm_idx=None):
  # FIX: rq (4H rejim keyfiyyeti) artiq Analyzer.run-da bir defe hesablanmis
  # Regime.analyze(d4) neticesinden oturulur -- evvelki versiyada burda
  # TEKRAR hesablanirdi (eyni d4 uzerinde Regime.analyze ikinci defe cagirilirdi).
  x15=d15.iloc[-1]  # FIX: deyisen adi evvelki versiyada "x1" idi, amma eslinde d15-i gosterirdi
  if d=="long":
   setup_ok=bool(x15.ema_fast>x15.ema_slow and x15.close>x15.ema_slow)
  else:
   setup_ok=bool(x15.ema_fast<x15.ema_slow and x15.close<x15.ema_slow)
  setup_score=100 if setup_ok else 60
  pb_score=80 if Strategy.pullback(d15,d) else 0
  if bos:
   bos_score=100 if bos.get("type")=="CLOSE_BOS" else 75
  else:
   bos_score=0
  retest_score=100 if seq else 0
  if seq:
   confirm_score=100 if Strategy.strong(d15,seq["confirm"],d) else 60
  else:
   confirm_score=0
  rsi_s=Strategy.rsi_score(d15,d)
  vol_s=Filters.volume_score(d15,confirm_idx)
  zone=Strategy.zone_score(d15,d)
  btc_s=btc_eval.get("score",50)
  rr_s=0
  score=(Config.SCORE_W_REGIME*rq + Config.SCORE_W_SETUP*setup_score +
         Config.SCORE_W_PULLBACK*pb_score + Config.SCORE_W_BOS*bos_score +
         Config.SCORE_W_RETEST*retest_score + Config.SCORE_W_CONFIRM*confirm_score +
         Config.SCORE_W_RSI*rsi_s + Config.SCORE_W_VOLUME*vol_s +
         Config.SCORE_W_ZONE*zone + Config.SCORE_W_BTC*btc_s +
         Config.SCORE_W_RR*rr_s)
  return round(score,1),rsi_s
 @staticmethod
 def add_rr_to_score(base_score,rr):
  if rr>=2.8:rr_s=100
  elif rr>=2.5:rr_s=80
  elif rr>=2.3:rr_s=60
  elif rr>=2.2:rr_s=40
  else:rr_s=0
  return round(base_score+Config.SCORE_W_RR*rr_s,1)
 @staticmethod
 def tier(score,rr):
  if score>=Config.TIER_A_SCORE and rr>=Config.MIN_RR_ELITE:return "A"
  if score>=Config.TIER_B_SCORE:return "B"
  return "C"

class Analyzer:
 def __init__(self,symbol):self.symbol=symbol;self.reject=""
 def run(self):
  s=self.symbol
  d4=BYBIT.klines(s,"240",300);d1=BYBIT.klines(s,"60",300);d15=BYBIT.klines(s,"15",300)
  ok=valid(d4,220) and valid(d1,220) and valid(d15,80)
  STORE.add_stage("DATA",ok)
  if not ok:self.reject="DATA";return None
  d4=Indicators.add(d4);d1=Indicators.add(d1);d15=Indicators.add(d15)
  reg=Regime.analyze(d4);d=reg["direction"]
  if d=="neutral":
   x4=d4.iloc[-1]
   eb=x4.close>x4.ema_slow and x4.ema_fast>x4.ema_slow
   es=x4.close<x4.ema_slow and x4.ema_fast<x4.ema_slow
   if not (eb or es):STORE.add_bos_debug("4H_NO_EMA_ALIGN")
   else:STORE.add_bos_debug("4H_LOW_DISTANCE")
  # FIX: evvelki versiyada bu 5 sert list literali kimi yazilirdi, yeni
  # Python HAMISINI derhal hesablayirdi (short-circuit YOX idi) -- 4H_TREND
  # ugursuz olsa bele 1H_SETUP/1H_PULLBACK/RSI/ATR yene hesablanirdi.
  # Indi ardicil if-lerle ilk ugursuzluqda derhal dayanir (performans + aydinliq).
  def gate(ok,name):
   STORE.add_stage(name,ok)
   return ok
  if not gate(d!="neutral","4H_TREND"):self.reject="4H_TREND";return None
  if not gate(Strategy.setup(d1,d),"1H_SETUP"):self.reject="1H_SETUP";return None
  if not gate(Strategy.pullback(d1,d),"1H_PULLBACK"):self.reject="1H_PULLBACK";return None
  if not gate(Strategy.rsi_allowed(d15,d),"RSI"):self.reject="RSI";return None
  if not gate(Filters.volatility(d15),"ATR"):self.reject="ATR";return None
  bos=Strategy.bos(d15,d)
  STORE.add_stage("BOS",bool(bos))
  STORE.add_bos_debug(bos.get("type") if bos else "NO_BREAK")
  if not bos:self.reject="BOS";return None
  seq=Strategy.sequence(d15,bos,d)
  STORE.add_stage("RETEST_CONFIRM",bool(seq))
  if not seq:self.reject="RETEST_CONFIRM";return None
  age=len(d15)-1-seq["confirm"]
  ok=age<=Config.MAX_CONFIRM_AGE
  STORE.add_stage("CONFIRM_AGE",ok)
  if not ok:self.reject="CONFIRM_AGE";return None
  atr_now=safe_float(d15.atr.iloc[-1])
  ext=abs(d15.close.iloc[-1]-d15.close.iloc[seq["confirm"]])
  ok=atr_now>0 and ext<=atr_now*Config.MAX_ENTRY_EXTENSION_ATR
  STORE.add_stage("ENTRY_CHASE",ok)
  if not ok:self.reject="ENTRY_CHASE";return None
  ok=Filters.volume(d15);STORE.add_stage("VOLUME",ok)
  if not ok:self.reject="VOLUME";return None
  t=BYBIT.ticker(s);ok=bool(t);STORE.add_stage("TICKER",ok)
  if not ok:self.reject="TICKER";return None
  ok=Filters.funding(t);STORE.add_stage("FUNDING",ok)
  if not ok:self.reject="FUNDING";return None
  ok=Filters.oi(BYBIT.oi(s));STORE.add_stage("OI",ok)
  if not ok:self.reject="OI";return None
  ok=Filters.spread(BYBIT.book(s));STORE.add_stage("SPREAD",ok)
  if not ok:self.reject="SPREAD";return None
  btc_eval=BTCFilter.evaluate(d)
  ok=btc_eval.get("allowed",True)
  STORE.add_stage("BTC_FILTER",ok)
  if not ok:self.reject="BTC_FILTER";return None
  lv=Risk.levels(d15,d1,d4,d);ok=bool(lv);STORE.add_stage("TARGET_RISK",ok)
  if not ok:self.reject="TARGET_RISK";return None
  score,rs=Scoring.calculate(reg["quality"],d15,d,seq,bos,btc_eval,seq["confirm"])
  score=Scoring.add_rr_to_score(score,lv["rr"])
  ok=score>=Config.MIN_SCORE;STORE.add_stage("SCORE",ok)
  if not ok:self.reject="SCORE";return None
  return {"id":str(uuid.uuid4()),  # FIX: her siqnala unikal id -- baglanma zamani simvol ile deyil, id ile uygunlasdirilir
    "symbol":s,"direction":d,"score":score,"rsi":safe_float(d15.rsi.iloc[-1]),
    "rsi_score":rs,"entry":lv["entry"],"sl":lv["sl"],"tp":lv["tp"],
    "rr":lv["rr"],"risk":lv["risk"],"atr_pct":safe_float(d15.atr_pct.iloc[-1]),
    "volume_ratio":safe_float(d15.volume_ratio.iloc[-Config.VOLUME_WINDOW:].max()),
    "funding":safe_float(t.get("funding"),0),"oi":safe_float(t.get("oi"),0),
    "regime_4h":d,"setup_1h":"ok","confirm_15m":bool(seq),
    "btc_score":btc_eval.get("score",50),
    "created_at":utc_iso(),"created_ts":time.time(),
    "start_ts":int(time.time()*1000),
    "last_checked_candle":0}

def analyze_symbol(s):
 try:
  a=Analyzer(s);x=a.run();return x,("" if x else a.reject)
 except Exception as e:
  tb=traceback.format_exc()
  log.error("ANALYZE ERROR [%s]:\n%s",s,tb)
  STORE.add_error(s,str(e),short_tb(tb))
  return None,"ERROR"

# ============================================
# HİSSƏ 7/8 — Store + Performance + Telegram
# ============================================
class Store:
 def __init__(self):
  self.lock=threading.RLock()
  self.active=[];self.closed=[]
  self.rejections={};self.stages={};self.errors=[]
  self.day=utc_now().date().isoformat()
  self.daily_count=0;self.override_count=0
  self.start_balance=Config.ACCOUNT_BALANCE
  self.balance=Config.ACCOUNT_BALANCE
 def reset_day(self):
  d=utc_now().date().isoformat()
  with self.lock:
   if d!=self.day:
    self.day=d;self.daily_count=0;self.override_count=0
 def reset_rejections(self):
  # FIX: evvelki versiyada bu, self.errors-u da sifirlayirdi, yeni her scan-in
  # basinda evvelki xetalarin tarixcesi itirdi (halbuki add_error() 50-ye
  # qeder saxlamaq ucun yazilmisdi -- meqsedle ziddiyyet idi). Indi yalniz
  # scan-spesifik statistika (rejections, stages) sifirlanir, xeta tarixcesi qalir.
  with self.lock:self.rejections={};self.stages={}
 def add_stage(self,name,ok):
  with self.lock:
   x=self.stages.setdefault(name,{"pass":0,"fail":0})
   x["pass" if ok else "fail"]+=1
 def add_bos_debug(self,name):
  with self.lock:
   q=self.stages.setdefault("BOS_DEBUG",{});q[name]=q.get(name,0)+1
 def bos_report(self):
  with self.lock:q=dict(self.stages.get("BOS_DEBUG",{}))
  return " | ".join(f"{k}: {v}" for k,v in q.items()) or "Yoxdur"
 def stage_report(self,n):
  order=["DATA","4H_TREND","1H_SETUP","1H_PULLBACK","RSI","ATR","BOS",
    "RETEST_CONFIRM","CONFIRM_AGE","ENTRY_CHASE","VOLUME","TICKER","FUNDING",
    "OI","SPREAD","BTC_FILTER","TARGET_RISK","SCORE"]
  with self.lock:q=dict(self.stages)
  return "\n".join(f"{x}: {q.get(x,{}).get('pass',0)}/"
    f"{q.get(x,{}).get('pass',0)+q.get(x,{}).get('fail',0)} keçdi | "
    f"{q.get(x,{}).get('fail',0)} keçmədi" for x in order)
 def add_rejection(self,s,r):
  with self.lock:self.rejections[s]=r or "UNKNOWN"
 def rejection_stats(self):
  with self.lock:
   q={}
   for v in self.rejections.values():q[v]=q.get(v,0)+1
   return q
 def add_error(self,s,err,detail=""):
  with self.lock:
   self.errors=(self.errors+[{"symbol":s,"error":err,"detail":detail,
     "time":utc_iso()}])[-Config.MAX_ERRORS_KEPT:]
 def error_report(self):
  with self.lock:q=list(self.errors)
  if not q:return "Xəta yoxdur."
  lines=[]
  for e in q[-5:]:
   lines.append(f"❌ {e['symbol']}: {e['error']}\n   └ {e['detail']}")
  return "\n".join(lines)
 def active_symbols(self):
  with self.lock:return [x["symbol"] for x in self.active]
 def has_active(self,symbol,direction=None):
  with self.lock:
   for a in self.active:
    if a["symbol"]==symbol:
     if direction is None or a["direction"]==direction:return True
   return False
 def can_add(self,score=0):
  self.reset_day()
  with self.lock:
   if len(self.active)>=Config.MAX_ACTIVE_SIGNALS:return False
   if self.daily_count<Config.MAX_DAILY_SIGNALS:return True
   if (score>=Config.OVERRIDE_SCORE and
       self.override_count<Config.MAX_OVERRIDE_DAILY):return True
   return False
 def add(self,x):
  with self.lock:
   if len(self.active)>=Config.MAX_ACTIVE_SIGNALS:return False
   if self.has_active(x["symbol"],x["direction"]):return False
   x.setdefault("id",str(uuid.uuid4()))
   is_override=x.get("override",False)
   if self.daily_count>=Config.MAX_DAILY_SIGNALS:
    if is_override and self.override_count<Config.MAX_OVERRIDE_DAILY:
     self.override_count+=1
    else:return False
   risk_amount=self.balance*Config.RISK_PERCENT/100
   x["risk_amount"]=risk_amount
   self.active.append(x);self.daily_count+=1;self.save();return True
 def close_position(self,signal_id,x,result,exit_price,result_r):
  with self.lock:
   # FIX: evvelki versiyada YALNIZ symbol-a gore filtrlenirdi. Eger eyni
   # simvolda hem LONG, hem SHORT eyni anda aktiv olsaydi (has_active
   # yoxlamasina gore nezeri cehetden mumkundur), biri baglananda IKISI de
   # silinirdi -- diger hele TP/SL-e catmasa bele. Indi her siqnalin
   # unikal id-si ile deqiq uygunlasdirilir.
   self.active=[a for a in self.active if a.get("id")!=signal_id]
   y=dict(x);y["closed_at"]=utc_iso()
   y["result"]=result;y["exit_price"]=exit_price;y["result_r"]=result_r
   risk_amount=y.get("risk_amount",self.balance*Config.RISK_PERCENT/100)
   gross_pnl=result_r*risk_amount
   # FIX: komissiya/surusme evvelki versiyada risk_amount (SL mesafesindeki
   # dollar itkisi) uzerinden hesablanirdi. Real ticaretde ise komissiya
   # movqenin NOTIONAL (miqdar x qiymet) deyerine tetbiq olunur. Notional
   # adeten risk_amount-dan qat-qat boyukdur (cunki SL adeten qiymetin kicik
   # faizindedir), ona gore evvelki hesablama komissiyani suni azaldirdi ve
   # balansi real veziyyetden daha yaxsi gosterirdi. Indi:
   #   qty = risk_amount / |entry-sl|  ;  notional = qty * entry
   entry_price=safe_float(y.get("entry"))
   risk_dist=safe_float(y.get("risk"))
   if entry_price>0 and risk_dist>0:
    qty=risk_amount/risk_dist
    notional=qty*entry_price
   else:
    notional=risk_amount
   commission=notional*Config.TAKER_FEE_PCT/100*2
   slippage=notional*Config.SLIPPAGE_PCT/100*2
   net_pnl=gross_pnl-commission-slippage
   y["gross_pnl"]=round(gross_pnl,4)
   y["commission"]=round(commission,4)
   y["slippage"]=round(slippage,4)
   y["pnl"]=round(net_pnl,4)
   y["duration_h"]=round((time.time()-x.get("created_ts",time.time()))/3600,2)
   self.balance+=net_pnl
   self.closed=(self.closed+[y])[-Config.MAX_CLOSED_SIGNALS_KEPT:]
   self.save()
   return y
 def save(self):
  # FIX: atomic yazma -- proses save() ortasinda coksa kohne fayl korlanmasin
  # deye evvelce .tmp faylina yazilir, sonra atomic sekilde evez olunur.
  try:
   tmp=Config.SIGNAL_FILE+".tmp"
   with open(tmp,"w",encoding="utf8") as f:
    json.dump({"active":self.active,"closed":self.closed,
      "day":self.day,"daily_count":self.daily_count,
      "override_count":self.override_count,
      "balance":self.balance,"start_balance":self.start_balance},f,indent=2)
   os.replace(tmp,Config.SIGNAL_FILE)
  except Exception as e:log.warning("save: %s",e)
 def load(self):
  try:
   with open(Config.SIGNAL_FILE,encoding="utf8") as f:x=json.load(f)
   self.active=x.get("active",[]);self.closed=x.get("closed",[])
   self.day=x.get("day",self.day);self.daily_count=x.get("daily_count",0)
   self.override_count=x.get("override_count",0)
   self.balance=x.get("balance",Config.ACCOUNT_BALANCE)
   self.start_balance=x.get("start_balance",Config.ACCOUNT_BALANCE)
   self.reset_day()
  except Exception as e:
   log.warning("load state: %s",e)

STORE=Store();STORE.load()

class Performance:
 @staticmethod
 def stats():
  c=STORE.closed
  tp=sum(1 for x in c if x.get("result")=="TP")
  sl=sum(1 for x in c if x.get("result")=="SL")
  tm=sum(1 for x in c if x.get("result")=="TIME")
  closed=tp+sl+tm
  decisive=tp+sl
  winrate=round(tp/decisive*100,2) if decisive>0 else 0.0
  tprate=round(tp/closed*100,2) if closed>0 else 0.0
  net_r=round(sum(safe_float(x.get("result_r")) for x in c),2)
  net_pnl=round(sum(safe_float(x.get("pnl")) for x in c),2)
  avg_r=round(net_r/decisive,2) if decisive>0 else 0.0
  tier_stats={}
  for x in c:
   tr=x.get("tier","?")
   if tr not in tier_stats:tier_stats[tr]={"w":0,"l":0,"t":0,"pnl":0.0}
   if x.get("result")=="TP":tier_stats[tr]["w"]+=1
   elif x.get("result")=="SL":tier_stats[tr]["l"]+=1
   elif x.get("result")=="TIME":tier_stats[tr]["t"]+=1
   tier_stats[tr]["pnl"]+=safe_float(x.get("pnl"))
  return {"start_balance":round(STORE.start_balance,2),
          "balance":round(STORE.balance,2),
          "closed":closed,"tp":tp,"sl":sl,"time":tm,
          "wins":tp,"losses":sl,
          "winrate":winrate,"tprate":tprate,
          "net_r":net_r,"pnl":net_pnl,"avg_r":avg_r,
          "today_signals":STORE.daily_count,
          "active":len(STORE.active),
          "tier_stats":tier_stats}

class Telegram:
 @staticmethod
 def send(text):
  if not Config.BOT_TOKEN:return False
  try:
   r=requests.post(f"https://api.telegram.org/bot{Config.BOT_TOKEN}/sendMessage",
     json={"chat_id":Config.CHAT_ID,"text":text},timeout=10)
   return r.ok
  except Exception as e:log.warning("tg: %s",e);return False
 @staticmethod
 def signal(x):
  d="🟢 LONG" if x["direction"]=="long" else "🔴 SHORT"
  tier=x.get("tier","C")
  tier_emoji={"A":"🥇 ELITE","B":"🥈 GOOD","C":"🥉 STD"}.get(tier,"STD")
  if x.get("override"):tier_emoji="⚡ OVERRIDE"
  risk_amount=x.get("risk_amount",0)
  body=(f"🚨 YENİ SİQNAL #{STORE.daily_count}  {tier_emoji}\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"📌 {x['symbol']}  {d}\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"🎯 Score: {x['score']}/100\n"
    f"📊 RSI: {x['rsi']:.1f}\n"
    f"🏛 4H: {x.get('regime_4h','?').upper()}\n"
    f"⏱ Setup: {x.get('setup_1h','ok')}  |  15M: {x.get('confirm_15m',False)}\n"
    f"💰 Entry: {x['entry']:.8g}\n"
    f"🛑 SL: {x['sl']:.8g}\n"
    f"✅ TP: {x['tp']:.8g}\n"
    f"⚖️ RR: 1:{x['rr']:.2f}\n"
    f"📈 ATR: {x['atr_pct']:.2f}%  🔊 Vol: {x['volume_ratio']:.2f}x\n"
    f"🪙 BTC: {x.get('btc_score',50)}/100\n"
    f"💵 Risk: {risk_amount:.2f} USDT\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"⚠️ Signal only — öz analizinlə")
  Telegram.send(body)
  Telegram.send(Telegram.status())
  return True
 @staticmethod
 def closed(y):
  d="🟢 LONG" if y["direction"]=="long" else "🔴 SHORT"
  emoji={"TP":"✅","SL":"🔴","TIME":"⏱"}.get(y.get("result"),"❓")
  return Telegram.send(f"{emoji} CLOSED [{y.get('tier','?')}]\n"
    f"{y['symbol']} {d}\n"
    f"Result: {y.get('result')}\n"
    f"Entry: {y.get('entry',0):.8g}\n"
    f"Exit: {y.get('exit_price',0):.8g}\n"
    f"R: {y.get('result_r',0):.2f}\n"
    f"Gross: {y.get('gross_pnl',0):.2f} USDT\n"
    f"Comm: -{y.get('commission',0):.2f} | Slip: -{y.get('slippage',0):.2f}\n"
    f"Net PnL: {y.get('pnl',0):.2f} USDT\n"
    f"Balance: {STORE.balance:.2f} USDT\n"
    f"Duration: {y.get('duration_h',0):.1f}h")
 @staticmethod
 def status():
  if not STORE.active:return "📭 Aktiv siqnal yoxdur."
  lines=[f"📌 AKTİV SIQNALLAR ({len(STORE.active)}) — "
    f"{STORE.daily_count}/{Config.MAX_DAILY_SIGNALS} bu gün\n"]
  for x in STORE.active:
   t=x.get("tier","C");emoji={"A":"🥇","B":"🥈","C":"🥉"}.get(t,"")
   if x.get("override"):emoji="⚡"
   lines.append(f"{emoji} {x['symbol']} {x['direction'].upper()}\n"
     f"Score: {x['score']} | Entry: {x['entry']:.8g}\n"
     f"SL: {x['sl']:.8g} | TP: {x['tp']:.8g} | RR: 1:{x['rr']:.2f}")
  return "\n".join(lines)
 @staticmethod
 def scan_done(n,found,sent,tier_a=0,tier_b=0,tier_c=0):
  q=STORE.rejection_stats()
  r=" | ".join(f"{k}: {v}" for k,v in sorted(q.items(),key=lambda z:-z[1])) if q else "Yoxdur"
  errs=STORE.error_report()
  err_block=f"\n\n⚠️ XƏTALAR\n{errs}" if "❌" in errs else ""
  return Telegram.send(f"✅ SCAN TAMAMLANDI\n\nAnaliz olunan: {n}\n"
    f"Uyğun setup: {len(found)}\n"
    f"🥇 Elite: {tier_a} | 🥈 Good: {tier_b} | 🥉 Std: {tier_c}\n"
    f"Göndərilən: {sent}\n"
    f"Aktiv: {len(STORE.active)}/{Config.MAX_ACTIVE_SIGNALS}\n"
    f"Bugün: {STORE.daily_count}/{Config.MAX_DAILY_SIGNALS} "
    f"(override: {STORE.override_count}/{Config.MAX_OVERRIDE_DAILY})\n\n"
    f"📋 ŞƏRTLƏR\n{STORE.stage_report(n)}\n\n"
    f"🔎 BOS DETALLI\n{STORE.bos_report()}\n\n"
    f"🚫 İLK UĞURSUZ ŞƏRT\n{r}{err_block}")
 @staticmethod
 def handle(text):
  t=(text or "").strip().lower()
  try:
   if t in ("/status","/signals"):return Telegram.status()
   if t=="/stats":
    s=Performance.stats()
    base=(f"📊 STATS\n"
      f"💰 Balance: {s['balance']:.2f} (start {s['start_balance']:.2f})\n"
      f"📦 Closed: {s['closed']}\n"
      f"✅ TP: {s['tp']}\n"
      f"🔴 SL: {s['sl']}\n"
      f"⏱ TIME: {s['time']}\n"
      f"📈 Win Rate: {s['winrate']:.2f}% (TP/(TP+SL))\n"
      f"🎯 TP Rate: {s['tprate']:.2f}% (TP/Closed)\n"
      f"💵 Net R: {s['net_r']:.2f}R\n"
      f"💵 Net PnL: {s['pnl']:.2f} USDT\n"
      f"⚖️ Avg R: {s['avg_r']:.2f}\n"
      f"📌 Aktiv: {s['active']} | Bugün: {s['today_signals']}")
    ts=s.get("tier_stats",{})
    if ts:
     base+="\n\n🥇 TIER STATS"
     for tr in ["A","B","C"]:
      if tr in ts:
       x=ts[tr];tot=x["w"]+x["l"]
       wr=round(x["w"]/tot*100,1) if tot else 0
       base+=f"\n{tr}: {x['w']}W/{x['l']}L/{x['t']}T ({wr}%) | {x['pnl']:.2f} USDT"
    return base
   if t=="/rejections":
    q=STORE.rejection_stats()
    body="\n".join(f"{k}: {v}" for k,v in sorted(q.items(),key=lambda z:-z[1])) if q else "Yoxdur."
    return f"🚫 REJECTIONS\n{body}"
   if t=="/errors":
    return f"⚠️ XƏTALAR\n{STORE.error_report()}"
   if t=="/help":
    return ("/status — aktiv siqnallar\n"
      "/stats — statistika\n"
      "/scan — manual skan\n"
      "/rejections — rədd səbəbləri\n"
      "/errors — xətalar\n"
      "/help — bu mesaj")
   return None
  except Exception as e:
   # FIX: bu blok evvelki versiyada try/except-siz idi -- /stats ve ya
   # diger komanda daxilinde gozlenilmez xeta olsa, poller thread-i
   # sukutla coke biler ve bot Telegram komandalarina cavab vermeyi keserdi.
   log.warning("telegram handle: %s",e)
   return "⚠️ Komanda icra olunarkən xəta baş verdi."

# ============================================
# HİSSƏ 8/8 — PositionManager + Scanner + Flask + Main
# ============================================
class PositionManager:
 def check(self,x):
  symbol=x["symbol"];d=x["direction"]
  entry=safe_float(x.get("entry"),math.nan)
  sl=safe_float(x.get("sl"),math.nan)
  tp=safe_float(x.get("tp"),math.nan)
  if not all(math.isfinite(v) for v in [entry,sl,tp]):return
  created_ts=x.get("created_ts",time.time())
  age=(time.time()-created_ts)/3600
  k=BYBIT.kline_1m_live(symbol)
  if not k:return
  k_ts_sec=k.get("ts",0)/1000.0 if k.get("ts",0)>1e11 else k.get("ts",0)
  hi=safe_float(k.get("high"),math.nan)
  lo=safe_float(k.get("low"),math.nan)
  close=safe_float(k.get("close"),math.nan)
  if not (math.isfinite(hi) and math.isfinite(lo) and math.isfinite(close)):return
  last_checked=x.get("last_checked_candle",0)
  if k_ts_sec<=last_checked:return
  x["last_checked_candle"]=k_ts_sec
  result=None;exit_price=None;result_r=None
  # FIX: evvelki versiyada "sl_hit and tp_hit" ve "sl_hit" branch-lari eyni
  # neticeni (SL) verirdi -- artiq if/elif kimi sadelesdirilib, davranis eynidir.
  if d=="long":
   sl_hit=lo<=sl
   tp_hit=hi>=tp
   if sl_hit:
    result="SL";exit_price=sl;result_r=-1.0
   elif tp_hit:
    result="TP";exit_price=tp;result_r=x["rr"]
  else:
   sl_hit=hi>=sl
   tp_hit=lo<=tp
   if sl_hit:
    result="SL";exit_price=sl;result_r=-1.0
   elif tp_hit:
    result="TP";exit_price=tp;result_r=x["rr"]
  if not result and age>=Config.MAX_HOLD_HOURS:
   result="TIME";exit_price=close
   if d=="long":result_r=(close-entry)/(entry-sl) if entry!=sl else 0
   else:result_r=(entry-close)/(sl-entry) if sl!=entry else 0
  if result:
   # FIX: baglanma artiq simvol yox, siqnalin unikal id-si ile uygunlasdirilir
   y=STORE.close_position(x.get("id"),x,result,exit_price,result_r)
   Telegram.closed(y)
 def run(self):
  while not STOP_EVENT.is_set():
   try:
    for x in list(STORE.active):self.check(x)
   except Exception as e:
    tb=traceback.format_exc()
    log.error("MONITOR ERROR:\n%s",tb)
   STOP_EVENT.wait(Config.MONITOR_INTERVAL)

MANAGER=PositionManager()

class Scanner:
 def __init__(self):self.lock=threading.Lock();self.running=False
 def symbols(self):
  try:
   tickers=BYBIT.all_tickers()
   usdt_end=0;turn_pos=0;not_tradfi=0
   c=[]
   for x in tickers:
    s=x.get("symbol","");turn=safe_float(x.get("turnover24h"))
    base=s[:-4] if s.endswith("USDT") else ""
    if s.endswith("USDT"):usdt_end+=1
    if s.endswith("USDT") and turn>0:turn_pos+=1
    if s.endswith("USDT") and turn>0 and base not in Config.TRADFI:
     not_tradfi+=1;c.append((s,turn))
   c.sort(key=lambda z:z[1],reverse=True)
   top=[s for s,_ in c[:Config.CANDIDATE_LIMIT]]
   out=[]
   # FIX: instrument() sorgulari evvelki versiyada seriyali (bir-bir) idi;
   # CANDIDATE_LIMIT=80 simvol ucun bu, ilk skani xeyli yavaslada bilirdi.
   # Indi digger yerlerde oldugu kimi paralel islenir (netice keslendiyi
   # ucun novbeti skanlara tesiri yoxdur).
   def check_symbol(sym):
    i=BYBIT.instrument(sym)
    if not i:return None
    if i.get("status")!="Trading":return None
    if i.get("contractType")!="LinearPerpetual":return None
    if i.get("quoteCoin")!="USDT":return None
    if i.get("settleCoin")!="USDT":return None
    return sym
   with ThreadPoolExecutor(max_workers=Config.PARALLEL_WORKERS) as ex:
    for sym in ex.map(check_symbol,top):
     if sym:
      out.append(sym)
      if len(out)>=Config.SCAN_TOP_N:break
   log.info("Symbols: %d / %d (after filter)",len(out),len(c))
   if Config.DEBUG_SYMBOLS_ENABLED:
    Telegram.send(f"🔎 SYMBOLS: {len(out)} coin\n"
      f"all={len(tickers)} usdt={usdt_end} turn>0={turn_pos} trad={not_tradfi}")
   return out or Config.FALLBACK_COINS
  except Exception as e:
   tb=traceback.format_exc()
   log.error("SYMBOLS ERROR:\n%s",tb)
   STORE.add_error("SYMBOLS",str(e),short_tb(tb))
   return Config.FALLBACK_COINS
 def scan(self,force=False):
  if not self.lock.acquire(False):return
  if Config.SCAN_HOURS_ENABLED and not force:
   h=utc_now().hour
   if not (Config.SCAN_HOUR_START<=h<Config.SCAN_HOUR_END):
    log.info("Skan vaxtı deyil (%02d:00 UTC)",h)
    self.lock.release();return
  self.running=True;found=[];sent=0;symbols=[]
  tier_a=tier_b=tier_c=0
  try:
   STORE.reset_day();STORE.reset_rejections()
   active_full=len(STORE.active)>=Config.MAX_ACTIVE_SIGNALS
   daily_full=STORE.daily_count>=Config.MAX_DAILY_SIGNALS
   override_full=STORE.override_count>=Config.MAX_OVERRIDE_DAILY
   if active_full:
    log.info("Active limit doldu")
    return
   if daily_full and override_full:
    log.info("Daily+override doldu")
    return
   symbols=self.symbols()
   with ThreadPoolExecutor(max_workers=Config.PARALLEL_WORKERS) as ex:
    fs={ex.submit(analyze_symbol,s):s for s in symbols}
    for f in as_completed(fs):
     s=fs[f]
     try:x,r=f.result()
     except Exception as e:
      tb=traceback.format_exc()
      log.error("SCAN THREAD ERROR [%s]:\n%s",s,tb)
      STORE.add_error(s,str(e),short_tb(tb))
      x,r=None,"ERROR"
     if x:found.append(x)
     else:STORE.add_rejection(s,r)
   found.sort(key=lambda z:z["score"],reverse=True)
   for x in found:x["tier"]=Scoring.tier(x["score"],x["rr"])
   t_a=[x for x in found if x["tier"]=="A"]
   t_b=[x for x in found if x["tier"]=="B"]
   t_c=[x for x in found if x["tier"]=="C"]
   tier_a=len(t_a);tier_b=len(t_b);tier_c=len(t_c)
   log.info("Tiers | A=%d B=%d C=%d",tier_a,tier_b,tier_c)
   # FIX: evvelki "dc==0 / dc==1 / dc>=MAX" zenciri sert-kodlanmisdi ve
   # yalniz MAX_DAILY_SIGNALS=2 olanda dogru isleyirdi (mes. MAX_DAILY_SIGNALS=3
   # olsaydi dc==2 hec bir serte dusmezdi, o scan-de hec bir siqnal gonderilmezdi).
   # Indi SIGNAL_SCORE_THRESHOLDS siyahisi ve simulyasiya edilmis sayğacla
   # istenilen MAX_DAILY_SIGNALS / MAX_SIGNALS_TO_SEND deyeri ucun dogrudur.
   to_send=[];simulated_dc=STORE.daily_count
   for x in t_a:
    if len(to_send)>=Config.MAX_SIGNALS_TO_SEND:break
    if len(STORE.active)+len(to_send)>=Config.MAX_ACTIVE_SIGNALS:break
    score=x["score"]
    if simulated_dc<Config.MAX_DAILY_SIGNALS:
     idx=min(simulated_dc,len(Config.SIGNAL_SCORE_THRESHOLDS)-1)
     threshold=Config.SIGNAL_SCORE_THRESHOLDS[idx]
     if score<threshold:
      STORE.add_rejection(x["symbol"],f"SIGNAL_{simulated_dc+1}_LOW");continue
     to_send.append(x);simulated_dc+=1
    else:
     if score<Config.OVERRIDE_SCORE:
      STORE.add_rejection(x["symbol"],"OVERRIDE_LOW");continue
     if STORE.override_count<Config.MAX_OVERRIDE_DAILY:
      x["override"]=True
      to_send.append(x);simulated_dc+=1
   for x in to_send:
    if not Correlation.allowed(x["symbol"],STORE.active_symbols()):
     STORE.add_rejection(x["symbol"],"CORRELATION");continue
    if STORE.add(x):
     Telegram.signal(x);sent+=1
     if x.get("override"):
      Telegram.send(f"⚡ OVERRIDE SİQNAL\n"
        f"Score {x['score']} ≥ {Config.OVERRIDE_SCORE}\n"
        f"Gündəlik limit artırıldı")
   Telegram.scan_done(len(symbols),found,sent,tier_a,tier_b,tier_c)
   log.info("SCAN | %d | valid=%d | A=%d | sent=%d",
     len(symbols),len(found),tier_a,sent)
  finally:
   self.running=False;self.lock.release()

SCANNER=Scanner()

class ScannerWorker:
 def run(self):
  STOP_EVENT.wait(15)
  while not STOP_EVENT.is_set():
   try:SCANNER.scan()
   except Exception as e:
    tb=traceback.format_exc()
    log.error("SCANNER WORKER ERROR:\n%s",tb)
    STORE.add_error("SCANNER",str(e),short_tb(tb))
    Telegram.send(f"⚠️ SCANNER XƏTA\n{str(e)[:200]}\n\n{short_tb(tb,300)}")
   STOP_EVENT.wait(Config.CHECK_INTERVAL)

class TelegramPoller:
 def __init__(self):
  self.offset=0
  # FIX: offset indi diske yazilir -- restart zamani kohne /komandalarin
  # tekrar icra olunmasinin qarsisi alinir (evvelki versiyada her restart-da 0-dan basliyirdi).
  self._offset_file=os.path.join(Config.DATA_DIR,"tg_offset.txt")
  try:
   with open(self._offset_file) as f:self.offset=int(f.read().strip())
  except Exception:pass
 def _save_offset(self):
  try:
   with open(self._offset_file,"w") as f:f.write(str(self.offset))
  except Exception:pass
 def run(self):
  if not Config.BOT_TOKEN:return
  url=f"https://api.telegram.org/bot{Config.BOT_TOKEN}/getUpdates"
  while not STOP_EVENT.is_set():
   try:
    data=requests.get(url,params={"timeout":20,"offset":self.offset},timeout=25).json()
    for u in data.get("result",[]):
     self.offset=u["update_id"]+1;self._save_offset();m=u.get("message",{})
     if str(m.get("chat",{}).get("id",""))!=str(Config.CHAT_ID):continue
     text=m.get("text","")
     if text.strip().lower()=="/scan":
      if SCANNER.running:Telegram.send("⏳ Scan artıq işləyir.")
      else:
       Telegram.send("🔎 Scan başladıldı... (manual)")
       threading.Thread(target=SCANNER.scan,
         kwargs={"force":True},daemon=True).start()
     else:
      ans=Telegram.handle(text)
      if ans:Telegram.send(ans)
   except Exception as e:log.warning("poller: %s",e)
   STOP_EVENT.wait(2)

POLL=TelegramPoller()
app=Flask(__name__)

@app.get("/")
def home():return jsonify({"bot":BOT_VERSION,"status":"running",
  "active":len(STORE.active),"today":STORE.daily_count,
  "balance":round(STORE.balance,2)})
@app.get("/status")
def ws():return jsonify({"version":BOT_VERSION,"active":STORE.active,
  "daily_count":STORE.daily_count,"override_count":STORE.override_count,
  "balance":round(STORE.balance,2),"stats":Performance.stats()})
@app.get("/signals")
def wsg():return jsonify({"active":STORE.active})
@app.get("/stats")
def wst():return jsonify(Performance.stats())
@app.get("/rejections")
def wr():return jsonify(STORE.rejection_stats())
@app.get("/errors")
def werr():return jsonify({"errors":STORE.errors})

def flask_worker():
 try:app.run(host="0.0.0.0",port=Config.FLASK_PORT,debug=False,use_reloader=False)
 except Exception as e:log.warning("flask: %s",e)

def stop_handler(*_):STOP_EVENT.set()
signal.signal(signal.SIGINT,stop_handler)
signal.signal(signal.SIGTERM,stop_handler)

def startup():
 validate_config()
 baku_start=(Config.SCAN_HOUR_START+4)%24
 baku_end=(Config.SCAN_HOUR_END+4)%24
 Telegram.send(f"🤖 SWING AI {BOT_VERSION}\n4H → 1H → 15M\n"
   f"━━━━━━━━━━━━━━━━━━\n"
   f"🥇 1-ci: Score ≥ {Config.FIRST_SIGNAL_SCORE}\n"
   f"🥈 2-ci: Score ≥ {Config.SECOND_SIGNAL_SCORE}\n"
   f"🥉 3-cü: Score ≥ {Config.THIRD_SIGNAL_SCORE}\n"
   f"Gündəlik tavan: {Config.MAX_DAILY_SIGNALS} siqnal (ardıcıl artan eşik)\n"
   f"━━━━━━━━━━━━━━━━━━\n"
   f"⏰ UTC: {Config.SCAN_HOUR_START:02d}-{Config.SCAN_HOUR_END:02d} | "
   f"Bakı: {baku_start:02d}-{baku_end:02d}\n"
   f"Həftə sonu: ✅ | Manual: /scan\n"
   f"━━━━━━━━━━━━━━━━━━\n"
   f"💰 Balance: {STORE.balance:.2f} USDT\n"
   f"📊 Monitor: 1M kline (wick-safe)")

def start_threads():
 threading.Thread(target=flask_worker,daemon=True).start()
 threading.Thread(target=ScannerWorker().run,daemon=True).start()
 threading.Thread(target=MANAGER.run,daemon=True).start()
 threading.Thread(target=POLL.run,daemon=True).start()

def main():
 startup();start_threads()
 while not STOP_EVENT.is_set():time.sleep(1)

if __name__=="__main__":main()
   
