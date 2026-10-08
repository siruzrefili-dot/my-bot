import os,time,json,math,signal,logging,threading,traceback,uuid,hashlib
import requests,numpy as np,pandas as pd
from datetime import datetime,timezone
from concurrent.futures import ThreadPoolExecutor,as_completed
from flask import Flask,jsonify

BOT_VERSION="14.9 HOURLY-STATS"
class Config:
 BASE_URL="https://api.bybit.com";CATEGORY="linear";BOT_TOKEN=os.getenv("BOT_TOKEN","");CHAT_ID=os.getenv("CHAT_ID","1121794078")
 SCAN_TOP_N=40;CANDIDATE_LIMIT=80;CHECK_INTERVAL=300;MONITOR_INTERVAL=5;PARALLEL_WORKERS=3
 TREND_TF="240";SETUP_TF="60";ENTRY_TF="15";EMA_FAST=50;EMA_SLOW=200;RSI_PERIOD=14;ATR_PERIOD=14;VOLUME_LOOKBACK=20
 MIN_SCORE=60;MIN_RR=2.2;MAX_RR=3.5;ACCOUNT_BALANCE=1000.;RISK_PERCENT=1.
 MAX_ACTIVE_SIGNALS=2;MAX_DAILY_SIGNALS=3;MAX_SIGNALS_TO_SEND=1
 FIRST_SIGNAL_SCORE=80;SECOND_SIGNAL_SCORE=85;THIRD_SIGNAL_SCORE=88;SIGNAL_SCORE_THRESHOLDS=(80,85,88)
 OVERRIDE_SCORE=95;MAX_OVERRIDE_DAILY=0;TIER_A_SCORE=72;TIER_B_SCORE=66;MIN_RR_ELITE=2.4
 MIN_ATR_PCT=.20;MAX_ATR_PCT=10.;MIN_VOLUME_RATIO=1.30;VOLUME_WINDOW=5;MIN_EMA_DISTANCE_PCT=.09
 LONG_RSI_MIN=42.;LONG_RSI_MAX=62.;SHORT_RSI_MIN=38.;SHORT_RSI_MAX=58.
 MIN_SL_ATR=1.10;MAX_SL_ATR=2.50;SL_BUFFER_ATR=.15;BOS_BUFFER_ATR=.15;ALLOW_RECLAIM_BOS=True;BOS_LOOKBACK=100
 RETEST_MAX_BARS=15;RETEST_ATR_DISTANCE=.60;CONFIRMATION_MAX_BARS_AFTER_RETEST=5;MAX_CONFIRM_AGE=6;MAX_ENTRY_EXTENSION_ATR=2.
 TARGET_LOOKBACK_15=80;TARGET_LOOKBACK_1H=80;TARGET_LOOKBACK_4H=80;TARGET_BUFFER_ATR=.10
 MAX_ABS_FUNDING=.003;MIN_OI_CHANGE_PCT=-10.;OI_LOOKBACK=5;MAX_SPREAD_PCT=.20
 BTC_FILTER_ENABLED=True;CORRELATION_FILTER_ENABLED=False;MAX_CORRELATED_ACTIVE=2;CORRELATION_THRESHOLD=.85
 SCAN_HOUR_START=9;SCAN_HOUR_END=20;SCAN_HOURS_ENABLED=True;DEBUG_SYMBOLS_ENABLED=False;MAX_HOLD_HOURS=96
 DATA_DIR="swing_bot_data";FLASK_PORT=int(os.getenv("PORT","10000"));REQUEST_TIMEOUT=15
 CACHE_TTL=10;TICKER_CACHE_TTL=30;INSTRUMENT_CACHE_TTL=3600;REGIME_CACHE_TTL=60
 MAX_CLOSED_SIGNALS_KEPT=500;MAX_ERRORS_KEPT=50;SIGNAL_FILE=os.path.join(DATA_DIR,"signals.json")
 TAKER_FEE_PCT=.055;SLIPPAGE_PCT=.02
 SCORE_W_REGIME=.20;SCORE_W_SETUP=.10;SCORE_W_PULLBACK=.10;SCORE_W_BOS=.12;SCORE_W_RETEST=.10;SCORE_W_CONFIRM=.12
 SCORE_W_RSI=.08;SCORE_W_VOLUME=.06;SCORE_W_ZONE=.05;SCORE_W_BTC=.03;SCORE_W_RR=.04
 FALLBACK_COINS=["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT","ADAUSDT","AVAXUSDT","DOGEUSDT","LINKUSDT","SUIUSDT","ARBUSDT","OPUSDT"]
 TRADFI={"XAU","XAG","CL","USOIL","UKOIL","SPX","NDX","DJI","US30","NAS100","US500","GER40","UK100","JP225","HK50","AAPL","AMZN","AMD","COIN","GOOG","GOOGL","META","MSFT","MSTR","MU","NFLX","NVDA","ORCL","PLTR","QCOM","TSLA","TSM","HOOD","SMCI","RKLB","OPENAI","NBIS","HPE","ANTHROPIC","CRWV","ADBE","NOKIA","LITE","ASTS","AEHR","ALAB","COHR","CIEN","POET","BNC","AXTI","FLNC","CRCL","BE","ONDS","CBRS","PURR","STXX","QNTX","SKHYNIX","SNDK","SOXL"}

os.makedirs(Config.DATA_DIR,exist_ok=True)
logging.basicConfig(level=logging.INFO,format="%(asctime)s | %(levelname)s | %(message)s")
log=logging.getLogger("SWING_AI");STOP_EVENT=threading.Event()

def safe_float(v,d=0.):
 try:
  x=float(v);return d if not math.isfinite(x) else x
 except:return d
def clamp(v,lo,hi):return max(lo,min(hi,v))
def utc_now():return datetime.now(timezone.utc)
def utc_iso():return utc_now().isoformat()
def valid(df,n):return isinstance(df,pd.DataFrame) and len(df)>=n
def short_tb(tb,maxlen=200):
 try:return " | ".join([x.strip() for x in str(tb).strip().split("\n") if x.strip()][-2:])[:maxlen]
 except:return "?"

class Cache:
 def __init__(self,ttl):self.ttl=ttl;self.data={};self.lock=threading.RLock()
 def get(self,k):
  with self.lock:
   x=self.data.get(k)
   if x is None:return None
   if time.time()-x[1]>self.ttl:self.data.pop(k,None);return None
   return x[0]
 def set(self,k,v):
  with self.lock:self.data[k]=(v,time.time())
  return v
 def delete(self,k):
  with self.lock:self.data.pop(k,None)
 def clear(self):
  with self.lock:self.data.clear()
 def __len__(self):
  with self.lock:return len(self.data)
# ============================================
# HİSSƏ 2/8 — BybitClient + validate_config
# ============================================
class BybitClient:
 def __init__(self):
  self.base=Config.BASE_URL;self.cache=Cache(Config.CACHE_TTL);self.tcache=Cache(Config.TICKER_CACHE_TTL);self.icache=Cache(Config.INSTRUMENT_CACHE_TTL);self.regime_cache=Cache(Config.REGIME_CACHE_TTL)
  self.local=threading.local();self._lock=threading.Lock();self._last=0.
 def session(self):
  if not hasattr(self.local,"session"):
   s=requests.Session();s.headers.update({"User-Agent":"SwingAI/14.9"});self.local.session=s
  return self.local.session
 def _rate(self):
  with self._lock:
   w=.10-(time.time()-self._last)
   if w>0:time.sleep(w)
   self._last=time.time()
 def get(self,path,params=None,key=None,cache=None,retries=3):
  c=cache or self.cache
  if key:
   z=c.get(key)
   if z is not None:return z
  last=None
  for i in range(max(1,retries)):
   try:
    self._rate();r=self.session().get(self.base+path,params=params or {},timeout=Config.REQUEST_TIMEOUT)
    if r.status_code==429:
     time.sleep(1.5*(i+1));continue
    r.raise_for_status();z=r.json()
    if z.get("retCode")!=0:raise RuntimeError(z.get("retMsg","API"))
    if key:c.set(key,z)
    return z
   except Exception as e:
    last=e
    if i<retries-1:time.sleep(.8*(i+1))
  log.warning("API %s: %s",path,last);return None
 def klines(self,symbol,interval,limit=300,cache=None):
  z=self.get("/v5/market/kline",{"category":"linear","symbol":symbol,"interval":interval,"limit":limit},f"k:{symbol}:{interval}:{limit}",cache)
  rows=z.get("result",{}).get("list",[]) if z else []
  if not rows:return pd.DataFrame()
  rows=list(reversed(rows));cols=["timestamp","open","high","low","close","volume","turnover"];df=pd.DataFrame(rows,columns=cols)
  for c in cols:df[c]=pd.to_numeric(df[c],errors="coerce")
  df=df.dropna(subset=["open","high","low","close","volume"])
  return df.iloc[:-1].reset_index(drop=True) if len(df)>1 else df.reset_index(drop=True)
 def kline_1m_live(self,symbol):
  z=self.get("/v5/market/kline",{"category":"linear","symbol":symbol,"interval":"1","limit":2})
  rows=z.get("result",{}).get("list",[]) if z else []
  if not rows:return None
  r=list(reversed(rows))[-1]
  return {"ts":int(safe_float(r[0])),"open":safe_float(r[1]),"high":safe_float(r[2]),"low":safe_float(r[3]),"close":safe_float(r[4]),"volume":safe_float(r[5])}
 def ticker(self,symbol,fresh=False):
  c=None if fresh else self.tcache;k=None if fresh else f"t:{symbol}"
  z=self.get("/v5/market/tickers",{"category":"linear","symbol":symbol},k,c)
  a=z.get("result",{}).get("list",[]) if z else []
  if not a:return {}
  a=a[0]
  return {"symbol":a.get("symbol",""),"last_price":safe_float(a.get("lastPrice")),"change":safe_float(a.get("price24hPcnt"))*100,"funding":safe_float(a.get("fundingRate"),math.nan),"oi":safe_float(a.get("openInterest"),math.nan),"turnover24h":safe_float(a.get("turnover24h"))}
 def all_tickers(self):
  z=self.get("/v5/market/tickers",{"category":"linear"},"ALL_T",self.tcache)
  return z.get("result",{}).get("list",[]) if z else []
 def oi(self,symbol,limit=10):
  z=self.get("/v5/market/open-interest",{"category":"linear","symbol":symbol,"intervalTime":"1h","limit":limit},f"oi:{symbol}:{limit}")
  rows=z.get("result",{}).get("list",[]) if z else []
  if not rows:return pd.DataFrame()
  df=pd.DataFrame(rows)
  for c in ["openInterest","singleOpenInterest","timestamp"]:
   if c in df:df[c]=pd.to_numeric(df[c],errors="coerce")
  col="singleOpenInterest" if "singleOpenInterest" in df and df["singleOpenInterest"].notna().sum()>=2 else "openInterest"
  if col not in df:return pd.DataFrame()
  return df.rename(columns={col:"oi_value"}).sort_values("timestamp").reset_index(drop=True)
 def book(self,symbol):
  z=self.get("/v5/market/orderbook",{"category":"linear","symbol":symbol,"limit":25},f"b:{symbol}")
  return z.get("result",{}) if z else {}
 def instrument(self,symbol):
  z=self.icache.get(symbol)
  if z is not None:return z
  x=self.get("/v5/market/instruments-info",{"category":"linear","symbol":symbol},f"i:{symbol}")
  a=x.get("result",{}).get("list",[]) if x else []
  if not a:return {}
  a=a[0];p=a.get("priceFilter",{});q=a.get("lotSizeFilter",{})
  z={"tick":safe_float(p.get("tickSize")),"step":safe_float(q.get("qtyStep")),"min":safe_float(q.get("minOrderQty")),"status":a.get("status",""),"contractType":a.get("contractType",""),"quoteCoin":a.get("quoteCoin",""),"settleCoin":a.get("settleCoin",""),"baseCoin":a.get("baseCoin",""),"symbol":a.get("symbol","")}
  self.icache.set(symbol,z);return z

BYBIT=BybitClient()

def validate_config():
 if not 0<=Config.MIN_SCORE<=100 or Config.MIN_RR<=0 or Config.MAX_RR<Config.MIN_RR:raise ValueError("score/rr")
 if Config.RISK_PERCENT<=0 or Config.RISK_PERCENT>100:raise ValueError("risk")
 if not(Config.LONG_RSI_MIN<Config.LONG_RSI_MAX and Config.SHORT_RSI_MIN<Config.SHORT_RSI_MAX):raise ValueError("rsi")
 if not(0<Config.MIN_SL_ATR<Config.MAX_SL_ATR):raise ValueError("sl_atr")
 if not(Config.MIN_SCORE<=Config.TIER_B_SCORE<Config.TIER_A_SCORE<=100):raise ValueError("tier")
 if not(Config.FIRST_SIGNAL_SCORE>=Config.TIER_A_SCORE and Config.SECOND_SIGNAL_SCORE>=Config.FIRST_SIGNAL_SCORE and Config.THIRD_SIGNAL_SCORE>=Config.SECOND_SIGNAL_SCORE):raise ValueError("thresholds")
 if not(Config.OVERRIDE_SCORE>=Config.FIRST_SIGNAL_SCORE):raise ValueError("override")
 if Config.EMA_FAST>=Config.EMA_SLOW:raise ValueError("ema")
 if Config.MAX_ACTIVE_SIGNALS<=0 or Config.MAX_DAILY_SIGNALS<=0 or Config.MAX_SIGNALS_TO_SEND<=0:raise ValueError("limits")
 if Config.MONITOR_INTERVAL<=0 or Config.CHECK_INTERVAL<=0:raise ValueError("intervals")
 if Config.MIN_ATR_PCT<0 or Config.MAX_ATR_PCT<=Config.MIN_ATR_PCT:raise ValueError("atr_pct")
 if Config.MIN_VOLUME_RATIO<=0 or Config.VOLUME_WINDOW<=0:raise ValueError("volume")
 if Config.MAX_ABS_FUNDING<0 or Config.MAX_SPREAD_PCT<0:raise ValueError("market_filters")
 if Config.RISK_PERCENT>0 and Config.ACCOUNT_BALANCE<=0:raise ValueError("balance")
 if Config.MAX_HOLD_HOURS<=0:raise ValueError("hold")
 if Config.PARALLEL_WORKERS<=0 or Config.CANDIDATE_LIMIT<=0 or Config.SCAN_TOP_N<=0:raise ValueError("scan")
 if len(Config.SIGNAL_SCORE_THRESHOLDS)<3:raise ValueError("thresholds")
# ============================================
# HİSSƏ 3/8 — Indicators + Structure
# ============================================
class Indicators:
 @staticmethod
 def ema(s,n):return s.ewm(span=n,adjust=False).mean()
 @staticmethod
 def rsi(s,n=14):
  d=s.diff();g=d.clip(lower=0);l=-d.clip(upper=0);ag=g.ewm(alpha=1/n,adjust=False).mean();al=l.ewm(alpha=1/n,adjust=False).mean()
  rs=ag/al.replace(0,np.nan);r=100-100/(1+rs);r=r.where(al!=0,100);r=r.where(~((ag==0)&(al==0)),50);return r
 @staticmethod
 def atr(df,n=14):
  pc=df.close.shift(1);tr=pd.concat([df.high-df.low,(df.high-pc).abs(),(df.low-pc).abs()],axis=1).max(axis=1)
  return tr.ewm(alpha=1/n,adjust=False).mean()
 @staticmethod
 def add(df):
  x=df.copy()
  x["ema_fast"]=Indicators.ema(x.close,Config.EMA_FAST);x["ema_slow"]=Indicators.ema(x.close,Config.EMA_SLOW)
  x["rsi"]=Indicators.rsi(x.close,Config.RSI_PERIOD);x["atr"]=Indicators.atr(x,Config.ATR_PERIOD)
  x["volume_ma"]=x.volume.rolling(Config.VOLUME_LOOKBACK,min_periods=Config.VOLUME_LOOKBACK).mean()
  x["volume_ratio"]=x.volume/x.volume_ma.replace(0,np.nan)
  x["ema_distance_pct"]=(x.ema_fast-x.ema_slow)/x.close.replace(0,np.nan)*100
  x["atr_pct"]=x.atr/x.close.replace(0,np.nan)*100
  return x

class Structure:
 _cache={};_lock=threading.RLock()
 @staticmethod
 def swings(df,l=5,r=5):
  if not valid(df,l+r+3):return [],[]
  h=np.asarray(df.high,dtype=np.float64);lo=np.asarray(df.low,dtype=np.float64);n=len(df)
  key=(l,r,n,hashlib.blake2b(h.tobytes()+lo.tobytes(),digest_size=16).digest())
  with Structure._lock:
   c=Structure._cache.get(key)
   if c is not None:return c
  sh=[];sl=[]
  for i in range(l,n-r):
   if h[i]>=np.max(h[i-l:i]) and h[i]>np.max(h[i+1:i+r+1]):sh.append((i,float(h[i])))
   if lo[i]<=np.min(lo[i-l:i]) and lo[i]<np.min(lo[i+1:i+r+1]):sl.append((i,float(lo[i])))
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
# ============================================
class Regime:
 @staticmethod
 def analyze(df):
  if len(df)<220:return {"direction":"neutral","quality":0,"structure":"neutral"}
  x=df.iloc[-1];st=Structure.trend(df);dist=abs(safe_float(x.ema_distance_pct));atr_pct=safe_float(x.atr_pct)
  eb=bool(x.close>x.ema_slow and x.ema_fast>x.ema_slow);es=bool(x.close<x.ema_slow and x.ema_fast<x.ema_slow)
  if not(eb or es):return {"direction":"neutral","quality":25,"structure":st}
  if dist<Config.MIN_EMA_DISTANCE_PCT:return {"direction":"neutral","quality":35,"structure":st}
  d="long" if eb else "short";sm=st==("bullish" if eb else "bearish");so=st==("bearish" if eb else "bullish")
  q=min(dist/1.5,1.)*40+min(atr_pct/2.,1.)*20+(40 if sm else 0 if so else 25)
  return {"direction":d,"quality":min(100,max(0,q)),"structure":st}

class Strategy:
 @staticmethod
 def setup(df,d):
  if len(df)<100:return False
  x=df.iloc[-1]
  return bool(x.ema_fast>x.ema_slow and x.close>x.ema_slow) if d=="long" else bool(x.ema_fast<x.ema_slow and x.close<x.ema_slow)

 @staticmethod
 def pullback(df,d):
  if len(df)<8:return False
  x=df.iloc[-7:-1]
  ema=x.ema_fast.astype(float);atr=x.atr.astype(float)
  ok=(atr>0)&ema.notna()&atr.notna()
  if not ok.any():return False
  if d=="long":return bool((x.low[ok]<=ema[ok]+atr[ok]*1.5).any())
  return bool((x.high[ok]>=ema[ok]-atr[ok]*1.5).any())

 @staticmethod
 def rsi_allowed(df,d):
  r=safe_float(df.rsi.iloc[-1],math.nan)
  if not math.isfinite(r):return False
  return Config.LONG_RSI_MIN<=r<=Config.LONG_RSI_MAX if d=="long" else Config.SHORT_RSI_MIN<=r<=Config.SHORT_RSI_MAX

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
  if atr<=0 or abs(x.close-x.open)<atr*.25:return False
  if d=="long":return bool(x.close>x.open and x.close>=p.close and x.close>=x.high-atr*.30)
  return bool(x.close<x.open and x.close<=p.close and x.close<=x.low+atr*.30)

 @staticmethod
 def bos(df,d):
  sh,sl=Structure.swings(df);n=len(df);sw=sh if d=="long" else sl;cut=max(0,n-Config.BOS_LOOKBACK-15);cands=[]
  for si,lv in sw:
   if si<cut or si>=n-3:continue
   for i in range(si+1,n):
    atr=safe_float(df.atr.iloc[i],math.nan)
    if not math.isfinite(atr) or atr<=0:continue
    x=df.iloc[i];buf=atr*Config.BOS_BUFFER_ATR
    if d=="long":
     cb=x.close>=lv+buf;rc=Config.ALLOW_RECLAIM_BOS and Strategy.strong(df,i,d) and x.close>=lv and x.high>=lv
    else:
     cb=x.close<=lv-buf;rc=Config.ALLOW_RECLAIM_BOS and Strategy.strong(df,i,d) and x.close<=lv and x.low<=lv
    if cb or rc:
     cands.append({"level":lv,"bar":i,"swing":si,"type":"CLOSE_BOS" if cb else "RECLAIM_BOS"});break
  if not cands:return None
  best=max(cands,key=lambda z:z["bar"]);age=n-1-best["bar"];max_age=Config.RETEST_MAX_BARS+Config.CONFIRMATION_MAX_BARS_AFTER_RETEST+5
  return best if age<=max_age else None

 @staticmethod
 def sequence(df,bos,d):
  if not bos:return None
  lv=bos["level"];start=bos["bar"]+1;end=min(len(df),start+Config.RETEST_MAX_BARS+1);touch_idx=None
  for i in range(start,end):
   x=df.iloc[i];atr=safe_float(x.atr)
   if atr<=0:continue
   touched=x.low<=lv+atr*Config.RETEST_ATR_DISTANCE if d=="long" else x.high>=lv-atr*Config.RETEST_ATR_DISTANCE
   held=x.close>=lv if d=="long" else x.close<=lv
   if touched:touch_idx=i
   if touch_idx is None or not held:continue
   for j in range(i+1,min(len(df),i+2+Config.CONFIRMATION_MAX_BARS_AFTER_RETEST)):
    if Strategy.strong(df,j,d):
     if d=="long" and df.close.iloc[j]>lv:return {"bos":bos["bar"],"retest":touch_idx,"confirm":j,"level":lv}
     if d=="short" and df.close.iloc[j]<lv:return {"bos":bos["bar"],"retest":touch_idx,"confirm":j,"level":lv}
  return None

 @staticmethod
 def zone_score(df,d):
  hi=safe_float(df.high.iloc[-50:].max());lo=safe_float(df.low.iloc[-50:].min());p=safe_float(df.close.iloc[-1]);atr=safe_float(df.atr.iloc[-1])
  if atr<=0 or hi<=lo:return 0
  mid=(hi+lo)/2
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
  vr=safe_float(df.volume_ratio.iloc[confirm_idx],math.nan) if confirm_idx is not None and 0<=confirm_idx<len(df) else safe_float(df.volume_ratio.iloc[-Config.VOLUME_WINDOW:].max(),math.nan)
  return 0 if not math.isfinite(vr) else min(100,vr*60)
 @staticmethod
 def funding(t):
  if not t:return False
  f=safe_float(t.get("funding"),math.nan)
  return math.isfinite(f) and abs(f)<=Config.MAX_ABS_FUNDING
 @staticmethod
 def oi(df):
  if not valid(df,2) or "oi_value" not in df:return False
  a=safe_float(df.oi_value.iloc[-1],math.nan);b=safe_float(df.oi_value.iloc[-min(Config.OI_LOOKBACK,len(df))],math.nan)
  return math.isfinite(a) and math.isfinite(b) and b>0 and (a-b)/b*100>=Config.MIN_OI_CHANGE_PCT
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
   if not Config.BTC_FILTER_ENABLED:return {"allowed":True,"score":50}
   df=BYBIT.klines("BTCUSDT","240",250,cache=BYBIT.regime_cache)
   if not valid(df,220):return {"allowed":True,"score":50}
   df=Indicators.add(df);x=df.iloc[-1]
   bull=bool(x.ema_fast>x.ema_slow and x.close>x.ema_slow);bear=bool(x.ema_fast<x.ema_slow and x.close<x.ema_slow);st=Structure.trend(df)
   if direction=="long":
    if bull and st=="bullish":return {"allowed":True,"score":100}
    if bull:return {"allowed":True,"score":80}
    if not(bull or bear):return {"allowed":True,"score":55}
    if bear and st=="bearish":return {"allowed":False,"score":0}
    return {"allowed":True,"score":40}
   if bear and st=="bearish":return {"allowed":True,"score":100}
   if bear:return {"allowed":True,"score":80}
   if not(bull or bear):return {"allowed":True,"score":55}
   if bull and st=="bullish":return {"allowed":False,"score":0}
   return {"allowed":True,"score":40}
  except Exception as e:
   log.warning("BTC filter error: %s",e);return {"allowed":True,"score":50}

class Correlation:
 @staticmethod
 def allowed(symbol,active):
  if not Config.CORRELATION_FILTER_ENABLED:return True
  a=BYBIT.klines(symbol,"60",80)
  if not valid(a,40):return True
  ra=a.close.pct_change().tail(40);n=0
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
  if atr<=0 or e<=0:return None
  cut=max(0,len(df)-Config.TARGET_LOOKBACK_15)
  if d=="long":
   v=[x for i,x in sl if i>=cut and x<e]
   return max(v)-buf if v else e-atr*Config.MIN_SL_ATR
  v=[x for i,x in sh if i>=cut and x>e]
  return min(v)+buf if v else e+atr*Config.MIN_SL_ATR

 @staticmethod
 def target_candidates(df,d,e,lookback):
  sh,sl=Structure.swings(df);cut=max(0,len(df)-lookback);src=sh if d=="long" else sl
  v=[x for i,x in src if i>=cut and (x>e if d=="long" else x<e)]
  return sorted(set(v),reverse=(d=="short"))

 @staticmethod
 def levels(d15,d1,d4,d,entry=None):
  e=safe_float(entry,safe_float(d15.close.iloc[-1]));atr=safe_float(d15.atr.iloc[-1])
  if e<=0 or atr<=0:return None
  raw=Risk.structural_stop(d15,e,d)
  if raw is None:return None
  risk=abs(e-raw);minrisk=atr*Config.MIN_SL_ATR;maxrisk=atr*Config.MAX_SL_ATR
  if not(minrisk<=risk<=maxrisk):return None
  sl=raw
  cs=Risk.target_candidates(d1,d,e,Config.TARGET_LOOKBACK_1H)+Risk.target_candidates(d15,d,e,Config.TARGET_LOOKBACK_15)+Risk.target_candidates(d4,d,e,Config.TARGET_LOOKBACK_4H)
  cs=sorted(set(cs),reverse=(d=="short"))
  lo=e+risk*Config.MIN_RR if d=="long" else e-risk*Config.MAX_RR
  hi=e+risk*Config.MAX_RR if d=="long" else e-risk*Config.MIN_RR
  for lv in cs:
   tp=lv-atr*Config.TARGET_BUFFER_ATR if d=="long" else lv+atr*Config.TARGET_BUFFER_ATR
   if not(lo<=tp<=hi):continue
   rr=abs(tp-e)/risk
   if Config.MIN_RR<=rr<=Config.MAX_RR:return {"entry":e,"sl":sl,"tp":tp,"risk":risk,"rr":rr}
  return None
# ============================================
# HİSSƏ 6/8 — Scoring + Analyzer + analyze_symbol
# ============================================
class Scoring:
 @staticmethod
 def calculate(rq,d15,d,seq,bos,btc_eval,confirm_idx=None):
  x15=d15.iloc[-1]
  setup_ok=bool(x15.ema_fast>x15.ema_slow and x15.close>x15.ema_slow) if d=="long" else bool(x15.ema_fast<x15.ema_slow and x15.close<x15.ema_slow)
  setup_score=100 if setup_ok else 60
  pb_score=80 if Strategy.pullback(d15,d) else 0
  bos_score=100 if bos and bos.get("type")=="CLOSE_BOS" else 75 if bos else 0
  retest_score=100 if seq else 0
  confirm_score=100 if seq and Strategy.strong(d15,seq["confirm"],d) else 60 if seq else 0
  rsi_s=Strategy.rsi_score(d15,d);vol_s=Filters.volume_score(d15,confirm_idx);zone=Strategy.zone_score(d15,d);btc_s=btc_eval.get("score",50)
  score=(Config.SCORE_W_REGIME*rq+Config.SCORE_W_SETUP*setup_score+Config.SCORE_W_PULLBACK*pb_score+Config.SCORE_W_BOS*bos_score+Config.SCORE_W_RETEST*retest_score+Config.SCORE_W_CONFIRM*confirm_score+Config.SCORE_W_RSI*rsi_s+Config.SCORE_W_VOLUME*vol_s+Config.SCORE_W_ZONE*zone+Config.SCORE_W_BTC*btc_s)
  return round(score,1),rsi_s
 @staticmethod
 def add_rr_to_score(base_score,rr):
  rr_s=100 if rr>=2.8 else 80 if rr>=2.5 else 60 if rr>=2.3 else 40 if rr>=2.2 else 0
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
  ok=valid(d4,220) and valid(d1,220) and valid(d15,80);STORE.add_stage("DATA",ok)
  if not ok:self.reject="DATA";return None
  d4=Indicators.add(d4);d1=Indicators.add(d1);d15=Indicators.add(d15)
  reg=Regime.analyze(d4);d=reg["direction"]
  if d=="neutral":
   x4=d4.iloc[-1]
   if not(x4.close>x4.ema_slow and x4.ema_fast>x4.ema_slow) and not(x4.close<x4.ema_slow and x4.ema_fast<x4.ema_slow):STORE.add_bos_debug("4H_NO_EMA_ALIGN")
   else:STORE.add_bos_debug("4H_LOW_DISTANCE")
  def gate(ok,name):
   STORE.add_stage(name,ok);return ok
  if not gate(d!="neutral","4H_TREND"):self.reject="4H_TREND";return None
  if not gate(Strategy.setup(d1,d),"1H_SETUP"):self.reject="1H_SETUP";return None
  if not gate(Strategy.pullback(d1,d),"1H_PULLBACK"):self.reject="1H_PULLBACK";return None
  if not gate(Strategy.rsi_allowed(d15,d),"RSI"):self.reject="RSI";return None
  if not gate(Filters.volatility(d15),"ATR"):self.reject="ATR";return None
  bos=Strategy.bos(d15,d);STORE.add_stage("BOS",bool(bos));STORE.add_bos_debug(bos.get("type") if bos else "NO_BREAK")
  if not bos:self.reject="BOS";return None
  seq=Strategy.sequence(d15,bos,d);STORE.add_stage("RETEST_CONFIRM",bool(seq))
  if not seq:self.reject="RETEST_CONFIRM";return None
  age=len(d15)-1-seq["confirm"];ok=age<=Config.MAX_CONFIRM_AGE;STORE.add_stage("CONFIRM_AGE",ok)
  if not ok:self.reject="CONFIRM_AGE";return None
  atr_now=safe_float(d15.atr.iloc[-1]);confirm_close=safe_float(d15.close.iloc[seq["confirm"]])
  t=BYBIT.ticker(s,True);live_entry=safe_float(t.get("last_price"),0)
  ok=live_entry>0 and atr_now>0
  if ok:
   ext=abs(live_entry-confirm_close);ok=ext<=atr_now*Config.MAX_ENTRY_EXTENSION_ATR
  STORE.add_stage("ENTRY_CHASE",ok)
  if not ok:self.reject="ENTRY_CHASE";return None
  ok=Filters.volume(d15);STORE.add_stage("VOLUME",ok)
  if not ok:self.reject="VOLUME";return None
  ok=bool(t);STORE.add_stage("TICKER",ok)
  if not ok:self.reject="TICKER";return None
  ok=Filters.funding(t);STORE.add_stage("FUNDING",ok)
  if not ok:self.reject="FUNDING";return None
  oi_df=BYBIT.oi(s);ok=Filters.oi(oi_df);STORE.add_stage("OI",ok)
  if not ok:self.reject="OI";return None
  book=BYBIT.book(s);ok=Filters.spread(book);STORE.add_stage("SPREAD",ok)
  if not ok:self.reject="SPREAD";return None
  btc_eval=BTCFilter.evaluate(d);ok=btc_eval.get("allowed",True);STORE.add_stage("BTC_FILTER",ok)
  if not ok:self.reject="BTC_FILTER";return None
  base_score,rs=Scoring.calculate(reg["quality"],d15,d,seq,bos,btc_eval,seq["confirm"])
  lv=Risk.levels(d15,d1,d4,d,entry=live_entry);ok=bool(lv);STORE.add_stage("TARGET_RISK",ok)
  if not ok:
   STORE.add_scan_score(s,base_score,"–",rr_included=False);self.reject="TARGET_RISK";return None
  score=Scoring.add_rr_to_score(base_score,lv["rr"]);tier=Scoring.tier(score,lv["rr"]);STORE.add_scan_score(s,score,tier,rr_included=True)
  ok=score>=Config.MIN_SCORE;STORE.add_stage("SCORE",ok)
  if not ok:self.reject="SCORE";return None
  return {"id":str(uuid.uuid4()),"symbol":s,"direction":d,"score":score,"rsi":safe_float(d15.rsi.iloc[-1]),"rsi_score":rs,"entry":lv["entry"],"sl":lv["sl"],"tp":lv["tp"],"rr":lv["rr"],"risk":lv["risk"],"atr_pct":safe_float(d15.atr_pct.iloc[-1]),"volume_ratio":safe_float(d15.volume_ratio.iloc[-Config.VOLUME_WINDOW:].max()),"funding":safe_float(t.get("funding"),0),"oi":safe_float(t.get("oi"),0),"regime_4h":d,"setup_1h":"ok","confirm_15m":True,"btc_score":btc_eval.get("score",50),"created_at":utc_iso(),"created_ts":time.time(),"start_ts":int(time.time()*1000),"last_checked_candle":0}

def analyze_symbol(s):
 try:
  a=Analyzer(s);x=a.run();return x,("" if x else a.reject)
 except Exception as e:
  tb=traceback.format_exc();log.error("ANALYZE ERROR [%s]:\n%s",s,tb);STORE.add_error(s,str(e),short_tb(tb));return None,"ERROR"
# ============================================
# HİSSƏ 7/8 — Store + Performance + Telegram
# ============================================
class Store:
 def __init__(self):
  self.lock=threading.RLock();self.active=[];self.closed=[];self.rejections={};self.stages={};self.errors=[];self.scan_scores=[]
  self.day=utc_now().date().isoformat();self.daily_count=0;self.override_count=0
  self.start_balance=Config.ACCOUNT_BALANCE;self.balance=Config.ACCOUNT_BALANCE
 def reset_day(self):
  d=utc_now().date().isoformat()
  with self.lock:
   if d!=self.day:self.day=d;self.daily_count=0;self.override_count=0
 def reset_rejections(self):
  with self.lock:self.rejections={};self.stages={};self.scan_scores=[]
 def add_scan_score(self,symbol,score,tier,rr_included=True):
  with self.lock:self.scan_scores.append((symbol,score,tier,rr_included))
 def scan_score_report(self):
  with self.lock:s=list(self.scan_scores)
  if not s:return "Bu skanda BTC_FILTER-i keçən (score hesablanan) namizəd olmadı."
  s.sort(key=lambda x:-x[1]);top=s[:5]
  def fmt(sym,sc,tr,rr_ok):return f"{sym}: {sc}/100 {'['+tr+']' if rr_ok else '[RR tapılmadı]'}"
  lines="\n".join(f"{i+1}. {fmt(*row)}" for i,row in enumerate(top));avg=round(sum(x[1] for x in s)/len(s),1)
  return f"Ən yüksək: {fmt(*top[0])}\nOrta: {avg} (n={len(s)})\n{lines}"
 def add_stage(self,name,ok):
  with self.lock:
   x=self.stages.setdefault(name,{"pass":0,"fail":0});x["pass" if ok else "fail"]+=1
 def add_bos_debug(self,name):
  with self.lock:
   q=self.stages.setdefault("BOS_DEBUG",{});q[name]=q.get(name,0)+1
 def bos_report(self):
  with self.lock:q=dict(self.stages.get("BOS_DEBUG",{}))
  return " | ".join(f"{k}: {v}" for k,v in q.items()) or "Yoxdur"
 def stage_report(self,n=0):
  order=["DATA","4H_TREND","1H_SETUP","1H_PULLBACK","RSI","ATR","BOS","RETEST_CONFIRM","CONFIRM_AGE","ENTRY_CHASE","VOLUME","TICKER","FUNDING","OI","SPREAD","BTC_FILTER","TARGET_RISK","SCORE"]
  with self.lock:q=dict(self.stages)
  return "\n".join(f"{x}: {q.get(x,{}).get('pass',0)}/{q.get(x,{}).get('pass',0)+q.get(x,{}).get('fail',0)} keçdi | {q.get(x,{}).get('fail',0)} keçmədi" for x in order)
 def add_rejection(self,s,r):
  with self.lock:self.rejections[s]=r or "UNKNOWN"
 def rejection_stats(self):
  with self.lock:
   q={}
   for v in self.rejections.values():q[v]=q.get(v,0)+1
   return q
 def add_error(self,s,err,detail=""):
  with self.lock:self.errors=(self.errors+[{"symbol":s,"error":err,"detail":detail,"time":utc_iso()}])[-Config.MAX_ERRORS_KEPT:]
 def error_report(self):
  with self.lock:q=list(self.errors)
  if not q:return "Xəta yoxdur."
  return "\n".join(f"❌ {e['symbol']}: {e['error']}\n   └ {e['detail']}" for e in q[-5:])
 def active_symbols(self):
  with self.lock:return [x["symbol"] for x in self.active]
 def has_active(self,symbol,direction=None):
  with self.lock:
   return any(a.get("symbol")==symbol and (direction is None or a.get("direction")==direction) for a in self.active)
 def can_add(self,score=0):
  self.reset_day()
  with self.lock:
   if len(self.active)>=Config.MAX_ACTIVE_SIGNALS:return False
   if self.daily_count<Config.MAX_DAILY_SIGNALS:return True
   return score>=Config.OVERRIDE_SCORE and self.override_count<Config.MAX_OVERRIDE_DAILY
 def add(self,x):
  self.reset_day()
  with self.lock:
   if len(self.active)>=Config.MAX_ACTIVE_SIGNALS:return False
   if self.has_active(x["symbol"],x["direction"]):return False
   x.setdefault("id",str(uuid.uuid4()))
   is_override=bool(x.get("override",False))
   if self.daily_count>=Config.MAX_DAILY_SIGNALS:
    if not(is_override and x.get("score",0)>=Config.OVERRIDE_SCORE and self.override_count<Config.MAX_OVERRIDE_DAILY):return False
    self.override_count+=1
   risk_amount=self.balance*Config.RISK_PERCENT/100
   x["risk_amount"]=risk_amount
   self.active.append(x);self.daily_count+=1;self.save();return True
 def close_position(self,signal_id,x,result,exit_price,result_r):
  if not signal_id:
   log.error("close_position refused: missing signal id")
   return None
  with self.lock:
   idx=next((i for i,a in enumerate(self.active) if a.get("id")==signal_id),None)
   if idx is None:
    log.warning("close_position: signal id not found: %s",signal_id);return None
   a=self.active.pop(idx);y=dict(a);y.update({"closed_at":utc_iso(),"result":result,"exit_price":exit_price,"result_r":result_r})
   risk_amount=safe_float(y.get("risk_amount"),self.balance*Config.RISK_PERCENT/100);entry=safe_float(y.get("entry"));risk_dist=safe_float(y.get("risk"))
   qty=risk_amount/risk_dist if entry>0 and risk_dist>0 else 0
   notional=qty*entry if qty>0 else risk_amount
   commission=notional*Config.TAKER_FEE_PCT/100*2;slippage=notional*Config.SLIPPAGE_PCT/100*2
   gross=result_r*risk_amount;net=gross-commission-slippage
   y["gross_pnl"]=round(gross,4);y["commission"]=round(commission,4);y["slippage"]=round(slippage,4);y["pnl"]=round(net,4)
   y["duration_h"]=round((time.time()-safe_float(a.get("created_ts"),time.time()))/3600,2)
   self.balance+=net;self.closed=(self.closed+[y])[-Config.MAX_CLOSED_SIGNALS_KEPT:];self.save();return y
 def save(self):
  try:
   tmp=Config.SIGNAL_FILE+".tmp"
   with open(tmp,"w",encoding="utf8") as f:json.dump({"active":self.active,"closed":self.closed,"day":self.day,"daily_count":self.daily_count,"override_count":self.override_count,"balance":self.balance,"start_balance":self.start_balance},f,indent=2)
   os.replace(tmp,Config.SIGNAL_FILE)
  except Exception as e:log.warning("save: %s",e)
 def load(self):
  try:
   with open(Config.SIGNAL_FILE,encoding="utf8") as f:x=json.load(f)
   self.active=x.get("active",[]);self.closed=x.get("closed",[])
   used=set()
   for a in self.active:
    aid=str(a.get("id") or "")
    if not aid or aid in used:a["id"]=str(uuid.uuid4())
    used.add(a["id"])
   self.day=x.get("day",self.day);self.daily_count=x.get("daily_count",0);self.override_count=x.get("override_count",0)
   self.balance=x.get("balance",Config.ACCOUNT_BALANCE);self.start_balance=x.get("start_balance",Config.ACCOUNT_BALANCE)
   self.reset_day()
  except Exception as e:log.warning("load state: %s",e)

STORE=Store();STORE.load()

class Performance:
 @staticmethod
 def hourly():
  buckets={}
  with STORE.lock:c=list(STORE.closed)
  for x in c:
   try:h=int(x.get("created_at","")[11:13])
   except:continue
   b=buckets.setdefault(h,{"tp":0,"sl":0,"time":0,"r":0.,"pnl":0.});res=x.get("result")
   if res=="TP":b["tp"]+=1
   elif res=="SL":b["sl"]+=1
   elif res=="TIME":b["time"]+=1
   b["r"]+=safe_float(x.get("result_r"));b["pnl"]+=safe_float(x.get("pnl"))
  return buckets
 @staticmethod
 def stats():
  with STORE.lock:c=list(STORE.closed);balance=STORE.balance;start=STORE.start_balance;today=STORE.daily_count;active=len(STORE.active)
  tp=sum(x.get("result")=="TP" for x in c);sl=sum(x.get("result")=="SL" for x in c);tm=sum(x.get("result")=="TIME" for x in c);closed=tp+sl+tm;dec=tp+sl
  wr=round(tp/dec*100,2) if dec else 0.;tr=round(tp/closed*100,2) if closed else 0.;nr=round(sum(safe_float(x.get("result_r")) for x in c),2);pnl=round(sum(safe_float(x.get("pnl")) for x in c),2)
  ts={}
  for x in c:
   t=x.get("tier","?");z=ts.setdefault(t,{"w":0,"l":0,"t":0,"pnl":0.});r=x.get("result")
   if r=="TP":z["w"]+=1
   elif r=="SL":z["l"]+=1
   elif r=="TIME":z["t"]+=1
   z["pnl"]+=safe_float(x.get("pnl"))
  return {"start_balance":round(start,2),"balance":round(balance,2),"closed":closed,"tp":tp,"sl":sl,"time":tm,"wins":tp,"losses":sl,"winrate":wr,"tprate":tr,"net_r":nr,"pnl":pnl,"avg_r":round(nr/dec,2) if dec else 0.,"today_signals":today,"active":active,"tier_stats":ts}

class Telegram:
 @staticmethod
 def send(text):
  if not Config.BOT_TOKEN:return False
  try:return requests.post(f"https://api.telegram.org/bot{Config.BOT_TOKEN}/sendMessage",json={"chat_id":Config.CHAT_ID,"text":text},timeout=10).ok
  except Exception as e:log.warning("tg: %s",e);return False
 @staticmethod
 def signal(x):
  d="🟢 LONG" if x["direction"]=="long" else "🔴 SHORT";tier=x.get("tier","C");te={"A":"🥇 ELITE","B":"🥈 GOOD","C":"🥉 STD"}.get(tier,"STD")
  if x.get("override"):te="⚡ OVERRIDE"
  risk=x.get("risk_amount",0)
  body=(f"🚨 YENİ SİQNAL #{STORE.daily_count}  {te}\n━━━━━━━━━━━━━━━━━━\n📌 {x['symbol']}  {d}\n━━━━━━━━━━━━━━━━━━\n🎯 Score: {x['score']}/100\n📊 RSI: {x['rsi']:.1f}\n🏛 4H: {x.get('regime_4h','?').upper()}\n⏱ Setup: {x.get('setup_1h','ok')} | 15M: {x.get('confirm_15m',False)}\n💰 Entry: {x['entry']:.8g}\n🛑 SL: {x['sl']:.8g}\n✅ TP: {x['tp']:.8g}\n⚖️ RR: 1:{x['rr']:.2f}\n📈 ATR: {x['atr_pct']:.2f}% 🔊 Vol: {x['volume_ratio']:.2f}x\n🪙 BTC: {x.get('btc_score',50)}/100\n💵 Risk: {risk:.2f} USDT\n━━━━━━━━━━━━━━━━━━\n⚠️ Signal only — öz analizinlə")
  ok=Telegram.send(body)
  if ok:Telegram.send(Telegram.status())
  return ok
 @staticmethod
 def closed(y):
  d="🟢 LONG" if y["direction"]=="long" else "🔴 SHORT";em={"TP":"✅","SL":"🔴","TIME":"⏱"}.get(y.get("result"),"❓")
  return Telegram.send(f"{em} CLOSED [{y.get('tier','?')}]\n{y['symbol']} {d}\nResult: {y.get('result')}\nEntry: {y.get('entry',0):.8g}\nExit: {y.get('exit_price',0):.8g}\nR: {y.get('result_r',0):.2f}\nGross: {y.get('gross_pnl',0):.2f} USDT\nComm: -{y.get('commission',0):.2f} | Slip: -{y.get('slippage',0):.2f}\nNet PnL: {y.get('pnl',0):.2f} USDT\nBalance: {STORE.balance:.2f} USDT\nDuration: {y.get('duration_h',0):.1f}h")
 @staticmethod
 def status():
  with STORE.lock:a=list(STORE.active);dc=STORE.daily_count
  if not a:return "📭 Aktiv siqnal yoxdur."
  lines=[f"📌 AKTİV SIQNALLAR ({len(a)}) — {dc}/{Config.MAX_DAILY_SIGNALS} bu gün\n"]
  for x in a:
   t=x.get("tier","C");em="⚡" if x.get("override") else {"A":"🥇","B":"🥈","C":"🥉"}.get(t,"")
   lines.append(f"{em} {x['symbol']} {x['direction'].upper()}\nScore: {x['score']} | Entry: {x['entry']:.8g}\nSL: {x['sl']:.8g} | TP: {x['tp']:.8g} | RR: 1:{x['rr']:.2f}")
  return "\n".join(lines)
 @staticmethod
 def scan_done(n,found,sent,tier_a=0,tier_b=0,tier_c=0):
  q=STORE.rejection_stats();r=" | ".join(f"{k}: {v}" for k,v in sorted(q.items(),key=lambda z:-z[1])) if q else "Yoxdur";errs=STORE.error_report();eb=f"\n\n⚠️ XƏTALAR\n{errs}" if "❌" in errs else ""
  return Telegram.send(f"✅ SCAN TAMAMLANDI\n\nAnaliz olunan: {n}\nUyğun setup: {len(found)}\n🥇 Elite: {tier_a} | 🥈 Good: {tier_b} | 🥉 Std: {tier_c}\nGöndərilən: {sent}\nAktiv: {len(STORE.active)}/{Config.MAX_ACTIVE_SIGNALS}\nBugün: {STORE.daily_count}/{Config.MAX_DAILY_SIGNALS} (override: {STORE.override_count}/{Config.MAX_OVERRIDE_DAILY})\n\n📋 ŞƏRTLƏR\n{STORE.stage_report(n)}\n\n📈 SCORE-LAR (bu skan)\n{STORE.scan_score_report()}\n\n🔎 BOS DETALLI\n{STORE.bos_report()}\n\n🚫 İLK UĞURSUZ ŞƏRT\n{r}{eb}")
 @staticmethod
 def handle(text):
  t=(text or "").strip().lower()
  try:
   if t in ("/status","/signals"):return Telegram.status()
   if t=="/stats":
    s=Performance.stats();base=(f"📊 STATS\n💰 Balance: {s['balance']:.2f} (start {s['start_balance']:.2f})\n📦 Closed: {s['closed']}\n✅ TP: {s['tp']}\n🔴 SL: {s['sl']}\n⏱ TIME: {s['time']}\n📈 Win Rate: {s['winrate']:.2f}% (TP/(TP+SL))\n🎯 TP Rate: {s['tprate']:.2f}% (TP/Closed)\n💵 Net R: {s['net_r']:.2f}R\n💵 Net PnL: {s['pnl']:.2f} USDT\n⚖️ Avg R: {s['avg_r']:.2f}\n📌 Aktiv: {s['active']} | Bugün: {s['today_signals']}")
    ts=s.get("tier_stats",{})
    if ts:
     base+="\n\n🥇 TIER STATS"
     for tr in ["A","B","C"]:
      if tr in ts:
       x=ts[tr];tot=x["w"]+x["l"];wr=round(x["w"]/tot*100,1) if tot else 0;base+=f"\n{tr}: {x['w']}W/{x['l']}L/{x['t']}T ({wr}%) | {x['pnl']:.2f} USDT"
    return base
   if t=="/rejections":
    q=STORE.rejection_stats();body="\n".join(f"{k}: {v}" for k,v in sorted(q.items(),key=lambda z:-z[1])) if q else "Yoxdur.";return f"🚫 REJECTIONS\n{body}"
   if t=="/errors":return f"⚠️ XƏTALAR\n{STORE.error_report()}"
   if t in ("/hourly","/saat"):
    b=Performance.hourly()
    if not b:return "📊 SAAT ÜZRƏ\nHələ bağlanmış əməliyyat yoxdur."
    lines=["📊 SAAT ÜZRƏ (UTC, giriş vaxtına görə)"]
    for h in sorted(b):
     x=b[h];tot=x["tp"]+x["sl"];wr=round(x["tp"]/tot*100,1) if tot else 0;lines.append(f"{h:02d}:00 — {x['tp']}W/{x['sl']}L/{x['time']}T ({wr}%) | {x['r']:+.2f}R | {x['pnl']:+.2f} USDT")
    lines.append("\n(Hər saat bucket-i az sayda əməliyyat olduqca rəqəmlər etibarsızdır — nümunə böyüdükcə mənalı olur)")
    return "\n".join(lines)
   if t=="/help":return "/status — aktiv siqnallar\n/stats — statistika\n/scan — manual skan\n/rejections — rədd səbəbləri\n/errors — xətalar\n/hourly — saat üzrə nəticə\n/help — bu mesaj"
   return None
  except Exception as e:
   log.warning("telegram handle: %s",e);return "⚠️ Komanda icra olunarkən xəta baş verdi."
 # ============================================
# HİSSƏ 8/8 — PositionManager + Scanner + Flask + Main
# ============================================
class PositionManager:
 def check(self,x):
  symbol=x["symbol"];d=x["direction"];entry=safe_float(x.get("entry"),math.nan);sl=safe_float(x.get("sl"),math.nan);tp=safe_float(x.get("tp"),math.nan)
  if not all(math.isfinite(v) for v in [entry,sl,tp]):return
  age=(time.time()-safe_float(x.get("created_ts"),time.time()))/3600
  k=BYBIT.kline_1m_live(symbol)
  if not k:return
  hi=safe_float(k.get("high"),math.nan);lo=safe_float(k.get("low"),math.nan);close=safe_float(k.get("close"),math.nan)
  if not all(math.isfinite(v) for v in [hi,lo,close]):return
  ts=k.get("ts",0);ts=ts/1000 if ts>1e11 else ts
  x["last_checked_candle"]=ts
  result=None;exit_price=None;result_r=None
  if d=="long":
   sl_hit=lo<=sl;tp_hit=hi>=tp
   if sl_hit:result,exit_price,result_r="SL",sl,-1.
   elif tp_hit:result,exit_price,result_r="TP",tp,safe_float(x.get("rr"))
  else:
   sl_hit=hi>=sl;tp_hit=lo<=tp
   if sl_hit:result,exit_price,result_r="SL",sl,-1.
   elif tp_hit:result,exit_price,result_r="TP",tp,safe_float(x.get("rr"))
  if not result and age>=Config.MAX_HOLD_HOURS:
   result="TIME";exit_price=close
   result_r=(close-entry)/(entry-sl) if d=="long" and entry!=sl else (entry-close)/(sl-entry) if d=="short" and sl!=entry else 0.
  if result:
   y=STORE.close_position(x.get("id"),x,result,exit_price,result_r)
   if y:Telegram.closed(y)
 def run(self):
  while not STOP_EVENT.is_set():
   try:
    for x in list(STORE.active):self.check(x)
   except Exception:
    log.error("MONITOR ERROR:\n%s",traceback.format_exc())
   STOP_EVENT.wait(Config.MONITOR_INTERVAL)

MANAGER=PositionManager()

class Scanner:
 def __init__(self):self.lock=threading.Lock();self.running=False
 def symbols(self):
  try:
   tickers=BYBIT.all_tickers();c=[];usdt=turnpos=trad=0
   for x in tickers:
    s=x.get("symbol","");turn=safe_float(x.get("turnover24h"))
    if not s.endswith("USDT"):continue
    usdt+=1
    if turn>0:turnpos+=1
    base=s[:-4]
    if turn>0 and base not in Config.TRADFI:trad+=1;c.append((s,turn))
   c.sort(key=lambda z:z[1],reverse=True);top=[s for s,_ in c[:Config.CANDIDATE_LIMIT]];out=[]
   def chk(s):
    i=BYBIT.instrument(s)
    if not i or i.get("status")!="Trading" or i.get("contractType")!="LinearPerpetual" or i.get("quoteCoin")!="USDT" or i.get("settleCoin")!="USDT":return None
    return s
   with ThreadPoolExecutor(max_workers=Config.PARALLEL_WORKERS) as ex:
    for s in ex.map(chk,top):
     if s:out.append(s)
     if len(out)>=Config.SCAN_TOP_N:break
   log.info("Symbols: %d / %d",len(out),len(c))
   if Config.DEBUG_SYMBOLS_ENABLED:Telegram.send(f"🔎 SYMBOLS: {len(out)} coin\nall={len(tickers)} usdt={usdt} turn>0={turnpos} trad={trad}")
   return out or Config.FALLBACK_COINS
  except Exception as e:
   tb=traceback.format_exc();log.error("SYMBOLS ERROR:\n%s",tb);STORE.add_error("SYMBOLS",str(e),short_tb(tb));return Config.FALLBACK_COINS
 def scan(self,force=False):
  if not self.lock.acquire(False):return
  if Config.SCAN_HOURS_ENABLED and not force:
   h=utc_now().hour
   if not Config.SCAN_HOUR_START<=h<Config.SCAN_HOUR_END:
    log.info("Skan vaxtı deyil (%02d:00 UTC)",h);self.lock.release();return
  self.running=True;found=[];sent=0;symbols=[];tier_a=tier_b=tier_c=0
  try:
   STORE.reset_day();STORE.reset_rejections()
   if len(STORE.active)>=Config.MAX_ACTIVE_SIGNALS:
    log.info("Active limit doldu");return
   if STORE.daily_count>=Config.MAX_DAILY_SIGNALS and STORE.override_count>=Config.MAX_OVERRIDE_DAILY:
    log.info("Daily+override doldu");return
   symbols=self.symbols()
   with ThreadPoolExecutor(max_workers=Config.PARALLEL_WORKERS) as ex:
    fs={ex.submit(analyze_symbol,s):s for s in symbols}
    for f in as_completed(fs):
     s=fs[f]
     try:x,r=f.result()
     except Exception as e:
      tb=traceback.format_exc();log.error("SCAN THREAD ERROR [%s]:\n%s",s,tb);STORE.add_error(s,str(e),short_tb(tb));x,r=None,"ERROR"
     if x:found.append(x)
     else:STORE.add_rejection(s,r)
   found.sort(key=lambda z:z["score"],reverse=True)
   for x in found:x["tier"]=Scoring.tier(x["score"],x["rr"])
   tier_a=sum(x["tier"]=="A" for x in found);tier_b=sum(x["tier"]=="B" for x in found);tier_c=sum(x["tier"]=="C" for x in found)
   ordered=[x for x in found if x["tier"]=="A"]+[x for x in found if x["tier"]=="B"]+[x for x in found if x["tier"]=="C"]
   to_send=[];simulated_dc=STORE.daily_count
   for x in ordered:
    if len(to_send)>=Config.MAX_SIGNALS_TO_SEND:break
    if len(STORE.active)+len(to_send)>=Config.MAX_ACTIVE_SIGNALS:break
    score=x["score"]
    if simulated_dc<Config.MAX_DAILY_SIGNALS:
     idx=min(simulated_dc,len(Config.SIGNAL_SCORE_THRESHOLDS)-1);threshold=Config.SIGNAL_SCORE_THRESHOLDS[idx]
     if score<threshold:
      STORE.add_rejection(x["symbol"],f"SIGNAL_{simulated_dc+1}_LOW");continue
     to_send.append(x);simulated_dc+=1
    elif score>=Config.OVERRIDE_SCORE and STORE.override_count<len(to_send)+Config.MAX_OVERRIDE_DAILY:
     x["override"]=True;to_send.append(x);simulated_dc+=1
    else:STORE.add_rejection(x["symbol"],"OVERRIDE_LOW")
   for x in to_send:
    if not Correlation.allowed(x["symbol"],STORE.active_symbols()):
     STORE.add_rejection(x["symbol"],"CORRELATION");continue
    if not STORE.add(x):
     STORE.add_rejection(x["symbol"],"ADD_FAILED");continue
    if Telegram.signal(x):sent+=1
    if x.get("override"):Telegram.send(f"⚡ OVERRIDE SİQNAL\nScore {x['score']} ≥ {Config.OVERRIDE_SCORE}\nGündəlik limit artırıldı")
   Telegram.scan_done(len(symbols),found,sent,tier_a,tier_b,tier_c)
   log.info("SCAN | %d | valid=%d | A=%d | B=%d | C=%d | sent=%d",len(symbols),len(found),tier_a,tier_b,tier_c,sent)
  finally:
   self.running=False;self.lock.release()

SCANNER=Scanner()

class ScannerWorker:
 def run(self):
  STOP_EVENT.wait(15)
  while not STOP_EVENT.is_set():
   try:SCANNER.scan()
   except Exception as e:
    tb=traceback.format_exc();log.error("SCANNER WORKER ERROR:\n%s",tb);STORE.add_error("SCANNER",str(e),short_tb(tb));Telegram.send(f"⚠️ SCANNER XƏTA\n{str(e)[:200]}\n\n{short_tb(tb,300)}")
   STOP_EVENT.wait(Config.CHECK_INTERVAL)

class TelegramPoller:
 def __init__(self):
  self.offset=0;self._offset_file=os.path.join(Config.DATA_DIR,"tg_offset.txt")
  try:
   with open(self._offset_file) as f:self.offset=int(f.read().strip())
  except:pass
 def _save_offset(self):
  try:
   with open(self._offset_file,"w") as f:f.write(str(self.offset))
  except:pass
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
       threading.Thread(target=SCANNER.scan,kwargs={"force":True},daemon=True).start()
     else:
      ans=Telegram.handle(text)
      if ans:Telegram.send(ans)
   except Exception as e:log.warning("poller: %s",e)
   STOP_EVENT.wait(2)

POLL=TelegramPoller()
app=Flask(__name__)

@app.get("/")
def home():
 with STORE.lock:a=len(STORE.active);dc=STORE.daily_count;b=STORE.balance
 return jsonify({"bot":BOT_VERSION,"status":"running","active":a,"today":dc,"balance":round(b,2)})
@app.get("/status")
def ws():return jsonify({"version":BOT_VERSION,"active":STORE.active,"daily_count":STORE.daily_count,"override_count":STORE.override_count,"balance":round(STORE.balance,2),"stats":Performance.stats()})
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
signal.signal(signal.SIGINT,stop_handler);signal.signal(signal.SIGTERM,stop_handler)

def startup():
 validate_config();baku_start=(Config.SCAN_HOUR_START+4)%24;baku_end=(Config.SCAN_HOUR_END+4)%24
 Telegram.send(f"🤖 SWING AI {BOT_VERSION}\n4H → 1H → 15M\n━━━━━━━━━━━━━━━━━━\n🥇 1-ci: Score ≥ {Config.FIRST_SIGNAL_SCORE}\n🥈 2-ci: Score ≥ {Config.SECOND_SIGNAL_SCORE}\n🥉 3-cü: Score ≥ {Config.THIRD_SIGNAL_SCORE}\nGündəlik tavan: {Config.MAX_DAILY_SIGNALS} siqnal\n━━━━━━━━━━━━━━━━━━\n⏰ UTC: {Config.SCAN_HOUR_START:02d}-{Config.SCAN_HOUR_END:02d} | Bakı: {baku_start:02d}-{baku_end:02d}\nHəftə sonu: ✅ | Manual: /scan\n━━━━━━━━━━━━━━━━━━\n💰 Balance: {STORE.balance:.2f} USDT\n📊 Monitor: 1M live wick")

def start_threads():
 threading.Thread(target=flask_worker,daemon=True).start()
 threading.Thread(target=ScannerWorker().run,daemon=True).start()
 threading.Thread(target=MANAGER.run,daemon=True).start()
 threading.Thread(target=POLL.run,daemon=True).start()

def main():
 startup();start_threads()
 while not STOP_EVENT.is_set():time.sleep(1)

if __name__=="__main__":main()
