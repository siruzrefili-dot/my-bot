import os,time,json,math,signal,logging,threading,requests,numpy as np,pandas as pd
from datetime import datetime,timezone
from concurrent.futures import ThreadPoolExecutor,as_completed
from flask import Flask,jsonify

BOT_VERSION="10.7 BALANCED BOS FINAL"
class Config:
 BOT_TOKEN=os.getenv("BOT_TOKEN","");CHAT_ID=os.getenv("CHAT_ID","1121794078")
 BASE_URL="https://api.bybit.com";CATEGORY="linear";SCAN_TOP_N=40;CANDIDATE_LIMIT=80
 CHECK_INTERVAL=300;MONITOR_INTERVAL=5;PARALLEL_WORKERS=6
 TREND_TF="240";SETUP_TF="60";ENTRY_TF="15";EMA_FAST=50;EMA_SLOW=200;RSI_PERIOD=14;ATR_PERIOD=14;VOLUME_LOOKBACK=20
 MIN_SCORE=68;MIN_RR=2.;MAX_RR=3.;ACCOUNT_BALANCE=1000.;RISK_PERCENT=1.
 MAX_ACTIVE_SIGNALS=3;MAX_DAILY_SIGNALS=5;MAX_SIGNALS_TO_SEND=3
 MIN_ATR_PCT=.30;MAX_ATR_PCT=8.;MIN_VOLUME_RATIO=1.10;MIN_EMA_DISTANCE_PCT=.10
 MIN_SL_ATR=.80;MAX_SL_ATR=2.50;SL_BUFFER_ATR=.15
 LONG_RSI_MIN=42.;LONG_RSI_MAX=64.;SHORT_RSI_MIN=36.;SHORT_RSI_MAX=58.
 BOS_BUFFER_ATR=.05;ALLOW_RECLAIM_BOS=True
 BOS_LOOKBACK=35;RETEST_MAX_BARS=4;RETEST_ATR_DISTANCE=.60;CONFIRMATION_MAX_BARS_AFTER_RETEST=2
 TARGET_LOOKBACK_15=80;TARGET_LOOKBACK_1H=80;TARGET_LOOKBACK_4H=80;TARGET_BUFFER_ATR=.10;MAX_CONFIRM_AGE=1;MAX_ENTRY_EXTENSION_ATR=.80
 MAX_ABS_FUNDING=.002;MIN_OI_CHANGE_PCT=-8.;OI_LOOKBACK=5;MAX_SPREAD_PCT=.15
 BTC_FILTER_ENABLED=True;CORRELATION_FILTER_ENABLED=True;MAX_CORRELATED_ACTIVE=2;CORRELATION_THRESHOLD=.85
 MAX_HOLD_HOURS=96;DATA_DIR="swing_bot_data";FLASK_PORT=int(os.getenv("PORT","10000"))
 REQUEST_TIMEOUT=15;CACHE_TTL=10;TICKER_CACHE_TTL=30;INSTRUMENT_CACHE_TTL=3600;MAX_CLOSED_SIGNALS_KEPT=200
 SIGNAL_FILE=os.path.join(DATA_DIR,"signals.json")
 FALLBACK_COINS=["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT","ADAUSDT","AVAXUSDT","DOGEUSDT","LINKUSDT","SUIUSDT","ARBUSDT","OPUSDT"]
 TRADFI={"XAU","XAG","CL","USOIL","UKOIL","SPX","NDX","DJI","US30","NAS100","US500","GER40","UK100","JP225","HK50","AAPL","AMZN","AMD","COIN","GOOG","GOOGL","META","MSFT","MSTR","MU","NFLX","NVDA","ORCL","PLTR","QCOM","TSLA","TSM","HOOD","SMCI","RKLB","OPENAI","NBIS","HPE","ANTHROPIC","CRWV","ADBE","NOKIA","LITE","ASTS","AEHR","ALAB","COHR","CIEN","POET","BNC","AXTI","FLNC","CRCL","BE","ONDS","CBRS","PURR","STXX","QNTX","SKHYNIX","SNDK","SOXL"}
os.makedirs(Config.DATA_DIR,exist_ok=True)
logging.basicConfig(level=logging.INFO,format="%(asctime)s | %(levelname)s | %(message)s");log=logging.getLogger("SWING_AI");STOP_EVENT=threading.Event()
def safe_float(v,d=0.):
 try:
  x=float(v);return d if math.isfinite(x) else d
 except:return d
def utc_now():return datetime.now(timezone.utc)
def utc_iso():return utc_now().isoformat()
def valid(df,n):return isinstance(df,pd.DataFrame) and len(df)>=n

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

class BybitClient:
 def __init__(self):
  self.base=Config.BASE_URL;self.cache=Cache(Config.CACHE_TTL);self.tcache=Cache(Config.TICKER_CACHE_TTL);self.icache=Cache(Config.INSTRUMENT_CACHE_TTL);self.local=threading.local()
 def session(self):
  if not hasattr(self.local,"session"):self.local.session=requests.Session()
  return self.local.session
 def get(self,path,params=None,key=None,cache=None):
  c=cache or self.cache
  if key:
   z=c.get(key)
   if z is not None:return z
  try:
   r=self.session().get(self.base+path,params=params or {},timeout=Config.REQUEST_TIMEOUT);r.raise_for_status();z=r.json()
   if z.get("retCode")!=0:raise RuntimeError(z.get("retMsg","API error"))
   if key:c.set(key,z)
   return z
  except Exception as e:log.warning("API %s: %s",path,e);return None
 def klines(self,symbol,interval,limit=300):
  z=self.get("/v5/market/kline",{"category":"linear","symbol":symbol,"interval":interval,"limit":limit},f"k:{symbol}:{interval}:{limit}")
  rows=z.get("result",{}).get("list",[]) if z else []
  if not rows:return pd.DataFrame()
  rows=list(reversed(rows));cols=["timestamp","open","high","low","close","volume","turnover"];df=pd.DataFrame(rows,columns=cols)
  for c in cols:df[c]=pd.to_numeric(df[c],errors="coerce")
  df=df.dropna(subset=["open","high","low","close","volume"])
  return df.iloc[:-1].reset_index(drop=True) if len(df)>1 else df.reset_index(drop=True)
 def ticker(self,symbol,fresh=False):
  c=None if fresh else self.tcache;k=None if fresh else f"t:{symbol}";z=self.get("/v5/market/tickers",{"category":"linear","symbol":symbol},k,c)
  a=z.get("result",{}).get("list",[]) if z else []
  if not a:return {}
  a=a[0];return {"symbol":a.get("symbol",""),"last_price":safe_float(a.get("lastPrice")),"change":safe_float(a.get("price24hPcnt"))*100,"funding":safe_float(a.get("fundingRate"),math.nan),"oi":safe_float(a.get("openInterest"),math.nan),"turnover24h":safe_float(a.get("turnover24h"))}
 def all_tickers(self):
  z=self.get("/v5/market/tickers",{"category":"linear"},"ALL_LINEAR_TICKERS",self.tcache);return z.get("result",{}).get("list",[]) if z else []
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
  z=self.get("/v5/market/orderbook",{"category":"linear","symbol":symbol,"limit":25},f"b:{symbol}");return z.get("result",{}) if z else {}
 def instrument(self,symbol):
  z=self.icache.get(symbol)
  if z is not None:return z
  x=self.get("/v5/market/instruments-info",{"category":"linear","symbol":symbol},f"i:{symbol}")
  a=x.get("result",{}).get("list",[]) if x else []
  if not a:return {}
  a=a[0];p=a.get("priceFilter",{});q=a.get("lotSizeFilter",{})
  z={"tick":safe_float(p.get("tickSize")),"step":safe_float(q.get("qtyStep")),"min":safe_float(q.get("minOrderQty")),"max":safe_float(q.get("maxOrderQty")),"status":a.get("status",""),"contractType":a.get("contractType",""),"quoteCoin":a.get("quoteCoin",""),"settleCoin":a.get("settleCoin",""),"baseCoin":a.get("baseCoin",""),"symbol":a.get("symbol","")}
  self.icache.set(symbol,z);return z
BYBIT=BybitClient()

def validate_config():
 if not 0<=Config.MIN_SCORE<=100 or Config.MIN_RR<=0 or Config.MAX_RR<Config.MIN_RR:raise ValueError("score/rr")
 if Config.RISK_PERCENT<=0 or not(Config.LONG_RSI_MIN<Config.LONG_RSI_MAX and Config.SHORT_RSI_MIN<Config.SHORT_RSI_MAX):raise ValueError("risk/rsi")
 if not(0<Config.MIN_SL_ATR<Config.MAX_SL_ATR):raise ValueError("sl atr")

class Indicators:
 @staticmethod
 def ema(s,n):return s.ewm(span=n,adjust=False).mean()
 @staticmethod
 def rsi(s,n=14):
  d=s.diff();g=d.clip(lower=0);l=-d.clip(upper=0);ag=g.ewm(alpha=1/n,adjust=False).mean();al=l.ewm(alpha=1/n,adjust=False).mean();rs=ag/al.replace(0,np.nan);r=100-100/(1+rs);return r.where(al!=0,100).where(~((ag==0)&(al==0)),50)
 @staticmethod
 def atr(df,n=14):
  pc=df.close.shift(1);tr=pd.concat([df.high-df.low,(df.high-pc).abs(),(df.low-pc).abs()],axis=1).max(axis=1);return tr.ewm(alpha=1/n,adjust=False).mean()
 @staticmethod
 def add(df):
  x=df.copy();x["ema_fast"]=Indicators.ema(x.close,Config.EMA_FAST);x["ema_slow"]=Indicators.ema(x.close,Config.EMA_SLOW);x["rsi"]=Indicators.rsi(x.close,Config.RSI_PERIOD);x["atr"]=Indicators.atr(x,Config.ATR_PERIOD);x["volume_ma"]=x.volume.rolling(Config.VOLUME_LOOKBACK).mean();x["volume_ratio"]=x.volume/x.volume_ma.replace(0,np.nan);x["ema_distance_pct"]=(x.ema_fast-x.ema_slow)/x.close*100;x["atr_pct"]=x.atr/x.close*100;return x

class Structure:
 @staticmethod
 def swings(df,l=5,r=5):
  h=df.high.to_numpy();lo=df.low.to_numpy();sh=[];sl=[]
  for i in range(l,len(df)-r):
   if h[i]>=max(h[i-l:i]) and h[i]>max(h[i+1:i+r+1]):sh.append((i,float(h[i])))
   if lo[i]<=min(lo[i-l:i]) and lo[i]<min(lo[i+1:i+r+1]):sl.append((i,float(lo[i])))
  return sh,sl
 @staticmethod
 def trend(df):
  sh,sl=Structure.swings(df)
  if len(sh)<2 or len(sl)<2:return "neutral"
  if sh[-1][1]>sh[-2][1] and sl[-1][1]>sl[-2][1]:return "bullish"
  if sh[-1][1]<sh[-2][1] and sl[-1][1]<sl[-2][1]:return "bearish"
  return "neutral"

class Regime:
 @staticmethod
 def analyze(df):
  if len(df)<220:return {"direction":"neutral","quality":0,"structure":"neutral"}
  x=df.iloc[-1];st=Structure.trend(df);dist=abs(safe_float(x.ema_distance_pct));atr=safe_float(x.atr_pct);q=min(dist,1)*35+min(atr/3,1)*20+(45 if st!="neutral" else 20)
  lo=x.close>x.ema_slow and x.ema_fast>x.ema_slow and dist>=Config.MIN_EMA_DISTANCE_PCT
  so=x.close<x.ema_slow and x.ema_fast<x.ema_slow and dist>=Config.MIN_EMA_DISTANCE_PCT
  aligned_long=lo and st=="bullish"
  aligned_short=so and st=="bearish"
  q=min(dist,1)*35+min(atr/3,1)*20+(45 if aligned_long or aligned_short else 20)
  if aligned_long:return {"direction":"long","quality":min(100,q),"structure":st}
  if aligned_short:return {"direction":"short","quality":min(100,q),"structure":st}
  return {"direction":"neutral","quality":min(100,q),"structure":st}

class Strategy:
 @staticmethod
 def setup(df,d):
  if len(df)<100:return False
  x=df.iloc[-1];return bool(x.close>x.ema_fast and x.ema_fast>x.ema_slow) if d=="long" else bool(x.close<x.ema_fast and x.ema_fast<x.ema_slow)
 @staticmethod
 def pullback(df,d):
  if len(df)<8:return False
  x=df.iloc[-7:-1];ema=df.ema_fast.iloc[-1];atr=df.atr.iloc[-1]
  return bool((x.low<=ema+atr*1.5).any()) if d=="long" else bool((x.high>=ema-atr*1.5).any())
 @staticmethod
 def rsi_allowed(df,d):
  r=safe_float(df.rsi.iloc[-1],math.nan);return math.isfinite(r) and (Config.LONG_RSI_MIN<=r<=Config.LONG_RSI_MAX if d=="long" else Config.SHORT_RSI_MIN<=r<=Config.SHORT_RSI_MAX)
 @staticmethod
 def rsi_score(df,d):
  b=safe_float(df.rsi.iloc[-1],math.nan)
  if not math.isfinite(b):return 0
  if d=="long":
   return 100 if 48<=b<=58 else 80 if Config.LONG_RSI_MIN<=b<48 or 58<b<=62 else 65
  return 100 if 42<=b<=52 else 80 if 38<=b<42 or 52<b<=55 else 65
 @staticmethod
 def bos(df,d):
  sh,sl=Structure.swings(df);n=len(df);sw=sh if d=="long" else sl;cut=max(0,n-Config.BOS_LOOKBACK-10);best=None
  for si,level in sw:
   if si<cut or si>=n-2:continue
   for i in range(si+1,n):
    atr=safe_float(df.atr.iloc[i],math.nan)
    if not math.isfinite(atr) or atr<=0:continue
    x=df.iloc[i];buf=atr*Config.BOS_BUFFER_ATR
    close_break=(x.close>=level+buf) if d=="long" else (x.close<=level-buf)
    reclaim=False
    crossed=(x.high>=level) if d=="long" else (x.low<=level)
    if Config.ALLOW_RECLAIM_BOS and crossed:
     reclaim=Strategy.strong(df,i,d) and ((x.close>=level) if d=="long" else (x.close<=level))
    if close_break or reclaim:
     typ="CLOSE_BOS" if close_break else "RECLAIM_BOS"
     cand={"level":level,"bar":i,"swing":si,"type":typ}
     if best is None or i>best["bar"]:best=cand
     break
  return best if best and n-1-best["bar"]<=Config.RETEST_MAX_BARS+Config.CONFIRMATION_MAX_BARS_AFTER_RETEST+1 else None
 @staticmethod
 def strong(df,i,d):
  if i<1 or i>=len(df):return False
  x=df.iloc[i];p=df.iloc[i-1];atr=safe_float(x.atr)
  if atr<=0 or abs(x.close-x.open)<atr*.5:return False
  return bool(x.close>x.open and x.close>p.close and x.high-x.close<=atr*.25) if d=="long" else bool(x.close<x.open and x.close<p.close and x.close-x.low<=atr*.25)
 @staticmethod
 def sequence(df,bos,d):
  if not bos:return None
  lv=bos["level"];start=bos["bar"]+1
  for i in range(start,min(len(df),start+Config.RETEST_MAX_BARS+1)):
   x=df.iloc[i];atr=safe_float(x.atr)
   if atr<=0:continue
   touch=x.low<=lv+atr*Config.RETEST_ATR_DISTANCE if d=="long" else x.high>=lv-atr*Config.RETEST_ATR_DISTANCE
   hold=x.close>=lv if d=="long" else x.close<=lv
   if not(touch and hold):continue
   for j in range(i+1,min(len(df),i+1+Config.CONFIRMATION_MAX_BARS_AFTER_RETEST+1)):
    if Strategy.strong(df,j,d) and (df.close.iloc[j]>lv if d=="long" else df.close.iloc[j]<lv):return {"bos":bos["bar"],"retest":i,"confirm":j,"level":lv}
  return None
 @staticmethod
 def zone_score(df,d):
  hi=df.high.iloc[-50:].max();lo=df.low.iloc[-50:].min();mid=(hi+lo)/2;p=df.close.iloc[-1];atr=df.atr.iloc[-1]
  if atr<=0:return 0
  return 100 if (p<=mid if d=="long" else p>=mid) else 70 if (p<=mid+atr if d=="long" else p>=mid-atr) else 35

class Filters:
 @staticmethod
 def volatility(df):return valid(df,30) and Config.MIN_ATR_PCT<=safe_float(df.atr_pct.iloc[-1],math.nan)<=Config.MAX_ATR_PCT
 @staticmethod
 def volume(df):return valid(df,Config.VOLUME_LOOKBACK+2) and safe_float(df.volume_ratio.iloc[-1],math.nan)>=Config.MIN_VOLUME_RATIO
 @staticmethod
 def funding(t):return bool(t) and math.isfinite(safe_float(t.get("funding"),math.nan)) and abs(t["funding"])<=Config.MAX_ABS_FUNDING
 @staticmethod
 def oi(df):
  if not valid(df,2) or "oi_value" not in df:return False
  a=safe_float(df.oi_value.iloc[-1],math.nan);b=safe_float(df.oi_value.iloc[-min(Config.OI_LOOKBACK,len(df))],math.nan)
  return math.isfinite(a) and math.isfinite(b) and b>0 and (a-b)/b*100>=Config.MIN_OI_CHANGE_PCT
 @staticmethod
 def spread(book):
  try:
   b=float(book["b"][0][0]);a=float(book["a"][0][0]);return b>0 and a>=b and (a-b)/b*100<=Config.MAX_SPREAD_PCT
  except:return False

class BTCFilter:
 @staticmethod
 def allowed(direction):
  if not Config.BTC_FILTER_ENABLED:return True
  df=BYBIT.klines("BTCUSDT","240",250)
  if not valid(df,220):return False
  r=Regime.analyze(Indicators.add(df));return r["direction"] in ("neutral",direction)

class Correlation:
 @staticmethod
 def allowed(symbol,active):
  if not Config.CORRELATION_FILTER_ENABLED:return True
  n=0
  for s in active:
   if s==symbol:continue
   try:
    a=BYBIT.klines(symbol,"60",80);b=BYBIT.klines(s,"60",80)
    if not valid(a,40) or not valid(b,40):continue
    c=a.close.pct_change().tail(40).corr(b.close.pct_change().tail(40))
    if math.isfinite(safe_float(c,math.nan)) and c>=Config.CORRELATION_THRESHOLD:n+=1
   except:continue
  return n<Config.MAX_CORRELATED_ACTIVE
class Risk:
 @staticmethod
 def structural_stop(df,e,d):
  sh,sl=Structure.swings(df);atr=safe_float(df.atr.iloc[-1]);buf=atr*Config.SL_BUFFER_ATR
  if d=="long":
   v=[x for i,x in sl if i>=max(0,len(df)-Config.TARGET_LOOKBACK_15) and x<e];return max(v)-buf if v else e-atr*Config.MIN_SL_ATR
  v=[x for i,x in sh if i>=max(0,len(df)-Config.TARGET_LOOKBACK_15) and x>e];return min(v)+buf if v else e+atr*Config.MIN_SL_ATR
 @staticmethod
 def target_candidates(df,d,e,lookback):
  sh,sl=Structure.swings(df);cut=max(0,len(df)-lookback);v=[x for i,x in (sh if d=="long" else sl) if i>=cut and (x>e if d=="long" else x<e)]
  return sorted(set(v),reverse=(d=="short"))
 @staticmethod
 def levels(d15,d1,d4,d):
  e=safe_float(d15.close.iloc[-1]);atr=safe_float(d15.atr.iloc[-1])
  if e<=0 or atr<=0:return None
  raw=Risk.structural_stop(d15,e,d);minrisk=atr*Config.MIN_SL_ATR;maxrisk=atr*Config.MAX_SL_ATR
  sl=min(raw,e-minrisk) if d=="long" else max(raw,e+minrisk);risk=abs(e-sl)
  if risk<=0 or risk>maxrisk:return None
  cs=Risk.target_candidates(d1,d,e,Config.TARGET_LOOKBACK_1H)+Risk.target_candidates(d15,d,e,Config.TARGET_LOOKBACK_15)+Risk.target_candidates(d4,d,e,Config.TARGET_LOOKBACK_4H)
  cs=sorted(set(cs),reverse=(d=="short"));lo=e+risk*Config.MIN_RR if d=="long" else e-risk*Config.MIN_RR;hi=e+risk*Config.MAX_RR if d=="long" else e-risk*Config.MAX_RR
  for level in cs:
   tp=level-atr*Config.TARGET_BUFFER_ATR if d=="long" else level+atr*Config.TARGET_BUFFER_ATR
   if (lo<=tp<=hi if d=="long" else hi<=tp<=lo):
    rr=abs(tp-e)/risk
    if Config.MIN_RR<=rr<=Config.MAX_RR:return {"entry":e,"sl":sl,"tp":tp,"risk":risk,"rr":rr}
  return None

class Scoring:
 @staticmethod
 def calculate(d4,d15,d,seq):
  r=Strategy.rsi_score(d15,d);v=min(100,safe_float(d15.volume_ratio.iloc[-1])*70);a=100 if safe_float(d15.atr_pct.iloc[-1])>=Config.MIN_ATR_PCT else 0
  q=Regime.analyze(d4)["quality"];z=Strategy.zone_score(d15,d);score=.25*q+.15*90+.25*(100 if seq else 0)+.15*r+.10*z+.05*v+.05*a
  return round(score,1),r

class Analyzer:
 def __init__(self,symbol):self.symbol=symbol;self.reject=""
 def run(self):
  s=self.symbol;d4=BYBIT.klines(s,"240",300);d1=BYBIT.klines(s,"60",300);d15=BYBIT.klines(s,"15",300)
  ok=valid(d4,220) and valid(d1,220) and valid(d15,80);STORE.add_stage("DATA",ok)
  if not ok:self.reject="DATA";return None
  d4=Indicators.add(d4);d1=Indicators.add(d1);d15=Indicators.add(d15);reg=Regime.analyze(d4);d=reg["direction"]
  checks=[(d!="neutral","4H_TREND"),(Strategy.setup(d1,d),"1H_SETUP"),(Strategy.pullback(d1,d),"1H_PULLBACK"),(Strategy.rsi_allowed(d15,d),"RSI"),(Filters.volatility(d15),"ATR")]
  for ok,r in checks:
   STORE.add_stage(r,ok)
   if not ok:self.reject=r;return None
  bos=Strategy.bos(d15,d)
  STORE.add_stage("BOS",bool(bos))
  STORE.add_bos_debug(bos.get("type") if bos else "NO_BREAK")
  if not bos:self.reject="BOS";return None
  seq=Strategy.sequence(d15,bos,d)
  STORE.add_stage("RETEST_CONFIRM",bool(seq))
  if not seq:self.reject="RETEST_CONFIRM";return None
  age=len(d15)-1-seq["confirm"];ok=age<=Config.MAX_CONFIRM_AGE;STORE.add_stage("CONFIRM_AGE",ok)
  if not ok:self.reject="CONFIRM_AGE";return None
  ok=safe_float(d15.atr.iloc[-1])>0 and abs(d15.close.iloc[-1]-d15.close.iloc[seq["confirm"]])<=safe_float(d15.atr.iloc[-1])*Config.MAX_ENTRY_EXTENSION_ATR;STORE.add_stage("ENTRY_CHASE",ok)
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
  ok=BTCFilter.allowed(d);STORE.add_stage("BTC_FILTER",ok)
  if not ok:self.reject="BTC_FILTER";return None
  lv=Risk.levels(d15,d1,d4,d);ok=bool(lv);STORE.add_stage("TARGET_RISK",ok)
  if not ok:self.reject="TARGET_RISK";return None
  score,rsi=Scoring.calculate(d4,d15,d,seq);ok=score>=Config.MIN_SCORE;STORE.add_stage("SCORE",ok)
  if not ok:self.reject="SCORE";return None
  return {"symbol":s,"direction":d,"score":score,"rsi":safe_float(d15.rsi.iloc[-1]),"rsi_score":rsi,"entry":lv["entry"],"sl":lv["sl"],"tp":lv["tp"],"rr":lv["rr"],"risk":lv["risk"],"atr_pct":safe_float(d15.atr_pct.iloc[-1]),"volume_ratio":safe_float(d15.volume_ratio.iloc[-1]),"funding":safe_float(t.get("funding"),0),"oi":safe_float(t.get("oi"),0),"created_at":utc_iso(),"created_ts":time.time()}
def analyze_symbol(s):
 try:
  a=Analyzer(s);x=a.run();return x,("" if x else a.reject)
 except Exception as e:log.warning("%s: %s",s,e);return None,"ERROR"

class Store:
 def __init__(self):
  self.lock=threading.RLock();self.active=[];self.closed=[];self.rejections={};self.stages={};self.day=utc_now().date().isoformat();self.daily_count=0
 def reset_day(self):
  d=utc_now().date().isoformat()
  with self.lock:
   if d!=self.day:self.day=d;self.daily_count=0
 def reset_rejections(self):
  with self.lock:self.rejections={};self.stages={}
 def add_stage(self,name,ok):
  with self.lock:
   x=self.stages.setdefault(name,{"pass":0,"fail":0});x["pass" if ok else "fail"]+=1
 def add_bos_debug(self,name):
  with self.lock:
   q=self.stages.setdefault("BOS_DEBUG",{});q[name]=q.get(name,0)+1
 def bos_report(self):
  with self.lock:q=dict(self.stages.get("BOS_DEBUG",{}))
  return " | ".join(f"{k}: {v}" for k,v in q.items()) or "Yoxdur"
 def stage_report(self,n):
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
 def active_symbols(self):
  with self.lock:return [x["symbol"] for x in self.active]
 def can_add(self):
  self.reset_day();return len(self.active)<Config.MAX_ACTIVE_SIGNALS and self.daily_count<Config.MAX_DAILY_SIGNALS
 def add(self,x):
  with self.lock:
   if not self.can_add() or any(a["symbol"]==x["symbol"] for a in self.active):return False
   self.active.append(x);self.daily_count+=1;self.save();return True
 def remove(self,s,x):
  with self.lock:
   self.active=[a for a in self.active if a["symbol"]!=s];y=dict(x);y["closed_at"]=utc_iso();self.closed=(self.closed+[y])[-Config.MAX_CLOSED_SIGNALS_KEPT:];self.save()
 def save(self):
  try:
   with open(Config.SIGNAL_FILE,"w",encoding="utf8") as f:json.dump({"active":self.active,"closed":self.closed,"day":self.day,"daily_count":self.daily_count},f,indent=2)
  except Exception as e:log.warning("save: %s",e)
 def load(self):
  try:
   with open(Config.SIGNAL_FILE,encoding="utf8") as f:x=json.load(f)
   self.active=x.get("active",[]);self.closed=x.get("closed",[]);self.day=x.get("day",self.day);self.daily_count=x.get("daily_count",0);self.reset_day()
  except:pass
STORE=Store();STORE.load()
class Performance:
 @staticmethod
 def stats():
  c=STORE.closed;w=sum(x.get("result")=="TP" for x in c);l=sum(x.get("result")=="SL" for x in c);t=w+l
  return {"closed":t,"wins":w,"losses":l,"winrate":round(w/t*100,2) if t else 0,"pnl":round(sum(safe_float(x.get("pnl")) for x in c),2),"today_signals":STORE.daily_count}

class Telegram:
 @staticmethod
 def send(text):
  if not Config.BOT_TOKEN:return False
  try:return requests.post(f"https://api.telegram.org/bot{Config.BOT_TOKEN}/sendMessage",json={"chat_id":Config.CHAT_ID,"text":text},timeout=10).ok
  except Exception as e:log.warning("telegram: %s",e);return False
 @staticmethod
 def signal(x):
  d="🟢 LONG" if x["direction"]=="long" else "🔴 SHORT";return Telegram.send(f"🚨 SWING AI {BOT_VERSION}\n\n{x['symbol']} {d}\nScore: {x['score']}/100\nRSI: {x['rsi']:.1f}\nEntry: {x['entry']:.8g}\nSL: {x['sl']:.8g}\nTP: {x['tp']:.8g}\nRR: 1:{x['rr']:.2f}\nATR: {x['atr_pct']:.2f}%\nVol: {x['volume_ratio']:.2f}x\n⚠️ Signal only.")
 @staticmethod
 def status():
  return "📭 Aktiv siqnal yoxdur." if not STORE.active else "📌 AKTİV SIQNALLAR\n\n"+"\n".join(f"{x['symbol']} {x['direction'].upper()}\nEntry: {x['entry']:.8g}\nSL: {x['sl']:.8g}\nTP: {x['tp']:.8g}\nRR: 1:{x['rr']:.2f}" for x in STORE.active)
 @staticmethod
 def scan_done(n,found,sent):
  q=STORE.rejection_stats();r=" | ".join(f"{k}: {v}" for k,v in sorted(q.items(),key=lambda z:-z[1])) if q else "Yoxdur";st=STORE.stage_report(n)
  return Telegram.send(f"✅ SCAN TAMAMLANDI\n\nAnaliz olunan: {n}\nUyğun setup: {len(found)}\nGöndərilən: {sent}\nAktiv: {len(STORE.active)}\n\n📋 ŞƏRTLƏR\n{st}\n\n🔎 BOS DETALLI\n{STORE.bos_report()}\n\n🚫 İLK UĞURSUZ ŞƏRT\n{r}")
 @staticmethod
 def handle(text):
  t=(text or "").strip().lower()
  if t in ("/status","/signals"):return Telegram.status()
  if t=="/stats":
   s=Performance.stats();return f"📊 STATS\nClosed: {s['closed']}\nWins: {s['wins']}\nLosses: {s['losses']}\nWinrate: {s['winrate']}%\nPnL: {s['pnl']:.2f} USDT\nToday signals: {s['today_signals']}"
  if t=="/rejections":
   q=STORE.rejection_stats();return "🚫 REJECTIONS\n"+("\n".join(f"{k}: {v}" for k,v in sorted(q.items(),key=lambda z:-z[1])) if q else "Yoxdur.")
  if t=="/help":return "/status\n/signals\n/stats\n/scan\n/rejections\n/help"
  return None

class PositionManager:
 def check(self,x):
  t=BYBIT.ticker(x["symbol"],fresh=True)
  if not t:return
  p=safe_float(t.get("last_price"),math.nan);e=x["entry"];sl=x["sl"];tp=x["tp"];d=x["direction"];age=(time.time()-x["created_ts"])/3600;result=None
  if not math.isfinite(p):return
  if age>=Config.MAX_HOLD_HOURS:result="TIME"
  elif d=="long" and p<=sl:result="SL"
  elif d=="long" and p>=tp:result="TP"
  elif d=="short" and p>=sl:result="SL"
  elif d=="short" and p<=tp:result="TP"
  if result:
   rr=x["rr"] if result=="TP" else -1
   if result=="TIME":rr=(p-e)/(e-sl) if d=="long" else (e-p)/(sl-e)
   x["result"]=result;x["result_r"]=rr;x["exit_price"]=p;x["pnl"]=rr*(Config.ACCOUNT_BALANCE*Config.RISK_PERCENT/100);STORE.remove(x["symbol"],x);Telegram.send(f"🔔 CLOSED\n{x['symbol']} {d.upper()}\nResult: {result}\nExit: {p:.8g}\nR: {rr:.2f}\nPnL: {x['pnl']:.2f} USDT")
 def run(self):
  while not STOP_EVENT.is_set():
   try:
    for x in list(STORE.active):self.check(x)
   except Exception as e:log.warning("monitor: %s",e)
   STOP_EVENT.wait(Config.MONITOR_INTERVAL)
MANAGER=PositionManager()

class Scanner:
 def __init__(self):self.lock=threading.Lock();self.running=False
 def symbols(self):
  try:
   c=[]
   for x in BYBIT.all_tickers():
    s=x.get("symbol","");turn=safe_float(x.get("turnover24h"));base=s[:-4] if s.endswith("USDT") else ""
    if s.endswith("USDT") and turn>0 and base not in Config.TRADFI:c.append((s,turn))
   c.sort(key=lambda z:z[1],reverse=True);out=[]
   for s,_ in c[:Config.CANDIDATE_LIMIT]:
    i=BYBIT.instrument(s)
    if i and i.get("status")=="Trading" and i.get("contractType")=="LinearPerpetual" and i.get("quoteCoin")=="USDT" and i.get("settleCoin")=="USDT":out.append(s)
    if len(out)>=Config.SCAN_TOP_N:break
   return out or Config.FALLBACK_COINS
  except Exception as e:log.warning("symbols: %s",e);return Config.FALLBACK_COINS
 def scan(self):
  if not self.lock.acquire(False):return
  self.running=True;found=[];sent=0;symbols=[]
  try:
   STORE.reset_day();STORE.reset_rejections()
   if not STORE.can_add():
    Telegram.send(f"⏸ SCAN DAY LIMIT\nActive: {len(STORE.active)}\nToday: {STORE.daily_count}/{Config.MAX_DAILY_SIGNALS}");return
   symbols=self.symbols()
   with ThreadPoolExecutor(max_workers=Config.PARALLEL_WORKERS) as ex:
    fs={ex.submit(analyze_symbol,s):s for s in symbols}
    for f in as_completed(fs):
     s=fs[f]
     try:x,r=f.result()
     except Exception:x,r=None,"ERROR"
     if x:found.append(x)
     else:STORE.add_rejection(s,r)
   found.sort(key=lambda x:x["score"],reverse=True)
   for x in found:
    if sent>=Config.MAX_SIGNALS_TO_SEND or not STORE.can_add():break
    if not Correlation.allowed(x["symbol"],STORE.active_symbols()):STORE.add_rejection(x["symbol"],"CORRELATION");continue
    if STORE.add(x):Telegram.signal(x);sent+=1
   Telegram.scan_done(len(symbols),found,sent);log.info("SCAN | %d | valid=%d | sent=%d",len(symbols),len(found),sent)
  finally:
   self.running=False;self.lock.release()
SCANNER=Scanner()
class ScannerWorker:
 def run(self):
  while not STOP_EVENT.is_set():
   try:SCANNER.scan()
   except Exception as e:log.exception("scanner: %s",e)
   STOP_EVENT.wait(Config.CHECK_INTERVAL)
class TelegramPoller:
 def __init__(self):self.offset=0
 def run(self):
  if not Config.BOT_TOKEN:return
  url=f"https://api.telegram.org/bot{Config.BOT_TOKEN}/getUpdates"
  while not STOP_EVENT.is_set():
   try:
    data=requests.get(url,params={"timeout":20,"offset":self.offset},timeout=25).json()
    for u in data.get("result",[]):
     self.offset=u["update_id"]+1;m=u.get("message",{})
     if str(m.get("chat",{}).get("id",""))!=str(Config.CHAT_ID):continue
     text=m.get("text","")
     if text.strip().lower()=="/scan":
      if SCANNER.running:Telegram.send("⏳ Scan artıq işləyir.")
      else:Telegram.send("🔎 Scan başladıldı...");threading.Thread(target=SCANNER.scan,daemon=True).start()
     else:
      ans=Telegram.handle(text)
      if ans:Telegram.send(ans)
   except Exception as e:log.warning("poller: %s",e)
   STOP_EVENT.wait(2)
POLL=TelegramPoller();app=Flask(__name__)
@app.get("/")
def home():return jsonify({"bot":BOT_VERSION,"status":"running","active":len(STORE.active),"today":STORE.daily_count})
@app.get("/status")
def web_status():return jsonify({"version":BOT_VERSION,"active":STORE.active,"daily_count":STORE.daily_count,"stats":Performance.stats()})
@app.get("/signals")
def web_signals():return jsonify({"active":STORE.active})
@app.get("/stats")
def web_stats():return jsonify(Performance.stats())
@app.get("/rejections")
def web_rejections():return jsonify(STORE.rejection_stats())
def flask_worker():
 try:app.run(host="0.0.0.0",port=Config.FLASK_PORT,debug=False,use_reloader=False)
 except Exception as e:log.warning("flask: %s",e)
def stop_handler(*_):STOP_EVENT.set()
signal.signal(signal.SIGINT,stop_handler);signal.signal(signal.SIGTERM,stop_handler)
def startup():
 validate_config();Telegram.send(f"🤖 SWING AI {BOT_VERSION}\n4H → 1H → 15M\nRSI L {Config.LONG_RSI_MIN:.0f}-{Config.LONG_RSI_MAX:.0f} | S {Config.SHORT_RSI_MIN:.0f}-{Config.SHORT_RSI_MAX:.0f}\nATR ≥ {Config.MIN_ATR_PCT:.2f}% | SCORE ≥ {Config.MIN_SCORE}\nRR {Config.MIN_RR:.1f}-{Config.MAX_RR:.1f} | DAILY {Config.MAX_DAILY_SIGNALS}")
def start_threads():
 threading.Thread(target=flask_worker,daemon=True).start();threading.Thread(target=ScannerWorker().run,daemon=True).start();threading.Thread(target=MANAGER.run,daemon=True).start();threading.Thread(target=POLL.run,daemon=True).start()
def main():
 startup();start_threads()
 while not STOP_EVENT.is_set():time.sleep(1)
if __name__=="__main__":main()
