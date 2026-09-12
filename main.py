import os,time,json,math,threading,logging,requests,numpy as np,pandas as pd
from datetime import datetime,timezone
from concurrent.futures import ThreadPoolExecutor,as_completed
from flask import Flask,jsonify
from telegram import Bot
BOT_VERSION="10.8 BALANCED TREND FINAL"

class Config:
 BOT_TOKEN=os.getenv("BOT_TOKEN","");CHAT_ID=os.getenv("CHAT_ID","1121794078")
 BASE_URL="https://api.bybit.com";CATEGORY="linear"
 SCAN_TOP_N=40;CANDIDATE_LIMIT=80;CHECK_INTERVAL=300;MONITOR_INTERVAL=5;PARALLEL_WORKERS=6
 TREND_TF="240";SETUP_TF="60";ENTRY_TF="15"
 EMA_FAST=50;EMA_SLOW=200;RSI_PERIOD=14;ATR_PERIOD=14;VOLUME_LOOKBACK=20
 MIN_SCORE=68;MIN_RR=2.;MAX_RR=3.;ACCOUNT_BALANCE=1000.;RISK_PERCENT=1.
 MAX_ACTIVE_SIGNALS=3;MAX_DAILY_SIGNALS=5;MAX_SIGNALS_TO_SEND=3
 MIN_ATR_PCT=.30;MAX_ATR_PCT=8.;MIN_VOLUME_RATIO=1.10;MIN_EMA_DISTANCE_PCT=.10
 MIN_SL_ATR=.80;MAX_SL_ATR=2.50;SL_BUFFER_ATR=.15
 LONG_RSI_MIN=42.;LONG_RSI_MAX=64.;SHORT_RSI_MIN=36.;SHORT_RSI_MAX=58.
 BOS_LOOKBACK=35;BOS_BUFFER_ATR=.05;ALLOW_RECLAIM_BOS=True
 RETEST_MAX_BARS=4;RETEST_ATR_DISTANCE=.60;CONFIRMATION_MAX_BARS_AFTER_RETEST=2
 TARGET_LOOKBACK_15=80;TARGET_LOOKBACK_1H=80;TARGET_LOOKBACK_4H=80;TARGET_BUFFER_ATR=.10
 MAX_CONFIRM_AGE=1;MAX_ENTRY_EXTENSION_ATR=.80
 MAX_ABS_FUNDING=.002;MIN_OI_CHANGE_PCT=-8.;OI_LOOKBACK=5;MAX_SPREAD_PCT=.15
 BTC_FILTER_ENABLED=True;CORRELATION_FILTER_ENABLED=True
 MAX_CORRELATED_ACTIVE=2;CORRELATION_THRESHOLD=.85
 MAX_HOLD_HOURS=96;DATA_DIR="swing_bot_data";FLASK_PORT=int(os.getenv("PORT","10000"))
 REQUEST_TIMEOUT=15;CACHE_TTL=10;TICKER_CACHE_TTL=30;INSTRUMENT_CACHE_TTL=3600
 MAX_CLOSED_SIGNALS_KEPT=200
 SIGNAL_FILE=os.path.join(DATA_DIR,"signals.json")

os.makedirs(Config.DATA_DIR,exist_ok=True)
logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("SWING")

def now():
 return datetime.now(timezone.utc)
def safe(v,d=0.):
 try:
  x=float(v)
  return x if math.isfinite(x) else d
 except:return d
def pct(a,b):
 return abs(a-b)/b*100 if b else 0
def valid(df,n):
 return isinstance(df,pd.DataFrame) and len(df)>=n and all(c in df for c in ["open","high","low","close","volume"])
def savej(path,obj):
 try:
  with open(path,"w") as f:json.dump(obj,f,indent=2,default=str)
 except Exception as e:log.error("save %s",e)
def loadj(path,default):
 try:
  with open(path) as f:return json.load(f)
 except:return default

class Bybit:
 def __init__(self):
  self.s=requests.Session();self.cache={};self.lock=threading.RLock()
 def get(self,path,params={},key=""):
  k=key or path+json.dumps(params,sort_keys=True)
  t=time.time()
  with self.lock:
   z=self.cache.get(k)
   if z and t-z[0]<Config.CACHE_TTL:return z[1]
  try:
   r=self.s.get(Config.BASE_URL+path,params=params,timeout=Config.REQUEST_TIMEOUT)
   if r.status_code!=200:return None
   j=r.json()
   if j.get("retCode")!=0:return None
   with self.lock:self.cache[k]=(t,j)
   return j
  except Exception:return None
 def klines(self,symbol,tf,limit=300):
  j=self.get("/v5/market/kline",{"category":Config.CATEGORY,"symbol":symbol,"interval":tf,"limit":limit},f"k:{symbol}:{tf}:{limit}")
  rows=j.get("result",{}).get("list",[]) if j else []
  if not rows:return pd.DataFrame()
  rows=sorted(rows,key=lambda x:int(x[0]))
  d=pd.DataFrame(rows,columns=["time","open","high","low","close","volume","turnover"])
  for c in ["time","open","high","low","close","volume","turnover"]:d[c]=pd.to_numeric(d[c],errors="coerce")
  return d
 def ticker(self,symbol,fresh=False):
  key=f"tick:{symbol}"
  if fresh:
   with self.lock:self.cache.pop(key,None)
  j=self.get("/v5/market/tickers",{"category":Config.CATEGORY,"symbol":symbol},key)
  try:return j["result"]["list"][0]
  except:return {}
 def instruments(self):
  j=self.get("/v5/market/instruments-info",{"category":Config.CATEGORY,"limit":1000},"inst")
  return j.get("result",{}).get("list",[]) if j else []
 def funding(self,symbol):
  j=self.get("/v5/market/funding/history",{"category":Config.CATEGORY,"symbol":symbol,"limit":1},f"fund:{symbol}")
  try:return safe(j["result"]["list"][0]["fundingRate"])
  except:return math.nan
 def oi(self,symbol,limit=10):
  j=self.get("/v5/market/open-interest",{"category":"linear","symbol":symbol,"intervalTime":"1h","limit":limit},f"oi:{symbol}:{limit}")
  rows=j.get("result",{}).get("list",[]) if j else []
  if not rows:return pd.DataFrame()
  d=pd.DataFrame(rows)
  for c in ["openInterest","singleOpenInterest","timestamp"]:
   if c in d:d[c]=pd.to_numeric(d[c],errors="coerce")
  col="singleOpenInterest" if "singleOpenInterest" in d and d["singleOpenInterest"].notna().sum()>=2 else "openInterest"
  if col not in d:return pd.DataFrame()
  return d.rename(columns={col:"oi_value"}).sort_values("timestamp").reset_index(drop=True)
 def spread(self,symbol):
  x=self.ticker(symbol)
  bid=safe(x.get("bid1Price"));ask=safe(x.get("ask1Price"))
  return pct(ask,bid) if bid and ask else math.nan

BYBIT=Bybit()
def instrument_symbols():
 deny=("USDT","USDC")
 bad=("1000","1000000","100000","10000","10000000")
 tradfi=("AAPL","TSLA","NVDA","AMZN","GOOG","GOOGL","META","MSFT","MSTR","COIN","NFLX","AMD","INTC","PLTR","HOOD","ORCL","AVGO","QCOM","IBM","BA","DIS","NKE","PFE","XOM","CVX","GLD","SLV","SILVER","GOLD","OIL","WTI","BRN","COPPER")
 out=[]
 for x in BYBIT.instruments():
  s=x.get("symbol","")
  if x.get("status")!="Trading":continue
  if x.get("quoteCoin")!="USDT" or x.get("settleCoin")!="USDT":continue
  if x.get("contractType")!="LinearPerpetual":continue
  if any(s.startswith(z) for z in bad):continue
  if any(z in s for z in tradfi):continue
  out.append(s)
 return out

def top_symbols():
 syms=instrument_symbols()
 vals=[]
 for s in syms[:Config.CANDIDATE_LIMIT]:
  x=BYBIT.ticker(s)
  v=safe(x.get("turnover24h"))
  if v>0:vals.append((s,v))
 vals.sort(key=lambda z:z[1],reverse=True)
 return [x[0] for x in vals[:Config.SCAN_TOP_N]]

class Indicators:
 @staticmethod
 def add(df):
  d=df.copy()
  d["ema_fast"]=d.close.ewm(span=Config.EMA_FAST,adjust=False).mean()
  d["ema_slow"]=d.close.ewm(span=Config.EMA_SLOW,adjust=False).mean()
  delta=d.close.diff()
  gain=delta.clip(lower=0).rolling(Config.RSI_PERIOD).mean()
  loss=(-delta.clip(upper=0)).rolling(Config.RSI_PERIOD).mean()
  rs=gain/loss.replace(0,np.nan)
  d["rsi"]=(100-(100/(1+rs))).fillna(50)
  tr=pd.concat([d.high-d.low,(d.high-d.close.shift()).abs(),(d.low-d.close.shift()).abs()],axis=1).max(axis=1)
  d["atr"]=tr.rolling(Config.ATR_PERIOD).mean()
  d["atr_pct"]=d.atr/d.close*100
  d["volume_ma"]=d.volume.rolling(Config.VOLUME_LOOKBACK).mean()
  d["volume_ratio"]=d.volume/d.volume_ma.replace(0,np.nan)
  d["ema_distance_pct"]=(d.ema_fast-d.ema_slow)/d.close*100
  return d.dropna().reset_index(drop=True)

class Structure:
 @staticmethod
 def swings(df,left=2,right=2):
  highs=[];lows=[]
  for i in range(left,len(df)-right):
   h=df.high.iloc[i];l=df.low.iloc[i]
   if h>=df.high.iloc[i-left:i+right+1].max():highs.append((i,h))
   if l<=df.low.iloc[i-left:i+right+1].min():lows.append((i,l))
  return highs,lows
 @staticmethod
 def trend(df):
  sh,sl=Structure.swings(df)
  if len(sh)<2 or len(sl)<2:return "neutral"
  hh=sh[-1][1]>sh[-2][1];hl=sl[-1][1]>sl[-2][1]
  lh=sh[-1][1]<sh[-2][1];ll=sl[-1][1]<sl[-2][1]
  if hh and hl:return "bullish"
  if lh and ll:return "bearish"
  return "neutral"

class Regime:
 @staticmethod
 def analyze(df):
  if len(df)<220:return {"direction":"neutral","quality":0,"structure":"neutral"}
  x=df.iloc[-1];st=Structure.trend(df)
  dist=abs(safe(x.ema_distance_pct));atr=safe(x.atr_pct)
  ema_long=x.close>x.ema_fast and x.ema_fast>x.ema_slow and dist>=Config.MIN_EMA_DISTANCE_PCT
  ema_short=x.close<x.ema_fast and x.ema_fast<x.ema_slow and dist>=Config.MIN_EMA_DISTANCE_PCT
  long_ok=ema_long and st!="bearish"
  short_ok=ema_short and st!="bullish"
  bonus=45 if st in ("bullish","bearish") else 25
  q=min(100,min(dist,1)*35+min(atr/3,1)*20+bonus)
  if long_ok:return {"direction":"long","quality":q,"structure":st}
  if short_ok:return {"direction":"short","quality":q,"structure":st}
  return {"direction":"neutral","quality":q,"structure":st}
class Strategy:
 @staticmethod
 def setup(df,d):
  if len(df)<100:return False
  x=df.iloc[-1]
  return bool(x.close>x.ema_fast>x.ema_slow) if d=="long" else bool(x.close<x.ema_fast<x.ema_slow)
 @staticmethod
 def pullback(df,d):
  if len(df)<8:return False
  x=df.iloc[-7:-1];ema=safe(df.ema_fast.iloc[-1]);atr=safe(df.atr.iloc[-1])
  if atr<=0:return False
  return bool((x.low<=ema+atr*1.5).any()) if d=="long" else bool((x.high>=ema-atr*1.5).any())
 @staticmethod
 def rsi_allowed(df,d):
  r=safe(df.rsi.iloc[-1])
  return Config.LONG_RSI_MIN<=r<=Config.LONG_RSI_MAX if d=="long" else Config.SHORT_RSI_MIN<=r<=Config.SHORT_RSI_MAX
 @staticmethod
 def strong(df,i,d):
  if i<1:return False
  x=df.iloc[i];p=df.iloc[i-1];atr=safe(x.atr)
  if atr<=0:return False
  body=abs(x.close-x.open)
  if d=="long":return x.close>x.open and body>=atr*.35 and x.close>=p.close
  return x.close<x.open and body>=atr*.35 and x.close<=p.close
 @staticmethod
 def bos(df,d):
  sh,sl=Structure.swings(df);n=len(df);sw=sh if d=="long" else sl
  cut=max(0,n-Config.BOS_LOOKBACK-10);best=None
  for si,level in sw:
   if si<cut or si>=n-2:continue
   for i in range(si+1,n):
    atr=safe(df.atr.iloc[i],math.nan)
    if not math.isfinite(atr) or atr<=0:continue
    x=df.iloc[i];buf=atr*Config.BOS_BUFFER_ATR
    close_break=(x.close>=level+buf) if d=="long" else (x.close<=level-buf)
    reclaim=False
    crossed=(x.high>=level) if d=="long" else (x.low<=level)
    if Config.ALLOW_RECLAIM_BOS and crossed:
     reclaim=Strategy.strong(df,i,d) and ((x.close>=level) if d=="long" else (x.close<=level))
    if close_break or reclaim:
     cand={"level":level,"bar":i,"swing":si,"type":"CLOSE_BOS" if close_break else "RECLAIM_BOS"}
     if best is None or i>best["bar"]:best=cand
     break
  return best if best and n-1-best["bar"]<=Config.RETEST_MAX_BARS+Config.CONFIRMATION_MAX_BARS_AFTER_RETEST+1 else None
 @staticmethod
 def retest_confirm(df,bos,d):
  if not bos:return None
  n=len(df);atr=safe(df.atr.iloc[-1])
  start=bos["bar"]+1;end=min(n-1,start+Config.RETEST_MAX_BARS)
  ret=None
  for i in range(start,end+1):
   x=df.iloc[i];lv=bos["level"]
   touched=(x.low<=lv+atr*Config.RETEST_ATR_DISTANCE) if d=="long" else (x.high>=lv-atr*Config.RETEST_ATR_DISTANCE)
   if touched:
    ret=i;break
  if ret is None:return None
  cend=min(n-1,ret+Config.CONFIRMATION_MAX_BARS_AFTER_RETEST)
  for i in range(ret,cend+1):
   if Strategy.strong(df,i,d):
    return {"retest_bar":ret,"confirm_bar":i}
  return None
 @staticmethod
 def entry_ok(df,d):
  atr=safe(df.atr.iloc[-1]);close=safe(df.close.iloc[-1])
  if atr<=0:return False
  if d=="long":return close<=safe(df.ema_fast.iloc[-1])+atr*Config.MAX_ENTRY_EXTENSION_ATR
  return close>=safe(df.ema_fast.iloc[-1])-atr*Config.MAX_ENTRY_EXTENSION_ATR

class Filters:
 @staticmethod
 def volatility(df):
  a=safe(df.atr_pct.iloc[-1],math.nan)
  return math.isfinite(a) and Config.MIN_ATR_PCT<=a<=Config.MAX_ATR_PCT
 @staticmethod
 def volume(df):
  return safe(df.volume_ratio.iloc[-1],0)>=Config.MIN_VOLUME_RATIO
 @staticmethod
 def funding(symbol):
  x=BYBIT.funding(symbol)
  return math.isfinite(x) and abs(x)<=Config.MAX_ABS_FUNDING
 @staticmethod
 def oi(symbol):
  d=BYBIT.oi(symbol,Config.OI_LOOKBACK+2)
  if len(d)<2:return False
  a=safe(d.oi_value.iloc[0],math.nan);b=safe(d.oi_value.iloc[-1],math.nan)
  if not math.isfinite(a) or not math.isfinite(b) or a<=0:return False
  return (b-a)/a*100>=Config.MIN_OI_CHANGE_PCT
class Filters:
 @staticmethod
 def spread(symbol):
  x=BYBIT.spread(symbol)
  return math.isfinite(x) and x<=Config.MAX_SPREAD_PCT
 @staticmethod
 def ticker(symbol):
  x=BYBIT.ticker(symbol)
  p=safe(x.get("lastPrice"))
  return p>0 and safe(x.get("bid1Price"))>0 and safe(x.get("ask1Price"))>0
 @staticmethod
 def btc(symbol,d):
  if not Config.BTC_FILTER_ENABLED or symbol=="BTCUSDT":return True
  df=BYBIT.klines("BTCUSDT","60",220)
  if len(df)<200:return False
  df=Indicators.add(df)
  x=df.iloc[-1]
  if d=="long":return bool(x.close>x.ema_fast and x.ema_fast>x.ema_slow)
  return bool(x.close<x.ema_fast and x.ema_fast<x.ema_slow)

class Correlation:
 @staticmethod
 def allowed(symbol,active):
  if not Config.CORRELATION_FILTER_ENABLED:return True
  if not active:return True
  try:
   a=BYBIT.klines(symbol,"60",100)
   if len(a)<50:return False
   ar=Indicators.add(a).close.pct_change().dropna()
   for s in active:
    b=BYBIT.klines(s,"60",100)
    if len(b)<50:continue
    br=Indicators.add(b).close.pct_change().dropna()
    n=min(len(ar),len(br))
    c=ar.iloc[-n:].corr(br.iloc[-n:])
    if math.isfinite(c) and abs(c)>=Config.CORRELATION_THRESHOLD:return False
   return True
  except:return False

class Risk:
 @staticmethod
 def structural_stop(df,e,d):
  sh,sl=Structure.swings(df);atr=safe(df.atr.iloc[-1]);buf=atr*Config.SL_BUFFER_ATR
  cut=max(0,len(df)-Config.TARGET_LOOKBACK_15)
  if d=="long":
   v=[x for i,x in sl if i>=cut and x<e]
   return max(v)-buf if v else e-atr*Config.MIN_SL_ATR
  v=[x for i,x in sh if i>=cut and x>e]
  return min(v)+buf if v else e+atr*Config.MIN_SL_ATR
 @staticmethod
 def targets(df,d,e,lookback):
  sh,sl=Structure.swings(df);cut=max(0,len(df)-lookback)
  v=[x for i,x in (sh if d=="long" else sl) if i>=cut and (x>e if d=="long" else x<e)]
  return sorted(set(v),reverse=d=="short")
 @staticmethod
 def levels(d15,d1,d4,d):
  e=safe(d15.close.iloc[-1]);atr=safe(d15.atr.iloc[-1])
  if e<=0 or atr<=0:return None
  raw=Risk.structural_stop(d15,e,d)
  mn=atr*Config.MIN_SL_ATR;mx=atr*Config.MAX_SL_ATR
  if d=="long":sl=min(raw,e-mn)
  else:sl=max(raw,e+mn)
  risk=abs(e-sl)
  if risk<=0 or risk>mx:return None
  cs=[]
  for df,lb in [(d1,Config.TARGET_LOOKBACK_1H),(d15,Config.TARGET_LOOKBACK_15),(d4,Config.TARGET_LOOKBACK_4H)]:
   cs+=Risk.targets(df,d,e,lb)
  cs=sorted(set(cs),reverse=d=="short")
  lo=e+risk*Config.MIN_RR if d=="long" else e-risk*Config.MIN_RR
  hi=e+risk*Config.MAX_RR if d=="long" else e-risk*Config.MAX_RR
  for lv in cs:
   tp=lv-atr*Config.TARGET_BUFFER_ATR if d=="long" else lv+atr*Config.TARGET_BUFFER_ATR
   ok=lo<=tp<=hi if d=="long" else hi<=tp<=lo
   if ok:
    rr=abs(tp-e)/risk
    if Config.MIN_RR<=rr<=Config.MAX_RR:return {"entry":e,"sl":sl,"tp":tp,"risk":risk,"rr":rr}
  return None

class Score:
 @staticmethod
 def calc(d4,d1,d15,reg,bos,rc):
  d=reg["direction"];x=d15.iloc[-1];s=0
  s+=20 if reg["quality"]>=60 else 12
  s+=15
  s+=10
  s+=10 if Strategy.pullback(d1,d) else 0
  s+=10 if Strategy.rsi_allowed(d15,d) else 0
  s+=10 if Filters.volume(d15) else 0
  s+=10 if Filters.volatility(d15) else 0
  s+=5 if bos and bos["type"]=="CLOSE_BOS" else 3
  s+=10 if rc else 0
  return min(100,s)
class Store:
 def __init__(self):
  self.lock=threading.RLock();self.active=[];self.closed=[]
  self.rejections={};self.stages={};self.bos_debug={}
  self.day=now().date().isoformat();self.daily_count=0
  self.closed=loadj(os.path.join(Config.DATA_DIR,"closed.json"),[])
 def reset_day(self):
  d=now().date().isoformat()
  with self.lock:
   if d!=self.day:self.day=d;self.daily_count=0
 def reset_rejections(self):
  with self.lock:self.rejections={};self.stages={};self.bos_debug={}
 def add_stage(self,name,ok):
  with self.lock:
   x=self.stages.setdefault(name,{"pass":0,"fail":0});x["pass" if ok else "fail"]+=1
 def add_rejection(self,s,r):
  with self.lock:self.rejections[s]=r or "UNKNOWN"
 def rejection_stats(self):
  with self.lock:
   d={}
   for r in self.rejections.values():d[r]=d.get(r,0)+1
   return d
 def add_bos_debug(self,name):
  with self.lock:self.bos_debug[name]=self.bos_debug.get(name,0)+1
 def bos_report(self):
  with self.lock:
   if not self.bos_debug:return "Yoxdur"
   return " | ".join(f"{k}: {v}" for k,v in sorted(self.bos_debug.items(),key=lambda z:-z[1]))
 def stage_report(self,n=0):
  order=["DATA","4H_TREND","1H_SETUP","1H_PULLBACK","RSI","ATR","BOS","RETEST_CONFIRM","CONFIRM_AGE","ENTRY_CHASE","VOLUME","TICKER","FUNDING","OI","SPREAD","BTC_FILTER","TARGET_RISK","SCORE"]
  with self.lock:q=dict(self.stages)
  out=[]
  for x in order:
   z=q.get(x,{"pass":0,"fail":0});p=z["pass"];f=z["fail"]
   out.append(f"{x}: {p}/{p+f} keçdi | {f} keçmədi")
  return "\n".join(out)
 def can_add(self):
  with self.lock:return len(self.active)<Config.MAX_ACTIVE_SIGNALS and self.daily_count<Config.MAX_DAILY_SIGNALS
 def active_symbols(self):
  with self.lock:return [x["symbol"] for x in self.active]
 def add(self,x):
  with self.lock:
   if not self.can_add():return False
   if any(a["symbol"]==x["symbol"] for a in self.active):return False
   x["status"]="ACTIVE";x["created"]=now().isoformat()
   self.active.append(x);self.daily_count+=1;self.persist();return True
 def persist(self):
  savej(Config.SIGNAL_FILE,self.active)
  savej(os.path.join(Config.DATA_DIR,"closed.json"),self.closed)
STORE=Store()

def analyze_symbol(symbol):
 try:
  d4=BYBIT.klines(symbol,"240",300);d1=BYBIT.klines(symbol,"60",300);d15=BYBIT.klines(symbol,"15",300)
  ok=valid(d4,220) and valid(d1,220) and valid(d15,80);STORE.add_stage("DATA",ok)
  if not ok:return None,"DATA"
  d4=Indicators.add(d4);d1=Indicators.add(d1);d15=Indicators.add(d15)
  reg=Regime.analyze(d4);d=reg["direction"]
  ok=d!="neutral";STORE.add_stage("4H_TREND",ok)
  if not ok:return None,"4H_TREND"
  ok=Strategy.setup(d1,d);STORE.add_stage("1H_SETUP",ok)
  if not ok:return None,"1H_SETUP"
  ok=Strategy.pullback(d1,d);STORE.add_stage("1H_PULLBACK",ok)
  if not ok:return None,"1H_PULLBACK"
  ok=Strategy.rsi_allowed(d15,d);STORE.add_stage("RSI",ok)
  if not ok:return None,"RSI"
  ok=Filters.volatility(d15);STORE.add_stage("ATR",ok)
  if not ok:return None,"ATR"
  bos=Strategy.bos(d15,d);STORE.add_bos_debug(bos["type"] if bos else "NO_BREAK");STORE.add_stage("BOS",bool(bos))
  if not bos:return None,"BOS"
  rc=Strategy.retest_confirm(d15,bos,d);STORE.add_stage("RETEST_CONFIRM",bool(rc))
  if not rc:return None,"RETEST_CONFIRM"
  age=len(d15)-1-rc["confirm_bar"];ok=age<=Config.MAX_CONFIRM_AGE
  STORE.add_stage("CONFIRM_AGE",ok)
  if not ok:return None,"CONFIRM_AGE"
  ok=Strategy.entry_ok(d15,d);STORE.add_stage("ENTRY_CHASE",ok)
  if not ok:return None,"ENTRY_CHASE"
  ok=Filters.volume(d15);STORE.add_stage("VOLUME",ok)
  if not ok:return None,"VOLUME"
  ok=Filters.ticker(symbol);STORE.add_stage("TICKER",ok)
  if not ok:return None,"TICKER"
  ok=Filters.funding(symbol);STORE.add_stage("FUNDING",ok)
  if not ok:return None,"FUNDING"
  ok=Filters.oi(symbol);STORE.add_stage("OI",ok)
  if not ok:return None,"OI"
  ok=Filters.spread(symbol);STORE.add_stage("SPREAD",ok)
  if not ok:return None,"SPREAD"
  ok=Filters.btc(symbol,d);STORE.add_stage("BTC_FILTER",ok)
  if not ok:return None,"BTC_FILTER"
  lv=Risk.levels(d15,d1,d4,d);STORE.add_stage("TARGET_RISK",bool(lv))
  if not lv:return None,"TARGET_RISK"
  score=Score.calc(d4,d1,d15,reg,bos,rc)
  ok=score>=Config.MIN_SCORE;STORE.add_stage("SCORE",ok)
  if not ok:return None,"SCORE"
  x=d15.iloc[-1]
  return {
   "symbol":symbol,"direction":d.upper(),"entry":lv["entry"],"sl":lv["sl"],"tp":lv["tp"],
   "risk":lv["risk"],"rr":lv["rr"],"score":round(score,1),
   "rsi":safe(x.rsi),"atr_pct":safe(x.atr_pct),"volume_ratio":safe(x.volume_ratio),
   "bos_type":bos["type"],"bos_level":bos["level"],"confirm_age":age,
   "trend_structure":reg["structure"],"trend_quality":round(reg["quality"],1)
  },None
 except Exception as e:
  log.error("analyze %s: %s",symbol,e)
  return None,"ERROR"

class Telegram:
 @staticmethod
 def send(text):
  if not Config.BOT_TOKEN:return False
  try:
   Bot(Config.BOT_TOKEN).send_message(chat_id=Config.CHAT_ID,text=text)
   return True
  except Exception as e:log.error("telegram %s",e);return False
 @staticmethod
 def signal(x):
  side="🟢 LONG" if x["direction"]=="LONG" else "🔴 SHORT"
  return Telegram.send(
   f"🚨 SWING SIGNAL {side}\n\n"
   f"Coin: {x['symbol']}\n"
   f"Entry: {x['entry']:.8g}\n"
   f"SL: {x['sl']:.8g}\n"
   f"TP: {x['tp']:.8g}\n"
   f"RR: 1:{x['rr']:.2f}\n"
   f"Score: {x['score']:.1f}\n"
   f"RSI: {x['rsi']:.1f}\n"
   f"ATR: {x['atr_pct']:.2f}%\n"
   f"Volume: {x['volume_ratio']:.2f}x\n"
   f"BOS: {x['bos_type']}\n"
   f"4H Structure: {x['trend_structure']}"
  )
 @staticmethod
 def scan_done(n,found,sent):
  q=STORE.rejection_stats()
  r=" | ".join(f"{k}: {v}" for k,v in sorted(q.items(),key=lambda z:-z[1])) if q else "Yoxdur"
  return Telegram.send(
   f"✅ SCAN TAMAMLANDI\n\n"
   f"Analiz olunan: {n}\nUyğun setup: {len(found)}\nGöndərilən: {sent}\nAktiv: {len(STORE.active)}\n\n"
   f"📋 ŞƏRTLƏR\n{STORE.stage_report(n)}\n\n"
   f"🔎 BOS DETALLI\n{STORE.bos_report()}\n\n"
   f"🚫 İLK UĞURSUZ ŞƏRT\n{r}"
  )
class PositionManager:
 def monitor(self):
  while True:
   try:self.check()
   except Exception as e:log.error("monitor %s",e)
   time.sleep(Config.MONITOR_INTERVAL)
 def check(self):
  with STORE.lock:items=list(STORE.active)
  for x in items:
   t=BYBIT.ticker(x["symbol"],fresh=True)
   p=safe(t.get("lastPrice"))
   if p<=0:continue
   hit=None
   if x["direction"]=="LONG":
    if p<=x["sl"]:hit="SL"
    elif p>=x["tp"]:hit="TP"
   else:
    if p>=x["sl"]:hit="SL"
    elif p<=x["tp"]:hit="TP"
   created=x.get("created")
   expired=False
   try:expired=(now()-datetime.fromisoformat(created)).total_seconds()>Config.MAX_HOLD_HOURS*3600
   except:pass
   if hit or expired:self.close(x,hit or "TIMEOUT",p)
 def close(self,x,result,p):
  with STORE.lock:
   STORE.active=[a for a in STORE.active if a["symbol"]!=x["symbol"]]
   z=dict(x);z["status"]="CLOSED";z["result"]=result;z["exit"]=p;z["closed"]=now().isoformat()
   STORE.closed.append(z);STORE.closed=STORE.closed[-Config.MAX_CLOSED_SIGNALS_KEPT:];STORE.persist()
  emoji="✅" if result=="TP" else "❌" if result=="SL" else "⏱"
  Telegram.send(f"{emoji} SIGNAL {result}\n\n{x['symbol']} {x['direction']}\nEntry: {x['entry']:.8g}\nExit: {p:.8g}\nSL: {x['sl']:.8g}\nTP: {x['tp']:.8g}")

class Scanner:
 def __init__(self):
  self.lock=threading.Lock();self.running=False
 def scan(self):
  if not self.lock.acquire(False):return
  self.running=True;found=[];sent=0;symbols=[]
  try:
   STORE.reset_day();STORE.reset_rejections()
   if not STORE.can_add():
    Telegram.send(f"⏸ SCAN DAY LIMIT\nActive: {len(STORE.active)}\nToday: {STORE.daily_count}/{Config.MAX_DAILY_SIGNALS}")
    return
   symbols=top_symbols()
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
    if not Correlation.allowed(x["symbol"],STORE.active_symbols()):
     STORE.add_rejection(x["symbol"],"CORRELATION");continue
    if STORE.add(x):
     Telegram.signal(x);sent+=1
   Telegram.scan_done(len(symbols),found,sent)
  except Exception as e:
   log.exception("scan");Telegram.send(f"⚠️ SCAN ERROR\n{e}")
  finally:
   self.running=False;self.lock.release()
 def loop(self):
  while True:
   try:self.scan()
   except Exception:log.exception("loop")
   time.sleep(Config.CHECK_INTERVAL)

SCANNER=Scanner()
app=Flask(__name__)

@app.get("/")
def home():
 return jsonify({"bot":BOT_VERSION,"running":SCANNER.running,"active":len(STORE.active),"daily":STORE.daily_count})

@app.get("/status")
def status():
 return jsonify({"version":BOT_VERSION,"active":STORE.active,"closed":len(STORE.closed),"daily":STORE.daily_count})

@app.get("/signals")
def signals():
 return jsonify(STORE.active)

@app.get("/scan")
def manual_scan():
 if SCANNER.running:return jsonify({"ok":False,"message":"SCAN_ALREADY_RUNNING"}),409
 threading.Thread(target=SCANNER.scan,daemon=True).start()
 return jsonify({"ok":True,"message":"SCAN_STARTED"})

@app.get("/rejections")
def rejections():
 return jsonify({"stats":STORE.rejection_stats(),"details":STORE.rejections,"stages":STORE.stages,"bos":STORE.bos_debug})

def start():
 Telegram.send(
  f"🤖 {BOT_VERSION}\n"
  f"Bot aktivdir.\n"
  f"4H → 1H → 15M\n"
  f"MIN SCORE: {Config.MIN_SCORE}\n"
  f"RR: {Config.MIN_RR}-{Config.MAX_RR}"
 )
 threading.Thread(target=SCANNER.loop,daemon=True).start()
 threading.Thread(target=PositionManager().monitor,daemon=True).start()
 app.run(host="0.0.0.0",port=Config.FLASK_PORT,threaded=True)

if __name__=="__main__":
 start()
