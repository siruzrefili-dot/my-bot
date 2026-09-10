import os,time,json,math,signal,logging,threading,requests,numpy as np,pandas as pd
from datetime import datetime,timezone
from concurrent.futures import ThreadPoolExecutor,as_completed
from flask import Flask,jsonify

BOT_VERSION="10.3 CLEAN FINAL"

class Config:
 BOT_TOKEN=os.getenv("BOT_TOKEN","");CHAT_ID=os.getenv("CHAT_ID","1121794078")
 BASE_URL="https://api.bybit.com";CATEGORY="linear"
 SCAN_TOP_N=40;CANDIDATE_LIMIT=100;CHECK_INTERVAL=300;MONITOR_INTERVAL=5;PARALLEL_WORKERS=6
 TREND_TF="240";SETUP_TF="60";ENTRY_TF="15"
 EMA_FAST=50;EMA_SLOW=200;RSI_PERIOD=14;ATR_PERIOD=14;VOLUME_LOOKBACK=20
 MIN_SCORE=62;MIN_RR=2.;MAX_RR=3.;ACCOUNT_BALANCE=1000.;RISK_PERCENT=1.
 MAX_ACTIVE_SIGNALS=3;MAX_DAILY_SIGNALS=5;MAX_SIGNALS_TO_SEND=3
 MIN_ATR_PCT=.15;MAX_ATR_PCT=8.;MIN_VOLUME_RATIO=1.05
 MIN_EMA_DISTANCE_PCT=.10;MAX_SL_ATR=3.
 MAX_ABS_FUNDING=.002;MIN_OI_CHANGE_PCT=-8.;OI_LOOKBACK=5
 BOS_LOOKBACK=35;RETEST_MAX_BARS=4;RETEST_ATR_DISTANCE=.60;CONFIRMATION_MAX_BARS_AFTER_RETEST=2
 BTC_FILTER_ENABLED=True;CORRELATION_FILTER_ENABLED=True;MAX_CORRELATED_ACTIVE=2;CORRELATION_THRESHOLD=.85
 DEFAULT_LEVERAGE=10;MAX_HOLD_HOURS=96
 DATA_DIR="swing_bot_data";FLASK_PORT=int(os.getenv("PORT","10000"))
 REQUEST_TIMEOUT=15;CACHE_TTL=10;TICKER_CACHE_TTL=30;INSTRUMENT_CACHE_TTL=3600
 MAX_CLOSED_SIGNALS_KEPT=200
 SIGNAL_FILE=os.path.join(DATA_DIR,"signals.json")
 FALLBACK_COINS=["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT","ADAUSDT","AVAXUSDT","DOGEUSDT","LINKUSDT","SUIUSDT","ARBUSDT","OPUSDT"]
 NON_CRYPTO={"XAU","XAG","CL","AAPL","NVDA","TSLA","AMZN","META","MSFT","GOOG","GOOGL","COIN","MSTR","SOXL","SNDK","SKHYNIX","SKHY"}

os.makedirs(Config.DATA_DIR,exist_ok=True)
logging.basicConfig(level=logging.INFO,format="%(asctime)s | %(levelname)s | %(message)s")
log=logging.getLogger("SWING_AI");STOP_EVENT=threading.Event()

def safe_float(v,d=0.):
 try:
  x=float(v);return d if not math.isfinite(x) else x
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
  self.base=Config.BASE_URL;self.cache=Cache(Config.CACHE_TTL);self.tcache=Cache(Config.TICKER_CACHE_TTL);self.icache=Cache(Config.INSTRUMENT_CACHE_TTL)
  self.local=threading.local()
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
  z=self.get("/v5/market/kline",{"category":Config.CATEGORY,"symbol":symbol,"interval":interval,"limit":limit},f"k:{symbol}:{interval}:{limit}")
  rows=z.get("result",{}).get("list",[]) if z else []
  if not rows:return pd.DataFrame()
  rows=list(reversed(rows));cols=["timestamp","open","high","low","close","volume","turnover"]
  df=pd.DataFrame(rows,columns=cols)
  for c in cols:df[c]=pd.to_numeric(df[c],errors="coerce")
  df=df.dropna(subset=["open","high","low","close","volume"])
  return df.iloc[:-1].reset_index(drop=True) if len(df)>1 else df.reset_index(drop=True)
 def ticker(self,symbol):
  z=self.get("/v5/market/tickers",{"category":Config.CATEGORY,"symbol":symbol},f"t:{symbol}",self.tcache)
  a=z.get("result",{}).get("list",[]) if z else []
  if not a:return {}
  a=a[0]
  return {"symbol":a.get("symbol",""),"last_price":safe_float(a.get("lastPrice")),"change":safe_float(a.get("price24hPcnt"))*100,"funding":safe_float(a.get("fundingRate")),"oi":safe_float(a.get("openInterest")),"turnover24h":safe_float(a.get("turnover24h"))}
 def all_tickers(self):
  z=self.get("/v5/market/tickers",{"category":Config.CATEGORY},"ALL_LINEAR_TICKERS",self.tcache)
  return z.get("result",{}).get("list",[]) if z else []
 def oi(self,symbol,limit=10):
  z=self.get("/v5/market/open-interest",{"category":Config.CATEGORY,"symbol":symbol,"intervalTime":"1h","limit":limit},f"oi:{symbol}:{limit}")
  rows=z.get("result",{}).get("list",[]) if z else []
  if not rows:return pd.DataFrame()
  df=pd.DataFrame(rows)
  for c in ["openInterest","timestamp"]:
   if c in df:df[c]=pd.to_numeric(df[c],errors="coerce")
  return df.sort_values("timestamp").reset_index(drop=True)
 def book(self,symbol):
  z=self.get("/v5/market/orderbook",{"category":Config.CATEGORY,"symbol":symbol,"limit":25},f"b:{symbol}")
  return z.get("result",{}) if z else {}
 def instrument(self,symbol):
  z=self.icache.get(symbol)
  if z is not None:return z
  x=self.get("/v5/market/instruments-info",{"category":Config.CATEGORY,"symbol":symbol},f"i:{symbol}")
  a=x.get("result",{}).get("list",[]) if x else []
  if not a:return {}
  a=a[0];p=a.get("priceFilter",{});q=a.get("lotSizeFilter",{})
  z={"tick":safe_float(p.get("tickSize")),"step":safe_float(q.get("qtyStep")),"min":safe_float(q.get("minOrderQty")),"max":safe_float(q.get("maxOrderQty")),"status":a.get("status",""),"contractType":a.get("contractType",""),"quoteCoin":a.get("quoteCoin",""),"settleCoin":a.get("settleCoin",""),"baseCoin":a.get("baseCoin","")}
  self.icache.set(symbol,z);return z

BYBIT=BybitClient()

def validate_config():
 if not 0<=Config.MIN_SCORE<=100:raise ValueError("MIN_SCORE")
 if Config.MIN_RR<=0 or Config.MAX_RR<Config.MIN_RR:raise ValueError("RR")
 if Config.RISK_PERCENT<=0:raise ValueError("RISK_PERCENT")

class Indicators:
 @staticmethod
 def ema(s,n):return s.ewm(span=n,adjust=False).mean()
 @staticmethod
 def rsi(s,n=14):
  d=s.diff();g=d.clip(lower=0);l=-d.clip(upper=0);ag=g.ewm(alpha=1/n,adjust=False).mean();al=l.ewm(alpha=1/n,adjust=False).mean()
  rs=ag/al.replace(0,np.nan);r=100-100/(1+rs)
  return r.where(al!=0,100).where(~((ag==0)&(al==0)),50)
 @staticmethod
 def atr(df,n=14):
  pc=df.close.shift(1);tr=pd.concat([df.high-df.low,(df.high-pc).abs(),(df.low-pc).abs()],axis=1).max(axis=1)
  return tr.ewm(alpha=1/n,adjust=False).mean()
 @staticmethod
 def add(df):
  x=df.copy();x["ema_fast"]=Indicators.ema(x.close,Config.EMA_FAST);x["ema_slow"]=Indicators.ema(x.close,Config.EMA_SLOW)
  x["rsi"]=Indicators.rsi(x.close,Config.RSI_PERIOD);x["atr"]=Indicators.atr(x,Config.ATR_PERIOD);x["volume_ma"]=x.volume.rolling(Config.VOLUME_LOOKBACK).mean()
  x["volume_ratio"]=x.volume/x.volume_ma.replace(0,np.nan);x["ema_distance_pct"]=(x.ema_fast-x.ema_slow)/x.close*100;x["atr_pct"]=x.atr/x.close*100
  return x
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
  x=df.iloc[-1];st=Structure.trend(df);dist=abs(safe_float(x.ema_distance_pct));atr=safe_float(x.atr_pct)
  q=min(dist,1)*35+min(atr/3,1)*20+(45 if st!="neutral" else 20)
  lo=x.close>x.ema_slow and x.ema_fast>x.ema_slow and dist>=Config.MIN_EMA_DISTANCE_PCT
  so=x.close<x.ema_slow and x.ema_fast<x.ema_slow and dist>=Config.MIN_EMA_DISTANCE_PCT
  if lo:return {"direction":"long","quality":min(100,q),"structure":st}
  if so:return {"direction":"short","quality":min(100,q),"structure":st}
  return {"direction":"neutral","quality":min(100,q),"structure":st}

class Strategy:
 @staticmethod
 def setup(df,d):
  if len(df)<100:return False
  x=df.iloc[-1]
  return bool(x.close>x.ema_slow and x.ema_fast>x.ema_slow) if d=="long" else bool(x.close<x.ema_slow and x.ema_fast<x.ema_slow)
 @staticmethod
 def pullback(df,d):
  if len(df)<6:return False
  x=df.iloc[-6:];atr=safe_float(df.atr.iloc[-1]);ema=safe_float(df.ema_fast.iloc[-1])
  if atr<=0:return False
  return bool((x.low<=ema+atr*1.5).any()) if d=="long" else bool((x.high>=ema-atr*1.5).any())
 @staticmethod
 def rsi_score(df,d):
  b=safe_float(df.rsi.iloc[-1])
  if d=="long":
   if 48<=b<=58:return 100
   if 42<=b<48 or 58<b<=65:return 75
   if b<42:return 55
   return 45
  if 42<=b<=52:return 100
  if 35<=b<42 or 52<b<=58:return 75
  if b>58:return 55
  return 45
 @staticmethod
 def bos(df,d):
  sh,sl=Structure.swings(df);n=len(df);best=None;swings=sh if d=="long" else sl
  for si,level in reversed(swings):
   if si<max(0,n-Config.BOS_LOOKBACK-10):continue
   for i in range(si+1,n):
    if (d=="long" and df.close.iloc[i]>level) or (d=="short" and df.close.iloc[i]<level):
     if best is None or i>best["bar"]:best={"level":level,"bar":i,"swing":si}
     break
  if not best or n-1-best["bar"]>Config.RETEST_MAX_BARS+Config.CONFIRMATION_MAX_BARS_AFTER_RETEST+1:return None
  return best
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
   touch=(x.low<=lv+atr*Config.RETEST_ATR_DISTANCE) if d=="long" else (x.high>=lv-atr*Config.RETEST_ATR_DISTANCE)
   hold=(x.close>=lv) if d=="long" else (x.close<=lv)
   if not(touch and hold):continue
   for j in range(i+1,min(len(df),i+2+Config.CONFIRMATION_MAX_BARS_AFTER_RETEST)):
    if Strategy.strong(df,j,d) and ((d=="long" and df.close.iloc[j]>lv) or (d=="short" and df.close.iloc[j]<lv)):
     return {"bos":bos["bar"],"retest":i,"confirm":j,"level":lv}
  return None
 @staticmethod
 def stop(df,e,d):
  sh,sl=Structure.swings(df);atr=safe_float(df.atr.iloc[-1])
  if d=="long":
   v=[x for _,x in sl if x<e];return max(v)-atr*.1 if v else e-atr*1.5
  v=[x for _,x in sh if x>e];return min(v)+atr*.1 if v else e+atr*1.5
 @staticmethod
 def zone_score(df,d):
  hi=safe_float(df.high.iloc[-50:].max());lo=safe_float(df.low.iloc[-50:].min());mid=(hi+lo)/2;p=safe_float(df.close.iloc[-1]);atr=safe_float(df.atr.iloc[-1])
  if atr<=0:return 50
  if d=="long":return 100 if p<=mid else (70 if p<=mid+atr else 35)
  return 100 if p>=mid else (70 if p>=mid-atr else 35)

class Filters:
 @staticmethod
 def volatility(df):return valid(df,30) and Config.MIN_ATR_PCT<=safe_float(df.atr_pct.iloc[-1])<=Config.MAX_ATR_PCT
 @staticmethod
 def volume(df):return valid(df,Config.VOLUME_LOOKBACK+2) and safe_float(df.volume_ratio.iloc[-1])>=Config.MIN_VOLUME_RATIO
 @staticmethod
 def funding(t):return abs(safe_float(t.get("funding")))<=Config.MAX_ABS_FUNDING
 @staticmethod
 def oi(df):
  if not valid(df,2):return True
  try:
   a=float(df.openInterest.iloc[-1]);b=float(df.openInterest.iloc[-min(Config.OI_LOOKBACK,len(df))])
   return True if b<=0 else ((a-b)/b*100)>=Config.MIN_OI_CHANGE_PCT
  except:return True
 @staticmethod
 def spread(book):
  try:
   b=float(book["b"][0][0]);a=float(book["a"][0][0]);return b>0 and ((a-b)/b*100)<=.15
  except:return True

class BTCFilter:
 @staticmethod
 def allowed(direction):
  if not Config.BTC_FILTER_ENABLED:return True
  df=BYBIT.klines("BTCUSDT","240",250)
  if not valid(df,220):return True
  r=Regime.analyze(Indicators.add(df))
  return True if r["direction"]=="neutral" else r["direction"]==direction

class Correlation:
 @staticmethod
 def allowed(symbol,direction,active):
  if not Config.CORRELATION_FILTER_ENABLED:return True
  n=0
  for s in active:
   if s==symbol:continue
   try:
    a=BYBIT.klines(symbol,"60",80);b=BYBIT.klines(s,"60",80)
    if not valid(a,40) or not valid(b,40):continue
    m=min(len(a),len(b));c=a.close.pct_change().tail(m).corr(b.close.pct_change().tail(m))
    if safe_float(c)>=Config.CORRELATION_THRESHOLD:n+=1
   except:pass
  return n<Config.MAX_CORRELATED_ACTIVE

class Risk:
 @staticmethod
 def levels(df,d):
  e=safe_float(df.close.iloc[-1]);atr=safe_float(df.atr.iloc[-1])
  if e<=0 or atr<=0:return None
  raw=Strategy.stop(df,e,d);maxdist=atr*Config.MAX_SL_ATR
  if abs(e-raw)>maxdist:return None
  risk=abs(e-raw)
  if risk<=0:return None
  tp=e+risk*Config.MIN_RR if d=="long" else e-risk*Config.MIN_RR
  return {"entry":e,"sl":raw,"tp":tp,"risk":risk,"rr":Config.MIN_RR}

class Scoring:
 @staticmethod
 def calculate(d4,d1,d15,d,seq):
  rsi=Strategy.rsi_score(d15,d)
  vals={"trend":Regime.analyze(d4)["quality"],"pullback":90 if Strategy.pullback(d1,d) else 0,"bos":100 if seq else 0,"rsi":rsi,"zone":Strategy.zone_score(d15,d),"volume":min(100,safe_float(d15.volume_ratio.iloc[-1])*80),"atr":80 if Config.MIN_ATR_PCT<=safe_float(d15.atr_pct.iloc[-1])<=Config.MAX_ATR_PCT else 0}
  score=vals["trend"]*.20+vals["pullback"]*.15+vals["bos"]*.20+vals["rsi"]*.10+vals["zone"]*.10+vals["volume"]*.10+vals["atr"]*.15
  return round(score,1),rsi,vals

class Analyzer:
 def __init__(self,symbol):self.symbol=symbol;self.reject=""
 def run(self):
  s=self.symbol;d4=BYBIT.klines(s,"240",300);d1=BYBIT.klines(s,"60",300);d15=BYBIT.klines(s,"15",300)
  if not(valid(d4,220) and valid(d1,220) and valid(d15,80)):self.reject="DATA";return None
  d4=Indicators.add(d4);d1=Indicators.add(d1);d15=Indicators.add(d15);reg=Regime.analyze(d4);d=reg["direction"]
  if d=="neutral":self.reject="4H_TREND";return None
  if not Strategy.setup(d1,d):self.reject="1H_SETUP";return None
  if not Strategy.pullback(d1,d):self.reject="1H_PULLBACK";return None
  if not Filters.volatility(d15):self.reject="ATR";return None
  bos=Strategy.bos(d15,d)
  if not bos:self.reject="BOS";return None
  seq=Strategy.sequence(d15,bos,d)
  if not seq:self.reject="RETEST_CONFIRM";return None
  if not Filters.volume(d15):self.reject="VOLUME";return None
  ticker=BYBIT.ticker(s)
  if not ticker:self.reject="TICKER";return None
  if not Filters.funding(ticker):self.reject="FUNDING";return None
  if not Filters.oi(BYBIT.oi(s,10)):self.reject="OI";return None
  if not Filters.spread(BYBIT.book(s)):self.reject="SPREAD";return None
  if not BTCFilter.allowed(d):self.reject="BTC_FILTER";return None
  lv=Risk.levels(d15,d)
  if not lv:self.reject="RISK";return None
  score,rsi,parts=Scoring.calculate(d4,d1,d15,d,seq)
  if score<Config.MIN_SCORE:self.reject="SCORE";return None
  return {"symbol":s,"direction":d,"score":score,"rsi":safe_float(d15.rsi.iloc[-1]),"rsi_score":rsi,"entry":lv["entry"],"sl":lv["sl"],"tp":lv["tp"],"rr":lv["rr"],"atr_pct":safe_float(d15.atr_pct.iloc[-1]),"volume_ratio":safe_float(d15.volume_ratio.iloc[-1]),"funding":safe_float(ticker.get("funding")),"oi":safe_float(ticker.get("oi")),"bos":bos,"sequence":seq,"created_at":utc_iso(),"created_ts":time.time()}
def analyze_symbol(symbol):
 try:
  a=Analyzer(symbol);x=a.run();return x,("" if x else a.reject)
 except Exception as e:log.warning("%s analyze: %s",symbol,e);return None,"ERROR"

class Store:
 def __init__(self):
  self.lock=threading.RLock();self.active=[];self.closed=[];self.rejections={};self.day=utc_now().date().isoformat();self.daily_count=0
 def reset_day(self):
  d=utc_now().date().isoformat()
  if d!=self.day:self.day=d;self.daily_count=0
 def add_rejection(self,s,r):
  with self.lock:self.rejections[s]=r
 def active_symbols(self):
  with self.lock:return [x["symbol"] for x in self.active]
 def can_add(self):
  self.reset_day();return len(self.active)<Config.MAX_ACTIVE_SIGNALS and self.daily_count<Config.MAX_DAILY_SIGNALS
 def add(self,x):
  with self.lock:
   self.reset_day()
   if not self.can_add() or any(a["symbol"]==x["symbol"] for a in self.active):return False
   self.active.append(x);self.daily_count+=1;self.save();return True
 def remove(self,s,result):
  with self.lock:
   self.active=[x for x in self.active if x["symbol"]!=s];x=dict(result);x["closed_at"]=utc_iso();self.closed.append(x);self.closed=self.closed[-Config.MAX_CLOSED_SIGNALS_KEPT:];self.save()
 def save(self):
  try:
   with open(Config.SIGNAL_FILE,"w",encoding="utf8") as f:json.dump({"active":self.active,"closed":self.closed,"day":self.day,"daily_count":self.daily_count},f,indent=2)
  except Exception as e:log.warning("save: %s",e)
 def load(self):
  try:
   with open(Config.SIGNAL_FILE,"r",encoding="utf8") as f:x=json.load(f)
   self.active=x.get("active",[]);self.closed=x.get("closed",[]);self.day=x.get("day",self.day)
   if "daily_count" in x:self.daily_count=x.get("daily_count",0)
   else:self.daily_count=sum(1 for z in self.active+self.closed if str(z.get("created_at","")).startswith(self.day))
   self.reset_day()
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
  if not Config.BOT_TOKEN or not Config.CHAT_ID:return False
  try:return requests.post(f"https://api.telegram.org/bot{Config.BOT_TOKEN}/sendMessage",json={"chat_id":Config.CHAT_ID,"text":text},timeout=10).ok
  except Exception as e:log.warning("telegram: %s",e);return False
 @staticmethod
 def signal(x):
  d="🟢 LONG" if x["direction"]=="long" else "🔴 SHORT"
  return Telegram.send(f"🚨 SWING AI {BOT_VERSION}\n\n{x['symbol']} {d}\nScore: {x['score']}/100\nRSI: {x['rsi']:.1f}\nEntry: {x['entry']:.8g}\nSL: {x['sl']:.8g}\nTP: {x['tp']:.8g}\nRR: 1:{x['rr']:.1f}\nATR: {x['atr_pct']:.2f}%\nVol: {x['volume_ratio']:.2f}x\n⚠️ Signal only — no automatic order.")
 @staticmethod
 def status():
  if not STORE.active:return "📭 Aktiv siqnal yoxdur."
  return "📌 AKTİV SİQNALLAR\n\n"+"\n".join(f"{x['symbol']} {x['direction'].upper()}\nEntry: {x['entry']:.8g}\nSL: {x['sl']:.8g}\nTP: {x['tp']:.8g}\nRR: 1:{x['rr']:.1f}\n" for x in STORE.active)
 @staticmethod
 def handle(text):
  t=(text or "").strip().lower()
  if t in ("/status","/signals"):return Telegram.status()
  if t=="/stats":
   s=Performance.stats();return f"📊 STATS\nClosed: {s['closed']}\nWins: {s['wins']}\nLosses: {s['losses']}\nWinrate: {s['winrate']}%\nPnL: {s['pnl']:.2f} USDT\nToday signals: {s['today_signals']}"
  if t=="/rejections":
   q={}
   for v in STORE.rejections.values():q[v]=q.get(v,0)+1
   return "🚫 REJECTIONS\n"+("\n".join(f"{k}: {v}" for k,v in sorted(q.items(),key=lambda z:-z[1])) if q else "Yoxdur.")
  if t=="/help":return "/status\n/signals\n/stats\n/scan\n/rejections\n/help"
  return None

class PositionManager:
 def check(self,x):
  t=BYBIT.ticker(x["symbol"])
  if not t:return
  p=safe_float(t.get("last_price"));e=safe_float(x["entry"]);sl=safe_float(x["sl"]);tp=safe_float(x["tp"]);d=x["direction"];age=(time.time()-safe_float(x.get("created_ts",time.time())))/3600;result=None
  if age>=Config.MAX_HOLD_HOURS:result="TIME"
  elif d=="long" and p<=sl:result="SL"
  elif d=="long" and p>=tp:result="TP"
  elif d=="short" and p>=sl:result="SL"
  elif d=="short" and p<=tp:result="TP"
  if result:
   rr=x["rr"] if result=="TP" else -1
   if result=="TIME":rr=(p-e)/(e-sl) if d=="long" else (e-p)/(sl-e)
   x["result"]=result;x["result_r"]=rr;x["exit_price"]=p;x["pnl"]=rr*(Config.ACCOUNT_BALANCE*Config.RISK_PERCENT/100)
   STORE.remove(x["symbol"],x);Telegram.send(f"🔔 CLOSED\n{x['symbol']} {d.upper()}\nResult: {result}\nExit: {p:.8g}\nR: {rr:.2f}\nPnL: {x['pnl']:.2f} USDT")
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
   candidates=[]
   for x in BYBIT.all_tickers():
    s=x.get("symbol","");turn=safe_float(x.get("turnover24h"))
    if s.endswith("USDT") and turn>0 and s[:-4] not in Config.NON_CRYPTO:candidates.append((s,turn))
   candidates.sort(key=lambda z:z[1],reverse=True);out=[]
   for s,turn in candidates[:Config.CANDIDATE_LIMIT]:
    info=BYBIT.instrument(s)
    if info and info.get("status")=="Trading" and info.get("contractType")=="LinearPerpetual" and info.get("quoteCoin")=="USDT":out.append(s)
    if len(out)>=Config.SCAN_TOP_N:break
   return out or Config.FALLBACK_COINS[:Config.SCAN_TOP_N]
  except Exception as e:log.warning("symbols: %s",e);return Config.FALLBACK_COINS[:Config.SCAN_TOP_N]
 def scan(self):
  if not self.lock.acquire(False):log.info("SCAN skipped: another scan is running");return
  self.running=True
  try:
   STORE.reset_day()
   if not STORE.can_add():log.info("SCAN skipped: active=%d daily=%d",len(STORE.active),STORE.daily_count);return
   found=[]
   with ThreadPoolExecutor(max_workers=Config.PARALLEL_WORKERS) as ex:
    fs={ex.submit(analyze_symbol,s):s for s in self.symbols()}
    for f in as_completed(fs):
     s=fs[f]
     try:x,r=f.result()
     except Exception:x,r=None,"ERROR"
     if x:found.append(x)
     else:STORE.add_rejection(s,r)
   found.sort(key=lambda x:x["score"],reverse=True);sent=0
   for x in found:
    if sent>=Config.MAX_SIGNALS_TO_SEND or not STORE.can_add():break
    if not Correlation.allowed(x["symbol"],x["direction"],STORE.active_symbols()):
     STORE.add_rejection(x["symbol"],"CORRELATION");continue
    if STORE.add(x):
     Telegram.signal(x);sent+=1
   log.info("SCAN complete | candidates=%d | sent=%d | active=%d | today=%d",len(found),sent,len(STORE.active),STORE.daily_count)
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
    r=requests.get(url,params={"timeout":20,"offset":self.offset},timeout=25);data=r.json()
    for u in data.get("result",[]):
     self.offset=u["update_id"]+1;m=u.get("message",{})
     if str(m.get("chat",{}).get("id",""))!=str(Config.CHAT_ID):continue
     text=m.get("text","")
     if text.strip().lower()=="/scan":
      if SCANNER.running:Telegram.send("⏳ Scan artıq işləyir.")
      else:Telegram.send("🔎 Scan başladıldı...");threading.Thread(target=SCANNER.scan,daemon=True).start()
      continue
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
def web_rejections():
 q={}
 for v in STORE.rejections.values():q[v]=q.get(v,0)+1
 return jsonify(q)

def flask_worker():
 try:app.run(host="0.0.0.0",port=Config.FLASK_PORT,debug=False,use_reloader=False)
 except Exception as e:log.warning("flask: %s",e)
def stop_handler(*_):
 log.info("Stopping bot...");STOP_EVENT.set()
signal.signal(signal.SIGINT,stop_handler);signal.signal(signal.SIGTERM,stop_handler)

def startup():
 validate_config()
 Telegram.send(f"🤖 SWING AI {BOT_VERSION}\nBot başladı.\n4H → 1H → 15M\nMIN SCORE: {Config.MIN_SCORE}\nMAX DAILY: {Config.MAX_DAILY_SIGNALS}")
 log.info("="*50);log.info("SWING AI %s STARTED",BOT_VERSION)
 log.info("4H=%s | 1H=%s | 15M=%s",Config.TREND_TF,Config.SETUP_TF,Config.ENTRY_TF)
 log.info("MIN SCORE=%s | RR=1:%.1f | ACTIVE=%s | DAILY=%s",Config.MIN_SCORE,Config.MIN_RR,Config.MAX_ACTIVE_SIGNALS,Config.MAX_DAILY_SIGNALS)
 log.info("4H TREND -> 1H SETUP/PULLBACK -> 15M BOS/RETEST/CONFIRM")
 log.info("="*50)

def start_threads():
 threading.Thread(target=flask_worker,name="Flask",daemon=True).start()
 threading.Thread(target=ScannerWorker().run,name="Scanner",daemon=True).start()
 threading.Thread(target=MANAGER.run,name="Monitor",daemon=True).start()
 threading.Thread(target=POLL.run,name="Telegram",daemon=True).start()

def main():
 startup();start_threads()
 while not STOP_EVENT.is_set():time.sleep(1)
 log.info("BOT STOPPED")

if __name__=="__main__":main()
