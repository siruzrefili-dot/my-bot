import os,time,json,math,signal,logging,threading,tempfile,requests,numpy as np,pandas as pd
from datetime import datetime,timezone
from concurrent.futures import ThreadPoolExecutor,as_completed
from flask import Flask,jsonify

BOT_VERSION="9.9 BALANCED FIXED"
class Config:
 BOT_TOKEN=os.getenv("BOT_TOKEN","");CHAT_ID=os.getenv("CHAT_ID","1121794078");BASE_URL="https://api.bybit.com";CATEGORY="linear"
 SCAN_TOP_N=40;CHECK_INTERVAL=300;MONITOR_INTERVAL=5;PARALLEL_WORKERS=6
 TREND_TF="240";SETUP_TF="60";ENTRY_TF="15";MONITOR_TF="1"
 EMA_FAST=50;EMA_SLOW=200;RSI_PERIOD=14;ATR_PERIOD=14;SWING_LOOKBACK=5
 MIN_SCORE=62;MIN_RR=2.;MAX_RR=3.;ACCOUNT_BALANCE=1000.;RISK_PERCENT=1.
 MAX_ACTIVE_SIGNALS=3;MAX_DAILY_SIGNALS=5;MAX_SIGNALS_TO_SEND=3
 COMMISSION_PERCENT=.04;SLIPPAGE_PERCENT=.02
 MIN_ATR_PCT=.15;MAX_ATR_PCT=8.;MIN_VOLUME_RATIO=1.05;VOLUME_LOOKBACK=20
 MIN_EMA_DISTANCE_PCT=.10;MAX_SL_ATR=3.0
 MAX_ABS_FUNDING=.002;MIN_OI_CHANGE_PCT=-8.;OI_LOOKBACK=5
 BOS_LOOKBACK=35;RETEST_MAX_BARS=4;RETEST_ATR_DISTANCE=.60
 CONFIRMATION_MAX_BARS_AFTER_RETEST=2;CONFIRMATION_LOOKBACK=2
 BTC_FILTER_ENABLED=True;CORRELATION_FILTER_ENABLED=True;MAX_CORRELATED_ACTIVE=2;CORRELATION_THRESHOLD=.85
 PARTIAL_TP_PERCENT=50.;BREAKEVEN_AFTER_R=1.;TRAILING_AFTER_R=1.5;TRAILING_ATR_MULT=1.2;DEFAULT_LEVERAGE=10
 MAX_HOLD_HOURS=96
 DATA_DIR="swing_bot_data";FLASK_PORT=int(os.getenv("PORT","10000"))
 REQUEST_TIMEOUT=15;CACHE_TTL=10;TICKER_CACHE_TTL=30;INSTRUMENT_CACHE_TTL=3600;MAX_CLOSED_SIGNALS_KEPT=200
 FALLBACK_COINS=["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT","ADAUSDT","AVAXUSDT","DOGEUSDT","LINKUSDT","SUIUSDT","ARBUSDT","OPUSDT"]
 SIGNAL_FILE=os.path.join(DATA_DIR,"signals.json")
os.makedirs(Config.DATA_DIR,exist_ok=True)
logging.basicConfig(level=logging.INFO,format="%(asctime)s | %(levelname)s | %(message)s")
log=logging.getLogger("SWING_AI");STOP_EVENT=threading.Event()

def safe_float(v,d=0.):
 try:
  x=float(v);return d if not math.isfinite(x) else x
 except:return d
def utc_now():return datetime.now(timezone.utc)
def utc_iso():return utc_now().isoformat()
def utc_timestamp():return int(utc_now().timestamp())

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
  self.base=Config.BASE_URL;self.cache=Cache(Config.CACHE_TTL);self.tcache=Cache(Config.TICKER_CACHE_TTL);self.icache=Cache(Config.INSTRUMENT_CACHE_TTL);self.session=requests.Session()
 def get(self,path,params=None,key=None,cache=None):
  c=cache or self.cache
  if key:
   z=c.get(key)
   if z is not None:return z
  try:
   r=self.session.get(self.base+path,params=params or {},timeout=Config.REQUEST_TIMEOUT);r.raise_for_status();z=r.json()
   if z.get("retCode")!=0:raise RuntimeError(z.get("retMsg","API error"))
   if key:c.set(key,z)
   return z
  except Exception as e:log.warning("API %s: %s",path,e);return None
 def klines(self,symbol,interval,limit=300):
  z=self.get("/v5/market/kline",{"category":Config.CATEGORY,"symbol":symbol,"interval":interval,"limit":limit},f"k:{symbol}:{interval}:{limit}")
  rows=z.get("result",{}).get("list",[]) if z else []
  if not rows:return pd.DataFrame()
  rows=list(reversed(rows));cols=["timestamp","open","high","low","close","volume","turnover"];df=pd.DataFrame(rows,columns=cols)
  for c in cols:df[c]=pd.to_numeric(df[c],errors="coerce")
  df=df.dropna(subset=["open","high","low","close","volume"])
  if len(df)>1:df=df.iloc[:-1]
  return df.reset_index(drop=True)
 def ticker(self,symbol):
  z=self.get("/v5/market/tickers",{"category":Config.CATEGORY,"symbol":symbol},f"t:{symbol}",self.tcache);a=z.get("result",{}).get("list",[]) if z else []
  if not a:return {}
  a=a[0]
  return {"symbol":a.get("symbol",""),"last_price":safe_float(a.get("lastPrice")),"change":safe_float(a.get("price24hPcnt"))*100,"funding":safe_float(a.get("fundingRate")),"oi":safe_float(a.get("openInterest")),"turnover24h":safe_float(a.get("turnover24h"))}
 def all_tickers(self):
  z=self.get("/v5/market/tickers",{"category":Config.CATEGORY},"ALL_LINEAR_TICKERS",self.tcache);return z.get("result",{}).get("list",[]) if z else []
 def oi(self,symbol,limit=10):
  z=self.get("/v5/market/open-interest",{"category":Config.CATEGORY,"symbol":symbol,"intervalTime":"1h","limit":limit},f"oi:{symbol}:{limit}");rows=z.get("result",{}).get("list",[]) if z else []
  if not rows:return pd.DataFrame()
  df=pd.DataFrame(rows)
  for c in ["openInterest","timestamp"]:
   if c in df:df[c]=pd.to_numeric(df[c],errors="coerce")
  return df.sort_values("timestamp").reset_index(drop=True) if "timestamp" in df else df
 def book(self,symbol):
  z=self.get("/v5/market/orderbook",{"category":Config.CATEGORY,"symbol":symbol,"limit":25},f"b:{symbol}");return z.get("result",{}) if z else {}
 def instrument(self,symbol):
  z=self.icache.get(symbol)
  if z is not None:return z
  x=self.get("/v5/market/instruments-info",{"category":Config.CATEGORY,"symbol":symbol},f"i:{symbol}");a=x.get("result",{}).get("list",[]) if x else []
  if not a:return {}
  a=a[0];p=a.get("priceFilter",{});q=a.get("lotSizeFilter",{})
  z={"tick":safe_float(p.get("tickSize")),"step":safe_float(q.get("qtyStep")),"min":safe_float(q.get("minOrderQty")),"max":safe_float(q.get("maxOrderQty"))};self.icache.set(symbol,z);return z

BYBIT=BybitClient()
def valid(df,n):return isinstance(df,pd.DataFrame) and len(df)>=n
def validate_config():
 if not 0<=Config.MIN_SCORE<=100:raise ValueError("MIN_SCORE")
 if Config.MIN_RR<=0 or Config.MAX_RR<Config.MIN_RR:raise ValueError("RR")
 if Config.RISK_PERCENT<=0:raise ValueError("RISK_PERCENT")
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
  if d=="long":return bool(x.close>x.ema_slow and x.ema_fast>x.ema_slow and x.close>=x.ema_fast*.995)
  return bool(x.close<x.ema_slow and x.ema_fast<x.ema_slow and x.close<=x.ema_fast*1.005)

 @staticmethod
 def pullback(df,d):
  if len(df)<6:return False
  x=df.iloc[-6:];atr=safe_float(df.atr.iloc[-1]);ema=safe_float(df.ema_fast.iloc[-1])
  if atr<=0:return False
  if d=="long":return bool((x.low<=ema+atr).any())
  return bool((x.high>=ema-atr).any())

 @staticmethod
 def rsi_ok(df,d):
  if len(df)<3:return False
  a=safe_float(df.rsi.iloc[-2]);b=safe_float(df.rsi.iloc[-1])
  return b>=47 and b>=a if d=="long" else b<=53 and b<=a

 @staticmethod
 def bos(df,d):
  sh,sl=Structure.swings(df);n=len(df);best=None
  swings=sh if d=="long" else sl
  for si,level in reversed(swings):
   if si<max(0,n-Config.BOS_LOOKBACK-10):continue
   for i in range(si+1,n):
    if d=="long" and df.close.iloc[i]>level:
     if best is None or i>best["bar"]:best={"level":level,"bar":i,"swing":si}
     break
    if d=="short" and df.close.iloc[i]<level:
     if best is None or i>best["bar"]:best={"level":level,"bar":i,"swing":si}
     break
  if not best or n-1-best["bar"]>Config.RETEST_MAX_BARS+Config.CONFIRMATION_MAX_BARS_AFTER_RETEST+1:return None
  return best

 @staticmethod
 def strong(df,i,d):
  if i<1 or i>=len(df):return False
  x=df.iloc[i];p=df.iloc[i-1];atr=safe_float(x.atr)
  if atr<=0 or abs(x.close-x.open)<atr*.5:return False
  if d=="long":return bool(x.close>x.open and x.close>p.close and x.high-x.close<=atr*.25)
  return bool(x.close<x.open and x.close<p.close and x.close-x.low<=atr*.25)

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
    if Strategy.strong(df,j,d):
     if d=="long" and df.close.iloc[j]>lv:return {"bos":bos["bar"],"retest":i,"confirm":j,"level":lv}
     if d=="short" and df.close.iloc[j]<lv:return {"bos":bos["bar"],"retest":i,"confirm":j,"level":lv}
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
  if d=="long":
   if p<=mid:return 100
   return 70 if p<=mid+atr else 35
  if p>=mid:return 100
  return 70 if p>=mid-atr else 35
class RejectReport:
 def __init__(self):self.lock=threading.RLock();self.data={}
 def set(self,s,r):
  with self.lock:self.data[s]=r
 def clear(self):
  with self.lock:self.data={}
 def text(self):
  with self.lock:
   if not self.data:return "No rejected coins."
   return "🔎 REJECTION REPORT\n"+"\n".join(f"{s}: {r}" for s,r in list(self.data.items())[:40])
REJECTIONS=RejectReport()

class Filters:
 @staticmethod
 def funding(v):return abs(safe_float(v))<=Config.MAX_ABS_FUNDING
 @staticmethod
 def oi_change(df):
  if df is None or len(df)<2:return 0.
  a=safe_float(df.openInterest.iloc[0]);b=safe_float(df.openInterest.iloc[-1])
  return (b-a)/a*100 if a>0 else 0.
 @staticmethod
 def oi(v):return v>=Config.MIN_OI_CHANGE_PCT
 @staticmethod
 def spread(book):
  try:
   b=book.get("b",[]);a=book.get("a",[])
   if not b or not a:return 999.
   bv=safe_float(b[0][0]);av=safe_float(a[0][0]);m=(av+bv)/2
   return (av-bv)/m*100 if m>0 else 999.
  except:return 999.

class BTCFilter:
 @staticmethod
 def direction(client):
  d4=client.klines("BTCUSDT",Config.TREND_TF,230);d1=client.klines("BTCUSDT",Config.SETUP_TF,220)
  if not(valid(d4,220) and valid(d1,100)):return "neutral"
  d4=Indicators.add(d4);d1=Indicators.add(d1)
  r4=Regime.analyze(d4);x=d1.iloc[-1]
  if r4["direction"]=="long" and x.close>x.ema_slow and x.ema_fast>x.ema_slow:return "long"
  if r4["direction"]=="short" and x.close<x.ema_slow and x.ema_fast<x.ema_slow:return "short"
  return "neutral"
 @staticmethod
 def allowed(client,d,s):
  if not Config.BTC_FILTER_ENABLED or s=="BTCUSDT":return True
  b=BTCFilter.direction(client)
  return b in (d,"neutral")

class Risk:
 @staticmethod
 def leverage(atr,e):
  r=atr/e if e>0 else .05
  return min(2 if r>.05 else 3 if r>.03 else 4 if r>.02 else 5,Config.DEFAULT_LEVERAGE)
 @staticmethod
 def size(e,sl,lev):
  d=abs(e-sl)
  if e<=0 or d<=0:return {}
  risk=Config.ACCOUNT_BALANCE*Config.RISK_PERCENT/100
  qty=min(risk/d,(Config.ACCOUNT_BALANCE*max(1,lev))/e)
  return {"risk":risk,"qty":qty}
 @staticmethod
 def costs(n):return n*2*(Config.COMMISSION_PERCENT+Config.SLIPPAGE_PERCENT)/100

class Precision:
 @staticmethod
 def price(v,t,mode="nearest"):
  if t<=0:return v
  if mode=="up":return round(math.ceil(v/t-1e-12)*t,12)
  if mode=="down":return round(math.floor(v/t+1e-12)*t,12)
  return round(round(v/t)*t,12)
 @staticmethod
 def quantity(v,step,minimum,maximum):
  if step>0:v=math.floor(v/step+1e-12)*step
  if minimum>0 and v<minimum:return 0.
  return min(v,maximum) if maximum>0 else v

class Correlation:
 @staticmethod
 def allowed(client,tracker,symbol,direction,extra=None):
  if not Config.CORRELATION_FILTER_ENABLED:return True
  active=tracker.active()+(extra or [])
  base=client.klines(symbol,Config.ENTRY_TF,80)
  if len(base)<50:return True
  a=base.close.pct_change().dropna();ncor=0
  for s in active:
   if s.get("symbol")==symbol or s.get("direction")!=direction:continue
   o=client.klines(s["symbol"],Config.ENTRY_TF,80)
   if len(o)<50:continue
   b=o.close.pct_change().dropna();n=min(len(a),len(b))
   if n<30:continue
   c=safe_float(a.iloc[-n:].corr(b.iloc[-n:]),0)
   if c>=Config.CORRELATION_THRESHOLD:ncor+=1
  return ncor<Config.MAX_CORRELATED_ACTIVE
class SignalScanner:
 def __init__(self,client,tracker):self.client=client;self.tracker=tracker
 def reject(self,s,r):return {"signal":None,"symbol":s,"reason":r}

 def analyze(self,s):
  try:
   d4=self.client.klines(s,Config.TREND_TF,300);d1=self.client.klines(s,Config.SETUP_TF,250);d15=self.client.klines(s,Config.ENTRY_TF,250)
   if not(valid(d4,220) and valid(d1,100) and valid(d15,100)):return self.reject(s,"DATA")
   d4=Indicators.add(d4);d1=Indicators.add(d1);d15=Indicators.add(d15)
   reg=Regime.analyze(d4);d=reg["direction"]
   if d not in ("long","short"):return self.reject(s,"4H_TREND")
   if not Strategy.setup(d1,d):return self.reject(s,"1H_SETUP")
   if not Strategy.pullback(d1,d):return self.reject(s,"PULLBACK")
   if not Strategy.rsi_ok(d15,d):return self.reject(s,"RSI")
   bos=Strategy.bos(d15,d)
   if not bos:return self.reject(s,"BOS")
   seq=Strategy.sequence(d15,bos,d)
   if not seq:return self.reject(s,"RETEST_CONFIRM")
   if seq["confirm"]<max(0,len(d15)-Config.CONFIRMATION_LOOKBACK):return self.reject(s,"OLD_CONFIRM")
   vol=safe_float(d15.volume_ratio.iloc[-1])
   if vol<Config.MIN_VOLUME_RATIO:return self.reject(s,"VOLUME")
   atrpct=safe_float(d15.atr_pct.iloc[-1])
   if not Config.MIN_ATR_PCT<=atrpct<=Config.MAX_ATR_PCT:return self.reject(s,"ATR")

   e=safe_float(d15.close.iloc[-1]);atr=safe_float(d15.atr.iloc[-1])
   if e<=0 or atr<=0:return self.reject(s,"PRICE")
   lev=Risk.leverage(atr,e);sl=Strategy.stop(d15,e,d)
   sl_dist=abs(e-sl)
   if sl_dist>atr*Config.MAX_SL_ATR:
    sl=e-atr*Config.MAX_SL_ATR if d=="long" else e+atr*Config.MAX_SL_ATR
   if d=="long" and sl>=e:return self.reject(s,"SL")
   if d=="short" and sl<=e:return self.reject(s,"SL")

   inst=self.client.instrument(s);tick=safe_float(inst.get("tick"));step=safe_float(inst.get("step"));mn=safe_float(inst.get("min"));mx=safe_float(inst.get("max"))
   if tick>0:
    e=Precision.price(e,tick)
    sl=Precision.price(sl,tick,"down" if d=="long" else "up")
   rd=Risk.size(e,sl,lev)
   if not rd:return self.reject(s,"RISK")

   dist=abs(e-sl);tp=e+dist*Config.MIN_RR if d=="long" else e-dist*Config.MIN_RR
   if tick>0:tp=Precision.price(tp,tick,"up" if d=="long" else "down")
   rr=abs(tp-e)/dist if dist>0 else 0
   if rr<Config.MIN_RR:return self.reject(s,"RR")

   tk=self.client.ticker(s);fund=safe_float(tk.get("funding"));oi=Filters.oi_change(self.client.oi(s,Config.OI_LOOKBACK+1))
   if not Filters.funding(fund):return self.reject(s,"FUNDING")
   if not Filters.oi(oi):return self.reject(s,"OI")
   if not BTCFilter.allowed(self.client,d,s):return self.reject(s,"BTC")

   spread=Filters.spread(self.client.book(s))
   if spread>.25:return self.reject(s,"SPREAD")

   zone=Strategy.zone_score(d15,d)
   rsi=safe_float(d15.rsi.iloc[-1]);momentum=75 if (d=="long" and rsi>=50) or (d=="short" and rsi<=50) else 60
   quality=safe_float(reg["quality"]);vs=min(vol/1.5*100,100);ois=min(max(50+oi*2.5,0),100);rrs=min(rr/Config.MAX_RR*100,100);ss=max(0,100-spread/.25*100)
   score=min(100,quality*.23+vs*.14+ois*.08+rrs*.18+momentum*.10+ss*.08+zone*.09+10)
   if score<Config.MIN_SCORE:return self.reject(s,f"SCORE_{score:.1f}")

   qty=Precision.quantity(rd["qty"],step,mn,mx)
   if qty<=0:return self.reject(s,"QTY")

   return {"signal":{
    "id":f"{s}_{d}_{utc_timestamp()}","symbol":s,"direction":d,"entry":e,"sl":sl,"tp":tp,
    "rr":round(rr,2),"score":round(score,2),"leverage":lev,"risk_amount":rd["risk"],
    "position_size":qty,"notional":qty*e,"estimated_cost":Risk.costs(qty*e),"atr":atr,
    "volume_ratio":vol,"oi_change_pct":oi,"funding_rate":fund,"spread_pct":spread,
    "zone_score":zone,"sequence":"BOS -> RETEST -> CONFIRMATION","bos_level":seq["level"],
    "bos_bar":seq["bos"],"retest_bar":seq["retest"],"confirm_bar":seq["confirm"],
    "created_at":utc_iso(),"status":"NEW"
   },"symbol":s,"reason":None}
  except Exception as e:
   log.warning("Analyze %s: %s",s,e);return self.reject(s,"ERROR")

 def scan(self,symbols):
  out=[];rej={}
  with ThreadPoolExecutor(max_workers=Config.PARALLEL_WORKERS) as ex:
   fs={ex.submit(self.analyze,s):s for s in symbols[:Config.SCAN_TOP_N]}
   for f in as_completed(fs):
    s=fs[f]
    try:
     z=f.result()
     if z.get("signal"):out.append(z["signal"])
     else:rej[z["symbol"]]=z["reason"]
    except Exception as e:
     log.warning("Scan %s: %s",s,e);rej[s]="ERROR"
  REJECTIONS.clear()
  for s,r in rej.items():REJECTIONS.set(s,r)
  return sorted(out,key=lambda x:x["score"],reverse=True)
class PerformanceTracker:
 def __init__(self):
  self.lock=threading.RLock();self.signals=[];self.stats={};self.load()
 def load(self):
  try:
   if os.path.exists(Config.SIGNAL_FILE):
    with open(Config.SIGNAL_FILE,"r",encoding="utf8") as f:self.signals=json.load(f)
  except:self.signals=[]
  self.recalc()
 def save(self):
  try:
   with self.lock:
    a=[x for x in self.signals if x.get("status")=="ACTIVE"];c=[x for x in self.signals if x.get("status")!="ACTIVE"][-Config.MAX_CLOSED_SIGNALS_KEPT:]
    fd,tmp=tempfile.mkstemp(dir=Config.DATA_DIR);os.close(fd)
    with open(tmp,"w",encoding="utf8") as f:json.dump(c+a,f,indent=2)
    os.replace(tmp,Config.SIGNAL_FILE)
  except Exception as e:log.warning("Save: %s",e)
 def active(self):
  with self.lock:return [x for x in self.signals if x.get("status")=="ACTIVE"]
 def get(self,sid):
  with self.lock:return next((x for x in self.signals if x.get("id")==sid),None)
 def can_add(self,s):
  today=utc_now().date().isoformat()
  daily=sum(str(x.get("activated_at",x.get("created_at",""))).startswith(today) for x in self.signals)
  return len(self.active())<Config.MAX_ACTIVE_SIGNALS and daily<Config.MAX_DAILY_SIGNALS and not any(x.get("symbol")==s["symbol"] and x.get("status")=="ACTIVE" for x in self.signals)
 def add(self,s):
  with self.lock:
   if not self.can_add(s):return False
   x=dict(s);x.update({"status":"ACTIVE","activated_at":utc_iso(),"original_sl":s["sl"],"original_risk":abs(s["entry"]-s["sl"]),"current_r":0.,"partial_taken":False,"breakeven":False,"trailing":False})
   self.signals.append(x);self.recalc();self.save();return True
 def close(self,sid,result,r,price,reason,cost=0):
  with self.lock:
   x=self.get(sid)
   if not x or x.get("status")!="ACTIVE":return False
   risk=safe_float(x.get("risk_amount"),1);net=r-cost/risk;result="WIN" if net>.05 else "LOSS" if net<-.05 else "BE"
   if result=="BE":net=0
   x.update({"status":"CLOSED","result":result,"result_r":round(net,4),"gross_r":round(r,4),"close_price":price,"close_reason":reason,"cost":cost,"closed_at":utc_iso()})
   self.recalc();self.save();return True
 def recalc(self):
  c=[x for x in self.signals if x.get("status")=="CLOSED"];w=[x for x in c if x.get("result")=="WIN"];l=[x for x in c if x.get("result")=="LOSS"];b=[x for x in c if x.get("result")=="BE"]
  r=sum(safe_float(x.get("result_r")) for x in c);p=sum(safe_float(x.get("result_r"))*safe_float(x.get("risk_amount")) for x in c)
  gp=sum(max(safe_float(x.get("result_r"))*safe_float(x.get("risk_amount")),0) for x in w);gl=abs(sum(min(safe_float(x.get("result_r"))*safe_float(x.get("risk_amount")),0) for x in l))
  self.stats={"total":len(c),"wins":len(w),"losses":len(l),"be":len(b),"win_rate":round(len(w)/len(c)*100,2) if c else 0,"total_r":round(r,4),"pnl":round(p,4),"profit_factor":round(gp/gl,3) if gl else 0,"active":len(self.active())}
 def text(self):
  self.recalc();s=self.stats
  return f"TRADES: {s['total']}\nWINS: {s['wins']} | LOSS: {s['losses']} | BE: {s['be']}\nWIN RATE: {s['win_rate']}%\nTOTAL R: {s['total_r']}\nPNL: {s['pnl']:.2f} USDT\nPF: {s['profit_factor']}\nACTIVE: {s['active']}"
TRACKER=PerformanceTracker()
class PositionManager:
 def __init__(self,client,tracker,notify=None):
  self.client=client;self.tracker=tracker;self.notify=notify;self.running=False
 def start(self):
  if self.running:return
  self.running=True;threading.Thread(target=self.loop,daemon=True,name="Monitor").start()
 def stop(self):self.running=False
 def loop(self):
  while self.running and not STOP_EVENT.is_set():
   changed=False
   for s in self.tracker.active():
    try:
     if self.process(s):changed=True
    except Exception:log.exception("Monitor %s",s.get("symbol"))
   if changed:self.tracker.save()
   STOP_EVENT.wait(Config.MONITOR_INTERVAL)
 def process(self,s):
  price=safe_float(self.client.ticker(s["symbol"]).get("last_price"))
  if price<=0:return False
  d=s["direction"];sl=safe_float(s["sl"]);tp=safe_float(s["tp"])
  try:
   created=s.get("activated_at",s.get("created_at"));age=(utc_now()-datetime.fromisoformat(created.replace("Z","+00:00"))).total_seconds()/3600
   if age>=Config.MAX_HOLD_HOURS:self.close(s,"EXPIRED",price);return True
  except:pass
  if (d=="long" and price<=sl) or (d=="short" and price>=sl):self.close(s,"SL",price);return True
  if (d=="long" and price>=tp) or (d=="short" and price<=tp):self.close(s,"TP",price);return True
  risk=safe_float(s.get("original_risk"));entry=safe_float(s.get("entry"))
  if risk<=0:return False
  r=(price-entry)/risk if d=="long" else (entry-price)/risk;changed=abs(r-safe_float(s.get("current_r")))>0.01
  if changed:s["current_r"]=round(r,4)
  if not s.get("partial_taken") and r>=s["rr"]*Config.PARTIAL_TP_PERCENT/100:
   s["partial_taken"]=True;changed=True
   if self.notify:self.notify.notify_partial(s)
  if not s.get("breakeven") and r>=Config.BREAKEVEN_AFTER_R:
   s["sl"]=entry;s["breakeven"]=True;changed=True
   if self.notify:self.notify.notify_breakeven(s)
  if r>=Config.TRAILING_AFTER_R and self.trailing(s,price):changed=True
  return changed
 def trailing(self,s,price):
  df=self.client.klines(s["symbol"],Config.MONITOR_TF,40)
  if len(df)<20:return False
  atr=safe_float(Indicators.add(df).atr.iloc[-1])
  if atr<=0:return False
  d=s["direction"];old=safe_float(s["sl"]);new=price-atr*Config.TRAILING_ATR_MULT if d=="long" else price+atr*Config.TRAILING_ATR_MULT
  if s.get("breakeven"):new=max(new,s["entry"]) if d=="long" else min(new,s["entry"])
  if (d=="long" and new>old) or (d=="short" and new<old):
   s["sl"]=new;s["trailing"]=True
   if self.notify:self.notify.notify_trailing(s)
   return True
  return False
 def close(self,s,reason,price):
  risk=safe_float(s.get("original_risk"));entry=safe_float(s.get("entry"))
  if risk<=0:return False
  r=(price-entry)/risk if s["direction"]=="long" else (entry-price)/risk;cost=Risk.costs(safe_float(s.get("notional",0)));result="WIN" if r>.05 else "LOSS" if r<-.05 else "BE"
  if result=="BE":r=0
  ok=self.tracker.close(s["id"],result,r,price,reason,cost)
  if ok and self.notify:self.notify.notify_close(s,result,reason,price,r)
  return ok
POSITION_MANAGER=PositionManager(BYBIT,TRACKER)
class TelegramManager:
 def __init__(self):
  self.token=Config.BOT_TOKEN;self.chat=str(Config.CHAT_ID);self.running=True;self.offset=0
 def send(self,text,chat_id=None):
  if not self.token:return False
  try:
   r=requests.post(f"https://api.telegram.org/bot{self.token}/sendMessage",json={"chat_id":str(chat_id or self.chat),"text":text},timeout=Config.REQUEST_TIMEOUT)
   return r.status_code==200 and r.json().get("ok",False)
  except:return False
 def signal(self,s):
  return self.send(f"🚨 SWING AI {s['direction'].upper()}\n\n{s['symbol']}\nEntry: {s['entry']:.8g}\nSL: {s['sl']:.8g}\nTP: {s['tp']:.8g}\nRR: 1:{s['rr']:.2f}\nScore: {s['score']}/100\nLeverage: {s['leverage']}x\nRisk: {s['risk_amount']:.2f} USDT\n\nBOS → RETEST → CONFIRMATION")
 def notify_close(self,s,result,reason,price,r):
  icon="✅" if result=="WIN" else "❌" if result=="LOSS" else "🟡";self.send(f"{icon} CLOSED\n{s['symbol']} {s['direction'].upper()}\nResult: {result}\nReason: {reason}\nPrice: {price:.8g}\nR: {r:.2f}")
 def notify_breakeven(self,s):self.send(f"🛡️ BREAKEVEN\n{s['symbol']}\nSL → Entry")
 def notify_trailing(self,s):self.send(f"📈 TRAILING\n{s['symbol']}\nSL: {s['sl']:.8g}")
 def notify_partial(self,s):self.send(f"💰 PARTIAL TP LEVEL\n{s['symbol']}\nR: {s['current_r']:.2f}")
 def help(self,c):self.send("🤖 SWING AI COMMANDS\n\n/start - Bot məlumatı\n/status - Status\n/signals - Aktiv siqnallar\n/stats - Statistikalar\n/scan - Dərhal analiz\n/rejections - Rədd səbəbləri\n/help - Əmrlər",c)
 def status(self,c):self.send(f"🟢 STATUS\n\nVersion: {BOT_VERSION}\nScanner: {'ON' if SCANNER_WORKER.running else 'OFF'}\nMonitor: {'ON' if POSITION_MANAGER.running else 'OFF'}\nActive: {len(TRACKER.active())}/{Config.MAX_ACTIVE_SIGNALS}",c)
 def signals(self,c):
  a=TRACKER.active()
  if not a:return self.send("📭 Aktiv siqnal yoxdur.",c)
  t="📌 AKTİV SİQNALLAR\n\n"
  for s in a:t+=f"🔥 {s['symbol']} {s['direction'].upper()}\nEntry: {s['entry']:.8g}\nSL: {s['sl']:.8g}\nTP: {s['tp']:.8g}\nRR: 1:{s['rr']:.2f}\nScore: {s['score']}/100\nR: {s.get('current_r',0):.2f}\n\n"
  self.send(t,c)
 def stats(self,c):self.send("📊 STATISTICS\n\n"+TRACKER.text(),c)
 def scan_now(self,c):
  self.send("🔎 TOP 40 coin analiz olunur...",c)
  try:
   results=SCANNER.scan(SCANNER_WORKER.symbols())
   if not results:return self.send("❌ Uyğun siqnal tapılmadı.\n\n"+REJECTIONS.text(),c)
   t=f"🔎 SCAN NƏTİCƏSİ\n\nNamizədlər: {len(results)}\n\n"
   for s in results[:5]:t+=f"• {s['symbol']} {s['direction'].upper()}\nScore: {s['score']}/100 | RR: 1:{s['rr']:.2f}\nEntry: {s['entry']:.8g}\n\n"
   self.send(t,c)
  except Exception as e:self.send(f"❌ Scan xətası: {e}",c)
 def handle(self,text,c):
  cmd=text.strip().split()[0].lower().split("@")[0]
  if cmd in ("/start","/help"):self.help(c)
  elif cmd=="/status":self.status(c)
  elif cmd=="/signals":self.signals(c)
  elif cmd=="/stats":self.stats(c)
  elif cmd=="/scan":threading.Thread(target=self.scan_now,args=(c,),daemon=True).start()
  elif cmd=="/rejections":self.send(REJECTIONS.text(),c)
  else:self.send("❓ Naməlum əmr.\n/help yaz.",c)
 def poll(self):
  if not self.token:return
  while self.running and not STOP_EVENT.is_set():
   try:
    r=requests.get(f"https://api.telegram.org/bot{self.token}/getUpdates",params={"timeout":20,"offset":self.offset},timeout=30);d=r.json()
    for u in d.get("result",[]):
     self.offset=u["update_id"]+1;m=u.get("message",{});text=m.get("text","");c=str(m.get("chat",{}).get("id",""))
     if text and c and (not self.chat or c==self.chat) and text.startswith("/"):self.handle(text,c)
   except Exception as e:log.warning("Telegram: %s",e);STOP_EVENT.wait(3)
 def start_commands(self):
  if self.token:threading.Thread(target=self.poll,daemon=True,name="Telegram").start()
TELEGRAM=TelegramManager();POSITION_MANAGER.notify=TELEGRAM
class ScannerWorker:
 def __init__(self):self.running=False
 def start(self):
  if self.running:return
  self.running=True;threading.Thread(target=self.loop,daemon=True,name="Scanner").start()
 def stop(self):self.running=False
 def symbols(self):
  try:
   rows=BYBIT.all_tickers();a=[]
   for x in rows:
    s=x.get("symbol","");v=safe_float(x.get("turnover24h"))
    if s.endswith("USDT") and v>0:a.append((s,v))
   a.sort(key=lambda x:x[1],reverse=True)
   return [x[0] for x in a[:Config.SCAN_TOP_N]] or Config.FALLBACK_COINS
  except Exception as e:
   log.warning("Symbols: %s",e);return Config.FALLBACK_COINS
 def loop(self):
  while self.running and not STOP_EVENT.is_set():
   try:
    syms=self.symbols();results=SCANNER.scan(syms);sent=0;selected=[]
    for s in results:
     if sent>=Config.MAX_SIGNALS_TO_SEND:break
     if not TRACKER.can_add(s):continue
     if not Correlation.allowed(BYBIT,TRACKER,s["symbol"],s["direction"],selected):continue
     if TRACKER.add(s):
      selected.append(s);TELEGRAM.signal(s);sent+=1
    log.info("SCAN top40=%d candidates=%d new=%d",len(syms),len(results),sent)
   except Exception:log.exception("Scanner error")
   STOP_EVENT.wait(Config.CHECK_INTERVAL)

SCANNER_WORKER=ScannerWorker();SCANNER=SignalScanner(BYBIT,TRACKER)
app=Flask(__name__)

@app.route("/")
def home():return jsonify({"bot":BOT_VERSION,"status":"running","active":len(TRACKER.active())})
@app.route("/health")
def health():return jsonify({"status":"ok","version":BOT_VERSION,"active":len(TRACKER.active())})
@app.route("/stats")
def stats():TRACKER.recalc();return jsonify(TRACKER.stats)

def flask_start():
 try:app.run(host="0.0.0.0",port=Config.FLASK_PORT,debug=False,use_reloader=False)
 except Exception as e:log.warning("Flask: %s",e)

def shutdown(sig=None,frame=None):
 if STOP_EVENT.is_set():return
 STOP_EVENT.set();SCANNER_WORKER.stop();POSITION_MANAGER.stop();TELEGRAM.running=False;TRACKER.save();log.info("BOT STOPPED")

signal.signal(signal.SIGINT,shutdown);signal.signal(signal.SIGTERM,shutdown)

def main():
 validate_config();log.info("="*45);log.info("SWING AI BOT %s",BOT_VERSION);log.info("4H EMA + 1H SETUP/PULLBACK + 15M BOS/RETEST/CONFIRM");log.info("TOP 40 TURNOVER | BALANCED FIXED");log.info("="*45)
 POSITION_MANAGER.start();SCANNER_WORKER.start();TELEGRAM.start_commands()
 if Config.BOT_TOKEN:
  TELEGRAM.send(f"🟢 SWING AI BOT {BOT_VERSION}\nSTARTED\n\n4H EMA TREND\n1H SETUP + PULLBACK\n15M BOS → RETEST → CONFIRMATION\nTOP 40 LIQUIDITY\n/help")
 threading.Thread(target=flask_start,daemon=True,name="Flask").start()
 while not STOP_EVENT.is_set():STOP_EVENT.wait(5)
 shutdown()

if __name__=="__main__":main()
