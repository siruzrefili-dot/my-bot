import os,sys,time,json,math,signal,logging,threading,tempfile
from datetime import datetime,timezone
from typing import Dict,List
from concurrent.futures import ThreadPoolExecutor,as_completed
import requests,numpy as np,pandas as pd

BOT_VERSION="9.6 PROFESSIONAL"
class Config:
    BOT_TOKEN=os.getenv("BOT_TOKEN","")
    CHAT_ID=os.getenv("CHAT_ID","1121794078")
    BASE_URL="https://api.bybit.com"
    CATEGORY="linear"
    SCAN_TOP_N=40
    MAX_SIGNALS_TO_SEND=3
    CHECK_INTERVAL=300
    MONITOR_INTERVAL=5
    MONITOR_TF="1"
    PARALLEL_WORKERS=6
    TREND_TF="240"
    SETUP_TF="60"
    ENTRY_TF="15"
    EMA_FAST=50
    EMA_SLOW=200
    RSI_PERIOD=14
    ATR_PERIOD=14
    SWING_LOOKBACK=5
    MIN_SCORE=65
    MIN_RR=2.0
    MAX_RR=3.0
    ACCOUNT_BALANCE=1000.0
    RISK_PERCENT=1.0
    MAX_ACTIVE_SIGNALS=3
    MAX_DAILY_SIGNALS=5
    COMMISSION_PERCENT=0.04
    SLIPPAGE_PERCENT=0.02
    MIN_ATR_PCT=0.15
    MAX_ATR_PCT=8.0
    VOLUME_LOOKBACK=20
    MIN_VOLUME_RATIO=1.10
    MIN_EMA_DISTANCE_PCT=0.15
    FUNDING_FILTER_ENABLED=True
    MAX_ABS_FUNDING=0.0015
    OI_FILTER_ENABLED=True
    OI_LOOKBACK=5
    MIN_OI_CHANGE_PCT=-5.0
    BTC_FILTER_ENABLED=True
    CORRELATION_FILTER_ENABLED=True
    MAX_CORRELATED_ACTIVE=2
    CORRELATION_THRESHOLD=0.80
    REQUIRE_RETEST=True
    RETEST_MAX_BARS=4
    RETEST_ATR_DISTANCE=0.60
    CONFIRMATION_MAX_BARS_AFTER_RETEST=2
    PARTIAL_TP_PERCENT=50.0
    BREAKEVEN_AFTER_R=1.0
    TRAILING_AFTER_R=1.5
    TRAILING_ATR_MULT=1.2
    DEFAULT_LEVERAGE=10
    DATA_DIR="swing_bot_data"
    SIGNAL_FILE=os.path.join(DATA_DIR,"signals.json")
    STATS_FILE=os.path.join(DATA_DIR,"stats.json")
    STATE_FILE=os.path.join(DATA_DIR,"state.json")
    FLASK_PORT=int(os.getenv("PORT","10000"))
    REQUEST_TIMEOUT=15
    CACHE_TTL=10
    INSTRUMENT_CACHE_TTL=3600
    MAX_CLOSED_SIGNALS_KEPT=200
    MAX_SIGNAL_LOG_KEPT=1000
    TELEGRAM_MAX_RETRY_ATTEMPTS=5
    PENDING_SIGNAL_MAX_AGE_SEC=900
    FALLBACK_COINS=["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT","ADAUSDT","AVAXUSDT","DOGEUSDT","LINKUSDT","SUIUSDT","ARBUSDT","OPUSDT"]

os.makedirs(Config.DATA_DIR,exist_ok=True)
logging.basicConfig(level=logging.INFO,format="%(asctime)s | %(levelname)s | %(message)s")
log=logging.getLogger("SWING_AI")
STOP_EVENT=threading.Event()

def safe_float(v,d=0.0):
    try:
        x=float(v)
        return d if not math.isfinite(x) else x
    except:return d

def utc_now():return datetime.now(timezone.utc)
def utc_iso():return utc_now().isoformat()
def utc_timestamp():return int(utc_now().timestamp())

class APICache:
    def __init__(self,ttl):
        self.ttl=ttl;self.data={};self.lock=threading.RLock()
    def get(self,k):
        with self.lock:
            x=self.data.get(k)
            if not x:return None
            if time.time()-x[1]>self.ttl:
                self.data.pop(k,None);return None
            return x[0]
    def set(self,k,v):
        with self.lock:self.data[k]=(v,time.time())

class InstrumentCache(APICache):pass

class BybitClient:
    def __init__(self):
        self.base=Config.BASE_URL
        self.cache=APICache(Config.CACHE_TTL)
        self.instrument_cache=InstrumentCache(Config.INSTRUMENT_CACHE_TTL)
        self.session=requests.Session()

    def _get(self,path,params=None,key=None):
        if key:
            x=self.cache.get(key)
            if x is not None:return x
        try:
            r=self.session.get(self.base+path,params=params or {},timeout=Config.REQUEST_TIMEOUT)
            r.raise_for_status()
            x=r.json()
            if x.get("retCode")!=0:raise RuntimeError(x.get("retMsg","API error"))
            if key:self.cache.set(key,x)
            return x
        except Exception as e:
            log.warning("API %s: %s",path,e);return None

    def fetch_klines(self,symbol,interval,limit=300):
        x=self._get("/v5/market/kline",{"category":Config.CATEGORY,"symbol":symbol,"interval":interval,"limit":limit},f"k:{symbol}:{interval}:{limit}")
        if not x:return pd.DataFrame()
        rows=x.get("result",{}).get("list",[])
        if not rows:return pd.DataFrame()
        rows=list(reversed(rows))
        cols=["timestamp","open","high","low","close","volume","turnover"]
        df=pd.DataFrame(rows,columns=cols)
        for c in cols:df[c]=pd.to_numeric(df[c],errors="coerce")
        df=df.dropna(subset=["open","high","low","close","volume"])
        if len(df)>1:df=df.iloc[:-1]
        return df.reset_index(drop=True)

    def fetch_ticker(self,symbol):
        x=self._get("/v5/market/tickers",{"category":Config.CATEGORY,"symbol":symbol},f"t:{symbol}")
        rows=x.get("result",{}).get("list",[]) if x else []
        if not rows:return {}
        a=rows[0]
        return {"last_price":safe_float(a.get("lastPrice")),"price_change_24h":safe_float(a.get("price24hPcnt"))*100,"funding_rate":safe_float(a.get("fundingRate")),"open_interest":safe_float(a.get("openInterest"))}

    def fetch_open_interest(self,symbol,interval="1h",limit=10):
        x=self._get("/v5/market/open-interest",{"category":Config.CATEGORY,"symbol":symbol,"intervalTime":interval,"limit":limit},f"oi:{symbol}:{interval}:{limit}")
        rows=x.get("result",{}).get("list",[]) if x else []
        if not rows:return pd.DataFrame()
        df=pd.DataFrame(rows)
        for c in ["openInterest","timestamp"]:
            if c in df:df[c]=pd.to_numeric(df[c],errors="coerce")
        return df.sort_values("timestamp").reset_index(drop=True) if "timestamp" in df else df

    def fetch_orderbook(self,symbol,limit=25):
        x=self._get("/v5/market/orderbook",{"category":Config.CATEGORY,"symbol":symbol,"limit":limit},f"b:{symbol}")
        return x.get("result",{}) if x else {}

    def fetch_instrument(self,symbol):
        x=self.instrument_cache.get(symbol)
        if x is not None:return x
        d=self._get("/v5/market/instruments-info",{"category":Config.CATEGORY,"symbol":symbol},f"i:{symbol}")
        rows=d.get("result",{}).get("list",[]) if d else []
        if not rows:return {}
        a=rows[0];p=a.get("priceFilter",{});q=a.get("lotSizeFilter",{})
        x={"tick_size":safe_float(p.get("tickSize")),"qty_step":safe_float(q.get("qtyStep")),"min_order_qty":safe_float(q.get("minOrderQty")),"max_order_qty":safe_float(q.get("maxOrderQty"))}
        self.instrument_cache.set(symbol,x);return x

BYBIT=BybitClient()

def validate_config():
    if not 0<=Config.MIN_SCORE<=100:raise ValueError("MIN_SCORE")
    if Config.MIN_RR<=0 or Config.MAX_RR<Config.MIN_RR:raise ValueError("RR")
    if Config.RISK_PERCENT<=0:raise ValueError("RISK_PERCENT")
    return True
class Indicators:
    @staticmethod
    def ema(s,n):return s.ewm(span=n,adjust=False).mean()
    @staticmethod
    def rsi(s,n=14):
        d=s.diff();g=d.clip(lower=0);l=-d.clip(upper=0)
        ag=g.ewm(alpha=1/n,adjust=False).mean();al=l.ewm(alpha=1/n,adjust=False).mean()
        rs=ag/al.replace(0,np.nan);r=100-(100/(1+rs))
        r=r.where(al!=0,100);return r.where(~((ag==0)&(al==0)),50)
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
    @staticmethod
    def swings(df,left=5,right=5):
        h=df.high.to_numpy();l=df.low.to_numpy();sh=[];sl=[]
        for i in range(left,len(df)-right):
            if h[i]>=max(h[i-left:i]) and h[i]>max(h[i+1:i+right+1]):sh.append((i,float(h[i])))
            if l[i]<=min(l[i-left:i]) and l[i]<min(l[i+1:i+right+1]):sl.append((i,float(l[i])))
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
        if len(df)<220:return {"direction":"neutral","quality":0}
        x=df.iloc[-1];st=Structure.trend(df)
        dist=abs(safe_float(x.ema_distance_pct));atr=safe_float(x.atr_pct)
        q=min(dist,1)*40+min(atr/3,1)*20+(40 if st!="neutral" else 0)
        long_ok=x.close>x.ema_slow and x.ema_fast>x.ema_slow and st=="bullish" and dist>=Config.MIN_EMA_DISTANCE_PCT
        short_ok=x.close<x.ema_slow and x.ema_fast<x.ema_slow and st=="bearish" and dist>=Config.MIN_EMA_DISTANCE_PCT
        return {"direction":"long" if long_ok else "short" if short_ok else "neutral","quality":min(100,q),"structure":st}

def valid_df(df,n):
    return isinstance(df,pd.DataFrame) and len(df)>=n and all(c in df.columns for c in ["open","high","low","close","volume"])
class Strategy:
    @staticmethod
    def setup(df,d):
        if len(df)<100:return False
        x=df.iloc[-1]
        return x.ema_fast>x.ema_slow and x.close>x.ema_fast if d=="long" else x.ema_fast<x.ema_slow and x.close<x.ema_fast
    @staticmethod
    def pullback(df,d):
        x=df.iloc[-Config.RETEST_MAX_BARS:];atr=safe_float(df.atr.iloc[-1]);ema=safe_float(df.ema_fast.iloc[-1])
        if atr<=0:return False
        return bool((x.low<=ema+atr*Config.RETEST_ATR_DISTANCE).any()) if d=="long" else bool((x.high>=ema-atr*Config.RETEST_ATR_DISTANCE).any())
    @staticmethod
    def rsi_ok(df,d):
        if len(df)<3:return False
        a=safe_float(df.rsi.iloc[-2]);b=safe_float(df.rsi.iloc[-1])
        return a<50<=b if d=="long" else a>50>=b
    @staticmethod
    def bos(df,d):
        sh,sl=Structure.swings(df)
        if d=="long" and sh:
            level=sh[-1][1]
            for i in range(sh[-1][0]+1,len(df)):
                if df.close.iloc[i]>level:return {"level":level,"bar":i}
        if d=="short" and sl:
            level=sl[-1][1]
            for i in range(sl[-1][0]+1,len(df)):
                if df.close.iloc[i]<level:return {"level":level,"bar":i}
        return None
    @staticmethod
    def strong(df,i,d):
        if i<1 or i>=len(df):return False
        x=df.iloc[i];p=df.iloc[i-1];atr=safe_float(x.atr)
        if atr<=0 or abs(x.close-x.open)<atr*.5:return False
        if d=="long":return x.close>x.open and x.close>p.close and x.high-x.close<=atr*.25
        return x.close<x.open and x.close<p.close and x.close-x.low<=atr*.25
    @staticmethod
    def sequence(df,bos,d):
        if not bos:return None
        level=bos["level"];start=bos["bar"]+1
        for i in range(start,min(len(df),start+Config.RETEST_MAX_BARS)):
            x=df.iloc[i];atr=safe_float(x.atr)
            touch=x.low<=level+atr*Config.RETEST_ATR_DISTANCE if d=="long" else x.high>=level-atr*Config.RETEST_ATR_DISTANCE
            hold=x.close>=level if d=="long" else x.close<=level
            if touch and hold:
                for j in range(i+1,min(len(df),i+1+Config.CONFIRMATION_MAX_BARS_AFTER_RETEST)):
                    if Strategy.strong(df,j,d):
                        if d=="long" and df.close.iloc[j]>level:return {"bos":bos["bar"],"retest":i,"confirm":j,"level":level}
                        if d=="short" and df.close.iloc[j]<level:return {"bos":bos["bar"],"retest":i,"confirm":j,"level":level}
        return None
    @staticmethod
    def stop(df,e,d):
        sh,sl=Structure.swings(df);atr=safe_float(df.atr.iloc[-1])
        if d=="long":
            v=[x for _,x in sl if x<e];return max(v)-atr*.1 if v else e-atr*1.5
        v=[x for _,x in sh if x>e];return min(v)+atr*.1 if v else e+atr*1.5
    @staticmethod
    def target(e,sl,d):
        r=abs(e-sl)
        return e+r*Config.MIN_RR if d=="long" else e-r*Config.MIN_RR
    @staticmethod
    def pd_zone(df,d):
        hi=df.high.iloc[-50:].max();lo=df.low.iloc[-50:].min();mid=(hi+lo)/2;p=df.close.iloc[-1]
        return p<=mid if d=="long" else p>=mid
class MarketFilters:
    @staticmethod
    def funding(x):return not Config.FUNDING_FILTER_ENABLED or abs(safe_float(x))<=Config.MAX_ABS_FUNDING
    @staticmethod
    def oi_change(df):
        if df is None or len(df)<2:return 0
        a=safe_float(df.openInterest.iloc[0]);b=safe_float(df.openInterest.iloc[-1])
        return (b-a)/a*100 if a>0 else 0
    @staticmethod
    def oi(x):return not Config.OI_FILTER_ENABLED or x>=Config.MIN_OI_CHANGE_PCT
    @staticmethod
    def btc(x,d,s):
        if not Config.BTC_FILTER_ENABLED or s=="BTCUSDT" or abs(x)<.5:return True
        return x>0 if d=="long" else x<0
    @staticmethod
    def spread(book):
        try:
            bid=safe_float(book["b"][0][0]);ask=safe_float(book["a"][0][0]);mid=(bid+ask)/2
            return (ask-bid)/mid*100 if mid>0 else 999
        except:return 999
    @staticmethod
    def correlation(client,symbol):
        if not Config.CORRELATION_FILTER_ENABLED:return True
        active=TRACKER.active_signals()
        if not active:return True
        base=client.fetch_klines(symbol,Config.ENTRY_TF,80)
        if len(base)<50:return True
        a=base.close.pct_change().dropna()
        for s in active:
            if s["symbol"]==symbol:continue
            other=client.fetch_klines(s["symbol"],Config.ENTRY_TF,80)
            if len(other)<50:continue
            b=other.close.pct_change().dropna()
            n=min(len(a),len(b))
            if n<30:continue
            corr=a.iloc[-n:].corr(b.iloc[-n:])
            if safe_float(corr)>=Config.CORRELATION_THRESHOLD:
                count=sum(1 for z in active if z["symbol"]!=symbol and z["direction"]==s["direction"])
                if count>=Config.MAX_CORRELATED_ACTIVE:return False
        return True

class Risk:
    @staticmethod
    def size(e,sl):
        dist=abs(e-sl)
        if e<=0 or dist<=0:return {}
        amount=Config.ACCOUNT_BALANCE*Config.RISK_PERCENT/100
        return {"risk_amount":amount,"qty":amount/dist}
    @staticmethod
    def leverage(atr,e):
        r=atr/e
        return min(2 if r>.05 else 3 if r>.03 else 4 if r>.02 else 5,Config.DEFAULT_LEVERAGE)
    @staticmethod
    def costs(notional):
        return notional*(Config.COMMISSION_PERCENT+Config.SLIPPAGE_PERCENT)/100

class Precision:
    @staticmethod
    def step(v,s):
        return v if s<=0 else math.floor(v/s+1e-12)*s
class SignalScanner:
    def __init__(self,client):self.client=client
    def analyze(self,symbol):
        try:
            d4=self.client.fetch_klines(symbol,Config.TREND_TF,300)
            d1=self.client.fetch_klines(symbol,Config.SETUP_TF,250)
            d15=self.client.fetch_klines(symbol,Config.ENTRY_TF,250)
            if not(valid_df(d4,220) and valid_df(d1,100) and valid_df(d15,100)):return None
            d4=Indicators.add(d4);d1=Indicators.add(d1);d15=Indicators.add(d15)
            reg=Regime.analyze(d4);direction=reg["direction"]
            if direction not in ("long","short"):return None
            if not Strategy.setup(d1,direction) or not Strategy.pullback(d1,direction):return None
            if not Strategy.rsi_ok(d15,direction):return None
            bos=Strategy.bos(d15,direction);seq=Strategy.sequence(d15,bos,direction)
            if not seq or seq["confirm"]!=len(d15)-1:return None
            if not Strategy.pd_zone(d15,direction):return None
            if safe_float(d15.volume.iloc[-1]/d15.volume.iloc[-Config.VOLUME_LOOKBACK:].mean())<Config.MIN_VOLUME_RATIO:return None
            if not(Config.MIN_ATR_PCT<=safe_float(d15.atr_pct.iloc[-1])<=Config.MAX_ATR_PCT):return None
            entry=safe_float(d15.close.iloc[-1]);sl=Strategy.stop(d15,entry,direction)
            if direction=="long" and sl>=entry:return None
            if direction=="short" and sl<=entry:return None
            tp=Strategy.target(entry,sl,direction)
            inst=self.client.fetch_instrument(symbol);tick=safe_float(inst.get("tick_size"));step=safe_float(inst.get("qty_step"))
            if tick>0:
                entry=Precision.step(entry,tick);sl=Precision.step(sl,tick);tp=Precision.step(tp,tick)
            risk=abs(entry-sl);rr=abs(tp-entry)/risk if risk>0 else 0
            if rr<Config.MIN_RR:return None
            rr=min(rr,Config.MAX_RR)
            ticker=self.client.fetch_ticker(symbol)
            funding=safe_float(ticker.get("funding_rate"))
            if not MarketFilters.funding(funding):return None
            oi=MarketFilters.oi_change(self.client.fetch_open_interest(symbol,"1h",Config.OI_LOOKBACK+1))
            if not MarketFilters.oi(oi):return None
            btc=0 if symbol=="BTCUSDT" else safe_float(self.client.fetch_ticker("BTCUSDT").get("price_change_24h"))
            if not MarketFilters.btc(btc,direction,symbol):return None
            spread=MarketFilters.spread(self.client.fetch_orderbook(symbol))
            if spread>.20 or not MarketFilters.correlation(self.client,symbol):return None
            vol=safe_float(d15.volume_ratio.iloc[-1])
            momentum=80 if (direction=="long" and d15.rsi.iloc[-1]>50) or (direction=="short" and d15.rsi.iloc[-1]<50) else 60
            score=reg["quality"]*.25+min(vol/2*100,100)*.15+min(max(50+oi*2.5,0),100)*.1+min(rr/Config.MAX_RR*100,100)*.2+momentum*.1+(100-min(spread/.2*100,100))*.1+10
            if score<Config.MIN_SCORE:return None
            rd=Risk.size(entry,sl);qty=Precision.step(rd["qty"],step)
            if qty<=0:return None
            return {"id":f"{symbol}_{direction}_{utc_timestamp()}","symbol":symbol,"direction":direction,"entry":entry,"sl":sl,"tp":tp,"rr":rr,"score":round(min(score,100),2),"leverage":Risk.leverage(safe_float(d15.atr.iloc[-1]),entry),"risk_amount":rd["risk_amount"],"position_size":qty,"notional":qty*entry,"commission_cost":Risk.costs(qty*entry),"atr":safe_float(d15.atr.iloc[-1]),"volume_ratio":vol,"oi_change_pct":oi,"funding_rate":funding,"spread_pct":spread,"sequence":"BOS -> RETEST -> CONFIRMATION","bos_level":seq["level"],"created_at":utc_iso(),"status":"NEW"}
        except Exception as e:
            log.warning("Analyze %s: %s",symbol,e);return None
    def scan(self,symbols):
        out=[]
        with ThreadPoolExecutor(max_workers=Config.PARALLEL_WORKERS) as ex:
            fs=[ex.submit(self.analyze,s) for s in symbols[:Config.SCAN_TOP_N]]
            for f in as_completed(fs):
                try:
                    x=f.result()
                    if x:out.append(x)
                except:pass
        return sorted(out,key=lambda x:x["score"],reverse=True)

SCANNER=SignalScanner(BYBIT)
class PerformanceTracker:
    def __init__(self):
        self.lock=threading.RLock();self.signals=[];self.stats={};self.load()
    def load(self):
        try:
            if os.path.exists(Config.SIGNAL_FILE):
                with open(Config.SIGNAL_FILE,"r",encoding="utf8") as f:self.signals=json.load(f)
        except:pass
        self.recalc()
    def save(self):
        try:
            fd,tmp=tempfile.mkstemp(dir=Config.DATA_DIR);os.close(fd)
            with open(tmp,"w",encoding="utf8") as f:json.dump(self.signals[-Config.MAX_CLOSED_SIGNALS_KEPT:],f,indent=2)
            os.replace(tmp,Config.SIGNAL_FILE)
        except Exception as e:log.warning("Save: %s",e)
    def active(self):return [x for x in self.signals if x.get("status")=="ACTIVE"]
    def get(self,sid):
        return next((x for x in self.signals if x.get("id")==sid),None)
    def can_add(self,s):
        today=utc_now().date().isoformat()
        return len(self.active())<Config.MAX_ACTIVE_SIGNALS and sum(str(x.get("created_at","")).startswith(today) for x in self.signals)<Config.MAX_DAILY_SIGNALS and not any(x.get("symbol")==s["symbol"] and x.get("status")=="ACTIVE" for x in self.signals)
    def add(self,s):
        with self.lock:
            if not self.can_add(s):return False
            x=dict(s);x.update({"status":"ACTIVE","activated_at":utc_iso(),"original_sl":s["sl"],"original_risk":abs(s["entry"]-s["sl"]),"current_r":0.0,"partial_taken":False,"breakeven":False,"trailing":False})
            self.signals.append(x);self.save();return True
    def close(self,sid,result,r,price,reason):
        with self.lock:
            x=self.get(sid)
            if not x or x.get("status")!="ACTIVE":return False
            x.update({"status":"CLOSED","result":result,"result_r":round(r,4),"close_price":price,"close_reason":reason,"closed_at":utc_iso()})
            self.recalc();self.save();return True
    def recalc(self):
        c=[x for x in self.signals if x.get("status")=="CLOSED"];w=[x for x in c if x.get("result")=="WIN"];l=[x for x in c if x.get("result")=="LOSS"]
        r=sum(safe_float(x.get("result_r")) for x in c)
        pnl=sum(safe_float(x.get("result_r"))*safe_float(x.get("risk_amount")) for x in c)
        gp=sum(safe_float(x.get("result_r"))*safe_float(x.get("risk_amount")) for x in w)
        gl=abs(sum(safe_float(x.get("result_r"))*safe_float(x.get("risk_amount")) for x in l))
        self.stats={"total":len(c),"wins":len(w),"losses":len(l),"be":len(c)-len(w)-len(l),"win_rate":round(len(w)/len(c)*100,2) if c else 0,"total_r":round(r,4),"pnl":round(pnl,4),"profit_factor":round(gp/gl,3) if gl else 0,"active":len(self.active())}
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
            for s in self.tracker.active():
                try:self.process(s)
                except Exception:log.exception("Monitor %s",s.get("symbol"))
            STOP_EVENT.wait(Config.MONITOR_INTERVAL)
    def process(self,s):
        price=safe_float(self.client.fetch_ticker(s["symbol"]).get("last_price"))
        if price<=0:return
        d=s["direction"];sl=safe_float(s["sl"]);tp=safe_float(s["tp"])
        if (d=="long" and price<=sl) or (d=="short" and price>=sl):
            self.close(s,"SL",price);return
        if (d=="long" and price>=tp) or (d=="short" and price<=tp):
            self.close(s,"TP",price);return
        risk=safe_float(s["original_risk"]);entry=safe_float(s["entry"])
        r=(price-entry)/risk if d=="long" else (entry-price)/risk
        s["current_r"]=round(r,4)
        if not s["partial_taken"] and r>=s["rr"]*Config.PARTIAL_TP_PERCENT/100:
            s["partial_taken"]=True
            if self.notify:self.notify.notify_partial(s)
        if not s["breakeven"] and r>=Config.BREAKEVEN_AFTER_R:
            s["sl"]=entry;s["breakeven"]=True
            if self.notify:self.notify.notify_breakeven(s)
        if r>=Config.TRAILING_AFTER_R:self.trailing(s,price)
        self.tracker.save()
    def trailing(self,s,price):
        df=self.client.fetch_klines(s["symbol"],Config.MONITOR_TF,40)
        if len(df)<20:return
        df=Indicators.add(df);atr=safe_float(df.atr.iloc[-1])
        if atr<=0:return
        d=s["direction"];old=safe_float(s["sl"]);new=price-atr*Config.TRAILING_ATR_MULT if d=="long" else price+atr*Config.TRAILING_ATR_MULT
        if d=="long" and s["breakeven"]:new=max(new,s["entry"])
        if d=="short" and s["breakeven"]:new=min(new,s["entry"])
        if (d=="long" and new>old) or (d=="short" and new<old):
            s["sl"]=new;s["trailing"]=True
            if self.notify:self.notify.notify_trailing(s)
    def close(self,s,reason,price):
        risk=safe_float(s["original_risk"]);entry=safe_float(s["entry"])
        r=(price-entry)/risk if s["direction"]=="long" else (entry-price)/risk
        if reason=="TP":r=abs(r)
        result="WIN" if r>.05 else "LOSS" if r<-.05 else "BE"
        if result=="BE":r=0
        if self.tracker.close(s["id"],result,r,price,reason) and self.notify:self.notify.notify_close(s,result,reason,price,r)

POSITION_MANAGER=PositionManager(BYBIT,TRACKER)
class TelegramManager:
    def __init__(self):
        self.token=Config.BOT_TOKEN
        self.chat=str(Config.CHAT_ID)
        self.running=True
        self.offset=0
        self.lock=threading.RLock()

    def send(self,text,chat_id=None):
        if not self.token:return False
        cid=str(chat_id or self.chat)
        try:
            r=requests.post(
                f"https://api.telegram.org/bot{self.token}/sendMessage",
                json={"chat_id":cid,"text":text},
                timeout=Config.REQUEST_TIMEOUT
            )
            return r.status_code==200 and r.json().get("ok",False)
        except Exception as e:
            log.warning("Telegram send: %s",e)
            return False

    def signal(self,s):
        return self.send(
            f"🚨 SWING AI {s['direction'].upper()}\n\n"
            f"{s['symbol']}\n"
            f"Entry: {s['entry']:.8g}\n"
            f"SL: {s['sl']:.8g}\n"
            f"TP: {s['tp']:.8g}\n"
            f"RR: 1:{s['rr']:.2f}\n"
            f"Score: {s['score']}/100\n"
            f"Leverage: {s['leverage']}x\n"
            f"Risk: {s['risk_amount']:.2f} USDT\n\n"
            f"BOS → RETEST → CONFIRMATION"
        )

    def notify_close(self,s,result,reason,price,r):
        icon="✅" if result=="WIN" else "❌" if result=="LOSS" else "🟡"
        self.send(
            f"{icon} CLOSED\n"
            f"{s['symbol']} {s['direction'].upper()}\n"
            f"Result: {result}\n"
            f"Reason: {reason}\n"
            f"Price: {price:.8g}\n"
            f"R: {r:.2f}"
        )

    def notify_breakeven(self,s):
        self.send(f"🛡️ BREAKEVEN\n{s['symbol']}\nSL → Entry")

    def notify_trailing(self,s):
        self.send(f"📈 TRAILING\n{s['symbol']}\nSL: {s['sl']:.8g}")

    def notify_partial(self,s):
        self.send(f"💰 PARTIAL TP LEVEL\n{s['symbol']}\nR: {s['current_r']:.2f}")

    def commands(self,chat_id):
        active=TRACKER.active()

        if not active:
            active_text="Aktiv siqnal yoxdur."
        else:
            lines=[]
            for s in active:
                lines.append(
                    f"{s['symbol']} | {s['direction'].upper()}\n"
                    f"Entry: {s['entry']:.8g}\n"
                    f"SL: {s['sl']:.8g}\n"
                    f"TP: {s['tp']:.8g}\n"
                    f"RR: 1:{s['rr']:.2f}\n"
                    f"Score: {s['score']}/100"
                )
            active_text="\n\n".join(lines)

        text=(
            "🤖 SWING AI BOT\n\n"
            "📌 ƏMRLƏR:\n"
            "/start - Bot haqqında məlumat\n"
            "/status - Bot statusu\n"
            "/signals - Aktiv siqnallar\n"
            "/stats - Trading statistikası\n"
            "/scan - İndi analiz et\n"
            "/help - Əmrlər\n\n"
            f"📊 Aktiv: {len(active)}/{Config.MAX_ACTIVE_SIGNALS}\n\n"
            f"{active_text}"
        )
        self.send(text,chat_id)

    def status(self,chat_id):
        self.send(
            "🟢 SWING AI STATUS\n\n"
            f"Version: {BOT_VERSION}\n"
            f"Scanner: {'ON' if SCANNER_WORKER.running else 'OFF'}\n"
            f"Monitor: {'ON' if POSITION_MANAGER.running else 'OFF'}\n"
            f"Active signals: {len(TRACKER.active())}/{Config.MAX_ACTIVE_SIGNALS}\n"
            f"Scan interval: {Config.CHECK_INTERVAL}s\n"
            f"Monitor interval: {Config.MONITOR_INTERVAL}s",
            chat_id
        )

    def signals(self,chat_id):
        active=TRACKER.active()
        if not active:
            self.send("📭 Hazırda aktiv siqnal yoxdur.",chat_id)
            return

        text="📌 AKTİV SİQNALLAR\n\n"
        for s in active:
            text+=(
                f"🔥 {s['symbol']} {s['direction'].upper()}\n"
                f"Entry: {s['entry']:.8g}\n"
                f"SL: {s['sl']:.8g}\n"
                f"TP: {s['tp']:.8g}\n"
                f"RR: 1:{s['rr']:.2f}\n"
                f"Score: {s['score']}/100\n"
                f"Current R: {s.get('current_r',0):.2f}\n\n"
            )
        self.send(text,chat_id)

    def stats(self,chat_id):
        self.send("📊 TRADING STATISTICS\n\n"+TRACKER.text(),chat_id)

    def scan_now(self,chat_id):
        self.send("🔎 Analiz başlayır...",chat_id)
        try:
            results=SCANNER.scan(SCANNER_WORKER.symbols())
            if not results:
                self.send("❌ Hazırda uyğun siqnal tapılmadı.",chat_id)
                return

            text=f"🔎 SCAN NƏTİCƏSİ\n\nTapılan: {len(results)}\n\n"
            for s in results[:5]:
                text+=(
                    f"• {s['symbol']} {s['direction'].upper()}\n"
                    f"Score: {s['score']}/100\n"
                    f"RR: 1:{s['rr']:.2f}\n"
                    f"Entry: {s['entry']:.8g}\n\n"
                )
            self.send(text,chat_id)
        except Exception as e:
            self.send(f"❌ Scan xətası: {e}",chat_id)

    def help(self,chat_id):
        self.send(
            "🤖 SWING AI ƏMRLƏRİ\n\n"
            "/start — Bot haqqında məlumat\n"
            "/status — Botun işləmə vəziyyəti\n"
            "/signals — Aktiv LONG/SHORT siqnallar\n"
            "/stats — Win rate, P&L, R və PF\n"
            "/scan — Dərhal bütün coinləri analiz et\n"
            "/help — Əmrlər siyahısı",
            chat_id
        )

    def handle(self,text,chat_id):
        cmd=text.strip().split()[0].lower().split("@")[0]

        if cmd=="/start":
            self.commands(chat_id)
        elif cmd=="/status":
            self.status(chat_id)
        elif cmd=="/signals":
            self.signals(chat_id)
        elif cmd=="/stats":
            self.stats(chat_id)
        elif cmd=="/scan":
            threading.Thread(
                target=self.scan_now,
                args=(chat_id,),
                daemon=True
            ).start()
        elif cmd=="/help":
            self.help(chat_id)
        else:
            self.send("❓ Naməlum əmr.\n/help yaz.",chat_id)

    def poll(self):
        if not self.token:
            log.warning("BOT_TOKEN yoxdur. Telegram command sistemi aktiv deyil.")
            return

        while self.running and not STOP_EVENT.is_set():
            try:
                r=requests.get(
                    f"https://api.telegram.org/bot{self.token}/getUpdates",
                    params={
                        "timeout":20,
                        "offset":self.offset
                    },
                    timeout=30
                )
                data=r.json()
                if not data.get("ok"):
                    time.sleep(3)
                    continue

                for update in data.get("result",[]):
                    self.offset=update["update_id"]+1
                    msg=update.get("message",{})
                    text=msg.get("text","")
                    chat=msg.get("chat",{})
                    chat_id=str(chat.get("id",""))

                    if not text or not chat_id:
                        continue

                    if self.chat and chat_id!=self.chat:
                        self.send("⛔ Bu bot yalnız sahibinin Telegram ID-si üçün aktivdir.",chat_id)
                        continue

                    if text.startswith("/"):
                        self.handle(text,chat_id)

            except Exception as e:
                log.warning("Telegram polling: %s",e)
                STOP_EVENT.wait(3)

    def start_commands(self):
        if not self.token:
            return
        threading.Thread(
            target=self.poll,
            daemon=True,
            name="TelegramCommands"
        ).start()


TELEGRAM=TelegramManager()
POSITION_MANAGER.notify=TELEGRAM


class ScannerWorker:
    def __init__(self):
        self.running=False

    def start(self):
        if self.running:return
        self.running=True
        threading.Thread(
            target=self.loop,
            daemon=True,
            name="Scanner"
        ).start()

    def stop(self):
        self.running=False

    def symbols(self):
        try:
            x=BYBIT._get(
                "/v5/market/instruments-info",
                {"category":Config.CATEGORY,"limit":1000}
            )
            rows=x.get("result",{}).get("list",[]) if x else []
            z=[
                a["symbol"] for a in rows
                if a.get("symbol","").endswith("USDT")
                and a.get("status")=="Trading"
                and a.get("contractType")=="LinearPerpetual"
            ]
            return z or Config.FALLBACK_COINS
        except:
            return Config.FALLBACK_COINS

    def loop(self):
        while self.running and not STOP_EVENT.is_set():
            try:
                results=SCANNER.scan(self.symbols())
                sent=0
                for s in results:
                    if sent>=Config.MAX_SIGNALS_TO_SEND:
                        break
                    if TRACKER.add(s):
                        TELEGRAM.signal(s)
                        sent+=1
                log.info(
                    "SCAN candidates=%d new=%d",
                    len(results),sent
                )
            except Exception:
                log.exception("Scanner error")

            STOP_EVENT.wait(Config.CHECK_INTERVAL)


SCANNER_WORKER=ScannerWorker()


try:
    from flask import Flask,jsonify
    app=Flask(__name__)

    @app.route("/")
    def home():
        return jsonify({
            "bot":BOT_VERSION,
            "status":"running",
            "active":len(TRACKER.active())
        })

    @app.route("/health")
    def health():
        return jsonify({
            "status":"ok",
            "version":BOT_VERSION,
            "active":len(TRACKER.active())
        })

    @app.route("/stats")
    def stats():
        return jsonify(TRACKER.stats)

    def flask_start():
        app.run(
            host="0.0.0.0",
            port=Config.FLASK_PORT,
            debug=False,
            use_reloader=False
        )

except:
    app=None

    def flask_start():
        pass


def shutdown(sig=None,frame=None):
    if STOP_EVENT.is_set():
        return
    STOP_EVENT.set()
    SCANNER_WORKER.stop()
    POSITION_MANAGER.stop()
    TELEGRAM.running=False
    TRACKER.save()
    log.info("BOT STOPPED")


signal.signal(signal.SIGINT,shutdown)
signal.signal(signal.SIGTERM,shutdown)


def main():
    validate_config()

    log.info("="*40)
    log.info("SWING AI BOT %s",BOT_VERSION)
    log.info("4H + 1H + 15M | BOS + RETEST + CONFIRMATION")
    log.info("="*40)

    POSITION_MANAGER.start()
    SCANNER_WORKER.start()
    TELEGRAM.start_commands()

    if Config.BOT_TOKEN:
        TELEGRAM.send(
            f"🟢 SWING AI BOT {BOT_VERSION}\n"
            f"STARTED\n\n"
            f"Telegram komandaları aktivdir.\n"
            f"/help"
        )

    if app is not None:
        threading.Thread(
            target=flask_start,
            daemon=True
        ).start()

    while not STOP_EVENT.is_set():
        STOP_EVENT.wait(5)

    shutdown()


if __name__=="__main__":
    main()
