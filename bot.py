"""
BEE-M Trading Alert Bot — v7
Core truth: Liquidity taken → Market shifts → Enter on retracement

FLOW (locked):
  1. Liquidity sweep (recent, quality, wick+rejection)
  2. Displacement (strong impulsive move, large bodies)
  3. 15M BOS (close beyond structure, tied to sweep context)
  4. Retrace to OTE (Fib on actual BOS impulse leg)
  5. Entry — OTE only, or OTE + FVG (higher quality)

SETUP LIFECYCLE:
  created → valid → expired → invalidated

SCORING:
  Required: Sweep + Displacement + BOS + OTE
  Bonus:    FVG, equal highs/lows, immediate sweep, strong body, clean structure
  Rating:   A+ / A / B
"""

import os, time, logging, requests
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("BEEM")

TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
SCAN_INTERVAL    = int(os.environ.get("SCAN_INTERVAL",    "900"))
OTE_MONITOR_SECS = int(os.environ.get("OTE_MONITOR_SECS", "2700"))
TOP_N_PAIRS      = int(os.environ.get("TOP_N_PAIRS",       "30"))
SWEEP_LOOKBACK   = int(os.environ.get("SWEEP_LOOKBACK",    "30"))

MEXC_BASE = "https://contract.mexc.com/api/v1/contract"
alerted_today:   set  = set()
ote_watch_queue: list = []


# ═══════════════════════════════════════════════════════════════
# DATA
# ═══════════════════════════════════════════════════════════════

def get_all_tickers():
    try:
        r = requests.get(f"{MEXC_BASE}/ticker", timeout=15)
        r.raise_for_status()
        d = r.json()
        if not d.get("success"): return []
        return [t for t in d.get("data",[]) if str(t.get("symbol","")).endswith("_USDT")]
    except Exception as e:
        log.warning(f"get_all_tickers: {e}"); return []

def get_candles(symbol, interval, limit=120):
    try:
        r = requests.get(f"{MEXC_BASE}/kline/{symbol}",
            params={"interval":interval,"limit":limit}, timeout=10)
        r.raise_for_status()
        d = r.json()
        if not d.get("success"): return []
        raw  = d["data"]
        cols = [raw.get(k,[]) for k in ["time","open","high","low","close","vol"]]
        n    = len(cols[4])
        return [{"time":  cols[0][i] if i<len(cols[0]) else 0,
                 "open":  float(cols[1][i]) if i<len(cols[1]) else 0,
                 "high":  float(cols[2][i]) if i<len(cols[2]) else 0,
                 "low":   float(cols[3][i]) if i<len(cols[3]) else 0,
                 "close": float(cols[4][i]) if i<len(cols[4]) else 0,
                 "volume":float(cols[5][i]) if i<len(cols[5]) else 0}
                for i in range(n)]
    except Exception as e:
        log.warning(f"get_candles {symbol} {interval}: {e}"); return []

def get_price(symbol):
    try:
        r = requests.get(f"{MEXC_BASE}/ticker?symbol={symbol}", timeout=8)
        return float(r.json()["data"]["lastPrice"])
    except Exception as e:
        log.warning(f"get_price {symbol}: {e}"); return 0.0


# ═══════════════════════════════════════════════════════════════
# PAIR SCORING ENGINE
# ═══════════════════════════════════════════════════════════════

def score_pairs(tickers):
    if not tickers: return []
    volumes = []; changes = []
    for t in tickers:
        try:
            volumes.append(float(t.get("volume24",t.get("amount24",0))))
            changes.append(abs(float(t.get("priceChangeRate",t.get("riseFallRate",0))))*100)
        except: volumes.append(0); changes.append(0)
    med_vol    = sorted(volumes)[len(volumes)//2] if volumes else 1
    max_change = max(changes) if changes else 1
    scored = []
    for i,t in enumerate(tickers):
        sym = t.get("symbol","")
        if not sym: continue
        try:
            price  = float(t.get("lastPrice",0))
            h24    = float(t.get("high24Price",t.get("highPrice",price)))
            l24    = float(t.get("low24Price", t.get("lowPrice", price)))
            vol24  = volumes[i]
            chg    = float(t.get("priceChangeRate",t.get("riseFallRate",0)))*100
            if price==0 or l24==0: continue
            rng_pct = (h24-l24)/price*100
            vs  = min(rng_pct/15*25,25)
            vs2 = min((vol24/max(med_vol,1))/5*25,25)
            ms  = min(abs(chg)/max_change*25,25)
            total = vs+vs2+ms+15.0
            parts = []
            if vs>15:  parts.append(f"Volatile ({rng_pct:.1f}% range)")
            if vs2>15: parts.append("Strong volume")
            if ms>15:  parts.append(f"Momentum ({abs(chg):.1f}% move)")
            scored.append({"symbol":sym,"score":round(total,1),"change_pct":chg,
                           "volume_24h":vol24,"price":price,
                           "reason":" | ".join(parts) if parts else "Moderate"})
        except Exception as e: log.debug(f"score_pairs {sym}: {e}")
    scored.sort(key=lambda x:x["score"],reverse=True)
    return scored

def select_pairs(tickers, top_n=30):
    scored = score_pairs(tickers)
    if not scored: return []
    top_scored  = [s["symbol"] for s in scored[:10]]
    by_change   = sorted([s for s in scored if s["change_pct"]!=0],key=lambda x:x["change_pct"])
    top_losers  = [s["symbol"] for s in by_change[:5]]
    top_gainers = [s["symbol"] for s in by_change[-5:]]
    seen,pairs = set(),[]
    for sym in top_scored+top_gainers+top_losers:
        if sym not in seen: seen.add(sym); pairs.append(sym)
    log.info(f"Selected {len(pairs)} pairs | top3: {top_scored[:3]}")
    return pairs[:top_n]


# ═══════════════════════════════════════════════════════════════
# FVG HELPERS
# ═══════════════════════════════════════════════════════════════

def find_fvgs(candles):
    fvgs = []
    for i in range(len(candles)-2):
        c0,c2 = candles[i],candles[i+2]
        if c2["low"]  > c0["high"]: fvgs.append({"direction":"bullish","fvg_high":c2["low"], "fvg_low":c0["high"],"idx":i})
        elif c2["high"]< c0["low"]: fvgs.append({"direction":"bearish","fvg_high":c0["low"],"fvg_low":c2["high"],"idx":i})
    return fvgs

def unmitigated_daily_fvg(daily):
    if len(daily)<5: return None,None,None
    price = daily[-1]["close"]
    for fvg in reversed(find_fvgs(daily[:-1])):
        if fvg["direction"]=="bullish" and price<fvg["fvg_low"]:  return "bullish",fvg["fvg_high"],fvg["fvg_low"]
        if fvg["direction"]=="bearish" and price>fvg["fvg_high"]: return "bearish",fvg["fvg_high"],fvg["fvg_low"]
    return None,None,None

def fvg_inside_ote(candles, bias, ote_high, ote_low):
    recent = candles[-20:] if len(candles)>=20 else candles
    for fvg in reversed(find_fvgs(recent)):
        if fvg["direction"]==bias and fvg["fvg_low"]<ote_high and fvg["fvg_high"]>ote_low:
            return {"found":True,"fvg_high":fvg["fvg_high"],"fvg_low":fvg["fvg_low"]}
    return {"found":False}


# ═══════════════════════════════════════════════════════════════
# STEP 1 — DAILY BIAS (8 scenarios)
# ═══════════════════════════════════════════════════════════════

def daily_bias(daily):
    if len(daily)<3: return {"bias":"neutral","score":0,"reasons":[],"pdh":0,"pdl":0,"mid":0}
    prev,today,prev2 = daily[-2],daily[-1],daily[-3]
    pdh=prev["high"]; pdl=prev["low"]; mid=(pdh+pdl)/2
    score,reasons = 0,[]
    if prev["high"]>prev2["high"] and prev["close"]<prev2["high"]: score-=1; reasons.append("Sc1 Bear: Swept prior high, closed inside")
    if prev["low"] <prev2["low"]  and prev["close"]>prev2["low"]:  score+=1; reasons.append("Sc1 Bull: Swept prior low, closed inside")
    o=today["open"]
    if   o>pdh: score+=1; reasons.append("Sc2 Bull: Opened above PDH")
    elif o<pdl: score-=1; reasons.append("Sc2 Bear: Opened below PDL")
    else:                  reasons.append("Sc3 Neutral: Inside range")
    if o>pdh and today["low"] >pdh: score+=1; reasons.append("Sc4 Bull: Gap above PDH holding")
    if o<pdl and today["high"]<pdl: score-=1; reasons.append("Sc4 Bear: Gap below PDL holding")
    if   prev["close"]>pdh: score+=1; reasons.append("Sc5 Bull: Yesterday closed above PDH")
    elif prev["close"]<pdl: score-=1; reasons.append("Sc5 Bear: Yesterday closed below PDL")
    if prev["high"]>prev2["high"] and pdl<prev["close"]<prev2["high"]: score-=1; reasons.append("Sc5 Bear: Swept high, closed back inside")
    if prev["low"] <prev2["low"]  and prev2["low"]<prev["close"]<pdh:  score+=1; reasons.append("Sc5 Bull: Swept low, closed back inside")
    if   today["close"]>mid: score+=1; reasons.append(f"Sc6 Bull: Above midpoint ({mid:.4f})")
    else:                     score-=1; reasons.append(f"Sc6 Bear: Below midpoint ({mid:.4f})")
    if len(daily)>=6:
        r5=daily[-6:-1]; hs=[c["high"]for c in r5]; ls=[c["low"]for c in r5]
        if all(hs[i]<hs[i+1]for i in range(len(hs)-1)) and all(ls[i]<ls[i+1]for i in range(len(ls)-1)):
            score+=2; reasons.append("Sc7 Bull: Daily HH+HL")
        elif all(hs[i]>hs[i+1]for i in range(len(hs)-1)) and all(ls[i]>ls[i+1]for i in range(len(ls)-1)):
            score-=2; reasons.append("Sc7 Bear: Daily LH+LL")
    fd,fh,fl=unmitigated_daily_fvg(daily)
    if fd=="bullish": score+=1; reasons.append("Sc8 Bull: Daily FVG above draws price up")
    elif fd=="bearish":score-=1; reasons.append("Sc8 Bear: Daily FVG below draws price down")
    bias="bullish" if score>=2 else "bearish" if score<=-2 else "neutral"
    return {"bias":bias,"score":score,"reasons":reasons,"pdh":pdh,"pdl":pdl,"mid":mid}


# ═══════════════════════════════════════════════════════════════
# STEP 2 — 4H LOCATION via Fibonacci on actual swing structure
# ═══════════════════════════════════════════════════════════════

def find_4h_swing_points(h4):
    if len(h4)<10: return None,None,None,None
    sh_price=sh_idx=sl_price=sl_idx=None
    for i in range(len(h4)-3,1,-1):
        c=h4[i]
        if sh_price is None and i>=2 and i<=len(h4)-3 and \
           c["high"]>=h4[i-1]["high"] and c["high"]>=h4[i-2]["high"] and \
           c["high"]>=h4[i+1]["high"] and c["high"]>=h4[i+2]["high"]:
            sh_price=c["high"]; sh_idx=i
        if sl_price is None and i>=2 and i<=len(h4)-3 and \
           c["low"]<=h4[i-1]["low"] and c["low"]<=h4[i-2]["low"] and \
           c["low"]<=h4[i+1]["low"] and c["low"]<=h4[i+2]["low"]:
            sl_price=c["low"]; sl_idx=i
        if sh_price is not None and sl_price is not None: break
    return sh_price,sl_price,sh_idx,sl_idx

def location_4h(h4):
    sh,sl,sh_idx,sl_idx = find_4h_swing_points(h4)
    if sh is None or sl is None or sh==sl:
        r=h4[-20:]; sh=max(c["high"]for c in r); sl=min(c["low"]for c in r)
    rng=sh-sl; price=h4[-1]["close"]
    if rng==0: return {"location":"unknown","eq":price,"sh":sh,"sl":sl,"fib_618":price,"fib_705":price,"fib_792":price}
    bullish_leg=(sl_idx<sh_idx) if (sh_idx is not None and sl_idx is not None) else True
    eq=sl+0.500*rng; fib_236=sl+0.236*rng; fib_382=sl+0.382*rng
    fib_618=sl+0.618*rng; fib_705=sl+0.705*rng; fib_792=sl+0.792*rng
    loc="premium" if price>eq else "discount" if price<eq else "equilibrium"
    return {"location":loc,"eq":eq,"sh":sh,"sl":sl,"fib_236":fib_236,
            "fib_382":fib_382,"fib_618":fib_618,"fib_705":fib_705,
            "fib_792":fib_792,"bullish_leg":bullish_leg}


# ═══════════════════════════════════════════════════════════════
# STEP 3 — CLEAN STRUCTURE FILTER
# ═══════════════════════════════════════════════════════════════

def is_clean_structure(m15, lookback=20):
    if len(m15)<lookback: return {"clean":False,"reason":"Not enough candles"}
    window=m15[-lookback:]
    overlaps=0
    for i in range(1,len(window)):
        ph=max(window[i-1]["open"],window[i-1]["close"]); pl=min(window[i-1]["open"],window[i-1]["close"])
        ch=max(window[i]["open"],  window[i]["close"]);   cl=min(window[i]["open"],  window[i]["close"])
        if cl<ph and ch>pl: overlaps+=1
    ovr=overlaps/(len(window)-1)
    if ovr>0.65: return {"clean":False,"reason":f"Choppy — {ovr:.0%} overlap ratio"}
    wr=[]
    for c in window:
        r=c["high"]-c["low"]
        if r==0: continue
        wr.append(1-(abs(c["close"]-c["open"])/r))
    avg_wr=sum(wr)/len(wr) if wr else 0
    if avg_wr>0.6: return {"clean":False,"reason":f"Wick-heavy — {avg_wr:.0%} avg wick ratio"}
    up=sum(1 for c in window if c["close"]>c["open"])
    dn=sum(1 for c in window if c["close"]<c["open"])
    if max(up,dn)/len(window)<0.55: return {"clean":False,"reason":"No clear directional move"}
    return {"clean":True,"reason":f"Clean structure ({ovr:.0%} overlap, {avg_wr:.0%} wick ratio)"}


# ═══════════════════════════════════════════════════════════════
# STEP 4 — TRUE LIQUIDITY SWEEP
# Wick + close back inside = true sweep
# Two types: Immediate (A+) and Pre-swept (still valid)
# ═══════════════════════════════════════════════════════════════

def find_equal_levels(candles, bias, tolerance=0.002):
    prices=[c["high"]for c in candles] if bias=="bearish" else [c["low"]for c in candles]
    levels=[]
    for i in range(len(prices)):
        cluster=[prices[i]]
        for j in range(i+1,len(prices)):
            if abs(prices[j]-prices[i])/max(prices[i],0.0001)<tolerance: cluster.append(prices[j])
        if len(cluster)>=2: levels.append(sum(cluster)/len(cluster))
    return list(set(round(l,6) for l in levels))

def detect_true_sweep(m15, bias, pdh, pdl):
    if len(m15)<5: return {"swept":False,"reason":"Not enough candles"}
    window = m15[-SWEEP_LOOKBACK:] if len(m15)>=SWEEP_LOOKBACK else m15
    offset = len(m15)-len(window)
    eq_highs=find_equal_levels(window,"bearish")
    eq_lows =find_equal_levels(window,"bullish")
    best=None
    for i in range(len(window)-1):
        c=window[i]; age=len(window)-1-i; abs_idx=offset+i
        if bias=="bullish":
            if c["low"]<pdl and c["close"]>pdl:
                if (c["close"]-pdl)/max(pdl,0.0001)>0.015: continue
                reaction=c["close"]-c["low"]
                is_eq=any(abs(c["low"]-l)/max(l,0.0001)<0.002 for l in eq_lows)
                sw={"swept":True,"sweep_type":"immediate" if age<=5 else "pre_swept",
                    "sweep_level":c["low"],"candle_idx":abs_idx,"is_equal_hl":is_eq,
                    "reaction":reaction,"age_candles":age,"sweep_low":c["low"]}
                if best is None or age<best["age_candles"]: best=sw
        elif bias=="bearish":
            if c["high"]>pdh and c["close"]<pdh:
                if (pdh-c["close"])/max(pdh,0.0001)>0.015: continue
                reaction=c["high"]-c["close"]
                is_eq=any(abs(c["high"]-l)/max(l,0.0001)<0.002 for l in eq_highs)
                sw={"swept":True,"sweep_type":"immediate" if age<=5 else "pre_swept",
                    "sweep_level":c["high"],"candle_idx":abs_idx,"is_equal_hl":is_eq,
                    "reaction":reaction,"age_candles":age,"sweep_high":c["high"]}
                if best is None or age<best["age_candles"]: best=sw
    return best if best else {"swept":False,"reason":"No valid wick+rejection sweep"}


# ═══════════════════════════════════════════════════════════════
# STEP 5 — DISPLACEMENT
# ═══════════════════════════════════════════════════════════════

def detect_displacement(m15, bias, after_idx):
    prior=m15[max(0,after_idx-10):after_idx]
    if not prior: return {"valid":False,"avg_body":0,"reason":"No prior candles"}
    avg=sum(abs(c["close"]-c["open"])for c in prior)/len(prior)
    if avg==0: return {"valid":False,"avg_body":0,"reason":"Zero avg body"}
    disp_candles=m15[after_idx+1:after_idx+4]
    if not disp_candles: return {"valid":False,"avg_body":avg,"reason":"No candles after sweep"}
    for dc in disp_candles:
        body=abs(dc["close"]-dc["open"]); rng=dc["high"]-dc["low"]
        if rng==0: continue
        body_ratio=body/rng
        is_dir=(dc["close"]>dc["open"] if bias=="bullish" else dc["close"]<dc["open"])
        if body>=avg*1.8 and body_ratio>=0.5 and is_dir:
            try:
                idx=m15.index(dc); prev_c=m15[idx-1] if idx>0 else None
                next_c=m15[idx+1] if idx+1<len(m15) else None
                leaves_fvg=False
                if prev_c and next_c:
                    if bias=="bullish" and next_c["low"]>prev_c["high"]: leaves_fvg=True
                    if bias=="bearish" and next_c["high"]<prev_c["low"]: leaves_fvg=True
            except: leaves_fvg=False
            return {"valid":True,"avg_body":avg,"body_ratio":round(body_ratio,2),
                    "leaves_fvg":leaves_fvg,"reason":"Strong displacement confirmed"}
    return {"valid":False,"avg_body":avg,"reason":"Displacement too weak — slow or choppy"}


# ═══════════════════════════════════════════════════════════════
# STEP 6 — EXPANSION BOS tied to sweep context
# ═══════════════════════════════════════════════════════════════

def find_swing_points(candles, strength=2):
    swings=[]; n=len(candles)
    for i in range(strength,n-strength):
        c=candles[i]
        is_sh=all(c["high"]>=candles[i-j]["high"]for j in range(1,strength+1)) and \
              all(c["high"]>=candles[i+j]["high"]for j in range(1,strength+1))
        is_sl=all(c["low"] <=candles[i-j]["low"] for j in range(1,strength+1)) and \
              all(c["low"] <=candles[i+j]["low"]  for j in range(1,strength+1))
        if is_sh: swings.append({"type":"high","price":c["high"],"idx":i,"candle":c})
        if is_sl: swings.append({"type":"low", "price":c["low"], "idx":i,"candle":c})
    return swings

def find_relevant_structure(pre_sweep, bias):
    if len(pre_sweep)<6: return {"found":False}
    avg_body=sum(abs(c["close"]-c["open"])for c in pre_sweep)/len(pre_sweep)
    if avg_body==0: return {"found":False}
    swings=find_swing_points(pre_sweep,strength=2)
    ttype="high" if bias=="bullish" else "low"
    for sw in reversed(swings):
        if sw["type"]!=ttype: continue
        if abs(sw["candle"]["close"]-sw["candle"]["open"])>=avg_body*0.8:
            return {"found":True,"price":sw["price"],"idx":sw["idx"]}
    for sw in reversed(swings):
        if sw["type"]==ttype: return {"found":True,"price":sw["price"],"idx":sw["idx"]}
    return {"found":False}

def is_expansion_candle(candle, avg_body, bias, prev_candle):
    body=abs(candle["close"]-candle["open"]); rng=candle["high"]-candle["low"]
    if rng==0: return {"valid":False,"reason":"Zero range"}
    if body<avg_body*2.0: return {"valid":False,"reason":f"Body {body:.4f}<2x avg — inducement"}
    br=body/rng
    if br<0.55: return {"valid":False,"reason":f"Wick-heavy ({br:.0%}) — choppy break"}
    if bias=="bullish" and candle["close"]<=candle["open"]: return {"valid":False,"reason":"Bearish body"}
    if bias=="bearish" and candle["close"]>=candle["open"]: return {"valid":False,"reason":"Bullish body"}
    imb=False
    if prev_candle:
        if bias=="bullish" and candle["low"] >prev_candle["high"]: imb=True
        if bias=="bearish" and candle["high"]<prev_candle["low"]:  imb=True
    return {"valid":True,"leaves_imbalance":imb,"body_ratio":round(br,2),"reason":"Expansion BOS"}

def detect_bos(m15, bias, sweep_idx, avg_body, location):
    pre_sweep =m15[max(0,sweep_idx-20):sweep_idx]
    post_sweep=m15[sweep_idx+1:]
    if not pre_sweep or not post_sweep: return {"broken":False,"reason":"Not enough candles"}
    if location=="equilibrium": return {"broken":False,"reason":"BOS at equilibrium — low probability"}
    if bias=="bullish" and location=="premium":  return {"broken":False,"reason":"Bullish BOS in premium"}
    if bias=="bearish" and location=="discount": return {"broken":False,"reason":"Bearish BOS in discount"}
    struct=find_relevant_structure(pre_sweep,bias)
    if not struct["found"]: return {"broken":False,"reason":"No relevant pre-sweep swing structure"}
    level=struct["price"]
    for i,c in enumerate(post_sweep[-8:]):
        prev_c=post_sweep[i-1] if i>0 else pre_sweep[-1]
        if bias=="bullish":
            if c["close"]<=level: continue
            exp=is_expansion_candle(c,avg_body,bias,prev_c)
            if not exp["valid"]: return {"broken":False,"reason":f"Inducement BOS — {exp['reason']}"}
            return {"broken":True,"bos_level":level,"bos_origin":level,
                    "bos_type":"Broke last 15M swing HIGH (expansion)",
                    "leaves_imbalance":exp["leaves_imbalance"],"body_ratio":exp["body_ratio"]}
        elif bias=="bearish":
            if c["close"]>=level: continue
            exp=is_expansion_candle(c,avg_body,bias,prev_c)
            if not exp["valid"]: return {"broken":False,"reason":f"Inducement BOS — {exp['reason']}"}
            return {"broken":True,"bos_level":level,"bos_origin":level,
                    "bos_type":"Broke last 15M swing LOW (expansion)",
                    "leaves_imbalance":exp["leaves_imbalance"],"body_ratio":exp["body_ratio"]}
    return {"broken":False,"reason":"No candle closed beyond swing level"}


# ═══════════════════════════════════════════════════════════════
# STEP 7 — OTE on actual BOS impulse leg
# Sweep extreme → BOS break = the leg. Fib drawn on that leg.
# ═══════════════════════════════════════════════════════════════

def classify_move_quality(disp):
    """
    Strong move:  body_ratio >= 0.70 and leaves FVG
                  → expect SHALLOW pullback → entry near 0.382-0.50
    Moderate:     body_ratio 0.55-0.70
                  → standard OTE 0.618-0.705
    Weak/choppy:  body_ratio < 0.55 or no FVG
                  → expect DEEP retracement → wait for full OTE 0.705-0.79
    Returns: "strong" | "moderate" | "weak"
    """
    ratio = disp.get("body_ratio", 0)
    fvg   = disp.get("leaves_fvg", False)
    if ratio >= 0.70 and fvg:  return "strong"
    if ratio >= 0.55:           return "moderate"
    return "weak"


def calc_ote(m15, bias, sweep_idx, bos_level, move_quality="moderate"):
    """
    Fibonacci on the actual BOS impulse leg.
    Entry depth depends on move quality:
      strong   → shallow zone: 0.382 – 0.50  (market won't retrace deep)
      moderate → standard OTE: 0.618 – 0.705
      weak     → deep OTE:     0.705 – 0.79  (expect full pullback)
    """
    leg = m15[sweep_idx:sweep_idx+15]
    if not leg: return {}

    # Entry zones by move quality
    zones = {
        "strong":   (0.382, 0.500, 0.440),   # (high_ret, low_ret, ideal)
        "moderate": (0.618, 0.705, 0.660),
        "weak":     (0.705, 0.790, 0.750),
    }
    high_ret, low_ret, ideal_ret = zones.get(move_quality, zones["moderate"])

    if bias == "bullish":
        leg_low  = min(c["low"]  for c in leg)
        leg_high = bos_level
        rng = leg_high - leg_low
        if rng <= 0: return {}
        return {
            "ote_high":    leg_high - high_ret * rng,
            "ote_low":     leg_high - low_ret  * rng,
            "ideal":       leg_high - ideal_ret * rng,
            "leg_high":    leg_high, "leg_low": leg_low,
            "move_quality": move_quality,
        }
    elif bias == "bearish":
        leg_high = max(c["high"] for c in leg)
        leg_low  = bos_level
        rng = leg_high - leg_low
        if rng <= 0: return {}
        return {
            "ote_low":     leg_low + high_ret * rng,
            "ote_high":    leg_low + low_ret  * rng,
            "ideal":       leg_low + ideal_ret * rng,
            "leg_high":    leg_high, "leg_low": leg_low,
            "move_quality": move_quality,
        }
    return {}

# ═══════════════════════════════════════════════════════════════
# STEP 8 — SETUP SCORING  A+ / A / B
# ═══════════════════════════════════════════════════════════════

def score_setup(sweep, disp, bos, fvg_ote, structure):
    bonus=0; reasons=[]
    if fvg_ote and fvg_ote.get("found"):               bonus+=1; reasons.append("FVG inside OTE")
    if sweep.get("is_equal_hl"):                        bonus+=1; reasons.append("Equal highs/lows swept")
    if sweep.get("sweep_type")=="immediate":            bonus+=1; reasons.append("Immediate sweep → BOS")
    if disp.get("body_ratio",0)>=0.70:                  bonus+=1; reasons.append("Strong displacement (body≥70%)")
    if structure.get("clean"):                          bonus+=1; reasons.append("Clean structure")
    rating="A+" if bonus>=4 else "A" if bonus>=2 else "B"
    return {"rating":rating,"bonus":bonus,"reasons":[r+" ⭐" for r in reasons]}

# ═══════════════════════════════════════════════════════════════
# STEP 9 — RISK LEVELS
# ═══════════════════════════════════════════════════════════════

def find_liquidity_targets(m15, h4, daily, bias):
    """
    Find actual liquidity levels to use as TP targets.
    Priority order:
      1. PDH/PDL from daily bias data
      2. Equal highs/lows on 4H (clustered stop levels)
      3. Recent 4H swing highs/lows (external liquidity)
    Returns list of levels sorted by distance from current price.
    """
    targets = []
    price   = m15[-1]["close"] if m15 else 0
    if price == 0: return targets

    # PDH/PDL — always first targets
    if daily:
        pdh = daily[-2]["high"] if len(daily) >= 2 else 0
        pdl = daily[-2]["low"]  if len(daily) >= 2 else 0
        if bias == "bullish" and pdh > price:  targets.append(("PDH", pdh))
        if bias == "bearish" and pdl < price:  targets.append(("PDL", pdl))

    # Equal highs/lows on 4H — institutional stop clusters
    if h4:
        eq_levels = find_equal_levels(h4[-30:], "bearish" if bias=="bullish" else "bullish")
        for lvl in eq_levels:
            if bias == "bullish" and lvl > price:
                targets.append((f"Equal highs ({lvl:.4f})", lvl))
            if bias == "bearish" and lvl < price:
                targets.append((f"Equal lows ({lvl:.4f})",  lvl))

    # 4H swing extremes (external liquidity)
    if h4:
        recent_4h = h4[-20:]
        if bias == "bullish":
            sh = max(c["high"] for c in recent_4h)
            if sh > price: targets.append((f"4H swing high ({sh:.4f})", sh))
        if bias == "bearish":
            sl = min(c["low"]  for c in recent_4h)
            if sl < price: targets.append((f"4H swing low ({sl:.4f})",  sl))

    # Sort by distance from price — nearest first
    if bias == "bullish":
        targets.sort(key=lambda x: x[1])
    else:
        targets.sort(key=lambda x: x[1], reverse=True)

    return targets


def calc_risk(bias, sweep, bos, ote, m15=None, h4=None, daily=None):
    """
    STRUCTURAL SL — placed at actual structure, not a percentage buffer.
    For longs:  SL below the swing low that was swept (structural invalidation)
    For shorts: SL above the swing high that was swept

    LIQUIDITY TP — targets are actual liquidity levels:
      TP1 (50%): nearest liquidity level
      TP2 (30%): next liquidity level
      Runner (20%): furthest / external liquidity

    Falls back to RR multiples if no liquidity levels found.
    """
    if not ote: return {}
    entry = ote.get("ideal", 0)
    if not entry: return {}

    # ── Structural SL ─────────────────────────────────────────
    # SL goes BEYOND the sweep extreme — below the swept low (longs)
    # or above the swept high (shorts). Add a small buffer (0.05%)
    # so SL is just beyond the wick, not at it.
    if bias == "bullish":
        sl_sweep  = sweep.get("sweep_low",  0)
        sl_bos    = bos.get("bos_origin",   0)
        # Use whichever is lower — further from entry = safer structural level
        candidates = [l for l in [sl_sweep, sl_bos] if l > 0]
        sl = min(candidates) * 0.9995 if candidates else entry * 0.98
        rr = entry - sl
        if rr <= 0: return {}

    elif bias == "bearish":
        sl_sweep  = sweep.get("sweep_high", 0)
        sl_bos    = bos.get("bos_origin",   0)
        candidates = [l for l in [sl_sweep, sl_bos] if l > 0]
        sl = max(candidates) * 1.0005 if candidates else entry * 1.02
        rr = sl - entry
        if rr <= 0: return {}
    else:
        return {}

    # ── Liquidity-based TPs ───────────────────────────────────
    liq_targets = find_liquidity_targets(m15 or [], h4 or [], daily or [], bias)

    if len(liq_targets) >= 3:
        tp1    = liq_targets[0][1]; tp1_label = liq_targets[0][0]
        tp2    = liq_targets[1][1]; tp2_label = liq_targets[1][0]
        runner = liq_targets[2][1]; runner_label = liq_targets[2][0]
    elif len(liq_targets) == 2:
        tp1    = liq_targets[0][1]; tp1_label = liq_targets[0][0]
        tp2    = liq_targets[1][1]; tp2_label = liq_targets[1][0]
        runner = entry + rr*5 if bias=="bullish" else entry - rr*5
        runner_label = "External (5R)"
    elif len(liq_targets) == 1:
        tp1    = liq_targets[0][1]; tp1_label = liq_targets[0][0]
        tp2    = entry + rr*3 if bias=="bullish" else entry - rr*3
        tp2_label = "3R target"
        runner = entry + rr*5 if bias=="bullish" else entry - rr*5
        runner_label = "External (5R)"
    else:
        # Fallback — no liquidity levels found, use RR multiples
        tp1 = entry + rr*2 if bias=="bullish" else entry - rr*2; tp1_label="2R"
        tp2 = entry + rr*3 if bias=="bullish" else entry - rr*3; tp2_label="3R"
        runner = entry + rr*5 if bias=="bullish" else entry - rr*5; runner_label="5R"

    rr_ratio = round(abs(tp1 - entry) / rr, 2) if rr > 0 else 2.0

    return {
        "entry":        entry,
        "sl":           sl,
        "tp1":          tp1,   "tp1_label":    tp1_label,
        "tp2":          tp2,   "tp2_label":    tp2_label,
        "runner":       runner,"runner_label":  runner_label,
        "rr_ratio":     rr_ratio,
        "liq_targets":  liq_targets,
    }


# ═══════════════════════════════════════════════════════════════
# SETUP LIFECYCLE MONITOR
# States: valid → expired / invalidated / confirmed / skip
# ═══════════════════════════════════════════════════════════════

def monitor_ote_queue():
    now=time.time(); keep=[]
    for item in ote_watch_queue:
        sym=item["symbol"]; bias=item["bias"]
        if now>item["expires_at"]:
            send_telegram(f"<i>Setup expired — {sym.replace('_','/')} {bias.upper()}\n"
                f"Price never retraced to OTE {item['ote_low']:.4f}–{item['ote_high']:.4f}\n"
                f"Structure reset. Waiting for next setup.</i>")
            log.info(f"{sym}: setup expired"); continue
        price=get_price(sym)
        if price==0: keep.append(item); continue
        if bias=="bullish" and price<item.get("sweep_level",0)*0.999:
            send_telegram(f"<b>INVALIDATED — {sym.replace('_','/')} {bias.upper()}</b>\n"
                f"Broke below sweep extreme {item.get('sweep_level',0):.4f}\nNew structure forming. Wait for reset.")
            log.info(f"{sym}: invalidated — broke sweep extreme"); continue
        if bias=="bearish" and price>item.get("sweep_level",0)*1.001:
            send_telegram(f"<b>INVALIDATED — {sym.replace('_','/')} {bias.upper()}</b>\n"
                f"Broke above sweep extreme {item.get('sweep_level',0):.4f}\nNew structure forming. Wait for reset.")
            log.info(f"{sym}: invalidated — broke sweep extreme"); continue
        in_zone=item["ote_low"]<=price<=item["ote_high"]
        if not in_zone: keep.append(item); continue
        item["checks_in_zone"]=item.get("checks_in_zone",0)+1
        prev=item.get("last_price",price); thr=0.001
        reacted=(bias=="bullish" and price>prev*(1+thr)) or (bias=="bearish" and price<prev*(1-thr))
        item["last_price"]=price
        emoji="🟢" if bias=="bullish" else "🔴"
        if reacted:
            send_telegram(f"{emoji} <b>ENTRY CONFIRMED — {sym.replace('_','/')} {bias.upper()}</b>\n\n"
                f"Reaction in OTE at <b>{price:.4f}</b>\n\n"
                f"🎯 Entry: <b>{item['entry']:.4f}</b>\n🛑 SL: <b>{item['sl']:.4f}</b>\n"
                f"✅ TP1 (50%): <b>{item['tp1']:.4f}</b>\n✅ TP2 (30%): <b>{item['tp2']:.4f}</b>\n"
                f"🚀 Runner (20%): <b>{item['runner']:.4f}</b>\n\n<b>Execute now. Risk 1-2% only.</b>")
            log.info(f"{sym}: OTE reaction confirmed"); continue
        if item["checks_in_zone"]>=3:
            send_telegram(f"<b>SKIP — {sym.replace('_','/')} {bias.upper()}</b>\n"
                f"3 checks in OTE at {price:.4f} — no reaction.\nDo not chase.")
            log.info(f"{sym}: SKIP — 3 checks no reaction"); continue
        keep.append(item)
    ote_watch_queue.clear(); ote_watch_queue.extend(keep)


# ═══════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════

def send_telegram(msg):
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id":TELEGRAM_CHAT_ID,"text":msg,"parse_mode":"HTML"},timeout=10)
    except Exception as e: log.warning(f"Telegram: {e}")

def build_alert(sym, bd, loc_data, sweep, disp, bos, fvg_ote, ote, risk, setup_score, pair_score):
    bias=bd["bias"]; direction=bias.upper()
    emoji="🟢" if direction=="BULLISH" else "🔴"
    rating=setup_score["rating"]
    re={"A+":"🏆","A":"⭐","B":"✅"}.get(rating,"✅")
    now=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    reasons="\n".join(f"  • {r}" for r in bd["reasons"][:4])
    loc=loc_data.get("location","unknown")
    age=sweep.get("age_candles",0); stype=sweep.get("sweep_type","unknown")
    eq_lbl=" | Equal H/L ⭐" if sweep.get("is_equal_hl") else ""
    bonus_txt="\n".join(f"  {r}" for r in setup_score["reasons"]) or "  None"
    fvg_line=(f"FVG in OTE: ✅ <b>{fvg_ote['fvg_low']:.4f}–{fvg_ote['fvg_high']:.4f}</b>"
              if fvg_ote and fvg_ote.get("found") else "FVG in OTE: ⚠️ None — wait for reaction")
    fib_line=(f"Range: <b>{loc_data.get('sl',0):.4f}→{loc_data.get('sh',0):.4f}</b>  "
              f"EQ: <b>{loc_data.get('eq',0):.4f}</b>  "
              f"OTE: <b>{loc_data.get('fib_618',0):.4f}–{loc_data.get('fib_792',0):.4f}</b>")
    score_line=(f"Pair: <b>{pair_score.get('score',0):.0f}/100</b> | "
                f"24h: <b>{pair_score.get('change_pct',0):+.2f}%</b> | {pair_score.get('reason','')}")
    return f"""{emoji} <b>BEE-M SETUP — Rating: {rating} {re}</b> {emoji}
<b>Pair:</b> {sym.replace('_','/')}  |  <b>Direction:</b> {direction}
<b>Time:</b> {now}
{score_line}

<b>DAILY BIAS</b>  Score: {bd['score']:+d} | PDH: {bd['pdh']:.4f} | PDL: {bd['pdl']:.4f}
{reasons}

<b>4H LOCATION (Fib on swing)</b>  {loc.upper()} ✅
{fib_line}

<b>SWEEP</b>  {stype.replace('_',' ').title()} ({age} candles ago){eq_lbl}
Rejection: <b>{sweep.get('reaction',0):.4f}</b>

<b>DISPLACEMENT</b>  Body: <b>{disp.get('body_ratio',0):.0%}</b> | FVG left: <b>{'Yes' if disp.get('leaves_fvg') else 'No'}</b>
Move quality: <b>{ote.get('move_quality','moderate').upper()}</b> → {'Shallow entry (0.382–0.50)' if ote.get('move_quality')=='strong' else 'Deep entry (0.705–0.79)' if ote.get('move_quality')=='weak' else 'Standard OTE (0.618–0.705)'}

<b>BOS</b>  <b>{bos.get('bos_type','Confirmed')}</b> ✅
Imbalance: <b>{'Yes' if bos.get('leaves_imbalance') else 'No'}</b> | Body: <b>{bos.get('body_ratio',0):.0%}</b>

<b>OTE ENTRY ZONE (Fib on impulse leg)</b>
Leg: <b>{ote.get('leg_low',0):.4f} → {ote.get('leg_high',0):.4f}</b>
Zone: <b>{ote.get('ote_low',0):.4f} – {ote.get('ote_high',0):.4f}</b>
Ideal (0.705): <b>{ote.get('ideal',0):.4f}</b>
{fvg_line}

<b>RISK</b>
🎯 Entry: <b>{risk.get('entry',0):.4f}</b>
🛑 SL: <b>{risk.get('sl',0):.4f}</b>  (structural — beyond swept level)
✅ TP1 (50%): <b>{risk.get('tp1',0):.4f}</b>  [{risk.get('tp1_label','—')}]  (~1:{risk.get('rr_ratio',2):.1f})
✅ TP2 (30%): <b>{risk.get('tp2',0):.4f}</b>  [{risk.get('tp2_label','—')}]
🚀 Runner (20%): <b>{risk.get('runner',0):.4f}</b>  [{risk.get('runner_label','—')}]

<b>BONUS QUALITY</b>
{bonus_txt}

✔ Sweep (wick+rejection) ✔ Displacement ✔ Expansion BOS ✔ OTE
⏳ <i>Monitoring OTE. CONFIRMED or SKIP next.</i>
<b>You are trading: liquidity taken → shift → retracement.</b>""".strip()


# ═══════════════════════════════════════════════════════════════
# MAIN SCAN
# ═══════════════════════════════════════════════════════════════

def scan_pair(sym, pair_score):
    log.info(f"Scanning {sym} (score {pair_score.get('score',0):.0f})...")
    daily=get_candles(sym,"Day1",30); h4=get_candles(sym,"Hour4",50); m15=get_candles(sym,"Min15",120)
    if not daily or not h4 or not m15: log.warning(f"{sym}: missing data"); return

    bd=daily_bias(daily); bias=bd["bias"]
    if bias=="neutral": log.info(f"{sym}: neutral — NO TRADE"); return

    loc_data=location_4h(h4); loc=loc_data["location"]
    if (bias=="bullish" and loc=="premium") or (bias=="bearish" and loc=="discount"):
        log.info(f"{sym}: misaligned ({loc}) — NO TRADE"); return
    if loc=="unknown": log.info(f"{sym}: location unknown — NO TRADE"); return

    structure=is_clean_structure(m15)
    if not structure["clean"]: log.info(f"{sym}: {structure['reason']} — NO TRADE"); return

    pdh=bd["pdh"]; pdl=bd["pdl"]
    sweep=detect_true_sweep(m15,bias,pdh,pdl)
    if not sweep["swept"]: log.info(f"{sym}: {sweep.get('reason','no sweep')} — NO TRADE"); return

    si=sweep["candle_idx"]
    disp=detect_displacement(m15,bias,si)
    if not disp["valid"]: log.info(f"{sym}: {disp['reason']} — NO TRADE"); return

    bos=detect_bos(m15,bias,si,disp["avg_body"],loc)
    if not bos["broken"]: log.info(f"{sym}: {bos.get('reason','no BOS')} — NO TRADE"); return

    # Classify move quality → determines entry depth
    move_quality = classify_move_quality(disp)
    log.info(f"{sym}: move quality = {move_quality} (body ratio {disp.get('body_ratio',0):.0%})")

    ote=calc_ote(m15,bias,si,bos["bos_level"],move_quality)
    if not ote: log.info(f"{sym}: OTE failed — NO TRADE"); return

    fvg_ote=fvg_inside_ote(m15,bias,ote["ote_high"],ote["ote_low"])
    setup_score=score_setup(sweep,disp,bos,fvg_ote,structure)
    # Pass candle data so risk can find real liquidity targets
    risk=calc_risk(bias,sweep,bos,ote,m15=m15,h4=h4,daily=daily)
    if not risk: log.info(f"{sym}: risk calc failed — NO TRADE"); return

    today_str=datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key=f"{sym}_{bias}_{today_str}"
    if key in alerted_today: log.info(f"{sym}: already alerted today"); return
    alerted_today.add(key)

    log.info(f"{sym}: ✅ {setup_score['rating']} SETUP {bias.upper()}")
    msg=build_alert(sym,bd,loc_data,sweep,disp,bos,fvg_ote,ote,risk,setup_score,pair_score)
    send_telegram(msg)

    ote_watch_queue.append({
        "symbol":sym,"bias":bias,"ote_high":ote["ote_high"],"ote_low":ote["ote_low"],
        "entry":risk["entry"],"sl":risk["sl"],"tp1":risk["tp1"],"tp2":risk["tp2"],
        "runner":risk["runner"],"sweep_level":sweep.get("sweep_low",sweep.get("sweep_high",0)),
        "expires_at":time.time()+OTE_MONITOR_SECS,"checks_in_zone":0,"last_price":0.0,
        "rating":setup_score["rating"],
    })

def scan_all():
    log.info("=== Scan cycle ===")
    today_str=datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if not hasattr(scan_all,"_day") or scan_all._day!=today_str:
        alerted_today.clear(); scan_all._day=today_str; log.info("New day — reset")
    tickers=get_all_tickers()
    if not tickers: log.warning("No tickers"); return
    log.info(f"Total USDT pairs: {len(tickers)}")
    scored_map={s["symbol"]:s for s in score_pairs(tickers)}
    pairs=select_pairs(tickers,TOP_N_PAIRS)
    if not pairs: return
    for sym in pairs:
        try: scan_pair(sym,scored_map.get(sym,{"score":0,"change_pct":0,"reason":""})); time.sleep(1.2)
        except Exception as e: log.error(f"{sym}: {e}")
    if ote_watch_queue:
        log.info(f"OTE monitor: {len(ote_watch_queue)} active"); monitor_ote_queue()
    log.info("=== Done ===")


# ═══════════════════════════════════════════════════════════════
# KILL ZONES (WAT = UTC+1)
# ═══════════════════════════════════════════════════════════════

KILL_ZONES = [
    (2,  0,  5,  0, "London open"),       # 03:00-06:00 WAT
    (7,  0, 10,  0, "New York open"),     # 08:00-11:00 WAT
    (7,  0,  7, 30, "MEXC 8AM spike"),   # 08:00-08:30 WAT
    (13,30, 14, 30, "MEXC afternoon"),   # 14:30-15:30 WAT
    (16,  0, 17,  0, "MEXC daily reset"),# 17:00-18:00 WAT
]

def current_kill_zone():
    now=datetime.now(timezone.utc); m=now.hour*60+now.minute
    for (sh,sm,eh,em,name) in KILL_ZONES:
        if sh*60+sm<=m<eh*60+em: return name
    return ""

def minutes_to_next_kill_zone():
    now=datetime.now(timezone.utc); m=now.hour*60+now.minute
    best_d,best_n=9999,""
    for (sh,sm,eh,em,name) in KILL_ZONES:
        d=sh*60+sm-m
        if d<0: d+=1440
        if d<best_d: best_d=d; best_n=name
    return best_d,best_n

# ═══════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════

if __name__=="__main__":
    log.info("BEE-M Alert Bot v7 starting...")
    send_telegram(
        "🤖 <b>BEE-M Alert Bot v7 — Full Rebuild</b>\n\n"
        "<b>Core truth:</b> Liquidity taken → Market shifts → Enter on retracement\n\n"
        "<b>What changed in v7:</b>\n"
        "✔ TRUE sweep = wick + close back inside (not just a breakout)\n"
        "✔ Pre-swept setups valid (PDH/PDL taken earlier, BOS forming now)\n"
        "✔ Equal highs/lows detection (institutional stop clusters)\n"
        "✔ Clean structure filter (rejects choppy/overlapping markets)\n"
        "✔ OTE on actual impulse leg (sweep extreme → BOS break)\n"
        "✔ BOS tied to sweep context (not raw breakout buffer)\n"
        "✔ Setup scoring: A+ / A / B rating\n"
        "✔ Setup lifecycle: valid → expired → invalidated\n"
        "✔ Expiry + invalidation alerts sent automatically\n"
        "✔ Kill zones (WAT): London 03-06, NY 08-11, MEXC 08, 14:30, 17:00\n"
        "✔ All MEXC pairs scored each cycle\n\n"
        "<i>You are not trading OTE or FVG.\n"
        "You are trading: liquidity taken → shift → retracement.</i>"
    )
    last_zone=""
    while True:
        try:
            zone=current_kill_zone()
            if zone and zone!=last_zone:
                log.info(f"Kill zone: {zone}")
                send_telegram(f"🔔 <b>Kill zone — {zone}</b>\nScanning now.\n"
                    f"<i>{datetime.now(timezone.utc).strftime('%H:%M UTC')}</i>")
                last_zone=zone
            if not zone and last_zone:
                d,n=minutes_to_next_kill_zone()
                send_telegram(f"🔕 <b>{last_zone} closed</b>\nOTE monitor active.\nNext: <b>{n}</b> in {d//60}h {d%60}m")
                last_zone=""
            if zone:
                scan_all(); time.sleep(SCAN_INTERVAL)
            else:
                if ote_watch_queue: monitor_ote_queue()
                else:
                    d,n=minutes_to_next_kill_zone()
                    log.info(f"Outside kill zone. Next: {n} in {d//60}h {d%60}m")
                time.sleep(60)
        except Exception as e:
            log.error(f"Loop: {e}"); send_telegram(f"Bot error: {e}"); time.sleep(60)
