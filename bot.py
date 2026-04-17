# =========================
# BEE-M v25.1 FULL VISIBILITY (INSTRUMENTED)
# =========================
"""
Adds:
- Detailed scoring breakdown
- Explicit model tagging (Model 1–4)
- Market phase detection (trend/range/expansion)
- Verbose logs (why taken / skipped)
- Pair scoring + ranking
- CSV export for closed trades
- Fees + simple slippage model
"""

import os, time, logging, requests, csv
from datetime import datetime, timezone

# =========================
# CONFIG
# =========================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("BEEM_V25_1")

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN","")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID","")

MEXC = "https://contract.mexc.com/api/v1/contract"
SCAN_INTERVAL = 900
TOP_N = 60

ACCOUNT_BALANCE = 5.0
RISK_PER_TRADE = 0.05
FEE_RATE = 0.0006   # 0.06% per side
SLIPPAGE = 0.0005   # 0.05%

CSV_PATH = "/mnt/data/trades_v25_1.csv"

# =========================
# STATES
# =========================
market_state = {}
scanner_state = {}
liq_state = {}
states = {}

open_trades = {}
closed_trades = []

# =========================
# TELEGRAM
# =========================
def send(msg):
    if not TELEGRAM_TOKEN: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=10)
    except: pass

# =========================
# DATA
# =========================
def get_tickers():
    try:
        data = requests.get(f"{MEXC}/ticker").json().get("data",[])
        for t in data:
            sym = t["symbol"]
            market_state[sym] = {
                "price": float(t.get("lastPrice",0)),
                "high": float(t.get("high24Price",0)),
                "low": float(t.get("lower24Price",0)),
                "volume": float(t.get("volume24",0)),
                "open_interest": float(t.get("holdVol",0))
            }
        return data
    except:
        return []

def get_candles(sym, tf="Min15", limit=150):
    try:
        r = requests.get(f"{MEXC}/kline/{sym}", params={"interval":tf,"limit":limit})
        d = r.json()["data"]
        return [{"open":float(d["open"][i]),"high":float(d["high"][i]),
                 "low":float(d["low"][i]),"close":float(d["close"][i])}
                for i in range(len(d["close"]))][:-1]
    except:
        return []

# =========================
# SCANNER (IMBALANCE)
# =========================
def scan_imbalances(m15):
    res = {"fvg":False,"liquidity_void":False,"displacement":False,
           "momentum_shift":None,"score":0,"is_hot":False,"direction":None}

    if len(m15)<20: return res

    for i in range(2,len(m15)):
        if m15[i]["low"] > m15[i-2]["high"]:
            res["fvg"]=True; res["direction"]="bullish"
        elif m15[i]["high"] < m15[i-2]["low"]:
            res["fvg"]=True; res["direction"]="bearish"

    avg = sum(c["high"]-c["low"] for c in m15[-20:])/20
    last = m15[-1]
    if (last["high"]-last["low"]) > avg*2:
        res["displacement"]=True

    last3 = m15[-3:]
    bull = sum(1 for c in last3 if c["close"]>c["open"])
    bear = sum(1 for c in last3 if c["close"]<c["open"])
    if bull==3: res["momentum_shift"]="bullish"
    elif bear==3: res["momentum_shift"]="bearish"

    if res["displacement"] and res["fvg"]:
        res["liquidity_void"]=True

    if res["fvg"]: res["score"]+=2
    if res["displacement"]: res["score"]+=2
    if res["momentum_shift"]: res["score"]+=2

    if res["score"]>=4:
        res["is_hot"]=True

    return res

# =========================
# LIQUIDITY
# =========================
def compute_liq(m15):
    highs = [c["high"] for c in m15[-50:]]
    lows  = [c["low"] for c in m15[-50:]]
    pdh, pdl = max(highs[:-1]), min(lows[:-1])
    last = m15[-1]

    if last["high"]>pdh and last["close"]<pdh:
        return {"bias":"bearish","confidence":"HIGH"}
    if last["low"]<pdl and last["close"]>pdl:
        return {"bias":"bullish","confidence":"HIGH"}

    return {"bias":None,"confidence":"LOW"}

# =========================
# MARKET PHASE
# =========================
def detect_phase(m15):
    if len(m15)<20: return "unknown"
    ranges = [c["high"]-c["low"] for c in m15[-20:]]
    avg = sum(ranges)/20
    last = m15[-1]["high"]-m15[-1]["low"]

    if last > avg*1.8:
        return "expansion"
    if avg < (sum(ranges[-5:])/5)*1.2:
        return "range"
    return "trend"

# =========================
# BIAS
# =========================
def get_bias(h4, m15):
    high = max(c["high"] for c in h4[-50:])
    low  = min(c["low"] for c in h4[-50:])
    mid  = (high+low)/2
    price = h4[-1]["close"]

    htf = "bearish" if price>mid else "bullish"

    last3 = m15[-3:]
    if all(c["close"]>c["open"] for c in last3):
        return "bullish"
    if all(c["close"]<c["open"] for c in last3):
        return "bearish"

    return htf

# =========================
# BOS
# =========================
def detect_bos(m15, strength):
    last, prev = m15[-1], m15[-2]
    body = abs(last["close"]-last["open"])
    rng = last["high"]-last["low"] or 1e-6
    return (last["close"]>prev["high"] or last["close"]<prev["low"]) and (body/rng>=strength)

# =========================
# PAIR SCORING
# =========================
def compute_pair_score(sym):
    data = market_state.get(sym, {})
    scan = scanner_state.get(sym, {})

    if not data: return 0

    price, high, low = data["price"], data["high"], data["low"]
    vol, oi = data["volume"], data["open_interest"]

    change_pct = ((price-low)/(high-low+1e-6))*100
    volatility = (high-low)/(price+1e-6)

    imb_bonus = scan.get("score",0)
    liq_bonus = 5 if liq_state.get(sym,{}).get("confidence")=="HIGH" else 0

    return change_pct*1.5 + volatility*100 + vol*1e-6 + oi*1e-4 + imb_bonus + liq_bonus

# =========================
# POSITION SIZE
# =========================
def pos_size(entry, sl):
    risk_amt = ACCOUNT_BALANCE*RISK_PER_TRADE
    rpu = abs(entry-sl) or 1e-6
    return risk_amt/rpu

# =========================
# CSV LOG
# =========================
def log_csv(trade):
    file_exists = os.path.isfile(CSV_PATH)
    with open(CSV_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=trade.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(trade)

# =========================
# TRADE MGMT
# =========================
def update_trade(sym, price):
    global ACCOUNT_BALANCE
    if sym not in open_trades: return

    t = open_trades[sym]
    hit_tp = (price>=t["tp"] if t["bias"]=="bullish" else price<=t["tp"])
    hit_sl = (price<=t["sl"] if t["bias"]=="bullish" else price>=t["sl"])

    if hit_tp or hit_sl:
        pnl = t["risk"]*t["rr"] if hit_tp else -t["risk"]
        fee = (t["entry"]*t["size"] + price*t["size"]) * FEE_RATE
        pnl -= fee
        ACCOUNT_BALANCE += pnl

        res = "TP" if hit_tp else "SL"
        trade = {
            "pair": sym, "result": res, "pnl": pnl,
            "balance": ACCOUNT_BALANCE,
            "time": datetime.now(timezone.utc).isoformat()
        }
        log_csv(trade)
        closed_trades.append(res)
        del open_trades[sym]

        winrate = (closed_trades.count("TP")/len(closed_trades))*100
        send(f"📊 CLOSED {sym}\n{res}\nPnL:{pnl:.2f}$\nBal:{ACCOUNT_BALANCE:.2f}$\nWin:{winrate:.1f}%")

# =========================
# PROCESS
# =========================
def process(sym):
    m15 = get_candles(sym)
    h4  = get_candles(sym,"Min240")
    if len(m15)<50 or len(h4)<50: return

    price = m15[-1]["close"]
    update_trade(sym, price)
    if sym in open_trades: return

    scan = scan_imbalances(m15)
    scanner_state[sym]=scan

    liq = compute_liq(m15)
    liq_state[sym]=liq

    score = 0
    reasons = []

    if scan["fvg"]: score+=2; reasons.append("FVG")
    if scan["displacement"]: score+=2; reasons.append("Displacement")
    if scan["momentum_shift"]: score+=1; reasons.append("Momentum")
    if liq["confidence"]=="HIGH": score+=2; reasons.append("Liquidity")

    if score < 4:
        log.info(f"{sym} SKIP score={score} {reasons}")
        return

    bias = get_bias(h4,m15)
    if scan["direction"] and scan["direction"]!=bias:
        log.info(f"{sym} REJECT direction mismatch")
        return

    phase = detect_phase(m15)

    bos_strength = 0.45 if scan["momentum_shift"] else 0.6
    if not detect_bos(m15, bos_strength):
        log.info(f"{sym} NO BOS")
        return

    retrace = 0.25 if scan["is_hot"] and scan["displacement"] else 0.62

    high = max(c["high"] for c in m15[-20:])
    low  = min(c["low"] for c in m15[-20:])

    if bias=="bullish":
        entry = high-(high-low)*retrace
        sl = low
        tp = high+(high-low)
        model = "Model 2/4 Continuation"
    else:
        entry = low+(high-low)*retrace
        sl = high
        tp = low-(high-low)
        model = "Model 1 Reversal"

    entry *= (1+SLIPPAGE)
    tp *= (1-SLIPPAGE)

    rr = abs(tp-entry)/abs(entry-sl)
    if rr<2.5:
        log.info(f"{sym} RR too low")
        return

    size = pos_size(entry, sl)
    risk_amt = ACCOUNT_BALANCE*RISK_PER_TRADE

    open_trades[sym]={
        "bias":bias,"entry":entry,"sl":sl,"tp":tp,
        "rr":rr,"size":size,"risk":risk_amt
    }

    msg = (
        f"🚀 TRADE OPENED\n{sym}\n{model}\nPhase:{phase}\n"
        f"Bias:{bias}\nScore:{score} {reasons}\n"
        f"Entry:{entry:.4f}\nSL:{sl:.4f}\nTP:{tp:.4f}\nRR:1:{rr:.2f}"
    )
    send(msg)

# =========================
# MAIN
# =========================
def main():
    ticks = get_tickers()
    syms = [t["symbol"] for t in ticks if "_USDT" in t["symbol"]][:TOP_N]

    ranked = sorted(syms, key=lambda s: compute_pair_score(s), reverse=True)

    for s in ranked:
        try:
            process(s)
        except Exception as e:
            log.error(f"{s}: {e}")

if __name__=="__main__":
    log.info("BEE-M v25.1 FULL VISIBILITY RUNNING")
    while True:
        main()
        time.sleep(SCAN_INTERVAL)
