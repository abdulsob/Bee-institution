"""
BEE-M Trading Alert Bot v9  (System v3)
Core truth: Liquidity taken -> Market shifts -> Enter on retracement OR momentum continuation

SYSTEM v3 CHANGES vs v8:
  - 4H location is now a GUIDELINE not a hard block.
    Strong BOS + displacement overrides premium/discount restriction.
  - Two entry models:
      Model A (Retracement): 0.5 / OTE 0.618-0.792 / FVG inside zone
      Model B (Momentum):    small pullback after BOS / continuation / micro consolidation
  - Core requirements (non-negotiable): Daily bias + Sweep + BOS
  - Supporting (nice-to-have): premium/discount, OTE, FVG alignment
  - Minimum 1-2 quality trades per day target; 0-trade days from over-filtering eliminated

STATE MACHINE per pair (remembered across scan cycles):
  WAITING_FOR_SWEEP -> SWEEP_CONFIRMED -> WAITING_FOR_RETRACEMENT -> TRADE_TAKEN

24/7 scanning:
  Kill zones: full scan every 15 min
  Outside kill zones: scan every 30 min (still running)
  Status digest every 2 hours: all pair states + rejection reasons
"""

import os
import time
import logging
import requests
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("BEEM")

# ── Config ─────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN    = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID  = os.environ["TELEGRAM_CHAT_ID"]
SCAN_INTERVAL_KZ  = int(os.environ.get("SCAN_INTERVAL_KZ",  "900"))   # seconds between scans
MAX_SWEEP_AGE     = int(os.environ.get("MAX_SWEEP_AGE",     "5400"))
MAX_BOS_AGE       = int(os.environ.get("MAX_BOS_AGE",       "2700"))
TOP_N_PAIRS       = int(os.environ.get("TOP_N_PAIRS",        "30"))
SWEEP_LOOKBACK    = int(os.environ.get("SWEEP_LOOKBACK",     "30"))
STATUS_INTERVAL   = int(os.environ.get("STATUS_INTERVAL",   "7200"))

MEXC_BASE = "https://contract.mexc.com/api/v1/contract"

# ── Global state ───────────────────────────────────────────────────────────
alerted_today    = set()
pair_states      = {}
pair_rejections  = {}
last_status_time = 0.0

# ── Kill zones (WAT = UTC+1) ───────────────────────────────────────────────
KILL_ZONES = [
    (2,  0,  5,  0, "London open"),
    (7,  0, 10,  0, "New York open"),
    (7,  0,  7, 30, "MEXC 8AM spike"),
    (13, 30, 14, 30, "MEXC afternoon"),
    (16,  0, 17,  0, "MEXC daily reset"),
]


def current_kill_zone():
    now = datetime.now(timezone.utc)
    m   = now.hour * 60 + now.minute
    for sh, sm, eh, em, name in KILL_ZONES:
        if sh * 60 + sm <= m < eh * 60 + em:
            return name
    return ""


def minutes_to_next_kz():
    now = datetime.now(timezone.utc)
    m   = now.hour * 60 + now.minute
    best_d, best_n = 9999, ""
    for sh, sm, eh, em, name in KILL_ZONES:
        d = sh * 60 + sm - m
        if d < 0:
            d += 1440
        if d < best_d:
            best_d, best_n = d, name
    return best_d, best_n


# ═══════════════════════════════════════════════════════════════════════════
# STATE MACHINE HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def get_state(sym):
    if sym not in pair_states:
        pair_states[sym] = {"state": "WAITING_FOR_SWEEP"}
    return pair_states[sym]


def reset_state(sym, reason=""):
    if reason:
        log.info(f"{sym}: reset -- {reason}")
    pair_states[sym] = {"state": "WAITING_FOR_SWEEP"}


def state_age(st, key):
    ts = st.get(key, 0)
    return time.time() - ts if ts else float("inf")


def record_rejection(sym, step, reason):
    pair_rejections[sym] = {
        "step":   step,
        "reason": reason,
        "time":   datetime.now(timezone.utc).strftime("%H:%M UTC"),
    }


# ═══════════════════════════════════════════════════════════════════════════
# DATA
# ═══════════════════════════════════════════════════════════════════════════

def get_all_tickers():
    try:
        r = requests.get(f"{MEXC_BASE}/ticker", timeout=15)
        r.raise_for_status()
        d = r.json()
        if not d.get("success"):
            return []
        return [t for t in d.get("data", [])
                if str(t.get("symbol", "")).endswith("_USDT")]
    except Exception as e:
        log.warning(f"get_all_tickers: {e}")
        return []


def get_candles(symbol, interval, limit=120):
    try:
        r = requests.get(
            f"{MEXC_BASE}/kline/{symbol}",
            params={"interval": interval, "limit": limit},
            timeout=10,
        )
        r.raise_for_status()
        d    = r.json()
        if not d.get("success"):
            return []
        raw  = d["data"]
        cols = [raw.get(k, []) for k in ["time", "open", "high", "low", "close", "vol"]]
        n    = len(cols[4])
        return [
            {
                "time":   cols[0][i] if i < len(cols[0]) else 0,
                "open":   float(cols[1][i]) if i < len(cols[1]) else 0,
                "high":   float(cols[2][i]) if i < len(cols[2]) else 0,
                "low":    float(cols[3][i]) if i < len(cols[3]) else 0,
                "close":  float(cols[4][i]) if i < len(cols[4]) else 0,
                "volume": float(cols[5][i]) if i < len(cols[5]) else 0,
            }
            for i in range(n)
        ]
    except Exception as e:
        log.warning(f"get_candles {symbol} {interval}: {e}")
        return []


def get_price(symbol):
    try:
        r = requests.get(f"{MEXC_BASE}/ticker?symbol={symbol}", timeout=8)
        return float(r.json()["data"]["lastPrice"])
    except Exception as e:
        log.warning(f"get_price {symbol}: {e}")
        return 0.0


# ═══════════════════════════════════════════════════════════════════════════
# PAIR SCORING ENGINE
# ═══════════════════════════════════════════════════════════════════════════

def score_pairs(tickers):
    if not tickers:
        return []
    volumes = []
    changes = []
    for t in tickers:
        try:
            volumes.append(float(t.get("volume24", t.get("amount24", 0))))
            changes.append(abs(float(t.get("priceChangeRate", t.get("riseFallRate", 0)))) * 100)
        except Exception:
            volumes.append(0)
            changes.append(0)
    med_vol    = sorted(volumes)[len(volumes) // 2] if volumes else 1
    max_change = max(changes) if changes else 1
    scored     = []
    for i, t in enumerate(tickers):
        sym = t.get("symbol", "")
        if not sym:
            continue
        try:
            price  = float(t.get("lastPrice", 0))
            h24    = float(t.get("high24Price", t.get("highPrice", price)))
            l24    = float(t.get("low24Price",  t.get("lowPrice",  price)))
            vol24  = volumes[i]
            chg    = float(t.get("priceChangeRate", t.get("riseFallRate", 0))) * 100
            if price == 0 or l24 == 0:
                continue
            rng_pct   = (h24 - l24) / price * 100
            vol_score = min(rng_pct / 15 * 25, 25)
            vs2       = min((vol24 / max(med_vol, 1)) / 5 * 25, 25)
            ms        = min(abs(chg) / max_change * 25, 25)
            total     = vol_score + vs2 + ms + 15.0
            parts = []
            if vol_score > 15: parts.append(f"Volatile ({rng_pct:.1f}% range)")
            if vs2 > 15:       parts.append("Strong volume")
            if ms > 15:        parts.append(f"Momentum ({abs(chg):.1f}%)")
            scored.append({
                "symbol":     sym,
                "score":      round(total, 1),
                "change_pct": chg,
                "volume_24h": vol24,
                "price":      price,
                "reason":     " | ".join(parts) if parts else "Moderate",
            })
        except Exception as e:
            log.debug(f"score_pairs {sym}: {e}")
    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored


def select_pairs(tickers, top_n=30):
    scored      = score_pairs(tickers)
    if not scored:
        return []
    top_scored  = [s["symbol"] for s in scored[:10]]
    by_change   = sorted([s for s in scored if s["change_pct"] != 0],
                         key=lambda x: x["change_pct"])
    top_losers  = [s["symbol"] for s in by_change[:5]]
    top_gainers = [s["symbol"] for s in by_change[-5:]]
    seen, pairs = set(), []
    for sym in top_scored + top_gainers + top_losers:
        if sym not in seen:
            seen.add(sym)
            pairs.append(sym)
    log.info(f"Selected {len(pairs)} pairs | top3: {top_scored[:3]}")
    return pairs[:top_n]


# ═══════════════════════════════════════════════════════════════════════════
# FVG HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def find_fvgs(candles):
    fvgs = []
    for i in range(len(candles) - 2):
        c0, c2 = candles[i], candles[i + 2]
        if c2["low"] > c0["high"]:
            fvgs.append({"direction": "bullish",
                         "fvg_high": c2["low"], "fvg_low": c0["high"], "idx": i})
        elif c2["high"] < c0["low"]:
            fvgs.append({"direction": "bearish",
                         "fvg_high": c0["low"], "fvg_low": c2["high"], "idx": i})
    return fvgs


def unmitigated_daily_fvg(daily):
    if len(daily) < 5:
        return None, None, None
    price = daily[-1]["close"]
    for fvg in reversed(find_fvgs(daily[:-1])):
        if fvg["direction"] == "bullish" and price < fvg["fvg_low"]:
            return "bullish", fvg["fvg_high"], fvg["fvg_low"]
        if fvg["direction"] == "bearish" and price > fvg["fvg_high"]:
            return "bearish", fvg["fvg_high"], fvg["fvg_low"]
    return None, None, None


def fvg_inside_ote(candles, bias, ote_high, ote_low):
    recent = candles[-20:] if len(candles) >= 20 else candles
    for fvg in reversed(find_fvgs(recent)):
        if fvg["direction"] == bias:
            if fvg["fvg_low"] < ote_high and fvg["fvg_high"] > ote_low:
                return {"found": True,
                        "fvg_high": fvg["fvg_high"],
                        "fvg_low":  fvg["fvg_low"]}
    return {"found": False}


# ═══════════════════════════════════════════════════════════════════════════
# STEP 1 -- DAILY BIAS (8 scenarios)
# ═══════════════════════════════════════════════════════════════════════════

def daily_bias(daily):
    if len(daily) < 3:
        return {"bias": "neutral", "score": 0, "reasons": [],
                "pdh": 0, "pdl": 0, "mid": 0}
    prev, today, prev2 = daily[-2], daily[-1], daily[-3]
    pdh = prev["high"]
    pdl = prev["low"]
    mid = (pdh + pdl) / 2
    score   = 0
    reasons = []

    if prev["high"] > prev2["high"] and prev["close"] < prev2["high"]:
        score -= 1
        reasons.append("Sc1 Bear: Swept prior high, closed inside")
    if prev["low"] < prev2["low"] and prev["close"] > prev2["low"]:
        score += 1
        reasons.append("Sc1 Bull: Swept prior low, closed inside")

    o = today["open"]
    if o > pdh:
        score += 1
        reasons.append("Sc2 Bull: Opened above PDH")
    elif o < pdl:
        score -= 1
        reasons.append("Sc2 Bear: Opened below PDL")
    else:
        reasons.append("Sc3 Neutral: Inside PD range")

    if o > pdh and today["low"] > pdh:
        score += 1
        reasons.append("Sc4 Bull: Gap above PDH holding")
    if o < pdl and today["high"] < pdl:
        score -= 1
        reasons.append("Sc4 Bear: Gap below PDL holding")

    if prev["close"] > pdh:
        score += 1
        reasons.append("Sc5 Bull: Yesterday closed above PDH")
    elif prev["close"] < pdl:
        score -= 1
        reasons.append("Sc5 Bear: Yesterday closed below PDL")
    if prev["high"] > prev2["high"] and pdl < prev["close"] < prev2["high"]:
        score -= 1
        reasons.append("Sc5 Bear: Swept high, closed back inside")
    if prev["low"] < prev2["low"] and prev2["low"] < prev["close"] < pdh:
        score += 1
        reasons.append("Sc5 Bull: Swept low, closed back inside")

    if today["close"] > mid:
        score += 1
        reasons.append(f"Sc6 Bull: Above midpoint ({mid:.4f})")
    else:
        score -= 1
        reasons.append(f"Sc6 Bear: Below midpoint ({mid:.4f})")

    if len(daily) >= 6:
        r5 = daily[-6:-1]
        hs = [c["high"] for c in r5]
        ls = [c["low"]  for c in r5]
        if all(hs[i] < hs[i+1] for i in range(len(hs)-1)) and \
           all(ls[i] < ls[i+1] for i in range(len(ls)-1)):
            score += 2
            reasons.append("Sc7 Bull: Daily HH+HL")
        elif all(hs[i] > hs[i+1] for i in range(len(hs)-1)) and \
             all(ls[i] > ls[i+1] for i in range(len(ls)-1)):
            score -= 2
            reasons.append("Sc7 Bear: Daily LH+LL")

    fd, fh, fl = unmitigated_daily_fvg(daily)
    if fd == "bullish":
        score += 1
        reasons.append("Sc8 Bull: Daily FVG above draws price up")
    elif fd == "bearish":
        score -= 1
        reasons.append("Sc8 Bear: Daily FVG below draws price down")

    bias = "bullish" if score >= 2 else "bearish" if score <= -2 else "neutral"
    return {"bias": bias, "score": score, "reasons": reasons,
            "pdh": pdh, "pdl": pdl, "mid": mid}


# ═══════════════════════════════════════════════════════════════════════════
# STEP 2 -- 4H LOCATION via Fibonacci on actual swing structure
# ═══════════════════════════════════════════════════════════════════════════

def find_4h_swing_points(h4):
    if len(h4) < 10:
        return None, None, None, None
    sh_price = sh_idx = sl_price = sl_idx = None
    for i in range(len(h4) - 3, 1, -1):
        c = h4[i]
        if sh_price is None and 2 <= i <= len(h4) - 3 and \
           c["high"] >= h4[i-1]["high"] and c["high"] >= h4[i-2]["high"] and \
           c["high"] >= h4[i+1]["high"] and c["high"] >= h4[i+2]["high"]:
            sh_price = c["high"]
            sh_idx   = i
        if sl_price is None and 2 <= i <= len(h4) - 3 and \
           c["low"] <= h4[i-1]["low"] and c["low"] <= h4[i-2]["low"] and \
           c["low"] <= h4[i+1]["low"] and c["low"] <= h4[i+2]["low"]:
            sl_price = c["low"]
            sl_idx   = i
        if sh_price is not None and sl_price is not None:
            break
    return sh_price, sl_price, sh_idx, sl_idx


def location_4h(h4):
    sh, sl, sh_idx, sl_idx = find_4h_swing_points(h4)
    if sh is None or sl is None or sh == sl:
        r  = h4[-20:]
        sh = max(c["high"] for c in r)
        sl = min(c["low"]  for c in r)
    rng   = sh - sl
    price = h4[-1]["close"]
    if rng == 0:
        return {"location": "unknown", "eq": price, "sh": sh, "sl": sl,
                "fib_618": price, "fib_705": price, "fib_792": price}
    bl  = (sl_idx < sh_idx) if (sh_idx is not None and sl_idx is not None) else True
    eq  = sl + 0.500 * rng
    loc = "premium" if price > eq else "discount" if price < eq else "equilibrium"
    return {
        "location":    loc,
        "eq":          eq,
        "sh":          sh,
        "sl":          sl,
        "fib_236":     sl + 0.236 * rng,
        "fib_382":     sl + 0.382 * rng,
        "fib_618":     sl + 0.618 * rng,
        "fib_705":     sl + 0.705 * rng,
        "fib_792":     sl + 0.792 * rng,
        "bullish_leg": bl,
    }


# ═══════════════════════════════════════════════════════════════════════════
# STEP 3 -- CLEAN STRUCTURE FILTER
# ═══════════════════════════════════════════════════════════════════════════

def is_clean_structure(m15, lookback=20):
    if len(m15) < lookback:
        return {"clean": False, "reason": "Not enough candles"}
    window   = m15[-lookback:]
    overlaps = 0
    for i in range(1, len(window)):
        ph = max(window[i-1]["open"], window[i-1]["close"])
        pl = min(window[i-1]["open"], window[i-1]["close"])
        ch = max(window[i]["open"],   window[i]["close"])
        cl = min(window[i]["open"],   window[i]["close"])
        if cl < ph and ch > pl:
            overlaps += 1
    ovr = overlaps / (len(window) - 1)
    if ovr > 0.65:
        return {"clean": False, "reason": f"Choppy -- {ovr:.0%} overlap ratio"}
    wr = []
    for c in window:
        r = c["high"] - c["low"]
        if r > 0:
            wr.append(1 - abs(c["close"] - c["open"]) / r)
    avg_wr = sum(wr) / len(wr) if wr else 0
    if avg_wr > 0.6:
        return {"clean": False, "reason": f"Wick-heavy -- {avg_wr:.0%} avg wick ratio"}
    up = sum(1 for c in window if c["close"] > c["open"])
    dn = sum(1 for c in window if c["close"] < c["open"])
    if max(up, dn) / len(window) < 0.55:
        return {"clean": False, "reason": "No clear directional move -- ranging"}
    return {"clean": True,
            "reason": f"Clean ({ovr:.0%} overlap, {avg_wr:.0%} wick ratio)"}


# ═══════════════════════════════════════════════════════════════════════════
# STEP 4 -- TRUE LIQUIDITY SWEEP (wick + close back inside)
# ═══════════════════════════════════════════════════════════════════════════

def find_equal_levels(candles, bias, tolerance=0.002):
    prices = [c["high"] for c in candles] if bias == "bearish" \
              else [c["low"] for c in candles]
    levels = []
    for i in range(len(prices)):
        cluster = [prices[i]]
        for j in range(i + 1, len(prices)):
            if abs(prices[j] - prices[i]) / max(prices[i], 0.0001) < tolerance:
                cluster.append(prices[j])
        if len(cluster) >= 2:
            levels.append(sum(cluster) / len(cluster))
    return list(set(round(l, 6) for l in levels))


def detect_true_sweep(m15, bias, pdh, pdl):
    if len(m15) < 5:
        return {"swept": False, "reason": "Not enough candles"}
    window  = m15[-SWEEP_LOOKBACK:] if len(m15) >= SWEEP_LOOKBACK else m15
    offset  = len(m15) - len(window)
    eq_highs = find_equal_levels(window, "bearish")
    eq_lows  = find_equal_levels(window, "bullish")
    best = None
    for i in range(len(window) - 1):
        c       = window[i]
        age     = len(window) - 1 - i
        abs_idx = offset + i
        if bias == "bullish":
            if c["low"] < pdl and c["close"] > pdl:
                if (c["close"] - pdl) / max(pdl, 0.0001) > 0.015:
                    continue
                reaction = c["close"] - c["low"]
                is_eq    = any(abs(c["low"] - l) / max(l, 0.0001) < 0.002
                               for l in eq_lows)
                sw = {
                    "swept":       True,
                    "sweep_type":  "immediate" if age <= 5 else "pre_swept",
                    "sweep_level": c["low"],
                    "candle_idx":  abs_idx,
                    "is_equal_hl": is_eq,
                    "reaction":    reaction,
                    "age_candles": age,
                    "sweep_low":   c["low"],
                }
                if best is None or age < best["age_candles"]:
                    best = sw
        elif bias == "bearish":
            if c["high"] > pdh and c["close"] < pdh:
                if (pdh - c["close"]) / max(pdh, 0.0001) > 0.015:
                    continue
                reaction = c["high"] - c["close"]
                is_eq    = any(abs(c["high"] - l) / max(l, 0.0001) < 0.002
                               for l in eq_highs)
                sw = {
                    "swept":        True,
                    "sweep_type":   "immediate" if age <= 5 else "pre_swept",
                    "sweep_level":  c["high"],
                    "candle_idx":   abs_idx,
                    "is_equal_hl":  is_eq,
                    "reaction":     reaction,
                    "age_candles":  age,
                    "sweep_high":   c["high"],
                }
                if best is None or age < best["age_candles"]:
                    best = sw
    return best if best else {"swept": False, "reason": "No valid wick+rejection sweep found"}


# ═══════════════════════════════════════════════════════════════════════════
# STEP 5 -- DISPLACEMENT
# ═══════════════════════════════════════════════════════════════════════════

def detect_displacement(m15, bias, after_idx):
    prior = m15[max(0, after_idx - 10): after_idx]
    if not prior:
        return {"valid": False, "avg_body": 0, "reason": "No prior candles"}
    avg = sum(abs(c["close"] - c["open"]) for c in prior) / len(prior)
    if avg == 0:
        return {"valid": False, "avg_body": 0, "reason": "Zero avg body"}
    disp_candles = m15[after_idx + 1: after_idx + 4]
    if not disp_candles:
        return {"valid": False, "avg_body": avg, "reason": "No candles after sweep"}
    for dc in disp_candles:
        body = abs(dc["close"] - dc["open"])
        rng  = dc["high"] - dc["low"]
        if rng == 0:
            continue
        br     = body / rng
        is_dir = (dc["close"] > dc["open"] if bias == "bullish"
                  else dc["close"] < dc["open"])
        if body >= avg * 1.8 and br >= 0.5 and is_dir:
            leaves_fvg = False
            try:
                idx = m15.index(dc)
                if 0 < idx < len(m15) - 2:
                    pc = m15[idx - 1]
                    nc = m15[idx + 1]
                    if bias == "bullish" and nc["low"] > pc["high"]:
                        leaves_fvg = True
                    if bias == "bearish" and nc["high"] < pc["low"]:
                        leaves_fvg = True
            except Exception:
                pass
            return {"valid": True, "avg_body": avg, "body_ratio": round(br, 2),
                    "leaves_fvg": leaves_fvg, "reason": "Strong displacement confirmed"}
    return {"valid": False, "avg_body": avg,
            "reason": "Displacement too weak -- slow or choppy move"}


# ═══════════════════════════════════════════════════════════════════════════
# STEP 6 -- EXPANSION BOS tied to sweep context
# ═══════════════════════════════════════════════════════════════════════════

def find_swing_points(candles, strength=2):
    swings = []
    n = len(candles)
    for i in range(strength, n - strength):
        c     = candles[i]
        is_sh = all(c["high"] >= candles[i-j]["high"] for j in range(1, strength+1)) and \
                all(c["high"] >= candles[i+j]["high"] for j in range(1, strength+1))
        is_sl = all(c["low"]  <= candles[i-j]["low"]  for j in range(1, strength+1)) and \
                all(c["low"]  <= candles[i+j]["low"]   for j in range(1, strength+1))
        if is_sh:
            swings.append({"type": "high", "price": c["high"], "idx": i, "candle": c})
        if is_sl:
            swings.append({"type": "low",  "price": c["low"],  "idx": i, "candle": c})
    return swings


def find_relevant_structure(pre_sweep, bias):
    if len(pre_sweep) < 6:
        return {"found": False}
    avg_body = sum(abs(c["close"] - c["open"]) for c in pre_sweep) / len(pre_sweep)
    if avg_body == 0:
        return {"found": False}
    swings = find_swing_points(pre_sweep, strength=2)
    ttype  = "high" if bias == "bullish" else "low"
    for sw in reversed(swings):
        if sw["type"] != ttype:
            continue
        if abs(sw["candle"]["close"] - sw["candle"]["open"]) >= avg_body * 0.8:
            return {"found": True, "price": sw["price"], "idx": sw["idx"]}
    for sw in reversed(swings):
        if sw["type"] == ttype:
            return {"found": True, "price": sw["price"], "idx": sw["idx"]}
    return {"found": False}


def is_expansion_candle(candle, avg_body, bias, prev_candle):
    body = abs(candle["close"] - candle["open"])
    rng  = candle["high"] - candle["low"]
    if rng == 0:
        return {"valid": False, "reason": "Zero range"}
    if body < avg_body * 2.0:
        return {"valid": False, "reason": f"Body {body:.4f} less than 2x avg -- inducement BOS"}
    br = body / rng
    if br < 0.55:
        return {"valid": False, "reason": f"Wick-heavy ({br:.0%}) -- choppy break"}
    if bias == "bullish" and candle["close"] <= candle["open"]:
        return {"valid": False, "reason": "Bearish body on bullish BOS"}
    if bias == "bearish" and candle["close"] >= candle["open"]:
        return {"valid": False, "reason": "Bullish body on bearish BOS"}
    imb = False
    if prev_candle:
        if bias == "bullish" and candle["low"]  > prev_candle["high"]:
            imb = True
        if bias == "bearish" and candle["high"] < prev_candle["low"]:
            imb = True
    return {"valid": True, "leaves_imbalance": imb,
            "body_ratio": round(br, 2), "reason": "Expansion BOS"}


def detect_bos(m15, bias, sweep_idx, avg_body, location):
    pre_sweep  = m15[max(0, sweep_idx - 20): sweep_idx]
    post_sweep = m15[sweep_idx + 1:]
    if not pre_sweep or not post_sweep:
        return {"broken": False, "reason": "Not enough candles around sweep"}
    # v3: location is a guideline only -- BOS quality determines validity.
    # Equilibrium is noted in the alert but no longer a hard block.
    if location == "equilibrium":
        log.debug("BOS at equilibrium -- lower probability, proceeding per v3")
    struct = find_relevant_structure(pre_sweep, bias)
    if not struct["found"]:
        return {"broken": False, "reason": "No relevant pre-sweep swing structure found"}
    level = struct["price"]
    for i, c in enumerate(post_sweep[-8:]):
        prev_c = post_sweep[i - 1] if i > 0 else pre_sweep[-1]
        if bias == "bullish":
            if c["close"] <= level:
                continue
            exp = is_expansion_candle(c, avg_body, bias, prev_c)
            if not exp["valid"]:
                return {"broken": False,
                        "reason": f"Inducement BOS rejected -- {exp['reason']}"}
            return {"broken": True, "bos_level": level, "bos_origin": level,
                    "bos_type": "Broke last 15M swing HIGH (expansion)",
                    "leaves_imbalance": exp["leaves_imbalance"],
                    "body_ratio":       exp["body_ratio"]}
        elif bias == "bearish":
            if c["close"] >= level:
                continue
            exp = is_expansion_candle(c, avg_body, bias, prev_c)
            if not exp["valid"]:
                return {"broken": False,
                        "reason": f"Inducement BOS rejected -- {exp['reason']}"}
            return {"broken": True, "bos_level": level, "bos_origin": level,
                    "bos_type": "Broke last 15M swing LOW (expansion)",
                    "leaves_imbalance": exp["leaves_imbalance"],
                    "body_ratio":       exp["body_ratio"]}
    return {"broken": False, "reason": "No candle closed beyond the swing level"}


# ═══════════════════════════════════════════════════════════════════════════
# STEP 7 -- MOVE QUALITY + OTE on actual BOS impulse leg
# ═══════════════════════════════════════════════════════════════════════════

def classify_move_quality(disp):
    ratio = disp.get("body_ratio", 0)
    fvg   = disp.get("leaves_fvg", False)
    if ratio >= 0.70 and fvg:
        return "strong"
    if ratio >= 0.55:
        return "moderate"
    return "weak"


def calc_ote(m15, bias, sweep_idx, bos_level, move_quality="moderate"):
    zones = {
        "strong":   (0.382, 0.500, 0.440),
        "moderate": (0.618, 0.705, 0.660),
        "weak":     (0.705, 0.790, 0.750),
    }
    high_ret, low_ret, ideal_ret = zones.get(move_quality, zones["moderate"])
    leg = m15[sweep_idx: sweep_idx + 15]
    if not leg:
        return {}
    if bias == "bullish":
        leg_low  = min(c["low"]  for c in leg)
        leg_high = bos_level
        rng = leg_high - leg_low
        if rng <= 0:
            return {}
        return {"ote_high": leg_high - high_ret * rng,
                "ote_low":  leg_high - low_ret  * rng,
                "ideal":    leg_high - ideal_ret * rng,
                "leg_high": leg_high, "leg_low": leg_low,
                "move_quality": move_quality}
    elif bias == "bearish":
        leg_high = max(c["high"] for c in leg)
        leg_low  = bos_level
        rng = leg_high - leg_low
        if rng <= 0:
            return {}
        return {"ote_low":  leg_low + high_ret * rng,
                "ote_high": leg_low + low_ret  * rng,
                "ideal":    leg_low + ideal_ret * rng,
                "leg_high": leg_high, "leg_low": leg_low,
                "move_quality": move_quality}
    return {}


# ═══════════════════════════════════════════════════════════════════════════
# MODEL B -- MOMENTUM ENTRY (v3 new)
# Use when market is strong and not retracing deep.
# Conditions: Sweep confirmed + strong displacement + clean BOS.
# Entry: small pullback after BOS, continuation candle, or micro consolidation.
# ═══════════════════════════════════════════════════════════════════════════

def detect_momentum_entry(m15, bias, bos_level, bos_idx=None):
    """
    Model B entry: price is strong, not retracing to OTE.
    Looks for one of:
      1. Small pullback (< 0.5 retrace of BOS leg) with continuation candle
      2. Micro consolidation (2-3 tight candles) followed by breakout
      3. Immediate continuation candle after BOS
    Returns dict with 'valid', 'entry_type', 'entry_price', 'reason'.
    """
    if len(m15) < 6:
        return {"valid": False, "reason": "Not enough candles for momentum check"}

    # Work with last 8 candles after the BOS
    recent = m15[-8:] if len(m15) >= 8 else m15
    avg_body = sum(abs(c["close"] - c["open"]) for c in m15[-20:]) / max(len(m15[-20:]), 1)
    if avg_body == 0:
        return {"valid": False, "reason": "Zero avg body"}

    price = m15[-1]["close"]

    # Check 1: Immediate continuation -- last candle is directional and strong
    last = m15[-1]
    last_body = abs(last["close"] - last["open"])
    last_rng  = last["high"] - last["low"]
    is_dir    = (last["close"] > last["open"]) if bias == "bullish" else (last["close"] < last["open"])
    if last_rng > 0 and last_body / last_rng >= 0.55 and last_body >= avg_body * 1.5 and is_dir:
        return {
            "valid":       True,
            "entry_type":  "continuation",
            "entry_price": price,
            "reason":      f"Model B: Momentum continuation candle ({last_body/last_rng:.0%} body ratio)",
        }

    # Check 2: Small pullback (< 50% retrace) with reversal candle
    if len(recent) >= 3:
        # Find the extreme in the BOS direction
        if bias == "bullish":
            leg_high = max(c["high"] for c in recent)
            leg_low  = min(c["low"]  for c in recent)
            rng = leg_high - leg_low
            if rng > 0:
                retrace_pct = (leg_high - price) / rng
                if 0.05 < retrace_pct < 0.50:
                    # Confirm a bullish reversal candle
                    if last["close"] > last["open"]:
                        return {
                            "valid":       True,
                            "entry_type":  "small_pullback",
                            "entry_price": price,
                            "reason":      f"Model B: Small pullback {retrace_pct:.0%} + bullish close",
                        }
        elif bias == "bearish":
            leg_high = max(c["high"] for c in recent)
            leg_low  = min(c["low"]  for c in recent)
            rng = leg_high - leg_low
            if rng > 0:
                retrace_pct = (price - leg_low) / rng
                if 0.05 < retrace_pct < 0.50:
                    if last["close"] < last["open"]:
                        return {
                            "valid":       True,
                            "entry_type":  "small_pullback",
                            "entry_price": price,
                            "reason":      f"Model B: Small pullback {retrace_pct:.0%} + bearish close",
                        }

    # Check 3: Micro consolidation (last 3 candles tight range) then breakout
    if len(recent) >= 4:
        consol = recent[-4:-1]
        consol_highs = [c["high"] for c in consol]
        consol_lows  = [c["low"]  for c in consol]
        consol_rng   = max(consol_highs) - min(consol_lows)
        breakout     = recent[-1]
        if consol_rng > 0 and consol_rng < avg_body * 2.5:
            if bias == "bullish" and breakout["close"] > max(consol_highs):
                return {
                    "valid":       True,
                    "entry_type":  "micro_consolidation_breakout",
                    "entry_price": price,
                    "reason":      "Model B: Micro consolidation breakout (bullish)",
                }
            if bias == "bearish" and breakout["close"] < min(consol_lows):
                return {
                    "valid":       True,
                    "entry_type":  "micro_consolidation_breakout",
                    "entry_price": price,
                    "reason":      "Model B: Micro consolidation breakout (bearish)",
                }

    return {"valid": False, "reason": "No Model B entry pattern detected"}


# ═══════════════════════════════════════════════════════════════════════════
# STEP 8 -- SETUP SCORING  A+ / A / B
# ═══════════════════════════════════════════════════════════════════════════

def score_setup(sweep, disp, bos, fvg_ote, structure):
    bonus   = 0
    reasons = []
    if fvg_ote and fvg_ote.get("found"):
        bonus += 1
        reasons.append("FVG inside OTE [*]")
    if sweep.get("is_equal_hl"):
        bonus += 1
        reasons.append("Equal highs/lows swept [*]")
    if sweep.get("sweep_type") == "immediate":
        bonus += 1
        reasons.append("Immediate sweep to BOS [*]")
    if disp.get("body_ratio", 0) >= 0.70:
        bonus += 1
        reasons.append("Strong displacement body >= 70% [*]")
    if structure.get("clean"):
        bonus += 1
        reasons.append("Clean structure [*]")
    rating = "A+" if bonus >= 4 else "A" if bonus >= 2 else "B"
    return {"rating": rating, "bonus": bonus, "reasons": reasons}


# ═══════════════════════════════════════════════════════════════════════════
# STEP 9 -- STRUCTURAL SL + LIQUIDITY TPs
# ═══════════════════════════════════════════════════════════════════════════

def find_liquidity_targets(m15, h4, daily, bias):
    targets = []
    price   = m15[-1]["close"] if m15 else 0
    if price == 0:
        return targets
    if len(daily) >= 2:
        pdh = daily[-2]["high"]
        pdl = daily[-2]["low"]
        if bias == "bullish" and pdh > price:
            targets.append(("PDH", pdh))
        if bias == "bearish" and pdl < price:
            targets.append(("PDL", pdl))
    if h4:
        eq_lvls = find_equal_levels(
            h4[-30:],
            "bearish" if bias == "bullish" else "bullish"
        )
        for lvl in eq_lvls:
            if bias == "bullish" and lvl > price:
                targets.append(("Equal highs", lvl))
            if bias == "bearish" and lvl < price:
                targets.append(("Equal lows",  lvl))
    if h4:
        recent_4h = h4[-20:]
        if bias == "bullish":
            sh = max(c["high"] for c in recent_4h)
            if sh > price:
                targets.append(("4H swing high", sh))
        if bias == "bearish":
            sl = min(c["low"] for c in recent_4h)
            if sl < price:
                targets.append(("4H swing low", sl))
    if bias == "bullish":
        targets.sort(key=lambda x: x[1])
    else:
        targets.sort(key=lambda x: x[1], reverse=True)
    return targets


def calc_risk(bias, sweep, bos, ote, m15=None, h4=None, daily=None):
    if not ote:
        return {}
    entry = ote.get("ideal", 0)
    if not entry:
        return {}
    if bias == "bullish":
        cands = [l for l in [sweep.get("sweep_low", 0),
                              bos.get("bos_origin", 0)] if l > 0]
        sl    = min(cands) * 0.9995 if cands else entry * 0.98
        rr    = entry - sl
        if rr <= 0:
            return {}
    elif bias == "bearish":
        cands = [l for l in [sweep.get("sweep_high", 0),
                              bos.get("bos_origin", 0)] if l > 0]
        sl    = max(cands) * 1.0005 if cands else entry * 1.02
        rr    = sl - entry
        if rr <= 0:
            return {}
    else:
        return {}

    liq = find_liquidity_targets(m15 or [], h4 or [], daily or [], bias)

    def tp_val(idx, fallback_mult):
        if idx < len(liq):
            return liq[idx][1], liq[idx][0]
        v = entry + rr * fallback_mult if bias == "bullish" \
            else entry - rr * fallback_mult
        return v, f"{fallback_mult}R fallback"

    tp1,    tp1_lbl = tp_val(0, 2)
    tp2,    tp2_lbl = tp_val(1, 3)
    runner, run_lbl = tp_val(2, 5)
    rr_ratio = round(abs(tp1 - entry) / rr, 2) if rr > 0 else 2.0

    return {"entry": entry, "sl": sl,
            "tp1": tp1, "tp1_label": tp1_lbl,
            "tp2": tp2, "tp2_label": tp2_lbl,
            "runner": runner, "runner_label": run_lbl,
            "rr_ratio": rr_ratio}


# ═══════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════════════

# Tracks the last Telegram update_id processed (avoids replaying old ones)
_last_update_id = 0


def send_telegram(msg):
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID,
                  "text":    msg,
                  "parse_mode": "HTML"},
            timeout=10,
        )
        if not r.ok:
            log.warning(f"Telegram {r.status_code}: {r.text[:80]}")
    except Exception as e:
        log.warning(f"Telegram send failed: {e}")


def get_telegram_updates():
    """Poll for new messages/commands sent to the bot."""
    global _last_update_id
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params={"offset": _last_update_id + 1, "timeout": 2, "limit": 10},
            timeout=8,
        )
        if not r.ok:
            return []
        updates = r.json().get("result", [])
        if updates:
            _last_update_id = updates[-1]["update_id"]
        return updates
    except Exception as e:
        log.debug(f"getUpdates: {e}")
        return []


def handle_commands():
    """Check for incoming Telegram commands and respond."""
    updates = get_telegram_updates()
    for upd in updates:
        msg  = upd.get("message") or upd.get("channel_post", {})
        text = msg.get("text", "").strip().lower()
        if not text:
            continue
        log.info(f"Command received: {text}")
        if text in ("/status", "/s"):
            send_telegram(build_scan_status())
        elif text in ("/pairs", "/p"):
            send_telegram(build_pairs_status())
        elif text in ("/help", "/h"):
            send_telegram(
                "<b>BEE-M Bot Commands</b>\n\n"
                "/status  or  /s  — Full scan status: every pair + current system step\n"
                "/pairs   or  /p  — Quick list: only pairs in an active setup\n"
                "/help    or  /h  — Show this menu"
            )


def build_scan_status():
    """
    Full status: every tracked pair showing exactly which step of the
    BEE-M system it is currently at, plus the last rejection reason.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    active_lines = []
    for sym, st in sorted(pair_states.items()):
        state = st.get("state", "WAITING_FOR_SWEEP")
        if state == "WAITING_FOR_SWEEP":
            continue
        bias   = st.get("bias", "?").upper()
        pair   = sym.replace("_", "/")
        rating = st.get("setup_score", {}).get("rating", "")

        if state == "SWEEP_CONFIRMED":
            age = int(state_age(st, "sweep_time") / 60)
            lvl = st.get("sweep", {}).get("sweep_level", 0)
            active_lines.append(
                f"  <b>{pair}</b>  {bias}\n"
                f"    Step 4 SWEEP confirmed @ {lvl:.4f}  ({age}min ago)\n"
                f"    Waiting: Step 5 Displacement + Step 6 BOS"
            )
        elif state == "WAITING_FOR_RETRACEMENT":
            age = int(state_age(st, "bos_time") / 60)
            ote = st.get("ote", {})
            fvg = st.get("fvg_ote", {})
            fvg_tag = "  FVG in zone" if fvg and fvg.get("found") else ""
            active_lines.append(
                f"  <b>{pair}</b>  {bias}  [{rating}]\n"
                f"    Steps 1-6 COMPLETE  ({age}min since BOS)\n"
                f"    Step 7: Watching OTE {ote.get('ote_low',0):.4f}-{ote.get('ote_high',0):.4f}{fvg_tag}\n"
                f"    Waiting: Model A reaction OR Model B momentum"
            )

    waiting_lines = []
    step_map = {
        "Step 1 (Daily Bias)":    "Step 1 DAILY BIAS",
        "Step 2 (4H Location)":   "Step 2 4H LOCATION",
        "Step 3 (Structure)":     "Step 3 STRUCTURE",
        "Step 4 (Sweep)":         "Step 4 SWEEP",
        "Step 5 (Displacement)":  "Step 5 DISPLACEMENT",
        "Step 6 (BOS)":           "Step 6 BOS",
        "Step 7 (OTE)":           "Step 7 OTE/ENTRY",
        "Step 7 (OTE + Model B)": "Step 7 OTE + MODEL B",
        "Data":                   "DATA ERROR",
    }
    for sym, rej in sorted(pair_rejections.items()):
        state = pair_states.get(sym, {}).get("state", "WAITING_FOR_SWEEP")
        if state != "WAITING_FOR_SWEEP":
            continue
        pair  = sym.replace("_", "/")
        step  = rej.get("step", "?")
        why   = rej.get("reason", "?")
        t     = rej.get("time", "")
        label = step_map.get(step, step)
        waiting_lines.append(f"  {pair}  |  {label} failed  |  {why}  [{t}]")

    lines = [
        "<b>BEE-M SCAN STATUS</b>",
        f"<i>{now}</i>",
        f"Pairs tracked: {len(pair_states)}  |  Active setups: {len(active_lines)}",
        "",
    ]
    if active_lines:
        lines.append("<b>-- ACTIVE SETUPS --</b>")
        lines.extend(active_lines)
        lines.append("")
    if waiting_lines:
        lines.append("<b>-- LAST REJECTION PER PAIR --</b>")
        lines.extend(waiting_lines[:20])
        if len(waiting_lines) > 20:
            lines.append(f"  ...and {len(waiting_lines) - 20} more pairs")
        lines.append("")
    if not active_lines and not waiting_lines:
        lines.append("No pairs scanned yet -- first cycle may still be running.")
    lines.append(f"Next scan in ~{SCAN_INTERVAL_KZ // 60} min  |  /help for commands")
    return "\n".join(lines)


def build_pairs_status():
    """
    Quick view: only pairs in an active setup, one line each.
    """
    now   = datetime.now(timezone.utc).strftime("%H:%M UTC")
    lines = [f"<b>BEE-M ACTIVE PAIRS</b>  {now}", ""]
    active = [(sym, st) for sym, st in sorted(pair_states.items())
              if st.get("state") != "WAITING_FOR_SWEEP"]
    if not active:
        lines.append("No active setups right now.")
        lines.append("Send /status to see all pairs and why they are waiting.")
        return "\n".join(lines)
    step_labels = {
        "SWEEP_CONFIRMED":         "Step 4 SWEEP confirmed -- waiting for BOS",
        "WAITING_FOR_RETRACEMENT": "Step 6 BOS confirmed -- watching entry zone",
        "TRADE_TAKEN":             "Step 7 Trade fired",
    }
    for sym, st in active:
        pair   = sym.replace("_", "/")
        bias   = st.get("bias", "?").upper()
        state  = st.get("state", "?")
        rating = st.get("setup_score", {}).get("rating", "")
        label  = step_labels.get(state, state)
        rtag   = f"  [{rating}]" if rating else ""
        lines.append(f"<b>{pair}</b>  {bias}{rtag}\n  {label}")
    lines.append("\nSend /status for full detail on all scanned pairs.")
    return "\n".join(lines)


def build_setup_alert(sym, bd, loc_data, sweep, disp, bos,
                      fvg_ote, ote, risk, setup_score, pair_score):
    bias      = bd["bias"]
    direction = bias.upper()
    rating    = setup_score["rating"]
    now       = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    loc       = loc_data.get("location", "unknown")
    mq        = ote.get("move_quality", "moderate")
    mq_map    = {
        "strong":   "Shallow zone (0.382-0.50) -- strong move",
        "moderate": "Standard OTE (0.618-0.705)",
        "weak":     "Deep OTE (0.705-0.79) -- wait for full retrace",
    }
    mq_desc   = mq_map.get(mq, "Standard OTE")
    eq_tag    = " | Equal H/L bonus" if sweep.get("is_equal_hl") else ""
    bonus_txt = "\n".join(f"  {r}" for r in setup_score["reasons"]) or "  None"
    fvg_line  = ("FVG in OTE: YES -- " +
                 f"{fvg_ote['fvg_low']:.4f} to {fvg_ote['fvg_high']:.4f} (preferred entry)"
                 if fvg_ote and fvg_ote.get("found")
                 else "FVG in OTE: None -- wait for clean reaction in zone")
    sc_line   = (f"Pair: {pair_score.get('score', 0):.0f}/100"
                 f" | 24h: {pair_score.get('change_pct', 0):+.2f}%"
                 f" | {pair_score.get('reason', '')}")

    lines = [
        f"<b>BEE-M SETUP -- {direction} -- Rating: {rating}</b>",
        f"<b>Pair:</b> {sym.replace('_', '/')}  |  <b>Time:</b> {now}",
        sc_line,
        "",
        "<b>-- DAILY BIAS --</b>",
        f"Score: {bd['score']:+d} | PDH: {bd['pdh']:.4f} | PDL: {bd['pdl']:.4f}",
    ]
    lines.extend(f"  {r}" for r in bd["reasons"][:4])
    loc_note = ""
    if loc in ("premium", "discount"):
        if (bias == "bullish" and loc == "premium") or (bias == "bearish" and loc == "discount"):
            loc_note = f"  ⚠ Not ideal ({loc}) -- BOS quality overrides (v3)"
        else:
            loc_note = f"  ✔ Ideal zone ({loc})"
    lines += [
        "",
        "<b>-- 4H LOCATION (Fib on swing) --</b>",
        f"Price in <b>{loc.upper()}</b>{('  [GUIDELINE ONLY]' if loc in ('premium','discount') else '')}",
        loc_note,
        f"Range: {loc_data.get('sl', 0):.4f} to {loc_data.get('sh', 0):.4f}",
        f"EQ (50%): {loc_data.get('eq', 0):.4f}",
        f"OTE band: {loc_data.get('fib_618', 0):.4f} to {loc_data.get('fib_792', 0):.4f}",
        "",
        "<b>-- SWEEP --</b>",
        (f"{sweep.get('sweep_type', '').replace('_', ' ').title()}"
         f" ({sweep.get('age_candles', 0)} candles ago){eq_tag}"),
        f"Level: {sweep.get('sweep_level', 0):.4f} | Rejection: {sweep.get('reaction', 0):.4f}",
        "",
        "<b>-- DISPLACEMENT --</b>",
        f"Body ratio: {disp.get('body_ratio', 0):.0%} | FVG left: {'Yes' if disp.get('leaves_fvg') else 'No'}",
        f"Move quality: <b>{mq.upper()}</b> -- {mq_desc}",
        "",
        "<b>-- BOS --</b>",
        bos.get("bos_type", "Confirmed"),
        f"Imbalance: {'Yes' if bos.get('leaves_imbalance') else 'No'} | Body: {bos.get('body_ratio', 0):.0%}",
        "",
        "<b>-- OTE ENTRY ZONE --</b>",
        f"Leg: {ote.get('leg_low', 0):.4f} to {ote.get('leg_high', 0):.4f}",
        f"Zone: <b>{ote.get('ote_low', 0):.4f} to {ote.get('ote_high', 0):.4f}</b>",
        f"Ideal (0.705): <b>{ote.get('ideal', 0):.4f}</b>",
        fvg_line,
        "",
        "<b>-- RISK --</b>",
        f"Entry:        <b>{risk.get('entry', 0):.4f}</b>",
        f"SL:           <b>{risk.get('sl', 0):.4f}</b>  (structural)",
        f"TP1 (50%):    <b>{risk.get('tp1', 0):.4f}</b>  [{risk.get('tp1_label', '--')}]  (~1:{risk.get('rr_ratio', 2):.1f})",
        f"TP2 (30%):    <b>{risk.get('tp2', 0):.4f}</b>  [{risk.get('tp2_label', '--')}]",
        f"Runner (20%): <b>{risk.get('runner', 0):.4f}</b>  [{risk.get('runner_label', '--')}]",
        "",
        "<b>-- BONUS QUALITY --</b>",
        bonus_txt,
        "",
        "Sweep | Displacement | Expansion BOS | OTE all confirmed.",
        "Monitoring OTE zone now. CONFIRMED or SKIP alert coming next.",
        "<b>Risk 1-2% only. You are trading: liquidity taken - shift - retracement.</b>",
    ]
    return "\n".join(lines)


def build_status_digest(scored_pairs):
    now  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    active   = [(sym, st) for sym, st in pair_states.items()
                if st.get("state") != "WAITING_FOR_SWEEP"]
    rejected = list(pair_rejections.items())[-15:]

    lines = [
        "<b>BEE-M STATUS DIGEST</b>",
        f"Time: {now}",
        f"24/7 scan active -- every 15 min",
        f"Pairs tracked: {len(pair_states)} | Active setups: {len(active)}",
        "",
    ]

    if active:
        lines.append("<b>-- ACTIVE SETUPS --</b>")
        for sym, st in active:
            state  = st.get("state", "?")
            bias   = st.get("bias", "?").upper()
            rating = st.get("setup_score", {}).get("rating", "?")
            if state == "SWEEP_CONFIRMED":
                age = int(state_age(st, "sweep_time") / 60)
                lines.append(
                    f"  {sym.replace('_','/')}: Sweep confirmed ({bias})"
                    f" -- waiting for BOS [{age}min ago]"
                )
            elif state == "WAITING_FOR_RETRACEMENT":
                age = int(state_age(st, "bos_time") / 60)
                ote = st.get("ote", {})
                lines.append(
                    f"  {sym.replace('_','/')}: BOS confirmed ({bias}) {rating}"
                    f" -- waiting for retrace to"
                    f" {ote.get('ote_low',0):.4f}-{ote.get('ote_high',0):.4f}"
                    f" [{age}min ago]"
                )
        lines.append("")

    if rejected:
        lines.append("<b>-- RECENT REJECTIONS (why no trade) --</b>")
        for sym, rej in rejected:
            lines.append(
                f"  {sym.replace('_','/')}: {rej['step']}"
                f" -- {rej['reason']} [{rej['time']}]"
            )
        lines.append("")

    if scored_pairs:
        lines.append("<b>-- TOP PAIRS THIS CYCLE --</b>")
        for p in scored_pairs[:8]:
            sym = p["symbol"].replace("_", "/")
            lines.append(
                f"  {sym}: score {p['score']:.0f}"
                f" | {p['change_pct']:+.1f}%"
                f" | {p['reason']}"
            )
        lines.append("")

    lines.append(f"Next scan in ~{SCAN_INTERVAL_KZ // 60} min")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# STATE MACHINE
# ═══════════════════════════════════════════════════════════════════════════

def run_state_machine(sym, m15, h4, daily, bd, loc_data, structure, pair_score):
    st   = get_state(sym)
    bias = bd["bias"]
    loc  = loc_data["location"]
    pdh  = bd["pdh"]
    pdl  = bd["pdl"]
    now  = time.time()

    # Bias flip resets narrative
    if st["state"] != "WAITING_FOR_SWEEP":
        if st.get("bias", bias) != bias:
            reset_state(sym, f"Bias flipped {st.get('bias')} to {bias}")
            return

    # ── WAITING_FOR_SWEEP ────────────────────────────────────────────────────
    if st["state"] == "WAITING_FOR_SWEEP":
        sweep = detect_true_sweep(m15, bias, pdh, pdl)
        if not sweep["swept"]:
            record_rejection(sym, "Step 4 (Sweep)", sweep.get("reason", "No valid sweep"))
            log.info(f"{sym}: [WAITING_FOR_SWEEP] {sweep.get('reason', 'no sweep')}")
            return

        st.update({
            "state":      "SWEEP_CONFIRMED",
            "bias":       bias,
            "sweep":      sweep,
            "sweep_time": now,
            "bd":         bd,
            "loc_data":   loc_data,
            "pair_score": pair_score,
        })

        stype  = sweep["sweep_type"].replace("_", " ").title()
        eq_tag = " | Equal H/L" if sweep.get("is_equal_hl") else ""
        log.info(f"{sym}: SWEEP_CONFIRMED ({stype}{eq_tag})")
        send_telegram(
            f"<b>Sweep detected -- {sym.replace('_', '/')} {bias.upper()}</b>\n"
            f"Type: {stype}{eq_tag}\n"
            f"Level: {sweep.get('sweep_level', 0):.4f}\n"
            f"<i>Waiting for displacement + BOS...</i>"
        )

    # ── SWEEP_CONFIRMED ───────────────────────────────────────────────────────
    if st["state"] == "SWEEP_CONFIRMED":
        if state_age(st, "sweep_time") > MAX_SWEEP_AGE:
            mins = MAX_SWEEP_AGE // 60
            record_rejection(sym, "Step 6 (BOS)", f"Sweep expired after {mins}min -- no BOS")
            reset_state(sym, f"Sweep expired after {mins}min")
            send_telegram(
                f"<i>Sweep expired -- {sym.replace('_', '/')} {st.get('bias', bias).upper()}\n"
                f"No BOS within {mins} minutes. Structure reset.</i>"
            )
            return

        opp_bias  = "bearish" if bias == "bullish" else "bullish"
        opp_sweep = detect_true_sweep(m15, opp_bias, pdh, pdl)
        if opp_sweep["swept"] and \
           opp_sweep["age_candles"] < st["sweep"].get("age_candles", 999):
            record_rejection(sym, "Step 4 (Sweep)", "Opposite sweep invalidated structure")
            reset_state(sym, "Opposite sweep -- setup invalidated")
            send_telegram(
                f"<b>INVALIDATED -- {sym.replace('_', '/')} {bias.upper()}</b>\n"
                f"Opposite sweep occurred. Previous structure broken.\n"
                f"<i>Waiting for new setup.</i>"
            )
            return

        sweep = st["sweep"]
        si    = sweep["candle_idx"]
        disp  = detect_displacement(m15, bias, si)
        if not disp["valid"]:
            record_rejection(sym, "Step 5 (Displacement)", disp["reason"])
            log.info(f"{sym}: [SWEEP_CONFIRMED] {disp['reason']}")
            return

        bos = detect_bos(m15, bias, si, disp["avg_body"], loc)
        if not bos["broken"]:
            record_rejection(sym, "Step 6 (BOS)", bos.get("reason", "BOS not confirmed"))
            log.info(f"{sym}: [SWEEP_CONFIRMED] {bos.get('reason', 'no BOS')}")
            return

        mq  = classify_move_quality(disp)
        ote = calc_ote(m15, bias, si, bos["bos_level"], mq)
        if not ote:
            record_rejection(sym, "Step 7 (OTE)", "OTE calculation failed")
            reset_state(sym, "OTE calc failed after BOS")
            return

        fvg_ote     = fvg_inside_ote(m15, bias, ote["ote_high"], ote["ote_low"])
        setup_score = score_setup(sweep, disp, bos, fvg_ote, structure)
        risk        = calc_risk(bias, sweep, bos, ote, m15=m15, h4=h4, daily=daily)
        if not risk:
            record_rejection(sym, "Step 9 (Risk)", "Risk calculation failed")
            reset_state(sym, "Risk calc failed after BOS")
            return

        st.update({
            "state":       "WAITING_FOR_RETRACEMENT",
            "disp":        disp,
            "bos":         bos,
            "bos_time":    now,
            "ote":         ote,
            "fvg_ote":     fvg_ote,
            "setup_score": setup_score,
            "risk":        risk,
            "checks":      0,
            "last_price":  0.0,
        })

        log.info(
            f"{sym}: WAITING_FOR_RETRACEMENT | {setup_score['rating']} | "
            f"OTE {ote['ote_low']:.4f}-{ote['ote_high']:.4f} ({mq})"
        )

        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        key       = f"{sym}_{bias}_{today_str}"
        if key not in alerted_today:
            alerted_today.add(key)
            msg = build_setup_alert(
                sym, bd, loc_data, sweep, disp, bos,
                fvg_ote, ote, risk, setup_score, pair_score
            )
            send_telegram(msg)

    # ── WAITING_FOR_RETRACEMENT ───────────────────────────────────────────────
    if st["state"] == "WAITING_FOR_RETRACEMENT":
        if state_age(st, "bos_time") > MAX_BOS_AGE:
            ote  = st.get("ote", {})
            mins = MAX_BOS_AGE // 60
            record_rejection(
                sym, "Step 7 (OTE retracement)",
                f"Price never retraced to OTE in {mins}min"
            )
            reset_state(sym, f"OTE expired after {mins}min")
            send_telegram(
                f"<i>Setup expired -- {sym.replace('_', '/')} {st.get('bias', bias).upper()}\n"
                f"Price never retraced to OTE "
                f"{ote.get('ote_low', 0):.4f}-{ote.get('ote_high', 0):.4f}\n"
                f"Structure reset. Waiting for next setup.</i>"
            )
            return

        price     = get_price(sym)
        if price == 0:
            return

        sweep_lvl = st["sweep"].get("sweep_low", st["sweep"].get("sweep_high", 0))

        if bias == "bullish" and sweep_lvl > 0 and price < sweep_lvl * 0.999:
            record_rejection(sym, "Step 4 (Invalidation)", f"Broke below sweep extreme {sweep_lvl:.4f}")
            reset_state(sym, "Broke below sweep extreme")
            send_telegram(
                f"<b>INVALIDATED -- {sym.replace('_', '/')} {bias.upper()}</b>\n"
                f"Broke below sweep extreme {sweep_lvl:.4f}.\n"
                f"Structure broken. New setup needed."
            )
            return

        if bias == "bearish" and sweep_lvl > 0 and price > sweep_lvl * 1.001:
            record_rejection(sym, "Step 4 (Invalidation)", f"Broke above sweep extreme {sweep_lvl:.4f}")
            reset_state(sym, "Broke above sweep extreme")
            send_telegram(
                f"<b>INVALIDATED -- {sym.replace('_', '/')} {bias.upper()}</b>\n"
                f"Broke above sweep extreme {sweep_lvl:.4f}.\n"
                f"Structure broken. New setup needed."
            )
            return

        ote     = st["ote"]
        risk    = st["risk"]
        in_zone = ote["ote_low"] <= price <= ote["ote_high"]

        if not in_zone:
            # ── v3: Model A not reached -- try Model B (momentum entry) ──
            mom = detect_momentum_entry(m15, bias, bos.get("bos_level", 0))
            if mom["valid"]:
                log.info(f"{sym}: Model B triggered -- {mom['reason']}")
                # Build Model B risk using current price as entry
                risk_b = dict(risk)
                risk_b["entry"] = price
                # Adjust SL to recent swing extreme
                recent_slice = m15[-10:]
                if bias == "bullish":
                    risk_b["sl"] = min(c["low"] for c in recent_slice) * 0.9995
                else:
                    risk_b["sl"] = max(c["high"] for c in recent_slice) * 1.0005
                send_telegram(
                    f"<b>ENTRY CONFIRMED (Model B) -- {sym.replace('_', '/')} {bias.upper()}</b>\n\n"
                    f"<i>{mom['reason']}</i>\n"
                    f"Price: <b>{price:.4f}</b>  (no OTE retrace -- momentum entry)\n\n"
                    f"Entry:        <b>{risk_b['entry']:.4f}</b>\n"
                    f"SL:           <b>{risk_b['sl']:.4f}</b>  (recent swing extreme)\n"
                    f"TP1 (50%):    <b>{risk_b['tp1']:.4f}</b>  [{risk_b.get('tp1_label', '--')}]\n"
                    f"TP2 (30%):    <b>{risk_b['tp2']:.4f}</b>  [{risk_b.get('tp2_label', '--')}]\n"
                    f"Runner (20%): <b>{risk_b['runner']:.4f}</b>  [{risk_b.get('runner_label', '--')}]\n\n"
                    f"Rating: <b>{st['setup_score']['rating']}</b>\n"
                    f"Core confirmed: Bias ✔  Sweep ✔  BOS ✔\n"
                    f"<b>Execute now. Risk 1-2% only.</b>"
                )
                log.info(f"{sym}: Model B ENTRY CONFIRMED at {price:.4f}")
                st["state"] = "TRADE_TAKEN"
                return

            # Model B also not valid -- log and wait
            record_rejection(
                sym, "Step 7 (OTE + Model B)",
                f"Price {price:.4f} not in OTE "
                f"{ote['ote_low']:.4f}-{ote['ote_high']:.4f} "
                f"and no momentum pattern -- {mom['reason']}"
            )
            log.info(
                f"{sym}: [WAITING_FOR_RETRACEMENT] price {price:.4f} not in OTE, "
                f"Model B: {mom['reason']}"
            )
            return

        # ── Model A: price is inside OTE zone ──
        st["checks"]     = st.get("checks", 0) + 1
        prev             = st.get("last_price", price)
        thr              = 0.001
        reacted          = (bias == "bullish" and price > prev * (1 + thr)) or \
                           (bias == "bearish" and price < prev * (1 - thr))
        st["last_price"] = price

        if reacted:
            fvg_note = ""
            fvg_ote  = st.get("fvg_ote", {})
            if fvg_ote and fvg_ote.get("found"):
                fvg_note = (
                    f"\nFVG in OTE: {fvg_ote['fvg_low']:.4f} – {fvg_ote['fvg_high']:.4f}"
                    f"  (preferred entry zone)"
                )
            send_telegram(
                f"<b>ENTRY CONFIRMED (Model A) -- {sym.replace('_', '/')} {bias.upper()}</b>\n\n"
                f"Reaction in OTE at <b>{price:.4f}</b>{fvg_note}\n\n"
                f"Entry:        <b>{risk['entry']:.4f}</b>\n"
                f"SL:           <b>{risk['sl']:.4f}</b>\n"
                f"TP1 (50%):    <b>{risk['tp1']:.4f}</b>  [{risk.get('tp1_label', '--')}]\n"
                f"TP2 (30%):    <b>{risk['tp2']:.4f}</b>  [{risk.get('tp2_label', '--')}]\n"
                f"Runner (20%): <b>{risk['runner']:.4f}</b>  [{risk.get('runner_label', '--')}]\n\n"
                f"Rating: <b>{st['setup_score']['rating']}</b>\n"
                f"<b>Execute now. Risk 1-2% only.</b>"
            )
            log.info(f"{sym}: Model A ENTRY CONFIRMED at {price:.4f}")
            st["state"] = "TRADE_TAKEN"
            return

        if st["checks"] >= 3:
            record_rejection(sym, "Step 7 (OTE no reaction)", "3 candles in zone -- no reaction, skipped")
            reset_state(sym, "3-candle skip rule triggered")
            send_telegram(
                f"<b>SKIP -- {sym.replace('_', '/')} {bias.upper()}</b>\n\n"
                f"3 checks in OTE at {price:.4f} -- no reaction.\n"
                f"<i>Rule: 3 candles, no reaction = skip. Do not chase. State reset.</i>"
            )
            return

        log.info(f"{sym}: [WAITING_FOR_RETRACEMENT] Model A check {st['checks']}/3 at {price:.4f}")

    # ── TRADE_TAKEN ───────────────────────────────────────────────────────────
    if st["state"] == "TRADE_TAKEN":
        reset_state(sym, "Trade complete -- ready for next setup")


# ═══════════════════════════════════════════════════════════════════════════
# MAIN SCAN
# ═══════════════════════════════════════════════════════════════════════════

def scan_pair(sym, pair_score):
    daily = get_candles(sym, "Day1",  30)
    h4    = get_candles(sym, "Hour4", 50)
    m15   = get_candles(sym, "Min15", 120)
    if not daily or not h4 or not m15:
        record_rejection(sym, "Data", "Missing candle data from MEXC API")
        log.warning(f"{sym}: missing data")
        return

    bd   = daily_bias(daily)
    bias = bd["bias"]
    if bias == "neutral":
        st = get_state(sym)
        if st["state"] != "WAITING_FOR_SWEEP":
            reset_state(sym, "Bias went neutral")
        record_rejection(sym, "Step 1 (Daily Bias)",
                         f"Neutral bias (score {bd['score']})")
        log.info(f"{sym}: neutral bias (score {bd['score']})")
        return

    loc_data         = location_4h(h4)
    loc              = loc_data["location"]
    location_aligned = not (
        (bias == "bullish" and loc == "premium") or
        (bias == "bearish" and loc == "discount")
    )
    if loc == "unknown":
        record_rejection(sym, "Step 2 (4H Location)", "Cannot determine 4H location")
        log.info(f"{sym}: location unknown")
        return
    # v3: location is a GUIDELINE, not a hard block. Misaligned trades still
    # proceed -- BOS strength + displacement quality will decide the alert.
    if not location_aligned:
        log.info(f"{sym}: location misaligned ({bias} in {loc}) -- proceeding per v3 (BOS decides)")

    st = get_state(sym)
    if st["state"] == "WAITING_FOR_SWEEP":
        structure = is_clean_structure(m15)
        if not structure["clean"]:
            record_rejection(sym, "Step 3 (Structure)", structure["reason"])
            log.info(f"{sym}: {structure['reason']}")
            return
    else:
        structure = {"clean": True, "reason": "In active setup -- filter bypassed"}

    run_state_machine(sym, m15, h4, daily, bd, loc_data, structure, pair_score)


def scan_all():
    log.info("=== Scan cycle ===")
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if not hasattr(scan_all, "_day") or scan_all._day != today_str:
        alerted_today.clear()
        scan_all._day = today_str
        log.info("New day -- dedup reset")

    tickers = get_all_tickers()
    if not tickers:
        log.warning("No tickers fetched")
        return []

    log.info(f"Total USDT futures: {len(tickers)}")
    all_scored = score_pairs(tickers)
    scored_map = {s["symbol"]: s for s in all_scored}
    pairs      = select_pairs(tickers, TOP_N_PAIRS)

    # Always include pairs already in an active state even if not top-N
    active_syms = [sym for sym, st in pair_states.items()
                   if st.get("state") != "WAITING_FOR_SWEEP"]
    all_pairs   = list(dict.fromkeys(pairs + active_syms))
    if active_syms:
        log.info(f"Active state pairs also scanned: {active_syms}")

    log.info(f"Scanning {len(all_pairs)} pairs | {len(active_syms)} in active state")

    for sym in all_pairs:
        try:
            ps = scored_map.get(sym, {"score": 0, "change_pct": 0, "reason": ""})
            scan_pair(sym, ps)
            time.sleep(1.2)
        except Exception as e:
            log.error(f"{sym}: {e}")

    log.info("=== Done ===")
    return all_scored


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT -- 24/7 smart loop
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    log.info("BEE-M Alert Bot v9 starting...")

    startup_lines = [
        "<b>BEE-M Alert Bot v9 -- System v3</b>",
        "",
        "Core truth: Liquidity taken -&gt; Market shifts -&gt; Enter on retracement OR momentum",
        "",
        "<b>v3 System rules:</b>",
        "  Core (non-negotiable): Daily bias + Sweep + BOS",
        "  4H location: guideline only -- strong BOS overrides",
        "  Entry Model A: OTE 0.618-0.792 / 0.5 retracement / FVG in zone",
        "  Entry Model B (NEW): momentum -- continuation, small pullback, micro consolidation",
        "",
        "<b>State machine per pair:</b>",
        "  WAITING_FOR_SWEEP",
        "  -&gt; SWEEP_CONFIRMED  (bias + sweep level locked in memory)",
        "  -&gt; WAITING_FOR_RETRACEMENT  (BOS + OTE/momentum entry watched)",
        "  -&gt; TRADE_TAKEN  -&gt; reset",
        "  State is remembered across every scan cycle.",
        "",
        "<b>Scanning:</b>",
        "  24/7 full scan every 15 min -- no kill zones",
        "  Status digest every 2 hours",
        "  Active setups scanned every cycle regardless of pair ranking",
        "",
        "Any core rule not met = NO TRADE.",
        "Bot tells you exactly which step failed and why.",
    ]
    send_telegram("\n".join(startup_lines))

    last_scored = []

    while True:
        try:
            now = time.time()

            # Check for Telegram commands (/status, /pairs, /help)
            handle_commands()

            last_scored = scan_all()

            # Auto status digest every STATUS_INTERVAL seconds
            if now - last_status_time >= STATUS_INTERVAL:
                last_status_time = now
                digest = build_status_digest(last_scored)
                send_telegram(digest)

            # Sleep in 30s chunks so commands are answered promptly
            # even while waiting between scan cycles
            log.info(f"Sleeping {SCAN_INTERVAL_KZ}s (24/7 full scan) -- commands checked every 30s")
            slept = 0
            while slept < SCAN_INTERVAL_KZ:
                time.sleep(30)
                slept += 30
                handle_commands()

        except Exception as e:
            log.error(f"Loop error: {e}")
            send_telegram(f"Bot error: {e}")
            time.sleep(60)
