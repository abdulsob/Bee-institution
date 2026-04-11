"""
BEE-M Trading Alert Bot — v3
Upgrades over v2:
  - Dynamic pair selection: scans ALL MEXC futures pairs
  - Scores every pair on: volatility, volume, momentum, clean structure
  - Top gainers + top losers + top-scored pairs all included
  - Correct BOS rule: breaks last 15M swing HIGH (longs) or swing LOW (shorts)
  - All original rules: 8 bias scenarios, location, liquidity path,
    sweep tier, FVG in OTE, 3-candle skip rule, SL at sweep/BOS origin
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

TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
SCAN_INTERVAL    = int(os.environ.get("SCAN_INTERVAL", "900"))    # 15 min
OTE_MONITOR_SECS = int(os.environ.get("OTE_MONITOR_SECS", "720")) # 12 min
TOP_N_PAIRS      = int(os.environ.get("TOP_N_PAIRS", "30"))        # how many pairs to scan per cycle

MEXC_BASE = "https://contract.mexc.com/api/v1/contract"

alerted_today:    set  = set()
ote_watch_queue:  list = []


# ═══════════════════════════════════════════════════════════════
# DATA FETCHING
# ═══════════════════════════════════════════════════════════════

def get_all_tickers() -> list:
    """
    Fetch ALL MEXC futures tickers in one call.
    Returns list of ticker dicts with symbol, lastPrice, priceChange,
    volume24, high24, low24, etc.
    """
    try:
        r = requests.get(f"{MEXC_BASE}/ticker", timeout=15)
        r.raise_for_status()
        data = r.json()
        if not data.get("success"):
            return []
        tickers = data.get("data", [])
        # Keep only USDT-settled perpetuals
        return [t for t in tickers if str(t.get("symbol","")).endswith("_USDT")]
    except Exception as e:
        log.warning(f"get_all_tickers: {e}")
        return []


def get_candles(symbol: str, interval: str, limit: int = 100) -> list:
    try:
        r = requests.get(
            f"{MEXC_BASE}/kline/{symbol}",
            params={"interval": interval, "limit": limit},
            timeout=10,
        )
        r.raise_for_status()
        d = r.json()
        if not d.get("success"):
            return []
        raw  = d["data"]
        cols = [raw.get(k, []) for k in ["time","open","high","low","close","vol"]]
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


def get_price(symbol: str) -> float:
    try:
        r = requests.get(f"{MEXC_BASE}/ticker?symbol={symbol}", timeout=8)
        return float(r.json()["data"]["lastPrice"])
    except Exception as e:
        log.warning(f"get_price {symbol}: {e}")
        return 0.0


# ═══════════════════════════════════════════════════════════════
# PAIR SCORING ENGINE
# Scores each pair 0–100 combining:
#   1. Volatility    — ATR relative to price (range expansion)
#   2. Volume        — 24h quote volume vs median
#   3. Momentum      — 24h % change magnitude (gainers + losers)
#   4. Clean structure — low wick-to-body ratio on recent candles
# ═══════════════════════════════════════════════════════════════

def score_pairs(tickers: list) -> list:
    """
    Takes all ticker dicts, scores each one, returns sorted list
    of {symbol, score, change_pct, volume_24h, reason} dicts.
    """
    if not tickers:
        return []

    scored = []
    volumes = []
    changes = []

    # First pass — collect raw values for normalisation
    for t in tickers:
        try:
            vol    = float(t.get("volume24", t.get("amount24", 0)))
            change = abs(float(t.get("priceChangeRate", t.get("riseFallRate", 0)))) * 100
            volumes.append(vol)
            changes.append(change)
        except Exception:
            volumes.append(0); changes.append(0)

    median_vol = sorted(volumes)[len(volumes)//2] if volumes else 1
    max_change = max(changes) if changes else 1

    for i, t in enumerate(tickers):
        symbol = t.get("symbol", "")
        if not symbol:
            continue
        try:
            price     = float(t.get("lastPrice", 0))
            high24    = float(t.get("high24Price", t.get("highPrice", price)))
            low24     = float(t.get("low24Price",  t.get("lowPrice",  price)))
            vol24     = volumes[i]
            change_pct= float(t.get("priceChangeRate", t.get("riseFallRate", 0))) * 100
            abs_change= abs(change_pct)

            if price == 0 or low24 == 0:
                continue

            # ── 1. Volatility score (0–25) ────────────────────
            # Range as % of price — more range = more opportunity
            day_range_pct = (high24 - low24) / price * 100
            vol_score     = min(day_range_pct / 15 * 25, 25)  # caps at 15% range

            # ── 2. Volume score (0–25) ────────────────────────
            # vs median volume — strong volume = institutional interest
            vol_ratio  = vol24 / max(median_vol, 1)
            vol_score2 = min(vol_ratio / 5 * 25, 25)  # caps at 5x median

            # ── 3. Momentum score (0–25) ──────────────────────
            # % change magnitude — strong movers have clear direction
            mom_score = min(abs_change / max_change * 25, 25)

            # ── 4. Structure score (0–25) ─────────────────────
            # Using open/close vs high/low to estimate body dominance
            # High body % = cleaner candle = cleaner structure
            body      = abs(float(t.get("lastPrice",0)) - float(t.get("ask1",price)))
            candle_rng= high24 - low24
            body_pct  = (candle_rng * 0.6) / max(candle_rng, 0.0001)  # approximation
            struct_score = min(body_pct * 25, 25)

            total = vol_score + vol_score2 + mom_score + struct_score

            reason_parts = []
            if vol_score  > 15: reason_parts.append(f"High volatility ({day_range_pct:.1f}% range)")
            if vol_score2 > 15: reason_parts.append(f"Strong volume ({vol_ratio:.1f}x median)")
            if mom_score  > 15: reason_parts.append(f"Strong momentum ({abs_change:.1f}% move)")
            reason = " | ".join(reason_parts) if reason_parts else "Moderate setup"

            scored.append({
                "symbol":     symbol,
                "score":      round(total, 1),
                "change_pct": change_pct,
                "volume_24h": vol24,
                "price":      price,
                "reason":     reason,
            })
        except Exception as e:
            log.debug(f"score_pairs {symbol}: {e}")
            continue

    # Sort by score descending
    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored


def select_pairs(tickers: list, top_n: int = 30) -> list:
    """
    Select the best pairs to scan this cycle:
      - Top 10 by composite score
      - Top 5 daily gainers (directional opportunity)
      - Top 5 daily losers  (directional opportunity)
      - Deduplicated
    Returns list of symbol strings.
    """
    if not tickers:
        return []

    scored = score_pairs(tickers)
    if not scored:
        return []

    # Top N by composite score
    top_scored = [s["symbol"] for s in scored[:10]]

    # Top gainers and losers by raw change %
    by_change = sorted(
        [s for s in scored if s["change_pct"] != 0],
        key=lambda x: x["change_pct"]
    )
    top_losers  = [s["symbol"] for s in by_change[:5]]       # most negative
    top_gainers = [s["symbol"] for s in by_change[-5:]]      # most positive

    # Combine and deduplicate preserving order
    seen  = set()
    pairs = []
    for sym in top_scored + top_gainers + top_losers:
        if sym not in seen:
            seen.add(sym)
            pairs.append(sym)

    log.info(f"Selected {len(pairs)} pairs this cycle")
    log.info(f"  Top scored: {top_scored[:5]}")
    log.info(f"  Top gainers: {top_gainers}")
    log.info(f"  Top losers: {top_losers}")

    return pairs[:top_n]


# ═══════════════════════════════════════════════════════════════
# FVG HELPERS
# ═══════════════════════════════════════════════════════════════

def find_fvgs(candles: list) -> list:
    fvgs = []
    for i in range(len(candles) - 2):
        c0, c2 = candles[i], candles[i+2]
        if c2["low"] > c0["high"]:
            fvgs.append({"direction":"bullish","fvg_high":c2["low"],"fvg_low":c0["high"],"idx":i})
        elif c2["high"] < c0["low"]:
            fvgs.append({"direction":"bearish","fvg_high":c0["low"],"fvg_low":c2["high"],"idx":i})
    return fvgs


def unmitigated_daily_fvg(daily: list) -> tuple:
    if len(daily) < 5:
        return None, None, None
    price = daily[-1]["close"]
    for fvg in reversed(find_fvgs(daily[:-1])):
        if fvg["direction"] == "bullish" and price < fvg["fvg_low"]:
            return "bullish", fvg["fvg_high"], fvg["fvg_low"]
        if fvg["direction"] == "bearish" and price > fvg["fvg_high"]:
            return "bearish", fvg["fvg_high"], fvg["fvg_low"]
    return None, None, None


def fvg_inside_ote(m15: list, bias: str, ote_high: float, ote_low: float) -> dict:
    recent = m15[-20:] if len(m15) >= 20 else m15
    for fvg in reversed(find_fvgs(recent)):
        if fvg["direction"] == bias:
            if fvg["fvg_low"] < ote_high and fvg["fvg_high"] > ote_low:
                return {"found":True,"fvg_high":fvg["fvg_high"],"fvg_low":fvg["fvg_low"]}
    return {"found":False}


# ═══════════════════════════════════════════════════════════════
# STEP 1 — DAILY BIAS (all 8 scenarios)
# ═══════════════════════════════════════════════════════════════

def daily_bias(daily: list) -> dict:
    if len(daily) < 3:
        return {"bias":"neutral","score":0,"reasons":[],"pdh":0,"pdl":0,"mid":0}

    prev, today, prev2 = daily[-2], daily[-1], daily[-3]
    pdh   = prev["high"]
    pdl   = prev["low"]
    mid   = (pdh + pdl) / 2
    score = 0
    reasons = []

    # Sc1 — Sweep reversal
    if prev["high"] > prev2["high"] and prev["close"] < prev2["high"]:
        score -= 1; reasons.append("Sc1 Bear: Prev swept prior high, closed inside")
    if prev["low"] < prev2["low"] and prev["close"] > prev2["low"]:
        score += 1; reasons.append("Sc1 Bull: Prev swept prior low, closed inside")

    # Sc2 + Sc3 — Open location
    o = today["open"]
    if   o > pdh: score += 1; reasons.append("Sc2 Bull: Opened above PDH")
    elif o < pdl: score -= 1; reasons.append("Sc2 Bear: Opened below PDL")
    else:                      reasons.append("Sc3 Neutral: Inside PD range — need sweep")

    # Sc4 — Gap continuation
    if o > pdh and today["low"]  > pdh: score += 1; reasons.append("Sc4 Bull: Gap above PDH, lows holding")
    if o < pdl and today["high"] < pdl: score -= 1; reasons.append("Sc4 Bear: Gap below PDL, highs holding")

    # Sc5 — Previous day close
    if   prev["close"] > pdh: score += 1; reasons.append("Sc5 Bull: Yesterday closed above PDH")
    elif prev["close"] < pdl: score -= 1; reasons.append("Sc5 Bear: Yesterday closed below PDL")
    if prev["high"] > prev2["high"] and pdl < prev["close"] < prev2["high"]:
        score -= 1; reasons.append("Sc5 Bear: Swept high, closed back inside")
    if prev["low"] < prev2["low"] and prev2["low"] < prev["close"] < pdh:
        score += 1; reasons.append("Sc5 Bull: Swept low, closed back inside")

    # Sc6 — Midpoint
    if   today["close"] > mid: score += 1; reasons.append(f"Sc6 Bull: Above midpoint ({mid:.4f})")
    else:                       score -= 1; reasons.append(f"Sc6 Bear: Below midpoint ({mid:.4f})")

    # Sc7 — HTF order flow (5 candle trend)
    if len(daily) >= 6:
        r5 = daily[-6:-1]
        hs = [c["high"] for c in r5]
        ls = [c["low"]  for c in r5]
        if all(hs[i]<hs[i+1] for i in range(len(hs)-1)) and \
           all(ls[i]<ls[i+1] for i in range(len(ls)-1)):
            score += 2; reasons.append("Sc7 Bull: Daily HH+HL — bullish order flow")
        elif all(hs[i]>hs[i+1] for i in range(len(hs)-1)) and \
             all(ls[i]>ls[i+1] for i in range(len(ls)-1)):
            score -= 2; reasons.append("Sc7 Bear: Daily LH+LL — bearish order flow")

    # Sc8 — Unmitigated daily FVG
    fd, fh, fl = unmitigated_daily_fvg(daily)
    if   fd == "bullish": score += 1; reasons.append(f"Sc8 Bull: Daily FVG above draws price up ({fl:.4f}–{fh:.4f})")
    elif fd == "bearish": score -= 1; reasons.append(f"Sc8 Bear: Daily FVG below draws price down ({fl:.4f}–{fh:.4f})")

    bias = "bullish" if score >= 2 else "bearish" if score <= -2 else "neutral"
    return {"bias":bias,"score":score,"reasons":reasons,"pdh":pdh,"pdl":pdl,"mid":mid}


# ═══════════════════════════════════════════════════════════════
# STEP 2 — 4H LOCATION
# ═══════════════════════════════════════════════════════════════

def location_4h(h4: list) -> str:
    if len(h4) < 20:
        return "unknown"
    r  = h4[-20:]
    sh = max(c["high"] for c in r)
    sl = min(c["low"]  for c in r)
    eq = (sh + sl) / 2
    p  = h4[-1]["close"]
    return "premium" if p > eq else "discount" if p < eq else "equilibrium"


# ═══════════════════════════════════════════════════════════════
# STEP 3 — LIQUIDITY PATH + SWEEP TIER
# ═══════════════════════════════════════════════════════════════

def classify_sweep_tier(bias: str, pdh: float, pdl: float,
                         sweep_lvl: float, h4: list) -> dict:
    tol = 0.002
    if bias == "bullish" and abs(sweep_lvl - pdl) / max(pdl, 1) < tol:
        return {"tier":2,"label":"Tier 2 — PDL (major level)","conviction":"HIGH"}
    if bias == "bearish" and abs(sweep_lvl - pdh) / max(pdh, 1) < tol:
        return {"tier":2,"label":"Tier 2 — PDH (major level)","conviction":"HIGH"}
    if len(h4) >= 10:
        lvls = [c["low"] for c in h4[-15:]] if bias=="bullish" else [c["high"] for c in h4[-15:]]
        for lvl in lvls:
            if abs(sweep_lvl - lvl) / max(lvl, 1) < tol:
                return {"tier":3,"label":"Tier 3 — Equal highs/lows / 4H swing","conviction":"HIGH"}
    return {"tier":1,"label":"Tier 1 — Micro recent level","conviction":"MODERATE"}


def check_liq_path(h4: list, bias: str) -> dict:
    if len(h4) < 10:
        return {"clear":False,"reason":"Not enough 4H data"}
    r  = h4[-10:]
    p  = h4[-1]["close"]
    nh = max(c["high"] for c in r)
    nl = min(c["low"]  for c in r)
    dh = abs(nh - p)
    dl = abs(p  - nl)
    if bias == "bullish":
        if dl < dh * 0.5:
            return {"clear":False,"reason":f"Opposing liquidity below ({nl:.4f}) closer — wait for sweep"}
        return {"clear":True,"target":nh,"reason":f"Clear path to liquidity above at {nh:.4f}"}
    if bias == "bearish":
        if dh < dl * 0.5:
            return {"clear":False,"reason":f"Opposing liquidity above ({nh:.4f}) closer — wait for sweep"}
        return {"clear":True,"target":nl,"reason":f"Clear path to liquidity below at {nl:.4f}"}
    return {"clear":False,"reason":"No bias"}


# ═══════════════════════════════════════════════════════════════
# STEP 4 — 15M CONFIRMATION
# ── MEANINGFUL BOS — 5-CRITERIA EXPANSION BOS ────────────────
#
# A valid BOS must satisfy ALL of the following:
#
#   1. RELEVANT STRUCTURE  — breaks the most recent swing high/low
#                            that caused a real prior reaction,
#                            not random noise candles.
#
#   2. CLOSE BEYOND LEVEL  — candle BODY must close beyond the level.
#                            Wicks are traps. Close = confirmation.
#
#   3. DISPLACEMENT         — the breaking candle (or series) must be:
#                            • Body >= 2x average body (expansion range)
#                            • Leaves a Fair Value Gap (imbalance)
#                            • NOT slow, choppy, or wick-heavy
#                            This separates EXPANSION BOS from INDUCEMENT BOS.
#
#   4. LIQUIDITY CONTEXT   — BOS must happen AFTER liquidity is taken.
#                            Long:  sweep of sell-side (PDL/equal lows) first
#                            Short: sweep of buy-side (PDH/equal highs) first
#                            BOS without prior sweep = weak, rejected.
#
#   5. LOCATION ALIGNMENT  — BOS must occur from premium/discount zone.
#                            Long BOS from discount. Short BOS from premium.
#                            BOS in middle of range = low probability.
#
# Sequence: Sweep → Displacement → Expansion BOS (strict order)
# ═══════════════════════════════════════════════════════════════

def find_swing_points(candles: list, strength: int = 2) -> list:
    """
    Find all swing highs and lows in a candle list.
    strength = how many candles on each side must be lower/higher.
    Returns list of {type:'high'|'low', price, idx, candle}.
    A higher strength value = only major swings qualify (less noise).
    """
    swings = []
    n = len(candles)
    for i in range(strength, n - strength):
        c = candles[i]
        # Swing high: higher than `strength` candles on each side
        is_sh = all(c["high"] >= candles[i-j]["high"] for j in range(1, strength+1)) and \
                all(c["high"] >= candles[i+j]["high"] for j in range(1, strength+1))
        # Swing low: lower than `strength` candles on each side
        is_sl = all(c["low"] <= candles[i-j]["low"] for j in range(1, strength+1)) and \
                all(c["low"] <= candles[i+j]["low"] for j in range(1, strength+1))
        if is_sh:
            swings.append({"type":"high","price":c["high"],"idx":i,"candle":c})
        if is_sl:
            swings.append({"type":"low","price":c["low"],"idx":i,"candle":c})
    return swings


def find_most_recent_relevant_swing(candles: list, bias: str) -> dict:
    """
    Find the most recent swing point that caused a REAL prior reaction.
    'Real reaction' = the candle at that swing had a body >= 1.2x avg body,
    meaning there was genuine intent at that level (not noise).

    For LONGS  → find the most recent swing HIGH (the level BOS must break)
    For SHORTS → find the most recent swing LOW  (the level BOS must break)

    Returns {found, price, idx} or {found: False}
    """
    if len(candles) < 6:
        return {"found": False}

    # Average body size for context
    avg_body = sum(abs(c["close"] - c["open"]) for c in candles) / len(candles)
    if avg_body == 0:
        return {"found": False}

    swings = find_swing_points(candles, strength=2)
    if not swings:
        return {"found": False}

    target_type = "high" if bias == "bullish" else "low"

    # Walk backwards to find the most recent RELEVANT swing of the right type
    for sw in reversed(swings):
        if sw["type"] != target_type:
            continue
        c    = sw["candle"]
        body = abs(c["close"] - c["open"])
        # Level is relevant if the candle at that swing showed real intent
        # (body at least 80% of average — filters out tiny indecision wicks)
        if body >= avg_body * 0.8:
            return {"found": True, "price": sw["price"], "idx": sw["idx"]}

    # Fallback: use any swing of the right type (less strict)
    for sw in reversed(swings):
        if sw["type"] == target_type:
            return {"found": True, "price": sw["price"], "idx": sw["idx"]}

    return {"found": False}


def is_expansion_bos_candle(candle: dict, avg_body: float,
                              bias: str, prev_candle: dict) -> dict:
    """
    Check if a candle qualifies as an EXPANSION BOS candle.
    Rejects INDUCEMENT BOS (weak, slow, choppy break).

    Criteria:
      - Body >= 2x average (strong impulsive move)
      - Close in the right direction (no reversal wick eating body)
      - Low wick ratio: body is dominant (not wick-heavy)
      - Leaves imbalance vs previous candle (FVG signature)

    Returns {valid, reason} — reason explains the rejection if invalid.
    """
    body      = abs(candle["close"] - candle["open"])
    full_rng  = candle["high"] - candle["low"]
    if full_rng == 0:
        return {"valid": False, "reason": "Zero range candle"}

    # 1. Body must be >= 2x average body (expansion, not grind)
    if body < avg_body * 2.0:
        return {"valid": False, "reason": f"Body too small ({body:.4f} < 2x avg {avg_body*2:.4f}) — inducement BOS"}

    # 2. Body must dominate the candle (> 55% of full range)
    body_ratio = body / full_rng
    if body_ratio < 0.55:
        return {"valid": False, "reason": f"Wick-heavy candle (body ratio {body_ratio:.2f}) — choppy break"}

    # 3. Close must be in the right direction (not reversed)
    if bias == "bullish" and candle["close"] <= candle["open"]:
        return {"valid": False, "reason": "Bearish candle body — no bullish expansion"}
    if bias == "bearish" and candle["close"] >= candle["open"]:
        return {"valid": False, "reason": "Bullish candle body — no bearish expansion"}

    # 4. Check for imbalance vs previous candle (FVG signature)
    # Bullish imbalance: this candle's low > previous candle's high (gap up)
    # Bearish imbalance: this candle's high < previous candle's low (gap down)
    leaves_imbalance = False
    if prev_candle:
        if bias == "bullish" and candle["low"] > prev_candle["high"]:
            leaves_imbalance = True
        if bias == "bearish" and candle["high"] < prev_candle["low"]:
            leaves_imbalance = True

    return {
        "valid":            True,
        "leaves_imbalance": leaves_imbalance,
        "body_ratio":       round(body_ratio, 2),
        "reason":           "Expansion BOS confirmed",
    }


def detect_sweep(m15: list, bias: str, pdh: float, pdl: float) -> dict:
    """
    Criterion 4 — Liquidity context:
    Wick must pierce PDL (bull) or PDH (bear) and CLOSE back inside.
    Wicks are traps. Body must confirm.
    """
    if len(m15) < 5:
        return {"swept": False}
    for i, c in enumerate(m15[-10:-1]):
        idx = len(m15) - 10 + i
        if bias == "bullish" and c["low"] < pdl and c["close"] > pdl:
            return {"swept": True, "sweep_low": c["low"], "candle_idx": idx}
        if bias == "bearish" and c["high"] > pdh and c["close"] < pdh:
            return {"swept": True, "sweep_high": c["high"], "candle_idx": idx}
    return {"swept": False}


def detect_displacement(m15: list, bias: str, after_idx: int) -> dict:
    """
    Criterion 3 (initial check) — strong impulsive candle right after sweep.
    Returns {valid, avg_body} so avg_body can be reused in BOS check.
    """
    prior = m15[max(0, after_idx - 10): after_idx]
    if not prior:
        return {"valid": False, "avg_body": 0}
    avg = sum(abs(c["close"] - c["open"]) for c in prior) / len(prior)
    if avg == 0:
        return {"valid": False, "avg_body": 0}
    ni = after_idx + 1
    if ni >= len(m15):
        return {"valid": False, "avg_body": avg}
    dc   = m15[ni]
    body = abs(dc["close"] - dc["open"])
    full = dc["high"] - dc["low"]
    body_ratio = body / full if full > 0 else 0

    # Must be >= 1.8x avg AND body-dominant (not wick-heavy)
    strong = body >= avg * 1.8 and body_ratio >= 0.5
    if bias == "bullish":
        valid = strong and dc["close"] > dc["open"]
    elif bias == "bearish":
        valid = strong and dc["close"] < dc["open"]
    else:
        valid = False

    return {"valid": valid, "avg_body": avg}


def detect_bos(m15: list, bias: str, after_idx: int, avg_body: float,
               location: str) -> dict:
    """
    ── MEANINGFUL BOS — ALL 5 CRITERIA ──────────────────────────

    Criterion 1: Breaks RELEVANT structure (most recent swing high/low
                 that caused a real prior reaction — not noise).

    Criterion 2: Candle CLOSES beyond the level (no wick fakeouts).

    Criterion 3: Breaking candle is EXPANSION (body >= 2x avg, body-dominant,
                 leaves imbalance/FVG) — rejects INDUCEMENT BOS.

    Criterion 4: Already satisfied by detect_sweep() running first.
                 BOS only checked AFTER liquidity sweep confirmed.

    Criterion 5: Location — BOS must occur from discount (longs)
                 or premium (shorts), not mid-range.
    """
    pre_sweep  = m15[max(0, after_idx - 20): after_idx]
    post_sweep = m15[after_idx + 1:]

    if not pre_sweep or not post_sweep:
        return {"broken": False, "reason": "Not enough candles"}

    # ── Criterion 1: Find the relevant swing level ────────────
    swing = find_most_recent_relevant_swing(pre_sweep, bias)
    if not swing["found"]:
        return {"broken": False, "reason": "No relevant swing structure found — noise only"}

    level = swing["price"]

    # ── Criterion 5: Location filter ─────────────────────────
    # BOS from mid-range = low probability — reject
    if location == "equilibrium":
        return {"broken": False, "reason": "BOS in middle of 4H range — low probability location"}

    if bias == "bullish" and location == "premium":
        return {"broken": False, "reason": "Bullish BOS in premium zone — misaligned"}

    if bias == "bearish" and location == "discount":
        return {"broken": False, "reason": "Bearish BOS in discount zone — misaligned"}

    # ── Criteria 2 + 3: Check post-sweep candles ─────────────
    for i, c in enumerate(post_sweep[-8:]):
        prev_c = post_sweep[i - 1] if i > 0 else pre_sweep[-1]

        if bias == "bullish":
            # Criterion 2: CLOSE above the swing high
            if c["close"] <= level:
                continue
            # Criterion 3: Must be expansion, not inducement
            exp = is_expansion_bos_candle(c, avg_body, bias, prev_c)
            if not exp["valid"]:
                return {
                    "broken":  False,
                    "reason":  f"Inducement BOS rejected — {exp['reason']}",
                }
            return {
                "broken":          True,
                "bos_level":       level,
                "bos_origin":      level,
                "bos_type":        "Broke last 15M swing HIGH (expansion)",
                "leaves_imbalance": exp["leaves_imbalance"],
                "body_ratio":      exp["body_ratio"],
                "reason":          "Expansion BOS — all 5 criteria met",
            }

        elif bias == "bearish":
            # Criterion 2: CLOSE below the swing low
            if c["close"] >= level:
                continue
            # Criterion 3: Must be expansion
            exp = is_expansion_bos_candle(c, avg_body, bias, prev_c)
            if not exp["valid"]:
                return {
                    "broken":  False,
                    "reason":  f"Inducement BOS rejected — {exp['reason']}",
                }
            return {
                "broken":          True,
                "bos_level":       level,
                "bos_origin":      level,
                "bos_type":        "Broke last 15M swing LOW (expansion)",
                "leaves_imbalance": exp["leaves_imbalance"],
                "body_ratio":      exp["body_ratio"],
                "reason":          "Expansion BOS — all 5 criteria met",
            }

    return {"broken": False, "reason": "No candle closed beyond swing level"}


# ═══════════════════════════════════════════════════════════════
# STEP 5 — OTE ZONE (0.618 – 0.792 fib on BOS leg)
# ═══════════════════════════════════════════════════════════════

def calc_ote(m15: list, bias: str, bos_idx: int) -> dict:
    leg = m15[max(0, bos_idx - 5): bos_idx + 2]
    if not leg:
        return {}
    lh  = max(c["high"] for c in leg)
    ll  = min(c["low"]  for c in leg)
    rng = lh - ll
    if rng == 0:
        return {}
    if bias == "bullish":
        return {
            "ote_high": lh - 0.618 * rng,
            "ote_low":  lh - 0.792 * rng,
            "ideal":    lh - 0.705 * rng,
            "leg_high": lh, "leg_low": ll,
        }
    if bias == "bearish":
        return {
            "ote_low":  ll + 0.618 * rng,
            "ote_high": ll + 0.792 * rng,
            "ideal":    ll + 0.705 * rng,
            "leg_high": lh, "leg_low": ll,
        }
    return {}


# ═══════════════════════════════════════════════════════════════
# STEP 6 — RISK LEVELS
# SL = sweep extreme OR BOS origin — whichever is further (cleaner)
# ═══════════════════════════════════════════════════════════════

def calc_risk(bias: str, sweep: dict, bos: dict, ote: dict) -> dict:
    if not ote:
        return {}
    entry = ote.get("ideal", 0)
    if not entry:
        return {}
    if bias == "bullish":
        sl = min(
            sweep.get("sweep_low",  entry) * 0.9995,
            bos.get("bos_origin",   entry) * 0.9995,
        )
        rr = entry - sl
        if rr <= 0: return {}
        return {
            "entry": entry, "sl": sl,
            "tp1": entry + rr*2, "tp2": entry + rr*3,
            "runner": entry + rr*5, "rr_ratio": 2.0,
        }
    if bias == "bearish":
        sl = max(
            sweep.get("sweep_high", entry) * 1.0005,
            bos.get("bos_origin",   entry) * 1.0005,
        )
        rr = sl - entry
        if rr <= 0: return {}
        return {
            "entry": entry, "sl": sl,
            "tp1": entry - rr*2, "tp2": entry - rr*3,
            "runner": entry - rr*5, "rr_ratio": 2.0,
        }
    return {}


# ═══════════════════════════════════════════════════════════════
# 3-CANDLE NO-REACTION SKIP RULE
# ═══════════════════════════════════════════════════════════════

def monitor_ote_queue() -> None:
    now  = time.time()
    keep = []
    for item in ote_watch_queue:
        sym  = item["symbol"]
        bias = item["bias"]

        if now > item["expires_at"]:
            log.info(f"OTE expired: {sym} {bias}")
            continue

        price = get_price(sym)
        if price == 0:
            keep.append(item); continue

        in_zone = item["ote_low"] <= price <= item["ote_high"]
        if not in_zone:
            keep.append(item); continue

        item["checks_in_zone"] = item.get("checks_in_zone", 0) + 1
        prev    = item.get("last_price", price)
        thr     = 0.001
        reacted = (
            (bias == "bullish" and price > prev * (1 + thr)) or
            (bias == "bearish" and price < prev * (1 - thr))
        )
        item["last_price"] = price

        if reacted:
            emoji = "🟢" if bias == "bullish" else "🔴"
            send_telegram(
                f"{emoji} <b>ENTRY CONFIRMED — {sym.replace('_','/')} {bias.upper()}</b>\n\n"
                f"Reaction inside OTE at <b>{price:.4f}</b> ✅\n\n"
                f"🎯 Entry:        <b>{item['entry']:.4f}</b>\n"
                f"🛑 SL:           <b>{item['sl']:.4f}</b>\n"
                f"✅ TP1 (50%):    <b>{item['tp1']:.4f}</b>\n"
                f"✅ TP2 (30%):    <b>{item['tp2']:.4f}</b>\n"
                f"🚀 Runner (20%): <b>{item['runner']:.4f}</b>\n\n"
                f"<b>Execute now. Risk 1–2% only.</b>"
            )
            log.info(f"{sym}: OTE reaction confirmed — entry alert sent")
            continue

        if item["checks_in_zone"] >= 3:
            send_telegram(
                f"⛔ <b>SKIP — {sym.replace('_','/')} {bias.upper()}</b>\n\n"
                f"Price entered OTE at <b>{price:.4f}</b> but showed "
                f"<b>no reaction after 3 checks</b>.\n\n"
                f"3 candles in zone, no reaction → skip this trade.\n"
                f"Do not chase. Next setup will come."
            )
            log.info(f"{sym}: SKIP — 3 checks in OTE, no reaction")
            continue

        keep.append(item)

    ote_watch_queue.clear()
    ote_watch_queue.extend(keep)


# ═══════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════

def send_telegram(msg: str) -> None:
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        log.warning(f"Telegram: {e}")


def build_alert(sym: str, bd: dict, loc: str, liq: dict,
                swp: dict, tier: dict, bos: dict,
                fvg_ote: dict, ote: dict, risk: dict,
                pair_score: dict) -> str:
    direction = bd["bias"].upper()
    emoji     = "🟢" if direction == "BULLISH" else "🔴"
    now       = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    reasons   = "\n".join(f"  • {r}" for r in bd["reasons"][:4])
    fvg_line  = (
        f"FVG in OTE: ✅ <b>{fvg_ote['fvg_low']:.4f}–{fvg_ote['fvg_high']:.4f}</b> ← preferred entry"
        if fvg_ote["found"]
        else "FVG in OTE: ⚠️ None — wait for clean reaction inside zone"
    )
    score_line = (
        f"Pair score: <b>{pair_score.get('score',0):.0f}/100</b>  "
        f"| 24h move: <b>{pair_score.get('change_pct',0):+.2f}%</b>  "
        f"| {pair_score.get('reason','')}"
    )

    return f"""{emoji} <b>BEE-M SETUP DETECTED</b> {emoji}
<b>Pair:</b> {sym.replace('_','/')}  |  <b>Direction:</b> {direction}
<b>Time:</b> {now}
{score_line}

<b>━━ STEP 1 — DAILY BIAS ━━</b>
Score: {bd['score']:+d}  |  PDH: {bd['pdh']:.4f}  |  PDL: {bd['pdl']:.4f}
{reasons}

<b>━━ STEP 2 — 4H LOCATION ━━</b>
Price in <b>{loc.upper()}</b> ✅

<b>━━ STEP 3 — LIQUIDITY PATH ━━</b>
{liq.get('reason','')} ✅
Sweep: <b>{tier['label']}</b>  |  Conviction: <b>{tier['conviction']}</b>

<b>━━ STEP 4 — 15M CONFIRMATION ━━</b>
Sweep ✅  Displacement ✅
BOS: <b>{bos.get('bos_type','Confirmed')}</b> ✅
Imbalance left: <b>{'Yes — FVG formed' if bos.get('leaves_imbalance') else 'No — clean break'}</b>
Body dominance: <b>{bos.get('body_ratio', 0):.0%}</b> of candle range

<b>━━ STEP 5 — ENTRY ZONE (OTE) ━━</b>
Zone: <b>{ote['ote_low']:.4f} — {ote['ote_high']:.4f}</b>
Ideal (~0.705): <b>{ote['ideal']:.4f}</b>
{fvg_line}

<b>━━ STEP 6 — RISK LEVELS ━━</b>
🎯 Entry:        <b>{risk['entry']:.4f}</b>
🛑 SL:           <b>{risk['sl']:.4f}</b>  (sweep extreme or BOS origin)
✅ TP1 (50%):    <b>{risk['tp1']:.4f}</b>  (~1:{risk['rr_ratio']:.1f} RR)
✅ TP2 (30%):    <b>{risk['tp2']:.4f}</b>
🚀 Runner (20%): <b>{risk['runner']:.4f}</b>

<b>━━ FINAL CHECKLIST ━━</b>
✔ Daily bias confirmed ({bd['score']:+d})
✔ 4H location valid ({loc})
✔ Liquidity path clear
✔ Sweep: {tier['label']}
✔ Displacement + Expansion BOS confirmed
✔ BOS: {bos.get('bos_type','')}
✔ OTE zone calculated

⏳ <i>Monitoring OTE zone now.
CONFIRMED or SKIP alert coming next.</i>
<b>Risk 1–2% only. Predefine levels before entry.</b>""".strip()


# ═══════════════════════════════════════════════════════════════
# MAIN SCAN
# ═══════════════════════════════════════════════════════════════

def scan_pair(sym: str, pair_score: dict) -> None:
    log.info(f"Scanning {sym} (score {pair_score.get('score',0):.0f})...")

    daily = get_candles(sym, "Day1",  30)
    h4    = get_candles(sym, "Hour4", 50)
    m15   = get_candles(sym, "Min15", 100)

    if not daily or not h4 or not m15:
        log.warning(f"{sym}: missing data — skip"); return

    # Step 1 — Daily bias
    bd   = daily_bias(daily)
    bias = bd["bias"]
    if bias == "neutral":
        log.info(f"{sym}: neutral bias — NO TRADE"); return

    # Step 2 — 4H location
    loc = location_4h(h4)
    if (bias == "bullish" and loc == "premium") or \
       (bias == "bearish" and loc == "discount"):
        log.info(f"{sym}: misaligned location ({loc}) — NO TRADE"); return

    # Step 3 — Liquidity path
    liq = check_liq_path(h4, bias)
    if not liq["clear"]:
        log.info(f"{sym}: {liq['reason']} — NO TRADE"); return

    # Step 4 — Sweep → Displacement → BOS (strict sequence)
    pdh = bd["pdh"]; pdl = bd["pdl"]
    swp = detect_sweep(m15, bias, pdh, pdl)
    if not swp["swept"]:
        log.info(f"{sym}: no sweep — NO TRADE"); return

    si   = swp["candle_idx"]
    disp = detect_displacement(m15, bias, si)
    if not disp["valid"]:
        log.info(f"{sym}: no strong displacement — NO TRADE"); return

    # Pass avg_body and location into BOS so all 5 criteria can be checked
    bos = detect_bos(m15, bias, si, disp["avg_body"], loc)
    if not bos["broken"]:
        log.info(f"{sym}: BOS rejected — {bos.get('reason','no BOS')} — NO TRADE"); return

    # Sweep tier
    slvl = swp.get("sweep_low", swp.get("sweep_high", pdl))
    tier = classify_sweep_tier(bias, pdh, pdl, slvl, h4)

    # Step 5 — OTE + FVG check
    ote     = calc_ote(m15, bias, si)
    if not ote:
        log.info(f"{sym}: OTE failed — NO TRADE"); return
    fvg_ote = fvg_inside_ote(m15, bias, ote["ote_high"], ote["ote_low"])

    # Step 6 — Risk levels
    risk = calc_risk(bias, swp, bos, ote)
    if not risk:
        log.info(f"{sym}: risk calc failed — NO TRADE"); return

    # Dedup
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key       = f"{sym}_{bias}_{today_str}"
    if key in alerted_today:
        log.info(f"{sym}: already alerted today"); return
    alerted_today.add(key)

    # ── ALL RULES PASSED — send alert ─────────────────────────
    log.info(f"{sym}: ✅ FULL SETUP {bias.upper()} — alerting")
    msg = build_alert(sym, bd, loc, liq, swp, tier, bos, fvg_ote, ote, risk, pair_score)
    send_telegram(msg)

    # Add to OTE monitor queue
    ote_watch_queue.append({
        "symbol":         sym,
        "bias":           bias,
        "ote_high":       ote["ote_high"],
        "ote_low":        ote["ote_low"],
        "entry":          risk["entry"],
        "sl":             risk["sl"],
        "tp1":            risk["tp1"],
        "tp2":            risk["tp2"],
        "runner":         risk["runner"],
        "expires_at":     time.time() + OTE_MONITOR_SECS,
        "checks_in_zone": 0,
        "last_price":     0.0,
    })


def scan_all() -> None:
    log.info("=== Starting scan cycle ===")

    # Reset dedup daily
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if not hasattr(scan_all, "_day") or scan_all._day != today_str:
        alerted_today.clear()
        scan_all._day = today_str
        log.info("New day — dedup reset")

    # ── Dynamic pair selection ─────────────────────────────────
    log.info("Fetching all MEXC futures tickers...")
    tickers = get_all_tickers()
    if not tickers:
        log.warning("No tickers fetched — skipping cycle")
        return

    log.info(f"Total USDT futures pairs available: {len(tickers)}")
    scored_map = {s["symbol"]: s for s in score_pairs(tickers)}
    pairs      = select_pairs(tickers, TOP_N_PAIRS)

    if not pairs:
        log.warning("No pairs selected — skipping cycle")
        return

    # ── Scan each selected pair ────────────────────────────────
    for sym in pairs:
        try:
            pair_score = scored_map.get(sym, {"score": 0, "change_pct": 0, "reason": ""})
            scan_pair(sym, pair_score)
            time.sleep(1.2)
        except Exception as e:
            log.error(f"{sym}: {e}")

    # ── OTE monitor ───────────────────────────────────────────
    if ote_watch_queue:
        log.info(f"Monitoring {len(ote_watch_queue)} OTE setups...")
        monitor_ote_queue()

    log.info("=== Scan complete ===")


# ═══════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    log.info("BEE-M Alert Bot v3 starting...")
    send_telegram(
        "🤖 <b>BEE-M Alert Bot v4 — Full System</b>\n\n"
        "<b>Dynamic pair selection:</b>\n"
        "• Scores ALL MEXC USDT futures on volatility, volume, momentum, structure\n"
        "• Auto-selects top gainers, losers + highest-scored pairs each cycle\n\n"
        "<b>Meaningful BOS — 5 criteria active:</b>\n"
        "✔ Breaks RELEVANT swing high/low (not noise)\n"
        "✔ Candle CLOSES beyond level (no wick fakeouts)\n"
        "✔ EXPANSION BOS only (body ≥ 2x avg, body-dominant, leaves imbalance)\n"
        "✔ BOS only after liquidity sweep (sell-side for longs, buy-side for shorts)\n"
        "✔ Location aligned (longs from discount, shorts from premium)\n\n"
        "<b>All other rules active:</b>\n"
        "✔ 8 daily bias scenarios\n"
        "✔ 4H premium/discount location\n"
        "✔ Liquidity path + sweep tier (1/2/3)\n"
        "✔ FVG inside OTE check\n"
        "✔ 3-candle no-reaction skip rule\n"
        "✔ SL at sweep extreme or BOS origin\n"
        "✔ TP1 (50%) / TP2 (30%) / Runner (20%)\n\n"
        "<i>Inducement BOS = rejected. Only expansion BOS fires an alert.\n"
        "Any rule not met = NO TRADE. Trust the process.</i>"
    )
    while True:
        try:
            scan_all()
        except Exception as e:
            log.error(f"Loop error: {e}")
            send_telegram(f"⚠️ Bot error: {e}")
        log.info(f"Sleeping {SCAN_INTERVAL}s until next scan...")
        time.sleep(SCAN_INTERVAL)
