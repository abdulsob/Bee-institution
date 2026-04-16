"""
BEE-M Trading Alert Bot v16  (State-Aware Decision Engine)

MASTER FLOW:
  Narrative → Sweep → CLASSIFY ONCE → Lock Model → BOS → Entry → Manage → (Flip?)

STATE MACHINE per pair:
  WAITING_FOR_SWEEP
  → SWEEP_CONFIRMED          ← sweep detected, not yet classified
  → BEHAVIOR_CLASSIFICATION  ← NEW: classify behavior ONCE, lock model, never re-decide
  → WAITING_FOR_RETRACEMENT  ← BOS confirmed, waiting for OTE/momentum entry
  → TRADE_TAKEN
  → INVALIDATED              ← watches for flip conditions
      If confirmed → new narrative + re-enter opposite direction
      If timeout   → reset to WAITING_FOR_SWEEP

CRITICAL RULE (from PDF):
  > Once MODEL is selected after sweep → DO NOT reclassify until RESET
  This prevents flip-flopping, duplicate signals, and confusion during expansion.

MODEL 1 — REVERSAL:   sweep at PDH/PDL/extremes → displacement → BOS → OTE entry
MODEL 2 — CONTINUATION: trending market → impulse → pullback holds HL/LH → BOS → entry
MODEL 3 — HTF EXPANSION: sweep → strong expansion into HTF level → LH/HL forms → BOS → entry

DECISION ENGINE (classify_behavior):
  strong_reversal_after_sweep  → MODEL_1
  trend_continuation_intact    → MODEL_2
  strong_expansion_into_htf   → MODEL_3

4H LOCATION: Guideline only — bullish in discount ✔  bearish in premium ✔
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
MAX_SWEEP_AGE     = int(os.environ.get("MAX_SWEEP_AGE",    "14400"))  # 4h — BOS can be slow on majors
MAX_BOS_AGE       = int(os.environ.get("MAX_BOS_AGE",       "7200"))  # 2h OTE wait
MAX_FLIP_AGE      = int(os.environ.get("MAX_FLIP_AGE",      "3600"))  # 60 min to confirm flip
TOP_N_PAIRS       = int(os.environ.get("TOP_N_PAIRS",        "30"))
SWEEP_LOOKBACK    = int(os.environ.get("SWEEP_LOOKBACK",     "30"))
STATUS_INTERVAL   = int(os.environ.get("STATUS_INTERVAL",   "7200"))

MEXC_BASE = "https://contract.mexc.com/api/v1/contract"

# ── BOS Strength Filter (PDF2 fix #3) ──────────────────────────────────────
# Hard gate: BOS body ratio must meet this threshold. Weak BOS = false entries.
MIN_BOS_STRENGTH = float(os.environ.get("MIN_BOS_STRENGTH", "0.6"))

# ── Spread / Volatility Kill Filter (PDF2 fix #4) ──────────────────────────
MAX_SPREAD_PCT    = float(os.environ.get("MAX_SPREAD_PCT", "0.005"))    # 0.5%
MAX_CANDLE_RANGE_MULT = float(os.environ.get("MAX_CANDLE_RANGE_MULT", "4.0"))  # 4x avg

# ── Global state ───────────────────────────────────────────────────────────
alerted_today    = set()
pair_states      = {}
pair_rejections  = {}
last_status_time = 0.0

# ── Model 4 — Top Gainers / Losers tracking ────────────────────────────────
# Updated each scan cycle from the MEXC ticker list.
# Keys: symbol string → bool (is_top_gainer / is_top_loser)
_model4_top_gainers: set = set()
_model4_top_losers:  set = set()

# ── Session context flags (PDF1 fix) ───────────────────────────────────────
# Tracked per pair per day. Reset each new UTC day.
# Keys: {sym: {"london_sweep": bool, "ny_sweep": bool, "internal_sweep": bool, "strong_trend": bool}}
session_flags: dict = {}

# ── Session flag helpers (PDF1) ────────────────────────────────────────────

def _get_session_flags(sym: str) -> dict:
    """Return the session context flags dict for a pair, initialising if missing."""
    if sym not in session_flags:
        session_flags[sym] = {
            "london_sweep":   False,
            "ny_sweep":       False,
            "internal_sweep": False,
            "strong_trend":   False,
            "day":            "",
        }
    return session_flags[sym]


def _reset_session_flags(sym: str) -> None:
    """Reset all session flags for a pair (called once per new UTC day)."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    session_flags[sym] = {
        "london_sweep":   False,
        "ny_sweep":       False,
        "internal_sweep": False,
        "strong_trend":   False,
        "day":            today,
    }


def _ensure_session_flags_fresh(sym: str) -> dict:
    """Auto-reset flags if the stored day differs from today, then return them."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    flags = _get_session_flags(sym)
    if flags.get("day") != today:
        _reset_session_flags(sym)
        flags = session_flags[sym]
    return flags


def is_london_session(now_wat_hour: int) -> bool:
    """True if WAT hour falls inside the Primary (London) window 07:30-09:30."""
    return 7 <= now_wat_hour < 10


def is_newyork_session(now_wat_hour: int) -> bool:
    """True if WAT hour falls inside the Second Wave (NY) window 15:00-18:00."""
    return 15 <= now_wat_hour < 18


# ═══════════════════════════════════════════════════════════════════════════
# MODEL 4 — MOMENTUM MODIFIER  (PDF: Core State Machine + Model 4 Momentum Layer)
#
# Model 4 is NOT a signal model. It ONLY enhances valid trades from Models 1–3.
# It CANNOT create trades, override sweep logic, override BOS logic, change
# structure classification, or force entries.
#
# Activation: MID/LATE momentum phase + no dump + impulse strength > threshold
# Enhancements: earlier entry, TP expansion, exhaustion protection, psych TPs
# ═══════════════════════════════════════════════════════════════════════════

def _model4_momentum_phase() -> str:
    """Return the current momentum phase based on UTC hour."""
    hour = datetime.now(timezone.utc).hour
    if 8 <= hour < 12:
        return "EARLY"
    elif 12 <= hour < 15:
        return "MID"
    elif 15 <= hour < 17:
        return "LATE"
    else:
        return "LOW"


def _model4_impulse_strength(m15: list, bias: str) -> float:
    """
    Measure the strength of the most recent impulsive move (0.0–1.0).
    Calculated as the avg body ratio of the last 3 directional candles.
    """
    if len(m15) < 5:
        return 0.0
    recent = m15[-8:]
    directional = []
    for c in recent:
        rng = c["high"] - c["low"]
        if rng == 0:
            continue
        body = abs(c["close"] - c["open"])
        is_dir = (c["close"] > c["open"]) if bias == "bullish" else (c["close"] < c["open"])
        if is_dir:
            directional.append(body / rng)
    if not directional:
        return 0.0
    return sum(sorted(directional)[-3:]) / min(len(directional), 3)


def _model4_retracement_depth(m15: list, bias: str) -> float:
    """
    Measure the depth of the current retracement (0.0 = no retrace, 1.0 = full).
    Uses the last 20 candles to find the impulse high/low and current position.
    """
    if len(m15) < 5:
        return 0.5
    window = m15[-20:]
    price  = m15[-1]["close"]
    if bias == "bullish":
        leg_high = max(c["high"] for c in window)
        leg_low  = min(c["low"]  for c in window)
        rng = leg_high - leg_low
        if rng <= 0:
            return 0.5
        return (leg_high - price) / rng
    else:
        leg_high = max(c["high"] for c in window)
        leg_low  = min(c["low"]  for c in window)
        rng = leg_high - leg_low
        if rng <= 0:
            return 0.5
        return (price - leg_low) / rng


def _model4_consecutive_large_candles(m15: list, bias: str) -> int:
    """Count consecutive large directional candles at the end of the series."""
    if len(m15) < 4:
        return 0
    avg_range = sum(c["high"] - c["low"] for c in m15[-20:]) / max(len(m15[-20:]), 1)
    count = 0
    for c in reversed(m15[-6:]):
        rng  = c["high"] - c["low"]
        is_dir = (c["close"] > c["open"]) if bias == "bullish" else (c["close"] < c["open"])
        if is_dir and rng > avg_range * 1.8:
            count += 1
        else:
            break
    return count


def next_psychological_level(price: float) -> float:
    """Return the next psychological price level above the given price."""
    levels = [1, 2, 3, 5, 8, 10, 20, 50, 100]
    for level in levels:
        if price < level:
            return float(level)
    return price * 1.5


def update_model4_state(sym: str, m15: list, bias: str) -> None:
    """
    Evaluate and update all Model 4 state variables for a pair.
    Must be called AFTER valid structure is confirmed (sweep exists).
    Model 4 NEVER creates a trade — it only annotates the existing state.
    """
    st = get_state(sym)

    # ── Priority pair flag ────────────────────────────────────────────────
    st["is_top_gainer"]    = sym in _model4_top_gainers
    st["is_top_loser"]     = sym in _model4_top_losers
    st["is_priority_pair"] = st["is_top_gainer"] or st["is_top_loser"]

    # ── Momentum phase ────────────────────────────────────────────────────
    st["momentum_phase"] = _model4_momentum_phase()

    # ── No-dump condition (retracement shallow = strength still intact) ───
    retrace_depth    = _model4_retracement_depth(m15, bias)
    st["no_dump"]    = retrace_depth < 0.3

    # ── Impulse strength ──────────────────────────────────────────────────
    impulse_strength = _model4_impulse_strength(m15, bias)

    # ── Model 4 activation (strict — per PDF) ────────────────────────────
    threshold = 0.6 if st["is_priority_pair"] else 0.7
    if (
        st["momentum_phase"] in ("MID", "LATE")
        and st["no_dump"]
        and impulse_strength > threshold
    ):
        st["momentum_model"] = True
    else:
        st["momentum_model"] = False

    # ── Momentum shutdown rule (prevents overheating trend chasing) ───────
    if retrace_depth > 0.5:
        st["momentum_model"] = False

    # ── Exhaustion detector (runs regardless of momentum_model) ──────────
    st["exhaustion"] = False
    if st["is_top_gainer"]:
        if retrace_depth > 0.6:
            st["exhaustion"] = True
        if impulse_strength < 0.5:
            st["exhaustion"] = True
        consec = _model4_consecutive_large_candles(m15, bias)
        if consec >= 3:
            st["exhaustion"] = True

    # ── Psychological TP target ───────────────────────────────────────────
    if st["momentum_model"]:
        price = m15[-1]["close"] if m15 else 0
        st["tp_target"] = next_psychological_level(price) if price > 0 else None
    else:
        st["tp_target"] = None

    log.debug(
        f"{sym}: Model4 — active={st['momentum_model']}  phase={st['momentum_phase']}  "
        f"priority={st['is_priority_pair']}  no_dump={st['no_dump']}  "
        f"exhaustion={st['exhaustion']}  psych_tp={st['tp_target']}"
    )


def model4_get_min_retrace(st: dict) -> float:
    """
    Entry adjustment — Model 4 allows earlier entries on priority pairs.
    PDF §11: min_retrace = 0.25 (priority+momentum) else 0.5
    """
    if st.get("momentum_model") and st.get("is_priority_pair"):
        return 0.25
    return 0.5


def model4_get_bos_threshold(st: dict) -> float:
    """
    BOS strength filter — Model 4 lowers the required threshold slightly.
    PDF §11: 0.6 when momentum active, else 0.7
    """
    if st.get("momentum_model"):
        return 0.6
    return 0.7


def model4_get_tp_multiplier(st: dict) -> float:
    """
    TP expansion multiplier for Model 4.
    PDF §11: 1.5 for top losers, 1.8 for top gainers, 1.0 otherwise
    """
    if not st.get("momentum_model"):
        return 1.0
    if st.get("is_top_loser"):
        return 1.5
    return 1.8


def model4_allow_early_exit(st: dict) -> bool:
    """
    Hold behaviour — Model 4 disables early exits to ride momentum.
    PDF §11: allow_early_exit = False when momentum_model active
    """
    return not st.get("momentum_model", False)


def _update_model4_top_pairs(all_scored: list, top_n: int = 20) -> None:
    """
    Refresh the global top-gainer / top-loser sets from the latest scorer output.
    Called once per scan cycle before individual pairs are processed.
    """
    global _model4_top_gainers, _model4_top_losers
    if not all_scored:
        return
    by_change = sorted(
        [s for s in all_scored if s.get("change_pct", 0) != 0],
        key=lambda x: x["change_pct"]
    )
    _model4_top_losers  = {s["symbol"] for s in by_change[:top_n]}
    _model4_top_gainers = {s["symbol"] for s in by_change[-top_n:]}
    log.debug(
        f"Model4 top pairs updated — "
        f"gainers={len(_model4_top_gainers)}  losers={len(_model4_top_losers)}"
    )



    """
    Returns (ok: bool, reason: str).
    Blocks entry when the latest candle's range is erratic (fake liquidity sweeps).
    MAX_CANDLE_RANGE_MULT env var controls the threshold (default 4x avg).
    """
    if len(m15) < 20:
        return True, ""
    recent = m15[-20:]
    avg_range = sum(c["high"] - c["low"] for c in recent[:-1]) / max(len(recent) - 1, 1)
    if avg_range == 0:
        return True, ""
    last_range = m15[-1]["high"] - m15[-1]["low"]
    if last_range > avg_range * MAX_CANDLE_RANGE_MULT:
        return False, (
            f"Candle range {last_range:.4f} is {last_range/avg_range:.1f}x avg "
            f"({avg_range:.4f}) — erratic/fake liquidity, blocked"
        )
    return True, ""


# ── Trade limiter — per-window caps + quality gate + same-pair cooldown ──────
# Flat per-session cap was too loose. Three layers now:
#   1. Per-WINDOW cap (not per-session) — each window has its own max
#   2. Quality gate — only A/A+ setups consume a slot; B-grade setups are
#      blocked when the window is already 50%+ full
#   3. Same-pair cooldown — once a pair fires in a window, locked for that window

WINDOW_CAPS = {
    "Primary (07:30-09:30 WAT)":    2,   # 2h window — max 2 trades
    "Optional (12:30-13:30 WAT)":   1,   # 1h window — max 1 trade
    "Second Wave (15:00-18:00 WAT)": 2,  # 3h window — max 2 trades
}
# Fallback for any unrecognised window name
DEFAULT_WINDOW_CAP = int(os.environ.get("DEFAULT_WINDOW_CAP", "2"))

window_trades    = {}   # {window_name: count}  — resets each new UTC day
window_trade_day = {}   # {window_name: "YYYY-MM-DD"}
window_pair_lock = {}   # {window_name: set(sym)}  — cooldown per pair per window

# ── Trade log — PnL fix #7 ──────────────────────────────────────────────────
# Every fired alert is logged here. After 50 trades you know your real edge.
trade_log = []

# ── Execution windows (WAT = UTC+1 = UTC+1h) ──────────────────────────────
# Bias + levels locked at 6:30 AM WAT each day.
# Entry alerts fire freely during execution windows.
# Outside windows: sweeps and BOS are still tracked, but entry alerts are
# HELD and sent at the next window open with "[HELD]" tag.
#
# WAT = UTC+1, so we add 1h to convert to UTC for comparison.

BIAS_LOCK_HOUR_WAT   = 6    # 6:30 AM WAT -> 5:30 UTC
BIAS_LOCK_MINUTE_WAT = 30

EXECUTION_WINDOWS_WAT = [
    (7, 30,  9, 30, "Primary (07:30-09:30 WAT)"),
    (12, 30, 13, 30, "Optional (12:30-13:30 WAT)"),
    (15,  0, 18,  0, "Second Wave (15:00-18:00 WAT)"),
]


def _wat_now():
    """Current time as (hour, minute) in WAT (UTC+1)."""
    now = datetime.now(timezone.utc)
    wat_hour   = (now.hour + 1) % 24
    return wat_hour, now.minute


def in_execution_window():
    """Returns (True, window_name) if now is inside an execution window, else (False, '')."""
    h, m = _wat_now()
    mins = h * 60 + m
    for sh, sm, eh, em, name in EXECUTION_WINDOWS_WAT:
        if sh * 60 + sm <= mins < eh * 60 + em:
            return True, name
    return False, ""


def is_bias_lock_time():
    """True if we are at or just past 6:30 AM WAT (within the current 15-min scan cycle)."""
    h, m = _wat_now()
    return h == BIAS_LOCK_HOUR_WAT and m >= BIAS_LOCK_MINUTE_WAT


def minutes_to_next_window():
    """Returns (minutes, name) to the next execution window."""
    h, m  = _wat_now()
    cur   = h * 60 + m
    best_d, best_n = 9999, ""
    for sh, sm, eh, em, name in EXECUTION_WINDOWS_WAT:
        d = sh * 60 + sm - cur
        if d <= 0:
            d += 1440
        if d < best_d:
            best_d, best_n = d, name
    return best_d, best_n


def _window_end_utc_for_timestamp(ts):
    """
    Given a Unix timestamp (when a sweep/BOS was detected), return the UTC
    Unix timestamp of the END of the kill zone that was active at that moment.
    Returns None if the event occurred outside all execution windows.

    Logic:
      - Convert ts → WAT minutes-since-midnight
      - Find which EXECUTION_WINDOW it falls inside
      - Return the window's end time as a UTC Unix timestamp (same calendar day)
    """
    dt_utc   = datetime.fromtimestamp(ts, tz=timezone.utc)
    wat_hour = (dt_utc.hour + 1) % 24
    wat_min  = dt_utc.minute
    # If WAT hour wrapped past midnight, the WAT date is one day ahead of UTC date.
    # We only need the end-time in seconds so we work with offsets.
    wat_mins_now = wat_hour * 60 + wat_min

    for sh, sm, eh, em, name in EXECUTION_WINDOWS_WAT:
        if sh * 60 + sm <= wat_mins_now < eh * 60 + em:
            # Calculate how many seconds from ts until this window ends
            window_end_wat_mins = eh * 60 + em
            mins_remaining      = window_end_wat_mins - wat_mins_now
            return ts + mins_remaining * 60, name   # (end_ts_utc, window_name)

    return None, None   # sweep/BOS happened outside any window


def sweep_expired(st):
    """
    Returns (expired: bool, reason: str).

    A sweep expires when the kill zone it was detected in has ended.
    Grace period: 15 minutes past window close — catches setups that fire
    right at the boundary and need one more scan cycle to confirm BOS.

    Fallback: if the sweep was detected OUTSIDE a window (e.g. bot caught it
    during a between-session scan), fall back to MAX_SWEEP_AGE flat timeout.
    """
    sweep_ts = st.get("sweep_time", 0)
    if not sweep_ts:
        return False, ""

    end_ts, win_name = _window_end_utc_for_timestamp(sweep_ts)

    if end_ts is not None:
        grace   = 15 * 60   # 15-minute grace buffer
        expired = time.time() > end_ts + grace
        if expired:
            win_end_str = datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime("%H:%M UTC")
            return True, f"Kill zone '{win_name}' ended at {win_end_str} — no BOS within window"
        return False, ""
    else:
        # Sweep detected outside a window — use flat fallback timeout
        age     = state_age(st, "sweep_time")
        expired = age > MAX_SWEEP_AGE
        if expired:
            return True, f"Sweep detected outside window, expired after {MAX_SWEEP_AGE // 60}min flat timeout"
        return False, ""


def ote_expired(st):
    """
    Returns (expired: bool, reason: str).

    OTE/retracement wait expires when the kill zone the BOS was confirmed in
    has ended (plus 15-min grace). Same fallback logic as sweep_expired.
    """
    bos_ts = st.get("bos_time", 0)
    if not bos_ts:
        return False, ""

    end_ts, win_name = _window_end_utc_for_timestamp(bos_ts)

    if end_ts is not None:
        grace   = 15 * 60
        expired = time.time() > end_ts + grace
        if expired:
            win_end_str = datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime("%H:%M UTC")
            return True, f"Kill zone '{win_name}' ended at {win_end_str} — price never reached OTE within window"
        return False, ""
    else:
        age     = state_age(st, "bos_time")
        expired = age > MAX_BOS_AGE
        if expired:
            return True, f"BOS outside window, OTE expired after {MAX_BOS_AGE // 60}min flat timeout"
        return False, ""


def build_narrative(bd, loc_data, m15, market_condition=None, live_price=None):
    """
    Build the trade narrative — adaptive to current market context.
    live_price: pass the real-time ticker price so narrative reflects
                actual current price, not the last closed candle's close.
    market_condition: output of classify_market_condition() if available.
    """
    bias      = bd["bias"]
    strength  = bd.get("strength", "unknown")
    pdh       = bd["pdh"]
    pdl       = bd["pdl"]
    # Use live price if provided — fallback to last closed candle close
    price     = live_price if live_price and live_price > 0 else (m15[-1]["close"] if m15 else 0)
    cond      = market_condition or {}
    condition = cond.get("condition", "ranging")
    cond_note = cond.get("condition_note", "")

    # What price has done — read live from candles
    if bd.get("eq_lows") and bias == "bullish":
        what = f"Equal lows swept at {max(bd['eq_lows']):.4f}"
    elif bd.get("eq_highs") and bias == "bearish":
        what = f"Equal highs swept at {min(bd['eq_highs']):.4f}"
    elif price > pdh:
        what = f"Price above PDH {pdh:.4f} — trading above range"
    elif price < pdl:
        what = f"Price below PDL {pdl:.4f} — trading below range"
    else:
        mid = (pdh + pdl) / 2
        side = "upper half" if price > mid else "lower half"
        what = f"Price inside PD range ({pdl:.4f}–{pdh:.4f}), {side}"

    # Expectation and target depend on condition
    if bias == "bullish":
        target = f"PDH {pdh:.4f}" + (f" / EQH {min(bd['eq_highs']):.4f}" if bd.get("eq_highs") else "")
        inval  = "Break and CLOSE below sweep low / HL"
        if condition == "trending":
            cond_note_lower = cond_note.lower()
            if "trending toward" in cond_note_lower:
                expect = f"Trending toward PDH — ride continuation shorts on pullbacks, target {target}"
            else:
                expect = f"Trending up — pullback holds HL → continuation BOS → push to {target}"
        elif condition == "extreme":
            expect = f"At PDL extreme — sweep → displacement up → BOS → retrace → long to {target}"
        else:
            expect = f"Sweep PDL area → displacement up → BOS → retrace → long to {target}"
    else:
        target = f"PDL {pdl:.4f}" + (f" / EQL {max(bd['eq_lows']):.4f}" if bd.get("eq_lows") else "")
        inval  = "Break and CLOSE above sweep high / LH"
        if condition == "trending":
            cond_note_lower = cond_note.lower()
            if "trending toward" in cond_note_lower:
                expect = f"Trending toward PDL — ride continuation shorts on pullbacks, target {target}"
            else:
                expect = f"Trending down — pullback holds LH → continuation BOS → push to {target}"
        elif condition == "extreme":
            expect = f"At PDH extreme — sweep → displacement down → BOS → retrace → short to {target}"
        else:
            expect = f"Sweep PDH area → displacement down → BOS → retrace → short to {target}"

    return {
        "bias":       bias,
        "strength":   strength,
        "what":       what,
        "target":     target,
        "expect":     expect,
        "inval":      inval,
        "score":      bd["score"],
        "condition":  condition,
        "cond_note":  cond_note,
    }


def update_narrative(sym, m15, h4, daily, live_price=None):
    """
    Called every scan cycle for pairs in active states.
    Rebuilds the narrative if market context has changed.
    Uses live_price for accurate current price context.
    """
    st = pair_states.get(sym)
    if not st or st.get("state") not in ("SWEEP_CONFIRMED", "WAITING_FOR_RETRACEMENT"):
        return

    old_narr = st.get("narrative", {})
    old_bias = old_narr.get("bias", "")
    old_what = old_narr.get("what", "")

    bd       = daily_bias(daily)
    loc      = location_4h(h4)
    mc       = classify_market_condition(h4, daily, bd)
    new_narr = build_narrative(bd, loc, m15, market_condition=mc, live_price=live_price)

    # ── PDF1 fix #4: Update strong_trend session flag ─────────────────────
    flags = _ensure_session_flags_fresh(sym)
    structure_str = mc.get("condition", "ranging")
    bos_strength_est = st.get("bos", {}).get("body_ratio", 0)
    if structure_str == "trending" and bos_strength_est > 0.7:
        flags["strong_trend"] = True
    else:
        flags["strong_trend"] = False

    # Detect material changes
    bias_changed      = (old_bias and new_narr["bias"] != old_bias)
    condition_changed = (old_narr.get("condition") and
                         new_narr["condition"] != old_narr.get("condition"))
    what_changed      = (old_what and new_narr["what"] != old_what)

    if bias_changed or condition_changed:
        # Material change — update stored narrative and alert
        st["narrative"] = new_narr
        change_desc = []
        if bias_changed:
            change_desc.append(f"Bias: {old_bias.upper()} → {new_narr['bias'].upper()}")
        if condition_changed:
            change_desc.append(f"Condition: {old_narr.get('condition','?')} → {new_narr['condition']}")
        log.info(f"{sym}: Narrative updated — {', '.join(change_desc)}")
        send_telegram(
            f"<b>📋 NARRATIVE UPDATE — {sym.replace('_', '/')}</b>\n"
            f"{'  |  '.join(change_desc)}\n\n"
            f"<b>Updated story:</b>\n"
            f"  What: {new_narr['what']}\n"
            f"  Expect: {new_narr['expect']}\n"
            f"  Inval: {new_narr['inval']}\n\n"
            f"<i>Review your active setup against the new narrative.</i>"
        )
    elif what_changed:
        # Minor update — refresh silently
        st["narrative"] = new_narr
        log.info(f"{sym}: Narrative refreshed (what changed: {new_narr['what']})")


# ═══════════════════════════════════════════════════════════════════════════
# STATE MACHINE HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def get_state(sym):
    if sym not in pair_states:
        pair_states[sym] = {
            "state": "WAITING_FOR_SWEEP",
            # ── Model 4 state variables ────────────────────────────────────
            "momentum_model":  False,
            "momentum_phase":  None,
            "is_top_gainer":   False,
            "is_top_loser":    False,
            "is_priority_pair": False,
            "no_dump":         False,
            "exhaustion":      False,
            "tp_target":       None,
        }
    return pair_states[sym]


def reset_state(sym, reason=""):
    if reason:
        log.info(f"{sym}: reset -- {reason}")
    pair_states[sym] = {"state": "WAITING_FOR_SWEEP"}


def invalidate_state(sym, old_bias, reason=""):
    """
    Move to INVALIDATED state instead of immediately resetting.
    Watches for opposite sweep + displacement + BOS (flip conditions).
    If no flip confirmed within MAX_FLIP_AGE, resets to WAITING_FOR_SWEEP.
    """
    flip_bias = "bearish" if old_bias == "bullish" else "bullish"
    log.info(f"{sym}: INVALIDATED ({old_bias}) -- {reason} -- watching for {flip_bias} flip")
    pair_states[sym] = {
        "state":          "INVALIDATED",
        "old_bias":       old_bias,
        "flip_bias":      flip_bias,
        "invalidated_at": time.time(),
        "reason":         reason,
    }


def state_age(st, key):
    ts = st.get(key, 0)
    return time.time() - ts if ts else float("inf")


def _current_session_name():
    """
    Returns the name of the current execution window, or None if outside all windows.
    Used by the trade limiter to key trade counts per session.
    """
    h, m = _wat_now()
    cur  = h * 60 + m
    for sh, sm, eh, em, name in EXECUTION_WINDOWS_WAT:
        if sh * 60 + sm <= cur < eh * 60 + em:
            return name
    return None


def _check_trade_limit(win_name, sym, setup_grade):
    """
    Three-layer trade gate — returns (allowed: bool, reason: str).

    Layer 1 — Per-window cap:
        Each window has a fixed max (Primary=2, Optional=1, Second Wave=2).
        Resets at the start of each new UTC day.

    Layer 2 — Quality gate:
        When a window is at 50%+ capacity, only A/A+ setups are allowed.
        B-grade setups are blocked — save the remaining slot for a better one.

    Layer 3 — Same-pair cooldown:
        Once a pair fires an alert in a window, it is locked for the rest
        of that window. Prevents re-entering the same structure after a reset.
    """
    if not win_name:
        return True, ""   # outside window — no limit applies

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Reset counters and locks if new day
    if window_trade_day.get(win_name) != today:
        window_trades[win_name]    = 0
        window_trade_day[win_name] = today
        window_pair_lock[win_name] = set()

    cap   = WINDOW_CAPS.get(win_name, DEFAULT_WINDOW_CAP)
    count = window_trades.get(win_name, 0)

    # Layer 3 — same-pair cooldown
    locked_pairs = window_pair_lock.get(win_name, set())
    if sym in locked_pairs:
        return False, (
            f"{sym} already fired in {win_name} today — "
            f"same-pair cooldown active for this window"
        )

    # Layer 1 — hard cap
    if count >= cap:
        return False, (
            f"Window cap reached for {win_name} "
            f"({count}/{cap}) — waiting for next window"
        )

    # Layer 2 — quality gate at 50%+ capacity
    if count >= cap / 2 and setup_grade == "B":
        return False, (
            f"Quality gate: {win_name} is at {count}/{cap} — "
            f"B-grade setup blocked, saving slot for A/A+"
        )

    return True, ""


def _register_trade(win_name, sym, bias, model, bos_strength, setup_grade,
                    entry_price=0.0, sl=0.0, tp1=0.0):
    """
    Increment window count, apply same-pair lock, and append to trade_log.
    Now includes entry_price, sl, tp1 for outcome tracking (PDF2 fix #1).
    Call exactly once when an alert is actually sent.
    """
    if win_name:
        window_trades[win_name] = window_trades.get(win_name, 0) + 1
        if win_name not in window_pair_lock:
            window_pair_lock[win_name] = set()
        window_pair_lock[win_name].add(sym)

    cap   = WINDOW_CAPS.get(win_name, DEFAULT_WINDOW_CAP) if win_name else "?"
    count = window_trades.get(win_name, 0) if win_name else 0

    trade_log.append({
        "pair":         sym,
        "model":        model,
        "window":       win_name or "outside_window",
        "bias":         bias,
        "bos_strength": round(bos_strength, 3),
        "grade":        setup_grade,
        "time":         datetime.now(timezone.utc).isoformat(),
        # ── Outcome tracking fields (PDF2 fix #1) ──────────────────────────
        "entry_price":  round(entry_price, 6) if entry_price else None,
        "sl":           round(sl, 6)          if sl          else None,
        "tp1":          round(tp1, 6)         if tp1         else None,
        "sl_hit":       None,   # True/False — to be filled after trade closes
        "tp_hit":       None,   # True/False — to be filled after trade closes
        "partials":     [],     # list of partial exit prices
        "result":       None,   # "win" / "loss" / "breakeven" — to be filled manually
    })
    log.info(
        f"TRADE LOGGED — {sym} {bias.upper()} [{model}] grade={setup_grade} "
        f"window={win_name}  count={count}/{cap}  total={len(trade_log)}"
    )


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
        candles = [
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
        # Drop the last candle if it appears to be the current open (partial) candle.
        # MEXC includes the in-progress candle as the final entry — its volume is
        # incomplete and its OHLC can flip multiple times before close. Using it
        # causes false sweeps, phantom BOS signals, and stale price reads.
        # We keep it for daily/4H (where the candle is hours old and stable) but
        # strip it for 15M where it can be seconds old.
        if interval == "Min15" and len(candles) >= 2:
            candles = candles[:-1]
        return candles
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
# STEP 1 -- DAILY BIAS  (6-factor model, max ±9)
#
# Factor 1: Daily structure HH+HL / LH+LL          +2 / -2
# Factor 2: Liquidity draw (equal highs/lows)       +2 / -2
# Factor 3: Previous daily candle narrative         +1 / -1
# Factor 4: Sweep + rejection context               +2 / -2
# Factor 5: Premium / discount location             +1 / -1
# Factor 6: HTF imbalance / FVG draw                +1 / -1
#
# Outcome:  +4 or more  = Strong Bullish
#           +2 to +3    = Bullish Lean
#           -1 to +1    = Neutral -> NO TRADE
#           -2 to -3    = Bearish Lean
#           -4 or less  = Strong Bearish
# ═══════════════════════════════════════════════════════════════════════════

def daily_bias(daily):
    if len(daily) < 3:
        return {"bias": "neutral", "score": 0, "strength": "neutral",
                "reasons": [], "pdh": 0, "pdl": 0, "mid": 0}

    # Use last 5-10 candles for structure, last 3 for narrative
    candles   = daily[-10:] if len(daily) >= 10 else daily
    prev      = daily[-2]   # yesterday
    prev2     = daily[-3]   # two days ago
    today     = daily[-1]   # current (partial) daily candle
    pdh       = prev["high"]
    pdl       = prev["low"]
    mid       = (pdh + pdl) / 2
    score     = 0
    reasons   = []

    # ── Factor 1: Daily structure (+/-2) ────────────────────────────────────
    # Use last 5 closed candles to detect HH+HL or LH+LL
    if len(candles) >= 5:
        last5 = candles[-6:-1]   # 5 closed candles
        hs    = [c["high"] for c in last5]
        ls    = [c["low"]  for c in last5]
        hh_hl = (hs[-1] > hs[-2] > hs[-3]) and (ls[-1] > ls[-2] > ls[-3])
        lh_ll = (hs[-1] < hs[-2] < hs[-3]) and (ls[-1] < ls[-2] < ls[-3])
        # Relaxed: at least 2 of 3 swings trending
        bull_swings = sum([hs[-1] > hs[-2], hs[-2] > hs[-3],
                           ls[-1] > ls[-2], ls[-2] > ls[-3]])
        bear_swings = sum([hs[-1] < hs[-2], hs[-2] < hs[-3],
                           ls[-1] < ls[-2], ls[-2] < ls[-3]])
        if bull_swings >= 3:
            score += 2
            reasons.append("F1 Bull +2: Daily HH+HL structure")
        elif bear_swings >= 3:
            score -= 2
            reasons.append("F1 Bear -2: Daily LH+LL structure")
        else:
            reasons.append("F1 Neutral: No clear daily structure")

    # ── Factor 2: Liquidity draw (+/-2) ─────────────────────────────────────
    # Equal highs above = price drawn up; equal lows below = price drawn down
    window = candles[-20:] if len(candles) >= 20 else candles
    price  = today["close"]
    eq_highs = find_equal_levels(window, "bearish", tolerance=0.003)
    eq_lows  = find_equal_levels(window, "bullish",  tolerance=0.003)
    above_eq = [l for l in eq_highs if l > price]
    below_eq = [l for l in eq_lows  if l < price]
    if above_eq and not below_eq:
        score += 2
        reasons.append(f"F2 Bull +2: Equal highs above @ {min(above_eq):.4f} -- draw up")
    elif below_eq and not above_eq:
        score -= 2
        reasons.append(f"F2 Bear -2: Equal lows below @ {max(below_eq):.4f} -- draw down")
    elif above_eq and below_eq:
        # Closer target wins
        nearest_above = min(above_eq) - price
        nearest_below = price - max(below_eq)
        if nearest_above < nearest_below:
            score += 1
            reasons.append(f"F2 Bull +1: Equal highs closer ({nearest_above:.4f} away)")
        else:
            score -= 1
            reasons.append(f"F2 Bear -1: Equal lows closer ({nearest_below:.4f} away)")
    else:
        reasons.append("F2 Neutral: No obvious external liquidity draw")

    # ── Factor 3: Previous daily candle narrative (+/-1) ─────────────────────
    prev_body = abs(prev["close"] - prev["open"])
    prev_rng  = prev["high"] - prev["low"]
    prev_br   = prev_body / prev_rng if prev_rng > 0 else 0
    # Strong close: body ratio >= 0.6 AND closed in the direction of move
    is_bull_close = prev["close"] > prev["open"] and prev_br >= 0.6
    is_bear_close = prev["close"] < prev["open"] and prev_br >= 0.6
    # Displacement candle: body >= 1.5x average of prior 5 candles
    avg_body5 = sum(abs(c["close"] - c["open"]) for c in candles[-7:-2]) / 5 if len(candles) >= 7 else prev_body
    is_disp   = prev_body >= avg_body5 * 1.5
    if is_bull_close or (is_disp and prev["close"] > prev["open"]):
        score += 1
        reasons.append(f"F3 Bull +1: Strong bullish close (body {prev_br:.0%})")
    elif is_bear_close or (is_disp and prev["close"] < prev["open"]):
        score -= 1
        reasons.append(f"F3 Bear -1: Strong bearish close (body {prev_br:.0%})")
    else:
        reasons.append(f"F3 Neutral: Indecisive previous candle (body {prev_br:.0%})")

    # ── Factor 4: Sweep + rejection context (+/-2) ───────────────────────────
    # Did yesterday or the day before sweep a key level and close back?
    swept_low_closed_bull  = (prev["low"] < prev2["low"]  and prev["close"] > prev2["low"])
    swept_high_closed_bear = (prev["high"] > prev2["high"] and prev["close"] < prev2["high"])
    # Also check PDH/PDL sweeps
    swept_pdl_bull = (prev["low"] < pdl and prev["close"] > pdl)   # caught within same candle
    swept_pdh_bear = (prev["high"] > pdh and prev["close"] < pdh)
    if swept_low_closed_bull:
        score += 2
        reasons.append(f"F4 Bull +2: Swept prior low ({prev2['low']:.4f}) + closed bullish")
    elif swept_pdl_bull and not swept_low_closed_bull:
        score += 1
        reasons.append(f"F4 Bull +1: PDL sweep + closed back inside")
    if swept_high_closed_bear:
        score -= 2
        reasons.append(f"F4 Bear -2: Swept prior high ({prev2['high']:.4f}) + closed bearish")
    elif swept_pdh_bear and not swept_high_closed_bear:
        score -= 1
        reasons.append(f"F4 Bear -1: PDH sweep + closed back inside")
    if not any([swept_low_closed_bull, swept_pdl_bull,
                swept_high_closed_bear, swept_pdh_bear]):
        reasons.append("F4 Neutral: No sweep+rejection context")

    # ── Factor 5: Premium / discount location (+/-1) ─────────────────────────
    if price < mid:
        score += 1
        reasons.append(f"F5 Bull +1: Price in discount ({price:.4f} < mid {mid:.4f})")
    elif price > mid:
        score -= 1
        reasons.append(f"F5 Bear -1: Price in premium ({price:.4f} > mid {mid:.4f})")
    else:
        reasons.append("F5 Neutral: Price at equilibrium")

    # ── Factor 6: HTF imbalance / FVG draw (+/-1) ────────────────────────────
    fd, fh, fl = unmitigated_daily_fvg(daily)
    if fd == "bullish":
        score += 1
        reasons.append(f"F6 Bull +1: Unmitigated bullish FVG above ({fl:.4f}-{fh:.4f})")
    elif fd == "bearish":
        score -= 1
        reasons.append(f"F6 Bear -1: Unmitigated bearish FVG below ({fl:.4f}-{fh:.4f})")
    else:
        reasons.append("F6 Neutral: No unmitigated daily FVG")

    # ── Outcome ───────────────────────────────────────────────────────────────
    if score >= 4:
        strength = "strong_bullish"
        bias     = "bullish"
    elif score >= 2:
        strength = "bullish_lean"
        bias     = "bullish"
    elif score <= -4:
        strength = "strong_bearish"
        bias     = "bearish"
    elif score <= -2:
        strength = "bearish_lean"
        bias     = "bearish"
    else:
        strength = "neutral"
        bias     = "neutral"

    return {
        "bias":     bias,
        "strength": strength,
        "score":    score,
        "reasons":  reasons,
        "pdh":      pdh,
        "pdl":      pdl,
        "mid":      mid,
        "eq_highs": above_eq,
        "eq_lows":  below_eq,
    }


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
# STEP 4 -- LIQUIDITY SWEEP  (structure-based, not candle-based)
#
# Rule: A sweep is VALID when price TRADES INTO a defined liquidity pool
# and triggers stops above/below it.
#   - Wick OR full candle close beyond level: both valid
#   - No immediate rejection candle required
#   - No wick rejection required (it's a bonus, not a requirement)
#   - What matters: "Was liquidity actually taken?"
#
# Liquidity pools (in priority order):
#   - Equal highs / equal lows (most reliable)
#   - PDH / PDL
#   - Session swing highs/lows
#   - Clear recent swing highs/lows
#
# High-probability sweep: 2+ of these:
#   A. Obvious liquidity level (equal H/L or PDH/PDL)
#   B. HTF context aligns (premium -> sell sweep, discount -> buy sweep)
#   C. Displacement follows sweep
#   D. MSS/CHoCH after sweep
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


def _swing_highs(candles, strength=2):
    """Return swing high prices in the candle window."""
    result = []
    n = len(candles)
    for i in range(strength, n - strength):
        if all(candles[i]["high"] >= candles[i-j]["high"] for j in range(1, strength+1)) and \
           all(candles[i]["high"] >= candles[i+j]["high"] for j in range(1, strength+1)):
            result.append(candles[i]["high"])
    return result


def _swing_lows(candles, strength=2):
    """Return swing low prices in the candle window."""
    result = []
    n = len(candles)
    for i in range(strength, n - strength):
        if all(candles[i]["low"] <= candles[i-j]["low"] for j in range(1, strength+1)) and \
           all(candles[i]["low"] <= candles[i+j]["low"] for j in range(1, strength+1)):
            result.append(candles[i]["low"])
    return result


def detect_true_sweep(m15, bias, pdh, pdl, loc="unknown"):
    """
    Detect a valid liquidity sweep.
    Validity = price traded INTO a defined pool (stop run occurred).
    Does NOT require: wick rejection, immediate reversal, close back inside.
    A full candle close beyond the level is equally valid.
    """
    if len(m15) < 5:
        return {"swept": False, "reason": "Not enough candles"}

    window   = m15[-SWEEP_LOOKBACK:] if len(m15) >= SWEEP_LOOKBACK else m15
    offset   = len(m15) - len(window)
    eq_highs = find_equal_levels(window, "bearish", tolerance=0.003)
    eq_lows  = find_equal_levels(window, "bullish",  tolerance=0.003)
    sw_highs = _swing_highs(window, strength=2)
    sw_lows  = _swing_lows(window,  strength=2)

    # Build all valid liquidity levels for each direction
    bull_levels = list(set([pdl] + eq_lows + sw_lows))   # lows are buy-side liquidity
    bear_levels = list(set([pdh] + eq_highs + sw_highs))  # highs are sell-side liquidity

    best = None

    for i in range(len(window) - 1):
        c       = window[i]
        age     = len(window) - 1 - i
        abs_idx = offset + i

        if bias == "bullish":
            # Price must have traded BELOW a buy-side liquidity level
            # (wick below OR full close below -- both count)
            for lvl in bull_levels:
                if c["low"] < lvl:   # liquidity taken -- stop run occurred
                    # Reject if this is just mid-range noise (no clear level)
                    is_pdl      = abs(lvl - pdl) / max(pdl, 0.0001) < 0.001
                    is_eq_low   = any(abs(lvl - l) / max(l, 0.0001) < 0.003 for l in eq_lows)
                    is_sw_low   = any(abs(lvl - l) / max(l, 0.0001) < 0.003 for l in sw_lows)
                    is_clear    = is_pdl or is_eq_low or is_sw_low
                    if not is_clear:
                        continue

                    # Confluence scoring
                    confluence = 0
                    if is_pdl or is_eq_low:
                        confluence += 1     # A: obvious level
                    if loc in ("discount", "equilibrium"):
                        confluence += 1     # B: HTF context aligns
                    # C + D checked later after BOS/displacement confirmed

                    # Wick rejection is a BONUS -- record if present
                    has_rejection = c["close"] > lvl   # closed back above level
                    reaction      = abs(c["close"] - c["low"]) if has_rejection else 0

                    sw = {
                        "swept":        True,
                        "sweep_type":   "immediate" if age <= 5 else "pre_swept",
                        "sweep_level":  lvl,
                        "sweep_low":    c["low"],
                        "candle_idx":   abs_idx,
                        "is_equal_hl":  is_eq_low,
                        "is_pdx":       is_pdl,
                        "has_rejection": has_rejection,
                        "reaction":     reaction,
                        "confluence":   confluence,
                        "age_candles":  age,
                    }
                    if best is None or age < best["age_candles"] or \
                       (age == best["age_candles"] and confluence > best["confluence"]):
                        best = sw
                    break   # one level per candle is enough

        elif bias == "bearish":
            for lvl in bear_levels:
                if c["high"] > lvl:   # liquidity taken
                    is_pdh      = abs(lvl - pdh) / max(pdh, 0.0001) < 0.001
                    is_eq_high  = any(abs(lvl - l) / max(l, 0.0001) < 0.003 for l in eq_highs)
                    is_sw_high  = any(abs(lvl - l) / max(l, 0.0001) < 0.003 for l in sw_highs)
                    is_clear    = is_pdh or is_eq_high or is_sw_high
                    if not is_clear:
                        continue

                    confluence = 0
                    if is_pdh or is_eq_high:
                        confluence += 1
                    if loc in ("premium", "equilibrium"):
                        confluence += 1

                    has_rejection = c["close"] < lvl
                    reaction      = abs(c["high"] - c["close"]) if has_rejection else 0

                    sw = {
                        "swept":         True,
                        "sweep_type":    "immediate" if age <= 5 else "pre_swept",
                        "sweep_level":   lvl,
                        "sweep_high":    c["high"],
                        "candle_idx":    abs_idx,
                        "is_equal_hl":   is_eq_high,
                        "is_pdx":        is_pdh,
                        "has_rejection": has_rejection,
                        "reaction":      reaction,
                        "confluence":    confluence,
                        "age_candles":   age,
                    }
                    if best is None or age < best["age_candles"] or \
                       (age == best["age_candles"] and confluence > best["confluence"]):
                        best = sw
                    break

    if best:
        return best

    # Nothing found — give a clear reason
    if bias == "bullish":
        return {"swept": False,
                "reason": f"No candle traded below any buy-side level "
                           f"(PDL {pdl:.4f}, {len(eq_lows)} eq lows, {len(sw_lows)} swing lows)"}
    else:
        return {"swept": False,
                "reason": f"No candle traded above any sell-side level "
                           f"(PDH {pdh:.4f}, {len(eq_highs)} eq highs, {len(sw_highs)} swing highs)"}


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
            # ── PDF2 fix #3: MIN_BOS_STRENGTH hard gate ───────────────────
            if exp["body_ratio"] < MIN_BOS_STRENGTH:
                return {"broken": False,
                        "reason": f"BOS body ratio {exp['body_ratio']:.0%} < MIN_BOS_STRENGTH {MIN_BOS_STRENGTH:.0%} — weak BOS rejected"}
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
            # ── PDF2 fix #3: MIN_BOS_STRENGTH hard gate ───────────────────
            if exp["body_ratio"] < MIN_BOS_STRENGTH:
                return {"broken": False,
                        "reason": f"BOS body ratio {exp['body_ratio']:.0%} < MIN_BOS_STRENGTH {MIN_BOS_STRENGTH:.0%} — weak BOS rejected"}
            return {"broken": True, "bos_level": level, "bos_origin": level,
                    "bos_type": "Broke last 15M swing LOW (expansion)",
                    "leaves_imbalance": exp["leaves_imbalance"],
                    "body_ratio":       exp["body_ratio"]}
    return {"broken": False, "reason": "No candle closed beyond the swing level"}


# ═══════════════════════════════════════════════════════════════════════════
# STEP 7 -- MOVE QUALITY + OTE on actual BOS impulse leg
#
# PDF correction applied:
#   The OTE Fib must be drawn on the IMPULSIVE LEG THAT CAUSED THE BOS —
#   not just any swing, not just the closest candles after the sweep.
#
#   Rule: Did this move break a previous high/low? Was there clear
#   displacement? Did it come after the sweep? If yes → that is the leg.
#
#   If multiple swings exist, always use the CLEANEST DISPLACEMENT LEG
#   that directly caused the BOS — not the closest one.
# ═══════════════════════════════════════════════════════════════════════════

def classify_move_quality(disp):
    ratio = disp.get("body_ratio", 0)
    fvg   = disp.get("leaves_fvg", False)
    if ratio >= 0.70 and fvg:
        return "strong"
    if ratio >= 0.55:
        return "moderate"
    return "weak"


def _find_bos_impulse_leg(m15, bias, sweep_idx, bos_level):
    """
    Identify the specific impulsive leg that directly caused the BOS.

    Per BEE-M PDF rules, the correct leg must satisfy ALL THREE:
      1. Comes AFTER the sweep (starts at or after sweep_idx)
      2. Contains the candle whose CLOSE actually broke the BOS level
      3. Is the CLEANEST DISPLACEMENT leg — scored by body ratio AND
         whether it leaves an imbalance (FVG) behind it.
         "If multiple swings exist, always choose the cleanest displacement
         leg that directly caused the BOS — not the closest one."

    Scoring (higher = better candidate):
      - avg_body_ratio:  portion of each directional candle that is body (0-1)
      - imbalance_bonus: +0.15 if any candle in the leg leaves a price gap
                         (high of prev < low of next for bull / vice versa for bear)
      - min threshold:   avg_body_ratio >= 0.45 (filters out choppy legs)

    Search window extended to 40 candles (from 20) so late BOS is captured.

    Returns dict with leg_low, leg_high, leg_start, score, leaves_imbalance
    or None if no qualifying leg found.
    """
    # Extended search window — BOS can arrive several candles after sweep
    search = m15[sweep_idx: sweep_idx + 40]
    if len(search) < 2:
        return None

    # Pre-sweep average body for context (not used in scoring, kept for debug)
    pre_window = m15[max(0, sweep_idx - 10): sweep_idx]
    avg_body_pre = (
        sum(abs(c["close"] - c["open"]) for c in pre_window)
        / max(len(pre_window), 1)
    )

    best_leg   = None
    best_score = 0.0

    for start in range(len(search) - 1):
        c_start = search[start]

        if bias == "bullish":
            leg_low       = c_start["low"]
            leg_high      = c_start["high"]
            bos_broken_at = -1
            total_body    = 0
            candle_count  = 0
            leaves_imbalance = False

            for end in range(start, len(search)):
                c    = search[end]
                prev = search[end - 1] if end > 0 else c_start

                # Leg invalidated: lower low means it's not a clean upward impulse
                if c["low"] < leg_low:
                    break

                leg_high = max(leg_high, c["high"])
                body = abs(c["close"] - c["open"])
                rng  = c["high"] - c["low"]

                if rng > 0 and c["close"] > c["open"]:  # directional bull candle
                    total_body += body / rng
                    candle_count += 1

                # Imbalance check: gap between prev high and this candle's low
                if c["low"] > prev["high"]:
                    leaves_imbalance = True

                if c["close"] > bos_level and bos_broken_at == -1:
                    bos_broken_at = end
                    break   # leg ends exactly at the BOS break

            if bos_broken_at == -1:
                continue  # this start point never reached BOS level

            rng = leg_high - leg_low
            if rng <= 0:
                continue

            avg_ratio = total_body / max(candle_count, 1)
            if avg_ratio < 0.45:
                continue  # too choppy — not a displacement leg

            # Score = body quality + imbalance bonus
            score = avg_ratio + (0.15 if leaves_imbalance else 0.0)

            if score > best_score:
                best_score = score
                best_leg   = {
                    "leg_low":         leg_low,
                    "leg_high":        leg_high,
                    "leg_start":       sweep_idx + start,
                    "score":           round(score, 3),
                    "leaves_imbalance": leaves_imbalance,
                    "avg_body_ratio":  round(avg_ratio, 3),
                }

        elif bias == "bearish":
            leg_high      = c_start["high"]
            leg_low       = c_start["low"]
            bos_broken_at = -1
            total_body    = 0
            candle_count  = 0
            leaves_imbalance = False

            for end in range(start, len(search)):
                c    = search[end]
                prev = search[end - 1] if end > 0 else c_start

                if c["high"] > leg_high:
                    break

                leg_low = min(leg_low, c["low"])
                body = abs(c["close"] - c["open"])
                rng  = c["high"] - c["low"]

                if rng > 0 and c["close"] < c["open"]:  # directional bear candle
                    total_body += body / rng
                    candle_count += 1

                # Imbalance check: gap between prev low and this candle's high
                if c["high"] < prev["low"]:
                    leaves_imbalance = True

                if c["close"] < bos_level and bos_broken_at == -1:
                    bos_broken_at = end
                    break

            if bos_broken_at == -1:
                continue

            rng = leg_high - leg_low
            if rng <= 0:
                continue

            avg_ratio = total_body / max(candle_count, 1)
            if avg_ratio < 0.45:
                continue

            score = avg_ratio + (0.15 if leaves_imbalance else 0.0)

            if score > best_score:
                best_score = score
                best_leg   = {
                    "leg_low":         leg_low,
                    "leg_high":        leg_high,
                    "leg_start":       sweep_idx + start,
                    "score":           round(score, 3),
                    "leaves_imbalance": leaves_imbalance,
                    "avg_body_ratio":  round(avg_ratio, 3),
                }

    return best_leg  # dict or None


def calc_ote(m15, bias, sweep_idx, bos_level, move_quality="moderate"):
    """
    Calculate OTE zone using the IMPULSIVE LEG THAT CAUSED THE BOS.

    Per BEE-M PDF rules:
    - Fib drawn on the specific leg that BROKE structure (not nearest swing).
    - If multiple legs exist, use the CLEANEST DISPLACEMENT leg (scored by
      body ratio + imbalance bonus) — not the closest one.
    - Bullish:  Fib Low → High  |  Bearish:  Fib High → Low
    - OTE zone: 0.618–0.705 (moderate) | 0.705–0.79 (weak) | 0.382–0.50 (strong)

    Fallback behaviour (when impulse leg detection fails):
    - Use the BOS candle's own range anchored to the BOS level.
    - This is a tighter, more honest fallback than the old "sweep window" approach.
    - Flagged as "fallback" in leg_source so alerts can note it.
    """
    zones = {
        "strong":   (0.382, 0.500, 0.440),
        "moderate": (0.618, 0.705, 0.660),
        "weak":     (0.705, 0.790, 0.750),
    }
    high_ret, low_ret, ideal_ret = zones.get(move_quality, zones["moderate"])

    # ── Find the actual BOS impulse leg ──────────────────────────────────────
    impulse = _find_bos_impulse_leg(m15, bias, sweep_idx, bos_level)

    if impulse:
        leg_low  = impulse["leg_low"]
        leg_high = impulse["leg_high"]
        log.debug(
            f"OTE: BOS impulse leg — "
            f"low={leg_low:.4f}  high={leg_high:.4f}  "
            f"score={impulse['score']}  imbalance={impulse['leaves_imbalance']}  "
            f"quality={move_quality}"
        )
        leg_source      = "impulse"
        leg_imbalance   = impulse["leaves_imbalance"]
        leg_score       = impulse["score"]
    else:
        # ── Tight fallback: BOS candle anchored to BOS level ─────────────────
        # We do NOT use a broad sweep window — that risks using a random swing.
        # Instead anchor to the BOS level and use the nearest candle's range.
        log.warning(
            f"OTE: no clean impulse leg found after sweep_idx={sweep_idx} "
            f"for bos_level={bos_level:.4f} ({bias}). Using BOS-anchor fallback."
        )
        bos_candle_window = m15[sweep_idx: sweep_idx + 10]
        if not bos_candle_window:
            return {}
        if bias == "bullish":
            # Anchor: lowest point in window → BOS level as top
            leg_low  = min(c["low"] for c in bos_candle_window)
            leg_high = bos_level
        else:
            # Anchor: BOS level as bottom → highest point in window
            leg_high = max(c["high"] for c in bos_candle_window)
            leg_low  = bos_level
        leg_source    = "fallback"
        leg_imbalance = False
        leg_score     = 0.0

    if bias == "bullish":
        rng = leg_high - leg_low
        if rng <= 0:
            return {}
        return {
            "ote_high":        leg_high - high_ret  * rng,
            "ote_low":         leg_high - low_ret   * rng,
            "ideal":           leg_high - ideal_ret * rng,
            "leg_high":        leg_high,
            "leg_low":         leg_low,
            "move_quality":    move_quality,
            "leg_source":      leg_source,
            "leg_imbalance":   leg_imbalance,
            "leg_score":       leg_score,
        }
    elif bias == "bearish":
        rng = leg_high - leg_low
        if rng <= 0:
            return {}
        return {
            "ote_low":         leg_low  + high_ret  * rng,
            "ote_high":        leg_low  + low_ret   * rng,
            "ideal":           leg_low  + ideal_ret * rng,
            "leg_high":        leg_high,
            "leg_low":         leg_low,
            "move_quality":    move_quality,
            "leg_source":      leg_source,
            "leg_imbalance":   leg_imbalance,
            "leg_score":       leg_score,
        }
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
    """
    SL Rule (per playbook):
      Primary:  Beyond the sweep extreme (the wick that took liquidity)
      Fallback: BOS origin if it gives a tighter, cleaner structural level
                AND the sweep extreme is too far (>3R from entry)

    Logic:
      1. Always calculate both candidates.
      2. Use sweep extreme as default — it is the true invalidation point.
      3. Only use BOS origin instead if:
           a. Sweep extreme is missing or zero, OR
           b. BOS origin is closer to entry AND produces a more sensible RR
              (i.e. the sweep extreme would give >3R to TP1, making sizing impractical)
      4. Add a small buffer beyond the chosen level (0.05%) to avoid stop hunts.
    """
    if not ote:
        return {}
    entry = ote.get("ideal", 0)
    if not entry:
        return {}

    if bias == "bullish":
        sweep_extreme = sweep.get("sweep_low",   0)   # the wick low that took stops
        bos_origin    = bos.get("bos_origin",    0)   # swing low that was the BOS base

        # Both must be below entry to be valid SL candidates
        sweep_sl  = sweep_extreme * 0.9995 if sweep_extreme > 0 else 0
        bos_sl    = bos_origin    * 0.9995 if bos_origin    > 0 else 0

        if sweep_sl > 0 and bos_sl > 0:
            # Prefer sweep extreme — it is the true structural invalidation
            # Only fall back to BOS origin if it is meaningfully tighter AND
            # sweep extreme is more than 3% below entry (too wide to size properly)
            sweep_dist = entry - sweep_sl
            bos_dist   = entry - bos_sl
            too_wide   = sweep_dist / entry > 0.03   # >3% distance
            bos_closer = bos_dist < sweep_dist
            if too_wide and bos_closer and bos_sl > 0:
                sl       = bos_sl
                sl_label = "bos_origin"
            else:
                sl       = sweep_sl
                sl_label = "sweep_extreme"
        elif sweep_sl > 0:
            sl, sl_label = sweep_sl, "sweep_extreme"
        elif bos_sl > 0:
            sl, sl_label = bos_sl,   "bos_origin"
        else:
            sl, sl_label = entry * 0.98, "fallback_2pct"

        rr = entry - sl
        if rr <= 0:
            return {}

    elif bias == "bearish":
        sweep_extreme = sweep.get("sweep_high",  0)   # the wick high that took stops
        bos_origin    = bos.get("bos_origin",    0)   # swing high that was the BOS base

        sweep_sl  = sweep_extreme * 1.0005 if sweep_extreme > 0 else 0
        bos_sl    = bos_origin    * 1.0005 if bos_origin    > 0 else 0

        if sweep_sl > 0 and bos_sl > 0:
            sweep_dist = sweep_sl - entry
            bos_dist   = bos_sl   - entry
            too_wide   = sweep_dist / entry > 0.03
            bos_closer = bos_dist < sweep_dist
            if too_wide and bos_closer and bos_sl > 0:
                sl       = bos_sl
                sl_label = "bos_origin"
            else:
                sl       = sweep_sl
                sl_label = "sweep_extreme"
        elif sweep_sl > 0:
            sl, sl_label = sweep_sl, "sweep_extreme"
        elif bos_sl > 0:
            sl, sl_label = bos_sl,   "bos_origin"
        else:
            sl, sl_label = entry * 1.02, "fallback_2pct"

        rr = sl - entry
        if rr <= 0:
            return {}
    else:
        return {}

    log.debug(f"calc_risk: SL={sl:.4f} [{sl_label}]  entry={entry:.4f}  rr={rr:.4f}")

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

    return {
        "entry":        entry,
        "sl":           sl,
        "sl_label":     sl_label,
        "tp1":          tp1,   "tp1_label":    tp1_lbl,
        "tp2":          tp2,   "tp2_label":    tp2_lbl,
        "runner":       runner,"runner_label": run_lbl,
        "rr_ratio":     rr_ratio,
    }


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


def build_trade_log_report():
    """
    /tradelog command — shows all fired alerts and session trade counts.
    After 50+ trades this becomes your real edge data.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"<b>📊 Trade Log — {now}</b>", ""]

    # Window counts today
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    lines.append("<b>Today's window counts:</b>")
    for sh, sm, eh, em, name in EXECUTION_WINDOWS_WAT:
        day_check    = window_trade_day.get(name)
        count        = window_trades.get(name, 0) if day_check == today else 0
        cap          = WINDOW_CAPS.get(name, DEFAULT_WINDOW_CAP)
        locked       = window_pair_lock.get(name, set()) if day_check == today else set()
        cap_emoji    = "🔴" if count >= cap else ("🟡" if count >= cap / 2 else "🟢")
        lock_str     = f"  🔒 {', '.join(s.replace('_USDT','') for s in locked)}" if locked else ""
        lines.append(f"  {cap_emoji} {name}: {count}/{cap}{lock_str}")

    lines.append("")
    total = len(trade_log)
    lines.append(f"<b>Total alerts fired all-time: {total}</b>")

    if not trade_log:
        lines.append("No trades logged yet.")
        return "\n".join(lines)

    # Last 10 entries
    lines.append("")
    lines.append("<b>Last 10 alerts:</b>")
    for entry in trade_log[-10:]:
        t     = entry.get("time", "")[:16].replace("T", " ")
        pair  = entry["pair"].replace("_", "/")
        bias  = entry["bias"].upper()
        model = entry["model"]
        sess  = entry.get("session", "?")
        bos   = entry.get("bos_strength", 0)
        res   = entry.get("result") or "pending"
        lines.append(
            f"  {t}  {pair} {bias}  [{model}]\n"
            f"    Session: {sess}  BOS: {bos:.0%}  Result: {res}"
        )

    # Breakdown by session
    if total >= 5:
        lines.append("")
        lines.append("<b>Breakdown by session:</b>")
        from collections import Counter
        sess_counts = Counter(e.get("session", "?") for e in trade_log)
        for sess, cnt in sess_counts.most_common():
            lines.append(f"  {sess}: {cnt} trades")

    lines.append("")
    lines.append(f"<i>Update 'result' field in trade_log to track your real edge after 50+ trades.</i>")
    return "\n".join(lines)


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
        elif text in ("/setups", "/st"):
            send_telegram(build_confirmed_setups())
        elif text in ("/tradelog", "/tl"):
            recent = trade_log[-20:] if len(trade_log) > 20 else trade_log
            if not recent:
                send_telegram("<i>No trades logged yet. Trades appear here after the first setup fires.</i>")
            else:
                session_counts = {}
                for t in trade_log:
                    sn = t.get("session", "?")
                    session_counts[sn] = session_counts.get(sn, 0) + 1
                lines = [f"<b>📋 Trade Log — last {len(recent)} of {len(trade_log)} total</b>", ""]
                for t in recent:
                    result_tag = "✅" if t.get("result") == "win" else "❌" if t.get("result") == "loss" else "⏳"
                    lines.append(
                        f"{result_tag} {t['pair'].replace('_','/')} {t['bias'].upper()}  "
                        f"[{t['model']}]  BOS={t['bos_strength']:.0%}  {t['time'][11:16]}UTC"
                    )
                lines += ["", "<b>Session counts (today):</b>"]
                for sn, cnt in session_counts.items():
                    lines.append(f"  {sn}: {cnt}/{MAX_TRADES_PER_SESSION}")
                lines.append(f"\n<i>{len(trade_log)} trades logged. After 50 = real edge data.</i>")
                send_telegram("\n".join(lines))
        elif text in ("/tradelog", "/tl"):
            send_telegram(build_trade_log_report())

        elif text in ("/help", "/h"):
            send_telegram(
                "<b>BEE-M Bot Commands</b>\n\n"
                "/setups   or  /st — <b>Confirmed setups only</b>: Entry, SL, TP\n"
                "/pairs    or  /p  — Active setups: all pairs in non-waiting state\n"
                "/status   or  /s  — Full scan status: every pair + system step\n"
                "/tradelog or  /tl — Trade log: all fired alerts + session counts\n"
                "/help     or  /h  — Show this menu"
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

        if state == "BEHAVIOR_CLASSIFICATION":
            age    = int(state_age(st, "sweep_time") / 60)
            lvl    = st.get("sweep", {}).get("sweep_level", 0)
            locked = st.get("locked_model", "?")
            active_lines.append(
                f"  <b>{pair}</b>  {bias}\n"
                f"    🔍 Sweep @ {lvl:.4f}  ({age}min ago)  →  {locked} locked\n"
                f"    Waiting: confirmation (displacement + BOS)"
            )
        elif state == "SWEEP_CONFIRMED":
            age = int(state_age(st, "sweep_time") / 60)
            lvl = st.get("sweep", {}).get("sweep_level", 0)
            active_lines.append(
                f"  <b>{pair}</b>  {bias}\n"
                f"    ⚔️ MODEL_1 sweep @ {lvl:.4f}  ({age}min ago)\n"
                f"    Waiting: Displacement + BOS"
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
        elif state == "INVALIDATED":
            age       = int((time.time() - st.get("invalidated_at", time.time())) / 60)
            remaining = max(0, int((MAX_FLIP_AGE - (time.time() - st.get("invalidated_at", time.time()))) / 60))
            flip_bias = st.get("flip_bias", "?").upper()
            reason    = st.get("reason", "")
            active_lines.append(
                f"  <b>{pair}</b>  ⚠️ INVALIDATED ({age}min ago)\n"
                f"    Was: {bias}  →  Watching for: {flip_bias} flip\n"
                f"    Needs: Opposite sweep + Displacement + BOS\n"
                f"    Reason: {reason}\n"
                f"    Flip window: {remaining}min remaining"
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
        "Model 2 (Continuation)": "Model 2 CONTINUATION",
        "Invalidation":           "INVALIDATED",
        "Flip Step 1 (Sweep)":    "Flip: waiting for sweep",
        "Flip Step 2 (Displacement)": "Flip: sweep ✔ waiting for displacement",
        "Flip Step 3 (BOS)":      "Flip: sweep+disp ✔ waiting for BOS",
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
        "BEHAVIOR_CLASSIFICATION":   "🔍 Sweep detected — model locked, awaiting BOS",
        "SWEEP_CONFIRMED":           "⚔️ MODEL_1 — Sweep confirmed, waiting for BOS",
        "WAITING_FOR_RETRACEMENT":   "🎯 BOS confirmed — watching OTE entry zone",
        "TRADE_TAKEN":               "✅ Trade alert fired",
        "INVALIDATED":               "⚠️ INVALIDATED — watching for flip conditions",
    }
    for sym, st in active:
        pair   = sym.replace("_", "/")
        bias   = st.get("bias", "?").upper()
        state  = st.get("state", "?")
        rating = st.get("setup_score", {}).get("rating", "")
        locked = st.get("locked_model", "")
        label  = step_labels.get(state, state)
        # Show which model was locked for BEHAVIOR_CLASSIFICATION state
        if state == "BEHAVIOR_CLASSIFICATION" and locked:
            label = label.replace("model locked", f"{locked} locked")
        rtag   = f"  [{rating}]" if rating else ""
        lines.append(f"<b>{pair}</b>  {bias}{rtag}\n  {label}")
    lines.append("\nSend /status for full detail on all scanned pairs.")
    return "\n".join(lines)


def build_confirmed_setups():
    """
    /setups command — only pairs with a CONFIRMED entry setup.
    Shows Entry, SL, and TP levels only. Clean and fast to scan.
    A 'confirmed setup' means the pair is in WAITING_FOR_RETRACEMENT
    (Sweep + Displacement + BOS all done — entry zone is live).
    """
    now  = datetime.now(timezone.utc).strftime("%H:%M UTC")
    h, m = _wat_now()
    in_win, win_name = in_execution_window()
    win_tag = f"🟢 {win_name}" if in_win else "⏸ Outside window — your call"

    confirmed = [
        (sym, st) for sym, st in sorted(pair_states.items())
        if st.get("state") == "WAITING_FOR_RETRACEMENT"
    ]

    lines = [
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "<b>✅ CONFIRMED SETUPS</b>",
        f"🕐 {now}  ({h:02d}:{m:02d} WAT)",
        f"Window: {win_tag}",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    if not confirmed:
        lines.append("No confirmed setups right now.")
        lines.append("")
        lines.append("Confirmed = Sweep ✔  Displacement ✔  BOS ✔  Entry zone live.")
        lines.append("Use /pairs to see all active states.")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        return "\n".join(lines)

    for sym, st in confirmed:
        pair   = sym.replace("_", "/")
        bias   = st.get("bias", "?").upper()
        risk   = st.get("risk", {})
        ote    = st.get("ote", {})
        rating = st.get("setup_score", {}).get("rating", "")
        fvg    = st.get("fvg_ote", {})
        age    = int(state_age(st, "bos_time") / 60)
        bias_icon = "🟢" if bias == "BULLISH" else "🔴"

        fvg_line = ""
        if fvg and fvg.get("found"):
            fvg_line = f"\n   📌 FVG: {fvg.get('fvg_low',0):.4f} – {fvg.get('fvg_high',0):.4f}  ← preferred entry"

        lines.append(f"{bias_icon} <b>{pair}</b>  {bias}  [{rating}]")

        if risk:
            lines.append(f"   Entry:        <b>{risk.get('entry', 0):.4f}</b>{fvg_line}")
            lines.append(f"   SL:           <b>{risk.get('sl', 0):.4f}</b>")
            lines.append(f"   TP1 (50%):    <b>{risk.get('tp1', 0):.4f}</b>  [{risk.get('tp1_label','--')}]")
            lines.append(f"   TP2 (30%):    <b>{risk.get('tp2', 0):.4f}</b>  [{risk.get('tp2_label','--')}]")
            lines.append(f"   Runner (20%): <b>{risk.get('runner', 0):.4f}</b>  [{risk.get('runner_label','--')}]")
        else:
            # Fallback: show OTE zone if risk dict missing
            lines.append(f"   OTE zone: {ote.get('ote_low',0):.4f} – {ote.get('ote_high',0):.4f}{fvg_line}")
            lines.append(f"   SL: below sweep extreme")

        lines.append(f"   ⏳ BOS confirmed {age}min ago  |  Risk 1-2% only")
        lines.append("")

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"{len(confirmed)} setup(s) confirmed  |  /pairs for full active list")
    return "\n".join(lines)


def build_setup_alert(sym, bd, loc_data, sweep, disp, bos,
                      fvg_ote, ote, risk, setup_score, pair_score,
                      narrative=None, in_window=True, win_name=""):
    bias      = bd["bias"]
    direction = bias.upper()
    strength  = bd.get("strength", "").replace("_", " ").title()
    rating    = setup_score["rating"]
    now       = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    loc       = loc_data.get("location", "unknown")
    mq        = ote.get("move_quality", "moderate")
    mq_map    = {
        "strong":   "Shallow zone (0.382-0.50) — strong move",
        "moderate": "Standard OTE (0.618-0.705)",
        "weak":     "Deep OTE (0.705-0.79) — wait for full retrace",
    }
    mq_desc  = mq_map.get(mq, "Standard OTE")
    eq_tag   = " | Equal H/L" if sweep.get("is_equal_hl") else ""
    pdx_tag  = " | PDH/PDL"   if sweep.get("is_pdx")     else ""
    rej_tag  = " | Rejection ✔" if sweep.get("has_rejection") else ""
    conf     = sweep.get("confluence", 0)
    fvg_line = ("FVG in OTE: YES — " +
                f"{fvg_ote['fvg_low']:.4f}–{fvg_ote['fvg_high']:.4f} (preferred entry)"
                if fvg_ote and fvg_ote.get("found")
                else "FVG in OTE: None — react cleanly inside zone")
    sc_line  = (f"Pair: {pair_score.get('score', 0):.0f}/100"
                f" | 24h: {pair_score.get('change_pct', 0):+.2f}%"
                f" | {pair_score.get('reason', '')}")
    win_line = f"🟢 Window: {win_name}" if in_window else "⏸ Setup HELD — fires when execution window opens"
    bonus_txt = "\n".join(f"  {r}" for r in setup_score["reasons"]) or "  None"

    narr_lines = []
    if narrative:
        narr_lines = [
            "",
            "<b>— NARRATIVE (Playbook Step 1) —</b>",
            f"  Bias: <b>{narrative.get('strength','').replace('_',' ').title()}</b>  (score {narrative.get('score',0):+d})",
            f"  What price did: {narrative.get('what','')}",
            f"  Target liquidity: {narrative.get('target','')}",
            f"  Expectation: {narrative.get('expect','')}",
            f"  Invalidation: {narrative.get('inval','')}",
        ]

    loc_note = ""
    if loc == "discount":
        if bias == "bullish":
            loc_note = "  ✔ Ideal — bullish bias in discount zone"
        else:
            loc_note = "  ℹ Bearish in discount — continuation model or strong BOS needed"
    elif loc == "premium":
        if bias == "bearish":
            loc_note = "  ✔ Ideal — bearish bias in premium zone"
        else:
            loc_note = "  ℹ Bullish in premium — continuation model or strong BOS needed"
    elif loc == "equilibrium":
        loc_note = "  ℹ Equilibrium — wait for clear displacement"

    lines = [
        f"<b>BEE-M SETUP — {direction} — {strength} — Rating: {rating}</b>",
        f"<b>Pair:</b> {sym.replace('_', '/')}  |  <b>Time:</b> {now}",
        win_line,
        sc_line,
    ]
    lines.extend(narr_lines)
    lines += [
        "",
        "<b>— DAILY BIAS (6-factor) —</b>",
        f"Score: {bd['score']:+d}  |  PDH: {bd['pdh']:.4f}  |  PDL: {bd['pdl']:.4f}",
    ]
    lines.extend(f"  {r}" for r in bd["reasons"])
    lines += [
        "",
        "<b>— 4H LOCATION —</b>",
        f"Price in <b>{loc.upper()}</b>  [guideline only]",
        loc_note,
        f"Range: {loc_data.get('sl', 0):.4f}–{loc_data.get('sh', 0):.4f}  |  EQ: {loc_data.get('eq', 0):.4f}",
        f"OTE band: {loc_data.get('fib_618', 0):.4f}–{loc_data.get('fib_792', 0):.4f}",
        "",
        "<b>— SWEEP —</b>",
        (f"{sweep.get('sweep_type', '').replace('_', ' ').title()}"
         f"  ({sweep.get('age_candles', 0)} candles ago){eq_tag}{pdx_tag}{rej_tag}"),
        f"Level: {sweep.get('sweep_level', 0):.4f}  |  Confluence: {conf}/2+",
        "",
        "<b>— DISPLACEMENT —</b>",
        f"Body ratio: {disp.get('body_ratio', 0):.0%}  |  FVG left: {'Yes' if disp.get('leaves_fvg') else 'No'}",
        f"Move quality: <b>{mq.upper()}</b> — {mq_desc}",
        "",
        "<b>— BOS —</b>",
        bos.get("bos_type", "Confirmed"),
        f"Imbalance: {'Yes' if bos.get('leaves_imbalance') else 'No'}  |  Body: {bos.get('body_ratio', 0):.0%}",
        "",
        "<b>— OTE ENTRY ZONE —</b>",
        (f"Leg: {ote.get('leg_low', 0):.4f}\u2013{ote.get('leg_high', 0):.4f}  "
         f"[{'\u2705 impulse' if ote.get('leg_source') == 'impulse' else '\u26a0\ufe0f fallback'}]"
         f"{'  \u26a1 imbalance' if ote.get('leg_imbalance') else ''}"),
        f"Zone: <b>{ote.get('ote_low', 0):.4f}\u2013{ote.get('ote_high', 0):.4f}</b>",
        f"Ideal (0.705): <b>{ote.get('ideal', 0):.4f}</b>",
        fvg_line,
        "",
        "<b>— RISK —</b>",
        f"Entry:        <b>{risk.get('entry', 0):.4f}</b>",
        f"SL:           <b>{risk.get('sl', 0):.4f}</b>  [{risk.get('sl_label', 'structural')}]",
        f"TP1 (50%):    <b>{risk.get('tp1', 0):.4f}</b>  [{risk.get('tp1_label', '--')}]  (~1:{risk.get('rr_ratio', 2):.1f})",
        f"TP2 (30%):    <b>{risk.get('tp2', 0):.4f}</b>  [{risk.get('tp2_label', '--')}]",
        f"Runner (20%): <b>{risk.get('runner', 0):.4f}</b>  [{risk.get('runner_label', '--')}]",
        "",
        "<b>— BONUS QUALITY —</b>",
        bonus_txt,
        "",
        "Narrative ✔  Sweep ✔  Displacement ✔  BOS ✔  Entry zone set.",
        "<b>Risk 1-2% only.</b>",
    ]
    return "\n".join(lines)


def build_status_digest(scored_pairs):
    now    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    h, m   = _wat_now()
    in_win, win_name = in_execution_window()
    win_tag = f"🟢 {win_name}" if in_win else "⏸ Outside window"

    active   = [(sym, st) for sym, st in pair_states.items()
                if st.get("state") != "WAITING_FOR_SWEEP"]
    rejected = list(pair_rejections.items())[-15:]

    # ── Header ────────────────────────────────────────────────────────────────
    lines = [
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "<b>📊 BEE-M STATUS DIGEST</b>",
        f"🕐 {now}  ({h:02d}:{m:02d} WAT)",
        f"Window: {win_tag}",
        f"Pairs tracked: <b>{len(pair_states)}</b>  |  Active setups: <b>{len(active)}</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    # ── Active setups ─────────────────────────────────────────────────────────
    if active:
        lines.append("<b>🔔 ACTIVE SETUPS</b>")
        lines.append("")
        for sym, st in active:
            state  = st.get("state", "?")
            bias   = st.get("bias", "?").upper()
            rating = st.get("setup_score", {}).get("rating", "")
            pair   = sym.replace("_", "/")
            bias_icon = "🟢" if bias == "BULLISH" else "🔴"

            if state == "SWEEP_CONFIRMED":
                age = int(state_age(st, "sweep_time") / 60)
                lvl = st.get("sweep", {}).get("sweep_level", 0)
                lines.append(f"{bias_icon} <b>{pair}</b>  {bias}  |  Step 4 — Sweep @ {lvl:.4f}")
                lines.append(f"   ⏳ {age}min ago  |  Waiting: Displacement + BOS")

            elif state == "WAITING_FOR_RETRACEMENT":
                age = int(state_age(st, "bos_time") / 60)
                ote = st.get("ote", {})
                risk = st.get("risk", {})
                fvg  = st.get("fvg_ote", {})
                fvg_tag = "  📌 FVG in zone" if fvg and fvg.get("found") else ""
                lines.append(f"{bias_icon} <b>{pair}</b>  {bias}  [{rating}]  |  Step 6 — BOS ✔")
                lines.append(f"   OTE zone: {ote.get('ote_low',0):.4f} – {ote.get('ote_high',0):.4f}{fvg_tag}")
                if risk:
                    lines.append(f"   Entry: {risk.get('entry',0):.4f}  |  SL: {risk.get('sl',0):.4f}  |  TP1: {risk.get('tp1',0):.4f}")
                lines.append(f"   ⏳ {age}min since BOS  |  Waiting: Model A/B entry")

            elif state == "INVALIDATED":
                age       = int((time.time() - st.get("invalidated_at", time.time())) / 60)
                remaining = max(0, int((MAX_FLIP_AGE - (time.time() - st.get("invalidated_at", time.time()))) / 60))
                flip_bias = st.get("flip_bias", "?").upper()
                lines.append(f"⚠️ <b>{pair}</b>  INVALIDATED  |  Was: {bias}  →  Watching: {flip_bias}")
                lines.append(f"   {age}min ago  |  Flip window: {remaining}min left")

            elif state == "TRADE_TAKEN":
                lines.append(f"{bias_icon} <b>{pair}</b>  {bias}  |  ✅ Trade fired — resetting")

            lines.append("")
        lines.append("─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─")
        lines.append("")

    # ── Recent rejections ─────────────────────────────────────────────────────
    if rejected:
        lines.append("<b>❌ RECENT REJECTIONS  (why no trade)</b>")
        lines.append("")
        for sym, rej in rejected:
            lines.append(
                f"  <b>{sym.replace('_','/')}</b>  |  {rej['step']}\n"
                f"  ↳ {rej['reason']}  [{rej['time']}]"
            )
            lines.append("")
        lines.append("─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─")
        lines.append("")

    # ── Top pairs ─────────────────────────────────────────────────────────────
    if scored_pairs:
        lines.append("<b>📈 TOP PAIRS THIS CYCLE</b>")
        lines.append("")
        for p in scored_pairs[:8]:
            sym = p["symbol"].replace("_", "/")
            chg_icon = "▲" if p['change_pct'] >= 0 else "▼"
            lines.append(
                f"  <b>{sym}</b>  {chg_icon}{abs(p['change_pct']):.1f}%"
                f"  |  Score: {p['score']:.0f}"
                f"  |  {p['reason']}"
            )
        lines.append("")

    # ── Footer ────────────────────────────────────────────────────────────────
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"Next scan in ~{SCAN_INTERVAL_KZ // 60} min  |  /setups  /pairs  /status  /help")
    return "\n".join(lines)



# ═══════════════════════════════════════════════════════════════════════════
# MODEL SELECTION — TREND DETECTION + CONTINUATION SETUP
# ═══════════════════════════════════════════════════════════════════════════

def classify_market_condition(h4, daily, bd):
    """
    THE 'WHERE AM I?' DECISION LAYER  (Playbook Step 1B)

    This runs BEFORE model selection. Returns one of:
      "extreme"      → price at/near PDH, PDL, or major swing — use Reversal Model
      "trending"     → clear HH+HL or LH+LL, price NOT at extremes — use Continuation Model
      "ranging"      → no clear structure — default to Reversal Model (wait for sweep)

    Also returns:
      trend_dir      → "bullish" | "bearish" | "none"
      condition_note → human-readable explanation for the alert
    """
    if len(h4) < 10 or len(daily) < 3:
        return {"condition": "ranging", "trend_dir": "none",
                "condition_note": "Insufficient data — defaulting to ranging"}

    price    = h4[-1]["close"]
    pdh      = bd["pdh"]
    pdl      = bd["pdl"]
    pd_range = pdh - pdl

    # ── 1. Am I at extremes? ─────────────────────────────────────────────────
    # "Extreme" means price is RIGHT AT the level — within 3% of PDH/PDL
    # (tightened from 8% so mid-trend moves toward POI are NOT blocked)
    # Also check 4H swing high/low but only within 3%
    recent_4h = h4[-20:]
    h4_high   = max(c["high"] for c in recent_4h)
    h4_low    = min(c["low"]  for c in recent_4h)
    h4_rng    = h4_high - h4_low if h4_high != h4_low else 1

    near_pdh     = pd_range > 0 and (price >= pdh - pd_range * 0.03)
    near_pdl     = pd_range > 0 and (price <= pdl + pd_range * 0.03)
    near_h4_high = h4_rng > 0 and (price >= h4_high - h4_rng * 0.03)
    near_h4_low  = h4_rng > 0 and (price <= h4_low  + h4_rng * 0.03)
    at_extreme   = near_pdh or near_pdl or near_h4_high or near_h4_low

    if at_extreme:
        extreme_side = "PDH/4H-high" if (near_pdh or near_h4_high) else "PDL/4H-low"
        return {
            "condition":      "extreme",
            "trend_dir":      "none",
            "condition_note": f"Price at {extreme_side} — Reversal Model active",
        }

    # ── 2. Is there a clear trend? ───────────────────────────────────────────
    # Use last 6 CLOSED 4H candles, check for HH+HL or LH+LL
    # Need at least 4 of 6 swing comparisons to agree
    s = h4[-7:-1] if len(h4) >= 7 else h4[:-1]
    if len(s) < 4:
        return {"condition": "ranging", "trend_dir": "none",
                "condition_note": "Not enough 4H candles to determine trend"}

    hs = [c["high"] for c in s]
    ls = [c["low"]  for c in s]
    n  = len(hs) - 1   # number of comparisons

    bull_h = sum(hs[i] < hs[i+1] for i in range(n))
    bull_l = sum(ls[i] < ls[i+1] for i in range(n))
    bear_h = sum(hs[i] > hs[i+1] for i in range(n))
    bear_l = sum(ls[i] > ls[i+1] for i in range(n))

    # Threshold: at least 60% of swings trending (relaxed from v12's 120%)
    threshold = n * 0.60

    is_bull = (bull_h >= threshold) and (bull_l >= threshold)
    is_bear = (bear_h >= threshold) and (bear_l >= threshold)

    # Price must be holding above/below the 4H midpoint to confirm trend
    h4_mid           = h4_low + h4_rng * 0.5
    holds_above_mid  = price > h4_mid
    holds_below_mid  = price < h4_mid

    if is_bull and holds_above_mid:
        return {
            "condition":      "trending",
            "trend_dir":      "bullish",
            "condition_note": f"4H HH+HL ({bull_h}/{n} highs, {bull_l}/{n} lows) + above mid — Continuation Model",
        }

    if is_bear and holds_below_mid:
        return {
            "condition":      "trending",
            "trend_dir":      "bearish",
            "condition_note": f"4H LH+LL ({bear_h}/{n} highs, {bear_l}/{n} lows) + below mid — Continuation Model",
        }

    # ── 3. Trending TOWARD a POI? ────────────────────────────────────────────
    # Price is inside the PD range, not yet at extreme, but 15M structure is
    # making directional moves toward PDH (bullish bias) or PDL (bearish bias).
    # Example: Bias bearish, target PDL — price is mid-range but LH+LL on 15M
    #          → valid continuation shorts ON THE WAY to PDL
    # This is the "in-transit" scenario — riding the move to your POI.
    if pd_range > 0:
        # Where is price relative to PD range? (0.0 = at PDL, 1.0 = at PDH)
        price_pct = (price - pdl) / pd_range

        # Bullish bias: price in lower or middle portion, heading toward PDH
        # (below 65% of range — not yet at PDH extreme)
        if bd["bias"] == "bullish" and price_pct < 0.65:
            # Check 15M for HH+HL structure (upward progress)
            # Use 4H as proxy — if not classic uptrend but bias is bullish
            # and price is in lower half, treat as trending-toward-POI
            if holds_above_mid or price_pct > 0.35:
                return {
                    "condition":      "trending",
                    "trend_dir":      "bullish",
                    "condition_note": (
                        f"Bullish bias, price {price_pct:.0%} through range — "
                        f"trending toward PDH {pdh:.4f} — Continuation Model"
                    ),
                }

        # Bearish bias: price in upper or middle portion, heading toward PDL
        # (above 35% of range — not yet at PDL extreme)
        if bd["bias"] == "bearish" and price_pct > 0.35:
            if holds_below_mid or price_pct < 0.65:
                return {
                    "condition":      "trending",
                    "trend_dir":      "bearish",
                    "condition_note": (
                        f"Bearish bias, price {price_pct:.0%} through range — "
                        f"trending toward PDL {pdl:.4f} — Continuation Model"
                    ),
                }

    # ── 4. Ranging — no clear structure ─────────────────────────────────────
    return {
        "condition":      "ranging",
        "trend_dir":      "none",
        "condition_note": f"No clear structure (bull:{bull_h+bull_l} bear:{bear_h+bear_l} of {n*2}) — Reversal Model",
    }

# ── PDF2 fix #5: Narrative → Model mapping ────────────────────────────────
# Condition      → Preferred Model
# trending       → Model 2 (Continuation)
# extreme        → Model 1 (Reversal)
# expansion/htf  → Model 3 (HTF Expansion)
# This mapping is enforced inside classify_behavior routing above.


def detect_continuation_setup(m15, h4, bias):
    """
    MODEL 2 — CONTINUATION DETECTION  (dedicated path, no reversal fallback)

    Does NOT require PDH/PDL sweep or major liquidity event.
    Detects purely from price structure:
      1. Impulse leg in trend direction (creates HH or LL on 15M)
      2. Pullback that holds structure (HL stays above prior HL, or LH below prior LH)
      3. Internal sweep OR compression during pullback
      4. Continuation BOS — price closes above minor swing high (bull) or below swing low (bear)

    Returns:
      valid, entry_price, sl_level, target, entry_type, reason,
      pullback_depth_pct  (how deep the pullback was — context for alert)
    """
    if len(m15) < 20:
        return {"valid": False, "reason": "Not enough 15M candles"}

    recent   = m15[-40:] if len(m15) >= 40 else m15
    price    = m15[-1]["close"]
    avg_body = sum(abs(c["close"] - c["open"]) for c in recent) / max(len(recent), 1)
    if avg_body == 0:
        return {"valid": False, "reason": "Zero avg body"}

    if bias == "bullish":
        # ── Step 1: Find most recent impulse high ────────────────────────────
        # Impulse = a strong directional move, identified as the highest point
        # in the last 20 candles
        impulse_window = recent[-20:]
        impulse_idx    = max(range(len(impulse_window)), key=lambda i: impulse_window[i]["high"])
        impulse_high   = impulse_window[impulse_idx]["high"]

        # Need at least 3 candles after the impulse for a pullback
        candles_after_impulse = recent[-(20 - impulse_idx):]
        if len(candles_after_impulse) < 3:
            return {"valid": False, "reason": "Impulse too recent — no pullback formed yet"}

        # ── Step 2: Pullback low after impulse ───────────────────────────────
        pullback_low = min(c["low"] for c in candles_after_impulse)

        # ── Step 2a: Pullback depth — must be meaningful but not too deep ────
        impulse_range = impulse_high - min(c["low"] for c in impulse_window[:impulse_idx+1])
        if impulse_range <= 0:
            return {"valid": False, "reason": "Zero impulse range"}
        pullback_depth = (impulse_high - pullback_low) / impulse_range
        if pullback_depth < 0.20:
            return {"valid": False, "reason": f"Pullback too shallow ({pullback_depth:.0%}) — not enough retrace"}
        if pullback_depth > 0.75:
            return {"valid": False, "reason": f"Pullback too deep ({pullback_depth:.0%}) — structure may be broken"}

        # ── Step 2b: Structure holds — pullback low > prior swing low ────────
        prior_low = min(c["low"] for c in recent[:-20]) if len(recent) > 20 else pullback_low
        if pullback_low <= prior_low:
            return {"valid": False, "reason": f"HL broken — pullback ({pullback_low:.4f}) broke prior low ({prior_low:.4f})"}

        # ── Step 3: Internal sweep OR compression ────────────────────────────
        pb_candles     = candles_after_impulse
        internal_sweep = any(
            pb_candles[i]["low"] < pb_candles[i-1]["low"] and
            pb_candles[i]["close"] > pb_candles[i-1]["low"]
            for i in range(1, len(pb_candles))
        )
        last4      = pb_candles[-4:] if len(pb_candles) >= 4 else pb_candles
        ranges4    = [c["high"] - c["low"] for c in last4]
        compression = (
            len(ranges4) >= 3 and
            ranges4[-1] < ranges4[0] * 0.65 and
            all(r > 0 for r in ranges4)
        )

        # ── Step 4: Continuation BOS ─────────────────────────────────────────
        # Price must close above the highest point of the pullback (minor swing high)
        pb_swing_high = max(c["high"] for c in pb_candles[:-1]) if len(pb_candles) > 1 else impulse_high
        cont_bos      = price > pb_swing_high

        if not cont_bos:
            return {
                "valid":  False,
                "reason": f"No continuation BOS yet — price {price:.4f} needs to break {pb_swing_high:.4f}",
            }

        entry_type = "internal_sweep_BOS" if internal_sweep else \
                     "compression_BOS" if compression else "continuation_BOS"

        return {
            "valid":             True,
            "entry_price":       price,
            "sl_level":          pullback_low * 0.9995,
            "target":            impulse_high,
            "entry_type":        entry_type,
            "pullback_depth_pct": round(pullback_depth * 100, 1),
            "reason": (
                f"HH impulse to {impulse_high:.4f} → pullback {pullback_depth:.0%} → "
                f"HL held at {pullback_low:.4f} → BOS above {pb_swing_high:.4f} [{entry_type}]"
            ),
        }

    elif bias == "bearish":
        # Mirror logic for bearish
        impulse_window = recent[-20:]
        impulse_idx    = min(range(len(impulse_window)), key=lambda i: impulse_window[i]["low"])
        impulse_low    = impulse_window[impulse_idx]["low"]

        candles_after_impulse = recent[-(20 - impulse_idx):]
        if len(candles_after_impulse) < 3:
            return {"valid": False, "reason": "Impulse too recent — no pullback formed yet"}

        pullback_high  = max(c["high"] for c in candles_after_impulse)
        impulse_range  = max(c["high"] for c in impulse_window[:impulse_idx+1]) - impulse_low
        if impulse_range <= 0:
            return {"valid": False, "reason": "Zero impulse range"}

        pullback_depth = (pullback_high - impulse_low) / impulse_range
        if pullback_depth < 0.20:
            return {"valid": False, "reason": f"Pullback too shallow ({pullback_depth:.0%})"}
        if pullback_depth > 0.75:
            return {"valid": False, "reason": f"Pullback too deep ({pullback_depth:.0%}) — structure may be broken"}

        prior_high = max(c["high"] for c in recent[:-20]) if len(recent) > 20 else pullback_high
        if pullback_high >= prior_high:
            return {"valid": False, "reason": f"LH broken — pullback ({pullback_high:.4f}) broke prior high ({prior_high:.4f})"}

        pb_candles     = candles_after_impulse
        internal_sweep = any(
            pb_candles[i]["high"] > pb_candles[i-1]["high"] and
            pb_candles[i]["close"] < pb_candles[i-1]["high"]
            for i in range(1, len(pb_candles))
        )
        last4       = pb_candles[-4:] if len(pb_candles) >= 4 else pb_candles
        ranges4     = [c["high"] - c["low"] for c in last4]
        compression = (
            len(ranges4) >= 3 and
            ranges4[-1] < ranges4[0] * 0.65 and
            all(r > 0 for r in ranges4)
        )

        pb_swing_low = min(c["low"] for c in pb_candles[:-1]) if len(pb_candles) > 1 else impulse_low
        cont_bos     = price < pb_swing_low

        if not cont_bos:
            return {
                "valid":  False,
                "reason": f"No continuation BOS yet — price {price:.4f} needs to break {pb_swing_low:.4f}",
            }

        entry_type = "internal_sweep_BOS" if internal_sweep else \
                     "compression_BOS" if compression else "continuation_BOS"

        return {
            "valid":             True,
            "entry_price":       price,
            "sl_level":          pullback_high * 1.0005,
            "target":            impulse_low,
            "entry_type":        entry_type,
            "pullback_depth_pct": round(pullback_depth * 100, 1),
            "reason": (
                f"LL impulse to {impulse_low:.4f} → pullback {pullback_depth:.0%} → "
                f"LH held at {pullback_high:.4f} → BOS below {pb_swing_low:.4f} [{entry_type}]"
            ),
        }

    return {"valid": False, "reason": "Unknown bias"}


# ═══════════════════════════════════════════════════════════════════════════
# DECISION ENGINE  —  classify_behavior() + Model 3 detection
# ═══════════════════════════════════════════════════════════════════════════

def detect_htf_expansion(m15, h4, bias, sweep, pdh, pdl):
    """
    MODEL 3 — HTF Expansion → Shift detection.

    Pattern: Sweep → strong expansion candle into a HTF level (PDH/PDL or
    4H swing) → price forms LH (bearish) or HL (bullish) → BOS confirms shift.

    Three conditions required:
      1. Strong expansion: the displacement candle that follows the sweep
         has body ratio >= 0.75 AND range >= 1.5x the 20-candle avg range
      2. Expansion targets a HTF level: the expansion high (bull) or low (bear)
         reaches within 2% of PDH, PDL, or a 4H swing extreme
      3. Structural shift forming: a LH (bear) or HL (bull) has appeared after
         the expansion — i.e. price rejected off the HTF level

    Returns dict with valid, reason, htf_level, expansion_candle_idx
    """
    if len(m15) < 10 or not sweep.get("swept"):
        return {"valid": False, "reason": "Insufficient data or no sweep"}

    si  = sweep.get("candle_idx", len(m15) - 5)
    # Look at candles after the sweep
    post_sweep = m15[si:]
    if len(post_sweep) < 3:
        return {"valid": False, "reason": "Not enough candles after sweep"}

    # Condition 1: Find a strong expansion candle in post-sweep window
    avg_range = sum(c["high"] - c["low"] for c in m15[-20:]) / 20
    if avg_range <= 0:
        return {"valid": False, "reason": "Zero average range"}

    expansion_c   = None
    expansion_idx = -1
    for i, c in enumerate(post_sweep[:8]):   # look within 8 candles of sweep
        rng  = c["high"] - c["low"]
        body = abs(c["close"] - c["open"])
        body_ratio = body / rng if rng > 0 else 0
        is_directional = (bias == "bullish" and c["close"] > c["open"]) or                          (bias == "bearish" and c["close"] < c["open"])
        if body_ratio >= 0.75 and rng >= avg_range * 1.5 and is_directional:
            expansion_c   = c
            expansion_idx = si + i
            break

    if not expansion_c:
        return {"valid": False, "reason": "No strong expansion candle found after sweep"}

    # Condition 2: Expansion reaches toward a HTF level
    pd_range   = pdh - pdl if pdh > pdl else 1
    h4_recent  = h4[-20:]
    h4_high    = max(c["high"] for c in h4_recent)
    h4_low     = min(c["low"]  for c in h4_recent)

    htf_levels = [pdh, pdl, h4_high, h4_low]
    htf_level  = None
    if bias == "bullish":
        exp_extreme = expansion_c["high"]
        for lvl in [pdh, h4_high]:
            if lvl > 0 and abs(exp_extreme - lvl) / lvl <= 0.02:
                htf_level = lvl
                break
    else:
        exp_extreme = expansion_c["low"]
        for lvl in [pdl, h4_low]:
            if lvl > 0 and abs(exp_extreme - lvl) / lvl <= 0.02:
                htf_level = lvl
                break

    if htf_level is None:
        return {
            "valid":  False,
            "reason": f"Expansion did not reach HTF level (exp extreme {exp_extreme:.4f}, "
                      f"nearest: PDH {pdh:.4f} / PDL {pdl:.4f} / 4H-H {h4_high:.4f} / 4H-L {h4_low:.4f})",
        }

    # Condition 3: Structural shift forming — LH (bear) or HL (bull) after expansion
    post_exp = m15[expansion_idx + 1:]
    if len(post_exp) < 2:
        return {"valid": False, "reason": "Not enough candles after expansion for shift check"}

    if bias == "bullish":
        # After bullish expansion into PDH/4H-H, we need a HL to form
        # (price pulled back but held a higher low vs pre-expansion)
        pre_exp_low  = min(c["low"] for c in m15[max(0, si-5):si+1])
        post_exp_low = min(c["low"] for c in post_exp[:6])
        shift_forming = post_exp_low > pre_exp_low
        shift_desc    = f"HL held at {post_exp_low:.4f} (above pre-exp low {pre_exp_low:.4f})"
    else:
        pre_exp_high  = max(c["high"] for c in m15[max(0, si-5):si+1])
        post_exp_high = max(c["high"] for c in post_exp[:6])
        shift_forming = post_exp_high < pre_exp_high
        shift_desc    = f"LH formed at {post_exp_high:.4f} (below pre-exp high {pre_exp_high:.4f})"

    if not shift_forming:
        return {"valid": False, "reason": f"No structural shift after expansion — {'HL' if bias=='bullish' else 'LH'} not confirmed"}

    return {
        "valid":              True,
        "htf_level":          htf_level,
        "expansion_candle":   expansion_c,
        "expansion_idx":      expansion_idx,
        "shift_desc":         shift_desc,
        "reason": (
            f"Expansion into HTF {htf_level:.4f} "
            f"(body {abs(expansion_c['close']-expansion_c['open'])/(expansion_c['high']-expansion_c['low']):.0%}) "
            f"→ {shift_desc}"
        ),
    }


def classify_behavior(m15, h4, daily, bias, sweep, mc, pdh, pdl):
    """
    THE DECISION ENGINE — classifies market behavior ONCE after a sweep.

    Called exactly once per setup when entering BEHAVIOR_CLASSIFICATION.
    Returns "MODEL_1", "MODEL_2", or "MODEL_3" — never None (falls back to MODEL_1).

    Routing logic:
      MODEL_3 checked FIRST — it's the most specific pattern (expansion into HTF)
      MODEL_2 checked SECOND — requires clear trend context
      MODEL_1 is the default — reversal at extremes or ranging markets

    CRITICAL: result is written to pair_states[sym]["locked_model"] and NEVER
    re-evaluated until state resets. This prevents flip-flopping between models.

    PDF2 fix #2: Once locked, ALL future logic must respect that model only.
    No re-evaluation until reset — enforced structurally by the state machine.
    """
    # MODEL_3: sweep → strong expansion into HTF level → shift forming
    m3 = detect_htf_expansion(m15, h4, bias, sweep, pdh, pdl)
    if m3["valid"]:
        log.debug(f"classify_behavior → MODEL_3: {m3['reason']}")
        return "MODEL_3", m3

    # MODEL_2: trending market, structure intact, NOT at extreme
    is_trending   = mc["condition"] == "trending" and mc.get("trend_dir") == bias
    if is_trending:
        cont = detect_continuation_setup(m15, h4, bias)
        if cont["valid"]:
            log.debug(f"classify_behavior → MODEL_2: {cont['reason']}")
            return "MODEL_2", cont

    # MODEL_1: default — reversal at extremes, ranging, or MODEL_2 conditions not met
    log.debug(f"classify_behavior → MODEL_1 (default): mc={mc['condition']}")
    return "MODEL_1", {}


# ═══════════════════════════════════════════════════════════════════════════
# STATE MACHINE
# ═══════════════════════════════════════════════════════════════════════════

def run_state_machine(sym, m15, h4, daily, bd, loc_data, structure, pair_score, market_condition=None, live_price=None):
    st        = get_state(sym)
    bias      = bd["bias"]
    loc       = loc_data["location"]
    pdh       = bd["pdh"]
    pdl       = bd["pdl"]
    now       = time.time()
    mc        = market_condition or {"condition": "ranging", "trend_dir": "none",
                                      "condition_note": "default"}

    # Bias flip resets everything
    if st["state"] not in ("WAITING_FOR_SWEEP",):
        if st.get("bias", bias) != bias:
            reset_state(sym, f"Bias flipped {st.get('bias')} to {bias}")
            return

    # Fix 2: Update narrative adaptively on every cycle for active setups
    if st["state"] in ("BEHAVIOR_CLASSIFICATION", "SWEEP_CONFIRMED", "WAITING_FOR_RETRACEMENT"):
        update_narrative(sym, m15, h4, daily, live_price=live_price)

    # ════════════════════════════════════════════════════════════════════════
    # BEHAVIOR_CLASSIFICATION STATE
    # Model is classified ONCE after sweep — never re-decided until reset.
    # Rule: "Once MODEL is selected after sweep → DO NOT reclassify until RESET"
    # ════════════════════════════════════════════════════════════════════════
    if st["state"] == "BEHAVIOR_CLASSIFICATION":
        locked  = st.get("locked_model")
        m3_ctx  = st.get("locked_model_ctx", {})

        if not locked:
            log.warning(f"{sym}: BEHAVIOR_CLASSIFICATION with no locked_model — re-classifying")
            locked, ctx = classify_behavior(m15, h4, daily, bias, st.get("sweep", {}), mc, pdh, pdl)
            st["locked_model"]     = locked
            st["locked_model_ctx"] = ctx
        else:
            # PDF2 fix #2: Model is locked — NEVER re-classify. Log and route only.
            log.debug(f"{sym}: Model already locked as {locked} — no re-classification")

        log.debug(f"{sym}: routing locked={locked}")

        if locked == "MODEL_2":
            # ── MODEL 2 — CONTINUATION ────────────────────────────────────────
            cont = detect_continuation_setup(m15, h4, bias)
            if not cont["valid"]:
                record_rejection(sym, "Model 2 — waiting", cont.get("reason", "Conditions not met yet"))
                log.info(f"{sym}: [MODEL_2 LOCKED] waiting — {cont.get('reason', '')}")
                return   # wait — do NOT fall through to Model 1

            in_window, win_name = in_execution_window()
            win_tag = f"🟢 {win_name}" if in_window else "⚠️ Outside window — your call"
            narr    = build_narrative(bd, loc_data, m15, market_condition=mc, live_price=live_price)
            entry   = cont["entry_price"]
            sl      = cont["sl_level"]
            rr      = abs(entry - sl)
            target  = cont.get("target", entry)
            tp1     = entry + rr * 2 if bias == "bullish" else entry - rr * 2
            tp2     = target
            runner  = entry + rr * 5 if bias == "bullish" else entry - rr * 5

            today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            key       = f"{sym}_{bias}_cont_{today_str}"
            if key not in alerted_today:
                if win_name:
                    allowed_b, limit_reason_b = _check_trade_limit(win_name, sym, "A")
                    if not allowed_b:
                        record_rejection(sym, "Trade Limiter (Model 2)", limit_reason_b)
                        log.info(f"{sym}: TRADE LIMIT BLOCKED (Model 2) — {limit_reason_b}")
                        send_telegram(f"<i>⛔ Blocked — {sym.replace('_', '/')} {bias.upper()} Model 2\n{limit_reason_b}</i>")
                        st["state"] = "TRADE_TAKEN"
                        return

                alerted_today.add(key)
                is_in_transit = "trending toward" in mc.get("condition_note", "").lower()
                model_label   = "Model 2 — In-Transit" if is_in_transit else "Model 2 — Trend Continuation"
                log.info(f"{sym}: MODEL 2 ENTRY [{model_label}] — {cont['entry_type']}")
                send_telegram(
                    f"<b>🔵 {'IN-TRANSIT' if is_in_transit else 'CONTINUATION'} ENTRY — {sym.replace('_', '/')} {bias.upper()}</b>\n"
                    f"Model: <b>{model_label}</b>  [Locked at sweep]\n"
                    f"Window: {win_tag}\n\n"
                    f"<b>Condition:</b> {mc['condition_note']}\n"
                    f"<b>Setup:</b> {cont['reason']}\n"
                    f"<b>Pullback depth:</b> {cont.get('pullback_depth_pct', '?')}%\n\n"
                    f"<b>Narrative:</b>\n"
                    f"  What: {narr.get('what', '')}\n"
                    f"  Expect: {narr.get('expect', '')}\n"
                    f"  Inval: {narr.get('inval', '')}\n\n"
                    f"Entry:        <b>{entry:.4f}</b>  ({cont['entry_type']})\n"
                    f"SL:           <b>{sl:.4f}</b>  (below pullback HL/LH)\n"
                    f"TP1 (50%):    <b>{tp1:.4f}</b>  (~1:2)\n"
                    f"TP2 (30%):    <b>{tp2:.4f}</b>  ({'POI target' if is_in_transit else 'impulse H/L'})\n"
                    f"Runner (20%): <b>{runner:.4f}</b>  (external liquidity)\n\n"
                    f"Bias: {bd['score']:+d}  |  Location: {loc.upper()}\n"
                    f"<b>Risk 1-2% only.</b>"
                )
                _register_trade(win_name, sym, bias, model_label, 0.0, "A",
                                entry_price=entry, sl=sl, tp1=tp1)

            st["state"] = "TRADE_TAKEN"
            return

        elif locked == "MODEL_3":
            # ── MODEL 3 — HTF EXPANSION → SHIFT ──────────────────────────────
            sweep  = st.get("sweep", {})
            si     = sweep.get("candle_idx", len(m15) - 5)

            disp = detect_displacement(m15, bias, si)
            if not disp["valid"]:
                record_rejection(sym, "Model 3 — Displacement", disp["reason"])
                log.info(f"{sym}: [MODEL_3] waiting for displacement — {disp['reason']}")
                return

            bos = detect_bos(m15, bias, si, disp["avg_body"], loc)
            if not bos["broken"]:
                record_rejection(sym, "Model 3 — BOS", bos.get("reason", "BOS not confirmed"))
                log.info(f"{sym}: [MODEL_3] waiting for BOS — {bos.get('reason', '')}")
                return

            bos_body_ratio = bos.get("body_ratio", 0)
            if bos_body_ratio < 0.70:
                record_rejection(sym, "Model 3 — BOS strength", f"Weak BOS body {bos_body_ratio:.0%} < 70%")
                return

            mq  = classify_move_quality(disp)
            ote = calc_ote(m15, bias, si, bos["bos_level"], mq)
            if not ote:
                reset_state(sym, "Model 3: OTE calc failed")
                return

            fvg_ote     = fvg_inside_ote(m15, bias, ote["ote_high"], ote["ote_low"])
            setup_score = score_setup(sweep, disp, bos, fvg_ote, structure)
            risk        = calc_risk(bias, sweep, bos, ote, m15=m15, h4=h4, daily=daily)
            if not risk:
                reset_state(sym, "Model 3: risk calc failed")
                return

            htf_lvl     = m3_ctx.get("htf_level", 0)
            narr        = build_narrative(bd, loc_data, m15, market_condition=mc, live_price=live_price)
            in_window, win_name = in_execution_window()
            win_tag     = f"🟢 {win_name}" if in_window else "⚠️ Outside window — your call"

            today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            key       = f"{sym}_{bias}_m3_{today_str}"
            if key not in alerted_today:
                if win_name:
                    allowed_m3, limit_m3 = _check_trade_limit(win_name, sym, setup_score.get("rating", "B"))
                    if not allowed_m3:
                        record_rejection(sym, "Trade Limiter (Model 3)", limit_m3)
                        send_telegram(f"<i>⛔ Blocked — {sym.replace('_', '/')} Model 3\n{limit_m3}</i>")
                        st.update({"state": "WAITING_FOR_RETRACEMENT",
                                   "disp": disp, "bos": bos, "bos_time": now,
                                   "ote": ote, "fvg_ote": fvg_ote,
                                   "setup_score": setup_score, "risk": risk,
                                   "checks": 0, "last_price": 0.0, "narrative": narr})
                        return

                alerted_today.add(key)
                log.info(f"{sym}: MODEL 3 — HTF expansion. HTF={htf_lvl:.4f} BOS={bos['bos_level']:.4f}")
                send_telegram(
                    f"<b>🔶 HTF EXPANSION ENTRY — {sym.replace('_', '/')} {bias.upper()}</b>\n"
                    f"Model: <b>Model 3 — HTF Expansion → Shift</b>  [Locked at sweep]\n"
                    f"Window: {win_tag}\n\n"
                    f"<b>Pattern:</b> {m3_ctx.get('reason', '')}\n"
                    f"<b>HTF Level:</b> {htf_lvl:.4f}\n"
                    f"<b>BOS:</b> {bos['bos_level']:.4f} ({bos.get('bos_type', '')})\n\n"
                    f"<b>Narrative:</b>\n"
                    f"  What: {narr.get('what', '')}\n"
                    f"  Expect: {narr.get('expect', '')}\n"
                    f"  Inval: {narr.get('inval', '')}\n\n"
                    f"OTE Zone:     <b>{ote['ote_low']:.4f}–{ote['ote_high']:.4f}</b>\n"
                    f"Ideal:        <b>{ote['ideal']:.4f}</b>  (0.705)\n"
                    f"Entry:        <b>{risk['entry']:.4f}</b>\n"
                    f"SL:           <b>{risk['sl']:.4f}</b>  [{risk['sl_label']}]\n"
                    f"TP1 (50%):    <b>{risk['tp1']:.4f}</b>  [{risk.get('tp1_label', '--')}]\n"
                    f"TP2 (30%):    <b>{risk['tp2']:.4f}</b>  [{risk.get('tp2_label', '--')}]\n"
                    f"Runner (20%): <b>{risk['runner']:.4f}</b>  [{risk.get('runner_label', '--')}]\n\n"
                    f"Rating: <b>{setup_score['rating']}</b>  |  RR: {risk['rr_ratio']}:1\n"
                    f"<b>Risk 1-2% only.</b>"
                )
                _register_trade(win_name, sym, bias, "Model 3 — HTF Expansion",
                                bos.get("body_ratio", 0), setup_score.get("rating", "B"),
                                entry_price=risk.get("entry", 0),
                                sl=risk.get("sl", 0),
                                tp1=risk.get("tp1", 0))

            st.update({"state": "WAITING_FOR_RETRACEMENT",
                       "disp": disp, "bos": bos, "bos_time": now,
                       "ote": ote, "fvg_ote": fvg_ote,
                       "setup_score": setup_score, "risk": risk,
                       "checks": 0, "last_price": 0.0, "narrative": narr})
            return

        else:
            # ── MODEL 1 — REVERSAL: transition to SWEEP_CONFIRMED ─────────────
            st["state"] = "SWEEP_CONFIRMED"
            log.info(f"{sym}: BEHAVIOR_CLASSIFICATION → MODEL_1 → entering SWEEP_CONFIRMED")
            # Fall through immediately to SWEEP_CONFIRMED block below

    # ── WAITING_FOR_SWEEP ────────────────────────────────────────────────────
    if st["state"] == "WAITING_FOR_SWEEP":
        sweep = detect_true_sweep(m15, bias, pdh, pdl, loc)
        if not sweep["swept"]:
            record_rejection(sym, "Step 4 (Sweep)", sweep.get("reason", "No valid sweep"))
            log.info(f"{sym}: [WAITING_FOR_SWEEP] {sweep.get('reason', 'no sweep')}")
            return

        # ── CLASSIFY BEHAVIOR ONCE — lock model here, never re-classify ──────
        # This is the core upgrade from the PDF. After the sweep we classify
        # behavior exactly once and store the result. All subsequent scan cycles
        # route directly to the locked model without re-evaluating.
        locked_model, model_ctx = classify_behavior(m15, h4, daily, bias, sweep, mc, pdh, pdl)
        log.info(f"{sym}: SWEEP → classified as {locked_model} (context: {model_ctx.get('reason', 'default')})")

        st.update({
            "state":             "BEHAVIOR_CLASSIFICATION",
            "bias":              bias,
            "sweep":             sweep,
            "sweep_time":        now,
            "bd":                bd,
            "loc_data":          loc_data,
            "pair_score":        pair_score,
            "locked_model":      locked_model,
            "locked_model_ctx":  model_ctx,
        })

        stype      = sweep["sweep_type"].replace("_", " ").title()
        eq_tag     = " | Equal H/L" if sweep.get("is_equal_hl") else ""
        pdx_tag    = " | PDH/PDL" if sweep.get("is_pdx") else ""
        rej_tag    = " | Rejection ✔" if sweep.get("has_rejection") else " | No rejection (waiting for BOS)"
        conf       = sweep.get("confluence", 0)
        conf_label = "High-prob" if conf >= 2 else "Standard"
        narr       = build_narrative(bd, loc_data, m15, live_price=live_price)
        in_window, win_name = in_execution_window()
        win_tag    = f"🟢 {win_name}" if in_window else "⚠️ Outside execution window — your call"
        log.info(f"{sym}: SWEEP_CONFIRMED ({stype}{eq_tag}{pdx_tag}) confluence={conf}")

        # ── PDF1 fix #2: Tag sweep by session ────────────────────────────────
        in_window_sw, win_name_sw = in_execution_window()
        wat_h_sw, _ = _wat_now()
        flags_sw = _ensure_session_flags_fresh(sym)
        if in_window_sw:
            if is_london_session(wat_h_sw):
                flags_sw["london_sweep"] = True
                london_key = f"{sym}_london_swept_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
                alerted_today.add(london_key)
                log.info(f"{sym}: London sweep registered for NY gate")
            elif is_newyork_session(wat_h_sw):
                flags_sw["ny_sweep"] = True
                ny_key = f"{sym}_ny_swept_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
                alerted_today.add(ny_key)
                log.info(f"{sym}: NY sweep registered")

        # ── PDF1 fix #3: Tag internal sweeps ─────────────────────────────────
        if sweep.get("is_equal_hl"):
            flags_sw["internal_sweep"] = True
            log.debug(f"{sym}: internal sweep flagged at classification")

        send_telegram(
            f"<b>🔔 SWEEP DETECTED — {sym.replace('_', '/')} {bias.upper()}</b>\n"
            f"Level: <b>{sweep.get('sweep_level', 0):.4f}</b>  [{stype}{eq_tag}{pdx_tag}]\n"
            f"Confluence: <b>{conf_label} ({conf}/2+)</b>{rej_tag}\n"
            f"\n<b>Narrative:</b>\n"
            f"  Bias: {narr['strength'].replace('_',' ').title()}  (score {narr['score']:+d})\n"
            f"  What: {narr['what']}\n"
            f"  Target: {narr['target']}\n"
            f"  Expect: {narr['expect']}\n"
            f"  Inval: {narr['inval']}\n"
            f"\nWindow: {win_tag}\n"
            f"<i>Tracking: Displacement + BOS next...</i>"
        )

    # ── SWEEP_CONFIRMED ───────────────────────────────────────────────────────
    if st["state"] == "SWEEP_CONFIRMED":
        expired, exp_reason = sweep_expired(st)
        if expired:
            record_rejection(sym, "Step 6 (BOS)", f"Sweep expired — {exp_reason}")
            reset_state(sym, f"Sweep expired — {exp_reason}")
            send_telegram(
                f"<i>\u23f1 Sweep expired \u2014 {sym.replace('_', '/')} {st.get('bias', bias).upper()}\n"
                f"{exp_reason}\n"
                f"No BOS confirmed within the kill zone. Structure reset.\n"
                f"<b>Rule:</b> Sweep + BOS must complete in the same session window.</i>"
            )
            return

        # ── OPPOSITE SWEEP INVALIDATION (PDF rules) ────────────────────────
        # Invalidate ONLY when ALL three conditions are met:
        #   1. Opposite sweep happens BEFORE BOS (no confirmation yet)
        #   2. It is a MAJOR liquidity level (PDH/PDL or equal H/L — not internal noise)
        #   3. Shows clear displacement intent (strong move, not just a wick)
        # Rule: "Only meaningful liquidity can invalidate your setup"
        # Rule: "Sweep without intent ≠ invalidation"
        opp_bias  = "bearish" if bias == "bullish" else "bullish"
        opp_sweep = detect_true_sweep(m15, opp_bias, pdh, pdl, loc)
        if opp_sweep["swept"] and opp_sweep["age_candles"] < st["sweep"].get("age_candles", 999):
            is_major_level = opp_sweep.get("is_pdx") or opp_sweep.get("is_equal_hl")
            has_displacement = opp_sweep.get("has_rejection") or opp_sweep.get("confluence", 0) >= 2
            if is_major_level and has_displacement:
                reason = (
                    f"Opposite {opp_bias} sweep — major level "
                    f"({'PDH/PDL' if opp_sweep.get('is_pdx') else 'equal H/L'}) "
                    f"with displacement. Narrative broken."
                )
                record_rejection(sym, "Invalidation", reason)
                invalidate_state(sym, bias, reason)
                send_telegram(
                    f"<b>⚠️ INVALIDATED — {sym.replace('_', '/')} {bias.upper()}</b>\n"
                    f"{reason}\n\n"
                    f"<i>Watching for {opp_bias.upper()} flip conditions:\n"
                    f"Displacement + BOS {opp_bias} needed to confirm flip.\n"
                    f"Will alert if market proves new direction.</i>"
                )
                return
            else:
                # Minor sweep or no displacement — ignore per PDF rules
                log.info(
                    f"{sym}: Opposite sweep detected but ignored — "
                    f"major={is_major_level} displacement={has_displacement} "
                    f"(PDF rule: sweep without intent ≠ invalidation)"
                )

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

        # ── BOS STRENGTH FILTER (PnL fix #2 + Model 4 §11) ────────────────
        # Model 4 lowers the threshold slightly when momentum is active.
        bos_body_ratio    = bos.get("body_ratio", 0)
        m4_bos_threshold  = model4_get_bos_threshold(st)
        if bos_body_ratio < m4_bos_threshold:
            reason = (
                f"BOS rejected — weak displacement body ({bos_body_ratio:.0%} < {m4_bos_threshold:.0%}). "
                f"Chop break, not a real BOS."
            )
            record_rejection(sym, "Step 6 (BOS strength)", reason)
            log.info(f"{sym}: {reason}")
            return

        # ── PDF1 fix #6: Tighter conditions when no London sweep ──────────────
        flags_bos = _ensure_session_flags_fresh(sym)
        if not flags_bos["london_sweep"]:
            if bos_body_ratio < 0.7:
                record_rejection(sym, "Step 6 (BOS — no London sweep)",
                                 f"No London sweep context — BOS body {bos_body_ratio:.0%} < 0.7, rejected")
                log.info(f"{sym}: NO LONDON SWEEP — BOS too weak ({bos_body_ratio:.0%}), skipped")
                return
                record_rejection(sym, "Step 6 (BOS — no London sweep)",
                                 f"No London sweep context — BOS body {bos_body_ratio:.0%} < 0.7, rejected")
                log.info(f"{sym}: NO LONDON SWEEP — BOS too weak ({bos_body_ratio:.0%}), skipped")
                return
            setup_grade_check = score_setup(sweep, disp, bos, {}, structure).get("rating", "B")
            if setup_grade_check == "B":
                record_rejection(sym, "Step 6 (BOS — no London sweep)",
                                 "No London sweep context — only A/A+ setups allowed")
                log.info(f"{sym}: NO LONDON SWEEP — B-grade setup blocked, A+ required")
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
            "narrative":   build_narrative(bd, loc_data, m15, market_condition=mc, live_price=live_price),
        })

        log.info(
            f"{sym}: WAITING_FOR_RETRACEMENT | {setup_score['rating']} | "
            f"OTE {ote['ote_low']:.4f}-{ote['ote_high']:.4f} ({mq})"
        )

        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        key       = f"{sym}_{bias}_{today_str}"
        if key not in alerted_today:
            in_window, win_name = in_execution_window()

            # ── TRADE LIMITER — 3 layers (PnL fix #1) ──────────────────────────
            # 1. Per-window cap  2. Quality gate  3. Same-pair cooldown
            # Only gates INSIDE windows — outside window = informational, no count.
            if in_window:
                setup_grade_for_gate = setup_score.get("rating", "B")
                allowed, limit_reason = _check_trade_limit(win_name, sym, setup_grade_for_gate)
                if not allowed:
                    record_rejection(sym, "Trade Limiter", limit_reason)
                    log.info(f"{sym}: TRADE LIMIT BLOCKED — {limit_reason}")
                    send_telegram(
                        f"<i>⛔ Blocked — {sym.replace('_', '/')} {bias.upper()}\n"
                        f"{limit_reason}</i>"
                    )
                    return

            # ── MODEL ENFORCEMENT (PnL fix #4) ───────────────────────────────
            # Reversal (Model A) = sweep at extremes → OTE retrace
            # Continuation (Model B) = trending → pullback holds → BOS
            # This path is always Model A (reversal) — Model B fires separately above.
            enforced_model = "Model A — Reversal (OTE)"

            alerted_today.add(key)
            msg = build_setup_alert(
                sym, bd, loc_data, sweep, disp, bos,
                fvg_ote, ote, risk, setup_score, pair_score,
                narrative=build_narrative(bd, loc_data, m15, market_condition=mc, live_price=live_price),
                in_window=in_window,
                win_name=win_name,
            )
            send_telegram(msg)

            # Register trade — increment window count + apply pair lock
            _register_trade(win_name, sym, bias, enforced_model,
                            bos.get("body_ratio", 0), setup_score.get("rating", "B"),
                            entry_price=risk.get("entry", 0),
                            sl=risk.get("sl", 0),
                            tp1=risk.get("tp1", 0))
            log.info(f"{sym}: Setup alert sent ({'in window: ' + win_name if in_window else 'outside window'})")

    # ── WAITING_FOR_RETRACEMENT ───────────────────────────────────────────────
    if st["state"] == "WAITING_FOR_RETRACEMENT":

        in_window, win_name = in_execution_window()

        # ── MODEL 4: Update momentum state each cycle ─────────────────────
        update_model4_state(sym, m15, bias)

        # ── MODEL 4: Exhaustion — block entry OR exit early ───────────────
        if st.get("exhaustion"):
            record_rejection(sym, "Model 4 Exhaustion", "Top gainer showing exhaustion — entry blocked")
            log.info(f"{sym}: MODEL 4 EXHAUSTION — entry blocked")
            return

        ote_exp, ote_exp_reason = ote_expired(st)
        if ote_exp:
            ote  = st.get("ote", {})
            record_rejection(sym, "Step 7 (OTE retracement)", f"OTE expired — {ote_exp_reason}")
            reset_state(sym, f"OTE expired — {ote_exp_reason}")
            send_telegram(
                f"<i>\u23f1 Setup expired \u2014 {sym.replace('_', '/')} {st.get('bias', bias).upper()}\n"
                f"{ote_exp_reason}\n"
                f"OTE zone {ote.get('ote_low', 0):.4f}\u2013{ote.get('ote_high', 0):.4f} never reached within window.\n"
                f"Structure reset. Waiting for next setup.</i>"
            )
            return

        price     = get_price(sym)
        if price == 0:
            return

        sweep_lvl = st["sweep"].get("sweep_low", st["sweep"].get("sweep_high", 0))

        # ── POST-ENTRY INVALIDATION (PDF rule) ──────────────────────────────
        # Once BOS is confirmed and we are waiting for OTE retrace, opposite
        # sweeps are IGNORED — they are normal drawdown/mitigation, not reversal.
        # Rule: "After confirmation, trust your structure — not every wick."
        # Only hard invalidation: price closes BEYOND the original sweep extreme.
        if bias == "bullish" and sweep_lvl > 0 and price < sweep_lvl * 0.999:
            reason = f"Price broke below sweep extreme {sweep_lvl:.4f}"
            record_rejection(sym, "Invalidation", reason)
            invalidate_state(sym, bias, reason)
            send_telegram(
                f"<b>⚠️ INVALIDATED — {sym.replace('_', '/')} BULLISH</b>\n"
                f"{reason}\n\n"
                f"<i>Watching for bearish flip conditions:\n"
                f"Opposite sweep + Displacement + BOS down\n"
                f"Will alert if market confirms new direction.</i>"
            )
            return

        if bias == "bearish" and sweep_lvl > 0 and price > sweep_lvl * 1.001:
            reason = f"Price broke above sweep extreme {sweep_lvl:.4f}"
            record_rejection(sym, "Invalidation", reason)
            invalidate_state(sym, bias, reason)
            send_telegram(
                f"<b>⚠️ INVALIDATED — {sym.replace('_', '/')} BEARISH</b>\n"
                f"{reason}\n\n"
                f"<i>Watching for bullish flip conditions:\n"
                f"Opposite sweep + Displacement + BOS up\n"
                f"Will alert if market confirms new direction.</i>"
            )
            return

        ote     = st["ote"]
        risk    = st["risk"]
        in_zone = ote["ote_low"] <= price <= ote["ote_high"]

        if not in_zone:
            # ── v3: Model A not reached -- try Model B (momentum entry) ──
            mom = detect_momentum_entry(m15, bias, bos.get("bos_level", 0))
            if mom["valid"]:
                in_window, win_name = in_execution_window()
                win_tag = f"🟢 {win_name}" if in_window else "⚠️ Outside execution window — your call"
                log.info(f"{sym}: Model B triggered -- {mom['reason']}")
                risk_b = dict(risk)
                risk_b["entry"] = price
                recent_slice = m15[-10:]
                if bias == "bullish":
                    risk_b["sl"] = min(c["low"] for c in recent_slice) * 0.9995
                else:
                    risk_b["sl"] = max(c["high"] for c in recent_slice) * 1.0005
                narr = st.get("narrative", {})

                # ── Model 4: TP expansion on Model B ─────────────────────
                m4_tp_mult   = model4_get_tp_multiplier(st)
                m4_psych_tp  = st.get("tp_target")
                m4_hold_note = ""
                m4_tp_line   = ""
                if st.get("momentum_model"):
                    m4_hold_note = "\n🔥 <b>Model 4 ACTIVE</b> — Hold for expanded TP (no early exit)"
                    if m4_psych_tp:
                        m4_tp_line = f"\n   Psych TP target: <b>{m4_psych_tp:.4f}</b>  (Model 4)"
                    if m4_tp_mult != 1.0:
                        m4_tp_line += f"  [TP x{m4_tp_mult}]"

                send_telegram(
                    f"<b>🔥 ENTRY — Model B — {sym.replace('_', '/')} {bias.upper()}</b>\n"
                    f"<i>{mom['reason']}</i>\n"
                    f"Window: {win_tag}\n\n"
                    f"<b>Narrative:</b>  {narr.get('expect', '')}\n"
                    f"Invalidation: {narr.get('inval', '')}\n\n"
                    f"Price:        <b>{price:.4f}</b>  (momentum — no OTE retrace)\n"
                    f"Entry:        <b>{risk_b['entry']:.4f}</b>\n"
                    f"SL:           <b>{risk_b['sl']:.4f}</b>\n"
                    f"TP1 (50%):    <b>{risk_b['tp1']:.4f}</b>  [{risk_b.get('tp1_label', '--')}]\n"
                    f"TP2 (30%):    <b>{risk_b['tp2']:.4f}</b>  [{risk_b.get('tp2_label', '--')}]\n"
                    f"Runner (20%): <b>{risk_b['runner']:.4f}</b>  [{risk_b.get('runner_label', '--')}]"
                    f"{m4_tp_line}\n\n"
                    f"Rating: <b>{st['setup_score']['rating']}</b>  |  "
                    f"Core: Bias ✔  Sweep ✔  BOS ✔\n"
                    f"<b>Risk 1-2% only. Narrative confirmed — execute.</b>"
                    f"{m4_hold_note}"
                )
                log.info(f"{sym}: Model B ENTRY CONFIRMED at {price:.4f}")
                st["state"] = "TRADE_TAKEN"
                return

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
        in_window, win_name = in_execution_window()
        st["checks"]     = st.get("checks", 0) + 1
        prev             = st.get("last_price", price)
        thr              = 0.001
        reacted          = (bias == "bullish" and price > prev * (1 + thr)) or \
                           (bias == "bearish" and price < prev * (1 - thr))
        st["last_price"] = price

        if reacted:
            in_window, win_name = in_execution_window()
            win_tag = f"🟢 {win_name}" if in_window else "⚠️ Outside execution window — your call"
            fvg_note = ""
            fvg_ote  = st.get("fvg_ote", {})
            if fvg_ote and fvg_ote.get("found"):
                fvg_note = (
                    f"\nFVG in OTE: {fvg_ote['fvg_low']:.4f}–{fvg_ote['fvg_high']:.4f}"
                    f"  ← preferred entry"
                )
            narr = st.get("narrative", {})

            # ── Model 4: TP expansion and psych target ────────────────────
            m4_tp_mult   = model4_get_tp_multiplier(st)
            m4_psych_tp  = st.get("tp_target")
            m4_hold_note = ""
            m4_tp_line   = ""
            if st.get("momentum_model"):
                m4_hold_note = "\n🔥 <b>Model 4 ACTIVE</b> — Hold for expanded TP (no early exit)"
                if m4_psych_tp:
                    m4_tp_line = f"\n   Psych TP target: <b>{m4_psych_tp:.4f}</b>  (Model 4)"
                if m4_tp_mult != 1.0:
                    m4_tp_line += f"  [TP x{m4_tp_mult}]"

            send_telegram(
                f"<b>🔥 ENTRY — Model A — {sym.replace('_', '/')} {bias.upper()}</b>\n"
                f"Window: {win_tag}\n\n"
                f"<b>Narrative:</b>  {narr.get('expect', '')}\n"
                f"Invalidation: {narr.get('inval', '')}\n\n"
                f"Reaction in OTE at <b>{price:.4f}</b>{fvg_note}\n\n"
                f"Entry:        <b>{risk['entry']:.4f}</b>\n"
                f"SL:           <b>{risk['sl']:.4f}</b>\n"
                f"TP1 (50%):    <b>{risk['tp1']:.4f}</b>  [{risk.get('tp1_label', '--')}]\n"
                f"TP2 (30%):    <b>{risk['tp2']:.4f}</b>  [{risk.get('tp2_label', '--')}]\n"
                f"Runner (20%): <b>{risk['runner']:.4f}</b>  [{risk.get('runner_label', '--')}]"
                f"{m4_tp_line}\n\n"
                f"Rating: <b>{st['setup_score']['rating']}</b>\n"
                f"<b>Risk 1-2% only. Narrative confirmed — execute.</b>"
                f"{m4_hold_note}"
            )
            log.info(f"{sym}: Model A ENTRY CONFIRMED at {price:.4f}")
            st["state"] = "TRADE_TAKEN"
            return

        if st["checks"] >= 3:
            record_rejection(sym, "Step 7 (OTE no reaction)", "3 candles in zone -- no reaction, skipped")
            reset_state(sym, "3-candle skip rule triggered")
            send_telegram(
                f"<b>SKIP — {sym.replace('_', '/')} {bias.upper()}</b>\n\n"
                f"3 checks in OTE at {price:.4f} — no reaction.\n"
                f"<i>Rule: 3 candles, no reaction = skip. Do not chase. State reset.</i>"
            )
            return

        log.info(f"{sym}: [WAITING_FOR_RETRACEMENT] Model A check {st['checks']}/3 at {price:.4f}")

    # ── INVALIDATED — Structured Flip Model ──────────────────────────────────
    # TWO MODES:
    #   Conservative (default): Opposite sweep + Displacement + BOS
    #   Aggressive: Strong displacement + BOS only (no sweep required)
    #     → Triggers when displacement body ≥ 2.5x avg (very strong move)
    #     → Captures faster reversals that don't sweep a major level first
    if st["state"] == "INVALIDATED":
        flip_bias  = st.get("flip_bias", "")
        inv_reason = st.get("reason", "")
        inv_time   = st.get("invalidated_at", 0)

        # Timeout — no flip confirmed, reset cleanly
        if time.time() - inv_time > MAX_FLIP_AGE:
            mins = MAX_FLIP_AGE // 60
            reset_state(sym, f"Flip window expired ({mins}min) — no confirmation")
            send_telegram(
                f"<i>🔄 {sym.replace('_', '/')} — Flip window expired\n"
                f"No {flip_bias.upper()} confirmation within {mins} min.\n"
                f"Reset to WAITING_FOR_SWEEP.</i>"
            )
            return

        if not flip_bias:
            reset_state(sym, "Invalid flip state")
            return

        log.info(f"{sym}: [INVALIDATED] watching for {flip_bias} flip (conservative or aggressive)")

        # ── Try aggressive mode first (faster) ──────────────────────────────
        # Strong displacement + BOS — no sweep required
        # Use idx of most recent candles as proxy for "after invalidation"
        agg_si   = max(0, len(m15) - 8)   # look at last 8 candles
        agg_disp = detect_displacement(m15, flip_bias, agg_si)
        agg_bos  = detect_bos(m15, flip_bias, agg_si, agg_disp.get("avg_body", 0), loc) \
                   if agg_disp["valid"] else {"broken": False}

        # Aggressive only fires if displacement is VERY strong (≥2.5x avg body)
        avg_body   = agg_disp.get("avg_body", 0)
        body_ratio = agg_disp.get("body_ratio", 0)
        is_very_strong_disp = (
            agg_disp["valid"] and
            avg_body > 0 and
            body_ratio >= 0.70   # strong expansion candle
        )

        flip_mode    = None
        flip_si_used = None
        flip_sweep   = None

        if is_very_strong_disp and agg_bos["broken"]:
            flip_mode    = "aggressive"
            flip_si_used = agg_si
            flip_disp    = agg_disp
            flip_bos_res = agg_bos
            log.info(f"{sym}: [INVALIDATED] AGGRESSIVE flip — strong disp+BOS ({body_ratio:.0%})")
        else:
            # ── Conservative mode: sweep + displacement + BOS ────────────────
            flip_sweep = detect_true_sweep(m15, flip_bias, pdh, pdl, loc)
            if not flip_sweep["swept"]:
                record_rejection(sym, "Flip Step 1 (Sweep)",
                                 f"No {flip_bias} sweep — {flip_sweep.get('reason', '')}")
                log.info(f"{sym}: [INVALIDATED] waiting for sweep or strong displacement")
                return

            flip_si_used = flip_sweep["candle_idx"]
            flip_disp    = detect_displacement(m15, flip_bias, flip_si_used)
            if not flip_disp["valid"]:
                record_rejection(sym, "Flip Step 2 (Displacement)", flip_disp["reason"])
                log.info(f"{sym}: [INVALIDATED] sweep found, waiting for displacement")
                return

            flip_bos_res = detect_bos(m15, flip_bias, flip_si_used, flip_disp["avg_body"], loc)
            if not flip_bos_res["broken"]:
                record_rejection(sym, "Flip Step 3 (BOS)", flip_bos_res.get("reason", "No BOS yet"))
                log.info(f"{sym}: [INVALIDATED] sweep+disp found, waiting for BOS")
                return

            flip_mode = "conservative"
            log.info(f"{sym}: [INVALIDATED] CONSERVATIVE flip — sweep+disp+BOS all confirmed")

        # ── Flip confirmed (either mode) ─────────────────────────────────────
        flipped_bd           = dict(bd)
        flipped_bd["bias"]   = flip_bias
        mc_flip              = classify_market_condition(h4, daily, flipped_bd)
        flip_narr            = build_narrative(flipped_bd, loc_data, m15, market_condition=mc_flip)

        flip_mq  = classify_move_quality(flip_disp)
        flip_ote = calc_ote(m15, flip_bias, flip_si_used, flip_bos_res["bos_level"], flip_mq)
        flip_fvg = fvg_inside_ote(m15, flip_bias,
                                   flip_ote.get("ote_high", 0),
                                   flip_ote.get("ote_low", 0)) if flip_ote else {"found": False}

        in_window, win_name = in_execution_window()
        win_tag = f"🟢 {win_name}" if in_window else "⚠️ Outside window — your call"

        flip_risk = calc_risk(flip_bias,
                              flip_sweep or {"sweep_low": 0, "sweep_high": 0},
                              flip_bos_res, flip_ote,
                              m15=m15, h4=h4, daily=daily) if flip_ote else {}

        fvg_note  = (f"\nFVG in OTE: {flip_fvg['fvg_low']:.4f}–{flip_fvg['fvg_high']:.4f}  ← preferred"
                     if flip_fvg and flip_fvg.get("found") else "")
        mode_tag  = "⚡ Aggressive (Disp+BOS)" if flip_mode == "aggressive" else "🛡 Conservative (Sweep+Disp+BOS)"
        sweep_tag = (f"  ✔ Opposite sweep taken\n" if flip_sweep and flip_sweep.get("swept")
                     else "  ⚡ No sweep required (aggressive mode)\n")

        log.info(f"{sym}: FLIP CONFIRMED [{flip_mode}] — {flip_bias.upper()}")

        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        flip_key  = f"{sym}_{flip_bias}_flip_{today_str}"
        if flip_key not in alerted_today:
            alerted_today.add(flip_key)
            send_telegram(
                f"<b>🔄 FLIP CONFIRMED — {sym.replace('_', '/')} {flip_bias.upper()}</b>\n"
                f"Mode: {mode_tag}\n"
                f"Window: {win_tag}\n\n"
                f"Original {st.get('old_bias','').upper()} invalidated:\n"
                f"<i>{inv_reason}</i>\n\n"
                f"<b>Confirmed:</b>\n"
                f"{sweep_tag}"
                f"  ✔ Strong displacement {flip_bias} ({flip_disp.get('body_ratio',0):.0%} body)\n"
                f"  ✔ BOS confirmed {flip_bias}\n\n"
                f"<b>New Narrative:</b>\n"
                f"  What: {flip_narr.get('what', '')}\n"
                f"  Expect: {flip_narr.get('expect', '')}\n"
                f"  Inval: {flip_narr.get('inval', '')}\n\n"
                + (
                    f"Entry:        <b>{flip_risk.get('entry', 0):.4f}</b>{fvg_note}\n"
                    f"SL:           <b>{flip_risk.get('sl', 0):.4f}</b>\n"
                    f"TP1 (50%):    <b>{flip_risk.get('tp1', 0):.4f}</b>  [{flip_risk.get('tp1_label','--')}]\n"
                    f"TP2 (30%):    <b>{flip_risk.get('tp2', 0):.4f}</b>  [{flip_risk.get('tp2_label','--')}]\n"
                    f"Runner (20%): <b>{flip_risk.get('runner', 0):.4f}</b>  [{flip_risk.get('runner_label','--')}]\n\n"
                    if flip_risk else
                    f"Entry zone: {flip_ote.get('ote_low',0):.4f}–{flip_ote.get('ote_high',0):.4f}\n\n"
                    if flip_ote else
                    "Wait for retracement entry after BOS.\n\n"
                ) +
                f"<b>Risk 1-2% only.</b>"
            )

        reset_state(sym, f"Flip confirmed [{flip_mode}] {flip_bias} — tracking new setup")

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

    # Fetch live price separately — more accurate than m15[-1]["close"]
    # which could be the last CLOSED candle (up to 15min stale)
    live_price = get_price(sym)

    # ── SESSION FLAGS — ensure fresh for today (PDF1 fix #1 / #7) ───────────
    flags = _ensure_session_flags_fresh(sym)
    wat_hour, _ = _wat_now()

    # ── VOLATILITY KILL FILTER (PDF2 fix #4) ─────────────────────────────────
    vol_ok, vol_reason = _volatility_ok(m15)
    if not vol_ok:
        record_rejection(sym, "Volatility Filter", vol_reason)
        log.info(f"{sym}: VOLATILITY FILTER — {vol_reason}")
        return

    # ── SESSION BEHAVIOUR GATE (PnL fix #5 + PDF1 NY filter) ────────────────
    # Your system: London creates the move, NY sweeps/continues it.
    # Rule: if we are in the Second Wave (NY) session and no London sweep
    # occurred today, only allow entry if:
    #   1. NY session itself created a sweep, OR
    #   2. A strong trend exists, OR
    #   3. Internal liquidity was taken (equal highs/lows swept).
    # Active setups (already in SWEEP_CONFIRMED etc.) always pass through.
    in_window_now, win_name_now = in_execution_window()
    st_for_gate = pair_states.get(sym, {})
    is_active_setup = st_for_gate.get("state") not in (None, "WAITING_FOR_SWEEP", "BEHAVIOR_CLASSIFICATION")

    # ── PDF1 fix #2: Tag sweep by session ────────────────────────────────────
    # (Done here pre-emptively based on the current window so flags are current
    #  before the NY gate check below runs.)
    if in_window_now:
        if is_london_session(wat_hour):
            london_key = f"{sym}_london_swept_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
            if london_key in alerted_today:
                flags["london_sweep"] = True
        if is_newyork_session(wat_hour):
            ny_key = f"{sym}_ny_swept_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
            if ny_key in alerted_today:
                flags["ny_sweep"] = True

    # ── PDF1 fix #3: Detect internal sweeps (equal highs/lows on 15M) ────────
    if not flags["internal_sweep"] and len(m15) >= 10:
        eq_hi = find_equal_levels(m15[-20:], "bearish", tolerance=0.002)
        eq_lo = find_equal_levels(m15[-20:], "bullish",  tolerance=0.002)
        price_now = m15[-1]["close"]
        # Internal sweep = price has traded into an equal-high or equal-low cluster
        swept_eq_hi = any(c["high"] >= lvl for c in m15[-5:] for lvl in eq_hi if lvl > 0)
        swept_eq_lo = any(c["low"]  <= lvl for c in m15[-5:] for lvl in eq_lo if lvl > 0)
        if swept_eq_hi or swept_eq_lo:
            flags["internal_sweep"] = True
            log.debug(f"{sym}: internal sweep flagged (equal H/L taken)")

    # ── PDF1 fix #5: NY session entry filter (CORE) ──────────────────────────
    if (in_window_now
            and is_newyork_session(wat_hour)
            and not is_active_setup):
        if not flags["london_sweep"]:
            # No London sweep — require one of the alternative conditions
            valid_ny_condition = (
                flags["ny_sweep"] or
                flags["strong_trend"] or
                flags["internal_sweep"]
            )
            if not valid_ny_condition:
                record_rejection(
                    sym, "Session Gate",
                    "NY session — no London sweep, no NY sweep, no strong trend, "
                    "no internal liquidity taken. Blocked per PDF1 rule."
                )
                log.info(f"{sym}: NY GATE — blocked (no London sweep + no alternative condition)")
                return
            log.info(f"{sym}: NY GATE — passed via alternative "
                     f"(ny={flags['ny_sweep']} trend={flags['strong_trend']} internal={flags['internal_sweep']})")
        else:
            log.info(f"{sym}: NY GATE — London sweep confirmed, entry allowed")

    # Legacy london_key check for BEHAVIOR_CLASSIFICATION gate (kept for backward compat)
    if (in_window_now
            and "Second Wave" in win_name_now
            and not is_active_setup
            and not flags["london_sweep"]):
        # Already handled above — this block is now a no-op fallthrough
        pass

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

    # ── BIAS STRENGTH GATE (PnL fix #6) ─────────────────────────────────────
    # ±1 = Neutral → NO TRADE (already defined in your system, now enforced hard).
    # Only lean (+/-2) or strong (+/-4) bias allowed to proceed to setup scanning.
    # Active setups always pass through — they were validated when they started.
    st_bias_check = pair_states.get(sym, {})
    if st_bias_check.get("state", "WAITING_FOR_SWEEP") == "WAITING_FOR_SWEEP":
        if abs(bd["score"]) < 2:
            record_rejection(sym, "Step 1 (Bias Strength)",
                             f"Bias score {bd['score']:+d} — ±1 is neutral, no trade")
            log.info(f"{sym}: BIAS GATE — score {bd['score']:+d} too weak, blocked")
            return

    # ── NARRATIVE GATE (PnL fix #6) ──────────────────────────────────────────
    # Score ±1 = neutral → already blocked above.
    # Score abs < 2 = not enough conviction. Enforce hard here as a safety net
    # in case daily_bias() ever returns a non-neutral label at score ±1.
    # "±1 = Neutral → NO TRADE" is your rule. Make it structural, not advisory.
    if abs(bd["score"]) < 2:
        st = get_state(sym)
        if st["state"] != "WAITING_FOR_SWEEP":
            reset_state(sym, f"Bias too weak to hold setup (score {bd['score']})")
        record_rejection(sym, "Step 1 (Narrative gate)",
                         f"Bias score {bd['score']:+d} — minimum ±2 required (your rule: ±1 = no trade)")
        log.info(f"{sym}: NARRATIVE GATE blocked — score {bd['score']:+d} < ±2")
        return

    loc_data         = location_4h(h4)
    loc              = loc_data["location"]
    if loc == "unknown":
        record_rejection(sym, "Step 2 (4H Location)", "Cannot determine 4H location")
        log.info(f"{sym}: location unknown")
        return
    # Log alignment context — never a hard block
    location_aligned = (
        (bias == "bullish" and loc == "discount") or
        (bias == "bearish" and loc == "premium")
    )
    if location_aligned:
        log.info(f"{sym}: location ideal ({bias} in {loc})")
    else:
        log.info(f"{sym}: location context ({bias} in {loc}) — model selection will decide")

    st = get_state(sym)
    if st["state"] == "WAITING_FOR_SWEEP":
        structure = is_clean_structure(m15)
        if not structure["clean"]:
            # ── HARD CHOP FILTER (PnL fix #3) ────────────────────────────────
            # Ranging/choppy market = no trade. This is where bots lose money.
            # An advisory flag here costs PnL. Block hard and move on.
            record_rejection(sym, "Step 3 (Structure)", structure["reason"])
            log.info(f"{sym}: CHOP FILTER BLOCKED — {structure['reason']}")
            return
    else:
        structure = {"clean": True, "reason": "In active setup — filter bypassed"}

    # ── WHERE AM I? — Explicit market condition decision ─────────────────────
    # This is the first decision before any model runs.
    # "extreme" → Reversal Model  |  "trending" → Continuation Model
    mc = classify_market_condition(h4, daily, bd)
    log.info(f"{sym}: condition={mc['condition']} | {mc['condition_note']}")

    # ── SESSION BEHAVIOR LOGIC (PnL fix #5) ──────────────────────────────────
    # London creates the move. NY sweeps it and/or continues it.
    # If we are in the NY / Second Wave session and London has not swept
    # a significant level yet, we require evidence of a London sweep before
    # entering. This prevents bot from entering clean in NY with no context.
    in_window_now, win_now = in_execution_window()
    is_ny_session = win_now and ("Second Wave" in win_now or "15:00" in win_now)
    if is_ny_session and st.get("state") == "WAITING_FOR_SWEEP":
        # Check if London primary window produced any sweep by looking at
        # the pair state history — if the pair never advanced past WAITING_FOR_SWEEP
        # during Primary (07:30-09:30) then we treat the NY session as lower conviction.
        london_sweep_seen = pair_states.get(sym, {}).get("london_sweep_confirmed", False)
        if not london_sweep_seen:
            # Check live candle data for evidence of a London-session sweep
            # London candles: roughly candles from the last 6-8 hours
            london_window = m15[-32:]  # ~8h of 15M candles
            london_swept  = False
            for c in london_window:
                if c["high"] >= pdh * 0.998 or c["low"] <= pdl * 1.002:
                    london_swept = True
                    break
            if not london_swept:
                record_rejection(sym, "Session Logic",
                                 "NY/Second Wave session: no London sweep detected — lower conviction, skip")
                log.info(f"{sym}: SESSION FILTER — NY session with no London sweep, skipping")
                return
            else:
                # London sweep was visible in candle data — flag it
                pair_states[sym]["london_sweep_confirmed"] = True
                log.info(f"{sym}: SESSION FILTER — London sweep confirmed in candles, NY entry allowed")

    run_state_machine(sym, m15, h4, daily, bd, loc_data, structure, pair_score,
                      market_condition=mc, live_price=live_price)


def scan_all():
    log.info("=== Scan cycle ===")
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if not hasattr(scan_all, "_day") or scan_all._day != today_str:
        alerted_today.clear()
        session_flags.clear()   # PDF1 fix #7: reset all session flags each new day
        scan_all._day = today_str
        log.info("New day -- dedup reset + session flags cleared")

    tickers = get_all_tickers()
    if not tickers:
        log.warning("No tickers fetched")
        return []

    log.info(f"Total USDT futures: {len(tickers)}")
    all_scored = score_pairs(tickers)
    scored_map = {s["symbol"]: s for s in all_scored}
    pairs      = select_pairs(tickers, TOP_N_PAIRS)

    # ── Model 4: Update top-gainer / top-loser sets ───────────────────────
    _update_model4_top_pairs(all_scored, top_n=20)

    # Pinned pairs — always scanned regardless of dynamic scoring
    PINNED_PAIRS = [
        "BTC_USDT", "ETH_USDT", "SOL_USDT", "BNB_USDT", "XRP_USDT",
        "ADA_USDT", "AVAX_USDT", "TON_USDT", "HYPE_USDT",
        "ZEC_USDT", "DASH_USDT",
    ]
    # Filter pinned to only those that actually exist in the tickers response
    available_syms = {t.get("symbol") for t in tickers}
    valid_pinned   = [s for s in PINNED_PAIRS if s in available_syms]
    missing_pinned = [s for s in PINNED_PAIRS if s not in available_syms]
    if missing_pinned:
        log.warning(f"Pinned pairs not found on MEXC: {missing_pinned}")

    # Always include pairs already in an active state even if not top-N
    active_syms = [sym for sym, st in pair_states.items()
                   if st.get("state") != "WAITING_FOR_SWEEP"]
    # Order: pinned first → dynamic top-N → active state pairs
    all_pairs   = list(dict.fromkeys(valid_pinned + pairs + active_syms))
    if active_syms:
        log.info(f"Active state pairs also scanned: {active_syms}")
    log.info(f"Pinned pairs included: {valid_pinned}")

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
    log.info("BEE-M Alert Bot v16 starting...")

    startup_lines = [
        "<b>BEE-M Alert Bot v18 — Model 4 Momentum Layer Applied</b>",
        "",
        "<b>Master flow:</b>  Narrative → Session flags → Where am I? → Model (locked) → BOS → Entry → (Flip?)",
        "<b>Model 4 layer:</b>  Runs AFTER valid setup — enhances execution only, never creates trades",
        "",
        "<b>Model 4 — Momentum Modifier:</b>",
        "  • Activates on MID/LATE phase + no-dump + impulse strength > threshold",
        "  • Priority pairs (Top 20 gainers/losers): threshold lowered to 0.6, min retrace 0.25",
        "  • TP expansion: ×1.8 (top gainer) / ×1.5 (top loser) / ×1.0 (standard)",
        "  • Psychological TP targets: 1, 2, 3, 5, 8, 10, 20, 50, 100",
        "  • Exhaustion detector: blocks entry + exits trade if top gainer shows overheating",
        "  • Hold behaviour: early exit disabled when momentum active",
        "  • Shutdown rule: deactivates if retrace > 0.5 (prevents trend chasing)",
        "",
        "<b>PDF1 Fixes — Session Context Engine:</b>",
        "  1. Session flags per pair: london_sweep / ny_sweep / internal_sweep / strong_trend",
        "  2. Sweeps tagged by session (London vs NY) automatically",
        "  3. Internal sweeps (equal H/L) detected and flagged",
        "  4. strong_trend flag: HH_HL structure + BOS body > 70%",
        "  5. NY entry filter (CORE): No London sweep → require NY sweep OR strong trend OR internal sweep",
        "  6. No London sweep + B-grade setup → blocked (A/A+ only)",
        "  7. All session flags reset each new UTC day",
        "",
        "<b>PDF2 Fixes — Edge Sharpening:</b>",
        "  1. Trade outcome tracking: entry_price, sl, tp1, sl_hit, tp_hit, partials per log entry",
        "  2. Strict model locking: once MODEL_1/2/3 set → never re-evaluated until reset",
        "  3. MIN_BOS_STRENGTH=0.6 hard gate in detect_bos (env: MIN_BOS_STRENGTH)",
        "  4. Volatility kill filter: blocks if last candle range > 4x avg (env: MAX_CANDLE_RANGE_MULT)",
        "  5. Narrative → Model mapping: trending→M2 / extreme→M1 / expansion→M3 (enforced)",
        "",
        "<b>Bias:</b>  ±4 Strong  |  ±2/3 Lean  |  ±1 Neutral→NO TRADE",
        "<b>Pinned pairs:</b>  BTC ETH SOL BNB XRP ADA AVAX TON HYPE ZEC DASH",
        "<b>Scanning:</b>  24/7 every 15 min | /setups  /pairs  /status  /tradelog  /help",
    ]
    send_telegram("\n".join(startup_lines))

    last_scored    = []
    _bias_notified = ""   # track which day we sent the 6:30 notification
    _window_notified = "" # track which window we announced

    while True:
        try:
            now     = time.time()
            h, m    = _wat_now()
            today   = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            # ── 6:30 AM WAT bias-lock notification (once per day) ──────────
            if h == BIAS_LOCK_HOUR_WAT and m >= BIAS_LOCK_MINUTE_WAT \
               and _bias_notified != today:
                _bias_notified = today
                send_telegram(
                    f"<b>⏰ 06:30 WAT — Bias Lock Time</b>\n"
                    f"Asia session closed. Daily narrative forming.\n"
                    f"Bot is now scoring bias for all tracked pairs.\n"
                    f"First execution window opens at 07:30 WAT.\n"
                    f"Send /status to see all pair narratives."
                )

            # ── Execution window open notification ──────────────────────────
            in_window, win_name = in_execution_window()
            if in_window and _window_notified != win_name:
                _window_notified = win_name
                send_telegram(
                    f"<b>🟢 Window open: {win_name}</b>\n"
                    f"Entry alerts now active. Any held setups will fire now.\n"
                    f"Send /setups to see confirmed entry setups."
                )
            elif not in_window:
                _window_notified = ""  # reset so next window fires

            # Check for Telegram commands (/status, /pairs, /help)
            handle_commands()

            last_scored = scan_all()

            # Auto status digest every STATUS_INTERVAL seconds
            if now - last_status_time >= STATUS_INTERVAL:
                last_status_time = now
                digest = build_status_digest(last_scored)
                send_telegram(digest)

            # Sleep in 30s chunks so commands answered promptly
            log.info(f"Sleeping {SCAN_INTERVAL_KZ}s (24/7) -- commands checked every 30s")
            slept = 0
            while slept < SCAN_INTERVAL_KZ:
                time.sleep(30)
                slept += 30
                handle_commands()

        except Exception as e:
            log.error(f"Loop error: {e}")
            send_telegram(f"Bot error: {e}")
            time.sleep(60)
