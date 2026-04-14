# BEE-M Trading Alert Bot v13
### Five-Fix Edition — Structured Flip Model

> *"I don't look for trades. I identify the condition, then execute the correct model."*

A 24/7 automated trading alert bot built around the BEE-M v4 Execution Playbook. Scans MEXC futures pairs, determines market condition, selects the correct model, and sends fully-built trade alerts to Telegram — complete with narrative, entry, SL, and TP levels.

---

## Table of Contents

- [What This Bot Does](#what-this-bot-does)
- [Master Flow](#master-flow)
- [System Components](#system-components)
  - [Daily Bias (6-Factor Model)](#daily-bias-6-factor-model)
  - [Market Condition Layer](#market-condition-layer-where-am-i)
  - [Model 1 — Reversal](#model-1--reversal)
  - [Model 2 — Continuation](#model-2--continuation)
  - [Structured Flip Model](#structured-flip-model)
  - [Adaptive Narrative](#adaptive-narrative)
  - [Entry Models](#entry-models)
  - [Risk Management](#risk-management)
- [State Machine](#state-machine)
- [Execution Windows](#execution-windows)
- [Telegram Alerts](#telegram-alerts)
- [Telegram Commands](#telegram-commands)
- [Setup & Deployment](#setup--deployment)
- [Environment Variables](#environment-variables)
- [Requirements](#requirements)

---

## What This Bot Does

- Scans the **top 30 MEXC USDT futures pairs** every 15 minutes, 24/7
- Scores **daily bias** using a 6-factor model (max ±9 points)
- Explicitly decides **"where am I?"** — at extremes or inside a trend
- Routes to **Model 1 (Reversal)** or **Model 2 (Continuation)** accordingly
- Tracks each pair through a **state machine** that persists across scan cycles
- Sends **fully-built Telegram alerts** with narrative, entry, SL, and TP
- Monitors invalidated trades for **flip opportunities** in the opposite direction
- **Rebuilds the narrative** dynamically if market context changes mid-setup

---

## Master Flow

```
Narrative (6:30 AM WAT)
        ↓
Where am I?
(Extreme / Trending / Ranging)
        ↓
Choose Model
(Reversal OR Continuation)
        ↓
Wait for BOS
        ↓
Entry
(Retracement OR Momentum)
        ↓
Manage → Journal
        ↓
Invalidated? → Flip Model
```

---

## System Components

### Daily Bias (6-Factor Model)

Scored every cycle. Requires **±2 minimum** to trade. Neutral (-1 to +1) = no trade.

| Factor | What It Checks | Score |
|--------|---------------|-------|
| **F1** | Daily structure — HH+HL or LH+LL | ±2 |
| **F2** | Liquidity draw — equal highs/lows above or below | ±2 |
| **F3** | Previous daily candle narrative — strong close or displacement | ±1 |
| **F4** | Sweep + rejection context — swept prior level and closed back | ±2 |
| **F5** | Premium / discount location | ±1 |
| **F6** | HTF FVG draw — unmitigated daily imbalance | ±1 |

**Score interpretation:**

| Score | Strength |
|-------|---------|
| +4 or more | Strong Bullish |
| +2 to +3 | Bullish Lean |
| -1 to +1 | **Neutral → NO TRADE** |
| -2 to -3 | Bearish Lean |
| -4 or less | Strong Bearish |

---

### Market Condition Layer — "Where Am I?"

This is the first decision made before any model runs. Called on every scan cycle.

| Condition | Detection | Model Used |
|-----------|-----------|-----------|
| **Extreme** | Price within 8% of PDH, PDL, or 4H swing extreme | Model 1 — Reversal |
| **Trending** | 4H HH+HL or LH+LL (60%+ of swings agree), price holding above/below midpoint | Model 2 — Continuation |
| **Ranging** | No clear structure | Model 1 — Reversal (default) |

> **4H location is a guideline only — never a hard block.** Bullish in discount ✔ Bearish in premium ✔ are ideal. Opposite combinations are flagged in the alert but the trade still fires if all other conditions are met.

---

### Model 1 — Reversal

Used at **PDH / PDL / major liquidity zones / extremes**.

```
1. Liquidity sweep (PDH/PDL, equal H/L, or swing H/L)
2. Strong displacement
3. 15M BOS (Break of Structure)
   ↓
Entry: Retracement (OTE 0.618–0.792, 0.5, FVG) OR Momentum
SL: Beyond sweep extreme or BOS origin
```

**Sweep detection** is structure-based — price only needs to **trade into** a defined liquidity pool. A wick sweep and a full-candle close beyond a level are both valid. No wick rejection required.

**Sweep confluence scoring (0–4):**
- +1 for obvious level (PDH/PDL or equal H/L)
- +1 for HTF context alignment (premium → sell sweep, discount → buy sweep)

---

### Model 2 — Continuation

Used when market is **trending and NOT at extremes**. Does not require a PDH/PDL sweep.

```
1. Impulse leg in trend direction (creates HH or LL on 15M)
2. Pullback (must be 20–75% of impulse range — not too shallow, not too deep)
3. Structure holds — pullback low stays above prior HL (bull) / pullback high below prior LH (bear)
4. Internal sweep OR compression during pullback
5. Continuation BOS — price closes above pullback's swing high (bull) or below swing low (bear)
   ↓
Entry: At continuation BOS (internal_sweep_BOS / compression_BOS / continuation_BOS)
SL: Below pullback HL (bull) or above pullback LH (bear)
```

If the continuation check fails, the bot **falls through to Model 1** automatically for the same pair.

---

### Structured Flip Model

When a trade is **invalidated** (SL hit or structure broken), the bot does not immediately reset. It enters `INVALIDATED` state and watches for the market to confirm a new direction. Two modes:

#### Conservative Mode (default)
All three required:
1. Opposite liquidity sweep
2. Strong displacement in new direction
3. BOS in new direction

#### Aggressive Mode (fires first — faster)
Only two required:
1. Strong displacement (body ratio ≥ 70%) in new direction
2. BOS in new direction — no sweep needed

> ❌ No flip on one strong candle alone
> ❌ No flip without a confirmed BOS
> ✅ Only flip when the market **proves** new direction

**Flip window:** 60 minutes. If neither mode confirms within that time, the bot resets to `WAITING_FOR_SWEEP`.

---

### Adaptive Narrative

The narrative is built at **6:30 AM WAT** and rebuilt dynamically throughout the day. Every scan cycle, active setups are checked for:

- **Bias change** — daily scoring shifted to opposite direction
- **Condition change** — market moved from trending to extreme (or vice versa)

When a material change is detected, a `📋 NARRATIVE UPDATE` alert fires on Telegram with the old vs new context, updated expectation, and updated invalidation — before the SL gets hit.

Narrative fields:
- **Bias** — Bullish / Bearish with strength label
- **What price did** — swept level, continuation, inside range
- **Target liquidity** — PDH/PDL, equal highs/lows
- **Expectation** — the full story (sweep → displacement → BOS → retrace → entry → target)
- **Invalidation** — what would prove the narrative wrong

---

### Entry Models

Both apply to **Model 1 and Model 2** once BOS is confirmed.

| Model | Entry Type | Trigger |
|-------|-----------|---------|
| **Model A — Retracement** | OTE (0.618–0.792) | Price retraces to OTE zone and reacts |
| **Model A — Retracement** | 0.5 level | Price reaches 50% retracement |
| **Model A — Retracement** | FVG tap | Price touches an imbalance inside OTE |
| **Model B — Momentum** | Continuation candle | Strong directional close (≥55% body ratio) |
| **Model B — Momentum** | Small pullback | < 50% retrace, reversal close |
| **Model B — Momentum** | Micro consolidation breakout | 2–3 tight candles → breakout |

Model B fires when price is strong and not retracing to OTE. The alert clearly labels which model triggered.

---

### Risk Management

| Level | Size | Target |
|-------|------|--------|
| **TP1** | 50% | Internal liquidity (~1:2 RR) |
| **TP2** | 30% | 4H liquidity target |
| **Runner** | 20% | External liquidity |

- **Reversal SL:** Beyond sweep extreme or BOS origin
- **Continuation SL:** Below pullback HL (bull) / above pullback LH (bear)
- **Flip SL:** Beyond the flip sweep or BOS origin

> Rule: Risk **1% per trade, never more than 2%**. If the position size makes you hesitate before clicking, it's too big.

---

## State Machine

Each pair moves through these states independently, remembered across all scan cycles:

```
WAITING_FOR_SWEEP
      ↓  (sweep detected)
SWEEP_CONFIRMED
      ↓  (displacement + BOS confirmed)
WAITING_FOR_RETRACEMENT
      ↓  (OTE reaction or Momentum entry)
TRADE_TAKEN → reset

  At any point, if invalidated:
      ↓
INVALIDATED
      ↓  (flip confirmed, or 60min timeout)
WAITING_FOR_SWEEP
```

**Bias flip resets immediately.** If daily bias changes while a setup is active, the state resets and a fresh narrative is built.

**Active pairs are always scanned** — even if a pair drops out of the top-30 ranking, it stays in the scan loop until the setup resolves.

---

## Execution Windows

The bot scans 24/7 but recognises three priority execution windows in **WAT (West Africa Time = UTC+1)**:

| Time (WAT) | Window | Notes |
|-----------|--------|-------|
| **06:30** | Bias Lock | Daily narrative built. Asia session closed. |
| **07:30–09:30** | Primary Window | Main trades — highest priority |
| **12:30–13:30** | Optional Window | Mid-session setups |
| **15:00–18:00** | Second Wave | Afternoon continuation moves |

**Outside windows:** Alerts still fire. The `Window` line in every alert shows either `🟢 Primary (07:30-09:30 WAT)` or `⚠️ Outside execution window — your call`. You decide whether to take the trade.

---

## Telegram Alerts

### Alert Types

| Emoji | Type | When It Fires |
|-------|------|--------------|
| 🔔 | Sweep Detected | Liquidity sweep confirmed — waiting for BOS |
| 🔵 | Continuation Entry | Model 2 setup complete — all 5 steps confirmed |
| 🔥 | Entry — Model A | OTE reaction confirmed |
| 🔥 | Entry — Model B | Momentum entry confirmed |
| ⚠️ | Invalidated | Structure broken — flip watch begins |
| 🔄 | Flip Confirmed | Opposite direction confirmed (Conservative or Aggressive) |
| 📋 | Narrative Update | Bias or condition changed on an active setup |
| 📊 | Status Digest | Automatic every 2 hours |

### What Every Entry Alert Shows

```
🔥 ENTRY — Model A — BTC/USDT BULLISH
Window: 🟢 Primary (07:30-09:30 WAT)

Narrative: Trending up — pullback holds HL → continuation BOS → push to PDH 42800
Invalidation: Break and CLOSE below sweep low / HL

Reaction in OTE at 41250.0000
FVG in OTE: 41180.0000–41230.0000  ← preferred entry

Entry:        41250.0000
SL:           40890.0000
TP1 (50%):    42020.0000  [Equal highs]  (~1:2.1)
TP2 (30%):    42500.0000  [4H swing high]
Runner (20%): 43100.0000  [External liquidity]

Rating: A
Risk 1-2% only. Narrative confirmed — execute.
```

---

## Telegram Commands

Send these directly to the bot in your Telegram chat:

| Command | Shortcut | What It Shows |
|---------|----------|--------------|
| `/status` | `/s` | Full scan: every tracked pair + current step + last rejection reason |
| `/pairs` | `/p` | Quick view: only pairs in an active setup |
| `/help` | `/h` | Command menu |

**Response time:** Commands are checked every 30 seconds during sleep intervals, so you'll get a reply within 30 seconds at most.

### `/status` Output Example

```
BEE-M SCAN STATUS
14:22 UTC | Pairs tracked: 28 | Active setups: 2

── ACTIVE SETUPS ──
  BTC/USDT  BULLISH  [A]
    Steps 1-6 COMPLETE  (12min since BOS)
    Step 7: Watching OTE 41180.0000–41420.0000  FVG in zone
    Waiting: Model A reaction OR Model B momentum

  ETH/USDT  ⚠️ INVALIDATED  (8min ago)
    Was: BEARISH → Watching for: BULLISH flip
    Needs: Opposite sweep + Displacement + BOS
    Flip window: 52min remaining

── LAST REJECTION PER PAIR ──
  SOL/USDT  |  Step 1 DAILY BIAS failed  |  Neutral bias (score +1)
  DOGE/USDT  |  Step 4 SWEEP failed  |  No candle traded below buy-side level
  ...

Next scan in ~15 min  |  /help for commands
```

---

## Setup & Deployment

### 1. Create a Telegram Bot

1. Open Telegram and message **@BotFather**
2. Send `/newbot` and follow the prompts
3. Copy the **bot token** you receive
4. Start a conversation with your new bot, then visit:
   `https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates`
5. Send any message to the bot and refresh the URL — copy the `chat_id` from the response

### 2. Deploy on Railway (Recommended)

Railway is the simplest way to host this bot 24/7.

1. Go to [railway.app](https://railway.app) and create a free account
2. Click **New Project → Deploy from GitHub repo** (or use **Empty Project → Add Service → GitHub Repo**)
3. Connect your GitHub account and push `bot_v13.py` to a repo
4. In Railway, go to your service → **Variables** tab and add:
   - `TELEGRAM_TOKEN` = your bot token
   - `TELEGRAM_CHAT_ID` = your chat ID
5. Create a `requirements.txt` in your repo (see below)
6. Railway auto-detects Python and deploys. The bot starts immediately.

**To redeploy after updating the file:** Push to GitHub — Railway redeploys automatically.

### 3. Deploy Locally (Testing)

```bash
# Install dependencies
pip install requests

# Set environment variables
export TELEGRAM_TOKEN="your_bot_token_here"
export TELEGRAM_CHAT_ID="your_chat_id_here"

# Run
python3 bot_v13.py
```

---

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `TELEGRAM_TOKEN` | ✅ Yes | — | Your Telegram bot token from BotFather |
| `TELEGRAM_CHAT_ID` | ✅ Yes | — | Your Telegram chat ID |
| `SCAN_INTERVAL_KZ` | No | `900` | Seconds between scan cycles (default: 15 min) |
| `MAX_SWEEP_AGE` | No | `5400` | Max seconds to wait for BOS after sweep (default: 90 min) |
| `MAX_BOS_AGE` | No | `2700` | Max seconds to wait for OTE after BOS (default: 45 min) |
| `MAX_FLIP_AGE` | No | `3600` | Max seconds to confirm a flip after invalidation (default: 60 min) |
| `TOP_N_PAIRS` | No | `30` | Number of top pairs to scan each cycle |
| `SWEEP_LOOKBACK` | No | `30` | Number of 15M candles to look back for sweep detection |
| `STATUS_INTERVAL` | No | `7200` | Seconds between automatic status digests (default: 2 hours) |

---

## Requirements

**`requirements.txt`**
```
requests
```

That's the only external dependency. Everything else (`os`, `time`, `logging`, `datetime`) is Python standard library.

**Python version:** 3.8 or higher

---

## System Rules (Non-Negotiable)

These rules are enforced in code — they cannot be overridden by any scan result:

1. **Bias must score ±2 or more** — neutral bias = no trade, full stop
2. **BOS is required for every entry** — no BOS, no alert fires
3. **No flip without displacement + BOS** — one strong candle is not enough
4. **Risk 1% per trade, max 2%** — position sizing is your responsibility
5. **2 consecutive losses = stop for the day** — journal both before next session
6. **A missed trade is not a lost trade** — the market always gives another

---

## Version History

| Version | Key Changes |
|---------|------------|
| v13 | Five-fix edition: real continuation model, adaptive narrative, two-mode flip, explicit market condition layer, over-filtering removed |
| v12 | Structured Flip Model — INVALIDATED state, conservative flip |
| v11 | Model 2 Continuation added, location warning fixed |
| v10 | Execution windows, narrative builder, 6:30 WAT bias lock |
| v9 | 24/7 scanning, no kill zones, /status /pairs commands |
| v8 | Original state machine — sweep → BOS → OTE |
