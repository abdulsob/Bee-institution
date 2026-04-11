# BEE-M Alert Bot — Final Version
## Setup takes about 15 minutes total

---

## What this bot does

Every 15 minutes it:
1. Fetches ALL MEXC USDT futures tickers (200+ pairs)
2. Scores every single pair across 4 dimensions:
   - Volatility (day range % of price)
   - Volume (vs median pair volume)
   - Momentum (size of 24h move)
   - Structure (body dominance approximation)
3. Selects the top 10 scored pairs + top 5 daily gainers + top 5 daily losers
4. Runs your full BEE-M system on each selected pair
5. Sends a Telegram alert only when ALL rules pass

Any rule that fails = no alert. You only hear from the bot when
the setup is genuinely valid.

---

## BEE-M rules checked (in order)

Step 1 — All 8 daily bias scenarios
Step 2 — 4H premium/discount location aligned with bias
Step 3 — Liquidity path clear + sweep tier classified (Tier 1/2/3)
Step 4 — 15M Expansion BOS (all 5 criteria):
  - Breaks relevant swing high/low (not noise)
  - Candle CLOSES beyond the level (no wick fakeouts)
  - Expansion candle only (body >= 2x avg, body-dominant, leaves FVG)
  - Only after liquidity sweep (buy-side for shorts, sell-side for longs)
  - Location aligned (longs from discount, shorts from premium)
Step 5 — FVG inside OTE zone check (preferred entry)
Step 5 — OTE zone 0.618-0.792, ideal ~0.705
Step 6 — SL at sweep extreme or BOS origin (whichever further/cleaner)
Step 6 — TP1 50% / TP2 30% / Runner 20%

After alert: bot monitors OTE zone
  - Reaction detected -> CONFIRMED ENTRY alert
  - 3 checks in zone, no reaction -> SKIP alert

---

## STEP 1 — Create Telegram Bot (5 min)

1. Open Telegram -> search @BotFather
2. Send: /newbot
3. Name it: BEE-M Alerts
4. Username: beemtrades_bot (must end in _bot)
5. Copy the TOKEN it gives you (looks like 7123456789:AAHdqT...)
6. Search @userinfobot -> message it -> copy your ID number

---

## STEP 2 — Put files on GitHub (5 min)

1. Go to github.com -> New repository -> name: beem-bot -> Public
2. Click "uploading an existing file"
3. Upload ALL files from this folder (bot.py, requirements.txt,
   railway.toml, .gitignore, README.md)
4. Click "Commit changes"

---

## STEP 3 — Deploy on Railway (5 min)

1. Go to railway.app -> sign up with GitHub (free)
2. Click New Project -> Deploy from GitHub repo
3. Select your beem-bot repo
4. Click Variables tab -> Add Variable:

   TELEGRAM_TOKEN   = (your token from BotFather)
   TELEGRAM_CHAT_ID = (your ID from @userinfobot)

5. Railway auto-deploys. Within 30 seconds you get a Telegram message:
   "BEE-M Alert Bot v4 — Full System"

If you don't receive it: check Variables are correct and check Logs tab.

---

## Optional environment variables

Add these in Railway Variables to customise:

  SCAN_INTERVAL    = 900    (seconds between scans, default 15 min)
  OTE_MONITOR_SECS = 720    (how long to watch OTE zone, default 12 min)
  TOP_N_PAIRS      = 30     (max pairs to scan per cycle, default 30)

---

## What a setup alert looks like

🟢 BEE-M SETUP DETECTED 🟢
Pair: SOL/USDT  |  Direction: BULLISH
Time: 2025-04-12 09:15 UTC
Pair score: 74/100 | 24h move: +8.4% | High volatility | Strong volume

━━ STEP 1 — DAILY BIAS ━━
Score: +4 | PDH: 142.80 | PDL: 136.50
  • Sc5 Bull: Yesterday closed above PDH
  • Sc6 Bull: Above midpoint (139.65)
  • Sc7 Bull: Daily HH+HL — bullish order flow

━━ STEP 2 — 4H LOCATION ━━
Price in DISCOUNT ✅

━━ STEP 3 — LIQUIDITY PATH ━━
Clear path to liquidity above at 147.20 ✅
Sweep: Tier 2 — PDL (major level) | Conviction: HIGH

━━ STEP 4 — 15M CONFIRMATION ━━
Sweep ✅  Displacement ✅
BOS: Broke last 15M swing HIGH (expansion) ✅
Imbalance left: Yes — FVG formed
Body dominance: 72% of candle range

━━ STEP 5 — ENTRY ZONE (OTE) ━━
Zone: 138.40 — 139.10
Ideal (~0.705): 138.72
FVG in OTE: ✅ 138.50–138.90 ← preferred entry

━━ STEP 6 — RISK LEVELS ━━
🎯 Entry:        138.72
🛑 SL:           135.90
✅ TP1 (50%):    144.08  (~1:2.0 RR)
✅ TP2 (30%):    146.76
🚀 Runner (20%): 152.12

━━ FINAL CHECKLIST ━━
✔ Daily bias confirmed (+4)
✔ 4H location valid (discount)
✔ Liquidity path clear
✔ Sweep: Tier 2 — PDL (major level)
✔ Displacement + Expansion BOS confirmed
✔ BOS: Broke last 15M swing HIGH (expansion)
✔ OTE zone calculated

⏳ Monitoring OTE zone now. CONFIRMED or SKIP alert coming next.
Risk 1–2% only. Predefine levels before entry.

---

## How to act on an alert

1. Alert arrives on Telegram
2. Open MEXC -> find the pair
3. Check if price is approaching the OTE zone
4. Wait for the CONFIRMED ENTRY alert from the bot
5. If CONFIRMED -> execute your order at the entry price
6. If SKIP -> do nothing. Next setup will come.
7. Always risk 1-2% of account only

---

## Important

- Bot reads public market data only — no MEXC account access needed
- No API key required on the MEXC side
- You always execute manually — bot alerts only
- Railway free tier: 500 hours/month — enough for 24/7
- Same pair+direction won't alert twice in one day (dedup)
- Inducement BOS is rejected automatically — only expansion BOS fires
