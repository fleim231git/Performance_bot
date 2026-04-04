# 🦀 Trading Performance Bot

> AI-powered analytics bot for crypto trading groups — real-time stats, smart distance analysis, and natural language queries over 293,000+ trades.

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![Anthropic](https://img.shields.io/badge/Claude-Haiku_4.5-CC785C?style=flat)](https://anthropic.com)
[![Telegram](https://img.shields.io/badge/Telegram-Bot-26A5E4?style=flat&logo=telegram)](https://telegram.org)
[![Render](https://img.shields.io/badge/Deployed-Render-46E3B7?style=flat)](https://render.com)

---

## What it does

A production Telegram bot that turns raw trade signals into actionable analytics. The bot parses trade messages in real time, stores them in SQLite, and lets users query the data in natural language — powered by Claude's Tool Use API.

**Ask it anything:**
- *"Top coins on Bybit this week with distances"*
- *"How did Denis perform in March?"*
- *"Which coins had 100% win rate in the last hour?"*
- *"Show me stable distance coins on Binance"*

---

## Architecture

```
Telegram Group (trades) ──► Parser ──► SQLite (293k+ trades)
                                              │
Telegram Chat (queries) ──► Claude AI ──► Tool Use ──► SQL Query ──► Response
                                │
                           Whisper API
                        (voice messages)
```

**The AI layer is agentic** — Claude decides which tools to call based on the question, not hardcoded logic.

---

## Key Features

### 🧠 Claude Tool Use (5 tools)
Claude autonomously selects and calls the right database tool based on user intent:

| Tool | Description |
|------|-------------|
| `get_trader_stats` | Per-trader breakdown with coins, distances, exchanges |
| `get_top_coins` | Top coins filtered by period, exchange, distance range |
| `get_coin_stats` | Deep stats for a single coin |
| `get_all_traders` | Full group leaderboard |
| `get_period_stats` | Summary for any time period including hourly |

### 📐 Smart Distance Logic
Distance = how far price must move to trigger an order (in %).

- **⚡️ Working distance** — median distance of profitable trades (where it actually works)
- **🛡 Insurance distance** — 90th percentile (no point going higher)
- **🎯 Stable distance** — shown when spread ≤ 3% (consistent execution)

Only shown when spread > 3% — otherwise a simple range is displayed.

### 🏦 Per-Exchange Breakdown
Binance, Bybit, OKX tracked separately. Same coin can have very different optimal distances per exchange.

### 🎤 Voice Messages (Whisper)
Reply to the bot with a voice message — it transcribes via OpenAI Whisper and responds as normal.

### 💰 Cost Tracking
Every API call logs token usage and cost. `/cost` command shows session spend. Max 3 tool calls per request to prevent runaway costs.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11+ |
| Bot framework | python-telegram-bot 21.5 |
| AI | Anthropic Claude Haiku 4.5 (Tool Use) |
| Voice | OpenAI Whisper API |
| Database | SQLite (293k+ records) |
| Deployment | Render Background Worker + Persistent Disk |
| CI/CD | GitHub auto-deploy |

---

## Trade Message Parser

Supports 4 different message formats from the trading group:

```
✅ Денис(Binance), ⬆ BUY 1.20 0.36 (D3N): Profit +5$ (+10%) #COIN sold ... (+0.3%)
✅ Борис(ByBit), BUY_0.75: Profit +0.1$ (+4.9%) #COIN sold ... (+0.1%)
✅ Костя(ByBit), BUY-0.55-0.20-(K0A): Profit +1$ (+1.5%) #COIN sold ... (+0.0%)
✅ Саня(BINANCE), SELL_qv140000_vb45000: Loss -7$ (-14.9%) #COIN bought ... (-0.6%)
```

Includes cap logic: profit > 110% → capped at 110%, loss < -106% → capped at -106%.

---

## Bot Commands

```
/report [N]          — Report for last N days (default: 7)
/report_daily        — Today's report
/period март 2026    — Monthly stats
/top [N]             — Top N traders
/trader Денис март   — Trader detail
/coins [exchange]    — Top coins with distances
/coins binance       — Binance only
/coin BTCUSDT        — Coin stats by exchange
/stats               — Global stats
/cost                — API spend this session
/clear               — Clear AI conversation history
```

---

## Setup

### Environment Variables (Render)

```env
BOT_TOKEN=your_telegram_bot_token
ANTHROPIC_API_KEY=your_anthropic_key
OPENAI_API_KEY=your_openai_key        # Optional, for Whisper
SOURCE_CHAT_ID=-1001234567890         # Chat where trades come in
REPORT_CHAT_ID=-1001234567890         # Chat for scheduled reports
DIALOG_CHAT_ID=-1001234567890         # Chat for AI queries
BOT_USERNAME=your_bot_username
```

### Render Configuration

```yaml
# render.yaml
services:
  - type: worker
    name: trading-bot
    runtime: python
    buildCommand: pip install -r requirements.txt
    startCommand: python bot.py
    disk:
      name: trades-db
      mountPath: /data
      sizeGB: 1
```

---

## Scheduled Reports

- **Daily at 20:00** — today's summary + weekly stats + top traders
- **Sunday at 20:00** — extended weekly report

---

## Author

**Grigori Marandiuc** — AI Automation Developer  
📍 Chisinau, Moldova  
🔗 [github.com/fleim231git](https://github.com/fleim231git)  
✉️ fleim231@mail.ru

---

*Built with Claude as a daily development tool.*
