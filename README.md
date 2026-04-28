# Alpha PM — AI Portfolio Manager (Ollama Edition)

Runs **100% locally and free** on your Mac.  
No Anthropic API key. No subscription. No cloud.

Uses **Ollama** (local LLM) + **yfinance** (real stock data) + RSS news feeds.

---

## How the Data Pipeline Works

```
yfinance  ──→  Real prices, financials, 52w range, margins
RSS feeds ──→  Live news headlines (Yahoo Finance, MarketWatch, CNBC, Reuters)
              │
              ▼
         Context string built in Python
              │
              ▼
         Local Ollama model analyses the real data
              │
              ▼
         Structured JSON response → frontend charts
```

No hallucinated prices. Stock data comes from yfinance directly.

---

## Quick Start

### Step 1 — Install Ollama

```bash
# macOS — one command
curl -fsSL https://ollama.ai/install.sh | sh

# Or download from: https://ollama.ai/download
```

### Step 2 — Pull a model

```bash
# Recommended default (4.7 GB, works on most Macs with 8 GB RAM)
ollama pull llama3.1:8b

# Lightweight option (2 GB, 4 GB RAM minimum)
ollama pull llama3.2:3b

# Best quality (9 GB, needs 16 GB RAM)
ollama pull qwen2.5:14b
```

### Step 3 — Run Alpha PM

```bash
cd alpha-pm
chmod +x run.sh
./run.sh
```

Browser opens automatically at **http://localhost:8000**.

---

## Model Selection Guide

| Model | RAM needed | Download | Quality | Speed |
|---|---|---|---|---|
| `llama3.2:3b` | 4 GB | 2 GB | ★★☆ | Very fast |
| `llama3.1:8b` | 8 GB | 4.7 GB | ★★★ | Fast |
| `mistral:7b` | 8 GB | 4.1 GB | ★★★ | Fast |
| `qwen2.5:14b` | 16 GB | 9 GB | ★★★★ | Medium |
| `llama3.1:70b` | 48 GB | 40 GB | ★★★★★ | Slow |

To change model:
```bash
export ALPHA_PM_MODEL=qwen2.5:14b
./run.sh
```

Or edit `OLLAMA_MODEL` in `main.py` line 40.

---

## Features

### 📊 Daily Brief (auto at 9:00 AM)
- Fetches real prices for: S&P 500, Nasdaq, Dow, VIX, 10Y/2Y yields,  
  Gold, Oil, Copper, EUR/USD, USD/JPY, USD/SGD, BTC, ETH
- Fetches 11 sector ETF returns (XLK, XLF, XLV, XLE, XLY, ...)
- Pulls headlines from Yahoo Finance, MarketWatch, CNBC, Reuters
- LLM analyses all real data → sentiment score, macro themes, sector rotation

### 💡 Equity Plays (5 ideas)
1. LLM nominates 5 tickers based on market context + news
2. yfinance fetches **real fundamentals** for each ticker:  
   P/E, EV/EBITDA, margins, revenue growth, analyst targets, 52w range
3. LLM writes thesis, catalysts, stop-loss using the **actual numbers**

### 📂 Portfolio Manager
- Buy/sell with avg cost basis tracking
- De-risk analysis fetches fresh yfinance data + ticker news on demand
- P&L by position bar chart, sector exposure donut chart

### 📈 Performance Analytics
- Equity curve vs $1M baseline
- Drawdown from peak chart
- Daily return distribution histogram
- Win/loss breakdown, profit factor

---

## Scheduling Logic

| Time | What happens |
|---|---|
| App started **before 9 AM** | Scheduler waits; runs at exactly 09:00 |
| App started **after 9 AM** (no data yet) | Catch-up run triggers immediately |
| App started **after 9 AM** (data exists) | Nothing; next run tomorrow at 09:00 |
| **Every day at 09:00** | Brief + plays auto-generate in background |

---

## File Structure

```
alpha-pm/
├── main.py           # FastAPI backend + Ollama + yfinance
├── static/
│   └── index.html    # Frontend (Bloomberg-style dark UI)
├── data/
│   ├── portfolio.json
│   ├── snapshots.json
│   ├── briefs/         # YYYY-MM-DD.json (with real sector scores)
│   ├── plays/          # YYYY-MM-DD.json (with real yfinance prices)
│   └── reports/        # TICKER_DATE.json
├── requirements.txt    # Python deps (no anthropic SDK)
├── run.sh             # One-command launcher
└── README.md
```

---

## Troubleshooting

**"Cannot reach Ollama"**
```bash
# Start Ollama manually in a separate terminal
ollama serve
```

**LLM returns garbled JSON**
- Switch to a larger/better model: `export ALPHA_PM_MODEL=qwen2.5:14b`
- Or increase context: edit `LLM_CTX_WINDOW = 16384` in `main.py`

**yfinance rate-limited or returning no data**
- This occasionally happens. Wait a few minutes and retry.
- Weekends/holidays: indices data may be stale (last close).

**Generation is slow**
- Normal for larger models. `llama3.2:3b` is the fastest option.
- Speed doesn't matter much — daily brief only runs once at 9 AM.
- M1/M2/M3 Macs with enough RAM run these models very efficiently.

---

*FastAPI · APScheduler · Chart.js · Ollama · yfinance · feedparser*
