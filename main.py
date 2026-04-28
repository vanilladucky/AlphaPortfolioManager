#!/usr/bin/env python3
"""
Alpha PM — AI Portfolio Manager
Pipeline: Brief → Plays (brief-informed) → 5 Reports (auto) → Decision
Cost tracking: every API call logged to terminal + database + frontend
"""

import asyncio, json, logging, os, re
from contextlib import suppress
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, List, Optional
from zoneinfo import ZoneInfo

import feedparser, httpx, yfinance as yf
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import database as db

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
AI_MODEL          = "claude-haiku-4-5-20251001"
AI_URL            = "https://api.anthropic.com/v1/messages"
IN_RATE           = 1.0 / 1_000_000   # $1.00 / M input tokens
OUT_RATE          = 5.0 / 1_000_000   # $5.00 / M output tokens

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("alpha-pm")

STATIC = Path(__file__).parent / "static"
STATIC.mkdir(parents=True, exist_ok=True)

LIVE_PRICE_POLL_SECONDS = 5 * 60
SGT = ZoneInfo("Asia/Singapore")
MARKET_WINDOWS_SGT = {
    "SGX": (time(9, 0), time(17, 0)),
    "HKEX": (time(9, 30), time(16, 0)),
    "US": (time(21, 30), time(4, 0)),  # spans midnight
}

# ═══════════════════════════════════════════════════════════════
# MARKET DATA SOURCES
# ═══════════════════════════════════════════════════════════════

INDICES = {
    "S&P 500":"^GSPC","Nasdaq 100":"QQQ","Dow Jones":"^DJI",
    "Russell 2000":"^RUT","VIX":"^VIX","10Y Yield":"^TNX","2Y Yield":"^IRX",
}
COMMODITIES = {"Gold":"GC=F","Oil WTI":"CL=F","Copper":"HG=F","Silver":"SI=F","Nat Gas":"NG=F"}
FX  = {"EUR/USD":"EURUSD=X","USD/JPY":"JPY=X","GBP/USD":"GBPUSD=X","DXY":"DX-Y.NYB","USD/SGD":"SGD=X","AUD/USD":"AUDUSD=X"}
CRYPTO = {"Bitcoin":"BTC-USD","Ethereum":"ETH-USD"}
SECTOR_ETFS = {
    "Technology":"XLK","Financials":"XLF","Healthcare":"XLV","Energy":"XLE",
    "Consumer Disc":"XLY","Consumer Staples":"XLP","Industrials":"XLI",
    "Materials":"XLB","Real Estate":"XLRE","Utilities":"XLU","Comm Services":"XLC",
}
NEWS_FEEDS = [
    ("Yahoo Finance","https://finance.yahoo.com/rss/topstories"),
    ("MarketWatch",  "https://feeds.marketwatch.com/marketwatch/topstories/"),
    ("CNBC",         "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
]

# ═══════════════════════════════════════════════════════════════
# YFINANCE HELPERS
# ═══════════════════════════════════════════════════════════════

def _fetch_prices(sym_map: dict) -> dict:
    out = {}
    for name, sym in sym_map.items():
        try:
            h = yf.Ticker(sym).history(period="5d", interval="1d")
            if len(h) >= 2:
                c, p, f = float(h["Close"].iloc[-1]), float(h["Close"].iloc[-2]), float(h["Close"].iloc[0])
                out[name] = {"price": round(c,4), "chg_1d": round((c-p)/p*100,2), "chg_5d": round((c-f)/f*100,2)}
            elif len(h) == 1:
                out[name] = {"price": round(float(h["Close"].iloc[-1]),4), "chg_1d":0.0, "chg_5d":0.0}
        except Exception as e:
            log.warning("Price fetch failed %s: %s", sym, e)
    return out

def _fetch_stock_info(ticker: str) -> dict:
    try:
        t = yf.Ticker(ticker); info = t.info; hist = t.history(period="1y")
        curr = info.get("currentPrice") or info.get("regularMarketPrice") \
               or (float(hist["Close"].iloc[-1]) if len(hist) else 0)
        ytd = 0.0
        if len(hist):
            y = hist[hist.index.year == datetime.now().year]
            if len(y) > 1:
                ytd = (float(y["Close"].iloc[-1]) - float(y["Close"].iloc[0])) / float(y["Close"].iloc[0]) * 100
        def pct(v): return f"{round(v*100,1)}%" if v else "N/A"
        def big(v):
            if not v: return "N/A"
            if v>=1e12: return f"${v/1e12:.2f}T"
            if v>=1e9:  return f"${v/1e9:.2f}B"
            if v>=1e6:  return f"${v/1e6:.2f}M"
            return f"${v:,.0f}"
        return {
            "ticker":ticker.upper(),"name":info.get("longName") or info.get("shortName",ticker),
            "sector":info.get("sector","Unknown"),"industry":info.get("industry","Unknown"),
            "current_price":round(float(curr),2),"market_cap":big(info.get("marketCap")),
            "pe_ratio":round(info.get("trailingPE",0) or 0,1) or "N/A",
            "fwd_pe":round(info.get("forwardPE",0) or 0,1) or "N/A",
            "ev_ebitda":round(info.get("enterpriseToEbitda",0) or 0,1) or "N/A",
            "pb_ratio":round(info.get("priceToBook",0) or 0,2) or "N/A",
            "revenue_ttm":big(info.get("totalRevenue")),
            "gross_margin":pct(info.get("grossMargins")),"ebitda_margin":pct(info.get("ebitdaMargins")),
            "net_margin":pct(info.get("profitMargins")),"revenue_growth":pct(info.get("revenueGrowth")),
            "earnings_growth":pct(info.get("earningsGrowth")),"roe":pct(info.get("returnOnEquity")),
            "debt_equity":round(info.get("debtToEquity",0) or 0,2),"fcf":big(info.get("freeCashflow")),
            "analyst_target":round(info.get("targetMeanPrice",0) or 0,2) or "N/A",
            "analyst_rating":info.get("recommendationKey","N/A"),
            "52w_high":round(info.get("fiftyTwoWeekHigh",0) or 0,2),
            "52w_low":round(info.get("fiftyTwoWeekLow",0) or 0,2),
            "ytd_return":round(ytd,1),"beta":round(info.get("beta",0) or 0,2),
            "dividend_yield":pct(info.get("dividendYield")),
            "description":(info.get("longBusinessSummary") or "")[:400],
        }
    except Exception as e:
        log.error("Stock info failed %s: %s", ticker, e)
        return {"ticker":ticker,"error":str(e),"current_price":0}

def _slim(stock: dict) -> dict:
    keep = ["ticker","name","sector","industry","current_price","market_cap",
            "pe_ratio","fwd_pe","ev_ebitda","pb_ratio","revenue_ttm","gross_margin",
            "ebitda_margin","net_margin","revenue_growth","earnings_growth","roe",
            "debt_equity","fcf","analyst_target","analyst_rating",
            "52w_high","52w_low","ytd_return","beta","dividend_yield"]
    return {k: stock[k] for k in keep if k in stock}

async def fetch_prices(sym_map): return await asyncio.to_thread(_fetch_prices, sym_map)
async def fetch_stock_info(ticker): return await asyncio.to_thread(_fetch_stock_info, ticker)

def _fetch_last_prices(tickers: List[str]) -> dict:
    out = {}
    for ticker in sorted({t.upper().strip() for t in tickers if t and str(t).strip()}):
        try:
            h = yf.Ticker(ticker).history(period="5d", interval="1d")
            if len(h) == 0:
                continue
            close = h["Close"].dropna()
            if len(close) == 0:
                continue
            out[ticker] = round(float(close.iloc[-1]), 2)
        except Exception as e:
            log.warning("Live price fetch failed %s: %s", ticker, e)
    return out

async def fetch_last_prices(tickers: List[str]) -> dict:
    return await asyncio.to_thread(_fetch_last_prices, tickers)

async def fetch_news(n=20) -> list:
    out = []
    async with httpx.AsyncClient(timeout=12.0) as c:
        for src, url in NEWS_FEEDS:
            try:
                r = await c.get(url, headers={"User-Agent":"Mozilla/5.0"}, follow_redirects=True)
                for e in feedparser.parse(r.text).entries[:5]:
                    t = e.get("title","").strip()
                    if t: out.append({"title":t,"source":src})
            except Exception as ex:
                log.warning("News feed %s failed: %s", src, ex)
    return out[:n]

async def build_market_context() -> str:
    log.info("Fetching live market data from yfinance...")
    res = await asyncio.gather(
        fetch_prices(INDICES), fetch_prices(COMMODITIES), fetch_prices(FX),
        fetch_prices(CRYPTO), fetch_prices(SECTOR_ETFS), fetch_news(20),
        return_exceptions=True,
    )
    def safe(x): return x if isinstance(x, dict) else {}
    def safel(x): return x if isinstance(x, list) else []
    idx,com,fx_,cry,sec,news = [safe(r) if i<5 else safel(r) for i,r in enumerate(res)]
    def tbl(d):
        return "\n".join(
            f"  {n}: {v.get('price','?')}  (1d {v.get('chg_1d',0):+.2f}%  5d {v.get('chg_5d',0):+.2f}%)"
            for n,v in d.items() if isinstance(v,dict)
        )
    sec_scores = {n:{"score":round(50+v.get("chg_5d",0)*5,1),"chg_5d":v.get("chg_5d",0)}
                  for n,v in sec.items() if isinstance(v,dict)}
    return f"""════════════════════════════════════════════
REAL-TIME MARKET DATA — {datetime.now().strftime('%Y-%m-%d %H:%M')} SGT
════════════════════════════════════════════
INDICES:\n{tbl(idx)}
COMMODITIES:\n{tbl(com)}
FX:\n{tbl(fx_)}
CRYPTO:\n{tbl(cry)}
SECTOR ETFs:\n{tbl(sec)}
SECTOR SCORES:\n{json.dumps(sec_scores,indent=2)}
HEADLINES:\n{chr(10).join(f"  • [{h['source']}] {h['title']}" for h in news[:15])}
════════════════════════════════════════════"""

# ═══════════════════════════════════════════════════════════════
# CLAUDE API  — with cost tracking + retry
# ═══════════════════════════════════════════════════════════════

app = FastAPI(title="Alpha PM")
scheduler = AsyncIOScheduler()
_ws_clients: List[WebSocket] = []
_live_price_task: Optional[asyncio.Task] = None

async def broadcast(msg: dict):
    dead = []
    for ws in _ws_clients:
        try: await ws.send_json(msg)
        except: dead.append(ws)
    for ws in dead: _ws_clients.remove(ws)

def _infer_market(ticker: str) -> str:
    """
    Map ticker suffix to a trading window.
    China tickers (.SS/.SZ) are intentionally grouped with HKEX hours.
    """
    t = (ticker or "").upper()
    if t.endswith(".SI"):
        return "SGX"
    if t.endswith((".HK", ".SS", ".SZ", ".SH")):
        return "HKEX"
    return "US"

def _is_market_open_sgt(market: str, now_sgt: datetime) -> bool:
    open_t, close_t = MARKET_WINDOWS_SGT[market]
    now_t = now_sgt.time()
    wd = now_sgt.weekday()  # Mon=0 ... Sun=6

    if market in ("SGX", "HKEX"):
        return wd < 5 and open_t <= now_t < close_t

    # US session spans midnight in SGT (21:30 -> 04:00)
    if now_t >= open_t:
        return wd < 5
    if now_t < close_t:
        return wd in (1, 2, 3, 4, 5)
    return False

async def run_live_price_refresh() -> None:
    human_pf = db.load_portfolio()
    agent_pf = db.load_agent_portfolio()

    human_tickers = {p["ticker"].upper() for p in human_pf["positions"] if p.get("ticker")}
    agent_tickers = {p["ticker"].upper() for p in agent_pf["positions"] if p.get("ticker")}
    all_tickers = sorted(human_tickers | agent_tickers)
    if not all_tickers:
        return

    now_sgt = datetime.now(SGT)
    markets_by_ticker = {t: _infer_market(t) for t in all_tickers}
    tracked_markets = set(markets_by_ticker.values())
    open_markets = {m for m in tracked_markets if _is_market_open_sgt(m, now_sgt)}
    if not open_markets:
        return

    tickers_to_poll = [t for t in all_tickers if markets_by_ticker[t] in open_markets]
    prices = await fetch_last_prices(tickers_to_poll)
    if not prices:
        return

    for ticker, px in prices.items():
        if ticker in human_tickers:
            db.update_last_price(ticker, px)
        if ticker in agent_tickers:
            db.agent_update_last_price(ticker, px)

    await broadcast({
        "type": "portfolio_live_update",
        "updated_at": now_sgt.strftime("%Y-%m-%d %H:%M:%S"),
        "markets_open": sorted(open_markets),
        "portfolio": db.load_portfolio(),
        "agent_portfolio": db.load_agent_portfolio(),
    })
    log.info(
        "📡  Live refresh @ %s SGT | markets=%s | tickers=%s",
        now_sgt.strftime("%H:%M"),
        ",".join(sorted(open_markets)),
        ",".join(sorted(prices.keys())),
    )

async def live_price_poller() -> None:
    log.info(
        "📡  Live price poller started (%ds cadence, market-hours gated)",
        LIVE_PRICE_POLL_SECONDS,
    )
    while True:
        try:
            await run_live_price_refresh()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error("Live price poller failed: %s", exc, exc_info=True)
        await asyncio.sleep(LIVE_PRICE_POLL_SECONDS)

async def call_claude(prompt: str, system: str,
                      use_search: bool = False,
                      max_tokens: int = 4000,
                      job: str = "unknown") -> str:
    """
    Call Claude Haiku. Tracks token usage, computes cost, logs to terminal,
    saves to database, and broadcasts a cost_update over WebSocket.
    Auto-retries on 429 with exponential backoff.
    """
    if not ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY not set.")
    headers = {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
    }
    body: dict = {
        "model": AI_MODEL, "max_tokens": max_tokens, "system": system,
        "messages": [{"role":"user","content":prompt}],
    }
    if use_search:
        body["tools"] = [{"type":"web_search_20250305","name":"web_search"}]

    wait_times = [65, 90, 120]
    for attempt, wait in enumerate(wait_times + [None], start=1):
        async with httpx.AsyncClient(timeout=180.0) as client:
            r = await client.post(AI_URL, headers=headers, json=body)

        if r.status_code == 429:
            if wait is None:
                raise RuntimeError(f"Rate-limited after {attempt-1} retries.")
            log.warning("⏳  Rate limit (attempt %d). Waiting %ds...", attempt, wait)
            await broadcast({"type":"info","msg":f"Rate limit — waiting {wait}s (attempt {attempt})..."})
            await asyncio.sleep(wait)
            continue

        if not r.is_success:
            raise RuntimeError(f"Claude API {r.status_code}: {r.text[:400]}")

        data = r.json()
        text = "\n".join(b["text"] for b in data.get("content",[]) if b.get("type")=="text")

        # ── Cost accounting ──────────────────────────────────────
        usage   = data.get("usage", {})
        in_tok  = usage.get("input_tokens",  0)
        out_tok = usage.get("output_tokens", 0)
        cost    = in_tok * IN_RATE + out_tok * OUT_RATE

        # Terminal log — always visible in the server window
        log.info(
            "💰  %-30s  %6d in + %5d out tokens  =  $%.5f",
            job, in_tok, out_tok, cost
        )

        # Persist to database
        db.save_cost(job, in_tok, out_tok, cost, AI_MODEL)

        # Broadcast to all connected frontends
        daily = db.load_daily_cost()
        alltime = db.load_all_time_cost()
        await broadcast({
            "type":         "cost_update",
            "job":          job,
            "input_tokens": in_tok,
            "output_tokens":out_tok,
            "cost_usd":     round(cost, 6),
            "daily_total":  round(daily["cost_usd"], 6),
            "daily_calls":  daily["calls"],
            "alltime_total":round(alltime["cost_usd"], 6),
        })

        return text

    raise RuntimeError("call_claude: exhausted retries")


def parse_json(text: str) -> Any:
    if not text: return None
    cleaned = re.sub(r"```(?:json)?\s*","",text); cleaned = re.sub(r"```","",cleaned).strip()
    try: return json.loads(cleaned)
    except: pass
    for pat in [r"(\[[\s\S]*\])", r"(\{[\s\S]*\})"]:
        m = re.search(pat, cleaned)
        if m:
            try: return json.loads(m.group(1))
            except: pass
    try: return json.loads(cleaned + "}"*(cleaned.count("{")-cleaned.count("}")))
    except: pass
    return None

# ═══════════════════════════════════════════════════════════════
# PIPELINE STEP 1 — DAILY BRIEF
# ═══════════════════════════════════════════════════════════════

async def generate_brief() -> Optional[dict]:
    log.info("📊  [1/3] Generating daily brief...")
    await broadcast({"type":"loading_start","job":"brief",
                     "msg":"Step 1/3 — Fetching market data and generating brief..."})
    try:
        t = date.today().isoformat()
        market_ctx = await build_market_context()
        log.info("Market data ready. Calling Claude Haiku (brief)...")

        system = (
            "You are Chief Market Strategist at a top investment bank. "
            "Analyse the real yfinance market data and return a structured brief. "
            "Return ONLY a valid JSON object — no markdown, no preamble."
        )
        prompt = f"""Real-time market data for {t}:

{market_ctx}

Return ONLY this JSON:
{{
  "date": "{t}",
  "generated_at": "{datetime.now().strftime('%H:%M')}",
  "sentiment": "risk-on",
  "sentiment_score": 62,
  "executive_summary": "3 sentences citing specific real numbers from the data",
  "macro": {{
    "us": "US outlook citing real index levels",
    "global_growth": "Global picture from real data",
    "inflation": "Rates citing real yield levels",
    "rates": "10Y and 2Y yield environment"
  }},
  "markets": {{
    "us_equities": "S&P 500 and Nasdaq levels",
    "bonds": "Treasury yields",
    "commodities": "Oil, Gold, Copper real prices",
    "fx": "DXY, EUR/USD, USD/JPY, USD/SGD",
    "asia": "Asian markets",
    "europe": "European markets"
  }},
  "themes": [
    {{"theme":"...","implication":"...","sectors":["Technology"]}}
  ],
  "sector_rotation": [
    {{"sector":"Technology",      "stance":"overweight",  "reason":"cite real 5d ETF return","score":72}},
    {{"sector":"Financials",      "stance":"neutral",     "reason":"cite real data","score":52}},
    {{"sector":"Energy",          "stance":"underweight", "reason":"cite real data","score":35}},
    {{"sector":"Healthcare",      "stance":"overweight",  "reason":"cite real data","score":65}},
    {{"sector":"Consumer Disc",   "stance":"neutral",     "reason":"cite real data","score":50}},
    {{"sector":"Industrials",     "stance":"overweight",  "reason":"cite real data","score":63}},
    {{"sector":"Utilities",       "stance":"underweight", "reason":"cite real data","score":32}},
    {{"sector":"Materials",       "stance":"neutral",     "reason":"cite real data","score":47}},
    {{"sector":"Real Estate",     "stance":"underweight", "reason":"cite real data","score":38}},
    {{"sector":"Comm Services",   "stance":"neutral",     "reason":"cite real data","score":55}},
    {{"sector":"Consumer Staples","stance":"neutral",     "reason":"cite real data","score":48}}
  ],
  "risks": [{{"risk":"...","probability":"high","impact":"..."}}],
  "catalysts": [{{"event":"...","date":"this week","market_impact":"..."}}]
}}"""

        raw   = await call_claude(prompt, system, use_search=False, job="brief")
        brief = parse_json(raw)
        if not brief:
            raise ValueError(f"Could not parse brief JSON. Raw: {raw[:300]}")

        # Blend sector scores with real ETF momentum
        sec_data = await fetch_prices(SECTOR_ETFS)
        for entry in brief.get("sector_rotation", []):
            name = entry.get("sector","")
            if name in sec_data and isinstance(sec_data[name], dict):
                d5 = sec_data[name].get("chg_5d", 0)
                entry["score"]       = round((round(50+d5*5,0)+entry.get("score",50))/2, 0)
                entry["real_5d_chg"] = d5

        # Attach cost metadata to the brief itself
        daily = db.load_daily_cost()
        brief["_meta"] = {
            "model":     AI_MODEL,
            "generated_at": datetime.now().strftime("%H:%M"),
            "cost_usd":  round(daily["cost_usd"], 6),
            "calls_so_far": daily["calls"],
        }

        db.save_brief(brief)
        await broadcast({"type":"brief_complete","data":brief})
        log.info("✅  [1/3] Brief complete — daily cost so far: $%.5f", daily["cost_usd"])
        return brief

    except Exception as exc:
        log.error("Brief generation failed: %s", exc, exc_info=True)
        await broadcast({"type":"error","job":"brief","msg":str(exc)})
        return None

# ═══════════════════════════════════════════════════════════════
# PIPELINE STEP 2 — EQUITY PLAYS  (informed by brief)
# ═══════════════════════════════════════════════════════════════

async def generate_plays(brief_context: Optional[dict] = None) -> list:
    log.info("💡  [2/3] Generating equity plays (brief-informed)...")
    await broadcast({"type":"loading_start","job":"plays",
                     "msg":"Step 2/3 — Generating brief-informed equity plays..."})
    try:
        t         = date.today().isoformat()
        portfolio = db.load_portfolio()
        held      = ", ".join(p["ticker"] for p in portfolio["positions"]) or "none"

        # Load brief from DB if not passed in
        if not brief_context:
            brief_context = db.load_brief(t) or {}

        # Distill brief into a compact context block for the nomination prompt
        bc = brief_context
        sector_lines = "\n".join(
            f"  {s['sector']}: {s['stance'].upper()} (score {s.get('score',50)}) — {s['reason']}"
            for s in bc.get("sector_rotation", [])
        )
        themes_lines = "\n".join(
            f"  • {th['theme']}: {th['implication']}"
            for th in bc.get("themes", [])
        )
        risks_lines = "\n".join(
            f"  • [{r['probability'].upper()}] {r['risk']}"
            for r in bc.get("risks", [])
        )

        brief_summary = f"""TODAY'S MACRO VIEW (from daily brief):
Sentiment: {bc.get('sentiment','neutral').upper()} — score {bc.get('sentiment_score',50)}/100
Summary: {bc.get('executive_summary','')}

SECTOR ROTATION STANCE:
{sector_lines}

KEY THEMES:
{themes_lines}

KEY RISKS:
{risks_lines}"""

        # Real-time index + sector data (condensed)
        idx_data = await fetch_prices(INDICES)
        sec_data = await fetch_prices(SECTOR_ETFS)
        news_items = await fetch_news(8)

        idx_lines = "\n".join(
            f"  {n}: {v.get('price','?')} (5d {v.get('chg_5d',0):+.2f}%)"
            for n,v in idx_data.items() if isinstance(v,dict)
        )
        sec_lines = "\n".join(
            f"  {n}: {v.get('chg_5d',0):+.2f}% (5d)"
            for n,v in sec_data.items() if isinstance(v,dict)
        )
        news_lines = "\n".join(f"  • {h['title']}" for h in news_items)

        # ── Step 1: nominate tickers ────────────────────────────
        step1_system = (
            "You are a senior equity analyst. Select stocks that align with today's macro view. "
            "Return ONLY a valid JSON array. No markdown."
        )
        step1_prompt = f"""Today: {t}. Already held (avoid): {held}

{brief_summary}

LIVE INDEX LEVELS:
{idx_lines}

SECTOR ETF 5-DAY RETURNS:
{sec_lines}

RECENT HEADLINES:
{news_lines}

Select exactly 5 stocks that best align with the macro view above.
Favour sectors marked OVERWEIGHT. Avoid sectors marked UNDERWEIGHT.
Return ONLY a JSON array:
[
  {{"ticker":"NVDA","reason":"aligns with overweight Tech — AI capex cycle"}},
  {{"ticker":"JPM", "reason":"..."}},
  {{"ticker":"XOM", "reason":"..."}},
  {{"ticker":"LLY", "reason":"..."}},
  {{"ticker":"AMZN","reason":"..."}}
]"""

        raw1        = await call_claude(step1_prompt, step1_system,
                                        use_search=False, job="plays-nomination")
        nominations = parse_json(raw1)
        if not isinstance(nominations, list) or not nominations:
            raise ValueError(f"Invalid nominations. Raw: {raw1[:300]}")
        nominations = nominations[:5]
        log.info("Nominated: %s", [n.get("ticker","?") for n in nominations])

        # ── Step 2: per-ticker thesis with real data + pacing ───
        plays = []
        for i, nom in enumerate(nominations):
            ticker = str(nom.get("ticker","")).upper().strip()
            if not ticker: continue

            if i > 0:
                log.info("  ⏳ Pacing — waiting 15s before %s...", ticker)
                await asyncio.sleep(15)

            log.info("  Fetching yfinance data for %s...", ticker)
            stock = await fetch_stock_info(ticker)
            if stock.get("error") or not stock.get("current_price"):
                log.warning("  Skipping %s — data unavailable", ticker)
                continue

            slim   = _slim(stock)
            curr   = stock["current_price"]
            at     = stock.get("analyst_target")
            target = at if isinstance(at,(int,float)) and at > curr else round(curr*1.20,2)
            stop   = round(curr*0.90,2)

            # Find this sector's macro stance from the brief
            sector_stance = "neutral"
            for s in bc.get("sector_rotation",[]):
                if s.get("sector","").lower() in stock.get("sector","").lower():
                    sector_stance = s.get("stance","neutral"); break

            step2_system = (
                "You are a senior equity analyst writing a research note. "
                "Reference the macro context in your thesis. "
                "Use only the real yfinance numbers — do not invent figures. "
                "Return ONLY a valid JSON object. No markdown."
            )
            step2_prompt = f"""Write a BUY thesis for {ticker}.

MACRO CONTEXT (from today's brief):
{bc.get('executive_summary','')}
Sector stance for {stock.get('sector','Unknown')}: {sector_stance.upper()}

REAL yfinance DATA:
{json.dumps(slim,indent=2)}

Return ONLY this JSON:
{{
  "ticker":        "{ticker}",
  "company":       "{stock.get('name',ticker)}",
  "sector":        "{stock.get('sector','Unknown')}",
  "cap":           "Large Cap",
  "current_price": {curr},
  "target_price":  {target},
  "stop_loss":     {stop},
  "timeframe":     "3-6 months",
  "rating":        "BUY",
  "conviction":    "HIGH",
  "thesis":        "3-4 sentences citing real metrics and today's macro context",
  "catalysts":     ["Catalyst 1","Catalyst 2","Catalyst 3"],
  "risks":         ["Risk 1","Risk 2"],
  "technical":     "52w ${stock.get('52w_low')}–${stock.get('52w_high')}. YTD {stock.get('ytd_return')}%. Beta {stock.get('beta')}.",
  "valuation":     "Key valuation vs sector peers",
  "entry":         "Entry strategy",
  "macro_alignment": "{sector_stance}"
}}"""

            raw2 = await call_claude(step2_prompt, step2_system,
                                     use_search=False, job=f"plays-thesis-{ticker}")
            play = parse_json(raw2)
            if play:
                play["current_price"] = curr
                play["sector"]        = stock.get("sector", play.get("sector","Unknown"))
                plays.append(play)
                log.info("  ✅ %s @ $%.2f", ticker, curr)
            else:
                log.warning("  ⚠ Could not parse play for %s", ticker)

        if not plays:
            raise ValueError("No plays generated successfully")

        # Attach cost metadata
        daily = db.load_daily_cost()
        plays_with_meta = {
            "plays": plays,
            "_meta": {
                "model": AI_MODEL,
                "generated_at": datetime.now().strftime("%H:%M"),
                "cost_usd_plays": round(daily["cost_usd"], 6),
            }
        }
        db.save_plays(plays)
        await broadcast({"type":"plays_complete","data":plays,"meta":plays_with_meta["_meta"]})
        log.info("✅  [2/3] %d plays saved — daily cost so far: $%.5f",
                 len(plays), daily["cost_usd"])
        return plays

    except Exception as exc:
        log.error("Plays generation failed: %s", exc, exc_info=True)
        await broadcast({"type":"error","job":"plays","msg":str(exc)})
        return []

# ═══════════════════════════════════════════════════════════════
# PIPELINE STEP 3 — RESEARCH REPORTS  (auto for all 5 plays)
# ═══════════════════════════════════════════════════════════════

async def generate_report(ticker: str, company: str, auto: bool = False) -> Optional[dict]:
    label = f"report-{ticker}" + ("-auto" if auto else "")
    log.info("📄  Generating research report: %s...", ticker)
    await broadcast({"type":"loading_start","job":f"report_{ticker}",
                     "msg":f"{'Step 3/3 — Auto-generating' if auto else 'Generating'} report for {ticker}..."})
    try:
        stock  = await fetch_stock_info(ticker)
        slim   = _slim(stock)
        curr   = stock.get("current_price", 0)
        at     = stock.get("analyst_target")
        target = at if isinstance(at,(int,float)) and at > 0 else round(curr*1.20,2)

        # Pull brief context for the report
        brief = db.load_brief() or {}
        exec_summary = brief.get("executive_summary","")
        sector_stance = "neutral"
        for s in brief.get("sector_rotation",[]):
            if s.get("sector","").lower() in stock.get("sector","").lower():
                sector_stance = s.get("stance","neutral"); break

        system = (
            "You are an MD-level Equity Research analyst. "
            "Ground the report in the real yfinance data and today's macro context. "
            "Use web search for latest earnings news and analyst updates. "
            "CRITICAL: output a single raw JSON object and nothing else. "
            "No preamble, no explanation, no markdown fences — just the JSON."
        )
        prompt = f"""Full equity research report for {ticker} ({company}). Today: {date.today().isoformat()}.

TODAY'S MACRO CONTEXT:
{exec_summary}
Sector ({stock.get('sector','Unknown')}) stance: {sector_stance.upper()}

REAL yfinance DATA:
{json.dumps(slim,indent=2)}

Use web search for latest earnings, analyst upgrades/downgrades, news.

Return ONLY this JSON:
{{
  "ticker":             "{ticker}",
  "company":            "{stock.get('name',company)}",
  "rating":             "BUY",
  "price_target":       {target},
  "current_price":      {curr},
  "upside_pct":         {round((target-curr)/curr*100,1) if curr else 0},
  "macro_alignment":    "{sector_stance}",
  "executive_summary":  "3-4 sentences citing real metrics and macro context",
  "investment_thesis":  "4-5 sentence detailed thesis using real data and today's brief",
  "company_overview":   "Business model and competitive position",
  "financials": {{
    "revenue_ttm":   "{stock.get('revenue_ttm','N/A')}",
    "revenue_growth":"{stock.get('revenue_growth','N/A')}",
    "gross_margin":  "{stock.get('gross_margin','N/A')}",
    "ebitda_margin": "{stock.get('ebitda_margin','N/A')}",
    "net_margin":    "{stock.get('net_margin','N/A')}",
    "pe_ratio":      "{stock.get('pe_ratio','N/A')}",
    "fwd_pe":        "{stock.get('fwd_pe','N/A')}",
    "ev_ebitda":     "{stock.get('ev_ebitda','N/A')}",
    "fcf":           "{stock.get('fcf','N/A')}",
    "roe":           "{stock.get('roe','N/A')}",
    "beta":          "{stock.get('beta','N/A')}",
    "dividend_yield":"{stock.get('dividend_yield','N/A')}"
  }},
  "catalysts": [{{"catalyst":"...","timeline":"near-term","impact":"..."}}],
  "risks":     [{{"risk":"...","severity":"HIGH","mitigation":"..."}}],
  "valuation": [
    {{"method":"Analyst Consensus",  "implied_price":{at or target},"notes":"Street mean target"}},
    {{"method":"15% Upside (cons.)", "implied_price":{round(curr*1.15,2)},"notes":"Conservative"}},
    {{"method":"20% Upside (base)",  "implied_price":{round(curr*1.20,2)},"notes":"Base-case"}}
  ],
  "scenario_analysis": {{
    "bull": {{"price":{round(curr*1.35,2)},"probability":"25%","trigger":"beat + re-rate"}},
    "base": {{"price":{target},            "probability":"50%","trigger":"in-line execution"}},
    "bear": {{"price":{round(curr*0.82,2)},"probability":"25%","trigger":"macro headwind / miss"}}
  }},
  "technical":            "52w ${stock.get('52w_low')}–${stock.get('52w_high')}. YTD {stock.get('ytd_return')}%. Beta {stock.get('beta')}.",
  "competitive_position": "Market position and key moats",
  "consensus_vs_ours":    "Analyst rating: {stock.get('analyst_rating','N/A')}. Where we differ from Street.",
  "decision_factors": {{
    "buy_if":  "Specific condition that makes this a clear buy",
    "pass_if": "Condition that would make you skip this trade",
    "watch":   "Key metric or event to monitor before deciding"
  }}
}}"""

        raw    = await call_claude(prompt, system, use_search=True,
                                   max_tokens=8000, job=label)
        result = parse_json(raw)
        if not result:
            raise ValueError(f"Could not parse report JSON. Raw: {raw[:300]}")
        result["current_price"] = curr

        # Attach cost metadata to the report
        daily = db.load_daily_cost()
        result["_meta"] = {
            "model":        AI_MODEL,
            "generated_at": datetime.now().strftime("%H:%M"),
            "cost_usd":     round(daily["cost_usd"], 6),
            "auto":         auto,
        }

        db.save_report(ticker, result)
        await broadcast({"type":"report_complete","ticker":ticker,"data":result,"auto":auto})
        log.info("✅  Report for %s saved — daily cost so far: $%.5f",
                 ticker, daily["cost_usd"])
        return result

    except Exception as exc:
        log.error("Report failed for %s: %s", ticker, exc, exc_info=True)
        await broadcast({"type":"error","job":f"report_{ticker}","msg":str(exc)})
        return None

# ═══════════════════════════════════════════════════════════════
# DE-RISK
# ═══════════════════════════════════════════════════════════════

async def run_derisk(ticker: str):
    portfolio = db.load_portfolio()
    pos = next((p for p in portfolio["positions"] if p["ticker"]==ticker), None)
    if not pos: return
    log.info("⚡  De-risk: %s...", ticker)
    await broadcast({"type":"loading_start","job":f"dr_{ticker}",
                     "msg":f"Fetching live price and news for {ticker}..."})
    try:
        stock   = await fetch_stock_info(ticker)
        slim    = _slim(stock)
        lp      = stock.get("current_price") or pos.get("last_price") or pos["avg_cost"]
        pnl_pct = (lp - pos["avg_cost"]) / pos["avg_cost"] * 100

        system = (
            "You are a Senior Portfolio Risk Manager. "
            "Use real yfinance data plus web search for latest news. "
            "Return ONLY a valid JSON object. No markdown."
        )
        prompt = f"""De-risk analysis for {ticker}. Today: {date.today().isoformat()}.

POSITION:
  Shares: {pos['shares']} | Avg cost: ${pos['avg_cost']:.2f}
  Current: ${lp:.2f} (yfinance) | Unrealised P&L: {pnl_pct:.1f}%
  Value: ${lp*pos['shares']:,.0f}

REAL yfinance DATA:
{json.dumps(slim,indent=2)}

Use web search for latest news, earnings, analyst changes for {ticker}.

Return ONLY this JSON:
{{
  "current_price":      {lp},
  "recommendation":     "HOLD",
  "conviction":         "HIGH",
  "thesis_intact":      true,
  "updated_target":     0,
  "updated_stop":       {round(lp*0.90,2)},
  "unrealized_pnl_pct": {round(pnl_pct,2)},
  "summary":            "3 sentence risk assessment citing real metrics and news",
  "developments":       ["Key development 1","Key development 2"],
  "risks_ahead":        ["Risk 1","Risk 2"],
  "action":             "Specific action with price levels"
}}"""

        raw    = await call_claude(prompt, system, use_search=True,
                                   job=f"derisk-{ticker}")
        result = parse_json(raw)
        if not result:
            raise ValueError(f"Could not parse de-risk JSON. Raw: {raw[:300]}")

        result["current_price"] = lp
        db.update_last_price(ticker, lp)
        db.take_snapshot()

        updated = db.load_portfolio()
        await broadcast({"type":"derisk_complete","ticker":ticker,
                         "data":result,"portfolio":updated})
        log.info("✅  De-risk %s: %s", ticker, result.get("recommendation","?"))

    except Exception as exc:
        log.error("De-risk failed for %s: %s", ticker, exc, exc_info=True)
        await broadcast({"type":"error","job":f"dr_{ticker}","msg":str(exc)})


# ═══════════════════════════════════════════════════════════════
# AUTO DE-RISK — runs daily on ALL human positions
# ═══════════════════════════════════════════════════════════════

async def auto_derisk_all_positions() -> dict:
    """
    Run de-risk analysis on every open human position.
    Returns {ticker: derisk_result} for use by the agent.
    """
    portfolio = db.load_portfolio()
    if not portfolio["positions"]:
        log.info("⚡  No open positions to de-risk.")
        return {}

    log.info("⚡  Auto de-risking %d position(s)...", len(portfolio["positions"]))
    await broadcast({"type": "loading_start", "job": "derisk_all",
                     "msg": f"Auto de-risking {len(portfolio['positions'])} position(s)..."})
    results = {}
    for i, pos in enumerate(portfolio["positions"]):
        if i > 0:
            log.info("  ⏳ Pacing — waiting 15s before next de-risk...")
            await asyncio.sleep(15)
        await run_derisk(pos["ticker"])
        # Grab the result from DB to pass to agent later
        # (run_derisk already saved the last_price update)

    await broadcast({"type": "derisk_all_complete",
                     "msg": "All positions de-risked"})
    log.info("✅  All positions de-risked")
    return results


# ═══════════════════════════════════════════════════════════════
# AI AGENT — autonomous investment decisions
# ═══════════════════════════════════════════════════════════════

AGENT_MAX_POSITION_PCT = 0.15   # max 15% of portfolio per position
AGENT_STD_POSITION_PCT = 0.10   # standard position size: 10%
AGENT_MIN_CASH_PCT     = 0.20   # always keep ≥20% cash
AGENT_MAX_POSITIONS    = 8      # max concurrent positions


async def agent_decide_new_play(play: dict, report: dict) -> None:
    """
    Agent reviews one research report and autonomously decides to BUY or PASS.
    Executes the trade if BUY.
    """
    ticker  = play.get("ticker","")
    company = play.get("company","")
    curr    = play.get("current_price", 0)
    if not ticker or not curr:
        return

    # Skip if agent already holds this ticker
    agent_pf = db.load_agent_portfolio()
    if any(p["ticker"] == ticker for p in agent_pf["positions"]):
        log.info("🤖  Agent: already holds %s — skipping", ticker)
        return

    # Check position count limit
    if len(agent_pf["positions"]) >= AGENT_MAX_POSITIONS:
        log.info("🤖  Agent: at max positions (%d) — skipping %s",
                 AGENT_MAX_POSITIONS, ticker)
        return

    pos_val = sum(p["shares"] * (p.get("last_price") or p["avg_cost"])
                  for p in agent_pf["positions"])
    total_val = agent_pf["cash"] + pos_val
    available_cash = agent_pf["cash"] - total_val * AGENT_MIN_CASH_PCT

    system = (
        "You are an autonomous AI portfolio manager making real investment decisions. "
        "Be decisive and specific. Return ONLY a valid JSON object. No markdown."
    )
    prompt = f"""You are an AI portfolio manager. Review this research report and decide whether to BUY.

RESEARCH REPORT:
{json.dumps(report, indent=2)[:3000]}

AGENT PORTFOLIO STATE:
  Total value:     ${total_val:,.2f}
  Cash available:  ${agent_pf['cash']:,.2f}
  Min cash buffer: ${total_val * AGENT_MIN_CASH_PCT:,.2f} (20% of portfolio)
  Deployable cash: ${max(available_cash, 0):,.2f}
  Open positions:  {len(agent_pf['positions'])} / {AGENT_MAX_POSITIONS}
  Existing tickers: {[p['ticker'] for p in agent_pf['positions']]}

POSITION SIZING RULES:
  Standard position: {AGENT_STD_POSITION_PCT*100:.0f}% of total portfolio = ${total_val*AGENT_STD_POSITION_PCT:,.2f}
  Max position:      {AGENT_MAX_POSITION_PCT*100:.0f}% of total portfolio = ${total_val*AGENT_MAX_POSITION_PCT:,.2f}
  HIGH conviction →  {AGENT_MAX_POSITION_PCT*100:.0f}% allocation
  MEDIUM conviction → {AGENT_STD_POSITION_PCT*100:.0f}% allocation
  LOW conviction →   5% allocation
  If deployable cash < position size needed → PASS

Current price of {ticker}: ${curr}

Decide: BUY (with share count) or PASS. Be analytical and reference the report metrics.

Return ONLY this JSON:
{{
  "action":       "BUY",
  "shares":       134,
  "position_pct": 10.2,
  "conviction":   "HIGH",
  "reasoning":    "3-4 sentence justification citing specific metrics from the report",
  "key_factors":  ["Factor 1 with specific data", "Factor 2", "Risk acknowledged: ..."]
}}"""

    try:
        raw    = await call_claude(prompt, system, use_search=False,
                                   max_tokens=1000, job=f"agent-decision-{ticker}")
        result = parse_json(raw)
        if not result:
            log.warning("🤖  Agent: could not parse decision for %s", ticker)
            return

        action    = result.get("action","PASS").upper()
        shares    = float(result.get("shares", 0))
        reasoning = result.get("reasoning","")
        conviction= result.get("conviction","MEDIUM")
        daily     = db.load_daily_cost()

        log.info("🤖  Agent decision for %-6s: %-4s  %.0f shares @ $%.2f  [%s]",
                 ticker, action, shares, curr, conviction)
        log.info("    Reasoning: %s", reasoning[:120])

        # Record the decision regardless of action
        db.save_agent_decision("new_play", ticker, action, shares, curr,
                                reasoning, conviction, daily["cost_usd"])

        if action == "BUY" and shares > 0:
            cost = shares * curr
            if cost > available_cash:
                # Recalculate to fit available cash
                shares = max(1, int(available_cash / curr))
                log.info("🤖  Agent: adjusted %s to %.0f shares (cash constrained)", ticker, shares)

            if shares > 0 and shares * curr <= agent_pf["cash"]:
                ok = db.agent_execute_buy(ticker, company,
                                          play.get("sector","Unknown"),
                                          shares, curr)
                if ok:
                    agent_pf = db.load_agent_portfolio()
                    pos_val2 = sum(p["shares"]*(p.get("last_price") or p["avg_cost"])
                                   for p in agent_pf["positions"])
                    log.info("🤖  Agent BOUGHT %s: %.0f shares @ $%.2f = $%.2f",
                             ticker, shares, curr, shares*curr)
                    await broadcast({
                        "type":      "agent_trade",
                        "action":    "BUY",
                        "ticker":    ticker,
                        "shares":    shares,
                        "price":     curr,
                        "reasoning": reasoning,
                        "conviction":conviction,
                        "portfolio": db.load_agent_portfolio(),
                    })

    except Exception as e:
        log.error("🤖  Agent decision failed for %s: %s", ticker, e)


async def agent_review_position(ticker: str, derisk_result: dict) -> None:
    """
    Agent reviews a de-risk result for one of its own positions
    and autonomously decides to HOLD, ADD, REDUCE, or EXIT.
    """
    agent_pf = db.load_agent_portfolio()
    pos = next((p for p in agent_pf["positions"] if p["ticker"] == ticker), None)
    if not pos:
        return

    lp      = derisk_result.get("current_price") or pos.get("last_price") or pos["avg_cost"]
    pnl_pct = (lp - pos["avg_cost"]) / pos["avg_cost"] * 100
    rec     = derisk_result.get("recommendation","HOLD")

    pos_val = sum(p["shares"] * (p.get("last_price") or p["avg_cost"])
                  for p in agent_pf["positions"])
    total_val = agent_pf["cash"] + pos_val

    system = (
        "You are an autonomous AI portfolio manager managing a real position. "
        "Be decisive. Reference the specific de-risk data. Return ONLY valid JSON. No markdown."
    )
    prompt = f"""Review this de-risk analysis for your position in {ticker} and decide the action.

DE-RISK ANALYSIS (just completed):
  Recommendation:   {rec}
  Conviction:       {derisk_result.get('conviction','')}
  Thesis intact:    {derisk_result.get('thesis_intact',True)}
  Updated target:   ${derisk_result.get('updated_target',0)}
  Updated stop:     ${derisk_result.get('updated_stop',0)}
  Summary:          {derisk_result.get('summary','')}
  Action suggested: {derisk_result.get('action','')}
  Risks ahead:      {derisk_result.get('risks_ahead',[])}
  Developments:     {derisk_result.get('developments',[])}

YOUR POSITION:
  Ticker:          {ticker}
  Shares held:     {pos['shares']}
  Avg cost:        ${pos['avg_cost']:.2f}
  Current price:   ${lp:.2f}
  Unrealised P&L:  {pnl_pct:+.1f}%
  Position value:  ${lp * pos['shares']:,.2f}
  Portfolio total: ${total_val:,.2f}

RULES:
  - If thesis_intact=false OR recommendation=EXIT → seriously consider EXIT
  - If recommendation=REDUCE → consider selling 30-50% of position
  - Stop loss: if current price < avg_cost * 0.88 → EXIT (protect capital)
  - Profit taking: if P&L > 25% → consider partial profit-taking (REDUCE 30%)
  - Otherwise: HOLD unless there is a specific reason to act

Return ONLY this JSON:
{{
  "action":       "HOLD",
  "shares_delta": 0,
  "reasoning":    "3 sentence justification citing specific data points above",
  "conviction":   "HIGH"
}}
(shares_delta: positive = buy more, negative = sell that many shares, 0 = hold)"""

    try:
        raw    = await call_claude(prompt, system, use_search=False,
                                   max_tokens=600, job=f"agent-review-{ticker}")
        result = parse_json(raw)
        if not result:
            return

        action      = result.get("action","HOLD").upper()
        shares_delta= float(result.get("shares_delta",0))
        reasoning   = result.get("reasoning","")
        conviction  = result.get("conviction","MEDIUM")
        daily       = db.load_daily_cost()

        log.info("🤖  Agent review %-6s: %-6s  delta=%.0f  [%s]",
                 ticker, action, shares_delta, conviction)

        db.save_agent_decision("position_review", ticker, action,
                                abs(shares_delta), lp, reasoning, conviction,
                                daily["cost_usd"])

        company = pos.get("company","")

        if action == "EXIT" or (action == "REDUCE" and shares_delta < 0):
            sell_shares = pos["shares"] if action=="EXIT" else abs(shares_delta)
            sell_shares = min(sell_shares, pos["shares"])
            if sell_shares > 0:
                db.agent_execute_sell(ticker, company, sell_shares, lp)
                log.info("🤖  Agent SOLD %s: %.0f shares @ $%.2f", ticker, sell_shares, lp)
                await broadcast({
                    "type":      "agent_trade",
                    "action":    action,
                    "ticker":    ticker,
                    "shares":    sell_shares,
                    "price":     lp,
                    "reasoning": reasoning,
                    "portfolio": db.load_agent_portfolio(),
                })
        elif action == "ADD" and shares_delta > 0:
            agent_pf2 = db.load_agent_portfolio()
            if shares_delta * lp <= agent_pf2["cash"] * 0.8:
                db.agent_execute_buy(ticker, company,
                                     pos.get("sector","Unknown"),
                                     shares_delta, lp)
                log.info("🤖  Agent ADDED %s: %.0f shares @ $%.2f", ticker, shares_delta, lp)
                await broadcast({
                    "type":      "agent_trade",
                    "action":    "ADD",
                    "ticker":    ticker,
                    "shares":    shares_delta,
                    "price":     lp,
                    "reasoning": reasoning,
                    "portfolio": db.load_agent_portfolio(),
                })

    except Exception as e:
        log.error("🤖  Agent review failed for %s: %s", ticker, e)


async def run_agent_derisk(ticker: str) -> Optional[dict]:
    """
    Full Claude + web-search de-risk for an agent position.
    Identical in quality to the human de-risk, but scoped to agent data.
    Returns the parsed de-risk dict, or None on failure.
    """
    agent_pf = db.load_agent_portfolio()
    pos = next((p for p in agent_pf["positions"] if p["ticker"] == ticker), None)
    if not pos:
        return None

    log.info("🤖  Agent de-risking %s...", ticker)
    try:
        stock   = await fetch_stock_info(ticker)
        slim    = _slim(stock)
        lp      = stock.get("current_price") or pos.get("last_price") or pos["avg_cost"]
        pnl_pct = (lp - pos["avg_cost"]) / pos["avg_cost"] * 100
        pos_val = lp * pos["shares"]

        # Automatic rule-based thresholds the agent must respect
        stop_triggered    = pnl_pct <= -10.0
        profit_triggered  = pnl_pct >= 25.0
        mandatory_note    = ""
        if stop_triggered:
            mandatory_note = f"⚠ STOP LOSS TRIGGERED: position is down {pnl_pct:.1f}% which breaches the -10% hard stop. You MUST recommend EXIT."
        elif profit_triggered:
            mandatory_note = f"⚠ PROFIT TARGET TRIGGERED: position is up {pnl_pct:.1f}% which exceeds +25%. You MUST recommend at least REDUCE (sell 40%)."

        system = (
            "You are an autonomous AI portfolio risk manager reviewing one of your own positions. "
            "Be disciplined and objective. Enforce stop-loss and profit-taking rules strictly. "
            "Use web search for latest news on this stock. "
            "Return ONLY a valid JSON object. No markdown."
        )
        prompt = f"""De-risk review of your agent position in {ticker}. Today: {date.today().isoformat()}.

AGENT POSITION:
  Shares held:     {pos['shares']}
  Avg cost:        ${pos['avg_cost']:.2f}
  Current price:   ${lp:.2f}  (from yfinance)
  Unrealised P&L:  {pnl_pct:+.1f}%
  Position value:  ${pos_val:,.2f}
  Held since:      {pos.get('buy_date','unknown')}

REAL yfinance DATA:
{json.dumps(slim, indent=2)}

{mandatory_note}

AGENT RISK RULES (enforce strictly):
  • Hard stop loss:     EXIT if P&L ≤ -10%  {"← TRIGGERED NOW" if stop_triggered else ""}
  • Profit taking:      REDUCE 40% if P&L ≥ +25%  {"← TRIGGERED NOW" if profit_triggered else ""}
  • Trailing stop:      REDUCE 30% if position falls >8% from recent high
  • Thesis broken:      EXIT if fundamental story has materially changed

Use web search for latest earnings, analyst changes, news for {ticker}.

Return ONLY this JSON:
{{
  "current_price":      {round(lp, 2)},
  "recommendation":     "HOLD",
  "conviction":         "HIGH",
  "thesis_intact":      true,
  "updated_target":     0,
  "updated_stop":       {round(lp * 0.90, 2)},
  "unrealized_pnl_pct": {round(pnl_pct, 2)},
  "action_reason":      "profit_taking | loss_capping | thesis_broken | derisk | hold",
  "shares_to_sell":     0,
  "summary":            "3 sentence assessment with specific data and rule references",
  "developments":       ["Key development from news 1", "Key development 2"],
  "risks_ahead":        ["Risk 1", "Risk 2"],
  "action":             "Specific action: e.g. EXIT all shares — stop loss triggered at -12%"
}}"""

        raw    = await call_claude(prompt, system, use_search=True,
                                   max_tokens=2000, job=f"agent-derisk-{ticker}")
        result = parse_json(raw)
        if not result:
            # If parsing fails, still enforce hard rules
            result = {
                "current_price":      lp,
                "recommendation":     "EXIT" if stop_triggered else "HOLD",
                "unrealized_pnl_pct": round(pnl_pct, 2),
                "action_reason":      "loss_capping" if stop_triggered else "hold",
                "shares_to_sell":     pos["shares"] if stop_triggered else 0,
                "summary":            f"Rule-based: {pnl_pct:+.1f}% P&L.",
                "thesis_intact":      not stop_triggered,
            }

        # Always enforce hard rules regardless of LLM output
        if stop_triggered:
            result["recommendation"] = "EXIT"
            result["action_reason"]  = "loss_capping"
            result["shares_to_sell"] = pos["shares"]
        elif profit_triggered and result.get("recommendation") == "HOLD":
            result["recommendation"] = "REDUCE"
            result["action_reason"]  = "profit_taking"
            result["shares_to_sell"] = round(pos["shares"] * 0.40)

        result["current_price"] = lp
        db.agent_update_last_price(ticker, lp)
        return result

    except Exception as e:
        log.error("🤖  Agent de-risk failed for %s: %s", ticker, e)
        return None


async def run_agent_pipeline(plays: list, reports: dict) -> None:
    """
    Full agent pipeline — completely independent of human decisions:
      1. Review each new play report → BUY or PASS
      2. Run full Claude de-risk on every existing agent position
         → enforce stop-loss / profit-taking exits automatically
         → for HOLD outcomes, run secondary position review (HOLD/ADD/REDUCE/EXIT)
    """
    log.info("🤖  ═══════════════════════════════════════════")
    log.info("🤖  AGENT PIPELINE STARTING  (independent of human portfolio)")
    log.info("🤖  ═══════════════════════════════════════════")
    await broadcast({"type":"loading_start","job":"agent_pipeline",
                     "msg":"🤖 AI Agent making autonomous investment decisions..."})

    # ── Step 1: decide on new plays ────────────────────────────
    log.info("🤖  [1/2] Reviewing %d new plays...", len(plays))
    for i, play in enumerate(plays):
        ticker = play.get("ticker","")
        report = reports.get(ticker)
        if not report:
            log.warning("🤖  No report for %s — skipping", ticker); continue
        if i > 0:
            log.info("🤖  ⏳ 15s pacing...")
            await asyncio.sleep(15)
        await agent_decide_new_play(play, report)

    # ── Step 2: full de-risk on ALL existing agent positions ───
    agent_pf = db.load_agent_portfolio()
    if agent_pf["positions"]:
        log.info("🤖  [2/2] Running full de-risk on %d agent position(s)...",
                 len(agent_pf["positions"]))
        await asyncio.sleep(15)

        for i, pos in enumerate(agent_pf["positions"]):
            if i > 0:
                log.info("🤖  ⏳ 15s pacing...")
                await asyncio.sleep(15)

            derisk = await run_agent_derisk(pos["ticker"])
            if not derisk:
                continue

            rec     = derisk.get("recommendation","HOLD").upper()
            reason  = derisk.get("action_reason","hold")
            summary = derisk.get("summary","")
            lp      = derisk.get("current_price", pos.get("last_price", pos["avg_cost"]))
            daily   = db.load_daily_cost()

            log.info("🤖  %-6s → %-6s  [%s]  %.1f%% P&L",
                     pos["ticker"], rec, reason, derisk.get("unrealized_pnl_pct",0))

            db.save_agent_decision(
                "position_derisk", pos["ticker"], rec,
                derisk.get("shares_to_sell",0), lp,
                summary, derisk.get("conviction","MEDIUM"), daily["cost_usd"],
                action_reason=reason,
            )

            company = pos.get("company","")

            if rec == "EXIT":
                sell_shares = pos["shares"]
                ok = db.agent_execute_sell(pos["ticker"], company, sell_shares, lp)
                if ok:
                    log.info("🤖  CLOSED  %s: %.0f sh @ $%.2f  [%s]",
                             pos["ticker"], sell_shares, lp, reason)
                    await broadcast({
                        "type":      "agent_trade",
                        "action":    "EXIT",
                        "ticker":    pos["ticker"],
                        "shares":    sell_shares,
                        "price":     lp,
                        "reason":    reason,
                        "reasoning": summary,
                        "portfolio": db.load_agent_portfolio(),
                    })

            elif rec == "REDUCE":
                sell_shares = int(derisk.get("shares_to_sell", round(pos["shares"]*0.35)))
                sell_shares = max(1, min(sell_shares, pos["shares"]))
                ok = db.agent_execute_sell(pos["ticker"], company, sell_shares, lp)
                if ok:
                    log.info("🤖  REDUCED %s: sold %.0f / %.0f sh @ $%.2f  [%s]",
                             pos["ticker"], sell_shares, pos["shares"], lp, reason)
                    await broadcast({
                        "type":      "agent_trade",
                        "action":    "REDUCE",
                        "ticker":    pos["ticker"],
                        "shares":    sell_shares,
                        "price":     lp,
                        "reason":    reason,
                        "reasoning": summary,
                        "portfolio": db.load_agent_portfolio(),
                    })
            else:
                # For non-forced holds, run a second-pass portfolio-manager review
                # that can choose HOLD / ADD / REDUCE / EXIT using the de-risk context.
                await agent_review_position(pos["ticker"], derisk)
    else:
        log.info("🤖  No existing agent positions to de-risk.")

    # ── Summary ────────────────────────────────────────────────
    agent_pf = db.load_agent_portfolio()
    pos_val  = sum(p["shares"]*(p.get("last_price") or p["avg_cost"])
                   for p in agent_pf["positions"])
    total    = agent_pf["cash"] + pos_val
    txs      = agent_pf["transactions"]
    sells    = [t for t in txs if t["type"]=="sell" and t.get("pnl") is not None]
    wins     = [t for t in sells if t["pnl"] > 0]

    log.info("🤖  ═══════════════════════════════════════════")
    log.info("🤖  AGENT PIPELINE COMPLETE")
    log.info("🤖  Portfolio: $%.2f | Cash: $%.2f | Positions: %d",
             total, agent_pf["cash"], len(agent_pf["positions"]))
    log.info("🤖  Closed trades: %d  |  Win rate: %s",
             len(sells), f"{len(wins)/len(sells)*100:.0f}%" if sells else "n/a")
    log.info("🤖  ═══════════════════════════════════════════")

    await broadcast({
        "type":          "agent_pipeline_complete",
        "msg":           f"Agent complete — {len(agent_pf['positions'])} positions, "
                         f"{len(sells)} closed trades",
        "portfolio":     agent_pf,
        "decisions":     db.load_agent_decisions(20),
    })

async def daily_morning_run():
    if datetime.now().weekday() >= 5:
        log.info("⏭  daily_morning_run skipped — weekend")
        return

    log.info("═══════════════════════════════════════════════════")
    log.info("  9:00 AM FULL PIPELINE STARTING")
    log.info("  Brief → Plays → Reports → De-risk → Agent")
    log.info("═══════════════════════════════════════════════════")
    await broadcast({"type":"daily_run_start",
                     "msg":"Daily pipeline: Brief → Plays → Reports → De-risk → Agent"})

    # ── 1. Brief ──────────────────────────────────────────────
    brief = await generate_brief()
    if not brief:
        log.error("Pipeline aborted — brief failed"); return

    log.info("⏳  30s gap before plays...")
    await asyncio.sleep(30)

    # ── 2. Plays (brief-informed) ──────────────────────────────
    plays = await generate_plays(brief_context=brief)
    if not plays:
        log.error("Pipeline aborted — plays failed"); return

    log.info("⏳  30s gap before reports...")
    await asyncio.sleep(30)

    # ── 3. Auto-generate research reports for all 5 plays ──────
    log.info("📄  [3/5] Auto-generating research reports for all %d plays...", len(plays))
    reports: dict = {}   # ticker → report dict
    for i, play in enumerate(plays):
        if i > 0:
            log.info("  ⏳ 20s pacing before next report...")
            await asyncio.sleep(20)
        report = await generate_report(play["ticker"], play.get("company",""), auto=True)
        if report:
            reports[play["ticker"]] = report

    log.info("⏳  30s gap before human de-risk...")
    await asyncio.sleep(30)

    # ── 4. Auto de-risk ALL human positions ───────────────────
    log.info("⚡  [4/5] Auto de-risking all human positions...")
    await auto_derisk_all_positions()

    log.info("⏳  30s gap before agent pipeline...")
    await asyncio.sleep(30)

    # ── 5. AI Agent pipeline ───────────────────────────────────
    log.info("🤖  [5/5] AI Agent making autonomous decisions...")
    await run_agent_pipeline(plays, reports)

    # ── Done ───────────────────────────────────────────────────
    daily   = db.load_daily_cost()
    alltime = db.load_all_time_cost()
    log.info("═══════════════════════════════════════════════════")
    log.info("  ✅  FULL PIPELINE COMPLETE")
    log.info("  Today's API cost:  $%.5f  (%d calls)", daily["cost_usd"], daily["calls"])
    log.info("  All-time cost:     $%.5f", alltime["cost_usd"])
    log.info("═══════════════════════════════════════════════════")

    await broadcast({
        "type":         "daily_run_complete",
        "msg":          "Full pipeline complete — brief, plays, reports, de-risk, agent all done",
        "daily_cost":   round(daily["cost_usd"],   6),
        "daily_calls":  daily["calls"],
        "alltime_cost": round(alltime["cost_usd"], 6),
    })

# ═══════════════════════════════════════════════════════════════
# STARTUP / SHUTDOWN
# ═══════════════════════════════════════════════════════════════

@app.on_event("startup")
async def startup():
    global _live_price_task
    db.init_db()
    db.migrate_json_files()

    if not ANTHROPIC_API_KEY:
        log.error("⚠  ANTHROPIC_API_KEY not set! AI features will fail.")
    else:
        log.info("✅  API key set  (model: %s)", AI_MODEL)

    daily   = db.load_daily_cost()
    alltime = db.load_all_time_cost()
    log.info("💰  Today's cost so far:  $%.5f", daily["cost_usd"])
    log.info("💰  All-time cost:        $%.5f", alltime["cost_usd"])

    scheduler.add_job(daily_morning_run, CronTrigger(hour=9, minute=0, day_of_week="mon-fri"),
                      id="daily_run", replace_existing=True)
    scheduler.start()
    job = scheduler.get_job("daily_run")
    log.info("⏰  Scheduler started. Next run: %s", job.next_run_time if job else "?")

    now           = datetime.now()
    brief_missing = db.load_brief() is None
    plays_missing = db.load_plays() is None
    is_weekday    = now.weekday() < 5  # Mon=0 … Fri=4
    if not is_weekday:
        log.info("⏳  Weekend — no pipeline run today.")
    elif now.hour >= 9 and (brief_missing or plays_missing):
        log.info("⚡  Missed 9 AM run — running catch-up now...")
        asyncio.create_task(daily_morning_run())
    elif now.hour < 9:
        log.info("⏳  Before 9 AM — waiting for scheduled run.")
    else:
        log.info("✅  Today's data already exists.")

    _live_price_task = asyncio.create_task(live_price_poller())
    log.info("✅  Live price updater enabled (every 5 minutes)")

@app.on_event("shutdown")
async def shutdown():
    global _live_price_task
    if _live_price_task:
        _live_price_task.cancel()
        with suppress(asyncio.CancelledError):
            await _live_price_task
        _live_price_task = None
    scheduler.shutdown()
    log.info("Shutdown complete.")

# ═══════════════════════════════════════════════════════════════
# WEBSOCKET
# ═══════════════════════════════════════════════════════════════

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    _ws_clients.append(ws)
    # Send current cost state on connect so the counter is always accurate
    daily   = db.load_daily_cost()
    alltime = db.load_all_time_cost()
    await ws.send_json({
        "type":          "cost_sync",
        "daily_total":   round(daily["cost_usd"],   6),
        "daily_calls":   daily["calls"],
        "alltime_total": round(alltime["cost_usd"], 6),
    })
    await ws.send_json({"type":"connected"})
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        if ws in _ws_clients:
            _ws_clients.remove(ws)

# ═══════════════════════════════════════════════════════════════
# REST API
# ═══════════════════════════════════════════════════════════════

@app.get("/api/status")
async def status():
    job     = scheduler.get_job("daily_run")
    nxt     = str(job.next_run_time) if job and job.next_run_time else None
    daily   = db.load_daily_cost()
    alltime = db.load_all_time_cost()
    return {
        "time":         datetime.now().strftime("%H:%M:%S"),
        "date":         date.today().isoformat(),
        "brief_ready":  db.load_brief() is not None,
        "plays_ready":  db.load_plays() is not None,
        "next_run":     nxt,
        "api_key_set":  bool(ANTHROPIC_API_KEY),
        "model":        AI_MODEL,
        "db_path":      str(db.DB_PATH),
        "daily_cost":   round(daily["cost_usd"],   6),
        "alltime_cost": round(alltime["cost_usd"], 6),
        "daily_calls":  daily["calls"],
        "clients":      len(_ws_clients),
    }

@app.get("/api/portfolio")
async def get_portfolio(): return db.load_portfolio()

@app.get("/api/snapshots")
async def get_snapshots(): return db.load_snapshots()

@app.get("/api/brief")
async def get_brief(date_str: Optional[str] = None):
    b = db.load_brief(date_str) or db.load_latest_brief()
    if not b: raise HTTPException(404,"No brief available")
    return b

@app.get("/api/brief/history")
async def brief_history(): return db.load_brief_dates()

@app.get("/api/plays")
async def get_plays(date_str: Optional[str] = None):
    p = db.load_plays(date_str) or db.load_latest_plays()
    if not p: raise HTTPException(404,"No plays available")
    return p

@app.get("/api/plays/history")
async def plays_history(): return db.load_play_dates()

@app.get("/api/reports")
async def list_reports(): return db.load_all_report_meta()

@app.get("/api/report/{ticker}")
async def get_report(ticker: str):
    r = db.load_latest_report(ticker.upper())
    if not r: raise HTTPException(404,f"No report for {ticker}")
    return r

@app.get("/api/quote/{ticker}")
async def get_quote(ticker: str):
    t = ticker.strip().upper()
    if not re.fullmatch(r"[A-Z0-9][A-Z0-9.\-]{0,14}", t):
        raise HTTPException(400, "Invalid ticker format")

    stock = await fetch_stock_info(t)
    if stock.get("error"):
        raise HTTPException(404, f"Ticker {t} not found")

    price = stock.get("current_price")
    if not isinstance(price, (int, float)) or price <= 0:
        raise HTTPException(404, f"No current price available for {t}")

    return {
        "ticker": t,
        "company": stock.get("name", t),
        "sector": stock.get("sector", "Unknown"),
        "price": round(float(price), 2),
    }

@app.get("/api/costs/today")
async def costs_today(): return db.load_daily_cost()

@app.get("/api/costs/breakdown")
async def costs_breakdown(date_str: Optional[str] = None):
    return db.load_cost_breakdown(date_str)

@app.get("/api/costs/history")
async def costs_history(): return db.load_cost_history()

@app.post("/api/brief/generate")
async def trigger_brief():
    asyncio.create_task(generate_brief()); return {"status":"started"}

@app.post("/api/plays/generate")
async def trigger_plays():
    asyncio.create_task(generate_plays()); return {"status":"started"}

@app.post("/api/report/{ticker}")
async def trigger_report(ticker: str, company: str = ""):
    asyncio.create_task(generate_report(ticker.upper(), company)); return {"status":"started"}

@app.post("/api/derisk/{ticker}")
async def trigger_derisk(ticker: str):
    asyncio.create_task(run_derisk(ticker.upper())); return {"status":"started"}

@app.post("/api/run")
async def trigger_full_run():
    asyncio.create_task(daily_morning_run()); return {"status":"started"}

class TradeRequest(BaseModel):
    type: str; ticker: str; company: str; shares: float; price: float

@app.post("/api/trade")
async def execute_trade(req: TradeRequest):
    ticker = req.ticker.upper()
    try:
        company = (req.company or ticker).strip() or ticker
        sector = "Unknown"
        plays = db.load_plays()
        if plays:
            p = next((x for x in plays if x.get("ticker")==ticker), None)
            if p: sector = p.get("sector","Unknown")
        if req.type == "buy":
            if sector == "Unknown":
                stock = await fetch_stock_info(ticker)
                if not stock.get("error"):
                    sector = stock.get("sector", "Unknown")
                    if company == ticker and stock.get("name"):
                        company = stock["name"]
            return db.execute_buy(ticker, company, sector, req.shares, req.price)
        else:
            return db.execute_sell(ticker, company, req.shares, req.price)
    except ValueError as e:
        raise HTTPException(400, str(e))

@app.post("/api/reset")
async def reset_portfolio(): return db.reset_portfolio()

@app.get("/api/agent/portfolio")
async def get_agent_portfolio(): return db.load_agent_portfolio()

@app.get("/api/agent/snapshots")
async def get_agent_snapshots(): return db.load_agent_snapshots()

@app.get("/api/agent/decisions")
async def get_agent_decisions(): return db.load_agent_decisions(100)

@app.post("/api/agent/reset")
async def reset_agent(): return db.reset_agent_portfolio()

# ═══════════════════════════════════════════════════════════════
# STATIC + ENTRY POINT
# ═══════════════════════════════════════════════════════════════

app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")

@app.get("/")
async def root(): return FileResponse(str(STATIC / "index.html"))

if __name__ == "__main__":
    import uvicorn
    print("\n" + "═"*55)
    print(f"  ALPHA PM  —  {AI_MODEL}")
    print(f"  Pipeline: Brief → Plays → 5 Reports → Decision")
    print(f"  API key: {'SET ✅' if ANTHROPIC_API_KEY else 'NOT SET ❌'}")
    print(f"  Database: data/alpha_pm.db")
    print("  http://localhost:8000")
    print("═"*55 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
