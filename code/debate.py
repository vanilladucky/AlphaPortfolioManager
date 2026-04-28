"""
debate.py — Analyst Debate Pipeline
Bull vs Bear vs Judge using a local Ollama model (DeepSeek R1 14B).
Runs as a background task after the main Claude pipeline each weekday.

Toggle:
    DEBATE_ENABLED = True   # set to False to skip entirely
    DEBATE_MODEL   = "deepseek-r1:14b"
"""

import asyncio
import json
import logging
import re
from datetime import date
from typing import Callable, Optional

import httpx

import database as db

# ── Config ────────────────────────────────────────────────────
# Set DEBATE_ENABLED = False to skip the debate step entirely.
DEBATE_ENABLED    = True
DEBATE_MODEL      = "deepseek-r1:7b"
DEBATE_OLLAMA_URL = "http://localhost:11434/api/chat"

log = logging.getLogger("alpha-pm")

# Tickers whose debate is currently in flight — read by main.py on WS connect
_running: set[str] = set()


# ── Helpers ───────────────────────────────────────────────────

def _strip_think(text: str) -> str:
    """Remove DeepSeek R1 <think>…</think> reasoning blocks from output."""
    return re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()


async def call_ollama_stream(
    system: str, prompt: str,
    broadcast: Callable, ticker: str, role: str,
) -> str:
    """Stream tokens from Ollama, broadcasting each one via WebSocket."""
    body = {
        "model":    DEBATE_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": prompt},
        ],
        "stream": True,
    }
    full_text = ""
    try:
        async with httpx.AsyncClient(timeout=300.0) as client:
            async with client.stream("POST", DEBATE_OLLAMA_URL, json=body) as r:
                async for line in r.aiter_lines():
                    if not line:
                        continue
                    try:
                        chunk = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    token = chunk.get("message", {}).get("content", "")
                    if not token:
                        continue
                    full_text += token
                    # Determine if we're inside a <think> block
                    in_think = full_text.count("<think>") > full_text.count("</think>")
                    # Strip the tag markers themselves from display
                    display = token.replace("<think>", "").replace("</think>", "")
                    if display:
                        await broadcast({
                            "type":    "debate_token",
                            "ticker":  ticker,
                            "role":    role,
                            "token":   display,
                            "thinking": in_think,
                        })
        return _strip_think(full_text)
    except httpx.ConnectError:
        raise RuntimeError(
            "Ollama not reachable at localhost:11434 — run `ollama serve` first"
        )


# ── Core debate logic ─────────────────────────────────────────

async def run_debate(
    ticker: str,
    report: dict,
    brief: dict,
    broadcast: Callable,
) -> Optional[dict]:
    """Bull vs Bear vs Judge debate for one ticker."""
    log.info("🗣  Debate starting: %s (%s)...", ticker, DEBATE_MODEL)
    _running.add(ticker)
    await broadcast({"type": "loading_start", "job": f"debate_{ticker}",
                     "msg": f"🗣 Debating {ticker} — bull vs bear ({DEBATE_MODEL})..."})
    try:
        # ── Build shared context ───────────────────────────────
        sector = report.get("sector", "Unknown")
        sector_stance = "neutral"
        for s in brief.get("sector_rotation", []):
            if s.get("sector", "").lower() in sector.lower():
                sector_stance = s.get("stance", "neutral")
                break

        fin = report.get("financials", {})

        def _cats(items):
            return ", ".join(
                (c.get("catalyst", "") if isinstance(c, dict) else str(c))
                for c in items[:3]
            )

        def _risks(items):
            return ", ".join(
                (r.get("risk", "") if isinstance(r, dict) else str(r))
                for r in items[:3]
            )

        context = f"""STOCK: {ticker} — {report.get('company', ticker)}
Sector: {sector} | Macro stance: {sector_stance.upper()}
Price: ${report.get('current_price','?')} | Target: ${report.get('price_target','?')} | Upside: {report.get('upside_pct','?')}%
Analyst rating: {report.get('rating','?')}

INVESTMENT THESIS:
{report.get('investment_thesis', report.get('executive_summary',''))}

KEY FINANCIALS:
Revenue TTM: {fin.get('revenue_ttm','?')} | Growth: {fin.get('revenue_growth','?')}
Gross margin: {fin.get('gross_margin','?')} | Net margin: {fin.get('net_margin','?')}
P/E: {fin.get('pe_ratio','?')} | Fwd P/E: {fin.get('fwd_pe','?')} | EV/EBITDA: {fin.get('ev_ebitda','?')}
FCF: {fin.get('fcf','?')} | ROE: {fin.get('roe','?')} | Beta: {fin.get('beta','?')}

CATALYSTS: {_cats(report.get('catalysts', []))}
RISKS:     {_risks(report.get('risks', []))}

TODAY'S MACRO CONTEXT:
{brief.get('executive_summary', '')}"""

        BULL_SYS = (
            "You are a senior equity analyst with a growth-oriented, long-biased perspective. "
            "Cite specific numbers from the data provided. 3–4 sentences only."
        )
        BEAR_SYS = (
            "You are a sceptical short-seller and risk analyst. "
            "Be specific and contrarian. 3–4 sentences only."
        )

        # ── Round 1: Bull opening ──────────────────────────────
        await broadcast({"type": "debate_role_start", "ticker": ticker, "role": "bull"})
        bull = await call_ollama_stream(
            system=BULL_SYS,
            prompt=f"Make the opening bull case for {ticker}:\n\n{context}",
            broadcast=broadcast, ticker=ticker, role="bull",
        )
        log.info("🐂  Bull R1 done: %s", ticker)
        await asyncio.sleep(5)

        # ── Round 1: Bear rebuttal ─────────────────────────────
        await broadcast({"type": "debate_role_start", "ticker": ticker, "role": "bear"})
        bear = await call_ollama_stream(
            system=BEAR_SYS,
            prompt=(
                f"The bull analyst argued:\n{bull}\n\n"
                f"Rebut this and make the bear case for {ticker}:\n\n{context}"
            ),
            broadcast=broadcast, ticker=ticker, role="bear",
        )
        log.info("🐻  Bear R1 done: %s", ticker)
        await asyncio.sleep(5)

        # ── Round 2: Bull counter-rebuttal ─────────────────────
        await broadcast({"type": "debate_role_start", "ticker": ticker, "role": "bull2"})
        bull2 = await call_ollama_stream(
            system=BULL_SYS,
            prompt=(
                f"The bear analyst challenged your thesis:\n{bear}\n\n"
                f"Counter this and strengthen your bull argument for {ticker}:\n\n{context}"
            ),
            broadcast=broadcast, ticker=ticker, role="bull2",
        )
        log.info("🐂  Bull R2 done: %s", ticker)
        await asyncio.sleep(5)

        # ── Round 2: Bear final rebuttal ───────────────────────
        await broadcast({"type": "debate_role_start", "ticker": ticker, "role": "bear2"})
        bear2 = await call_ollama_stream(
            system=BEAR_SYS,
            prompt=(
                f"The bull analyst countered:\n{bull2}\n\n"
                f"Give your final rebuttal and closing bear case for {ticker}:\n\n{context}"
            ),
            broadcast=broadcast, ticker=ticker, role="bear2",
        )
        log.info("🐻  Bear R2 done: %s", ticker)
        await asyncio.sleep(5)

        # ── Judge verdict (sees all 4 turns) ───────────────────
        await broadcast({"type": "debate_role_start", "ticker": ticker, "role": "judge"})
        verdict_raw = await call_ollama_stream(
            system=(
                "You are a senior portfolio manager chairing an investment committee. "
                "You have heard a two-round debate between bull and bear analysts. "
                "Weigh all arguments objectively. "
                "Begin your response with exactly one word — BUY, HOLD, or PASS — "
                "then give your reasoning in 2–3 sentences."
            ),
            prompt=(
                f"Give your verdict on {ticker}.\n\n"
                f"CONTEXT:\n{context}\n\n"
                f"BULL R1:\n{bull}\n\n"
                f"BEAR R1:\n{bear}\n\n"
                f"BULL R2:\n{bull2}\n\n"
                f"BEAR R2:\n{bear2}"
            ),
            broadcast=broadcast, ticker=ticker, role="judge",
        )
        log.info("⚖️   Judge done: %s", ticker)

        first = verdict_raw.strip().split()[0].upper().rstrip(":.") if verdict_raw.strip() else "HOLD"
        action = first if first in ("BUY", "HOLD", "PASS") else "HOLD"

        result = {
            "ticker":         ticker,
            "model":          DEBATE_MODEL,
            "bull_case":      bull,
            "bear_case":      bear,
            "bull_case_2":    bull2,
            "bear_case_2":    bear2,
            "verdict":        verdict_raw,
            "verdict_action": action,
        }
        db.save_debate(ticker, date.today().isoformat(),
                       bull, bear, bull2, bear2, verdict_raw, action, DEBATE_MODEL)
        _running.discard(ticker)
        await broadcast({"type": "debate_complete", "ticker": ticker, "data": result})
        log.info("✅  Debate %s → %s", ticker, action)
        return result

    except Exception as exc:
        _running.discard(ticker)
        log.error("🗣  Debate failed for %s: %s", ticker, exc)
        await broadcast({"type": "error", "job": f"debate_{ticker}", "msg": str(exc)})
        return None


async def run_all_debates(
    plays: list,
    reports: dict,
    brief: dict,
    broadcast: Callable,
) -> None:
    """Run bull-vs-bear debates for all plays as a background task."""
    if not DEBATE_ENABLED:
        log.info("🗣  Debates disabled (DEBATE_ENABLED=False) — skipping")
        return

    log.info("🗣  ════════════════════════════════════════════")
    log.info("🗣  DEBATE PIPELINE STARTING — %s", DEBATE_MODEL)
    log.info("🗣  ════════════════════════════════════════════")
    await broadcast({"type": "loading_start", "job": "debates",
                     "msg": f"🗣 Analyst debates starting ({DEBATE_MODEL})..."})
    debated = 0
    for i, play in enumerate(plays):
        ticker = play.get("ticker", "")
        report = reports.get(ticker)
        if not report:
            log.warning("🗣  No report for %s — skipping debate", ticker)
            continue
        if i > 0:
            log.info("🗣  ⏳ 20s pacing before next debate...")
            await asyncio.sleep(20)
        result = await run_debate(ticker, report, brief, broadcast)
        if result:
            debated += 1

    log.info("🗣  ════════════════════════════════════════════")
    log.info("🗣  DEBATE PIPELINE COMPLETE — %d/%d stocks", debated, len(plays))
    log.info("🗣  ════════════════════════════════════════════")
    await broadcast({"type": "debates_complete",
                     "msg": f"All debates complete ({debated}/{len(plays)} stocks)"})
