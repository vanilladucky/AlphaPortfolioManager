"""
database.py — SQLite persistence layer for Alpha PM
────────────────────────────────────────────────────
All data lives in a single file: data/alpha_pm.db

Why SQLite over JSON files:
  • WAL (Write-Ahead Log) mode: crash-safe. Power loss mid-write
    never corrupts the database — the journal is replayed on next open.
  • Atomic transactions: portfolio cash + positions update together or
    not at all. No half-written state.
  • Full history: query briefs, plays, and snapshots from any past date.
  • Single file: easy to back up, copy, or sync to iCloud/Dropbox.

Schema:
  portfolio     — single-row: cash, start_cash, start_date
  positions     — open holdings
  transactions  — full ledger of every buy/sell
  snapshots     — daily portfolio value history (equity curve)
  briefs        — daily market briefs (JSON blob, keyed by date)
  plays         — daily equity plays (JSON blob, keyed by date)
  reports       — research reports (JSON blob, keyed by ticker+date)
"""

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger("alpha-pm.db")

DB_PATH = Path(__file__).parent.parent / "data" / "alpha_pm.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

STARTING_CASH = 1_000_000.0


# ═══════════════════════════════════════════════════════════════
# CONNECTION & SCHEMA
# ═══════════════════════════════════════════════════════════════

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # WAL mode: crash-safe, allows concurrent reads during writes
    conn.execute("PRAGMA journal_mode=WAL")
    # Synchronous=NORMAL: safe and faster than FULL (default)
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_db():
    """Context manager that auto-commits on success and rolls back on error."""
    conn = _get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create all tables if they don't exist. Safe to call on every startup."""
    with get_db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS portfolio (
            id          INTEGER PRIMARY KEY DEFAULT 1,
            cash        REAL    NOT NULL DEFAULT 1000000.0,
            start_cash  REAL    NOT NULL DEFAULT 1000000.0,
            start_date  TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS positions (
            ticker      TEXT    PRIMARY KEY,
            company     TEXT    NOT NULL DEFAULT '',
            sector      TEXT    NOT NULL DEFAULT 'Unknown',
            shares      REAL    NOT NULL,
            avg_cost    REAL    NOT NULL,
            last_price  REAL,
            buy_date    TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS transactions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          INTEGER NOT NULL,
            type        TEXT    NOT NULL,
            ticker      TEXT    NOT NULL,
            company     TEXT    NOT NULL DEFAULT '',
            shares      REAL    NOT NULL,
            price       REAL    NOT NULL,
            total       REAL    NOT NULL,
            pnl         REAL,
            date        TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS snapshots (
            date        TEXT    PRIMARY KEY,
            value       REAL    NOT NULL,
            cash        REAL    NOT NULL,
            pos_value   REAL    NOT NULL,
            ts          INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS briefs (
            date        TEXT    PRIMARY KEY,
            generated_at TEXT   NOT NULL,
            data        TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS plays (
            date        TEXT    PRIMARY KEY,
            data        TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS reports (
            ticker      TEXT    NOT NULL,
            date        TEXT    NOT NULL,
            data        TEXT    NOT NULL,
            PRIMARY KEY (ticker, date)
        );

        CREATE TABLE IF NOT EXISTS api_costs (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            ts            INTEGER NOT NULL,
            date          TEXT    NOT NULL,
            job           TEXT    NOT NULL,
            input_tokens  INTEGER NOT NULL DEFAULT 0,
            output_tokens INTEGER NOT NULL DEFAULT 0,
            cost_usd      REAL    NOT NULL DEFAULT 0.0,
            model         TEXT    NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS debates (
            date           TEXT NOT NULL,
            ticker         TEXT NOT NULL,
            model          TEXT NOT NULL,
            bull_case      TEXT NOT NULL,
            bear_case      TEXT NOT NULL,
            verdict        TEXT NOT NULL,
            verdict_action TEXT NOT NULL,
            created_at     TEXT NOT NULL,
            PRIMARY KEY (date, ticker)
        );
        """)

        # Migrate debates table to two-round format
        for col in ("bull_case_2", "bear_case_2"):
            try:
                conn.execute(f"ALTER TABLE debates ADD COLUMN {col} TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass  # column already exists

        # Seed portfolio row if fresh database
        row = conn.execute("SELECT id FROM portfolio WHERE id=1").fetchone()
        if not row:
            conn.execute(
                "INSERT INTO portfolio (id, cash, start_cash, start_date) VALUES (1,?,?,?)",
                (STARTING_CASH, STARTING_CASH, date.today().isoformat()),
            )
            log.info("🆕  Fresh database initialised with $%.0f", STARTING_CASH)
        else:
            log.info("✅  Database loaded from %s", DB_PATH)

    # Initialise agent tables too
    init_agent_db()


# ═══════════════════════════════════════════════════════════════
# PORTFOLIO
# ═══════════════════════════════════════════════════════════════

def load_portfolio() -> dict:
    with get_db() as conn:
        pf  = conn.execute("SELECT * FROM portfolio WHERE id=1").fetchone()
        pos = conn.execute("SELECT * FROM positions ORDER BY ticker").fetchall()
        txs = conn.execute(
            "SELECT * FROM transactions ORDER BY ts DESC LIMIT 200"
        ).fetchall()

    positions = [
        {
            "ticker":     r["ticker"],
            "company":    r["company"],
            "sector":     r["sector"],
            "shares":     r["shares"],
            "avg_cost":   r["avg_cost"],
            "last_price": r["last_price"],
            "buy_date":   r["buy_date"],
        }
        for r in pos
    ]
    transactions = [
        {
            "id":      r["id"],
            "ts":      r["ts"],
            "type":    r["type"],
            "ticker":  r["ticker"],
            "company": r["company"],
            "shares":  r["shares"],
            "price":   r["price"],
            "total":   r["total"],
            "pnl":     r["pnl"],
            "date":    r["date"],
        }
        for r in txs
    ]
    return {
        "cash":         pf["cash"],
        "start_cash":   pf["start_cash"],
        "start_date":   pf["start_date"],
        "positions":    positions,
        "transactions": transactions,
    }


def execute_buy(ticker: str, company: str, sector: str,
                shares: float, price: float) -> dict:
    """Atomically update cash + position and insert transaction."""
    today_str = date.today().isoformat()
    cost      = shares * price
    ts        = int(datetime.now().timestamp() * 1000)

    with get_db() as conn:
        pf = conn.execute("SELECT cash FROM portfolio WHERE id=1").fetchone()
        if pf["cash"] < cost:
            raise ValueError("Insufficient cash")

        # Update or insert position
        existing = conn.execute(
            "SELECT shares, avg_cost FROM positions WHERE ticker=?", (ticker,)
        ).fetchone()

        if existing:
            new_shares   = existing["shares"] + shares
            new_avg_cost = (existing["shares"] * existing["avg_cost"] + shares * price) / new_shares
            conn.execute(
                """UPDATE positions
                   SET shares=?, avg_cost=?, last_price=?, company=?, sector=?
                   WHERE ticker=?""",
                (new_shares, new_avg_cost, price, company, sector, ticker),
            )
        else:
            conn.execute(
                """INSERT INTO positions (ticker,company,sector,shares,avg_cost,last_price,buy_date)
                   VALUES (?,?,?,?,?,?,?)""",
                (ticker, company, sector, shares, price, price, today_str),
            )

        conn.execute(
            "UPDATE portfolio SET cash=cash-? WHERE id=1", (cost,)
        )
        conn.execute(
            """INSERT INTO transactions (ts,type,ticker,company,shares,price,total,pnl,date)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (ts, "buy", ticker, company, shares, price, cost, None, today_str),
        )

    _take_snapshot()
    return load_portfolio()


def execute_sell(ticker: str, company: str,
                 shares: float, price: float) -> dict:
    """Atomically reduce/remove position and insert transaction."""
    today_str = date.today().isoformat()
    proceeds  = shares * price
    ts        = int(datetime.now().timestamp() * 1000)

    with get_db() as conn:
        pos = conn.execute(
            "SELECT shares, avg_cost FROM positions WHERE ticker=?", (ticker,)
        ).fetchone()
        if not pos or shares > pos["shares"]:
            raise ValueError("Invalid sell quantity")

        pnl = (price - pos["avg_cost"]) * shares

        if shares == pos["shares"]:
            conn.execute("DELETE FROM positions WHERE ticker=?", (ticker,))
        else:
            conn.execute(
                "UPDATE positions SET shares=shares-?, last_price=? WHERE ticker=?",
                (shares, price, ticker),
            )

        conn.execute(
            "UPDATE portfolio SET cash=cash+? WHERE id=1", (proceeds,)
        )
        conn.execute(
            """INSERT INTO transactions (ts,type,ticker,company,shares,price,total,pnl,date)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (ts, "sell", ticker, company, shares, price, proceeds, pnl, today_str),
        )

    _take_snapshot()
    return load_portfolio()


def update_last_price(ticker: str, price: float):
    """Update the cached last price for a position (no snapshot needed)."""
    with get_db() as conn:
        conn.execute(
            "UPDATE positions SET last_price=? WHERE ticker=?", (price, ticker)
        )


def reset_portfolio():
    """Wipe all positions, transactions, and snapshots. Reset cash to $1M."""
    with get_db() as conn:
        conn.execute("DELETE FROM positions")
        conn.execute("DELETE FROM transactions")
        conn.execute("DELETE FROM snapshots")
        conn.execute(
            "UPDATE portfolio SET cash=?, start_cash=?, start_date=? WHERE id=1",
            (STARTING_CASH, STARTING_CASH, date.today().isoformat()),
        )
    log.info("Portfolio reset to $%.0f", STARTING_CASH)
    return load_portfolio()


# ═══════════════════════════════════════════════════════════════
# SNAPSHOTS
# ═══════════════════════════════════════════════════════════════

def _take_snapshot():
    """Compute current total value and upsert today's snapshot."""
    with get_db() as conn:
        pf  = conn.execute("SELECT cash FROM portfolio WHERE id=1").fetchone()
        pos = conn.execute("SELECT shares, last_price, avg_cost FROM positions").fetchall()
        pos_val = sum(r["shares"] * (r["last_price"] or r["avg_cost"]) for r in pos)
        total   = pf["cash"] + pos_val
        today_s = date.today().isoformat()
        conn.execute(
            """INSERT INTO snapshots (date, value, cash, pos_value, ts)
               VALUES (?,?,?,?,?)
               ON CONFLICT(date) DO UPDATE SET
                 value=excluded.value,
                 cash=excluded.cash,
                 pos_value=excluded.pos_value,
                 ts=excluded.ts""",
            (today_s, round(total, 2), round(pf["cash"], 2),
             round(pos_val, 2), int(datetime.now().timestamp())),
        )
    return total


def take_snapshot():
    return _take_snapshot()


def load_snapshots() -> list:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM snapshots ORDER BY date"
        ).fetchall()
    return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════════════
# BRIEFS
# ═══════════════════════════════════════════════════════════════

def save_brief(data: dict):
    date_str = data.get("date", date.today().isoformat())
    gen_at   = data.get("generated_at", datetime.now().strftime("%H:%M"))
    with get_db() as conn:
        conn.execute(
            """INSERT INTO briefs (date, generated_at, data)
               VALUES (?,?,?)
               ON CONFLICT(date) DO UPDATE SET
                 generated_at=excluded.generated_at,
                 data=excluded.data""",
            (date_str, gen_at, json.dumps(data)),
        )


def load_brief(date_str: Optional[str] = None) -> Optional[dict]:
    date_str = date_str or date.today().isoformat()
    with get_db() as conn:
        row = conn.execute(
            "SELECT data FROM briefs WHERE date=?", (date_str,)
        ).fetchone()
    return json.loads(row["data"]) if row else None


def load_brief_dates() -> list:
    """Return all dates that have a saved brief, newest first."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT date, generated_at FROM briefs ORDER BY date DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def load_latest_brief() -> Optional[dict]:
    """Return the most recent brief regardless of date."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT data FROM briefs ORDER BY date DESC LIMIT 1"
        ).fetchone()
    return json.loads(row["data"]) if row else None


# ═══════════════════════════════════════════════════════════════
# PLAYS
# ═══════════════════════════════════════════════════════════════

def save_plays(plays: list, date_str: Optional[str] = None):
    date_str = date_str or date.today().isoformat()
    with get_db() as conn:
        conn.execute(
            """INSERT INTO plays (date, data) VALUES (?,?)
               ON CONFLICT(date) DO UPDATE SET data=excluded.data""",
            (date_str, json.dumps(plays)),
        )


def load_plays(date_str: Optional[str] = None) -> Optional[list]:
    date_str = date_str or date.today().isoformat()
    with get_db() as conn:
        row = conn.execute(
            "SELECT data FROM plays WHERE date=?", (date_str,)
        ).fetchone()
    return json.loads(row["data"]) if row else None


def load_play_dates() -> list:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT date FROM plays ORDER BY date DESC"
        ).fetchall()
    return [r["date"] for r in rows]


def load_latest_plays() -> Optional[list]:
    with get_db() as conn:
        row = conn.execute(
            "SELECT data FROM plays ORDER BY date DESC LIMIT 1"
        ).fetchone()
    return json.loads(row["data"]) if row else None


# ═══════════════════════════════════════════════════════════════
# REPORTS
# ═══════════════════════════════════════════════════════════════

def save_report(ticker: str, data: dict, date_str: Optional[str] = None):
    date_str = date_str or date.today().isoformat()
    with get_db() as conn:
        conn.execute(
            """INSERT INTO reports (ticker, date, data) VALUES (?,?,?)
               ON CONFLICT(ticker, date) DO UPDATE SET data=excluded.data""",
            (ticker.upper(), date_str, json.dumps(data)),
        )


def load_report(ticker: str, date_str: Optional[str] = None) -> Optional[dict]:
    date_str = date_str or date.today().isoformat()
    with get_db() as conn:
        row = conn.execute(
            "SELECT data FROM reports WHERE ticker=? AND date=?",
            (ticker.upper(), date_str),
        ).fetchone()
    return json.loads(row["data"]) if row else None


def load_latest_report(ticker: str) -> Optional[dict]:
    with get_db() as conn:
        row = conn.execute(
            "SELECT data FROM reports WHERE ticker=? ORDER BY date DESC LIMIT 1",
            (ticker.upper(),),
        ).fetchone()
    return json.loads(row["data"]) if row else None


def load_all_report_meta() -> list:
    """Return ticker + date for every saved report, newest first."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT ticker, date FROM reports ORDER BY date DESC, ticker"
        ).fetchall()
    return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════════════
# DEBATES
# ═══════════════════════════════════════════════════════════════

def save_debate(ticker: str, date_str: str,
                bull_case: str, bear_case: str,
                bull_case_2: str, bear_case_2: str,
                verdict: str, verdict_action: str, model: str):
    with get_db() as conn:
        conn.execute(
            """INSERT INTO debates
                   (date, ticker, model, bull_case, bear_case, bull_case_2, bear_case_2,
                    verdict, verdict_action, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(date, ticker) DO UPDATE SET
                   model=excluded.model,
                   bull_case=excluded.bull_case, bear_case=excluded.bear_case,
                   bull_case_2=excluded.bull_case_2, bear_case_2=excluded.bear_case_2,
                   verdict=excluded.verdict, verdict_action=excluded.verdict_action,
                   created_at=excluded.created_at""",
            (date_str, ticker.upper(), model,
             bull_case, bear_case, bull_case_2, bear_case_2,
             verdict, verdict_action, datetime.now().strftime("%H:%M")),
        )


def load_debate(ticker: str, date_str: Optional[str] = None) -> Optional[dict]:
    date_str = date_str or date.today().isoformat()
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM debates WHERE ticker=? AND date=?",
            (ticker.upper(), date_str),
        ).fetchone()
    return dict(row) if row else None


def load_latest_debate(ticker: str) -> Optional[dict]:
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM debates WHERE ticker=? ORDER BY date DESC LIMIT 1",
            (ticker.upper(),),
        ).fetchone()
    return dict(row) if row else None


# ═══════════════════════════════════════════════════════════════
# MIGRATION: import existing JSON files into SQLite (one-time)
# ═══════════════════════════════════════════════════════════════

def migrate_json_files():
    """
    If old JSON files exist from a previous version of Alpha PM,
    import them into SQLite automatically so no history is lost.
    """
    import os
    BASE = Path(__file__).parent.parent / "data"

    migrated = 0

    # portfolio.json
    port_f = BASE / "portfolio.json"
    if port_f.exists():
        try:
            old = json.loads(port_f.read_text())
            with get_db() as conn:
                conn.execute(
                    "UPDATE portfolio SET cash=?, start_cash=?, start_date=? WHERE id=1",
                    (old.get("cash", STARTING_CASH),
                     old.get("start_cash", STARTING_CASH),
                     old.get("start_date", date.today().isoformat())),
                )
                for pos in old.get("positions", []):
                    conn.execute(
                        """INSERT OR IGNORE INTO positions
                           (ticker,company,sector,shares,avg_cost,last_price,buy_date)
                           VALUES (?,?,?,?,?,?,?)""",
                        (pos["ticker"], pos.get("company",""),
                         pos.get("sector","Unknown"),
                         pos["shares"], pos["avg_cost"],
                         pos.get("last_price"), pos.get("buy_date",""))
                    )
                for tx in old.get("transactions", []):
                    conn.execute(
                        """INSERT OR IGNORE INTO transactions
                           (id,ts,type,ticker,company,shares,price,total,pnl,date)
                           VALUES (?,?,?,?,?,?,?,?,?,?)""",
                        (tx.get("id"), tx.get("id",0),
                         tx["type"], tx["ticker"], tx.get("company",""),
                         tx["shares"], tx["price"], tx["total"],
                         tx.get("pnl"), tx.get("date",""))
                    )
            port_f.rename(port_f.with_suffix(".json.migrated"))
            migrated += 1
            log.info("📦  Migrated portfolio.json → SQLite")
        except Exception as e:
            log.warning("portfolio.json migration failed: %s", e)

    # snapshots.json
    snap_f = BASE / "snapshots.json"
    if snap_f.exists():
        try:
            snaps = json.loads(snap_f.read_text())
            with get_db() as conn:
                for s in snaps:
                    conn.execute(
                        """INSERT OR IGNORE INTO snapshots (date,value,cash,pos_value,ts)
                           VALUES (?,?,?,?,?)""",
                        (s["date"], s["value"], s.get("cash",0),
                         s.get("pos_value",0), s.get("ts",0))
                    )
            snap_f.rename(snap_f.with_suffix(".json.migrated"))
            migrated += 1
            log.info("📦  Migrated snapshots.json → SQLite")
        except Exception as e:
            log.warning("snapshots.json migration failed: %s", e)

    # briefs/YYYY-MM-DD.json
    briefs_dir = BASE / "briefs"
    if briefs_dir.exists():
        for f in sorted(briefs_dir.glob("*.json")):
            try:
                data = json.loads(f.read_text())
                save_brief(data)
                f.rename(f.with_suffix(".json.migrated"))
                migrated += 1
            except Exception as e:
                log.warning("Brief migration %s failed: %s", f.name, e)
        if migrated:
            log.info("📦  Migrated brief JSON files → SQLite")

    # plays/YYYY-MM-DD.json
    plays_dir = BASE / "plays"
    if plays_dir.exists():
        for f in sorted(plays_dir.glob("*.json")):
            try:
                data = json.loads(f.read_text())
                date_str = f.stem
                save_plays(data, date_str)
                f.rename(f.with_suffix(".json.migrated"))
                migrated += 1
            except Exception as e:
                log.warning("Plays migration %s failed: %s", f.name, e)

    # reports/TICKER_DATE.json
    reports_dir = BASE / "reports"
    if reports_dir.exists():
        for f in sorted(reports_dir.glob("*.json")):
            try:
                parts = f.stem.split("_")
                if len(parts) >= 2:
                    ticker   = parts[0]
                    date_str = "_".join(parts[1:])
                    data     = json.loads(f.read_text())
                    save_report(ticker, data, date_str)
                    f.rename(f.with_suffix(".json.migrated"))
                    migrated += 1
            except Exception as e:
                log.warning("Report migration %s failed: %s", f.name, e)

    if migrated:
        log.info("✅  Migration complete: %d files imported into SQLite", migrated)


# ═══════════════════════════════════════════════════════════════
# API COST TRACKING
# ═══════════════════════════════════════════════════════════════

IN_RATE  = 1.0 / 1_000_000   # $1.00 per million input tokens
OUT_RATE = 5.0 / 1_000_000   # $5.00 per million output tokens


def save_cost(job: str, input_tokens: int, output_tokens: int,
              cost_usd: float, model: str = "claude-haiku-4-5-20251001"):
    """Record a single API call's token usage and cost."""
    today_s = date.today().isoformat()
    with get_db() as conn:
        conn.execute(
            """INSERT INTO api_costs (ts, date, job, input_tokens, output_tokens, cost_usd, model)
               VALUES (?,?,?,?,?,?,?)""",
            (int(datetime.now().timestamp()), today_s,
             job, input_tokens, output_tokens, cost_usd, model),
        )


def load_daily_cost(date_str: Optional[str] = None) -> dict:
    """Return total cost + token breakdown for a given day (default: today)."""
    date_str = date_str or date.today().isoformat()
    with get_db() as conn:
        row = conn.execute(
            """SELECT
                 COALESCE(SUM(input_tokens),  0) AS input_tokens,
                 COALESCE(SUM(output_tokens), 0) AS output_tokens,
                 COALESCE(SUM(cost_usd),      0) AS cost_usd,
                 COUNT(*)                         AS calls
               FROM api_costs WHERE date=?""",
            (date_str,),
        ).fetchone()
    return {
        "date":          date_str,
        "input_tokens":  row["input_tokens"],
        "output_tokens": row["output_tokens"],
        "cost_usd":      round(row["cost_usd"], 6),
        "calls":         row["calls"],
    }


def load_all_time_cost() -> dict:
    """Return lifetime totals."""
    with get_db() as conn:
        row = conn.execute(
            """SELECT
                 COALESCE(SUM(input_tokens),  0) AS input_tokens,
                 COALESCE(SUM(output_tokens), 0) AS output_tokens,
                 COALESCE(SUM(cost_usd),      0) AS cost_usd,
                 COUNT(*)                         AS calls
               FROM api_costs"""
        ).fetchone()
    return {
        "input_tokens":  row["input_tokens"],
        "output_tokens": row["output_tokens"],
        "cost_usd":      round(row["cost_usd"], 6),
        "calls":         row["calls"],
    }


def load_cost_history() -> list:
    """Return per-day cost summary, newest first."""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT date,
                 SUM(input_tokens)  AS input_tokens,
                 SUM(output_tokens) AS output_tokens,
                 SUM(cost_usd)      AS cost_usd,
                 COUNT(*)           AS calls
               FROM api_costs
               GROUP BY date
               ORDER BY date DESC"""
        ).fetchall()
    return [dict(r) for r in rows]


def load_cost_breakdown(date_str: Optional[str] = None) -> list:
    """Return individual call costs for a given day, newest first."""
    date_str = date_str or date.today().isoformat()
    with get_db() as conn:
        rows = conn.execute(
            """SELECT job, input_tokens, output_tokens, cost_usd, model, ts
               FROM api_costs WHERE date=?
               ORDER BY ts DESC""",
            (date_str,),
        ).fetchall()
    return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════════════
# AGENT PORTFOLIO  — mirrors human portfolio, fully separate
# ═══════════════════════════════════════════════════════════════

AGENT_STARTING_CASH = 1_000_000.0

# ── Schema (added to init_db via ALTER-safe pattern) ───────────

AGENT_SCHEMA = """
CREATE TABLE IF NOT EXISTS agent_portfolio (
    id          INTEGER PRIMARY KEY DEFAULT 1,
    cash        REAL    NOT NULL DEFAULT 1000000.0,
    start_cash  REAL    NOT NULL DEFAULT 1000000.0,
    start_date  TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_positions (
    ticker      TEXT    PRIMARY KEY,
    company     TEXT    NOT NULL DEFAULT '',
    sector      TEXT    NOT NULL DEFAULT 'Unknown',
    shares      REAL    NOT NULL,
    avg_cost    REAL    NOT NULL,
    last_price  REAL,
    buy_date    TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_transactions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          INTEGER NOT NULL,
    type        TEXT    NOT NULL,
    ticker      TEXT    NOT NULL,
    company     TEXT    NOT NULL DEFAULT '',
    shares      REAL    NOT NULL,
    price       REAL    NOT NULL,
    total       REAL    NOT NULL,
    pnl         REAL,
    date        TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_snapshots (
    date        TEXT    PRIMARY KEY,
    value       REAL    NOT NULL,
    cash        REAL    NOT NULL,
    pos_value   REAL    NOT NULL,
    ts          INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_decisions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           INTEGER NOT NULL,
    date         TEXT    NOT NULL,
    decision_type TEXT   NOT NULL,
    ticker       TEXT    NOT NULL,
    action       TEXT    NOT NULL,
    action_reason TEXT   NOT NULL DEFAULT '',
    shares       REAL    NOT NULL DEFAULT 0,
    price        REAL    NOT NULL DEFAULT 0,
    reasoning    TEXT    NOT NULL DEFAULT '',
    conviction   TEXT    NOT NULL DEFAULT '',
    cost_usd     REAL    NOT NULL DEFAULT 0
);
"""

def init_agent_db():
    """Create agent tables. Called from init_db on startup."""
    with get_db() as conn:
        conn.executescript(AGENT_SCHEMA)
        row = conn.execute("SELECT id FROM agent_portfolio WHERE id=1").fetchone()
        if not row:
            conn.execute(
                "INSERT INTO agent_portfolio (id,cash,start_cash,start_date) VALUES (1,?,?,?)",
                (AGENT_STARTING_CASH, AGENT_STARTING_CASH, date.today().isoformat()),
            )
            log.info("🤖  Agent portfolio initialised with $%.0f", AGENT_STARTING_CASH)

# ── Agent portfolio read ───────────────────────────────────────

def load_agent_portfolio() -> dict:
    with get_db() as conn:
        pf  = conn.execute("SELECT * FROM agent_portfolio WHERE id=1").fetchone()
        pos = conn.execute("SELECT * FROM agent_positions ORDER BY ticker").fetchall()
        txs = conn.execute(
            "SELECT * FROM agent_transactions ORDER BY ts DESC LIMIT 200"
        ).fetchall()
    return {
        "cash":         pf["cash"],
        "start_cash":   pf["start_cash"],
        "start_date":   pf["start_date"],
        "positions":    [dict(r) for r in pos],
        "transactions": [dict(r) for r in txs],
    }

def load_agent_snapshots() -> list:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM agent_snapshots ORDER BY date"
        ).fetchall()
    return [dict(r) for r in rows]

def load_agent_decisions(limit: int = 50) -> list:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM agent_decisions ORDER BY ts DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]

# ── Agent trade execution ──────────────────────────────────────

def _agent_take_snapshot():
    with get_db() as conn:
        pf  = conn.execute("SELECT cash FROM agent_portfolio WHERE id=1").fetchone()
        pos = conn.execute(
            "SELECT shares, last_price, avg_cost FROM agent_positions"
        ).fetchall()
        pos_val = sum(r["shares"] * (r["last_price"] or r["avg_cost"]) for r in pos)
        total   = pf["cash"] + pos_val
        today_s = date.today().isoformat()
        conn.execute(
            """INSERT INTO agent_snapshots (date,value,cash,pos_value,ts)
               VALUES (?,?,?,?,?)
               ON CONFLICT(date) DO UPDATE SET
                 value=excluded.value, cash=excluded.cash,
                 pos_value=excluded.pos_value, ts=excluded.ts""",
            (today_s, round(total,2), round(pf["cash"],2),
             round(pos_val,2), int(datetime.now().timestamp())),
        )
    return total

def agent_execute_buy(ticker: str, company: str, sector: str,
                      shares: float, price: float) -> bool:
    """Execute an agent buy. Returns True on success, False if insufficient cash."""
    today_s = date.today().isoformat()
    cost    = shares * price
    ts      = int(datetime.now().timestamp() * 1000)
    with get_db() as conn:
        pf = conn.execute("SELECT cash FROM agent_portfolio WHERE id=1").fetchone()
        if pf["cash"] < cost:
            log.warning("🤖  Agent: insufficient cash for %s ($%.2f needed, $%.2f available)",
                        ticker, cost, pf["cash"])
            return False
        existing = conn.execute(
            "SELECT shares, avg_cost FROM agent_positions WHERE ticker=?", (ticker,)
        ).fetchone()
        if existing:
            new_shares   = existing["shares"] + shares
            new_avg_cost = (existing["shares"]*existing["avg_cost"] + shares*price) / new_shares
            conn.execute(
                "UPDATE agent_positions SET shares=?,avg_cost=?,last_price=?,company=?,sector=? WHERE ticker=?",
                (new_shares, new_avg_cost, price, company, sector, ticker),
            )
        else:
            conn.execute(
                "INSERT INTO agent_positions (ticker,company,sector,shares,avg_cost,last_price,buy_date) VALUES (?,?,?,?,?,?,?)",
                (ticker, company, sector, shares, price, price, today_s),
            )
        conn.execute("UPDATE agent_portfolio SET cash=cash-? WHERE id=1", (cost,))
        conn.execute(
            "INSERT INTO agent_transactions (ts,type,ticker,company,shares,price,total,pnl,date) VALUES (?,?,?,?,?,?,?,?,?)",
            (ts,"buy",ticker,company,shares,price,cost,None,today_s),
        )
    _agent_take_snapshot()
    return True

def agent_execute_sell(ticker: str, company: str,
                       shares: float, price: float) -> bool:
    """Execute an agent sell. Returns True on success."""
    today_s  = date.today().isoformat()
    proceeds = shares * price
    ts       = int(datetime.now().timestamp() * 1000)
    with get_db() as conn:
        pos = conn.execute(
            "SELECT shares, avg_cost FROM agent_positions WHERE ticker=?", (ticker,)
        ).fetchone()
        if not pos or shares > pos["shares"]:
            return False
        pnl = (price - pos["avg_cost"]) * shares
        if shares == pos["shares"]:
            conn.execute("DELETE FROM agent_positions WHERE ticker=?", (ticker,))
        else:
            conn.execute(
                "UPDATE agent_positions SET shares=shares-?,last_price=? WHERE ticker=?",
                (shares, price, ticker),
            )
        conn.execute("UPDATE agent_portfolio SET cash=cash+? WHERE id=1", (proceeds,))
        conn.execute(
            "INSERT INTO agent_transactions (ts,type,ticker,company,shares,price,total,pnl,date) VALUES (?,?,?,?,?,?,?,?,?)",
            (ts,"sell",ticker,company,shares,price,proceeds,pnl,today_s),
        )
    _agent_take_snapshot()
    return True

def agent_update_last_price(ticker: str, price: float):
    with get_db() as conn:
        conn.execute(
            "UPDATE agent_positions SET last_price=? WHERE ticker=?", (price, ticker)
        )

def save_agent_decision(decision_type: str, ticker: str, action: str,
                        shares: float, price: float, reasoning: str,
                        conviction: str, cost_usd: float,
                        action_reason: str = ""):
    """Log an agent decision for later review and comparison."""
    with get_db() as conn:
        conn.execute(
            """INSERT INTO agent_decisions
               (ts,date,decision_type,ticker,action,action_reason,shares,price,reasoning,conviction,cost_usd)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (int(datetime.now().timestamp()), date.today().isoformat(),
             decision_type, ticker, action, action_reason,
             shares, price, reasoning, conviction, cost_usd),
        )

def reset_agent_portfolio():
    with get_db() as conn:
        conn.execute("DELETE FROM agent_positions")
        conn.execute("DELETE FROM agent_transactions")
        conn.execute("DELETE FROM agent_snapshots")
        conn.execute("DELETE FROM agent_decisions")
        conn.execute(
            "UPDATE agent_portfolio SET cash=?,start_cash=?,start_date=? WHERE id=1",
            (AGENT_STARTING_CASH, AGENT_STARTING_CASH, date.today().isoformat()),
        )
    log.info("🤖  Agent portfolio reset to $%.0f", AGENT_STARTING_CASH)
