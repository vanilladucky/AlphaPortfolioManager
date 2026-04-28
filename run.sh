#!/bin/bash
# ═══════════════════════════════════════════════════════════
#  Alpha PM — AI Portfolio Manager
#  Claude Haiku 4.5 + yfinance + web search
#  No Ollama needed. Just an Anthropic API key.
# ═══════════════════════════════════════════════════════════

set -e

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  ALPHA PM  —  Claude Haiku 4.5 edition"
echo "  Est. cost: ~\$0.07–0.13 / day"
echo "═══════════════════════════════════════════════════════"
echo ""

# ─── Check API key ─────────────────────────────────────────
if [ -z "$ANTHROPIC_API_KEY" ]; then
  echo "⚠  ANTHROPIC_API_KEY is not set."
  echo ""
  echo "   To set it permanently, add this to ~/.zshrc:"
  echo "   export ANTHROPIC_API_KEY=sk-ant-..."
  echo ""
  read -p "   Enter your API key now: " key
  if [ -n "$key" ]; then
    export ANTHROPIC_API_KEY="$key"
    echo "   Key set for this session."
    echo ""
    echo "   To make it permanent:"
    echo "   echo 'export ANTHROPIC_API_KEY=$key' >> ~/.zshrc"
  else
    echo "❌  No key provided. Exiting."
    exit 1
  fi
else
  echo "✅  API key found"
fi

# ─── Check Python ──────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
  echo "❌  python3 not found."
  echo "   Install: brew install python"
  exit 1
fi
PY_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "✅  Python $PY_VER"

# ─── Virtual environment ───────────────────────────────────
if [ ! -d ".venv" ]; then
  echo ""
  echo "📦  Creating virtual environment..."
  python3 -m venv .venv
fi
source .venv/bin/activate
echo "✅  Virtual environment active"

# ─── Install dependencies ──────────────────────────────────
echo "📦  Installing dependencies..."
python3 -m pip install -q -r code/requirements.txt
echo "✅  Dependencies ready"

# ─── Create directories ────────────────────────────────────
mkdir -p data

# ─── Open browser ──────────────────────────────────────────
(sleep 3 && open http://localhost:8000 2>/dev/null || true) &

# ─── Run ───────────────────────────────────────────────────
echo ""
echo "🚀  Starting at http://localhost:8000"
echo "    Model:       claude-haiku-4-5-20251001"
echo "    Daily run:   09:00 AM (auto-generates brief + plays)"
echo "    Press Ctrl+C to stop"
echo ""
cd code && python3 main.py
