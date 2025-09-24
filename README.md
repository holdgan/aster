
# Aster BTCUSDT Trading Bot

A dual-account trading bot for Aster BTCUSDT futures, designed for Genesis Stage 2.

## AI Assistant Configuration

This project includes configuration files for different AI assistants:

- **CLAUDE.md** - For Claude Code (claude.ai/code) - Architecture and refactoring focus
- **AI_GUIDE_UNIVERSAL.md** - Universal guide compatible with all AI models

All AI assistants are configured to follow Linus Torvalds' software engineering principles:
- Simplicity over complexity
- Clear data structures
- No special cases
- Backward compatibility

## Quick Start

```bash
# Setup
cp .env.example .env
# Edit .env with your API keys

# Install dependencies
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt

# Run bot
python aster_btcusdt_genesis.py --maker-account A --taker-account B

deactivate
```

## Architecture

**Simplified Design**: .env configuration → Direct API calls → Trading logic

No databases, no Redis, no complex abstractions. Just clean, maintainable code.
# aster
