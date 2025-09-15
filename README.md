# KrishiBot — Phase 0 (Project Setup)

This scaffold covers **Phase 0** only (no Git instructions). It sets up a reproducible workspace, configuration, environment variables, and governance templates.

## What’s included
- `config/project.yaml` — central configuration (paths, chunking, embeddings, DB URIs)
- `config/.env.example` — env var template
- `config/sources.csv` — register data sources
- `docs/datacards/` — data card templates (Markdown + JSON schema)
- `docs/charter.md` — project charter with KPIs (accuracy, faithfulness, latency)
- `src/krishibot/config.py` — Pydantic-based loader that merges YAML + .env
- `scripts/setup_env.sh` and `scripts/setup_env.ps1` — convenience scripts to create a Python 3.11 venv and install deps
- `pyproject.toml` (Poetry) and `requirements.txt` (pip) — choose one workflow
- `Makefile` and `tasks.py` — optional helpers (uniform commands across OSes)

## Quick start (pip, Python 3.11)
```bash
# 1) Create and activate a venv
python -m venv .venv
# Linux/macOS
source .venv/bin/activate
# Windows (PowerShell)
.\.venv\Scripts\Activate

# 2) Install deps
pip install -r requirements.txt

# 3) Copy env template and edit
cp config/.env.example .env

# 4) Validate config load
python -m krishibot.config --print
```

## Quick start (Poetry, Python 3.11)
```bash
poetry env use 3.11
poetry install
cp config/.env.example .env
poetry run python -m krishibot.config --print
```

> All paths and parameters are centralized in `config/project.yaml`. Environment-specific secrets live in `.env`.

