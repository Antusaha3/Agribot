
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

