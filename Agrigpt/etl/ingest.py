# etl/ingest.py
from __future__ import annotations
import hashlib, json, os, re, sys, mimetypes, shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# --------------------------------------------------------------------------------------
# Project-relative paths
# --------------------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]

# Where the pipeline expects “raw” copies (destination)
RAW_DIR = ROOT / "data" / "raw"              # => .../Agrigpt/data/raw/<source>/

# Where your ORIGINAL CSVs live (source)
LOCAL_CSV_DIR_DEFAULT = ROOT / "data_csv" / "raw"   # => .../Agrigpt/data_csv/raw/<source>/

# --------------------------------------------------------------------------------------
# Filename patterns per source (match your real file names)
# --------------------------------------------------------------------------------------
LOCAL_SOURCE_MAP = {
    "bangladesh_agri": ["*Bangladesh*Agricultural*Raw*Data*.csv", "*Bangladesh*Raw*Data*.csv"],
    "spas_bd":         ["SPAS-Dataset-BD*.csv"],
    "uddipok":         ["RC_Dataset_v2*.csv"],
}

TODAY = datetime.utcnow().date().isoformat()

SOURCE_META: Dict[str, Dict[str, str]] = {
    "bangladesh_agri": {
        "title": "Bangladesh Agricultural Dataset (104 crops)",
        "format": "csv", "language": "bn|en",
        "license": "CC BY (verify)", "notes": "Transplant/harvest; temperature",
        "accessed_at": TODAY,
    },
    "spas_bd": {
        "title": "SPAS-Dataset-BD (4191 rec; 73 crops)",
        "format": "csv", "language": "en",
        "license": "CC BY (verify)", "notes": "Agronomic & climate",
        "accessed_at": TODAY,
    },
    "uddipok": {
        "title": "UDDIPOK Bangla RC QA",
        "format": "csv", "language": "bn",
        "license": "Per paper", "notes": "Eval set",
        "accessed_at": TODAY,
    },
}

def ensure_dir(p: Path): p.mkdir(parents=True, exist_ok=True)
def now_iso() -> str: return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()

def guess_mime(path: Path) -> str:
    mime, _ = mimetypes.guess_type(str(path))
    return mime or "application/octet-stream"

def write_metadata_line(meta_path: Path, record: Dict):
    with meta_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

def normalize_name(name: str) -> str:
    name = name.strip()
    name = re.sub(r"\s+", "_", name)
    name = name.replace("__", "_")
    return name

def import_from_local_csv(csv_dir: Path, move: bool = False, debug: bool = False) -> List[Tuple[str, Path, Path]]:
    actions: List[Tuple[str, Path, Path]] = []
    for source, patterns in LOCAL_SOURCE_MAP.items():
        src_base = csv_dir / source
        out_dir  = RAW_DIR / source
        ensure_dir(out_dir)

        if debug:
            print(f"[DEBUG] Looking in: {src_base} for patterns: {patterns}")

        for pat in patterns:
            for src in (src_base.glob(pat) if src_base.exists() else []):
                dst = out_dir / normalize_name(src.name)
                if move:
                    shutil.move(str(src), dst)
                    action = "moved"
                else:
                    shutil.copy2(src, dst)
                    action = "copied"
                print(f"{action}: {src} -> {dst}")
                actions.append((source, src, dst))

    return actions

def register_existing_files_for_source(source: str, debug: bool = False) -> int:
    src_dir = RAW_DIR / source
    if not src_dir.exists():
        if debug:
            print(f"[DEBUG] RAW_DIR for {source} not found: {src_dir}")
        return 0

    meta_path = src_dir / "metadata.jsonl"
    meta = SOURCE_META.get(source, {})
    count = 0
    for path in sorted(src_dir.glob("*")):
        if not path.is_file() or path.name == "metadata.jsonl":
            continue
        record = {
            "source": source,
            "title":  meta.get("title", source),
            "format": meta.get("format", "csv"),
            "language": meta.get("language", "bn"),
            "license":  meta.get("license", "unspecified"),
            "notes":    meta.get("notes", ""),
            "fetched_at": meta.get("accessed_at", TODAY) or now_iso(),
            "file_name": path.name,
            "file_path": str(path.relative_to(RAW_DIR)),
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
            "mime": guess_mime(path),
            "url": None,
            "lineage": ["manual_copy"],
        }
        write_metadata_line(meta_path, record)
        count += 1
    return count

def main(import_from_csv: bool = False, csv_dir: Optional[str] = None, move: bool = False,
         only_source: Optional[str] = None, debug: bool = False):
    if debug:
        print(f"[DEBUG] ROOT={ROOT}")
        print(f"[DEBUG] LOCAL_CSV_DIR_DEFAULT={LOCAL_CSV_DIR_DEFAULT}")
        print(f"[DEBUG] RAW_DIR={RAW_DIR}")

    # Step 1: copy/move from data_csv/raw → data/raw
    if import_from_csv:
        base = Path(csv_dir) if csv_dir else LOCAL_CSV_DIR_DEFAULT
        if not base.exists():
            print(f"[ERROR] Local CSV directory not found: {base}")
            sys.exit(1)
        import_from_local_csv(base, move=move, debug=debug)

    # Step 2: register files under data/raw/<source>
    sources = [only_source] if only_source else list(LOCAL_SOURCE_MAP.keys())
    total = 0
    for s in sources:
        ensure_dir(RAW_DIR / s)
        n = register_existing_files_for_source(s, debug=debug)
        print(f"✔ {s}: {n} file(s) registered")
        total += n

    if total == 0:
        print("No files registered. Make sure your files are under data/raw/<source>/ or use --import-from-csv.")

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Register local datasets under data/raw/<source>/.")
    p.add_argument("--import-from-csv", action="store_true", help="Copy/move from data_csv/raw/ into data/raw/")
    p.add_argument("--csv-dir", type=str, default=None, help="Override local CSV dir (default: data_csv/raw/)")
    p.add_argument("--move", action="store_true", help="Move files instead of copy")
    p.add_argument("--only-source", type=str, default=None, choices=list(LOCAL_SOURCE_MAP.keys()))
    p.add_argument("--debug", action="store_true")
    args = p.parse_args()
    main(args.import_from_csv, args.csv_dir, args.move, args.only_source, args.debug)
