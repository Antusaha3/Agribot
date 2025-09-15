# etl/chunk.py
from __future__ import annotations
import json, hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
import pandas as pd

# -----------------------
# Paths (configurable)
# -----------------------
ROOT = Path(__file__).resolve().parents[1]

# Prefer environment variables if you need absolute paths (e.g. on Windows)
#   set INTERIM_DIR=D:\project=2\Agrigpt\notebook\data\interim
#   set PROCESSED_DIR=D:\project=2\Agrigpt\data\processed
#   set REPORTS_DIR=D:\project=2\Agrigpt\reports
import os
INTERIM = Path(os.getenv("INTERIM_DIR", ROOT / "D:/project=2/Agrigpt/notebook/data/interim"))
PROCESSED = Path(os.getenv("PROCESSED_DIR", ROOT / "data" / "processed"))
REPORTS = Path(os.getenv("REPORTS_DIR", ROOT / "reports"))
PROCESSED.mkdir(parents=True, exist_ok=True)
REPORTS.mkdir(parents=True, exist_ok=True)

STEMS = ["bangladesh_agri", "spas_bd"]  # which cleaned sources to include

def tok_count(s: str) -> int:
    return len((s or "").split())

def sha1_12(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]

def _read_clean_pair(stem: str) -> List[Tuple[str, Dict]]:
    txt_path = INTERIM / f"{stem}_clean.txt"
    meta_path = INTERIM / f"{stem}_clean.jsonl"
    if not txt_path.exists() or not meta_path.exists():
        raise FileNotFoundError(f"Missing clean pair for {stem}: {txt_path} / {meta_path}")
    txt_lines = [l.rstrip("\n") for l in txt_path.read_text(encoding="utf-8").splitlines()]
    meta_lines = [json.loads(l) for l in meta_path.read_text(encoding="utf-8").splitlines()]
    if len(txt_lines) != len(meta_lines):
        raise ValueError(f"{stem}: text/meta line mismatch ({len(txt_lines)} vs {len(meta_lines)})")
    return list(zip(txt_lines, meta_lines))

def make_chunks(lines: List[str], metas: List[Dict], max_tokens: int = 600, overlap: int = 120):
    """Sliding window over lines to build chunks ≤ max_tokens; step back ~overlap tokens between windows."""
    chunks = []
    token_lens = [tok_count(x) for x in lines]
    n = len(lines)
    i = 0
    while i < n:
        cur, cur_meta, cur_tok = [], [], 0
        j = i
        while j < n and cur_tok + token_lens[j] <= max_tokens:
            cur.append(lines[j])
            cur_meta.append(metas[j])
            cur_tok += token_lens[j]
            j += 1
        if not cur:  # single very long line
            cur = [lines[i]]
            cur_meta = [metas[i]]
            j = i + 1

        # Representative metadata from first line; merge section_path of window for traceability
        m0 = cur_meta[0]
        section_path = " | ".join(dict.fromkeys([m.get("section_path", "") for m in cur_meta if m.get("section_path")]))

        chunk = {
            "chunk_id": f"chunk_{sha1_12(m0['doc_id'] + '|' + str(i))}",
            "doc_id": m0["doc_id"],
            "source": m0["source"],
            "language": m0.get("language", "bn"),
            "section_path": section_path,
            "created_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "text": "\n".join(cur).strip(),
        }
        chunk["token_count"] = tok_count(chunk["text"])
        # keep light trace from first line
        fields = m0.get("fields", {})
        chunk["fields"] = json.dumps(fields, ensure_ascii=False)
        chunks.append(chunk)

        # advance pointer with ~overlap backstep
        if j >= n:
            break
        back_tokens, back_lines = 0, 0
        k = len(cur) - 1
        while k >= 0 and back_tokens < overlap:
            back_tokens += token_lens[i + k]
            back_lines += 1
            k -= 1
        i = max(i + len(cur) - back_lines, i + 1)  # avoid infinite loop
    return chunks

def main(max_tokens: int = 600, overlap: int = 120):
    all_chunks = []
    stats_rows = []
    for stem in STEMS:
        pairs = _read_clean_pair(stem)
        lines, metas = zip(*pairs)
        chunks = make_chunks(list(lines), list(metas), max_tokens=max_tokens, overlap=overlap)
        all_chunks.extend(chunks)

        # per-source stats
        df_tmp = pd.DataFrame([c for c in chunks if c["source"] == stem])
        stats_rows.append({
            "source": stem,
            "num_chunks": len(df_tmp),
            "p50_tokens": df_tmp["token_count"].median() if not df_tmp.empty else 0,
            "p95_tokens": df_tmp["token_count"].quantile(0.95) if not df_tmp.empty else 0,
            "max_tokens": df_tmp["token_count"].max() if not df_tmp.empty else 0,
        })

    df = pd.DataFrame(all_chunks)
    out_parquet = PROCESSED / "chunks.parquet"
    df.to_parquet(out_parquet, index=False)
    print(f"Wrote {out_parquet} rows={len(df)}")

    # save chunk stats report
    rep = pd.DataFrame(stats_rows)
    rep_path = REPORTS / "chunk_stats.csv"
    rep.to_csv(rep_path, index=False)
    print("Wrote", rep_path)
    print(rep)

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--max-tokens", type=int, default=600)
    p.add_argument("--overlap", type=int, default=120)
    args = p.parse_args()
    main(max_tokens=args.max_tokens, overlap=args.overlap)
