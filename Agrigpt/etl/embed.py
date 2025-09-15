# etl/embed.py
from __future__ import annotations
from pathlib import Path
import os, json
import numpy as np
import pandas as pd
import torch
from sentence_transformers import SentenceTransformer

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = Path(os.getenv("PROCESSED_DIR", ROOT / "data" / "processed"))
REPORTS = Path(os.getenv("REPORTS_DIR", ROOT / "reports"))
REPORTS.mkdir(parents=True, exist_ok=True)

MODEL_NAME = "intfloat/multilingual-e5-large"
BATCH = 64  # adjust if you have low VRAM

def load_chunks():
    df = pd.read_parquet(PROCESSED / "chunks.parquet")
    df = df[df["text"].astype(str).str.len() > 0].reset_index(drop=True)
    return df

def embed_passages(model: SentenceTransformer, texts):
    # E5 requires "passage:" prefix for docs
    prefixed = [f"passage: {t}" for t in texts]
    emb = model.encode(
        prefixed,
        batch_size=BATCH,
        normalize_embeddings=True,   # cosine-ready, L2-normalized
        convert_to_numpy=True,
        show_progress_bar=True,
    )
    return emb.astype(np.float32)

def main():
    print("Loading chunks …")
    df = load_chunks()
    print("Rows:", len(df))

    print("Loading model:", MODEL_NAME)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = SentenceTransformer(MODEL_NAME, device=device)

    print("Encoding …")
    embs = embed_passages(model, df["text"].tolist())

    # Parquet with embeddings (lists) — easy to inspect/debug
    df_out = df.copy()
    df_out["embedding"] = [e.tolist() for e in embs]
    out_parquet = PROCESSED / "chunks_with_emb.parquet"
    df_out.to_parquet(out_parquet, index=False)
    print("Wrote", out_parquet)

    # NPY + aligned IDs — handy for DB bulk load
    npy_path = PROCESSED / "chunk_embeds.npy"
    ids_path = PROCESSED / "chunk_ids.txt"
    np.save(npy_path, embs)
    ids_path.write_text("\n".join(df["chunk_id"].tolist()), encoding="utf-8")
    print("Wrote", npy_path, "and", ids_path)

    # Simple embedding report
    norms = np.linalg.norm(embs, axis=1)
    rep = {
        "model": MODEL_NAME,
        "rows": int(len(embs)),
        "dim": int(embs.shape[1]),
        "norm_mean": float(norms.mean()),
        "norm_std": float(norms.std()),
        "device": device,
    }
    (REPORTS / "embed_report.json").write_text(json.dumps(rep, indent=2), encoding="utf-8")
    print("Report:", rep)

if __name__ == "__main__":
    main()
