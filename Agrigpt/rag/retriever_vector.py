# rag/retriever_vector.py
from __future__ import annotations
import os
from typing import List, Optional, Dict, Any
import numpy as np

# Optional: if Postgres/pgvector not up, we want graceful fallback
try:
    import psycopg
    from psycopg.rows import dict_row
    from pgvector.psycopg import register_vector
    HAS_DB = True
except Exception:
    HAS_DB = False

from sentence_transformers import SentenceTransformer

MODEL_NAME = "intfloat/multilingual-e5-large"

def _conn():
    if not HAS_DB:
        return None
    try:
        c = psycopg.connect(
            host=os.getenv("PGHOST","localhost"),
            port=os.getenv("PGPORT","5432"),
            dbname=os.getenv("PGDATABASE","agrigpt"),
            user=os.getenv("PGUSER","postgres"),
            password=os.getenv("PGPASSWORD","12345"),
            row_factory=dict_row,
        )
        register_vector(c)
        return c
    except Exception:
        return None

_model = None
def _model_get():
    global _model
    if _model is None:
        _model = SentenceTransformer(MODEL_NAME)
    return _model

def embed_query(q: str) -> np.ndarray:
    v = _model_get().encode([f"query: {q}"], normalize_embeddings=True)[0]
    return v.astype(np.float32)

def fetch_knn(qvec: np.ndarray, fetch_k: int = 32,
              source: Optional[str] = None, language: Optional[str] = None) -> List[Dict[str,Any]]:
    conn = _conn()
    if conn is None:
        return []  # force graceful “no passages” -> sorry message

    where, params = [], {"qvec": qvec.tolist(), "k": fetch_k}
    if source:   where.append("c.source = %(source)s");   params["source"] = source
    if language: where.append("c.language = %(language)s"); params["language"] = language
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
      SELECT c.chunk_id, c.doc_id, c.source, c.language, c.section_path, c.text,
             1 - (e.embed <=> %(qvec)s::vector) AS cosine_sim
      FROM embeddings e
      JOIN chunks c ON c.chunk_id = e.chunk_id
      {where_sql}
      ORDER BY e.embed <=> %(qvec)s::vector
      LIMIT %(k)s;
    """
    with conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())

def mmr_rerank(qvec: np.ndarray, hits: List[Dict[str,Any]],
               lambda_mult: float = 0.7, top_k: int = 5) -> List[Dict[str,Any]]:
    if len(hits) <= top_k: return hits
    toks = [set(h["text"].split()) for h in hits]
    selected, selected_idx = [], []
    idx = int(np.argmax([h["cosine_sim"] for h in hits]))
    selected.append(hits[idx]); selected_idx.append(idx)
    def jaccard(a,b):
        inter = len(a & b); uni = len(a | b) or 1
        return inter/uni
    while len(selected) < top_k and len(selected) < len(hits):
        best_j, best_score = None, -1e9
        for j, h in enumerate(hits):
            if j in selected_idx: continue
            rel = h["cosine_sim"]
            red = max((jaccard(toks[j], toks[si]) for si in selected_idx), default=0.0)
            score = lambda_mult*rel - (1-lambda_mult)*red
            if score > best_score:
                best_score, best_j = score, j
        selected.append(hits[best_j]); selected_idx.append(best_j)
    return selected

def search(query: str, k: int = 5, fetch_k: int = 32,
           source: Optional[str] = None, language: Optional[str] = "bn",
           use_mmr: bool = True, lambda_mult: float = 0.7) -> List[Dict[str,Any]]:
    qvec = embed_query(query)
    raw = fetch_knn(qvec, fetch_k=fetch_k, source=source, language=language)
    return (mmr_rerank(qvec, raw, lambda_mult=lambda_mult, top_k=k) if use_mmr else raw[:k])
