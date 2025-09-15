from __future__ import annotations
import os, json
from pathlib import Path
import numpy as np
import pandas as pd
import psycopg
from psycopg.rows import dict_row
from pgvector.psycopg import register_vector

ROOT = Path(__file__).resolve().parents[1]
P = Path(os.getenv("PROCESSED_DIR", ROOT / "data" / "processed"))

def conn():
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

def upsert_documents(cur, df):
    docs = df[["doc_id","source","language"]].drop_duplicates().copy()
    docs["title"] = docs["source"].map({
        "bangladesh_agri":"Bangladesh Agricultural Dataset",
        "spas_bd":"SPAS-Dataset-BD"
    }).fillna(docs["source"])
    cur.execute("CREATE TEMP TABLE tmp_docs (doc_id text, source text, title text, language text) ON COMMIT DROP;")
    cur.copy("COPY tmp_docs (doc_id,source,title,language) FROM STDIN WITH (FORMAT CSV)",
             docs.to_csv(index=False, header=False))
    cur.execute("""
      INSERT INTO documents (doc_id,source,title,language)
      SELECT DISTINCT doc_id,source,title,language FROM tmp_docs
      ON CONFLICT (doc_id) DO NOTHING;
    """)

def upsert_chunks(cur, df: pd.DataFrame):
    import csv

    # Sanitize: COPY ... FORMAT CSV does not like raw newlines/tabs inside fields
    tocopy = df[[
        "chunk_id","doc_id","source","language","section_path","token_count","created_at","text"
    ]].copy()

    # Replace newlines and tabs in text/section_path with spaces (simplest & safest)
    for col in ["text", "section_path"]:
        tocopy[col] = (
            tocopy[col]
            .astype(str)
            .str.replace(r"\r\n|\r|\n", " ", regex=True)
            .str.replace("\t", " ", regex=False)
        )

    cur.execute("""
        CREATE TEMP TABLE tmp_chunks (
          chunk_id text, doc_id text, source text, language text,
          section_path text, token_count int, created_at timestamptz, text text
        ) ON COMMIT DROP;
    """)

    # Export as TSV with NO quoting; use escapechar to satisfy the csv writer
    payload = tocopy.to_csv(
        index=False, header=False,
        sep="\t",
        quoting=csv.QUOTE_NONE,   # no quoting
        escapechar="\\",          # <-- required with QUOTE_NONE
    )

    # Tell Postgres about the delimiter and escape char
    cur.copy(
        "COPY tmp_chunks (chunk_id,doc_id,source,language,section_path,token_count,created_at,text) "
        "FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', ESCAPE '\\')",
        payload
    )

    cur.execute("""
        INSERT INTO chunks (chunk_id, doc_id, source, language, section_path, token_count, created_at, text)
        SELECT chunk_id, doc_id, source, language, section_path, token_count, created_at, text
        FROM tmp_chunks
        ON CONFLICT (chunk_id) DO UPDATE SET
          section_path = EXCLUDED.section_path,
          token_count  = EXCLUDED.token_count,
          text         = EXCLUDED.text;
    """)

def upsert_embeddings(cur, df, embs):
    ids = df["chunk_id"].tolist()
    cur.execute("CREATE TEMP TABLE tmp_emb (chunk_id text, embed vector(1024)) ON COMMIT DROP;")
    payload = "\n".join(f"{cid}\t[{', '.join(f'{x:.7f}' for x in vec)}]" for cid, vec in zip(ids, embs))
    cur.copy("COPY tmp_emb (chunk_id, embed) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE E'\\b')", payload)
    cur.execute("""
      INSERT INTO embeddings (chunk_id, embed)
      SELECT chunk_id, embed FROM tmp_emb
      ON CONFLICT (chunk_id) DO UPDATE SET embed = EXCLUDED.embed;
    """)

def main():
    df = pd.read_parquet(P / "chunks.parquet")
    embs = np.load(P / "chunk_embeds.npy")
    assert len(df) == len(embs), "chunk rows != embedding rows"

    with conn() as c, c.cursor() as cur:
        upsert_documents(cur, df)
        upsert_chunks(cur, df)
        upsert_embeddings(cur, df, embs)
        c.commit()
    print("✓ Loaded into PostgreSQL/pgvector.")

if __name__ == "__main__":
    main()
