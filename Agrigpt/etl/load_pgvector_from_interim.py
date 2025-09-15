# etl/load_pgvector_from_interim.py
from __future__ import annotations
import os, json
from pathlib import Path
import psycopg
from pgvector.psycopg import register_vector
from sentence_transformers import SentenceTransformer

PG = dict(
    host=os.getenv("PGHOST","localhost"),
    port=os.getenv("PGPORT","5432"),
    dbname=os.getenv("PGDATABASE","agrigpt"),
    user=os.getenv("PGUSER","postgres"),
    password=os.getenv("PGPASSWORD","12345"),
)

ROOT = Path(__file__).resolve().parents[1]
INTERIM = Path(r"D:\project=2\Agrigpt\notebook\data\interim")  # <-- your folder

SOURCES = [
    ("doc:bangladesh_agri",  "Bangladesh Agricultural Dataset", "bangladesh_agri_clean.txt", "bangladesh_agri_clean.jsonl"),
    ("doc:spas_bd",          "SPAS Dataset BD",                 "spas_bd_clean.txt",         "spas_bd_clean.jsonl"),
    # add more if needed
]

MODEL = "intfloat/multilingual-e5-large"

def rows_from_pair(txt_path: Path, meta_path: Path):
    texts = txt_path.read_text(encoding="utf-8").splitlines()
    metas = [json.loads(l) for l in meta_path.read_text(encoding="utf-8").splitlines()]
    assert len(texts) == len(metas), f"line mismatch: {txt_path.name} vs {meta_path.name}"
    for i, (t, m) in enumerate(zip(texts, metas)):
        yield i, t, m

def main():
    print("Connecting to Postgres:", {k: PG[k] for k in PG if k != "password"})
    model = SentenceTransformer(MODEL)

    with psycopg.connect(**PG) as conn, conn.cursor() as cur:
        register_vector(conn)

        for doc_id, title, txt_name, meta_name in SOURCES:
            txtp, metap = INTERIM / txt_name, INTERIM / meta_name
            if not txtp.exists() or not metap.exists():
                print(f"skip (missing): {txtp} / {metap}")
                continue

            # for progress stats
            cur.execute("SELECT COUNT(*) FROM documents")
            before_docs = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM chunks")
            before_chunks = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM embeddings")
            before_embs = cur.fetchone()[0]

            # upsert document
            cur.execute("""
                INSERT INTO documents (doc_id, source, title, language)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (doc_id) DO NOTHING
            """, (doc_id, "interim", title, "bn"))

            batch_size = 64
            batch_texts, batch_ids = [], []

            rows = 0
            for i, text, meta in rows_from_pair(txtp, metap):
                text = (text or "").strip()
                if not text:
                    continue
                chunk_id = f"{doc_id}:{i}"
                lang     = meta.get("language") or "bn"
                section  = (meta.get("section_path") or "").strip()

                cur.execute("""
                    INSERT INTO chunks (chunk_id, doc_id, source, language, section_path, token_count, text)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (chunk_id) DO NOTHING
                """, (chunk_id, doc_id, "interim", lang, section, None, text))

                batch_texts.append("passage: " + text)
                batch_ids.append(chunk_id)
                rows += 1

                if len(batch_texts) >= batch_size:
                    embs = model.encode(batch_texts, normalize_embeddings=True)
                    for cid, emb in zip(batch_ids, embs):
                        cur.execute("""
                            INSERT INTO embeddings (chunk_id, embed)
                            VALUES (%s,%s)
                            ON CONFLICT (chunk_id) DO UPDATE SET embed = EXCLUDED.embed
                        """, (cid, emb.tolist()))
                    batch_texts.clear(); batch_ids.clear()

            # flush tail
            if batch_texts:
                embs = model.encode(batch_texts, normalize_embeddings=True)
                for cid, emb in zip(batch_ids, embs):
                    cur.execute("""
                        INSERT INTO embeddings (chunk_id, embed)
                        VALUES (%s,%s)
                        ON CONFLICT (chunk_id) DO UPDATE SET embed = EXCLUDED.embed
                    """, (cid, emb.tolist()))
                batch_texts.clear(); batch_ids.clear()

            # show deltas
            cur.execute("SELECT COUNT(*) FROM documents"); after_docs = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM chunks");    after_chunks = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM embeddings");after_embs = cur.fetchone()[0]
            print(f"✓ loaded {doc_id}: +docs={after_docs-before_docs}, +chunks={after_chunks-before_chunks}, +embeddings={after_embs-before_embs} (rows read={rows})")

        # vacuum/analyze helps planner
        cur.execute("ANALYZE embeddings;")
        cur.execute("ANALYZE chunks;")

if __name__ == "__main__":
    main()
