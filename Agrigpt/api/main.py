# api/main.py
from fastapi import FastAPI, HTTPException
from api.schemas import AskRequest, AskResponse, IngestRequest
from rag.router import answer
import logging

app = FastAPI(title="AgriGPT API", version="1.0")

# ---- Health checks ----
@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.get("/readyz")
def readyz():
    return {"status": "ready"}

# ---- Main Ask endpoint ----
@app.post("/api/ask", response_model=AskResponse)
def ask(req: AskRequest):
    try:
        out = answer(req.question, k=5)
        return AskResponse(answer=out, mode="GraphRAG or VectorRAG", sources=["neo4j","pgvector"])
    except Exception as e:
        logging.exception("Error in /api/ask")
        raise HTTPException(status_code=500, detail=str(e))

# ---- Ingest endpoint (admin only stub) ----
@app.post("/api/ingest")
def ingest(req: IngestRequest):
    # TODO: hook into etl/clean.py + etl/chunk.py + etl/embed.py
    return {"status": "ok", "urls": req.urls, "files": req.files}
