# rag/router.py
from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()

from rag import prompts
from rag.retriever_vector import search

# Graph retrievers
from rag.retriever_graph import resolve_crop, graph_answer_for_crop
try:
    # optional: only present if you created disease logic
    from rag.retriever_graph import graph_diseases_for_crop
except Exception:
    graph_diseases_for_crop = None  # graceful if not implemented yet


def _format_vector_context(question: str, passages: list[dict]) -> str:
    if not passages:
        return ""
    lines = []
    for i, p in enumerate(passages, 1):
        lines.append(f"[{i}] {p.get('text','')}\n(Source: {p.get('source','')})")
    block = "\n\n".join(lines)
    return prompts.VEC_USER.format(question=question, passages=block)


def _detect_intent(q: str) -> str | None:
    ql = q.lower()
    DISEASE = ["রোগ", "পোকা", "কীট", "disease", "pest", "blight", "blast", "bacterial", "fungal"]
    CLIMATE = ["জলবায়ু", "তাপমাত্রা", "আর্দ্রতা", "climate", "temperature", "humidity"]
    SEASON  = ["মৌসুম", "রোপণ", "রোপন", "কাটা", "transplant", "harvest", "season"]
    FERT    = ["সার", "fertilizer", "npk", "ডোজ", "মাত্রা"]

    def has(xs): return any(x in ql for x in xs)
    if has(DISEASE): return "disease"
    if has(CLIMATE): return "climate"
    if has(SEASON):  return "season"
    if has(FERT):    return "fertilizer"
    return None


def _as_bullets(bn_lines: list[str]) -> str:
    return "\n".join(f"• {b}" for b in bn_lines if b and b.strip())


def answer(question: str, k: int = 5) -> str:
    """
    Graph-first:
      - If intent is 'disease' and graph has disease facts → return Bangla bullets.
      - Else try climate/season overview via graph_answer_for_crop.
    Fallback:
      - Vector RAG with intent-augmented query.
    """
    intent = _detect_intent(question)
    crop = resolve_crop(question)

    # ----- 1) Graph paths
    if crop:
        # (a) Diseases for crop (only if function is available)
        if intent == "disease" and callable(graph_diseases_for_crop):
            res = graph_diseases_for_crop(question)
            if res.get("ok") and res.get("bullets_bn"):
                return _as_bullets(res["bullets_bn"])

        # (b) Climate/season overview for crop
        res = graph_answer_for_crop(question)
        if res.get("ok") and res.get("bullets_bn"):
            return _as_bullets(res["bullets_bn"])

    # ----- 2) Vector fallback (LLM)
    aug = {
        "disease": " রোগ পোকা কীট disease pest",
        "climate": " জলবায়ু তাপমাত্রা আর্দ্রতা climate temperature humidity",
        "season":  " মৌসুম season transplant harvest কাটা রোপণ",
        "fertilizer": " সার fertilizer NPK ডোজ মাত্রা",
    }.get(intent or "", "")
    q2 = f"{question} {aug}".strip()

    hits = search(q2, k=k, fetch_k=max(32, k * 8), language="bn")
    passages = [{"id":h.get("chunk_id",""),
                 "text":h.get("text",""),
                 "source":h.get("source","")} for h in hits]

    if not passages:
        return "মাফ করবেন, আমার কাছে সেই তথ্য নেই।"

    user_prompt = _format_vector_context(question, passages)
    # Using the LLM through generator; keeps answers short, BN, and grounded in passages
    from rag.generator import gen_from_passages
    return gen_from_passages(question, user_prompt, prompts.VEC_SYS)
