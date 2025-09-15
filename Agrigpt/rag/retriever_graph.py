# rag/retriever_graph.py
from __future__ import annotations
from typing import List, Dict, Any, Optional
from neo4j import GraphDatabase
import os, unicodedata, re, json, argparse, atexit

# ---- Neo4j connection (envs or defaults) ----
NEO4J_URI  = os.getenv("NEO4J_URI", "neo4j://127.0.0.1:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASSWORD", "12345agri")
_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
atexit.register(lambda: _driver and _driver.close())

def _run(cypher: str, **params):
    with _driver.session() as s:
        return [dict(r) for r in s.run(cypher, **params)]

# ---- optional manual alias hints (used only as last fallback) ----
_MANUAL_ALIASES = {
    "আমন": ["Aman"],
    "আউশ": ["Aus Rice"],
    "বোরো": ["Boro"],
}

# ---- input normalization for Bangla text ----
def _normalize_bn(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFC", s)
    s = re.sub(r"[\u200B-\u200D\u2060\uFEFF]", "", s)  # remove zero-width chars
    return s.strip()

# ---- pretty BN formatting helpers ----
_BN_DIGITS = str.maketrans("0123456789", "০১২৩৪৫৬৭৮৯")
def _bn_num(x) -> str:
    if x is None: return ""
    s = f"{x}"
    if isinstance(x, float):
        s = s.rstrip("0").rstrip(".")
    return s.translate(_BN_DIGITS)

_SEASON_MAP = {
    "Kharif 1": "খরিফ-১",
    "Kharif1": "খরিফ-১",
    "Kharif 2": "খরিফ-২",
    "Kharif2": "খরিফ-২",
    "Rabi": "রবি",
    "Aus": "আউশ",
    "Aman": "আমন",
    "Boro": "বোরো",
}
def _bn_season(s: str) -> str:
    if not s: return ""
    return _SEASON_MAP.get(s, s)

# ---------------- Crop resolution ----------------
def resolve_crop(q: str):
    q = _normalize_bn(q)

    cy_exact = """
    MATCH (c:Crop)
    WHERE toLower(coalesce(c.id,''))      = toLower($q)
       OR toLower(coalesce(c.name_bn,'')) = toLower($q)
       OR toLower(coalesce(c.name_en,'')) = toLower($q)
       OR (c.slug IS NOT NULL AND toLower(coalesce(c.slug,'')) = toLower($q))
    RETURN c.id AS id, c.name_bn AS name_bn, c.name_en AS name_en, c.slug AS slug
    LIMIT 1
    """
    rows = _run(cy_exact, q=q)
    if rows: return rows[0]

    cy_alias = """
    MATCH (a:Alias) WHERE toLower(coalesce(a.name,'')) = toLower($q)
    MATCH (a)-[:ALIAS_OF]->(c:Crop)
    RETURN c.id AS id, c.name_bn AS name_bn, c.name_en AS name_en, c.slug AS slug
    LIMIT 1
    """
    try:
        rows = _run(cy_alias, q=q)
        if rows: return rows[0]
    except Exception:
        pass

    cy_ft = """
    CALL db.index.fulltext.queryNodes('cropFulltext', $q) YIELD node, score
    RETURN node.id AS id, node.name_bn AS name_bn, node.name_en AS name_en, node.slug AS slug
    ORDER BY score DESC
    LIMIT 1
    """
    rows = _run(cy_ft, q=q)
    if rows: return rows[0]

    cy_contains = """
    MATCH (c:Crop)
    WHERE toLower(coalesce(c.name_bn,'')) CONTAINS toLower($q)
       OR toLower(coalesce(c.name_en,'')) CONTAINS toLower($q)
       OR (c.slug IS NOT NULL AND toLower(coalesce(c.slug,'')) CONTAINS toLower($q))
    RETURN c.id AS id, c.name_bn AS name_bn, c.name_en AS name_en, c.slug AS slug
    LIMIT 1
    """
    rows = _run(cy_contains, q=q)
    if rows: return rows[0]

    for key, targets in _MANUAL_ALIASES.items():
        if _normalize_bn(key) in q:
            hit = _run(cy_contains, q=targets[0])
            if hit: return hit[0]

    return None

# ---------------- Query helpers ----------------
def crop_seasons(crop_id: str) -> List[Dict[str, Any]]:
    cy = """
    MATCH (c:Crop {id:$id})-[r:SUITABLE_IN]->(s:Season)
    RETURN s.id AS season_id, s.name_bn AS season_name
    ORDER BY s.name_bn
    """
    return _run(cy, id=crop_id)

def crop_locations(crop_id: str, limit: int = 10) -> List[Dict[str, Any]]:
    cy = """
    MATCH (c:Crop {id:$id})-[r:CULTIVATED_IN]->(l:Location)
    RETURN l.id AS location_id, l.name_bn AS location_name,
           r.season AS season, r.transplant AS transplant,
           r.harvest AS harvest, r.production AS production,
           r.avg_temp_c AS avg_temp_c, r.avg_humidity AS avg_humidity
    ORDER BY coalesce(r.production, -1) DESC
    LIMIT $limit
    """
    return _run(cy, id=crop_id, limit=limit)

def crop_climate(crop_id: str) -> Optional[Dict[str, Any]]:
    cy = """
    MATCH (c:Crop {id:$id})
    RETURN c.min_temp_c AS min_temp_c, c.max_temp_c AS max_temp_c,
           c.min_rh AS min_rh,     c.max_rh AS max_rh
    """
    rows = _run(cy, id=crop_id)
    return rows[0] if rows else None

# ---- OPTIONAL: diseases (only used if you have these edges in graph) ----
def crop_diseases(crop_id: str) -> List[Dict[str, Any]]:
    cy = """
    MATCH (c:Crop {id:$id})-[:SUFFER_FROM]->(d:Disease)
    RETURN d.id AS disease_id, d.name_bn AS name_bn, d.name_en AS name_en, d.notes AS notes
    ORDER BY d.name_bn
    """
    try:
        return _run(cy, id=crop_id)
    except Exception:
        return []

# ---------------- Public: GraphRAG answer ----------------
def graph_answer_for_crop(text: str) -> Dict[str, Any]:
    crop = resolve_crop(text)
    if not crop:
        return {"ok": False, "reason": "crop_not_found", "query": text}

    seasons  = crop_seasons(crop["id"])
    locs     = crop_locations(crop["id"], limit=10)
    climate  = crop_climate(crop["id"])
    diseases = crop_diseases(crop["id"])   # may be empty if you haven't loaded them

    bullets: List[str] = []

    if seasons:
        names = [_bn_season(s.get("season_name","")) for s in seasons if s.get("season_name")]
        if names:
            bullets.append("উপযোগী মৌসুম: " + " / ".join(names))

    if climate and (climate.get("min_temp_c") is not None or climate.get("max_temp_c") is not None):
        lo = _bn_num(climate.get("min_temp_c")) if climate.get("min_temp_c") is not None else "—"
        hi = _bn_num(climate.get("max_temp_c")) if climate.get("max_temp_c") is not None else "—"
        bullets.append(f"তাপমাত্রা: {lo}–{hi}°C")

    if climate and (climate.get("min_rh") is not None or climate.get("max_rh") is not None):
        loh = _bn_num(climate.get("min_rh")) if climate.get("min_rh") is not None else "—"
        hih = _bn_num(climate.get("max_rh")) if climate.get("max_rh") is not None else "—"
        bullets.append(f"আপেক্ষিক আর্দ্রতা: {loh}%–{hih}%")

    if diseases:
        bullets.append("রোগ: " + " / ".join(d.get("name_bn") or d.get("name_en","") for d in diseases if (d.get("name_bn") or d.get("name_en"))))

    if locs:
        top = ", ".join(
            f"{r.get('location_name','')} ({_bn_season(r.get('season',''))})".strip()
            for r in locs[:5] if r.get("location_name")
        )
        if top.strip(", "):
            bullets.append(f"চাষের জেলা (উদাহরণ): {top}")

    return {
        "ok": True,
        "mode": "GraphRAG",
        "crop": crop,
        "facts": {
            "seasons": seasons,
            "locations": locs,
            "climate": climate,
            "diseases": diseases,   # may be []
        },
        "bullets_bn": bullets,
        "sources": ["neo4j:Crop/Season/Location/Disease"],
    }

__all__ = ["resolve_crop", "graph_answer_for_crop"]

# ---------- tiny CLI for quick testing ----------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--q", required=True, help="query text (Bangla/English)")
    args = ap.parse_args()
    out = graph_answer_for_crop(args.q)
    print(json.dumps(out, ensure_ascii=False, indent=2))
