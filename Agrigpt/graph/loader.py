# graph/loader.py
from __future__ import annotations
import os
import math
from pathlib import Path
import pandas as pd
from neo4j import GraphDatabase
from slugify import slugify

# --------- Paths & Config ----------
ROOT = Path(__file__).resolve().parents[1]
CSV_DIR = Path(os.getenv("GRAPH_CSV_DIR", ROOT / "graph" / "csv"))

URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")   # or neo4j://
USR = os.getenv("NEO4J_USER", "neo4j")
PWD = os.getenv("NEO4J_PASSWORD", "12345agri")


# --------- Helpers ----------
def _to_none(x):
    """None for NaN/empty/'nan'/'none'/'null', otherwise stripped string."""
    if x is None:
        return None
    if isinstance(x, float) and math.isnan(x):
        return None
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none", "null"):
        return None
    return s


def _read_csv(path: Path) -> pd.DataFrame:
    # keep everything as string so we can cleanly map to None
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    # Normalize obvious NA-likes to None
    for col in df.columns:
        df[col] = df[col].map(_to_none)
    return df


def get_driver():
    return GraphDatabase.driver(URI, auth=(USR, PWD))


# --------- Schema (constraints & indexes) ----------
def create_indexes(tx):
    # Uniqueness by id only (do NOT make slug unique)
    tx.run("CREATE CONSTRAINT crop_id_unique IF NOT EXISTS FOR (c:Crop) REQUIRE c.id IS UNIQUE;")
    tx.run("CREATE CONSTRAINT loc_id_unique  IF NOT EXISTS FOR (l:Location) REQUIRE l.id IS UNIQUE;")
    tx.run("CREATE CONSTRAINT seas_id_unique IF NOT EXISTS FOR (s:Season) REQUIRE s.id IS UNIQUE;")
    tx.run("CREATE CONSTRAINT dis_id_unique  IF NOT EXISTS FOR (d:Disease) REQUIRE d.id IS UNIQUE;")

    # Full-text used by resolve_crop()
    tx.run("""
    CREATE FULLTEXT INDEX cropFulltext IF NOT EXISTS
    FOR (c:Crop) ON EACH [c.name_bn, c.name_en, c.slug];
    """)


# --------- Loaders ----------
def load_nodes_crops(tx, df: pd.DataFrame):
    # Ensure expected columns exist
    for col in ("id", "name_bn", "name_en", "slug", "min_temp_c", "max_temp_c", "min_rh", "max_rh"):
        if col not in df.columns:
            df[col] = None

    for r in df.to_dict(orient="records"):
        # safe slug: derive from name_en/name_bn/id only if present; otherwise keep NULL
        base = _to_none(r.get("slug")) or _to_none(r.get("name_en")) or _to_none(r.get("name_bn")) or _to_none(r.get("id"))
        r["slug"] = slugify(base) if base else None

        tx.run("""
        MERGE (c:Crop {id:$id})
        SET c.name_bn    = $name_bn,
            c.name_en    = $name_en,
            c.slug       = $slug,
            c.min_temp_c = toFloat($min_temp_c),
            c.max_temp_c = toFloat($max_temp_c),
            c.min_rh     = toFloat($min_rh),
            c.max_rh     = toFloat($max_rh)
        """, **r)


def load_nodes_locations(tx, df: pd.DataFrame):
    for r in df.to_dict(orient="records"):
        tx.run("""
        MERGE (l:Location {id:$id})
        SET l.name_bn = $name_bn,
            l.level   = $level
        """, **r)


def load_nodes_seasons(tx, df: pd.DataFrame):
    for r in df.to_dict(orient="records"):
        tx.run("""
        MERGE (s:Season {id:$id})
        SET s.name_bn = $name_bn
        """, **r)


def load_nodes_diseases(tx, df: pd.DataFrame):
    for r in df.to_dict(orient="records"):
        tx.run("""
        MERGE (d:Disease {id:$id})
        SET d.name_bn = $name_bn,
            d.name_en = $name_en,
            d.notes   = $notes
        """, **r)


def load_rels_crop_season(tx, df: pd.DataFrame):
    for r in df.to_dict(orient="records"):
        tx.run("""
        MATCH (c:Crop {id:$crop_id}), (s:Season {id:$season_id})
        MERGE (c)-[r:SUITABLE_IN]->(s)
        SET r.transplant = $transplant,
            r.harvest    = $harvest
        """, **r)


def load_rels_crop_location(tx, df: pd.DataFrame):
    for r in df.to_dict(orient="records"):
        tx.run("""
        MATCH (c:Crop {id:$crop_id}), (l:Location {id:$location_id})
        MERGE (c)-[r:CULTIVATED_IN]->(l)
        SET r.season       = $season,
            r.transplant   = $transplant,
            r.harvest      = $harvest,
            r.avg_temp_c   = toFloat($avg_temp_c),
            r.avg_humidity = toFloat($avg_humidity),
            r.production   = toInteger($production)
        """, **r)


def load_rels_crop_disease(tx, df: pd.DataFrame):
    for r in df.to_dict(orient="records"):
        tx.run("""
        MATCH (c:Crop {id:$crop_id})
        MATCH (d:Disease {id:$disease_id})
        MERGE (c)-[r:SUFFER_FROM]->(d)
        SET r.notes = $notes
        """, **r)


# --------- Main ----------
def main():
    files = {
        "nodes_crops":        CSV_DIR / "nodes_crops.csv",
        "nodes_locations":    CSV_DIR / "nodes_locations.csv",
        "nodes_seasons":      CSV_DIR / "nodes_seasons.csv",
        "nodes_diseases":     CSV_DIR / "nodes_diseases.csv",       # optional
        "rels_crop_season":   CSV_DIR / "rels_crop_season.csv",
        "rels_crop_location": CSV_DIR / "rels_crop_location.csv",
        "rels_crop_disease":  CSV_DIR / "rels_crop_disease.csv",    # optional
    }

    # Existence checks for the required ones
    for key in ("nodes_crops", "nodes_locations", "nodes_seasons", "rels_crop_season", "rels_crop_location"):
        if not files[key].exists():
            raise FileNotFoundError(f"Missing: {files[key]}")

    with get_driver() as drv, drv.session() as sess:
        sess.execute_write(create_indexes)

        # load nodes
        sess.execute_write(load_nodes_crops,        _read_csv(files["nodes_crops"]))
        sess.execute_write(load_nodes_locations,    _read_csv(files["nodes_locations"]))
        sess.execute_write(load_nodes_seasons,      _read_csv(files["nodes_seasons"]))

        if files["nodes_diseases"].exists() and files["nodes_diseases"].stat().st_size > 0:
            sess.execute_write(load_nodes_diseases, _read_csv(files["nodes_diseases"]))

        # load rels
        cs = _read_csv(files["rels_crop_season"])
        if len(cs):
            sess.execute_write(load_rels_crop_season, cs)

        cl = _read_csv(files["rels_crop_location"])
        if len(cl):
            sess.execute_write(load_rels_crop_location, cl)

        if files["rels_crop_disease"].exists() and files["rels_crop_disease"].stat().st_size > 0:
            rd = _read_csv(files["rels_crop_disease"])
            if len(rd):
                sess.execute_write(load_rels_crop_disease, rd)

    print("✓ Graph loaded.")


if __name__ == "__main__":
    main()
