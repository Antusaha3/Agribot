from __future__ import annotations
import pandas as pd
from pathlib import Path
from slugify import slugify

ROOT = Path(__file__).resolve().parents[1]
CSV  = ROOT / "graph" / "csv"

SEED = CSV / "crop_disease_seed.csv"
NODES_D = CSV / "nodes_diseases.csv"
RELS_CD = CSV / "rels_crop_disease.csv"

def did(name_bn: str, name_en: str|None=None) -> str:
    base = name_en or name_bn or "unknown"
    return f"dis:{slugify(base)}"

def main():
    if not SEED.exists():
        raise FileNotFoundError(f"Missing seed file: {SEED}")

    seed = pd.read_csv(SEED)
    req = {"crop_id","crop_name_bn","disease_name_bn","disease_name_en","notes"}
    missing = req - set(seed.columns)
    if missing:
        raise SystemExit(f"Seed missing columns: {sorted(missing)}")

    # build disease nodes
    nodes = []
    rels  = []
    for _, r in seed.iterrows():
        d_id = did(r["disease_name_bn"], r.get("disease_name_en"))
        nodes.append({
            "id": d_id,
            "name_bn": r["disease_name_bn"],
            "name_en": r.get("disease_name_en", None),
            "notes":   r.get("notes", None),
        })
        rels.append({
            "crop_id":    r["crop_id"],
            "disease_id": d_id,
            "notes":      r.get("notes", None),
        })

    df_nodes = pd.DataFrame(nodes).drop_duplicates(subset=["id"]).reset_index(drop=True)
    df_rels  = pd.DataFrame(rels).drop_duplicates(subset=["crop_id","disease_id"]).reset_index(drop=True)

    df_nodes.to_csv(NODES_D, index=False, encoding="utf-8")
    df_rels.to_csv(RELS_CD, index=False, encoding="utf-8")
    print(f"Wrote {NODES_D} ({len(df_nodes)})")
    print(f"Wrote {RELS_CD} ({len(df_rels)})")

if __name__ == "__main__":
    main()
