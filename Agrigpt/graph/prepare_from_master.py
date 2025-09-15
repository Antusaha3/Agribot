# graph/prepare_from_master.py
from __future__ import annotations
import re, unicodedata, json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional
import pandas as pd
from slugify import slugify

# -----------------------
# Paths
# -----------------------
ROOT   = Path(__file__).resolve().parents[1]
MASTER = ROOT / "data" / "raw" / "agri_reference" / "agri_master.csv"  # <-- change if needed
OUTDIR = ROOT / "graph" / "csv"
OUTDIR.mkdir(parents=True, exist_ok=True)

# -----------------------
# Customize this mapping to your 44-col sheet
# Left = logical field; Right = exact column name in your CSV
# If a column doesn't exist, the script will fallback gracefully.
# -----------------------
COLMAP: Dict[str, str] = {
    # core
    "crop_name_bn":        "Crop_BN",         # ধান, গম, ভুট্টা...
    "crop_name_en":        "Crop_EN",
    "crop_type":           "Crop_Type",       # Cereals / Pulses / ...
    # variety
    "variety_name_bn":     "Variety_BN",
    "variety_name_en":     "Variety_EN",
    # disease
    "disease_name_bn":     "Disease_BN",
    "disease_name_en":     "Disease_EN",
    "disease_notes":       "Disease_Notes",   # optional
    # fertilizer
    "fert_name_bn":        "Fert_BN",
    "fert_name_en":        "Fert_EN",
    "npk_ratio":           "NPK",             # e.g., "20-10-10"
    "fert_stage":          "Fert_Stage",      # seedling/tillering/...
    "fert_dose":           "Fert_Dose",       # e.g., "80-60-40 kg/ha"
    # seasons (optional)
    "season":              "Season",
    "transplant":          "Transplant",
    "harvest":             "Harvest",
}

# -----------------------
# Helpers
# -----------------------
def norm_text(s: Optional[str]) -> str:
    if s is None: return ""
    t = unicodedata.normalize("NFC", str(s)).strip()
    t = re.sub(r"\s+", " ", t)
    return t

def has(df: pd.DataFrame, key: str) -> bool:
    return key in df.columns

def get(row: pd.Series, logical: str) -> str:
    col = COLMAP.get(logical)
    if not col: return ""
    if col not in row: return ""
    return norm_text(row[col])

def cid(prefix: str, *parts: str) -> str:
    joined = "-".join(slugify(p) for p in parts if p)
    return f"{prefix}:{joined}" if joined else f"{prefix}:unknown"

# -----------------------
# Main split
# -----------------------
def main():
    if not MASTER.exists():
        raise FileNotFoundError(f"Master CSV not found: {MASTER}")

    df = pd.read_csv(MASTER)
    # normalize column names to ease matching
    df.columns = [c.strip() for c in df.columns]

    rows_crop, rows_var, rows_dis, rows_fert = [], [], [], []
    rel_cv, rel_vd, rel_cf = [], [], []

    for _, r in df.iterrows():
        crop_bn = get(r, "crop_name_bn") or get(r, "crop_name_en")
        if not crop_bn:
            continue
        crop_en = get(r, "crop_name_en")
        crop_type = get(r, "crop_type")
        season = get(r, "season")
        transplant = get(r, "transplant")
        harvest = get(r, "harvest")

        # IDs
        crop_id = cid("crop", crop_bn or crop_en)

        # ---- Crop node
        rows_crop.append({
            "id": crop_id,
            "name_bn": crop_bn,
            "name_en": crop_en,
            "type": crop_type,
        })

        # ---- Variety node + rel
        variety_bn = get(r, "variety_name_bn") or get(r, "variety_name_en")
        if variety_bn:
            variety_en = get(r, "variety_name_en")
            var_id = cid("var", crop_bn or crop_en, variety_bn or variety_en)
            rows_var.append({
                "id": var_id,
                "name_bn": variety_bn,
                "name_en": variety_en,
                "crop_id": crop_id,
            })
            rel_cv.append({
                "crop_id": crop_id,
                "variety_id": var_id
            })

        # ---- Disease node + rel (via variety if present; else direct to crop later if you prefer)
        disease_bn = get(r, "disease_name_bn") or get(r, "disease_name_en")
        if disease_bn:
            disease_en = get(r, "disease_name_en")
            disease_notes = get(r, "disease_notes")
            dis_id = cid("dis", disease_bn or disease_en)
            rows_dis.append({
                "id": dis_id,
                "name_bn": disease_bn,
                "name_en": disease_en,
                "notes": disease_notes
            })
            if variety_bn:
                rel_vd.append({"variety_id": var_id, "disease_id": dis_id, "notes": disease_notes})
            else:
                # (optional) attach directly to crop if variety missing
                rel_vd.append({"variety_id": "", "disease_id": dis_id, "notes": f"(no variety) {disease_notes}"})

        # ---- Fertilizer node + rel (to crop; stage/dose as rel props)
        fert_bn = get(r, "fert_name_bn") or get(r, "fert_name_en")
        if fert_bn:
            fert_en = get(r, "fert_name_en")
            npk = get(r, "npk_ratio")
            stage = get(r, "fert_stage")
            dose = get(r, "fert_dose")
            fert_id = cid("fert", fert_bn or fert_en)
            rows_fert.append({
                "id": fert_id,
                "name_bn": fert_bn,
                "name_en": fert_en,
                "npk": npk
            })
            rel_cf.append({
                "crop_id": crop_id,
                "fert_id": fert_id,
                "stage": stage,
                "dose": dose
            })

    # ---- Deduplicate
    def dedup(lst, keys):
        df = pd.DataFrame(lst)
        if df.empty: return df
        return df.drop_duplicates(subset=keys).reset_index(drop=True)

    df_crops = dedup(rows_crop, ["id"])
    df_vars  = dedup(rows_var,  ["id"])
    df_dis   = dedup(rows_dis,  ["id"])
    df_fert  = dedup(rows_fert, ["id"])
    df_rel_cv = dedup(rel_cv,   ["crop_id","variety_id"])
    df_rel_vd = dedup(rel_vd,   ["variety_id","disease_id","notes"])
    df_rel_cf = dedup(rel_cf,   ["crop_id","fert_id","stage","dose"])

    # ---- Write out
    files = {
        "nodes_crops.csv": df_crops,
        "nodes_varieties.csv": df_vars,
        "nodes_diseases.csv": df_dis,
        "nodes_fertilizers.csv": df_fert,
        "rels_crop_variety.csv": df_rel_cv,
        "rels_variety_disease.csv": df_rel_vd,
        "rels_crop_fertilizer.csv": df_rel_cf,
    }
    for name, d in files.items():
        path = OUTDIR / name
        if not d.empty:
            d.to_csv(path, index=False, encoding="utf-8")
        else:
            # write an empty file with headers so loader won't crash
            pd.DataFrame(columns=list(d.columns) if hasattr(d, "columns") else []).to_csv(path, index=False, encoding="utf-8")
        print(f"Wrote {name} ({len(d)} rows)")

    # ---- Quick summary
    summary = {
        "crops": len(df_crops),
        "varieties": len(df_vars),
        "diseases": len(df_dis),
        "fertilizers": len(df_fert),
        "rel_crop_variety": len(df_rel_cv),
        "rel_variety_disease": len(df_rel_vd),
        "rel_crop_fertilizer": len(df_rel_cf),
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
