# graph/seed_from_clean.py
from __future__ import annotations
import os, json, re
from pathlib import Path
from typing import Iterable, Optional, Tuple, List, Dict
import pandas as pd
from slugify import slugify

# ---------- paths ----------
ROOT = Path(__file__).resolve().parents[1]

# If INTERIM_DIR env is set, use it; otherwise default to repo's data/interim
_env_interim = os.getenv("INTERIM_DIR")
if _env_interim:
    INTERIM = Path(_env_interim)
else:
    INTERIM = ROOT / "D:/project=2/Agrigpt/notebook/data/interim"

OUTDIR = ROOT / "graph" / "csv"
OUTDIR.mkdir(parents=True, exist_ok=True)

# ---------- helpers ----------
def crop_id(name: str) -> str:
    return f"crop:{slugify(name or 'unknown')}"

def loc_id(district: str) -> str:
    return f"loc:district:{slugify(district or 'unknown')}"

def season_id(season: str) -> str:
    return f"season:{slugify(season or 'unknown')}"

def disease_id(name: str) -> str:
    return f"dis:{slugify(name or 'unknown')}"

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def parse_range_from_line(text: str, key_bn: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Extract 'a–b' or 'a-b' numbers after a given key (e.g., তাপমাত্রা: 12–40).
    Works even if units (°C, %) exist after b.
    """
    try:
        idx = text.find(key_bn)
        if idx == -1:
            return None, None
        seg = text[idx: idx+120]  # small window
        m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*[–-]\s*([0-9]+(?:\.[0-9]+)?)", seg)
        if not m:
            return None, None
        return safe_float(m.group(1)), safe_float(m.group(2))
    except Exception:
        return None, None

def parse_value_after(text: str, key_bn: str) -> Optional[str]:
    """
    Find a single value right after a key (e.g., 'মৌসুম: Kharif 1 | ...' -> 'Kharif 1').
    """
    idx = text.find(key_bn)
    if idx == -1:
        return None
    tail = text[idx + len(key_bn):]
    # up to the next pipe or line end
    val = tail.split("|")[0].strip()
    # drop trailing punctuation
    return val.strip(" :।,;/")

def parse_list_after(text: str, keys: Iterable[str]) -> List[str]:
    """
    Heuristic: if the text contains something like 'রোগ: ব্লাস্ট, শীথ ব্লাইট; ...',
    split on common separators and return a clean list.
    """
    for k in keys:
        idx = text.find(k)
        if idx != -1:
            tail = text[idx + len(k):]
            # up to the next major break
            tail = tail.split("\n")[0]
            tail = tail.split("|")[0]
            # split by common separators
            parts = re.split(r"[;,/|、،]+", tail)
            out = [p.strip(" :।,;/-") for p in parts if p.strip()]
            # filter out overly long garbage
            out = [p for p in out if 0 < len(p) <= 80]
            return out[:15]
    return []

# ---------- main ----------
def main():
    # Load the cleaned pairs you already created in Phase 2
    ba_txt   = (INTERIM / "bangladesh_agri_clean.txt").read_text(encoding="utf-8").splitlines()
    ba_meta  = [json.loads(l) for l in (INTERIM / "bangladesh_agri_clean.jsonl").read_text(encoding="utf-8").splitlines()]

    sp_txt   = (INTERIM / "spas_bd_clean.txt").read_text(encoding="utf-8").splitlines()
    sp_meta  = [json.loads(l) for l in (INTERIM / "spas_bd_clean.jsonl").read_text(encoding="utf-8").splitlines()]

    # --- aggregates we will emit as CSVs ---
    crops: Dict[str, Dict] = {}                # Crop nodes
    locations: Dict[str, Dict] = {}            # Location nodes (district)
    seasons: Dict[str, Dict] = {}              # Season nodes
    crop_season_rows: List[Dict] = []          # rels_crop_season
    crop_loc_rows: List[Dict] = []             # rels_crop_location
    diseases: Dict[str, Dict] = {}             # Disease nodes
    crop_dis_rows: List[Dict] = []             # rels_crop_disease

    # ---- 1) From Bangladesh-Agri lines: crop climate + (maybe) season + diseases
    for t, m in zip(ba_txt, ba_meta):
        crop = (m.get("fields", {}).get("crop_name")) or parse_value_after(t, "ফসল:")
        if not crop:
            continue

        cid = crop_id(crop)

        # temperature & humidity ranges if present
        tmin, tmax   = parse_range_from_line(t, "তাপমাত্রা")
        rhmin, rhmax = parse_range_from_line(t, "আপেক্ষিক আর্দ্রতা")

        acc = crops.setdefault(cid, {
            "id": cid,
            "name_bn": crop,
            "name_en": "",         # keep for future
            "min_temp_c": None,
            "max_temp_c": None,
            "min_rh": None,
            "max_rh": None,
        })
        if tmin is not None:
            acc["min_temp_c"] = min(acc["min_temp_c"], tmin) if acc["min_temp_c"] is not None else tmin
        if tmax is not None:
            acc["max_temp_c"] = max(acc["max_temp_c"], tmax) if acc["max_temp_c"] is not None else tmax
        if rhmin is not None:
            acc["min_rh"] = min(acc["min_rh"], rhmin) if acc["min_rh"] is not None else rhmin
        if rhmax is not None:
            acc["max_rh"] = max(acc["max_rh"], rhmax) if acc["max_rh"] is not None else rhmax

        # season / transplant / harvest (if present)
        season     = (m.get("fields", {}).get("season"))     or parse_value_after(t, "মৌসুম:")
        transplant = (m.get("fields", {}).get("transplant")) or parse_value_after(t, "রোপণ:")
        harvest    = (m.get("fields", {}).get("harvest"))    or parse_value_after(t, "কাটাই:")

        if season:
            sid = season_id(season)
            seasons.setdefault(sid, {"id": sid, "name_bn": season})
            crop_season_rows.append({
                "crop_id": cid,
                "season_id": sid,
                "transplant": transplant or "",
                "harvest": harvest or "",
            })

        # try to detect disease list if available in this text/metadata
        meta_dis = (m.get("fields", {}).get("diseases"))
        if isinstance(meta_dis, list) and meta_dis:
            dis_list = [str(x).strip() for x in meta_dis if str(x).strip()]
        else:
            # Heuristics in Bangla/English
            dis_list = parse_list_after(t, keys=["রোগ:", "রোগসমূহ:", "Diseases:", "Disease:"])

        for dname in dis_list:
            did = disease_id(dname)
            diseases.setdefault(did, {"id": did, "name_bn": dname, "name_en": "", "notes": ""})
            crop_dis_rows.append({"crop_id": cid, "disease_id": did, "notes": ""})

    # ---- 2) From SPAS lines: per-district cultivation + average climate/production
    for t, m in zip(sp_txt, sp_meta):
        crop     = (m.get("fields", {}).get("crop_name")) or parse_value_after(t, "ফসল:")
        dist     = (m.get("fields", {}).get("district"))  or parse_value_after(t, "জেলা:")
        season   = (m.get("fields", {}).get("season"))    or parse_value_after(t, "মৌসুম:")
        trans    = (m.get("fields", {}).get("transplant")) or parse_value_after(t, "রোপণ:")
        harvest  = (m.get("fields", {}).get("harvest"))    or parse_value_after(t, "কাটাই:")
        avg_t    = m.get("fields", {}).get("avg_temp_c")
        avg_h    = m.get("fields", {}).get("avg_humidity")
        prod     = m.get("fields", {}).get("production")

        if not crop or not dist:
            continue

        cid = crop_id(crop)
        lid = loc_id(dist)
        locations.setdefault(lid, {"id": lid, "name_bn": dist, "level": "district"})
        if season:
            sid = season_id(season)
            seasons.setdefault(sid, {"id": sid, "name_bn": season})

        crop_loc_rows.append({
            "crop_id": cid,
            "location_id": lid,
            "season": season or "",
            "transplant": trans or "",
            "harvest": harvest or "",
            "avg_temp_c": avg_t if avg_t is not None else "",
            "avg_humidity": avg_h if avg_h is not None else "",
            "production": int(prod) if pd.notna(prod) else "",
        })

    # ---------- write CSVs ----------
    def write(df: pd.DataFrame, name: str):
        path = OUTDIR / name
        df.to_csv(path, index=False, encoding="utf-8")
        print(f"Wrote {name} ({len(df)} rows)")

    # nodes
    df_crops = pd.DataFrame.from_records(list(crops.values())) if crops else pd.DataFrame(
        columns=["id","name_bn","name_en","min_temp_c","max_temp_c","min_rh","max_rh"]
    )
    df_locs  = pd.DataFrame.from_records(list(locations.values())) if locations else pd.DataFrame(
        columns=["id","name_bn","level"]
    )
    df_seas  = pd.DataFrame.from_records(list(seasons.values())) if seasons else pd.DataFrame(
        [{"id":"season:kharif-1","name_bn":"Kharif 1"}]
    )

    # rels
    df_rel_cs = pd.DataFrame.from_records(crop_season_rows) if crop_season_rows else pd.DataFrame(
        columns=["crop_id","season_id","transplant","harvest"]
    )
    df_rel_cl = pd.DataFrame.from_records(crop_loc_rows) if crop_loc_rows else pd.DataFrame(
        columns=["crop_id","location_id","season","transplant","harvest","avg_temp_c","avg_humidity","production"]
    )

    # diseases (new)
    df_dis    = pd.DataFrame.from_records(list(diseases.values())) if diseases else pd.DataFrame(
        columns=["id","name_bn","name_en","notes"]
    )
    df_rel_cd = pd.DataFrame.from_records(crop_dis_rows) if crop_dis_rows else pd.DataFrame(
        columns=["crop_id","disease_id","notes"]
    )

    # de-duplicate where appropriate
    if not df_crops.empty:
        df_crops.drop_duplicates(subset=["id"], inplace=True)
    if not df_locs.empty:
        df_locs.drop_duplicates(subset=["id"], inplace=True)
    if not df_seas.empty:
        df_seas.drop_duplicates(subset=["id"], inplace=True)
    if not df_dis.empty:
        df_dis.drop_duplicates(subset=["id"], inplace=True)
    if not df_rel_cs.empty:
        df_rel_cs.drop_duplicates(subset=["crop_id","season_id","transplant","harvest"], inplace=True)
    if not df_rel_cl.empty:
        df_rel_cl.drop_duplicates(subset=["crop_id","location_id","season","transplant","harvest","production"], inplace=True)
    if not df_rel_cd.empty:
        df_rel_cd.drop_duplicates(subset=["crop_id","disease_id","notes"], inplace=True)

    # write
    write(df_crops, "nodes_crops.csv")
    write(df_locs,  "nodes_locations.csv")
    write(df_seas,  "nodes_seasons.csv")
    write(df_rel_cs,"rels_crop_season.csv")
    write(df_rel_cl,"rels_crop_location.csv")
    write(df_dis,   "nodes_diseases.csv")     # NEW
    write(df_rel_cd,"rels_crop_disease.csv")  # NEW

    print("All CSVs written to:", OUTDIR)

if __name__ == "__main__":
    main()
